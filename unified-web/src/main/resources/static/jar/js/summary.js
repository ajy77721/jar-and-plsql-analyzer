/**
 * Summary — Main orchestrator.
 * State + config loading + render entry point.
 * All methods come from component files (summary-*.js).
 *
 * Load order: helpers → analyzer → tables → trace → explorer → codeview → export → this file
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /* --- State --- */
    _config: null,
    _prefixList: null,
    _sbDomainMap: null,
    _viewContains: null,
    _viewStartsWith: null,
    _operationTypes: null,
    _jarIgnoreSegments: null,
    _batchKeywords: null,
    _sizeThresholds: null,
    _perfThresholds: null,
    _complexityRules: null,
    _epReports: null,
    _vertReport: null,
    _extReport: null,
    _distReport: null,
    _batchReport: null,
    _viewsReport: null,
    _classIdx: null,
    _loadedSlices: null,
    _loadingSlice: null,

    _clearSubTabData() {
        if (!this._analysis || !this._analysis.endpoints) return;
        for (const ep of this._analysis.endpoints) {
            delete ep.externalCalls;
            delete ep.httpCalls;
            delete ep.dynamicFlows;
            delete ep.aggregationFlows;
            delete ep.beans;
        }
        if (this._loadedSlices) {
            this._loadedSlices.externalCalls = false;
            this._loadedSlices.dynamicFlows = false;
            this._loadedSlices.aggregationFlows = false;
            this._loadedSlices.beans = false;
        }
        // Mark lazy tabs as needing re-render
        const tabIds = ['aggregation', 'dynamic', 'ext-report', 'vertical', 'vert-report'];
        for (const tid of tabIds) {
            const tab = document.getElementById('stab-' + tid);
            if (tab) tab.dataset.rendered = '';
        }
    },

    /* --- Config (loaded from domain-config.json) --- */
    async _loadConfig() {
        if (this._config) return;
        try {
            const resp = await fetch('domain-config.json');
            this._config = await resp.json();
        } catch (e) {
            console.warn('Failed to load domain-config.json', e);
            this._config = {};
        }
        const pairs = [];
        for (const [domain, info] of Object.entries(this._config.module_types || {})) {
            for (const prefix of (info.prefixes || [])) pairs.push([prefix, domain]);
        }
        pairs.sort((a, b) => b[0].length - a[0].length);
        this._prefixList = pairs;
        this._sbDomainMap = this._config.snapshot_buffer_domain_map || {};
        const vd = this._config.view_detection || {};
        this._viewContains = vd.contains || [];
        this._viewStartsWith = vd.starts_with || [];
        this._operationTypes = this._config.operation_types || {};
        this._jarIgnoreSegments = new Set(this._config.jar_name_ignore_segments || []);
        this._batchKeywords = [];
        for (const [type, info] of Object.entries(this._config.endpoint_classification || {})) {
            for (const kw of (info.keywords || [])) this._batchKeywords.push(kw);
        }
        this._sizeThresholds = this._config.size_thresholds || { S: 5, M: 20, L: 50 };
        this._perfThresholds = this._config.performance_thresholds || [];
        this._complexityRules = this._config.complexity_rules || null;
    },

    /* ===== Main Entry ===== */
    async render(analysis) {
        const container = document.getElementById('summary-container');
        if (!container) return;
        if (!analysis || !analysis.endpoints) {
            container.innerHTML = '<p class="empty-state" style="padding:40px">No analysis loaded</p>';
            return;
        }
        await this._loadConfig();
        this._render(analysis, container);
    },

    _render(analysis, container) {
        JA.nav.init();
        this._analysis = analysis;
        this._loadedSlices = { externalCalls: false, dynamicFlows: false, aggregationFlows: false, beans: false };
        this._loadingSlice = null;
        this._renderReady = new Promise(res => { this._renderReadyResolve = res; });
        const esc = JA.utils.escapeHtml;
        const totalEps = (analysis.endpoints || []).length;

        // Build class index from lightweight classIndex (or full classes as fallback)
        const classIdx = {};
        const classSource = analysis.classIndex || analysis.classes || [];
        for (const cls of classSource) {
            classIdx[cls.fullyQualifiedName] = cls;
            if (!classIdx[cls.simpleName]) classIdx[cls.simpleName] = cls;
        }
        this._classIdx = classIdx;

        // Entity @Document -> collection (precomputed by server or built from classIndex)
        let entityCollMap = analysis.entityCollMap || {};
        if (!analysis.entityCollMap) {
            entityCollMap = {};
            for (const cls of classSource) {
                for (const ann of (cls.annotations || [])) {
                    if (ann.name === 'Document' && ann.attributes) {
                        const cn = ann.attributes.collection || ann.attributes.value;
                        if (cn) { entityCollMap[cls.fullyQualifiedName] = cn; entityCollMap[cls.simpleName] = cn; }
                    }
                }
            }
        }

        // Repository -> collection (precomputed by server or built from classIndex)
        let repoCollMap = analysis.repoCollMap || {};
        if (!analysis.repoCollMap) {
            repoCollMap = {};
            for (const cls of classSource) {
                if (cls.stereotype !== 'REPOSITORY' && cls.stereotype !== 'SPRING_DATA') continue;
                const name = cls.simpleName || '';
                const stems = [];
                if (name.endsWith('Repository')) stems.push(name.slice(0, -10));
                if (name.endsWith('Repo')) stems.push(name.slice(0, -4));
                for (const stem of stems) {
                    for (const [eKey, coll] of Object.entries(entityCollMap)) {
                        if (eKey.endsWith(stem) || eKey === stem) {
                            repoCollMap[cls.fullyQualifiedName] = coll;
                            repoCollMap[cls.simpleName] = coll;
                            break;
                        }
                    }
                    if (repoCollMap[cls.simpleName]) break;
                }
                for (const method of (cls.methods || cls.methodSummaries || [])) {
                    for (const ann of (method.annotations || [])) {
                        for (const val of Object.values(ann.attributes || {})) {
                            const strs = Array.isArray(val) ? val.flat(3).map(String) : [String(val)];
                            for (const s of strs) {
                                for (const ref of this._extractCollRefs(s)) {
                                    if (!repoCollMap[cls.fullyQualifiedName]) {
                                        repoCollMap[cls.fullyQualifiedName] = ref;
                                        repoCollMap[cls.simpleName] = ref;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Show progress skeleton immediately
        container.innerHTML = `<div class="sum-page">
            <div class="sum-progress-bar" id="sum-progress">
                <div class="sum-progress-fill" id="sum-progress-fill" style="width:0%"></div>
            </div>
            <div class="sum-progress-text" id="sum-progress-text">Analyzing endpoints: 0 / ${totalEps}</div>
            <div class="sum-stats" id="sum-stats-live"></div>
            <div id="sum-content-area"></div>
        </div>`;

        // Process endpoints in async batches for progressive display
        const batchSize = 30;
        const endpoints = analysis.endpoints || [];
        const epReports = [];
        let idx = 0;

        const processBatch = () => {
            const end = Math.min(idx + batchSize, endpoints.length);
            for (let i = idx; i < end; i++) {
                const r = this._analyzeEndpoint(endpoints[i], classIdx, entityCollMap, repoCollMap);
                if (r) { r.sourceIdx = i; epReports.push(r); }
            }
            idx = end;

            // Update progress
            const pct = Math.round((idx / Math.max(endpoints.length, 1)) * 100);
            const fill = document.getElementById('sum-progress-fill');
            const text = document.getElementById('sum-progress-text');
            if (fill) fill.style.width = pct + '%';
            if (text) text.textContent = `Analyzing endpoints: ${idx} / ${totalEps} (${epReports.length} with data)`;

            // Update live stats
            this._updateLiveStats(epReports);

            if (idx < endpoints.length) {
                requestAnimationFrame(processBatch);
            } else {
                // All done — build final reports and render
                this._finishRender(analysis, epReports, container, esc);
            }
        };
        requestAnimationFrame(processBatch);
    },

    _updateLiveStats(epReports) {
        const el = document.getElementById('sum-stats-live');
        if (!el) return;
        const colls = new Set(), views = new Set();
        let cross = 0;
        for (const r of epReports) {
            for (const [name, c] of Object.entries(r.collections)) {
                colls.add(name);
                if (c.type === 'VIEW') views.add(name);
            }
            if (r.externalScopeCalls > 0) cross++;
        }
        el.innerHTML =
            `<div class="sum-stat-card"><div class="sum-stat-number">${epReports.length}</div><div class="sum-stat-label">Endpoints</div></div>` +
            `<div class="sum-stat-card"><div class="sum-stat-number">${colls.size}</div><div class="sum-stat-label">Collections</div></div>` +
            `<div class="sum-stat-card"><div class="sum-stat-number">${views.size}</div><div class="sum-stat-label">Views</div></div>` +
            `<div class="sum-stat-card"><div class="sum-stat-number">${cross}</div><div class="sum-stat-label">Cross-Module</div></div>`;
    },

    _finishRender(analysis, epReports, container, esc) {
        // Aggregate stats — computed inline so heavy report builds are deferred to first tab access
        const allColls = new Set(), allViews = new Set(), allModules = new Set(), allDomains = new Set();
        let crossModuleEps = 0, txnRequired = 0, totalExtCalls = 0, batchCount = 0;
        for (const r of epReports) {
            for (const [name, c] of Object.entries(r.collections)) {
                allColls.add(name);
                if (c.type === 'VIEW') allViews.add(name);
                if (c.domain && c.domain !== 'Other') allDomains.add(c.domain);
            }
            totalExtCalls += r.externalScopeCalls;
            if (r.externalScopeCalls > 0) crossModuleEps++;
            for (const m of r.modules) allModules.add(m);
            const writeDomains = new Set();
            for (const [, c] of Object.entries(r.collections)) {
                if (c.operations.has('WRITE') || c.operations.has('UPDATE') || c.operations.has('DELETE')) {
                    writeDomains.add(c.domain);
                }
            }
            if (writeDomains.size > 1) txnRequired++;
            if (this._isBatchEndpoint(r)) batchCount++;
        }

        // Count scheduled jobs (prefer precomputed from summary endpoint)
        let scheduledCount = analysis.scheduledCount || 0;
        if (!scheduledCount && analysis.scheduledJobs) {
            scheduledCount = analysis.scheduledJobs.length;
        }

        // Cache for exports and lazy tabs
        this._analysis = analysis;
        this._epReports = epReports;
        this._vertReport = null;
        this._extReport = null;
        this._distReport = null;
        this._batchReport = null;
        this._viewsReport = null;
        this._totalExtCalls = totalExtCalls;
        this._loadedSlices = { externalCalls: false, dynamicFlows: false, aggregationFlows: false, beans: false };
        this._loadingSlice = null;

        // Render final page
        let html = '<div class="sum-page">';

        // Compact stats bar — single line: actions + inline stats
        const stats = [
            { key: 'endpoints', num: epReports.length, label: 'Endpoints', desc: 'REST API endpoints found in controller classes' },
            { key: 'collections', num: allColls.size, label: 'Collections', desc: 'Unique MongoDB collections accessed across all endpoints' },
            { key: 'views', num: allViews.size, label: 'Views', desc: 'MongoDB view collections (read-only aggregation pipelines)' },
            { key: 'modules', num: allModules.size, label: 'Modules', desc: 'Separate JAR modules/libraries detected in the call chain' },
            { key: 'domains', num: allDomains.size, label: 'Domains', desc: 'Business domains identified from collection name prefixes' },
            { key: 'crossmodule', num: crossModuleEps, label: 'Cross-Module', desc: 'Endpoints that call into classes from other JAR modules' },
            { key: 'loc', num: epReports.reduce((s, r) => s + (r.totalLoc || 0), 0), label: 'LOC', desc: 'Total lines of code across all endpoint call trees' },
            { key: 'txn', num: txnRequired, label: 'Txn Req', desc: 'Endpoints that write to multiple collections and need transactions' },
            { key: 'batch', num: batchCount, label: 'Batch', desc: 'Endpoints classified as batch/scheduler jobs' },
            { key: 'scheduled', num: scheduledCount, label: 'Scheduled', desc: 'Methods annotated with @Scheduled (cron, fixedRate, fixedDelay)' }
        ];
        html += '<div class="sum-topbar">';
        html += '<div class="sum-topbar-actions">';
        html += '<button class="btn-sm btn-export" onclick="JA.summary.showExportModal()">Export</button>';
        html += '</div>';
        html += '<div class="sum-topbar-stats" id="sum-topbar-stats">';
        for (const s of stats) {
            html += `<span class="sum-chip" data-stat="${s.key}" onclick="JA.summary.showStatPopup('${s.key}')" title="${s.desc}">`;
            html += `<b>${s.num}</b> ${s.label}`;
            html += '</span>';
        }
        html += '</div>';
        html += '</div>';

        // Sub-tabs with rich tooltip guidance
        const tabDefs = [
            { cls: 'sum-tab-ep', id: 'ep-report', label: 'Endpoint Report', tip:
                'WHAT: Full call-tree analysis of every REST endpoint — collections, domains, LOC, DB operations, cross-module calls.\n'
                + 'WHY: Your starting point. Identifies the largest, most complex, and most cross-cutting endpoints that drive migration risk.\n'
                + 'LOOK FOR: Endpoints with high LOC (>500), many collections (>5), or cross-domain writes — these are your migration bottlenecks.\n'
                + 'TIP: Sort by Collections or LOC to find hotspots. Filter by domain to focus on one vertical at a time.' },
            { cls: 'sum-tab-coll', id: 'vert-report', label: 'Collection Analysis', tip:
                'WHAT: Maps every MongoDB collection to its owning domain — shows read/write/aggregate operations, endpoint usage, and verification status.\n'
                + 'WHY: Data ownership clarity is the foundation of vertical slicing. Shared collections block independent deployment.\n'
                + 'LOOK FOR: Collections used by multiple domains (shared data), collections with only READ ops from other domains (candidates for API replacement), and NOT_IN_DB collections (possible dead code).\n'
                + 'TIP: Use column filters to isolate a single domain and see all its owned collections plus which other domains access them.' },
            { cls: 'sum-tab-ext', id: 'ext-report', label: 'External Dependencies', tip:
                'WHAT: All cross-module JAR dependencies — which external libraries/modules each endpoint calls and how often.\n'
                + 'WHY: Direct JAR dependencies between modules must be replaced with REST APIs for independent deployability.\n'
                + 'LOOK FOR: Modules with high call counts (tight coupling), shared utility modules called from everywhere, and circular dependencies between domains.\n'
                + 'TIP: Each module listed here becomes either a shared library, an API contract, or gets absorbed into the calling vertical.' },
            { cls: 'sum-tab-dist', id: 'dist-report', label: 'Distributed Transactions', tip:
                'WHAT: Endpoints that write to multiple collections across different domains — potential distributed transaction boundaries.\n'
                + 'WHY: When domains become separate services, cross-domain writes can no longer be atomic. These need saga patterns or @Transactional boundaries.\n'
                + 'LOOK FOR: "REQUIRED - cross-domain writes" entries — each one is a future consistency risk. Single-domain writes are safe.\n'
                + 'TIP: Prioritize endpoints that write to 3+ domains — these are the hardest to decompose and may need architectural redesign.' },
            { cls: 'sum-tab-batch', id: 'batch-report', label: 'Batch Jobs', tip:
                'WHAT: Endpoints classified as batch/scheduler jobs based on naming patterns and annotations.\n'
                + 'WHY: Batch jobs often have hidden cross-domain dependencies, high resource impact, and different SLA requirements than REST endpoints.\n'
                + 'LOOK FOR: Batch jobs touching collections from multiple domains — these need special migration planning since they often run at scale.\n'
                + 'TIP: Compare batch jobs with the Scheduled Jobs tab to see which are triggered by cron vs. REST calls.' },
            { cls: 'sum-tab-sched', id: 'scheduled', label: 'Scheduled Jobs', tip:
                'WHAT: Methods annotated with @Scheduled — cron expressions, fixed-rate, and fixed-delay tasks found in the codebase.\n'
                + 'WHY: Scheduled tasks run autonomously and may have cross-domain data dependencies. During migration, they need to be assigned to the correct vertical.\n'
                + 'LOOK FOR: Cron expressions that overlap (resource contention), scheduled methods that access collections from other domains, and high-frequency tasks.\n'
                + 'TIP: Tasks with cross-domain collection access may need to be split or converted to event-driven patterns after verticalisation.' },
            { cls: 'sum-tab-agg', id: 'aggregation', label: 'Aggregation Flows', tip:
                'WHAT: MongoDB aggregation pipelines detected in the codebase — $lookup joins, pipeline stages ($match, $group, $project, $sort), collection references, and operation types.\n'
                + 'WHY: Aggregation pipelines are the most complex database operations. $lookup joins create hard dependencies between collections that cannot be split across services.\n'
                + 'LOOK FOR: $lookup joins between collections from different domains (these BLOCK vertical splitting), pipelines with 5+ stages (complexity risk), and DYNAMIC aggregations (runtime-built pipelines that static analysis may miss).\n'
                + 'TIP: Use column filters on Collections and Pipeline Stages to find all pipelines touching a specific collection. Click a row to see the full pipeline detail including $match fields, $group keys, and the call path from controller to repository.' },
            { cls: 'sum-tab-dyn', id: 'dynamic', label: 'Dynamic Flows', tip:
                'WHAT: Non-direct method dispatch patterns — interface dispatch, Spring bean injection, reflection, dynamic SQL/queries, and strategy patterns.\n'
                + 'WHY: Static analysis cannot fully resolve dynamic dispatch. These are blind spots where the actual runtime behavior may differ from what the analyzer detected.\n'
                + 'LOOK FOR: INTERFACE_DISPATCH (multiple implementations — which one runs?), REFLECTION (completely opaque calls), DYNAMIC_QUERY (runtime-built MongoDB queries that may access unknown collections).\n'
                + 'TIP: Each dynamic flow is a manual review item. Click a row to see the resolved-from class and the call path. Prioritize DYNAMIC_QUERY entries — they may access collections not captured in the static analysis.' },
            { cls: 'sum-tab-vert', id: 'vertical', label: 'Verticalisation', tip:
                'WHAT: Verification of domain boundary violations — direct method calls and collection accesses that cross module boundaries.\n'
                + 'WHY: Every cross-boundary call becomes an API contract after verticalisation. This tab shows exactly what needs to change.\n'
                + 'LOOK FOR: High-frequency cross-boundary methods (hot paths that need low-latency APIs), collection accesses from outside the owning domain, and circular call patterns between modules.\n'
                + 'TIP: Filter by source domain to see all outgoing dependencies of one vertical. These are the API contracts that domain must either expose or consume.' },
            { cls: 'sum-tab-claude', id: 'claude', label: 'Claude Insights', tip:
                'WHAT: AI-powered deep analysis — business process names, risk flags, migration recommendations, and complexity assessments for each endpoint.\n'
                + 'WHY: Static analysis tells you WHAT the code does. Claude tells you WHY it exists and how hard it will be to migrate.\n'
                + 'LOOK FOR: High-risk flags, endpoints Claude marks as "complex migration", and business process groupings that suggest natural vertical boundaries.\n'
                + 'TIP: Available after running Claude enrichment. Compare Claude\'s business names with your team\'s domain model to validate vertical boundaries.' },
            { cls: 'sum-tab-corr', id: 'corrections', label: 'Claude Corrections', tip:
                'WHAT: Side-by-side comparison of static analysis vs. Claude-corrected results — added, removed, and verified collections per endpoint.\n'
                + 'WHY: Static bytecode analysis has blind spots (dynamic queries, string-built collection names, config-driven routing). Claude verifies against the MongoDB catalog and source code.\n'
                + 'LOOK FOR: ADDED collections (missed by static analysis — potential blind spots in your migration plan), REMOVED collections (false positives from static analysis), and verification rates per domain.\n'
                + 'TIP: Available after running Claude correction scan. Focus on ADDED collections — these represent real data dependencies that static analysis missed.' },
            { cls: 'sum-tab-flow', id: 'flow', label: 'Flow Analysis', tip:
                'WHAT: Two one-click actions — (1) Prepare Test Data: walks each endpoint call tree and fetches representative MongoDB records for test datasets. (2) Optimization Analysis: detects missing indexes, N+1 patterns, bulk read/write candidates, aggregation rewrites, and classifies queries as static (cacheable) or transactional.\n'
                + 'WHY: Test data prep gives developers realistic data for unit tests without hand-crafting fixtures. Optimization analysis surfaces DB anti-patterns that slow your endpoints.\n'
                + 'LOOK FOR: HIGH-severity findings (N+1, full-table scans, unbounded sorts), STATIC_CACHEABLE queries (load once at startup), and AGGREGATION_REWRITE suggestions ($lookup can eliminate N separate queries).\n'
                + 'TIP: Download the .md report and share it with the developer who owns the affected endpoint — it includes ready-to-run createIndex commands and suggested code snippets.' }
        ];
        html += '<div class="sum-tabs">';
        for (let ti = 0; ti < tabDefs.length; ti++) {
            const t = tabDefs[ti];
            const active = ti === 0 ? ' active' : '';
            html += `<button class="sum-tab ${t.cls}${active}" data-stab="${t.id}" onclick="JA.summary.switchSubTab('${t.id}')" title="${esc(t.tip)}">${t.label}</button>`;
        }
        html += '</div>';

        // Render only the active tab immediately; others render on first access
        html += '<div class="sum-subtab" id="stab-ep-report">' + this._renderEndpointTable(epReports, esc) + '</div>';
        html += '<div class="sum-subtab" id="stab-vert-report" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-ext-report" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-dist-report" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-batch-report" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-scheduled" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-aggregation" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-dynamic" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-vertical" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-claude" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-corrections" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-flow" style="display:none"></div>';

        html += '</div>';
        container.innerHTML = html;

        // Stamp the container and ep-report tab with the current data version
        container.dataset.renderedVersion = String(JA.app._dataVersion || 0);
        const epTab = document.getElementById('stab-ep-report');
        if (epTab) {
            epTab.dataset.rendered = '1';
            epTab.dataset.renderedVersion = String(JA.app._dataVersion || 0);
        }

        // Update top stats bar when endpoint filters change
        const epState = this._pageState['sum-ep'];
        if (epState) {
            epState.onFilter = () => {
                const bar = document.getElementById('sum-topbar-stats');
                if (!bar) return;
                const data = epState.filtered;
                const colls = new Set(), mods = new Set(), doms = new Set();
                let loc = 0, cross = 0;
                for (const r of data) {
                    loc += r.totalLoc || 0;
                    if (r.externalScopeCalls > 0) cross++;
                    for (const c of Object.keys(r.collections || {})) { colls.add(c); doms.add(r.collections[c].domain); }
                    for (const m of (r.modules || [])) mods.add(m);
                }
                const up = { endpoints: data.length, collections: colls.size, modules: mods.size, domains: doms.size, crossmodule: cross, loc };
                for (const [k, v] of Object.entries(up)) {
                    const chip = bar.querySelector(`[data-stat="${k}"] b`);
                    if (chip) chip.textContent = v;
                }
            };
        }

        // Store lazy renderers (keyed by tab id suffix)
        // Use this._* references so closures always read the latest cached data
        this._lazyTabs = {
            'vert-report': () => {
                if (!this._vertReport) {
                    this._vertReport = this._buildVerticalisation(this._epReports);
                    this._viewsReport = this._buildViewsAnalysis(this._vertReport, this._epReports);
                }
                return this._renderVertTable(this._vertReport, this._viewsReport, esc);
            },
            'ext-report': () => {
                if (!this._extReport) this._extReport = this._buildExternalDeps(this._epReports);
                return this._renderExternalTable(this._extReport, esc);
            },
            'dist-report': () => {
                if (!this._distReport) this._distReport = this._buildDistributedTransactions(this._epReports);
                return this._renderDistTable(this._distReport, esc);
            },
            'batch-report': () => {
                if (!this._batchReport) this._batchReport = this._buildBatchAnalysis(this._epReports);
                return this._renderBatchTable(this._batchReport, esc);
            },
            'scheduled': () => this._renderScheduledTab ? this._renderScheduledTab(this._analysis, esc) : '<p class="sum-muted" style="padding:20px">Scheduled jobs loading...</p>',
            'aggregation': () => this._renderAggregationTab ? this._renderAggregationTab(this._epReports, esc) : '<p class="sum-muted" style="padding:20px">Aggregation flows loading...</p>',
            'dynamic': () => this._renderDynamicTab ? this._renderDynamicTab(this._epReports, esc) : '<p class="sum-muted" style="padding:20px">Dynamic flows loading...</p>',
            'vertical': () => this._renderVertVerification ? this._renderVertVerification(this._epReports, esc) : '',
            'claude': () => this._renderClaudeTab ? this._renderClaudeTab(this._epReports, esc) : '<p class="sum-muted" style="padding:20px">Claude insights loading...</p>',
            'corrections': () => this._renderCorrectionsTab ? this._renderCorrectionsTab(this._epReports, esc) : '<p class="sum-muted" style="padding:20px">Claude corrections loading...</p>',
            'flow': () => {
                const html = JA.flowAnalysis ? JA.flowAnalysis._renderFlowTab(this._analysis, esc) : '<p class="sum-muted" style="padding:20px">Flow analysis loading...</p>';
                // Pre-fill connection fields after DOM is ready
                if (JA.flowAnalysis) setTimeout(() => JA.flowAnalysis._prefillConnections(), 50);
                return html;
            }
        };
        if (this._renderReadyResolve) this._renderReadyResolve();
    },

    _SLICE_DEPS: {
        'aggregation': 'aggregationFlows',
        'dynamic': 'dynamicFlows',
        'ext-report': 'externalCalls',
        'vertical': 'externalCalls',
        'vert-report': 'externalCalls'
    },

    async _ensureSliceLoaded(sliceKey) {
        if (!this._loadedSlices || this._loadedSlices[sliceKey]) return;
        if (this._loadingSlice === sliceKey) return;
        this._loadingSlice = sliceKey;
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        try {
            let data;
            const version = JA.app._currentVersion;
            if (sliceKey === 'externalCalls') {
                data = await JA.api.getExternalCalls(jarId, version);
            } else if (sliceKey === 'dynamicFlows') {
                data = await JA.api.getDynamicFlows(jarId, version);
            } else if (sliceKey === 'aggregationFlows') {
                data = await JA.api.getAggregationFlows(jarId, version);
            } else if (sliceKey === 'beans') {
                data = await JA.api.getBeans(jarId, version);
            }
            if (data && Array.isArray(data)) {
                this._mergeSliceData(data, sliceKey);
            }
            this._loadedSlices[sliceKey] = true;
        } catch (e) {
            console.warn('Failed to load slice ' + sliceKey + ':', e);
        } finally {
            this._loadingSlice = null;
        }
    },

    _mergeSliceData(sliceArray, sliceKey) {
        const eps = this._analysis && this._analysis.endpoints;
        if (!eps) return;
        for (const item of sliceArray) {
            const idx = item.endpointIdx;
            if (idx == null || idx < 0 || idx >= eps.length) continue;
            const ep = eps[idx];
            if (sliceKey === 'externalCalls') {
                if (item.externalCalls) ep.externalCalls = item.externalCalls;
                if (item.httpCalls) ep.httpCalls = item.httpCalls;
            } else if (sliceKey === 'dynamicFlows') {
                if (item.dynamicFlows) ep.dynamicFlows = item.dynamicFlows;
            } else if (sliceKey === 'aggregationFlows') {
                if (item.aggregationFlows) ep.aggregationFlows = item.aggregationFlows;
            } else if (sliceKey === 'beans') {
                if (item.beans) ep.beans = item.beans;
            }
        }
        if (sliceKey === 'externalCalls' && this._epReports) {
            for (const r of this._epReports) {
                const ep = r.endpoint;
                if (!ep) continue;
                r.externalCalls = (ep.externalCalls || []).map(e => ({
                    className: e.className || '', simpleClassName: e.simpleClassName || '',
                    methodName: e.methodName || '', stereotype: e.stereotype || 'EXTERNAL',
                    sourceJar: e.sourceJar || 'main', module: e.module || 'External',
                    domain: e.domain || 'External',
                    breadcrumb: Array.isArray(e.breadcrumb) ? e.breadcrumb : []
                }));
                r.httpCalls = (ep.httpCalls || []).map(h => ({
                    className: h.className || '', simpleClassName: h.simpleClassName || '',
                    methodName: h.methodName || '', operationType: h.operationType || 'READ',
                    url: h.url || '', allUrls: h.url ? [h.url] : [],
                    breadcrumb: Array.isArray(h.breadcrumb) ? h.breadcrumb : []
                }));
                r.externalScopeCalls = r.externalCalls.length + r.httpCalls.length;
            }
            this._extReport = this._buildExternalDeps(this._epReports);
        }
    },

    async switchSubTab(tabId) {
        document.querySelectorAll('.sum-tab').forEach(t => t.classList.toggle('active', t.dataset.stab === tabId));
        document.querySelectorAll('.sum-subtab').forEach(tc => tc.style.display = tc.id === 'stab-' + tabId ? '' : 'none');

        // Update URL hash with sub-tab
        if (JA.app.currentJarId) {
            const jarId = encodeURIComponent(JA.app.currentJarId);
            const newHash = '#/jar/' + jarId + '/summary/' + tabId;
            if (window.location.hash !== newHash) {
                history.replaceState(null, '', newHash);
            }
        }

        // Wait for render to finish (batch processing may still be running)
        if (this._renderReady) await this._renderReady;

        const tab = document.getElementById('stab-' + tabId);
        if (!tab || !this._lazyTabs || !this._lazyTabs[tabId]) return;

        // Load on-demand data slice if needed
        const sliceDep = this._SLICE_DEPS[tabId];
        if (sliceDep && this._loadedSlices && !this._loadedSlices[sliceDep]) {
            tab.innerHTML = '<div class="sum-loading-slice"><div class="sum-spinner"></div> Loading data...</div>';
            tab.dataset.rendered = '';
            await this._ensureSliceLoaded(sliceDep);
        }

        // Check if data has changed since this tab was last rendered
        const currentVersion = JA.app._dataVersion || 0;
        const renderedVersion = tab.dataset.renderedVersion;
        const isStale = renderedVersion && Number(renderedVersion) !== currentVersion;

        // Render on first access or when data version has changed (stale cache)
        if (!tab.dataset.rendered || isStale) {
            tab.innerHTML = this._lazyTabs[tabId]();
            tab.dataset.rendered = '1';
            tab.dataset.renderedVersion = String(currentVersion);
        }
    }
});
