/**
 * Summary tables — card-based endpoint report + paginated table renderers.
 * Uses _initPage / _pageRender / _buildFilterBar from summary-helpers.js.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /* ===== Endpoint Report — Card Layout with pagination ===== */
    _renderEndpointTable(epReports, esc) {
        if (!epReports.length) return '<p class="sum-muted" style="padding:20px">No endpoints found</p>';
        let html = '<div class="sum-section">';
        html += '<div class="sum-section-title">Endpoint Report (' + epReports.length + ')</div>';
        html += '<div class="sum-section-desc">Each card represents a REST endpoint with its call tree analysis — collections, operations, dependencies, and size metrics.</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="Size is the method count in the call tree: S (1-5), M (6-20), L (21-50), XL (50+). Large endpoints are harder to migrate and test.">Size = method count in call tree (S/M/L/XL)</span>';
        html += '<span class="sum-tip" title="Performance rating is based on DB operation count: Low (1-5), Medium (6-20), High (20+). High DB operations may cause latency and contention.">Perf = DB operation count (Low/Med/High)</span>';
        html += '<span class="sum-tip" title="Operations inferred from method names: find/get/query=READ, save/insert=WRITE, update/modify=UPDATE, delete/remove=DELETE, aggregate=AGGREGATE.">Operations inferred from method name prefixes</span>';
        html += '<span class="sum-tip" title="Calls to classes in different JAR modules. Each cross-module call becomes a REST API contract after verticalisation.">Cross-module calls need REST API migration</span>';
        html += '</div>';
        // UI-026h (§79): converted to PA.tf — see summary-claude.js's §73 conversion note.
        html += this._buildFilterBarPA('sum-ep', epReports, r => r.primaryDomain);
        // Advanced filters toggle + panel
        html += this._buildEpAdvancedFilters(epReports);
        // Sort dropdown for card layout
        html += '<div class="sum-sort-bar">';
        html += '<span class="sum-sort-label">Sort by:</span>';
        html += `<select class="sum-filter-select" onchange="JA.summary._epCardSort(this.value)">`;
        html += '<option value="">Default</option>';
        html += '<option value="path">Path</option><option value="domain">Domain</option>';
        html += '<option value="colls-desc">Collections (desc)</option><option value="ops-desc">DB Ops (desc)</option>';
        html += '<option value="ext-desc">External (desc)</option><option value="loc-desc">LOC (desc)</option><option value="size">Size</option>';
        html += '</select>';
        html += '</div>';
        html += '<div class="sum-pager sum-pager-top" id="sum-ep-top-pager"></div>';
        html += '<div class="ep-card-list" id="sum-ep-tbody"></div>';
        html += '';
        html += '</div>';

        // No renderDetail: _renderEpCard builds its own eager `ep-card-detail` div, toggled by
        // the shared `toggleDetail('ep', idx)`'s `dataset.lazyEp` branch (reads `this._epReports`
        // directly) — independent of which pagination framework this table uses, so it needs no
        // change here, the same shape `sum-claude`'s conversion (§73) already established.
        this._epFilterState = { rangeFilters: {}, catFilters: {} };
        PA.tf.init('sum-ep', epReports, 25,
            (r, i, esc) => this._renderEpCard(r, i, esc),
            { domainFn: r => r.primaryDomain, extraFilter: item => this._epExtraFilter(item) }
        );
        setTimeout(() => PA.tf.render('sum-ep'), 0);
        return html;
    },

    _epCardSort(val) {
        const s = PA.tf.state('sum-ep');
        if (!s) return;
        const fns = {
            'path': a => a.fullPath || '',
            'domain': a => a.primaryDomain || '',
            'colls-desc': a => -(a.totalCollections || 0),
            'ops-desc': a => -(a.totalDbOperations || 0),
            'ext-desc': a => -(a.externalScopeCalls || 0),
            'loc-desc': a => -(a.totalLoc || 0),
            'size': a => ({ S: 0, M: 1, L: 2, XL: 3 }[a.sizeCategory] || 0)
        };
        if (!val || !fns[val]) {
            s.filtered = [...s.data];
            PA.tf.filter('sum-ep');
            return;
        }
        const fn = fns[val];
        s.filtered.sort((a, b) => {
            const va = fn(a), vb = fn(b);
            if (typeof va === 'number') return va - vb;
            return String(va).localeCompare(String(vb));
        });
        s.page = 0;
        PA.tf.render('sum-ep');
    },

    /* ===== Endpoint Report — Advanced Filters ===== */

    /**
     * UI-026h (§79) — `_pageFilter`'s generic range/category-filter branches, transcribed as one
     * `extraFilter` closure for `sum-ep`. Unlike `sum-vert` (§78), which had table-specific
     * category keys (`vert_complexity`/`vert_type`), `sum-ep`'s categories
     * (`httpMethod`/`sizeCategory`/`performanceImplication`) all go through `_pageFilter`'s generic
     * `item[key]` branch — except `operationTypes`, which is array-membership
     * (`ops.some(op => allowed.has(op))`), not equality, because an endpoint can have more than
     * one operation type.
     */
    _epExtraFilter(item) {
        const st = this._epFilterState;
        for (const [key, range] of Object.entries(st.rangeFilters || {})) {
            const val = item[key] || 0;
            if (range.min !== null && val < range.min) return false;
            if (range.max !== null && val > range.max) return false;
        }
        for (const [key, allowed] of Object.entries(st.catFilters || {})) {
            if (!allowed || !allowed.size) continue;
            if (key === 'operationTypes') {
                const ops = item.operationTypes || [];
                if (!ops.some(op => allowed.has(op))) return false;
            } else {
                const v = item[key];
                if (v != null && !allowed.has(String(v))) return false;
            }
        }
        return true;
    },

    _epRangeFields: [
        { key: 'totalCollections', label: 'Collections' },
        { key: 'totalViews',      label: 'Views' },
        { key: 'totalDbOperations', label: 'DB Ops' },
        { key: 'inScopeCalls',    label: 'Internal' },
        { key: 'externalScopeCalls', label: 'External' },
        { key: 'totalMethods',    label: 'Methods' },
        { key: 'totalLoc',        label: 'LOC' }
    ],

    _epCatFields: [
        { key: 'httpMethod',       label: 'HTTP Method', fn: r => r.httpMethod || '' },
        { key: 'sizeCategory',     label: 'Size',        fn: r => r.sizeCategory || '' },
        { key: 'performanceImplication', label: 'Performance', fn: r => r.performanceImplication || '' }
    ],

    _buildEpAdvancedFilters(epReports) {
        let html = '<div class="ep-adv-toggle-row">';
        html += '<button class="sum-filter-clear" id="ep-adv-toggle" onclick="JA.summary._epAdvToggle()" title="Show/hide range and category filters for all columns">Advanced Filters</button>';
        html += '<span class="ep-adv-badge" id="ep-adv-badge" style="display:none"></span>';
        html += '</div>';
        html += '<div class="ep-adv-panel" id="ep-adv-panel" style="display:none">';

        // Range filters
        html += '<div class="ep-adv-group"><span class="ep-adv-group-label">Range Filters</span>';
        for (const f of this._epRangeFields) {
            let max = 0;
            for (const r of epReports) { const v = r[f.key] || 0; if (v > max) max = v; }
            html += '<div class="ep-adv-range">';
            html += `<label class="ep-adv-label">${f.label}</label>`;
            html += `<input type="number" class="ep-adv-input" id="ep-rf-${f.key}-min" placeholder="Min" min="0" max="${max}" oninput="JA.summary._epAdvApply()">`;
            html += '<span class="ep-adv-sep">&ndash;</span>';
            html += `<input type="number" class="ep-adv-input" id="ep-rf-${f.key}-max" placeholder="Max" min="0" max="${max}" oninput="JA.summary._epAdvApply()">`;
            html += '</div>';
        }
        html += '</div>';

        // Category filters (checkbox pills)
        for (const cf of this._epCatFields) {
            const vals = new Set();
            for (const r of epReports) { const v = cf.fn(r); if (v) vals.add(v); }
            if (vals.size < 2) continue;
            const sorted = [...vals].sort();
            html += `<div class="ep-adv-group"><span class="ep-adv-group-label">${cf.label}</span>`;
            html += '<div class="ep-adv-pills">';
            for (const v of sorted) {
                html += `<label class="ep-adv-pill"><input type="checkbox" value="${JA.utils.escapeHtml(v)}" data-cat="${cf.key}" checked onchange="JA.summary._epAdvApply()"><span>${JA.utils.escapeHtml(v)}</span></label>`;
            }
            html += '</div></div>';
        }

        // Operation type filter
        const allOps = new Set();
        for (const r of epReports) { for (const op of (r.operationTypes || [])) allOps.add(op); }
        if (allOps.size > 0) {
            const sortedOps = [...allOps].sort();
            html += '<div class="ep-adv-group"><span class="ep-adv-group-label">Operations</span>';
            html += '<div class="ep-adv-pills">';
            for (const op of sortedOps) {
                html += `<label class="ep-adv-pill"><input type="checkbox" value="${op}" data-cat="operationTypes" checked onchange="JA.summary._epAdvApply()"><span class="sum-op-badge sum-op-${op.toLowerCase()}" style="font-size:10px">${op}</span></label>`;
            }
            html += '</div></div>';
        }

        html += '<div class="ep-adv-actions">';
        html += '<button class="ep-adv-clear" onclick="JA.summary._epAdvClear()">Clear All Filters</button>';
        html += '</div>';
        html += '</div>';
        return html;
    },

    _epAdvToggle() {
        const panel = document.getElementById('ep-adv-panel');
        if (!panel) return;
        panel.style.display = panel.style.display === 'none' ? '' : 'none';
        const btn = document.getElementById('ep-adv-toggle');
        if (btn) btn.classList.toggle('active', panel.style.display !== 'none');
    },

    _epAdvApply() {
        this._debounce('ep-adv', () => {
            if (!PA.tf.state('sum-ep')) return;
            // Collect range filters
            const ranges = {};
            for (const f of this._epRangeFields) {
                const minEl = document.getElementById('ep-rf-' + f.key + '-min');
                const maxEl = document.getElementById('ep-rf-' + f.key + '-max');
                const min = minEl && minEl.value !== '' ? parseInt(minEl.value) : null;
                const max = maxEl && maxEl.value !== '' ? parseInt(maxEl.value) : null;
                if (min !== null || max !== null) ranges[f.key] = { min, max };
            }
            this._epFilterState.rangeFilters = ranges;

            // Collect category filters (unchecked = exclude)
            const cats = {};
            const panel = document.getElementById('ep-adv-panel');
            if (panel) {
                for (const cf of this._epCatFields) {
                    const cbs = panel.querySelectorAll(`input[data-cat="${cf.key}"]`);
                    if (!cbs.length) continue;
                    const allowed = new Set();
                    cbs.forEach(cb => { if (cb.checked) allowed.add(cb.value); });
                    if (allowed.size < cbs.length) cats[cf.key] = allowed;
                }
                // Operation type filter
                const opCbs = panel.querySelectorAll('input[data-cat="operationTypes"]');
                if (opCbs.length) {
                    const allowed = new Set();
                    opCbs.forEach(cb => { if (cb.checked) allowed.add(cb.value); });
                    if (allowed.size < opCbs.length) cats['operationTypes'] = allowed;
                }
            }
            this._epFilterState.catFilters = cats;

            PA.tf.filter('sum-ep');
            this._epAdvUpdateBadge();
        }, 150);
    },

    _epAdvClear() {
        const panel = document.getElementById('ep-adv-panel');
        if (panel) {
            panel.querySelectorAll('input[type=number]').forEach(el => { el.value = ''; });
            panel.querySelectorAll('input[type=checkbox]').forEach(cb => { cb.checked = true; });
        }
        if (PA.tf.state('sum-ep')) {
            this._epFilterState.rangeFilters = {};
            this._epFilterState.catFilters = {};
            PA.tf.filter('sum-ep');
        }
        this._epAdvUpdateBadge();
    },

    _epAdvUpdateBadge() {
        const badge = document.getElementById('ep-adv-badge');
        if (!badge) return;
        const st = this._epFilterState;
        let count = Object.keys(st.rangeFilters || {}).length + Object.keys(st.catFilters || {}).length;
        if (count > 0) {
            badge.textContent = count + ' active';
            badge.style.display = '';
        } else {
            badge.style.display = 'none';
        }
    },

    /* ===== Collection Analysis — Advanced Filters ===== */

    /**
     * UI-026h (§78) — `sum-vert`'s combined type/verification/range/category filter state, moved
     * out of the local framework's per-table `s._typeFilter`/`s._verifFilter`/`s._rangeFilters`/
     * `s._catFilters` fields into one place `PA.tf`'s generic `extraFilter` hook reads. `PA.tf` has
     * no named concept for any of these four — this is the same shape `_aggFilterOp`/
     * `_dynFilterType` already used (§74/§75) for a single type filter, generalised to four filters
     * ANDed together, exactly matching `_pageFilter`'s own combination order for `sum-vert`.
     */
    _vertFilterState: { typeFilter: '', verifFilter: '', rangeFilters: {}, catFilters: {} },

    /** `_pageFilter`'s own predicate for `sum-vert`, transcribed as one `extraFilter` closure. */
    _vertExtraFilter(item) {
        const st = this._vertFilterState;
        if (st.typeFilter && (item.type || '') !== st.typeFilter) return false;
        const vf = st.verifFilter || '';
        if (vf === 'verified' && item.verification !== 'VERIFIED' && item.verification !== 'CLAUDE_VERIFIED') return false;
        if (vf === 'not_in_db' && item.verification !== 'NOT_IN_DB') return false;
        if (vf === 'other' && (item.verification === 'VERIFIED' || item.verification === 'CLAUDE_VERIFIED' || item.verification === 'NOT_IN_DB')) return false;
        for (const [key, range] of Object.entries(st.rangeFilters || {})) {
            const val = range.fn ? range.fn(item) : (item[key] || 0);
            if (range.min !== null && val < range.min) return false;
            if (range.max !== null && val > range.max) return false;
        }
        for (const [key, allowed] of Object.entries(st.catFilters || {})) {
            if (!allowed || !allowed.size) continue;
            if (key === 'vert_complexity') { if (!allowed.has(item._complexity || 'Low')) return false; }
            else if (key === 'vert_type') { if (!allowed.has(item.type || 'COLLECTION')) return false; }
        }
        return true;
    },

    _vertRangeFields: [
        { key: 'endpoints',  label: 'EPs',    fn: c => c.endpoints ? c.endpoints.size : 0 },
        { key: 'readCount',  label: 'Read',   fn: c => { const oc = c.opCounts || {}; return (oc.READ||0)+(oc.COUNT||0)+(oc.EXISTS||0); } },
        { key: 'writeCount', label: 'Write',  fn: c => { const oc = c.opCounts || {}; return (oc.WRITE||0)+(oc.UPDATE||0)+(oc.DELETE||0)+(oc.CALL||0); } },
        { key: 'aggCount',   label: 'Agg',    fn: c => (c.opCounts || {}).AGGREGATE || 0 },
        { key: 'usageCount', label: 'Usage',  fn: c => c.usageCount || 0 }
    ],

    _buildVertAdvancedFilters(vertReport) {
        let html = '<div class="ep-adv-toggle-row">';
        html += '<button class="sum-filter-clear" id="vert-adv-toggle" onclick="JA.summary._vertAdvToggle()" title="Show/hide advanced range and category filters">Advanced Filters</button>';
        html += '<span class="ep-adv-badge" id="vert-adv-badge" style="display:none"></span>';
        html += '</div>';
        html += '<div class="ep-adv-panel" id="vert-adv-panel" style="display:none">';

        // Range filters
        html += '<div class="ep-adv-group"><span class="ep-adv-group-label">Range Filters</span>';
        for (const f of this._vertRangeFields) {
            let max = 0;
            for (const c of vertReport) { const v = f.fn(c); if (v > max) max = v; }
            html += '<div class="ep-adv-range">';
            html += `<label class="ep-adv-label">${f.label}</label>`;
            html += `<input type="number" class="ep-adv-input" id="vert-rf-${f.key}-min" placeholder="Min" min="0" max="${max}" oninput="JA.summary._vertAdvApply()">`;
            html += '<span class="ep-adv-sep">&ndash;</span>';
            html += `<input type="number" class="ep-adv-input" id="vert-rf-${f.key}-max" placeholder="Max" min="0" max="${max}" oninput="JA.summary._vertAdvApply()">`;
            html += '</div>';
        }
        html += '</div>';

        // Complexity category filter
        const complexities = new Set();
        for (const c of vertReport) { if (c._complexity) complexities.add(c._complexity); }
        if (complexities.size > 1) {
            html += '<div class="ep-adv-group"><span class="ep-adv-group-label">Complexity</span><div class="ep-adv-pills">';
            for (const v of ['Low', 'Medium', 'High'].filter(x => complexities.has(x))) {
                html += `<label class="ep-adv-pill"><input type="checkbox" value="${v}" data-cat="vert_complexity" checked onchange="JA.summary._vertAdvApply()"><span>${v}</span></label>`;
            }
            html += '</div></div>';
        }

        // Type filter
        const types = new Set(vertReport.map(c => c.type || 'COLLECTION').filter(Boolean));
        if (types.size > 1) {
            html += '<div class="ep-adv-group"><span class="ep-adv-group-label">Type</span><div class="ep-adv-pills">';
            for (const v of [...types].sort()) {
                html += `<label class="ep-adv-pill"><input type="checkbox" value="${v}" data-cat="vert_type" checked onchange="JA.summary._vertAdvApply()"><span>${v}</span></label>`;
            }
            html += '</div></div>';
        }

        html += '<div class="ep-adv-actions">';
        html += '<button class="ep-adv-clear" onclick="JA.summary._vertAdvClear()">Clear All Filters</button>';
        html += '</div>';
        html += '</div>';
        return html;
    },

    _vertAdvToggle() {
        const panel = document.getElementById('vert-adv-panel');
        if (!panel) return;
        panel.style.display = panel.style.display === 'none' ? '' : 'none';
        const btn = document.getElementById('vert-adv-toggle');
        if (btn) btn.classList.toggle('active', panel.style.display !== 'none');
    },

    _vertAdvApply() {
        this._debounce('vert-adv', () => {
            if (!PA.tf.state('sum-vert')) return;
            const ranges = {};
            for (const f of this._vertRangeFields) {
                const minEl = document.getElementById('vert-rf-' + f.key + '-min');
                const maxEl = document.getElementById('vert-rf-' + f.key + '-max');
                const min = minEl && minEl.value !== '' ? parseInt(minEl.value) : null;
                const max = maxEl && maxEl.value !== '' ? parseInt(maxEl.value) : null;
                if (min !== null || max !== null) ranges[f.key] = { min, max, fn: f.fn };
            }
            this._vertFilterState.rangeFilters = ranges;

            const cats = {};
            const panel = document.getElementById('vert-adv-panel');
            if (panel) {
                const complexCbs = panel.querySelectorAll('input[data-cat="vert_complexity"]');
                if (complexCbs.length) {
                    const allowed = new Set();
                    complexCbs.forEach(cb => { if (cb.checked) allowed.add(cb.value); });
                    if (allowed.size < complexCbs.length) cats['vert_complexity'] = allowed;
                }
                const typeCbs = panel.querySelectorAll('input[data-cat="vert_type"]');
                if (typeCbs.length) {
                    const allowed = new Set();
                    typeCbs.forEach(cb => { if (cb.checked) allowed.add(cb.value); });
                    if (allowed.size < typeCbs.length) cats['vert_type'] = allowed;
                }
            }
            this._vertFilterState.catFilters = cats;

            PA.tf.filter('sum-vert');
            this._vertAdvUpdateBadge();
        }, 150);
    },

    _vertAdvClear() {
        const panel = document.getElementById('vert-adv-panel');
        if (panel) {
            panel.querySelectorAll('input[type=number]').forEach(el => { el.value = ''; });
            panel.querySelectorAll('input[type=checkbox]').forEach(cb => { cb.checked = true; });
        }
        if (PA.tf.state('sum-vert')) {
            this._vertFilterState.rangeFilters = {};
            this._vertFilterState.catFilters = {};
            PA.tf.filter('sum-vert');
        }
        this._vertAdvUpdateBadge();
    },

    _vertAdvUpdateBadge() {
        const badge = document.getElementById('vert-adv-badge');
        if (!badge) return;
        const st = this._vertFilterState;
        let count = Object.keys(st.rangeFilters || {}).length + Object.keys(st.catFilters || {}).length;
        if (count > 0) { badge.textContent = count + ' active'; badge.style.display = ''; }
        else badge.style.display = 'none';
    },

    _renderEpCard(r, i, esc) {
        const navIdx = JA.nav.ref(r.endpoint.controllerClass, r.endpoint.methodName);
        let html = `<div class="ep-card" data-search="${esc((r.endpointName + ' ' + r.fullPath + ' ' + r.primaryDomain).toLowerCase())}">`;

        const ctrlClass = (r.endpoint.controllerClass || '').replace(/'/g, "\\'");
        const ctrlMethod = (r.endpoint.methodName || '').replace(/'/g, "\\'");
        html += '<div class="ep-card-header">';
        html += `<span class="endpoint-method method-${r.httpMethod}">${esc(r.httpMethod)}</span>`;
        html += `<span class="ep-card-path ep-card-path-link" onclick="JA.summary.showClassCode('${ctrlClass}','${ctrlMethod}');event.stopPropagation()" title="View decompiled code">${esc(r.fullPath)}</span>`;
        html += `<span class="ep-card-name ep-nav-link" onclick="JA.nav.click(${navIdx},event)">${esc(r.endpointName)}</span>`;
        html += '</div>';

        html += '<div class="ep-card-meta">';
        html += `<span class="sum-domain-tag">${esc(r.primaryDomain)}</span>`;
        if (r.procName) {
            const src = r.procNameSources && r.procNameSources[r.procName];
            const srcTip = src ? ' (from ' + (src.simpleClassName || '') + '.' + (src.methodName || '') + ')' : '';
            html += `<span class="sum-procname-tag" title="@LogParameters procedureName${esc(srcTip)}">${esc(r.procName)}</span>`;
        }
        r.modules.forEach(m => { html += `<span class="sum-module-tag">${esc(m)}</span>`; });
        if (r.crossDomainCount > 0) r.domains.filter(d => d !== r.primaryDomain).forEach(d => { html += `<span class="sum-domain-tag">${esc(d)}</span>`; });
        html += '</div>';

        html += '<div class="ep-card-stats">';
        html += `<span class="ep-card-chip ep-chip-click" title="MongoDB collections accessed by this endpoint (click to see details)" onclick="JA.summary._chipDetail(${i},'colls');event.stopPropagation()">${r.totalCollections} <small>Colls</small></span>`;
        html += `<span class="ep-card-chip ep-chip-click" title="MongoDB views (read-only) accessed by this endpoint" onclick="JA.summary._chipDetail(${i},'views');event.stopPropagation()">${r.totalViews} <small>Views</small></span>`;
        html += `<span class="ep-card-chip ep-chip-click" title="Total database operations (reads + writes + aggregations) in the call tree" onclick="JA.summary._chipDetail(${i},'colls');event.stopPropagation()">${r.totalDbOperations} <small>DB Ops</small></span>`;
        html += `<span class="ep-card-chip ep-chip-ok ep-chip-click" title="Method calls within the same JAR module (internal services, repositories)" onclick="JA.summary._chipDetail(${i},'svc');event.stopPropagation()">${r.inScopeCalls} <small>Internal</small></span>`;
        html += `<span class="ep-card-chip ${r.externalScopeCalls > 0 ? 'ep-chip-warn' : ''} ep-chip-click" title="Method calls to external JAR modules — these are cross-module dependencies that may need REST API migration" onclick="JA.summary._chipDetail(${i},'ext');event.stopPropagation()">${r.externalScopeCalls} <small>External</small></span>`;
        // trueCount: httpCalls is capped at 50 by the server, so .length is the cap, not the count.
        if (r.httpCalls.length) html += `<span class="ep-card-chip ep-chip-click" style="border-color:var(--accent);color:var(--accent)" title="Outgoing HTTP/REST calls detected (RestTemplate, WebClient, Feign)" onclick="JA.summary._chipDetail(${i},'http');event.stopPropagation()">${JA.utils.trueCount(r, 'httpCalls')} <small>REST</small></span>`;
        html += `<span class="ep-card-chip ep-chip-click" title="Total methods in the call tree (click to browse with Node Navigator)" onclick="JA.summary.showNodeNav(${i});event.stopPropagation()">${r.totalMethods} <small>Methods</small></span>`;
        html += `<span class="ep-card-chip ep-chip-click" title="Total lines of code across all methods in the call tree" onclick="JA.summary.showCallTrace(${i});event.stopPropagation()">${r.totalLoc || 0} <small>LOC</small></span>`;
        html += '</div>';

        html += '<div class="ep-card-ops">';
        r.operationTypes.forEach(op => { html += `<span class="sum-op-badge sum-op-${op.toLowerCase()} sum-op-clickable" title="${esc(this._opTip[op] || op)}" onclick="JA.summary._showOpMethodsPopup('${op}',${i});event.stopPropagation()">${esc(op)}</span>`; });
        html += `<span class="sum-size-badge sum-size-${r.sizeCategory.toLowerCase()}" title="${esc(this._sizeTip(r.sizeCategory))}">${esc(r.sizeCategory)}</span>`;
        html += `<span class="ep-card-perf" title="${esc(this._perfTip(r.performanceImplication))}">${esc(r.performanceImplication)}</span>`;
        html += '</div>';

        const epSafeName = (r.endpointName || '').replace(/'/g, "\\'");
        html += '<div class="ep-card-actions">';
        html += `<button class="btn-sm btn-explore" onclick="JA.summary.showCallTrace(${i});event.stopPropagation()" title="Visual call tree — interactive diagram of all method calls from this endpoint">Explore</button>`;
        html += `<button class="btn-sm" onclick="JA.summary.showTrace(${i});event.stopPropagation()" title="Text trace — flat list of the call chain with class, method, stereotype, and depth">Trace</button>`;
        html += `<button class="btn-sm btn-nodes" onclick="JA.summary.showNodeNav(${i});event.stopPropagation()" title="Node navigator — browse all methods in the call tree with decompiled source code">Nodes</button>`;
        html += `<button class="btn-sm btn-export" onclick="JA.summary.showExportModal({endpointIdx:${i}});event.stopPropagation()" title="Export this endpoint data to Excel or JSON">Export</button>`;
        html += `<button class="btn-sm" onclick="JA.summary.toggleDetail('ep',${i});event.stopPropagation()" title="Expand inline details — collections by domain, call paths, services, external deps">Details</button>`;
        html += `<button class="btn-sm claude-enrich-btn" onclick="JA.summary._claudeEnrichSingle('${epSafeName}');event.stopPropagation()" title="Run Claude AI analysis — verifies collections, corrects operations, identifies false positives">Claude</button>`;
        html += '</div>';

        html += '</div>';

        html += `<div class="ep-card-detail" id="sum-ep-detail-${i}" style="display:none" data-lazy-ep="${i}">`;
        html += '<div class="sum-detail"></div></div>';
        return html;
    },

    _renderEpDetail(r, i, esc) {
        let html = '';

        // ProcNames with source method
        if (r.allProcNames && r.allProcNames.length) {
            html += '<div class="sum-detail-block"><div class="sum-detail-label">ProcNames (' + r.allProcNames.length + ')</div>';
            for (const pn of r.allProcNames) {
                const src = r.procNameSources && r.procNameSources[pn];
                html += '<span class="sum-procname-tag">' + esc(pn) + '</span>';
                if (src) {
                    const safeClass = (src.simpleClassName || '').replace(/'/g, "\\'");
                    const safeMethod = (src.methodName || '').replace(/'/g, "\\'");
                    html += ' <span class="sum-muted">from</span> ';
                    html += `<span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(safeClass)}','${esc(safeMethod)}')" title="View code">${esc(src.simpleClassName)}.${esc(src.methodName)}</span>`;
                }
                html += ' ';
            }
            html += '</div>';
        }

        // Collections by domain (scrollable)
        if (Object.keys(r.collDomainGroups).length) {
            html += '<div class="sum-detail-block"><div class="sum-detail-label">Collections by Domain (' + Object.keys(r.collections).length + ')</div>';
            html += this._scrollSection('ep-colls-' + i);
            for (const [domain, colls] of Object.entries(r.collDomainGroups)) {
                html += '<div class="sum-detail-domain sum-scroll-item"><span class="sum-domain-tag">' + esc(domain) + '</span> ';
                html += colls.map(c => {
                    const cInfo = r.collections[c];
                    return this._collBadgeRich(c, esc, cInfo);
                }).join(' ') + '</div>';
            }
            html += '</div></div>';
        }

        // Collection call paths — lazy-load call tree for real root→leaf paths
        const collCount = Object.keys(r.collections).length;
        if (collCount) {
            const trailId = 'ep-paths-' + i;
            html += '<div class="sum-detail-block sum-detail-block-full"><div class="sum-detail-label">Collection Call Paths (' + collCount + ')</div>';
            html += `<div id="${trailId}"><span class="sum-muted" style="font-size:11px">Loading...</span></div>`;
            html += '</div>';
            setTimeout(() => this._loadEpCallPaths(trailId, r, i, esc), 50);
        }

        // Write / Read / Aggregate
        for (const [label, arr] of [['Write', r.writeCollections], ['Read', r.readCollections], ['Aggregate', r.aggregateCollections]]) {
            if (arr.length) {
                html += `<div class="sum-detail-block"><div class="sum-detail-label">${label} (${arr.length})</div>`;
                html += arr.map(c => this._collBadgeRich(c, esc, r.collections[c])).join(' ') + '</div>';
            }
        }

        // Views
        if (r.viewsUsed.length) {
            html += '<div class="sum-detail-block"><div class="sum-detail-label">Views (' + r.viewsUsed.length + ')</div>';
            html += r.viewsUsed.map(c => this._collBadgeRich(c, esc, r.collections[c])).join(' ') + '</div>';
        }

        // Services (scrollable)
        if (r.serviceClasses.length) {
            html += '<div class="sum-detail-block"><div class="sum-detail-label">Services (' + r.serviceClasses.length + ')</div>';
            html += this._scrollSection('ep-svc-' + i);
            r.serviceClasses.forEach(s => {
                const safeName = s.replace(/'/g, "\\'");
                html += `<span class="sum-bean-tag sum-bean-clickable sum-scroll-item" onclick="event.stopPropagation();JA.summary.showClassCode('${safeName}')" title="View code">${esc(s)}</span> `;
            });
            html += '</div></div>';
        }

        // HTTP/REST calls (scrollable)
        if (r.httpCalls.length) {
            html += '<div class="sum-detail-block sum-detail-block-full"><div class="sum-detail-label">REST/HTTP Calls ('
                + JA.utils.trueCount(r, 'httpCalls') + ')'
                + JA.utils.capNote(r.httpCalls.length, JA.utils.trueCount(r, 'httpCalls')) + '</div>';
            html += this._scrollSection('ep-http-' + i);
            for (const h of r.httpCalls) {
                const safeClass = (h.simpleClassName || '').replace(/'/g, "\\'");
                const safeMethod = (h.methodName || '').replace(/'/g, "\\'");
                html += '<div class="sum-http-item sum-scroll-item">';
                html += `<span class="sum-http-badge">${esc(h.operationType || 'REST')}</span> `;
                html += `<span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${safeClass}','${safeMethod}')" title="View code">${esc(h.simpleClassName)}.${esc(h.methodName)}</span>`;
                if (h.url) html += ` <span class="sum-http-url">${esc(h.url)}</span>`;
                if (h.breadcrumb?.length) html += '<div class="sum-breadcrumb">' + this._renderBc(h.breadcrumb, esc) + '</div>';
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Cross-module dependencies (scrollable)
        if (r.externalCalls.length) {
            // The heading states the TRUE total while the list below can only show what the
            // server sent (capped at 100). Say so, rather than showing 100 rows under "225".
            html += '<div class="sum-detail-block sum-detail-block-full"><div class="sum-detail-label">Cross-Module Dependencies (' + r.externalScopeCalls + ' external calls)'
                + JA.utils.capNote(r.externalCalls.length, JA.utils.trueCount(r, 'externalCalls'))
                + ' <span class="sum-dep-warn">Direct calls — should be REST API</span></div>';
            html += this._scrollSection('ep-ext-' + i);
            const grouped = {};
            for (const ext of r.externalCalls) { (grouped[ext.sourceJar || 'main'] = grouped[ext.sourceJar || 'main'] || []).push(ext); }
            for (const [jar, exts] of Object.entries(grouped)) {
                html += `<div class="sum-ext-group sum-scroll-item"><div class="sum-ext-header"><span class="sum-module-tag">${esc(this._jarToProject(jar))}</span> <span class="sum-domain-tag">${esc(this._jarToDomain(jar))}</span> <span class="sum-ref-count">${exts.length}</span></div>`;
                for (const e of exts) {
                    const safeClass = (e.simpleClassName || '').replace(/'/g, "\\'");
                    const safeMethod = (e.methodName || '').replace(/'/g, "\\'");
                    html += `<div class="sum-ext-item"><span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${safeClass}','${safeMethod}')" title="View code">${esc(e.simpleClassName)}.${esc(e.methodName)}</span>`;
                    if (e.breadcrumb?.length) html += '<div class="sum-breadcrumb">' + this._renderBc(e.breadcrumb, esc) + '</div>';
                    html += '</div>';
                }
                html += '</div>';
            }
            html += '</div></div>';
        }
        return html;
    },

    /* ===== Collection Analysis (merged with Views) — Paginated ===== */
    _renderVertTable(vertReport, viewsReport, esc) {
        if (!vertReport.length) return '<p class="sum-muted" style="padding:20px">No collections detected</p>';

        // Enrich vertReport with view-specific data + weighted complexity
        const viewMap = {};
        for (const v of (viewsReport || [])) { viewMap[v.viewName] = v; }
        for (const c of vertReport) {
            const vInfo = viewMap[c.name];
            c._complexity = this._calcComplexity(c);
            c._alternative = vInfo ? vInfo.possibleAlternative : '';
        }

        let html = '<div class="sum-section"><div class="sum-section-title">Collection Analysis (' + vertReport.length + ')</div>';
        html += '<div class="sum-section-desc">All MongoDB collections and views detected via bytecode analysis — domain ownership, read/write operations, and verification status.</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="Collections are detected from: (1) Spring Data repository signatures, (2) @Document annotations, (3) MongoTemplate calls, (4) @Query/@Aggregation values, (5) $lookup/$merge stages, (6) String literals matching domain prefixes, (7) Field constants.">7 detection sources: repo signatures, annotations, template calls, query values, pipeline stages, string literals, field constants</span>';
        html += '<span class="sum-tip" title="Collections used by endpoints from multiple domains are shared data. These block independent deployment and must be owned by one domain with API access for others.">Collections used by multiple domains = shared data, blocks splitting</span>';
        html += '<span class="sum-tip" title="VERIFIED = exists in live MongoDB catalog. NOT_IN_DB = not found (possible dead code, test collection, or runtime-created). NO_CATALOG = no catalog connected to verify against.">Verification: VERIFIED (in DB), NOT_IN_DB (possible dead code), NO_CATALOG (not checked)</span>';
        html += '<span class="sum-tip" title="VIEW collections are read-only aggregation pipelines in MongoDB. They depend on base collections and add complexity. Consider replacing with application-level queries during migration.">VIEWs are DB-level aggregations — evaluate if application logic can replace them</span>';
        html += '</div>';

        // View toggle: Table vs Summary
        html += '<div class="sum-type-toggle" style="margin-bottom:6px">';
        html += '<button class="sum-type-btn active" data-vview="table" onclick="JA.summary._vertViewToggle(\'table\')" title="Detailed table with all columns — operations, detected via, references, and expandable detail rows">Table View</button>';
        html += '<button class="sum-type-btn" data-vview="summary" onclick="JA.summary._vertViewToggle(\'summary\')" title="Compact summary with grouped operation counts (Read, Write, Agg), verification status, and dynamic stats that update with filters">Summary View</button>';
        html += '</div>';

        // Summary view (hidden by default)
        html += '<div id="sum-vert-summary" style="display:none">' + this._buildCollSummaryView(vertReport, esc) + '</div>';

        // Table view wrapper
        html += '<div id="sum-vert-table-wrap">';
        // UI-026h (§78): converted to PA.tf — see summary-claude.js's §73 conversion note.
        html += this._buildFilterBarPA('sum-vert', vertReport, c => c.domain);
        html += this._buildVertAdvancedFilters(vertReport);

        // Type toggle
        html += '<div class="sum-type-toggle">';
        html += '<button class="sum-type-btn active" onclick="JA.summary._vertTypeFilter(\'\')" data-type="" title="Show all collections and views">All</button>';
        html += '<button class="sum-type-btn" onclick="JA.summary._vertTypeFilter(\'COLLECTION\')" data-type="COLLECTION" title="Show only regular MongoDB collections (read-write)">Collections</button>';
        html += '<button class="sum-type-btn" onclick="JA.summary._vertTypeFilter(\'VIEW\')" data-type="VIEW" title="Show only MongoDB views (read-only, based on aggregation pipeline)">Views</button>';
        html += '</div>';

        // Verification toggle (updated async after catalog fetch)
        html += '<div class="sum-type-toggle" id="sum-vert-verif-toggle" style="margin-top:4px">';
        html += this._buildVerifToggleHtml(vertReport);
        html += '</div>';

        html += '<div class="sum-summary-inline" id="sum-vert-stats" style="margin-top:6px"></div>';
        html += '<div class="sum-pager sum-pager-top" id="sum-vert-top-pager"></div>';
        html += '<div class="sum-table-wrap"><table class="sum-table" id="sum-vert-table"><thead><tr>';
        html += '<th style="width:40px" title="Row number">#</th>';
        html += `<th class="sum-th-sort" data-sort-col="1" onclick="PA.tf.sort('sum-vert',1)" title="MongoDB collection or view name detected from bytecode analysis">Collection</th>`;
        html += `<th class="sum-th-sort" data-sort-col="2" onclick="PA.tf.sort('sum-vert',2)" title="Whether this is a regular collection (COLL) or a MongoDB view (VIEW)">Type</th>`;
        html += `<th class="sum-th-sort" data-sort-col="3" onclick="PA.tf.sort('sum-vert',3)" title="Business domain this collection belongs to, based on naming prefix rules in domain config">Domain</th>`;
        html += `<th class="sum-th-sort" data-sort-col="4" onclick="PA.tf.sort('sum-vert',4)" style="width:50px" title="Number of REST API endpoints that access this collection">EPs</th>`;
        html += `<th class="sum-th-sort" data-sort-col="5" onclick="PA.tf.sort('sum-vert',5)" style="width:50px" title="Read operation count: READ + COUNT + EXISTS">Read</th>`;
        html += `<th class="sum-th-sort" data-sort-col="6" onclick="PA.tf.sort('sum-vert',6)" style="width:50px" title="Write operation count: WRITE + UPDATE + DELETE">Write</th>`;
        html += `<th class="sum-th-sort" data-sort-col="7" onclick="PA.tf.sort('sum-vert',7)" style="width:50px" title="Aggregation pipeline count">Agg</th>`;
        html += `<th class="sum-th-sort" data-sort-col="8" onclick="PA.tf.sort('sum-vert',8)" title="Read operations: READ, COUNT, EXISTS — data retrieval">Read Ops</th>`;
        html += `<th class="sum-th-sort" data-sort-col="9" onclick="PA.tf.sort('sum-vert',9)" title="Write operations: WRITE, UPDATE, DELETE — data mutation">Write Ops</th>`;
        html += `<th class="sum-th-sort" data-sort-col="10" onclick="PA.tf.sort('sum-vert',10)" title="Aggregation operations: AGGREGATE — pipeline queries">Agg Ops</th>`;
        html += `<th class="sum-th-sort" data-sort-col="11" onclick="PA.tf.sort('sum-vert',11)" style="width:50px" title="Total number of times this collection is referenced across all endpoints">Usage</th>`;
        html += '<th title="How this collection was detected: Repository mapping, @Document annotation, string literal in bytecode, @Query annotation, pipeline stage, MongoTemplate call, etc.">Detected Via</th>';
        html += `<th class="sum-th-sort" data-sort-col="13" onclick="PA.tf.sort('sum-vert',13)" title="Weighted complexity score based on endpoints, operations, cross-domain access, and pipeline usage">Complexity</th>`;
        html += '<th title="Service classes or methods that reference this collection">References</th>';
        html += '<th style="width:50px" title="Actions"></th>';
        html += '</tr></thead><tbody id="sum-vert-tbody"></tbody></table></div>';
        html += '';
        html += '</div>'; // close sum-vert-table-wrap
        html += '</div>'; // close sum-section

        const vertSortKeys = {
            1: { fn: c => c.name || '' },
            2: { fn: c => c.type || '' },
            3: { fn: c => c.domain || '' },
            4: { fn: c => c.endpoints ? c.endpoints.size : 0 },
            5: { fn: c => { const oc = c.opCounts || {}; return (oc.READ || 0) + (oc.COUNT || 0) + (oc.EXISTS || 0); } },
            6: { fn: c => { const oc = c.opCounts || {}; return (oc.WRITE || 0) + (oc.UPDATE || 0) + (oc.DELETE || 0) + (oc.CALL || 0); } },
            7: { fn: c => (c.opCounts ? c.opCounts.AGGREGATE || 0 : 0) },
            8: { fn: c => c.readOps ? c.readOps.size : 0 },
            9: { fn: c => c.writeOps ? c.writeOps.size : 0 },
            10: { fn: c => c.aggOps ? c.aggOps.size : 0 },
            11: { fn: c => c.usageCount || 0 },
            13: { fn: c => ({ Low: 0, Medium: 1, High: 2 }[c._complexity] || 0) }
        };
        // typeFilter/verifFilter/rangeFilters/catFilters had no PA.tf counterpart — combined into
        // one extraFilter closure, _vertExtraFilter, reading _vertFilterState (reset per render so
        // a re-render of this tab doesn't inherit a previous render's filter state).
        this._vertFilterState = { typeFilter: '', verifFilter: '', rangeFilters: {}, catFilters: {} };
        PA.tf.init('sum-vert', vertReport, 25,
            (c, i, esc) => this._renderVertRow(c, i, esc),
            {
                domainFn: c => c.domain,
                renderDetail: (c, i, esc) => this._renderVertDetail(c, i, esc),
                sortKeys: vertSortKeys,
                extraFilter: item => this._vertExtraFilter(item),
                onFilter: () => this._updateVertStats()
            }
        );

        // Summary view: paginated table with granular operation count columns + Verification
        const vsVerifLabel = c => c.verification === 'VERIFIED' || c.verification === 'CLAUDE_VERIFIED' ? 'Verified'
            : c.verification === 'NOT_IN_DB' ? 'Ambiguous' : 'Unknown';
        const vsSortKeys = {
            1: { fn: c => c.name || '' },
            2: { fn: c => c.domain || '' },
            3: { fn: c => (c.opCounts ? (c.opCounts.READ || 0) + (c.opCounts.COUNT || 0) + (c.opCounts.EXISTS || 0) : 0) },
            4: { fn: c => (c.opCounts ? (c.opCounts.WRITE || 0) + (c.opCounts.UPDATE || 0) + (c.opCounts.DELETE || 0) + (c.opCounts.CALL || 0) : 0) },
            5: { fn: c => (c.opCounts ? c.opCounts.AGGREGATE || 0 : 0) },
            6: { fn: c => c.endpoints ? c.endpoints.size : 0 },
            7: { fn: c => c.usageCount || 0 },
            8: { fn: c => ({ Low: 0, Medium: 1, High: 2 }[c._complexity] || 0) },
            9: { fn: c => vsVerifLabel(c) }
        };
        PA.tf.init('sum-vs', vertReport, 25,
            (c, i, esc) => this._renderSummaryRow(c, i, esc),
            {
                domainFn: c => c.domain,
                renderDetail: (c, i, esc) => this._renderVertDetail(c, i, esc),
                sortKeys: vsSortKeys,
                onFilter: () => this._updateSummaryStats()
            }
        );

        setTimeout(() => {
            PA.tf.render('sum-vert');
            this._updateVertStats();
            PA.tf.initColFilters('sum-vert', {
                2: { label: 'Type', valueFn: c => c.type || '' },
                3: { label: 'Domain', valueFn: c => c.domain || '' },
                8: { label: 'Read Ops', valueFn: c => c.readOps ? [...c.readOps] : ['—'] },
                9: { label: 'Write Ops', valueFn: c => c.writeOps ? [...c.writeOps] : ['—'] },
                10: { label: 'Agg Ops', valueFn: c => c.aggOps ? [...c.aggOps] : ['—'] },
                13: { label: 'Complexity', valueFn: c => c._complexity || 'Low' }
            });
            // Summary view table + all column filters
            PA.tf.render('sum-vs');
            this._updateSummaryStats();
            PA.tf.initColFilters('sum-vs', {
                2: { label: 'Domain', valueFn: c => c.domain || '' },
                3: { label: 'Read', valueFn: c => { const oc = c.opCounts || {}; return (oc.READ || 0) + (oc.COUNT || 0) + (oc.EXISTS || 0); } },
                4: { label: 'Write', valueFn: c => { const oc = c.opCounts || {}; return (oc.WRITE || 0) + (oc.UPDATE || 0) + (oc.DELETE || 0) + (oc.CALL || 0); } },
                5: { label: 'Agg', valueFn: c => (c.opCounts || {}).AGGREGATE || 0 },
                8: { label: 'Complexity', valueFn: c => c._complexity || 'Low' },
                9: { label: 'Status', valueFn: c => c.verification === 'VERIFIED' || c.verification === 'CLAUDE_VERIFIED' ? 'Verified' : c.verification === 'NOT_IN_DB' ? 'Ambiguous' : 'Unknown' }
            });
            // Fetch catalog and apply verification to collections missing it
            this._applyCatalogVerification(vertReport);
        }, 0);
        return html;
    },

    /** Build verification toggle button HTML from current vertReport data */
    _buildVerifToggleHtml(vertReport) {
        const vc = { all: vertReport.length, verified: 0, notInDb: 0, other: 0 };
        for (const c of vertReport) {
            if (c.verification === 'VERIFIED' || c.verification === 'CLAUDE_VERIFIED') vc.verified++;
            else if (c.verification === 'NOT_IN_DB') vc.notInDb++;
            else vc.other++;
        }
        let h = `<button class="sum-type-btn active" onclick="JA.summary._vertVerifFilter('')" data-verif="">All (${vc.all})</button>`;
        h += `<button class="sum-type-btn" onclick="JA.summary._vertVerifFilter('verified')" data-verif="verified" style="color:var(--green)">Verified (${vc.verified})</button>`;
        h += `<button class="sum-type-btn" onclick="JA.summary._vertVerifFilter('not_in_db')" data-verif="not_in_db" style="color:var(--orange)">Ambiguous (${vc.notInDb})</button>`;
        if (vc.other > 0) h += `<button class="sum-type-btn" onclick="JA.summary._vertVerifFilter('other')" data-verif="other">Unverified (${vc.other})</button>`;
        return h;
    },

    /** Fetch MongoDB catalog and apply verification to collections that lack it.
     *  Runs after initial render AND after Claude/correction data loads. */
    _applyCatalogVerification(vertReport) {
        // Skip if all collections already have verification values
        if (!vertReport.some(c => c.verification == null)) return;

        const jarId = JA.app.currentJarId;
        if (!jarId) return;

        JA.api.getCatalog(jarId).then(catalog => {
            if (!catalog || catalog.status !== 'available') {
                // No catalog — show hint
                const toggle = document.getElementById('sum-vert-verif-toggle');
                if (toggle && !toggle.querySelector('.sum-verif-hint')) {
                    toggle.insertAdjacentHTML('beforeend',
                        '<span class="sum-muted sum-verif-hint" style="font-size:11px;margin-left:8px">No MongoDB catalog — connect DB to enable verification</span>');
                }
                return;
            }

            // Build case-insensitive lookup set from catalog
            const catalogNames = new Set();
            for (const name of (catalog.collections || [])) catalogNames.add(name.toLowerCase());
            for (const name of (catalog.views || [])) catalogNames.add(name.toLowerCase());

            // Apply verification to ALL collections: override null AND re-verify existing values
            // This catches: frontend supplemental extraction (null), stale NO_CATALOG values,
            // and collections that changed between analysis runs
            let changed = false;
            for (const c of vertReport) {
                if (c.verification == null || c.verification === 'NO_CATALOG') {
                    const newVerif = catalogNames.has(c.name.toLowerCase()) ? 'VERIFIED' : 'NOT_IN_DB';
                    if (c.verification !== newVerif) {
                        c.verification = newVerif;
                        changed = true;
                    }
                }
            }
            if (!changed) return;

            // Also update the per-endpoint collection verification in epReports
            // so that detail views and exports reflect the catalog verification
            if (this._epReports) {
                for (const r of this._epReports) {
                    for (const [name, coll] of Object.entries(r.collections || {})) {
                        if (coll.verification == null || coll.verification === 'NO_CATALOG') {
                            coll.verification = catalogNames.has(name.toLowerCase()) ? 'VERIFIED' : 'NOT_IN_DB';
                        }
                    }
                }
            }

            // Rebuild toggle buttons with updated counts
            const toggle = document.getElementById('sum-vert-verif-toggle');
            if (toggle) {
                // Preserve current active verif filter
                const active = toggle.querySelector('[data-verif].sum-type-btn.active');
                const activeVerif = active ? active.dataset.verif : '';
                toggle.innerHTML = this._buildVerifToggleHtml(vertReport);
                // Restore active state
                toggle.querySelectorAll('[data-verif].sum-type-btn').forEach(b =>
                    b.classList.toggle('active', b.dataset.verif === activeVerif));
            }

            // Re-apply current filter to update BOTH table and summary views
            if (PA.tf.state('sum-vert')) PA.tf.filter('sum-vert');
            if (PA.tf.state('sum-vs')) {
                PA.tf.filter('sum-vs');
                this._updateSummaryStats();
            }
        }).catch(() => {});
    },

    _vertTypeFilter(type) {
        document.querySelectorAll('[data-type].sum-type-btn').forEach(b => b.classList.toggle('active', b.dataset.type === type));
        if (PA.tf.state('sum-vert')) { this._vertFilterState.typeFilter = type; PA.tf.filter('sum-vert'); }
    },

    _vertVerifFilter(verif) {
        document.querySelectorAll('[data-verif].sum-type-btn').forEach(b => b.classList.toggle('active', b.dataset.verif === verif));
        if (PA.tf.state('sum-vert')) { this._vertFilterState.verifFilter = verif; PA.tf.filter('sum-vert'); }
    },

    /** Toggle between Table view and Summary view */
    _vertViewToggle(view) {
        document.querySelectorAll('[data-vview].sum-type-btn').forEach(b => b.classList.toggle('active', b.dataset.vview === view));
        const tableWrap = document.getElementById('sum-vert-table-wrap');
        const summaryWrap = document.getElementById('sum-vert-summary');
        if (tableWrap) tableWrap.style.display = view === 'table' ? '' : 'none';
        if (summaryWrap) {
            summaryWrap.style.display = view === 'summary' ? '' : 'none';
            if (view === 'summary' && PA.tf.state('sum-vs')) {
                PA.tf.render('sum-vs');
                this._updateSummaryStats();
            }
        }
    },

    /** Build Collection Summary view — compact inline stats + paginated granular operation table */
    _buildCollSummaryView(vertReport, esc) {
        let html = '<div class="sum-coll-summary">';

        // Dynamic inline stat line (updated on filter changes)
        html += '<div class="sum-summary-inline" id="sum-vs-stats"></div>';

        // Filter bar + paginated table with granular operation columns + Verification
        // UI-026h (§77): converted to PA.tf — see summary-claude.js's §73 conversion note.
        html += this._buildFilterBarPA('sum-vs', vertReport, c => c.domain);
        html += '<div class="sum-pager sum-pager-top" id="sum-vs-top-pager"></div>';
        html += '<div class="sum-table-wrap"><table class="sum-table" id="sum-vs-table"><thead><tr>';
        html += '<th style="width:30px">#</th>';
        html += `<th class="sum-th-sort" data-sort-col="1" onclick="PA.tf.sort('sum-vs',1)">Collection</th>`;
        html += `<th class="sum-th-sort" data-sort-col="2" onclick="PA.tf.sort('sum-vs',2)">Domain</th>`;
        html += `<th class="sum-th-sort" data-sort-col="3" onclick="PA.tf.sort('sum-vs',3)" title="Read operations (find, get, query, count, exists)" style="width:50px">Read</th>`;
        html += `<th class="sum-th-sort" data-sort-col="4" onclick="PA.tf.sort('sum-vs',4)" title="Write operations (save, insert, update, delete)" style="width:50px">Write</th>`;
        html += `<th class="sum-th-sort" data-sort-col="5" onclick="PA.tf.sort('sum-vs',5)" title="Aggregation pipeline operations" style="width:50px">Agg</th>`;
        html += `<th class="sum-th-sort" data-sort-col="6" onclick="PA.tf.sort('sum-vs',6)" style="width:40px" title="Endpoint count">EPs</th>`;
        html += `<th class="sum-th-sort" data-sort-col="7" onclick="PA.tf.sort('sum-vs',7)" style="width:50px" title="Total usage references">Usage</th>`;
        html += `<th class="sum-th-sort" data-sort-col="8" onclick="PA.tf.sort('sum-vs',8)" title="Weighted complexity score">Complexity</th>`;
        html += `<th class="sum-th-sort" data-sort-col="9" onclick="PA.tf.sort('sum-vs',9)" title="MongoDB catalog verification status">Status</th>`;
        html += '</tr></thead><tbody id="sum-vs-tbody"></tbody></table></div>';
        html += '';
        html += '</div>';
        return html;
    },

    /** Update dynamic stats line from current filtered data */
    _updateSummaryStats() {
        const el = document.getElementById('sum-vs-stats');
        if (!el) return;
        const s = PA.tf.state('sum-vs');
        const data = s ? s.filtered : [];
        const total = data.length;
        const readColls = data.filter(c => c.readOps && c.readOps.size > 0).length;
        const writeColls = data.filter(c => c.writeOps && c.writeOps.size > 0).length;
        const both = data.filter(c => c.readOps && c.readOps.size > 0 && c.writeOps && c.writeOps.size > 0).length;
        const readOnly = readColls - both;
        const writeOnly = writeColls - both;
        const high = data.filter(c => c._complexity === 'High').length;
        const med = data.filter(c => c._complexity === 'Medium').length;
        const verified = data.filter(c => c.verification === 'VERIFIED' || c.verification === 'CLAUDE_VERIFIED').length;
        const notInDb = data.filter(c => c.verification === 'NOT_IN_DB').length;

        let h = `<span class="sum-si-item" title="Total collections detected via bytecode analysis (updates with active filters)"><b>${total}</b> Total</span>`;
        h += `<span class="sum-si-item sum-si-read" title="Collections with only read operations (find, get, query) and no write operations"><b>${readOnly}</b> Read Only</span>`;
        h += `<span class="sum-si-item sum-si-write" title="Collections with only write operations (save, update, delete) and no read operations"><b>${writeOnly}</b> Write Only</span>`;
        h += `<span class="sum-si-item sum-si-both" title="Collections accessed by both read and write operations — higher migration complexity"><b>${both}</b> Read+Write</span>`;
        if (high) h += `<span class="sum-si-item sum-si-high" title="Collections scored High complexity (score > 10) — many endpoints, cross-domain, aggregation pipelines"><b>${high}</b> High</span>`;
        if (med) h += `<span class="sum-si-item sum-si-med" title="Collections scored Medium complexity (score 4-10) — multiple endpoints or mixed operations"><b>${med}</b> Medium</span>`;
        if (verified) h += `<span class="sum-si-item" style="border-left:3px solid var(--green)" title="Collections confirmed to exist in the MongoDB catalog"><b>${verified}</b> Verified</span>`;
        if (notInDb) h += `<span class="sum-si-item" style="border-left:3px solid var(--orange)" title="Collections not found in MongoDB catalog — may be dynamic, renamed, or from another environment"><b>${notInDb}</b> Ambiguous</span>`;
        el.innerHTML = h;
    },

    /** Update Table View stats bar — totals across visible (filtered) rows */
    _updateVertStats() {
        const el = document.getElementById('sum-vert-stats');
        if (!el) return;
        const s = PA.tf.state('sum-vert');
        const data = s ? s.filtered : [];
        const total = data.length;
        let totalRead = 0, totalWrite = 0, totalAgg = 0, totalEps = new Set();
        for (const c of data) {
            const oc = c.opCounts || {};
            totalRead += (oc.READ || 0) + (oc.COUNT || 0) + (oc.EXISTS || 0);
            totalWrite += (oc.WRITE || 0) + (oc.UPDATE || 0) + (oc.DELETE || 0) + (oc.CALL || 0);
            totalAgg += oc.AGGREGATE || 0;
            if (c.endpoints) for (const ep of c.endpoints) totalEps.add(ep);
        }
        let h = `<span class="sum-si-item"><b>${total}</b> Collections</span>`;
        h += `<span class="sum-si-item"><b>${totalEps.size}</b> Endpoints</span>`;
        h += `<span class="sum-si-item sum-si-read"><b>${totalRead}</b> Read</span>`;
        h += `<span class="sum-si-item sum-si-write"><b>${totalWrite}</b> Write</span>`;
        h += `<span class="sum-si-item" style="border-left:3px solid var(--accent)"><b>${totalAgg}</b> Agg</span>`;
        h += `<span class="sum-si-item"><b>${totalRead + totalWrite + totalAgg}</b> Total Ops</span>`;
        el.innerHTML = h;
    },

    /** Render a single row in the Summary View table */
    _renderSummaryRow(c, i, esc) {
        const oc = c.opCounts || {};
        const read = (oc.READ || 0) + (oc.COUNT || 0) + (oc.EXISTS || 0);
        const write = (oc.WRITE || 0) + (oc.UPDATE || 0) + (oc.DELETE || 0) + (oc.CALL || 0);
        const agg = oc.AGGREGATE || 0;

        const verifLabel = c.verification === 'VERIFIED' || c.verification === 'CLAUDE_VERIFIED' ? 'Verified'
            : c.verification === 'NOT_IN_DB' ? 'Ambiguous' : 'Unknown';
        const verifCls = verifLabel === 'Verified' ? 'sum-verif-ok' : verifLabel === 'Ambiguous' ? 'sum-verif-warn' : '';

        let html = `<tr onclick="PA.tf.toggleDetail('sum-vs',${i})" style="cursor:pointer">`;
        html += `<td>${i + 1}</td>`;
        html += `<td>${this._collBadgeRich(c.name, esc, c)}</td>`;
        html += `<td><span class="sum-domain-tag">${esc(c.domain)}</span></td>`;
        html += `<td class="sum-center" title="${read} read operations (find, count, exists)">${read ? '<span class="sum-op-cnt sum-op-read">' + read + '</span>' : '0'}</td>`;
        html += `<td class="sum-center" title="${write} write operations (save, update, delete)">${write ? '<span class="sum-op-cnt sum-op-write">' + write + '</span>' : '0'}</td>`;
        html += `<td class="sum-center" title="${agg} aggregation operations">${agg ? '<span class="sum-op-cnt sum-op-aggregate">' + agg + '</span>' : '0'}</td>`;
        html += `<td class="sum-center" title="${c.endpoints.size} REST endpoint(s) access this collection">${c.endpoints.size}</td>`;
        html += `<td class="sum-center" title="${c.usageCount} total references across all endpoints">${c.usageCount}</td>`;
        html += `<td><span class="sum-complexity sum-complexity-${(c._complexity || 'low').toLowerCase()}" title="${esc(this._complexityTip(c._complexity || 'Low', c._complexityScore))}">${c._complexity}${c._complexityScore != null ? ' (' + c._complexityScore + ')' : ''}</span></td>`;
        html += `<td><span class="sum-verif ${verifCls}" title="${esc(this._verifTip(verifLabel))}">${verifLabel}</span></td>`;
        html += '</tr>';
        return html;
    },

    _renderVertRow(c, i, esc) {
        const isView = c.type === 'VIEW';
        const safeColl = (c.name || '').replace(/'/g, "\\'");
        const refs = [...c.sources].slice(0, 3).map(s => {
            const safe = s.replace(/'/g, "\\'");
            const meth = (c.sourceMethodMap && c.sourceMethodMap[s] || '').replace(/'/g, "\\'");
            return `<span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(safe)}','${esc(meth)}')">${esc(s)}</span>`;
        }).join(', ');
        const detected = c.detectedVia && c.detectedVia.size > 0
            ? [...c.detectedVia].map(d => `<span class="sum-detected-tag sum-detected-${d.toLowerCase().replace(/_/g, '-')}" title="${esc(this._detectedTip(d))}">${esc(this._detectedLabel(d))}</span>`).join('<br>')
            : '<span class="sum-muted">—</span>';

        // Read ops: READ, COUNT, EXISTS
        const readOps = c.readOps && c.readOps.size > 0
            ? [...c.readOps].map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()} sum-op-clickable" title="${esc(this._opTip[op] || op)}" onclick="JA.summary._showCollOpPopup('${safeColl}','${op}');event.stopPropagation()">${esc(op)}</span>`).join(' ')
            : '<span class="sum-muted">—</span>';
        // Write ops: WRITE, UPDATE, DELETE
        const writeOps = c.writeOps && c.writeOps.size > 0
            ? [...c.writeOps].map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()} sum-op-clickable" title="${esc(this._opTip[op] || op)}" onclick="JA.summary._showCollOpPopup('${safeColl}','${op}');event.stopPropagation()">${esc(op)}</span>`).join(' ')
            : '<span class="sum-muted">—</span>';
        // Agg ops: AGGREGATE
        const aggOps = c.aggOps && c.aggOps.size > 0
            ? [...c.aggOps].map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()} sum-op-clickable" title="${esc(this._opTip[op] || op)}" onclick="JA.summary._showCollOpPopup('${safeColl}','${op}');event.stopPropagation()">${esc(op)}</span>`).join(' ')
            : '<span class="sum-muted">—</span>';

        let html = `<tr onclick="PA.tf.toggleDetail('sum-vert',${i})" style="cursor:pointer">`;
        html += `<td>${i + 1}</td>`;
        html += `<td>${this._collBadgeRich(c.name, esc, c)}</td>`;
        html += `<td><span class="sum-type-badge ${isView ? 'sum-type-view' : 'sum-type-coll'}" title="${esc(this._typeTip(c.type))}">${isView ? 'VIEW' : 'COLL'}</span></td>`;
        const domCls = c.domain === 'Unclassified' ? 'sum-domain-tag sum-domain-unclassified' : 'sum-domain-tag';
        html += `<td><span class="${domCls}">${esc(c.domain)}</span></td>`;
        html += `<td class="sum-center">${c.endpoints.size}</td>`;
        const oc = c.opCounts || {};
        const readCnt = (oc.READ || 0) + (oc.COUNT || 0) + (oc.EXISTS || 0);
        const writeCnt = (oc.WRITE || 0) + (oc.UPDATE || 0) + (oc.DELETE || 0) + (oc.CALL || 0);
        const aggCnt = oc.AGGREGATE || 0;
        html += `<td class="sum-center">${readCnt ? '<span class="sum-op-cnt sum-op-read">' + readCnt + '</span>' : '0'}</td>`;
        html += `<td class="sum-center">${writeCnt ? '<span class="sum-op-cnt sum-op-write">' + writeCnt + '</span>' : '0'}</td>`;
        html += `<td class="sum-center">${aggCnt ? '<span class="sum-op-cnt sum-op-aggregate">' + aggCnt + '</span>' : '0'}</td>`;
        html += `<td>${readOps}</td>`;
        html += `<td>${writeOps}</td>`;
        html += `<td>${aggOps}</td>`;
        html += `<td class="sum-center">${c.usageCount}</td>`;
        html += `<td>${detected}</td>`;
        html += `<td><span class="sum-complexity sum-complexity-${(c._complexity || 'low').toLowerCase()}" title="${esc(this._complexityTip(c._complexity || 'Low', c._complexityScore))}">${c._complexity || 'Low'}${c._complexityScore != null ? ' (' + c._complexityScore + ')' : ''}</span></td>`;
        html += `<td class="sum-ref-cell">${refs}</td>`;
        html += `<td class="sum-center"><button class="btn-sm btn-export" style="font-size:10px;padding:1px 5px" onclick="JA.summary.showExportModal({collectionName:'${esc(c.name).replace(/'/g, "\\'")}'});event.stopPropagation()" title="Export this collection">Export</button></td>`;
        html += `</tr>`;
        return html;
    },

    _detectedLabel(code) {
        const labels = {
            'REPOSITORY_MAPPING': 'Repository',
            'DOCUMENT_ANNOTATION': '@Document',
            'STRING_LITERAL': 'String Literal',
            'FIELD_CONSTANT': 'Field Constant',
            'QUERY_ANNOTATION': '@Query',
            'PIPELINE_ANNOTATION': 'Pipeline',
            'BULK_WRITE_COLLECTOR': 'BulkWrite',
            'MONGO_TEMPLATE': 'MongoTemplate',
            'NATIVE_DRIVER': 'Native Driver',
            'NATIVE_COMMAND': 'runCommand',
            'REACTIVE_MONGO_TEMPLATE': 'Reactive',
            'GRIDFS': 'GridFS',
            'CHANGE_STREAM': 'ChangeStream',
            'AGGREGATION_API': 'Aggregation API',
            'PIPELINE_RUNTIME': 'Pipeline Runtime',
            'TEMPLATE_AGGREGATE': 'Template Agg',
            'PIPELINE_FIELD_REF': 'Field Ref',
            'CLAUDE': 'Claude'
        };
        return labels[code] || code;
    },

    /**
     * Bare inner content — see `summary-aggregation.js:_renderAggDetail`'s doc for why a
     * `<tr>`/`<td>` wrapper would nest invalidly inside `PA.tf.toggleDetail`'s own `<td>`. Now
     * shared, unmodified, by both `sum-vert` and `sum-vs` (§78) — both are on `PA.tf` as of this
     * section, so the `_renderVertDetailPA` wrapper-stripping shim §77 needed is gone.
     */
    _renderVertDetail(c, i, esc) {
        let html = '<div class="sum-detail">';

        // Endpoints (scrollable, clickable)
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Endpoints (' + c.endpoints.size + ')</div>';
        html += this._scrollSection('vert-eps-' + i);
        [...c.endpoints].forEach(ep => {
            const parts = ep.split('.');
            const epCls = (parts[0] || '').replace(/'/g, "\\'");
            const epMeth = (parts.length > 1 ? parts.slice(1).join('.') : '').replace(/'/g, "\\'");
            html += `<span class="sum-ext-method-link sum-scroll-item" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(epCls)}','${esc(epMeth)}')" title="View endpoint code">${esc(ep)}</span> `;
        });
        html += '</div></div>';

        // Complexity breakdown
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Complexity: ' + esc(c._complexity || 'Low') + (c._complexityScore != null ? ' (score ' + c._complexityScore + ')' : '') + '</div>';
        html += '<div class="sum-complexity-breakdown">';
        const hasPipeline = c.detectedVia && (c.detectedVia.has('PIPELINE_RUNTIME') || c.detectedVia.has('AGGREGATION_API') || c.detectedVia.has('TEMPLATE_AGGREGATE'));
        const factors = [
            { label: 'Endpoints', value: c.endpoints ? c.endpoints.size : 0, weight: 2 },
            { label: 'Read Ops', value: c.readOps ? c.readOps.size : 0, weight: 1 },
            { label: 'Write Ops', value: c.writeOps ? c.writeOps.size : 0, weight: 2 },
            { label: 'Aggregate', value: c.operations && c.operations.has('AGGREGATE') ? 1 : 0, weight: 3 },
            { label: 'Cross-Domain', value: c.domainSet && c.domainSet.size > 1 ? c.domainSet.size - 1 : 0, weight: 3 },
            { label: 'Pipeline', value: hasPipeline ? 1 : 0, weight: 2 },
            { label: 'Usage', value: c.usageCount || 0, weight: 0.5 }
        ];
        for (const f of factors) {
            var contrib = Math.round(f.value * f.weight * 10) / 10;
            html += '<span class="sum-cf-item' + (contrib > 0 ? ' sum-cf-active' : '') + '">' + esc(f.label) + ': ' + f.value + ' &times; ' + f.weight + ' = ' + contrib + '</span>';
        }
        html += '</div></div>';

        // References (scrollable)
        html += '<div class="sum-detail-block"><div class="sum-detail-label">References (' + c.sources.size + ')</div>';
        html += this._scrollSection('vert-refs-' + i);
        [...c.sources].forEach(s => {
            const safe = s.replace(/'/g, "\\'");
            const meth = (c.sourceMethodMap && c.sourceMethodMap[s] || '').replace(/'/g, "\\'");
            html += `<span class="sum-ext-method-link sum-scroll-item" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(safe)}','${esc(meth)}')">${esc(s)}</span> `;
        });
        html += '</div></div>';

        if (c._alternative) {
            html += '<div class="sum-detail-block"><div class="sum-detail-label">Alternative</div><span class="sum-muted">' + esc(c._alternative) + '</span></div>';
        }

        // Call Paths — lazy-load real root→leaf paths from call trees
        const pathsId = 'vert-paths-' + i;
        html += '<div class="sum-detail-block sum-detail-block-full"><div class="sum-detail-label">Call Paths</div>';
        html += `<div id="${pathsId}"><span class="sum-muted" style="font-size:11px">Loading...</span></div>`;
        html += '</div>';
        setTimeout(() => this._loadVertCallPaths(pathsId, c, esc), 50);

        html += '</div>';
        return html;
    },

    /* ===== Distributed Transactions — Paginated ===== */
    _renderDistTable(distReport, esc) {
        if (!distReport.length) return '<p class="sum-muted" style="padding:20px">No endpoints analyzed</p>';
        const required = distReport.filter(d => d.transactionRequirement.startsWith('REQUIRED'));
        let html = '<div class="sum-section"><div class="sum-section-title">Distributed Transactions (' + required.length + ' of ' + distReport.length + ' require transactions)</div>';
        html += '<div class="sum-section-desc">Endpoints that write to multiple MongoDB collections — potential transaction boundaries after microservice split.</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="REQUIRED = writes to collections in 2+ different domains. In a monolith this works atomically. After splitting into services, each domain is a separate DB — cross-domain writes need saga patterns.">REQUIRED: cross-domain writes need saga/compensation after splitting</span>';
        html += '<span class="sum-tip" title="SINGLE-DOMAIN = writes only within one domain boundary. Safe — stays atomic within the owning service after verticalisation.">SINGLE-DOMAIN: safe, stays atomic within one service</span>';
        html += '<span class="sum-tip" title="Prioritize endpoints that write to 3+ domains — these have the highest risk of partial failure and data inconsistency after migration.">3+ domain writes = highest migration risk, may need architectural redesign</span>';
        html += '</div>';
        html += this._buildFilterBarPA('sum-dist', distReport, d => d.primaryDomain);
        html += '<div class="sum-pager sum-pager-top" id="sum-dist-top-pager"></div>';
        html += '<div class="sum-table-wrap"><table class="sum-table" id="sum-dist-table"><thead><tr>';
        html += '<th style="width:40px" title="Row number">#</th>';
        html += `<th class="sum-th-sort" data-sort-col="1" onclick="PA.tf.sort('sum-dist',1)" title="REST API endpoint (Controller.method) that performs multi-collection operations">Endpoint</th>`;
        html += `<th class="sum-th-sort" data-sort-col="2" onclick="PA.tf.sort('sum-dist',2)" title="Primary business domain of this endpoint based on its controller location">Domain</th>`;
        html += `<th class="sum-th-sort" data-sort-col="3" onclick="PA.tf.sort('sum-dist',3)" title="Whether this endpoint needs @Transactional: REQUIRED if it writes to multiple collections, NOT_REQUIRED otherwise">Transaction</th>`;
        html += '<th title="Business domains where this endpoint performs write/update operations — cross-domain writes need careful transaction handling">Write Domains</th><th title="Business domains where this endpoint performs read operations">Read Domains</th>';
        html += `<th class="sum-th-sort" data-sort-col="6" onclick="PA.tf.sort('sum-dist',6)" style="width:50px" title="Total number of distinct MongoDB collections accessed by this endpoint">Colls</th>`;
        html += `<th class="sum-th-sort" data-sort-col="7" onclick="PA.tf.sort('sum-dist',7)" style="width:60px" title="Total lines of code across all methods in this endpoint's call tree">LOC</th>`;
        html += `<th class="sum-th-sort" data-sort-col="8" onclick="PA.tf.sort('sum-dist',8)" title="Estimated performance impact based on number of collections and operation complexity">Perf</th>`;
        html += '</tr></thead><tbody id="sum-dist-tbody"></tbody></table></div>';
        html += '</div>';

        const distSortKeys = {
            1: { fn: d => d.endpointName || '' },
            2: { fn: d => d.primaryDomain || '' },
            3: { fn: d => d.transactionRequirement || '' },
            6: { fn: d => d.totalCollections || 0 },
            7: { fn: d => d.totalLoc || 0 },
            8: { fn: d => d.performanceImplication || '' }
        };
        // UI-026h (§76): converted to PA.tf — see summary-claude.js's §73 conversion note.
        PA.tf.init('sum-dist', distReport, 25,
            (d, i, esc) => this._renderDistRow(d, i, esc),
            { domainFn: d => d.primaryDomain, renderDetail: (d, i, esc) => this._renderDistDetail(d, i, esc),
              sortKeys: distSortKeys }
        );
        setTimeout(() => {
            PA.tf.render('sum-dist');
            PA.tf.initColFilters('sum-dist', {
                2: { label: 'Domain', valueFn: d => d.primaryDomain || '' },
                3: { label: 'Transaction', valueFn: d => d.transactionRequirement || '' },
                8: { label: 'Performance', valueFn: d => d.performanceImplication || '' }
            });
        }, 0);
        return html;
    },

    _renderDistRow(d, i, esc) {
        const isReq = d.transactionRequirement.startsWith('REQUIRED');
        const wDoms = Object.entries(d.crossDomainDependencies).filter(([_, v]) => v.write.length > 0).map(([k]) => k);
        const rDoms = Object.entries(d.crossDomainDependencies).filter(([_, v]) => v.read.length > 0).map(([k]) => k);
        const dParts = (d.endpointName || '').split('.');
        const dCls = (dParts[0] || '').replace(/'/g, "\\'");
        const dMeth = (dParts.length > 1 ? dParts.slice(1).join('.') : '').replace(/'/g, "\\'");

        let html = `<tr onclick="PA.tf.toggleDetail('sum-dist',${i})" style="cursor:pointer">`;
        html += `<td>${i + 1}</td>`;
        html += `<td><span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(dCls)}','${esc(dMeth)}')" title="View code">${esc(d.endpointName)}</span></td>`;
        html += `<td><span class="sum-domain-tag">${esc(d.primaryDomain)}</span></td>`;
        html += `<td><span class="sum-txn-badge ${isReq ? 'sum-txn-req' : 'sum-txn-none'}" title="${isReq ? 'REQUIRED — This endpoint writes to multiple collections across different business domains. Needs @Transactional or compensation logic.' : 'No cross-domain writes detected — transaction coordination not required'}">${isReq ? 'REQUIRED' : d.transactionRequirement}</span></td>`;
        html += '<td class="sum-domain-stack">' + wDoms.map(d => '<span class="sum-domain-tag">' + esc(d) + '</span>').join('') + '</td>';
        html += '<td class="sum-domain-stack">' + rDoms.map(d => '<span class="sum-domain-tag">' + esc(d) + '</span>').join('') + '</td>';
        html += `<td class="sum-center">${d.totalCollections}</td>`;
        html += `<td class="sum-center">${d.totalLoc || 0}</td>`;
        html += `<td class="sum-perf-cell" title="${esc(this._perfTip(d.performanceImplication))}">${esc(d.performanceImplication)}</td></tr>`;
        return html;
    },

    /** Bare inner content — see `summary-aggregation.js:_renderAggDetail`'s doc for why a
     *  `<tr>`/`<td>` wrapper would nest invalidly inside `PA.tf.toggleDetail`'s own `<td>`. */
    _renderDistDetail(d, i, esc) {
        let html = '<div class="sum-detail">';
        for (const [domain, deps] of Object.entries(d.crossDomainDependencies)) {
            html += `<div class="sum-detail-block"><div class="sum-detail-label"><span class="sum-domain-tag">${esc(domain)}</span></div>`;
            if (deps.write.length) html += '<div><b style="color:var(--red,var(--red));font-size:10px">WRITE:</b> ' + deps.write.map(c => this._collBadge(c, esc)).join(' ') + '</div>';
            if (deps.read.length) html += '<div><b style="color:var(--blue,var(--blue));font-size:10px">READ:</b> ' + deps.read.map(c => this._collBadge(c, esc)).join(' ') + '</div>';
            if (deps.aggregate.length) html += '<div><b style="color:var(--purple,var(--accent));font-size:10px">AGGREGATE:</b> ' + deps.aggregate.map(c => this._collBadge(c, esc)).join(' ') + '</div>';
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    /* ===== Batch Jobs — Paginated ===== */
    _renderBatchTable(batchReport, esc) {
        if (!batchReport.length) return '<p class="sum-muted" style="padding:20px">No batch jobs detected</p>';
        let html = '<div class="sum-section"><div class="sum-section-title">Batch Jobs (' + batchReport.length + ')</div>';
        html += '<div class="sum-section-desc">Endpoints classified as batch/scheduler jobs based on naming patterns (batch, scheduler, cron, job).</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="Batch jobs often touch many collections across domains because they aggregate, reconcile, or sync data. Each cross-domain access is a dependency that affects migration order.">Batch jobs often have hidden cross-domain data dependencies</span>';
        html += '<span class="sum-tip" title="XL-sized batch jobs with High DB operations are resource-heavy. After migration, they may overwhelm a single service. Consider splitting or running in a dedicated batch service.">Large batch jobs (XL + High perf) need dedicated scaling strategy</span>';
        html += '<span class="sum-tip" title="Compare with the Scheduled Jobs tab to distinguish REST-triggered batches from cron-triggered ones. Cron jobs need to be assigned to exactly one vertical.">Cross-reference with Scheduled Jobs tab for trigger mechanism</span>';
        html += '</div>';
        html += this._buildFilterBarPA('sum-batch', batchReport, b => b.primaryDomain);
        html += '<div class="sum-pager sum-pager-top" id="sum-batch-top-pager"></div>';
        html += '<div class="sum-table-wrap"><table class="sum-table" id="sum-batch-table"><thead><tr>';
        html += '<th style="width:40px" title="Row number">#</th>';
        html += `<th class="sum-th-sort" data-sort-col="1" onclick="PA.tf.sort('sum-batch',1)" title="Batch job or scheduled task name (Controller.method) detected by naming pattern">Name</th>`;
        html += `<th class="sum-th-sort" data-sort-col="2" onclick="PA.tf.sort('sum-batch',2)" title="Primary business domain this batch job belongs to">Domain</th>`;
        html += '<th title="All business domains touched by this batch job — cross-domain batch jobs may need careful ordering">Domains</th>';
        html += `<th class="sum-th-sort" data-sort-col="4" onclick="PA.tf.sort('sum-batch',4)" style="width:50px" title="Number of MongoDB collections accessed by this batch job">Colls</th>`;
        html += `<th class="sum-th-sort" data-sort-col="5" onclick="PA.tf.sort('sum-batch',5)" style="width:60px" title="Total number of methods in this batch job's call tree">Methods</th>`;
        html += `<th class="sum-th-sort" data-sort-col="6" onclick="PA.tf.sort('sum-batch',6)" style="width:60px" title="Total lines of code across all methods in this batch job's call tree">LOC</th>`;
        html += `<th class="sum-th-sort" data-sort-col="7" onclick="PA.tf.sort('sum-batch',7)" title="Size category (S/M/L/XL) based on method count and code complexity">Size</th>`;
        html += `<th class="sum-th-sort" data-sort-col="8" onclick="PA.tf.sort('sum-batch',8)" title="Estimated performance impact based on collection count and operation types">Perf</th>`;
        html += '</tr></thead><tbody id="sum-batch-tbody"></tbody></table></div>';
        html += '</div>';

        const batchSortKeys = {
            1: { fn: b => b.batchName || '' },
            2: { fn: b => b.primaryDomain || '' },
            4: { fn: b => b.totalCollections || 0 },
            5: { fn: b => b.totalMethods || 0 },
            6: { fn: b => b.totalLoc || 0 },
            7: { fn: b => ({ S: 0, M: 1, L: 2, XL: 3 }[b.sizeCategory] || 0) },
            8: { fn: b => b.performanceImplication || '' }
        };
        PA.tf.init('sum-batch', batchReport, 25,
            (b, i, esc) => this._renderBatchRow(b, i, esc),
            { domainFn: b => b.primaryDomain, renderDetail: (b, i, esc) => this._renderBatchDetail(b, i, esc),
              sortKeys: batchSortKeys }
        );
        setTimeout(() => {
            PA.tf.render('sum-batch');
            PA.tf.initColFilters('sum-batch', {
                2: { label: 'Domain', valueFn: b => b.primaryDomain || '' },
                7: { label: 'Size', valueFn: b => b.sizeCategory || '' },
                8: { label: 'Performance', valueFn: b => b.performanceImplication || '' }
            });
        }, 0);
        return html;
    },

    _renderBatchRow(b, i, esc) {
        const bParts = (b.batchName || '').split('.');
        const bCls = (bParts[0] || '').replace(/'/g, "\\'");
        const bMeth = (bParts.length > 1 ? bParts.slice(1).join('.') : '').replace(/'/g, "\\'");

        let html = `<tr onclick="PA.tf.toggleDetail('sum-batch',${i})" style="cursor:pointer">`;
        html += `<td>${i + 1}</td>`;
        html += `<td><span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(bCls)}','${esc(bMeth)}')" title="View code">${esc(b.batchName)}</span></td>`;
        html += `<td><span class="sum-domain-tag">${esc(b.primaryDomain)}</span></td>`;
        html += `<td>${b.domains.map(d => '<span class="sum-domain-tag">' + esc(d) + '</span>').join(' ')}</td>`;
        html += `<td class="sum-center">${b.totalCollections}</td><td class="sum-center">${b.totalMethods}</td>`;
        html += `<td class="sum-center">${b.totalLoc || 0}</td>`;
        html += `<td><span class="sum-size-badge sum-size-${b.sizeCategory.toLowerCase()}" title="${esc(this._sizeTip(b.sizeCategory))}">${esc(b.sizeCategory)}</span></td>`;
        html += `<td class="sum-perf-cell" title="${esc(this._perfTip(b.performanceImplication))}">${esc(b.performanceImplication)}</td></tr>`;
        return html;
    },

    /** Bare inner content — see `summary-aggregation.js:_renderAggDetail`'s doc for why a
     *  `<tr>`/`<td>` wrapper would nest invalidly inside `PA.tf.toggleDetail`'s own `<td>`. */
    _renderBatchDetail(b, i, esc) {
        let html = '<div class="sum-detail">';
        for (const [domain, deps] of Object.entries(b.externalDependencies)) {
            html += `<div class="sum-detail-block"><div class="sum-detail-label"><span class="sum-domain-tag">${esc(domain)}</span> (${deps.length})</div>`;
            for (const d of deps.slice(0, 15)) {
                html += this._collBadge(d.collection, esc) + ' ';
                html += d.usageTypes.map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${op}</span>`).join('') + ' ';
            }
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    /* ===== External Dependencies — Paginated ===== */
    _renderExternalTable(extReport, esc) {
        if (!extReport.crossModule || !extReport.crossModule.length) return '<p class="sum-muted" style="padding:20px">No cross-module dependencies detected</p>';
        const total = extReport.crossModule.length;
        let html = '<div class="sum-section"><div class="sum-section-title">Cross-Module (' + total + ' modules)</div>';
        html += '<div class="sum-section-desc">External JAR modules referenced from endpoint call trees — each represents a compile-time dependency.</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="Each cross-module dependency is a direct method call to a class in another JAR. After verticalisation, these become REST API contracts between services.">Each dependency becomes a REST API contract after splitting</span>';
        html += '<span class="sum-tip" title="Modules used by many endpoints (high call count) indicate tight coupling. These are the hardest to decouple and should be migrated first or extracted as shared libraries.">High call count = tight coupling, prioritize for migration</span>';
        html += '<span class="sum-tip" title="Utility/common modules used by all domains may stay as shared libraries. Domain-specific modules should become APIs owned by that domain.">Shared utilities can stay as libraries; domain modules should become APIs</span>';
        html += '</div>';
        html += this._buildFilterBarPA('sum-ext', extReport.crossModule, m => m.domain);
        html += '<div class="sum-pager sum-pager-top" id="sum-ext-top-pager"></div>';
        html += '<div class="sum-table-wrap"><table class="sum-table" id="sum-ext-table"><thead><tr>';
        html += '<th style="width:40px" title="Row number">#</th>';
        html += `<th class="sum-th-sort" data-sort-col="1" onclick="PA.tf.sort('sum-ext',1)" title="External JAR module/library referenced from endpoint call trees">Module</th>`;
        html += `<th class="sum-th-sort" data-sort-col="2" onclick="PA.tf.sort('sum-ext',2)" title="Business domain this external module belongs to based on JAR-to-domain mapping">Domain</th>`;
        html += `<th class="sum-th-sort" data-sort-col="3" onclick="PA.tf.sort('sum-ext',3)" style="width:60px" title="Number of distinct classes called from this external module">Classes</th>`;
        html += '<th title="Specific methods called on classes in this external module">Methods</th>';
        html += `<th class="sum-th-sort" data-sort-col="5" onclick="PA.tf.sort('sum-ext',5)" style="width:70px" title="Number of REST endpoints that call into this external module">Endpoints</th>`;
        html += `<th class="sum-th-sort" data-sort-col="6" onclick="PA.tf.sort('sum-ext',6)" style="width:50px" title="Total number of method invocations to this external module across all endpoints">Calls</th>`;
        html += '</tr></thead><tbody id="sum-ext-tbody"></tbody></table></div>';
        html += '</div>';

        const extSortKeys = {
            1: { fn: m => m.project || '' },
            2: { fn: m => m.domain || '' },
            3: { fn: m => m.classes ? (m.classes.size || 0) : 0 },
            5: { fn: m => m.endpoints ? (m.endpoints.size || 0) : 0 },
            6: { fn: m => m.count || 0 }
        };
        PA.tf.init('sum-ext', extReport.crossModule, 25,
            (m, i, esc) => this._renderExtRow(m, i, esc),
            { domainFn: m => m.domain, renderDetail: (m, i, esc) => this._renderExtDetail(m, i, esc),
              sortKeys: extSortKeys }
        );
        setTimeout(() => {
            PA.tf.render('sum-ext');
            PA.tf.initColFilters('sum-ext', {
                2: { label: 'Domain', valueFn: m => m.domain || '' }
            });
        }, 0);
        return html;
    },

    _renderExtRow(m, i, esc) {
        let html = `<tr onclick="PA.tf.toggleDetail('sum-ext',${i})" style="cursor:pointer">`;
        html += `<td>${i + 1}</td><td><span class="sum-module-tag">${esc(m.project)}</span></td>`;
        html += `<td><span class="sum-domain-tag">${esc(m.domain)}</span></td>`;
        html += `<td class="sum-center">${m.classes.size}</td>`;
        html += '<td class="sum-mono-sm">';
        const methodList = [...m.methods];
        methodList.slice(0, 3).forEach(x => {
            const parts = x.split('.');
            const mName = parts.length > 1 ? parts[1] : x;
            const cName = parts.length > 1 ? parts[0] : '';
            html += `<span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(cName)}','${esc(mName)}')" title="View code">${esc(x)}</span> `;
        });
        if (m.methods.size > 3) html += `<span class="sum-muted">+${m.methods.size - 3}</span>`;
        html += '</td>';
        html += `<td class="sum-center">${m.endpoints.size}</td><td class="sum-center">${m.count}</td></tr>`;
        return html;
    },

    /** Bare inner content — see `summary-aggregation.js:_renderAggDetail`'s doc for why a
     *  `<tr>`/`<td>` wrapper would nest invalidly inside `PA.tf.toggleDetail`'s own `<td>`. */
    _renderExtDetail(m, i, esc) {
        const methodList = [...m.methods];
        let html = '<div class="sum-detail">';
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Methods (' + m.methods.size + ')</div>';
        methodList.forEach(x => {
            const parts = x.split('.');
            const mName = parts.length > 1 ? parts[1] : x;
            const cName = parts.length > 1 ? parts[0] : '';
            html += `<span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(cName)}','${esc(mName)}')" title="View code">${esc(x)}</span> `;
        });
        html += '</div>';
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Endpoints (' + m.endpoints.size + ')</div>' + [...m.endpoints].map(ep => `<span class="sum-bean-tag">${esc(ep)}</span>`).join(' ') + '</div>';
        if (m.breadcrumbs.length) {
            html += '<div class="sum-detail-block sum-detail-block-full"><div class="sum-detail-label">Call Paths</div>';
            for (const bc of m.breadcrumbs) html += '<div class="sum-breadcrumb">' + this._renderBc(bc, esc) + '</div>';
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    _openExtMethod(simpleName, methodName) {
        if (!JA.summary._classIdx) return;
        let fqn = simpleName;
        for (const k of Object.keys(JA.summary._classIdx)) {
            if (k.endsWith('.' + simpleName) || k === simpleName) { fqn = k; break; }
        }
        JA.summary.showClassCode(fqn, methodName);
    },

    async _loadVertCallPaths(pathsId, c, esc) {
        const el = document.getElementById(pathsId);
        if (!el) return;
        const jarId = JA.app.currentJarId || '';
        const collName = c.name;
        const epReports = this._epReports || [];
        const epNames = c.accessedByEndpoints ? [...c.accessedByEndpoints] : [...(c.endpoints || [])];

        const seen = new Set();
        let html = '';
        let count = 0;

        for (const epName of epNames.slice(0, 8)) {
            const r = epReports.find(r => r.endpointName === epName);
            if (!r) continue;
            try {
                let tree = r.endpoint && r.endpoint.callTree;
                if (!tree && r.sourceIdx != null && jarId) {
                    // sourceIdx indexes the endpoint array fetched for JA.app._currentVersion, and the server
                    // resolves the index against whichever version it is given. Omitting version here meant a
                    // screen showing STATIC or CORRECTED_PREV resolved its index against the CURRENT document
                    // and rendered a different endpoint's call tree — deterministic, not a race.
                    tree = await JA.api.getCallTree(jarId, r.sourceIdx, JA.app._currentVersion);
                }
                if (!tree) continue;
                this._walkWithPath(tree, (node, path) => {
                    if (!node.collectionsAccessed || !node.collectionsAccessed.includes(collName)) return;
                    const bc = this._buildBreadcrumb(path, 20);
                    const key = bc.map(b => b.label).join('>');
                    if (seen.has(key)) return;
                    seen.add(key);
                    count++;
                    html += `<div class="sum-bc-group sum-scroll-item">`;
                    html += `<span class="sum-muted" style="font-size:10px">${esc(epName)}</span>`;
                    html += '<div class="sum-breadcrumb">' + this._renderBc(bc, esc) + '</div>';
                    html += '</div>';
                });
            } catch (e) { /* skip */ }
            if (count >= 10) break;
        }

        if (el) el.innerHTML = count ? html : '<span class="sum-muted" style="font-size:11px">No call paths available</span>';
    },

    async _loadEpCallPaths(trailId, r, epIdx, esc) {
        const el = document.getElementById(trailId);
        if (!el) return;
        const jarId = JA.app.currentJarId || '';
        const collections = r.collections || {};

        try {
            let tree = r.endpoint && r.endpoint.callTree;
            if (!tree && r.sourceIdx != null && jarId) {
                tree = await JA.api.getCallTree(jarId, r.sourceIdx, JA.app._currentVersion);
            }

            if (tree) {
                // Walk the real call tree — collect root→leaf paths for every node
                // that has collectionsAccessed set (these are the actual DB caller nodes)
                const bcMap = {};
                this._walkWithPath(tree, (node, path) => {
                    if (!node.collectionsAccessed || !node.collectionsAccessed.length) return;
                    for (const cn of node.collectionsAccessed) {
                        if (collections[cn]) this._addCollBc(bcMap, cn, path);
                    }
                });

                const bcColls = Object.keys(bcMap).filter(k => bcMap[k].length > 0);
                if (bcColls.length) {
                    let html = this._scrollSection('ep-paths-scroll-' + epIdx);
                    for (const cn of bcColls) {
                        html += `<div class="sum-bc-group sum-scroll-item">${this._collBadgeRich(cn, esc, collections[cn])}`;
                        for (const bc of bcMap[cn]) html += '<div class="sum-breadcrumb">' + this._renderBc(bc, esc) + '</div>';
                        html += '</div>';
                    }
                    html += '</div>';
                    el.innerHTML = html;
                    return;
                }
            }
        } catch (e) { /* fall through to synthetic */ }

        // Fallback: show synthetic breadcrumbs from _summary_static
        const bcColls = Object.keys(r.collBreadcrumbs || {}).filter(k => r.collBreadcrumbs[k].length > 0);
        if (bcColls.length) {
            let html = this._scrollSection('ep-paths-scroll-' + epIdx);
            for (const cn of bcColls) {
                html += `<div class="sum-bc-group sum-scroll-item">${this._collBadgeRich(cn, esc, collections[cn])}`;
                for (const bc of r.collBreadcrumbs[cn]) html += '<div class="sum-breadcrumb">' + this._renderBc(bc, esc) + '</div>';
                html += '</div>';
            }
            html += '</div>';
            el.innerHTML = html;
        } else {
            el.innerHTML = '<span class="sum-muted" style="font-size:11px">No call paths available</span>';
        }
    }
});
