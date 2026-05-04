/**
 * Summary — Main orchestrator.
 * State + render entry point + sub-tab switching.
 * All methods come from component files (summary-*.js).
 *
 * Load order: helpers → col-filter → analyzer → tables → trace → explorer →
 *             export-style → export-excel → export-modal → export →
 *             claude → corrections → corr-logbrowser → this file
 *
 * PL/SQL Analyzer: procedures/tables/schemas instead of endpoints/collections/domains.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    /* --- State --- */
    _procReports:       null,
    _txnReport:         null,
    _schemaSliceReport: null,
    _extReport:         null,
    _batchReport:       null,
    _totalExtCalls:     0,
    _lazyTabs:          null,

    /* ===== Main Entry ===== */
    async render(analysisData) {
        const container = document.getElementById('summary-container');
        if (!container) return;
        if (!analysisData) {
            container.innerHTML = '<p class="empty-state" style="padding:40px">No analysis loaded — run an analysis first.</p>';
            return;
        }
        if (!analysisData.callGraph) {
            try {
                const cg = await PA.api.getCallGraph();
                if (cg) analysisData.callGraph = cg;
            } catch (e) { /* ignore */ }
        }
        if (!analysisData.callGraph) {
            container.innerHTML = '<p class="empty-state" style="padding:40px">No call graph data available.</p>';
            return;
        }
        this._render(analysisData, container);
    },

    _render(analysisData, container) {
        const esc = PA.esc;
        const graph = analysisData.callGraph || {};
        const nodes = graph.nodes || graph.procedures || [];
        const totalProcs = nodes.length;

        // Show progress skeleton immediately
        container.innerHTML = `<div class="sum-page">
            <div class="sum-progress-bar" id="sum-progress">
                <div class="sum-progress-fill" id="sum-progress-fill" style="width:0%"></div>
            </div>
            <div class="sum-progress-text" id="sum-progress-text">Analyzing procedures: 0 / ${totalProcs}</div>
            <div class="sum-stats" id="sum-stats-live"></div>
            <div id="sum-content-area"></div>
        </div>`;

        // Process procedures in async batches for progressive display
        const batchSize = 15;
        const procReports = [];
        let idx = 0;

        const processBatch = async () => {
            const end = Math.min(idx + batchSize, nodes.length);
            for (let i = idx; i < end; i++) {
                const node = nodes[i];
                const procId = node.id || node.name;
                if (!procId) continue;
                try {
                    const detail = await PA.api.getProcDetail(procId);
                    if (detail) {
                        const r = this._analyzeProcedure(detail, analysisData);
                        if (r) procReports.push(r);
                    }
                } catch (e) {
                    procReports.push(this._minimalReport(node));
                }
            }
            idx = end;

            // Update progress
            const pct = Math.round((idx / Math.max(nodes.length, 1)) * 100);
            const fill = document.getElementById('sum-progress-fill');
            const text = document.getElementById('sum-progress-text');
            if (fill) fill.style.width = pct + '%';
            if (text) text.textContent = `Analyzing procedures: ${idx} / ${totalProcs} (${procReports.length} with data)`;

            // Update live stats
            this._updateLiveStats(procReports);

            if (idx < nodes.length) {
                requestAnimationFrame(() => processBatch());
            } else {
                // All done — build final reports and render
                this._finishRender(analysisData, procReports, container, esc);
            }
        };
        requestAnimationFrame(() => processBatch());
    },

    _updateLiveStats(procReports) {
        const el = document.getElementById('sum-stats-live');
        if (!el) return;
        const tables = new Set();
        const schemas = new Set();
        let cross = 0;
        for (const r of procReports) {
            for (const t of Object.keys(r.flowTables || {})) tables.add(t);
            for (const s of (r.schemas || [])) schemas.add(s);
            if (r.crossSchemaCalls > 0) cross++;
        }
        el.innerHTML =
            `<div class="sum-stat-card"><div class="sum-stat-number">${procReports.length}</div><div class="sum-stat-label">Procedures</div></div>` +
            `<div class="sum-stat-card"><div class="sum-stat-number">${tables.size}</div><div class="sum-stat-label">Tables</div></div>` +
            `<div class="sum-stat-card"><div class="sum-stat-number">${schemas.size}</div><div class="sum-stat-label">Schemas</div></div>` +
            `<div class="sum-stat-card"><div class="sum-stat-number">${cross}</div><div class="sum-stat-label">Cross-Schema</div></div>`;
    },

    _finishRender(analysisData, procReports, container, esc) {
        // Build secondary reports
        const txnReport         = this._buildTransactionReport(procReports);
        const schemaSliceReport = this._buildSchemaSliceReport(procReports);
        const extReport         = this._buildExternalDepsReport(procReports);
        const batchReport       = this._buildBatchReport(procReports);

        // Aggregate stats
        const allTables  = new Set();
        const allSchemas = new Set();
        let crossSchemaProcs = 0, totalLoc = 0, totalExtCalls = 0, txnRequired = 0;

        for (const r of procReports) {
            for (const t of Object.keys(r.flowTables || {})) allTables.add(t);
            for (const s of (r.schemas || [])) allSchemas.add(s);
            totalExtCalls += r.externalCalls || 0;
            if (r.crossSchemaCalls > 0) crossSchemaProcs++;
            totalLoc += r.loc || 0;
        }
        for (const d of txnReport) {
            if (d.transactionRequirement.startsWith('REQUIRED')) txnRequired++;
        }

        // Cache for exports and other tabs
        this._procReports       = procReports;
        this._txnReport         = txnReport;
        this._schemaSliceReport = schemaSliceReport;
        this._extReport         = extReport;
        this._batchReport       = batchReport;
        this._totalExtCalls     = totalExtCalls;

        // Build final page
        let html = '<div class="sum-page">';

        // --- Top-level 3-way view selector ---
        html += '<div class="sum-view-selector">';
        html += '<button class="sum-view-btn sum-view-active" data-view="procedures" onclick="PA.summary.switchView(\'procedures\')">';
        html += '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>';
        html += ' Procedures</button>';
        html += '<button class="sum-view-btn" data-view="static" onclick="PA.summary.switchView(\'static\')">';
        html += '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>';
        html += ' Static Analysis</button>';
        html += '<button class="sum-view-btn sum-view-claude" data-view="claude-ai" onclick="PA.summary.switchView(\'claude-ai\')">';
        html += '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2l2.4 7.4H22l-6.2 4.5 2.4 7.4L12 17l-6.2 4.3 2.4-7.4L2 9.4h7.6z"/></svg>';
        html += ' Claude AI</button>';
        html += '</div>';

        // --- View: Procedures (procedure report) ---
        html += '<div class="sum-view-panel" id="sview-procedures">';
        html += this._renderProcedureTable(procReports);
        html += '</div>';

        // --- View: Static Analysis (tabs for tables, transactions, etc.) ---
        html += '<div class="sum-view-panel" id="sview-static" style="display:none">';
        html += '<div class="sum-tabs">';
        html += '<button class="sum-tab active" data-stab="tables" onclick="PA.summary.switchSubTab(\'tables\')" title="Consolidated view of all tables">Table Analysis</button>';
        html += '<button class="sum-tab" data-stab="transactions" onclick="PA.summary.switchSubTab(\'transactions\')" title="Transaction management analysis">Transactions</button>';
        html += '<button class="sum-tab" data-stab="schema-slice" onclick="PA.summary.switchSubTab(\'schema-slice\')" title="Schema ownership mapping">Schema Slice</button>';
        html += '<button class="sum-tab" data-stab="external" onclick="PA.summary.switchSubTab(\'external\')" title="Cross-schema dependencies">External Deps</button>';
        html += '<button class="sum-tab" data-stab="batch" onclick="PA.summary.switchSubTab(\'batch\')" title="DBMS_SCHEDULER / DBMS_JOB patterns">Batch Jobs</button>';
        html += '</div>';
        html += '<div class="sum-subtab" id="stab-tables"></div>';
        html += '<div class="sum-subtab" id="stab-transactions" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-schema-slice" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-external" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-batch" style="display:none"></div>';
        html += '</div>';

        // --- View: Claude AI (tabs for progress, insights, corrections) ---
        html += '<div class="sum-view-panel" id="sview-claude-ai" style="display:none">';
        html += '<div class="sum-tabs sum-tabs-claude">';
        html += '<button class="sum-tab sum-tab-progress active" data-stab="claude-progress" onclick="PA.summary.switchSubTab(\'claude-progress\')" title="Live progress tracking">Progress</button>';
        html += '<button class="sum-tab" data-stab="claude" onclick="PA.summary.switchSubTab(\'claude\')" title="AI verification insights">Claude Insights</button>';
        html += '<button class="sum-tab" data-stab="corrections" onclick="PA.summary.switchSubTab(\'corrections\')" title="Table corrections">Claude Corrections</button>';
        html += '</div>';
        html += '<div class="sum-subtab" id="stab-claude-progress"></div>';
        html += '<div class="sum-subtab" id="stab-claude" style="display:none"></div>';
        html += '<div class="sum-subtab" id="stab-corrections" style="display:none"></div>';
        html += '</div>';

        html += '</div>';
        container.innerHTML = html;

        // Store lazy renderers (keyed by tab id)
        this._lazyTabs = {
            'tables':       () => this._renderTableAnalysis(procReports),
            'transactions': () => this._renderTransactionTable(txnReport),
            'schema-slice': () => this._renderSchemaSliceTable(schemaSliceReport),
            'external':     () => this._renderExternalTable(extReport),
            'batch':        () => this._renderBatchTable(batchReport),
            'claude-progress': () => typeof this._renderClaudeProgressTab === 'function'
                                    ? this._renderClaudeProgressTab()
                                    : '<p style="padding:20px;color:var(--text-muted)">Claude progress loading...</p>',
            'claude':       () => typeof this._renderClaudeTab === 'function'
                                    ? this._renderClaudeTab()
                                    : '<p style="padding:20px;color:var(--text-muted)">Claude insights loading...</p>',
            'corrections':  () => typeof this._renderCorrectionsTab === 'function'
                                    ? this._renderCorrectionsTab()
                                    : '<p style="padding:20px;color:var(--text-muted)">Claude corrections loading...</p>'
        };

        // Auto-render default sub-tabs for Static and Claude views (lazy on first view switch)
        this._staticDefaultRendered = false;
        this._claudeDefaultRendered = false;
    },

    switchView(viewId) {
        // Update view buttons
        document.querySelectorAll('.sum-view-btn').forEach(b =>
            b.classList.toggle('sum-view-active', b.dataset.view === viewId)
        );
        // Show/hide view panels
        document.querySelectorAll('.sum-view-panel').forEach(p =>
            p.style.display = p.id === 'sview-' + viewId ? '' : 'none'
        );
        // Lazy-render default sub-tab for the view on first switch
        if (viewId === 'static' && !this._staticDefaultRendered) {
            this._staticDefaultRendered = true;
            const tab = document.getElementById('stab-tables');
            if (tab && !tab.dataset.rendered && this._lazyTabs && this._lazyTabs['tables']) {
                tab.innerHTML = this._lazyTabs['tables']();
                tab.dataset.rendered = '1';
            }
        }
        if (viewId === 'claude-ai' && !this._claudeDefaultRendered) {
            this._claudeDefaultRendered = true;
            const tab = document.getElementById('stab-claude-progress');
            if (tab && !tab.dataset.rendered && this._lazyTabs && this._lazyTabs['claude-progress']) {
                tab.innerHTML = this._lazyTabs['claude-progress']();
                tab.dataset.rendered = '1';
            }
        }
    },

    async switchSubTab(tabId) {
        const targetTab = document.getElementById('stab-' + tabId);
        if (!targetTab) return;
        // Find the parent view panel so we only toggle siblings
        const viewPanel = targetTab.closest('.sum-view-panel');
        if (viewPanel) {
            viewPanel.querySelectorAll('.sum-tab').forEach(t =>
                t.classList.toggle('active', t.dataset.stab === tabId)
            );
            viewPanel.querySelectorAll('.sum-subtab').forEach(tc =>
                tc.style.display = tc.id === 'stab-' + tabId ? '' : 'none'
            );
        }
        // Lazy render on first access
        if (!targetTab.dataset.rendered && this._lazyTabs && this._lazyTabs[tabId]) {
            if (tabId === 'corrections') await this._loadCorrectionData();
            targetTab.innerHTML = this._lazyTabs[tabId]();
            targetTab.dataset.rendered = '1';
        }
    }
});
