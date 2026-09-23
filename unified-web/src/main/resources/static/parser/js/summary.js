window.PA = window.PA || {};

PA.summary = {
    load: async function() {
        await PA.summary._render();
    },

    applyScope: async function() {
        await PA.summary._render();
    },

    /**
     * UI-024 — was `PA.getScopedNodes()` (a `PA.nodeIndex.all()` scan) feeding `_computeStats`'s
     * own full pass over every scoped node. Unlike `complexity.js` (§98), the dashboard only ever
     * renders totals/breakdowns/highlights — never a row per procedure — so this genuinely
     * collapses to one small aggregate fetch (`SummaryStatsService`), the same shape as
     * `statements.js`/`detailHeader.js`'s own conversions. `_computeStats` (the local scan) is
     * gone entirely; the `data.xxx || computed` fallback pattern for the handful of root-level
     * scalars (`totalTables`/`totalEdges`/`maxDepth`/`errors`, already on the small, already-loaded
     * index root — not a scan) is preserved here instead.
     */
    _render: async function() {
        var container = document.getElementById('summaryContainer');
        if (!container) return;
        var data = PA.analysisData;
        if (!data) { container.innerHTML = '<div class="empty-msg">No analysis data</div>'; return; }

        var scopeLabel = PA._scope || 'full';
        var ctx = PA.context || {};
        var isPkgRoot = typeof PA._isPackageRoot === 'function' && PA._isPackageRoot();
        var effectiveScope = (!ctx.procId || isPkgRoot) ? 'full' : scopeLabel;
        var procId = effectiveScope === 'full' ? null : PA._currentScopeId();
        var entrySchema = data.entrySchema || data.entryPoint || '';

        if (container) container.innerHTML = '<div class="empty-msg">Loading summary...</div>';
        var stats = await PA.api.getSummaryStats(effectiveScope, procId, entrySchema);
        if (!stats) { container.innerHTML = '<div class="empty-msg">Failed to load summary</div>'; return; }

        stats.entryPoint = data.entryPoint || data.name || '';
        stats.totalTables = data.totalTables || stats.totalTables;
        stats.totalEdges = data.totalEdges || stats.totalCalls;
        stats.maxDepth = data.maxDepth || stats.maxDepthNode.value;
        stats.errors = (data.errors || []).length;
        stats.scopeLabel = scopeLabel;
        stats.scopedCount = stats.totalNodes;
        stats.fullCount = data.totalNodes || (data.nodes || []).length;
        stats.txn = data.transactionSummary || null;
        container.innerHTML = PA.summary._renderDashboard(stats);
    },

    _renderDashboard: function(s) {
        var esc = PA.esc;
        var html = '<div class="sum-grid">';

        if (s.scopeLabel && s.scopeLabel !== 'full' && s.scopedCount !== s.fullCount) {
            html += '<div class="sum-scope-note">Scope: <strong>' + PA.esc(s.scopeLabel) + '</strong> — showing ' + s.scopedCount + ' of ' + s.fullCount + ' procedures</div>';
        }

        /* Overview section hidden per feedback */

        html += '<div class="sum-card">';
        html += '<div class="sum-card-title" title="Breakdown of PL/SQL object types (Procedure, Function, Trigger, etc.)">Object Types</div>';
        html += '<div class="sum-breakdown-scroll">';
        html += PA.summary._renderBreakdown(s.types, s.totalNodes);
        html += '</div></div>';

        html += '<div class="sum-card">';
        html += '<div class="sum-card-title" title="Distribution of procedures across Oracle schemas">Schemas</div>';
        html += '<div class="sum-breakdown-scroll">';
        html += PA.summary._renderSchemaBreakdown(s.schemas, s.totalNodes);
        html += '</div></div>';

        /* Statement Types section hidden per feedback */

        html += '<div class="sum-card">';
        html += '<div class="sum-card-title" title="Notable procedures — largest, deepest, most table access, most calls">Highlights</div>';
        html += '<div class="sum-highlights">';
        html += PA.summary._highlight('Most LOC', s.maxLoc.name, s.maxLoc.value.toLocaleString() + ' lines', s.maxLoc.name);
        html += PA.summary._highlight('Deepest', s.maxDepthNode.name, 'depth ' + s.maxDepthNode.value, s.maxDepthNode.name);
        html += PA.summary._highlight('Most Tables', s.maxTables.name, s.maxTables.value + ' tables', s.maxTables.name);
        html += PA.summary._highlight('Most Calls', s.maxCalls.name, s.maxCalls.value + ' calls', s.maxCalls.name);
        html += '</div></div>';

        // Transaction Scope card (flow-wide)
        if (s.txn) {
            var t = s.txn;
            html += '<div class="sum-card">';
            html += '<div class="sum-card-title" title="Transaction control across the flow">Transaction Scope</div>';
            html += '<div class="sum-highlights">';
            html += PA.summary._highlight('Commits', 'COMMIT', (t.commits || 0) + '', '');
            html += PA.summary._highlight('Rollbacks', 'ROLLBACK', (t.rollbacks || 0) + '', '');
            html += PA.summary._highlight('Savepoints', 'SAVEPOINT', (t.savepoints || 0) + '', '');
            html += PA.summary._highlight('Autonomous', 'PRAGMA', (t.autonomousProcs || 0) + ' procs', '');
            html += '</div></div>';
        }

        html += '</div>';
        return html;
    },

    _kpi: function(value, label, color, tooltip) {
        return '<div class="sum-kpi"' + (tooltip ? ' title="' + PA.escAttr(tooltip) + '"' : '') + '><span class="sum-kpi-value" style="color:' + color + '">' + value + '</span><span class="sum-kpi-label">' + label + '</span></div>';
    },

    _renderBreakdown: function(map, total) {
        var entries = Object.entries(map).sort(function(a, b) { return b[1] - a[1]; });
        var html = '<div class="sum-breakdown">';
        for (var i = 0; i < entries.length; i++) {
            var pct = total > 0 ? Math.round((entries[i][1] / total) * 100) : 0;
            var color = entries[i][0] === 'FUNCTION' ? 'var(--badge-blue)' :
                        entries[i][0] === 'TRIGGER' ? 'var(--badge-orange)' : 'var(--accent)';
            html += '<div class="sum-bd-row">';
            html += '<span class="sum-bd-label">' + PA.esc(entries[i][0]) + '</span>';
            html += '<div class="sum-bd-bar-wrap"><div class="sum-bd-bar" style="width:' + pct + '%;background:' + color + '"></div></div>';
            html += '<span class="sum-bd-value">' + entries[i][1] + ' <small>(' + pct + '%)</small></span>';
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    _renderSchemaBreakdown: function(map, total) {
        var entries = Object.entries(map).sort(function(a, b) { return b[1] - a[1]; });
        var html = '<div class="sum-breakdown">';
        for (var i = 0; i < entries.length; i++) {
            var pct = total > 0 ? Math.round((entries[i][1] / total) * 100) : 0;
            var colorObj = PA.getSchemaColor(entries[i][0]);
            html += '<div class="sum-bd-row">';
            html += '<span class="sum-bd-label"><span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + ';font-size:9px;padding:1px 6px">' + PA.esc(entries[i][0]) + '</span></span>';
            html += '<div class="sum-bd-bar-wrap"><div class="sum-bd-bar" style="width:' + pct + '%;background:' + colorObj.fg + '"></div></div>';
            html += '<span class="sum-bd-value">' + entries[i][1] + ' <small>(' + pct + '%)</small></span>';
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    _renderStmtBreakdown: function(map, total) {
        var entries = Object.entries(map).sort(function(a, b) { return b[1] - a[1]; });
        var html = '<div class="sum-breakdown">';
        var max = entries.length > 0 ? entries[0][1] : 1;
        for (var i = 0; i < entries.length; i++) {
            var pct = max > 0 ? Math.round((entries[i][1] / max) * 100) : 0;
            var color = PA.statements && PA.statements._getTypeColor ? PA.statements._getTypeColor(entries[i][0]) : 'var(--accent)';
            html += '<div class="sum-bd-row">';
            html += '<span class="sum-bd-label" style="font-family:var(--font-mono);font-size:10px">' + PA.esc(entries[i][0]) + '</span>';
            html += '<div class="sum-bd-bar-wrap"><div class="sum-bd-bar" style="width:' + pct + '%;background:' + color + '"></div></div>';
            html += '<span class="sum-bd-value">' + entries[i][1].toLocaleString() + '</span>';
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    _highlight: function(title, name, detail, nodeId) {
        var isEmpty = !name || name === '-' || name === '';
        var html = '<div class="sum-hl' + (isEmpty ? ' sum-hl-empty' : '') + '">';
        html += '<span class="sum-hl-title">' + PA.esc(title) + '</span>';
        if (isEmpty) {
            html += '<span class="sum-hl-name sum-hl-name-disabled">—</span>';
        } else {
            html += '<span class="sum-hl-name" onclick="PA.showProcedure(\'' + PA.escJs(nodeId || '') + '\')" title="' + PA.escAttr(name) + '" style="cursor:pointer">' + PA.esc(name) + '</span>';
        }
        html += '<span class="sum-hl-detail">' + PA.esc(isEmpty ? '—' : detail) + '</span>';
        html += '</div>';
        return html;
    }
};
