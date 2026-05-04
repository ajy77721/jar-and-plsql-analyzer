window.PA = window.PA || {};

PA.summary = {
    load: function() {
        PA.summary._render();
    },

    applyScope: function() {
        PA.summary._render();
    },

    _render: function() {
        var container = document.getElementById('summaryContainer');
        if (!container) return;
        var data = PA.analysisData;
        if (!data) { container.innerHTML = '<div class="empty-msg">No analysis data</div>'; return; }

        var nodes = PA.getScopedNodes ? PA.getScopedNodes() : (data.nodes || []);
        var scopeLabel = PA._scope || 'full';
        var stats = PA.summary._computeStats(nodes, data);
        stats.scopeLabel = scopeLabel;
        stats.scopedCount = nodes.length;
        stats.fullCount = (data.nodes || []).length;
        container.innerHTML = PA.summary._renderDashboard(stats);
    },

    _computeStats: function(nodes, data) {
        var totalLoc = 0, totalTables = 0, totalCalls = 0, totalCursors = 0;
        var totalParams = 0, totalVars = 0, totalExHandlers = 0, totalStmts = 0;
        var schemaMap = {}, typeMap = {}, stmtTypeMap = {};
        var maxLoc = { name: '', value: 0 };
        var maxDepth = { name: '', value: 0 };
        var maxTables = { name: '', value: 0 };
        var maxCalls = { name: '', value: 0 };
        var encryptedCount = 0;

        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            var c = n.counts || {};
            var loc = n.linesOfCode || 0;
            var tables = c.tables || 0;
            var calls = (c.internalCalls || 0) + (c.externalCalls || 0);
            var cursors = c.cursors || 0;
            var params = c.parameters || 0;
            var vars = c.variables || 0;
            var exh = c.exceptionHandlers || 0;

            totalLoc += loc;
            totalTables += tables;
            totalCalls += calls;
            totalCursors += cursors;
            totalParams += params;
            totalVars += vars;
            totalExHandlers += exh;

            if (n.readable === false) encryptedCount++;

            if (loc > maxLoc.value) { maxLoc = { name: n.name || n.nodeId || '', value: loc }; }
            if ((n.depth || 0) > maxDepth.value) { maxDepth = { name: n.name || n.nodeId || '', value: n.depth }; }
            if (tables > maxTables.value) { maxTables = { name: n.name || n.nodeId || '', value: tables }; }
            if (calls > maxCalls.value) { maxCalls = { name: n.name || n.nodeId || '', value: calls }; }

            var schema = n.schema || 'UNKNOWN';
            schemaMap[schema] = (schemaMap[schema] || 0) + 1;

            var rawType = (n.objectType || 'PROCEDURE').toUpperCase();
            var objType = rawType.includes('FUNC') ? 'FUNCTION' : 'PROCEDURE';
            typeMap[objType] = (typeMap[objType] || 0) + 1;

            if (c.statements) {
                var skeys = Object.keys(c.statements);
                for (var s = 0; s < skeys.length; s++) {
                    var stype = skeys[s];
                    var scount = c.statements[skeys[s]] || 0;
                    stmtTypeMap[stype] = (stmtTypeMap[stype] || 0) + scount;
                    totalStmts += scount;
                }
            }
        }

        return {
            entryPoint: data.entryPoint || data.name || '',
            totalNodes: nodes.length,
            totalLoc: totalLoc,
            totalTables: data.totalTables || totalTables,
            totalEdges: data.totalEdges || totalCalls,
            maxDepth: data.maxDepth || maxDepth.value,
            totalCursors: totalCursors,
            totalParams: totalParams,
            totalVars: totalVars,
            totalExHandlers: totalExHandlers,
            totalStmts: totalStmts,
            encryptedCount: encryptedCount,
            schemas: schemaMap,
            types: typeMap,
            stmtTypes: stmtTypeMap,
            maxLoc: maxLoc,
            maxDepthNode: maxDepth,
            maxTables: maxTables,
            maxCalls: maxCalls,
            errors: (data.errors || []).length
        };
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
        var html = '<div class="sum-hl">';
        html += '<span class="sum-hl-title">' + PA.esc(title) + '</span>';
        html += '<span class="sum-hl-name" onclick="PA.showProcedure(\'' + PA.escJs(nodeId || '') + '\')" style="cursor:pointer">' + PA.esc(name || '-') + '</span>';
        html += '<span class="sum-hl-detail">' + PA.esc(detail) + '</span>';
        html += '</div>';
        return html;
    }
};
