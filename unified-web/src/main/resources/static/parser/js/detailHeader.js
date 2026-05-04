window.PA = window.PA || {};

PA._updateDetailHeader = function(detail) {
    var header = document.getElementById('detailHeader');
    if (!detail) { if (header) header.style.display = 'none'; return; }
    header.style.display = '';

    var schema = detail.schema || '';
    var name = detail.name || detail.nodeId || '';
    var objType = detail.objectType || '';
    var colorObj = PA.getSchemaColor(schema);

    var schemaBadge = document.getElementById('dhSchema');
    if (schemaBadge) {
        schemaBadge.textContent = schema || '?';
        schemaBadge.style.background = colorObj.bg;
        schemaBadge.style.color = colorObj.fg;
        schemaBadge.setAttribute('data-tip', 'Owner schema');
        schemaBadge.setAttribute('title', 'Oracle schema that owns this object');
    }
    var fullNameEl = document.getElementById('dhFullName');
    if (fullNameEl) fullNameEl.textContent = schema ? schema + '.' + name : name;

    var meta = document.getElementById('dhMeta');
    if (meta) {
        var metaHtml = '';
        if (objType) {
            var typeCls = objType === 'FUNCTION' ? 'F' : objType === 'TRIGGER' ? 'T' : 'P';
            var typeTooltip = typeCls === 'P' ? 'P=Procedure' : typeCls === 'F' ? 'F=Function' : 'T=Trigger';
            metaHtml += '<span class="lp-icon ' + typeCls + '" style="display:inline-flex;width:18px;height:18px;font-size:9px" title="' + typeTooltip + '">' + typeCls + '</span> ';
            metaHtml += '<span class="dh-meta-item">' + PA.esc(objType) + '</span>';
        }
        if (detail.readable === false) metaHtml += '<span class="dh-meta-item" data-tip="Source encrypted" style="color:var(--badge-red)"><span class="ct-lock">&#128274;</span> ENCRYPTED</span>';
        if (detail.depth != null) metaHtml += '<span class="dh-meta-item" data-tip="Call depth level"><span class="dh-meta-label">D</span>' + detail.depth + '</span>';
        if (detail.lineStart) {
            var lineRange = detail.lineEnd ? detail.lineStart + '-' + detail.lineEnd : '' + detail.lineStart;
            metaHtml += '<span class="dh-meta-item" data-tip="Source line range"><span class="dh-meta-label">L</span>' + lineRange + '</span>';
        }
        if (detail.sourceFile) {
            metaHtml += '<span class="dh-meta-item" style="cursor:pointer" onclick="PA.sourceView.openModal(\'' + PA.escJs(detail.nodeId || '') + '\')" data-tip="Click to view source"><span style="color:var(--accent);text-decoration:underline">' + PA.esc(detail.sourceFile) + '</span></span>';
        }
        meta.innerHTML = metaHtml;
    }

    PA.renderScopeToggle('dhScopeToggle');
    PA._dhRenderStats();

    var badgesEl = document.getElementById('dhBadges');
    if (badgesEl) {
        var ops = PA._collectOperations(detail);
        var bHtml = '';
        for (var j = 0; j < ops.length; j++) {
            bHtml += '<span class="dh-op-badge ' + PA.esc(ops[j]) + '">' + PA.esc(ops[j]) + '</span>';
        }
        badgesEl.innerHTML = bHtml;
    }

    PA._updateNavBar(detail);
};

PA._dhComputeStats = function() {
    var detail = PA.currentDetail;
    if (!detail) return null;
    var mode = PA._scope;
    var counts = detail.counts || {};

    if (mode === 'direct') {
        return {
            mode: 'direct',
            loc: detail.linesOfCode || 0,
            tables: counts.tables || (detail.tables ? detail.tables.length : 0),
            totalCalls: (counts.internalCalls || 0) + (counts.externalCalls || 0),
            intCalls: counts.internalCalls || 0,
            extCalls: counts.externalCalls || 0,
            triggers: detail.objectType === 'TRIGGER' ? 1 : 0,
            cursors: counts.cursors || (detail.cursors ? detail.cursors.length : 0),
            params: counts.parameters || (detail.parameters ? detail.parameters.length : 0),
            vars: detail.variables ? (detail.variables.total || 0) : (counts.variables || 0),
            exHandlers: counts.exceptionHandlers || (detail.exceptionHandlers ? detail.exceptionHandlers.length : 0),
            procs: 1
        };
    }

    if (mode === 'subflow') {
        return {
            mode: 'subflow',
            loc: counts.flowLinesOfCode || 0,
            tables: counts.subtreeTablesCount || 0,
            totalCalls: PA._dhUniqueCallCount(true),
            intCalls: PA._dhUniqueCallCountByType('internal', true),
            extCalls: PA._dhUniqueCallCountByType('external', true),
            triggers: PA._dhUniqueTriggerCount(true),
            cursors: PA._dhUniqueCursorCount(true),
            procs: (counts.subtreeNodesCount || 0) + 1
        };
    }

    if (mode === 'full') {
        var idx = PA.analysisData;
        return {
            mode: 'full',
            loc: idx ? (idx.totalLinesOfCode || 0) : 0,
            tables: idx ? (idx.totalTables || 0) : 0,
            totalCalls: idx ? (idx.totalEdges || 0) : 0,
            intCalls: PA._dhFullCallCountByType('internal'),
            extCalls: PA._dhFullCallCountByType('external'),
            triggers: PA._dhFullTriggerCount(),
            cursors: PA._dhFullCursorCount(),
            procs: idx ? (idx.totalNodes || 0) : 0
        };
    }

    var nodeCount = counts.subtreeNodesCount || 0;
    var nodeLoc = detail.linesOfCode || 0;
    var flowLoc = counts.flowLinesOfCode || 0;
    return {
        mode: 'subtree',
        loc: flowLoc > nodeLoc ? flowLoc - nodeLoc : 0,
        tables: PA._dhUniqueTableCountSubtreeOnly(),
        totalCalls: PA._dhUniqueCallCount(false),
        intCalls: PA._dhUniqueCallCountByType('internal', false),
        extCalls: PA._dhUniqueCallCountByType('external', false),
        triggers: PA._dhUniqueTriggerCount(false),
        cursors: PA._dhUniqueCursorCount(false),
        procs: nodeCount
    };
};

PA._dhUniqueCallCount = function(includeSelf) {
    var nodes = PA.analysisData ? PA.analysisData.nodes : [];
    var treeIds = PA.context.callTreeNodeIds;
    var currentId = (PA.context.procId || '').toUpperCase();
    var total = 0;
    for (var i = 0; i < nodes.length; i++) {
        var n = nodes[i];
        var nid = (n.nodeId || n.name || '').toUpperCase();
        if (!treeIds || !treeIds.has(nid)) continue;
        if (!includeSelf && nid === currentId) continue;
        var nc = n.counts || {};
        total += (nc.internalCalls || 0) + (nc.externalCalls || 0);
    }
    return total;
};

PA._dhUniqueCallCountByType = function(type, includeSelf) {
    var nodes = PA.analysisData ? PA.analysisData.nodes : [];
    var treeIds = PA.context.callTreeNodeIds;
    var currentId = (PA.context.procId || '').toUpperCase();
    var total = 0;
    for (var i = 0; i < nodes.length; i++) {
        var n = nodes[i];
        var nid = (n.nodeId || n.name || '').toUpperCase();
        if (!treeIds || !treeIds.has(nid)) continue;
        if (!includeSelf && nid === currentId) continue;
        var nc = n.counts || {};
        total += type === 'internal' ? (nc.internalCalls || 0) : (nc.externalCalls || 0);
    }
    return total;
};

PA._dhFullCallCountByType = function(type) {
    var nodes = PA.analysisData ? PA.analysisData.nodes : [];
    var total = 0;
    for (var i = 0; i < nodes.length; i++) {
        var nc = nodes[i].counts || {};
        total += type === 'internal' ? (nc.internalCalls || 0) : (nc.externalCalls || 0);
    }
    return total;
};

PA._dhFullTriggerCount = function() {
    var nodes = PA.analysisData ? PA.analysisData.nodes : [];
    var count = 0;
    for (var i = 0; i < nodes.length; i++) {
        if (nodes[i].objectType === 'TRIGGER') count++;
    }
    return count;
};

PA._dhFullCursorCount = function() {
    var nodes = PA.analysisData ? PA.analysisData.nodes : [];
    var total = 0;
    for (var i = 0; i < nodes.length; i++) {
        total += (nodes[i].counts ? nodes[i].counts.cursors : 0) || 0;
    }
    return total;
};

PA._dhUniqueTriggerCount = function(includeSelf) {
    var nodes = PA.analysisData ? PA.analysisData.nodes : [];
    var treeIds = PA.context.callTreeNodeIds;
    var currentId = (PA.context.procId || '').toUpperCase();
    var count = 0;
    for (var i = 0; i < nodes.length; i++) {
        var n = nodes[i];
        var nid = (n.nodeId || n.name || '').toUpperCase();
        if (!treeIds || !treeIds.has(nid)) continue;
        if (!includeSelf && nid === currentId) continue;
        if (n.objectType === 'TRIGGER') count++;
    }
    return count;
};

PA._dhUniqueCursorCount = function(includeSelf) {
    var nodes = PA.analysisData ? PA.analysisData.nodes : [];
    var treeIds = PA.context.callTreeNodeIds;
    var currentId = (PA.context.procId || '').toUpperCase();
    var total = 0;
    for (var i = 0; i < nodes.length; i++) {
        var n = nodes[i];
        var nid = (n.nodeId || n.name || '').toUpperCase();
        if (!treeIds || !treeIds.has(nid)) continue;
        if (!includeSelf && nid === currentId) continue;
        total += (n.counts ? n.counts.cursors : 0) || 0;
    }
    return total;
};

PA._dhUniqueTableCountSubtreeOnly = function() {
    var detail = PA.currentDetail;
    if (!detail) return 0;
    var counts = detail.counts || {};
    var totalFlow = counts.subtreeTablesCount || 0;
    var nodeTables = counts.tables || (detail.tables ? detail.tables.length : 0);
    return totalFlow > nodeTables ? totalFlow - nodeTables : totalFlow;
};

PA._dhRenderStats = function() {
    var statsEl = document.getElementById('dhStats');
    if (!statsEl) return;
    var s = PA._dhComputeStats();
    if (!s) return;

    var html = '';
    html += PA._dhStatBox(s.procs, 'Procs', 'accent', 'Total procedures and functions analyzed in the call tree');
    html += PA._dhStatBox(s.loc.toLocaleString(), 'LOC', '', 'Total lines of PL/SQL source code across all procedures');
    html += PA._dhStatBox(s.totalCalls, 'Dependencies', 'blue', 'Total call edges between procedures in the dependency graph');
    html += PA._dhStatBox(s.intCalls, 'INT', 'blue', 'Internal calls: calls to procedures within the same package');
    html += PA._dhStatBox(s.extCalls, 'EXT', 'orange', 'External calls: calls to procedures in other packages');
    html += PA._dhStatBox(s.tables, 'Tables', 'teal', 'Total unique database tables accessed by all procedures');

    statsEl.innerHTML = html;

    var totalsEl = document.getElementById('dhTotals');
    if (totalsEl) {
        var idx = PA.analysisData;
        if (idx && s.mode !== 'direct') {
            var th = '<span class="dh-totals-label" title="Aggregate counts across the entire analysis">Analysis Totals</span>';
            th += '<span class="dh-totals-item accent" title="Total procedures and functions in the full analysis">' + (idx.totalNodes || 0) + '<small>procs</small></span>';
            th += '<span class="dh-totals-item teal" title="Total unique database tables accessed">' + (idx.totalTables || 0) + '<small>tables</small></span>';
            th += '<span class="dh-totals-item" title="Total lines of PL/SQL source code">' + (idx.totalLinesOfCode || 0).toLocaleString() + '<small>LOC</small></span>';
            th += '<span class="dh-totals-item" title="Total call edges in the dependency graph">' + (idx.totalEdges || 0) + '<small>edges</small></span>';
            totalsEl.innerHTML = th;
            totalsEl.style.display = '';
        } else {
            totalsEl.style.display = 'none';
        }
    }

    var infoEl = document.getElementById('dhInfo');
    if (infoEl) {
        var detail = PA.currentDetail;
        if (detail && s.mode === 'direct') {
            var ih = '';
            if (detail.packageName) ih += '<span class="dh-info-badge">PKG: ' + PA.esc(detail.packageName) + '</span>';
            if (detail.depth != null) ih += '<span class="dh-info-badge">Depth ' + detail.depth + '</span>';
            if (detail.readable === false) ih += '<span class="dh-info-badge orange">ENCRYPTED</span>';
            var stmts = detail.statementSummary;
            if (stmts) {
                var total = 0;
                var keys = Object.keys(stmts);
                for (var sk = 0; sk < keys.length; sk++) total += stmts[keys[sk]] || 0;
                if (total > 0) ih += '<span class="dh-info-badge">' + total + ' stmts</span>';
            }
            infoEl.innerHTML = ih;
            infoEl.style.display = ih ? '' : 'none';
        } else {
            infoEl.style.display = 'none';
        }
    }
};

PA._dhStatBox = function(value, label, colorClass, tooltip) {
    return '<div class="dh-stat' + (colorClass ? ' ' + colorClass : '') + '"' + (tooltip ? ' title="' + PA.escAttr(tooltip) + '"' : '') + '><span class="dh-stat-value">' + value + '</span><span class="dh-stat-label">' + label + '</span></div>';
};

PA._updateNavBar = function(detail) {
    var navEl = document.getElementById('dhNav');
    if (!navEl) return;
    if (!detail || !detail.nodeId) { navEl.style.display = 'none'; return; }

    var callTree = PA.callTrace.treeData;
    var calls = (callTree && callTree.children) ? callTree.children.slice(0, 8) : [];
    if (!calls.length) { navEl.style.display = 'none'; return; }

    navEl.style.display = '';
    var html = '<span class="dh-nav-current">' + PA.esc(detail.name || '') + '</span>';
    html += '<span class="dh-nav-sep">&#8594;</span>';
    for (var i = 0; i < calls.length; i++) {
        var ch = calls[i];
        var ct = ch.callType || '';
        var badge = ct === 'INTERNAL' ? '<span class="dh-nav-badge int">INT</span>' : ct ? '<span class="dh-nav-badge ext">' + PA.esc(ct) + '</span>' : '';
        html += '<span class="dh-nav-item call" onclick="PA.showProcedure(\'' + PA.escJs(ch.id || '') + '\')">' + badge + PA.esc(ch.name || ch.id || '') + '</span>';
    }
    if (callTree.children.length > 8) {
        html += '<span class="dh-nav-more">+' + (callTree.children.length - 8) + ' more</span>';
    }
    navEl.innerHTML = html;
};

PA._collectOperations = function(detail) {
    var ops = new Set();
    if (detail.tables) {
        for (var i = 0; i < detail.tables.length; i++) {
            var t = detail.tables[i];
            if (t.operations) {
                var opKeys = Object.keys(t.operations);
                for (var k = 0; k < opKeys.length; k++) ops.add(opKeys[k].toUpperCase());
            }
        }
    }
    if (detail.statementSummary) {
        var ss = detail.statementSummary;
        ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'].forEach(function(op) {
            if (ss[op] || ss[op.toLowerCase()]) ops.add(op);
        });
    }
    return Array.from(ops).sort();
};
