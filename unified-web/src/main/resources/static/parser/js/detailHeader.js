window.PA = window.PA || {};

PA._updateDetailHeader = async function(detail) {
    var header = document.getElementById('detailHeader');
    if (!detail) {
        if (header) header.style.display = 'none';
        if (PA.detailsTab && PA.detailsTab.updateLabels) PA.detailsTab.updateLabels(null);
        return;
    }
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

    // §267 — Claude-generated purpose summary, additive/optional. detail.purposeSummary is merged
    // in server-side by GET /analyses/{name}/node/{file} only once generation has run for this node
    // (see ClaudePurposeSummaryService); until then this stays hidden, never a placeholder string.
    var purposeEl = document.getElementById('dhPurposeSummary');
    if (purposeEl) {
        if (detail.purposeSummary) {
            purposeEl.textContent = detail.purposeSummary;
            purposeEl.title = detail.purposeSummary;
            purposeEl.style.display = '';
        } else {
            purposeEl.style.display = 'none';
            purposeEl.textContent = '';
        }
    }

    var meta = document.getElementById('dhMeta');
    if (meta) {
        var metaHtml = '';
        if (objType) {
            var typeCls = objType === 'FUNCTION' ? 'F' : objType === 'TRIGGER' ? 'T' : objType === 'PACKAGE' ? 'PKG' : 'P';
            var typeTooltip = typeCls === 'P' ? 'P=Procedure' : typeCls === 'F' ? 'F=Function' : typeCls === 'T' ? 'T=Trigger' : 'PKG=Package';
            metaHtml += '<span class="lp-icon ' + typeCls + '" style="display:inline-flex;width:auto;padding:0 4px;height:18px;font-size:9px" title="' + typeTooltip + '">' + typeCls + '</span> ';
            metaHtml += '<span class="dh-meta-item">' + PA.esc(objType) + '</span>';
        }
        if (detail.readable === false) metaHtml += '<span class="dh-meta-item" data-tip="Source encrypted" style="color:var(--badge-red)"><span class="ct-lock">&var(--badge-teal);</span> ENCRYPTED</span>';
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
    await PA._dhRenderStats();

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

    // Per-procedure case counts in the Details sub-tab labels ("Cursors (43)", …). The retired
    // "All cases" accordion panel used to render these counts; they live on the tab labels now.
    if (PA.detailsTab && PA.detailsTab.updateLabels) PA.detailsTab.updateLabels(detail);
};

/**
 * UI-024 — the four scanning branches below (`pkg_root`/`subflow`/`full`/the `subtree` fallback)
 * used to call seven different `PA.nodeIndex.all()`-scanning functions between them; all seven are
 * now one fetch to `DetailHeaderCountsService` (`PA.api.getDetailHeaderCounts`), matching each
 * branch's own scope: `pkg_root`/`full` both mean server `scope=full`, `subflow`/the bottom
 * `subtree` fallback map directly onto the server's own `subflow`/`subtree` semantics — the same
 * self-included/self-excluded distinction the old `includeSelf` parameter encoded. `direct` mode
 * needs no fetch at all: it already reads straight from `detail.counts`, exactly as before.
 */
PA._dhComputeStats = async function() {
    var detail = PA.currentDetail;
    if (!detail) return null;
    var mode = PA._scope;

    // Package root: show full package aggregates
    if (PA._isPackageRoot()) {
        var idx = PA.analysisData;
        var pkgCounts = await PA.api.getDetailHeaderCounts('full', null);
        return {
            mode: 'pkg_root',
            loc: idx ? (idx.totalLinesOfCode || 0) : 0,
            tables: idx ? (idx.totalTables || 0) : 0,
            totalCalls: pkgCounts.totalCalls,   // Call Edges = INT + EXT (consistent with every other scope)
            intCalls: pkgCounts.intCalls,
            extCalls: pkgCounts.extCalls,
            triggers: pkgCounts.triggers,
            cursors: pkgCounts.cursors,
            procs: idx ? (idx.totalNodes || 0) : 0
        };
    }

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
        var subflowCounts = await PA.api.getDetailHeaderCounts('subflow', PA._currentScopeId());
        return {
            mode: 'subflow',
            loc: counts.flowLinesOfCode || 0,
            tables: counts.subtreeTablesCount || 0,
            totalCalls: subflowCounts.totalCalls,
            intCalls: subflowCounts.intCalls,
            extCalls: subflowCounts.extCalls,
            triggers: subflowCounts.triggers,
            cursors: subflowCounts.cursors,
            procs: (counts.subtreeNodesCount || 0) + 1
        };
    }

    if (mode === 'full') {
        var idx = PA.analysisData;
        var fullCounts = await PA.api.getDetailHeaderCounts('full', null);
        return {
            mode: 'full',
            loc: idx ? (idx.totalLinesOfCode || 0) : 0,
            tables: idx ? (idx.totalTables || 0) : 0,
            totalCalls: fullCounts.totalCalls,   // Call Edges = INT + EXT (consistent across scopes; Full ≥ any subset)
            intCalls: fullCounts.intCalls,
            extCalls: fullCounts.extCalls,
            triggers: fullCounts.triggers,
            cursors: fullCounts.cursors,
            procs: idx ? (idx.totalNodes || 0) : 0
        };
    }

    var nodeCount = counts.subtreeNodesCount || 0;
    var nodeLoc = detail.linesOfCode || 0;
    var flowLoc = counts.flowLinesOfCode || 0;
    var subtreeCounts = await PA.api.getDetailHeaderCounts('subtree', PA._currentScopeId());
    return {
        mode: 'subtree',
        loc: flowLoc > nodeLoc ? flowLoc - nodeLoc : 0,
        tables: PA._dhUniqueTableCountSubtreeOnly(),
        totalCalls: subtreeCounts.totalCalls,
        intCalls: subtreeCounts.intCalls,
        extCalls: subtreeCounts.extCalls,
        triggers: subtreeCounts.triggers,
        cursors: subtreeCounts.cursors,
        procs: nodeCount
    };
};

/**
 * UI-024 — the seven functions this replaced (`_dhUniqueCallCount`/`_dhUniqueCallCountByType`/
 * `_dhFullCallCountByType`/`_dhFullTriggerCount`/`_dhFullCursorCount`/`_dhUniqueTriggerCount`/
 * `_dhUniqueCursorCount`) are gone entirely — each was its own `PA.nodeIndex.all()` scan; all seven
 * are now one `PA.api.getDetailHeaderCounts` call in `_dhComputeStats` above, backed by
 * `DetailHeaderCountsService` server-side.
 */
PA._dhUniqueTableCountSubtreeOnly = function() {
    var detail = PA.currentDetail;
    if (!detail) return 0;
    var counts = detail.counts || {};
    var totalFlow = counts.subtreeTablesCount || 0;
    var nodeTables = counts.tables || (detail.tables ? detail.tables.length : 0);
    return totalFlow > nodeTables ? totalFlow - nodeTables : totalFlow;
};

PA._dhRenderStats = async function() {
    var statsEl = document.getElementById('dhStats');
    if (!statsEl) return;
    var s = await PA._dhComputeStats();
    if (!s) return;

    // Truthful breakdown: where the analysis index carries per-type node counts (typeCounts) and
    // the tables index per-kind counts (tableKindCounts), show procedures+functions / triggers /
    // tables / views separately instead of labelling the raw node total "Procs".
    var idx0 = PA.analysisData || {};
    var fullScope = (s.mode === 'full' || s.mode === 'pkg_root');
    var tc = idx0.typeCounts || {};
    var hasTypes = fullScope && (tc.PROCEDURE != null || tc.FUNCTION != null);
    var procsFuncs = hasTypes ? ((tc.PROCEDURE || 0) + (tc.FUNCTION || 0)) : s.procs;
    var trigCount = hasTypes ? (tc.TRIGGER || 0) : (s.triggers || 0);
    var kc = idx0.tableKindCounts || {};
    var hasKinds = fullScope && (typeof kc.TABLE === 'number');
    var realTables = hasKinds ? kc.TABLE : s.tables;
    var viewCount = hasKinds ? ((kc.VIEW || 0) + (kc.MATERIALIZED_VIEW || 0)) : null;

    var html = '';
    html += PA._dhStatBox(procsFuncs, 'Procs', 'accent', 'Procedures and functions' + (hasTypes ? ' (excludes triggers, tables and views)' : ' analyzed in the call tree'));
    html += PA._dhStatBox(s.loc.toLocaleString(), 'LOC', '', 'Total lines of source code across all procedures');
    html += PA._dhStatBox(s.totalCalls, 'Call Edges', 'blue', 'Unique caller→callee pairs in the dependency graph');
    html += PA._dhStatBox(s.intCalls, 'INT', 'blue', 'Internal calls: calls to procedures within the same package');
    html += PA._dhStatBox(s.extCalls, 'EXT', 'orange', 'External calls: calls to procedures in other packages');
    html += PA._dhStatBox(realTables, 'Tables', 'teal', 'Persistent database tables accessed by all procedures');
    html += PA._dhStatBox(trigCount, 'Triggers', 'orange', 'Triggers in the analysis');
    if (viewCount != null) html += PA._dhStatBox(viewCount, 'Views', 'blue', 'Views and materialized views');

    statsEl.innerHTML = html;

    var totalsEl = document.getElementById('dhTotals');
    if (totalsEl) {
        var idx = PA.analysisData;
        if (idx && s.mode !== 'direct') {
            var tcx = idx.typeCounts || {};
            var pf = (tcx.PROCEDURE != null || tcx.FUNCTION != null) ? ((tcx.PROCEDURE || 0) + (tcx.FUNCTION || 0)) : null;
            var tr = (tcx.TRIGGER != null) ? tcx.TRIGGER : null;
            var kcx = idx.tableKindCounts || {};
            var tb = (typeof kcx.TABLE === 'number') ? kcx.TABLE : null;
            var vw = (typeof kcx.VIEW === 'number') ? ((kcx.VIEW || 0) + (kcx.MATERIALIZED_VIEW || 0)) : null;
            var th = '<span class="dh-totals-label" title="Aggregate counts across the entire analysis">Analysis Totals</span>';
            th += '<span class="dh-totals-item accent" title="Procedures and functions">' + (pf != null ? pf : (idx.totalNodes || 0)) + '<small>procs</small></span>';
            if (tr != null) th += '<span class="dh-totals-item orange" title="Triggers">' + tr + '<small>triggers</small></span>';
            th += '<span class="dh-totals-item teal" title="Persistent database tables">' + (tb != null ? tb : (idx.totalTables || 0)) + '<small>tables</small></span>';
            if (vw != null) th += '<span class="dh-totals-item blue" title="Views and materialized views">' + vw + '<small>views</small></span>';
            th += '<span class="dh-totals-item" title="Total lines of source code">' + (idx.totalLinesOfCode || 0).toLocaleString() + '<small>LOC</small></span>';
            th += '<span class="dh-totals-item" title="Unique caller→callee pairs in the dependency graph">' + (idx.uniqueEdges || idx.totalEdges || 0) + '<small>edges</small></span>';
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
                // Generic per-construct breakdown — iterates ALL captured statement types the same
                // way for every dialect (IF/CASE/loops/SUBQUERY/CTE/MERGE/dynamic/…), so newly
                // captured constructs are visible with no per-type/per-dialect code.
                var CONSTRUCT_ORDER = ['IF','CASE','WHILE_LOOP','FOR_LOOP','CURSOR_FOR_LOOP','LOOP','REPEAT',
                    'SUBQUERY','CTE','MERGE','UPSERT','EXECUTE_IMMEDIATE','FORALL','TRY_CATCH'];
                var shown = {};
                CONSTRUCT_ORDER.forEach(function(k){
                    if (stmts[k]) { ih += '<span class="dh-info-badge">' + PA.esc(k.replace(/_/g,' ')) + ': ' + stmts[k] + '</span>'; shown[k] = 1; }
                });
                // any other captured type not in the curated order (kept generic, nothing hidden)
                keys.forEach(function(k){
                    var up = (k||'').toUpperCase();
                    if (shown[up]) return;
                    if (['SELECT','INSERT','UPDATE','DELETE','ASSIGNMENT','OPEN_CURSOR','CLOSE_CURSOR',
                         'FETCH','RETURN','SET','DECLARE','BLOCK','OTHER','COMMIT','ROLLBACK','SAVEPOINT',
                         'BEGIN_TRAN','CURSOR_ATTRIBUTE','EXIT','RAISE','THROW','PIPE_ROW','TRUNCATE','DROP',
                         'CREATE','DEALLOCATE','EXEC','OPEN','CLOSE','WITH'].indexOf(up) >= 0) return;
                    if (stmts[k]) ih += '<span class="dh-info-badge">' + PA.esc(String(k).replace(/_/g,' ')) + ': ' + stmts[k] + '</span>';
                });
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

PA.onScopeChange(function() { PA._dhRenderStats(); });

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
