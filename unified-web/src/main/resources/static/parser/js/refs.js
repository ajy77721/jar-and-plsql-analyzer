window.PA = window.PA || {};

PA.refs = {
    currentProcId: null,
    currentTab: 'calls',
    callsItems: [],
    callersItems: [],
    subtreeCallsItems: [],
    tableItems: [],
    subtreeTableItems: [],
    fullCallsItems: [],
    fullTableItems: [],
    _loaded: false,
    _activeScopeTypes: new Set(),

    switchTab: function(tab) {
        PA.refs.currentTab = tab;
        document.querySelectorAll('.refs-tab').forEach(function(t) { t.classList.remove('active'); });
        var btn = document.querySelector('.refs-tab[data-ref="' + tab + '"]');
        if (btn) btn.classList.add('active');
        PA.refs._activeScopeTypes.clear();
        document.querySelectorAll('#refScopePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.refs._buildTable();
    },

    load: async function(procId) {
        if (PA.refs._loaded && PA.refs.currentProcId === procId) return;
        PA.refs.currentProcId = procId;
        PA.refs.callsItems = [];
        PA.refs.callersItems = [];
        PA.refs._loaded = false;
        var container = document.getElementById('refsContainer');
        container.innerHTML = '<div class="empty-msg">Loading references...</div>';
        try {
            await PA.refs._buildCallsAndCallers(procId);
            PA.refs._loaded = true;
        } catch (e) { console.warn('[PA] refs.load failed', e); }
        PA.refs._initScopePills();
        PA.refs._buildTable();
    },

    /** Re-runs load() for the current procedure; backs the Retry link on the error state. */
    reload: function() {
        PA.refs._loaded = false;
        return PA.refs.load(PA.refs.currentProcId);
    },

    /**
     * UI-024/refs.js bug fix (user decision, this session): the "Calls" tab was showing every
     * procedure in the whole analysis via `PA.nodeIndex.all()` — a directory, not this procedure's
     * actual calls. Fixed by fetching this node's own call tree/caller tree (`PA.api.getCallTree`/
     * `getCallerTree`, both already used elsewhere — `callGraph.js`/`sourceView.js`) and reading
     * only the direct (depth-1) children, the same shape `sourceView.js`'s own caller list already
     * unwraps (`callerData.children`, or the response itself when it's already an array).
     *
     * <p>Deliberately does not touch `PA.nodeIndex` at all — unlike the old code, this fix does not
     * perpetuate the full-residency dependency UI-024 is about; it only ever asks the server for
     * one node's own edges.
     */
    _buildCallsAndCallers: async function(procId) {
        var callsItems = [];
        var callersItems = [];
        // One side failing still must not take the other down — but an empty list from a failed
        // fetch is NOT the same as a procedure that really calls nothing, and rendering both as
        // "No procedures found" is what hid these failures. The error is kept per side and shown.
        PA.refs.callsError = null;
        PA.refs.callersError = null;
        try {
            // maxDepth=1 — this view only ever reads the DIRECT (depth-1) children below, so
            // asking for the whole subtree was both wasteful and, worse, WRONG: the server caps a
            // call tree at MAX_CALL_TREE_NODES (2000) and spends that budget depth-first, so on a
            // wide/deep root the budget runs out before the root's own remaining direct children
            // are ever built. Measured on the loaded analyses: ERP$ERP_USP_RUN_CYCLE showed 13 of
            // its 18 callees on Postgres and 4 of 19 on Oracle — silently, with no truncation
            // marker, because the cut happened deeper in the tree and the missing direct children
            // simply never appeared. With maxDepth=1 the root's children are all that gets built,
            // the budget is never close to spent, and the list is exact (18/18 and 19/19, all 48
            // and 210 call sites respectively).
            var callTree = await PA.api.getCallTree(procId, 1);
            // getCallTree returns null and getCallerTree returns the PA.FAILED sentinel on a
            // non-ok response rather than throwing (page-envelope.js). Both look like "no edges"
            // to the code below, so test for them explicitly.
            if (callTree === null || (PA.failed && PA.failed(callTree))) throw new Error('call-tree request failed');
            var callChildren = Array.isArray(callTree) ? callTree : (callTree && callTree.children ? callTree.children : []);
            callsItems = PA.refs._dedup(callChildren);
        } catch (e) {
            console.error('[refs] call tree failed', e);
            PA.refs.callsError = (e && e.message) ? e.message : String(e);
        }
        try {
            var callerData = await PA.api.getCallerTree(procId);
            if (callerData === null || (PA.failed && PA.failed(callerData))) throw new Error('caller-tree request failed');
            var callerChildren = Array.isArray(callerData) ? callerData : (callerData && callerData.children ? callerData.children : []);
            callersItems = PA.refs._dedup(callerChildren);
        } catch (e) {
            console.error('[refs] caller tree failed', e);
            PA.refs.callersError = (e && e.message) ? e.message : String(e);
        }
        PA.refs.callsItems = callsItems;
        PA.refs.callersItems = callersItems;
    },

    /** The failure, if any, behind an empty list on the currently selected sub-tab. */
    _getErrorForScope: function() {
        return PA.refs.currentTab === 'callers' ? PA.refs.callersError : PA.refs.callsError;
    },

    _getItemsForScope: function() {
        return PA.refs.currentTab === 'callers' ? PA.refs.callersItems : PA.refs.callsItems;
    },

    /** Extract package and proc/func name from a node object. */
    _parsePkgProc: function(node) {
        var pkg = node.packageName || '';
        var name = node.name || node.id || node.nodeId || '';
        var proc = name;
        if (pkg) {
            // name may be "PKG.PROC" — strip the package prefix
            var dotIdx = name.indexOf('.');
            if (dotIdx >= 0) proc = name.substring(dotIdx + 1);
        } else {
            // No explicit packageName — try to derive from dotted name
            var parts = name.split('.');
            if (parts.length >= 2) {
                pkg = parts.slice(0, parts.length - 1).join('.');
                proc = parts[parts.length - 1];
            }
        }
        return { pkg: pkg, proc: proc };
    },

    // UI-024 (§131): `_assignCallTypes` removed — confirmed dead code, no caller anywhere in the
    // codebase (a full-file grep for the name found only its own declaration). Its own
    // `PA._findNodeInData` (nodeIndex) read never actually ran.

    _dedup: function(children) {
        var map = new Map();
        for (var i = 0; i < children.length; i++) {
            var child = children[i];
            var id = (child.id || child.nodeId || child.name || '').toUpperCase();
            if (!id) continue;
            if (map.has(id)) {
                var e = map.get(id);
                e.count++;
                if (child.callLineNumber) e.lines.push(child.callLineNumber);
                if (child.callLines) {
                    for (var k = 0; k < child.callLines.length; k++) e.lines.push(child.callLines[k]);
                }
            } else {
                var initLines = [];
                if (child.callLineNumber) initLines.push(child.callLineNumber);
                if (child.callLines) {
                    for (var k = 0; k < child.callLines.length; k++) initLines.push(child.callLines[k]);
                }
                map.set(id, { node: child, count: 1, lines: initLines });
            }
        }
        return Array.from(map.values());
    },

    _initScopePills: function() {
        var container = document.getElementById('refScopePills');
        if (!container) return;
        PA.refs._activeScopeTypes = new Set();
        var html = '<span class="op-filter-pill active" data-rs="INTERNAL" onclick="PA.refs.toggleScope(\'INTERNAL\')">INT</span>';
        html += '<span class="op-filter-pill active" data-rs="EXTERNAL" onclick="PA.refs.toggleScope(\'EXTERNAL\')">EXT</span>';
        container.innerHTML = html;
    },

    toggleScope: function(scope) {
        var pill = document.querySelector('#refScopePills .op-filter-pill[data-rs="' + scope + '"]');
        if (!pill) return;
        if (PA.refs._activeScopeTypes.size === 0) {
            PA.refs._activeScopeTypes.add(scope);
            document.querySelectorAll('#refScopePills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.rs === scope);
            });
        } else if (PA.refs._activeScopeTypes.has(scope)) {
            PA.refs._activeScopeTypes.delete(scope);
            pill.classList.remove('active');
            if (PA.refs._activeScopeTypes.size === 0) {
                document.querySelectorAll('#refScopePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.refs._activeScopeTypes.add(scope);
            pill.classList.add('active');
        }
        PA.tf.filter('ref');
    },

    _buildTable: function() {
        var items = PA.refs._getItemsForScope();
        var container = document.getElementById('refsContainer');
        if (!container) return;

        PA.refs._updateBadgeCounts();

        if (!items.length) {
            var err = PA.refs._getErrorForScope();
            container.innerHTML = err
                ? '<div class="empty-msg" style="color:var(--red,#c0392b);font-weight:600">'
                    + '&#9888; Failed to load &mdash; ' + PA.esc(err)
                    + ' <a href="#" onclick="PA.refs.reload();return false">Retry</a></div>'
                : '<div class="empty-msg">No procedures found</div>';
            return;
        }

        var pp = PA.refs._parsePkgProc;
        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'ref\',0)">Schema</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'ref\',1)">Procedure</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'ref\',2)">Type</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'ref\',3)">Scope</th>';
        html += '<th>Line(s)</th>';
        html += '</tr></thead><tbody id="ref-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('ref', items, 50, PA.refs._renderRow, {
            sortKeys: {
                0: { fn: function(it) { return (it.node.schemaName || it.node.schema || '').toUpperCase(); } },
                1: { fn: function(it) { return (it.node.name || '').toUpperCase(); } },
                2: { fn: function(it) { return (it.node.unitType || it.node.objectType || '').toUpperCase(); } },
                3: { fn: function(it) { return (it.node.callType || '').toUpperCase(); } }
            },
            searchFn: function(it, q) {
                var n = it.node;
                var text = ((n.schemaName || n.schema || '') + ' ' + (n.name || '') + ' ' + (n.callType || '')).toUpperCase();
                return text.includes(q);
            },
            extraFilter: PA.refs._scopeFilter,
            onFilter: PA.refs._updateBadgeCounts
        });

        var st = PA.tf.state('ref');
        if (st) { st.sortCol = 1; st.sortDir = 'asc'; }
        PA.tf.filter('ref');

        setTimeout(function() {
            PA.tf.initColFilters('ref', {
                0: { label: 'Schema', valueFn: function(it) { return it.node.schemaName || it.node.schema || '-'; } },
                1: { label: 'Procedure', valueFn: function(it) { return it.node.name || '-'; } },
                2: { label: 'Type', valueFn: function(it) { return it.node.unitType || it.node.objectType || 'PROCEDURE'; } },
                3: { label: 'Scope', valueFn: function(it) { return it.node.callType || '-'; } }
            });
            PA.tf._updateSortIndicators('ref');
        }, 0);
    },

    _scopeFilter: function(it) {
        var types = PA.refs._activeScopeTypes;
        if (types.size === 0) return true;
        var ct = (it.node.callType || '').toUpperCase();
        if (types.has('INTERNAL') && ct === 'INTERNAL') return true;
        if (types.has('EXTERNAL') && ct !== 'INTERNAL' && ct !== 'ROOT' && ct !== '' && ct !== 'TABLE') return true;
        return false;
    },

    _renderRow: function(item, idx, esc) {
        var n = item.node;
        var schema = n.schemaName || n.schema || '';
        var name = n.name || '?';
        var unitType = n.unitType || n.objectType || 'PROCEDURE';
        var callType = n.callType || '';
        var colorObj = PA.getSchemaColor(schema);

        var badgeLetter = { FUNCTION: 'F', PROCEDURE: 'P', TRIGGER: 'T', VIEW: 'V', MATERIALIZED_VIEW: 'MV', SEQUENCE: 'SQ', INDEX: 'IX', TYPE: 'TY', TYPE_BODY: 'TB', DOMAIN: 'DM', ALTER_TABLE: 'AT' }[unitType] || 'P';
        var typeBadge = '<span class="lp-icon ' + badgeLetter.charAt(0) + '" style="display:inline-flex;width:18px;height:18px;font-size:9px">' + badgeLetter + '</span>';
        var scopeBadge = callType === 'INTERNAL'
            ? '<span class="scope-badge int">INT</span>'
            : callType && callType !== 'ROOT' ? '<span class="scope-badge ext">' + esc(callType) + '</span>' : '';

        var html = '<tr class="to-row">';
        if (schema) html += '<td><span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + '">' + esc(schema) + '</span></td>';
        else html += '<td style="color:var(--text-muted);font-size:11px">-</td>';
        var schedBadge = n.scheduled
            ? ' <span class="scope-badge" title="This procedure is run by a scheduled job" style="background:var(--badge-purple-bg);color:var(--badge-purple)">&#9200; scheduled</span>'
            : '';
        html += '<td><span class="to-detail-proc" onclick="PA.showProcedure(\'' + PA.escJs(n.id || n.nodeId || name) + '\')">' + esc(name) + '</span>' + schedBadge + '</td>';
        html += '<td>' + typeBadge + ' <span style="font-size:11px">' + esc(unitType) + '</span></td>';
        html += '<td>' + scopeBadge + '</td>';
        // Line(s) — its own column now, separate from Procedure. The call happens in the
        // CURRENT procedure's source, so jump there at the exact line (not into the callee's
        // file). One line: a single inline badge, matching the Accesses tab's `.td-line`
        // convention. Multiple (the ×N grouping case): reuse the Hierarchy tab's `.ct-callsites`
        // flex-wrap chip row so many badges in one cell wrap instead of overrunning the column.
        var lines = (item.lines || []).filter(function(v, i, a) { return v && a.indexOf(v) === i; }).sort(function(x, y) { return x - y; });
        var curId = PA.context.procId;
        html += '<td>';
        if (lines.length === 1) {
            html += '<span class="to-detail-line" data-tip="Open call site at line" onclick="event.stopPropagation(); PA.sourceView.openNodeAtLine(\'' + PA.escJs(curId) + '\', ' + lines[0] + ')">L' + lines[0] + '</span>';
        } else if (lines.length > 1) {
            html += '<div class="ct-callsites" style="padding:0">';
            for (var li = 0; li < lines.length; li++) {
                html += '<span class="to-detail-line" data-tip="Open call site at line" onclick="event.stopPropagation(); PA.sourceView.openNodeAtLine(\'' + PA.escJs(curId) + '\', ' + lines[li] + ')">L' + lines[li] + '</span>';
            }
            html += '</div>';
        } else {
            html += '<span style="color:var(--text-muted);font-size:11px">-</span>';
        }
        html += '</td>';
        html += '</tr>';
        return html;
    },

    _updateBadgeCounts: function() {
        var allItems = PA.refs._getItemsForScope();
        var countEl = document.getElementById('refsTotalCount');
        if (countEl) {
            var intCount = allItems.filter(function(it) { return (it.node.callType || '') === 'INTERNAL'; }).length;
            var extCount = allItems.filter(function(it) { var ct = it.node.callType || ''; return ct !== 'INTERNAL' && ct !== 'ROOT' && ct !== ''; }).length;
            countEl.innerHTML = '<span>' + allItems.length + ' procs</span>' +
                (intCount > 0 ? ' <span class="scope-badge int" style="margin-left:4px">' + intCount + ' INT</span>' : '') +
                (extCount > 0 ? ' <span class="scope-badge ext" style="margin-left:4px">' + extCount + ' EXT</span>' : '');
        }
        var st = PA.tf.state('ref');
        var filtEl = document.getElementById('refsFilteredCount');
        if (filtEl && st) {
            if (st.filtered.length < allItems.length) {
                filtEl.textContent = st.filtered.length + ' shown';
                filtEl.style.display = '';
            } else {
                filtEl.style.display = 'none';
            }
        }
    },

    onSearch: function() {
        var val = document.getElementById('refSearch') ? document.getElementById('refSearch').value : '';
        PA.tf.setSearch('ref', val);
        PA.tf.filter('ref');
    },

    clearFilters: function() {
        var search = document.getElementById('refSearch');
        if (search) search.value = '';
        PA.refs._activeScopeTypes.clear();
        document.querySelectorAll('#refScopePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.tf.setSearch('ref', '');
        var st = PA.tf.state('ref');
        if (st) { st.colFilters = {}; }
        PA.tf.filter('ref');
        PA.tf._cfUpdateIcons('ref');
    },

    filter: function() {
        PA.refs.onSearch();
    }
};
