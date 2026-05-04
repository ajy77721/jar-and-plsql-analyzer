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
        PA.refs._loaded = false;
        var container = document.getElementById('refsContainer');
        container.innerHTML = '<div class="empty-msg">Loading references...</div>';
        try {
            PA.refs._buildAllProcs();
            PA.refs._loaded = true;
        } catch (e) { console.warn('[PA] refs.load failed', e); }
        PA.refs._initScopePills();
        PA.refs._buildTable();
    },

    _buildAllProcs: function() {
        var nodes = PA.analysisData ? PA.analysisData.nodes : [];
        var map = new Map();
        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            var nid = (n.nodeId || n.name || '').toUpperCase();
            if (map.has(nid)) continue;
            map.set(nid, {
                node: {
                    name: n.name || n.nodeId || '?',
                    schema: n.schema || '',
                    schemaName: n.schema || '',
                    packageName: n.packageName || '',
                    callType: 'INTERNAL',
                    unitType: n.objectType || 'PROCEDURE',
                    objectType: n.objectType || 'PROCEDURE',
                    id: n.nodeId || '',
                    nodeId: n.nodeId || '',
                    sourceFile: n.sourceFile || '',
                    loc: n.linesOfCode || 0
                },
                count: 1,
                lines: []
            });
        }
        PA.refs.callsItems = Array.from(map.values());
        PA.refs._assignCallTypes(PA.refs.callsItems);
    },

    _getItemsForScope: function() {
        return PA.refs.callsItems;
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

    _assignCallTypes: function(items) {
        var currentSchema = '';
        if (PA.currentDetail) currentSchema = (PA.currentDetail.schema || '').toUpperCase();
        if (!currentSchema && PA.context.procId) {
            var node = PA._findNodeInData(PA.context.procId);
            if (node) currentSchema = (node.schema || '').toUpperCase();
        }
        for (var i = 0; i < items.length; i++) {
            var n = items[i].node;
            var targetSchema = (n.schemaName || n.schema || '').toUpperCase();
            if (!targetSchema || !currentSchema) {
                n.callType = n.callType || 'INTERNAL';
            } else {
                n.callType = (targetSchema === currentSchema) ? 'INTERNAL' : 'EXTERNAL';
            }
        }
    },

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
            container.innerHTML = '<div class="empty-msg">No procedures found</div>';
            return;
        }

        var pp = PA.refs._parsePkgProc;
        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'ref\',0)">Schema</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'ref\',1)">Procedure</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'ref\',2)">Type</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'ref\',3)">Scope</th>';
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

        var typeBadge = unitType === 'FUNCTION'
            ? '<span class="lp-icon F" style="display:inline-flex;width:18px;height:18px;font-size:9px">F</span>'
            : '<span class="lp-icon P" style="display:inline-flex;width:18px;height:18px;font-size:9px">P</span>';
        var scopeBadge = callType === 'INTERNAL'
            ? '<span class="scope-badge int">INT</span>'
            : callType && callType !== 'ROOT' ? '<span class="scope-badge ext">' + esc(callType) + '</span>' : '';

        var html = '<tr class="to-row">';
        if (schema) html += '<td><span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + '">' + esc(schema) + '</span></td>';
        else html += '<td style="color:var(--text-muted);font-size:11px">-</td>';
        html += '<td><span class="to-detail-proc" onclick="PA.showProcedure(\'' + PA.escJs(n.id || n.nodeId || name) + '\')">' + esc(name) + '</span></td>';
        html += '<td>' + typeBadge + ' <span style="font-size:11px">' + esc(unitType) + '</span></td>';
        html += '<td>' + scopeBadge + '</td>';
        html += '</tr>';
        return html;
    },

    _updateBadgeCounts: function() {
        var allItems = PA.refs.callsItems;
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
