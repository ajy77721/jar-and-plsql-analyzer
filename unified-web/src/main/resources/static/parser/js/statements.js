window.PA = window.PA || {};

PA.statements = {
    data: [],
    _activeTypes: new Set(),

    load: function() {
        PA.statements._aggregate();
        PA.statements._initTypePills();
        PA.statements._initTable();
        PA.statements.applyScope();
    },

    _aggregate: function() {
        var nodes = PA.analysisData ? PA.analysisData.nodes : [];
        if (!nodes || !nodes.length) { PA.statements.data = []; return; }

        var typeMap = {};

        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            var stmts = (n.counts && n.counts.statements) ? n.counts.statements : null;
            if (!stmts) continue;

            var nodeId = n.nodeId || n.name || '';
            var nodeName = n.name || nodeId;
            var schema = n.schema || '';
            var sourceFile = n.sourceFile || '';

            var keys = Object.keys(stmts);
            for (var k = 0; k < keys.length; k++) {
                var stype = keys[k].toUpperCase();
                var count = stmts[keys[k]] || 0;
                if (count <= 0) continue;

                if (!typeMap[stype]) {
                    typeMap[stype] = { type: stype, totalCount: 0, procCount: 0, procs: [] };
                }
                typeMap[stype].totalCount += count;
                typeMap[stype].procCount++;
                typeMap[stype].procs.push({
                    nodeId: nodeId,
                    name: nodeName,
                    schema: schema,
                    sourceFile: sourceFile,
                    count: count
                });
            }
        }

        var result = [];
        var tkeys = Object.keys(typeMap);
        for (var t = 0; t < tkeys.length; t++) {
            var entry = typeMap[tkeys[t]];
            entry.procs.sort(function(a, b) { return b.count - a.count; });
            result.push(entry);
        }

        PA.statements.data = result;
    },

    _aggregateScoped: function() {
        var nodes = PA.analysisData ? PA.analysisData.nodes : [];
        if (!nodes || !nodes.length) return [];

        var ctx = PA.context;
        var currentProcId = (ctx.procId || '').toUpperCase();
        var nodeIds = ctx.callTreeNodeIds;
        var mode = PA._scope;
        var typeMap = {};

        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            var nid = (n.nodeId || n.name || '').toUpperCase();

            if (mode === 'direct') {
                if (nid !== currentProcId) continue;
            } else if (mode === 'subtree') {
                if (nid === currentProcId) continue;
                if (!nodeIds || !nodeIds.has(nid)) continue;
            } else if (mode === 'subflow') {
                if (nid !== currentProcId && (!nodeIds || !nodeIds.has(nid))) continue;
            }

            var stmts = (n.counts && n.counts.statements) ? n.counts.statements : null;
            if (!stmts) continue;

            var nodeId = n.nodeId || n.name || '';
            var nodeName = n.name || nodeId;
            var schema = n.schema || '';
            var sourceFile = n.sourceFile || '';

            var keys = Object.keys(stmts);
            for (var k = 0; k < keys.length; k++) {
                var stype = keys[k].toUpperCase();
                var count = stmts[keys[k]] || 0;
                if (count <= 0) continue;

                if (!typeMap[stype]) {
                    typeMap[stype] = { type: stype, totalCount: 0, procCount: 0, procs: [] };
                }
                typeMap[stype].totalCount += count;
                typeMap[stype].procCount++;
                typeMap[stype].procs.push({
                    nodeId: nodeId,
                    name: nodeName,
                    schema: schema,
                    sourceFile: sourceFile,
                    count: count
                });
            }
        }

        var result = [];
        var tkeys = Object.keys(typeMap);
        for (var t = 0; t < tkeys.length; t++) {
            var entry = typeMap[tkeys[t]];
            entry.procs.sort(function(a, b) { return b.count - a.count; });
            result.push(entry);
        }
        return result;
    },

    _initTypePills: function() {
        var container = document.getElementById('stmtTypePills');
        if (!container) return;

        var allTypes = new Set();
        for (var i = 0; i < PA.statements.data.length; i++) {
            allTypes.add(PA.statements.data[i].type);
        }

        var dmlTypes = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'];
        var ctrlTypes = ['IF', 'LOOP', 'FOR_LOOP', 'WHILE_LOOP', 'CASE'];
        var otherTypes = ['ASSIGNMENT', 'RETURN', 'EXECUTE_IMMEDIATE', 'RAISE', 'CURSOR', 'OPEN', 'FETCH', 'CLOSE', 'PIPE_ROW', 'EXIT', 'CONTINUE', 'GOTO', 'NULL_STATEMENT'];

        var ordered = [];
        var used = new Set();
        var groups = [dmlTypes, ctrlTypes, otherTypes];
        for (var g = 0; g < groups.length; g++) {
            for (var j = 0; j < groups[g].length; j++) {
                if (allTypes.has(groups[g][j])) {
                    ordered.push(groups[g][j]);
                    used.add(groups[g][j]);
                }
            }
        }
        allTypes.forEach(function(t) {
            if (!used.has(t)) ordered.push(t);
        });

        var html = '';
        for (var p = 0; p < ordered.length; p++) {
            var t = ordered[p];
            var cls = PA.statements._getTypeClass(t);
            html += '<span class="op-filter-pill active ' + cls + '" data-st="' + PA.escAttr(t) + '" onclick="PA.statements.toggleType(\'' + PA.escJs(t) + '\')">' + PA.esc(t) + '</span>';
        }
        container.innerHTML = html;
        PA.statements._activeTypes = new Set();
    },

    toggleType: function(t) {
        var pill = document.querySelector('#stmtTypePills .op-filter-pill[data-st="' + CSS.escape(t) + '"]');
        if (!pill) return;
        if (PA.statements._activeTypes.size === 0) {
            PA.statements._activeTypes.add(t);
            document.querySelectorAll('#stmtTypePills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.st === t);
            });
        } else if (PA.statements._activeTypes.has(t)) {
            PA.statements._activeTypes.delete(t);
            pill.classList.remove('active');
            if (PA.statements._activeTypes.size === 0) {
                document.querySelectorAll('#stmtTypePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.statements._activeTypes.add(t);
            pill.classList.add('active');
        }
        PA.tf.filter('stmt');
    },

    _initTable: function() {
        var container = document.getElementById('stmtContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'stmt\',0)">Statement Type</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'stmt\',1)">Count</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'stmt\',2)">Procedures</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'stmt\',3)">Avg/Proc</th>';
        html += '<th>Distribution</th>';
        html += '</tr></thead><tbody id="stmt-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('stmt', PA.statements.data, 50, PA.statements._renderRow, {
            sortKeys: {
                0: { fn: function(s) { return s.type; } },
                1: { fn: function(s) { return s.totalCount; } },
                2: { fn: function(s) { return s.procCount; } },
                3: { fn: function(s) { return s.procCount > 0 ? s.totalCount / s.procCount : 0; } }
            },
            renderDetail: PA.statements._renderDetail,
            searchFn: function(s, q) {
                if (s.type.toUpperCase().includes(q)) return true;
                for (var i = 0; i < s.procs.length; i++) {
                    if ((s.procs[i].name || '').toUpperCase().includes(q)) return true;
                }
                return false;
            },
            extraFilter: PA.statements._filter,
            onFilter: PA.statements._updateCounts
        });

        var st = PA.tf.state('stmt');
        if (st) { st.sortCol = 1; st.sortDir = 'desc'; }
        PA.tf.filter('stmt');

        setTimeout(function() {
            PA.tf.initColFilters('stmt', {
                0: { label: 'Statement Type', valueFn: function(s) { return s.type || ''; } }
            });
            PA.tf._updateSortIndicators('stmt');
        }, 0);
    },

    _filter: function(s) {
        var types = PA.statements._activeTypes;
        if (types.size > 0 && !types.has(s.type)) return false;
        return true;
    },

    _getTypeClass: function(type) {
        var dml = { SELECT: 'blue', INSERT: 'green', UPDATE: 'orange', DELETE: 'red', MERGE: 'purple' };
        if (dml[type]) return 'stmt-' + dml[type];
        var ctrl = { IF: 'ctrl', CASE: 'ctrl', LOOP: 'ctrl', FOR_LOOP: 'ctrl', WHILE_LOOP: 'ctrl' };
        if (ctrl[type]) return 'stmt-ctrl';
        return 'stmt-other';
    },

    _getTypeColor: function(type) {
        var colors = {
            SELECT: 'var(--badge-blue)', INSERT: 'var(--badge-green)',
            UPDATE: 'var(--badge-orange)', DELETE: 'var(--badge-red)',
            MERGE: 'var(--badge-purple)', ASSIGNMENT: 'var(--text-muted)',
            IF: 'var(--badge-teal)', CASE: 'var(--badge-teal)',
            LOOP: 'var(--badge-teal)', FOR_LOOP: 'var(--badge-teal)',
            WHILE_LOOP: 'var(--badge-teal)',
            RETURN: '#a78bfa', EXECUTE_IMMEDIATE: '#f472b6',
            RAISE: 'var(--badge-red)', CURSOR: 'var(--purple)',
            OPEN: 'var(--badge-green)', FETCH: 'var(--badge-blue)',
            CLOSE: 'var(--badge-red)'
        };
        return colors[type] || 'var(--text-muted)';
    },

    _renderRow: function(s, idx, esc) {
        var color = PA.statements._getTypeColor(s.type);
        var avg = s.procCount > 0 ? (s.totalCount / s.procCount).toFixed(1) : '0';
        var maxCount = 1;
        for (var i = 0; i < PA.statements.data.length; i++) {
            if (PA.statements.data[i].totalCount > maxCount) maxCount = PA.statements.data[i].totalCount;
        }
        var barPct = Math.min(100, Math.round((s.totalCount / maxCount) * 100));

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'stmt\',' + idx + ')">';
        html += '<td><span class="stmt-type-badge" style="color:' + color + '">' + esc(s.type) + '</span></td>';
        html += '<td style="font-weight:700;color:' + color + '">' + s.totalCount + '</td>';
        html += '<td>' + s.procCount + '</td>';
        html += '<td style="color:var(--text-muted)">' + avg + '</td>';
        html += '<td><div class="stmt-bar-wrap"><div class="stmt-bar" style="width:' + barPct + '%;background:' + color + '"></div><span class="stmt-bar-label">' + barPct + '%</span></div></td>';
        html += '</tr>';
        return html;
    },

    _renderDetail: function(s, idx, esc) {
        var procs = s.procs || [];
        var html = '<div class="to-detail">';

        if (procs.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No procedure details</div>';
            html += '</div>';
            return html;
        }

        html += '<div class="to-detail-section">';
        html += '<div class="to-detail-section-title">Procedures using ' + esc(s.type) + ' (' + procs.length + ')</div>';

        for (var i = 0; i < procs.length; i++) {
            var p = procs[i];
            var colorObj = PA.getSchemaColor(p.schema);
            html += '<div class="to-detail-item">';
            html += '<span class="stmt-proc-count" style="color:' + PA.statements._getTypeColor(s.type) + '">' + p.count + '&times;</span>';
            if (p.schema) {
                html += '<span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + ';font-size:8px;padding:1px 5px">' + esc(p.schema) + '</span>';
            }
            html += '<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure(\'' + PA.escJs(p.nodeId) + '\')">' + esc(p.name) + '</span>';
            if (p.sourceFile) {
                html += '<span class="to-detail-line" data-tip="View source code" onclick="event.stopPropagation(); PA.sourceView.openModal(\'' + PA.escJs(p.nodeId) + '\')">source</span>';
            }
            html += '</div>';
        }

        html += '</div></div>';
        return html;
    },

    onSearch: function() {
        var val = document.getElementById('stmtSearch') ? document.getElementById('stmtSearch').value : '';
        PA.tf.setSearch('stmt', val);
        PA.tf.filter('stmt');
    },

    clearFilters: function() {
        var search = document.getElementById('stmtSearch');
        if (search) search.value = '';
        PA.statements._activeTypes.clear();
        document.querySelectorAll('#stmtTypePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.tf.setSearch('stmt', '');
        var st = PA.tf.state('stmt');
        if (st) { st.colFilters = {}; }
        if (PA.tf._state['stmt']) PA.tf.setData('stmt', PA.statements.data);
        PA.tf.filter('stmt');
        PA.tf._cfUpdateIcons('stmt');
    },

    _updateCounts: function() {
        var st = PA.tf.state('stmt');
        var allTotal = (PA.statements.data || []).length;
        var shown = st ? st.filtered.length : allTotal;
        var grandTotal = 0;
        var filtered = st ? st.filtered : PA.statements.data;
        for (var i = 0; i < filtered.length; i++) grandTotal += filtered[i].totalCount;

        var totalEl = document.getElementById('stmtTotalCount');
        if (totalEl) totalEl.textContent = allTotal + ' types';
        var filtEl = document.getElementById('stmtFilteredCount');
        if (filtEl) {
            if (shown < allTotal) { filtEl.textContent = shown + ' shown'; filtEl.style.display = ''; }
            else filtEl.style.display = 'none';
        }
        var sumEl = document.getElementById('stmtSumCount');
        if (sumEl) sumEl.textContent = grandTotal + ' stmts';
    },

    applyScope: function() {
        var ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.tf.setData('stmt', PA.statements.data);
            PA.statements._updateCounts();
            return;
        }
        PA.statements._applyScopeData();
    },

    _applyScopeData: function() {
        var mode = PA._scope;
        var ctx = PA.context;
        if (!ctx || !ctx.procId || mode === 'full') {
            PA.tf.setData('stmt', PA.statements.data);
            PA.statements._updateCounts();
            return;
        }
        var scoped = PA.statements._aggregateScoped();
        PA.tf.setData('stmt', scoped);
        PA.statements._updateCounts();
    }
};
