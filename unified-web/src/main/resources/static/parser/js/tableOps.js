window.PA = window.PA || {};

PA.tableOps = {
    data: [],
    activeOps: new Set(),

    async load() {
        PA.tableOps.data = await PA.api.getTableOperations();
        PA.tableOps.activeOps.clear();
        PA.tableOps._initOpPills();
        PA.tableOps._initTable();
        PA.tableOps.applyScope();
    },

    _initTable() {
        var container = document.getElementById('toContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'to\',0)">Table</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'to\',1)">Type</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'to\',3)">Count</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'to\',4)">Triggers</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'to\',5)">Procedures</th>';
        html += '<th data-sort-col="6" onclick="PA.tf.sort(\'to\',6)">Usage</th>';
        html += '</tr></thead><tbody id="to-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('to', PA.tableOps.data, 50, PA.tableOps._renderRow, {
            sortKeys: {
                0: { fn: function(t) { return (t.tableName || '').toUpperCase(); } },
                1: { fn: function(t) { return (t.objectType || 'TABLE').toUpperCase(); } },
                3: { fn: function(t) { return t.accessCount || (t.accessDetails || []).length; } },
                4: { fn: function(t) { return (t.triggers || []).length; } },
                5: { fn: function(t) { return new Set((t.accessDetails || []).map(function(d) { return d.procedureName || ''; })).size; } },
                6: { fn: function(t) { return PA.tableOps._usageCount(t); } }
            },
            renderDetail: PA.tableOps._renderDetail,
            searchFn: function(t, q) {
                return (t.tableName || '').toUpperCase().includes(q) ||
                    (t.objectType || '').toUpperCase().includes(q) ||
                    (t.accessDetails || []).some(function(d) { return (d.procedureName || '').toUpperCase().includes(q); });
            },
            extraFilter: PA.tableOps._opFilter,
            onFilter: PA.tableOps._updateCounts
        });

        var s = PA.tf.state('to');
        if (s) { s.sortCol = 3; s.sortDir = 'desc'; }

        PA.tf.filter('to');

        setTimeout(function() {
            PA.tf.initColFilters('to', {
                0: { label: 'Table', valueFn: function(t) { return t.tableName || ''; } },
                1: { label: 'Type', valueFn: function(t) { return t.objectType || 'TABLE'; } },
                2: { label: 'Operation', valueFn: function(t) { return (t.operations || []).slice(); } },
                3: { label: 'Count', valueFn: function(t) { return String(t.accessCount || (t.accessDetails || []).length); } },
                4: { label: 'Triggers', valueFn: function(t) { return String((t.triggers || []).length); } },
                5: { label: 'Procedure', valueFn: function(t) {
                    var procs = new Set((t.accessDetails || []).map(function(d) { return d.procedureName || ''; }).filter(Boolean));
                    return Array.from(procs);
                }},
                6: { label: 'Usage', valueFn: function(t) {
                    var procs = new Set((t.accessDetails || []).map(function(d) { return d.procedureName || ''; }).filter(Boolean));
                    return Array.from(procs);
                }}
            });
            PA.tf._updateSortIndicators('to');
        }, 0);
    },

    _opFilter: function(t) {
        var ops = PA.tableOps.activeOps;
        if (ops.size > 0 && !(t.operations || []).some(function(op) { return ops.has(op); })) return false;
        return true;
    },

    _initOpPills: function() {
        var container = document.getElementById('toOpPills');
        if (!container) return;
        var ops = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE', 'TRUNCATE', 'DROP'];
        var html = '';
        for (var i = 0; i < ops.length; i++) {
            var op = ops[i];
            html += '<span class="op-filter-pill ' + op + ' active" data-op="' + op + '" onclick="PA.tableOps.toggleOp(\'' + op + '\')" data-tip="Filter by ' + op + '">' + op + '</span>';
        }
        container.innerHTML = html;
    },

    toggleOp: function(op) {
        var pill = document.querySelector('#toOpPills .op-filter-pill[data-op="' + op + '"]');
        if (!pill) return;
        if (PA.tableOps.activeOps.size === 0) {
            PA.tableOps.activeOps.add(op);
            document.querySelectorAll('#toOpPills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.op === op);
            });
        } else if (PA.tableOps.activeOps.has(op)) {
            PA.tableOps.activeOps.delete(op);
            pill.classList.remove('active');
            if (PA.tableOps.activeOps.size === 0) {
                document.querySelectorAll('#toOpPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.tableOps.activeOps.add(op);
            pill.classList.add('active');
        }
        PA.tf.filter('to');
    },

    clearFilters: function() {
        document.getElementById('toSearch').value = '';
        PA.tableOps.activeOps.clear();
        document.querySelectorAll('#toOpPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.tf.setSearch('to', '');
        var s = PA.tf.state('to');
        if (s) { s.colFilters = {}; }
        PA.tf.filter('to');
        PA.tf._cfUpdateIcons('to');
    },

    onSearch: function() {
        var el = document.getElementById('toSearch');
        PA.tf.setSearch('to', el ? el.value : '');
        PA.tf.filter('to');
    },

    _updateCounts: function() {
        var s = PA.tf.state('to');
        var allTotal = (PA.tableOps.data || []).length;
        var dataTotal = s ? s.data.length : allTotal;
        var shown = s ? s.filtered.length : allTotal;
        var totalEl = document.getElementById('toTotalCount');
        var scope = PA._scope;
        if (scope !== 'full' && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' tables (' + scope + ')';
        } else {
            totalEl.textContent = allTotal + ' tables';
        }
        var fc = document.getElementById('toFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else { fc.style.display = 'none'; }

        // Re-apply Claude enrichment badges after every render cycle
        if (PA.claude && PA.claude._applyEnrichmentBadges) {
            PA.claude._applyEnrichmentBadges();
        }
    },

    _renderRow: function(t, idx, esc) {
        var ops = (t.operations || []).map(function(op) { return '<span class="op-badge ' + op + '">' + op + '</span>'; }).join('');
        var count = t.accessCount || (t.accessDetails || []).length;
        var procs = new Set((t.accessDetails || []).map(function(d) { return d.procedureName || ''; }).filter(Boolean));
        var procCount = procs.size;
        var objType = t.objectType || 'TABLE';
        var typeCls = objType === 'VIEW' ? 'view' : (objType === 'MATERIALIZED VIEW' || objType === 'MATERIALIZED_VIEW') ? 'mv' : 'table';

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'to\',' + idx + ')">';
        html += '<td><span style="font-weight:600;color:var(--teal);cursor:pointer" data-tip="Click to view table details" onclick="event.stopPropagation(); PA.tableDetail.open(\'' + PA.escJs(t.tableName || '') + '\', \'' + PA.escJs(t.tableSchema || '') + '\')">' + esc((t.tableSchema ? t.tableSchema + '.' : '') + (t.tableName || '')) + '</span></td>';
        html += '<td><span class="to-type-badge ' + typeCls + '">' + esc(objType) + '</span></td>';
        html += '<td>' + ops + '</td>';
        html += '<td style="font-weight:700;color:var(--accent)">' + count + '</td>';
        var triggers = t.triggers || [];
        if (triggers.length > 0) {
            html += '<td><span class="to-trigger-count" data-tip="Click to view triggers" onclick="event.stopPropagation(); PA.tableOps.showTriggers(' + idx + ')">' + triggers.length + '</span></td>';
        } else {
            html += '<td style="color:var(--text-muted)">-</td>';
        }
        html += '<td style="font-size:11px;color:var(--text-muted)">' + procCount + ' proc' + (procCount !== 1 ? 's' : '') + '</td>';
        var usageHtml = PA.tableOps._renderUsageSummary(t);
        html += '<td class="to-usage-cell">' + usageHtml + '</td>';
        html += '</tr>';
        return html;
    },

    _renderDetail: function(table, idx, esc) {
        var details = table.accessDetails || [];
        var fullCount = table._fullAccessCount || details.length;
        var isScoped = PA._scope !== 'full' && table._fullAccessDetails;
        var html = '<div class="to-detail">';

        if (details.length > 0) {
            html += '<div class="to-detail-section">';
            var scopeNote = isScoped && fullCount > details.length
                ? ' <span style="color:var(--text-muted);font-weight:400">(' + fullCount + ' total)</span>' : '';
            html += '<div class="to-detail-section-title">Access Details (' + details.length + ')' + scopeNote + '</div>';
            for (var i = 0; i < details.length; i++) {
                var d = details[i];
                html += '<div class="to-detail-item">';
                html += '<span class="op-badge ' + (d.operation || '') + '">' + (d.operation || '?') + '</span>';
                html += '<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure(\'' + PA.escJs(d.procedureId || d.procedureName || '') + '\')">' + esc(d.procedureName || '') + '</span>';
                var sf = d.sourceFile;
                if (!sf && d.procedureId && PA.analysisData && PA.analysisData.nodes) {
                    var node = PA.analysisData.nodes.find(function(n) { return n.id === d.procedureId || n.nodeId === d.procedureId; });
                    if (node) sf = node.sourceFile || '';
                }
                if (sf && d.lineNumber) {
                    html += '<span class="to-detail-line" data-tip="Open source at line" onclick="event.stopPropagation(); PA.sourceView.openAtLine(\'' + PA.escJs(sf) + '\', ' + d.lineNumber + ')">L' + d.lineNumber + '</span>';
                }
                if (d.whereFilters && d.whereFilters.length > 0) {
                    var wf = d.whereFilters.map(function(f) { return (f.columnName || '') + ' ' + (f.operator || '') + ' ' + (f.value || ''); }).join(', ');
                    html += '<span class="to-where">WHERE: ' + esc(wf) + '</span>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        if (details.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No access details available</div>';
        }

        var _trigs = table.triggers || [];
        var _trigTableOps = [];
        for (var ti = 0; ti < _trigs.length; ti++) {
            var tOps = _trigs[ti].tableOps || [];
            for (var oi = 0; oi < tOps.length; oi++) {
                _trigTableOps.push({ triggerName: _trigs[ti].name || '', operation: tOps[oi].operation || '', tableName: tOps[oi].tableName || '', schema: tOps[oi].schema || '', objectType: tOps[oi].objectType || '' });
            }
        }
        if (_trigTableOps.length > 0) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Tables via Triggers (' + _trigTableOps.length + ')</div>';
            for (var tti = 0; tti < _trigTableOps.length; tti++) {
                var tto = _trigTableOps[tti];
                html += '<div class="to-detail-item">';
                html += '<span class="op-badge ' + esc(tto.operation) + '">' + esc(tto.operation || '?') + '</span>';
                html += '<span class="to-detail-proc" onclick="event.stopPropagation(); PA.tableDetail.open(\'' + PA.escJs(tto.tableName) + '\',\'' + PA.escJs(tto.schema) + '\')">' + esc((tto.schema ? tto.schema + '.' : '') + tto.tableName) + '</span>';
                html += '<span style="font-size:9px;color:var(--text-muted);margin-left:4px">via ' + esc(tto.triggerName) + '</span>';
                html += '</div>';
            }
            html += '</div>';
        }

        if (PA.claude && PA.claude.renderDetailEnrichment) {
            html += PA.claude.renderDetailEnrichment(table.tableName);
        }

        html += '</div>';
        return html;
    },

    applyScope: function() {
        var ctx = PA.context;
        if (!ctx || !ctx.procId) {
            if (PA.tf._state['to']) PA.tf.setData('to', PA.tableOps.data);
            PA.tableOps._updateCounts();
            return;
        }
        PA.tableOps._applyScopeData();
    },

    _applyScopeData: function() {
        var mode = PA._scope;
        var ctx = PA.context;

        if (!ctx || !ctx.procId || mode === 'full') {
            PA.tf.setData('to', PA.tableOps.data);
            PA.tableOps._updateCounts();
            return;
        }

        var currentProcId = (ctx.procId || '').toUpperCase();
        var parts = currentProcId.split('.');
        var currentLast = parts.pop();
        var currentPkg = parts.length > 0 ? parts.pop() : '';
        var nodeIds = ctx.callTreeNodeIds;

        var _matchCurrent = function(pid) {
            if (pid === currentProcId) return true;
            if (currentPkg) return pid.endsWith('.' + currentPkg + '.' + currentLast);
            return pid === currentLast;
        };

        var _matchNode = function(pid) {
            if (!nodeIds || nodeIds.size === 0) return false;
            var up = pid.toUpperCase();
            if (nodeIds.has(up)) return true;
            var matched = false;
            nodeIds.forEach(function(nid) {
                if (up.includes(nid) || nid.includes(up)) matched = true;
            });
            return matched;
        };

        var _matchesScope = function(pid) {
            var up = (pid || '').toUpperCase();
            if (mode === 'direct') return _matchCurrent(up);
            if (mode === 'subtree') return !_matchCurrent(up) && _matchNode(up);
            return _matchCurrent(up) || _matchNode(up);
        };

        var filtered = PA.tableOps.data.map(function(t) {
            var allDetails = t.accessDetails || [];
            var scopedDetails = allDetails.filter(function(d) {
                var pid = d.procedureId || d.procedureName || '';
                return _matchesScope(pid);
            });
            if (scopedDetails.length === 0) return null;
            var fullProcs = new Set(allDetails.map(function(d) { return d.procedureName || ''; }).filter(Boolean));
            return Object.assign({}, t, {
                accessDetails: scopedDetails,
                accessCount: scopedDetails.length,
                _fullAccessDetails: allDetails,
                _fullAccessCount: t.accessCount || allDetails.length,
                _fullProcCount: fullProcs.size
            });
        }).filter(Boolean);

        PA.tf.setData('to', filtered);
        PA.tableOps._updateCounts();
    },

    _usageCount: function(t) {
        var details = t.accessDetails || [];
        return new Set(details.map(function(d) { return d.procedureName || ''; }).filter(Boolean)).size;
    },

    _renderUsageSummary: function(t) {
        var details = t.accessDetails || [];
        if (!details.length) return '<span style="color:var(--text-muted)">-</span>';
        var procOps = {};
        for (var i = 0; i < details.length; i++) {
            var d = details[i];
            var pName = d.procedureName || '';
            if (!pName) continue;
            if (!procOps[pName]) procOps[pName] = new Set();
            if (d.operation) procOps[pName].add(d.operation);
        }
        var keys = Object.keys(procOps);
        var scopedCount = keys.length;
        var fullCount = t._fullProcCount || scopedCount;
        var isScoped = PA._scope !== 'full' && PA.context.procId && t._fullAccessDetails;

        var html = '<span class="to-usage-count">' + scopedCount;
        if (isScoped && fullCount > scopedCount) html += '<span class="to-usage-total">/' + fullCount + '</span>';
        html += '</span>';

        var max = Math.min(keys.length, 3);
        for (var j = 0; j < max; j++) {
            var pn = keys[j];
            var opsArr = Array.from(procOps[pn]);
            html += '<div class="to-usage-item">';
            html += '<span class="to-usage-proc" onclick="event.stopPropagation();PA.showProcedure(\'' + PA.escJs(pn) + '\')">' + PA.esc(pn) + '</span>';
            for (var k = 0; k < opsArr.length; k++) {
                html += '<span class="op-badge ' + opsArr[k] + '" style="font-size:7px;padding:0 3px">' + opsArr[k].charAt(0) + '</span>';
            }
            html += '</div>';
        }
        if (keys.length > 3) html += '<span style="font-size:9px;color:var(--text-muted)">+' + (keys.length - 3) + ' more</span>';
        return html;
    },

    focusTable: function(tableName) {
        PA.switchRightTab('tableOps');
        var searchInput = document.getElementById('toSearch');
        if (searchInput) {
            searchInput.value = tableName;
            PA.tf.setSearch('to', tableName);
            PA.tf.filter('to');
        }
    },

    showTriggers: function(idx) {
        var s = PA.tf.state('to');
        if (!s) return;
        var t = s.filtered[idx];
        if (!t) return;
        PA.tableDetail.open(t.tableName || '', t.tableSchema || '', 'triggers');
    },
    showTriggerDetail: function(nodeId) { PA.triggers.showNodeDetail(nodeId); },
    showTriggerDef: function(tableIdx, trigIdx) { PA.triggers.showDef(tableIdx, trigIdx); }
};
