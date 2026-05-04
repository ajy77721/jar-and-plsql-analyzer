window.PA = window.PA || {};

PA.exHandlers = {
    data: [],

    load: function() {
        PA.exHandlers._aggregate();
        PA.exHandlers._initTable();
        PA.exHandlers.applyScope();
    },

    _aggregate: function() {
        var nodes = PA.analysisData ? PA.analysisData.nodes : [];
        if (!nodes || !nodes.length) { PA.exHandlers.data = []; return; }

        var result = [];
        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            var ehCount = (n.counts && n.counts.exceptionHandlers) ? n.counts.exceptionHandlers : 0;
            if (ehCount <= 0) continue;

            result.push({
                nodeId: n.nodeId || n.name || '',
                name: n.name || n.nodeId || '',
                schema: n.schema || '',
                objectType: n.objectType || '',
                sourceFile: n.sourceFile || '',
                detailFile: n.detailFile || '',
                lineStart: n.lineStart || 0,
                lineEnd: n.lineEnd || 0,
                handlerCount: ehCount,
                handlers: null,
                _loaded: false
            });
        }

        result.sort(function(a, b) { return b.handlerCount - a.handlerCount; });
        PA.exHandlers.data = result;
    },

    _initTable: function() {
        var container = document.getElementById('exhContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'exh\',0)">Procedure</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'exh\',1)">Schema</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'exh\',2)">Type</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'exh\',3)">Handlers</th>';
        html += '<th>Lines</th>';
        html += '</tr></thead><tbody id="exh-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('exh', PA.exHandlers.data, 50, PA.exHandlers._renderRow, {
            sortKeys: {
                0: { fn: function(e) { return (e.name || '').toUpperCase(); } },
                1: { fn: function(e) { return (e.schema || '').toUpperCase(); } },
                2: { fn: function(e) { return (e.objectType || '').toUpperCase(); } },
                3: { fn: function(e) { return e.handlerCount; } }
            },
            renderDetail: PA.exHandlers._renderDetail,
            searchFn: function(e, q) {
                return (e.name || '').toUpperCase().includes(q) ||
                       (e.schema || '').toUpperCase().includes(q) ||
                       (e.objectType || '').toUpperCase().includes(q);
            },
            onFilter: PA.exHandlers._updateCounts
        });

        var st = PA.tf.state('exh');
        if (st) { st.sortCol = 3; st.sortDir = 'desc'; }
        PA.tf.filter('exh');

        setTimeout(function() {
            PA.tf.initColFilters('exh', {
                0: { label: 'Procedure', valueFn: function(e) { return e.name || ''; } },
                1: { label: 'Schema', valueFn: function(e) { return e.schema || '-'; } },
                2: { label: 'Type', valueFn: function(e) { return e.objectType || ''; } }
            });
            PA.tf._updateSortIndicators('exh');
        }, 0);
    },

    _renderRow: function(e, idx, esc) {
        var colorObj = PA.getSchemaColor(e.schema);
        var lineRange = e.lineStart ? (e.lineEnd ? e.lineStart + '-' + e.lineEnd : '' + e.lineStart) : '-';

        var html = '<tr class="to-row" onclick="PA.exHandlers._onRowClick(' + idx + ')">';
        html += '<td><span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure(\'' + PA.escJs(e.nodeId) + '\')">' + esc(e.name) + '</span></td>';
        if (e.schema) {
            html += '<td><span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + '">' + esc(e.schema) + '</span></td>';
        } else {
            html += '<td style="color:var(--text-muted)">-</td>';
        }
        html += '<td style="font-size:11px">' + esc(e.objectType) + '</td>';
        html += '<td><span style="font-weight:700;color:var(--badge-orange)">' + e.handlerCount + '</span></td>';
        html += '<td style="font-size:10px;color:var(--text-muted)">' + lineRange + '</td>';
        html += '</tr>';
        return html;
    },

    _onRowClick: async function(idx) {
        var st = PA.tf.state('exh');
        if (!st) return;
        var item = st.filtered[idx];
        if (!item) return;

        if (!item._loaded && item.detailFile) {
            try {
                var detail = await PA.api.getNodeDetail(item.detailFile.replace(/^nodes\//, ''));
                if (detail && detail.exceptionHandlers) {
                    item.handlers = detail.exceptionHandlers;
                }
                item._loaded = true;
            } catch (e) {
                console.warn('[PA] Failed to load exception handlers for', item.nodeId, e);
                item._loaded = true;
            }
        }

        var detailRow = document.getElementById('exh-detail-' + idx);
        if (detailRow && detailRow.dataset.lazy) {
            var esc = PA.esc || function(v) { return v; };
            detailRow.innerHTML = '<td colspan="20">' + PA.exHandlers._renderDetail(item, idx, esc) + '</td>';
            delete detailRow.dataset.lazy;
        }

        PA.tf.toggleDetail('exh', idx);
    },

    _renderDetail: function(e, idx, esc) {
        var handlers = e.handlers || [];
        var html = '<div class="to-detail">';

        if (handlers.length === 0 && e._loaded) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No handler details available</div>';
            html += '</div>';
            return html;
        }

        if (handlers.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">Loading...</div>';
            html += '</div>';
            return html;
        }

        html += '<div class="to-detail-section">';
        html += '<div class="to-detail-section-title" style="color:var(--badge-orange)">Exception Handlers (' + handlers.length + ')</div>';

        for (var i = 0; i < handlers.length; i++) {
            var h = handlers[i];
            var exName = h.exception || 'UNKNOWN';
            var isOthers = exName === 'OTHERS';
            var color = isOthers ? 'var(--badge-red)' : 'var(--badge-orange)';

            html += '<div class="to-detail-item">';
            html += '<span class="exh-badge" style="color:' + color + ';border-color:' + color + '">' + esc(exName) + '</span>';
            if (h.line) {
                var lineLabel = h.lineEnd && h.lineEnd !== h.line ? 'L' + h.line + '-' + h.lineEnd : 'L' + h.line;
                var sf = e.sourceFile || '';
                if (sf) {
                    html += '<span class="to-detail-line" data-tip="Open source at line" onclick="event.stopPropagation(); PA.sourceView.openAtLine(\'' + PA.escJs(sf) + '\', ' + h.line + ')">' + lineLabel + '</span>';
                } else {
                    html += '<span style="font-size:10px;color:var(--text-muted)">' + lineLabel + '</span>';
                }
            }
            html += '<span style="font-size:10px;color:var(--text-muted)">' + (h.statementsCount || 0) + ' stmts</span>';
            html += '</div>';
        }

        html += '</div></div>';
        return html;
    },

    onSearch: function() {
        var val = document.getElementById('exhSearch') ? document.getElementById('exhSearch').value : '';
        PA.tf.setSearch('exh', val);
        PA.tf.filter('exh');
    },

    clearFilters: function() {
        var search = document.getElementById('exhSearch');
        if (search) search.value = '';
        PA.tf.setSearch('exh', '');
        var st = PA.tf.state('exh');
        if (st) { st.colFilters = {}; }
        if (PA.tf._state['exh']) PA.tf.setData('exh', PA.exHandlers.data);
        PA.tf.filter('exh');
        PA.tf._cfUpdateIcons('exh');
    },

    _updateCounts: function() {
        var st = PA.tf.state('exh');
        var allTotal = (PA.exHandlers.data || []).length;
        var shown = st ? st.filtered.length : allTotal;
        var totalHandlers = 0;
        var filtered = st ? st.filtered : PA.exHandlers.data;
        for (var i = 0; i < filtered.length; i++) totalHandlers += filtered[i].handlerCount;

        var totalEl = document.getElementById('exhTotalCount');
        if (totalEl) totalEl.textContent = allTotal + ' procs';
        var filtEl = document.getElementById('exhFilteredCount');
        if (filtEl) {
            if (shown < allTotal) { filtEl.textContent = shown + ' shown'; filtEl.style.display = ''; }
            else filtEl.style.display = 'none';
        }
        var sumEl = document.getElementById('exhSumCount');
        if (sumEl) sumEl.textContent = totalHandlers + ' handlers';
    },

    applyScope: function() {
        var ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.tf.setData('exh', PA.exHandlers.data);
            PA.exHandlers._updateCounts();
            return;
        }
        PA.exHandlers._applyScopeData();
    },

    _applyScopeData: function() {
        var mode = PA._scope;
        var ctx = PA.context;
        if (!ctx || !ctx.procId || mode === 'full') {
            PA.tf.setData('exh', PA.exHandlers.data);
            PA.exHandlers._updateCounts();
            return;
        }

        var currentProcId = (ctx.procId || '').toUpperCase();
        var nodeIds = ctx.callTreeNodeIds;

        var filtered = PA.exHandlers.data.filter(function(e) {
            var nid = (e.nodeId || '').toUpperCase();
            if (mode === 'direct') return nid === currentProcId;
            if (mode === 'subtree') return nid !== currentProcId && nodeIds && nodeIds.has(nid);
            return nid === currentProcId || (nodeIds && nodeIds.has(nid));
        });
        PA.tf.setData('exh', filtered);
        PA.exHandlers._updateCounts();
    }
};
