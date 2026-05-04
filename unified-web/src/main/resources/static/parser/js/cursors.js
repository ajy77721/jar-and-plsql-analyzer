window.PA = window.PA || {};

PA.cursors = {
    data: [],

    async load() {
        PA.cursors.data = await PA.api.getCursorOperations();
        PA.cursors._initTypePills();
        PA.cursors._initScopeToggle();
        PA.cursors._initTable();
        PA.cursors.applyScope();
    },

    _initTypePills: function() {
        var container = document.getElementById('curTypePills');
        if (!container) return;
        var types = ['EXPLICIT', 'REF_CURSOR', 'FOR_LOOP', 'OPEN_FOR'];
        var labels = { REF_CURSOR: 'REF', FOR_LOOP: 'FOR', OPEN_FOR: 'OPEN FOR' };
        var html = '';
        for (var i = 0; i < types.length; i++) {
            var t = types[i];
            var label = labels[t] || t;
            html += '<span class="op-filter-pill active" data-ct="' + t + '" onclick="PA.cursors.toggleType(\'' + t + '\')" title="Toggle ' + t + '">' + label + '</span>';
        }
        container.innerHTML = html;
        PA.cursors._activeTypes = new Set();
    },

    _initScopeToggle: function() {
        PA.cursors._renderScopeToggle();
    },

    _renderScopeToggle: function() {
        var container = document.getElementById('curScopeToggle');
        if (!container) return;
        var ctx = PA.context;
        if (!ctx || !ctx.procId) { container.style.display = 'none'; return; }
        container.style.display = '';
        var mode = PA._scope;
        var modes = [
            { key: 'direct', label: 'Direct', title: 'Current procedure only' },
            { key: 'subtree', label: 'Subtree', title: 'All descendants, excluding current node' },
            { key: 'subflow', label: 'SubFlow', title: 'Current node + all descendants combined' },
            { key: 'full', label: 'Full', title: 'All nodes in the entire analysis' }
        ];
        var html = '';
        for (var i = 0; i < modes.length; i++) {
            var m = modes[i];
            html += '<button class="btn-sm' + (mode === m.key ? ' active' : '') + '" data-scope="' + m.key + '" onclick="PA.setScope(\'' + m.key + '\')" title="' + m.title + '">' + m.label + '</button>';
        }
        container.innerHTML = html;
    },

    _activeTypes: new Set(),

    toggleType: function(t) {
        var pill = document.querySelector('#curTypePills .op-filter-pill[data-ct="' + t + '"]');
        if (!pill) return;
        if (PA.cursors._activeTypes.size === 0) {
            PA.cursors._activeTypes.add(t);
            document.querySelectorAll('#curTypePills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.ct === t);
            });
        } else if (PA.cursors._activeTypes.has(t)) {
            PA.cursors._activeTypes.delete(t);
            pill.classList.remove('active');
            if (PA.cursors._activeTypes.size === 0) {
                document.querySelectorAll('#curTypePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.cursors._activeTypes.add(t);
            pill.classList.add('active');
        }
        PA.tf.filter('cur');
    },

    _initTable: function() {
        var container = document.getElementById('curContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'cur\',0)">Cursor</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'cur\',1)">Type</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'cur\',3)">Usages</th>';
        html += '<th>Definition</th>';
        html += '</tr></thead><tbody id="cur-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('cur', PA.cursors.data, 50, PA.cursors._renderRow, {
            sortKeys: {
                0: { fn: function(c) { return (c.cursorName || '').toUpperCase(); } },
                1: { fn: function(c) { return (c.cursorType || '').toUpperCase(); } },
                3: { fn: function(c) { return c.accessCount || 0; } }
            },
            renderDetail: PA.cursors._renderDetail,
            searchFn: function(c, q) {
                return (c.cursorName || '').toUpperCase().includes(q) ||
                    (c.queryText || '').toUpperCase().includes(q);
            },
            extraFilter: PA.cursors._filter,
            onFilter: PA.cursors._updateCounts
        });

        var s = PA.tf.state('cur');
        if (s) { s.sortCol = 3; s.sortDir = 'desc'; }
        PA.tf.filter('cur');

        setTimeout(function() {
            PA.tf.initColFilters('cur', {
                0: { label: 'Cursor', valueFn: function(c) { return c.cursorName || ''; } },
                1: { label: 'Type', valueFn: function(c) { return c.cursorType || 'EXPLICIT'; } },
                2: { label: 'Operations', valueFn: function(c) { return (c.operations || []).slice(); } }
            });
            PA.tf._updateSortIndicators('cur');
        }, 0);
    },

    _filter: function(c) {
        var types = PA.cursors._activeTypes;
        if (types.size > 0 && !types.has(c.cursorType || 'EXPLICIT')) return false;
        return true;
    },

    onSearch: function() {
        var el = document.getElementById('curSearch');
        PA.tf.setSearch('cur', el ? el.value : '');
        PA.tf.filter('cur');
    },

    applyFilters: function() {
        PA.tf.filter('cur');
    },

    clearFilters: function() {
        document.getElementById('curSearch').value = '';
        PA.cursors._activeTypes.clear();
        document.querySelectorAll('#curTypePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.tf.setSearch('cur', '');
        var s = PA.tf.state('cur');
        if (s) { s.colFilters = {}; }
        if (PA.tf._state['cur']) PA.tf.setData('cur', PA.cursors.data);
        PA.tf.filter('cur');
        PA.tf._cfUpdateIcons('cur');
    },

    _updateCounts: function() {
        var s = PA.tf.state('cur');
        var allTotal = (PA.cursors.data || []).length;
        var dataTotal = s ? s.data.length : allTotal;
        var shown = s ? s.filtered.length : allTotal;
        var totalEl = document.getElementById('curTotalCount');
        var scope = PA._scope;
        if (scope !== 'full' && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' cursors (' + scope + ')';
        } else {
            totalEl.textContent = allTotal + ' cursors';
        }
        var fc = document.getElementById('curFilteredCount');
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else if (fc) fc.style.display = 'none';
    },

    _renderRow: function(c, idx, esc) {
        var ops = (c.operations || []).map(function(op) {
            var color = op === 'DECLARE' ? 'var(--purple)' : op === 'OPEN' ? 'var(--green)' :
                        op === 'FETCH' || op === 'FETCH_BULK' ? 'var(--blue)' : op === 'CLOSE' ? 'var(--red)' :
                        op === 'FOR_LOOP' ? 'var(--teal)' : 'var(--text-muted)';
            return '<span class="op-badge" style="background:color-mix(in srgb, ' + color + ' 15%, transparent);color:' + color + '">' + op + '</span>';
        }).join('');

        var typeColor = c.cursorType === 'REF_CURSOR' ? 'var(--orange)' :
                        c.cursorType === 'FOR_LOOP' ? 'var(--teal)' :
                        c.cursorType === 'OPEN_FOR' ? 'var(--blue)' : 'var(--purple)';
        var typeLabel = c.cursorType === 'REF_CURSOR' ? 'REF' :
                        c.cursorType === 'FOR_LOOP' ? 'FOR' :
                        c.cursorType === 'OPEN_FOR' ? 'OPEN FOR' : 'EXPLICIT';

        var defPreview = c.queryText ? esc(c.queryText.substring(0, 60)) + (c.queryText.length > 60 ? '...' : '') : '-';

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'cur\',' + idx + ')">';
        html += '<td><span style="font-weight:600;color:var(--purple)">' + esc(c.cursorName || '') + '</span></td>';
        html += '<td><span style="font-size:9px;font-weight:700;color:' + typeColor + '">' + typeLabel + '</span></td>';
        html += '<td>' + ops + '</td>';
        html += '<td>' + (c.accessCount || 0) + '</td>';
        html += '<td style="font-size:10px;font-family:var(--font-mono);color:var(--text-muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + defPreview + '</td>';
        html += '</tr>';
        return html;
    },

    _renderDetail: function(c, idx, esc) {
        var details = c.accessDetails || [];
        var html = '<div class="to-detail">';

        if (c.queryText) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title" style="color:var(--purple)">Cursor Definition</div>';
            html += '<pre style="font-size:11px;line-height:1.4;background:var(--bg);padding:8px;border-radius:4px;max-height:200px;overflow:auto;margin:4px 0;white-space:pre-wrap">' + esc(c.queryText) + '</pre>';
            html += '</div>';
        }

        if (details.length > 0) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Usage Details (' + details.length + ')</div>';
            for (var i = 0; i < details.length; i++) {
                var d = details[i];
                var opColor = d.operation === 'DECLARE' ? 'var(--purple)' :
                              d.operation === 'OPEN' ? 'var(--green)' :
                              d.operation === 'FETCH' || d.operation === 'FETCH_BULK' ? 'var(--blue)' :
                              d.operation === 'CLOSE' ? 'var(--red)' :
                              d.operation === 'FOR_LOOP' ? 'var(--teal)' : 'var(--text-muted)';
                html += '<div class="to-detail-item">';
                html += '<span class="op-badge" style="background:color-mix(in srgb, ' + opColor + ' 15%, transparent);color:' + opColor + '">' + (d.operation || '?') + '</span>';
                html += '<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure(\'' + PA.escJs(d.procedureId || d.procedureName || '') + '\')">' + esc(d.procedureName || '') + '</span>';
                if (d.lineNumber) {
                    html += '<span class="to-detail-line" data-tip="Open source at line" onclick="event.stopPropagation(); PA.sourceView.openAtLine(\'' + PA.escJs(d.sourceFile || '') + '\', ' + d.lineNumber + ')">L' + d.lineNumber + '</span>';
                }
                if (d.queryText && d.operation !== 'DECLARE') {
                    html += '<div class="to-join-predicate">' + esc(d.queryText.substring(0, 120)) + (d.queryText.length > 120 ? '...' : '') + '</div>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        if (!c.queryText && details.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No details available</div>';
        }

        html += '</div>';
        return html;
    },

    applyScope: function() {
        PA.cursors._renderScopeToggle();
        var ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.tf.setData('cur', PA.cursors.data);
            PA.cursors._updateCounts();
            return;
        }
        PA.cursors._applyScopeData();
    },

    _applyScopeData: function() {
        var mode = PA._scope;
        var ctx = PA.context;
        if (!ctx || !ctx.procId || mode === 'full') {
            PA.tf.setData('cur', PA.cursors.data);
            PA.cursors._updateCounts();
            return;
        }
        var nodeIds = ctx.callTreeNodeIds;
        var currentProcId = (ctx.procId || '').toUpperCase();

        var filtered = PA.cursors.data.filter(function(c) {
            return (c.accessDetails || []).some(function(d) {
                var pid = (d.procedureId || d.procedureName || '').toUpperCase();
                if (mode === 'direct') return pid === currentProcId;
                if (mode === 'subtree') return pid !== currentProcId && nodeIds && nodeIds.has(pid);
                return pid === currentProcId || (nodeIds && nodeIds.has(pid));
            });
        });
        PA.tf.setData('cur', filtered);
        PA.cursors._updateCounts();
    }
};
