window.PA = window.PA || {};

PA.sequences = {
    data: [],
    _activeOps: new Set(),

    async load() {
        PA.sequences.data = await PA.api.getSequenceOperations();
        PA.sequences._initOpPills();
        PA.sequences._initTable();
        PA.sequences.applyScope();
    },

    _initOpPills: function() {
        var container = document.getElementById('seqOpPills');
        if (!container) return;
        var ops = ['NEXTVAL', 'CURRVAL'];
        var html = '';
        for (var i = 0; i < ops.length; i++) {
            var o = ops[i];
            html += '<span class="op-filter-pill active" data-so="' + o + '" onclick="PA.sequences.toggleOp(\'' + o + '\')">' + o + '</span>';
        }
        container.innerHTML = html;
        PA.sequences._activeOps = new Set();
    },

    _initScopeToggle: function() {
        PA.sequences._renderScopeToggle();
    },

    _renderScopeToggle: function() {
        var container = document.getElementById('seqScopeToggle');
        if (!container) return;
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

    toggleOp: function(op) {
        var pill = document.querySelector('#seqOpPills .op-filter-pill[data-so="' + op + '"]');
        if (!pill) return;
        if (PA.sequences._activeOps.size === 0) {
            PA.sequences._activeOps.add(op);
            document.querySelectorAll('#seqOpPills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.so === op);
            });
        } else if (PA.sequences._activeOps.has(op)) {
            PA.sequences._activeOps.delete(op);
            pill.classList.remove('active');
            if (PA.sequences._activeOps.size === 0) {
                document.querySelectorAll('#seqOpPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.sequences._activeOps.add(op);
            pill.classList.add('active');
        }
        PA.tf.filter('seq');
    },

    _initTable: function() {
        var container = document.getElementById('seqContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'seq\',0)">Sequence</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'seq\',1)">Schema</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'seq\',3)">Usages</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'seq\',4)">Procedures</th>';
        html += '</tr></thead><tbody id="seq-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('seq', PA.sequences.data, 50, PA.sequences._renderRow, {
            sortKeys: {
                0: { fn: function(s) { return (s.sequenceName || '').toUpperCase(); } },
                1: { fn: function(s) { return (s.sequenceSchema || '').toUpperCase(); } },
                3: { fn: function(s) { return s.accessCount || 0; } },
                4: { fn: function(s) { return PA.sequences._procCount(s); } }
            },
            renderDetail: PA.sequences._renderDetail,
            searchFn: function(s, q) {
                return (s.sequenceName || '').toUpperCase().includes(q) ||
                    (s.sequenceSchema || '').toUpperCase().includes(q);
            },
            extraFilter: PA.sequences._filter,
            onFilter: PA.sequences._updateCounts
        });

        var st = PA.tf.state('seq');
        if (st) { st.sortCol = 3; st.sortDir = 'desc'; }
        PA.tf.filter('seq');

        setTimeout(function() {
            PA.tf.initColFilters('seq', {
                0: { label: 'Sequence', valueFn: function(s) { return s.sequenceName || ''; } },
                1: { label: 'Schema', valueFn: function(s) { return s.sequenceSchema || '-'; } },
                2: { label: 'Operations', valueFn: function(s) { return (s.operations || []).slice(); } }
            });
            PA.tf._updateSortIndicators('seq');
        }, 0);
    },

    _procCount: function(s) {
        var seen = {};
        var details = s.accessDetails || [];
        for (var i = 0; i < details.length; i++) {
            var pid = details[i].procedureId || details[i].procedureName || '';
            if (pid) seen[pid] = true;
        }
        return Object.keys(seen).length;
    },

    _filter: function(s) {
        var ops = PA.sequences._activeOps;
        if (ops.size === 0) return true;
        var seqOps = s.operations || [];
        for (var i = 0; i < seqOps.length; i++) {
            if (ops.has(seqOps[i])) return true;
        }
        return false;
    },

    onSearch: function() {
        var el = document.getElementById('seqSearch');
        PA.tf.setSearch('seq', el ? el.value : '');
        PA.tf.filter('seq');
    },

    clearFilters: function() {
        var search = document.getElementById('seqSearch');
        if (search) search.value = '';
        PA.sequences._activeOps.clear();
        document.querySelectorAll('#seqOpPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.tf.setSearch('seq', '');
        var st = PA.tf.state('seq');
        if (st) { st.colFilters = {}; }
        if (PA.tf._state['seq']) PA.tf.setData('seq', PA.sequences.data);
        PA.tf.filter('seq');
        PA.tf._cfUpdateIcons('seq');
    },

    _updateCounts: function() {
        var st = PA.tf.state('seq');
        var allTotal = (PA.sequences.data || []).length;
        var dataTotal = st ? st.data.length : allTotal;
        var shown = st ? st.filtered.length : allTotal;
        var totalEl = document.getElementById('seqTotalCount');
        if (totalEl) {
            var scope = PA._scope;
            if (scope !== 'full' && PA.context.procId) {
                totalEl.textContent = dataTotal + '/' + allTotal + ' sequences (' + scope + ')';
            } else {
                totalEl.textContent = allTotal + ' sequences';
            }
        }
        var fc = document.getElementById('seqFilteredCount');
        if (fc) {
            if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
            else fc.style.display = 'none';
        }
    },

    _renderRow: function(s, idx, esc) {
        var ops = (s.operations || []).map(function(op) {
            var color = op === 'NEXTVAL' ? 'var(--seq-nextval)' : 'var(--seq-currval)';
            return '<span class="op-badge" style="background:color-mix(in srgb, ' + color + ' 15%, transparent);color:' + color + '">' + op + '</span>';
        }).join('');

        var colorObj = PA.getSchemaColor(s.sequenceSchema);
        var schemaHtml = s.sequenceSchema
            ? '<span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + '">' + esc(s.sequenceSchema) + '</span>'
            : '<span style="color:var(--text-muted)">-</span>';

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'seq\',' + idx + ')">';
        html += '<td><span class="seq-name">' + esc(s.sequenceName || '') + '</span></td>';
        html += '<td>' + schemaHtml + '</td>';
        html += '<td>' + ops + '</td>';
        html += '<td style="font-weight:700">' + (s.accessCount || 0) + '</td>';
        html += '<td>' + PA.sequences._procCount(s) + '</td>';
        html += '</tr>';
        return html;
    },

    _renderDetail: function(s, idx, esc) {
        var details = s.accessDetails || [];
        var html = '<div class="to-detail">';

        if (details.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No usage details available</div>';
            html += '</div>';
            return html;
        }

        html += '<div class="to-detail-section">';
        html += '<div class="to-detail-section-title" style="color:var(--seq-nextval)">Usage Details (' + details.length + ')</div>';

        for (var i = 0; i < details.length; i++) {
            var d = details[i];
            var opColor = d.operation === 'NEXTVAL' ? 'var(--seq-nextval)' : 'var(--seq-currval)';
            html += '<div class="to-detail-item">';
            html += '<span class="op-badge" style="background:color-mix(in srgb, ' + opColor + ' 15%, transparent);color:' + opColor + '">' + (d.operation || '?') + '</span>';
            html += '<span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure(\'' + PA.escJs(d.procedureId || d.procedureName || '') + '\')">' + esc(d.procedureName || '') + '</span>';
            if (d.lineNumber) {
                html += '<span class="to-detail-line" data-tip="Open source at line" onclick="event.stopPropagation(); PA.sourceView.openAtLine(\'' + PA.escJs(d.sourceFile || '') + '\', ' + d.lineNumber + ')">L' + d.lineNumber + '</span>';
            }
            html += '</div>';
        }

        html += '</div></div>';
        return html;
    },

    applyScope: function() {
        PA.sequences._renderScopeToggle();
        var ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.tf.setData('seq', PA.sequences.data);
            PA.sequences._updateCounts();
            return;
        }
        PA.sequences._applyScopeData();
    },

    _applyScopeData: function() {
        var mode = PA._scope;
        var ctx = PA.context;
        if (!ctx || !ctx.procId || mode === 'full') {
            PA.tf.setData('seq', PA.sequences.data);
            PA.sequences._updateCounts();
            return;
        }
        var nodeIds = ctx.callTreeNodeIds;
        var currentProcId = (ctx.procId || '').toUpperCase();

        var filtered = PA.sequences.data.filter(function(s) {
            return (s.accessDetails || []).some(function(d) {
                var pid = (d.procedureId || d.procedureName || '').toUpperCase();
                if (mode === 'direct') return pid === currentProcId;
                if (mode === 'subtree') return pid !== currentProcId && nodeIds && nodeIds.has(pid);
                return pid === currentProcId || (nodeIds && nodeIds.has(pid));
            });
        });
        PA.tf.setData('seq', filtered);
        PA.sequences._updateCounts();
    }
};
