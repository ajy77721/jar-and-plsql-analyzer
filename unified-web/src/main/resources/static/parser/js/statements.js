window.PA = window.PA || {};

PA.statements = {
    data: [],
    _activeTypes: new Set(),

    load: async function() {
        await PA.statements._fetchAndSetTotals();
        PA.statements._initTypePills();
        PA.statements._initTable();
        await PA.statements.applyScope();
    },

    /**
     * UI-024 — was `_aggregate()`, a full `PA.nodeIndex.all()` scan in the browser. Every field it
     * read (`counts.statements`/`nodeId`/`name`/`schema`/`sourceFile`) already lives on the server's
     * own index nodes, so this is now `GET .../statements/summary` (scope=full), computed exactly
     * the same way server-side (`StatementsSummaryService`) instead of duplicated client-side.
     */
    _fetchAndSetTotals: async function() {
        try {
            PA.statements.data = await PA.api.getStatementsSummary('full', null);
            // PA.api returns the PA.FAILED sentinel on a non-ok response instead of throwing
            // (page-envelope.js), so the catch alone would miss every HTTP error.
            if (PA.failed && PA.failed(PA.statements.data)) {
                PA.statements.data = [];
                PA.statements._loadError = 'statements/summary request failed';
                return;
            }
            PA.statements._loadError = null;
        } catch (e) {
            // `[]` here rendered as "No data found" — a failed /statements/summary call and an
            // analysis with genuinely no statements looked identical. Kept and re-applied after
            // _initTable, which resets the tf state.
            console.error('[statements] summary failed', e);
            PA.statements.data = [];
            PA.statements._loadError = (e && e.message) ? e.message : String(e);
        }
    },

    /** Set by the fetchers; re-applied to the table after every _initTable. */
    _loadError: null,

    _applyLoadError: function() {
        if (!PA.tf || !PA.tf.setLoadError) return;
        PA.tf.setLoadError('stmt', PA.statements._loadError, function() { PA.statements.load(); });
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
            RETURN: 'var(--accent-hover)', EXECUTE_IMMEDIATE: 'var(--pink)',
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

    /**
     * Per-detail-panel state, keyed by the PA.tf id (`stmtd-{rowIdx}`). Holds the free-text
     * column boxes and the owning statement type, so a re-filter can be answered without
     * re-deriving either from the DOM.
     */
    _detailPanels: {},

    /** Rows per page inside a detail panel. Same 50 the 22 other PA.tf call sites use. */
    DETAIL_PAGE_SIZE: 50,

    /**
     * The drill-down for one statement type: every procedure that uses it, with its own count.
     *
     * <p>This was a flat, unbounded `<div>` list — no header, no sort, no pagination and no way to
     * narrow it. `procCount` reaches 87 on the PostgreSQL analysis and 44 on DB2, so "which
     * procedures use IF" meant scrolling a wall of undifferentiated lines. It is now a real table
     * driven by `PA.tf`, exactly as `shared/access-details.js` converted the equivalent "Access
     * Details" list: the same `.ad-panel` markup and styles (so no new CSS), the same
     * `{id}-tbody`/`{id}-pager` element contract, and the same mount-on-a-zero-timeout trick —
     * `PA.tf.toggleDetail` assigns `innerHTML` immediately after this returns, so by the time the
     * timeout runs the table is in the document.
     */
    _renderDetail: function(s, idx, esc) {
        var procs = s.procs || [];
        var html = '<div class="to-detail">';

        if (procs.length === 0) {
            html += '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No procedure details</div>';
            html += '</div>';
            return html;
        }

        var id = 'stmtd-' + idx;
        PA.statements._detailPanels[id] = { type: s.type, total: s.totalCount, procs: procs, colFilters: {} };
        setTimeout(function() { PA.statements._mountDetail(id); }, 0);

        html += '<div class="to-detail-section ad-panel">';
        html += '<div class="to-detail-section-title">Procedures using ' + esc(s.type)
              + ' (' + procs.length + ')</div>';

        html += '<div class="ad-controls" onclick="event.stopPropagation()">';
        html += '<input type="search" class="ad-search" id="' + id + '-q"'
              + ' placeholder="Filter ' + procs.length + ' procedures — name, schema or count"'
              + ' oninput="PA.statements.detailSearch(\'' + PA.escJs(id) + '\', this.value)">';
        html += '<span class="ad-shown" id="' + id + '-shown"></span>';
        html += '</div>';

        html += '<table class="to-table ad-table" id="' + id + '"><thead><tr>'
              + '<th data-sort-col="0" onclick="event.stopPropagation();PA.tf.sort(\'' + PA.escJs(id) + '\',0)">Procedure</th>'
              + '<th data-sort-col="1" onclick="event.stopPropagation();PA.tf.sort(\'' + PA.escJs(id) + '\',1)">Schema</th>'
              + '<th data-sort-col="2" onclick="event.stopPropagation();PA.tf.sort(\'' + PA.escJs(id) + '\',2)">Count</th>'
              + '<th data-sort-col="3" onclick="event.stopPropagation();PA.tf.sort(\'' + PA.escJs(id) + '\',3)">Share</th>'
              + '<th>Source</th>'
              + '</tr>'
              // One box per column. A single global box cannot answer "the ERP_USP_* procedures in
              // schema X" — those are different questions, so they get different inputs.
              + '<tr class="ad-filter-row">'
              + '<th>' + PA.statements._detailColFilter(id, 'proc', 'procedure') + '</th>'
              + '<th>' + PA.statements._detailColFilter(id, 'schema', 'schema') + '</th>'
              + '<th>' + PA.statements._detailColFilter(id, 'count', 'min count') + '</th>'
              + '<th></th><th></th>'
              + '</tr></thead>';
        html += '<tbody id="' + id + '-tbody"></tbody></table>';
        // `pagination-bar` is the class the app's other 44 pagers use and is already styled;
        // `ad-pager` only tweaks its borders inside a detail panel.
        html += '<div id="' + id + '-pager" class="pagination-bar ad-pager"></div>';
        html += '</div></div>';
        return html;
    },

    _detailColFilter: function(id, key, placeholder) {
        return '<input type="search" class="ad-cf" id="' + id + '-cf-' + key + '"'
             + ' placeholder="' + PA.escAttr(placeholder) + '" onclick="event.stopPropagation()"'
             + ' oninput="PA.statements.detailColFilter(\'' + PA.escJs(id) + '\',\'' + PA.escJs(key) + '\',this.value)">';
    },

    _mountDetail: function(id) {
        var st = PA.statements._detailPanels[id];
        if (!st) return;
        if (!document.getElementById(id + '-tbody')) return;   // row was collapsed again first

        PA.tf.init(id, st.procs, PA.statements.DETAIL_PAGE_SIZE, PA.statements._renderDetailRow(id), {
            sortKeys: {
                0: { fn: function(p) { return (p.name || '').toUpperCase(); } },
                1: { fn: function(p) { return (p.schema || '').toUpperCase(); } },
                2: { fn: function(p) { return p.count || 0; } },
                3: { fn: function(p) { return p.count || 0; } }
            },
            // Only the three visible fields are searched. PA.tf's default stringifies every
            // property, which here would match on nodeId — an opaque id that is not on screen, so
            // a hit against it reads as a false positive.
            searchFn: function(p, q) {
                return String(p.name || '').toUpperCase().includes(q)
                    || String(p.schema || '').toUpperCase().includes(q)
                    || String(p.count == null ? '' : p.count).includes(q);
            },
            extraFilter: function(p) {
                var cf = (PA.statements._detailPanels[id] || {}).colFilters || {};
                if (cf.proc && String(p.name || '').toUpperCase().indexOf(cf.proc) === -1) return false;
                if (cf.schema && String(p.schema || '').toUpperCase().indexOf(cf.schema) === -1) return false;
                if (cf.count) {
                    // Numeric where the box holds a number ("show me the heavy users"), substring
                    // otherwise, so a partially typed filter still behaves.
                    var min = parseFloat(cf.count);
                    if (!isNaN(min) && /^\s*\d+(\.\d+)?\s*$/.test(cf.count)) {
                        if ((p.count || 0) < min) return false;
                    } else if (String(p.count == null ? '' : p.count).indexOf(cf.count) === -1) {
                        return false;
                    }
                }
                return true;
            },
            onFilter: function() { PA.statements._updateDetailShown(id); }
        });

        // Heaviest user first: the question this panel answers is "who does the most of this".
        var tfs = PA.tf.state(id);
        if (tfs) { tfs.sortCol = 2; tfs.sortDir = 'desc'; }
        PA.tf.filter(id);
        if (PA.tf._updateSortIndicators) PA.tf._updateSortIndicators(id);
        PA.statements._updateDetailShown(id);
    },

    _renderDetailRow: function(id) {
        return function(p, idx, esc) {
            var st = PA.statements._detailPanels[id] || {};
            var color = PA.statements._getTypeColor(st.type);
            var total = st.total || 0;
            var pct = total > 0 ? Math.round(((p.count || 0) / total) * 100) : 0;
            var colorObj = PA.getSchemaColor(p.schema);

            var html = '<tr class="ad-row">';
            html += '<td><span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure(\''
                  + PA.escJs(p.nodeId) + '\')">' + esc(p.name || '') + '</span></td>';
            html += '<td>' + (p.schema
                ? '<span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg
                  + ';font-size:8px;padding:1px 5px">' + esc(p.schema) + '</span>'
                : '<span class="ad-dim">-</span>') + '</td>';
            html += '<td><span class="stmt-proc-count" style="color:' + color + '">' + (p.count || 0) + '&times;</span></td>';
            html += '<td><div class="stmt-bar-wrap"><div class="stmt-bar" style="width:' + Math.min(100, pct)
                  + '%;background:' + color + '"></div><span class="stmt-bar-label">' + pct + '%</span></div></td>';
            html += '<td>' + (p.sourceFile
                ? '<span class="to-detail-line" data-tip="View source code" onclick="event.stopPropagation(); PA.sourceView.openModal(\''
                  + PA.escJs(p.nodeId) + '\')">source</span>'
                : '<span class="ad-dim">-</span>') + '</td>';
            html += '</tr>';
            return html;
        };
    },

    /** Reports how many rows the filter leaves, so a narrowed list is never mistaken for all. */
    _updateDetailShown: function(id) {
        var el = document.getElementById(id + '-shown');
        var s = PA.tf.state(id);
        if (!el || !s) return;
        var total = (s.data || []).length;
        var shown = (s.filtered || []).length;
        el.textContent = shown === total ? '' : shown + ' of ' + total;
    },

    detailSearch: function(id, value) {
        if (!PA.tf.state(id)) return;
        PA.tf.setSearch(id, window.SearchQuery
            ? SearchQuery.normUpper(value)
            : String(value || '').trim().toUpperCase());
        PA.tf.filter(id);
        PA.statements._updateDetailShown(id);
    },

    detailColFilter: function(id, key, value) {
        var st = PA.statements._detailPanels[id];
        if (!st || !PA.tf.state(id)) return;
        st.colFilters = st.colFilters || {};
        st.colFilters[key] = window.SearchQuery
            ? SearchQuery.normUpper(value)
            : String(value || '').trim().toUpperCase();
        PA.tf.filter(id);
        PA.statements._updateDetailShown(id);
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

    /**
     * UI-024 — was `_applyScopeData()` calling the local `_aggregateScoped()` scan; now fetches the
     * same scoped aggregate from the server (`StatementsSummaryService`, via `ParserScopeResolver`
     * — the exact component that already reproduces `scope.js`'s own semantics for every other
     * scoped facet table). `PA.statements.data` itself stays the full/unscoped aggregate (used by
     * the type-filter pills); only the table's own row data (`PA.tf`) is swapped per scope.
     */
    applyScope: async function() {
        var ctx = PA.context;
        var mode = PA._scope;
        if (!ctx || !ctx.procId || mode === 'full') {
            PA.tf.setData('stmt', PA.statements.data);
            PA.statements._updateCounts();
            PA.statements._applyLoadError();
            return;
        }
        var scoped;
        try {
            scoped = await PA.api.getStatementsSummary(mode, PA._currentScopeId());
            if (PA.failed && PA.failed(scoped)) {
                scoped = [];
                PA.statements._loadError = 'statements/summary request failed';
            } else {
                PA.statements._loadError = null;
            }
        } catch (e) {
            console.error('[statements] scoped summary failed', e);
            scoped = [];
            PA.statements._loadError = (e && e.message) ? e.message : String(e);
        }
        PA.tf.setData('stmt', scoped);
        PA.statements._updateCounts();
        PA.statements._applyLoadError();
    }
};
