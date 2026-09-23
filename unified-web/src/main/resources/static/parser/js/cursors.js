window.PA = window.PA || {};

/*
 * Cursor report — server-paged (PLAN-08 §4.3 step 12).
 *
 * See sequences.js for the shape of the conversion. The search box, the column headers, the type
 * pills, the column-filter popovers and the scope selector all travel with the page request; any
 * one of them left in the browser would apply to the fifty rows in memory and look convincing.
 */
PA.cursors = {
    // Retained for callers that test "is this loaded"; the rows are no longer held client-side.
    data: [],
    _src: null,
    _summary: null,

    async load() {
        PA.cursors._src = PA.api.facetPageSource('cursors', PA.cursors._scopeParams());
        // The table has always opened on Usages descending; asked for up front so the first fetch
        // is already in that order rather than being re-fetched after init.
        PA.cursors._src.setParam('sort', 'USAGES_DESC');
        PA.cursors._initTypePills();
        PA.cursors._initScopeToggle();
        PA.cursors._initTable();
        PA.tf.pendingColFilters('cur', PA.cursors._colDefs());
        // Waits for page 1 so _loadSummary can tell an empty facet from a populated one and skip
        // a request that would have nothing to report.
        var st = PA.tf.state('cur');
        if (st && st.firstLoad) await st.firstLoad;
        await PA.cursors._loadSummary();
    },

    /** Keys are column indices and MUST match `cur`'s <th> order. */
    _colDefs: function() {
        return {
            0: { label: 'Cursor', valueFn: function(c) { return c.cursorName || ''; } },
            1: { label: 'Type', valueFn: function(c) { return c.cursorType || 'EXPLICIT'; } },
            2: { label: 'Operations', valueFn: function(c) { return (c.operations || []).slice(); } }
        };
    },

    /** The scope selector as request parameters. */
    _scopeParams: function() {
        var ctx = PA.context || {};
        if (!ctx.procId || PA._isPackageRoot()) return { scope: 'full' };
        return { scope: PA._scope, procId: PA._currentScopeId() };
    },

    _loadSummary: async function() {
        try {
            PA.cursors._summary = await PA.api.facetSummary('cursors', PA.cursors._scopeParams());
            PA.cursors._updateCounts();
            PA.cursors._initColFilters();
        } catch (e) {
            PA.tf.colFiltersFailed('cur', PA.cursors._colDefs(), 'request failed');
        }
    },

    _initColFilters: function() {
        var sum = PA.cursors._summary;
        if (!sum || !sum.columnValues) {
            PA.tf.colFiltersFailed('cur', PA.cursors._colDefs(), 'no filter values returned');
            return;
        }
        PA.tf.initColFilters('cur', PA.cursors._colDefs(), sum.columnValues);
        PA.tf._updateSortIndicators('cur');
    },

    /** Sends the pill selection as `types` and reloads page 1. */
    _pushTypes: function() {
        var src = PA.cursors._src;
        if (!src) return;
        var t = PA.cursors._activeTypes;
        // No pill selected means "all", which is an absent parameter — not `types=`, which would
        // be a request for the empty set.
        src.setParam('types', t.size ? [...t].join(',') : '');
        PA.tf._restart('cur');
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
            { key: 'direct', label: 'Direct', title: 'Direct: only THIS procedure/node — nothing it calls.' },
            { key: 'subtree', label: 'Subtree', title: 'Subtree: everything this node calls (all descendants), but NOT this node itself.' },
            { key: 'subflow', label: 'SubFlow', title: 'SubFlow: this node PLUS all of its descendants combined (the whole flow rooted here).' },
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
        PA.cursors._pushTypes();
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

        PA.tf.init('cur', PA.cursors._src, 50, PA.cursors._renderRow, {
            // The endpoint's closed sort enum, keyed by the column index the <th> carries. A
            // column absent from this map is not sortable — the honest answer, since sorting in
            // the browser would reorder the fifty rows on screen and nothing else.
            serverSorts: {
                0: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                1: { asc: 'TYPE_ASC', desc: 'TYPE_DESC' },
                3: { asc: 'USAGES_ASC', desc: 'USAGES_DESC' }
            },
            renderDetail: PA.cursors._renderDetail,
            onFilter: PA.cursors._updateCounts
        });

        // Reflects the sort already requested in load(), so the header arrow matches the order the
        // first page arrived in. Column filters are attached once the summary supplies their
        // value lists.
        var s = PA.tf.state('cur');
        if (s) { s.sortCol = 3; s.sortDir = 'desc'; }
        PA.tf._updateSortIndicators('cur');
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
        var s = PA.tf.state('cur');
        if (s) { s.colFilters = {}; }
        var src = PA.cursors._src;
        if (src) {
            // Cleared together and reloaded once, rather than one round trip per parameter.
            src.setParam('q', '');
            src.setParam('types', '');
            src.setParam('cf', []);
        }
        PA.tf.setSearch('cur', '');
        PA.tf._restart('cur');
        PA.tf._cfUpdateIcons('cur');
    },

    /**
     * The header line. Total and total-in-scope come from `/cursors/summary`, which are properties
     * of the whole result set; the "shown" count is the envelope's `total`, the number of rows
     * matching the filters — not the size of the page, so it does not shrink while paging.
     */
    _updateCounts: function() {
        var s = PA.tf.state('cur');
        var sum = PA.cursors._summary;
        var allTotal = sum ? sum.total : 0;
        var dataTotal = sum ? sum.scopedTotal : allTotal;
        var shown = (s && s.totalIsExact) ? s.serverTotal : dataTotal;
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
            html += '<pre style="font-size:11px;line-height:1.4;background:var(--bg);padding:8px;border-radius:var(--radius-sm);max-height:200px;overflow:auto;margin:4px 0;white-space:pre-wrap">' + esc(c.queryText) + '</pre>';
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

    /**
     * The Direct / Subtree / SubFlow / Full selector, now a request parameter.
     *
     * It used to filter the whole facet here, which is why the whole facet had to be downloaded.
     * `ParserScopeResolver` resolves the same node set from the call tree server-side. The summary
     * is refetched too: the scope changes both the counts and the values a column filter offers.
     */
    applyScope: function() {
        PA.cursors._renderScopeToggle();
        var src = PA.cursors._src;
        if (!src) return;
        var params = PA.cursors._scopeParams();
        src.setParam('scope', params.scope);
        src.setParam('procId', params.procId || '');
        PA.tf._restart('cur');
        PA.cursors._loadSummary();
    }
};
