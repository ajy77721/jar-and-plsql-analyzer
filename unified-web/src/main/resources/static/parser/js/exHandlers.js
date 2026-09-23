window.PA = window.PA || {};

/*
 * Exception handlers report — server-paged (UI-029).
 *
 * Used to aggregate PA.nodeIndex.all() in the browser — every procedure in the analysis — to find
 * the ones with exceptionHandlers > 0. Converted to /procedures?cf=9:Y (see
 * PA.api.exHandlersPageSource), the same shape as tableOps.js's conversion: search, sort and scope
 * all have to travel with the request, or they would silently apply to the fifty rows on screen.
 *
 * One honest degradation, stated rather than hidden: the previous default sort was Handlers
 * descending, computed client-side. `ParserFacetSpecs.PROCEDURES` has no sort key for handler
 * count (or for Type), so those two columns are not sortable here any more — the same "a column
 * absent from serverSorts is not sortable" rule tableOps.js already established. The default is
 * Procedure name ascending instead.
 */
PA.exHandlers = {
    _src: null,

    load: function() {
        PA.exHandlers._initTable();
        PA.exHandlers.applyScope();
    },

    /** The scope selector as request parameters — same shape as PA.tableOps._scopeParams. */
    _scopeParams: function() {
        var ctx = PA.context || {};
        if (!ctx.procId || PA._isPackageRoot()) return { scope: 'full' };
        return { scope: PA._scope, procId: PA._currentScopeId() };
    },

    _initTable: function() {
        var container = document.getElementById('exhContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'exh\',0)">Procedure</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'exh\',1)">Schema</th>';
        html += '<th>Type</th>';
        html += '<th>Handlers</th>';
        html += '<th>Lines</th>';
        html += '</tr></thead><tbody id="exh-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.exHandlers._src = PA.api.exHandlersPageSource(PA.exHandlers._scopeParams());
        PA.exHandlers._src.setParam('sort', 'NAME_ASC');

        PA.tf.init('exh', PA.exHandlers._src, 50, PA.exHandlers._renderRow, {
            // A column absent here is not sortable — Type and Handlers, both computed/derived
            // fields the facet has no sort key for. See the file header for why.
            serverSorts: {
                0: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                1: { asc: 'SCHEMA_ASC', desc: 'SCHEMA_DESC' }
            },
            renderDetail: PA.exHandlers._renderDetail,
            onFilter: PA.exHandlers._updateCounts
        });

        var st = PA.tf.state('exh');
        if (st) { st.sortCol = 0; st.sortDir = 'asc'; }
        PA.tf.filter('exh');
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
        var src = PA.exHandlers._src;
        if (src) src.setParam('q', '');
        PA.tf._restart('exh');
        PA.tf._cfUpdateIcons('exh');
    },

    /**
     * Counts come from the page envelope's own total, not from rows in memory — with one page
     * loaded, counting `data.length` would read "50 procedures" however many actually match.
     */
    _updateCounts: function() {
        var st = PA.tf.state('exh');
        var shown = (st && st.totalIsExact) ? st.serverTotal : (st ? st.filtered.length : 0);
        var totalEl = document.getElementById('exhTotalCount');
        if (totalEl) totalEl.textContent = shown + ' procs';
        var filtEl = document.getElementById('exhFilteredCount');
        if (filtEl) filtEl.style.display = 'none';
        var sumEl = document.getElementById('exhSumCount');
        if (sumEl) sumEl.style.display = 'none'; // per-row handler counts no longer summed across
                                                  // every match — see the file header
    },

    /**
     * Scope now travels with the request — the same conversion as PA.tableOps.applyScope. It used
     * to filter PA.exHandlers.data (itself already a whole-corpus aggregation) in the browser
     * against ctx.callTreeNodeIds; with one page in memory that would scope fifty rows and report
     * the result as the whole answer.
     */
    applyScope: function() {
        var src = PA.exHandlers._src;
        if (!src) return;
        var params = PA.exHandlers._scopeParams();
        src.setParam('scope', params.scope);
        src.setParam('procId', params.procId || '');
        PA.tf._restart('exh');
        PA.exHandlers._updateCounts();
    }
};
