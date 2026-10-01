window.PA = window.PA || {};

/*
 * Views & Materialized Views — the estate facet's first screen.
 *
 * Server-paged like cursors.js: the list is a `PA.pageSource` over `/views`, so the search box and
 * the type pills travel with the page request (`q`, `types`) rather than filtering the fifty rows
 * on screen. The list rows already carry definition/columns/base tables/used-by; the only thing a
 * row does NOT carry is the multi-level chain, so expanding a row lazily fetches
 * `/views/{view}` (row + chain + usedBy) once and caches it.
 *
 * A view the analysis does not have is a 404 — reported as "not analyzed", never as an empty list.
 */
PA.estateViews = {
    data: [],
    _src: null,
    /** name -> detail object, or null for a 404. `undefined` means "not fetched yet". */
    _details: {},
    _activeTypes: new Set(),

    async load() {
        PA.estateViews._details = {};
        PA.estateViews._activeTypes = new Set();
        PA.estateViews._src = PA.api.viewsPageSource({});
        PA.estateViews._src.setParam('sort', 'NAME_ASC');
        PA.estateViews._initTypePills();
        PA.estateViews._initTable();
    },

    /** Reset when the analysis changes. */
    reset() {
        PA.estateViews._details = {};
        PA.estateViews.data = [];
    },

    /**
     * Cross-estate link target: switch to the Estate → Views screen, filter to this view and open
     * its detail row. Used by the chain/base-table/used-by links on the other estate screens.
     */
    showView: function(name) {
        if (!name) return;
        if (PA.switchRightTab) PA.switchRightTab('estate');
        if (PA.estateTab && PA.estateTab.switchSub) PA.estateTab.switchSub('views');
        var el = document.getElementById('vwSearch');
        if (el) el.value = name;
        var src = PA.estateViews._src;
        if (src && PA.tf) {
            src.setParam('q', name);
            PA.tf._restart('vw');
        }
        PA.estateViews._focus('vw', name, 'name');
    },

    /** Poll for the named row in a server-paged table, then toggle its detail open and scroll to it. */
    _focus: function(tfId, name, field) {
        var tries = 0;
        var want = String(name).toUpperCase();
        var iv = setInterval(function() {   // poll-exempt: local DOM poll for a just-navigated row, no network request
            var st = PA.tf && PA.tf.state ? PA.tf.state(tfId) : null;
            var rows = st && st.filtered;
            if (rows && rows.length) {
                for (var i = 0; i < rows.length; i++) {
                    if (String(rows[i][field] || '').toUpperCase() === want) {
                        clearInterval(iv);
                        var idx = st.page * st.pageSize + i;
                        PA.tf.toggleDetail(tfId, idx);
                        var rowEl = document.getElementById(tfId + '-detail-' + idx);
                        if (rowEl && rowEl.scrollIntoView) rowEl.scrollIntoView({ block: 'center' });
                        return;
                    }
                }
            }
            if (++tries > 40) clearInterval(iv);
        }, 100);
    },

    _initTypePills: function() {
        var container = document.getElementById('vwTypePills');
        if (!container) return;
        var types = [
            { v: 'VIEW', label: 'View' },
            { v: 'MATERIALIZED_VIEW', label: 'Materialized View' }
        ];
        var html = '';
        for (var i = 0; i < types.length; i++) {
            html += '<span class="op-filter-pill active" data-vwt="' + types[i].v + '" '
                + 'onclick="PA.estateViews.toggleType(\'' + types[i].v + '\')">' + types[i].label + '</span>';
        }
        container.innerHTML = html;
    },

    toggleType: function(t) {
        var pill = document.querySelector('#vwTypePills .op-filter-pill[data-vwt="' + t + '"]');
        if (!pill) return;
        if (PA.estateViews._activeTypes.size === 0) {
            PA.estateViews._activeTypes.add(t);
            document.querySelectorAll('#vwTypePills .op-filter-pill').forEach(function(p) {
                p.classList.toggle('active', p.dataset.vwt === t);
            });
        } else if (PA.estateViews._activeTypes.has(t)) {
            PA.estateViews._activeTypes.delete(t);
            pill.classList.remove('active');
            if (PA.estateViews._activeTypes.size === 0) {
                document.querySelectorAll('#vwTypePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
            }
        } else {
            PA.estateViews._activeTypes.add(t);
            pill.classList.add('active');
        }
        PA.estateViews._pushTypes();
    },

    _pushTypes: function() {
        var src = PA.estateViews._src;
        if (!src) return;
        var t = PA.estateViews._activeTypes;
        // No pill selected means "all", which is an absent parameter — not `types=`.
        src.setParam('types', t.size ? [...t].join(',') : '');
        PA.tf._restart('vw');
    },

    _initTable: function() {
        var container = document.getElementById('vwContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'vw\',0)">View</th>';
        html += '<th>Type</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'vw\',2)">Schema</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'vw\',3)">Columns</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'vw\',4)">Base Tables</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'vw\',5)">Used By</th>';
        html += '</tr></thead><tbody id="vw-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('vw', PA.estateViews._src, 50, PA.estateViews._renderRow, {
            serverSorts: {
                0: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                2: { asc: 'SCHEMA_ASC', desc: 'SCHEMA_DESC' },
                3: { asc: 'COLUMNS_ASC', desc: 'COLUMNS_DESC' },
                4: { asc: 'BASETABLES_ASC', desc: 'BASETABLES_DESC' },
                5: { asc: 'USEDBY_ASC', desc: 'USEDBY_DESC' }
            },
            renderDetail: PA.estateViews._renderDetail,
            onFilter: function() {
                PA.estateViews._updateCounts();
                PA.estateViews._maybeEmpty();
            }
        });
        var st = PA.tf.state('vw');
        if (st) { st.sortCol = 0; st.sortDir = 'asc'; }
        PA.tf._updateSortIndicators('vw');
    },

    onSearch: function() {
        var el = document.getElementById('vwSearch');
        PA.estateViews._src.setParam('q', el ? el.value : '');
        PA.tf._restart('vw');
    },

    clearFilters: function() {
        var el = document.getElementById('vwSearch');
        if (el) el.value = '';
        PA.estateViews._activeTypes.clear();
        document.querySelectorAll('#vwTypePills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        var src = PA.estateViews._src;
        if (src) { src.setParam('q', ''); src.setParam('types', ''); }
        PA.tf._restart('vw');
    },

    /** True when a server-side filter (search or type pill) is narrowing the result set. */
    _isFiltered: function() {
        var src = PA.estateViews._src;
        return !!(src && src.param('q')) || PA.estateViews._activeTypes.size > 0;
    },

    _updateCounts: function() {
        var st = PA.tf.state('vw');
        var totalEl = document.getElementById('vwTotalCount');
        if (!totalEl || !st) return;
        var total = st.totalIsExact ? st.serverTotal : -1;
        totalEl.textContent = (total >= 0 ? total : (st.filtered ? st.filtered.length : 0))
            + (PA.estateViews._isFiltered() ? ' matched' : ' views');
    },

    /** Friendly empty state — the framework's default "No data found" says nothing about why. */
    _maybeEmpty: function() {
        var st = PA.tf.state('vw');
        var tb = document.getElementById('vw-tbody');
        if (!st || !tb) return;
        if (st.loading || st.loadError) return;
        if (st.totalIsExact && st.serverTotal === 0) {
            tb.innerHTML = '<tr><td colspan="20" class="empty-msg" style="padding:24px;text-align:center">'
                + (PA.estateViews._isFiltered()
                    ? 'No views match the current filters.'
                    : 'No views or materialized views in this analysis.')
                + '</td></tr>';
        }
    },

    _renderRow: function(v, idx, esc) {
        var isMv = v.objectType === 'MATERIALIZED_VIEW' || v.isMaterialized;
        var typeBadge = '<span class="est-badge ' + (isMv ? 'mv' : 'view') + '">'
            + (isMv ? 'MATERIALIZED VIEW' : 'VIEW') + '</span>';

        var schemaHtml = '<span style="color:var(--text-muted)">-</span>';
        if (v.schema) {
            var c = PA.getSchemaColor(v.schema);
            schemaHtml = '<span class="ct-schema-badge" style="background:' + c.bg + ';color:' + c.fg + '">' + esc(v.schema) + '</span>';
        }

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'vw\',' + idx + ')">';
        html += '<td><span style="font-weight:600;color:var(--badge-blue)">' + esc(v.name || '') + '</span></td>';
        html += '<td>' + typeBadge + '</td>';
        html += '<td>' + schemaHtml + '</td>';
        html += '<td>' + (v.columnCount || 0) + '</td>';
        html += '<td>' + (v.baseTableCount || 0) + '</td>';
        html += '<td>' + (v.usedByCount || 0) + '</td>';
        html += '</tr>';
        return html;
    },

    /** Lazy detail row: fetch `/views/{view}` on first open, then render from the cache. */
    _renderDetail: function(v, idx, esc) {
        var cached = PA.estateViews._details[v.name];
        if (cached === undefined) {
            PA.estateViews._fetchDetail(v, idx);
            return '<div class="est-detail"><div class="est-loading">Loading view detail…</div></div>';
        }
        if (!cached) return '<div class="est-detail">' + PA.estateViews._notAnalyzed(v.name) + '</div>';
        return '<div class="est-detail">' + PA.estateViews._detailHtml(cached, esc) + '</div>';
    },

    _fetchDetail: function(v, idx) {
        var rowEl = document.getElementById('vw-detail-' + idx);
        var fill = function(detail) {
            PA.estateViews._details[v.name] = detail;
            if (rowEl && rowEl.isConnected) {
                rowEl.innerHTML = '<td colspan="20">' + PA.estateViews._renderDetail(v, idx, PA.esc) + '</td>';
            }
        };
        PA.api.getViewDetail(v.name).then(function(d) {
            fill((d && !(PA.failed && PA.failed(d))) ? d : null);
        }).catch(function() {
            fill(null);
        });
    },

    _notAnalyzed: function(name) {
        return '<div class="est-notanalyzed"><b>Not analyzed — ' + PA.esc(name) + '</b>'
            + 'This view is listed, but its detail artifact is not present in this analysis. '
            + 'Re-run the analysis to populate the view definition, columns and chain.</div>';
    },

    _detailHtml: function(d, esc) {
        var isMv = d.objectType === 'MATERIALIZED_VIEW' || d.isMaterialized;
        var html = '';

        html += '<div class="est-meta">';
        html += '<span class="est-badge ' + (isMv ? 'mv' : 'view') + '">' + (isMv ? 'MATERIALIZED VIEW' : 'VIEW') + '</span>';
        if (d.schema) html += '<span class="est-meta-item">schema <b>' + esc(d.schema) + '</b></span>';
        html += '<span class="est-meta-item"><b>' + (d.columnCount || 0) + '</b> columns</span>';
        html += '<span class="est-meta-item"><b>' + (d.baseTableCount || 0) + '</b> base tables</span>';
        if (d.joinCount) html += '<span class="est-meta-item"><b>' + d.joinCount + '</b> joins</span>';
        if (d.indexCount) html += '<span class="est-meta-item"><b>' + d.indexCount + '</b> indexes</span>';
        html += '<span class="est-meta-item"><b>' + (d.usedByCount || 0) + '</b> used by</span>';
        if (isMv) {
            if (d.mvRefreshMode) html += '<span class="est-badge warn">REFRESH ' + esc(d.mvRefreshMode) + '</span>';
            if (d.mvRefreshMethod) html += '<span class="est-badge">' + esc(d.mvRefreshMethod) + '</span>';
            if (d.lastRefresh) html += '<span class="est-meta-item">last refresh <b>' + esc(d.lastRefresh) + '</b></span>';
        }
        html += '</div>';

        // Definition — DB-sourced SQL is DATA: escaped, inside <pre>.
        html += '<div class="est-section"><div class="est-section-title">Definition</div>';
        html += d.definition
            ? '<pre class="est-sql">' + esc(d.definition) + '</pre>'
            : '<div class="est-empty">No definition captured for this view.</div>';
        html += '</div>';

        // Columns
        html += '<div class="est-section"><div class="est-section-title">Columns (' + (d.columnCount || 0) + ')</div>';
        if (d.columns && d.columns.length) {
            html += '<table class="est-cols"><thead><tr><th>Name</th><th>Data Type</th><th>Nullable</th></tr></thead><tbody>';
            for (var i = 0; i < d.columns.length; i++) {
                var col = d.columns[i];
                html += '<tr><td class="est-col-name">' + esc(col.name || '') + '</td>'
                    + '<td class="est-col-type">' + esc(col.dataType || '') + '</td>'
                    + '<td>' + (col.nullable ? 'YES' : 'NO') + '</td></tr>';
            }
            html += '</tbody></table>';
        } else {
            html += '<div class="est-empty">No columns captured.</div>';
        }
        html += '</div>';

        // Base tables
        html += '<div class="est-section"><div class="est-section-title">Base Tables (' + (d.baseTableCount || 0) + ')</div>';
        html += PA.estateViews._baseTablesHtml(d.baseTables, esc);
        html += '</div>';

        // Joins inside the view definition (catalog/grammar-captured).
        if (d.joins && d.joins.length) {
            html += '<div class="est-section"><div class="est-section-title">Joins (' + d.joins.length + ')</div>';
            html += PA.estateViews._joinsHtml(d.joins, esc);
            html += '</div>';
        }

        // Indexes on the view itself (e.g. an MV's snapshot index).
        if (d.indexes && d.indexes.length) {
            html += '<div class="est-section"><div class="est-section-title">Indexes on this view (' + d.indexes.length + ')</div>';
            html += PA.estateViews._indexesHtml(d.indexes, esc);
            html += '</div>';
        }

        // Indexes on the view's base tables.
        if (d.baseTableIndexes && d.baseTableIndexes.length) {
            html += '<div class="est-section"><div class="est-section-title">Indexes on base tables (' + d.baseTableIndexes.length + ')</div>';
            html += PA.estateViews._baseTableIndexesHtml(d.baseTableIndexes, esc);
            html += '</div>';
        }

        // Constraints on the view's own columns.
        if (d.constraints && d.constraints.length) {
            html += '<div class="est-section"><div class="est-section-title">Constraints (' + d.constraints.length + ')</div>';
            html += PA.estateViews._constraintsHtml(d.constraints, esc);
            html += '</div>';
        }

        // Cross-estate: base tables in the chain that carry triggers.
        if (d.triggerBoundTables && d.triggerBoundTables.length) {
            html += '<div class="est-section"><div class="est-section-title">Triggers on base tables</div>';
            html += PA.estateViews._triggerBoundHtml(d.triggerBoundTables, esc);
            html += '</div>';
        }

        // Chain visualiser
        if (d.chain) {
            html += '<div class="est-section">' + PA.estateViews._chainHtml(d.chain, esc) + '</div>';
        }

        // Used by — procedures, other views and triggers.
        html += '<div class="est-section"><div class="est-section-title">Used By (' + (d.usedByCount || 0) + ')</div>';
        html += PA.estateViews._usedByHtml(d.usedBy, esc);
        html += '</div>';

        // Reverse estate edges: other views reading this view, and triggers whose body touches it.
        if (d.usedByViews && d.usedByViews.length) {
            html += '<div class="est-section"><div class="est-section-title">Used by other views (' + d.usedByViews.length + ')</div>';
            html += PA.estateViews._usedByViewsHtml(d.usedByViews, esc);
            html += '</div>';
        }
        if (d.usedByTriggers && d.usedByTriggers.length) {
            html += '<div class="est-section"><div class="est-section-title">Referenced by triggers (' + d.usedByTriggers.length + ')</div>';
            html += PA.estateViews._usedByTriggersHtml(d.usedByTriggers, esc);
            html += '</div>';
        }

        // Dynamic SQL statements naming this view.
        if (d.dynamicSqlRefs && d.dynamicSqlRefs.length) {
            html += '<div class="est-section"><div class="est-section-title">Dynamic SQL references (' + d.dynamicSqlRefs.length + ')</div>';
            html += PA.estateViews._dynamicSqlHtml(d.dynamicSqlRefs, esc);
            html += '</div>';
        }

        return html;
    },

    _baseTablesHtml: function(baseTables, esc) {
        if (!baseTables || !baseTables.length) return '<div class="est-empty">No base tables captured.</div>';
        var html = '<div class="est-chips">';
        for (var i = 0; i < baseTables.length; i++) {
            var b = baseTables[i];
            var op = (b.operation || 'SELECT').toUpperCase();
            html += '<span class="est-chip">'
                + '<span class="est-op-badge ' + op.toLowerCase() + '">' + esc(op) + '</span>'
                + '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.tableDetail.open(\''
                + PA.escJs(b.name || '') + '\',\'' + PA.escJs(b.schema || '') + '\')">' + esc(b.name || '') + '</span>'
                + (b.schema ? '<span class="est-chip-sub">' + esc(b.schema) + '</span>' : '')
                + '</span>';
        }
        html += '</div>';
        return html;
    },

    /** Joins inside the view definition: type, both tables, the ON condition, and the source line. */
    _joinsHtml: function(joins, esc) {
        var html = '<table class="est-cols"><thead><tr><th>Type</th><th>Left</th><th>Joined</th>'
            + '<th>Condition</th><th>Line</th></tr></thead><tbody>';
        for (var i = 0; i < joins.length; i++) {
            var j = joins[i];
            html += '<tr>'
                + '<td>' + esc(j.joinType || 'JOIN') + '</td>'
                + '<td class="est-col-name">' + esc((j.leftTableAlias ? j.leftTableAlias + '.' : '') + (j.leftTable || '')) + '</td>'
                + '<td class="est-col-name">' + esc((j.joinedTableAlias ? j.joinedTableAlias + '.' : '') + (j.joinedTable || '')) + '</td>'
                + '<td class="est-col-type">' + esc(j.condition || '') + '</td>'
                + '<td>' + (j.line ? 'L' + j.line : '') + '</td>'
                + '</tr>';
        }
        html += '</tbody></table>';
        return html;
    },

    /** One row per index: name, unique flag, and its columns. */
    _indexesHtml: function(indexes, esc) {
        var html = '<table class="est-cols"><thead><tr><th>Index</th><th>Unique</th><th>Columns</th></tr></thead><tbody>';
        for (var i = 0; i < indexes.length; i++) {
            var ix = indexes[i];
            var cols = (ix.columns || []).join(', ');
            html += '<tr>'
                + '<td class="est-col-name">' + esc(ix.name || '') + '</td>'
                + '<td>' + (ix.unique ? 'YES' : 'NO') + '</td>'
                + '<td class="est-col-type">' + esc(cols) + '</td>'
                + '</tr>';
        }
        html += '</tbody></table>';
        return html;
    },

    /** Indexes grouped under their base table, each base table a clickable table link. */
    _baseTableIndexesHtml: function(rows, esc) {
        var html = '<table class="est-cols"><thead><tr><th>Base Table</th><th>Index</th><th>Unique</th><th>Columns</th></tr></thead><tbody>';
        for (var i = 0; i < rows.length; i++) {
            var r = rows[i];
            html += '<tr>'
                + '<td><span class="est-usedby-proc" onclick="event.stopPropagation();PA.tableDetail.open(\''
                + PA.escJs(r.table || '') + '\',\'' + PA.escJs(r.tableSchema || '') + '\')">' + esc(r.table || '') + '</span></td>'
                + '<td class="est-col-name">' + esc(r.name || '') + '</td>'
                + '<td>' + (r.unique ? 'YES' : 'NO') + '</td>'
                + '<td class="est-col-type">' + esc((r.columns || []).join(', ')) + '</td>'
                + '</tr>';
        }
        html += '</tbody></table>';
        return html;
    },

    _constraintsHtml: function(constraints, esc) {
        var html = '<table class="est-cols"><thead><tr><th>Name</th><th>Type</th><th>Condition</th></tr></thead><tbody>';
        for (var i = 0; i < constraints.length; i++) {
            var c = constraints[i];
            html += '<tr>'
                + '<td class="est-col-name">' + esc(c.name || '') + '</td>'
                + '<td>' + esc(c.type || '') + '</td>'
                + '<td class="est-col-type">' + esc(c.condition || '') + '</td>'
                + '</tr>';
        }
        html += '</tbody></table>';
        return html;
    },

    /** Base tables that carry triggers — each trigger a clickable link into the triggers screen. */
    _triggerBoundHtml: function(tables, esc) {
        var html = '';
        for (var i = 0; i < tables.length; i++) {
            var t = tables[i];
            html += '<div class="est-var-group"><div class="est-var-name">'
                + '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.tableDetail.open(\''
                + PA.escJs(t.table || '') + '\',\'' + PA.escJs(t.schema || '') + '\')">' + esc(t.table || '') + '</span></div>';
            html += '<div class="est-chips">';
            var trs = t.triggers || [];
            for (var j = 0; j < trs.length; j++) {
                var tr = trs[j];
                html += '<span class="est-chip">'
                    + '<span class="est-op-badge ' + String(tr.event || '').toLowerCase().split(' ')[0] + '">' + esc(tr.timing || '') + ' ' + esc(tr.event || '') + '</span>'
                    + '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.estateTriggers.showTrigger(\''
                    + PA.escJs(tr.name || '') + '\')">' + esc(tr.name || '') + '</span>'
                    + (tr.status ? '<span class="est-chip-sub">' + esc(tr.status) + '</span>' : '')
                    + '</span>';
            }
            html += '</div></div>';
        }
        return html;
    },

    _usedByViewsHtml: function(views, esc) {
        var html = '<div class="est-usedby">';
        for (var i = 0; i < views.length; i++) {
            var v = views[i];
            html += '<div class="est-usedby-item">'
                + '<span class="est-op-badge ' + String(v.operation || 'SELECT').toLowerCase() + '">' + esc(v.operation || 'SELECT') + '</span>'
                + '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.estateViews.showView(\''
                + PA.escJs(v.name || '') + '\')">' + esc(v.name || '') + '</span>'
                + (v.schema ? '<span class="est-usedby-line">' + esc(v.schema) + '</span>' : '')
                + '</div>';
        }
        html += '</div>';
        return html;
    },

    _usedByTriggersHtml: function(triggers, esc) {
        var html = '<div class="est-usedby">';
        for (var i = 0; i < triggers.length; i++) {
            var t = triggers[i];
            html += '<div class="est-usedby-item">'
                + '<span class="est-op-badge ' + String(t.operation || 'SELECT').toLowerCase() + '">' + esc(t.operation || 'SELECT') + '</span>'
                + '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.estateTriggers.showTrigger(\''
                + PA.escJs(t.name || '') + '\')">' + esc(t.name || '') + '</span>'
                + '<span class="est-usedby-line">' + esc((t.timing || '') + ' ' + (t.event || '')) + '</span>'
                + '</div>';
        }
        html += '</div>';
        return html;
    },

    /** Dynamic SQL statements naming the view — each procedure a clickable link. */
    _dynamicSqlHtml: function(refs, esc) {
        var html = '<div class="est-usedby">';
        for (var i = 0; i < refs.length; i++) {
            var r = refs[i];
            html += '<div class="est-usedby-item">'
                + '<span class="est-op-badge select">' + esc(r.type || 'DYNAMIC') + '</span>'
                + '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.showProcedure(\''
                + PA.escJs(r.procedureId || '') + '\')">' + esc(r.procedureName || r.procedureId || '') + '</span>'
                + (r.lineNumber ? '<span class="est-usedby-line">L' + r.lineNumber + '</span>' : '')
                + '<span class="est-chip-sub" style="font-family:var(--font-mono)">' + esc(r.sqlExpression || '') + '</span>'
                + '</div>';
        }
        html += '</div>';
        return html;
    },

    _usedByHtml: function(usedBy, esc) {
        if (!usedBy || !usedBy.length) return '<div class="est-empty">Nothing in this analysis reads this view.</div>';
        var html = '<div class="est-usedby">';
        for (var i = 0; i < usedBy.length; i++) {
            var u = usedBy[i];
            html += '<div class="est-usedby-item">';
            if (u.nodeType) html += '<span class="est-badge">' + esc(u.nodeType) + '</span>';
            html += '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.showProcedure(\'' + PA.escJs(u.nodeId || '') + '\')">' + esc(u.nodeName || u.nodeId || '') + '</span>';
            html += PA.estateViews._opsHtml(u.operations, esc);
            if (u.nodeDepth != null) html += '<span class="est-usedby-line">depth ' + u.nodeDepth + '</span>';
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    /** `operations` is `{OP: [line, ...]}`; render one badge per op, with the first line. */
    _opsHtml: function(operations, esc) {
        if (!operations || typeof operations !== 'object') return '';
        var html = '<span class="est-usedby-ops">';
        for (var op in operations) {
            if (!Object.prototype.hasOwnProperty.call(operations, op)) continue;
            var lines = operations[op] || [];
            html += '<span class="est-op-badge ' + String(op).toLowerCase() + '">' + esc(op) + '</span>';
            if (lines.length) html += '<span class="est-usedby-line">L' + lines[0] + '</span>';
        }
        html += '</span>';
        return html;
    },

    /**
     * The chain visualiser: one row per level (level 0 is the view itself), an arrow between
     * levels labelled with the operations that connect them, terminal base tables styled as tables.
     * `levels` is `[[A],[B],[T]]` for A -> B -> base table T.
     */
    _chainHtml: function(chain, esc) {
        var levels = chain.levels || [];
        if (!levels.length) return '';
        var nodeBy = {};
        (chain.nodes || []).forEach(function(n) { nodeBy[(n.name || '').toUpperCase()] = n; });
        var baseSet = {};
        (chain.baseTables || []).forEach(function(b) { baseSet[(b || '').toUpperCase()] = true; });
        var edgesByFrom = {};
        (chain.edges || []).forEach(function(e) {
            var k = (e.from || '').toUpperCase();
            (edgesByFrom[k] = edgesByFrom[k] || []).push(e);
        });

        var html = '<div class="est-chain">';
        html += '<div class="est-chain-title">Dependency Chain</div>';
        html += '<div class="est-chain-meta">' + levels.length + ' level' + (levels.length === 1 ? '' : 's');
        if (chain.cyclic) html += '<span class="est-flag est-flag-warn">cycle detected</span>';
        if (chain.truncated) html += '<span class="est-flag est-flag-warn">truncated</span>';
        html += '</div>';

        for (var i = 0; i < levels.length; i++) {
            var names = levels[i] || [];
            if (i > 0) {
                var vias = {};
                var prev = levels[i - 1] || [];
                for (var p = 0; p < prev.length; p++) {
                    var es = edgesByFrom[(prev[p] || '').toUpperCase()] || [];
                    for (var e = 0; e < es.length; e++) { if (es[e].via) vias[es[e].via] = true; }
                }
                var viaTxt = Object.keys(vias).join(', ');
                html += '<div class="est-chain-arrow">&#8595;'
                    + (viaTxt ? '<span class="est-chain-via">' + esc(viaTxt) + '</span>' : '')
                    + '</div>';
            }
            html += '<div class="est-chain-step"><span class="est-step-idx">L' + i + '</span><div class="est-step-nodes">';
            if (!names.length) {
                html += '<span class="est-node est-node-empty">(none)</span>';
            } else {
                for (var j = 0; j < names.length; j++) {
                    var nm = names[j] || '';
                    var nd = nodeBy[nm.toUpperCase()];
                    var cls, sub, onclick;
                    if (nd) {
                        var kind = (nd.kind || nd.objectType || 'VIEW').toUpperCase();
                        cls = kind === 'MATERIALIZED_VIEW' ? 'est-node-mv' : (kind === 'VIEW' ? 'est-node-view' : 'est-node-table');
                        sub = nd.schema || '';
                        onclick = (kind === 'VIEW' || kind === 'MATERIALIZED_VIEW')
                            ? ' onclick="event.stopPropagation();PA.estateViews.showView(\'' + PA.escJs(nm) + '\')"'
                            : ' onclick="event.stopPropagation();PA.tableDetail.open(\'' + PA.escJs(nm) + '\',\'' + PA.escJs(nd.schema || '') + '\')"';
                    } else if (baseSet[nm.toUpperCase()]) {
                        cls = 'est-node-table';
                        sub = 'base table';
                        onclick = ' onclick="event.stopPropagation();PA.tableDetail.open(\'' + PA.escJs(nm) + '\',\'\')"';
                    } else {
                        cls = 'est-node-table';
                        sub = '';
                        onclick = '';
                    }
                    html += '<span class="est-node ' + cls + '"' + onclick + '>' + esc(nm)
                        + (sub ? '<span class="est-node-sub">' + esc(sub) + '</span>' : '')
                        + '</span>';
                }
            }
            html += '</div></div>';
        }
        html += '</div>';
        return html;
    }
};
