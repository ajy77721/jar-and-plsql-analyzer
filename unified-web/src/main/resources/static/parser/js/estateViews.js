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
     * Cross-estate link target: switch to the Details → Views & MVs sub-tab, filter to this view and
     * open its detail row. Used by the chain/base-table/used-by links on the other estate screens.
     */
    showView: function(name) {
        if (!name) return;
        if (PA.switchRightTab) PA.switchRightTab('details');
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
        // Task 52 — the "def" link (same affordance the Tables tab's view rows carry): the user
        // reading the LIST wants the definition SQL on one click, not a row expand + scroll.
        html += '<td><span style="font-weight:600;color:var(--badge-blue)">' + esc(v.name || '') + '</span>'
            + ' <span class="ut-link" style="font-size:9px" onclick="event.stopPropagation();PA.estateViews.openDef(\'' + PA.escJs(v.name || '') + '\')">def</span></td>';
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
        return '<div class="est-detail">' + PA.estateViews._detailHtml(cached, esc, idx) + '</div>';
    },

    /**
     * Task 52 — the row's "def" link: the view's definition SQL, one click from the list.
     * Reuses the SAME per-view detail cache the expand panel fills (`_details`), so a view already
     * opened costs nothing and the modal can never disagree with the panel. The modal also lists
     * the view's columns as chips — clicking a chip highlights that column's occurrences in the
     * definition SQL above it, which is the "see where this field comes from" ask without adding
     * any server surface.
     */
    /** `line`, when given, is a line in the view's own definition SQL — the modal opens with
     * that line marked and scrolled into view (the click target of a Joins row's "L<n>"). */
    openDef: function(name, line) {
        var cached = PA.estateViews._details[name];
        if (cached !== undefined) { PA.estateViews._showDefModal(name, cached, line); return; }
        PA.api.getViewDetail(name).then(function(d) {
            var detail = (d && !(PA.failed && PA.failed(d))) ? d : null;
            PA.estateViews._details[name] = detail;
            PA.estateViews._showDefModal(name, detail, line);
        }).catch(function() {
            PA.estateViews._showDefModal(name, null, line);
        });
    },

    _showDefModal: function(name, d, line) {
        var isMv = d && (d.objectType === 'MATERIALIZED_VIEW' || d.isMaterialized);
        var title = (isMv ? 'Materialized View' : 'View') + ' — ' + name;
        if (!d || !d.definition) {
            PA.definitionModal.open({
                title: title,
                sections: [{ html: '<div class="est-empty">No definition captured for this view.'
                    + (d ? '' : ' This view is listed, but its detail artifact is not present in this analysis — re-run the analysis to populate it.')
                    + '</div>' }]
            });
            return;
        }
        var chips = '';
        if (d.columns && d.columns.length) {
            chips = '<div class="est-chips">';
            for (var i = 0; i < d.columns.length; i++) {
                var cn = d.columns[i].name || '';
                if (!cn) continue;
                chips += '<span class="est-chip"><span class="est-col-name ut-link" '
                    + 'onclick="PA.definitionModal.highlight(\'defmodal-pre\',\'' + PA.escJs(cn) + '\')">' + PA.esc(cn) + '</span></span>';
            }
            chips += '</div>';
        }
        PA.definitionModal.open({
            title: title,
            sections: [
                { title: 'Definition', pre: d.definition, preId: 'defmodal-pre' },
                { title: 'Columns — click one to find it in the definition',
                  html: chips || '<div class="est-empty">No columns captured.</div>' }
            ]
        });
        if (line) PA.definitionModal.highlightLine('defmodal-pre', line);
    },

    /** The detail panel's own column name → the same highlight, scoped to the panel's definition. */
    _highlightCol: function(idx, name) {
        PA.definitionModal.highlight('vw-def-' + idx, name);
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

    _detailHtml: function(d, esc, idx) {
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

        // Definition — DB-sourced SQL is DATA: escaped, inside <pre>. The id lets the Columns
        // table's own name cells (below) highlight their occurrences in it.
        html += '<div class="est-section"><div class="est-section-title">Definition</div>';
        html += d.definition
            ? '<pre class="est-sql" id="vw-def-' + idx + '">' + esc(d.definition) + '</pre>'
            : '<div class="est-empty">No definition captured for this view.</div>';
        html += '</div>';

        // Columns — a column NAME is clickable: it highlights where that column appears in the
        // view's definition SQL above (task 52 — "see the field in the definition" without
        // eyeballing the SQL). No per-column server surface exists or is needed.
        html += '<div class="est-section"><div class="est-section-title">Columns (' + (d.columnCount || 0) + ')</div>';
        if (d.columns && d.columns.length) {
            html += '<table class="est-cols"><thead><tr><th>Name</th><th>Data Type</th><th>Nullable</th></tr></thead><tbody>';
            for (var i = 0; i < d.columns.length; i++) {
                var col = d.columns[i];
                html += '<tr><td class="est-col-name ut-link" onclick="PA.estateViews._highlightCol(' + idx + ',\'' + PA.escJs(col.name || '') + '\')">' + esc(col.name || '') + '</td>'
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
            html += PA.estateViews._joinsHtml(d.joins, esc, idx, d.name);
            html += '</div>';
        }

        // Indexes on the view itself (e.g. an MV's snapshot index).
        if (d.indexes && d.indexes.length) {
            html += '<div class="est-section"><div class="est-section-title">Indexes on this view (' + d.indexes.length + ')</div>';
            html += PA.estateViews._indexesHtml(d.indexes, esc, idx);
            html += '</div>';
        }

        // Indexes on the view's base tables.
        if (d.baseTableIndexes && d.baseTableIndexes.length) {
            html += '<div class="est-section"><div class="est-section-title">Indexes on base tables (' + d.baseTableIndexes.length + ')</div>';
            html += PA.estateViews._baseTableIndexesHtml(d.baseTableIndexes, esc, idx);
            html += '</div>';
        }

        // Constraints on the view's own columns.
        if (d.constraints && d.constraints.length) {
            html += '<div class="est-section"><div class="est-section-title">Constraints (' + d.constraints.length + ')</div>';
            html += PA.estateViews._constraintsHtml(d.constraints, esc, idx);
            html += '</div>';
        }

        // Cross-estate: base tables in the chain that carry triggers.
        if (d.triggerBoundTables && d.triggerBoundTables.length) {
            html += '<div class="est-section"><div class="est-section-title">Triggers on base tables</div>';
            html += PA.estateViews._triggerBoundHtml(d.triggerBoundTables, esc, idx);
            html += '</div>';
        }

        // Chain visualiser
        if (d.chain) {
            html += '<div class="est-section">' + PA.estateViews._chainHtml(d.chain, esc) + '</div>';
        }

        // Used by — procedures, other views and triggers.
        html += '<div class="est-section"><div class="est-section-title">Used By (' + (d.usedByCount || 0) + ')</div>';
        html += PA.estateViews._usedByHtml(d.usedBy, esc, idx);
        html += '</div>';

        // Reverse estate edges: other views reading this view, and triggers whose body touches it.
        if (d.usedByViews && d.usedByViews.length) {
            html += '<div class="est-section"><div class="est-section-title">Used by other views (' + d.usedByViews.length + ')</div>';
            html += PA.estateViews._usedByViewsHtml(d.usedByViews, esc, idx);
            html += '</div>';
        }
        if (d.usedByTriggers && d.usedByTriggers.length) {
            html += '<div class="est-section"><div class="est-section-title">Referenced by triggers (' + d.usedByTriggers.length + ')</div>';
            html += PA.estateViews._usedByTriggersHtml(d.usedByTriggers, esc, idx);
            html += '</div>';
        }

        // Dynamic SQL statements naming this view.
        if (d.dynamicSqlRefs && d.dynamicSqlRefs.length) {
            html += '<div class="est-section"><div class="est-section-title">Dynamic SQL references (' + d.dynamicSqlRefs.length + ')</div>';
            html += PA.estateViews._dynamicSqlHtml(d.dynamicSqlRefs, esc, idx);
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
    _joinsHtml: function(joins, esc, idx, viewName) {
        return PA.usageTable.attach('ut-vw-joins-' + idx, joins || [], {
            showTitle: false, nameLabel: 'Join', snippetLabel: 'Condition', countLabel: 'joins',
            nameClass: 'est-col-name',
            name: function (j) {
                var l = (j.leftTableAlias ? j.leftTableAlias + '.' : '') + (j.leftTable || '');
                var r = (j.joinedTableAlias ? j.joinedTableAlias + '.' : '') + (j.joinedTable || '');
                return l + ' → ' + r;
            },
            line: function (j) { return j.line; },
            // A join's line is a line IN THE VIEW'S OWN definition SQL — clicking opens the
            // definition modal with that line marked (task 52).
            lineLink: function (j) {
                return (j.line != null && viewName)
                    ? "PA.estateViews.openDef('" + PA.escJs(viewName) + "', " + j.line + ")"
                    : null;
            },
            snippet: function (j) { return j.condition; },
            badges: function (j) { return [j.joinType || 'JOIN']; }
        });
    },

    /** One row per index: name, unique flag, and its columns. */
    _indexesHtml: function(indexes, esc, idx) {
        return PA.usageTable.attach('ut-vw-indexes-' + idx, indexes || [], {
            showTitle: false, nameLabel: 'Index', snippetLabel: 'Columns', countLabel: 'indexes',
            nameClass: 'est-col-name',
            name: function (ix) { return ix.name; },
            snippet: function (ix) { return (ix.columns || []).join(', '); },
            badges: function (ix) { return [ix.unique ? 'UNIQUE' : 'INDEX']; }
        });
    },

    /** Indexes grouped under their base table, each base table a clickable table link. */
    _baseTableIndexesHtml: function(rows, esc, idx) {
        return PA.usageTable.attach('ut-vw-btindexes-' + idx, rows || [], {
            showTitle: false, nameLabel: 'Index', snippetLabel: 'Columns', countLabel: 'indexes',
            nameClass: 'est-col-name',
            name: function (r) { return r.name; },
            sub: function (r) {
                return { text: r.table || '', link: "PA.tableDetail.open('" + PA.escJs(r.table || '') + "','" + PA.escJs(r.tableSchema || '') + "')" };
            },
            snippet: function (r) { return (r.columns || []).join(', '); },
            badges: function (r) { return [r.unique ? 'UNIQUE' : 'INDEX']; },
            search: function (r) { return r.table || ''; }
        });
    },

    _constraintsHtml: function(constraints, esc, idx) {
        return PA.usageTable.attach('ut-vw-constraints-' + idx, constraints || [], {
            showTitle: false, nameLabel: 'Constraint', snippetLabel: 'Condition', countLabel: 'constraints',
            nameClass: 'est-col-name',
            name: function (c) { return c.name; },
            snippet: function (c) { return c.condition; },
            badges: function (c) { return [c.type || 'CONSTRAINT']; }
        });
    },

    /** Base tables that carry triggers — each trigger a clickable link into the triggers screen. */
    _triggerBoundHtml: function(tables, esc, idx) {
        var rows = [];
        for (var i = 0; i < (tables || []).length; i++) {
            var t = tables[i];
            var trs = t.triggers || [];
            for (var j = 0; j < trs.length; j++) {
                rows.push({ table: t.table, schema: t.schema, trigger: trs[j] });
            }
        }
        return PA.usageTable.attach('ut-vw-triggerbound-' + idx, rows, {
            showTitle: false, nameLabel: 'Trigger', countLabel: 'triggers', showSnippet: false,
            name: function (r) { return r.trigger.name; },
            nameLink: function (r) { return "PA.estateTriggers.showTrigger('" + PA.escJs(r.trigger.name || '') + "')"; },
            sub: function (r) {
                return { text: (r.schema ? r.schema + '.' : '') + (r.table || ''),
                         link: "PA.tableDetail.open('" + PA.escJs(r.table || '') + "','" + PA.escJs(r.schema || '') + "')" };
            },
            badges: function (r) { return [r.trigger.timing, r.trigger.event, r.trigger.status]; },
            search: function (r) { return r.table || ''; }
        });
    },

    _usedByViewsHtml: function(views, esc, idx) {
        return PA.usageTable.attach('ut-vw-usedbyviews-' + idx, views || [], {
            showTitle: false, nameLabel: 'View', countLabel: 'views', showSnippet: false,
            name: function (v) { return v.name; },
            nameLink: function (v) { return "PA.estateViews.showView('" + PA.escJs(v.name || '') + "')"; },
            sub: function (v) { return { text: v.schema || '' }; },
            badges: function (v) { return [v.operation || 'SELECT']; }
        });
    },

    _usedByTriggersHtml: function(triggers, esc, idx) {
        return PA.usageTable.attach('ut-vw-usedbytriggers-' + idx, triggers || [], {
            showTitle: false, nameLabel: 'Trigger', countLabel: 'triggers', showSnippet: false,
            name: function (t) { return t.name; },
            nameLink: function (t) { return "PA.estateTriggers.showTrigger('" + PA.escJs(t.name || '') + "')"; },
            sub: function (t) { return { text: t.schema || '' }; },
            badges: function (t) { return [t.operation || 'SELECT', t.timing, t.event]; }
        });
    },

    /** Dynamic SQL statements naming the view — each procedure a clickable link. */
    _dynamicSqlHtml: function(refs, esc, idx) {
        return PA.usageTable.attach('ut-vw-dynsql-' + idx, refs || [], {
            showTitle: false, nameLabel: 'Procedure', countLabel: 'references',
            name: function (r) { return r.procedureName || r.procedureId; },
            nameLink: function (r) { return "PA.showProcedure('" + PA.escJs(r.procedureId || '') + "')"; },
            line: function (r) { return r.lineNumber; },
            // A reference's line is a line in the procedure's source — click opens it there (task 52).
            lineLink: function (r) {
                return (r.procedureId && r.lineNumber != null)
                    ? "PA.sourceView.openNodeAtLine('" + PA.escJs(r.procedureId) + "', " + r.lineNumber + ")"
                    : null;
            },
            snippet: function (r) { return r.sqlExpression; },
            badges: function (r) { return [r.type || 'DYNAMIC']; },
            source: PA.usageTable.nodeSource
        });
    },

    _usedByHtml: function(usedBy, esc, idx) {
        if (!usedBy || !usedBy.length) return '<div class="est-empty">Nothing in this analysis reads this view.</div>';
        return PA.usageTable.attach('ut-vw-usedby-' + idx, usedBy, {
            showTitle: false, nameLabel: 'Procedure', countLabel: 'usages',
            name: function (u) { return u.nodeName || u.nodeId; },
            nameLink: function (u) { return "PA.showProcedure('" + PA.escJs(u.nodeId || '') + "')"; },
            sub: function (u) { return { text: u.nodeType || '' }; },
            badges: function (u) { return Object.keys(u.operations || {}); },
            line: function (u) { return PA.usageTable.firstLine(u); },
            // The usage line is a line in the PROCEDURE'S SOURCE — clicking opens that source at
            // the line (task 52). Without a node id there is nothing to open, so no link.
            lineLink: function (u) {
                var ln = PA.usageTable.firstLine(u);
                return (u.nodeId && ln != null)
                    ? "PA.sourceView.openNodeAtLine('" + PA.escJs(u.nodeId) + "', " + ln + ")"
                    : null;
            },
            source: PA.usageTable.nodeSource,
            full: function (u) {
                var ops = u.operations || {};
                var parts = Object.keys(ops).map(function (op) {
                    return op + ' L' + (ops[op] || []).join(', L');
                });
                return '<pre>' + esc(parts.join('\n')) + '</pre>';
            }
        });
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
