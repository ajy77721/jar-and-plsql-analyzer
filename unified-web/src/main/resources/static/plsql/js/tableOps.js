/**
 * PA.tableOps — Table Operations with operation pill filters, pagination, expandable details.
 * Uses PA.tf (shared table framework) for pagination, sorting, column filters.
 *
 * UI-028 — server-paged for the plsql live-crawl backend (`PA.api.tablesPageSource`). The scope
 * logic here is not the parser app's own Direct/Subtree/SubFlow (`ParserScopeResolver`) — it is
 * this screen's own `all`/`scoped` toggle over `ctx.scopedTables`/`ctx.callTreeNodeIds`, with fuzzy
 * id matching. That logic is ported server-side in `PlsqlScopeFilter`, applied before paging via
 * the `directProcId`/`nodeIds` request parameters — see its own javadoc for exactly which
 * fallbacks it reproduces and why.
 *
 * MID-021 (§110): the parser-backed case is converted too now — `tablesPageSource` itself
 * dispatches by `_parserName()`, hitting `ParserAnalysisController.getTablesPage` (which gained
 * `directProcId`/`nodeIds` via `PlsqlScopeFilter.directTables`/`subtreeTables`, the `usedBy[]`-
 * shaped sibling of `direct`/`subtree`), and mapping each page through `PA.api._adaptParserTableRow`
 * so this file's own rendering code never has to know which backend produced a row. `load()` no
 * longer branches at all; `_src` is always set. See `plsql-tableops-parser-shape-bug.test.mjs` for
 * the shape-adapter tests and §109's own doc comment for the bug this conversion's investigation
 * found along the way.
 */
window.PA = window.PA || {};

PA.tableOps = {
    data: [],
    _src: null,
    activeOps: new Set(),  // active operation filters (empty = all)
    scopeMode: 'all',      // 'all' | 'scoped'
    _detailSeq: 0,         // per-expand panel id token — see _renderDetail
    _facetPromise: null,   // BUG-FIX (see _initColFiltersAsync): whole-table-list fetch used only
                            // to compute column-filter facet values, reset each load()

    async load() {
        PA.tableOps.activeOps.clear();
        PA.tableOps.scopeMode = 'all';
        // MID-021 (§110): both backends now go through the same paged source — tablesPageSource
        // itself dispatches by _parserName(). The old parser-backend branch fetched the whole
        // unpaged array here (and, until §109, handed the renderer the wrong shape entirely).
        PA.tableOps.data = [];
        PA.tableOps._facetPromise = null;
        PA.tableOps._src = PA.api.tablesPageSource({});
        PA.tableOps._initOpPills();
        PA.tableOps._initTable();
        PA.tableOps._updateScopeToggle();
    },

    /**
     * The column-filter `cf` key `PA.tf`'s `_cfPush`/`applyFilters` send is literally this file's
     * own `<th data-sort-col>` number (`tableFramework.js` has no separate "display index" vs
     * "wire index" — see its own `initColFilters`/`_cfPush`), and the two backends `_src` can point
     * at declare *different* numbers for the same field:
     *   - `PlsqlFacetSpecs.TABLES` (live-crawl):  "0"=schemaName, "1"=tableType
     *   - `ParserFacetSpecs.TABLES` (parser):     "0"=schema,     "1"=name,  "2"=objectType
     * Schema happens to be "0" on both; Type is "1" on one backend and "2" on the other. This file
     * used a fixed 0/1/2 (Table/Type/Schema) regardless of backend — on a parser-backed analysis
     * that sent Type filters to column "1", which the parser backend reads as the *table name*
     * (`FacetSpec.scalarColumn("name", ...)`), so the popover looked right but every selection
     * silently matched on the wrong field. Resolved by picking the Type column's wire number per
     * backend, at table-build time, rather than assuming the two ever agreed.
     */
    _typeColIdx() {
        return (PA.api._parserName && PA.api._parserName()) ? 2 : 1;
    },

    /** Initialize the table framework with data */
    _initTable() {
        const container = document.getElementById('toContainer');
        if (!container) return;
        const typeCol = PA.tableOps._typeColIdx();

        // Build table shell with sortable headers. Column numbers are chosen so Schema (0) and
        // Type (backend-dependent, see _typeColIdx) match what each backend's own filterColumns()
        // map expects; Count/Triggers/Scope never send a `cf` value so their numbers just need to
        // stay clear of 0 and typeCol within this one render. Table is "9" — a free-text filter
        // (contains/regex) on the table name, deliberately NOT "3": that number means "Operations"
        // in `ParserFacetSpecs.TABLES.filterColumns` (actively used by `parser/js/tableOps.js`'s own
        // Operation checkbox against the very same backend spec instance), so reusing it here for a
        // different field would either collide at spec-construction time (if declared as text) or
        // silently filter on the wrong column (if a stray cf=3 were ever sent). "9" is unused by
        // either backend's TABLES spec.
        let html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="9" onclick="PA.tf.sort(\'to\',9)">Table</th>';
        html += `<th data-sort-col="${typeCol}" onclick="PA.tf.sort('to',${typeCol})">Type</th>`;
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'to\',0)">Schema</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'to\',4)">Count</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'to\',5)">Triggers</th>';
        html += '<th data-sort-col="6" onclick="PA.tf.sort(\'to\',6)">Scope</th>';
        html += '</tr></thead><tbody id="to-tbody"></tbody></table>';
        container.innerHTML = html;

        // Initialize framework. sortKeys/searchFn/extraFilter apply only on the array path (dead
        // in practice since MID-021/§110 — both backends always set _src now — kept as PA.tf's own
        // documented fallback shape, same as joins.js/cursors.js); serverSorts applies on the src
        // path — PA.tf.sort() branches on which is present, so both can be declared together.
        PA.tf.init('to', PA.tableOps._src || PA.tableOps.data, 50, PA.tableOps._renderRow, {
            sortKeys: {
                9: { fn: t => (t.tableName || '').toUpperCase() },
                [typeCol]: { fn: t => (t.tableType || 'TABLE').toUpperCase() },
                0: { fn: t => (t.schemaName || '').toUpperCase() },
                4: { fn: t => t.accessCount || (t.accessDetails || []).length },
                5: { fn: t => (t.triggers || []).filter(tr => tr.status === 'ENABLED').length },
                6: { fn: t => t.external ? 1 : 0 }
            },
            serverSorts: {
                9: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                [typeCol]: { asc: 'TYPE_ASC', desc: 'TYPE_DESC' },
                0: { asc: 'SCHEMA_ASC', desc: 'SCHEMA_DESC' },
                4: { asc: 'USAGES_ASC', desc: 'USAGES_DESC' }
                // 5 (Triggers) and 6 (Scope/external) have no facet sort key, same as the
                // Type/Handlers columns UI-029 (§46) left unsortable for the same reason.
            },
            renderDetail: PA.tableOps._renderDetail,
            searchFn: (t, q) => (t.tableName || '').toUpperCase().includes(q) ||
                                (t.schemaName || '').toUpperCase().includes(q),
            extraFilter: PA.tableOps._opFilter,
            onFilter: PA.tableOps._updateCounts
        });

        // Default sort: by count descending
        const s = PA.tf.state('to');
        if (s) { s.sortCol = 4; s.sortDir = 'desc'; }
        if (PA.tableOps._src) PA.tableOps._src.setParam('sort', 'USAGES_DESC');

        PA.tf.filter('to');

        // Init column filters after table is in DOM.
        //
        // BUG-FIX (root cause of the "filters missing until some other click" report): this tab is
        // always server-paged now (§110, MID-021 — `_src` is never null), and `PA.tf.initColFilters`
        // silently no-ops on a server-paged table unless it is handed a `serverValues` map (the
        // popover's value list can't be collected from just the one page in memory). The old call
        // here never passed one, so the filter icons never attached at all — on the very first load
        // *or* after any later re-render; there was no async data dependency racing anything, the
        // call was simply always a dead no-op (confirmed live: `[PA.tf] column filters need
        // serverValues on server-paged table to` logs on every load, and 0 `.cf-icon` elements exist
        // in the DOM before or after further clicks). Fixed the same way the parser-side equivalent
        // (`parser/js/tableOps.js`, `PA.api.facetSummary`) already does it for its own grid: fetch
        // the values once, then build the filter bar from real data instead of skipping it.
        // Placeholder icons appear the instant the table itself is in the DOM — before that fetch
        // even starts — so the header never shows silence while `_initColFiltersAsync` is in flight.
        PA.tf.pendingColFilters('to', {
            0: { label: 'Schema' }, [typeCol]: { label: 'Type' }, 9: { label: 'Table' }
        });
        setTimeout(() => { PA.tableOps._initColFiltersAsync(); }, 0);
    },

    /**
     * Builds the `serverValues` map `PA.tf.initColFilters` requires on a server-paged table, then
     * actually attaches the column filter icons.
     *
     * There is no facet-summary endpoint on the plsql live-crawl/parser backends (unlike the
     * parser app's own `tables/page` facet sibling) — but `PA.api.getTableOperations()` already
     * exists and is already used elsewhere (`export.js`, `app.js`) to read the whole per-table list
     * once; it's bounded by the number of distinct tables in the analysis (tens, not the thousands
     * of access lines the paged grid itself was moved off of), and returns rows in the exact same
     * `tableName`/`schemaName`/`tableType`/`external` shape this file's own `valueFn`s already read.
     * Reused here rather than adding new backend surface just for a filter popover's value list.
     */
    async _initColFiltersAsync() {
        const typeCol = PA.tableOps._typeColIdx();
        // Scope (external/internal) is deliberately not offered here: `PlsqlFacetSpecs.TABLES` has
        // a working "3"=external cf column, but `ParserFacetSpecs.TABLES` has no such field at all
        // — sending `cf=6:...` (or any number) for it against a parser-backed analysis is rejected
        // with a 400 ("Column '6' is not filterable") and would take the whole grid down, not just
        // leave the filter inert. Rather than ship a filter that works on one backend and 400s on
        // the other, Scope stays without a popover until both backends expose the same field — the
        // existing "External" checkbox (`_opFilter`/`applyFilters`) already covers the plsql-backend
        // case for anyone who needs it.
        const columns = {
            0: { label: 'Schema', valueFn: t => t.schemaName || '-' },
            [typeCol]: { label: 'Type', valueFn: t => (t.tableType || 'TABLE').toUpperCase() },
            // "9" (Table) is text-mode (contains/regex) — a free-text identifier, not an enumerable
            // set, and unlike the two above it needs no serverValues at all, so it works even if
            // the fetch below fails.
            9: { label: 'Table', valueFn: t => t.tableName || '', mode: 'text' }
        };
        let serverValues;
        try {
            if (!PA.tableOps._facetPromise) PA.tableOps._facetPromise = PA.api.getTableOperations();
            const all = await PA.tableOps._facetPromise;
            const schemaVals = new Set(), typeVals = new Set();
            for (const t of (all || [])) {
                schemaVals.add(columns[0].valueFn(t));
                typeVals.add(columns[typeCol].valueFn(t));
            }
            serverValues = {
                '0': [...schemaVals].sort(),
                [String(typeCol)]: [...typeVals].sort()
            };
        } catch (e) {
            // Whole-list fetch failed — fall back to the closed Type enum that is always correct
            // regardless of dataset (TEMP included: task 52 — the server's Type column reads the
            // row's effective kind, so the fallback must know the same set); Schema genuinely
            // needs the fetch, so it's simply omitted here rather than guessed at.
            serverValues = { [String(typeCol)]: ['TABLE', 'VIEW', 'MATERIALIZED VIEW', 'MATERIALIZED_VIEW', 'TEMP'] };
        }
        // A tab switch away and back calls load() -> _initTable() again, tearing down and rebuilding
        // #toContainer's whole <table> each time, so there's no stale-state to guard against here —
        // unlike _renderDetail's async fetch, this can't land after the row it belongs to is gone.
        PA.tf.initColFilters('to', columns, serverValues);
        PA.tf._updateSortIndicators('to');
    },

    /** Combined filter predicate: operation pills + type dropdown + ext/trig checkboxes */
    _opFilter(t) {
        const ops = PA.tableOps.activeOps;
        if (ops.size > 0 && !(t.operations || []).some(op => ops.has(op))) return false;
        const typeFilter = document.getElementById('toTypeFilter')?.value || '';
        if (typeFilter && (t.tableType || 'TABLE').toUpperCase() !== typeFilter) return false;
        if (document.getElementById('toExtOnly')?.checked && !t.external) return false;
        if (document.getElementById('toTrigOnly')?.checked && (t.triggers || []).length === 0) return false;
        return true;
    },

    /** Initialize operation filter pills */
    _initOpPills() {
        const container = document.getElementById('toOpPills');
        if (!container) return;
        const ops = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'];
        let html = '';
        for (const op of ops) {
            html += `<span class="op-filter-pill ${op} active" data-op="${op}" onclick="PA.tableOps.toggleOp('${op}')" title="Toggle ${op} filter">${op}</span>`;
        }
        container.innerHTML = html;
    },

    toggleOp(op) {
        const pill = document.querySelector(`#toOpPills .op-filter-pill[data-op="${op}"]`);
        if (!pill) return;
        if (PA.tableOps.activeOps.size === 0) {
            PA.tableOps.activeOps.add(op);
            document.querySelectorAll('#toOpPills .op-filter-pill').forEach(p => {
                p.classList.toggle('active', p.dataset.op === op);
            });
        } else if (PA.tableOps.activeOps.has(op)) {
            PA.tableOps.activeOps.delete(op);
            pill.classList.remove('active');
            if (PA.tableOps.activeOps.size === 0) {
                document.querySelectorAll('#toOpPills .op-filter-pill').forEach(p => p.classList.add('active'));
            }
        } else {
            PA.tableOps.activeOps.add(op);
            pill.classList.add('active');
        }
        PA.tableOps.applyFilters();
    },

    /**
     * Sends the pill/type/checkbox selection as `types`/`cf` and reloads page 1 — the src path,
     * always taken now (§110). Same reasoning as `parser/js/tableOps.js`'s `_pushOps`: with one page in memory a client-side
     * predicate would filter the fifty rows on screen while the header still counts every table.
     */
    applyFilters() {
        const src = PA.tableOps._src;
        if (!src) { PA.tf.filter('to'); return; }
        const ops = PA.tableOps.activeOps;
        src.setParam('types', ops.size ? [...ops].join(',') : '');
        const cf = [];
        const typeFilter = document.getElementById('toTypeFilter')?.value || '';
        // Same backend-dependent wire number as the column-filter icon — see `_typeColIdx`'s own
        // doc comment. This dropdown pushed a hardcoded "1" before, which is `name` (not `type`) on
        // a parser-backed analysis, so picking "Table"/"View" here silently matched zero rows.
        if (typeFilter) cf.push(PA.tableOps._typeColIdx() + ':' + typeFilter);
        // NOTE: "External"/"Triggers" below only have a matching cf column on the plsql live-crawl
        // backend (`PlsqlFacetSpecs.TABLES`'s "3"/"4"); the parser backend's own facet spec has no
        // such field at all, so checking either box against a parser-backed analysis 400s the whole
        // grid ("Column '4' is not filterable"). Pre-existing gap, not introduced by this fix, and
        // not one a client-side index correction can paper over — there is no field to point at.
        if (document.getElementById('toExtOnly')?.checked) cf.push('3:Y');
        if (document.getElementById('toTrigOnly')?.checked) cf.push('4:Y');
        src.setParam('cf', cf);
        PA.tf._restart('to');
    },

    clearFilters() {
        document.getElementById('toSearch').value = '';
        document.getElementById('toTypeFilter').value = '';
        const extBox = document.getElementById('toExtOnly'); if (extBox) extBox.checked = false;
        const trigBox = document.getElementById('toTrigOnly'); if (trigBox) trigBox.checked = false;
        PA.tableOps.activeOps.clear();
        document.querySelectorAll('#toOpPills .op-filter-pill').forEach(p => p.classList.add('active'));
        PA.tf.setSearch('to', '');
        const s = PA.tf.state('to');
        if (s) { s.colFilters = {}; }
        const src = PA.tableOps._src;
        if (src) { src.setParam('q', ''); src.setParam('types', ''); src.setParam('cf', []); }
        PA.tableOps.applyFilters();
        PA.tf._cfUpdateIcons('to');
    },

    /** Called from search input */
    onSearch() {
        PA.tf.setSearch('to', document.getElementById('toSearch')?.value || '');
        PA.tf.filter('to');
    },

    _updateCounts() {
        const s = PA.tf.state('to');
        const totalEl = document.getElementById('toTotalCount');
        const fc = document.getElementById('toFilteredCount');
        if (PA.tableOps._src) {
            // No whole-array count to compare against by design; the server's own total (once a
            // page has loaded) is the only honest number here — "N tables", scoped or not, rather
            // than a "shown of total" pair the array path could compute for free.
            const total = (s && s.totalIsExact) ? s.serverTotal : (s ? s.filtered.length : 0);
            totalEl.textContent = total + ' tables' + (PA.tableOps.scopeMode === 'scoped' ? ' (scoped)' : '');
            fc.style.display = 'none';
            return;
        }
        const allTotal = (PA.tableOps.data || []).length;
        const dataTotal = s ? s.data.length : allTotal;
        const shown = s ? s.filtered.length : allTotal;
        if (PA.tableOps.scopeMode === 'scoped' && PA.context.procId) {
            totalEl.textContent = dataTotal + '/' + allTotal + ' tables (scoped)';
        } else {
            totalEl.textContent = allTotal + ' tables';
        }
        if (shown < dataTotal) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else { fc.style.display = 'none'; }
    },

    /** Render a single table row */
    _renderRow(t, idx, esc) {
        const tid = PA.escAttr(t.tableName || '');
        const ops = (t.operations || []).map(op => `<span class="op-badge ${op}">${op}</span>`).join('');
        const count = t.accessCount || (t.accessDetails || []).length;
        // UI-002 (dedicated project, §133+) — accessDetails no longer travels with the grid row, so
        // this reads the scalar AnalyzerController.stripAccessDetails now computes before removing
        // the array, rather than scanning an array that is no longer there.
        const claudeCount = t.claudeVerifiedCount || (t.accessDetails || []).filter(d => d.sourceFile === 'claude-verified').length;
        const triggers = t.triggers || [];
        const enabledTriggers = triggers.filter(tr => tr.status === 'ENABLED');
        const scope = t.external
            ? '<span class="scope-badge ext">EXTERNAL</span>'
            : '<span class="scope-badge int">INTERNAL</span>';
        const trigLabel = enabledTriggers.length > 0
            ? `<span class="trig-badge">${enabledTriggers.length} trigger${enabledTriggers.length > 1 ? 's' : ''}</span>`
            : '<span style="color:var(--text-muted);font-size:10px">-</span>';

        const tt = (t.tableType || 'TABLE').toUpperCase();
        const ttColor = tt === 'VIEW' ? 'var(--orange)' : tt === 'MATERIALIZED VIEW' ? 'var(--purple)' : 'var(--text-muted)';
        const ttLabel = tt === 'MATERIALIZED VIEW' ? 'MV' : tt;
        let ttBadge = `<span style="font-size:9px;font-weight:700;color:${ttColor}">${ttLabel}</span>`;
        if (tt === 'VIEW' || tt === 'MATERIALIZED VIEW') {
            ttBadge += ` <span style="font-size:9px;cursor:pointer;color:var(--accent);text-decoration:underline" onclick="event.stopPropagation(); PA.tableInfo.open('${PA.escJs(t.tableName || '')}', '${PA.escJs(t.schemaName || '')}')">def</span>`;
        }

        // Tag actual persistent tables vs. SQL Server temp tables/table variables — see
        // PA.classifyTableKind's doc comment (shared/tableFramework.js).
        const kind = PA.classifyTableKind(t.tableName);
        const kindBadge = kind.code === 'TABLE' ? '' : `<span class="kind-badge ${kind.cls}" title="${esc(kind.label)}">${esc(kind.label)}</span>`;

        let html = `<tr class="to-row" onclick="PA.tf.toggleDetail('to',${idx})">`;
        html += `<td><span style="font-weight:600;color:var(--teal);cursor:pointer" onclick="event.stopPropagation(); PA.tableInfo.open('${PA.escJs(t.tableName || '')}', '${PA.escJs(t.schemaName || '')}')">${esc(t.tableName || '')}</span> ${kindBadge}</td>`;
        html += `<td>${ttBadge}</td>`;
        html += `<td>${esc(t.schemaName || '-')}</td>`;
        html += `<td>${ops}</td>`;
        html += `<td>${count}${claudeCount > 0 ? ' <span class="claude-badge" title="' + claudeCount + ' found by Claude AI">AI</span>' : ''}</td>`;
        html += `<td>${trigLabel}</td>`;
        html += `<td>${scope}</td>`;
        html += `</tr>`;
        return html;
    },

    /**
     * UI-002 (dedicated project, §133+) — the same Direct/Full-Flow accessDetails matching
     * `_applyScopeData`'s own pre-conversion array-path fallback already implemented (below, in the
     * `else` branch this same section's history left in place), extracted so it can also scope one
     * table's own freshly-fetched access list at expand time — see `_renderDetail`. Not duplicated:
     * a scoped row's server-computed `accessCount` and its expand panel's own filtered array must
     * never disagree, and this is the exact algorithm `PlsqlScopeFilter.trim` already applies
     * server-side to the grid itself, so reusing it here (rather than writing a second one) is what
     * keeps the two in step.
     */
    _scopedAccessDetails(allDetails) {
        const ctx = PA.context;
        if (!ctx || !ctx.procId) return allDetails;
        if (PA.tableOps.scopeMode === 'scoped') {
            const currentProcId = (ctx.procId || '').toUpperCase();
            const parts = currentProcId.split('.');
            const currentLast = parts.pop();
            const currentPkg = parts.length > 0 ? parts.pop() : '';
            return allDetails.filter(d => {
                const pid = (d.procedureId || d.procedureName || '').toUpperCase();
                if (pid === currentProcId) return true;
                if (currentPkg) return pid.endsWith('.' + currentPkg + '.' + currentLast);
                return pid === currentLast;
            });
        }
        const nodeIds = ctx.callTreeNodeIds;
        if (!nodeIds || nodeIds.size === 0) return allDetails;
        return allDetails.filter(d => {
            const pid = (d.procedureId || d.procedureName || '').toUpperCase();
            if (nodeIds.has(pid)) return true;
            for (const nid of nodeIds) {
                if (pid.includes(nid) || nid.includes(pid)) return true;
            }
            return false;
        });
    },

    /**
     * Render detail content for expandable row.
     *
     * UI-002 (dedicated project, §133+) — `table.accessDetails` no longer arrives with the grid
     * row (see `AnalyzerController.getTableOperationsPage`'s own doc comment: it was the actual
     * unbounded payload this row's DoD names, one entry per access line on every row of every
     * page). Fetched fresh here instead, only for the one table whose panel is opened — via the
     * same already-existing, already-bounded `/tables/{tableName}` endpoint `tableDetail.js`'s own
     * Accesses tab already uses — then scoped client-side with `_scopedAccessDetails` to match
     * whatever the grid's own server-computed `accessCount` already reflects.
     */
    _renderDetail(table, idx, esc) {
        const triggers = (table.triggers || []).filter(tr => tr.status === 'ENABLED');
        let html = '<div class="to-detail">';

        // One shared panel for both pages and for the popup modal — see shared/access-details.js.
        // Was a flat unbounded div list; N reaches 1,385 on real analyses.
        //
        // The id carries a per-expand sequence, not just the row index: the index is reused
        // across re-renders (a re-sort assigns it to a different table), and this panel's
        // getTableDetail fetch is ASYNC — a slow response landing after a re-render used to find
        // the SAME id belonging to a different table's panel and write this table's access
        // details into it. A unique id makes the stale fetch's lookup miss instead.
        const seq = ++PA.tableOps._detailSeq;
        const wrapId = 'ad-s-' + seq + '-wrap';
        html += '<div id="' + wrapId + '" style="padding:8px 0;color:var(--text-muted);font-size:11px">Loading access details…</div>';

        setTimeout(async () => {
            let allDetails = [];
            try {
                const detail = await PA.api.getTableDetail(table.tableName);
                allDetails = (detail && detail.accessDetails) || [];
            } catch (e) {
                allDetails = [];
            }

            const scopeActive = PA.tableOps.scopeMode === 'scoped'
                || (PA.tableOps.scopeMode === 'all' && PA.context && PA.context.callTreeNodeIds
                    && PA.context.callTreeNodeIds.size > 0);
            const details = scopeActive ? PA.tableOps._scopedAccessDetails(allDetails) : allDetails;
            const fullCount = allDetails.length;

            const wrap = document.getElementById(wrapId);
            if (!wrap) return; // row was collapsed/rerendered before the fetch resolved

            if (details.length === 0) {
                wrap.innerHTML = '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">No access details available</div>';
                return;
            }
            const adId = 'ad-s-' + seq;
            wrap.innerHTML = AccessDetails.render(adId, details,
                { fullCount: fullCount, scoped: scopeActive && details.length < fullCount });
            AccessDetails.mount(adId, details, {
                // See the parser copy: joins column provenance in from the statement index.
                analysisName: PA.api && PA.api._analysisName,
                tableName: table.tableName,
                jumpFn: d => (d.sourceFile && d.lineNumber)
                    ? `PA.codeModal.openAtLine('${PA.escJs(d.sourceFile)}', ${d.lineNumber})`
                    : null
            });
        }, 0);

        if (triggers.length > 0) {
            html += '<div class="to-detail-section" style="border-top:1px solid var(--border);padding-top:8px">';
            html += '<div class="to-detail-section-title" style="color:var(--orange)">Enabled Triggers (' + triggers.length + ')</div>';
            for (const tr of triggers) {
                html += '<div class="to-trigger-item">';
                html += `<span class="trig-badge">${esc(tr.triggerType || '')}</span>`;
                html += `<span class="to-trigger-name" style="cursor:pointer;color:var(--accent);text-decoration:underline" onclick="event.stopPropagation(); PA.triggerModal.open('${PA.escJs(table.tableName || '')}', '${PA.escJs(tr.triggerName || '')}')">${esc(tr.triggerName || '')}</span>`;
                html += `<span class="to-trigger-event">${esc(tr.triggeringEvent || '')}</span>`;
                if (tr.calledProcedures && tr.calledProcedures.length > 0) {
                    html += '<div class="to-trigger-calls">Calls: ';
                    for (const proc of tr.calledProcedures) {
                        html += `<span class="to-detail-proc" style="margin-right:8px" onclick="event.stopPropagation(); PA.showProcedure('${PA.escJs(proc)}')">${esc(proc)}</span>`;
                    }
                    html += '</div>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        // UI-002 (dedicated project, §133+) — used to also check `details.length === 0` here for a
        // combined "no access details or triggers" message, but access details are no longer known
        // synchronously (fetched on demand above). The wrap's own async callback already shows "No
        // access details available" for that case; nothing further to add here when triggers is
        // also empty.

        html += '</div>';
        return html;
    },

    /** Apply scope based on PA.context — switches dataset in table framework */
    applyScope() {
        const ctx = PA.context;
        if (!ctx || !ctx.procId) {
            PA.tableOps.scopeMode = 'all';
            PA.tableOps._updateScopeToggle();
            if (PA.tf._state['to']) PA.tf.setData('to', PA.tableOps.data);
            PA.tableOps._updateCounts();
            return;
        }
        // Default to 'all' when proc has no direct tables to avoid empty view
        if (!ctx.scopedTables || ctx.scopedTables.length === 0) {
            PA.tableOps.scopeMode = 'all';
        } else {
            PA.tableOps.scopeMode = 'scoped';
        }
        PA.tableOps._updateScopeToggle();
        PA.tableOps._applyScopeData();
    },

    /** Switch scope mode: 'scoped' or 'all' */
    setScope(mode) {
        PA.tableOps.scopeMode = mode;
        PA.tableOps._updateScopeToggle();
        PA.tableOps._applyScopeData();
    },

    /**
     * Internal: apply scope filter to table data.
     *
     * "Direct" (scoped): tables accessed by the SELECTED proc only (not its children).
     *   - Filters tables to those in scopedTables
     *   - Filters accessDetails to only the selected proc (PA.context.procId)
     *
     * "Full Flow" (all): all tables in the call subtree.
     *   - Filters tables to those in scopedTables
     *   - Filters accessDetails to procs in callTreeNodeIds (entire subtree)
     *
     * When no proc is selected: shows all tables from the analysis.
     */
    _applyScopeData() {
        const mode = PA.tableOps.scopeMode;
        const ctx = PA.context;
        const src = PA.tableOps._src;

        if (src) {
            // Server path (PlsqlScopeFilter, §46/§50): the same "Direct"/"Full Flow" semantics,
            // ported field-for-field — see its own javadoc for exactly which fallback each mode
            // reproduces. No proc selected, or "all" with no call-tree walked yet, means unscoped.
            if (!ctx || !ctx.procId) {
                src.setParam('directProcId', '');
                src.setParam('nodeIds', []);
            } else if (mode === 'scoped') {
                src.setParam('directProcId', ctx.procId);
                src.setParam('nodeIds', []);
            } else if (ctx.callTreeNodeIds && ctx.callTreeNodeIds.size > 0) {
                src.setParam('directProcId', '');
                src.setParam('nodeIds', Array.from(ctx.callTreeNodeIds));
            } else {
                // "Full Flow" with no walked call tree yet: same as the array path's own
                // scopedNames/scopedKeys fallback below, but that fallback needs the whole array
                // to filter over — nothing to port here until that path is also converted.
                src.setParam('directProcId', '');
                src.setParam('nodeIds', []);
            }
            PA.tf._restart('to');
            PA.tableOps._updateCounts();
            return;
        }

        // No proc selected → show all analysis tables
        if (!ctx || !ctx.procId) {
            PA.tf.setData('to', PA.tableOps.data);
            PA.tableOps._updateCounts();
            return;
        }

        // Proc selected but has no scoped tables → show empty in Direct, use callTree for Full Flow
        if (!ctx.scopedTables || ctx.scopedTables.length === 0) {
            if (mode === 'scoped') {
                PA.tf.setData('to', []);
                PA.tableOps._updateCounts();
                return;
            }
            // Full Flow: fall through to use callTreeNodeIds
        }

        // Build set of flow-scoped table keys from the detail endpoint
        const scopedKeys = new Set();
        const scopedNames = new Set();
        for (const t of (ctx.scopedTables || [])) {
            const name = (t.tableName || '').toUpperCase();
            const schema = (t.schemaName || '').toUpperCase();
            scopedNames.add(name);
            if (schema) scopedKeys.add(schema + '.' + name);
        }

        const nodeIds = ctx.callTreeNodeIds;
        const _matchNode = (pid) => {
            if (!nodeIds || nodeIds.size === 0) return false;
            const up = pid.toUpperCase();
            if (nodeIds.has(up)) return true;
            for (const nid of nodeIds) {
                if (up.includes(nid) || nid.includes(up)) return true;
            }
            return false;
        };

        if (mode === 'scoped') {
            const currentProcId = (ctx.procId || '').toUpperCase();
            const parts = currentProcId.split('.');
            const currentLast = parts.pop();
            const currentPkg = parts.length > 0 ? parts.pop() : '';
            const direct = PA.tableOps.data.map(t => {
                const allDetails = t.accessDetails || [];
                const directDetails = allDetails.filter(d => {
                    const pid = (d.procedureId || d.procedureName || '').toUpperCase();
                    if (pid === currentProcId) return true;
                    if (currentPkg) {
                        return pid.endsWith('.' + currentPkg + '.' + currentLast);
                    }
                    return pid === currentLast;
                });
                if (directDetails.length === 0) return null;
                return Object.assign({}, t, {
                    accessDetails: directDetails,
                    accessCount: directDetails.length,
                    _fullAccessDetails: allDetails,
                    _fullAccessCount: t.accessCount || allDetails.length
                });
            }).filter(Boolean);
            PA.tf.setData('to', direct);
        } else {
            // "Full Flow": show tables accessed by any node in the call subtree
            if (nodeIds && nodeIds.size > 0) {
                const flowScoped = PA.tableOps.data.map(t => {
                    const allDetails = t.accessDetails || [];
                    const scopedDetails = allDetails.filter(d => {
                        const pid = (d.procedureId || d.procedureName || '').toUpperCase();
                        return _matchNode(pid);
                    });
                    if (scopedDetails.length === 0) return null;
                    return Object.assign({}, t, {
                        accessDetails: scopedDetails,
                        accessCount: scopedDetails.length,
                        _fullAccessDetails: allDetails,
                        _fullAccessCount: t.accessCount || allDetails.length
                    });
                }).filter(Boolean);
                PA.tf.setData('to', flowScoped);
            } else if (scopedNames.size > 0) {
                const flowTables = PA.tableOps.data.filter(t => {
                    const name = (t.tableName || '').toUpperCase();
                    const schema = (t.schemaName || '').toUpperCase();
                    if (schema && scopedKeys.size > 0) return scopedKeys.has(schema + '.' + name);
                    return scopedNames.has(name);
                });
                PA.tf.setData('to', flowTables);
            } else {
                PA.tf.setData('to', []);
            }
        }
        PA.tableOps._updateCounts();
    },

    /** Update scope toggle button states + visibility */
    _updateScopeToggle() {
        const toggle = document.getElementById('toScopeToggle');
        if (!toggle) return;
        const ctx = PA.context;
        if (!ctx || !ctx.procId) {
            toggle.style.display = 'none';
            return;
        }
        toggle.style.display = '';
        const btnScoped = toggle.querySelector('[data-scope="scoped"]');
        const btnAll = toggle.querySelector('[data-scope="all"]');
        if (btnScoped) btnScoped.classList.toggle('active', PA.tableOps.scopeMode === 'scoped');
        if (btnAll) btnAll.classList.toggle('active', PA.tableOps.scopeMode === 'all');
    },

    focusTable(tableName) {
        PA.switchRightTab('tableOps');
        // Set search to find the table
        const searchInput = document.getElementById('toSearch');
        if (searchInput) {
            searchInput.value = tableName;
            PA.tf.setSearch('to', tableName);
            PA.tf.filter('to');
        }
    }
};
