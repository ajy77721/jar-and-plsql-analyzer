window.PA = window.PA || {};

/*
 * Table operations — server-paged.
 *
 * Converted alongside its endpoint rather than after it: the grid's search, sort, column filters
 * and Direct/Subtree/SubFlow scope all have to move server-side together. Any one of them left in
 * the browser would filter the fifty rows in memory and look entirely convincing.
 */
PA.tableOps = {
    /**
     * Retained and left empty. `tableDetail._loadTableData` treats this as a warm cache and falls
     * back to a full fetch when it is empty, which is now always — correct, and one table's worth
     * of work rather than the grid's.
     */
    data: [],
    _src: null,
    _summary: null,
    activeOps: new Set(),

    /**
     * Once-per-session cache guard (§259) — same `_loaded` shape `jar/js/codeTree.js` and PL/SQL's
     * `PA.joins`/`PA.cursors`/`PA.predicates` (`plsql/js/app.js`) already use. The actual gate that
     * stops `switchRightTab` from calling `load()` again on a plain tab revisit lives in
     * `parser/js/app.js` (`PA._rightTabLoaded.tableOps`, no longer reset on switch-away — see that
     * file's own `switchRightTab` comment); this flag mirrors that state on the data side so a
     * caller here can tell "has this tab's data ever been fetched" without reaching into app.js.
     * `load()` itself is still always a real reload when called directly (e.g.
     * `claudeEnrichment.js`'s deliberate post-enrichment refresh) — this flag is informational, not
     * a short-circuit, so that forced reload keeps working.
     */
    _loaded: false,

    async load() {
        PA.tableOps._src = PA.api.tablesPageSource(PA.tableOps._scopeParams());
        // The grid has always opened on Count descending; asked for up front so page 1 arrives in
        // that order rather than being re-fetched after init.
        PA.tableOps._src.setParam('sort', 'USAGES_DESC');
        PA.tableOps.activeOps.clear();
        PA.tableOps._initOpPills();
        PA.tableOps._initTable();
        var st = PA.tf.state('to');
        if (st && st.firstLoad) await st.firstLoad;
        await PA.tableOps._loadSummary();
        PA.tableOps._loaded = true;
    },

    /** The scope selector as request parameters. */
    _scopeParams: function() {
        var ctx = PA.context || {};
        if (!ctx.procId || PA._isPackageRoot()) return { scope: 'full' };
        return { scope: PA._scope, procId: PA._currentScopeId() };
    },

    _loadSummary: async function() {
        try {
            PA.tableOps._summary = await PA.api.facetSummary('tables/page',
                    PA.tableOps._scopeParams());
            PA.tableOps._updateCounts();
            // `facetSummary` reports a failed request by resolving to `null` (it calls `PA.apiFail`
            // itself), not by throwing — so a null summary here is the failure case, same as the
            // catch below, and must not be handed to `initColFilters` as if it were "no values yet".
            if (!PA.tableOps._summary) {
                PA.tf.colFiltersFailed('to', PA.tableOps._colDefs(), 'request failed');
                return;
            }
            PA.tf.initColFilters('to', PA.tableOps._colDefs(), PA.tableOps._summary.columnValues);
            PA.tf._updateSortIndicators('to');
        } catch (e) {
            PA.tf.colFiltersFailed('to', PA.tableOps._colDefs(), 'request failed');
        }
    },

    _initTable() {
        var container = document.getElementById('toContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'to\',0)">Schema</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'to\',1)">Table</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'to\',2)">Type</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'to\',4)">Count</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'to\',5)">Triggers</th>';
        html += '<th data-sort-col="6" onclick="PA.tf.sort(\'to\',6)">Procedures</th>';
        html += '<th data-sort-col="7" onclick="PA.tf.sort(\'to\',7)">Usage</th>';
        html += '</tr></thead><tbody id="to-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('to', PA.tableOps._src, 50, PA.tableOps._renderRow, {
            // The endpoint's closed sort enum, keyed by the column index the <th> carries. A column
            // absent from this map is not sortable — the honest answer, since sorting in the browser
            // would reorder the fifty rows on screen and nothing else.
            serverSorts: {
                0: { asc: 'SCHEMA_ASC', desc: 'SCHEMA_DESC' },
                1: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                2: { asc: 'TYPE_ASC', desc: 'TYPE_DESC' },
                4: { asc: 'USAGES_ASC', desc: 'USAGES_DESC' },
                6: { asc: 'PROCS_ASC', desc: 'PROCS_DESC' }
            },
            renderDetail: PA.tableOps._renderDetail,
            onFilter: PA.tableOps._updateCounts
        });

        var s = PA.tf.state('to');
        if (s) { s.sortCol = 4; s.sortDir = 'desc'; }  // default: sort by Count (col 4) desc

        // BUG-FIX (§259, real-audit §258 finding #2): `PA.tf.init` just above already fetched page 1
        // with `sort=USAGES_DESC` baked into `_src` (set in `load()`, before `_initTable` runs), so on
        // the server-paged path calling `filter('to')` here only re-runs an identical restart 200ms
        // later — the exact "same URL twice within one click" duplicate the audit captured
        // (`tables/page?...sort=USAGES_DESC...` firing back-to-back). The array-path fallback still
        // needs `filter()` to run its own client-side sort/search, so it's kept for that case only.
        if (!PA.tableOps._src) PA.tf.filter('to');

        // Placeholder icons the instant the table is in the DOM. The real attach happens once
        // `_loadSummary` (called from `load()`, after this) resolves or fails — not on a blind
        // `setTimeout(0)` racing that fetch, which could fire before or after it settled.
        PA.tf.pendingColFilters('to', PA.tableOps._colDefs());
        PA.tf._updateSortIndicators('to');
    },

    /** Keys are column indices and MUST match the <th> order in `_initTable`. */
    _colDefs() {
        return {
            0: { label: 'Schema', valueFn: function(t) { return t.tableSchema || ''; } },
            1: { label: 'Table', valueFn: function(t) { return t.tableName || ''; } },
            2: { label: 'Type', valueFn: function(t) { return t.objectType || 'TABLE'; } },
            3: { label: 'Operation', valueFn: function(t) { return (t.operations || []).slice(); } },
            4: { label: 'Count', valueFn: function(t) { return String(t.accessCount || (t.accessDetails || []).length); } },
            5: { label: 'Triggers', valueFn: function(t) { return String((t.triggers || []).length); } },
            // UI-002 (dedicated project, §133+) — was derived from accessDetails (one entry per
            // access line, deduplicated back down to unique procedure names); usedBy is already
            // one entry per distinct accessing procedure, so this is the same set for free.
            6: { label: 'Procedure', valueFn: function(t) {
                return (t.usedBy || []).map(function(u) { return u.nodeName || ''; }).filter(Boolean);
            }},
            7: { label: 'Usage', valueFn: function(t) {
                return (t.usedBy || []).map(function(u) { return u.nodeName || ''; }).filter(Boolean);
            }}
        };
    },

    /**
     * Sends the pill selection as `types` and reloads page 1.
     *
     * <p>Was a client-side predicate over `t.operations`. With one page in memory that hides rows
     * from the fifty on screen while the header still counts every table — a filter that appears
     * to work and silently disagrees with its own count.
     */
    _pushOps: function() {
        var src = PA.tableOps._src;
        if (!src) return;
        var ops = PA.tableOps.activeOps;
        // No pill selected means "all", which is an ABSENT parameter — not `types=`, which would
        // be a request for the empty set.
        src.setParam('types', ops.size ? [...ops].join(',') : '');
        PA.tf._restart('to');
        PA.tableOps._loadSummary();
    },

    _initOpPills: function() {
        var container = document.getElementById('toOpPills');
        if (!container) return;
        // TEMP is a fourth, filterable category alongside the operations: the server's tables
        // facet exposes the row's true kind as a `types` value, so the same pill mechanism filters
        // to temp tables (`types=TEMP`) with no extra request parameter. Selecting it with an
        // operation is an OR, exactly as selecting two operations already is.
        var ops = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE', 'TRUNCATE', 'DROP', 'TEMP'];
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
        PA.tableOps._pushOps();
    },

    clearFilters: function() {
        document.getElementById('toSearch').value = '';
        PA.tableOps.activeOps.clear();
        document.querySelectorAll('#toOpPills .op-filter-pill').forEach(function(p) { p.classList.add('active'); });
        PA.tf.setSearch('to', '');
        var s = PA.tf.state('to');
        if (s) { s.colFilters = {}; }
        var src = PA.tableOps._src;
        if (src) {
            src.setParam('q', '');
            src.setParam('types', '');
            src.setParam('cf', []);
        }
        PA.tf._restart('to');
        PA.tableOps._loadSummary();
        PA.tf._cfUpdateIcons('to');
    },

    onSearch: function() {
        var el = document.getElementById('toSearch');
        PA.tf.setSearch('to', el ? el.value : '');
        PA.tf.filter('to');
    },

    _updateCounts: function() {
        // Counts come from the summary endpoint, not from rows in memory: with one page loaded,
        // `data.length` is fifty and the header would read "50 tables" however many there are.
        var s = PA.tf.state('to');
        var sum = PA.tableOps._summary;
        var allTotal = sum ? sum.total : 0;
        var dataTotal = sum ? sum.scopedTotal : allTotal;
        var shown = (s && s.totalIsExact) ? s.serverTotal : dataTotal;
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
        // UI-002 (dedicated project, §133+) — each usedBy entry already is one distinct accessing
        // procedure; no need to flatten into accessDetails and re-derive uniqueness from names.
        var procCount = (t.usedBy || []).length || new Set((t.accessDetails || []).map(function(d) { return d.procedureName || ''; }).filter(Boolean)).size;
        var objType = t.objectType || 'TABLE';
        var typeCls = objType === 'VIEW' ? 'view' : (objType === 'MATERIALIZED VIEW' || objType === 'MATERIALIZED_VIEW') ? 'mv' : 'table';

        // Tag each row as an actual persistent table vs. a SQL Server temp table/table variable —
        // see PA.classifyTableKind's own doc comment (shared/tableFramework.js) for why this
        // matters (found live: #stage6/##g103/@ids105 all mixed into one "tables" list with no way
        // to tell them apart from a real table at a glance).
        // Prefer the parse-time classification (tableKind) — it covers every dialect's temp syntax
        // (Oracle GLOBAL TEMPORARY, Db2 DECLARE GLOBAL TEMPORARY, Postgres/MySQL TEMPORARY), not
        // just SQL Server's #/##/@ name prefixes. Fall back to the name-pattern classifier for
        // rows written before the field existed.
        var kind;
        if (t.tableKind === 'TEMP' || t.kind === 'TEMP') {
            var scope = t.tempScope ? (' — ' + t.tempScope) : '';
            kind = { code: 'TEMP', label: 'TEMP', cls: 'kind-temp',
                     tip: 'TEMP' + scope + ' — session-scoped, not persisted. Modernization note: '
                        + 'replace with application-memory or the target stack\'s own temp handling.' };
        } else {
            kind = PA.classifyTableKind(t.tableName);
        }
        var kindBadge = kind.code === 'TABLE' ? '' : '<span class="kind-badge ' + kind.cls + '" title="' + esc(kind.tip || kind.label) + '">' + esc(kind.label) + '</span>';

        var html = '<tr class="to-row" onclick="PA.tf.toggleDetail(\'to\',' + idx + ')">';
        html += '<td><span style="color:var(--text-muted)">' + esc(t.tableSchema || '-') + '</span></td>';
        html += '<td><span style="font-weight:600;color:var(--teal);cursor:pointer" data-tip="Click to view table details" onclick="event.stopPropagation(); PA.tableDetail.open(\'' + PA.escJs(t.tableName || '') + '\', \'' + PA.escJs(t.tableSchema || '') + '\')">' + esc(t.tableName || '') + '</span> ' + kindBadge + '</td>';
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

    /**
     * UI-002 (dedicated project, §133+) — `table.accessDetails` no longer arrives with the grid
     * row (see `api.js`'s `tablesPageSource`, whose own doc comment explains why); flattened here,
     * synchronously, from `table.usedBy` instead — the row's own already-resident, already-scoped
     * (per `PlsqlScopeFilter.directTables`/`subtreeTables`) compact wire shape — only for the one
     * table whose panel is actually opened. No network round trip needed: unlike the live-crawl
     * sibling (`plsql/js/tableOps.js`), this backend's per-node-per-operation shape was already on
     * the row all along; the fix here is not fetching more, it's building less, less often.
     */
    _renderDetail: function(table, idx, esc) {
        var details = [];
        var usedBy = table.usedBy || [];
        for (var i = 0; i < usedBy.length; i++) {
            var ops = usedBy[i].operations || {};
            for (var op in ops) {
                if (!Object.prototype.hasOwnProperty.call(ops, op)) continue;
                var lines = ops[op] || [];
                for (var j = 0; j < lines.length; j++) {
                    details.push({
                        operation: op,
                        procedureId: usedBy[i].nodeId || '',
                        procedureName: usedBy[i].nodeName || '',
                        lineNumber: lines[j],
                        sourceFile: ''
                    });
                }
            }
        }
        var fullCount = table._fullAccessCount || details.length;
        var isScoped = PA._scope !== 'full' && table._fullAccessDetails;
        var html = '<div class="to-detail">';

        // One shared panel for both pages and for the popup modal — see shared/access-details.js.
        // Was a flat unbounded div list; N reaches 1,385 on real analyses.
        var adId = 'ad-p-' + idx;
        html += AccessDetails.render(adId, details, { fullCount: fullCount, scoped: isScoped });
        // PA.tf.toggleDetail assigns innerHTML immediately after this renderer returns, so the
        // markup is in the document by the time a zero-timeout task runs.
        setTimeout(function () {
            AccessDetails.mount(adId, details, {
                // Lets the panel join column provenance in from the statement index; the rows
                // themselves come from usedBy[].operations, which carries no columns at all.
                analysisName: PA.api && PA.api._analysisName,
                tableName: table.tableName,
                // The per-row sourceFile is empty on this page, so the accessor's source is
                // resolved from its node id instead.
                jumpFn: function (d) {
                    return d.procedureId
                        ? "PA.sourceView.openNodeAtLine('" + PA.escJs(d.procedureId) + "', " + d.lineNumber + ")"
                        : null;
                }
            });
        }, 0);


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
            html += PA.usageTable.attach('ut-to-triggers-' + idx, _trigTableOps, {
                showTitle: false, nameLabel: 'Table', countLabel: 'tables', showSnippet: false,
                name: function (tto) { return (tto.schema ? tto.schema + '.' : '') + tto.tableName; },
                nameLink: function (tto) { return "PA.tableDetail.open('" + PA.escJs(tto.tableName) + "','" + PA.escJs(tto.schema) + "')"; },
                sub: function (tto) { return { text: 'via ' + (tto.triggerName || '') }; },
                badges: function (tto) { return [tto.operation, tto.objectType]; },
                search: function (tto) { return tto.triggerName || ''; }
            });
            html += '</div>';
        }

        if (PA.claude && PA.claude.renderDetailEnrichment) {
            html += PA.claude.renderDetailEnrichment(table.tableName);
        }

        html += '</div>';
        return html;
    },

    /**
     * Scope now travels with the request.
     *
     * <p>It used to be applied by filtering each row's `accessDetails` in the browser. With one page
     * in memory that filters fifty rows and reports the result as the whole answer — the scope
     * selector would appear to work and would be wrong for every analysis larger than a page.
     */
    applyScope: function() {
        var src = PA.tableOps._src;
        if (!src) return;
        var params = PA.tableOps._scopeParams();
        src.setParam('scope', params.scope);
        src.setParam('procId', params.procId || '');
        PA.tf._restart('to');
        PA.tableOps._loadSummary();
    },

    // _applyScopeData deleted: scope is a request parameter now. It filtered each row's
    // accessDetails in the browser, which with one page loaded scopes fifty rows and reports the
    // result as the whole answer.


    // UI-002 (dedicated project, §133+) — unchanged output, but no longer needs accessDetails:
    // each usedBy entry already is one distinct accessing procedure. Dead code (no callers), kept
    // consistent with the same conversion rather than left reading a field that no longer exists.
    _usageCount: function(t) {
        return (t.usedBy || []).length;
    },

    /**
     * UI-002 (dedicated project, §133+) — was grouped from accessDetails (one entry per access
     * line, re-grouped back into per-procedure operation sets). `usedBy[]` already carries exactly
     * that grouping natively (`{nodeName, operations: {OP: [lines]}}`), so this reads it directly
     * instead of flattening and re-deriving it.
     */
    _renderUsageSummary: function(t) {
        var usedBy = t.usedBy || [];
        if (!usedBy.length) return '<span style="color:var(--text-muted)">-</span>';
        var procOps = {};
        for (var i = 0; i < usedBy.length; i++) {
            var pName = usedBy[i].nodeName || '';
            if (!pName) continue;
            if (!procOps[pName]) procOps[pName] = new Set();
            var ops = usedBy[i].operations || {};
            for (var op in ops) {
                if (Object.prototype.hasOwnProperty.call(ops, op) && (ops[op] || []).length) procOps[pName].add(op);
            }
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
