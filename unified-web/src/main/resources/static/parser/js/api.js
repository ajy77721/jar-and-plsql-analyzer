window.PA = window.PA || {};

PA.api = {
    _analysisName: null,

    setAnalysis(name) { PA.api._analysisName = name; },

    /**
     * UI-034 — the same in-flight-promise-dedup pattern `jar/js/api.js`'s `getCallTree`
     * (`_callTreeInFlight`) and `_dedupe` generalization use: a second call for the exact same
     * request while one is already pending joins that pending promise instead of firing a second
     * network round trip. Deliberately NOT applied to `claudeStatus`/`claudeProgress` (must-be-fresh
     * polling calls, same exclusion as the `jar` module) or to any mutating call (`runAnalysis`,
     * `clearCache`, `claudeVerify`/`claudeKillSession`/`claudeApply`/`claudeReview`/`claudeRevert`).
     */
    _inFlight: {},

    async _dedupe(key, fn) {
        const pending = PA.api._inFlight[key];
        if (pending) return pending;
        const p = fn();
        PA.api._inFlight[key] = p;
        try {
            return await p;
        } finally {
            delete PA.api._inFlight[key];
        }
    },

    /** Clear the in-memory analysis cache. Pass a name to clear one analysis, omit for all. */
    async clearCache(name) {
        const url = '/api/parser/cache/clear' + (name ? '?name=' + encodeURIComponent(name) : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error('clear cache failed');
        return res.json();
    },

    async listHistory() {
        const res = await fetch('/api/parser/analyses');
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    /**
     * History as projected rows (Q31): `errorCount` instead of `errors[]`, `dbType` derived by the
     * server, no `jdbcUrl`.
     *
     * @param opts {limit, q, cf} — `cf` is a list of `<column>:<v1>|<v2>` column filters
     */
    async listHistoryPage(opts) {
        var o = opts || {};
        var params = new URLSearchParams();
        params.set('limit', o.limit || 200);
        params.set('sort', o.sort || 'TIME_DESC');
        if (o.q) params.set('q', o.q);
        (o.cf || []).forEach(function (f) { params.append('cf', f); });
        const res = await fetch('/api/parser/analyses?' + params.toString(),
            { headers: { 'Accept': PA.PAGE_MEDIA_TYPE } });
        if (!res.ok) { PA.apiFail(res); return null; }
        return PA.normalizePage(await res.json());
    },

    /**
     * Counts, dropdown values and running totals for the current filter (Q31).
     *
     * These were recomputed in the browser over the whole history on every keystroke, twice — once
     * for the History toolbar and once for the Performance Summary's stat cards.
     */
    async historyFacets(opts) {
        var o = opts || {};
        var params = new URLSearchParams();
        if (o.q) params.set('q', o.q);
        (o.cf || []).forEach(function (f) { params.append('cf', f); });
        const qs = params.toString();
        const res = await fetch('/api/parser/analyses/facets' + (qs ? '?' + qs : ''));
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    /** The error messages of one analysis — fetched only when a detail panel shows them. */
    async analysisErrors(name) {
        const res = await fetch('/api/parser/analyses/' + encodeURIComponent(name) + '/errors');
        if (!res.ok) return PA.apiFail(res);
        return (await res.json()).errors || [];
    },

    async runAnalysis(entryPoint, body) {
        const payload = body || { entryPoint: entryPoint };
        if (!payload.entryPoint) payload.entryPoint = entryPoint;
        const res = await fetch('/api/parser/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
        return res.json();
    },

    async getIndex() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getIndex|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/index`);
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    /**
     * UI-024 (§113) — used to be a single unparamterised `fetch`, which routes to
     * `FacetPageResponder`'s legacy bare-array path (no `limit`, no `Accept` header, so
     * `wantsEnvelope()` is false) — one unbounded server-side read and response, the exact defect
     * MID-021/MID-022 exist to catch elsewhere. `PA.loadAnalysis`'s two callers (`app.js:30,1070`)
     * still need every node resident synchronously afterward — 39 downstream call sites across 20
     * files read `PA.nodeIndex` synchronously, and making *that* lazy is real, separate, higher-risk
     * work with no corpus here to validate a cache-miss redesign against (recorded in the registry,
     * §113, rather than attempted blind). What *is* fixed here: the read is now paginated — bounded
     * per request, same as every other converted facet — while this method's own external contract
     * (returns the full array) stays identical, so neither caller needed to change.
     */
    async getProcedures() {
        const name = PA.api._analysisName;
        const src = PA.pageSource(`/api/parser/analyses/${encodeURIComponent(name)}/procedures`, {});
        const all = [];
        try {
            for (;;) {
                const page = await src.fetchPage(500);
                if (page.items && page.items.length) all.push.apply(all, page.items);
                if (!page.page || !page.page.hasMore) break;
            }
        } catch (e) {
            return PA.apiFail({ status: 0, url: `/api/parser/analyses/${name}/procedures` });
        }
        return all;
    },

    /**
     * UI-024 (§112) — `export.js`'s own full-node read, not the grid's. Deliberately a different
     * endpoint from `getProcedures()` above: that one now answers with the paged envelope
     * (`ParserFacetSpecs.PROCEDURES`'s 8-of-21-field projection), which is the wrong shape for
     * export's Call Flow/Complexity/Summary sheets — this hits `/procedures/export` instead, the
     * unpaged, unprojected array, fetched once per export rather than once per analysis load.
     */
    async getProceduresForExport() {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/procedures/export`);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    /**
     * MID-002's threshold branch: once the sidebar's procedure count is past
     * `PA._LP_SERVER_FILTER_THRESHOLD`, `leftPanel.js` searches here instead of scanning its own
     * in-memory copy. `/procedures` already supports `q` (same id+name+schema key the panel's own
     * client-side filter uses — see `ParserFacetSpecs.PROCEDURES`), so this is a thin wrapper, not a
     * new server capability.
     */
    async searchProcedures(q, limit) {
        const name = PA.api._analysisName;
        const params = new URLSearchParams();
        if (q) params.set('q', q);
        params.set('limit', String(limit || 200));
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/procedures?${params}`);
        // See getStatementsSummary: return the sentinel, not a bare [], so PA.failed() works and
        // the left panel can show "search failed" instead of "No matches".
        if (!res.ok) return PA.apiFail(res);
        const body = await res.json();
        // Paged response is an envelope ({ items, page }); the plain-array legacy shape only comes
        // back for a caller that sends none of limit/q/sort/cf — this call always sends both.
        return Array.isArray(body) ? body : (body.items || []);
    },

    async getNodeDetail(nodeFile) {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/node/${encodeURIComponent(nodeFile)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async getTableOperations() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getTableOperations|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/tables`);
            if (!res.ok) return PA.apiFail(res);
            const data = await res.json();
            return (data.tables || []).filter(t => t.dbValidated !== true || t.dbFound !== false).map(t => ({
                tableName: t.name,
                tableSchema: t.schema || '',
                objectType: t.objectType || 'TABLE',
                triggers: t.triggers || [],
                operations: t.allOperations || [],
                accessCount: (t.usedBy || []).reduce((sum, u) => {
                    return sum + Object.values(u.operations || {}).reduce((s, lines) => s + lines.length, 0);
                }, 0),
                accessDetails: (t.usedBy || []).flatMap(u => {
                    return Object.entries(u.operations || {}).flatMap(([op, lines]) => {
                        return (lines || []).map(line => ({
                            operation: op,
                            procedureId: u.nodeId || '',
                            procedureName: u.nodeName || '',
                            lineNumber: line,
                            sourceFile: ''
                        }));
                    });
                })
            }));
        });
    },

    /**
     * UI-001 — one table's operations row, by name, without the whole-corpus flatMap
     * {@link getTableOperations} does. `tableDetail.js` used to fall back to
     * `getTableOperations()` (every table, every access line) just to find one row by name when
     * `PA.tableOps.data` had not loaded yet — the exact "huge read for a small answer" shape this
     * registry has fixed elsewhere. `/tables/page` already supports `q`, searched on table name
     * (`ParserFacetSpecs.TABLES`), so this asks for exactly the one row instead.
     *
     * @returns the same shape {@link getTableOperations} returns for a matching row, or null
     */
    async getTableDetailByName(tableName) {
        if (!tableName) return null;
        const name = PA.api._analysisName;
        const params = new URLSearchParams();
        params.set('q', tableName);
        params.set('limit', '5'); // small headroom for a schema-qualified near-match, not a scan
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/tables/page?${params}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        const body = await res.json();
        const rows = Array.isArray(body) ? body : (body.items || []);
        const upper = tableName.toUpperCase();
        const t = rows.find(r => (r.name || '').toUpperCase() === upper);
        if (!t) return null;
        var details = [];
        (t.usedBy || []).forEach(function (u) {
            var ops = u.operations || {};
            Object.keys(ops).forEach(function (op) {
                (ops[op] || []).forEach(function (line) {
                    details.push({
                        operation: op,
                        procedureId: u.nodeId || '',
                        procedureName: u.nodeName || '',
                        lineNumber: line,
                        sourceFile: ''
                    });
                });
            });
        });
        return {
            tableName: t.name,
            tableSchema: t.schema || '',
            objectType: t.objectType || 'TABLE',
            triggers: t.triggers || [],
            operations: t.allOperations || [],
            accessCount: details.length,
            accessDetails: details
        };
    },

    /**
     * Table rows for a list: `{tableName, tableSchema, objectType, operations[], accessCount}`.
     *
     * <p>The left panel renders a name, operation badges and a count. It used to get them from
     * {@link getTableOperations}, which flat-maps `usedBy[].operations` into one object per access
     * line — about 1M at the stated target — and then discards every one of them. Measured on the
     * ORACLE analysis in `data/`: 148.4 KB and 2,435 discarded objects become 4.9 KB and 41 rows.
     *
     * <p>Field names are mapped to the same shape `getTableOperations` returns, so the panel's
     * renderer is unchanged. `accessDetails` is deliberately absent rather than empty: a caller that
     * needs the access lines must go to `getTableOperations`, and an empty array would let it read
     * "no accesses" from a document that simply does not carry them.
     */
    async getTableList() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getTableList|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/tables/list`);
            if (!res.ok) { PA.apiFail(res); return null; }
            const data = await res.json();
            return (data.tables || []).map(t => ({
                tableName: t.name,
                tableSchema: t.schema || '',
                objectType: t.objectType || 'TABLE',
                operations: t.operations || [],
                accessCount: t.accessCount || 0
            }));
        });
    },

    // Table counts only. The topbar needs one integer; fetching the whole index for it cost
    // 152 KB measured (~307 KB at the largest observed) on every analysis load.
    async getTablesSummary() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getTablesSummary|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/tables/summary`);
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    async getTablesIndex() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getTablesIndex|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/tables`);
            if (!res.ok) { PA.apiFail(res); return null; }
            const data = await res.json();
            if (data && Array.isArray(data.tables)) {
                data.tables = data.tables.filter(t => t.dbValidated !== true || t.dbFound !== false);
            }
            return data;
        });
    },

    /**
     * UI-024 — server-side aggregation for `statements.js`'s statement-type summary, replacing
     * its two `PA.nodeIndex.all()` scans. `scope`/`procId` mirror every other scoped facet call
     * here (`exHandlersPageSource` etc.) — `full`/no procId means unscoped.
     */
    async getStatementsSummary(scope, procId) {
        const name = PA.api._analysisName;
        const params = new URLSearchParams();
        if (scope) params.set('scope', scope);
        if (procId) params.set('procId', procId);
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/statements/summary?${params}`);
        // `PA.apiFail(res); return [];` discarded apiFail's return value and handed back a plain
        // empty array, so `PA.failed()` — the identity test the whole sentinel design rests on —
        // was false and the caller could not tell a failed request from an empty result. Returning
        // the sentinel keeps the same shape (it IS a frozen empty array) and makes it detectable.
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    /**
     * UI-024 — server-side aggregation for `detailHeader.js`'s seven scope-derived stat counts
     * (`_dhUniqueCallCount`/`_dhUniqueCallCountByType`/`_dhUniqueTriggerCount`/
     * `_dhUniqueCursorCount`/`_dhFullCallCountByType`/`_dhFullTriggerCount`/`_dhFullCursorCount`),
     * collapsed into one request. `scope`/`procId` mean the same as everywhere else here.
     */
    async getDetailHeaderCounts(scope, procId) {
        const name = PA.api._analysisName;
        const params = new URLSearchParams();
        if (scope) params.set('scope', scope);
        if (procId) params.set('procId', procId);
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/detail-header/counts?${params}`);
        if (!res.ok) { PA.apiFail(res); return { intCalls: 0, extCalls: 0, totalCalls: 0, triggers: 0, cursors: 0 }; }
        return res.json();
    },

    /**
     * UI-024 — server-side filter for `tableDetail.js`'s "parsed source triggers" merge step,
     * replacing its `PA.nodeIndex.all()` scan for `TRIGGER`-type nodes matching one table.
     */
    async getTriggersForTable(tableName) {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/tables/${encodeURIComponent(tableName)}/triggers`);
        // See getStatementsSummary: return the sentinel, not a bare [], so PA.failed() works and
        // tableDetail can tell "trigger request failed" from "this table has no triggers".
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    /**
     * UI-024 — server-side computation for `complexity.js`'s per-procedure risk score, replacing
     * its `PA.nodeIndex.all()` scan. `weights`/`thresholds`/`algorithm`/`exponent` are the client's
     * own already-resolved effective config (server defaults + `localStorage` overrides,
     * `_resolveConfig`) — the server has no config of its own to apply, it only computes.
     */
    async getComplexityScores(weights, thresholds, algorithm, exponent) {
        const name = PA.api._analysisName;
        const params = new URLSearchParams();
        if (weights) {
            if (weights.loc != null) params.set('wLoc', weights.loc);
            if (weights.tables != null) params.set('wTables', weights.tables);
            if (weights.callsOut != null) params.set('wCallsOut', weights.callsOut);
            if (weights.cursors != null) params.set('wCursors', weights.cursors);
            if (weights.dynamicSql != null) params.set('wDynamicSql', weights.dynamicSql);
            if (weights.depth != null) params.set('wDepth', weights.depth);
        }
        if (thresholds) {
            if (thresholds.medium != null) params.set('thMedium', thresholds.medium);
            if (thresholds.high != null) params.set('thHigh', thresholds.high);
        }
        if (algorithm) params.set('algorithm', algorithm);
        if (exponent != null) params.set('exponent', exponent);
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/complexity/scores?${params}`);
        // See getStatementsSummary: return the sentinel, not a bare [], so PA.failed() works.
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    /**
     * UI-024 — server-side aggregation for `summary.js`'s scoped dashboard, replacing its
     * `PA.getScopedNodes()`/`PA.nodeIndex.all()` scan. Unlike `getComplexityScores`, this
     * genuinely collapses to a small aggregate, not a row per procedure.
     */
    async getSummaryStats(scope, procId, entrySchema) {
        const name = PA.api._analysisName;
        const params = new URLSearchParams();
        if (scope) params.set('scope', scope);
        if (procId) params.set('procId', procId);
        if (entrySchema) params.set('entrySchema', entrySchema);
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/summary/stats?${params}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async getJoinOperations() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getJoinOperations|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/joins`);
            if (!res.ok) return PA.apiFail(res);
            const data = await res.json();
            return Array.isArray(data) ? data : (data.joins || data.items || []);
        });
    },

    /**
     * UI-013 §60/§62/§64 — `joinSummary.js`'s per-query grouping (`_buildQueryItems`), computed
     * server-side by `JoinQuerySummary` instead of folding the whole `/joins` array client-side.
     * Used only when `PA.joins.data` isn't already resident (the Join Summary tab is reachable
     * without the plain Joins tab ever loading first) — see `joinSummary.js:loadQueryOnly`.
     */
    async getJoinQuerySummary() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getJoinQuerySummary|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/joins/summary/queries`);
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    async getCursorOperations() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getCursorOperations|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/cursors`);
            if (!res.ok) return PA.apiFail(res);
            const data = await res.json();
            return Array.isArray(data) ? data : (data.cursors || data.items || []);
        });
    },

    async getSequenceOperations() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getSequenceOperations|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/sequences`);
            if (!res.ok) return PA.apiFail(res);
            const data = await res.json();
            return Array.isArray(data) ? data : (data.sequences || data.items || []);
        });
    },

    /*
     * Paged facets (PLAN-08 §4.3 step 12).
     *
     * A page source hands the pager one page at a time instead of the whole facet. The parameters
     * that used to be applied in the browser — search, sort, filter pills, column filters and the
     * Direct/Subtree/SubFlow scope — travel with the request, because with one page in memory
     * applying any of them client-side would filter that page and look convincing while being
     * wrong.
     */

    /** A `PA.pageSource` over a converted facet endpoint. */
    facetPageSource(facet, params, mapRow) {
        const name = PA.api._analysisName;
        return PA.pageSource(
            `/api/parser/analyses/${encodeURIComponent(name)}/${facet}`, params || {}, mapRow);
    },

    /**
     * UI-029 — procedures with exception handlers, one page at a time.
     *
     * <p>`cf: ['9:Y']` is the hidden preset filter column added to `ParserFacetSpecs.PROCEDURES`:
     * not a user-facing popover, just this screen's own question ("which procedures have exception
     * handlers") asked of the shared `/procedures` endpoint instead of scanning
     * `PA.nodeIndex.all()` in the browser. Any caller-supplied `cf` is preserved alongside it —
     * there is none today, but a future column filter on this screen should not silently replace
     * this preset.
     */
    exHandlersPageSource(params) {
        const p = Object.assign({}, params || {});
        p.cf = ['9:Y'].concat(p.cf || []);
        return PA.api.facetPageSource('procedures', p, function (n) {
            return {
                nodeId: n.nodeId || n.name || '',
                name: n.name || n.nodeId || '',
                schema: n.schema || '',
                objectType: n.objectType || '',
                sourceFile: n.sourceFile || '',
                detailFile: n.detailFile || '',
                lineStart: n.lineStart || 0,
                lineEnd: n.lineEnd || 0,
                handlerCount: (n.counts && n.counts.exceptionHandlers) || 0,
                handlers: null,
                _loaded: false
            };
        });
    },

    /**
     * The Table Operations grid, one page at a time.
     *
     * <p>`/tables/page` serves `api/tables/index.json` in the crawler's own field names, so each row
     * is adapted here to the shape the grid was written against.
     *
     * <p>UI-002 (dedicated project, §133+) — `accessDetails` used to be rebuilt from `usedBy` here
     * too, for every one of the fifty rows on screen, on every page turn, even though the grid only
     * ever reads it for the one row whose panel is expanded. `usedBy` is kept on the row instead
     * (already the compact, one-entry-per-accessing-procedure wire shape — not the ~1M-object cost
     * the flat-map itself was; that was the whole-corpus version FE-012 already fixed). `accessCount`
     * is summed directly from `usedBy[].operations`' line arrays, without materialising a details
     * array at all. `tableOps.js`'s own `_renderRow`/`_renderUsageSummary` were converted alongside
     * this to read `usedBy` directly (each entry already *is* one distinct accessing procedure, so
     * `usedBy.length` is the procedure count for free); `_renderDetail` builds `accessDetails` from
     * `table.usedBy` only when a row's own panel actually opens.
     */
    tablesPageSource(params) {
        return PA.api.facetPageSource('tables/page', params, function (t) {
            var usedBy = t.usedBy || [];
            var count = 0;
            for (var i = 0; i < usedBy.length; i++) {
                var ops = usedBy[i].operations || {};
                for (var op in ops) {
                    if (!Object.prototype.hasOwnProperty.call(ops, op)) continue;
                    count += (ops[op] || []).length;
                }
            }
            return {
                tableName: t.name,
                tableSchema: t.schema || '',
                objectType: t.objectType || 'TABLE',
                triggers: t.triggers || [],
                operations: t.allOperations || [],
                accessCount: count,
                usedBy: usedBy
            };
        });
    },

    /**
     * Header counts and filter value lists for a converted facet: `{total, scopedTotal,
     * typeCounts, types, columnValues}`. These came off the full array before; one small request
     * replaces reading a multi-megabyte one.
     */
    async facetSummary(facet, params) {
        const name = PA.api._analysisName;
        const q = new URLSearchParams();
        Object.keys(params || {}).forEach(k => {
            const v = params[k];
            if (v !== null && v !== undefined && v !== '') q.set(k, v);
        });
        const url = `/api/parser/analyses/${encodeURIComponent(name)}/${facet}/summary`
            + (q.toString() ? '?' + q.toString() : '');
        const res = await fetch(url);
        // 204 means the analysis has no such facet at all — a different answer from "zero rows
        // matched", and the caller renders it differently.
        if (res.status === 204) return null;
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async getDynamicSql() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getDynamicSql|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/dynamic-sql`);
            if (!res.ok) return PA.apiFail(res);
            const data = await res.json();
            return Array.isArray(data) ? data : (data.items || []);
        });
    },

    async dbTableInfo(tableName, schema) {
        let url = `/api/plsql/db/table-info/${encodeURIComponent(tableName)}`;
        if (schema) url += `?schema=${encodeURIComponent(schema)}`;
        const res = await fetch(url);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    /**
     * UI-019 (§142) — `maxDepth`, when given, fetches only that many levels rooted at `nodeId`;
     * a node cut there carries `moreChildren`/`childCount` rather than its real children, so the
     * caller can ask again, rooted at that node, for the next slice. Omitted (the default), this
     * is byte-for-byte the same whole-tree fetch every existing caller already relies on.
     */
    async getCallTree(nodeId, maxDepth) {
        const name = PA.api._analysisName;
        let url = `/api/parser/analyses/${encodeURIComponent(name)}/call-tree/${encodeURIComponent(nodeId)}`;
        if (maxDepth !== undefined && maxDepth !== null) url += `?maxDepth=${encodeURIComponent(maxDepth)}`;
        return PA.api._dedupe('getCallTree|' + name + '|' + nodeId + '|' + (maxDepth == null ? '' : maxDepth), async () => {
            const res = await fetch(url);
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    async getCallerTree(nodeId) {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getCallerTree|' + name + '|' + nodeId, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/call-tree/${encodeURIComponent(nodeId)}/callers`);
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    /**
     * The most-connected slice of the call graph.
     *
     * <p>The viz draws at most `MAX_SIMULATED_NODES` (FE-018) but used to fetch every edge to do
     * it: 847 today, 35,776 at 832 procedures in the load proof, so ~344,000 and ~100 MB at the
     * stated target. Degree cannot be computed client-side without first holding the whole graph,
     * which is the transfer being avoided, so the server selects.
     *
     * <p>`getCallGraph` stays for `export.js`, which needs every edge — an export of part of a
     * graph is wrong.
     */
    async getCallGraphTop(limit) {
        const name = PA.api._analysisName;
        const q = limit ? '?limit=' + encodeURIComponent(limit) : '';
        return PA.api._dedupe('getCallGraphTop|' + name + '|' + q, async () => {
            const res = await fetch(
                `/api/parser/analyses/${encodeURIComponent(name)}/call-graph/top${q}`);
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    async getCallGraph() {
        const name = PA.api._analysisName;
        return PA.api._dedupe('getCallGraph|' + name, async () => {
            const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/call-graph`);
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    async getSource(sourceFile) {
        const name = PA.api._analysisName;
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/source/${encodeURIComponent(sourceFile)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        const text = await res.text();
        return { content: text, owner: '', objectName: sourceFile };
    },

    async claudeStatus() {
        const res = await fetch('/api/parser/claude/status');
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeVerify(analysisName, resume) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ resume: !!resume })
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeProgress(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/progress`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    // The seven scalars the verification grids render. The full result document is 138,301 bytes
    // measured; five screens were fetching all of it to show three to seven numbers.
    // Returns null for 204 (never verified) and throws for a real failure, so a caller can tell
    // "no data" from "broken" — conflating the two is what hid a permanently empty grid.
    async claudeVerificationSummary(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/verification-summary`);
        if (res.status === 204) return null;
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeResult(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/result`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    /**
     * UI-013 §60/§62 — the per-table coverage counts `claudeEnrichment.js`'s overview modal used
     * to fold out of the whole `claudeResult()` document (138,301 B measured elsewhere for a
     * document `getVerificationSummary` exists specifically to avoid re-fetching). Unlike the
     * `PA.nodeIndex`-based aggregations (§62), nothing else on the page holds this document for
     * free, so this one is a genuine payload reduction, not just a CPU-cost move.
     */
    async claudeVerificationSummaryTables(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/verification-summary/tables`);
        if (res.status === 204) return [];
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeChunksSummary(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/chunks/summary`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeChunkDetail(analysisName, chunkId) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/chunks/${encodeURIComponent(chunkId)}`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeKillSession(analysisName, sessionId) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/sessions/${encodeURIComponent(sessionId)}/kill`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeVersions(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions`);
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeLoadStatic(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions/load-static`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeLoadClaude(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions/load-claude`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeRevert(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions/revert`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeReview(analysisName, payload) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/review`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeLoadPrev(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/versions/load-prev`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async claudeApply(analysisName) {
        const res = await fetch(`/api/parser/analyses/${encodeURIComponent(analysisName)}/claude/apply`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error(res.status + '');
        return res.json();
    },

    async getComplexityConfig() {
        const res = await fetch('/api/parser/config/complexity');
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async getJoinComplexityConfig() {
        const res = await fetch('/api/parser/config/join-complexity');
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    }
};
