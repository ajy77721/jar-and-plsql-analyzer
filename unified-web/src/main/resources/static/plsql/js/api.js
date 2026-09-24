/**
 * PA.api — API module for PL/SQL Analyzer.
 */
window.PA = window.PA || {};

PA.api = {
    /**
     * UI-034 — the same in-flight-promise-dedup pattern as `jar/js/api.js`'s `getCallTree`
     * (`_callTreeInFlight`)/`_dedupe` and `parser/js/api.js`'s `_dedupe`: a second call for the
     * exact same request while one is already pending joins the pending promise instead of firing
     * a second network round trip. Not applied to mutating calls (`analyze`, `deleteAnalysis`) or
     * to any must-be-fresh polling endpoint (none of that shape exist in this file today).
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

    // ---- Analysis ----
    async analyze(username, objectName, objectType, procedureName, project, env, fast) {
        const body = { username, objectName, objectType };
        if (procedureName) body.procedureName = procedureName;
        if (project) body.project = project;
        if (env) body.env = env;
        const endpoint = fast ? '/api/plsql/analysis/analyze-fast' : '/api/plsql/analysis/analyze';
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async getLatestResult() {
        return PA.api._dedupe('getLatestResult', async () => {
            const res = await fetch('/api/plsql/analysis');
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    async listVersions() {
        return PA.api._dedupe('listVersions', async () => {
            const res = await fetch('/api/plsql/analysis/versions');
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    async loadVersion(version) {
        return PA.api._dedupe('loadVersion|' + version, async () => {
            const res = await fetch(`/api/plsql/analysis/versions/${version}`);
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    // ---- Connection info ----
    async getDbConnections(project, env) {
        let url = '/api/plsql/db/connections';
        const params = [];
        if (project) params.push('project=' + encodeURIComponent(project));
        if (env) params.push('env=' + encodeURIComponent(env));
        if (params.length) url += '?' + params.join('&');
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) return { available: false };
        return res.json();
    },

    async getAnalysisConnections(name) {
        const res = await fetch(`/api/plsql/analysis/history/${encodeURIComponent(name)}/connections`, { cache: 'no-store' });
        if (!res.ok) return { available: false };
        return res.json();
    },

    // ---- History (named analyses) ----
    async listHistory() {
        const res = await fetch('/api/parser/analyses');
        if (!res.ok) return PA.apiFail(res);
        const rows = await res.json();
        // /api/parser/analyses speaks the parser index.json vocabulary (entryPoint, totalNodes,
        // totalTables, errors[]), but the history renderer reads the older PLSQL-service field
        // names. Without this adaptation every card rendered as "<schema>." with 0 procs / 0
        // tables. Same mapping loadAnalysis() below already performs for a single analysis.
        return (Array.isArray(rows) ? rows : []).map(r => {
            const entryPoint = r.entryPoint || r.name || '';
            const dotIdx = entryPoint.indexOf('.');
            return Object.assign({}, r, {
                entryObjectName: dotIdx >= 0 ? entryPoint.substring(dotIdx + 1) : entryPoint,
                flowNodeCount: r.totalNodes || 0,
                flowTableCount: r.totalTables || 0,
                flowErrorCount: Array.isArray(r.errors) ? r.errors.length : 0,
                // The list endpoint carries no sequence/join/cursor totals. Left undefined on
                // purpose so the renderer omits those segments rather than asserting a false 0.
                hasConnections: !!(r.connectionName || r.jdbcUrl)
            });
        });
    },

    async loadAnalysis(name) {
        // Try parser service first (newer file-based storage)
        const parserRes = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}/index`);
        if (parserRes.ok) {
            const idx = await parserRes.json();
            // Adapt parser index.json format to the UI's expected analysis format
            const entryPoint = idx.entryPoint || name;
            const dotIdx = entryPoint.indexOf('.');
            const entryObjectName = dotIdx >= 0 ? entryPoint.substring(dotIdx + 1) : entryPoint;
            return {
                name: name,
                _isParser: true,
                _packageAnalysis: idx._packageAnalysis === true,
                entryObjectName: entryObjectName,
                entrySchema: idx.entrySchema || '',
                entryProcedure: '',
                procedureCount: idx.totalNodes || 0,
                tableCount: idx.totalTables || 0,
                totalEdges: idx.totalEdges || 0,
                totalLinesOfCode: idx.totalLinesOfCode || 0,
                maxDepth: idx.maxDepth || 0,
                crawlTimeMs: idx.crawlTimeMs || 0,
                errors: idx.errors || [],
                status: 'complete'
            };
        }
        // Fall back to old PLSQL service
        const res = await fetch(`/api/plsql/analysis/history/${encodeURIComponent(name)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async deleteAnalysis(name) {
        // History is listed from the parser store (see listHistory), so delete must target the
        // parser endpoint first. Going straight to the legacy PLSQL route made every Delete
        // button on the history list a silent 404. Falls back for legacy-only analyses.
        const parserRes = await fetch(`/api/parser/analyses/${encodeURIComponent(name)}`, { method: 'DELETE' });
        if (parserRes.ok) return { deleted: name };
        const res = await fetch(`/api/plsql/analysis/history/${encodeURIComponent(name)}`, { method: 'DELETE' });
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    // Helper: get current parser analysis name (if the loaded analysis is from parser service)
    _parserName() {
        return (window.PA && PA.analysisData && PA.analysisData._isParser) ? PA.analysisData.name : null;
    },

    // ---- Call Graph ----
    async getCallGraph() {
        const n = this._parserName();
        return PA.api._dedupe('getCallGraph|' + (n || ''), async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/call-graph`);
                if (!res.ok) { PA.apiFail(res); return null; }
                return res.json();
            }
            const res = await fetch('/api/plsql/analysis/call-graph');
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    /**
     * `opts.silentOn404` — for callers that try several *candidate* node ids in a row
     * (`autoSelectEntryProc`/`loadSidebarData`'s "which of schema.obj.proc / schema.obj / obj is
     * the real entry node" probes) a 404 here is an expected, silently-handled outcome, not a real
     * failure — the caller already `try/catch`es it and moves to the next candidate. Without this
     * flag every probe still ran `PA.apiFail(res)` unconditionally, which raises the shared
     * "A request returned 404" banner for a miss the caller was always going to recover from,
     * even on an analysis that goes on to load correctly. Real, non-probing callers (`showProcedure`
     * et al.) omit the flag and keep the banner.
     */
    async getProcDetail(procName, opts) {
        const n = this._parserName();
        const silent404 = !!(opts && opts.silentOn404);
        return PA.api._dedupe('getProcDetail|' + (n || '') + '|' + procName, async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/node/${encodeURIComponent(procName)}`);
                if (!res.ok) { if (!(silent404 && res.status === 404)) PA.apiFail(res); return null; }
                return res.json();
            }
            const res = await fetch(`/api/plsql/analysis/detail/${encodeURIComponent(procName)}`);
            if (!res.ok) { if (!(silent404 && res.status === 404)) PA.apiFail(res); return null; }
            return res.json();
        });
    },

    /**
     * UI-030c/d and UI-030-BUG's fix — all procedure reports plus the four cross-procedure reports,
     * assembled server-side in one request, on **either** backend: the live-crawl
     * `/api/plsql/analysis/reports` (UI-030c) or the parser `/api/parser/analyses/{name}/reports`
     * (UI-030-BUG's own fix, built against the parser backend's real `subtreeTables` shape rather
     * than porting the client logic that was silently broken against it). Callers still fall back to
     * the old per-procedure `getProcDetail` loop on any fetch failure, same as before.
     */
    async getProcedureReports() {
        const n = this._parserName();
        return PA.api._dedupe('getProcedureReports|' + (n || ''), async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/reports`);
                if (!res.ok) { PA.apiFail(res); return null; }
                return res.json();
            }
            const res = await fetch('/api/plsql/analysis/reports');
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    async getCallTree(procName) {
        const n = this._parserName();
        return PA.api._dedupe('getCallTree|' + (n || '') + '|' + procName, async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/call-tree/${encodeURIComponent(procName)}`);
                if (!res.ok) { PA.apiFail(res); return null; }
                return res.json();
            }
            const res = await fetch(`/api/plsql/analysis/call-tree/${encodeURIComponent(procName)}`);
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    async getCallerTree(procName) {
        const n = this._parserName();
        return PA.api._dedupe('getCallerTree|' + (n || '') + '|' + procName, async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/call-tree/${encodeURIComponent(procName)}/callers`);
                if (!res.ok) { PA.apiFail(res); return null; }
                return res.json();
            }
            const res = await fetch(`/api/plsql/analysis/call-tree/${encodeURIComponent(procName)}/callers`);
            if (!res.ok) { PA.apiFail(res); return null; }
            return res.json();
        });
    },

    // ---- Tables ----
    /**
     * MID-021 (§109) — BUG FOUND AND FIXED: the parser-backend branch used to return
     * `/api/parser/analyses/{name}/tables`'s response as-is: `{totalTables, tables:[...]}`, each
     * row shaped `{name, schema, objectType, allOperations, usedBy:[{nodeId, nodeName, operations:
     * {OP:[lines]}}]}` — the raw crawler artifact (confirmed against real on-disk data and
     * `TableFacetReader.getTables`'s source directly, not assumed). `tableOps.js`'s renderer has
     * always read `tableName`/`schemaName`/`tableType`/`operations`/`accessDetails`/`accessCount`
     * instead, and `PA.tf.init` was handed the whole `{tables:[...]}` object rather than an array —
     * so the parser-backed Table Ops screen was silently broken (empty/garbage grid), the same class
     * of bug as UI-030-BUG. Adapts the response into the shape `tableOps.js` already expects,
     * client-side, rather than changing the wire format `ParserFacetSpecs.TABLES`'s paged sibling
     * and `TableFacetReader` both still depend on.
     */
    async getTableOperations() {
        const n = this._parserName();
        return PA.api._dedupe('getTableOperations|' + (n || ''), async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/tables`);
                if (!res.ok) return PA.apiFail(res);
                const raw = await res.json();
                return PA.api._adaptParserTables(raw);
            }
            const res = await fetch('/api/plsql/analysis/tables');
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    /** MID-021 (§109) — see getTableOperations' own doc comment for why this exists. */
    _adaptParserTables(raw) {
        const tables = (raw && Array.isArray(raw.tables)) ? raw.tables : [];
        return tables.map(PA.api._adaptParserTableRow);
    },

    /**
     * MID-021 (§110) — factored out of `_adaptParserTables` so the same per-row mapping can also
     * run as `PA.pageSource`'s own `mapRow` hook for the new `/tables/page`-equivalent parser
     * endpoint (`tablesPageSource`, below) — one page of raw `usedBy[]` rows in, `accessDetails[]`
     * rows out, exactly as `tableOps.js` already renders for the live-crawl backend.
     *
     * MID-021 (§138): `u.predicates` — a flat array of `{columnName, operator, value, lineNumber}`,
     * the artifact's own new field (`ChunkedFlowWriter.writeTableIndex`, `TableOperationInfo
     * .whereFilters` finally carried through) — is grouped by `lineNumber` and attached as each
     * matching `accessDetails` entry's own `whereFilters`, the exact field `predicates.js`'s
     * `_buildPredicateData` already reads. This is the wiring fix that unblocks the parser
     * backend's own predicates screen: the data was always extracted, just dropped before this.
     */
    _adaptParserTableRow(t) {
        const accessDetails = [];
        (t.usedBy || []).forEach(u => {
            const predicatesByLine = {};
            (u.predicates || []).forEach(p => {
                (predicatesByLine[p.lineNumber] || (predicatesByLine[p.lineNumber] = [])).push(p);
            });
            const ops = u.operations || {};
            Object.keys(ops).forEach(op => {
                (ops[op] || []).forEach(lineNumber => {
                    accessDetails.push({
                        operation: op,
                        procedureName: u.nodeName || '',
                        procedureId: u.nodeId || '',
                        lineNumber: lineNumber,
                        whereFilters: predicatesByLine[lineNumber] || []
                    });
                });
            });
        });
        return {
            tableName: t.name || '',
            schemaName: t.schema || '',
            tableType: t.objectType || 'TABLE',
            operations: t.allOperations || [],
            accessDetails: accessDetails,
            accessCount: accessDetails.length,
            external: t.external || false,
            triggers: t.triggers || []
        };
    },

    async getTableDetail(tableName) {
        const res = await fetch(`/api/plsql/analysis/tables/${encodeURIComponent(tableName)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    // ---- Sequences ----
    async getSequenceOperations() {
        const n = this._parserName();
        return PA.api._dedupe('getSequenceOperations|' + (n || ''), async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/sequences`);
                if (!res.ok) return PA.apiFail(res);
                return res.json();
            }
            const res = await fetch('/api/plsql/analysis/sequences');
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    // ---- Joins ----
    async getJoinOperations() {
        const n = this._parserName();
        return PA.api._dedupe('getJoinOperations|' + (n || ''), async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/joins`);
                if (!res.ok) return PA.apiFail(res);
                return res.json();
            }
            const res = await fetch('/api/plsql/analysis/joins');
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    // ---- Cursors ----
    async getCursorOperations() {
        const n = this._parserName();
        return PA.api._dedupe('getCursorOperations|' + (n || ''), async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/cursors`);
                if (!res.ok) return PA.apiFail(res);
                return res.json();
            }
            const res = await fetch('/api/plsql/analysis/cursors');
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    /*
     * UI-028 — paged siblings of the whole-array reads above, for the plsql live-crawl backend
     * only (checked via _parserName(): when a parser-backed analysis is loaded, the caller stays
     * on the array path unchanged — see plsql/js/tableOps.js's own comment for why. `params` may
     * carry `directProcId` or `nodeIds`, PlsqlScopeFilter's two scope modes; passed straight
     * through as request parameters, same reasoning as the parser app's own facetPageSource.
     *
     * MID-021 (§107): `joins.js`/`cursors.js` also get a *parser*-backend paged source now —
     * `ParserAnalysisController.getJoins`/`getCursors` gained the identical `directProcId`/
     * `nodeIds` params (reusing `PlsqlScopeFilter`, confirmed row-shape-compatible with
     * `accessDetails[]` before wiring this), so `_applyScopeData`'s existing `if (src) {...}`
     * branch works unchanged for either backend once `_src` points at the right URL — no new
     * client-side scope logic needed, just the right page source.
     *
     * MID-021 (§110): `tableOps.js` gets the same treatment now too — `getTablesPage` gained
     * `directProcId`/`nodeIds` via `PlsqlScopeFilter.directTables`/`subtreeTables` (the `usedBy[]`-
     * shaped variant §107 found was needed), and each page is mapped through
     * `_adaptParserTableRow` so the grid keeps reading `accessDetails[]`/`accessCount` regardless of
     * backend. `predicates.js` stays array-only (in fact, unsupported — see its own load() comment):
     * the parser crawler's `usedBy[]` entries never carry `whereFilters` at all, confirmed against
     * real on-disk `api/tables/index.json` data, so there is no predicate data to page over — a
     * crawler-side gap, not a wiring gap this layer can close.
     */
    tablesPageSource(params) {
        const n = this._parserName();
        if (n) {
            return PA.pageSource(`/api/parser/analyses/${encodeURIComponent(n)}/tables/page`,
                    params || {}, PA.api._adaptParserTableRow);
        }
        return PA.pageSource('/api/plsql/analysis/tables/page', params || {});
    },

    joinsPageSource(params) {
        const n = this._parserName();
        if (n) return PA.pageSource(`/api/parser/analyses/${encodeURIComponent(n)}/joins`, params || {});
        return PA.pageSource('/api/plsql/analysis/joins/page', params || {});
    },

    cursorsPageSource(params) {
        const n = this._parserName();
        if (n) return PA.pageSource(`/api/parser/analyses/${encodeURIComponent(n)}/cursors`, params || {});
        return PA.pageSource('/api/plsql/analysis/cursors/page', params || {});
    },

    /**
     * MID-021 (predicates.js's own paged-conversion project) — the parser backend gets a paged
     * source too now. Unlike `tablesPageSource`/`joinsPageSource`/`cursorsPageSource`, no `mapRow`
     * is needed here: `ParserAnalysisController.getPredicates` already aggregates server-side via
     * `PlsqlPredicatesFacet.build` (the same class the live-crawl backend uses), so every row
     * arrives in the one shared display shape regardless of which backend produced it.
     */
    predicatesPageSource(params) {
        const n = this._parserName();
        if (n) {
            return PA.pageSource(`/api/parser/analyses/${encodeURIComponent(n)}/predicates`, params || {});
        }
        return PA.pageSource('/api/plsql/analysis/predicates/page', params || {});
    },

    // ---- Errors ----
    async getParseErrors() {
        const res = await fetch('/api/plsql/analysis/errors');
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    // ---- Procedures ----
    async getProcedures() {
        const n = this._parserName();
        return PA.api._dedupe('getProcedures|' + (n || ''), async () => {
            if (n) {
                const res = await fetch(`/api/parser/analyses/${encodeURIComponent(n)}/procedures`);
                if (!res.ok) return PA.apiFail(res);
                return res.json();
            }
            const res = await fetch('/api/plsql/analysis/procedures');
            if (!res.ok) return PA.apiFail(res);
            return res.json();
        });
    },

    // ---- Source ----
    async getSource(owner, objectName) {
        const res = await fetch(`/api/plsql/analysis/source/${encodeURIComponent(owner)}/${encodeURIComponent(objectName)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    // ---- References ----
    async getReferences(objectName, owner) {
        let url = `/api/plsql/analysis/references/${encodeURIComponent(objectName)}`;
        if (owner) url += `?owner=${encodeURIComponent(owner)}`;
        const res = await fetch(url);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    // ---- Search ----
    async search(query, type = 'all') {
        const res = await fetch(`/api/plsql/analysis/search?q=${encodeURIComponent(query)}&type=${type}`);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    // ---- Database ----
    async getDbUsers() {
        const res = await fetch('/api/plsql/db/users');
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async testDbConnection(username) {
        const res = await fetch('/api/plsql/db/test', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username })
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async listDbObjects(username) {
        const res = await fetch(`/api/plsql/db/objects/${encodeURIComponent(username)}`);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async listPackages(username) {
        const res = await fetch(`/api/plsql/db/packages/${encodeURIComponent(username)}`);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async listPackageProcs(username, packageName) {
        const res = await fetch(`/api/plsql/db/package/${encodeURIComponent(username)}/${encodeURIComponent(packageName)}`);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async getDbSource(username, objectName, objectType) {
        const res = await fetch(`/api/plsql/db/source/${encodeURIComponent(username)}/${encodeURIComponent(objectName)}?objectType=${encodeURIComponent(objectType)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    // ---- Smart Find ----
    async findObject(input) {
        const res = await fetch(`/api/plsql/db/find/${encodeURIComponent(input)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    // ---- Claude Verification ----
    async getClaudeStatus() {
        const res = await fetch('/api/plsql/claude/status');
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async startClaudeVerification(resume) {
        const res = await fetch('/api/plsql/claude/verify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ resume: resume ? 'true' : 'false' })
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async getClaudeProgress() {
        const res = await fetch('/api/plsql/claude/progress');
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async getClaudeResult() {
        const res = await fetch('/api/plsql/claude/result');
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async getClaudeResultByName(name) {
        const res = await fetch(`/api/plsql/claude/result/${encodeURIComponent(name)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async listClaudeChunks() {
        const res = await fetch('/api/plsql/claude/chunks');
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async listClaudeChunkSummaries() {
        const res = await fetch('/api/plsql/claude/chunks/summary');
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async getClaudeChunk(chunkId) {
        const res = await fetch(`/api/plsql/claude/chunks/${encodeURIComponent(chunkId)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async getTableChunkMapping() {
        const res = await fetch('/api/plsql/claude/table-chunks');
        // This one returns a MAP, not a list. apiFail's sentinel is an array, and callers index it
        // by table name — an array happens to behave the same way there, but returning the wrong
        // shape from a function documented as a map is the kind of thing that bites two changes
        // later. The banner still fires; only the returned shape differs.
        if (!res.ok) { PA.apiFail(res); return {}; }
        return res.json();
    },

    async listClaudeSessions(analysisName) {
        let url = '/api/plsql/claude/sessions';
        if (analysisName) url += `?analysisName=${encodeURIComponent(analysisName)}`;
        const res = await fetch(url);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async killClaudeSession(sessionId) {
        const res = await fetch(`/api/plsql/claude/sessions/${encodeURIComponent(sessionId)}/kill`, { method: 'POST' });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async killAllClaudeSessions() {
        const res = await fetch('/api/plsql/claude/sessions/kill-all', { method: 'POST' });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    connectClaudeProgress(onProgress) {
        const es = new EventSource('/api/plsql/claude/progress/stream');
        es.addEventListener('claude-progress', e => onProgress(e.data));
        let errCount = 0;
        es.addEventListener('error', () => {
            if (++errCount > 3 || es.readyState === EventSource.CLOSED) es.close();
        });
        return es;
    },

    // ---- Table Info ----
    /** Get table metadata from pre-cached analysis result (instant, no DB query) */
    async getTableMetadata(tableName) {
        const res = await fetch(`/api/plsql/analysis/tables/${encodeURIComponent(tableName)}/metadata`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async getTableTriggers(tableName) {
        const res = await fetch(`/api/plsql/analysis/tables/${encodeURIComponent(tableName)}/triggers`);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    /** Fallback: get table info from live DB (slower) */
    async getTableInfo(tableName, schema) {
        let url = `/api/plsql/db/table-info/${encodeURIComponent(tableName)}`;
        if (schema) url += `?schema=${encodeURIComponent(schema)}`;
        const res = await fetch(url);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    async executeQuery(sql, schema) {
        const res = await fetch('/api/plsql/db/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql, schema })
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    // ---- Analysis Jobs ----
    async listJobs() {
        const res = await fetch('/api/queue');
        if (!res.ok) return PA.apiFail(res);
        const data = await res.json();
        // /api/queue returns {current, queued, history} — flatten into a list for the Jobs panel
        const jobs = [];
        if (data.current) jobs.push(data.current);
        if (data.queued) jobs.push(...data.queued);
        if (data.history) jobs.push(...data.history);
        return jobs;
    },

    async getJob(jobId) {
        const res = await fetch(`/api/queue/${encodeURIComponent(jobId)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        const data = await res.json();
        // Queue API returns {error: "..."} for not-found instead of 404
        if (data && data.error) return null;
        return data;
    },

    async cancelJob(jobId) {
        const res = await fetch(`/api/queue/${encodeURIComponent(jobId)}/cancel`, { method: 'POST' });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    // ---- Config CRUD ----
    async listProjects() {
        const res = await fetch('/api/plsql/config/projects');
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async createProject(project) {
        const res = await fetch('/api/plsql/config/projects', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(project)
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async updateProject(name, project) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(name)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(project)
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async deleteProject(name) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(name)}`, { method: 'DELETE' });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async listEnvironments(projectName) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments`);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async createEnvironment(projectName, env) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(env)
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async updateEnvironment(projectName, envName, env) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(env)
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async deleteEnvironment(projectName, envName) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}`, { method: 'DELETE' });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async listConnections(projectName, envName) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}/connections`);
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async testConnection(conn) {
        const res = await fetch('/api/plsql/config/test-connection', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(conn)
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    /**
     * Standard TCP port → database-type table from the server
     * (`GET /api/plsql/config/db-type-suggest`, the authoritative copy of `DbType.standardPortMap`):
     * `{ "1521": "ORACLE", "3306": "MYSQL", ... }`. The setup modal uses it to auto-select a DB type
     * as a port is typed. Never raises the shared error banner — returns `{}` on failure so the
     * caller quietly keeps its built-in fallback table.
     */
    async getDbTypeSuggestions() {
        const res = await fetch('/api/plsql/config/db-type-suggest', { cache: 'no-store' });
        if (!res.ok) return {};
        return res.json();
    },

    // Upload a TLS certificate/wallet for a connection.
    // content: PEM text for .pem, base64 for .zip/.jks → { ok:true, path:"certs/{name}/{filename}" }
    async uploadCert(connectionName, filename, content) {
        const res = await fetch(`/api/plsql/config/certs/${encodeURIComponent(connectionName)}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename, content })
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async addConnection(projectName, envName, conn) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}/connections`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(conn)
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async updateConnection(projectName, envName, connName, conn) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}/connections/${encodeURIComponent(connName)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(conn)
        });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async deleteConnection(projectName, envName, connName) {
        const res = await fetch(`/api/plsql/config/projects/${encodeURIComponent(projectName)}/environments/${encodeURIComponent(envName)}/connections/${encodeURIComponent(connName)}`, { method: 'DELETE' });
        if (!res.ok) return PA.apiFail(res);
        return res.json();
    },

    async resolveEnvironment(project, env) {
        const res = await fetch(`/api/plsql/config/resolve?project=${encodeURIComponent(project)}&env=${encodeURIComponent(env)}`);
        if (!res.ok) { PA.apiFail(res); return null; }
        return res.json();
    },

    // ---- SSE Progress ----
    connectProgress(onProgress, onComplete, onError) {
        const es = new EventSource('/api/plsql/progress');
        let closed = false;
        let gotAnyEvent = false;
        let errorCount = 0;
        const closeOnce = () => { if (!closed) { closed = true; es.close(); } };
        es.addEventListener('progress', e => { gotAnyEvent = true; errorCount = 0; onProgress(e.data); });
        es.addEventListener('complete', e => { gotAnyEvent = true; onComplete(e.data); closeOnce(); });
        es.addEventListener('error', e => {
            errorCount++;
            // Only treat as fatal error if we got a server-sent error event with data,
            // or if we've had too many consecutive errors (SSE is truly broken)
            if (e.data) {
                closeOnce();
                onError(e.data);
            } else if (errorCount > 5) {
                // SSE is dead — but DON'T call onError, just close SSE silently.
                // Job polling will handle the rest.
                closeOnce();
                console.warn('[SSE] Too many errors, closing SSE. Job polling continues.');
            }
            // Otherwise: EventSource auto-reconnects, let it retry
        });
        return es;
    }
};
