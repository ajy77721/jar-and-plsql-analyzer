/**
 * REST API client — all fetch calls live here.
 */
window.JA = window.JA || {};

JA.api = {

    /**
     * UI-003 (§134) — must match `JarFacetSpecs.MAIN_JAR_SENTINEL` on the server exactly:
     * `sourceJar`'s own column filter can't match an empty value (`FacetPageResponder
     * .columnFilters` drops empty `cf` pieces before they reach the filter, so `cf=1:` silently
     * matches every JAR, not just the main app's — caught in calibration, see that constant's own
     * comment), so main-app classes are asked for using this sentinel instead of `''`.
     */
    MAIN_JAR_SENTINEL: '__MAIN_JAR__',

    /**
     * UI-033 — the `getCallTree`/`_callTreeInFlight` in-flight-promise-dedup pattern (see that
     * method's own comment below for the full rationale), generalized so other GETs proven to be
     * double-fired on repeat clicks (`getSummary`, `getSummaryHeaders`, `getClassTree`, and the
     * four `_ensureSliceLoaded` slice calls) can reuse it without copy-pasting the key/finally
     * bookkeeping. Deliberately NOT applied to the never-cache polling/status endpoints
     * (`getClaudeStatus`, `getClaudeProgress`, `anySessionRunning`, `listSessions`) — those must
     * hit the live server on every single call, per §231's own exclusion list.
     *
     * <p>Keyed by whatever string the caller builds (mirroring `getCallTree`'s own
     * `id|idx|version` convention) rather than by the resolved value — storing the value only
     * dedupes calls that arrive after the first has finished, which was never the problem; a
     * second call that arrives *while one is pending* is the case this exists for.
     */
    _inFlight: {},

    async _dedupe(key, fn) {
        const pending = JA.api._inFlight[key];
        if (pending) return pending;               // join the request already running
        const p = fn();
        JA.api._inFlight[key] = p;
        try {
            return await p;
        } finally {
            // Cleared on failure too, same as `_callTreeInFlight`: a failed attempt must not make
            // every later click return the same rejection forever.
            delete JA.api._inFlight[key];
        }
    },

    async listJars() {
        const res = await fetch('/api/jar/jars', { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Failed to list JARs');
        return res.json();
    },

    async uploadJar(file, mode, projectPath, renameSuffix, basePackage, entries) {
        const form = new FormData();
        form.append('file', file);
        if (entries && entries.length) form.append('entries', JSON.stringify(entries));
        let url = '/api/jar/jars?mode=' + encodeURIComponent(mode || 'static');
        if (projectPath) url += '&projectPath=' + encodeURIComponent(projectPath);
        if (renameSuffix) url += '&renameSuffix=' + encodeURIComponent(renameSuffix);
        if (basePackage && basePackage.trim()) url += '&basePackage=' + encodeURIComponent(basePackage.trim());
        const res = await fetch(url, { method: 'POST', body: form });
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || res.statusText);
        }
        return res.json();
    },

    async getAnalysis(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id);
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Analysis not found');
        return res.json();
    },

    async getSummary(id, version) {
        return JA.api._dedupe('getSummary|' + id + '|' + (version || ''), async () => {
            let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary';
            if (version) url += '?version=' + encodeURIComponent(version);
            const res = await fetch(url, { cache: 'no-store' });
            if (!res.ok) throw new Error(await res.text() || 'Summary not found');
            return res.json();
        });
    },

    /**
     * Q21 + Q22 — the header bar's scalars and the version flags, in one small request.
     *
     * 274 B measured, against 130 420 B for /summary/headers plus 115 B for /versions, which is
     * what drawing the title bar used to cost (PLAN-03 §1.2 I2 measured the underlying document at
     * 430 KB). The version flags are in here because the bar cannot be drawn without them and two
     * requests for one widget is what PLAN-08 Q22 asks to remove.
     */
    async getSummaryHeader(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/header';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Summary header not found');
        return res.json();
    },

    /**
     * Q21b — the endpoint list. Absent `limit`/`after` returns the whole array, as before.
     *
     * Pass `PA.pageSource(url)` to a pager to page it; `q` searches server-side, which has to move
     * with the paging or a client-side filter would search only the page in memory.
     */
    async getSummaryEndpoints(id, opts) {
        const o = opts || {};
        const q = new URLSearchParams();
        if (o.version) q.set('version', o.version);
        if (o.limit != null) q.set('limit', o.limit);
        if (o.after) q.set('after', o.after);
        if (o.q) q.set('q', o.q);
        const qs = q.toString();
        const url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/endpoints'
                  + (qs ? '?' + qs : '');
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Endpoints not found');
        return res.json();
    },

    /**
     * UI-026f/g — every endpoint's data-access report, plus the five cross-endpoint aggregations
     * (`vertReport`/`viewsReport`/`distReport`/`batchReport`/`extReport`), precomputed server-side.
     * Replaces `summary.js`'s own `_analyzeEndpoint` batch loop (§71: a synchronous, CPU-bound pass
     * over already-loaded data, not a per-procedure network call — that pattern is UI-030's real
     * subject, on the PL/SQL side).
     *
     * <p>`425` means the summary cache isn't built yet (call {@link #getSummary}/{@link
     * #getSummaryHeader} first) — every real caller already does, on JAR-open, so this is not a
     * new requirement in practice.
     */
    async getSummaryReports(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/reports';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Summary reports not found');
        return res.json();
    },

    /**
     * UI-025 — the endpoint list panel, one page at a time. Server order is `ORDINAL` only (the
     * analysis order the panel has always shown); there is nothing to sort by here beyond that.
     * Each row carries `endpointIndex` — its position in `analysis.endpoints[]`, the same value
     * `_origIdx` used to read straight off the in-memory array before this row.
     */
    endpointListPages(id, version) {
        const params = {};
        if (version) params.version = version;
        return PA.pageSource(
            '/api/jar/jars/' + encodeURIComponent(id) + '/summary/endpoints', params);
    },

    /**
     * The summary, preferring the small headers document and falling back to the full one.
     *
     * <h3>Why this is one function instead of seven try/catch pairs</h3>
     *
     * The pattern `try { getSummaryHeaders() } catch { getSummary() }` appeared at seven call
     * sites. `/summary/headers` is ~120 KB; `/summary` is the whole analysis — 47 MB on the
     * measured jar, and it has to be parsed into JS heap before anything renders. So the fallback
     * is a ~400x transfer, and every one of those seven sites took it **silently**: the page
     * simply became slow, with nothing to say why or that anything had gone wrong.
     *
     * Consolidated here so the degradation is reported once, in one place, and so the next caller
     * inherits the behaviour rather than copying the pattern and forgetting the warning.
     *
     * @param onDegraded optional; called with the header error when the full document is used
     */
    async loadSummary(id, version, onDegraded) {
        try {
            return await JA.api.getSummaryHeaders(id, version);
        } catch (headerError) {
            // Not silent. A user waiting on 47 MB deserves to know that is what is happening.
            if (typeof onDegraded === 'function') {
                onDegraded(headerError);
            } else if (JA.toast && JA.toast.warning) {
                JA.toast.warning('Summary headers unavailable — loading the full analysis, '
                    + 'which is much larger and slower. (' + (headerError.message || headerError) + ')');
            }
            console.warn('summary headers failed; falling back to the full document', headerError);
            return JA.api.getSummary(id, version);
        }
    },

    async getSummaryHeaders(id, version) {
        return JA.api._dedupe('getSummaryHeaders|' + id + '|' + (version || ''), async () => {
            let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/headers';
            if (version) url += '?version=' + encodeURIComponent(version);
            const res = await fetch(url, { cache: 'no-store' });
            if (!res.ok) throw new Error(await res.text() || 'Summary headers not found');
            return res.json();
        });
    },

    async getExternalCalls(id, version) {
        return JA.api._dedupe('getExternalCalls|' + id + '|' + (version || ''), async () => {
            let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/external-calls';
            if (version) url += '?version=' + encodeURIComponent(version);
            const res = await fetch(url, { cache: 'no-store' });
            if (!res.ok) throw new Error(await res.text() || 'External calls not found');
            return res.json();
        });
    },

    async getConnections(id) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(id) + '/connections', { cache: 'no-store' });
        if (!res.ok) return { available: false };
        return res.json();
    },

    /**
     * The JAR's detected database calls (JAR -> DB procedure/function/package).
     *
     * <p>Paged: pass `{limit, after, sort, q, types}`. The `X-Analyzer-Db-Calls-Available` response
     * header says whether the analysis recorded the facet at all — a JAR analysed before this
     * feature existed answers with an empty envelope and the header `false`, which the screen
     * renders as a "re-analysis needed" note rather than an empty list.
     *
     * @returns {Promise<{available:boolean, reason:(string|null), envelope:Object}|null>} null on a
     *          failed request (already reported through JA.apiFail).
     */
    async getDbCalls(id, opts) {
        const o = opts || {};
        const q = new URLSearchParams();
        if (o.limit != null) q.set('limit', o.limit);
        if (o.after) q.set('after', o.after);
        if (o.sort) q.set('sort', o.sort);
        if (o.q) q.set('q', o.q);
        if (o.types) q.set('types', o.types);
        const url = '/api/jar/jars/' + encodeURIComponent(id) + '/db-calls'
            + (q.toString() ? '?' + q.toString() : '');
        const res = await fetch(url, { cache: 'no-store', headers: { 'Accept': PA.PAGE_MEDIA_TYPE } });
        if (!res.ok) { JA.apiFail(res); return null; }
        const headerAvailable = res.headers.get('X-Analyzer-Db-Calls-Available') !== 'false';
        const body = await res.json();
        return {
            available: headerAvailable && body.available !== false,
            reason: body.reason || null,
            envelope: PA.normalizePage(body)
        };
    },

    /** Counts by kind / schema-package / procedure for the Database Calls tab. */
    async getDbCallsSummary(id) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(id) + '/db-calls/summary', { cache: 'no-store' });
        if (!res.ok) { JA.apiFail(res); return null; }
        return res.json();
    },

    async getDomainConfig(id) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(id) + '/domain-config', { cache: 'no-store' });
        if (!res.ok) { JA.apiFail(res); return null; }
        return res.json();
    },

    async getDynamicFlows(id, version) {
        return JA.api._dedupe('getDynamicFlows|' + id + '|' + (version || ''), async () => {
            let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/dynamic-flows';
            if (version) url += '?version=' + encodeURIComponent(version);
            const res = await fetch(url, { cache: 'no-store' });
            if (!res.ok) throw new Error(await res.text() || 'Dynamic flows not found');
            return res.json();
        });
    },

    async getAggregationFlows(id, version) {
        return JA.api._dedupe('getAggregationFlows|' + id + '|' + (version || ''), async () => {
            let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/aggregation-flows';
            if (version) url += '?version=' + encodeURIComponent(version);
            const res = await fetch(url, { cache: 'no-store' });
            if (!res.ok) throw new Error(await res.text() || 'Aggregation flows not found');
            return res.json();
        });
    },

    async getBeans(id, version) {
        return JA.api._dedupe('getBeans|' + id + '|' + (version || ''), async () => {
            let url = '/api/jar/jars/' + encodeURIComponent(id) + '/summary/beans';
            if (version) url += '?version=' + encodeURIComponent(version);
            const res = await fetch(url, { cache: 'no-store' });
            if (!res.ok) throw new Error(await res.text() || 'Beans not found');
            return res.json();
        });
    },

    /**
     * Unpaged class tree, used only as the fallback when the paged source is unavailable.
     *
     * `members=false` because this response is 11.8 MB with members and ~760 KB without, and the
     * tree paints class rows only — members are fetched per class on expand. Counts
     * (methodCount/fieldCount) are still returned, so rows show correct numbers.
     */
    async getClassTree(id, version) {
        return JA.api._dedupe('getClassTree|' + id + '|' + (version || ''), async () => {
            let url = '/api/jar/jars/' + encodeURIComponent(id) + '/classes/tree?members=false';
            if (version) url += '&version=' + encodeURIComponent(version);
            const res = await fetch(url, { cache: 'no-store' });
            if (!res.ok) throw new Error(await res.text() || 'Classes not found');
            return res.json();
        });
    },

    /**
     * Page source for the Code Structure tree (Q40 / L1).
     *
     * Server-ordered by source JAR, then package, then class name — the order the tree groups by,
     * so a page boundary lands between packages. `q` is sent with the page rather than filtered in
     * the browser, because with one page in memory a client-side filter would silently search that
     * page alone.
     */
    classTreePages(id, version, query, cf) {
        const params = { sort: 'PACKAGE' };
        if (version) params.version = version;
        if (query) params.q = query;
        if (cf) params.cf = cf;
        return PA.pageSource(
            '/api/jar/jars/' + encodeURIComponent(id) + '/classes/tree', params);
    },

    /**
     * UI-003 (§134) — one row per package (name, source JAR, class count), not one per class.
     * Lets the Code Structure tab's default (package) view render its own top level without
     * fetching every class up front; a package's own classes are then fetched only when it is
     * expanded, via `classTreePages`'s new `cf` filter.
     */
    async getClassPackages(id, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/classes/packages';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Package summary not found');
        return res.json();
    },

    /** Page source for the bundled-JAR list (Q41 / L2) — headers only, bodies on click. */
    bundledJarPages(id, query) {
        const params = { sort: 'NAME_ASC' };
        if (query) params.q = query;
        return PA.pageSource(
            '/api/jar/jars/' + encodeURIComponent(id) + '/bundled-jars', params);
    },

    /** One file out of one bundled JAR, addressed by name — never by array position. */
    async getBundledJarContent(id, jarName, fileName) {
        const url = '/api/jar/jars/' + encodeURIComponent(id) + '/bundled-jars/content'
            + '?jar=' + encodeURIComponent(jarName) + '&file=' + encodeURIComponent(fileName);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error('Not found: ' + jarName + '/' + fileName);
        return res.text();
    },

    async getClassByIndex(id, idx, version) {
        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/classes/by-index/' + idx;
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Class not found');
        return res.json();
    },

    /**
     * Q43 / L4 — the call paths that perform one operation on one collection.
     *
     * Replaces the Collection popup's unbounded `Promise.all` of getCallTree: 15 requests and
     * 114 411 B on the worst measured cell (ENTERPRISE.ORDERS / READ, web-1.0.0-SNAPSHOT), each
     * of which re-parsed the whole 3.9 MB analysis.json server-side because streamCallTree seeks
     * the endpoint array to one index. One request, one parse.
     *
     * `version` is passed. Every getCallTree call site omits it, so on a screen showing a
     * non-current version the index was resolved against the current document — see the
     * `sourceIdx` note in summary-popups.js.
     */
    async getCollectionCallPaths(id, collection, op, opts) {
        const o = opts || {};
        const q = new URLSearchParams({ collection: collection, op: op });
        if (o.version) q.set('version', o.version);
        if (o.limit != null) q.set('limit', o.limit);
        if (o.after) q.set('after', o.after);
        if (o.q) q.set('q', o.q);
        const url = '/api/jar/jars/' + encodeURIComponent(id) + '/collections/call-paths?' + q;
        const res = await fetch(url, {
            cache: 'no-store',
            headers: { 'Accept': 'application/vnd.analyzer.page+json' }
        });
        if (res.status === 204) return { items: [], page: { hasMore: false, total: 0 } };
        if (!res.ok) throw new Error(await res.text() || 'Collection call paths not found');
        return PA.normalizePage(await res.json());
    },

    /**
     * In-flight call-tree requests, keyed by exactly what identifies one: id + index + version.
     *
     * <p>A call tree for a large endpoint takes seconds to build, and nothing on screen said so.
     * A user clicking "Trace" and seeing no response clicked again — and because this request is
     * `cache: 'no-store'`, every click was a full server round trip. Four clicks meant four
     * concurrent builds of the same tree, each making the others slower. The caching in
     * `showTrace` could not help: it stores the result *after* the request returns, so it does
     * nothing while one is pending.
     *
     * <p>Keyed by the promise rather than the resolved value, which is the whole point. Storing
     * the value only dedupes calls that arrive after the first has finished — the case that was
     * never the problem.
     */
    _callTreeInFlight: {},

    async getCallTree(id, idx, version) {
        const key = id + '|' + idx + '|' + (version || '');
        const pending = JA.api._callTreeInFlight[key];
        if (pending) return pending;               // join the request already running

        let url = '/api/jar/jars/' + encodeURIComponent(id) + '/endpoints/by-index/' + idx + '/call-tree';
        if (version) url += '?version=' + encodeURIComponent(version);

        const p = (async () => {
            // Ask for the DAG. On endpoint 271 of this very jar the expanded tree is 300,167,238
            // nodes — 120 MB in 3.6 minutes with a cap that kept 0.07% of it — while the DAG is
            // 2,282 nodes and 7.2 MB in 0.44 seconds. JA.dag.inflate then produces the nested shape
            // this function has always returned, to a bounded depth.
            const dagRes = await fetch(url + (url.includes('?') ? '&' : '?') + 'format=dag',
                                       { cache: 'no-store' });
            if (dagRes.ok) {
                const payload = await dagRes.json();
                // A document written before the flat format answers null here; fall through to the
                // expanded endpoint rather than showing an empty tree.
                if (JA.dag && JA.dag.isDag(payload)) return JA.dag.inflate(payload);
            }
            // MID-016: the server's bare default is now `dag` too, so this fallback has to ask
            // for the expanded form explicitly. Before this it relied on the server's default
            // still being the expanded tree — an implicit contract that flipping the default
            // silently broke until this line existed, which is exactly the regression the
            // server-side comment on this endpoint records finding before it shipped.
            const res = await fetch(url + (url.includes('?') ? '&' : '?') + 'format=full',
                                    { cache: 'no-store' });
            if (!res.ok) throw new Error(await res.text() || 'Call tree not found');
            return res.json();
        })();

        JA.api._callTreeInFlight[key] = p;
        try {
            return await p;
        } finally {
            // Cleared on failure too: a failed attempt must not make every later click return the
            // same rejection forever.
            delete JA.api._callTreeInFlight[key];
        }
    },

    async deleteJar(id) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(id), { method: 'DELETE' });
        if (!res.ok) throw new Error('Delete failed');
        return res.json();
    },

    async getClaudeStatus() {
        try {
            const res = await fetch('/api/jar/jars/settings/claude-status');
            if (!res.ok) return { configured: false };
            return res.json();
        } catch (e) {
            return { configured: false };
        }
    },

    async getClaudeProgress(jarId) {
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-progress', { cache: 'no-store' });
            if (!res.ok) return { status: 'IDLE' };
            return res.json();
        } catch (e) {
            return { status: 'IDLE' };
        }
    },

    async claudeEnrichSingle(jarId, endpointName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-enrich-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpointName })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeRescan(jarId, resume) {
        const url = '/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-rescan'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async listSessions() {
        const res = await fetch('/api/jar/jars/sessions');
        if (!res.ok) throw new Error('Failed to list sessions');
        return res.json();
    },

    /**
     * Is any JAR-side Claude session running?
     *
     * Replaces `listSessions().some(s => s.status === 'RUNNING')`, which fetched the whole session
     * list — every row with its type, detail, timestamps and formatted duration — to read one bit,
     * every 30 s, on every open page. Scoped to JAR so the sidebar dot keeps its existing meaning.
     */
    async anySessionRunning() {
        const res = await fetch('/api/sessions/running?analyzer=JAR');
        if (!res.ok) throw new Error('Failed to check session state');
        return (await res.json()).running === true;
    },

    async killSession(sessionId) {
        const res = await fetch('/api/jar/jars/sessions/' + encodeURIComponent(sessionId) + '/kill', {
            method: 'POST'
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeCorrect(jarId, resume) {
        const url = '/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-correct'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeCorrectSingle(jarId, endpointName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-correct-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpointName })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getCorrections(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/corrections');
        if (!res.ok) throw new Error('Corrections not found');
        return res.json();
    },

    async getVersions(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/versions', { cache: 'no-store' });
        if (!res.ok) throw new Error('Failed to get version info');
        return res.json();
    },

    async revertClaude(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/revert-claude', { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeFullScan(jarId, resume) {
        const url = '/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-full-scan'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getCorrectionForEndpoint(jarId, endpoint) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/corrections/' + encodeURIComponent(endpoint));
        if (!res.ok) throw new Error('Correction not found');
        return res.json();
    },

    async getCorrectionLogs(jarId, endpointName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/correction-logs/' + encodeURIComponent(endpointName));
        if (!res.ok) throw new Error('Correction logs not found');
        return res.json();
    },

    async listAllCorrectionLogs(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/correction-logs');
        if (!res.ok) throw new Error('Failed to list correction logs');
        return res.json();
    },

    async getCorrectionLogFile(jarId, fileName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/correction-logs/file/' + encodeURIComponent(fileName));
        if (!res.ok) throw new Error('Log file not found');
        return res.text();
    },

    async listRunLogs(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/run-logs');
        if (!res.ok) throw new Error('Failed to list run logs');
        return res.json();
    },

    async getRunLog(jarId, logName) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/run-logs/' + encodeURIComponent(logName));
        if (!res.ok) throw new Error('Log not found');
        return res.text();
    },

    async getCatalog(jarId) {
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/catalog', { cache: 'no-store' });
            if (!res.ok) { JA.apiFail(res); return null; }
            return res.json();
        } catch (e) {
            return null;
        }
    },

    async killJarSessions(jarId) {
        const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/kill-sessions', { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getClaudeStats(jarName) {
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarName) + '/claude-stats', { cache: 'no-store' });
            if (!res.ok) { JA.apiFail(res); return null; }
            return res.json();
        } catch (e) {
            return null;
        }
    },

    async getClaudeSessionDetail(jarName, sessionId) {
        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarName) + '/claude-stats/' + encodeURIComponent(sessionId), { cache: 'no-store' });
            if (!res.ok) { JA.apiFail(res); return null; }
            return res.json();
        } catch (e) {
            return null;
        }
    }
};

// Wrap user-initiated API calls with activity tracker notifications.
// Background/polling calls are excluded.
(function() {
    const silent = new Set([
        'getClaudeStatus', 'getClaudeProgress', 'listSessions', 'anySessionRunning',
        'getCatalog', 'listRunLogs', 'listAllCorrectionLogs',
        'getClaudeStats', 'getClaudeSessionDetail',
        'getExternalCalls', 'getDynamicFlows', 'getAggregationFlows', 'getBeans',
        // TASK-REGISTRY §196 — these three are synchronous PA.pageSource FACTORIES, not async
        // data fetches: they must return the page-source object itself, immediately, so
        // PA.tf.init/_fetchPackageClasses can call .fetchPage() on it right away. JA.apiTracker
        // .track is declared `async`, so it unconditionally returns a Promise regardless of what
        // the wrapped function returns — wrapping these three silently replaced their real return
        // value with a pending Promise everywhere they're called, which has no __pageSource flag
        // and no .fetchPage of its own. That produced two different-looking failures depending on
        // which guard a caller happened to have: "source.fetchPage is not a function"
        // (codeTree.js's per-package expand, which checks typeof before calling) and
        // "rows is not iterable" (PA.tf.init, which assumes PA.isPageSource's answer is reliable
        // and never expected the object itself to be a Promise) — both traced back to this one
        // wrapper, not to the page-source objects or the endpoints they call at all.
        'endpointListPages', 'classTreePages', 'bundledJarPages'
    ]);
    const labels = {
        listJars: 'Loading JARs',
        getSummary: 'Loading Summary',
        getSummaryHeaders: 'Loading Summary',
        getSummaryHeader: 'Loading Header',
        getSummaryEndpoints: 'Loading Endpoints',
        getClassTree: 'Loading Code Structure',
        getClassByIndex: 'Loading Class',
        getCallTree: 'Loading Call Tree',
        getAnalysis: 'Loading Full Analysis',
        uploadJar: 'Uploading JAR',
        deleteJar: 'Deleting JAR',
        claudeEnrichSingle: 'Claude Enrich',
        claudeRescan: 'Claude Rescan',
        claudeCorrect: 'Claude Correct',
        claudeCorrectSingle: 'Claude Correct',
        claudeFullScan: 'Claude Full Scan',
        getCorrections: 'Loading Corrections',
        getCorrectionForEndpoint: 'Loading Correction',
        getCorrectionLogs: 'Loading Logs',
        getCorrectionLogFile: 'Loading Log',
        getVersions: 'Loading Versions',
        revertClaude: 'Reverting Claude',
        getRunLog: 'Loading Log',
        killSession: 'Killing Session',
        killJarSessions: 'Killing Sessions'
    };
    for (const key of Object.keys(JA.api)) {
        if (typeof JA.api[key] !== 'function' || silent.has(key)) continue;
        const orig = JA.api[key];
        JA.api[key] = function() {
            const label = labels[key] || key;
            if (!JA.apiTracker) return orig.apply(JA.api, arguments);
            return JA.apiTracker.track(label, () => orig.apply(JA.api, arguments));
        };
    }
})();
