/**
 * REST API client for WAR Analyzer — all fetch calls point to /api/war/wars.
 * Overrides JA.api set by the jar api.js loaded before this file.
 */
window.JA = window.JA || {};

// The JAR api.js loads first and defines JA.api; this file replaced it wholesale, which silently
// dropped the nine methods it does not redefine. Six of those are called by shared scripts the WAR
// page loads (codeTree, summary, summary-popups), so those features died with a TypeError and no
// build failure — a page-specific breakage nobody sees until they open that tab.
//
// Captured here and merged back below, so WAR's own endpoints still win while jar-only methods
// remain callable. Where such a method has no WAR endpoint the request 404s, which the existing
// `if (!res.ok) throw` paths already surface as an error state — a handled failure rather than an
// unhandled one.
const _jarApi = JA.api || {};

/**
 * Real bug, found live via repeated 404s (`/api/jar/jars/{warId}/...` — the WRONG prefix): a
 * handful of jar/js/api.js methods are relied on by shared code (`codeTree.js`'s `classTreePages`,
 * `endpointList.js`'s `endpointListPages`, `getClassPackages`) with NO WAR endpoint at all, or a
 * differently-shaped one. Merely inheriting them via `inheritUnoverriddenJarMethods()` below is
 * not enough — `typeof JA.api.classTreePages === 'function'` (the guard `codeTree.js` uses to
 * decide whether the fast path is available) is true for the INHERITED, wrong-prefixed version
 * too, so the guard passes and the wrong URL still gets called. Each of these needs a REAL WAR
 * implementation, not just to exist.
 *
 * WAR analyses are small (tens of endpoints/classes, not thousands), so real server-side cursor
 * paging is unnecessary — every one of these fetches the already-available flat data once
 * (`getSummary`/`getClassTree`) and paginates/filters/groups it in the browser, wrapped in an
 * object matching `PA.pageSource`'s exact contract (`shared/page-envelope.js`): `fetchPage(limit)`
 * returns `{items, page}`, and `setParam`/`param`/`reset` are what `PA.tf.filter`'s search box
 * actually calls (NOT a query argument to `fetchPage` — confirmed by reading `tableFramework.js`
 * before assuming the shape).
 */
function _localPageSource(loadAll, filterFn) {
    let cachedAll = null;
    let cursor = 0;
    const query = {};
    const load = async () => {
        if (!cachedAll) cachedAll = await loadAll();
        return cachedAll;
    };
    return {
        __pageSource: true,
        fetchPage(limit) {
            return load().then(all => {
                const filtered = filterFn ? filterFn(all, query) : all;
                const items = filtered.slice(cursor, cursor + limit);
                cursor += items.length;
                const hasMore = cursor < filtered.length;
                return {
                    items,
                    page: {
                        nextCursor: hasMore ? String(cursor) : null,
                        hasMore, total: filtered.length, totalIsExact: true,
                        limit, sort: null
                    }
                };
            });
        },
        setParam(key, value) {
            const next = (value === null || value === undefined) ? '' : String(value);
            const same = String(query[key] === undefined ? '' : query[key]) === next;
            if (same) return false;
            query[key] = next;
            cursor = 0;
            return true;
        },
        param(key) { return query[key]; },
        reset() { cursor = 0; }
    };
}

JA.api = {

    endpointListPages(id, version) {
        return _localPageSource(
            async () => {
                const analysis = await JA.api.getSummary(id, version);
                return (analysis.endpoints || []).map((ep, i) => Object.assign({}, ep, { endpointIndex: i }));
            },
            (all, query) => {
                const q = (query.q || '').trim().toLowerCase();
                if (!q) return all;
                return all.filter(ep =>
                    (ep.fullPath || '').toLowerCase().indexOf(q) >= 0
                    || (ep.methodName || '').toLowerCase().indexOf(q) >= 0
                    || (ep.controllerClass || '').toLowerCase().indexOf(q) >= 0
                    || (ep.controllerSimpleName || '').toLowerCase().indexOf(q) >= 0);
            });
    },

    /**
     * Mirrors `jar/js/codeTree.js`'s two call shapes: `(id, version, query, cf)` for the top-level
     * tree and package-expand fetches, and `(id, version, className)` for the class-name lookup
     * (search's `q` positional arg reused as a name filter there — see that call site).
     */
    classTreePages(id, version, query, cf) {
        return _localPageSource(
            () => JA.api.getClassTree(id, version),
            (all, q) => {
                let filtered = all;
                if (query) {
                    const lf = String(query).toLowerCase();
                    filtered = filtered.filter(c =>
                        (c.simpleName || '').toLowerCase().indexOf(lf) >= 0
                        || (c.fullyQualifiedName || '').toLowerCase().indexOf(lf) >= 0);
                }
                if (Array.isArray(cf)) {
                    for (const entry of cf) {
                        const sep = entry.indexOf(':');
                        if (sep < 0) continue;
                        const col = entry.slice(0, sep), val = entry.slice(sep + 1);
                        if (col === '0') filtered = filtered.filter(c => c.packageName === val);
                        if (col === '1') filtered = filtered.filter(c =>
                            (c.sourceJar || JA.api.MAIN_JAR_SENTINEL) === val);
                    }
                }
                const q2 = (q.q || '').trim().toLowerCase();
                if (q2) filtered = filtered.filter(c =>
                    (c.simpleName || '').toLowerCase().indexOf(q2) >= 0
                    || (c.fullyQualifiedName || '').toLowerCase().indexOf(q2) >= 0);
                return filtered;
            });
    },

    /** Client-side equivalent of `AnalyzerController#getClassPackages` — same output shape
     *  (`{packageName, sourceJar, classCount}[]`), grouped from the already-fetched flat list. */
    async getClassPackages(id, version) {
        const all = await JA.api.getClassTree(id, version);
        const counts = new Map();
        for (const c of all) {
            const pkg = c.packageName || '';
            const jar = c.sourceJar || '';
            const key = jar + ' ' + pkg;
            const existing = counts.get(key);
            if (existing) existing.classCount++;
            else counts.set(key, { packageName: pkg, sourceJar: jar, classCount: 1 });
        }
        return Array.from(counts.values());
    },

    /** `/summary/header` (singular) has no WAR route — reuse the already-correct plural
     *  `getSummaryHeaders`, whose fields are a superset of what the header bar needs. */
    async getSummaryHeader(id, version) {
        return JA.api.getSummaryHeaders(id, version);
    },

    /** Real WAR route now exists (`WarAnalyzerController#getConnections`) — the data was already
     *  being stored on every upload, just never readable. */
    async getConnections(id) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(id) + '/connections', { cache: 'no-store' });
        if (!res.ok) return { available: false };
        return res.json();
    },

    /** Real WAR route now exists (`WarAnalyzerController#getDomainConfig`). */
    async getDomainConfig(id) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(id) + '/domain-config', { cache: 'no-store' });
        if (!res.ok) { JA.apiFail(res); return null; }
        return res.json();
    },

    /**
     * Real bug, found live: `codeTree.js`'s `_loadBundledJars` checks `!PA.pageSource` to decide
     * whether to use the simple GET fallback — but `PA.pageSource` always exists (it's defined
     * unconditionally by `page-envelope.js`), so it ALWAYS took the paged branch and called
     * `bundledJarPages` regardless. WAR only has a real, non-paginated `/bundled-jars` route (see
     * `WarAnalyzerController` — deliberately not the paged `AnalysisQuery`-based one, same
     * reasoning as `endpointListPages`/`classTreePages` above), so this wraps that single GET in
     * the same `_localPageSource` shape the other two use.
     */
    bundledJarPages(id, query) {
        return _localPageSource(
            async () => {
                const res = await fetch('/api/war/wars/' + encodeURIComponent(id) + '/bundled-jars', { cache: 'no-store' });
                if (!res.ok) { JA.apiFail(res); return []; }
                return res.json();
            },
            (all, q) => {
                const query2 = (q.q || query || '').trim().toLowerCase();
                if (!query2) return all;
                return all.filter(j => (j.name || '').toLowerCase().indexOf(query2) >= 0);
            });
    },

    /** Port of the JAR version — same URL shape, WAR prefix. */
    async getBundledJarContent(id, jarName, fileName) {
        const url = '/api/war/wars/' + encodeURIComponent(id) + '/bundled-jars/content'
            + '?jar=' + encodeURIComponent(jarName) + '&file=' + encodeURIComponent(fileName);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error('Not found: ' + jarName + '/' + fileName);
        return res.text();
    },

    async listJars() {
        const res = await fetch('/api/war/wars', { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Failed to list WARs');
        return res.json();
    },

    async uploadJar(file, mode, projectPath, renameSuffix, basePackage) {
        const form = new FormData();
        form.append('file', file);
        let url = '/api/war/wars?mode=' + encodeURIComponent(mode || 'static');
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
        let url = '/api/war/wars/' + encodeURIComponent(id);
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Analysis not found');
        return res.json();
    },

    async getSummary(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Summary not found');
        return res.json();
    },

    async getSummaryHeaders(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/headers';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Summary headers not found');
        return res.json();
    },

    async getExternalCalls(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/external-calls';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'External calls not found');
        return res.json();
    },

    async getDynamicFlows(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/dynamic-flows';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Dynamic flows not found');
        return res.json();
    },

    async getAggregationFlows(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/aggregation-flows';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Aggregation flows not found');
        return res.json();
    },

    async getBeans(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/summary/beans';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Beans not found');
        return res.json();
    },

    async getClassTree(id, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/classes/tree';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Classes not found');
        return res.json();
    },

    async getClassByIndex(id, idx, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/classes/by-index/' + idx;
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Class not found');
        return res.json();
    },

    async getCallTree(id, idx, version) {
        let url = '/api/war/wars/' + encodeURIComponent(id) + '/endpoints/by-index/' + idx + '/call-tree';
        if (version) url += '?version=' + encodeURIComponent(version);
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(await res.text() || 'Call tree not found');
        return res.json();
    },

    async deleteJar(id) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(id), { method: 'DELETE' });
        if (!res.ok) throw new Error('Delete failed');
        return res.json();
    },

    async getClaudeStatus() {
        return { configured: false };
    },

    async getClaudeProgress(warId) {
        try {
            const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/claude-progress', { cache: 'no-store' });
            if (!res.ok) return { status: 'IDLE' };
            return res.json();
        } catch (e) {
            return { status: 'IDLE' };
        }
    },

    async claudeEnrichSingle(warId, endpointName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/claude-enrich-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpointName })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeRescan(warId, resume) {
        const url = '/api/war/wars/' + encodeURIComponent(warId) + '/claude-rescan'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async listSessions() {
        const res = await fetch('/api/war/wars/sessions');
        if (!res.ok) throw new Error('Failed to list sessions');
        return res.json();
    },

    /**
     * Is any Claude session running?
     *
     * Present here as well as in jar/js/api.js because this file replaces `JA.api` wholesale, and
     * the WAR page loads jar/js/app.js — whose sidebar-dot poll calls this. Scoped to JAR because
     * WAR and JAR share one ClaudeSessionManager: /api/war/wars/sessions and
     * /api/jar/jars/sessions are two routes onto the same session map.
     */
    async anySessionRunning() {
        const res = await fetch('/api/sessions/running?analyzer=JAR');
        if (!res.ok) throw new Error('Failed to check session state');
        return (await res.json()).running === true;
    },

    async killSession(sessionId) {
        const res = await fetch('/api/war/wars/sessions/' + encodeURIComponent(sessionId) + '/kill', {
            method: 'POST'
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeCorrect(warId, resume) {
        const url = '/api/war/wars/' + encodeURIComponent(warId) + '/claude-correct'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeCorrectSingle(warId, endpointName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/claude-correct-single', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpointName })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getCorrections(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/corrections');
        if (!res.ok) throw new Error('Corrections not found');
        return res.json();
    },

    async getVersions(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/versions', { cache: 'no-store' });
        if (!res.ok) throw new Error('Failed to get version info');
        return res.json();
    },

    async revertClaude(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/revert-claude', { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async claudeFullScan(warId, resume) {
        const url = '/api/war/wars/' + encodeURIComponent(warId) + '/claude-full-scan'
            + (resume !== undefined ? '?resume=' + resume : '');
        const res = await fetch(url, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getCorrectionForEndpoint(warId, endpoint) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/corrections/' + encodeURIComponent(endpoint));
        if (!res.ok) throw new Error('Correction not found');
        return res.json();
    },

    async getCorrectionLogs(warId, endpointName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/correction-logs/' + encodeURIComponent(endpointName));
        if (!res.ok) throw new Error('Correction logs not found');
        return res.json();
    },

    async listAllCorrectionLogs(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/correction-logs');
        if (!res.ok) throw new Error('Failed to list correction logs');
        return res.json();
    },

    async getCorrectionLogFile(warId, fileName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/correction-logs/file/' + encodeURIComponent(fileName));
        if (!res.ok) throw new Error('Log file not found');
        return res.text();
    },

    async listRunLogs(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/run-logs');
        if (!res.ok) throw new Error('Failed to list run logs');
        return res.json();
    },

    async getRunLog(warId, logName) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/run-logs/' + encodeURIComponent(logName));
        if (!res.ok) throw new Error('Log not found');
        return res.text();
    },

    async getCatalog(warId) {
        try {
            const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/catalog', { cache: 'no-store' });
            if (!res.ok) { JA.apiFail(res); return null; }
            return res.json();
        } catch (e) {
            return null;
        }
    },

    async killJarSessions(warId) {
        const res = await fetch('/api/war/wars/' + encodeURIComponent(warId) + '/kill-sessions', { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
    },

    async getClaudeStats(warName) {
        try {
            const res = await fetch('/api/war/wars/' + encodeURIComponent(warName) + '/claude-stats', { cache: 'no-store' });
            if (!res.ok) { JA.apiFail(res); return null; }
            return res.json();
        } catch (e) {
            return null;
        }
    },

    async getClaudeSessionDetail(warName, sessionId) {
        try {
            const res = await fetch('/api/war/wars/' + encodeURIComponent(warName) + '/claude-stats/' + encodeURIComponent(sessionId), { cache: 'no-store' });
            if (!res.ok) { JA.apiFail(res); return null; }
            return res.json();
        } catch (e) {
            return null;
        }
    }
};

// Wrap user-initiated API calls with activity tracker notifications.
(function() {
    const silent = new Set([
        'getClaudeStatus', 'getClaudeProgress', 'listSessions', 'anySessionRunning',
        'getCatalog', 'listRunLogs', 'listAllCorrectionLogs',
        'getClaudeStats', 'getClaudeSessionDetail',
        'getExternalCalls', 'getDynamicFlows', 'getAggregationFlows', 'getBeans',
        // Real bug, found live via Playwright ("rows is not iterable" in PA.tf.init): these three
        // are synchronous PA.pageSource-shaped FACTORIES (see `_localPageSource` above), not async
        // data fetches — they must return the page-source object itself, immediately. This
        // tracker wraps every method in `JA.apiTracker.track`, which is declared `async` and so
        // unconditionally returns a Promise regardless of what's wrapped — silently replacing the
        // real return value with a pending Promise (no `__pageSource` flag, no `.fetchPage`)
        // everywhere these are called. jar/js/api.js's OWN identical tracker already excludes
        // these same three for this exact reason (see its own, longer comment) — this file has a
        // separate tracker block and needed the same exclusion, which it never had.
        'endpointListPages', 'classTreePages', 'bundledJarPages'
    ]);
    const labels = {
        listJars: 'Loading WARs',
        getSummary: 'Loading Summary',
        getSummaryHeaders: 'Loading Summary',
        getClassTree: 'Loading Code Structure',
        getClassByIndex: 'Loading Class',
        getCallTree: 'Loading Call Tree',
        getAnalysis: 'Loading Full Analysis',
        uploadJar: 'Uploading WAR',
        deleteJar: 'Deleting WAR',
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

// Fill in anything the JAR API defined that WAR does not redefine. Applied AFTER the tracker above
// so WAR's own methods keep their instrumentation, and ordered so a WAR override is never replaced
// by the jar version.
(function inheritUnoverriddenJarMethods() {
    Object.keys(_jarApi).forEach(function (name) {
        if (!(name in JA.api)) JA.api[name] = _jarApi[name];
    });
})();
