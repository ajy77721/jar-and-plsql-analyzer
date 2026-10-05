/*
 * Page envelope compatibility layer.
 *
 * The application has ~22 tables that today fetch a whole array and slice it in the browser. They
 * are being converted to server-side cursor paging one endpoint at a time, which means at any point
 * some endpoints answer with a bare array and others with a {items, page} envelope.
 *
 * This file is loaded on every page, before anything else, so that no screen has to know which
 * shape it is about to receive. Without it, converting one endpoint would break every other screen
 * that happens to call it.
 *
 * Ships in step 0 of the migration and is safe in both directions: an old screen reading a new
 * endpoint, and a new screen reading an endpoint not yet converted.
 */
(function () {
    'use strict';

    var PA = window.PA = window.PA || {};

    /** Media type that opts a caller into the envelope without sending paging parameters. */
    PA.PAGE_MEDIA_TYPE = 'application/vnd.analyzer.page+json';

    /**
     * The one place a dialect is named: {@code Object.keys(PA._DS_LABEL)} is the datasource list
     * every dropdown and filter derives from, so a sixth dialect reaches every screen at once.
     * Hardcoding the list in a screen is the defect this prevents — a PostgreSQL analysis once
     * showed in the history table but was missing from the filter beside it. Defined here, in the
     * file every page loads first, so a page that never loads the parser's history.js (the plsql
     * setup modal) still has it.
     */
    PA._DS_LABEL = { ORACLE: 'Oracle', SQLSERVER: 'SQL Server', MYSQL: 'MySQL', DB2: 'Db2', POSTGRES: 'PostgreSQL' };

    /**
     * Normalises either response shape into an envelope.
     *
     * A bare array is reported as a single complete page, so calling code can always read
     * `env.items` and `env.page.hasMore` regardless of which side of the migration the endpoint is
     * on. `total` is only claimed as exact when it genuinely is.
     */
    PA.normalizePage = function (body) {
        if (Array.isArray(body)) {
            return {
                items: body,
                page: {
                    nextCursor: null, hasMore: false,
                    total: body.length, totalIsExact: true,
                    limit: body.length, sort: null
                }
            };
        }
        if (body && Array.isArray(body.items) && body.page) {
            return body;
        }
        // An object that is neither shape: most likely a single resource or an error body. Wrap it
        // rather than throwing, so one unexpected payload cannot take a screen down.
        return {
            items: (body === null || body === undefined) ? [] : [body],
            page: {
                nextCursor: null, hasMore: false,
                total: (body === null || body === undefined) ? 0 : 1,
                totalIsExact: true, limit: 1, sort: null
            }
        };
    };

    /**
     * An async page source for the two client pagers.
     *
     * Passing one of these instead of an array is the single change each pager needs; every table
     * using that pager is then converted by changing its call site alone, with no change to
     * rendering code.
     *
     * Holds the cursor internally and never parses it — the cursor is opaque by contract, and a
     * client that constructs one would break the moment the server's ordering changed.
     */
    /**
     * @param {string} url     the paged endpoint
     * @param {Object} params  parameters that travel with every page request
     * @param {Function} [mapRow]  optional per-row adapter, applied to each item as it arrives
     *
     * <p>The mapper exists because an endpoint may serve the artifact's own field names while the
     * grid was written against a reader's projection of them. Converting the grid instead would
     * mean renaming fields in the row renderer, the sort keys, the column filters and the detail
     * panel at once — a change with no way to verify it a piece at a time. Mapping one page of
     * fifty rows is the cheaper and more reversible half.
     */
    PA.pageSource = function (url, params, mapRow) {
        var cursor = null;
        var exhausted = false;
        var query = {};
        if (params) {
            Object.keys(params).forEach(function (k) { query[k] = params[k]; });
        }

        var EMPTY_PAGE = function (limit, sort) {
            return { nextCursor: null, hasMore: false, total: 0, totalIsExact: true,
                     limit: limit, sort: sort || null };
        };

        return {
            __pageSource: true,

            /** Fetches the next page. Returns `{items, page}`; `items` is empty once exhausted. */
            fetchPage: function (limit) {
                if (exhausted) {
                    return Promise.resolve({ items: [], page: EMPTY_PAGE(limit, query.sort) });
                }
                var q = new URLSearchParams();
                Object.keys(query).forEach(function (k) {
                    var v = query[k];
                    // An absent parameter and an empty one are different requests: `q=` would be a
                    // search for the empty string. Only send what the caller actually set.
                    if (v === null || v === undefined || v === '') return;
                    // An array becomes repeated parameters, which is how a multi-valued filter
                    // (one entry per column) stays unambiguous without inventing a separator that
                    // some value will eventually contain.
                    if (Array.isArray(v)) v.forEach(function (item) { q.append(k, item); });
                    else q.set(k, v);
                });
                q.set('limit', limit);
                if (cursor) q.set('after', cursor);

                return fetch(url + (url.indexOf('?') >= 0 ? '&' : '?') + q.toString(),
                             { headers: { 'Accept': PA.PAGE_MEDIA_TYPE } })
                    .then(function (res) {
                        if (res.status === 204) {
                            // "No data" is its own answer, not an empty array that might mean the
                            // filter matched nothing.
                            return null;
                        }
                        if (res.status === 409) {
                            // The result set was replaced mid-scroll by a re-analysis. Offset paging
                            // would have silently returned rows from the superseded run.
                            var err = new Error('RESULT_SET_REPLACED');
                            err.resultSetReplaced = true;
                            throw err;
                        }
                        if (!res.ok) throw new Error('page fetch ' + res.status);
                        return res.json();
                    })
                    .then(function (body) {
                        if (body === null) {
                            exhausted = true;
                            cursor = null;
                            return { items: [], page: EMPTY_PAGE(limit, query.sort) };
                        }
                        var env = PA.normalizePage(body);
                        cursor = env.page.nextCursor;
                        exhausted = !env.page.hasMore;
                        // Applied AFTER the cursor is read: the cursor belongs to the envelope,
                        // not to the rows, and a mapper that dropped or reordered items must not
                        // be able to affect where the next page resumes from.
                        if (typeof mapRow === 'function' && env.items) {
                            env.items = env.items.map(mapRow);
                        }
                        return env;
                    });
            },

            /**
             * Sets (or clears, with null/'') a request parameter and restarts from the first page.
             *
             * Search and sort must travel with pagination: with one page in memory a client-side
             * search would silently search that page alone. Changing either invalidates the cursor,
             * because a cursor is only meaningful within one ordering and one filter — hence the
             * implicit reset rather than leaving it to each caller to remember.
             */
            setParam: function (key, value) {
                var isArray = Array.isArray(value);
                var next = isArray ? value.slice()
                         : ((value === null || value === undefined) ? '' : String(value));
                var prev = query[key];
                var same = isArray
                    ? (Array.isArray(prev) && prev.length === next.length
                        && prev.every(function (v, i) { return v === next[i]; }))
                    : String(prev === undefined ? '' : prev) === next;
                if (same) return false;
                query[key] = next;
                cursor = null;
                exhausted = false;
                return true;
            },

            /** The parameters currently in force, for a caller that needs to echo them. */
            param: function (key) { return query[key]; },

            /** Restarts from the first page — after a sort change, a filter change, or a 409. */
            reset: function () {
                cursor = null;
                exhausted = false;
            },

            /** True once the server has said there is nothing further. */
            isExhausted: function () { return exhausted; }
        };
    };

    /** True when a pager was handed a page source rather than a materialised array. */
    PA.isPageSource = function (x) {
        return !!(x && x.__pageSource);
    };
})();

/* ─────────────────────────────────────────────────────────────────────────────
 * Failed requests must not look like empty results.
 *
 * The browser API layers answer `return []` on any non-OK response, so a 500 and "this table has
 * no writers" arrive as the same value. Screens then render their empty state — "No tables found",
 * "Called By: None", "No parse errors" — and the user believes a statement about their code that
 * was really a failed request. On a tool whose entire job is reporting what code touches, that is
 * the worst defect available.
 *
 * FAILED is the fix, and its shape is the point:
 *   - it IS an array, and it is empty, so every existing render path keeps working untouched;
 *   - it is a single frozen instance, so `PA.failed(x)` is an identity test, not a guess;
 *   - returning it raises a banner, so the failure is visible immediately at every call site,
 *     before any screen has been individually taught about it.
 *
 * That ordering matters: one mechanical edit per call site makes ~72 silent failures loud, and
 * screens can then improve their empty text one line at a time instead of all at once.
 * ──────────────────────────────────────────────────────────────────────────── */
(function () {
    var PAsafe = (window.PA = window.PA || {});
    var JAsafe = (window.JA = window.JA || {});

    /** The sentinel. Frozen so a caller cannot push into it and lose its identity. */
    var FAILED = Object.freeze([]);

    /** True when this value is the failure sentinel rather than a genuine empty result. */
    function failed(v) { return v === FAILED; }

    var _lastBannerAt = 0;
    var _seen = {};

    /**
     * Records a failed request, shows a throttled banner, and returns the sentinel.
     *
     * <p>Throttled because a screen that fans out will fail many times at once, and twenty stacked
     * banners tell the user less than one. The URL is kept so the message names what broke.
     */
    function apiFail(res, url) {
        var status = res && res.status ? res.status : 0;
        // Taken from the Response itself. Call sites do not pass it: a first attempt threaded a
        // `url` variable through and 76 of 80 sites had no such variable in scope, which would have
        // thrown ReferenceError on nearly every request in the app. res.url is always present.
        if (!url) url = (res && res.url) ? res.url : '';
        // An expired AWS session is not a panel failure: route it to sign-in and keep the red
        // "a request failed" banner out of the way. That banner tells the user their data failed;
        // the truth is they need to authenticate again.
        if (handleUnauthorized(res)) {
            if (window.console) console.warn('[api] unauthorized — routing to sign-in', status, url);
            return FAILED;
        }
        var key = status + ' ' + (url || '');
        _seen[key] = (_seen[key] || 0) + 1;

        var now = Date.now();
        if (now - _lastBannerAt > 4000) {
            _lastBannerAt = now;
            _banner(status, url);
        }
        if (window.console) console.warn('[api] request failed', status, url);
        return FAILED;
    }

    /**
     * True when a response is the AWS SSO gate rather than some other auth scheme.
     *
     * The gate answers {@code 401 {ok:false,error:"Authenticate via AWS SSO first"}} on any
     * non-public {@code /api/**} path. The admin portal and the login endpoints have their own
     * auth, so their 401s are not ours to act on.
     */
    function isSsoGate401(res) {
        if (!res || res.status !== 401) return false;
        var path = '';
        try { path = new URL((res && res.url) || '', window.location.origin).pathname; }
        catch (e) { path = (res && res.url) || ''; }
        if (path.indexOf('/api/') !== 0) return false;
        if (path.indexOf('/api/admin/') === 0) return false;   // admin portal's own 401
        if (path.indexOf('/api/auth/') === 0) return false;    // login endpoints are public
        return true;
    }

    var _unauthHandledAt = 0;

    /**
     * Route an expired AWS session to re-authentication, once.
     *
     * On the home page — which owns the sign-in modal — the page registers {@code PA._onUnauthorized}
     * and that runs the resume-or-prompt flow in place. On a tool page there is no modal, so the
     * user is sent to the one sign-in surface (the home page) carrying where to return to.
     *
     * A fan-out of parallel 401s produces a single prompt: a second call inside the debounce window
     * is a no-op, so ten failing panels cannot stack ten logins.
     *
     * @returns true when the response was an SSO-gate 401 and has been routed
     */
    function handleUnauthorized(res) {
        if (!isSsoGate401(res)) return false;
        var now = Date.now();
        if (now - _unauthHandledAt < 1500) return true;
        _unauthHandledAt = now;
        if (typeof PAsafe._onUnauthorized === 'function') {
            try { PAsafe._onUnauthorized(res); }
            catch (e) { /* the page handler must not break the caller */ }
            return true;
        }
        var next = window.location.pathname + window.location.search;
        window.location.href = '/?login=1&next=' + encodeURIComponent(next);
        return true;
    }

    function _banner(status, url) {
        var id = 'api-fail-banner';
        var el = document.getElementById(id);
        if (!el) {
            el = document.createElement('div');
            el.id = id;
            el.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:99999;'
                + 'padding:7px 14px;font-size:12px;font-family:inherit;'
                + 'background:#7f1d1d;color:#fff;box-shadow:0 1px 6px rgba(0,0,0,.35);'
                + 'display:flex;align-items:center;gap:10px';
            document.body.appendChild(el);
        }
        var what = status === 0 ? 'could not be reached'
                 : status === 404 ? 'returned 404 (nothing loaded)'
                 : 'failed with HTTP ' + status;
        el.innerHTML = '<span><strong>A request ' + what + '.</strong> '
            + 'Panels below may look empty — that is this error, not your data.</span>'
            + '<span style="opacity:.75;font-size:11px;flex:1;overflow:hidden;'
            + 'text-overflow:ellipsis;white-space:nowrap">' + String(url || '') + '</span>'
            + '<button style="background:transparent;border:1px solid rgba(255,255,255,.5);'
            + 'color:#fff;border-radius:3px;cursor:pointer;font-size:11px;padding:1px 7px"'
            + ' onclick="this.parentNode.remove()">Dismiss</button>';
        el.style.display = 'flex';
    }

    PAsafe.FAILED = FAILED;
    PAsafe.failed = failed;
    PAsafe.apiFail = apiFail;
    PAsafe.handleUnauthorized = handleUnauthorized;
    // Both namespaces: the jar and war pages use JA, the rest use PA, and a shared helper that
    // exists under only one name is a helper half the app cannot call.
    JAsafe.FAILED = FAILED;
    JAsafe.failed = failed;
    JAsafe.apiFail = apiFail;
    JAsafe.handleUnauthorized = handleUnauthorized;
})();

/* ─────────────────────────────────────────────────────────────────────────────
 * Mounting a top-level overlay.
 *
 * Overlays were appended with `document.body.insertAdjacentHTML('beforeend', html)`, and the markup
 * declares a FIXED id. Clicking a button three times therefore produced three overlays stacked on
 * one another, all sharing one id — and the close button
 * (`document.getElementById('sum-trace-overlay').remove()`) removes only the FIRST match, so
 * closing took away the one underneath while the visible one stayed. The screen looked stuck.
 *
 * Removing the previous instance first makes re-opening idempotent: one overlay, one id, close
 * works. An id is required precisely because that is what makes "the same overlay" identifiable —
 * markup without one is appended as-is rather than silently guessed at.
 * ──────────────────────────────────────────────────────────────────────────── */
(function () {
    function mountOverlay(html, opts) {
        var m = /\bid="([^"]+)"/.exec(String(html));
        if (m) {
            // Every instance, not just the first: if duplicates already accumulated before this
            // helper existed, opening again should clear the whole pile rather than add to it.
            var existing = document.querySelectorAll('[id="' + m[1] + '"]');
            for (var i = 0; i < existing.length; i++) existing[i].remove();
        }
        document.body.insertAdjacentHTML('beforeend', html);
        return m ? document.getElementById(m[1]) : null;
    }

    (window.PA = window.PA || {}).mountOverlay = mountOverlay;
    (window.JA = window.JA || {}).mountOverlay = mountOverlay;
})();

/* ─────────────────────────────────────────────────────────────────────────────
 * Client-side call-tree inflation.
 *
 * The server can return a call tree in two shapes. The expanded nested tree is what the API always
 * returned, and it is exponential in shared subtrees: on endpoint 271 of unified-web it is
 * 300,167,238 nodes, which produced 120 MB in 3.6 minutes with a 200,000-node cap — 0.07% of the
 * answer, silently truncated. The DAG for the same endpoint is 2,282 nodes and 7.2 MB, delivered in
 * 0.44 seconds, because it is proportional to distinct methods rather than paths through them.
 *
 * This inflates the DAG to the nested shape every caller already expects, but only to a bounded
 * depth. Nodes at the frontier carry `_dagChildIds` and `_dagPending`; a renderer expanding one
 * calls `inflateChildren` to materialise the next level from the dictionary it already holds.
 *
 * The exponential blow-up is therefore never paid: it only ever existed because the whole tree was
 * materialised at once, and nobody can look at 300 million nodes.
 * ──────────────────────────────────────────────────────────────────────────── */
(function () {
    /** Levels materialised up front. Matches the trace renderers' own initial depth. */
    var INITIAL_DEPTH = 3;

    function isDag(payload) {
        return !!(payload && payload.format === 'dag-v1' && payload.nodes);
    }

    /**
     * Turns a DAG payload into the nested node the renderers expect.
     *
     * @param payload {format, rootId, nodeCount, nodes}
     * @param depth   levels to materialise now; deeper nodes are left pending
     */
    function inflate(payload, depth) {
        if (!isDag(payload)) return payload;          // already a tree, or nothing
        var dict = payload.nodes;
        var max = typeof depth === 'number' ? depth : INITIAL_DEPTH;

        function build(id, level) {
            var rec = dict[id];
            if (!rec) return null;
            // Copy, because the same record is shared by every path that reaches it — mutating it
            // would corrupt every other appearance. This copy is why sharing is safe.
            var node = {};
            for (var k in rec) if (k !== 'childIds') node[k] = rec[k];
            var kids = rec.childIds || [];
            if (kids.length === 0) { node.children = []; return node; }
            if (level >= max) {
                // Frontier: remember what is below without building it.
                node.children = [];
                node._dagChildIds = kids;
                node._dagPending = kids.length;
                return node;
            }
            node.children = [];
            for (var i = 0; i < kids.length; i++) {
                var c = build(kids[i], level + 1);
                if (c) node.children.push(c);
            }
            return node;
        }

        var root = build(payload.rootId, 0);
        if (root) {
            root._dag = dict;                          // kept for later expansion
            root._dagNodeCount = payload.nodeCount;
        }
        return root;
    }

    /**
     * Materialises one pending node's children from the dictionary.
     *
     * @returns true if anything was built, so a caller can tell "expanded" from "nothing to do"
     */
    function inflateChildren(node, dict, depth) {
        if (!node || !node._dagChildIds || !dict) return false;
        var ids = node._dagChildIds;
        delete node._dagChildIds;
        delete node._dagPending;
        var sub = { format: 'dag-v1', nodes: dict, rootId: null };
        node.children = [];
        for (var i = 0; i < ids.length; i++) {
            sub.rootId = ids[i];
            var c = inflate(sub, typeof depth === 'number' ? depth : 1);
            if (c) { delete c._dag; delete c._dagNodeCount; node.children.push(c); }
        }
        return true;
    }

    var api = { isDag: isDag, inflate: inflate, inflateChildren: inflateChildren,
                INITIAL_DEPTH: INITIAL_DEPTH };
    (window.PA = window.PA || {}).dag = api;
    (window.JA = window.JA || {}).dag = api;
})();
