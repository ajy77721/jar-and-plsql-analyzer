/**
 * shared/api-activity-panel.js — "what call is going, and why" panel.
 *
 * Many screens across every app (JAR/WAR/Parser/PL/SQL/Home) show a blank/loading area while an
 * API call is genuinely still in flight, with no visible cue that anything is happening beyond
 * whatever that one page chose to render. This adds ONE always-visible, always-present control —
 * a small fixed button, bottom-left, on every page — that opens a panel listing every currently
 * in-flight API call, with method+URL, the page it was made from, and elapsed time (live while
 * the panel is open).
 *
 * DATA SOURCE: a universal `window.fetch` wrapper here, NOT a second parallel interception layer
 * bolted onto each app's own tracker. JA.apiTracker (jar/js/utils.js, shared by war/js/api.js
 * since both set `window.JA`) already renders its own on-screen rows with human labels
 * ("Loading Summary" etc.) but only for the calls each app's api.js chooses to wrap, and parser/
 * plsql's `PA.api._dedupe` tracks in-flight *keys* for de-duplication, not for display, and skips
 * mutating calls entirely. Wrapping `fetch` once here catches literally every `/api/...` call in
 * every app, uniformly, regardless of whether that app's own tracker exists, names it, or wraps it
 * at all — which is what "why is this blank" needs across the whole surface, not just JAR/WAR.
 *
 * Where JA.apiTracker DOES exist, its human label is opportunistically attached (best-effort: it
 * calls `start(label)` synchronously immediately before invoking the wrapped fetch, so a label
 * seen within the last 200ms is almost always the one for the fetch that just fired). Where no
 * tracker exists, or the heuristic misses, the call is shown as "METHOD /path" — real data, not
 * invented context.
 *
 * SCOPE: this panel answers exactly one question — "is a normal page/screen call still running
 * right now, so I know whether to keep waiting?" It therefore:
 *   - shows ONLY calls that are currently in flight. There is no history/"recent completed" list;
 *     a call is dropped the instant it settles (success or failure), never shown afterwards.
 *   - never shows the Queue/analysis-job endpoints (`/api/queue`, `/api/parser/analyze`,
 *     `/api/plsql/analysis/analyze[-fast]`, and the upload+analyze POSTs to `/api/jar/jars` /
 *     `/api/war/wars`). Those are long-running background jobs already tracked by the separate
 *     Queue tab / persistent job-progress indicator — see EXCLUDED_ENDPOINTS below.
 */
(function () {
    'use strict';
    if (window.__apiActivityPanelInstalled) return;
    window.__apiActivityPanelInstalled = true;

    var esc = (window.Esc && window.Esc.html) ? window.Esc.html : function (s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
            return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
        });
    };

    var registry = window.__apiActivity = window.__apiActivity || {
        inFlight: new Map(),
        counter: 0,
        _lastLabel: null,
        _lastLabelAt: 0
    };
    var LABEL_WINDOW_MS = 200;

    /**
     * Queue / analysis-job endpoints — deliberately never tracked here. Those are long-running
     * background jobs surfaced by the separate Queue tab / persistent job-progress indicator; this
     * panel is scoped to ordinary page-driven request/response calls only, so a call matching any
     * of these is skipped before it ever enters the registry.
     */
    var EXCLUDED_ENDPOINTS = [
        /^\/api\/queue(\/|$|\?)/,
        /^\/api\/parser\/analyze(\/|$|\?)/,
        /^\/api\/plsql\/analysis\/analyze(-fast)?(\/|$|\?)/
    ];
    function isExcluded(method, path) {
        for (var i = 0; i < EXCLUDED_ENDPOINTS.length; i++) {
            if (EXCLUDED_ENDPOINTS[i].test(path)) return true;
        }
        // The upload+analyze POSTs hit the same collection URL as the ordinary GET list call, so
        // only the POST (the one that kicks off analysis) is excluded — the GET list stays visible.
        if (method === 'POST' && /^\/api\/jar\/jars\/?(\?|$)/.test(path)) return true;
        if (method === 'POST' && /^\/api\/war\/wars\/?(\?|$)/.test(path)) return true;
        return false;
    }

    var listeners = [];
    function notify() { for (var i = 0; i < listeners.length; i++) { try { listeners[i](); } catch (e) {} } }

    function shortLabel(method, url) {
        var path = url;
        try {
            var u = new URL(url, window.location.origin);
            path = u.pathname;
        } catch (e) { /* relative/opaque url, use as-is */ }
        return method + ' ' + path;
    }

    function recordStart(method, url) {
        var id = ++registry.counter;
        var label = null;
        if (registry._lastLabel && (Date.now() - registry._lastLabelAt) < LABEL_WINDOW_MS) {
            label = registry._lastLabel;
        }
        registry.inFlight.set(id, {
            id: id,
            method: method,
            url: url,
            label: label,
            display: shortLabel(method, url),
            page: window.location.pathname,
            startedAt: Date.now(),
            status: 'pending'
        });
        notify();
        return id;
    }

    function recordDone(id, ok, statusCode) {
        // A settled call is dropped immediately — this panel shows in-flight only, never a history
        // of completed calls, so there is nothing left to do here but stop tracking it.
        if (!registry.inFlight.has(id)) return;
        registry.inFlight.delete(id);
        notify();
    }

    // ---- Universal fetch instrumentation (the actual data source) ----
    if (!window.__apiActivityFetchWrapped && typeof window.fetch === 'function') {
        window.__apiActivityFetchWrapped = true;
        var origFetch = window.fetch;
        window.fetch = function (input, init) {
            var url = (typeof input === 'string') ? input : ((input && input.url) || String(input));
            var method = ((init && init.method) || (input && input.method) || 'GET').toUpperCase();
            if (!/\/api\//.test(url)) return origFetch.apply(this, arguments);
            var path = url;
            try { path = new URL(url, window.location.origin).pathname; } catch (e) { /* relative/opaque, use as-is */ }
            if (isExcluded(method, path)) return origFetch.apply(this, arguments);

            var id = recordStart(method, url);
            var result;
            try {
                result = origFetch.apply(this, arguments);
            } catch (syncErr) {
                recordDone(id, false, 0);
                throw syncErr;
            }
            return result.then(function (res) {
                recordDone(id, !!res.ok, res.status);
                // An expired AWS session surfaces as a 401 on any gated /api/** call. Route it to
                // the sign-in flow here — the one place every request passes — so a screen that
                // never calls PA.apiFail still gets the prompt instead of a silent empty panel.
                // handleUnauthorized is idempotent, so a fan-out prompts once.
                if (res.status === 401 && window.PA && typeof window.PA.handleUnauthorized === 'function') {
                    window.PA.handleUnauthorized(res);
                }
                return res;
            }, function (err) {
                recordDone(id, false, 0);
                throw err;
            });
        };
    }

    // ---- Opportunistic label capture from each app's own tracker (JA.apiTracker) ----
    function hookLabelSource(ns) {
        if (!ns || !ns.apiTracker || ns.apiTracker.__activityLabelHooked || typeof ns.apiTracker.start !== 'function') return;
        ns.apiTracker.__activityLabelHooked = true;
        var origStart = ns.apiTracker.start;
        ns.apiTracker.start = function (label) {
            registry._lastLabel = label;
            registry._lastLabelAt = Date.now();
            return origStart.apply(this, arguments);
        };
    }
    hookLabelSource(window.JA);
    hookLabelSource(window.PA);
    // App scripts sometimes finish attaching apiTracker after this file runs; a couple of cheap
    // retries covers that without a persistent timer.
    var hookTries = 0;
    var hookRetry = setInterval(function () {   // poll-exempt: self-clearing bootstrap retry (max 10), no network request
        hookLabelSource(window.JA);
        hookLabelSource(window.PA);
        if (++hookTries >= 10) clearInterval(hookRetry);
    }, 300);

    // ---- Button ----
    function ensureButton() {
        if (document.getElementById('api-activity-btn')) return;
        var btn = document.createElement('button');
        btn.id = 'api-activity-btn';
        btn.className = 'api-activity-btn';
        btn.type = 'button';
        btn.title = 'API activity — see what calls are running and why';
        btn.setAttribute('aria-label', 'API activity');
        btn.innerHTML = '<span class="api-activity-icon">&#8635;</span>' +
            '<span class="api-activity-badge" style="display:none">0</span>';
        btn.addEventListener('click', openPanel);
        document.body.appendChild(btn);
        updateBadge();
    }

    function updateBadge() {
        var btn = document.getElementById('api-activity-btn');
        if (!btn) return;
        var badge = btn.querySelector('.api-activity-badge');
        var n = registry.inFlight.size;
        if (n > 0) {
            badge.style.display = '';
            badge.textContent = n > 99 ? '99+' : String(n);
            btn.classList.add('api-activity-btn--active');
        } else {
            badge.style.display = 'none';
            btn.classList.remove('api-activity-btn--active');
        }
    }
    listeners.push(updateBadge);

    // ---- Panel (modal) ----
    function fmtElapsed(ms) {
        var s = Math.round(ms / 1000);
        if (s < 60) return s + 's';
        var m = Math.floor(s / 60);
        return m + 'm ' + (s % 60) + 's';
    }

    function rowHtml(entry, now) {
        var elapsedMs = now - entry.startedAt;
        var labelText = entry.label ? entry.label + ' — ' + entry.display : entry.display;
        return '<div class="api-activity-row api-activity-row--pending">' +
            '<span class="api-activity-row-dot"></span>' +
            '<div class="api-activity-row-main">' +
                '<div class="api-activity-row-label">' + esc(labelText) + '</div>' +
                '<div class="api-activity-row-meta">page: ' + esc(entry.page) + '</div>' +
            '</div>' +
            '<div class="api-activity-row-side">' +
                '<div class="api-activity-row-time">' + fmtElapsed(elapsedMs) + '</div>' +
                '<div class="api-activity-row-status">in flight</div>' +
            '</div>' +
        '</div>';
    }

    function sortedInFlight() {
        return Array.from(registry.inFlight.values()).sort(function (a, b) { return a.startedAt - b.startedAt; });
    }

    function panelHtml() {
        var now = Date.now();
        var inFlight = sortedInFlight();

        var inFlightHtml = inFlight.length
            ? inFlight.map(function (e) { return rowHtml(e, now); }).join('')
            : '<div class="api-activity-empty">Nothing in flight right now — if the screen still looks blank, something else is wrong.</div>';

        return (
            '<div id="api-activity-modal" class="api-activity-overlay">' +
              '<div class="api-activity-panel">' +
                '<div class="api-activity-hdr">' +
                  '<div class="api-activity-title">API Activity</div>' +
                  '<button type="button" class="api-activity-close" id="api-activity-close-btn" aria-label="Close">&times;</button>' +
                '</div>' +
                '<div class="api-activity-body">' +
                  '<div class="api-activity-section">' +
                    '<div class="api-activity-section-title">In flight (' + inFlight.length + ')</div>' +
                    '<div class="api-activity-list" id="api-activity-inflight-list">' + inFlightHtml + '</div>' +
                  '</div>' +
                '</div>' +
              '</div>' +
            '</div>'
        );
    }

    var refreshTimer = null;

    function refreshPanel() {
        var modal = document.getElementById('api-activity-modal');
        if (!modal) { stopRefresh(); return; }
        var body = modal.querySelector('.api-activity-body');
        if (!body) { stopRefresh(); return; }
        var now = Date.now();
        var inFlight = sortedInFlight();
        var inflightList = modal.querySelector('#api-activity-inflight-list');
        if (inflightList) {
            inflightList.innerHTML = inFlight.length
                ? inFlight.map(function (e) { return rowHtml(e, now); }).join('')
                : '<div class="api-activity-empty">Nothing in flight right now — if the screen still looks blank, something else is wrong.</div>';
        }
        var titleEl = modal.querySelector('.api-activity-section-title');
        if (titleEl) titleEl.textContent = 'In flight (' + inFlight.length + ')';
    }

    function stopRefresh() {
        if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null; }
    }

    function closePanel() {
        stopRefresh();
        var modal = document.getElementById('api-activity-modal');
        if (modal) modal.remove();
    }

    function openPanel() {
        // Always via the shared helper (page-envelope.js) — never a raw insertAdjacentHTML — so
        // clicking the button twice replaces the panel instead of stacking two of them.
        var mount = (window.PA && window.PA.mountOverlay) || (window.JA && window.JA.mountOverlay);
        if (!mount) return;
        var modal = mount(panelHtml());
        if (!modal) return;

        modal.addEventListener('click', function (ev) {
            if (ev.target === modal) closePanel();
        });
        var closeBtn = modal.querySelector('#api-activity-close-btn');
        if (closeBtn) closeBtn.addEventListener('click', closePanel);

        // Only polls while the panel is actually open — no background timer otherwise.
        stopRefresh();
        refreshTimer = setInterval(refreshPanel /* poll-exempt: re-renders in-memory tracker state only, no network request */, 500);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', ensureButton);
    } else {
        ensureButton();
    }
})();
