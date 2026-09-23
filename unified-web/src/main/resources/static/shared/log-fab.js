/**
 * A `seq`-cursor client for the log endpoints, shared by all four log viewers.
 *
 * Every viewer used to fetch `?lines=10000` on a 3–10 s timer — the largest response in the
 * application, against source logs measured at 9.36 MB and 4.09 MB — and then re-filter the whole
 * buffer in JavaScript on every keystroke. Each tick re-sent the window the client already had.
 *
 * This holds the last seq it has seen and asks only for what arrived after it, with the level and
 * text filters passed to the server so a filtered view is one request rather than a refetch plus a
 * client-side sweep. Two rules make it safe:
 *
 *   - `reset` from the server means the log rotated and every seq the client holds is meaningless.
 *     The buffer is dropped and refilled from the new tail. Appending across a rotation would
 *     render a log that never existed.
 *   - changing a filter resets the cursor, because the delta is defined relative to the filter it
 *     was issued under.
 *
 * Lives in log-fab.js rather than its own file because every page that has a log viewer already
 * loads this one, and a fifth script tag is a fifth chance to forget one.
 */
window.LogTailSource = function (url, firstPaintLimit) {
    var FIRST = firstPaintLimit || 1000;   // PageRequest.MAX_LIMIT is the server-side ceiling
    var DELTA = 200;

    var lines = [];        // raw log lines, oldest first — the buffer the viewer renders
    var seq = null;        // highest seq held; null means "nothing yet, ask for the tail"
    var level = '';
    var query = '';

    function params(limit, since) {
        var q = new URLSearchParams();
        q.set('limit', limit);
        if (since !== null && since !== undefined) q.set('sinceSeq', since);
        if (level) q.set('level', level);
        if (query) q.set('q', query);
        return q.toString();
    }

    return {
        /** The buffer, as one string, ready for a <pre>. */
        text: function () {
            return lines.length ? lines.join('\n') : '';
        },

        watermark: 0,
        fileSize: 0,

        /** True when the current filter selected nothing, so the viewer can say so. */
        isEmpty: function () { return lines.length === 0; },

        /**
         * Sets the server-side filter. Returns true if it changed, in which case the caller must
         * re-render — the cursor has been reset and the buffer is stale.
         */
        setFilter: function (newLevel, newQuery) {
            var l = newLevel || '', q = newQuery || '';
            if (l === level && q === query) return false;
            level = l;
            query = q;
            seq = null;
            lines = [];
            return true;
        },

        /** Fetches whatever is new (or the tail, on a first call or after a filter change). */
        poll: function () {
            var self = this;
            var first = (seq === null);
            var u = url + (url.indexOf('?') >= 0 ? '&' : '?')
                  + params(first ? FIRST : DELTA, first ? null : seq);

            return fetch(u, { headers: { 'Accept': PA_PAGE_MEDIA_TYPE } })
                .then(function (res) {
                    if (!res.ok) throw new Error('HTTP ' + res.status);
                    return res.json();
                })
                .then(function (body) {
                    var env = (window.PA && PA.normalizePage) ? PA.normalizePage(body)
                                                              : { items: body.items || [] };
                    self.watermark = body.watermark || 0;
                    self.fileSize = body.fileSize || 0;

                    if (body.reset) {
                        // The log rotated under us. Start again from the new tail rather than
                        // appending post-rotation lines onto pre-rotation ones.
                        lines = [];
                        seq = null;
                        return self.poll();
                    }

                    var items = env.items || [];
                    for (var i = 0; i < items.length; i++) {
                        lines.push(items[i].message);
                        if (items[i].seq > (seq || 0)) seq = items[i].seq;
                    }
                    if (seq === null) seq = self.watermark;   // nothing matched; still up to date
                    return items.length;
                });
        }
    };
};

/** page-envelope.js defines this; repeated here so log-fab.js has no load-order dependency. */
var PA_PAGE_MEDIA_TYPE = 'application/vnd.analyzer.page+json';

/**
 * Shared Log FAB + overlay viewer.
 * Works on all pages — tries multiple log endpoints.
 */
const LogFab = (() => {

    let _open = false;
    let _pollTimer = null;
    let _logUrl = null;
    let _tail = null;   // LogTailSource — holds the seq cursor and the line buffer

    const PLSQL_LOGS = '/api/plsql/logs';
    const JAR_LOGS   = '/api/jar/logs';

    /**
     * The log endpoint for the screen the FAB is actually on, with the other as a fallback.
     *
     * This used to be a fixed `['/api/plsql/logs', '/api/jar/logs']` probed in order, so the first
     * one that answered won — and `/api/plsql/logs` always answers. The JAR and WAR screens
     * therefore opened "Application Log" onto the PL/SQL analyser's endpoint every time, and no
     * amount of clicking from the JAR screen could reach `/api/jar/logs`.
     *
     * Both endpoints now resolve the same ${LOG_PATH}/unified-analyzer.log server-side (there is
     * one application log for the unified app; the JAR controller's old ${logging.file.name} path
     * was a separate bug, fixed alongside this). Ordering by screen still matters: it is what makes
     * the panel honest about which analyser it is reporting on, and it keeps working if the two
     * ever diverge. The fallback means a screen never loses its log viewer over this.
     */
    function _logUrlsForScreen() {
        const p = (location.pathname || '').toLowerCase();
        // /jar/... and /war/... are the JAR-analyser screens; /plsql/... and /parser/... the PL/SQL
        // ones. The landing page (/) belongs to neither, so it keeps the historical order.
        if (p.indexOf('/jar') === 0 || p.indexOf('/war') === 0) return [JAR_LOGS, PLSQL_LOGS];
        return [PLSQL_LOGS, JAR_LOGS];
    }

    function init() {
        if (document.getElementById('log-fab')) return;
        const fab = document.createElement('button');
        fab.id = 'log-fab';
        fab.className = 'log-fab';
        fab.title = 'View application logs';
        fab.setAttribute('aria-label', 'Open logs');
        fab.innerHTML = '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>';
        fab.addEventListener('click', toggle);
        document.body.appendChild(fab);
    }

    function toggle() { _open ? close() : open(); }

    async function open() {
        if (document.getElementById('log-fab-overlay')) return;
        _open = true;

        const fab = document.getElementById('log-fab');
        if (fab) fab.classList.add('log-fab-active');

        const html = '<div class="log-fab-overlay" id="log-fab-overlay">' +
            '<div class="log-fab-panel">' +
                '<div class="log-fab-header">' +
                    '<span class="log-fab-title">Application Log</span>' +
                    '<span class="log-fab-meta" id="log-fab-meta"></span>' +
                    '<button class="log-fab-btn" onclick="LogFab.refresh()">Refresh</button>' +
                    '<label class="log-fab-auto-label">' +
                        '<input type="checkbox" id="log-fab-auto" onchange="LogFab._toggleAuto()"> Auto' +
                    '</label>' +
                    '<button class="log-fab-btn" onclick="LogFab.close()">Close</button>' +
                '</div>' +
                '<div class="log-fab-filter-bar">' +
                    '<input type="text" id="log-fab-filter" class="log-fab-filter" placeholder="Filter logs (e.g. ERROR, Claude, analysis...)" oninput="LogFab._applyFilter()">' +
                    '<select id="log-fab-level" class="log-fab-level-select" onchange="LogFab._applyFilter()">' +
                        '<option value="">All Levels</option>' +
                        '<option value="ERROR">ERROR</option>' +
                        '<option value="WARN">WARN</option>' +
                        '<option value="INFO">INFO</option>' +
                        '<option value="DEBUG">DEBUG</option>' +
                    '</select>' +
                '</div>' +
                '<pre class="log-fab-content" id="log-fab-content">Loading...</pre>' +
            '</div>' +
        '</div>';
        PA.mountOverlay(html);

        document.getElementById('log-fab-overlay').addEventListener('click', function(e) {
            if (e.target.id === 'log-fab-overlay') LogFab.close();
        });

        await refresh();
    }

    function close() {
        _open = false;
        _tail = null;   // a reopened panel should show the current tail, not a stale buffer
        _stopAuto();
        const overlay = document.getElementById('log-fab-overlay');
        if (overlay) overlay.remove();
        const fab = document.getElementById('log-fab');
        if (fab) fab.classList.remove('log-fab-active');
    }

    /**
     * Pulls whatever is new and appends it.
     *
     * The filter is read here and pushed to the server; when it changes, the tail source resets its
     * cursor and the next poll refills the buffer. Nothing is filtered in the browser any more, so
     * a keystroke no longer sweeps a ten-thousand-line string.
     */
    async function refresh() {
        const content = document.getElementById('log-fab-content');
        const meta = document.getElementById('log-fab-meta');
        if (!content) return;

        try {
            const src = await _source();
            src.setFilter(document.getElementById('log-fab-level')?.value || '',
                          document.getElementById('log-fab-filter')?.value || '');
            await src.poll();
            content.textContent = src.isEmpty() ? '(no matching lines)' : src.text();
            content.scrollTop = content.scrollHeight;
            if (meta) {
                const sizeKb = Math.round((src.fileSize || 0) / 1024);
                // Name the endpoint: the panel is shared by four screens and "which log am I
                // looking at" was previously unanswerable from the UI.
                meta.textContent = src.watermark + ' lines | ' + sizeKb + 'KB | ' + (_logUrl || '');
            }
        } catch (err) {
            content.textContent = 'Failed to load logs: ' + err.message + '\n\nThe log file may not exist yet.';
            if (meta) meta.textContent = 'Error';
        }
    }

    /**
     * The FAB ships on pages of both analyzers, so it discovers which log endpoint answers — once,
     * with a one-line probe rather than the old ten-thousand-line one.
     */
    async function _source() {
        if (_tail) return _tail;
        for (const url of _logUrlsForScreen()) {
            try {
                const resp = await fetch(url + '?limit=1');
                if (resp.ok) {
                    _logUrl = url;
                    _tail = LogTailSource(url);
                    return _tail;
                }
            } catch (e) {}
        }
        throw new Error('No log endpoint available');
    }

    let _filterDebounce = null;

    function _applyFilter() {
        // The filter now lives on the server; re-render by asking for the filtered tail. Debounced,
        // because oninput fires per keystroke and each one is now a request.
        clearTimeout(_filterDebounce);
        _filterDebounce = setTimeout(refresh, 250);
    }

    function _toggleAuto() {
        var checked = document.getElementById('log-fab-auto')?.checked;
        if (checked) {
            _pollTimer = PollConfig.every('logRefreshMs', function() { refresh(); });
        } else {
            _stopAuto();
        }
    }

    function _stopAuto() {
        _pollTimer = PollConfig.clear(_pollTimer);
    }

    return { init, toggle, open, close, refresh, _applyFilter, _toggleAuto };

})();
