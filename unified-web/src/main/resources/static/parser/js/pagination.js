window.PA = window.PA || {};

/**
 * Reusable client-side paginator for parser list/table views.
 *
 * Usage in a view's render():
 *   var pg = PA.pager.slice('myKey', items);          // slice to current page
 *   ...build table rows from pg.pageItems...
 *   container.innerHTML = tableHtml + pg.controls;     // append page controls
 *   PA.pager.register('myKey', function(){ MyView.render(); });  // once is fine (idempotent)
 * On search/filter/scope change call PA.pager.reset('myKey') before render() so paging
 * jumps back to page 1.
 */
PA.pager = {
    _state: {},
    _renderers: {},
    _lastTotals: {},
    defaultSize: 10,

    /** The shared page-size policy (shared/tableFramework.js), with a local fallback. */
    _ps: function () {
        if (window.PA && PA.pageSize) return PA.pageSize;
        return {
            DEFAULT: 10, OPTIONS: [10, 25, 50, 100, 250], ALL: 'all',
            resolve: function () { return 10; },
            set: function () {},
            optionsHtml: function (cur, serverPaged) {
                var out = [10, 25, 50, 100, 250].map(function (n) {
                    return '<option value="' + n + '"' + (n === cur ? ' selected' : '') + '>' + n + '/page</option>';
                }).join('');
                if (!serverPaged) out += '<option value="all"' + (cur >= 1000000 ? ' selected' : '') + '>All</option>';
                return out;
            }
        };
    },

    get: function (key, size) {
        if (!this._state[key]) this._state[key] = { page: 1, size: size || this._ps().resolve(false) };
        return this._state[key];
    },
    /**
     * Back to page 1. For a page-source key this also drops every cached page and rewinds the
     * cursor, because a search, sort or scope change makes the old cursor meaningless.
     */
    reset: function (key) {
        if (this._state[key]) this._state[key].page = 1;
        var st = this._srcState[key];
        if (st) {
            // A page source owns a single cursor, so rewinding it while a fetch is in flight
            // would make that response a page nobody asked for. Deferred rather than dropped:
            // dropping it leaves the view showing rows that do not match the current filter.
            if (st.loading) { st.pendingReset = true; return; }
            st.src.reset();
            st.pages = {}; st.page = 1; st.highest = 1;
            st.total = -1; st.exact = false; st.hasMore = false;
        }
    },
    register: function (key, fn) { this._renderers[key] = fn; },

    /**
     * Returns { pageItems, controls, page, pages, total }.
     *
     * `items` is either a materialised array (the original behaviour, unchanged) or a page source
     * from `PA.pageSource(url, params)`. With a page source only the current page is in memory,
     * so the caller must forward its search box to the server (`src.setParam('q', …)` followed by
     * `PA.pager.reset(key)`) — a client-side filter would silently search one page.
     *
     * Stays synchronous either way: on a page not yet held it returns an empty page and a
     * "Loading…" bar, then calls the renderer registered with `PA.pager.register` once the
     * response arrives. That is what lets a call site convert without becoming async.
     */
    slice: function (key, items, size) {
        if (window.PA && PA.isPageSource && PA.isPageSource(items)) {
            return this._sliceSource(key, items, size);
        }
        items = items || [];
        var st = this.get(key, size);
        var total = items.length;
        var pages = Math.max(1, Math.ceil(total / st.size));
        if (st.page > pages) st.page = pages;
        if (st.page < 1) st.page = 1;
        var start = (st.page - 1) * st.size;
        var pageItems = items.slice(start, start + st.size);
        return {
            pageItems: pageItems, page: st.page, pages: pages, total: total,
            controls: this._controls(key, st, total, pages, start, pageItems.length)
        };
    },

    /* ---------- server-paged mode ---------- */

    _srcState: {},

    _src: function (key, src, size) {
        var st = this._srcState[key];
        if (!st) {
            st = this._srcState[key] = {
                src: src, pages: {}, page: 1, size: size || this._ps().resolve(true),
                highest: 1, total: -1, exact: false, hasMore: false,
                loading: false, error: null
            };
        }
        // A call site may rebuild its source (new analysis, new scope); adopt it and start over.
        if (st.src !== src) {
            st.src = src; st.pages = {}; st.page = 1; st.highest = 1;
            st.total = -1; st.exact = false; st.hasMore = false; st.error = null;
        }
        return st;
    },

    _sliceSource: function (key, src, size) {
        var st = this._src(key, src, size);
        var items = st.pages[st.page];
        if (items === undefined && !st.loading) this._fetch(key, st.page);
        items = items || [];
        var start = (st.page - 1) * st.size;
        return {
            pageItems: items,
            page: st.page,
            pages: st.exact ? Math.max(1, Math.ceil(st.total / st.size))
                            : st.highest + (st.hasMore ? 1 : 0),
            total: st.exact ? st.total : -1,
            controls: this._srcControls(key, st, start, items.length)
        };
    },

    _fetch: function (key, page) {
        var self = this, st = this._srcState[key];
        if (!st || st.loading) return;
        st.loading = true;
        st.error = null;
        st.src.fetchPage(st.size).then(function (env) {
            st.pages[page] = env.items;
            st.highest = Math.max(st.highest, page);
            st.hasMore = !!env.page.hasMore;
            st.total = env.page.total;
            st.exact = !!env.page.totalIsExact && env.page.total >= 0;
            st.page = page;
            st.loading = false;
            self._rerender(key);
            self._drainPendingReset(key);
        }).catch(function (err) {
            st.loading = false;
            if (err && err.resultSetReplaced) {
                // A re-analysis replaced the result set mid-scroll; restart rather than mix runs.
                st.error = 'The analysis was re-run; showing the first page again.';
                self.reset(key);
                self._rerender(key);
                return;
            }
            st.error = String((err && err.message) || err);
            self._rerender(key);
            self._drainPendingReset(key);
        });
    },

    _drainPendingReset: function (key) {
        var st = this._srcState[key];
        if (st && st.pendingReset) {
            st.pendingReset = false;
            this.reset(key);
            this._fetch(key, 1);
        }
    },

    _srcControls: function (key, st, start, shown) {
        function btn(label, page, disabled, active, title) {
            return '<button class="' + (active ? 'active' : '') + '"'
                + (disabled ? ' disabled' : '')
                + (title ? ' title="' + title + '"' : '')
                + ' onclick="PA.pager.go(\'' + key + '\',' + page + ')">' + label + '</button>';
        }
        var info;
        if (st.loading) info = 'Loading…';
        else if (st.exact) info = (st.total ? (start + 1) : 0) + '–' + (start + shown) + ' of ' + st.total;
        else info = (shown ? (start + 1) : 0) + '–' + (start + shown);

        // Numeric sizes only: a page source exists precisely so the whole result set is never sent,
        // so "All" is not offered here.
        var sizes = this._ps().optionsHtml(st.size, true);

        var html = '<div class="pagination-bar">'
            + '<div class="page-info">' + info + '</div>'
            + '<div class="page-size"><select onchange="PA.pager.setSize(\'' + key + '\', this.value)">' + sizes + '</select></div>';

        var busy = st.loading;
        html += '<div class="page-btns">'
            + btn('«', 1, st.page <= 1 || busy, false, 'First')
            + btn('‹', st.page - 1, st.page <= 1 || busy, false, 'Prev');
        if (st.exact) {
            var pages = Math.max(1, Math.ceil(st.total / st.size));
            var win = 2, from = Math.max(1, st.page - win), to = Math.min(pages, st.page + win);
            for (var p = from; p <= to; p++) {
                // Only pages already held, or the one after the furthest held, are addressable —
                // a cursor gives no way to jump to page 12 out of nowhere.
                var reachable = st.pages[p] !== undefined || p === st.highest + 1;
                html += btn(String(p), p, !reachable || busy, p === st.page);
            }
        } else {
            // The server could not count cheaply, so render the page number and a next arrow
            // rather than a fabricated "of N".
            html += '<button class="active" disabled>' + st.page + '</button>';
        }
        var canNext = st.hasMore || st.pages[st.page + 1] !== undefined;
        html += btn('›', st.page + 1, !canNext || busy, false, 'Next') + '</div>';
        if (st.error) {
            html += '<div class="page-info" style="color:var(--red,#c00)">'
                + (PA.esc ? PA.esc(st.error) : st.error) + '</div>';
        }
        return html + '</div>';
    },

    // Emits the SAME markup as the shared PA.tf pager (.pagination-bar / .page-info /
    // .page-size / .page-btns) so it inherits the app's themed pagination.css and looks
    // identical to every other paginated view (light or dark theme).
    _controls: function (key, st, total, pages, start, shown) {
        if (total === 0) return '';
        this._lastTotals[key] = total;   // so an "All" selection knows the size to expand to
        var end = start + shown;
        function btn(label, page, disabled, active, title) {
            return '<button class="' + (active ? 'active' : '') + '"'
                + (disabled ? ' disabled' : '')
                + (title ? ' title="' + title + '"' : '')
                + ' onclick="PA.pager.go(\'' + key + '\',' + page + ')">' + label + '</button>';
        }
        var sizes = this._ps().optionsHtml(st.size, false);
        var html = '<div class="pagination-bar">'
            + '<div class="page-info">' + (total ? (start + 1) : 0) + '–' + end + ' of ' + total + '</div>'
            + '<div class="page-size"><select onchange="PA.pager.setSize(\'' + key + '\', this.value)">' + sizes + '</select></div>';
        if (pages > 1) {
            var nums = '', win = 2, from = Math.max(1, st.page - win), to = Math.min(pages, st.page + win);
            if (from > 1) { nums += btn('1', 1, false, st.page === 1); if (from > 2) nums += '<span style="padding:0 2px">…</span>'; }
            for (var p = from; p <= to; p++) nums += btn(String(p), p, false, p === st.page);
            if (to < pages) { if (to < pages - 1) nums += '<span style="padding:0 2px">…</span>'; nums += btn(String(pages), pages, false, st.page === pages); }
            html += '<div class="page-btns">'
                + btn('«', 1, st.page <= 1, false, 'First') + btn('‹', st.page - 1, st.page <= 1, false, 'Prev')
                + nums
                + btn('›', st.page + 1, st.page >= pages, false, 'Next') + btn('»', pages, st.page >= pages, false, 'Last')
                + '</div>';
        }
        return html + '</div>';
    },

    go: function (key, page) {
        var src = this._srcState[key];
        if (src) {
            if (page < 1 || src.loading) return;
            if (src.pages[page] !== undefined) { src.page = page; this._rerender(key); return; }
            if (page === src.highest + 1 && src.hasMore) this._fetch(key, page);
            return;
        }
        var st = this.get(key); st.page = page; this._rerender(key);
    },
    setSize: function (key, size) {
        var ps = this._ps();
        if (String(size) === ps.ALL) {
            var srcAll = this._srcState[key];
            if (srcAll) return;   // no "All" for a page source
            var stAll = this.get(key);
            stAll.size = Math.max(1, this._lastTotals[key] || stAll.size);
            ps.set(ps.ALL);
            stAll.page = 1;
            this._rerender(key);
            return;
        }
        var n = parseInt(size, 10) || ps.DEFAULT;
        ps.set(n);
        var src = this._srcState[key];
        if (src) {
            // The page size is the server's `limit`, so every cached page was cut at the old size.
            src.size = n;
            this.reset(key);
            this._fetch(key, 1);
            return;
        }
        var st = this.get(key); st.size = n; st.page = 1; this._rerender(key);
    },
    _rerender: function (key) { var fn = this._renderers[key]; if (fn) fn(); }
};
// Styling reuses the app's themed css/pagination.css (.pagination-bar / .page-btns / …),
// so the pager matches every other view in both light and dark themes.
