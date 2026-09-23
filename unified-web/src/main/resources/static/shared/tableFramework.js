/**
 * The paginating table framework, shared by the parser and plsql pages.
 *
 * ## Why this file exists
 *
 * There were two copies — 752 lines under `parser/js/` and 457 under `plsql/js/` — differing on 357
 * lines. The parser copy is a strict superset: plsql had no member the parser lacked, and every
 * shared member had an identical signature. What plsql lacked entirely was the server-paging layer
 * (`src`, `serverSorts`, `pageCache`, `serverTotal`, `hasMore`), which is the thing that lets a
 * grid show a million rows without downloading them.
 *
 * Two copies meant every fix cost twice, and only ever landed once in practice. This is the copy
 * that survives.
 *
 * ## The one behavioural difference, preserved deliberately
 *
 * The plsql copy hid the pager entirely for tables of 20 rows or fewer; the parser copy always
 * showed it, with an item count and a page-size selector. That is a visible difference, and a
 * de-fork is not the place to change what a screen looks like — the two things would be
 * indistinguishable in the diff if anything went wrong. `hidePagerBelow` keeps each page's current
 * appearance; unifying them later is a one-line change and a deliberate decision of its own.
 */
/**
 * PA.tf — Shared Table Framework (pagination, filtering, sorting, column filters).
 *
 * Usage:
 *   PA.tf.init(id, data, pageSize, renderRow, opts)
 *   PA.tf.render(id)
 *   PA.tf.filter(id)
 *   PA.tf.sort(id, colIdx)
 *   PA.tf.goPage(id, p)
 *   PA.tf.setPageSize(id, sz)
 *
 * Options (opts):
 *   sortKeys:     { colIdx: { fn: item => comparable, desc?: bool } }
 *   renderDetail: (item, i, esc) => '<tr>...</tr>'  // expandable detail row
 *   domainFn:     item => domainString               // for domain filter dropdown
 *   onFilter:     () => void                         // callback after filter applied
 *   searchFn:     (item, query) => bool              // custom search (default: text match)
 *   extraFilter:  (item) => bool                     // additional filter predicate
 */
window.PA = window.PA || {};

PA.tf = {

    /**
     * Tables with this many rows or fewer render no pager at all.
     *
     * <p>0 — always show it — is the parser page's long-standing behaviour and the default. The
     * plsql page sets 20 at startup, which is what its own fork did. Set once per page, not per
     * table: it is a property of how a page presents small tables, not of any one grid.
     */
    hidePagerBelow: 0,

    _state: {},

    /**
     * Initialize a paginated table.
     *
     * `data` is either a **materialised array** (the original behaviour, unchanged) or a
     * **page source** built by `PA.pageSource(url, params)`. A page source means only one page is
     * ever in memory, so search and sort have to be answered by the server — a client-side search
     * over one page looks like it worked while missing every other page. A converted call site
     * therefore declares `opts.serverSorts` and its search box is forwarded as `q`.
     *
     * @param {string} id - unique table id (used to find DOM elements: {id}-tbody, {id}-pager, etc.)
     * @param {Array|Object} data - full dataset, or a `PA.pageSource`
     * @param {number} pageSize - items per page
     * @param {Function} renderRow - (item, globalIndex, esc) => HTML string for <tr>
     * @param {Object} opts - see above, plus `serverSorts: { colIdx: {asc, desc} }`
     */
    init(id, data, pageSize, renderRow, opts) {
        opts = opts || {};
        const src = (window.PA && PA.isPageSource && PA.isPageSource(data)) ? data : null;
        const rows = src ? [] : (data || []);
        const s = {
            id,
            data: rows,
            filtered: src ? [] : [...rows],
            page: 0,
            pageSize: pageSize || 50,
            renderRow,
            renderDetail: opts.renderDetail || null,
            sortKeys: opts.sortKeys || {},
            sortCol: -1,
            sortDir: 'asc',
            domainFn: opts.domainFn || null,
            onFilter: opts.onFilter || null,
            searchFn: opts.searchFn || null,
            extraFilter: opts.extraFilter || null,
            searchQuery: '',
            domainFilter: '',
            colFilters: {},
            colFilterDefs: {},

            // --- server-paging state; all null/absent on the array path ---
            src: src,
            serverSorts: opts.serverSorts || null,
            pageCache: src ? {} : null,
            highestPage: 0,
            serverTotal: -1,
            totalIsExact: false,
            hasMore: false,
            loading: false,
            loadError: null,
            reloadFn: null
        };
        PA.tf._state[id] = s;
        // Page 1 is fetched here, and the promise is kept so a call site can wait for it. That
        // matters for anything that needs to know how many rows there are before deciding what
        // else to request — see the summary skip in sequences.js / cursors.js.
        if (src) s.firstLoad = PA.tf._fetch(id, 0);
        return s;
    },

    /** True when this table is backed by a page source rather than an array. */
    isServerPaged(id) {
        const s = PA.tf._state[id];
        return !!(s && s.src);
    },

    /**
     * Fetches one page from the source and renders it.
     *
     * Cursor paging only moves forward, so pages already seen are kept in `pageCache`; that is
     * what keeps the existing prev/next buttons working without ever asking the client to
     * construct a cursor.
     */
    _fetch(id, page) {
        const s = PA.tf._state[id];
        if (!s || !s.src || s.loading) return Promise.resolve();
        s.loading = true;
        s.loadError = null;
        PA.tf.render(id);
        return s.src.fetchPage(s.pageSize).then(env => {
            s.pageCache[page] = env.items;
            s.highestPage = Math.max(s.highestPage, page);
            s.hasMore = !!env.page.hasMore;
            s.serverTotal = env.page.total;
            s.totalIsExact = !!env.page.totalIsExact && env.page.total >= 0;
            s.page = page;
            s.filtered = env.items;
            s.loading = false;
            PA.tf.render(id);
            if (s.onFilter) s.onFilter();
            PA.tf._drainPendingRestart(id);
        }).catch(err => {
            s.loading = false;
            if (err && err.resultSetReplaced) {
                // A re-analysis replaced the result set mid-scroll. Restarting is the only correct
                // answer; offset paging would have quietly mixed rows from two runs.
                s.loadError = 'The analysis was re-run; showing the first page again.';
                PA.tf._restart(id);
                return;
            }
            s.loadError = String((err && err.message) || err);
            PA.tf.render(id);
            PA.tf._drainPendingRestart(id);
        });
    },

    /**
     * Runs a restart that arrived while a fetch was in flight.
     *
     * A page source owns a single cursor, so two overlapping fetches would advance it twice and
     * the second response would be a page nobody asked for. Restarts are therefore serialised
     * rather than fired concurrently — the alternative, dropping them, leaves the table showing
     * rows that do not match the filter the user just typed.
     */
    _drainPendingRestart(id) {
        const s = PA.tf._state[id];
        if (s && s._pendingRestart) {
            s._pendingRestart = false;
            PA.tf._restart(id);
        }
    },

    /** Drops every cached page and reloads page 1 — after a search, filter or sort change. */
    _restart(id) {
        const s = PA.tf._state[id];
        if (!s || !s.src) return;
        if (s.loading) { s._pendingRestart = true; return; }
        s.src.reset();
        s.pageCache = {};
        s.filtered = [];
        s.highestPage = 0;
        s.page = 0;
        s.serverTotal = -1;
        s.totalIsExact = false;
        PA.tf._fetch(id, 0);
    },

    /** How many pages the pager may offer; never a fabricated count. */
    _serverTotalPages(s) {
        if (s.totalIsExact) return Math.max(1, Math.ceil(s.serverTotal / s.pageSize));
        return s.highestPage + (s.hasMore ? 2 : 1);
    },

    /** Get state for a table */
    state(id) { return PA.tf._state[id]; },

    /** Set extra filter function (callable from outside, e.g. operation pills) */
    setExtraFilter(id, fn) {
        const s = PA.tf._state[id];
        if (s) { s.extraFilter = fn; }
    },

    /** Set search query programmatically */
    setSearch(id, query) {
        const s = PA.tf._state[id];
        if (s) { s.searchQuery = query; }
    },

    /** Set domain filter programmatically */
    setDomainFilter(id, domain) {
        const s = PA.tf._state[id];
        if (s) { s.domainFilter = domain; }
    },

    /** Replace data and re-render */
    /**
     * Marks an array-backed table as having failed to load, so `render` shows a distinct error
     * row rather than "No data found".
     *
     * Server-paged tables get this for free from `_fetch`. Tables that fetch their own rows and
     * then hand an array to `setData` (complexity, statements) had no way to say "this is empty
     * because the request failed" — they do now. Pass a falsy `msg` to clear.
     *
     * `reloadFn`, when given, is what the inline Retry link calls; omit it for a bare message.
     */
    setLoadError(id, msg, reloadFn) {
        const s = PA.tf._state[id];
        if (!s) return;
        s.loadError = msg ? String(msg) : null;
        s.reloadFn = msg ? (reloadFn || null) : null;
        PA.tf.render(id);
    },

    /** Runs the reload registered by `setLoadError` and clears the error row while it is in flight. */
    retry(id) {
        const s = PA.tf._state[id];
        if (!s || !s.reloadFn) return;
        const fn = s.reloadFn;
        s.loadError = null;
        s.reloadFn = null;
        PA.tf.render(id);
        fn();
    },

    setData(id, data) {
        const s = PA.tf._state[id];
        if (!s) return;
        if (window.PA && PA.isPageSource && PA.isPageSource(data)) {
            s.src = data;
            s.pageCache = {};
            PA.tf._restart(id);
            return;
        }
        // Swapping an array in over a page source would silently go back to client-side paging.
        if (s.src) { console.warn('[PA.tf] setData with an array on server-paged table', id); }
        s.data = data || [];
        s.page = 0;
        PA.tf.filter(id);
    },

    /** Apply all filters (search, domain, column, extra) and re-render */
    filter(id) {
        const s = PA.tf._state[id];
        if (!s) return;
        const esc = PA.esc || (v => v);

        if (s.src) {
            // Search moves to the server with pagination; filtering the one page in memory would
            // look like it worked and quietly miss the rest of the result set.
            //
            // Debounced, because the search box calls this on every keystroke. That cost nothing
            // when the filter ran over an array already in memory; unthrottled it would now be one
            // request per character, and out-of-order responses could leave the wrong page shown.
            clearTimeout(s._filterTimer);
            s._filterTimer = setTimeout(function () {
                s.src.setParam('q', (s.searchQuery || '').trim());
                // Only sent by a table that actually has a domain dropdown; sending it blindly
                // would rely on the server ignoring a parameter it does not know, which is the
                // habit that makes a mistyped filter look like it worked.
                if (s.domainFn) s.src.setParam('domain', s.domainFilter || '');
                PA.tf._restart(id);
            }, 200);
            return;
        }

        let list = s.data;

        // Search filter
        const q = (s.searchQuery || '').trim().toUpperCase();
        if (q) {
            if (s.searchFn) {
                list = list.filter(item => s.searchFn(item, q));
            } else {
                // Default: stringify and match
                list = list.filter(item => {
                    for (const v of Object.values(item)) {
                        if (v != null && String(v).toUpperCase().includes(q)) return true;
                    }
                    return false;
                });
            }
        }

        // Domain filter
        if (s.domainFilter && s.domainFn) {
            list = list.filter(item => s.domainFn(item) === s.domainFilter);
        }

        // Extra filter (operation pills, etc.)
        if (s.extraFilter) {
            list = list.filter(item => s.extraFilter(item));
        }

        // Column filters
        for (const [colIdx, selected] of Object.entries(s.colFilters)) {
            const def = s.colFilterDefs[colIdx];
            if (!def) continue;
            if (def.mode === 'text') {
                if (!selected || !selected.value) continue;
                list = list.filter(item => PA.tf._textMatch(String(def.valueFn(item) ?? ''), selected));
                continue;
            }
            if (!selected || !selected.size) continue;
            list = list.filter(item => {
                const v = def.valueFn(item);
                if (Array.isArray(v)) return v.some(vi => selected.has(String(vi)));
                return selected.has(String(v));
            });
        }

        // Apply sort
        if (s.sortCol >= 0 && s.sortKeys[s.sortCol]) {
            const sk = s.sortKeys[s.sortCol];
            const fn = sk.fn;
            const dir = s.sortDir === 'desc' ? -1 : 1;
            list = [...list].sort((a, b) => {
                const va = fn(a), vb = fn(b);
                if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
                return String(va || '').localeCompare(String(vb || '')) * dir;
            });
        }

        s.filtered = list;

        // Clamp page
        const totalPages = Math.ceil(list.length / s.pageSize) || 1;
        if (s.page >= totalPages) s.page = totalPages - 1;
        if (s.page < 0) s.page = 0;

        PA.tf.render(id);

        if (s.onFilter) s.onFilter();
    },

    /** Sort by column index — toggle asc/desc */
    sort(id, colIdx) {
        const s = PA.tf._state[id];
        if (!s) return;
        if (s.src) {
            // Only through the endpoint's closed enum — a free-form column name would be both an
            // injection surface and an unindexed-scan surface. A column the endpoint cannot sort
            // on is simply not offered, rather than sorting the current page and looking right.
            const names = s.serverSorts && s.serverSorts[colIdx];
            if (!names) {
                console.warn('[PA.tf] no server sort declared for column', colIdx, 'of', id);
                return;
            }
            const dir = s.sortCol === colIdx ? (s.sortDir === 'asc' ? 'desc' : 'asc') : 'asc';
            const enumName = dir === 'asc' ? names.asc : names.desc;
            if (!enumName) return;
            s.sortCol = colIdx;
            s.sortDir = dir;
            s.src.setParam('sort', enumName);
            PA.tf._updateSortIndicators(id);
            PA.tf._restart(id);
            return;
        }
        if (s.sortCol === colIdx) {
            s.sortDir = s.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
            s.sortCol = colIdx;
            s.sortDir = 'asc';
        }
        PA.tf._updateSortIndicators(id);
        PA.tf.filter(id);
    },

    /**
     * Sets the initial server sort without a user click, for a table whose default order differs
     * from the endpoint's natural one. Must be called before the first fetch completes to avoid a
     * wasted request; `init` returns the state so a call site can set it immediately.
     */
    setServerSort(id, colIdx, dir) {
        const s = PA.tf._state[id];
        if (!s || !s.src) return;
        const names = s.serverSorts && s.serverSorts[colIdx];
        const enumName = names && (dir === 'desc' ? names.desc : names.asc);
        if (!enumName) return;
        s.sortCol = colIdx;
        s.sortDir = dir === 'desc' ? 'desc' : 'asc';
        s.src.setParam('sort', enumName);
    },

    /** Navigate to page */
    goPage(id, p) {
        const s = PA.tf._state[id];
        if (!s) return;
        if (s.src) {
            if (p < 0 || s.loading) return;
            if (s.pageCache[p] !== undefined) {
                s.page = p;
                s.filtered = s.pageCache[p];
                PA.tf.render(id);
                if (s.onFilter) s.onFilter();
                return;
            }
            // Only the page directly after the furthest one fetched is addressable — anything
            // beyond it would need a cursor the server has never handed us.
            if (p === s.highestPage + 1 && s.hasMore) PA.tf._fetch(id, p);
            return;
        }
        s.page = Math.max(0, Math.min(p, Math.ceil(s.filtered.length / s.pageSize) - 1));
        PA.tf.render(id);
    },

    /** Change page size */
    setPageSize(id, sz) {
        const s = PA.tf._state[id];
        if (!s) return;
        s.pageSize = sz;
        s.page = 0;
        // The page size is the server's `limit`, so every cached page was cut at the old size.
        if (s.src) { PA.tf._restart(id); return; }
        PA.tf.render(id);
    },

    /** Render current page into DOM */
    render(id) {
        const s = PA.tf._state[id];
        if (!s) return;
        const esc = PA.esc || (v => v);

        const tbody = document.getElementById(id + '-tbody');
        if (!tbody) return;

        // Server-paged: the response IS the page, there is nothing to slice.
        const serverPaged = !!s.src;
        const total = serverPaged ? s.serverTotal : s.filtered.length;
        const totalPages = serverPaged
            ? PA.tf._serverTotalPages(s)
            : (Math.ceil(total / s.pageSize) || 1);
        const start = s.page * s.pageSize;
        const pageData = serverPaged
            ? (s.pageCache[s.page] || [])
            : s.filtered.slice(start, Math.min(start + s.pageSize, total));

        if (serverPaged && s.loading && pageData.length === 0) {
            tbody.innerHTML = '<tr><td colspan="20" class="empty-msg" style="padding:20px;text-align:center">Loading…</td></tr>';
        } else if (s.loadError) {
            // Not gated on `serverPaged` any more. Array-backed tables (complexity, statements)
            // fetch their rows themselves and used to hand `[]` to setData when that fetch threw,
            // which rendered "No data found" — a failed request and a genuinely empty corpus were
            // indistinguishable. They now call PA.tf.setLoadError and land here instead.
            tbody.innerHTML = '<tr><td colspan="20" class="tf-load-error" style="padding:20px;text-align:center;'
                + 'color:var(--red,#c0392b);font-weight:600">'
                + '&#9888; Failed to load &mdash; '
                + (PA.esc ? PA.esc(s.loadError) : s.loadError)
                + (s.reloadFn ? ' <a href="#" style="font-weight:600" onclick="PA.tf.retry(\'' + id + '\');return false">Retry</a>' : '')
                + '</td></tr>';
        } else if (pageData.length === 0) {
            tbody.innerHTML = '<tr><td colspan="20" class="empty-msg" style="padding:20px;text-align:center">No data found</td></tr>';
        } else {
            let html = '';
            for (let i = 0; i < pageData.length; i++) {
                const globalIdx = start + i;
                html += s.renderRow(pageData[i], globalIdx, esc);
                // Detail row (lazy placeholder)
                if (s.renderDetail) {
                    html += `<tr class="to-detail-row" id="${id}-detail-${globalIdx}" style="display:none" data-lazy="1"><td colspan="20"></td></tr>`;
                }
            }
            tbody.innerHTML = html;
        }

        // Render pager (bottom)
        PA.tf._renderPager(id, total, totalPages);
        // Render pager (top, if exists)
        PA.tf._renderPager(id + '-top', total, totalPages, id);
    },

    /** Toggle detail row */
    toggleDetail(id, idx) {
        const row = document.getElementById(id + '-detail-' + idx);
        if (!row) return;
        const s = PA.tf._state[id];
        if (!s) return;

        // Lazy render on first open. `idx` is the global row index; in server-paged mode
        // `s.filtered` holds only the current page, so it has to be rebased.
        if (row.dataset.lazy && s.renderDetail) {
            const item = s.src ? s.filtered[idx - s.page * s.pageSize] : s.filtered[idx];
            if (item) {
                const esc = PA.esc || (v => v);
                row.innerHTML = '<td colspan="20">' + s.renderDetail(item, idx, esc) + '</td>';
            }
            delete row.dataset.lazy;
        }

        const visible = row.classList.contains('open');
        row.classList.toggle('open', !visible);
        row.style.display = visible ? 'none' : 'table-row';
        const parentRow = row.previousElementSibling;
        if (parentRow) parentRow.classList.toggle('expanded', !visible);
    },

    /**
     * Whether a case-insensitive substring/regex match matches `text`.
     *
     * Shared by the array-backed filter path here and used as the model for the server-side
     * equivalent (`FacetPageResponder.columnFilters`) — the two must agree on what "contains" and
     * "regex" mean, or a table filters differently depending on whether it happens to be paged.
     */
    _textMatch(text, cfval) {
        if (!cfval) return true;
        const cs = !!cfval.cs;
        if (cfval.mode === 'regex') {
            try {
                const re = new RegExp(cfval.value, cs ? '' : 'i');
                return re.test(text);
            } catch (e) {
                return false;
            }
        }
        return cs ? text.includes(cfval.value) : text.toUpperCase().includes(cfval.value.toUpperCase());
    },

    // ==================== COLUMN FILTERS ====================

    /**
     * Register column filters for a table.
     *
     * @param {string} id - table id
     * @param {Object} columns - { colIdx: { label, valueFn: item => string|string[], mode?: 'text' } }
     *   Checkbox columns (the default, `mode` omitted) enumerate exact values, collected from the
     *   whole dataset up front. `mode: 'text'` opts a column into a free-text popover instead
     *   (contains/regex, with a case-sensitivity toggle) — for identifier columns too large or too
     *   free-form to enumerate. The two are mutually exclusive per column, but a single call can mix
     *   both kinds across its columns; existing checkbox-only callers are unaffected.
     */
    initColFilters(id, columns, serverValues) {
        const s = PA.tf._state[id];
        if (!s) return;
        const hasCheckboxCol = Object.values(columns).some(c => c.mode !== 'text');
        if (s.src && hasCheckboxCol && !serverValues) {
            // The value list in a popover is collected from the whole dataset, and in server-paged
            // mode only one page is in memory. Showing the icon would offer this page's values and
            // filter to a subset of one page — visibly wrong. A converted call site must pass the
            // `columnValues` map from the facet summary instead; without it, no icon is the honest
            // answer. Text-mode columns need no such list, so they are unaffected by this guard.
            console.warn('[PA.tf] column filters need serverValues on server-paged table', id);
            return;
        }
        s.colFilterValues = serverValues || null;
        s.colFilterDefs = columns;
        s.colFilters = {};

        // Any placeholder icon for these columns (see `pendingColFilters`) is now stale — the real,
        // interactive icon replaces it below rather than sitting next to it.
        PA.tf._cfRemovePlaceholders(id);

        // Attach filter icons to headers
        PA.tf._cfEachHeader(id, columns, (th, ci) => {
            if (th.querySelector('.cf-icon')) return;
            th.style.position = 'relative';
            const btn = PA.tf._cfMakeIcon(columns[ci].label);
            btn.onclick = (e) => { e.stopPropagation(); PA.tf._cfToggle(id, ci, th); };
            th.appendChild(btn);
        });
    },

    /**
     * Shows a non-interactive placeholder filter icon the moment a table renders, for any column
     * that WILL get a real filter once its async fetch (facet summary, table-detail lookup, …)
     * resolves.
     *
     * Without this, the gap between "table rendered" and "`initColFilters` ran" is silent — the
     * header shows nothing where an icon is about to appear, which reads to a user as "the filter
     * isn't there" rather than "still loading". Same trigger position and visual language as the
     * real `.cf-icon` (same SVG, same spot in the header) so it reads as "this one's not ready yet",
     * not a different UI element.
     *
     * `columns` only needs `{ label }` per column here — `valueFn`/`mode` aren't required until the
     * real `initColFilters(id, columns, serverValues)` call. Safe to call before `PA.tf.init` has
     * even run its first render, and safe to call again (e.g. on a re-render) — it no-ops once a
     * real or pending icon already occupies a header.
     */
    pendingColFilters(id, columns) {
        PA.tf._cfEachHeader(id, columns, (th, ci) => {
            if (th.querySelector('.cf-icon')) return;
            th.style.position = 'relative';
            const btn = PA.tf._cfMakeIcon('Filter ' + (columns[ci].label || '') + ' — loading…');
            btn.classList.add('cf-pending');
            btn.dataset.cfPending = '1';
            th.appendChild(btn);
        });
    },

    /**
     * Replaces any pending placeholder icons with a muted "filter unavailable" state, for when the
     * async fetch a column's filter depends on fails. Leaving the placeholder pulsing forever would
     * be its own silent failure; this makes the failure visible and explains it on hover.
     */
    colFiltersFailed(id, columns, reason) {
        const msg = reason ? String(reason) : 'could not be loaded';
        PA.tf._cfEachHeader(id, columns, (th, ci) => {
            const existing = th.querySelector('.cf-icon');
            if (existing && !existing.dataset.cfPending) return; // a real icon already loaded
            if (existing) existing.remove();
            th.style.position = 'relative';
            const btn = PA.tf._cfMakeIcon('Filter ' + (columns[ci].label || '') + ' unavailable: ' + msg);
            btn.classList.add('cf-error');
            th.appendChild(btn);
        });
    },

    /** Removes any pending/error placeholder icons for `id`, leaving real `.cf-icon`s untouched. */
    _cfRemovePlaceholders(id) {
        const tbody = document.getElementById(id + '-tbody');
        const table = tbody?.closest('table');
        if (!table) return;
        table.querySelectorAll('.cf-icon.cf-pending, .cf-icon.cf-error').forEach(icon => icon.remove());
    },

    /** Shared header-walk used by `initColFilters`/`pendingColFilters`/`colFiltersFailed`. */
    _cfEachHeader(id, columns, fn) {
        const tbody = document.getElementById(id + '-tbody');
        const table = tbody?.closest('table');
        if (!table) return;
        table.querySelectorAll('th').forEach((th, thIdx) => {
            const ci = th.dataset.sortCol != null ? parseInt(th.dataset.sortCol) : thIdx;
            if (!columns[ci]) return;
            fn(th, ci);
        });
    },

    _cfIconSvg: '<svg viewBox="0 0 16 16" width="11" height="11" fill="currentColor" style="vertical-align:-1px;margin-left:3px"><path d="M1 2h14l-5 6v5l-4 2V8z"/></svg>',

    _cfMakeIcon(title) {
        const btn = document.createElement('span');
        btn.className = 'cf-icon';
        btn.title = title;
        btn.innerHTML = PA.tf._cfIconSvg;
        return btn;
    },

    _cfToggle(id, colIdx, thEl) {
        const s = PA.tf._state[id];
        const def = s && s.colFilterDefs[colIdx];
        if (def && def.mode === 'text') { PA.tf._cfToggleText(id, colIdx, thEl); return; }
        PA.tf._cfClose();
        if (!s || !s.colFilterDefs[colIdx]) return;

        const allValues = PA.tf._cfValues(s, colIdx);
        if (!allValues.length) return;

        const active = s.colFilters[colIdx];
        const esc = PA.esc || (v => v);

        const rect = thEl.getBoundingClientRect();
        const pop = document.createElement('div');
        pop.className = 'cf-popover';
        pop.id = 'cf-popover';
        pop.style.top = (rect.bottom + window.scrollY + 2) + 'px';
        pop.style.left = Math.max(0, rect.left + window.scrollX - 40) + 'px';

        let html = `<div class="cf-header">${esc(def.label)}</div>`;
        html += '<div class="cf-actions"><button class="cf-btn" onclick="PA.tf._cfSelectAll()">All</button>';
        html += '<button class="cf-btn" onclick="PA.tf._cfSelectNone()">None</button></div>';
        html += '<div class="cf-list">';
        for (const v of allValues) {
            const checked = !active || active.has(v) ? 'checked' : '';
            html += `<label class="cf-item"><input type="checkbox" value="${PA.escAttr ? PA.escAttr(v) : v}" ${checked}><span>${esc(v)}</span></label>`;
        }
        html += '</div>';
        html += `<div class="cf-footer">`;
        html += `<button class="cf-apply" onclick="PA.tf._cfApply('${id}',${colIdx})">Apply</button>`;
        html += `<button class="cf-btn" onclick="PA.tf._cfClear('${id}',${colIdx})">Clear</button>`;
        html += `<button class="cf-cancel" onclick="PA.tf._cfClose()">Cancel</button>`;
        html += '</div>';
        pop.innerHTML = html;
        document.body.appendChild(pop);

        setTimeout(() => {
            PA.tf._cfOutsideHandler = (e) => {
                if (!pop.contains(e.target)) PA.tf._cfClose();
            };
            document.addEventListener('click', PA.tf._cfOutsideHandler, true);
        }, 0);
    },

    _cfClose() {
        const pop = document.getElementById('cf-popover');
        if (pop) pop.remove();
        if (PA.tf._cfOutsideHandler) {
            document.removeEventListener('click', PA.tf._cfOutsideHandler, true);
            PA.tf._cfOutsideHandler = null;
        }
    },

    _cfSelectAll() {
        const pop = document.getElementById('cf-popover');
        if (pop) pop.querySelectorAll('input[type=checkbox]').forEach(cb => { cb.checked = true; });
    },

    _cfSelectNone() {
        const pop = document.getElementById('cf-popover');
        if (pop) pop.querySelectorAll('input[type=checkbox]').forEach(cb => { cb.checked = false; });
    },

    _cfApply(id, colIdx) {
        const pop = document.getElementById('cf-popover');
        if (!pop) return;
        const s = PA.tf._state[id];
        if (!s) return;

        const selected = new Set();
        pop.querySelectorAll('input[type=checkbox]:checked').forEach(cb => selected.add(cb.value));

        // If all selected → remove filter
        if (selected.size >= PA.tf._cfValues(s, colIdx).length) {
            delete s.colFilters[colIdx];
        } else {
            s.colFilters[colIdx] = selected;
        }

        PA.tf._cfClose();
        PA.tf._cfUpdateIcons(id);
        if (s.src) { PA.tf._cfPush(id); return; }
        PA.tf.filter(id);
    },

    /** The text-mode popover: contains/regex, plus a case-sensitivity toggle. */
    _cfToggleText(id, colIdx, thEl) {
        PA.tf._cfClose();
        const s = PA.tf._state[id];
        if (!s || !s.colFilterDefs[colIdx]) return;
        const def = s.colFilterDefs[colIdx];
        const active = s.colFilters[colIdx] || {};
        const esc = PA.esc || (v => v);

        const rect = thEl.getBoundingClientRect();
        const pop = document.createElement('div');
        pop.className = 'cf-popover cf-popover-text';
        pop.id = 'cf-popover';
        pop.style.top = (rect.bottom + window.scrollY + 2) + 'px';
        pop.style.left = Math.max(0, rect.left + window.scrollX - 40) + 'px';

        const mode = active.mode === 'regex' ? 'regex' : 'contains';
        const value = active.value || '';
        const cs = !!active.cs;
        const radioName = 'cf-text-mode-' + id + '-' + colIdx;

        let html = `<div class="cf-header">${esc(def.label)}</div>`;
        html += '<div class="cf-text-modes">';
        html += `<label class="cf-item"><input type="radio" name="${radioName}" value="contains" ${mode === 'contains' ? 'checked' : ''}> Contains</label>`;
        html += `<label class="cf-item"><input type="radio" name="${radioName}" value="regex" ${mode === 'regex' ? 'checked' : ''}> Regex</label>`;
        html += '</div>';
        html += `<div class="cf-text-input"><input type="text" id="cf-text-value" value="${PA.escAttr ? PA.escAttr(value) : value}" placeholder="Filter value…" style="width:100%;box-sizing:border-box"></div>`;
        html += `<label class="cf-item"><input type="checkbox" id="cf-text-cs" ${cs ? 'checked' : ''}> Case sensitive</label>`;
        html += `<div class="cf-footer">`;
        html += `<button class="cf-apply" onclick="PA.tf._cfApplyText('${id}',${colIdx})">Apply</button>`;
        html += `<button class="cf-btn" onclick="PA.tf._cfClear('${id}',${colIdx})">Clear</button>`;
        html += `<button class="cf-cancel" onclick="PA.tf._cfClose()">Cancel</button>`;
        html += '</div>';
        pop.innerHTML = html;
        document.body.appendChild(pop);

        const input = pop.querySelector('#cf-text-value');
        if (input) {
            input.focus();
            input.addEventListener('keydown', e => { if (e.key === 'Enter') PA.tf._cfApplyText(id, colIdx); });
        }

        setTimeout(() => {
            PA.tf._cfOutsideHandler = (e) => {
                if (!pop.contains(e.target)) PA.tf._cfClose();
            };
            document.addEventListener('click', PA.tf._cfOutsideHandler, true);
        }, 0);
    },

    _cfApplyText(id, colIdx) {
        const pop = document.getElementById('cf-popover');
        if (!pop) return;
        const s = PA.tf._state[id];
        if (!s) return;
        const checkedRadio = pop.querySelector('input[type=radio]:checked');
        const mode = checkedRadio && checkedRadio.value === 'regex' ? 'regex' : 'contains';
        const valueEl = pop.querySelector('#cf-text-value');
        const value = ((valueEl && valueEl.value) || '').trim();
        const csEl = pop.querySelector('#cf-text-cs');
        const cs = !!(csEl && csEl.checked);

        if (!value) delete s.colFilters[colIdx];
        else s.colFilters[colIdx] = { mode, value, cs };

        PA.tf._cfClose();
        PA.tf._cfUpdateIcons(id);
        if (s.src) { PA.tf._cfPush(id); return; }
        PA.tf.filter(id);
    },

    /**
     * The values a column's popover offers.
     *
     * Server-paged tables read them from the facet summary; array-backed tables collect them from
     * the dataset they hold. Both the popover and the "everything is ticked, so drop the filter"
     * test go through here, so those two can never disagree about what "all" means.
     */
    _cfValues(s, colIdx) {
        if (s.src) {
            const vals = s.colFilterValues && s.colFilterValues[String(colIdx)];
            return vals ? vals.slice() : [];
        }
        const def = s.colFilterDefs[colIdx];
        if (!def) return [];
        const valSet = new Set();
        for (const item of s.data) {
            const v = def.valueFn(item);
            if (Array.isArray(v)) { for (const vi of v) { if (vi != null && vi !== '') valSet.add(String(vi)); } }
            else if (v != null && v !== '') valSet.add(String(v));
        }
        return [...valSet].sort();
    },

    /**
     * Sends the current column filters and reloads page 1.
     *
     * Checkbox columns send `cf=<col>:<v>|<v>`, unchanged. Text-mode columns send
     * `cf=<col>:contains:<value>` or `cf=<col>:regex:<value>`, with a trailing `:ci` when the
     * case-sensitive box is unchecked — see `ParserAnalysisController`'s facet layer for the
     * server-side half of this contract.
     */
    _cfPush(id) {
        const s = PA.tf._state[id];
        if (!s || !s.src) return;
        const cf = [];
        for (const [colIdx, val] of Object.entries(s.colFilters)) {
            const def = s.colFilterDefs[colIdx];
            if (def && def.mode === 'text') {
                if (!val || !val.value) continue;
                const mode = val.mode === 'regex' ? 'regex' : 'contains';
                let token = colIdx + ':' + mode + ':' + val.value;
                if (!val.cs) token += ':ci';
                cf.push(token);
                continue;
            }
            if (!val || !val.size) continue;
            cf.push(colIdx + ':' + [...val].join('|'));
        }
        s.src.setParam('cf', cf);
        PA.tf._restart(id);
    },

    _cfClear(id, colIdx) {
        const s = PA.tf._state[id];
        if (s) delete s.colFilters[colIdx];
        PA.tf._cfClose();
        PA.tf._cfUpdateIcons(id);
        if (s && s.src) { PA.tf._cfPush(id); return; }
        PA.tf.filter(id);
    },

    _cfUpdateIcons(id) {
        const s = PA.tf._state[id];
        if (!s) return;
        const tbody = document.getElementById(id + '-tbody');
        const table = tbody?.closest('table');
        if (!table) return;
        table.querySelectorAll('.cf-icon').forEach(icon => {
            const th = icon.closest('th');
            if (!th) return;
            const ci = th.dataset.sortCol != null ? parseInt(th.dataset.sortCol) : [...th.parentElement.children].indexOf(th);
            const val = s.colFilters[ci];
            const active = val && (val.size ? val.size > 0 : !!val.value);
            icon.classList.toggle('cf-active', !!active);
        });
    },

    // ==================== SORT INDICATORS ====================

    _updateSortIndicators(id) {
        const s = PA.tf._state[id];
        if (!s) return;
        const tbody = document.getElementById(id + '-tbody');
        const table = tbody?.closest('table');
        if (!table) return;
        table.querySelectorAll('th[data-sort-col]').forEach(th => {
            const ci = parseInt(th.dataset.sortCol);
            th.classList.remove('sort-asc', 'sort-desc');
            if (ci === s.sortCol) {
                th.classList.add(s.sortDir === 'desc' ? 'sort-desc' : 'sort-asc');
            }
        });
    },

    // ==================== PAGINATION ====================

    /**
     * The pager for server-paged mode.
     *
     * Narrower than the array pager on purpose: a cursor cannot jump to page 12, so only pages
     * already fetched plus the next one are offered, and when the server reports the total is not
     * exact we render "Page 3" with a next arrow instead of inventing "Page 3 of 17".
     */
    _serverPagerHtml(id, s) {
        const shown = (s.pageCache[s.page] || []).length;
        const start = s.page * s.pageSize;
        let html = '<div class="page-info">';
        if (s.loading) html += 'Loading…';
        else if (s.totalIsExact) html += s.serverTotal + ' items';
        else html += (shown ? (start + 1) : 0) + '–' + (start + shown);
        html += '</div>';

        html += `<div class="page-size"><select onchange="PA.tf.setPageSize('${id}',+this.value)">`;
        for (const sz of [10, 20, 50, 100]) {
            html += `<option value="${sz}" ${s.pageSize === sz ? 'selected' : ''}>${sz}/page</option>`;
        }
        html += '</select></div>';

        const busy = s.loading;
        html += '<div class="page-btns">';
        html += `<button onclick="PA.tf.goPage('${id}',0)" ${s.page <= 0 || busy ? 'disabled' : ''} title="First page">&laquo;</button>`;
        html += `<button onclick="PA.tf.goPage('${id}',${s.page - 1})" ${s.page <= 0 || busy ? 'disabled' : ''} title="Previous page">&lsaquo;</button>`;
        if (s.totalIsExact) {
            const totalPages = PA.tf._serverTotalPages(s);
            let startP = Math.max(0, s.page - 3), endP = Math.min(totalPages - 1, startP + 6);
            if (endP - startP < 6) startP = Math.max(0, endP - 6);
            for (let i = startP; i <= endP; i++) {
                const reachable = s.pageCache[i] !== undefined || i === s.highestPage + 1;
                html += `<button onclick="PA.tf.goPage('${id}',${i})" class="${i === s.page ? 'active' : ''}" `
                     + `${reachable && !busy ? '' : 'disabled'}>${i + 1}</button>`;
            }
        } else {
            html += `<button class="active" disabled>${s.page + 1}</button>`;
        }
        const canNext = s.hasMore || s.pageCache[s.page + 1] !== undefined;
        html += `<button onclick="PA.tf.goPage('${id}',${s.page + 1})" ${canNext && !busy ? '' : 'disabled'} title="Next page">&rsaquo;</button>`;
        html += '</div>';
        if (s.loadError) {
            html += '<div class="page-info" style="color:var(--red,#c00)">'
                + (PA.esc ? PA.esc(s.loadError) : s.loadError) + '</div>';
        }
        return html;
    },

    _renderPager(pagerId, total, totalPages, tableId) {
        const bar = document.getElementById(pagerId + '-pager') || document.getElementById(pagerId);
        if (!bar) return;
        const id = tableId || pagerId;
        const s = PA.tf._state[id];
        if (!s) return;

        // Show the pager whenever there is data (was: only when > 20 rows). This makes the
        // count + rows-per-page control visible for small lists too; the page nav buttons
        // are shown only when there is more than one page.
        if (total === 0) { bar.style.display = 'none'; return; }
        // The plsql page suppresses the pager for small tables (its fork used `total <= 20`);
        // the parser page always shows it. Preserved rather than unified, so this de-fork changes
        // no screen — see the note at the top of this file.
        if (total <= PA.tf.hidePagerBelow) { bar.style.display = 'none'; return; }
        bar.style.display = '';

        // Show pagination ABOVE the table (user preference): the pager markup sits
        // right after its table container in the HTML — move it to just before the
        // container, once. Generic across every tf table.
        if (!bar.dataset.movedTop) {
            var prev = bar.previousElementSibling;
            if (prev) bar.parentNode.insertBefore(bar, prev);
            bar.dataset.movedTop = '1';
        }

        if (s.src) { bar.innerHTML = PA.tf._serverPagerHtml(id, s); return; }

        const p = s.page;
        let html = '<div class="page-info">' + total + ' items</div>';
        html += `<div class="page-size"><select onchange="PA.tf.setPageSize('${id}',+this.value)">`;
        for (const sz of [10, 20, 50, 100]) {
            html += `<option value="${sz}" ${s.pageSize === sz ? 'selected' : ''}>${sz}/page</option>`;
        }
        html += `<option value="${total}" ${s.pageSize >= total ? 'selected' : ''}>All</option>`;
        html += '</select></div>';

        if (totalPages > 1) {
            html += '<div class="page-btns">';
            html += `<button onclick="PA.tf.goPage('${id}',0)" ${p <= 0 ? 'disabled' : ''} title="First page">&laquo;</button>`;
            html += `<button onclick="PA.tf.goPage('${id}',${p - 1})" ${p <= 0 ? 'disabled' : ''} title="Previous page">&lsaquo;</button>`;

            let startP = Math.max(0, p - 3), endP = Math.min(totalPages - 1, startP + 6);
            if (endP - startP < 6) startP = Math.max(0, endP - 6);
            for (let i = startP; i <= endP; i++) {
                html += `<button onclick="PA.tf.goPage('${id}',${i})" class="${i === p ? 'active' : ''}">${i + 1}</button>`;
            }

            html += `<button onclick="PA.tf.goPage('${id}',${p + 1})" ${p >= totalPages - 1 ? 'disabled' : ''} title="Next page">&rsaquo;</button>`;
            html += `<button onclick="PA.tf.goPage('${id}',${totalPages - 1})" ${p >= totalPages - 1 ? 'disabled' : ''} title="Last page">&raquo;</button>`;
            html += '</div>';
        }
        bar.innerHTML = html;
    }
};

/**
 * Explicit user direction: tag each table reference as an actual persistent table vs. a SQL
 * Server temp table / table variable, and surface that on screen — real objects found live while
 * investigating a table-count mismatch (SQLSERVER~SQLSERVERDEMO~LOCAL~USP_RUNMRPCYCLE): the
 * "tables" list mixed real tables with `#stage6` (local temp table), `##g103` (global temp table),
 * and `@ids105` (table variable) — all valid T-SQL, but a genuinely different kind of object from
 * a persistent database table, and worth telling apart on sight rather than guessing from the name.
 *
 * Classified purely from the name's prefix — this IS the actual signal T-SQL itself uses to
 * distinguish these object kinds, so no parser/backend change is needed to get this right; every
 * consumer (Tables tab, Global Search results) can call this directly against whatever name string
 * it already has.
 */
window.PA = window.PA || {};
PA.classifyTableKind = function(name) {
    const n = (name || '').trim();
    if (n.startsWith('##')) return { code: 'GLOBAL_TEMP', label: 'Global Temp', cls: 'kind-global-temp' };
    if (n.startsWith('#')) return { code: 'LOCAL_TEMP', label: 'Local Temp', cls: 'kind-local-temp' };
    if (n.startsWith('@')) return { code: 'VARIABLE', label: 'Table Var', cls: 'kind-variable' };
    return { code: 'TABLE', label: 'Table', cls: 'kind-table' };
};
