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
    _state: {},

    /**
     * Initialize a paginated table.
     * @param {string} id - unique table id (used to find DOM elements: {id}-tbody, {id}-pager, etc.)
     * @param {Array} data - full dataset
     * @param {number} pageSize - items per page
     * @param {Function} renderRow - (item, globalIndex, esc) => HTML string for <tr>
     * @param {Object} opts - see above
     */
    init(id, data, pageSize, renderRow, opts) {
        opts = opts || {};
        const s = {
            id,
            data: data || [],
            filtered: [...(data || [])],
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
            colFilterDefs: {}
        };
        PA.tf._state[id] = s;
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
    setData(id, data) {
        const s = PA.tf._state[id];
        if (!s) return;
        s.data = data || [];
        s.page = 0;
        PA.tf.filter(id);
    },

    /** Apply all filters (search, domain, column, extra) and re-render */
    filter(id) {
        const s = PA.tf._state[id];
        if (!s) return;
        const esc = PA.esc || (v => v);

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
            if (!selected || !selected.size) continue;
            const def = s.colFilterDefs[colIdx];
            if (!def) continue;
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
        if (s.sortCol === colIdx) {
            s.sortDir = s.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
            s.sortCol = colIdx;
            s.sortDir = 'asc';
        }
        PA.tf._updateSortIndicators(id);
        PA.tf.filter(id);
    },

    /** Navigate to page */
    goPage(id, p) {
        const s = PA.tf._state[id];
        if (!s) return;
        s.page = Math.max(0, Math.min(p, Math.ceil(s.filtered.length / s.pageSize) - 1));
        PA.tf.render(id);
    },

    /** Change page size */
    setPageSize(id, sz) {
        const s = PA.tf._state[id];
        if (!s) return;
        s.pageSize = sz;
        s.page = 0;
        PA.tf.render(id);
    },

    /** Render current page into DOM */
    render(id) {
        const s = PA.tf._state[id];
        if (!s) return;
        const esc = PA.esc || (v => v);

        const tbody = document.getElementById(id + '-tbody');
        if (!tbody) return;

        const total = s.filtered.length;
        const totalPages = Math.ceil(total / s.pageSize) || 1;
        const start = s.page * s.pageSize;
        const end = Math.min(start + s.pageSize, total);
        const pageData = s.filtered.slice(start, end);

        if (pageData.length === 0) {
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

        // Lazy render on first open
        if (row.dataset.lazy && s.renderDetail) {
            const item = s.filtered[idx];
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

    // ==================== COLUMN FILTERS ====================

    /**
     * Register column filters for a table.
     * @param {string} id - table id
     * @param {Object} columns - { colIdx: { label, valueFn: item => string|string[] } }
     */
    initColFilters(id, columns) {
        const s = PA.tf._state[id];
        if (!s) return;
        s.colFilterDefs = columns;
        s.colFilters = {};

        const FILTER_SVG = '<svg viewBox="0 0 16 16" width="11" height="11" fill="currentColor" style="vertical-align:-1px;margin-left:3px"><path d="M1 2h14l-5 6v5l-4 2V8z"/></svg>';

        // Find the table element
        const tbody = document.getElementById(id + '-tbody');
        const table = tbody?.closest('table');
        if (!table) return;

        // Attach filter icons to headers
        table.querySelectorAll('th').forEach((th, thIdx) => {
            const ci = th.dataset.sortCol != null ? parseInt(th.dataset.sortCol) : thIdx;
            if (!columns[ci]) return;
            if (th.querySelector('.cf-icon')) return;
            th.style.position = 'relative';
            const btn = document.createElement('span');
            btn.className = 'cf-icon';
            btn.title = 'Filter ' + columns[ci].label;
            btn.innerHTML = FILTER_SVG;
            btn.onclick = (e) => { e.stopPropagation(); PA.tf._cfToggle(id, ci, th); };
            th.appendChild(btn);
        });
    },

    _cfToggle(id, colIdx, thEl) {
        PA.tf._cfClose();
        const s = PA.tf._state[id];
        if (!s || !s.colFilterDefs[colIdx]) return;
        const def = s.colFilterDefs[colIdx];

        // Collect unique values from full dataset
        const valSet = new Set();
        for (const item of s.data) {
            const v = def.valueFn(item);
            if (Array.isArray(v)) { for (const vi of v) { if (vi != null && vi !== '') valSet.add(String(vi)); } }
            else if (v != null && v !== '') valSet.add(String(v));
        }
        const allValues = [...valSet].sort();
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
        const def = s.colFilterDefs[colIdx];
        const totalValues = new Set();
        for (const item of s.data) {
            const v = def.valueFn(item);
            if (Array.isArray(v)) { for (const vi of v) { if (vi != null && vi !== '') totalValues.add(String(vi)); } }
            else if (v != null && v !== '') totalValues.add(String(v));
        }
        if (selected.size >= totalValues.size) {
            delete s.colFilters[colIdx];
        } else {
            s.colFilters[colIdx] = selected;
        }

        PA.tf._cfClose();
        PA.tf._cfUpdateIcons(id);
        PA.tf.filter(id);
    },

    _cfClear(id, colIdx) {
        const s = PA.tf._state[id];
        if (s) delete s.colFilters[colIdx];
        PA.tf._cfClose();
        PA.tf._cfUpdateIcons(id);
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
            icon.classList.toggle('cf-active', !!(s.colFilters[ci] && s.colFilters[ci].size));
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

    _renderPager(pagerId, total, totalPages, tableId) {
        const bar = document.getElementById(pagerId + '-pager') || document.getElementById(pagerId);
        if (!bar) return;
        const id = tableId || pagerId;
        const s = PA.tf._state[id];
        if (!s) return;

        if (total <= 20) { bar.style.display = 'none'; return; }
        bar.style.display = '';

        const p = s.page;
        let html = '<div class="page-info">' + total + ' items</div>';
        html += `<div class="page-size"><select onchange="PA.tf.setPageSize('${id}',+this.value)">`;
        for (const sz of [20, 50, 100]) {
            html += `<option value="${sz}" ${s.pageSize === sz ? 'selected' : ''}>${sz}/page</option>`;
        }
        html += `<option value="${total}" ${s.pageSize >= total ? 'selected' : ''}>All</option>`;
        html += '</select></div>';

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
        bar.innerHTML = html;
    }
};
