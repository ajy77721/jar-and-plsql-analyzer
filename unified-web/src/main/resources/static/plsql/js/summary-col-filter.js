/**
 * Summary column filter — click-header popover with checkboxes.
 * Integrates with _initPage pagination framework in summary-helpers.js.
 * Domain-agnostic — identical to JAR version but uses PA namespace.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    /**
     * Register column filters for a paginated table.
     * Call AFTER _initPage and AFTER the table HTML is in the DOM.
     * @param {string} id - table id (e.g. 'sum-proc')
     * @param {Object} columns - { colIdx: { label, valueFn: item => string|string[] } }
     */
    _initColFilters(id, columns) {
        const s = this._pageState[id];
        if (!s) return;
        s.colFilters = {};
        s.colFilterDefs = columns;

        const tbody = document.getElementById(id + '-tbody');
        const table = tbody?.closest('table');
        if (!table) return;

        const FILTER_SVG = '<svg viewBox="0 0 16 16" width="12" height="12" fill="currentColor" style="vertical-align:-1px"><path d="M1 2h14l-5 6v5l-4 2V8z"/></svg>';

        table.querySelectorAll('th[data-sort-col]').forEach(th => {
            const ci = parseInt(th.dataset.sortCol);
            if (!columns[ci]) return;
            const btn = document.createElement('span');
            btn.className = 'cf-icon';
            btn.title = 'Filter ' + columns[ci].label;
            btn.innerHTML = FILTER_SVG;
            btn.onclick = (e) => { e.stopPropagation(); this._cfToggle(id, ci, th); };
            th.appendChild(btn);
        });

        table.querySelectorAll('th:not([data-sort-col])').forEach((th, thIdx) => {
            const allThs = [...table.querySelectorAll('thead th')];
            const ci = allThs.indexOf(th);
            if (ci < 0 || !columns[ci]) return;
            if (th.querySelector('.cf-icon')) return;
            th.style.position = 'relative';
            const btn = document.createElement('span');
            btn.className = 'cf-icon';
            btn.title = 'Filter ' + columns[ci].label;
            btn.innerHTML = FILTER_SVG;
            btn.onclick = (e) => { e.stopPropagation(); this._cfToggle(id, ci, th); };
            th.appendChild(btn);
        });
    },

    _cfToggle(id, colIdx, thEl) {
        this._cfClose();

        const s = this._pageState[id];
        if (!s || !s.colFilterDefs[colIdx]) return;
        const def = s.colFilterDefs[colIdx];

        const valSet = new Set();
        for (const item of s.data) {
            const v = def.valueFn(item);
            if (Array.isArray(v)) { for (const vi of v) { if (vi) valSet.add(String(vi)); } }
            else if (v != null && v !== '') valSet.add(String(v));
        }
        const allValues = [...valSet].sort();
        if (!allValues.length) return;

        const active = s.colFilters[colIdx];

        const rect = thEl.getBoundingClientRect();
        const pop = document.createElement('div');
        pop.className = 'cf-popover';
        pop.id = 'cf-popover';
        pop.style.top = (rect.bottom + window.scrollY + 2) + 'px';
        pop.style.left = Math.max(0, rect.left + window.scrollX - 40) + 'px';

        let html = `<div class="cf-header">${PA.esc(def.label)}</div>`;
        html += '<div class="cf-actions"><button class="cf-btn" onclick="PA.summary._cfSelectAll()">All</button>';
        html += '<button class="cf-btn" onclick="PA.summary._cfSelectNone()">None</button></div>';
        html += '<div class="cf-list">';
        for (const v of allValues) {
            const checked = !active || active.has(v) ? 'checked' : '';
            html += `<label class="cf-item"><input type="checkbox" value="${PA.esc(v)}" ${checked}><span>${PA.esc(v)}</span></label>`;
        }
        html += '</div>';
        html += `<div class="cf-footer">`;
        html += `<button class="cf-apply" onclick="PA.summary._cfApply('${id}',${colIdx})">Apply</button>`;
        html += `<button class="cf-btn" onclick="PA.summary._cfClear('${id}',${colIdx})">Clear</button>`;
        html += `<button class="cf-cancel" onclick="PA.summary._cfClose()">Cancel</button>`;
        html += '</div>';
        pop.innerHTML = html;
        document.body.appendChild(pop);

        setTimeout(() => {
            this._cfOutsideHandler = (e) => {
                if (!pop.contains(e.target)) this._cfClose();
            };
            document.addEventListener('click', this._cfOutsideHandler, true);
        }, 0);
    },

    _cfClose() {
        const pop = document.getElementById('cf-popover');
        if (pop) pop.remove();
        if (this._cfOutsideHandler) {
            document.removeEventListener('click', this._cfOutsideHandler, true);
            this._cfOutsideHandler = null;
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
        const s = this._pageState[id];
        if (!s) return;

        const selected = new Set();
        pop.querySelectorAll('input[type=checkbox]:checked').forEach(cb => selected.add(cb.value));

        const def = s.colFilterDefs[colIdx];
        const totalValues = new Set();
        for (const item of s.data) {
            const v = def.valueFn(item);
            if (Array.isArray(v)) { for (const vi of v) { if (vi) totalValues.add(String(vi)); } }
            else if (v != null && v !== '') totalValues.add(String(v));
        }

        if (selected.size >= totalValues.size) {
            delete s.colFilters[colIdx];
        } else {
            s.colFilters[colIdx] = selected;
        }

        this._cfClose();
        this._cfUpdateIcons(id);
        this._pageFilter(id);
    },

    _cfClear(id, colIdx) {
        const s = this._pageState[id];
        if (s) delete s.colFilters[colIdx];
        this._cfClose();
        this._cfUpdateIcons(id);
        this._pageFilter(id);
    },

    _cfUpdateIcons(id) {
        const s = this._pageState[id];
        if (!s) return;
        const tbody = document.getElementById(id + '-tbody');
        const table = tbody?.closest('table');
        if (!table) return;
        table.querySelectorAll('.cf-icon').forEach(icon => {
            const th = icon.closest('th');
            const ci = th?.dataset?.sortCol != null ? parseInt(th.dataset.sortCol) : -1;
            if (ci < 0) {
                const allThs = [...table.querySelectorAll('thead th')];
                const idx = allThs.indexOf(th);
                icon.classList.toggle('cf-active', !!(s.colFilters[idx] && s.colFilters[idx].size));
            } else {
                icon.classList.toggle('cf-active', !!(s.colFilters[ci] && s.colFilters[ci].size));
            }
        });
    }
});
