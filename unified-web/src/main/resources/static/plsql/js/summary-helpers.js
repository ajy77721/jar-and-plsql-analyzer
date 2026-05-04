/**
 * Summary helpers — shared utility methods used across all summary components.
 * Adds methods to PA.summary (must load before other summary-*.js files).
 *
 * PL/SQL Analyzer adaptation: procedures/tables/schemas instead of endpoints/collections/domains.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    toggleDetail(prefix, idx) {
        const row = document.getElementById('sum-' + prefix + '-detail-' + idx);
        if (!row) return;
        if (row.dataset.lazy) {
            const tableId = 'sum-' + prefix;
            const s = this._pageState[tableId];
            if (s && s.renderDetail) {
                const item = s.data[idx];
                if (item) {
                    const esc = PA.esc;
                    const detailHtml = s.renderDetail(item, idx, esc);
                    const tmp = document.createElement('tbody');
                    tmp.innerHTML = detailHtml;
                    if (tmp.firstElementChild) {
                        row.innerHTML = tmp.firstElementChild.innerHTML;
                        for (const attr of tmp.firstElementChild.attributes) {
                            if (attr.name !== 'style' && attr.name !== 'id') row.setAttribute(attr.name, attr.value);
                        }
                    }
                }
            }
            delete row.dataset.lazy;
        }
        row.style.display = row.style.display === 'none' ? '' : 'none';
    },

    _chipDetail(idx, section) {
        const detail = document.getElementById('sum-proc-detail-' + idx);
        if (!detail) return;
        if (detail.style.display === 'none') detail.style.display = '';
        const targetMap = { tables: 'proc-tables-', calls: 'proc-calls-', ext: 'proc-ext-', schema: 'proc-schema-' };
        const targetId = targetMap[section];
        if (targetId) {
            const el = document.getElementById(targetId + idx);
            if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); el.classList.add('sum-flash'); setTimeout(() => el.classList.remove('sum-flash'), 1200); }
        }
    },

    _debounceTimers: {},
    _debounce(key, fn, ms) {
        clearTimeout(this._debounceTimers[key]);
        this._debounceTimers[key] = setTimeout(fn, ms);
    },

    filterRows(tableId, query) {
        this._debounce('fr-' + tableId, () => {
            const table = document.getElementById(tableId);
            if (!table) return;
            const q = query.toLowerCase().trim();
            table.querySelectorAll('tbody tr').forEach(row => {
                if (row.classList.contains('sum-detail-row')) return;
                row.style.display = !q || row.textContent.toLowerCase().includes(q) ? '' : 'none';
            });
        }, 200);
    },

    /** Deterministic HSL color for a schema name */
    _schemaColor(name) {
        if (!name) return '#94a3b8';
        let h = 0;
        for (let i = 0; i < name.length; i++) h = ((h << 5) - h + name.charCodeAt(i)) | 0;
        h = ((h % 360) + 360) % 360;
        return 'hsl(' + h + ', 55%, 45%)';
    },

    _schemaBg(name) {
        if (!name) return '#f1f5f9';
        let h = 0;
        for (let i = 0; i < name.length; i++) h = ((h << 5) - h + name.charCodeAt(i)) | 0;
        h = ((h % 360) + 360) % 360;
        return 'hsl(' + h + ', 60%, 93%)';
    },

    /** Walk a PL/SQL call tree recursively with path tracking */
    _walkWithPath(node, cb, path) {
        if (!node) return;
        const cur = path ? [...path, node] : [node];
        cb(node, cur);
        if (!node.circular) {
            for (const child of (node.children || [])) this._walkWithPath(child, cb, cur);
        }
    },

    /** Build breadcrumb from call path */
    _buildBreadcrumb(path, maxDepth) {
        const start = Math.max(0, path.length - maxDepth);
        const segs = [];
        for (let i = start; i < path.length; i++) {
            const n = path[i];
            segs.push({
                label: (n.name || n.id || '?'),
                full: (n.schemaName || '') + '.' + (n.packageName || '') + '.' + (n.name || ''),
                schemaName: n.schemaName || '',
                packageName: n.packageName || '',
                name: n.name || '',
                callType: n.callType || 'INTERNAL',
                isExternal: n.callType === 'EXTERNAL',
                level: i + 1
            });
        }
        return segs;
    },

    /** Infer operation type from SQL statement types */
    _inferOp(node) {
        if (!node) return null;
        const stmts = node.statementCounts || {};
        if (stmts.INSERT > 0 || stmts.UPDATE > 0 || stmts.DELETE > 0) return 'WRITE';
        if (stmts.MERGE > 0) return 'UPSERT';
        if (stmts.SELECT > 0) return 'READ';
        // Check table operations
        if (node.tables && node.tables.length) {
            const ops = new Set();
            for (const t of node.tables) {
                for (const op of (t.operations || [])) ops.add(op.toUpperCase());
            }
            if (ops.has('INSERT') || ops.has('UPDATE') || ops.has('DELETE')) return 'WRITE';
            if (ops.has('MERGE')) return 'UPSERT';
            if (ops.has('SELECT')) return 'READ';
        }
        return null;
    },

    /** Categorize procedure by lines of code */
    _sizeCategory(loc) {
        if (loc <= 50) return 'S';
        if (loc <= 200) return 'M';
        if (loc <= 500) return 'L';
        return 'XL';
    },

    /** Weighted complexity calculation for a procedure report */
    _calcComplexity(report) {
        if (!report) return 'Low';
        let score = 0;
        score += (report.tableCount || 0) * 1;
        score += (report.writeTableCount || 0) * 2;
        score += (report.crossSchemaCalls || 0) * 3;
        score += (report.loc || 0) > 200 ? 2 : 0;
        score += (report.callDepth || 0) > 5 ? 2 : 0;
        score += (report.totalCalls || 0) > 20 ? 1 : 0;
        report._complexityScore = Math.round(score * 10) / 10;
        if (score <= 4) return 'Low';
        if (score <= 10) return 'Medium';
        return 'High';
    },

    /** Count tree nodes */
    _countTreeNodes(node) {
        if (!node) return 0;
        let count = 1;
        if (node.children && !node.circular) {
            for (const c of node.children) count += this._countTreeNodes(c);
        }
        return count;
    },

    /** Collect all tables accessed in tree */
    _collectTreeTables(node) {
        const set = new Set();
        this._walkTreeTables(node, set);
        return [...set];
    },

    _walkTreeTables(node, set) {
        if (!node) return;
        if (node.tables) for (const t of node.tables) set.add(t.tableName || t);
        if (node.nodeTables) for (const t of node.nodeTables) set.add(t.tableName || t);
        if (node.children && !node.circular) {
            for (const ch of node.children) this._walkTreeTables(ch, set);
        }
    },

    /** Count external (cross-schema) calls in tree */
    _countExternalCalls(node) {
        if (!node) return 0;
        let count = (node.callType === 'EXTERNAL') ? 1 : 0;
        if (node.children && !node.circular) {
            for (const c of node.children) count += this._countExternalCalls(c);
        }
        return count;
    },

    /** Build a colored schema badge */
    _schemaBadge(name, esc) {
        const bg = this._schemaBg(name);
        const border = this._schemaColor(name);
        return `<span class="sum-schema-badge" style="background:${bg};border-left:3px solid ${border}">${esc(name || 'unknown')}</span>`;
    },

    /** Build a colored table badge */
    _tableBadge(name, esc, ops) {
        const bg = this._schemaBg(name);
        const border = this._schemaColor(name);
        let extra = '';
        if (ops && ops.length) {
            extra = ' ' + ops.map(op => `<span class="op-badge ${op}">${op}</span>`).join('');
        }
        return `<span class="sum-table-badge" style="background:${bg};border-left:3px solid ${border}">${esc(name || 'unknown')}${extra}</span>`;
    },

    /** Scrollable + searchable section wrapper */
    _scrollSection(id) {
        return `<input type="text" class="sum-scroll-filter" placeholder="Search..." oninput="PA.summary._filterScrollSection(this)" data-target="${id}" style="width:100%;padding:4px 8px;margin-bottom:4px;font-size:11px;border:1px solid var(--border);border-radius:4px"><div class="sum-scroll-wrap" id="${id}" style="max-height:200px;overflow-y:auto">`;
    },

    _filterScrollSection(input) {
        const id = input.dataset.target;
        const wrap = document.getElementById(id);
        if (!wrap) return;
        const q = input.value.toLowerCase();
        wrap.querySelectorAll('.sum-scroll-item').forEach(el => {
            el.style.display = el.textContent.toLowerCase().includes(q) ? '' : 'none';
        });
    },

    /* ========== Reusable Pagination + Sort + Filter ========== */

    _pageState: {},

    _initPage(id, data, pageSize, renderRowFn, schemaFn, detailFn, opts) {
        opts = opts || {};
        data.forEach((item, i) => { item._origIdx = i; });
        this._pageState[id] = {
            data: data,
            filtered: [...data],
            page: 0,
            pageSize: pageSize || 25,
            renderRow: renderRowFn,
            renderDetail: detailFn || null,
            domainFn: schemaFn || null,
            textQuery: '',
            domainFilter: '',
            sortCol: opts.defaultSort || -1,
            sortDir: 'asc',
            sortKeys: opts.sortKeys || null
        };
    },

    _pageRender(id) {
        const s = this._pageState[id];
        if (!s) return;
        const esc = PA.esc;
        const start = s.page * s.pageSize;
        const pageData = s.filtered.slice(start, start + s.pageSize);
        const totalPages = Math.max(1, Math.ceil(s.filtered.length / s.pageSize));

        const tbody = document.getElementById(id + '-tbody');
        if (tbody) {
            let html = '';
            pageData.forEach((item, pi) => {
                const origIdx = item._origIdx != null ? item._origIdx : s.data.indexOf(item);
                html += s.renderRow(item, origIdx, esc);
                if (s.renderDetail) {
                    html += `<tr class="sum-detail-row" id="${id}-detail-${origIdx}" style="display:none" data-lazy="1"><td></td></tr>`;
                }
            });
            tbody.innerHTML = html;
        }

        const pagerHtml = this._buildPagerHtml(id, s, start, totalPages);
        const topPager = document.getElementById(id + '-pager-top');
        if (topPager) topPager.innerHTML = pagerHtml;
        const pager = document.getElementById(id + '-pager');
        if (pager) pager.innerHTML = pagerHtml;

        this._updateSortHeaders(id, s);
    },

    _buildPagerHtml(id, s, start, totalPages) {
        const showing = `${Math.min(start + 1, s.filtered.length)}-${Math.min(start + s.pageSize, s.filtered.length)} of ${s.filtered.length}`;
        let html = `<span class="page-info">${showing}</span>`;
        html += `<select class="page-size" onchange="PA.summary._pageSizeChange('${id}',parseInt(this.value))" style="font-size:11px;padding:2px 4px;border-radius:4px;border:1px solid var(--border)">`;
        for (const sz of [10, 25, 50, 100]) {
            html += `<option value="${sz}"${sz === s.pageSize ? ' selected' : ''}>${sz} / page</option>`;
        }
        html += '</select>';
        html += `<span class="page-btns">`;
        html += `<button onclick="PA.summary._pageGo('${id}',${s.page - 1})" ${s.page <= 0 ? 'disabled' : ''}>&laquo;</button>`;
        const maxBtns = 7;
        let pStart = Math.max(0, s.page - 3);
        let pEnd = Math.min(totalPages, pStart + maxBtns);
        if (pEnd - pStart < maxBtns) pStart = Math.max(0, pEnd - maxBtns);
        for (let p = pStart; p < pEnd; p++) {
            html += `<button class="${p === s.page ? 'active' : ''}" onclick="PA.summary._pageGo('${id}',${p})">${p + 1}</button>`;
        }
        html += `<button onclick="PA.summary._pageGo('${id}',${s.page + 1})" ${s.page >= totalPages - 1 ? 'disabled' : ''}>&raquo;</button>`;
        html += '</span>';
        if (totalPages > 1) {
            html += `<select onchange="PA.summary._pageGo('${id}',parseInt(this.value))" style="font-size:11px;padding:2px 4px;border-radius:4px;border:1px solid var(--border)">`;
            for (let p = 0; p < totalPages; p++) {
                html += `<option value="${p}"${p === s.page ? ' selected' : ''}>Page ${p + 1}</option>`;
            }
            html += '</select>';
        }
        return html;
    },

    _pageGo(id, page) {
        const s = this._pageState[id];
        if (!s) return;
        const totalPages = Math.max(1, Math.ceil(s.filtered.length / s.pageSize));
        s.page = Math.max(0, Math.min(page, totalPages - 1));
        this._pageRender(id);
    },

    _pageSizeChange(id, size) {
        const s = this._pageState[id];
        if (!s) return;
        s.pageSize = size;
        s.page = 0;
        this._pageRender(id);
    },

    _pageFilter(id) {
        const s = this._pageState[id];
        if (!s) return;
        const q = s.textQuery.toLowerCase().trim();
        const d = s.domainFilter;
        const cf = s.colFilters || {};
        s.filtered = s.data.filter(item => {
            if (d && s.domainFn && s.domainFn(item) !== d) return false;
            if (q) {
                const text = JSON.stringify(item, (k, v) => v instanceof Set ? [...v] : v).toLowerCase();
                if (!text.includes(q)) return false;
            }
            for (const [ci, allowed] of Object.entries(cf)) {
                if (!allowed || !allowed.size) continue;
                const def = s.colFilterDefs && s.colFilterDefs[ci];
                if (!def) continue;
                const v = def.valueFn(item);
                if (Array.isArray(v)) {
                    if (!v.some(vi => allowed.has(String(vi)))) return false;
                } else {
                    if (!allowed.has(String(v ?? ''))) return false;
                }
            }
            return true;
        });
        if (s.sortCol >= 0 && s.sortKeys && s.sortKeys[s.sortCol]) {
            this._applySortInternal(s);
        }
        s.page = 0;
        this._pageRender(id);
        if (s.onFilter) s.onFilter(s);
    },

    _pageSort(id, colIdx) {
        const s = this._pageState[id];
        if (!s || !s.sortKeys || !s.sortKeys[colIdx]) return;
        if (s.sortCol === colIdx) {
            s.sortDir = s.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
            s.sortCol = colIdx;
            s.sortDir = 'asc';
        }
        this._applySortInternal(s);
        s.page = 0;
        this._pageRender(id);
    },

    _applySortInternal(s) {
        const fn = s.sortKeys[s.sortCol].fn;
        const dir = s.sortDir === 'asc' ? 1 : -1;
        s.filtered.sort((a, b) => {
            const va = fn(a), vb = fn(b);
            if (va == null && vb == null) return 0;
            if (va == null) return dir;
            if (vb == null) return -dir;
            if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
            return String(va).localeCompare(String(vb)) * dir;
        });
    },

    _updateSortHeaders(id, s) {
        const tbody = document.getElementById(id + '-tbody');
        const table = tbody?.closest('table');
        if (!table) return;
        table.querySelectorAll('th[data-sort-col]').forEach(th => {
            const ci = parseInt(th.dataset.sortCol);
            th.classList.remove('sort-asc', 'sort-desc', 'sort-active');
            if (ci === s.sortCol) {
                th.classList.add('sort-active', s.sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        });
    },

    _applyFilter(id) {
        this._debounce('af-' + id, () => {
            const s = this._pageState[id];
            if (!s) return;
            const textEl = document.getElementById(id + '-filter-text');
            const domEl = document.getElementById(id + '-filter-schema');
            s.textQuery = textEl ? textEl.value : '';
            s.domainFilter = domEl ? domEl.value : '';
            this._pageFilter(id);
        }, 200);
    },

    _clearFilter(id) {
        const s = this._pageState[id];
        if (!s) return;
        const textEl = document.getElementById(id + '-filter-text');
        const domEl = document.getElementById(id + '-filter-schema');
        if (textEl) textEl.value = '';
        if (domEl) domEl.value = '';
        s.textQuery = '';
        s.domainFilter = '';
        s.colFilters = {};
        s.sortCol = -1;
        s.sortDir = 'asc';
        s.filtered = [...s.data];
        s.page = 0;
        this._pageRender(id);
        if (s.onFilter) s.onFilter(s);
        if (this._cfUpdateIcons) this._cfUpdateIcons(id);
    },

    /** Build filter bar HTML (text input + schema dropdown + clear) */
    _buildFilterBar(id, data, schemaFn) {
        const schemas = new Set();
        if (schemaFn) { for (const item of data) { const d = schemaFn(item); if (d) schemas.add(d); } }
        const sorted = [...schemas].sort();

        let html = '<div class="pagination-bar" style="border-top:none;border-bottom:1px solid var(--border);justify-content:flex-start;gap:8px">';
        html += `<input type="text" class="form-input" id="${id}-filter-text" placeholder="Filter..." oninput="PA.summary._applyFilter('${id}')" style="max-width:200px;height:28px;font-size:12px">`;
        if (sorted.length > 1) {
            html += `<select class="form-select" id="${id}-filter-schema" onchange="PA.summary._applyFilter('${id}')" style="height:28px;font-size:12px">`;
            html += '<option value="">All Schemas</option>';
            for (const d of sorted) html += `<option value="${PA.esc(d)}">${PA.esc(d)}</option>`;
            html += '</select>';
        }
        html += `<button class="btn btn-sm" onclick="PA.summary._clearFilter('${id}')" title="Clear all filters &amp; sort">Clear</button>`;
        html += '</div>';
        return html;
    },

    /* ========== Tooltip Descriptions ========== */

    _opTip: {
        SELECT:    'SELECT — Query/read operation on the table',
        INSERT:    'INSERT — Insert new rows into the table',
        UPDATE:    'UPDATE — Modify existing rows in the table',
        DELETE:    'DELETE — Remove rows from the table',
        MERGE:     'MERGE — Upsert operation (insert or update)',
        READ:      'READ — Data retrieval via SELECT or cursor',
        WRITE:     'WRITE — Data modification (INSERT/UPDATE/DELETE)',
        UPSERT:    'UPSERT — Merge or insert-on-duplicate operation'
    },

    _complexityTip(label, score) {
        const formula = 'Calculation: (tables x 1) + (write_tables x 2) + (cross_schema x 3) + (LOC>200 ? 2 : 0) + (depth>5 ? 2 : 0) + (calls>20 ? 1 : 0)';
        const base = label === 'High'
            ? 'High complexity (score > 10) — Many tables, cross-schema access, large code base.'
            : label === 'Medium'
            ? 'Medium complexity (score 4-10) — Multiple tables or mixed DML operations.'
            : 'Low complexity (score <= 4) — Few tables, simple operations, single schema.';
        return (score != null ? base + ' Score: ' + score + '. ' : base + ' ') + formula;
    },

    _sizeTip(cat) {
        return cat === 'S' ? 'Small — 50 or fewer lines of code.'
            : cat === 'M' ? 'Medium — 51-200 lines of code.'
            : cat === 'L' ? 'Large — 201-500 lines of code.'
            : 'Extra Large — 500+ lines of code.';
    },

    _unitTypeTip(type) {
        return type === 'PROCEDURE' ? 'Stored procedure — executable PL/SQL block'
            : type === 'FUNCTION' ? 'Stored function — returns a value'
            : type === 'TRIGGER' ? 'Database trigger — fires on DML events'
            : type === 'PACKAGE' ? 'Package — container for related procedures and functions'
            : type || 'Unknown unit type';
    },

    /* ========== Procedure Detail Popup ========== */

    showProcPopup(idx) {
        const r = (this._procReports || [])[idx];
        if (!r) return;
        const esc = PA.esc;

        let old = document.getElementById('proc-popup-overlay');
        if (old) old.remove();

        let html = '';

        // Header
        const unitCls = r.unitType === 'FUNCTION' ? 'F' : r.unitType === 'TRIGGER' ? 'T' : 'P';
        const complexCls = r.complexity === 'High' ? 'red' : r.complexity === 'Medium' ? 'orange' : 'green';
        html += '<div class="pp-header">';
        html += '<span class="lp-icon ' + unitCls + '" style="display:inline-flex;font-size:18px">' + unitCls + '</span>';
        html += '<strong class="pp-name">' + esc(r.name) + '</strong>';
        html += '<span style="color:' + this._schemaColor(r.schemaName) + ';font-weight:600">' + esc(r.schemaName) + '</span>';
        if (r.packageName) html += '<span style="color:var(--text-muted);font-size:11px">' + esc(r.packageName) + '</span>';
        html += '<span class="badge" style="background:var(--badge-' + complexCls + '-bg);color:var(--badge-' + complexCls + ')">' + r.complexity + '</span>';
        html += '<button class="btn btn-sm" onclick="document.getElementById(\'proc-popup-overlay\').remove()" style="margin-left:auto">Close</button>';
        html += '</div>';

        // Stats row
        html += '<div class="pp-stats">';
        html += '<span class="pp-stat"><b>' + r.tableCount + '</b> Tables</span>';
        html += '<span class="pp-stat"><b>' + (r.writeTableCount || 0) + '</b> Write</span>';
        html += '<span class="pp-stat"><b>' + (r.readTableCount || 0) + '</b> Read</span>';
        html += '<span class="pp-stat"><b>' + r.loc + '</b> LOC</span>';
        html += '<span class="pp-stat"><b>' + (r.crossSchemaCalls || 0) + '</b> Cross-Schema</span>';
        html += '<span class="pp-stat"><b>' + (r.totalCalls || 0) + '</b> Calls</span>';
        for (const op of (r.allOps || [])) html += '<span class="op-badge ' + op + '">' + op + '</span> ';
        html += '</div>';

        // Actions bar
        html += '<div class="pp-actions">';
        html += '<button class="btn btn-sm" onclick="PA.summary.showTrace(' + idx + ')">Trace</button>';
        html += '<button class="btn btn-sm" onclick="PA.summary.showCallTrace(' + idx + ')">Explore</button>';
        html += '<button class="btn btn-sm" onclick="PA.summary.showExportModal({procIdx:' + idx + '})">Export</button>';
        html += '</div>';

        // Content sections in scrollable body
        html += '<div class="pp-body">';

        // Parameters
        if (r.parameters && r.parameters.length) {
            html += '<div class="pp-section">';
            html += '<div class="pp-section-title">Parameters (' + r.parameters.length + ')</div>';
            html += '<div class="pp-section-content">';
            for (const p of r.parameters) {
                html += '<div class="dh-param dh-param-' + (p.mode || 'IN').toLowerCase().replace(/\s/g, '') + '">';
                html += '<span class="dh-param-mode">' + esc(p.mode || 'IN') + '</span>';
                html += '<span class="dh-param-name">' + esc(p.name || '?') + '</span>';
                html += '<span class="dh-param-type">' + esc(p.dataType || '?') + '</span>';
                html += '</div> ';
            }
            html += '</div></div>';
        }

        // Variables
        if (r.variables && r.variables.length) {
            const shown = r.variables.slice(0, 10);
            const more = r.variables.length - shown.length;
            html += '<div class="pp-section">';
            html += '<div class="pp-section-title">Variables (' + r.variables.length + ')</div>';
            html += '<div class="pp-section-content">';
            for (const v of shown) {
                html += '<div style="display:flex;gap:8px;padding:2px 0;font-size:11px">';
                html += '<span style="font-weight:600;font-family:var(--font-mono)">' + esc(v.name || '?') + '</span>';
                html += '<span style="color:var(--text-muted)">' + esc(v.dataType || '?') + '</span>';
                html += '</div>';
            }
            if (more > 0) html += '<div style="font-size:10px;color:var(--text-muted);padding:4px 0">+' + more + ' more...</div>';
            html += '</div></div>';
        }

        // Statement counts
        const stmts = r.statementCounts || {};
        const stmtKeys = Object.keys(stmts).filter(k => stmts[k] > 0);
        if (stmtKeys.length) {
            html += '<div class="pp-section">';
            html += '<div class="pp-section-title">Statements (' + stmtKeys.reduce((s, k) => s + stmts[k], 0) + ')</div>';
            html += '<div class="pp-section-content" style="display:flex;gap:6px;flex-wrap:wrap">';
            for (const k of stmtKeys.sort((a, b) => stmts[b] - stmts[a])) {
                html += '<span class="dh-stmt-badge"><b>' + stmts[k] + '</b> ' + esc(k) + '</span>';
            }
            html += '</div></div>';
        }

        // Direct Tables
        const directKeys = Object.keys(r.directTables || {});
        if (directKeys.length) {
            html += '<div class="pp-section">';
            html += '<div class="pp-section-title">Tables (' + directKeys.length + ')</div>';
            html += '<div class="pp-section-content" style="display:flex;gap:4px;flex-wrap:wrap">';
            for (const name of directKeys) {
                const t = r.directTables[name];
                const ops = [...(t.operations || [])];
                html += this._tableBadge(name, esc, ops) + ' ';
            }
            html += '</div></div>';
        }

        // Flow Tables (if different from direct)
        const flowKeys = Object.keys(r.flowTables || {});
        if (flowKeys.length > directKeys.length) {
            html += '<div class="pp-section">';
            html += '<div class="pp-section-title">All Tables in Flow (' + flowKeys.length + ')</div>';
            html += '<div class="pp-section-content" style="display:flex;gap:4px;flex-wrap:wrap">';
            for (const name of flowKeys) {
                const t = r.flowTables[name];
                const ops = [...(t.operations || [])];
                html += this._tableBadge(name, esc, ops);
                if (t.external) html += ' <span class="scope-badge ext">EXT</span>';
                html += ' ';
            }
            html += '</div></div>';
        }

        // Calls
        if (r.calls && r.calls.length) {
            html += '<div class="pp-section">';
            html += '<div class="pp-section-title">Calls (' + r.calls.length + ')</div>';
            html += '<div class="pp-section-content">';
            for (const c of r.calls) {
                const badge = c.callType === 'EXTERNAL'
                    ? '<span class="lp-type-badge EXTERNAL" style="margin-right:4px">EXT</span>'
                    : '<span class="lp-type-badge INTERNAL" style="margin-right:4px">INT</span>';
                html += '<div style="padding:2px 0;font-size:11px">' + badge;
                html += '<span style="color:' + this._schemaColor(c.schemaName) + ';font-weight:600">' + esc(c.schemaName || '') + '</span>';
                if (c.packageName) html += '.' + esc(c.packageName);
                html += '.<strong>' + esc(c.name || c.id || '?') + '</strong>';
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Called By
        if (r.calledBy && r.calledBy.length) {
            html += '<div class="pp-section">';
            html += '<div class="pp-section-title">Called By (' + r.calledBy.length + ')</div>';
            html += '<div class="pp-section-content">';
            for (const c of r.calledBy) {
                html += '<div style="padding:2px 0;font-size:11px"><strong>' + esc(c.name || c.id || '?') + '</strong></div>';
            }
            html += '</div></div>';
        }

        html += '</div>'; // pp-body

        const overlay = document.createElement('div');
        overlay.id = 'proc-popup-overlay';
        overlay.className = 'pp-overlay';
        overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
        overlay.innerHTML = '<div class="pp-panel">' + html + '</div>';
        document.body.appendChild(overlay);
    }
});
