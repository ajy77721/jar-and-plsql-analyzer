/**
 * Summary helpers — shared utility methods used across all summary components.
 * Adds methods to JA.summary (must load before other summary-*.js files).
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    toggleDetail(prefix, idx) {
        const row = document.getElementById('sum-' + prefix + '-detail-' + idx);
        if (!row) return;
        // Lazy render on first open (paginated tables only)
        if (row.dataset.lazy) {
            const tableId = 'sum-' + prefix;
            const s = this._pageState[tableId];
            if (s && s.renderDetail) {
                const item = s.data[idx];
                if (item) {
                    const esc = JA.utils.escapeHtml;
                    const detailHtml = s.renderDetail(item, idx, esc);
                    const tmp = document.createElement('tbody');
                    tmp.innerHTML = detailHtml;
                    if (tmp.firstElementChild) {
                        row.innerHTML = tmp.firstElementChild.innerHTML;
                        // Copy class/colspan from rendered row
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

    /** Stat chip click — open detail section + scroll to relevant block */
    _chipDetail(idx, section) {
        const detail = document.getElementById('sum-ep-detail-' + idx);
        if (!detail) return;
        // Open if closed
        if (detail.style.display === 'none') detail.style.display = '';
        // Map section key to scroll target id prefix
        const targetMap = {
            colls: 'ep-colls-', views: 'ep-views-', svc: 'ep-svc-',
            ext: 'ep-ext-', http: 'ep-http-', paths: 'ep-paths-'
        };
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

    /** Deterministic HSL color for a collection name — same name always same color */
    _collColor(name) {
        if (!name) return '#94a3b8';
        let h = 0;
        for (let i = 0; i < name.length; i++) h = ((h << 5) - h + name.charCodeAt(i)) | 0;
        h = ((h % 360) + 360) % 360;
        return 'hsl(' + h + ', 55%, 45%)';
    },

    /** Light background variant for collection color */
    _collBg(name) {
        if (!name) return '#f1f5f9';
        let h = 0;
        for (let i = 0; i < name.length; i++) h = ((h << 5) - h + name.charCodeAt(i)) | 0;
        h = ((h % 360) + 360) % 360;
        return 'hsl(' + h + ', 60%, 93%)';
    },

    _walkWithPath(node, cb, path) {
        if (!node) return;
        const cur = path ? [...path, node] : [node];
        cb(node, cur);
        for (const child of (node.children || [])) this._walkWithPath(child, cb, cur);
    },

    _buildBreadcrumb(path, maxDepth) {
        const start = Math.max(0, path.length - maxDepth);
        const segs = [];
        for (let i = start; i < path.length; i++) {
            const n = path[i];
            segs.push({
                label: (n.simpleClassName || '?') + '.' + (n.methodName || '?'),
                full: (n.className || '') + '.' + (n.methodName || ''),
                className: n.className || n.simpleClassName || '',
                methodName: n.methodName || '',
                jar: n.sourceJar || 'main',
                isExternal: i > 0 && (n.sourceJar || null) !== (path[i - 1].sourceJar || null),
                level: i + 1
            });
        }
        return segs;
    },

    _renderBc(bc, esc) {
        const chainIdx = this._registerChain ? this._registerChain(bc) : -1;
        return bc.map((seg, si) => {
            let cls = 'sum-bc-seg sum-clickable';
            if (seg.isExternal) cls += ' sum-bc-external';
            const lvl = seg.level || (si + 1);
            const levelBadge = `<span class="sum-bc-level">L${lvl}</span>`;
            let onclick;
            if (chainIdx >= 0) {
                onclick = `JA.summary.showBcCode(${chainIdx},${si});event.stopPropagation()`;
            } else {
                const ck = (seg.className || '').replace(/'/g, "\\'");
                const mk = (seg.methodName || '').replace(/'/g, "\\'");
                onclick = `JA.summary.showClassCode('${ck}','${mk}');event.stopPropagation()`;
            }
            return (si > 0 ? '<span class="sum-bc-arrow">&rarr;</span>' : '') +
                levelBadge +
                `<span class="${cls}" title="${esc(seg.full || '')}" onclick="${onclick}">${esc(seg.label)}</span>`;
        }).join('');
    },

    _inferOp(methodName, stereotype) {
        if (!methodName) return null;
        const n = methodName.toLowerCase();
        if (stereotype === 'LOGGING' || stereotype === 'JDK') return null;
        // Data classes: no stereotype, ENTITY (@Document/@Entity), or OTHER (plain POJOs)
        // set/get prefixes on these are in-memory bean mutations, NOT database operations
        const isDataClass = !stereotype || stereotype === 'ENTITY' || stereotype === 'OTHER';
        for (const [opType, prefixes] of Object.entries(this._operationTypes || {})) {
            for (const prefix of prefixes) {
                if (n.startsWith(prefix)) {
                    if (isDataClass && (prefix === 'set' || prefix === 'get')) continue;
                    return opType;
                }
            }
        }
        return null;
    },

    /** Weighted complexity scoring from domain-config complexity_rules */
    _calcComplexity(c) {
        const rules = this._complexityRules;
        if (!rules || !rules.factors) {
            return c.usageCount <= 2 ? 'Low' : c.usageCount <= 5 ? 'Medium' : 'High';
        }
        const t = rules.thresholds || { low_max: 4, medium_max: 10 };
        const hasPipeline = c.detectedVia && (c.detectedVia.has('PIPELINE_RUNTIME') || c.detectedVia.has('AGGREGATION_API') || c.detectedVia.has('TEMPLATE_AGGREGATE'));
        // Factor value lookup by id
        const vals = {
            endpoints:      c.endpoints ? c.endpoints.size : 0,
            read_ops:       c.readOps ? c.readOps.size : 0,
            write_ops:      c.writeOps ? c.writeOps.size : 0,
            aggregate_ops:  c.operations && c.operations.has('AGGREGATE') ? 1 : 0,
            cross_domain:   c.domainSet && c.domainSet.size > 1 ? c.domainSet.size - 1 : 0,
            pipeline_usage: hasPipeline ? 1 : 0,
            usage_count:    c.usageCount || 0
        };
        let score = 0;
        for (const f of rules.factors) {
            const v = vals[f.id] || 0;
            score += v * (f.weight || 0);
        }
        c._complexityScore = Math.round(score * 10) / 10;
        return score <= t.low_max ? 'Low' : score <= t.medium_max ? 'Medium' : 'High';
    },

    _classifyColl(name) {
        if (!name) return 'COLLECTION';
        for (const s of (this._viewContains || [])) { if (name.includes(s)) return 'VIEW'; }
        for (const s of (this._viewStartsWith || [])) { if (name.startsWith(s)) return 'VIEW'; }
        return 'COLLECTION';
    },

    _detectDomain(name) {
        if (!name) return 'Other';
        if (name.startsWith('SB_') && this._sbDomainMap) {
            for (const [p, d] of Object.entries(this._sbDomainMap)) { if (name.startsWith(p)) return d; }
        }
        let n = name;
        if (n.startsWith('SB_')) n = n.substring(3);
        if (this._prefixList) {
            for (const [prefix, domain] of this._prefixList) { if (n.startsWith(prefix)) return domain; }
        }
        return 'Other';
    },

    _extractCollRefs(str) {
        const refs = new Set();
        if (!str) return refs;
        let m;
        for (const sw of (this._viewStartsWith || [])) {
            const escaped = sw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const re = new RegExp('\\b(' + escaped + '[A-Z][A-Z0-9_]+)\\b', 'g');
            while ((m = re.exec(str)) !== null) refs.add(m[1]);
        }
        for (const vc of (this._viewContains || [])) {
            const escaped = vc.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const re = new RegExp('\\b([A-Z][A-Z0-9]*' + escaped + '[A-Z0-9_]*)\\b', 'g');
            while ((m = re.exec(str)) !== null) refs.add(m[1]);
        }
        const genP = /\b([A-Z]{2,6}_[A-Z][A-Z0-9_]{2,})\b/g;
        while ((m = genP.exec(str)) !== null) { if (this._detectDomain(m[1]) !== 'Other') refs.add(m[1]); }
        for (const ref of this._extractPipelineCollRefs(str)) refs.add(ref);
        return refs;
    },

    _extractPipelineCollRefs(str) {
        const refs = new Set();
        if (!str) return refs;
        let m;
        const lookupRe = /\$lookup[^}]*["']from["']\s*:\s*["']([A-Z][A-Z0-9_]+)["']/g;
        while ((m = lookupRe.exec(str)) !== null) { if (this._detectDomain(m[1]) !== 'Other') refs.add(m[1]); }
        const outRe = /\$out["']?\s*:\s*["']([A-Z][A-Z0-9_]+)["']/g;
        while ((m = outRe.exec(str)) !== null) { if (this._detectDomain(m[1]) !== 'Other') refs.add(m[1]); }
        const outCollRe = /\$out[^}]*["']coll["']\s*:\s*["']([A-Z][A-Z0-9_]+)["']/g;
        while ((m = outCollRe.exec(str)) !== null) { if (this._detectDomain(m[1]) !== 'Other') refs.add(m[1]); }
        const mergeRe = /\$merge["']?\s*:\s*["']([A-Z][A-Z0-9_]+)["']/g;
        while ((m = mergeRe.exec(str)) !== null) { if (this._detectDomain(m[1]) !== 'Other') refs.add(m[1]); }
        const mergeIntoRe = /\$merge[^}]*["']into["']\s*:\s*["']([A-Z][A-Z0-9_]+)["']/g;
        while ((m = mergeIntoRe.exec(str)) !== null) { if (this._detectDomain(m[1]) !== 'Other') refs.add(m[1]); }
        return refs;
    },

    _jarToProject(jarName) {
        if (!jarName || jarName === 'main') {
            // Use POM artifactId for main JAR if available, else derive from uploaded JAR filename
            if (this._jarModuleMap && this._jarModuleMap['main']) return this._jarModuleMap['main'];
            const fn = JA.app?.currentAnalysis?.jarName;
            if (fn) {
                let n = fn.replace(/-\d[\d.]*(?:-SNAPSHOT)?\.jar$/i, '').replace(/\.jar$/i, '');
                return n.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
            }
            return 'Primary';
        }
        // Prefer backend-provided POM artifactId over filename parsing
        if (this._jarModuleMap && this._jarModuleMap[jarName]) return this._jarModuleMap[jarName];
        let name = jarName.replace(/-\d[\d.]*(?:-SNAPSHOT)?\.jar$/i, '').replace(/\.jar$/i, '');
        return name.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
    },

    _jarToDomain(jarName) {
        if (!jarName || jarName === 'main') {
            // Use uploaded JAR filename for domain derivation
            if (this._jarModuleMap && this._jarModuleMap['main']) {
                const name = this._jarModuleMap['main'].toLowerCase();
                const ignore = this._jarIgnoreSegments || new Set();
                const parts = name.split('-').filter(p => !ignore.has(p));
                if (parts.length) return parts.map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
            }
            return 'Core';
        }
        // Use POM artifactId for domain derivation if available
        let name;
        if (this._jarModuleMap && this._jarModuleMap[jarName]) {
            name = this._jarModuleMap[jarName].toLowerCase();
        } else {
            name = jarName.replace(/-\d[\d.]*(?:-SNAPSHOT)?\.jar$/i, '').replace(/\.jar$/i, '').toLowerCase();
        }
        const ignore = this._jarIgnoreSegments || new Set();
        const parts = name.split('-').filter(p => !ignore.has(p));
        if (!parts.length) return name.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
        return parts.map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
    },

    /** Build a colored collection badge with consistent color per name */
    _collBadge(name, esc, extra, verification) {
        const isView = this._classifyColl ? this._classifyColl(name) === 'VIEW' : false;
        const bg = this._collBg(name);
        const border = this._collColor(name);
        const cls = isView ? 'sum-coll-view' : 'sum-coll-data';
        let verifIcon = '';
        if (verification === 'CLAUDE_ADDED') verifIcon = '<span class="sum-verif sum-verif-added" title="Added by Claude">+</span>';
        else if (verification === 'CLAUDE_VERIFIED') verifIcon = '<span class="sum-verif sum-verif-claude-ok" title="Verified by Claude">&#10003;</span>';
        else if (verification === 'VERIFIED') verifIcon = '<span class="sum-verif sum-verif-ok" title="Verified in MongoDB">&#10003;</span>';
        else if (verification === 'NOT_IN_DB') verifIcon = '<span class="sum-verif sum-verif-warn" title="Not found in MongoDB catalog">&#9888;</span>';
        else if (verification === 'CLAUDE_REMOVED') verifIcon = '<span class="sum-verif sum-verif-removed" title="Removed by Claude">&#10007;</span>';
        return `<span class="sum-coll-badge ${cls}" style="background:${bg};border-left:3px solid ${border}" title="${esc(name)}">${verifIcon}${esc(name)}${extra || ''}</span>`;
    },

    /** Rich collection badge with verification + pipeline ($lookup) indicator */
    _collBadgeRich(name, esc, collInfo) {
        const verification = collInfo ? collInfo.verification : null;
        let extra = '';
        if (collInfo && collInfo.sources) extra = ` <span class="sum-ref-count">(${collInfo.sources.size})</span>`;
        const isPipeline = collInfo && collInfo.detectedVia &&
            (collInfo.detectedVia.has('PIPELINE_RUNTIME') || collInfo.detectedVia.has('AGGREGATION_API') || collInfo.detectedVia.has('TEMPLATE_AGGREGATE'));
        if (isPipeline) extra += '<span class="sum-pipeline-badge" title="$lookup pipeline collection">$lookup</span>';
        return this._collBadge(name, esc, extra, verification);
    },

    /** Helper: scrollable + searchable section wrapper for detail views */
    _scrollSection(id) {
        return `<input type="text" class="sum-scroll-filter" placeholder="Search..." oninput="JA.summary._filterScrollSection(this)" data-target="${id}"><div class="sum-scroll-wrap" id="${id}">`;
    },

    /** Filter items inside a scroll section by text */
    _filterScrollSection(input) {
        const id = input.dataset.target;
        const wrap = document.getElementById(id);
        if (!wrap) return;
        const q = input.value.toLowerCase();
        wrap.querySelectorAll('.sum-scroll-item').forEach(el => {
            el.style.display = el.textContent.toLowerCase().includes(q) ? '' : 'none';
        });
    },

    _classifyEndpointType(ep) {
        const text = ((ep.fullPath || '') + ' ' + (ep.methodName || '')).toLowerCase();
        for (const kw of (this._batchKeywords || [])) {
            if (text.includes(kw)) return 'BATCH';
        }
        return 'REST';
    },

    _sizeCategory(m) {
        const t = this._sizeThresholds || { S: 5, M: 20, L: 50 };
        if (m <= (t.S || 5)) return 'S';
        if (m <= (t.M || 20)) return 'M';
        if (m <= (t.L || 50)) return 'L';
        return 'XL';
    },

    _perfImpl(d) {
        const thresholds = this._perfThresholds || [];
        for (const t of thresholds) {
            if (d <= t.maxDbOps) return t.label;
        }
        return thresholds.length ? thresholds[thresholds.length - 1].label : 'Unknown';
    },

    _addColl(collections, name, operation, source, forceDomain, detectedVia, verification, methodName) {
        if (!name) return;
        if (!collections[name]) {
            let domain = forceDomain || this._detectDomain(name);
            // Keep high-confidence sources even without domain match; drop low-confidence
            if (domain === 'Other' && !forceDomain) {
                // Only STRING_LITERAL and FIELD_CONSTANT are low confidence (could be random UPPER_CASE strings).
                // All other detection sources are specific to MongoDB operations and high confidence.
                const lowConf = !detectedVia || detectedVia === 'STRING_LITERAL' || detectedVia === 'FIELD_CONSTANT';
                if (!lowConf) domain = 'Unclassified';
                else return;
            }
            collections[name] = { name, type: this._classifyColl(name), domain, operations: new Set(), sources: new Set(), sourceMethodMap: {}, detectedVia: new Set(), verification: null };
        }
        if (operation) collections[name].operations.add(operation);
        if (source) collections[name].sources.add(source);
        if (source && methodName && !collections[name].sourceMethodMap[source]) {
            collections[name].sourceMethodMap[source] = methodName;
        }
        if (detectedVia) collections[name].detectedVia.add(detectedVia);
        if (verification && verification !== 'NO_CATALOG') {
            collections[name].verification = verification; // CLAUDE_* overrides static
        }
    },

    _addCollBc(map, collName, path) {
        if (!map[collName]) map[collName] = [];
        if (map[collName].length >= 3) return;
        const bc = this._buildBreadcrumb(path, 5);
        const key = bc.map(b => b.label).join('>');
        for (const e of map[collName]) { if (e.map(b => b.label).join('>') === key) return; }
        map[collName].push(bc);
    },

    _countTreeNodes(node) {
        if (!node) return 0;
        let count = 1;
        if (node.children) for (const c of node.children) count += this._countTreeNodes(c);
        return count;
    },

    _collectTreeCollections(node) {
        const set = new Set();
        this._walkTreeCollections(node, set);
        return [...set];
    },

    _walkTreeCollections(node, set) {
        if (!node) return;
        if (node.collectionsAccessed) for (const c of node.collectionsAccessed) set.add(c);
        if (node.children) for (const ch of node.children) this._walkTreeCollections(ch, set);
    },

    _countExternalCalls(node) {
        if (!node) return 0;
        let count = node.crossModule ? 1 : 0;
        if (node.children) for (const c of node.children) count += this._countExternalCalls(c);
        return count;
    },

    _toCamelCase(className) {
        if (!className) return '?';
        return className.charAt(0).toLowerCase() + className.slice(1);
    },

    _shortType(fqn) {
        if (!fqn) return '?';
        return fqn.split('.').pop();
    },

    /* ========== Reusable Pagination + Sort + Filter ========== */

    _pageState: {},

    /**
     * Initialize pagination for a table.
     * @param {string} id - unique table id (e.g. 'sum-vert-table')
     * @param {Array} data - full dataset
     * @param {number} pageSize - rows per page (default 25)
     * @param {Function} renderRowFn - (item, originalIndex, esc) => HTML string
     * @param {Function} domainFn - (item) => domain string (for dropdown filter)
     * @param {Function} detailFn - (item, originalIndex, esc) => detail row HTML (optional)
     * @param {Object} [opts] - { sortKeys: [{key, fn}], defaultSort }
     */
    _initPage(id, data, pageSize, renderRowFn, domainFn, detailFn, opts) {
        opts = opts || {};
        data.forEach((item, i) => {
            item._origIdx = i;
            if (!item._searchText) {
                item._searchText = this._buildSearchText(item);
            }
        });
        this._pageState[id] = {
            data: data,
            filtered: [...data],
            page: 0,
            pageSize: pageSize || 25,
            renderRow: renderRowFn,
            renderDetail: detailFn || null,
            domainFn: domainFn || null,
            textQuery: '',
            domainFilter: '',
            sortCol: opts.defaultSort || -1,
            sortDir: 'asc',
            sortKeys: opts.sortKeys || null,
            typeFilterFn: opts.typeFilterFn || null
        };
    },

    _buildSearchText(item) {
        const parts = [];
        for (const [k, v] of Object.entries(item)) {
            if (k.startsWith('_') || v == null || typeof v === 'object') continue;
            parts.push(String(v));
        }
        if (item.endpoint) {
            const ep = item.endpoint;
            if (ep.fullPath) parts.push(ep.fullPath);
            if (ep.methodName) parts.push(ep.methodName);
            if (ep.controllerClass) parts.push(ep.controllerClass);
            if (ep.controllerSimpleName) parts.push(ep.controllerSimpleName);
        }
        if (item.collections) {
            if (Array.isArray(item.collections)) parts.push(item.collections.join(' '));
            else if (item.collections instanceof Set) parts.push([...item.collections].join(' '));
        }
        if (item.domains) {
            if (Array.isArray(item.domains)) parts.push(item.domains.join(' '));
        }
        if (item.operationTypes && Array.isArray(item.operationTypes)) parts.push(item.operationTypes.join(' '));
        return parts.join(' ').toLowerCase();
    },

    _pageRender(id) {
        const s = this._pageState[id];
        if (!s) return;
        const esc = JA.utils.escapeHtml;
        const start = s.page * s.pageSize;
        const pageData = s.filtered.slice(start, start + s.pageSize);
        const totalPages = Math.max(1, Math.ceil(s.filtered.length / s.pageSize));

        // Render rows
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

        // Build pager HTML (shared by top + bottom)
        const pagerHtml = this._buildPagerHtml(id, s, start, totalPages);

        // Render top pager
        const topPager = document.getElementById(id + '-pager-top');
        if (topPager) topPager.innerHTML = pagerHtml;

        // Render bottom pager
        const pager = document.getElementById(id + '-pager');
        if (pager) pager.innerHTML = pagerHtml;

        // Update sort arrows on table headers
        this._updateSortHeaders(id, s);
    },

    _buildPagerHtml(id, s, start, totalPages) {
        const showing = `${Math.min(start + 1, s.filtered.length)}-${Math.min(start + s.pageSize, s.filtered.length)} of ${s.filtered.length}`;
        let html = `<span class="sum-pager-info">${showing}</span>`;

        // Page size selector
        html += `<select class="sum-pager-size" onchange="JA.summary._pageSizeChange('${id}',parseInt(this.value))">`;
        for (const sz of [10, 25, 50, 100]) {
            html += `<option value="${sz}"${sz === s.pageSize ? ' selected' : ''}>${sz} / page</option>`;
        }
        html += '</select>';

        html += `<button class="sum-pager-btn" onclick="JA.summary._pageGo('${id}',${s.page - 1})" ${s.page <= 0 ? 'disabled' : ''}>&laquo;</button>`;
        // Page numbers (show up to 7)
        const maxBtns = 7;
        let pStart = Math.max(0, s.page - 3);
        let pEnd = Math.min(totalPages, pStart + maxBtns);
        if (pEnd - pStart < maxBtns) pStart = Math.max(0, pEnd - maxBtns);
        for (let p = pStart; p < pEnd; p++) {
            html += `<button class="sum-pager-btn${p === s.page ? ' active' : ''}" onclick="JA.summary._pageGo('${id}',${p})">${p + 1}</button>`;
        }
        html += `<button class="sum-pager-btn" onclick="JA.summary._pageGo('${id}',${s.page + 1})" ${s.page >= totalPages - 1 ? 'disabled' : ''}>&raquo;</button>`;
        // Page dropdown
        if (totalPages > 1) {
            html += `<select class="sum-pager-select" onchange="JA.summary._pageGo('${id}',parseInt(this.value))">`;
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
        const tf = s._typeFilter || '';
        const cf = s.colFilters || {};
        s.filtered = s.data.filter(item => {
            if (d && s.domainFn && s.domainFn(item) !== d) return false;
            if (tf) {
                const itemType = s.typeFilterFn ? s.typeFilterFn(item) : (item.type || '');
                if (itemType !== tf) return false;
            }
            // Verification filter
            const vf = s._verifFilter || '';
            if (vf === 'verified' && item.verification !== 'VERIFIED' && item.verification !== 'CLAUDE_VERIFIED') return false;
            if (vf === 'not_in_db' && item.verification !== 'NOT_IN_DB') return false;
            if (vf === 'other' && (item.verification === 'VERIFIED' || item.verification === 'CLAUDE_VERIFIED' || item.verification === 'NOT_IN_DB')) return false;
            if (q) {
                const text = item._searchText || '';
                if (!text.includes(q)) return false;
            }
            // Column filters (checkbox popovers)
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
            // Range filters (min/max on numeric fields)
            if (s._rangeFilters) {
                for (const [key, range] of Object.entries(s._rangeFilters)) {
                    const val = item[key] || 0;
                    if (range.min !== null && val < range.min) return false;
                    if (range.max !== null && val > range.max) return false;
                }
            }
            // Category filters (checkbox sets)
            if (s._catFilters) {
                for (const [key, allowed] of Object.entries(s._catFilters)) {
                    if (!allowed || !allowed.size) continue;
                    if (key === 'operationTypes') {
                        const ops = item.operationTypes || [];
                        if (!ops.some(op => allowed.has(op))) return false;
                    } else {
                        const v = item[key];
                        if (v != null && !allowed.has(String(v))) return false;
                    }
                }
            }
            return true;
        });
        // Re-apply current sort after filter
        if (s.sortCol >= 0 && s.sortKeys && s.sortKeys[s.sortCol]) {
            this._applySortInternal(s);
        }
        s.page = 0;
        this._pageRender(id);
        if (s.onFilter) s.onFilter(s);
    },

    /* --- Sort --- */
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
            const domEl = document.getElementById(id + '-filter-domain');
            s.textQuery = textEl ? textEl.value : '';
            s.domainFilter = domEl ? domEl.value : '';
            this._pageFilter(id);
        }, 200);
    },

    _clearFilter(id) {
        const s = this._pageState[id];
        if (!s) return;
        const textEl = document.getElementById(id + '-filter-text');
        const domEl = document.getElementById(id + '-filter-domain');
        if (textEl) textEl.value = '';
        if (domEl) domEl.value = '';
        s.textQuery = '';
        s.domainFilter = '';
        s._typeFilter = '';
        s._verifFilter = '';
        s.colFilters = {};
        s._rangeFilters = {};
        s._catFilters = {};
        s.sortCol = -1;
        s.sortDir = 'asc';
        s.filtered = [...s.data];
        s.page = 0;
        this._pageRender(id);
        if (s.onFilter) s.onFilter(s);
        if (this._cfUpdateIcons) this._cfUpdateIcons(id);
        // Reset advanced filter panel inputs (if present)
        const advPanel = document.getElementById('ep-adv-panel');
        if (advPanel && id === 'sum-ep') {
            advPanel.querySelectorAll('input[type=number]').forEach(el => { el.value = ''; });
            advPanel.querySelectorAll('input[type=checkbox]').forEach(cb => { cb.checked = true; });
            const badge = document.getElementById('ep-adv-badge');
            if (badge) badge.style.display = 'none';
        }
        // Reset type and verification toggle buttons
        document.querySelectorAll('[data-type].sum-type-btn').forEach(b => b.classList.toggle('active', b.dataset.type === ''));
        document.querySelectorAll('[data-verif].sum-type-btn').forEach(b => b.classList.toggle('active', b.dataset.verif === ''));
        // Reset dynamic flow type pills
        document.querySelectorAll('#dyn-type-pills .dyn-pill').forEach(p => p.classList.toggle('active', p.dataset.dtype === ''));
        if (JA.summary._dynActiveType !== undefined) JA.summary._dynActiveType = '';
    },

    /** Build filter bar HTML (text input + domain dropdown + clear) */
    _buildFilterBar(id, data, domainFn) {
        const domains = new Set();
        if (domainFn) { for (const item of data) { const d = domainFn(item); if (d) domains.add(d); } }
        const sortedDomains = [...domains].sort();

        let html = '<div class="sum-filter-bar">';
        html += `<input type="text" class="sum-filter-input" id="${id}-filter-text" placeholder="Filter..." oninput="JA.summary._applyFilter('${id}')">`;
        if (sortedDomains.length > 1) {
            html += `<select class="sum-filter-select" id="${id}-filter-domain" onchange="JA.summary._applyFilter('${id}')">`;
            html += '<option value="">All Domains</option>';
            for (const d of sortedDomains) html += `<option value="${JA.utils.escapeHtml(d)}">${JA.utils.escapeHtml(d)}</option>`;
            html += '</select>';
        }
        html += `<button class="sum-filter-clear" onclick="JA.summary._clearFilter('${id}')" title="Clear all filters &amp; sort">Clear</button>`;
        html += '</div>';
        return html;
    },

    /* ========== Tooltip Descriptions ========== */

    _opTip: {
        READ:      'READ — find, get, query, search, fetch, load operations',
        WRITE:     'WRITE — save, insert, persist, create, store operations',
        UPDATE:    'UPDATE — update, modify, set, patch, replace operations',
        DELETE:    'DELETE — delete, remove, clear, purge, drop operations',
        AGGREGATE: 'AGGREGATE — aggregation pipeline, graphLookup, mapReduce',
        COUNT:     'COUNT — count, exists, estimatedDocumentCount',
        CALL:      'CALL — stored procedure/function call via JDBC, JPA, or callable statement'
    },

    _complexityTip(label, score) {
        const formula = 'Calculation: (endpoints × 2) + (read_ops × 1) + (write_ops × 2) + (aggregate × 3) + (cross_domain × 3) + (pipeline × 2) + (usage × 0.5)';
        const base = label === 'High'
            ? 'High complexity (score > 10) — Many endpoints, cross-domain access, aggregation pipelines, or heavy write operations. Needs dedicated migration planning.'
            : label === 'Medium'
            ? 'Medium complexity (score 4-10) — Multiple endpoints or mixed read/write operations. Moderate migration effort.'
            : 'Low complexity (score ≤ 4) — Few endpoints, simple operations, single domain. Straightforward to migrate.';
        return (score != null ? base + ' Score: ' + score + '. ' : base + ' ') + formula;
    },

    _verifTip(label) {
        return label === 'Verified' ? 'Verified — Collection confirmed to exist in the MongoDB catalog (case-insensitive match against live database)'
            : label === 'Ambiguous' ? 'Ambiguous — Collection not found in MongoDB catalog. May be dynamic, renamed, or from a different environment. Detected via bytecode pattern but not confirmed in DB.'
            : 'Unknown — No MongoDB catalog connected for verification. Upload JAR with MongoDB connection string or use "Fetch Catalog" to enable.';
    },

    _typeTip(type) {
        return type === 'VIEW' ? 'VIEW — MongoDB view (read-only, based on aggregation pipeline on a base collection). Detected by naming convention from domain-config.'
            : 'COLL — Regular MongoDB collection (supports read and write operations). Detected via @Document, MongoTemplate calls, repository mapping, or bytecode analysis.';
    },

    _sizeTip(cat) {
        return cat === 'S' ? 'Small — ≤ 5 methods in call tree. Calculation: count all unique method calls from endpoint entry point through the entire call graph.'
            : cat === 'M' ? 'Medium — 6-20 methods in call tree. Calculation: count all unique method calls from endpoint entry point through the entire call graph.'
            : cat === 'L' ? 'Large — 21-50 methods in call tree. Calculation: count all unique method calls from endpoint entry point through the entire call graph.'
            : 'Extra Large — 50+ methods in call tree. Calculation: count all unique method calls from endpoint entry point through the entire call graph.';
    },

    _perfTip(label) {
        if (!label) return '';
        const calc = ' Calculation: based on total DB operation count (find, save, update, delete, aggregate calls) in the endpoint call tree.';
        if (label.includes('No Impact')) return 'No Impact — No database operations detected in this endpoint.' + calc;
        if (label.includes('Low')) return 'Low Impact — ≤ 5 DB operations, minimal latency.' + calc;
        if (label.includes('Medium')) return 'Medium Impact — 6-20 DB operations, estimated 100-200ms additional latency.' + calc;
        if (label.includes('High')) return 'High Impact — 20+ DB operations, estimated 200ms+ additional latency.' + calc;
        return label;
    },

    _detectedTip(code) {
        const tips = {
            'REPOSITORY_MAPPING':      'Repository — Spring Data repository interface mapped to this collection',
            'DOCUMENT_ANNOTATION':     '@Document — Class annotated with @Document(collection=...)',
            'STRING_LITERAL':          'String Literal — Collection name found as a string constant in bytecode',
            'FIELD_CONSTANT':          'Field Constant — Collection name found in a static/final field',
            'QUERY_ANNOTATION':        '@Query — Collection referenced in @Query or @Aggregation annotation value',
            'PIPELINE_ANNOTATION':     'Pipeline — Collection referenced in aggregation pipeline annotation',
            'BULK_WRITE_COLLECTOR':    'BulkWrite — Detected via BulkOperations/BulkWrite collector pattern',
            'MONGO_TEMPLATE':          'MongoTemplate — Direct MongoTemplate.find/save/update call',
            'NATIVE_DRIVER':           'Native Driver — Direct MongoDB Java driver API call',
            'NATIVE_COMMAND':          'runCommand — MongoDB runCommand invocation',
            'REACTIVE_MONGO_TEMPLATE': 'Reactive — ReactiveMongoTemplate operation',
            'GRIDFS':                  'GridFS — GridFS file storage operation',
            'CHANGE_STREAM':           'ChangeStream — MongoDB change stream subscription',
            'AGGREGATION_API':         'Aggregation API — Programmatic aggregation using Aggregation.newAggregation()',
            'PIPELINE_RUNTIME':        'Pipeline Runtime — Runtime-constructed aggregation pipeline stages',
            'TEMPLATE_AGGREGATE':      'Template Agg — MongoTemplate.aggregate() call',
            'PIPELINE_FIELD_REF':      'Field Ref — Collection name referenced in pipeline field mapping',
            'CLAUDE':                  'Claude — Collection identified or added by Claude AI analysis'
        };
        return tips[code] || code;
    }
});
