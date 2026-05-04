/**
 * Summary vertical — Verticalisation verification report.
 * Identifies cross-module direct method calls that should become REST API calls.
 * Shows: which beans/collections are accessed directly across module boundaries,
 * the owning domain, and affected endpoints.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _buildVertVerification(epReports) {
        // Collect all cross-module direct calls grouped by target bean
        const beanMap = {};
        const collCrossMap = {};
        const primaryDomain = this._jarToDomain('main');

        for (const r of epReports) {
            // Cross-module method calls (direct, should be REST)
            for (const ext of (r.externalCalls || [])) {
                const key = (ext.className || ext.simpleClassName || '') + '.' + (ext.methodName || '');
                if (!beanMap[key]) {
                    beanMap[key] = {
                        className: ext.className || ext.simpleClassName || '',
                        simpleClassName: ext.simpleClassName || '',
                        methodName: ext.methodName || '',
                        stereotype: ext.stereotype || 'UNKNOWN',
                        sourceModule: ext.module || 'External',
                        sourceDomain: ext.domain || 'External',
                        calledByEndpoints: new Set(),
                        callerDomains: new Set(),
                        callCount: 0,
                        accessType: 'DIRECT_METHOD_CALL',
                        recommendation: 'REST_API'
                    };
                }
                beanMap[key].calledByEndpoints.add(r.endpointName);
                beanMap[key].callerDomains.add(r.primaryDomain);
                beanMap[key].callCount++;
            }

            // Collections accessed from a different domain than the endpoint's primary
            for (const [collName, coll] of Object.entries(r.collections)) {
                if (coll.domain && coll.domain !== r.primaryDomain && coll.domain !== 'Other') {
                    const key = collName;
                    if (!collCrossMap[key]) {
                        collCrossMap[key] = {
                            name: collName,
                            type: coll.type,
                            ownerDomain: coll.domain,
                            accessedByDomains: new Set(),
                            accessedByEndpoints: new Set(),
                            operations: new Set(),
                            accessType: 'DIRECT_COLLECTION_ACCESS',
                            recommendation: coll.type === 'VIEW' ? 'EVALUATE' : 'REST_API'
                        };
                    }
                    collCrossMap[key].accessedByDomains.add(r.primaryDomain);
                    collCrossMap[key].accessedByEndpoints.add(r.endpointName);
                    for (const op of coll.operations) collCrossMap[key].operations.add(op);
                }
            }
        }

        const beans = Object.values(beanMap).sort((a, b) => b.callCount - a.callCount);
        beans.forEach((b, i) => { b._srcIdx = i; });
        const collections = Object.values(collCrossMap).sort((a, b) =>
            b.accessedByEndpoints.size - a.accessedByEndpoints.size);
        collections.forEach((c, i) => { c._srcIdx = i; });

        return { beans, collections, primaryDomain };
    },

    _renderVertVerification(epReports, esc) {
        const report = this._buildVertVerification(epReports);
        this._vertVerReport = report;
        let html = '';

        // Summary stats — compact inline chips
        const totalBeans = report.beans.length;
        const totalColls = report.collections.length;
        const totalCalls = report.beans.reduce((s, b) => s + b.callCount, 0);
        const writeColls = report.collections.filter(c => c.operations.has('WRITE') || c.operations.has('UPDATE')).length;

        html += '<div class="sum-section">';
        html += '<div class="vert-compact-header">';
        html += '<span class="vert-compact-title">Verticalisation Verification</span>';
        html += '<span class="vert-chip vert-chip-warn"><b>' + totalBeans + '</b> Method Calls</span>';
        html += '<span class="vert-chip vert-chip-warn"><b>' + totalCalls + '</b> Invocations</span>';
        html += '<span class="vert-chip vert-chip-danger"><b>' + totalColls + '</b> Cross-Domain Colls</span>';
        html += '<span class="vert-chip vert-chip-danger"><b>' + writeColls + '</b> Writes</span>';
        html += '</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="Direct method calls across module boundaries are compile-time dependencies. After verticalisation, each one becomes a REST API call with network latency, serialization overhead, and failure modes.">Every direct cross-module call becomes a REST API with latency + failure modes</span>';
        html += '<span class="sum-tip" title="Cross-domain collection access means one service reads/writes another service\'s database directly. This violates database-per-service and must be replaced with API calls or events.">Cross-domain collection access violates database-per-service principle</span>';
        html += '<span class="sum-tip" title="Focus on WRITE operations first — reads can often be replaced with cached API calls or CQRS projections, but writes need strong consistency guarantees (API + compensation/saga).">Prioritize cross-domain WRITEs — reads are easier to decouple</span>';
        html += '</div>';

        // Toggle buttons — only one table visible at a time
        html += '<div class="vert-section-toggle">';
        html += `<button class="vert-sec-btn active" data-sec="vertm" onclick="JA.summary._vertToggleSection('vertm')">Direct Method Calls (${totalBeans})</button>`;
        html += `<button class="vert-sec-btn" data-sec="vertc" onclick="JA.summary._vertToggleSection('vertc')">Cross-Domain Collections (${totalColls})</button>`;
        html += '</div>';

        // Section 1: Direct Method Calls (visible by default)
        html += '<div id="vert-sec-vertm">';
        if (report.beans.length) {
            html += this._buildFilterBar('sum-vertm', report.beans, b => b.sourceDomain);
            html += '<div class="sum-pager sum-pager-top" id="sum-vertm-pager-top"></div>';
            html += '<div class="sum-table-wrap"><table class="sum-table" id="sum-vertm-table"><thead><tr>';
            html += '<th style="width:40px" title="Row number">#</th>';
            html += `<th class="sum-th-sort" data-sort-col="1" onclick="JA.summary._pageSort('sum-vertm',1)" title="Fully qualified bean class and method being called directly across module boundaries">Bean.Method</th>`;
            html += `<th class="sum-th-sort" data-sort-col="2" onclick="JA.summary._pageSort('sum-vertm',2)" title="Spring stereotype of the target bean">Stereotype</th>`;
            html += `<th class="sum-th-sort" data-sort-col="3" onclick="JA.summary._pageSort('sum-vertm',3)" title="The module/JAR that owns this bean">Owner Module</th>`;
            html += `<th class="sum-th-sort" data-sort-col="4" onclick="JA.summary._pageSort('sum-vertm',4)" title="Business domain that owns this bean">Owner Domain</th>`;
            html += `<th class="sum-th-sort" data-sort-col="5" onclick="JA.summary._pageSort('sum-vertm',5)" style="width:60px" title="Total number of direct cross-module invocations">Calls</th>`;
            html += `<th class="sum-th-sort" data-sort-col="6" onclick="JA.summary._pageSort('sum-vertm',6)" style="width:60px" title="Number of distinct endpoints making this call">EPs</th>`;
            html += '<th title="Current access pattern">Current</th>';
            html += '<th title="Recommended access pattern">Recommended</th>';
            html += '</tr></thead><tbody id="sum-vertm-tbody"></tbody></table></div>';
            html += '<div class="sum-pager" id="sum-vertm-pager"></div>';

            const vertmSortKeys = {
                1: { fn: b => (b.simpleClassName + '.' + b.methodName) },
                2: { fn: b => b.stereotype || '' },
                3: { fn: b => b.sourceModule || '' },
                4: { fn: b => b.sourceDomain || '' },
                5: { fn: b => b.callCount || 0 },
                6: { fn: b => b.calledByEndpoints ? b.calledByEndpoints.size : 0 }
            };
            this._initPage('sum-vertm', report.beans, 25,
                (b, i, esc) => this._renderVertmRow(b, i, esc),
                b => b.sourceDomain,
                (b, i, esc) => this._renderVertmDetail(b, i, esc),
                { sortKeys: vertmSortKeys }
            );
            setTimeout(() => {
                this._pageRender('sum-vertm');
                this._initColFilters('sum-vertm', {
                    2: { label: 'Stereotype', valueFn: b => b.stereotype || '' },
                    3: { label: 'Owner Module', valueFn: b => b.sourceModule || '' },
                    4: { label: 'Owner Domain', valueFn: b => b.sourceDomain || '' }
                });
            }, 0);
        } else {
            html += '<p class="sum-muted" style="padding:12px">No cross-module method calls detected.</p>';
        }
        html += '</div>';

        // Section 2: Cross-Domain Collections (hidden by default)
        html += '<div id="vert-sec-vertc" style="display:none">';
        if (report.collections.length) {
            html += this._renderVertCrossSection(report, esc);
        } else {
            html += '<p class="sum-muted" style="padding:12px">No cross-domain collection access detected.</p>';
        }
        html += '</div>';

        html += '</div>';
        return html;
    },

    /** Toggle between Direct Method Calls and Cross-Domain sections */
    _vertToggleSection(sec) {
        document.querySelectorAll('.vert-sec-btn').forEach(b =>
            b.classList.toggle('active', b.dataset.sec === sec));
        const vertm = document.getElementById('vert-sec-vertm');
        const vertc = document.getElementById('vert-sec-vertc');
        if (vertm) vertm.style.display = sec === 'vertm' ? '' : 'none';
        if (vertc) vertc.style.display = sec === 'vertc' ? '' : 'none';

        // Lazy-init cross-domain table on first show
        if (sec === 'vertc' && vertc && !vertc.dataset.rendered) {
            vertc.dataset.rendered = '1';
            setTimeout(() => {
                this._pageRender('sum-vertc');
                this._initColFilters('sum-vertc', {
                    2: { label: 'Type', valueFn: c => c.type || '' },
                    3: { label: 'Owner Domain', valueFn: c => c.ownerDomain || '' }
                });
            }, 0);
        }
    },

    /* --- Direct Method Calls row renderers --- */

    _renderVertmRow(b, i, esc) {
        const popIdx = b._srcIdx != null ? b._srcIdx : i;
        let html = `<tr onclick="JA.summary._showVertPopup('bean',${popIdx})" style="cursor:pointer">`;
        html += `<td>${i + 1}</td>`;
        const safeClass = (b.simpleClassName || '').replace(/'/g, "\\'");
        const safeMethod = (b.methodName || '').replace(/'/g, "\\'");
        html += `<td><span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${esc(safeClass)}','${esc(safeMethod)}')">${esc(b.simpleClassName)}.${esc(b.methodName)}</span></td>`;
        html += `<td><span class="sum-stereo-tag">${esc(b.stereotype)}</span></td>`;
        html += `<td><span class="sum-module-tag">${esc(b.sourceModule)}</span></td>`;
        html += `<td><span class="sum-domain-tag">${esc(b.sourceDomain)}</span></td>`;
        html += `<td class="sum-center">${b.callCount}</td>`;
        html += `<td class="sum-center">${b.calledByEndpoints.size}</td>`;
        html += `<td><span class="vert-access-badge vert-access-direct">DIRECT</span></td>`;
        html += `<td><span class="vert-access-badge vert-access-rest">REST API</span></td>`;
        html += '</tr>';
        return html;
    },

    _renderVertmDetail(b, i, esc) {
        let html = `<tr class="sum-detail-row" id="sum-vertm-detail-${i}" style="display:none"><td colspan="9"><div class="sum-detail">`;
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Called By Endpoints (' + b.calledByEndpoints.size + ')</div>' +
            [...b.calledByEndpoints].map(ep => `<span class="sum-bean-tag">${esc(ep)}</span>`).join(' ') + '</div>';
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Caller Domains</div>' +
            [...b.callerDomains].map(d => `<span class="sum-domain-tag">${esc(d)}</span>`).join(' ') + '</div>';
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Action</div>' +
            '<span class="vert-action">Replace direct <code>' + esc(b.simpleClassName) + '.' + esc(b.methodName) + '()</code> call with REST endpoint in <b>' + esc(b.sourceDomain) + '</b> module</span></div>';
        html += '</div></td></tr>';
        return html;
    },

    /* --- Cross-Domain Collection Access section --- */

    _renderVertCrossSection(report, esc) {
        const pd = report.primaryDomain;
        let html = '';

        // 4-option direction toggle
        html += '<div class="vert-cross-toggle">';
        html += `<button class="vert-cross-btn active" data-cross="all" onclick="JA.summary._vertCrossFilter('all')">All</button>`;
        html += `<button class="vert-cross-btn" data-cross="c2e-coll" onclick="JA.summary._vertCrossFilter('c2e-coll')">${esc(pd)}\u2192External (Collections)</button>`;
        html += `<button class="vert-cross-btn" data-cross="c2e-ops" onclick="JA.summary._vertCrossFilter('c2e-ops')">${esc(pd)}\u2192External (Operations)</button>`;
        html += `<button class="vert-cross-btn" data-cross="e2c-coll" onclick="JA.summary._vertCrossFilter('e2c-coll')">External\u2192${esc(pd)} (Collections)</button>`;
        html += `<button class="vert-cross-btn" data-cross="e2c-ops" onclick="JA.summary._vertCrossFilter('e2c-ops')">External\u2192${esc(pd)} (Operations)</button>`;
        html += '</div>';

        html += this._buildFilterBar('sum-vertc', report.collections, c => c.ownerDomain);
        html += '<div class="sum-pager sum-pager-top" id="sum-vertc-pager-top"></div>';
        html += '<div class="sum-table-wrap"><table class="sum-table" id="sum-vertc-table"><thead><tr>';
        html += '<th style="width:40px" title="Row number">#</th>';
        html += `<th class="sum-th-sort" data-sort-col="1" onclick="JA.summary._pageSort('sum-vertc',1)" title="MongoDB collection being accessed from a domain that does not own it">Collection</th>`;
        html += `<th class="sum-th-sort" data-sort-col="2" onclick="JA.summary._pageSort('sum-vertc',2)" title="Whether this is a regular collection or a MongoDB view">Type</th>`;
        html += `<th class="sum-th-sort" data-sort-col="3" onclick="JA.summary._pageSort('sum-vertc',3)" title="Business domain that owns this collection">Owner Domain</th>`;
        html += '<th title="Other business domains that directly access this collection">Accessed By</th>';
        html += '<th title="Database operations performed on this collection from outside its owning domain">Operations</th>';
        html += `<th class="sum-th-sort" data-sort-col="6" onclick="JA.summary._pageSort('sum-vertc',6)" style="width:60px" title="Number of endpoints from other domains accessing this collection">EPs</th>`;
        html += '<th title="Current access pattern">Current</th>';
        html += '<th title="Recommended access pattern">Recommended</th>';
        html += '</tr></thead><tbody id="sum-vertc-tbody"></tbody></table></div>';
        html += '<div class="sum-pager" id="sum-vertc-pager"></div>';

        const vertcSortKeys = {
            1: { fn: c => c.name || '' },
            2: { fn: c => c.type || '' },
            3: { fn: c => c.ownerDomain || '' },
            6: { fn: c => c.accessedByEndpoints ? c.accessedByEndpoints.size : 0 }
        };
        // Store full dataset for mode switching
        this._vertCrossAllData = report.collections;
        this._vertCrossPrimary = report.primaryDomain;

        this._initPage('sum-vertc', report.collections, 25,
            (c, i, esc) => this._renderVertcRow(c, i, esc),
            c => c.ownerDomain,
            (c, i, esc) => this._renderVertcDetail(c, i, esc),
            { sortKeys: vertcSortKeys }
        );
        // Render deferred to _vertToggleSection on first show

        return html;
    },

    /* --- Cross-Domain direction filter --- */

    _vertCrossFilter(mode) {
        // Update toggle buttons
        document.querySelectorAll('.vert-cross-btn').forEach(b =>
            b.classList.toggle('active', b.dataset.cross === mode));

        const s = this._pageState['sum-vertc'];
        if (!s) return;
        const pd = this._vertCrossPrimary;
        const allData = this._vertCrossAllData;
        if (!allData) return;

        s._crossMode = mode;

        if (mode === 'all') {
            // Reset to full collection data
            s.data = allData;
            allData.forEach((item, i) => { item._origIdx = i; });
            s.colFilters = {};
            this._pageFilter('sum-vertc');
            return;
        }

        const isOps = mode.endsWith('-ops');
        const isC2E = mode.startsWith('c2e');

        // Filter by direction
        let filtered;
        if (isC2E) {
            // Current->External: our endpoints touch other domains' collections
            filtered = allData.filter(c =>
                c.ownerDomain !== pd && c.accessedByDomains.has(pd));
        } else {
            // External->Current: other domains touch our collections
            filtered = allData.filter(c =>
                c.ownerDomain === pd && [...c.accessedByDomains].some(d => d !== pd));
        }

        if (isOps) {
            // Flatten to collection x operation rows
            const flat = [];
            for (const c of filtered) {
                for (const op of c.operations) {
                    flat.push({
                        name: c.name,
                        type: c.type,
                        ownerDomain: c.ownerDomain,
                        accessedByDomains: c.accessedByDomains,
                        accessedByEndpoints: c.accessedByEndpoints,
                        operations: new Set([op]),
                        _singleOp: op,
                        _srcIdx: c._srcIdx,
                        accessType: c.accessType,
                        recommendation: c.recommendation
                    });
                }
            }
            flat.sort((a, b) => (a._singleOp || '').localeCompare(b._singleOp || '') ||
                (a.name || '').localeCompare(b.name || ''));
            s.data = flat;
        } else {
            s.data = filtered;
        }

        s.data.forEach((item, i) => { item._origIdx = i; });
        s.colFilters = {};
        s.filtered = [...s.data];
        s.page = 0;
        s.sortCol = -1;
        s.sortDir = 'asc';
        this._pageRender('sum-vertc');
        if (this._cfUpdateIcons) this._cfUpdateIcons('sum-vertc');
    },

    /* --- Cross-Domain Collection row renderers --- */

    _renderVertcRow(c, i, esc) {
        const hasWrite = c.operations.has('WRITE') || c.operations.has('UPDATE');
        const popIdx = c._srcIdx != null ? c._srcIdx : i;
        let html = `<tr onclick="JA.summary._showVertPopup('coll',${popIdx})" style="cursor:pointer">`;
        html += `<td>${i + 1}</td>`;
        html += `<td><span class="sum-coll-badge ${c.type === 'VIEW' ? 'sum-coll-view' : 'sum-coll-data'}">${esc(c.name)}</span></td>`;
        html += `<td>${c.type === 'VIEW' ? '<span class="sum-type-badge sum-type-view">VIEW</span>' : '<span class="sum-type-badge sum-type-coll">COLL</span>'}</td>`;
        html += `<td><span class="sum-domain-tag">${esc(c.ownerDomain)}</span></td>`;
        html += '<td>' + [...c.accessedByDomains].map(d => `<span class="sum-domain-tag">${esc(d)}</span>`).join(' ') + '</td>';
        html += '<td>' + [...c.operations].map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${op}</span>`).join(' ') + '</td>';
        html += `<td class="sum-center">${c.accessedByEndpoints.size}</td>`;
        html += `<td><span class="vert-access-badge vert-access-direct">DIRECT</span></td>`;
        html += `<td><span class="vert-access-badge ${hasWrite ? 'vert-access-rest' : 'vert-access-eval'}">` +
            `${hasWrite ? 'REST API' : 'EVALUATE'}</span></td>`;
        html += '</tr>';
        return html;
    },

    _renderVertcDetail(c, i, esc) {
        let html = `<tr class="sum-detail-row" id="sum-vertc-detail-${i}" style="display:none"><td colspan="9"><div class="sum-detail">`;
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Accessed By Endpoints (' + c.accessedByEndpoints.size + ')</div>' +
            [...c.accessedByEndpoints].map(ep => `<span class="sum-bean-tag">${esc(ep)}</span>`).join(' ') + '</div>';
        const hasWrite = c.operations.has('WRITE') || c.operations.has('UPDATE');
        html += '<div class="sum-detail-block"><div class="sum-detail-label">Action</div>' +
            '<span class="vert-action">Collection <code>' + esc(c.name) + '</code> belongs to <b>' + esc(c.ownerDomain) +
            '</b> \u2014 ' + (hasWrite ? 'cross-domain writes must go through REST API' : 'read-only access, evaluate if API wrapper needed') + '</span></div>';
        html += '</div></td></tr>';
        return html;
    }
});
