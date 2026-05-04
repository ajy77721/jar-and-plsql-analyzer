/**
 * Summary vert-popup — rich popup for verticalisation bean/collection rows.
 * Shows 3 tabs: Callers | Endpoints | Code Trail.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _showVertPopup(type, idx) {
        const report = this._vertVerReport;
        if (!report) return;
        const item = type === 'bean' ? report.beans[idx] : report.collections[idx];
        if (!item) return;

        const esc = JA.utils.escapeHtml;
        const title = type === 'bean'
            ? esc(item.simpleClassName) + '.' + esc(item.methodName)
            : esc(item.name);

        let html = '<div class="vp-overlay" id="vp-overlay" onclick="if(event.target===this)JA.summary._closeVertPopup()">';
        html += '<div class="vp-panel">';
        html += '<div class="vp-header">';
        html += `<div class="vp-title">${title}</div>`;
        html += '<div class="vp-meta">';
        if (type === 'bean') {
            html += `<span class="sum-stereo-tag">${esc(item.stereotype)}</span>`;
            html += `<span class="sum-module-tag">${esc(item.sourceModule)}</span>`;
            html += `<span class="sum-domain-tag">${esc(item.sourceDomain)}</span>`;
            html += `<span class="vp-count">${item.callCount} calls</span>`;
        } else {
            html += `<span class="sum-type-badge ${item.type === 'VIEW' ? 'sum-type-view' : 'sum-type-coll'}">${item.type === 'VIEW' ? 'VIEW' : 'COLL'}</span>`;
            html += `<span class="sum-domain-tag">${esc(item.ownerDomain)}</span>`;
            html += `<span class="vp-count">${item.accessedByEndpoints.size} endpoints</span>`;
        }
        html += '</div>';
        html += `<button class="vp-close" onclick="JA.summary._closeVertPopup()">&#10005;</button>`;
        html += '</div>';

        // Tabs
        html += '<div class="vp-tabs">';
        html += '<button class="vp-tab active" data-vp-tab="callers" onclick="JA.summary._vpSwitchTab(\'callers\')">Callers</button>';
        html += '<button class="vp-tab" data-vp-tab="endpoints" onclick="JA.summary._vpSwitchTab(\'endpoints\')">Endpoints</button>';
        html += '<button class="vp-tab" data-vp-tab="trail" onclick="JA.summary._vpSwitchTab(\'trail\')">Code Trail</button>';
        html += '</div>';

        // Tab content
        html += '<div class="vp-body">';
        html += this._vpBuildCallersTab(type, item, esc);
        html += this._vpBuildEndpointsTab(type, item, esc);
        html += this._vpBuildTrailTab(type, item, esc);
        html += '</div>';

        html += '</div></div>';

        document.body.insertAdjacentHTML('beforeend', html);
    },

    _closeVertPopup() {
        const el = document.getElementById('vp-overlay');
        if (el) el.remove();
    },

    _vpSwitchTab(tabName) {
        document.querySelectorAll('.vp-tab').forEach(b => b.classList.toggle('active', b.dataset.vpTab === tabName));
        document.querySelectorAll('.vp-tab-content').forEach(c => c.style.display = c.dataset.vpContent === tabName ? '' : 'none');
    },

    /* === Tab 1: Callers === */
    _vpBuildCallersTab(type, item, esc) {
        let html = '<div class="vp-tab-content" data-vp-content="callers">';
        if (type === 'bean') {
            html += this._vpBeanCallers(item, esc);
        } else {
            html += this._vpCollCallers(item, esc);
        }
        html += '</div>';
        return html;
    },

    _vpBeanCallers(item, esc) {
        const epReports = this._epReports || [];
        // Find all endpoints that call this bean, group by domain
        const domainGroups = {};
        for (const r of epReports) {
            for (const ext of (r.externalCalls || [])) {
                const key = (ext.className || ext.simpleClassName || '') + '.' + (ext.methodName || '');
                if (key !== (item.className + '.' + item.methodName) &&
                    key !== (item.simpleClassName + '.' + item.methodName)) continue;
                const domain = r.primaryDomain;
                if (!domainGroups[domain]) domainGroups[domain] = [];
                domainGroups[domain].push({
                    endpoint: r.endpointName,
                    httpMethod: r.httpMethod,
                    path: r.fullPath,
                    controllerClass: r.endpoint?.controllerClass || '',
                    breadcrumb: ext.breadcrumb
                });
            }
        }

        let html = '<div class="vp-section-title">Callers by Domain</div>';
        for (const [domain, callers] of Object.entries(domainGroups).sort((a, b) => b[1].length - a[1].length)) {
            html += `<div class="vp-domain-group">`;
            html += `<div class="vp-domain-header"><span class="sum-domain-tag">${esc(domain)}</span> <span class="vp-count">${callers.length} caller${callers.length !== 1 ? 's' : ''}</span></div>`;
            for (const c of callers) {
                const safeCtrl = (c.controllerClass || '').replace(/'/g, "\\'");
                html += '<div class="vp-caller-item">';
                html += `<span class="endpoint-method method-${c.httpMethod}" style="font-size:9px;padding:1px 4px">${esc(c.httpMethod)}</span> `;
                html += `<span class="sum-ext-method-link" onclick="JA.summary.showClassCode('${safeCtrl}');event.stopPropagation()" title="View code">${esc(c.endpoint)}</span>`;
                html += `<span class="vp-path">${esc(c.path)}</span>`;
                if (c.breadcrumb?.length) {
                    html += '<div class="sum-breadcrumb">' + this._renderBc(c.breadcrumb, esc) + '</div>';
                }
                html += '</div>';
            }
            html += '</div>';
        }
        if (!Object.keys(domainGroups).length) {
            html += '<p class="sum-muted">No callers found</p>';
        }
        return html;
    },

    _vpCollCallers(item, esc) {
        const epReports = this._epReports || [];
        const domainGroups = {};
        for (const r of epReports) {
            if (!r.collections || !r.collections[item.name]) continue;
            const domain = r.primaryDomain;
            if (!domainGroups[domain]) domainGroups[domain] = [];
            const coll = r.collections[item.name];
            domainGroups[domain].push({
                endpoint: r.endpointName,
                httpMethod: r.httpMethod,
                path: r.fullPath,
                operations: coll.operations ? [...coll.operations] : [],
                sources: coll.sources ? [...coll.sources] : [],
                breadcrumbs: r.collBreadcrumbs?.[item.name] || []
            });
        }

        let html = '<div class="vp-section-title">Accessing Endpoints by Domain</div>';
        for (const [domain, eps] of Object.entries(domainGroups).sort((a, b) => b[1].length - a[1].length)) {
            html += `<div class="vp-domain-group">`;
            html += `<div class="vp-domain-header"><span class="sum-domain-tag">${esc(domain)}</span> <span class="vp-count">${eps.length} endpoint${eps.length !== 1 ? 's' : ''}</span></div>`;
            for (const e of eps) {
                html += '<div class="vp-caller-item">';
                html += `<span class="endpoint-method method-${e.httpMethod}" style="font-size:9px;padding:1px 4px">${esc(e.httpMethod)}</span> `;
                html += `<span class="sum-ext-method-link">${esc(e.endpoint)}</span>`;
                html += ` <span class="vp-path">${esc(e.path)}</span>`;
                html += ' ' + e.operations.map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${op}</span>`).join(' ');
                html += '</div>';
            }
            html += '</div>';
        }
        return html;
    },

    /* === Tab 2: Endpoints === */
    _vpBuildEndpointsTab(type, item, esc) {
        const endpoints = type === 'bean'
            ? [...(item.calledByEndpoints || [])]
            : [...(item.accessedByEndpoints || [])];

        let html = '<div class="vp-tab-content" data-vp-content="endpoints" style="display:none">';
        html += `<div class="vp-section-title">Endpoint Consumers (${endpoints.length})</div>`;

        const epReports = this._epReports || [];
        for (const epName of endpoints.sort()) {
            const r = epReports.find(r => r.endpointName === epName);
            if (!r) {
                html += `<div class="vp-ep-item"><span class="sum-bean-tag">${esc(epName)}</span></div>`;
                continue;
            }

            html += '<div class="vp-ep-item">';
            html += `<span class="endpoint-method method-${r.httpMethod}" style="font-size:9px;padding:1px 4px">${esc(r.httpMethod)}</span> `;
            html += `<span class="vp-ep-path">${esc(r.fullPath)}</span> `;
            html += `<span class="sum-bean-tag">${esc(r.endpointName)}</span>`;
            html += '<div class="vp-ep-meta">';
            html += `<span class="sum-domain-tag">${esc(r.primaryDomain)}</span>`;
            html += `<span class="ep-card-chip" style="font-size:10px">${r.totalCollections} <small>Colls</small></span>`;
            html += `<span class="ep-card-chip" style="font-size:10px">${r.totalDbOperations} <small>DB Ops</small></span>`;
            html += `<span class="ep-card-chip" style="font-size:10px">${r.totalMethods} <small>Methods</small></span>`;
            html += '</div>';

            // Show explore button
            const idx = epReports.indexOf(r);
            if (idx >= 0) {
                html += `<button class="btn-sm btn-explore" style="margin-top:4px" onclick="JA.summary._closeVertPopup();JA.summary.showCallTrace(${idx})">Explore</button>`;
            }
            html += '</div>';
        }
        if (!endpoints.length) html += '<p class="sum-muted">No endpoints found</p>';
        html += '</div>';
        return html;
    },

    /* === Tab 3: Code Trail === */
    _vpBuildTrailTab(type, item, esc) {
        let html = '<div class="vp-tab-content" data-vp-content="trail" style="display:none">';
        html += '<div class="vp-section-title">Breadcrumb Code Trail</div>';

        const epReports = this._epReports || [];
        const endpoints = type === 'bean'
            ? [...(item.calledByEndpoints || [])]
            : [...(item.accessedByEndpoints || [])];

        let trailCount = 0;
        for (const epName of endpoints) {
            const r = epReports.find(r => r.endpointName === epName);
            if (!r) continue;

            // For beans: find the external call breadcrumb
            // For collections: find the collection breadcrumb
            let breadcrumbs = [];
            if (type === 'bean') {
                for (const ext of (r.externalCalls || [])) {
                    const key = (ext.className || ext.simpleClassName || '') + '.' + (ext.methodName || '');
                    if (key === (item.className + '.' + item.methodName) ||
                        key === (item.simpleClassName + '.' + item.methodName)) {
                        if (ext.breadcrumb) breadcrumbs.push(ext.breadcrumb);
                    }
                }
            } else {
                breadcrumbs = r.collBreadcrumbs?.[item.name] || [];
            }

            if (!breadcrumbs.length) continue;
            trailCount++;
            if (trailCount > 10) break;

            html += '<div class="vp-trail-group">';
            html += `<div class="vp-trail-header"><span class="endpoint-method method-${r.httpMethod}" style="font-size:9px;padding:1px 4px">${esc(r.httpMethod)}</span> <span class="vp-trail-ep">${esc(epName)}</span></div>`;

            for (const bc of breadcrumbs) {
                html += '<div class="vp-trail-chain">';
                bc.forEach((seg, si) => {
                    if (si > 0) html += '<div class="vp-trail-arrow">&#9660;</div>';
                    const safeCls = (seg.className || '').replace(/'/g, "\\'");
                    const safeMeth = (seg.methodName || '').replace(/'/g, "\\'");
                    const isLast = si === bc.length - 1;
                    html += `<div class="vp-trail-node ${seg.isExternal ? 'vp-trail-external' : ''} ${isLast ? 'vp-trail-target' : ''}">`;
                    html += `<span class="vp-trail-label sum-ext-method-link" onclick="JA.summary.showClassCode('${safeCls}','${safeMeth}');event.stopPropagation()" title="${esc(seg.full || '')}">${esc(seg.label)}</span>`;
                    if (seg.isExternal) html += ' <span class="vp-trail-cross">cross-module</span>';
                    html += '</div>';
                });
                html += '</div>';
            }
            html += '</div>';
        }
        if (!trailCount) html += '<p class="sum-muted">No call trails available</p>';
        html += '</div>';
        return html;
    }
});
