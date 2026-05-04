/**
 * Summary popups — stat card click handlers showing detail modals.
 * Each stat card opens a popup with detailed, properly labeled information.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    showStatPopup(type) {
        const esc = JA.utils.escapeHtml;
        const ep = this._epReports || [];
        // Build reports on demand if not yet computed (lazy tab access deferred them)
        if (!this._vertReport && ep.length && (type === 'collections' || type === 'views' || type === 'domains')) {
            this._vertReport = this._buildVerticalisation(ep);
            this._viewsReport = this._buildViewsAnalysis(this._vertReport, ep);
        }
        if (!this._distReport && ep.length && type === 'txn') {
            this._distReport = this._buildDistributedTransactions(ep);
        }
        if (!this._batchReport && ep.length && type === 'batch') {
            this._batchReport = this._buildBatchAnalysis(ep);
        }
        const vert = this._vertReport || [];
        const dist = this._distReport || [];
        const batch = this._batchReport || [];
        const ext = this._extReport || { crossModule: [] };

        let title = '', body = '';

        switch (type) {
            case 'endpoints':
                title = `Endpoints (${ep.length})`;
                body = this._popupEndpoints(ep, esc);
                break;
            case 'collections':
                title = `Collections (${vert.length})`;
                body = this._popupCollections(vert, esc);
                break;
            case 'views':
                title = `Views (${vert.filter(c => c.type === 'VIEW').length})`;
                body = this._popupViews(vert, esc);
                break;
            case 'modules': {
                const mods = new Set();
                ep.forEach(r => r.modules.forEach(m => mods.add(m)));
                title = `Modules (${mods.size})`;
                body = this._popupModules(ep, mods, esc);
                break;
            }
            case 'domains': {
                const doms = new Set();
                vert.forEach(c => { if (c.domain && c.domain !== 'Other') doms.add(c.domain); });
                title = `Domains (${doms.size})`;
                body = this._popupDomains(ep, vert, doms, esc);
                break;
            }
            case 'crossmodule':
                title = `Cross-Module Dependencies (${ext.crossModule.length} modules, ${this._totalExtCalls || 0} total calls)`;
                body = this._popupCrossModule(ep, ext, esc);
                break;
            case 'txn':
                title = `Distributed Transactions — ${dist.filter(d => d.transactionRequirement.startsWith('REQUIRED')).length} Required`;
                body = this._popupTxn(dist, esc);
                break;
            case 'batch':
                title = `Batch Jobs (${batch.length})`;
                body = this._popupBatch(batch, esc);
                break;
            case 'scheduled':
                this.switchSubTab('scheduled');
                return;
        }

        this._showPopup(title, body);
    },

    _showPopup(title, body) {
        const old = document.getElementById('stat-popup-overlay');
        if (old) old.remove();
        const html = `<div class="stat-popup-overlay" id="stat-popup-overlay" onclick="if(event.target===this)this.remove()">
            <div class="stat-popup-panel">
                <div class="stat-popup-header">
                    <span class="stat-popup-title">${title}</span>
                    <button class="btn-sm" onclick="document.getElementById('stat-popup-overlay').remove()">Close</button>
                </div>
                <div class="stat-popup-body">${body}</div>
            </div>
        </div>`;
        document.body.insertAdjacentHTML('beforeend', html);
    },

    /* ===== Endpoints Popup ===== */
    _popupEndpoints(ep, esc) {
        if (!ep.length) return '<div class="stat-popup-empty">No endpoints</div>';
        let html = '<div class="stat-popup-summary">';
        const internal = ep.filter(r => r.externalScopeCalls === 0).length;
        const external = ep.length - internal;
        const byMethod = {};
        ep.forEach(r => { byMethod[r.httpMethod] = (byMethod[r.httpMethod] || 0) + 1; });
        html += `<span class="stat-chip stat-chip-ok">${internal} Internal Only</span>`;
        html += `<span class="stat-chip stat-chip-warn">${external} Cross-Module</span>`;
        for (const [m, c] of Object.entries(byMethod).sort()) {
            html += `<span class="stat-chip">${m}: ${c}</span>`;
        }
        html += '</div>';
        html += '<div class="stat-popup-list">' + ep.map(r =>
            `<div class="stat-popup-item" onclick="JA.summary.switchSubTab('ep-report');document.getElementById('stat-popup-overlay')?.remove()">
                <div class="stat-popup-item-row">
                    <span class="endpoint-method method-${r.httpMethod}">${esc(r.httpMethod)}</span>
                    <span class="stat-popup-path">${esc(r.fullPath)}</span>
                    <span class="sum-domain-tag">${esc(r.primaryDomain)}</span>
                </div>
                <div class="stat-popup-item-row stat-popup-detail">
                    <span>${esc(r.endpointName)}</span>
                    <span class="stat-popup-meta">${r.inScopeCalls} internal &middot; ${r.externalScopeCalls} external &middot; ${r.totalCollections} collections &middot; ${r.totalDbOperations} DB ops</span>
                </div>
            </div>`
        ).join('') + '</div>';
        return html;
    },

    /* ===== Collections Popup ===== */
    _popupCollections(vert, esc) {
        if (!vert.length) return '<div class="stat-popup-empty">No collections</div>';
        const grouped = {};
        vert.forEach(c => { (grouped[c.domain] = grouped[c.domain] || []).push(c); });
        let html = '<div class="stat-popup-summary">';
        const colls = vert.filter(c => c.type !== 'VIEW').length;
        const views = vert.filter(c => c.type === 'VIEW').length;
        html += `<span class="stat-chip">${colls} Collections</span>`;
        html += `<span class="stat-chip stat-chip-view">${views} Views</span>`;
        html += `<span class="stat-chip">${Object.keys(grouped).length} Domains</span>`;
        html += '</div>';
        for (const [domain, items] of Object.entries(grouped).sort((a, b) => a[0].localeCompare(b[0]))) {
            html += `<div class="stat-popup-group">
                <div class="stat-popup-group-title"><span class="sum-domain-tag">${esc(domain)}</span> (${items.length} collections)</div>`;
            html += items.map(c =>
                `<div class="stat-popup-item-sm">
                    <span class="sum-coll-badge ${c.type === 'VIEW' ? 'sum-coll-view' : 'sum-coll-data'}">${esc(c.name)}</span>
                    ${[...c.operations].map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${op}</span>`).join('')}
                    <span class="stat-popup-meta">${c.usageCount} uses across ${c.endpoints.size} endpoints</span>
                </div>`
            ).join('');
            html += '</div>';
        }
        return html;
    },

    /* ===== Views Popup ===== */
    _popupViews(vert, esc) {
        const views = vert.filter(c => c.type === 'VIEW');
        if (!views.length) return '<div class="stat-popup-empty">No views detected</div>';
        let html = '<div class="stat-popup-summary">';
        const totalUsage = views.reduce((s, c) => s + c.usageCount, 0);
        html += `<span class="stat-chip stat-chip-view">${views.length} Views</span>`;
        html += `<span class="stat-chip">${totalUsage} Total Usages</span>`;
        html += '</div>';
        html += '<div class="stat-popup-list">' + views.map(c =>
            `<div class="stat-popup-item-sm">
                <span class="sum-coll-badge sum-coll-view">${esc(c.name)}</span>
                <span class="sum-domain-tag">${esc(c.domain)}</span>
                ${[...c.operations].map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${op}</span>`).join('')}
                <span class="stat-popup-meta">${c.usageCount} uses &middot; ${c.endpoints.size} endpoints &middot; ${[...c.sources].slice(0, 3).join(', ')}</span>
            </div>`
        ).join('') + '</div>';
        return html;
    },

    /* ===== Modules Popup ===== */
    _popupModules(ep, mods, esc) {
        const modData = {};
        ep.forEach(r => r.modules.forEach(m => {
            if (!modData[m]) modData[m] = { endpoints: 0, internal: 0, external: 0 };
            modData[m].endpoints++;
            modData[m].internal += r.inScopeCalls;
            modData[m].external += r.externalScopeCalls;
        }));
        let html = '<div class="stat-popup-summary">';
        html += `<span class="stat-chip">${mods.size} Modules</span>`;
        html += `<span class="stat-chip">${ep.length} Endpoints</span>`;
        html += '</div>';
        html += '<div class="stat-popup-list">' + [...mods].sort().map(m => {
            const d = modData[m] || { endpoints: 0, internal: 0, external: 0 };
            return `<div class="stat-popup-item">
                <span class="sum-module-tag">${esc(m)}</span>
                <span class="stat-popup-meta">${d.endpoints} endpoints &middot; ${d.internal} internal calls &middot; ${d.external} external calls</span>
            </div>`;
        }).join('') + '</div>';
        return html;
    },

    /* ===== Domains Popup ===== */
    _popupDomains(ep, vert, doms, esc) {
        // Group collections by domain
        const domColls = {};
        vert.forEach(c => { if (c.domain && c.domain !== 'Other') { (domColls[c.domain] = domColls[c.domain] || []).push(c); } });
        // Group endpoints by domain
        const domEps = {};
        ep.forEach(r => { (domEps[r.primaryDomain] = domEps[r.primaryDomain] || []).push(r.endpointName); });

        let html = '<div class="stat-popup-summary">';
        html += `<span class="stat-chip">${doms.size} Domains</span>`;
        html += `<span class="stat-chip">${vert.length} Collections</span>`;
        html += `<span class="stat-chip">${ep.length} Endpoints</span>`;
        html += '</div>';
        html += [...doms].sort().map(d => {
            const colls = domColls[d] || [];
            const eps = domEps[d] || [];
            const views = colls.filter(c => c.type === 'VIEW').length;
            return `<div class="stat-popup-group">
                <div class="stat-popup-group-title">
                    <span class="sum-domain-tag">${esc(d)}</span>
                    <span class="stat-popup-meta">${colls.length} collections${views ? ' (' + views + ' views)' : ''} &middot; ${eps.length} endpoints</span>
                </div>
                <div class="stat-popup-list">
                    ${colls.slice(0, 10).map(c =>
                        `<div class="stat-popup-item-sm">
                            <span class="sum-coll-badge ${c.type === 'VIEW' ? 'sum-coll-view' : 'sum-coll-data'}">${esc(c.name)}</span>
                            ${[...c.operations].map(op => `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${op}</span>`).join('')}
                        </div>`
                    ).join('')}
                    ${colls.length > 10 ? '<div class="stat-popup-meta">... and ' + (colls.length - 10) + ' more collections</div>' : ''}
                    ${eps.length > 0 ? '<div class="stat-popup-meta" style="margin-top:4px">Endpoints: ' + eps.slice(0, 8).map(n => esc(n)).join(', ') + (eps.length > 8 ? ' + ' + (eps.length - 8) + ' more' : '') + '</div>' : ''}
                </div>
            </div>`;
        }).join('');
        return html;
    },

    /* ===== Cross-Module Popup ===== */
    _popupCrossModule(ep, ext, esc) {
        if (!ext.crossModule.length) return '<div class="stat-popup-empty">No cross-module dependencies</div>';
        const crossEps = ep.filter(r => r.externalScopeCalls > 0);
        let html = '<div class="stat-popup-summary">';
        html += `<span class="stat-chip stat-chip-warn">${crossEps.length} Endpoints with cross-module calls</span>`;
        html += `<span class="stat-chip">${ext.crossModule.length} External modules</span>`;
        html += `<span class="stat-chip">${this._totalExtCalls || 0} Total external calls</span>`;
        html += '</div>';
        html += '<div class="stat-popup-list">' + ext.crossModule.map(m =>
            `<div class="stat-popup-item">
                <div class="stat-popup-item-row">
                    <span class="sum-module-tag">${esc(m.project)}</span>
                    <span class="sum-domain-tag">${esc(m.domain)}</span>
                </div>
                <div class="stat-popup-item-row stat-popup-detail">
                    <span class="stat-popup-meta">${m.classes.size} classes &middot; ${m.methods.size} methods &middot; ${m.count} calls &middot; used by ${m.endpoints.size} endpoints</span>
                </div>
                <div class="stat-popup-item-row stat-popup-detail">
                    <span class="stat-popup-meta stat-popup-warn">Direct method calls — should be REST API for verticalisation</span>
                </div>
            </div>`
        ).join('') + '</div>';
        return html;
    },

    /* ===== Transactions Popup ===== */
    _popupTxn(dist, esc) {
        const required = dist.filter(d => d.transactionRequirement.startsWith('REQUIRED'));
        const single = dist.filter(d => d.transactionRequirement === 'SINGLE-DOMAIN');
        const none = dist.filter(d => d.transactionRequirement === 'NOT REQUIRED');
        let html = '<div class="stat-popup-summary">';
        html += `<span class="stat-chip stat-chip-warn">${required.length} Required (cross-domain writes)</span>`;
        html += `<span class="stat-chip">${single.length} Single-Domain</span>`;
        html += `<span class="stat-chip stat-chip-ok">${none.length} Not Required</span>`;
        html += '</div>';
        if (!required.length) return html + '<div class="stat-popup-empty">No distributed transactions detected</div>';
        html += '<div class="stat-popup-list">' + required.map(d => {
            const writeDoms = Object.entries(d.crossDomainDependencies || {}).filter(([_, v]) => v.write?.length > 0);
            const readDoms = Object.entries(d.crossDomainDependencies || {}).filter(([_, v]) => v.read?.length > 0);
            return `<div class="stat-popup-item">
                <div class="stat-popup-item-row">
                    <span class="stat-popup-name">${esc(d.endpointName)}</span>
                    <span class="sum-txn-badge sum-txn-req">REQUIRED</span>
                    <span class="sum-domain-tag">${esc(d.primaryDomain)}</span>
                </div>
                <div class="stat-popup-item-row stat-popup-detail">
                    <span class="stat-popup-meta">Writes: ${writeDoms.map(([k, v]) => esc(k) + ' (' + v.write.length + ' colls)').join(', ')}</span>
                </div>
                <div class="stat-popup-item-row stat-popup-detail">
                    <span class="stat-popup-meta">Reads: ${readDoms.map(([k, v]) => esc(k) + ' (' + v.read.length + ' colls)').join(', ')}</span>
                </div>
                <div class="stat-popup-item-row stat-popup-detail">
                    <span class="stat-popup-meta">${d.totalCollections} collections &middot; ${esc(d.performanceImplication)}</span>
                </div>
            </div>`;
        }).join('') + '</div>';
        return html;
    },

    /* ===== Collection Operation Popup — shows methods accessing a collection with given op ===== */
    _showCollOpPopup(collName, op) {
        const esc = JA.utils.escapeHtml;
        const epReports = this._epReports || [];
        const methods = [];
        const seen = new Set();

        for (const r of epReports) {
            if (!r.endpoint || !r.endpoint.callTree) continue;
            this._walkWithPath(r.endpoint.callTree, (node, path) => {
                if (!node.collectionsAccessed || !node.collectionsAccessed.includes(collName)) return;
                const inferred = this._inferOp(node.methodName, node.stereotype);
                const nodeOp = node.operationType || inferred || '';
                if (nodeOp.toUpperCase() !== op.toUpperCase()) return;
                const key = (node.className || node.simpleClassName || '') + '.' + (node.methodName || '');
                if (seen.has(key)) return;
                seen.add(key);
                methods.push({
                    simpleClassName: node.simpleClassName || (node.className || '').split('.').pop(),
                    className: node.className || '',
                    methodName: node.methodName || '',
                    endpoint: r.endpointName,
                    sourceJar: node.sourceJar || null
                });
            });
        }

        let body = '';
        if (!methods.length) {
            body = '<div class="stat-popup-empty">No methods found for ' + esc(op) + ' on ' + esc(collName) + '</div>';
        } else {
            body = '<div class="stat-popup-summary">';
            body += `<span class="stat-chip">${methods.length} Methods</span>`;
            body += `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${esc(op)}</span>`;
            body += `<span class="sum-coll-badge sum-coll-data">${esc(collName)}</span>`;
            body += '</div>';
            body += '<div class="stat-popup-list">';
            for (const m of methods) {
                const safeClass = (m.className || m.simpleClassName || '').replace(/'/g, "\\'");
                const safeMethod = (m.methodName || '').replace(/'/g, "\\'");
                body += `<div class="stat-popup-item" onclick="JA.summary.showClassCode('${esc(safeClass)}','${esc(safeMethod)}')">`;
                body += `<span class="sum-ext-method-link">${esc(m.simpleClassName)}.${esc(m.methodName)}</span>`;
                body += `<span class="stat-popup-meta">${esc(m.endpoint)}</span>`;
                if (m.sourceJar) body += `<span class="sum-module-tag">${esc(this._jarToProject(m.sourceJar))}</span>`;
                body += '</div>';
            }
            body += '</div>';
        }

        this._showPopup(esc(op) + ' on ' + esc(collName), body);
    },

    /* ===== Operation Methods Popup ===== */
    _showOpMethodsPopup(op, endpointIdx) {
        const esc = JA.utils.escapeHtml;
        const ep = (this._epReports || [])[endpointIdx];
        if (!ep || !ep.endpoint) return;

        const tree = ep.endpoint.callTree;
        if (!tree) return;

        const methods = [];
        const seen = new Set();
        this._walkOpMethods(tree, op, methods, seen);

        let body = '';
        if (!methods.length) {
            body = '<div class="stat-popup-empty">No methods found for ' + esc(op) + '</div>';
        } else {
            body = '<div class="stat-popup-summary">';
            body += `<span class="stat-chip">${methods.length} Methods</span>`;
            body += `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${esc(op)}</span>`;
            body += '</div>';
            body += '<div class="stat-popup-list">';
            for (const m of methods) {
                const safeClass = (m.simpleClassName || '').replace(/'/g, "\\'");
                const safeMethod = (m.methodName || '').replace(/'/g, "\\'");
                body += `<div class="stat-popup-item" onclick="JA.summary.showClassCode('${esc(safeClass)}','${esc(safeMethod)}')">`;
                body += `<span class="sum-ext-method-link">${esc(m.simpleClassName)}.${esc(m.methodName)}</span>`;
                if (m.sourceJar) body += `<span class="sum-module-tag">${esc(this._jarToProject(m.sourceJar))}</span>`;
                if (m.colls.length) body += '<span class="stat-popup-meta">' + m.colls.map(c => esc(c)).join(', ') + '</span>';
                body += '</div>';
            }
            body += '</div>';
        }

        const title = esc(op) + ' Operations — ' + esc(ep.endpointName);
        this._showPopup(title, body);
    },

    _walkOpMethods(node, op, results, seen) {
        if (!node) return;
        const colls = node.collectionsAccessed || [];
        const stereotype = node.stereotype || '';
        const mn = (node.methodName || '').toLowerCase();
        let matched = false;

        // Check inferred operation from method name
        const inferred = this._inferOp(node.methodName, stereotype);
        if (inferred === op) matched = true;

        // Check direct collection operations
        if (node.collectionOperations) {
            for (const co of node.collectionOperations) {
                if ((co.operation || '').toUpperCase() === op) matched = true;
            }
        }

        // Fallback: check operation types stored on the node
        if (node.operationType && node.operationType.toUpperCase() === op) matched = true;

        if (matched) {
            const key = (node.className || node.simpleClassName || '') + '.' + (node.methodName || '');
            if (!seen.has(key)) {
                seen.add(key);
                results.push({
                    className: node.className || '',
                    simpleClassName: node.simpleClassName || (node.className || '').split('.').pop(),
                    methodName: node.methodName || '',
                    sourceJar: node.sourceJar || null,
                    colls: colls
                });
            }
        }

        if (node.children) {
            for (const child of node.children) this._walkOpMethods(child, op, results, seen);
        }
    },

    /* ===== Batch Jobs Popup ===== */
    _popupBatch(batch, esc) {
        if (!batch.length) return '<div class="stat-popup-empty">No batch jobs detected</div>';
        const totalColls = batch.reduce((s, b) => s + b.totalCollections, 0);
        let html = '<div class="stat-popup-summary">';
        html += `<span class="stat-chip">${batch.length} Batch Jobs</span>`;
        html += `<span class="stat-chip">${totalColls} Collections Used</span>`;
        html += '</div>';
        html += '<div class="stat-popup-list">' + batch.map(b => {
            const deps = Object.entries(b.externalDependencies || {});
            return `<div class="stat-popup-item">
                <div class="stat-popup-item-row">
                    <span class="stat-popup-name">${esc(b.batchName)}</span>
                    <span class="sum-domain-tag">${esc(b.primaryDomain)}</span>
                    <span class="sum-size-badge sum-size-${b.sizeCategory.toLowerCase()}">${esc(b.sizeCategory)}</span>
                </div>
                <div class="stat-popup-item-row stat-popup-detail">
                    <span class="stat-popup-meta">${b.totalCollections} collections &middot; ${b.totalMethods} methods &middot; ${esc(b.performanceImplication)}</span>
                </div>
                ${deps.length > 0 ? `<div class="stat-popup-item-row stat-popup-detail">
                    <span class="stat-popup-meta">Domains: ${deps.map(([d, colls]) => esc(d) + ' (' + colls.length + ')').join(', ')}</span>
                </div>` : ''}
            </div>`;
        }).join('') + '</div>';
        return html;
    }
});
