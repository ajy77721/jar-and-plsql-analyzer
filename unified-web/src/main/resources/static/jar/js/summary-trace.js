/**
 * Summary trace — flat Dynatrace-style trace overlay + node-by-node navigator.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /* ===== Flat Trace Overlay ===== */
    async showTrace(idx) {
        const r = this._epReports[idx];
        if (!r) { JA.toast.warn('Endpoint not found at index ' + idx); return; }
        if (!r.endpoint || !r.endpoint.callTree) {
            const jarId = JA.app.currentJarId || '';
            const sourceIdx = r.sourceIdx != null ? r.sourceIdx : idx;
            try {
                const tree = await JA.api.getCallTree(jarId, sourceIdx);
                if (tree) r.endpoint.callTree = tree;
            } catch (e) { /* fall through */ }
            if (!r.endpoint.callTree) { JA.toast.warn('No call tree available for this endpoint'); return; }
        }
        const esc = JA.utils.escapeHtml;
        const controllerJar = r.endpoint.callTree.sourceJar || null;

        let html = '<div class="sum-trace-overlay" id="sum-trace-overlay" onclick="if(event.target===this)this.remove()">';
        html += '<div class="sum-trace-panel">';
        html += '<div class="sum-trace-header">';
        html += `<span class="endpoint-method method-${r.httpMethod}">${esc(r.httpMethod)}</span> `;
        html += `<span style="font-weight:700;font-size:15px">${esc(r.fullPath)}</span>`;
        html += `<span class="sum-muted" style="margin-left:12px">${esc(r.endpointName)}</span>`;
        html += '<button class="btn-sm" style="margin-left:auto" onclick="document.getElementById(\'sum-trace-overlay\').remove()">Close</button>';
        html += '</div>';

        html += '<div class="sum-trace-stats">';
        html += `<span class="sum-stat"><b>${r.totalMethods}</b> Methods</span>`;
        html += `<span class="sum-stat"><b>${r.totalCollections}</b> Collections</span>`;
        html += `<span class="sum-stat"><b>${r.externalScopeCalls}</b> Cross-Module</span>`;
        html += `<span class="sum-stat"><b>${r.crossDomainCount}</b> Cross-Domain</span>`;
        html += r.modules.map(m => `<span class="sum-module-tag">${esc(m)}</span>`).join(' ');
        html += '</div>';

        html += '<div class="sum-trace-tree">';
        html += this._renderTraceNode(r.endpoint.callTree, controllerJar, esc, 0);
        html += '</div></div></div>';

        document.body.insertAdjacentHTML('beforeend', html);
    },

    _renderTraceNode(node, controllerJar, esc, depth) {
        if (!node || depth > 25) return '';
        const isExt = node.id && node.id.startsWith('ext:');
        const nodeJar = node.sourceJar || null;
        const isCrossModule = node.crossModule !== undefined ? node.crossModule : (!isExt && nodeJar !== controllerJar);
        const stereo = node.stereotype || '';
        const isRepo = stereo === 'REPOSITORY' || stereo === 'SPRING_DATA';

        let cls = 'sum-trace-node';
        if (isCrossModule) cls += ' sum-trace-external';
        if (isExt) cls += ' sum-trace-lib';
        if (isRepo) cls += ' sum-trace-db';

        let badge = '';
        if (stereo === 'CONTROLLER') badge = '<span class="badge badge-controller">CTRL</span>';
        else if (stereo === 'SERVICE') badge = '<span class="badge badge-service">SVC</span>';
        else if (isRepo) badge = '<span class="badge badge-repository">REPO</span>';
        else if (stereo === 'COMPONENT') badge = '<span class="badge badge-component">CMP</span>';
        else if (isExt) badge = `<span class="badge badge-${(stereo || 'other').toLowerCase()}">${esc(stereo || 'EXT')}</span>`;

        const modLabel = node.module || this._jarToProject(nodeJar || 'main');
        let moduleBadge = '';
        if (isCrossModule) moduleBadge = ` <span class="sum-module-tag">${esc(modLabel)}</span>`;

        const classKey = (node.className || node.simpleClassName || '').replace(/'/g, "\\'");
        const methodKey = (node.methodName || '').replace(/'/g, "\\'");
        let html = `<div class="${cls}" style="padding-left:${depth * 20}px">`;
        html += badge;
        html += ` <span class="sum-trace-class sum-clickable" onclick="JA.summary.showClassCode('${classKey}','${methodKey}');event.stopPropagation()" title="Click to view class code">${esc(node.simpleClassName || '?')}</span>`;
        html += `<span class="sum-trace-dot">.</span>`;
        html += `<span class="sum-trace-method sum-clickable" onclick="JA.summary.showClassCode('${classKey}','${methodKey}');event.stopPropagation()" title="Click to view method">${esc(node.methodName || '?')}</span>`;
        html += `<span class="sum-trace-return">(${esc((node.parameterTypes || []).join(', '))}) : ${esc(node.returnType || 'void')}</span>`;
        html += moduleBadge;
        html += this._dispatchBadge(node, esc);
        if (isCrossModule) html += ' <span class="sum-trace-ext-icon">EXTERNAL</span>';

        if (node.operationType) {
            html += ` <span class="sum-op-badge sum-op-${node.operationType.toLowerCase()}">${node.operationType}</span>`;
        }
        if (node.collectionsAccessed && node.collectionsAccessed.length) {
            for (const coll of node.collectionsAccessed) {
                const domain = (node.collectionDomains && node.collectionDomains[coll]) || '';
                html += ` <span class="sum-coll-badge sum-coll-data" title="${esc(domain)}">${esc(coll)}</span>`;
            }
        } else if (node.children && node.children.length) {
            const descColls = this._countDescendantCollections(node);
            if (descColls > 0) {
                html += ` <span class="sum-coll-badge sum-coll-desc" title="${descColls} collection(s) in subtree">${descColls} coll↓</span>`;
            }
        }
        if (node.annotationDetails && node.annotationDetails.length) {
            html += '<span class="sum-trace-anns">';
            for (const ad of node.annotationDetails) {
                const attrs = ad.attributes ? Object.entries(ad.attributes).map(([k,v]) => k+'='+JSON.stringify(v)).join(', ') : '';
                html += ` <span class="sum-trace-ann" title="${esc(attrs)}">@${esc(ad.name)}</span>`;
            }
            html += '</span>';
        }
        html += '</div>';

        if (node.children) {
            for (const child of node.children) {
                html += this._renderTraceNode(child, controllerJar, esc, depth + 1);
            }
        }
        return html;
    },

    _countDescendantCollections(node) {
        const set = new Set();
        this._collectDescendant(node, set);
        return set.size;
    },
    _collectDescendant(node, set, depth) {
        if (!depth) depth = 0;
        if (depth > 50) return; // guard against extremely deep trees
        if (node.collectionsAccessed) for (const c of node.collectionsAccessed) set.add(c);
        if (node.children) for (const ch of node.children) this._collectDescendant(ch, set, depth + 1);
    },

    /* ===== Node-by-Node Navigator ===== */
    _nodeNavState: null,

    async showNodeNav(idx) {
        const r = this._epReports[idx];
        if (!r) { JA.toast.warn('Endpoint not found at index ' + idx); return; }
        if (!r.endpoint || !r.endpoint.callTree) {
            const jarId2 = JA.app.currentJarId || '';
            const sourceIdx = r.sourceIdx != null ? r.sourceIdx : idx;
            try {
                const tree = await JA.api.getCallTree(jarId2, sourceIdx);
                if (tree) r.endpoint.callTree = tree;
            } catch (e) { /* fall through */ }
            if (!r.endpoint.callTree) { JA.toast.warn('No call tree available for this endpoint'); return; }
        }
        const jarId = JA.app.currentJarId || '';
        const epName = r.endpoint.controllerSimpleName + '.' + r.endpoint.methodName;
        const esc = JA.utils.escapeHtml;

        let nodesData = null;
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/endpoints/${encodeURIComponent(epName)}/nodes`);
            if (resp.ok) nodesData = await resp.json();
        } catch (e) { /* fallback below */ }

        if (!nodesData || !nodesData.nodes || !nodesData.nodes.length) {
            const flatNodes = [];
            this._flattenTree(r.endpoint.callTree, flatNodes, 0);
            nodesData = { endpoint: epName, totalNodes: flatNodes.length, nodes: flatNodes };
        }

        this._nodeNavState = { nodes: nodesData.nodes, currentIdx: 0, jarId, endpoint: epName };
        this._renderNodeNav(r, esc);
    },

    _flattenTree(node, list, depth) {
        if (!node || depth > 50) return;
        list.push({
            nodeId: list.length, depth,
            className: node.className, simpleClassName: node.simpleClassName,
            methodName: node.methodName, returnType: node.returnType,
            stereotype: node.stereotype, annotations: node.annotations,
            sourceCode: node.sourceCode || null,
            collectionsAccessed: node.collectionsAccessed || [],
            domain: node.domain, operationType: node.operationType,
            crossModule: node.crossModule, module: node.module,
            childCount: node.children ? node.children.length : 0,
            annotationDetails: node.annotationDetails || [],
            stringLiterals: node.stringLiterals || [],
            collectionDomains: node.collectionDomains || {},
            recursive: node.recursive || false,
            callType: node.callType || null,
            dispatchType: node.dispatchType || null,
            resolvedFrom: node.resolvedFrom || null,
            qualifierHint: node.qualifierHint || null
        });
        if (node.children) {
            for (const c of node.children) this._flattenTree(c, list, depth + 1);
        }
    },

    _renderNodeNav(r, esc) {
        const s = this._nodeNavState;
        if (!s) return;
        const node = s.nodes[s.currentIdx];
        const total = s.nodes.length;
        const idx = s.currentIdx;

        const old = document.getElementById('node-nav-overlay');
        if (old) old.remove();

        let html = '<div class="sum-trace-overlay" id="node-nav-overlay" onclick="if(event.target===this)this.remove()">';
        html += '<div class="sum-trace-panel" style="max-width:900px">';

        html += '<div class="sum-trace-header">';
        html += `<span class="endpoint-method method-${r.httpMethod}">${esc(r.httpMethod)}</span> `;
        html += `<span style="font-weight:700">${esc(r.fullPath)}</span>`;
        html += '<span class="sum-muted" style="margin-left:8px">Node ' + (idx + 1) + ' / ' + total + '</span>';
        html += '<button class="btn-sm" style="margin-left:auto" onclick="document.getElementById(\'node-nav-overlay\').remove()">Close</button>';
        html += '</div>';

        // Navigation bar
        html += '<div class="node-nav-bar">';
        html += `<button class="btn-sm" ${idx === 0 ? 'disabled' : ''} onclick="JA.summary._nodeNavGo(${idx - 1})">Back</button>`;
        html += '<span class="node-nav-breadcrumb">';
        const depthParts = [];
        for (let i = 0; i <= idx; i++) {
            if (s.nodes[i].depth < node.depth || i === idx) depthParts.push(s.nodes[i]);
        }
        const trail = depthParts.slice(-5);
        html += trail.map((n, ti) => {
            const isCurrent = (ti === trail.length - 1);
            const clickable = !isCurrent ? ` class="sum-clickable" onclick="JA.summary._nodeNavGo(${n.nodeId})"` : '';
            return `<span${clickable} style="${isCurrent ? 'font-weight:700;color:#2563eb' : 'color:#6b7280'}">${esc(n.simpleClassName || '?')}.${esc(n.methodName || '?')}</span>`;
        }).join(' > ');
        html += '</span>';
        html += `<button class="btn-sm" ${idx === total - 1 ? 'disabled' : ''} onclick="JA.summary._nodeNavGo(${idx + 1})">Next</button>`;
        html += '</div>';

        // Node detail card
        const stereo = node.stereotype || '';
        let badge = '';
        if (stereo === 'CONTROLLER') badge = '<span class="badge badge-controller">CTRL</span>';
        else if (stereo === 'SERVICE') badge = '<span class="badge badge-service">SVC</span>';
        else if (stereo === 'REPOSITORY' || stereo === 'SPRING_DATA') badge = '<span class="badge badge-repository">REPO</span>';
        else if (stereo === 'COMPONENT') badge = '<span class="badge badge-component">CMP</span>';
        else if (stereo) badge = `<span class="badge">${esc(stereo)}</span>`;

        const nodeClassKey = (node.className || node.simpleClassName || '').replace(/'/g, "\\'");
        const nodeMethodKey = (node.methodName || '').replace(/'/g, "\\'");
        html += '<div class="node-detail-card">';
        html += '<div class="node-detail-header">' + badge;
        html += ` <span style="font-size:15px;font-weight:700">${esc(node.simpleClassName || '?')}.${esc(node.methodName || '?')}</span>`;
        html += `<span class="sum-muted" style="margin-left:8px">${esc(node.className || '')}</span>`;
        html += this._dispatchBadge(node, esc);
        if (node.crossModule) html += ' <span class="sum-trace-ext-icon">EXTERNAL</span>';
        if (node.module) html += ` <span class="sum-module-tag">${esc(node.module)}</span>`;
        if (node.domain) html += ` <span class="sum-domain-tag">${esc(node.domain)}</span>`;
        html += ` <button class="btn-sm code-view-btn" style="margin-left:8px" onclick="JA.summary.showClassCode('${nodeClassKey}','${nodeMethodKey}');event.stopPropagation()">Code</button>`;
        html += '</div>';

        html += '<div class="node-meta-row">';
        html += `<span>Depth: ${node.depth}</span>`;
        html += `<span>Children: ${node.childCount}</span>`;
        if (node.returnType) html += `<span>Returns: ${esc(node.returnType)}</span>`;
        if (node.operationType) html += ` <span class="sum-op-badge sum-op-${node.operationType.toLowerCase()}">${esc(node.operationType)}</span>`;
        html += '</div>';

        if (node.collectionsAccessed && node.collectionsAccessed.length) {
            html += '<div class="node-meta-row">';
            for (const c of node.collectionsAccessed) {
                const dom = (node.collectionDomains && node.collectionDomains[c]) || '';
                html += `<span class="sum-coll-badge sum-coll-data" title="${esc(dom)}">${esc(c)}</span> `;
            }
            html += '</div>';
        }

        if (node.annotationDetails && node.annotationDetails.length) {
            html += '<div class="node-meta-row">';
            for (const ad of node.annotationDetails) {
                const attrs = ad.attributes ? Object.entries(ad.attributes).map(([k,v]) => k + '=' + JSON.stringify(v)).join(', ') : '';
                html += `<span class="sum-trace-ann" title="${esc(attrs)}">@${esc(ad.name)}</span> `;
            }
            html += '</div>';
        }

        if (node.sqlStatements && node.sqlStatements.length) {
            html += '<div class="node-source-block">';
            html += '<div class="node-source-label">SQL / JPQL Statements</div>';
            for (const sql of node.sqlStatements) {
                html += '<pre class="node-source-code node-sql-block">' + esc(sql) + '</pre>';
            }
            html += '</div>';
        }

        if (node.sourceCode) {
            html += '<div class="node-source-block">';
            html += '<div class="node-source-label">Actual Decompiled Source (CFR)</div>';
            html += '<pre class="node-source-code">' + esc(node.sourceCode) + '</pre>';
            html += '</div>';
        } else {
            html += '<div class="node-source-block"><div class="node-source-label sum-muted">No decompiled source available for this node</div></div>';
        }

        html += '</div>'; // node-detail-card
        html += '</div></div>'; // panel + overlay

        document.body.insertAdjacentHTML('beforeend', html);
    },

    _nodeNavGo(newIdx) {
        if (!this._nodeNavState) return;
        const s = this._nodeNavState;
        if (newIdx < 0 || newIdx >= s.nodes.length) return;

        const node = s.nodes[newIdx];
        if (!node.sourceCode && s.jarId && s.endpoint) {
            fetch(`/api/jar/jars/${encodeURIComponent(s.jarId)}/endpoints/${encodeURIComponent(s.endpoint)}/nodes/${newIdx}`)
                .then(r => r.ok ? r.json() : null)
                .then(data => {
                    if (data && data.sourceCode) s.nodes[newIdx] = { ...node, ...data };
                    s.currentIdx = newIdx;
                    const r = this._epReports.find(r => r.endpoint.controllerSimpleName + '.' + r.endpoint.methodName === s.endpoint);
                    if (r) this._renderNodeNav(r, JA.utils.escapeHtml);
                })
                .catch(() => {
                    s.currentIdx = newIdx;
                    const r = this._epReports.find(r => r.endpoint.controllerSimpleName + '.' + r.endpoint.methodName === s.endpoint);
                    if (r) this._renderNodeNav(r, JA.utils.escapeHtml);
                });
        } else {
            s.currentIdx = newIdx;
            const r = this._epReports.find(r => r.endpoint.controllerSimpleName + '.' + r.endpoint.methodName === s.endpoint);
            if (r) this._renderNodeNav(r, JA.utils.escapeHtml);
        }
    }
});
