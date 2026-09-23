/**
 * Summary trace — flat Dynatrace-style trace overlay + node-by-node navigator.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /**
     * Disables the Trace buttons for one endpoint while its tree loads.
     *
     * <p>Matched by their onclick, because these buttons are rendered inline from several places
     * (summary-tables, summary-claude) and carry no shared class or id to target.
     */
    _setTraceButtonsBusy(idx, busy) {
        document.querySelectorAll('button[onclick*="showTrace(' + idx + ')"]').forEach(function (b) {
            b.disabled = busy;
            b.style.opacity = busy ? '0.55' : '';
            b.style.cursor = busy ? 'progress' : '';
            if (busy) {
                if (!b.dataset.label) b.dataset.label = b.textContent;
                b.textContent = 'Loading…';
            } else if (b.dataset.label) {
                b.textContent = b.dataset.label;
                delete b.dataset.label;
            }
        });
    },

    /* ===== Flat Trace Overlay ===== */
    /** Endpoints whose trace is being fetched, so a second click is a no-op rather than a queue. */
    _traceLoading: {},

    /**
     * Rows the trace tree will build — UI-021.
     *
     * <p>Both walks below were unbounded. `_renderTraceNode` recursed the whole tree into one HTML
     * string, which is the render the comment in `showTrace` measures at <b>120 MB</b>;
     * `_flattenTree` capped *depth* at 50 and left breadth alone, which is the same mistake
     * `_flattenOps` had — one measured endpoint expands to 300,167,238 nodes, and it gets there by
     * breadth, not by depth.
     *
     * <p>2,000 to match `JA.callGraph.MAX_FLATTENED_OPS`: the two views sit beside each other and a
     * reader comparing them should not have to discover that they stop at different sizes.
     */
    MAX_TRACE_NODES: 2000,

    /**
     * Nodes in the whole tree, so a truncation notice can say what it truncated.
     *
     * <p>Iterative: the flatten walk guarded depth at 50 because the trees are deep enough for
     * that to matter, and a recursive count would fail on exactly the trees this cap is for.
     * `recursive` nodes are counted and not followed, the rule the renderer uses.
     */
    _countTreeNodes(root) {
        if (!root) return 0;
        // `getCallTree`'s payload is a DAG lazily inflated to `PA.dag.INITIAL_DEPTH` levels
        // (shared/page-envelope.js): the SAME underlying node is legitimately reachable via many
        // different call paths (a shared utility method called from thousands of sites), and
        // `inflate()` keeps it deduped in one dictionary (`root._dag`, size `root._dagNodeCount`)
        // for exactly that reason — expanding every frontier node found while walking `children`
        // (an earlier version of this fix tried that) re-materialises the same subtree once per
        // path that reaches it, reproducing the same combinatorial blow-up the DAG format exists
        // to avoid (confirmed: it crashed the browser tab on a real endpoint). The dictionary
        // already has the true, deduplicated total — no walk needed at all.
        if (root._dag && typeof root._dagNodeCount === 'number') return root._dagNodeCount;
        let n = 0;
        const stack = [root];
        while (stack.length) {
            const node = stack.pop();
            n++;
            if (node.children && !node.recursive) {
                for (const c of node.children) stack.push(c);
            }
        }
        return n;
    },

    async showTrace(idx) {
        const r = this._epReports[idx];
        if (!r) { JA.toast.warn('Endpoint not found at index ' + idx); return; }

        // Ignore repeat clicks while one is in flight.
        //
        // JA.api.getCallTree already collapses concurrent fetches into a single request, so extra
        // clicks were not extra HTTP traffic — but the button still looked dead, so people kept
        // clicking, and each click queued another render of a tree that can reach 120 MB. Refusing
        // here, visibly, is what actually stops that.
        if (JA.summary._traceLoading[idx]) {
            JA.toast.info('Still building this call tree — it can take a minute on a large endpoint.');
            return;
        }

        if (!r.endpoint || !r.endpoint.callTree) {
            JA.summary._traceLoading[idx] = true;
            JA.summary._setTraceButtonsBusy(idx, true);
            // Say something before the wait, not after it. Building a call tree for a large
            // endpoint takes seconds, and the silence is what made people click again — four
            // clicks, four concurrent builds. JA.api.getCallTree now collapses those into one
            // request, but the honest fix for the clicking is telling the user it started.
            const _busy = JA.toast.loading ? JA.toast.loading('Building call tree…') : null;
            const jarId = JA.app.currentJarId || '';
            const sourceIdx = r.sourceIdx != null ? r.sourceIdx : idx;
            try {
                // sourceIdx indexes the endpoint array fetched for JA.app._currentVersion, and the server
                // resolves the index against whichever version it is given. Omitting version here meant a
                // screen showing STATIC or CORRECTED_PREV resolved its index against the CURRENT document
                // and rendered a different endpoint's call tree — deterministic, not a race.
                const tree = await JA.api.getCallTree(jarId, sourceIdx, JA.app._currentVersion);
                if (tree) r.endpoint.callTree = tree;
            } catch (e) { /* fall through */ }
            finally {
                if (_busy && JA.toast.dismiss) JA.toast.dismiss(_busy);
                delete JA.summary._traceLoading[idx];
                JA.summary._setTraceButtonsBusy(idx, false);
            }
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
        // The budget is carried in an object so the recursion shares one counter. A depth-local
        // limit would bound each branch and not the total, which is how a tree that is wide rather
        // than deep slips through a cap that looks present.
        const budget = { left: this.MAX_TRACE_NODES };
        // Same DAG-frontier issue as §238's "Nodes" browser fix: `getCallTree` only materialises
        // the first `PA.dag.INITIAL_DEPTH` (3) levels up front; a node past that carries
        // `_dagChildIds`/an empty `children`, meant to be expanded on demand. Unlike the "Nodes"
        // browser (which lists every UNIQUE node once, where expanding-per-path re-explodes a
        // shared node's subtree once per path that reaches it), a TRACE legitimately re-displays
        // the same method once per call path — that IS what a trace means — so expanding on demand
        // as the walk reaches each frontier node is the correct fix here, not a dictionary-iterate
        // rewrite. Safe because this walk is already hard-bounded by `budget`/`depth > 25` above
        // and below, unlike `_countTreeNodes`'s old unbounded walk that crashed the tab.
        const dict = r.endpoint.callTree._dag || null;
        html += this._renderTraceNode(r.endpoint.callTree, controllerJar, esc, 0, budget, dict);
        if (budget.left <= 0) {
            // Stated with the total, not just the cap. "Showing 2,000 rows" cannot be told apart
            // from "this endpoint has 2,000 rows", and the second is a wrong answer that looks
            // like a complete one.
            const total = this._countTreeNodes(r.endpoint.callTree);
            html += '<div class="cg-truncated">Showing the first '
                 + this.MAX_TRACE_NODES.toLocaleString() + ' of ' + total.toLocaleString()
                 + ' calls. This endpoint\'s call tree is too large to render whole.</div>';
        }
        html += '</div></div></div>';

        JA.mountOverlay(html);
    },

    _renderTraceNode(node, controllerJar, esc, depth, budget, dict) {
        // Order matters: the depth check comes first, so a node that is skipped for being too deep
        // does not spend a row of the budget. Decrementing before it made the ceiling arrive early
        // on deep trees by exactly the number of nodes never drawn.
        if (!node || depth > 25) return '';
        if (budget && budget.left <= 0) return '';
        if (budget) budget.left--;
        dict = dict || node._dag || null;
        if (node._dagChildIds && dict) PA.dag.inflateChildren(node, dict, 1);
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
        // Capped at 12 levels. Real traces reach depth 71; at 20px a level that is 1,420px of
        // padding and the method name sits outside the panel entirely. Past the cap the depth is
        // carried by the badge below instead of by more whitespace.
        const _cap = 12;
        const _indent = Math.min(depth, _cap) * 20;
        let html = `<div class="${cls}" style="padding-left:${_indent}px">`;
        if (depth > _cap) {
            html += `<span class="ct-depth-badge" title="Call depth ${depth} — indent capped at ${_cap}">L${depth}</span>`;
        }
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
                if (budget && budget.left <= 0) break;
                html += this._renderTraceNode(child, controllerJar, esc, depth + 1, budget, dict);
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
    // Monotonic navigation token. MUST be initialised: `++undefined` is NaN and `NaN !== NaN`,
    // so an uninitialised counter makes the staleness guard reject every response and the panel
    // never updates at all.
    _nodeNavSeq: 0,

    async showNodeNav(idx) {
        const r = this._epReports[idx];
        if (!r) { JA.toast.warn('Endpoint not found at index ' + idx); return; }
        if (!r.endpoint || !r.endpoint.callTree) {
            const jarId2 = JA.app.currentJarId || '';
            const sourceIdx = r.sourceIdx != null ? r.sourceIdx : idx;
            try {
                const tree = await JA.api.getCallTree(jarId2, sourceIdx, JA.app._currentVersion);
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
            // totalNodes is the tree's size, not the slice's. Reporting flatNodes.length after
            // the cap would say a 300,000-node endpoint has 2,000 — the count is what a reader
            // uses to judge whether they are seeing everything.
            nodesData = {
                endpoint: epName,
                totalNodes: flatNodes.truncated
                    ? this._countTreeNodes(r.endpoint.callTree) : flatNodes.length,
                truncated: flatNodes.truncated === true,
                nodes: flatNodes
            };
        }

        this._nodeNavState = { nodes: nodesData.nodes, currentIdx: 0, jarId, endpoint: epName };
        this._renderNodeNav(r, esc);
    },

    /**
     * A DAG-backed tree (`getCallTree`'s normal shape, per `shared/page-envelope.js`) keeps every
     * distinct method ONE dictionary entry, since the same method is legitimately reachable via
     * many call paths (a shared utility called from thousands of sites). Browsing "every node"
     * means every DICTIONARY entry, once each — walking `children` and expanding frontier nodes
     * as they're found (an earlier version of this fix tried that) re-materialises the same
     * subtree once per path that reaches it, reproducing the same combinatorial blow-up the DAG
     * format exists to avoid (confirmed: it crashed the browser tab on a real endpoint). Iterating
     * the dictionary directly is O(unique nodes), never re-visits anything, and needs no expansion
     * at all — the dictionary already holds every node's full record.
     */
    _flattenDagDict(dict, list) {
        for (const id in dict) {
            if (list.length >= JA.summary.MAX_TRACE_NODES) { list.truncated = true; return; }
            const node = dict[id];
            list.push({
                nodeId: list.length, depth: 0,
                className: node.className, simpleClassName: node.simpleClassName,
                methodName: node.methodName, returnType: node.returnType,
                stereotype: node.stereotype, annotations: node.annotations,
                sourceCode: node.sourceCode || null,
                collectionsAccessed: node.collectionsAccessed || [],
                domain: node.domain, operationType: node.operationType,
                crossModule: node.crossModule, module: node.module,
                childCount: node.childIds ? node.childIds.length : 0,
                annotationDetails: node.annotationDetails || [],
                stringLiterals: node.stringLiterals || [],
                collectionDomains: node.collectionDomains || {},
                recursive: node.recursive || false,
                callType: node.callType || null,
                dispatchType: node.dispatchType || null,
                resolvedFrom: node.resolvedFrom || null,
                qualifierHint: node.qualifierHint || null
            });
        }
    },

    _flattenTree(node, list, depth) {
        // Node count as well as depth. The depth guard alone bounded nothing that matters: a tree
        // 50 deep is small, and a tree 3 deep with 100,000 children at each level is not.
        if (!node || depth > 50) return;
        if (list.length >= JA.summary.MAX_TRACE_NODES) {
            list.truncated = true;
            return;
        }
        // DAG-backed root: use the dictionary walk above instead, which is what actually gives an
        // accurate, deduplicated "every node" list — see `_flattenDagDict`'s own comment.
        if (depth === 0 && node._dag) { this._flattenDagDict(node._dag, list); return; }
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
            return `<span${clickable} style="${isCurrent ? 'font-weight:700;color:var(--blue)' : 'color:var(--text-muted)'}">${esc(n.simpleClassName || '?')}.${esc(n.methodName || '?')}</span>`;
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

        JA.mountOverlay(html);
    },

    _nodeNavGo(newIdx) {
        if (!this._nodeNavState) return;
        const s = this._nodeNavState;
        if (newIdx < 0 || newIdx >= s.nodes.length) return;

        // Every navigation takes a token, and a response only applies if it is still the newest
        // one. Without this, clicking through nodes quickly lets a SLOWER EARLIER request land
        // after a faster later one: it would set currentIdx back and re-render the node you had
        // already moved past. The same applies if you close the panel and open another endpoint
        // mid-flight — hence the identity check on the state object itself, not just the index.
        // Mirrors PA.sourceModal._reqSeq/_isCurrent on the PL/SQL side.
        const token = ++this._nodeNavSeq;
        const isCurrent = () => token === this._nodeNavSeq && this._nodeNavState === s;

        const apply = () => {
            if (!isCurrent()) return;   // superseded by a later navigation
            s.currentIdx = newIdx;
            const r = this._epReports.find(r => r.endpoint.controllerSimpleName + '.' + r.endpoint.methodName === s.endpoint);
            if (r) this._renderNodeNav(r, JA.utils.escapeHtml);
        };

        const node = s.nodes[newIdx];
        if (!node.sourceCode && s.jarId && s.endpoint) {
            fetch(`/api/jar/jars/${encodeURIComponent(s.jarId)}/endpoints/${encodeURIComponent(s.endpoint)}/nodes/${newIdx}`)
                .then(r => r.ok ? r.json() : null)
                .then(data => {
                    if (!isCurrent()) return;
                    if (data && data.sourceCode) s.nodes[newIdx] = { ...node, ...data };
                    apply();
                })
                .catch(apply);
        } else {
            apply();   // already loaded; same guard so ordering is uniform
        }
    }
});
