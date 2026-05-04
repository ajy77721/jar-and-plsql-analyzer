/**
 * Endpoint Flow — Structured operation report.
 *
 * Click any class/method name to navigate (IntelliJ-style).
 * If target is an interface → implementation picker popup.
 * Collection detection: _VI_ or _V_ = VIEW, SB_ prefix = collection.
 */
window.JA = window.JA || {};

JA.callGraph = {

    _fullTree: null,
    _stack: [],
    _containerId: null,
    _classIndex: null,
    _endpoint: null,

    render(callTree, containerId, endpoint) {
        this._containerId = containerId;
        this._fullTree = callTree;
        this._endpoint = endpoint || null;
        this._stack = [];
        window._cgNodes = [];
        JA.nav._targets = [];

        this._buildClassIndex();
        JA.nav.init();

        if (!callTree) {
            document.getElementById(containerId).innerHTML =
                '<p class="graph-placeholder">No call tree available</p>';
            this._updateBreadcrumb();
            return;
        }

        this._pushAndRender(callTree);
    },

    _buildClassIndex() {
        this._classIndex = {};
        const analysis = JA.app.currentAnalysis;
        if (!analysis) return;
        const classSource = analysis.classIndex || analysis.classes || [];
        for (const cls of classSource) {
            this._classIndex[cls.fullyQualifiedName] = cls;
            if (!this._classIndex[cls.simpleName]) {
                this._classIndex[cls.simpleName] = cls;
            }
        }
    },

    /* ---- navigation ---- */

    openMethod(node) {
        if (!node || !node.children || node.children.length === 0) {
            JA.toast.warn('No further calls from this method', 2000);
            return;
        }
        this._pushAndRender(node);
        JA.toast.info((node.simpleClassName || '') + '.' + (node.methodName || ''), 1500);
    },

    goBack() {
        if (this._stack.length <= 1) return;
        this._stack.pop();
        const current = this._stack[this._stack.length - 1];
        this._renderReport(current.node);
        this._updateBreadcrumb();
    },

    goToRoot() {
        if (!this._fullTree) return;
        this._stack = [];
        this._pushAndRender(this._fullTree);
    },

    goToLevel(index) {
        if (index < 0) { this.goToRoot(); return; }
        this._stack = this._stack.slice(0, index + 1);
        const current = this._stack[this._stack.length - 1];
        this._renderReport(current.node);
        this._updateBreadcrumb();
    },

    /* ---- internal ---- */

    _pushAndRender(node) {
        this._stack.push({
            node: node,
            label: (node.simpleClassName || '') + '.' + (node.methodName || '')
        });
        this._renderReport(node);
        this._updateBreadcrumb();
    },

    _updateBreadcrumb() {
        const bar = document.getElementById('graph-breadcrumb-bar');
        const crumb = document.getElementById('graph-breadcrumb');
        if (!bar || !crumb) return;

        if (this._stack.length <= 1) { bar.style.display = 'none'; return; }
        bar.style.display = '';

        const esc = JA.utils.escapeHtml;
        let html = '';
        this._stack.forEach((entry, i) => {
            if (i > 0) html += ' <span class="crumb-sep">&#9654;</span> ';
            if (i < this._stack.length - 1) {
                html += `<span class="crumb-item crumb-link" onclick="JA.callGraph.goToLevel(${i})">${esc(entry.label)}</span>`;
            } else {
                html += `<span class="crumb-item crumb-current">${esc(entry.label)}</span>`;
            }
        });
        crumb.innerHTML = html;
    },

    /* ======== REPORT ======== */

    _renderReport(node) {
        const container = document.getElementById(this._containerId);
        if (!container) return;
        const esc = JA.utils.escapeHtml;

        const operations = [];
        this._flattenOps(node, 0, operations);
        const beans = this._collectBeans(operations);
        const collections = this._detectCollections(beans);

        let html = '<div class="ep-report">';
        html += this._htmlHeader(node, esc);
        html += this._htmlCallChain(beans, esc);
        if (collections.length > 0) {
            html += this._htmlCollections(collections, esc);
        }
        html += this._htmlOperations(operations, esc);
        html += '</div>';

        container.innerHTML = html;
    },

    /* ---- data extraction ---- */

    _flattenOps(node, depth, result) {
        if (!node) return;
        result.push({ node, depth, order: result.length + 1 });
        for (const child of (node.children || [])) {
            this._flattenOps(child, depth + 1, result);
        }
    },

    _collectBeans(operations) {
        const seen = new Set();
        const beans = [];
        for (const op of operations) {
            const key = op.node.className || op.node.simpleClassName;
            if (key && !seen.has(key)) {
                seen.add(key);
                beans.push({
                    className: op.node.className,
                    simpleClassName: op.node.simpleClassName,
                    stereotype: op.node.stereotype,
                    depth: op.depth
                });
            }
        }
        return beans;
    },

    _detectCollections(beans) {
        const collMap = new Map();
        for (const bean of beans) {
            const classData = this._classIndex[bean.className]
                || this._classIndex[bean.simpleClassName];
            if (!classData) continue;

            for (const ann of (classData.annotations || [])) {
                if (ann.name === 'Document' && ann.attributes) {
                    const cn = ann.attributes.collection || ann.attributes.value;
                    if (cn && !collMap.has(cn)) {
                        collMap.set(cn, { name: cn, type: this._classifyCollection(cn), source: bean.simpleClassName });
                    }
                }
            }
            for (const method of (classData.methods || [])) {
                for (const ann of (method.annotations || [])) {
                    for (const val of Object.values(ann.attributes || {})) {
                        for (const ref of this._extractCollRefs(String(val))) {
                            if (!collMap.has(ref)) {
                                collMap.set(ref, { name: ref, type: this._classifyCollection(ref), source: bean.simpleClassName + '.' + method.name });
                            }
                        }
                    }
                }
            }
        }
        return Array.from(collMap.values());
    },

    _classifyCollection(name) {
        if (!name) return 'DATA';
        if (name.includes('_VI_') || name.includes('_V_')) return 'VIEW';
        return 'DATA';
    },

    _extractCollRefs(str) {
        const refs = new Set();
        let m;
        const sbP = /\b(SB_[\w]+)/g;
        while ((m = sbP.exec(str)) !== null) refs.add(m[1]);
        const vP = /\b(\w+(?:_VI_|_V_)\w*)\b/g;
        while ((m = vP.exec(str)) !== null) refs.add(m[1]);
        return refs;
    },

    _describeMethod(node) {
        const name = node.methodName || '';
        const stereo = node.stereotype || 'OTHER';
        const readable = name.replace(/([A-Z])/g, ' $1').replace(/^./, s => s.toUpperCase()).trim();
        switch (stereo) {
            case 'REST_CONTROLLER': case 'CONTROLLER': return 'Handles HTTP request';
            case 'SERVICE':
                if (/^(get|find|fetch|load|retrieve|read)/i.test(name)) return 'Retrieves data — ' + readable;
                if (/^(save|create|add|insert|register|store)/i.test(name)) return 'Creates/saves — ' + readable;
                if (/^(update|modify|set|patch|edit|change)/i.test(name)) return 'Updates — ' + readable;
                if (/^(delete|remove|clear|purge)/i.test(name)) return 'Removes — ' + readable;
                if (/^(validate|check|verify|assert|ensure)/i.test(name)) return 'Validates — ' + readable;
                if (/^(convert|map|transform|format|parse|build)/i.test(name)) return 'Transforms — ' + readable;
                if (/^(send|notify|publish|emit|dispatch)/i.test(name)) return 'Notification — ' + readable;
                if (/^(process|handle|execute|run|perform)/i.test(name)) return 'Processes — ' + readable;
                return 'Business logic — ' + readable;
            case 'REPOSITORY': case 'SPRING_DATA':
                if (/^(find|get|read|query|search|fetch)/i.test(name)) return 'DB query — ' + readable;
                if (/^(save|insert|persist|upsert)/i.test(name)) return 'DB write — ' + readable;
                if (/^(delete|remove)/i.test(name)) return 'DB delete — ' + readable;
                if (/^(count)/i.test(name)) return 'DB count — ' + readable;
                if (/^(exists)/i.test(name)) return 'Existence check — ' + readable;
                if (/^(aggregate)/i.test(name)) return 'Aggregation — ' + readable;
                return 'Data access — ' + readable;
            case 'MONGODB': return 'MongoDB — ' + readable;
            case 'LOGGING': return 'Log — ' + readable;
            case 'HTTP': return 'HTTP — ' + readable;
            case 'SPRING': return 'Framework — ' + readable;
            case 'COMPONENT': return 'Component — ' + readable;
            default: return readable;
        }
    },

    _stereoLabel(s) {
        const m = { 'REST_CONTROLLER':'CTRL', 'CONTROLLER':'CTRL', 'SERVICE':'SVC', 'REPOSITORY':'REPO',
                    'COMPONENT':'COMP', 'CONFIGURATION':'CFG', 'ENTITY':'ENT', 'SPRING_DATA':'DATA',
                    'SPRING':'SPRING', 'MONGODB':'MONGO', 'LOGGING':'LOG', 'HTTP':'HTTP', 'JDK':'JDK', 'LIBRARY':'LIB' };
        return m[s] || 'OTHER';
    },

    /* ---- HTML builders ---- */

    _navLink(className, methodName, displayText, extraClass) {
        const esc = JA.utils.escapeHtml;
        const idx = JA.nav.ref(className, methodName);
        return `<span class="${extraClass || ''} ep-nav-link" onclick="JA.nav.click(${idx},event)">${esc(displayText)}</span>`;
    },

    _htmlHeader(node, esc) {
        const ep = this._endpoint;
        let html = '<div class="ep-header">';

        if (ep) {
            html += '<div class="ep-header-top">';
            html += `<span class="endpoint-method method-${ep.httpMethod || 'ALL'}">${esc(ep.httpMethod || 'ALL')}</span>`;
            html += `<span class="ep-header-path">${esc(ep.fullPath || '/')}</span>`;
            html += '</div>';
        }

        html += '<div class="ep-header-sig">';
        html += this._navLink(node.className, '', node.simpleClassName || '', 'ep-header-class');
        html += '<span class="ep-header-dot">.</span>';
        html += this._navLink(node.className, node.methodName, node.methodName || '', 'ep-header-mname');
        const params = (node.parameterTypes || []).join(', ');
        html += `<span class="ep-header-params">(${esc(params)})</span>`;
        if (node.returnType && node.returnType !== 'void') {
            html += `<span class="ep-header-return"> &rarr; ${esc(node.returnType)}</span>`;
        }
        html += '</div>';
        html += '</div>';
        return html;
    },

    _htmlCallChain(beans, esc) {
        if (beans.length === 0) return '';
        let html = '<div class="ep-section">';
        html += '<div class="ep-section-title">Call Chain</div>';
        html += '<div class="ep-bean-chain">';

        beans.forEach((bean, i) => {
            if (i > 0) html += '<span class="ep-chain-arrow">&rarr;</span>';
            const color = JA.utils.stereotypeColor(bean.stereotype);
            const label = this._stereoLabel(bean.stereotype);
            const idx = JA.nav.ref(bean.className, '');
            html += `<div class="ep-bean-chip ep-nav-link" onclick="JA.nav.click(${idx},event)" style="background:${color}12;color:${color};border-color:${color}40">`;
            html += `<span class="ep-bean-stereo">${esc(label)}</span> `;
            html += `<span class="ep-bean-name">${esc(bean.simpleClassName || '')}</span>`;
            html += '</div>';
        });

        html += '</div></div>';
        return html;
    },

    _htmlCollections(collections, esc) {
        let html = '<div class="ep-section">';
        html += '<div class="ep-section-title">Collections &amp; Views</div>';
        html += '<div class="ep-coll-list">';
        for (const coll of collections) {
            const isView = coll.type === 'VIEW';
            const cls = isView ? 'ep-coll-view' : 'ep-coll-data';
            const icon = isView ? '&#128065;' : '&#128230;';
            const typeLabel = isView ? 'VIEW' : 'COLLECTION';
            html += `<div class="ep-coll-tag ${cls}">`;
            html += `<span class="ep-coll-icon">${icon}</span>`;
            html += `<span class="ep-coll-name">${esc(coll.name)}</span>`;
            html += `<span class="ep-coll-type">${typeLabel}</span>`;
            html += '</div>';
        }
        html += '</div></div>';
        return html;
    },

    _htmlOperations(operations, esc) {
        const count = operations.length;
        let html = '<div class="ep-section">';
        html += `<div class="ep-section-title">Operation Flow (${count} step${count !== 1 ? 's' : ''})</div>`;

        // Dispatch legend — shown once above the list so users know what each badge means
        const hasAmbiguous  = operations.some(o => o.node.dispatchType === 'AMBIGUOUS_IMPL');
        const hasRecursive  = operations.some(o => o.node.recursive || o.node.dispatchType === 'RECURSIVE');
        const hasQualified  = operations.some(o => o.node.dispatchType === 'QUALIFIED');
        const hasHeuristic  = operations.some(o => o.node.dispatchType === 'HEURISTIC');
        const hasDynamic    = operations.some(o => o.node.dispatchType === 'DYNAMIC_DISPATCH');
        const hasPrimary    = operations.some(o => o.node.dispatchType === 'PRIMARY');
        const hasIfaceFall  = operations.some(o => o.node.dispatchType === 'INTERFACE_FALLBACK');
        const hasListInject = operations.some(o => o.node.dispatchType === 'LIST_INJECT');
        const hasDefaultMeth= operations.some(o => o.node.dispatchType === 'DEFAULT_METHOD');
        const anyBadge = hasAmbiguous || hasRecursive || hasQualified || hasHeuristic || hasDynamic || hasPrimary || hasIfaceFall || hasListInject || hasDefaultMeth;
        if (anyBadge) {
            html += '<div class="ep-dispatch-legend">';
            html += '<span class="ep-legend-label">Dispatch legend:</span>';
            if (hasAmbiguous)  html += ' <span class="ep-legend-item" style="color:#f97316;border-color:#f97316">AMBIGUOUS — multiple implementations, could not narrow to one</span>';
            if (hasRecursive)  html += ' <span class="ep-legend-item" style="color:#ef4444;border-color:#ef4444">RECURSIVE — method calls itself (back-edge in call stack)</span>';
            if (hasQualified)  html += ' <span class="ep-legend-item" style="color:#10b981;border-color:#10b981">QUALIFIED — resolved via @Qualifier bean name</span>';
            if (hasHeuristic)  html += ' <span class="ep-legend-item" style="color:#6366f1;border-color:#6366f1">HEURISTIC — resolved via field name / segment match</span>';
            if (hasDynamic)    html += ' <span class="ep-legend-item" style="color:#f59e0b;border-color:#f59e0b">DYNAMIC — runtime dispatch, all implementations shown</span>';
            if (hasPrimary)    html += ' <span class="ep-legend-item" style="color:#8b5cf6;border-color:#8b5cf6">@PRIMARY — resolved via @Primary bean</span>';
            if (hasIfaceFall)  html += ' <span class="ep-legend-item" style="color:#ef4444;border-color:#ef4444">IFACE ONLY — no implementation found, showing interface</span>';
            if (hasListInject) html += ' <span class="ep-legend-item" style="color:#06b6d4;border-color:#06b6d4">LIST&lt;T&gt; — all beans collected via List&lt;Interface&gt; injection</span>';
            if (hasDefaultMeth)html += ' <span class="ep-legend-item" style="color:#64748b;border-color:#64748b">DEFAULT — interface default method (Java 8+)</span>';
            html += '</div>';
        }

        html += '<div class="ep-ops-list">';

        for (const op of operations) {
            const node = op.node;
            const color = JA.utils.stereotypeColor(node.stereotype);
            const label = this._stereoLabel(node.stereotype);
            const desc = this._describeMethod(node);
            const hasChildren = (node.children || []).length > 0;
            const childCount = (node.children || []).length;
            const indent = op.depth * 20;

            let cls = 'ep-op';
            let onclick = '';
            if (hasChildren && op.order > 1) {
                cls += ' ep-op-drillable';
                onclick = ` onclick="JA.callGraph.openMethod(${this._nodeRef(node)})"`;
            }

            html += `<div class="${cls}" style="margin-left:${indent}px"${onclick}>`;
            html += `<div class="ep-op-num" style="background:${color}">${op.order}</div>`;
            html += '<div class="ep-op-content">';

            // Clickable class.method
            html += '<div class="ep-op-header">';
            html += `<span class="ep-op-stereo" style="color:${color}">[${esc(label)}]</span> `;
            html += this._navLink(node.className, '', node.simpleClassName || '', 'ep-op-class');
            html += '<span class="ep-op-dot">.</span>';
            html += this._navLink(node.className, node.methodName, node.methodName || '', 'ep-op-method');
            if (hasChildren && op.order > 1) {
                html += `<span class="ep-op-drill">&#9654; ${childCount}</span>`;
            }
            // Dispatch type badge for interface/implementation resolution
            const effectiveDt = (node.recursive || node.dispatchType === 'RECURSIVE') ? 'RECURSIVE' : node.dispatchType;
            if (effectiveDt && effectiveDt !== 'DIRECT') {
                const dtColors = { DYNAMIC_DISPATCH: '#f59e0b', QUALIFIED: '#10b981',
                                   HEURISTIC: '#6366f1', INTERFACE_FALLBACK: '#ef4444',
                                   PRIMARY: '#8b5cf6',  AMBIGUOUS_IMPL: '#f97316',
                                   RECURSIVE: '#ef4444', LIST_INJECT: '#06b6d4',
                                   DEFAULT_METHOD: '#64748b' };
                const dtColor = dtColors[effectiveDt] || '#6b7280';
                const resolvedTip = node.resolvedFrom || 'n/a';
                let dtLabel;
                if (effectiveDt === 'AMBIGUOUS_IMPL') {
                    dtLabel = node.qualifierHint ? `AMBIGUOUS (${node.qualifierHint})` : 'AMBIGUOUS';
                } else if (effectiveDt === 'RECURSIVE') {
                    dtLabel = '↺ RECURSIVE — back-edge';
                } else if (effectiveDt === 'QUALIFIED') {
                    dtLabel = node.qualifierHint ? `QUALIFIED @${node.qualifierHint}` : 'QUALIFIED';
                } else if (effectiveDt === 'HEURISTIC') {
                    dtLabel = node.qualifierHint ? `HEURISTIC (field: ${node.qualifierHint})` : 'HEURISTIC';
                } else if (effectiveDt === 'INTERFACE_FALLBACK') {
                    dtLabel = 'IFACE ONLY — no impl found';
                } else if (effectiveDt === 'PRIMARY') {
                    dtLabel = '@PRIMARY bean';
                } else if (effectiveDt === 'DYNAMIC_DISPATCH') {
                    dtLabel = 'DYNAMIC';
                } else if (effectiveDt === 'LIST_INJECT') {
                    dtLabel = node.qualifierHint ? `LIST<T> ${node.qualifierHint}` : 'LIST<T> injection';
                } else if (effectiveDt === 'DEFAULT_METHOD') {
                    dtLabel = 'DEFAULT method (Java 8+)';
                } else {
                    dtLabel = effectiveDt;
                }
                html += ` <span class="ep-dispatch-badge" style="color:${dtColor};border-color:${dtColor}"
                           title="Resolved from: ${esc(resolvedTip)}">${esc(dtLabel)}</span>`;
            }
            if (node.resolvedFrom && effectiveDt !== 'RECURSIVE') {
                const ifaceSimple = node.resolvedFrom.split('.').pop();
                html += ` <span class="ep-resolved-from" title="${esc(node.resolvedFrom)}">via ${esc(ifaceSimple)}</span>`;
            }
            html += '</div>';

            html += this._htmlDispatchDetail(node, esc);

            html += `<div class="ep-op-desc">${esc(desc)}</div>`;

            const params = node.parameterTypes || [];
            if (params.length > 0) {
                html += `<div class="ep-op-meta">Params: ${esc(params.join(', '))}</div>`;
            }
            if (node.returnType && node.returnType !== 'void') {
                html += `<div class="ep-op-meta">Returns: ${esc(node.returnType)}</div>`;
            }
            if (node.recursive) {
                html += '<div class="ep-op-recursive">&#8635; Recursive</div>';
            }

            html += '</div></div>';
        }

        html += '</div></div>';
        return html;
    },

    _nodeRef(node) {
        if (!window._cgNodes) window._cgNodes = [];
        const idx = window._cgNodes.length;
        window._cgNodes.push(node);
        return `window._cgNodes[${idx}]`;
    },

    drillToImpl(ambiguousNode, implIdx) {
        const impl = (ambiguousNode.children || [])[implIdx];
        if (!impl) return;
        this._stack.push({ node: ambiguousNode, label: (ambiguousNode.simpleClassName || '') + '.' + (ambiguousNode.methodName || '') });
        this._stack.push({ node: impl,          label: (impl.simpleClassName || '')          + '.' + (impl.methodName || '') });
        this._renderReport(impl);
        this._updateBreadcrumb();
    },

    _htmlDispatchDetail(node, esc) {
        const dt = node.dispatchType;
        const isRec = node.recursive || dt === 'RECURSIVE';
        const effectiveDt = isRec ? 'RECURSIVE' : dt;
        if ((!effectiveDt || effectiveDt === 'DIRECT') && !isRec) return '';

        const palette = {
            AMBIGUOUS_IMPL:    { color: '#f97316', bg: 'rgba(249,115,22,0.07)',   border: '#fdba74' },
            RECURSIVE:         { color: '#ef4444', bg: 'rgba(239,68,68,0.07)',    border: '#fca5a5' },
            QUALIFIED:         { color: '#10b981', bg: 'rgba(16,185,129,0.07)',   border: '#6ee7b7' },
            HEURISTIC:         { color: '#6366f1', bg: 'rgba(99,102,241,0.07)',   border: '#a5b4fc' },
            DYNAMIC_DISPATCH:  { color: '#f59e0b', bg: 'rgba(245,158,11,0.07)',   border: '#fcd34d' },
            PRIMARY:           { color: '#8b5cf6', bg: 'rgba(139,92,246,0.07)',   border: '#c4b5fd' },
            INTERFACE_FALLBACK:{ color: '#ef4444', bg: 'rgba(239,68,68,0.07)',    border: '#fca5a5' },
            LIST_INJECT:       { color: '#06b6d4', bg: 'rgba(6,182,212,0.07)',    border: '#67e8f9' },
            DEFAULT_METHOD:    { color: '#64748b', bg: 'rgba(100,116,139,0.07)',  border: '#94a3b8' },
        };
        const key = effectiveDt || 'RECURSIVE';
        const p = palette[key] || { color: '#6b7280', bg: 'rgba(107,114,128,0.07)', border: '#d1d5db' };

        let html = `<div class="ep-dispatch-detail" style="border-color:${p.border};background:${p.bg}">`;

        if (effectiveDt === 'AMBIGUOUS_IMPL') {
            const candidates = node.children || [];
            const iface = node.resolvedFrom ? node.resolvedFrom.split('.').pop() : '';
            const ambRef = this._nodeRef(node);
            html += `<span class="ep-detail-label" style="color:${p.color}">⚠ ${candidates.length} possible implementations${iface ? ` of <em>${esc(iface)}</em>` : ''} — select one to trace:</span>`;
            if (candidates.length > 0) {
                html += '<div class="ep-impl-chips">';
                candidates.forEach((c, idx) => {
                    html += `<button class="ep-impl-chip" style="border-color:${p.border};color:${p.color}" onclick="JA.callGraph.drillToImpl(${ambRef},${idx});event.stopPropagation()">&#9654; ${esc(c.simpleClassName || c.className || '?')}</button>`;
                });
                html += '</div>';
            }

        } else if (effectiveDt === 'QUALIFIED') {
            const resolved = (node.children || [])[0];
            html += `<span class="ep-detail-label" style="color:${p.color}">QUALIFIED — @Qualifier("${esc(node.qualifierHint || '')}")`;
            if (resolved) html += ` resolved to: ${this._navLink(resolved.className, resolved.methodName, resolved.simpleClassName || resolved.className || '?', '')}`;
            html += '</span>';

        } else if (effectiveDt === 'HEURISTIC') {
            html += `<span class="ep-detail-label" style="color:${p.color}">HEURISTIC — field name <em>"${esc(node.qualifierHint || '')}"</em> matched`;
            if (node.resolvedFrom) html += ` ${esc(node.resolvedFrom.split('.').pop())}`;
            html += '</span>';

        } else if (effectiveDt === 'PRIMARY') {
            const resolved = (node.children || [])[0];
            html += `<span class="ep-detail-label" style="color:${p.color}">@PRIMARY bean selected`;
            if (resolved) html += ` → ${esc(resolved.simpleClassName || resolved.className || '?')}`;
            html += '</span>';

        } else if (effectiveDt === 'INTERFACE_FALLBACK') {
            html += `<span class="ep-detail-label" style="color:${p.color}">IFACE ONLY — no implementation found in analyzed classes`;
            if (node.resolvedFrom) html += ` (${esc(node.resolvedFrom.split('.').pop())})`;
            html += '</span>';

        } else if (effectiveDt === 'RECURSIVE' || isRec) {
            html += `<span class="ep-detail-label" style="color:${p.color}">&#8635; Recursive — this method calls itself; traversal stopped to prevent infinite loop.</span>`;

        } else if (effectiveDt === 'DYNAMIC_DISPATCH') {
            const candidates = node.children || [];
            html += `<span class="ep-detail-label" style="color:${p.color}">DYNAMIC — runtime dispatch across ${candidates.length} implementation${candidates.length !== 1 ? 's' : ''}:</span>`;
            if (candidates.length > 0) {
                const ambRef = this._nodeRef(node);
                html += '<div class="ep-impl-chips">';
                candidates.forEach((c, idx) => {
                    html += `<button class="ep-impl-chip" style="border-color:${p.border};color:${p.color}" onclick="JA.callGraph.drillToImpl(${ambRef},${idx});event.stopPropagation()">&#9654; ${esc(c.simpleClassName || c.className || '?')}</button>`;
                });
                html += '</div>';
            }

        } else if (effectiveDt === 'LIST_INJECT') {
            const candidates = node.children || [];
            html += `<span class="ep-detail-label" style="color:${p.color}">LIST&lt;T&gt; — all ${candidates.length} implementations injected via List&lt;${esc(node.qualifierHint || 'Interface')}&gt;</span>`;

        } else if (effectiveDt === 'DEFAULT_METHOD') {
            html += `<span class="ep-detail-label" style="color:${p.color}">DEFAULT method — Java 8+ default interface method; no concrete override exists.</span>`;
        }

        html += '</div>';
        return html;
    }
};
