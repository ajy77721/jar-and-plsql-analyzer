/**
 * Summary explorer — interactive drill-down call trace viewer.
 * Breadcrumb navigation, scope stats, collections at each level.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _traceState: null, // { tree, trail: [node,...], epReport, jarId }

    async showCallTrace(idx) {
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
        const root = r.endpoint.callTree;
        this._traceState = { tree: root, trail: [root], epReport: r, jarId: JA.app.currentJarId || '' };
        this._renderCallTrace();
    },

    _traceNavigateTo(node) {
        if (!this._traceState) return;
        const s = this._traceState;
        const existIdx = s.trail.indexOf(node);
        if (existIdx >= 0) {
            s.trail = s.trail.slice(0, existIdx + 1);
        } else {
            s.trail.push(node);
        }
        this._renderCallTrace();
    },

    _traceBack() {
        if (!this._traceState || this._traceState.trail.length <= 1) return;
        this._traceState.trail.pop();
        this._renderCallTrace();
    },

    // ---- dispatch helpers (shared by node card + children list) ----

    _DT_META: {
        AMBIGUOUS_IMPL:    { color: '#f97316', bg: 'rgba(249,115,22,0.08)', border: '#fdba74' },
        RECURSIVE:         { color: '#ef4444', bg: 'rgba(239,68,68,0.08)',  border: '#fca5a5' },
        QUALIFIED:         { color: '#10b981', bg: 'rgba(16,185,129,0.08)', border: '#6ee7b7' },
        HEURISTIC:         { color: '#6366f1', bg: 'rgba(99,102,241,0.08)', border: '#a5b4fc' },
        DYNAMIC_DISPATCH:  { color: '#f59e0b', bg: 'rgba(245,158,11,0.08)', border: '#fcd34d' },
        PRIMARY:           { color: '#8b5cf6', bg: 'rgba(139,92,246,0.08)', border: '#c4b5fd' },
        INTERFACE_FALLBACK:{ color: '#ef4444', bg: 'rgba(239,68,68,0.08)',  border: '#fca5a5' },
        LIST_INJECT:       { color: '#06b6d4', bg: 'rgba(6,182,212,0.08)',   border: '#67e8f9' },
        DEFAULT_METHOD:    { color: '#64748b', bg: 'rgba(100,116,139,0.08)', border: '#94a3b8' },
    },

    /** Returns the effective dispatch type, treating recursive nodes as 'RECURSIVE'
     *  regardless of their raw dispatchType (which is 'DIRECT' at the backend). */
    _effectiveDt(node) {
        if (node.recursive || node.callType === 'RECURSIVE') return 'RECURSIVE';
        return node.dispatchType;
    },

    /** Full dispatch badge — matches callGraph.js style */
    _dispatchBadge(node, esc) {
        const dt = this._effectiveDt(node);
        if (!dt || dt === 'DIRECT') return '';
        const m = this._DT_META[dt] || { color: '#6b7280', bg: 'rgba(107,114,128,0.08)', border: '#d1d5db' };
        let label;
        if (dt === 'AMBIGUOUS_IMPL')     label = node.qualifierHint ? `⚠ AMBIGUOUS (${node.qualifierHint})` : '⚠ AMBIGUOUS';
        else if (dt === 'RECURSIVE')     label = '↺ RECURSIVE — back-edge';
        else if (dt === 'QUALIFIED')     label = node.qualifierHint ? `QUALIFIED @${node.qualifierHint}` : 'QUALIFIED';
        else if (dt === 'HEURISTIC')     label = node.qualifierHint ? `HEURISTIC (field: ${node.qualifierHint})` : 'HEURISTIC';
        else if (dt === 'INTERFACE_FALLBACK') label = 'IFACE ONLY — no impl found';
        else if (dt === 'PRIMARY')       label = '@PRIMARY bean';
        else if (dt === 'DYNAMIC_DISPATCH') label = 'DYNAMIC';
        else if (dt === 'LIST_INJECT')   label = node.qualifierHint ? `LIST<T> ${node.qualifierHint}` : 'LIST<T> injection';
        else if (dt === 'DEFAULT_METHOD') label = 'DEFAULT method (Java 8+)';
        else label = dt;

        const tip = node.resolvedFrom ? `Resolved from: ${node.resolvedFrom}` : label;
        let html = ` <span class="sum-trace-ext-icon" style="color:${m.color};background:${m.bg};border-color:${m.border}" title="${esc(tip)}">${esc(label)}</span>`;

        // "via InterfaceName" tag — shown when not recursive
        if (node.resolvedFrom && dt !== 'RECURSIVE' && dt !== 'LIST_INJECT') {
            const iface = node.resolvedFrom.split('.').pop();
            html += ` <span style="font-size:10px;color:#94a3b8;font-style:italic">via ${esc(iface)}</span>`;
        }
        return html;
    },

    /** Dispatch legend block — shown once when children have non-trivial dispatch */
    _dispatchLegend(nodes, esc) {
        const types = new Set(nodes.map(n => this._effectiveDt(n)).filter(dt => dt && dt !== 'DIRECT'));
        if (!types.size) return '';
        const m = this._DT_META;
        let html = '<div class="trace-dispatch-legend">';
        html += '<span class="trace-legend-label">Dispatch legend:</span>';
        if (types.has('AMBIGUOUS_IMPL'))     html += ` <span class="trace-legend-item" style="color:${m.AMBIGUOUS_IMPL.color};border-color:${m.AMBIGUOUS_IMPL.color}">AMBIGUOUS — multiple impls, could not narrow to one</span>`;
        if (types.has('RECURSIVE'))          html += ` <span class="trace-legend-item" style="color:${m.RECURSIVE.color};border-color:${m.RECURSIVE.color}">RECURSIVE — back-edge in call stack</span>`;
        if (types.has('QUALIFIED'))          html += ` <span class="trace-legend-item" style="color:${m.QUALIFIED.color};border-color:${m.QUALIFIED.color}">QUALIFIED — resolved via @Qualifier bean name</span>`;
        if (types.has('HEURISTIC'))          html += ` <span class="trace-legend-item" style="color:${m.HEURISTIC.color};border-color:${m.HEURISTIC.color}">HEURISTIC — resolved via field name match</span>`;
        if (types.has('DYNAMIC_DISPATCH'))   html += ` <span class="trace-legend-item" style="color:${m.DYNAMIC_DISPATCH.color};border-color:${m.DYNAMIC_DISPATCH.color}">DYNAMIC — runtime dispatch, all impls shown</span>`;
        if (types.has('PRIMARY'))            html += ` <span class="trace-legend-item" style="color:${m.PRIMARY.color};border-color:${m.PRIMARY.color}">@PRIMARY — resolved via @Primary bean</span>`;
        if (types.has('INTERFACE_FALLBACK')) html += ` <span class="trace-legend-item" style="color:${m.INTERFACE_FALLBACK.color};border-color:${m.INTERFACE_FALLBACK.color}">IFACE ONLY — no implementation found</span>`;
        if (types.has('LIST_INJECT'))        html += ` <span class="trace-legend-item" style="color:${m.LIST_INJECT.color};border-color:${m.LIST_INJECT.color}">LIST&lt;T&gt; — all beans collected via List injection</span>`;
        if (types.has('DEFAULT_METHOD'))     html += ` <span class="trace-legend-item" style="color:${m.DEFAULT_METHOD.color};border-color:${m.DEFAULT_METHOD.color}">DEFAULT — interface default method (Java 8+)</span>`;
        html += '</div>';
        return html;
    },

    /** Detail block shown inside node card for AMBIGUOUS and RECURSIVE */
    _dispatchDetailBlock(node, esc) {
        const dt = this._effectiveDt(node);
        if (!dt || dt === 'DIRECT') return '';
        const m = this._DT_META[dt] || { color: '#6b7280', bg: 'rgba(107,114,128,0.08)', border: '#d1d5db' };

        if (dt === 'AMBIGUOUS_IMPL') {
            const iface = node.resolvedFrom || '';
            const ifaceSimple = iface.split('.').pop();
            const hint = node.qualifierHint || '';
            const candidates = (node.children || []);
            let html = `<div class="trace-dispatch-detail" style="border-color:${m.border};background:${m.bg}">`;
            html += `<div class="trace-dispatch-detail-title" style="color:${m.color}">⚠ Ambiguous Implementation</div>`;
            if (iface) html += `<div class="trace-dispatch-detail-row"><b>Interface:</b> <span title="${esc(iface)}">${esc(ifaceSimple)}</span> <span class="sum-muted" style="font-size:10px">${esc(iface)}</span></div>`;
            if (hint)  html += `<div class="trace-dispatch-detail-row"><b>Candidates:</b> ${esc(hint)}</div>`;
            if (candidates.length > 0) {
                html += `<div class="trace-dispatch-detail-row"><b>Select implementation to trace:</b></div>`;
                html += '<div class="trace-impl-buttons">';
                candidates.forEach((c, idx) => {
                    const stereo = c.stereotype || '';
                    const nodeCount = this._countTreeNodes(c);
                    html += `<button class="trace-impl-select-btn" style="border-color:${m.border};color:${m.color}" onclick="JA.summary._traceDrillChild(${idx});event.stopPropagation()">`;
                    html += `&#9654; ${esc(c.simpleClassName || c.className || '?')}`;
                    if (stereo) html += ` <span style="font-size:9px;opacity:0.7">${esc(stereo.substring(0,4))}</span>`;
                    if (nodeCount > 1) html += ` <span style="font-size:9px;opacity:0.55">${nodeCount}n</span>`;
                    html += '</button>';
                });
                html += '</div>';
            }
            html += '</div>';
            return html;
        }

        if (dt === 'RECURSIVE' || node.recursive) {
            // Find which ancestor in the trail this cycles to
            let cycleTarget = '';
            if (this._traceState && this._traceState.trail.length > 1) {
                const trail = this._traceState.trail;
                // The current node IS the recursive one — find which ancestor matches
                for (let i = trail.length - 2; i >= 0; i--) {
                    const anc = trail[i];
                    if (anc.className === node.className && anc.methodName === node.methodName) {
                        const crumbs = trail.slice(0, i + 1).map(n => (n.simpleClassName || '?') + '.' + (n.methodName || '?'));
                        cycleTarget = crumbs.join(' → ');
                        break;
                    }
                }
                if (!cycleTarget) {
                    // Back-edge: find by matching class name from parent context
                    const parent = trail[trail.length - 2];
                    if (parent) {
                        // Look through parent children to find this node, then trace back
                        for (let i = 0; i < trail.length - 1; i++) {
                            const anc = trail[i];
                            if (anc.simpleClassName === node.simpleClassName) {
                                cycleTarget = trail.slice(0, i + 1).map(n => (n.simpleClassName || '?') + '.' + (n.methodName || '?')).join(' → ');
                                break;
                            }
                        }
                    }
                }
            }
            let html = `<div class="trace-dispatch-detail" style="border-color:${m.border};background:${m.bg}">`;
            html += `<div class="trace-dispatch-detail-title" style="color:${m.color}">↺ Recursive Call — Back-edge Detected</div>`;
            html += `<div class="trace-dispatch-detail-row">This method calls itself, creating a cycle in the call stack. The analyzer stopped traversal here to prevent infinite recursion.</div>`;
            if (cycleTarget) html += `<div class="trace-dispatch-detail-row"><b>Cycle path:</b> <span style="font-family:monospace;font-size:11px">${esc(cycleTarget)} → (back to start)</span></div>`;
            html += '</div>';
            return html;
        }

        if (dt === 'QUALIFIED') {
            let html = `<div class="trace-dispatch-detail" style="border-color:${m.border};background:${m.bg}">`;
            html += `<div class="trace-dispatch-detail-title" style="color:${m.color}">QUALIFIED Resolution</div>`;
            if (node.qualifierHint) html += `<div class="trace-dispatch-detail-row"><b>@Qualifier:</b> ${esc(node.qualifierHint)}</div>`;
            if (node.resolvedFrom)  html += `<div class="trace-dispatch-detail-row"><b>Interface:</b> ${esc(node.resolvedFrom.split('.').pop())} <span class="sum-muted" style="font-size:10px">${esc(node.resolvedFrom)}</span></div>`;
            html += '</div>';
            return html;
        }

        if (dt === 'HEURISTIC') {
            let html = `<div class="trace-dispatch-detail" style="border-color:${m.border};background:${m.bg}">`;
            html += `<div class="trace-dispatch-detail-title" style="color:${m.color}">HEURISTIC Resolution</div>`;
            if (node.qualifierHint) html += `<div class="trace-dispatch-detail-row"><b>Matched by field name:</b> ${esc(node.qualifierHint)}</div>`;
            if (node.resolvedFrom)  html += `<div class="trace-dispatch-detail-row"><b>Interface:</b> ${esc(node.resolvedFrom.split('.').pop())} <span class="sum-muted" style="font-size:10px">${esc(node.resolvedFrom)}</span></div>`;
            html += '</div>';
            return html;
        }

        if (dt === 'INTERFACE_FALLBACK') {
            let html = `<div class="trace-dispatch-detail" style="border-color:${m.border};background:${m.bg}">`;
            html += `<div class="trace-dispatch-detail-title" style="color:${m.color}">IFACE ONLY — No Implementation Found</div>`;
            html += `<div class="trace-dispatch-detail-row">No concrete implementation was found in the analyzed classes. This interface method could not be resolved to a bean.</div>`;
            if (node.resolvedFrom) html += `<div class="trace-dispatch-detail-row"><b>Interface:</b> ${esc(node.resolvedFrom)}</div>`;
            html += '</div>';
            return html;
        }

        if (dt === 'LIST_INJECT') {
            const iface = node.resolvedFrom || '';
            const ifaceSimple = iface.split('.').pop();
            const hint = node.qualifierHint || '';
            const candidates = (node.children || []);
            let html = `<div class="trace-dispatch-detail" style="border-color:${m.border};background:${m.bg}">`;
            html += `<div class="trace-dispatch-detail-title" style="color:${m.color}">LIST&lt;T&gt; Injection — All Beans Run</div>`;
            if (iface) html += `<div class="trace-dispatch-detail-row"><b>Injected as:</b> List&lt;${esc(ifaceSimple)}&gt; <span class="sum-muted" style="font-size:10px">${esc(iface)}</span></div>`;
            html += `<div class="trace-dispatch-detail-row">All registered implementations are injected via <code>List&lt;${esc(ifaceSimple)}&gt;</code> and will execute at runtime.</div>`;
            if (candidates.length > 0) {
                html += `<div class="trace-dispatch-detail-row"><b>Trace any implementation:</b></div>`;
                html += '<div class="trace-impl-buttons">';
                candidates.forEach((c, idx) => {
                    const stereo = c.stereotype || '';
                    const nodeCount = this._countTreeNodes(c);
                    html += `<button class="trace-impl-select-btn" style="border-color:${m.border};color:${m.color}" onclick="JA.summary._traceDrillChild(${idx});event.stopPropagation()">`;
                    html += `&#9654; ${esc(c.simpleClassName || c.className || '?')}`;
                    if (stereo) html += ` <span style="font-size:9px;opacity:0.7">${esc(stereo.substring(0,4))}</span>`;
                    if (nodeCount > 1) html += ` <span style="font-size:9px;opacity:0.55">${nodeCount}n</span>`;
                    html += '</button>';
                });
                html += '</div>';
            }
            html += '</div>';
            return html;
        }

        if (dt === 'DEFAULT_METHOD') {
            const iface = node.resolvedFrom || '';
            const ifaceSimple = iface.split('.').pop();
            let html = `<div class="trace-dispatch-detail" style="border-color:${m.border};background:${m.bg}">`;
            html += `<div class="trace-dispatch-detail-title" style="color:${m.color}">DEFAULT Method (Java 8+)</div>`;
            html += `<div class="trace-dispatch-detail-row">This is a default method on an interface — no concrete class overrides it. The interface body itself provides the implementation.</div>`;
            if (iface) html += `<div class="trace-dispatch-detail-row"><b>Interface:</b> ${esc(ifaceSimple)} <span class="sum-muted" style="font-size:10px">${esc(iface)}</span></div>`;
            html += '</div>';
            return html;
        }

        return '';
    },

    _renderCallTrace() {
        const s = this._traceState;
        if (!s) return;
        const esc = JA.utils.escapeHtml;
        const r = s.epReport;
        const current = s.trail[s.trail.length - 1];
        const children = current.children || [];
        const depth = s.trail.length - 1;

        const old = document.getElementById('call-trace-overlay');
        if (old) old.remove();

        let html = '<div class="sum-trace-overlay" id="call-trace-overlay" onclick="if(event.target===this)this.remove()">';
        html += '<div class="sum-trace-panel" style="max-width:1100px;max-height:92vh;overflow-y:auto">';

        // Header
        html += '<div class="sum-trace-header">';
        html += `<span class="endpoint-method method-${r.httpMethod}">${esc(r.httpMethod)}</span> `;
        html += `<span style="font-weight:700">${esc(r.fullPath)}</span>`;
        html += `<span class="sum-muted" style="margin-left:8px">Depth: ${depth}</span>`;
        html += '<button class="btn-sm" style="margin-left:auto" onclick="document.getElementById(\'call-trace-overlay\').remove()">Close</button>';
        html += '</div>';

        // Breadcrumb trail — mark recursive nodes in the crumb itself
        html += '<div class="trace-breadcrumb">';
        s.trail.forEach((n, i) => {
            const isCurrent = (i === s.trail.length - 1);
            const label = (n.simpleClassName || '?') + '.' + (n.methodName || '?');
            const recMark = n.recursive ? ' ↺' : '';
            const ambMark = n.dispatchType === 'AMBIGUOUS_IMPL' ? ' ⚠' : '';
            if (isCurrent) {
                html += `<span class="trace-crumb trace-crumb-active">${esc(label)}${recMark}${ambMark}</span>`;
            } else {
                html += `<span class="trace-crumb trace-crumb-link" onclick="JA.summary._traceNavigateTo(JA.summary._traceState.trail[${i}])">${esc(label)}${recMark}${ambMark}</span>`;
            }
            if (!isCurrent) html += '<span class="trace-crumb-sep">&rarr;</span>';
        });
        html += '</div>';

        // Current node detail card
        html += this._buildTraceNodeCard(current, esc, true);

        // Children list
        if (children.length > 0) {
            html += '<div class="trace-children-section">';
            html += `<div class="trace-children-header">Called Methods (${children.length})</div>`;

            // Dispatch legend — shown once above the list
            html += this._dispatchLegend(children, esc);

            html += '<div class="trace-children-list">';
            children.forEach((child, ci) => {
                const childNodes = this._countTreeNodes(child);
                const childColls = this._collectTreeCollections(child);
                const childExternal = child.crossModule ? true : false;
                const stereo = child.stereotype || '';
                let badgeCls = '';
                if (stereo === 'CONTROLLER') badgeCls = 'badge-controller';
                else if (stereo === 'SERVICE') badgeCls = 'badge-service';
                else if (stereo === 'REPOSITORY' || stereo === 'SPRING_DATA') badgeCls = 'badge-repository';
                else if (stereo === 'COMPONENT') badgeCls = 'badge-component';

                const childClassKey = (child.className || child.simpleClassName || '').replace(/'/g, "\\'");
                const childMethodKey = (child.methodName || '').replace(/'/g, "\\'");
                html += `<div class="trace-child-row" onclick="JA.summary._traceDrillChild(${ci})">`;
                html += '<div class="trace-child-main">';
                if (stereo) html += `<span class="badge ${badgeCls}">${esc(stereo.substring(0, 4))}</span> `;
                html += `<span class="trace-child-name">${esc(child.simpleClassName || '?')}.${esc(child.methodName || '?')}</span>`;

                // Full dispatch badge (replaces the old basic RECURSIVE / AMBIGUOUS spans)
                html += this._dispatchBadge(child, esc);

                if (childExternal) html += ' <span class="sum-trace-ext-icon">EXT</span>';
                if (child.module) html += ` <span class="sum-module-tag" style="font-size:10px">${esc(child.module)}</span>`;
                if (child.domain) html += ` <span class="sum-domain-tag" style="font-size:10px">${esc(child.domain)}</span>`;
                html += ` <button class="btn-sm code-view-btn" style="font-size:10px;padding:2px 8px" onclick="JA.summary.showClassCode('${childClassKey}','${childMethodKey}');event.stopPropagation()">Code</button>`;
                html += '</div>';

                html += '<div class="trace-child-counts">';
                html += `<span class="trace-count" title="Total nodes in subtree">${childNodes} nodes</span>`;
                if (childColls.length > 0) html += ` <span class="trace-count" title="Collections in subtree">${childColls.length} colls</span>`;
                if (child.children && child.children.length > 0) html += ` <span class="trace-count" title="Direct children">${child.children.length} calls</span>`;
                if (childColls.length > 0 && childColls.length <= 5) {
                    html += ' ';
                    childColls.forEach(c => { html += `<span class="sum-coll-badge sum-coll-data" style="font-size:9px">${esc(c)}</span> `; });
                }
                html += '</div>';

                // Inline implementation picker for AMBIGUOUS and LIST_INJECT children
                const isAmbi = child.dispatchType === 'AMBIGUOUS_IMPL';
                const isList = child.dispatchType === 'LIST_INJECT';
                if ((isAmbi || isList) && child.children && child.children.length > 0) {
                    const pickerLabel = isList ? 'Trace any implementation:' : 'Pick implementation:';
                    const pickerColor = isList ? '#06b6d4' : '#f97316';
                    html += '<div class="trace-child-impls" onclick="event.stopPropagation()">';
                    html += `<span class="trace-child-impls-label" style="color:${pickerColor}">${pickerLabel}</span>`;
                    child.children.forEach((impl, implIdx) => {
                        const implStereo = impl.stereotype || '';
                        html += `<button class="trace-impl-select-btn trace-impl-compact" style="border-color:${pickerColor};color:${pickerColor}" onclick="JA.summary._traceNavigateToImpl(${ci},${implIdx})">`;
                        html += `&#9654; ${esc(impl.simpleClassName || impl.className || '?')}`;
                        if (implStereo) html += ` <span style="font-size:9px;opacity:0.7">${esc(implStereo.substring(0,4))}</span>`;
                        html += '</button>';
                    });
                    html += '</div>';
                }

                html += '</div>';
            });
            html += '</div></div>';
        } else {
            html += '<div class="trace-children-section"><div class="sum-muted" style="padding:12px">Leaf node — no further calls</div></div>';
        }

        html += '</div></div>';
        document.body.insertAdjacentHTML('beforeend', html);
    },

    _traceDrillChild(childIdx) {
        if (!this._traceState) return;
        const current = this._traceState.trail[this._traceState.trail.length - 1];
        const child = (current.children || [])[childIdx];
        if (child) this._traceNavigateTo(child);
    },

    _traceNavigateToImpl(ambiguousChildIdx, implIdx) {
        if (!this._traceState) return;
        const trail = this._traceState.trail;
        const current = trail[trail.length - 1];
        const ambiguous = (current.children || [])[ambiguousChildIdx];
        if (!ambiguous) return;
        const impl = (ambiguous.children || [])[implIdx];
        if (!impl) return;
        // Dedup: prevent double-push if user clicks the same impl button twice
        if (trail.length >= 2 && trail[trail.length - 2] === ambiguous && trail[trail.length - 1] === impl) return;
        this._traceState.trail.push(ambiguous);
        this._traceState.trail.push(impl);
        this._renderCallTrace();
    },

    _buildTraceNodeCard(node, esc, showSource) {
        let html = '<div class="trace-node-card">';
        const stereo = node.stereotype || '';
        let badge = '';
        if (stereo === 'CONTROLLER') badge = '<span class="badge badge-controller">CONTROLLER</span>';
        else if (stereo === 'SERVICE') badge = '<span class="badge badge-service">SERVICE</span>';
        else if (stereo === 'REPOSITORY' || stereo === 'SPRING_DATA') badge = '<span class="badge badge-repository">REPOSITORY</span>';
        else if (stereo === 'COMPONENT') badge = '<span class="badge badge-component">COMPONENT</span>';
        else if (stereo) badge = `<span class="badge">${esc(stereo)}</span>`;

        const cardClassKey = (node.className || node.simpleClassName || '').replace(/'/g, "\\'");
        const cardMethodKey = (node.methodName || '').replace(/'/g, "\\'");
        html += '<div class="trace-node-header">' + badge;
        html += ` <span class="trace-node-title">${esc(node.simpleClassName || '?')}.${esc(node.methodName || '?')}</span>`;
        html += `<span class="sum-muted" style="margin-left:8px;font-size:11px">${esc(node.className || '')}</span>`;

        // Full dispatch badge for current node header
        html += this._dispatchBadge(node, esc);

        if (node.crossModule) html += ' <span class="sum-trace-ext-icon">EXTERNAL</span>';
        if (node.module) html += ` <span class="sum-module-tag">${esc(node.module)}</span>`;
        if (node.domain) html += ` <span class="sum-domain-tag">${esc(node.domain)}</span>`;
        html += ` <button class="btn-sm code-view-btn" style="margin-left:8px" onclick="JA.summary.showClassCode('${cardClassKey}','${cardMethodKey}');event.stopPropagation()">Code</button>`;
        html += '</div>';

        // Dispatch detail block — expanded info for AMBIGUOUS, RECURSIVE, QUALIFIED, HEURISTIC, IFACE_ONLY
        html += this._dispatchDetailBlock(node, esc);

        // Stats row
        const totalNodes = this._countTreeNodes(node);
        const allColls = this._collectTreeCollections(node);
        const extCalls = this._countExternalCalls(node);
        html += '<div class="trace-stats-row">';
        html += `<span class="trace-stat">${totalNodes} nodes in scope</span>`;
        html += `<span class="trace-stat">${allColls.length} collections</span>`;
        html += `<span class="trace-stat">${(node.children || []).length} direct calls</span>`;
        html += `<span class="trace-stat">${extCalls} external calls</span>`;
        if (node.returnType) html += `<span class="trace-stat">Returns: ${esc(node.returnType)}</span>`;
        if (node.operationType) html += ` <span class="sum-op-badge sum-op-${node.operationType.toLowerCase()}">${esc(node.operationType)}</span>`;
        html += '</div>';

        // Collections at this level
        if (node.collectionsAccessed && node.collectionsAccessed.length) {
            html += '<div class="trace-section"><b>Collections at this level:</b> ';
            for (const c of node.collectionsAccessed) {
                const dom = (node.collectionDomains && node.collectionDomains[c]) || '';
                const op = node.operationType || '';
                html += `<span class="sum-coll-badge sum-coll-data" title="${esc(dom)}">${esc(c)}</span>`;
                if (op) html += `<span class="sum-op-badge sum-op-${op.toLowerCase()}" style="font-size:9px">${esc(op)}</span> `;
                else html += ' ';
            }
            html += '</div>';
        }

        // All collections in scope
        if (allColls.length > 0) {
            html += '<div class="trace-section"><b>All collections in scope (' + allColls.length + '):</b> ';
            allColls.forEach(c => { html += `<span class="sum-coll-badge sum-coll-data" style="font-size:10px">${esc(c)}</span> `; });
            html += '</div>';
        }

        // Annotations
        if (node.annotationDetails && node.annotationDetails.length) {
            html += '<div class="trace-section"><b>Annotations:</b> ';
            for (const ad of node.annotationDetails) {
                const attrs = ad.attributes ? Object.entries(ad.attributes).map(([k,v]) => k + '=' + JSON.stringify(v)).join(', ') : '';
                html += `<span class="sum-trace-ann" title="${esc(attrs)}">@${esc(ad.name)}</span> `;
            }
            html += '</div>';
        }

        // SQL Statements
        if (node.sqlStatements && node.sqlStatements.length) {
            html += '<div class="node-source-block">';
            html += '<div class="node-source-label">SQL / JPQL Statements</div>';
            for (const sql of node.sqlStatements) {
                html += '<pre class="node-source-code node-sql-block">' + esc(sql) + '</pre>';
            }
            html += '</div>';
        }

        // Source Code
        if (showSource && node.sourceCode) {
            html += '<div class="node-source-block">';
            html += '<div class="node-source-label">Decompiled Source (CFR)</div>';
            html += '<pre class="node-source-code">' + esc(node.sourceCode) + '</pre>';
            html += '</div>';
        } else if (showSource) {
            html += '<div class="node-source-block"><div class="node-source-label sum-muted">No decompiled source for this node</div></div>';
        }

        html += '</div>';
        return html;
    }
});
