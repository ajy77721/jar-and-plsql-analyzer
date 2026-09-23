/**
 * PA.callTrace — Depth-indented HTML call trace (jar-analyzer style).
 * Renders call tree as nested rows with expand/collapse, breadcrumb, schema badges.
 */
window.PA = window.PA || {};

const SCHEMA_COLORS = {
    'CUSTOMER':   { bg: 'var(--badge-blue-bg)', fg: 'var(--badge-blue)' },
    'CUSTOMER_I': { bg: 'var(--badge-teal-bg)', fg: 'var(--badge-teal)' },
    'OPUS_CORE':  { bg: 'var(--badge-green-bg)', fg: 'var(--badge-green)' },
    'POLICY':     { bg: 'var(--accent-subtle)', fg: 'var(--accent)' },
    'CLAIMS':     { bg: 'var(--badge-red-bg)', fg: 'var(--badge-red)' },
    'ACCOUNTING': { bg: 'var(--badge-orange-bg)', fg: 'var(--badge-orange)' },
    'DEFAULT':    { bg: 'var(--bg)', fg: 'var(--text-secondary)' }
};

function getSchemaColor(schema) {
    if (!schema) return SCHEMA_COLORS.DEFAULT;
    return SCHEMA_COLORS[schema.toUpperCase()] || SCHEMA_COLORS.DEFAULT;
}

PA.getSchemaColor = getSchemaColor;

PA.callTrace = {
    treeData: null,
    breadcrumbStack: [],  // [{id, name}]
    stepCounter: 0,

    // ── Re-root state ──
    _gen: 0,                 // stale-request guard: every async re-root captures & re-checks this
    _originalRootId: null,   // the analysis's original entry point — never changes within an analysis
    _originalAnalysis: null, // which analysis _originalRootId belongs to (reset on analysis switch)

    /** Normalized node-id equality: uppercase + unify $/. separators + schema-prefix/endsWith fallback. */
    _sameNode(a, b) {
        if (!a || !b) return false;
        const na = String(a).toUpperCase().replace(/\$/g, '.');
        const nb = String(b).toUpperCase().replace(/\$/g, '.');
        if (na === nb) return true;
        const sa = na.indexOf('.') >= 0 ? na.substring(na.indexOf('.') + 1) : na;
        const sb = nb.indexOf('.') >= 0 ? nb.substring(nb.indexOf('.') + 1) : nb;
        return sa === sb || na.endsWith('.' + nb) || nb.endsWith('.' + na);
    },

    /** Load call tree for a procedure and render it (also establishes the analysis's original root).
     *
     *  UI-019 (§142): fetches `_fetchDepth()` levels, not the whole tree — deeper nodes are fetched
     *  on demand as the user expands them (`toggle()`), or all at once if they run "Expand All" or
     *  search (`_ensureFullyLoaded()`). */
    async load(procId) {
        const myGen = ++PA.callTrace._gen;
        const data = await PA.api.getCallTree(procId, PA.callTrace._fetchDepth());
        if (myGen !== PA.callTrace._gen) return;   // superseded by a newer re-root
        if (!data || !data.id) {
            document.getElementById('ctContainer').innerHTML = '<div class="empty-msg">No call tree data for: ' + PA.esc(procId) + '</div>';
            return;
        }
        // First load of a given analysis = its original root (survives re-rooting; reset on analysis switch).
        const analysis = (PA.api && PA.api._analysisName) || '';
        if (PA.callTrace._originalAnalysis !== analysis) {
            PA.callTrace._originalAnalysis = analysis;
            PA.callTrace._originalRootId = data.id;
        }
        PA.callTrace._fullyLoaded = false;
        PA.callTrace.treeData = data;
        PA.callTrace.breadcrumbStack = [{ id: data.id, name: data.name || data.id }];
        await PA.callTrace.render(data);
    },

    /** Hard re-root ("Make Root"): treat this node as the new tree root, resetting the breadcrumb. */
    async makeRoot(procId) {
        const myGen = ++PA.callTrace._gen;
        const data = await PA.api.getCallTree(procId, PA.callTrace._fetchDepth());
        if (myGen !== PA.callTrace._gen) return;   // superseded
        if (!data || !data.id) return;
        PA.callTrace._fullyLoaded = false;
        PA.callTrace.treeData = data;
        PA.callTrace.breadcrumbStack = [{ id: data.id, name: data.name || data.id }];
        await PA.callTrace.render(data);
    },

    /** The analysis's original root node id — the real $-form id captured from the first render
     *  (NOT analysisData.entryPoint, which is the bare unqualified name and won't resolve a subtree). */
    _analysisEntry() {
        return PA.callTrace._originalRootId || (PA.analysisData && PA.analysisData.entryPoint) || null;
    },

    /** Whether the currently-shown node is the analysis's original entry point. */
    _atAnalysisRoot() {
        var entry = PA.callTrace._analysisEntry();
        var cur = (PA.context && PA.context.procId) || (PA.callTrace.treeData && PA.callTrace.treeData.id);
        return !entry || PA.callTrace._sameNode(cur, entry);
    },

    /** Show/hide the toolbar "Analysis Root" revert button based on current position. */
    _updateRootBtn() {
        var btn = document.getElementById('btnAnalysisRoot');
        if (btn) btn.style.display = PA.callTrace._atAnalysisRoot() ? 'none' : 'inline-flex';
    },

    /** Return to the analysis's original entry point via the canonical show-as-current fn. */
    goAnalysisRoot() {
        var entry = PA.callTrace._analysisEntry();
        if (entry) PA.showProcedure(entry);
    },

    /** Drill down into a child node (soft re-root: remembers the path in the breadcrumb). */
    async drillDown(procId) {
        const myGen = ++PA.callTrace._gen;
        const data = await PA.api.getCallTree(procId, PA.callTrace._fetchDepth());
        if (myGen !== PA.callTrace._gen) return;   // superseded
        if (!data || !data.id) return;
        PA.callTrace._fullyLoaded = false;
        PA.callTrace.treeData = data;
        PA.callTrace.breadcrumbStack.push({ id: data.id, name: data.name || data.id });
        await PA.callTrace.render(data);
    },

    /** Go to a specific breadcrumb position */
    async goTo(index) {
        if (index < 0 || index >= PA.callTrace.breadcrumbStack.length) return;
        const item = PA.callTrace.breadcrumbStack[index];
        if (!item) return;
        const myGen = ++PA.callTrace._gen;
        const data = await PA.api.getCallTree(item.id, PA.callTrace._fetchDepth());
        if (myGen !== PA.callTrace._gen) return;   // superseded
        if (data) {
            PA.callTrace._fullyLoaded = false;
            PA.callTrace.breadcrumbStack = PA.callTrace.breadcrumbStack.slice(0, index + 1);
            PA.callTrace.treeData = data;
            await PA.callTrace.render(data);
        }
    },

    /** Go back to root (first in breadcrumb) */
    goRoot() {
        if (PA.callTrace.breadcrumbStack.length > 0) {
            PA.callTrace.goTo(0);
        }
    },

    /** "Make Root" control handler — route through the ONE canonical show-as-current function
     *  so it behaves exactly like selecting the proc (tree + detail + tabs + breadcrumb + URL,
     *  with the built-in stale-request guard). */
    onMakeRoot(procId, event) {
        if (event) event.stopPropagation();
        PA.showProcedure(procId);
    },

    _nodeMap: {},
    /** The tree as delivered, so expandAll can count what has not been rendered. */
    _root: null,
    _initialDepth: 1,

    /**
     * Hierarchy can collapse repeated calls to the same callee into ONE row — opt-in, via the
     * Unique toggle, not the default.
     *
     * <p>Real data drives the feature's existence: ERP_USP_RUN_CYCLE calls
     * ERP_USP_WRITE_AUDIT_TRAIL from dozens of distinct lines, and the uncollapsed tree spends
     * dozens of rows saying the same thing. Turning Unique on answers "what is the SHAPE of the
     * call structure" with one row per distinct callee and a &times;N badge; the call-site lines
     * are not dropped, they move onto the badge (tooltip) and an expandable chip list under the
     * row. But by default every individual call occurrence gets its own top-level row with its
     * own line number — that is what users actually want to see first; grouping is there for
     * whoever explicitly asks for the summarized view.
     *
     * <p>Trace (Flat mode) makes the identical default choice — see PA.trace._unique in app.js.
     */
    _unique: false,

    /** Grouping identity for a child row: the node id when there is one, else the qualified name. */
    _groupKey(n) {
        if (n.id) return String(n.id).toUpperCase();
        return [n.schemaName || '', n.packageName || '', n.name || ''].join('.').toUpperCase();
    },

    /**
     * The children this view should draw for `node` — the raw list in all-call-sites mode, or one
     * representative per distinct callee in unique mode.
     *
     * <p>Memoised on the node (`_groupedChildren`), because the representatives are *new objects*
     * and `renderNode` hands out a sticky `_step` per object: rebuilding them on every re-render
     * (lazy child render, search expansion) would hand the same visual row a different step each
     * time and break the toggle/`_nodeMap` lookups its DOM ids were built with.
     */
    _childrenFor(node) {
        const kids = (node && node.children) || [];
        if (!PA.callTrace._unique || kids.length < 2) return kids;
        if (node._groupedChildren && node._groupedSrc === kids) return node._groupedChildren;
        const order = [], byKey = {};
        for (const child of kids) {
            const key = PA.callTrace._groupKey(child);
            if (!byKey[key]) { byKey[key] = { members: [] }; order.push(key); }
            byKey[key].members.push(child);
        }
        const out = order.map(key => {
            const members = byKey[key].members;
            if (members.length === 1) return members[0];
            // Representative: a real, expandable occurrence — prefer non-circular, then the one
            // carrying the most children, so collapsing never hides a subtree that exists.
            let rep = members[0];
            for (const m of members) {
                const better = (rep.circular && !m.circular)
                    || (rep.circular === m.circular
                        && ((m.children || []).length > (rep.children || []).length
                            || (!!m.moreChildren && !rep.moreChildren)));
                if (better) rep = m;
            }
            const lines = members.map(m => m.callLineNumber || 0).filter(l => l > 0)
                                 .sort((a, b) => a - b);
            return Object.assign({}, rep, { _dupCount: members.length, _callLines: lines });
        });
        node._groupedSrc = kids;
        node._groupedChildren = out;
        return out;
    },

    /** Drop every memoised grouping + step, so the next render rebuilds rows from scratch. */
    _resetGrouping(root) {
        const stack = [root];
        while (stack.length) {
            const n = stack.pop();
            if (!n) continue;
            delete n._step;
            if (n._groupedChildren) {
                for (const g of n._groupedChildren) delete g._step;
                delete n._groupedChildren; delete n._groupedSrc;
            }
            if (n.children && !n.circular) for (const c of n.children) stack.push(c);
        }
    },

    /** Toolbar toggle: collapsed (one row per callee) vs every individual call site. */
    async toggleUnique() {
        PA.callTrace._unique = !PA.callTrace._unique;
        const btn = document.getElementById('ctUniqueBtn');
        if (btn) btn.classList.toggle('active', PA.callTrace._unique);
        if (!PA.callTrace.treeData) return;
        PA.callTrace._resetGrouping(PA.callTrace.treeData);
        await PA.callTrace.render(PA.callTrace.treeData);
    },

    /** Show/hide the call-site line chips under a collapsed row. */
    toggleCallSites(step, event) {
        if (event) event.stopPropagation();
        const el = document.getElementById('ct-callsites-' + step);
        if (el) el.style.display = (el.style.display === 'none') ? '' : 'none';
    },

    /**
     * UI-019 (§142) — true once `_root` genuinely holds every descendant, not just what's been
     * fetched so far. `expandAll()`/search both need this before they can trust `_root` for a
     * count or a full-tree scan; ordinary browsing never needs it at all, since `toggle()` fetches
     * one node's own next slice on demand instead.
     */
    _fullyLoaded: false,

    /** How many levels one navigation fetch asks for — enough to match what render() already
     *  shows expanded (`_initialDepth`) plus one, so the visible rows never wait on a second
     *  request, only what's still collapsed does. */
    _fetchDepth() { return PA.callTrace._initialDepth + 1; },

    /**
     * Fetches a node's own next slice and splices it in, in place — used by `toggle()` when a
     * container is `data-loaded="0"` for a node that was never fetched at all (`moreChildren`),
     * as opposed to one that's simply collapsed in the DOM (`node.children` already populated,
     * no fetch needed — that path is unchanged, see `_renderChildrenLazy`).
     */
    async _fetchNextSlice(node) {
        const sub = await PA.api.getCallTree(node.id, PA.callTrace._fetchDepth());
        if (!sub || !Array.isArray(sub.children)) return;
        node.children = sub.children;
        if (sub.moreChildren) { node.moreChildren = true; node.childCount = sub.childCount; }
        else { delete node.moreChildren; delete node.childCount; }
    },

    /** True if any node in the tree was cut by a depth fetch and never followed up on — the real
     *  invariant `_ensureFullyLoaded` cares about, checked directly rather than trusted from a
     *  flag: a tree built some other way (a synthetic fixture, a future caller) that happens to
     *  have no `moreChildren` anywhere is just as "fully loaded" as one this file fetched in full,
     *  and asking it to fetch again would be wrong, not merely redundant. */
    _hasUnfetchedChildren(root) {
        if (!root) return false;
        const stack = [root];
        while (stack.length) {
            const node = stack.pop();
            if (node.moreChildren) return true;
            if (node.children && !node.circular) {
                for (const child of node.children) stack.push(child);
            }
        }
        return false;
    },

    /**
     * Loads every remaining node before `expandAll()`/search run — both need `_root` to be the
     * real, complete tree (an accurate count; a search that can reach every node), not whatever
     * partial slice ordinary lazy browsing has fetched so far. A full re-render, same as any other
     * `render()` call — the DOM itself stays exactly as lazy as before this fix; only the *data*
     * backing it stops being partial.
     */
    async _ensureFullyLoaded() {
        if (PA.callTrace._fullyLoaded || !PA.callTrace._root) return;
        if (!PA.callTrace._hasUnfetchedChildren(PA.callTrace._root)) {
            // Already complete, just never fetched by this file — nothing to ask the server for.
            PA.callTrace._fullyLoaded = true;
            return;
        }
        const rootId = PA.callTrace._root.id;
        const full = await PA.api.getCallTree(rootId);   // no maxDepth: the whole tree, as before
        if (!full || !full.id) return;
        PA.callTrace._fullyLoaded = true;
        PA.callTrace.treeData = full;
        await PA.callTrace.render(full);
    },

    /** Levels of indentation before it stops growing — see renderNode. */
    _MAX_INDENT_LEVELS: 12,

    /** Rows above which "Expand All" asks first. Sized against the 1,250-node trace measured here. */
    _EXPAND_ALL_WARN: 400,

    _indexDataCache: null,

    _getNodeMetrics: function(nodeId) {
        if (!PA.nodeIndex.count()) return null;
        if (!PA.callTrace._indexDataCache) {
            var cache = {};
            var nodes = PA.nodeIndex.all();
            for (var i = 0; i < nodes.length; i++) {
                var n = nodes[i];
                cache[(n.nodeId || '').toUpperCase()] = n;
                if (n.name) cache[n.name.toUpperCase()] = n;
            }
            PA.callTrace._indexDataCache = cache;
        }
        var key = (nodeId || '').toUpperCase();
        return PA.callTrace._indexDataCache[key] || null;
    },

    _calcRisk: function(n) {
        if (!n || !PA.complexity || !PA.complexity.scoreRaw) return null;
        var c = n.counts || {};
        var stmts = c.statements || {};
        var dynSql = (stmts.EXECUTE_IMMEDIATE || 0) + (stmts.DBMS_SQL || 0);
        // Delegate to the ONE shared scorer so the badge always matches the complexity table,
        // including for non-linear formulas (log/exponential/balanced).
        var sr = PA.complexity.scoreRaw({
            loc: n.linesOfCode || 0, tables: c.tables || 0, callsOut: c.callsOut || 0,
            cursors: c.cursors || 0, dynSql: dynSql, depth: n.depth || 0
        });
        var cls = sr.risk === 'HIGH' ? 'high' : (sr.risk === 'MEDIUM' ? 'medium' : 'low');
        var label = sr.risk.charAt(0); // H / M / L
        return { label: label, cls: cls, score: sr.score };
    },

    /**
     * Render the call tree as depth-indented HTML.
     *
     * <p>UI-024 (§164): `_getNodeMetrics` (called from the fully-synchronous `renderNode`
     * recursion below) reads `PA.nodeIndex` for every node the walk visits, but every node id it
     * will ever ask for is already inside `data` — the whole tree is fetched before `render` is
     * ever called (every one of its 6 callers already has the complete `treeData`/`full` in hand).
     * So, same as `_collectTreeNodeIds` (§162) and `sourceView.js`'s helpers (§163): one
     * `await PA.nodeIndex.ensureLoaded()` here, at the single common entry point every caller
     * already funnels through, is enough — `renderNode`/`_getNodeMetrics` themselves need no
     * internal async boundary at all.
     */
    async render(data) {
        await PA.nodeIndex.ensureLoaded();
        // Capture the analysis's original root id (the real $-form node id from the first render)
        // so "Analysis Root" can re-root back to it. Reset when the analysis changes.
        var analysis = (PA.api && PA.api._analysisName) || '';
        if (data && data.id && PA.callTrace._originalAnalysis !== analysis) {
            PA.callTrace._originalAnalysis = analysis;
            PA.callTrace._originalRootId = data.id;
        }
        PA.callTrace.updateBreadcrumb();
        const uniqBtn = document.getElementById('ctUniqueBtn');
        if (uniqBtn) uniqBtn.classList.toggle('active', PA.callTrace._unique);
        PA.callTrace.stepCounter = 0;
        PA.callTrace._nodeMap = {};
        PA.callTrace._indexDataCache = null;
        const searchBox = document.getElementById('exploreSearch');
        if (searchBox) searchBox.value = '';
        const container = document.getElementById('ctContainer');

        if (!data || !data.id) {
            container.innerHTML = '<div class="empty-msg">Select a procedure from the left panel</div>';
            return;
        }

        // UI-016. The pre-walk that used to be here assigned every node a step number and put it
        // in _nodeMap before a single row was drawn — a full-tree traversal on every trace load,
        // to support a click handler that can only ever fire on a row that was rendered. Nodes are
        // now indexed as they are rendered, in renderNode.
        //
        // The root is kept so expandAll can still count the whole tree. That walk did not
        // disappear; it moved to the moment the user asks to render everything, which is the only
        // moment it is justified. Measured: 1,250 nodes on ERP$ERP_USP_RUN_CYCLE, and one measured
        // JAR endpoint expands to 300,167,238 — a size where "before first paint" is a hung tab.
        PA.callTrace._root = data;
        let html = '';
        html += PA.callTrace.renderNode(data, 0, null);
        container.innerHTML = html;

        // Hierarchy/Trace merge: Flat mode (PA.trace, app.js) draws from this SAME `data` object.
        // Every caller of render() — load(), makeRoot(), drillDown(), goTo(), _ensureFullyLoaded() —
        // re-fetches treeData and lands here, so refreshing Flat's DOM from this one spot keeps it
        // current with whatever root/subtree Tree mode is showing, even while Flat is hidden
        // (display:none costs nothing correctness-wise, only some unseen render work).
        if (PA.trace && PA.trace.renderFromTree) PA.trace.renderFromTree(data);
    },

    // _indexNodes(node, parentSourceFile) was here — UI-016. It also set _parentSourceFile on
    // every node, which nothing in this file ever read: the caller that renders an @line link
    // (parser/js/app.js) builds its own flatNodes with its own _parentSourceFile. So the walk
    // maintained one field for a click handler and one field for nobody.

    /** Render a single node — children rendered only if within initial depth */
    renderNode(node, depth, parentSourceFile) {
        // Assigned here rather than in a pre-walk, and only once: a node re-rendered by
        // _renderChildrenLazy must keep the step its DOM ids were built with, or toggling a
        // subtree twice would look up a different node.
        if (node._step === undefined) {
            node._step = ++PA.callTrace.stepCounter;
        }
        // Registered on every render, not only on first sight: render() clears _nodeMap, and a node
        // that kept a sticky _step from an earlier render would otherwise never re-enter the map,
        // leaving its expand arrow pointing at nothing.
        PA.callTrace._nodeMap[node._step] = node;
        const step = node._step;
        // Indentation is capped. Real traces here reach depth 71 (measured: ERP$ERP_USP_RUN_CYCLE,
        // 1,250 nodes), and at 20px a level that is 1,420px of blank space before the name — the
        // label sits far outside a panel that is a few hundred pixels wide, so the rows at the
        // bottom of a deep trace were unreadable rather than merely cramped. Past the cap the
        // structure is carried by the depth badge instead of by more whitespace.
        const indent = Math.min(depth, PA.callTrace._MAX_INDENT_LEVELS) * 20;
        // No extra marker is needed past the cap: every row already carries an L<depth> badge
        // below, so flattening the indent loses the visual nesting but not the depth itself.
        // UI-019 (§142): `moreChildren` means real children exist but weren't fetched — the toggle
        // arrow must still show, even though `node.children` is empty until `toggle()` fetches them.
        const shownChildren = PA.callTrace._childrenFor(node);
        const hasChildren = shownChildren.length > 0 || !!node.moreChildren;
        const id = PA.escAttr(node.id || '');
        const name = node.name || node.id || '?';
        const schema = node.schemaName || '';
        const pkg = node.packageName || '';
        const fullName = [schema, pkg, name].filter(Boolean).join('.');
        const callType = node.callType || '';
        const startLine = node.startLine || 0;
        const endLine = node.endLine || 0;
        const callLine = node.callLineNumber || 0;
        const sourceFile = node.sourceFile || (schema && pkg ? schema + '.' + pkg : '');
        const isCircular = node.circular;
        const colorObj = getSchemaColor(schema);
        const childCount = !hasChildren || isCircular ? 0
            : (shownChildren.length > 0 ? shownChildren.length : (node.childCount || 0));

        let html = '';

        html += `<div class="ct-row" data-id="${id}" data-depth="${depth}" id="ct-${step}" title="Click: show details | Double-click: drill down into this procedure" onclick="PA.callTrace.onRowClick('${PA.escJs(node.id)}', event)" ondblclick="PA.callTrace.onRowDblClick('${PA.escJs(node.id)}', event)">`;
        html += `<div class="ct-indent" style="width:${indent}px"></div>`;

        if (hasChildren && !isCircular) {
            const collapsed = depth >= PA.callTrace._initialDepth;
            html += `<span class="ct-toggle" data-step="${step}" onclick="PA.callTrace.toggle(${step}, ${depth}, event)">${collapsed ? '&#9654;' : '&#9660;'}</span>`;
        } else {
            html += `<span class="ct-toggle" style="visibility:hidden">&#9660;</span>`;
        }

        html += `<span class="ct-depth-badge" data-tip="Call depth ${depth}">L${depth}</span>`;
        html += `<span class="ct-step" style="color:${colorObj.fg}">${step}</span>`;

        if (schema) {
            html += `<span class="ct-schema-badge" style="background:${colorObj.bg};color:${colorObj.fg}" data-tip="Owner schema">${PA.esc(schema)}</span>`;
        }

        if (node.readable === false) {
            html += `<span class="ct-lock" data-tip="Encrypted/wrapped source">&#128274;</span>`;
        }

        html += `<span class="ct-name" data-tip="Click to view source" onclick="PA.callTrace.onNameClick('${PA.escJs(node.id)}', event)">${PA.esc(fullName)}</span>`;

        // "Make Root" — re-root the tree at this node. Only on navigable, non-circular child rows
        // (circular refs and bare variable/cursor references have no independent subtree to show).
        if (!isCircular && depth > 0 && (schema || startLine > 0)) {
            html += `<span class="ct-makeroot" data-tip="Make this node the tree root" onclick="PA.callTrace.onMakeRoot('${PA.escJs(node.id)}', event)">&#9654; Make Root</span>`;
        }

        if (startLine > 0) {
            const bodyLabel = (endLine > 0 && endLine !== startLine) ? `L${startLine}-${endLine}` : `L${startLine}`;
            html += `<span class="ct-line" data-tip="Open source at line ${startLine}" onclick="PA.callTrace.onLineClick('${PA.escJs(sourceFile)}', ${startLine}, event)">${bodyLabel}</span>`;
        }
        // Collapsed row: one callee, N call sites. The count is clickable and carries every line
        // number in its tooltip, so nothing the uncollapsed view showed is lost here.
        const dupLines = node._callLines || [];
        if (node._dupCount > 1) {
            html += `<span class="trace-count-badge ct-dup-badge" title="Called ${node._dupCount} times from `
                  + `${PA.esc(parentSourceFile || 'the caller')}${dupLines.length ? ' — lines ' + dupLines.join(', ') : ''}`
                  + `\nClick to list the call sites." onclick="PA.callTrace.toggleCallSites(${step}, event)">&times;${node._dupCount}</span>`;
        }
        if (!(node._dupCount > 1) && callLine > 0 && parentSourceFile) {
            html += `<span class="ct-call-line" data-tip="Called at line ${callLine}" onclick="PA.callTrace.onLineClick('${PA.escJs(parentSourceFile)}', ${callLine}, event)">@${callLine}</span>`;
        }

        const callTypeTooltips = {
            'INTERNAL': 'Same module/schema — call stays within the analyzed unit',
            'EXTERNAL': 'Cross-module call — goes to a different package/module or schema',
            'TRIGGER': 'Database trigger — fires automatically on table DML',
            'DYNAMIC': 'Dynamic SQL — built at runtime via EXECUTE IMMEDIATE'
        };
        // The server cuts a call tree that would be exponential to expand, and says so on the node.
        // Without this the tree simply appears shorter than it is — the exact silent-truncation
        // defect the server-side flag was added to prevent. Surfaced like the CIRCULAR badge beside
        // it, which is the convention a reader of this screen already knows.
        if (node.childrenTruncated) {
            const _omitted = node.omittedChildCount || 0;
            html += `<span class="ct-call-badge TRUNCATED" title="This branch was cut: ${_omitted} more `
                  + `call${_omitted === 1 ? '' : 's'} not expanded, because fully expanding this tree `
                  + `would be prohibitively large. The calls exist; they are not shown here.">`
                  + `+${_omitted} not shown</span>`;
        }
        if (isCircular) {
            html += `<span class="ct-call-badge CIRCULAR" title="Circular reference — this procedure is already in the call chain above">CIRCULAR</span>`;
        } else if (callType) {
            html += `<span class="ct-call-badge ${PA.escAttr(callType)}" title="${callTypeTooltips[callType] || callType}">${PA.esc(callType)}</span>`;
        }

        const metrics = PA.callTrace._getNodeMetrics(node.id);
        const nodeLoc = metrics ? (metrics.linesOfCode || 0) : (endLine > startLine ? endLine - startLine + 1 : 0);
        if (nodeLoc > 0) {
            html += `<span class="ct-loc-badge" data-tip="Lines of code">${nodeLoc.toLocaleString()}</span>`;
        }
        const risk = PA.callTrace._calcRisk(metrics);
        if (risk) {
            html += `<span class="ct-cx-badge ${risk.cls}" data-tip="Complexity: ${risk.score} pts">${risk.label}</span>`;
        }

        if (childCount > 0) {
            html += `<span style="font-size:9px;color:var(--text-muted);margin-left:4px">(${childCount})</span>`;
        }

        html += `</div>`;

        // The individual call sites the &times;N badge stands for, one clickable chip each — the
        // line-number detail collapsing would otherwise cost, kept a click away instead of dropped.
        if (dupLines.length > 1) {
            html += `<div class="ct-callsites" id="ct-callsites-${step}" style="display:none;margin-left:${indent + 40}px">`;
            html += `<span class="ct-callsites-label">${dupLines.length} call sites:</span>`;
            for (const l of dupLines) {
                html += `<span class="ct-call-line" data-tip="Called at line ${l}" onclick="PA.callTrace.onLineClick('${PA.escJs(parentSourceFile || '')}', ${l}, event)">@${l}</span>`;
            }
            html += `</div>`;
        }

        if (hasChildren && !isCircular) {
            const renderNow = depth < PA.callTrace._initialDepth;
            if (renderNow) {
                html += `<div class="ct-children" id="ct-children-${step}" data-loaded="1">`;
                for (const child of shownChildren) {
                    html += PA.callTrace.renderNode(child, depth + 1, sourceFile);
                }
                html += `</div>`;
            } else {
                html += `<div class="ct-children" id="ct-children-${step}" data-loaded="0" style="display:none"></div>`;
            }
        }

        return html;
    },

    /**
     * Nodes in the whole tree, including ones never rendered.
     *
     * <p>Iterative rather than recursive: the trace reaches depth 71 today, and a malformed cycle
     * would blow the stack rather than report anything. `circular` nodes are counted but not
     * followed, which is the same rule the renderer uses.
     */
    _countNodes(root) {
        if (!root) return 0;
        let n = 0;
        const stack = [root];
        while (stack.length) {
            const node = stack.pop();
            n++;
            // Counted the way the view draws them, so the "this trace has N nodes" warning matches
            // the number of rows Expand All is about to build (collapsed mode draws fewer).
            if (!node.circular) {
                for (const child of PA.callTrace._childrenFor(node)) stack.push(child);
            }
        }
        return n;
    },

    _renderChildrenLazy(step, depth) {
        const node = PA.callTrace._nodeMap[step];
        if (!node || !node.children) return;
        const container = document.getElementById('ct-children-' + step);
        if (!container || container.dataset.loaded === '1') return;
        const sf = node.sourceFile || (node.schemaName && node.packageName ? node.schemaName + '.' + node.packageName : '');
        let html = '';
        for (const child of PA.callTrace._childrenFor(node)) {
            html += PA.callTrace.renderNode(child, depth + 1, sf);
        }
        container.innerHTML = html;
        container.dataset.loaded = '1';
    },

    /**
     * Toggle expand/collapse.
     *
     * UI-019 (§142): a container can be `data-loaded="0"` for two different reasons now — either
     * the node's children are already in memory and just haven't been rendered yet (pure DOM
     * laziness, unchanged since UI-016), or the node itself was fetched with `moreChildren` and
     * has no children in memory at all yet (network laziness, new). `_fetchNextSlice` handles only
     * the second case; the first still renders instantly, with no `await` in its way.
     */
    async toggle(step, depth, event) {
        event.stopPropagation();
        const container = document.getElementById('ct-children-' + step);
        const icon = document.querySelector(`.ct-toggle[data-step="${step}"]`);
        if (!container) return;
        if (container.style.display === 'none') {
            if (container.dataset.loaded === '0') {
                const node = PA.callTrace._nodeMap[step];
                if (node && node.moreChildren && (!node.children || node.children.length === 0)) {
                    await PA.callTrace._fetchNextSlice(node);
                }
                PA.callTrace._renderChildrenLazy(step, depth);
            }
            container.style.display = '';
            if (icon) icon.innerHTML = '&#9660;';
        } else {
            container.style.display = 'none';
            if (icon) icon.innerHTML = '&#9654;';
        }
    },

    /**
     * Expands every level, not just the ones already in the document.
     *
     * <p>The previous version took a single `querySelectorAll` snapshot and expanded what was in it.
     * Expanding a node inserts *its* children, which carry their own unexpanded containers — and
     * those were not in the snapshot, so they stayed collapsed. On a deep trace one click expanded
     * one more level and the button looked broken. Repeating until a pass finds nothing left is the
     * fix; the tree is finite, and the iteration cap is only a guard against a malformed cycle.
     *
     * <p>It also asks first on a large tree. Fully expanding the 1,250-node trace measured here
     * builds every row at once, and a user reaching for "Expand All" to find one procedure would
     * rather have been warned than have the tab freeze.
     *
     * <p>UI-019 (§142): {@code _root} may now be a partial fetch (ordinary browsing is lazy by
     * default), so this awaits {@code _ensureFullyLoaded()} first — the same accurate-count
     * requirement UI-016 already established, now also covering data that hasn't reached the
     * client at all yet, not just data that reached it but wasn't rendered.
     */
    async expandAll() {
        await PA.callTrace._ensureFullyLoaded();
        // Counted from the tree, not from _nodeMap. Since UI-016 made indexing lazy, _nodeMap holds
        // only what has been rendered — which on a collapsed trace is a handful of rows, so the
        // warning below would have said "this trace has 3 nodes" before rendering 1,250 of them.
        // A safety prompt that under-reports is worse than none, because it reads as reassurance.
        const total = PA.callTrace._countNodes(PA.callTrace._root);
        if (total > PA.callTrace._EXPAND_ALL_WARN &&
            !window.confirm('This trace has ' + total + ' nodes. Expanding all of them will render '
                          + 'every row and may take a moment.\n\nExpand anyway?')) {
            return;
        }
        for (let pass = 0; pass < 200; pass++) {
            const pending = document.querySelectorAll('.ct-children[data-loaded="0"]');
            if (pending.length === 0) break;
            pending.forEach(el => {
                const step = parseInt(el.id.replace('ct-children-', ''), 10);
                const row = document.getElementById('ct-' + step);
                const depth = row ? parseInt(row.dataset.depth || '0', 10) : 0;
                PA.callTrace._renderChildrenLazy(step, depth);
            });
        }
        document.querySelectorAll('.ct-children').forEach(el => { el.style.display = ''; });
        document.querySelectorAll('.ct-toggle').forEach(el => { if (el.style.visibility !== 'hidden') el.innerHTML = '&#9660;'; });
    },

    collapseAll() {
        document.querySelectorAll('.ct-children').forEach(el => el.style.display = 'none');
        document.querySelectorAll('.ct-toggle').forEach(el => { if (el.style.visibility !== 'hidden') el.innerHTML = '&#9654;'; });
    },

    /** Clicking a row: highlight it and load detail for this node */
    onRowClick(procId, event) {
        document.querySelectorAll('.ct-row.active').forEach(el => el.classList.remove('active'));
        const row = event.currentTarget;
        row.classList.add('active');

        // Load detail for this child node (header + stats) without re-rendering the tree
        PA.loadNodeDetail(procId);
    },

    /** Double-clicking a row: drill down (re-root tree at this node) */
    onRowDblClick(procId, event) {
        event.stopPropagation();
        PA.callTrace.drillDown(procId);
    },

    /** Clicking the procedure name: open source in a modal popup */
    onNameClick(procId, event) {
        event.stopPropagation();
        PA.loadNodeDetail(procId);
        PA.sourceView.openModal(procId);
    },

    /** Clicking a line number: open the source view at that line */
    onLineClick(sourceFile, line, event) {
        event.stopPropagation();
        if (sourceFile) {
            PA.sourceView.openAtLine(sourceFile, line);
        }
    },

    /** Matches shown before the search stops looking. */
    MAX_SEARCH_HITS: 200,

    /**
     * Nodes examined before the search gives up.
     *
     * <p>Capping the matches bounds what is *rendered*; it does not bound the *walk*. A query that
     * matches nothing still visits every node, and the tree in memory is only as small as whatever
     * the server sent — one measured JAR endpoint expands to 300,167,238 nodes. Without this, "no
     * matches" would be the slowest possible answer on the largest possible tree, per keystroke.
     *
     * <p>Larger than MAX_SEARCH_HITS by a wide margin, because scanning is cheap next to rendering
     * and stopping early is a wrong answer, not a slow one — so the notice says the search was cut.
     */
    MAX_SEARCH_SCAN: 50000,

    /**
     * The nodes matching {@code q}, each with the path from the root — UI-018.
     *
     * <p>Searched in the tree data rather than in the DOM, which is the whole point: the previous
     * version called {@code expandAll()} to put every row in the document and then hid the ones
     * that did not match. Two things wrong with that. It materialised the entire tree to answer a
     * question about a handful of nodes; and since UI-016 gave `expandAll` an accurate count, it
     * also meant a <b>confirm dialog on every keystroke</b> — the box is wired to `oninput`.
     *
     * <p>Matches the same two strings the rendered row shows, so results do not change: the
     * qualified name that `.ct-name` carries, and the owner schema in `.ct-schema-badge`.
     *
     * <p>Iterative, and capped twice — on matches and on nodes examined. A trace can reach 300
     * million nodes, and "the search box is slow" is a worse failure than "the first 200 matches".
     * Capping only the matches would have left a query that matches nothing walking the whole
     * tree on every keystroke, which is the case that is both slowest and most common while
     * someone is still typing.
     */
    _findMatches(q, cap) {
        const out = [];
        if (!PA.callTrace._root) return { paths: out, truncated: false, scanned: 0 };
        let scanned = 0;
        // Path-carrying stack: each entry is the chain from the root to that node, so an ancestor
        // chain is available without parent pointers on the data.
        const stack = [[PA.callTrace._root]];
        while (stack.length) {
            const path = stack.pop();
            const node = path[path.length - 1];
            scanned++;
            if (scanned > PA.callTrace.MAX_SEARCH_SCAN) {
                return { paths: out, truncated: true, scanned: scanned - 1 };
            }
            const schema = node.schemaName || '';
            const pkg = node.packageName || '';
            const name = node.name || node.id || '';
            const fullName = [schema, pkg, name].filter(Boolean).join('.').toLowerCase();
            if (fullName.includes(q) || schema.toLowerCase().includes(q)) {
                out.push(path);
                if (out.length >= cap) return { paths: out, truncated: true, scanned };
            }
            if (!node.circular) {
                // Same children the renderer draws: a match on a row that collapsing folded away
                // could never be expanded to, so searching them would report unreachable hits.
                for (const child of PA.callTrace._childrenFor(node)) stack.push(path.concat([child]));
            }
        }
        return { paths: out, truncated: false, scanned };
    },

    /**
     * Renders the ancestors of a match, so the match itself exists in the document.
     *
     * <p>Top-down, because {@code _renderChildrenLazy} builds one level from a node that is
     * already rendered — the root always is, and each step makes the next one available.
     */
    _expandPath(path) {
        for (let i = 0; i < path.length - 1; i++) {
            const ancestor = path[i];
            if (ancestor._step === undefined) return;   // its parent was never rendered
            PA.callTrace._renderChildrenLazy(ancestor._step, i);
            const container = document.getElementById('ct-children-' + ancestor._step);
            if (container) container.style.display = '';
        }
    },

    /**
     * Search within the Explore tree — expands only the paths that contain a match.
     *
     * UI-019 (§142): a non-empty query needs the *whole* tree in memory the same way `expandAll()`
     * does (UI-018's own doc comment above `_findMatches` — searching in-memory data, not the DOM,
     * is "the whole point"), so this awaits `_ensureFullyLoaded()` first. That call re-renders the
     * container from scratch and clears the search box (`render()`'s own behaviour) — restored
     * right after, since the box already holds what the user actually typed.
     */
    async search(query) {
        const q = (query || '').trim().toLowerCase();
        if (!q) {
            document.querySelectorAll('#ctContainer .ct-row').forEach(r => {
                r.style.display = ''; r.classList.remove('ct-search-hit');
            });
            document.querySelectorAll('#ctContainer .ct-children').forEach(el => el.style.display = '');
            PA.callTrace._renderSearchNotice(null);
            return;
        }

        if (!PA.callTrace._fullyLoaded) {
            await PA.callTrace._ensureFullyLoaded();
            const searchBox = document.getElementById('exploreSearch');
            if (searchBox) searchBox.value = query;
        }

        const found = PA.callTrace._findMatches(q, PA.callTrace.MAX_SEARCH_HITS);
        for (const path of found.paths) PA.callTrace._expandPath(path);
        PA.callTrace._renderSearchNotice(found);

        const allRows = document.querySelectorAll('#ctContainer .ct-row');
        allRows.forEach(r => {
            const name = (r.querySelector('.ct-name')?.textContent || '').toLowerCase();
            const schema = (r.querySelector('.ct-schema-badge')?.textContent || '').toLowerCase();
            const hit = name.includes(q) || schema.includes(q);
            r.classList.toggle('ct-search-hit', hit);
            if (hit) {
                r.style.display = '';
                let parent = r.parentElement;
                while (parent && parent.id !== 'ctContainer') {
                    if (parent.classList.contains('ct-children')) parent.style.display = '';
                    parent = parent.parentElement;
                }
            } else {
                r.style.display = 'none';
            }
        });
    },

    /**
     * Says when the search stopped early.
     *
     * <p>A capped result list and a complete one look identical, so without this "200 matches" is
     * indistinguishable from "exactly 200 matches exist" — the same reason every other ceiling in
     * this codebase is paired with a notice. A DOM node rather than paint, so it survives scrolling.
     */
    _renderSearchNotice(found) {
        const existing = document.getElementById('ctSearchNotice');
        if (existing) existing.remove();
        if (!found || !found.truncated) return;
        const container = document.getElementById('ctContainer');
        if (!container) return;
        const div = document.createElement('div');
        div.id = 'ctSearchNotice';
        div.className = 'cg-truncated';
        div.textContent = found.paths.length >= PA.callTrace.MAX_SEARCH_HITS
            ? 'Showing the first ' + found.paths.length
              + ' matches. Narrow the search to see the rest.'
            : 'Searched the first ' + found.scanned.toLocaleString()
              + ' calls and stopped. Narrow the search, or expand a branch and search within it.';
        container.insertBefore(div, container.firstChild);
    },

    /** Update breadcrumb bar */
    updateBreadcrumb() {
        PA.callTrace._updateRootBtn();
        const bc = document.getElementById('breadcrumb');
        let html = '';
        for (let i = 0; i < PA.callTrace.breadcrumbStack.length; i++) {
            const item = PA.callTrace.breadcrumbStack[i];
            if (i > 0) html += '<span class="bc-sep">&rsaquo;</span>';
            if (i === PA.callTrace.breadcrumbStack.length - 1) {
                html += `<span class="bc-item bc-current">${PA.esc(item.name)}</span>`;
            } else {
                html += `<span class="bc-item" onclick="PA.callTrace.goTo(${i})">${PA.esc(item.name)}</span>`;
            }
        }
        bc.innerHTML = html;
    }
};
