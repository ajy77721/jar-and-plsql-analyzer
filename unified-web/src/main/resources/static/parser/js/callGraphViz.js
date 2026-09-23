window.PA = window.PA || {};

PA.callGraphViz = {
    _canvas: null,
    _ctx: null,
    _allNodes: [],
    _allEdges: [],
    _nodes: [],
    _edges: [],
    _transform: { x: 0, y: 0, scale: 1 },
    _drag: null,
    _hoveredNode: null,
    _selectedNode: null,
    _expandedNode: null,
    _visibleIds: null,
    _animFrame: null,
    _activePath: [],      // nodeIds from root → current selected node

    // UI-039 — §245 Bug 2 fix: a single click's own `expandNode()`→`_simulate()`→`_layoutTree()`
    // relayout used to run synchronously on every mouseup, which could move the clicked node out
    // from under a real double-click's second half before it landed (measured live in §245: a
    // 430px/230px jump in the same frame as the first click). `_pendingClick` holds the node
    // identified by a single click plus its debounce timer while we wait to see whether a second
    // click arrives (making it a double-click) before committing to the relayout.
    _pendingClick: null,

    // UI-037 — back/forward node-travel history, modeled directly on `PA.sourceModal._history`/
    // `_historyFwd` (`sourceView.js` ~lines 36-160): every genuine navigation (single-click select
    // or double-click re-root) pushes the node just left onto `_history`; `back()`/`forward()` walk
    // that stack the same way the source modal does, and a genuinely new navigation (not a
    // back/forward replay) clears `_historyFwd`, exactly mirroring the source modal's rule.
    _history: [],
    _historyFwd: [],

    /**
     * §261 — monotonically increasing token identifying the in-flight `load()`. `load()` is not
     * re-entrant: it `await`s two independent async steps (`getCallGraphTop`, then `_buildGraph`'s
     * own `nodeIndex.ensureLoaded()`), and nothing before this stopped a second `load()` starting
     * while the first was still suspended at one of them. Confirmed by reading the call sites:
     * `app.js` guards the *first* open with `PA._rightTabLoaded.callGraphViz`, but that flag is
     * cleared whenever a new analysis is selected — so selecting analysis A, switching to Graph,
     * then immediately selecting analysis B and switching back starts a second `load()` while A's
     * fetch is still outstanding. Whichever request happened to finish last then won, and could
     * paint analysis A's nodes over analysis B's freshly-built canvas.
     *
     * Every `await` point in `load()` now re-checks this token and bails out if a newer load has
     * started since, so a superseded load can never write to `_allNodes`/the canvas.
     */
    _loadToken: 0,

    /**
     * §261 — tear down everything the *previous* `load()` attached before building a new one.
     *
     * Two concrete leaks this fixes, both confirmed live:
     *
     * 1. `_initCanvas()` created a `new ResizeObserver(...)` and observed `graphContainer` without
     *    ever keeping a reference to it. `load()` only replaces the container's *children*
     *    (`container.innerHTML = ...`), never the container itself — so every re-load added another
     *    live observer to the same element. The stale ones keep a detached `<canvas>` alive and
     *    still fire `_draw()` on every resize, each one writing to a canvas that is no longer in
     *    the document.
     * 2. A `_pendingClick` debounce timer armed by the last click before a reload kept running
     *    across it, then fired `expandNode()` for a node id belonging to the *previous* analysis
     *    against the newly-built graph. Verified live: the timer fired after a full reload
     *    (`ghostTimerFiredAfterReload: true`).
     */
    _teardown: function() {
        var v = PA.callGraphViz;
        if (v._resizeObserver) {
            try { v._resizeObserver.disconnect(); } catch (e) { /* already gone */ }
            v._resizeObserver = null;
        }
        if (v._pendingClick) {
            clearTimeout(v._pendingClick.timer);
            v._pendingClick = null;
        }
        if (v._drawFrame) {
            cancelAnimationFrame(v._drawFrame);
            v._drawFrame = null;
        }
        if (v._animFrame) {
            cancelAnimationFrame(v._animFrame);
            v._animFrame = null;
        }
        // The window-level drag listeners (see `_initCanvas`) outlive the canvas they were armed
        // for, so they must come off explicitly or every reload stacks another copy.
        if (v._winMouseUp) {
            window.removeEventListener('mouseup', v._winMouseUp, true);
            v._winMouseUp = null;
        }
        if (v._winMouseMove) {
            window.removeEventListener('mousemove', v._winMouseMove, true);
            v._winMouseMove = null;
        }
        v._drag = null;
        v._hoveredNode = null;
        v._hoveredEdge = null;
        v._canvas = null;
        v._ctx = null;
    },

    load: async function() {
        var container = document.getElementById('graphContainer');
        if (!container) return;

        var token = ++PA.callGraphViz._loadToken;
        PA.callGraphViz._teardown();
        PA.callGraphViz._scopeIdMap = null; // §260: node ids are per-analysis, cache must not leak across loads
        container.innerHTML = '<canvas id="graphCanvas"></canvas><div class="graph-controls" id="graphControls"></div><div class="graph-tooltip" id="graphTooltip" style="display:none"></div>';

        try {
            // Bounded fetch: ask for what can actually be drawn rather than everything.
            var data = await PA.api.getCallGraphTop(PA.callGraphViz.MAX_SIMULATED_NODES);
            if (token !== PA.callGraphViz._loadToken) return;   // §261: superseded by a newer load
            // The server reports the real totals, so the notice can say how many nodes exist
            // rather than how many happened to arrive.
            PA.callGraphViz._serverTotalNodes = data && data.truncated ? (data.totalNodes || 0) : 0;
            if (!data || !data.edges || data.edges.length === 0) {
                container.innerHTML = '<div class="empty-msg">No call graph data available</div>';
                return;
            }
            await PA.callGraphViz._buildGraph(data);
            if (token !== PA.callGraphViz._loadToken) return;   // §261: superseded while enriching
            PA.callGraphViz._initCanvas();
            PA.callGraphViz._renderControls();
            PA.callGraphViz._simulate();
        } catch (e) {
            if (token !== PA.callGraphViz._loadToken) return;
            console.error('[PA] callGraphViz.load failed', e);
            container.innerHTML = '<div class="empty-msg">Failed to load call graph</div>';
        }
    },

    /**
     * UI-024, and a real bug found while fixing it: this used to pre-populate `nodeMap` from
     * `PA.nodeIndex.all()` — every node in the whole analysis, not just the ones `data.edges`
     * (already a bounded, server-truncated set — see `load()`) actually references. Two problems,
     * not one:
     *
     * 1. The obvious one — a full-corpus scan for a graph that only ever draws a bounded subset.
     * 2. **`_getRootIds()` (below) treats any node with no incoming edge as a graph root.** Every
     *    node from the full index that the truncated edge set never touches has `inEdges === 0` by
     *    construction, so it was being classified as a root alongside the graph's genuine roots —
     *    the initial visible set (`_visibleIds = new Set(rootIds)`) could include every disconnected
     *    node in the whole analysis, not just the real ones. Confirmed by reading `_getRootIds`
     *    directly, not by running the view.
     *
     * Fixed by only ever creating a `nodeMap` entry for an id that actually appears in an edge,
     * enriching it from `PA.nodeIndex.byId` (the O(1) lookup, not a scan) when available and
     * falling back to the edge's own `from`/`fromSchema` fields otherwise — the same fallback the
     * old code already had for an id `PA.nodeIndex` didn't know about.
     */
    _buildGraph: async function(data) {
        var nodeMap = {};
        var edges = data.edges || [];
        // UI-024 (§163) — one preload for the whole graph build, not per-node: `load()` (this
        // method's only caller) already runs once when the call-graph tab opens, so this is the
        // same tab-open-preload pattern already used elsewhere, not a per-lookup await.
        await PA.nodeIndex.ensureLoaded();

        function ensureNode(id, label, schema) {
            if (!id || nodeMap[id]) return;
            var indexed = PA.nodeIndex.byId(id);
            nodeMap[id] = {
                id: id,
                label: (indexed && indexed.name) || label || id,
                schema: (indexed && indexed.schema) || schema || '',
                objectType: (indexed && indexed.objectType) || '',
                depth: (indexed && indexed.depth != null) ? indexed.depth : 0,
                loc: (indexed && indexed.linesOfCode) || 0,
                x: Math.random() * 600 - 300, y: Math.random() * 400 - 200,
                vx: 0, vy: 0, radius: 0, inEdges: 0, outEdges: 0
            };
        }

        var graphEdges = [];
        for (var i = 0; i < edges.length; i++) {
            var e = edges[i];
            var fromId = e.fromNodeId || e.from || '';
            var toId = e.toNodeId || e.to || '';
            ensureNode(fromId, e.from, e.fromSchema);
            ensureNode(toId, e.to, e.toSchema);
            if (nodeMap[fromId]) nodeMap[fromId].outEdges++;
            if (nodeMap[toId]) nodeMap[toId].inEdges++;
            graphEdges.push({
                from: fromId, to: toId,
                count: e.count || 1, backEdge: e.backEdge || false,
                callType: e.callType || '', edgeType: e.edgeType || '',
                // §262 — carried through for the edge hover tooltip (`_updateEdgeTooltip`).
                // `lines` (the call-site line numbers) and `minDepth` are merged per-edge by the
                // server's `CallGraphMapper` and were being dropped on the floor here; `condition`
                // is read defensively under the several names a conditional/guarded call has been
                // seen under, since an edge that only *sometimes* fires is a materially different
                // fact from one that always does.
                lines: Array.isArray(e.lines) ? e.lines : null,
                minDepth: (e.minDepth != null) ? e.minDepth : null,
                condition: e.condition || e.callCondition || e.joinCondition || e.guard || ''
            });
        }

        var nodes = Object.values(nodeMap);
        var maxEdges = 1;
        for (var i = 0; i < nodes.length; i++) {
            var total = nodes[i].inEdges + nodes[i].outEdges;
            if (total > maxEdges) maxEdges = total;
        }
        for (var i = 0; i < nodes.length; i++) {
            var total = nodes[i].inEdges + nodes[i].outEdges;
            var baseR = Math.max(8, Math.min(24, 8 + (total / maxEdges) * 16));
            if (nodes[i].depth === 0 && nodes[i].inEdges === 0) baseR = Math.max(baseR, 18);
            nodes[i].radius = baseR;
        }

        PA.callGraphViz._allNodes = nodes;
        PA.callGraphViz._allEdges = graphEdges;
        PA.callGraphViz._buildChildMap();
        // §222 follow-up: for the common single-root-entry-point shape, seeding the initial visible
        // set to root(s) only made a perfectly correct, already-loaded small graph look broken (e.g.
        // "1 nodes, 0 edges" for a real 51-node/218-edge graph). Root-only collapse is genuinely
        // needed once a graph is large enough that `_simulate()`'s force layout gets slow/unreadable
        // — that's exactly what `MAX_SIMULATED_NODES` (the same 300-node cap `load()` already uses
        // to truncate the fetched set, see above) marks as "too big to render at once". So: below
        // that same threshold, default straight to the full graph (`_visibleIds = null`, the same
        // end state `showAll()` produces); at/above it, keep today's root-only seeding unchanged —
        // the "All"/expand/collapse controls still work identically in both cases either way.
        if (nodes.length < PA.callGraphViz.MAX_SIMULATED_NODES) {
            PA.callGraphViz._visibleIds = null;
        } else {
            var rootIds = PA.callGraphViz._getRootIds();
            PA.callGraphViz._visibleIds = new Set(rootIds);
        }
        PA.callGraphViz._expandedNode = null;
        PA.callGraphViz._activePath = [];
        // UI-037 — a freshly (re)built graph has no travel history yet.
        PA.callGraphViz._history = [];
        PA.callGraphViz._historyFwd = [];
        // §261 — node ids are per-analysis, so search hits and the focus target from the previous
        // graph are meaningless here; leaving them set would draw highlight rings for ids that no
        // longer exist and pin focus mode to a node that is gone.
        PA.callGraphViz._searchMatches = [];
        PA.callGraphViz._searchIdx = -1;
        PA.callGraphViz._searchTerm = '';
        PA.callGraphViz._focusId = null;
        // §262 — chain ids embed a member node id, so an expanded-chain set from the previous
        // analysis is meaningless here and would silently keep an unrelated run expanded.
        PA.callGraphViz._expandedChains = new Set();
        PA.callGraphViz._chainOf = {};
        PA.callGraphViz._chains = {};
        PA.callGraphViz._applyVisibility();
    },

    /**
     * The page-level Direct/Subtree/SubFlow/Full toggle (`scope.js`) lives in the shared detail
     * header, visible above every right-tab including this one — so it fires here too
     * (`app.js`'s `onScopeChange`). Before this fix the handler ignored `PA._scope` entirely and
     * just re-ran `_applyVisibility()` with whatever expand/collapse state was already set,
     * i.e. clicking Subtree/SubFlow/Full on the Graph tab visibly did nothing: the exact "some
     * more information... coming empty" / "disabled" complaint — the toggle wasn't producing an
     * empty graph so much as a *frozen* one that looked broken next to the other tabs it does
     * filter (joins/sequences/tableDetail/...).
     *
     * Now scoped the same way those tabs are, off `PA.context.callTreeNodeIds` /
     * `PA._currentScopeId()`:
     *   - direct  → only the current node
     *   - subtree → its descendants, excluding itself
     *   - subflow → itself + its descendants (mirrors the tree's own root+expand view)
     *   - full    → every node the graph loaded (`_visibleIds = null`)
     * Package-root context (`__PKG_ROOT__`) has no single "current node", so it always behaves as
     * full, matching every other scope-aware tab's `PA._isPackageRoot()` special case.
     */
    applyScope: function() {
        var mode = PA._scope;
        if (mode && PA.context) {
            // §260 — `PA._currentScopeId()`/`PA.context.callTreeNodeIds` are bare procedure names
            // ("ERP_USP_RUN_CYCLE"); this graph's own node ids are schema-prefixed
            // ("ANALYZER$ERP_USP_RUN_CYCLE", read straight off `fromNodeId`/`toNodeId` server data).
            // Comparing them directly (as this did before) never matched anything, so Direct/
            // Subtree/SubFlow always resolved to an empty node set — confirmed live: a fresh page
            // load with zero navigation already rendered 0 nodes/0 edges on any of those three.
            // `_resolveGraphId` maps the bare id to whichever loaded graph node id actually ends
            // with it, so the filter has a real id to match against.
            var currentId = PA.callGraphViz._resolveGraphId(PA._currentScopeId());
            var treeIds = PA.context.callTreeNodeIds;
            if (PA._isPackageRoot()) {
                PA.callGraphViz._visibleIds = null;   // no single current node to scope from — behave as full
            } else if (mode === 'direct') {
                PA.callGraphViz._visibleIds = new Set([currentId]);
            } else if (mode === 'subtree') {
                var subtreeIds = new Set();
                if (treeIds) treeIds.forEach(function(id) {
                    var gid = PA.callGraphViz._resolveGraphId(id);
                    if (gid !== currentId) subtreeIds.add(gid);
                });
                PA.callGraphViz._visibleIds = subtreeIds;
            } else if (mode === 'subflow') {
                var subflowIds = new Set([currentId]);
                if (treeIds) treeIds.forEach(function(id) { subflowIds.add(PA.callGraphViz._resolveGraphId(id)); });
                PA.callGraphViz._visibleIds = subflowIds;
            } else {
                PA.callGraphViz._visibleIds = null;   // full
            }
            PA.callGraphViz._expandedNode = null;
            PA.callGraphViz._activePath = [];
        }
        PA.callGraphViz._applyVisibility();
        if (PA.callGraphViz._canvas) {
            // The scope-filtered node/edge set just changed — re-run the tree layout the same
            // way `load()`/`showAll()`/`expandNode()` already do, so Direct/Subtree/SubFlow/Full
            // each produce a fresh left-to-right layout instead of stale positions.
            if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
            PA.callGraphViz._simulate();
            PA.callGraphViz._updateControlsCount();
        }
    },

    _buildChildMap: function() {
        var edges = PA.callGraphViz._allEdges;
        var childMap = {};
        var parentMap = {};
        // §261 — `_parentMap` only ever recorded *that* a node has a caller (a boolean, all
        // `_getRootIds` needs). Focus mode needs the callers themselves, so the actual reverse
        // adjacency is built alongside it here rather than re-scanning every edge on each redraw.
        var parentsOf = {};
        for (var i = 0; i < edges.length; i++) {
            var from = edges[i].from;
            var to = edges[i].to;
            if (!childMap[from]) childMap[from] = [];
            childMap[from].push(to);
            parentMap[to] = true;
            if (!parentsOf[to]) parentsOf[to] = [];
            parentsOf[to].push(from);
        }
        PA.callGraphViz._childMap = childMap;
        PA.callGraphViz._parentMap = parentMap;
        PA.callGraphViz._parentsOf = parentsOf;
    },

    /**
     * §260 — resolves a bare procedure name (as returned by `PA._currentScopeId()` /
     * `PA.context.callTreeNodeIds`, e.g. `"ERP_USP_RUN_CYCLE"`) to the matching schema-prefixed
     * graph node id (e.g. `"ANALYZER$ERP_USP_RUN_CYCLE"`) actually used in `_allNodes`/`_visibleIds`.
     * Exact id matches pass through unchanged; case-insensitive, since both sides are already
     * `.toUpperCase()`'d inconsistently in different places. Built lazily and cached per-load
     * (`_scopeIdMap`, cleared in `load()`) since this can run on every scope-toggle click.
     */
    _resolveGraphId: function(bareId) {
        if (!bareId) return bareId;
        if (!PA.callGraphViz._scopeIdMap) {
            var map = {};
            var nodes = PA.callGraphViz._allNodes || [];
            for (var i = 0; i < nodes.length; i++) {
                var id = nodes[i].id;
                var idx = id.indexOf('$');
                var bare = (idx >= 0 ? id.substring(idx + 1) : id).toUpperCase();
                map[bare] = id;
                map[id.toUpperCase()] = id;
            }
            PA.callGraphViz._scopeIdMap = map;
        }
        var resolved = PA.callGraphViz._scopeIdMap[bareId.toUpperCase()];
        return resolved || bareId;
    },

    _getRootIds: function() {
        var nodes = PA.callGraphViz._allNodes;
        var parentMap = PA.callGraphViz._parentMap || {};
        var roots = [];
        for (var i = 0; i < nodes.length; i++) {
            if (!parentMap[nodes[i].id]) roots.push(nodes[i].id);
        }
        if (roots.length === 0 && nodes.length > 0) roots.push(nodes[0].id);
        return roots;
    },

    _applyVisibility: function() {
        var visibleIds = PA.callGraphViz._visibleIds;
        if (!visibleIds) {
            PA.callGraphViz._nodes = PA.callGraphViz._allNodes;
            PA.callGraphViz._edges = PA.callGraphViz._allEdges;
            PA.callGraphViz._collapseChains();
            return;
        }
        var nodeMap = {};
        PA.callGraphViz._nodes = PA.callGraphViz._allNodes.filter(function(n) {
            if (visibleIds.has(n.id)) { nodeMap[n.id] = true; return true; }
            return false;
        });
        PA.callGraphViz._edges = PA.callGraphViz._allEdges.filter(function(e) {
            return nodeMap[e.from] && nodeMap[e.to];
        });
        PA.callGraphViz._collapseChains();
    },

    /* =====================================================================================
     * §262 — deep linear call-chain collapsing.
     *
     * The Postgres demo analysis is **69 levels deep**, and almost all of that depth is a single
     * unbranching run: procedure calls procedure calls procedure, one caller and one callee each,
     * for dozens of levels. `_layoutTree` faithfully lays that out as 69 sequential columns —
     * ~10,000 world pixels wide — so `fitAll` cannot show it above the legibility floor
     * (`MIN_FIT_SCALE`, see its doc, where this same analysis is what forced that clamp in), and a
     * reader is left panning sideways through dozens of nodes that, structurally, say one thing:
     * "and then it keeps going".
     *
     * A run like that carries no branching information, so it can be stated rather than drawn. A
     * maximal run of nodes that each have exactly one visible caller and one visible callee is
     * replaced by a single placeholder node, and the run's incoming and outgoing edges are rewired
     * to it. Clicking the placeholder expands that one run back into its individual nodes
     * (`toggleChain`); the "Chains" button turns the whole behaviour off.
     *
     * Deliberately done as a post-pass over the already-computed `_nodes`/`_edges` rather than
     * inside the layout: everything downstream — `_layoutTree`, `_draw`, `_findNodeAt`, `fitAll`,
     * viewport culling — keeps operating on an ordinary node/edge list and needs no knowledge that
     * chains exist. `_allNodes`/`_allEdges` are never mutated, so search, `_getRootIds`,
     * `_findPath` and the scope filters all continue to see the real, complete graph.
     * ===================================================================================== */

    /**
     * Shortest linear run worth collapsing.
     *
     * Below this the placeholder costs more than it saves: replacing 3 nodes with one box that has
     * to be clicked to read is a worse trade than simply showing the 3 nodes. At 4+ the collapse
     * starts removing more columns from the layout than it adds.
     */
    MIN_CHAIN_RUN: 4,

    /** Whether linear runs are collapsed at all — toggled by the "Chains" control-bar button. */
    _chainMode: true,

    /** Chain ids the reader has explicitly opened; they stay expanded until toggled shut again. */
    _expandedChains: null,

    /** member node id → the chain id currently standing in for it (rebuilt every `_applyVisibility`). */
    _chainOf: null,

    /** chain id → the placeholder node object (rebuilt every `_applyVisibility`). */
    _chains: null,

    _collapseChains: function() {
        var v = PA.callGraphViz;
        v._chainOf = {};
        v._chains = {};
        if (!v._chainMode) return;
        var nodes = v._nodes;
        var edges = v._edges;
        if (!nodes || nodes.length < v.MIN_CHAIN_RUN + 1) return;

        var present = {};
        for (var i = 0; i < nodes.length; i++) present[nodes[i].id] = nodes[i];

        /**
         * Nodes that must stay individually visible no matter what. Hiding any of these inside a
         * placeholder would break a feature that is already working: a search hit the reader was
         * just sent to would vanish, the breadcrumb path would point at something not on screen,
         * and the focus target would have nothing to focus on.
         */
        var pinned = new Set();
        function pin(id) { if (id) pinned.add(id); }
        v._getRootIds().forEach(pin);
        (v._activePath || []).forEach(pin);
        (v._searchMatches || []).forEach(pin);
        if (v._selectedNode) pin(v._selectedNode.id);
        pin(v._expandedNode);
        pin(v._focusId);

        // Ask the layout, not the adjacency, where each node lands — see `_bfsDepths`. A run is a
        // stretch of consecutive layout columns holding exactly one node each, which is precisely
        // the shape that makes the drawn tree long and thin and forces the reader to pan sideways
        // through it.
        var bfs = v._bfsDepths(nodes, edges, present);
        var columns = {};
        for (var ci = 0; ci < nodes.length; ci++) {
            var nid = nodes[ci].id;
            var dep = bfs.depth[nid];
            if (dep == null) continue;
            (columns[dep] = columns[dep] || []).push(nid);
        }
        var depths = Object.keys(columns).map(Number).sort(function(a, b) { return a - b; });

        var runs = [];
        var cursor = 0;
        while (cursor < depths.length) {
            var run = [];
            var expected = depths[cursor];
            var j = cursor;
            while (j < depths.length && depths[j] === expected && columns[depths[j]].length === 1) {
                var cand = columns[depths[j]][0];
                if (pinned.has(cand)) break;
                // Consecutive singleton columns are only a *chain* if the layout actually drew
                // them as one: each member must be the BFS child of the one before it. Without
                // this check, two unrelated single-node components appended by `_bfsDepths`'s
                // disconnected-component passes could be merged into one bogus "chain".
                if (run.length && bfs.parent[cand] !== run[run.length - 1]) break;
                run.push(cand);
                expected++;
                j++;
            }
            if (run.length >= v.MIN_CHAIN_RUN) {
                runs.push(run);
                cursor = j;
            } else {
                cursor++;
            }
        }
        if (!runs.length) return;

        // Rewiring needs the real in/out endpoints of each run's head and tail, counted over
        // distinct pairs (parallel edges between one pair are one structural link).
        var outs = {}, ins = {}, pairSeen = {};
        for (var e = 0; e < edges.length; e++) {
            var ed = edges[e];
            if (!present[ed.from] || !present[ed.to]) continue;
            var pk = ed.from + ' ' + ed.to;
            if (pairSeen[pk]) continue;
            pairSeen[pk] = true;
            (outs[ed.from] = outs[ed.from] || []).push(ed.to);
            (ins[ed.to] = ins[ed.to] || []).push(ed.from);
        }

        var replaced = {};
        var chainNodes = [];
        for (var q = 0; q < runs.length; q++) {
            var rn = runs[q];
            // Keyed on the head's id so the same run keeps the same id across relayouts — that is
            // what lets `_expandedChains` remember which chains the reader opened.
            var chainId = '__chain__$' + rn[0];
            if (v._expandedChains && v._expandedChains.has(chainId)) continue;
            var head = present[rn[0]];
            var tail = present[rn[rn.length - 1]];
            var cn = {
                id: chainId, chain: true, members: rn.slice(),
                label: rn.length + ' nodes',
                schema: '', objectType: '', depth: head.depth, loc: 0,
                x: head.x, y: head.y, vx: 0, vy: 0, radius: 20,
                inEdges: (ins[rn[0]] || []).length,
                outEdges: (outs[tail.id] || []).length
            };
            chainNodes.push(cn);
            v._chains[chainId] = cn;
            for (var m = 0; m < rn.length; m++) {
                replaced[rn[m]] = chainId;
                v._chainOf[rn[m]] = chainId;
            }
        }
        if (!chainNodes.length) return;

        v._nodes = nodes.filter(function(nd) { return !replaced[nd.id]; }).concat(chainNodes);

        // Edges untouched by any collapse are passed through byte-for-byte (same objects), so the
        // hovered-edge identity check in `_draw`/`_findEdgeAt` and every existing edge behaviour
        // is unaffected. Only rewritten edges are deduped, because collapsing a run can otherwise
        // produce several identical chain→node links.
        var rewrittenSeen = {};
        var newEdges = [];
        for (var x = 0; x < edges.length; x++) {
            var oe = edges[x];
            var f = replaced[oe.from];
            var t = replaced[oe.to];
            if (!f && !t) { newEdges.push(oe); continue; }
            var nf = f || oe.from;
            var nt = t || oe.to;
            if (nf === nt) continue;   // wholly internal to one chain — nothing left to draw
            var key = nf + ' ' + nt;
            if (rewrittenSeen[key]) continue;
            rewrittenSeen[key] = true;
            newEdges.push({
                from: nf, to: nt, count: oe.count, backEdge: oe.backEdge,
                callType: oe.callType, edgeType: oe.edgeType,
                lines: oe.lines, minDepth: oe.minDepth, condition: oe.condition
            });
        }
        v._edges = newEdges;
    },

    /**
     * Open or re-close one collapsed chain. Runs the same relayout path every other
     * visibility-changing action here uses (`_applyVisibility` → `_simulate`), so the reader gets
     * the identical fresh left-to-right tree they get from expand/collapse/scope changes.
     */
    toggleChain: function(chainId) {
        var v = PA.callGraphViz;
        if (!v._expandedChains) v._expandedChains = new Set();
        if (v._expandedChains.has(chainId)) v._expandedChains.delete(chainId);
        else v._expandedChains.add(chainId);
        // The placeholder object is about to stop existing; leaving it as the selection would make
        // `_layoutTree` try to root the whole tree on a node that is no longer in the node map.
        if (v._selectedNode && v._selectedNode.chain) v._selectedNode = null;
        if (v._focusId && String(v._focusId).indexOf('__chain__$') === 0) v._focusId = null;
        v._hoveredNode = null;
        v._hoveredEdge = null;
        v._hideTooltip();
        v._applyVisibility();
        v._updateControlsCount();
        if (v._animFrame) cancelAnimationFrame(v._animFrame);
        v._simulate();
    },

    /**
     * If `nodeId` is currently hidden inside a collapsed chain, open that chain so the node is a
     * real, positioned node again. Used by `revealNode` — a search hit that resolves to a node
     * buried in a placeholder must still be reachable, which is the whole point of the search.
     */
    _expandChainContaining: function(nodeId) {
        var v = PA.callGraphViz;
        var cid = (v._chainOf || {})[nodeId];
        if (!cid) return false;
        if (!v._expandedChains) v._expandedChains = new Set();
        v._expandedChains.add(cid);
        v._applyVisibility();
        v._updateControlsCount();
        if (v._animFrame) cancelAnimationFrame(v._animFrame);
        v._simulate();
        return true;
    },

    /** The "Chains" control-bar toggle: collapse long linear runs, or show every node individually. */
    toggleChainMode: function() {
        var v = PA.callGraphViz;
        v._chainMode = !v._chainMode;
        v._expandedChains = new Set();
        if (v._selectedNode && v._selectedNode.chain) v._selectedNode = null;
        v._hoveredNode = null;
        v._hoveredEdge = null;
        v._hideTooltip();
        v._applyVisibility();
        v._renderControls();
        if (v._animFrame) cancelAnimationFrame(v._animFrame);
        v._simulate();
    },

    expandNode: function(nodeId) {
        var childMap = PA.callGraphViz._childMap || {};
        var activePath = PA.callGraphViz._activePath;

        // Explicit user rule: clicking the ROOT/entry-point node shows the COMPLETE graph (every
        // node at every level), not just root + its immediate children — the same "just one level"
        // treatment every other node gets. Confirmed live before this fix: clicking the root on a
        // 114-node analysis only surfaced 17 nodes (root + 16 direct children); every deeper
        // descendant stayed hidden even though the root, structurally, IS the ancestor of all of
        // them. Every other rule (single-click travel extends the path one level at a time;
        // clicking a different sibling branch replaces the path instead of accumulating; double-
        // click re-roots to just that node + its direct children) was already verified working
        // correctly and is untouched here.
        if (PA.callGraphViz._getRootIds().indexOf(nodeId) >= 0) {
            // showAll() itself doesn't touch _activePath (it's meant to be layered under whatever
            // path state existed, e.g. history back/forward) — but a root click here is a genuine
            // "start fresh from the top" action, so the path must reset too, or the very next
            // single-click's "is this a child of the last path node" check would compare against
            // a stale, now-meaningless path from before this root click.
            PA.callGraphViz._activePath = [];
            PA.callGraphViz.showAll();
            return;
        }

        // Check if nodeId is already in the active path
        var existingIdx = activePath.indexOf(nodeId);
        if (existingIdx >= 0) {
            // Clicked a node already in the path — truncate to that point
            PA.callGraphViz._activePath = activePath.slice(0, existingIdx + 1);
        } else {
            // Check if nodeId is a child of the last node in the path
            var lastNode = activePath.length ? activePath[activePath.length - 1] : null;
            var lastChildren = lastNode ? (childMap[lastNode] || []) : [];
            if (lastChildren.indexOf(nodeId) >= 0) {
                // Extend path
                PA.callGraphViz._activePath = activePath.concat([nodeId]);
            } else {
                // New branch — start fresh path from root(s) to this node via BFS
                var newPath = PA.callGraphViz._findPath(nodeId);
                PA.callGraphViz._activePath = newPath.length ? newPath : [nodeId];
            }
        }

        // Build visible set: all nodes in activePath + children of the last node.
        //
        // Real bug, found via an actual simulated mouse click (not a direct expandNode() call,
        // which never caught this): the old version below only added the TERMINAL node's own
        // children to the visible set, dropping every other child of every ANCESTOR node in the
        // path along the way. So after traveling root -> childA, childA's own siblings (childB,
        // childC, ... — root's other direct children) were removed from the canvas entirely, with
        // nothing left to click to switch to them. Confirmed live: a real mouse click on childB's
        // on-screen coordinates right after clicking childA did nothing, because childB was no
        // longer part of `_visibleIds` at all. Explicit user rule: "same level also we can
        // multiple" — siblings at every visited level must stay clickable, not just the newest
        // node's own children. Fixed by adding EVERY path node's children, not only the
        // terminal's — the whole ancestor chain's sibling sets, plus the new leaf's own children.
        var path = PA.callGraphViz._activePath;
        var vis = new Set(PA.callGraphViz._getRootIds());
        for (var i = 0; i < path.length; i++) {
            vis.add(path[i]);
            var siblingsAtThisLevel = childMap[path[i]] || [];
            for (var j = 0; j < siblingsAtThisLevel.length; j++) vis.add(siblingsAtThisLevel[j]);
        }
        var terminalNode = path.length ? path[path.length - 1] : nodeId;

        PA.callGraphViz._visibleIds = vis;
        PA.callGraphViz._expandedNode = terminalNode;
        PA.callGraphViz._applyVisibility();
        PA.callGraphViz._updateControlsCount();
        if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
        PA.callGraphViz._simulate();
    },

    // BFS to find path from any root to target node
    _findPath: function(targetId) {
        var childMap = PA.callGraphViz._childMap || {};
        var roots = PA.callGraphViz._getRootIds();
        var queue = roots.map(function(r) { return [r]; });
        var visited = new Set(roots);
        while (queue.length) {
            var path = queue.shift();
            var last = path[path.length - 1];
            if (last === targetId) return path;
            var kids = childMap[last] || [];
            for (var i = 0; i < kids.length; i++) {
                if (!visited.has(kids[i])) {
                    visited.add(kids[i]);
                    queue.push(path.concat([kids[i]]));
                }
            }
        }
        return [];
    },

    collapseAll: function() {
        var roots = new Set(PA.callGraphViz._getRootIds());
        PA.callGraphViz._visibleIds = roots;
        PA.callGraphViz._expandedNode = null;
        PA.callGraphViz._activePath = [];
        PA.callGraphViz._selectedNode = null;
        PA.callGraphViz._applyVisibility();
        PA.callGraphViz._updateControlsCount();
        PA.callGraphViz._draw();
        PA.callGraphViz._closeNodeDetail();
    },

    showAll: function() {
        PA.callGraphViz._visibleIds = null;
        PA.callGraphViz._expandedNode = null;
        PA.callGraphViz._applyVisibility();
        PA.callGraphViz._updateControlsCount();
        if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
        PA.callGraphViz._simulate();
    },

    _updateControlsCount: function() {
        var ctrl = document.getElementById('graphControls');
        if (!ctrl) return;
        var span = ctrl.querySelector('span');
        if (span) span.textContent = PA.callGraphViz._nodes.length + ' nodes, ' + PA.callGraphViz._edges.length + ' edges';
    },

    _initCanvas: function() {
        var canvas = document.getElementById('graphCanvas');
        if (!canvas) return;
        var container = canvas.parentElement;
        canvas.width = container.clientWidth;
        canvas.height = container.clientHeight;
        PA.callGraphViz._canvas = canvas;
        PA.callGraphViz._ctx = canvas.getContext('2d');
        PA.callGraphViz._transform = { x: canvas.width / 2, y: canvas.height / 2, scale: 1 };

        canvas.addEventListener('mousedown', PA.callGraphViz._onMouseDown);
        canvas.addEventListener('mousemove', PA.callGraphViz._onMouseMove);
        canvas.addEventListener('wheel', PA.callGraphViz._onWheel, { passive: false });
        canvas.addEventListener('dblclick', PA.callGraphViz._onDblClick);
        canvas.addEventListener('mouseleave', PA.callGraphViz._onMouseLeave);

        /**
         * §261 — the drag must be released and tracked at the *window*, not the canvas.
         *
         * Before this, `mouseup` was bound to the canvas alone, so releasing the button anywhere
         * else — over the left procedure panel, the controls bar, or outside the window — never
         * cleared `_drag`. The node then stayed latched to the pointer: the very next plain
         * mouse-move over the canvas (no button held) kept running the drag branch of
         * `_onMouseMove` and flung the node across the graph. Reproduced live with real CDP input
         * events: press a node, move, release at (30, 500) over the left panel, then simply move
         * the pointer across the canvas — node `ERP$ERP_USP_RUN_CYCLE` travelled from
         * (461, 461) to (6235, -103) with no button down. That is the single biggest contributor
         * to the graph feeling unstable, because a node dragged off-canvas never lets go.
         *
         * Capture phase so the release is seen even if something else stops propagation, and the
         * move handler is window-level *only while a drag is active* so an off-canvas drag keeps
         * tracking smoothly (the normal hover path stays on the canvas, unchanged).
         */
        PA.callGraphViz._winMouseUp = function(ev) { PA.callGraphViz._onMouseUp(ev); };
        PA.callGraphViz._winMouseMove = function(ev) {
            var d = PA.callGraphViz._drag;
            if (!d) return;
            // A drag in progress with no button still held (release missed entirely, e.g. the
            // pointer left the window) — end it rather than let the node follow the cursor.
            if (ev.buttons === 0) { PA.callGraphViz._endDrag(); return; }
            PA.callGraphViz._onMouseMove(ev);
        };
        window.addEventListener('mouseup', PA.callGraphViz._winMouseUp, true);
        window.addEventListener('mousemove', PA.callGraphViz._winMouseMove, true);

        var ro = new ResizeObserver(function() {
            if (PA.callGraphViz._canvas !== canvas) return;  // stale observer from an earlier load
            var oldW = canvas.width;
            var oldH = canvas.height;
            var newW = container.clientWidth;
            var newH = container.clientHeight;
            if (newW === 0 || newH === 0) return;
            if (newW === oldW && newH === oldH) return;
            canvas.width = newW;
            canvas.height = newH;
            /**
             * §261 — this used to re-fit only when the canvas had previously been 0x0. That missed
             * the case that actually bites: the tab mounts while the container is merely *short*
             * (measured live at 1315x70 on a real tab switch, not 0), `fitAll()` runs against that
             * 70px height and produces a transform that is wrong — and, before the `fitAll()` fix
             * below, outright negative — which then persisted forever because the container growing
             * from 70px to its real 717px did not satisfy `oldH === 0`. The user was left with a
             * permanently broken view and no clue that pressing "Fit" would repair it.
             *
             * A resize large enough to matter now re-fits, which is also simply what a reader
             * expects a graph pane to do when it changes size.
             */
            if (oldW === 0 || oldH === 0 || Math.abs(newH - oldH) > 40 || Math.abs(newW - oldW) > 40) {
                PA.callGraphViz.fitAll();
            } else {
                PA.callGraphViz._draw();
            }
        });
        ro.observe(container);
        PA.callGraphViz._resizeObserver = ro;
    },

    /** §261 — shared drag termination, used by the normal mouseup and by the lost-button guard. */
    _endDrag: function() {
        PA.callGraphViz._drag = null;
        if (PA.callGraphViz._canvas) PA.callGraphViz._canvas.style.cursor = 'grab';
    },

    _onMouseLeave: function() {
        // Hover state belongs to the pointer being over the canvas; a drag deliberately survives
        // leaving it (that is the whole point of the window-level listeners above).
        if (PA.callGraphViz._drag) return;
        PA.callGraphViz._hoveredNode = null;
        PA.callGraphViz._hoveredEdge = null;
        PA.callGraphViz._updateTooltip(null);
        PA.callGraphViz._scheduleDraw();
    },

    /**
     * Largest graph the force layout will simulate.
     *
     * <p>The repulsion step is all-pairs: every node against every other, for 200 frames. That is
     * n²/2 × 200 operations — about 20,000 at 15 nodes and **6.4 billion** at 8,000, which is the
     * scale this product is aimed at. There is no partial degradation between those two numbers;
     * the tab simply stops responding.
     *
     * <p>Capping by degree keeps the part of the graph a reader is actually looking for: the
     * heavily-connected procedures are the ones worth seeing, and an arbitrary first-N slice would
     * be both slower to justify and less useful.
     */
    MAX_SIMULATED_NODES: 300,

    /**
     * UI-039 — standard OS-level double-click disambiguation window. A native browser `dblclick`
     * fires only after two full `mousedown`/`mouseup` cycles have already completed at the same
     * point, so as long as the single-click relayout is held off for at least this long, a real
     * double-click's second click always lands before the tree has moved. 220ms sits inside the
     * commonly-used 200-250ms band (Windows/macOS default double-click intervals are both well
     * above this).
     */
    CLICK_DEBOUNCE_MS: 220,

    /** Total nodes the server reported, when it sliced the graph before sending it. */
    _serverTotalNodes: 0,

    /** The most-connected nodes, which are the ones a reader is looking for in a large graph. */
    _topByDegree: function (nodes, edges, limit) {
        var degree = {};
        for (var i = 0; i < edges.length; i++) {
            var a = edges[i].source || edges[i].from;
            var b = edges[i].target || edges[i].to;
            degree[a] = (degree[a] || 0) + 1;
            degree[b] = (degree[b] || 0) + 1;
        }
        return nodes.slice()
            .sort(function (x, y) { return (degree[y.id] || 0) - (degree[x.id] || 0); })
            .slice(0, limit);
    },

    _simulate: function() {
        var nodes = PA.callGraphViz._nodes;
        var edges = PA.callGraphViz._edges;
        var nodeMap = {};
        for (var i = 0; i < nodes.length; i++) nodeMap[nodes[i].id] = nodes[i];

        var iterations = 0;
        var maxIter = 200;
        // Guard here rather than at the call site: every path into the simulation must be capped,
        // and a check the caller has to remember is a check that will be forgotten.
        if (nodes.length > PA.callGraphViz.MAX_SIMULATED_NODES) {
            // Retained as a second line of defence: the server bounds the response now, but a
            // stale deploy or a direct call to the old route can still deliver an unbounded one,
            // and the renderer must not depend on someone else having been careful.
            PA.callGraphViz._truncated = nodes.length;
            nodes = PA.callGraphViz._topByDegree(nodes, edges, PA.callGraphViz.MAX_SIMULATED_NODES);
            PA.callGraphViz._nodes = nodes;
            var kept = {};
            for (var k = 0; k < nodes.length; k++) kept[nodes[k].id] = true;
            edges = edges.filter(function (e) {
                return kept[e.source || e.from] && kept[e.target || e.to];
            });
            PA.callGraphViz._edges = edges;
            nodeMap = {};
            for (var m = 0; m < nodes.length; m++) nodeMap[nodes[m].id] = nodes[m];
        } else {
            // Not truncated locally, but the server may already have sliced it — in which case the
            // reader still needs telling, with the count the server knows and this page does not.
            PA.callGraphViz._truncated = PA.callGraphViz._serverTotalNodes || 0;
        }
        PA.callGraphViz._renderTruncationNotice();

        // §NEW (2026-09-11): replaced the all-pairs force-directed simulation (200 frames of
        // O(n^2) repulsion, see the MAX_SIMULATED_NODES comment above) with a one-shot
        // left-to-right BFS tree layout. `iterations`/`maxIter`/`alpha` above are now unused by
        // the layout itself but left in place since other code in this function still reads
        // `nodes`/`edges`/`nodeMap` the truncation step above already rebuilt.
        PA.callGraphViz._layoutTree(nodes, edges, nodeMap);
        PA.callGraphViz._draw();
        PA.callGraphViz.fitAll();
    },

    /**
     * Left-to-right BFS tree layout (org-chart rotated 90 degrees).
     *
     * Root = the currently selected node if it is part of the visible set, else the graph's real
     * entry point(s) from `_getRootIds()` (nodes with no incoming edge in the *full* edge set —
     * same definition `_isRoot`/`_getRootIds` already use elsewhere in this file), else just the
     * first visible node as a last resort so the layout never throws on an edge-case empty root
     * set.
     *
     * BFS walks the *currently visible* edge set only (the same `edges` the scope toggle /
     * expand-collapse already filtered down to) so back-edges and cross-links never cause a
     * revisit: a node is placed once, at the depth it was first discovered, via the standard
     * BFS visited-set — exactly the multi-parent handling the task calls for. Any node the BFS
     * from the primary root never reaches (a disconnected component within the visible set) gets
     * its own BFS pass appended afterwards, so every visible node ends up placed exactly once.
     *
     * Edge drawing is untouched: `_draw()` still iterates the full `_edges` list and draws a line
     * for every edge whose endpoints are visible, regardless of which one BFS treated as the
     * "discovering" parent — so a node with multiple real callers still gets a line from each of
     * them, it is just positioned once.
     */
    /**
     * §262 — the BFS pass `_layoutTree` has always run, extracted verbatim so that
     * `_collapseChains` can ask the same question the layout answers ("which column does each
     * node land in, and which node discovered it") without duplicating — and inevitably drifting
     * from — the layout's own root choice and traversal order.
     *
     * Returns `{depth, order, parent, root}`. `parent` is the BFS *discovering* edge, which is
     * the spine of the drawn tree; it is the new part, and it is what makes a chain detectable.
     * The raw call graph cannot answer this: measured live on the 69-level Postgres analysis,
     * every node in the deep tail (`erp_usp_stage_13` … `erp_usp_stage_68`) has out-degree **10**,
     * not 1 — one call to the next stage plus nine calls to shared leaf utilities that BFS already
     * placed at depth 1-2. A degree-based "linear run" test therefore finds nothing at all on the
     * exact graph this feature exists for (confirmed: 8 degree-linear nodes out of 114, none of
     * them in the tail). What is actually linear is the *layout*: 58 of its 70 columns hold
     * exactly one node, which is what stretches it to 10,350 world pixels.
     */
    _bfsDepths: function(nodes, edges, nodeMap) {
        var childAdj = {};
        for (var i = 0; i < edges.length; i++) {
            var e = edges[i];
            if (!nodeMap[e.from] || !nodeMap[e.to]) continue;
            if (!childAdj[e.from]) childAdj[e.from] = [];
            childAdj[e.from].push(e.to);
        }

        var root = null;
        if (PA.callGraphViz._selectedNode && nodeMap[PA.callGraphViz._selectedNode.id]) {
            root = PA.callGraphViz._selectedNode.id;
        } else {
            var rootIds = PA.callGraphViz._getRootIds().filter(function(id) { return nodeMap[id]; });
            root = rootIds.length ? rootIds[0] : nodes[0].id;
        }

        var depth = {};
        var order = [];
        var parent = {};
        var visited = new Set();

        function bfsFrom(startId) {
            visited.add(startId);
            depth[startId] = 0;
            var queue = [startId];
            while (queue.length) {
                var cur = queue.shift();
                order.push(cur);
                var kids = childAdj[cur] || [];
                for (var k = 0; k < kids.length; k++) {
                    var kid = kids[k];
                    if (!visited.has(kid)) {
                        visited.add(kid);
                        depth[kid] = depth[cur] + 1;
                        parent[kid] = cur;
                        queue.push(kid);
                    }
                }
            }
        }

        bfsFrom(root);
        // Disconnected-within-visible-set nodes: give each its own tree, same depth numbering,
        // so nothing is left unplaced (and _findNodeAt/_draw never see a NaN position).
        for (var n = 0; n < nodes.length; n++) {
            if (!visited.has(nodes[n].id)) bfsFrom(nodes[n].id);
        }
        return { depth: depth, order: order, parent: parent, root: root };
    },

    _layoutTree: function(nodes, edges, nodeMap) {
        if (!nodes.length) return;
        var bfs = PA.callGraphViz._bfsDepths(nodes, edges, nodeMap);
        var depth = bfs.depth;
        var order = bfs.order;

        var columns = {};
        for (var o = 0; o < order.length; o++) {
            var id = order[o];
            var d = depth[id];
            if (!columns[d]) columns[d] = [];
            columns[d].push(id);
        }

        var depths = Object.keys(columns).map(Number).sort(function(a, b) { return a - b; });

        /**
         * §261 — column spacing adapts to how deep the graph actually is.
         *
         * A flat 200px per level is fine for the 5-10 level graphs this was written against, but
         * the Postgres demo analysis is 70 levels deep: 70 x 200 = 13,800 world px against a
         * ~1,050px tall layout, a 13:1 aspect ratio that no viewport can show usefully (measured
         * live). Tightening the columns on deep graphs cuts that down without costing legibility,
         * because the floor here is set by how wide a node's *label* is, not by the node itself —
         * a typical `erp_fn_bom_component_qty`-length name is ~130px at the 10px font `_draw`
         * uses, so 150px is the point below which neighbouring labels would start colliding.
         *
         * `_layoutTree` is the only place these were used, and both are still plain constants for
         * the shallow graphs that were already laid out correctly.
         */
        var spacingX = depths.length > 12 ? 150 : 200;
        var spacingY = 70;
        for (var ci = 0; ci < depths.length; ci++) {
            var d2 = depths[ci];
            var col = columns[d2];
            var totalH = (col.length - 1) * spacingY;
            var startY = -totalH / 2;
            for (var j = 0; j < col.length; j++) {
                var node = nodeMap[col[j]];
                node.x = d2 * spacingX;
                node.y = startY + j * spacingY;
                node.vx = 0; node.vy = 0;
            }
        }
    },

    /* The canvas holds resolved colour values, not tokens, so it will not
       restyle itself when the theme flips — repaint it explicitly. MdbColor
       has already cleared its cache by the time this fires.
       Color audit follow-up: the node-detail panel (`.cgd-lvl-badge` et al, built by
       `_showNodeDetail` with an inline `style="background:" + depthColor.ring` resolved from
       `MdbColor` at render time) was NOT in that repaint list, so a panel left open across a
       theme toggle kept showing the OLD theme's depth color baked into its inline style —
       confirmed live: a stale muddy light-theme orange chip sitting right next to the
       correctly-repainted bright dark-theme tooltip on the same screen. Re-running
       `_showNodeDetail` for the currently-selected node re-resolves that color from the (now
       theme-flipped) `MdbColor` cache, same as the canvas/controls repaint above. */
    _onThemeChange: function() {
        if (PA.callGraphViz._ctx) {
            PA.callGraphViz._draw();
            PA.callGraphViz._renderControls();
        }
        if (PA.callGraphViz._selectedNode && document.getElementById('cgDetailPanel')) {
            PA.callGraphViz._showNodeDetail(PA.callGraphViz._selectedNode);
        }
    },

    /**
     * Put a banner in the DOM stating that the graph was cut down, and by how much.
     *
     * <p>Without it the layout simply shows fewer nodes than the analysis found, and a reader has
     * no way to tell a small call graph from a large one that was trimmed to fit. That is the
     * difference between a summary and a wrong answer.
     *
     * <p>A DOM node rather than something painted on the canvas: the canvas is pan/zoomed by a
     * transform, so anything drawn into it scrolls away from the viewport and can be missed
     * entirely. The notice has to stay put to do its job.
     */
    _renderTruncationNotice: function () {
        var canvas = PA.callGraphViz._canvas;
        if (!canvas || !canvas.parentNode) return;
        var el = document.getElementById('cgTruncatedNotice');
        var total = PA.callGraphViz._truncated;
        if (!total) {
            if (el) el.remove();
            return;
        }
        if (!el) {
            el = document.createElement('div');
            el.id = 'cgTruncatedNotice';
            el.className = 'cg-truncated';
            canvas.parentNode.insertBefore(el, canvas);
        }
        el.textContent = 'Graph too large to lay out: showing the '
            + PA.callGraphViz.MAX_SIMULATED_NODES + ' most-connected of ' + total
            + ' nodes. Use search, or open a procedure to see its own callers and callees.';
    },

    _draw: function() {
        var ctx = PA.callGraphViz._ctx;
        var canvas = PA.callGraphViz._canvas;
        if (!ctx || !canvas) return;
        var t = PA.callGraphViz._transform;
        var nodes = PA.callGraphViz._nodes;
        var edges = PA.callGraphViz._edges;
        var nodeMap = {};
        for (var i = 0; i < nodes.length; i++) nodeMap[nodes[i].id] = nodes[i];

        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.save();
        ctx.translate(t.x, t.y);
        ctx.scale(t.scale, t.scale);

        // §261 — focus mode (see `toggleFocus`) and search highlighting (see `search`).
        var focusSet = PA.callGraphViz._focusSet();
        var searchSet = PA.callGraphViz._searchMatches.length ? new Set(PA.callGraphViz._searchMatches) : null;
        var activeSearchId = (PA.callGraphViz._searchIdx >= 0)
            ? PA.callGraphViz._searchMatches[PA.callGraphViz._searchIdx] : null;

        /**
         * §261 — viewport culling.
         *
         * `_draw` painted every node and every edge regardless of whether it was anywhere near the
         * visible area. That is wasted work at any zoom, and it is worst in exactly the situation
         * that matters: zoomed in on one part of a large graph, where nearly everything being
         * drawn is off-screen. The world-space rectangle currently on screen is computed once here
         * and both loops skip anything outside it (with a generous margin so labels, arrowheads
         * and glows belonging to a just-off-screen node are never clipped away mid-stroke).
         */
        var margin = 120 / (t.scale || 1);
        var viewMinX = (0 - t.x) / t.scale - margin;
        var viewMaxX = (canvas.width - t.x) / t.scale + margin;
        var viewMinY = (0 - t.y) / t.scale - margin;
        var viewMaxY = (canvas.height - t.y) / t.scale + margin;
        var onScreen = function(x, y) {
            return x >= viewMinX && x <= viewMaxX && y >= viewMinY && y <= viewMaxY;
        };

        for (var i = 0; i < edges.length; i++) {
            var e = edges[i];
            var src = nodeMap[e.from];
            var tgt = nodeMap[e.to];
            if (!src || !tgt) continue;
            // Skip only when BOTH endpoints are off-screen on the same side, so a long edge
            // crossing the viewport is still drawn.
            if ((src.x < viewMinX && tgt.x < viewMinX) || (src.x > viewMaxX && tgt.x > viewMaxX) ||
                (src.y < viewMinY && tgt.y < viewMinY) || (src.y > viewMaxY && tgt.y > viewMaxY)) continue;

            var edgeFocused = !focusSet || (focusSet.has(e.from) && focusSet.has(e.to));

            var edgeDepthColor = PA.callGraphViz._getDepthColor(src.depth);
            var edgeStroke = e.backEdge ? window.MdbColor.alpha('--red', 0.5) : edgeDepthColor.ring;
            var edgeAlpha = e.backEdge ? 0.5 : 0.25;
            // Trigger edges are highlighted distinctly: FIRES_TRIGGER (routine→trigger) amber,
            // TRIGGER_FN (trigger→handler function, Postgres) purple — both dashed.
            var isTrigEdge = e.edgeType === 'FIRES_TRIGGER' || e.edgeType === 'TRIGGER_FN';
            if (e.edgeType === 'FIRES_TRIGGER') { edgeStroke = window.MdbColor.get('--badge-orange'); edgeAlpha = 0.6; }
            else if (e.edgeType === 'TRIGGER_FN') { edgeStroke = window.MdbColor.get('--badge-purple'); edgeAlpha = 0.6; }

            if (!edgeFocused) edgeAlpha *= 0.15;

            // §262 — the hovered edge is drawn at full strength and noticeably thicker, so the
            // tooltip that just appeared is unambiguously attached to a particular line. Without
            // this, in a dense fan-out it is impossible to tell *which* of several converging
            // edges the tooltip is describing.
            var isHoveredEdge = (PA.callGraphViz._hoveredEdge === e);
            if (isHoveredEdge) edgeAlpha = 1;

            ctx.beginPath();
            ctx.moveTo(src.x, src.y);
            ctx.lineTo(tgt.x, tgt.y);
            ctx.globalAlpha = edgeAlpha;
            ctx.strokeStyle = edgeStroke;
            ctx.lineWidth = Math.min(3, 0.5 + (e.count || 1) * 0.5) + (isHoveredEdge ? 2 : 0);
            if (e.backEdge || isTrigEdge) ctx.setLineDash([4, 3]);
            else ctx.setLineDash([]);
            ctx.stroke();
            ctx.globalAlpha = 1;

            var dx = tgt.x - src.x;
            var dy = tgt.y - src.y;
            var dist = Math.sqrt(dx * dx + dy * dy) || 1;
            var arrowDist = dist - tgt.radius - 4;
            if (arrowDist > 10) {
                var ax = src.x + (dx / dist) * arrowDist;
                var ay = src.y + (dy / dist) * arrowDist;
                var angle = Math.atan2(dy, dx);
                ctx.beginPath();
                ctx.moveTo(ax, ay);
                ctx.lineTo(ax - 6 * Math.cos(angle - 0.4), ay - 6 * Math.sin(angle - 0.4));
                ctx.lineTo(ax - 6 * Math.cos(angle + 0.4), ay - 6 * Math.sin(angle + 0.4));
                ctx.closePath();
                ctx.globalAlpha = (e.backEdge ? 0.7 : 0.45) * (edgeFocused ? 1 : 0.15);
                ctx.fillStyle = edgeStroke;
                ctx.fill();
                ctx.globalAlpha = 1;
            }

            /**
             * §261 — call count / call type on the edge itself.
             *
             * `count` and `callType` were already parsed off every edge in `_buildGraph` and then
             * used for nothing but line thickness, which conveys "more" but never "how many". A
             * repeated call is a materially different fact from a single one when reading a
             * dependency graph, so it is written on the edge — but only once the view is zoomed in
             * far enough for the text to be legible and sparse enough not to become clutter, and
             * only for counts above one (labelling every edge "x1" would be pure noise). Focus
             * mode suppresses the dimmed edges' labels for the same reason.
             */
            if (t.scale > 0.75 && edgeFocused && (e.count > 1 || e.edgeType === 'FIRES_TRIGGER')) {
                var mx = (src.x + tgt.x) / 2;
                var my = (src.y + tgt.y) / 2;
                if (onScreen(mx, my)) {
                    var eLabel = e.count > 1 ? ('×' + e.count) : 'trigger';
                    ctx.font = 'bold 8px sans-serif';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    var ew = ctx.measureText(eLabel).width + 6;
                    ctx.globalAlpha = 0.9;
                    ctx.fillStyle = window.MdbColor.get('--bg-card');
                    PA.callGraphViz._roundRect(ctx, mx - ew / 2, my - 5.5, ew, 11, 3);
                    ctx.fill();
                    ctx.strokeStyle = edgeStroke;
                    ctx.lineWidth = 0.6;
                    ctx.setLineDash([]);
                    ctx.stroke();
                    ctx.fillStyle = window.MdbColor.get('--text-muted');
                    ctx.fillText(eLabel, mx, my);
                    ctx.globalAlpha = 1;
                }
            }
        }
        ctx.setLineDash([]);

        var activePathSet = new Set(PA.callGraphViz._activePath);
        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            if (!onScreen(n.x, n.y)) continue;   // §261 viewport culling, see above
            var nodeFocused = !focusSet || focusSet.has(n.id);
            ctx.globalAlpha = nodeFocused ? 1 : 0.18;

            /**
             * §262 — a collapsed linear run. Drawn as a stack of offset cards rather than another
             * circle precisely so it cannot be mistaken for a procedure: it is a *count* of hidden
             * procedures, and the reader has to be able to tell at a glance that clicking it does
             * something different from clicking a node.
             */
            if (n.chain) {
                var chainHovered = (PA.callGraphViz._hoveredNode === n);
                var cw = 46, ch = 26;
                var cx0 = n.x - cw / 2, cy0 = n.y - ch / 2;
                ctx.strokeStyle = window.MdbColor.get('--text-muted');
                ctx.lineWidth = 1;
                for (var sp = 2; sp >= 1; sp--) {
                    ctx.globalAlpha = (nodeFocused ? 1 : 0.18) * (0.25 * sp);
                    ctx.fillStyle = window.MdbColor.get('--bg-card');
                    PA.callGraphViz._roundRect(ctx, cx0 + sp * 3, cy0 + sp * 3, cw, ch, 4);
                    ctx.fill();
                    ctx.stroke();
                }
                ctx.globalAlpha = nodeFocused ? 1 : 0.18;
                ctx.fillStyle = window.MdbColor.get('--bg-card');
                PA.callGraphViz._roundRect(ctx, cx0, cy0, cw, ch, 4);
                ctx.fill();
                ctx.strokeStyle = chainHovered ? window.MdbColor.get('--accent') : window.MdbColor.get('--text-muted');
                ctx.lineWidth = chainHovered ? 2 : 1.2;
                ctx.setLineDash([4, 3]);
                ctx.stroke();
                ctx.setLineDash([]);

                ctx.fillStyle = window.MdbColor.get('--accent');
                ctx.font = 'bold 11px sans-serif';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText('⋯ ' + n.members.length, n.x, n.y);

                if (t.scale > 0.4 || chainHovered) {
                    ctx.fillStyle = window.MdbColor.get('--text-muted');
                    ctx.font = '9px sans-serif';
                    ctx.textBaseline = 'alphabetic';
                    ctx.fillText('chain — click to expand', n.x, n.y + ch / 2 + 14);
                }
                ctx.globalAlpha = 1;
                continue;
            }

            var colorObj = PA.getSchemaColor(n.schema);
            var isSelected = PA.callGraphViz._selectedNode === n;
            var isHovered = PA.callGraphViz._hoveredNode === n;
            var isInPath = activePathSet.has(n.id);
            var isTerminal = PA.callGraphViz._activePath.length > 0 && PA.callGraphViz._activePath[PA.callGraphViz._activePath.length - 1] === n.id;
            var typeLabel = PA.callGraphViz._getTypeLabel(n.objectType);
            var typeColor = PA.callGraphViz._getTypeColor(typeLabel);
            var depthColor = PA.callGraphViz._getDepthColor(n.depth);
            var isRoot = PA.callGraphViz._isRoot(n);

            if (isRoot) {
                ctx.save();
                ctx.shadowColor = window.MdbColor.alpha('--orange', 0.6);
                ctx.shadowBlur = 18;
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.radius + 4, 0, Math.PI * 2);
                ctx.fillStyle = window.MdbColor.alpha('--orange', 0.12);
                ctx.fill();
                ctx.restore();
            } else if (isTerminal) {
                ctx.save();
                ctx.shadowColor = window.MdbColor.alpha('--green', 0.7);
                ctx.shadowBlur = 20;
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.radius + 5, 0, Math.PI * 2);
                ctx.fillStyle = window.MdbColor.alpha('--green', 0.18);
                ctx.fill();
                ctx.restore();
            } else if (isInPath) {
                ctx.save();
                ctx.shadowColor = window.MdbColor.alpha('--accent', 0.5);
                ctx.shadowBlur = 12;
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.radius + 3, 0, Math.PI * 2);
                ctx.fillStyle = window.MdbColor.alpha('--accent', 0.12);
                ctx.fill();
                ctx.restore();
            } else if (isHovered || isSelected) {
                ctx.save();
                ctx.shadowColor = depthColor.glow;
                ctx.shadowBlur = 10;
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.radius + 2, 0, Math.PI * 2);
                ctx.fillStyle = depthColor.glow;
                ctx.fill();
                ctx.restore();
            }

            ctx.beginPath();
            ctx.arc(n.x, n.y, n.radius, 0, Math.PI * 2);
            ctx.fillStyle = isTerminal ? window.MdbColor.get('--green') : (isSelected ? typeColor.fg : typeColor.bg);
            ctx.fill();
            ctx.strokeStyle = isRoot ? window.MdbColor.get('--orange')
                : (isTerminal ? window.MdbColor.get('--badge-green')
                : (isInPath ? window.MdbColor.get('--accent') : depthColor.ring));
            ctx.lineWidth = isRoot ? 3 : (isTerminal ? 3 : (isInPath ? 2.5 : (isHovered ? 2.5 : 1.5)));
            ctx.stroke();

            /**
             * §261 — search hit marker. Every match gets a dashed halo so the reader can see at a
             * glance how the matches are distributed through the graph; the *current* match (the
             * one Enter / the next-prev buttons have jumped to) gets a heavier solid ring so it is
             * distinguishable from its siblings without needing to read any labels.
             */
            if (searchSet && searchSet.has(n.id)) {
                var isActiveHit = (n.id === activeSearchId);
                ctx.save();
                ctx.globalAlpha = 1;   // a hit is never dimmed, even outside the focus set
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.radius + (isActiveHit ? 8 : 6), 0, Math.PI * 2);
                ctx.strokeStyle = window.MdbColor.get('--badge-purple');
                ctx.lineWidth = isActiveHit ? 3 : 1.5;
                ctx.setLineDash(isActiveHit ? [] : [3, 3]);
                if (isActiveHit) {
                    ctx.shadowColor = window.MdbColor.alpha('--badge-purple', 0.8);
                    ctx.shadowBlur = 14;
                }
                ctx.stroke();
                ctx.restore();
                ctx.setLineDash([]);
            }

            var iconR = Math.max(7, n.radius * 0.55);
            ctx.fillStyle = (isTerminal || isSelected) ? window.MdbColor.get('--accent-on') : typeColor.fg;
            ctx.font = 'bold ' + Math.max(7, iconR * 1.1) + 'px sans-serif';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(typeLabel, n.x, n.y);

            if (t.scale > 0.4 || isHovered || isSelected) {
                ctx.textBaseline = 'alphabetic';
                ctx.fillStyle = isSelected ? window.MdbColor.get('--accent-on') : colorObj.fg;
                ctx.font = (isHovered || isSelected ? 'bold ' : '') + '10px sans-serif';
                ctx.textAlign = 'center';
                ctx.fillText(n.label, n.x, n.y + n.radius + 12);

                var lvlLabel = isRoot ? 'ROOT' : 'L' + n.depth;
                var lvlW = ctx.measureText(lvlLabel).width + 6;
                var lvlX = n.x - lvlW / 2;
                var lvlY = n.y - n.radius - 8;
                ctx.fillStyle = depthColor.ring;
                PA.callGraphViz._roundRect(ctx, lvlX, lvlY - 8, lvlW, 11, 3);
                ctx.fill();
                ctx.fillStyle = window.MdbColor.get('--accent-on');
                ctx.font = 'bold 7px sans-serif';
                ctx.textBaseline = 'middle';
                ctx.fillText(lvlLabel, n.x, lvlY - 2.5);
            }
        }

        ctx.restore();
    },

    _screenToWorld: function(sx, sy) {
        var t = PA.callGraphViz._transform;
        return { x: (sx - t.x) / t.scale, y: (sy - t.y) / t.scale };
    },

    _findNodeAt: function(wx, wy) {
        var nodes = PA.callGraphViz._nodes;
        for (var i = nodes.length - 1; i >= 0; i--) {
            var dx = wx - nodes[i].x;
            var dy = wy - nodes[i].y;
            if (dx * dx + dy * dy <= nodes[i].radius * nodes[i].radius) return nodes[i];
        }
        return null;
    },

    /**
     * §262 — how close (in *screen* pixels) the cursor must come to an edge's line to hover it.
     *
     * Screen pixels, not world pixels, so the target stays the same physical size at every zoom:
     * in world units the threshold is divided by `scale` at hit-test time. 6px is the same order
     * as the widest edge this file ever strokes (`lineWidth` is capped at 3 in `_draw`), i.e. just
     * wide enough to be grabbable without letting a single cursor position sit "on" several
     * unrelated edges at once.
     */
    EDGE_HIT_PX: 6,

    /**
     * §262 — squared distance from a point to the line *segment* a→b, not to the infinite line.
     *
     * The segment form matters: edges in this layout are short spans between columns, and an
     * infinite-line test would report the cursor as touching an edge while hovering somewhere far
     * off either end of it, along the same bearing. `t` is clamped to [0,1] to project onto the
     * segment's own extent.
     */
    _distSqToSegment: function(px, py, ax, ay, bx, by) {
        var vx = bx - ax, vy = by - ay;
        var lenSq = vx * vx + vy * vy;
        var t = lenSq > 0 ? ((px - ax) * vx + (py - ay) * vy) / lenSq : 0;
        if (t < 0) t = 0; else if (t > 1) t = 1;
        var dx = px - (ax + t * vx);
        var dy = py - (ay + t * vy);
        return dx * dx + dy * dy;
    },

    /**
     * §262 — the edge whose drawn line passes nearest the given world point, or null.
     *
     * Nodes always win: this is only ever consulted after `_findNodeAt` has already returned null
     * (see `_onMouseMove`), and it additionally ignores the portion of an edge that is buried under
     * either endpoint's own disc, so the ring of pixels just outside a node — where several of its
     * edges converge — does not flicker between them.
     *
     * Returns the *closest* candidate rather than the first within threshold, because at a fan-out
     * node many edges are within 6px of each other and picking the first in array order would make
     * the hover feel arbitrary.
     */
    _findEdgeAt: function(wx, wy) {
        var v = PA.callGraphViz;
        var edges = v._edges;
        var nodes = v._nodes;
        var nodeMap = {};
        for (var i = 0; i < nodes.length; i++) nodeMap[nodes[i].id] = nodes[i];
        var scale = (v._transform && v._transform.scale) || 1;
        var thresh = v.EDGE_HIT_PX / scale;
        var threshSq = thresh * thresh;
        var best = null, bestSq = Infinity;
        for (var e = 0; e < edges.length; e++) {
            var ed = edges[e];
            var src = nodeMap[ed.from];
            var tgt = nodeMap[ed.to];
            if (!src || !tgt) continue;
            // Inside either endpoint's disc the node owns the pixel, not the edge.
            var sdx = wx - src.x, sdy = wy - src.y;
            if (sdx * sdx + sdy * sdy <= src.radius * src.radius) continue;
            var tdx = wx - tgt.x, tdy = wy - tgt.y;
            if (tdx * tdx + tdy * tdy <= tgt.radius * tgt.radius) continue;
            var dSq = v._distSqToSegment(wx, wy, src.x, src.y, tgt.x, tgt.y);
            if (dSq <= threshSq && dSq < bestSq) { bestSq = dSq; best = ed; }
        }
        return best;
    },

    _onMouseDown: function(e) {
        var rect = PA.callGraphViz._canvas.getBoundingClientRect();
        var w = PA.callGraphViz._screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
        var node = PA.callGraphViz._findNodeAt(w.x, w.y);
        if (node) {
            PA.callGraphViz._drag = { node: node, ox: node.x - w.x, oy: node.y - w.y, startX: e.clientX, startY: e.clientY, moved: false };
            // §262 — a chain placeholder is not a procedure, so it must not become the selection:
            // `_layoutTree` roots the whole tree on `_selectedNode`, and `_showNodeDetail` would
            // try to look it up in `PA.nodeIndex`. It is still draggable, and its click is handled
            // in `_onMouseUp`.
            if (!node.chain) {
                PA.callGraphViz._selectedNode = node;
                // §261 — with focus mode on, picking a node is what moves the focus; that is the
                // "click to focus on a node's dependencies" interaction, and it reuses the selection
                // the mousedown already performs rather than adding a second, competing gesture.
                if (PA.callGraphViz._focusMode) PA.callGraphViz._focusId = node.id;
            }
        } else {
            PA.callGraphViz._drag = { pan: true, sx: e.clientX, sy: e.clientY, tx: PA.callGraphViz._transform.x, ty: PA.callGraphViz._transform.y };
        }
        PA.callGraphViz._draw();
    },

    _onMouseMove: function(e) {
        var rect = PA.callGraphViz._canvas.getBoundingClientRect();
        var d = PA.callGraphViz._drag;
        if (d && d.node) {
            var dx = e.clientX - d.startX;
            var dy = e.clientY - d.startY;
            if (Math.abs(dx) > 3 || Math.abs(dy) > 3) d.moved = true;
            var w = PA.callGraphViz._screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
            d.node.x = w.x + d.ox;
            d.node.y = w.y + d.oy;
            PA.callGraphViz._scheduleDraw();
        } else if (d && d.pan) {
            PA.callGraphViz._transform.x = d.tx + (e.clientX - d.sx);
            PA.callGraphViz._transform.y = d.ty + (e.clientY - d.sy);
            PA.callGraphViz._scheduleDraw();
        } else {
            var w = PA.callGraphViz._screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
            var node = PA.callGraphViz._findNodeAt(w.x, w.y);
            // §262 — an edge is only considered once the cursor is over no node at all, so the
            // existing node-hover behaviour is bit-for-bit unchanged whenever a node is under the
            // pointer and edges can never steal a node's tooltip.
            var edge = node ? null : PA.callGraphViz._findEdgeAt(w.x, w.y);
            var nodeChanged = (node !== PA.callGraphViz._hoveredNode);
            var edgeChanged = (edge !== PA.callGraphViz._hoveredEdge);
            if (nodeChanged || edgeChanged) {
                PA.callGraphViz._hoveredNode = node;
                PA.callGraphViz._hoveredEdge = edge;
                PA.callGraphViz._canvas.style.cursor = (node || edge) ? 'pointer' : 'grab';
                if (node) PA.callGraphViz._updateTooltip(node, e.clientX, e.clientY);
                else if (edge) PA.callGraphViz._updateEdgeTooltip(edge, e.clientX, e.clientY);
                else PA.callGraphViz._hideTooltip();
                PA.callGraphViz._scheduleDraw();
            } else if (node) {
                // Same node, pointer moved: keep the tooltip with the cursor. Previously the
                // tooltip was positioned once on entry and then sat still while the pointer moved
                // on across the node, which reads as a stuck artifact rather than a hover hint.
                PA.callGraphViz._updateTooltip(node, e.clientX, e.clientY);
            } else if (edge) {
                PA.callGraphViz._updateEdgeTooltip(edge, e.clientX, e.clientY);
            }
        }
    },

    /**
     * UI-039 — the relayout-triggering part of a single click (`expandNode()`/`_recordVisit()`/
     * `_showNodeDetail()`) is debounced here instead of run immediately: it is deferred by
     * `CLICK_DEBOUNCE_MS`, giving a genuine second click (`_onDblClick`, which fires only after
     * this same mouseup has already run once) time to arrive and cancel it before the tree moves
     * out from under it (§245 Bug 2). Immediate feedback for the click itself is NOT delayed —
     * `_onMouseDown` already sets `_selectedNode`/redraws synchronously on every mousedown
     * (single or double), giving the existing selection-highlight treatment with no relayout, so
     * the UI never looks unresponsive during the wait.
     */
    _onMouseUp: async function() {
        var d = PA.callGraphViz._drag;
        if (d && d.node && !d.moved) {
            var node = d.node;
            if (PA.callGraphViz._pendingClick) {
                clearTimeout(PA.callGraphViz._pendingClick.timer);
            }
            PA.callGraphViz._pendingClick = {
                node: node,
                timer: setTimeout(function() {
                    PA.callGraphViz._pendingClick = null;
                    // §262 — clicking a collapsed chain opens that chain; it is not a node
                    // navigation, so it records no history and opens no detail panel.
                    if (node.chain) { PA.callGraphViz.toggleChain(node.id); return; }
                    // §260: capture the FROM state before `expandNode` moves `_expandedNode` —
                    // see `_reRootTo`'s doc for why the from-state, not the destination, is recorded.
                    var fromNode = PA.callGraphViz._expandedNode;
                    PA.callGraphViz.expandNode(node.id);
                    // UI-037: single-click select is a real node-to-node travel step, exactly like
                    // the source modal's `_history.push` on every `openModal` — record it so Back
                    // can return here.
                    PA.callGraphViz._recordVisit(fromNode);
                    PA.callGraphViz._showNodeDetail(node);
                }, PA.callGraphViz.CLICK_DEBOUNCE_MS)
            };
        }
        PA.callGraphViz._endDrag();
    },

    /**
     * §261 — coalesce repaints onto one animation frame.
     *
     * `_draw()` is a full repaint: every visible edge and every visible node, re-resolving theme
     * colours per element. The drag and pan handlers called it straight from `mousemove`, which
     * fires far faster than the display refreshes, so a drag on the 114-node / 847-edge Postgres
     * analysis was doing several full repaints per frame and throwing most of them away. Routing
     * the interactive paths through here caps that at one repaint per frame. `_draw()` itself is
     * unchanged and still available for the places that must paint synchronously (`fitAll`, the
     * theme repaint, tests).
     */
    _scheduleDraw: function() {
        var v = PA.callGraphViz;
        if (v._drawFrame) return;
        v._drawFrame = requestAnimationFrame(function() {
            v._drawFrame = null;
            v._draw();
        });
    },

    /**
     * UI-037 — the source modal's `_history`/`_historyFwd` pattern (`sourceView.js` ~lines 36-160),
     * copied verbatim: every navigation unconditionally pushes the node just visited and clears
     * the forward stack (`openModal`'s `_history.push(...)` + `_historyFwd = []`, unconditional on
     * every call including the ones `back()`/`forward()` themselves trigger) — so going back and
     * then forward past a fresh navigation loses the rest of the forward trail, exactly like the
     * source modal does. Copied, not reinvented.
     */
    _recordVisit: function(nodeId) {
        PA.callGraphViz._history.push(nodeId);
        PA.callGraphViz._historyFwd = [];
        PA.callGraphViz._updateHistoryButtons();
    },

    _updateHistoryButtons: function() {
        var backBtn = document.getElementById('cgBackBtn');
        var fwdBtn = document.getElementById('cgFwdBtn');
        if (backBtn) backBtn.disabled = PA.callGraphViz._history.length === 0;
        if (fwdBtn) fwdBtn.disabled = PA.callGraphViz._historyFwd.length === 0;
    },

    /**
     * §260 fix — `_history` used to record the DESTINATION of every re-root, never the node being
     * left. That meant the very first re-root from the full graph (nothing pushed yet for the full
     * view itself) always left `_history.length === 1` right after it — permanently indistinguishable
     * from "no history at all", so `cgBackBtn` stayed disabled even though the user clearly had
     * somewhere to go back to. Fixed by recording the FROM state (an entry of `null` means "the
     * full/collapsed view", matching `_expandedNode`'s own null-means-full convention) instead of the
     * TO state, at every real navigation entry point (`_onDblClick`, single-click select, breadcrumb
     * click) — `back()`/`forward()` themselves now call `_reRootTo`/`_reRootToFull` with
     * `skipHistory=true` and manage the two stacks explicitly so replay never re-records itself.
     */
    back: function() {
        if (!PA.callGraphViz._history.length) return;
        var prev = PA.callGraphViz._history.pop();
        PA.callGraphViz._historyFwd.push(PA.callGraphViz._expandedNode);
        if (prev === null) {
            PA.callGraphViz._reRootToFull(true);
        } else {
            PA.callGraphViz._reRootTo(prev, true);
        }
        PA.callGraphViz._updateHistoryButtons();
    },

    /** Mirrors `PA.sourceModal.forward()` — see `back()`'s doc for the from/to recording model. */
    forward: function() {
        if (!PA.callGraphViz._historyFwd.length) return;
        var next = PA.callGraphViz._historyFwd.pop();
        PA.callGraphViz._history.push(PA.callGraphViz._expandedNode);
        if (next === null) {
            PA.callGraphViz._reRootToFull(true);
        } else {
            PA.callGraphViz._reRootTo(next, true);
        }
        PA.callGraphViz._updateHistoryButtons();
    },

    /**
     * Hard re-root to `nodeId`: visible set becomes exactly this node plus its own direct
     * children — the same state `_onDblClick` already builds (§241) — now shared by `_onDblClick`
     * itself, `back()` and `forward()` so all three "re-root to a specific node" entry points stay
     * identical.
     *
     * §260 correctness fix: this used to end with an unconditional `applyScope()` call (UI-038's
     * "keep scope in sync with the new root"). Verified live that `applyScope()` derives its
     * "current node" from `PA._currentScopeId()` — the PAGE-level procedure the whole right panel
     * is showing, not the node just re-rooted to — so with any scope other than nothing selected
     * (the default is `'direct'`, always truthy) it immediately overwrote the `vis`/`_expandedNode`
     * this function had just set, silently undoing every double-click re-root. Confirmed via
     * `page.evaluate` calling `_reRootTo` directly: `_expandedNode` came back `null` every time.
     * The initial full-graph load never called `applyScope()` at all (scope only ever applied via
     * an explicit Direct/Subtree/SubFlow/Full toolbar click, `app.js`'s `onScopeChange`), so
     * removing this call here restores that same, actually-working contract instead of the
     * silently-broken one — genuine re-root and the toolbar's own scope filtering are independent.
     *
     * @param skipHistory §260 — true when called by `back()`/`forward()` replaying an existing
     *   stack entry; those manage `_history`/`_historyFwd` themselves, so the normal from-state
     *   recording below must be skipped or every Back would immediately push a new Forward-only
     *   dead entry and corrupt the stacks.
     */
    _reRootTo: function(nodeId, skipHistory) {
        var node = PA.callGraphViz._allNodes.filter(function(n) { return n.id === nodeId; })[0];
        if (!node) return;
        var fromNode = PA.callGraphViz._expandedNode;
        var childMap = PA.callGraphViz._childMap || {};
        var vis = new Set([nodeId]);
        var children = childMap[nodeId] || [];
        for (var i = 0; i < children.length; i++) vis.add(children[i]);

        PA.callGraphViz._selectedNode = node;
        PA.callGraphViz._activePath = [nodeId];
        PA.callGraphViz._expandedNode = nodeId;
        PA.callGraphViz._visibleIds = vis;
        PA.callGraphViz._applyVisibility();
        PA.callGraphViz._updateControlsCount();
        if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
        PA.callGraphViz._simulate();
        if (!skipHistory) PA.callGraphViz._recordVisit(fromNode);
        PA.callGraphViz._showNodeDetail(node);
    },

    /** §260 — the "full view" counterpart of `_reRootTo`, needed so `back()`/`forward()` can
     *  replay a `null` (full-view) history entry through the same render pipeline `showAll()` uses,
     *  without `showAll()`'s own unconditional history-clearing (`showAll` is a user "start over"
     *  action; navigating back TO full view is not). See `_reRootTo`'s doc for why the
     *  `applyScope()` re-sync was removed rather than skipped-by-flag. */
    _reRootToFull: function(skipHistory) {
        var fromNode = PA.callGraphViz._expandedNode;
        PA.callGraphViz._visibleIds = null;
        PA.callGraphViz._expandedNode = null;
        PA.callGraphViz._activePath = [];
        PA.callGraphViz._selectedNode = null;
        PA.callGraphViz._applyVisibility();
        PA.callGraphViz._updateControlsCount();
        if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
        PA.callGraphViz._simulate();
        if (!skipHistory) PA.callGraphViz._recordVisit(fromNode);
        PA.callGraphViz._closeNodeDetail();
    },

    _onWheel: function(e) {
        e.preventDefault();
        var factor = e.deltaY > 0 ? 0.9 : 1.1;
        var t = PA.callGraphViz._transform;
        var rect = PA.callGraphViz._canvas.getBoundingClientRect();
        var mx = e.clientX - rect.left;
        var my = e.clientY - rect.top;
        t.x = mx - (mx - t.x) * factor;
        t.y = my - (my - t.y) * factor;
        t.scale *= factor;
        t.scale = Math.max(0.1, Math.min(5, t.scale));
        PA.callGraphViz._draw();
    },

    /**
     * §237's tree layout roots itself off `_selectedNode` (see `_layoutTree`) whenever that node
     * is part of the currently visible set — but nothing before this fix ever discarded the
     * `_activePath` built up by earlier single-clicks ("traveling" through the tree via
     * `expandNode`'s path-continuation logic). Double-click used to call `PA.showProcedure(node.id)`
     * instead, which navigates away to the procedure-detail screen entirely and never touches the
     * graph's own root/path state at all — so on any return to the Graph tab the stale path from
     * whatever nodes were single-clicked earlier was still sitting in `_activePath`/`_expandedNode`,
     * which is exactly the "child node only with last parent we select while travelling" bug
     * report: double-clicking a node did not make *that* node the root with *its own* children,
     * it left the previous traveled-to parent's context in place (or, worse, left the Graph tab
     * altogether).
     *
     * Fixed by re-rooting in place, reusing the exact mechanism §237 already built for
     * selection-driven re-rooting: set `_selectedNode` (which `_layoutTree` already treats as the
     * root when visible) and reset `_activePath`/`_expandedNode` to start fresh at the
     * double-clicked node — the same "new branch" shape `expandNode` uses for an unrelated node,
     * but without walking back through the real graph root first, so no stale prior parent survives.
     * Visible set becomes exactly this node plus its own direct children (its own `_showNodeDetail`
     * panel still opens, matching the existing single-click behavior).
     */
    _onDblClick: function(e) {
        var node;
        // UI-039 — a real browser `dblclick` fires only after both `mousedown`/`mouseup` cycles
        // have already completed, i.e. after `_onMouseUp` has already armed `_pendingClick` once
        // for this exact gesture. Use the node identified by that FIRST click, not a fresh
        // hit-test at the second click's coordinates: hit-testing again here is exactly what was
        // broken before this fix (a relayout that already ran, or even one racing to complete
        // between the two clicks, moves the node out from under a coordinate-based re-test).
        if (PA.callGraphViz._pendingClick) {
            clearTimeout(PA.callGraphViz._pendingClick.timer);
            node = PA.callGraphViz._pendingClick.node;
            PA.callGraphViz._pendingClick = null;
        } else {
            // No pending single-click state — e.g. a dblclick synthesized without a matching prior
            // mouseup. Fall back to a fresh hit-test, the pre-fix behavior, so a dblclick never
            // silently does nothing just because this file's own bookkeeping missed a step.
            var rect = PA.callGraphViz._canvas.getBoundingClientRect();
            var w = PA.callGraphViz._screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
            node = PA.callGraphViz._findNodeAt(w.x, w.y);
        }
        if (!node) return;
        // §262 — double-clicking a chain placeholder expands it, same as a single click. There is
        // no "re-root to a chain": `_reRootTo` resolves its argument against `_allNodes`, which
        // has no entry for a placeholder, so without this the gesture would silently do nothing.
        if (node.chain) { PA.callGraphViz.toggleChain(node.id); return; }
        // UI-037/UI-038: shared with `back()`/`forward()` — same re-root state, same history
        // push, same scope resync. See `_reRootTo`.
        PA.callGraphViz._reRootTo(node.id);
    },

    /**
     * §262 — the one place that positions and shows `#graphTooltip`.
     *
     * Extracted from `_updateTooltip` unchanged so the new edge tooltip (`_updateEdgeTooltip`)
     * renders through exactly the same element, the same `.graph-tooltip` styling and the same
     * cursor-following placement rather than growing a second, near-identical tooltip system.
     */
    _showTooltip: function(html, cx, cy) {
        var tip = document.getElementById('graphTooltip');
        if (!tip) return;
        tip.style.display = '';
        tip.style.left = (cx + 12) + 'px';
        tip.style.top = (cy - 10) + 'px';
        tip.innerHTML = html;
    },

    _hideTooltip: function() {
        var tip = document.getElementById('graphTooltip');
        if (tip) tip.style.display = 'none';
    },

    _updateTooltip: function(node, cx, cy) {
        var tip = document.getElementById('graphTooltip');
        if (!tip) return;
        if (!node) { tip.style.display = 'none'; return; }
        // §262 — a collapsed chain placeholder is not a real procedure; it has no schema, depth
        // stats or index entry, so it gets its own summary instead of the per-node one below.
        if (node.chain) { PA.callGraphViz._updateChainTooltip(node, cx, cy); return; }

        tip.style.display = '';
        tip.style.left = (cx + 12) + 'px';
        tip.style.top = (cy - 10) + 'px';
        var depthC = PA.callGraphViz._getDepthColor(node.depth);
        var isRt = PA.callGraphViz._isRoot(node);
        var lvl = isRt ? 'ROOT' : 'L' + node.depth;
        tip.innerHTML = '<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">' +
            '<span style="background:' + depthC.ring + ';color:var(--text-inverse);padding:1px 6px;border-radius:var(--radius-sm);font-size:9px;font-weight:700">' + lvl + '</span>' +
            '<strong>' + PA.esc(node.label) + '</strong></div>' +
            (node.schema ? '<span style="color:' + PA.getSchemaColor(node.schema).fg + '">Schema: ' + PA.esc(node.schema) + '</span><br>' : '') +
            (node.objectType ? 'Type: ' + PA.esc(node.objectType) + '<br>' : '') +
            'Depth: ' + node.depth +
            (node.loc > 0 ? ' | LOC: ' + node.loc : '') +
            '<br>In: ' + node.inEdges + ' | Out: ' + node.outEdges +
            PA.callGraphViz._tooltipDataSection(node);
    },

    /**
     * §261 — what this procedure actually *does*, shown on hover.
     *
     * A node on the canvas can only carry a name, a one-letter type glyph and a depth badge before
     * it stops being readable, so everything else belongs in the tooltip. All of it is already
     * resident and needs no fetch: `_buildGraph` preloads `PA.nodeIndex` before a single node is
     * drawn, so this is an O(1) `byId` lookup on the hover path, which deliberately stays
     * synchronous (the click path, `_showNodeDetail`, is the one allowed to await).
     *
     * Read against the index's real shape rather than an assumed one — verified live that an index
     * node carries `callCount` plus a `counts` object (`tables`, `callsOut`, `internalCalls`,
     * `externalCalls`, `cursors`, `exceptionHandlers`, `sequences`, `subtreeTablesCount`,
     * `inCycle`, and a `statements` map of statement type → occurrences), and that triggers
     * additionally carry `triggerTiming`/`triggerEvent`/`triggerTable`. An earlier draft of this
     * read `tablesAccessed`/`joins`, which the index does not populate at all — every one of the
     * 114 nodes in the Postgres analysis had zero — so it would have rendered nothing, forever,
     * while looking correct in review.
     */
    _MAX_TOOLTIP_STMTS: 6,

    _tooltipDataSection: function(node) {
        if (!PA.nodeIndex || !PA.nodeIndex.byId) return '';
        var idx = PA.nodeIndex.byId(node.id);
        if (!idx) return '';
        var c = idx.counts || {};
        var out = '';

        // Statement mix — the closest thing the index has to "what operations does this run",
        // and the most informative single line available without a per-node fetch.
        var stmts = c.statements && typeof c.statements === 'object' ? c.statements : null;
        if (stmts) {
            var keys = Object.keys(stmts).sort(function(a, b) { return stmts[b] - stmts[a]; });
            if (keys.length) {
                out += '<div class="cg-tip-row"><span class="cg-tip-label">Stmts</span>';
                for (var i = 0; i < keys.length && i < PA.callGraphViz._MAX_TOOLTIP_STMTS; i++) {
                    out += '<span class="cg-tip-op">' + PA.esc(keys[i])
                         + (stmts[keys[i]] > 1 ? ' ' + stmts[keys[i]] : '') + '</span>';
                }
                if (keys.length > PA.callGraphViz._MAX_TOOLTIP_STMTS) {
                    out += '<span class="cg-tip-op">+' + (keys.length - PA.callGraphViz._MAX_TOOLTIP_STMTS) + '</span>';
                }
                out += '</div>';
            }
        }

        var facts = [];
        if (c.tables) facts.push(c.tables + ' table' + (c.tables === 1 ? '' : 's'));
        if (c.subtreeTablesCount && c.subtreeTablesCount !== c.tables) facts.push(c.subtreeTablesCount + ' in subtree');
        if (c.cursors) facts.push(c.cursors + ' cursor' + (c.cursors === 1 ? '' : 's'));
        if (c.sequences) facts.push(c.sequences + ' sequence' + (c.sequences === 1 ? '' : 's'));
        if (c.exceptionHandlers) facts.push(c.exceptionHandlers + ' handler' + (c.exceptionHandlers === 1 ? '' : 's'));
        if (facts.length) {
            out += '<div class="cg-tip-row"><span class="cg-tip-label">Uses</span>'
                 + '<span class="cg-tip-tables">' + PA.esc(facts.join(' · ')) + '</span></div>';
        }

        var calls = [];
        if (idx.callCount) calls.push(idx.callCount.toLocaleString() + ' calls');
        if (c.internalCalls) calls.push(c.internalCalls + ' internal');
        if (c.externalCalls) calls.push(c.externalCalls + ' external');
        if (calls.length) {
            out += '<div class="cg-tip-row"><span class="cg-tip-label">Calls</span>'
                 + '<span class="cg-tip-tables">' + PA.esc(calls.join(' · ')) + '</span></div>';
        }

        // A trigger's firing condition is the single most useful thing about it, and the graph
        // otherwise shows it as an indistinguishable "T" circle.
        if (idx.triggerTable || idx.triggerEvent) {
            out += '<div class="cg-tip-row"><span class="cg-tip-label">Fires</span>'
                 + '<span class="cg-tip-tables">'
                 + PA.esc([idx.triggerTiming, idx.triggerEvent].filter(Boolean).join(' '))
                 + (idx.triggerTable ? ' on ' + PA.esc(idx.triggerTable) : '')
                 + '</span></div>';
        }

        if (c.inCycle) {
            out += '<div class="cg-tip-row"><span class="cg-tip-label" style="color:var(--red)">In cycle</span>'
                 + '<span class="cg-tip-tables">this procedure participates in a call cycle</span></div>';
        }
        return out;
    },

    /**
     * §262 — hovering an EDGE, as distinct from hovering a node.
     *
     * The graph already had node tooltips and it already drew a `×N` count label near an edge at
     * high zoom, but the edge itself — which procedure calls which, how, how often, from which
     * source lines — could not be interrogated at all: the count label is only drawn above
     * `scale > 0.75` and only when `count > 1`, so for the overwhelming majority of edges there was
     * nothing to read anywhere. Everything shown here was already parsed off the edge in
     * `_buildGraph` and, before this, used for nothing but line thickness and dash pattern.
     *
     * Rendered through `_showTooltip` into the same `#graphTooltip` element with the same
     * `.cg-tip-row`/`.cg-tip-label` rows the node tooltip uses, so there is one tooltip system.
     */
    _updateEdgeTooltip: function(edge, cx, cy) {
        var v = PA.callGraphViz;
        if (!edge) { v._hideTooltip(); return; }
        var esc = PA.esc;
        var nodeMap = {};
        for (var i = 0; i < v._nodes.length; i++) nodeMap[v._nodes[i].id] = v._nodes[i];
        var src = nodeMap[edge.from];
        var tgt = nodeMap[edge.to];
        var srcLabel = (src && src.label) || edge.from;
        var tgtLabel = (tgt && tgt.label) || edge.to;

        // The edge's own colour, matching exactly how `_draw` strokes it, so the tooltip is
        // visibly attached to the line under the cursor rather than being a generic popup.
        var stroke = edge.backEdge ? window.MdbColor.get('--red')
            : (edge.edgeType === 'FIRES_TRIGGER' ? window.MdbColor.get('--badge-orange')
            : (edge.edgeType === 'TRIGGER_FN' ? window.MdbColor.get('--badge-purple')
            : v._getDepthColor(src ? src.depth : 0).ring));

        var kind = v._edgeKindLabel(edge);
        var html = '<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">'
            + '<span style="background:' + stroke + ';color:var(--text-inverse);padding:1px 6px;'
            + 'border-radius:var(--radius-sm);font-size:9px;font-weight:700">' + esc(kind) + '</span>'
            + '<strong>call edge</strong></div>';

        html += '<div class="cg-tip-edge-ends">'
            + '<span class="cg-tip-end">' + esc(srcLabel) + '</span>'
            + '<span class="cg-tip-arrow">&#8594;</span>'
            + '<span class="cg-tip-end">' + esc(tgtLabel) + '</span>'
            + '</div>';

        var facts = [];
        facts.push((edge.count || 1) + ' call' + ((edge.count || 1) === 1 ? '' : 's'));
        if (edge.callType) facts.push(edge.callType);
        if (edge.edgeType && edge.edgeType !== edge.callType) facts.push(edge.edgeType);
        if (edge.backEdge) facts.push('back edge (cycle)');
        html += '<div class="cg-tip-row"><span class="cg-tip-label">Call</span>'
             + '<span class="cg-tip-tables">' + esc(facts.join(' · ')) + '</span></div>';

        if (edge.condition) {
            html += '<div class="cg-tip-row"><span class="cg-tip-label">When</span>'
                 + '<span class="cg-tip-tables">' + esc(String(edge.condition)) + '</span></div>';
        }

        // Call-site line numbers: the single most actionable thing about an edge, because it is
        // what a reader needs in order to go and look at the call in the source.
        if (edge.lines && edge.lines.length) {
            var shown = edge.lines.slice(0, v._MAX_TOOLTIP_STMTS);
            html += '<div class="cg-tip-row"><span class="cg-tip-label">Lines</span>'
                 + '<span class="cg-tip-tables">' + esc(shown.join(', '))
                 + (edge.lines.length > shown.length ? ' +' + (edge.lines.length - shown.length) : '')
                 + '</span></div>';
        }

        if (edge.minDepth != null) {
            html += '<div class="cg-tip-row"><span class="cg-tip-label">Depth</span>'
                 + '<span class="cg-tip-tables">first reached at L' + esc(String(edge.minDepth)) + '</span></div>';
        }

        v._showTooltip(html, cx, cy);
    },

    /**
     * §262 — hover summary for a collapsed chain placeholder.
     *
     * Names the procedures it is standing in for, so the run can be read without expanding it at
     * all — which is usually the question ("what's down there?") rather than "show me all 60".
     */
    _updateChainTooltip: function(node, cx, cy) {
        var v = PA.callGraphViz;
        var esc = PA.esc;
        var members = node.members || [];
        var byId = {};
        for (var i = 0; i < v._allNodes.length; i++) byId[v._allNodes[i].id] = v._allNodes[i];

        var html = '<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">'
            + '<span style="background:var(--accent);color:var(--text-inverse);padding:1px 6px;'
            + 'border-radius:var(--radius-sm);font-size:9px;font-weight:700">CHAIN</span>'
            + '<strong>' + members.length + ' procedures in a straight line</strong></div>';

        var shown = Math.min(members.length, v._MAX_TOOLTIP_STMTS);
        html += '<div class="cg-tip-chain-list">';
        for (var m = 0; m < shown; m++) {
            var mn = byId[members[m]];
            html += '<span>' + (m + 1) + '. ' + esc((mn && mn.label) || members[m]) + '</span>';
        }
        if (members.length > shown) {
            // Always name the last one too: the end of a deep chain is what the reader is
            // actually trying to find out.
            var lastN = byId[members[members.length - 1]];
            html += '<span>… +' + (members.length - shown - 1) + ' more</span>';
            html += '<span>' + members.length + '. ' + esc((lastN && lastN.label) || members[members.length - 1]) + '</span>';
        }
        html += '</div>';
        html += '<div class="cg-tip-row"><span class="cg-tip-label">Depths</span>'
             + '<span class="cg-tip-tables">collapsed ' + members.length + ' levels — click to expand</span></div>';

        v._showTooltip(html, cx, cy);
    },

    /** Short human name for what kind of edge this is — the badge at the top of the edge tooltip. */
    _edgeKindLabel: function(edge) {
        if (edge.edgeType === 'FIRES_TRIGGER') return 'TRIGGER';
        if (edge.edgeType === 'TRIGGER_FN') return 'TRIGGER FN';
        if (edge.backEdge) return 'CYCLE';
        if (edge.callType) return String(edge.callType).toUpperCase();
        if (edge.edgeType) return String(edge.edgeType).toUpperCase();
        return 'CALLS';
    },

    _getTypeLabel: function(objectType) {
        var t = (objectType || '').toUpperCase();
        if (t === 'FUNCTION') return 'F';
        if (t === 'TRIGGER') return 'T';
        if (t.indexOf('PACKAGE') !== -1) return 'PKG';
        return 'P';
    },

    /* Canvas needs real colour strings — ctx.fillStyle silently ignores
       var(--token) — so these resolve through MdbColor, which re-reads the
       computed values whenever the theme flips. */
    _getTypeColor: function(typeLabel) {
        var C = window.MdbColor;
        var map = {
            'P':   { bg: C.get('--badge-blue-bg'),   fg: C.get('--badge-blue') },
            'F':   { bg: C.get('--badge-green-bg'),  fg: C.get('--badge-green') },
            'T':   { bg: C.get('--badge-orange-bg'), fg: C.get('--badge-orange') },
            'PKG': { bg: C.get('--accent-subtle'),   fg: C.get('--accent') }
        };
        return map[typeLabel] || map['P'];
    },

    _getDepthColor: function(depth) {
        var i = Math.min(depth, 7);
        var glowAlpha = [0.35, 0.25, 0.2, 0.2, 0.2, 0.15, 0.15, 0.15][i];
        return {
            ring: window.MdbColor.cat(i),
            glow: window.MdbColor.alpha('--cat-' + (i + 1), glowAlpha)
        };
    },

    _isRoot: function(node) {
        return node.depth === 0 && node.inEdges === 0;
    },

    _renderControls: function() {
        var ctrl = document.getElementById('graphControls');
        if (!ctrl) return;
        var html = '';
        // UI-037 — Back/Forward node-travel history, styled/markup-matched to the existing
        // control buttons on this same bar (`btn btn-sm`, `data-tip`), `disabled` state driven by
        // stack length exactly like the source modal's `cmBack`/`cmFwd` buttons.
        html += '<button class="btn btn-sm" id="cgBackBtn" onclick="PA.callGraphViz.back()" data-tip="Back to previous node"' + (PA.callGraphViz._history.length === 0 ? ' disabled' : '') + '>&#8592;</button>';
        html += '<button class="btn btn-sm" id="cgFwdBtn" onclick="PA.callGraphViz.forward()" data-tip="Forward"' + (PA.callGraphViz._historyFwd.length === 0 ? ' disabled' : '') + '>&#8594;</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.zoomIn()" data-tip="Zoom in">+</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.zoomOut()" data-tip="Zoom out">&minus;</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.fitAll()" data-tip="Fit graph to screen">Fit</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.resetLayout()" data-tip="Re-layout nodes">Reset</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.showAll()" data-tip="Show all nodes">All</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.collapseAll()" data-tip="Collapse to roots only">Collapse</button>';
        // §261 — focus mode: dim everything except the selected node's direct callers/callees.
        html += '<button class="btn btn-sm' + (PA.callGraphViz._focusMode ? ' cg-btn-on' : '') + '" id="cgFocusBtn"'
             + ' onclick="PA.callGraphViz.toggleFocus()"'
             + ' data-tip="Focus: dim everything except the selected node\'s direct callers and callees">Focus</button>';
        // §262 — collapse long unbranching call chains into one placeholder each. On by default;
        // it does nothing at all to a graph with no run of `MIN_CHAIN_RUN`+ linear nodes.
        html += '<button class="btn btn-sm' + (PA.callGraphViz._chainMode ? ' cg-btn-on' : '') + '" id="cgChainBtn"'
             + ' onclick="PA.callGraphViz.toggleChainMode()"'
             + ' data-tip="Chains: collapse long unbranching call chains into a single expandable marker">Chains</button>';
        html += '<button class="btn btn-sm" id="cgFullscreenBtn" onclick="PA.callGraphFullscreen.toggle()" data-tip="Toggle fullscreen">&#x26F6;</button>';
        /**
         * §261 — the search this tab's own "graph too large, use search" notice already told
         * readers to use. Matches the whole loaded graph, not just what is on screen, and reveals
         * (expanding the tree if needed) then centres on each hit.
         */
        html += '<span class="cg-search">';
        html += '<input type="text" id="cgSearchInput" class="cg-search-input" placeholder="Find node…"'
             + ' value="' + PA.esc(PA.callGraphViz._searchTerm || '') + '"'
             + ' oninput="PA.callGraphViz._onSearchInput(this)"'
             + ' onkeydown="PA.callGraphViz._onSearchKey(this, event)"'
             + ' data-tip="Find a procedure by name — Enter for the next match, Shift+Enter for the previous">';
        html += '<span class="cg-search-status" id="cgSearchStatus"></span>';
        html += '<button class="btn btn-sm cg-search-nav" onclick="PA.callGraphViz.searchStep(-1)" data-tip="Previous match">&#8249;</button>';
        html += '<button class="btn btn-sm cg-search-nav" onclick="PA.callGraphViz.searchStep(1)" data-tip="Next match">&#8250;</button>';
        html += '</span>';
        html += '<span class="cg-legend">';
        html += '<span class="cg-legend-item" style="color:var(--orange)" data-tip="Root entry point"><span class="cg-legend-dot" style="background:var(--badge-orange-bg);border-color:var(--orange);box-shadow:0 0 6px var(--badge-orange-bg)">&#9733;</span></span>';
        html += '<span class="cg-legend-item" style="color:var(--blue)" data-tip="Procedure node"><span class="cg-legend-dot" style="background:var(--badge-blue-bg);border-color:var(--blue)">P</span></span>';
        html += '<span class="cg-legend-item" style="color:var(--badge-green)" data-tip="Function node"><span class="cg-legend-dot" style="background:var(--badge-green-bg);border-color:var(--badge-green)">F</span></span>';
        html += '<span class="cg-legend-item" style="color:var(--badge-orange)" data-tip="Trigger node"><span class="cg-legend-dot" style="background:var(--badge-orange-bg);border-color:var(--badge-orange)">T</span></span>';
        html += '<span class="cg-legend-item" style="color:var(--accent)" data-tip="Package node"><span class="cg-legend-dot" style="background:var(--accent-subtle);border-color:var(--accent)">PKG</span></span>';
        html += '</span>';
        html += '<span class="cg-depth-legend">';
        var depthLabels = ['L0', 'L1', 'L2', 'L3', 'L4+'];
        var depthPalette = [0,1,2,3,4].map(function(i){ return window.MdbColor.cat(i); });
        for (var dl = 0; dl < depthLabels.length; dl++) {
            html += '<span class="cg-depth-tag" style="background:' + depthPalette[dl] + '" data-tip="Depth level ' + dl + '">' + depthLabels[dl] + '</span>';
        }
        html += '</span>';
        html += '<span style="font-size:10px;color:var(--text-muted);margin-left:8px">' + PA.callGraphViz._nodes.length + ' nodes, ' + PA.callGraphViz._edges.length + ' edges</span>';
        ctrl.innerHTML = html;
        // The bar is rebuilt wholesale (theme flips, focus toggles), so the match counter has to
        // be repainted from state rather than left as the empty span the markup above starts with.
        PA.callGraphViz._updateSearchStatus();
        PA.callGraphFullscreen.init();
    },

    zoomIn: function() {
        if (!PA.callGraphViz._canvas) return;
        var t = PA.callGraphViz._transform;
        var cx = PA.callGraphViz._canvas.width / 2;
        var cy = PA.callGraphViz._canvas.height / 2;
        t.x = cx - (cx - t.x) * 1.2;
        t.y = cy - (cy - t.y) * 1.2;
        t.scale *= 1.2;
        PA.callGraphViz._draw();
    },

    zoomOut: function() {
        if (!PA.callGraphViz._canvas) return;
        var t = PA.callGraphViz._transform;
        var cx = PA.callGraphViz._canvas.width / 2;
        var cy = PA.callGraphViz._canvas.height / 2;
        t.x = cx - (cx - t.x) * 0.8;
        t.y = cy - (cy - t.y) * 0.8;
        t.scale *= 0.8;
        PA.callGraphViz._draw();
    },

    /**
     * Smallest zoom `fitAll` will ever choose.
     *
     * Below roughly this level the view stops being a graph and becomes noise: `_draw()` only
     * renders node labels and level badges when `scale > 0.4` (see `_draw`), and a node's 8-24px
     * radius shrinks under two screen pixels. Measured live on the 114-node/70-level Postgres
     * analysis, unclamped `fitAll` chose **0.0866** — a 1.04px node radius with every label
     * suppressed, which rendered the whole analysis as a single unreadable streak across the pane.
     *
     * Showing a readable part of a graph and letting the reader pan, search and focus is strictly
     * more useful than showing all of it at a size where nothing can be identified.
     */
    MIN_FIT_SCALE: 0.45,

    /**
     * Fit the visible graph into the canvas.
     *
     * §261 — two genuine defects fixed here, both reproduced live:
     *
     * 1. **Negative scale.** `Math.min(scaleX, scaleY, 2)` with `padding = 60` makes `scaleY`
     *    negative the moment the canvas is shorter than `2 * padding = 120px`, and `Math.min` then
     *    happily selects it. A real tab switch mounts this canvas at 1315x70 before the flex
     *    layout settles, and `_simulate()` calls `fitAll()` right then — measured scale
     *    **-0.0476**, i.e. the graph drawn mirrored through the origin and 20x too small. The
     *    padding is now clamped to what the canvas can actually spare, and the final scale is
     *    floored, so a negative or non-finite scale is unreachable.
     *
     * 2. **Illegibly small scale** on a legitimately large graph — see `MIN_FIT_SCALE`. When the
     *    graph cannot fit at that floor the view is anchored on the graph's entry point (its root)
     *    rather than on the bounding box centre, so the reader starts at the meaningful end of the
     *    tree instead of in the middle of nowhere.
     */
    fitAll: function() {
        if (!PA.callGraphViz._canvas) return;
        var nodes = PA.callGraphViz._nodes;
        if (!nodes.length) return;
        var minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            if (!isFinite(n.x) || !isFinite(n.y)) continue;
            var r = n.radius || 0;
            if (n.x - r < minX) minX = n.x - r;
            if (n.x + r > maxX) maxX = n.x + r;
            if (n.y - r < minY) minY = n.y - r;
            if (n.y + r > maxY) maxY = n.y + r;
        }
        if (!isFinite(minX) || !isFinite(minY)) return;   // nothing placeable to fit

        var w = PA.callGraphViz._canvas.width;
        var h = PA.callGraphViz._canvas.height;
        if (!(w > 0) || !(h > 0)) return;

        // Never ask for more padding than the canvas can give: on a 70px-tall canvas the old fixed
        // 60px padding left -50px of usable height, which is where the negative scale came from.
        var padding = Math.min(60, w * 0.1, h * 0.1);
        var availW = Math.max(1, w - padding * 2);
        var availH = Math.max(1, h - padding * 2);

        var spanX = (maxX - minX) || 1;
        var spanY = (maxY - minY) || 1;
        var scale = Math.min(availW / spanX, availH / spanY, 2);
        if (!isFinite(scale) || scale <= 0) scale = 1;

        var fitsWhole = scale >= PA.callGraphViz.MIN_FIT_SCALE;
        scale = Math.max(scale, PA.callGraphViz.MIN_FIT_SCALE);

        var cx, cy;
        if (fitsWhole) {
            cx = (minX + maxX) / 2;
            cy = (minY + maxY) / 2;
        } else {
            // Too big to show legibly all at once — anchor on the entry point instead of the
            // bounding-box centre, and sit it near the left edge since the layout runs rightwards.
            var rootIds = PA.callGraphViz._getRootIds();
            var anchor = null;
            for (var k = 0; k < nodes.length && !anchor; k++) {
                if (rootIds.indexOf(nodes[k].id) >= 0) anchor = nodes[k];
            }
            if (!anchor) anchor = nodes[0];
            cx = anchor.x + (availW / scale) / 2 - 40;
            cy = anchor.y;
        }
        PA.callGraphViz._transform = { x: w / 2 - cx * scale, y: h / 2 - cy * scale, scale: scale };
        PA.callGraphViz._overviewOnly = !fitsWhole;
        PA.callGraphViz._draw();
    },

    /* =====================================================================================
     * §261 — Search, focus and centering.
     *
     * The truncation notice this file already renders ends with "Use search, or open a procedure
     * to see its own callers and callees" — but no search existed anywhere on this tab, so the one
     * instruction given to a reader looking at a graph too large to read pointed at nothing. That
     * is what these add.
     * ===================================================================================== */

    /** Ids currently matching the search box, in layout order; `_searchIdx` points at the active one. */
    _searchMatches: [],
    _searchIdx: -1,
    _searchTerm: '',

    /** When set, everything except this node and its direct callers/callees is drawn dimmed. */
    _focusId: null,
    _focusMode: false,

    /**
     * Centre the view on a node without changing the zoom, unless the current zoom is below the
     * readable floor (in which case jumping to a node is also the moment to make it legible).
     */
    centerOn: function(node, minScale) {
        var v = PA.callGraphViz;
        if (!v._canvas || !node) return;
        var t = v._transform;
        var scale = t.scale;
        var floor = minScale || v.MIN_FIT_SCALE;
        if (scale < floor) scale = floor;
        v._transform = {
            x: v._canvas.width / 2 - node.x * scale,
            y: v._canvas.height / 2 - node.y * scale,
            scale: scale
        };
        v._draw();
    },

    /**
     * Make `nodeId` visible if the current expand/collapse state is hiding it, then centre on it.
     *
     * Reuses `expandNode`, which already knows how to build a root→node path and a visible set
     * that keeps every ancestor's siblings clickable — rather than inventing a second, subtly
     * different notion of "reveal".
     */
    revealNode: function(nodeId) {
        var v = PA.callGraphViz;
        var node = null;
        for (var i = 0; i < v._allNodes.length; i++) {
            if (v._allNodes[i].id === nodeId) { node = v._allNodes[i]; break; }
        }
        if (!node) return null;
        // §262 — a node swallowed by a collapsed chain is not in `_nodes` and has stale
        // coordinates, so opening its chain has to happen before anything tries to centre on it.
        v._expandChainContaining(nodeId);
        for (var c = 0; c < v._nodes.length; c++) {
            if (v._nodes[c].id === nodeId) { node = v._nodes[c]; break; }
        }
        var visible = v._nodes.indexOf(node) >= 0;
        if (!visible) {
            v.expandNode(nodeId);
            // `expandNode` re-lays-out and re-fits; re-resolve the node object's new position.
            for (var j = 0; j < v._nodes.length; j++) {
                if (v._nodes[j].id === nodeId) { node = v._nodes[j]; break; }
            }
        }
        v.centerOn(node);
        return node;
    },

    /**
     * Match against the *whole* loaded graph, not just what is currently on screen: the entire
     * point of searching a collapsed or truncated graph is to find something you cannot see.
     * Matching is a case-insensitive substring over the node label and its schema-qualified id.
     */
    search: function(term) {
        var v = PA.callGraphViz;
        v._searchTerm = term || '';
        var q = v._searchTerm.trim().toUpperCase();
        if (!q) {
            v._searchMatches = [];
            v._searchIdx = -1;
            v._updateSearchStatus();
            v._draw();
            return 0;
        }
        var matches = [];
        for (var i = 0; i < v._allNodes.length; i++) {
            var n = v._allNodes[i];
            if ((n.label || '').toUpperCase().indexOf(q) >= 0 ||
                (n.id || '').toUpperCase().indexOf(q) >= 0) {
                matches.push(n.id);
            }
        }
        v._searchMatches = matches;
        v._searchIdx = matches.length ? 0 : -1;
        v._updateSearchStatus();
        v._draw();
        return matches.length;
    },

    /** Jump to the next match, wrapping. `dir` -1 steps backwards. */
    searchStep: function(dir) {
        var v = PA.callGraphViz;
        if (!v._searchMatches.length) return;
        var n = v._searchMatches.length;
        v._searchIdx = ((v._searchIdx + (dir || 1)) % n + n) % n;
        v._updateSearchStatus();
        v.revealNode(v._searchMatches[v._searchIdx]);
    },

    /** Called by the search box's Enter key and the next/prev buttons. */
    _onSearchInput: function(el) {
        var v = PA.callGraphViz;
        var count = v.search(el.value);
        if (count > 0) v.revealNode(v._searchMatches[0]);
    },

    _onSearchKey: function(el, ev) {
        if (ev.key === 'Enter') {
            ev.preventDefault();
            PA.callGraphViz.searchStep(ev.shiftKey ? -1 : 1);
        } else if (ev.key === 'Escape') {
            el.value = '';
            PA.callGraphViz.search('');
        }
    },

    _updateSearchStatus: function() {
        var el = document.getElementById('cgSearchStatus');
        if (!el) return;
        var v = PA.callGraphViz;
        if (!v._searchTerm.trim()) { el.textContent = ''; return; }
        el.textContent = v._searchMatches.length
            ? (v._searchIdx + 1) + '/' + v._searchMatches.length
            : '0/0';
    },

    /**
     * Focus mode: dim everything except the selected node and the nodes directly wired to it.
     *
     * Deliberately one hop in each direction (callers and callees), which is the question a reader
     * actually has in front of a dense graph — "what touches this?" — and is the only depth that
     * stays readable when a node has dozens of edges. Nothing is removed from the graph, only
     * de-emphasised, so the surrounding structure is still there for context and one click on a
     * dimmed node moves the focus to it.
     */
    toggleFocus: function() {
        var v = PA.callGraphViz;
        v._focusMode = !v._focusMode;
        if (v._focusMode) {
            v._focusId = (v._selectedNode && v._selectedNode.id)
                || v._expandedNode
                || (v._nodes.length ? v._nodes[0].id : null);
        } else {
            v._focusId = null;
        }
        v._renderControls();
        v._draw();
    },

    /** Ids drawn at full strength while focus mode is on; null when focus mode is off. */
    _focusSet: function() {
        var v = PA.callGraphViz;
        if (!v._focusMode || !v._focusId) return null;
        var set = new Set([v._focusId]);
        var kids = (v._childMap || {})[v._focusId] || [];
        for (var i = 0; i < kids.length; i++) set.add(kids[i]);
        var parents = (v._parentsOf || {})[v._focusId] || [];
        for (var j = 0; j < parents.length; j++) set.add(parents[j]);
        return set;
    },

    _roundRect: function(ctx, x, y, w, h, r) {
        ctx.beginPath();
        ctx.moveTo(x + r, y);
        ctx.lineTo(x + w - r, y);
        ctx.quadraticCurveTo(x + w, y, x + w, y + r);
        ctx.lineTo(x + w, y + h - r);
        ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
        ctx.lineTo(x + r, y + h);
        ctx.quadraticCurveTo(x, y + h, x, y + h - r);
        ctx.lineTo(x, y + r);
        ctx.quadraticCurveTo(x, y, x + r, y);
        ctx.closePath();
    },

    resetLayout: function() {
        if (!PA.callGraphViz._canvas) return;
        var nodes = PA.callGraphViz._nodes;
        for (var i = 0; i < nodes.length; i++) {
            nodes[i].x = Math.random() * 600 - 300;
            nodes[i].y = Math.random() * 400 - 200;
            nodes[i].vx = 0; nodes[i].vy = 0;
        }
        if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
        PA.callGraphViz._simulate();
    },

    /* ===== Node Detail Panel ===== */
    // UI-024 (§163) — async now: this is a click handler (`_onMouseUp`), not the hover path
    // (`_onMouseMove` only ever touches the geometric `_nodes` array, never `PA.nodeIndex` — the
    // "instant hover" conflict §131 flagged never actually applied to this file; measured, not
    // assumed). A click already tolerates a brief await the way every other detail-panel open in
    // this app does.
    _showNodeDetail: async function(node) {
        await PA.nodeIndex.ensureLoaded();
        PA.callGraphViz._closeNodeDetail();
        var container = document.getElementById('graphContainer');
        if (!container) return;

        var path = PA.callGraphViz._activePath;
        var esc = PA.esc;
        var depthColor = PA.callGraphViz._getDepthColor(node.depth);
        var isRt = PA.callGraphViz._isRoot(node);
        var lvlLabel = isRt ? 'ROOT' : 'L' + node.depth;
        var typeLabel = PA.callGraphViz._getTypeLabel(node.objectType);
        var schemaColor = PA.getSchemaColor(node.schema);

        // Path breadcrumb
        var pathHtml = '';
        if (path.length > 0) {
            pathHtml = '<div class="cgd-path">';
            for (var i = 0; i < path.length; i++) {
                if (i > 0) pathHtml += '<span class="cgd-path-arrow">›</span>';
                var nodeInPath = PA.callGraphViz._allNodes.find(function(n) { return n.id === path[i]; });
                var isLast = (i === path.length - 1);
                // UI-036: every earlier breadcrumb node re-runs `expandNode` on that node's own id
                // — the exact "clicked a node already in the path" truncate branch `expandNode`
                // already has (see `expandNode` above), reused verbatim. The current (last) node
                // stays inert — clicking "here" has nothing to do.
                var pathNodeAttrs = isLast ? '' : ' onclick="PA.callGraphViz._breadcrumbClick(\'' + esc(path[i]).replace(/'/g, "\\'") + '\')" data-tip="Jump to this node"';
                pathHtml += '<span class="cgd-path-node' + (isLast ? ' cgd-path-current' : '') + '"' + pathNodeAttrs + '>' + esc(nodeInPath ? nodeInPath.label : path[i]) + '</span>';
            }
            pathHtml += '</div>';
        }

        // Try to get node data from analysisData for operations/joins/tables
        var indexNode = null;
        {
            // Indexed rather than scanned; the graph resolves a node per drawn element.
            indexNode = PA.nodeIndex.byId(node.id) || PA.nodeIndex.byName(node.id);
        }

        // Operations from index node
        var opsHtml = '';
        if (indexNode) {
            var ops = PA._collectOperations ? PA._collectOperations(indexNode) : [];
            if (ops && ops.length) {
                opsHtml = '<div class="cgd-section-title">Operations</div><div class="cgd-ops">';
                for (var oi = 0; oi < ops.length; oi++) {
                    opsHtml += '<span class="dh-op-badge ' + esc(ops[oi]) + '">' + esc(ops[oi]) + '</span>';
                }
                opsHtml += '</div>';
            }
        }

        // Tables accessed from index node
        var tablesHtml = '';
        if (indexNode && indexNode.tablesAccessed && indexNode.tablesAccessed.length) {
            tablesHtml = '<div class="cgd-section-title">Tables Accessed (' + indexNode.tablesAccessed.length + ')</div>';
            tablesHtml += '<table class="cgd-table"><thead><tr><th>Table</th><th>Ops</th></tr></thead><tbody>';
            for (var ti = 0; ti < indexNode.tablesAccessed.length; ti++) {
                var t = indexNode.tablesAccessed[ti];
                var tName = typeof t === 'string' ? t : (t.tableName || t.name || '');
                var tOps = typeof t === 'object' && t.operations ? t.operations.join(', ') : '';
                tablesHtml += '<tr><td>' + esc(tName) + '</td><td>' + esc(tOps) + '</td></tr>';
            }
            tablesHtml += '</tbody></table>';
        }

        // Joins from index node
        var joinsHtml = '';
        if (indexNode && indexNode.joins && indexNode.joins.length) {
            joinsHtml = '<div class="cgd-section-title">Joins (' + indexNode.joins.length + ')</div>';
            joinsHtml += '<table class="cgd-table"><thead><tr><th>Type</th><th>Tables</th></tr></thead><tbody>';
            for (var ji = 0; ji < indexNode.joins.length; ji++) {
                var jn = indexNode.joins[ji];
                var jType = jn.joinType || jn.type || '';
                var jTables = jn.tables ? jn.tables.join(' ⟷ ') : (jn.leftTable && jn.rightTable ? jn.leftTable + ' ⟷ ' + jn.rightTable : '');
                joinsHtml += '<tr><td>' + esc(jType) + '</td><td>' + esc(jTables) + '</td></tr>';
            }
            joinsHtml += '</tbody></table>';
        }

        // Children count info
        var childMap = PA.callGraphViz._childMap || {};
        var childCount = (childMap[node.id] || []).length;
        var childInfo = childCount > 0 ? '<span class="cgd-chip">' + childCount + ' child' + (childCount !== 1 ? 'ren' : '') + '</span>' : '';

        var html = '<div class="cgd-panel" id="cgDetailPanel">';
        html += '<div class="cgd-header">';
        html += '<div class="cgd-title-row">';
        html += '<span class="cgd-lvl-badge" style="background:' + depthColor.ring + '">' + esc(lvlLabel) + '</span>';
        html += '<span class="cgd-type-badge">' + esc(typeLabel) + '</span>';
        if (node.schema) html += '<span class="cgd-schema" style="color:' + schemaColor.fg + ';background:' + schemaColor.bg + '">' + esc(node.schema) + '</span>';
        html += '<span class="cgd-name">' + esc(node.label) + '</span>';
        html += '</div>';
        html += '<button class="cgd-close" onclick="PA.callGraphViz._closeNodeDetail()" title="Close">&#10005;</button>';
        html += '</div>';

        if (pathHtml) html += pathHtml;

        html += '<div class="cgd-chips">';
        html += '<span class="cgd-chip">In: ' + node.inEdges + '</span>';
        html += '<span class="cgd-chip">Out: ' + node.outEdges + '</span>';
        if (node.loc > 0) html += '<span class="cgd-chip">' + node.loc + ' LOC</span>';
        html += childInfo;
        html += '</div>';

        if (opsHtml || tablesHtml || joinsHtml) {
            html += '<div class="cgd-body">';
            html += opsHtml;
            html += tablesHtml;
            html += joinsHtml;
            if (!opsHtml && !tablesHtml && !joinsHtml) html += '<p class="cgd-muted">Loading details...</p>';
            html += '</div>';
        } else {
            // Async load if index node has no inline data
            var detailId = 'cgd-async-' + Date.now();
            html += '<div class="cgd-body" id="' + detailId + '">';
            html += '<p class="cgd-muted">Loading details...</p>';
            html += '</div>';
            // Kick off async detail load
            setTimeout(function() { PA.callGraphViz._loadNodeDetailAsync(detailId, node); }, 50);
        }

        html += '<div class="cgd-footer">';
        html += '<button class="btn btn-sm" onclick="PA.showProcedure(\'' + esc(node.id).replace(/'/g, "\\'") + '\')" title="Open full detail view">Open Detail</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.collapseAll()" title="Reset to roots">Clear</button>';
        html += '</div>';
        html += '</div>';

        container.insertAdjacentHTML('beforeend', html);
    },

    /**
     * UI-036 — a breadcrumb node is always already in `_activePath`, so `expandNode`'s own
     * "clicked a node already in the path — truncate to that point" branch (see `expandNode`
     * above) is exactly the logic this needs; called here verbatim, no new truncation logic.
     * UI-037: records the jump in the back/forward history, matching every other re-root entry
     * point. §260: the `applyScope()` re-sync this used to end with is removed — see `_reRootTo`'s
     * doc — it derived "current node" from the page-level procedure, not `nodeId`, so it silently
     * undid the truncation this function exists to perform.
     */
    _breadcrumbClick: function(nodeId) {
        var node = PA.callGraphViz._allNodes.filter(function(n) { return n.id === nodeId; })[0];
        if (!node) return;
        // §260: record the FROM state (matching `_reRootTo`/single-click), not the destination.
        var fromNode = PA.callGraphViz._expandedNode;
        PA.callGraphViz.expandNode(nodeId);
        PA.callGraphViz._recordVisit(fromNode);
        PA.callGraphViz._showNodeDetail(node);
    },

    _closeNodeDetail: function() {
        var el = document.getElementById('cgDetailPanel');
        if (el) el.remove();
    },

    _loadNodeDetailAsync: async function(containerId, node) {
        var el = document.getElementById(containerId);
        if (!el) return;
        var esc = PA.esc;
        var html = '';

        try {
            // Try fetching via nodeFile from analysisData
            var indexNode = null;
            {
                // Indexed rather than scanned; the graph resolves a node per drawn element.
                // Already loaded by _showNodeDetail's own await before this was scheduled, but
                // ensureLoaded() is idempotent/cheap on a hit, so awaiting again here is cheap
                // insurance against a future caller of this method that skips that step.
                await PA.nodeIndex.ensureLoaded();
                indexNode = PA.nodeIndex.byId(node.id) || PA.nodeIndex.byName(node.id);
            }

            var detail = null;
            if (indexNode && indexNode.nodeFile) {
                detail = await PA.api.getNodeDetail(indexNode.nodeFile);
            }

            if (detail) {
                var ops = PA._collectOperations ? PA._collectOperations(detail) : [];
                if (ops && ops.length) {
                    html += '<div class="cgd-section-title">Operations</div><div class="cgd-ops">';
                    for (var oi = 0; oi < ops.length; oi++) {
                        html += '<span class="dh-op-badge ' + esc(ops[oi]) + '">' + esc(ops[oi]) + '</span>';
                    }
                    html += '</div>';
                }

                // Tables
                var tables = detail.tablesAccessed || detail.tableOperations || [];
                if (tables.length) {
                    html += '<div class="cgd-section-title">Tables Accessed (' + tables.length + ')</div>';
                    html += '<table class="cgd-table"><thead><tr><th>Table</th><th>Ops</th></tr></thead><tbody>';
                    for (var ti = 0; ti < tables.length; ti++) {
                        var t = tables[ti];
                        var tName = typeof t === 'string' ? t : (t.tableName || t.name || '');
                        var tOps = typeof t === 'object' && t.operations ? (Array.isArray(t.operations) ? t.operations.join(', ') : Object.keys(t.operations).join(', ')) : '';
                        html += '<tr><td>' + esc(tName) + '</td><td>' + esc(tOps) + '</td></tr>';
                    }
                    html += '</tbody></table>';
                }

                // Joins
                var joins = detail.joins || [];
                if (joins.length) {
                    html += '<div class="cgd-section-title">Joins (' + joins.length + ')</div>';
                    html += '<table class="cgd-table"><thead><tr><th>Type</th><th>Tables</th></tr></thead><tbody>';
                    for (var ji = 0; ji < joins.length; ji++) {
                        var jn = joins[ji];
                        var jType = jn.joinType || jn.type || '';
                        var jTables = jn.tables ? jn.tables.join(' ⟷ ') : (jn.leftTable && jn.rightTable ? jn.leftTable + ' ⟷ ' + jn.rightTable : '');
                        html += '<tr><td>' + esc(jType) + '</td><td>' + esc(jTables) + '</td></tr>';
                    }
                    html += '</tbody></table>';
                }

                if (!html) html = '<p class="cgd-muted">No detailed data available</p>';
            } else {
                html = '<p class="cgd-muted">No detail data available</p>';
            }
        } catch (e) {
            html = '<p class="cgd-muted">Could not load details</p>';
        }

        if (el) el.innerHTML = html;
    }
};

/* Repaint the graph when the MongoDB theme switches between light and dark. */
document.addEventListener("themechange", function () {
    if (window.PA && PA.callGraphViz) PA.callGraphViz._onThemeChange();
});
