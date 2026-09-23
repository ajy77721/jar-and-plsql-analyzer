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

PA.callTrace = {
    /** Levels of indentation before it stops growing — see renderNode. */
    _MAX_INDENT_LEVELS: 12,

    /**
     * How many levels are built into the DOM up front.
     *
     * <p>This page used to render the WHOLE tree eagerly — every node, at every depth, into one
     * HTML string. On the real trace measured here (1,250 nodes, depth 71) that is the entire
     * structure materialised before anything appears, which is the "still loading, nothing comes"
     * the user reported. The parser page has always loaded a level at a time; this one had not, and
     * the two renderers had simply drifted.
     */
    _initialDepth: 1,

    /** step -> node, so a collapsed branch can be built when it is first opened. */
    _nodeMap: {},

    /** The tree as delivered, so expandAll can count what has not been rendered. */
    _root: null,

    // _indexNodes(node) was here — UI-016, matching the parser page.
    //
    // Its doc comment gave the objection to removing it: "Numbering during render would renumber
    // every time something expanded." True of unconditional numbering, and answered by assigning a
    // step only when a node does not already have one, in renderNode. A re-rendered node keeps the
    // step its DOM ids were built with.
    //
    // The old code also reset stepCounter to 0 immediately after the walk, while renderNode had a
    // fallback that numbered from the counter — so any node reaching that fallback would have been
    // given a step already in use, and opening it would have found the wrong subtree. It was
    // unreachable, because both the walk and the renderer skip the children of a `circular` node,
    // but it stopped being defensive the moment those two rules disagreed. Lazy numbering removes
    // the need for the fallback and the reset together.

    /**
     * Nodes in the whole tree, including ones never rendered.
     *
     * <p>Iterative, not recursive: traces reach depth 71 here and a malformed cycle should report
     * something rather than blow the stack. `circular` nodes are counted and not followed, the same
     * rule the renderer uses.
     */
    _countNodes(root) {
        if (!root) return 0;
        let n = 0;
        const stack = [root];
        while (stack.length) {
            const node = stack.pop();
            n++;
            if (node.children && !node.circular) {
                for (const child of node.children) stack.push(child);
            }
        }
        return n;
    },

    treeData: null,
    breadcrumbStack: [],  // [{id, name}]
    stepCounter: 0,
    _gen: 0,              // stale-request guard; see the note above load()

    /*
     * Stale-response guard (§B5). All three entry points below write the SAME module state —
     * treeData and breadcrumbStack — and repaint the singleton #ctContainer. Out-of-order
     * responses therefore leave the breadcrumb describing one procedure and the tree showing
     * another, which reads as correct and is not.
     *
     * The parser twin of this file has carried a `_gen` counter for exactly this since it was
     * fixed; this PL/SQL copy never got it. The two files are otherwise near-identical, which is
     * how the omission survived — a fix applied to one of a duplicated pair.
     *
     * Note drillDown/goTo mutate breadcrumbStack, so the check has to come before that mutation
     * as well as before the render.
     */

    /** Load call tree for a procedure and render it */
    async load(procId) {
        const isCurrent = UIBusy.seq(PA.callTrace, '_gen');
        const data = await PA.api.getCallTree(procId);
        if (!isCurrent()) return;
        if (!data || !data.id) {
            document.getElementById('ctContainer').innerHTML = '<div class="empty-msg">No call tree data for: ' + PA.esc(procId) + '</div>';
            return;
        }
        PA.callTrace.treeData = data;
        PA.callTrace.breadcrumbStack = [{ id: data.id, name: data.name || data.id }];
        PA.callTrace.render(data);
    },

    /** Drill down into a child node */
    async drillDown(procId) {
        const isCurrent = UIBusy.seq(PA.callTrace, '_gen');
        const data = await PA.api.getCallTree(procId);
        if (!isCurrent()) return;
        if (!data || !data.id) return;
        PA.callTrace.treeData = data;
        PA.callTrace.breadcrumbStack.push({ id: data.id, name: data.name || data.id });
        PA.callTrace.render(data);
    },

    /** Go to a specific breadcrumb position */
    async goTo(index) {
        if (index < 0) return;
        const isCurrent = UIBusy.seq(PA.callTrace, '_gen');
        const item = PA.callTrace.breadcrumbStack[index];
        if (!item) return;
        const data = await PA.api.getCallTree(item.id);
        // Truncate the breadcrumb only once this response is known to be the winning one —
        // truncating up front discarded entries even when the response was then thrown away.
        if (!isCurrent()) return;
        if (data) {
            // The slice belongs INSIDE this branch. Outside it, a null response shortened the
            // breadcrumb while the deep tree stayed on screen — the trail then claimed you were two
            // levels down while you were looking at seven. The parser twin has always had it here.
            PA.callTrace.breadcrumbStack = PA.callTrace.breadcrumbStack.slice(0, index + 1);
            PA.callTrace.treeData = data;
            PA.callTrace.render(data);
        }
    },

    /** Go back to root (first in breadcrumb) */
    goRoot() {
        if (PA.callTrace.breadcrumbStack.length > 0) {
            PA.callTrace.goTo(0);
        }
    },

    /** Render the call tree as depth-indented HTML */
    render(data) {
        PA.callTrace.updateBreadcrumb();
        PA.callTrace.stepCounter = 0;
        const searchBox = document.getElementById('exploreSearch');
        if (searchBox) searchBox.value = '';
        const container = document.getElementById('ctContainer');

        if (!data || !data.id) {
            container.innerHTML = '<div class="empty-msg">Select a procedure from the left panel</div>';
            return;
        }

        PA.callTrace._nodeMap = {};
        PA.callTrace.stepCounter = 0;
        PA.callTrace._root = data;

        let html = '';
        html += PA.callTrace.renderNode(data, 0, true);
        container.innerHTML = html;
    },

    /** Render a single node and its children recursively */
    renderNode(node, depth, expanded) {
        // Assigned on first render and never again, so DOM ids stay stable across a re-render.
        if (node._step == null) {
            node._step = ++PA.callTrace.stepCounter;
            PA.callTrace._nodeMap[node._step] = node;
        }
        const step = node._step;
        // Capped: real traces reach depth 71 here, which at 20px a level is 1,420px of leading
        // whitespace and pushes the procedure name outside the panel entirely. Past the cap the
        // depth badge carries the level instead.
        const indent = Math.min(depth, PA.callTrace._MAX_INDENT_LEVELS) * 20;
        // No extra marker is needed past the cap: every row already carries an L<depth> badge
        // below, so flattening the indent loses the visual nesting but not the depth itself.
        const hasChildren = node.children && node.children.length > 0;
        const id = PA.escAttr(node.id || '');
        const name = node.name || node.id || '?';
        const schema = node.schemaName || '';
        const pkg = node.packageName || '';
        const fullName = [schema, pkg, name].filter(Boolean).join('.');
        const callType = node.callType || '';
        const lineNum = node.callLineNumber || node.startLine || 0;
        const endLine = node.endLine || 0;
        // sourceFile: use stored value, or construct from schema + package
        const sourceFile = node.sourceFile || (schema && pkg ? schema + '.' + pkg : '');
        const isCircular = node.circular;
        const colorObj = getSchemaColor(schema);

        let html = '';

        // Main row
        html += `<div class="ct-row" data-id="${id}" data-depth="${depth}" id="ct-${step}" title="Click: show details | Double-click: drill down into this procedure" onclick="PA.callTrace.onRowClick('${PA.escJs(node.id)}', event)" ondblclick="PA.callTrace.onRowDblClick('${PA.escJs(node.id)}', event)">`;
        html += `<div class="ct-indent" style="width:${indent}px"></div>`;

        // Toggle
        if (hasChildren && !isCircular) {
            html += `<span class="ct-toggle" data-step="${step}" onclick="PA.callTrace.toggle(${step}, event)">&#9660;</span>`;
        } else {
            html += `<span class="ct-toggle" style="visibility:hidden">&#9660;</span>`;
        }

        // Depth level badge (same level = same number)
        html += `<span class="ct-depth-badge" title="Depth level ${depth}">L${depth}</span>`;

        // Step number
        html += `<span class="ct-step" style="color:${colorObj.fg}">${step}</span>`;

        // Schema badge
        if (schema) {
            html += `<span class="ct-schema-badge" style="background:${colorObj.bg};color:${colorObj.fg}">${PA.esc(schema)}</span>`;
        }

        // Placeholder/encrypted indicator
        if (node.placeholder) {
            html += `<span class="ct-lock" title="Source not available — wrapped/encrypted or not in analyzed sources">&#128274;</span>`;
        }

        // Procedure name
        html += `<span class="ct-name" title="Click to open source code for ${PA.escAttr(fullName)}" onclick="PA.callTrace.onNameClick('${PA.escJs(node.id)}', event)">${PA.esc(fullName)}</span>`;

        // Line number (show range if endLine available)
        if (lineNum > 0) {
            const lineLabel = (endLine > 0 && endLine !== lineNum) ? `L${lineNum}-${endLine}` : `L${lineNum}`;
            html += `<span class="ct-line" title="Click to open source at line ${lineNum}" onclick="PA.callTrace.onLineClick('${PA.escJs(sourceFile)}', ${lineNum}, event)">${lineLabel}</span>`;
        }

        // Call type badge
        const callTypeTooltips = {
            'INTERNAL': 'Same package/schema — call stays within the analyzed package',
            'EXTERNAL': 'Cross-package call — goes to a different package or schema',
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

        html += `</div>`;

        // Children — built now only within the initial depth; deeper branches are built when
        // opened. Rendering all of them cost the whole tree before the first row appeared.
        if (hasChildren && !isCircular) {
            const renderNow = depth < PA.callTrace._initialDepth;
            if (renderNow) {
                html += `<div class="ct-children" id="ct-children-${step}" data-loaded="1" data-depth="${depth}">`;
                for (const child of node.children) {
                    html += PA.callTrace.renderNode(child, depth + 1, true);
                }
                html += `</div>`;
            } else {
                html += `<div class="ct-children" id="ct-children-${step}" data-loaded="0" data-depth="${depth}" style="display:none"></div>`;
            }
        }

        return html;
    },

    /** Toggle expand/collapse */
    toggle(step, event) {
        event.stopPropagation();
        const container = document.getElementById('ct-children-' + step);
        const icon = document.querySelector(`.ct-toggle[data-step="${step}"]`);
        if (!container) return;
        // Build this branch the first time it is opened. Everything below the initial depth is an
        // empty container until now, which is what keeps the first paint bounded.
        PA.callTrace._renderChildrenLazy(step);
        if (container.style.display === 'none') {
            container.style.display = '';
            if (icon) icon.innerHTML = '&#9660;';
        } else {
            container.style.display = 'none';
            if (icon) icon.innerHTML = '&#9654;';
        }
    },

    /**
     * Builds every branch not yet in the DOM.
     *
     * <p>Shared by search and Expand All. Loops rather than taking a single snapshot, because
     * building a branch inserts children carrying their own unbuilt containers.
     */
    _materialiseAll() {
        for (let pass = 0; pass < 200; pass++) {
            const pending = document.querySelectorAll('.ct-children[data-loaded="0"]');
            if (pending.length === 0) return;
            pending.forEach(el => {
                PA.callTrace._renderChildrenLazy(parseInt(el.id.replace('ct-children-', ''), 10));
            });
        }
    },

    /** Materialises one collapsed branch. No-op once it has been built. */
    _renderChildrenLazy(step) {
        const container = document.getElementById('ct-children-' + step);
        if (!container || container.dataset.loaded === '1') return;
        const node = PA.callTrace._nodeMap[step];
        if (!node || !node.children) { container.dataset.loaded = '1'; return; }
        const depth = parseInt(container.dataset.depth || '0', 10);
        let html = '';
        for (const child of node.children) {
            html += PA.callTrace.renderNode(child, depth + 1, true);
        }
        container.innerHTML = html;
        container.dataset.loaded = '1';
    },

    /**
     * Expands every level, not just the containers currently in the document.
     *
     * <p>Expanding a branch inserts its children, which carry their own unbuilt containers. A single
     * querySelectorAll snapshot would miss those, so one click would open one more level and the
     * button would look broken — the same defect fixed on the parser page. Repeats until a pass
     * finds nothing left; the iteration cap only guards a malformed cycle.
     *
     * <p>Asks first on a large trace: the measured tree here is 1,250 nodes, and building every row
     * at once is exactly the wait this lazy rendering was added to remove.
     */
    expandAll() {
        // From the tree, not from _nodeMap: indexing is lazy now, so _nodeMap holds only rendered
        // rows and the prompt would have understated a 1,250-node trace as a handful.
        const total = PA.callTrace._countNodes(PA.callTrace._root);
        if (total > 400 && !window.confirm(
                'This trace has ' + total + ' nodes. Expanding all of them will render every row '
                + 'and may take a moment.\n\nExpand anyway?')) {
            return;
        }
        PA.callTrace._materialiseAll();
        document.querySelectorAll('.ct-children').forEach(el => el.style.display = '');
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

    /** Clicking the procedure name: open the popup detail module */
    onNameClick(procId, event) {
        event.stopPropagation();
        PA.loadNodeDetail(procId); // update main screen header too
        PA.codeModal.open(procId); // open the popup module
    },

    /** Clicking a line number: open the popup module at that line */
    onLineClick(sourceFile, line, event) {
        event.stopPropagation();
        if (sourceFile) {
            PA.codeModal.openAtLine(sourceFile, line);
        }
    },

    /** Search within the Explore tree — highlights matching rows, expands parents */
    search(query) {
        const q = (query || '').trim().toLowerCase();
        // Search walks the DOM, and with lazy rendering most of the tree is not in it. Without this
        // a search would quietly miss every collapsed branch and report "no match" for a procedure
        // that is plainly there — a wrong answer, and exactly the failure mode the lazy rendering
        // was not supposed to introduce.
        //
        // Materialising only when the user searches is the trade: the first paint stays bounded,
        // which is the problem being fixed, and someone typing in the search box has asked to look
        // everywhere. _materialiseAll is a no-op once nothing is left collapsed.
        if (q) PA.callTrace._materialiseAll();
        const rows = document.querySelectorAll('#ctContainer .ct-row');
        if (!q) {
            rows.forEach(r => { r.style.display = ''; r.classList.remove('ct-search-hit'); });
            document.querySelectorAll('#ctContainer .ct-children').forEach(el => el.style.display = '');
            return;
        }
        // First pass: mark matches
        const hitSteps = new Set();
        rows.forEach(r => {
            const name = (r.querySelector('.ct-name')?.textContent || '').toLowerCase();
            const schema = (r.querySelector('.ct-schema-badge')?.textContent || '').toLowerCase();
            const hit = name.includes(q) || schema.includes(q);
            r.classList.toggle('ct-search-hit', hit);
            if (hit) hitSteps.add(r.id);
        });
        // Expand all so matches are visible, then hide non-matching
        document.querySelectorAll('#ctContainer .ct-children').forEach(el => el.style.display = '');
        rows.forEach(r => {
            if (r.classList.contains('ct-search-hit')) {
                r.style.display = '';
                // Ensure all parent ct-children are visible
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

    /** Update breadcrumb bar */
    updateBreadcrumb() {
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
