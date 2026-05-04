/**
 * PA.callTrace — Depth-indented HTML call trace (jar-analyzer style).
 * Renders call tree as nested rows with expand/collapse, breadcrumb, schema badges.
 */
window.PA = window.PA || {};

const SCHEMA_COLORS = {
    'CUSTOMER':   { bg: '#dbeafe', fg: '#1d4ed8' },
    'CUSTOMER_I': { bg: '#ccfbf1', fg: '#0f766e' },
    'OPUS_CORE':  { bg: '#dcfce7', fg: '#15803d' },
    'POLICY':     { bg: '#f3e8ff', fg: '#7e22ce' },
    'CLAIMS':     { bg: '#fee2e2', fg: '#b91c1c' },
    'ACCOUNTING': { bg: '#fef3c7', fg: '#a16207' },
    'DEFAULT':    { bg: '#f1f5f9', fg: '#475569' }
};

function getSchemaColor(schema) {
    if (!schema) return SCHEMA_COLORS.DEFAULT;
    return SCHEMA_COLORS[schema.toUpperCase()] || SCHEMA_COLORS.DEFAULT;
}

PA.callTrace = {
    treeData: null,
    breadcrumbStack: [],  // [{id, name}]
    stepCounter: 0,

    /** Load call tree for a procedure and render it */
    async load(procId) {
        const data = await PA.api.getCallTree(procId);
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
        const data = await PA.api.getCallTree(procId);
        if (!data || !data.id) return;
        PA.callTrace.treeData = data;
        PA.callTrace.breadcrumbStack.push({ id: data.id, name: data.name || data.id });
        PA.callTrace.render(data);
    },

    /** Go to a specific breadcrumb position */
    async goTo(index) {
        if (index < 0) return;
        const item = PA.callTrace.breadcrumbStack[index];
        PA.callTrace.breadcrumbStack = PA.callTrace.breadcrumbStack.slice(0, index + 1);
        const data = await PA.api.getCallTree(item.id);
        if (data) {
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

        let html = '';
        html += PA.callTrace.renderNode(data, 0, true);
        container.innerHTML = html;
    },

    /** Render a single node and its children recursively */
    renderNode(node, depth, expanded) {
        PA.callTrace.stepCounter++;
        const step = PA.callTrace.stepCounter;
        const indent = depth * 20;
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
        if (isCircular) {
            html += `<span class="ct-call-badge CIRCULAR" title="Circular reference — this procedure is already in the call chain above">CIRCULAR</span>`;
        } else if (callType) {
            html += `<span class="ct-call-badge ${PA.escAttr(callType)}" title="${callTypeTooltips[callType] || callType}">${PA.esc(callType)}</span>`;
        }

        html += `</div>`;

        // Children (rendered inline, can be toggled)
        if (hasChildren && !isCircular) {
            html += `<div class="ct-children" id="ct-children-${step}">`;
            for (const child of node.children) {
                html += PA.callTrace.renderNode(child, depth + 1, true);
            }
            html += `</div>`;
        }

        return html;
    },

    /** Toggle expand/collapse */
    toggle(step, event) {
        event.stopPropagation();
        const container = document.getElementById('ct-children-' + step);
        const icon = document.querySelector(`.ct-toggle[data-step="${step}"]`);
        if (!container) return;
        if (container.style.display === 'none') {
            container.style.display = '';
            if (icon) icon.innerHTML = '&#9660;';
        } else {
            container.style.display = 'none';
            if (icon) icon.innerHTML = '&#9654;';
        }
    },

    expandAll() {
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
