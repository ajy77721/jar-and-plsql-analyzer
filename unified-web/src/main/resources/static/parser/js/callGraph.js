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

PA.getSchemaColor = getSchemaColor;

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

    _nodeMap: {},
    _initialDepth: 1,
    _indexDataCache: null,

    _getNodeMetrics: function(nodeId) {
        if (!PA.analysisData || !PA.analysisData.nodes) return null;
        if (!PA.callTrace._indexDataCache) {
            var cache = {};
            var nodes = PA.analysisData.nodes;
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
        if (!n) return null;
        var w = (PA.complexity && PA.complexity._weights) || {};
        var th = (PA.complexity && PA.complexity._thresholds) || {};
        var c = n.counts || {};
        var stmts = c.statements || {};
        var dynSql = (stmts.EXECUTE_IMMEDIATE || 0) + (stmts.DBMS_SQL || 0);
        var score = Math.round(
            ((n.linesOfCode || 0) * (w.loc != null ? w.loc : 0.3))
            + ((c.tables || 0) * (w.tables != null ? w.tables : 15))
            + ((c.callsOut || 0) * (w.callsOut != null ? w.callsOut : 10))
            + ((c.cursors || 0) * (w.cursors != null ? w.cursors : 8))
            + (dynSql * (w.dynamicSql != null ? w.dynamicSql : 20))
            + ((n.depth || 0) * (w.depth != null ? w.depth : 5))
        );
        var thMed = th.medium != null ? th.medium : 50;
        var thHigh = th.high != null ? th.high : 150;
        if (score >= thHigh) return { label: 'H', cls: 'high', score: score };
        if (score >= thMed) return { label: 'M', cls: 'medium', score: score };
        return { label: 'L', cls: 'low', score: score };
    },

    /** Render the call tree as depth-indented HTML */
    render(data) {
        PA.callTrace.updateBreadcrumb();
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

        PA.callTrace._indexNodes(data, null);
        let html = '';
        html += PA.callTrace.renderNode(data, 0, null);
        container.innerHTML = html;
    },

    _indexNodes(node, parentSourceFile) {
        PA.callTrace.stepCounter++;
        const step = PA.callTrace.stepCounter;
        node._step = step;
        node._parentSourceFile = parentSourceFile || '';
        const sf = node.sourceFile || (node.schemaName && node.packageName ? node.schemaName + '.' + node.packageName : '');
        PA.callTrace._nodeMap[step] = node;
        if (node.children && !node.circular) {
            for (const child of node.children) {
                PA.callTrace._indexNodes(child, sf);
            }
        }
    },

    /** Render a single node — children rendered only if within initial depth */
    renderNode(node, depth, parentSourceFile) {
        const step = node._step;
        const indent = depth * 20;
        const hasChildren = node.children && node.children.length > 0;
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
        const childCount = hasChildren && !isCircular ? node.children.length : 0;

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

        if (startLine > 0) {
            const bodyLabel = (endLine > 0 && endLine !== startLine) ? `L${startLine}-${endLine}` : `L${startLine}`;
            html += `<span class="ct-line" data-tip="Open source at line ${startLine}" onclick="PA.callTrace.onLineClick('${PA.escJs(sourceFile)}', ${startLine}, event)">${bodyLabel}</span>`;
        }
        if (callLine > 0 && parentSourceFile) {
            html += `<span class="ct-call-line" data-tip="Called at line ${callLine}" onclick="PA.callTrace.onLineClick('${PA.escJs(parentSourceFile)}', ${callLine}, event)">@${callLine}</span>`;
        }

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

        if (hasChildren && !isCircular) {
            const renderNow = depth < PA.callTrace._initialDepth;
            if (renderNow) {
                html += `<div class="ct-children" id="ct-children-${step}" data-loaded="1">`;
                for (const child of node.children) {
                    html += PA.callTrace.renderNode(child, depth + 1, sourceFile);
                }
                html += `</div>`;
            } else {
                html += `<div class="ct-children" id="ct-children-${step}" data-loaded="0" style="display:none"></div>`;
            }
        }

        return html;
    },

    _renderChildrenLazy(step, depth) {
        const node = PA.callTrace._nodeMap[step];
        if (!node || !node.children) return;
        const container = document.getElementById('ct-children-' + step);
        if (!container || container.dataset.loaded === '1') return;
        const sf = node.sourceFile || (node.schemaName && node.packageName ? node.schemaName + '.' + node.packageName : '');
        let html = '';
        for (const child of node.children) {
            html += PA.callTrace.renderNode(child, depth + 1, sf);
        }
        container.innerHTML = html;
        container.dataset.loaded = '1';
    },

    /** Toggle expand/collapse */
    toggle(step, depth, event) {
        event.stopPropagation();
        const container = document.getElementById('ct-children-' + step);
        const icon = document.querySelector(`.ct-toggle[data-step="${step}"]`);
        if (!container) return;
        if (container.style.display === 'none') {
            if (container.dataset.loaded === '0') {
                PA.callTrace._renderChildrenLazy(step, depth);
            }
            container.style.display = '';
            if (icon) icon.innerHTML = '&#9660;';
        } else {
            container.style.display = 'none';
            if (icon) icon.innerHTML = '&#9654;';
        }
    },

    expandAll() {
        document.querySelectorAll('.ct-children').forEach(el => {
            if (el.dataset.loaded === '0') {
                const step = parseInt(el.id.replace('ct-children-', ''), 10);
                const row = document.getElementById('ct-' + step);
                const depth = row ? parseInt(row.dataset.depth || '0', 10) : 0;
                PA.callTrace._renderChildrenLazy(step, depth);
            }
            el.style.display = '';
        });
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

    /** Search within the Explore tree — expands all lazy nodes first, then filters */
    search(query) {
        const q = (query || '').trim().toLowerCase();
        const rows = document.querySelectorAll('#ctContainer .ct-row');
        if (!q) {
            rows.forEach(r => { r.style.display = ''; r.classList.remove('ct-search-hit'); });
            document.querySelectorAll('#ctContainer .ct-children').forEach(el => el.style.display = '');
            return;
        }
        PA.callTrace.expandAll();
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
