/**
 * Summary trace — flat Dynatrace-style trace overlay for PL/SQL call trees.
 * Shows sequential list of all calls with depth indication.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    /** Show flat trace for a procedure report */
    showTrace(idx) {
        const r = this._procReports ? this._procReports[idx] : null;
        if (!r) { PA.toast('Procedure not found at index ' + idx, 'error'); return; }
        const esc = PA.esc;

        // We need the call tree from the API
        this._loadTraceTree(r, esc);
    },

    async _loadTraceTree(r, esc) {
        let callTree = null;
        try {
            const detail = await PA.api.getProcDetail(r.id || r.name);
            if (detail && detail.callTree) callTree = detail.callTree;
        } catch (e) { /* fallback */ }

        if (!callTree) {
            PA.toast('No call tree available for ' + r.name, 'error');
            return;
        }

        // Flatten the tree
        const flatNodes = [];
        this._flattenTree(callTree, flatNodes, 0);

        // Build overlay
        let html = '<div style="position:fixed;inset:0;z-index:1000;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center" id="sum-trace-overlay" onclick="if(event.target===this)this.remove()">';
        html += '<div style="background:var(--bg-card);border-radius:12px;width:90vw;max-width:1200px;max-height:90vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,0.3)">';

        // Header
        html += '<div style="display:flex;align-items:center;gap:8px;padding:12px 16px;border-bottom:1px solid var(--border);flex-shrink:0">';
        html += `<span class="lp-icon P" style="display:inline-flex">P</span>`;
        html += `<span style="font-weight:700;font-size:15px">${esc(r.name)}</span>`;
        html += `<span style="color:${this._schemaColor(r.schemaName)};font-weight:600">${esc(r.schemaName)}</span>`;
        html += '<div style="margin-left:auto;display:flex;gap:8px;align-items:center">';
        html += `<span style="font-size:11px;color:var(--text-muted)">${flatNodes.length} nodes | ${r.tableCount} tables | depth ${r.callDepth}</span>`;
        html += '<button class="btn btn-sm" onclick="document.getElementById(\'sum-trace-overlay\').remove()">Close</button>';
        html += '</div></div>';

        // Stats bar
        html += '<div style="display:flex;gap:6px;padding:8px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap;flex-shrink:0">';
        html += `<span class="dh-stat" style="padding:4px 10px"><span class="dh-stat-value" style="font-size:14px">${flatNodes.length}</span><span class="dh-stat-label">Nodes</span></span>`;
        html += `<span class="dh-stat" style="padding:4px 10px"><span class="dh-stat-value" style="font-size:14px">${r.tableCount}</span><span class="dh-stat-label">Tables</span></span>`;
        html += `<span class="dh-stat" style="padding:4px 10px"><span class="dh-stat-value" style="font-size:14px">${r.crossSchemaCalls}</span><span class="dh-stat-label">Cross-Schema</span></span>`;
        for (const s of r.schemas) {
            html += `<span style="padding:3px 8px;border-radius:4px;font-size:10px;font-weight:700;background:${this._schemaBg(s)};color:${this._schemaColor(s)}">${esc(s)}</span>`;
        }
        html += '</div>';

        // Trace nodes
        html += '<div style="flex:1;overflow:auto;padding:8px 0">';
        for (let i = 0; i < flatNodes.length; i++) {
            html += this._renderTraceNode(flatNodes[i], i, esc);
        }
        html += '</div>';

        html += '</div></div>';
        document.body.insertAdjacentHTML('beforeend', html);
    },

    /** Flatten call tree to sequential list with depth */
    _flattenTree(node, list, depth) {
        if (!node || depth > 50) return;
        list.push({
            nodeId: list.length,
            depth,
            id: node.id,
            name: node.name || node.id || '?',
            schemaName: node.schemaName || '',
            packageName: node.packageName || '',
            unitType: node.unitType || '',
            callType: node.callType || 'INTERNAL',
            startLine: node.startLine || 0,
            endLine: node.endLine || 0,
            tables: node.nodeTables || node.tables || [],
            childCount: node.children ? node.children.length : 0,
            circular: node.circular || false,
            stats: node.nodeStats || node.stats || {}
        });
        if (node.children && !node.circular) {
            for (const c of node.children) this._flattenTree(c, list, depth + 1);
        }
    },

    /** Render a single trace node row */
    _renderTraceNode(node, step, esc) {
        const depthColors = ['var(--accent)', 'var(--green)', 'var(--orange)', 'var(--red)', 'var(--purple)', 'var(--teal)', 'var(--pink)'];
        const color = depthColors[node.depth % depthColors.length];
        const isExternal = node.callType === 'EXTERNAL';
        const isCircular = node.circular;

        let html = '<div class="trace-row" style="padding-left:' + (node.depth * 20 + 12) + 'px">';

        // Step number
        html += `<span class="trace-step">${step + 1}</span>`;

        // Depth bar
        html += `<span class="trace-bar" style="width:${Math.min(node.depth * 15 + 30, 200)}px;background:${color}"></span>`;

        // Name
        html += `<span class="trace-name" title="${esc(node.schemaName + '.' + (node.packageName || '') + '.' + node.name)}">${esc(node.name)}</span>`;

        // Info badges
        html += '<span class="trace-info">';
        if (node.schemaName) {
            html += `<span style="font-size:9px;padding:1px 6px;border-radius:4px;font-weight:700;background:${this._schemaBg(node.schemaName)};color:${this._schemaColor(node.schemaName)}">${esc(node.schemaName)}</span>`;
        }
        if (isExternal) html += '<span class="ct-call-badge EXTERNAL">EXT</span>';
        if (isCircular) html += '<span class="ct-call-badge CIRCULAR">CIRCULAR</span>';
        if (node.unitType) {
            const utCls = node.unitType === 'FUNCTION' ? 'F' : node.unitType === 'TRIGGER' ? 'T' : 'P';
            html += `<span class="lp-icon ${utCls}" style="display:inline-flex;width:16px;height:16px;font-size:8px">${utCls}</span>`;
        }

        // Tables at this node
        if (node.tables && node.tables.length) {
            for (const t of node.tables) {
                const tName = t.tableName || t;
                const tOps = (t.operations || []).join(',');
                html += `<span style="font-size:9px;padding:1px 6px;background:var(--badge-teal-bg);color:var(--badge-teal);border-radius:4px;font-weight:700;font-family:var(--font-mono)" title="${esc(tOps)}">${esc(tName)}</span>`;
            }
        }

        if (node.childCount > 0) {
            html += `<span style="font-size:9px;color:var(--text-muted)">${node.childCount} calls</span>`;
        }
        html += '</span>';

        html += '</div>';
        return html;
    }
});
