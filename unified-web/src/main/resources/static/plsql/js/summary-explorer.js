/**
 * Summary explorer — interactive drill-down call trace viewer.
 * Breadcrumb navigation, scope stats, tables at each level.
 * PL/SQL Analyzer: navigates procedure call trees.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    _traceState: null, // { tree, trail: [node,...], procReport }

    showCallTrace(idx) {
        const r = this._procReports ? this._procReports[idx] : null;
        if (!r) { PA.toast('Procedure not found at index ' + idx, 'error'); return; }
        this._loadExplorerTree(r);
    },

    async _loadExplorerTree(r) {
        let callTree = null;
        try {
            const detail = await PA.api.getProcDetail(r.id || r.name);
            if (detail && detail.callTree) callTree = detail.callTree;
        } catch (e) { /* fallback */ }

        if (!callTree) {
            PA.toast('No call tree available for ' + r.name, 'error');
            return;
        }

        this._traceState = { tree: callTree, trail: [callTree], procReport: r };
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

    _renderCallTrace() {
        const s = this._traceState;
        if (!s) return;
        const esc = PA.esc;
        const r = s.procReport;
        const current = s.trail[s.trail.length - 1];
        const children = (current.children || []).filter(c => !c.circular);
        const depth = s.trail.length - 1;

        const old = document.getElementById('call-trace-overlay');
        if (old) old.remove();

        let html = '<div style="position:fixed;inset:0;z-index:1000;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center" id="call-trace-overlay" onclick="if(event.target===this)this.remove()">';
        html += '<div style="background:var(--bg-card);border-radius:12px;width:90vw;max-width:1100px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,0.3)">';

        // Header
        html += '<div style="display:flex;align-items:center;gap:8px;padding:12px 16px;border-bottom:1px solid var(--border);flex-shrink:0">';
        html += `<span class="lp-icon P" style="display:inline-flex">P</span>`;
        html += `<span style="font-weight:700;font-size:15px">${esc(r.name)}</span>`;
        html += `<span style="color:var(--text-muted);font-size:11px">Depth: ${depth}</span>`;
        html += '<button class="btn btn-sm" style="margin-left:auto" onclick="document.getElementById(\'call-trace-overlay\').remove()">Close</button>';
        html += '</div>';

        // Breadcrumb trail
        html += '<div style="display:flex;align-items:center;gap:4px;padding:8px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap;font-size:12px;flex-shrink:0">';
        if (s.trail.length > 1) {
            html += '<button class="btn btn-sm" onclick="PA.summary._traceBack()" style="font-size:10px;padding:2px 8px">Back</button>';
        }
        s.trail.forEach((n, i) => {
            const isCurrent = (i === s.trail.length - 1);
            const label = n.name || n.id || '?';
            if (isCurrent) {
                html += `<span style="font-weight:700;color:var(--accent)">${esc(label)}</span>`;
            } else {
                html += `<span style="cursor:pointer;color:var(--text-muted)" onclick="PA.summary._traceNavigateTo(PA.summary._traceState.trail[${i}])">${esc(label)}</span>`;
            }
            if (!isCurrent) html += '<span style="color:var(--text-muted);opacity:0.5">&rsaquo;</span>';
        });
        html += '</div>';

        // Current node detail card
        html += this._buildExplorerNodeCard(current, esc);

        // Children list
        html += '<div style="flex:1;overflow:auto;padding:8px 16px">';
        if (children.length > 0) {
            html += `<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:var(--text-muted);margin-bottom:8px">Called Procedures (${children.length})</div>`;
            children.forEach((child, ci) => {
                const childNodes = this._countTreeNodes(child);
                const childTables = this._collectTreeTables(child);
                const isExt = child.callType === 'EXTERNAL';
                const unitCls = child.unitType === 'FUNCTION' ? 'F' : child.unitType === 'TRIGGER' ? 'T' : 'P';

                html += `<div style="padding:8px 12px;border:1px solid var(--border);border-radius:8px;margin-bottom:4px;cursor:pointer;transition:background 0.15s" onclick="PA.summary._traceDrillChild(${ci})" onmouseover="this.style.background='rgba(99,102,241,0.04)'" onmouseout="this.style.background=''">`;
                html += '<div style="display:flex;align-items:center;gap:8px">';
                html += `<span class="lp-icon ${unitCls}" style="display:inline-flex;width:18px;height:18px;font-size:9px">${unitCls}</span>`;
                html += `<span style="font-weight:700;font-family:var(--font-mono);font-size:12px">${esc(child.name || child.id || '?')}</span>`;
                if (child.schemaName) html += `<span style="color:${this._schemaColor(child.schemaName)};font-size:10px;font-weight:600">${esc(child.schemaName)}</span>`;
                if (isExt) html += '<span class="ct-call-badge EXTERNAL">EXT</span>';
                if (child.circular) html += '<span class="ct-call-badge CIRCULAR">CIRCULAR</span>';
                html += '</div>';

                html += '<div style="display:flex;gap:6px;margin-top:4px;align-items:center;font-size:10px;color:var(--text-muted)">';
                html += `<span>${childNodes} nodes</span>`;
                if (childTables.length > 0) html += `<span>${childTables.length} tables</span>`;
                if (child.children && child.children.length > 0) html += `<span>${child.children.length} calls</span>`;
                if (childTables.length > 0 && childTables.length <= 5) {
                    childTables.forEach(t => {
                        html += `<span style="font-size:9px;padding:1px 4px;background:var(--badge-teal-bg);color:var(--badge-teal);border-radius:3px;font-family:var(--font-mono)">${esc(t)}</span>`;
                    });
                }
                html += '</div>';
                html += '</div>';
            });
        } else {
            html += '<div style="padding:20px;text-align:center;color:var(--text-muted)">Leaf node - no further calls</div>';
        }
        html += '</div>';

        html += '</div></div>';
        document.body.insertAdjacentHTML('beforeend', html);
    },

    _traceDrillChild(childIdx) {
        if (!this._traceState) return;
        const current = this._traceState.trail[this._traceState.trail.length - 1];
        const children = (current.children || []).filter(c => !c.circular);
        const child = children[childIdx];
        if (child) this._traceNavigateTo(child);
    },

    _buildExplorerNodeCard(node, esc) {
        let html = '<div style="padding:12px 16px;border-bottom:1px solid var(--border);flex-shrink:0">';

        // Name + metadata
        html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">';
        const unitCls = node.unitType === 'FUNCTION' ? 'F' : node.unitType === 'TRIGGER' ? 'T' : 'P';
        html += `<span class="lp-icon ${unitCls}" style="display:inline-flex">${unitCls}</span>`;
        html += `<span style="font-weight:700;font-size:15px;font-family:var(--font-mono)">${esc(node.name || node.id || '?')}</span>`;
        if (node.schemaName) html += `<span style="color:${this._schemaColor(node.schemaName)};font-weight:600">${esc(node.schemaName)}</span>`;
        if (node.packageName) html += `<span style="color:var(--text-muted);font-size:11px">${esc(node.packageName)}</span>`;
        if (node.callType === 'EXTERNAL') html += '<span class="ct-call-badge EXTERNAL">EXT</span>';
        html += '</div>';

        // Stats
        const totalNodes = this._countTreeNodes(node);
        const allTables = this._collectTreeTables(node);
        const extCalls = this._countExternalCalls(node);
        html += '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">';
        html += `<span class="dh-stat" style="padding:4px 10px"><span class="dh-stat-value" style="font-size:14px">${totalNodes}</span><span class="dh-stat-label">Nodes</span></span>`;
        html += `<span class="dh-stat" style="padding:4px 10px"><span class="dh-stat-value" style="font-size:14px">${allTables.length}</span><span class="dh-stat-label">Tables</span></span>`;
        html += `<span class="dh-stat" style="padding:4px 10px"><span class="dh-stat-value" style="font-size:14px">${(node.children || []).length}</span><span class="dh-stat-label">Direct Calls</span></span>`;
        html += `<span class="dh-stat" style="padding:4px 10px"><span class="dh-stat-value" style="font-size:14px">${extCalls}</span><span class="dh-stat-label">External</span></span>`;
        html += '</div>';

        // Tables at this level
        const nodeTables = node.nodeTables || node.tables || [];
        if (nodeTables.length) {
            html += '<div style="margin-bottom:6px"><span style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-muted)">Tables at this level: </span>';
            for (const t of nodeTables) {
                const tName = t.tableName || t;
                const ops = (t.operations || []);
                html += this._tableBadge(tName, esc, ops) + ' ';
            }
            html += '</div>';
        }

        // All tables in scope
        if (allTables.length > 0) {
            html += '<div><span style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-muted)">All tables in scope (' + allTables.length + '): </span>';
            allTables.forEach(t => {
                html += `<span style="font-size:9px;padding:1px 6px;background:var(--badge-teal-bg);color:var(--badge-teal);border-radius:4px;font-family:var(--font-mono);font-weight:700;margin-right:3px">${esc(t)}</span>`;
            });
            html += '</div>';
        }

        html += '</div>';
        return html;
    }
});
