/**
 * PA.triggerModal — Trigger detail popup with analysis panel + source code.
 * Shows: table ON, event/type, referenced tables with DML ops,
 * called procedures (navigable), and full source code.
 */
window.PA = window.PA || {};

PA.triggerModal = {
    open(tableName, triggerName) {
        const modal = document.getElementById('triggerModal');
        modal.style.display = 'flex';
        document.getElementById('triggerModalName').textContent = triggerName || '';
        document.getElementById('triggerModalEvent').textContent = '';
        document.getElementById('triggerModalType').textContent = '';
        document.getElementById('triggerAnalysisContent').innerHTML = '<div class="empty-msg">Loading trigger analysis...</div>';
        document.getElementById('triggerSourceCode').innerHTML = '<div class="empty-msg">Loading trigger source...</div>';

        PA.triggerModal._loadTrigger(tableName, triggerName);
    },

    close() {
        document.getElementById('triggerModal').style.display = 'none';
        document.getElementById('triggerAnalysisContent').innerHTML = '';
        document.getElementById('triggerSourceCode').innerHTML = '';
    },

    async _loadTrigger(tableName, triggerName) {
        try {
            const triggers = await PA.api.getTableTriggers(tableName);
            const tr = triggers.find(t => (t.triggerName || '').toUpperCase() === (triggerName || '').toUpperCase());
            if (!tr) {
                document.getElementById('triggerAnalysisContent').innerHTML = '<div class="empty-msg">Trigger not found in analysis data</div>';
                document.getElementById('triggerSourceCode').innerHTML = '';
                return;
            }

            document.getElementById('triggerModalEvent').textContent = tr.triggeringEvent || '';
            document.getElementById('triggerModalType').textContent = tr.triggerType || '';

            this._renderAnalysisPanel(tr, tableName);
            this._renderSourceCode(tr);
        } catch (e) {
            document.getElementById('triggerAnalysisContent').innerHTML =
                `<div class="empty-msg">Error: ${PA.esc(e.message || String(e))}</div>`;
        }
    },

    _renderAnalysisPanel(tr, tableName) {
        const esc = PA.esc;
        let html = '';

        // Table ON section
        html += '<div style="margin-bottom:14px">';
        html += '<div style="font-size:10px;font-weight:700;color:var(--text-muted);text-transform:uppercase;margin-bottom:6px;letter-spacing:0.5px">Trigger On</div>';
        html += '<div style="display:flex;align-items:center;gap:6px;padding:8px 10px;background:var(--bg-card);border:1px solid var(--border);border-radius:6px">';
        html += '<span style="color:var(--orange);font-size:16px">&#9638;</span>';
        html += `<span style="font-family:var(--font-mono);font-weight:700;font-size:13px;color:var(--teal);cursor:pointer" onclick="PA.triggerModal.close(); PA.tableInfo.open('${PA.escJs(tableName)}')">${esc(tableName)}</span>`;
        html += '</div>';
        html += `<div style="font-size:11px;color:var(--text-muted);margin-top:4px;padding-left:2px">${esc(tr.triggeringEvent || '')} &mdash; ${esc(tr.triggerType || '')}</div>`;
        html += '</div>';

        // Trigger info
        html += '<div style="margin-bottom:14px">';
        html += '<div style="font-size:10px;font-weight:700;color:var(--text-muted);text-transform:uppercase;margin-bottom:6px;letter-spacing:0.5px">Trigger Info</div>';
        html += '<div style="font-size:11px;line-height:1.8">';
        if (tr.triggerOwner) html += `<div><span style="color:var(--text-muted)">Owner:</span> <span style="font-family:var(--font-mono)">${esc(tr.triggerOwner)}</span></div>`;
        html += `<div><span style="color:var(--text-muted)">Status:</span> <span class="badge" style="font-size:9px;background:${tr.status === 'ENABLED' ? 'var(--badge-green-bg);color:var(--badge-green)' : '#fee2e2;color:#b91c1c'}">${esc(tr.status || 'UNKNOWN')}</span></div>`;
        html += '</div></div>';

        // Referenced tables with DML operations
        const refTableOps = tr.referencedTableOps || [];
        const refTables = tr.referencedTables || [];
        if (refTableOps.length > 0 || refTables.length > 0) {
            html += '<div style="margin-bottom:14px">';
            html += `<div style="font-size:10px;font-weight:700;color:var(--text-muted);text-transform:uppercase;margin-bottom:6px;letter-spacing:0.5px">Referenced Tables (${refTableOps.length || refTables.length})</div>`;

            if (refTableOps.length > 0) {
                for (const ref of refTableOps) {
                    html += '<div style="padding:6px 8px;margin-bottom:4px;background:var(--bg-card);border:1px solid var(--border);border-radius:6px">';
                    html += `<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">`;
                    html += `<span style="font-family:var(--font-mono);font-size:12px;font-weight:600;color:var(--teal);cursor:pointer" onclick="PA.triggerModal.close(); PA.tableInfo.open('${PA.escJs(ref.tableName)}')">${esc(ref.tableName)}</span>`;
                    for (const op of (ref.operations || [])) {
                        html += `<span class="op-badge ${op}" style="font-size:9px">${esc(op)}</span>`;
                    }
                    html += '</div>';
                    // Check if this table has procedures in the analysis
                    html += this._renderTableProcsLink(ref.tableName);
                    html += '</div>';
                }
            } else {
                for (const t of refTables) {
                    html += `<div style="padding:4px 8px;margin-bottom:2px"><span class="badge" style="cursor:pointer;font-size:11px" onclick="PA.triggerModal.close(); PA.tableInfo.open('${PA.escJs(t)}')">${esc(t)}</span></div>`;
                }
            }
            html += '</div>';
        }

        // Called procedures
        const calledProcs = tr.calledProcedures || [];
        if (calledProcs.length > 0) {
            html += '<div style="margin-bottom:14px">';
            html += `<div style="font-size:10px;font-weight:700;color:var(--text-muted);text-transform:uppercase;margin-bottom:6px;letter-spacing:0.5px">Called Procedures (${calledProcs.length})</div>`;
            for (const proc of calledProcs) {
                html += '<div style="padding:5px 8px;margin-bottom:3px;background:var(--bg-card);border:1px solid var(--border);border-radius:6px;display:flex;align-items:center;gap:6px">';
                html += `<span style="color:var(--accent);font-size:13px">&#9654;</span>`;
                html += `<span class="to-detail-proc" style="cursor:pointer;font-size:12px;font-family:var(--font-mono)" onclick="PA.triggerModal.close(); PA.showProcedure('${PA.escJs(proc)}')">${esc(proc)}</span>`;
                html += '</div>';
            }
            html += '</div>';
        }

        // DML summary
        html += this._renderDmlSummary(tr);

        document.getElementById('triggerAnalysisContent').innerHTML = html;
    },

    _renderTableProcsLink(tableName) {
        if (!PA.tableOps || !PA.tableOps.data) return '';
        const table = PA.tableOps.data.find(t => (t.tableName || '').toUpperCase() === tableName.toUpperCase());
        if (!table || !table.accessDetails || table.accessDetails.length === 0) return '';
        const procs = [...new Set(table.accessDetails.map(d => d.procedureName).filter(Boolean))];
        if (procs.length === 0) return '';
        let html = '<div style="margin-top:4px;padding-left:4px">';
        html += `<span style="font-size:9px;color:var(--text-muted)">Used by: </span>`;
        for (const p of procs.slice(0, 5)) {
            html += `<span class="to-detail-proc" style="font-size:10px;cursor:pointer;margin-right:4px" onclick="PA.triggerModal.close(); PA.showProcedure('${PA.escJs(p)}')">${PA.esc(p)}</span>`;
        }
        if (procs.length > 5) html += `<span style="font-size:9px;color:var(--text-muted)">+${procs.length - 5} more</span>`;
        html += '</div>';
        return html;
    },

    _renderDmlSummary(tr) {
        if (!tr.triggerBody) return '';
        const body = tr.triggerBody.toUpperCase();
        const ops = [];
        if (body.includes('INSERT INTO')) ops.push('INSERT');
        if (/UPDATE\s+\w+/.test(body)) ops.push('UPDATE');
        if (body.includes('DELETE FROM') || /DELETE\s+\w+/.test(body)) ops.push('DELETE');
        if (/SELECT\s+/.test(body) && body.includes('FROM')) ops.push('SELECT');
        if (ops.length === 0) return '';

        let html = '<div style="margin-bottom:14px">';
        html += '<div style="font-size:10px;font-weight:700;color:var(--text-muted);text-transform:uppercase;margin-bottom:6px;letter-spacing:0.5px">DML Operations in Body</div>';
        html += '<div style="display:flex;gap:4px;flex-wrap:wrap">';
        for (const op of ops) {
            html += `<span class="op-badge ${op}" style="font-size:10px">${PA.esc(op)}</span>`;
        }
        html += '</div></div>';
        return html;
    },

    _renderSourceCode(tr) {
        const sourceEl = document.getElementById('triggerSourceCode');
        if (!tr.triggerBody) {
            sourceEl.innerHTML = '<div class="empty-msg">Trigger source not available</div>';
            return;
        }

        const lines = tr.triggerBody.split('\n');
        const keywords = /\b(CREATE|OR|REPLACE|TRIGGER|BEFORE|AFTER|INSTEAD|OF|ON|FOR|EACH|ROW|WHEN|DECLARE|BEGIN|END|IF|THEN|ELSE|ELSIF|INSERT|INTO|UPDATE|DELETE|FROM|SELECT|SET|VALUES|WHERE|AND|OR|NOT|NULL|IS|IN|CURRENT_TIMESTAMP|VARCHAR|INTEGER|DECIMAL|DATE|TIMESTAMP)\b/gi;
        const strings = /('(?:[^']|'')*')/g;

        let html = '<div style="padding:8px 0;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px;padding-left:16px">';
        html += '<span style="font-size:10px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px">Source Code</span>';
        html += `<span style="font-size:10px;color:var(--text-muted)">${lines.length} lines</span>`;
        html += '</div>';
        html += '<pre style="margin:0;padding:12px 16px;font-size:12px;line-height:1.6;font-family:var(--font-mono);overflow:auto;height:calc(100% - 36px)">';

        for (let i = 0; i < lines.length; i++) {
            const ln = String(i + 1).padStart(4, ' ');
            let line = PA.esc(lines[i]);
            // Highlight strings first (protect from keyword highlighting)
            const strPlaceholders = [];
            line = line.replace(/&#39;(?:[^&]|&(?!#39;))*&#39;/g, m => {
                strPlaceholders.push(`<span style="color:#a6e3a1">${m}</span>`);
                return `\x00STR${strPlaceholders.length - 1}\x00`;
            });
            // Highlight keywords
            line = line.replace(keywords, m => `<span style="color:#cba6f7;font-weight:600">${m}</span>`);
            // Highlight :NEW/:OLD
            line = line.replace(/:(NEW|OLD)\b/gi, m => `<span style="color:#f9e2af;font-weight:600">${m}</span>`);
            // Restore strings
            line = line.replace(/\x00STR(\d+)\x00/g, (_, idx) => strPlaceholders[parseInt(idx)]);

            html += `<span style="color:var(--text-muted);user-select:none">${ln}</span>  ${line}\n`;
        }
        html += '</pre>';
        sourceEl.innerHTML = html;
    }
};
