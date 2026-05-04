/**
 * PA.export — Export modal for analysis data (JSON / Excel).
 * Supports full analysis export or per-table scoped export.
 */
window.PA = window.PA || {};

PA.export = {
    _fmt: 'excel',

    /** Show export modal — fetches live counts from API */
    async show(scope) {
        const old = document.getElementById('export-modal-overlay');
        if (old) old.remove();

        const esc = PA.esc || (v => v);
        const data = PA.analysisData;
        if (!data) { PA.toast('No analysis loaded', 'error'); return; }

        const name = data.name || 'analysis';

        // Fetch live data from API to get accurate counts
        try {
            if (!PA.export._procedures) PA.export._procedures = await PA.api.getProcedures();
            if (!PA.export._tables) PA.export._tables = await PA.api.getTableOperations();
        } catch(e) { console.warn('Export: failed prefetch', e); }
        const procs = (PA.export._procedures || data.procedures || []).length;
        const tables = (PA.export._tables || data.tableOperations || []).length;
        const callNodes = data.procedureCount || data.nodeCount || ((data.callGraph && data.callGraph.children) ? data.callGraph.children.length : 0);

        let html = `<div class="modal" id="export-modal-overlay" style="display:flex" onclick="if(event.target===this)this.remove()">
        <div class="modal-box" style="width:520px;max-width:90vw">
            <div class="modal-head">
                <h3>Export Analysis</h3>
                <button class="modal-x" onclick="document.getElementById('export-modal-overlay').remove()">&times;</button>
            </div>
            <div class="modal-body">
                <div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap">
                    <span class="badge">${esc(name)}</span>
                    <span class="badge" style="background:var(--badge-blue-bg);color:var(--badge-blue)">${procs} procedures</span>
                    <span class="badge" style="background:var(--badge-teal-bg);color:var(--badge-teal)">${tables} tables</span>
                    <span class="badge">${callNodes} call nodes</span>
                </div>

                <div style="font-weight:600;font-size:12px;margin-bottom:6px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px">Select Reports</div>
                <div style="display:flex;flex-direction:column;gap:4px;margin-bottom:16px">
                    <label style="display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer">
                        <input type="checkbox" id="exp-call-tree" checked> Call Tree (${callNodes} nodes)
                    </label>
                    <label style="display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer">
                        <input type="checkbox" id="exp-table-ops" checked> Table Operations (${tables} tables)
                    </label>
                    <label style="display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer">
                        <input type="checkbox" id="exp-procedures" checked> Procedures (${procs} procs)
                    </label>
                    <label style="display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer">
                        <input type="checkbox" id="exp-claude" checked> Claude Verification (if available)
                    </label>
                </div>

                <div style="font-weight:600;font-size:12px;margin-bottom:6px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px">Format</div>
                <div style="display:flex;gap:8px;margin-bottom:16px">
                    <button class="btn btn-sm" id="exp-fmt-json" onclick="PA.export._setFmt('json')" style="flex:1">JSON</button>
                    <button class="btn btn-sm btn-primary" id="exp-fmt-excel" onclick="PA.export._setFmt('excel')" style="flex:1">Excel (.xlsx)</button>
                </div>

                <div style="font-weight:600;font-size:12px;margin-bottom:6px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px">Filename</div>
                <div style="display:flex;gap:4px;margin-bottom:16px;align-items:center">
                    <input type="text" id="exp-filename" class="form-input" value="${esc(name)}_report" style="flex:1">
                    <span id="exp-ext" style="font-size:12px;color:var(--text-muted)">.xlsx</span>
                </div>

                <button class="btn btn-primary" style="width:100%" onclick="PA.export._run()">Export</button>
            </div>
        </div></div>`;

        document.body.insertAdjacentHTML('beforeend', html);
        PA.export._fmt = 'excel';
    },

    _setFmt(fmt) {
        PA.export._fmt = fmt;
        document.getElementById('exp-fmt-json')?.classList.toggle('btn-primary', fmt === 'json');
        document.getElementById('exp-fmt-json')?.classList.toggle('btn-sm', fmt !== 'json');
        document.getElementById('exp-fmt-excel')?.classList.toggle('btn-primary', fmt === 'excel');
        document.getElementById('exp-fmt-excel')?.classList.toggle('btn-sm', fmt !== 'excel');
        const extEl = document.getElementById('exp-ext');
        if (extEl) extEl.textContent = fmt === 'json' ? '.json' : '.xlsx';
    },

    async _run() {
        const data = PA.analysisData;
        if (!data) { PA.toast('No analysis loaded', 'error'); return; }

        // Ensure we have API data
        try {
            if (!PA.export._procedures) PA.export._procedures = await PA.api.getProcedures();
            if (!PA.export._tables) PA.export._tables = await PA.api.getTableOperations();
        } catch(e) { console.warn('Export: fetch error', e); }

        // Merge API data into data for export
        const exportData = Object.assign({}, data);
        exportData.procedures = PA.export._procedures || data.procedures || [];
        exportData.tableOperations = PA.export._tables || data.tableOperations || [];

        const sel = {
            callTree: document.getElementById('exp-call-tree')?.checked,
            tableOps: document.getElementById('exp-table-ops')?.checked,
            procedures: document.getElementById('exp-procedures')?.checked,
            claude: document.getElementById('exp-claude')?.checked
        };

        const baseName = (document.getElementById('exp-filename')?.value || 'export').trim();
        const ext = PA.export._fmt === 'json' ? 'json' : 'xlsx';
        const filename = baseName + '.' + ext;

        if (PA.export._fmt === 'json') {
            PA.export._exportJSON(exportData, sel, filename);
        } else {
            PA.export._exportExcel(exportData, sel, filename);
        }

        PA.export._procedures = null;
        PA.export._tables = null;
        document.getElementById('export-modal-overlay')?.remove();
    },

    _exportJSON(data, sel, filename) {
        const report = {
            _exportedAt: new Date().toISOString(),
            _tool: 'PL/SQL Analyzer',
            analysisName: data.name || '',
            entryPoint: data.entryPoint || ''
        };

        if (sel.procedures) {
            report.procedures = (data.procedures || []).map(p => ({
                id: p.id, name: p.name, packageName: p.packageName, schemaName: p.schemaName,
                unitType: p.unitType, callType: p.callType, lineCount: p.lineCount
            }));
        }
        if (sel.tableOps) {
            report.tableOperations = (data.tableOperations || []).map(t => ({
                tableName: t.tableName, schemaName: t.schemaName, tableType: t.tableType,
                operations: t.operations, external: t.external,
                accessCount: t.accessCount || (t.accessDetails || []).length,
                triggers: (t.triggers || []).map(tr => ({ triggerName: tr.triggerName, triggerType: tr.triggerType, triggeringEvent: tr.triggeringEvent }))
            }));
        }
        if (sel.callTree && data.callGraph) {
            report.callGraph = data.callGraph;
        }
        if (sel.claude && PA.claude && PA.claude.result) {
            report.claudeVerification = PA.claude.result;
        }

        const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = filename; a.click();
        URL.revokeObjectURL(url);
        PA.toast('JSON exported: ' + filename, 'success');
    },

    _exportExcel(data, sel, filename) {
        if (typeof XLSX === 'undefined') {
            PA.toast('XLSX library not loaded. Check internet connection.', 'error');
            return;
        }

        try {
            const wb = XLSX.utils.book_new();
            const headerStyle = { font: { bold: true, color: { rgb: 'FFFFFF' } }, fill: { fgColor: { rgb: '6366F1' } }, alignment: { horizontal: 'center' } };
            const altRow = { fill: { fgColor: { rgb: 'F1F5F9' } } };

            // Summary sheet
            const sumData = [
                ['PL/SQL Analyzer — Export Report'],
                ['Analysis:', data.name || ''],
                ['Entry Point:', data.entryPoint || ''],
                ['Exported:', new Date().toISOString()],
                [],
                ['Procedures:', (data.procedures || []).length],
                ['Tables:', (data.tableOperations || []).length],
                ['Call Nodes:', (data.callGraph && data.callGraph.children) ? data.callGraph.children.length : 0]
            ];
            const sumWs = XLSX.utils.aoa_to_sheet(sumData);
            sumWs['!cols'] = [{ wch: 20 }, { wch: 60 }];
            XLSX.utils.book_append_sheet(wb, sumWs, 'Summary');

            // Procedures sheet
            if (sel.procedures && data.procedures?.length) {
                const rows = [['ID', 'Name', 'Package', 'Schema', 'Type', 'Call Type', 'Lines']];
                for (const p of data.procedures) {
                    rows.push([p.id, p.name, p.packageName || '', p.schemaName || '', p.unitType || '', p.callType || '', p.lineCount || 0]);
                }
                const ws = XLSX.utils.aoa_to_sheet(rows);
                ws['!cols'] = [{ wch: 40 }, { wch: 25 }, { wch: 25 }, { wch: 15 }, { wch: 12 }, { wch: 12 }, { wch: 8 }];
                // Style header row
                for (let c = 0; c < 7; c++) {
                    const cell = ws[XLSX.utils.encode_cell({ r: 0, c })];
                    if (cell) cell.s = headerStyle;
                }
                XLSX.utils.book_append_sheet(wb, ws, 'Procedures');
            }

            // Table Operations sheet
            if (sel.tableOps && data.tableOperations?.length) {
                const rows = [['Table', 'Schema', 'Type', 'Operations', 'Access Count', 'External', 'Triggers']];
                for (const t of data.tableOperations) {
                    rows.push([
                        t.tableName || '', t.schemaName || '', t.tableType || 'TABLE',
                        (t.operations || []).join(', '),
                        t.accessCount || (t.accessDetails || []).length,
                        t.external ? 'Yes' : 'No',
                        (t.triggers || []).map(tr => tr.triggerName).join(', ')
                    ]);
                }
                const ws = XLSX.utils.aoa_to_sheet(rows);
                ws['!cols'] = [{ wch: 30 }, { wch: 15 }, { wch: 12 }, { wch: 30 }, { wch: 12 }, { wch: 10 }, { wch: 40 }];
                for (let c = 0; c < 7; c++) {
                    const cell = ws[XLSX.utils.encode_cell({ r: 0, c })];
                    if (cell) cell.s = headerStyle;
                }
                // Alternate row colors
                for (let r = 1; r < rows.length; r++) {
                    if (r % 2 === 0) {
                        for (let c = 0; c < 7; c++) {
                            const cell = ws[XLSX.utils.encode_cell({ r, c })];
                            if (cell) cell.s = altRow;
                        }
                    }
                }
                XLSX.utils.book_append_sheet(wb, ws, 'Table Operations');
            }

            // Claude Verification sheet
            if (sel.claude && PA.claude?.result?.tableResults) {
                const rows = [['Table', 'Schema', 'Status', 'Static Ops', 'Claude Ops', 'Notes']];
                for (const [tblName, tr] of Object.entries(PA.claude.result.tableResults)) {
                    rows.push([
                        tblName, tr.schemaName || '',
                        tr.status || '',
                        (tr.staticOperations || []).join(', '),
                        (tr.verifiedOperations || []).join(', '),
                        tr.notes || ''
                    ]);
                }
                const ws = XLSX.utils.aoa_to_sheet(rows);
                ws['!cols'] = [{ wch: 30 }, { wch: 15 }, { wch: 12 }, { wch: 25 }, { wch: 25 }, { wch: 50 }];
                for (let c = 0; c < 6; c++) {
                    const cell = ws[XLSX.utils.encode_cell({ r: 0, c })];
                    if (cell) cell.s = headerStyle;
                }
                XLSX.utils.book_append_sheet(wb, ws, 'Claude Verification');
            }

            XLSX.writeFile(wb, filename, { cellStyles: true });
            PA.toast('Excel exported: ' + filename, 'success');
        } catch (err) {
            console.error('Excel export failed:', err);
            PA.toast('Excel export failed: ' + err.message, 'error');
        }
    }
};
