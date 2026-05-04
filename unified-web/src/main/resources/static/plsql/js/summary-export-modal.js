/**
 * Summary export modal — export dialog with format options.
 * PL/SQL Analyzer: Procedures, Tables, Transactions, Batch, Schema Slice, External.
 *
 * Usage:
 *   PA.summary.showExportModal()                           — full export
 *   PA.summary.showExportModal({ procIdx: 5 })             — single procedure
 *   PA.summary.showExportModal({ tableName: 'ORDERS' })    — single table
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    showExportModal(scope) {
        if (!this._procReports) { PA.toast('No analysis loaded', 'error'); return; }
        const old = document.getElementById('export-modal-overlay');
        if (old) old.remove();

        const isProcScoped = scope && scope.procIdx != null;
        const isTableScoped = scope && scope.tableName != null;
        const isScoped = isProcScoped || isTableScoped;

        const esc = PA.esc;
        const proc = this._procReports || [];
        const txn = this._txnReport || [];
        const batch = this._batchReport || [];
        const ext = this._extReport || [];
        const slice = this._schemaSliceReport || [];

        let scopeLabel = '';
        if (isProcScoped) {
            const r = proc[scope.procIdx];
            scopeLabel = `<div style="padding:8px 12px;background:var(--badge-blue-bg);border-radius:6px;margin-bottom:12px;font-size:12px">Single Procedure: <strong>${esc(r ? r.name : '?')}</strong></div>`;
        } else if (isTableScoped) {
            scopeLabel = `<div style="padding:8px 12px;background:var(--badge-teal-bg);border-radius:6px;margin-bottom:12px;font-size:12px">Single Table: <strong>${esc(scope.tableName)}</strong></div>`;
        }

        const analysisName = esc((PA.analysisData && PA.analysisData.name) || 'plsql-analysis');

        let html = `<div class="modal" id="export-modal-overlay" onclick="if(event.target===this)this.remove()">
        <div class="modal-box" style="width:560px">
            <div class="modal-head">
                <h3>${isProcScoped ? 'Export Procedure' : isTableScoped ? 'Export Table' : 'Export Analysis'}</h3>
                <button class="modal-x" onclick="document.getElementById('export-modal-overlay').remove()">&times;</button>
            </div>
            <div class="modal-body" style="padding:20px">
                ${scopeLabel}
                <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px">
                    <div class="dh-stat"><div class="dh-stat-value">${proc.length}</div><div class="dh-stat-label">Procedures</div></div>
                    <div class="dh-stat"><div class="dh-stat-value">${txn.filter(t => t.transactionRequirement.startsWith('REQUIRED')).length}</div><div class="dh-stat-label">Txn Req</div></div>
                    <div class="dh-stat"><div class="dh-stat-value">${batch.length}</div><div class="dh-stat-label">Batch</div></div>
                    <div class="dh-stat"><div class="dh-stat-value">${slice.length}</div><div class="dh-stat-label">Schemas</div></div>
                    <div class="dh-stat"><div class="dh-stat-value">${ext.length}</div><div class="dh-stat-label">Ext Deps</div></div>
                </div>
                <div style="font-size:11px;color:var(--text-muted);margin-bottom:4px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px">${analysisName}</div>

                <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted);margin:12px 0 6px">Select Reports</div>
                <div style="display:flex;flex-direction:column;gap:4px;margin-bottom:12px">
                    <label style="display:flex;align-items:center;gap:6px;font-size:12px"><input type="checkbox" id="exp-procedures" checked> Procedure Report <span style="color:var(--text-muted);margin-left:auto">${proc.length}</span></label>
                    <label style="display:flex;align-items:center;gap:6px;font-size:12px"><input type="checkbox" id="exp-tables" checked> Table Analysis <span style="color:var(--text-muted);margin-left:auto"></span></label>
                    <label style="display:flex;align-items:center;gap:6px;font-size:12px"><input type="checkbox" id="exp-transactions" checked> Transactions <span style="color:var(--text-muted);margin-left:auto">${txn.length}</span></label>
                    <label style="display:flex;align-items:center;gap:6px;font-size:12px"><input type="checkbox" id="exp-batch" checked> Batch Jobs <span style="color:var(--text-muted);margin-left:auto">${batch.length}</span></label>
                    <label style="display:flex;align-items:center;gap:6px;font-size:12px"><input type="checkbox" id="exp-schemaSlice" checked> Schema Slice <span style="color:var(--text-muted);margin-left:auto">${slice.length}</span></label>
                    <label style="display:flex;align-items:center;gap:6px;font-size:12px"><input type="checkbox" id="exp-external" checked> External Dependencies <span style="color:var(--text-muted);margin-left:auto">${ext.length}</span></label>
                </div>

                <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted);margin:12px 0 6px">Format</div>
                <div style="display:flex;gap:6px;margin-bottom:12px">
                    <button class="btn btn-sm" id="exp-fmt-json" onclick="PA.summary._setExportFmt('json')" style="flex:1;text-align:center">
                        <span style="font-weight:700">{ }</span> JSON
                    </button>
                    <button class="btn btn-sm btn-primary" id="exp-fmt-excel" onclick="PA.summary._setExportFmt('excel')" style="flex:1;text-align:center">
                        <span style="font-weight:700">&#9638;</span> Excel (.xlsx)
                    </button>
                </div>

                <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted);margin:12px 0 6px">Filename</div>
                <div style="display:flex;align-items:center;gap:4px;margin-bottom:16px">
                    <input type="text" id="exp-filename" class="form-input" value="" style="flex:1;height:32px">
                    <span id="exp-filename-ext" style="font-weight:600;color:var(--text-muted)">.xlsx</span>
                </div>

                <button class="btn btn-primary" onclick="PA.summary._runExport()" style="width:100%;justify-content:center;padding:10px">Export Selected Reports</button>
            </div>
        </div></div>`;

        document.body.insertAdjacentHTML('beforeend', html);
        this._exportFmt = 'excel';
        this._exportScope = isScoped ? scope : null;
        this._updateExportFilename();
    },

    _exportFmt: 'excel',
    _exportScope: null,

    _setExportFmt(fmt) {
        this._exportFmt = fmt;
        const jsonBtn = document.getElementById('exp-fmt-json');
        const excelBtn = document.getElementById('exp-fmt-excel');
        if (jsonBtn) { jsonBtn.classList.toggle('btn-primary', fmt === 'json'); jsonBtn.classList.toggle('btn-sm', fmt !== 'json'); }
        if (excelBtn) { excelBtn.classList.toggle('btn-primary', fmt === 'excel'); excelBtn.classList.toggle('btn-sm', fmt !== 'excel'); }
        this._updateExportFilename();
    },

    _updateExportFilename() {
        const ext = this._exportFmt === 'json' ? 'json' : 'xlsx';
        const name = (PA.analysisData && PA.analysisData.name) || 'plsql-analysis';
        const baseName = name.replace(/[^a-zA-Z0-9._-]/g, '_') + '_report';
        const input = document.getElementById('exp-filename');
        const extLabel = document.getElementById('exp-filename-ext');
        if (input) input.value = baseName;
        if (extLabel) extLabel.textContent = '.' + ext;
    },

    _getModalFilename() {
        const input = document.getElementById('exp-filename');
        const extLabel = document.getElementById('exp-filename-ext');
        const ext = (extLabel?.textContent || '.xlsx').replace('.', '');
        const raw = (input?.value || 'export').trim();
        const base = raw.endsWith('.' + ext) ? raw.slice(0, -(ext.length + 1)) : raw;
        return base + '.' + ext;
    },

    _runExport() {
        const sel = {
            procedures: document.getElementById('exp-procedures')?.checked,
            tables: document.getElementById('exp-tables')?.checked,
            transactions: document.getElementById('exp-transactions')?.checked,
            batch: document.getElementById('exp-batch')?.checked,
            schemaSlice: document.getElementById('exp-schemaSlice')?.checked,
            external: document.getElementById('exp-external')?.checked
        };

        this._modalFilename = this._getModalFilename();

        if (this._exportFmt === 'json') {
            this._exportSelectedJSON(sel);
        } else {
            this._exportXlsxFrontend(sel);
        }

        this._exportScope = null;
        document.getElementById('export-modal-overlay')?.remove();
    },

    _exportSelectedJSON(sel) {
        const name = (PA.analysisData && PA.analysisData.name) || 'plsql-analysis';
        const setReplacer = (key, value) => value instanceof Set ? [...value] : value;
        const report = {
            _exportedAt: new Date().toISOString(),
            _tool: 'PL/SQL Analyzer',
            analysisName: name,
            summary: {
                totalProcedures: (this._procReports || []).length,
                totalTransactions: (this._txnReport || []).length,
                totalBatch: (this._batchReport || []).length,
                totalExternal: (this._extReport || []).length,
                totalSchemas: (this._schemaSliceReport || []).length
            }
        };
        let sections = 0;
        if (sel.procedures && this._procReports?.length) {
            report.procedures = JSON.parse(JSON.stringify(this._procReports, setReplacer));
            sections++;
        }
        if (sel.transactions && this._txnReport?.length) {
            report.transactions = JSON.parse(JSON.stringify(this._txnReport, setReplacer));
            sections++;
        }
        if (sel.batch && this._batchReport?.length) {
            report.batchJobs = JSON.parse(JSON.stringify(this._batchReport, setReplacer));
            sections++;
        }
        if (sel.external && this._extReport?.length) {
            report.externalDeps = JSON.parse(JSON.stringify(this._extReport, setReplacer));
            sections++;
        }
        if (sel.schemaSlice && this._schemaSliceReport?.length) {
            report.schemaSlice = JSON.parse(JSON.stringify(this._schemaSliceReport, setReplacer));
            sections++;
        }

        const filename = this._modalFilename || (name.replace(/[^a-zA-Z0-9._-]/g, '_') + '_report.json');
        const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
        URL.revokeObjectURL(url);
        PA.toast('Exported JSON with ' + sections + ' sections', 'success');
    }
});
