/**
 * Summary export — styled .xlsx with multiple worksheets via xlsx-js-style.
 * PL/SQL Analyzer: Procedures, Tables, Transactions, Batch, Schema Slice, External Deps.
 *
 * Load order: summary-export-style.js -> this file
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    /** Sanitize a string value for safe Excel cell insertion */
    _xlsSafe(v) {
        if (typeof v !== 'string') return v;
        if (v.length > 0 && '=+\\-@\t\r'.indexOf(v.charAt(0)) !== -1) return "'" + v;
        return v;
    },

    /** Frontend (browser-side) Excel export using xlsx-js-style */
    _exportXlsxFrontend(sel) {
        const name = (PA.analysisData && PA.analysisData.name) || 'plsql-analysis';
        PA.toast('Generating Excel report...', 'success');
        try {
            const wb = XLSX.utils.book_new();
            this._xlsxSummarySheet(wb);
            if (sel.procedures) this._xlsxProceduresSheet(wb);
            if (sel.procedures) this._xlsxProcTablesSheet(wb);
            if (sel.tables) this._xlsxTablesSheet(wb);
            if (sel.tables) this._xlsxTableSummarySheet(wb);
            if (sel.transactions) this._xlsxTransactionsSheet(wb);
            if (sel.batch) this._xlsxBatchSheet(wb);
            if (sel.schemaSlice) this._xlsxSchemaSliceSheet(wb);
            if (sel.external) this._xlsxExternalSheet(wb);
            const filename = this._modalFilename || (name.replace(/[^a-zA-Z0-9._-]/g, '_') + '_report.xlsx');
            XLSX.writeFile(wb, filename, { cellStyles: true });
            PA.toast('Excel report downloaded', 'success');
        } catch (err) {
            console.error('Frontend Excel export failed:', err);
            PA.toast('Excel export failed: ' + err.message, 'error');
        }
    },

    /* ===== Sheet: Summary ===== */
    _xlsxSummarySheet(wb) {
        const proc = this._procReports || [];
        const txn = this._txnReport || [];
        const batch = this._batchReport || [];
        const ext = this._extReport || [];
        const slice = this._schemaSliceReport || [];

        const allTables = new Set();
        const allSchemas = new Set();
        let totalLoc = 0, crossSchema = 0;
        for (const r of proc) {
            for (const t of Object.keys(r.flowTables || {})) allTables.add(t);
            for (const s of (r.schemas || [])) allSchemas.add(s);
            totalLoc += r.loc || 0;
            if (r.crossSchemaCalls > 0) crossSchema++;
        }

        const txnRequired = txn.filter(t => t.transactionRequirement.startsWith('REQUIRED')).length;

        const rows = [
            ['PL/SQL Analysis Summary'],
            [],
            ['Analysis Name', (PA.analysisData && PA.analysisData.name) || '-'],
            ['Entry Schema', (PA.analysisData && PA.analysisData.entrySchema) || '-'],
            ['Entry Object', (PA.analysisData && PA.analysisData.entryObjectName) || '-'],
            ['Entry Procedure', (PA.analysisData && PA.analysisData.entryProcedure) || '-'],
            ['Generated At', new Date().toISOString()],
            [],
            ['Metric', 'Count'],
            ['Procedures Analyzed', proc.length],
            ['Total Tables', allTables.size],
            ['Total Schemas', allSchemas.size],
            ['Cross-Schema Procedures', crossSchema],
            ['Total LOC', totalLoc],
            ['Transaction Required', txnRequired],
            ['Batch Jobs Detected', batch.length],
            ['External Dependencies', ext.length],
            ['Schema Slices', slice.length]
        ];

        this._xlsxAddStyledSheet(wb, 'Summary', rows,
            [{ wch: 25 }, { wch: 40 }],
            { titleRows: [0], headerRow: 8 }
        );
    },

    /* ===== Sheet: Procedures ===== */
    _xlsxProceduresSheet(wb) {
        const proc = this._procReports || [];
        const rows = [
            ['Procedure', 'Schema', 'Package', 'Unit Type', 'Call Type', 'Tables', 'Write Tables', 'Read Tables', 'LOC', 'Complexity', 'Cross-Schema Calls', 'Operations', 'Size Category']
        ];
        for (const r of proc) {
            rows.push([
                this._xlsSafe(r.name), this._xlsSafe(r.schemaName), this._xlsSafe(r.packageName || ''),
                r.unitType, r.callType,
                r.tableCount, r.writeTableCount, r.readTableCount,
                r.loc, r.complexity, r.crossSchemaCalls,
                r.allOps.join(', '), r.sizeCategory
            ]);
        }
        this._xlsxAddStyledSheet(wb, 'Procedures', rows,
            [{ wch: 35 }, { wch: 15 }, { wch: 20 }, { wch: 12 }, { wch: 10 }, { wch: 8 }, { wch: 10 }, { wch: 10 }, { wch: 8 }, { wch: 12 }, { wch: 14 }, { wch: 25 }, { wch: 10 }],
            {
                conditionalFn: (ws, rows) => {
                    for (let r = 1; r < rows.length; r++) {
                        const complexity = rows[r][9];
                        if (complexity === 'High') this._xlsxCellFill(ws, r, 9, 'redFill');
                        else if (complexity === 'Medium') this._xlsxCellFill(ws, r, 9, 'orangeFill');
                        else this._xlsxCellFill(ws, r, 9, 'greenFill');
                    }
                }
            }
        );
    },

    /* ===== Sheet: Procedure-Tables ===== */
    _xlsxProcTablesSheet(wb) {
        const proc = this._procReports || [];
        const rows = [
            ['Procedure', 'Schema', 'Table Name', 'Table Schema', 'Operations', 'Scope']
        ];
        for (const r of proc) {
            for (const [name, t] of Object.entries(r.flowTables || {})) {
                rows.push([
                    this._xlsSafe(r.name), this._xlsSafe(r.schemaName),
                    this._xlsSafe(name), this._xlsSafe(t.schemaName || ''),
                    [...(t.operations || [])].join(', '),
                    t.external ? 'EXTERNAL' : 'INTERNAL'
                ]);
            }
        }
        this._xlsxAddStyledSheet(wb, 'Procedure-Tables', rows,
            [{ wch: 35 }, { wch: 15 }, { wch: 35 }, { wch: 15 }, { wch: 25 }, { wch: 12 }]
        );
    },

    /* ===== Sheet: Tables ===== */
    _xlsxTablesSheet(wb) {
        const proc = this._procReports || [];
        const tableMap = {};
        for (const r of proc) {
            for (const [name, t] of Object.entries(r.flowTables || {})) {
                if (!tableMap[name]) {
                    tableMap[name] = { name, schemaName: t.schemaName || r.schemaName, operations: new Set(), procedures: new Set(), external: t.external, usageCount: 0 };
                }
                for (const op of (t.operations || new Set())) tableMap[name].operations.add(op);
                tableMap[name].procedures.add(r.name);
                tableMap[name].usageCount++;
            }
        }

        const rows = [['Table', 'Schema', 'Operations', 'Procedures Using', 'Usage Count', 'Scope']];
        for (const t of Object.values(tableMap).sort((a, b) => b.usageCount - a.usageCount)) {
            rows.push([
                this._xlsSafe(t.name), this._xlsSafe(t.schemaName || ''),
                [...t.operations].join(', '),
                [...t.procedures].join(', '),
                t.usageCount,
                t.external ? 'EXTERNAL' : 'INTERNAL'
            ]);
        }
        this._xlsxAddStyledSheet(wb, 'Tables', rows,
            [{ wch: 35 }, { wch: 15 }, { wch: 25 }, { wch: 50 }, { wch: 12 }, { wch: 12 }]
        );
    },

    /* ===== Sheet: Table Summary ===== */
    _xlsxTableSummarySheet(wb) {
        const data = PA.analysisData;
        const tableOps = (data && data.tableOperations) || [];
        const rows = [['Table', 'Schema', 'Table Type', 'Operations', 'External', 'Trigger Count']];
        for (const t of tableOps) {
            rows.push([
                this._xlsSafe(t.tableName), this._xlsSafe(t.schemaName || ''),
                t.tableType || 'TABLE',
                (t.operations || []).join(', '),
                t.external ? 'YES' : 'NO',
                (t.triggers || []).length
            ]);
        }
        this._xlsxAddStyledSheet(wb, 'Table Summary', rows,
            [{ wch: 35 }, { wch: 15 }, { wch: 12 }, { wch: 30 }, { wch: 10 }, { wch: 12 }]
        );
    },

    /* ===== Sheet: Transactions ===== */
    _xlsxTransactionsSheet(wb) {
        const txn = this._txnReport || [];
        const rows = [['Procedure', 'Schema', 'Package', 'Transaction Requirement', 'Write Tables', 'Total Tables', 'LOC', 'Complexity']];
        for (const t of txn) {
            rows.push([
                this._xlsSafe(t.procName), this._xlsSafe(t.schemaName),
                this._xlsSafe(t.packageName || ''),
                t.transactionRequirement,
                t.writeTables.join(', '),
                t.totalTables, t.loc, t.complexity
            ]);
        }
        this._xlsxAddStyledSheet(wb, 'Transactions', rows,
            [{ wch: 35 }, { wch: 15 }, { wch: 20 }, { wch: 45 }, { wch: 40 }, { wch: 10 }, { wch: 8 }, { wch: 12 }],
            {
                conditionalFn: (ws, rows) => {
                    for (let r = 1; r < rows.length; r++) {
                        const req = rows[r][3] || '';
                        if (req.startsWith('REQUIRED')) this._xlsxCellFill(ws, r, 3, 'redFill');
                        else if (req.startsWith('RECOMMENDED')) this._xlsxCellFill(ws, r, 3, 'orangeFill');
                    }
                }
            }
        );
    },

    /* ===== Sheet: Batch Jobs ===== */
    _xlsxBatchSheet(wb) {
        const batch = this._batchReport || [];
        const rows = [['Procedure', 'Schema', 'Package', 'Matched Pattern', 'Tables', 'Write Tables', 'LOC', 'Complexity']];
        for (const b of batch) {
            rows.push([
                this._xlsSafe(b.procName), this._xlsSafe(b.schemaName),
                this._xlsSafe(b.packageName || ''),
                b.matchedKeyword,
                b.tableCount, b.writeTables.join(', '),
                b.loc, b.complexity
            ]);
        }
        this._xlsxAddStyledSheet(wb, 'Batch Jobs', rows,
            [{ wch: 35 }, { wch: 15 }, { wch: 20 }, { wch: 20 }, { wch: 8 }, { wch: 40 }, { wch: 8 }, { wch: 12 }]
        );
    },

    /* ===== Sheet: Schema Slice ===== */
    _xlsxSchemaSliceSheet(wb) {
        const slice = this._schemaSliceReport || [];
        const rows = [['Schema', 'Procedures', 'Tables', 'Operations', 'Call Count']];
        for (const s of slice) {
            rows.push([
                this._xlsSafe(s.schema),
                [...s.procedures].join(', '),
                [...s.tables].join(', '),
                [...s.operations].join(', '),
                s.callCount
            ]);
        }
        this._xlsxAddStyledSheet(wb, 'Schema Slice', rows,
            [{ wch: 20 }, { wch: 50 }, { wch: 50 }, { wch: 25 }, { wch: 12 }]
        );
    },

    /* ===== Sheet: External Dependencies ===== */
    _xlsxExternalSheet(wb) {
        const ext = this._extReport || [];
        const rows = [['External Schema', 'Procedures Called', 'Packages', 'Called By', 'Total Calls']];
        for (const e of ext) {
            rows.push([
                this._xlsSafe(e.schema),
                [...e.procedures].join(', '),
                [...e.packages].join(', '),
                [...e.callers].join(', '),
                e.count
            ]);
        }
        this._xlsxAddStyledSheet(wb, 'External Deps', rows,
            [{ wch: 20 }, { wch: 50 }, { wch: 30 }, { wch: 50 }, { wch: 12 }]
        );
    }
});
