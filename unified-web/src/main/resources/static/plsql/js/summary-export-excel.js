/**
 * Summary export — styled .xlsx with multiple worksheets via xlsx-js-style.
 * PL/SQL Analyzer: Procedures, Tables, Transactions, Batch, Schema Slice, External Deps.
 *
 * Load order: summary-export-style.js -> this file
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    /** Export config — loaded once from /config/plsql-export-config.json */
    _exportCfg: null,

    async _loadExportConfig() {
        if (this._exportCfg) return this._exportCfg;
        try {
            const r = await fetch('/api/plsql/config/export-config');
            this._exportCfg = r.ok ? await r.json() : {};
        } catch (e) { this._exportCfg = {}; }
        return this._exportCfg;
    },

    /** Sanitize a string value for safe Excel cell insertion */
    _xlsSafe(v) {
        if (typeof v !== 'string') return v;
        if (v.length > 0 && '=+\\-@\t\r'.indexOf(v.charAt(0)) !== -1) return "'" + v;
        return v;
    },

    /** Frontend (browser-side) Excel export using xlsx-js-style */
    async _exportXlsxFrontend(sel) {
        const name = (PA.analysisData && PA.analysisData.name) || 'plsql-analysis';
        PA.toast('Generating Excel report...', 'success');
        try {
            await this._loadExportConfig();
            const wb = XLSX.utils.book_new();
            await this._xlsxSummarySheet(wb);
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

    /* ===== Sheet: Summary (with detail sub-table) ===== */
    async _xlsxSummarySheet(wb) {
        const cfg = this._exportCfg || {};
        const proc = this._procReports || [];
        const txn = this._txnReport || [];
        const batch = this._batchReport || [];
        const ext = this._extReport || [];
        const slice = this._schemaSliceReport || [];
        const tableOps = (PA.analysisData && PA.analysisData.tableOperations) || [];

        // ── Aggregate counts ──────────────────────────────────────────────────
        const allTables = new Set(), allSchemas = new Set();
        let totalLoc = 0, crossSchema = 0;
        for (const r of proc) {
            for (const t of Object.keys(r.flowTables || {})) allTables.add(t);
            for (const s of (r.schemas || [])) allSchemas.add(s);
            totalLoc += r.loc || 0;
            if ((r.crossSchemaCalls || 0) > 0) crossSchema++;
        }

        const txnRequired = txn.filter(t => (t.transactionRequirement || '').startsWith('REQUIRED')).length;

        // Object type counts from tableOperations
        const viewCount = tableOps.filter(t => t.tableType === 'VIEW').length;
        const mvCount   = tableOps.filter(t => t.tableType === 'MATERIALIZED VIEW').length;
        const seqCount  = tableOps.filter(t => t.tableType === 'SEQUENCE').length;
        const tableCount = tableOps.filter(t => !t.tableType || t.tableType === 'TABLE').length;

        // Proc vs Function split
        const procCount = proc.filter(r => (r.unitType || '').toUpperCase() === 'PROCEDURE').length;
        const funcCount = proc.filter(r => (r.unitType || '').toUpperCase() === 'FUNCTION').length;

        // Triggers — count from tableOps
        const triggerCount = tableOps.reduce((sum, t) => sum + ((t.triggers || []).length), 0);

        // Cursors + joins from procReports
        const totalCursors = proc.reduce((s, r) => s + (r.cursorCount || 0), 0);
        const totalJoins   = proc.reduce((s, r) => s + (r.joinCount   || 0), 0);

        // ── Top metric rows ───────────────────────────────────────────────────
        const rows = [
            ['PL/SQL Analysis Summary'],
            [],
            ['Analysis Name',    (PA.analysisData && PA.analysisData.name)            || '-'],
            ['Entry Schema',     (PA.analysisData && PA.analysisData.entrySchema)      || '-'],
            ['Entry Object',     (PA.analysisData && PA.analysisData.entryObjectName)  || '-'],
            ['Entry Procedure',  (PA.analysisData && PA.analysisData.entryProcedure)   || '-'],
            ['Generated At',     new Date().toISOString()],
            [],
            ['Metric',                    'Count'],
            ['Procedures',                procCount],
            ['Functions',                 funcCount],
            ['Tables',                    tableCount],
            ['Views',                     viewCount],
            ['Materialized Views',        mvCount],
            ['Sequences',                 seqCount],
            ['Cursors',                   totalCursors],
            ['Joins',                     totalJoins],
            ['Triggers',                  triggerCount],
            ['Total LOC',                 totalLoc],
            ['Cross-Schema Procedures',   crossSchema],
            ['Transactions Required',     txnRequired],
            ['Batch Jobs',                batch.length],
            ['External Dependencies',     ext.length],
            [],
            // ── Detail sub-table header ──────────────────────────────────────
            ['Procedure / Function', 'Type', 'Query Node ID',
             'Tables', 'Views', 'MVs', 'Seqs', 'Cursors', 'Joins', 'Triggers',
             'LOC', 'Operations', 'Rating', 'Reason']
        ];

        const detailHeaderRow = rows.length - 1; // 0-based index of sub-table header

        // ── Build rating for each proc/func ───────────────────────────────────
        const rCfg = (cfg.complexity_rating) || {};

        // Range-based scorer: find the matching range for a value, return its score
        const rangeScore = (ranges, fallbackRanges, value, key) => {
            const r = (ranges && ranges.length) ? ranges : fallbackRanges;
            for (const band of r) {
                const mn = band['min_' + key] || 0;
                const mx = band['max_' + key] != null ? band['max_' + key] : Infinity;
                if (value >= mn && value <= mx) return band.score || 1;
            }
            return 1;
        };

        const locRanges = (rCfg.loc_thresholds && rCfg.loc_thresholds.ranges) || [];
        const opsRanges = (rCfg.ops_thresholds && rCfg.ops_thresholds.ranges) || [];
        const locFallback = [{ min_loc: 0, max_loc: 99, score: 1 }, { min_loc: 100, max_loc: 299, score: 2 }, { min_loc: 300, max_loc: Infinity, score: 3 }];
        const opsFallback = [{ min_ops: 0, max_ops: 3,  score: 1 }, { min_ops: 4,   max_ops: 9,   score: 2 }, { min_ops: 10,  max_ops: Infinity, score: 3 }];

        const jScore  = (rCfg.join_score    || { per_join: 0.5, max_contribution: 2 });
        const cScore  = (rCfg.cursor_score  || { per_cursor: 0.5, max_contribution: 1.5 });
        const csBonus = (rCfg.cross_schema_bonus || { bonus: 1.5 }).bonus;
        const tBonus  = (rCfg.trigger_bonus || { per_trigger: 0.5, max_contribution: 1.5 });
        const finals  = (rCfg.final_thresholds || { high_min: 6, medium_min: 3 });
        const reasons = rCfg.reasons || {
            high:   'HIGH — {loc} LOC, {ops} operations, {joins} joins, {cursors} cursors{trigger_suffix}{cross_suffix}',
            medium: 'MEDIUM — {loc} LOC, {ops} operations, {joins} joins{trigger_suffix}{cross_suffix}',
            low:    'LOW — {loc} LOC, {ops} operations'
        };

        const fillReason = (tmpl, vals) => tmpl
            .replace('{loc}',       vals.loc)
            .replace('{ops}',       vals.ops)
            .replace('{joins}',     vals.joins)
            .replace('{cursors}',   vals.cursors)
            .replace('{trigger_suffix}', vals.triggers > 0 ? ', ' + vals.triggers + ' triggers' : '')
            .replace('{cross_suffix}',   vals.cross    ? ', cross-schema' : '');

        const detailRows = [];
        for (const r of proc) {
            const loc      = r.loc       || 0;
            const ops      = (r.allOps   || []).length;
            const joins    = r.joinCount    || 0;
            const cursors  = r.cursorCount  || 0;
            const trigs    = tableOps.filter(t => (t.procedures || []).includes(r.name))
                                     .reduce((s, t) => s + (t.triggers || []).length, 0);

            // Score calculation — uses range-based thresholds from config
            const locScore = rangeScore(locRanges, locFallback, loc, 'loc');
            const opsScore = rangeScore(opsRanges, opsFallback, ops, 'ops');
            const joinContrib    = Math.min(joins   * jScore.per_join,    jScore.max_contribution);
            const cursorContrib  = Math.min(cursors * cScore.per_cursor,  cScore.max_contribution);
            const trigContrib    = Math.min(trigs   * tBonus.per_trigger, tBonus.max_contribution);
            const crossContrib   = (r.crossSchemaCalls || 0) > 0 ? csBonus : 0;
            const score = locScore + opsScore + joinContrib + cursorContrib + trigContrib + crossContrib;

            const rating = score >= finals.high_min ? 'HIGH'
                         : score >= finals.medium_min ? 'MEDIUM' : 'LOW';

            const reasonTmpl = rating === 'HIGH' ? reasons.high
                             : rating === 'MEDIUM' ? reasons.medium : reasons.low;
            const reason = fillReason(reasonTmpl, { loc, ops, joins, cursors, triggers: trigs, cross: (r.crossSchemaCalls || 0) > 0 });

            // Counts per proc for table types
            const procTables = Object.values(r.flowTables || {});
            const pTables  = procTables.filter(t => !t.tableType || t.tableType === 'TABLE').length;
            const pViews   = procTables.filter(t => t.tableType === 'VIEW').length;
            const pMVs     = procTables.filter(t => t.tableType === 'MATERIALIZED VIEW').length;
            const pSeqs    = procTables.filter(t => t.tableType === 'SEQUENCE').length;

            // Full name: schema.package.name or schema.name
            const fullName = [r.schemaName, r.packageName, r.name].filter(Boolean).join('.');

            // Query node ID: schema.package.procName (used as stable reference key)
            const queryNodeId = [r.schemaName, r.packageName || r.packageName, r.name].filter(Boolean).join('.');

            detailRows.push([
                this._xlsSafe(fullName),
                r.unitType || 'PROCEDURE',
                this._xlsSafe(queryNodeId),
                pTables, pViews, pMVs, pSeqs,
                cursors, joins, trigs,
                loc,
                (r.allOps || []).join(', '),
                rating,
                this._xlsSafe(reason)
            ]);
        }

        // Sort: HIGH first, then MEDIUM, then LOW
        const order = { HIGH: 0, MEDIUM: 1, LOW: 2 };
        detailRows.sort((a, b) => (order[a[12]] || 2) - (order[b[12]] || 2));

        // Append detail rows
        for (const dr of detailRows) rows.push(dr);

        // ── Column widths for the combined sheet ─────────────────────────────
        const colCfg = (cfg.detail_subtable && cfg.detail_subtable.columns) || [];
        const detailWidths = colCfg.length
            ? colCfg.map(c => ({ wch: c.width || 12 }))
            : [{ wch: 45 }, { wch: 12 }, { wch: 28 }, { wch: 8 }, { wch: 8 }, { wch: 8 }, { wch: 8 }, { wch: 8 }, { wch: 8 }, { wch: 8 }, { wch: 8 }, { wch: 30 }, { wch: 10 }, { wch: 55 }];

        // Use the wider of the two sections
        const summaryWidths = [{ wch: 28 }, { wch: 14 }];
        const colWidths = detailWidths.length > summaryWidths.length ? detailWidths : summaryWidths;

        this._xlsxAddStyledSheet(wb, 'Summary', rows, colWidths, {
            titleRows: [0],
            headerRow: 8,
            extraHeaderRows: [detailHeaderRow],
            conditionalFn: (ws, allRows) => {
                for (let r = detailHeaderRow + 1; r < allRows.length; r++) {
                    const rating = allRows[r][12];
                    if (rating === 'HIGH')   this._xlsxCellFill(ws, r, 12, 'redFill');
                    else if (rating === 'MEDIUM') this._xlsxCellFill(ws, r, 12, 'orangeFill');
                    else if (rating === 'LOW')    this._xlsxCellFill(ws, r, 12, 'greenFill');
                }
            }
        });
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
