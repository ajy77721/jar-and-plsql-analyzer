/**
 * Summary tables — table/grid renderers for all report types.
 * PL/SQL Analyzer: procedures, tables, transactions, schema slices, external deps, batch.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    /** Render overview summary row at top of page */
    _renderSummaryRow(procReports) {
        const esc = PA.esc;
        const allTables = new Set();
        const allSchemas = new Set();
        let crossSchema = 0, totalLoc = 0, writeTables = 0;

        for (const r of procReports) {
            for (const t of Object.keys(r.flowTables || {})) allTables.add(t);
            for (const s of (r.schemas || [])) allSchemas.add(s);
            if (r.crossSchemaCalls > 0) crossSchema++;
            totalLoc += r.loc || 0;
            writeTables += r.writeTableCount || 0;
        }

        const stats = [
            { num: procReports.length, label: 'Procedures', color: 'var(--accent)' },
            { num: allTables.size, label: 'Tables', color: 'var(--teal)' },
            { num: allSchemas.size, label: 'Schemas', color: 'var(--blue)' },
            { num: crossSchema, label: 'Cross-Schema', color: 'var(--red)' },
            { num: totalLoc, label: 'Total LOC', color: 'var(--text)' },
            { num: writeTables, label: 'Write Tables', color: 'var(--orange)' }
        ];

        let html = '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">';
        for (const s of stats) {
            html += `<div class="dh-stat"><div class="dh-stat-value" style="color:${s.color}">${s.num}</div><div class="dh-stat-label">${s.label}</div></div>`;
        }
        html += '</div>';
        return html;
    },

    /** Main procedure table with stats */
    _renderProcedureTable(reports) {
        const esc = PA.esc;
        let html = '';

        // Filter bar
        html += this._buildFilterBar('sum-proc', reports, r => r.schemaName);

        // Pager top
        html += '<div class="pagination-bar" id="sum-proc-pager-top"></div>';

        // Table
        html += '<div style="overflow:auto;flex:1">';
        html += '<table class="to-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-proc\',0)">Procedure</th>';
        html += '<th data-sort-col="1" onclick="PA.summary._pageSort(\'sum-proc\',1)">Schema</th>';
        html += '<th data-sort-col="2" onclick="PA.summary._pageSort(\'sum-proc\',2)">Package</th>';
        html += '<th data-sort-col="3" onclick="PA.summary._pageSort(\'sum-proc\',3)">Type</th>';
        html += '<th data-sort-col="4" onclick="PA.summary._pageSort(\'sum-proc\',4)">Tables</th>';
        html += '<th data-sort-col="5" onclick="PA.summary._pageSort(\'sum-proc\',5)">LOC</th>';
        html += '<th data-sort-col="6" onclick="PA.summary._pageSort(\'sum-proc\',6)">Complexity</th>';
        html += '<th data-sort-col="7" onclick="PA.summary._pageSort(\'sum-proc\',7)">Cross-Schema</th>';
        html += '<th>Operations</th>';
        html += '<th>Actions</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-proc-tbody"></tbody>';
        html += '</table></div>';

        // Pager bottom
        html += '<div class="pagination-bar" id="sum-proc-pager"></div>';

        // Initialize pagination (detail via popup, not inline)
        this._initPage('sum-proc', reports, 25,
            (r, i, esc) => this._renderProcCard(r, i, esc),
            r => r.schemaName,
            null,
            {
                sortKeys: [
                    { fn: r => r.name },
                    { fn: r => r.schemaName },
                    { fn: r => r.packageName },
                    { fn: r => r.unitType },
                    { fn: r => r.tableCount },
                    { fn: r => r.loc },
                    { fn: r => r._complexityScore || 0 },
                    { fn: r => r.crossSchemaCalls }
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-proc');
            this._initColFilters('sum-proc', {
                1: { label: 'Schema', valueFn: r => r.schemaName },
                2: { label: 'Package', valueFn: r => r.packageName || '-' },
                3: { label: 'Type', valueFn: r => r.unitType },
                6: { label: 'Complexity', valueFn: r => r.complexity },
                7: { label: 'Cross-Schema', valueFn: r => r.crossSchemaCalls > 0 ? 'Yes' : 'No' }
            });
        }, 0);

        return html;
    },

    /** Single procedure row */
    _renderProcCard(r, idx, esc) {
        const complexCls = r.complexity === 'High' ? 'red' : r.complexity === 'Medium' ? 'orange' : 'green';
        const unitCls = r.unitType === 'FUNCTION' ? 'F' : r.unitType === 'TRIGGER' ? 'T' : 'P';
        const callBadge = r.callType === 'EXTERNAL'
            ? '<span class="lp-type-badge EXTERNAL">EXT</span>'
            : '<span class="lp-type-badge INTERNAL">INT</span>';

        let html = `<tr class="to-row">`;
        html += `<td><span class="lp-icon ${unitCls}" style="display:inline-flex;margin-right:6px">${unitCls}</span><strong>${esc(r.name)}</strong> ${callBadge}</td>`;
        html += `<td><span style="color:${this._schemaColor(r.schemaName)};font-weight:600">${esc(r.schemaName)}</span></td>`;
        html += `<td>${esc(r.packageName || '-')}</td>`;
        html += `<td>${esc(r.unitType)}</td>`;
        html += `<td><span style="font-weight:700">${r.tableCount}</span> <span style="font-size:10px;color:var(--text-muted)">(${r.writeTableCount}W / ${r.readTableCount}R)</span></td>`;
        html += `<td>${r.loc}</td>`;
        html += `<td><span class="badge" style="background:var(--badge-${complexCls}-bg);color:var(--badge-${complexCls})" title="${esc(this._complexityTip(r.complexity, r._complexityScore))}">${r.complexity}</span></td>`;
        html += `<td>${r.crossSchemaCalls > 0 ? '<span style="color:var(--red);font-weight:700">' + r.crossSchemaCalls + '</span>' : '0'}</td>`;
        html += '<td>';
        for (const op of r.allOps) {
            html += `<span class="op-badge ${op}">${op}</span> `;
        }
        html += '</td>';
        html += '<td style="white-space:nowrap">';
        html += `<button class="sum-proc-details-btn" onclick="PA.summary.showProcPopup(${idx})">Details</button> `;
        html += `<button class="btn btn-sm" onclick="PA.summary.showTrace(${idx})" style="font-size:10px;padding:2px 8px">Trace</button> `;
        html += `<button class="btn btn-sm" onclick="PA.summary.showCallTrace(${idx})" style="font-size:10px;padding:2px 8px">Explore</button>`;
        html += '</td>';
        html += '</tr>';
        return html;
    },

    /** Expandable detail for a procedure */
    _renderProcDetail(r, idx, esc) {
        let html = `<tr class="to-detail-row" id="sum-proc-detail-${idx}" style="display:none"><td colspan="10">`;
        html += '<div class="to-detail" style="padding:16px 24px">';

        // Parameters
        if (r.parameters && r.parameters.length) {
            html += '<div class="to-detail-section" id="proc-params-' + idx + '">';
            html += '<div class="to-detail-section-title">Parameters (' + r.parameters.length + ')</div>';
            for (const p of r.parameters) {
                html += `<div class="dh-param dh-param-${(p.mode || 'IN').toLowerCase().replace(/\s/g, '')}">`;
                html += `<span class="dh-param-mode">${esc(p.mode || 'IN')}</span>`;
                html += `<span class="dh-param-name">${esc(p.name || '?')}</span>`;
                html += `<span class="dh-param-type">${esc(p.dataType || '?')}</span>`;
                html += '</div> ';
            }
            html += '</div>';
        }

        // Direct Tables
        const directKeys = Object.keys(r.directTables || {});
        if (directKeys.length) {
            html += '<div class="to-detail-section" id="proc-tables-' + idx + '">';
            html += '<div class="to-detail-section-title">Direct Tables (' + directKeys.length + ')</div>';
            html += this._scrollSection('proc-direct-tables-' + idx);
            for (const name of directKeys) {
                const t = r.directTables[name];
                const ops = [...(t.operations || [])];
                html += '<div class="sum-scroll-item" style="display:inline-block;margin:2px">';
                html += this._tableBadge(name, esc, ops);
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Flow Tables (full call tree)
        const flowKeys = Object.keys(r.flowTables || {});
        if (flowKeys.length > directKeys.length) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">All Tables in Flow (' + flowKeys.length + ')</div>';
            html += this._scrollSection('proc-flow-tables-' + idx);
            for (const name of flowKeys) {
                const t = r.flowTables[name];
                const ops = [...(t.operations || [])];
                html += '<div class="sum-scroll-item" style="display:inline-block;margin:2px">';
                html += this._tableBadge(name, esc, ops);
                if (t.external) html += ' <span class="scope-badge ext">EXT</span>';
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Calls
        if (r.calls && r.calls.length) {
            html += '<div class="to-detail-section" id="proc-calls-' + idx + '">';
            html += '<div class="to-detail-section-title">Calls (' + r.calls.length + ')</div>';
            html += this._scrollSection('proc-calls-list-' + idx);
            for (const c of r.calls) {
                const badge = c.callType === 'EXTERNAL'
                    ? '<span class="lp-type-badge EXTERNAL" style="margin-right:4px">EXT</span>'
                    : '<span class="lp-type-badge INTERNAL" style="margin-right:4px">INT</span>';
                html += `<div class="sum-scroll-item" style="padding:2px 0;font-size:11px">${badge}`;
                html += `<span style="color:${this._schemaColor(c.schemaName)};font-weight:600">${esc(c.schemaName || '')}</span>`;
                if (c.packageName) html += '.' + esc(c.packageName);
                html += '.<strong>' + esc(c.name || c.id || '?') + '</strong>';
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Called By
        if (r.calledBy && r.calledBy.length) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Called By (' + r.calledBy.length + ')</div>';
            for (const c of r.calledBy) {
                html += `<div style="padding:2px 0;font-size:11px"><span style="font-weight:600">${esc(c.name || c.id || '?')}</span></div>`;
            }
            html += '</div>';
        }

        // Statement counts
        const stmts = r.statementCounts || {};
        const stmtKeys = Object.keys(stmts).filter(k => stmts[k] > 0);
        if (stmtKeys.length) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Statement Counts</div>';
            html += '<div style="display:flex;gap:6px;flex-wrap:wrap">';
            for (const k of stmtKeys) {
                html += `<span class="dh-stmt-badge"><b>${stmts[k]}</b> ${esc(k)}</span>`;
            }
            html += '</div></div>';
        }

        html += '</div></td></tr>';
        return html;
    },

    /** Transaction detection results table */
    _renderTransactionTable(txnReport) {
        const esc = PA.esc;
        let html = '';

        html += this._buildFilterBar('sum-txn', txnReport, r => r.schemaName);
        html += '<div class="pagination-bar" id="sum-txn-pager-top"></div>';

        html += '<div style="overflow:auto;flex:1">';
        html += '<table class="to-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-txn\',0)">Procedure</th>';
        html += '<th data-sort-col="1" onclick="PA.summary._pageSort(\'sum-txn\',1)">Schema</th>';
        html += '<th data-sort-col="2" onclick="PA.summary._pageSort(\'sum-txn\',2)">Transaction Requirement</th>';
        html += '<th data-sort-col="3" onclick="PA.summary._pageSort(\'sum-txn\',3)">Write Tables</th>';
        html += '<th data-sort-col="4" onclick="PA.summary._pageSort(\'sum-txn\',4)">Total Tables</th>';
        html += '<th data-sort-col="5" onclick="PA.summary._pageSort(\'sum-txn\',5)">Complexity</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-txn-tbody"></tbody>';
        html += '</table></div>';

        html += '<div class="pagination-bar" id="sum-txn-pager"></div>';

        this._initPage('sum-txn', txnReport, 25,
            (r, i, esc) => {
                const reqCls = r.transactionRequirement.startsWith('REQUIRED') ? 'red'
                    : r.transactionRequirement.startsWith('RECOMMENDED') ? 'orange' : 'green';
                let row = `<tr class="to-row">`;
                row += `<td><strong>${esc(r.procName)}</strong>${r.packageName ? ' <span style="color:var(--text-muted);font-size:10px">(' + esc(r.packageName) + ')</span>' : ''}</td>`;
                row += `<td><span style="color:${this._schemaColor(r.schemaName)};font-weight:600">${esc(r.schemaName)}</span></td>`;
                row += `<td><span class="badge" style="background:var(--badge-${reqCls}-bg);color:var(--badge-${reqCls})">${esc(r.transactionRequirement)}</span></td>`;
                row += `<td>${r.writeTables.map(t => '<span class="op-badge DELETE">' + esc(t) + '</span>').join(' ') || '-'}</td>`;
                row += `<td>${r.totalTables}</td>`;
                row += `<td>${esc(r.complexity)}</td>`;
                row += '</tr>';
                return row;
            },
            r => r.schemaName,
            null,
            {
                sortKeys: [
                    { fn: r => r.procName },
                    { fn: r => r.schemaName },
                    { fn: r => r.transactionRequirement },
                    { fn: r => r.writeTables.length },
                    { fn: r => r.totalTables },
                    { fn: r => r.complexity === 'High' ? 3 : r.complexity === 'Medium' ? 2 : 1 }
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-txn');
            this._initColFilters('sum-txn', {
                1: { label: 'Schema', valueFn: r => r.schemaName },
                2: { label: 'Txn Requirement', valueFn: r => r.transactionRequirement },
                5: { label: 'Complexity', valueFn: r => r.complexity }
            });
        }, 0);

        return html;
    },

    /** Cross-schema analysis table (paginated) */
    _renderSchemaSliceTable(sliceReport) {
        const esc = PA.esc;
        const data = sliceReport.map(s => ({
            schema: s.schema,
            procCount: s.procedures.size,
            procedures: s.procedures,
            tableCount: s.tables.size,
            tables: s.tables,
            operations: [...(s.operations || [])],
            callCount: s.callCount
        }));

        let html = this._buildFilterBar('sum-schema', data, r => r.schema);
        html += '<div class="pagination-bar" id="sum-schema-pager-top"></div>';
        html += '<div style="overflow:auto;flex:1">';
        html += '<table class="to-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-schema\',0)">Schema</th>';
        html += '<th data-sort-col="1" onclick="PA.summary._pageSort(\'sum-schema\',1)">Procedures</th>';
        html += '<th data-sort-col="2" onclick="PA.summary._pageSort(\'sum-schema\',2)">Tables</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="4" onclick="PA.summary._pageSort(\'sum-schema\',4)">Call Count</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-schema-tbody"></tbody>';
        html += '</table></div>';
        html += '<div class="pagination-bar" id="sum-schema-pager"></div>';

        this._initPage('sum-schema', data, 25,
            (r, i, esc) => {
                let row = '<tr class="to-row">';
                row += `<td><span style="color:${this._schemaColor(r.schema)};font-weight:700;font-size:14px">${esc(r.schema)}</span></td>`;
                row += `<td><strong>${r.procCount}</strong><div style="font-size:10px;color:var(--text-muted);max-width:300px;overflow:hidden;text-overflow:ellipsis">${[...r.procedures].slice(0, 5).map(p => esc(p)).join(', ')}${r.procCount > 5 ? '...' : ''}</div></td>`;
                row += `<td><strong>${r.tableCount}</strong><div style="font-size:10px;color:var(--text-muted);max-width:300px;overflow:hidden;text-overflow:ellipsis">${[...r.tables].slice(0, 5).map(t => esc(t)).join(', ')}${r.tableCount > 5 ? '...' : ''}</div></td>`;
                row += '<td>';
                for (const op of r.operations) row += `<span class="op-badge ${op}">${op}</span> `;
                row += '</td>';
                row += `<td><strong>${r.callCount}</strong></td>`;
                row += '</tr>';
                return row;
            },
            r => r.schema,
            null,
            {
                sortKeys: [
                    { fn: r => r.schema },
                    { fn: r => r.procCount },
                    { fn: r => r.tableCount },
                    null,
                    { fn: r => r.callCount }
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-schema');
            this._initColFilters('sum-schema', {
                0: { label: 'Schema', valueFn: r => r.schema }
            });
        }, 0);

        return html;
    },

    /** External dependencies table (paginated) */
    _renderExternalTable(extReport) {
        const esc = PA.esc;
        if (!extReport || !extReport.length) {
            return '<div style="padding:24px;text-align:center;color:var(--text-muted)">No external schema dependencies detected.</div>';
        }

        const data = extReport.map(ext => ({
            schema: ext.schema,
            procCount: ext.procedures.size,
            procedures: ext.procedures,
            packages: ext.packages,
            callerCount: ext.callers.size,
            callers: ext.callers,
            count: ext.count
        }));

        let html = this._buildFilterBar('sum-ext', data, r => r.schema);
        html += '<div class="pagination-bar" id="sum-ext-pager-top"></div>';
        html += '<div style="overflow:auto;flex:1">';
        html += '<table class="to-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-ext\',0)">External Schema</th>';
        html += '<th data-sort-col="1" onclick="PA.summary._pageSort(\'sum-ext\',1)">Procedures Called</th>';
        html += '<th>Packages</th>';
        html += '<th data-sort-col="3" onclick="PA.summary._pageSort(\'sum-ext\',3)">Called By</th>';
        html += '<th data-sort-col="4" onclick="PA.summary._pageSort(\'sum-ext\',4)">Total Calls</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-ext-tbody"></tbody>';
        html += '</table></div>';
        html += '<div class="pagination-bar" id="sum-ext-pager"></div>';

        this._initPage('sum-ext', data, 25,
            (r, i, esc) => {
                let row = '<tr class="to-row">';
                row += `<td><span style="color:${this._schemaColor(r.schema)};font-weight:700;font-size:14px">${esc(r.schema)}</span></td>`;
                row += `<td><strong>${r.procCount}</strong><div style="font-size:10px;color:var(--text-muted)">${[...r.procedures].slice(0, 5).map(p => esc(p)).join(', ')}</div></td>`;
                row += `<td>${[...r.packages].map(p => '<span class="badge">' + esc(p) + '</span>').join(' ') || '-'}</td>`;
                row += `<td><strong>${r.callerCount}</strong><div style="font-size:10px;color:var(--text-muted)">${[...r.callers].slice(0, 5).map(c => esc(c)).join(', ')}</div></td>`;
                row += `<td><strong>${r.count}</strong></td>`;
                row += '</tr>';
                return row;
            },
            r => r.schema,
            null,
            {
                sortKeys: [
                    { fn: r => r.schema },
                    { fn: r => r.procCount },
                    null,
                    { fn: r => r.callerCount },
                    { fn: r => r.count }
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-ext');
            this._initColFilters('sum-ext', {
                0: { label: 'Schema', valueFn: r => r.schema }
            });
        }, 0);

        return html;
    },

    /** Batch job detection table (paginated) */
    _renderBatchTable(batchReport) {
        const esc = PA.esc;
        if (!batchReport || !batchReport.length) {
            return '<div style="padding:24px;text-align:center;color:var(--text-muted)">No batch/scheduler patterns detected (DBMS_SCHEDULER, DBMS_JOB, etc.)</div>';
        }

        let html = this._buildFilterBar('sum-batch', batchReport, r => r.schemaName);
        html += '<div class="pagination-bar" id="sum-batch-pager-top"></div>';
        html += '<div style="overflow:auto;flex:1">';
        html += '<table class="to-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-batch\',0)">Procedure</th>';
        html += '<th data-sort-col="1" onclick="PA.summary._pageSort(\'sum-batch\',1)">Schema</th>';
        html += '<th data-sort-col="2" onclick="PA.summary._pageSort(\'sum-batch\',2)">Package</th>';
        html += '<th data-sort-col="3" onclick="PA.summary._pageSort(\'sum-batch\',3)">Matched Pattern</th>';
        html += '<th data-sort-col="4" onclick="PA.summary._pageSort(\'sum-batch\',4)">Tables</th>';
        html += '<th>Write Tables</th>';
        html += '<th data-sort-col="6" onclick="PA.summary._pageSort(\'sum-batch\',6)">LOC</th>';
        html += '<th data-sort-col="7" onclick="PA.summary._pageSort(\'sum-batch\',7)">Complexity</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-batch-tbody"></tbody>';
        html += '</table></div>';
        html += '<div class="pagination-bar" id="sum-batch-pager"></div>';

        this._initPage('sum-batch', batchReport, 25,
            (b, i, esc) => {
                let row = '<tr class="to-row">';
                row += `<td><strong>${esc(b.procName)}</strong></td>`;
                row += `<td><span style="color:${this._schemaColor(b.schemaName)};font-weight:600">${esc(b.schemaName)}</span></td>`;
                row += `<td>${esc(b.packageName || '-')}</td>`;
                row += `<td><span class="badge" style="background:var(--badge-orange-bg);color:var(--badge-orange)">${esc(b.matchedKeyword)}</span></td>`;
                row += `<td>${b.tableCount}</td>`;
                row += `<td>${b.writeTables.map(t => '<span class="op-badge DELETE">' + esc(t) + '</span>').join(' ') || '-'}</td>`;
                row += `<td>${b.loc}</td>`;
                row += `<td>${esc(b.complexity)}</td>`;
                row += '</tr>';
                return row;
            },
            r => r.schemaName,
            null,
            {
                sortKeys: [
                    { fn: r => r.procName },
                    { fn: r => r.schemaName },
                    { fn: r => r.packageName },
                    { fn: r => r.matchedKeyword },
                    { fn: r => r.tableCount },
                    null,
                    { fn: r => r.loc },
                    { fn: r => r.complexity === 'High' ? 3 : r.complexity === 'Medium' ? 2 : 1 }
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-batch');
            this._initColFilters('sum-batch', {
                1: { label: 'Schema', valueFn: r => r.schemaName },
                3: { label: 'Pattern', valueFn: r => r.matchedKeyword },
                7: { label: 'Complexity', valueFn: r => r.complexity }
            });
        }, 0);

        return html;
    },

    /** Render table analysis (all tables from all procedures) */
    _renderTableAnalysis(procReports) {
        const esc = PA.esc;
        // Build consolidated table map
        const tableMap = {};
        for (const r of procReports) {
            for (const [name, t] of Object.entries(r.flowTables || {})) {
                if (!tableMap[name]) {
                    tableMap[name] = {
                        name, schemaName: t.schemaName || r.schemaName,
                        operations: new Set(), procedures: new Set(),
                        external: t.external || false, usageCount: 0
                    };
                }
                for (const op of (t.operations || new Set())) tableMap[name].operations.add(op);
                tableMap[name].procedures.add(r.name);
                tableMap[name].usageCount++;
            }
        }

        const tables = Object.values(tableMap).sort((a, b) => b.usageCount - a.usageCount);

        let html = this._buildFilterBar('sum-tbl', tables, t => t.schemaName);
        html += '<div class="pagination-bar" id="sum-tbl-pager-top"></div>';

        html += '<div style="overflow:auto;flex:1">';
        html += '<table class="to-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-tbl\',0)">Table</th>';
        html += '<th data-sort-col="1" onclick="PA.summary._pageSort(\'sum-tbl\',1)">Schema</th>';
        html += '<th>Operations</th>';
        html += '<th data-sort-col="3" onclick="PA.summary._pageSort(\'sum-tbl\',3)">Used By</th>';
        html += '<th data-sort-col="4" onclick="PA.summary._pageSort(\'sum-tbl\',4)">Usage Count</th>';
        html += '<th>Scope</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-tbl-tbody"></tbody>';
        html += '</table></div>';
        html += '<div class="pagination-bar" id="sum-tbl-pager"></div>';

        this._initPage('sum-tbl', tables, 25,
            (t, i, esc) => {
                let row = '<tr class="to-row">';
                row += `<td><strong style="font-family:var(--font-mono)">${esc(t.name)}</strong></td>`;
                row += `<td><span style="color:${this._schemaColor(t.schemaName)};font-weight:600">${esc(t.schemaName || '-')}</span></td>`;
                row += '<td>';
                for (const op of t.operations) row += `<span class="op-badge ${op}">${op}</span> `;
                row += '</td>';
                row += `<td><strong>${t.procedures.size}</strong><div style="font-size:10px;color:var(--text-muted)">${[...t.procedures].slice(0, 3).map(p => esc(p)).join(', ')}${t.procedures.size > 3 ? '...' : ''}</div></td>`;
                row += `<td>${t.usageCount}</td>`;
                row += `<td>${t.external ? '<span class="scope-badge ext">EXT</span>' : '<span class="scope-badge int">INT</span>'}</td>`;
                row += '</tr>';
                return row;
            },
            t => t.schemaName,
            null,
            {
                sortKeys: [
                    { fn: t => t.name },
                    { fn: t => t.schemaName },
                    null,
                    { fn: t => t.procedures.size },
                    { fn: t => t.usageCount }
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-tbl');
            this._initColFilters('sum-tbl', {
                1: { label: 'Schema', valueFn: t => t.schemaName || '-' }
            });
        }, 0);

        return html;
    }
});
