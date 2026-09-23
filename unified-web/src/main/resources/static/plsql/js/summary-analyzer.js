/**
 * Summary analyzer — procedure analysis + report builders.
 * Produces: procReports, tableReport, txnReport, schemaSliceReport, extReport, batchReport.
 *
 * PL/SQL Analyzer adaptation: walks call graphs to build per-procedure reports.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    /**
     * Analyze all procedures from the call graph.
     * @param {Object} analysisData - PA.analysisData
     * @returns {Array} procReports
     */
    async _analyzeAllProcedures(analysisData) {
        if (!analysisData || !analysisData.callGraph) return [];
        const reports = [];
        const graph = analysisData.callGraph;
        const nodes = graph.nodes || graph.procedures || [];

        for (const node of nodes) {
            const procId = node.id || node.name;
            if (!procId) continue;
            try {
                const detail = await PA.api.getProcDetail(procId);
                if (detail) {
                    const r = this._analyzeProcedure(detail, analysisData);
                    if (r) reports.push(r);
                }
            } catch (e) {
                // Build minimal report from graph node
                reports.push(this._minimalReport(node));
            }
        }
        return reports;
    },

    /** Build per-procedure report from detail response */
    _analyzeProcedure(detail, analysisData) {
        if (!detail) return null;
        const esc = PA.esc;
        const entrySchema = (analysisData && analysisData.entrySchema || '').toUpperCase();

        // Direct tables from this procedure
        const directTables = {};
        for (const t of (detail.tables || [])) {
            const name = (t.tableName || t).toUpperCase();
            if (!directTables[name]) {
                directTables[name] = {
                    name, schemaName: t.schemaName || '', operations: new Set(),
                    external: t.external || false
                };
            }
            for (const op of (t.operations || [])) directTables[name].operations.add(op.toUpperCase());
        }

        // Full flow tables (from call tree)
        const flowTables = {};
        if (detail.callTree) {
            this._walkWithPath(detail.callTree, (node) => {
                const tables = node.nodeTables || node.tables || [];
                for (const t of tables) {
                    const name = (t.tableName || t).toUpperCase();
                    if (!flowTables[name]) {
                        flowTables[name] = {
                            name, schemaName: t.schemaName || '', operations: new Set(),
                            external: t.external || false, sources: new Set()
                        };
                    }
                    for (const op of (t.operations || [])) flowTables[name].operations.add(op.toUpperCase());
                    flowTables[name].sources.add(node.name || node.id || '?');
                }
            });
        }

        // Merge direct tables into flow tables
        for (const [name, t] of Object.entries(directTables)) {
            if (!flowTables[name]) {
                flowTables[name] = { ...t, sources: new Set([detail.name || '']) };
            } else {
                for (const op of t.operations) flowTables[name].operations.add(op);
            }
        }

        // Count cross-schema calls
        let crossSchemaCalls = 0;
        let totalCalls = 0;
        let callDepth = 0;
        const schemas = new Set();
        schemas.add((detail.schemaName || '').toUpperCase());

        if (detail.callTree) {
            this._walkWithPath(detail.callTree, (node, path) => {
                totalCalls++;
                if (path.length - 1 > callDepth) callDepth = path.length - 1;
                const nodeSchema = (node.schemaName || '').toUpperCase();
                if (nodeSchema) schemas.add(nodeSchema);
                if (node.callType === 'EXTERNAL' || (nodeSchema && nodeSchema !== entrySchema && entrySchema)) {
                    crossSchemaCalls++;
                }
            });
        }

        // Count calls from detail
        const internalCalls = (detail.calls || []).filter(c => c.callType !== 'EXTERNAL').length;
        const externalCalls = (detail.calls || []).filter(c => c.callType === 'EXTERNAL').length;

        // LOC
        const loc = (detail.endLine && detail.startLine) ? (detail.endLine - detail.startLine + 1) : 0;

        // Write tables
        const writeTables = Object.values(flowTables).filter(t =>
            t.operations.has('INSERT') || t.operations.has('UPDATE') || t.operations.has('DELETE') || t.operations.has('MERGE')
        );
        const readTables = Object.values(flowTables).filter(t => t.operations.has('SELECT'));

        // Build ops set
        const allOps = new Set();
        for (const t of Object.values(flowTables)) {
            for (const op of t.operations) allOps.add(op);
        }

        const report = {
            id: detail.id || detail.name,
            name: detail.name,
            schemaName: detail.schemaName || '',
            packageName: detail.packageName || '',
            unitType: detail.unitType || 'PROCEDURE',
            callType: detail.callType || 'INTERNAL',
            startLine: detail.startLine || 0,
            endLine: detail.endLine || 0,
            loc,
            directTables,
            flowTables,
            tableCount: Object.keys(flowTables).length,
            writeTableCount: writeTables.length,
            readTableCount: readTables.length,
            writeTables: writeTables.map(t => t.name),
            readTables: readTables.map(t => t.name),
            allOps: [...allOps],
            schemas: [...schemas],
            crossSchemaCalls,
            totalCalls,
            callDepth,
            internalCalls,
            externalCalls,
            parameters: detail.parameters || [],
            variables: detail.variables || [],
            statementCounts: detail.statementCounts || {},
            calledBy: detail.calledBy || [],
            calls: detail.calls || [],
            sizeCategory: this._sizeCategory(loc),
            complexity: 'Low'
        };
        report.complexity = this._calcComplexity(report);
        return report;
    },

    /** Minimal report from a call graph node when detail is unavailable */
    _minimalReport(node) {
        return {
            id: node.id || node.name,
            name: node.name || node.id || '?',
            schemaName: node.schemaName || '',
            packageName: node.packageName || '',
            unitType: node.unitType || 'PROCEDURE',
            callType: node.callType || 'INTERNAL',
            startLine: 0, endLine: 0, loc: 0,
            directTables: {}, flowTables: {},
            tableCount: 0, writeTableCount: 0, readTableCount: 0,
            writeTables: [], readTables: [],
            allOps: [], schemas: [node.schemaName || ''],
            crossSchemaCalls: 0, totalCalls: 0, callDepth: 0,
            internalCalls: 0, externalCalls: 0,
            parameters: [], variables: [], statementCounts: {},
            calledBy: [], calls: [],
            sizeCategory: 'S', complexity: 'Low'
        };
    },

    /** Detect multi-table DML in same call tree (transaction analysis) */
    _buildTransactionReport(procReports) {
        const results = [];
        for (const r of procReports) {
            const schemaWrites = {};
            for (const [name, t] of Object.entries(r.flowTables)) {
                const hasWrite = t.operations.has('INSERT') || t.operations.has('UPDATE') || t.operations.has('DELETE') || t.operations.has('MERGE');
                if (hasWrite) {
                    const schema = t.schemaName || r.schemaName || 'DEFAULT';
                    (schemaWrites[schema] = schemaWrites[schema] || []).push(name);
                }
            }
            const writeSchemas = Object.keys(schemaWrites);
            const allWriteTables = Object.values(schemaWrites).flat();
            const txnReq = allWriteTables.length > 1
                ? (writeSchemas.length > 1
                    ? 'REQUIRED - cross-schema multi-table DML (' + writeSchemas.join(', ') + ')'
                    : 'RECOMMENDED - multi-table DML in single schema')
                : allWriteTables.length === 1 ? 'SINGLE TABLE' : 'READ ONLY';

            results.push({
                procName: r.name, procId: r.id,
                schemaName: r.schemaName, packageName: r.packageName,
                unitType: r.unitType,
                transactionRequirement: txnReq,
                writeSchemas, writeTables: allWriteTables,
                totalTables: r.tableCount,
                loc: r.loc, complexity: r.complexity
            });
        }
        return results;
    },

    /** Cross-schema analysis (equivalent to JAR verticalisation) */
    _buildSchemaSliceReport(procReports) {
        const schemaMap = {};
        for (const r of procReports) {
            for (const schema of r.schemas) {
                if (!schemaMap[schema]) {
                    schemaMap[schema] = {
                        schema, procedures: new Set(), tables: new Set(),
                        operations: new Set(), callCount: 0, procReports: []
                    };
                }
                schemaMap[schema].procedures.add(r.name);
                schemaMap[schema].callCount++;
                schemaMap[schema].procReports.push(r);
                for (const [tName, t] of Object.entries(r.flowTables)) {
                    if ((t.schemaName || '').toUpperCase() === schema || !t.schemaName) {
                        schemaMap[schema].tables.add(tName);
                        for (const op of t.operations) schemaMap[schema].operations.add(op);
                    }
                }
            }
        }
        return Object.values(schemaMap).sort((a, b) => b.callCount - a.callCount);
    },

    /** External schema dependencies */
    _buildExternalDepsReport(procReports) {
        const extMap = {};
        for (const r of procReports) {
            for (const call of (r.calls || [])) {
                if (call.callType !== 'EXTERNAL') continue;
                const schema = (call.schemaName || 'EXTERNAL').toUpperCase();
                if (!extMap[schema]) {
                    extMap[schema] = {
                        schema, procedures: new Set(), callers: new Set(),
                        packages: new Set(), count: 0
                    };
                }
                extMap[schema].procedures.add(call.name || call.id || '?');
                extMap[schema].callers.add(r.name);
                if (call.packageName) extMap[schema].packages.add(call.packageName);
                extMap[schema].count++;
            }
        }
        return Object.values(extMap).sort((a, b) => b.count - a.count);
    },

    /** Detect DBMS_SCHEDULER/DBMS_JOB patterns */
    _buildBatchReport(procReports) {
        const batchKeywords = ['DBMS_SCHEDULER', 'DBMS_JOB', 'SCHEDULE', 'CRON', 'BATCH', 'NIGHTLY', 'DAILY', 'WEEKLY'];
        const results = [];
        for (const r of procReports) {
            const text = ((r.name || '') + ' ' + (r.packageName || '')).toUpperCase();
            let isBatch = false;
            let matchedKeyword = '';
            for (const kw of batchKeywords) {
                if (text.includes(kw)) { isBatch = true; matchedKeyword = kw; break; }
            }
            // Also check calls for DBMS_SCHEDULER/DBMS_JOB
            if (!isBatch) {
                for (const call of (r.calls || [])) {
                    const callText = ((call.name || '') + ' ' + (call.packageName || '')).toUpperCase();
                    for (const kw of batchKeywords) {
                        if (callText.includes(kw)) { isBatch = true; matchedKeyword = kw; break; }
                    }
                    if (isBatch) break;
                }
            }
            if (!isBatch) continue;
            results.push({
                procName: r.name, procId: r.id,
                schemaName: r.schemaName, packageName: r.packageName,
                matchedKeyword,
                tableCount: r.tableCount, writeTables: r.writeTables,
                loc: r.loc, complexity: r.complexity,
                schemas: r.schemas
            });
        }
        return results;
    }
});
