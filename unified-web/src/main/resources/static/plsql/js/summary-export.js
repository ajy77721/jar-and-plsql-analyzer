/**
 * Summary export — JSON export/import orchestrator.
 * PL/SQL Analyzer version.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    exportAllReports() {
        if (!this._procReports) { PA.toast('No analysis loaded', 'error'); return; }
        const name = ((PA.analysisData && PA.analysisData.name) || 'plsql-analysis').replace(/[^a-zA-Z0-9._-]/g, '_');
        const setReplacer = (key, value) => value instanceof Set ? [...value] : value;

        this._downloadJSON(name + '_procedures.json', JSON.parse(JSON.stringify(this._procReports || [], setReplacer)));
        this._downloadJSON(name + '_transactions.json', JSON.parse(JSON.stringify(this._txnReport || [], setReplacer)));
        this._downloadJSON(name + '_batch.json', JSON.parse(JSON.stringify(this._batchReport || [], setReplacer)));
        this._downloadJSON(name + '_schema_slice.json', JSON.parse(JSON.stringify(this._schemaSliceReport || [], setReplacer)));
        this._downloadJSON(name + '_external_deps.json', JSON.parse(JSON.stringify(this._extReport || [], setReplacer)));
        PA.toast('Exported 5 report files', 'success');
    },

    exportJSON() {
        const data = PA.analysisData;
        if (!data) { PA.toast('No analysis loaded', 'error'); return; }
        const key = 'PA-' + Date.now().toString(36) + '-' + Math.random().toString(36).substring(2, 8);
        const report = {
            _key: key,
            _exportedAt: new Date().toISOString(),
            _tool: 'PL/SQL Analyzer',
            name: data.name,
            entrySchema: data.entrySchema,
            entryObjectName: data.entryObjectName,
            entryProcedure: data.entryProcedure,
            procedureCount: data.procedureCount,
            tableCount: data.tableCount,
            nodeCount: data.nodeCount,
            flowStats: data.flowStats,
            callGraph: data.callGraph,
            tableOperations: data.tableOperations
        };
        const filename = ((data.name || 'plsql').replace(/[^a-zA-Z0-9._-]/g, '_')) + '_report_' + key + '.json';
        this._downloadJSON(filename, report);
        PA.toast('Exported with key: ' + key, 'success');
    },

    importJSON(event) {
        const file = event.target.files[0];
        if (!file) return;
        PA.toast('Loading JSON report...', 'success');
        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const data = JSON.parse(e.target.result);
                if (!data.callGraph && !data.tableOperations) {
                    PA.toast('Invalid JSON: missing callGraph or tableOperations', 'error');
                    return;
                }
                PA.analysisData = data;
                PA.goAnalysis();
                PA.updateTopbar(data);
                PA.toast('Loaded: ' + (data.name || 'imported'), 'success');
            } catch (err) {
                PA.toast('Parse error: ' + err.message, 'error');
            }
        };
        reader.readAsText(file);
        event.target.value = '';
    },

    _downloadJSON(filename, data) {
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
        URL.revokeObjectURL(url);
    }
});
