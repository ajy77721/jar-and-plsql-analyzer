window.PA = window.PA || {};

PA.export = {
    _fmt: 'json',

    show() {
        var modal = document.getElementById('exportModal');
        if (!modal) return;
        var info = document.getElementById('exportInfo');
        if (info && PA.analysisData) {
            var d = PA.analysisData;
            info.innerHTML =
                '<div style="font-weight:600;color:var(--accent);margin-bottom:8px">' + PA.esc(d.entryPoint || d.name || '') + '</div>' +
                '<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:11px;color:var(--text-muted)">' +
                '<span>' + (d.totalNodes || 0) + ' procedures</span>' +
                '<span>' + (d.totalTables || 0) + ' tables</span>' +
                '<span>' + (d.totalEdges || 0) + ' edges</span>' +
                '<span>' + (d.totalLinesOfCode || 0) + ' LOC</span>' +
                '</div>';
        }
        PA.export.setFormat('json');
        modal.style.display = '';
    },

    close(event) {
        if (event && event.target !== event.currentTarget) return;
        document.getElementById('exportModal').style.display = 'none';
    },

    setFormat(fmt) {
        PA.export._fmt = fmt;
        document.getElementById('expFmtJson').classList.toggle('active', fmt === 'json');
        document.getElementById('expFmtCsv').classList.toggle('active', fmt === 'csv');
    },

    async download() {
        if (!PA.analysisData) { PA.toast('No analysis loaded', 'error'); return; }
        PA.toast('Preparing export...', 'success');

        var sections = {
            procs: document.getElementById('expProcs').checked,
            tables: document.getElementById('expTables').checked,
            joins: document.getElementById('expJoins').checked,
            cursors: document.getElementById('expCursors').checked,
            callGraph: document.getElementById('expCallGraph').checked,
            summary: document.getElementById('expSummary').checked
        };

        try {
            var data = await PA.export._gather(sections);
            if (PA.export._fmt === 'json') {
                PA.export._downloadJSON(data);
            } else {
                PA.export._downloadCSV(data, sections);
            }
            PA.toast('Export complete', 'success');
        } catch (e) {
            PA.toast('Export failed: ' + (e.message || e), 'error');
        }
    },

    async _gather(sections) {
        var result = {};
        var d = PA.analysisData;

        if (sections.summary) {
            result.summary = {
                name: d.name || '', entryPoint: d.entryPoint || '', entrySchema: d.entrySchema || '',
                totalNodes: d.totalNodes || 0, totalTables: d.totalTables || 0,
                totalEdges: d.totalEdges || 0, totalLinesOfCode: d.totalLinesOfCode || 0,
                maxDepth: d.maxDepth || 0, totalCycles: d.totalCycles || 0
            };
        }

        if (sections.procs && d.nodes) {
            result.procedures = d.nodes.map(function(n) {
                return {
                    nodeId: n.nodeId, name: n.name, schema: n.schema || '',
                    packageName: n.packageName || '', objectType: n.objectType || '',
                    depth: n.depth, lineStart: n.lineStart, lineEnd: n.lineEnd,
                    linesOfCode: n.linesOfCode || 0, sourceFile: n.sourceFile || '',
                    readable: n.readable !== false
                };
            });
        }

        if (sections.tables) {
            try { result.tableOperations = await PA.api.getTableOperations(); } catch(e) { result.tableOperations = []; }
        }

        if (sections.joins) {
            try { result.joins = await PA.api.getJoinOperations(); } catch(e) { result.joins = []; }
        }

        if (sections.cursors) {
            try { result.cursors = await PA.api.getCursorOperations(); } catch(e) { result.cursors = []; }
        }

        if (sections.callGraph) {
            try { result.callGraph = await PA.api.getCallGraph(); } catch(e) { result.callGraph = null; }
        }

        return result;
    },

    _downloadJSON(data) {
        var json = JSON.stringify(data, null, 2);
        var blob = new Blob([json], { type: 'application/json' });
        var name = (PA.analysisData.name || 'analysis') + '_export.json';
        PA.export._triggerDownload(blob, name);
    },

    _downloadCSV(data, sections) {
        var files = [];

        if (sections.procs && data.procedures) {
            var headers = ['nodeId', 'name', 'schema', 'packageName', 'objectType', 'depth', 'lineStart', 'lineEnd', 'linesOfCode', 'sourceFile', 'readable'];
            files.push({ name: 'procedures.csv', content: PA.export._toCSV(headers, data.procedures) });
        }

        if (sections.tables && data.tableOperations) {
            var rows = [];
            data.tableOperations.forEach(function(t) {
                (t.accessDetails || []).forEach(function(d) {
                    rows.push({ tableName: t.tableName, operations: (t.operations || []).join(';'), operation: d.operation, procedureId: d.procedureId, procedureName: d.procedureName, lineNumber: d.lineNumber });
                });
                if (!(t.accessDetails || []).length) {
                    rows.push({ tableName: t.tableName, operations: (t.operations || []).join(';'), operation: '', procedureId: '', procedureName: '', lineNumber: '' });
                }
            });
            files.push({ name: 'table_operations.csv', content: PA.export._toCSV(['tableName', 'operations', 'operation', 'procedureId', 'procedureName', 'lineNumber'], rows) });
        }

        if (sections.joins && data.joins) {
            var jRows = [];
            (Array.isArray(data.joins) ? data.joins : []).forEach(function(j) {
                (j.accessDetails || []).forEach(function(d) {
                    jRows.push({ leftTable: j.leftTable, rightTable: j.rightTable, joinType: d.joinType || '', onPredicate: d.onPredicate || '', procedureName: d.procedureName || '', lineNumber: d.lineNumber || '' });
                });
            });
            files.push({ name: 'joins.csv', content: PA.export._toCSV(['leftTable', 'rightTable', 'joinType', 'onPredicate', 'procedureName', 'lineNumber'], jRows) });
        }

        if (sections.cursors && data.cursors) {
            var cRows = [];
            (Array.isArray(data.cursors) ? data.cursors : []).forEach(function(c) {
                (c.accessDetails || []).forEach(function(d) {
                    cRows.push({ cursorName: c.cursorName, cursorType: c.cursorType || '', operation: d.operation || '', procedureName: d.procedureName || '', lineNumber: d.lineNumber || '', queryText: (c.queryText || '').substring(0, 200) });
                });
            });
            files.push({ name: 'cursors.csv', content: PA.export._toCSV(['cursorName', 'cursorType', 'operation', 'procedureName', 'lineNumber', 'queryText'], cRows) });
        }

        if (files.length === 1) {
            var blob = new Blob([files[0].content], { type: 'text/csv' });
            PA.export._triggerDownload(blob, (PA.analysisData.name || 'analysis') + '_' + files[0].name);
        } else {
            for (var i = 0; i < files.length; i++) {
                var blob = new Blob([files[i].content], { type: 'text/csv' });
                PA.export._triggerDownload(blob, (PA.analysisData.name || 'analysis') + '_' + files[i].name);
            }
        }
    },

    _toCSV(headers, rows) {
        var esc = function(v) {
            var s = String(v == null ? '' : v);
            if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0) return '"' + s.replace(/"/g, '""') + '"';
            return s;
        };
        var lines = [headers.join(',')];
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            lines.push(headers.map(function(h) { return esc(row[h]); }).join(','));
        }
        return lines.join('\n');
    },

    _triggerDownload(blob, filename) {
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url; a.download = filename;
        document.body.appendChild(a); a.click();
        setTimeout(function() { document.body.removeChild(a); URL.revokeObjectURL(url); }, 100);
    }
};
