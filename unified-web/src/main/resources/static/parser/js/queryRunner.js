window.PA = window.PA || {};

/**
 * Query Runner — read-only SQL query execution modal.
 * Opens from table detail or standalone. Only SELECT queries are allowed.
 */
PA.queryRunner = {
    _modal: null,
    _textarea: null,
    _resultsArea: null,
    _statusBar: null,
    _running: false,

    /**
     * Open the query runner modal.
     * @param {string} [tableName] - Pre-fill query for this table
     * @param {string} [schema] - Schema prefix for the table
     */
    open(tableName, schema) {
        PA.queryRunner._ensureModal();
        var modal = PA.queryRunner._modal;
        modal.style.display = '';
        if (PA.modalStack) modal.style.zIndex = PA.modalStack.nextZ();

        var ta = PA.queryRunner._textarea;
        if (tableName) {
            var fullName = schema ? schema + '.' + tableName : tableName;
            ta.value = PA.queryRunner._sampleQuery(fullName);
        } else if (!ta.value.trim()) {
            ta.value = '';
        }

        PA.queryRunner._updateContext(tableName, schema);
        ta.focus();
        ta.setSelectionRange(ta.value.length, ta.value.length);
        PA.queryRunner._clearResults();
    },

    /** Show which datasource + procedure this Query Runner is operating against. */
    _updateContext(tableName, schema) {
        var el = document.getElementById('qrContext');
        if (!el) return;
        var d = PA.analysisData || {};
        var dbt = PA.queryRunner._dialect();
        var dsName = d.project || d.connectionName || '';
        var proc = (PA.context && PA.context.procId) || d.entryPoint || d.name || '';
        // Strip $-form schema prefix for a readable proc label.
        proc = String(proc).replace(/\$/g, '.');
        var badge = (PA.dbSelector && PA.dbSelector.badgeHtml) ? PA.dbSelector.badgeHtml(dbt) : dbt;
        var html = badge;
        if (dsName) html += '<span class="qr-ctx-item" title="Datasource / project">' + PA.esc(dsName) + '</span>';
        if (proc)   html += '<span class="qr-ctx-item qr-ctx-proc" title="Current procedure">&#402; ' + PA.esc(proc) + '</span>';
        if (tableName) {
            var tbl = (schema ? schema + '.' : '') + tableName;
            html += '<span class="qr-ctx-item" title="Table">&#8863; ' + PA.esc(tbl) + '</span>';
        }
        el.innerHTML = html;
    },

    /** Infer the SQL dialect of the loaded analysis from dbType or jdbcUrl. */
    _dialect() {
        var d = PA.analysisData || {};
        var dbt = (d.dbType || '').toUpperCase();
        if (dbt) return dbt;
        var url = (d.jdbcUrl || '').toLowerCase();
        if (url.indexOf('sqlserver') >= 0) return 'SQLSERVER';
        if (url.indexOf(':mysql') >= 0) return 'MYSQL';
        if (url.indexOf(':db2') >= 0) return 'DB2';
        if (url.indexOf(':postgresql') >= 0) return 'POSTGRES';
        return 'ORACLE';
    },

    /** Dialect-appropriate "first 20 rows" sample query. */
    _sampleQuery(fullName) {
        switch (PA.queryRunner._dialect()) {
            case 'SQLSERVER': return 'SELECT TOP 20 * FROM ' + fullName;
            case 'MYSQL':     return 'SELECT * FROM ' + fullName + ' LIMIT 20';
            case 'POSTGRES':  return 'SELECT * FROM ' + fullName + ' LIMIT 20';
            case 'DB2':       return 'SELECT * FROM ' + fullName + ' FETCH FIRST 20 ROWS ONLY';
            default:          return 'SELECT * FROM ' + fullName + ' WHERE ROWNUM <= 20';
        }
    },

    /**
     * Close the query runner modal.
     */
    close(event) {
        if (event && event.target !== event.currentTarget) return;
        if (PA.queryRunner._modal) {
            PA.queryRunner._modal.style.display = 'none';
        }
    },

    /**
     * Execute the current query.
     */
    async execute() {
        if (PA.queryRunner._running) return;

        var sql = PA.queryRunner._textarea.value.trim();
        if (!sql) {
            PA.queryRunner._showError('Please enter a SQL query.');
            return;
        }

        var maxRowsEl = document.getElementById('qrMaxRows');
        var maxRows = maxRowsEl ? parseInt(maxRowsEl.value, 10) : 100;

        PA.queryRunner._running = true;
        PA.queryRunner._showLoading();

        try {
            var connInfo = PA.home ? PA.home.getFormData() : {};
            var analysis = PA.analysisData || {};
            // Prefer project/env/schema from the loaded analysis over the home form
            // (the analysis may have been run against a different env / database).
            var qProject = analysis.project || connInfo.project || '';
            var qEnv = analysis.environment || connInfo.environment || '';
            var qSchema = PA.tableDetail && PA.tableDetail._schema
                ? PA.tableDetail._schema
                : (analysis.entrySchema || connInfo.owner || '');
            var res = await fetch('/api/plsql/db/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql: sql, maxRows: String(maxRows), schema: qSchema, project: qProject, environment: qEnv })
            });

            if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
            var data = await res.json();

            if (data.unsupported) {
                PA.queryRunner._showError(data.error || 'Query runner is not supported for this database dialect.');
            } else if (data.error) {
                PA.queryRunner._showError(data.error);
            } else if (!res.ok) {
                PA.queryRunner._showError('Query failed (HTTP ' + res.status + ').');
            } else {
                PA.queryRunner._showResults(data);
            }
        } catch (e) {
            PA.queryRunner._showError('Network error: ' + e.message);
        } finally {
            PA.queryRunner._running = false;
        }
    },

    /**
     * Clear the textarea and results.
     */
    clear() {
        if (PA.queryRunner._textarea) PA.queryRunner._textarea.value = '';
        PA.queryRunner._clearResults();
    },

    // ---- Internal rendering ----

    _showLoading() {
        var area = PA.queryRunner._resultsArea;
        area.innerHTML =
            '<div class="qr-status-bar">' +
                '<span class="qr-status-text"><span class="qr-spinner"></span>Executing query...</span>' +
            '</div>' +
            '<div class="qr-empty">Running query...</div>';
    },

    _showError(msg) {
        var area = PA.queryRunner._resultsArea;
        area.innerHTML =
            '<div class="qr-status-bar">' +
                '<span class="qr-status-text error">Error</span>' +
            '</div>' +
            '<div class="qr-error">' + PA.esc(msg) + '</div>';
    },

    _showResults(data) {
        var area = PA.queryRunner._resultsArea;
        var cols = data.columns || [];
        var rows = data.rows || [];
        var html = '';

        // Status bar
        html += '<div class="qr-status-bar">';
        html += '<span class="qr-status-text success">' + data.rowCount + ' row' + (data.rowCount !== 1 ? 's' : '') + '</span>';
        html += '<span class="badge" style="font-size:10px">' + cols.length + ' columns</span>';
        if (data.executionMs !== undefined) {
            html += '<span class="qr-timing">' + data.executionMs + 'ms</span>';
        }
        html += '</div>';

        // Truncation warning
        if (data.truncated) {
            html += '<div class="qr-truncated">Results truncated. Showing first ' + data.rowCount + ' rows. Increase max rows or refine your query.</div>';
        }

        if (rows.length === 0) {
            html += '<div class="qr-empty">Query returned no rows.</div>';
            area.innerHTML = html;
            return;
        }

        // Table
        html += '<div class="qr-table-wrap"><table class="qr-table"><thead><tr>';
        html += '<th style="width:40px;text-align:center">#</th>';
        for (var c = 0; c < cols.length; c++) {
            var colName = typeof cols[c] === 'object' ? cols[c].name : cols[c];
            html += '<th>' + PA.esc(colName) + '</th>';
        }
        html += '</tr></thead><tbody>';

        for (var r = 0; r < rows.length; r++) {
            html += '<tr>';
            html += '<td style="text-align:center;color:var(--text-muted);font-size:10px">' + (r + 1) + '</td>';
            var row = rows[r];
            for (var ci = 0; ci < cols.length; ci++) {
                var val = ci < row.length ? row[ci] : null;
                if (val === null || val === undefined) {
                    html += '<td class="null-val">NULL</td>';
                } else {
                    html += '<td title="' + PA.escAttr(String(val)) + '">' + PA.esc(String(val)) + '</td>';
                }
            }
            html += '</tr>';
        }

        html += '</tbody></table></div>';
        area.innerHTML = html;
    },

    _clearResults() {
        if (PA.queryRunner._resultsArea) {
            PA.queryRunner._resultsArea.innerHTML = '<div class="qr-empty">Press Ctrl+Enter or click Run to execute your query.</div>';
        }
    },

    // ---- Modal DOM construction (once) ----

    _ensureModal() {
        if (PA.queryRunner._modal) return;

        var overlay = document.createElement('div');
        overlay.className = 'src-modal-overlay';
        overlay.id = 'queryRunnerModal';
        // A closing panel must not keep a query it has stopped applying; see
        // SearchQuery.resetWithin in shared/ui-busy.js.
        if (window.SearchQuery) SearchQuery.resetWithin(overlay);
        overlay.style.display = 'none';
        overlay.onclick = function(e) { PA.queryRunner.close(e); };

        var modal = document.createElement('div');
        modal.className = 'qr-modal';
        modal.onclick = function(e) { e.stopPropagation(); };

        // Header
        modal.innerHTML =
            '<div class="src-modal-header">' +
                '<span class="src-modal-title">Query Runner</span>' +
                '<span style="font-size:10px;color:var(--text-muted);margin-left:8px">(read-only)</span>' +
                '<span class="qr-context" id="qrContext"></span>' +
                '<div class="src-modal-actions">' +
                    '<button class="btn btn-sm" onclick="PA.queryRunner.close()" title="Close (Esc)">&times;</button>' +
                '</div>' +
            '</div>' +
            '<div class="qr-input-area">' +
                '<textarea class="qr-textarea" id="qrTextarea" placeholder="Enter a SELECT query..." spellcheck="false"></textarea>' +
            '</div>' +
            '<div class="qr-toolbar">' +
                '<button class="btn btn-primary btn-sm" onclick="PA.queryRunner.execute()">Run</button>' +
                '<button class="btn btn-sm" onclick="PA.queryRunner.clear()">Clear</button>' +
                '<label style="font-size:11px;color:var(--text-muted);display:flex;align-items:center;gap:4px">' +
                    'Max rows:' +
                    '<select id="qrMaxRows" class="qr-max-rows">' +
                        '<option value="20">20</option>' +
                        '<option value="50">50</option>' +
                        '<option value="100" selected>100</option>' +
                        '<option value="200">200</option>' +
                        '<option value="500">500</option>' +
                    '</select>' +
                '</label>' +
                '<span class="qr-kbd-hint">Ctrl+Enter to run &middot; Esc to close</span>' +
            '</div>' +
            '<div class="qr-results" id="qrResults">' +
                '<div class="qr-empty">Press Ctrl+Enter or click Run to execute your query.</div>' +
            '</div>';

        overlay.appendChild(modal);
        document.body.appendChild(overlay);

        PA.queryRunner._modal = overlay;
        PA.queryRunner._textarea = document.getElementById('qrTextarea');
        PA.queryRunner._resultsArea = document.getElementById('qrResults');

        // Keyboard handlers on the textarea
        PA.queryRunner._textarea.addEventListener('keydown', function(e) {
            if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
                e.preventDefault();
                PA.queryRunner.execute();
            }
        });

        // Global keyboard handler for the modal overlay
        overlay.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                PA.queryRunner.close();
            }
        });

        // Make overlay focusable for Escape to work when clicking outside textarea
        overlay.setAttribute('tabindex', '-1');
    }
};
