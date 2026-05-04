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

        var ta = PA.queryRunner._textarea;
        if (tableName) {
            var fullName = schema ? schema + '.' + tableName : tableName;
            ta.value = 'SELECT * FROM ' + fullName + ' WHERE ROWNUM <= 20';
        } else if (!ta.value.trim()) {
            ta.value = '';
        }

        ta.focus();
        ta.setSelectionRange(ta.value.length, ta.value.length);
        PA.queryRunner._clearResults();
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
            var res = await fetch('/api/plsql/db/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql: sql, maxRows: String(maxRows), schema: connInfo.owner || '', project: connInfo.project || '', environment: connInfo.environment || '' })
            });

            var data = await res.json();

            if (data.error) {
                PA.queryRunner._showError(data.error);
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
