/**
 * Summary Claude — Claude Insights sub-tab for PL/SQL Analyzer.
 * Shows Claude verification results, progress, and per-procedure analysis.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    _claudePollId: null,
    _lastClaudeStatus: null,

    _renderClaudeTab() {
        const esc = PA.esc;
        const procReports = this._procReports || [];

        if (!PA.analysisData || !procReports.length) {
            return '<div style="padding:24px;text-align:center;color:var(--text-muted)">No analysis data available.</div>';
        }

        let html = '<div style="padding:16px 24px">';

        // Status area
        html += '<div id="claude-status-msg" style="padding:12px 16px;background:var(--bg);border:1px solid var(--border);border-radius:8px;margin-bottom:12px;font-size:12px">Checking for Claude analysis data...</div>';

        // Action header
        html += '<div id="claude-action-header" style="display:none;margin-bottom:12px">';
        html += '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">';
        html += '<span class="badge" style="background:var(--badge-purple-bg);color:var(--badge-purple);font-size:11px;padding:4px 10px">CLAUDE</span>';
        html += '<span style="font-weight:700;font-size:14px">PL/SQL Verification Insights</span>';
        html += '<div style="margin-left:auto;display:flex;gap:4px">';
        html += '<button class="btn btn-sm" onclick="PA.summary._checkClaudeStatus()">Check Status</button>';
        html += '<button class="btn btn-sm btn-primary" onclick="PA.summary._startClaudeVerification()">Start Verification</button>';
        html += '<button class="btn btn-sm" onclick="PA.summary._showClaudeLogs()">Live Logs</button>';
        html += '</div>';
        html += '</div></div>';

        // Progress area
        html += '<div id="claude-progress-area" style="display:none;margin-bottom:12px">';
        html += '<div style="height:6px;background:var(--border);border-radius:3px;overflow:hidden;margin-bottom:6px">';
        html += '<div id="claude-progress-fill" style="height:100%;width:0%;background:var(--accent);transition:width 0.3s;border-radius:3px"></div>';
        html += '</div>';
        html += '<div id="claude-progress-text" style="font-size:11px;color:var(--text-muted)"></div>';
        html += '</div>';

        // Verification results — per-chunk cards
        html += '<div id="claude-results-area" style="display:none">';
        html += '<div id="claude-results-content"></div>';
        html += '</div>';

        // Procedure cards with Claude data
        html += '<div id="claude-proc-cards" style="display:none">';
        html += this._buildFilterBar('sum-claude', procReports, r => r.schemaName);
        html += '<div class="pagination-bar" id="sum-claude-pager-top"></div>';
        html += '<div id="sum-claude-tbody"></div>';
        html += '<div class="pagination-bar" id="sum-claude-pager"></div>';
        html += '</div>';

        html += '</div>';

        // Init pagination
        this._initPage('sum-claude', procReports, 25,
            (r, i, esc) => this._renderClaudeCard(r, i, esc),
            r => r.schemaName,
            null,
            {
                sortKeys: [
                    null, null, null, null, null
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-claude');
            this._initColFilters('sum-claude', {
                0: { label: 'Schema', valueFn: r => r.schemaName },
                1: { label: 'Type', valueFn: r => r.unitType },
                2: { label: 'Complexity', valueFn: r => r.complexity }
            });
        }, 0);

        // Auto-check status
        setTimeout(() => this._checkClaudeStatus(), 300);

        return html;
    },

    _renderClaudeCard(r, idx, esc) {
        const unitCls = r.unitType === 'FUNCTION' ? 'F' : r.unitType === 'TRIGGER' ? 'T' : 'P';
        let html = `<div style="padding:10px 16px;border:1px solid var(--border);border-radius:8px;margin-bottom:6px;background:var(--bg-card);cursor:pointer" onclick="PA.summary.toggleDetail('claude',${idx})">`;

        // Header
        html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">';
        html += `<span class="lp-icon ${unitCls}" style="display:inline-flex">${unitCls}</span>`;
        html += `<strong style="font-family:var(--font-mono);font-size:13px">${esc(r.name)}</strong>`;
        html += `<span style="color:${this._schemaColor(r.schemaName)};font-weight:600;font-size:11px">${esc(r.schemaName)}</span>`;
        if (r.packageName) html += `<span style="color:var(--text-muted);font-size:10px">${esc(r.packageName)}</span>`;
        html += '</div>';

        // Stats row
        html += '<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;font-size:11px">';
        html += `<span style="padding:2px 8px;background:var(--bg);border:1px solid var(--border);border-radius:4px">${r.tableCount} tables</span>`;
        html += `<span style="padding:2px 8px;background:var(--bg);border:1px solid var(--border);border-radius:4px">${r.loc} LOC</span>`;
        html += `<span style="padding:2px 8px;background:var(--bg);border:1px solid var(--border);border-radius:4px">${r.crossSchemaCalls} cross-schema</span>`;
        for (const op of r.allOps) html += `<span class="op-badge ${op}">${op}</span>`;
        html += '</div>';

        // Actions
        html += '<div style="display:flex;gap:4px;margin-top:6px">';
        html += `<button class="btn btn-sm" onclick="PA.summary.showTrace(${idx});event.stopPropagation()" style="font-size:10px;padding:2px 8px">Trace</button>`;
        html += `<button class="btn btn-sm" onclick="PA.summary.showCallTrace(${idx});event.stopPropagation()" style="font-size:10px;padding:2px 8px">Explore</button>`;
        html += `<button class="btn btn-sm" onclick="PA.summary.showExportModal({procIdx:${idx}});event.stopPropagation()" style="font-size:10px;padding:2px 8px">Export</button>`;
        html += '</div>';

        html += '</div>';

        // Expandable detail
        html += `<div id="sum-claude-detail-${idx}" style="display:none;padding:12px 16px;background:var(--bg-top-bar);border:1px solid var(--border);border-radius:0 0 8px 8px;margin-top:-7px;margin-bottom:6px">`;

        // Tables
        if (Object.keys(r.flowTables || {}).length) {
            html += '<div style="margin-bottom:8px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-muted);margin-bottom:4px">Tables in Flow (' + Object.keys(r.flowTables).length + ')</div>';
            for (const [name, t] of Object.entries(r.flowTables)) {
                const ops = [...(t.operations || [])];
                html += this._tableBadge(name, esc, ops) + ' ';
            }
            html += '</div>';
        }

        // Calls
        if (r.calls && r.calls.length) {
            html += '<div style="margin-bottom:8px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-muted);margin-bottom:4px">Calls (' + r.calls.length + ')</div>';
            for (const c of r.calls.slice(0, 10)) {
                const badge = c.callType === 'EXTERNAL' ? '<span class="lp-type-badge EXTERNAL" style="margin-right:4px">EXT</span>' : '';
                html += `<div style="font-size:11px;padding:1px 0">${badge}<strong>${esc(c.name || '?')}</strong></div>`;
            }
            if (r.calls.length > 10) html += '<div style="font-size:10px;color:var(--text-muted)">...and ' + (r.calls.length - 10) + ' more</div>';
            html += '</div>';
        }

        html += '</div>';
        return html;
    },

    async _checkClaudeStatus() {
        const msgEl = document.getElementById('claude-status-msg');
        if (!msgEl) return;

        const headerEl = document.getElementById('claude-action-header');
        const cardsEl = document.getElementById('claude-proc-cards');
        const showPanel = () => {
            if (headerEl) headerEl.style.display = '';
            if (cardsEl) cardsEl.style.display = '';
        };

        try {
            const status = await PA.api.getClaudeStatus();
            if (!status) {
                msgEl.innerHTML = 'Claude CLI not available. Install Claude CLI to enable AI verification.';
                msgEl.style.borderColor = 'var(--border)';
                showPanel();
                return;
            }

            if (status.cliAvailable) {
                showPanel();

                const progress = status.progress;
                if (progress && progress.running) {
                    this._updateClaudeProgress(progress);
                    this._startClaudePoll();
                    return;
                }

                if (status.hasVerification) {
                    msgEl.innerHTML = '<span class="badge" style="background:var(--badge-green-bg);color:var(--badge-green)">VERIFIED</span> Claude verification data available.';
                    msgEl.style.borderColor = 'var(--green)';

                    // Load and display results
                    try {
                        const result = await PA.api.getClaudeResult();
                        if (result) this._displayClaudeResults(result);
                    } catch (e) { /* ignore */ }
                    return;
                }

                msgEl.innerHTML = 'Claude CLI available. Click <strong>Start Verification</strong> to run AI analysis on all procedures.'
                    + ' <button class="btn btn-sm btn-primary" onclick="PA.summary._startClaudeVerification()" style="margin-left:12px">Start Verification</button>';
                msgEl.style.borderColor = 'var(--accent)';
            } else {
                msgEl.innerHTML = 'Claude CLI not detected. Ensure Claude CLI is installed and accessible.';
                showPanel();
            }
        } catch (e) {
            msgEl.innerHTML = 'Could not check Claude status: ' + PA.esc(e.message);
            showPanel();
        }
    },

    _displayClaudeResults(result) {
        const area = document.getElementById('claude-results-area');
        if (!area) return;
        area.style.display = '';
        const content = document.getElementById('claude-results-content');
        if (!content) return;
        const esc = PA.esc;

        if (!result.tables || !result.tables.length) {
            content.innerHTML = '<div style="color:var(--text-muted);padding:12px">No verification data in result.</div>';
            return;
        }

        const confirmed = result.tables.filter(t => t.overallStatus === 'CONFIRMED').length;
        const modified = result.tables.filter(t => t.overallStatus === 'MODIFIED').length;

        let html = '<div style="margin-bottom:12px">';
        html += '<div style="font-size:12px;font-weight:700;margin-bottom:8px">Verification Summary</div>';
        html += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">';
        html += `<span class="dh-stat" style="padding:6px 12px"><span class="dh-stat-value" style="font-size:16px">${result.tables.length}</span><span class="dh-stat-label">Tables Verified</span></span>`;
        html += `<span class="dh-stat green" style="padding:6px 12px"><span class="dh-stat-value" style="font-size:16px">${confirmed}</span><span class="dh-stat-label">Confirmed</span></span>`;
        html += `<span class="dh-stat orange" style="padding:6px 12px"><span class="dh-stat-value" style="font-size:16px">${modified}</span><span class="dh-stat-label">Modified</span></span>`;
        html += '</div>';
        html += '</div>';

        // Paginated verification table
        const tableData = result.tables.map(t => ({
            tableName: t.tableName,
            overallStatus: t.overallStatus || 'UNKNOWN',
            verifications: t.claudeVerifications || [],
            opCount: (t.claudeVerifications || []).length
        }));

        html += this._buildFilterBar('sum-cver', tableData, () => null);
        html += '<div class="pagination-bar" id="sum-cver-pager-top"></div>';
        html += '<div style="overflow:auto;flex:1">';
        html += '<table class="to-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-cver\',0)">Table</th>';
        html += '<th data-sort-col="1" onclick="PA.summary._pageSort(\'sum-cver\',1)">Status</th>';
        html += '<th data-sort-col="2" onclick="PA.summary._pageSort(\'sum-cver\',2)">Operations</th>';
        html += '<th>Details</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-cver-tbody"></tbody>';
        html += '</table></div>';
        html += '<div class="pagination-bar" id="sum-cver-pager"></div>';

        content.innerHTML = html;

        this._initPage('sum-cver', tableData, 25,
            (t, i, esc) => {
                const statusBg = t.overallStatus === 'CONFIRMED' ? 'var(--badge-green-bg)' : 'var(--badge-orange-bg)';
                const statusColor = t.overallStatus === 'CONFIRMED' ? 'var(--badge-green)' : 'var(--badge-orange)';
                let row = '<tr class="to-row">';
                row += `<td><strong style="font-family:var(--font-mono)">${esc(t.tableName)}</strong></td>`;
                row += `<td><span class="badge" style="background:${statusBg};color:${statusColor}">${esc(t.overallStatus)}</span></td>`;
                row += `<td>${t.opCount}</td>`;
                row += '<td>';
                for (const v of t.verifications) {
                    row += `<span class="op-badge ${v.operation || ''}" style="font-size:9px">${esc(v.operation || '?')}: ${esc(v.status || '?')}</span> `;
                }
                row += '</td>';
                row += '</tr>';
                return row;
            },
            () => null,
            null,
            {
                sortKeys: [
                    { fn: t => t.tableName },
                    { fn: t => t.overallStatus },
                    { fn: t => t.opCount }
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-cver');
            this._initColFilters('sum-cver', {
                1: { label: 'Status', valueFn: t => t.overallStatus }
            });
        }, 0);
    },

    async _startClaudeVerification() {
        try {
            PA.toast('Starting Claude verification...', 'success');
            const result = await PA.api.startClaudeVerification(false);
            if (result && result.error) {
                PA.toast('Failed: ' + result.error, 'error');
                return;
            }
            PA.toast('Claude verification started', 'success');
            this._startClaudePoll();

            const progArea = document.getElementById('claude-progress-area');
            if (progArea) progArea.style.display = '';
        } catch (e) {
            PA.toast('Failed to start verification: ' + e.message, 'error');
        }
    },

    _updateClaudeProgress(progress) {
        const progArea = document.getElementById('claude-progress-area');
        const fill = document.getElementById('claude-progress-fill');
        const text = document.getElementById('claude-progress-text');
        const msgEl = document.getElementById('claude-status-msg');

        if (progArea) progArea.style.display = '';

        const pct = progress.percentComplete || 0;
        const done = progress.completedChunks || 0;
        const total = progress.totalChunks || 0;

        if (fill) fill.style.width = pct + '%';
        if (text) text.textContent = `Verifying: ${done}/${total} chunks (${Math.round(pct)}%)`;

        if (msgEl) {
            if (progress.running) {
                msgEl.innerHTML = '<span class="badge" style="background:var(--badge-blue-bg);color:var(--badge-blue)">RUNNING</span> Claude verification in progress...';
                msgEl.style.borderColor = 'var(--blue)';
            } else {
                msgEl.innerHTML = '<span class="badge" style="background:var(--badge-green-bg);color:var(--badge-green)">COMPLETE</span> Claude verification complete.';
                msgEl.style.borderColor = 'var(--green)';
            }
        }

        this._lastClaudeStatus = progress;
    },

    _startClaudePoll() {
        if (this._claudePollId) return;
        this._claudePollId = setInterval(async () => {
            try {
                const progress = await PA.api.getClaudeProgress();
                if (!progress) return;
                this._updateClaudeProgress(progress);

                if (!progress.running) {
                    clearInterval(this._claudePollId);
                    this._claudePollId = null;

                    if (progress.percentComplete >= 100) {
                        PA.toast('Claude verification complete', 'success');
                        // Load results
                        try {
                            const result = await PA.api.getClaudeResult();
                            if (result) {
                                this._displayClaudeResults(result);
                                PA._mergeClaudeResults(result);
                            }
                        } catch (e) { /* ignore */ }
                    }
                }
            } catch (e) { /* continue polling */ }
        }, PollConfig.claudeProgressMs);
    },

    async _showClaudeLogs() {
        let overlay = document.getElementById('claude-log-overlay');
        if (overlay) overlay.remove();

        overlay = document.createElement('div');
        overlay.id = 'claude-log-overlay';
        overlay.style.cssText = 'position:fixed;inset:0;z-index:1000;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center';
        overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
        overlay.innerHTML = `<div style="background:var(--bg-card);border-radius:12px;width:80vw;max-width:900px;max-height:80vh;display:flex;flex-direction:column;overflow:hidden">
            <div style="display:flex;align-items:center;padding:12px 16px;border-bottom:1px solid var(--border)">
                <span style="font-weight:700">Claude Verification Logs</span>
                <button class="btn btn-sm" style="margin-left:auto" onclick="document.getElementById('claude-log-overlay').remove()">Close</button>
            </div>
            <pre id="claude-log-pre" style="flex:1;overflow:auto;padding:16px;font-family:var(--font-mono);font-size:11px;line-height:1.5;color:var(--text);background:var(--bg);margin:0">Loading...</pre>
        </div>`;
        document.body.appendChild(overlay);

        try {
            const progress = await PA.api.getClaudeProgress();
            const pre = document.getElementById('claude-log-pre');
            if (!pre) return;
            let log = `Status: ${progress?.running ? 'RUNNING' : 'IDLE'}\n`;
            log += `Completed: ${progress?.completedChunks || 0}/${progress?.totalChunks || 0}\n`;
            log += `Progress: ${Math.round(progress?.percentComplete || 0)}%\n\n`;
            if (progress?.errors?.length) {
                log += '--- Errors ---\n';
                for (const e of progress.errors) log += e + '\n';
            }
            pre.textContent = log;
        } catch (e) {
            const pre = document.getElementById('claude-log-pre');
            if (pre) pre.textContent = 'Failed to load: ' + e.message;
        }
    }
});
