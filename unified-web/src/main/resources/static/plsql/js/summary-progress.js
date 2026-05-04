/**
 * Summary — Claude Progress sub-tab.
 * Shows live progress for current/last Claude verification run.
 * Auto-polls every 30 seconds, resilient to API failures.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    _progressPollId: null,
    _lastProgress: null,
    _progressPollErrors: 0,

    _renderClaudeProgressTab() {
        const esc = PA.esc;

        let html = '<div class="cp-container" style="padding:16px 24px">';

        // Header
        html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:16px">';
        html += '<span class="badge" style="background:var(--badge-purple-bg);color:var(--badge-purple);font-size:11px;padding:4px 10px">CLAUDE</span>';
        html += '<span style="font-weight:700;font-size:14px">Verification Progress</span>';
        html += '<div style="margin-left:auto;display:flex;gap:4px">';
        html += '<button class="btn btn-sm" onclick="PA.summary._refreshProgress()">Refresh</button>';
        html += '<button class="btn btn-sm btn-primary" onclick="PA.summary._startClaudeFromProgress()">Start / Resume</button>';
        html += '</div>';
        html += '</div>';

        // Status banner
        html += '<div id="cp-status-banner" class="cp-status-banner cp-status-idle">';
        html += '<span class="cp-status-icon">&#9679;</span>';
        html += '<span id="cp-status-text">Checking status...</span>';
        html += '</div>';

        // Progress bar
        html += '<div class="cp-progress-wrap" id="cp-progress-wrap" style="display:none">';
        html += '<div class="cp-progress-bar"><div class="cp-progress-fill" id="cp-progress-fill"></div></div>';
        html += '<div class="cp-progress-label" id="cp-progress-label">0%</div>';
        html += '</div>';

        // Stats row
        html += '<div class="cp-stats" id="cp-stats" style="display:none"></div>';

        // Chunk table
        html += '<div id="cp-chunk-table" style="margin-top:12px"></div>';

        // Last poll info
        html += '<div id="cp-poll-info" style="font-size:10px;color:var(--text-muted);margin-top:8px;text-align:right"></div>';

        html += '</div>';

        // Start polling
        setTimeout(() => this._refreshProgress(), 300);
        this._startProgressPoll();

        return html;
    },

    async _refreshProgress() {
        try {
            const [status, progress, chunks] = await Promise.all([
                PA.api.getClaudeStatus().catch(() => null),
                PA.api.getClaudeProgress().catch(() => null),
                PA.api.listClaudeChunkSummaries().catch(() => [])
            ]);

            this._progressPollErrors = 0;
            this._lastProgress = { status, progress, chunks, fetchedAt: new Date() };
            this._renderProgressState(this._lastProgress);
        } catch (e) {
            this._progressPollErrors++;
            if (this._lastProgress) {
                this._renderProgressState(this._lastProgress);
                const info = document.getElementById('cp-poll-info');
                if (info) info.textContent = 'Last update: ' + this._formatTime(this._lastProgress.fetchedAt) + ' (poll error #' + this._progressPollErrors + ')';
            } else {
                const banner = document.getElementById('cp-status-banner');
                if (banner) {
                    banner.className = 'cp-status-banner cp-status-error';
                    const text = document.getElementById('cp-status-text');
                    if (text) text.textContent = 'Failed to fetch status: ' + e.message;
                }
            }
        }
    },

    _renderProgressState(state) {
        const esc = PA.esc;
        const { status, progress, chunks, fetchedAt } = state;

        // Status banner
        const banner = document.getElementById('cp-status-banner');
        const statusText = document.getElementById('cp-status-text');
        if (banner && statusText) {
            if (progress && progress.running) {
                banner.className = 'cp-status-banner cp-status-running';
                statusText.textContent = 'Claude verification running...';
            } else if (status && status.hasVerification) {
                banner.className = 'cp-status-banner cp-status-complete';
                statusText.textContent = 'Verification complete';
            } else if (!status || !status.cliAvailable) {
                banner.className = 'cp-status-banner cp-status-error';
                statusText.textContent = 'Claude CLI not available';
            } else {
                banner.className = 'cp-status-banner cp-status-idle';
                statusText.textContent = 'Ready — click Start to begin verification';
            }
        }

        // Progress bar
        const pWrap = document.getElementById('cp-progress-wrap');
        const pFill = document.getElementById('cp-progress-fill');
        const pLabel = document.getElementById('cp-progress-label');
        if (progress) {
            if (pWrap) pWrap.style.display = '';
            const pct = Math.round(progress.percentComplete || 0);
            if (pFill) pFill.style.width = pct + '%';
            if (pLabel) pLabel.textContent = pct + '%';
        }

        // Stats
        const statsEl = document.getElementById('cp-stats');
        if (statsEl && progress) {
            statsEl.style.display = '';
            const done = progress.completedChunks || 0;
            const total = progress.totalChunks || 0;
            const errors = (progress.errors || []).length;
            statsEl.innerHTML =
                '<span class="cp-stat"><b>' + done + '</b> / ' + total + ' Chunks</span>' +
                '<span class="cp-stat cp-stat-ok"><b>' + (done - errors) + '</b> Complete</span>' +
                (errors > 0 ? '<span class="cp-stat cp-stat-err"><b>' + errors + '</b> Errors</span>' : '') +
                '<span class="cp-stat"><b>' + Math.round(progress.percentComplete || 0) + '%</b></span>';
        }

        // Chunk table (paginated)
        const tableEl = document.getElementById('cp-chunk-table');
        if (tableEl && chunks && chunks.length) {
            const chunkData = chunks.map((c, i) => ({
                idx: i + 1,
                id: c.id || ('chunk_' + (i + 1)),
                name: c.name || '',
                procedures: c.procedures || [],
                status: c.status || 'PENDING',
                error: c.error || null,
                tableCount: c.tableCount || 0
            }));

            let html = this._buildFilterBar('sum-chunks', chunkData, () => null);
            html += '<div class="pagination-bar" id="sum-chunks-pager-top"></div>';
            html += '<div style="overflow:auto;flex:1">';
            html += '<table class="to-table cp-chunk-tbl">';
            html += '<thead><tr>';
            html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-chunks\',0)">Chunk</th>';
            html += '<th>Procedures</th>';
            html += '<th data-sort-col="2" onclick="PA.summary._pageSort(\'sum-chunks\',2)">Status</th>';
            html += '<th>Details</th>';
            html += '</tr></thead>';
            html += '<tbody id="sum-chunks-tbody"></tbody>';
            html += '</table></div>';
            html += '<div class="pagination-bar" id="sum-chunks-pager"></div>';
            tableEl.innerHTML = html;

            this._initPage('sum-chunks', chunkData, 25,
                (c, i, esc) => {
                    const chunkStatus = c.status;
                    const statusCls = chunkStatus === 'COMPLETE' || chunkStatus === 'COMPLETED'
                        ? 'cp-chunk-ok' : chunkStatus === 'ERROR' || chunkStatus === 'FAILED'
                        ? 'cp-chunk-err' : chunkStatus === 'RUNNING'
                        ? 'cp-chunk-run' : 'cp-chunk-pending';
                    const statusLabel = chunkStatus === 'COMPLETE' || chunkStatus === 'COMPLETED'
                        ? '&#10003; Complete' : chunkStatus === 'ERROR' || chunkStatus === 'FAILED'
                        ? '&#10007; Error' : chunkStatus === 'RUNNING'
                        ? '&#9654; Running' : '&#9679; Pending';

                    let row = '<tr class="to-row" onclick="PA.claude.showChunkLog(\'' + PA.escJs(c.id) + '\')" style="cursor:pointer">';
                    row += '<td><strong>Chunk ' + c.idx + '</strong></td>';
                    row += '<td style="font-size:11px">';
                    if (c.name) {
                        row += esc(c.name);
                    } else {
                        const maxShow = 3;
                        const shown = c.procedures.slice(0, maxShow);
                        row += shown.map(p => '<code style="font-size:10px">' + esc(typeof p === 'string' ? p : p.name || '?') + '</code>').join(', ');
                        if (c.procedures.length > maxShow) row += ' <span style="color:var(--text-muted);font-size:10px">+' + (c.procedures.length - maxShow) + ' more</span>';
                    }
                    row += '</td>';
                    row += '<td><span class="cp-chunk-badge ' + statusCls + '">' + statusLabel + '</span></td>';
                    row += '<td style="font-size:11px;color:var(--text-muted)">';
                    if (c.error) row += '<span style="color:var(--red)">' + esc(c.error.length > 80 ? c.error.substring(0, 80) + '...' : c.error) + '</span>';
                    else if (c.tableCount) row += c.tableCount + ' tables';
                    else row += '-';
                    row += '</td>';
                    row += '</tr>';
                    return row;
                },
                () => null,
                null,
                {
                    sortKeys: [
                        { fn: c => c.idx },
                        null,
                        { fn: c => c.status }
                    ]
                }
            );
            setTimeout(() => {
                this._pageRender('sum-chunks');
                this._initColFilters('sum-chunks', {
                    2: { label: 'Status', valueFn: c => c.status }
                });
            }, 0);
        } else if (tableEl && (!chunks || !chunks.length)) {
            tableEl.innerHTML = '<div style="padding:16px;text-align:center;color:var(--text-muted);font-size:12px">No chunk data available yet. Start a Claude verification run to see chunk progress.</div>';
        }

        // Poll info
        const info = document.getElementById('cp-poll-info');
        if (info && fetchedAt) {
            info.textContent = 'Last updated: ' + this._formatTime(fetchedAt) + ' — auto-refreshes every 30s';
        }

        // Stop polling if not running
        if (!progress || !progress.running) {
            this._stopProgressPoll();
        }
    },

    _startProgressPoll() {
        this._stopProgressPoll();
        this._progressPollId = setInterval(() => {
            const tab = document.getElementById('stab-claude-progress');
            if (!tab || tab.style.display === 'none') return;
            this._refreshProgress();
        }, PollConfig.claudeProgressMs);
    },

    _stopProgressPoll() {
        if (this._progressPollId) {
            clearInterval(this._progressPollId);
            this._progressPollId = null;
        }
    },

    async _startClaudeFromProgress() {
        try {
            PA.toast('Starting Claude verification...', 'success');
            await PA.api.startClaudeVerification(true);
            PA.toast('Claude verification started', 'success');
            this._startProgressPoll();
            setTimeout(() => this._refreshProgress(), 1000);
        } catch (e) {
            PA.toast('Failed: ' + e.message, 'error');
        }
    },

    _formatTime(date) {
        if (!date) return '';
        const d = date instanceof Date ? date : new Date(date);
        return d.toLocaleTimeString();
    }
});
