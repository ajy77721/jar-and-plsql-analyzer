/**
 * PA.claude — Claude AI Verification module.
 * Handles verification of static analysis results via Claude CLI.
 * Shows comparison view: Static vs Claude-verified findings with filters & pagination.
 */
window.PA = window.PA || {};

PA.claude = {
    result: null,
    pollTimer: null,
    sseConnection: null,
    tableChunkMap: {},  // table name → [chunkIds] for per-table log viewing
    // Filter & pagination state
    viewMode: 'both',  // 'both' | 'static' | 'claude'
    activeOps: new Set(),
    page: 1,
    pageSize: 50,

    // ==================== PROC ID RESOLUTION ====================

    _resolveProcId(procedureName) {
        if (!procedureName) return null;
        const upper = procedureName.toUpperCase();
        if (upper.includes('.')) return procedureName;

        const procs = PA.analysisData && PA.analysisData.procedures;
        if (!procs) return null;
        for (const p of procs) {
            const pid = (p.id || p.name || '').toUpperCase();
            const baseId = pid.includes('/') ? pid.substring(0, pid.indexOf('/')) : pid;
            if (pid === upper || pid.endsWith('.' + upper) || baseId.endsWith('.' + upper)) return p.id || p.name;
        }
        const schema = PA.analysisData && PA.analysisData.entrySchema;
        const obj = PA.analysisData && PA.analysisData.entryObjectName;
        if (schema && obj) return schema + '.' + obj + '.' + procedureName;
        if (obj) return obj + '.' + procedureName;
        return procedureName;
    },

    _openProc(procedureName) {
        const resolved = PA.claude._resolveProcId(procedureName);
        if (resolved) {
            PA.showProcedure(resolved);
        } else {
            PA.codeModal.open(procedureName);
        }
    },

    _openProcAtLine(procedureName, sourceFile, lineNumber) {
        if (sourceFile && lineNumber > 0) {
            PA.codeModal.openAtLine(sourceFile, lineNumber);
        } else if (procedureName && lineNumber > 0) {
            const resolved = PA.claude._resolveProcId(procedureName);
            if (resolved) {
                const base = resolved.includes('/') ? resolved.substring(0, resolved.indexOf('/')) : resolved;
                const parts = base.split('.');
                if (parts.length >= 2) {
                    PA.codeModal.openAtLine(parts[0] + '.' + parts[1], lineNumber);
                    return;
                }
            }
            if (resolved) PA.showProcedure(resolved);
        } else if (procedureName) {
            const resolved = PA.claude._resolveProcId(procedureName);
            if (resolved) PA.showProcedure(resolved);
        }
    },

    // ==================== SCOPE ====================

    /** Apply scope: re-render with only tables from the selected proc's subtree */
    applyScope() {
        if (PA.claude.result) {
            PA.claude.renderResult(PA.claude.result);
        }
    },

    // ==================== STATUS & CONTROL ====================

    async checkStatus() {
        try {
            const status = await PA.api.getClaudeStatus();
            if (!status) { PA.toast('Could not reach Claude API', 'error'); return; }

            if (!status.cliAvailable) {
                PA.toast('Claude CLI not available on this system', 'error');
                return;
            }

            const currentAnalysis = status.currentAnalysis || '';
            const loadedAnalysis = PA.analysisData ? (PA.analysisData.name || '') : '';

            const progress = status.progress;
            if (progress && progress.running) {
                if (loadedAnalysis && currentAnalysis && loadedAnalysis !== currentAnalysis) {
                    PA.toast('Claude verification running for "' + currentAnalysis + '" but loaded analysis is "' + loadedAnalysis + '"', 'error');
                }
                document.getElementById('claudeProgress').style.display = '';
                document.getElementById('claudeKillBtn').style.display = '';
                const pct = progress.percentComplete || 0;
                document.getElementById('claudeProgressFill').style.width = pct + '%';
                const analysisLabel = currentAnalysis ? ` [${currentAnalysis}]` : '';
                document.getElementById('claudeProgressText').textContent =
                    `${progress.completedChunks || 0}/${progress.totalChunks || 0} chunks (${pct}%)${analysisLabel}`;
                document.getElementById('claudeSummary').innerHTML =
                    '<span class="badge" style="background:var(--badge-blue-bg);color:var(--badge-blue)">RUNNING</span>' +
                    `<span class="badge">${progress.completedChunks || 0}/${progress.totalChunks || 0} chunks</span>` +
                    (currentAnalysis ? `<span class="badge">${PA.esc(currentAnalysis)}</span>` : '');
                PA.claude.startPoll();
                PA.claude._setButtonsRunning(true);
                // Sync topbar button/progress
                const hdrBtn = document.getElementById('enableClaudeBtn');
                if (hdrBtn) hdrBtn.style.display = 'none';
                PA._showClaudeHeaderProgress(true, pct);
                PA._startClaudeHeaderPoll();
                PA.toast('Claude verification running' + analysisLabel + ' (' + pct + '% complete)', 'success');
                if (status.hasVerification) {
                    await PA.claude.loadResult();
                }
                return;
            }

            PA.claude._setButtonsRunning(false);
            // Ensure topbar button visible when not running
            const hdrBtn = document.getElementById('enableClaudeBtn');
            if (hdrBtn && PA.analysisData) hdrBtn.style.display = '';
            PA._showClaudeHeaderProgress(false, 0);

            const label = loadedAnalysis ? ` for "${loadedAnalysis}"` : '';
            PA.toast('Claude CLI available. ' +
                (status.hasVerification ? 'Loading verification results' + label + '...' : 'No verification' + label + '.'), 'success');

            if (status.hasVerification) {
                await PA.claude.loadResult();
            }
            if (progress && progress.isComplete) {
                document.getElementById('claudeResumeBtn').style.display = 'none';
            } else if (progress && progress.totalChunks > 0 && progress.completedChunks > 0) {
                document.getElementById('claudeResumeBtn').style.display = '';
            }
        } catch (e) {
            PA.toast('Error checking status: ' + e.message, 'error');
        }
    },

    async startVerification(resume) {
        if (PA.claude.pollTimer) {
            PA.toast('Verification is already running. Wait for it to finish or cancel first.', 'error');
            return;
        }
        try {
            const status = await PA.api.getClaudeStatus();
            if (status && status.progress && status.progress.running) {
                PA.toast('Verification already running on server. Cancel it first or wait.', 'error');
                return;
            }
        } catch (e) { /* proceed anyway */ }

        if (!confirm(resume
            ? 'Resume Claude verification from checkpoint?'
            : 'Start fresh Claude verification? This will process all table operations in chunks.')) return;

        try {
            const resp = await PA.api.startClaudeVerification(resume);
            if (resp.error) { PA.toast(resp.error, 'error'); return; }

            PA.toast('Verification started (session: ' + resp.sessionId + ')', 'success');
            PA.claude._setButtonsRunning(true);

            document.getElementById('claudeProgress').style.display = '';
            document.getElementById('claudeKillBtn').style.display = '';
            document.getElementById('claudeProgressText').textContent = 'Starting verification...';
            document.getElementById('claudeProgressFill').style.width = '2%';
            document.getElementById('claudeSummary').innerHTML =
                '<span class="badge" style="background:var(--badge-blue-bg);color:var(--badge-blue)">RUNNING</span>';

            PA.claude.startPoll();

            try {
                PA.claude.sseConnection = PA.api.connectClaudeProgress((msg) => {
                    document.getElementById('claudeProgressText').textContent = msg;
                    const m = msg.match(/Chunk\s+(\d+)\/(\d+)/i);
                    if (m) {
                        const pct = Math.round((parseInt(m[1]) / parseInt(m[2])) * 100);
                        document.getElementById('claudeProgressFill').style.width = pct + '%';
                    }
                });
            } catch (sseErr) {
                console.warn('[Claude] SSE not available, using polling only:', sseErr);
            }
        } catch (e) {
            PA.toast('Failed to start: ' + e.message, 'error');
        }
    },

    async killAll() {
        if (!confirm('Stop all running Claude sessions?')) return;
        try {
            const resp = await PA.api.killAllClaudeSessions();
            PA.toast('Killed ' + (resp.killed || 0) + ' sessions', 'success');
            PA.claude.stopPoll();
            PA.claude._setButtonsRunning(false);
            document.getElementById('claudeProgress').style.display = 'none';
            document.getElementById('claudeKillBtn').style.display = 'none';
        } catch (e) {
            PA.toast('Error: ' + e.message, 'error');
        }
    },

    _setButtonsRunning(running) {
        const startBtn = document.getElementById('claudeStartBtn');
        const resumeBtn = document.getElementById('claudeResumeBtn');
        if (startBtn) { startBtn.disabled = running; startBtn.style.opacity = running ? '0.5' : ''; }
        if (resumeBtn) { resumeBtn.disabled = running; resumeBtn.style.opacity = running ? '0.5' : ''; }
    },

    startPoll() {
        PA.claude.stopPoll();
        const POLL_MAX_MS = 2 * 60 * 60 * 1000;
        const pollStart = Date.now();
        let errorCount = 0;
        PA.claude.pollTimer = setInterval(async () => {
            if (Date.now() - pollStart > POLL_MAX_MS) {
                PA.claude.stopPoll();
                PA.toast('Claude poll timeout (2h)', 'error');
                return;
            }
            try {
                const progress = await PA.api.getClaudeProgress();
                if (!progress) { errorCount++; if (errorCount > 10) { PA.claude.stopPoll(); } return; }
                errorCount = 0;

                const pct = progress.percentComplete || 0;
                const completedChunks = progress.completedChunks || 0;
                const totalChunks = progress.totalChunks || 0;
                const errorChunks = progress.errorChunks || 0;

                document.getElementById('claudeProgressFill').style.width = pct + '%';
                const analysisLabel = progress.analysisName ? ` [${progress.analysisName}]` : '';
                document.getElementById('claudeProgressText').textContent =
                    `${completedChunks}/${totalChunks} chunks (${Math.round(pct)}%)${errorChunks > 0 ? ' \u2014 ' + errorChunks + ' errors' : ''}${analysisLabel}`;

                document.getElementById('claudeSummary').innerHTML =
                    `<span class="badge">${completedChunks}/${totalChunks} chunks</span>` +
                    `<span class="badge" style="background:var(--badge-green-bg);color:var(--badge-green)">${Math.round(pct)}%</span>` +
                    (errorChunks > 0 ? `<span class="badge" style="background:var(--badge-red-bg);color:var(--badge-red)">${errorChunks} errors</span>` : '') +
                    (progress.running ? '<span class="badge" style="background:var(--badge-blue-bg);color:var(--badge-blue)">RUNNING</span>' : '');

                if (!progress.running && completedChunks >= totalChunks && totalChunks > 0) {
                    PA.claude.stopPoll();
                    PA.claude._setButtonsRunning(false);
                    document.getElementById('claudeProgress').style.display = 'none';
                    document.getElementById('claudeKillBtn').style.display = 'none';
                    PA.toast('Claude verification complete!', 'success');
                    await PA.claude.loadResult();
                } else if (!progress.running && completedChunks > 0) {
                    PA.claude.stopPoll();
                    PA.claude._setButtonsRunning(false);
                    document.getElementById('claudeProgressText').textContent =
                        `Stopped at ${completedChunks}/${totalChunks} chunks. Click Resume to continue.`;
                    document.getElementById('claudeResumeBtn').style.display = '';
                    document.getElementById('claudeKillBtn').style.display = 'none';
                    await PA.claude.loadResult();
                } else if (!progress.running && completedChunks === 0) {
                    PA.claude.stopPoll();
                }
            } catch (e) {
                console.warn('[Claude] Poll error:', e);
                errorCount++;
                if (errorCount > 10) { PA.claude.stopPoll(); PA.toast('Claude poll stopped (errors)', 'error'); }
            }
        }, PollConfig.claudeProgressMs);
    },

    stopPoll() {
        if (PA.claude.pollTimer) { clearInterval(PA.claude.pollTimer); PA.claude.pollTimer = null; }
        if (PA.claude.sseConnection) { PA.claude.sseConnection.close(); PA.claude.sseConnection = null; }
    },

    // ==================== RESULTS ====================

    async loadResult() {
        try {
            const result = await PA.api.getClaudeResult();
            if (!result || result.status === 'no_verification') {
                document.getElementById('claudeContainer').innerHTML =
                    '<div class="empty-msg">No verification data. Click "Start Verification" to begin.</div>';
                document.getElementById('claudeFilters').style.display = 'none';
                return;
            }
            if (result.status === 'no_analysis') {
                document.getElementById('claudeContainer').innerHTML =
                    '<div class="empty-msg">No analysis loaded. Run an analysis first.</div>';
                document.getElementById('claudeFilters').style.display = 'none';
                return;
            }
            PA.claude.result = result;
            PA.claude.hasData = true;
            PA.claude.page = 1;
            PA.claude._initFilters();
            // Show the header-level Static/Claude toggle now that data exists
            if (PA._showViewToggleIfClaude) PA._showViewToggleIfClaude();
            // Load table→chunk mapping for per-table log viewing
            try { PA.claude.tableChunkMap = await PA.api.getTableChunkMapping(); }
            catch (e) { PA.claude.tableChunkMap = {}; }
            PA.claude.renderResult(result);
            // Auto-merge Claude results into analysis data
            if (result.tables && result.tables.length > 0 && !PA.context.claudeMerged) {
                PA._mergeClaudeResults(result);
            }
        } catch (e) {
            document.getElementById('claudeContainer').innerHTML =
                '<div class="empty-msg">Error loading result: ' + PA.esc(e.message) + '</div>';
        }
    },

    // ==================== FILTERS & VIEW ====================

    _initFilters() {
        document.getElementById('claudeFilters').style.display = '';
        // Init op pills
        const container = document.getElementById('claudeOpPills');
        if (container) {
            const ops = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'];
            let html = '';
            for (const op of ops) {
                html += `<span class="op-filter-pill ${op} active" data-op="${op}" onclick="PA.claude.toggleOp('${op}')">${op}</span>`;
            }
            container.innerHTML = html;
        }
        PA.claude.activeOps.clear();
        // Reset view toggle
        document.querySelectorAll('.view-toggle button').forEach(b => b.classList.toggle('active', b.dataset.cv === PA.claude.viewMode));
    },

    setView(mode) {
        PA.claude.viewMode = mode;
        PA.claude.page = 1;
        document.querySelectorAll('.view-toggle button').forEach(b => b.classList.toggle('active', b.dataset.cv === mode));
        if (PA.claude.result) PA.claude.renderResult(PA.claude.result);
    },

    toggleOp(op) {
        const pill = document.querySelector(`#claudeOpPills .op-filter-pill[data-op="${op}"]`);
        if (!pill) return;
        if (PA.claude.activeOps.size === 0) {
            PA.claude.activeOps.add(op);
            document.querySelectorAll('#claudeOpPills .op-filter-pill').forEach(p => {
                p.classList.toggle('active', p.dataset.op === op);
            });
        } else if (PA.claude.activeOps.has(op)) {
            PA.claude.activeOps.delete(op);
            pill.classList.remove('active');
            if (PA.claude.activeOps.size === 0) {
                document.querySelectorAll('#claudeOpPills .op-filter-pill').forEach(p => p.classList.add('active'));
            }
        } else {
            PA.claude.activeOps.add(op);
            pill.classList.add('active');
        }
        PA.claude.page = 1;
        if (PA.claude.result) PA.claude.renderResult(PA.claude.result);
    },

    clearFilters() {
        document.getElementById('claudeSearch').value = '';
        document.getElementById('claudeStatusFilter').value = '';
        PA.claude.activeOps.clear();
        PA.claude.viewMode = 'both';
        PA.claude.page = 1;
        document.querySelectorAll('#claudeOpPills .op-filter-pill').forEach(p => p.classList.add('active'));
        document.querySelectorAll('.view-toggle button').forEach(b => b.classList.toggle('active', b.dataset.cv === 'both'));
        if (PA.claude.result) PA.claude.renderResult(PA.claude.result);
    },

    applyFilters() {
        PA.claude.page = 1;
        if (PA.claude.result) PA.claude.renderResult(PA.claude.result);
    },

    _filterTables(tables) {
        let list = [...tables];

        // Scope filtering: when a proc is selected, only show tables in the proc's subtree
        const ctx = PA.context;
        if (ctx && ctx.procId && ctx.scopedTables && ctx.scopedTables.length > 0) {
            const scopedKeys = new Set();
            const scopedNames = new Set();
            for (const st of ctx.scopedTables) {
                const name = (st.tableName || '').toUpperCase();
                const schema = (st.schemaName || '').toUpperCase();
                scopedNames.add(name);
                if (schema) scopedKeys.add(schema + '.' + name);
            }
            list = list.filter(t => {
                const name = (t.tableName || '').toUpperCase();
                const schema = (t.schemaName || '').toUpperCase();
                if (schema && scopedKeys.size > 0) return scopedKeys.has(schema + '.' + name);
                return scopedNames.has(name);
            });
        }

        const search = (document.getElementById('claudeSearch')?.value || '').toUpperCase();
        const statusFilter = document.getElementById('claudeStatusFilter')?.value || '';
        const opFilter = PA.claude.activeOps;

        if (search) {
            list = list.filter(t => (t.tableName || '').toUpperCase().includes(search) ||
                                     (t.schemaName || '').toUpperCase().includes(search));
        }
        if (statusFilter) {
            list = list.filter(t => t.overallStatus === statusFilter);
        }
        if (opFilter.size > 0) {
            list = list.filter(t => {
                const allOps = new Set([...(t.staticOperations || []),
                    ...(t.claudeVerifications || []).map(v => v.operation).filter(Boolean)]);
                return [...opFilter].some(o => allOps.has(o));
            });
        }
        return list;
    },

    // ==================== RENDERING ====================

    renderResult(result) {
        const container = document.getElementById('claudeContainer');
        if (!result || !result.tables) {
            container.innerHTML = '<div class="empty-msg">No verification data</div>';
            return;
        }

        // Summary badges
        const isPartial = result.error && result.error.startsWith('Partial result');
        const summary = document.getElementById('claudeSummary');
        summary.innerHTML = `
            ${isPartial ? `<span class="badge" style="background:var(--badge-orange-bg);color:var(--badge-orange)" title="Not all chunks completed \u2014 click Resume to continue">PARTIAL</span>` : ''}
            <span class="badge" style="background:var(--badge-green-bg);color:var(--badge-green)">${result.confirmedCount || 0} Confirmed</span>
            <span class="badge" style="background:var(--badge-red-bg);color:var(--badge-red)">${result.removedCount || 0} Removed</span>
            <span class="badge" style="background:var(--badge-blue-bg);color:var(--badge-blue)">${result.newCount || 0} New</span>
            <span class="badge">${isPartial ? (result.error.match(/\d+\/\d+/) || [result.totalChunks + ' chunks'])[0] + ' chunks' : (result.totalChunks || 0) + ' chunks'}</span>
            ${result.errorChunks > 0 ? `<span class="badge" style="background:var(--badge-orange-bg);color:var(--badge-orange)">${result.errorChunks} errors</span>` : ''}
        `;

        // Sort and filter
        const order = { MODIFIED: 0, PARTIAL: 1, EXPANDED: 2, CONFIRMED: 3, UNVERIFIED: 4 };
        const sorted = [...result.tables].sort((a, b) =>
            (order[a.overallStatus] || 5) - (order[b.overallStatus] || 5));
        const filtered = PA.claude._filterTables(sorted);

        // Pagination
        const total = filtered.length;
        const totalPages = Math.ceil(total / PA.claude.pageSize) || 1;
        if (PA.claude.page > totalPages) PA.claude.page = totalPages;
        const start = (PA.claude.page - 1) * PA.claude.pageSize;
        const pageData = filtered.slice(start, start + PA.claude.pageSize);

        // Filtered count badge
        const fc = document.getElementById('claudeFilteredCount');
        if (total < result.tables.length) { fc.textContent = total + '/' + result.tables.length + ' shown'; fc.style.display = ''; }
        else { fc.style.display = 'none'; }

        // Render based on view mode
        const mode = PA.claude.viewMode;
        let html = '';

        if (mode === 'static') {
            html = PA.claude._renderStaticView(pageData);
        } else if (mode === 'claude') {
            html = PA.claude._renderClaudeView(pageData);
        } else {
            html = PA.claude._renderBothView(pageData);
        }

        // Pagination bar
        let paginationHtml = '';
        if (total > 20) {
            paginationHtml = PA.claude._renderPagination(total, totalPages);
        }

        container.innerHTML = '<div style="flex:1;overflow:auto">' + html + '</div>' + paginationHtml;
    },

    // --- Both view (comparison) ---
    _renderBothView(list) {
        let html = '<table class="to-table"><thead><tr>';
        html += '<th>Table</th><th>Schema</th><th>Status</th><th>Static Ops</th><th>Claude Findings</th><th>Actions</th>';
        html += '</tr></thead><tbody>';

        for (let idx = 0; idx < list.length; idx++) {
            const t = list[idx];
            html += PA.claude._renderComparisonRow(t, idx);
        }
        html += '</tbody></table>';
        return html;
    },

    // --- Static-only view ---
    _renderStaticView(list) {
        let html = '<table class="to-table"><thead><tr>';
        html += '<th>Table</th><th>Schema</th><th>Operations</th><th>Access Count</th><th>External</th><th>Actions</th>';
        html += '</tr></thead><tbody>';

        for (let idx = 0; idx < list.length; idx++) {
            const t = list[idx];
            const ops = (t.staticOperations || []).map(op => `<span class="op-badge ${op}">${op}</span>`).join('');
            const scope = t.isExternal
                ? '<span class="scope-badge ext">EXTERNAL</span>'
                : '<span class="scope-badge int">INTERNAL</span>';

            html += `<tr class="to-row" onclick="PA.claude.toggleDetail(${idx})">`;
            html += `<td style="font-weight:600;color:var(--teal)">${PA.esc(t.tableName || '')}</td>`;
            html += `<td>${PA.esc(t.schemaName || '-')}</td>`;
            html += `<td>${ops}</td>`;
            html += `<td>${t.staticAccessCount || 0}</td>`;
            html += `<td>${scope}</td>`;
            html += `<td><button class="btn btn-sm" onclick="event.stopPropagation(); PA.tableInfo.open('${PA.escJs(t.tableName)}', '${PA.escJs(t.schemaName || '')}')">Table Info</button></td>`;
            html += `</tr>`;

            html += `<tr class="to-detail-row" id="claude-detail-${idx}"><td colspan="6">`;
            html += '<div class="to-detail"><div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Static Analysis Operations</div>';
            html += '<div class="to-detail-item">';
            for (const op of (t.staticOperations || [])) {
                html += `<span class="op-badge ${op}">${op}</span>`;
            }
            html += ` <span style="font-size:10px;color:var(--text-muted)">(${t.staticAccessCount || 0} accesses)</span>`;
            html += '</div></div></div>';
            html += `</td></tr>`;
        }
        html += '</tbody></table>';
        return html;
    },

    // --- Claude-only view ---
    _renderClaudeView(list) {
        // Filter to only tables with Claude data
        const withClaude = list.filter(t => (t.claudeVerifications || []).length > 0);
        if (withClaude.length === 0) {
            return '<div class="empty-msg">No Claude verification data for current filter</div>';
        }

        let html = '<table class="to-table"><thead><tr>';
        html += '<th>Table</th><th>Schema</th><th>Status</th><th>Verified Ops</th><th>Details</th><th>Actions</th>';
        html += '</tr></thead><tbody>';

        for (let idx = 0; idx < withClaude.length; idx++) {
            const t = withClaude[idx];
            const statusClass = {
                CONFIRMED: 'badge-green', PARTIAL: 'badge-orange',
                EXPANDED: 'badge-blue', MODIFIED: 'badge-red', UNVERIFIED: 'badge-gray'
            }[t.overallStatus] || 'badge-gray';

            const vlist = t.claudeVerifications || [];
            const confirmed = vlist.filter(v => v.status === 'CONFIRMED').length;
            const removed = vlist.filter(v => v.status === 'REMOVED').length;
            const newOps = vlist.filter(v => v.status === 'NEW').length;

            // All ops from Claude
            const ops = [...new Set(vlist.map(v => v.operation).filter(Boolean))];
            const opBadges = ops.map(op => `<span class="op-badge ${op}">${op}</span>`).join('');

            let detailSummary = '';
            if (confirmed > 0) detailSummary += `<span style="color:var(--badge-green)">${confirmed} OK</span> `;
            if (removed > 0) detailSummary += `<span style="color:var(--badge-red)">${removed} removed</span> `;
            if (newOps > 0) detailSummary += `<span style="color:var(--badge-blue)">${newOps} new</span> `;

            html += `<tr class="to-row" onclick="PA.claude.toggleDetail(${idx})">`;
            html += `<td style="font-weight:600;color:var(--teal)">${PA.esc(t.tableName || '')}</td>`;
            html += `<td>${PA.esc(t.schemaName || '-')}</td>`;
            html += `<td><span class="badge" style="background:var(--${statusClass}-bg);color:var(--${statusClass})">${t.overallStatus}</span></td>`;
            html += `<td>${opBadges}</td>`;
            html += `<td style="font-size:11px">${detailSummary || '-'}</td>`;
            html += `<td><button class="btn btn-sm" onclick="event.stopPropagation(); PA.tableInfo.open('${PA.escJs(t.tableName)}', '${PA.escJs(t.schemaName || '')}')">Table Info</button></td>`;
            html += `</tr>`;

            html += `<tr class="to-detail-row" id="claude-detail-${idx}"><td colspan="6">`;
            html += PA.claude._renderClaudeDetail(t);
            html += `</td></tr>`;
        }
        html += '</tbody></table>';
        return html;
    },

    _renderClaudeDetail(table) {
        const vlist = table.claudeVerifications || [];
        if (vlist.length === 0) return '<div class="to-detail"><div class="empty-msg">No Claude data</div></div>';

        let html = '<div class="to-detail"><div class="to-detail-section">';
        html += '<div class="to-detail-section-title">Claude Verification Details (' + vlist.length + ')</div>';
        for (const v of vlist) {
            html += PA.claude._renderVerificationItem(v);
        }
        html += '</div></div>';
        return html;
    },

    _renderVerificationItem(v) {
        const statusColor = { CONFIRMED: 'var(--badge-green)', REMOVED: 'var(--badge-red)', NEW: 'var(--badge-blue)' }[v.status] || 'var(--text-muted)';
        const statusBg = { CONFIRMED: 'var(--badge-green-bg)', REMOVED: 'var(--badge-red-bg)', NEW: 'var(--badge-blue-bg)' }[v.status] || 'var(--badge-gray-bg)';
        const procRef = v.procedureId || v.procedureName || '';
        const sf = v.sourceFile || '';
        let html = '<div class="to-detail-item">';
        html += `<span class="badge" style="background:${statusBg};color:${statusColor};font-size:9px">${v.status || '?'}</span>`;
        html += `<span class="op-badge ${v.operation || ''}">${v.operation || '?'}</span>`;
        if (procRef) html += `<span class="to-detail-proc" onclick="event.stopPropagation(); PA.claude._openProc('${PA.escJs(procRef)}')">${PA.esc(v.procedureName || procRef)}</span>`;
        if (v.lineNumber > 0 && sf) html += `<span class="to-detail-line" onclick="event.stopPropagation(); PA.codeModal.openAtLine('${PA.escJs(sf)}', ${v.lineNumber})">L${v.lineNumber}</span>`;
        else if (v.lineNumber > 0 && procRef) html += `<span class="to-detail-line" onclick="event.stopPropagation(); PA.claude._openProcAtLine('${PA.escJs(procRef)}', '', ${v.lineNumber})">L${v.lineNumber}</span>`;
        if (v.reason) html += `<span style="font-size:10px;color:var(--text-muted);margin-left:8px">${PA.esc(v.reason)}</span>`;
        html += '</div>';
        return html;
    },

    _renderComparisonRow(t, idx) {
        const statusClass = {
            CONFIRMED: 'badge-green', PARTIAL: 'badge-orange',
            EXPANDED: 'badge-blue', MODIFIED: 'badge-red', UNVERIFIED: 'badge-gray'
        }[t.overallStatus] || 'badge-gray';

        const staticOps = (t.staticOperations || []).map(op => `<span class="op-badge ${op}">${op}</span>`).join('');

        let claudeSummary = '';
        const vlist = t.claudeVerifications || [];
        const confirmed = vlist.filter(v => v.status === 'CONFIRMED').length;
        const removed = vlist.filter(v => v.status === 'REMOVED').length;
        const newOps = vlist.filter(v => v.status === 'NEW').length;
        if (confirmed > 0) claudeSummary += `<span style="color:var(--badge-green)">${confirmed} OK</span> `;
        if (removed > 0) claudeSummary += `<span style="color:var(--badge-red)">${removed} removed</span> `;
        if (newOps > 0) claudeSummary += `<span style="color:var(--badge-blue)">${newOps} new</span> `;
        if (!claudeSummary) claudeSummary = '<span style="color:var(--text-muted)">-</span>';

        let html = `<tr class="to-row" onclick="PA.claude.toggleDetail(${idx})">`;
        html += `<td style="font-weight:600;color:var(--teal)">${PA.esc(t.tableName || '')}</td>`;
        html += `<td>${PA.esc(t.schemaName || '-')}</td>`;
        html += `<td><span class="badge" style="background:var(--${statusClass}-bg);color:var(--${statusClass})">${t.overallStatus}</span></td>`;
        html += `<td>${staticOps}</td>`;
        html += `<td style="font-size:11px">${claudeSummary}</td>`;
        const chunkIds = (PA.claude.tableChunkMap || {})[(t.tableName || '').toUpperCase()] || [];
        let actionsHtml = `<button class="btn btn-sm" onclick="event.stopPropagation(); PA.tableInfo.open('${PA.escJs(t.tableName)}', '${PA.escJs(t.schemaName || '')}')">Table Info</button>`;
        if (chunkIds.length > 0) {
            actionsHtml += ` <button class="btn btn-sm" onclick="event.stopPropagation(); PA.claude.showChunkLog('${PA.escJs(chunkIds[0])}')" title="View Claude prompt &amp; response for this table">View Log</button>`;
            if (chunkIds.length > 1) {
                actionsHtml += ` <span style="font-size:10px;color:var(--text-muted)">(${chunkIds.length} chunks)</span>`;
            }
        }
        html += `<td>${actionsHtml}</td>`;
        html += `</tr>`;

        html += `<tr class="to-detail-row" id="claude-detail-${idx}"><td colspan="6">`;
        html += PA.claude.renderDetail(t);
        html += `</td></tr>`;
        return html;
    },

    renderDetail(table) {
        let html = '<div class="to-detail">';

        const vlist = table.claudeVerifications || [];
        if (vlist.length > 0) {
            html += '<div class="to-detail-section">';
            html += '<div class="to-detail-section-title">Claude Verification Details (' + vlist.length + ')</div>';
            for (const v of vlist) {
                html += PA.claude._renderVerificationItem(v);
            }
            html += '</div>';
        }

        if (table.staticOperations && table.staticOperations.length > 0) {
            html += '<div class="to-detail-section" style="border-top:1px solid var(--border);padding-top:8px">';
            html += '<div class="to-detail-section-title">Static Analysis Operations</div>';
            html += '<div class="to-detail-item">';
            for (const op of table.staticOperations) {
                html += `<span class="op-badge ${op}">${op}</span>`;
            }
            html += ` <span style="font-size:10px;color:var(--text-muted)">(${table.staticAccessCount || 0} accesses)</span>`;
            html += '</div></div>';
        }

        // Chunk log links
        const chunkIds = (PA.claude.tableChunkMap || {})[(table.tableName || '').toUpperCase()] || [];
        if (chunkIds.length > 0) {
            html += '<div class="to-detail-section" style="border-top:1px solid var(--border);padding-top:8px">';
            html += '<div class="to-detail-section-title">Prompt Logs (' + chunkIds.length + ' chunk' + (chunkIds.length > 1 ? 's' : '') + ')</div>';
            html += '<div class="to-detail-item" style="flex-wrap:wrap;gap:4px">';
            for (const cid of chunkIds) {
                html += `<button class="btn btn-sm" style="font-size:10px" onclick="event.stopPropagation(); PA.claude.showChunkLog('${PA.escJs(cid)}')">${PA.esc(cid)}</button>`;
            }
            html += '</div></div>';
        }

        html += '</div>';
        return html;
    },

    // ==================== PAGINATION ====================

    _renderPagination(total, totalPages) {
        const p = PA.claude.page;
        let html = '<div class="pagination-bar">';
        html += '<div class="page-info">' + total + ' tables</div>';
        html += '<div class="page-size"><select onchange="PA.claude.setPageSize(+this.value)">';
        for (const sz of [20, 50, 100]) {
            html += `<option value="${sz}" ${PA.claude.pageSize === sz ? 'selected' : ''}>${sz}/page</option>`;
        }
        html += `<option value="${total}" ${PA.claude.pageSize >= total ? 'selected' : ''}>All</option>`;
        html += '</select></div>';

        html += '<div class="page-btns">';
        html += `<button onclick="PA.claude.goPage(1)" ${p <= 1 ? 'disabled' : ''}>&laquo;</button>`;
        html += `<button onclick="PA.claude.goPage(${p - 1})" ${p <= 1 ? 'disabled' : ''}>&lsaquo;</button>`;
        let startP = Math.max(1, p - 3), endP = Math.min(totalPages, startP + 6);
        if (endP - startP < 6) startP = Math.max(1, endP - 6);
        for (let i = startP; i <= endP; i++) {
            html += `<button onclick="PA.claude.goPage(${i})" class="${i === p ? 'active' : ''}">${i}</button>`;
        }
        html += `<button onclick="PA.claude.goPage(${p + 1})" ${p >= totalPages ? 'disabled' : ''}>&rsaquo;</button>`;
        html += `<button onclick="PA.claude.goPage(${totalPages})" ${p >= totalPages ? 'disabled' : ''}>&raquo;</button>`;
        html += '</div></div>';
        return html;
    },

    goPage(p) { PA.claude.page = Math.max(1, p); if (PA.claude.result) PA.claude.renderResult(PA.claude.result); },
    setPageSize(sz) { PA.claude.pageSize = sz; PA.claude.page = 1; if (PA.claude.result) PA.claude.renderResult(PA.claude.result); },

    toggleDetail(idx) {
        const row = document.getElementById('claude-detail-' + idx);
        if (!row) return;
        const parentRow = row.previousElementSibling;
        if (row.classList.contains('open')) {
            row.classList.remove('open');
            if (parentRow) parentRow.classList.remove('expanded');
        } else {
            row.classList.add('open');
            if (parentRow) parentRow.classList.add('expanded');
        }
    },

    // ==================== SESSIONS ====================

    _sessionsData: [],
    _sessionsFilter: {},

    onHomeSearch(q) {
        PA._claudeHomeSearch = q || '';
        PA._renderClaudeHome();
    },

    async showSessions() {
        const modal = document.getElementById('claudeSessionsModal');
        modal.style.display = '';
        const container = document.getElementById('claudeSessionsList');
        container.innerHTML = '<div class="empty-msg">Loading...</div>';

        try {
            const analysisName = PA.analysisData ? PA.analysisData.name : null;
            const sessions = await PA.api.listClaudeSessions(analysisName);
            PA.claude._sessionsData = sessions || [];
            PA.claude._sessionsFilter = {};
            PA.claude._renderSessionsTable();
        } catch (e) {
            container.innerHTML = '<div class="empty-msg">Error: ' + PA.esc(e.message) + '</div>';
        }
    },

    _filterSessions(col, value) {
        if (value) PA.claude._sessionsFilter[col] = value;
        else delete PA.claude._sessionsFilter[col];
        PA.claude._renderSessionsTable();
    },

    _renderSessionsTable() {
        const container = document.getElementById('claudeSessionsList');
        const all = PA.claude._sessionsData;
        const filters = PA.claude._sessionsFilter;

        const filtered = all.filter(s => {
            for (const [col, val] of Object.entries(filters)) {
                const sv = String(s[col] || '').toUpperCase();
                if (!sv.includes(val.toUpperCase())) return false;
            }
            return true;
        });

        if (all.length === 0) {
            container.innerHTML = '<div class="empty-msg">No Claude sessions found</div>';
            return;
        }

        // Summary bar
        const running = all.filter(s => s.status === 'RUNNING').length;
        const complete = all.filter(s => s.status === 'COMPLETE').length;
        const failed = all.filter(s => s.status === 'FAILED').length;
        const killed = all.filter(s => s.status === 'KILLED').length;

        let html = '<div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap">';
        html += `<span class="badge" style="font-size:11px">${all.length} total</span>`;
        if (running) html += `<span class="badge" style="background:var(--badge-blue-bg);color:var(--badge-blue);font-size:11px">${running} running</span>`;
        if (complete) html += `<span class="badge" style="background:var(--badge-green-bg);color:var(--badge-green);font-size:11px">${complete} complete</span>`;
        if (failed) html += `<span class="badge" style="background:#fee2e2;color:#b91c1c;font-size:11px">${failed} failed</span>`;
        if (killed) html += `<span class="badge" style="background:#fef3c7;color:#a16207;font-size:11px">${killed} killed</span>`;
        if (Object.keys(filters).length > 0) html += `<span style="font-size:10px;color:var(--text-muted)">${filtered.length} shown</span>`;
        if (running > 0) html += `<button class="btn btn-sm btn-danger" onclick="PA.claude._killAllSessions()" style="margin-left:auto">Kill All Running</button>`;
        html += '</div>';

        // Table with filter row
        html += '<table class="to-table"><thead>';
        html += '<tr><th>ID</th><th>Analysis</th><th>Type</th><th>Status</th><th>Detail</th><th>Started</th><th>Duration</th><th>Actions</th></tr>';
        html += '<tr style="background:var(--bg)">';
        const filterCols = ['id', 'analysisName', 'type', 'status', 'detail', 'startedAt', '', ''];
        for (const col of filterCols) {
            if (col) {
                const val = filters[col] || '';
                html += `<th style="padding:2px"><input type="text" class="form-input" style="font-size:10px;padding:2px 4px;width:100%" placeholder="Filter..." value="${PA.esc(val)}" oninput="PA.claude._filterSessions('${col}',this.value)"></th>`;
            } else {
                html += '<th></th>';
            }
        }
        html += '</tr></thead><tbody>';

        for (const s of filtered) {
            const statusColor = { RUNNING: 'var(--blue)', COMPLETE: 'var(--badge-green)', FAILED: 'var(--badge-red)', KILLED: 'var(--badge-orange,#f59e0b)' }[s.status] || 'var(--text-muted)';
            const startTime = s.startedAt ? s.startedAt.replace('T', ' ').substring(0, 19) : '-';
            html += '<tr class="to-row">';
            html += `<td style="font-family:var(--font-mono);font-size:10px">${PA.esc(s.id || '')}</td>`;
            html += `<td style="font-size:10px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${PA.esc(s.analysisName || '')}">${PA.esc(s.analysisName || '-')}</td>`;
            html += `<td><span class="badge" style="font-size:9px">${PA.esc(s.type || '')}</span></td>`;
            html += `<td style="color:${statusColor};font-weight:700;font-size:11px">${PA.esc(s.status || '')}</td>`;
            html += `<td style="font-size:10px;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${PA.esc(s.detail || '')}">${PA.esc(s.detail || '')}</td>`;
            html += `<td style="font-size:10px;font-family:var(--font-mono)">${PA.esc(startTime)}</td>`;
            html += `<td style="font-size:11px">${PA.esc(s.durationFormatted || '-')}</td>`;
            html += `<td>`;
            if (s.status === 'RUNNING') {
                html += `<button class="btn btn-sm btn-danger" onclick="PA.claude.killSession('${PA.escJs(s.id)}')">Kill</button>`;
            }
            html += `</td>`;
            html += '</tr>';
            if (s.error) {
                html += `<tr><td colspan="8" style="color:var(--badge-red);font-size:10px;padding:4px 12px">${PA.esc(s.error)}</td></tr>`;
            }
        }

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    async _killAllSessions() {
        if (!confirm('Kill all running Claude sessions?')) return;
        try {
            await PA.api.killAllClaudeSessions();
            PA.toast('All sessions killed', 'success');
            PA.claude.showSessions();
        } catch (e) {
            PA.toast('Error: ' + e.message, 'error');
        }
    },

    async killSession(sessionId) {
        if (!confirm('Kill session ' + sessionId + '?')) return;
        try {
            await PA.api.killClaudeSession(sessionId);
            PA.toast('Session killed', 'success');
            PA.claude.showSessions();
        } catch (e) {
            PA.toast('Error: ' + e.message, 'error');
        }
    },

    // ==================== CHUNK LOGS ====================

    async showChunkBrowser() {
        const modal = document.getElementById('claudeSessionsModal');
        modal.style.display = '';
        const container = document.getElementById('claudeSessionsList');
        container.innerHTML = '<div class="empty-msg">Loading chunks...</div>';

        try {
            const chunks = await PA.api.listClaudeChunks();
            if (!chunks || chunks.length === 0) {
                container.innerHTML = '<div class="empty-msg">No chunks found</div>';
                return;
            }

            let html = '<div style="font-size:12px;font-weight:700;margin-bottom:8px;color:var(--text-muted)">' + chunks.length + ' chunks</div>';
            html += '<table class="to-table"><thead><tr>';
            html += '<th>Chunk ID</th><th>Actions</th>';
            html += '</tr></thead><tbody>';

            for (const chunkId of chunks) {
                html += '<tr class="to-row">';
                html += `<td style="font-family:var(--font-mono);font-size:11px">${PA.esc(chunkId)}</td>`;
                html += `<td><button class="btn btn-sm" onclick="PA.claude.showChunkLog('${PA.escJs(chunkId)}')">View Log</button></td>`;
                html += '</tr>';
            }

            html += '</tbody></table>';
            container.innerHTML = html;
        } catch (e) {
            container.innerHTML = '<div class="empty-msg">Error: ' + PA.esc(e.message) + '</div>';
        }
    },

    async showChunkLog(chunkId) {
        const modal = document.getElementById('claudeLogModal');
        modal.style.display = '';
        document.getElementById('claudeLogTitle').textContent = 'Chunk: ' + chunkId;
        document.getElementById('claudeLogInput').innerHTML = '<div class="empty-msg">Loading...</div>';
        document.getElementById('claudeLogOutput').innerHTML = '<div class="empty-msg">Loading...</div>';

        try {
            const fragment = await PA.api.getClaudeChunk(chunkId);
            if (!fragment) { PA.toast('Chunk not found', 'error'); return; }

            const inputEl = document.getElementById('claudeLogInput');
            // Parse input to show prompt text prominently
            let inputObj = fragment.input;
            if (typeof inputObj === 'string') { try { inputObj = JSON.parse(inputObj); } catch(_) {} }
            let inputHtml = '';
            if (inputObj && typeof inputObj === 'object' && inputObj.prompt) {
                // Metadata summary
                inputHtml += '<div class="cl-section"><div class="cl-section-title">Chunk Metadata</div>';
                inputHtml += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">';
                if (inputObj.chunkId) inputHtml += '<span class="badge">' + PA.esc(inputObj.chunkId) + '</span>';
                if (inputObj.name) inputHtml += '<span class="badge">' + PA.esc(inputObj.name) + '</span>';
                if (inputObj.tableCount) inputHtml += '<span class="badge">' + inputObj.tableCount + ' tables</span>';
                if (inputObj.promptLength) inputHtml += '<span class="badge">' + inputObj.promptLength + ' chars</span>';
                if (inputObj.procedures) inputHtml += '<span class="badge">' + (Array.isArray(inputObj.procedures) ? inputObj.procedures.length : '?') + ' procs</span>';
                if (inputObj.timestamp) inputHtml += '<span class="badge" style="font-size:9px">' + PA.esc(inputObj.timestamp) + '</span>';
                inputHtml += '</div></div>';
                // Full prompt text
                inputHtml += '<div class="cl-section"><div class="cl-section-title">Full Prompt Text</div>';
                inputHtml += '<pre class="cl-prompt">' + PA.esc(inputObj.prompt) + '</pre></div>';
            } else {
                // Fallback: raw dump
                inputHtml += '<div class="cl-section"><div class="cl-section-title">INPUT (Raw)</div>';
                inputHtml += '<pre class="cl-prompt">' + PA.esc(typeof fragment.input === 'string' ? fragment.input : JSON.stringify(fragment.input, null, 2)) + '</pre></div>';
            }
            inputEl.innerHTML = inputHtml;

            const outputEl = document.getElementById('claudeLogOutput');
            let outputHtml = '<div style="font-size:11px;font-weight:700;margin-bottom:8px;color:var(--text-muted)">OUTPUT (Claude Response)</div>';
            if (fragment.error) {
                outputHtml += '<div style="color:var(--badge-red);padding:12px;background:var(--badge-red-bg);border-radius:6px">' + PA.esc(fragment.error) + '</div>';
            }
            outputHtml += '<pre style="font-size:11px;white-space:pre-wrap;color:var(--sidebar-text);background:#1e1e2e;padding:12px;border-radius:6px;overflow:auto;max-height:100%">'
                + PA.esc(typeof fragment.output === 'string' ? fragment.output : JSON.stringify(fragment.output, null, 2))
                + '</pre>';
            outputEl.innerHTML = outputHtml;
        } catch (e) {
            PA.toast('Error loading chunk: ' + e.message, 'error');
        }
    }
};

// ==================== TABLE INFO MODULE ====================
PA.tableInfo = {
    data: null,
    currentTab: 'columns',

    async open(tableName, schema) {
        const modal = document.getElementById('tableInfoModal');
        modal.style.display = '';
        document.getElementById('tableInfoTitle').textContent = tableName;
        document.getElementById('tableInfoSchema').textContent = schema || '?';
        document.getElementById('tableInfoContent').innerHTML = '<div class="empty-msg">Loading table info...</div>';

        try {
            let data = await PA.api.getTableMetadata(tableName);
            if (!data || !data.found) {
                data = await PA.api.getTableInfo(tableName, schema || null);
            }

            PA.tableInfo.data = data;
            if (!data || !data.found) {
                document.getElementById('tableInfoContent').innerHTML =
                    '<div class="empty-msg">Table not found in any configured schema</div>';
                return;
            }
            document.getElementById('tableInfoSchema').textContent = data.schema || '?';
            PA.tableInfo._updateTabs(data);
            PA.tableInfo.currentTab = 'columns';
            PA.tableInfo.renderTab();
        } catch (e) {
            document.getElementById('tableInfoContent').innerHTML =
                '<div class="empty-msg">Error: ' + PA.esc(e.message) + '</div>';
        }
    },

    _updateTabs(data) {
        const tabsEl = document.getElementById('tableInfoTabs');
        const oldDef = tabsEl.querySelector('[data-ti="definition"]');
        if (oldDef) oldDef.remove();

        if (data.isView && data.viewDefinition) {
            const btn = document.createElement('button');
            btn.className = 'btn btn-sm';
            btn.dataset.ti = 'definition';
            btn.textContent = 'Definition';
            btn.onclick = () => PA.tableInfo.switchTab('definition');
            const queryBtn = tabsEl.querySelector('[data-ti="query"]');
            if (queryBtn) tabsEl.insertBefore(btn, queryBtn);
            else tabsEl.appendChild(btn);
        }

        const titleEl = document.getElementById('tableInfoTitle');
        if (data.isView) {
            titleEl.innerHTML = PA.esc(data.tableName) +
                ' <span style="font-size:10px;color:var(--orange);font-weight:400;margin-left:6px">VIEW</span>';
        } else {
            titleEl.textContent = data.tableName;
        }
    },

    close() {
        document.getElementById('tableInfoModal').style.display = 'none';
    },

    switchTab(tab) {
        PA.tableInfo.currentTab = tab;
        document.querySelectorAll('#tableInfoTabs .btn').forEach(b => b.classList.remove('active'));
        document.querySelector(`#tableInfoTabs .btn[data-ti="${tab}"]`)?.classList.add('active');
        PA.tableInfo.renderTab();
    },

    renderTab() {
        const data = PA.tableInfo.data;
        const container = document.getElementById('tableInfoContent');
        if (!data) return;

        switch (PA.tableInfo.currentTab) {
            case 'columns': PA.tableInfo.renderColumns(container, data); break;
            case 'constraints': PA.tableInfo.renderConstraints(container, data); break;
            case 'indexes': PA.tableInfo.renderIndexes(container, data); break;
            case 'definition': PA.tableInfo.renderDefinition(container, data); break;
            case 'query': PA.tableInfo.renderQuery(container, data); break;
        }
    },

    _tiFilter(tableId) {
        const table = document.getElementById(tableId);
        if (!table) return;
        const filterRow = table.querySelector('.ti-filter-row');
        if (!filterRow) return;
        const inputs = filterRow.querySelectorAll('input');
        const filters = [];
        inputs.forEach((inp, i) => filters.push((inp.value || '').toUpperCase()));
        const rows = table.querySelectorAll('tbody tr');
        let shown = 0;
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            let match = true;
            filters.forEach((f, i) => {
                if (f && cells[i]) {
                    const txt = (cells[i].textContent || '').toUpperCase();
                    if (!txt.includes(f)) match = false;
                }
            });
            row.style.display = match ? '' : 'none';
            if (match) shown++;
        });
        const countEl = document.getElementById(tableId + '-count');
        if (countEl) countEl.textContent = shown + ' / ' + rows.length;
    },

    _tiFilterRow(tableId, colCount) {
        let html = '<tr class="ti-filter-row" style="background:var(--bg)">';
        for (let i = 0; i < colCount; i++) {
            html += `<th style="padding:2px"><input type="text" class="form-input" style="font-size:10px;padding:2px 4px;width:100%" placeholder="Filter..." oninput="PA.tableInfo._tiFilter('${tableId}')"></th>`;
        }
        html += '</tr>';
        return html;
    },

    renderColumns(container, data) {
        const cols = data.columns || [];
        if (cols.length === 0) { container.innerHTML = '<div class="empty-msg">No columns</div>'; return; }

        let html = '<div style="display:flex;align-items:center;gap:8px;padding:4px 8px;font-size:11px;color:var(--text-muted)"><span id="tiCols-count">' + cols.length + ' / ' + cols.length + '</span> columns</div>';
        html += '<table class="to-table" id="tiCols"><thead><tr>';
        html += '<th>#</th><th>Column</th><th>Type</th><th>Length</th><th>Precision</th><th>Nullable</th><th>Default</th>';
        html += '</tr>';
        html += this._tiFilterRow('tiCols', 7);
        html += '</thead><tbody>';
        for (const c of cols) {
            html += '<tr class="to-row">';
            html += `<td style="color:var(--text-muted)">${c.columnId}</td>`;
            html += `<td style="font-weight:600;font-family:var(--font-mono);font-size:12px">${PA.esc(c.columnName)}</td>`;
            html += `<td style="color:var(--accent)">${PA.esc(c.dataType)}</td>`;
            html += `<td>${c.dataLength || '-'}</td>`;
            html += `<td>${c.dataPrecision != null ? c.dataPrecision + (c.dataScale != null ? ',' + c.dataScale : '') : '-'}</td>`;
            html += `<td>${c.nullable ? '<span style="color:var(--badge-green)">Y</span>' : '<span style="color:var(--badge-red)">N</span>'}</td>`;
            html += `<td style="font-size:10px;color:var(--text-muted)">${PA.esc(c.dataDefault || '')}</td>`;
            html += '</tr>';
        }
        html += '</tbody></table>';
        container.innerHTML = html;
    },

    renderConstraints(container, data) {
        const cons = data.constraints || [];
        if (cons.length === 0) { container.innerHTML = '<div class="empty-msg">No constraints</div>'; return; }

        const grouped = {};
        for (const c of cons) {
            if (!grouped[c.constraintName]) grouped[c.constraintName] = { type: c.constraintType, ref: c.refConstraint, columns: [] };
            grouped[c.constraintName].columns.push(c.columnName);
        }
        const entries = Object.entries(grouped);

        let html = '<div style="display:flex;align-items:center;gap:8px;padding:4px 8px;font-size:11px;color:var(--text-muted)"><span id="tiCons-count">' + entries.length + ' / ' + entries.length + '</span> constraints</div>';
        html += '<table class="to-table" id="tiCons"><thead><tr>';
        html += '<th>Constraint</th><th>Type</th><th>Columns</th><th>References</th>';
        html += '</tr>';
        html += this._tiFilterRow('tiCons', 4);
        html += '</thead><tbody>';
        for (const [name, info] of entries) {
            const typeLabel = { P: 'PRIMARY KEY', R: 'FOREIGN KEY', U: 'UNIQUE', C: 'CHECK' }[info.type] || info.type;
            const typeColor = { P: 'var(--accent)', R: 'var(--teal)', U: 'var(--orange)', C: 'var(--text-muted)' }[info.type] || 'var(--text-muted)';
            html += '<tr class="to-row">';
            html += `<td style="font-family:var(--font-mono);font-size:11px">${PA.esc(name)}</td>`;
            html += `<td style="color:${typeColor};font-weight:700;font-size:11px">${typeLabel}</td>`;
            html += `<td>${info.columns.map(c => PA.esc(c)).join(', ')}</td>`;
            html += `<td style="font-size:11px;color:var(--text-muted)">${PA.esc(info.ref || '-')}</td>`;
            html += '</tr>';
        }
        html += '</tbody></table>';
        container.innerHTML = html;
    },

    renderIndexes(container, data) {
        const idxs = data.indexes || [];
        if (idxs.length === 0) { container.innerHTML = '<div class="empty-msg">No indexes</div>'; return; }

        const grouped = {};
        for (const i of idxs) {
            if (!grouped[i.indexName]) grouped[i.indexName] = { uniqueness: i.uniqueness, columns: [] };
            grouped[i.indexName].columns.push(i.columnName);
        }
        const entries = Object.entries(grouped);

        let html = '<div style="display:flex;align-items:center;gap:8px;padding:4px 8px;font-size:11px;color:var(--text-muted)"><span id="tiIdx-count">' + entries.length + ' / ' + entries.length + '</span> indexes</div>';
        html += '<table class="to-table" id="tiIdx"><thead><tr>';
        html += '<th>Index</th><th>Uniqueness</th><th>Columns</th>';
        html += '</tr>';
        html += this._tiFilterRow('tiIdx', 3);
        html += '</thead><tbody>';
        for (const [name, info] of entries) {
            html += '<tr class="to-row">';
            html += `<td style="font-family:var(--font-mono);font-size:11px">${PA.esc(name)}</td>`;
            html += `<td>${info.uniqueness === 'UNIQUE' ? '<span style="color:var(--accent);font-weight:700">UNIQUE</span>' : 'NONUNIQUE'}</td>`;
            html += `<td>${info.columns.map(c => PA.esc(c)).join(', ')}</td>`;
            html += '</tr>';
        }
        html += '</tbody></table>';
        container.innerHTML = html;
    },

    renderDefinition(container, data) {
        const def = data.viewDefinition;
        if (!def) {
            container.innerHTML = '<div class="empty-msg">No view definition available</div>';
            return;
        }

        const refTables = PA.tableInfo._extractReferencedTables(def);
        const lines = def.split('\n');
        let html = '<div style="display:flex;flex-direction:column;height:100%;gap:8px">';

        html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:0 4px">';
        html += `<div style="font-size:12px;color:var(--text-muted)">`;
        html += `<span style="color:var(--orange);font-weight:700">VIEW</span> `;
        html += `<span style="font-family:var(--font-mono)">${PA.esc(data.schema || '')}.</span>`;
        html += `<span style="font-family:var(--font-mono);font-weight:700;color:var(--accent)">${PA.esc(data.tableName)}</span>`;
        html += ` <span style="opacity:0.6">(${lines.length} lines)</span>`;
        html += `</div>`;
        html += `<button class="btn btn-sm" onclick="PA.tableInfo._copyDefinition()">Copy SQL</button>`;
        html += '</div>';

        if (refTables.length > 0) {
            html += '<div style="padding:4px 4px;display:flex;flex-wrap:wrap;align-items:center;gap:6px">';
            html += '<span style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted)">Referenced Tables:</span>';
            for (const t of refTables) {
                const schema = data.schema || '';
                html += `<span style="cursor:pointer;font-size:11px;font-weight:600;color:var(--teal);padding:2px 8px;background:var(--badge-teal-bg);border-radius:4px" `
                    + `onclick="PA.tableInfo.open('${PA.escJs(t)}', '${PA.escJs(schema)}')" `
                    + `title="Open table info for ${PA.escAttr(t)}">${PA.esc(t)}</span>`;
            }
            html += '</div>';
        }

        html += '<div class="src-container" style="flex:1;margin:0;overflow:auto">';
        html += '<table class="src-table" style="width:100%"><tbody>';
        for (let i = 0; i < lines.length; i++) {
            const lineNum = i + 1;
            const lineText = PA.tableInfo._highlightSql(lines[i]);
            html += `<tr>`;
            html += `<td class="src-line-num" style="padding:0 8px;text-align:right;color:var(--text-muted);font-size:11px;user-select:none;min-width:40px">${lineNum}</td>`;
            html += `<td class="src-line" style="padding:0 8px;white-space:pre;font-family:var(--font-mono);font-size:12px">${lineText}</td>`;
            html += `</tr>`;
        }
        html += '</tbody></table></div></div>';
        container.innerHTML = html;
    },

    _extractReferencedTables(sql) {
        if (!sql) return [];
        const tables = new Set();
        const cleaned = sql.replace(/--[^\n]*/g, '').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\s+/g, ' ');
        const pattern = /\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_$#]*(?:\.[A-Za-z_][A-Za-z0-9_$#]*)?)/gi;
        let m;
        while ((m = pattern.exec(cleaned)) !== null) {
            let name = m[1].toUpperCase();
            tables.add(name.includes('.') ? name.split('.').pop() : name);
        }
        return [...tables].sort();
    },

    _highlightSql(line) {
        if (!line) return '';
        const escaped = PA.esc(line);
        const keywords = ['SELECT','FROM','WHERE','AND','OR','JOIN','LEFT','RIGHT','INNER','OUTER',
            'ON','AS','IN','NOT','NULL','IS','BETWEEN','LIKE','EXISTS','UNION','ALL',
            'GROUP','BY','ORDER','HAVING','DISTINCT','CASE','WHEN','THEN','ELSE','END',
            'INSERT','INTO','UPDATE','SET','DELETE','MERGE','VALUES','CREATE','VIEW',
            'WITH','OVER','PARTITION','ROW_NUMBER','RANK','DECODE','NVL','TO_CHAR','TO_DATE','TO_NUMBER',
            'COUNT','SUM','AVG','MIN','MAX','SUBSTR','TRIM','UPPER','LOWER','REPLACE'];
        return escaped.replace(
            new RegExp('\\b(' + keywords.join('|') + ')\\b', 'gi'),
            '<span style="color:var(--accent);font-weight:600">$1</span>'
        );
    },

    _copyDefinition() {
        const def = PA.tableInfo.data?.viewDefinition;
        if (!def) return;
        navigator.clipboard.writeText(def).then(
            () => PA.toast('View definition copied', 'success'),
            () => PA.toast('Copy failed', 'error')
        );
    },

    renderQuery(container, data) {
        const tableName = data.tableName || '';
        const schema = data.schema || '';
        const defaultSql = `SELECT * FROM ${schema ? schema + '.' : ''}${tableName} WHERE ROWNUM <= 50`;

        let html = '<div style="display:flex;flex-direction:column;height:100%;gap:8px">';
        html += '<div style="display:flex;gap:8px;align-items:flex-start">';
        html += `<textarea id="tiQuerySql" class="form-input" style="flex:1;height:80px;font-family:var(--font-mono);font-size:12px;resize:vertical">${PA.esc(defaultSql)}</textarea>`;
        html += `<button class="btn btn-primary" onclick="PA.tableInfo.runQuery()">Run</button>`;
        html += '</div>';
        html += '<div style="font-size:10px;color:var(--text-muted)">Only SELECT/WITH queries allowed. Max 500 rows.</div>';
        html += '<div id="tiQueryResult" style="flex:1;overflow:auto"></div>';
        html += '</div>';
        container.innerHTML = html;
    },

    async runQuery() {
        const sql = document.getElementById('tiQuerySql')?.value;
        if (!sql) return;
        const resultEl = document.getElementById('tiQueryResult');
        resultEl.innerHTML = '<div class="empty-msg">Running query...</div>';

        try {
            const resp = await PA.api.executeQuery(sql, PA.tableInfo.data?.schema || null);
            if (!resp.success) {
                resultEl.innerHTML = '<div style="color:var(--badge-red);padding:12px">' + PA.esc(resp.error || 'Query failed') + '</div>';
                return;
            }

            const cols = resp.columns || [];
            const rows = resp.rows || [];

            let html = '<div style="font-size:10px;color:var(--text-muted);margin-bottom:4px">' + rows.length + ' rows' + (resp.truncated ? ' (truncated)' : '') + '</div>';
            html += '<table class="to-table"><thead><tr>';
            for (const c of cols) {
                html += `<th>${PA.esc(c.name)} <span style="font-weight:400;opacity:0.6">${PA.esc(c.type)}</span></th>`;
            }
            html += '</tr></thead><tbody>';
            for (const row of rows) {
                html += '<tr class="to-row">';
                for (const val of row) {
                    html += `<td style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${PA.escAttr(val || '')}">${PA.esc(val || 'NULL')}</td>`;
                }
                html += '</tr>';
            }
            html += '</tbody></table>';
            resultEl.innerHTML = html;
        } catch (e) {
            resultEl.innerHTML = '<div style="color:var(--badge-red);padding:12px">' + PA.esc(e.message) + '</div>';
        }
    }
};
