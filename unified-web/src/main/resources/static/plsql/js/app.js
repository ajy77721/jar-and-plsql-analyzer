/**
 * PA — Main application module.
 * Two screens: Home (analysis list + new analysis) and Analysis (left panel + right content).
 */
window.PA = window.PA || {};

// ==================== UTILITY HELPERS ====================
PA.esc = function(s) { return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); };
PA.escAttr = function(s) { return (s || '').replace(/"/g,'&quot;').replace(/'/g,'&#39;').replace(/</g,'&lt;'); };
PA.escJs = function(s) { return (s || '').replace(/\\/g,'\\\\').replace(/'/g,"\\'"); };

PA.toast = function(msg, type) {
    const el = document.getElementById('toast');
    el.textContent = msg;
    el.className = 'toast visible ' + (type || 'success');
    setTimeout(() => { el.className = 'toast'; }, 4000);
};

// ==================== SHARED CONTEXT (procedure scope) ====================
PA.context = {
    procId: null,               // currently selected procedure ID
    procDetail: null,           // full detail response from /detail/{procId}
    scopedTables: [],           // tables from detail (proc subtree only)
    callTreeNodeIds: new Set(), // all proc IDs in the call tree
    claudeMerged: false         // true after Claude results merged
};

/** Walk call tree recursively, collect all node IDs into a Set */
PA._collectTreeNodeIds = function(tree, set) {
    if (!tree) return set || new Set();
    if (!set) set = new Set();
    const id = (tree.id || '').toUpperCase();
    if (id) set.add(id);
    if (tree.children && !tree.circular) {
        for (const child of tree.children) {
            PA._collectTreeNodeIds(child, set);
        }
    }
    return set;
};

/** Render context breadcrumb into a container element */
PA._renderContextBreadcrumb = function(containerId) {
    const el = document.getElementById(containerId);
    if (!el) return;
    const ctx = PA.context;
    if (!ctx.procId || !ctx.procDetail) {
        el.style.display = 'none';
        return;
    }
    el.style.display = '';
    const d = ctx.procDetail;
    const schema = d.schemaName || '';
    const pkg = d.packageName || '';
    const name = d.name || '';
    const colorObj = PA.getSchemaColor(schema);
    const tableCount = (ctx.scopedTables || []).length;
    const nodeCount = ctx.callTreeNodeIds.size;

    let html = '<div class="context-bc-inner">';
    if (schema) html += `<span class="context-bc-seg" style="color:${colorObj.fg}">${PA.esc(schema)}</span><span class="context-bc-sep">&rsaquo;</span>`;
    if (pkg) html += `<span class="context-bc-seg">${PA.esc(pkg)}</span><span class="context-bc-sep">&rsaquo;</span>`;
    html += `<span class="context-bc-seg context-bc-active">${PA.esc(name)}</span>`;
    html += `<span class="context-bc-info">${nodeCount} procs | ${tableCount} tables</span>`;
    html += '</div>';
    el.innerHTML = html;
};

// ==================== CLAUDE MERGE ====================

/**
 * Merge Claude verification results into PA.analysisData.
 * Called automatically when verification completes.
 * - Updates existing tables with verified operations
 * - Adds new tables discovered by Claude
 * - Marks operations confirmed/removed/new
 * - Refreshes all active views
 */
PA._mergeClaudeResults = function(claudeResult) {
    if (!claudeResult || !claudeResult.tables || !PA.analysisData) return;
    const data = PA.analysisData;
    if (!data.tableOperations) data.tableOperations = [];

    const existingMap = {};
    for (const t of data.tableOperations) {
        existingMap[(t.tableName || '').toUpperCase()] = t;
    }

    let newCount = 0, updatedCount = 0;

    for (const ct of claudeResult.tables) {
        const key = (ct.tableName || '').toUpperCase();
        const existing = existingMap[key];

        if (existing) {
            // Merge Claude findings into existing table
            existing._claudeVerified = true;
            existing._claudeStatus = ct.overallStatus;
            if (ct.claudeVerifications) {
                existing._claudeVerifications = ct.claudeVerifications;
                // Merge new operations discovered by Claude
                const existOps = new Set(existing.operations || []);
                for (const v of ct.claudeVerifications) {
                    if (v.operation && !existOps.has(v.operation)) {
                        existing.operations = existing.operations || [];
                        existing.operations.push(v.operation);
                        existOps.add(v.operation);
                    }
                }
            }
            updatedCount++;
        } else {
            // New table discovered by Claude — add to analysis
            const newTable = {
                tableName: ct.tableName,
                schemaName: ct.schemaName || '',
                tableType: 'TABLE',
                operations: [...new Set((ct.claudeVerifications || []).map(v => v.operation).filter(Boolean))],
                accessCount: (ct.claudeVerifications || []).length,
                accessDetails: [],
                triggers: [],
                external: true,
                _claudeVerified: true,
                _claudeStatus: ct.overallStatus,
                _claudeVerifications: ct.claudeVerifications,
                _claudeDiscovered: true
            };
            data.tableOperations.push(newTable);
            newCount++;
        }
    }

    PA.context.claudeMerged = true;
    console.log('[PA] Claude merge complete: updated=' + updatedCount + ', new=' + newCount);

    // Refresh Table Ops with merged data
    if (PA.tableOps) {
        PA.tableOps.data = data.tableOperations;
        if (PA.tableOps.scopeMode === 'all') {
            PA.tf.setData('to', data.tableOperations);
        }
    }

    PA.toast('Claude results merged: ' + updatedCount + ' updated, ' + newCount + ' new tables', 'success');
};

// ==================== SCREEN SWITCHING ====================
PA.currentScreen = 'home';
PA.analysisData = null;

PA.showScreen = function(name) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    const el = document.getElementById(name === 'analysis' ? 'screenAnalysis' : 'screenHome');
    if (el) el.classList.add('active');
    PA.currentScreen = name;
};

PA.goHome = function() {
    PA.showScreen('home');
    PA.loadHistory();
    PA.jobs.loadJobs();
    if (window.location.hash !== '#/home' && window.location.hash !== '') {
        history.pushState(null, '', '#/home');
    }
};

PA.goAnalysis = function() {
    PA.showScreen('analysis');
    // Remove Claude running banner from home screen if present
    const banner = document.getElementById('claudeRunningBanner');
    if (banner) banner.remove();
};

// ==================== INITIALIZATION ====================
PA.init = async function() {
    // Fire all init API calls in parallel for fast page load
    const [projectsRes, usersRes] = await Promise.allSettled([
        PA.api.listProjects().catch(() => []),
        PA.api.getDbUsers().catch(() => [])
    ]);

    // Populate project dropdown
    const projects = projectsRes.status === 'fulfilled' ? projectsRes.value : [];
    const projSel = document.getElementById('homeProject');
    projSel.innerHTML = '<option value="">Legacy (global)</option>';
    for (const p of projects) {
        projSel.innerHTML += `<option value="${PA.escAttr(p.name)}">${PA.esc(p.name)}</option>`;
    }

    // Populate schema/owner dropdown
    const users = usersRes.status === 'fulfilled' ? usersRes.value : [];
    const sel = document.getElementById('homeSchema');
    sel.innerHTML = '<option value="">Auto-detect (all schemas)</option>';
    for (const u of users) {
        sel.innerHTML += `<option value="${PA.escAttr(u.username)}">${PA.esc(u.username)}</option>`;
    }

    PA.updateFormSteps();

    // Load history, jobs, check running state, check claude — all in parallel
    await Promise.allSettled([
        PA.loadHistory(),
        PA.jobs.loadJobs().then(() => PA._checkRunningJobsOnInit()),
        PA._checkClaudeOnInit()
    ]);

    // Hash-based URL routing
    window.addEventListener('popstate', () => PA._handleRoute());
    PA._handleRoute();
};

PA._handleRoute = function() {
    const hash = window.location.hash || '';

    // #/analysis/{name}/proc/{procId}
    const procMatch = hash.match(/^#\/analysis\/([^/]+)\/proc\/(.+)$/);
    if (procMatch) {
        const name = decodeURIComponent(procMatch[1]);
        const procId = decodeURIComponent(procMatch[2]);
        if (!PA.analysisData || PA.analysisData.name !== name) {
            PA.loadAnalysis(name, { fromRoute: true, procId: procId });
        } else if (PA.context.procId !== procId) {
            PA.showProcedure(procId, { fromRoute: true });
        }
        return;
    }

    // #/analysis/{name}
    const analysisMatch = hash.match(/^#\/analysis\/([^/]+)$/);
    if (analysisMatch) {
        const name = decodeURIComponent(analysisMatch[1]);
        if (!PA.analysisData || PA.analysisData.name !== name) {
            PA.loadAnalysis(name, { fromRoute: true });
        }
        return;
    }

    // #/home or empty — go home if currently viewing analysis
    if (hash === '#/home' || !hash) {
        if (PA.currentScreen === 'analysis') PA.goHome();
    }
};

/**
 * On page load, check if any analysis job is still running (survives page refresh).
 * If so, show progress bar and start polling for completion.
 */
PA._checkRunningJobsOnInit = async function() {
    try {
        // Reuse jobs already loaded by loadJobs() instead of fetching again
        const jobs = PA.jobs._allJobs || [];
        const running = jobs.find(j => j.status === 'RUNNING' || j.status === 'QUEUED');
        if (!running) return;

        PA.jobs.currentJobId = running.id;
        PA.jobs.startPolling();

        // Show progress bar on home screen
        const prog = document.getElementById('homeProgress');
        const progText = document.getElementById('homeProgressText');
        const progFill = document.getElementById('homeProgressFill');
        prog.style.display = 'block';
        const pct = running.totalSteps > 0 ? Math.round((running.stepNumber / running.totalSteps) * 100) : 0;
        progFill.style.width = pct + '%';
        progText.textContent = running.currentStep || 'Running...';

        // Start polling for completion
        let completionHandled = false;
        PA._pollForJobCompletion(running.id, prog, progText, progFill,
            () => completionHandled, () => { completionHandled = true; });

        PA.toast('Reconnected to running analysis job', 'success');
    } catch (e) {
        // Silent — don't bother user on init failure
    }
};

/**
 * On page load, silently check if a Claude verification session is running.
 * If so, show a persistent banner on the home screen so the user knows.
 */
PA._checkClaudeOnInit = async function() {
    try {
        const status = await PA.api.getClaudeStatus();
        if (!status || !status.cliAvailable) return;
        const progress = status.progress;
        if (progress && progress.running) {
            PA._claudeRunningOnInit = true;
            PA._claudeRunningAnalysis = status.currentAnalysis || '';
            const pct = progress.percentComplete || 0;
            const chunks = `${progress.completedChunks || 0}/${progress.totalChunks || 0}`;
            const analysisLabel = status.currentAnalysis ? ` for <b>${PA.esc(status.currentAnalysis)}</b>` : '';
            // Show banner on home screen
            const banner = document.createElement('div');
            banner.id = 'claudeRunningBanner';
            banner.style.cssText = 'padding:10px 16px;margin:0 0 12px;border-radius:8px;border:1px solid var(--badge-blue);background:var(--badge-blue-bg);display:flex;align-items:center;gap:10px;font-size:12px;width:100%';
            banner.innerHTML =
                `<span style="font-weight:700;color:var(--badge-blue)">Claude Verification Running</span>` +
                `<span>${analysisLabel} ${chunks} chunks (${Math.round(pct)}%)</span>` +
                `<div style="flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden"><div style="height:100%;width:${pct}%;background:var(--accent);border-radius:2px"></div></div>` +
                `<span style="color:var(--text-muted);font-size:11px">Load the analysis and open Claude tab to see progress</span>`;
            const homeContainer = document.querySelector('.home-container');
            if (homeContainer) homeContainer.insertBefore(banner, homeContainer.firstChild);
        } else if (status.hasVerification) {
            // Has completed results — no banner needed, will show when Claude tab is opened
        }
    } catch (e) {
        // Silent — don't bother user on init failure
    }
};

// ==================== HISTORY (SCREEN 1) ====================
PA._allHistory = [];
PA._historySearchQuery = '';
PA._claudeSessionsByAnalysis = {};

PA.loadHistory = async function() {
    try {
        const [history, sessions] = await Promise.all([
            PA.api.listHistory().catch(() => []),
            PA.api.listClaudeSessions().catch(() => [])
        ]);
        PA._allHistory = history || [];
        PA._claudeSessionsByAnalysis = {};
        for (const s of (sessions || [])) {
            const name = s.analysisName || '';
            if (!PA._claudeSessionsByAnalysis[name]) PA._claudeSessionsByAnalysis[name] = [];
            PA._claudeSessionsByAnalysis[name].push(s);
        }
        PA._renderFilteredHistory();
        PA._renderClaudeHome();
    } catch (e) {
        document.getElementById('historyList').innerHTML = '<div class="empty-msg">Could not load history</div>';
    }
};

PA.onHistorySearch = function(q) {
    PA._historySearchQuery = (q || '').toUpperCase();
    PA._renderFilteredHistory();
};

PA._renderFilteredHistory = function() {
    const container = document.getElementById('historyList');
    const countEl = document.getElementById('historyCount');
    let history = PA._allHistory || [];
    const q = PA._historySearchQuery;
    if (q) {
        history = history.filter(item => {
            const txt = ((item.entrySchema||'') + '.' + (item.entryObjectName||'') + (item.entryProcedure ? '.' + item.entryProcedure : '') + ' ' + (item.name||'')).toUpperCase();
            return txt.includes(q);
        });
    }
    if (countEl) countEl.textContent = history.length;

    if (history.length === 0) {
        container.innerHTML = '<div class="empty-msg">No analyses found</div>';
        return;
    }

    let html = '';
    for (const item of history) {
        const entry = (item.entrySchema || '') + '.' + (item.entryObjectName || '');
        const proc = item.entryProcedure ? '.' + item.entryProcedure : '';
        const ts = item.timestamp ? new Date(item.timestamp).toLocaleString() : '';

        // Claude status badge — show enriched iteration or session status
        const claudeSessions = PA._claudeSessionsByAnalysis[item.name] || [];
        let claudeBadge = '';
        if (item.hasClaude && item.analysisMode === 'CLAUDE_ENRICHED') {
            const iter = item.claudeIteration || 1;
            const at = item.claudeEnrichedAt ? new Date(item.claudeEnrichedAt).toLocaleString() : '';
            claudeBadge = `<span class="badge" style="background:linear-gradient(135deg,#dbeafe,#c7d2fe);color:#1d4ed8;margin-left:6px;font-weight:700" title="Claude enriched iter ${iter}${at ? ' at ' + at : ''}">&#10003; Enriched (${iter})</span>`;
        } else if (claudeSessions.length > 0) {
            const completed = claudeSessions.filter(s => s.status === 'COMPLETED' || s.status === 'completed').length;
            const running = claudeSessions.filter(s => s.status === 'RUNNING' || s.status === 'running').length;
            if (running > 0) {
                claudeBadge = `<span class="badge" style="background:rgba(168,85,247,0.15);color:#a855f7;margin-left:6px" title="${running} running, ${completed} completed Claude sessions">AI ${running} running</span>`;
            } else if (completed > 0) {
                claudeBadge = `<span class="badge" style="background:rgba(34,197,94,0.15);color:#22c55e;margin-left:6px" title="${completed} completed Claude sessions">AI done</span>`;
            } else {
                claudeBadge = `<span class="badge" style="background:rgba(168,85,247,0.15);color:#a855f7;margin-left:6px" title="${claudeSessions.length} Claude sessions">AI ${claudeSessions.length}</span>`;
            }
        } else {
            claudeBadge = `<span class="badge" style="background:var(--badge-gray-bg);color:var(--text-muted);margin-left:6px;opacity:0.5" title="No Claude analysis yet">No AI</span>`;
        }

        const connBadge = item.hasConnections
            ? `<span class="badge" style="background:rgba(14,165,233,0.15);color:#0ea5e9;margin-left:6px;font-size:9px" title="DB connection info stored">🔗 Conn</span>`
            : '';
        html += `<div class="history-card" onclick="PA.loadAnalysis('${PA.escJs(item.name)}')">
            <div class="hc-info">
                <div class="hc-title">${PA.esc(entry)}${PA.esc(proc)}${claudeBadge}${connBadge}</div>
                <div class="hc-meta">${PA.esc(ts)}${item.sizeKb ? ' | ' + item.sizeKb + ' KB' : ''}</div>
                <div class="hc-stats">${item.flowNodeCount || item.procedureCount || 0} procs | ${item.flowTableCount || item.tableCount || 0} tables | ${item.flowSequenceCount != null ? item.flowSequenceCount : (item.sequenceCount || 0)} seqs | ${item.flowJoinCount != null ? item.flowJoinCount : (item.joinCount || 0)} joins | ${item.flowCursorCount != null ? item.flowCursorCount : (item.cursorCount || 0)} cursors | <a href="#" onclick="event.stopPropagation();PA.loadAnalysis('${PA.escJs(item.name)}').then(()=>PA.showErrors())" style="color:${(item.flowErrorCount || item.errorCount || 0) > 0 ? '#ef4444' : 'inherit'};text-decoration:${(item.flowErrorCount || item.errorCount || 0) > 0 ? 'underline' : 'none'};cursor:${(item.flowErrorCount || item.errorCount || 0) > 0 ? 'pointer' : 'default'}">${item.flowErrorCount != null ? item.flowErrorCount : (item.errorCount || 0)} errors</a></div>
            </div>
            <div class="hc-actions">
                <button class="btn btn-sm btn-primary" onclick="event.stopPropagation(); PA.loadAnalysis('${PA.escJs(item.name)}')">Load</button>
                <button class="btn btn-sm btn-danger" onclick="event.stopPropagation(); PA.deleteAnalysis('${PA.escJs(item.name)}')">Delete</button>
            </div>
        </div>`;
    }
    container.innerHTML = html;
};

PA.renderHistory = function(history) {
    PA._allHistory = history || [];
    PA._renderFilteredHistory();
};

PA._claudeHomeSearch = '';

PA._renderClaudeHome = function() {
    const container = document.getElementById('claudeHomeList');
    const countEl = document.getElementById('claudeSessionCount');
    if (!container) return;

    // Flatten all sessions
    let allSessions = [];
    for (const sessions of Object.values(PA._claudeSessionsByAnalysis)) {
        allSessions.push(...sessions);
    }
    allSessions.sort((a, b) => {
        const ta = a.startedAt || a.createdAt || '';
        const tb = b.startedAt || b.createdAt || '';
        return tb.localeCompare(ta);
    });

    if (countEl) countEl.textContent = allSessions.length;

    const q = (PA._claudeHomeSearch || '').toUpperCase();
    if (q) {
        allSessions = allSessions.filter(s => {
            const txt = ((s.analysisName||'') + ' ' + (s.type||'') + ' ' + (s.status||'')).toUpperCase();
            return txt.includes(q);
        });
    }

    if (allSessions.length === 0) {
        container.innerHTML = '<div class="empty-msg">No Claude sessions yet</div>';
        return;
    }

    let html = '';
    for (const s of allSessions.slice(0, 50)) {
        const status = (s.status || '').toUpperCase();
        let statusColor = 'var(--text-muted)';
        if (status === 'COMPLETED') statusColor = '#22c55e';
        else if (status === 'RUNNING') statusColor = '#a855f7';
        else if (status === 'FAILED') statusColor = '#ef4444';

        const type = s.type || s.sessionType || 'verification';
        const ts = s.startedAt ? new Date(s.startedAt).toLocaleString() : (s.createdAt ? new Date(s.createdAt).toLocaleString() : '');
        const duration = s.durationMs ? (s.durationMs / 1000).toFixed(0) + 's' : '';
        const analysis = s.analysisName || '';

        html += `<div class="history-card" style="padding:8px 12px" onclick="PA.claude.showSessions()">`;
        html += `<div class="hc-info" style="flex:1">`;
        html += `<div style="font-size:12px;font-weight:600;color:${statusColor}">${PA.esc(status)} <span style="color:var(--text-muted);font-weight:400;font-size:11px">${PA.esc(type)}</span></div>`;
        html += `<div style="font-size:11px;color:var(--text-secondary);margin-top:2px">${PA.esc(analysis)}</div>`;
        if (ts || duration) html += `<div style="font-size:10px;color:var(--text-muted);margin-top:1px">${PA.esc(ts)}${duration ? ' | ' + duration : ''}</div>`;
        html += `</div></div>`;
    }
    container.innerHTML = html;
};

PA.showErrors = async function() {
    try {
        const res = await fetch('/api/plsql/analysis/errors');
        const errors = await res.json();
        if (!errors || !errors.length) { PA.toast('No parse errors', 'success'); return; }

        let html = '<div style="max-height:70vh;overflow:auto;padding:4px">';
        html += `<div style="margin-bottom:12px;color:#a6adc8;font-size:13px">${errors.reduce((s,e) => s + e.errorCount, 0)} parse errors across ${errors.length} units</div>`;
        for (const unit of errors) {
            const label = (unit.schemaName || '') + '.' + (unit.unitName || '');
            const sf = unit.sourceFile || ((unit.schemaName || '') + '.' + (unit.unitName || ''));
            html += `<div style="margin-bottom:14px">`;
            html += `<div style="font-weight:700;font-size:13px;color:#cdd6f4;margin-bottom:4px;cursor:pointer" onclick="PA.codeModal.open('${PA.escJs(sf)}');document.getElementById('errorsModal').style.display='none'">${PA.esc(label)} <span style="color:#6c7086;font-weight:400">${PA.esc(unit.unitType || '')}</span></div>`;
            for (const err of (unit.errors || [])) {
                const lineMatch = err.match(/^line (\d+)/);
                const line = lineMatch ? lineMatch[1] : null;
                if (line) {
                    html += `<div style="font-size:12px;color:#ef4444;padding:2px 0 2px 16px;cursor:pointer" onclick="PA.codeModal.openAtLine('${PA.escJs(sf)}',${line});document.getElementById('errorsModal').style.display='none'" title="Click to jump to line ${line}">&#8226; ${PA.esc(err)}</div>`;
                } else {
                    html += `<div style="font-size:12px;color:#ef4444;padding:2px 0 2px 16px">&#8226; ${PA.esc(err)}</div>`;
                }
            }
            html += '</div>';
        }
        html += '</div>';

        let modal = document.getElementById('errorsModal');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'errorsModal';
            modal.className = 'modal';
            modal.innerHTML = `<div class="modal-box" style="width:700px;max-width:90vw;max-height:80vh">
                <div class="modal-head"><h3>Parse Errors</h3><button class="modal-x" onclick="document.getElementById('errorsModal').style.display='none'">&times;</button></div>
                <div class="modal-body" id="errorsModalBody"></div>
            </div>`;
            document.body.appendChild(modal);
        }
        document.getElementById('errorsModalBody').innerHTML = html;
        modal.style.display = 'flex';
    } catch (e) {
        PA.toast('Failed to load errors: ' + e.message, 'error');
    }
};

PA.deleteAnalysis = async function(name) {
    if (!confirm('Delete analysis "' + name + '"?')) return;
    try {
        const result = await PA.api.deleteAnalysis(name);
        if (result && result.deleted) {
            PA.toast('Deleted: ' + name, 'success');
            await PA.loadHistory();
        } else {
            PA.toast('Could not delete', 'error');
        }
    } catch (e) {
        PA.toast('Delete failed: ' + e.message, 'error');
    }
};

// ==================== LOAD ANALYSIS ====================
PA.loadAnalysis = async function(name, opts) {
    const fromRoute = opts && opts.fromRoute;
    const routeProcId = opts && opts.procId;
    PA.toast('Loading ' + name + '...', 'success');
    try {
        const result = await PA.api.loadAnalysis(name);
        if (!result) { PA.toast('Analysis not found', 'error'); return; }
        PA.analysisData = result;
        PA.version._currentView = null;
        PA.goAnalysis();
        PA.updateTopbar(result);
        // Load version info, sidebar, table ops, and claude status in parallel
        await Promise.allSettled([
            PA.version.loadInfo().then(() => PA.version.update(result)),
            PA.loadSidebarData(),
            PA.tableOps.load(),
            PA.claude ? PA.claude.checkStatus().catch(() => {}) : Promise.resolve()
        ]);
        // Reset lazy-load flags for tabs that load on first switch
        if (PA.joins) PA.joins._loaded = false;
        if (PA.cursors) PA.cursors._loaded = false;
        if (PA.predicates) PA.predicates._loaded = false;
        // Hide version banner on fresh load
        const banner = document.getElementById('versionBanner');
        if (banner) banner.style.display = 'none';
        PA.toast('Loaded: ' + (result.entryObjectName || '') + ' (' + (result.procedureCount || 0) + ' procs)', 'success');

        // Update URL hash
        const targetHash = '#/analysis/' + encodeURIComponent(name);
        if (window.location.hash !== targetHash && !routeProcId) {
            if (fromRoute) {
                history.replaceState(null, '', targetHash);
            } else {
                history.pushState(null, '', targetHash);
            }
        }

        // Auto-select: if route specifies a proc, use that; otherwise auto-detect entry
        if (routeProcId) {
            PA.showProcedure(routeProcId, { fromRoute: true });
        } else {
            PA.autoSelectEntryProc(result);
        }
    } catch (e) {
        PA.toast('Failed: ' + e.message, 'error');
        if (!fromRoute) return;
        history.replaceState(null, '', '#/home');
    }
};

/**
 * Auto-select the entry procedure so Explore/Trace are populated on load.
 * Always starts from the ROOT entry point — never a child node.
 * Builds the expected node ID from the analysis metadata and calls showProcedure.
 */
PA.autoSelectEntryProc = async function(result) {
    if (!result) return;

    // Use backend-resolved rootId first (already fuzzy-matched)
    if (result.rootId) {
        console.log('[PA] autoSelectEntryProc: using rootId', result.rootId);
        try {
            const detail = await PA.api.getProcDetail(result.rootId);
            if (detail && detail.callTree) {
                PA.showProcedure(result.rootId);
                return;
            }
        } catch (e) {
            console.warn('[PA] autoSelectEntryProc: rootId failed', result.rootId);
        }
    }

    const schema = (result.entrySchema || '').toUpperCase();
    const obj = (result.entryObjectName || '').toUpperCase();
    const proc = (result.entryProcedure || '').toUpperCase();

    const idsToTry = [];
    if (proc) idsToTry.push([schema, obj, proc].filter(Boolean).join('.'));
    idsToTry.push([schema, obj].filter(Boolean).join('.'));
    if (obj) idsToTry.push(obj);

    for (const entryId of idsToTry) {
        if (!entryId) continue;
        console.log('[PA] autoSelectEntryProc: trying', entryId);
        try {
            const detail = await PA.api.getProcDetail(entryId);
            if (detail && detail.callTree) {
                PA.showProcedure(entryId);
                return;
            }
        } catch (e) {
            console.warn('[PA] autoSelectEntryProc: no detail for', entryId);
        }
    }

    const firstItem = document.querySelector('#lpProcedures .lp-item[data-id]');
    if (firstItem && firstItem.dataset.id) {
        PA.showProcedure(firstItem.dataset.id);
    }
};

PA.updateTopbar = function(result) {
    const title = document.getElementById('topbarTitle');
    const stats = document.getElementById('topbarStats');
    const entry = (result.entrySchema || '') + '.' + (result.entryObjectName || '');
    const proc = result.entryProcedure ? '.' + result.entryProcedure : '';
    title.textContent = entry + proc;

    // Show flow-scoped stats if available, otherwise raw counts
    const fs = result.flowStats || {};
    const flowProcs = fs.totalNodes || result.nodeCount || 0;
    const flowInt = fs.internalCalls || 0;
    const flowExt = fs.externalCalls || 0;
    const flowTables = result.flowTableCount || result.tableCount || 0;
    const flowLoc = result.flowLoc || 0;
    const flowDepth = fs.maxDepth || 0;
    const flowOps = (result.flowOperations || []);

    const flowSeqs = result.sequenceCount || 0;
    const flowJoins = result.joinCount || 0;
    const flowCursors = result.cursorCount || 0;
    const flowEdges = result.edgeCount || 0;
    const flowNodes = result.nodeCount || 0;
    const flowFiles = result.fileCount || 0;
    const dynCalls = fs.dynamicCalls || 0;

    let statHtml = `<span style="margin-right:6px" title="Parsed source files">${flowFiles} files</span>`;
    statHtml += `<span style="margin-right:6px" title="Total procedures/functions">${flowProcs} procs</span>`;
    statHtml += `<span style="margin-right:6px" title="Call graph nodes / edges">${flowNodes} nodes · ${flowEdges} edges</span>`;
    statHtml += `<span style="color:var(--badge-green);margin-right:4px" title="Internal calls (same package/schema)">${flowInt} int</span>`;
    statHtml += `<span style="color:var(--badge-red);margin-right:4px" title="External calls (cross-package/schema)">${flowExt} ext</span>`;
    if (dynCalls > 0) statHtml += `<span style="color:var(--badge-yellow);margin-right:4px" title="Dynamic SQL calls (EXECUTE IMMEDIATE)">${dynCalls} dyn</span>`;
    statHtml += `<span style="margin-right:4px">|</span>`;
    statHtml += `<span style="margin-right:6px" title="Database tables accessed">${flowTables} tables</span>`;
    if (flowSeqs > 0) statHtml += `<span style="margin-right:6px" title="Sequences used">${flowSeqs} seqs</span>`;
    if (flowJoins > 0) statHtml += `<span style="margin-right:6px" title="SQL joins detected">${flowJoins} joins</span>`;
    if (flowCursors > 0) statHtml += `<span style="margin-right:6px" title="Cursors declared">${flowCursors} cursors</span>`;
    if (flowOps.length) statHtml += flowOps.map(op => `<span class="op-badge ${op}" style="font-size:8px" title="${op} operations found in flow">${op}</span>`).join('');
    statHtml += `<span style="margin-left:4px">| ${flowLoc} LOC · depth ${flowDepth}</span>`;
    const errCount = result.flowErrorCount != null ? result.flowErrorCount : (result.errorCount || 0);
    if (errCount > 0) {
        statHtml += `<span style="margin-left:4px;color:#ef4444;cursor:pointer;text-decoration:underline" onclick="PA.showErrors()" title="Click to view parse errors">· ${errCount} errors</span>`;
    }
    stats.innerHTML = statHtml;

    // Update version badge
    PA.version.update(result);

    // Update Claude button
    PA.updateClaudeButton(result);

    // Load and display connection info for this analysis
    if (result.name) PA._loadConnectionsForAnalysis(result.name);
};

PA._loadConnectionsForAnalysis = async function(name) {
    try {
        const info = await PA.api.getAnalysisConnections(name);
        const el = document.getElementById('topbarConnInfo');
        if (!el) return;
        if (!info || !info.available) { el.innerHTML = ''; return; }
        const oracle = info.oracle || {};
        const url = oracle.jdbcUrl || '';
        const schemas = (oracle.schemas || []).join(', ');
        const masked = url.replace(/(\/\/[^:]+:)[^@]+(@)/, '$1***$2');
        el.innerHTML = `<span class="topbar-conn-badge" title="Oracle: ${masked}\nSchemas: ${schemas}" onclick="PA._showConnectionsModal('${name}')">🔗 ${schemas || 'DB'}</span>`;
    } catch (e) {
        const el = document.getElementById('topbarConnInfo');
        if (el) el.innerHTML = '';
    }
};

PA._showConnectionsModal = async function(name) {
    try {
        const info = await PA.api.getAnalysisConnections(name);
        if (!info || !info.available) { PA.toast('No connection info stored for this analysis', 'error'); return; }
        const oracle = info.oracle || {};
        const url = (oracle.jdbcUrl || 'N/A').replace(/(\/\/[^:]+:)[^@]+(@)/, '$1***$2');
        const schemas = (oracle.schemas || []).join('\n  ');
        const project = info.project ? `\nProject: ${info.project}` : '';
        const env = info.environment ? `\nEnvironment: ${info.environment}` : '';
        alert(`Connection Info — ${name}\n\nOracle JDBC URL:\n  ${url}${project}${env}\n\nSchemas:\n  ${schemas}\n\nAnalyzed: ${info.analyzedAt || 'N/A'}`);
    } catch (e) { PA.toast('Failed to load connection info', 'error'); }
};

// ==================== CLAUDE TRIGGER BUTTON (topbar) ====================

PA.updateClaudeButton = function(result) {
    const btn = document.getElementById('enableClaudeBtn');
    if (!btn) return;

    const mode = (result && result.analysisMode) || 'STATIC';
    const iter = (result && result.claudeIteration) || 0;
    const isEnriched = mode === 'CLAUDE_ENRICHED' && iter > 0;

    btn.style.display = '';
    if (isEnriched) {
        btn.textContent = 'Re-run Claude';
        btn.className = 'enable-claude-btn rescan';
        btn.title = 'Re-run Claude AI verification (current enriched version will be kept as previous)';
    } else {
        btn.textContent = 'Enable Claude';
        btn.className = 'enable-claude-btn';
        btn.title = 'Run Claude AI verification on this analysis';
    }
};

PA.triggerClaude = async function() {
    const result = PA.analysisData;
    if (!result) { PA.toast('No analysis loaded', 'error'); return; }

    const mode = (result.analysisMode) || 'STATIC';
    const isRescan = mode === 'CLAUDE_ENRICHED';
    const name = result.entryObjectName || result.name || '';

    const title = isRescan ? 'Re-run Claude Verification' : 'Enable Claude Verification';
    const msg = isRescan
        ? 'Re-run Claude AI verification on "' + name + '"?\n\nClaude will re-analyze all table operations. The current enriched version will be kept as the previous version for revert.'
        : 'Run Claude AI verification on "' + name + '"?\n\nClaude will verify table operations, add missing ones, and mark confirmed/new findings.';

    if (!confirm(msg)) return;

    const btn = document.getElementById('enableClaudeBtn');
    if (btn) btn.style.display = 'none';
    PA._showClaudeHeaderProgress(true, 0);

    try {
        PA.toast('Starting Claude verification...', 'success');
        const resp = await PA.api.startClaudeVerification(false);
        if (resp && resp.error) {
            PA.toast('Failed: ' + resp.error, 'error');
            if (btn) btn.style.display = '';
            PA._showClaudeHeaderProgress(false, 0);
            return;
        }
        PA.toast('Claude verification started', 'success');
        PA._startClaudeHeaderPoll();
    } catch (e) {
        PA.toast('Failed to start verification: ' + e.message, 'error');
        if (btn) btn.style.display = '';
        PA._showClaudeHeaderProgress(false, 0);
    }
};

PA._showClaudeHeaderProgress = function(show, pct) {
    const bar = document.getElementById('claudeHeaderProgress');
    const fill = document.getElementById('claudeHeaderProgressFill');
    if (!bar) return;
    bar.style.display = show ? '' : 'none';
    if (fill) fill.style.width = (pct || 0) + '%';
};

PA._claudeHeaderPollId = null;

PA._startClaudeHeaderPoll = function() {
    if (PA._claudeHeaderPollId) return;
    PA._claudeHeaderPollId = setInterval(async () => {
        try {
            const progress = await PA.api.getClaudeProgress();
            if (!progress) return;
            const pct = progress.percentComplete || 0;
            PA._showClaudeHeaderProgress(true, pct);

            if (!progress.running) {
                clearInterval(PA._claudeHeaderPollId);
                PA._claudeHeaderPollId = null;
                PA._showClaudeHeaderProgress(false, 0);

                if (pct >= 100) {
                    PA.toast('Claude verification complete!', 'success');
                    try {
                        const claudeResult = await PA.api.getClaudeResult();
                        if (claudeResult) PA._mergeClaudeResults(claudeResult);
                    } catch (e) { /* ignore */ }
                    await PA.version.loadInfo();
                    const latestData = PA.analysisData;
                    if (latestData) {
                        latestData.analysisMode = 'CLAUDE_ENRICHED';
                        latestData.claudeIteration = (latestData.claudeIteration || 0) + 1;
                        PA.version.update(latestData);
                        PA.updateClaudeButton(latestData);
                    }
                } else {
                    const btn = document.getElementById('enableClaudeBtn');
                    if (btn) btn.style.display = '';
                }
            }
        } catch (e) { /* continue polling */ }
    }, PollConfig.claudeProgressMs);
};

// ==================== VERSION MANAGEMENT ====================
PA.version = {
    _currentView: null,  // null=best, 'static', 'previous'
    _info: null,

    async loadInfo() {
        try {
            const resp = await fetch('/api/plsql/claude/versions');
            PA.version._info = await resp.json();
        } catch (e) {
            PA.version._info = null;
        }
    },

    update(result) {
        const badge = document.getElementById('topbarVersionBadge');
        if (!badge) return;

        const mode = (result && result.analysisMode) || 'STATIC';
        const iter = (result && result.claudeIteration) || 0;
        const enrichedAt = result && result.claudeEnrichedAt;
        const vi = PA.version._info;
        const view = PA.version._currentView;

        let html = '';
        if (view === 'static') {
            html = '<span class="pa-mode-badge pa-mode-static">STATIC (original)</span>';
            if (vi && vi.hasClaude) {
                html += '<button class="pa-mode-toggle" onclick="PA.version.switchTo(null)">View Enriched</button>';
            }
        } else if (view === 'previous') {
            const prevIter = iter > 0 ? iter : '?';
            html = '<span class="pa-mode-badge pa-mode-previous">PREV ENRICHED (iter ' + prevIter + ')</span>';
            html += '<button class="pa-mode-toggle" onclick="PA.version.switchTo(null)">View Latest</button>';
            html += '<button class="pa-mode-toggle pa-mode-revert" onclick="PA.version.revert()">Revert to This</button>';
        } else if (mode === 'CLAUDE_ENRICHED') {
            const iterLabel = iter ? ' (iter ' + iter + ')' : '';
            const atLabel = enrichedAt ? ' at ' + new Date(enrichedAt).toLocaleString() : '';
            html = '<span class="pa-mode-badge pa-mode-enriched" title="Claude enriched' + atLabel + '">&#10003; ENRICHED' + iterLabel + '</span>';
            html += '<button class="pa-mode-toggle" onclick="PA.version.switchTo(\'static\')" title="View original static analysis">View Static</button>';
            if (vi && vi.hasClaudePrev) {
                html += '<button class="pa-mode-toggle" onclick="PA.version.switchTo(\'previous\')" title="View previous enriched version">View Previous</button>';
            }
        } else {
            html = '<span class="pa-mode-badge pa-mode-static">STATIC</span>';
        }

        badge.innerHTML = html;
        badge.style.display = html ? '' : 'none';
    },

    async switchTo(version) {
        const banner = document.getElementById('versionBanner');
        try {
            PA.toast('Loading ' + (version || 'latest') + ' version...', 'success');
            let url;
            if (version === 'static') {
                url = '/api/plsql/claude/versions/load-static';
            } else if (version === 'previous') {
                url = '/api/plsql/claude/versions/load-prev';
            } else {
                url = '/api/plsql/claude/versions/load-claude';
            }
            const resp = await fetch(url, { method: 'POST' });
            if (!resp.ok) throw new Error('Failed to load version');
            PA.version._currentView = version;

            // Reload the analysis
            const result = await PA.api.getLatestResult();
            if (result) {
                PA.analysisData = result;
                PA.updateTopbar(result);
                await PA.loadSidebarData();
                await PA.tableOps.load();
                if (PA.joins) PA.joins._loaded = false;
                if (PA.cursors) PA.cursors._loaded = false;
                if (PA.predicates) PA.predicates._loaded = false;
                PA.autoSelectEntryProc(result);
            }

            // Show/hide banner
            if (banner) {
                if (version === 'static') {
                    banner.innerHTML = 'Viewing original static analysis &mdash; <button class="pa-mode-toggle" onclick="PA.version.switchTo(null)">Return to Enriched</button>';
                    banner.style.display = '';
                } else if (version === 'previous') {
                    banner.innerHTML = 'Viewing previous enriched version &mdash; <button class="pa-mode-toggle" onclick="PA.version.switchTo(null)">Return to Latest</button> <button class="pa-mode-toggle pa-mode-revert" onclick="PA.version.revert()">Revert to This</button>';
                    banner.style.display = '';
                } else {
                    banner.style.display = 'none';
                }
            }

            PA.toast('Loaded ' + (version || 'latest') + ' version', 'success');
        } catch (e) {
            PA.toast('Failed: ' + e.message, 'error');
        }
    },

    async revert() {
        if (!confirm('Revert to previous Claude version? Current enriched version will be replaced.')) return;
        try {
            const resp = await fetch('/api/plsql/claude/versions/revert', { method: 'POST' });
            if (!resp.ok) throw new Error('Revert failed');
            PA.version._currentView = null;
            PA.version._info = null;
            await PA.version.loadInfo();

            const result = await PA.api.getLatestResult();
            if (result) {
                PA.analysisData = result;
                PA.updateTopbar(result);
                await PA.loadSidebarData();
                await PA.tableOps.load();
                if (PA.joins) PA.joins._loaded = false;
                if (PA.cursors) PA.cursors._loaded = false;
                if (PA.predicates) PA.predicates._loaded = false;
                PA.autoSelectEntryProc(result);
            }
            const banner = document.getElementById('versionBanner');
            if (banner) banner.style.display = 'none';
            PA.toast('Reverted to previous version', 'success');
        } catch (e) {
            PA.toast('Revert failed: ' + e.message, 'error');
        }
    }
};

// ==================== ANALYSIS JOBS ====================
PA.jobs = {
    pollInterval: null,
    currentJobId: null,

    async loadJobs() {
        try {
            const jobs = await PA.api.listJobs();
            PA.jobs.renderJobs(jobs);
        } catch (e) {
            console.warn('[PA] Failed to load jobs', e);
        }
    },

    _allJobs: [],
    _jobSearchQuery: '',

    onSearch(q) {
        PA.jobs._jobSearchQuery = (q || '').toUpperCase();
        PA.jobs._renderFiltered();
    },

    _renderFiltered() {
        const container = document.getElementById('jobsList');
        const countEl = document.getElementById('jobsCount');
        const q = PA.jobs._jobSearchQuery;
        let jobs = PA.jobs._allJobs || [];
        if (q) {
            jobs = jobs.filter(j => {
                const txt = ((j.schema||'') + '.' + (j.objectName||'') + (j.procedureName ? '.' + j.procedureName : '') + ' ' + (j.status||'')).toUpperCase();
                return txt.includes(q);
            });
        }
        countEl.textContent = jobs.length;
        if (jobs.length === 0) {
            container.innerHTML = '<div class="empty-msg">No jobs</div>';
            return;
        }
        PA.jobs._renderJobCards(jobs, container);
    },

    renderJobs(jobs) {
        PA.jobs._allJobs = jobs || [];
        PA.jobs._renderFiltered();
    },

    _renderJobCards(jobs, container) {
        let html = '';
        for (const job of jobs) {
            const entry = (job.schema || '') + '.' + (job.objectName || '');
            const proc = job.procedureName ? '.' + job.procedureName : '';
            const elapsed = job.elapsedMs ? (job.elapsedMs / 1000).toFixed(1) + 's' : '';
            const pct = job.totalSteps > 0 ? Math.round((job.stepNumber / job.totalSteps) * 100) : 0;

            let statusBadge = '';
            let statusClass = '';
            switch (job.status) {
                case 'RUNNING':
                    statusBadge = `<span class="badge" style="background:var(--badge-blue-bg,#dbeafe);color:var(--badge-blue,#1d4ed8)">RUNNING</span>`;
                    statusClass = 'job-running';
                    break;
                case 'QUEUED':
                    statusBadge = `<span class="badge" style="background:var(--badge-yellow-bg,#fef3c7);color:var(--badge-yellow,#a16207)">QUEUED</span>`;
                    statusClass = 'job-queued';
                    break;
                case 'COMPLETE':
                    statusBadge = `<span class="badge" style="background:var(--badge-green-bg);color:var(--badge-green)">COMPLETE</span>`;
                    statusClass = 'job-complete';
                    break;
                case 'FAILED':
                    statusBadge = `<span class="badge" style="background:var(--badge-red-bg);color:var(--badge-red)">FAILED</span>`;
                    statusClass = 'job-failed';
                    break;
                case 'CANCELLED':
                    statusBadge = `<span class="badge" style="background:var(--bg-tertiary,#333);color:var(--text-muted)">CANCELLED</span>`;
                    statusClass = 'job-cancelled';
                    break;
            }

            html += `<div class="history-card ${statusClass}" id="job-${PA.escAttr(job.id)}">`;
            html += `<div class="hc-info" style="flex:1">`;
            html += `<div class="hc-title" style="display:flex;align-items:center;gap:8px">${statusBadge} ${PA.esc(entry)}${PA.esc(proc)}</div>`;
            html += `<div class="hc-meta" style="margin-top:4px">${PA.esc(job.currentStep || '')} ${elapsed ? '| ' + elapsed : ''}</div>`;

            if (job.status === 'RUNNING' || job.status === 'QUEUED') {
                html += `<div class="progress-bar" style="margin-top:6px;height:4px"><div class="progress-fill" style="width:${pct}%"></div></div>`;
            }
            if (job.error) {
                html += `<div style="color:var(--badge-red);font-size:11px;margin-top:4px">${PA.esc(job.error)}</div>`;
            }
            html += `</div>`;

            html += `<div class="hc-actions">`;
            if (job.status === 'RUNNING' || job.status === 'QUEUED') {
                html += `<button class="btn btn-sm btn-danger" onclick="event.stopPropagation(); PA.jobs.cancel('${PA.escJs(job.id)}')">Cancel</button>`;
            }
            if (job.status === 'COMPLETE' && job.resultName) {
                html += `<button class="btn btn-sm btn-primary" onclick="event.stopPropagation(); PA.loadAnalysis('${PA.escJs(job.resultName)}')">Load</button>`;
            }
            html += `</div></div>`;
        }
        container.innerHTML = html;
    },

    async cancel(jobId) {
        try {
            const result = await PA.api.cancelJob(jobId);
            if (result && result.cancelled) {
                PA.toast('Job cancelled', 'success');
                PA.jobs.loadJobs();
            } else {
                PA.toast('Could not cancel job', 'error');
            }
        } catch (e) {
            PA.toast('Cancel failed: ' + e.message, 'error');
        }
    },

    startPolling() {
        PA.jobs.stopPolling();
        PA.jobs.loadJobs();
        const pollStart = Date.now();
        PA.jobs.pollInterval = setInterval(() => {
            // Auto-stop after 30 minutes max
            if (Date.now() - pollStart > 30 * 60 * 1000) { PA.jobs.stopPolling(); return; }
            PA.jobs.loadJobs();
        }, PollConfig.jobStatusMs);
    },

    stopPolling() {
        if (PA.jobs.pollInterval) {
            clearInterval(PA.jobs.pollInterval);
            PA.jobs.pollInterval = null;
        }
    }
};

// ==================== START NEW ANALYSIS ====================

PA.updateFormSteps = function() {
    const step2 = document.getElementById('afStep2');
    const step3 = document.getElementById('afStep3');
    if (!step2 || !step3) return;

    const project = document.getElementById('homeProject').value;
    const env = document.getElementById('homeEnv').value;
    const isLegacy = !project;
    const step1Done = isLegacy || (project && env);

    if (step1Done) {
        step2.classList.remove('disabled');
    } else {
        step2.classList.add('disabled');
        step3.classList.add('disabled');
        return;
    }

    step3.classList.remove('disabled');
};

PA.onProjectChange = async function() {
    const projSel = document.getElementById('homeProject');
    const envSel = document.getElementById('homeEnv');
    const schemaSel = document.getElementById('homeSchema');
    const hintEl = document.getElementById('afConnHint');
    const ownerHint = document.getElementById('afOwnerHint');
    const projectName = projSel.value;

    envSel.innerHTML = '<option value="">Select env</option>';
    envSel.disabled = true;

    if (!projectName) {
        if (hintEl) hintEl.textContent = 'Optional \u2014 uses default DB config if empty';
        if (ownerHint) ownerHint.textContent = 'DB schema that owns the object';
        try {
            const users = await PA.api.getDbUsers();
            schemaSel.innerHTML = '<option value="">Auto-detect (all schemas)</option>';
            for (const u of users) {
                schemaSel.innerHTML += `<option value="${PA.escAttr(u.username)}">${PA.esc(u.username)}</option>`;
            }
        } catch (e) { /* ignore */ }
        PA.updateFormSteps();
        return;
    }

    if (hintEl) hintEl.textContent = 'Select environment below';
    if (ownerHint) ownerHint.textContent = 'Pick owner after selecting env';

    try {
        const envs = await PA.api.listEnvironments(projectName);
        envSel.disabled = false;
        for (const e of envs) {
            envSel.innerHTML += `<option value="${PA.escAttr(e.name)}">${PA.esc(e.name)}${e.zone ? ' (' + PA.esc(e.zone) + ')' : ''}</option>`;
        }
        if (envs.length === 1) {
            envSel.selectedIndex = 1;
            PA.onEnvChange();
        }
    } catch (e) { /* ignore */ }

    schemaSel.innerHTML = '<option value="">Select env first</option>';
    PA.updateFormSteps();
};

PA.onEnvChange = async function() {
    const projectName = document.getElementById('homeProject').value;
    const envName = document.getElementById('homeEnv').value;
    const schemaSel = document.getElementById('homeSchema');
    const ownerHint = document.getElementById('afOwnerHint');

    schemaSel.innerHTML = '<option value="">Auto-detect (all schemas)</option>';
    if (!projectName || !envName) { PA.updateFormSteps(); return; }

    if (ownerHint) ownerHint.textContent = 'Loading owners...';

    try {
        const resolved = await PA.api.resolveEnvironment(projectName, envName);
        if (resolved && resolved.connections) {
            for (const c of resolved.connections) {
                schemaSel.innerHTML += `<option value="${PA.escAttr(c.username)}">${PA.esc(c.username)}${c.description ? ' \u2014 ' + PA.esc(c.description) : ''}</option>`;
            }
        } else if (resolved && resolved.users) {
            for (const u of resolved.users) {
                schemaSel.innerHTML += `<option value="${PA.escAttr(u.username)}">${PA.esc(u.username)}</option>`;
            }
        }
        if (ownerHint) ownerHint.textContent = 'Select owner or leave auto-detect';
    } catch (e) {
        if (ownerHint) ownerHint.textContent = 'Failed to load owners';
    }
    PA.updateFormSteps();
};

PA.startAnalysis = async function() {
    const username = document.getElementById('homeSchema').value || null;
    const objectName = document.getElementById('homeObject').value.trim();
    const objectType = document.getElementById('homeType').value;
    const project = document.getElementById('homeProject').value || null;
    const env = document.getElementById('homeEnv').value || null;

    if (!objectName) {
        PA.toast('Enter an object name (e.g. PKG_CUSTOMER or PKG.PROC_NAME)', 'error');
        return;
    }

    // Show progress
    const prog = document.getElementById('homeProgress');
    const progText = document.getElementById('homeProgressText');
    const progFill = document.getElementById('homeProgressFill');
    prog.style.display = 'block';
    progFill.style.width = '10%';
    progText.textContent = 'Submitting analysis job...';

    // Submit the analysis FIRST, then connect SSE and start polling
    let jobId = null;
    try {
        const fastMode = document.getElementById('homeFastMode') && document.getElementById('homeFastMode').checked;
        const resp = await PA.api.analyze(username, objectName, objectType, null, project, env, fastMode);
        if (resp && resp.error) {
            prog.style.display = 'none';
            PA.toast(resp.error, 'error');
            return;
        }
        if (resp && resp.jobId) {
            jobId = resp.jobId;
            PA.jobs.currentJobId = jobId;
            progText.textContent = (resp.status === 'queued' ? 'Queued' : 'Submitted') + ': ' + jobId;
        }
    } catch (e) {
        prog.style.display = 'none';
        PA.toast('Failed to start: ' + e.message, 'error');
        return;
    }

    // NOW start job polling (job exists in backend)
    PA.jobs.startPolling();

    // Track whether completion was handled (by SSE or by polling)
    let completionHandled = false;

    // Connect to unified queue SSE for real-time progress
    try {
        const queueES = new EventSource('/api/queue/events');
        const qJobId = jobId;
        ['job-started','job-progress','job-complete','job-failed','job-cancelled'].forEach(evt => {
            queueES.addEventListener(evt, (e) => {
                try {
                    const d = JSON.parse(e.data);
                    const job = d.job;
                    if (!job || job.id !== qJobId) return;
                    if (evt === 'job-started') {
                        progText.textContent = 'Processing...';
                        progFill.style.width = '15%';
                    } else if (evt === 'job-progress') {
                        progText.textContent = job.currentStep || '';
                        if (job.progressPercent > 0) progFill.style.width = job.progressPercent + '%';
                    } else if (evt === 'job-complete') {
                        if (completionHandled) return;
                        completionHandled = true;
                        progFill.style.width = '100%';
                        progText.textContent = 'Complete!';
                        setTimeout(() => { prog.style.display = 'none'; }, 2000);
                        PA.jobs.stopPolling();
                        PA._loadCompletedAnalysis(qJobId);
                        PA.toast('Analysis complete', 'success');
                        queueES.close();
                    } else if (evt === 'job-failed' || evt === 'job-cancelled') {
                        if (completionHandled) return;
                        completionHandled = true;
                        prog.style.display = 'none';
                        PA.jobs.stopPolling();
                        PA.toast(evt === 'job-cancelled' ? 'Cancelled' : ('Failed: ' + (job.error || '')), 'error');
                        queueES.close();
                    }
                } catch (err) { console.warn('Queue SSE parse error:', err); }
            });
        });
        queueES.onerror = () => { queueES.close(); };
    } catch (e) {
        console.warn('Queue SSE unavailable:', e);
    }

    // Also connect PL/SQL-specific SSE (backward compat, sends finer-grained messages)
    PA.api.connectProgress(
        (msg) => {
            if (!completionHandled) progText.textContent = msg;
        },
        async (msg) => {
            if (completionHandled) return;
            completionHandled = true;
            progFill.style.width = '100%';
            progText.textContent = msg;
            setTimeout(() => { prog.style.display = 'none'; }, 2000);
            PA.jobs.stopPolling();
            await PA._loadCompletedAnalysis(jobId);
            PA.toast(msg, 'success');
        },
        (msg) => {
            console.warn('[SSE] Error:', msg, '— queue SSE or job polling will handle completion');
        }
    );

    // Also poll for job completion independently (backup for SSE failure)
    PA._pollForJobCompletion(jobId, prog, progText, progFill, () => completionHandled, () => { completionHandled = true; });
};

/**
 * Poll the job endpoint until it completes, fails, or is cancelled.
 * This is the RELIABLE path — SSE is a nice-to-have for live progress text.
 */
PA._pollForJobCompletion = function(jobId, prog, progText, progFill, isHandled, markHandled) {
    if (!jobId) return;
    const MAX_POLL_MS = 30 * 60 * 1000; // 30 min max
    const pollStart = Date.now();
    let errorCount = 0;

    const interval = setInterval(async () => {
        if (isHandled()) { clearInterval(interval); return; }
        if (Date.now() - pollStart > MAX_POLL_MS) {
            clearInterval(interval);
            prog.style.display = 'none';
            PA.jobs.stopPolling();
            PA.toast('Analysis timed out (30 min)', 'error');
            return;
        }

        try {
            const job = await PA.api.getJob(jobId);
            if (!job) { errorCount++; if (errorCount > 20) { clearInterval(interval); } return; }
            errorCount = 0;

            // Update progress bar from job state
            if (job.totalSteps > 0) {
                const pct = Math.round((job.stepNumber / job.totalSteps) * 100);
                progFill.style.width = pct + '%';
            }
            if (job.currentStep) progText.textContent = job.currentStep;

            if (job.status === 'COMPLETE') {
                if (isHandled()) { clearInterval(interval); return; }
                markHandled();
                clearInterval(interval);
                progFill.style.width = '100%';
                progText.textContent = 'Complete!';
                setTimeout(() => { prog.style.display = 'none'; }, 2000);
                PA.jobs.stopPolling();
                await PA._loadCompletedAnalysis(jobId);
                PA.toast('Analysis complete: ' + (job.resultName || ''), 'success');
            } else if (job.status === 'FAILED') {
                markHandled();
                clearInterval(interval);
                prog.style.display = 'none';
                PA.jobs.stopPolling();
                PA.jobs.loadJobs();
                PA.toast('Analysis failed: ' + (job.error || 'Unknown error'), 'error');
            } else if (job.status === 'CANCELLED') {
                markHandled();
                clearInterval(interval);
                prog.style.display = 'none';
                PA.jobs.stopPolling();
                PA.jobs.loadJobs();
                PA.toast('Analysis cancelled', 'error');
            }
        } catch (e) {
            errorCount++;
            if (errorCount > 20) {
                clearInterval(interval);
                console.error('[PA] Job poll gave up after 20 errors');
            }
        }
    }, PollConfig.jobStatusMs);
};

/**
 * Load a completed analysis — by job resultName or fallback to latest.
 */
PA._loadCompletedAnalysis = async function(jobId) {
    // Try to get result name from job
    let resultName = null;
    if (jobId) {
        try {
            const job = await PA.api.getJob(jobId);
            if (job && job.resultName) resultName = job.resultName;
        } catch (e) { /* fallback below */ }
    }

    // Load by name if available, otherwise latest
    let result = null;
    if (resultName) {
        result = await PA.api.loadAnalysis(resultName);
    }
    if (!result) {
        result = await PA.api.getLatestResult();
    }

    if (result && (result.status === 'complete' || result.callGraph || result.procedureCount > 0)) {
        PA.analysisData = result;
        PA.version._currentView = null;
        PA.goAnalysis();
        PA.updateTopbar(result);
        await Promise.allSettled([
            PA.version.loadInfo().then(() => PA.version.update(result)),
            PA.loadSidebarData(),
            PA.tableOps.load()
        ]);
        if (PA.joins) PA.joins._loaded = false;
        if (PA.cursors) PA.cursors._loaded = false;
        if (PA.predicates) PA.predicates._loaded = false;
        PA.autoSelectEntryProc(result);
    }
    PA.loadHistory();
    PA.jobs.loadJobs();
};

// ==================== LEFT PANEL (SCREEN 2) ====================
PA.switchLeftTab = function(tab) {
    document.querySelectorAll('.ltab').forEach(t => t.classList.remove('active'));
    document.querySelector(`.ltab[data-tab="${tab}"]`)?.classList.add('active');
    document.querySelectorAll('.left-panel-list').forEach(p => p.style.display = 'none');
    const map = { procedures: 'lpProcedures', tables: 'lpTables' };
    const panel = document.getElementById(map[tab]);
    if (panel) panel.style.display = '';
};

PA.filterLeft = function() {
    const q = (document.getElementById('leftFilter')?.value || '').toLowerCase();
    const visible = document.querySelector('.left-panel-list[style=""]') ||
                    document.querySelector('.left-panel-list:not([style*="none"])');
    if (!visible) return;

    if (!q) {
        // Reset: show everything
        visible.querySelectorAll('.lp-item, .tree-node').forEach(el => el.style.display = '');
        visible.querySelectorAll('.tree-children').forEach(el => el.style.display = '');
        return;
    }

    // For flow tree: show nodes whose name or search attribute matches, and their ancestors
    visible.querySelectorAll('.lp-item').forEach(item => {
        const txt = (item.dataset.filter || item.textContent || '').toLowerCase();
        item.style.display = txt.includes(q) ? '' : 'none';
    });
    // Also search tree-node elements by data-search
    visible.querySelectorAll('.tree-node').forEach(node => {
        const search = (node.dataset.search || '').toLowerCase();
        const hasVisibleChild = node.querySelector('.lp-item:not([style*="display: none"]), .lp-item:not([style*="none"])');
        if (search.includes(q) || hasVisibleChild) {
            node.style.display = '';
            // Expand matching tree nodes
            const children = node.querySelector('.tree-children');
            if (children) children.style.display = '';
        }
    });
};

PA.loadSidebarData = async function() {
    // Load procedures from call flow (not all procedures)
    // Use the entry proc's call tree to get only flow-relevant procedures
    const result = PA.analysisData;
    if (result) {
        const entrySchema = (result.entrySchema || '').toUpperCase();
        const entryObj = (result.entryObjectName || '').toUpperCase();
        const entryProc = (result.entryProcedure || '').toUpperCase();

        // Build progressively less specific IDs to try
        const idsToTry = [];
        if (entryProc) idsToTry.push([entrySchema, entryObj, entryProc].filter(Boolean).join('.'));
        idsToTry.push([entrySchema, entryObj].filter(Boolean).join('.'));
        if (entryObj) idsToTry.push(entryObj);

        let loaded = false;
        for (const entryId of idsToTry) {
            if (!entryId || loaded) continue;
            try {
                console.log('[PA] loadSidebarData: trying entryId =', entryId);
                const detail = await PA.api.getProcDetail(entryId);
                if (detail && detail.callTree) {
                    PA.renderFlowTree(detail.callTree);
                    loaded = true;
                    break;
                }
                // Fallback: try call tree directly
                const tree = await PA.api.getCallTree(entryId);
                if (tree && tree.children && tree.children.length > 0) {
                    PA.renderFlowTree(tree);
                    loaded = true;
                    break;
                }
            } catch (e) {
                console.warn('[PA] loadSidebarData: failed for', entryId, e.message || e);
            }
        }

        if (!loaded) {
            console.warn('[PA] loadSidebarData: all IDs failed, showing flat list');
            const procedures = await PA.api.getProcedures();
            PA.renderProcedureList(procedures);
        }
    } else {
        const procedures = await PA.api.getProcedures();
        PA.renderProcedureList(procedures);
    }
    const tables = await PA.api.getTableOperations();
    PA.renderTableList(tables);
};

/**
 * Render the call flow tree in the sidebar.
 * Shows ONLY procedures that are part of the call flow from the entry point.
 * The tree structure mirrors the actual call hierarchy (parent → children calls).
 */
PA.renderFlowTree = function(callTree) {
    const panel = document.getElementById('lpProcedures');
    if (!callTree) {
        panel.innerHTML = '<div class="empty-msg">No call flow data</div>';
        return;
    }

    // Count total unique nodes in the flow
    const seen = new Set();
    const countNodes = (node) => {
        if (!node || seen.has(node.id)) return;
        seen.add(node.id);
        if (node.children && !node.circular) {
            for (const c of node.children) countNodes(c);
        }
    };
    countNodes(callTree);
    const totalCount = seen.size;

    // Header
    let html = `<div style="padding:6px 10px;font-size:10px;color:var(--text-muted);border-bottom:1px solid var(--border);display:flex;justify-content:space-between">`;
    html += `<span>Call Flow</span><span>${totalCount} procedures</span></div>`;

    // Render tree recursively
    const visited = new Set();
    html += PA._renderFlowNode(callTree, 0, visited, true);

    panel.innerHTML = html;
};

/** Render a single flow node and its children recursively */
PA._renderFlowNode = function(node, depth, visited, expanded) {
    if (!node) return '';
    const id = node.id || '';
    const name = node.name || id.split('.').pop() || '?';
    const schema = node.schemaName || '';
    const pkg = node.packageName || '';
    const callType = node.callType || '';
    const unitType = node.unitType || '';
    const isCircular = node.circular || false;
    const children = (node.children && !isCircular) ? node.children : [];
    const hasChildren = children.length > 0;
    const isVisited = visited.has(id);

    // Track visited to handle dedup display
    visited.add(id);

    const colorObj = PA.getSchemaColor(schema);
    const typeLabel = unitType === 'FUNCTION' ? 'F' : unitType === 'TRIGGER' ? 'T' : 'P';
    const indent = depth * 16;

    // Rich tooltip
    const loc = (node.startLine && node.endLine) ? (node.endLine - node.startLine + 1) : 0;
    let tip = (unitType || 'PROCEDURE') + (pkg ? ' in Package ' + pkg : '');
    tip += '\n' + id;
    if (callType) tip += '\nScope: ' + callType;
    if (node.startLine) tip += '\nLines: ' + node.startLine + '-' + (node.endLine || '?') + (loc ? ' (' + loc + ' LOC)' : '');
    if (children.length) tip += '\nCalls: ' + children.length + ' procedure' + (children.length > 1 ? 's' : '');
    const tipAttr = PA.escAttr(tip);

    let html = '';

    if (hasChildren && !isVisited) {
        // Node with children — collapsible
        html += `<div class="tree-node" style="margin-left:${indent}px" data-search="${PA.escAttr(id)}">`;
        html += `<div class="tree-toggle ${expanded ? 'expanded' : ''}" onclick="PA.toggleTreeNode(this)">`;
        html += `<span class="tree-arrow">${expanded ? '&#9660;' : '&#9654;'}</span>`;
        // Schema badge (small)
        if (schema && depth > 0) {
            html += `<span class="ct-schema-badge" style="background:${colorObj.bg};color:${colorObj.fg};font-size:7px;padding:0 4px;margin-right:2px">${PA.esc(schema.substring(0, 3))}</span>`;
        }
        html += `<span class="lp-icon ${typeLabel}" style="width:14px;height:14px;font-size:7px">${typeLabel}</span>`;
        html += `<span class="lp-name flow-name" style="font-size:11px;cursor:pointer" title="${tipAttr}"`;
        html += ` onclick="event.stopPropagation(); PA.showProcedure('${PA.escJs(id)}')">${PA.esc(name)}</span>`;
        // Call type badge
        if (callType === 'EXTERNAL') {
            html += `<span class="lp-type-badge EXTERNAL" style="font-size:6px;padding:0 3px">EXT</span>`;
        } else if (callType === 'TRIGGER') {
            html += `<span class="lp-type-badge TRIGGER" style="font-size:6px;padding:0 3px">TRG</span>`;
        }
        html += `<span class="tree-count">${children.length}</span>`;
        html += `</div>`;

        // Children container
        html += `<div class="tree-children" ${expanded ? '' : 'style="display:none"'}>`;
        for (const child of children) {
            html += PA._renderFlowNode(child, depth + 1, visited, depth < 1);
        }
        html += `</div></div>`;
    } else {
        // Leaf node (no children or circular/already visited)
        html += `<div class="lp-item tree-leaf" style="margin-left:${indent}px;padding-left:20px"`;
        html += ` data-id="${PA.escAttr(id)}" data-filter="${PA.escAttr(id + ' ' + name)}"`;
        html += ` onclick="PA.showProcedure('${PA.escJs(id)}')">`;
        if (schema && depth > 0) {
            html += `<span class="ct-schema-badge" style="background:${colorObj.bg};color:${colorObj.fg};font-size:7px;padding:0 4px;margin-right:2px">${PA.esc(schema.substring(0, 3))}</span>`;
        }
        html += `<span class="lp-icon ${typeLabel}" style="width:14px;height:14px;font-size:7px">${typeLabel}</span>`;
        html += `<span class="lp-name" style="font-size:11px" title="${tipAttr}">${PA.esc(name)}</span>`;
        if (callType === 'EXTERNAL') {
            html += `<span class="lp-type-badge EXTERNAL" style="font-size:6px;padding:0 3px">EXT</span>`;
        } else if (callType === 'TRIGGER') {
            html += `<span class="lp-type-badge TRIGGER" style="font-size:6px;padding:0 3px">TRG</span>`;
        }
        if (isCircular) {
            html += `<span style="font-size:7px;color:var(--red);margin-left:4px">&#x21BA;</span>`;
        } else if (isVisited) {
            html += `<span style="font-size:7px;color:var(--text-muted);margin-left:4px">&#x2192;</span>`;
        }
        html += `</div>`;
    }

    return html;
};

/** Fallback: render flat procedure list (when no call tree available) */
PA.renderProcedureList = function(procedures) {
    const panel = document.getElementById('lpProcedures');
    if (!procedures || procedures.length === 0) {
        panel.innerHTML = '<div class="empty-msg">No procedures found</div>';
        return;
    }

    let html = `<div style="padding:6px 10px;font-size:10px;color:var(--text-muted);border-bottom:1px solid var(--border)">All Procedures (${procedures.length})</div>`;
    const sorted = [...procedures].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    for (const p of sorted) {
        const typeLabel = p.unitType === 'FUNCTION' ? 'F' : p.unitType === 'TRIGGER' ? 'T' : 'P';
        const ct = p.callType || '';
        const ctBadge = ct === 'INTERNAL'
            ? '<span class="lp-type-badge INTERNAL" style="font-size:7px;padding:0 3px">INT</span>'
            : ct === 'EXTERNAL'
            ? '<span class="lp-type-badge EXTERNAL" style="font-size:7px;padding:0 3px">EXT</span>'
            : '';
        html += `<div class="lp-item tree-leaf" data-id="${PA.escAttr(p.id)}" data-filter="${PA.escAttr(p.id + ' ' + (p.name||''))}"
                      onclick="PA.showProcedure('${PA.escJs(p.id)}')">
            <span class="lp-icon ${typeLabel}" style="width:16px;height:16px;font-size:8px">${typeLabel}</span>
            <span class="lp-name" title="${PA.escAttr(p.id)}">${PA.esc(p.name || p.id)}</span>
            ${ctBadge}
        </div>`;
    }
    panel.innerHTML = html;
};

/** Toggle tree node expand/collapse */
PA.toggleTreeNode = function(el) {
    const children = el.nextElementSibling;
    if (!children) return;
    const arrow = el.querySelector('.tree-arrow');
    if (children.style.display === 'none') {
        children.style.display = '';
        el.classList.add('expanded');
        if (arrow) arrow.innerHTML = '&#9660;';
    } else {
        children.style.display = 'none';
        el.classList.remove('expanded');
        if (arrow) arrow.innerHTML = '&#9654;';
    }
};

PA.renderTableList = function(tables) {
    const panel = document.getElementById('lpTables');
    if (!tables || tables.length === 0) {
        panel.innerHTML = '<div class="empty-msg">No tables found</div>';
        return;
    }
    const sorted = [...tables].sort((a, b) => (a.tableName || '').localeCompare(b.tableName || ''));
    // Count by type
    const tblCount = sorted.filter(t => !t.tableType || t.tableType === 'TABLE').length;
    const viewCount = sorted.filter(t => t.tableType === 'VIEW').length;
    const mvCount = sorted.filter(t => t.tableType === 'MATERIALIZED VIEW').length;
    let header = `<div style="padding:6px 10px;font-size:10px;color:var(--text-muted);border-bottom:1px solid var(--border);display:flex;gap:6px;flex-wrap:wrap">`;
    header += `<span>${sorted.length} total</span>`;
    if (tblCount > 0) header += `<span>${tblCount} tables</span>`;
    if (viewCount > 0) header += `<span style="color:var(--orange)">${viewCount} views</span>`;
    if (mvCount > 0) header += `<span style="color:var(--purple,#7e22ce)">${mvCount} MVs</span>`;
    header += `</div>`;

    let html = header;
    for (const t of sorted) {
        const ops = (t.operations || []).map(op => `<span class="op-badge ${op}" style="font-size:7px;padding:0 3px">${op[0]}</span>`).join('');
        const count = t.accessCount || (t.accessDetails || []).length;
        const scope = t.external
            ? '<span class="lp-type-badge EXTERNAL" style="font-size:7px;padding:0 3px">EXT</span>'
            : '';
        // Type indicator
        const tt = (t.tableType || 'TABLE').toUpperCase();
        const ttColor = tt === 'VIEW' ? 'var(--orange)' : tt === 'MATERIALIZED VIEW' ? 'var(--purple,#7e22ce)' : 'var(--text-muted)';
        const ttLabel = tt === 'MATERIALIZED VIEW' ? 'MV' : tt === 'VIEW' ? 'V' : 'T';
        const ttBadge = `<span style="font-size:7px;font-weight:700;color:${ttColor};min-width:12px;text-align:center">${ttLabel}</span>`;

        html += `<div class="lp-item" data-filter="${PA.escAttr((t.tableName || '') + ' ' + tt)}"
                      onclick="PA.tableOps.focusTable('${PA.escJs(t.tableName || '')}')">
            ${ttBadge}
            <span class="lp-name" style="color:var(--teal);font-size:11px">${PA.esc(t.tableName || '')}</span>
            <span style="display:flex;gap:1px;align-items:center">${ops}</span>
            ${scope}
            <span class="lp-badge">${count}</span>
        </div>`;
    }
    panel.innerHTML = html;
};

// Schema tree removed — flow tree already shows the call hierarchy

// ==================== RIGHT PANEL (SCREEN 2) ====================
PA.switchRightTab = function(view) {
    document.querySelectorAll('.rtab').forEach(t => t.classList.remove('active'));
    document.querySelector(`.rtab[data-view="${view}"]`)?.classList.add('active');
    document.querySelectorAll('.right-view').forEach(v => v.classList.remove('active'));
    const viewId = {
        callTrace: 'viewCallTrace',
        trace: 'viewTrace',
        refs: 'viewRefs',
        tableOps: 'viewTableOps',
        joins: 'viewJoins',
        cursors: 'viewCursors',
        predicates: 'viewPredicates',
        source: 'viewSource',
        claude: 'viewClaude',
        summary: 'viewSummary'
    }[view];
    const el = document.getElementById(viewId);
    if (el) el.classList.add('active');
    // Render Summary tab on first switch
    if (view === 'summary' && PA.summary && PA.summary.render && PA.analysisData) {
        PA.summary.render(PA.analysisData);
    }

    // Apply scope when switching to Table Ops
    if (view === 'tableOps' && PA.context.procId) {
        PA._renderContextBreadcrumb('toBreadcrumb');
        if (PA.tableOps && PA.tableOps.applyScope) PA.tableOps.applyScope();
    }

    // Auto-load references when switching to References tab
    if (view === 'refs' && PA.currentDetail && PA.currentDetail.id) {
        PA.refs.load(PA.currentDetail.id);
    }

    // Auto-load Claude results when switching to Claude tab — also reconnects to running sessions
    if (view === 'claude') {
        PA._renderContextBreadcrumb('claudeBreadcrumb');
        if (PA.claude && PA.claude.applyScope) PA.claude.applyScope();
        PA.claude.checkStatus();
    }

    // Auto-load source when switching to Source tab (unless caller will do it)
    if (view === 'source' && PA.currentDetail && !PA._skipSourceAutoLoad) {
        const sf = PA.currentDetail.sourceFile;
        const startLine = PA.currentDetail.startLine || 0;
        if (sf) {
            PA.sourceView.open(sf, startLine);
        } else {
            const schema = PA.currentDetail.schemaName || '';
            const pkg = PA.currentDetail.packageName || '';
            if (schema && pkg) {
                PA.sourceView.open(schema + '.' + pkg, startLine);
            }
        }
    }
    if (view === 'joins' && PA.joins && PA.joins.load && !PA.joins._loaded) {
        PA.joins._loaded = true;
        PA.joins.load();
    }

    if (view === 'cursors' && PA.cursors && PA.cursors.load && !PA.cursors._loaded) {
        PA.cursors._loaded = true;
        PA.cursors.load();
    }

    if (view === 'predicates' && PA.predicates) {
        if (PA.predicates.load && !PA.predicates._loaded) {
            PA.predicates._loaded = true;
            PA.predicates.load();
        } else if (PA.predicates._loaded && PA.context && PA.context.procId && PA.predicates.applyScope) {
            PA.predicates.applyScope();
        }
    }

    PA._skipSourceAutoLoad = false;
    PA._currentRightTab = view;
};

// ==================== ANALYSIS VIEW TOGGLE ====================
PA.analysisViewMode = 'static';

PA.setAnalysisView = function(mode) {
    PA.analysisViewMode = mode;
    document.querySelectorAll('.dh-vt-btn').forEach(function(b) {
        b.classList.toggle('active', b.dataset.vt === mode);
    });
    if (mode === 'claude') {
        PA.switchRightTab('claude');
        PA.claude.setView('claude');
    } else {
        if (PA._currentRightTab === 'claude') PA.switchRightTab('callTrace');
    }
};

PA._showViewToggleIfClaude = function() {
    var toggle = document.getElementById('dhViewToggle');
    if (!toggle) return;
    // Show toggle only if claude verification data exists for this analysis
    var hasClaudeData = PA.claude && PA.claude.hasData;
    toggle.style.display = hasClaudeData ? '' : 'none';
};

// ==================== DETAIL HEADER ====================
PA.currentDetail = null;

PA.populateDetailHeader = function(detail) {
    PA.currentDetail = detail;
    const header = document.getElementById('detailHeader');
    if (!detail || !detail.id) { header.style.display = 'none'; return; }
    header.style.display = '';

    const schema = detail.schemaName || '';
    const pkg = detail.packageName || '';
    const name = detail.name || detail.id;
    const fullName = [schema, pkg, name].filter(Boolean).join('.');
    const colorObj = PA.getSchemaColor(schema);

    // Schema badge + full name
    const schemaBadge = document.getElementById('dhSchema');
    schemaBadge.textContent = schema || '?';
    schemaBadge.style.background = colorObj.bg;
    schemaBadge.style.color = colorObj.fg;
    document.getElementById('dhFullName').textContent = fullName;

    // Meta (type + unit type)
    const meta = document.getElementById('dhMeta');
    let metaHtml = '';
    if (detail.unitType) metaHtml += `<span class="dh-meta-item"><span class="dh-meta-label">Type</span> ${PA.esc(detail.unitType)}</span>`;
    if (detail.callType) metaHtml += `<span class="dh-meta-item"><span class="dh-meta-label">Scope</span> ${PA.esc(detail.callType)}</span>`;
    if (detail.startLine) {
        const lineRange = detail.endLine ? `${detail.startLine}-${detail.endLine}` : `${detail.startLine}`;
        metaHtml += `<span class="dh-meta-item"><span class="dh-meta-label">Lines</span> ${lineRange}</span>`;
    }
    meta.innerHTML = metaHtml;

    // Stats boxes with Node/Subtree toggle
    const stats = detail.stats || {};
    const ns = detail.nodeStats || {};
    const statsEl = document.getElementById('dhStats');

    // Store both stat sets on the detail for toggle
    PA._detailStatsMode = PA._detailStatsMode || 'subtree';

    PA._renderDetailStats = function() {
        const mode = PA._detailStatsMode;
        const d = PA.currentDetail;
        if (!d) return;
        const s = d.stats || {};
        const n = d.nodeStats || {};
        const isSubtree = mode === 'subtree';

        const calls = isSubtree ? (s.totalNodes || 0) : (n.directCalls || 0);
        const tables = isSubtree ? (d.tableCount || 0) : (n.tableCount || 0);
        const dbOps = isSubtree ? (d.dbOpCount || 0) : (n.dbOps || 0);
        const intC = isSubtree ? (s.internalCalls || 0) : (n.internalCalls || 0);
        const extC = isSubtree ? (s.externalCalls || 0) : (n.externalCalls || 0);
        const dynC = isSubtree ? (s.dynamicCalls || 0) : (n.dynamicCalls || 0);
        const loc = isSubtree ? (d.totalLoc || 0) : (d.nodeLoc || 0);
        const depth = isSubtree ? (s.maxDepth || 0) : '-';
        const ops = isSubtree ? (d.operations || []) : (n.operations || []);

        const toggleBtnStyle = 'font-size:9px;padding:2px 8px;border-radius:4px;cursor:pointer;font-weight:700;border:1px solid var(--border);margin-right:6px';
        const nodeActive = !isSubtree ? 'background:var(--accent);color:white;border-color:var(--accent)' : 'background:var(--bg-card);color:var(--text-muted)';
        const subtreeActive = isSubtree ? 'background:var(--accent);color:white;border-color:var(--accent)' : 'background:var(--bg-card);color:var(--text-muted)';

        let html = `<span style="${toggleBtnStyle};${nodeActive}" onclick="PA._detailStatsMode='node'; PA._renderDetailStats();" title="Show stats for this procedure only (its own code, its direct calls)">Node</span>`;
        html += `<span style="${toggleBtnStyle};${subtreeActive}" onclick="PA._detailStatsMode='subtree'; PA._renderDetailStats();" title="Show stats for the full subtree from this procedure (all downstream calls, their tables, total LOC)">Subtree</span>`;
        html += `<div class="dh-stat accent" title="${isSubtree ? 'Total procedures/functions called in this subtree' : 'Direct children called by this procedure'}"><span class="dh-stat-value">${calls}</span><span class="dh-stat-label">${isSubtree ? 'Total Calls' : 'Direct Calls'}</span></div>`;
        html += `<div class="dh-stat teal" title="${isSubtree ? 'Database tables accessed anywhere in this subtree' : 'Tables accessed directly by this procedure (not its children)'}"><span class="dh-stat-value">${tables}</span><span class="dh-stat-label">Tables</span></div>`;
        html += `<div class="dh-stat blue" title="${isSubtree ? 'Total SQL operations (SELECT/INSERT/UPDATE/DELETE) in this subtree' : 'SQL operations directly in this procedure'}"><span class="dh-stat-value">${dbOps}</span><span class="dh-stat-label">DB Ops</span></div>`;
        html += `<div class="dh-stat green" title="${isSubtree ? 'Calls to procedures in the same package/schema' : 'Direct calls to same-package procedures'}"><span class="dh-stat-value">${intC}</span><span class="dh-stat-label">Internal</span></div>`;
        html += `<div class="dh-stat red" title="${isSubtree ? 'Calls to procedures in other packages/schemas' : 'Direct calls to external packages'}"><span class="dh-stat-value">${extC}</span><span class="dh-stat-label">External</span></div>`;
        html += `<div class="dh-stat purple" title="${isSubtree ? 'EXECUTE IMMEDIATE calls with dynamic SQL' : 'Direct EXECUTE IMMEDIATE calls'}"><span class="dh-stat-value">${dynC}</span><span class="dh-stat-label">Dynamic</span></div>`;
        if (isSubtree) html += `<div class="dh-stat" title="Deepest nesting level in the call chain from this procedure"><span class="dh-stat-value">${depth}</span><span class="dh-stat-label">Max Depth</span></div>`;
        html += `<div class="dh-stat" title="${isSubtree ? 'Total lines of code across all procedures in this subtree' : 'Lines of code in this procedure only (startLine to endLine)'}"><span class="dh-stat-value">${loc}</span><span class="dh-stat-label">${isSubtree ? 'Flow LOC' : 'Node LOC'}</span></div>`;

        document.getElementById('dhStats').innerHTML = html;

        // Also update badges for this mode
        const opTooltips = {
            SELECT: 'Reads data from tables (SELECT queries)',
            INSERT: 'Inserts new rows into tables',
            UPDATE: 'Modifies existing rows in tables',
            DELETE: 'Removes rows from tables',
            MERGE: 'Upsert operation (INSERT or UPDATE based on condition)'
        };
        let bHtml = '';
        for (const op of ops) {
            bHtml += `<span class="dh-op-badge ${PA.esc(op)}" title="${opTooltips[op] || op}">${PA.esc(op)}</span>`;
        }
        if (d.hasTransaction) {
            bHtml += `<span class="dh-txn-badge commit" title="This flow contains an explicit COMMIT statement">COMMIT</span>`;
        } else {
            bHtml += `<span class="dh-txn-badge none" title="No explicit COMMIT or ROLLBACK found in this flow">NO TXN</span>`;
        }
        document.getElementById('dhBadges').innerHTML = bHtml;
    };

    PA._renderDetailStats();

    // Render totals bar (always subtree — combined metrics at a glance)
    PA._renderTotalsBar(detail);

    // Render parameters, variables, statement counts
    PA._renderDetailInfo(detail);

    // Render caller/callee navigation bar
    PA._renderDetailNav(detail);

    // Show/hide Static vs Claude AI toggle
    PA._showViewToggleIfClaude();
};

PA._renderTotalsBar = function(detail) {
    const el = document.getElementById('dhTotals');
    if (!el) return;
    const ft = detail ? detail.flowTotals : null;
    if (!ft) { el.style.display = 'none'; return; }
    const sep = '<span class="dh-totals-sep">|</span>';
    let html = '<span class="dh-totals-label">Flow Totals</span>';
    html += `<span class="dh-totals-item accent"><span class="tv">${ft.loc || 0}</span><span class="tl">LOC</span></span>${sep}`;
    html += `<span class="dh-totals-item teal"><span class="tv">${ft.tables || 0}</span><span class="tl">Tables</span></span>${sep}`;
    html += `<span class="dh-totals-item blue"><span class="tv">${ft.dbOps || 0}</span><span class="tl">DB Ops</span></span>${sep}`;
    html += `<span class="dh-totals-item green"><span class="tv">${ft.internalCalls || 0}</span><span class="tl">Internal</span></span>${sep}`;
    html += `<span class="dh-totals-item red"><span class="tv">${ft.externalCalls || 0}</span><span class="tl">External</span></span>${sep}`;
    html += `<span class="dh-totals-item purple"><span class="tv">${ft.dynamicCalls || 0}</span><span class="tl">Dynamic</span></span>${sep}`;
    html += `<span class="dh-totals-item"><span class="tv">${ft.totalNodes || 0}</span><span class="tl">Nodes</span></span>`;
    el.innerHTML = html;
    el.style.display = '';
};

/** Render a compact Details button instead of inline Parameters/Variables/Statements/Tables/Calls */
PA._renderDetailInfo = function(detail) {
    const infoEl = document.getElementById('dhInfo');
    if (!infoEl) return;
    if (!detail) { infoEl.style.display = 'none'; return; }

    const params = detail.parameters || [];
    const vars = detail.variables || [];
    const totalStmts = detail.statementCount || 0;
    const tables = detail.nodeTables || [];
    const calls = detail.calls || [];
    if (params.length === 0 && vars.length === 0 && totalStmts === 0 && tables.length === 0 && calls.length === 0) {
        infoEl.style.display = 'none';
        return;
    }

    infoEl.style.display = '';
    let html = '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">';
    html += `<button class="btn btn-sm btn-primary" onclick="PA._showDetailPopup()" style="font-weight:700;padding:5px 16px">Details</button>`;
    if (params.length) html += `<span class="badge" style="font-size:10px">${params.length} Params</span>`;
    if (vars.length) html += `<span class="badge" style="font-size:10px">${vars.length} Vars</span>`;
    if (totalStmts) html += `<span class="badge" style="font-size:10px">${totalStmts} Stmts</span>`;
    if (tables.length) html += `<span class="badge" style="font-size:10px">${tables.length} Tables</span>`;
    if (calls.length) html += `<span class="badge" style="font-size:10px">${calls.length} Calls</span>`;
    html += '</div>';
    infoEl.innerHTML = html;
};

/** Show full detail popup for the current procedure */
PA._showDetailPopup = function() {
    const detail = PA.currentDetail;
    if (!detail) return;
    const esc = PA.esc;

    let old = document.getElementById('detail-popup-overlay');
    if (old) old.remove();

    let html = '';

    // Header
    const schema = detail.schemaName || '';
    const fullName = [schema, detail.packageName, detail.name].filter(Boolean).join('.');
    html += '<div class="pp-header">';
    html += '<strong class="pp-name">' + esc(fullName) + '</strong>';
    if (detail.unitType) html += '<span class="badge">' + esc(detail.unitType) + '</span>';
    html += '<button class="btn btn-sm" onclick="document.getElementById(\'detail-popup-overlay\').remove()" style="margin-left:auto">Close</button>';
    html += '</div>';

    html += '<div class="pp-body">';

    // Parameters
    const params = detail.parameters || [];
    if (params.length) {
        html += '<div class="pp-section"><div class="pp-section-title">Parameters (' + params.length + ')</div><div class="pp-section-content">';
        for (const p of params) {
            html += '<div class="dh-param dh-param-' + (p.mode || 'IN').toLowerCase().replace(/\s/g, '') + '">';
            html += '<span class="dh-param-mode">' + esc(p.mode || 'IN') + '</span>';
            html += '<span class="dh-param-name">' + esc(p.name || '?') + '</span>';
            html += '<span class="dh-param-type">' + esc(p.dataType || '?') + '</span>';
            if (p.noCopy) html += '<span class="dh-param-nocopy">NOCOPY</span>';
            html += '</div> ';
        }
        html += '</div></div>';
    }

    // Variables
    const vars = detail.variables || [];
    if (vars.length) {
        const shown = vars.slice(0, 20);
        html += '<div class="pp-section"><div class="pp-section-title">Variables (' + vars.length + ')</div><div class="pp-section-content">';
        for (const v of shown) {
            html += '<div style="display:flex;gap:8px;padding:2px 0;font-size:11px">';
            html += '<span style="font-weight:600;font-family:var(--font-mono)">' + esc(v.name || '?') + '</span>';
            html += '<span style="color:var(--text-muted)">' + esc(v.dataType || '?') + '</span>';
            if (v.constant) html += '<span style="color:var(--orange);font-size:9px;font-weight:700">CONST</span>';
            html += '</div>';
        }
        if (vars.length > 20) html += '<div style="font-size:10px;color:var(--text-muted);padding:4px 0">+' + (vars.length - 20) + ' more...</div>';
        html += '</div></div>';
    }

    // Statements
    const stmtCounts = detail.statementCounts || {};
    const totalStmts = detail.statementCount || 0;
    const stmtKeys = Object.keys(stmtCounts).filter(k => stmtCounts[k] > 0);
    if (stmtKeys.length) {
        html += '<div class="pp-section"><div class="pp-section-title">Statements (' + totalStmts + ')</div>';
        html += '<div class="pp-section-content" style="display:flex;gap:6px;flex-wrap:wrap">';
        for (const k of stmtKeys.sort((a, b) => stmtCounts[b] - stmtCounts[a])) {
            const cls = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'].includes(k) ? 'op-badge ' + k : 'dh-stmt-badge';
            html += '<span class="' + cls + '"><b>' + stmtCounts[k] + '</b> ' + esc(k) + '</span>';
        }
        html += '</div></div>';
    }

    // Tables
    const tables = detail.nodeTables || [];
    if (tables.length) {
        html += '<div class="pp-section"><div class="pp-section-title">Tables (' + tables.length + ')</div>';
        html += '<div class="pp-section-content" style="display:flex;gap:4px;flex-wrap:wrap">';
        for (const t of tables) {
            const ops = (t.operations || []).map(o => o.charAt(0)).join(',');
            html += '<span class="dh-table-chip" title="' + PA.escAttr(t.tableName) + ': ' + (t.operations || []).join(', ') + '">';
            html += '<span class="chip-name">' + esc(t.tableName) + '</span>';
            if (ops) html += '<span class="chip-ops">(' + ops + ')</span>';
            html += '</span>';
        }
        html += '</div></div>';
    }

    // Calls
    const calls = detail.calls || [];
    if (calls.length) {
        html += '<div class="pp-section"><div class="pp-section-title">Calls (' + calls.length + ')</div><div class="pp-section-content">';
        for (const c of calls) {
            const childName = c.name || (c.targetId || c.id || '?').split('.').pop();
            const ct = (c.callType || '').toUpperCase();
            const typeClass = ct === 'EXTERNAL' ? 'EXT' : 'INT';
            html += '<div style="padding:2px 0;font-size:11px">';
            html += '<span class="chip-type ' + typeClass + '" style="margin-right:4px">' + typeClass + '</span>';
            html += '<strong>' + esc(childName) + '</strong>';
            html += '</div>';
        }
        html += '</div></div>';
    }

    // Called By
    const calledBy = detail.calledBy || [];
    if (calledBy.length) {
        html += '<div class="pp-section"><div class="pp-section-title">Called By (' + calledBy.length + ')</div><div class="pp-section-content">';
        for (const c of calledBy) {
            html += '<div style="padding:2px 0;font-size:11px"><strong>' + esc(c.name || c.id || '?') + '</strong></div>';
        }
        html += '</div></div>';
    }

    html += '</div>';

    const overlay = document.createElement('div');
    overlay.id = 'detail-popup-overlay';
    overlay.className = 'pp-overlay';
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    overlay.innerHTML = '<div class="pp-panel">' + html + '</div>';
    document.body.appendChild(overlay);
};

/** Expand collapsed variables list */
PA._expandVars = function() {
    const detail = PA.currentDetail;
    if (!detail) return;
    const container = document.getElementById('dhVarItems');
    const moreBtn = document.getElementById('dhVarMore');
    if (!container || !moreBtn) return;
    moreBtn.remove();
    const vars = detail.variables || [];
    for (let i = 10; i < vars.length; i++) {
        const v = vars[i];
        let span = document.createElement('span');
        span.className = 'dh-var';
        span.title = `Line ${v.lineNumber}: ${v.name} ${v.dataType}${v.constant ? ' CONSTANT' : ''}`;
        span.innerHTML = `<span class="dh-var-name">${PA.esc(v.name)}</span><span class="dh-var-type">${PA.esc(v.dataType)}</span>${v.constant ? '<span class="dh-var-const">CONST</span>' : ''}`;
        container.appendChild(span);
    }
};

/** Render caller/callee navigation bar in detail header */
PA._renderDetailNav = function(detail) {
    const navEl = document.getElementById('dhNav');
    if (!navEl) return;
    if (!detail || !detail.id) { navEl.style.display = 'none'; return; }

    const calledBy = detail.calledBy || [];
    const calls = detail.calls || [];
    const procName = detail.name || detail.id.split('.').pop();

    // If nothing to navigate to, hide
    if (calledBy.length === 0 && calls.length === 0) {
        navEl.style.display = 'none';
        return;
    }

    navEl.style.display = '';
    let html = '';

    // Caller section (who calls this proc)
    if (calledBy.length > 0) {
        html += '<span class="dh-nav-label">Called by</span>';
        const showCallers = calledBy.slice(0, 3);
        for (const c of showCallers) {
            const callerId = c.callerId || c.id || '';
            const callerName = c.name || callerId.split('.').pop() || '?';
            const pkg = c.packageName || '';
            const callerSf = pkg ? ((callerId.split('.')[0] || '') + '.' + pkg.replace(/"/g, '')) : '';
            const line = c.lineNumber || 0;
            html += `<span class="dh-nav-caller" onclick="PA.codeModal.open('${PA.escJs(callerId)}')" title="Open detail: ${PA.escAttr(callerName)}${line ? ' (line ' + line + ')' : ''}">`;
            html += `&larr; ${PA.esc(callerName.replace(/"/g, ''))}`;
            if (line > 0 && callerSf) {
                html += `<span class="dh-nav-line" onclick="event.stopPropagation(); PA.codeModal.openAtLine('${PA.escJs(callerSf)}', ${line})" title="Open at line ${line}">L${line}</span>`;
            }
            html += `</span>`;
        }
        if (calledBy.length > 3) {
            html += `<span style="font-size:10px;color:var(--text-muted)">+${calledBy.length - 3} more</span>`;
        }
    }

    // Current proc — clickable to open source at proc start
    const sf = detail.sourceFile || '';
    const startLine = detail.startLine || 0;
    html += `<span class="dh-nav-sep">&bull;</span>`;
    if (sf && startLine > 0) {
        html += `<span class="dh-nav-current" onclick="PA.codeModal.open('${PA.escJs(detail.id || '')}')" title="Open detail popup" style="cursor:pointer">${PA.esc(procName)}</span>`;
    } else {
        html += `<span class="dh-nav-current">${PA.esc(procName)}</span>`;
    }

    // Children section (who this proc calls)
    if (calls.length > 0) {
        html += `<span class="dh-nav-sep">&rarr;</span>`;
        html += `<span class="dh-nav-label">Calls</span>`;
        html += '<span class="dh-nav-children">';
        const showCalls = calls.slice(0, 8);
        for (const c of showCalls) {
            const childId = c.targetId || c.id || '';
            const childName = c.name || childId.split('.').pop() || '?';
            const ct = (c.callType || '').toUpperCase();
            const typeClass = ct === 'EXTERNAL' ? 'EXT' : ct === 'DYNAMIC' ? 'DYN' : 'INT';
            const callLine = c.lineNumber || 0;
            html += `<span class="dh-nav-child" onclick="PA.codeModal.open('${PA.escJs(childId)}')" title="Open detail: ${PA.escAttr(childName)}${callLine ? ' (called at line ' + callLine + ')' : ''}">`;
            if (ct) html += `<span class="nav-type ${typeClass}">${typeClass}</span>`;
            html += `${PA.esc(childName)}`;
            if (callLine > 0 && sf) {
                html += `<span class="dh-nav-line" onclick="event.stopPropagation(); PA.codeModal.openAtLine('${PA.escJs(sf)}', ${callLine})" title="Open at line ${callLine}">L${callLine}</span>`;
            }
            html += `</span>`;
        }
        if (calls.length > 8) {
            html += `<span style="font-size:10px;color:var(--text-muted)">+${calls.length - 8} more</span>`;
        }
        html += '</span>';
    }

    navEl.innerHTML = html;
};

// ==================== ACTIONS ====================

/** Show procedure — loads detail + populates header + call trace + context */
PA._showProcGen = 0;
PA.showProcedure = async function(procId, opts) {
    const fromRoute = opts && opts.fromRoute;
    const gen = ++PA._showProcGen;

    // Highlight in left panel
    document.querySelectorAll('.lp-item').forEach(el => el.classList.remove('active'));
    const item = document.querySelector(`.lp-item[data-id="${CSS.escape(procId)}"]`);
    if (item) {
        item.classList.add('active');
        item.scrollIntoView({ block: 'nearest' });
    }

    // Set context early so hashchange guard works
    PA.context.procId = procId;

    PA.switchRightTab('callTrace');

    // Update URL hash
    if (PA.analysisData && PA.analysisData.name) {
        const targetHash = '#/analysis/' + encodeURIComponent(PA.analysisData.name) + '/proc/' + encodeURIComponent(procId);
        if (window.location.hash !== targetHash) {
            if (fromRoute) {
                history.replaceState(null, '', targetHash);
            } else {
                history.pushState(null, '', targetHash);
            }
        }
    }

    try {
        // Load full detail (stats, tree, tables)
        console.log('[PA] showProcedure: loading detail for', procId);
        const detail = await PA.api.getProcDetail(procId);
        if (gen !== PA._showProcGen) return;
        console.log('[PA] showProcedure: detail response', detail ? 'OK' : 'NULL',
            detail ? 'callTree=' + (detail.callTree ? 'yes' : 'no') : '');
        if (detail && detail.callTree) {
            PA.populateDetailHeader(detail);

            // Populate shared context for scoped views
            PA.context.procDetail = detail;
            PA.context.scopedTables = detail.tables || detail.tableOperations || [];
            PA.context.callTreeNodeIds = PA._collectTreeNodeIds(detail.callTree);
            console.log('[PA] context populated: procId=' + procId +
                ', scopedTables=' + PA.context.scopedTables.length +
                ', callTreeNodes=' + PA.context.callTreeNodeIds.size);

            // Render breadcrumbs in Table Ops and Claude
            PA._renderContextBreadcrumb('toBreadcrumb');
            PA._renderContextBreadcrumb('claudeBreadcrumb');

            // Apply scope to all data-driven tabs
            if (PA.tableOps && PA.tableOps.applyScope) PA.tableOps.applyScope();
            if (PA.joins && PA.joins._loaded && PA.joins.applyScope) PA.joins.applyScope();
            if (PA.cursors && PA.cursors._loaded && PA.cursors.applyScope) PA.cursors.applyScope();
            if (PA.predicates && PA.predicates._loaded && PA.predicates.applyScope) PA.predicates.applyScope();

            // Render call trace from the detail's call tree
            PA.callTrace.treeData = detail.callTree;
            PA.callTrace.breadcrumbStack = [{ id: detail.callTree.id, name: detail.callTree.name || detail.callTree.id }];
            PA.callTrace.render(detail.callTree);
            // Pre-render flat trace
            PA.trace.renderFromTree(detail.callTree);
            return;
        }
    } catch (e) {
        console.error('getProcDetail failed for', procId, e);
    }

    // Fallback: load call tree directly
    try {
        await PA.callTrace.load(procId);
    } catch (e) {
        console.error('callTrace.load failed for', procId, e);
        document.getElementById('ctContainer').innerHTML =
            '<div class="empty-msg">Failed to load call tree for: ' + PA.esc(procId) + '</div>';
    }
};

/**
 * Load detail for a node (updates header + stats only, does NOT re-render call tree).
 * Used when clicking a child row in the Explore view. Debounced to prevent rapid-fire.
 */
PA._loadNodeDetailTimer = null;
PA._loadNodeDetailLast = null;
PA.loadNodeDetail = function(procId) {
    // Debounce rapid clicks on the same node, but allow back-navigation (A → B → A)
    // Only skip if this exact procId is already the current context
    if (PA.context.procId === procId && PA._loadNodeDetailLast === procId) return;
    PA._loadNodeDetailLast = procId;
    clearTimeout(PA._loadNodeDetailTimer);
    PA._loadNodeDetailTimer = setTimeout(async () => {
        try {
            const detail = await PA.api.getProcDetail(procId);
            if (detail) {
                PA.populateDetailHeader(detail);

                // Update shared context so Table Ops + Claude scope to this child
                PA.context.procId = procId;
                PA.context.procDetail = detail;
                PA.context.scopedTables = detail.tables || detail.tableOperations || [];
                if (detail.callTree) {
                    PA.context.callTreeNodeIds = PA._collectTreeNodeIds(detail.callTree);
                }

                // Re-render breadcrumbs
                PA._renderContextBreadcrumb('toBreadcrumb');
                PA._renderContextBreadcrumb('claudeBreadcrumb');

                // Re-apply scope to all data tabs
                if (PA.tableOps && PA.tableOps.applyScope) PA.tableOps.applyScope();
                if (PA.predicates && PA.predicates._loaded && PA.predicates.applyScope) PA.predicates.applyScope();

                // Also highlight in left panel sidebar
                document.querySelectorAll('.lp-item').forEach(el => el.classList.remove('active'));
                const item = document.querySelector(`.lp-item[data-id="${CSS.escape(procId)}"]`);
                if (item) item.classList.add('active');
            }
        } catch (e) {
            console.warn('[PA] loadNodeDetail failed for', procId, e);
        }
    }, 150);
};

/** Open source view — switches to Source tab and opens at the given line */
PA.openSource = function(sourceFile, lineNumber) {
    PA._skipSourceAutoLoad = true;
    PA.switchRightTab('source');
    PA.sourceView.open(sourceFile, lineNumber);
};

// ==================== CODE MODAL ====================
PA.codeModal = {
    history: [],  // stack of {procId, sourceFile, line}

    async open(procId) {
        try {
            const cleanId = procId && procId.includes('/') ? procId.substring(0, procId.indexOf('/')) : procId;
            const node = PA.codeModal._findNodeFromAllTrees(procId) || PA.codeModal._findNodeFromAllTrees(cleanId);
            let sourceFile = node ? (node.sourceFile || null) : null;
            let startLine = node ? (node.startLine || 0) : 0;
            const nodeName = node ? (node.name || '') : '';
            const nodeSchema = node ? (node.schemaName || '') : '';
            const nodePkg = node ? (node.packageName || '') : '';
            const procName = nodeName || (cleanId || '').split('.').pop() || '';

            // Resolve owner + objectName from the node's schema info — never guess
            let owner, objectName;
            if (sourceFile) {
                const sfParts = sourceFile.split('.');
                owner = sfParts[0] || '';
                objectName = sfParts.length >= 2 ? sfParts[1] : sfParts[0];
            } else if (nodeSchema) {
                owner = nodeSchema;
                objectName = nodePkg || procName;
            } else {
                const parts = (cleanId || '').split('.');
                if (parts.length >= 3) {
                    owner = parts[0];
                    objectName = parts[1];
                } else if (parts.length === 2) {
                    owner = (PA.analysisData && PA.analysisData.entrySchema) || parts[0];
                    objectName = parts[0];
                } else {
                    owner = (PA.analysisData && PA.analysisData.entrySchema) || '';
                    objectName = parts[0];
                }
            }

            console.log('[CodeModal] open:', procId, '→ owner:', owner, 'obj:', objectName, 'startLine:', startLine);

            // Source is already in the analysis sourceMap — no DB calls needed
            let data = null;
            if (owner && objectName) {
                data = await PA.api.getSource(owner, objectName);
            }

            if (!data || !data.content) {
                PA.toast('Source not available for: ' + procId, 'error');
                console.warn('[CodeModal] No source for:', procId, 'tried owner:', owner, 'obj:', objectName);
                return;
            }

            // Detect Oracle wrapped/encrypted source
            const firstLine = (data.content || '').split('\n')[0] || '';
            const isWrapped = /\bwrapped\b/i.test(firstLine);

            // If startLine is 0, search source for the procedure/function name
            if (!startLine && procName && !isWrapped) {
                startLine = PA.codeModal._findProcInSource(data.content, procName);
            }

            // Build breadcrumb parts
            const breadcrumbParts = [];
            if (nodeSchema || (data.owner || '')) breadcrumbParts.push(nodeSchema || data.owner);
            if (nodePkg || (data.objectName || '')) breadcrumbParts.push(nodePkg || data.objectName);
            if (procName) breadcrumbParts.push(procName);

            PA.codeModal.history.push({ procId, owner: data.owner, objectName: data.objectName, breadcrumbParts });
            PA.codeModal._historyFwd = []; // clear forward stack on new navigation

            const modal = document.getElementById('codeModal');
            modal.style.display = '';

            // Load proc detail for the sidebar (async, non-blocking)
            PA.codeModal._loadDetail(procId, node);

            // Update breadcrumb in modal header
            PA.codeModal.updateModalBreadcrumb();

            // Render info bar
            PA.codeModal._renderInfoBar(node, procId);

            // Show/hide Claude toggle
            PA.codeModal._updateViewToggle();

            const sf = (data.owner || owner) + '.' + (data.objectName || objectName);
            if (isWrapped) {
                const container = document.getElementById('codeModalSource');
                container.innerHTML = `<div style="padding:32px;text-align:center">
                    <div style="font-size:48px;margin-bottom:16px;opacity:0.4">&#128274;</div>
                    <div style="font-size:16px;font-weight:600;color:var(--text);margin-bottom:8px">Wrapped / Encrypted Source</div>
                    <div style="font-size:13px;color:var(--text-muted);max-width:500px;margin:0 auto">
                        This Oracle package (<b>${PA.esc(sf)}</b>) is wrapped (encrypted) and cannot be read.<br>
                        The source code was compiled with Oracle's <code>WRAP</code> utility and is not human-readable.
                    </div>
                    <div style="margin-top:16px;font-size:11px;color:var(--text-muted)">
                        To view the source, ask the package owner for the unwrapped <code>.pks</code>/<code>.pkb</code> files.
                    </div>
                </div>`;
            } else {
                PA.codeModal.renderSource(data.content, startLine || 0, sf);
            }
            PA.codeModal.updateBackBtn();
        } catch (err) {
            console.error('[CodeModal] Error opening:', procId, err);
            PA.toast('Error loading source: ' + (err.message || err), 'error');
        }
    },

    /** Find sourceFile from the call tree for a given procId */
    findSourceFile(procId) {
        if (!PA.currentDetail || !PA.currentDetail.callTree) return null;
        return PA.codeModal._findField(PA.currentDetail.callTree, procId, 'sourceFile');
    },

    /** Find startLine from the call tree for a given procId */
    findNodeStartLine(procId) {
        if (!PA.currentDetail || !PA.currentDetail.callTree) return 0;
        return PA.codeModal._findField(PA.currentDetail.callTree, procId, 'startLine') || 0;
    },

    /** Walk tree to find a field value for a matching node.
     *  Matches exact ID, baseId, or endsWith (e.g. "PKG.PROC" matches "SCHEMA.PKG.PROC") */
    _findField(tree, procId, field) {
        const upper = procId.toUpperCase();
        const treeId = (tree.id || '').toUpperCase();
        const treeBaseId = (tree.baseId || treeId).toUpperCase();
        if (treeId === upper || treeBaseId === upper || treeId.endsWith('.' + upper) || treeBaseId.endsWith('.' + upper)) {
            return tree[field] || null;
        }
        if (tree.children) {
            for (const child of tree.children) {
                const found = PA.codeModal._findField(child, procId, field);
                if (found) return found;
            }
        }
        return null;
    },

    /** Walk tree to find the full node object for a matching procId */
    _findNode(tree, procId) {
        const upper = procId.toUpperCase();
        const treeId = (tree.id || '').toUpperCase();
        const treeBaseId = (tree.baseId || treeId).toUpperCase();
        if (treeId === upper || treeBaseId === upper || treeId.endsWith('.' + upper) || treeBaseId.endsWith('.' + upper)) {
            return tree;
        }
        if (tree.children) {
            for (const child of tree.children) {
                const found = PA.codeModal._findNode(child, procId);
                if (found) return found;
            }
        }
        return null;
    },

    /** Search source text for PROCEDURE/FUNCTION name, return line number */
    _findProcInSource(content, procName) {
        if (!content || !procName) return 0;
        const lines = content.split('\n');
        // Strip overload suffix (e.g. PROC/3IN_2OUT → PROC) and dots (SCHEMA.PKG.PROC → PROC)
        let clean = procName.includes('/') ? procName.substring(0, procName.indexOf('/')) : procName;
        clean = clean.includes('.') ? clean.substring(clean.lastIndexOf('.') + 1) : clean;
        const upper = clean.toUpperCase();
        // Look for "PROCEDURE name" or "FUNCTION name" pattern
        const patterns = [
            new RegExp('\\b(PROCEDURE|FUNCTION)\\s+' + upper.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'i')
        ];
        for (let i = 0; i < lines.length; i++) {
            for (const pat of patterns) {
                if (pat.test(lines[i])) return i + 1;
            }
        }
        return 0;
    },

    /** Search call tree(s) for a node matching procId */
    _findNodeFromAllTrees(procId) {
        // Search in current detail call tree
        if (PA.currentDetail && PA.currentDetail.callTree) {
            const found = PA.codeModal._findNode(PA.currentDetail.callTree, procId);
            if (found) return found;
        }
        // Also search in callTrace treeData (might be different after drill-down)
        if (PA.callTrace && PA.callTrace.treeData) {
            const found = PA.codeModal._findNode(PA.callTrace.treeData, procId);
            if (found) return found;
        }
        return null;
    },

    /** Update breadcrumb bar in the modal header */
    updateModalBreadcrumb() {
        var titleEl = document.getElementById('codeModalTitle');
        if (!titleEl) return;
        var current = PA.codeModal.history[PA.codeModal.history.length - 1];
        if (!current) return;

        var parts = current.breadcrumbParts || [(current.owner || ''), (current.objectName || current.procId)];
        var html = '';
        for (var i = 0; i < parts.length; i++) {
            if (i > 0) html += '<span class="cm-bc-sep">&#9656;</span>';
            var isCurrent = (i === parts.length - 1);
            if (isCurrent) {
                html += '<span class="cm-bc-item cm-bc-active">' + PA.esc(parts[i]) + '</span>';
            } else {
                html += '<span class="cm-bc-item cm-bc-link">' + PA.esc(parts[i]) + '</span>';
            }
        }
        titleEl.innerHTML = html;
    },

    /** Open source directly by sourceFile (SCHEMA.PACKAGE) at a specific line */
    async openAtLine(sourceFile, lineNumber) {
        if (!sourceFile) {
            PA.toast('No source file info', 'error');
            return;
        }
        const sfParts = sourceFile.split('.');
        let owner = sfParts[0] || '';
        let objectName = sfParts.length >= 2 ? sfParts[1] : sfParts[0];

        // Source is already in the analysis sourceMap — no DB calls needed
        let data = null;
        if (owner && objectName) {
            data = await PA.api.getSource(owner, objectName);
        }

        if (!data || !data.content) {
            PA.toast('Source not available for: ' + sourceFile, 'error');
            return;
        }

        // Detect wrapped source
        const firstLine = (data.content || '').split('\n')[0] || '';
        const isWrapped = /\bwrapped\b/i.test(firstLine);

        const breadcrumbParts = [data.owner || owner, data.objectName || objectName];
        if (lineNumber > 0) breadcrumbParts.push('Line ' + lineNumber);
        PA.codeModal.history.push({ procId: sourceFile, owner: data.owner, objectName: data.objectName, breadcrumbParts });

        const modal = document.getElementById('codeModal');
        modal.style.display = '';
        PA.codeModal.updateModalBreadcrumb();
        const sf = (data.owner || owner) + '.' + (data.objectName || objectName);

        if (isWrapped) {
            const container = document.getElementById('codeModalSource');
            container.innerHTML = `<div style="padding:32px;text-align:center">
                <div style="font-size:48px;margin-bottom:16px;opacity:0.4">&#128274;</div>
                <div style="font-size:16px;font-weight:600;color:var(--text);margin-bottom:8px">Wrapped / Encrypted Source</div>
                <div style="font-size:13px;color:var(--text-muted);max-width:500px;margin:0 auto">
                    This Oracle package (<b>${PA.esc(sf)}</b>) is wrapped (encrypted) and cannot be read.
                </div>
            </div>`;
        } else {
            PA.codeModal.renderSource(data.content, lineNumber || 0, sf);
        }
        PA.codeModal.updateBackBtn();
    },

    renderSource(content, lineNumber, sourceFile) {
        const container = document.getElementById('codeModalSource');
        PA.sourceView.renderContentTo(container, content, lineNumber, sourceFile);
        PA.codeModal._lastContent = content;
        PA.codeModal.populateSidebar();
    },

    copySource() {
        const content = PA.codeModal._lastContent;
        if (!content) { PA.toast('No source to copy', 'error'); return; }
        navigator.clipboard.writeText(content).then(
            () => PA.toast('Source copied to clipboard', 'success'),
            () => PA.toast('Copy failed', 'error')
        );
    },

    /** Load detail from API and populate sidebar */
    async _loadDetail(procId, node) {
        try {
            const resp = await fetch('/api/plsql/analysis/detail/' + encodeURIComponent(procId));
            if (resp.ok) {
                PA.codeModal._detail = await resp.json();
            } else {
                // Use currentDetail as fallback
                PA.codeModal._detail = PA.currentDetail || {};
            }
        } catch(e) {
            PA.codeModal._detail = PA.currentDetail || {};
        }
        PA.codeModal.populateSidebar();
    },

    /** Render info bar: type badge, name, scope, lines */
    _renderInfoBar(node, procId) {
        var bar = document.getElementById('cmInfoBar');
        if (!bar) return;
        var detail = PA.codeModal._detail || PA.currentDetail || {};
        var unitType = (node && node.unitType) || detail.unitType || '';
        var callType = (node && node.callType) || detail.callType || '';
        var name = (node && node.name) || detail.name || (procId || '').split('.').pop() || '';
        var schema = (node && node.schemaName) || detail.schemaName || '';
        var pkg = (node && node.packageName) || detail.packageName || '';
        var startLine = (node && node.startLine) || detail.startLine || 0;
        var endLine = (node && node.endLine) || detail.endLine || 0;
        var fqn = [schema, pkg, name].filter(Boolean).join('.');
        var loc = (startLine > 0 && endLine > 0) ? (endLine - startLine + 1) : 0;

        var html = '';
        // Type badge
        if (unitType) {
            var typeCls = unitType === 'FUNCTION' ? 'type-func' : 'type-proc';
            html += '<span class="cm-info-badge ' + typeCls + '">' + PA.esc(unitType) + '</span>';
        }
        // Scope badge
        if (callType) {
            var scopeCls = callType === 'EXTERNAL' ? 'scope-ext' : callType === 'DYNAMIC' ? 'scope-dyn' : 'scope-int';
            html += '<span class="cm-info-badge ' + scopeCls + '">' + PA.esc(callType) + '</span>';
        }
        // Name
        html += '<span class="cm-info-name">' + PA.esc(name) + '</span>';
        // FQN
        if (fqn !== name) html += '<span class="cm-info-fqn">' + PA.esc(fqn) + '</span>';
        // Lines
        if (loc > 0) html += '<span class="cm-info-lines">' + loc + ' lines (L' + startLine + '-' + endLine + ')</span>';
        else if (startLine > 0) html += '<span class="cm-info-lines">L' + startLine + '</span>';

        bar.innerHTML = html;
    },

    /** Show/hide Claude AI toggle based on data availability */
    _updateViewToggle() {
        var toggle = document.getElementById('cmViewToggle');
        if (!toggle) return;
        var hasClaudeData = PA.claude && PA.claude.hasData;
        toggle.style.display = hasClaudeData ? '' : 'none';
        // Reset to static view
        PA.codeModal._currentView = 'static';
        document.querySelectorAll('.cm-vt-btn').forEach(function(b) { b.classList.toggle('active', b.dataset.cmv === 'static'); });
        var staticPane = document.getElementById('cmPaneStatic');
        var claudePane = document.getElementById('cmPaneClaude');
        if (staticPane) staticPane.classList.add('active');
        if (claudePane) claudePane.classList.remove('active');
    },

    /** Switch between Static and Claude AI view in the popup */
    setView(mode) {
        PA.codeModal._currentView = mode;
        document.querySelectorAll('.cm-vt-btn').forEach(function(b) { b.classList.toggle('active', b.dataset.cmv === mode); });
        var staticPane = document.getElementById('cmPaneStatic');
        var claudePane = document.getElementById('cmPaneClaude');
        if (mode === 'claude') {
            if (staticPane) staticPane.classList.remove('active');
            if (claudePane) claudePane.classList.add('active');
            PA.codeModal._renderClaudePane();
        } else {
            if (staticPane) staticPane.classList.add('active');
            if (claudePane) claudePane.classList.remove('active');
        }
    },

    /** Render Claude AI view for the current proc */
    _renderClaudePane() {
        var el = document.getElementById('cmClaudeContent');
        if (!el) return;
        var detail = PA.codeModal._detail || {};
        var tables = detail.nodeTables || detail.tables || [];
        if (!tables.length) {
            el.innerHTML = '<div class="empty-msg">No table operations for this procedure</div>';
            return;
        }
        // Show claude verification results for tables in this proc
        var result = PA.claude && PA.claude.result;
        if (!result || !result.tables) {
            el.innerHTML = '<div class="empty-msg">No Claude verification data available. Run verification first.</div>';
            return;
        }
        var html = '<div style="font-size:11px;font-weight:700;color:var(--text-muted);margin-bottom:8px">Claude AI Verification — Tables in this procedure</div>';
        for (var i = 0; i < tables.length; i++) {
            var t = tables[i];
            var tName = (t.tableName || '').toUpperCase();
            // Find in Claude results
            var claudeEntry = null;
            for (var j = 0; j < result.tables.length; j++) {
                if ((result.tables[j].tableName || '').toUpperCase() === tName) { claudeEntry = result.tables[j]; break; }
            }
            html += '<div style="border:1px solid var(--border);border-radius:6px;padding:8px 12px;margin-bottom:8px">';
            html += '<div style="font-weight:700;font-size:12px;font-family:var(--font-mono)">' + PA.esc(tName) + '</div>';
            html += '<div style="font-size:10px;color:var(--text-muted);margin-top:2px">Static ops: ' + PA.esc((t.operations || []).join(', ')) + '</div>';
            if (claudeEntry) {
                var status = claudeEntry.verificationStatus || 'UNVERIFIED';
                var statusColor = status === 'CONFIRMED' ? 'var(--badge-green)' : status === 'EXPANDED' ? 'var(--accent)' : 'var(--badge-yellow)';
                html += '<div style="margin-top:4px"><span style="font-size:10px;font-weight:700;color:' + statusColor + '">' + PA.esc(status) + '</span></div>';
                if (claudeEntry.claudeOperations) html += '<div style="font-size:10px;color:var(--text-muted)">Claude ops: ' + PA.esc(claudeEntry.claudeOperations.join(', ')) + '</div>';
                if (claudeEntry.notes) html += '<div style="font-size:10px;color:var(--text);margin-top:4px">' + PA.esc(claudeEntry.notes) + '</div>';
            } else {
                html += '<div style="font-size:10px;color:var(--text-muted);margin-top:4px">Not verified by Claude</div>';
            }
            html += '</div>';
        }
        el.innerHTML = html;
    },

    /** Populate right sidebar with parameters, variables, tables, calls, called by */
    populateSidebar() {
        var detail = PA.codeModal._detail || PA.currentDetail || {};

        // --- Parameters ---
        var paramEl = document.getElementById('cmParamList');
        var paramCountEl = document.getElementById('cmParamCount');
        var params = detail.parameters || [];
        if (paramCountEl) paramCountEl.textContent = params.length || '';
        if (paramEl) {
            var html = '';
            for (var i = 0; i < params.length; i++) {
                var p = params[i];
                html += '<div class="cm-sb-item" onclick="PA.codeModal.jumpToLine(' + (p.lineNumber || 0) + ')" title="' + PA.esc((p.mode||'') + ' ' + (p.name||'') + ' ' + (p.dataType||'')) + '">';
                if (p.mode) html += '<span class="cm-sb-mode">' + PA.esc(p.mode) + '</span>';
                html += '<span class="cm-sb-method">' + PA.esc(p.name || '?') + '</span>';
                html += '<span class="cm-sb-type"> ' + PA.esc(p.dataType || '') + '</span>';
                html += '</div>';
            }
            paramEl.innerHTML = html || '<div class="cm-sb-empty">None</div>';
        }

        // --- Variables ---
        var varEl = document.getElementById('cmVarList');
        var varCountEl = document.getElementById('cmVarCount');
        var vars = detail.variables || [];
        if (varCountEl) varCountEl.textContent = vars.length || '';
        if (varEl) {
            var html = '';
            var startLine = detail.startLine || 0;
            var globals = vars.filter(function(v) { return startLine > 0 && v.lineNumber && v.lineNumber < startLine; });
            var locals = vars.filter(function(v) { return !startLine || !v.lineNumber || v.lineNumber >= startLine; });
            if (globals.length > 0) {
                html += '<div style="font-size:8px;color:#585b70;padding:3px 12px;text-transform:uppercase;letter-spacing:0.5px">Package-Level</div>';
                for (var i = 0; i < globals.length; i++) {
                    var v = globals[i];
                    html += '<div class="cm-sb-item" onclick="PA.codeModal.jumpToLine(' + (v.lineNumber || 0) + ')">';
                    html += '<span class="cm-sb-field">' + PA.esc(v.name || '?') + '</span>';
                    html += '<span class="cm-sb-type"> ' + PA.esc(v.dataType || '') + '</span>';
                    if (v.constant) html += '<span class="cm-sb-const"> const</span>';
                    if (v.lineNumber) html += '<span class="cm-sb-line">:' + v.lineNumber + '</span>';
                    html += '</div>';
                }
            }
            if (locals.length > 0) {
                html += '<div style="font-size:8px;color:#585b70;padding:3px 12px;text-transform:uppercase;letter-spacing:0.5px">Local</div>';
                for (var i = 0; i < locals.length; i++) {
                    var v = locals[i];
                    html += '<div class="cm-sb-item" onclick="PA.codeModal.jumpToLine(' + (v.lineNumber || 0) + ')">';
                    html += '<span class="cm-sb-field">' + PA.esc(v.name || '?') + '</span>';
                    html += '<span class="cm-sb-type"> ' + PA.esc(v.dataType || '') + '</span>';
                    if (v.constant) html += '<span class="cm-sb-const"> const</span>';
                    if (v.lineNumber) html += '<span class="cm-sb-line">:' + v.lineNumber + '</span>';
                    html += '</div>';
                }
            }
            varEl.innerHTML = html || '<div class="cm-sb-empty">None</div>';
        }

        // --- Tables ---
        var tableEl = document.getElementById('cmTableList');
        var tableCountEl = document.getElementById('cmTableCount');
        var tables = detail.nodeTables || detail.tables || [];
        if (tableCountEl) tableCountEl.textContent = tables.length || '';
        if (tableEl) {
            var html = '';
            for (var i = 0; i < tables.length; i++) {
                var t = tables[i];
                html += '<div class="cm-sb-item" onclick="PA.tableOps.focusTable(\'' + PA.escJs(t.tableName || '') + '\')" title="' + PA.esc(t.tableName || '') + '">';
                html += '<span class="cm-sb-field">' + PA.esc(t.tableName || '?') + '</span>';
                var ops = t.operations || [];
                for (var j = 0; j < ops.length; j++) {
                    var opLetter = (typeof ops[j] === 'string' ? ops[j] : String(ops[j])).charAt(0);
                    html += '<span class="cm-sb-op ' + PA.esc(opLetter) + '">' + PA.esc(opLetter) + '</span>';
                }
                html += '</div>';
            }
            tableEl.innerHTML = html || '<div class="cm-sb-empty">None</div>';
        }

        // --- Calls Made ---
        var callsEl = document.getElementById('cmCallsList');
        var callsCountEl = document.getElementById('cmCallsCount');
        var calls = detail.calls || [];
        if (callsCountEl) callsCountEl.textContent = calls.length || '';
        if (callsEl) {
            var html = '';
            for (var i = 0; i < calls.length; i++) {
                var c = calls[i];
                var badge = c.callType === 'EXTERNAL' ? '<span class="cm-sb-badge ext">EXT</span>' : '<span class="cm-sb-badge int">INT</span>';
                html += '<div class="cm-sb-item" onclick="PA.codeModal.open(\'' + PA.escJs(c.targetId || c.name) + '\')">';
                html += badge + ' <span class="cm-sb-method">' + PA.esc(c.name || c.targetId || '?') + '</span>';
                if (c.lineNumber) html += '<span class="cm-sb-line">:' + c.lineNumber + '</span>';
                html += '</div>';
            }
            callsEl.innerHTML = html || '<div class="cm-sb-empty">None</div>';
        }

        // --- Called By ---
        var calledByEl = document.getElementById('cmCalledByList');
        var calledByCountEl = document.getElementById('cmCalledByCount');
        var calledBy = detail.calledBy || [];
        if (calledByCountEl) calledByCountEl.textContent = calledBy.length || '';
        if (calledByEl) {
            var html = '';
            for (var i = 0; i < calledBy.length; i++) {
                var c = calledBy[i];
                html += '<div class="cm-sb-item" onclick="PA.codeModal.open(\'' + PA.escJs(c.callerId || c.name) + '\')">';
                html += '<span class="cm-sb-badge caller">&larr;</span> <span class="cm-sb-method">' + PA.esc(c.name || c.callerId || '?') + '</span>';
                if (c.lineNumber) html += '<span class="cm-sb-line">:' + c.lineNumber + '</span>';
                html += '</div>';
            }
            calledByEl.innerHTML = html || '<div class="cm-sb-empty">None</div>';
        }

        // --- Package Symbols ---
        PA.codeModal._populatePackageSymbols(detail);

        // Also update info bar now that detail is loaded
        var current = PA.codeModal.history[PA.codeModal.history.length - 1];
        if (current) PA.codeModal._renderInfoBar(null, current.procId);
    },

    async _populatePackageSymbols(detail) {
        var symEl = document.getElementById('cmSymbolsList');
        var symCountEl = document.getElementById('cmSymbolsCount');
        if (!symEl) return;

        var pkg = (detail.packageName || '').replace(/"/g, '');
        var schema = (detail.schemaName || '').replace(/"/g, '');
        if (!pkg) {
            if (symCountEl) symCountEl.textContent = '';
            symEl.innerHTML = '<div class="cm-sb-empty">Not in a package</div>';
            return;
        }

        try {
            var procs = PA._cachedProcedures || await PA.api.getProcedures();
            PA._cachedProcedures = procs;

            var members = procs.filter(function(p) {
                var pPkg = (p.packageName || '').replace(/"/g, '').toUpperCase();
                var pSchema = (p.schemaName || '').replace(/"/g, '').toUpperCase();
                return pPkg === pkg.toUpperCase() && (!schema || pSchema === schema.toUpperCase());
            });
            members.sort(function(a, b) {
                var ta = (a.unitType || '').toUpperCase();
                var tb = (b.unitType || '').toUpperCase();
                if (ta !== tb) return ta < tb ? -1 : 1;
                return (a.name || '').localeCompare(b.name || '');
            });

            var fnCount = members.filter(function(m) { return (m.unitType||'').toUpperCase() === 'FUNCTION'; }).length;
            var prCount = members.length - fnCount;
            if (symCountEl) symCountEl.textContent = members.length ? (prCount + ' PR / ' + fnCount + ' FN') : '';
            var html = '';
            html += '<div style="font-size:9px;color:#6c7086;padding:2px 12px;border-bottom:1px solid var(--border);margin-bottom:2px">' + PA.esc(schema + '.' + pkg) + '</div>';
            var currentName = (detail.name || '').replace(/"/g, '').toUpperCase();
            for (var i = 0; i < members.length; i++) {
                var m = members[i];
                var mName = (m.name || '').replace(/"/g, '');
                var isActive = mName.toUpperCase() === currentName;
                var mType = (m.unitType || 'PROCEDURE').toUpperCase();
                var mScope = (m.callType || '').toUpperCase();
                var loc = (m.startLine && m.endLine) ? (m.endLine - m.startLine + 1) : 0;
                var tooltip = mType + ' ' + PA.esc(schema + '.' + pkg + '.' + mName);
                if (mScope) tooltip += '\\nScope: ' + mScope;
                if (m.startLine) tooltip += '\\nLines: ' + m.startLine + '-' + (m.endLine || '?') + (loc ? ' (' + loc + ' LOC)' : '');
                var typeBadge = mType === 'FUNCTION'
                    ? '<span class="cm-sb-badge" style="background:rgba(34,197,94,0.15);color:#22c55e;font-size:9px">FN</span>'
                    : '<span class="cm-sb-badge" style="background:rgba(99,102,241,0.15);color:#818cf8;font-size:9px">PR</span>';
                html += '<div class="cm-sb-item' + (isActive ? ' active' : '') + '" onclick="PA.codeModal.open(\'' + PA.escJs(m.id || '') + '\')" title="' + tooltip + '">';
                html += typeBadge + ' <span class="cm-sb-method">' + PA.esc(mName) + '</span>';
                if (m.startLine) html += '<span class="cm-sb-line">:' + m.startLine + '</span>';
                html += '</div>';
            }
            symEl.innerHTML = html || '<div class="cm-sb-empty">No members found</div>';
        } catch (e) {
            symEl.innerHTML = '<div class="cm-sb-empty">Failed to load</div>';
        }
    },

    /** Jump to a specific line in the modal source */
    jumpToLine(lineNumber) {
        if (!lineNumber) return;
        var container = document.getElementById('codeModalSource');
        if (!container) return;
        // Lines use dynamic ID prefix like "src-ln-{timestamp}-{lineNumber}"
        // Find by matching the ID ending with "-{lineNumber}"
        var allLines = container.querySelectorAll('.src-line');
        var lineEl = null;
        var suffix = '-' + lineNumber;
        for (var i = 0; i < allLines.length; i++) {
            if (allLines[i].id && allLines[i].id.endsWith(suffix)) {
                lineEl = allLines[i];
                break;
            }
        }
        // Fallback: use nth-child (1-indexed, inside .source-code container)
        if (!lineEl) {
            var sourceCode = container.querySelector('.source-code');
            if (sourceCode && sourceCode.children[lineNumber - 1]) {
                lineEl = sourceCode.children[lineNumber - 1];
            }
        }
        if (lineEl) {
            container.querySelectorAll('.src-line.cm-jump-highlight').forEach(function(el) { el.classList.remove('cm-jump-highlight'); });
            lineEl.classList.add('cm-jump-highlight');
            lineEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    },

    // ==================== SEARCH / FIND ====================
    _searchMatches: [],
    _searchCurrent: -1,

    toggleSearch() {
        const bar = document.getElementById('cmSearchBar');
        if (!bar) return;
        if (bar.style.display === 'none') {
            bar.style.display = '';
            document.getElementById('cmSearchInput').focus();
        } else {
            PA.codeModal.closeSearch();
        }
    },

    search(query) {
        PA.codeModal._clearSearchHighlights();
        PA.codeModal._searchMatches = [];
        PA.codeModal._searchCurrent = -1;
        const countEl = document.getElementById('cmSearchCount');

        if (!query || query.length < 2) {
            if (countEl) countEl.textContent = '0';
            return;
        }

        const container = document.getElementById('codeModalSource');
        if (!container) return;

        // Find all matching text nodes in source lines
        const lines = container.querySelectorAll('.src-line');
        const queryLower = query.toLowerCase();
        var matches = [];

        lines.forEach(function(line) {
            const codeEl = line.querySelector('.src-code') || line;
            const text = codeEl.textContent || '';
            const textLower = text.toLowerCase();
            var idx = 0;
            while ((idx = textLower.indexOf(queryLower, idx)) !== -1) {
                matches.push({ line: line, element: codeEl, index: idx, length: query.length });
                idx += query.length;
            }
        });

        PA.codeModal._searchMatches = matches;
        if (countEl) countEl.textContent = matches.length;

        // Highlight all matches using DOM range
        PA.codeModal._highlightMatches(matches);

        // Jump to first match
        if (matches.length > 0) {
            PA.codeModal._searchCurrent = 0;
            PA.codeModal._activateMatch(0);
        }
    },

    _highlightMatches(matches) {
        // Add cm-has-match class to lines that contain matches for visual indicator
        for (var i = 0; i < matches.length; i++) {
            matches[i].line.classList.add('cm-has-match');
        }
    },

    _activateMatch(index) {
        var matches = PA.codeModal._searchMatches;
        if (index < 0 || index >= matches.length) return;

        // Remove previous active
        var container = document.getElementById('codeModalSource');
        if (container) {
            container.querySelectorAll('.src-line.cm-match-active').forEach(function(el) { el.classList.remove('cm-match-active'); });
        }

        var match = matches[index];
        match.line.classList.add('cm-match-active');
        match.line.scrollIntoView({ behavior: 'smooth', block: 'center' });

        var countEl = document.getElementById('cmSearchCount');
        if (countEl) countEl.textContent = (index + 1) + '/' + matches.length;
    },

    _clearSearchHighlights() {
        var container = document.getElementById('codeModalSource');
        if (!container) return;
        container.querySelectorAll('.cm-has-match').forEach(function(el) { el.classList.remove('cm-has-match'); });
        container.querySelectorAll('.cm-match-active').forEach(function(el) { el.classList.remove('cm-match-active'); });
    },

    searchNext() {
        if (PA.codeModal._searchMatches.length === 0) return;
        PA.codeModal._searchCurrent = (PA.codeModal._searchCurrent + 1) % PA.codeModal._searchMatches.length;
        PA.codeModal._activateMatch(PA.codeModal._searchCurrent);
    },

    searchPrev() {
        if (PA.codeModal._searchMatches.length === 0) return;
        PA.codeModal._searchCurrent = (PA.codeModal._searchCurrent - 1 + PA.codeModal._searchMatches.length) % PA.codeModal._searchMatches.length;
        PA.codeModal._activateMatch(PA.codeModal._searchCurrent);
    },

    closeSearch() {
        var bar = document.getElementById('cmSearchBar');
        if (bar) bar.style.display = 'none';
        var input = document.getElementById('cmSearchInput');
        if (input) input.value = '';
        PA.codeModal._clearSearchHighlights();
        PA.codeModal._searchMatches = [];
        PA.codeModal._searchCurrent = -1;
        var countEl = document.getElementById('cmSearchCount');
        if (countEl) countEl.textContent = '0';
    },

    searchKeydown(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            if (e.shiftKey) PA.codeModal.searchPrev();
            else PA.codeModal.searchNext();
        } else if (e.key === 'Escape') {
            PA.codeModal.closeSearch();
        }
    },

    _historyFwd: [],

    back() {
        if (PA.codeModal.history.length <= 1) return;
        var current = PA.codeModal.history.pop(); // remove current
        PA.codeModal._historyFwd.push(current); // save for forward
        var prev = PA.codeModal.history[PA.codeModal.history.length - 1];
        PA.codeModal.history.pop(); // will be re-added by open()
        PA.codeModal.open(prev.procId);
    },

    forward() {
        if (!PA.codeModal._historyFwd || PA.codeModal._historyFwd.length === 0) return;
        var next = PA.codeModal._historyFwd.pop();
        PA.codeModal.open(next.procId);
    },

    close() {
        document.getElementById('codeModal').style.display = 'none';
        document.querySelector('.cm-panel')?.classList.remove('cm-fullscreen');
        PA.codeModal.history = [];
        PA.codeModal._historyFwd = [];
        PA.codeModal.closeSearch();
        document.querySelectorAll('.cm-sb-filter').forEach(f => { f.value = ''; });
    },

    toggleFullscreen() {
        const panel = document.querySelector('.cm-panel');
        if (!panel) return;
        panel.classList.toggle('cm-fullscreen');
        const btn = document.getElementById('cmFullscreenBtn');
        if (btn) btn.title = panel.classList.contains('cm-fullscreen') ? 'Exit fullscreen' : 'Toggle fullscreen';
    },

    filterSection(input) {
        const q = (input.value || '').trim().toLowerCase();
        const section = input.closest('.cm-sb-section');
        if (!section) return;
        section.querySelectorAll('.cm-sb-item').forEach(item => {
            if (!q) { item.style.display = ''; return; }
            const txt = (item.textContent || '').toLowerCase();
            item.style.display = txt.includes(q) ? '' : 'none';
        });
    },

    updateBackBtn() {
        var backBtn = document.getElementById('codeModalBack');
        var fwdBtn = document.getElementById('codeModalFwd');
        var stackEl = document.getElementById('cmStackCount');
        if (backBtn) backBtn.disabled = PA.codeModal.history.length <= 1;
        if (fwdBtn) fwdBtn.disabled = !PA.codeModal._historyFwd || PA.codeModal._historyFwd.length === 0;
        if (stackEl) {
            var total = PA.codeModal.history.length + (PA.codeModal._historyFwd ? PA.codeModal._historyFwd.length : 0);
            stackEl.textContent = total > 1 ? total + ' levels' : '';
        }
    }
};

// Ctrl+F handler for code modal search
document.addEventListener('keydown', function(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
        var modal = document.getElementById('codeModal');
        if (modal && modal.style.display !== 'none') {
            e.preventDefault();
            PA.codeModal.toggleSearch();
        }
    }
});

// ==================== FLAT TRACE (Dynatrace-style) ====================
PA.trace = {
    flatNodes: [],

    renderFromTree(tree) {
        PA.trace.flatNodes = [];
        PA.trace.flatten(tree, 0);
        const searchBox = document.getElementById('traceSearch');
        if (searchBox) searchBox.value = '';
        PA.trace.render();
    },

    flatten(node, depth) {
        PA.trace.flatNodes.push({ ...node, _depth: depth });
        if (node.children && !node.circular) {
            for (const child of node.children) {
                PA.trace.flatten(child, depth + 1);
            }
        }
    },

    render() {
        const container = document.getElementById('traceContainer');
        const nodes = PA.trace.flatNodes;
        if (!nodes || nodes.length === 0) {
            container.innerHTML = '<div class="empty-msg">No trace data</div>';
            return;
        }

        const maxDepth = Math.max(...nodes.map(n => n._depth), 1);
        let html = '';
        for (let i = 0; i < nodes.length; i++) {
            const n = nodes[i];
            const schema = n.schemaName || '';
            const pkg = n.packageName || '';
            const name = n.name || n.id || '?';
            const fullName = [schema, pkg, name].filter(Boolean).join('.');
            const colorObj = PA.getSchemaColor(schema);
            const barWidth = Math.max(20, ((n._depth + 1) / (maxDepth + 1)) * 200);
            const callType = n.callType || '';
            const lineNum = n.callLineNumber || n.startLine || 0;

            html += `<div class="trace-row" onclick="PA.trace.onRowClick(${i}, event)">`;
            html += `<span class="ct-depth-badge" title="Depth level ${n._depth}">L${n._depth}</span>`;
            html += `<span class="trace-step" style="color:${colorObj.fg}">${i + 1}</span>`;
            // Depth bar (visual indicator)
            html += `<span class="trace-bar" style="width:${barWidth}px;background:${colorObj.bg};margin-left:${n._depth * 8}px"></span>`;
            // Schema badge
            if (schema) {
                html += `<span class="ct-schema-badge" style="background:${colorObj.bg};color:${colorObj.fg}">${PA.esc(schema)}</span>`;
            }
            // Placeholder/encrypted indicator
            if (n.placeholder) {
                html += `<span class="ct-lock" title="Source not available — wrapped/encrypted or not in analyzed sources">&#128274;</span>`;
            }
            // Name — rich tooltip
            const nType = n.unitType || 'PROCEDURE';
            const nLoc = (n.startLine && n.endLine) ? (n.endLine - n.startLine + 1) : 0;
            let nTip = nType + (pkg ? ' in Package ' + pkg : '') + '\n' + fullName;
            if (callType) nTip += '\nScope: ' + callType;
            if (n.startLine) nTip += '\nLines: ' + n.startLine + '-' + (n.endLine || '?') + (nLoc ? ' (' + nLoc + ' LOC)' : '');
            html += `<span class="trace-name" title="${PA.escAttr(nTip)}" onclick="PA.codeModal.open('${PA.escJs(n.id)}'); event.stopPropagation();">${PA.esc(fullName)}</span>`;
            // Right info
            html += `<span class="trace-info">`;
            if (lineNum > 0) {
                const sf = PA.escJs(n.sourceFile || (schema && pkg ? schema + '.' + pkg : ''));
                const endL = n.endLine || 0;
                const lineLabel = (endL > 0 && endL !== lineNum) ? `L${lineNum}-${endL}` : `L${lineNum}`;
                html += `<span class="ct-line" onclick="PA.codeModal.openAtLine('${sf}', ${lineNum}); event.stopPropagation();">${lineLabel}</span>`;
            }
            if (n.circular) {
                html += `<span class="ct-call-badge CIRCULAR">CIRCULAR</span>`;
            } else if (callType) {
                html += `<span class="ct-call-badge ${PA.escAttr(callType)}">${PA.esc(callType)}</span>`;
            }
            html += `</span>`;
            html += `</div>`;
        }
        container.innerHTML = html;
    },

    onRowClick(index, event) {
        document.querySelectorAll('.trace-row.active').forEach(el => el.classList.remove('active'));
        event.currentTarget.classList.add('active');
    },

    search(query) {
        const q = (query || '').trim().toLowerCase();
        const rows = document.querySelectorAll('#traceContainer .trace-row');
        if (!q) {
            rows.forEach(r => { r.style.display = ''; r.classList.remove('ct-search-hit'); });
            return;
        }
        rows.forEach(r => {
            const name = (r.querySelector('.trace-name')?.textContent || '').toLowerCase();
            const schema = (r.querySelector('.ct-schema-badge')?.textContent || '').toLowerCase();
            const hit = name.includes(q) || schema.includes(q);
            r.style.display = hit ? '' : 'none';
            r.classList.toggle('ct-search-hit', hit);
        });
    },

    expandAll() { /* flat view — no-op */ },
    collapseAll() { /* flat view — no-op */ }
};

// ==================== SCHEMA COLORS HELPER ====================
PA.getSchemaColor = function(schema) {
    const SCHEMA_COLORS = {
        'CUSTOMER':   { bg: '#dbeafe', fg: '#1d4ed8' },
        'CUSTOMER_I': { bg: '#ccfbf1', fg: '#0f766e' },
        'OPUS_CORE':  { bg: '#dcfce7', fg: '#15803d' },
        'POLICY':     { bg: '#f3e8ff', fg: '#7e22ce' },
        'CLAIMS':     { bg: '#fee2e2', fg: '#b91c1c' },
        'ACCOUNTING': { bg: '#fef3c7', fg: '#a16207' },
        'DEFAULT':    { bg: '#f1f5f9', fg: '#475569' }
    };
    if (!schema) return SCHEMA_COLORS.DEFAULT;
    return SCHEMA_COLORS[schema.toUpperCase()] || SCHEMA_COLORS.DEFAULT;
};

/** Global search */
PA.globalSearch = async function(query) {
    if (query.length < 2) {
        document.querySelectorAll('.left-panel-list .lp-item').forEach(item => { item.style.display = ''; });
        return;
    }
    const q = query.toLowerCase();
    document.querySelectorAll('.left-panel-list .lp-item').forEach(item => {
        const txt = (item.dataset.filter || '').toLowerCase();
        item.style.display = txt.includes(q) ? '' : 'none';
    });
};

// ==================== DB BROWSE MODAL ====================
PA._browseData = [];
PA._browseAll = [];

PA.openBrowse = function() {
    const modal = document.getElementById('browseModal');
    if (modal) modal.style.display = 'flex';
    const sel = document.getElementById('browseSchema');
    if (!sel || sel.options.length > 1) return;
    const schemas = (PA.currentAnalysis && PA.currentAnalysis.schemas) || [];
    if (schemas.length === 0 && PA.currentAnalysis) {
        const procs = PA.currentAnalysis.procedures || [];
        const s = new Set();
        for (const p of procs) { if (p.schema) s.add(p.schema); }
        schemas.push(...s);
    }
    sel.innerHTML = '<option value="">-- Select Schema --</option>';
    for (const s of schemas) {
        sel.innerHTML += `<option value="${s}">${s}</option>`;
    }
};

PA.closeBrowse = function() {
    const modal = document.getElementById('browseModal');
    if (modal) modal.style.display = 'none';
};

PA.loadBrowseObjects = async function() {
    const schema = document.getElementById('browseSchema')?.value;
    const list = document.getElementById('browseList');
    if (!schema) { list.innerHTML = '<div class="empty-msg">Select a schema to browse</div>'; return; }
    list.innerHTML = '<div class="empty-msg">Loading...</div>';
    try {
        const res = await fetch(`/api/plsql/db/objects?schema=${encodeURIComponent(schema)}`);
        PA._browseAll = res.ok ? await res.json() : [];
        PA.filterBrowse();
    } catch (e) {
        list.innerHTML = '<div class="empty-msg">Failed to load objects</div>';
    }
};

PA.filterBrowse = function() {
    const q = (document.getElementById('browseFilter')?.value || '').toUpperCase();
    const list = document.getElementById('browseList');
    const items = q ? PA._browseAll.filter(o => (o.objectName || o.name || '').toUpperCase().includes(q)) : PA._browseAll;
    if (items.length === 0) { list.innerHTML = '<div class="empty-msg">No objects found</div>'; return; }
    const esc = PA.escHtml || (s => { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; });
    let html = '<table style="width:100%;font-size:12px;border-collapse:collapse"><thead><tr style="background:var(--bg-tertiary);color:var(--text-muted);font-size:10px;text-transform:uppercase">';
    html += '<th style="padding:6px 10px;text-align:left">Name</th><th style="padding:6px 10px;text-align:left">Type</th></tr></thead><tbody>';
    for (const o of items.slice(0, 200)) {
        const name = o.objectName || o.name || '';
        const type = o.objectType || o.type || '';
        html += `<tr style="border-top:1px solid var(--border-color);cursor:pointer" onclick="PA.closeBrowse(); PA.showProcedure('${PA.escJs ? PA.escJs(name) : name}')">`;
        html += `<td style="padding:5px 10px;font-family:var(--font-mono);font-weight:600">${esc(name)}</td>`;
        html += `<td style="padding:5px 10px;color:var(--text-muted)">${esc(type)}</td></tr>`;
    }
    html += '</tbody></table>';
    if (items.length > 200) html += `<div style="padding:8px;text-align:center;color:var(--text-muted);font-size:11px">${items.length - 200} more items...</div>`;
    list.innerHTML = html;
};

// ==================== REFERENCES (Calls Made / Called By) ====================
PA.refs = {
    currentProcId: null,
    currentTab: 'calls',
    callsItems: [],
    callersItems: [],

    switchTab(tab) {
        PA.refs.currentTab = tab;
        document.querySelectorAll('.refs-tab').forEach(t => t.classList.remove('active'));
        document.querySelector(`.refs-tab[data-ref="${tab}"]`)?.classList.add('active');
        PA.refs._rebuildTable();
    },

    async load(procId) {
        PA.refs.currentProcId = procId;
        PA.refs.callsItems = [];
        PA.refs.callersItems = [];
        const container = document.getElementById('refsContainer');
        container.innerHTML = '<div class="empty-msg">Loading references...</div>';

        const [calls, callers] = await Promise.all([
            PA.api.getCallTree(procId),
            PA.api.getCallerTree(procId)
        ]);

        if (calls && calls.children) PA.refs.callsItems = PA.refs._dedup(calls.children);
        if (callers && callers.children) PA.refs.callersItems = PA.refs._dedup(callers.children);

        PA.refs._populateSchemaFilter();
        PA.refs._rebuildTable();
    },

    _dedup(children) {
        const map = new Map();
        for (const child of children) {
            const id = (child.id || '').toUpperCase();
            if (!id) continue;
            if (map.has(id)) {
                const e = map.get(id);
                e.count++;
                if (child.callLineNumber) e.lines.push(child.callLineNumber);
            } else {
                map.set(id, { node: child, count: 1, lines: child.callLineNumber ? [child.callLineNumber] : [] });
            }
        }
        return [...map.values()];
    },

    _populateSchemaFilter() {
        const all = [...PA.refs.callsItems, ...PA.refs.callersItems];
        const schemas = new Set();
        for (const item of all) {
            const s = (item.node.schemaName || '').toUpperCase();
            if (s) schemas.add(s);
            if (!item.node.schemaName && item.node.packageName) schemas.add(item.node.packageName.toUpperCase());
        }
        const sel = document.getElementById('refSchemaFilter');
        const cur = sel.value;
        sel.innerHTML = '<option value="">All Schemas</option>';
        for (const s of [...schemas].sort()) {
            sel.innerHTML += `<option value="${PA.escAttr(s)}">${PA.esc(s)}</option>`;
        }
        sel.value = cur;
    },

    /** Rebuild table shell and framework when data or tab changes */
    _rebuildTable() {
        const items = PA.refs.currentTab === 'calls' ? PA.refs.callsItems : PA.refs.callersItems;
        const container = document.getElementById('refsContainer');

        // Build table shell with sortable headers
        let html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'refs\',0)">Owner / Schema</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'refs\',1)">Package</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'refs\',2)">Name</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'refs\',3)">Type</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'refs\',4)">Scope</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'refs\',5)">Ref Count</th>';
        html += '</tr></thead><tbody id="refs-tbody"></tbody></table>';
        container.innerHTML = html;

        // Apply dropdown filters as extraFilter
        const extraFilter = (i) => {
            const schemaF = (document.getElementById('refSchemaFilter')?.value || '').toUpperCase();
            const typeF = (document.getElementById('refTypeFilter')?.value || '').toUpperCase();
            const scopeF = (document.getElementById('refScopeFilter')?.value || '').toUpperCase();
            if (schemaF) {
                const s = (i.node.schemaName || '').toUpperCase();
                const p = (i.node.packageName || '').toUpperCase();
                if (s !== schemaF && !((!s) && p === schemaF)) return false;
            }
            if (typeF && (i.node.unitType || '').toUpperCase() !== typeF) return false;
            if (scopeF && (i.node.callType || 'EXTERNAL').toUpperCase() !== scopeF) return false;
            return true;
        };

        PA.tf.init('refs', items, 50, PA.refs._renderRow, {
            sortKeys: {
                0: { fn: i => (i.node.schemaName || '').toUpperCase() },
                1: { fn: i => (i.node.packageName || '').toUpperCase() },
                2: { fn: i => (i.node.name || i.node.id || '').toUpperCase() },
                3: { fn: i => (i.node.unitType || '').toUpperCase() },
                4: { fn: i => (i.node.callType || 'EXTERNAL').toUpperCase() },
                5: { fn: i => i.count }
            },
            renderDetail: PA.refs._renderDetail,
            searchFn: (i, q) => (i.node.id || '').toUpperCase().includes(q) ||
                                (i.node.name || '').toUpperCase().includes(q) ||
                                (i.node.packageName || '').toUpperCase().includes(q) ||
                                (i.node.schemaName || '').toUpperCase().includes(q),
            extraFilter,
            onFilter: PA.refs._updateCounts
        });

        // Default sort: internal first, then count desc
        const s = PA.tf.state('refs');
        if (s) { s.sortCol = 5; s.sortDir = 'desc'; }

        PA.tf.filter('refs');

        setTimeout(() => {
            PA.tf.initColFilters('refs', {
                0: { label: 'Schema', valueFn: i => i.node.schemaName || '-' },
                3: { label: 'Type', valueFn: i => i.node.unitType || 'PROCEDURE' },
                4: { label: 'Scope', valueFn: i => i.node.callType || 'EXTERNAL' }
            });
            PA.tf._updateSortIndicators('refs');
        }, 0);
    },

    clearFilters() {
        document.getElementById('refSearch').value = '';
        document.getElementById('refSchemaFilter').value = '';
        document.getElementById('refTypeFilter').value = '';
        document.getElementById('refScopeFilter').value = '';
        PA.tf.setSearch('refs', '');
        const s = PA.tf.state('refs');
        if (s) { s.colFilters = {}; }
        PA.tf.filter('refs');
        PA.tf._cfUpdateIcons('refs');
    },

    filter() {
        PA.tf.setSearch('refs', document.getElementById('refSearch')?.value || '');
        PA.tf.filter('refs');
    },

    _updateCounts() {
        const items = PA.refs.currentTab === 'calls' ? PA.refs.callsItems : PA.refs.callersItems;
        const s = PA.tf.state('refs');
        const total = items.length;
        const shown = s ? s.filtered.length : total;
        let intC = 0, extC = 0;
        for (const i of items) {
            if (i.node.callType === 'INTERNAL') intC++; else extC++;
        }
        document.getElementById('refsTotalCount').textContent = total + ' refs';
        const fc = document.getElementById('refsFilteredCount');
        if (shown < total) { fc.textContent = shown + ' shown'; fc.style.display = ''; }
        else { fc.style.display = 'none'; }
        document.getElementById('refsInternalCount').textContent = intC + ' int';
        document.getElementById('refsExternalCount').textContent = extC + ' ext';
    },

    _renderRow(item, idx, esc) {
        const n = item.node;
        const schema = n.schemaName || '';
        const pkg = n.packageName || '';
        const name = n.name || n.id || '?';
        const unitType = n.unitType || 'PROCEDURE';
        const callType = n.callType || 'EXTERNAL';
        const colorObj = PA.getSchemaColor(schema);
        const typeBadge = unitType === 'FUNCTION'
            ? '<span class="lp-icon F" style="display:inline-flex;width:18px;height:18px;font-size:9px">F</span>'
            : '<span class="lp-icon P" style="display:inline-flex;width:18px;height:18px;font-size:9px">P</span>';
        const scopeBadge = callType === 'INTERNAL'
            ? '<span class="scope-badge int">INTERNAL</span>'
            : '<span class="scope-badge ext">EXTERNAL</span>';

        let html = `<tr class="to-row" onclick="PA.tf.toggleDetail('refs',${idx})">`;
        if (schema) {
            html += `<td><span class="ct-schema-badge" style="background:${colorObj.bg};color:${colorObj.fg}">${esc(schema)}</span></td>`;
        } else {
            html += `<td style="color:var(--text-muted);font-size:11px">-</td>`;
        }
        html += `<td style="font-weight:600">${esc(pkg || '-')}</td>`;
        html += `<td><span class="to-detail-proc" onclick="event.stopPropagation(); PA.codeModal.open('${PA.escJs(n.id)}')">${esc(name)}</span></td>`;
        html += `<td>${typeBadge} <span style="font-size:11px">${esc(unitType)}</span></td>`;
        html += `<td>${scopeBadge}</td>`;
        html += `<td style="font-weight:700;color:var(--accent)">${item.count}</td>`;
        html += `</tr>`;
        return html;
    },

    _renderDetail(item, idx, esc) {
        const n = item.node;
        const lines = [...new Set(item.lines)].sort((a, b) => a - b);
        const schema = n.schemaName || '';
        const pkg = n.packageName || '';
        const name = n.name || '';
        const fullName = [schema, pkg, name].filter(Boolean).join('.');
        const sf = n.sourceFile || '';

        let html = '<div class="to-detail">';
        html += '<div class="to-detail-section">';
        html += '<div class="to-detail-section-title">Full Path</div>';
        html += `<div class="to-detail-item" style="font-family:var(--font-mono);font-size:12px;color:var(--text)">`;
        if (schema) html += `<span style="color:var(--text-muted)">${esc(schema)}</span><span style="color:var(--text-muted);margin:0 2px">.</span>`;
        if (pkg) html += `<span style="font-weight:600">${esc(pkg)}</span><span style="color:var(--text-muted);margin:0 2px">.</span>`;
        html += `<span style="color:var(--accent);font-weight:700">${esc(name)}</span>`;
        html += `</div></div>`;

        if (lines.length > 0) {
            html += '<div class="to-detail-section">';
            html += `<div class="to-detail-section-title">Call Locations (${lines.length})</div>`;
            for (const ln of lines) {
                html += '<div class="to-detail-item">';
                html += `<span class="to-detail-line" onclick="event.stopPropagation(); PA.codeModal.openAtLine('${PA.escJs(sf || fullName)}', ${ln})">Line ${ln}</span>`;
                html += `<span style="font-size:10px;color:var(--text-muted)">in ${esc(sf || fullName)}</span>`;
                html += '</div>';
            }
            html += '</div>';
        }

        html += '<div class="to-detail-section" style="border-top:1px solid var(--border);padding-top:8px">';
        html += `<button class="btn btn-sm btn-primary" onclick="event.stopPropagation(); PA.codeModal.open('${PA.escJs(n.id)}')">Open Detail</button>`;
        html += `<button class="btn btn-sm" style="margin-left:6px" onclick="event.stopPropagation(); PA.showProcedure('${PA.escJs(n.id)}')">Show in Explore</button>`;
        html += '</div>';
        html += '</div>';
        return html;
    }
};

// ==================== INIT ====================
document.addEventListener('DOMContentLoaded', () => {
    PA.init();
    // ESC to close code modal
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            const modal = document.getElementById('codeModal');
            if (modal && modal.style.display !== 'none') {
                PA.codeModal.close();
            }
        }
    });
    // Backdrop click to close code modal
    document.getElementById('codeModal')?.addEventListener('click', (e) => {
        if (e.target.classList.contains('modal')) PA.codeModal.close();
    });
});
