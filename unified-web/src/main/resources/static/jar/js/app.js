/**
 * Main entry point — wires sidebar, tabs, upload, and delegates to sub-modules.
 * Shows toast notifications for all async actions so the user always knows what's happening.
 */
window.JA = window.JA || {};

JA.app = {

    currentAnalysis: null,
    currentJarId: null,
    jars: [],
    _uploading: false,
    _loading: false,
    _currentVersion: null,  // 'static' | 'corrected' | 'previous' | null (auto/default)
    _versionInfo: null,
    _dataVersion: 0,  // incremented whenever analysis data changes; sub-tabs use this to detect staleness

    /* ---- lifecycle ---- */

    async init() {
        await this.loadJars();
        this._pollSessionDot();
        if (JA.dashPanel) JA.dashPanel.render();

        // Hash-based URL routing
        window.addEventListener('popstate', () => this._handleRoute());
        this._handleRoute();
    },

    /** Read window.location.hash and navigate accordingly.
     *  Formats: #/jar/{id}  #/jar/{id}/{tab}  #/jar/{id}/{tab}/{subTab} */
    _handleRoute() {
        const hash = window.location.hash || '';
        const m = hash.match(/^#\/jar\/([^\/]+)(?:\/([^\/]+))?(?:\/([^\/]+))?$/);
        if (m) {
            const id = decodeURIComponent(m[1]);
            const tab = m[2] || null;
            const subTab = m[3] || null;
            if (id !== this.currentJarId) {
                this._pendingTab = tab;
                this._pendingSubTab = subTab;
                this.selectJar(id, { fromRoute: true });
            } else {
                if (tab) switchTab(tab);
                if (subTab && tab === 'summary') JA.summary.switchSubTab(subTab);
            }
        } else if (hash === '#/home' || !hash) {
            if (this.currentAnalysis) this.goHome();
        }
    },

    /** Navigate to welcome/home screen */
    goHome() {
        this.currentJarId = null;
        this.currentAnalysis = null;
        this._currentVersion = null;
        this._versionInfo = null;
        // Stop any active Claude/correction polling
        if (JA.summary._claudePollId) { clearInterval(JA.summary._claudePollId); JA.summary._claudePollId = null; }
        if (JA.summary._correctionPollId) { clearInterval(JA.summary._correctionPollId); JA.summary._correctionPollId = null; }
        // Clear stale summary state from previous JAR
        JA.summary._epReports = null;
        JA.summary._vertReport = null;
        JA.summary._distReport = null;
        JA.summary._batchReport = null;
        JA.summary._viewsReport = null;
        JA.summary._extReport = null;
        JA.summary._nodeNavState = null;
        document.getElementById('analysis').style.display = 'none';
        document.getElementById('welcome').style.display = '';
        JA.sidebar.render(this.jars, null);
        if (JA.dashPanel) JA.dashPanel.render();
        if (window.location.hash !== '#/home') {
            history.pushState(null, '', '#/home');
        }
        if (JA.chatToggle) JA.chatToggle.onJarCleared();
    },

    /** Periodically check if any Claude session is running and update the sidebar dot */
    _pollSessionDot() {
        let failCount = 0;
        const update = async () => {
            try {
                const sessions = await JA.api.listSessions();
                failCount = 0;
                const running = sessions.some(s => s.status === 'RUNNING');
                const dot = document.getElementById('sessions-dot');
                if (dot) dot.classList.toggle('active', running);
            } catch (e) {
                failCount++;
                if (failCount >= 3 && this._sessionPollId) {
                    clearInterval(this._sessionPollId);
                    failCount = 0;
                    this._sessionPollId = setInterval(update, PollConfig.sessionPollMs * 2);
                }
            }
        };
        update();
        this._sessionPollId = setInterval(update, PollConfig.sessionPollMs);
    },

    /* ---- data ---- */

    async loadJars() {
        // Show brief sync indicator on sidebar
        const jarList = document.getElementById('jar-list');
        if (jarList) jarList.classList.add('sidebar-syncing');
        try {
            this.jars = await JA.api.listJars();
            JA.sidebar.render(this.jars, this.currentJarId);
            // Update dashboard on welcome screen
            if (JA.dashboard) {
                JA.dashboard.render(this.jars);
                // Hide feature cards when there are JARs (dashboard replaces them)
                const features = document.getElementById('welcome-features');
                if (features) features.style.display = this.jars.length ? 'none' : '';
            }
            if (JA.dashPanel && document.getElementById('drp-activity-body')) {
                JA.dashPanel._loadActivity();
            }
        } catch (err) {
            console.error('Failed to load jars:', err);
            JA.toast.error('Failed to load JAR list');
        } finally {
            if (jarList) jarList.classList.remove('sidebar-syncing');
        }
    },

    /* ---- upload modal ---- */

    showUploadModal() {
        if (this._uploading) return;
        const btn = document.getElementById('upload-label');
        const accept = btn?.dataset?.fileAccept || '.jar';
        const label = btn?.dataset?.fileLabel || 'JAR';

        const existing = document.getElementById('upload-modal-overlay');
        if (existing) existing.remove();

        const overlay = document.createElement('div');
        overlay.id = 'upload-modal-overlay';
        overlay.className = 'upload-modal-overlay';
        overlay.innerHTML = `
            <div class="upload-modal-panel">
                <div class="upload-modal-header">
                    <span>Upload ${JA.utils.escapeHtml(label)}</span>
                    <button class="upload-modal-close" onclick="JA.app._closeUploadModal()">&#x2715;</button>
                </div>
                <div class="upload-modal-body">
                    <div class="upload-modal-field">
                        <label class="upload-modal-label">File <span class="upload-modal-ext">(${JA.utils.escapeHtml(accept)})</span></label>
                        <div class="upload-modal-file-row">
                            <input type="text" id="modal-file-name" class="claude-path-input upload-modal-filename" readonly placeholder="No file selected">
                            <button class="btn-sm" onclick="document.getElementById('modal-file-input').click()">Browse</button>
                            <input type="file" id="modal-file-input" accept="${JA.utils.escapeHtml(accept)}" hidden>
                        </div>
                    </div>
                    <div class="upload-modal-field">
                        <label class="upload-modal-label">Base Package <span class="upload-modal-optional">(optional)</span></label>
                        <input type="text" id="modal-base-package" class="claude-path-input" placeholder="e.g. com.example  (no spaces, no trailing dot)">
                    </div>
                    <div class="upload-modal-field">
                        <label class="upload-modal-label">Analysis Mode</label>
                        <div class="mode-toggle">
                            <label class="mode-option">
                                <input type="radio" name="modal-analysis-mode" value="static" checked>
                                <span class="mode-label">Static</span>
                            </label>
                            <label class="mode-option">
                                <input type="radio" name="modal-analysis-mode" value="claude">
                                <span class="mode-label">Claude</span>
                            </label>
                        </div>
                    </div>
                    <div class="upload-modal-field upload-modal-project-path-row" style="display:none">
                        <label class="upload-modal-label">Source Project Path <span class="upload-modal-optional">(for Claude)</span></label>
                        <input type="text" id="modal-project-path" class="claude-path-input" placeholder="Absolute path to source project">
                    </div>
                </div>
                <div class="upload-modal-footer">
                    <button class="btn-sm" onclick="JA.app._closeUploadModal()">Cancel</button>
                    <button class="upload-btn upload-modal-submit-btn" onclick="JA.app._submitUploadModal()">Upload</button>
                </div>
            </div>`;
        document.body.appendChild(overlay);

        overlay.querySelector('#modal-file-input').addEventListener('change', e => {
            const f = e.target.files[0];
            overlay.querySelector('#modal-file-name').value = f ? f.name : '';
        });

        overlay.querySelectorAll('input[name="modal-analysis-mode"]').forEach(radio => {
            radio.addEventListener('change', () => {
                const row = overlay.querySelector('.upload-modal-project-path-row');
                if (row) row.style.display = radio.value === 'claude' && radio.checked ? '' : 'none';
            });
        });

        overlay.addEventListener('click', e => { if (e.target === overlay) this._closeUploadModal(); });
        overlay.addEventListener('keydown', e => { if (e.key === 'Escape') this._closeUploadModal(); });

        setTimeout(() => overlay.querySelector('#modal-base-package')?.focus(), 50);
    },

    _closeUploadModal() {
        const overlay = document.getElementById('upload-modal-overlay');
        if (overlay) overlay.remove();
    },

    async _submitUploadModal() {
        const overlay = document.getElementById('upload-modal-overlay');
        if (!overlay) return;

        const fileInput = overlay.querySelector('#modal-file-input');
        const file = fileInput?.files[0];
        if (!file) { JA.toast.error('Please select a file to upload'); return; }

        const basePackage = overlay.querySelector('#modal-base-package')?.value || '';
        const modeRadio = overlay.querySelector('input[name="modal-analysis-mode"]:checked');
        const mode = modeRadio ? modeRadio.value : 'static';
        const projectPath = overlay.querySelector('#modal-project-path')?.value || '';

        this._closeUploadModal();
        await this.handleUpload(file, basePackage, mode, projectPath);
    },

    /* ---- actions ---- */

    async handleUpload(file, basePackage, mode, projectPath) {
        if (!file) return;

        // Check for duplicate JAR/WAR name
        const existing = this.jars.find(j => j.jarName === file.name || j.id === file.name);
        if (existing) {
            const choice = await this._showDuplicatePopup(file.name, existing);
            if (choice === 'cancel') return;
            if (choice === 'rename') {
                this._uploadRenameTs = Date.now();
            }
        }

        // Confirm if Claude mode selected (expensive operation)
        if (mode === 'claude') {
            const confirmed = await JA.utils.confirm({
                title: 'Run Claude Analysis',
                message: `<p>Upload <strong>${JA.utils.escapeHtml(file.name)}</strong> with <strong>Claude enrichment</strong>?</p>`
                    + `<p>Static analysis will run first, then Claude will analyze each endpoint in the background. This may take several minutes depending on the JAR size.</p>`,
                confirmLabel: 'Upload with Claude',
                confirmClass: 'confirm-btn-claude'
            });
            if (!confirmed) return;
        }

        this._uploading = true;
        document.querySelector('.sidebar')?.classList.add('sidebar-blocked');
        document.querySelectorAll('.tab').forEach(t => t.classList.add('tab-blocked'));
        this._showLoading(true);
        this._showProgressPanel(true);
        JA.toast.info('Uploading ' + file.name + ' (' + JA.utils.formatSize(file.size) + ')...');

        // SSE progress stream
        const progressLog = document.getElementById('progress-log');
        progressLog.innerHTML = '';
        this._progressLines = [];
        let eventSource = null;
        try {
            eventSource = new EventSource('/api/jar/progress');
            eventSource.addEventListener('progress', (e) => {
                const parts = e.data.split('|');
                const type = parts[0];
                const message = parts.slice(1).join('|');
                this._addProgressLine(type, message);
            });
        } catch (sseErr) {
            console.warn('SSE not available:', sseErr);
        }

        try {
            const renameSuffix = this._uploadRenameTs || null;
            this._uploadRenameTs = null;

            // Upload — now returns immediately with queued status
            const summary = await JA.api.uploadJar(file, mode, projectPath, renameSuffix, basePackage);
            if (eventSource) eventSource.close();
            eventSource = null;

            if (summary.status === 'queued') {
                const jobId = summary.jobId;
                const jarId = summary.jarName;
                this._addProgressLine('step', 'Queued for processing (job ' + jobId + ')...');
                this._updateStatusButton('pending', 'Queued...');

                // Track job via SSE with polling fallback
                await new Promise((resolve, reject) => {
                    let done = false;
                    let queueES = null;
                    let pollId = null;
                    const finish = (err) => {
                        if (done) return;
                        done = true;
                        if (queueES) try { queueES.close(); } catch(e){}
                        if (pollId) clearInterval(pollId);
                        if (err) reject(err); else resolve();
                    };

                    // Polling fallback — checks job status every 2s
                    const startPolling = () => {
                        if (pollId) return;
                        let notFoundCount = 0;
                        pollId = setInterval(async () => {
                            try {
                                const d = await fetch('/api/queue/' + jobId).then(r => r.json());
                                if (!d) return;
                                if (d.error) {
                                    // "Job not found" means the server was restarted and lost the job.
                                    // Stop polling after 3 consecutive misses to avoid infinite retry.
                                    notFoundCount++;
                                    if (notFoundCount >= 3) {
                                        finish(new Error('Job lost (server may have restarted). Please re-upload.'));
                                    }
                                    return;
                                }
                                notFoundCount = 0;
                                if (d.status === 'RUNNING') {
                                    this._addProgressLine('step', d.currentStep || '');
                                    if (d.progressPercent > 0) this._updateStatusButton('pending', d.progressPercent + '%');
                                } else if (d.status === 'COMPLETE') {
                                    this._addProgressLine('complete', 'Analysis complete!');
                                    finish();
                                } else if (d.status === 'FAILED') {
                                    this._addProgressLine('error', 'Failed: ' + (d.error || 'unknown'));
                                    finish(new Error(d.error || 'Analysis failed'));
                                } else if (d.status === 'CANCELLED') {
                                    this._addProgressLine('error', 'Cancelled');
                                    finish(new Error('Job cancelled'));
                                }
                            } catch (e) { /* retry next interval */ }
                        }, 2000);
                    };

                    // Try SSE first for real-time updates
                    try {
                        queueES = new EventSource('/api/queue/events');
                        const handleEvent = (e) => {
                            try {
                                const d = JSON.parse(e.data);
                                const job = d.job;
                                if (!job || job.id !== jobId) return;
                                if (e.type === 'job-started') {
                                    this._addProgressLine('step', 'Processing started...');
                                    this._updateStatusButton('pending', 'Processing...');
                                } else if (e.type === 'job-progress') {
                                    this._addProgressLine('step', job.currentStep || '');
                                    if (job.progressPercent > 0) this._updateStatusButton('pending', job.progressPercent + '% — ' + (job.currentStep || ''));
                                } else if (e.type === 'job-complete') {
                                    this._addProgressLine('complete', 'Analysis complete!');
                                    finish();
                                } else if (e.type === 'job-failed') {
                                    this._addProgressLine('error', 'Failed: ' + (job.error || 'unknown'));
                                    finish(new Error(job.error || 'Analysis failed'));
                                } else if (e.type === 'job-cancelled') {
                                    this._addProgressLine('error', 'Cancelled');
                                    finish(new Error('Job cancelled'));
                                }
                            } catch (err) { console.warn('Queue SSE parse error:', err); }
                        };
                        ['job-started','job-progress','job-complete','job-failed','job-cancelled'].forEach(evt => {
                            queueES.addEventListener(evt, handleEvent);
                        });
                        queueES.onerror = () => {
                            try { queueES.close(); } catch(e){}
                            queueES = null;
                            startPolling();
                        };
                    } catch (e) {
                        startPolling();
                    }
                });

                // Job complete — load results
                await this.loadJars();
                this._addProgressLine('complete', 'Loading results...');
                let analysis;
                try { analysis = await JA.api.getSummaryHeaders(jarId); }
                catch (e) { analysis = await JA.api.getSummary(jarId); }
                this._showAnalysis(analysis);
                this._updateStatusButton('static-done', summary.mode === 'claude' ? 'Claude enriching...' : 'Analysis Complete');
                JA.toast.success(analysis.totalClasses + ' classes, ' + analysis.totalEndpoints + ' endpoints loaded', 4000);
                if (JA.chatToggle) JA.chatToggle.onJarChanged(jarId);

                if (summary.mode === 'claude') {
                    this._addProgressLine('step', 'Claude enrichment started in background...');
                    this._startClaudePolling(jarId);
                }
            } else {
                // Fallback for non-queued response (shouldn't happen, but backward compat)
                await this.loadJars();
                this._addProgressLine('complete', 'Static analysis complete — loading results...');
                const jarId = summary.jarName;
                let analysis;
                try { analysis = await JA.api.getSummaryHeaders(jarId); }
                catch (e) { analysis = await JA.api.getSummary(jarId); }
                this._showAnalysis(analysis);
                this._updateStatusButton('static-done', 'Analysis Complete');
                JA.toast.success(analysis.totalClasses + ' classes, ' + analysis.totalEndpoints + ' endpoints loaded', 4000);
                if (JA.chatToggle) JA.chatToggle.onJarChanged(jarId);
            }
        } catch (err) {
            console.error('Upload error:', err);
            JA.toast.error('Upload failed: ' + err.message, 5000);
            this._updateStatusButton('error', 'Upload Failed');
            this._addProgressLine('error', 'Upload failed: ' + err.message);
            setTimeout(() => this._showProgressPanel(false), 3000);
        } finally {
            if (eventSource) eventSource.close();
            this._uploading = false;
            document.querySelector('.sidebar')?.classList.remove('sidebar-blocked');
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('tab-blocked'));
            this._showLoading(false);
        }
    },

    /* ---- Claude background polling ---- */

    _startClaudePolling(jarId) {
        this._stopClaudePolling();
        this._claudePollJar = jarId;
        this._claudePollTimer = setInterval(async () => {
            try {
                const status = await JA.api.getClaudeProgress(jarId);
                // Always update sidebar progress (global indicator)
                this._onClaudePollUpdate(jarId, status);
                if (status.status === 'COMPLETE' || status.status === 'FAILED') {
                    this._stopClaudePolling();
                    this._removeSidebarScan(jarId);
                    // Refresh sidebar badges first so _showAnalysis picks up new claudeStatus
                    await this.loadJars();
                    // Only update main UI if still viewing this JAR
                    if (this.currentJarId !== jarId) return;
                    this._showHeaderProgress(false);
                    if (status.status === 'COMPLETE') {
                        this._addProgressLine('complete', 'Full scan complete — data corrected');
                        // Auto-reload with corrected data
                        try {
                            await this._loadVersionInfo();
                            this._currentVersion = null; // show latest
                            let enriched;
                            try { enriched = await JA.api.getSummaryHeaders(jarId); }
                            catch (e) { enriched = await JA.api.getSummary(jarId); }
                            this._showAnalysis(enriched);
                            JA.toast.success('Full scan complete for ' + this._jarName(jarId) + ' — corrections applied');
                        } catch (reloadErr) {
                            JA.toast.success('Full scan complete for ' + this._jarName(jarId) + ' — reload to see corrected data');
                        }
                        this._ensureClaudeButton('Re-scan Claude', true);
                    } else {
                        this._addProgressLine('error', 'Claude scan failed');
                        this._ensureClaudeButton('Re-scan Claude', true);
                    }
                }
            } catch (e) {
                console.warn('Claude poll error:', e);
            }
        }, PollConfig.claudeProgressMs);
    },

    _stopClaudePolling() {
        if (this._claudePollTimer) {
            clearInterval(this._claudePollTimer);
            this._claudePollTimer = null;
        }
    },

    _onClaudePollUpdate(jarId, status) {
        // Update Claude section in summary ONLY if viewing this JAR
        if (this.currentJarId === jarId && typeof JA.summary._updateClaudeProgress === 'function') {
            JA.summary._updateClaudeProgress(status);
        }
        // Update sidebar global progress (always visible regardless of selected JAR)
        if (status.status === 'RUNNING') {
            const pct = status.totalEndpoints > 0
                ? Math.round((status.completedEndpoints / status.totalEndpoints) * 100) : 0;
            this._updateSidebarProgress(jarId, status.completedEndpoints, status.totalEndpoints, pct);
            // Only update header progress if viewing this JAR
            if (this.currentJarId === jarId) {
                this._showHeaderProgress(true, pct);
            }
        }
    },

    /** Track active scans for sidebar display */
    _activeScans: {},

    /** Update the sidebar global Claude progress indicator (supports multiple scans) */
    _updateSidebarProgress(jarId, done, total, pct) {
        this._activeScans[jarId] = { name: this._jarName(jarId), done, total, pct };
        this._renderSidebarProgress();
    },

    _removeSidebarScan(jarId) {
        delete this._activeScans[jarId];
        this._renderSidebarProgress();
    },

    _renderSidebarProgress() {
        const el = document.getElementById('claude-sidebar-progress');
        if (!el) return;
        const entries = Object.entries(this._activeScans);
        if (!entries.length) { el.style.display = 'none'; return; }
        el.style.display = '';
        let html = '<div class="csp-label">Claude Scans</div>';
        for (const [jarId, s] of entries) {
            html += `<div class="csp-entry" data-scan-jar-id="${JA.utils.escapeHtml(jarId)}" title="Click to view">`;
            html += `<div class="csp-jar">${JA.utils.escapeHtml(s.name)}</div>`;
            html += '<div class="csp-row">';
            html += `<div class="csp-bar"><div class="csp-fill" style="width:${s.pct}%"></div></div>`;
            html += `<div class="csp-count">${s.done}/${s.total} (${s.pct}%)</div>`;
            html += '</div></div>';
        }
        el.innerHTML = html;
        // Event delegation for scan entry clicks
        el.onclick = (e) => {
            const entry = e.target.closest('[data-scan-jar-id]');
            if (entry) JA.app.selectJar(entry.dataset.scanJarId);
        };
    },

    _hideSidebarProgress() {
        this._activeScans = {};
        this._renderSidebarProgress();
    },

    /* ---- Progress panel + status button ---- */

    _addProgressLine(type, message) {
        const log = document.getElementById('progress-log');
        if (!log) return;
        const line = document.createElement('div');
        line.className = 'progress-line ' + type;
        line.textContent = message;
        log.appendChild(line);
        log.scrollTop = log.scrollHeight;
        if (!this._progressLines) this._progressLines = [];
        this._progressLines.push({ type, message, time: new Date().toLocaleTimeString() });
    },

    _showProgressPanel(show) {
        const panel = document.getElementById('progress-log-panel');
        if (panel) panel.style.display = show ? '' : 'none';
    },

    _updateStatusButton(state, text) {
        const btn = document.getElementById('analysis-status-btn');
        if (!btn) return;
        btn.className = 'analysis-status-btn status-' + state;
        btn.textContent = text;
        btn.style.display = '';
    },

    _toggleProgressLog() {
        const log = document.getElementById('progress-log-panel');
        if (!log) return;
        const isOpen = log.style.display !== 'none';
        log.style.display = isOpen ? 'none' : '';
    },

    async selectJar(id, opts) {
        if (this._loading || this._uploading) return;
        this._loading = true;
        const fromRoute = opts && opts.fromRoute;
        document.querySelector('.sidebar')?.classList.add('sidebar-blocked');
        document.querySelectorAll('.tab').forEach(t => t.classList.add('tab-blocked'));

        // Reset version state
        this._currentVersion = null;
        this._versionInfo = null;
        this._removeBanner();

        // Stop stale polls from previous JAR immediately
        this._stopClaudePolling();
        if (JA.summary._claudePollId) { clearInterval(JA.summary._claudePollId); JA.summary._claudePollId = null; }
        if (JA.summary._correctionPollId) { clearInterval(JA.summary._correctionPollId); JA.summary._correctionPollId = null; }

        // Reset header: hide status button + progress bar from previous JAR
        const statusBtn = document.getElementById('analysis-status-btn');
        if (statusBtn) statusBtn.style.display = 'none';
        this._showHeaderProgress(false);

        this.currentJarId = id;
        JA.sidebar._expanded = {};
        JA.sidebar.render(this.jars, id);
        if (JA.dashPanel) JA.dashPanel.destroy();

        // Update URL hash for deep linking / browser navigation
        if (!fromRoute) {
            const targetHash = '#/jar/' + encodeURIComponent(id);
            if (window.location.hash !== targetHash && !window.location.hash.startsWith(targetHash + '/')) {
                history.pushState(null, '', targetHash);
            }
        }

        // Show info about what's being loaded
        const jarInfo = this.jars.find(j => j.id === id);
        const infoMsg = jarInfo
            ? 'Loading ' + (jarInfo.jarName || id) + ' (' + (jarInfo.totalClasses || '?') + ' classes, ' + (jarInfo.totalEndpoints || '?') + ' endpoints)'
            : 'Loading ' + id;

        // Clear old content immediately so stale data doesn't linger
        const ids = ['summary-container', 'class-tree', 'endpoint-list'];
        for (const eid of ids) { const el = document.getElementById(eid); if (el) el.innerHTML = ''; }
        document.getElementById('graph-container').innerHTML = '<p class="graph-placeholder">Loading...</p>';

        this._showLoading(true);

        const loadToast = JA.toast.loading(infoMsg + '...');

        try {
            const t0 = performance.now();
            let analysis;
            try {
                analysis = await JA.api.getSummaryHeaders(id, this._currentVersion);
            } catch (headerErr) {
                analysis = await JA.api.getSummary(id, this._currentVersion);
            }
            const loadMs = Math.round(performance.now() - t0);
            JA.toast.dismiss(loadToast);
            // Load version info before rendering so toggle buttons appear
            await this._loadVersionInfo();
            this._showAnalysis(analysis);
            JA.toast.success(
                analysis.totalClasses + ' classes, ' + analysis.totalEndpoints + ' endpoints loaded in ' + loadMs + 'ms',
                4000
            );
            // Check if Claude enrichment is still running for this JAR
            this._checkExistingClaudeProgress(id);
            if (JA.chatToggle) JA.chatToggle.onJarChanged(id);
        } catch (err) {
            JA.toast.dismiss(loadToast);
            console.error('selectJar error:', err);
            JA.toast.error('Failed to load: ' + err.message, 5000);
            // Reset state so the URL doesn't stay on a broken JAR id
            this.currentJarId = null;
            this.currentAnalysis = null;
            JA.sidebar.render(this.jars, null);
            history.replaceState(null, '', '#/home');
            document.getElementById('analysis').style.display = 'none';
            document.getElementById('welcome').style.display = '';
        } finally {
            this._loading = false;
            document.querySelector('.sidebar')?.classList.remove('sidebar-blocked');
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('tab-blocked'));
            this._showLoading(false);
        }
    },

    async _loadConnectionsForJar(id) {
        const el = document.getElementById('jar-conn-info');
        if (!el) return;
        try {
            const info = await JA.api.getConnections(id);
            if (!info || !info.available) { el.innerHTML = ''; return; }
            const parts = [];
            if (info.mongodb) {
                const db = info.mongodb.database || '';
                parts.push(`MongoDB${db ? ': ' + db : ''}`);
            }
            if (info.oracle) {
                const url = (info.oracle.jdbcUrl || '').replace(/(\/\/[^:]+:)[^@]+(@)/, '$1***$2');
                parts.push(`Oracle${url ? ': ' + url : ''}`);
            }
            if (parts.length) {
                el.innerHTML = `<span class="dash-conn-badge" title="${parts.join('\n')}" onclick="JA.app._showConnectionsModal('${id.replace(/'/g, "\\'")}')">🔗 ${parts.length} connection${parts.length > 1 ? 's' : ''}</span>`;
            } else {
                el.innerHTML = '';
            }
        } catch (e) { el.innerHTML = ''; }
    },

    async _showConnectionsModal(id) {
        try {
            const info = await JA.api.getConnections(id);
            if (!info || !info.available) { JA.toast.error('No connection info stored for this JAR'); return; }
            let msg = `Connection Info — ${id}\n`;
            if (info.mongodb) {
                msg += `\nMongoDB:\n  URI: ${info.mongodb.uri || 'N/A'}`;
                if (info.mongodb.database) msg += `\n  Database: ${info.mongodb.database}`;
            }
            if (info.oracle) {
                const url = (info.oracle.jdbcUrl || 'N/A').replace(/(\/\/[^:]+:)[^@]+(@)/, '$1***$2');
                msg += `\nOracle JDBC URL:\n  ${url}`;
            }
            msg += `\n\nExtracted: ${info.extractedAt || 'N/A'}`;
            alert(msg);
        } catch (e) { JA.toast.error('Failed to load connection info'); }
    },

    async deleteJar(id) {
        const jarInfo = this.jars.find(j => j.id === id);
        const name = jarInfo ? (jarInfo.jarName || id) : id;
        const confirmed = await JA.utils.confirm({
            title: 'Delete JAR Analysis',
            message: `<p>Are you sure you want to delete <strong>${JA.utils.escapeHtml(name)}</strong>?</p>`
                + `<p>This will remove the analysis data, all Claude fragments, and per-endpoint output.</p>`
                + `<p class="confirm-warn">This action cannot be undone.</p>`,
            confirmLabel: 'Delete',
            confirmClass: 'confirm-btn-danger'
        });
        if (!confirmed) return;
        try {
            // Stop Claude polling if this JAR is being polled
            if (this._claudePollJar === id) {
                this._stopClaudePolling();
            }
            this._removeSidebarScan(id);
            // Optimistic: remove from sidebar immediately
            this.jars = this.jars.filter(j => j.id !== id);
            JA.sidebar.render(this.jars, this.currentJarId);

            const result = await JA.api.deleteJar(id);
            const killMsg = result.sessionsKilled > 0
                ? ' (' + result.sessionsKilled + ' Claude session' + (result.sessionsKilled > 1 ? 's' : '') + ' killed)'
                : '';
            JA.toast.success('Deleted ' + name + killMsg);
            if (this.currentJarId === id) {
                this.goHome();
            }
            await this.loadJars();
        } catch (err) {
            console.error('Delete error:', err);
            JA.toast.error('Delete failed: ' + err.message);
        }
    },

    /* ---- duplicate detection popup ---- */

    _showDuplicatePopup(fileName, existing) {
        return new Promise(resolve => {
            const esc = JA.utils.escapeHtml;
            const analyzedAt = existing.analyzedAt ? new Date(existing.analyzedAt).toLocaleString() : 'unknown';
            const overlay = document.createElement('div');
            overlay.className = 'dup-overlay';
            overlay.innerHTML = `
                <div class="dup-dialog">
                    <h3>JAR Already Exists</h3>
                    <p><strong>${esc(fileName)}</strong> was already analyzed on <strong>${analyzedAt}</strong>
                    with ${existing.totalClasses} classes and ${existing.totalEndpoints} endpoints.</p>
                    <p>What would you like to do?</p>
                    <div class="dup-actions">
                        <button class="dup-btn dup-btn-overwrite" data-choice="overwrite">Overwrite</button>
                        <button class="dup-btn dup-btn-rename" data-choice="rename">Save as Copy (with timestamp)</button>
                        <button class="dup-btn dup-btn-cancel" data-choice="cancel">Cancel</button>
                    </div>
                </div>`;
            document.body.appendChild(overlay);
            overlay.addEventListener('click', e => {
                const btn = e.target.closest('[data-choice]');
                if (!btn) return;
                overlay.remove();
                resolve(btn.dataset.choice);
            });
        });
    },

    /* ---- view helpers ---- */

    _showAnalysis(analysis) {
        this.currentAnalysis = analysis;
        // Bump data version so sub-tabs know to re-render on next switch
        this._dataVersion = (this._dataVersion || 0) + 1;
        this.currentJarId = (analysis.jarName || '').replace(/[^a-zA-Z0-9._-]/g, '_');

        document.getElementById('welcome').style.display  = 'none';
        document.getElementById('analysis').style.display = '';

        document.getElementById('jar-title').textContent = analysis.projectName || analysis.jarName;
        const dateStr = analysis.analyzedAt ? new Date(analysis.analyzedAt).toLocaleString() : '';
        // Check if this JAR has Claude enrichment
        const jarInfo = this.jars.find(j => j.id === this.currentJarId);
        const claudeStatus = jarInfo ? (jarInfo.claudeStatus || 'IDLE') : 'IDLE';
        const isRunning = claudeStatus === 'RUNNING';
        const isComplete = claudeStatus === 'COMPLETE';
        const btnLabel = isComplete ? 'Re-scan Claude' : 'Enable Claude';
        const btnClass = isComplete ? 'enable-claude-btn rescan-claude-btn' : 'enable-claude-btn';

        // Analysis mode badge — use sidebar data as fallback for old JARs
        const mode = analysis.analysisMode || (jarInfo && jarInfo.analysisMode) || 'STATIC';
        const isCorrected = mode === 'CORRECTED';
        const ver = this._currentVersion;
        const vi = this._versionInfo;
        let modeBadge = '';
        if (ver === 'static') {
            // Viewing static version explicitly
            modeBadge = `<span class="jar-mode-badge jar-mode-static">STATIC (original)</span>`;
            if (vi && vi.hasCorrected) modeBadge += `<button class="mode-toggle-btn" onclick="JA.app._toggleVersion(null)">View Corrected</button>`;
        } else if (ver === 'previous') {
            // Viewing previous corrected version
            const iter = vi ? vi.claudeIteration - 1 : '?';
            modeBadge = `<span class="jar-mode-badge jar-mode-previous">PREVIOUS CORRECTED (iter ${iter})</span>`;
            modeBadge += `<button class="mode-toggle-btn" onclick="JA.app._toggleVersion(null)">View Latest</button>`;
            modeBadge += `<button class="mode-toggle-btn mode-revert-btn" onclick="JA.app._revertVersion()">Revert to This</button>`;
        } else if (isCorrected) {
            // Latest corrected analysis
            const corrAt = analysis.correctionAppliedAt ? new Date(analysis.correctionAppliedAt).toLocaleString() : '';
            const corrCount = analysis.correctionCount || 0;
            const iter = analysis.claudeIteration || (vi && vi.claudeIteration) || '';
            const iterLabel = iter ? ' (iter ' + iter + ')' : '';
            modeBadge = `<span class="jar-mode-badge jar-mode-corrected" onclick="JA.app._showModeInfo()" title="Claude corrected${corrCount ? ': ' + corrCount + ' corrections' : ''}${corrAt ? ' at ' + corrAt : ''}">&#10003; CORRECTED${iterLabel}</span>`;
            modeBadge += `<button class="mode-toggle-btn" onclick="JA.app._toggleVersion('static')" title="View original static analysis">View Static</button>`;
            if (vi && vi.hasCorrectedPrev) modeBadge += `<button class="mode-toggle-btn" onclick="JA.app._toggleVersion('previous')" title="View previous corrected version">View Previous</button>`;
        } else {
            // Static analysis — no corrected exists, clicking offers to run Claude
            modeBadge = `<span class="jar-mode-badge jar-mode-static" onclick="JA.app._onStaticBadgeClick()" title="Static bytecode analysis — click to run Claude correction" style="cursor:pointer">STATIC</span>`;
        }

        const bp = analysis.basePackage;
        document.getElementById('jar-stats').innerHTML = `
            ${modeBadge}
            <span class="stat"><strong>${analysis.totalClasses}</strong> classes</span>
            <span class="stat"><strong>${analysis.totalEndpoints}</strong> endpoints</span>
            <span class="stat"><strong>${JA.utils.formatSize(analysis.jarSize)}</strong></span>
            ${bp ? `<span class="stat stat-base-pkg" title="Base package filter applied at upload">pkg: <strong>${JA.utils.escapeHtml(bp)}</strong></span>` : ''}
            ${dateStr ? '<span class="stat stat-date">Analyzed: ' + dateStr + '</span>' : ''}
            <button class="${btnClass}" id="enable-claude-btn" onclick="JA.app._triggerClaudeEnrichment()" style="${isRunning ? 'display:none' : ''}">${btnLabel}</button>
            ${isComplete ? '<span class="jar-claude-badge jar-claude-complete">AI Enriched</span>' : ''}`;

        // Load and display connection info
        JA.app._loadConnectionsForJar(this.currentJarId);

        JA.toast.info('Rendering summary...', 1500);
        setTimeout(() => {
            JA.codeTree.setLazy();
            JA.endpointList.render(analysis.endpoints || []);
            JA.summary.render(analysis);

            // Reset graph area
            document.getElementById('graph-container').innerHTML =
                '<p class="graph-placeholder">Select an endpoint to view its call flow</p>';
            const bar = document.getElementById('graph-breadcrumb-bar');
            if (bar) bar.style.display = 'none';

            JA.sidebar.render(this.jars, this.currentJarId);

            // Apply pending tab/sub-tab from URL route, or default to summary
            const pendingTab = this._pendingTab || 'summary';
            const pendingSubTab = this._pendingSubTab || null;
            this._pendingTab = null;
            this._pendingSubTab = null;
            switchTab(pendingTab);
            if (pendingSubTab && pendingTab === 'summary') {
                JA.summary.switchSubTab(pendingSubTab);
            }
        }, 50);
    },

    /** Click on STATIC badge — check if Claude running, otherwise offer to start */
    async _onStaticBadgeClick() {
        const jarId = this.currentJarId;
        if (!jarId) return;
        const name = this._jarName(jarId);

        // Check if Claude is already running for this JAR
        try {
            const status = await JA.api.getClaudeProgress(jarId);
            if (status.status === 'RUNNING') {
                const done = status.completedEndpoints || 0;
                const total = status.totalEndpoints || 0;
                const pct = total > 0 ? Math.round((done / total) * 100) : 0;
                JA.toast.info('Claude scan already in progress for ' + name + ' — ' + done + '/' + total + ' (' + pct + '%)', 4000);
                // Ensure polling is active
                this._startClaudePolling(jarId);
                return;
            }
        } catch (e) { /* fall through to offer scan */ }

        const confirmed = await JA.utils.confirm({
            title: 'Static Analysis Only',
            message: '<p><strong>' + JA.utils.escapeHtml(name) + '</strong> has only static bytecode analysis.</p>'
                + '<p>Run a <strong>Claude full scan</strong> to correct collections, operation types, '
                + 'and verify against MongoDB catalog?</p>'
                + '<p>The static version is always preserved. Corrected data is saved separately.</p>',
            confirmLabel: 'Start Claude Scan',
            confirmClass: 'confirm-btn-claude'
        });
        if (confirmed) this._triggerClaudeEnrichment();
    },

    /** Click on CORRECTED badge — show info */
    _showModeInfo() {
        const a = this.currentAnalysis;
        if (!a) return;
        const corrAt = a.correctionAppliedAt ? new Date(a.correctionAppliedAt).toLocaleString() : 'unknown';
        const corrCount = a.correctionCount || '?';
        const iter = a.claudeIteration || '?';
        JA.toast.info('Claude corrected: ' + corrCount + ' corrections, iteration ' + iter + ', at ' + corrAt, 4000);
    },

    /** Fetch and cache version info for current JAR */
    async _loadVersionInfo() {
        const jarId = this.currentJarId;
        if (!jarId) return;
        try {
            this._versionInfo = await JA.api.getVersions(jarId);
        } catch (e) {
            console.warn('Failed to load version info:', e);
            this._versionInfo = null;
        }
    },

    /** Revert corrected to previous version (with confirmation) */
    async _revertVersion() {
        const jarId = this.currentJarId;
        if (!jarId) return;
        const name = this._jarName(jarId);
        const confirmed = await JA.utils.confirm({
            title: 'Revert to Previous Version',
            message: '<p>Revert <strong>' + JA.utils.escapeHtml(name) + '</strong> to the previous corrected version?</p>'
                + '<p>The current corrected analysis will be replaced by the previous one.</p>'
                + '<p class="confirm-warn">This action cannot be undone.</p>',
            confirmLabel: 'Revert',
            confirmClass: 'confirm-btn-warn'
        });
        if (!confirmed) return;
        try {
            await JA.api.revertClaude(jarId);
            this._currentVersion = null;
            this._versionInfo = null;
            JA.toast.success('Reverted to previous version');
            // Reload
            let analysis;
            try { analysis = await JA.api.getSummaryHeaders(jarId); }
            catch (e) { analysis = await JA.api.getSummary(jarId); }
            await this._loadVersionInfo();
            this._removeBanner();
            this._showAnalysis(analysis);
            await this.loadJars();
        } catch (e) {
            JA.toast.error('Revert failed: ' + e.message);
        }
    },

    /** Switch to a specific version: 'static', 'previous', or null (best available / latest corrected) */
    async _toggleVersion(version) {
        const jarId = this.currentJarId;
        if (!jarId) return;

        // Load version info if not cached
        if (!this._versionInfo) await this._loadVersionInfo();

        const label = version === 'static' ? 'static' : version === 'previous' ? 'previous corrected' : 'latest';
        try {
            JA.toast.info('Loading ' + label + ' version...');
            let analysis;
            try {
                analysis = await JA.api.getSummaryHeaders(jarId, version);
            } catch (e) {
                analysis = await JA.api.getSummary(jarId, version);
            }
            this._currentVersion = version;
            this._removeBanner();
            this._showAnalysis(analysis);
            if (version === 'static' || version === 'previous') this._showBanner(version);
            JA.toast.success('Viewing ' + label + ' version');
        } catch (e) {
            JA.toast.error('Failed to load ' + label + ' version: ' + e.message);
        }
    },

    _showBanner(version) {
        this._removeBanner();
        const banner = document.createElement('div');
        banner.id = 'version-banner';
        banner.className = 'version-banner';
        if (version === 'static') {
            banner.innerHTML = 'Viewing original static analysis (read-only) &mdash; '
                + '<button class="mode-toggle-btn" onclick="JA.app._toggleVersion(null)">Return to Corrected</button>';
        } else if (version === 'previous') {
            banner.innerHTML = 'Viewing previous corrected version (read-only) &mdash; '
                + '<button class="mode-toggle-btn" onclick="JA.app._toggleVersion(null)">Return to Latest</button> '
                + '<button class="mode-toggle-btn mode-revert-btn" onclick="JA.app._revertVersion()">Revert to This Version</button>';
        }
        const analysis = document.getElementById('analysis');
        if (analysis) analysis.insertBefore(banner, analysis.firstChild);
    },

    _removeBanner() {
        const el = document.getElementById('version-banner') || document.getElementById('backup-banner');
        if (el) el.remove();
    },

    _showLoading(show) {
        document.getElementById('loading').style.display  = show ? '' : 'none';
        document.getElementById('analysis').style.display = show ? 'none' :
            (this.currentAnalysis ? '' : 'none');
        if (show) document.getElementById('welcome').style.display = 'none';
        else if (!this.currentAnalysis) document.getElementById('welcome').style.display = '';
    },

    /** On selectJar of a previously analyzed JAR, check if Claude is still running */
    _checkExistingClaudeProgress(jarId) {
        JA.api.getClaudeProgress(jarId).then(status => {
            // Race guard: user may have switched JAR during fetch
            if (this.currentJarId !== jarId) return;
            if (status.status === 'RUNNING') {
                const pct = status.totalEndpoints > 0
                    ? Math.round((status.completedEndpoints / status.totalEndpoints) * 100) : 0;
                // Show progress in sidebar (global) + header (current JAR)
                this._updateSidebarProgress(jarId, status.completedEndpoints, status.totalEndpoints, pct);
                const enableBtn = document.getElementById('enable-claude-btn');
                if (enableBtn) enableBtn.style.display = 'none';
                this._showHeaderProgress(true, pct);
                this._startClaudePolling(jarId);
            }
        });
    },

    /**
     * Check if a Claude scan is already running for this JAR.
     * If so, show a warning and ask the user to cancel it before starting a new one.
     * Returns true if safe to proceed, false if user cancelled.
     */
    async _confirmScanOverride(jarId) {
        try {
            const status = await JA.api.getClaudeProgress(jarId);
            if (status.status !== 'RUNNING') return true;

            const name = this._jarName(jarId);
            const done = status.completedEndpoints || 0;
            const total = status.totalEndpoints || 0;
            const pct = total > 0 ? Math.round((done / total) * 100) : 0;

            const confirmed = await JA.utils.confirm({
                title: 'Scan Already Running',
                message: '<p>A Claude scan is already running for <strong>' + JA.utils.escapeHtml(name) + '</strong>.</p>'
                    + '<p>Progress: <strong>' + done + '/' + total + '</strong> endpoints (' + pct + '%)</p>'
                    + '<p class="confirm-warn">Cancel the running scan and start a new one?</p>',
                confirmLabel: 'Cancel & Restart',
                confirmClass: 'confirm-btn-warn'
            });
            if (!confirmed) return false;

            await JA.api.killJarSessions(jarId);
            this._stopClaudePolling();
            this._showHeaderProgress(false);
            JA.toast.info('Previous scan cancelled for ' + name);
            return true;
        } catch (e) {
            // If check fails, allow proceeding (backend will handle conflicts)
            return true;
        }
    },

    /** Trigger full Claude scan (correct + merge + replace). Works for first scan or re-scan. */
    async _triggerClaudeEnrichment() {
        const jarId = this.currentJarId;
        if (!jarId) return;

        // Check if a scan is already running — warn and cancel if so
        if (!(await this._confirmScanOverride(jarId))) return;

        const jarInfo = this.jars.find(j => j.id === jarId);
        const name = jarInfo ? (jarInfo.projectName || jarInfo.jarName || jarId) : jarId;
        const isRescan = jarInfo && jarInfo.claudeStatus === 'COMPLETE';

        const title = isRescan ? 'Re-scan Claude Analysis' : 'Enable Claude Analysis';
        const desc = isRescan
            ? '<p>Re-run full Claude correction scan on <strong>' + JA.utils.escapeHtml(name) + '</strong>?</p>'
                + '<p>Claude will re-analyze all endpoints, generate fresh corrections, and update the corrected analysis.</p>'
                + '<p>The current corrected version will be kept as the previous version for revert. Static analysis is always preserved.</p>'
            : '<p>Run full Claude correction scan on <strong>' + JA.utils.escapeHtml(name) + '</strong>?</p>'
                + '<p>Claude will analyze each endpoint, correct collection names and operation types, '
                + 'and save corrected data separately from the static analysis.</p>'
                + '<p>The static version is always preserved. You can switch between versions at any time.</p>';

        const confirmed = await JA.utils.confirm({
            title,
            message: desc,
            confirmLabel: isRescan ? 'Re-scan' : 'Start Full Scan',
            confirmClass: 'confirm-btn-claude'
        });
        if (!confirmed) return;

        try {
            // Hide enable button, show progress bar
            const enableBtn = document.getElementById('enable-claude-btn');
            if (enableBtn) enableBtn.style.display = 'none';
            this._showHeaderProgress(true, 0);

            JA.toast.info('Starting full Claude scan for ' + name + '...');
            const result = await JA.api.claudeFullScan(jarId, false);
            JA.toast.success('Full scan started for ' + name + ' — ' + result.totalEndpoints + ' endpoints queued');
            // Immediately show sidebar progress
            this._updateSidebarProgress(jarId, 0, result.totalEndpoints, 0);
            this._startClaudePolling(jarId);
            // Optimistic: update sidebar badge immediately
            const ji = this.jars.find(j => j.id === jarId);
            if (ji) { ji.claudeStatus = 'RUNNING'; ji.claudeTotal = result.totalEndpoints; ji.claudeCompleted = 0; }
            JA.sidebar.render(this.jars, this.currentJarId);
            await this.loadJars(); // sync with backend
        } catch (e) {
            JA.toast.error('Failed to start Claude for ' + name + ': ' + e.message);
            this._showHeaderProgress(false);
            const enableBtn = document.getElementById('enable-claude-btn');
            if (enableBtn) enableBtn.style.display = '';
        }
    },

    /** Resolve display name for a JAR id */
    _jarName(id) {
        const j = this.jars.find(j => j.id === id);
        return j ? (j.projectName || j.jarName || id) : id;
    },

    /** Ensure the Claude button exists, is visible, and has the right label */
    _ensureClaudeButton(label, show) {
        let btn = document.getElementById('enable-claude-btn');
        if (!btn) {
            // Button was never rendered (stale RUNNING state) — create it
            const stats = document.getElementById('jar-stats');
            if (stats) {
                btn = document.createElement('button');
                btn.id = 'enable-claude-btn';
                btn.onclick = () => JA.app._triggerClaudeEnrichment();
                stats.appendChild(btn);
            }
        }
        if (btn) {
            btn.textContent = label;
            btn.className = label === 'Re-scan Claude' ? 'enable-claude-btn rescan-claude-btn' : 'enable-claude-btn';
            btn.style.display = show ? '' : 'none';
        }
    },

    /** Show/hide the header progress bar during Claude scan */
    _showHeaderProgress(show, pct) {
        let bar = document.getElementById('claude-header-progress');
        if (show) {
            if (!bar) {
                bar = document.createElement('div');
                bar.id = 'claude-header-progress';
                bar.className = 'claude-header-progress';
                bar.innerHTML = '<div class="claude-progress-fill" id="claude-progress-fill"></div>';
                const header = document.querySelector('.analysis-header');
                if (header) header.appendChild(bar);
            }
            bar.style.display = '';
            const fill = document.getElementById('claude-progress-fill');
            if (fill) fill.style.width = (pct || 0) + '%';
        } else if (bar) {
            bar.style.display = 'none';
        }
    }
};

/* ---- global handlers referenced from HTML onclick ---- */

function switchTab(tabName) {
    document.querySelectorAll('.tab').forEach(t =>
        t.classList.toggle('active', t.dataset.tab === tabName));
    document.querySelectorAll('.tab-content').forEach(tc =>
        tc.style.display = tc.id === 'tab-' + tabName ? '' : 'none');
    if (tabName !== 'summary' && JA.summary._clearSubTabData) {
        JA.summary._clearSubTabData();
    }
    if (tabName === 'structure' && JA.codeTree && !JA.codeTree._loaded) {
        JA.codeTree.loadAndRender();
    }
    // When switching to summary, check if data version changed and re-render if stale
    if (tabName === 'summary' && JA.app.currentAnalysis) {
        const container = document.getElementById('summary-container');
        if (container) {
            const renderedVersion = container.dataset.renderedVersion;
            const currentVersion = JA.app._dataVersion || 0;
            if (renderedVersion && Number(renderedVersion) !== currentVersion) {
                JA.summary.render(JA.app.currentAnalysis);
            }
        }
    }
    // Update URL hash with tab
    if (JA.app.currentJarId) {
        const jarId = encodeURIComponent(JA.app.currentJarId);
        const newHash = '#/jar/' + jarId + '/' + tabName;
        if (window.location.hash !== newHash) {
            history.replaceState(null, '', newHash);
        }
    }
}
function filterTree()  { JA.codeTree.filter(document.getElementById('search-classes').value); }
function expandAll()   { JA.codeTree.expandAll(); }
function collapseAll() { JA.codeTree.collapseAll(); }

/* ---- boot ---- */
document.addEventListener('DOMContentLoaded', () => JA.app.init());
