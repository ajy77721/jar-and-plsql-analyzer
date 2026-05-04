/**
 * Summary Corrections — Claude Corrections sub-tab.
 * Shows side-by-side static vs Claude-corrected collections and operations.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _correctionData: null,
    _correctionPollId: null,

    _renderCorrectionsTab(epReports, esc) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId || !epReports.length) {
            return '<div class="corr-section corr-empty"><p>No analysis data available.</p></div>';
        }

        let html = '<div class="corr-section">';

        // Status area
        html += '<div id="corr-status-msg" class="claude-status">Checking for correction data...</div>';

        // Action header
        html += '<div class="claude-header" id="corr-action-header">';
        html += '<span class="claude-badge" style="background:#8b5cf6">CORRECTIONS</span>';
        html += '<span class="claude-title">Collection & Operation Corrections</span>';
        html += '<div class="claude-actions">';
        html += '<button class="btn-sm" id="corr-check-btn" onclick="JA.summary._checkCorrStatus()">Check Status</button>';
        html += '<button class="btn-sm btn-explore" onclick="JA.summary._corrScan(true)">Resume Scan</button>';
        html += '<button class="btn-sm" onclick="JA.summary._corrScan(false)" title="Re-analyze all endpoints">Fresh Scan</button>';
        html += '<button class="btn-sm" onclick="JA.summary._browseAllCorrLogs()" title="Browse all Claude decision logs (input/output)">Browse Claude Logs</button>';
        html += '</div>';
        html += '</div>';

        // Correction cards area
        html += '<div id="corr-cards-wrap" style="display:none">';
        html += '<div class="corr-summary-bar" id="corr-summary-bar"></div>';
        html += '<div class="corr-ep-list" id="corr-ep-list"></div>';
        html += '</div>';

        html += '</div>';

        setTimeout(() => this._checkCorrStatus(), 300);

        return html;
    },

    async _checkCorrStatus() {
        const jarId = JA.app.currentJarId || '';
        const msgEl = document.getElementById('corr-status-msg');
        if (!msgEl || !jarId) return;

        const cardsEl = document.getElementById('corr-cards-wrap');
        const showCards = () => { if (cardsEl) cardsEl.style.display = ''; };

        try {
            const res = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/corrections');
            if (!res.ok) throw new Error('Not found');
            const data = await res.json();
            this._correctionData = data;

            showCards();
            if (data.totalCorrections > 0) {
                msgEl.innerHTML = '<span class="claude-badge" style="background:#8b5cf6">CORRECTED</span> '
                    + data.totalCorrections + ' endpoints corrected.';
                msgEl.className = 'claude-status claude-status-ok';
                this._renderCorrectionCards(data);
            } else {
                msgEl.innerHTML = 'No correction data yet. Click <b>Resume Scan</b> to start Claude correction analysis.';
                msgEl.className = 'claude-status claude-status-none';
            }
        } catch (e) {
            msgEl.innerHTML = 'No correction data. Use the buttons above to start a correction scan.';
            msgEl.className = 'claude-status claude-status-none';
        }
    },

    _renderCorrectionCards(data) {
        const listEl = document.getElementById('corr-ep-list');
        const summaryEl = document.getElementById('corr-summary-bar');
        if (!listEl) return;

        this._corrDetailIdx = 0; // reset for fresh render
        const corrections = data.corrections || {};
        const epReports = this._epReports || [];
        const esc = JA.utils.escapeHtml;

        // Summary stats
        let totalAdded = 0, totalRemoved = 0, totalVerified = 0;
        for (const [, corr] of Object.entries(corrections)) {
            for (const nc of (corr.corrections || [])) {
                const c = nc.collections || {};
                totalAdded += (c.added || []).length;
                totalRemoved += (c.removed || []).length;
                totalVerified += (c.verified || []).length;
            }
        }

        if (summaryEl) {
            summaryEl.innerHTML =
                '<span class="corr-stat"><b>' + Object.keys(corrections).length + '</b> endpoints</span>' +
                '<span class="corr-stat corr-added-stat">+' + totalAdded + ' added</span>' +
                '<span class="corr-stat corr-removed-stat">-' + totalRemoved + ' removed</span>' +
                '<span class="corr-stat corr-verified-stat">' + totalVerified + ' verified</span>';
        }

        // Render each corrected endpoint
        let html = '';
        for (const [epName, corr] of Object.entries(corrections)) {
            html += this._renderCorrCard(epName, corr, epReports, esc);
        }
        if (!html) html = '<p class="sum-muted" style="padding:20px">No corrections available yet.</p>';
        listEl.innerHTML = html;
    },

    _corrDetailIdx: 0,

    _renderCorrCard(epName, corr, epReports, esc) {
        const staticReport = epReports.find(r => r.endpointName === epName);
        const summary = corr.endpointSummary;
        const detailId = 'corr-detail-' + (this._corrDetailIdx++);

        // Compute counts
        let addedCount = 0, removedCount = 0, verifiedCount = 0;
        for (const nc of (corr.corrections || [])) {
            const c = nc.collections || {};
            addedCount += (c.added || []).length;
            removedCount += (c.removed || []).length;
            verifiedCount += (c.verified || []).length;
        }
        const nodeCount = (corr.corrections || []).length;
        const staticCount = staticReport ? Object.keys(staticReport.collections).length : 0;
        const correctedCount = summary && summary.allCollections ? summary.allCollections.length : 0;

        let html = '<div class="ep-card corr-card corr-card-compact">';

        // ---- Compact header (always visible) ----
        html += '<div class="corr-compact-header" onclick="JA.summary._toggleCorrDetail(\'' + detailId + '\',this)">';
        html += '<div class="corr-compact-row1">';
        if (staticReport) {
            html += '<span class="endpoint-method method-' + staticReport.httpMethod + '">' + esc(staticReport.httpMethod) + '</span>';
            html += '<span class="ep-card-path">' + esc(staticReport.fullPath) + '</span>';
        }
        html += '<span class="ep-card-name">' + esc(epName) + '</span>';
        if (summary && summary.confidence) {
            const pct = Math.round(summary.confidence * 100);
            const cls = pct >= 90 ? 'corr-conf-high' : pct >= 70 ? 'corr-conf-med' : 'corr-conf-low';
            html += '<span class="corr-confidence ' + cls + '">' + pct + '%</span>';
        }
        html += '</div>';

        // Compact stats row
        html += '<div class="corr-compact-stats">';
        if (summary && summary.primaryOperation) {
            html += '<span class="sum-op-badge sum-op-' + summary.primaryOperation.toLowerCase() + '">' + esc(summary.primaryOperation) + '</span>';
        }
        html += '<span class="corr-stat-chip">Static: ' + staticCount + '</span>';
        html += '<span class="corr-stat-chip">Corrected: ' + correctedCount + '</span>';
        if (addedCount) html += '<span class="corr-stat-chip corr-added-stat">+' + addedCount + ' added</span>';
        if (removedCount) html += '<span class="corr-stat-chip corr-removed-stat">-' + removedCount + ' removed</span>';
        if (verifiedCount) html += '<span class="corr-stat-chip corr-verified-stat">' + verifiedCount + ' verified</span>';
        if (nodeCount) html += '<span class="corr-stat-chip">' + nodeCount + ' nodes</span>';
        if (summary && summary.crossModuleCalls && summary.crossModuleCalls.length) {
            for (const m of summary.crossModuleCalls) {
                html += '<span class="sum-module-tag">' + esc(m) + '</span>';
            }
        }
        html += '</div>';
        html += '</div>'; // end compact header

        // ---- Expandable detail (hidden by default) ----
        html += '<div class="corr-detail-panel" id="' + detailId + '" style="display:none">';

        // Side-by-side: Static vs Corrected
        html += '<div class="corr-side-by-side">';
        html += '<div class="corr-column">';
        html += '<div class="corr-col-header">Static Analysis (' + staticCount + ')</div>';
        if (staticReport && staticCount) {
            for (const [coll, info] of Object.entries(staticReport.collections)) {
                const src = info.sources ? Array.from(info.sources).join(', ') : '';
                html += '<span class="corr-coll-badge" title="' + esc(src) + '">' + esc(coll) + '</span> ';
            }
        } else {
            html += '<span class="sum-muted">None detected</span>';
        }
        html += '</div>';

        html += '<div class="corr-column">';
        html += '<div class="corr-col-header">Claude Corrected (' + correctedCount + ')</div>';
        if (summary && summary.allCollections && summary.allCollections.length) {
            for (const coll of summary.allCollections) {
                let cls = 'corr-coll-badge';
                if (this._isCorrAdded(corr, coll)) cls += ' corr-added';
                else if (this._isCorrVerified(corr, coll)) cls += ' corr-verified';
                html += '<span class="' + cls + '">' + esc(coll) + '</span> ';
            }
        } else {
            html += '<span class="sum-muted">No collections</span>';
        }
        const removed = this._getCorrRemoved(corr);
        if (removed.length) {
            html += '<div style="margin-top:6px">';
            for (const r of removed) {
                html += '<span class="corr-coll-badge corr-removed">' + esc(r) + '</span> ';
            }
            html += '</div>';
        }
        html += '</div>';
        html += '</div>'; // end side-by-side

        // Node corrections
        if (nodeCount) {
            html += '<div class="corr-nodes">';
            html += '<div class="corr-col-header" style="margin-top:8px">Node Corrections (' + nodeCount + ')</div>';
            for (const nc of corr.corrections) {
                html += '<div class="corr-node-item">';
                html += '<span class="corr-node-id">' + esc(nc.nodeId || '?') + '</span>';
                if (nc.operationType) {
                    html += ' <span class="sum-op-badge sum-op-' + nc.operationType.toLowerCase() + '">' + esc(nc.operationType) + '</span>';
                }
                if (nc.operationTypeReason) {
                    html += ' <span class="corr-reason">' + esc(nc.operationTypeReason) + '</span>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        html += '</div>'; // end detail panel

        // Actions (always visible)
        const safeName = epName.replace(/'/g, "\\'");
        html += '<div class="ep-card-actions">';
        html += '<button class="btn-sm" onclick="JA.summary._toggleCorrDetail(\'' + detailId + '\');event.stopPropagation()">Details</button>';
        html += '<button class="btn-sm" onclick="JA.summary._viewCorrLogs(\'' + safeName + '\');event.stopPropagation()" title="View Claude prompt input and response output for this endpoint">Browse Claude Log</button>';
        html += '<button class="btn-sm btn-explore" onclick="JA.summary._corrSingle(\'' + safeName + '\');event.stopPropagation()">Re-correct</button>';
        html += '</div>';

        html += '</div>';
        return html;
    },

    _toggleCorrDetail(id, headerEl) {
        const el = document.getElementById(id);
        if (!el) return;
        const show = el.style.display === 'none';
        el.style.display = show ? '' : 'none';
        // Toggle class on parent card for styling
        const card = el.closest('.corr-card');
        if (card) card.classList.toggle('corr-card-expanded', show);
    },

    _isCorrAdded(corr, collName) {
        for (const nc of (corr.corrections || [])) {
            for (const a of (nc.collections?.added || [])) {
                if (a.name === collName) return true;
            }
        }
        return false;
    },

    _isCorrVerified(corr, collName) {
        for (const nc of (corr.corrections || [])) {
            if ((nc.collections?.verified || []).includes(collName)) return true;
        }
        return false;
    },

    _getCorrRemoved(corr) {
        const removed = [];
        for (const nc of (corr.corrections || [])) {
            for (const r of (nc.collections?.removed || [])) {
                if (!removed.includes(r)) removed.push(r);
            }
        }
        return removed;
    },

    async _corrScan(resume) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }

        // Check if a scan is already running — warn and cancel if so
        if (!(await JA.app._confirmScanOverride(jarId))) return;

        const jarName = JA.app._jarName(jarId);
        const title = resume ? 'Resume Correction Preview' : 'Fresh Correction Preview';
        const msg = resume
            ? '<p>Resume generating correction previews? Already-corrected endpoints will be skipped.</p>'
                + '<p>This generates corrections for review only — use <b>Full Scan</b> from the header to apply them.</p>'
            : '<p>Generate <strong>fresh correction previews</strong> for ALL endpoints?</p>'
                + '<p>This generates corrections for side-by-side review. Use <b>Full Scan</b> to apply corrections to the actual data.</p>'
                + '<p class="confirm-warn">This discards previous correction previews.</p>';
        const confirmed = await JA.utils.confirm({
            title, message: msg,
            confirmLabel: resume ? 'Resume' : 'Fresh Preview',
            confirmClass: resume ? 'confirm-btn-claude' : 'confirm-btn-warn'
        });
        if (!confirmed) return;
        try {
            JA.toast?.info((resume ? 'Resuming correction preview for ' : 'Generating fresh previews for ') + jarName + '...');
            const res = await JA.api.claudeCorrect(jarId, resume);
            JA.toast?.success('Correction preview started for ' + jarName + ' — ' + res.totalEndpoints + ' endpoints queued');
            this._startCorrPoll();
        } catch (e) {
            JA.toast?.error('Failed for ' + jarName + ': ' + e.message);
        }
    },

    async _corrSingle(endpointName) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }
        const jarName = JA.app._jarName(jarId);
        const esc = JA.utils.escapeHtml;
        const confirmed = await JA.utils.confirm({
            title: 'Correct Endpoint',
            message: '<p>Run Claude correction on <strong>' + esc(endpointName) + '</strong>?</p>',
            confirmLabel: 'Correct',
            confirmClass: 'confirm-btn-claude'
        });
        if (!confirmed) return;
        try {
            JA.toast?.info('Correcting ' + endpointName + ' in ' + jarName);
            await JA.api.claudeCorrectSingle(jarId, endpointName);
            JA.toast?.success('Correction started for ' + endpointName.split('.').pop() + ' in ' + jarName);
            this._startCorrPoll();
        } catch (e) {
            JA.toast?.error('Failed for ' + jarName + ': ' + e.message);
        }
    },

    async _viewCorrLogs(endpointName) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }
        const esc = JA.utils.escapeHtml;

        try {
            const logs = await JA.api.getCorrectionLogs(jarId, endpointName);
            if (!logs || !logs.length) {
                JA.toast?.info('No correction logs found for ' + endpointName + '. Run a correction first.');
                return;
            }

            // Build modal
            let html = '<div class="corr-log-overlay" onclick="if(event.target===this)this.remove()">';
            html += '<div class="corr-log-modal">';
            html += '<div class="corr-log-header">';
            html += '<span class="claude-badge" style="background:#8b5cf6">LOGS</span>';
            html += '<span class="corr-log-title">' + esc(endpointName) + '</span>';
            html += '<button class="corr-log-close" onclick="this.closest(\'.corr-log-overlay\').remove()">&times;</button>';
            html += '</div>';

            // Tab buttons for each log file
            html += '<div class="corr-log-tabs">';
            logs.forEach((log, i) => {
                const sizeKb = (log.size / 1024).toFixed(1);
                const cls = log.type === 'prompt' ? 'corr-log-tab-prompt' : 'corr-log-tab-response';
                html += '<button class="corr-log-tab ' + cls + (i === 0 ? ' active' : '') + '" '
                    + 'onclick="JA.summary._switchCorrLogTab(this,' + i + ')">'
                    + esc(log.label) + ' <span class="corr-log-size">(' + sizeKb + 'KB)</span></button>';
            });
            html += '</div>';

            // Content panels
            logs.forEach((log, i) => {
                html += '<div class="corr-log-content" id="corr-log-panel-' + i + '" style="' + (i > 0 ? 'display:none' : '') + '">';
                html += '<pre class="corr-log-pre">' + esc(log.content) + '</pre>';
                html += '</div>';
            });

            html += '</div></div>';

            // Remove any existing modal
            document.querySelector('.corr-log-overlay')?.remove();
            document.body.insertAdjacentHTML('beforeend', html);
        } catch (e) {
            JA.toast?.error('Failed to load logs: ' + e.message);
        }
    },

    _switchCorrLogTab(btn, idx) {
        const modal = btn.closest('.corr-log-modal');
        modal.querySelectorAll('.corr-log-tab').forEach(t => t.classList.remove('active'));
        btn.classList.add('active');
        modal.querySelectorAll('.corr-log-content').forEach((p, i) => {
            p.style.display = i === idx ? '' : 'none';
        });
    },

    _startCorrPoll() {
        if (this._correctionPollId) return;
        const jarId = JA.app.currentJarId || '';
        this._correctionPollId = setInterval(async () => {
            // Stop if user switched to a different JAR
            if (JA.app.currentJarId !== jarId) {
                clearInterval(this._correctionPollId);
                this._correctionPollId = null;
                return;
            }
            try {
                await this._checkCorrStatus();
                // Stop polling if no sessions running
                const sessions = await JA.api.listSessions();
                const running = sessions.some(s => s.status === 'RUNNING');
                if (!running) {
                    clearInterval(this._correctionPollId);
                    this._correctionPollId = null;
                    // Re-fetch and re-render analysis so all tabs show corrected data
                    if (JA.app.currentJarId === jarId) {
                        try {
                            const enriched = await JA.api.getSummary(jarId);
                            JA.app._showAnalysis(enriched);
                        } catch (e) {
                            // Bump version as fallback so tabs re-render on next switch
                            JA.app._dataVersion = (JA.app._dataVersion || 0) + 1;
                        }
                    }
                }
            } catch (e) { /* continue */ }
        }, PollConfig.correctionMs);
    }
});
