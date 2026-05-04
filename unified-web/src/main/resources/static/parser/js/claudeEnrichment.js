window.PA = window.PA || {};

PA.claude = {
    mode: 'static',
    available: false,
    sessionId: null,
    _sse: null,
    _result: null,
    _chunkOverlayBound: false,
    _viewMode: 'both',
    _activeOps: new Set(),
    _page: 1,
    _pageSize: 50,
    _uiReady: false,

    init: function() {
        // Clear stale Claude cache on every page load — always start fresh
        PA.claude._result = null;
        PA.claude.mode = 'static';
        PA.claude.sessionId = null;
        try { sessionStorage.removeItem('pa_claude_mode'); } catch (e) {}

        PA.claude.checkStatus();
        PA.claude._bindModeButtons();
    },

    // ==================== STATUS ====================

    checkStatus: function() {
        PA.api.claudeStatus().then(function(data) {
            PA.claude.available = data && data.available === true;
            PA.claude._updateStatusDot();
        }).catch(function() {
            PA.claude.available = false;
            PA.claude._updateStatusDot();
        });
    },

    _updateStatusDot: function() {
        var el = document.getElementById('claudeStatusIndicator');
        if (!el) return;
        if (PA.claude.available) {
            el.className = 'claude-status available';
            el.innerHTML = '<span class="claude-status-dot"></span>Claude';
        } else {
            el.className = 'claude-status unavailable';
            el.innerHTML = '<span class="claude-status-dot"></span>Claude';
        }
    },

    // ==================== MODE TOGGLE ====================

    _bindModeButtons: function() {
        var btns = document.querySelectorAll('.claude-mode-btn');
        btns.forEach(function(btn) {
            btn.addEventListener('click', function() {
                var m = btn.dataset.mode;
                if (m) PA.claude.switchMode(m);
            });
        });
    },

    switchMode: function(newMode) {
        if (newMode === PA.claude.mode) return;
        var name = PA.claude._analysisName();
        if (!name) return;

        if (newMode === 'static') {
            PA.api.claudeLoadStatic(name).then(function() {
                PA.claude._applyMode('static');
                PA.toast('Static analysis view', 'success');
            }).catch(function(e) {
                PA.toast('Failed to load static view: ' + (e.message || e), 'error');
            });
        } else if (newMode === 'claude') {
            PA.api.claudeLoadClaude(name).then(function(data) {
                PA.claude._applyMode('claude');
                PA.claude._loadResult(name);
                PA.toast('Claude enriched view (' + (data.confirmedCount || 0) + ' confirmed)', 'success');
            }).catch(function(e) {
                PA.toast('No Claude data available. Run verification first.', 'warn');
            });
        } else if (newMode === 'previous') {
            PA.api.claudeLoadPrev(name).then(function() {
                PA.claude._applyMode('previous');
                PA.claude._loadResult(name);
                PA.toast('Loaded previous Claude version', 'success');
            }).catch(function(e) {
                PA.toast('No previous version available', 'warn');
            });
        }
    },

    _applyMode: function(m) {
        PA.claude.mode = m;
        document.querySelectorAll('.claude-mode-btn').forEach(function(btn) {
            btn.classList.toggle('active', btn.dataset.mode === m);
        });
        PA.claude._updateModeBadge(m);
        PA.claude._persistMode(m);

        if (m === 'static') {
            PA.claude._result = null;
            PA.claude._clearEnrichmentBadges();
        }

        // Trigger full content refresh so all tabs reflect the new data
        PA.claude._refreshVisibleContent();
    },

    _updateModeBadge: function(m) {
        var badge = document.getElementById('claudeModeBadge');
        if (!badge) return;
        if (m === 'claude') {
            badge.className = 'pa-mode-badge pa-mode-claude';
            badge.textContent = 'Claude Enriched';
        } else if (m === 'previous') {
            badge.className = 'pa-mode-badge pa-mode-previous';
            badge.textContent = 'Previous Version';
            // Fetch version info to show iteration/timestamp details
            PA.claude._loadVersionBadgeInfo(badge);
        } else {
            badge.className = 'pa-mode-badge pa-mode-static';
            badge.textContent = 'Static Analysis';
        }
    },

    // ==================== VERIFICATION ====================

    startVerification: function(resume) {
        var name = PA.claude._analysisName();
        if (!name) { PA.toast('Load an analysis first', 'warn'); return; }
        if (!PA.claude.available) { PA.toast('Claude is not available', 'error'); return; }

        var verifyBtn = document.getElementById('claudeVerifyBtn');
        if (verifyBtn) verifyBtn.disabled = true;

        PA.api.claudeVerify(name, !!resume).then(function(data) {
            PA.claude.sessionId = data.sessionId;
            PA.toast('Verification started', 'success');
            PA.claude._showProgress(true);
            PA.claude._startSSE(name);
        }).catch(function(e) {
            var msg = (e.message || '').indexOf('409') >= 0
                ? 'Verification already running'
                : 'Failed to start: ' + (e.message || e);
            PA.toast(msg, 'error');
            if (verifyBtn) verifyBtn.disabled = false;
        });
    },

    killVerification: function() {
        var name = PA.claude._analysisName();
        if (!name || !PA.claude.sessionId) return;

        PA.api.claudeKillSession(name, PA.claude.sessionId).then(function() {
            PA.toast('Verification killed', 'warn');
            PA.claude._stopSSE();
            PA.claude._showProgress(false);
        }).catch(function(e) {
            PA.toast('Failed to kill: ' + (e.message || e), 'error');
        });
    },

    // ==================== SSE PROGRESS ====================

    _startSSE: function(name) {
        PA.claude._stopSSE();
        var url = '/api/parser/analyses/' + encodeURIComponent(name) + '/claude/progress/stream';
        var es = new EventSource(url);
        PA.claude._sse = es;

        es.addEventListener('progress', function(e) {
            try {
                var d = JSON.parse(e.data);
                PA.claude._updateProgress(d);
            } catch (ex) {}
        });

        es.addEventListener('complete', function() {
            PA.claude._stopSSE();
            PA.claude._showProgress(false);
            PA.toast('Verification complete!', 'success');
            PA.claude._loadResult(name);
            PA.claude._applyMode('claude');
        });

        es.addEventListener('error', function() {
            PA.claude._stopSSE();
            PA.claude._showProgress(false);
            PA.toast('Verification stream ended', 'warn');
        });

        es.onerror = function() {
            PA.claude._stopSSE();
            PA.claude._showProgress(false);
        };
    },

    _stopSSE: function() {
        if (PA.claude._sse) {
            PA.claude._sse.close();
            PA.claude._sse = null;
        }
        var verifyBtn = document.getElementById('claudeVerifyBtn');
        if (verifyBtn) verifyBtn.disabled = false;
    },

    _showProgress: function(show) {
        var wrap = document.getElementById('claudeProgressWrap');
        if (!wrap) return;
        if (show) {
            wrap.classList.add('active');
        } else {
            wrap.classList.remove('active');
        }
    },

    _updateProgress: function(d) {
        var fill = document.getElementById('claudeProgressFill');
        var text = document.getElementById('claudeProgressText');
        if (!fill || !text) return;

        var pct = d.percent || 0;
        fill.style.width = pct + '%';

        var completed = d.completedChunks || 0;
        var total = d.totalChunks || 0;
        var failed = d.failedChunks || 0;
        var status = d.status || '';
        var parts = [];
        parts.push(Math.round(pct) + '%');
        if (total > 0) parts.push(completed + '/' + total + ' chunks');
        if (failed > 0) parts.push(failed + ' failed');
        if (status) parts.push(status);
        text.textContent = parts.join(' | ');
    },

    // ==================== RESULT LOADING ====================

    _loadResult: function(name) {
        PA.api.claudeResult(name).then(function(result) {
            PA.claude._result = result;
            PA.claude._applyEnrichmentBadges();
            PA.claude._page = 1;
            PA.claude.renderResult(result);
        }).catch(function() {
            PA.claude._result = null;
        });
    },

    getResult: function() {
        return PA.claude._result;
    },

    renderResult: function(result) {
        if (!result || !result.tables) return;

        var confirmed = 0, removed = 0, newOps = 0;
        var byOp = { SELECT: [0,0,0], INSERT: [0,0,0], UPDATE: [0,0,0], DELETE: [0,0,0], MERGE: [0,0,0] };
        var removedTables = {}, newTables = {};

        for (var i = 0; i < result.tables.length; i++) {
            var vs = result.tables[i].claudeVerifications || [];
            for (var j = 0; j < vs.length; j++) {
                var v = vs[j];
                var s = (v.status || '').toUpperCase();
                var op = (v.operation || 'OTHER').toUpperCase();
                if (s === 'CONFIRMED') { confirmed++; if (byOp[op]) byOp[op][0]++; }
                else if (s === 'REMOVED') {
                    removed++;
                    if (byOp[op]) byOp[op][1]++;
                    var rt = (result.tables[i].tableName || '').toUpperCase();
                    removedTables[rt] = (removedTables[rt] || 0) + 1;
                }
                else if (s === 'NEW') {
                    newOps++;
                    if (byOp[op]) byOp[op][2]++;
                    var nt = (result.tables[i].tableName || '').toUpperCase();
                    newTables[nt] = (newTables[nt] || 0) + 1;
                }
            }
        }

        var total = confirmed + removed + newOps;
        if (total === 0) return;
        var pct = Math.round(confirmed / total * 100);

        PA.toast('Claude: ' + pct + '% confirmed (' + confirmed + '/' + total + '), ' + removed + ' removed, ' + newOps + ' new', 'success');
    },

    // ==================== ENRICHMENT BADGES ON TABLE OPS ====================

    _applyEnrichmentBadges: function() {
        if (!PA.claude._result || !PA.claude._result.tables) return;
        if (PA.claude.mode === 'static') return;

        var tableRows = document.querySelectorAll('#to-tbody .to-row');
        var s = PA.tf ? PA.tf.state('to') : null;
        if (!s || !tableRows.length) return;

        for (var i = 0; i < tableRows.length; i++) {
            var row = tableRows[i];
            var dataItem = s.filtered[i];
            if (!dataItem) continue;
            var tableName = (dataItem.tableName || '').toUpperCase();
            var tvr = PA.claude._findTableResult(tableName);
            if (!tvr) continue;

            var firstTd = row.querySelector('td');
            if (!firstTd) continue;
            var existing = firstTd.querySelector('.cv-table-status');
            if (existing) existing.remove();
            var span = document.createElement('span');
            span.className = 'cv-table-status ' + (tvr.overallStatus || 'UNVERIFIED');
            span.textContent = tvr.overallStatus || 'UNVERIFIED';
            firstTd.appendChild(span);
        }
    },

    _clearEnrichmentBadges: function() {
        var badges = document.querySelectorAll('.cv-table-status, .cv-badge');
        badges.forEach(function(b) { b.remove(); });
    },

    _findTableResult: function(tableName) {
        if (!PA.claude._result || !PA.claude._result.tables) return null;
        var upper = tableName.toUpperCase();
        for (var i = 0; i < PA.claude._result.tables.length; i++) {
            var t = PA.claude._result.tables[i];
            if ((t.tableName || '').toUpperCase() === upper) return t;
        }
        return null;
    },

    renderDetailEnrichment: function(tableName) {
        if (PA.claude.mode === 'static' || !PA.claude._result) return '';
        var tvr = PA.claude._findTableResult((tableName || '').toUpperCase());
        if (!tvr || !tvr.claudeVerifications || !tvr.claudeVerifications.length) return '';

        var html = '<div class="cv-detail-section">';
        html += '<div class="cv-detail-title">Claude Verification</div>';
        for (var i = 0; i < tvr.claudeVerifications.length; i++) {
            var v = tvr.claudeVerifications[i];
            html += '<div class="cv-detail-item">';
            html += '<span class="op-badge ' + PA.esc(v.operation || '') + '">' + PA.esc(v.operation || '?') + '</span>';
            html += '<span class="cv-badge ' + PA.esc(v.status || '') + '">' + PA.esc(v.status || '') + '</span>';
            if (v.procedureName) {
                html += '<span style="font-size:11px;color:var(--text)">' + PA.esc(v.procedureName) + '</span>';
            }
            if (v.lineNumber) {
                html += '<span style="font-size:10px;color:var(--teal);font-family:var(--font-mono)">L' + v.lineNumber + '</span>';
            }
            if (v.reason) {
                html += '<span class="cv-detail-reason">' + PA.esc(v.reason) + '</span>';
            }
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    // ==================== CHUNK VIEWER ====================

    openChunkViewer: function() {
        PA.chunkViewer.open();
    },

    closeChunkViewer: function() {
        PA.chunkViewer.close();
    },

    // ==================== CONTENT REFRESH ====================

    /**
     * After a mode switch, refresh all visible content so tabs reflect new data.
     * Resets the loaded-tab cache and re-triggers the currently active right tab.
     * If a procedure is selected, reloads its detail and call tree.
     */
    _refreshVisibleContent: function() {
        // Reset tab-loaded flags so data is re-fetched from the (now switched) backend
        PA._rightTabLoaded = {};

        var procId = PA.context && PA.context.procId;
        var currentTab = PA._currentRightTab || 'callTrace';

        if (procId) {
            // Re-show the procedure entirely: reloads node detail + call tree from backend
            // Clear procId first so showProcedure does not short-circuit
            var savedProcId = procId;
            PA.context.procId = null;
            PA.showProcedure(savedProcId);
        }

        // Force-reload the active right tab even if showProcedure already switched to callTrace
        // (e.g. if user was on tableOps, we want that tab to re-render with new data)
        if (currentTab !== 'callTrace' && procId) {
            // switchRightTab checks _rightTabLoaded, which we cleared above
            // Use a short delay to let showProcedure's async loads settle first
            setTimeout(function() {
                PA.switchRightTab(currentTab);
            }, 300);
        }

        // If no proc is selected but tableOps data may be stale, reload it
        if (!procId && PA.tableOps && PA.tableOps.load) {
            PA.tableOps.load();
        }
    },

    // ==================== SESSION PERSISTENCE ====================

    _persistMode: function(m) {
        try {
            sessionStorage.setItem('pa_claude_mode', m);
        } catch (e) { /* sessionStorage unavailable */ }
    },

    /**
     * Restore the previously active mode from sessionStorage.
     * Polls until an analysis is loaded, then applies the saved mode.
     */
    _restoreMode: function() {
        try {
            var saved = sessionStorage.getItem('pa_claude_mode');
            if (!saved || saved === 'static') return;
            var attempts = 0;
            var timer = setInterval(function() {
                attempts++;
                if (attempts > 40) { clearInterval(timer); return; }
                var name = PA.claude._analysisName();
                if (!name) return;
                clearInterval(timer);
                // Verify Claude data exists before restoring non-static mode
                PA.api.claudeResult(name).then(function(result) {
                    if (result && result.tables && result.tables.length > 0 && saved !== PA.claude.mode) {
                        PA.claude.switchMode(saved);
                    } else {
                        PA.claude._persistMode('static');
                    }
                }).catch(function() {
                    PA.claude._persistMode('static');
                });
            }, 500);
        } catch (e) { /* sessionStorage unavailable */ }
    },

    // ==================== VERSION BADGE INFO ====================

    _loadVersionBadgeInfo: function(badge) {
        var name = PA.claude._analysisName();
        if (!name) return;
        PA.api.claudeVersions(name).then(function(info) {
            if (PA.claude.mode !== 'previous') return; // mode may have changed
            var label = 'Previous Version';
            if (info.iteration) {
                label += ' (v' + info.iteration + ')';
            }
            if (info.lastUpdated) {
                var ts = info.lastUpdated;
                // Show just the date portion if it's an ISO string
                if (ts.indexOf('T') > 0) ts = ts.substring(0, 10);
                label += ' ' + ts;
            }
            badge.textContent = label;
        }).catch(function() {
            // Keep default text on failure
        });
    },

    // ==================== APPLY ACCEPTED CHANGES ====================

    applyChanges: function() {
        var name = PA.claude._analysisName();
        if (!name) { PA.toast('Load an analysis first', 'warn'); return; }

        var btn = document.getElementById('claudeApplyBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Merging...'; }

        PA.api.claudeApply(name).then(function(data) {
            var removed = data.removed || 0;
            var added = data.added || 0;
            PA.toast('Merged view created: ' + removed + ' removed, ' + added + ' added (static unchanged)', 'success');
            if (btn) { btn.disabled = false; btn.textContent = 'Merge Accepted'; }
            PA.claude._refreshVisibleContent();
            if (PA.tableDetail) {
                PA.tableDetail._claudeResult = null;
                PA.tableDetail._claudeResultAnalysis = null;
            }
        }).catch(function(e) {
            PA.toast('Failed to generate merged view: ' + (e.message || e), 'error');
            if (btn) { btn.disabled = false; btn.textContent = 'Merge Accepted'; }
        });
    },

    // ==================== OVERVIEW MODAL ====================

    openOverview: async function() {
        var name = PA.claude._analysisName();
        if (!name) { PA.toast('Load an analysis first', 'warn'); return; }

        PA.claude._closeOverview();
        var overlay = document.createElement('div');
        overlay.className = 'tds-overlay';
        overlay.id = 'claudeOverviewOverlay';
        overlay.onclick = function(e) { if (e.target === overlay) PA.claude._closeOverview(); };

        var modal = document.createElement('div');
        modal.className = 'tds-modal';
        modal.style.maxWidth = '800px';
        modal.onclick = function(e) { e.stopPropagation(); };
        modal.innerHTML = '<div class="tds-header"><span style="font-weight:700">Claude Enrichment Overview</span><button class="btn btn-sm tds-close" onclick="PA.claude._closeOverview()">&times;</button></div><div class="tds-body"><div class="empty-msg">Loading...</div></div>';

        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        requestAnimationFrame(function() { overlay.classList.add('open'); });

        try {
            var result = await PA.api.claudeResult(name);
            PA.claude._renderOverviewBody(modal.querySelector('.tds-body'), result, name);
        } catch(e) {
            modal.querySelector('.tds-body').innerHTML = '<div class="empty-msg">No Claude enrichment data available.<br><span style="font-size:11px;color:var(--text-muted)">Run Claude verification first.</span></div>';
        }
    },

    _closeOverview: function() {
        var el = document.getElementById('claudeOverviewOverlay');
        if (el) { el.classList.remove('open'); setTimeout(function() { if (el.parentNode) el.remove(); }, 200); }
        delete PA.tf._state['cov'];
    },

    _renderOverviewBody: function(body, result, analysisName) {
        var esc = PA.esc;
        var html = '';

        // Session status card
        var ts = result.timestamp ? new Date(result.timestamp).toLocaleString() : '-';
        var dur = result.claudeTimeMs ? Math.round(result.claudeTimeMs / 1000) + 's' : '-';
        var errChunks = result.errorChunks || 0;
        var status = errChunks > 0 ? 'COMPLETED WITH ERRORS' : 'COMPLETED';
        var statusColor = errChunks > 0 ? 'var(--orange)' : 'var(--green)';
        if (result.error) { status = 'FAILED'; statusColor = 'var(--red)'; }

        html += '<div class="co-session">';
        html += '<div class="co-session-status" style="color:' + statusColor + '">' + status + '</div>';
        html += '<div class="co-session-grid">';
        html += '<div class="co-kv"><span class="co-kv-l">Last Run</span><span class="co-kv-v">' + esc(ts) + '</span></div>';
        html += '<div class="co-kv"><span class="co-kv-l">Duration</span><span class="co-kv-v">' + esc(dur) + '</span></div>';
        html += '<div class="co-kv"><span class="co-kv-l">API Calls</span><span class="co-kv-v">' + (result.claudeCallCount || 0) + '</span></div>';
        html += '<div class="co-kv"><span class="co-kv-l">Chunks</span><span class="co-kv-v">' + (result.totalChunks || 0) + (errChunks ? ' <span style="color:var(--red)">(' + errChunks + ' errors)</span>' : '') + '</span></div>';
        html += '</div></div>';

        // Verification summary
        var confirmed = result.confirmedCount || 0;
        var removed = result.removedCount || 0;
        var newCount = result.newCount || 0;
        var total = confirmed + removed + newCount;
        html += '<div class="co-summary">';
        html += '<div class="co-summary-card" style="border-left:3px solid var(--green)"><div class="co-summary-num" style="color:var(--green)">' + confirmed + '</div><div class="co-summary-label">Confirmed</div></div>';
        html += '<div class="co-summary-card" style="border-left:3px solid var(--red)"><div class="co-summary-num" style="color:var(--red)">' + removed + '</div><div class="co-summary-label">Removed</div></div>';
        html += '<div class="co-summary-card" style="border-left:3px solid var(--blue)"><div class="co-summary-num" style="color:var(--blue)">' + newCount + '</div><div class="co-summary-label">New</div></div>';
        html += '<div class="co-summary-card" style="border-left:3px solid var(--text-muted)"><div class="co-summary-num">' + total + '</div><div class="co-summary-label">Total</div></div>';
        html += '</div>';

        if (result.error) {
            html += '<div style="padding:8px 12px;background:#fff1f2;border-radius:6px;margin:8px 0;font-size:11px;color:var(--red)">' + esc(result.error) + '</div>';
        }

        html += '<div class="co-merge-bar">';
        html += '<button class="btn btn-sm" onclick="PA.claude._mergeFromOverview(this)">Merge Accepted</button>';
        html += '<span class="co-merge-info">Apply accepted changes to create a merged enriched view (static analysis stays unchanged)</span>';
        html += '</div>';

        // All table verifications in a TF table
        var tables = result.tables || [];
        var rows = [];
        for (var ti = 0; ti < tables.length; ti++) {
            var t = tables[ti];
            var vfs = t.claudeVerifications || [];
            var tConf = 0, tRem = 0, tNew = 0, tAccepted = 0, tRejected = 0, tPending = 0;
            for (var vi = 0; vi < vfs.length; vi++) {
                var st = (vfs[vi].status || '').toUpperCase();
                var ud = (vfs[vi].userDecision || '').toUpperCase();
                if (st === 'CONFIRMED') tConf++;
                else if (st === 'REMOVED') tRem++;
                else if (st === 'NEW') tNew++;
                if (ud === 'ACCEPTED') tAccepted++;
                else if (ud === 'REJECTED') tRejected++;
                else tPending++;
            }
            if (vfs.length === 0) continue;
            rows.push({
                tableName: t.tableName || '',
                overallStatus: t.overallStatus || '',
                total: vfs.length,
                confirmed: tConf, removed: tRem, newOps: tNew,
                accepted: tAccepted, rejected: tRejected, pending: tPending
            });
        }

        html += '<div style="margin-top:12px"><strong style="font-size:12px">Table Verifications (' + rows.length + ' tables)</strong></div>';
        html += '<table class="to-table" style="margin-top:6px"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'cov\',0)">Table</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'cov\',1)">Status</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'cov\',2)">Total</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'cov\',3)" style="color:var(--green)">Confirmed</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'cov\',4)" style="color:var(--red)">Removed</th>';
        html += '<th data-sort-col="5" onclick="PA.tf.sort(\'cov\',5)" style="color:var(--blue)">New</th>';
        html += '<th data-sort-col="6" onclick="PA.tf.sort(\'cov\',6)">Accepted</th>';
        html += '<th data-sort-col="7" onclick="PA.tf.sort(\'cov\',7)">Rejected</th>';
        html += '<th data-sort-col="8" onclick="PA.tf.sort(\'cov\',8)">Pending</th>';
        html += '</tr></thead><tbody id="cov-tbody"></tbody></table>';
        html += '<div class="pagination-bar" id="cov-pager"></div>';

        body.innerHTML = html;

        PA.tf.init('cov', rows, 50, function(r, idx, esc) {
            var rowHtml = '<tr class="to-row" style="cursor:pointer" onclick="PA.claude._closeOverview();PA.tableDetail.open(\'' + PA.escJs(r.tableName) + '\',\'\',\'claude\')">';
            rowHtml += '<td style="font-weight:600;color:var(--teal)">' + esc(r.tableName) + '</td>';
            rowHtml += '<td><span class="cv-badge ' + (r.overallStatus === 'CONFIRMED' ? 'confirmed' : r.overallStatus === 'PARTIALLY_CONFIRMED' ? 'confirmed' : '') + '" style="font-size:9px">' + esc(r.overallStatus) + '</span></td>';
            rowHtml += '<td class="cx-num">' + r.total + '</td>';
            rowHtml += '<td class="cx-num" style="color:var(--green)">' + r.confirmed + '</td>';
            rowHtml += '<td class="cx-num" style="color:var(--red)">' + r.removed + '</td>';
            rowHtml += '<td class="cx-num" style="color:var(--blue)">' + r.newOps + '</td>';
            rowHtml += '<td class="cx-num">' + (r.accepted > 0 ? '<span style="color:var(--green)">' + r.accepted + '</span>' : '0') + '</td>';
            rowHtml += '<td class="cx-num">' + (r.rejected > 0 ? '<span style="color:var(--red)">' + r.rejected + '</span>' : '0') + '</td>';
            rowHtml += '<td class="cx-num">' + (r.pending > 0 ? '<span style="color:var(--orange);font-weight:700">' + r.pending + '</span>' : '0') + '</td>';
            rowHtml += '</tr>';
            return rowHtml;
        }, {
            sortKeys: {
                0: { fn: function(r) { return r.tableName.toUpperCase(); } },
                1: { fn: function(r) { return r.overallStatus; } },
                2: { fn: function(r) { return r.total; } },
                3: { fn: function(r) { return r.confirmed; } },
                4: { fn: function(r) { return r.removed; } },
                5: { fn: function(r) { return r.newOps; } },
                6: { fn: function(r) { return r.accepted; } },
                7: { fn: function(r) { return r.rejected; } },
                8: { fn: function(r) { return r.pending; } }
            }
        });

        var s = PA.tf.state('cov');
        if (s) { s.sortCol = 8; s.sortDir = 'desc'; }
        PA.tf.filter('cov');

        setTimeout(function() {
            PA.tf.initColFilters('cov', {
                0: { label: 'Table', valueFn: function(r) { return r.tableName || ''; } },
                1: { label: 'Status', valueFn: function(r) { return r.overallStatus; } }
            });
        }, 0);
    },

    _mergeFromOverview: function(btn) {
        if (btn) { btn.disabled = true; btn.textContent = 'Merging...'; }
        var name = PA.claude._analysisName();
        if (!name) { PA.toast('No analysis loaded', 'warn'); return; }
        PA.api.claudeApply(name).then(function(data) {
            if (btn) { btn.disabled = false; btn.textContent = 'Merge Accepted'; }
            PA.toast('Merged view created (' + (data.confirmedCount || 0) + ' confirmed)', 'success');
        }).catch(function(e) {
            if (btn) { btn.disabled = false; btn.textContent = 'Merge Accepted'; }
            PA.toast('Merge failed: ' + (e.message || e), 'error');
        });
    },

    // ==================== HELPERS ====================

    _analysisName: function() {
        return (PA.analysisData && PA.analysisData.name) || PA.api._analysisName || null;
    }
};
