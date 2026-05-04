/**
 * Summary Claude — Claude Insights sub-tab.
 * Shows Claude-enriched analysis data when available.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _renderClaudeTab(epReports, esc) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId || !epReports.length) {
            return '<div class="claude-section claude-empty"><p>No analysis data available.</p></div>';
        }

        let html = '<div class="claude-section">';

        // Claude Usage Stats panel — populated asynchronously
        html += '<div id="claude-usage-panel-container"></div>';

        // Status area — shown first; _checkClaudeStatus() populates based on actual state
        html += '<div id="claude-status-msg" class="claude-status">Checking for Claude analysis data...</div>';

        // Action header — hidden until _checkClaudeStatus() confirms Claude data exists
        html += '<div class="claude-header" id="claude-action-header" style="display:none">';
        html += '<span class="claude-badge">CLAUDE</span>';
        html += '<span class="claude-title">Deep Analysis Insights</span>';
        html += '<div class="claude-actions">';
        html += `<button class="btn-sm" id="claude-check-btn" onclick="JA.summary._checkClaudeStatus()">Check Status</button>`;
        html += `<button class="btn-sm btn-explore" onclick="JA.summary._claudeFullScan(true)">Resume Scan</button>`;
        html += `<button class="btn-sm" onclick="JA.summary._claudeFullScan(false)" title="Re-scan all endpoints, correct and replace static data">Fresh Scan</button>`;
        html += `<button class="btn-sm" onclick="JA.summary._showClaudeLogs()">Live Logs</button>`;
        html += `<button class="btn-sm" onclick="JA.summary._showRunLogs()" title="View per-run correction logs">Run Logs</button>`;
        html += `<button class="btn-sm" onclick="JA.sessions.show()" title="View all Claude sessions">Sessions</button>`;
        html += '</div>';
        html += '</div>';

        // Paginated endpoint cards — hidden until Claude data is confirmed
        html += '<div id="claude-cards-wrap" style="display:none">';
        html += this._buildFilterBar('sum-claude', epReports, r => r.primaryDomain);
        html += '<div class="sum-pager sum-pager-top" id="sum-claude-pager-top"></div>';
        html += '<div class="claude-ep-list" id="sum-claude-tbody"></div>';
        html += '<div class="sum-pager" id="sum-claude-pager"></div>';
        html += '</div>';
        html += '</div>';

        this._initPage('sum-claude', epReports, 25,
            (r, i, esc) => this._renderClaudeCard(r, i, esc),
            r => r.primaryDomain
        );
        setTimeout(() => this._pageRender('sum-claude'), 0);

        // Auto-check for Claude fragments
        setTimeout(() => this._checkClaudeStatus(), 300);

        // Load Claude API call statistics
        setTimeout(() => {
            const jarName = JA.app._jarName(jarId);
            JA.api.getClaudeStats(jarName).then(stats => {
                const container = document.getElementById('claude-usage-panel-container');
                if (container) this._renderClaudeUsagePanel(stats, container);
            });
        }, 100);

        return html;
    },

    _renderClaudeCard(r, idx, esc) {
        let html = `<div class="ep-card claude-card" onclick="JA.summary.toggleDetail('claude',${idx})">`;
        // Header
        html += '<div class="ep-card-header">';
        html += `<span class="endpoint-method method-${r.httpMethod}">${esc(r.httpMethod)}</span>`;
        html += `<span class="ep-card-path">${esc(r.fullPath)}</span>`;
        html += `<span class="ep-card-name">${esc(r.endpointName)}</span>`;
        html += '</div>';

        // Claude-specific fields
        html += '<div class="ep-card-meta">';
        if (r.procName) html += `<span class="claude-procname">proc: ${esc(r.procName)}</span>`;
        r.domains.forEach(d => { html += `<span class="sum-domain-tag">${esc(d)}</span>`; });
        html += `<span class="ep-card-chip">${r.totalCollections} colls</span>`;
        html += `<span class="ep-card-chip">${r.totalDbOperations} ops</span>`;
        html += `<span class="ep-card-chip">${r.externalScopeCalls} cross</span>`;
        html += `<span class="ep-card-chip">${r.totalLoc || 0} LOC</span>`;
        html += '</div>';

        // Operations row
        html += '<div class="ep-card-ops">';
        r.operationTypes.forEach(op => { html += `<span class="sum-op-badge sum-op-${op.toLowerCase()}">${esc(op)}</span>`; });
        html += `<span class="sum-size-badge sum-size-${r.sizeCategory.toLowerCase()}">${esc(r.sizeCategory)}</span>`;
        html += '</div>';

        // Actions
        const safeName = (r.endpointName || '').replace(/'/g, "\\'");
        html += '<div class="ep-card-actions">';
        html += `<button class="btn-sm btn-explore" onclick="JA.summary.showCallTrace(${idx});event.stopPropagation()">Explore</button>`;
        html += `<button class="btn-sm" onclick="JA.summary.showTrace(${idx});event.stopPropagation()">Trace</button>`;
        html += `<button class="btn-sm btn-nodes" onclick="JA.summary.showNodeNav(${idx});event.stopPropagation()">Nodes</button>`;
        html += `<button class="btn-sm btn-export" onclick="JA.summary.showExportModal({endpointIdx:${idx}});event.stopPropagation()">Export</button>`;
        html += `<button class="btn-sm claude-enrich-btn" onclick="JA.summary._claudeEnrichSingle('${safeName}');event.stopPropagation()" title="Run Claude analysis on this endpoint">Analyze</button>`;
        html += `<button class="btn-sm" onclick="JA.summary._viewAnalysisLogs('${safeName}');event.stopPropagation()" title="View prompt sent to Claude and response received">Logs</button>`;
        html += '</div>';
        html += '</div>';

        // Expandable detail — full rich view matching endpoint detail
        html += `<div class="ep-card-detail" id="sum-claude-detail-${idx}" style="display:none">`;
        html += '<div class="sum-detail">' + this._renderClaudeDetail(r, idx, esc) + '</div></div>';
        return html;
    },

    /** Full detail for Claude card — reuses endpoint detail structure */
    _renderClaudeDetail(r, idx, esc) {
        let html = '';

        // Collections by domain (scrollable)
        if (Object.keys(r.collDomainGroups).length) {
            html += '<div class="sum-detail-block"><div class="sum-detail-label">Collections by Domain (' + Object.keys(r.collections).length + ')</div>';
            html += this._scrollSection('claude-colls-' + idx);
            for (const [domain, colls] of Object.entries(r.collDomainGroups)) {
                html += '<div class="sum-detail-domain sum-scroll-item"><span class="sum-domain-tag">' + esc(domain) + '</span> ';
                html += colls.map(c => {
                    const cInfo = r.collections[c];
                    return this._collBadgeRich(c, esc, cInfo);
                }).join(' ') + '</div>';
            }
            html += '</div></div>';
        }

        // Collection call paths (scrollable)
        const bcColls = Object.keys(r.collBreadcrumbs || {}).filter(k => r.collBreadcrumbs[k].length > 0);
        if (bcColls.length) {
            html += '<div class="sum-detail-block sum-detail-block-full"><div class="sum-detail-label">Collection Call Paths (' + bcColls.length + ')</div>';
            html += this._scrollSection('claude-paths-' + idx);
            for (const cn of bcColls) {
                html += `<div class="sum-bc-group sum-scroll-item">${this._collBadgeRich(cn, esc, r.collections[cn])}`;
                for (const bc of r.collBreadcrumbs[cn]) { html += '<div class="sum-breadcrumb">' + this._renderBc(bc, esc) + '</div>'; }
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Write / Read / Aggregate collections
        for (const [label, arr] of [['Write', r.writeCollections], ['Read', r.readCollections], ['Aggregate', r.aggregateCollections]]) {
            if (arr.length) {
                html += `<div class="sum-detail-block"><div class="sum-detail-label">${label} (${arr.length})</div>`;
                html += arr.map(c => this._collBadgeRich(c, esc, r.collections[c])).join(' ') + '</div>';
            }
        }

        // Views used
        if (r.viewsUsed.length) {
            html += '<div class="sum-detail-block"><div class="sum-detail-label">Views (' + r.viewsUsed.length + ')</div>';
            html += r.viewsUsed.map(c => this._collBadgeRich(c, esc, r.collections[c])).join(' ') + '</div>';
        }

        // Service beans (scrollable)
        if (r.serviceClasses.length) {
            html += '<div class="sum-detail-block"><div class="sum-detail-label">Services (' + r.serviceClasses.length + ')</div>';
            html += this._scrollSection('claude-svc-' + idx);
            r.serviceClasses.forEach(s => {
                const safeName = s.replace(/'/g, "\\'");
                html += `<span class="sum-bean-tag sum-bean-clickable sum-scroll-item" onclick="event.stopPropagation();JA.summary.showClassCode('${safeName}')" title="View code">${esc(s)}</span> `;
            });
            html += '</div></div>';
        }

        // HTTP/REST calls (scrollable)
        if (r.httpCalls.length) {
            html += '<div class="sum-detail-block sum-detail-block-full"><div class="sum-detail-label">REST/HTTP Calls (' + r.httpCalls.length + ')</div>';
            html += this._scrollSection('claude-http-' + idx);
            for (const h of r.httpCalls) {
                const safeClass = (h.simpleClassName || '').replace(/'/g, "\\'");
                const safeMethod = (h.methodName || '').replace(/'/g, "\\'");
                html += '<div class="sum-http-item sum-scroll-item">';
                html += `<span class="sum-http-badge">${esc(h.operationType || 'REST')}</span> `;
                html += `<span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${safeClass}','${safeMethod}')" title="View code">${esc(h.simpleClassName)}.${esc(h.methodName)}</span>`;
                if (h.url) html += ` <span class="sum-http-url">${esc(h.url)}</span>`;
                if (h.breadcrumb?.length) html += '<div class="sum-breadcrumb">' + this._renderBc(h.breadcrumb, esc) + '</div>';
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Cross-module dependencies (scrollable, full detail)
        if (r.externalCalls.length) {
            html += '<div class="sum-detail-block sum-detail-block-full"><div class="sum-detail-label">Cross-Module Dependencies (' + r.externalScopeCalls + ') <span class="sum-dep-warn">Direct calls — should be REST API</span></div>';
            html += this._scrollSection('claude-ext-' + idx);
            const grouped = {};
            for (const ext of r.externalCalls) { (grouped[ext.sourceJar || 'main'] = grouped[ext.sourceJar || 'main'] || []).push(ext); }
            for (const [jar, exts] of Object.entries(grouped)) {
                html += `<div class="sum-ext-group sum-scroll-item"><div class="sum-ext-header"><span class="sum-module-tag">${esc(this._jarToProject(jar))}</span> <span class="sum-domain-tag">${esc(this._jarToDomain(jar))}</span> <span class="sum-ref-count">${exts.length}</span></div>`;
                for (const e of exts) {
                    const safeClass = (e.simpleClassName || '').replace(/'/g, "\\'");
                    const safeMethod = (e.methodName || '').replace(/'/g, "\\'");
                    html += `<div class="sum-ext-item"><span class="sum-ext-method-link" onclick="event.stopPropagation();JA.summary._openExtMethod('${safeClass}','${safeMethod}')" title="View code">${esc(e.simpleClassName)}.${esc(e.methodName)}</span>`;
                    if (e.breadcrumb?.length) html += '<div class="sum-breadcrumb">' + this._renderBc(e.breadcrumb, esc) + '</div>';
                    html += '</div>';
                }
                html += '</div>';
            }
            html += '</div></div>';
        }

        return html;
    },

    // _scrollSection and _filterScrollSection are in summary-helpers.js

    async _checkClaudeStatus() {
        const jarId = JA.app.currentJarId || '';
        const msgEl = document.getElementById('claude-status-msg');
        if (!msgEl || !jarId) return;

        const headerEl = document.getElementById('claude-action-header');
        const cardsEl = document.getElementById('claude-cards-wrap');
        const showPanel = () => {
            if (headerEl) headerEl.style.display = '';
            if (cardsEl) cardsEl.style.display = '';
        };

        // First check live enrichment progress
        try {
            const progress = await JA.api.getClaudeProgress(jarId);
            // Race guard: user may have switched JARs during the await
            if (JA.app.currentJarId !== jarId) return;
            if (progress.status === 'RUNNING') {
                showPanel();
                this._updateClaudeProgress(progress);
                return;
            }
            if (progress.status === 'COMPLETE') {
                showPanel();
                msgEl.innerHTML = '<span class="claude-badge">CORRECTED</span> Full scan complete — '
                    + (progress.completedEndpoints || 0) + ' endpoints corrected and applied to analysis.';
                msgEl.className = 'claude-status claude-status-ok';
                return;
            }
            if (progress.status === 'FAILED') {
                showPanel();
                const errCount = (progress.errors || []).length;
                msgEl.innerHTML = '<span class="claude-badge" style="background:#ef4444">FAILED</span> Claude scan failed or was interrupted'
                    + (errCount ? ' (' + errCount + ' error' + (errCount > 1 ? 's' : '') + ')' : '')
                    + '. Static data unchanged. Retry with the buttons above.';
                msgEl.className = 'claude-status claude-status-none';
                return;
            }
        } catch (e) { /* fall through to fragment check */ }

        // Fallback: check for existing fragments
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/claude-fragments`);
            // Race guard after fetch
            if (JA.app.currentJarId !== jarId) return;
            if (resp.ok) {
                const data = await resp.json();
                const count = Array.isArray(data) ? data.length : Object.keys(data).length;
                if (count > 0) {
                    showPanel();
                    msgEl.innerHTML = `<span class="claude-badge">ENRICHED</span> Claude analysis found — ${count} fragments.`;
                    msgEl.className = 'claude-status claude-status-ok';
                } else {
                    msgEl.innerHTML = 'No Claude analysis data. Run a full scan to correct collections and operations.'
                        + ' <button class="btn-sm btn-explore" onclick="JA.summary._claudeFullScan(false)" style="margin-left:12px">Start Full Scan</button>';
                    msgEl.className = 'claude-status claude-status-none';
                }
            } else {
                msgEl.innerHTML = 'Static analysis only. Run a full Claude scan to correct and verify data.'
                    + ' <button class="btn-sm btn-explore" onclick="JA.summary._claudeFullScan(false)" style="margin-left:12px">Start Full Scan</button>';
                msgEl.className = 'claude-status claude-status-none';
            }
        } catch (e) {
            msgEl.innerHTML = 'Could not check Claude status.';
            msgEl.className = 'claude-status claude-status-none';
        }
    },

    /** Called by polling loop — updates Claude progress UI */
    _updateClaudeProgress(status) {
        const msgEl = document.getElementById('claude-status-msg');
        if (!msgEl) return;
        this._lastClaudeStatus = status;

        if (status.status === 'RUNNING' || status.status === 'COMPLETE' || status.status === 'FAILED') {
            const done = status.completedEndpoints || 0;
            const total = status.totalEndpoints || 0;
            const pct = total > 0 ? Math.round((done / total) * 100) : 0;

            let html = '<div class="claude-progress-live">';
            if (status.status === 'RUNNING') {
                html += '<div class="claude-progress-bar-wrap">';
                html += `<div class="claude-progress-bar" style="width:${pct}%"></div>`;
                html += '</div>';
                html += `<span class="claude-progress-text">Correcting endpoints: ${done}/${total} (${pct}%) — data will be replaced on completion</span>`;
            } else if (status.status === 'COMPLETE') {
                html += `<span class="claude-badge">CORRECTED</span> <span class="claude-progress-text">Full scan complete — ${done} endpoints corrected and applied.</span>`;
            } else {
                const errCount = (status.errors || []).length;
                html += `<span class="claude-badge" style="background:#ef4444">FAILED</span> <span class="claude-progress-text">Scan failed${errCount ? ' (' + errCount + ' errors)' : ''}. Static data unchanged.</span>`;
            }

            // View Logs button
            html += ` <button class="btn-sm" onclick="JA.summary._showClaudeLogs();event.stopPropagation()">View Logs</button>`;

            // Per-endpoint badges (clickable)
            if (status.endpoints) {
                html += '<div class="claude-ep-badges">';
                html += `<input type="text" class="claude-ep-filter" placeholder="Filter..." oninput="JA.summary._filterClaudeBadges(this)">`;
                for (const [key, epStatus] of Object.entries(status.endpoints)) {
                    const short = key.split('.').pop() || key;
                    const safeKey = key.replace(/'/g, "\\'");
                    let cls = 'claude-ep-pending';
                    let icon = '';
                    if (epStatus === 'PROCESSING') { cls = 'claude-ep-processing'; icon = '&#9881; '; }
                    else if (epStatus === 'DONE') { cls = 'claude-ep-done'; icon = '&#10003; '; }
                    else if (epStatus === 'ERROR') { cls = 'claude-ep-error'; icon = '&#10007; '; }
                    html += `<span class="claude-ep-badge ${cls} claude-ep-click" title="${key}" data-ep="${key}" onclick="JA.summary._onClaudeBadgeClick('${safeKey}','${epStatus}');event.stopPropagation()">${icon}${short}</span>`;
                }
                html += '</div>';
            }

            // Error summary
            if (status.errors?.length) {
                html += `<div class="claude-error-summary">${status.errors.length} error(s) — click error badges for details</div>`;
            }
            html += '</div>';
            msgEl.innerHTML = html;
            msgEl.className = 'claude-status claude-status-' + (status.status === 'RUNNING' ? 'running' : status.status === 'COMPLETE' ? 'ok' : 'none');
        }
    },

    /** Filter endpoint badges by text */
    _filterClaudeBadges(input) {
        const q = input.value.toLowerCase();
        input.parentElement.querySelectorAll('.claude-ep-badge').forEach(el => {
            el.style.display = el.dataset.ep.toLowerCase().includes(q) ? '' : 'none';
        });
    },

    /** Handle click on a Claude endpoint badge */
    _onClaudeBadgeClick(epKey, epStatus) {
        if (epStatus === 'DONE') {
            // Jump to that endpoint in the Endpoint Report tab
            this._jumpToEndpoint(epKey);
        } else if (epStatus === 'PROCESSING') {
            this._showEpLog(epKey, 'Currently processing...');
        } else if (epStatus === 'ERROR') {
            // Show error from tracker
            const errors = (this._lastClaudeStatus?.errors || []).filter(e => e.startsWith(epKey));
            this._showEpLog(epKey, errors.length ? errors.join('\n') : 'Error (no details available)');
        } else {
            // PENDING — show queued message
            this._showEpLog(epKey, 'Queued — waiting for processing');
        }
    },

    /** Jump to an endpoint in the Endpoint Report tab */
    _jumpToEndpoint(epKey) {
        // Switch to endpoint report tab
        if (JA.summary.switchSubTab) JA.summary.switchSubTab('ep-report');
        // Find the endpoint by name in epReports
        const reports = this._epReports || [];
        const idx = reports.findIndex(r => r.endpointName === epKey);
        if (idx >= 0) {
            // Navigate pagination to the page containing this endpoint
            const s = this._pageState['sum-ep'];
            if (s) {
                const filtIdx = s.filtered.indexOf(reports[idx]);
                if (filtIdx >= 0) {
                    const page = Math.floor(filtIdx / s.pageSize);
                    s.page = page;
                    this._pageRender('sum-ep');
                    // Scroll to and flash the card after render
                    setTimeout(() => {
                        const cards = document.querySelectorAll('#sum-ep-tbody .ep-card');
                        const cardIdx = filtIdx - page * s.pageSize;
                        if (cards[cardIdx]) {
                            cards[cardIdx].scrollIntoView({ behavior: 'smooth', block: 'center' });
                            cards[cardIdx].classList.add('sum-flash');
                            setTimeout(() => cards[cardIdx].classList.remove('sum-flash'), 1500);
                        }
                    }, 100);
                }
            }
        } else {
            JA.toast?.info('Endpoint not found in current report: ' + epKey);
        }
    },

    /** Show log popup for a single endpoint */
    async _showEpLog(epKey, fallbackMsg) {
        const jarId = JA.app.currentJarId || '';
        let content = fallbackMsg;

        // Try to load fragment files for this endpoint
        if (jarId) {
            try {
                const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/claude-fragments`);
                if (resp.ok) {
                    const frags = await resp.json();
                    const epSafe = epKey.replace(/[^a-zA-Z0-9._-]/g, '_');
                    const related = frags.filter(f => f.name.includes(epSafe));
                    if (related.length) {
                        // Load each related fragment
                        const parts = [];
                        for (const frag of related) {
                            try {
                                const r2 = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/claude-fragments/${encodeURIComponent(frag.name)}`);
                                if (r2.ok) {
                                    const text = await r2.text();
                                    parts.push(`--- ${frag.name} (${frag.size} bytes) ---\n${text}`);
                                }
                            } catch (e) { /* skip */ }
                        }
                        if (parts.length) content = parts.join('\n\n');
                    }
                }
            } catch (e) { /* use fallback */ }
        }

        this._showLogPopup('Endpoint: ' + epKey, content);
    },

    /** Show full Claude enrichment logs (live-updating if running) */
    async _showClaudeLogs() {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }

        // Show popup immediately with loading state
        let overlay = document.getElementById('claude-log-overlay');
        if (overlay) overlay.remove();
        const esc = JA.utils.escapeHtml;
        overlay = document.createElement('div');
        overlay.id = 'claude-log-overlay';
        overlay.className = 'claude-log-overlay';
        overlay.innerHTML = `<div class="claude-log-popup">` +
            `<div class="claude-log-header"><span>Claude Enrichment Logs</span>` +
            `<button class="btn-sm" onclick="document.getElementById('claude-log-overlay').remove()">Close</button></div>` +
            `<pre class="claude-log-pre" id="claude-live-log">Loading...</pre>` +
            `<div class="claude-log-footer">` +
            `<button class="btn-sm" onclick="JA.summary._loadFragmentList()">Show Fragments</button>` +
            `<pre class="claude-log-pre claude-log-frags" id="claude-frag-list" style="display:none;max-height:200px"></pre>` +
            `</div></div>`;
        document.body.appendChild(overlay);

        // Load current status
        try {
            const status = this._lastClaudeStatus || await JA.api.getClaudeProgress(jarId);
            this._updateLiveLog(status);
            // Start polling if running
            if (status.status === 'RUNNING') this._startClaudePoll();
        } catch (e) {
            const pre = document.getElementById('claude-live-log');
            if (pre) pre.textContent = 'Failed to load: ' + e.message;
        }
    },

    /** Load fragment file list into the log popup */
    async _loadFragmentList() {
        const jarId = JA.app.currentJarId || '';
        const el = document.getElementById('claude-frag-list');
        if (!el) return;
        el.style.display = '';
        el.textContent = 'Loading fragments...';
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/claude-fragments`);
            if (resp.ok) {
                const frags = await resp.json();
                let text = `--- Fragment Files (${frags.length}) ---\n`;
                for (const f of frags) text += `${f.name} (${f.size} bytes)\n`;
                el.textContent = text;
            } else {
                el.textContent = 'No fragments found.';
            }
        } catch (e) {
            el.textContent = 'Failed: ' + e.message;
        }
    },

    /** Trigger Claude correction for a single endpoint */
    async _claudeEnrichSingle(endpointName) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }
        const jarName = JA.app._jarName(jarId);
        const esc = JA.utils.escapeHtml;
        const confirmed = await JA.utils.confirm({
            title: 'Correct Endpoint with Claude',
            message: `<p>Run Claude correction on <strong>${esc(endpointName)}</strong>?</p>`
                + `<p>Claude will verify collections and operation types against the decompiled source code. This may take 1-3 minutes.</p>`
                + `<p>Note: This generates correction data only. Use <b>Full Scan</b> to apply corrections to the analysis.</p>`,
            confirmLabel: 'Correct',
            confirmClass: 'confirm-btn-claude'
        });
        if (!confirmed) return;
        try {
            JA.toast?.info('Correcting ' + endpointName + ' in ' + jarName);
            await JA.api.claudeCorrectSingle(jarId, endpointName);
            JA.toast?.success('Correction started for ' + endpointName.split('.').pop() + ' in ' + jarName);
            this._startClaudePoll();
        } catch (e) {
            JA.toast?.error('Correction failed for ' + jarName + ': ' + e.message);
        }
    },

    /** Trigger full Claude correction scan (correct + rotate + merge + save corrected) */
    async _claudeFullScan(resume) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }

        // Check if a scan is already running — warn and cancel if so
        if (!(await JA.app._confirmScanOverride(jarId))) return;

        const jarName = JA.app._jarName(jarId);
        const title = resume ? 'Resume Full Scan' : 'Fresh Full Scan';
        const msg = resume
            ? `<p>Resume Claude correction scan?</p><p>Already-corrected endpoints will be skipped. `
                + `When complete, corrections are applied to the analysis data.</p>`
            : `<p>Run a <strong>fresh full scan</strong> of ALL endpoints?</p>`
                + `<p>Claude will verify collections and operations, and save corrected data separately. The static version is always preserved.</p>`
                + `<p class="confirm-warn">The current corrected version will become the previous version (available for revert).</p>`;
        const confirmed = await JA.utils.confirm({
            title,
            message: msg,
            confirmLabel: resume ? 'Resume Scan' : 'Fresh Scan',
            confirmClass: resume ? 'confirm-btn-claude' : 'confirm-btn-warn'
        });
        if (!confirmed) return;
        try {
            JA.toast?.info((resume ? 'Resuming full scan for ' : 'Starting fresh scan for ') + jarName + '...');
            const result = await JA.api.claudeFullScan(jarId, resume);
            JA.toast?.success('Full scan started for ' + jarName + ' — ' + result.totalEndpoints + ' endpoints queued');
            this._startClaudePoll();
            // Also start header polling for progress bar
            JA.app._showHeaderProgress(true, 0);
            JA.app._startClaudePolling(jarId);
        } catch (e) {
            JA.toast?.error('Failed to start scan for ' + jarName + ': ' + e.message);
        }
    },

    /** Start polling Claude progress (if not already) */
    _startClaudePoll() {
        if (this._claudePollId) return;
        const jarId = JA.app.currentJarId || '';
        this._claudePollId = setInterval(async () => {
            // Stop if user switched to a different JAR
            if (JA.app.currentJarId !== jarId) {
                clearInterval(this._claudePollId);
                this._claudePollId = null;
                return;
            }
            try {
                const status = await JA.api.getClaudeProgress(jarId);
                // Race guard: user may have switched JARs during the await
                if (JA.app.currentJarId !== jarId) {
                    clearInterval(this._claudePollId);
                    this._claudePollId = null;
                    return;
                }
                this._updateClaudeProgress(status);
                // Also update live log popup if open
                this._updateLiveLog(status);
                if (status.status === 'COMPLETE' || status.status === 'FAILED' || status.status === 'IDLE') {
                    clearInterval(this._claudePollId);
                    this._claudePollId = null;
                    if (status.status === 'COMPLETE') {
                        JA.toast?.success('Claude scan complete for ' + JA.app._jarName(jarId) + ' — data corrected');
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
                }
            } catch (e) { /* continue polling */ }
        }, PollConfig.claudeProgressMs);
    },

    /** Update live log popup if it's open */
    _updateLiveLog(status) {
        const pre = document.getElementById('claude-live-log');
        if (!pre) return;
        let log = `Status: ${status.status}  |  ${status.completedEndpoints || 0}/${status.totalEndpoints || 0} endpoints\n`;
        if (status.startedAt) log += `Started: ${status.startedAt}\n`;
        log += '\n';
        if (status.endpoints) {
            for (const [k, v] of Object.entries(status.endpoints)) {
                const icon = v === 'DONE' ? '[OK]' : v === 'PROCESSING' ? '[>>]' : v === 'ERROR' ? '[!!]' : '[  ]';
                log += `${icon} ${k}\n`;
            }
        }
        if (status.errors?.length) {
            log += '\n--- Errors ---\n';
            for (const e of status.errors) log += e + '\n';
        }
        pre.textContent = log;
        // Auto-scroll to bottom
        pre.scrollTop = pre.scrollHeight;
    },

    /** Show per-run correction logs */
    async _showRunLogs() {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }
        try {
            const logs = await JA.api.listRunLogs(jarId);
            if (!logs.length) {
                this._showLogPopup('Run Logs — ' + JA.app._jarName(jarId), 'No run logs yet. Run a full scan to generate logs.');
                return;
            }
            // Show list of logs, clicking one loads it
            let content = 'Available run logs for ' + JA.app._jarName(jarId) + ':\n\n';
            for (const l of logs) {
                content += l.name + '  (' + Math.round(l.size / 1024) + 'KB)\n';
            }
            this._showLogPopup('Run Logs — ' + JA.app._jarName(jarId), content);
            // Add clickable log links
            const pre = document.getElementById('claude-log-content');
            if (pre) {
                let html = '<div style="margin-bottom:12px">Click a log to view:</div>';
                for (const l of logs) {
                    const safeName = l.name.replace(/'/g, "\\'");
                    html += '<div style="margin:4px 0"><a href="#" onclick="JA.summary._loadRunLog(\'' + safeName + '\');return false" '
                        + 'style="color:#8b5cf6;text-decoration:underline">' + JA.utils.escapeHtml(l.name)
                        + '</a> <span style="color:#888">(' + Math.round(l.size / 1024) + 'KB)</span></div>';
                }
                pre.innerHTML = html;
            }
        } catch (e) {
            this._showLogPopup('Run Logs', 'Failed to load: ' + e.message);
        }
    },

    /** Load and display a specific run log */
    async _loadRunLog(logName) {
        const jarId = JA.app.currentJarId || '';
        const pre = document.getElementById('claude-log-content');
        if (!pre) return;
        pre.textContent = 'Loading ' + logName + '...';
        try {
            const content = await JA.api.getRunLog(jarId, logName);
            pre.textContent = content;
        } catch (e) {
            pre.textContent = 'Failed to load: ' + e.message;
        }
    },

    /** Generic log popup */
    _showLogPopup(title, content) {
        let overlay = document.getElementById('claude-log-overlay');
        if (overlay) overlay.remove();
        const esc = JA.utils.escapeHtml;
        overlay = document.createElement('div');
        overlay.id = 'claude-log-overlay';
        overlay.className = 'claude-log-overlay';
        overlay.innerHTML = `<div class="claude-log-popup">` +
            `<div class="claude-log-header"><span>${esc(title)}</span>` +
            `<button class="btn-sm" onclick="document.getElementById('claude-log-overlay').remove()">Close</button></div>` +
            `<pre class="claude-log-pre" id="claude-log-content">${esc(content)}</pre></div>`;
        document.body.appendChild(overlay);
    },

    // ── Claude Usage Stats Panel ──────────────────────────────────────

    /** Format milliseconds into a human-readable duration string */
    _formatDuration(ms) {
        if (ms == null || ms <= 0) return '0s';
        if (ms < 1000) return ms + 'ms';
        const totalSec = Math.round(ms / 1000);
        if (totalSec < 60) return totalSec + 's';
        const min = Math.floor(totalSec / 60);
        const sec = totalSec % 60;
        return sec > 0 ? min + 'm ' + sec + 's' : min + 'm';
    },

    /** Format KB to MB when appropriate */
    _formatSizeKb(kb) {
        if (kb == null || kb <= 0) return '0 KB';
        if (kb >= 1024) return (kb / 1024).toFixed(1) + ' MB';
        return Math.round(kb) + ' KB';
    },

    /** Format ISO datetime into short display format */
    _formatShortDate(isoStr) {
        if (!isoStr) return '-';
        try {
            const d = new Date(isoStr);
            const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
            return d.getDate() + ' ' + months[d.getMonth()] + ' ' + String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
        } catch (e) { return isoStr; }
    },

    /** Render the Claude Usage Stats panel into a container element */
    _renderClaudeUsagePanel(stats, container) {
        if (!stats || !stats.totalCalls) {
            container.innerHTML = '<div class="claude-usage-panel"><div class="claude-usage-empty">No Claude API calls recorded.</div></div>';
            return;
        }

        const esc = JA.utils.escapeHtml;
        let html = '<div class="claude-usage-panel">';
        html += '<div class="claude-usage-header"><span class="claude-badge">USAGE</span> <span class="claude-title">Claude API Call Statistics</span></div>';

        // Metric cards
        html += '<div class="claude-usage-cards">';
        html += this._renderUsageCard('Total API Calls', stats.totalCalls, false);
        html += this._renderUsageCard('Total Sessions', stats.sessionCount || 0, false);
        html += this._renderUsageCard('Avg Call Duration', this._formatDuration(stats.avgDurationMs), false);
        html += this._renderUsageCard('Total Prompt Size', this._formatSizeKb(stats.totalPromptSizeKb), false);
        html += this._renderUsageCard('Total Response Size', this._formatSizeKb(stats.totalResponseSizeKb), false);
        html += this._renderUsageCard('Errors', stats.errorCount || 0, (stats.errorCount || 0) > 0);
        html += '</div>';

        // Sessions table
        if (stats.sessions && stats.sessions.length) {
            html += '<table class="claude-sessions-table">';
            html += '<thead><tr><th>Session ID</th><th>Type</th><th>Calls</th><th>Duration</th><th>Started</th><th>Status</th></tr></thead>';
            html += '<tbody>';
            for (const s of stats.sessions) {
                const sid = esc(s.sessionId || '');
                const shortSid = sid.length > 10 ? sid.substring(0, 10) + '...' : sid;
                const dur = this._formatDuration(s.durationMs);
                const started = this._formatShortDate(s.startedAt);
                const hasEnd = !!s.endedAt;
                const status = hasEnd ? 'OK' : 'Running';
                const statusCls = hasEnd ? 'claude-status-ok-cell' : 'claude-status-running-cell';
                html += '<tr class="claude-session-row" data-session-id="' + sid + '" onclick="JA.summary._toggleClaudeSessionDetail(this, \'' + sid.replace(/'/g, "\\'") + '\')">';
                html += '<td title="' + sid + '">' + shortSid + '</td>';
                html += '<td>' + esc(s.sessionType || '-') + '</td>';
                html += '<td>' + (s.callCount || 0) + '</td>';
                html += '<td>' + dur + '</td>';
                html += '<td>' + started + '</td>';
                html += '<td class="' + statusCls + '">' + status + '</td>';
                html += '</tr>';
                // Placeholder for detail expansion
                html += '<tr class="claude-session-detail-row" data-detail-for="' + sid + '" style="display:none"><td colspan="6"><div class="claude-session-detail">Loading...</div></td></tr>';
            }
            html += '</tbody></table>';
        }

        html += '</div>';
        container.innerHTML = html;
    },

    /** Render a single usage metric card */
    _renderUsageCard(label, value, isError) {
        const valCls = isError ? 'value error' : 'value';
        return '<div class="claude-usage-card"><div class="label">' + JA.utils.escapeHtml(label) + '</div><div class="' + valCls + '">' + JA.utils.escapeHtml(String(value)) + '</div></div>';
    },

    /** Toggle session detail expansion — fetch call records on first open */
    async _toggleClaudeSessionDetail(rowEl, sessionId) {
        const detailRow = rowEl.nextElementSibling;
        if (!detailRow || !detailRow.classList.contains('claude-session-detail-row')) return;

        const isVisible = detailRow.style.display !== 'none';
        if (isVisible) {
            detailRow.style.display = 'none';
            return;
        }

        detailRow.style.display = '';
        const detailDiv = detailRow.querySelector('.claude-session-detail');

        // If already loaded, just show
        if (detailDiv.dataset.loaded) return;

        // Fetch session detail
        const jarId = JA.app.currentJarId || '';
        const jarName = JA.app._jarName(jarId);
        const calls = await JA.api.getClaudeSessionDetail(jarName, sessionId);

        if (!calls || !calls.length) {
            detailDiv.innerHTML = '<em>No call details available for this session.</em>';
            detailDiv.dataset.loaded = '1';
            return;
        }

        const esc = JA.utils.escapeHtml;
        let html = '<table><thead><tr><th>#</th><th>Endpoint</th><th>Chunk</th><th>Prompt KB</th><th>Response KB</th><th>Duration</th><th>Status</th></tr></thead><tbody>';
        calls.forEach((c, i) => {
            const statusCls = c.error ? 'claude-call-error' : '';
            html += '<tr class="' + statusCls + '">';
            html += '<td>' + (i + 1) + '</td>';
            html += '<td>' + esc(c.endpoint || c.endpointName || '-') + '</td>';
            html += '<td>' + (c.chunk != null ? c.chunk : '-') + '</td>';
            html += '<td>' + (c.promptSizeKb != null ? c.promptSizeKb.toFixed(1) : '-') + '</td>';
            html += '<td>' + (c.responseSizeKb != null ? c.responseSizeKb.toFixed(1) : '-') + '</td>';
            html += '<td>' + this._formatDuration(c.durationMs) + '</td>';
            html += '<td>' + (c.error ? '<span style="color:#ef4444">ERROR</span>' : 'OK') + '</td>';
            html += '</tr>';
        });
        html += '</tbody></table>';
        detailDiv.innerHTML = html;
        detailDiv.dataset.loaded = '1';
    },

    /** View analysis prompt/response logs for a specific endpoint */
    async _viewAnalysisLogs(endpointName) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }
        const esc = JA.utils.escapeHtml;

        try {
            // Fetch fragment index
            const resp = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-fragments');
            if (!resp.ok) { JA.toast?.info('No analysis fragments available'); return; }
            const fragments = await resp.json();
            if (!fragments || !fragments.length) {
                JA.toast?.info('No analysis fragments found. Run an analysis first.');
                return;
            }

            // Load _index.json to map trace IDs to endpoint names
            const indexFrag = fragments.find(f => f.name === '_index.json');
            let traceIndex = null;
            if (indexFrag) {
                try {
                    const idxResp = await fetch('/api/jar/jars/' + encodeURIComponent(jarId) + '/claude-fragments/_index.json');
                    if (idxResp.ok) traceIndex = await idxResp.json();
                } catch (e) { /* index unavailable, fall back to name matching */ }
            }

            // Find matching trace IDs for this endpoint
            const matchingTraceIds = new Set();
            if (traceIndex && traceIndex.fragments) {
                for (const [tid, info] of Object.entries(traceIndex.fragments)) {
                    const ep = info.endpoint || info.endpointName || info.method || '';
                    if (ep === endpointName || ep.endsWith('.' + endpointName.split('.').pop())) {
                        matchingTraceIds.add(tid);
                    }
                }
            }

            // Filter fragment files: match by trace ID from index, or by sanitized name
            const epSafe = endpointName.replace(/[^a-zA-Z0-9._-]/g, '_');
            const matchingFiles = fragments.filter(f => {
                const name = f.name || '';
                if (name.startsWith('_')) return false; // skip _index.json, _meta.json
                // Match by trace ID prefix (e.g., E001_input.json matches traceId E001)
                if (matchingTraceIds.size) {
                    for (const tid of matchingTraceIds) {
                        if (name.startsWith(tid + '_') || name === tid + '.json') return true;
                    }
                }
                // Fallback: match by endpoint name in filename
                return name.includes(epSafe);
            });

            if (!matchingFiles.length) {
                JA.toast?.info('No analysis logs found for ' + endpointName);
                return;
            }

            // Fetch content for each matching fragment
            const logs = [];
            for (const frag of matchingFiles) {
                try {
                    const content = await fetch('/api/jar/jars/' + encodeURIComponent(jarId)
                        + '/claude-fragments/' + encodeURIComponent(frag.name)).then(r => r.text());
                    const name = frag.name;
                    const isInput = name.includes('input');
                    const isOutput = name.includes('output');
                    const isError = name.includes('error');
                    logs.push({
                        label: name.replace(/\.(json|txt)$/, ''),
                        type: isInput ? 'prompt' : isOutput ? 'response' : isError ? 'error' : 'other',
                        content: content || '(empty)',
                        size: content ? content.length : 0,
                        filename: name
                    });
                } catch (e) { /* skip unreadable */ }
            }

            if (!logs.length) {
                JA.toast?.info('No analysis logs found for ' + endpointName);
                return;
            }

            // Sort: inputs before outputs, then by filename
            logs.sort((a, b) => {
                if (a.type !== b.type) {
                    const order = { prompt: 0, response: 1, error: 2, other: 3 };
                    return (order[a.type] || 9) - (order[b.type] || 9);
                }
                return a.filename.localeCompare(b.filename);
            });

            // Build modal (reuse correction log styling)
            let html = '<div class="corr-log-overlay" onclick="if(event.target===this)this.remove()">';
            html += '<div class="corr-log-modal">';
            html += '<div class="corr-log-header">';
            html += '<span class="claude-badge">ANALYSIS LOGS</span>';
            html += '<span class="corr-log-title">' + esc(endpointName) + '</span>';
            html += '<button class="corr-log-close" onclick="this.closest(\'.corr-log-overlay\').remove()">&times;</button>';
            html += '</div>';

            // Tab buttons
            html += '<div class="corr-log-tabs">';
            logs.forEach((log, i) => {
                const sizeKb = (log.size / 1024).toFixed(1);
                const cls = log.type === 'prompt' ? 'corr-log-tab-prompt'
                          : log.type === 'error' ? 'corr-log-tab-prompt'
                          : 'corr-log-tab-response';
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

            document.querySelector('.corr-log-overlay')?.remove();
            document.body.insertAdjacentHTML('beforeend', html);
        } catch (e) {
            JA.toast?.error('Failed to load analysis logs: ' + e.message);
        }
    }
});
