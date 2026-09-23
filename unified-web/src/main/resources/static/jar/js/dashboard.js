/**
 * JAR Dashboard — overview of all analyzed JARs with stats and Claude status.
 * Shown in the main content area when no JAR is selected.
 */
window.JA = window.JA || {};

JA.dashboard = {

    render(jars) {
        const container = document.getElementById('dashboard-content');
        if (!container) return;
        if (!jars || !jars.length) {
            container.innerHTML = this._renderEmpty();
            return;
        }
        container.innerHTML = this._renderDashboard(jars);
    },

    _renderEmpty() {
        const label = JA.artifactLabel();
        return `<div class="dash-empty">
            <p>No ${label}s analyzed yet. Upload a ${label} to get started.</p>
        </div>`;
    },

    _renderDashboard(jars) {
        const esc = JA.utils.escapeHtml;

        // Aggregate stats
        let totalClasses = 0, totalEndpoints = 0, totalSize = 0;
        let claudeRunning = 0, claudeComplete = 0;
        for (const j of jars) {
            totalClasses += j.totalClasses || 0;
            totalEndpoints += j.totalEndpoints || 0;
            totalSize += j.jarSize || 0;
            if (j.claudeStatus === 'RUNNING') claudeRunning++;
            if (j.claudeStatus === 'COMPLETE') claudeComplete++;
        }

        let html = '<div class="dash-page">';

        // Stats bar
        html += '<div class="dash-stats">';
        html += this._stat(jars.length, JA.artifactLabel() + 's');
        html += this._stat(totalClasses, 'Classes');
        html += this._stat(totalEndpoints, 'Endpoints');
        html += this._stat(JA.utils.formatSize(totalSize), 'Total Size');
        if (claudeRunning > 0) html += this._stat(claudeRunning, 'Claude Running', 'dash-stat-running');
        if (claudeComplete > 0) html += this._stat(claudeComplete, 'Claude Done', 'dash-stat-ok');
        html += '</div>';

        // Actions
        html += '<div class="dash-actions">';
        html += `<button class="btn-sm" onclick="JA.sessions.show()">Claude Sessions</button>`;
        html += `<button class="btn-sm" onclick="JA.dashboard.togglePerf()">Performance Summary</button>`;
        html += '</div>';

        // Performance Summary — mirrors plsql/js/app.js's PA.homeMenu._renderPerf (aggregate stats
        // across every analyzed JAR), collapsed by default; toggled via the button above.
        // Phases table appended below the aggregate stat block once loaded (mirrors PL/SQL's
        // parser/js/perfSummary.js "Phases (ms)" column — same field names, parseMs/discoverMs/
        // saveMs/totalMs, straight off QueueJob via GET /api/queue, so JAR and WAR analyses show
        // the same parse/discover/save breakdown as PL/SQL rather than a different UI).
        html += '<div id="dash-perf-panel" style="display:none;margin-bottom:16px">'
              + this._renderPerfPanel(jars)
              + '<div id="dash-perf-phases" style="margin-top:10px"></div>'
              + '</div>';

        // JAR cards
        html += '<div class="dash-jars">';
        for (const j of jars) {
            html += this._renderJarCard(j, esc);
        }
        html += '</div>';

        // Claude Usage Stats — loaded async after render
        html += '<div id="dash-claude-usage-wrap" style="margin-top:20px"></div>';

        html += '</div>';

        // Fetch and render Claude stats after dashboard is in DOM
        setTimeout(() => this._loadClaudeStats(jars), 100);

        return html;
    },

    async _loadClaudeStats(jars) {
        const wrap = document.getElementById('dash-claude-usage-wrap');
        if (!wrap) return;
        wrap.innerHTML = '<div style="color:var(--text-muted);font-size:12px;padding:8px 0">Loading Claude stats…</div>';

        try {
            const results = await Promise.allSettled(
                jars.map(j => JA.api.getClaudeStats(j.id).then(s => ({ jar: j, stats: s })))
            );

            let totalCalls = 0, totalSessions = 0, totalPromptKb = 0, totalResponseKb = 0;
            let totalErrors = 0, totalDurMs = 0, durCount = 0;
            const allSessions = [];

            for (const r of results) {
                if (r.status !== 'fulfilled' || !r.value.stats) continue;
                const s = r.value.stats;
                const jarName = r.value.jar.projectName || r.value.jar.jarName || r.value.jar.id;
                totalCalls      += s.totalCalls         || 0;
                totalSessions   += s.sessionCount       || 0;
                totalPromptKb   += s.totalPromptSizeKb  || 0;
                totalResponseKb += s.totalResponseSizeKb|| 0;
                totalErrors     += s.errorCount         || 0;
                if (s.avgDurationMs) { totalDurMs += s.avgDurationMs; durCount++; }
                if (s.sessions) {
                    for (const sess of s.sessions) allSessions.push({ _jarName: jarName, ...sess });
                }
            }

            if (totalCalls === 0) { wrap.innerHTML = ''; return; }

            const avgDurMs = durCount > 0 ? Math.round(totalDurMs / durCount) : 0;
            const esc = JA.utils.escapeHtml;

            // Store for modal reuse
            JA.dashboard._claudeStatsCache = { totalCalls, totalSessions, avgDurMs, totalPromptKb, totalResponseKb, totalErrors, allSessions };

            // Compact summary strip with "View Details" button
            const errBadge = totalErrors > 0
                ? `<span style="background:var(--badge-red-bg);color:var(--badge-red);padding:1px 8px;border-radius:var(--radius-sm);font-weight:700">${totalErrors} errors</span>`
                : '';
            wrap.innerHTML =
                '<div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;padding:10px 14px;background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius-md);border-left:3px solid var(--accent-hover)">' +
                '<span class="claude-badge" style="background:var(--accent-hover)">USAGE</span>' +
                '<span style="font-size:13px;font-weight:700;color:var(--text)">Claude API Statistics</span>' +
                '<span style="font-size:12px;color:var(--text-muted)">' + totalCalls + ' calls · ' + totalSessions + ' sessions · ' + JA.summary._formatSizeKb(totalPromptKb) + ' prompt</span>' +
                (errBadge ? errBadge : '') +
                '<button onclick="JA.dashboard._openStatsModal()" style="margin-left:auto;padding:4px 14px;font-size:12px;font-weight:600;background:var(--accent-hover);color:var(--text-inverse);border:none;border-radius:var(--radius);cursor:pointer">View Details</button>' +
                '</div>';
        } catch (e) {
            wrap.innerHTML = '';
        }
    },

    _openStatsModal() {
        const d = JA.dashboard._claudeStatsCache;
        if (!d) return;
        const esc = JA.utils.escapeHtml;

        // Remove stale modal
        const old = document.getElementById('claudeStatsModal');
        if (old) old.remove();

        const el = document.createElement('div');
        el.id = 'claudeStatsModal';
        el.style.cssText = 'position:fixed;inset:0;z-index:19000;background:var(--bg-overlay);backdrop-filter:blur(2px);display:flex;align-items:center;justify-content:center;padding:16px';
        el.addEventListener('click', e => { if (e.target === el) el.remove(); });

        // KPI cards
        // bg MUST be the tinted --badge-*-bg token and accent the saturated --badge-* token.
        // Previously several cards used the same solid token for both, e.g. background var(--blue)
        // with color var(--blue) — blue text on blue, i.e. an unreadable solid block. The paired
        // badge tokens are designed for exactly this and stay legible in light and dark.
        const cards = [
            { label: 'Total API Calls',      value: d.totalCalls,                              bg: 'var(--badge-blue-bg)',   accent: 'var(--badge-blue)' },
            { label: 'Total Sessions',       value: d.totalSessions,                           bg: 'var(--badge-green-bg)',  accent: 'var(--badge-green)' },
            { label: 'Avg Call Duration',    value: JA.summary._formatDuration(d.avgDurMs),    bg: 'var(--badge-teal-bg)',   accent: 'var(--badge-teal)' },
            { label: 'Total Prompt Size',    value: JA.summary._formatSizeKb(d.totalPromptKb), bg: 'var(--badge-purple-bg)', accent: 'var(--badge-purple)' },
            { label: 'Total Response Size',  value: JA.summary._formatSizeKb(d.totalResponseKb), bg: 'var(--badge-orange-bg)', accent: 'var(--badge-orange)' },
            { label: 'Errors',               value: d.totalErrors,
              bg: d.totalErrors > 0 ? 'var(--badge-red-bg)' : 'var(--badge-gray-bg)',
              accent: d.totalErrors > 0 ? 'var(--badge-red)' : 'var(--badge-gray)' },
        ];
        let kpiHtml = '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px">';
        for (const c of cards) {
            kpiHtml += `<div style="flex:1;min-width:110px;background:${c.bg};border-radius:var(--radius-md);padding:14px 16px;border:1px solid var(--border)">` +
                `<div style="font-size:10px;text-transform:uppercase;letter-spacing:0.5px;color:${c.accent};margin-bottom:4px">${esc(c.label)}</div>` +
                `<div style="font-size:22px;font-weight:700;color:${c.accent}">${esc(String(c.value))}</div>` +
                `</div>`;
        }
        kpiHtml += '</div>';

        // Sessions table
        let tableHtml = '';
        if (d.allSessions.length) {
            const sorted = [...d.allSessions].sort((a, b) => (b.startedAt || '').localeCompare(a.startedAt || ''));
            // Tint background + saturated foreground, never two saturated tokens together.
            const typeColors = {
                CORRECT: { bg: 'var(--badge-blue-bg)',   fg: 'var(--badge-blue)' },
                ENRICH:  { bg: 'var(--badge-green-bg)',  fg: 'var(--badge-green)' },
                RESCAN:  { bg: 'var(--badge-purple-bg)', fg: 'var(--badge-purple)' },
                VERIFY:  { bg: 'var(--badge-teal-bg)',   fg: 'var(--badge-teal)' }
            };
            tableHtml = '<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:12px">';
            tableHtml += '<thead><tr style="background:var(--sidebar-bg)">';
            for (const h of ['JAR', 'Session ID', 'Type', 'Calls', 'Duration', 'Started', 'Status']) {
                tableHtml += `<th style="padding:8px 12px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted);border-bottom:1px solid var(--sidebar-border);white-space:nowrap">${h}</th>`;
            }
            tableHtml += '</tr></thead><tbody>';
            for (let i = 0; i < sorted.length; i++) {
                const s = sorted[i];
                const sid = esc(s.sessionId || '');
                const shortSid = sid.length > 12 ? sid.substring(0, 12) + '…' : sid;
                const dur = JA.summary._formatDuration(s.durationMs);
                const started = JA.summary._formatShortDate(s.startedAt);
                const hasEnd = !!s.endedAt;
                // Both arms were --sidebar-bg, so the intended zebra striping never rendered.
                const rowBg = i % 2 === 0 ? 'var(--sidebar-bg)' : 'var(--bg-card)';
                const typeKey = (s.sessionType || '').toUpperCase();
                const tc = typeColors[typeKey] || { bg: 'var(--sidebar-bg)', fg: 'var(--text-muted)' };
                // Paired tint + ink. Previously bg and fg were both --green
                // (green-on-green, 1:1) for the running state.
                const statusBg = hasEnd ? 'var(--badge-green-bg)' : 'var(--badge-blue-bg)';
                const statusFg = hasEnd ? 'var(--badge-green)'    : 'var(--badge-blue)';
                tableHtml += `<tr style="background:${rowBg}">`;
                tableHtml += `<td style="padding:8px 12px;color: var(--text-secondary);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;border-bottom:1px solid var(--sidebar-border)" title="${esc(s._jarName)}">${esc(s._jarName)}</td>`;
                tableHtml += `<td style="padding:8px 12px;color:var(--text-muted);font-family:monospace;border-bottom:1px solid var(--sidebar-border)" title="${sid}">${shortSid}</td>`;
                tableHtml += `<td style="padding:8px 12px;border-bottom:1px solid var(--sidebar-border)"><span style="background:${tc.bg};color:${tc.fg};padding:2px 8px;border-radius:var(--radius-sm);font-size:10px;font-weight:700">${esc(s.sessionType || '-')}</span></td>`;
                tableHtml += `<td style="padding:8px 12px;color:var(--text);font-weight:600;text-align:right;border-bottom:1px solid var(--sidebar-border)">${s.callCount || 0}</td>`;
                tableHtml += `<td style="padding:8px 12px;color: var(--text-secondary);border-bottom:1px solid var(--sidebar-border)">${dur}</td>`;
                tableHtml += `<td style="padding:8px 12px;color:var(--text-muted);white-space:nowrap;border-bottom:1px solid var(--sidebar-border)">${started}</td>`;
                tableHtml += `<td style="padding:8px 12px;border-bottom:1px solid var(--sidebar-border)"><span style="background:${statusBg};color:${statusFg};padding:2px 8px;border-radius:var(--radius-sm);font-size:10px;font-weight:700">${hasEnd ? 'OK' : 'Running'}</span></td>`;
                tableHtml += '</tr>';
            }
            tableHtml += '</tbody></table></div>';
        }

        el.innerHTML =
            '<div style="background:var(--sidebar-bg);border-radius:var(--radius-xl);width:900px;max-width:96vw;max-height:88vh;display:flex;flex-direction:column;box-shadow: var(--shadow-lg);border:1px solid var(--sidebar-border)">' +
            '  <div style="display:flex;align-items:center;padding:14px 20px;border-bottom:1px solid var(--sidebar-border)">' +
            '    <span class="claude-badge" style="background:var(--accent-hover);margin-right:10px">USAGE</span>' +
            '    <span style="flex:1;font-size:14px;font-weight:700;color:var(--text)">Claude API Call Statistics</span>' +
            '    <button onclick="document.getElementById(\'claudeStatsModal\').remove()" style="background:none;border:none;color:var(--text-muted);font-size:18px;cursor:pointer;padding:0 4px;line-height:1">&times;</button>' +
            '  </div>' +
            '  <div style="padding:20px;overflow-y:auto">' +
            kpiHtml + tableHtml +
            '  </div>' +
            '</div>';

        document.body.appendChild(el);
    },

    _usageCard(label, value, isError) {
        const esc = JA.utils.escapeHtml;
        const valCls = isError ? 'value error' : 'value';
        return '<div class="claude-usage-card"><div class="label">' + esc(label) + '</div><div class="' + valCls + '">' + esc(String(value)) + '</div></div>';
    },

    togglePerf() {
        const el = document.getElementById('dash-perf-panel');
        if (!el) return;
        const opening = el.style.display === 'none';
        el.style.display = opening ? 'block' : 'none';
        if (opening) JA.dashboard._loadPhasesTable();
    },

    /**
     * Per-analysis parse/discover/save/total breakdown, sourced from completed QueueJob history
     * (GET /api/queue) rather than the JAR/WAR listing endpoint, since that breakdown lives only
     * on the job (JarAnalysisExecutor/WarAnalysisExecutor attach it via QueueJob#setTiming).
     * Same table shape/columns as PL/SQL's Performance Summary "Phases (ms)" column.
     */
    async _loadPhasesTable() {
        const host = document.getElementById('dash-perf-phases');
        if (!host) return;
        host.innerHTML = '<div style="color:var(--text-muted);font-size:11px;padding:6px 0">Loading phase timings…</div>';
        try {
            const res = await fetch('/api/queue?kind=JAR_UPLOAD,JAR_TARGETED,WAR_UPLOAD&historyLimit=50');
            if (!res.ok) { host.innerHTML = ''; return; }
            const data = await res.json();
            const rows = (data.history || []).filter(j =>
                (j.status || '').toUpperCase() === 'COMPLETE' && (j.parseMs != null || j.discoverMs != null));
            if (!rows.length) { host.innerHTML = ''; return; }

            let html = '<table style="width:100%;border-collapse:collapse;font-size:11px">';
            html += '<thead><tr style="text-align:left;color:var(--text-muted)">'
                  + '<th style="padding:4px 6px">Name</th><th style="padding:4px 6px">Type</th>'
                  + '<th style="padding:4px 6px">Parse</th><th style="padding:4px 6px">Discover</th>'
                  + '<th style="padding:4px 6px">Save</th><th style="padding:4px 6px">Total</th></tr></thead><tbody>';
            for (const j of rows) {
                html += '<tr style="border-top:1px solid var(--border-subtle)">'
                      + '<td style="padding:4px 6px">' + JA.utils.escapeHtml(j.resultName || j.displayName || j.id) + '</td>'
                      + '<td style="padding:4px 6px">' + JA.utils.escapeHtml((j.type || '').replace('_UPLOAD', '').replace('_TARGETED', '')) + '</td>'
                      + '<td style="padding:4px 6px">' + (j.parseMs != null ? j.parseMs + 'ms' : '—') + '</td>'
                      + '<td style="padding:4px 6px">' + (j.discoverMs != null ? j.discoverMs + 'ms' : '—') + '</td>'
                      + '<td style="padding:4px 6px">' + (j.saveMs != null ? j.saveMs + 'ms' : '—') + '</td>'
                      + '<td style="padding:4px 6px;font-weight:700">' + (j.totalMs != null ? j.totalMs + 'ms' : '—') + '</td>'
                      + '</tr>';
            }
            html += '</tbody></table>';
            host.innerHTML = '<div style="font-weight:700;font-size:.82rem;color:var(--text);margin:10px 0 6px">Phase Timing (parse / discover / save)</div>' + html;
        } catch (e) {
            host.innerHTML = '<div style="color:var(--text-muted);font-size:11px">Phase timing unavailable</div>';
        }
    },

    /** Aggregate performance stats across every analyzed JAR — same stat-row shape/tokens as
     *  plsql/js/app.js's PA.homeMenu._renderPerf (Total Analyses / Nodes / Tables / LOC / Avg Crawl
     *  Time), adapted to JAR's own fields (classes/endpoints/size/analysis duration) since JAR has
     *  no notion of "tables"/"call depth". */
    _renderPerfPanel(jars) {
        if (!jars || !jars.length) {
            return '<div style="color:var(--text-muted);font-size:.82rem;padding:8px 0">No analyses to summarize.</div>';
        }
        const total = jars.length;
        const totalClasses = jars.reduce((s, j) => s + (j.totalClasses || 0), 0);
        const totalEndpoints = jars.reduce((s, j) => s + (j.totalEndpoints || 0), 0);
        const totalSize = jars.reduce((s, j) => s + (j.jarSize || 0), 0);
        const avgClasses = total > 0 ? (totalClasses / total).toFixed(1) : 0;
        const avgEndpoints = total > 0 ? (totalEndpoints / total).toFixed(1) : 0;
        const durations = jars.filter(j => j.analysisDurationMs > 0).map(j => j.analysisDurationMs);
        const avgDurationMs = durations.length ? durations.reduce((s, d) => s + d, 0) / durations.length : 0;
        const claudeComplete = jars.filter(j => j.claudeStatus === 'COMPLETE').length;

        const stat = (label, value, unit = '') =>
            `<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--border-subtle);font-size:.82rem">
                <span style="color:var(--text-muted)">${label}</span>
                <span style="font-weight:700;color:var(--text)">${value}<span style="font-weight:400;color:var(--text-muted);font-size:.75rem"> ${unit}</span></span>
            </div>`;

        return `<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius-md);padding:14px 16px">
            <div style="font-weight:700;font-size:.9rem;color:var(--text);margin-bottom:6px">Performance Summary</div>
            ${stat('Total ' + JA.artifactLabel() + 's Analyzed', total)}
            ${stat('Total Classes', totalClasses.toLocaleString())}
            ${stat('Total Endpoints', totalEndpoints.toLocaleString())}
            ${stat('Avg Classes / ' + JA.artifactLabel(), avgClasses)}
            ${stat('Avg Endpoints / ' + JA.artifactLabel(), avgEndpoints)}
            ${stat('Total Size', JA.utils.formatSize(totalSize))}
            ${stat('Avg Analysis Time', avgDurationMs > 0 ? (avgDurationMs / 1000).toFixed(1) : '–', avgDurationMs > 0 ? 's' : '')}
            ${stat('Claude Scans Complete', claudeComplete)}
        </div>`;
    },

    _stat(num, label, cls) {
        return `<div class="dash-stat-card ${cls || ''}">
            <div class="dash-stat-num">${num}</div>
            <div class="dash-stat-lbl">${label}</div>
        </div>`;
    },

    _renderJarCard(j, esc) {
        const id = j.id;
        const safeId = (id || '').replace(/'/g, "\\'");
        const claudeStatus = j.claudeStatus || 'IDLE';
        const claudeCls = 'dash-claude-' + claudeStatus.toLowerCase();

        let html = `<div class="dash-jar-card" onclick="JA.app.selectJar('${safeId}')">`;

        // Header
        html += '<div class="dash-jar-header">';
        html += `<span class="dash-jar-name">${esc(j.projectName || j.jarName || id)}</span>`;
        if (claudeStatus !== 'IDLE') {
            html += `<span class="dash-claude-badge ${claudeCls}">${esc(claudeStatus)}</span>`;
        }
        if (j.analysisMode === 'CORRECTED') {
            const iter = j.claudeIteration || '';
            const iterLabel = iter > 1 ? ' (iter ' + iter + ')' : '';
            html += '<span class="jar-mode-badge jar-mode-corrected">CORRECTED' + iterLabel + '</span>';
        }
        html += '</div>';

        // Stats
        html += '<div class="dash-jar-stats">';
        html += `<span>${j.totalClasses || 0} classes</span>`;
        html += `<span>${j.totalEndpoints || 0} endpoints</span>`;
        if (j.jarSize) html += `<span>${JA.utils.formatSize(j.jarSize)}</span>`;
        if (j.hasConnections) html += `<span class="dash-conn-badge" title="DB connections found in config">🔗 Connections</span>`;
        html += '</div>';

        // Claude progress
        if (claudeStatus === 'RUNNING' && j.claudeTotal > 0) {
            const pct = Math.round((j.claudeCompleted / j.claudeTotal) * 100);
            html += '<div class="dash-jar-progress">';
            html += `<div class="dash-progress-bar"><div class="dash-progress-fill" style="width:${pct}%"></div></div>`;
            html += `<span class="dash-progress-text">${j.claudeCompleted}/${j.claudeTotal} (${pct}%)</span>`;
            html += '</div>';
        }

        // Date
        if (j.analyzedAt) {
            html += `<div class="dash-jar-date">${this._formatDate(j.analyzedAt)}</div>`;
        }

        // Actions
        html += '<div class="dash-jar-actions">';
        html += `<button class="btn-sm btn-explore" onclick="JA.app.selectJar('${safeId}');event.stopPropagation()">Open</button>`;
        // §B4: both start a request, so the card's button is disabled until it settles. The
        // stopPropagation stays first so the card's own onclick (selectJar) never also fires.
        html += `<button class="btn-sm" onclick="event.stopPropagation();UIBusy.run(this, function(){ return JA.dashboard.triggerClaude('${safeId}'); })">Claude</button>`;
        html += `<button class="btn-sm" onclick="event.stopPropagation();UIBusy.run(this, function(){ return JA.app.deleteJar('${safeId}'); })" style="color:var(--red)">Delete</button>`;
        html += '</div>';

        html += '</div>';
        return html;
    },

    async triggerClaude(jarId) {
        // Re-entry guard as well as the element guard: the card is re-rendered by loadJars, so the
        // node that was disabled may be replaced while the confirm dialog is still open.
        if (this._starting.has(jarId)) return;
        this._starting.add(jarId);
        try {
            await this._triggerClaudeInner(jarId);
        } finally {
            this._starting.delete(jarId);
        }
    },

    /** jar ids with a Claude scan start in flight — see triggerClaude */
    _starting: new Set(),

    async _triggerClaudeInner(jarId) {
        // Check if a scan is already running for this JAR
        if (!(await JA.app._confirmScanOverride(jarId))) return;

        const confirmed = await JA.utils.confirm({
            title: 'Full Claude Correction Scan',
            message: `<p>Run full Claude scan on <strong>${JA.utils.escapeHtml(jarId)}</strong>?</p>`
                + `<p>Claude will verify each endpoint's collections and operation types `
                + `and save corrected data separately.</p>`
                + `<p>The static version is always preserved. You can switch between versions at any time.</p>`,
            confirmLabel: 'Start Full Scan',
            confirmClass: 'confirm-btn-claude'
        });
        if (!confirmed) return;

        try {
            const j = (JA.app.jars || []).find(j => j.id === jarId);
            const name = j ? (j.projectName || j.jarName || jarId) : jarId;
            const result = await JA.api.claudeFullScan(jarId, false);
            JA.toast?.success('Full scan started for ' + name + ' — ' + result.totalEndpoints + ' endpoints queued');
        } catch (e) {
            const j = (JA.app.jars || []).find(j => j.id === jarId);
            const name = j ? (j.projectName || j.jarName || jarId) : jarId;
            JA.toast?.error('Failed to start scan for ' + name + ': ' + e.message);
        }
    },

    _formatDate(isoStr) {
        if (!isoStr) return '';
        try {
            const d = new Date(isoStr);
            if (isNaN(d.getTime())) return '';
            return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })
                + ' ' + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
        } catch (e) { return ''; }
    }
};
