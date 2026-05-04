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
        return `<div class="dash-empty">
            <p>No JARs analyzed yet. Upload a JAR to get started.</p>
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
        html += this._stat(jars.length, 'JARs');
        html += this._stat(totalClasses, 'Classes');
        html += this._stat(totalEndpoints, 'Endpoints');
        html += this._stat(JA.utils.formatSize(totalSize), 'Total Size');
        if (claudeRunning > 0) html += this._stat(claudeRunning, 'Claude Running', 'dash-stat-running');
        if (claudeComplete > 0) html += this._stat(claudeComplete, 'Claude Done', 'dash-stat-ok');
        html += '</div>';

        // Actions
        html += '<div class="dash-actions">';
        html += `<button class="btn-sm" onclick="JA.sessions.show()">Claude Sessions</button>`;
        html += '</div>';

        // JAR cards
        html += '<div class="dash-jars">';
        for (const j of jars) {
            html += this._renderJarCard(j, esc);
        }
        html += '</div>';

        html += '</div>';
        return html;
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
        html += `<button class="btn-sm" onclick="JA.dashboard.triggerClaude('${safeId}');event.stopPropagation()">Claude</button>`;
        html += `<button class="btn-sm" onclick="JA.app.deleteJar('${safeId}');event.stopPropagation()" style="color:var(--red)">Delete</button>`;
        html += '</div>';

        html += '</div>';
        return html;
    },

    async triggerClaude(jarId) {
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
