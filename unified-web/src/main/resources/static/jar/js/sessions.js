/**
 * Claude Session Manager UI — shows all sessions across the system with kill support.
 * Accessible from sidebar button and Claude Insights tab.
 */
window.JA = window.JA || {};

JA.sessions = {

    _pollId: null,

    async show() {
        // Remove existing overlay if any
        const existing = document.getElementById('session-overlay');
        if (existing) existing.remove();

        const overlay = document.createElement('div');
        overlay.id = 'session-overlay';
        overlay.className = 'session-overlay';
        overlay.innerHTML = `
            <div class="session-panel">
                <div class="session-header">
                    <span class="session-title">Claude Sessions</span>
                    <div class="session-header-actions">
                        <button class="btn-sm" onclick="JA.sessions.refresh()">Refresh</button>
                        <button class="btn-sm" onclick="JA.sessions.close()">Close</button>
                    </div>
                </div>
                <div class="session-body" id="session-body">
                    <div class="session-loading">Loading sessions...</div>
                </div>
            </div>`;
        document.body.appendChild(overlay);
        overlay.addEventListener('click', e => {
            if (e.target === overlay) this.close();
        });

        await this.refresh();
        this._startPoll();
    },

    close() {
        const overlay = document.getElementById('session-overlay');
        if (overlay) overlay.remove();
        this._stopPoll();
    },

    async refresh() {
        const body = document.getElementById('session-body');
        if (!body) return;

        try {
            const sessions = await JA.api.listSessions();
            if (!sessions.length) {
                body.innerHTML = '<div class="session-empty">No Claude sessions have been run yet.</div>';
                return;
            }
            body.innerHTML = this._renderSessions(sessions);
        } catch (e) {
            body.innerHTML = '<div class="session-empty">Failed to load sessions: ' + JA.utils.escapeHtml(e.message) + '</div>';
        }
    },

    _renderSessions(sessions) {
        const esc = JA.utils.escapeHtml;
        let html = '<div class="session-list">';

        for (const s of sessions) {
            const statusCls = 'session-status-' + s.status.toLowerCase();
            const isRunning = s.status === 'RUNNING';

            html += `<div class="session-item ${statusCls}">`;
            html += '<div class="session-item-header">';
            html += `<span class="session-id">${esc(s.id)}</span>`;
            html += `<span class="session-status-badge ${statusCls}">${esc(s.status)}</span>`;
            html += `<span class="session-type">${esc(this._formatType(s.type))}</span>`;
            html += '</div>';

            html += '<div class="session-item-meta">';
            html += `<span class="session-jar">${esc(s.jarName || s.id || 'Unknown')}</span>`;
            if (s.detail) html += `<span class="session-detail">${esc(s.detail)}</span>`;
            html += '</div>';

            html += '<div class="session-item-time">';
            html += `<span>Started: ${this._formatTime(s.startedAt)}</span>`;
            if (s.completedAt) {
                html += `<span>Ended: ${this._formatTime(s.completedAt)}</span>`;
                html += `<span class="session-duration">${this._formatDuration(s.startedAt, s.completedAt)}</span>`;
            } else if (isRunning) {
                html += `<span class="session-running-time">Running for ${this._formatDuration(s.startedAt, new Date().toISOString())}</span>`;
            }
            html += '</div>';

            if (s.error) {
                html += `<div class="session-error">${esc(s.error)}</div>`;
            }

            if (isRunning) {
                const safeId = s.id.replace(/'/g, "\\'");
                html += `<div class="session-item-actions">`;
                html += `<button class="btn-sm session-kill-btn" onclick="JA.sessions.killSession('${safeId}')">Kill Session</button>`;
                html += '</div>';
            }

            html += '</div>';
        }
        html += '</div>';

        // Summary
        const running = sessions.filter(s => s.status === 'RUNNING').length;
        const completed = sessions.filter(s => s.status === 'COMPLETE').length;
        const failed = sessions.filter(s => s.status === 'FAILED').length;
        const killed = sessions.filter(s => s.status === 'KILLED').length;
        html += '<div class="session-summary">';
        html += `Total: ${sessions.length}`;
        if (running) html += ` | <span class="session-status-running">Running: ${running}</span>`;
        if (completed) html += ` | Completed: ${completed}`;
        if (failed) html += ` | <span class="session-status-failed">Failed: ${failed}</span>`;
        if (killed) html += ` | <span class="session-status-killed">Killed: ${killed}</span>`;
        html += '</div>';

        return html;
    },

    async killSession(sessionId) {
        const confirmed = await JA.utils.confirm({
            title: 'Kill Claude Session',
            message: `<p>Are you sure you want to kill session <strong>${JA.utils.escapeHtml(sessionId)}</strong>?</p>`
                + `<p>This will terminate the running Claude CLI process and stop the enrichment.</p>`,
            confirmLabel: 'Kill',
            confirmClass: 'confirm-btn-danger'
        });
        if (!confirmed) return;

        try {
            await JA.api.killSession(sessionId);
            JA.toast?.success('Session ' + sessionId + ' killed');
            await this.refresh();
        } catch (e) {
            JA.toast?.error('Failed to kill session: ' + e.message);
        }
    },

    _startPoll() {
        this._stopPoll();
        this._pollId = setInterval(() => {
            if (!document.getElementById('session-overlay')) {
                this._stopPoll();
                return;
            }
            this.refresh();
        }, PollConfig.sessionPollMs);
    },

    _stopPoll() {
        if (this._pollId) {
            clearInterval(this._pollId);
            this._pollId = null;
        }
    },

    _formatType(type) {
        const map = {
            SINGLE_ENDPOINT: 'Single Endpoint',
            RESCAN: 'Rescan (Resume)',
            FRESH_SCAN: 'Fresh Scan',
            UPLOAD: 'Upload'
        };
        return map[type] || type;
    },

    _formatTime(isoStr) {
        if (!isoStr) return '-';
        try {
            const d = new Date(isoStr);
            if (isNaN(d.getTime())) return isoStr;
            return d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
                + ' ' + d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
        } catch (e) { return isoStr; }
    },

    _formatDuration(start, end) {
        try {
            const ms = new Date(end) - new Date(start);
            if (isNaN(ms) || ms < 0) return '';
            const sec = Math.floor(ms / 1000);
            if (sec < 60) return sec + 's';
            const min = Math.floor(sec / 60);
            const remSec = sec % 60;
            if (min < 60) return min + 'm ' + remSec + 's';
            const hr = Math.floor(min / 60);
            return hr + 'h ' + (min % 60) + 'm';
        } catch (e) { return ''; }
    }
};
