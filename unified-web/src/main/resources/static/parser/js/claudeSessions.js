window.PA = window.PA || {};

PA.claudeSessions = {
    _pollTimer: null,

    open: function() {
        PA.claudeSessions.close();
        var overlay = document.createElement('div');
        overlay.className = 'tds-overlay';
        overlay.id = 'claudeSessionsOverlay';
        overlay.onclick = function(e) { if (e.target === overlay) PA.claudeSessions.close(); };

        var modal = document.createElement('div');
        modal.className = 'tds-modal';
        modal.style.maxWidth = '900px';
        modal.onclick = function(e) { e.stopPropagation(); };
        modal.innerHTML =
            '<div class="tds-header">' +
                '<span style="font-weight:700">Claude Sessions</span>' +
                '<div style="display:flex;gap:6px;align-items:center">' +
                    '<button class="btn btn-sm" onclick="PA.claudeSessions.killAll()" style="color:var(--red)">Kill All</button>' +
                    '<button class="btn btn-sm" onclick="PA.claudeSessions.refresh()">Refresh</button>' +
                    '<button class="btn btn-sm" onclick="PA.claudeSessions.close()">&times;</button>' +
                '</div>' +
            '</div>' +
            '<div class="tds-body" id="claudeSessionsBody" style="padding:0;overflow:auto;max-height:65vh">' +
                '<div class="empty-msg">Loading...</div>' +
            '</div>';

        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        requestAnimationFrame(function() { overlay.classList.add('open'); });

        PA.claudeSessions.refresh();
        PA.claudeSessions._pollTimer = setInterval(function() { PA.claudeSessions.refresh(); }, 5000);
    },

    close: function() {
        if (PA.claudeSessions._pollTimer) {
            clearInterval(PA.claudeSessions._pollTimer);
            PA.claudeSessions._pollTimer = null;
        }
        var el = document.getElementById('claudeSessionsOverlay');
        if (el) { el.classList.remove('open'); setTimeout(function() { if (el.parentNode) el.remove(); }, 200); }
    },

    refresh: function() {
        var body = document.getElementById('claudeSessionsBody');
        if (!body) return;

        fetch('/api/parser/claude/sessions')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                var sessions = data.sessions || data || [];
                var summary = data.summary || {};
                PA.claudeSessions._render(body, sessions, summary);
            })
            .catch(function(e) {
                body.innerHTML = '<div class="empty-msg" style="color:var(--red)">Failed to load sessions: ' + PA.esc(e.message || '') + '</div>';
            });
    },

    _render: function(body, sessions, summary) {
        if (!sessions.length) {
            body.innerHTML = '<div class="empty-msg">No Claude sessions yet. Run a verification to create one.</div>';
            return;
        }

        var running = summary.running || 0;
        var html = '<div style="padding:10px 16px;font-size:11px;color:var(--text-muted);border-bottom:1px solid var(--border);display:flex;gap:12px">';
        html += '<span>' + sessions.length + ' total</span>';
        if (running > 0) html += '<span style="color:var(--blue);font-weight:600">' + running + ' running</span>';
        if (summary.complete) html += '<span style="color:var(--green)">' + summary.complete + ' complete</span>';
        if (summary.failed) html += '<span style="color:var(--red)">' + summary.failed + ' failed</span>';
        if (summary.killed) html += '<span style="color:var(--orange)">' + summary.killed + ' killed</span>';
        html += '</div>';

        html += '<table style="width:100%;border-collapse:collapse;font-size:12px">';
        html += '<thead><tr style="background:var(--bg);font-size:10px;text-transform:uppercase;color:var(--text-muted)">';
        html += '<th style="padding:6px 12px;text-align:left">ID</th>';
        html += '<th style="padding:6px 12px;text-align:left">Analysis</th>';
        html += '<th style="padding:6px 12px;text-align:left">Type</th>';
        html += '<th style="padding:6px 12px;text-align:left">Status</th>';
        html += '<th style="padding:6px 12px;text-align:left">Detail</th>';
        html += '<th style="padding:6px 12px;text-align:left">Duration</th>';
        html += '<th style="padding:6px 12px;text-align:left">Action</th>';
        html += '</tr></thead><tbody>';

        var statusColors = { RUNNING: 'var(--blue)', COMPLETE: 'var(--green)', FAILED: 'var(--red)', KILLED: 'var(--orange)' };

        for (var i = 0; i < sessions.length; i++) {
            var s = sessions[i];
            var sc = statusColors[s.status] || 'var(--text-muted)';
            html += '<tr style="border-top:1px solid var(--border)">';
            html += '<td style="padding:6px 12px;font-family:var(--font-mono);font-size:10px">' + PA.esc(s.id || '') + '</td>';
            html += '<td style="padding:6px 12px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + PA.esc(s.analysisName || '') + '</td>';
            html += '<td style="padding:6px 12px">' + PA.esc(s.type || '') + '</td>';
            html += '<td style="padding:6px 12px;color:' + sc + ';font-weight:700">' + PA.esc(s.status || '') + '</td>';
            html += '<td style="padding:6px 12px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + PA.esc(s.detail || '') + '">' + PA.esc(s.detail || '') + '</td>';
            html += '<td style="padding:6px 12px">' + PA.esc(s.durationFormatted || '-') + '</td>';
            html += '<td style="padding:6px 12px">';
            if (s.status === 'RUNNING') {
                html += '<button class="btn btn-sm" style="color:var(--red);font-size:10px;padding:2px 8px" onclick="PA.claudeSessions.kill(\'' + PA.esc(s.analysisName || '') + '\',\'' + PA.esc(s.id || '') + '\')">Kill</button>';
            }
            html += '</td></tr>';
        }
        html += '</tbody></table>';
        body.innerHTML = html;
    },

    kill: function(analysisName, sessionId) {
        if (!analysisName || !sessionId) return;
        fetch('/api/parser/analyses/' + encodeURIComponent(analysisName) + '/claude/sessions/' + encodeURIComponent(sessionId) + '/kill', { method: 'POST' })
            .then(function() {
                PA.toast('Session killed', 'warn');
                PA.claudeSessions.refresh();
            })
            .catch(function(e) {
                PA.toast('Failed to kill session: ' + (e.message || e), 'error');
            });
    },

    killAll: function() {
        fetch('/api/parser/claude/sessions/kill-all', { method: 'POST' })
            .then(function() {
                PA.toast('All sessions killed', 'warn');
                PA.claudeSessions.refresh();
            })
            .catch(function(e) {
                PA.toast('Failed to kill sessions: ' + (e.message || e), 'error');
            });
    }
};
