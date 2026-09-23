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

        PA.claudeSessions._pollTimer = PollConfig.every('claudeProgressMs',
            function() { PA.claudeSessions.refresh(); });
    },

    close: function() {
        PA.claudeSessions._pollTimer = PollConfig.clear(PA.claudeSessions._pollTimer);
        var el = document.getElementById('claudeSessionsOverlay');
        if (el) { el.classList.remove('open'); setTimeout(function() { if (el.parentNode) el.remove(); }, 200); }
    },

    /**
     * One tick: the live session list, plus the usage totals only if they changed.
     *
     * The totals come from an append-only call log that used to be re-read and re-aggregated on
     * the server for every one of these ticks, at 5 s, to render seven numbers that had not moved.
     * Now the response carries an ETag over the log's length, so an unchanged tick is a 304 and
     * the cached totals are re-rendered without re-fetching or re-aggregating anything. The tick
     * itself is at the configured claudeProgressMs (30 s) rather than a hard-coded 5 s.
     */
    _statsEtag: null,
    _statsCache: null,

    refresh: function() {
        var body = document.getElementById('claudeSessionsBody');
        if (!body) return;

        var headers = PA.claudeSessions._statsEtag
            ? { 'If-None-Match': PA.claudeSessions._statsEtag } : {};

        Promise.all([
            fetch('/api/parser/claude/sessions').then(function(r) { return r.json(); }).catch(function() { return {}; }),
            fetch('/api/parser/claude/stats', { headers: headers }).then(function(r) {
                if (r.status === 304) return PA.claudeSessions._statsCache || {};
                PA.claudeSessions._statsEtag = r.headers.get('ETag');
                return r.json().then(function(j) {
                    PA.claudeSessions._statsCache = j;
                    return j;
                });
            }).catch(function() { return PA.claudeSessions._statsCache || {}; })
        ]).then(function(res) {
            var data = res[0] || {};
            var sessions = data.sessions || (Array.isArray(data) ? data : []) || [];
            var summary = data.summary || {};
            PA.claudeSessions._render(body, sessions, summary, res[1] || {});
        }).catch(function(e) {
            body.innerHTML = '<div class="empty-msg" style="color:var(--red)">Failed to load: ' + PA.esc(e.message || '') + '</div>';
        });
    },

    _fmtBytes: function(n) {
        n = n || 0;
        if (n < 1024) return n + ' B';
        if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
        return (n / 1048576).toFixed(2) + ' MB';
    },

    _fmtDur: function(ms) {
        ms = ms || 0;
        if (ms < 1000) return ms + ' ms';
        var s = ms / 1000;
        if (s < 60) return s.toFixed(1) + 's';
        return Math.floor(s / 60) + 'm ' + Math.round(s % 60) + 's';
    },

    _card: function(label, value) {
        return '<div style="flex:1;min-width:110px;background:var(--bg);border:1px solid var(--border);border-radius:var(--radius-md);padding:8px 12px">'
            + '<div style="font-size:16px;font-weight:700;color:var(--text)">' + value + '</div>'
            + '<div style="font-size:10px;text-transform:uppercase;color:var(--text-muted)">' + label + '</div></div>';
    },

    _renderUsage: function(stats) {
        var log = (stats && stats.log) || {};
        var calls = log.totalCalls || stats.totalCalls || 0;
        var html = '<div style="padding:12px 16px;border-bottom:1px solid var(--border)">';
        html += '<div style="font-size:11px;text-transform:uppercase;color:var(--text-muted);margin-bottom:8px">Claude API Usage (persisted)</div>';
        html += '<div style="display:flex;gap:8px;flex-wrap:wrap">';
        html += this._card('API Calls', calls);
        html += this._card('Sessions', log.sessionCount || 0);
        html += this._card('Prompt', this._fmtBytes(log.totalPromptBytes || stats.totalPromptBytes || 0));
        html += this._card('Response', this._fmtBytes(log.totalResponseBytes || stats.totalResponseBytes || 0));
        html += this._card('Avg Duration', this._fmtDur(log.avgDurationMs || stats.avgDurationMs || 0));
        html += this._card('Total Duration', this._fmtDur(log.totalDurationMs || stats.totalDurationMs || 0));
        html += this._card('Errors', log.errors || stats.errors || 0);
        html += '</div>';

        var sess = log.sessions || [];
        if (sess.length) {
            html += '<table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:12px">';
            html += '<thead><tr style="font-size:9px;text-transform:uppercase;color:var(--text-muted)">'
                + '<th style="padding:4px 8px;text-align:left">Session</th>'
                + '<th style="padding:4px 8px;text-align:left">Type</th>'
                + '<th style="padding:4px 8px;text-align:right">Calls</th>'
                + '<th style="padding:4px 8px;text-align:right">Prompt</th>'
                + '<th style="padding:4px 8px;text-align:right">Response</th>'
                + '<th style="padding:4px 8px;text-align:right">Duration</th>'
                + '<th style="padding:4px 8px;text-align:right">Errors</th></tr></thead><tbody>';
            for (var i = 0; i < sess.length; i++) {
                var s = sess[i];
                html += '<tr style="border-top:1px solid var(--border)">'
                    + '<td style="padding:4px 8px;font-family:var(--font-mono);font-size:9px">' + PA.esc(String(s.sessionId || '').slice(0, 8)) + '</td>'
                    + '<td style="padding:4px 8px">' + PA.esc(s.sessionType || '') + '</td>'
                    + '<td style="padding:4px 8px;text-align:right">' + (s.callCount || 0) + '</td>'
                    + '<td style="padding:4px 8px;text-align:right">' + this._fmtBytes(s.promptBytes) + '</td>'
                    + '<td style="padding:4px 8px;text-align:right">' + this._fmtBytes(s.responseBytes) + '</td>'
                    + '<td style="padding:4px 8px;text-align:right">' + this._fmtDur(s.durationMs) + '</td>'
                    + '<td style="padding:4px 8px;text-align:right;color:' + ((s.errorCount || 0) > 0 ? 'var(--red)' : 'inherit') + '">' + (s.errorCount || 0) + '</td></tr>';
            }
            html += '</tbody></table>';
        }
        html += '</div>';
        return html;
    },

    _render: function(body, sessions, summary, stats) {
        var usageHtml = PA.claudeSessions._renderUsage(stats || {});

        if (!sessions.length) {
            body.innerHTML = usageHtml
                + '<div class="empty-msg">No live Claude sessions this server session.</div>';
            return;
        }

        var running = summary.running || 0;
        var html = usageHtml;
        html += '<div style="padding:10px 16px;font-size:11px;color:var(--text-muted);border-bottom:1px solid var(--border);display:flex;gap:12px">';
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
