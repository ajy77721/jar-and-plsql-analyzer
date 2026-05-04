window.JA = window.JA || {};

JA.dashPanel = {

    _pollId: null,

    render() {
        const el = document.getElementById('dash-right-panel');
        if (!el) return;
        el.innerHTML =
            '<div class="drp-col drp-col-activity">' +
                '<div class="drp-section-title">Recent Activity</div>' +
                '<div class="drp-section-body drp-scroll" id="drp-activity-body">Loading...</div>' +
            '</div>' +
            '<div class="drp-col drp-col-logs">' +
                '<div class="drp-section-title">' +
                    'Application Log' +
                    '<button class="drp-btn" onclick="JA.logviewer.toggle()">Full Log</button>' +
                '</div>' +
                '<div class="drp-section-body drp-log-body drp-scroll" id="drp-log-body">Loading...</div>' +
            '</div>';

        this._loadActivity();
        this._loadLogTail();
        this._startPoll();
    },

    destroy() {
        if (this._pollId) {
            clearInterval(this._pollId);
            this._pollId = null;
        }
    },

    _startPoll() {
        this.destroy();
        this._pollId = setInterval(() => {
            if (!document.getElementById('drp-log-body')) {
                this.destroy();
                return;
            }
            this._loadLogTail();
        }, 10000);
    },

    _loadActivity() {
        var body = document.getElementById('drp-activity-body');
        if (!body) return;
        var jars = JA.app.jars || [];
        if (!jars.length) {
            body.innerHTML = '<div class="drp-empty">No analyses yet</div>';
            return;
        }
        var sorted = jars.slice().sort(function(a, b) {
            return (b.analyzedAt || '').localeCompare(a.analyzedAt || '');
        });
        var recent = sorted.slice(0, 8);
        var html = '<div class="drp-activity-list">';
        for (var i = 0; i < recent.length; i++) {
            var j = recent[i];
            var name = j.projectName || j.jarName || j.id || '?';
            var ts = this._timeAgo(j.analyzedAt);
            var cs = j.claudeStatus || '';
            var csBadge = '';
            if (cs === 'RUNNING') csBadge = '<span class="drp-badge drp-badge-running">AI</span>';
            else if (cs === 'COMPLETE') csBadge = '<span class="drp-badge drp-badge-done">AI</span>';
            html += '<div class="drp-activity-item" onclick="JA.app.selectJar(\'' + JA.utils.escapeHtml((j.id || '').replace(/'/g, "\\'")) + '\')">';
            html += '<span class="drp-ai-name">' + JA.utils.escapeHtml(name) + csBadge + '</span>';
            html += '<span class="drp-ai-meta">' + (j.totalEndpoints || 0) + ' eps &middot; ' + (j.totalClasses || 0) + ' cls</span>';
            html += '<span class="drp-ai-time">' + ts + '</span>';
            html += '</div>';
        }
        html += '</div>';
        body.innerHTML = html;
    },

    async _loadLogTail() {
        var body = document.getElementById('drp-log-body');
        if (!body) return;
        try {
            var resp = await fetch('/api/jar/logs?lines=50');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            var data = await resp.json();
            var content = data.content || '';
            var lines = content.split('\n').filter(function(l) { return l.trim(); });
            var tail = lines.slice(-20);
            if (!tail.length) {
                body.innerHTML = '<div class="drp-empty">No log entries</div>';
                return;
            }
            var html = '<pre class="drp-log-pre">';
            for (var i = 0; i < tail.length; i++) {
                var line = tail[i];
                var cls = '';
                if (line.includes(' ERROR ')) cls = 'drp-log-err';
                else if (line.includes(' WARN ')) cls = 'drp-log-warn';
                html += '<div class="drp-log-line ' + cls + '">' + JA.utils.escapeHtml(line) + '</div>';
            }
            html += '</pre>';
            body.innerHTML = html;
            body.scrollTop = body.scrollHeight;
        } catch (e) {
            body.innerHTML = '<div class="drp-empty">Log not available</div>';
        }
    },

    _duration(start, end) {
        if (!start || !end) return '';
        try {
            var ms = new Date(end) - new Date(start);
            if (isNaN(ms) || ms < 0) return '';
            var sec = Math.floor(ms / 1000);
            if (sec < 60) return sec + 's';
            var min = Math.floor(sec / 60);
            if (min < 60) return min + 'm ' + (sec % 60) + 's';
            var hr = Math.floor(min / 60);
            return hr + 'h ' + (min % 60) + 'm';
        } catch (e) { return ''; }
    },

    _timeAgo(isoStr) {
        if (!isoStr) return '';
        try {
            var d = new Date(isoStr);
            var now = new Date();
            var diff = Math.floor((now - d) / 60000);
            if (diff < 1) return 'Just now';
            if (diff < 60) return diff + 'm ago';
            var hrs = Math.floor(diff / 60);
            if (hrs < 24) return hrs + 'h ago';
            var days = Math.floor(hrs / 24);
            if (days < 7) return days + 'd ago';
            return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
        } catch (e) { return ''; }
    }
};
