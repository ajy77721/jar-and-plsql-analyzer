window.PA = window.PA || {};

/**
 * Persistent, global job-progress indicator (mirrors JAR's #claude-sidebar-progress pattern).
 *
 * Unlike #homeProgress (which lives inside screenHome and disappears the instant the user
 * navigates to the analysis screen or another home tab), `#parserPersistentProgress` sits
 * outside every `.screen` element in index.html, so `PA.showScreen` never hides it. Any job
 * tracked here (single-analyze via PA.startAnalysis, or each Quick Submit job) stays visible
 * across Home / analysis detail / History / Performance Summary / Queue until it finishes.
 */
PA.jobTracker = {
    jobs: {},

    register: function(jobId, label) {
        if (!jobId) return;
        this.jobs[jobId] = {
            label: label || jobId,
            pct: 0,
            statusText: 'Queued...',
            viaPolling: false
        };
        this.render();
    },

    update: function(jobId, pct, text) {
        var j = this.jobs[jobId];
        if (!j) return;
        if (typeof pct === 'number') j.pct = pct;
        if (text) j.statusText = text;
        this.render();
    },

    /** Called when the SSE stream drops and the job falls back to polling — closes gap #7. */
    markPolling: function(jobId) {
        var j = this.jobs[jobId];
        if (!j) return;
        if (j.viaPolling) return; // already flagged; avoid re-render spam
        j.viaPolling = true;
        j.statusText = 'Reconnected via polling...';
        this.render();
    },

    remove: function(jobId) {
        delete this.jobs[jobId];
        this.render();
    },

    goTo: function() {
        if (PA.showScreen) PA.showScreen('home');
        if (PA.switchHomeTab) PA.switchHomeTab('queue');
    },

    render: function() {
        var el = document.getElementById('parserPersistentProgress');
        if (!el) return;
        var ids = Object.keys(this.jobs);
        if (ids.length === 0) {
            el.style.display = 'none';
            el.innerHTML = '';
            return;
        }
        el.style.display = '';
        var self = this;
        var html = '<div class="ppp-label">Analyzing (' + ids.length + ')</div>';
        ids.forEach(function(id) {
            var j = self.jobs[id];
            var pct = Math.max(3, Math.min(100, j.pct || 0));
            html += '<div class="ppp-entry" onclick="PA.jobTracker.goTo()">'
                + '<div class="ppp-name">' + PA.esc(j.label) + '</div>'
                + '<div class="ppp-row"><div class="ppp-bar"><div class="ppp-fill" style="width:' + pct + '%"></div></div>'
                + '<div class="ppp-count">' + Math.round(pct) + '%</div></div>'
                + '<div class="ppp-status">' + PA.esc(j.statusText || '') + '</div>'
                + '</div>';
        });
        el.innerHTML = html;
    }
};

/**
 * Lightweight persistent error banner for analyze-trigger failures (gap #9), used alongside the
 * existing 4-second PA.toast so a failure that happens while the user is looking elsewhere is
 * still visible when they come back.
 */
PA.showJobBanner = function(msg, type) {
    var el = document.getElementById('parserJobBanner');
    if (!el) return;
    var msgEl = document.getElementById('parserJobBannerMsg');
    if (msgEl) msgEl.textContent = msg;
    el.className = 'parser-job-banner visible ' + (type || 'error');
    el.style.display = 'flex';
};

PA.hideJobBanner = function() {
    var el = document.getElementById('parserJobBanner');
    if (!el) return;
    el.style.display = 'none';
    el.className = 'parser-job-banner';
};
