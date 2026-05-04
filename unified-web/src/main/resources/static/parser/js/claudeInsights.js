window.PA = window.PA || {};

PA.claudeInsights = {
    _pollId: null,
    _lastStatus: null,

    load: function() {
        var container = document.getElementById('claudeInsightsContainer');
        if (!container) return;
        var name = PA.claudeInsights._analysisName();
        if (!name) {
            container.innerHTML = '<div class="empty-msg">Load an analysis first</div>';
            return;
        }
        container.innerHTML = PA.claudeInsights._renderTab();
        setTimeout(function() { PA.claudeInsights._checkStatus(); }, 300);
    },

    _analysisName: function() {
        return (PA.analysisData && PA.analysisData.name) || PA.api._analysisName || null;
    },

    _renderTab: function() {
        var name = PA.claudeInsights._analysisName();
        var esc = PA.esc;
        var html = '<div class="ci-section">';
        html += '<div id="ci-status-msg" class="ci-status-bar">Checking for Claude verification data...</div>';

        html += '<div class="ci-header" id="ci-action-header" style="display:none">';
        html += '<span class="ci-badge">CLAUDE INSIGHTS</span>';
        html += '<span class="ci-title">Deep Analysis of ' + esc(name) + '</span>';
        html += '<div class="ci-actions">';
        html += '<button class="btn btn-sm" onclick="PA.claudeInsights._checkStatus()">Check Status</button>';
        html += '<button class="btn btn-sm btn-primary" onclick="PA.claudeInsights._startScan(true)">Resume Scan</button>';
        html += '<button class="btn btn-sm" onclick="PA.claudeInsights._startScan(false)">Fresh Scan</button>';
        html += '<button class="btn btn-sm" onclick="PA.claudeInsights._viewChunks()">View Chunks</button>';
        html += '</div>';
        html += '</div>';

        html += '<div id="ci-result-wrap" style="display:none">';
        html += '<div id="ci-summary-cards"></div>';
        html += '<div id="ci-table-list"></div>';
        html += '</div>';

        html += '</div>';
        return html;
    },

    _checkStatus: async function() {
        var name = PA.claudeInsights._analysisName();
        var msgEl = document.getElementById('ci-status-msg');
        if (!msgEl || !name) return;

        var headerEl = document.getElementById('ci-action-header');
        var resultEl = document.getElementById('ci-result-wrap');
        var showPanel = function() {
            if (headerEl) headerEl.style.display = '';
            if (resultEl) resultEl.style.display = '';
        };

        try {
            var progress = await PA.api.claudeProgress(name);
            PA.claudeInsights._lastStatus = progress;

            if (progress.hasRunningSession) {
                showPanel();
                PA.claudeInsights._updateProgress(progress);
                PA.claudeInsights._startPoll();
                return;
            }

            var result = null;
            try { result = await PA.api.claudeResult(name); } catch (e) {}

            if (result && result.tables && result.tables.length > 0) {
                showPanel();
                var confirmed = result.confirmedCount || 0;
                var removed = result.removedCount || 0;
                var newCount = result.newCount || 0;
                var total = confirmed + removed + newCount;
                msgEl.innerHTML = '<span class="ci-badge">VERIFIED</span> Claude verification complete &mdash; '
                    + total + ' operations analyzed (' + confirmed + ' confirmed, ' + removed + ' removed, ' + newCount + ' new).';
                msgEl.className = 'ci-status-bar ci-status-ok';
                PA.claudeInsights._renderResult(result);
                return;
            }

            showPanel();
            msgEl.innerHTML = 'No Claude verification data. Run a scan to verify table operations with AI.'
                + ' <button class="btn btn-sm btn-primary" onclick="PA.claudeInsights._startScan(false)" style="margin-left:12px">Start Scan</button>';
            msgEl.className = 'ci-status-bar ci-status-none';
        } catch (e) {
            showPanel();
            msgEl.innerHTML = 'Could not check Claude status: ' + PA.esc(e.message || '');
            msgEl.className = 'ci-status-bar ci-status-none';
        }
    },

    _updateProgress: function(progress) {
        var msgEl = document.getElementById('ci-status-msg');
        if (!msgEl) return;

        var pct = progress.percent || 0;
        var completed = progress.completedChunks || 0;
        var total = progress.totalChunks || 0;
        var failed = progress.failedChunks || 0;
        var status = progress.status || '';

        var html = '<div class="ci-progress-live">';
        html += '<div class="ci-progress-bar-wrap"><div class="ci-progress-bar-fill" style="width:' + pct + '%"></div></div>';
        html += '<span class="ci-progress-text">Verifying: ' + completed + '/' + total + ' chunks (' + Math.round(pct) + '%)';
        if (failed > 0) html += ' &mdash; ' + failed + ' failed';
        if (status) html += ' &mdash; ' + PA.esc(status);
        html += '</span>';
        html += '</div>';

        msgEl.innerHTML = html;
        msgEl.className = 'ci-status-bar ci-status-running';
    },

    _startScan: async function(resume) {
        var name = PA.claudeInsights._analysisName();
        if (!name) { PA.toast('Load an analysis first', 'warn'); return; }
        if (!PA.claude.available) { PA.toast('Claude is not available', 'error'); return; }

        try {
            PA.toast((resume ? 'Resuming' : 'Starting') + ' Claude verification...', 'success');
            await PA.api.claudeVerify(name, !!resume);
            PA.toast('Verification started', 'success');
            PA.claudeInsights._startPoll();
        } catch (e) {
            var msg = (e.message || '').indexOf('409') >= 0
                ? 'Verification already running'
                : 'Failed: ' + (e.message || e);
            PA.toast(msg, 'error');
        }
    },

    _startPoll: function() {
        if (PA.claudeInsights._pollId) return;
        var name = PA.claudeInsights._analysisName();
        PA.claudeInsights._pollId = setInterval(async function() {
            var currentName = PA.claudeInsights._analysisName();
            if (currentName !== name) {
                clearInterval(PA.claudeInsights._pollId);
                PA.claudeInsights._pollId = null;
                return;
            }
            try {
                var progress = await PA.api.claudeProgress(name);
                PA.claudeInsights._lastStatus = progress;
                PA.claudeInsights._updateProgress(progress);

                var isComplete = progress.isComplete === true;
                var hasRunning = progress.hasRunningSession === true;

                if (isComplete && !hasRunning) {
                    clearInterval(PA.claudeInsights._pollId);
                    PA.claudeInsights._pollId = null;
                    PA.toast('Claude verification complete!', 'success');
                    PA.claudeInsights._checkStatus();
                }
            } catch (e) {}
        }, 2000);
    },

    _renderResult: function(result) {
        var cardsEl = document.getElementById('ci-summary-cards');
        var listEl = document.getElementById('ci-table-list');
        if (!cardsEl || !listEl) return;

        var esc = PA.esc;
        var confirmed = result.confirmedCount || 0;
        var removed = result.removedCount || 0;
        var newCount = result.newCount || 0;
        var total = confirmed + removed + newCount;
        var tables = result.tables || [];

        var html = '<div class="ci-cards">';
        html += PA.claudeInsights._card('Confirmed', confirmed, '#22c55e');
        html += PA.claudeInsights._card('Removed', removed, '#ef4444');
        html += PA.claudeInsights._card('New', newCount, '#6366f1');
        html += PA.claudeInsights._card('Total', total, 'var(--text)');
        html += PA.claudeInsights._card('Tables', tables.length, 'var(--teal)');

        var dur = result.claudeTimeMs ? Math.round(result.claudeTimeMs / 1000) + 's' : '-';
        html += PA.claudeInsights._card('Duration', dur, 'var(--text-muted)');
        html += PA.claudeInsights._card('API Calls', result.claudeCallCount || 0, 'var(--text-muted)');
        var errChunks = result.errorChunks || 0;
        if (errChunks > 0) html += PA.claudeInsights._card('Errors', errChunks, '#ef4444');
        html += '</div>';
        cardsEl.innerHTML = html;

        var thtml = '';
        for (var i = 0; i < tables.length; i++) {
            thtml += PA.claudeInsights._renderTableCard(tables[i], i, esc);
        }
        if (!thtml) thtml = '<div class="empty-msg">No table verification data</div>';
        listEl.innerHTML = thtml;
    },

    _card: function(label, value, color) {
        return '<div class="ci-stat-card"><div class="ci-stat-value" style="color:' + color + '">' + value + '</div>'
            + '<div class="ci-stat-label">' + label + '</div></div>';
    },

    _renderTableCard: function(tableResult, idx, esc) {
        var tableName = tableResult.tableName || '';
        var status = tableResult.overallStatus || 'UNVERIFIED';
        var verifications = tableResult.claudeVerifications || [];
        if (!verifications.length) return '';

        var conf = 0, rem = 0, nw = 0;
        for (var i = 0; i < verifications.length; i++) {
            var s = (verifications[i].status || '').toUpperCase();
            if (s === 'CONFIRMED') conf++;
            else if (s === 'REMOVED') rem++;
            else if (s === 'NEW') nw++;
        }

        var html = '<div class="ci-table-card">';
        html += '<div class="ci-table-header" onclick="PA.claudeInsights._toggleDetail(' + idx + ')">';
        html += '<span class="ci-table-name">' + esc(tableName) + '</span>';
        html += '<span class="ci-table-status ci-status-' + status.toLowerCase() + '">' + esc(status) + '</span>';
        html += '<span class="ci-table-counts">';
        if (conf) html += '<span class="ci-cnt ci-cnt-confirmed">' + conf + ' confirmed</span>';
        if (rem) html += '<span class="ci-cnt ci-cnt-removed">' + rem + ' removed</span>';
        if (nw) html += '<span class="ci-cnt ci-cnt-new">' + nw + ' new</span>';
        html += '<span class="ci-cnt">' + verifications.length + ' total</span>';
        html += '</span>';
        html += '</div>';

        html += '<div class="ci-table-detail" id="ci-detail-' + idx + '" style="display:none">';
        for (var j = 0; j < verifications.length; j++) {
            var v = verifications[j];
            html += '<div class="ci-verification-row">';
            html += '<span class="ci-op-badge ci-op-' + (v.operation || 'OTHER').toLowerCase() + '">' + esc(v.operation || '?') + '</span>';
            html += '<span class="ci-v-status ci-v-' + (v.status || '').toLowerCase() + '">' + esc(v.status || '?') + '</span>';
            if (v.procedureName) html += '<span class="ci-v-proc">' + esc(v.procedureName) + '</span>';
            if (v.lineNumber) html += '<span class="ci-v-line">L' + v.lineNumber + '</span>';
            if (v.reason) html += '<span class="ci-v-reason">' + esc(v.reason) + '</span>';
            html += '</div>';
        }
        html += '</div>';
        html += '</div>';
        return html;
    },

    _toggleDetail: function(idx) {
        var el = document.getElementById('ci-detail-' + idx);
        if (!el) return;
        el.style.display = el.style.display === 'none' ? '' : 'none';
    },

    _viewChunks: function() {
        if (PA.chunkViewer && PA.chunkViewer.open) PA.chunkViewer.open();
    }
};
