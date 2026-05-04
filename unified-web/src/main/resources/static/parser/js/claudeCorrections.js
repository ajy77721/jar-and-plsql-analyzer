window.PA = window.PA || {};

PA.claudeCorrections = {
    _pollId: null,
    _result: null,

    load: function() {
        var container = document.getElementById('claudeCorrectionsContainer');
        if (!container) return;
        var name = PA.claudeCorrections._analysisName();
        if (!name) {
            container.innerHTML = '<div class="empty-msg">Load an analysis first</div>';
            return;
        }
        container.innerHTML = PA.claudeCorrections._renderTab();
        setTimeout(function() { PA.claudeCorrections._checkStatus(); }, 300);
    },

    _analysisName: function() {
        return (PA.analysisData && PA.analysisData.name) || PA.api._analysisName || null;
    },

    _renderTab: function() {
        var name = PA.claudeCorrections._analysisName();
        var esc = PA.esc;
        var html = '<div class="cc-section">';
        html += '<div id="cc-status-msg" class="ci-status-bar">Checking for correction data...</div>';

        html += '<div class="ci-header" id="cc-action-header" style="display:none">';
        html += '<span class="ci-badge" style="background:#8b5cf6">CORRECTIONS</span>';
        html += '<span class="ci-title">Review Claude Corrections for ' + esc(name) + '</span>';
        html += '<div class="ci-actions">';
        html += '<button class="btn btn-sm" onclick="PA.claudeCorrections._checkStatus()">Refresh</button>';
        html += '<button class="btn btn-sm" onclick="PA.claudeCorrections._acceptAll()">Accept All</button>';
        html += '<button class="btn btn-sm" onclick="PA.claudeCorrections._rejectAll()">Reject All</button>';
        html += '<button class="btn btn-sm btn-primary" onclick="PA.claudeCorrections._applyChanges()">Apply Accepted</button>';
        html += '</div>';
        html += '</div>';

        html += '<div id="cc-result-wrap" style="display:none">';
        html += '<div id="cc-summary-bar" class="cc-summary-bar"></div>';
        html += '<div id="cc-table-list"></div>';
        html += '</div>';

        html += '</div>';
        return html;
    },

    _checkStatus: async function() {
        var name = PA.claudeCorrections._analysisName();
        var msgEl = document.getElementById('cc-status-msg');
        if (!msgEl || !name) return;

        var headerEl = document.getElementById('cc-action-header');
        var resultEl = document.getElementById('cc-result-wrap');
        var showPanel = function() {
            if (headerEl) headerEl.style.display = '';
            if (resultEl) resultEl.style.display = '';
        };

        try {
            var result = await PA.api.claudeResult(name);
            if (result && result.tables && result.tables.length > 0) {
                PA.claudeCorrections._result = result;
                showPanel();

                var confirmed = result.confirmedCount || 0;
                var removed = result.removedCount || 0;
                var newCount = result.newCount || 0;
                var total = confirmed + removed + newCount;

                var pending = 0, accepted = 0, rejected = 0;
                for (var ti = 0; ti < result.tables.length; ti++) {
                    var vs = result.tables[ti].claudeVerifications || [];
                    for (var vi = 0; vi < vs.length; vi++) {
                        var ud = (vs[vi].userDecision || '').toUpperCase();
                        if (ud === 'ACCEPTED') accepted++;
                        else if (ud === 'REJECTED') rejected++;
                        else pending++;
                    }
                }

                msgEl.innerHTML = '<span class="ci-badge" style="background:#8b5cf6">CORRECTIONS</span> '
                    + total + ' operations &mdash; '
                    + '<span style="color:#22c55e">' + accepted + ' accepted</span>, '
                    + '<span style="color:#ef4444">' + rejected + ' rejected</span>, '
                    + '<span style="color:var(--text-muted)">' + pending + ' pending</span>';
                msgEl.className = 'ci-status-bar ci-status-ok';

                PA.claudeCorrections._renderResult(result);
            } else {
                showPanel();
                msgEl.innerHTML = 'No Claude verification data yet. Run a Claude scan from the <b>Claude Insights</b> tab first.';
                msgEl.className = 'ci-status-bar ci-status-none';
            }
        } catch (e) {
            showPanel();
            msgEl.innerHTML = 'No correction data available. Run Claude verification first.';
            msgEl.className = 'ci-status-bar ci-status-none';
        }
    },

    _renderResult: function(result) {
        var summaryEl = document.getElementById('cc-summary-bar');
        var listEl = document.getElementById('cc-table-list');
        if (!summaryEl || !listEl) return;

        var esc = PA.esc;
        var tables = result.tables || [];
        var confirmed = result.confirmedCount || 0;
        var removed = result.removedCount || 0;
        var newCount = result.newCount || 0;

        summaryEl.innerHTML =
            '<span class="cc-stat"><b>' + tables.length + '</b> tables</span>' +
            '<span class="cc-stat" style="color:#22c55e">' + confirmed + ' confirmed</span>' +
            '<span class="cc-stat" style="color:#ef4444">' + removed + ' removed</span>' +
            '<span class="cc-stat" style="color:#6366f1">' + newCount + ' new</span>';

        var html = '';
        for (var i = 0; i < tables.length; i++) {
            var t = tables[i];
            var vs = t.claudeVerifications || [];
            if (!vs.length) continue;
            html += PA.claudeCorrections._renderCorrectionCard(t, i, esc);
        }
        if (!html) html = '<div class="empty-msg">No corrections to review</div>';
        listEl.innerHTML = html;
    },

    _renderCorrectionCard: function(tableResult, idx, esc) {
        var tableName = tableResult.tableName || '';
        var vs = tableResult.claudeVerifications || [];
        var status = tableResult.overallStatus || 'UNVERIFIED';

        var conf = 0, rem = 0, nw = 0, acc = 0, rej = 0, pend = 0;
        for (var i = 0; i < vs.length; i++) {
            var s = (vs[i].status || '').toUpperCase();
            var ud = (vs[i].userDecision || '').toUpperCase();
            if (s === 'CONFIRMED') conf++;
            else if (s === 'REMOVED') rem++;
            else if (s === 'NEW') nw++;
            if (ud === 'ACCEPTED') acc++;
            else if (ud === 'REJECTED') rej++;
            else pend++;
        }

        var html = '<div class="cc-card">';

        html += '<div class="cc-card-header" onclick="PA.claudeCorrections._toggleDetail(' + idx + ')">';
        html += '<span class="ci-table-name">' + esc(tableName) + '</span>';
        html += '<span class="ci-table-status ci-status-' + status.toLowerCase() + '">' + esc(status) + '</span>';
        html += '<span class="ci-table-counts">';
        if (conf) html += '<span class="ci-cnt ci-cnt-confirmed">' + conf + '</span>';
        if (rem) html += '<span class="ci-cnt ci-cnt-removed">' + rem + '</span>';
        if (nw) html += '<span class="ci-cnt ci-cnt-new">' + nw + '</span>';
        html += '<span class="ci-cnt">' + vs.length + ' ops</span>';
        if (acc) html += '<span class="cc-decision-cnt cc-accepted">' + acc + ' accepted</span>';
        if (rej) html += '<span class="cc-decision-cnt cc-rejected">' + rej + ' rejected</span>';
        if (pend) html += '<span class="cc-decision-cnt cc-pending">' + pend + ' pending</span>';
        html += '</span>';
        html += '</div>';

        html += '<div class="cc-card-detail" id="cc-detail-' + idx + '" style="display:none">';

        html += '<div class="cc-side-by-side">';
        html += '<div class="cc-column"><div class="cc-col-header">Static Analysis</div>';
        for (var j = 0; j < vs.length; j++) {
            var v = vs[j];
            if ((v.status || '').toUpperCase() === 'NEW') continue;
            html += '<div class="cc-op-row">';
            html += '<span class="ci-op-badge ci-op-' + (v.operation || 'other').toLowerCase() + '">' + esc(v.operation || '?') + '</span>';
            if (v.procedureName) html += '<span class="cc-proc">' + esc(v.procedureName) + '</span>';
            if (v.lineNumber) html += '<span class="cc-line">L' + v.lineNumber + '</span>';
            html += '</div>';
        }
        html += '</div>';

        html += '<div class="cc-column"><div class="cc-col-header">Claude Verification</div>';
        for (var k = 0; k < vs.length; k++) {
            var vv = vs[k];
            html += '<div class="cc-op-row">';
            html += '<span class="ci-op-badge ci-op-' + (vv.operation || 'other').toLowerCase() + '">' + esc(vv.operation || '?') + '</span>';
            html += '<span class="ci-v-status ci-v-' + (vv.status || '').toLowerCase() + '">' + esc(vv.status || '?') + '</span>';
            if (vv.procedureName) html += '<span class="cc-proc">' + esc(vv.procedureName) + '</span>';
            if (vv.lineNumber) html += '<span class="cc-line">L' + vv.lineNumber + '</span>';
            if (vv.reason) html += '<span class="ci-v-reason">' + esc(vv.reason) + '</span>';

            var decision = (vv.userDecision || '').toUpperCase();
            var tblSafe = tableName.replace(/'/g, "\\'");
            var opSafe = (vv.operation || '').replace(/'/g, "\\'");
            var procSafe = (vv.procedureName || '').replace(/'/g, "\\'");
            var ln = vv.lineNumber || 0;

            html += '<span class="cc-decision-btns">';
            html += '<button class="cc-btn cc-btn-accept' + (decision === 'ACCEPTED' ? ' active' : '') + '" '
                + 'onclick="PA.claudeCorrections._decide(\'' + tblSafe + '\',\'' + opSafe + '\',\'' + procSafe + '\',' + ln + ',\'ACCEPTED\');event.stopPropagation()">Accept</button>';
            html += '<button class="cc-btn cc-btn-reject' + (decision === 'REJECTED' ? ' active' : '') + '" '
                + 'onclick="PA.claudeCorrections._decide(\'' + tblSafe + '\',\'' + opSafe + '\',\'' + procSafe + '\',' + ln + ',\'REJECTED\');event.stopPropagation()">Reject</button>';
            html += '</span>';
            html += '</div>';
        }
        html += '</div>';
        html += '</div>';

        html += '</div>';
        html += '</div>';
        return html;
    },

    _toggleDetail: function(idx) {
        var el = document.getElementById('cc-detail-' + idx);
        if (!el) return;
        el.style.display = el.style.display === 'none' ? '' : 'none';
    },

    _decide: async function(tableName, operation, procedureName, lineNumber, decision) {
        var name = PA.claudeCorrections._analysisName();
        if (!name) return;

        try {
            await PA.api.claudeReview(name, {
                decisions: [{
                    tableName: tableName,
                    operation: operation,
                    procedureName: procedureName,
                    lineNumber: lineNumber,
                    decision: decision
                }]
            });

            if (PA.claudeCorrections._result) {
                for (var ti = 0; ti < PA.claudeCorrections._result.tables.length; ti++) {
                    var t = PA.claudeCorrections._result.tables[ti];
                    if ((t.tableName || '').toUpperCase() !== tableName.toUpperCase()) continue;
                    var vs = t.claudeVerifications || [];
                    for (var vi = 0; vi < vs.length; vi++) {
                        var v = vs[vi];
                        if ((v.operation || '').toUpperCase() === operation.toUpperCase()
                            && (v.procedureName || '') === procedureName
                            && (v.lineNumber || 0) === lineNumber) {
                            v.userDecision = decision;
                        }
                    }
                }
                PA.claudeCorrections._renderResult(PA.claudeCorrections._result);
            }

            PA.toast(decision === 'ACCEPTED' ? 'Accepted' : 'Rejected', 'success');
        } catch (e) {
            PA.toast('Failed to save decision: ' + (e.message || e), 'error');
        }
    },

    _acceptAll: async function() {
        var name = PA.claudeCorrections._analysisName();
        if (!name) return;
        try {
            await PA.api.claudeReview(name, { bulk: 'ACCEPTED' });
            PA.toast('All pending operations accepted', 'success');
            PA.claudeCorrections._checkStatus();
        } catch (e) {
            PA.toast('Failed: ' + (e.message || e), 'error');
        }
    },

    _rejectAll: async function() {
        var name = PA.claudeCorrections._analysisName();
        if (!name) return;
        try {
            await PA.api.claudeReview(name, { bulk: 'REJECTED' });
            PA.toast('All pending operations rejected', 'success');
            PA.claudeCorrections._checkStatus();
        } catch (e) {
            PA.toast('Failed: ' + (e.message || e), 'error');
        }
    },

    _applyChanges: async function() {
        var name = PA.claudeCorrections._analysisName();
        if (!name) return;
        try {
            PA.toast('Applying accepted changes...', 'success');
            var data = await PA.api.claudeApply(name);
            var removed = data.removed || 0;
            var added = data.added || 0;
            PA.toast('Applied: ' + removed + ' removed, ' + added + ' added', 'success');
            PA.claude._refreshVisibleContent();
        } catch (e) {
            PA.toast('Failed to apply: ' + (e.message || e), 'error');
        }
    }
};
