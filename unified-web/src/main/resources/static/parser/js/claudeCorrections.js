window.PA = window.PA || {};

PA.claudeCorrections = {
    _pollId: null,
    _result: null,
    _openCards: null,   // Set of card indices that are expanded

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

        // Static-is-readonly notice — always visible
        html += '<div class="cc-readonly-notice">'
            + '<span class="cc-readonly-icon">&#128274;</span>'
            + ' <b>Static analysis is read-only.</b> Accepting or rejecting corrections only affects the <b>Claude layer</b> — the original static analysis data is never modified.'
            + '</div>';

        html += '<div class="ci-header" id="cc-action-header" style="display:none">';
        html += '<span class="ci-badge" style="background:var(--accent)">CORRECTIONS</span>';
        html += '<span class="ci-title">Review Claude Corrections for ' + esc(name) + '</span>';
        html += '<div class="ci-actions">';
        html += '<button class="btn btn-sm" onclick="PA.claudeCorrections._toggleAllCards()">Toggle All</button>';
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
                // Default: all cards open
                if (!PA.claudeCorrections._openCards) {
                    PA.claudeCorrections._openCards = new Set();
                    for (var ii = 0; ii < result.tables.length; ii++) PA.claudeCorrections._openCards.add(ii);
                }
                showPanel();

                var confirmed = result.confirmedCount || 0;
                var removed   = result.removedCount   || 0;
                var newCount  = result.newCount        || 0;
                var total     = confirmed + removed + newCount;

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

                msgEl.innerHTML = '<span class="ci-badge" style="background:var(--accent)">CORRECTIONS</span> '
                    + total + ' operations &mdash; '
                    + '<span style="color:var(--green)">' + accepted + ' accepted</span>, '
                    + '<span style="color:var(--red)">' + rejected + ' rejected</span>, '
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
        var listEl    = document.getElementById('cc-table-list');
        if (!summaryEl || !listEl) return;

        var esc    = PA.esc;
        var tables = result.tables || [];
        var confirmed = result.confirmedCount || 0;
        var removed   = result.removedCount   || 0;
        var newCount  = result.newCount        || 0;

        summaryEl.innerHTML =
            '<span class="cc-stat"><b>' + tables.length + '</b> tables</span>' +
            '<span class="cc-stat" style="color:var(--green)">' + confirmed + ' confirmed</span>' +
            '<span class="cc-stat" style="color:var(--red)">' + removed + ' removed</span>' +
            '<span class="cc-stat" style="color:var(--accent)">' + newCount + ' new</span>';

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

    // Update just the status bar and summary bar counts — does NOT touch card list DOM
    _updateStatusCounts: function(result) {
        var msgEl = document.getElementById('cc-status-msg');
        var summaryEl = document.getElementById('cc-summary-bar');
        if (!result) return;
        var confirmed = result.confirmedCount || 0;
        var removed   = result.removedCount   || 0;
        var newCount  = result.newCount        || 0;
        var total     = confirmed + removed + newCount;
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
        if (msgEl) {
            msgEl.innerHTML = '<span class="ci-badge" style="background:var(--accent)">CORRECTIONS</span> '
                + total + ' operations &mdash; '
                + '<span style="color:var(--green)">' + accepted + ' accepted</span>, '
                + '<span style="color:var(--red)">' + rejected + ' rejected</span>, '
                + '<span style="color:var(--text-muted)">' + pending + ' pending</span>';
        }
        if (summaryEl) {
            summaryEl.innerHTML =
                '<span class="cc-stat"><b>' + result.tables.length + '</b> tables</span>' +
                '<span class="cc-stat" style="color:var(--green)">' + confirmed + ' confirmed</span>' +
                '<span class="cc-stat" style="color:var(--red)">' + removed + ' removed</span>' +
                '<span class="cc-stat" style="color:var(--accent)">' + newCount + ' new</span>';
        }
    },

    // Re-render a single card in place — preserves all other cards' DOM state
    _rerenderCard: function(idx) {
        var el = document.getElementById('cc-card-' + idx);
        if (!el || !PA.claudeCorrections._result) return;
        var tables = PA.claudeCorrections._result.tables;
        var t = tables[idx];
        if (!t) return;
        var esc = PA.esc;
        el.outerHTML = PA.claudeCorrections._renderCorrectionCard(t, idx, esc);
    },

    _renderCorrectionCard: function(tableResult, idx, esc) {
        var tableName = tableResult.tableName || '';
        var vs        = tableResult.claudeVerifications || [];
        var status    = tableResult.overallStatus || 'UNVERIFIED';
        var isOpen    = PA.claudeCorrections._openCards && PA.claudeCorrections._openCards.has(idx);

        var conf = 0, rem = 0, nw = 0, acc = 0, rej = 0, pend = 0;
        for (var i = 0; i < vs.length; i++) {
            var s  = (vs[i].status       || '').toUpperCase();
            var ud = (vs[i].userDecision || '').toUpperCase();
            if (s === 'CONFIRMED') conf++;
            else if (s === 'REMOVED') rem++;
            else if (s === 'NEW') nw++;
            if (ud === 'ACCEPTED') rej++;        // count by decision
            else if (ud === 'REJECTED') rej++;
            else pend++;
        }
        // recount properly
        acc = 0; rej = 0; pend = 0;
        for (var ii = 0; ii < vs.length; ii++) {
            var ud2 = (vs[ii].userDecision || '').toUpperCase();
            if (ud2 === 'ACCEPTED') acc++;
            else if (ud2 === 'REJECTED') rej++;
            else pend++;
        }

        var html = '<div class="cc-card" id="cc-card-' + idx + '">';

        html += '<div class="cc-card-header" onclick="PA.claudeCorrections._toggleDetail(' + idx + ')">';
        html += '<span class="cc-card-chevron" id="cc-chevron-' + idx + '">' + (isOpen ? '&#9660;' : '&#9654;') + '</span>';
        html += '<span class="ci-table-name">' + esc(tableName) + '</span>';
        html += '<span class="ci-table-status ci-status-' + status.toLowerCase() + '">' + esc(status) + '</span>';
        html += '<span class="ci-table-counts">';
        if (conf) html += '<span class="ci-cnt ci-cnt-confirmed">' + conf + '</span>';
        if (rem)  html += '<span class="ci-cnt ci-cnt-removed">'   + rem  + '</span>';
        if (nw)   html += '<span class="ci-cnt ci-cnt-new">'       + nw   + '</span>';
        html += '<span class="ci-cnt">' + vs.length + ' ops</span>';
        if (acc)  html += '<span class="cc-decision-cnt cc-accepted">'  + acc  + ' accepted</span>';
        if (rej)  html += '<span class="cc-decision-cnt cc-rejected">'  + rej  + ' rejected</span>';
        if (pend) html += '<span class="cc-decision-cnt cc-pending">'   + pend + ' pending</span>';
        html += '</span>';
        html += '</div>';

        html += '<div class="cc-card-detail" id="cc-detail-' + idx + '" style="' + (isOpen ? '' : 'display:none') + '">';

        html += '<div class="cc-side-by-side">';
        html += '<div class="cc-column"><div class="cc-col-header cc-col-static">&#128274; Static Analysis <span class="cc-immutable-tag">immutable</span></div>';
        for (var j = 0; j < vs.length; j++) {
            var v = vs[j];
            if ((v.status || '').toUpperCase() === 'NEW') continue;
            html += '<div class="cc-op-row">';
            html += '<span class="ci-op-badge ci-op-' + (v.operation || 'other').toLowerCase() + '">' + esc(v.operation || '?') + '</span>';
            if (v.procedureName) html += '<span class="cc-proc">' + esc(v.procedureName) + '</span>';
            if (v.lineNumber)    html += '<span class="cc-line">L' + v.lineNumber + '</span>';
            html += '</div>';
        }
        html += '</div>';

        html += '<div class="cc-column"><div class="cc-col-header cc-col-claude">&#129302; Claude Verification</div>';
        for (var k = 0; k < vs.length; k++) {
            var vv       = vs[k];
            var decision = (vv.userDecision || '').toUpperCase();
            var tblSafe  = tableName.replace(/'/g, "\\'");
            var opSafe   = (vv.operation     || '').replace(/'/g, "\\'");
            var procSafe = (vv.procedureName || '').replace(/'/g, "\\'");
            var ln       = vv.lineNumber || 0;

            html += '<div class="cc-op-row">';
            html += '<span class="ci-op-badge ci-op-' + (vv.operation || 'other').toLowerCase() + '">' + esc(vv.operation || '?') + '</span>';
            html += '<span class="ci-v-status ci-v-' + (vv.status || '').toLowerCase() + '">' + esc(vv.status || '?') + '</span>';
            if (vv.procedureName) html += '<span class="cc-proc">' + esc(vv.procedureName) + '</span>';
            if (vv.lineNumber)    html += '<span class="cc-line">L' + vv.lineNumber + '</span>';
            if (vv.reason)        html += '<span class="ci-v-reason">' + esc(vv.reason) + '</span>';

            html += '<span class="cc-decision-btns">';
            html += '<button class="cc-btn cc-btn-accept' + (decision === 'ACCEPTED' ? ' active' : '') + '" '
                + 'onclick="PA.claudeCorrections._decide(\'' + tblSafe + '\',\'' + opSafe + '\',\'' + procSafe + '\',' + ln + ',' + idx + ',\'ACCEPTED\');event.stopPropagation()">Accept</button>';
            html += '<button class="cc-btn cc-btn-reject' + (decision === 'REJECTED' ? ' active' : '') + '" '
                + 'onclick="PA.claudeCorrections._decide(\'' + tblSafe + '\',\'' + opSafe + '\',\'' + procSafe + '\',' + ln + ',' + idx + ',\'REJECTED\');event.stopPropagation()">Reject</button>';
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
        var ch = document.getElementById('cc-chevron-' + idx);
        if (!el) return;
        var nowOpen = el.style.display === 'none';
        el.style.display = nowOpen ? '' : 'none';
        if (ch) ch.innerHTML = nowOpen ? '&#9660;' : '&#9654;';
        if (!PA.claudeCorrections._openCards) PA.claudeCorrections._openCards = new Set();
        if (nowOpen) PA.claudeCorrections._openCards.add(idx);
        else         PA.claudeCorrections._openCards.delete(idx);
    },

    _toggleAllCards: function() {
        var tables = PA.claudeCorrections._result ? PA.claudeCorrections._result.tables : [];
        var anyOpen = PA.claudeCorrections._openCards && PA.claudeCorrections._openCards.size > 0;
        PA.claudeCorrections._openCards = new Set();
        for (var i = 0; i < tables.length; i++) {
            var el = document.getElementById('cc-detail-' + i);
            var ch = document.getElementById('cc-chevron-' + i);
            if (!el) continue;
            if (anyOpen) {
                el.style.display = 'none';
                if (ch) ch.innerHTML = '&#9654;';
            } else {
                el.style.display = '';
                if (ch) ch.innerHTML = '&#9660;';
                PA.claudeCorrections._openCards.add(i);
            }
        }
    },

    _decide: async function(tableName, operation, procedureName, lineNumber, cardIdx, decision) {
        var name = PA.claudeCorrections._analysisName();
        if (!name) return;

        try {
            await PA.api.claudeReview(name, {
                decisions: [{
                    tableName:     tableName,
                    operation:     operation,
                    procedureName: procedureName,
                    lineNumber:    lineNumber,
                    decision:      decision
                }]
            });

            // Update local model
            if (PA.claudeCorrections._result) {
                for (var ti = 0; ti < PA.claudeCorrections._result.tables.length; ti++) {
                    var t = PA.claudeCorrections._result.tables[ti];
                    if ((t.tableName || '').toUpperCase() !== tableName.toUpperCase()) continue;
                    var vs = t.claudeVerifications || [];
                    for (var vi = 0; vi < vs.length; vi++) {
                        var v = vs[vi];
                        if ((v.operation      || '').toUpperCase() === operation.toUpperCase()
                            && (v.procedureName || '')               === procedureName
                            && (v.lineNumber    || 0)                === lineNumber) {
                            v.userDecision = decision;
                        }
                    }
                }
                // Re-render only the affected card — other cards stay open/closed as-is
                PA.claudeCorrections._rerenderCard(cardIdx);
                // Update summary bar and status counts only (not the card list)
                PA.claudeCorrections._updateStatusCounts(PA.claudeCorrections._result);
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
            PA.claudeCorrections._openCards = null; // reset so all open on reload
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
            PA.claudeCorrections._openCards = null;
            PA.claudeCorrections._checkStatus();
        } catch (e) {
            PA.toast('Failed: ' + (e.message || e), 'error');
        }
    },

    _applyChanges: async function() {
        var name = PA.claudeCorrections._analysisName();
        if (!name) return;
        try {
            PA.toast('Applying accepted changes to Claude layer...', 'success');
            var data = await PA.api.claudeApply(name);
            var removed = data.removed || 0;
            var added   = data.added   || 0;
            PA.toast('Applied to Claude layer: ' + removed + ' removed, ' + added + ' added — static analysis unchanged', 'success');
            PA.claude._refreshVisibleContent();
        } catch (e) {
            PA.toast('Failed to apply: ' + (e.message || e), 'error');
        }
    }
};
