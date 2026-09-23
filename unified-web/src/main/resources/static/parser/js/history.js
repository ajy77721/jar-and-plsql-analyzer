PA._allHistory = [];
PA._historySearchQuery = '';

PA.quickSubmit = {
    toggle() {
        var panel = document.getElementById('quickSubmitPanel');
        if (!panel) return;
        var shown = panel.style.display !== 'none';
        panel.style.display = shown ? 'none' : '';
        if (!shown) {
            var ta = document.getElementById('quickSubmitInput');
            if (ta) ta.focus();
        }
    },
    close() {
        var panel = document.getElementById('quickSubmitPanel');
        // A closing panel must not keep a query it has stopped applying; see
        // SearchQuery.resetWithin in shared/ui-busy.js.
        if (window.SearchQuery) SearchQuery.resetWithin(panel);
        if (panel) panel.style.display = 'none';
    },
    async submitAll() {
        var ta = document.getElementById('quickSubmitInput');
        var statusEl = document.getElementById('quickSubmitStatus');
        var resultsEl = document.getElementById('quickSubmitResults');
        if (!ta) return;

        var lines = ta.value.split('\n').map(function(l) { return l.trim(); }).filter(function(l) { return l.length > 0; });
        if (lines.length === 0) { if (statusEl) statusEl.textContent = 'Nothing to submit'; return; }

        var formData = PA.home ? PA.home.getFormData() : {};
        if (statusEl) statusEl.textContent = 'Submitting ' + lines.length + ' job(s)…';
        if (resultsEl) resultsEl.innerHTML = '';

        var submitted = 0, failed = 0;
        var resultHtml = '';
        for (var i = 0; i < lines.length; i++) {
            var entry = lines[i];
            // Parse SCHEMA.PKG.PROC or PKG.PROC or PROC
            var parts = entry.split('.');
            var entryPoint = parts[parts.length - 1];
            var owner = formData.owner || '';
            // If 3 parts: SCHEMA.PKG → entryPoint = PKG.PROC, owner = SCHEMA
            if (parts.length === 3) {
                entryPoint = parts[1] + '.' + parts[2];
                if (!owner) owner = parts[0].toUpperCase();
            } else if (parts.length === 2) {
                entryPoint = entry; // PKG.PROC
            }

            var body = { entryPoint: entryPoint };
            if (owner) body.owner = owner;
            if (formData.project) body.project = formData.project;
            if (formData.environment) body.environment = formData.environment;
            // Forward dialect context so non-Oracle batches are not treated as Oracle.
            if (formData.dbType) body.dbType = formData.dbType;
            if (formData.connName) body.connName = formData.connName;
            // Use the selected object type; only default to PACKAGE for Oracle.
            body.objectType = formData.objectType
                || ((formData.dbType && formData.dbType !== 'ORACLE') ? 'PROCEDURE' : 'PACKAGE');

            try {
                var resp = await PA.api.runAnalysis(entryPoint, body);
                if (resp && resp.error) {
                    failed++;
                    resultHtml += '<div style="color:var(--red)">&#10005; ' + PA.esc(entry) + ': ' + PA.esc(resp.error) + '</div>';
                } else {
                    submitted++;
                    resultHtml += '<div style="color:var(--green)">&#10003; ' + PA.esc(entry) + ' queued</div>';
                    // Track each batch job the same way a single Analyze does (gap #8) — fire and
                    // forget; PA._waitForJob resolves on its own once the job finishes and keeps
                    // this job visible in the persistent indicator + toasts/refreshes history.
                    if (resp && resp.jobId) PA._waitForJob(resp.jobId, null, null, null, entry);
                }
            } catch (e) {
                failed++;
                resultHtml += '<div style="color:var(--red)">&#10005; ' + PA.esc(entry) + ': ' + PA.esc(e.message || 'error') + '</div>';
            }
        }

        if (statusEl) statusEl.textContent = submitted + ' queued' + (failed > 0 ? ', ' + failed + ' failed' : '');
        if (resultsEl) resultsEl.innerHTML = resultHtml;

        if (submitted > 0) {
            PA.switchHomeTab('queue');
            PA.toast(submitted + ' job(s) submitted to queue', 'success');
        }
    }
};

/**
 * Rows per history request (Q31 / I11).
 *
 * Generous rather than a screenful: both this list and the Performance Summary filter and sort the
 * set in the browser, which is correct and faster than a round trip while the whole set fits in one
 * page. When it stops fitting, `_historyTruncated` is set and the search box starts asking the
 * server instead — the switch is automatic, not a follow-up task.
 */
PA._HISTORY_PAGE = 200;
PA._historyTruncated = false;
PA._historyFacets = null;

PA.loadHistory = async function() {
    try {
        var env = await PA.api.listHistoryPage({
            limit: PA._HISTORY_PAGE,
            q: PA._historyTruncated ? PA._historySearchQuery : null,
            cf: PA._historyTruncated ? PA._historyServerFilters() : null
        });
        // A server without the paged endpoint answers a bare array; the shim wraps it and this
        // screen behaves exactly as it did before.
        PA._allHistory = env ? env.items : (await PA.api.listHistory() || []);
        PA._historyTruncated = !!(env && env.page && env.page.hasMore);
        PA._historyTotal = (env && env.page && env.page.totalIsExact)
            ? env.page.total : PA._allHistory.length;
        PA._historyFacets = await PA.api.historyFacets({
            q: PA._historyTruncated ? PA._historySearchQuery : null,
            cf: PA._historyTruncated ? PA._historyServerFilters() : null
        });
        PA._renderFilteredHistory();
        if (document.getElementById('homeTabPerfSummary') && document.getElementById('homeTabPerfSummary').style.display !== 'none') {
            PA.perfSummary.render();
        }
    } catch (e) {
        document.getElementById('historyList').innerHTML = '<div class="empty-msg">Could not load history</div>';
    }
};

PA._historyDsFilter = '';
PA._historyProjFilter = '';

/**
 * Datasource dialect — read, not derived (PLAN-08 §4.3 #13).
 *
 * The jdbcUrl substring scanner that used to live here also lived in `perfSummary.js:13-22`, and
 * the two copies disagreed on the fallback for an unrecognised URL: this one showed an em dash,
 * that one claimed ORACLE. The server now derives it once, into `dbType`, so both screens show the
 * same thing and the URL no longer has to be sent to the browser at all.
 */
PA._dialectOf = function(item) {
    return (item.dbType || '').toUpperCase() || '—';
};

/** The datasource / project selections as server-side column filters. */
PA._historyServerFilters = function() {
    var cf = [];
    if (PA._historyDsFilter) cf.push('dbType:' + PA._historyDsFilter);
    if (PA._historyProjFilter) cf.push('project:' + PA._historyProjFilter);
    return cf;
};
PA._DS_LABEL = { ORACLE: 'Oracle', SQLSERVER: 'SQL Server', MYSQL: 'MySQL', DB2: 'Db2', POSTGRES: 'PostgreSQL' };

PA.onHistorySearch = function(q) {
    PA._historySearchQuery = (q || '').toUpperCase();
    // Once the history no longer fits in one page, a browser-side search would search that page
    // and present the result as the whole history. Ask the server instead, debounced.
    if (PA._historyTruncated) {
        if (PA._historySearchTimer) clearTimeout(PA._historySearchTimer);
        PA._historySearchTimer = setTimeout(function() { PA.loadHistory(); }, 250);
        return;
    }
    PA._renderFilteredHistory();
};
PA.onHistoryDs = function(v) {
    PA._historyDsFilter = v;
    if (!v) {
        // Datasource = All → reset project to All too.
        PA._historyProjFilter = '';
    } else {
        // Cascade: if the selected project doesn't belong to this datasource, clear it.
        var p = PA._historyProjFilter;
        if (p && PA._historyProjToDs && PA._historyProjToDs[p] !== v) PA._historyProjFilter = '';
    }
    if (PA._historyTruncated) { PA.loadHistory(); return; }
    PA._renderFilteredHistory();
};
PA.onHistoryProj = function(v) {
    PA._historyProjFilter = v;
    // Cascade: selecting a project auto-sets its datasource (a project has exactly one).
    if (v && PA._historyProjToDs && PA._historyProjToDs[v]) PA._historyDsFilter = PA._historyProjToDs[v];
    if (PA._historyTruncated) { PA.loadHistory(); return; }
    PA._renderFilteredHistory();
};

PA._renderHistoryFilters = function() {
    var host = document.getElementById('historyFilters');
    if (!host) return;
    var dsSeen = {}, projToDs = {};
    // Prefer the server's facet lists. They cover the whole history, so once the list is paged the
    // dropdowns still offer every value that exists rather than only those on the current page —
    // a filter that silently omits its own options is worse than no filter.
    var f = PA._historyFacets;
    if (f) {
        Object.keys(f.dsCounts || {}).forEach(function(k) { dsSeen[k] = 1; });
        Object.keys(f.projectToDs || {}).forEach(function(p) { projToDs[p] = f.projectToDs[p]; });
    } else {
        (PA._allHistory || []).forEach(function(it) {
            var dl = PA._dialectOf(it);
            dsSeen[dl] = 1;
            if (it.project) projToDs[it.project] = dl;   // project -> its datasource
        });
    }
    PA._historyProjToDs = projToDs;
    // Drop a stale filter whose value no longer exists in the reloaded data, so the dropdown's
    // visible "All" matches what actually gets filtered (avoids a confusing empty list).
    if (PA._historyDsFilter && !dsSeen[PA._historyDsFilter]) PA._historyDsFilter = '';
    if (PA._historyProjFilter && !projToDs[PA._historyProjFilter]) PA._historyProjFilter = '';
    var ds = '<option value="">All datasources</option>';
    // Derived from the label map, NOT a literal list.
    //
    // This was ['ORACLE','SQLSERVER','MYSQL','DB2'] — four entries, written before PostgreSQL
    // support existed. The server reported all five datasources correctly and the dropdown simply
    // never offered Postgres, so a Postgres analysis was present in the table but unfilterable.
    // The comment fifteen lines above says "a filter that silently omits its own options is worse
    // than no filter"; a hardcoded list is how that happens anyway.
    //
    // _DS_LABEL is the single place a dialect is named, so a sixth dialect appears here for free.
    Object.keys(PA._DS_LABEL).forEach(function(k) {
        if (dsSeen[k]) ds += '<option value="' + k + '"' + (PA._historyDsFilter === k ? ' selected' : '') + '>' + PA._DS_LABEL[k] + '</option>';
    });
    var pr = '<option value="">All projects</option>';
    Object.keys(projToDs).sort().forEach(function(p) {
        // Cascade: when a datasource is selected, only list projects under it.
        if (PA._historyDsFilter && projToDs[p] !== PA._historyDsFilter) return;
        pr += '<option value="' + PA.esc(p) + '"' + (PA._historyProjFilter === p ? ' selected' : '') + '>' + PA.esc(p) + '</option>';
    });
    host.innerHTML = '<select class="ps-filter-select" onchange="PA.onHistoryDs(this.value)">' + ds + '</select>'
                   + '<select class="ps-filter-select" onchange="PA.onHistoryProj(this.value)">' + pr + '</select>';
};

PA._renderFilteredHistory = function() {
    var container = document.getElementById('historyList');
    var countEl = document.getElementById('historyCount');
    PA._renderHistoryFilters();
    var list = PA._allHistory || [];
    var q = PA._historySearchQuery, ds = PA._historyDsFilter, proj = PA._historyProjFilter;
    list = list.filter(function(item) {
        if (ds && PA._dialectOf(item) !== ds) return false;
        if (proj && item.project !== proj) return false;
        if (q) {
            var txt = ((item.entryPoint || '') + ' ' + (item.entrySchema || '') + ' ' + (item.name || '') + ' ' + (item.connectionName || '') + ' ' + (item.project || '')).toUpperCase();
            if (!txt.includes(q)) return false;
        }
        return true;
    });
    // The rendered count, and the server's total when it is larger — so a truncated list never
    // reads as if it were the whole history.
    if (countEl) {
        countEl.textContent = (PA._historyTruncated && PA._historyTotal > list.length)
            ? list.length + ' of ' + PA._historyTotal
            : list.length;
    }

    if (list.length === 0) {
        container.innerHTML = '<div class="empty-msg">No analyses found</div>';
        return;
    }

    var html = '';
    for (var i = 0; i < list.length; i++) {
        var item = list[i];
        var entry = item.entryPoint || item.name || '';
        var schema = item.entrySchema || '';
        var ts = item.timestamp ? new Date(item.timestamp).toLocaleString() : '';
        var procs = item.totalNodes || 0;
        var tables = item.totalTables || 0;
        var loc = item.totalLinesOfCode || 0;
        // errorCount from the projected row; item.errors only on a legacy unpaged response.
        var errors = item.errorCount !== undefined
            ? item.errorCount : ((item.errors && item.errors.length) || 0);
        var duration = PA.formatDurationShort(item.totalDurationMs || item.crawlTimeMs);
        var proj = item.project || '';
        var env = item.environment || '';
        var connName = item.connectionName || '';

        html += '<div class="history-card" onclick="PA.loadAnalysis(\'' + PA.escJs(item.name) + '\')">';
        html += '<div class="hc-info">';
        html += '<div class="hc-title">' + PA.esc(entry);
        if (schema) html += ' <span style="color:var(--text-muted);font-size:11px">(' + PA.esc(schema) + ')</span>';
        html += '</div>';
        if (proj || env) {
            html += '<div class="hc-env">';
            html += '<span class="hc-env-badge ds">' + PA.esc(PA._DS_LABEL[PA._dialectOf(item)] || PA._dialectOf(item)) + '</span>';
            if (proj) html += '<span class="hc-env-badge proj">' + PA.esc(proj) + '</span>';
            if (env) html += '<span class="hc-env-badge env">' + PA.esc(env) + '</span>';
            if (connName) html += '<span class="hc-env-badge conn">' + PA.esc(connName) + '</span>';
            html += '</div>';
        }
        if (ts) html += '<div class="hc-meta">' + PA.esc(ts) + '</div>';
        html += '<div class="hc-stats">' + procs + ' procs | ' + tables + ' tables | ' + loc.toLocaleString() + ' LOC';
        if (duration) html += ' | ' + PA.esc(duration);
        if (errors > 0) html += ' | <span style="color:var(--red)">' + errors + ' errors</span>';
        html += '</div></div>';
        html += '<div class="hc-actions">';
        html += '<button class="btn btn-sm btn-primary" onclick="event.stopPropagation(); PA.loadAnalysis(\'' + PA.escJs(item.name) + '\')">Load</button>';
        html += '<button class="btn btn-sm btn-danger" onclick="event.stopPropagation(); PA.deleteAnalysis(\'' + PA.escJs(item.name) + '\')">Delete</button>';
        html += '</div></div>';
    }
    container.innerHTML = html;
};

PA.deleteAnalysis = async function(name) {
    if (!confirm('Delete analysis "' + name + '"?')) return;
    try {
        var res = await fetch('/api/parser/analyses/' + encodeURIComponent(name), { method: 'DELETE' });
        if (res.ok) {
            PA.toast('Deleted: ' + name, 'success');
            var before = (PA._allHistory || []).length;
            PA._allHistory = (PA._allHistory || []).filter(function(item) { return item.name !== name; });
            // The "N of M" label reads _historyTotal, which is the server's count from the last
            // page fetch. Removing the row locally without decrementing it left the truncated
            // list claiming a total that included the analysis just deleted.
            if (typeof PA._historyTotal === 'number' && PA._allHistory.length < before) {
                PA._historyTotal = Math.max(PA._allHistory.length, PA._historyTotal - 1);
            }
            PA._renderFilteredHistory();
        } else {
            PA.toast('Could not delete', 'error');
        }
    } catch (e) {
        PA.toast('Delete failed: ' + (e.message || e), 'error');
    }
};

PA.startAnalysis = async function() {
    var formData = PA.home ? PA.home.getFormData() : {};
    var entryPoint = formData.entryPoint || '';
    if (!entryPoint) {
        var objectInput = document.getElementById('homeObject');
        entryPoint = objectInput ? objectInput.value.trim() : '';
    }
    if (!entryPoint) {
        PA.toast('Enter an object name (e.g. PKG_CUSTOMER or SCHEMA.PKG_NAME)', 'error');
        return;
    }

    var prog = document.getElementById('homeProgress');
    var progText = document.getElementById('homeProgressText');
    var progFill = document.getElementById('homeProgressFill');
    if (prog) { prog.style.display = 'block'; }
    if (progFill) progFill.style.width = '5%';
    if (progText) progText.textContent = 'Queuing ' + entryPoint + '...';

    try {
        var body = { entryPoint: entryPoint };
        if (formData.owner)       body.owner       = formData.owner;
        if (formData.objectType)  body.objectType  = formData.objectType;
        if (formData.project)     body.project     = formData.project;
        if (formData.environment) body.environment = formData.environment;
        if (formData.dbType)      body.dbType      = formData.dbType;
        if (formData.connName)    body.connName    = formData.connName;

        var resp = await PA.api.runAnalysis(entryPoint, body);
        if (resp && resp.error) { if (prog) prog.style.display = 'none'; PA.toast(resp.error, 'error'); return; }

        // Switch to Queue tab so user can watch progress
        PA.switchHomeTab('queue');

        var jobId = resp && resp.jobId;
        if (!jobId) {
            // Unexpected non-queued response — treat as instant complete (backward compat)
            if (prog) prog.style.display = 'none';
            PA.toast('Analysis submitted', 'success');
            await PA.loadHistory();
            return;
        }

        if (progText) progText.textContent = 'Queued — waiting to start...';
        if (progFill) progFill.style.width = '10%';

        // Poll job status via SSE until done
        await PA._waitForJob(jobId, prog, progText, progFill, entryPoint);

    } catch (e) {
        if (prog) prog.style.display = 'none';
        var failMsg = 'Analysis failed to start: ' + (e.message || e);
        PA.toast(failMsg, 'error');
        if (PA.showJobBanner) PA.showJobBanner(failMsg, 'error');
    }
};

/**
 * Tracks one queued job to completion via SSE (falling back to polling), driving both the
 * caller's own progress bar (if it passed one — `startAnalysis` does, `quickSubmit` doesn't) and
 * the persistent cross-screen indicator in `PA.jobTracker` (gap #1/#2/#6), which is updated
 * regardless so the job stays visible even after the caller's own UI is gone (e.g. user
 * navigated away, or this was a Quick Submit batch job with no dedicated bar at all).
 */
PA._waitForJob = function(jobId, prog, progText, progFill, label) {
    if (PA.jobTracker) PA.jobTracker.register(jobId, label);
    return new Promise(function(resolve) {
        var es = null;
        var done = false;

        function finish(resultName, failed, errMsg) {
            if (done) return;
            done = true;
            if (es) { try { es.close(); } catch(e){} es = null; }
            if (prog) prog.style.display = 'none';
            if (PA.jobTracker) PA.jobTracker.remove(jobId);
            if (failed) {
                // Show whatever real reason the server gave (job.error) rather than just the
                // bare status word, and surface it via a persistent banner too (gap #3/#4/#9) —
                // a 4s toast is easy to miss if the user stepped away while this job ran.
                var reason = errMsg && String(errMsg).trim() && errMsg !== 'FAILED' && errMsg !== 'CANCELLED'
                    ? errMsg : 'unknown error (status: ' + (errMsg || 'FAILED') + ')';
                var msg = 'Analysis failed' + (label ? ' — ' + label : '') + ': ' + reason;
                PA.toast(msg, 'error');
                if (PA.showJobBanner) PA.showJobBanner(msg, 'error');
            } else {
                PA.toast('Analysis complete' + (label ? ' — ' + label : ''), 'success');
            }
            PA.loadHistory().then(function() {
                if (resultName) PA.loadAnalysis(resultName);
            });
            resolve();
        }

        function poll() {
            fetch('/api/queue/' + encodeURIComponent(jobId))
                .then(function(r) { return r.json(); })
                .then(function(job) {
                    var status = (job.status || '').toUpperCase();
                    var pct = job.progressPercent || 0;
                    var stepText = job.currentStep || job.lastMessage || 'Running...';
                    if (progFill) progFill.style.width = Math.max(10, pct) + '%';
                    if (progText) progText.textContent = stepText;
                    if (PA.jobTracker) PA.jobTracker.update(jobId, Math.max(10, pct), stepText);
                    if (status === 'COMPLETE') {
                        if (progFill) progFill.style.width = '100%';
                        finish(job.resultName, false, null);
                    } else if (status === 'FAILED' || status === 'CANCELLED') {
                        finish(null, true, job.error || status);
                    }
                })
                .catch(function() {});
        }

        // Listen for SSE queue events; fall back to polling
        try {
            es = new EventSource('/api/queue/events');
            var handleEvent = function(e) {
                try {
                    var envelope = JSON.parse(e.data);
                    // Queue events are {eventType, job, state}; the job fields live under .job.
                    // This used to read `data.id` off the envelope, which is always undefined, so
                    // the guard below rejected every event and this screen has only ever worked
                    // via its polling fallback. Tolerate a bare job too, in case an older server
                    // is on the other end.
                    var data = (envelope && envelope.job) ? envelope.job : envelope;
                    if (!data || data.id !== jobId) return;
                    var status = (data.status || '').toUpperCase();
                    var pct = data.progressPercent || 0;
                    var stepText = data.currentStep || data.lastMessage || 'Running...';
                    if (progFill) progFill.style.width = Math.max(10, pct) + '%';
                    if (progText) progText.textContent = stepText;
                    if (PA.jobTracker) PA.jobTracker.update(jobId, Math.max(10, pct), stepText);
                    if (status === 'COMPLETE') {
                        if (progFill) progFill.style.width = '100%';
                        finish(data.resultName, false, null);
                    } else if (status === 'FAILED' || status === 'CANCELLED') {
                        finish(null, true, data.error || status);
                    }
                } catch(ex) {}
            };
            ['job-progress', 'job-complete', 'job-failed'].forEach(function(evt) {
                es.addEventListener(evt, handleEvent);
            });
            es.onerror = function() {
                try { es.close(); } catch(e){} es = null;
                // Fall back to polling every 2s — flag it so the user isn't left guessing why
                // the indicator went quiet for a beat (gap #7).
                if (!done) {
                    if (PA.jobTracker) PA.jobTracker.markPolling(jobId);
                    PA.toast('Live updates lost — switched to polling', 'error');
                    var pollHandle = PollConfig.every('jobStatusMs', function() {
                        if (done) { pollHandle = PollConfig.clear(pollHandle); return; }
                        poll();
                    });
                }
            };
        } catch(e) {
            // SSE not available — poll every 2s
            if (PA.jobTracker) PA.jobTracker.markPolling(jobId);
            var pollHandle = PollConfig.every('jobStatusMs', function() {
                if (done) { pollHandle = PollConfig.clear(pollHandle); return; }
                poll();
            });
        }
    });
};
