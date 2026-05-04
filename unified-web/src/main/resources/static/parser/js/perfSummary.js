PA.perfSummary = {
    _sortCol: 'duration',
    _sortDir: 'desc',
    _data: [],

    render: function() {
        var list = PA._allHistory || [];
        PA.perfSummary._data = list;
        var container = document.getElementById('perfSummaryContainer');
        if (!container) return;
        if (list.length === 0) { container.innerHTML = '<div class="empty-msg">No analyses yet</div>'; return; }

        var totalTime = 0, totalDb = 0, totalSuccess = 0, totalErrors = 0, totalProcs = 0, totalTables = 0, totalLoc = 0;
        for (var i = 0; i < list.length; i++) {
            var it = list[i];
            totalTime += (it.crawlTimeMs || 0);
            totalDb += (it.dbCallCount || 0);
            totalProcs += (it.totalNodes || 0);
            totalTables += (it.totalTables || 0);
            totalLoc += (it.totalLinesOfCode || 0);
            if (it.errors && it.errors.length > 0) totalErrors++; else totalSuccess++;
        }

        var html = '<div class="ps-overview">';
        html += PA.perfSummary._statCard(list.length, 'Analyses');
        html += PA.perfSummary._statCard(totalSuccess, 'Success', '#15803d');
        html += PA.perfSummary._statCard(totalErrors, 'With Errors', totalErrors > 0 ? '#b91c1c' : '#15803d');
        html += PA.perfSummary._statCard(PA.formatDuration(totalTime), 'Total Time');
        html += PA.perfSummary._statCard(totalDb, 'DB Calls');
        html += PA.perfSummary._statCard(totalProcs.toLocaleString(), 'Procedures');
        html += PA.perfSummary._statCard(totalTables.toLocaleString(), 'Tables');
        html += PA.perfSummary._statCard(totalLoc.toLocaleString(), 'LOC');
        html += '</div>';

        html += '<div class="ps-table-wrap">';
        html += PA.perfSummary._renderTable(list);
        html += '</div>';

        container.innerHTML = html;
    },

    _statCard: function(value, label, color) {
        var style = color ? 'color:' + color : '';
        return '<div class="ps-stat-card"><div class="ps-stat-value" style="' + style + '">' + value + '</div><div class="ps-stat-label">' + PA.esc(label) + '</div></div>';
    },

    _renderTable: function(list) {
        var sorted = list.slice().sort(PA.perfSummary._comparator());
        var maxTime = 0;
        for (var i = 0; i < sorted.length; i++) {
            if ((sorted[i].crawlTimeMs || 0) > maxTime) maxTime = sorted[i].crawlTimeMs;
        }

        var sc = PA.perfSummary._sortCol, sd = PA.perfSummary._sortDir;
        var arrow = function(col) { return sc === col ? (sd === 'asc' ? ' ▲' : ' ▼') : ''; };

        var html = '<table class="ps-table"><thead><tr>';
        html += '<th onclick="PA.perfSummary._sort(\'name\')">#' + arrow('name') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'entry\')">Entry Point' + arrow('entry') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'status\')">Status' + arrow('status') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'duration\')">Duration' + arrow('duration') + '</th>';
        html += '<th>Time Bar</th>';
        html += '<th onclick="PA.perfSummary._sort(\'db\')">DB Calls' + arrow('db') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'procs\')">Procs' + arrow('procs') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'tables\')">Tables' + arrow('tables') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'loc\')">LOC' + arrow('loc') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'errors\')">Errors' + arrow('errors') + '</th>';
        html += '</tr></thead><tbody>';

        for (var i = 0; i < sorted.length; i++) {
            var it = sorted[i];
            var hasErr = it.errors && it.errors.length > 0;
            var errCount = hasErr ? it.errors.length : 0;
            var statusCls = hasErr ? (errCount > 0 && (it.totalNodes || 0) > 0 ? 'partial' : 'error') : 'success';
            var statusLabel = hasErr ? (statusCls === 'partial' ? 'Partial' : 'Error') : 'Success';
            var ms = it.crawlTimeMs || 0;
            var pct = maxTime > 0 ? Math.round(ms / maxTime * 100) : 0;
            var barColor = statusCls === 'success' ? '#22c55e' : statusCls === 'partial' ? '#f59e0b' : '#ef4444';

            html += '<tr onclick="PA.perfSummary.showDetail(' + i + ')" data-idx="' + i + '">';
            html += '<td style="color:var(--text-muted);font-size:10px">' + (i + 1) + '</td>';
            html += '<td style="font-weight:600;font-size:11px">' + PA.esc(it.entryPoint || it.name || '') + '</td>';
            html += '<td><span class="ps-status ' + statusCls + '">' + statusLabel + '</span></td>';
            html += '<td style="white-space:nowrap;font-family:monospace;font-size:11px">' + PA.formatDuration(ms) + '</td>';
            html += '<td><div class="ps-bar"><div class="ps-bar-fill" style="width:' + pct + '%;background:' + barColor + '"></div></div></td>';
            html += '<td style="text-align:center;font-weight:600">' + (it.dbCallCount || 0) + '</td>';
            html += '<td style="text-align:center">' + (it.totalNodes || 0) + '</td>';
            html += '<td style="text-align:center">' + (it.totalTables || 0) + '</td>';
            html += '<td style="text-align:center">' + (it.totalLinesOfCode || 0).toLocaleString() + '</td>';
            html += '<td style="text-align:center">';
            if (errCount > 0) html += '<span style="color:#ef4444;font-weight:700">' + errCount + '</span>';
            else html += '<span style="color:#22c55e">0</span>';
            html += '</td></tr>';
        }

        html += '</tbody></table>';
        return html;
    },

    _comparator: function() {
        var col = PA.perfSummary._sortCol, dir = PA.perfSummary._sortDir;
        var m = dir === 'asc' ? 1 : -1;
        return function(a, b) {
            var va, vb;
            switch (col) {
                case 'name': va = (a.name || ''); vb = (b.name || ''); return va.localeCompare(vb) * m;
                case 'entry': va = (a.entryPoint || ''); vb = (b.entryPoint || ''); return va.localeCompare(vb) * m;
                case 'status':
                    va = (a.errors && a.errors.length) ? 1 : 0;
                    vb = (b.errors && b.errors.length) ? 1 : 0;
                    return (va - vb) * m;
                case 'duration': return ((a.crawlTimeMs || 0) - (b.crawlTimeMs || 0)) * m;
                case 'db': return ((a.dbCallCount || 0) - (b.dbCallCount || 0)) * m;
                case 'procs': return ((a.totalNodes || 0) - (b.totalNodes || 0)) * m;
                case 'tables': return ((a.totalTables || 0) - (b.totalTables || 0)) * m;
                case 'loc': return ((a.totalLinesOfCode || 0) - (b.totalLinesOfCode || 0)) * m;
                case 'errors':
                    va = (a.errors ? a.errors.length : 0);
                    vb = (b.errors ? b.errors.length : 0);
                    return (va - vb) * m;
                default: return 0;
            }
        };
    },

    _sort: function(col) {
        if (PA.perfSummary._sortCol === col) {
            PA.perfSummary._sortDir = PA.perfSummary._sortDir === 'asc' ? 'desc' : 'asc';
        } else {
            PA.perfSummary._sortCol = col;
            PA.perfSummary._sortDir = col === 'name' || col === 'entry' ? 'asc' : 'desc';
        }
        PA.perfSummary.render();
    },

    showDetail: function(idx) {
        var sorted = (PA._allHistory || []).slice().sort(PA.perfSummary._comparator());
        var it = sorted[idx];
        if (!it) return;

        var hasErr = it.errors && it.errors.length > 0;
        var statusCls = hasErr ? 'error' : 'success';
        var statusLabel = hasErr ? 'Error' : 'Success';

        var html = '<div class="ps-detail-overlay" onclick="this.remove()">';
        html += '<div class="ps-detail-panel" onclick="event.stopPropagation()">';
        html += '<div class="ps-detail-header"><h3>' + PA.esc(it.entryPoint || it.name) + '</h3>';
        html += '<button class="btn btn-sm" onclick="this.closest(\'.ps-detail-overlay\').remove()">&times;</button></div>';
        html += '<div class="ps-detail-body">';

        html += '<div class="ps-detail-section"><div class="ps-detail-section-title">Analysis Overview</div>';
        html += '<div class="ps-detail-grid">';
        html += '<div class="ps-detail-kv"><span class="ps-k">Status</span><span class="ps-v"><span class="ps-status ' + statusCls + '">' + statusLabel + '</span></span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Schema</span><span class="ps-v">' + PA.esc(it.entrySchema || '-') + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Duration</span><span class="ps-v">' + PA.formatDuration(it.crawlTimeMs) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">DB Calls</span><span class="ps-v">' + (it.dbCallCount || 0) + '</span></div>';
        html += '</div></div>';

        html += '<div class="ps-detail-section"><div class="ps-detail-section-title">Metrics</div>';
        html += '<div class="ps-detail-grid">';
        html += '<div class="ps-detail-kv"><span class="ps-k">Procedures</span><span class="ps-v">' + (it.totalNodes || 0) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Tables</span><span class="ps-v">' + (it.totalTables || 0) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Edges</span><span class="ps-v">' + (it.totalEdges || 0) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">LOC</span><span class="ps-v">' + (it.totalLinesOfCode || 0).toLocaleString() + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Max Depth</span><span class="ps-v">' + (it.maxDepth || 0) + '</span></div>';
        html += '</div></div>';

        html += '<div class="ps-detail-section" id="psClaudeStats"><div class="ps-detail-section-title">Claude Verification</div>';
        html += '<div class="ps-detail-grid" id="psClaudeGrid"><div style="color:var(--text-muted);font-size:11px;grid-column:span 2">Loading...</div></div></div>';

        if (hasErr) {
            html += '<div class="ps-detail-section"><div class="ps-detail-section-title">Errors (' + it.errors.length + ')</div>';
            html += '<ul class="ps-error-list">';
            for (var i = 0; i < it.errors.length; i++) {
                html += '<li>' + PA.esc(it.errors[i]) + '</li>';
            }
            html += '</ul></div>';
        }

        html += '<div style="text-align:right;margin-top:12px"><button class="btn btn-primary btn-sm" onclick="this.closest(\'.ps-detail-overlay\').remove(); PA.loadAnalysis(\'' + PA.escJs(it.name) + '\')">Open Analysis</button></div>';
        html += '</div></div></div>';

        document.body.insertAdjacentHTML('beforeend', html);
        PA.perfSummary._loadClaudeStats(it.name);
    },

    _loadClaudeStats: function(name) {
        fetch('/api/analyses/' + encodeURIComponent(name) + '/claude/result')
            .then(function(r) { return r.ok ? r.json() : null; })
            .then(function(cr) {
                var grid = document.getElementById('psClaudeGrid');
                if (!grid) return;
                if (!cr) { grid.innerHTML = '<div style="color:var(--text-muted);font-size:11px;grid-column:span 2">No verification data</div>'; return; }
                var h = '';
                h += '<div class="ps-detail-kv"><span class="ps-k">Confirmed</span><span class="ps-v" style="color:#15803d">' + (cr.confirmedCount || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Removed</span><span class="ps-v" style="color:#b91c1c">' + (cr.removedCount || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">New</span><span class="ps-v" style="color:#2563eb">' + (cr.newCount || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Chunks</span><span class="ps-v">' + (cr.totalChunks || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Error Chunks</span><span class="ps-v">' + (cr.errorChunks || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Claude API Calls</span><span class="ps-v" style="font-weight:700">' + (cr.claudeCallCount || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Claude Time</span><span class="ps-v">' + PA.formatDuration(cr.claudeTimeMs || 0) + '</span></div>';
                grid.innerHTML = h;
            })
            .catch(function() {
                var grid = document.getElementById('psClaudeGrid');
                if (grid) grid.innerHTML = '<div style="color:var(--text-muted);font-size:11px;grid-column:span 2">No verification data</div>';
            });
    }
};

PA.switchHomeTab = function(tab) {
    document.querySelectorAll('.home-tab').forEach(function(el) {
        el.classList.toggle('active', el.dataset.htab === tab);
    });
    var hist = document.getElementById('homeTabHistory');
    var perf = document.getElementById('homeTabPerfSummary');
    if (hist) { hist.style.display = tab === 'history' ? '' : 'none'; hist.classList.toggle('active', tab === 'history'); }
    if (perf) { perf.style.display = tab === 'perfSummary' ? '' : 'none'; perf.classList.toggle('active', tab === 'perfSummary'); }
    if (tab === 'perfSummary') PA.perfSummary.render();
};
