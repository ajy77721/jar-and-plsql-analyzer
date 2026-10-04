PA.perfSummary = {
    _sortCol: 'duration',
    _sortDir: 'desc',
    _data: [],
    _page: 1,
    _pageSize: 10,
    _filterText: '',
    _filterDs: '',
    _filterProj: '',

    /**
     * Datasource dialect — read, not derived (PLAN-08 §4.3 #13).
     *
     * This was a jdbcUrl substring scanner duplicated in `history.js:101-109`, and the two copies
     * disagreed: an unrecognised URL was ORACLE here and an em dash there. The server derives it
     * once now, into `dbType`, and no longer sends the URL to this screen at all.
     */
    _dialect: function(it) {
        return (it.dbType ? String(it.dbType).toUpperCase() : '') || 'ORACLE';
    },

    /**
     * The 3-phase timing breakdown ("[timing] ..." log line, made visible in the UI) — parse /
     * discover / save, in ms. Populated for PL/SQL history entries (AnalysisInfo.parseMs/
     * discoverMs/saveMs); JAR/WAR show the same breakdown in their own dashboard panel
     * (jar/js/dashboard.js#_loadPhasesTable), sourced from QueueJob via GET /api/queue.
     */
    _phasesText: function(it) {
        if (it.parseMs == null && it.discoverMs == null && it.saveMs == null) return '—';
        return (it.parseMs || 0) + ' / ' + (it.discoverMs || 0) + ' / ' + (it.saveMs || 0);
    },

    /** Errors as a count. The messages are fetched by `showDetail`, for the one row that shows them. */
    _errCount: function(it) {
        return it.errorCount !== undefined
            ? it.errorCount : ((it.errors && it.errors.length) || 0);
    },
    _dsMeta: {
        ORACLE:    { label: 'Oracle',     color: 'var(--badge-orange)', bg: 'var(--badge-orange-bg)' },
        SQLSERVER: { label: 'SQL Server', color: 'var(--badge-red)', bg: 'var(--badge-red-bg)' },
        MYSQL:     { label: 'MySQL',      color: 'var(--badge-blue)', bg: 'var(--badge-blue-bg)' },
        DB2:       { label: 'Db2',        color: 'var(--accent)', bg: 'var(--accent-subtle)' },
        POSTGRES:  { label: 'PostgreSQL', color: 'var(--badge-blue)', bg: 'var(--badge-blue-bg)' }
    },

    _fmtWhen: function(ts) {
        if (!ts) return '—';
        try { return new Date(ts).toLocaleString(); } catch (e) { return '—'; }
    },

    // Build projToDs and drop any selected filter whose value no longer exists in the data,
    // so the visible "All" in the dropdown matches what actually gets filtered.
    _syncFilters: function(list) {
        var self = PA.perfSummary;
        var dsSeen = {}, projToDs = {};
        for (var i = 0; i < list.length; i++) {
            var dl = self._dialect(list[i]);
            dsSeen[dl] = true;
            if (list[i].project) projToDs[list[i].project] = dl;
        }
        self._projToDs = projToDs;
        if (self._filterDs && !dsSeen[self._filterDs]) self._filterDs = '';
        if (self._filterProj && !projToDs[self._filterProj]) self._filterProj = '';
    },

    render: function() {
        var list = PA._allHistory || [];
        PA.perfSummary._data = list;
        var container = document.getElementById('perfSummaryContainer');
        if (!container) return;
        if (list.length === 0) { container.innerHTML = '<div class="empty-msg">No analyses yet</div>'; return; }
        PA.perfSummary._syncFilters(list);

        // Overview stat cards reflect the CURRENT filter (recomputed in _renderBody on change).
        var html = '<div id="psOverview">' + PA.perfSummary._overviewHtml(PA.perfSummary._filtered(list)) + '</div>';

        // Top-right filter toolbar: datasource + project (cascading) + free-text.
        html += '<div class="ps-toolbar">';
        html += '<span class="ps-toolbar-title">Analyses</span>';
        html += '<div class="ps-toolbar-controls">';
        html += '<span id="psFilterSelects">' + PA.perfSummary._filterSelectsHtml(list) + '</span>';
        html += '<input class="ps-filter-input" type="text" placeholder="Filter entry / schema / conn…" value="' + PA.esc(PA.perfSummary._filterText) + '" oninput="PA.perfSummary.onFilterText(this.value)">';
        html += '</div></div>';

        html += '<div id="psTableHost"></div>';

        container.innerHTML = html;
        PA.perfSummary._renderBody();
    },

    /**
     * The overview stat cards for the current filter.
     *
     * <p>Eight running totals over every analysis, recomputed on every keystroke — twice per
     * render, since `render` and `_renderBody` both call this. The server computes them for the
     * same filter now (Q31), and `PA._historyFacets` is used whenever it is available and describes
     * the whole set; the local sweep stays as the fallback and for the case where the browser holds
     * the full history and is filtering it itself, where it is genuinely cheaper than a round trip.
     */
    _overviewHtml: function(list) {
        var f = PA._historyFacets;
        if (f && PA._historyTruncated) {
            var hf = '<div class="ps-overview">';
            hf += PA.perfSummary._statCard(f.total, 'Analyses');
            hf += PA.perfSummary._statCard(f.success, 'Success', 'var(--green)');
            hf += PA.perfSummary._statCard(f.withErrors, 'With Errors',
                    f.withErrors > 0 ? 'var(--red)' : 'var(--green)');
            hf += PA.perfSummary._statCard(PA.formatDuration(f.totalDurationMs), 'Total Time');
            hf += PA.perfSummary._statCard(f.dbCallCount, 'DB Calls');
            hf += PA.perfSummary._statCard(((f.totalProcs != null ? f.totalProcs : f.totalNodes) || 0).toLocaleString(), 'Procedures');
            hf += PA.perfSummary._statCard((f.totalTables || 0).toLocaleString(), 'Tables');
            hf += PA.perfSummary._statCard((f.totalLinesOfCode || 0).toLocaleString(), 'LOC');
            hf += '</div>';
            return hf;
        }
        var totalTime = 0, totalDb = 0, totalSuccess = 0, totalErrors = 0, totalProcs = 0, totalTables = 0, totalLoc = 0;
        for (var i = 0; i < list.length; i++) {
            var it = list[i];
            totalTime += (it.totalDurationMs || it.crawlTimeMs || 0);
            totalDb += (it.dbCallCount || 0);
            totalProcs += PA.perfSummary._procsOf(it);
            totalTables += (it.totalTables || 0);
            totalLoc += (it.totalLinesOfCode || 0);
            if (PA.perfSummary._errCount(it) > 0) totalErrors++; else totalSuccess++;
        }
        var h = '<div class="ps-overview">';
        h += PA.perfSummary._statCard(list.length, 'Analyses');
        h += PA.perfSummary._statCard(totalSuccess, 'Success', 'var(--green)');
        h += PA.perfSummary._statCard(totalErrors, 'With Errors', totalErrors > 0 ? 'var(--red)' : 'var(--green)');
        h += PA.perfSummary._statCard(PA.formatDuration(totalTime), 'Total Time');
        h += PA.perfSummary._statCard(totalDb, 'DB Calls');
        h += PA.perfSummary._statCard(totalProcs.toLocaleString(), 'Procedures');
        h += PA.perfSummary._statCard(totalTables.toLocaleString(), 'Tables');
        h += PA.perfSummary._statCard(totalLoc.toLocaleString(), 'LOC');
        h += '</div>';
        return h;
    },

    /**
     * "Procs" for one history row = procedures + functions only. The raw node total lumps
     * triggers/tables/packages; the row's typeCounts carries the truthful split, with totalNodes as
     * the fallback for an analysis crawled before that field existed.
     */
    _procsOf: function(it) {
        var tc = (it && it.typeCounts) || {};
        if (tc.PROCEDURE != null || tc.FUNCTION != null) {
            return (tc.PROCEDURE || 0) + (tc.FUNCTION || 0);
        }
        return (it && it.totalNodes) || 0;
    },

    _statCard: function(value, label, color) {
        var style = color ? 'color:' + color : '';
        return '<div class="ps-stat-card"><div class="ps-stat-value" style="' + style + '">' + value + '</div><div class="ps-stat-label">' + PA.esc(label) + '</div></div>';
    },

    // Build the cascading datasource + project dropdowns. Project options are limited to the
    // selected datasource; a project maps to exactly one datasource (project -> _projToDs).
    _filterSelectsHtml: function(list) {
        var self = PA.perfSummary;
        var dsSeen = {}, projToDs = {};
        for (var i = 0; i < list.length; i++) {
            var dl = self._dialect(list[i]);
            dsSeen[dl] = true;
            if (list[i].project) projToDs[list[i].project] = dl;
        }
        self._projToDs = projToDs;
        var dsOpts = '<option value="">All datasources</option>';
        // Derived from PA._DS_LABEL, not a literal — the same four-entry list in history.js left
        // PostgreSQL analyses present in the table but absent from the filter. cacheManager.js
        // already listed five, which is how the drift was visible: three copies, two of them stale.
        Object.keys(PA._DS_LABEL).forEach(function(k) {
            if (dsSeen[k]) dsOpts += '<option value="' + k + '"' + (self._filterDs === k ? ' selected' : '') + '>' + self._dsMeta[k].label + '</option>';
        });
        var prOpts = '<option value="">All projects</option>';
        Object.keys(projToDs).sort().forEach(function(p) {
            if (self._filterDs && projToDs[p] !== self._filterDs) return;  // cascade
            prOpts += '<option value="' + PA.esc(p) + '"' + (self._filterProj === p ? ' selected' : '') + '>' + PA.esc(p) + '</option>';
        });
        return '<select class="ps-filter-select" onchange="PA.perfSummary.onFilterDs(this.value)">' + dsOpts + '</select>'
             + '<select class="ps-filter-select" onchange="PA.perfSummary.onFilterProj(this.value)">' + prOpts + '</select>';
    },

    _refreshSelects: function() {
        var host = document.getElementById('psFilterSelects');
        if (host) host.innerHTML = PA.perfSummary._filterSelectsHtml(PA._allHistory || []);
    },

    _filtered: function(list) {
        var self = PA.perfSummary;
        var q = (self._filterText || '').toUpperCase();
        var ds = self._filterDs || '';
        var proj = self._filterProj || '';
        return list.filter(function(it) {
            if (ds && self._dialect(it) !== ds) return false;
            if (proj && (it.project || '') !== proj) return false;
            if (q) {
                var t = ((it.entryPoint || '') + ' ' + (it.entrySchema || '') + ' ' + (it.name || '') + ' ' + (it.connectionName || '') + ' ' + (it.project || '') + ' ' + self._fmtWhen(it.timestamp)).toUpperCase();
                if (t.indexOf(q) < 0) return false;
            }
            return true;
        });
    },

    onFilterText: function(v) { PA.perfSummary._filterText = v; PA.perfSummary._page = 1; PA.perfSummary._renderBody(); },
    onFilterDs: function(v) {
        var self = PA.perfSummary;
        self._filterDs = v;
        if (!v) {
            self._filterProj = '';                       // All datasources → All projects
        } else if (self._filterProj && self._projToDs && self._projToDs[self._filterProj] !== v) {
            self._filterProj = '';                       // project no longer under this datasource
        }
        self._page = 1;
        self._refreshSelects();
        self._renderBody();
    },
    onFilterProj: function(v) {
        var self = PA.perfSummary;
        self._filterProj = v;
        if (v && self._projToDs && self._projToDs[v]) self._filterDs = self._projToDs[v];  // auto-set datasource
        self._page = 1;
        self._refreshSelects();
        self._renderBody();
    },
    goPage:       function(p) { PA.perfSummary._page = p; PA.perfSummary._renderBody(); },

    // Re-renders ONLY the table + pager (keeps the toolbar/search-input focus intact).
    _renderBody: function() {
        var host = document.getElementById('psTableHost');
        if (!host) return;
        PA.perfSummary._syncFilters(PA._allHistory || []);
        var filtered = PA.perfSummary._filtered(PA._allHistory || []);
        // Keep the overview stat cards in sync with the filter.
        var ov = document.getElementById('psOverview');
        if (ov) ov.innerHTML = PA.perfSummary._overviewHtml(filtered);
        host.innerHTML = PA.perfSummary._renderPager(filtered.length)
                       + '<div class="ps-table-wrap ps-table-scroll">' + PA.perfSummary._renderTable(filtered) + '</div>';
    },

    _renderPager: function(total) {
        var self = PA.perfSummary;
        var pages = Math.max(1, Math.ceil(total / self._pageSize));
        if (self._page > pages) self._page = pages;
        var from = total === 0 ? 0 : (self._page - 1) * self._pageSize + 1;
        var to = Math.min(self._page * self._pageSize, total);
        var prevD = self._page <= 1 ? 'disabled' : '';
        var nextD = self._page >= pages ? 'disabled' : '';
        var h = '<div class="pagination-bar ps-pager">';
        h += '<span class="page-info">' + from + '–' + to + ' of ' + total + '</span>';
        h += '<span class="page-btns">';
        h += '<button ' + prevD + ' onclick="PA.perfSummary.goPage(1)">«</button>';
        h += '<button ' + prevD + ' onclick="PA.perfSummary.goPage(' + (self._page - 1) + ')">‹</button>';
        h += '<span class="page-info">Page ' + self._page + ' / ' + pages + '</span>';
        h += '<button ' + nextD + ' onclick="PA.perfSummary.goPage(' + (self._page + 1) + ')">›</button>';
        h += '<button ' + nextD + ' onclick="PA.perfSummary.goPage(' + pages + ')">»</button>';
        h += '</span></div>';
        return h;
    },

    _renderTable: function(list) {
        var self = PA.perfSummary;
        var sorted = list.slice().sort(self._comparator());
        var maxTime = 0;
        for (var i = 0; i < sorted.length; i++) {
            var _d = (sorted[i].totalDurationMs || sorted[i].crawlTimeMs || 0);
            if (_d > maxTime) maxTime = _d;
        }

        // page slice
        var pageStart = (self._page - 1) * self._pageSize;
        var pageRows = sorted.slice(pageStart, pageStart + self._pageSize);

        var sc = self._sortCol, sd = self._sortDir;
        var arrow = function(col) { return sc === col ? (sd === 'asc' ? ' ▲' : ' ▼') : ''; };

        var html = '<table class="ps-table"><thead><tr>';
        html += '<th onclick="PA.perfSummary._sort(\'name\')">#' + arrow('name') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'entry\')">Entry Point' + arrow('entry') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'datasource\')">Datasource' + arrow('datasource') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'when\')">Date / Time' + arrow('when') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'status\')">Status' + arrow('status') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'duration\')">Duration' + arrow('duration') + '</th>';
        html += '<th>Time Bar</th>';
        // Shared across PL/SQL, JAR and WAR — all three executors now attach the same
        // parse/discover/save/total breakdown to their QueueJob, and either surface it here
        // (PL/SQL, via history) or via PA.perfSummary.loadFromQueue (JAR/WAR).
        html += '<th title="parse / discover / save">Phases (ms)</th>';
        html += '<th onclick="PA.perfSummary._sort(\'db\')">DB Calls' + arrow('db') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'procs\')">Procs' + arrow('procs') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'tables\')">Tables' + arrow('tables') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'loc\')">LOC' + arrow('loc') + '</th>';
        html += '<th onclick="PA.perfSummary._sort(\'errors\')">Errors' + arrow('errors') + '</th>';
        html += '</tr></thead><tbody>';

        for (var i = 0; i < pageRows.length; i++) {
            var it = pageRows[i];
            var errCount = self._errCount(it);
            var hasErr = errCount > 0;
            var statusCls = hasErr ? (errCount > 0 && (it.totalNodes || 0) > 0 ? 'partial' : 'error') : 'success';
            var statusLabel = hasErr ? (statusCls === 'partial' ? 'Partial' : 'Error') : 'Success';
            var ms = it.totalDurationMs || it.crawlTimeMs || 0;
            var pct = maxTime > 0 ? Math.round(ms / maxTime * 100) : 0;
            var barColor = statusCls === 'success' ? 'var(--green)' : statusCls === 'partial' ? 'var(--orange)' : 'var(--red)';
            var dk = self._dialect(it), dm = self._dsMeta[dk] || { label: dk, color: 'var(--text-secondary)', bg: 'var(--bg-hover)' };

            html += '<tr onclick="PA.perfSummary.showDetail(' + (pageStart + i) + ')" data-idx="' + (pageStart + i) + '">';
            html += '<td style="color:var(--text-muted);font-size:10px">' + (pageStart + i + 1) + '</td>';
            html += '<td style="font-weight:600;font-size:11px">' + PA.esc(it.entryPoint || it.name || '') + '</td>';
            html += '<td><span class="ps-ds-badge" style="color:' + dm.color + ';background:' + dm.bg + '">' + PA.esc(dm.label) + '</span></td>';
            html += '<td style="white-space:nowrap;font-size:11px;color:var(--text-muted)">' + PA.esc(self._fmtWhen(it.timestamp)) + '</td>';
            html += '<td><span class="ps-status ' + statusCls + '">' + statusLabel + '</span></td>';
            html += '<td style="white-space:nowrap;font-family:monospace;font-size:11px">' + PA.formatDuration(ms) + '</td>';
            html += '<td><div class="ps-bar"><div class="ps-bar-fill" style="width:' + pct + '%;background:' + barColor + '"></div></div></td>';
            html += '<td style="white-space:nowrap;font-family:monospace;font-size:10px;color:var(--text-muted)">' + self._phasesText(it) + '</td>';
            html += '<td style="text-align:center;font-weight:600">' + (it.dbCallCount || 0) + '</td>';
            html += '<td style="text-align:center">' + PA.perfSummary._procsOf(it) + '</td>';
            html += '<td style="text-align:center">' + (it.totalTables || 0) + '</td>';
            html += '<td style="text-align:center">' + (it.totalLinesOfCode || 0).toLocaleString() + '</td>';
            html += '<td style="text-align:center">';
            if (errCount > 0) html += '<span style="color:var(--red);font-weight:700">' + errCount + '</span>';
            else html += '<span style="color:var(--green)">0</span>';
            html += '</td></tr>';
        }
        if (pageRows.length === 0) {
            html += '<tr><td colspan="13" class="empty-msg" style="text-align:center">No analyses match the filter</td></tr>';
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
                case 'datasource': va = PA.perfSummary._dialect(a); vb = PA.perfSummary._dialect(b); return va.localeCompare(vb) * m;
                case 'when': return ((a.timestamp || 0) - (b.timestamp || 0)) * m;
                case 'status':
                    va = PA.perfSummary._errCount(a) ? 1 : 0;
                    vb = PA.perfSummary._errCount(b) ? 1 : 0;
                    return (va - vb) * m;
                case 'duration': return ((a.totalDurationMs || a.crawlTimeMs || 0) - (b.totalDurationMs || b.crawlTimeMs || 0)) * m;
                case 'db': return ((a.dbCallCount || 0) - (b.dbCallCount || 0)) * m;
                case 'procs': return (PA.perfSummary._procsOf(a) - PA.perfSummary._procsOf(b)) * m;
                case 'tables': return ((a.totalTables || 0) - (b.totalTables || 0)) * m;
                case 'loc': return ((a.totalLinesOfCode || 0) - (b.totalLinesOfCode || 0)) * m;
                case 'errors':
                    va = PA.perfSummary._errCount(a);
                    vb = PA.perfSummary._errCount(b);
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
            PA.perfSummary._sortDir = col === 'name' || col === 'entry' || col === 'datasource' ? 'asc' : 'desc';
        }
        PA.perfSummary._renderBody();
    },

    showDetail: function(idx) {
        var sorted = PA.perfSummary._filtered(PA._allHistory || []).slice().sort(PA.perfSummary._comparator());
        var it = sorted[idx];
        if (!it) return;

        var errCount = PA.perfSummary._errCount(it);
        var hasErr = errCount > 0;
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
        html += '<div class="ps-detail-kv"><span class="ps-k">Duration</span><span class="ps-v">' + PA.formatDuration(it.totalDurationMs || it.crawlTimeMs) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">DB Calls</span><span class="ps-v">' + (it.dbCallCount || 0) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Parse / Discover / Save</span><span class="ps-v" style="font-family:monospace">' + PA.perfSummary._phasesText(it) + ' ms</span></div>';
        html += '</div></div>';

        html += '<div class="ps-detail-section"><div class="ps-detail-section-title">Metrics</div>';
        html += '<div class="ps-detail-grid">';
        html += '<div class="ps-detail-kv"><span class="ps-k">Procedures</span><span class="ps-v">' + PA.perfSummary._procsOf(it) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Tables</span><span class="ps-v">' + (it.totalTables || 0) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Edges</span><span class="ps-v">' + (it.totalEdges || 0) + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">LOC</span><span class="ps-v">' + (it.totalLinesOfCode || 0).toLocaleString() + '</span></div>';
        html += '<div class="ps-detail-kv"><span class="ps-k">Max Depth</span><span class="ps-v">' + (it.maxDepth || 0) + '</span></div>';
        html += '</div></div>';

        html += '<div class="ps-detail-section" id="psClaudeStats"><div class="ps-detail-section-title">Claude Verification</div>';
        html += '<div class="ps-detail-grid" id="psClaudeGrid"><div style="color:var(--text-muted);font-size:11px;grid-column:span 2">Loading...</div></div></div>';

        if (hasErr) {
            // The list row carries only the count; the messages are fetched for this one analysis
            // when its panel opens, rather than sent for every row on every history load.
            html += '<div class="ps-detail-section"><div class="ps-detail-section-title">Errors ('
                 + errCount + ')</div>';
            html += '<ul class="ps-error-list" id="psErrorList">'
                 + (it.errors ? it.errors.map(function(e) { return '<li>' + PA.esc(e) + '</li>'; }).join('')
                              : '<li style="color:var(--text-muted)">Loading…</li>')
                 + '</ul></div>';
        }

        html += '<div style="text-align:right;margin-top:12px"><button class="btn btn-primary btn-sm" onclick="this.closest(\'.ps-detail-overlay\').remove(); PA.loadAnalysis(\'' + PA.escJs(it.name) + '\')">Open Analysis</button></div>';
        html += '</div></div></div>';

        PA.mountOverlay(html);
        var psEl = document.querySelector('.ps-detail-overlay:last-of-type');
        if (psEl && PA.modalStack) psEl.style.zIndex = PA.modalStack.nextZ();
        PA.perfSummary._loadClaudeStats(it.name);
        if (hasErr && !it.errors) PA.perfSummary._loadErrors(it.name);
    },

    /** Fills the Errors list of an open detail panel. */
    _loadErrors: function(name) {
        PA.api.analysisErrors(name)
            .then(function(errors) {
                var ul = document.getElementById('psErrorList');
                if (!ul) return;
                // PA.failed distinguishes "the request failed" from "there are none". This panel
                // only opens when the error badge is set, so "No messages recorded" contradicted
                // the badge the user had just clicked.
                ul.innerHTML = errors.length
                    ? errors.map(function(e) { return '<li>' + PA.esc(e) + '</li>'; }).join('')
                    : (PA.failed && PA.failed(errors)
                        ? '<li style="color:var(--badge-red,#c0392b)">Could not load the messages — '
                          + 'this is a request failure, not an empty list.</li>'
                        : '<li style="color:var(--text-muted)">No messages recorded</li>');
            })
            .catch(function(err) {
                // Distinguished from "no errors recorded", which is a different fact.
                console.error('[perfSummary] error messages failed', err);
                var ul = document.getElementById('psErrorList');
                if (ul) ul.innerHTML = '<li style="color:var(--red)">Could not load messages</li>';
            });
    },

    _loadClaudeStats: function(name) {
        // This used to fetch '/api/analyses/...', which no controller maps, so it always 404'd and
        // the catch below rendered "No verification data" — indistinguishable from an analysis that
        // was genuinely never verified. Going through PA.api is the fix: api.js is the only file
        // that should know URL prefixes, and bypassing it is why the path was wrong.
        PA.api.claudeVerificationSummary(name)
            .then(function(cr) {
                var grid = document.getElementById('psClaudeGrid');
                if (!grid) return;
                if (!cr) { grid.innerHTML = '<div style="color:var(--text-muted);font-size:11px;grid-column:span 2">Not yet verified</div>'; return; }
                var h = '';
                h += '<div class="ps-detail-kv"><span class="ps-k">Confirmed</span><span class="ps-v" style="color:var(--badge-green)">' + (cr.confirmedCount || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Removed</span><span class="ps-v" style="color:var(--badge-red)">' + (cr.removedCount || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">New</span><span class="ps-v" style="color:var(--blue)">' + (cr.newCount || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Chunks</span><span class="ps-v">' + (cr.totalChunks || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Error Chunks</span><span class="ps-v">' + (cr.errorChunks || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Claude API Calls</span><span class="ps-v" style="font-weight:700">' + (cr.claudeCallCount || 0) + '</span></div>';
                h += '<div class="ps-detail-kv"><span class="ps-k">Claude Time</span><span class="ps-v">' + PA.formatDuration(cr.claudeTimeMs || 0) + '</span></div>';
                grid.innerHTML = h;
            })
            .catch(function(err) {
                // A genuine failure must NOT read as "no data". Reporting both the same way is
                // precisely how this grid stayed empty for every user without anyone noticing.
                console.error('[perfSummary] verification summary failed', err);
                var grid = document.getElementById('psClaudeGrid');
                if (grid) grid.innerHTML = '<div style="color:var(--badge-orange);font-size:11px;grid-column:span 2">'
                    + 'Verification unavailable (' + (err && err.message ? err.message : 'error') + ')</div>';
            });
    }
};

PA.switchHomeTab = function(tab) {
    document.querySelectorAll('.home-tab').forEach(function(el) {
        el.classList.toggle('active', el.dataset.htab === tab);
    });
    var hist = document.getElementById('homeTabHistory');
    var perf = document.getElementById('homeTabPerfSummary');
    var queue = document.getElementById('homeTabQueue');
    var search = document.getElementById('historySearch');
    if (hist) { hist.style.display = tab === 'history' ? '' : 'none'; hist.classList.toggle('active', tab === 'history'); }
    if (perf) { perf.style.display = tab === 'perfSummary' ? '' : 'none'; perf.classList.toggle('active', tab === 'perfSummary'); }
    if (queue) { queue.style.display = tab === 'queue' ? '' : 'none'; queue.classList.toggle('active', tab === 'queue'); }
    if (search) search.style.display = tab === 'history' ? '' : 'none';
    // The top datasource/project filter row belongs to Analysis History only — hide it on
    // Performance Summary and Queue (those tabs have their own controls / none).
    var filters = document.getElementById('historyFilters');
    if (filters) filters.style.display = tab === 'history' ? '' : 'none';
    if (tab === 'history' && PA._shouldLoadTab('history')) PA.loadHistory();
    if (tab === 'perfSummary' && PA._shouldLoadTab('perfSummary')) PA.perfSummary.render();
    if (tab === 'queue' && PA._shouldLoadTab('queue')) PA.queueTab.load();
};

// Rapid re-clicking of the same home tab (Analysis History / Performance Summary / Queue
// Analysis) used to fire one API call per click. The 2nd/3rd click within a short window is
// now treated as an accidental repeat and ignored; a 4th+ click in that same window is treated
// as a deliberate "hard refresh" request and is allowed through.
PA._tabLoadGuard = {};
PA._shouldLoadTab = function(tab) {
    var WINDOW_MS = 700;
    var BURST_THRESHOLD = 4;
    var now = Date.now();
    var g = PA._tabLoadGuard[tab] || { count: 0, last: 0 };
    if (now - g.last > WINDOW_MS) g.count = 0; // previous burst expired, start counting fresh
    g.count++;
    g.last = now;
    PA._tabLoadGuard[tab] = g;
    if (g.count === 1) return true;                       // first click: load as normal
    if (g.count >= BURST_THRESHOLD) { g.count = 0; return true; } // deliberate rapid burst: force reload
    return false;                                          // 2nd/3rd click in the window: debounce
};

PA.queueTab = {
    _timer: null,

    load: function() {
        PA.queueTab._timer = PollConfig.clear(PA.queueTab._timer);
        PA.queueTab._timer = PollConfig.every('jobStatusMs', function() {
            var queueEl = document.getElementById('homeTabQueue');
            if (!queueEl || queueEl.style.display === 'none') {
                PA.queueTab._timer = PollConfig.clear(PA.queueTab._timer);
                return;
            }
            PA.queueTab._render();
        });
    },

    _render: async function() {
        var container = document.getElementById('queueContainer');
        if (!container) return;
        try {
            // Filter and bounds applied by the server. This tab renders PL/SQL kinds only and
            // never shows more than 20 finished jobs, so asking for the whole global queue and
            // discarding most of it in the browser was pure transfer.
            var res = await fetch('/api/queue?kind=' + PA.queueTab._KINDS + '&historyLimit=20');
            if (!res.ok) { container.innerHTML = '<div class="empty-msg">Failed to load queue</div>'; return; }
            var data = await res.json();
            PA.queueTab._renderData(container, data);
        } catch (e) {
            container.innerHTML = '<div class="empty-msg">Error: ' + PA.esc(e.message || e) + '</div>';
        }
    },

    // Job types that belong to the PL/SQL analyser — the server filters to these; the local set
    // stays because the same renderer is fed by SSE state, which is unfiltered by nature.
    _PLSQL_TYPES: new Set([
        'PLSQL_ANALYSIS', 'PLSQL_FAST_ANALYSIS', 'PARSER_ANALYSIS', 'PLSQL_CLAUDE_VERIFY'
    ]),

    _KINDS: 'PLSQL_ANALYSIS,PLSQL_FAST_ANALYSIS,PARSER_ANALYSIS,PLSQL_CLAUDE_VERIFY',

    _isPlsql: function(job) {
        return PA.queueTab._PLSQL_TYPES.has((job.type || '').toUpperCase());
    },

    _renderData: function(container, data) {
        var rawRunning = data.current || data.running || null;
        var rawQueued  = data.queued  || [];
        var rawHistory = data.history || [];

        // Show only PL/SQL jobs on this home screen
        var running = rawRunning && PA.queueTab._isPlsql(rawRunning) ? rawRunning : null;
        var queued  = rawQueued.filter(PA.queueTab._isPlsql);
        var history = rawHistory.filter(PA.queueTab._isPlsql);

        var html = '';
        html += '<div style="margin-bottom:8px;font-size:11px;color:var(--text-muted)">Auto-refreshes every '
             + Math.round(PollConfig.jobStatusMs / 1000) + 's</div>';

        html += '<div style="margin-bottom:4px;font-weight:600;font-size:12px;color:var(--text-primary)">Running</div>';
        if (running) {
            html += PA.queueTab._jobRow(running, 'running');
        } else {
            html += '<div class="empty-msg" style="margin:4px 0 8px">No job running</div>';
        }

        html += '<div style="margin:8px 0 4px;font-weight:600;font-size:12px;color:var(--text-primary)">Pending (' + queued.length + ')</div>';
        if (queued.length > 0) {
            for (var i = 0; i < queued.length; i++) html += PA.queueTab._jobRow(queued[i], 'queued');
        } else {
            html += '<div class="empty-msg" style="margin:4px 0 8px">Queue is empty</div>';
        }

        html += '<div style="margin:8px 0 4px;font-weight:600;font-size:12px;color:var(--text-primary)">Recent (' + Math.min(history.length, 20) + ')</div>';
        if (history.length > 0) {
            var recent = history.slice(0, 20);
            for (var j = 0; j < recent.length; j++) html += PA.queueTab._jobRow(recent[j], 'history');
        } else {
            html += '<div class="empty-msg" style="margin:4px 0 8px">No history</div>';
        }

        container.innerHTML = html;
    },

    _jobRow: function(job, section) {
        var statusColor = { RUNNING: 'var(--badge-blue)', QUEUED: 'var(--badge-orange)', COMPLETE: 'var(--badge-green)', CANCELLED: 'var(--text-muted)', FAILED: 'var(--badge-red)' };
        var status = (job.status || '').toUpperCase();
        var color = statusColor[status] || 'var(--text-muted)';
        var elapsed = job.elapsedMs != null ? ' · ' + PA.formatDuration(job.elapsedMs) : '';
        var type = (job.type || '').replace(/_/g, ' ');
        var name = job.displayName || job.id || '';
        var step = job.currentStep ? ('<div style="color:var(--text-muted);font-size:10px;margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + PA.esc(job.currentStep) + '</div>') : '';
        var progress = (status === 'RUNNING' && job.progressPercent > 0)
            ? '<div style="height:3px;background:var(--bg-tertiary);border-radius:var(--radius-sm);margin-top:3px"><div style="height:3px;background:var(--blue);border-radius:var(--radius-sm);width:' + job.progressPercent + '%"></div></div>'
            : '';
        var canCancel = (section === 'queued' || section === 'running' || status === 'RUNNING' || status === 'QUEUED');
        var cancelBtn = canCancel
            ? '<button onclick="PA.queueTab._cancel(\'' + PA.esc(job.id) + '\')" style="flex-shrink:0;margin-left:8px;font-size:10px;padding:1px 6px;background:var(--red);color:var(--text-inverse);border:none;border-radius:var(--radius-sm);cursor:pointer">Cancel</button>'
            : '';
        return '<div style="padding:5px 8px;margin-bottom:4px;background:var(--bg-secondary);border-radius:var(--radius-sm);font-size:11px">'
            + '<div style="display:flex;align-items:center">'
            + '<span style="min-width:72px;color:' + color + ';font-weight:600">' + PA.esc(status) + '</span>'
            + '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + PA.esc(name) + '">' + PA.esc(name) + '</span>'
            + '<span style="color:var(--text-muted);margin-left:6px;flex-shrink:0">' + PA.esc(type) + elapsed + '</span>'
            + cancelBtn
            + '</div>'
            + step
            + progress
            + '</div>';
    },

    _cancel: async function(jobId) {
        try {
            await fetch('/api/queue/' + encodeURIComponent(jobId) + '/cancel', { method: 'POST' });
            PA.queueTab._render();
        } catch (e) {
            PA.toast('Cancel failed: ' + (e.message || e), 'error');
        }
    }
};
