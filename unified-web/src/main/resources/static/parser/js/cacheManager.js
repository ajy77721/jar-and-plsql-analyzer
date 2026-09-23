window.PA = window.PA || {};

/**
 * Cache Manager — per-connection cache footprint + clear.
 * Opened from the topbar "Clear Cache" button. Lists each connection
 * (datasource / project / env) with its cached-analysis count + on-disk size,
 * and lets you clear one connection (in-memory + shared source cache) or all.
 */
PA.cacheManager = {
    _data: [],
    _dsFilter: '',
    _projFilter: '',
    _DS_LABEL: { ORACLE: 'Oracle', SQLSERVER: 'SQL Server', MYSQL: 'MySQL', DB2: 'Db2', POSTGRES: 'PostgreSQL', UNKNOWN: '—' },

    open: async function () {
        var ov = document.getElementById('cacheMgrOverlay');
        if (!ov) {
            ov = document.createElement('div');
            ov.id = 'cacheMgrOverlay';
            ov.className = 'cfg-overlay';
            ov.onclick = function (e) { if (e.target === ov) PA.cacheManager.close(); };
            document.body.appendChild(ov);
        }
        ov.style.display = '';
        if (PA.modalStack) ov.style.zIndex = PA.modalStack.nextZ();
        ov.innerHTML = '<div class="cfg-modal" style="width:920px;max-width:95vw;height:auto;max-height:82vh" onclick="event.stopPropagation()">'
            + '<div class="cfg-header"><h2>Cache Manager</h2>'
            + '<button class="btn btn-sm" onclick="PA.cacheManager.close()" title="Close">&times;</button></div>'
            + '<div class="cfg-panel" style="flex:1;overflow:auto"><div id="cacheMgrBody"><div class="empty-msg">Loading…</div></div></div>'
            + '</div>';
        await PA.cacheManager._load();
    },

    close: function () {
        var ov = document.getElementById('cacheMgrOverlay');
        if (ov) ov.style.display = 'none';
    },

    _load: async function () {
        try {
            var r = await fetch('/api/parser/cache/by-connection');
            PA.cacheManager._data = r.ok ? await r.json() : [];
        } catch (e) {
            PA.cacheManager._data = [];
        }
        PA.cacheManager._render();
    },

    onFilterDs: function (v) { PA.cacheManager._dsFilter = v; PA.cacheManager._render(); },
    onFilterProj: function (v) { PA.cacheManager._projFilter = v; PA.cacheManager._render(); },

    _dialect: function (g) { return (g.dbType || 'UNKNOWN').toUpperCase(); },

    _fmtSize: function (bytes) {
        var b = Number(bytes) || 0;
        if (b < 1024) return b + ' B';
        if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
        return (b / 1048576).toFixed(1) + ' MB';
    },

    _render: function () {
        var host = document.getElementById('cacheMgrBody');
        if (!host) return;
        var list = PA.cacheManager._data || [];

        // filter dropdown option sets
        var dsSeen = {}, projSeen = {};
        list.forEach(function (g) { dsSeen[PA.cacheManager._dialect(g)] = 1; if (g.project) projSeen[g.project] = 1; });
        var ds = '<option value="">All datasources</option>';
        // Was the only one of the three copies that listed all five. Derived now, so a sixth
        // dialect cannot leave any of them behind again.
        Object.keys(PA._DS_LABEL).forEach(function (k) {
            if (dsSeen[k]) ds += '<option value="' + k + '"' + (PA.cacheManager._dsFilter === k ? ' selected' : '') + '>' + PA.cacheManager._DS_LABEL[k] + '</option>';
        });
        var pr = '<option value="">All projects</option>';
        Object.keys(projSeen).sort().forEach(function (p) {
            pr += '<option value="' + PA.esc(p) + '"' + (PA.cacheManager._projFilter === p ? ' selected' : '') + '>' + PA.esc(p) + '</option>';
        });

        var filtered = list.filter(function (g) {
            if (PA.cacheManager._dsFilter && PA.cacheManager._dialect(g) !== PA.cacheManager._dsFilter) return false;
            if (PA.cacheManager._projFilter && g.project !== PA.cacheManager._projFilter) return false;
            return true;
        });

        var html = '';
        html += '<div class="home-filter-row" style="justify-content:space-between">';
        html += '<div style="display:flex;gap:8px">'
            + '<select class="ps-filter-select" onchange="PA.cacheManager.onFilterDs(this.value)">' + ds + '</select>'
            + '<select class="ps-filter-select" onchange="PA.cacheManager.onFilterProj(this.value)">' + pr + '</select></div>';
        html += '<button class="btn btn-sm btn-danger" onclick="PA.cacheManager._clearAll()">Clear All (' + filtered.length + ')</button>';
        html += '</div>';

        if (filtered.length === 0) {
            html += '<div class="empty-msg">No cached connections</div>';
            host.innerHTML = html;
            return;
        }

        html += '<table class="to-table" style="width:100%"><thead><tr>'
            + '<th>Datasource</th><th>Project</th><th>Environment</th><th>Connection</th>'
            + '<th style="text-align:center">Analyses</th><th style="text-align:right">Size</th><th></th></tr></thead><tbody>';
        for (var i = 0; i < filtered.length; i++) {
            var g = filtered[i];
            var dk = PA.cacheManager._dialect(g);
            html += '<tr>';
            html += '<td><span class="ps-ds-badge" style="background:var(--accent-subtle);color:var(--accent)">' + PA.esc(PA.cacheManager._DS_LABEL[dk] || dk) + '</span></td>';
            html += '<td>' + PA.esc(g.project || '—') + '</td>';
            html += '<td>' + PA.esc(g.environment || '—') + '</td>';
            html += '<td style="font-family:var(--font-mono);font-size:11px">' + PA.esc(g.connectionName || '—') + '</td>';
            html += '<td style="text-align:center">' + (g.analysisCount || 0) + '</td>';
            html += '<td style="text-align:right">' + PA.cacheManager._fmtSize(g.sizeBytes) + '</td>';
            html += '<td style="text-align:right"><button class="btn btn-sm btn-danger" onclick="PA.cacheManager._clearConn(\''
                + PA.escJs(g.project || '') + '\',\'' + PA.escJs(g.environment || '') + '\',\'' + PA.escJs(g.connectionName || '') + '\')">Clear</button></td>';
            html += '</tr>';
        }
        html += '</tbody></table>';
        html += '<div style="margin-top:8px;font-size:11px;color:var(--text-muted)">Clear removes in-memory caches for the connection’s analyses and the shared downloaded-source cache (re-fetched next analysis). Analysis results are kept.</div>';
        host.innerHTML = html;
    },

    _clearConn: async function (project, env, conn) {
        if (!confirm('Clear cache for connection "' + (conn || project) + '"?')) return;
        try {
            var qs = 'project=' + encodeURIComponent(project) + '&environment=' + encodeURIComponent(env) + '&connName=' + encodeURIComponent(conn);
            var r = await fetch('/api/parser/cache/clear-connection?' + qs, { method: 'POST' });
            var j = r.ok ? await r.json() : null;
            PA.toast('Cache cleared (' + ((j && j.evicted) || 0) + ' analyses)', 'success');
            await PA.cacheManager._load();
        } catch (e) {
            PA.toast('Failed to clear cache', 'error');
        }
    },

    _clearAll: async function () {
        var list = (PA.cacheManager._data || []).filter(function (g) {
            if (PA.cacheManager._dsFilter && PA.cacheManager._dialect(g) !== PA.cacheManager._dsFilter) return false;
            if (PA.cacheManager._projFilter && g.project !== PA.cacheManager._projFilter) return false;
            return true;
        });
        if (list.length === 0) return;
        if (!confirm('Clear cache for all ' + list.length + ' connection(s)?')) return;
        var total = 0;
        for (var i = 0; i < list.length; i++) {
            var g = list[i];
            try {
                var qs = 'project=' + encodeURIComponent(g.project || '') + '&environment=' + encodeURIComponent(g.environment || '') + '&connName=' + encodeURIComponent(g.connectionName || '');
                var r = await fetch('/api/parser/cache/clear-connection?' + qs, { method: 'POST' });
                if (r.ok) { var j = await r.json(); total += (j.evicted || 0); }
            } catch (e) { /* continue */ }
        }
        PA.toast('Cleared cache for ' + list.length + ' connection(s) (' + total + ' analyses)', 'success');
        await PA.cacheManager._load();
    }
};
