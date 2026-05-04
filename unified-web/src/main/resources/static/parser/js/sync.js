window.PA = window.PA || {};

PA.sync = (function() {
    var _jars = [];
    var _parsers = [];
    var _jarQuery = '';
    var _parserQuery = '';
    var _selectedJar = null;
    var _selectedParser = null;

    function open() {
        _close();
        var overlay = document.createElement('div');
        overlay.className = 'sync-overlay';
        overlay.id = 'syncOverlay';
        overlay.onclick = function(e) { if (e.target === overlay) close(); };

        overlay.innerHTML =
            '<div class="sync-modal" onclick="event.stopPropagation()">' +
                '<div class="sync-header">' +
                    '<span class="sync-header-title">Sync: JAR Analyzer &amp; PL/SQL Parse Analyzer</span>' +
                    '<div class="sync-header-actions">' +
                        '<button class="btn btn-sm" onclick="PA.sync.refresh()" title="Refresh">&#8635; Refresh</button>' +
                        '<button class="btn btn-sm" onclick="PA.sync.close()">&times;</button>' +
                    '</div>' +
                '</div>' +
                '<div class="sync-body">' +
                    '<div class="sync-columns">' +
                        '<div class="sync-col">' +
                            '<div class="sync-col-header">' +
                                '<span class="sync-col-title">JAR Analyses (Java)</span>' +
                                '<span class="sync-col-count" id="syncJarCount">0</span>' +
                            '</div>' +
                            '<div class="sync-col-search"><input type="text" placeholder="Search JARs..." oninput="PA.sync.filterJars(this.value)"></div>' +
                            '<div class="sync-list" id="syncJarList"><div class="sync-empty">Loading...</div></div>' +
                        '</div>' +
                        '<div class="sync-col">' +
                            '<div class="sync-col-header">' +
                                '<span class="sync-col-title">Parse Analyzer (PL/SQL)</span>' +
                                '<span class="sync-col-count" id="syncParserCount">0</span>' +
                            '</div>' +
                            '<div class="sync-col-search"><input type="text" placeholder="Search analyses..." oninput="PA.sync.filterParsers(this.value)"></div>' +
                            '<div class="sync-list" id="syncParserList"><div class="sync-empty">Loading...</div></div>' +
                        '</div>' +
                    '</div>' +
                    '<div class="sync-detail" id="syncDetail">' +
                        '<div class="sync-empty">Select an item to see details</div>' +
                    '</div>' +
                '</div>' +
            '</div>';

        document.body.appendChild(overlay);
        requestAnimationFrame(function() { overlay.classList.add('open'); });
        _load();
    }

    function close() {
        _close();
    }

    function _close() {
        var el = document.getElementById('syncOverlay');
        if (el) { el.classList.remove('open'); setTimeout(function() { if (el.parentNode) el.remove(); }, 200); }
        _selectedJar = null;
        _selectedParser = null;
    }

    async function _load() {
        _jarQuery = '';
        _parserQuery = '';
        await Promise.all([_loadJars(), _loadParsers()]);
    }

    async function _loadJars() {
        try {
            var res = await fetch('/api/jar/jars');
            _jars = res.ok ? await res.json() : [];
        } catch (e) { _jars = []; }
        _renderJars();
    }

    async function _loadParsers() {
        try {
            var res = await fetch('/api/parser/analyses');
            _parsers = res.ok ? await res.json() : [];
        } catch (e) { _parsers = []; }
        _renderParsers();
    }

    function refresh() {
        var jarList = document.getElementById('syncJarList');
        var parserList = document.getElementById('syncParserList');
        if (jarList) jarList.innerHTML = '<div class="sync-empty">Loading...</div>';
        if (parserList) parserList.innerHTML = '<div class="sync-empty">Loading...</div>';
        _selectedJar = null;
        _selectedParser = null;
        _renderDetail();
        _load();
    }

    function filterJars(q) {
        _jarQuery = (q || '').toUpperCase();
        _renderJars();
    }

    function filterParsers(q) {
        _parserQuery = (q || '').toUpperCase();
        _renderParsers();
    }

    function _renderJars() {
        var container = document.getElementById('syncJarList');
        var countEl = document.getElementById('syncJarCount');
        if (!container) return;

        var list = _jars;
        if (_jarQuery) {
            list = list.filter(function(j) {
                return ((j.jarName || '') + ' ' + (j.groupId || '') + ' ' + (j.artifactId || '')).toUpperCase().indexOf(_jarQuery) >= 0;
            });
        }
        if (countEl) countEl.textContent = list.length;

        if (list.length === 0) {
            container.innerHTML = '<div class="sync-empty">No JAR analyses found</div>';
            return;
        }

        var html = '';
        for (var i = 0; i < list.length; i++) {
            var j = list[i];
            var name = j.jarName || j.artifactId || 'Unknown';
            var endpoints = j.endpointCount || 0;
            var classes = j.classCount || 0;
            var claudeStatus = j.claudeStatus || '';
            var badge = claudeStatus === 'COMPLETE' ? 'claude' : 'static';
            var badgeText = claudeStatus === 'COMPLETE' ? 'Claude' : 'Static';
            var ts = j.analyzedAt ? new Date(j.analyzedAt).toLocaleDateString() : '';

            html += '<div class="sync-item' + (_selectedJar === name ? ' selected' : '') + '" onclick="PA.sync.selectJar(\'' + _escJs(name) + '\')">';
            html += '<div class="sync-item-icon jar">JAR</div>';
            html += '<div class="sync-item-info">';
            html += '<div class="sync-item-name">' + _esc(name) + '</div>';
            html += '<div class="sync-item-meta">' + endpoints + ' endpoints | ' + classes + ' classes' + (ts ? ' | ' + _esc(ts) : '') + '</div>';
            html += '</div>';
            html += '<span class="sync-item-badge ' + badge + '">' + badgeText + '</span>';
            html += '<button class="sync-item-action" onclick="event.stopPropagation(); PA.sync.openJar(\'' + _escJs(name) + '\')" title="Open in JAR Analyzer">Open</button>';
            html += '</div>';
        }
        container.innerHTML = html;
    }

    function _renderParsers() {
        var container = document.getElementById('syncParserList');
        var countEl = document.getElementById('syncParserCount');
        if (!container) return;

        var list = _parsers;
        if (_parserQuery) {
            list = list.filter(function(p) {
                return ((p.name || '') + ' ' + (p.entryPoint || '') + ' ' + (p.entrySchema || '')).toUpperCase().indexOf(_parserQuery) >= 0;
            });
        }
        if (countEl) countEl.textContent = list.length;

        if (list.length === 0) {
            container.innerHTML = '<div class="sync-empty">No parser analyses found</div>';
            return;
        }

        var html = '';
        for (var i = 0; i < list.length; i++) {
            var p = list[i];
            var name = p.name || '';
            var entry = p.entryPoint || name;
            var schema = p.entrySchema || '';
            var nodes = p.totalNodes || 0;
            var tables = p.totalTables || 0;
            var ts = p.timestamp ? new Date(p.timestamp).toLocaleDateString() : '';

            html += '<div class="sync-item' + (_selectedParser === name ? ' selected' : '') + '" onclick="PA.sync.selectParser(\'' + _escJs(name) + '\')">';
            html += '<div class="sync-item-icon plsql">SQL</div>';
            html += '<div class="sync-item-info">';
            html += '<div class="sync-item-name">' + _esc(entry);
            if (schema) html += ' <span style="color:var(--text-muted);font-size:10px">(' + _esc(schema) + ')</span>';
            html += '</div>';
            html += '<div class="sync-item-meta">' + nodes + ' procs | ' + tables + ' tables' + (ts ? ' | ' + _esc(ts) : '') + '</div>';
            html += '</div>';
            html += '<button class="sync-item-action" onclick="event.stopPropagation(); PA.sync.loadParser(\'' + _escJs(name) + '\')" title="Load in Parser">Load</button>';
            html += '</div>';
        }
        container.innerHTML = html;
    }

    function selectJar(name) {
        _selectedJar = (_selectedJar === name) ? null : name;
        _renderJars();
        _renderDetail();
    }

    function selectParser(name) {
        _selectedParser = (_selectedParser === name) ? null : name;
        _renderParsers();
        _renderDetail();
    }

    function _renderDetail() {
        var el = document.getElementById('syncDetail');
        if (!el) return;

        if (!_selectedJar && !_selectedParser) {
            el.innerHTML = '<div class="sync-empty">Select an item to see details</div>';
            return;
        }

        var html = '';

        if (_selectedJar) {
            var j = _jars.find(function(x) { return x.jarName === _selectedJar; });
            if (j) {
                html += '<div class="sync-detail-title">JAR: ' + _esc(j.jarName || '') + '</div>';
                html += '<div class="sync-detail-grid">';
                html += _kv('Artifact', j.artifactId || j.jarName || '-');
                html += _kv('Group', j.groupId || '-');
                html += _kv('Version', j.version || '-');
                html += _kv('Endpoints', j.endpointCount || 0);
                html += _kv('Classes', j.classCount || 0);
                html += _kv('Claude', j.claudeStatus || 'N/A');
                if (j.analyzedAt) html += _kv('Analyzed', new Date(j.analyzedAt).toLocaleString());
                html += '</div>';
            }
        }

        if (_selectedParser) {
            var p = _parsers.find(function(x) { return x.name === _selectedParser; });
            if (p) {
                if (html) html += '<div style="border-top:1px solid var(--border);margin:10px 0"></div>';
                html += '<div class="sync-detail-title">Parser: ' + _esc(p.entryPoint || p.name || '') + '</div>';
                html += '<div class="sync-detail-grid">';
                html += _kv('Entry Point', p.entryPoint || '-');
                html += _kv('Schema', p.entrySchema || '-');
                html += _kv('Procedures', p.totalNodes || 0);
                html += _kv('Tables', p.totalTables || 0);
                html += _kv('LOC', (p.totalLinesOfCode || 0).toLocaleString());
                html += _kv('Crawl Time', PA.formatDurationShort ? PA.formatDurationShort(p.crawlTimeMs) : (p.crawlTimeMs || 0) + 'ms');
                if (p.timestamp) html += _kv('Analyzed', new Date(p.timestamp).toLocaleString());
                html += '</div>';
            }
        }

        el.innerHTML = html || '<div class="sync-empty">Details not available</div>';
    }

    function openJar(name) {
        window.open('/jar/#jar=' + encodeURIComponent(name), '_blank');
    }

    function loadParser(name) {
        close();
        if (PA.loadAnalysis) PA.loadAnalysis(name);
    }

    function _kv(label, value) {
        return '<div class="sync-kv"><span class="sync-kv-label">' + _esc(label) + '</span><span class="sync-kv-value">' + _esc(String(value)) + '</span></div>';
    }

    function _esc(s) { return PA.esc ? PA.esc(s) : String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
    function _escJs(s) { return PA.escJs ? PA.escJs(s) : String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'"); }

    return {
        open: open,
        close: close,
        refresh: refresh,
        filterJars: filterJars,
        filterParsers: filterParsers,
        selectJar: selectJar,
        selectParser: selectParser,
        openJar: openJar,
        loadParser: loadParser
    };
})();
