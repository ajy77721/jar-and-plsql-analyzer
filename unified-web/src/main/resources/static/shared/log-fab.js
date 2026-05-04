/**
 * Shared Log FAB + overlay viewer.
 * Works on all pages — tries multiple log endpoints.
 */
const LogFab = (() => {

    let _open = false;
    let _pollTimer = null;
    let _rawContent = '';
    let _logUrl = null;

    const LOG_URLS = ['/api/plsql/logs', '/api/jar/logs'];

    function init() {
        if (document.getElementById('log-fab')) return;
        const fab = document.createElement('button');
        fab.id = 'log-fab';
        fab.className = 'log-fab';
        fab.title = 'View application logs';
        fab.setAttribute('aria-label', 'Open logs');
        fab.innerHTML = '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>';
        fab.addEventListener('click', toggle);
        document.body.appendChild(fab);
    }

    function toggle() { _open ? close() : open(); }

    async function open() {
        if (document.getElementById('log-fab-overlay')) return;
        _open = true;

        const fab = document.getElementById('log-fab');
        if (fab) fab.classList.add('log-fab-active');

        const html = '<div class="log-fab-overlay" id="log-fab-overlay">' +
            '<div class="log-fab-panel">' +
                '<div class="log-fab-header">' +
                    '<span class="log-fab-title">Application Log</span>' +
                    '<span class="log-fab-meta" id="log-fab-meta"></span>' +
                    '<button class="log-fab-btn" onclick="LogFab.refresh()">Refresh</button>' +
                    '<label class="log-fab-auto-label">' +
                        '<input type="checkbox" id="log-fab-auto" onchange="LogFab._toggleAuto()"> Auto' +
                    '</label>' +
                    '<button class="log-fab-btn" onclick="LogFab.close()">Close</button>' +
                '</div>' +
                '<div class="log-fab-filter-bar">' +
                    '<input type="text" id="log-fab-filter" class="log-fab-filter" placeholder="Filter logs (e.g. ERROR, Claude, analysis...)" oninput="LogFab._applyFilter()">' +
                    '<select id="log-fab-level" class="log-fab-level-select" onchange="LogFab._applyFilter()">' +
                        '<option value="">All Levels</option>' +
                        '<option value="ERROR">ERROR</option>' +
                        '<option value="WARN">WARN</option>' +
                        '<option value="INFO">INFO</option>' +
                        '<option value="DEBUG">DEBUG</option>' +
                    '</select>' +
                '</div>' +
                '<pre class="log-fab-content" id="log-fab-content">Loading...</pre>' +
            '</div>' +
        '</div>';
        document.body.insertAdjacentHTML('beforeend', html);

        document.getElementById('log-fab-overlay').addEventListener('click', function(e) {
            if (e.target.id === 'log-fab-overlay') LogFab.close();
        });

        await refresh();
    }

    function close() {
        _open = false;
        _stopAuto();
        const overlay = document.getElementById('log-fab-overlay');
        if (overlay) overlay.remove();
        const fab = document.getElementById('log-fab');
        if (fab) fab.classList.remove('log-fab-active');
    }

    async function refresh() {
        const content = document.getElementById('log-fab-content');
        const meta = document.getElementById('log-fab-meta');
        if (!content) return;

        try {
            const data = await _fetchLogs();
            _rawContent = data.content || '';
            _applyFilter();
            if (meta) {
                const sizeKb = Math.round((data.fileSize || 0) / 1024);
                meta.textContent = data.lineCount + ' lines | ' + sizeKb + 'KB' + (data.truncated ? ' (truncated)' : '');
            }
        } catch (err) {
            content.textContent = 'Failed to load logs: ' + err.message + '\n\nThe log file may not exist yet.';
            if (meta) meta.textContent = 'Error';
        }
    }

    async function _fetchLogs() {
        if (_logUrl) {
            const resp = await fetch(_logUrl + '?lines=10000');
            if (resp.ok) return resp.json();
        }
        for (const url of LOG_URLS) {
            try {
                const resp = await fetch(url + '?lines=10000');
                if (resp.ok) {
                    _logUrl = url;
                    return resp.json();
                }
            } catch (e) {}
        }
        throw new Error('No log endpoint available');
    }

    function _applyFilter() {
        const content = document.getElementById('log-fab-content');
        if (!content || !_rawContent) return;

        const filterText = (document.getElementById('log-fab-filter')?.value || '').toLowerCase();
        const levelFilter = document.getElementById('log-fab-level')?.value || '';

        if (!filterText && !levelFilter) {
            content.textContent = _rawContent;
        } else {
            const lines = _rawContent.split('\n');
            const filtered = lines.filter(function(line) {
                if (levelFilter && !line.includes(' ' + levelFilter + ' ')) return false;
                if (filterText && !line.toLowerCase().includes(filterText)) return false;
                return true;
            });
            content.textContent = filtered.join('\n') || '(no matching lines)';
        }
        content.scrollTop = content.scrollHeight;
    }

    function _toggleAuto() {
        var checked = document.getElementById('log-fab-auto')?.checked;
        if (checked) {
            _pollTimer = setInterval(function() { refresh(); }, PollConfig.logRefreshMs);
        } else {
            _stopAuto();
        }
    }

    function _stopAuto() {
        if (_pollTimer) {
            clearInterval(_pollTimer);
            _pollTimer = null;
        }
    }

    return { init, toggle, open, close, refresh, _applyFilter, _toggleAuto };

})();
