PA.logViewer = {
    _open: false,
    _pollTimer: null,
    _rawContent: '',

    toggle: function() {
        if (this._open) this.close();
        else this.open();
    },

    open: async function() {
        if (document.getElementById('logviewer-overlay')) return;
        this._open = true;

        var html = '<div class="logviewer-overlay" id="logviewer-overlay">'
            + '<div class="logviewer-panel">'
            + '<div class="logviewer-header">'
            + '<span class="logviewer-title">Application Log</span>'
            + '<span class="logviewer-meta" id="logviewer-meta"></span>'
            + '<button class="btn btn-sm" onclick="PA.logViewer.refresh()">Refresh</button>'
            + '<label class="logviewer-auto-label">'
            + '<input type="checkbox" id="logviewer-auto" onchange="PA.logViewer._toggleAuto()"> Auto'
            + '</label>'
            + '<button class="btn btn-sm" onclick="PA.logViewer.close()">Close</button>'
            + '</div>'
            + '<div class="logviewer-filter-bar">'
            + '<input type="text" id="logviewer-filter" class="logviewer-filter" placeholder="Filter logs (e.g. ERROR, Claude, procedure name...)" oninput="PA.logViewer._applyFilter()">'
            + '<select id="logviewer-level" class="logviewer-level-select" onchange="PA.logViewer._applyFilter()">'
            + '<option value="">All Levels</option>'
            + '<option value="ERROR">ERROR</option>'
            + '<option value="WARN">WARN</option>'
            + '<option value="INFO">INFO</option>'
            + '<option value="DEBUG">DEBUG</option>'
            + '</select>'
            + '</div>'
            + '<pre class="logviewer-content" id="logviewer-content">Loading...</pre>'
            + '</div></div>';
        document.body.insertAdjacentHTML('beforeend', html);

        document.getElementById('logviewer-overlay').addEventListener('click', function(e) {
            if (e.target.id === 'logviewer-overlay') PA.logViewer.close();
        });

        await this.refresh();
    },

    close: function() {
        this._open = false;
        this._stopAuto();
        var el = document.getElementById('logviewer-overlay');
        if (el) el.remove();
    },

    refresh: async function() {
        var content = document.getElementById('logviewer-content');
        var meta = document.getElementById('logviewer-meta');
        if (!content) return;

        try {
            var res = await fetch('/api/plsql/logs?lines=10000');
            if (!res.ok) throw new Error('HTTP ' + res.status);
            var data = await res.json();
            PA.logViewer._rawContent = data.content || '';
            PA.logViewer._applyFilter();
            if (meta) {
                var lineCount = data.lineCount || 0;
                var sizeKb = Math.round((data.fileSize || 0) / 1024);
                meta.textContent = lineCount + ' lines | ' + sizeKb + 'KB'
                    + (data.truncated ? ' (truncated)' : '');
            }
        } catch (e) {
            content.textContent = 'Failed to load logs: ' + e.message
                + '\n\nThe log file may not exist yet. It will appear after the first log entry.';
            if (meta) meta.textContent = 'Error';
        }
    },

    _applyFilter: function() {
        var content = document.getElementById('logviewer-content');
        if (!content || !PA.logViewer._rawContent) return;

        var filterText = (document.getElementById('logviewer-filter') || {}).value || '';
        filterText = filterText.toLowerCase();
        var levelFilter = (document.getElementById('logviewer-level') || {}).value || '';

        if (!filterText && !levelFilter) {
            content.textContent = PA.logViewer._rawContent;
        } else {
            var lines = PA.logViewer._rawContent.split('\n');
            var filtered = lines.filter(function(line) {
                if (levelFilter && line.indexOf(' ' + levelFilter + ' ') < 0) return false;
                if (filterText && line.toLowerCase().indexOf(filterText) < 0) return false;
                return true;
            });
            content.textContent = filtered.join('\n') || '(no matching lines)';
        }
        content.scrollTop = content.scrollHeight;
    },

    _toggleAuto: function() {
        var checked = document.getElementById('logviewer-auto');
        if (checked && checked.checked) {
            PA.logViewer._pollTimer = setInterval(function() { PA.logViewer.refresh(); }, 3000);
        } else {
            PA.logViewer._stopAuto();
        }
    },

    _stopAuto: function() {
        if (PA.logViewer._pollTimer) {
            clearInterval(PA.logViewer._pollTimer);
            PA.logViewer._pollTimer = null;
        }
    }
};
