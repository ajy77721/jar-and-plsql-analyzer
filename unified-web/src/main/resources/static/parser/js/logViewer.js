PA.logViewer = {
    _open: false,
    _pollTimer: null,
    _tail: null,

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
        PA.mountOverlay(html);

        var lvOverlay = document.getElementById('logviewer-overlay');
        if (PA.modalStack) lvOverlay.style.zIndex = PA.modalStack.nextZ();
        lvOverlay.addEventListener('click', function(e) {
            if (e.target.id === 'logviewer-overlay') PA.logViewer.close();
        });

        await this.refresh();
    },

    close: function() {
        this._open = false;
        this._tail = null;   // reopening should show the current tail, not a stale buffer
        this._stopAuto();
        var el = document.getElementById('logviewer-overlay');
        if (el) el.remove();
    },

    /**
     * Pulls whatever is new and appends it.
     *
     * Was `?lines=10000` on every tick — the largest response in the application — then a
     * client-side level and text sweep over the whole buffer on every keystroke. Now a `seq`
     * cursor: the first paint asks for the tail, every tick after it asks only for lines newer
     * than the last one held, and both filters are applied by the server.
     */
    refresh: async function() {
        var content = document.getElementById('logviewer-content');
        var meta = document.getElementById('logviewer-meta');
        if (!content) return;

        var lv = PA.logViewer;
        lv._tail = lv._tail || LogTailSource('/api/plsql/logs');
        try {
            var levelEl = document.getElementById('logviewer-level');
            var filterEl = document.getElementById('logviewer-filter');
            lv._tail.setFilter(levelEl ? levelEl.value : '', filterEl ? filterEl.value : '');
            await lv._tail.poll();
            content.textContent = lv._tail.isEmpty() ? '(no matching lines)' : lv._tail.text();
            content.scrollTop = content.scrollHeight;
            if (meta) {
                var sizeKb = Math.round((lv._tail.fileSize || 0) / 1024);
                meta.textContent = lv._tail.watermark + ' lines | ' + sizeKb + 'KB';
            }
        } catch (e) {
            content.textContent = 'Failed to load logs: ' + e.message
                + '\n\nThe log file may not exist yet. It will appear after the first log entry.';
            if (meta) meta.textContent = 'Error';
        }
    },

    /** Filter changes are a server round-trip now, not a re-sweep of a local string. */
    _applyFilter: function() {
        clearTimeout(PA.logViewer._filterDebounce);
        PA.logViewer._filterDebounce = setTimeout(function() { PA.logViewer.refresh(); }, 250);
    },

    _toggleAuto: function() {
        var checked = document.getElementById('logviewer-auto');
        if (checked && checked.checked) {
            PA.logViewer._pollTimer = PollConfig.every('logRefreshMs', function() { PA.logViewer.refresh(); });
        } else {
            PA.logViewer._stopAuto();
        }
    },

    _stopAuto: function() {
        PA.logViewer._pollTimer = PollConfig.clear(PA.logViewer._pollTimer);
    }
};
