/**
 * PA.logViewer — Floating log viewer panel showing last 10K lines of app log.
 * Supports text search, log level filtering, and auto-refresh.
 */
window.PA = window.PA || {};

PA.logViewer = {
    _open: false,
    _pollTimer: null,
    _tail: null,

    toggle() {
        if (this._open) this.close();
        else this.open();
    },

    async open() {
        if (document.getElementById('logviewer-overlay')) return;
        this._open = true;

        const html = `<div class="logviewer-overlay" id="logviewer-overlay">
            <div class="logviewer-panel">
                <div class="logviewer-header">
                    <span class="logviewer-title">Application Log</span>
                    <span class="logviewer-meta" id="logviewer-meta"></span>
                    <button class="btn btn-sm" onclick="PA.logViewer.refresh()">Refresh</button>
                    <label class="logviewer-auto-label">
                        <input type="checkbox" id="logviewer-auto" onchange="PA.logViewer._toggleAuto()"> Auto
                    </label>
                    <button class="btn btn-sm" onclick="PA.logViewer.close()">Close</button>
                </div>
                <div class="logviewer-filter-bar">
                    <input type="text" id="logviewer-filter" class="logviewer-filter form-input"
                           placeholder="Filter logs (e.g. ERROR, Claude, analysis...)"
                           oninput="PA.logViewer._applyFilter()">
                    <select id="logviewer-level" class="logviewer-level-select form-select" onchange="PA.logViewer._applyFilter()">
                        <option value="">All Levels</option>
                        <option value="ERROR">ERROR</option>
                        <option value="WARN">WARN</option>
                        <option value="INFO">INFO</option>
                        <option value="DEBUG">DEBUG</option>
                    </select>
                </div>
                <pre class="logviewer-content" id="logviewer-content">Loading...</pre>
            </div>
        </div>`;
        PA.mountOverlay(html);

        // Close on overlay click
        document.getElementById('logviewer-overlay').addEventListener('click', e => {
            if (e.target.id === 'logviewer-overlay') PA.logViewer.close();
        });

        await this.refresh();
    },

    close() {
        this._open = false;
        this._tail = null;   // reopening should show the current tail, not a stale buffer
        this._stopAuto();
        document.getElementById('logviewer-overlay')?.remove();
    },

    /**
     * Pulls whatever is new and appends it.
     *
     * Was `?lines=10000` on every tick — the largest response in the application — then a
     * client-side level and text sweep over the whole buffer on every keystroke. Now a `seq`
     * cursor: the first paint asks for the tail, every tick after it asks only for lines newer
     * than the last one held, and both filters are applied by the server.
     */
    async refresh() {
        const content = document.getElementById('logviewer-content');
        const meta = document.getElementById('logviewer-meta');
        if (!content) return;

        this._tail = this._tail || LogTailSource('/api/plsql/logs');
        try {
            this._tail.setFilter(document.getElementById('logviewer-level')?.value || '',
                                 document.getElementById('logviewer-filter')?.value || '');
            await this._tail.poll();
            content.textContent = this._tail.isEmpty() ? '(no matching lines)' : this._tail.text();
            content.scrollTop = content.scrollHeight;
            if (meta) {
                const sizeKb = Math.round((this._tail.fileSize || 0) / 1024);
                meta.textContent = this._tail.watermark + ' lines | ' + sizeKb + 'KB';
            }
        } catch (err) {
            content.textContent = 'Failed to load logs: ' + err.message
                + '\n\nThe log file may not exist yet. It will be created after the first log entry.';
            if (meta) meta.textContent = 'Error';
        }
    },

    /** Filter changes are a server round-trip now, not a re-sweep of a local string. */
    _applyFilter() {
        clearTimeout(this._filterDebounce);
        this._filterDebounce = setTimeout(() => this.refresh(), 250);
    },

    _toggleAuto() {
        const checked = document.getElementById('logviewer-auto')?.checked;
        if (checked) {
            this._pollTimer = setInterval(() => this.refresh(), PollConfig.logRefreshMs);
        } else {
            this._stopAuto();
        }
    },

    _stopAuto() {
        if (this._pollTimer) {
            clearInterval(this._pollTimer);
            this._pollTimer = null;
        }
    }
};
