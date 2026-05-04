/**
 * PA.logViewer — Floating log viewer panel showing last 10K lines of app log.
 * Supports text search, log level filtering, and auto-refresh.
 */
window.PA = window.PA || {};

PA.logViewer = {
    _open: false,
    _pollTimer: null,
    _rawContent: '',

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
        document.body.insertAdjacentHTML('beforeend', html);

        // Close on overlay click
        document.getElementById('logviewer-overlay').addEventListener('click', e => {
            if (e.target.id === 'logviewer-overlay') PA.logViewer.close();
        });

        await this.refresh();
    },

    close() {
        this._open = false;
        this._stopAuto();
        document.getElementById('logviewer-overlay')?.remove();
    },

    async refresh() {
        const content = document.getElementById('logviewer-content');
        const meta = document.getElementById('logviewer-meta');
        if (!content) return;

        try {
            const resp = await fetch('/api/plsql/logs?lines=10000');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            const data = await resp.json();
            this._rawContent = data.content || '';
            this._applyFilter();
            if (meta) {
                const sizeKb = Math.round((data.fileSize || 0) / 1024);
                meta.textContent = data.lineCount + ' lines | ' + sizeKb + 'KB'
                    + (data.truncated ? ' (truncated)' : '');
            }
        } catch (err) {
            content.textContent = 'Failed to load logs: ' + err.message
                + '\n\nThe log file may not exist yet. It will be created after the first log entry.';
            if (meta) meta.textContent = 'Error';
        }
    },

    _applyFilter() {
        const content = document.getElementById('logviewer-content');
        if (!content || !this._rawContent) return;

        const filterText = (document.getElementById('logviewer-filter')?.value || '').toLowerCase();
        const levelFilter = document.getElementById('logviewer-level')?.value || '';

        if (!filterText && !levelFilter) {
            content.textContent = this._rawContent;
        } else {
            const lines = this._rawContent.split('\n');
            const filtered = lines.filter(line => {
                if (levelFilter && !line.includes(' ' + levelFilter + ' ')) return false;
                if (filterText && !line.toLowerCase().includes(filterText)) return false;
                return true;
            });
            content.textContent = filtered.join('\n') || '(no matching lines)';
        }
        // Auto-scroll to bottom
        content.scrollTop = content.scrollHeight;
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
