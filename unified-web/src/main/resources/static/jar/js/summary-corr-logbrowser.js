/**
 * Summary Corrections — Browse All Decision Logs.
 * Shows a modal with a list of all correction log files,
 * grouped by endpoint, with click-to-view content.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    /** Open the browse-all correction logs modal. */
    async _browseAllCorrLogs() {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) { JA.toast?.error('No JAR loaded'); return; }
        const esc = JA.utils.escapeHtml;

        try {
            const files = await JA.api.listAllCorrectionLogs(jarId);
            if (!files || !files.length) {
                JA.toast?.info('No correction logs found. Run a correction scan first.');
                return;
            }

            // Group files by endpoint
            const grouped = this._groupCorrLogsByEndpoint(files);

            // Build the modal
            const html = this._buildCorrLogBrowserModal(grouped, esc);
            document.querySelector('.corr-log-overlay')?.remove();
            document.body.insertAdjacentHTML('beforeend', html);
        } catch (e) {
            JA.toast?.error('Failed to load correction logs: ' + e.message);
        }
    },

    /** Group log file entries by endpoint name. */
    _groupCorrLogsByEndpoint(files) {
        const groups = {};
        for (const f of files) {
            const ep = f.endpoint || 'unknown';
            if (!groups[ep]) groups[ep] = [];
            groups[ep].push(f);
        }
        // Sort endpoints alphabetically
        const sorted = {};
        for (const key of Object.keys(groups).sort()) {
            sorted[key] = groups[key];
        }
        return sorted;
    },

    /** Build the two-panel browse modal HTML. */
    _buildCorrLogBrowserModal(grouped, esc) {
        let html = '<div class="corr-log-overlay" onclick="if(event.target===this)this.remove()">';
        html += '<div class="corr-log-modal corr-logbrowser-modal">';

        // Header
        html += '<div class="corr-log-header">';
        html += '<span class="claude-badge" style="background:#8b5cf6">DECISION LOGS</span>';
        html += '<span class="corr-log-title">All Correction Logs</span>';
        html += '<button class="corr-log-close" onclick="this.closest(\'.corr-log-overlay\').remove()">&times;</button>';
        html += '</div>';

        // Two-panel body
        html += '<div class="corr-logbrowser-body">';

        // Left panel: file list grouped by endpoint
        html += '<div class="corr-logbrowser-list">';
        for (const [ep, files] of Object.entries(grouped)) {
            html += '<div class="corr-logbrowser-group">';
            html += '<div class="corr-logbrowser-ep">' + esc(ep) + '</div>';
            for (const f of files) {
                const sizeKb = (f.size / 1024).toFixed(1);
                const typeCls = f.type === 'prompt' ? 'corr-logbrowser-prompt' : 'corr-logbrowser-response';
                const safeName = f.name.replace(/'/g, "\\'");
                html += '<button class="corr-logbrowser-item ' + typeCls + '" '
                    + 'onclick="JA.summary._loadCorrLogFile(\'' + safeName + '\',this)" '
                    + 'title="' + esc(f.name) + '">';
                html += '<span class="corr-logbrowser-item-label">' + esc(f.label) + '</span>';
                html += '<span class="corr-log-size">' + sizeKb + ' KB</span>';
                html += '</button>';
            }
            html += '</div>';
        }
        html += '</div>';

        // Right panel: content viewer
        html += '<div class="corr-logbrowser-viewer">';
        html += '<div class="corr-logbrowser-placeholder" id="corr-logbrowser-viewer">';
        html += 'Select a log file from the list to view its content.';
        html += '</div>';
        html += '</div>';

        html += '</div>'; // end body
        html += '</div></div>'; // end modal, overlay
        return html;
    },

    /** Load and display a single correction log file in the viewer panel. */
    async _loadCorrLogFile(fileName, btn) {
        const jarId = JA.app.currentJarId || '';
        if (!jarId) return;
        const esc = JA.utils.escapeHtml;
        const viewer = document.getElementById('corr-logbrowser-viewer');
        if (!viewer) return;

        // Highlight selected item
        const modal = btn.closest('.corr-logbrowser-modal');
        if (modal) {
            modal.querySelectorAll('.corr-logbrowser-item').forEach(b => b.classList.remove('active'));
        }
        btn.classList.add('active');

        viewer.className = 'corr-log-content';
        viewer.innerHTML = '<pre class="corr-log-pre">Loading ' + esc(fileName) + '...</pre>';

        try {
            const content = await JA.api.getCorrectionLogFile(jarId, fileName);
            viewer.innerHTML = '<pre class="corr-log-pre">' + esc(content || '(empty)') + '</pre>';
        } catch (e) {
            viewer.innerHTML = '<pre class="corr-log-pre" style="color:#ef4444">Failed to load: '
                + esc(e.message) + '</pre>';
        }
    }
});
