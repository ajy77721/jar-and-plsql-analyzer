/**
 * Flow Analysis — test-data preparation & optimization report.
 *
 * Renders as a sub-tab inside the existing Summary page.
 * Exposes: JA.flowAnalysis._renderFlowTab(analysis, esc)  ← called by summary.js lazy renderer
 *          JA.flowAnalysis.run(mode)                       ← called from buttons in the tab
 */
window.JA = window.JA || {};
JA.flowAnalysis = {

    _currentRunId: null,
    _pollTimer:    null,

    // ── Entry point called by summary.js lazy renderer ───────────────────────

    _renderFlowTab(analysis) {
        const jarId = JA.app.currentJarId || '';
        return `
<div class="flow-tab-wrap" style="padding:20px">
  <div class="flow-section-hdr">
    <h3 style="margin:0 0 4px">Flow Analysis</h3>
    <p style="margin:0;color:#6b7280;font-size:13px">
      Run either option against the analyzed JAR. Results appear below and you can download
      the full Markdown report to share with your team.
    </p>
  </div>

  <!-- Config form -->
  <div class="flow-card" style="margin-top:16px">
    <div class="flow-field-row">
      <div class="flow-field">
        <label>MongoDB URI</label>
        <input id="flow-mongo-uri" type="text" placeholder="mongodb://localhost:27017"
               style="width:100%;padding:6px 10px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;box-sizing:border-box">
      </div>
      <div class="flow-field">
        <label>MongoDB Database</label>
        <input id="flow-mongo-db" type="text" placeholder="myapp"
               style="width:100%;padding:6px 10px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;box-sizing:border-box">
      </div>
      <div class="flow-field flow-field-sm">
        <label>Max Endpoints <small style="font-weight:400">(0 = all)</small></label>
        <input id="flow-max-ep" type="number" value="20"
               style="width:100%;padding:6px 10px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;box-sizing:border-box">
      </div>
    </div>
    <div class="flow-field" style="margin-top:10px">
      <label>Anthropic API Key <small style="font-weight:400">(optional — enables Claude AI suggestions in report)</small></label>
      <input id="flow-claude-key" type="password" placeholder="sk-ant-..."
             style="width:100%;padding:6px 10px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;box-sizing:border-box">
    </div>
    <div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap">
      <button id="flow-btn-testdata"
              style="background:#4f46e5;color:#fff;border:none;padding:9px 20px;border-radius:5px;font-size:13px;cursor:pointer"
              onclick="JA.flowAnalysis.run('testdata')">
        Prepare Test Data
      </button>
      <button id="flow-btn-optimize"
              style="background:#059669;color:#fff;border:none;padding:9px 20px;border-radius:5px;font-size:13px;cursor:pointer"
              onclick="JA.flowAnalysis.run('optimize')">
        Run Optimization Analysis
      </button>
    </div>
  </div>

  <!-- Status / results area -->
  <div id="flow-results-area" style="margin-top:18px"></div>
</div>

<style>
.flow-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:18px;box-shadow:0 1px 4px rgba(0,0,0,.06)}
.flow-field-row{display:flex;gap:14px;flex-wrap:wrap}
.flow-field{flex:1;min-width:180px}
.flow-field-sm{flex:0 0 130px}
.flow-field label{display:block;font-size:12px;font-weight:600;color:#374151;margin-bottom:4px}
.flow-badge-high{background:#fee2e2;color:#dc2626;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.flow-badge-med{background:#fef3c7;color:#d97706;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.flow-badge-low{background:#d1fae5;color:#059669;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.flow-spin{display:inline-block;width:14px;height:14px;border:2px solid #e5e7eb;border-top-color:#4f46e5;border-radius:50%;animation:flow-spin .7s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes flow-spin{to{transform:rotate(360deg)}}
.flow-results-tbl{width:100%;border-collapse:collapse;font-size:13px}
.flow-results-tbl th,.flow-results-tbl td{padding:8px 12px;text-align:left;border-bottom:1px solid #f0f0f0}
.flow-results-tbl th{background:#f9fafb;font-weight:600;color:#374151}
</style>`;
    },

    // ── Pre-fill saved connection info ────────────────────────────────────────

    async _prefillConnections() {
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/connections`);
            const data = await resp.json();
            if (data.mongoUri && document.getElementById('flow-mongo-uri'))
                document.getElementById('flow-mongo-uri').value = data.mongoUri;
            if (data.mongoDb && document.getElementById('flow-mongo-db'))
                document.getElementById('flow-mongo-db').value = data.mongoDb;
        } catch (_) {}
    },

    // ── Run ───────────────────────────────────────────────────────────────────

    async run(mode) {
        const jarId = JA.app.currentJarId;
        if (!jarId) { alert('No JAR loaded'); return; }

        const mongoUri  = (document.getElementById('flow-mongo-uri')  || {}).value || '';
        const mongoDb   = (document.getElementById('flow-mongo-db')   || {}).value || '';
        const claudeKey = (document.getElementById('flow-claude-key') || {}).value || '';
        const maxEp     = parseInt((document.getElementById('flow-max-ep') || {}).value) || 0;

        const btnId  = mode === 'testdata' ? 'flow-btn-testdata' : 'flow-btn-optimize';
        const btnOther = mode === 'testdata' ? 'flow-btn-optimize' : 'flow-btn-testdata';
        const label  = mode === 'testdata' ? 'Preparing test data…' : 'Running optimization analysis…';

        const btn = document.getElementById(btnId);
        const other = document.getElementById(btnOther);
        if (btn) { btn.disabled = true; btn.innerHTML = `<span class="flow-spin"></span>${label}`; }
        if (other) other.disabled = true;

        const area = document.getElementById('flow-results-area');
        if (area) area.innerHTML = `<div class="flow-card"><span class="flow-spin"></span> ${label}</div>`;

        try {
            const startResp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/run`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mode, mongoUri, mongoDb, claudeApiKey: claudeKey, maxEndpoints: maxEp })
            });
            const start = await startResp.json();
            if (start.status === 'error') throw new Error(start.error);
            this._currentRunId = start.runId;
            this._pollTimer = setTimeout(() => this._poll(jarId, start.runId, mode), 2000);
        } catch (e) {
            this._resetButtons(mode);
            if (area) area.innerHTML = `<div class="flow-card" style="color:#dc2626">Error: ${JA.utils.escapeHtml(e.message)}</div>`;
        }
    },

    async _poll(jarId, runId, mode) {
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/result/${runId}`);
            const data = await resp.json();

            if (data.status === 'running') {
                this._pollTimer = setTimeout(() => this._poll(jarId, runId, mode), 2500);
                return;
            }

            this._resetButtons(mode);

            const area = document.getElementById('flow-results-area');
            if (!area) return;

            if (data.status === 'error') {
                area.innerHTML = `<div class="flow-card" style="border-color:#fca5a5;color:#dc2626">
                    <b>Analysis failed:</b> ${JA.utils.escapeHtml(data.error || 'unknown error')}
                </div>`;
                return;
            }

            this._renderResults(area, jarId, runId, data.results || [], mode);

        } catch (e) {
            this._resetButtons(mode);
            const area = document.getElementById('flow-results-area');
            if (area) area.innerHTML = `<div class="flow-card" style="color:#dc2626">Poll error: ${JA.utils.escapeHtml(e.message)}</div>`;
        }
    },

    _renderResults(area, jarId, runId, results, mode) {
        let high = 0, med = 0, low = 0;
        for (const r of results) {
            for (const f of (r.optimizations || [])) {
                if (f.severity === 'HIGH') high++;
                else if (f.severity === 'MEDIUM') med++;
                else low++;
            }
        }

        const modeLabel = mode === 'testdata' ? 'Test Data Preparation' : 'Optimization Analysis';

        let html = `<div class="flow-card">
            <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px">
                <div>
                    <b>${modeLabel} complete</b>
                    <span style="margin-left:12px;font-size:13px">
                        <b>${results.length}</b> endpoints &nbsp;
                        <span class="flow-badge-high">${high} HIGH</span>&nbsp;
                        <span class="flow-badge-med">${med} MEDIUM</span>&nbsp;
                        <span class="flow-badge-low">${low} LOW</span>
                    </span>
                </div>
                <button onclick="JA.flowAnalysis._download('${encodeURIComponent(jarId)}','${runId}')"
                        style="background:#0284c7;color:#fff;border:none;padding:7px 16px;border-radius:5px;font-size:13px;cursor:pointer">
                    Download .md Report
                </button>
            </div>`;

        if (results.length > 0) {
            html += `<div style="margin-top:16px;overflow-x:auto">
            <table class="flow-results-tbl">
                <thead><tr>
                    <th>Severity</th><th>Category</th><th>Endpoint</th>
                    <th>Collection</th><th>Description</th>
                </tr></thead><tbody>`;

            for (const r of results) {
                for (const f of (r.optimizations || [])) {
                    const badge = f.severity === 'HIGH'
                        ? `<span class="flow-badge-high">HIGH</span>`
                        : f.severity === 'MEDIUM'
                        ? `<span class="flow-badge-med">MEDIUM</span>`
                        : `<span class="flow-badge-low">LOW</span>`;
                    html += `<tr>
                        <td>${badge}</td>
                        <td style="white-space:nowrap">${JA.utils.escapeHtml((f.category||'').replace(/_/g,' '))}</td>
                        <td><code style="font-size:11px">${JA.utils.escapeHtml(r.endpoint||'')}</code></td>
                        <td><code style="font-size:11px">${JA.utils.escapeHtml(f.table||'-')}</code></td>
                        <td style="max-width:360px">${JA.utils.escapeHtml(f.description||'')}</td>
                    </tr>`;
                }
            }

            if (high + med + low === 0) {
                html += `<tr><td colspan="5" style="text-align:center;color:#6b7280;padding:16px">No optimization findings — flow looks clean</td></tr>`;
            }

            html += '</tbody></table></div>';
        }

        html += '</div>';
        area.innerHTML = html;
    },

    async _download(jarId, runId) {
        const resp = await fetch(`/api/jar/jars/${decodeURIComponent(jarId)}/flow/report/${runId}`);
        const text = await resp.text();
        const blob = new Blob([text], { type: 'text/markdown' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = decodeURIComponent(jarId).replace('.jar','') + '-flow-analysis.md';
        a.click();
    },

    _resetButtons(mode) {
        const testBtn = document.getElementById('flow-btn-testdata');
        const optBtn  = document.getElementById('flow-btn-optimize');
        if (testBtn) { testBtn.disabled = false; testBtn.innerHTML = 'Prepare Test Data'; }
        if (optBtn)  { optBtn.disabled = false;  optBtn.innerHTML  = 'Run Optimization Analysis'; }
    }
};
