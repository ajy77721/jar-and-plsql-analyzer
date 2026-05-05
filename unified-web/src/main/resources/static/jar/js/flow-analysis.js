/**
 * Flow Analysis tab — test-data preparation & optimization report.
 *
 * Shows the endpoint list from the current analysis.
 * MongoDB and Claude details are loaded silently from the system.
 * The user just picks endpoints and clicks one of two buttons.
 */
window.JA = window.JA || {};
JA.flowAnalysis = {

    _currentRunId: null,

    // ── Entry point called by summary.js lazy renderer ───────────────────────

    _renderFlowTab(analysis) {
        const endpoints = (analysis && analysis.endpoints) || [];

        let epRows = '';
        for (const ep of endpoints) {
            const method = (ep.httpMethod || 'GET').toUpperCase();
            const path   = ep.fullPath || ep.path || ep.endpointName || '';
            const safe   = JA.utils.escapeHtml(path);
            const methodCls = method === 'GET' ? '#2563eb' : method === 'POST' ? '#16a34a' : method === 'PUT' ? '#d97706' : method === 'DELETE' ? '#dc2626' : '#6b7280';
            const fullKey = JA.utils.escapeHtml(method + ' ' + path);
            epRows += `<label class="flow-ep-row">
                <input type="checkbox" class="flow-ep-cb" value="${fullKey}" checked>
                <span style="color:${methodCls};font-weight:600;min-width:52px;display:inline-block;font-size:11px">${JA.utils.escapeHtml(method)}</span>
                <code style="font-size:12px">${safe}</code>
            </label>`;
        }

        if (!epRows) {
            epRows = '<p style="color:#9ca3af;padding:8px 0">No endpoints found in this analysis</p>';
        }

        return `
<div class="flow-tab-wrap" style="padding:20px">

  <!-- Action buttons -->
  <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px">
    <div style="flex:1;min-width:200px">
      <div class="flow-action-card" onclick="JA.flowAnalysis._setMode('testdata')" id="flow-card-testdata"
           style="border:2px solid #4f46e5;border-radius:8px;padding:16px;cursor:pointer;background:#f5f3ff">
        <div style="font-weight:700;color:#4f46e5;font-size:14px;margin-bottom:4px">Prepare Test Data</div>
        <div style="font-size:12px;color:#6b7280">Walks the call tree and fetches real MongoDB records to build parameterised test datasets</div>
      </div>
    </div>
    <div style="flex:1;min-width:200px">
      <div class="flow-action-card" onclick="JA.flowAnalysis._setMode('optimize')" id="flow-card-optimize"
           style="border:2px solid #e5e7eb;border-radius:8px;padding:16px;cursor:pointer;background:#fff">
        <div style="font-weight:700;color:#374151;font-size:14px;margin-bottom:4px">Run Optimization Analysis</div>
        <div style="font-size:12px;color:#6b7280">Detects N+1, missing indexes, bulk read/write, aggregation rewrites, static vs transactional classification</div>
      </div>
    </div>
  </div>

  <!-- Endpoint list -->
  <div class="flow-card">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
      <b style="font-size:13px">Endpoints to analyze</b>
      <span style="font-size:12px;color:#6b7280">
        <a href="#" onclick="JA.flowAnalysis._selectAll(true);return false" style="color:#4f46e5;text-decoration:none">Select all</a>
        &nbsp;·&nbsp;
        <a href="#" onclick="JA.flowAnalysis._selectAll(false);return false" style="color:#4f46e5;text-decoration:none">Deselect all</a>
        &nbsp;&nbsp;
        <span id="flow-ep-count" style="color:#374151;font-weight:600">${endpoints.length} selected</span>
      </span>
    </div>
    <div id="flow-ep-list" style="max-height:240px;overflow-y:auto;display:flex;flex-direction:column;gap:4px">
      ${epRows}
    </div>
  </div>

  <!-- Override connection (collapsed) -->
  <div style="margin-top:10px">
    <a href="#" id="flow-override-toggle" onclick="JA.flowAnalysis._toggleOverride();return false"
       style="font-size:12px;color:#6b7280;text-decoration:none">
      ⚙ Override connection settings
    </a>
    <div id="flow-override-panel" style="display:none;margin-top:10px">
      <div class="flow-card" style="border-color:#e5e7eb">
        <div class="flow-field-row">
          <div class="flow-field">
            <label>MongoDB URI <small style="font-weight:400">(leave blank to use from analysis)</small></label>
            <input id="flow-mongo-uri" type="text" placeholder="mongodb://localhost:27017"
                   style="width:100%;padding:6px 10px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;box-sizing:border-box">
          </div>
          <div class="flow-field">
            <label>MongoDB Database</label>
            <input id="flow-mongo-db" type="text" placeholder="auto-detected"
                   style="width:100%;padding:6px 10px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;box-sizing:border-box">
          </div>
        </div>
        <div class="flow-field" style="margin-top:10px">
          <label>Anthropic API Key <small style="font-weight:400">(optional — leave blank to use ANTHROPIC_API_KEY env var)</small></label>
          <input id="flow-claude-key" type="password" placeholder="sk-ant-..."
                 style="width:100%;padding:6px 10px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;box-sizing:border-box">
        </div>
      </div>
    </div>
  </div>

  <!-- Run button -->
  <div style="margin-top:16px">
    <button id="flow-run-btn"
            style="background:#4f46e5;color:#fff;border:none;padding:10px 28px;border-radius:6px;font-size:14px;font-weight:600;cursor:pointer"
            onclick="JA.flowAnalysis.run()">
      ▶ Run
    </button>
    <span id="flow-mode-label" style="margin-left:12px;font-size:13px;color:#6b7280">Mode: Prepare Test Data</span>
  </div>

  <!-- Status / results area -->
  <div id="flow-results-area" style="margin-top:18px"></div>
</div>

<style>
.flow-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.05)}
.flow-field-row{display:flex;gap:14px;flex-wrap:wrap}
.flow-field{flex:1;min-width:180px}
.flow-field label{display:block;font-size:12px;font-weight:600;color:#374151;margin-bottom:4px}
.flow-ep-row{display:flex;align-items:center;gap:8px;padding:4px 6px;border-radius:4px;cursor:pointer;font-size:13px}
.flow-ep-row:hover{background:#f9fafb}
.flow-ep-cb{cursor:pointer;accent-color:#4f46e5}
.flow-badge-high{background:#fee2e2;color:#dc2626;padding:2px 7px;border-radius:10px;font-size:11px;font-weight:600}
.flow-badge-med{background:#fef3c7;color:#d97706;padding:2px 7px;border-radius:10px;font-size:11px;font-weight:600}
.flow-badge-low{background:#d1fae5;color:#059669;padding:2px 7px;border-radius:10px;font-size:11px;font-weight:600}
.flow-spin{display:inline-block;width:13px;height:13px;border:2px solid #e5e7eb;border-top-color:#4f46e5;border-radius:50%;animation:flow-spin .7s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes flow-spin{to{transform:rotate(360deg)}}
.flow-results-tbl{width:100%;border-collapse:collapse;font-size:13px}
.flow-results-tbl th,.flow-results-tbl td{padding:8px 12px;text-align:left;border-bottom:1px solid #f0f0f0}
.flow-results-tbl th{background:#f9fafb;font-weight:600;color:#374151}
</style>`;
    },

    // ── Initialise state after render ─────────────────────────────────────────

    _mode: 'testdata',

    _init() {
        // Update checkbox count on change
        const list = document.getElementById('flow-ep-list');
        if (list) list.addEventListener('change', () => this._updateCount());
        this._setMode('testdata');
    },

    _setMode(mode) {
        this._mode = mode;
        const tdCard  = document.getElementById('flow-card-testdata');
        const optCard = document.getElementById('flow-card-optimize');
        const label   = document.getElementById('flow-mode-label');
        const btn     = document.getElementById('flow-run-btn');

        if (mode === 'testdata') {
            if (tdCard)  { tdCard.style.border  = '2px solid #4f46e5'; tdCard.style.background  = '#f5f3ff'; }
            if (optCard) { optCard.style.border = '2px solid #e5e7eb'; optCard.style.background = '#fff'; }
            if (label)   label.textContent = 'Mode: Prepare Test Data';
            if (btn)     btn.style.background = '#4f46e5';
        } else {
            if (tdCard)  { tdCard.style.border  = '2px solid #e5e7eb'; tdCard.style.background  = '#fff'; }
            if (optCard) { optCard.style.border = '2px solid #059669'; optCard.style.background = '#f0fdf4'; }
            if (label)   label.textContent = 'Mode: Run Optimization Analysis';
            if (btn)     btn.style.background = '#059669';
        }
    },

    _selectAll(checked) {
        document.querySelectorAll('.flow-ep-cb').forEach(cb => cb.checked = checked);
        this._updateCount();
    },

    _updateCount() {
        const total    = document.querySelectorAll('.flow-ep-cb').length;
        const selected = document.querySelectorAll('.flow-ep-cb:checked').length;
        const el = document.getElementById('flow-ep-count');
        if (el) el.textContent = selected + ' of ' + total + ' selected';
    },

    _toggleOverride() {
        const panel = document.getElementById('flow-override-panel');
        if (!panel) return;
        const open = panel.style.display === 'none';
        panel.style.display = open ? '' : 'none';
        const toggle = document.getElementById('flow-override-toggle');
        if (toggle) toggle.textContent = open ? '⚙ Hide connection settings' : '⚙ Override connection settings';
    },

    // ── Run ───────────────────────────────────────────────────────────────────

    async run() {
        const jarId = JA.app.currentJarId;
        if (!jarId) { alert('No JAR loaded'); return; }

        const selectedPaths = Array.from(document.querySelectorAll('.flow-ep-cb:checked'))
                                   .map(cb => cb.value);
        if (selectedPaths.length === 0) { alert('Select at least one endpoint'); return; }

        const mongoUri  = (document.getElementById('flow-mongo-uri')  || {}).value || '';
        const mongoDb   = (document.getElementById('flow-mongo-db')   || {}).value || '';
        const claudeKey = (document.getElementById('flow-claude-key') || {}).value || '';

        const btn = document.getElementById('flow-run-btn');
        const lbl = document.getElementById('flow-mode-label');
        if (btn) { btn.disabled = true; btn.innerHTML = '<span class="flow-spin"></span>Running…'; }

        const area = document.getElementById('flow-results-area');
        if (area) area.innerHTML = '<div class="flow-card"><span class="flow-spin"></span> Analysis running…</div>';

        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/run`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    mode: this._mode,
                    endpointPaths: selectedPaths,
                    mongoUri,
                    mongoDb,
                    claudeApiKey: claudeKey
                })
            });
            const start = await resp.json();
            if (start.error) throw new Error(start.error);
            this._currentRunId = start.runId;
            setTimeout(() => this._poll(jarId, start.runId), 2500);
        } catch (e) {
            if (btn) { btn.disabled = false; btn.innerHTML = '▶ Run'; }
            if (area) area.innerHTML = `<div class="flow-card" style="color:#dc2626">Error: ${JA.utils.escapeHtml(e.message)}</div>`;
        }
    },

    async _poll(jarId, runId) {
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/result/${runId}`);
            const data = await resp.json();

            if (data.status === 'running') {
                setTimeout(() => this._poll(jarId, runId), 2500);
                return;
            }

            const btn = document.getElementById('flow-run-btn');
            if (btn) { btn.disabled = false; btn.innerHTML = '▶ Run'; }

            const area = document.getElementById('flow-results-area');
            if (!area) return;

            if (data.status === 'error') {
                area.innerHTML = `<div class="flow-card" style="border-color:#fca5a5;color:#dc2626">
                    <b>Analysis failed:</b> ${JA.utils.escapeHtml(data.error || 'unknown error')}
                </div>`;
                return;
            }

            this._renderResults(area, jarId, runId, data.results || []);

        } catch (e) {
            const btn = document.getElementById('flow-run-btn');
            if (btn) { btn.disabled = false; btn.innerHTML = '▶ Run'; }
        }
    },

    _renderResults(area, jarId, runId, results) {
        let high = 0, med = 0, low = 0;
        for (const r of results) {
            for (const f of (r.optimizations || [])) {
                if (f.severity === 'HIGH') high++;
                else if (f.severity === 'MEDIUM') med++;
                else low++;
            }
        }

        let html = `<div class="flow-card">
            <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px">
                <div>
                    <b>Analysis complete</b>
                    <span style="margin-left:12px;font-size:13px">
                        <b>${results.length}</b> endpoints &nbsp;
                        <span class="flow-badge-high">${high} HIGH</span>&nbsp;
                        <span class="flow-badge-med">${med} MEDIUM</span>&nbsp;
                        <span class="flow-badge-low">${low} LOW</span>
                    </span>
                </div>
                <button onclick="JA.flowAnalysis._download('${encodeURIComponent(jarId)}','${runId}')"
                        style="background:#0284c7;color:#fff;border:none;padding:7px 16px;border-radius:5px;font-size:13px;cursor:pointer;font-weight:600">
                    ⬇ Download .md Report
                </button>
            </div>`;

        if (high + med + low > 0) {
            html += `<div style="margin-top:16px;overflow-x:auto">
            <table class="flow-results-tbl">
                <thead><tr>
                    <th>Severity</th><th>Category</th><th>Endpoint</th>
                    <th>Collection</th><th>Description</th>
                </tr></thead><tbody>`;

            for (const r of results) {
                for (const f of (r.optimizations || [])) {
                    const badge = f.severity === 'HIGH'
                        ? '<span class="flow-badge-high">HIGH</span>'
                        : f.severity === 'MEDIUM'
                        ? '<span class="flow-badge-med">MEDIUM</span>'
                        : '<span class="flow-badge-low">LOW</span>';
                    html += `<tr>
                        <td>${badge}</td>
                        <td style="white-space:nowrap;color:#374151">${JA.utils.escapeHtml((f.category||'').replace(/_/g,' '))}</td>
                        <td><code style="font-size:11px;color:#4f46e5">${JA.utils.escapeHtml(r.endpoint||'')}</code></td>
                        <td><code style="font-size:11px">${JA.utils.escapeHtml(f.table||'-')}</code></td>
                        <td style="max-width:340px;color:#374151">${JA.utils.escapeHtml(f.description||'')}</td>
                    </tr>`;
                }
            }
            html += '</tbody></table></div>';
        } else {
            html += '<p style="margin-top:12px;color:#6b7280">No optimization findings — flow looks clean</p>';
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
    }
};
