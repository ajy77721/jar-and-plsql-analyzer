/**
 * Flow Analysis tab — test-data preparation & optimization report.
 *
 * Tabs: "Run Analysis" | "History"
 * History: cards per completed run, filter by mode, "View Details" opens modal.
 * Claude logs: collapsed by default everywhere (click to expand).
 */
window.JA = window.JA || {};
JA.flowAnalysis = {

    _currentRunId:    null,
    _pollTimer:       null,
    _claudeAvailable: null,
    _mode:            'testdata',
    _activeView:      'run',      // 'run' | 'history'
    _historyFilter:   'all',      // 'all' | 'optimize' | 'testdata'

    // ── Entry point called by summary.js lazy renderer ───────────────────────

    _renderFlowTab(analysis) {
        const endpoints = (analysis && analysis.endpoints) || [];

        let epRows = '';
        for (const ep of endpoints) {
            const method  = (ep.httpMethod || 'GET').toUpperCase();
            const path    = ep.fullPath || ep.path || ep.endpointName || '';
            const safe    = JA.utils.escapeHtml(path);
            const mCls    = method === 'GET' ? 'var(--blue)' : method === 'POST' ? 'var(--green)'
                          : method === 'PUT' ? 'var(--orange)' : method === 'DELETE' ? 'var(--red)' : 'var(--bg-inset)';
            const fullKey = JA.utils.escapeHtml(method + ' ' + path);
            epRows += `<label class="flow-ep-row">
                <input type="checkbox" class="flow-ep-cb" value="${fullKey}" checked>
                <span style="color:${mCls};font-weight:600;min-width:52px;display:inline-block;font-size:11px">${JA.utils.escapeHtml(method)}</span>
                <code style="font-size:12px">${safe}</code>
            </label>`;
        }
        if (!epRows) epRows = '<p style="color:var(--text-muted);padding:8px 0">No endpoints found in this analysis</p>';

        return `
<div class="flow-tab-wrap" style="padding:20px">

  <!-- Tab bar -->
  <div style="display:flex;gap:0;margin-bottom:20px;border-bottom:2px solid var(--border)">
    <button id="flow-tab-run" onclick="JA.flowAnalysis._switchView('run')"
            style="background:none;border:none;border-bottom:3px solid var(--accent-hover);padding:8px 20px;font-size:13px;font-weight:700;color:var(--accent-hover);cursor:pointer;margin-bottom:-2px">
      ▶ Run Analysis
    </button>
    <button id="flow-tab-history" onclick="JA.flowAnalysis._switchView('history')"
            style="background:none;border:none;border-bottom:3px solid transparent;padding:8px 20px;font-size:13px;font-weight:600;color:var(--text-muted);cursor:pointer;margin-bottom:-2px">
      🕐 History
    </button>
  </div>

  <!-- ─── Run Analysis panel ─────────────────────────────── -->
  <div id="flow-run-panel">

    <!-- Mode cards -->
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px">
      <div style="flex:1;min-width:200px">
        <div class="flow-action-card" onclick="JA.flowAnalysis._setMode('testdata')" id="flow-card-testdata"
             style="border:2px solid var(--accent-hover);border-radius:var(--radius-md);padding:16px;cursor:pointer;background:var(--accent-subtle)">
          <div style="font-weight:700;color:var(--accent-hover);font-size:14px;margin-bottom:4px">Prepare Test Data</div>
          <div style="font-size:12px;color:var(--text-muted)">Walk call tree, fetch real MongoDB records, build parameterised test datasets</div>
        </div>
      </div>
      <div style="flex:1;min-width:200px">
        <div class="flow-action-card" onclick="JA.flowAnalysis._setMode('optimize')" id="flow-card-optimize"
             style="border:2px solid var(--border);border-radius:var(--radius-md);padding:16px;cursor:pointer;background:var(--bg-card)">
          <div style="font-weight:700;color:var(--text-secondary);font-size:14px;margin-bottom:4px">Run Optimization Analysis</div>
          <div style="font-size:12px;color:var(--text-muted)">Detect N+1, missing indexes, bulk ops, aggregation rewrites · Claude AI insights per node</div>
        </div>
      </div>
    </div>

    <!-- Claude banner -->
    <div id="flow-claude-status-banner" style="margin-bottom:12px"></div>

    <!-- Endpoint list -->
    <div class="flow-card">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
        <b style="font-size:13px">Endpoints to analyze</b>
        <span style="font-size:12px;color:var(--text-muted)">
          <a href="#" onclick="JA.flowAnalysis._selectAll(true);return false" style="color:var(--accent-hover);text-decoration:none">Select all</a>
          &nbsp;·&nbsp;
          <a href="#" onclick="JA.flowAnalysis._selectAll(false);return false" style="color:var(--accent-hover);text-decoration:none">Deselect all</a>
          &nbsp;&nbsp;
          <span id="flow-ep-count" style="color:var(--text-secondary);font-weight:600">${endpoints.length} selected</span>
        </span>
      </div>
      <div id="flow-ep-list" style="max-height:240px;overflow-y:auto;display:flex;flex-direction:column;gap:4px">
        ${epRows}
      </div>
    </div>

    <!-- MongoDB connection — profile selector loaded from JAR config -->
    <div id="flow-conn-section" style="margin-top:12px">
      <div id="flow-conn-loading" style="font-size:12px;color:var(--text-muted)">Loading environment profiles…</div>
    </div>

    <!-- Run button -->
    <div style="margin-top:16px;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <button id="flow-run-btn"
              style="background:var(--accent-hover);color:var(--text-inverse);border:none;padding:10px 28px;border-radius:var(--radius);font-size:14px;font-weight:600;cursor:pointer"
              onclick="JA.flowAnalysis.run()">
        ▶ Run
      </button>
      <span id="flow-mode-label" style="font-size:13px;color:var(--text-muted)">Mode: Prepare Test Data</span>
    </div>

    <!-- Results area -->
    <div id="flow-results-area" style="margin-top:18px"></div>
  </div>

  <!-- ─── History panel ──────────────────────────────────── -->
  <div id="flow-history-panel" style="display:none">

    <!-- Filter sub-tabs -->
    <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center">
      <span style="font-size:12px;color:var(--text-muted);font-weight:600">Filter:</span>
      <button id="flow-hf-all"      onclick="JA.flowAnalysis._setHistoryFilter('all')"
              class="flow-hf-btn flow-hf-active">All</button>
      <button id="flow-hf-optimize" onclick="JA.flowAnalysis._setHistoryFilter('optimize')"
              class="flow-hf-btn">Optimize</button>
      <button id="flow-hf-testdata" onclick="JA.flowAnalysis._setHistoryFilter('testdata')"
              class="flow-hf-btn">Test Data</button>
      <button onclick="JA.flowAnalysis._loadHistory()"
              style="margin-left:auto;background:var(--bg-card);color:var(--accent-hover);border:1px solid var(--accent-hover);padding:5px 14px;border-radius:var(--radius);font-size:12px;cursor:pointer;font-weight:600">
        ↻ Refresh
      </button>
    </div>

    <div id="flow-history-list"></div>
  </div>

</div>

<style>
.flow-card{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius-md);padding:16px;box-shadow: var(--shadow-sm)}
.flow-field-row{display:flex;gap:14px;flex-wrap:wrap}
.flow-field{flex:1;min-width:180px}
.flow-field label{display:block;font-size:12px;font-weight:600;color:var(--text-secondary);margin-bottom:4px}
.flow-ep-row{display:flex;align-items:center;gap:8px;padding:4px 6px;border-radius:var(--radius-sm);cursor:pointer;font-size:13px}
.flow-ep-row:hover{background:var(--bg)}
.flow-ep-cb{cursor:pointer;accent-color:var(--accent-hover)}
.flow-badge-high{background:var(--badge-red-bg);color:var(--red);padding:2px 7px;border-radius:var(--radius-lg);font-size:11px;font-weight:600}
.flow-badge-med{background:var(--badge-orange-bg);color:var(--badge-orange);padding:2px 7px;border-radius:var(--radius-lg);font-size:11px;font-weight:600}
.flow-badge-low{background:var(--badge-green-bg);color:var(--badge-green);padding:2px 7px;border-radius:var(--radius-lg);font-size:11px;font-weight:600}
.flow-badge-opt{background:var(--badge-green-bg);color:var(--badge-green);border:1px solid var(--badge-green-bg);padding:2px 9px;border-radius:var(--radius-lg);font-size:11px;font-weight:600}
.flow-badge-td{background:var(--accent-subtle);color:var(--accent-hover);border:1px solid var(--accent-subtle);padding:2px 9px;border-radius:var(--radius-lg);font-size:11px;font-weight:600}
.flow-spin{display:inline-block;width:13px;height:13px;border:2px solid var(--border);border-top-color:var(--accent-hover);border-radius:50%;animation:flow-spin .7s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes flow-spin{to{transform:rotate(360deg)}}
.flow-results-tbl{width:100%;border-collapse:collapse;font-size:13px}
.flow-results-tbl th,.flow-results-tbl td{padding:8px 12px;text-align:left;border-bottom:1px solid var(--border-subtle)}
.flow-results-tbl th{background:var(--bg);font-weight:600;color:var(--text-secondary)}
.flow-progress-log{max-height:180px;overflow-y:auto;font-size:12px;font-family:monospace;line-height:1.7;color:var(--text-secondary)}
.flow-claude-log-card{border:1px solid var(--accent-subtle);border-radius:var(--radius);margin-bottom:10px;overflow:hidden}
.flow-claude-log-hdr{background:var(--accent-subtle);padding:8px 14px;display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none}
.flow-hf-btn{background:var(--bg-card);color:var(--text-muted);border:1px solid var(--border);padding:4px 14px;border-radius:var(--radius);font-size:12px;cursor:pointer;font-weight:600}
.flow-hf-active{background:var(--accent-hover);color:var(--text-inverse);border-color:var(--accent-hover)}
.flow-hist-card{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius-md);padding:14px 18px;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;box-shadow: var(--shadow-sm)}
.flow-hist-card:hover{border-color:var(--accent-subtle);box-shadow:0 2px 8px var(--focus-ring)}
/* Modal overlay */
.flow-modal-overlay{position:fixed;top:0;left:0;right:0;bottom:0;background:var(--bg-overlay);z-index:9999;display:flex;align-items:flex-start;justify-content:center;padding:32px 16px;overflow-y:auto}
.flow-modal-box{background:var(--bg-card);border-radius:var(--radius-lg);width:100%;max-width:1000px;box-shadow: var(--shadow-lg);position:relative}
.flow-modal-hdr{display:flex;align-items:center;justify-content:space-between;padding:16px 24px;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--bg-card);z-index:1;border-radius:10px 10px 0 0}
.flow-modal-body{padding:20px 24px}
</style>`;
    },

    // ── Init ──────────────────────────────────────────────────────────────────

    _envProfiles: [],

    _init() {
        const list = document.getElementById('flow-ep-list');
        if (list) list.addEventListener('change', () => this._updateCount());
        this._setMode('testdata');
        this._checkClaudeStatus();
        this._loadEnvProfiles();
    },

    async _loadEnvProfiles() {
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        const section = document.getElementById('flow-conn-section');
        if (!section) return;
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/env-profiles`);
            if (!resp.ok) throw new Error(await resp.text() || resp.status + ' from ' + resp.url);
            const profiles = await resp.json();
            this._envProfiles = profiles || [];
            this._renderConnSection(section);
        } catch (e) {
            this._renderConnSection(section); // fallback to manual entry
        }
    },

    _renderConnSection(section) {
        const profiles = this._envProfiles;

        if (!profiles || profiles.length === 0) {
            // No profiles found — show plain manual input + test button
            section.innerHTML = `
              <div class="flow-card" style="border-color:var(--border);padding:12px 16px">
                <div style="font-size:12px;font-weight:600;color:var(--text-secondary);margin-bottom:8px">MongoDB Connection</div>
                <div class="flow-field-row">
                  <div class="flow-field">
                    <label>URI</label>
                    <input id="flow-mongo-uri" type="text" placeholder="mongodb://localhost:27017"
                           oninput="JA.flowAnalysis._onManualUriChange()"
                           style="width:100%;padding:6px 10px;border:1px solid var(--border);border-radius:var(--radius-sm);font-size:13px;box-sizing:border-box">
                  </div>
                  <div class="flow-field">
                    <label>Database</label>
                    <input id="flow-mongo-db" type="text" placeholder="mydb"
                           style="width:100%;padding:6px 10px;border:1px solid var(--border);border-radius:var(--radius-sm);font-size:13px;box-sizing:border-box">
                  </div>
                </div>
                <div style="margin-top:10px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
                  <button onclick="JA.flowAnalysis._testConnection()" id="flow-test-conn-btn"
                          style="background:var(--blue);color:var(--text-inverse);border:none;padding:6px 18px;border-radius:var(--radius);font-size:12px;font-weight:600;cursor:pointer">
                    ⚡ Test Connection
                  </button>
                  <span id="flow-conn-result" style="font-size:12px"></span>
                </div>
              </div>`;
            return;
        }

        // Build profile options
        const opts = profiles.map((p, i) =>
            `<option value="${i}">${JA.utils.escapeHtml(p.profileName)}${p.resolvedUri ? '' : ' ⚠ credentials required'}</option>`
        ).join('');

        // Render with profile dropdown + dynamic credential fields + test button
        section.innerHTML = `
          <div class="flow-card" style="border-color:var(--accent-subtle);padding:12px 16px;background:var(--badge-blue-bg)">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
              <div style="font-size:12px;font-weight:700;color:var(--accent-hover)">🔗 MongoDB Environment</div>
              <span style="font-size:11px;color:var(--text-muted)">${profiles.length} profile${profiles.length !== 1 ? 's' : ''} found in JAR config</span>
            </div>
            <div class="flow-field-row" style="align-items:flex-end">
              <div class="flow-field" style="flex:0 0 auto;min-width:160px">
                <label>Environment Profile</label>
                <select id="flow-profile-select"
                        style="width:100%;padding:6px 10px;border:1px solid var(--accent-subtle);border-radius:var(--radius-sm);font-size:13px;background:var(--bg-card)"
                        onchange="JA.flowAnalysis._onProfileChange()">
                  ${opts}
                </select>
              </div>
              <div id="flow-cred-fields" style="flex:1;display:flex;gap:10px;flex-wrap:wrap"></div>
            </div>
            <div style="margin-top:10px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
              <div id="flow-uri-preview" style="flex:1;font-size:11px;color:var(--text-muted);font-family:monospace;word-break:break-all"></div>
              <button onclick="JA.flowAnalysis._testConnection()" id="flow-test-conn-btn"
                      style="flex:0 0 auto;background:var(--blue);color:var(--text-inverse);border:none;padding:6px 18px;border-radius:var(--radius);font-size:12px;font-weight:600;cursor:pointer">
                ⚡ Test Connection
              </button>
            </div>
            <div id="flow-conn-result" style="margin-top:6px;font-size:12px"></div>
            <input type="hidden" id="flow-mongo-uri">
            <input type="hidden" id="flow-mongo-db">
          </div>`;

        this._onProfileChange();
    },

    _onProfileChange() {
        const sel = document.getElementById('flow-profile-select');
        if (!sel) return;
        const idx = parseInt(sel.value) || 0;
        const profile = this._envProfiles[idx];
        if (!profile) return;

        const credDiv = document.getElementById('flow-cred-fields');
        const preview = document.getElementById('flow-uri-preview');
        const uriInput = document.getElementById('flow-mongo-uri');
        const dbInput  = document.getElementById('flow-mongo-db');

        if (profile.resolvedUri) {
            // No credentials needed — URI is ready
            if (credDiv) credDiv.innerHTML = `<div style="font-size:12px;color:var(--badge-green);padding:6px 0;font-weight:600">✓ No credentials required</div>`;
            if (preview)   preview.textContent = profile.resolvedUri;
            if (uriInput)  uriInput.value = profile.resolvedUri;
            if (dbInput)   dbInput.value  = profile.database || '';
        } else {
            // Render input for each required variable
            const vars = profile.requiredVars || [];
            let fields = '';
            for (const v of vars) {
                const isPassword = v.toLowerCase().includes('password') || v.toLowerCase().includes('pwd');
                fields += `<div class="flow-field">
                  <label>${JA.utils.escapeHtml(v)}</label>
                  <input type="${isPassword ? 'password' : 'text'}"
                         id="flow-cred-${JA.utils.escapeHtml(v)}"
                         placeholder="${JA.utils.escapeHtml(v)}"
                         oninput="JA.flowAnalysis._resolveUri()"
                         style="width:100%;padding:6px 10px;border:1px solid var(--border);border-radius:var(--radius-sm);font-size:13px;box-sizing:border-box">
                </div>`;
            }
            if (credDiv) credDiv.innerHTML = fields;
            if (preview)  preview.textContent = profile.uriTemplate;
            if (uriInput) uriInput.value = '';
            if (dbInput)  dbInput.value  = profile.database || '';
        }
    },

    _resolveUri() {
        const sel = document.getElementById('flow-profile-select');
        if (!sel) return;
        const idx = parseInt(sel.value) || 0;
        const profile = this._envProfiles[idx];
        if (!profile) return;

        const vars = profile.requiredVars || [];
        let uri = profile.uriTemplate;
        let allFilled = true;
        for (const v of vars) {
            const input = document.getElementById('flow-cred-' + v);
            const val = input ? input.value.trim() : '';
            if (!val) { allFilled = false; }
            // Replace ${VAR} and ${VAR:default} patterns
            uri = uri.replace(new RegExp('\\$\\{' + v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '(?::[^}]*)?\\}', 'g'), val || ('${' + v + '}'));
        }

        const preview = document.getElementById('flow-uri-preview');
        const uriInput = document.getElementById('flow-mongo-uri');
        const dbInput  = document.getElementById('flow-mongo-db');

        if (preview)  preview.textContent = uri;
        if (uriInput) uriInput.value = allFilled ? uri : '';
        if (dbInput)  dbInput.value  = profile.database || '';
    },

    // ── Tab switching ─────────────────────────────────────────────────────────

    _switchView(view) {
        this._activeView = view;
        const runPanel  = document.getElementById('flow-run-panel');
        const histPanel = document.getElementById('flow-history-panel');
        const tabRun    = document.getElementById('flow-tab-run');
        const tabHist   = document.getElementById('flow-tab-history');

        if (view === 'history') {
            if (runPanel)  runPanel.style.display  = 'none';
            if (histPanel) histPanel.style.display = '';
            if (tabRun)  { tabRun.style.borderBottomColor  = 'transparent'; tabRun.style.color  = 'var(--text-muted)'; tabRun.style.fontWeight = '600'; }
            if (tabHist) { tabHist.style.borderBottomColor = 'var(--accent-hover)';     tabHist.style.color = 'var(--accent-hover)'; tabHist.style.fontWeight = '700'; }
            this._loadHistory();
        } else {
            if (runPanel)  runPanel.style.display  = '';
            if (histPanel) histPanel.style.display = 'none';
            if (tabRun)  { tabRun.style.borderBottomColor  = 'var(--accent-hover)'; tabRun.style.color  = 'var(--accent-hover)'; tabRun.style.fontWeight = '700'; }
            if (tabHist) { tabHist.style.borderBottomColor = 'transparent'; tabHist.style.color = 'var(--text-muted)'; tabHist.style.fontWeight = '600'; }
        }
    },

    // ── History ───────────────────────────────────────────────────────────────

    _setHistoryFilter(f) {
        this._historyFilter = f;
        ['all','optimize','testdata'].forEach(k => {
            const btn = document.getElementById('flow-hf-' + k);
            if (btn) {
                btn.classList.toggle('flow-hf-active', k === f);
            }
        });
        this._loadHistory();
    },

    async _loadHistory() {
        const jarId = JA.app.currentJarId;
        if (!jarId) return;
        const listEl = document.getElementById('flow-history-list');
        if (!listEl) return;
        listEl.innerHTML = `<div style="color:var(--text-muted);font-size:13px;padding:12px 0"><span class="flow-spin"></span> Loading history…</div>`;
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/history`);
            if (!resp.ok) throw new Error(await resp.text() || resp.status + ' from ' + resp.url);
            const entries = await resp.json();
            this._renderHistoryList(listEl, entries, jarId);
        } catch (e) {
            listEl.innerHTML = `<div style="color:var(--red);font-size:13px">Failed to load history: ${JA.utils.escapeHtml(e.message)}</div>`;
        }
    },

    _renderHistoryList(listEl, entries, jarId) {
        const filter = this._historyFilter;
        const filtered = filter === 'all' ? entries : entries.filter(e => e.mode === filter);

        if (filtered.length === 0) {
            listEl.innerHTML = `<div style="color:var(--text-muted);font-size:13px;padding:20px 0;text-align:center">
                No ${filter === 'all' ? '' : filter + ' '}runs yet for this JAR.
            </div>`;
            return;
        }

        let html = '';
        for (const e of filtered) {
            const isOpt = e.mode === 'optimize';
            const modeBadge = isOpt
                ? '<span class="flow-badge-opt">Optimize</span>'
                : '<span class="flow-badge-td">Test Data</span>';

            const ts = new Date(e.timestamp);
            const dateStr = ts.toLocaleDateString() + ' ' + ts.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});

            const epList = (e.endpoints || []).slice(0, 3).map(ep => `<code style="font-size:11px;color:var(--accent-hover)">${JA.utils.escapeHtml(ep)}</code>`).join(' ');
            const epMore = e.endpoints.length > 3 ? ` <span style="font-size:11px;color:var(--text-muted)">+${e.endpoints.length-3} more</span>` : '';

            const statsHtml = isOpt
                ? `<span class="flow-badge-high" style="font-size:11px">${e.highCount} HIGH</span>
                   <span class="flow-badge-med" style="font-size:11px;margin-left:4px">${e.medCount} MED</span>
                   <span class="flow-badge-low" style="font-size:11px;margin-left:4px">${e.lowCount} LOW</span>`
                : `<span style="font-size:12px;color:var(--accent-hover);font-weight:600">${e.datasetCount} dataset${e.datasetCount!==1?'s':''}</span>`;

            html += `<div class="flow-hist-card">
                <div style="flex:1;min-width:200px">
                    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:6px">
                        ${modeBadge}
                        <span style="font-size:12px;color:var(--text-muted)">${dateStr}</span>
                    </div>
                    <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:6px">${epList}${epMore}</div>
                    <div style="display:flex;gap:6px;align-items:center">${statsHtml}</div>
                </div>
                <button onclick="JA.flowAnalysis._openHistoryDetail('${encodeURIComponent(jarId)}','${e.runId}')"
                        style="background:var(--accent-hover);color:var(--text-inverse);border:none;padding:8px 18px;border-radius:var(--radius);font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap">
                    View Details
                </button>
            </div>`;
        }
        listEl.innerHTML = html;
    },

    async _openHistoryDetail(encodedJarId, runId) {
        const jarId = decodeURIComponent(encodedJarId);
        // Show loading modal
        this._showModal(`<div class="flow-modal-hdr">
            <span style="font-weight:700;font-size:15px">Loading…</span>
            <button onclick="JA.flowAnalysis._closeModal()" style="background:none;border:none;font-size:20px;cursor:pointer;color:var(--text-muted);line-height:1">×</button>
        </div>
        <div class="flow-modal-body"><span class="flow-spin"></span> Fetching run details…</div>`);

        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/status/${runId}`);
            if (!resp.ok) throw new Error(await resp.text() || resp.status + ' from ' + resp.url);
            const data = await resp.json();

            const modeLabel = data.mode === 'testdata' ? 'Test Data' : 'Optimization';
            const ts = new Date();
            const epLabel = (data.log || []).length > 0 ? '' : '';

            const bodyHtml = this._buildResultsHtml(jarId, runId, data, { collapseLogs: true, idPrefix: 'modal-' + runId + '-' });

            this._showModal(`
                <div class="flow-modal-hdr">
                    <div>
                        <span style="font-weight:700;font-size:15px">Run #${runId} — Details</span>
                    </div>
                    <div style="display:flex;gap:8px;align-items:center">
                        <button onclick="JA.flowAnalysis._download('${encodeURIComponent(jarId)}','${runId}')"
                                style="background:var(--blue);color:var(--text-inverse);border:none;padding:6px 14px;border-radius:var(--radius);font-size:12px;cursor:pointer;font-weight:600">
                            ⬇ Download .md
                        </button>
                        <button onclick="JA.flowAnalysis._closeModal()" style="background:none;border:none;font-size:22px;cursor:pointer;color:var(--text-muted);line-height:1;padding:0 4px">×</button>
                    </div>
                </div>
                <div class="flow-modal-body">${bodyHtml}</div>
            `);
        } catch (e) {
            this._showModal(`<div class="flow-modal-hdr">
                <span style="color:var(--red)">Error loading details</span>
                <button onclick="JA.flowAnalysis._closeModal()" style="background:none;border:none;font-size:20px;cursor:pointer;color:var(--text-muted)">×</button>
            </div>
            <div class="flow-modal-body" style="color:var(--red)">${JA.utils.escapeHtml(e.message)}</div>`);
        }
    },

    _showModal(html) {
        let overlay = document.getElementById('flow-modal-overlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'flow-modal-overlay';
            overlay.className = 'flow-modal-overlay';
            overlay.addEventListener('click', e => { if (e.target === overlay) this._closeModal(); });
            document.body.appendChild(overlay);
        }
        overlay.innerHTML = `<div class="flow-modal-box">${html}</div>`;
        overlay.style.display = 'flex';
        document.body.style.overflow = 'hidden';
    },

    _closeModal() {
        const overlay = document.getElementById('flow-modal-overlay');
        if (overlay) overlay.style.display = 'none';
        document.body.style.overflow = '';
    },

    // ── Claude / mode helpers ─────────────────────────────────────────────────

    async _checkClaudeStatus() {
        try {
            const resp = await fetch('/api/jar/jars/claude-status');
            if (!resp.ok) throw new Error(await resp.text() || resp.status + ' from ' + resp.url);
            const data = await resp.json();
            this._claudeAvailable = data.available;
            this._renderClaudeBanner(data.available, data.message);
        } catch (e) {
            this._claudeAvailable = false;
            this._renderClaudeBanner(false, 'Claude CLI status unknown');
        }
    },

    _renderClaudeBanner(available, message) {
        const el = document.getElementById('flow-claude-status-banner');
        if (!el) return;
        if (available) {
            el.innerHTML = `<div style="padding:8px 14px;background:var(--badge-green-bg);border:1px solid var(--badge-green-bg);border-radius:var(--radius);font-size:12px;color:var(--badge-green)">
                🤖 <b>Claude AI ready</b> — flow analysis will automatically send each call-tree node to Claude for impact analysis and fix suggestions.
            </div>`;
        } else {
            el.innerHTML = `<div style="padding:8px 14px;background:var(--badge-orange-bg);border:1px solid var(--badge-orange-bg);border-radius:var(--radius);font-size:12px;color:var(--badge-orange)">
                ⚠️ <b>Claude CLI not available.</b> Run <code>claude login</code> in your terminal to enable AI insights. Static analysis still runs.
                <span style="margin-left:6px;cursor:pointer;color:var(--accent-hover);font-weight:600" onclick="JA.flowAnalysis._checkClaudeStatus()">↻ Recheck</span>
            </div>`;
        }
    },

    _setMode(mode) {
        this._mode = mode;
        const tdCard  = document.getElementById('flow-card-testdata');
        const optCard = document.getElementById('flow-card-optimize');
        const label   = document.getElementById('flow-mode-label');
        const btn     = document.getElementById('flow-run-btn');

        if (mode === 'testdata') {
            if (tdCard)  { tdCard.style.border  = '2px solid var(--accent-hover)'; tdCard.style.background  = 'var(--accent-subtle)'; }
            if (optCard) { optCard.style.border = '2px solid var(--border)'; optCard.style.background = 'var(--bg-card)'; }
            if (label)   label.textContent = 'Mode: Prepare Test Data';
            if (btn)     btn.style.background = 'var(--accent-hover)';
        } else {
            if (tdCard)  { tdCard.style.border  = '2px solid var(--border)'; tdCard.style.background  = 'var(--bg-card)'; }
            if (optCard) { optCard.style.border = '2px solid var(--badge-green)'; optCard.style.background = 'var(--badge-green-bg)'; }
            if (label)   label.textContent = 'Mode: Run Optimization Analysis';
            if (btn)     btn.style.background = 'var(--green)';
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

    _onManualUriChange() {
        // clear previous test result when URI changes
        const r = document.getElementById('flow-conn-result');
        if (r) r.innerHTML = '';
    },

    async _testConnection() {
        const jarId = JA.app.currentJarId;
        if (!jarId) return;

        const uriInput = document.getElementById('flow-mongo-uri');
        const uri = uriInput ? uriInput.value.trim() : '';
        if (!uri) {
            const r = document.getElementById('flow-conn-result');
            if (r) r.innerHTML = '<span style="color:var(--red)">⚠ Enter or resolve a URI first</span>';
            return;
        }

        const btn = document.getElementById('flow-test-conn-btn');
        const result = document.getElementById('flow-conn-result');
        if (btn)    { btn.disabled = true; btn.innerHTML = '<span class="flow-spin"></span>Testing…'; }
        if (result) result.innerHTML = '';

        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/test-connection`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mongoUri: uri })
            });
            if (!resp.ok) throw new Error(await resp.text() || resp.status + ' from ' + resp.url);
            const data = await resp.json();

            if (data.success) {
                let msg = `<span style="color:var(--badge-green);font-weight:600">✓ Connected</span>`
                    + ` &nbsp;<span style="color:var(--text-muted)">${data.latencyMs}ms · DB: <b>${JA.utils.escapeHtml(data.database || '')}</b>`
                    + ` · ${data.collectionCount} collection${data.collectionCount !== 1 ? 's' : ''}</span>`;
                if (data.sampleCollections && data.sampleCollections.length) {
                    msg += `<br><span style="color:var(--text-muted);font-size:11px">Sample: ${data.sampleCollections.map(c => JA.utils.escapeHtml(c)).join(', ')}</span>`;
                }
                if (result) result.innerHTML = msg;
            } else {
                if (result) result.innerHTML = `<span style="color:var(--red);font-weight:600">✗ Failed</span>`
                    + ` <span style="color:var(--text-muted)">${JA.utils.escapeHtml(data.message || 'Connection refused')}</span>`;
            }
        } catch (e) {
            if (result) result.innerHTML = `<span style="color:var(--red)">✗ ${JA.utils.escapeHtml(e.message)}</span>`;
        } finally {
            if (btn) { btn.disabled = false; btn.innerHTML = '⚡ Test Connection'; }
        }
    },

    // ── Run ───────────────────────────────────────────────────────────────────

    async run() {
        const jarId = JA.app.currentJarId;
        if (!jarId) { alert('No JAR loaded'); return; }

        const selectedPaths = Array.from(document.querySelectorAll('.flow-ep-cb:checked')).map(cb => cb.value);
        if (selectedPaths.length === 0) { alert('Select at least one endpoint'); return; }

        const mongoUri = (document.getElementById('flow-mongo-uri') || {}).value || '';
        const mongoDb  = (document.getElementById('flow-mongo-db')  || {}).value || '';

        if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }

        const btn = document.getElementById('flow-run-btn');
        if (btn) { btn.disabled = true; btn.innerHTML = '<span class="flow-spin"></span>Starting…'; }

        const area = document.getElementById('flow-results-area');
        if (area) area.innerHTML = this._progressHtml({ status: 'running', phase: 'Submitting…', log: [] });

        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/run`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mode: this._mode, endpointPaths: selectedPaths, mongoUri, mongoDb, claudeApiKey: '' })
            });
            if (!resp.ok) throw new Error(await resp.text() || resp.status + ' from ' + resp.url);
            const start = await resp.json();
            if (start.error) throw new Error(start.error);
            this._currentRunId = start.runId;
            this._schedulePoll(jarId, start.runId);
        } catch (e) {
            if (btn) { btn.disabled = false; btn.innerHTML = '▶ Run'; }
            if (area) area.innerHTML = `<div class="flow-card" style="color:var(--red)">Error: ${JA.utils.escapeHtml(e.message)}</div>`;
        }
    },

    // ── Polling ───────────────────────────────────────────────────────────────

    _schedulePoll(jarId, runId) {
        // Interval from PollConfig, not a literal: this is a job poller and must move with the
        // configured cadence like every other one.
        this._pollTimer = setTimeout(() => this._poll(jarId, runId),
            (window.PollConfig && PollConfig.jobStatusMs) || 1500);
    },

    async _poll(jarId, runId) {
        try {
            const resp = await fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/status/${runId}`);
            if (!resp.ok) throw new Error(await resp.text() || resp.status + ' from ' + resp.url);
            const data = await resp.json();

            const area = document.getElementById('flow-results-area');
            const btn  = document.getElementById('flow-run-btn');

            if (data.status === 'running') {
                if (area) area.innerHTML = this._progressHtml(data);
                this._schedulePoll(jarId, runId);
                return;
            }

            if (btn) { btn.disabled = false; btn.innerHTML = '▶ Run'; }
            if (!area) return;

            if (data.status === 'error') {
                area.innerHTML = `<div class="flow-card" style="border-color:var(--badge-red-bg);color:var(--red)">
                    <b>Analysis failed:</b> ${JA.utils.escapeHtml(data.error || 'unknown error')}
                </div>`;
                return;
            }

            // Render results inline (logs collapsed by default)
            area.innerHTML = this._buildResultsHtml(jarId, runId, data, { collapseLogs: true });

        } catch (e) {
            const btn = document.getElementById('flow-run-btn');
            if (btn) { btn.disabled = false; btn.innerHTML = '▶ Run'; }
        }
    },

    // ── Live progress ─────────────────────────────────────────────────────────

    _progressHtml(data) {
        const total  = data.totalEndpoints   || 0;
        const done   = data.doneEndpoints    || 0;
        const cTotal = data.totalClaudeNodes || 0;
        const cDone  = data.doneClaudeNodes  || 0;
        const phase  = data.phase || 'Running…';
        const log    = data.log   || [];

        const epBar = total > 0 ? `<div style="margin-top:10px">
            <div style="font-size:12px;color:var(--text-muted);margin-bottom:4px">Endpoints: <b>${done} / ${total}</b></div>
            <div style="background:var(--bg-hover);border-radius:var(--radius-sm);height:8px;overflow:hidden">
              <div style="background:var(--accent-hover);height:8px;width:${Math.round((done/total)*100)}%;transition:width .4s"></div>
            </div></div>` : '';

        const cBar = cTotal > 0 ? `<div style="margin-top:8px">
            <div style="font-size:12px;color:var(--text-muted);margin-bottom:4px">
              🤖 Claude nodes: <b>${cDone} / ${cTotal}</b>
              ${data.currentClaudeNode ? `<span style="color:var(--accent-hover)"> — <code style="font-size:11px">${JA.utils.escapeHtml(data.currentClaudeNode)}</code></span>` : ''}
            </div>
            <div style="background:var(--bg-hover);border-radius:var(--radius-sm);height:8px;overflow:hidden">
              <div style="background:var(--accent-hover);height:8px;width:${Math.round((cDone/cTotal)*100)}%;transition:width .4s"></div>
            </div></div>` : '';

        const logHtml = log.length > 0 ? `<div style="margin-top:12px">
            <div style="font-size:11px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">Live log</div>
            <div class="flow-progress-log" id="flow-progress-log">
              ${log.slice(-30).map(l => `<div>${JA.utils.escapeHtml(l)}</div>`).join('')}
            </div></div>` : '';

        return `<div class="flow-card">
            <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px">
                <div>
                    <span class="flow-spin"></span>
                    <b style="font-size:14px">Analysis running</b>
                    <span style="margin-left:10px;font-size:13px;color:var(--text-muted)">${JA.utils.escapeHtml(phase)}</span>
                </div>
                <button onclick="JA.flowAnalysis._manualRefresh()"
                        style="background:var(--bg-card);color:var(--accent-hover);border:1px solid var(--accent-hover);padding:6px 14px;border-radius:var(--radius);font-size:12px;cursor:pointer;font-weight:600">
                    ↻ Refresh
                </button>
            </div>
            ${epBar}${cBar}${logHtml}
        </div>`;
    },

    _manualRefresh() {
        const jarId = JA.app.currentJarId;
        if (!jarId || !this._currentRunId) return;
        if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
        this._poll(jarId, this._currentRunId);
    },

    // ── Results builder (shared by inline view and history modal) ─────────────
    // opts.collapseLogs  = true  → Claude log bodies start hidden
    // opts.idPrefix      = str   → prefix all generated IDs (avoids conflicts in modal)

    _buildResultsHtml(jarId, runId, data, opts = {}) {
        const collapseLogs = opts.collapseLogs !== false ? true : false;
        const pfx = opts.idPrefix || '';

        const results = data.results || [];
        let high = 0, med = 0, low = 0;
        for (const r of results) for (const f of (r.optimizations || [])) {
            if (f.severity === 'HIGH') high++;
            else if (f.severity === 'MEDIUM') med++;
            else low++;
        }

        let claudeEnriched = 0;
        for (const r of results) for (const f of (r.optimizations||[])) if (f.claudeInsight) claudeEnriched++;

        const allLogs = results.flatMap(r => (r.claudeLogs||[]).map(l => ({endpoint: r.endpoint, ...l})));
        const claudeRan = allLogs.length > 0;
        const runLog = data.log || [];

        // ── Header card ──
        let html = `<div class="flow-card">
            <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-bottom:4px">
                <div>
                    <b style="font-size:15px">Analysis complete</b>
                    <span style="margin-left:12px;font-size:13px">
                        <b>${results.length}</b> endpoint${results.length!==1?'s':''} &nbsp;
                        <span class="flow-badge-high">${high} HIGH</span>&nbsp;
                        <span class="flow-badge-med">${med} MEDIUM</span>&nbsp;
                        <span class="flow-badge-low">${low} LOW</span>
                    </span>
                    ${claudeRan
                        ? `<span style="margin-left:10px;font-size:12px;color:var(--accent-hover)">🤖 <b>${claudeEnriched}</b> insights · <b>${allLogs.length}</b> node${allLogs.length!==1?'s':''}</span>`
                        : `<span style="margin-left:10px;font-size:12px;color:var(--text-muted)">🤖 Claude not active</span>`}
                </div>
                <div style="display:flex;gap:8px">
                    ${runLog.length > 0 ? `<button onclick="JA.flowAnalysis._toggleEl('${pfx}flow-run-log-panel')"
                            style="background:var(--bg-card);color:var(--text-muted);border:1px solid var(--border);padding:6px 14px;border-radius:var(--radius);font-size:12px;cursor:pointer">
                        📋 Progress log
                    </button>` : ''}
                    ${!opts.idPrefix ? `<button onclick="JA.flowAnalysis._download('${encodeURIComponent(jarId)}','${runId}')"
                            style="background:var(--blue);color:var(--text-inverse);border:none;padding:7px 16px;border-radius:var(--radius);font-size:13px;cursor:pointer;font-weight:600">
                        ⬇ Download .md Report
                    </button>` : ''}
                </div>
            </div>`;

        if (runLog.length > 0) {
            html += `<div id="${pfx}flow-run-log-panel" style="display:none;margin-top:10px;border-top:1px solid var(--border-subtle);padding-top:10px">
                <div class="flow-progress-log">${runLog.map(l => `<div>${JA.utils.escapeHtml(l)}</div>`).join('')}</div>
            </div>`;
        }
        html += '</div>';

        // ── Findings table ──
        if (high + med + low > 0) {
            html += `<div class="flow-card" style="margin-top:12px;overflow-x:auto">
                <div style="font-weight:700;font-size:13px;margin-bottom:10px">Findings</div>`;
            if (claudeEnriched > 0) {
                html += `<div style="margin-bottom:8px;font-size:12px;color:var(--accent-hover)">
                    🤖 <b>${claudeEnriched}</b> finding${claudeEnriched>1?'s':''} have Claude AI insights — click a row to expand.
                </div>`;
            }
            html += `<table class="flow-results-tbl"><thead><tr>
                <th>Severity</th><th>Category</th><th>Endpoint</th><th>Collection</th><th>Description</th>
            </tr></thead><tbody>`;

            let rowIdx = 0;
            for (const r of results) {
                for (const f of (r.optimizations || [])) {
                    const badge = f.severity === 'HIGH' ? '<span class="flow-badge-high">HIGH</span>'
                                : f.severity === 'MEDIUM' ? '<span class="flow-badge-med">MEDIUM</span>'
                                : '<span class="flow-badge-low">LOW</span>';
                    const hasInsight = !!f.claudeInsight;
                    const rid = pfx + 'flow-ci-' + rowIdx++;
                    const descCell = hasInsight
                        ? `<span style="cursor:pointer" onclick="JA.flowAnalysis._toggleEl('${rid}')">${JA.utils.escapeHtml(f.description||'')} <span style="color:var(--accent-hover);font-size:11px">🤖 ▸</span></span>`
                        : JA.utils.escapeHtml(f.description||'');
                    html += `<tr style="cursor:${hasInsight?'pointer':'default'}" onclick="${hasInsight?`JA.flowAnalysis._toggleEl('${rid}')`:''}" title="${hasInsight?'Click to see Claude insight':''}">
                        <td>${badge}</td>
                        <td style="white-space:nowrap">${JA.utils.escapeHtml((f.category||'').replace(/_/g,' '))}</td>
                        <td><code style="font-size:11px;color:var(--accent-hover)">${JA.utils.escapeHtml(r.endpoint||'')}</code></td>
                        <td><code style="font-size:11px">${JA.utils.escapeHtml(f.table||'-')}</code></td>
                        <td style="max-width:360px">${descCell}</td>
                    </tr>`;
                    if (hasInsight) {
                        const lines = f.claudeInsight.split('\n').map(l => JA.utils.escapeHtml(l)).join('<br>');
                        html += `<tr id="${rid}" style="display:none;background:var(--accent-subtle)">
                            <td colspan="5" style="padding:10px 16px;border-left:3px solid var(--accent-hover)">
                                <span style="font-size:11px;font-weight:700;color:var(--accent-hover);text-transform:uppercase;letter-spacing:.05em">🤖 Claude AI Insight</span><br>
                                <span style="font-size:12px;color:var(--text-secondary);line-height:1.6">${lines}</span>
                            </td>
                        </tr>`;
                    }
                }
            }
            html += '</tbody></table></div>';
        }

        // ── Claude call logs ── collapsed by default
        if (allLogs.length > 0) {
            html += `<div class="flow-card" style="margin-top:12px">
                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
                    <div>
                        <span style="font-weight:700;font-size:13px;color:var(--accent-hover)">🤖 Claude Analysis Logs</span>
                        <span style="font-size:12px;color:var(--text-muted);margin-left:8px">${allLogs.length} node${allLogs.length!==1?'s':''} — click a card to expand</span>
                    </div>
                    <div style="display:flex;gap:6px">
                        <button onclick="JA.flowAnalysis._expandAllLogs('${pfx}')"
                                style="background:var(--bg-card);color:var(--accent-hover);border:1px solid var(--accent-subtle);padding:5px 12px;border-radius:var(--radius-sm);font-size:12px;cursor:pointer">
                            ▼ Expand all
                        </button>
                        <button onclick="JA.flowAnalysis._collapseAllLogs('${pfx}')"
                                style="background:var(--accent-subtle);color:var(--accent-hover);border:1px solid var(--accent-subtle);padding:5px 12px;border-radius:var(--radius-sm);font-size:12px;cursor:pointer">
                            ▲ Collapse all
                        </button>
                    </div>
                </div>`;

            for (let i = 0; i < allLogs.length; i++) {
                const log   = allLogs[i];
                const lid   = pfx + 'flow-log-body-' + i;
                const nodeShort = (log.node || '?').replace(/.*\.(.*\..*)$/, '$1');
                const bodyStyle = collapseLogs ? 'display:none' : '';

                html += `<div class="flow-claude-log-card">
                    <div class="flow-claude-log-hdr" onclick="JA.flowAnalysis._toggleEl('${lid}')">
                        <div>
                            <span style="font-size:12px;font-weight:700;color:var(--accent-hover)">${JA.utils.escapeHtml(nodeShort)}</span>
                            <span style="font-size:11px;color:var(--text-muted);margin-left:8px">chunk ${JA.utils.escapeHtml(String(log.chunk||'1/1'))}</span>
                            <code style="font-size:11px;color:var(--accent-hover);margin-left:8px">${JA.utils.escapeHtml(log.endpoint||'')}</code>
                        </div>
                        <span style="font-size:11px;color:var(--text-muted)" id="${lid}-arrow">${collapseLogs ? '▼ expand' : '▲ collapse'}</span>
                    </div>
                    <div id="${lid}" style="${bodyStyle}">
                        <div style="padding:10px 14px;border-bottom:1px solid var(--accent-subtle)">
                            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
                                <span style="font-size:11px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em">INPUT — Prompt sent to Claude</span>
                                <button onclick="JA.flowAnalysis._toggleEl('${lid}-prompt')" style="font-size:11px;color:var(--text-muted);background:none;border:none;cursor:pointer;padding:0">▲ collapse</button>
                            </div>
                            <pre id="${lid}-prompt" style="background:var(--sidebar-bg);color:var(--sidebar-text);padding:12px;border-radius:var(--radius-sm);font-size:11px;overflow-x:auto;margin:0;white-space:pre-wrap;max-height:400px;overflow-y:auto">${JA.utils.escapeHtml(log.prompt||'')}</pre>
                        </div>
                        <div style="padding:10px 14px">
                            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
                                <span style="font-size:11px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em">OUTPUT — Claude response</span>
                                <button onclick="JA.flowAnalysis._toggleEl('${lid}-resp')" style="font-size:11px;color:var(--text-muted);background:none;border:none;cursor:pointer;padding:0">▲ collapse</button>
                            </div>
                            <pre id="${lid}-resp" style="background:var(--badge-green-bg);color:var(--badge-green);padding:12px;border-radius:var(--radius-sm);font-size:11px;overflow-x:auto;margin:0;white-space:pre-wrap;max-height:400px;overflow-y:auto">${JA.utils.escapeHtml(log.rawResponse||'(no response)')}</pre>
                        </div>
                    </div>
                </div>`;
            }
            html += '</div>';
        }

        // ── Call tree per endpoint ──
        for (const r of results) {
            const steps = r.flowSteps;
            const totalQ = this._countQueries(steps);
            html += `<div class="flow-card" style="margin-top:12px">
                <div style="font-size:12px;font-weight:600;color:var(--text-secondary);margin-bottom:8px">
                    Call tree — <code style="color:var(--accent-hover)">${JA.utils.escapeHtml(r.endpoint||'')}</code>
                    <span style="color:var(--text-muted);font-weight:400;margin-left:8px">${totalQ} DB operation${totalQ!==1?'s':''}</span>
                </div>
                <pre style="background:var(--sidebar-bg);color:var(--sidebar-text);padding:14px;border-radius:var(--radius);font-size:12px;overflow-x:auto;margin:0">${JA.utils.escapeHtml(this._renderTree(steps, ''))}</pre>`;
            if (totalQ === 0) {
                html += `<p style="font-size:12px;color:var(--text-muted);margin:6px 0 0">No DB operations detected. Common causes: JPA without explicit <code>@Query</code>, or dynamic queries built at runtime.</p>`;
            }
            html += '</div>';
        }

        if (results.length === 0) {
            html += '<p style="margin-top:12px;color:var(--text-muted);font-size:13px">No matching endpoints found.</p>';
        }

        return html;
    },

    // ── Shared helpers ────────────────────────────────────────────────────────

    _expandAllLogs(pfx = '') {
        document.querySelectorAll(`[id^="${pfx}flow-log-body-"]`).forEach(el => {
            if (!el.id.endsWith('-prompt') && !el.id.endsWith('-resp')) {
                el.style.display = '';
            }
        });
    },

    _collapseAllLogs(pfx = '') {
        document.querySelectorAll(`[id^="${pfx}flow-log-body-"]`).forEach(el => {
            if (!el.id.endsWith('-prompt') && !el.id.endsWith('-resp')) {
                el.style.display = 'none';
            }
        });
    },

    _countQueries(step) {
        if (!step) return 0;
        let n = (step.queries || []).length;
        for (const c of (step.children || [])) n += this._countQueries(c);
        return n;
    },

    _renderTree(step, indent) {
        if (!step) return '';
        const kindIcon = { CONTROLLER: '⬛', SERVICE: '🔷', REPOSITORY: '🗄', OTHER: '◻' };
        const icon  = kindIcon[step.kind] || '◻';
        const cls   = (step.class || '').replace(/.*\./, '');
        const lines = [];
        lines.push(`${indent}${icon} ${cls}.${step.method || '?'}()`);
        for (const q of (step.queries || [])) {
            const opLabel = q.type === 'SELECT' ? 'FIND' : (q.type || 'QUERY');
            lines.push(`${indent}    └─ ${opLabel} → ${q.table || '?'}`);
        }
        const children = (step.children || []).filter(c => c.kind !== 'OTHER' || (c.queries || []).length > 0);
        for (let i = 0; i < children.length; i++) {
            const last      = i === children.length - 1;
            const connector = last ? '└─ ' : '├─ ';
            const childCont = last ? '   ' : '│  ';
            const childLines = this._renderTree(children[i], indent + childCont).split('\n');
            if (childLines.length > 0) childLines[0] = indent + connector + childLines[0].slice((indent + childCont).length);
            lines.push(...childLines);
        }
        return lines.join('\n');
    },

    _toggleInsight(rowId) {
        const row = document.getElementById(rowId);
        if (row) row.style.display = row.style.display === 'none' ? '' : 'none';
    },

    _toggleEl(id) {
        const el = document.getElementById(id);
        if (el) el.style.display = el.style.display === 'none' ? '' : 'none';
    },

    async _download(jarId, runId) {
        const resp = await fetch(`/api/jar/jars/${decodeURIComponent(jarId)}/flow/report/${runId}`);
        const text = await resp.text();
        const blob = new Blob([text], { type: 'text/markdown' });
        const a    = document.createElement('a');
        a.href     = URL.createObjectURL(blob);
        a.download = decodeURIComponent(jarId).replace('.jar','') + '-flow-analysis.md';
        a.click();
    }
};
