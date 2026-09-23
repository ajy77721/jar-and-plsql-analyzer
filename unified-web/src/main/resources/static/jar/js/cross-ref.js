/**
 * Cross-Reference tab — two top-level tabs:
 *   1. Link Endpoint  — form to link a JAR endpoint to an Oracle proc
 *   2. Saved Details  — list of past runs + detail panel with 3 sub-tabs:
 *        Linked Flow | Java→Oracle | Oracle→Java
 */
window.JA = window.JA || {};
JA.crossRef = {

    // ── State ────────────────────────────────────────────────────────────────
    _tab: 'link',          // 'link' | 'saved'
    _jarId: null,
    _endpoints: [],
    _analyzedPaths: new Set(),
    _selectedEp: null,
    _epFilter: 'ALL',
    _selectedProc: null,
    _availableProcs: [],
    _procsLoaded: false,
    _linkSuccess: null,    // { crossRefId, mode, summary } after successful link

    // Saved-details state
    _savedList: [],        // history items loaded from server
    _savedLoading: false,
    _savedActiveId: null,  // currently open run
    _savedResult: null,    // full CrossRefResult for active run
    _savedSubTab: 'flow',  // 'flow' | 'java2oracle' | 'oracle2java'
    _claudeRunning: false, // true while Claude enrichment SSE is active
    _j2oSelected: null,    // selected proc-link key in Java→Oracle sub-tab
    _o2jSelected: null,    // selected oracle node id in Oracle→Java sub-tab
    _j2oFilter: '',
    _o2jFilter: '',
    _o2jRightFilter: '',
    _searchQuery: '',
    _searchSelected: null,
    _claudeDiscFilter: '',
    _claudeDiscSelected: null,
    _treeJavaNodeId: null,   // java node pinned in Source Tree tab
    _treeProcRef: null,      // oracle proc pinned in Source Tree tab

    // ── Entry point ───────────────────────────────────────────────────────────

    render(analysis, jarId) {
        if (this._jarId !== jarId) {
            this._selectedEp = null;
            this._epFilter = 'ALL';
            this._tab = 'link';
            this._analyzedPaths = new Set();
            this._selectedProc = null;
            this._procsLoaded = false;
            this._availableProcs = [];
            this._linkSuccess = null;
            this._savedList = [];
            this._savedActiveId = null;
            this._savedResult = null;
            this._savedSubTab = 'flow';
            this._j2oSelected = null;
            this._o2jSelected = null;
            this._searchQuery = '';
            this._searchSelected = null;
            this._claudeDiscFilter = '';
            this._claudeDiscSelected = null;
            this._treeJavaNodeId = null;
            this._treeProcRef = null;
        }
        this._jarId = jarId;
        this._endpoints = (analysis && analysis.endpoints) || [];
        const container = document.getElementById('crossref-container');
        if (!container) return;
        container.innerHTML = this._css() + this._buildShell();
        this._showTab(this._tab);
        this._loadAnalyzedPaths(jarId);
    },

    // ── Shell ─────────────────────────────────────────────────────────────────

    _buildShell() {
        return `
<div class="cr-wrap">
  <div class="cr-topnav">
    <button class="cr-topbtn" data-tab="link"  onclick="JA.crossRef._showTab('link')">🔗 Link Endpoint</button>
    <button class="cr-topbtn" data-tab="saved" onclick="JA.crossRef._showTab('saved')">💾 Saved Details</button>
  </div>
  <div id="cr-body"></div>
</div>`;
    },

    _showTab(tab) {
        this._tab = tab;
        document.querySelectorAll('.cr-topbtn').forEach(b =>
            b.classList.toggle('active', b.dataset.tab === tab));
        const body = document.getElementById('cr-body');
        if (!body) return;
        if (tab === 'link') {
            body.innerHTML = this._buildLinkView();
            if (this._selectedEp) this._selectEndpoint(this._selectedEp.idx, this._selectedEp.method, this._selectedEp.path);
            if (this._selectedProc) this._selectProc(this._selectedProc.procName, this._selectedProc.schema, this._selectedProc.sessionId, this._selectedProc.analyzed);
            if (!this._procsLoaded) this._loadAvailableProcs();
        } else {
            this._renderSavedTab();
            if (!this._savedList.length && !this._savedLoading) this._loadSavedList();
        }
    },

    // ── Analyzed paths (for green tagging in endpoint picker) ────────────────

    _loadAnalyzedPaths(jarId) {
        fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/flow/history`)
            .then(r => r.ok ? r.json() : [])
            .then(history => {
                const paths = new Set();
                (history || []).forEach(run => (run.endpoints || []).forEach(p => paths.add(p)));
                this._analyzedPaths = paths;
                const dd = document.getElementById('cr-ep-dropdown');
                if (dd && dd.classList.contains('open')) {
                    const q = (document.getElementById('cr-ep-search') || {}).value || '';
                    this._renderDropdownItems(this._getFilteredEndpoints(q), q.toLowerCase().trim());
                }
                const countEl = document.getElementById('cr-ep-count');
                if (countEl && paths.size > 0)
                    countEl.textContent = `(${this._endpoints.length} total · ${paths.size} analyzed)`;
            })
            .catch(() => {});
    },

    // ════════════════════════════════════════════════════════════════
    // TAB 1 — LINK ENDPOINT
    // ════════════════════════════════════════════════════════════════

    _buildLinkView() {
        const total = this._endpoints.length;
        const banner = this._linkSuccess ? this._buildSuccessBanner(this._linkSuccess) : '';
        return `
${banner}
<div class="cr-form">
  <div class="cr-notice">
    Select a JAR endpoint and an Oracle package/procedure, then click <strong>Link</strong> to
    generate a side-by-side Java ↔ Oracle comparison.
  </div>

  <label>Endpoint <span class="cr-ep-count" id="cr-ep-count">(${total} total)</span></label>
  <div class="cr-ep-filter-bar">
    ${['ALL','GET','POST','PUT','DELETE'].map(m =>
      `<button class="cr-ep-filter-btn${this._epFilter===m?' active':''}" data-method="${m}"
               onclick="JA.crossRef._setMethodFilter('${m}')">${m}</button>`).join('')}
  </div>
  <div class="cr-ep-picker" id="cr-ep-picker">
    <span class="cr-ep-search-icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg></span>
    <input class="cr-ep-search" id="cr-ep-search" type="text"
           placeholder="Search by path, method, or class name…" autocomplete="off"
           oninput="JA.crossRef._filterEndpoints()"
           onfocus="JA.crossRef._openDropdown()"
           onkeydown="JA.crossRef._onSearchKey(event)">
    <div class="cr-ep-dropdown" id="cr-ep-dropdown"></div>
  </div>
  <div class="cr-ep-selected-display" id="cr-ep-selected">
    <span id="cr-ep-selected-text" style="flex:1"></span>
    <button onclick="JA.crossRef._clearEpSelection()" class="cr-clear-btn">✕</button>
  </div>

  <label style="margin-top:4px">Oracle Package / Procedure
    <span class="cr-ep-count" id="cr-proc-count"></span>
  </label>
  <div class="cr-ep-picker" id="cr-proc-picker">
    <span class="cr-ep-search-icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg></span>
    <input class="cr-ep-search" id="cr-proc-search" type="text"
           placeholder="Search Oracle procs (e.g. PG_UWGE_CI)…" autocomplete="off"
           oninput="JA.crossRef._filterProcs()"
           onfocus="JA.crossRef._openProcDropdown()"
           onkeydown="JA.crossRef._onProcKey(event)">
    <div class="cr-ep-dropdown" id="cr-proc-dropdown"></div>
  </div>
  <div class="cr-ep-selected-display" id="cr-proc-selected">
    <span id="cr-proc-selected-text" style="flex:1"></span>
    <button onclick="JA.crossRef._clearProcSelection()" class="cr-clear-btn">✕</button>
  </div>

  <input type="hidden" id="cr-proc-input">

  <label for="cr-schema-input">Oracle Schema <span style="font-weight:400;color:var(--text-muted)">(auto-filled)</span></label>
  <input id="cr-schema-input" type="text" placeholder="e.g. CUSTOMER" class="cr-text-input">

  <div style="display:flex;align-items:center;gap:12px;margin-top:4px">
    <button class="cr-btn" id="cr-link-btn" onclick="JA.crossRef._runLink()">Link</button>
    <span id="cr-link-status" style="font-size:.82rem;color:var(--text-muted)"></span>
  </div>
</div>`;
    },

    _buildSuccessBanner(s) {
        const esc = JA.utils.escapeHtml;
        const m = s.summary || {};
        return `
<div class="cr-success-banner">
  <span class="cr-success-icon">✓</span>
  <div style="flex:1">
    <strong>Link complete</strong> —
    <span class="cr-badge cr-badge-${s.mode==='FULL_COMPARISON'?'high':'med'}">${esc(s.mode||'')}</span>
    &nbsp;${m.plsqlNodesFound||0} PL/SQL confirmed &nbsp;·&nbsp; ${m.totalJavaNodes||0} Java nodes
  </div>
  <button class="cr-btn" style="padding:5px 14px;font-size:.8rem"
          onclick="JA.crossRef._openSavedRun('${esc(s.crossRefId)}')">View in Saved Details →</button>
  <button onclick="JA.crossRef._linkSuccess=null;JA.crossRef._showTab('link')"
          class="cr-clear-btn" style="margin-left:4px">✕</button>
</div>`;
    },

    // ── Link form — endpoint picker ───────────────────────────────────────────

    _methodColor(m) {
        m = (m||'').toUpperCase();
        return m==='GET'?'var(--blue)':m==='POST'?'var(--green)':m==='PUT'?'var(--orange)':m==='DELETE'?'var(--red)':m==='PATCH'?'var(--accent-hover)':'var(--bg-inset)';
    },

    _setMethodFilter(method) {
        this._epFilter = method;
        document.querySelectorAll('.cr-ep-filter-btn').forEach(b =>
            b.classList.toggle('active', b.dataset.method === method));
        this._filterEndpoints();
        const s = document.getElementById('cr-ep-search'); if (s) s.focus();
    },

    _getFilteredEndpoints(query) {
        const q = (query||'').toLowerCase().trim();
        return this._endpoints.filter(ep => {
            const method = (ep.httpMethod||'GET').toUpperCase();
            const path   = (ep.fullPath||ep.path||ep.endpointName||'').toLowerCase();
            const cls    = (ep.controllerClass||ep.className||'').toLowerCase();
            const mthd   = (ep.methodName||'').toLowerCase();
            if (this._epFilter !== 'ALL' && method !== this._epFilter) return false;
            if (!q) return true;
            return path.includes(q)||cls.includes(q)||mthd.includes(q)||method.toLowerCase().includes(q);
        });
    },

    _filterEndpoints() {
        const q = (document.getElementById('cr-ep-search')||{}).value||'';
        const filtered = this._getFilteredEndpoints(q);
        const countEl = document.getElementById('cr-ep-count');
        if (countEl) countEl.textContent = `(${filtered.length} / ${this._endpoints.length})`;
        this._renderDropdownItems(filtered, q);
        this._openDropdown();
    },

    _renderDropdownItems(endpoints, highlight) {
        const dd = document.getElementById('cr-ep-dropdown');
        if (!dd) return;
        if (!endpoints.length) {
            dd.innerHTML = '<div style="padding:12px;color:var(--text-muted);font-size:.83rem;text-align:center">No endpoints match</div>';
            return;
        }
        const esc = JA.utils.escapeHtml;
        const hl = (highlight||'').toLowerCase();
        const hi = t => {
            if (!hl||!t) return esc(t||'');
            const i = t.toLowerCase().indexOf(hl);
            return i<0 ? esc(t) : esc(t.slice(0,i))+`<mark style="background:var(--badge-orange-bg);padding:0">${esc(t.slice(i,i+hl.length))}</mark>`+esc(t.slice(i+hl.length));
        };
        const renderItem = ep => {
            const method = (ep.httpMethod||'GET').toUpperCase();
            const path   = ep.fullPath||ep.path||ep.endpointName||'';
            const idx    = ep.index!=null?ep.index:this._endpoints.indexOf(ep);
            const color  = this._methodColor(method);
            const pair   = JA.utils.badgePair(color);
            const isSel  = this._selectedEp && this._selectedEp.idx===idx;
            const isAna  = this._analyzedPaths.has(path);
            return `<div class="cr-ep-item${isSel?' selected':''}${isAna?' analyzed':''}"
                        data-idx="${idx}" data-method="${esc(method)}" data-path="${esc(path)}"
                        onmousedown="JA.crossRef._selectEndpoint(${idx},'${esc(method)}','${esc(path).replace(/'/g,"\\'")}')">
  <span class="cr-ep-method" style="background:${pair.bg};color:${pair.ink}">${esc(method)}</span>
  <span class="cr-ep-path">${hi(path)}</span>
  ${isAna?'<span class="cr-ep-analyzed-tag">✓ analyzed</span>':''}
  <span class="cr-ep-idx">#${idx}</span>
</div>`;
        };
        const analyzed = endpoints.filter(ep => this._analyzedPaths.has(ep.fullPath||ep.path||''));
        const rest      = endpoints.filter(ep => !this._analyzedPaths.has(ep.fullPath||ep.path||''));
        const showGrp   = analyzed.length>0 && rest.length>0;
        const CAP = 200;
        let html = '', total = 0;
        if (analyzed.length) {
            if (showGrp) html += `<div class="cr-ep-group-label">✓ Already Analyzed (${analyzed.length})</div>`;
            analyzed.slice(0,CAP).forEach(ep => { html += renderItem(ep); total++; });
        }
        if (rest.length && total<CAP) {
            if (showGrp) html += `<div class="cr-ep-group-label">All Endpoints</div>`;
            rest.slice(0,CAP-total).forEach(ep => { html += renderItem(ep); });
        }
        if (endpoints.length>CAP) html += `<div style="padding:8px 12px;font-size:.75rem;color:var(--text-muted);text-align:center">Showing ${CAP} of ${endpoints.length} — type to narrow</div>`;
        dd.innerHTML = html;
    },

    _openDropdown() {
        const dd = document.getElementById('cr-ep-dropdown');
        if (!dd) return;
        if (!dd.classList.contains('open')) {
            const q = (document.getElementById('cr-ep-search')||{}).value||'';
            this._renderDropdownItems(this._getFilteredEndpoints(q), q.toLowerCase().trim());
        }
        dd.classList.add('open');
        setTimeout(() => document.addEventListener('click', this._outsideClickHandler, {once:true}), 0);
    },

    _outsideClickHandler(e) {
        const picker = document.getElementById('cr-ep-picker');
        if (picker && !picker.contains(e.target)) {
            const dd = document.getElementById('cr-ep-dropdown');
            if (dd) dd.classList.remove('open');
        }
    },

    _selectEndpoint(idx, method, path) {
        this._selectedEp = {idx, method, path};
        const s = document.getElementById('cr-ep-search');
        if (s) s.value = method+' '+path;
        const disp = document.getElementById('cr-ep-selected');
        const txt  = document.getElementById('cr-ep-selected-text');
        if (disp && txt) {
            const color = this._methodColor(method);
            const pair = JA.utils.badgePair(color);
            txt.innerHTML = `<span style="font-size:.72rem;font-weight:700;padding:2px 6px;border-radius:var(--radius-sm);background:${pair.bg};color:${pair.ink};margin-right:6px">${JA.utils.escapeHtml(method)}</span>${JA.utils.escapeHtml(path)}`;
            disp.classList.add('show');
        }
        const dd = document.getElementById('cr-ep-dropdown');
        if (dd) dd.classList.remove('open');
        const c = document.getElementById('cr-ep-count');
        if (c) c.textContent = `(${this._endpoints.length} total)`;
    },

    _clearEpSelection() {
        this._selectedEp = null;
        const s = document.getElementById('cr-ep-search');
        if (s) { s.value=''; s.focus(); }
        const d = document.getElementById('cr-ep-selected');
        if (d) d.classList.remove('show');
        this._filterEndpoints();
    },

    _onSearchKey(e) {
        const dd = document.getElementById('cr-ep-dropdown');
        if (!dd||!dd.classList.contains('open')) return;
        const items = dd.querySelectorAll('.cr-ep-item');
        const cur   = dd.querySelector('.cr-ep-item.selected');
        let idx = -1; items.forEach((item,i) => { if (item===cur) idx=i; });
        if (e.key==='ArrowDown') { e.preventDefault(); const n=items[Math.min(idx+1,items.length-1)]; if(n){if(cur)cur.classList.remove('selected');n.classList.add('selected');n.scrollIntoView({block:'nearest'});} }
        else if (e.key==='ArrowUp') { e.preventDefault(); const p=items[Math.max(idx-1,0)]; if(p){if(cur)cur.classList.remove('selected');p.classList.add('selected');p.scrollIntoView({block:'nearest'});} }
        else if (e.key==='Enter') { e.preventDefault(); const s=dd.querySelector('.cr-ep-item.selected')||items[0]; if(s) this._selectEndpoint(parseInt(s.dataset.idx,10),s.dataset.method,s.dataset.path); }
        else if (e.key==='Escape') dd.classList.remove('open');
    },

    // ── Link form — proc picker ───────────────────────────────────────────────

    _loadAvailableProcs() {
        fetch(`/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/available-procs`)
            .then(r => r.ok ? r.json() : [])
            .then(procs => {
                this._availableProcs = procs||[];
                this._procsLoaded = true;
                const c = document.getElementById('cr-proc-count');
                if (c) {
                    const analyzed = this._availableProcs.filter(p=>p.analyzed).length;
                    c.textContent = analyzed>0 ? `(${this._availableProcs.length} · ${analyzed} analyzed)` : `(${this._availableProcs.length})`;
                }
                const dd = document.getElementById('cr-proc-dropdown');
                if (dd && dd.classList.contains('open')) {
                    const q = (document.getElementById('cr-proc-search')||{}).value||'';
                    this._renderProcDropdownItems(this._getFilteredProcs(q), q);
                }
            })
            .catch(() => { this._procsLoaded = true; });
    },

    _getFilteredProcs(query) {
        const q = (query||'').toLowerCase().trim();
        if (!q) return this._availableProcs;
        return this._availableProcs.filter(p =>
            (p.procName||'').toLowerCase().includes(q) ||
            (p.schema||'').toLowerCase().includes(q));
    },

    _filterProcs() {
        const q = (document.getElementById('cr-proc-search')||{}).value||'';
        this._renderProcDropdownItems(this._getFilteredProcs(q), q);
        this._openProcDropdown();
    },

    _renderProcDropdownItems(procs, highlight) {
        const dd = document.getElementById('cr-proc-dropdown');
        if (!dd) return;
        if (!this._procsLoaded) { dd.innerHTML='<div style="padding:12px;color:var(--text-muted);font-size:.83rem;text-align:center">Loading…</div>'; return; }
        if (!procs.length && !this._availableProcs.length) { dd.innerHTML='<div style="padding:12px;color:var(--text-muted);font-size:.83rem;text-align:center">No PL/SQL analyses found.<br><span style="font-size:.75rem">Type a proc name manually.</span></div>'; return; }
        if (!procs.length) { dd.innerHTML='<div style="padding:12px;color:var(--text-muted);font-size:.83rem;text-align:center">No matches</div>'; return; }
        const esc = JA.utils.escapeHtml;
        const h = (highlight||'').toLowerCase();
        const hi = t => { if (!h||!t) return esc(t||''); const i=t.toLowerCase().indexOf(h); return i<0?esc(t):esc(t.slice(0,i))+`<mark style="background:var(--badge-orange-bg);padding:0">${esc(t.slice(i,i+h.length))}</mark>`+esc(t.slice(i+h.length)); };
        const analyzed = procs.filter(p=>p.analyzed);
        const other    = procs.filter(p=>!p.analyzed);
        const showGrp  = analyzed.length>0 && other.length>0;
        const CAP = 200;
        let html='', rendered=0;
        if (analyzed.length) {
            if (showGrp) html+=`<div class="cr-ep-group-label">✓ PL/SQL Analyzed (${analyzed.length})</div>`;
            analyzed.slice(0,CAP).forEach(p => {
                const isSel = this._selectedProc && this._selectedProc.procName===p.procName;
                html+=`<div class="cr-ep-item analyzed${isSel?' selected':''}" data-proc="${esc(p.procName)}" data-schema="${esc(p.schema||'')}" data-session="${esc(p.sessionId||'')}"
                            onmousedown="JA.crossRef._selectProc('${esc(p.procName).replace(/'/g,"\\'")}','${esc(p.schema||'')}','${esc(p.sessionId||'')}',true)">
  <span class="cr-ep-path" style="font-family:monospace">${hi(p.procName)}</span>
  <span class="cr-ep-analyzed-tag">✓ analyzed</span>
  <span class="cr-ep-idx" style="color:var(--accent-hover);font-size:.7rem">${esc(p.schema||'')}</span>
</div>`;
                rendered++;
            });
        }
        if (other.length && rendered<CAP) {
            if (showGrp) html+=`<div class="cr-ep-group-label">Other Procs</div>`;
            other.slice(0,CAP-rendered).forEach(p => {
                const isSel = this._selectedProc && this._selectedProc.procName===p.procName;
                html+=`<div class="cr-ep-item${isSel?' selected':''}" data-proc="${esc(p.procName)}"
                            onmousedown="JA.crossRef._selectProc('${esc(p.procName).replace(/'/g,"\\'")}','${esc(p.schema||'')}','',false)">
  <span class="cr-ep-path" style="font-family:monospace">${hi(p.procName)}</span>
  <span class="cr-ep-idx">${esc(p.schema||'')}</span>
</div>`;
            });
        }
        dd.innerHTML = html;
    },

    _openProcDropdown() {
        const dd = document.getElementById('cr-proc-dropdown');
        if (!dd) return;
        if (!dd.classList.contains('open')) {
            const q = (document.getElementById('cr-proc-search')||{}).value||'';
            this._renderProcDropdownItems(this._getFilteredProcs(q), q);
        }
        dd.classList.add('open');
        setTimeout(() => document.addEventListener('click', this._outsideProcClickHandler, {once:true}), 0);
    },

    _outsideProcClickHandler(e) {
        const picker = document.getElementById('cr-proc-picker');
        if (picker && !picker.contains(e.target)) {
            const dd = document.getElementById('cr-proc-dropdown');
            if (dd) dd.classList.remove('open');
        }
    },

    _selectProc(procName, schema, sessionId, analyzed) {
        this._selectedProc = {procName, schema, sessionId, analyzed};
        const inp = document.getElementById('cr-proc-input');
        if (inp) inp.value = procName;
        const si = document.getElementById('cr-schema-input');
        if (si && schema && !si.value) si.value = schema;
        const s = document.getElementById('cr-proc-search');
        if (s) s.value = procName;
        const disp = document.getElementById('cr-proc-selected');
        const txt  = document.getElementById('cr-proc-selected-text');
        if (disp && txt) {
            const esc = JA.utils.escapeHtml;
            txt.innerHTML = `<span style="font-family:monospace;font-size:.85rem">${esc(procName)}</span>`
                +(schema?` <span style="font-size:.75rem;color:var(--text-muted);margin-left:4px">${esc(schema)}</span>`:'')
                +(analyzed?' <span class="cr-ep-analyzed-tag">✓ analyzed</span>':'');
            disp.classList.add('show');
        }
        const dd = document.getElementById('cr-proc-dropdown');
        if (dd) dd.classList.remove('open');
    },

    _clearProcSelection() {
        this._selectedProc = null;
        const inp = document.getElementById('cr-proc-input'); if (inp) inp.value='';
        const s = document.getElementById('cr-proc-search'); if (s) { s.value=''; s.focus(); }
        const d = document.getElementById('cr-proc-selected'); if (d) d.classList.remove('show');
        this._filterProcs();
    },

    _onProcKey(e) {
        const dd = document.getElementById('cr-proc-dropdown');
        if (!dd||!dd.classList.contains('open')) return;
        const items = dd.querySelectorAll('.cr-ep-item');
        const cur   = dd.querySelector('.cr-ep-item.selected');
        let idx=-1; items.forEach((item,i) => { if(item===cur) idx=i; });
        if (e.key==='ArrowDown') { e.preventDefault(); const n=items[Math.min(idx+1,items.length-1)]; if(n){if(cur)cur.classList.remove('selected');n.classList.add('selected');n.scrollIntoView({block:'nearest'});} }
        else if (e.key==='ArrowUp') { e.preventDefault(); const p=items[Math.max(idx-1,0)]; if(p){if(cur)cur.classList.remove('selected');p.classList.add('selected');p.scrollIntoView({block:'nearest'});} }
        else if (e.key==='Enter') { e.preventDefault(); const s=dd.querySelector('.cr-ep-item.selected')||items[0]; if(s) this._selectProc(s.dataset.proc,s.dataset.schema||'',s.dataset.session||'',s.classList.contains('analyzed')); }
        else if (e.key==='Escape') dd.classList.remove('open');
    },

    // ── Run link ──────────────────────────────────────────────────────────────

    _runLink() {
        const procInput   = document.getElementById('cr-proc-input');
        const schemaInput = document.getElementById('cr-schema-input');
        const statusEl    = document.getElementById('cr-link-status');
        const btn         = document.getElementById('cr-link-btn');
        const ep          = this._selectedEp;
        const procName    = (procInput&&procInput.value.trim()) || (document.getElementById('cr-proc-search')||{}).value?.trim()||'';

        if (!ep||!procName) {
            if (statusEl) statusEl.textContent = !ep ? 'Please select an endpoint first.' : 'Please enter an Oracle proc/package name.';
            return;
        }
        if (btn) btn.disabled = true;
        if (statusEl) statusEl.innerHTML = '<span style="color:var(--blue)">⏳ Linking… this may take a moment</span>';

        fetch(`/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/link`, {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({
                endpointPath: ep.path,
                procName,
                schema: schemaInput&&schemaInput.value.trim()||null,
                endpointIndex: ep.idx
            })
        })
        .then(r => r.json())
        .then(data => {
            if (btn) btn.disabled = false;
            if (data.error) {
                if (statusEl) statusEl.innerHTML = `<span style="color:var(--badge-red)">Error: ${JA.utils.escapeHtml(data.error)}</span>`;
                return;
            }
            // Store success and invalidate saved list cache so it reloads
            this._linkSuccess = data;
            this._savedList = [];
            this._savedActiveId = null;
            this._savedResult = null;
            // Re-render link view showing the success banner
            const body = document.getElementById('cr-body');
            if (body) body.innerHTML = this._buildLinkView();
        })
        .catch(err => {
            if (btn) btn.disabled = false;
            if (statusEl) statusEl.innerHTML = `<span style="color:var(--badge-red)">Error: ${JA.utils.escapeHtml(err.message)}</span>`;
        });
    },

    // ════════════════════════════════════════════════════════════════
    // TAB 2 — SAVED DETAILS
    // ════════════════════════════════════════════════════════════════

    _renderSavedTab() {
        const body = document.getElementById('cr-body');
        if (!body) return;
        body.innerHTML = `
<div class="cr-saved-layout">
  <div class="cr-saved-sidebar">
    <div class="cr-saved-sidebar-hdr">
      <input class="cr-saved-search" id="cr-saved-search" type="text"
             placeholder="🔍 Search saved links…"
             oninput="JA.crossRef._filterSavedList()">
    </div>
    <div class="cr-saved-list" id="cr-saved-list">
      <div class="cr-loading">Loading…</div>
    </div>
  </div>
  <div class="cr-saved-detail" id="cr-saved-detail">
    <div class="cr-detail-empty">
      <div style="font-size:2rem;margin-bottom:8px">🔗</div>
      <div style="color:var(--text-muted);font-size:.9rem">Select a saved link from the left to view details.</div>
    </div>
  </div>
</div>`;
        if (this._savedList.length) {
            this._renderSavedList();
            if (this._savedActiveId) this._openSavedRunDetail(this._savedActiveId);
        }
    },

    _loadSavedList() {
        this._savedLoading = true;
        fetch(`/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/history`)
            .then(r => r.ok ? r.json() : [])
            .then(list => {
                this._savedList = list||[];
                this._savedLoading = false;
                this._renderSavedList();
                // Auto-open first item or last-opened
                if (!this._savedActiveId && this._savedList.length) {
                    this._openSavedRunDetail(this._savedList[0].crossRefId);
                } else if (this._savedActiveId) {
                    this._openSavedRunDetail(this._savedActiveId);
                }
            })
            .catch(() => {
                this._savedLoading = false;
                const el = document.getElementById('cr-saved-list');
                if (el) el.innerHTML = '<div style="padding:16px;color:var(--badge-red);font-size:.83rem">Failed to load.</div>';
            });
    },

    _filterSavedList() {
        this._renderSavedList();
    },

    _renderSavedList() {
        const el = document.getElementById('cr-saved-list');
        if (!el) return;
        const esc = JA.utils.escapeHtml;
        const q = ((document.getElementById('cr-saved-search')||{}).value||'').toLowerCase();
        let items = this._savedList;
        if (q) items = items.filter(it =>
            (it.endpointPath||'').toLowerCase().includes(q) ||
            (it.procName||'').toLowerCase().includes(q));

        if (!items.length) {
            el.innerHTML = `<div style="padding:16px;color:var(--text-muted);font-size:.83rem;text-align:center">${this._savedList.length ? 'No matches' : 'No saved links yet.<br>Use the Link Endpoint tab to create one.'}</div>`;
            return;
        }
        el.innerHTML = items.map(it => {
            const ts = it.timestamp ? _relTime(it.timestamp) : '';
            const isActive = it.crossRefId === this._savedActiveId;
            const modeColor = it.mode==='FULL_COMPARISON'?'var(--badge-green)':'var(--badge-orange)';
            const modeBg    = it.mode==='FULL_COMPARISON'?'var(--badge-green-bg)':'var(--badge-orange-bg)';
            const shortEp = (it.endpointPath||'').replace(/^\/api\/v\d+\//,'');
            return `<div class="cr-saved-item${isActive?' active':''}"
                        onclick="JA.crossRef._openSavedRunDetail('${esc(it.crossRefId)}')">
  <div style="display:flex;align-items:flex-start;gap:4px">
    <div style="flex:1;min-width:0">
      <div class="cr-saved-item-ep" title="${esc(it.endpointPath||'')}">
        ${esc(shortEp)}
      </div>
      <div class="cr-saved-item-proc">${esc(it.procName||'')}</div>
      <div class="cr-saved-item-meta">
        <span class="cr-badge" style="background:${modeBg};color:${modeColor};font-size:.68rem;padding:1px 5px">${it.mode==='FULL_COMPARISON'?'FULL':'STATIC'}</span>
        <span style="font-size:.72rem;color:var(--text-muted)">${esc(ts)}</span>
      </div>
    </div>
    <button class="cr-del-btn" title="Delete"
            onclick="event.stopPropagation();JA.crossRef._deleteRun('${esc(it.crossRefId)}')">✕</button>
  </div>
</div>`;
        }).join('');
    },

    _deleteRun(crossRefId) {
        if (!confirm('Delete this cross-ref run?')) return;
        fetch(`/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}`, { method: 'DELETE' })
            .then(r => r.ok ? r.json() : Promise.reject())
            .then(() => {
                this._savedList = this._savedList.filter(it => it.crossRefId !== crossRefId);
                if (this._savedActiveId === crossRefId) {
                    this._savedActiveId = null;
                    this._savedResult = null;
                    const detail = document.getElementById('cr-saved-detail');
                    if (detail) detail.innerHTML = `<div class="cr-detail-empty"><div style="font-size:2rem;margin-bottom:8px">🔗</div><div style="color:var(--text-muted);font-size:.9rem">Select a saved link from the left to view details.</div></div>`;
                }
                this._renderSavedList();
            })
            .catch(() => alert('Delete failed.'));
    },

    _openSavedRun(crossRefId) {
        this._showTab('saved');
        this._savedActiveId = crossRefId;
        if (!this._savedList.length && !this._savedLoading) {
            this._loadSavedList();
        } else {
            this._renderSavedList();
            this._openSavedRunDetail(crossRefId);
        }
    },

    _openSavedRunDetail(crossRefId) {
        this._savedActiveId = crossRefId;
        this._renderSavedList(); // re-render to update active highlight
        const detail = document.getElementById('cr-saved-detail');
        if (!detail) return;
        // If we already have the result cached, use it
        if (this._savedResult && this._savedResult.crossRefId === crossRefId) {
            this._renderDetailPanel(this._savedResult);
            return;
        }
        detail.innerHTML = '<div class="cr-loading" style="padding:20px">Loading…</div>';
        this._savedResult = null;
        fetch(`/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}`)
            .then(r => r.ok ? r.json() : null)
            .then(data => {
                if (!data) { detail.innerHTML='<div style="padding:20px;color:var(--badge-red)">Result not found.</div>'; return; }
                this._savedResult = data;
                this._renderDetailPanel(data);
            })
            .catch(err => {
                detail.innerHTML = `<div style="padding:20px;color:var(--badge-red)">Error: ${JA.utils.escapeHtml(err.message)}</div>`;
            });
    },

    _renderDetailPanel(r) {
        const detail = document.getElementById('cr-saved-detail');
        if (!detail) return;
        const esc = JA.utils.escapeHtml;
        const s = r.summary||{};
        const modeClass = r.mode==='FULL_COMPARISON'?'cr-badge-high':'cr-badge-med';

        detail.innerHTML = `
<div class="cr-detail-header">
  <div class="cr-detail-title">
    <code class="cr-detail-ep" title="${esc(r.endpointPath||'')}">${esc(r.endpointPath||'')}</code>
    <span class="cr-detail-sep">↔</span>
    <code class="cr-detail-proc">${esc(r.procName||'')}</code>
  </div>
  <div class="cr-detail-meta">
    <span class="cr-badge ${modeClass}">${r.mode}</span>
    <span style="font-size:.78rem;color:var(--text-muted)">${s.plsqlNodesFound||0} PL/SQL confirmed</span>
    <span style="font-size:.78rem;color:var(--text-muted)">·</span>
    <span style="font-size:.78rem;color:var(--text-muted)">${(r.procLinks||[]).length} links</span>
    <a class="cr-btn cr-btn-sec" style="padding:4px 10px;font-size:.78rem;text-decoration:none;margin-left:auto"
       href="/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/report/${encodeURIComponent(r.crossRefId)}"
       download>↓ Report</a>
  </div>
</div>
<div class="cr-subtab-bar">
  <button class="cr-subtab-btn${this._savedSubTab==='flow'?' active':''}" data-sub="flow"
          onclick="JA.crossRef._showSubTab('flow')">Linked Flow</button>
  <button class="cr-subtab-btn${this._savedSubTab==='java2oracle'?' active':''}" data-sub="java2oracle"
          onclick="JA.crossRef._showSubTab('java2oracle')">Java → Oracle</button>
  <button class="cr-subtab-btn${this._savedSubTab==='oracle2java'?' active':''}" data-sub="oracle2java"
          onclick="JA.crossRef._showSubTab('oracle2java')">Oracle → Java</button>
  <button class="cr-subtab-btn${this._savedSubTab==='search'?' active':''}" data-sub="search"
          onclick="JA.crossRef._showSubTab('search')">🔍 Search + Source</button>
  <button class="cr-subtab-btn${this._savedSubTab==='claude'?' active':''}" data-sub="claude"
          onclick="JA.crossRef._showSubTab('claude')">🤖 Claude Discussion</button>
  <button class="cr-subtab-btn${this._savedSubTab==='tree'?' active':''}" data-sub="tree"
          onclick="JA.crossRef._showSubTab('tree')">🌳 Source Tree</button>
</div>
<div class="cr-subtab-body" id="cr-subtab-body">
  ${this._buildSubTabContent(r)}
</div>`;
    },

    _showSubTab(sub) {
        this._savedSubTab = sub;
        if (sub === 'java2oracle') { this._j2oFilter=''; this._j2oSelected=null; }
        if (sub === 'oracle2java') { this._o2jFilter=''; this._o2jSelected=null; this._o2jRightFilter=''; }
        if (sub === 'search') { this._searchSelected=null; }
        if (sub === 'claude') { this._claudeDiscSelected=null; }
        // tree tab: keep _treeJavaNodeId / _treeProcRef so navigating back restores position
        document.querySelectorAll('.cr-subtab-btn').forEach(b =>
            b.classList.toggle('active', b.dataset.sub === sub));
        const body = document.getElementById('cr-subtab-body');
        if (body && this._savedResult) {
            body.innerHTML = this._buildSubTabContent(this._savedResult);
            if (sub === 'tree') this._initSourceTreeTab(this._savedResult);
        }
    },

    _buildSubTabContent(r) {
        if (this._savedSubTab==='flow')        return this._buildFlowTab(r);
        if (this._savedSubTab==='java2oracle') return this._buildJ2OTab(r);
        if (this._savedSubTab==='oracle2java') return this._buildO2JTab(r);
        if (this._savedSubTab==='search')      return this._buildSearchTab(r);
        if (this._savedSubTab==='claude')      return this._buildClaudeDiscussionTab(r);
        if (this._savedSubTab==='tree')        return this._buildSourceTreeTab(r);
        return '';
    },

    // ── Sub-tab: Linked Flow ──────────────────────────────────────────────────

    _buildFlowTab(r) {
        const esc = JA.utils.escapeHtml;
        const s = r.summary||{};
        const allLinks = r.procLinks||[];
        const hints = r.lowConfidenceHints||[];
        const javaOnlyProcs = r.javaOnlyProcs||[];
        const oracleOnlyProcs = r.oracleOnlyProcs||[];
        const operationGaps = r.operationGaps||[];
        const passACnt = allLinks.filter(l=>l.zoneSource==='PASS_A'||l.zoneSource==='PASS_A+B').length;
        const passBCnt = allLinks.filter(l=>l.zoneSource==='PASS_B'||l.zoneSource==='PASS_A+B').length;

        const topId = 'cr-flow-top-' + Math.random().toString(36).slice(2);
        const botId = 'cr-flow-bot-' + Math.random().toString(36).slice(2);

        let html = `
<div class="cr-flow-scroll" id="${topId}">
<div class="cr-summary-grid">
  ${this._stat(allLinks.length,'Confirmed Links','var(--green)')}
  ${this._stat(javaOnlyProcs.length,'Java-Only Gaps','var(--orange)')}
  ${this._stat(oracleOnlyProcs.length,'Oracle-Only','var(--orange)')}
  ${this._stat(operationGaps.length,'Op Gaps','var(--orange)')}
  ${this._stat(hints.length,'Low-Conf Hints','var(--bg-inset)')}
  ${this._stat(s.unmatchedJavaNodes,'Infra Nodes','var(--bg-inset)')}
</div>
<div class="cr-pass-row">
  <span>Pass A (table→Oracle): <strong style="color:var(--badge-blue)">${passACnt}</strong></span>
  <span>Pass B (Pg* naming): <strong style="color:var(--accent-hover)">${passBCnt}</strong></span>
  <span style="color:var(--text-muted)">LOW/MEDIUM demoted to hints</span>
  <span style="margin-left:auto">
    <button onclick="document.getElementById('${botId}').scrollIntoView({behavior:'smooth'})" style="font-size:.75rem;padding:2px 8px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg);cursor:pointer">↓ Bottom</button>
  </span>
</div>`;

        if (r.mode==='STATIC_INFERENCE') {
            html += `<div class="cr-notice">Static Inference mode — Oracle proc names inferred from Java class naming. Run PL/SQL analysis for <strong>${esc(r.procName)}</strong> to enable Full Comparison.</div>`;
        }

        // ── Claude Enrichment Panel ───────────────────────────────────────────
        html += this._buildClaudePanel(r);

        // ── Gap Analysis ─────────────────────────────────────────────────────
        if (javaOnlyProcs.length || oracleOnlyProcs.length || operationGaps.length) {
            html += `<div class="cr-section cr-gap-section">
<div class="cr-section-title" style="color:var(--badge-orange)">Gap Analysis — What Is Not Converted</div>`;

            if (javaOnlyProcs.length) {
                const joClaudeColors = {'NOT_CONVERTED':'var(--red)','HAS_ORACLE_EQUIVALENT':'var(--green)','INFRA_ONLY':'var(--bg-inset)','UNKNOWN':'var(--orange)'};
                html += `<details open><summary class="cr-gap-summary-hdr">
  <span style="color:var(--badge-orange);font-weight:700">Java-Only (${javaOnlyProcs.length})</span>
  <span style="font-size:.78rem;color:var(--text-muted);margin-left:8px">Java methods with no matching Oracle proc</span>
</summary>
<table class="cr-table" style="margin-top:6px"><thead><tr><th>Class</th><th>Method</th><th>Op</th><th>Collections</th><th>Claude</th><th></th></tr></thead><tbody>`;
                javaOnlyProcs.forEach(p => {
                    const cls = (p.javaClass||'').split('.').pop();
                    const tables = (p.tables||[]).slice(0,3).map(t=>`<code style="font-size:.73rem">${esc(t)}</code>`).join(' ');
                    const claudeBadge = p.claudeDone
                        ? `<span title="${esc(p.claudeReason||'')}" style="background:${joClaudeColors[p.claudeVerdict]||'var(--bg-inset)'};color: var(--text);font-size:.68rem;padding:1px 5px;border-radius:var(--radius-sm)">${esc(p.claudeVerdict||'')}</span>`
                        : '<span style="color:var(--text-muted);font-size:.72rem">—</span>';
                    const nodeId = esc(cls + '#' + (p.javaMethod||''));
                    html+=`<tr><td><code>${esc(cls)}</code></td><td>${esc(p.javaMethod||'')}</td><td>${esc(p.operationType||'-')}</td><td>${tables||'-'}</td><td>${claudeBadge}</td>
<td><button onclick="JA.crossRef._openSourceTree('${nodeId}','')" style="font-size:.7rem;padding:1px 5px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg);cursor:pointer" title="View in Source Tree">🌳</button></td></tr>`;
                });
                html+='</tbody></table></details>';
            }

            if (oracleOnlyProcs.length) {
                const ooClaudeColors = {'NOT_IN_JAVA':'var(--red)','HAS_JAVA_EQUIVALENT':'var(--green)','INTERNAL_ONLY':'var(--bg-inset)','UNKNOWN':'var(--orange)'};
                html += `<details open><summary class="cr-gap-summary-hdr">
  <span style="color:var(--accent);font-weight:700">Oracle-Only (${oracleOnlyProcs.length})</span>
  <span style="font-size:.78rem;color:var(--text-muted);margin-left:8px">Oracle procs with no Java callers</span>
</summary>
<table class="cr-table" style="margin-top:6px"><thead><tr><th>Oracle Proc</th><th>Depth</th><th>Tables</th><th>Claude</th><th></th></tr></thead><tbody>`;
                oracleOnlyProcs.forEach(p => {
                    const tables = (p.tables||[]).slice(0,3).map(t=>`<code style="font-size:.73rem">${esc(t)}</code>`).join(' ');
                    const claudeBadge = p.claudeDone
                        ? `<span title="${esc(p.claudeReason||'')}" style="background:${ooClaudeColors[p.claudeVerdict]||'var(--bg-inset)'};color: var(--text);font-size:.68rem;padding:1px 5px;border-radius:var(--radius-sm)">${esc(p.claudeVerdict||'')}</span>`
                        : '<span style="color:var(--text-muted);font-size:.72rem">—</span>';
                    const pr = esc(p.procRef||'');
                    html+=`<tr><td><code>${pr}</code></td><td>${p.depth}</td><td>${tables||'-'}</td><td>${claudeBadge}</td>
<td><button onclick="JA.crossRef._openSourceTree('','${pr}')" style="font-size:.7rem;padding:1px 5px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg);cursor:pointer" title="View in Source Tree">🌳</button></td></tr>`;
                });
                html+='</tbody></table></details>';
            }

            if (operationGaps.length) {
                html += `<details open><summary class="cr-gap-summary-hdr">
  <span style="color:var(--red);font-weight:700">Operation Gaps (${operationGaps.length})</span>
  <span style="font-size:.78rem;color:var(--text-muted);margin-left:8px">Mismatched operations on shared tables</span>
</summary>
<table class="cr-table" style="margin-top:6px"><thead><tr><th>Table</th><th>Java Op</th><th>Oracle Op</th><th>Type</th></tr></thead><tbody>`;
                operationGaps.forEach(g => {
                    html+=`<tr><td><code>${esc(g.table||'-')}</code></td><td>${esc(g.javaOperation||'-')}</td><td>${esc(g.oracleOperation||'-')}</td><td><span class="cr-badge cr-badge-low">${esc(g.gapType||'-')}</span></td></tr>`;
                });
                html+='</tbody></table></details>';
            }
            html += '</div>';
        }

        // ── Confirmed Links ───────────────────────────────────────────────────
        const groups = new Map();
        for (const link of allLinks) {
            const ref = link.resolvedProcRef||link.inferredProcRef||'';
            const pkg = ref.includes('.')?ref.substring(0,ref.indexOf('.')):(ref||'UNKNOWN');
            if (!groups.has(pkg)) groups.set(pkg,[]);
            groups.get(pkg).push(link);
        }
        html += `<div class="cr-section"><div class="cr-section-title">Confirmed Links (${allLinks.length}) — ${groups.size} Package${groups.size!==1?'s':''}</div>`;
        for (const [pkg, pkgLinks] of groups) {
            const gid = 'crfl-'+pkg.replace(/[^a-zA-Z0-9]/g,'_');
            html += `<div class="cr-pkg-group">
<div class="cr-pkg-group-hdr" onclick="document.getElementById('${gid}').classList.toggle('open')">
  <span class="cr-pkg-name"><code>${esc(pkg)}</code></span>
  <span class="cr-pkg-count">${pkgLinks.length} method${pkgLinks.length!==1?'s':''}</span>
  <span class="cr-pkg-chevron">▾</span>
</div>
<div class="cr-pkg-group-body open" id="${gid}">`;
            pkgLinks.forEach(link => { html += this._renderProcCard(link); });
            html += '</div></div>';
        }
        html += '</div>';

        if ((r.routingNodes||[]).length)  html += this._renderRoutingNodes(r.routingNodes);
        if ((r.infraNodes||[]).length)    html += this._renderInfraNodes(r.infraNodes);

        // ── Low-confidence hints (collapsed by default) ───────────────────────
        if (hints.length) {
            html += `<div class="cr-section">
<details>
<summary class="cr-gap-summary-hdr" style="cursor:pointer">
  <span style="color:var(--text-muted);font-weight:700">Low-Confidence Hints (${hints.length})</span>
  <span style="font-size:.78rem;color:var(--text-muted);margin-left:8px">Fuzzy matches and zone-inherited links — not confirmed</span>
</summary>
<table class="cr-table" style="margin-top:8px"><thead><tr><th>Class</th><th>Method</th><th>Inferred Oracle</th><th>Zone</th></tr></thead><tbody>`;
            hints.forEach(h => {
                const cls = (h.javaClass||'').split('.').pop();
                html+=`<tr><td><code>${esc(cls)}</code></td><td>${esc(h.javaMethod||'')}</td><td><code style="font-size:.78rem">${esc(h.resolvedProcRef||'-')}</code></td><td><span style="font-size:.72rem;color:var(--text-muted)">${esc(h.zoneSource||'-')}</span></td></tr>`;
            });
            html+='</tbody></table></details></div>';
        }

        html += `<div id="${botId}" style="padding:8px 0;font-size:.75rem;color:var(--text-muted);text-align:center">
  — End of report —
  <button onclick="document.getElementById('${topId}').scrollIntoView({behavior:'smooth'})" style="margin-left:12px;font-size:.75rem;padding:2px 8px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg);cursor:pointer">↑ Top</button>
</div>`;

        html += '</div>';
        return html;
    },

    _stat(val, lbl, color) {
        const c = color||'var(--blue)';
        return `<div class="cr-stat"><div class="cr-stat-val" style="color:${c}">${val!=null?val:0}</div><div class="cr-stat-lbl">${lbl}</div></div>`;
    },


    _buildClaudePanel(r) {
        const esc = JA.utils.escapeHtml;
        const links = r.procLinks||[];
        const javaOnly = r.javaOnlyProcs||[];
        const oracleOnly = r.oracleOnlyProcs||[];
        const totalItems = links.length + javaOnly.length + oracleOnly.length + 1; // +1 overall

        const linksDone   = links.filter(l=>l.claudeDone).length;
        const josDone     = javaOnly.filter(j=>j.claudeDone).length;
        const oosDone     = oracleOnly.filter(o=>o.claudeDone).length;
        const overallDone = r.claudeOverallVerdict ? 1 : 0;
        const totalDone   = linksDone + josDone + oosDone + overallDone;

        const converted    = links.filter(l=>l.claudeVerdict==='CONVERTED').length;
        const partial      = links.filter(l=>l.claudeVerdict==='PARTIAL').length;
        const notConverted = links.filter(l=>l.claudeVerdict==='NOT_CONVERTED').length;
        const pct = totalItems > 0 ? Math.round(totalDone/totalItems*100) : 0;
        const isRunning = this._claudeRunning;
        const hasResults = totalDone > 0;

        const ovVerdictColors = {
            'FULLY_CONVERTED':'var(--green)','MOSTLY_CONVERTED':'var(--blue)',
            'PARTIAL':'var(--orange)','SIGNIFICANT_GAPS':'var(--red)'
        };
        const ovColor = ovVerdictColors[r.claudeOverallVerdict] || 'var(--sidebar-hover)';

        return `<div class="cr-claude-panel" id="cr-claude-panel">
<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
  <span style="font-weight:700;font-size:.9rem;color:var(--accent-hover)">🤖 Claude Full Analysis</span>
  ${!isRunning
    ? `<button class="cr-btn" onclick="JA.crossRef._runClaudeEnrichment('${esc(r.crossRefId)}')"
         style="font-size:.78rem;padding:4px 14px;background:var(--accent-hover);color:var(--text-inverse);border-color:var(--accent-hover);font-weight:600">
         ${hasResults ? 'Re-run Analysis' : 'Run Full Analysis'} (${totalItems} items)
       </button>
       <button class="cr-btn cr-btn-sec" onclick="JA.crossRef._mergeClaudeResults('${esc(r.crossRefId)}')"
         title="Re-apply existing chunk files to result (use if verdicts are missing after a completed run)"
         style="font-size:.78rem;padding:4px 10px">⟳ Reload Results</button>`
    : `<button class="cr-btn cr-btn-danger" onclick="JA.crossRef._cancelClaudeEnrichment('${esc(r.crossRefId)}')"
         style="font-size:.78rem;padding:4px 14px">Cancel</button>`
  }
  <span style="font-size:.75rem;color:var(--text-muted)">Analyzes: proc links + gaps + overall verdict</span>
</div>
${hasResults||isRunning ? `
<div style="margin-top:10px">
  <div style="display:flex;justify-content:space-between;font-size:.75rem;color:var(--text-muted);margin-bottom:3px">
    <span id="cr-claude-status-text">${isRunning ? 'Analyzing...' : totalDone+'/'+totalItems+' complete'}</span>
    <span>${pct}%</span>
  </div>
  <div style="background:var(--bg-hover);border-radius:var(--radius-sm);height:8px;overflow:hidden">
    <div id="cr-claude-bar" style="background:linear-gradient(90deg,var(--accent-hover),var(--accent-hover));height:8px;width:${pct}%;transition:width .4s"></div>
  </div>
  <div style="margin-top:8px;display:flex;gap:16px;flex-wrap:wrap;font-size:.78rem">
    <span style="color:var(--badge-green);font-weight:600">✓ CONVERTED: ${converted}</span>
    <span style="color:var(--badge-orange);font-weight:600">~ PARTIAL: ${partial}</span>
    <span style="color:var(--red);font-weight:600">✗ NOT_CONVERTED: ${notConverted}</span>
    <span style="color:var(--text-muted)">J-gaps: ${josDone}/${javaOnly.length} | O-gaps: ${oosDone}/${oracleOnly.length}</span>
  </div>
</div>` : ''}
${r.claudeOverallVerdict ? `
<div style="margin-top:12px;padding:10px 12px;background:var(--bg-card);border:2px solid ${ovColor};border-radius:var(--radius-md)">
  <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
    <span style="font-weight:700;font-size:.9rem;color:${ovColor}">${esc(r.claudeOverallVerdict)}</span>
    <span style="background:${ovColor};color:var(--text-inverse);font-size:.72rem;padding:1px 7px;border-radius:var(--radius-lg)">${r.claudeConversionPct||0}% converted</span>
    <span style="font-size:.75rem;color:var(--text-muted)">(${esc(r.claudeOverallConfidence||'')} confidence)</span>
  </div>
  <div style="margin-top:5px;font-size:.8rem;color:var(--text-secondary)">${esc(r.claudeOverallReason||'')}</div>
  ${(r.claudeOverallGaps||[]).length ? `
  <div style="margin-top:6px;font-size:.77rem;color:var(--badge-orange)">
    <strong>Key gaps:</strong> ${(r.claudeOverallGaps||[]).map(g=>`<span style="background:var(--badge-orange-bg);padding:1px 5px;border-radius:var(--radius-sm);margin-right:3px">${esc(g)}</span>`).join('')}
  </div>` : ''}
</div>` : ''}
</div>`;
    },

    _runClaudeEnrichment(crossRefId) {
        this._claudeRunning = true;
        // Re-render the panel
        const panelEl = document.getElementById('cr-claude-panel');
        if (panelEl && this._savedResult) panelEl.outerHTML = this._buildClaudePanel(this._savedResult);

        const jarId = this._jarId;
        const url = `/api/jar/jars/${encodeURIComponent(jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}/claude/run`;
        const evtSource = new EventSource(url);
        // POST via fetch (EventSource is GET — use fetch with SSE manually for POST)
        evtSource.close();

        // Use fetch + ReadableStream to POST and receive SSE
        fetch(url, { method: 'POST', headers: { 'Accept': 'text/event-stream' } })
            .then(resp => {
                const reader = resp.body.getReader();
                const decoder = new TextDecoder();
                const read = () => reader.read().then(({ done, value }) => {
                    if (done) {
                        this._claudeRunning = false;
                        this._reloadResultAndRefresh(crossRefId);
                        return;
                    }
                    const text = decoder.decode(value, { stream: true });
                    const lines = text.split('\n');
                    for (const line of lines) {
                        if (line.startsWith('data:')) {
                            try {
                                const data = JSON.parse(line.slice(5).trim());
                                this._updateClaudeProgress(data);
                                if (data.status === 'COMPLETE' || data.status === 'CANCELLED') {
                                    this._claudeRunning = false;
                                    this._reloadResultAndRefresh(crossRefId);
                                }
                            } catch (e) {}
                        }
                        if (line.startsWith('event: done')) {
                            this._claudeRunning = false;
                        }
                    }
                    read();
                }).catch(() => { this._claudeRunning = false; });
                read();
            })
            .catch(e => {
                this._claudeRunning = false;
                alert('Failed to start Claude analysis: ' + e.message);
            });
    },

    _cancelClaudeEnrichment(crossRefId) {
        this._claudeRunning = false;
        fetch(`/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}/claude/cancel`,
              { method: 'DELETE' }).catch(()=>{});
        const panelEl = document.getElementById('cr-claude-panel');
        if (panelEl && this._savedResult) panelEl.outerHTML = this._buildClaudePanel(this._savedResult);
    },

    _mergeClaudeResults(crossRefId) {
        const url = `/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}/claude/merge`;
        fetch(url, { method: 'POST' })
            .then(r => r.json())
            .then(data => {
                if (data.error) { alert('Merge failed: ' + data.error); return; }
                // Reload result and refresh panel
                this._reloadResultAndRefresh(crossRefId);
            })
            .catch(e => alert('Merge failed: ' + e.message));
    },

    _updateClaudeProgress(data) {
        const bar = document.getElementById('cr-claude-bar');
        const statusText = document.getElementById('cr-claude-status-text');
        if (!bar || !statusText) return;
        const done = data.completed || 0;
        const total = data.total || 1;
        const pct = Math.round(done/total*100);
        bar.style.width = pct + '%';
        statusText.textContent = `${done}/${total} (${pct}%)`;
    },

    _reloadResultAndRefresh(crossRefId) {
        fetch(`/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}`)
            .then(r => r.json())
            .then(result => {
                this._savedResult = result;
                const body = document.getElementById('cr-subtab-body');
                if (body) body.innerHTML = this._buildSubTabContent(result);
            }).catch(()=>{});
    },

    _renderProcCard(link) {
        const esc = JA.utils.escapeHtml;
        const simpleClass = (link.javaClass||'').split('.').pop();
        const confClass = link.matchConfidence==='HIGH'?'cr-badge-high':link.matchConfidence==='MEDIUM'?'cr-badge-med':'cr-badge-low';
        const cardId = 'crc-'+Math.random().toString(36).slice(2);
        const zoneColors = {'PASS_A':'var(--blue)','PASS_B':'var(--accent-hover)','PASS_A+B':'var(--teal)','PASS_C':'var(--orange)','PASS_D_INHERITED':'var(--bg-inset)'};
        const zSrc = link.zoneSource||'';
        const zBadge = zSrc?`<span style="background:${zoneColors[zSrc]||'var(--bg-inset)'};color: var(--text);font-size:.7rem;padding:1px 5px;border-radius:var(--radius-sm);font-family:monospace">${esc(zSrc)}</span>`:'';
        const lpBadge = link.hasLogParameters?`<span title="@LogParameters" style="color:var(--badge-orange);font-size:.75rem">@LP</span>`:'';

        // Claude verdict badge
        const vColors = {'CONVERTED':'var(--green)','PARTIAL':'var(--orange)','NOT_CONVERTED':'var(--red)','NO_ORACLE_EQUIVALENT':'var(--bg-inset)'};
        const claudeBadge = link.claudeDone && link.claudeVerdict
            ? `<span title="${esc(link.claudeReason||'')}" style="background:${vColors[link.claudeVerdict]||'var(--bg-inset)'};color: var(--text);font-size:.68rem;padding:1px 5px;border-radius:var(--radius-sm);cursor:pointer" onclick="event.stopPropagation();JA.crossRef._showChunkDetail('${esc(link.claudeChunkId||'')}','${esc(this._savedActiveId||'')}')">AI:${esc(link.claudeVerdict)}</span>`
            : '';

        const nodeIdSafe = esc(link.javaNodeId || (simpleClass + '#' + (link.javaMethod||'')));
        const procRefSafe = esc(link.resolvedProcRef||link.inferredProcRef||'');
        const crossRefIdSafe = esc(this._savedActiveId||'');
        const srcBtn = `<button onclick="event.stopPropagation();JA.crossRef._openSourceTree('${nodeIdSafe}','${procRefSafe}')" title="Open in Source Tree tab" style="font-size:.72rem;padding:1px 6px;border:1px solid var(--accent-subtle);border-radius:var(--radius-sm);background:var(--accent-subtle);color:var(--accent-hover);cursor:pointer;flex-shrink:0">🌳 Tree</button>`;

        let html=`<div class="cr-proc-card">
<div class="cr-proc-card-hdr" onclick="document.getElementById('${cardId}').classList.toggle('open')">
  <h4>${esc(simpleClass)}.${esc(link.javaMethod||'')}</h4>
  <span class="cr-badge ${confClass}">${link.matchConfidence}</span>
  ${zBadge}${lpBadge}${claudeBadge}
  ${srcBtn}
  <code style="font-size:.78rem;color:var(--blue)">${esc(link.resolvedProcRef||link.inferredProcRef||'-')}</code>
  ${link.plsqlNodeFound?'<span style="color:var(--badge-green);font-size:.8rem">✓</span>':''}
  <span style="color:var(--text-muted);font-size:.8rem">▼</span>
</div>
<div class="cr-proc-card-body" id="${cardId}">`;

        if ((link.tableMatches||[]).length) {
            html+=`<table class="cr-table" style="margin-bottom:10px"><thead><tr><th>Collection (Java)</th><th>Oracle Table</th><th>Java Op</th><th>Oracle Op</th><th>Status</th></tr></thead><tbody>`;
            link.tableMatches.forEach(tm => {
                const stClass = tm.matchStatus==='MATCHED'?'cr-match':tm.matchStatus==='INFERRED'?'cr-inferred':tm.matchStatus==='JAVA_ONLY'?'cr-java-only':tm.matchStatus==='ORACLE_ONLY'?'cr-oracle-only':'cr-mismatch';
                const icon = tm.matchStatus==='MATCHED'?'✓':tm.matchStatus==='INFERRED'?'~':'⚠';
                html+=`<tr><td><code>${esc(tm.mongoCollection||'-')}</code></td><td><code>${esc(tm.oracleTable||'-')}</code></td><td>${esc(tm.javaOperation||'-')}</td><td>${esc(tm.oracleOperation||'-')}</td><td class="${stClass}">${icon} ${esc(tm.matchStatus||'-')}</td></tr>`;
            });
            html+='</tbody></table>';
        }

        const params = (link.parameterMatches||[]).slice(0,20);
        if (params.length) {
            html+=`<details style="margin-bottom:8px"><summary style="font-size:.8rem;cursor:pointer;color:var(--text-secondary)">Parameters (${params.length})</summary>
<table class="cr-table" style="margin-top:6px"><thead><tr><th>Java Literal</th><th>Oracle Param</th><th>Status</th></tr></thead><tbody>`;
            params.forEach(pm => {
                const st=pm.matched===null?'INFERRED':pm.matched?'MATCHED':'NOT_FOUND';
                const sc=pm.matched===null?'cr-inferred':pm.matched?'cr-match':'cr-java-only';
                html+=`<tr><td><code>${esc(pm.javaLiteral)}</code></td><td><code>${esc(pm.oracleParam)}</code></td><td class="${sc}">${st}</td></tr>`;
            });
            html+='</tbody></table></details>';
        }

        // Claude verdict detail (if done)
        if (link.claudeDone && link.claudeVerdict) {
            const vColors = {'CONVERTED':'var(--green)','PARTIAL':'var(--orange)','NOT_CONVERTED':'var(--red)','NO_ORACLE_EQUIVALENT':'var(--bg-inset)'};
            const vc = vColors[link.claudeVerdict]||'var(--sidebar-hover)';
            html+=`<div style="margin-top:8px;padding:8px 10px;background:var(--accent-subtle);border:1px solid var(--accent-subtle);border-radius:var(--radius);font-size:.8rem">
<div style="font-weight:700;color:${vc};margin-bottom:4px">Claude: ${esc(link.claudeVerdict)} <span style="color:var(--text-muted);font-weight:400">(${esc(link.claudeConfidence||'')})</span></div>
<div style="color:var(--text-secondary);margin-bottom:4px">${esc(link.claudeReason||'')}</div>`;
            if ((link.claudeGaps||[]).length) {
                html+=`<div style="color:var(--badge-orange);font-size:.77rem">Gaps: ${(link.claudeGaps||[]).map(g=>'• '+esc(g)).join('  ')}</div>`;
            }
            if (link.claudeChunkId) {
                html+=`<a href="#" onclick="event.preventDefault();JA.crossRef._showChunkDetail('${esc(link.claudeChunkId)}','${esc(this._savedActiveId||'')}');" style="font-size:.75rem;color:var(--text-muted);margin-top:4px;display:inline-block">View full Claude prompt/response →</a>`;
            }
            html+='</div>';
        }

        html+='</div></div>';
        return html;
    },

    _showChunkDetail(chunkId, crossRefId) {
        if (!chunkId || !crossRefId) return;
        const url = `/api/jar/jars/${encodeURIComponent(this._jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}/claude/chunk/${encodeURIComponent(chunkId)}`;
        fetch(url).then(r=>r.json()).then(data => {
            const esc = JA.utils.escapeHtml;
            let existing = document.getElementById('cr-chunk-modal');
            if (!existing) {
                existing = document.createElement('div');
                existing.id = 'cr-chunk-modal';
                existing.style.cssText = 'position:fixed;inset:0;background:var(--bg-overlay);z-index:9999;display:flex;align-items:center;justify-content:center';
                document.body.appendChild(existing);
            }
            existing.innerHTML = `<div style="background:var(--bg-card);border-radius:var(--radius-lg);max-width:800px;width:95vw;max-height:90vh;overflow:auto;padding:20px;position:relative">
<button onclick="document.getElementById('cr-chunk-modal').remove()" style="position:absolute;top:10px;right:14px;background:none;border:none;font-size:1.2rem;cursor:pointer;color:var(--text-muted)">✕</button>
<h3 style="margin:0 0 12px;font-size:.95rem">Claude Chunk Detail — ${esc(chunkId)}</h3>
<div style="margin-bottom:8px"><strong>Verdict:</strong> <span style="color:var(--accent-hover)">${esc(data.verdict||'-')}</span> | <strong>Confidence:</strong> ${esc(data.confidence||'-')} | <strong>Time:</strong> ${data.durationMs||0}ms</div>
<div style="margin-bottom:8px"><strong>Reason:</strong> ${esc(data.reason||'-')}</div>
${(data.gaps||[]).length?`<div style="margin-bottom:8px"><strong>Gaps:</strong> ${(data.gaps||[]).map(g=>`<span style="background:var(--badge-orange-bg);padding:1px 5px;border-radius:var(--radius-sm);font-size:.78rem;margin-right:4px">${esc(g)}</span>`).join('')}</div>`:''}
<details style="margin-bottom:8px"><summary style="cursor:pointer;font-weight:600;font-size:.82rem">Prompt</summary>
<pre style="font-size:.72rem;background:var(--bg);padding:10px;border-radius:var(--radius-sm);overflow:auto;max-height:300px;white-space:pre-wrap;word-break:break-word">${esc(data.prompt||'')}</pre>
</details>
<details open><summary style="cursor:pointer;font-weight:600;font-size:.82rem">Response</summary>
<pre style="font-size:.72rem;background:var(--bg);padding:10px;border-radius:var(--radius-sm);overflow:auto;max-height:300px;white-space:pre-wrap;word-break:break-word">${esc(data.response||'')}</pre>
</details>
</div>`;
        }).catch(e => alert('Could not load chunk detail: ' + e.message));
    },

    // ── Source Tree sub-tab ───────────────────────────────────────────────────

    /** Called from any ⬡/🌳 button — stores selection then switches to Source Tree tab. */
    _openSourceTree(javaNodeId, procRef) {
        this._treeJavaNodeId = javaNodeId || null;
        this._treeProcRef    = procRef    || null;
        this._showSubTab('tree');
    },

    /** Build the Source Tree sub-tab: 3-pane inline view (no modal). */
    _buildSourceTreeTab(r) {
        const esc = JA.utils.escapeHtml;
        const jarId   = this._jarId;
        const activeId = r.crossRefId || this._savedActiveId || '';
        const javaNodeId = this._treeJavaNodeId || '';
        const anchorProc = this._treeProcRef || '';

        // Proc picker: let user jump to any proc link directly
        const links = r.procLinks || [];
        const pickerOptions = links.map((l,i) => {
            const cls = (l.javaClass||'').split('.').pop();
            const ref = l.resolvedProcRef || l.inferredProcRef || '';
            const selected = ref === anchorProc ? ' selected' : '';
            return `<option value="${esc(i)}"${selected}>${esc(cls+'.'+l.javaMethod)} ↔ ${esc(ref)}</option>`;
        }).join('');

        return `
<div class="cr-tree-tab-wrap">
  <!-- toolbar -->
  <div class="cr-tree-toolbar">
    <span style="font-weight:700;font-size:.88rem;color:var(--text)">🌳 Source Tree</span>
    ${anchorProc
      ? `<code style="font-size:.76rem;color:var(--badge-orange);background:var(--badge-orange-bg);padding:2px 8px;border-radius:var(--radius-sm);margin-left:6px">${esc(anchorProc)}</code>`
      : '<span style="font-size:.78rem;color:var(--text-muted);margin-left:6px">No proc selected — pick one below or click 🌳 from a proc card</span>'}
    ${javaNodeId
      ? `<code style="font-size:.76rem;color:var(--accent-hover);background:var(--accent-subtle);padding:2px 8px;border-radius:var(--radius-sm);margin-left:4px">${esc(javaNodeId)}</code>`
      : ''}
    <select id="cr-tree-picker" onchange="JA.crossRef._treePickerChange(this.value, ${JSON.stringify(activeId)})"
      style="margin-left:auto;padding:4px 8px;border:1px solid var(--border);border-radius:var(--radius);font-size:.8rem;max-width:340px">
      <option value="">— Jump to a proc link —</option>
      ${pickerOptions}
    </select>
  </div>
  <!-- 3-pane body -->
  <div class="cr-tree-body">
    <!-- LEFT: Oracle call tree -->
    <div class="cr-tree-left">
      <div class="cr-tree-pane-hdr cr-tree-pane-hdr-oracle">
        🔷 Oracle Call Tree
        <span id="cr-tree-count" style="font-weight:400;color:var(--text-muted);font-size:.7rem;margin-left:4px"></span>
      </div>
      <div id="cr-tree-nodes" class="cr-tree-nodes">
        ${anchorProc || r.plsqlSessionId
          ? '<div style="padding:10px;color:var(--text-muted);font-size:.78rem">Loading tree…</div>'
          : '<div style="padding:10px;color:var(--text-muted);font-size:.78rem">No PL/SQL session. Run in STATIC_INFERENCE mode — Oracle tree not available.</div>'}
      </div>
    </div>
    <!-- CENTER: Oracle SQL -->
    <div class="cr-tree-center">
      <div class="cr-tree-pane-hdr cr-tree-pane-hdr-oracle" id="cr-tree-oracle-hdr">
        🔷 Oracle SQL
        <span id="cr-tree-oracle-meta" style="font-weight:400;font-size:.7rem;color:var(--text-muted);margin-left:6px"></span>
      </div>
      <div id="cr-tree-oracle-src" class="cr-tree-src-body cr-tree-src-oracle">
        <span class="cr-tree-hint">← Click a node in the tree to view its SQL</span>
      </div>
    </div>
    <!-- RIGHT: Java source -->
    <div class="cr-tree-right">
      <div class="cr-tree-pane-hdr cr-tree-pane-hdr-java" id="cr-tree-java-hdr">
        ☕ Java Source
        <span id="cr-tree-java-meta" style="font-weight:400;font-size:.7rem;color:var(--text-muted);margin-left:6px"></span>
      </div>
      <div id="cr-tree-java-src" class="cr-tree-src-body cr-tree-src-java">
        ${javaNodeId
          ? '<span class="cr-tree-hint">Loading…</span>'
          : '<span class="cr-tree-hint">No Java node — click 🌳 from a proc card to load both sides.</span>'}
      </div>
    </div>
  </div>
</div>`;
    },

    /** Called after the DOM is rendered — kicks off async loads. */
    _initSourceTreeTab(r) {
        const jarId   = this._jarId;
        const activeId = r.crossRefId || this._savedActiveId || '';
        const javaNodeId = this._treeJavaNodeId || '';
        const anchorProc = this._treeProcRef || '';

        if (r.plsqlSessionId || anchorProc) {
            this._svLoadTree(jarId, activeId, anchorProc, 'cr-tree');
        }
        if (javaNodeId && activeId) {
            this._svLoadJavaInline(jarId, activeId, javaNodeId);
        }
    },

    /** Navigate to a proc link from the dropdown picker. */
    _treePickerChange(idxStr, activeId) {
        if (!idxStr || !this._savedResult) return;
        const links = this._savedResult.procLinks || [];
        const link = links[parseInt(idxStr)];
        if (!link) return;
        const cls = (link.javaClass||'').split('.').pop();
        this._treeJavaNodeId = link.javaNodeId || (cls + '#' + link.javaMethod);
        this._treeProcRef    = link.resolvedProcRef || link.inferredProcRef || '';
        // Re-render the tab so toolbar updates, then init loads
        const body = document.getElementById('cr-subtab-body');
        if (body) {
            body.innerHTML = this._buildSubTabContent(this._savedResult);
            this._initSourceTreeTab(this._savedResult);
        }
    },

    /** Load Java source into the inline tree tab right pane. */
    _svLoadJavaInline(jarId, activeId, javaNodeId) {
        const javaBody = document.getElementById('cr-tree-java-src');
        const javaMeta = document.getElementById('cr-tree-java-meta');
        if (!javaBody) return;
        fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/cross-ref/result/${encodeURIComponent(activeId)}/source/java?nodeId=${encodeURIComponent(javaNodeId)}`)
            .then(r => r.json())
            .then(data => {
                if (!javaBody) return;
                if (data.error) { javaBody.textContent = '// Not found: ' + data.error; return; }
                if (javaMeta) javaMeta.textContent = `${data.stereotype||''} · ${data.operationType||''}`;
                const meta = `// ═══ ${data.className||javaNodeId} ═══\n// ${data.signature||''}\n// Stereotype: ${data.stereotype||'-'} | Op: ${data.operationType||'-'}\n\n`;
                javaBody.innerHTML = this._syntaxHighlightJava(meta + (data.sourceCode || '// (no source code available)'));
            })
            .catch(e => { if (javaBody) javaBody.textContent = '// Error: ' + e.message; });
    },

    // ── Tree Source Viewer (modal — kept for Search+Source tab) ──────────────
    // Three-pane modal:
    //   LEFT  — Oracle call tree (all nodes, depth-indented, clickable)
    //   CENTER — Oracle SQL source for selected node
    //   RIGHT  — Java source for the matched Java node
    //
    // State stored directly on the modal element so re-renders don't lose it.

    _openSourceViewer(javaNodeId, oracleProcRef, crossRefId) {
        const esc = JA.utils.escapeHtml;
        const jarId  = this._jarId;
        const activeId = crossRefId || this._savedActiveId || '';

        let modal = document.getElementById('cr-source-modal');
        if (modal) modal.remove();
        modal = document.createElement('div');
        modal.id = 'cr-source-modal';
        modal.style.cssText = 'position:fixed;inset:0;background:var(--bg-overlay);z-index:9999;display:flex;align-items:stretch;justify-content:center;padding:16px;box-sizing:border-box';
        document.body.appendChild(modal);
        modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); }, { once: true });

        // ── shell ──────────────────────────────────────────────────────────
        modal.innerHTML = `
<div id="cr-sv-wrap" style="background:var(--bg-card);border-radius:var(--radius-lg);width:100%;max-width:1600px;display:flex;flex-direction:column;overflow:hidden">
  <!-- header -->
  <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-bottom:1px solid var(--border);flex-shrink:0;background:var(--bg)">
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <span style="font-weight:700;font-size:.92rem;color:var(--text)">⬡ Source Tree Viewer</span>
      ${javaNodeId ? `<code id="cr-sv-java-label" style="font-size:.76rem;color:var(--accent-hover);background:var(--accent-subtle);padding:2px 7px;border-radius:var(--radius-sm)">${esc(javaNodeId)}</code>` : ''}
      <span style="color: var(--text-secondary)">↔</span>
      <code id="cr-sv-oracle-label" style="font-size:.76rem;color:var(--badge-orange);background:var(--badge-orange-bg);padding:2px 7px;border-radius:var(--radius-sm)">${esc(oracleProcRef||'(no proc)')}</code>
    </div>
    <button onclick="document.getElementById('cr-source-modal').remove()" style="background:none;border:none;font-size:1.4rem;cursor:pointer;color:var(--text-muted);padding:0 4px">✕</button>
  </div>
  <!-- 3-pane body -->
  <div style="display:flex;flex:1;overflow:hidden;min-height:0;height:calc(100vh - 110px)">
    <!-- LEFT: Oracle call tree -->
    <div id="cr-sv-tree-col" style="width:270px;min-width:200px;flex-shrink:0;display:flex;flex-direction:column;border-right:2px solid var(--border);background:var(--bg-card)">
      <div style="padding:6px 10px;background:var(--badge-orange-bg);font-size:.75rem;font-weight:700;color:var(--badge-orange);border-bottom:1px solid var(--border);flex-shrink:0">
        🔷 Oracle Call Tree
        <span id="cr-sv-tree-count" style="font-weight:400;color:var(--text-muted);margin-left:4px"></span>
      </div>
      <div id="cr-sv-tree-body" style="flex:1;overflow-y:auto;overflow-x:hidden;padding:6px 0">
        <div style="padding:8px 12px;color:var(--text-muted);font-size:.78rem">Loading tree…</div>
      </div>
    </div>
    <!-- CENTER: Oracle SQL source -->
    <div style="flex:1;display:flex;flex-direction:column;border-right:2px solid var(--border);min-width:0">
      <div id="cr-sv-oracle-hdr" style="padding:6px 12px;background:var(--badge-orange-bg);font-size:.75rem;font-weight:700;color:var(--badge-orange);border-bottom:1px solid var(--border);flex-shrink:0;display:flex;align-items:center;gap:8px">
        🔷 Oracle SQL
        <span id="cr-sv-oracle-meta" style="font-weight:400;font-size:.72rem;color:var(--text-muted)"></span>
      </div>
      <div id="cr-sv-oracle-body" style="flex:1;overflow:auto;padding:12px 14px;font-family:monospace;font-size:.74rem;white-space:pre;line-height:1.6;color:var(--text);background:var(--badge-orange-bg)">
        <span style="color:var(--text-muted)">Select a node in the tree →</span>
      </div>
    </div>
    <!-- RIGHT: Java source -->
    <div style="flex:1;display:flex;flex-direction:column;min-width:0">
      <div id="cr-sv-java-hdr" style="padding:6px 12px;background:var(--accent-subtle);font-size:.75rem;font-weight:700;color:var(--accent-hover);border-bottom:1px solid var(--border);flex-shrink:0;display:flex;align-items:center;gap:8px">
        ☕ Java Source
        <span id="cr-sv-java-meta" style="font-weight:400;font-size:.72rem;color:var(--text-muted)"></span>
      </div>
      <div id="cr-sv-java-body" style="flex:1;overflow:auto;padding:12px 14px;font-family:monospace;font-size:.74rem;white-space:pre;line-height:1.6;color:var(--text);background:var(--badge-blue-bg)">
        ${javaNodeId ? '<span style="color:var(--text-muted)">Loading…</span>' : '<span style="color:var(--text-muted)">(no Java node matched)</span>'}
      </div>
    </div>
  </div>
</div>`;

        // ── load Java source immediately ───────────────────────────────────
        if (javaNodeId && activeId) {
            this._svLoadJava(jarId, activeId, javaNodeId);
        }

        // ── load Oracle tree, then auto-select the anchor proc ────────────
        this._svLoadTree(jarId, activeId, oracleProcRef);
    },

    /**
     * Fetch full Oracle call tree and render it.
     * px = DOM id prefix: 'cr-sv' for modal, 'cr-tree' for inline tab.
     */
    _svLoadTree(jarId, activeId, anchorProcRef, px) {
        px = px || 'cr-sv';
        const treeBody = document.getElementById(px + '-' + (px==='cr-sv'?'tree-body':'nodes'));
        const countEl  = document.getElementById(px + '-' + (px==='cr-sv'?'tree-count':'count'));
        if (!treeBody) return;

        fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/cross-ref/result/${encodeURIComponent(activeId)}/source/oracle-tree`)
            .then(r => r.json())
            .then(data => {
                const nodes = data.nodes || [];
                if (nodes.length === 0) {
                    treeBody.innerHTML = '<div style="padding:10px 12px;color:var(--text-muted);font-size:.78rem">No Oracle nodes available.</div>';
                    return;
                }
                if (countEl) countEl.textContent = `(${nodes.length} nodes)`;

                // Build parent→children map
                const byId = {};
                nodes.forEach(n => { byId[n.nodeId] = n; });
                const children = {};
                const roots = [];
                nodes.forEach(n => {
                    if (n.parentNodeId && byId[n.parentNodeId]) {
                        (children[n.parentNodeId] = children[n.parentNodeId] || []).push(n);
                    } else {
                        roots.push(n);
                    }
                });

                // Group all nodes by package for package-header rows
                const byPackage = {};
                nodes.forEach(n => {
                    const pkg = n.packageName || '';
                    if (pkg) (byPackage[pkg] = byPackage[pkg] || []).push(n);
                });

                // Sort children by depth then name
                const sortNodes = arr => arr.sort((a,b) => (a.depth-b.depth) || (a.name||'').localeCompare(b.name||''));
                sortNodes(roots);
                Object.values(children).forEach(sortNodes);

                const esc = JA.utils.escapeHtml;
                const depthColors = ['var(--blue)','var(--accent-hover)','var(--orange)','var(--blue)','var(--green)','var(--bg-inset)'];
                const oracleBodyId = px === 'cr-sv' ? 'cr-sv-oracle-body' : 'cr-tree-oracle-src';
                const oracleMetaId = px === 'cr-sv' ? 'cr-sv-oracle-meta' : 'cr-tree-oracle-meta';

                const renderNode = (node, expanded) => {
                    const kids = children[node.nodeId] || [];
                    const hasKids = kids.length > 0;
                    const depth = node.depth || 0;
                    const indent = depth * 14;
                    const procRef = node.packageName ? `${node.packageName}.${node.objectName}` : (node.objectName||node.name||'');
                    const isAnchor = anchorProcRef && procRef.toUpperCase() === anchorProcRef.toUpperCase();
                    const color = depthColors[Math.min(depth, depthColors.length-1)];
                    const typeIcon = node.objectType === 'FUNCTION' ? 'ƒ' : node.objectType === 'PROCEDURE' ? '⚙' : '◈';
                    const expandId = px + '-kids-' + node.nodeId.replace(/[^a-z0-9]/gi,'_');

                    let html = `<div class="cr-sv-node${isAnchor?' cr-sv-node-anchor':''}" data-procref="${esc(procRef)}"
  style="display:flex;align-items:flex-start;padding:4px 8px 4px ${indent+8}px;cursor:pointer;border-left:3px solid ${isAnchor?color:'transparent'};background:${isAnchor?color+'12':'transparent'}"
  onclick="JA.crossRef._svSelectNode(this,'${esc(procRef).replace(/'/g,"\\'")}','${esc(activeId)}','${esc(jarId)}','${px}')">
  <span style="color:${color};font-size:.7rem;margin-right:4px;flex-shrink:0;margin-top:1px">${typeIcon}</span>
  <span style="flex:1;min-width:0">
    <span style="font-size:.73rem;font-weight:${isAnchor?700:400};color:${isAnchor?color:'var(--text-secondary)'};word-break:break-all">${esc(node.objectName||node.name||'-')}</span>
    ${node.packageName ? `<span style="font-size:.65rem;color:var(--text-muted);display:block">${esc(node.packageName)}</span>` : ''}
    <span style="font-size:.63rem;color:var(--text-muted)">d:${depth} · ${node.linesOfCode||0}loc${(node.tables||[]).length?' · '+node.tables.length+'tbl':''}</span>
  </span>
  ${hasKids ? `<span style="color:var(--text-muted);font-size:.7rem;flex-shrink:0;margin-top:2px" id="${esc(expandId)}-icon">▶</span>` : ''}
</div>`;

                    if (hasKids) {
                        html += `<div id="${esc(expandId)}" style="display:${expanded?'block':'none'}">`;
                        kids.forEach(k => { html += renderNode(k, false); });
                        html += '</div>';
                    }
                    return html;
                };

                // Inject package group headers before first node of each package
                const renderedPkgs = new Set();
                const renderNodeWithPkgHeader = (node, expanded) => {
                    let html = '';
                    const pkg = node.packageName || '';
                    if (pkg && !renderedPkgs.has(pkg)) {
                        renderedPkgs.add(pkg);
                        const pkgNodes = byPackage[pkg] || [];
                        const fnCount = pkgNodes.filter(n => n.objectType === 'FUNCTION').length;
                        const pcCount = pkgNodes.filter(n => n.objectType === 'PROCEDURE').length;
                        const pkgId = px + '-pkg-' + pkg.replace(/[^a-z0-9]/gi,'_');
                        html += `<div class="cr-sv-node cr-sv-pkg-hdr" data-pkg="${esc(pkg)}"
  style="display:flex;align-items:center;padding:5px 8px;cursor:pointer;background:var(--badge-blue-bg);border-left:3px solid var(--badge-blue);border-top:1px solid var(--border);margin-top:2px"
  onclick="JA.crossRef._svShowPackageAnalysis('${esc(pkg).replace(/'/g,"\\'")}','${esc(oracleBodyId)}','${esc(oracleMetaId)}','${esc(px)}')">
  <span style="color:var(--badge-blue);font-size:.75rem;margin-right:5px">📦</span>
  <span style="flex:1;min-width:0">
    <span style="font-size:.75rem;font-weight:700;color:var(--badge-blue)">${esc(pkg)}</span>
    <span style="font-size:.63rem;color:var(--text-muted);margin-left:6px">${fnCount} fn · ${pcCount} proc · ${pkgNodes.length} nodes</span>
  </span>
  <span style="font-size:.63rem;background:var(--badge-blue-bg);color:var(--badge-blue);padding:1px 5px;border-radius:var(--radius-sm);flex-shrink:0">Analyse →</span>
</div>`;
                    }
                    html += renderNode(node, expanded);
                    return html;
                };

                // Store nodes/children in instance for package analysis clicks
                this._treeNodesCache = { byId, children, byPackage };

                let treeHtml = '';
                roots.forEach(r => { treeHtml += renderNodeWithPkgHeader(r, true); });
                treeBody.innerHTML = treeHtml;

                // Add expand/collapse toggle on arrow click (don't trigger node select)
                treeBody.addEventListener('click', e => {
                    const iconEl = e.target.closest('[id$="-icon"]');
                    if (!iconEl) return;
                    const expandId = iconEl.id.replace('-icon','');
                    const container = document.getElementById(expandId);
                    if (!container) return;
                    e.stopPropagation();
                    const open = container.style.display !== 'none';
                    container.style.display = open ? 'none' : 'block';
                    iconEl.textContent = open ? '▶' : '▼';
                });

                // Auto-select anchor proc
                if (anchorProcRef) {
                    const all = treeBody.querySelectorAll('[data-procref]');
                    for (const el of all) {
                        if ((el.getAttribute('data-procref')||'').toUpperCase() === anchorProcRef.toUpperCase()) {
                            this._svSelectNode(el, anchorProcRef, activeId, jarId, px);
                            el.scrollIntoView({ block: 'center' });
                            // Expand its parent chain
                            let parent = el.parentElement;
                            while (parent && parent !== treeBody) {
                                if (parent.id && parent.id.includes('cr-sv-kids-')) {
                                    parent.style.display = 'block';
                                    const icon = document.getElementById(parent.id + '-icon');
                                    if (icon) icon.textContent = '▼';
                                }
                                parent = parent.parentElement;
                            }
                            break;
                        }
                    }
                }
            })
            .catch(e => {
                if (treeBody) treeBody.innerHTML = `<div style="padding:10px;color:var(--red);font-size:.78rem">Tree load error: ${e.message}</div>`;
            });
    },

    /** Called when a tree node is clicked — loads Oracle SQL into center pane.
     *  px = 'cr-sv' (modal) or 'cr-tree' (inline tab). */
    _svSelectNode(el, procRef, activeId, jarId, px) {
        px = px || 'cr-sv';
        // Highlight selected — only within the same tree container
        const treeContainer = el.closest(px==='cr-sv' ? '#cr-sv-tree-body' : '#cr-tree-nodes');
        if (treeContainer) {
            treeContainer.querySelectorAll('.cr-sv-node').forEach(n => {
                n.style.background = n.classList.contains('cr-sv-node-anchor') ? '' : 'transparent';
                n.style.fontWeight = '';
            });
        }
        el.style.background = 'var(--badge-orange-bg)';
        el.style.fontWeight = '600';

        // Update label
        const lbl = document.getElementById(px === 'cr-sv' ? 'cr-sv-oracle-label' : 'cr-tree-oracle-hdr');
        if (lbl && px === 'cr-sv') lbl.textContent = procRef;

        // Load Oracle SQL — target correct pane IDs
        const oracleBodyId = px === 'cr-sv' ? 'cr-sv-oracle-body' : 'cr-tree-oracle-src';
        const oracleMetaId = px === 'cr-sv' ? 'cr-sv-oracle-meta' : 'cr-tree-oracle-meta';
        const oracleBody = document.getElementById(oracleBodyId);
        const oracleMeta = document.getElementById(oracleMetaId);
        if (oracleBody) oracleBody.innerHTML = '<span style="color:var(--text-muted)">Loading…</span>';

        fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/cross-ref/result/${encodeURIComponent(activeId)}/source/oracle?procRef=${encodeURIComponent(procRef)}`)
            .then(r => r.json())
            .then(data => {
                if (!oracleBody) return;
                if (data.error) {
                    oracleBody.textContent = '-- Not found: ' + data.error + '\n-- Try a different node in the tree.';
                    return;
                }
                const params = (data.parameters||[]).map(p => `${p.name} ${p.direction} ${p.type||''}`.trim()).join(', ');
                const tables = (data.tables||[]).join(', ') || '—';
                if (oracleMeta) oracleMeta.textContent = `${data.objectType||''} · ${data.linesOfCode||0} lines · ${(data.tables||[]).length} tables`;
                const meta = `-- ═══ ${procRef} ═══\n-- Type: ${data.objectType||'-'} | Depth: ${data.depth||0} | LOC: ${data.linesOfCode||0}\n-- Params: ${params||'—'}\n-- Tables: ${tables}\n\n`;
                oracleBody.innerHTML = this._syntaxHighlightSQL(meta + (data.source || '-- (no SQL source available)'));
            })
            .catch(e => { if (oracleBody) oracleBody.textContent = '-- Error: ' + e.message; });
    },

    /**
     * Render full package analysis into Oracle center pane:
     * all procs/funcs with their complete call trace tree.
     * Uses cached nodes from _treeNodesCache (populated by _svLoadTree).
     */
    _svShowPackageAnalysis(pkgName, oracleBodyId, oracleMetaId, px) {
        const oracleBody = document.getElementById(oracleBodyId);
        const oracleMeta = document.getElementById(oracleMetaId);
        if (!oracleBody) return;

        const cache = this._treeNodesCache;
        if (!cache) { oracleBody.textContent = '-- Tree not loaded yet.'; return; }

        const { byId, children, byPackage } = cache;
        const pkgNodes = (byPackage[pkgName] || []).slice().sort((a,b) => {
            const typeOrder = { FUNCTION: 0, PROCEDURE: 1 };
            const tA = typeOrder[a.objectType] ?? 2;
            const tB = typeOrder[b.objectType] ?? 2;
            return tA - tB || (a.objectName||'').localeCompare(b.objectName||'');
        });

        if (pkgNodes.length === 0) {
            oracleBody.textContent = '-- No nodes found for package: ' + pkgName;
            return;
        }

        const esc = JA.utils.escapeHtml;
        const depthColors = ['var(--blue)','var(--accent-hover)','var(--orange)','var(--blue)','var(--green)','var(--bg-inset)'];

        // Recursive call-trace renderer (text, not DOM)
        const renderCallTrace = (node, indent) => {
            const typeIcon = node.objectType === 'FUNCTION' ? 'ƒ' : node.objectType === 'PROCEDURE' ? '⚙' : '◈';
            const procRef = node.packageName ? node.packageName + '.' + node.objectName : (node.objectName || node.name || '?');
            const tables = (node.tables||[]).length ? '  [tables: ' + node.tables.join(', ') + ']' : '';
            const params = (node.parameterNames||[]).length ? '(' + node.parameterNames.join(', ') + ')' : '()';
            let lines = [indent + typeIcon + ' ' + procRef + params + '  ' + (node.linesOfCode||0) + ' LOC' + tables];
            const kids = (children[node.nodeId] || []).slice().sort((a,b)=>(a.depth-b.depth)||(a.objectName||'').localeCompare(b.objectName||''));
            kids.forEach((k, i) => {
                const isLast = i === kids.length - 1;
                const branch = isLast ? '└─ ' : '├─ ';
                const childIndent = indent + (isLast ? '   ' : '│  ');
                lines = lines.concat(renderCallTrace(k, indent + branch).split('\n').map((l,li) => li===0 ? l : childIndent.slice(branch.length) + l));
            });
            return lines.join('\n');
        };

        // Build HTML sections per node
        let html = `<div style="font-family:monospace;font-size:.76rem;line-height:1.7;color:var(--text);padding:14px 16px">`;
        html += `<div style="font-size:.9rem;font-weight:700;color:var(--badge-blue);margin-bottom:12px;border-bottom:2px solid var(--badge-blue-bg);padding-bottom:6px">📦 ${esc(pkgName)} — ${pkgNodes.length} objects</div>`;

        pkgNodes.forEach((node, idx) => {
            const typeIcon = { FUNCTION: 'ƒ', PROCEDURE: '⚙', TRIGGER: '⚡', VIEW: '◎', MATERIALIZED_VIEW: '◎' }[node.objectType] || '◈';
            const color = node.objectType === 'FUNCTION' ? 'var(--accent-hover)' : 'var(--orange)';
            const tables = (node.tables||[]).length ? node.tables.join(', ') : '—';
            const params = (node.parameterNames||[]).length ? node.parameterNames.join(', ') : '—';
            const kids = children[node.nodeId] || [];

            html += `<div style="margin-bottom:16px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden">`;
            // Header
            html += `<div style="background:${JA.utils.badgePair(color).bg};border-left:4px solid ${color};padding:7px 12px;display:flex;align-items:center;gap:8px">
  <span style="color:${color};font-weight:700;font-size:.82rem">${typeIcon} ${esc(node.objectName)}</span>
  <span style="font-size:.68rem;color:var(--text-muted)">${esc(node.objectType||'')} · ${node.linesOfCode||0} LOC · depth ${node.depth||0}</span>
  ${(node.tables||[]).length ? `<span style="font-size:.66rem;background:var(--badge-orange-bg);color:var(--badge-orange);padding:1px 5px;border-radius:var(--radius-sm)">${(node.tables||[]).length} tables</span>` : ''}
</div>`;
            // Meta row
            html += `<div style="background:var(--bg);padding:5px 12px;font-size:.72rem;color:var(--text-muted);border-bottom:1px solid var(--border)">
  <span><b>Params:</b> ${esc(params)}</span>
  <span style="margin-left:12px"><b>Tables:</b> ${esc(tables)}</span>
</div>`;
            // Call trace
            if (kids.length > 0) {
                const trace = renderCallTrace(node, '');
                html += `<div style="padding:8px 12px;background:var(--bg-card);white-space:pre;overflow-x:auto;font-size:.72rem;color:var(--text-secondary);max-height:300px;overflow-y:auto">${esc(trace)}</div>`;
            } else {
                html += `<div style="padding:8px 12px;background:var(--bg-card);font-size:.71rem;color:var(--text-muted);font-style:italic">No outbound calls.</div>`;
            }
            html += `</div>`;
        });

        html += `</div>`;
        oracleBody.innerHTML = html;
        if (oracleMeta) oracleMeta.textContent = `📦 ${pkgName} · ${pkgNodes.length} objects`;

        // Highlight the package header row in the tree
        const treeContainer = document.getElementById(px === 'cr-sv' ? 'cr-sv-tree-body' : 'cr-tree-nodes');
        if (treeContainer) {
            treeContainer.querySelectorAll('.cr-sv-node').forEach(n => {
                n.style.background = 'transparent';
                n.style.fontWeight = '';
            });
            treeContainer.querySelectorAll(`.cr-sv-pkg-hdr[data-pkg="${pkgName}"]`).forEach(n => {
                n.style.background = 'var(--badge-blue-bg)';
                n.style.fontWeight = '700';
            });
        }
    },

    /** Load Java source into the right pane. */
    _svLoadJava(jarId, activeId, javaNodeId) {
        const javaBody = document.getElementById('cr-sv-java-body');
        const javaMeta = document.getElementById('cr-sv-java-meta');
        if (!javaBody) return;

        fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/cross-ref/result/${encodeURIComponent(activeId)}/source/java?nodeId=${encodeURIComponent(javaNodeId)}`)
            .then(r => r.json())
            .then(data => {
                if (!javaBody) return;
                if (data.error) { javaBody.textContent = '// Not found: ' + data.error; return; }
                if (javaMeta) javaMeta.textContent = `${data.stereotype||''} · ${data.operationType||''}`;
                const meta = `// ═══ ${data.className||javaNodeId} ═══\n// ${data.signature||''}\n// Stereotype: ${data.stereotype||'-'} | Op: ${data.operationType||'-'}\n\n`;
                javaBody.innerHTML = this._syntaxHighlightJava(meta + (data.sourceCode || '// (no source code available)'));
            })
            .catch(e => { if (javaBody) javaBody.textContent = '// Error: ' + e.message; });
    },

    _syntaxHighlightJava(code) {
        // Proper tokenizer: comments → strings → keywords/types, no double-escape issues
        const _e = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
        const KW = new Set(['public','private','protected','static','void','class','interface','extends','implements',
            'return','new','if','else','for','while','try','catch','finally','throw','throws','import','package',
            'this','super','null','true','false','final','abstract','synchronized','volatile','enum','instanceof',
            'break','continue','case','default','do','switch']);
        const TY = new Set(['String','List','Map','Set','int','long','boolean','Object','ResponseEntity',
            'Integer','Long','Boolean','Collection','Optional','ArrayList','HashMap']);
        let res = '', i = 0, n = code.length;
        while (i < n) {
            if (code[i]==='/' && code[i+1]==='/') {
                let e = code.indexOf('\n', i); if (e<0) e=n;
                res += `<span style="color:var(--text-muted);font-style:italic">${_e(code.slice(i,e))}</span>`; i=e; continue;
            }
            if (code[i]==='/' && code[i+1]==='*') {
                let e = code.indexOf('*/',i+2); e = e<0 ? n : e+2;
                res += `<span style="color:var(--text-muted);font-style:italic">${_e(code.slice(i,e))}</span>`; i=e; continue;
            }
            if (code[i]==='"') {
                let j=i+1; while(j<n){if(code[j]==='\\'){j+=2;continue;}if(code[j]==='"'){j++;break;}j++;}
                res += `<span style="color:var(--badge-green)">${_e(code.slice(i,j))}</span>`; i=j; continue;
            }
            if (code[i]==="'") {
                let j=i+1; while(j<n){if(code[j]==='\\'){j+=2;continue;}if(code[j]==="'"){j++;break;}j++;}
                res += `<span style="color:var(--badge-green)">${_e(code.slice(i,j))}</span>`; i=j; continue;
            }
            if (/[a-zA-Z_$]/.test(code[i])) {
                let j=i; while(j<n && /[a-zA-Z0-9_$]/.test(code[j]))j++;
                const w=code.slice(i,j);
                if (KW.has(w)) res += `<span style="color:var(--accent-hover);font-weight:600">${_e(w)}</span>`;
                else if (TY.has(w)) res += `<span style="color:var(--badge-blue)">${_e(w)}</span>`;
                else res += _e(w);
                i=j; continue;
            }
            res += _e(code[i]); i++;
        }
        return res;
    },

    _syntaxHighlightSQL(code) {
        const _e = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
        const KW = new Set('SELECT INSERT UPDATE DELETE FROM WHERE JOIN LEFT RIGHT INNER OUTER ON AND OR NOT IN IS NULL BEGIN END PROCEDURE FUNCTION PACKAGE DECLARE IF THEN ELSE ELSIF LOOP FOR WHILE RETURN EXCEPTION WHEN OTHERS COMMIT ROLLBACK CURSOR OPEN FETCH CLOSE TYPE RECORD TABLE INDEX BY INTO VALUES SET AS IS BODY REPLACE CREATE OR'.split(' '));
        const TY = new Set('VARCHAR2 NUMBER DATE TIMESTAMP BOOLEAN PLS_INTEGER BINARY_INTEGER SYS_REFCURSOR CHAR NVARCHAR2 CLOB BLOB INTEGER'.split(' '));
        let res = '', i = 0, n = code.length;
        while (i < n) {
            if (code[i]==='-' && code[i+1]==='-') {
                let e = code.indexOf('\n', i); if (e<0) e=n;
                res += `<span style="color:var(--text-muted);font-style:italic">${_e(code.slice(i,e))}</span>`; i=e; continue;
            }
            if (code[i]==="'" ) {
                let j=i+1; while(j<n){if(code[j]==="'"&&code[j+1]==="'"){j+=2;continue;}if(code[j]==="'"){j++;break;}j++;}
                res += `<span style="color:var(--badge-green)">${_e(code.slice(i,j))}</span>`; i=j; continue;
            }
            if (/[a-zA-Z_]/.test(code[i])) {
                let j=i; while(j<n && /[a-zA-Z0-9_]/.test(code[j]))j++;
                const w=code.slice(i,j), wu=w.toUpperCase();
                if (KW.has(wu)) res += `<span style="color:var(--badge-orange);font-weight:600">${_e(w)}</span>`;
                else if (TY.has(wu)) res += `<span style="color:var(--badge-blue)">${_e(w)}</span>`;
                else res += _e(w);
                i=j; continue;
            }
            res += _e(code[i]); i++;
        }
        return res;
    },

    // ── Sub-tab: Search + Source viewer ──────────────────────────────────────

    _buildSearchTab(r) {
        const esc = JA.utils.escapeHtml;
        const q = this._searchQuery || '';
        const links = r.procLinks || [];
        const javaOnly = r.javaOnlyProcs || [];
        const oracleOnly = r.oracleOnlyProcs || [];

        // Build unified search index
        const entries = [];
        links.forEach(l => {
            const simpleClass = (l.javaClass||'').split('.').pop();
            const jKey = simpleClass + '#' + (l.javaMethod||'');
            const oRef = l.resolvedProcRef||l.inferredProcRef||'';
            entries.push({ type:'LINK', label: jKey + ' ↔ ' + oRef, javaNodeId: jKey, procRef: oRef, link: l });
        });
        javaOnly.forEach(p => {
            const cls = (p.javaClass||'').split('.').pop();
            const jKey = cls + '#' + (p.javaMethod||'');
            entries.push({ type:'JAVA_ONLY', label: jKey + ' (Java-only)', javaNodeId: jKey, procRef: '' });
        });
        oracleOnly.forEach(p => {
            const ref = p.procRef||'';
            entries.push({ type:'ORACLE_ONLY', label: ref + ' (Oracle-only)', javaNodeId: '', procRef: ref });
        });

        const ql = q.toLowerCase();
        const filtered = ql ? entries.filter(e => e.label.toLowerCase().includes(ql)) : entries;

        const typeColor = {LINK:'var(--blue)', JAVA_ONLY:'var(--orange)', ORACLE_ONLY:'var(--accent-hover)'};
        const typeLabel = {LINK:'LINK', JAVA_ONLY:'JAVA', ORACLE_ONLY:'ORACLE'};

        const listHtml = filtered.slice(0, 200).map((e, i) => {
            const isActive = this._searchSelected === i + '|' + e.label;
            return `<div class="cr-j2o-item${isActive?' active':''}"
                        onclick="JA.crossRef._searchSelect(${i},'${esc(e.label).replace(/'/g,"\\'")}','${esc(e.javaNodeId).replace(/'/g,"\\'")}','${esc(e.procRef).replace(/'/g,"\\'")}','${esc(r.crossRefId)}')">
  <div class="cr-j2o-key" style="font-size:.79rem">${esc(e.label)}</div>
  <div class="cr-j2o-meta">
    <span style="font-size:.66rem;padding:1px 4px;border-radius:var(--radius-sm);background:${JA.utils.badgePair(typeColor[e.type]).bg};color:${JA.utils.badgePair(typeColor[e.type]).ink};font-weight:700">${typeLabel[e.type]}</span>
    ${e.link?.claudeDone ? `<span style="font-size:.66rem;padding:1px 4px;border-radius:var(--radius-sm);background:var(--accent-subtle);color:var(--accent-hover)">${esc(e.link.claudeVerdict||'')}</span>` : ''}
  </div>
</div>`;
        }).join('') || '<div class="cr-trace-empty">No results — try searching a class name, method, or Oracle proc</div>';

        return `
<div class="cr-split-layout">
  <div class="cr-split-left" style="width:360px">
    <div class="cr-split-hdr">Unified Search <span class="cr-split-count">(${filtered.length}/${entries.length})</span></div>
    <div class="cr-split-search-row" style="padding:8px">
      <input class="cr-split-search" type="text" placeholder="Search Java method or Oracle proc…"
             value="${esc(q)}" autofocus
             oninput="JA.crossRef._searchFilter(this.value, ${JSON.stringify(r.crossRefId)})">
      <div style="font-size:.72rem;color:var(--text-muted);margin-top:4px">
        ${links.length} links · ${javaOnly.length} Java-only · ${oracleOnly.length} Oracle-only
      </div>
    </div>
    <div class="cr-split-list" id="cr-search-list">${listHtml}</div>
  </div>
  <div class="cr-split-right">
    <div id="cr-search-detail">${this._searchSelected ? '<div class="cr-trace-empty">Loading…</div>' : '<div class="cr-trace-empty">Select a result on the left to view source comparison.</div>'}</div>
  </div>
</div>`;
    },

    _searchFilter(val, crossRefId) {
        this._searchQuery = val;
        if (!this._savedResult) return;
        const r = this._savedResult;
        const esc = JA.utils.escapeHtml;
        const links = r.procLinks||[];
        const javaOnly = r.javaOnlyProcs||[];
        const oracleOnly = r.oracleOnlyProcs||[];
        const entries = [];
        links.forEach(l => {
            const simpleClass = (l.javaClass||'').split('.').pop();
            const jKey = simpleClass + '#' + (l.javaMethod||'');
            const oRef = l.resolvedProcRef||l.inferredProcRef||'';
            entries.push({ type:'LINK', label: jKey + ' ↔ ' + oRef, javaNodeId: jKey, procRef: oRef, link: l });
        });
        javaOnly.forEach(p => {
            const cls = (p.javaClass||'').split('.').pop();
            const jKey = cls + '#' + (p.javaMethod||'');
            entries.push({ type:'JAVA_ONLY', label: jKey + ' (Java-only)', javaNodeId: jKey, procRef: '' });
        });
        oracleOnly.forEach(p => entries.push({ type:'ORACLE_ONLY', label: (p.procRef||'') + ' (Oracle-only)', javaNodeId: '', procRef: p.procRef||'' }));

        const ql = val.toLowerCase();
        const filtered = ql ? entries.filter(e => e.label.toLowerCase().includes(ql)) : entries;
        const typeColor = {LINK:'var(--blue)', JAVA_ONLY:'var(--orange)', ORACLE_ONLY:'var(--accent-hover)'};
        const typeLabel = {LINK:'LINK', JAVA_ONLY:'JAVA', ORACLE_ONLY:'ORACLE'};

        const el = document.getElementById('cr-search-list');
        const hdr = el?.closest('.cr-split-left')?.querySelector('.cr-split-hdr');
        if (hdr) hdr.innerHTML = `Unified Search <span class="cr-split-count">(${filtered.length}/${entries.length})</span>`;
        if (el) el.innerHTML = filtered.slice(0,200).map((e,i) => {
            const isActive = this._searchSelected === i+'|'+e.label;
            return `<div class="cr-j2o-item${isActive?' active':''}"
                        onclick="JA.crossRef._searchSelect(${i},'${esc(e.label).replace(/'/g,"\\'")}','${esc(e.javaNodeId).replace(/'/g,"\\'")}','${esc(e.procRef).replace(/'/g,"\\'")}','${esc(crossRefId)}')">
  <div class="cr-j2o-key" style="font-size:.79rem">${esc(e.label)}</div>
  <div class="cr-j2o-meta">
    <span style="font-size:.66rem;padding:1px 4px;border-radius:var(--radius-sm);background:${JA.utils.badgePair(typeColor[e.type]).bg};color:${JA.utils.badgePair(typeColor[e.type]).ink};font-weight:700">${typeLabel[e.type]}</span>
    ${e.link?.claudeDone?`<span style="font-size:.66rem;padding:1px 4px;border-radius:var(--radius-sm);background:var(--accent-subtle);color:var(--accent-hover)">${esc(e.link.claudeVerdict||'')}</span>`:''}
  </div>
</div>`;
        }).join('') || '<div class="cr-trace-empty">No results</div>';
    },

    _searchSelect(idx, label, javaNodeId, procRef, crossRefId) {
        this._searchSelected = idx + '|' + label;
        // Highlight active in list
        document.querySelectorAll('#cr-search-list .cr-j2o-item').forEach((el, i) => {
            el.classList.toggle('active', i === idx);
        });
        const detail = document.getElementById('cr-search-detail');
        if (!detail) return;
        if (!javaNodeId && !procRef) { detail.innerHTML = '<div class="cr-trace-empty">No source available for this entry.</div>'; return; }

        // Find the link for this entry to get Claude info
        const r = this._savedResult;
        const links = r ? (r.procLinks||[]) : [];
        const link = procRef ? links.find(l => (l.resolvedProcRef||l.inferredProcRef||'') === procRef) : null;
        const jarId = this._jarId;

        detail.innerHTML = `
<div style="display:flex;flex-direction:column;height:100%;overflow:hidden">
  <div style="padding:8px 12px;border-bottom:1px solid var(--border);flex-shrink:0;background:var(--bg);display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    ${javaNodeId ? `<code style="font-size:.77rem;color:var(--accent-hover);background:var(--accent-subtle);padding:2px 6px;border-radius:var(--radius-sm)">${JA.utils.escapeHtml(javaNodeId)}</code>` : ''}
    ${javaNodeId && procRef ? '<span style="color:var(--text-muted)">↔</span>' : ''}
    ${procRef ? `<code style="font-size:.77rem;color:var(--badge-orange);background:var(--badge-orange-bg);padding:2px 6px;border-radius:var(--radius-sm)">${JA.utils.escapeHtml(procRef)}</code>` : ''}
    ${link && link.claudeDone ? `<span style="background:var(--accent-hover);color:var(--text-inverse);font-size:.7rem;padding:2px 7px;border-radius:var(--radius-lg)" title="${JA.utils.escapeHtml(link.claudeReason||'')}">AI: ${JA.utils.escapeHtml(link.claudeVerdict||'')}</span>` : ''}
    ${link && link.claudeChunkId ? `<a href="#" onclick="event.preventDefault();JA.crossRef._showChunkDetail('${JA.utils.escapeHtml(link.claudeChunkId)}','${JA.utils.escapeHtml(crossRefId)}')" style="font-size:.72rem;color:var(--text-muted);margin-left:auto">🤖 View Claude analysis →</a>` : ''}
  </div>
  ${link && link.claudeDone ? `
  <div style="padding:8px 12px;border-bottom:1px solid var(--border);flex-shrink:0;font-size:.78rem;color:var(--text-secondary);background:var(--accent-subtle)">
    <strong>Claude:</strong> ${JA.utils.escapeHtml(link.claudeReason||'')}
    ${(link.claudeGaps||[]).length ? ' | <span style="color:var(--badge-orange)">Gaps: '+link.claudeGaps.map(g=>JA.utils.escapeHtml(g)).join(', ')+'</span>' : ''}
  </div>` : ''}
  <div style="display:flex;flex:1;overflow:hidden;min-height:0">
    <div style="flex:1;display:flex;flex-direction:column;border-right:2px solid var(--border);min-width:0">
      <div style="padding:5px 10px;background:var(--accent-subtle);font-size:.75rem;font-weight:700;color:var(--accent-hover);flex-shrink:0">☕ Java Source ${javaNodeId ? '' : '<span style="font-weight:400;color:var(--text-muted)">(none selected)</span>'}</div>
      <div id="cr-ss-java" style="flex:1;overflow:auto;padding:10px;font-family:monospace;font-size:.73rem;white-space:pre;line-height:1.5">${javaNodeId ? 'Loading…' : '<span style="color:var(--text-muted)">(no Java node)</span>'}</div>
    </div>
    <div style="flex:1;display:flex;flex-direction:column;min-width:0">
      <div style="padding:5px 10px;background:var(--badge-orange-bg);font-size:.75rem;font-weight:700;color:var(--badge-orange);flex-shrink:0">🔷 Oracle SQL ${procRef ? '' : '<span style="font-weight:400;color:var(--text-muted)">(none selected)</span>'}</div>
      <div id="cr-ss-oracle" style="flex:1;overflow:auto;padding:10px;font-family:monospace;font-size:.73rem;white-space:pre;line-height:1.5">${procRef ? 'Loading…' : '<span style="color:var(--text-muted)">(no Oracle proc)</span>'}</div>
    </div>
  </div>
</div>`;

        if (javaNodeId) {
            fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}/source/java?nodeId=${encodeURIComponent(javaNodeId)}`)
                .then(r=>r.json()).then(data => {
                    const el = document.getElementById('cr-ss-java');
                    if (!el) return;
                    if (data.error) { el.textContent = 'Not found: ' + data.error; return; }
                    const meta = `// ${data.className||''}\n// ${data.signature||''}\n// Stereotype: ${data.stereotype||''} | Op: ${data.operationType||''}\n\n`;
                    el.innerHTML = this._syntaxHighlightJava(meta + (data.sourceCode||'// No source'));
                }).catch(e => { const el=document.getElementById('cr-ss-java'); if(el) el.textContent='Error: '+e.message; });
        }
        if (procRef) {
            fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}/source/oracle?procRef=${encodeURIComponent(procRef)}`)
                .then(r=>r.json()).then(data => {
                    const el = document.getElementById('cr-ss-oracle');
                    if (!el) return;
                    if (data.error) { el.textContent = 'Not found: ' + data.error; return; }
                    const meta = `-- ${data.packageName||''}.${data.objectName||''}\n-- Depth: ${data.depth||0} | LOC: ${data.linesOfCode||0}\n-- Tables: ${(data.tables||[]).join(', ')||'-'}\n-- Params: ${(data.parameters||[]).map(p=>p.name+' '+p.direction).join(', ')||'-'}\n\n`;
                    el.innerHTML = this._syntaxHighlightSQL(meta + (data.source||'-- No source'));
                }).catch(e => { const el=document.getElementById('cr-ss-oracle'); if(el) el.textContent='Error: '+e.message; });
        }
    },

    // ── Sub-tab: Claude Discussion ────────────────────────────────────────────

    _buildClaudeDiscussionTab(r) {
        const esc = JA.utils.escapeHtml;
        const links = r.procLinks||[];
        const javaOnly = r.javaOnlyProcs||[];
        const oracleOnly = r.oracleOnlyProcs||[];

        const doneLinkCount    = links.filter(l=>l.claudeDone).length;
        const doneJavaOnly     = javaOnly.filter(j=>j.claudeDone).length;
        const doneOracleOnly   = oracleOnly.filter(o=>o.claudeDone).length;
        const totalDone = doneLinkCount + doneJavaOnly + doneOracleOnly + (r.claudeOverallVerdict ? 1 : 0);

        const q = this._claudeDiscFilter || '';
        const ql = q.toLowerCase();

        // Build flat list for display
        const items = [];
        if (r.claudeOverallVerdict) {
            items.push({ label: '⭐ OVERALL VERDICT', chunkId: r.claudeOverallChunkId,
                verdict: r.claudeOverallVerdict, conf: r.claudeOverallConfidence,
                reason: r.claudeOverallReason, type:'OVERALL' });
        }
        links.filter(l=>l.claudeDone).forEach(l => {
            const simpleClass = (l.javaClass||'').split('.').pop();
            const label = simpleClass+'#'+(l.javaMethod||'')+' ↔ '+(l.resolvedProcRef||'');
            if (!ql || label.toLowerCase().includes(ql))
                items.push({ label, chunkId: l.claudeChunkId, verdict: l.claudeVerdict,
                    conf: l.claudeConfidence, reason: l.claudeReason, gaps: l.claudeGaps, type:'LINK' });
        });
        javaOnly.filter(j=>j.claudeDone).forEach(j => {
            const cls = (j.javaClass||'').split('.').pop();
            const label = cls+'#'+(j.javaMethod||'')+' (Java-only)';
            if (!ql || label.toLowerCase().includes(ql))
                items.push({ label, chunkId: j.claudeChunkId, verdict: j.claudeVerdict,
                    conf: j.claudeConfidence, reason: j.claudeReason, type:'JAVA_ONLY' });
        });
        oracleOnly.filter(o=>o.claudeDone).forEach(o => {
            const label = (o.procRef||'')+' (Oracle-only)';
            if (!ql || label.toLowerCase().includes(ql))
                items.push({ label, chunkId: o.claudeChunkId, verdict: o.claudeVerdict,
                    conf: o.claudeConfidence, reason: o.claudeReason, type:'ORACLE_ONLY' });
        });

        const verdColors = {CONVERTED:'var(--green)',PARTIAL:'var(--orange)',NOT_CONVERTED:'var(--red)',
            NO_ORACLE_EQUIVALENT:'var(--bg-inset)',FULLY_CONVERTED:'var(--green)',MOSTLY_CONVERTED:'var(--blue)',
            SIGNIFICANT_GAPS:'var(--red)',NOT_CONVERTED_JO:'var(--red)',HAS_ORACLE_EQUIVALENT:'var(--green)',
            INFRA_ONLY:'var(--bg-inset)',NOT_IN_JAVA:'var(--red)',HAS_JAVA_EQUIVALENT:'var(--green)',INTERNAL_ONLY:'var(--bg-inset)',UNKNOWN:'var(--bg-inset)'};

        const selIdx = this._claudeDiscSelected;
        const selItem = selIdx != null ? items[selIdx] : null;

        const listHtml = items.map((item, i) => {
            const vc = verdColors[item.verdict] || 'var(--bg-inset)';
            const isActive = i === selIdx;
            return `<div class="cr-j2o-item${isActive?' active':''}" onclick="JA.crossRef._claudeDiscSelect(${i}, '${esc(r.crossRefId)}')">
  <div class="cr-j2o-key" style="font-size:.77rem">${esc(item.label)}</div>
  <div class="cr-j2o-meta">
    <span style="font-size:.66rem;padding:1px 5px;border-radius:var(--radius-sm);background:${vc};color:var(--text-inverse)">${esc(item.verdict||'?')}</span>
    <span style="font-size:.66rem;color:var(--text-muted)">${esc(item.conf||'')}</span>
  </div>
</div>`;
        }).join('') || `<div class="cr-trace-empty" style="text-align:center;padding:24px 12px">
  ${totalDone===0
    ? `<div>No Claude results in result.json yet.</div>
       <div style="margin-top:10px;font-size:.8rem;color:var(--text-muted)">If you already ran an analysis, click below to reload chunk files from disk:</div>
       <button class="cr-btn cr-btn-sec" style="margin-top:12px;font-size:.8rem;padding:5px 14px"
         onclick="JA.crossRef._mergeClaudeResults('${esc(r.crossRefId)}')">⟳ Reload Results from Disk</button>`
    : 'No results match filter.'
  }
</div>`;

        const rightHtml = selItem ? this._buildClaudeDiscDetail(selItem, r) :
            `<div class="cr-trace-empty">Select an item to view the full Claude prompt and response.</div>`;

        return `
<div class="cr-split-layout">
  <div class="cr-split-left" style="width:360px">
    <div class="cr-split-hdr">Claude Chunks <span class="cr-split-count">(${totalDone} analyzed)</span></div>
    <div class="cr-split-search-row">
      <input class="cr-split-search" type="text" placeholder="Filter by class, proc, verdict…"
             value="${esc(q)}"
             oninput="JA.crossRef._claudeDiscFilter=this.value;JA.crossRef._showSubTab('claude')">
    </div>
    <div class="cr-split-list">${listHtml}</div>
  </div>
  <div class="cr-split-right">
    <div class="cr-split-detail" id="cr-claude-disc-detail">${rightHtml}</div>
  </div>
</div>`;
    },

    _claudeDiscSelect(idx, crossRefId) {
        this._claudeDiscSelected = idx;
        // Rebuild the tab — the list is already rendered, just update detail
        const r = this._savedResult;
        if (!r) return;
        // Re-render the whole tab for simplicity (it's a non-scroll page)
        const body = document.getElementById('cr-subtab-body');
        if (body) body.innerHTML = this._buildClaudeDiscussionTab(r);
    },

    _buildClaudeDiscDetail(item, r) {
        const esc = JA.utils.escapeHtml;
        const verdColors = {CONVERTED:'var(--green)',PARTIAL:'var(--orange)',NOT_CONVERTED:'var(--red)',
            NO_ORACLE_EQUIVALENT:'var(--bg-inset)',FULLY_CONVERTED:'var(--green)',MOSTLY_CONVERTED:'var(--blue)',
            SIGNIFICANT_GAPS:'var(--red)',NOT_IN_JAVA:'var(--red)',HAS_JAVA_EQUIVALENT:'var(--green)',
            INTERNAL_ONLY:'var(--bg-inset)',HAS_ORACLE_EQUIVALENT:'var(--green)',INFRA_ONLY:'var(--bg-inset)',UNKNOWN:'var(--bg-inset)'};
        const vc = verdColors[item.verdict] || 'var(--bg-inset)';

        let html = `
<div style="padding:10px 12px;border-bottom:1px solid var(--border);background:var(--accent-subtle);flex-shrink:0">
  <div style="font-size:.83rem;font-weight:700;color:var(--text);margin-bottom:4px">${esc(item.label)}</div>
  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <span style="background:${vc};color:var(--text-inverse);font-size:.75rem;padding:2px 8px;border-radius:var(--radius-lg)">${esc(item.verdict||'?')}</span>
    <span style="font-size:.75rem;color:var(--text-muted)">${esc(item.conf||'')}</span>
    ${item.chunkId?`<span style="font-size:.7rem;font-family:monospace;color:var(--text-muted)">#${esc(item.chunkId)}</span>`:''}
  </div>
  <div style="margin-top:6px;font-size:.8rem;color:var(--text-secondary)">${esc(item.reason||'')}</div>
  ${(item.gaps||[]).length?`<div style="margin-top:4px;font-size:.77rem;color:var(--badge-orange)">Gaps: ${(item.gaps||[]).map(g=>`<span style="background:var(--badge-orange-bg);padding:1px 5px;border-radius:var(--radius-sm);margin-right:3px">${esc(g)}</span>`).join('')}</div>`:''}
</div>`;

        if (!item.chunkId) {
            html += '<div class="cr-trace-empty">No chunk file available.</div>';
            return html;
        }

        // Load chunk detail async
        const jarId = this._jarId;
        const crossRefId = r.crossRefId;
        setTimeout(() => {
            const detailEl = document.getElementById('cr-claude-disc-detail');
            if (!detailEl) return;
            fetch(`/api/jar/jars/${encodeURIComponent(jarId)}/cross-ref/result/${encodeURIComponent(crossRefId)}/claude/chunk/${encodeURIComponent(item.chunkId)}`)
                .then(resp => resp.json())
                .then(data => {
                    const loadingEl = detailEl.querySelector('#cr-disc-chunk-load');
                    if (!loadingEl) return;
                    loadingEl.innerHTML = `
<div style="padding:10px 12px">
  <div style="font-size:.8rem;font-weight:700;color:var(--text-secondary);margin-bottom:8px">Duration: ${data.durationMs||0}ms</div>
  <details style="margin-bottom:10px">
    <summary style="cursor:pointer;font-weight:700;font-size:.83rem;color:var(--text-secondary);padding:6px 0">📨 Prompt</summary>
    <pre style="margin-top:6px;font-size:.71rem;background:var(--bg);padding:10px;border-radius:var(--radius-sm);overflow:auto;max-height:400px;white-space:pre-wrap;word-break:break-word;border:1px solid var(--border)">${esc(data.prompt||'')}</pre>
  </details>
  <details open>
    <summary style="cursor:pointer;font-weight:700;font-size:.83rem;color:var(--text-secondary);padding:6px 0">💬 Response</summary>
    <pre style="margin-top:6px;font-size:.71rem;background:var(--badge-green-bg);padding:10px;border-radius:var(--radius-sm);overflow:auto;max-height:400px;white-space:pre-wrap;word-break:break-word;border:1px solid var(--badge-green-bg)">${esc(data.response||'')}</pre>
  </details>
</div>`;
                }).catch(e => {
                    const loadingEl = detailEl.querySelector('#cr-disc-chunk-load');
                    if (loadingEl) loadingEl.innerHTML = `<div style="padding:12px;color:var(--red);font-size:.8rem">Error loading chunk: ${e.message}</div>`;
                });
        }, 50);

        html += `<div id="cr-disc-chunk-load" style="flex:1;overflow:auto"><div class="cr-loading">Loading prompt & response…</div></div>`;
        return html;
    },

    _renderRoutingNodes(routingNodes) {
        const esc = JA.utils.escapeHtml;
        let html=`<div class="cr-section"><div class="cr-section-title">Call Chain — Service Layer (${routingNodes.length})</div>
<p style="font-size:.8rem;color:var(--text-muted);margin:0 0 10px">Java classes routing calls to Oracle through the Pg* layer.</p>
<details><summary style="font-size:.82rem;cursor:pointer;font-weight:600">Expand call chain (${routingNodes.length})</summary>
<table class="cr-table" style="margin-top:8px"><thead><tr><th>Java Class</th><th>Method</th><th>Calls Oracle</th><th>Through</th></tr></thead><tbody>`;
        routingNodes.forEach(n => {
            const pkgs=(n.calledOraclePackages||[]).map(p=>`<code style="font-size:.75rem">${esc(p)}</code>`).join(' ');
            const via=(n.directProcCallers||[]).slice(0,3).map(c=>`<span style="font-size:.72rem;color:var(--accent-hover)">${esc(c)}</span>`).join(', ');
            html+=`<tr><td><code>${esc((n.javaClass||'').split('.').pop())}</code></td><td>${esc(n.javaMethod||'')}</td><td>${pkgs||'-'}</td><td>${via||'-'}</td></tr>`;
        });
        html+='</tbody></table></details></div>';
        return html;
    },

    _renderInfraNodes(infraNodes) {
        const esc = JA.utils.escapeHtml;
        let html=`<div class="cr-section"><div class="cr-section-title">Infrastructure Nodes — No Oracle Equivalent (${infraNodes.length})</div>
<details><summary style="font-size:.82rem;cursor:pointer;color:var(--text-muted)">Click to expand</summary>
<table class="cr-table" style="margin-top:8px"><thead><tr><th>Class</th><th>Method</th><th>Reason</th></tr></thead><tbody>`;
        infraNodes.forEach(n => {
            html+=`<tr><td><code>${esc((n.javaClass||'').split('.').pop())}</code></td><td>${esc(n.javaMethod||'')}</td><td><span class="cr-badge cr-badge-infra">${esc(n.reason||'')}</span></td></tr>`;
        });
        html+='</tbody></table></details></div>';
        return html;
    },

    // ── Sub-tab: Java → Oracle ────────────────────────────────────────────────

    _buildJ2OTab(r) {
        const esc = JA.utils.escapeHtml;
        const links = r.procLinks||[];
        const q = this._j2oFilter.toLowerCase();

        // Build deduplicated list: one entry per jKey (collapse multi-package into one card)
        const byKey = new Map();
        links.forEach(link => {
            const simpleClass = (link.javaClass||'').split('.').pop();
            const jKey = `${simpleClass}.${link.javaMethod||''}`;
            if (!byKey.has(jKey)) byKey.set(jKey, []);
            byKey.get(jKey).push(link);
        });

        let entries = [...byKey.entries()];
        if (q) entries = entries.filter(([k]) => k.toLowerCase().includes(q));

        // Auto-select first if nothing selected
        if (!this._j2oSelected && entries.length) this._j2oSelected = entries[0][0];

        const listHtml = this._buildJ2OListHtml(entries);

        // Right panel
        let rightHtml = '<div class="cr-trace-empty">Select a Java method on the left.</div>';
        if (this._j2oSelected && byKey.has(this._j2oSelected)) {
            rightHtml = this._buildJ2ODetail(this._j2oSelected, byKey.get(this._j2oSelected));
        }

        return `
<div class="cr-split-layout">
  <div class="cr-split-left">
    <div class="cr-split-hdr">
      Java Methods <span class="cr-split-count">(${entries.length})</span>
    </div>
    <div class="cr-split-search-row">
      <input class="cr-split-search" type="text" placeholder="Filter methods…"
             value="${esc(this._j2oFilter)}"
             oninput="JA.crossRef._j2oSearch(this.value)">
    </div>
    <div class="cr-split-list" id="cr-j2o-list">${listHtml}</div>
  </div>
  <div class="cr-split-right">
    <div class="cr-split-hdr">Oracle Mapping</div>
    <div class="cr-split-detail" id="cr-j2o-detail">${rightHtml}</div>
  </div>
</div>`;
    },

    _j2oSearch(val) {
        this._j2oFilter = val;
        if (!this._savedResult) return;
        const links = this._savedResult.procLinks || [];
        const byKey = new Map();
        links.forEach(link => {
            const simpleClass = (link.javaClass||'').split('.').pop();
            const jKey = `${simpleClass}.${link.javaMethod||''}`;
            if (!byKey.has(jKey)) byKey.set(jKey, []);
            byKey.get(jKey).push(link);
        });
        const q = val.toLowerCase();
        let entries = [...byKey.entries()];
        if (q) entries = entries.filter(([k]) => k.toLowerCase().includes(q));
        if (this._j2oSelected && !entries.find(([k]) => k === this._j2oSelected))
            this._j2oSelected = entries.length ? entries[0][0] : null;
        else if (!this._j2oSelected && entries.length)
            this._j2oSelected = entries[0][0];
        // Targeted update — don't touch the search input DOM
        const hdr = document.querySelector('#cr-j2o-list')?.closest('.cr-split-left')?.querySelector('.cr-split-hdr');
        if (hdr) hdr.innerHTML = `Java Methods <span class="cr-split-count">(${entries.length})</span>`;
        const listEl = document.getElementById('cr-j2o-list');
        if (listEl) listEl.innerHTML = this._buildJ2OListHtml(entries);
        const detail = document.getElementById('cr-j2o-detail');
        if (detail) detail.innerHTML = (this._j2oSelected && byKey.has(this._j2oSelected))
            ? this._buildJ2ODetail(this._j2oSelected, byKey.get(this._j2oSelected))
            : '<div class="cr-trace-empty">Select a Java method on the left.</div>';
    },

    _buildJ2OListHtml(entries) {
        const esc = JA.utils.escapeHtml;
        const zoneColors = {'PASS_A':'var(--blue)','PASS_B':'var(--accent-hover)','PASS_A+B':'var(--teal)','PASS_C':'var(--orange)','PASS_D_INHERITED':'var(--bg-inset)'};
        return entries.map(([jKey, jLinks]) => {
            const first = jLinks[0];
            const isActive = jKey === this._j2oSelected;
            const pkg = (first.resolvedProcRef||first.inferredProcRef||'').split('.')[0]||'';
            const zSrc = first.zoneSource||'';
            const zColor = zoneColors[zSrc]||'var(--bg-inset)';
            const confClass = first.matchConfidence==='HIGH'?'cr-badge-high':first.matchConfidence==='MEDIUM'?'cr-badge-med':'cr-badge-low';
            return `<div class="cr-j2o-item${isActive?' active':''}"
                        onclick="JA.crossRef._j2oSelect('${esc(jKey).replace(/'/g,"\\'")}')">
  <div class="cr-j2o-key">${esc(jKey)}</div>
  <div class="cr-j2o-meta">
    <span class="cr-badge ${confClass}" style="font-size:.68rem">${first.matchConfidence}</span>
    ${pkg?`<code style="font-size:.72rem;color:var(--text-secondary)">${esc(pkg)}</code>`:''}
    ${zSrc?`<span style="font-size:.68rem;padding:1px 4px;border-radius:var(--radius-sm);background:${zColor};color:var(--text-inverse);font-family:monospace">${esc(zSrc)}</span>`:''}
    ${first.plsqlNodeFound?'<span style="color:var(--badge-green);font-size:.72rem">✓</span>':''}
  </div>
</div>`;
        }).join('') || `<div class="cr-trace-empty">${this._j2oFilter?'No matches':'No proc links found.'}</div>`;
    },

    _j2oSelect(jKey) {
        this._j2oSelected = jKey;
        // Fast update: just swap right panel and re-highlight left
        const links = this._savedResult ? (this._savedResult.procLinks||[]) : [];
        const byKey = new Map();
        links.forEach(link => {
            const simpleClass = (link.javaClass||'').split('.').pop();
            const k = `${simpleClass}.${link.javaMethod||''}`;
            if (!byKey.has(k)) byKey.set(k,[]);
            byKey.get(k).push(link);
        });
        // Update active class in list
        document.querySelectorAll('.cr-j2o-item').forEach(el => {
            el.classList.toggle('active', el.querySelector('.cr-j2o-key')?.textContent===jKey);
        });
        // Update right panel
        const detail = document.getElementById('cr-j2o-detail');
        if (detail && byKey.has(jKey)) {
            detail.innerHTML = this._buildJ2ODetail(jKey, byKey.get(jKey));
        }
    },

    _buildJ2ODetail(jKey, jLinks) {
        const esc = JA.utils.escapeHtml;
        const first = jLinks[0];
        const simpleClass = (first.javaClass||'').split('.').pop();
        const zoneColors = {'PASS_A':'var(--blue)','PASS_B':'var(--accent-hover)','PASS_A+B':'var(--teal)','PASS_C':'var(--orange)','PASS_D_INHERITED':'var(--bg-inset)'};

        let html = `
<div class="cr-j2o-detail-hdr">
  <div style="font-size:.88rem;font-weight:700;color:var(--text)">${esc(simpleClass)}</div>
  <div style="font-size:.8rem;color:var(--text-muted)">.${esc(first.javaMethod||'')}</div>
  <div style="font-size:.72rem;color:var(--text-muted);margin-top:2px;word-break:break-all">${esc(first.javaClass||'')}</div>
</div>`;

        jLinks.forEach((link, i) => {
            const ref  = link.resolvedProcRef||link.inferredProcRef||'-';
            const pkg  = ref.includes('.')?ref.split('.')[0]:ref;
            const proc = ref.includes('.')?ref.split('.').slice(1).join('.'):'-';
            const zSrc = link.zoneSource||'';
            const zColor = zoneColors[zSrc]||'var(--bg-inset)';
            const confClass = link.matchConfidence==='HIGH'?'cr-badge-high':link.matchConfidence==='MEDIUM'?'cr-badge-med':'cr-badge-low';

            html += `<div class="cr-j2o-mapping-card">
<table class="cr-detail-table">
  <tr><td>Oracle Proc</td><td><code style="color:var(--blue);font-weight:600">${esc(ref)}</code></td></tr>
  <tr><td>Package</td><td><code>${esc(pkg)}</code></td></tr>
  ${proc&&proc!=='-'?`<tr><td>Procedure</td><td><code>${esc(proc)}</code></td></tr>`:''}
  <tr><td>Confidence</td><td><span class="cr-badge ${confClass}">${link.matchConfidence}</span></td></tr>
  <tr><td>Zone Source</td><td>${zSrc?`<span style="background:${zColor};color:var(--text-inverse);font-size:.72rem;padding:1px 5px;border-radius:var(--radius-sm);font-family:monospace">${esc(zSrc)}</span>`:'-'}</td></tr>
  <tr><td>PL/SQL Found</td><td>${link.plsqlNodeFound?'<span style="color:var(--badge-green);font-weight:600">✓ Confirmed</span>':'<span style="color:var(--text-muted)">Not confirmed</span>'}</td></tr>
  ${link.javaOperationType?`<tr><td>Operation</td><td>${esc(link.javaOperationType)}</td></tr>`:''}
  ${link.javaStereotype?`<tr><td>Stereotype</td><td>${esc(link.javaStereotype)}</td></tr>`:''}
</table>`;

            // Table matches
            const tms = link.tableMatches||[];
            if (tms.length) {
                html += `<div class="cr-j2o-section-lbl">Table Matches (${tms.length})</div>
<table class="cr-table cr-table-compact"><thead><tr><th>Java Collection</th><th>Oracle Table</th><th>Java Op</th><th>Oracle Op</th><th>Status</th></tr></thead><tbody>`;
                tms.forEach(tm => {
                    const stClass=tm.matchStatus==='MATCHED'?'cr-match':tm.matchStatus==='INFERRED'?'cr-inferred':tm.matchStatus==='JAVA_ONLY'?'cr-java-only':tm.matchStatus==='ORACLE_ONLY'?'cr-oracle-only':'cr-mismatch';
                    const icon=tm.matchStatus==='MATCHED'?'✓':tm.matchStatus==='INFERRED'?'~':'⚠';
                    html+=`<tr><td><code>${esc(tm.mongoCollection||'-')}</code></td><td><code>${esc(tm.oracleTable||'-')}</code></td><td>${esc(tm.javaOperation||'-')}</td><td>${esc(tm.oracleOperation||'-')}</td><td class="${stClass}">${icon} ${esc(tm.matchStatus||'-')}</td></tr>`;
                });
                html += '</tbody></table>';
            }
            html += '</div>';
        });
        return html;
    },

    // ── Sub-tab: Oracle → Java ────────────────────────────────────────────────

    _buildO2JTab(r) {
        const esc = JA.utils.escapeHtml;
        const nodes = r.oracleNodes||[];
        const qLeft = this._o2jFilter.toLowerCase();

        let filteredNodes = nodes;
        if (qLeft) filteredNodes = nodes.filter(n =>
            (n.packageName||'').toLowerCase().includes(qLeft) ||
            (n.objectName||'').toLowerCase().includes(qLeft));

        // Auto-select first if nothing selected
        if (!this._o2jSelected && filteredNodes.length) this._o2jSelected = filteredNodes[0].nodeId;

        const leftHtml = this._buildO2JListHtml(filteredNodes, qLeft);

        // Right panel
        let rightHtml = '<div class="cr-trace-empty">Select an Oracle proc on the left.</div>';
        if (this._o2jSelected) {
            const node = nodes.find(n => n.nodeId === this._o2jSelected);
            if (node) rightHtml = this._buildO2JDetail(node, r);
        }

        return `
<div class="cr-split-layout">
  <div class="cr-split-left">
    <div class="cr-split-hdr">
      Oracle Procs <span class="cr-split-count">(${filteredNodes.length})</span>
    </div>
    <div class="cr-split-search-row">
      <input class="cr-split-search" type="text" placeholder="Filter Oracle procs…"
             value="${esc(this._o2jFilter)}"
             oninput="JA.crossRef._o2jSearchLeft(this.value)">
    </div>
    <div class="cr-split-list" id="cr-o2j-list">${leftHtml}</div>
  </div>
  <div class="cr-split-right">
    <div class="cr-split-hdr" style="display:flex;align-items:center;gap:8px">
      Java Callers
      <input class="cr-split-search" type="text" placeholder="Filter callers…"
             style="flex:1;max-width:180px"
             value="${esc(this._o2jRightFilter)}"
             oninput="JA.crossRef._o2jSearchRight(this.value)">
    </div>
    <div class="cr-split-detail" id="cr-o2j-detail">${rightHtml}</div>
  </div>
</div>`;
    },

    _o2jSearchLeft(val) {
        this._o2jFilter = val;
        if (!this._savedResult) return;
        const nodes = this._savedResult.oracleNodes || [];
        const qLeft = val.toLowerCase();
        const filteredNodes = qLeft ? nodes.filter(n =>
            (n.packageName||'').toLowerCase().includes(qLeft) ||
            (n.objectName||'').toLowerCase().includes(qLeft)) : nodes;
        if (this._o2jSelected && !filteredNodes.find(n => n.nodeId === this._o2jSelected))
            this._o2jSelected = filteredNodes.length ? filteredNodes[0].nodeId : null;
        else if (!this._o2jSelected && filteredNodes.length)
            this._o2jSelected = filteredNodes[0].nodeId;
        // Targeted update — don't touch the search input DOM
        const hdr = document.querySelector('#cr-o2j-list')?.closest('.cr-split-left')?.querySelector('.cr-split-hdr');
        if (hdr) hdr.innerHTML = `Oracle Procs <span class="cr-split-count">(${filteredNodes.length})</span>`;
        const listEl = document.getElementById('cr-o2j-list');
        if (listEl) listEl.innerHTML = this._buildO2JListHtml(filteredNodes, qLeft);
        const detail = document.getElementById('cr-o2j-detail');
        if (detail) {
            const node = this._o2jSelected ? nodes.find(n => n.nodeId === this._o2jSelected) : null;
            detail.innerHTML = node
                ? this._buildO2JDetail(node, this._savedResult)
                : '<div class="cr-trace-empty">Select an Oracle proc on the left.</div>';
        }
    },

    _buildO2JListHtml(filteredNodes, qLeft) {
        const esc = JA.utils.escapeHtml;
        const depthColors = ['var(--blue)','var(--green)','var(--accent)','var(--orange)','var(--blue)'];
        return filteredNodes.map(n => {
            const depth = n.depth||0;
            const color = depthColors[Math.min(depth, depthColors.length-1)];
            const procRef = n.packageName ? `${n.packageName}.${n.objectName}` : (n.objectName||'');
            const allCallers = [...new Set([...(n.javaCallers||[]),...(n.javaTableCallers||[])])];
            const isActive = n.nodeId === this._o2jSelected;
            const depthBadge = depth===0
                ? '<span class="cr-depth-badge" style="background:var(--badge-blue-bg);color:var(--badge-blue)">ENTRY</span>'
                : `<span class="cr-depth-badge" style="background:var(--bg);color:var(--text-muted)">D${depth}</span>`;
            return `<div class="cr-o2j-item${isActive?' active':''}"
                        style="padding-left:${8+depth*14}px"
                        onclick="JA.crossRef._o2jSelect('${esc(n.nodeId).replace(/'/g,"\\'")}')">
  <div class="cr-o2j-proc" style="color:${color}">${esc(procRef)}</div>
  <div class="cr-o2j-meta">
    ${depthBadge}
    <span style="font-size:.7rem;color:var(--text-muted)">${esc(n.objectType||'')}</span>
    ${allCallers.length?`<span class="cr-depth-badge" style="background:var(--badge-green-bg);color:var(--badge-green)">${allCallers.length} Java caller${allCallers.length>1?'s':''}</span>`:''}
  </div>
</div>`;
        }).join('') || `<div class="cr-trace-empty">${qLeft?'No matches':'No Oracle nodes in this result.'}</div>`;
    },

    _o2jSearchRight(val) {
        this._o2jRightFilter = val;
        if (!this._savedResult || !this._o2jSelected) return;
        const node = (this._savedResult.oracleNodes||[]).find(n=>n.nodeId===this._o2jSelected);
        if (node) {
            const detail = document.getElementById('cr-o2j-detail');
            if (detail) detail.innerHTML = this._buildO2JDetail(node, this._savedResult);
        }
    },

    _o2jSelect(nodeId) {
        this._o2jSelected = nodeId;
        // Update active class in list (no DOM rebuild)
        document.querySelectorAll('.cr-o2j-item').forEach(el => {
            el.classList.toggle('active', el.getAttribute('onclick')?.includes(nodeId.replace(/'/g,"\\'")));
        });
        if (!this._savedResult) return;
        const node = (this._savedResult.oracleNodes||[]).find(n=>n.nodeId===nodeId);
        const detail = document.getElementById('cr-o2j-detail');
        if (detail && node) detail.innerHTML = this._buildO2JDetail(node, this._savedResult);
    },

    _buildO2JDetail(node, r) {
        const esc = JA.utils.escapeHtml;
        const procRef = node.packageName ? `${node.packageName}.${node.objectName}` : (node.objectName||'');
        const allCallers  = [...new Set([...(node.javaCallers||[]),...(node.javaTableCallers||[])])];
        const qRight = this._o2jRightFilter.toLowerCase();

        let filtCallers = allCallers;
        if (qRight) filtCallers = allCallers.filter(c => c.toLowerCase().includes(qRight));

        // Find matching proc links for each caller
        const links = r.procLinks||[];
        const byKey = new Map();
        links.forEach(link => {
            const simpleClass = (link.javaClass||'').split('.').pop();
            const k = `${simpleClass}.${link.javaMethod||''}`;
            if (!byKey.has(k)) byKey.set(k,[]);
            byKey.get(k).push(link);
        });

        let html = `
<div class="cr-j2o-detail-hdr">
  <div style="font-size:.88rem;font-weight:700;color:var(--badge-blue)"><code>${esc(procRef)}</code></div>
  <div style="font-size:.78rem;color:var(--text-muted);margin-top:2px">${esc(node.objectType||'')} · depth ${node.depth||0}</div>
  ${node.schema?`<div style="font-size:.72rem;color:var(--text-muted)">Schema: ${esc(node.schema)}</div>`:''}
</div>`;

        if (!filtCallers.length) {
            html += `<div class="cr-trace-empty">${allCallers.length?'No callers match filter':'No Java callers found for this Oracle proc.'}</div>`;
            return html;
        }

        const pgCallers    = filtCallers.filter(c => !(node.javaTableCallers||[]).includes(c) && (node.javaCallers||[]).includes(c));
        const tableCallers = filtCallers.filter(c => (node.javaTableCallers||[]).includes(c));

        if (pgCallers.length) {
            html += `<div class="cr-j2o-section-lbl">Direct Callers — Pg-naming (${pgCallers.length})</div>`;
            pgCallers.forEach(caller => { html += this._renderCallerCard(caller, byKey, 'var(--badge-green-bg)', 'var(--green)'); });
        }
        if (tableCallers.length) {
            html += `<div class="cr-j2o-section-lbl">Table-based Callers (${tableCallers.length})</div>`;
            tableCallers.forEach(caller => { html += this._renderCallerCard(caller, byKey, 'var(--badge-blue-bg)', 'var(--blue)'); });
        }

        return html;
    },

    _renderCallerCard(caller, byKey, bg, fg) {
        const esc = JA.utils.escapeHtml;
        const jLinks = byKey.get(caller)||[];
        const first = jLinks[0];
        const zoneColors = {'PASS_A':'var(--blue)','PASS_B':'var(--accent-hover)','PASS_A+B':'var(--teal)','PASS_C':'var(--orange)','PASS_D_INHERITED':'var(--bg-inset)'};
        const zSrc = first?.zoneSource||'';
        const zColor = zoneColors[zSrc]||'var(--bg-inset)';
        const confClass = first?.matchConfidence==='HIGH'?'cr-badge-high':first?.matchConfidence==='MEDIUM'?'cr-badge-med':'cr-badge-low';

        return `<div class="cr-caller-card" style="border-left:3px solid ${fg}">
  <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
    <code style="font-size:.83rem;font-weight:600;color:var(--text)">${esc(caller)}</code>
    ${first?`<span class="cr-badge ${confClass}" style="font-size:.68rem">${first.matchConfidence}</span>`:''}
    ${zSrc?`<span style="font-size:.68rem;padding:1px 4px;border-radius:var(--radius-sm);background:${zColor};color:var(--text-inverse);font-family:monospace">${esc(zSrc)}</span>`:''}
    ${first?.plsqlNodeFound?'<span style="color:var(--badge-green);font-size:.75rem">✓ confirmed</span>':''}
    <button onclick="JA.crossRef._gotoJ2O('${esc(caller).replace(/'/g,"\\'")}\")"
            style="margin-left:auto;font-size:.72rem;padding:2px 8px;border:1px solid var(--border);background:var(--bg-card);border-radius:var(--radius-sm);cursor:pointer;color:var(--blue)">
      → Java view
    </button>
  </div>
  ${first?.javaOperationType?`<div style="font-size:.72rem;color:var(--text-muted);margin-top:2px">Operation: ${esc(first.javaOperationType)}</div>`:''}
</div>`;
    },

    _gotoJ2O(jKey) {
        this._savedSubTab = 'java2oracle';
        this._j2oSelected = jKey;
        this._j2oFilter = '';
        document.querySelectorAll('.cr-subtab-btn').forEach(b =>
            b.classList.toggle('active', b.dataset.sub === 'java2oracle'));
        const body = document.getElementById('cr-subtab-body');
        if (body && this._savedResult) body.innerHTML = this._buildJ2OTab(this._savedResult);
        setTimeout(() => {
            const active = document.querySelector('.cr-j2o-item.active');
            if (active) active.scrollIntoView({block:'nearest'});
        }, 50);
    },

    // ── CSS ───────────────────────────────────────────────────────────────────

    _css() {
        return `<style>
/* ── Container fix ── */
#tab-crossref { overflow-y: auto !important; }
#tab-crossref .cr-wrap { min-height: 100%; }

/* ── Shell ── */
.cr-wrap { display:flex; flex-direction:column; height:100%; max-height:100%; overflow:hidden; }
.cr-topnav { display:flex; gap:8px; padding:12px 16px 0; border-bottom:1px solid var(--border); background:var(--bg-card); flex-shrink:0; }
.cr-topbtn { padding:8px 18px; border:none; background:none; border-bottom:2px solid transparent; cursor:pointer; font-size:.88rem; font-weight:600; color:var(--text-muted); margin-bottom:-1px; }
.cr-topbtn.active { color:var(--blue); border-bottom-color:var(--blue); }
.cr-topbtn:hover:not(.active) { color:var(--text-secondary); }
#cr-body { flex:1; overflow-y:auto; padding:16px; min-height:0; }

/* ── Link form ── */
.cr-form { background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius-md); padding:20px; max-width:600px; }
.cr-form label { display:block; font-size:.82rem; font-weight:600; color:var(--text-secondary); margin-bottom:4px; }
.cr-text-input { width:100%; padding:8px 10px; border:1px solid var(--border); border-radius:var(--radius); font-size:.9rem; margin-bottom:14px; box-sizing:border-box; }
.cr-notice { padding:10px 14px; background:var(--badge-blue-bg); border:1px solid var(--badge-blue-bg); border-radius:var(--radius); font-size:.83rem; color:var(--blue); margin-bottom:14px; }
.cr-btn { padding:9px 22px; background:var(--blue); color:var(--text-inverse); border:none; border-radius:var(--radius); cursor:pointer; font-size:.9rem; font-weight:500; }
.cr-btn:disabled { background:var(--bg-inset); cursor:not-allowed; }
.cr-btn-sec { background:var(--bg-card); color:var(--text-secondary); border:1px solid var(--border); }
.cr-clear-btn { background:none; border:none; cursor:pointer; color:var(--text-muted); font-size:1rem; line-height:1; padding:0 4px; }

/* ── Success banner ── */
.cr-success-banner { display:flex; align-items:center; gap:12px; padding:12px 16px; background:var(--badge-green-bg); border:1px solid var(--badge-green-bg); border-radius:var(--radius-md); margin-bottom:16px; flex-wrap:wrap; }
.cr-success-icon { font-size:1.3rem; color:var(--badge-green); }

/* ── Endpoint picker ── */
.cr-ep-filter-bar { display:flex; gap:6px; margin-bottom:6px; flex-wrap:wrap; }
.cr-ep-filter-btn { padding:3px 10px; border:1px solid var(--border); background:var(--bg-card); border-radius:var(--radius-xl); font-size:.75rem; cursor:pointer; color:var(--text-secondary); }
.cr-ep-filter-btn.active { background:var(--blue); color:var(--text-inverse); border-color:var(--blue); }
.cr-ep-picker { position:relative; margin-bottom:14px; }
.cr-ep-search { width:100%; padding:8px 10px 8px 32px; border:1px solid var(--border); border-radius:var(--radius); font-size:.9rem; box-sizing:border-box; background:var(--bg-card); }
.cr-ep-search:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px var(--focus-ring); }
.cr-ep-search-icon { position:absolute; left:9px; top:50%; transform:translateY(-50%); color:var(--text-muted); pointer-events:none; line-height:1; }
.cr-ep-dropdown { position:absolute; top:calc(100% + 2px); left:0; right:0; background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); box-shadow: var(--shadow-lg); max-height:260px; overflow-y:auto; z-index:200; display:none; }
.cr-ep-dropdown.open { display:block; }
.cr-ep-item { padding:8px 12px; cursor:pointer; font-size:.85rem; display:flex; align-items:center; gap:8px; border-bottom:1px solid var(--border-subtle); }
.cr-ep-item:last-child { border-bottom:none; }
.cr-ep-item:hover,.cr-ep-item.selected { background:var(--badge-blue-bg); }
.cr-ep-item.analyzed { background:var(--badge-green-bg); }
.cr-ep-item.analyzed:hover,.cr-ep-item.analyzed.selected { background:var(--badge-green-bg); }
.cr-ep-method { font-size:.72rem; font-weight:700; padding:2px 6px; border-radius:var(--radius-sm); flex-shrink:0; min-width:40px; text-align:center; }
.cr-ep-path { flex:1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; color:var(--text); }
.cr-ep-idx { font-size:.72rem; color:var(--text-muted); flex-shrink:0; }
.cr-ep-count { font-size:.78rem; color:var(--text-muted); margin-bottom:4px; }
.cr-ep-selected-display { display:none; padding:8px 10px; background:var(--badge-blue-bg); border:1px solid var(--badge-blue-bg); border-radius:var(--radius); font-size:.83rem; color:var(--badge-blue); margin-top:4px; align-items:center; gap:8px; }
.cr-ep-selected-display.show { display:flex; }
.cr-ep-analyzed-tag { font-size:.68rem; font-weight:700; padding:1px 6px; border-radius:var(--radius-lg); background:var(--badge-green-bg); color:var(--badge-green); flex-shrink:0; }
.cr-ep-group-label { padding:5px 12px 3px; font-size:.72rem; font-weight:700; color:var(--text-muted); letter-spacing:.05em; text-transform:uppercase; background:var(--bg); border-bottom:1px solid var(--border-subtle); position:sticky; top:0; }

/* ── Saved tab: two-column layout ── */
.cr-saved-layout { display:flex; gap:0; height:calc(100vh - 130px); min-height:400px; border:1px solid var(--border); border-radius:var(--radius-md); overflow:hidden; background:var(--bg-card); }
.cr-saved-sidebar { width:280px; flex-shrink:0; border-right:1px solid var(--border); display:flex; flex-direction:column; }
.cr-saved-sidebar-hdr { padding:10px; border-bottom:1px solid var(--border); }
.cr-saved-search { width:100%; padding:7px 10px; border:1px solid var(--border); border-radius:var(--radius); font-size:.83rem; box-sizing:border-box; }
.cr-saved-search:focus { outline:none; border-color:var(--blue); }
.cr-saved-list { flex:1; overflow-y:auto; }
.cr-saved-item { padding:10px 12px; cursor:pointer; border-bottom:1px solid var(--border-subtle); transition:background .1s; }
.cr-saved-item:hover { background:var(--bg); }
.cr-saved-item.active { background:var(--badge-blue-bg); border-right:2px solid var(--blue); }
.cr-saved-item-ep { font-size:.8rem; font-weight:600; color:var(--text); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; margin-bottom:2px; }
.cr-saved-item-proc { font-size:.75rem; font-family:monospace; color:var(--blue); margin-bottom:3px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.cr-saved-item-meta { display:flex; gap:6px; align-items:center; }
.cr-del-btn { flex-shrink:0; background:none; border:none; cursor:pointer; color: var(--text-secondary); font-size:.8rem; padding:2px 4px; border-radius:var(--radius-sm); line-height:1; opacity:0; transition:opacity .15s,color .15s; }
.cr-saved-item:hover .cr-del-btn { opacity:1; }
.cr-del-btn:hover { color:var(--red) !important; background:var(--badge-red-bg); opacity:1 !important; }
.cr-saved-detail { flex:1; display:flex; flex-direction:column; overflow:hidden; min-width:0; }
.cr-detail-empty { display:flex; flex-direction:column; align-items:center; justify-content:center; height:100%; }
.cr-detail-header { padding:12px 16px; border-bottom:1px solid var(--border); flex-shrink:0; }
.cr-detail-title { display:flex; align-items:baseline; gap:6px; flex-wrap:wrap; margin-bottom:6px; }
.cr-detail-ep { font-size:.82rem; color:var(--text-secondary); max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cr-detail-sep { color:var(--text-muted); }
.cr-detail-proc { font-size:.82rem; color:var(--blue); font-weight:600; }
.cr-detail-meta { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }

/* ── Sub-tabs ── */
.cr-subtab-bar { display:flex; gap:0; border-bottom:1px solid var(--border); flex-shrink:0; }
.cr-subtab-btn { padding:8px 16px; border:none; background:none; border-bottom:2px solid transparent; cursor:pointer; font-size:.83rem; font-weight:600; color:var(--text-muted); margin-bottom:-1px; }
.cr-subtab-btn.active { color:var(--blue); border-bottom-color:var(--blue); }
.cr-subtab-btn:hover:not(.active) { color:var(--text-secondary); }
.cr-subtab-body { flex:1; overflow-y:auto; min-height:0; }

/* ── Linked Flow sub-tab ── */
.cr-flow-scroll { padding:16px; }
.cr-summary-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(130px,1fr)); gap:8px; margin-bottom:14px; }
.cr-stat { background:var(--bg); border:1px solid var(--border); border-radius:var(--radius-md); padding:10px; text-align:center; }
.cr-stat-val { font-size:1.3rem; font-weight:700; color:var(--blue); }
.cr-stat-lbl { font-size:.72rem; color:var(--text-muted); margin-top:2px; }
.cr-pass-row { font-size:.78rem; color:var(--text-muted); margin:0 0 14px; display:flex; gap:14px; flex-wrap:wrap; }
.cr-section { margin-bottom:20px; }
.cr-section-title { font-size:.88rem; font-weight:700; color:var(--text-secondary); border-bottom:2px solid var(--border); padding-bottom:6px; margin-bottom:10px; }
.cr-gap-section { background:var(--badge-orange-bg); border:1px solid var(--badge-orange-bg); border-radius:var(--radius-md); padding:12px 14px; }
.cr-gap-section .cr-section-title { border-bottom-color:var(--badge-orange-bg); }
.cr-claude-panel { background:var(--accent-subtle); border:1px solid var(--accent-subtle); border-radius:var(--radius-md); padding:12px 14px; margin-bottom:14px; }
.cr-btn-danger { background:var(--badge-red-bg); border-color:var(--badge-red-bg); color:var(--red); }
.cr-gap-summary-hdr { display:flex; align-items:center; padding:6px 0; list-style:none; cursor:pointer; }
.cr-gap-summary-hdr::-webkit-details-marker { display:none; }
details > summary.cr-gap-summary-hdr { padding:4px 0; }
.cr-pkg-group { margin-bottom:10px; border:1px solid var(--accent-subtle); border-radius:var(--radius-md); overflow:hidden; }
.cr-pkg-group-hdr { display:flex; align-items:center; gap:10px; padding:8px 12px; background:var(--badge-blue-bg); cursor:pointer; user-select:none; }
.cr-pkg-group-hdr:hover { background:var(--badge-blue-bg); }
.cr-pkg-name { flex:1; font-weight:700; font-size:.85rem; color:var(--badge-blue); }
.cr-pkg-count { font-size:.75rem; color:var(--text-muted); }
.cr-pkg-chevron { font-size:.75rem; color:var(--text-muted); }
.cr-pkg-group-body { display:none; padding:8px; }
.cr-pkg-group-body.open { display:block; }
.cr-proc-card { background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); padding:10px 12px; margin-bottom:6px; }
.cr-proc-card:last-child { margin-bottom:0; }
.cr-proc-card-hdr { display:flex; align-items:center; gap:8px; cursor:pointer; flex-wrap:wrap; }
.cr-proc-card-hdr h4 { margin:0; font-size:.85rem; font-weight:600; flex:1; min-width:0; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.cr-proc-card-body { display:none; margin-top:8px; }
.cr-proc-card-body.open { display:block; }
.cr-table { width:100%; border-collapse:collapse; font-size:.8rem; }
.cr-table th { background:var(--bg); padding:6px 8px; text-align:left; font-weight:600; color:var(--text-secondary); border-bottom:1px solid var(--border); }
.cr-table td { padding:6px 8px; border-bottom:1px solid var(--border-subtle); vertical-align:top; }
.cr-table tr:hover td { background:var(--bg); }
.cr-table-compact td,.cr-table-compact th { padding:4px 6px; }
.cr-match { color:var(--badge-green); font-weight:600; }
.cr-inferred { color:var(--badge-blue); font-style:italic; }
.cr-java-only { color:var(--badge-orange); }
.cr-oracle-only { color:var(--accent-hover); }
.cr-mismatch { color:var(--badge-red); }

/* ── Split layout (Java→Oracle, Oracle→Java) ── */
.cr-split-layout { display:flex; height:100%; overflow:hidden; }
.cr-split-left { width:310px; flex-shrink:0; border-right:1px solid var(--border); display:flex; flex-direction:column; }
.cr-split-right { flex:1; display:flex; flex-direction:column; min-width:0; }
.cr-split-hdr { padding:8px 12px; font-size:.78rem; font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:.05em; border-bottom:1px solid var(--border); flex-shrink:0; display:flex; align-items:center; gap:6px; }
.cr-split-count { font-weight:400; font-size:.75rem; }
.cr-split-search-row { padding:6px 8px; border-bottom:1px solid var(--border-subtle); flex-shrink:0; }
.cr-split-search { width:100%; padding:5px 8px; border:1px solid var(--border); border-radius:var(--radius); font-size:.82rem; box-sizing:border-box; }
.cr-split-search:focus { outline:none; border-color:var(--blue); }
.cr-split-list { flex:1; overflow-y:auto; }
.cr-split-detail { flex:1; overflow-y:auto; padding:12px; }

/* ── Java→Oracle list items ── */
.cr-j2o-item { padding:8px 12px; cursor:pointer; border-bottom:1px solid var(--border-subtle); transition:background .1s; }
.cr-j2o-item:hover { background:var(--bg); }
.cr-j2o-item.active { background:var(--badge-blue-bg); border-right:2px solid var(--blue); }
.cr-j2o-key { font-size:.82rem; font-weight:600; color:var(--text); font-family:monospace; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.cr-j2o-meta { display:flex; gap:4px; align-items:center; margin-top:3px; flex-wrap:wrap; }
.cr-j2o-detail-hdr { padding:10px 0 10px; border-bottom:1px solid var(--border); margin-bottom:10px; }
.cr-j2o-mapping-card { border:1px solid var(--border); border-radius:var(--radius); padding:10px; margin-bottom:10px; }
.cr-j2o-section-lbl { font-size:.75rem; font-weight:700; color:var(--text-secondary); margin:8px 0 4px; text-transform:uppercase; letter-spacing:.04em; }
.cr-detail-table { width:100%; border-collapse:collapse; font-size:.8rem; margin-bottom:8px; }
.cr-detail-table td { padding:4px 6px; border-bottom:1px solid var(--border-subtle); vertical-align:top; }
.cr-detail-table td:first-child { font-weight:600; color:var(--text-secondary); white-space:nowrap; width:35%; }

/* ── Oracle→Java list items ── */
.cr-o2j-item { padding:7px 12px; cursor:pointer; border-bottom:1px solid var(--border-subtle); transition:background .1s; }
.cr-o2j-item:hover { background:var(--bg); }
.cr-o2j-item.active { background:var(--badge-blue-bg); border-right:2px solid var(--blue); }
.cr-o2j-proc { font-size:.82rem; font-weight:600; font-family:monospace; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.cr-o2j-meta { display:flex; gap:4px; align-items:center; margin-top:2px; flex-wrap:wrap; }
.cr-depth-badge { font-size:.68rem; padding:1px 5px; border-radius:var(--radius-lg); }
.cr-caller-card { padding:8px 10px; margin-bottom:6px; border-radius:var(--radius); background:var(--bg); border:1px solid var(--border); }

/* ── Badges ── */
.cr-badge { display:inline-block; padding:2px 7px; border-radius:var(--radius-lg); font-size:.75rem; font-weight:600; }
.cr-badge-high { background:var(--badge-green-bg); color:var(--badge-green); }
.cr-badge-med  { background:var(--badge-orange-bg); color:var(--badge-orange); }
.cr-badge-low  { background:var(--badge-red-bg); color:var(--badge-red); }
.cr-badge-infra{ background:var(--bg); color:var(--text-muted); }

/* ── Loading / empty states ── */
.cr-loading { padding:16px; color:var(--text-muted); font-size:.83rem; text-align:center; }
.cr-trace-empty { color:var(--text-muted); font-size:.83rem; padding:16px; text-align:center; }

/* ── Tree source viewer (modal + inline tab shared) ── */
.cr-sv-node { transition:background .1s; }
.cr-sv-node:hover { background:var(--badge-orange-bg) !important; }
.cr-sv-node-anchor { font-weight:700; }

/* ── Source Tree inline sub-tab ── */
.cr-tree-tab-wrap { display:flex; flex-direction:column; height:100%; overflow:hidden; }
.cr-tree-toolbar { display:flex; align-items:center; gap:8px; padding:8px 14px; border-bottom:1px solid var(--border); background:var(--bg); flex-shrink:0; flex-wrap:wrap; }
.cr-tree-body { display:flex; flex:1; overflow:hidden; min-height:0; }
.cr-tree-left { width:260px; min-width:180px; flex-shrink:0; display:flex; flex-direction:column; border-right:2px solid var(--border); background:var(--bg-card); }
.cr-tree-center { flex:1.2; display:flex; flex-direction:column; border-right:2px solid var(--border); min-width:0; }
.cr-tree-right  { flex:1; display:flex; flex-direction:column; min-width:0; }
.cr-tree-pane-hdr { padding:6px 12px; font-size:.76rem; font-weight:700; border-bottom:1px solid var(--border); flex-shrink:0; display:flex; align-items:center; }
.cr-tree-pane-hdr-oracle { background:var(--badge-orange-bg); color:var(--badge-orange); }
.cr-tree-pane-hdr-java   { background:var(--accent-subtle); color:var(--accent-hover); }
.cr-tree-nodes { flex:1; overflow-y:auto; overflow-x:hidden; padding:4px 0; }
.cr-tree-src-body { flex:1; overflow:auto; padding:12px 14px; font-family:monospace; font-size:.74rem; white-space:pre; line-height:1.6; color:var(--text); }
.cr-tree-src-oracle { background:var(--badge-orange-bg); }
.cr-tree-src-java   { background:var(--badge-blue-bg); }
.cr-tree-hint { color:var(--text-muted); font-family:sans-serif; white-space:normal; }
</style>`;
    }
};

// ── Helpers ───────────────────────────────────────────────────────────────────
function _relTime(ts) {
    const diff = Date.now() - ts;
    if (diff < 60000) return 'just now';
    if (diff < 3600000) return Math.floor(diff/60000)+'m ago';
    if (diff < 86400000) return Math.floor(diff/3600000)+'h ago';
    return Math.floor(diff/86400000)+'d ago';
}
