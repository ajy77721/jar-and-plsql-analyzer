// Generic modal z-index manager — every modal calls PA.modalStack.nextZ() on open
// so each new modal always appears on top of all currently open ones.
PA.modalStack = (function() {
    var _z = 1060;
    return {
        nextZ: function() { _z += 10; return _z; },
        reset: function() { _z = 1060; }
    };
})();

PA.init = async function() {
    if (PA.home && PA.home.init) PA.home.init();
    window.addEventListener('popstate', function() { PA._handleRoute(); });
    PA._handleRoute();
    if (PA.claude && PA.claude.init) PA.claude.init();
};

PA.loadAnalysis = async function(name, opts) {
    var fromRoute = opts && opts.fromRoute;
    var routeProcId = opts && opts.procId;
    PA.toast('Loading ' + name + '...', 'success');
    try {
        PA.api.setAnalysis(name);
        var result = await PA.api.getIndex();
        if (!result) { PA.toast('Analysis not found', 'error'); return; }
        result.name = name;
        // UI-024 (§165): was `result.nodes = await PA.api.getProcedures()` — an unconditional
        // fetch of every node, on every analysis load, regardless of what any consumer needed.
        // `PA.nodeIndex.ensureLoaded()` is now the only thing that ever populates
        // `PA.analysisData.nodes`, triggered lazily by whichever consumer needs it first
        // (`PA._renderLeftProcedures`, below, for the common below-threshold case). Not set here
        // at all — `ensureLoaded()`'s own identity check (`PA.analysisData.nodes` not yet an
        // array) is what tells it a fetch is needed for this analysis.
        PA.analysisData = result;
        PA._rightTabLoaded = {};
        // Clear stale tab data so export always fetches fresh data for this analysis
        if (PA.cursors) PA.cursors.data = [];
        if (PA.sequences) PA.sequences.data = [];
        if (PA.joins) PA.joins.data = [];
        if (PA.tableOps) { PA.tableOps.data = []; PA.tableOps._loaded = false; }
        // Estate facets are per-analysis: drop their cached details and reload guards.
        PA._detailsSubLoaded.views = false;
        PA._detailsSubLoaded.triggers = false;
        PA._detailsSubLoaded.globals = false;
        PA._detailsSubLoaded.jobs = false;
        if (PA.estateViews) PA.estateViews.reset();
        if (PA.estateTriggers) PA.estateTriggers.reset();
        if (PA.estateGlobals) PA.estateGlobals.reset();
        PA.goAnalysis();
        PA.updateTopbar(result);
        await PA._renderLeftProcedures();
        PA._cachedTablesIndex = null;
        PA._tablesIndexPromise = null;
        PA._renderLeftTables();
        PA._syncTopbarTableCount();   // scalar fetch; the full index stays lazy
        PA.toast('Loaded: ' + (result.entryPoint || name) + ' (' + (result.totalNodes || 0) + ' procs)', 'success');

        var targetHash = '#/analysis/' + encodeURIComponent(name);
        if (window.location.hash !== targetHash && !routeProcId) {
            if (fromRoute) { history.replaceState(null, '', targetHash); }
            else { history.pushState(null, '', targetHash); }
        }

        if (routeProcId) { PA.showProcedure(routeProcId, { fromRoute: true }); }
        else { PA._autoSelectFirstProc(result); }
    } catch (e) {
        PA.toast('Failed: ' + (e.message || e), 'error');
        if (fromRoute) history.replaceState(null, '', '#/home');
    }
};

PA._autoSelectFirstProc = function(result) {
    if (!result) return;
    if (result._packageAnalysis && result.entryPoint) {
        PA.showProcedure(result.entryPoint);
        return;
    }
    if (!result.nodes || !result.nodes.length) return;
    var root = result.nodes.find(function(n) { return n.depth === 0; });
    if (!root) root = result.nodes[0];
    var id = root.nodeId || root.name;
    if (id) PA.showProcedure(id);
};

/**
 * Syncs the topbar table count from a scalar endpoint instead of the whole tables index.
 *
 * The count shown is the *validated* one — the server applies the same predicate the client used to
 * apply after downloading every table, so the displayed number is unchanged.
 */
PA._syncTopbarTableCount = function() {
    return PA.api.getTablesSummary().then(function(d) {
        if (!d || !PA.analysisData) return;
        var n = (typeof d.validatedTableCount === 'number') ? d.validatedTableCount : d.totalTables;
        if (typeof n === 'number') {
            PA.analysisData.totalTables = n;
            // Truthful header breakdown: per-kind counts from the same single streaming pass
            // (TABLE / VIEW / MATERIALIZED_VIEW / TEMP). Kept on analysisData so the sync topbar
            // and the Analysis Totals line can render the real split.
            if (d.tableKindCounts) PA.analysisData.tableKindCounts = d.tableKindCounts;
            PA.updateTopbar(PA.analysisData);
        }
    }).catch(function() { /* topbar keeps the count from the index; not worth a toast */ });
};

PA._tablesIndexPromise = null;
PA._ensureTablesIndex = function() {
    if (PA._cachedTablesIndex) return Promise.resolve(PA._cachedTablesIndex);
    if (PA._tablesIndexPromise) return PA._tablesIndexPromise;
    PA._tablesIndexPromise = PA.api.getTablesIndex().then(function(d) {
        PA._cachedTablesIndex = d;
        // Sync topbar tables count to the validated (filtered) count from the tables index
        if (d && Array.isArray(d.tables) && PA.analysisData) {
            PA.analysisData.totalTables = d.tables.length;
            PA.updateTopbar(PA.analysisData);
        }
        return d;
    }).catch(function() {
        PA._tablesIndexPromise = null;
        return null;
    });
    return PA._tablesIndexPromise;
};

PA.updateTopbar = function(result) {
    var title = document.getElementById('topbarTitle');
    var stats = document.getElementById('topbarStats');
    if (!title || !stats) return;
    title.textContent = result.entryPoint || result.name || '';
    var html = '';
    // Truthful per-type labels: `totalNodes` is the raw node count (procedures + functions +
    // triggers + packages + jobs + views-as-nodes lumped), which used to be shown as "N procs" —
    // the "51 Procs" defect. Where the analysis index carries the per-type breakdown (typeCounts)
    // show the real split; the raw total stays available (title) and is the fallback for older
    // artifacts. Tables/Views come from the tables index (tableKindCounts), synced separately.
    var tc = result.typeCounts || {};
    var pf = (tc.PROCEDURE || 0) + (tc.FUNCTION || 0);
    var trg = tc.TRIGGER || 0;
    var kc = result.tableKindCounts || {};
    var tbl = (typeof kc.TABLE === 'number') ? kc.TABLE : result.totalTables || 0;
    var vw = (typeof kc.VIEW === 'number') ? (kc.VIEW + (kc.MATERIALIZED_VIEW || 0)) : null;
    var rawNodes = result.totalNodes || 0;
    var procsLabel = (tc.PROCEDURE != null || tc.FUNCTION != null) ? pf : rawNodes;
    var procsTitle = (tc.PROCEDURE != null || tc.FUNCTION != null)
        ? (pf + ' procedures and functions (' + rawNodes + ' total nodes incl. triggers/packages)')
        : 'Total procedures and functions analyzed in the call tree';
    html += '<span style="margin-right:6px" title="' + procsTitle + '">' + procsLabel + ' procs</span>';
    if (trg > 0) html += '<span style="margin-right:6px" title="Triggers in the analysis">' + trg + ' triggers</span>';
    html += '<span style="margin-right:6px" title="Persistent database tables accessed by all procedures">' + tbl + ' tables</span>';
    if (vw != null) html += '<span style="margin-right:6px" title="Views and materialized views">' + vw + ' views</span>';
    html += '<span style="margin-right:6px" title="Unique caller→callee pairs in the dependency graph">' + (result.uniqueEdges || result.totalEdges || 0) + ' edges</span>';
    html += '<span style="margin-right:6px" title="Total lines of PL/SQL source code across all procedures">' + (result.totalLinesOfCode || 0).toLocaleString() + ' LOC</span>';
    html += '<span style="margin-right:6px" title="Maximum call depth — deepest nesting level in the call tree">depth ' + (result.maxDepth || 0) + '</span>';
    var errors = (result.errors && result.errors.length) || 0;
    if (errors > 0) html += '<span class="topbar-error-link" onclick="PA.errorModal.show()">' + errors + ' errors</span>';
    stats.innerHTML = html;

    // Render connection info strip
    PA._updateConnStrip(result);

    // Hide the Oracle-only "Static" analysis toggle for non-Oracle analyses
    if (PA.claude && PA.claude.applyDialect) PA.claude.applyDialect(result);
};

PA._updateConnStrip = function(result) {
    var strip = document.getElementById('topbarConnStrip');
    if (!strip) return;
    var proj = result.project || '';
    var env = result.environment || '';
    var connName = result.connectionName || '';
    var jdbcUrl = result.jdbcUrl || '';
    var schema = result.entrySchema || '';

    // Always show strip when an analysis is loaded
    strip.style.display = 'flex';

    // Extract host from jdbcUrl for compact display
    var host = '';
    if (jdbcUrl) {
        var m = jdbcUrl.match(/\/\/([^:/]+)/);
        if (m) host = m[1];
    }

    // Dialect badge — from dbType, else inferred from the jdbcUrl prefix.
    var dbType = (result.dbType || '').toUpperCase();
    if (!dbType && jdbcUrl) {
        var ju = jdbcUrl.toLowerCase();
        dbType = ju.indexOf('sqlserver') >= 0 ? 'SQLSERVER'
            : ju.indexOf(':mysql') >= 0 ? 'MYSQL'
            : ju.indexOf(':db2') >= 0 ? 'DB2'
            : ju.indexOf(':postgresql') >= 0 ? 'POSTGRES'
            : ju.indexOf(':oracle') >= 0 ? 'ORACLE' : '';
    }
    PA._dbType = dbType || PA._dbType || '';   // stash for views that need the dialect (e.g. Transactions Details)

    var html = '';
    if (dbType && PA.dbSelector && PA.dbSelector.badgeHtml) html += PA.dbSelector.badgeHtml(dbType);
    if (proj) html += '<span class="conn-strip-badge proj" title="Project">' + PA.esc(proj) + '</span>';
    if (env)  html += '<span class="conn-strip-badge env"  title="Environment">' + PA.esc(env) + '</span>';
    if (schema && !proj) html += '<span class="conn-strip-badge conn" title="Schema">' + PA.esc(schema) + '</span>';
    if (connName) html += '<span class="conn-strip-badge conn" title="Connections used: ' + PA.escAttr(connName) + '">' + PA.esc(connName) + '</span>';
    if (host) html += '<span class="conn-strip-host" title="' + PA.escAttr(jdbcUrl) + '">' + PA.esc(host) + '</span>';

    // Connection button — opens conn manager if proj+env known, otherwise shows info
    if (proj && env) {
        html += '<span class="conn-strip-test conn-strip-lock" id="connTestBadge" title="Click to manage connections" onclick="PA.connMgr.open(\'' + PA.escJs(proj) + '\',\'' + PA.escJs(env) + '\')">&#8212;</span>';
    } else {
        // No project/env stored — show an edit button that opens conn manager with whatever we have
        html += '<span class="conn-strip-edit-btn" id="connTestBadge" title="Edit connection" onclick="PA.connMgr.open(\'' + PA.escJs(proj) + '\',\'' + PA.escJs(env) + '\')">&#9881; Edit</span>';
    }
    strip.innerHTML = html;

    // Run connection test asynchronously using stored credentials
    if (proj && env) {
        PA._runConnTest(proj, env);
    }
};

PA._runConnTest = function(proj, env) {
    var badge = document.getElementById('connTestBadge');
    if (badge) { badge.textContent = '...'; badge.className = 'conn-strip-test testing'; }
    fetch('/api/plsql/config/projects/' + encodeURIComponent(proj) + '/environments/' + encodeURIComponent(env) + '/quick-test')
        .then(function(r) { return r.ok ? r.json() : null; })
        .then(function(res) {
            var b = document.getElementById('connTestBadge');
            if (!b) return;
            if (!res) { b.textContent = '?'; b.className = 'conn-strip-test fail'; return; }
            if (res.success) {
                b.textContent = '✓ Connection';
                b.className = 'conn-strip-test ok';
                b.title = res.message || 'Connected';
            } else {
                b.textContent = '✗ Connection';
                b.className = 'conn-strip-test fail';
                b.title = res.message || 'Failed';
            }
        })
        .catch(function() {
            var b = document.getElementById('connTestBadge');
            if (b) { b.textContent = '?'; b.className = 'conn-strip-test fail'; }
        });
};

// ── Connection Manager Modal ─────────────────────────────────────────────────
PA.connMgr = (function() {
    var _proj = '', _env = '', _conns = [];

    function open(proj, env) {
        _proj = proj; _env = env;
        _ensureOverlay();
        _render();
        document.getElementById('connMgrOverlay').style.display = 'flex';
        _load();
    }

    function close() {
        var el = document.getElementById('connMgrOverlay');
        if (el) el.style.display = 'none';
    }

    function _ensureOverlay() {
        if (document.getElementById('connMgrOverlay')) return;
        var el = document.createElement('div');
        el.id = 'connMgrOverlay';
        el.className = 'conn-mgr-overlay';
        el.innerHTML =
            '<div class="conn-mgr-modal">' +
            '  <div class="conn-mgr-header">' +
            '    <span class="conn-mgr-title" id="connMgrTitle">Connection Manager</span>' +
            '    <button class="btn btn-sm" onclick="PA.connMgr.close()" title="Close">&times;</button>' +
            '  </div>' +
            '  <div class="conn-mgr-body" id="connMgrBody"><div style="padding:24px;color:var(--text-muted)">Loading…</div></div>' +
            '</div>';
        el.addEventListener('click', function(e) { if (e.target === el) close(); });
        document.body.appendChild(el);
    }

    function _render() {
        var t = document.getElementById('connMgrTitle');
        if (t) t.textContent = _proj + ' / ' + _env + ' — Connections';
    }

    function _load() {
        var body = document.getElementById('connMgrBody');
        if (!body) return;
        body.innerHTML = '<div style="padding:24px;color:var(--text-muted)">Loading…</div>';
        fetch('/api/plsql/config/projects/' + encodeURIComponent(_proj) + '/environments/' + encodeURIComponent(_env) + '/connections')
            .then(function(r) { return r.ok ? r.json() : Promise.reject(r.status); })
            .then(function(conns) {
                _conns = conns || [];
                _renderConns(body);
            })
            .catch(function(err) {
                body.innerHTML = '<div style="padding:16px;color:var(--badge-red)">Failed to load connections: ' + PA.esc(String(err)) + '</div>';
            });
    }

    function _renderConns(body) {
        if (!_conns.length) {
            body.innerHTML = '<div style="padding:16px;color:var(--text-muted)">No connections configured for this environment.</div>';
            return;
        }
        var html = '<div class="conn-mgr-list">';
        for (var i = 0; i < _conns.length; i++) {
            var c = _conns[i];
            var connId = 'cm_' + i;
            var host = c.hostname || '';
            var port = c.port || 1521;
            var svc  = c.serviceName || c.sid || c.tnsAlias || '';
            var desc = c.description || c.name || '';
            var hasSavedPwd = !!(c.password);
            // Pre-seed local test state for connections that already have a saved password
            if (hasSavedPwd && c.tested) {
                _conns[i]._testedPwd = c.password;
                _conns[i]._testOk   = true;
            }
            html += '<div class="conn-mgr-row" id="connRow_' + i + '">';
            html += '  <div class="conn-mgr-row-info">';
            html += '    <span class="conn-mgr-name">' + PA.esc(c.name || '') + '</span>';
            html += '    <span class="conn-mgr-meta">' + PA.esc(c.username || '') + '@' + PA.esc(host);
            if (svc) html += ':' + PA.esc(String(port)) + '/' + PA.esc(svc);
            html += '</span>';
            if (desc && desc !== c.name) html += '    <span class="conn-mgr-desc">' + PA.esc(desc) + '</span>';
            html += '  </div>';
            html += '  <div class="conn-mgr-row-actions">';
            html += '    <div class="conn-mgr-pwd-wrap">';
            // Pre-populate with saved password so user sees it is set; they can edit and re-test
            html += '      <input type="password" class="conn-mgr-pwd form-input" id="' + connId + '_pwd" placeholder="Password" value="' + PA.esc(c.password || '') + '" autocomplete="new-password" oninput="PA.connMgr._onPwdChange(' + i + ')">';
            html += '      <button class="btn btn-sm conn-mgr-eye" type="button" title="Show/hide password" onclick="PA.connMgr._togglePwd(\'' + connId + '_pwd\')" tabindex="-1">&#128065;</button>';
            html += '    </div>';
            html += '    <button class="btn btn-sm" id="' + connId + '_testBtn" onclick="PA.connMgr._test(' + i + ')" title="Test connection with entered password">Test</button>';
            html += '    <span class="conn-mgr-status" id="' + connId + '_status">' + (hasSavedPwd && c.tested ? '<span style="color:var(--green);font-size:10px;font-weight:700">&#10003; Saved</span>' : '') + '</span>';
            html += '    <button class="btn btn-sm btn-primary conn-mgr-save" id="' + connId + '_saveBtn" onclick="PA.connMgr._save(' + i + ')"' + (hasSavedPwd && c.tested ? '' : ' disabled') + ' title="Save password">Save</button>';
            html += '  </div>';
            html += '</div>';
        }
        html += '</div>';
        body.innerHTML = html;
    }

    function _togglePwd(inputId) {
        var inp = document.getElementById(inputId);
        if (!inp) return;
        inp.type = inp.type === 'password' ? 'text' : 'password';
    }

    function _onPwdChange(idx) {
        // User edited the password — require re-test before Save
        var c = _conns[idx];
        if (!c) return;
        var connId = 'cm_' + idx;
        var pwdEl    = document.getElementById(connId + '_pwd');
        var saveBtn  = document.getElementById(connId + '_saveBtn');
        var statusEl = document.getElementById(connId + '_status');
        var newPwd = pwdEl ? pwdEl.value : '';
        // If they typed back the original saved password, restore the saved-ok state
        if (c.password && newPwd === c.password && c.tested) {
            _conns[idx]._testedPwd = c.password;
            _conns[idx]._testOk   = true;
            if (saveBtn) saveBtn.disabled = false;
            if (statusEl) statusEl.innerHTML = '<span style="color:var(--green);font-size:10px;font-weight:700">&#10003; Saved</span>';
        } else {
            _conns[idx]._testOk = false;
            if (saveBtn) saveBtn.disabled = true;
            if (statusEl) statusEl.textContent = '';
        }
    }

    function _test(idx) {
        var c = _conns[idx];
        if (!c) return;
        var connId = 'cm_' + idx;
        var pwdEl = document.getElementById(connId + '_pwd');
        var statusEl = document.getElementById(connId + '_status');
        var saveBtn = document.getElementById(connId + '_saveBtn');
        var testBtn = document.getElementById(connId + '_testBtn');
        var pwd = pwdEl ? pwdEl.value : '';

        if (statusEl) { statusEl.textContent = 'Testing…'; statusEl.className = 'conn-mgr-status testing'; }
        if (testBtn) testBtn.disabled = true;
        if (saveBtn) saveBtn.disabled = true;

        var payload = Object.assign({}, c, { password: pwd, tested: false });
        fetch('/api/plsql/config/projects/' + encodeURIComponent(_proj) + '/environments/' + encodeURIComponent(_env) + '/connections/test', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(function(r) { return r.json(); })
        .then(function(res) {
            if (testBtn) testBtn.disabled = false;
            if (res.success) {
                if (statusEl) { statusEl.textContent = '✓ OK'; statusEl.className = 'conn-mgr-status ok'; statusEl.title = res.message || ''; }
                if (saveBtn) saveBtn.disabled = false;
                // Mark tested in local copy so save knows pwd was tested
                _conns[idx]._testedPwd = pwd;
                _conns[idx]._testOk = true;
            } else {
                if (statusEl) { statusEl.textContent = '✗ ' + (res.message || 'Failed'); statusEl.className = 'conn-mgr-status fail'; statusEl.title = res.message || ''; }
                _conns[idx]._testOk = false;
            }
        })
        .catch(function(e) {
            if (testBtn) testBtn.disabled = false;
            if (statusEl) { statusEl.textContent = '✗ Error'; statusEl.className = 'conn-mgr-status fail'; statusEl.title = String(e); }
        });
    }

    function _save(idx) {
        var c = _conns[idx];
        if (!c || !c._testOk) return;
        var connId = 'cm_' + idx;
        var saveBtn = document.getElementById(connId + '_saveBtn');
        var statusEl = document.getElementById(connId + '_status');
        if (saveBtn) saveBtn.disabled = true;

        var payload = Object.assign({}, c, { password: c._testedPwd, tested: true });
        delete payload._testedPwd; delete payload._testOk;

        fetch('/api/plsql/config/projects/' + encodeURIComponent(_proj) + '/environments/' + encodeURIComponent(_env) + '/connections/' + encodeURIComponent(c.name), {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(function(r) { return r.json(); })
        .then(function(res) {
            if (res.error) {
                if (statusEl) { statusEl.textContent = '✗ ' + res.error; statusEl.className = 'conn-mgr-status fail'; }
                if (saveBtn) saveBtn.disabled = false;
            } else {
                if (statusEl) { statusEl.textContent = '✓ Saved'; statusEl.className = 'conn-mgr-status ok'; }
                PA.toast('Connection \'' + (c.name || '') + '\' saved', 'success');
                // Update local cache
                _conns[idx] = Object.assign({}, res, { _testOk: false });
            }
        })
        .catch(function(e) {
            if (statusEl) { statusEl.textContent = '✗ Error'; statusEl.className = 'conn-mgr-status fail'; }
            if (saveBtn) saveBtn.disabled = false;
        });
    }

    return { open: open, close: close, _togglePwd: _togglePwd, _onPwdChange: _onPwdChange, _test: _test, _save: _save };
})();

PA._showProcGen = 0;

PA._showLoadingDetail = function(procId) {
    var header = document.getElementById('detailHeader');
    if (header) header.style.display = '';
    var fullNameEl = document.getElementById('dhFullName');
    if (fullNameEl) fullNameEl.textContent = procId || 'Loading…';
    var meta = document.getElementById('dhMeta');
    if (meta) meta.innerHTML = '<span style="color:var(--text-muted);font-size:11px">Loading…</span>';
    var statsEl = document.getElementById('dhStats');
    if (statsEl) statsEl.innerHTML = '';
    var badgesEl = document.getElementById('dhBadges');
    if (badgesEl) badgesEl.innerHTML = '';
    var navEl = document.getElementById('dhNav');
    if (navEl) navEl.style.display = 'none';
    var ctContainer = document.getElementById('ctContainer');
    if (ctContainer) ctContainer.innerHTML = '<div class="empty-msg" style="padding:24px;color:var(--text-muted)">Loading…</div>';
    var traceContainer = document.getElementById('traceContainer');
    if (traceContainer) traceContainer.innerHTML = '';
};

PA._clearTabContents = function() {
    var ids = ['toContainer', 'joinContainer', 'jsContainer', 'curContainer', 'seqContainer',
               'stmtContainer', 'exhContainer', 'summaryContainer', 'claudeInsightsContainer',
               'claudeCorrectionsContainer', 'graphContainer',
               'vwContainer', 'tgContainer', 'glContainer', 'jbContainer'];
    for (var i = 0; i < ids.length; i++) {
        var el = document.getElementById(ids[i]);
        if (el) el.innerHTML = '';
    }
    if (PA.tf && PA.tf._state) {
        var tabKeys = ['to', 'join', 'js', 'cur', 'seq', 'stmt', 'exh', 'vw', 'tg', 'gl', 'jb'];
        for (var j = 0; j < tabKeys.length; j++) {
            delete PA.tf._state[tabKeys[j]];
        }
    }
    PA._detailsSubLoaded.views = false;
    PA._detailsSubLoaded.triggers = false;
    PA._detailsSubLoaded.globals = false;
    PA._detailsSubLoaded.jobs = false;
    PA.summaryTab.teardown();
};

PA.showProcedure = async function(procId, opts) {
    var fromRoute = opts && opts.fromRoute;
    var gen = ++PA._showProcGen;
    // Cancel any pending loadNodeDetail hover-fetch
    clearTimeout(PA._loadNodeDetailTimer);
    ++PA._loadNodeDetailGen;

    PA.currentDetail = null;
    PA._rightTabLoaded = {};
    PA._clearTabContents();
    PA._showLoadingDetail(procId);
    PA._scope = 'direct';
    PA._renderScopeToggle();
    PA.refs._loaded = false;
    PA.refs.callsItems = [];
    PA.refs.callersItems = [];
    PA.sourceModal._history = [];
    PA.sourceModal._historyFwd = [];
    PA.sourceModal._searchMatches = [];
    PA.sourceModal._searchIdx = -1;

    document.querySelectorAll('.lp-item').forEach(function(el) { el.classList.remove('active'); });
    var item = document.querySelector('.lp-item[data-id="' + CSS.escape(procId) + '"]');
    if (item) { item.classList.add('active'); item.scrollIntoView({ block: 'nearest' }); }

    PA.context.procId = procId;
    PA.switchRightTab('explore');

    if (PA.analysisData && PA.analysisData.name) {
        var targetHash = '#/analysis/' + encodeURIComponent(PA.analysisData.name) + '/proc/' + encodeURIComponent(procId);
        if (window.location.hash !== targetHash) {
            if (fromRoute) { history.replaceState(null, '', targetHash); }
            else { history.pushState(null, '', targetHash); }
        }
    }

    var node = await PA._findNodeInData(procId);
    var detailFile = node ? node.detailFile : null;

    // Package root: create synthetic detail and set scope synchronously
    var _isPkgRootCall = !detailFile && PA.analysisData && PA.analysisData._packageAnalysis &&
            PA.analysisData.entryPoint &&
            procId.toUpperCase() === PA.analysisData.entryPoint.toUpperCase();
    if (_isPkgRootCall) {
        var idx = PA.analysisData;
        var pkgDetail = {
            nodeId: procId,
            name: idx.entryPoint || procId,
            schema: idx.entrySchema || '',
            objectType: 'PACKAGE',
            packageName: '',
            depth: 0,
            linesOfCode: idx.totalLinesOfCode || 0,
            counts: {
                tables: idx.totalTables || 0,
                internalCalls: 0,
                externalCalls: idx.uniqueEdges || idx.totalEdges || 0
            },
            tables: [],
            _isPkgRoot: true
        };
        PA.currentDetail = pkgDetail;
        PA.context.procDetail = pkgDetail;
        PA.context.scopedTables = [];
        // Set scope context NOW so tabs that load before the tree fetch use correct scope. UI-024
        // (§162): _buildNodeIdMap is async now (awaits PA.nodeIndex.ensureLoaded()), but resolves
        // in the same microtask whenever the array is already resident — which it always is today,
        // since PA.loadAnalysis's own eager fetch still runs first. No observable timing change
        // unless that eager fetch is later removed, at which point this await is the real guard.
        var allDollarIds = new Set();
        var idMapSync = await PA._buildNodeIdMap();
        (await PA.nodeIndex.ensureLoaded()).forEach(function(n) {
            var uid = (n.nodeId || '').toUpperCase();
            if (uid) allDollarIds.add(uid);
            if (idMapSync[uid]) allDollarIds.add(idMapSync[uid]);
        });
        PA.context.callTreeNodeIds = allDollarIds;
        PA.context.callTreeRootId = '__PKG_ROOT__';
        PA._rightTabLoaded = {};
        PA._updateDetailHeader(pkgDetail);
    }

    if (detailFile) {
        try {
            var detail = await PA.api.getNodeDetail(detailFile.replace(/^nodes\//, ''));
            if (gen !== PA._showProcGen) return;
            if (detail) {
                if (node) {
                    detail.nodeId = node.nodeId || procId;
                    detail.schema = detail.schema || node.schema;
                    detail.packageName = detail.packageName || node.packageName;
                    detail.objectType = detail.objectType || node.objectType;
                    detail.depth = detail.depth != null ? detail.depth : node.depth;
                    detail.lineStart = detail.lineStart || node.lineStart;
                    detail.lineEnd = detail.lineEnd || node.lineEnd;
                    detail.linesOfCode = detail.linesOfCode || node.linesOfCode;
                    detail.sourceFile = detail.sourceFile || node.sourceFile;
                    if (node.readable === false) detail.readable = false;
                    if (!detail.counts) detail.counts = node.counts || {};
                    else if (node.counts) {
                        if (!detail.counts.subtreeTablesCount && node.counts.subtreeTablesCount) detail.counts.subtreeTablesCount = node.counts.subtreeTablesCount;
                        if (!detail.counts.subtreeNodesCount && node.counts.subtreeNodesCount) detail.counts.subtreeNodesCount = node.counts.subtreeNodesCount;
                        if (!detail.counts.flowLinesOfCode && node.counts.flowLinesOfCode) detail.counts.flowLinesOfCode = node.counts.flowLinesOfCode;
                    }
                }
                PA.currentDetail = detail;
                PA._updateDetailHeader(detail);
                PA.context.procDetail = detail;
                PA.context.scopedTables = detail.tables || detail.subtreeTables || [];
                PA._rightTabLoaded = {};
            }
        } catch (e) {
            console.warn('[PA] Failed to load node detail for', procId, e);
        }
    }

    try {
        var treeData = await PA.api.getCallTree(procId);
        if (gen !== PA._showProcGen) return;
        if (treeData) {
            if (!_isPkgRootCall) {
                // UI-024 (§162): idMap resolved once here, then threaded through the whole
                // recursion — _collectTreeNodeIds itself stays synchronous.
                PA.context.callTreeNodeIds = PA._collectTreeNodeIds(treeData, undefined, await PA._buildNodeIdMap());
                PA.context.callTreeRootId = (treeData.id || procId).toUpperCase();
            }
            PA.callTrace.treeData = treeData;
            PA.callTrace.breadcrumbStack = [{ id: treeData.id || procId, name: treeData.name || procId }];
            // PA.callTrace.render() itself now also drives PA.trace.renderFromTree() (see callGraph.js) —
            // Flat mode of the merged Hierarchy view is kept in sync on every render, not just here.
            await PA.callTrace.render(treeData);
        } else {
            document.getElementById('ctContainer').innerHTML = '<div class="empty-msg">No call tree data for: ' + PA.esc(procId) + '</div>';
        }
    } catch (e) {
        console.error('[PA] callTrace.load failed for', procId, e);
        document.getElementById('ctContainer').innerHTML = '<div class="empty-msg">Failed to load call tree for: ' + PA.esc(procId) + '</div>';
    }
};

/**
 * UI-024 — was a `PA.nodeIndex.all()` linear scan checking `nodeId === procId || name === procId`
 * per node in one pass. `nodeIndex.js`'s own header doc says exactly this class of `.find()` scan
 * is what its O(1) maps exist to replace (11 of them were named there directly) — this was simply
 * one nobody had gotten to yet. `byId` then `byName`, in that order, matches the original's own
 * field-check order for the overwhelming majority of real data; the one case they can disagree on
 * is a node whose `name` collides with a *different* node's `nodeId` positioned earlier in the
 * array, which the original's single combined-condition scan would have returned first — judged
 * negligible for real analyses (ids are schema$pkg$proc-formatted, names are bare identifiers) and
 * documented here rather than silently assumed identical.
 *
 * <p>UI-024 (§161): now genuinely lazy, not just O(1) — awaits `PA.nodeIndex.ensureLoaded()`
 * before the lookup instead of relying on `PA.loadAnalysis`'s own eager fetch to have already
 * populated the index. Safe to convert to `async` because every one of its 3 real call sites
 * (`app.js` ×2, `sourceView.js` ×1) is already inside an async function — checked directly, not
 * assumed, before making this change.
 */
PA._findNodeInData = async function(procId) {
    if (!procId) return null;
    await PA.nodeIndex.ensureLoaded();
    return PA.nodeIndex.byId(procId) || PA.nodeIndex.byName(procId) || null;
};

PA._loadNodeDetailTimer = null;
PA._loadNodeDetailGen = 0;
PA.loadNodeDetail = function(procId) {
    if (PA.context.procId === procId) return;
    clearTimeout(PA._loadNodeDetailTimer);
    var gen = ++PA._loadNodeDetailGen;
    PA._loadNodeDetailTimer = setTimeout(async function() {
        // Another call came in while we were waiting — abort
        if (gen !== PA._loadNodeDetailGen) return;
        var node = await PA._findNodeInData(procId);
        var detailFile = node ? node.detailFile : null;
        if (!detailFile) return;
        try {
            var detail = await PA.api.getNodeDetail(detailFile.replace(/^nodes\//, ''));
            // Stale check after async gap: a newer call may have started
            if (gen !== PA._loadNodeDetailGen) return;
            if (detail) {
                if (node) {
                    detail.nodeId = node.nodeId || procId;
                    detail.schema = detail.schema || node.schema;
                    detail.objectType = detail.objectType || node.objectType;
                    detail.depth = detail.depth != null ? detail.depth : node.depth;
                    detail.lineStart = detail.lineStart || node.lineStart;
                    detail.lineEnd = detail.lineEnd || node.lineEnd;
                    detail.linesOfCode = detail.linesOfCode || node.linesOfCode;
                    detail.sourceFile = detail.sourceFile || node.sourceFile;
                    detail.packageName = detail.packageName || node.packageName;
                    if (node.readable === false) detail.readable = false;
                    if (!detail.counts) detail.counts = node.counts || {};
                    else if (node.counts) {
                        if (!detail.counts.subtreeTablesCount && node.counts.subtreeTablesCount) detail.counts.subtreeTablesCount = node.counts.subtreeTablesCount;
                        if (!detail.counts.subtreeNodesCount && node.counts.subtreeNodesCount) detail.counts.subtreeNodesCount = node.counts.subtreeNodesCount;
                        if (!detail.counts.flowLinesOfCode && node.counts.flowLinesOfCode) detail.counts.flowLinesOfCode = node.counts.flowLinesOfCode;
                    }
                }
                PA.currentDetail = detail;
                PA._updateDetailHeader(detail);
                PA.context.procId = procId;
                PA.context.procDetail = detail;
                PA.context.scopedTables = detail.tables || detail.subtreeTables || [];
                PA._rightTabLoaded = {};

                // Update scope context so tab filters use the real dollar-format nodeId
                try {
                    var treeData = await PA.api.getCallTree(procId);
                    if (gen !== PA._loadNodeDetailGen) return;
                    if (treeData) {
                        // UI-024 (§162): idMap resolved once here, then threaded through the
                        // recursion — see the other call site's own identical comment.
                        PA.context.callTreeNodeIds = PA._collectTreeNodeIds(treeData, undefined, await PA._buildNodeIdMap());
                        PA.context.callTreeRootId = (treeData.id || procId).toUpperCase();
                    }
                } catch (te) {
                    if (detail.nodeId) PA.context.callTreeRootId = detail.nodeId.toUpperCase();
                }

                document.querySelectorAll('.lp-item').forEach(function(el) { el.classList.remove('active'); });
                var item = document.querySelector('.lp-item[data-id="' + CSS.escape(procId) + '"]');
                if (item) item.classList.add('active');
            }
        } catch (e) {
            console.warn('[PA] loadNodeDetail failed for', procId, e);
        }
    }, 150);
};

PA._heavyTabs = { tableOps: 'toContainer',
    complexity: 'cxContainer', callGraphViz: 'graphContainer' };
PA._heavyTabTfKeys = { tableOps: 'to', complexity: 'cx' };

PA._detailsSubLoaded = { sequences: false, joinQuery: false, dynamic: false,
    transactions: false, cursors: false, statements: false, exHandlers: false,
    /* Estate facets (views/MVs, triggers, package globals, scheduled jobs) now live as the last
       four Details sub-tabs — analysis-wide, not node-scoped, loaded once per visit. */
    views: false, triggers: false, globals: false, jobs: false };
PA._detailsCurrentSub = 'sequences';

PA._exploreCurrentSub = 'hierarchy';

/**
 * Hierarchy/Trace merge (UI consolidation): the two former sub-tabs rendered the SAME
 * `treeData` fetched once by `PA.showProcedure` (Hierarchy via `PA.callTrace.render`, Trace via
 * `PA.trace.renderFromTree`) — they are now one sub-tab ("Hierarchy") with a Tree/Flat layout
 * toggle instead of two tab buttons. `PA._hierarchyMode` tracks which layout is visible;
 * `PA.callTrace.render()` keeps BOTH layouts' DOM populated on every navigation (load,
 * drillDown, Make Root, breadcrumb) regardless of which one is shown, so switching the toggle
 * never needs a fetch or a re-render — it only flips visibility.
 */
PA._hierarchyMode = 'tree';

PA.exploreTab = {
    switchSub: function(sub) {
        PA._exploreCurrentSub = sub;
        document.querySelectorAll('#viewExplore .dt-subtab').forEach(function(b) {
            b.classList.toggle('active', b.dataset.exview === sub);
        });
        document.querySelectorAll('#viewExplore .dt-subview').forEach(function(v) { v.classList.remove('active'); });
        var viewMap = { hierarchy: 'exViewHierarchy', references: 'exViewReferences', txn: 'exViewTxn' };
        var el = document.getElementById(viewMap[sub]);
        if (el) el.classList.add('active');
        PA.exploreTab._loadSub(sub);
    },
    _loadSub: function(sub) {
        if (sub === 'references' && PA.context.procId && !PA.refs._loaded) {
            PA.refs.load(PA.context.procId);
        }
        if (sub === 'txn' && PA.txnTrace) {
            PA.txnTrace.render();
        }
    },
    /** Tree/Flat layout toggle within the merged Hierarchy sub-tab — pure visibility switch. */
    setHierarchyMode: function(mode) {
        PA._hierarchyMode = mode;
        var treeOn = mode !== 'flat';
        document.querySelectorAll('#hierarchyModeTabs .mode-tab').forEach(function(b) {
            b.classList.toggle('active', b.dataset.mode === mode);
        });
        var ctContainer = document.getElementById('ctContainer');
        var traceContainer = document.getElementById('traceContainer');
        var treeControls = document.getElementById('ctTreeControls');
        var flatControls = document.getElementById('ctFlatControls');
        if (ctContainer) ctContainer.style.display = treeOn ? '' : 'none';
        if (traceContainer) traceContainer.style.display = treeOn ? 'none' : '';
        if (treeControls) treeControls.style.display = treeOn ? '' : 'none';
        if (flatControls) flatControls.style.display = treeOn ? 'none' : '';
    },
    applyScope: function() {
        if (PA._exploreCurrentSub === 'references' && PA.refs._loaded) {
            PA.refs._buildTable();
        }
        if (PA._exploreCurrentSub === 'txn' && PA.txnTrace) {
            PA.txnTrace.render();
        }
    }
};

PA._summaryCurrentSub = 'dashboard';
PA._summarySubLoaded = { dashboard: false, claudeInsights: false, claudeCorrections: false };

PA.summaryTab = {
    switchSub: function(sub) {
        PA._summaryCurrentSub = sub;
        document.querySelectorAll('#viewSummary .dt-subtab').forEach(function(b) {
            b.classList.toggle('active', b.dataset.sumview === sub);
        });
        document.querySelectorAll('#viewSummary .dt-subview').forEach(function(v) { v.classList.remove('active'); });
        var viewMap = { dashboard: 'sumViewDashboard', claudeInsights: 'sumViewClaudeInsights', claudeCorrections: 'sumViewClaudeCorrections' };
        var el = document.getElementById(viewMap[sub]);
        if (el) el.classList.add('active');
        PA.summaryTab._loadSub(sub);
    },
    _loadSub: function(sub) {
        if (sub === 'dashboard' && !PA._summarySubLoaded.dashboard) {
            PA._summarySubLoaded.dashboard = true;
            if (PA.summary && PA.summary.load) PA.summary.load();
        }
        if (sub === 'claudeInsights' && !PA._summarySubLoaded.claudeInsights) {
            PA._summarySubLoaded.claudeInsights = true;
            if (PA.claudeInsights && PA.claudeInsights.load) PA.claudeInsights.load();
        }
        if (sub === 'claudeCorrections' && !PA._summarySubLoaded.claudeCorrections) {
            PA._summarySubLoaded.claudeCorrections = true;
            if (PA.claudeCorrections && PA.claudeCorrections.load) PA.claudeCorrections.load();
        }
    },
    teardown: function() {
        PA._summarySubLoaded = { dashboard: false, claudeInsights: false, claudeCorrections: false };
    }
};

PA.detailsTab = {
    switchSub: function(sub) {
        PA._detailsCurrentSub = sub;
        document.querySelectorAll('#viewDetails .dt-subtab').forEach(function(b) {
            b.classList.toggle('active', b.dataset.dtview === sub);
        });
        document.querySelectorAll('#viewDetails .dt-subview').forEach(function(v) { v.classList.remove('active'); });
        var viewMap = { sequences: 'dtViewSequences', joinQuery: 'dtViewJoinQuery', dynamic: 'dtViewDynamic',
            transactions: 'dtViewTransactions', cursors: 'dtViewCursors', statements: 'dtViewStatements',
            exHandlers: 'dtViewExHandlers',
            /* Estate sections hosted inside Details (were the top-level Estate tab). */
            views: 'estViewViews', triggers: 'estViewTriggers', globals: 'estViewGlobals', jobs: 'estViewJobs' };
        var el = document.getElementById(viewMap[sub]);
        if (el) el.classList.add('active');
        PA.detailsTab._loadSub(sub);
    },
    _loadSub: function(sub) {
        if (sub === 'sequences' && !PA._detailsSubLoaded.sequences) {
            PA._detailsSubLoaded.sequences = true;
            if (PA.sequences && PA.sequences.load) PA.sequences.load();
        }
        if (sub === 'joinQuery' && !PA._detailsSubLoaded.joinQuery) {
            PA._detailsSubLoaded.joinQuery = true;
            if (PA.joinSummary && PA.joinSummary.loadQueryOnly) PA.joinSummary.loadQueryOnly();
        }
        if (sub === 'dynamic' && !PA._detailsSubLoaded.dynamic) {
            PA._detailsSubLoaded.dynamic = true;
            if (PA.dynamicSql && PA.dynamicSql.load) PA.dynamicSql.load();
        }
        if (sub === 'transactions' && !PA._detailsSubLoaded.transactions) {
            PA._detailsSubLoaded.transactions = true;
            if (PA.transactions && PA.transactions.load) PA.transactions.load();
        }
        if (sub === 'cursors' && !PA._detailsSubLoaded.cursors) {
            PA._detailsSubLoaded.cursors = true;
            if (PA.cursors && PA.cursors.load) PA.cursors.load();
        }
        if (sub === 'statements' && !PA._detailsSubLoaded.statements) {
            PA._detailsSubLoaded.statements = true;
            if (PA.statements && PA.statements.load) PA.statements.load();
        }
        if (sub === 'exHandlers' && !PA._detailsSubLoaded.exHandlers) {
            PA._detailsSubLoaded.exHandlers = true;
            if (PA.exHandlers && PA.exHandlers.load) PA.exHandlers.load();
        }
        /* Estate sections (lazy-load once per analysis, same as the other detail facets). */
        if (sub === 'views' && !PA._detailsSubLoaded.views) {
            PA._detailsSubLoaded.views = true;
            if (PA.estateViews && PA.estateViews.load) PA.estateViews.load();
        }
        if (sub === 'triggers' && !PA._detailsSubLoaded.triggers) {
            PA._detailsSubLoaded.triggers = true;
            if (PA.estateTriggers && PA.estateTriggers.load) PA.estateTriggers.load();
        }
        if (sub === 'globals' && !PA._detailsSubLoaded.globals) {
            PA._detailsSubLoaded.globals = true;
            if (PA.estateGlobals && PA.estateGlobals.load) PA.estateGlobals.load();
        }
        if (sub === 'jobs' && !PA._detailsSubLoaded.jobs) {
            PA._detailsSubLoaded.jobs = true;
            if (PA.estateJobs && PA.estateJobs.load) PA.estateJobs.load();
        }
    },
    teardown: function() {
        var subs = [
            { key: 'sequences', container: 'seqContainer', tfKey: 'seq' },
            { key: 'joinQuery', container: 'jqContainer', tfKey: 'jq' },
            { key: 'dynamic', container: 'dynContainer', tfKey: 'dyn' },
            { key: 'transactions', container: 'txnContainer', tfKey: null },
            { key: 'cursors', container: 'curContainer', tfKey: 'cur' },
            { key: 'statements', container: 'stmtContainer', tfKey: 'stmt' },
            { key: 'exHandlers', container: 'exhContainer', tfKey: 'exh' },
            { key: 'views', container: 'vwContainer', tfKey: 'vw' },
            { key: 'triggers', container: 'tgContainer', tfKey: 'tg' },
            { key: 'globals', container: 'glContainer', tfKey: 'gl' },
            { key: 'jobs', container: 'jbContainer', tfKey: 'jb' }
        ];
        for (var i = 0; i < subs.length; i++) {
            var s = subs[i];
            if (PA._detailsSubLoaded[s.key]) {
                var cel = document.getElementById(s.container);
                if (cel) cel.innerHTML = '';
                if (s.tfKey && PA.tf && PA.tf._state) delete PA.tf._state[s.tfKey];
                PA._detailsSubLoaded[s.key] = false;
            }
        }
    },
    applyScope: function() {
        if (PA._detailsSubLoaded.sequences && PA.sequences && PA.sequences.applyScope) PA.sequences.applyScope();
        if (PA._detailsSubLoaded.joinQuery && PA.joinSummary && PA.joinSummary.applyScope) PA.joinSummary.applyScope();
        if (PA._detailsSubLoaded.dynamic && PA.dynamicSql && PA.dynamicSql.applyScope) PA.dynamicSql.applyScope();
        if (PA._detailsSubLoaded.transactions && PA.transactions && PA.transactions.applyScope) PA.transactions.applyScope();
        if (PA._detailsSubLoaded.cursors && PA.cursors && PA.cursors.applyScope) PA.cursors.applyScope();
        if (PA._detailsSubLoaded.statements && PA.statements && PA.statements.applyScope) PA.statements.applyScope();
        if (PA._detailsSubLoaded.exHandlers && PA.exHandlers && PA.exHandlers.applyScope) PA.exHandlers.applyScope();
    },

    /**
     * Case counts in the Details sub-tab labels — "Cursors (43)", "Dynamic SQL (32)", "Globals (3)".
     *
     * These are the counts the retired "All cases" accordion panel used to show, computed from the
     * current node doc (`PA.currentDetail`) by `PA.allCases.counts`. A count of 0 leaves the label
     * bare (applicable-only, exactly as the panel omitted empty groups). "Triggers fired" is not on
     * the node doc, so it is filled in asynchronously from the tables index.
     *
     * Called when the Details tab opens and whenever a new node detail is loaded, so the labels are
     * never stale — it is cheap and idempotent.
     */
    updateLabels: function(detail) {
        if (typeof document === 'undefined') return;
        detail = detail || PA.currentDetail;
        var counts = (PA.allCases && PA.allCases.counts) ? PA.allCases.counts(detail || {}) : null;
        if (!counts) return;
        // Sub-tab key -> the counts field that labels it.
        var map = { sequences: 'sequences', joinQuery: 'joins', dynamic: 'dynamic',
            transactions: 'transactions', cursors: 'cursors', statements: 'statements',
            exHandlers: 'exHandlers', views: 'views', globals: 'globals' };
        document.querySelectorAll('#viewDetails .dt-subtab').forEach(function(btn) {
            var key = btn.dataset.dtview;
            // Remember the authored label once; the suffix is re-derived on every call.
            if (btn.dataset.baseLabel == null) btn.dataset.baseLabel = btn.textContent.trim();
            var n = (key in map) ? counts[map[key]] : null;
            btn.textContent = (n != null && n > 0)
                ? btn.dataset.baseLabel + ' (' + n + ')' : btn.dataset.baseLabel;
        });
        // "Triggers fired" arrives from the tables index, not the node doc.
        if (detail && PA.allCases && PA.allCases.triggerCount) {
            PA.allCases.triggerCount(detail, function(n) {
                PA.detailsTab._setTabCount('triggers', n);
            });
        }
    },

    /** Sets one sub-tab label's count suffix (used by the async Triggers-fired count). */
    _setTabCount: function(key, n) {
        if (typeof document === 'undefined') return;
        var btn = document.querySelector('#viewDetails .dt-subtab[data-dtview="' + key + '"]');
        if (!btn) return;
        if (btn.dataset.baseLabel == null) btn.dataset.baseLabel = btn.textContent.trim();
        btn.textContent = (n > 0) ? btn.dataset.baseLabel + ' (' + n + ')' : btn.dataset.baseLabel;
    }
};

/**
 * Estate cross-link router — the estate facets (Views & MVs / Triggers / Globals / Scheduled jobs)
 * are no longer a top-level tab; they are the last four sub-tabs of the Details tab. This object is
 * kept as a thin shim so the estate screens' cross-links (PA.estateViews.showView, etc.) and any
 * external callers can still say "open the estate view for X" without knowing the host changed.
 */
PA.estateTab = {
    switchSub: function(sub) {
        if (PA.switchRightTab) PA.switchRightTab('details');
        if (PA.detailsTab && PA.detailsTab.switchSub) PA.detailsTab.switchSub(sub);
    }
};

PA.switchRightTab = function(view) {
    /* Legacy deep-link alias: the Estate top-level tab was removed; its sections are now Details
       sub-tabs. Keep '#estate' style callers working by redirecting to the Details tab. */
    if (view === 'estate') view = 'details';
    var prev = PA._currentRightTab;
    if (prev && prev !== view) {
        if (prev === 'details') {
            PA.detailsTab.teardown();
        } else if (prev === 'summary') {
            PA.summaryTab.teardown();
        // BUG-FIX (§259, real-audit §258 finding #3): Table Ops is excluded from this teardown.
        // Tearing down its container/state on every switch-away is exactly why revisiting it re-ran
        // the full duplicate-call sequence from scratch every time — `PA._rightTabLoaded.tableOps`
        // got reset to false here on every departure, so `switchRightTab`'s own "already loaded,
        // skip load()" guard (a few lines down) never had a chance to fire. PL/SQL's own Joins tab
        // (confirmed correctly caching in §258) never tears its DOM/state down on tab-away either —
        // this makes Table Ops match that same proven-correct shape. The other heavy tabs
        // (cursors/statements/exHandlers/complexity/callGraphViz) were not implicated by the audit
        // and keep their existing teardown-on-leave behavior unchanged.
        } else if (PA._heavyTabs[prev] && PA._rightTabLoaded[prev] && prev !== 'tableOps') {
            var cid = PA._heavyTabs[prev];
            var cel = document.getElementById(cid);
            if (cel) cel.innerHTML = '';
            var tfKey = PA._heavyTabTfKeys[prev];
            if (tfKey && PA.tf && PA.tf._state) delete PA.tf._state[tfKey];
            PA._rightTabLoaded[prev] = false;
        }
    }

    document.querySelectorAll('.rtab').forEach(function(t) { t.classList.remove('active'); });
    var btn = document.querySelector('.rtab[data-view="' + view + '"]');
    if (btn) btn.classList.add('active');
    document.querySelectorAll('.right-view').forEach(function(v) { v.classList.remove('active'); });
    var viewId = {
        explore: 'viewExplore', tableOps: 'viewTableOps',
        /* callTrace: 'viewCallTrace', trace: 'viewTrace', refs: 'viewRefs', */
        details: 'viewDetails',
        summary: 'viewSummary', complexity: 'viewComplexity', callGraphViz: 'viewCallGraphViz', source: 'viewSource'
    }[view];
    var el = document.getElementById(viewId);
    if (el) el.classList.add('active');

    if (view === 'tableOps') {
        if (!PA._rightTabLoaded.tableOps && PA.tableOps && PA.tableOps.load) {
            PA._rightTabLoaded.tableOps = true; PA.tableOps.load();
        }
    }
    if (view === 'details') {
        // Counts in the sub-tab labels are computed once, here, from the current node doc.
        PA.detailsTab.updateLabels(PA.currentDetail);
        PA.detailsTab._loadSub(PA._detailsCurrentSub);
    }
    if (view === 'summary') {
        if (!PA._rightTabLoaded.summary) {
            PA._rightTabLoaded.summary = true;
        }
        PA.summaryTab._loadSub(PA._summaryCurrentSub);
    }
    if (view === 'callGraphViz') {
        if (!PA._rightTabLoaded.callGraphViz && PA.callGraphViz && PA.callGraphViz.load) {
            PA._rightTabLoaded.callGraphViz = true; PA.callGraphViz.load();
        }
    }
    if (view === 'complexity') {
        if (!PA._rightTabLoaded.complexity && PA.complexity && PA.complexity.load) {
            PA._rightTabLoaded.complexity = true; PA.complexity.load();
        }
    }
    if (view === 'source' && PA.currentDetail && !PA._skipSourceAutoLoad) {
        var sf = PA.currentDetail.sourceFile;
        var startLine = PA.currentDetail.lineStart || 0;
        if (sf) PA.sourceView.open(sf, startLine);
    }
    if (view === 'explore') {
        PA.exploreTab._loadSub(PA._exploreCurrentSub);
    }
    /* if (view === 'refs' && PA.context.procId) {
        PA.refs._loaded = false;
        PA.refs.load(PA.context.procId);
    } */

    PA._skipSourceAutoLoad = false;
    PA._currentRightTab = view;
};

PA.trace = {
    flatNodes: [],
    _display: [],
    _rendered: 0,
    _batchSize: 200,
    _maxDepth: 1,
    _unique: false,
    _type: '',
    _q: '',
    /** Calls the server cut out of this tree (MID-006 budget) — 0 when the tree is complete. */
    _omitted: 0,
    renderFromTree: function(tree) {
        PA.trace.flatNodes = [];
        PA.trace._omitted = 0;
        PA.trace._flatten(tree, 0);
        PA.trace._rendered = 0;
        PA.trace._q = '';
        var searchBox = document.getElementById('traceSearch');
        if (searchBox) searchBox.value = '';
        PA.trace._render();
    },

    /**
     * Effective call type. This is the SAME `callType` the server put on the node and the same one
     * Hierarchy renders — Trace deliberately does not recompute it.
     *
     * <p>It used to reclassify every row against the ROOT's schema ("different schema from the root
     * => EXTERNAL"), which is a different question from the one the badge answers. A call is
     * internal/external relative to ITS OWN CALLER, not to the root of whatever tree you happen to
     * be looking at. Real Oracle data shows the divergence directly:
     * ANALYZER_REF$USP_REF_LOG -> ANALYZER_REF$REF_AUDIT is one schema calling itself — the server
     * and Hierarchy both say INTERNAL — but with root schema ANALYZER the old rule here relabelled
     * it EXTERNAL. Same edge, two views, two answers; that is the "Hierarchy and Trace disagree"
     * report. The server-side classifier already handles the case (non-Oracle dialects omitting
     * edge schemas) this reclassification was originally working around.
     */
    _effType: function(n) {
        if (n.circular) return 'CIRCULAR';
        return n.callType || '';
    },

    /** Collapse repeated occurrences of the same procedure into one row with a call count. */
    _uniqueNodes: function() {
        var seen = {}, out = [];
        PA.trace.flatNodes.forEach(function(n) {
            var key = ((n.schemaName || '') + '.' + (n.packageName || '') + '.' + (n.name || n.id || '')).toUpperCase();
            if (seen[key]) { seen[key]._count++; return; }
            var copy = Object.assign({}, n, { _count: 1, _depth: 0 });  // flat in unique mode
            seen[key] = copy; out.push(copy);
        });
        return out;
    },

    /** Rows to show = (unique or full) filtered by type + search text. */
    _displayNodes: function() {
        var base = PA.trace._unique ? PA.trace._uniqueNodes() : PA.trace.flatNodes;
        var q = PA.trace._q, type = PA.trace._type;
        if (!q && !type) return base;
        return base.filter(function(n) {
            if (type && PA.trace._effType(n) !== type) return false;
            if (q) {
                var name = ((n.schemaName || '') + '.' + (n.packageName || '') + '.' + (n.name || n.id || '')).toLowerCase();
                if (name.indexOf(q) < 0) return false;
            }
            return true;
        });
    },

    setType: function(v) { PA.trace._type = v || ''; PA.trace._render(); },
    toggleUnique: function() {
        PA.trace._unique = !PA.trace._unique;
        var b = document.getElementById('traceUniqueBtn');
        if (b) b.classList.toggle('active', PA.trace._unique);
        PA.trace._render();
    },
    _flatten: function(node, depth, parentSourceFile) {
        PA.trace.flatNodes.push(Object.assign({}, node, { _depth: depth, _parentSourceFile: parentSourceFile || '' }));
        // Hierarchy renders a "+N not shown" badge for these (callGraph.js). Trace showed nothing
        // at all, so a cut tree simply looked shorter than it is — the same silent-truncation
        // defect the server-side flag exists to prevent, just on the other view.
        if (node.childrenTruncated) PA.trace._omitted += (node.omittedChildCount || 0);
        var sf = node.sourceFile || '';
        if (node.children && !node.circular) {
            for (var i = 0; i < node.children.length; i++) PA.trace._flatten(node.children[i], depth + 1, sf);
        }
    },
    _renderRow: function(n, i, maxDepth) {
        var schema = n.schemaName || '';
        var pkg = n.packageName || '';
        var name = n.name || n.id || '?';
        var fullName = [schema, pkg, name].filter(Boolean).join('.');
        var colorObj = PA.getSchemaColor(schema);
        var barWidth = Math.max(20, ((n._depth + 1) / (maxDepth + 1)) * 200);
        // Server-supplied callType, verbatim — identical to what Hierarchy shows for this node.
        // See _effType for why the old root-schema reclassification was removed.
        var callType = n.callType || '';
        var startL = n.startLine || 0;
        var endL = n.endLine || 0;
        var callLine = n.callLineNumber || 0;
        var sf = n.sourceFile || '';

        // Each row is a 5-column CSS grid (see css/trace.css) so the sticky header built in
        // `_render` lines up with it and its grips can drive the same `--trc-*` width variables.
        // The cells keep every class the old flat span list used, so nothing that styled a badge
        // moved. Grouping by cell is what gives the proc chain and the @line refs their own,
        // separately resizable columns instead of competing for one flex line.
        var html = '<div class="trace-row" data-depth="' + n._depth + '">';
        // 1 — depth / step / depth bar
        html += '<span class="trace-cell tc-step">';
        html += '<span class="ct-depth-badge" data-tip="Call depth ' + n._depth + '">L' + n._depth + '</span>';
        html += '<span class="trace-step" style="color:' + colorObj.fg + '">' + (i + 1) + '</span>';
        html += '<span class="trace-bar" style="width:' + barWidth + 'px;background:' + colorObj.bg + ';margin-left:' + (n._depth * 8) + 'px"></span>';
        html += '</span>';
        // 2 — owner schema
        html += '<span class="trace-cell tc-schema">';
        if (schema) html += '<span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + '" data-tip="Owner schema">' + PA.esc(schema) + '</span>';
        html += '</span>';
        // 3 — procedure chain
        html += '<span class="trace-cell tc-name">';
        if (n.readable === false) html += '<span class="ct-lock" data-tip="Encrypted/wrapped">&#128274;</span>';
        html += '<span class="trace-name" data-tip="Click to view source" onclick="PA.sourceView.openModal(\'' + PA.escJs(n.id || '') + '\'); event.stopPropagation();">' + PA.esc(fullName) + '</span>';
        if (n._count > 1) html += '<span class="trace-count-badge" data-tip="Called ' + n._count + ' times in the trace">&times;' + n._count + '</span>';
        html += '</span>';
        // 4 — source body / call-site @line references
        html += '<span class="trace-cell tc-info"><span class="trace-info">';
        if (startL > 0) {
            var bodyLabel = (endL > 0 && endL !== startL) ? ('L' + startL + '-' + endL) : ('L' + startL);
            html += '<span class="ct-line" data-tip="Open source at line" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(sf) + '\', ' + startL + '); event.stopPropagation();">' + bodyLabel + '</span>';
        }
        if (callLine > 0 && n._parentSourceFile) {
            html += '<span class="ct-call-line" data-tip="Called at line ' + callLine + '" onclick="PA.sourceView.openAtLine(\'' + PA.escJs(n._parentSourceFile) + '\', ' + callLine + '); event.stopPropagation();">@' + callLine + '</span>';
        }
        html += '</span></span>';
        // 5 — call type / truncation
        html += '<span class="trace-cell tc-type">';
        // Same badge Hierarchy renders on the same node (callGraph.js) — the branch was cut here.
        if (n.childrenTruncated) {
            var _om = n.omittedChildCount || 0;
            html += '<span class="ct-call-badge TRUNCATED" data-tip="This branch was cut: ' + _om
                 + ' more call' + (_om === 1 ? '' : 's') + ' not expanded. The calls exist; they are not shown here.">+'
                 + _om + ' not shown</span>';
        }
        if (n.circular) html += '<span class="ct-call-badge CIRCULAR">CIRCULAR</span>';
        else if (callType) html += '<span class="ct-call-badge ' + PA.escAttr(callType) + '">' + PA.esc(callType) + '</span>';
        html += '</span>';
        html += '</div>';
        return html;
    },
    _render: function() {
        var container = document.getElementById('traceContainer');
        if (!PA.trace.flatNodes || PA.trace.flatNodes.length === 0) { container.innerHTML = '<div class="empty-msg">No trace data</div>'; return; }
        PA.trace._display = PA.trace._displayNodes();
        var nodes = PA.trace._display;
        PA.trace._maxDepth = 1;
        for (var k = 0; k < nodes.length; k++) { if (nodes[k]._depth > PA.trace._maxDepth) PA.trace._maxDepth = nodes[k]._depth; }
        var badge = document.getElementById('traceCount');
        if (badge) {
            // The root row is the procedure you selected, not a call it makes — counting it made
            // the badge read one higher than the number of calls actually listed.
            var callCount = 0;
            for (var c = 0; c < nodes.length; c++) { if ((nodes[c].callType || '') !== 'ROOT') callCount++; }
            badge.textContent = callCount + (PA.trace._unique ? ' unique' : '') + (callCount === 1 ? ' call' : ' calls')
                + (PA.trace._omitted > 0 ? ' (+' + PA.trace._omitted + ' not shown)' : '');
            badge.title = PA.trace._omitted > 0
                ? PA.trace._omitted + ' more calls exist but were not expanded — the server cuts a tree that '
                    + 'would be prohibitively large to expand in full. They exist; they are not listed here.'
                : '';
        }
        if (nodes.length === 0) { container.innerHTML = '<div class="empty-msg">No calls match the filter.</div>'; return; }
        // Sticky, resizable header + the rows inside a horizontal scroller. One column per cell
        // the row renderer emits; the grips are attached by PA.tblResize.enhanceGrid, which reads
        // the var/default/min off each header cell.
        container.innerHTML = '<div class="tbl-scroll"><div class="trace-grid" id="traceGrid">'
            + '<div class="trace-head" id="trace-head">'
            + '<div class="trace-hcell" data-col="0" data-var="--trc-step" data-default="150" data-min="80">Step</div>'
            + '<div class="trace-hcell" data-col="1" data-var="--trc-schema" data-default="130" data-min="70">Schema</div>'
            + '<div class="trace-hcell" data-col="2" data-var="--trc-name" data-default="340" data-min="140">Procedure</div>'
            + '<div class="trace-hcell" data-col="3" data-var="--trc-info" data-default="220" data-min="100">Source / Calls</div>'
            + '<div class="trace-hcell" data-col="4" data-var="--trc-type" data-default="120" data-min="80">Type</div>'
            + '</div><div id="traceRows"></div></div></div>';
        PA.trace._rendered = 0;
        PA.trace._renderBatch();
        if (PA.tblResize && PA.tblResize.enhanceGrid) {
            PA.tblResize.enhanceGrid(document.getElementById('trace-head'), 'trace');
        }
    },
    _renderBatch: function() {
        var rowsContainer = document.getElementById('traceRows');
        if (!rowsContainer) return;
        var nodes = PA.trace._display;
        var start = PA.trace._rendered;
        var end = Math.min(start + PA.trace._batchSize, nodes.length);
        var html = '';
        for (var i = start; i < end; i++) {
            html += PA.trace._renderRow(nodes[i], i, PA.trace._maxDepth);
        }
        var existing = rowsContainer.querySelector('.trace-show-more');
        if (existing) existing.remove();
        rowsContainer.insertAdjacentHTML('beforeend', html);
        PA.trace._rendered = end;

        if (end < nodes.length) {
            var remaining = nodes.length - end;
            rowsContainer.insertAdjacentHTML('beforeend',
                '<div class="trace-show-more" onclick="PA.trace._renderBatch()" style="padding:8px;text-align:center;font-size:11px;color:var(--accent);cursor:pointer;border-top:1px solid var(--border)">Show more (' + remaining + ' remaining of ' + nodes.length + ' total)</div>');
        }
    },
    _renderAll: function() {
        while (PA.trace._rendered < PA.trace._display.length) {
            PA.trace._renderBatch();
        }
    },
    search: function(query) {
        PA.trace._q = (query || '').trim().toLowerCase();
        PA.trace._render();   // re-render so search works together with Unique + type filter
    },
    expandAll: function() {
        if (PA.trace._rendered < PA.trace._display.length) PA.trace._renderAll();
        document.querySelectorAll('#traceContainer .trace-row').forEach(function(r) { r.style.display = ''; });
    },
    collapseAll: function() {
        document.querySelectorAll('#traceContainer .trace-row').forEach(function(r) {
            r.style.display = (parseInt(r.dataset.depth, 10) > 0) ? 'none' : '';
        });
    }
};


PA.globalSearch = function(query) {
    if (!query || query.length < 2) {
        document.querySelectorAll('.left-panel-list .lp-item').forEach(function(item) { item.style.display = ''; });
        return;
    }
    var q = SearchQuery.norm(query);
    document.querySelectorAll('.left-panel-list .lp-item').forEach(function(item) {
        item.style.display = (item.dataset.filter || '').toLowerCase().includes(q) ? '' : 'none';
    });
};

PA.pullData = async function() {
    if (!PA.analysisData || !PA.analysisData.name) { PA.toast('No analysis loaded', 'error'); return; }
    PA.toast('Pulling latest data...', 'success');
    try {
        PA.api.setAnalysis(PA.analysisData.name);
        var result = await PA.api.getIndex();
        if (!result) { PA.toast('Analysis not found', 'error'); return; }
        result.name = PA.analysisData.name;
        // UI-024 (§165): see PA.loadAnalysis's own comment — no longer eagerly fetched here.
        // A fresh `result` object means `ensureLoaded()`'s identity check sees a new
        // `PA.analysisData` and re-fetches on first use after this refresh, same as a normal load.
        PA.analysisData = result;
        PA._cachedTablesIndex = null;
        PA._tablesIndexPromise = null;
        PA.updateTopbar(result);
        await PA._renderLeftProcedures();
        PA._renderLeftTables();
        PA._syncTopbarTableCount();
        PA.toast('Data refreshed (' + (result.totalNodes || 0) + ' procs)', 'success');
    } catch(e) {
        PA.toast('Pull failed: ' + (e.message || e), 'error');
    }
};

// Clear the in-memory analysis cache (current analysis, or all). Then refresh the tab
// so the just-rebuilt data is shown.
PA.clearCache = async function () {
    try {
        var name = (PA.api && PA.api._analysisName) || null;
        var r = await PA.api.clearCache(name);
        PA.toast('Cache cleared (' + (r.scope || 'all') + ')', 'success');
        if (PA.refreshTab) PA.refreshTab();
    } catch (e) {
        PA.toast('Failed to clear cache', 'error');
    }
};

PA.refreshTab = function() {
    var tab = PA._currentRightTab;
    if (!tab) return;
    if (tab === 'details') {
        PA.detailsTab.teardown();
        PA.detailsTab._loadSub(PA._detailsCurrentSub);
        PA.toast('Refreshed: Details/' + PA._detailsCurrentSub, 'success');
        return;
    }
    if (tab === 'explore') {
        if (PA._exploreCurrentSub === 'references') {
            PA.refs._loaded = false;
            PA.refs.load(PA.context.procId);
        }
        PA.toast('Refreshed: Explore/' + PA._exploreCurrentSub, 'success');
        return;
    }
    PA._rightTabLoaded[tab] = false;
    var cid = PA._heavyTabs[tab];
    if (cid) {
        var cel = document.getElementById(cid);
        if (cel) cel.innerHTML = '';
    }
    var tfKey = PA._heavyTabTfKeys[tab];
    if (tfKey && PA.tf && PA.tf._state) delete PA.tf._state[tfKey];
    PA.switchRightTab(tab);
    PA.toast('Refreshed: ' + tab, 'success');
};

PA.onScopeChange(function() {
    PA._dhRenderStats();
    var tab = PA._currentRightTab;
    if (tab === 'tableOps' && PA._rightTabLoaded.tableOps && PA.tableOps && PA.tableOps.applyScope) PA.tableOps.applyScope();
    if (tab === 'details') PA.detailsTab.applyScope();
    else if (tab === 'summary' && PA._rightTabLoaded.summary && PA.summary && PA.summary.applyScope) PA.summary.applyScope();
    else if (tab === 'complexity' && PA._rightTabLoaded.complexity && PA.complexity && PA.complexity.applyScope) PA.complexity.applyScope();
    else if (tab === 'callGraphViz' && PA._rightTabLoaded.callGraphViz && PA.callGraphViz && PA.callGraphViz.applyScope) PA.callGraphViz.applyScope();
    if (tab === 'explore') PA.exploreTab.applyScope();
    /* if (tab === 'refs' && PA.refs._loaded) PA.refs._buildTable(); */
});

document.addEventListener('DOMContentLoaded', function() { PA.init(); });
