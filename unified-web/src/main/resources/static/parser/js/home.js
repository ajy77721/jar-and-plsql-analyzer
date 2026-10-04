PA.home = (function() {
    var _projects = [];
    var _envs = [];
    var _connections = []; // all connections for selected env

    var DB_META = {
        ORACLE:    { step2Label: 'Owner',      step2Hint: 'DB schema that owns the object',   objTypes: ['PACKAGE','PROCEDURE','FUNCTION','TRIGGER','VIEW'], autoOption: 'Auto-detect (all schemas)', targetPlaceholder: 'e.g. PKG_CUSTOMER or PKG.PROC_NAME' },
        MYSQL:     { step2Label: 'Connection', step2Hint: 'MySQL connection (database)',        objTypes: ['PROCEDURE','FUNCTION','TRIGGER','VIEW'],           autoOption: 'All connections',            targetPlaceholder: 'e.g. film_in_stock or get_total' },
        SQLSERVER: { step2Label: 'Connection', step2Hint: 'SQL Server connection (database)',   objTypes: ['PROCEDURE','FUNCTION','TRIGGER','VIEW'],           autoOption: 'All connections',            targetPlaceholder: 'e.g. usp_GetOrders or ufn_Total' },
        DB2:       { step2Label: 'Connection', step2Hint: 'IBM Db2 connection (database)',       objTypes: ['PROCEDURE','FUNCTION','TRIGGER','VIEW'],           autoOption: 'All connections',            targetPlaceholder: 'e.g. ERP_USP_RUN_CYCLE' },
        POSTGRES:  { step2Label: 'Connection', step2Hint: 'PostgreSQL connection (database)',     objTypes: ['PROCEDURE','FUNCTION','TRIGGER','VIEW'],           autoOption: 'All schemas',                targetPlaceholder: 'e.g. usp_run_cycle or erp.usp_run_cycle' }
    };

    async function init() {
        await loadProjects();
        loadClaudeStats();
        _updateStep2Labels();   // set the Target example for the initial dialect
        if (PA.loadHistory) PA.loadHistory();   // prefetch analysis history on the home/parse screen load
    }

    async function loadProjects() {
        try {
            var res = await fetch('/api/plsql/config/projects');
            _projects = res.ok ? await res.json() : [];
        } catch (e) { _projects = []; }

        var sel = document.getElementById('homeProject');
        if (!sel) return;
        _renderProjectSelect();

        var legacy = _projects.length === 0;
        var legacyLabel = document.getElementById('homeLegacyLabel');
        if (legacyLabel) legacyLabel.style.display = legacy ? 'inline' : 'none';

        _envs = []; _connections = [];
        _renderEnvSelect();
        _renderOwnerSelect();
    }

    /** Raw dbType selector value ('' = none selected → no dialect filter). */
    function _rawDbType() {
        var sel = document.getElementById('homeDbType');
        return sel ? (sel.value || '') : '';
    }

    /** True if a project has any connection of the given dbType (or no filter). */
    function _projectHasDb(proj, db) {
        if (!db) return true;
        var envs = (proj && proj.environments) || [];
        for (var i = 0; i < envs.length; i++) {
            if (_envHasDb(envs[i], db)) return true;
        }
        return false;
    }

    /** True if an environment has any connection of the given dbType (or no filter). */
    function _envHasDb(env, db) {
        if (!db) return true;
        var conns = (env && env.connections) || [];
        for (var j = 0; j < conns.length; j++) {
            if ((conns[j].dbType || 'ORACLE').toUpperCase() === db) return true;
        }
        return false;
    }

    /** Populate the project dropdown, filtered by the selected database type. */
    function _renderProjectSelect() {
        var sel = document.getElementById('homeProject');
        if (!sel) return;
        var db = _rawDbType();
        var prev = sel.value;
        var html = '<option value="">Select project</option>';
        var stillValid = false;
        for (var i = 0; i < _projects.length; i++) {
            var p = _projects[i];
            if (!_projectHasDb(p, db)) continue;
            html += '<option value="' + PA.esc(p.name) + '">' + PA.esc(p.name) + '</option>';
            if (p.name === prev) stillValid = true;
        }
        sel.innerHTML = html;
        sel.value = stillValid ? prev : '';
    }

    function onProjectChange() {
        var sel = document.getElementById('homeProject');
        var projName = sel ? sel.value : '';
        _envs = []; _connections = [];
        if (projName) {
            var proj = _projects.find(function(p) { return p.name === projName; });
            if (proj && proj.environments) _envs = proj.environments;
        }
        _renderEnvSelect();
        _renderOwnerSelect();
    }

    function _renderEnvSelect() {
        var sel = document.getElementById('homeEnv');
        if (!sel) return;
        var db = _rawDbType();
        var prev = sel.value;
        var html = '<option value="">Select environment</option>';
        var stillValid = false;
        for (var i = 0; i < _envs.length; i++) {
            var env = _envs[i];
            if (!_envHasDb(env, db)) continue;   // filter environments by selected database
            html += '<option value="' + PA.esc(env.name) + '">' + PA.esc(env.name);
            if (env.zone) html += ' (' + PA.esc(env.zone) + ')';
            html += '</option>';
            if (env.name === prev) stillValid = true;
        }
        sel.innerHTML = html;
        sel.value = stillValid ? prev : '';
    }

    function onEnvChange() {
        var projSel = document.getElementById('homeProject');
        var envSel  = document.getElementById('homeEnv');
        var projName = projSel ? projSel.value : '';
        var envName  = envSel  ? envSel.value  : '';
        _connections = [];
        if (projName && envName) {
            var proj = _projects.find(function(p) { return p.name === projName; });
            if (proj) {
                var env = (proj.environments || []).find(function(e) { return e.name === envName; });
                if (env && env.connections) _connections = env.connections;
            }
        }
        _renderOwnerSelect();
    }

    function onDbTypeChange() {
        // Cascade: filter projects by the chosen DB, then re-derive envs/connections.
        _renderProjectSelect();
        onProjectChange();   // rebuilds _envs from the (possibly reset) project and re-renders env
        onEnvChange();       // rebuilds _connections from the (preserved) env and re-renders owner
        _renderObjTypeSelect();
        _updateStep2Labels();
    }

    function _currentDbType() {
        var sel = document.getElementById('homeDbType');
        return sel ? (sel.value || 'ORACLE') : 'ORACLE';
    }

    function _renderOwnerSelect() {
        var sel = document.getElementById('homeOwner');
        if (!sel) return;
        var dbType = _currentDbType();
        var meta   = DB_META[dbType] || DB_META.ORACLE;

        var html = '<option value="">' + PA.esc(meta.autoOption) + '</option>';
        var seen = {};

        var connsForType = _connections.filter(function(c) {
            return (c.dbType || 'ORACLE').toUpperCase() === dbType;
        });

        if (dbType === 'ORACLE') {
            // Oracle: show unique schema owners. Prefer the connection's declared/discovered
            // `schemas` list (may include schemas beyond the login username, e.g. cross-schema
            // grants); fall back to the username alone when no schemas were discovered/saved.
            //
            // The option LABEL is the bare schema name and nothing else: it is the only thing the
            // user is picking (the value is the schema name too), it keeps long lists scannable,
            // and it makes the browser's type-to-find work — a shared prefix on every option
            // (e.g. "Schema: ") breaks keyboard search outright. The connection / login detail
            // still matters for context, so it moves to the option tooltip, plus an <optgroup>
            // header per connection when more than one connection contributes schemas.
            var multiConn = connsForType.length > 1;
            connsForType.forEach(function(c) {
                var schemas = (Array.isArray(c.schemas) && c.schemas.length) ? c.schemas : [c.username];
                var group = '';
                schemas.forEach(function(s) {
                    var o = (s || '').toUpperCase();
                    if (!o || seen[o]) return;
                    seen[o] = true;
                    var tip = 'Schema ' + o + ' — connection ' + (c.name || '?')
                            + ' (connected as ' + ((c.username || '').toUpperCase() || '?') + ')';
                    group += '<option value="' + PA.escAttr(o) + '" title="' + PA.escAttr(tip) + '">'
                           + PA.esc(o) + '</option>';
                });
                if (!group) return;
                html += multiConn
                    ? '<optgroup label="' + PA.escAttr(c.name || 'connection') + '">' + group + '</optgroup>'
                    : group;
            });
        } else {
            // MySQL / SQL Server / Db2 / Postgres: the picker selects a CONNECTION (value =
            // connection name, resolved to serviceName on submit), so the connection name is
            // the correct short primary label. The database and any known schemas are detail,
            // so they go in the tooltip rather than being concatenated into the label.
            connsForType.forEach(function(c) {
                var key = c.name;
                if (!key || seen[key]) return;
                seen[key] = true;
                var tipParts = [];
                if (c.serviceName) tipParts.push('database ' + c.serviceName);
                if (Array.isArray(c.schemas) && c.schemas.length) tipParts.push('schemas: ' + c.schemas.join(', '));
                var tip = c.name + (tipParts.length ? ' — ' + tipParts.join(' — ') : '');
                html += '<option value="' + PA.escAttr(c.name) + '" title="' + PA.escAttr(tip) + '">'
                      + PA.esc(c.name) + '</option>';
            });
        }

        sel.innerHTML = html;
    }

    function _renderObjTypeSelect() {
        var sel = document.getElementById('homeObjType');
        if (!sel) return;
        var dbType = _currentDbType();
        var meta   = DB_META[dbType] || DB_META.ORACLE;
        var current = sel.value;
        var html = '';
        meta.objTypes.forEach(function(t) {
            html += '<option value="' + t + '"' + (t === current ? ' selected' : '') + '>' + t + '</option>';
        });
        sel.innerHTML = html;
        // If previous selection no longer valid, first option wins
        if (!meta.objTypes.includes(sel.value)) sel.value = meta.objTypes[0];
    }

    function _updateStep2Labels() {
        var dbType = _currentDbType();
        var meta   = DB_META[dbType] || DB_META.ORACLE;
        var labelEl = document.getElementById('homeStep2Label');
        var hintEl  = document.getElementById('homeStep2Hint');
        if (labelEl) labelEl.textContent = meta.step2Label;
        if (hintEl)  hintEl.textContent  = meta.step2Hint;
        // Update the Target placeholder to the dialect-specific example pattern.
        // When no database is chosen yet, prompt the user to pick one first.
        var objInput = document.getElementById('homeObject');
        if (objInput) {
            objInput.placeholder = _rawDbType()
                ? (meta.targetPlaceholder || 'e.g. PROC_NAME')
                : 'Select a database first…';
        }
    }

    function loadClaudeStats() {
        fetch('/api/parser/claude/stats')
            .then(function(r) { return r.ok ? r.json() : null; })
            .then(function(data) {
                if (!data) return;
                var calls = data.totalCalls || 0;
                if (calls === 0) return;
                var sessions = (data.sessions && data.sessions.total) || 0;
                var bytes = data.totalPromptBytes || 0;
                var el = document.getElementById('claudeApiStats');
                if (!el) return;
                var callsEl    = document.getElementById('claudeApiCallsBadge');
                var sessionsEl = document.getElementById('claudeApiSessionsBadge');
                var promptEl   = document.getElementById('claudeApiPromptBadge');
                var respEl     = document.getElementById('claudeApiResponseBadge');
                var durEl      = document.getElementById('claudeApiDurationBadge');
                var errEl      = document.getElementById('claudeApiErrorsBadge');
                if (callsEl)    callsEl.textContent    = calls + ' call' + (calls !== 1 ? 's' : '');
                if (sessionsEl) sessionsEl.textContent = sessions + ' session' + (sessions !== 1 ? 's' : '');
                if (promptEl)   promptEl.textContent   = _fmtBytes(bytes) + ' prompt';
                if (respEl)     respEl.textContent     = _fmtBytes(data.totalResponseBytes || 0) + ' response';
                if (durEl)      durEl.textContent      = _fmtDuration(data.totalDurationMs || 0) + ' total';
                var errs = data.errors || 0;
                if (errEl) {
                    errEl.style.display = errs > 0 ? '' : 'none';
                    errEl.style.color = 'var(--red)';
                    errEl.textContent = errs + ' error' + (errs !== 1 ? 's' : '');
                }
            })
            .catch(function() {});
    }

    function _fmtDuration(ms) {
        if (!ms || ms < 1000) return (ms || 0) + ' ms';
        var s = ms / 1000;
        if (s < 60) return s.toFixed(1) + 's';
        var m = Math.floor(s / 60);
        return m + 'm ' + Math.round(s % 60) + 's';
    }

    function _fmtBytes(n) {
        if (n < 1024) return n + ' B';
        if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
        return (n / 1048576).toFixed(1) + ' MB';
    }

    function getFormData() {
        var dbType = _currentDbType();

        // Owner/schema and object-type selects are hidden — schema, and whether the entered name
        // is a package / procedure / function / pkg.proc / pkg.func, are now detected entirely
        // server-side (ObjectResolutionService.findObjectSmart), never guessed or defaulted here.
        // The homeOwner select still exists (hidden) purely because for non-Oracle dialects it
        // doubles as the connection picker, so its value still resolves connName below.
        var ownerSel = document.getElementById('homeOwner');
        var ownerVal = ownerSel ? ownerSel.value : '';
        var connName = '';
        if (dbType !== 'ORACLE' && ownerVal) {
            var conn = _connections.find(function(c) { return c.name === ownerVal; });
            connName = conn ? conn.name : ownerVal;
        }

        return {
            project:     (document.getElementById('homeProject')  || {}).value || '',
            environment: (document.getElementById('homeEnv')       || {}).value || '',
            dbType:      dbType,
            owner:       '',
            connName:    connName,
            objectType:  '',
            fastReRun:   !!((document.getElementById('homeFastReRun') || {}).checked),
            entryPoint:  (document.getElementById('homeObject')   || {}).value || ''
        };
    }

    // ---- Input help: what to type for each database dialect ----
    var HELP = {
        ORACLE: {
            title: 'Oracle',
            owner: 'Schema/owner that owns the object (e.g. HR, SCOTT). Leave as Auto-detect to search all schemas.',
            target: 'Package, procedure or function name — e.g. <code>PKG_CUSTOMER</code> or <code>PROC_UPDATE_ORDER</code>. Case-insensitive.',
            type: 'Pick PACKAGE to analyze a whole package body, or PROCEDURE / FUNCTION for a standalone unit.'
        },
        MYSQL: {
            title: 'MySQL',
            owner: 'The database (schema) name — e.g. <code>sakila</code>. MySQL has no package concept.',
            target: 'A stored PROCEDURE or FUNCTION name — e.g. <code>film_in_stock</code>.',
            type: 'Choose PROCEDURE or FUNCTION. PACKAGE does not apply to MySQL.'
        },
        SQLSERVER: {
            title: 'SQL Server',
            owner: 'The schema, usually <code>dbo</code> (or a custom schema). Not the database name.',
            target: 'A procedure or function name — e.g. <code>usp_GetOrders</code> or <code>ufn_Total</code>. Bracketed names work too.',
            type: 'Choose PROCEDURE or FUNCTION. PACKAGE does not apply to SQL Server.'
        },
        DB2: {
            title: 'IBM Db2',
            owner: 'The schema (e.g. <code>DB2INST1</code>). Leave as Auto-detect to search all schemas.',
            target: 'A procedure or function name — e.g. <code>ERP_USP_RUN_CYCLE</code>.',
            type: 'Choose PROCEDURE or FUNCTION. PACKAGE does not apply to Db2.'
        },
        // PostgreSQL was missing from both HELP and the render order below, so the one dialect a
        // user is most likely to need guidance on had no card at all.
        POSTGRES: {
            title: 'PostgreSQL',
            owner: 'The schema — usually <code>public</code>, or a named schema such as <code>erp</code>. '
                 + 'Not the database name.',
            target: 'A PL/pgSQL function or procedure name — e.g. <code>erp_usp_run_cycle</code>. '
                  + 'Names are lower-case unless they were created quoted.',
            type: 'Choose PROCEDURE or FUNCTION. PostgreSQL has no packages; a schema groups routines instead.'
        }
    };

    function showHelp() {
        closeHelp();
        var sel = (document.getElementById('homeDbType') || {}).value || '';
        // Derived from HELP, so a dialect added there gets a card automatically. As a literal this
        // list silently omitted PostgreSQL — and had it ever fallen out of step the other way,
        // `HELP[k]` would have been undefined and `h.title` would have thrown, taking the whole
        // help overlay with it.
        var order = Object.keys(HELP);
        var esc = (PA && PA.esc) ? PA.esc : function(s) { return s; };

        var overlay = document.createElement('div');
        overlay.className = 'tds-overlay';
        overlay.id = 'homeHelpOverlay';
        overlay.onclick = function(e) { if (e.target === overlay) closeHelp(); };

        var modal = document.createElement('div');
        modal.className = 'tds-modal';
        modal.style.maxWidth = '640px';
        modal.onclick = function(e) { e.stopPropagation(); };

        var html = '<div class="tds-header"><div class="tds-header-left">'
            + '<span style="font-weight:700;font-size:13px">&#63; How to fill in the analysis form</span>'
            + '</div><button class="btn btn-sm tds-close" onclick="PA.home.closeHelp()">&times;</button></div>';
        html += '<div class="tds-body" style="max-height:70vh;overflow:auto">';
        html += '<div style="font-size:11px;color:var(--text-muted);margin-bottom:10px">'
            + 'Pick a <b>Database</b>, then a <b>Project</b> / <b>Environment</b> (connection), set the <b>Owner</b> '
            + '(schema) and the <b>Target</b> object, choose its type and click <b>Analyze</b>.'
            + (sel ? ' Guidance for your selected database is highlighted below.' : '') + '</div>';

        for (var i = 0; i < order.length; i++) {
            var k = order[i];
            var h = HELP[k];
            if (!h) continue;   // never let a missing entry throw and blank the whole overlay
            var active = (k === sel);
            html += '<div class="home-help-card" style="border:1px solid ' + (active ? 'var(--accent)' : 'var(--border)')
                + ';border-radius:var(--radius);padding:10px 12px;margin-bottom:8px;'
                + (active ? 'background:color-mix(in srgb, var(--accent) 6%, transparent)' : '') + '">';
            html += '<div style="font-weight:700;font-size:12px;margin-bottom:6px">' + esc(h.title)
                + (active ? ' <span class="badge" style="font-size:9px">selected</span>' : '') + '</div>';
            html += '<div style="font-size:11px;line-height:1.6">';
            html += '<div><b>Owner:</b> ' + h.owner + '</div>';
            html += '<div><b>Target:</b> ' + h.target + '</div>';
            html += '<div><b>Type:</b> ' + h.type + '</div>';
            html += '</div></div>';
        }
        html += '</div>';

        modal.innerHTML = html;
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        if (PA.modalStack) overlay.style.zIndex = PA.modalStack.nextZ();
        requestAnimationFrame(function() { overlay.classList.add('open'); });
        document.addEventListener('keydown', helpEsc);
    }

    function helpEsc(e) { if (e.key === 'Escape') closeHelp(); }

    function closeHelp() {
        var el = document.getElementById('homeHelpOverlay');
        if (el) { el.classList.remove('open'); setTimeout(function() { if (el.parentNode) el.remove(); }, 150); }
        document.removeEventListener('keydown', helpEsc);
    }

    return {
        init: init,
        loadProjects: loadProjects,
        onProjectChange: onProjectChange,
        onEnvChange: onEnvChange,
        onDbTypeChange: onDbTypeChange,
        getFormData: getFormData,
        loadClaudeStats: loadClaudeStats,
        showHelp: showHelp,
        closeHelp: closeHelp
    };
})();
