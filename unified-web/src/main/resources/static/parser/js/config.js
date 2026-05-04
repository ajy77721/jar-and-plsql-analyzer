window.PA = window.PA || {};

PA.config = {
    _projects: [],
    _selectedProject: null,
    _selectedEnv: null,
    _selectedConn: null,
    _testPassed: false,

    API: '/api/plsql/config',

    showModal: function() {
        document.getElementById('cfgModal').style.display = 'flex';
        this.loadProjects();
    },

    closeModal: function(e) {
        if (e && e.target !== e.currentTarget) return;
        document.getElementById('cfgModal').style.display = 'none';
    },

    async loadProjects() {
        try {
            var res = await fetch(this.API + '/projects');
            this._projects = res.ok ? await res.json() : [];
        } catch (e) { this._projects = []; }
        this._renderProjects();
    },

    _renderProjects: function() {
        var el = document.getElementById('cfgProjectList');
        var self = this;
        if (!this._projects.length) {
            el.innerHTML = '<div class="cfg-empty">No projects.<br>Click "+ Add Project".</div>';
            document.getElementById('cfgEnvPanel').innerHTML = '';
            document.getElementById('cfgDetailPanel').innerHTML = '';
            return;
        }
        var html = '';
        for (var i = 0; i < this._projects.length; i++) {
            var p = this._projects[i];
            var active = this._selectedProject === p.name ? ' active' : '';
            html += '<div class="cfg-item' + active + '" onclick="PA.config.selectProject(\'' + PA.escJs(p.name) + '\')">';
            html += '<div class="cfg-item-name">' + PA.esc(p.name) + '</div>';
            html += '<div class="cfg-item-desc">' + PA.esc(p.description || '') + '</div>';
            html += '<div class="cfg-item-meta">' + ((p.environments || []).length) + ' env(s)</div>';
            html += '<button class="cfg-del" onclick="event.stopPropagation();PA.config.deleteProject(\'' + PA.escJs(p.name) + '\')" title="Delete">&#10005;</button>';
            html += '</div>';
        }
        el.innerHTML = html;
    },

    async selectProject(name) {
        this._selectedProject = name;
        this._selectedEnv = null;
        this._selectedConn = null;
        this._renderProjects();
        try {
            var res = await fetch(this.API + '/projects/' + encodeURIComponent(name) + '/environments');
            var envs = res.ok ? await res.json() : [];
            var proj = this._projects.find(function(p) { return p.name === name; });
            if (proj) proj.environments = envs;
            this._renderEnvs(envs);
        } catch (e) { this._renderEnvs([]); }
        document.getElementById('cfgDetailPanel').innerHTML = '<div class="cfg-empty">Select an environment</div>';
    },

    _renderEnvs: function(envs) {
        var el = document.getElementById('cfgEnvPanel');
        var html = '<div class="cfg-section-head"><span>Environments</span>';
        html += '<button class="btn btn-sm btn-primary" onclick="PA.config.showAddEnv()">+ Env</button></div>';
        if (!envs || !envs.length) {
            html += '<div class="cfg-empty">No environments.</div>';
        } else {
            for (var i = 0; i < envs.length; i++) {
                var e = envs[i];
                var connCount = (e.connections || []).length;
                var active = this._selectedEnv === e.name ? ' active' : '';
                html += '<div class="cfg-item' + active + '" onclick="PA.config.selectEnv(\'' + PA.escJs(e.name) + '\')">';
                html += '<div class="cfg-item-name">' + PA.esc(e.name);
                if (e.zone) html += ' <span class="cfg-zone">' + PA.esc(e.zone) + '</span>';
                html += '</div>';
                html += '<div class="cfg-item-meta">' + connCount + ' connection(s)</div>';
                html += '<button class="cfg-del" onclick="event.stopPropagation();PA.config.deleteEnv(\'' + PA.escJs(e.name) + '\')" title="Delete">&#10005;</button>';
                html += '</div>';
            }
        }
        el.innerHTML = html;
    },

    async selectEnv(name) {
        this._selectedEnv = name;
        this._selectedConn = null;
        var proj = this._projects.find(function(p) { return p.name === PA.config._selectedProject; });
        if (proj) this._renderEnvs(proj.environments);
        var env = proj && proj.environments ? proj.environments.find(function(e) { return e.name === name; }) : null;
        if (env) this._renderConnections(env);
    },

    _renderConnections: function(env) {
        var el = document.getElementById('cfgDetailPanel');
        var html = '<div class="cfg-section-head"><span>' + PA.esc(env.name) + ' — Connections</span>';
        html += '<button class="btn btn-sm btn-primary" onclick="PA.config.showConnForm(null)">+ New Connection</button></div>';
        if (!env.connections || !env.connections.length) {
            html += '<div class="cfg-empty">No connections.<br>Click "+ New Connection" to add one.</div>';
        } else {
            html += '<div class="cfg-conn-list">';
            for (var i = 0; i < env.connections.length; i++) {
                var c = env.connections[i];
                var url = c.jdbcUrl || (c.hostname ? c.hostname + ':' + (c.port || 1521) + '/' + (c.serviceName || c.sid || '') : 'N/A');
                html += '<div class="cfg-conn-item" onclick="PA.config.showConnForm(\'' + PA.escJs(c.name) + '\')">';
                html += '<div style="display:flex;align-items:center;gap:8px">';
                html += '<span class="cfg-conn-status ' + (c.tested ? 'ok' : 'untested') + '">' + (c.tested ? '&#10003;' : '?') + '</span>';
                html += '<div>';
                html += '<div class="cfg-item-name">' + PA.esc(c.name) + '</div>';
                html += '<div class="cfg-item-desc">' + PA.esc(c.username || '') + '@' + PA.esc(url) + '</div>';
                html += '</div></div>';
                html += '<div style="display:flex;gap:4px;align-items:center">';
                html += '<span class="cfg-conn-type-badge">' + PA.esc(c.connectionType || 'SERVICE_NAME') + '</span>';
                if (c.role && c.role !== 'DEFAULT') html += '<span class="cfg-conn-role-badge">' + PA.esc(c.role) + '</span>';
                html += '<button class="cfg-del" style="position:static;opacity:1" onclick="event.stopPropagation();PA.config.deleteConn(\'' + PA.escJs(c.name) + '\')" title="Delete">&#10005;</button>';
                html += '</div></div>';
            }
            html += '</div>';
        }
        el.innerHTML = html;
    },

    showConnForm: function(connName) {
        this._selectedConn = connName;
        this._testPassed = false;
        var proj = this._projects.find(function(p) { return p.name === PA.config._selectedProject; });
        var env = proj && proj.environments ? proj.environments.find(function(e) { return e.name === PA.config._selectedEnv; }) : null;
        var existing = connName && env && env.connections ? env.connections.find(function(c) { return c.name === connName; }) : null;
        var c = existing || { connectionType: 'SERVICE_NAME', role: 'DEFAULT', port: 1521 };
        var isEdit = !!existing;
        var el = document.getElementById('cfgDetailPanel');

        var html = '<div class="cfg-section-head"><span>' + (isEdit ? 'Edit' : 'New') + ' Connection</span>';
        html += '<button class="btn btn-sm" onclick="PA.config.selectEnv(\'' + PA.escJs(this._selectedEnv) + '\')">&larr; Back</button></div>';
        html += '<div class="cfg-form" style="max-height:calc(80vh - 120px);overflow-y:auto">';

        html += '<div class="cfg-row"><label>Connection Name</label>';
        html += '<input id="cfConn" value="' + PA.esc(c.name || '') + '" class="form-input cfg-inp" placeholder="e.g. OPUS_CORE_PROD"' + (isEdit ? ' readonly style="opacity:0.6"' : '') + '></div>';
        html += '<div class="cfg-row"><label>Description</label>';
        html += '<input id="cfDesc" value="' + PA.esc(c.description || '') + '" class="form-input cfg-inp" placeholder="Optional description"></div>';

        html += '<div style="margin:12px 0 6px;font-size:12px;font-weight:700;color:var(--text)">Authentication</div>';
        html += '<div class="cfg-row"><label>Username</label>';
        html += '<input id="cfUser" value="' + PA.esc(c.username || '') + '" class="form-input cfg-inp"></div>';
        html += '<div class="cfg-row"><label>Password</label>';
        html += '<input id="cfPass" type="password" value="' + PA.esc(c.password || '') + '" class="form-input cfg-inp"></div>';
        html += '<div class="cfg-row"><label>Role</label>';
        html += '<select id="cfRole" class="form-input cfg-inp">';
        html += '<option value="DEFAULT"' + (c.role === 'DEFAULT' ? ' selected' : '') + '>Default</option>';
        html += '<option value="SYSDBA"' + (c.role === 'SYSDBA' ? ' selected' : '') + '>SYSDBA</option>';
        html += '<option value="SYSOPER"' + (c.role === 'SYSOPER' ? ' selected' : '') + '>SYSOPER</option>';
        html += '</select></div>';

        html += '<div style="margin:12px 0 6px;font-size:12px;font-weight:700;color:var(--text)">Connection Details</div>';
        html += '<div class="cfg-row"><label>Connection Type</label>';
        html += '<select id="cfType" class="form-input cfg-inp" onchange="PA.config._onTypeChange()">';
        html += '<option value="SERVICE_NAME"' + (c.connectionType === 'SERVICE_NAME' ? ' selected' : '') + '>Service Name</option>';
        html += '<option value="SID"' + (c.connectionType === 'SID' ? ' selected' : '') + '>SID</option>';
        html += '<option value="TNS"' + (c.connectionType === 'TNS' ? ' selected' : '') + '>TNS</option>';
        html += '<option value="CUSTOM"' + (c.connectionType === 'CUSTOM' ? ' selected' : '') + '>Custom JDBC URL</option>';
        html += '</select></div>';

        html += '<div id="cfFieldsServiceName" class="cfg-type-fields">';
        html += '<div class="cfg-row"><label>Hostname</label><input id="cfHost" value="' + PA.esc(c.hostname || '') + '" class="form-input cfg-inp" placeholder="e.g. dbhost.company.com"></div>';
        html += '<div class="cfg-row"><label>Port</label><input id="cfPort" type="number" value="' + (c.port || 1521) + '" class="form-input cfg-inp" style="width:100px"></div>';
        html += '<div class="cfg-row"><label>Service Name</label><input id="cfSvc" value="' + PA.esc(c.serviceName || '') + '" class="form-input cfg-inp" placeholder="e.g. ORCL"></div>';
        html += '</div>';

        html += '<div id="cfFieldsSid" class="cfg-type-fields" style="display:none">';
        html += '<div class="cfg-row"><label>Hostname</label><input id="cfHostSid" value="' + PA.esc(c.hostname || '') + '" class="form-input cfg-inp"></div>';
        html += '<div class="cfg-row"><label>Port</label><input id="cfPortSid" type="number" value="' + (c.port || 1521) + '" class="form-input cfg-inp" style="width:100px"></div>';
        html += '<div class="cfg-row"><label>SID</label><input id="cfSid" value="' + PA.esc(c.sid || '') + '" class="form-input cfg-inp" placeholder="e.g. ORCL"></div>';
        html += '</div>';

        html += '<div id="cfFieldsTns" class="cfg-type-fields" style="display:none">';
        html += '<div class="cfg-row"><label>TNS Alias</label><input id="cfTns" value="' + PA.esc(c.tnsAlias || '') + '" class="form-input cfg-inp" placeholder="e.g. MYDB_PROD"></div>';
        html += '</div>';

        html += '<div id="cfFieldsCustom" class="cfg-type-fields" style="display:none">';
        html += '<div class="cfg-row"><label>JDBC URL</label><input id="cfCustomUrl" value="' + PA.esc(c.customUrl || '') + '" class="form-input cfg-inp" placeholder="jdbc:oracle:thin:@..."></div>';
        html += '</div>';

        html += '<div style="margin-top:16px;display:flex;gap:8px;align-items:center">';
        html += '<button class="btn btn-sm" id="cfTestBtn" onclick="PA.config._testConn()" style="color:#22c55e">&#9889; Test Connection</button>';
        html += '<button class="btn btn-primary btn-sm" id="cfSaveBtn" onclick="PA.config._saveConn(' + isEdit + ')" disabled>Save</button>';
        html += '<span id="cfTestStatus" style="font-size:11px;color:var(--text-muted)">Test required before save</span>';
        html += '</div>';
        html += '<div id="cfTestResult" style="margin-top:8px"></div>';
        html += '</div>';

        el.innerHTML = html;
        this._onTypeChange();
    },

    _onTypeChange: function() {
        var type = (document.getElementById('cfType') || {}).value || 'SERVICE_NAME';
        var panels = ['ServiceName', 'Sid', 'Tns', 'Custom'];
        for (var i = 0; i < panels.length; i++) {
            var p = document.getElementById('cfFields' + panels[i]);
            if (p) p.style.display = 'none';
        }
        var map = { SERVICE_NAME: 'ServiceName', SID: 'Sid', TNS: 'Tns', CUSTOM: 'Custom' };
        var target = document.getElementById('cfFields' + (map[type] || 'ServiceName'));
        if (target) target.style.display = 'block';
        this._testPassed = false;
        var saveBtn = document.getElementById('cfSaveBtn');
        if (saveBtn) saveBtn.disabled = true;
        var status = document.getElementById('cfTestStatus');
        if (status) { status.textContent = 'Test required before save'; status.style.color = 'var(--text-muted)'; }
    },

    _buildConnFromForm: function() {
        var type = (document.getElementById('cfType') || {}).value || 'SERVICE_NAME';
        var conn = {
            name: ((document.getElementById('cfConn') || {}).value || '').trim(),
            description: ((document.getElementById('cfDesc') || {}).value || '').trim(),
            username: ((document.getElementById('cfUser') || {}).value || '').trim(),
            password: (document.getElementById('cfPass') || {}).value || '',
            role: (document.getElementById('cfRole') || {}).value || 'DEFAULT',
            connectionType: type
        };
        if (type === 'SERVICE_NAME') {
            conn.hostname = ((document.getElementById('cfHost') || {}).value || '').trim();
            conn.port = parseInt((document.getElementById('cfPort') || {}).value) || 1521;
            conn.serviceName = ((document.getElementById('cfSvc') || {}).value || '').trim();
            if (conn.hostname && conn.serviceName) {
                conn.jdbcUrl = 'jdbc:oracle:thin:@//' + conn.hostname + ':' + conn.port + '/' + conn.serviceName;
            }
        } else if (type === 'SID') {
            conn.hostname = ((document.getElementById('cfHostSid') || {}).value || '').trim();
            conn.port = parseInt((document.getElementById('cfPortSid') || {}).value) || 1521;
            conn.sid = ((document.getElementById('cfSid') || {}).value || '').trim();
            if (conn.hostname && conn.sid) {
                conn.jdbcUrl = 'jdbc:oracle:thin:@' + conn.hostname + ':' + conn.port + ':' + conn.sid;
            }
        } else if (type === 'TNS') {
            conn.tnsAlias = ((document.getElementById('cfTns') || {}).value || '').trim();
            conn.jdbcUrl = 'jdbc:oracle:thin:@' + conn.tnsAlias;
        } else if (type === 'CUSTOM') {
            conn.customUrl = ((document.getElementById('cfCustomUrl') || {}).value || '').trim();
            conn.jdbcUrl = conn.customUrl;
        }
        return conn;
    },

    async _testConn() {
        var conn = this._buildConnFromForm();
        var btn = document.getElementById('cfTestBtn');
        var result = document.getElementById('cfTestResult');
        var status = document.getElementById('cfTestStatus');
        var saveBtn = document.getElementById('cfSaveBtn');
        btn.textContent = 'Testing...'; btn.disabled = true;

        try {
            var res = await fetch(this.API + '/test-connection', {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(conn)
            });
            var data = await res.json();
            if (data.success) {
                this._testPassed = true;
                saveBtn.disabled = false;
                result.innerHTML = '<div class="cfg-test-ok">&#10003; ' + PA.esc(data.message) + ' — ' + PA.esc(data.version || '') + '</div>';
                status.textContent = 'Test passed — ready to save'; status.style.color = '#22c55e';
            } else {
                this._testPassed = false; saveBtn.disabled = true;
                result.innerHTML = '<div class="cfg-test-fail">&#10007; ' + PA.esc(data.message || data.error) + '</div>';
                status.textContent = 'Test failed — fix and retry'; status.style.color = '#ef4444';
            }
        } catch (e) {
            this._testPassed = false; saveBtn.disabled = true;
            result.innerHTML = '<div class="cfg-test-fail">&#10007; ' + PA.esc(e.message) + '</div>';
            status.textContent = 'Test error'; status.style.color = '#ef4444';
        }
        btn.innerHTML = '&#9889; Test Connection'; btn.disabled = false;
    },

    async _saveConn(isEdit) {
        if (!this._testPassed) { alert('Test must pass before saving.'); return; }
        var conn = this._buildConnFromForm();
        conn.tested = true;
        if (!conn.name) { PA.toast('Connection name is required', 'error'); return; }
        if (!conn.username) { PA.toast('Username is required', 'error'); return; }

        var base = this.API + '/projects/' + encodeURIComponent(this._selectedProject) +
                   '/environments/' + encodeURIComponent(this._selectedEnv) + '/connections';
        try {
            var method = isEdit ? 'PUT' : 'POST';
            var url = isEdit ? base + '/' + encodeURIComponent(this._selectedConn) : base;
            var res = await fetch(url, { method: method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(conn) });
            var data = await res.json();
            if (data.error) { PA.toast(data.error, 'error'); return; }
            PA.toast('Connection saved', 'success');
            await this.selectProject(this._selectedProject);
            this.selectEnv(this._selectedEnv);
        } catch (e) { PA.toast('Save failed: ' + e.message, 'error'); }
    },

    async deleteConn(connName) {
        if (!confirm('Delete connection "' + connName + '"?')) return;
        var url = this.API + '/projects/' + encodeURIComponent(this._selectedProject) +
                  '/environments/' + encodeURIComponent(this._selectedEnv) + '/connections/' + encodeURIComponent(connName);
        try { await fetch(url, { method: 'DELETE' }); PA.toast('Deleted', 'success'); await this.selectProject(this._selectedProject); this.selectEnv(this._selectedEnv); }
        catch (e) { PA.toast('Delete failed', 'error'); }
    },

    showAddProject: function() {
        var name = prompt('Project name:');
        if (!name) return;
        var desc = prompt('Description (optional):') || '';
        this._doAddProject(name, desc);
    },

    async _doAddProject(name, desc) {
        try {
            var res = await fetch(this.API + '/projects', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, description: desc }) });
            var data = await res.json();
            if (data.error) { PA.toast(data.error, 'error'); return; }
            PA.toast('Project created', 'success');
            await this.loadProjects();
            this.selectProject(name);
        } catch (e) { PA.toast('Failed: ' + e.message, 'error'); }
    },

    async deleteProject(name) {
        if (!confirm('Delete project "' + name + '" and all its environments?')) return;
        try {
            await fetch(this.API + '/projects/' + encodeURIComponent(name), { method: 'DELETE' });
            if (this._selectedProject === name) { this._selectedProject = null; this._selectedEnv = null; }
            PA.toast('Deleted', 'success'); await this.loadProjects();
        } catch (e) { PA.toast('Delete failed', 'error'); }
    },

    showAddEnv: function() {
        var name = prompt('Environment name (e.g. PROD, UAT, DEV):');
        if (!name) return;
        var zone = prompt('Zone (optional, e.g. EU-CENTRAL):') || '';
        this._doAddEnv(name, zone);
    },

    async _doAddEnv(name, zone) {
        try {
            var res = await fetch(this.API + '/projects/' + encodeURIComponent(this._selectedProject) + '/environments', {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, zone: zone })
            });
            var data = await res.json();
            if (data.error) { PA.toast(data.error, 'error'); return; }
            PA.toast('Environment created', 'success');
            await this.selectProject(this._selectedProject);
            this.selectEnv(name);
        } catch (e) { PA.toast('Failed: ' + e.message, 'error'); }
    },

    async deleteEnv(name) {
        if (!confirm('Delete environment "' + name + '" and all connections?')) return;
        try {
            await fetch(this.API + '/projects/' + encodeURIComponent(this._selectedProject) + '/environments/' + encodeURIComponent(name), { method: 'DELETE' });
            this._selectedEnv = null;
            PA.toast('Deleted', 'success'); await this.selectProject(this._selectedProject);
        } catch (e) { PA.toast('Delete failed', 'error'); }
    }
};
