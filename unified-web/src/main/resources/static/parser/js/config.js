window.PA = window.PA || {};

PA.config = {
    _projects: [],
    _selectedProject: null,
    _selectedEnv: null,
    _selectedConn: null,

    API: '/api/plsql/config',

    showModal: function() {
        var cfgEl = document.getElementById('cfgModal');
        cfgEl.style.display = 'flex';
        if (PA.modalStack) cfgEl.style.zIndex = PA.modalStack.nextZ();
        this.loadProjects();
    },

    closeModal: function(e) {
        if (e && e.target !== e.currentTarget) return;
        // A closing panel must not keep a query it has stopped applying; see
        // SearchQuery.resetWithin in shared/ui-busy.js.
        if (window.SearchQuery) SearchQuery.resetWithin(document.getElementById('cfgModal'));
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
            html += '<div class="cfg-item-name">' + PA.esc(p.name)
                 + (p.dbType ? ' ' + PA.dbSelector.badgeHtml(p.dbType) : '') + '</div>';
            html += '<div class="cfg-item-desc">' + PA.esc(p.description || '') + '</div>';
            html += '<div class="cfg-item-meta">' + ((p.environments || []).length) + ' env(s)</div>';
            html += '<button class="cfg-del" onclick="event.stopPropagation();UIBusy.run(this, function(){ return PA.config.deleteProject(\'' + PA.escJs(p.name) + '\'); })" title="Delete">&#10005;</button>';
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
                html += '<button class="cfg-del" onclick="event.stopPropagation();UIBusy.run(this, function(){ return PA.config.deleteEnv(\'' + PA.escJs(e.name) + '\'); })" title="Delete">&#10005;</button>';
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
                var defaultPort = c.dbType === 'MYSQL' ? 3306 : c.dbType === 'SQLSERVER' ? 1433 : c.dbType === 'DB2' ? 50000 : c.dbType === 'POSTGRES' ? 5432 : 1521;
                var url = c.jdbcUrl || (c.hostname
                    ? c.hostname + ':' + (c.port || defaultPort) + '/' + (c.serviceName || c.sid || '')
                    : 'N/A');
                html += '<div class="cfg-conn-item" onclick="PA.config.showConnForm(\'' + PA.escJs(c.name) + '\')">';
                html += '<div style="display:flex;align-items:center;gap:8px">';
                html += '<span class="cfg-conn-status ' + (c.tested ? 'ok' : 'untested') + '">' + (c.tested ? '&#10003;' : '?') + '</span>';
                html += '<div>';
                html += '<div class="cfg-item-name">' + PA.esc(c.name) + '</div>';
                html += '<div class="cfg-item-desc">' + PA.esc(c.username || '') + '@' + PA.esc(url) + '</div>';
                html += '</div></div>';
                html += '<div style="display:flex;gap:4px;align-items:center">';
                // DB type badge
                html += PA.dbSelector.badgeHtml(c.dbType || 'ORACLE');
                html += '<span class="cfg-conn-type-badge">' + PA.esc(c.connectionType || 'SERVICE_NAME') + '</span>';
                if (c.role && c.role !== 'DEFAULT') html += '<span class="cfg-conn-role-badge">' + PA.esc(c.role) + '</span>';
                html += '<button class="cfg-del" style="position:static;opacity:1" onclick="event.stopPropagation();UIBusy.run(this, function(){ return PA.config.deleteConn(\'' + PA.escJs(c.name) + '\'); })" title="Delete">&#10005;</button>';
                html += '</div></div>';
            }
            html += '</div>';
        }
        el.innerHTML = html;
    },

    showConnForm: function(connName) {
        this._selectedConn = connName;
        var proj = this._projects.find(function(p) { return p.name === PA.config._selectedProject; });
        var env  = proj && proj.environments ? proj.environments.find(function(e) { return e.name === PA.config._selectedEnv; }) : null;
        var existing = connName && env && env.connections ? env.connections.find(function(c) { return c.name === connName; }) : null;
        var isEdit = !!existing;
        // A project tied to a datasource fixes every connection's DB type (lock the selector).
        var projDb = proj && proj.dbType ? String(proj.dbType).toUpperCase() : '';
        // Clone the existing connection so setting dbType for the form never mutates the cached model.
        var c = existing ? Object.assign({}, existing) : { connectionType: 'SERVICE_NAME', role: 'DEFAULT', port: 0, dbType: projDb || 'ORACLE' };
        if (projDb) c.dbType = projDb;   // new + existing connections follow the project's datasource
        var lockDb = !!projDb;

        var el = document.getElementById('cfgDetailPanel');

        // Header row
        var headerHtml = '<div class="cfg-section-head"><span>' + (isEdit ? 'Edit' : 'New') + ' Connection</span>';
        headerHtml += '<button class="btn btn-sm" onclick="PA.config.selectEnv(\'' + PA.escJs(this._selectedEnv) + '\')">&larr; Back</button></div>';

        // Wrapper div for the form body (PA.connForm renders into this)
        var wrapId = 'connFormWrap';
        // Action bar (PA.connTest renders into this)
        var actionId = 'connFormActionWrap';

        el.innerHTML = headerHtml
            + '<div id="' + wrapId   + '" style="overflow-y:auto;max-height:calc(80vh - 150px)"></div>'
            + '<div id="' + actionId + '"></div>';

        // Init PA.connTest with project/env context and a save callback
        PA.connTest.init({
            apiBase:  this.API,
            isEdit:   isEdit,
            project:  this._selectedProject,
            env:      this._selectedEnv,
            connName: connName,
            onSave:   function () {
                PA.config.selectProject(PA.config._selectedProject);
                PA.config.selectEnv(PA.config._selectedEnv);
            }
        });

        // Render form and action bar
        PA.connForm.render(document.getElementById(wrapId), c, isEdit, lockDb);
        PA.connTest.render(document.getElementById(actionId));
    },

    async deleteConn(connName) {
        if (!confirm('Delete connection "' + connName + '"?')) return;
        var url = this.API + '/projects/' + encodeURIComponent(this._selectedProject) +
                  '/environments/' + encodeURIComponent(this._selectedEnv) + '/connections/' + encodeURIComponent(connName);
        // fetch() only rejects on a network failure — a 500 resolves normally. Without this check
        // a delete that the server refused still reported "Deleted" and re-rendered as if gone.
        try {
            var res = await fetch(url, { method: 'DELETE' });
            if (!res.ok) throw new Error('HTTP ' + res.status);
            PA.toast('Deleted', 'success');
            await this.selectProject(this._selectedProject);
            this.selectEnv(this._selectedEnv);
        } catch (e) { PA.toast('Delete failed (' + e.message + ')', 'error'); }
    },

    // ── Lightweight form modal (replaces browser prompt()) ──
    _closeFormModal: function() { var m = document.getElementById('cfgFormModal'); if (m) m.remove(); },
    _formModal: function(opts) {
        PA.config._closeFormModal();
        var fields = opts.fields || [];
        var body = '';
        for (var i = 0; i < fields.length; i++) {
            var f = fields[i];
            body += '<label class="cfm-row"><span>' + PA.esc(f.label) + (f.required ? ' <b style="color:var(--red)">*</b>' : '') + '</span>';
            if (f.type === 'select') {
                body += '<select id="cfm_' + f.key + '">';
                (f.options || []).forEach(function(o) { body += '<option value="' + PA.escAttr(o.value) + '">' + PA.esc(o.label) + '</option>'; });
                body += '</select>';
            } else {
                body += '<input id="cfm_' + f.key + '" type="text" placeholder="' + PA.escAttr(f.placeholder || '') + '">';
            }
            body += '</label>';
        }
        var html = '<div class="cfm-overlay" id="cfgFormModal">'
            + '<div class="cfm-box">'
            + '<div class="cfm-head"><span>' + PA.esc(opts.title) + '</span><button class="cfm-x" onclick="PA.config._closeFormModal()">&times;</button></div>'
            + '<div class="cfm-body">' + body + '<div class="cfm-err" id="cfmErr"></div></div>'
            + '<div class="cfm-foot"><button class="btn btn-sm" onclick="PA.config._closeFormModal()">Cancel</button>'
            + '<button class="btn btn-sm btn-primary" id="cfmOk">' + PA.esc(opts.okLabel || 'Create') + '</button></div>'
            + '</div></div>';
        PA.mountOverlay(html);
        var overlay = document.getElementById('cfgFormModal');
        overlay.onclick = function(e) { if (e.target === overlay) PA.config._closeFormModal(); };
        if (PA.modalStack) overlay.style.zIndex = PA.modalStack.nextZ();
        document.getElementById('cfmOk').onclick = function() {
            var vals = {};
            for (var j = 0; j < fields.length; j++) vals[fields[j].key] = (document.getElementById('cfm_' + fields[j].key).value || '').trim();
            for (var k = 0; k < fields.length; k++) {
                if (fields[k].required && !vals[fields[k].key]) { document.getElementById('cfmErr').textContent = fields[k].label + ' is required.'; return; }
            }
            PA.config._closeFormModal();
            opts.onSubmit(vals);
        };
        var first = document.querySelector('#cfgFormModal input, #cfgFormModal select');
        if (first) first.focus();
    },

    showAddProject: function() {
        PA.config._formModal({
            title: 'New Project', okLabel: 'Create Project',
            fields: [
                { key: 'name', label: 'Project name', required: true, placeholder: 'e.g. OracleDemo' },
                { key: 'description', label: 'Description', placeholder: 'optional' },
                { key: 'dbType', label: 'Datasource', type: 'select', required: true, options: [
                    { value: 'ORACLE', label: 'Oracle' }, { value: 'SQLSERVER', label: 'SQL Server' },
                    { value: 'MYSQL', label: 'MySQL' }, { value: 'DB2', label: 'IBM Db2' },
                    { value: 'POSTGRES', label: 'PostgreSQL' }
                ] }
            ],
            onSubmit: function(v) { PA.config._doAddProject(v.name, v.description, v.dbType); }
        });
    },

    async _doAddProject(name, desc, dbType) {
        try {
            var res = await fetch(this.API + '/projects', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, description: desc, dbType: dbType }) });
            if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
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
            var res = await fetch(this.API + '/projects/' + encodeURIComponent(name), { method: 'DELETE' });
            if (!res.ok) throw new Error('HTTP ' + res.status);
            if (this._selectedProject === name) { this._selectedProject = null; this._selectedEnv = null; }
            PA.toast('Deleted', 'success'); await this.loadProjects();
        } catch (e) { PA.toast('Delete failed (' + e.message + ')', 'error'); }
    },

    showAddEnv: function() {
        if (!PA.config._selectedProject) { PA.toast('Select a project first', 'warn'); return; }
        PA.config._formModal({
            title: 'New Environment', okLabel: 'Create Environment',
            fields: [
                { key: 'name', label: 'Environment name', required: true, placeholder: 'e.g. PROD, UAT, DEV' },
                { key: 'zone', label: 'Zone', placeholder: 'optional, e.g. EU-CENTRAL' }
            ],
            onSubmit: function(v) { PA.config._doAddEnv(v.name, v.zone); }
        });
    },

    async _doAddEnv(name, zone) {
        try {
            var res = await fetch(this.API + '/projects/' + encodeURIComponent(this._selectedProject) + '/environments', {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, zone: zone })
            });
            if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
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
            var res = await fetch(this.API + '/projects/' + encodeURIComponent(this._selectedProject) + '/environments/' + encodeURIComponent(name), { method: 'DELETE' });
            if (!res.ok) throw new Error('HTTP ' + res.status);
            this._selectedEnv = null;
            PA.toast('Deleted', 'success'); await this.selectProject(this._selectedProject);
        } catch (e) { PA.toast('Delete failed (' + e.message + ')', 'error'); }
    }
};
