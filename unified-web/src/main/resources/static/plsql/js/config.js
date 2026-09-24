/**
 * PA.config — DB Config & Environment Management module.
 * Project → Environment → Connections (SQL Developer-style: each has own host/port/service/user/pwd).
 * Test must pass before save.
 */
window.PA = window.PA || {};

PA.config = {
    _projects: [],
    _selectedProject: null,
    _selectedEnv: null,
    _selectedConn: null,
    _testPassed: false,
    _tlsDbType: 'ORACLE',
    // Standard TCP port → DB type. Superseded at runtime by the server table
    // (GET /api/plsql/config/db-type-suggest); this built-in copy is the offline fallback so the
    // port hint still works if that call fails. Keys are STRINGS because they come from JSON.
    _portDbTypeMap: { '1521': 'ORACLE', '3306': 'MYSQL', '1433': 'SQLSERVER', '50000': 'DB2', '5432': 'POSTGRES' },
    // Last "recent connections" list built by _recentHtml, indexed by the picker's option value.
    _recentConns: [],
    // TLS certificate slots. `prop` maps to the camelCase ConnectionConfig JSON field;
    // `filename` is the fallback upload name (used for pasted content).
    _TLS_CERTS: {
        trust:      { label: 'Trust Cert',     title: 'CA / trust certificate (PEM)', prop: 'trustCertPath',      filename: 'trust.pem',       binary: false, accept: '.pem,.crt,.cer,.txt', pathId: 'cfTrustPath',      fileId: 'cfTrustFile',      pasteId: 'cfTrustPaste',      statusId: 'cfTrustStatus' },
        clientCert: { label: 'Client Cert',    title: 'Client certificate (PEM)',     prop: 'clientCertPath',     filename: 'client-cert.pem', binary: false, accept: '.pem,.crt,.cer,.txt', pathId: 'cfClientCertPath', fileId: 'cfClientCertFile', pasteId: 'cfClientCertPaste', statusId: 'cfClientCertStatus' },
        clientKey:  { label: 'Client Key',     title: 'Client private key (PEM)',     prop: 'clientKeyPath',      filename: 'client-key.pem',  binary: false, accept: '.pem,.key,.txt',      pathId: 'cfClientKeyPath',  fileId: 'cfClientKeyFile',  pasteId: 'cfClientKeyPaste',  statusId: 'cfClientKeyStatus' },
        wallet:     { label: 'Wallet (.zip)',  title: 'Oracle wallet (.zip)',         prop: 'walletPath',          filename: 'wallet.zip',      binary: true,  accept: '.zip',                pathId: 'cfWalletPath',     fileId: 'cfWalletFile',     pasteId: 'cfWalletPaste',     statusId: 'cfWalletStatus' },
        truststore: { label: 'Truststore',     title: 'Truststore (.jks)',            prop: 'truststorePath',      filename: 'truststore.jks',  binary: true,  accept: '.jks,.p12,.pfx',      pathId: 'cfTruststorePath', fileId: 'cfTruststoreFile', pasteId: 'cfTruststorePaste', statusId: 'cfTruststoreStatus' }
    },

    async showModal() {
        document.getElementById('configModal').style.display = 'flex';
        await this._loadPortSuggestions();
        await this.loadProjects();
    },

    /**
     * Pull the standard port → DB-type table from the server so the mapping has one authority
     * (`DbType.standardPortMap`). Best-effort: on any failure the built-in `_portDbTypeMap` stays in
     * place, so port auto-detection never depends on a successful fetch.
     */
    async _loadPortSuggestions() {
        try {
            const map = await PA.api.getDbTypeSuggestions();
            if (map && typeof map === 'object' && Object.keys(map).length) this._portDbTypeMap = map;
        } catch (e) { /* keep the built-in fallback table */ }
    },

    closeModal() {
        document.getElementById('configModal').style.display = 'none';
    },

    async loadProjects() {
        this._projects = await PA.api.listProjects();
        this._renderProjects();
    },

    _renderProjects() {
        const el = document.getElementById('cfgProjectList');
        // Recent connections sit at the very top of the setup modal, above the project tree.
        const recent = this._recentHtml();
        if (!this._projects.length) {
            el.innerHTML = recent + '<div class="cfg-empty">No projects. Click "+ Add Project".</div>';
            document.getElementById('cfgEnvPanel').innerHTML = '';
            document.getElementById('cfgDetailPanel').innerHTML = '';
            return;
        }
        el.innerHTML = recent + this._projects.map(p => `
            <div class="cfg-item ${this._selectedProject === p.name ? 'active' : ''}"
                 onclick="PA.config.selectProject('${this._esc(p.name)}')">
                <div class="cfg-item-name">${this._esc(p.name)}</div>
                <div class="cfg-item-desc">${this._esc(p.description || '')}</div>
                <div class="cfg-item-meta">${(p.environments || []).length} env(s)</div>
                <button class="cfg-del" onclick="event.stopPropagation();PA.config.deleteProject('${this._esc(p.name)}')" title="Delete">&#10005;</button>
            </div>
        `).join('');
    },

    /**
     * "Recent connections" picker for the top of the setup modal. Flattens every connection across
     * every project/environment (the config is global — there is no per-user store) and sorts by
     * `lastTestedAt` descending, which the backend stamps on every successful test/save
     * (ConfigController). Connections that predate the field carry no timestamp and fall to the
     * bottom, ordered by name. Selecting an entry jumps straight to that connection's editor.
     *
     * Returns '' when there are no connections, so the modal looks unchanged on a fresh install.
     */
    _recentHtml() {
        const recent = [];
        (this._projects || []).forEach(p => (p.environments || []).forEach(e => (e.connections || []).forEach(c => {
            recent.push({
                project: p.name,
                env: e.name,
                name: c.name,
                scope: this._connScope(c),
                tested: !!c.tested,
                lastTestedAt: c.lastTestedAt || 0
            });
        })));
        recent.sort((a, b) => (b.lastTestedAt - a.lastTestedAt) || a.name.localeCompare(b.name));
        this._recentConns = recent;
        if (!recent.length) return '';
        const opts = recent.map((r, i) =>
            `<option value="${i}">${this._esc(r.name)} — ${this._esc(r.project)}/${this._esc(r.env)}${r.scope === 'user' ? ' (private)' : ''}${r.tested ? '' : ' (untested)'}</option>`
        ).join('');
        return `<div class="cfg-recent" style="padding:8px 10px;border-bottom:1px solid var(--border)">
            <div style="font-size:11px;font-weight:700;color:var(--text);margin-bottom:4px">Recent connections</div>
            <select class="form-select cfg-inp" style="width:100%" onchange="PA.config.openRecent(this.value)">
                <option value="">Jump to a connection…</option>
                ${opts}
            </select>
        </div>`;
    },

    /** Navigate to the project/env of a recent-connection entry and open its editor. */
    async openRecent(idx) {
        if (idx === '' || idx == null) return;
        const r = this._recentConns && this._recentConns[parseInt(idx, 10)];
        if (!r) return;
        await this.selectProject(r.project);
        await this.selectEnv(r.env);
        this.showConnForm(r.name, null, r.scope);
    },

    async selectProject(name) {
        this._selectedProject = name;
        this._selectedEnv = null;
        this._selectedConn = null;
        this._renderProjects();
        const envs = await PA.api.listEnvironments(name);
        // Normalize each env's embedded connections (grouped {shared, mine} or legacy array) so the
        // env list's connection counts and any later render see a flat, scope-tagged list.
        (Array.isArray(envs) ? envs : []).forEach(e => {
            if (!e) return;
            e._connScoped = PA.api._isGroupedConnections(e.connections);
            e.connections = PA.api._normalizeConnections(e.connections);
        });
        const project = this._projects.find(p => p.name === name);
        if (project) project.environments = envs;
        this._renderEnvs(envs);
        document.getElementById('cfgDetailPanel').innerHTML = '';
    },

    _renderEnvs(envs) {
        const el = document.getElementById('cfgEnvPanel');
        let html = `<div class="cfg-section-head">
            <span>Environments</span>
            <button class="btn btn-sm btn-primary" onclick="PA.config.showAddEnv()">+ Env</button>
        </div>`;
        if (!envs || !envs.length) {
            html += '<div class="cfg-empty">No environments.</div>';
        } else {
            html += envs.map(e => {
                const connCount = (e.connections || []).length;
                return `
                <div class="cfg-item ${this._selectedEnv === e.name ? 'active' : ''}"
                     onclick="PA.config.selectEnv('${this._esc(e.name)}')">
                    <div class="cfg-item-name">${this._esc(e.name)}${e.zone ? ' <span class="cfg-zone">' + this._esc(e.zone) + '</span>' : ''}</div>
                    <div class="cfg-item-meta">${connCount} connection(s)</div>
                    <button class="cfg-del" onclick="event.stopPropagation();PA.config.deleteEnv('${this._esc(e.name)}')" title="Delete">&#10005;</button>
                </div>`;
            }).join('');
        }
        el.innerHTML = html;
    },

    async selectEnv(name) {
        this._selectedEnv = name;
        this._selectedConn = null;
        const project = this._projects.find(p => p.name === this._selectedProject);
        if (project) this._renderEnvs(project.environments);
        await this._loadEnvConnections(this._selectedProject, name);
        if (project) this._renderEnvs(project.environments);
        const env = project?.environments?.find(e => e.name === name);
        if (env) this._renderConnections(env);
    },

    /**
     * Resolve an environment's connections into a flat list tagged with scope. The connections may
     * arrive two ways: embedded in the project/env payload (already normalized by
     * `PA.api.listProjects`, flagged `_connScoped` when the server sent the grouped shape) or via
     * the dedicated GET connections endpoint (`PA.api.listConnections`). Whichever the backend
     * populates wins; the legacy plain-array response still renders (everything as "shared").
     */
    async _loadEnvConnections(projectName, envName) {
        const project = this._projects.find(p => p.name === projectName);
        const env = project?.environments?.find(e => e.name === envName);
        if (!env) return null;
        const embeddedScoped = !!env._connScoped || PA.api._isGroupedConnections(env.connections);
        env.connections = PA.api._normalizeConnections(env.connections);
        if (embeddedScoped) return env;
        try {
            const grouped = await PA.api.listConnections(projectName, envName);
            const flat = PA.api._normalizeConnections(grouped);
            if (flat.length || !env.connections.length) env.connections = flat;
        } catch (e) { /* keep the embedded list */ }
        return env;
    },

    /** The scope a connection lives in: 'user' (private) or 'shared' (team). */
    _connScope(c) {
        return (c && c.scope === 'user') ? 'user' : 'shared';
    },

    _renderConnections(env) {
        const el = document.getElementById('cfgDetailPanel');
        let html = `<div class="cfg-section-head">
            <span>${this._esc(env.name)} — Connections</span>
            <button class="btn btn-sm btn-primary" onclick="PA.config.showConnForm(null)">+ New Connection</button>
        </div>`;
        const conns = env.connections || [];
        if (!conns.length) {
            html += '<div class="cfg-empty">No connections. Click "+ New Connection" to add one (like SQL Developer).</div>';
        } else {
            // Two visually distinct groups: the user's private connections first, then team-shared.
            const mine = conns.filter(c => this._connScope(c) === 'user');
            const shared = conns.filter(c => this._connScope(c) === 'shared');
            html += '<div class="cfg-conn-list">'
                + this._connGroupHtml('My connections', 'PRIVATE', mine)
                + this._connGroupHtml('Shared', 'TEAM', shared)
                + '</div>';
        }
        el.innerHTML = html;
    },

    /** A titled group with a scope badge and its connection entries. */
    _connGroupHtml(title, badge, list) {
        const badgeStyle = badge === 'PRIVATE'
            ? 'background:var(--badge-gray-bg);color:var(--badge-gray)'
            : 'background:var(--badge-green-bg);color:var(--green,var(--green))';
        let html = `<div style="display:flex;align-items:center;gap:6px;margin:8px 0 4px">
            <span style="font-size:11px;font-weight:700;color:var(--text)">${title}</span>
            <span style="font-size:9px;font-weight:700;letter-spacing:0.5px;padding:1px 6px;border-radius:999px;${badgeStyle}">${badge}</span>
            <span style="font-size:10px;color:var(--text-muted)">(${list.length})</span>
        </div>`;
        if (!list.length) {
            html += '<div class="cfg-empty" style="padding:2px 0;font-size:11px">None.</div>';
            return html;
        }
        return html + list.map(c => this._connItemHtml(c)).join('');
    },

    /** One connection row, keeping the existing actions (edit / test / duplicate / delete). */
    _connItemHtml(c) {
        const scope = this._connScope(c);
        const url = c.jdbcUrl || (c.hostname ? c.hostname + ':' + (c.port||1521) + '/' + (c.serviceName||c.sid||'') : 'N/A');
        return `
                <div class="cfg-conn-item" onclick="PA.config.showConnForm('${this._esc(c.name)}', null, '${scope}')">
                    <div style="display:flex;align-items:center;gap:8px">
                        <span class="cfg-conn-status ${c.tested ? 'ok' : 'untested'}">${c.tested ? '&#10003;' : '?'}</span>
                        <div>
                            <div class="cfg-item-name">${this._esc(c.name)}</div>
                            <div class="cfg-item-desc">${this._esc(c.username || '')}@${this._esc(url)}</div>
                        </div>
                    </div>
                    <div style="display:flex;gap:4px;align-items:center">
                        <span class="cfg-conn-type-badge">${this._esc(c.connectionType || 'SERVICE_NAME')}</span>
                        ${c.role && c.role !== 'DEFAULT' ? '<span class="cfg-conn-role-badge">' + this._esc(c.role) + '</span>' : ''}
                        <button class="btn btn-sm" style="padding:0 6px;font-size:10px"
                                onclick="event.stopPropagation();PA.config.duplicateConn('${this._esc(c.name)}', '${scope}')"
                                title="Duplicate this connection's settings into a new one">Dup</button>
                        <button class="cfg-del" onclick="event.stopPropagation();PA.config.deleteConn('${this._esc(c.name)}', '${scope}')" title="Delete">&#10005;</button>
                    </div>
                </div>`;
    },

    /** A segmented-control option (radio) for the connection's scope. */
    _scopeRadio(value, label, id, current) {
        const on = current === value;
        return `<label id="${id}" style="display:flex;align-items:center;gap:5px;padding:4px 10px;border:1px solid ${on ? 'var(--accent)' : 'var(--border)'};border-radius:6px;cursor:pointer;font-size:12px">
            <input type="radio" name="cfScope" value="${value}" ${on ? 'checked' : ''} style="margin:0" onchange="PA.config._onScopeChange()">
            <span>${label}</span></label>`;
    },

    /** The scope currently selected in the form; defaults to 'user' when nothing is picked. */
    _scopeValue() {
        const el = document.querySelector('input[name="cfScope"]:checked');
        return el ? el.value : (this._formScope || 'user');
    },

    /** Visually flag the selected scope. Scope is not a connection parameter, so it leaves the
     *  test gate alone (no need to re-test just for moving private ↔ shared). */
    _onScopeChange() {
        const sel = this._scopeValue();
        ['user', 'shared'].forEach(s => {
            const lbl = document.getElementById(s === 'user' ? 'cfScopeUserLbl' : 'cfScopeSharedLbl');
            if (lbl) lbl.style.borderColor = (sel === s) ? 'var(--accent)' : 'var(--border)';
        });
    },

    /**
     * Open the connection editor. `connName` selects an existing connection to edit (null = new).
     * `source` is an optional pre-filled object used for the "Duplicate" flow: its NON-SECRET fields
     * arrive as a fresh, unsaved connection (name already suffixed by `duplicateConn`).
     * `scopeHint` ('user' | 'shared') disambiguates a name that exists in both scopes.
     *
     * The scope toggle defaults to 'user' (private) for a NEW connection; when editing it is preset
     * from the listing the connection came from and kept unless the user changes it.
     */
    showConnForm(connName, source, scopeHint) {
        this._selectedConn = connName;
        this._testPassed = false;
        const project = this._projects.find(p => p.name === this._selectedProject);
        const env = project?.environments?.find(e => e.name === this._selectedEnv);
        let existing = null;
        if (connName) {
            existing = env?.connections?.find(c => c.name === connName && (!scopeHint || this._connScope(c) === scopeHint))
                || env?.connections?.find(c => c.name === connName);
        }

        const el = document.getElementById('cfgDetailPanel');
        const c = existing || source || { connectionType: 'SERVICE_NAME', role: 'DEFAULT', port: 1521 };
        const isEdit = !!existing;
        const initialScope = existing ? this._connScope(existing)
            : (source ? this._connScope(source) : 'user');
        this._formScope = initialScope;

        el.innerHTML = `
        <div class="cfg-section-head">
            <span>${isEdit ? 'Edit' : 'New'} Connection</span>
            <button class="btn btn-sm" onclick="PA.config.selectEnv('${this._esc(this._selectedEnv)}')">&larr; Back</button>
        </div>
        <div class="cfg-form" style="max-height:calc(80vh - 120px);overflow-y:auto">
            <div class="cfg-row"><label>Connection Name</label>
                <input id="cfConn" value="${this._esc(c.name || '')}" class="form-input cfg-inp" placeholder="e.g. OPUS_CORE_PROD" oninput="PA.config._tlsNameChanged()" ${isEdit ? 'readonly style="opacity:0.6"' : ''}></div>
            <div class="cfg-row"><label>Description</label>
                <input id="cfDesc" value="${this._esc(c.description || '')}" class="form-input cfg-inp" placeholder="Optional description"></div>
            <div class="cfg-row"><label>Scope</label>
                <div style="display:flex;gap:6px">
                    ${this._scopeRadio('user', 'My connections (private)', 'cfScopeUserLbl', initialScope)}
                    ${this._scopeRadio('shared', 'Shared (team)', 'cfScopeSharedLbl', initialScope)}
                </div></div>

            <div style="margin:12px 0 6px;font-size:12px;font-weight:700;color:var(--text)">Authentication</div>
            <div class="cfg-row"><label>Username</label>
                <input id="cfUser" value="${this._esc(c.username || '')}" class="form-input cfg-inp"></div>
            <div class="cfg-row"><label>Password</label>
                <input id="cfPass" type="password" value="${this._esc(c.password || '')}" class="form-input cfg-inp"></div>
            <div class="cfg-row"><label>Role</label>
                <select id="cfRole" class="form-select cfg-inp">
                    <option value="DEFAULT" ${c.role==='DEFAULT'?'selected':''}>Default</option>
                    <option value="SYSDBA" ${c.role==='SYSDBA'?'selected':''}>SYSDBA</option>
                    <option value="SYSOPER" ${c.role==='SYSOPER'?'selected':''}>SYSOPER</option>
                </select></div>

            <div style="margin:12px 0 6px;font-size:12px;font-weight:700;color:var(--text)">Connection Details</div>
            <div class="cfg-row"><label>Database Type</label>
                <select id="cfDbType" class="form-select cfg-inp" onchange="PA.config._onDbTypeChange()">
                    ${['ORACLE','MYSQL','SQLSERVER','DB2','POSTGRES'].map(t =>
                        `<option value="${t}" ${((c.dbType || 'ORACLE').toUpperCase() === t) ? 'selected' : ''}>${t}</option>`
                    ).join('')}
                </select></div>
            <div class="cfg-row" style="margin-top:-6px"><label></label>
                <span id="cfDbTypeHint" style="font-size:11px;color:var(--text-muted)"></span></div>
            <div class="cfg-row"><label>Connection Type</label>
                <select id="cfType" class="form-select cfg-inp" onchange="PA.config._onTypeChange()">
                    <option value="SERVICE_NAME" ${c.connectionType==='SERVICE_NAME'?'selected':''}>Service Name</option>
                    <option value="SID" ${c.connectionType==='SID'?'selected':''}>SID</option>
                    <option value="TNS" ${c.connectionType==='TNS'?'selected':''}>TNS</option>
                    <option value="LDAP" ${c.connectionType==='LDAP'?'selected':''}>LDAP</option>
                    <option value="CUSTOM" ${c.connectionType==='CUSTOM'?'selected':''}>Custom JDBC URL</option>
                </select></div>

            <div id="cfFieldsServiceName" class="cfg-type-fields">
                <div class="cfg-row"><label>Hostname</label>
                    <input id="cfHost" value="${this._esc(c.hostname || '')}" class="form-input cfg-inp" placeholder="e.g. dbhost.company.com"></div>
                <div class="cfg-row"><label>Port</label>
                    <input id="cfPort" type="number" value="${c.port || 1521}" class="form-input cfg-inp" style="width:100px" oninput="PA.config._suggestDbType('cfPort')"></div>
                <div class="cfg-row"><label>Service Name</label>
                    <input id="cfSvc" value="${this._esc(c.serviceName || '')}" class="form-input cfg-inp" placeholder="e.g. ORCL"></div>
            </div>
            <div id="cfFieldsSid" class="cfg-type-fields" style="display:none">
                <div class="cfg-row"><label>Hostname</label>
                    <input id="cfHostSid" value="${this._esc(c.hostname || '')}" class="form-input cfg-inp"></div>
                <div class="cfg-row"><label>Port</label>
                    <input id="cfPortSid" type="number" value="${c.port || 1521}" class="form-input cfg-inp" style="width:100px" oninput="PA.config._suggestDbType('cfPortSid')"></div>
                <div class="cfg-row"><label>SID</label>
                    <input id="cfSid" value="${this._esc(c.sid || '')}" class="form-input cfg-inp" placeholder="e.g. ORCL"></div>
            </div>
            <div id="cfFieldsTns" class="cfg-type-fields" style="display:none">
                <div class="cfg-row"><label>TNS Alias</label>
                    <input id="cfTns" value="${this._esc(c.tnsAlias || '')}" class="form-input cfg-inp" placeholder="e.g. MYDB_PROD"></div>
            </div>
            <div id="cfFieldsCustom" class="cfg-type-fields" style="display:none">
                <div class="cfg-row"><label>JDBC URL</label>
                    <input id="cfCustomUrl" value="${this._esc(c.customUrl || '')}" class="form-input cfg-inp" placeholder="jdbc:oracle:thin:@..."></div>
            </div>
            <div id="cfFieldsLdap" class="cfg-type-fields" style="display:none">
                <div class="cfg-row"><label>LDAP URL</label>
                    <input id="cfLdapUrl" value="${this._esc(c.customUrl || '')}" class="form-input cfg-inp" placeholder="jdbc:oracle:thin:@ldap://..."></div>
            </div>

            <div style="margin:12px 0 6px;font-size:12px;font-weight:700;color:var(--text)">Schemas</div>
            <div class="cfg-row">
                <label>Schemas <span style="font-weight:400;color:var(--text-muted)">(auto-discovered on test; editable)</span></label>
                <input id="cfSchemas" value="${this._esc((c.schemas && c.schemas.length ? c.schemas : (c.username ? [c.username.toUpperCase()] : [])).join(', '))}"
                       class="form-input cfg-inp" placeholder="Discovered after Test Connection, e.g. ANALYZER, ANALYZER_REF">
            </div>

            ${this._tlsSectionHtml(c)}

            <div style="margin-top:16px;display:flex;gap:8px;align-items:center">
                <button class="btn btn-sm" id="cfTestBtn" onclick="PA.config._testConn()" style="color:var(--green,var(--green))">&#9889; Test Connection</button>
                <button class="btn btn-primary btn-sm" id="cfSaveBtn" onclick="PA.config._saveConn(${isEdit})" disabled>Save</button>
                <span id="cfTestStatus" style="font-size:11px;color:var(--text-muted)">Test required before save</span>
            </div>
            <div id="cfTestResult" style="margin-top:8px"></div>
        </div>`;

        this._onTypeChange();
        this._tlsInit(c);
    },

    // ── TLS / Security ────────────────────────────────────────────────────────
    _tlsSectionHtml(c) {
        const ssl = (c.sslMode || 'NONE').toUpperCase();
        const hv = c.hostnameValidation !== false;
        return `
        <div id="cfTlsHead" style="margin:12px 0 6px;font-size:12px;font-weight:700;color:var(--text);cursor:pointer;user-select:none"
             onclick="PA.config._toggleTls()">
            <span id="cfTlsCaret">&#9654;</span> TLS / Security
        </div>
        <div id="cfTlsBody" style="display:none">
            <div id="cfTlsNoName" style="display:none;font-size:11px;color:var(--text-muted);margin-bottom:6px">
                Enter a Connection Name above before uploading certificates.
            </div>
            <div class="cfg-row"><label>SSL Mode</label>
                <select id="cfSslMode" class="form-select cfg-inp" onchange="PA.config._onSslModeChange()">
                    <option value="NONE" ${ssl==='NONE'?'selected':''}>None</option>
                    <option value="REQUIRED" ${ssl==='REQUIRED'?'selected':''}>Required (encrypted, no verify)</option>
                    <option value="VERIFY_CA" ${ssl==='VERIFY_CA'?'selected':''}>Verify CA</option>
                    <option value="VERIFY_FULL" ${ssl==='VERIFY_FULL'?'selected':''}>Verify Full</option>
                </select></div>
            <div class="cfg-row"><label>Hostname Check</label>
                <label style="display:flex;align-items:center;gap:6px;font-weight:400;width:auto">
                    <input id="cfHostnameValidation" type="checkbox" ${hv?'checked':''}>
                    <span style="font-size:12px;color:var(--text-muted);font-weight:400">Verify server hostname matches certificate</span>
                </label></div>
            <div id="cfTlsTrustRow">${this._tlsRow('trust', c)}</div>
            <div id="cfTlsClientCertRow">${this._tlsRow('clientCert', c)}</div>
            <div id="cfTlsClientKeyRow">${this._tlsRow('clientKey', c)}</div>
            <div id="cfTlsWalletRow">${this._tlsRow('wallet', c)}</div>
            <div id="cfTlsTruststoreRow">${this._tlsRow('truststore', c)}</div>
            <div id="cfTlsTruststorePwRow" class="cfg-row" style="display:none"><label>Truststore Pwd</label>
                <input id="cfTruststorePassword" type="password" value="${this._esc(c.truststorePassword || '')}" class="form-input cfg-inp" placeholder="Truststore password"></div>
        </div>`;
    },

    _tlsRow(key, c) {
        const m = this._TLS_CERTS[key];
        const val = (c && c[m.prop]) || '';
        return `
            <div class="cfg-row"><label title="${m.title}">${m.label}</label>
                <input id="${m.pathId}" value="${this._esc(val)}" class="form-input cfg-inp" readonly
                       placeholder="No file uploaded" title="Uploaded relative path (read-only)"></div>
            <div class="cfg-row" style="align-items:flex-start">
                <label></label>
                <div style="flex:1;display:flex;flex-direction:column;gap:6px">
                    <input id="${m.fileId}" type="file" accept="${m.accept}" class="form-input cfg-inp"
                           onchange="PA.config._uploadCert('${key}')">
                    <textarea id="${m.pasteId}" rows="2" class="form-input cfg-inp"
                              placeholder="${m.binary ? 'Or paste base64 content' : 'Or paste PEM content'}"
                              style="font-family:monospace;font-size:11px"></textarea>
                    <div style="display:flex;gap:8px;align-items:center">
                        <button type="button" class="btn btn-sm" onclick="PA.config._uploadCert('${key}', true)">Upload pasted</button>
                        <span id="${m.statusId}" style="font-size:11px;color:var(--text-muted)"></span>
                    </div>
                </div>
            </div>`;
    },

    _tlsInit(c) {
        // The Database Type select is the single source of truth for the form's type (it already
        // reflects c.dbType), so the TLS rows below match whatever the user sees selected.
        this._tlsDbType = (document.getElementById('cfDbType')?.value || (c && c.dbType) || 'ORACLE').toUpperCase();
        this._tlsApplyDbType();
        this._onSslModeChange();
        this._tlsNameChanged();
        this._tlsUpdateTruststorePw();
    },

    _toggleTls() {
        const body = document.getElementById('cfTlsBody');
        const caret = document.getElementById('cfTlsCaret');
        if (!body) return;
        const show = !body.style.display || body.style.display === 'none';
        body.style.display = show ? 'block' : 'none';
        if (caret) caret.innerHTML = show ? '&#9660;' : '&#9654;';
    },

    _onSslModeChange() {
        const mode = document.getElementById('cfSslMode')?.value || 'NONE';
        const cb = document.getElementById('cfHostnameValidation');
        if (cb) {
            const wasDisabled = cb.disabled;
            if (mode === 'NONE') { cb.checked = false; cb.disabled = true; }
            else { cb.disabled = false; if (wasDisabled) cb.checked = true; }
        }
        this._tlsUpdateTruststorePw();
        this._tlsResetTestGate();
    },

    _tlsApplyDbType() {
        const db = this._tlsDbType || 'ORACLE';
        const wallet = document.getElementById('cfTlsWalletRow');
        const truststore = document.getElementById('cfTlsTruststoreRow');
        if (wallet) wallet.style.display = (db === 'ORACLE') ? '' : 'none';
        if (truststore) truststore.style.display = (db === 'DB2') ? '' : 'none';
        this._tlsUpdateTruststorePw();
    },

    _tlsUpdateTruststorePw() {
        const row = document.getElementById('cfTlsTruststorePwRow');
        if (!row) return;
        const path = (document.getElementById('cfTruststorePath')?.value || '').trim();
        row.style.display = path ? '' : 'none';
    },

    _tlsNameChanged() {
        const name = (document.getElementById('cfConn')?.value || '').trim();
        const hint = document.getElementById('cfTlsNoName');
        if (hint) hint.style.display = name ? 'none' : 'block';
        const body = document.getElementById('cfTlsBody');
        if (!body) return;
        body.querySelectorAll('input, select, textarea, button').forEach(el => { el.disabled = !name; });
    },

    _tlsResetTestGate() {
        this._testPassed = false;
        const saveBtn = document.getElementById('cfSaveBtn');
        if (saveBtn) saveBtn.disabled = true;
        const status = document.getElementById('cfTestStatus');
        if (status) { status.textContent = 'Test required before save'; status.style.color = 'var(--text-muted)'; }
    },

    async _uploadCert(key, fromPaste) {
        const m = this._TLS_CERTS[key];
        if (!m) return;
        const connName = (document.getElementById('cfConn')?.value || '').trim();
        const statusEl = document.getElementById(m.statusId);
        if (!connName) { if (statusEl) statusEl.textContent = 'Connection name required'; return; }

        let filename = m.filename;
        let content = '';
        if (fromPaste) {
            content = (document.getElementById(m.paste)?.value || '').trim();
            if (!content) { if (statusEl) statusEl.textContent = 'Nothing to upload'; return; }
        } else {
            const f = document.getElementById(m.fileId)?.files?.[0];
            if (!f) return;
            filename = f.name || m.filename;
            try {
                content = m.binary ? await this._readFileBase64(f) : await this._readFileText(f);
            } catch (e) {
                if (statusEl) statusEl.textContent = 'Read failed: ' + e.message;
                return;
            }
        }

        if (statusEl) statusEl.textContent = 'Uploading…';
        try {
            const res = await PA.api.uploadCert(connName, filename, content);
            if (res && res.ok && res.path) {
                const pathInput = document.getElementById(m.pathId);
                if (pathInput) pathInput.value = res.path;
                if (statusEl) statusEl.textContent = '✓ ' + res.path;
                if (fromPaste) {
                    const ta = document.getElementById(m.paste);
                    if (ta) ta.value = '';
                }
                if (key === 'truststore') this._tlsUpdateTruststorePw();
                this._tlsResetTestGate();
            } else {
                if (statusEl) statusEl.textContent = 'Upload failed';
            }
        } catch (e) {
            if (statusEl) statusEl.textContent = 'Upload error: ' + e.message;
        }
    },

    _readFileText(file) {
        return new Promise((resolve, reject) => {
            const r = new FileReader();
            r.onload = () => resolve(String(r.result || ''));
            r.onerror = () => reject(new Error('could not read file'));
            r.readAsText(file);
        });
    },

    _readFileBase64(file) {
        return new Promise((resolve, reject) => {
            const r = new FileReader();
            r.onload = () => {
                const s = String(r.result || '');
                const i = s.indexOf(',');
                resolve(i >= 0 ? s.slice(i + 1) : s);
            };
            r.onerror = () => reject(new Error('could not read file'));
            r.readAsDataURL(file);
        });
    },

    /**
     * Port → DB-type auto-detection. Runs on every keystroke in either Port field: looks the typed
     * port up in the standard-port table (server-supplied, with a built-in fallback) and, on a hit,
     * pre-selects that DB type plus a hint. The user retains the override because this only fires on
     * PORT input — picking a different type in the select is never undone except by editing the port.
     */
    _suggestDbType(portInputId) {
        const sel = document.getElementById('cfDbType');
        if (!sel) return;
        const hint = document.getElementById('cfDbTypeHint');
        const port = parseInt((document.getElementById(portInputId)?.value || '').trim(), 10);
        const suggested = (port > 0 && this._portDbTypeMap) ? this._portDbTypeMap[String(port)] : null;
        if (!suggested) { if (hint) hint.textContent = ''; return; }
        if (sel.value !== suggested) {
            sel.value = suggested;
            this._onDbTypeChange();
        }
        if (hint) hint.textContent = `Port ${port} suggests ${suggested}`;
    },

    /** The standard port for a DB type from the same table, or null when unknown. */
    _standardPortFor(dbType) {
        const m = this._portDbTypeMap || {};
        for (const p of Object.keys(m)) {
            if (String(m[p]).toUpperCase() === String(dbType).toUpperCase()) return parseInt(p, 10);
        }
        return null;
    },

    /**
     * Database Type select changed: retarget the TLS section (wallet is Oracle-only, truststore is
     * DB2-only), fill a blank port with this type's standard port, and — because the driver/URL just
     * changed — re-arm the test gate.
     */
    _onDbTypeChange() {
        const db = (document.getElementById('cfDbType')?.value || 'ORACLE').toUpperCase();
        this._tlsDbType = db;
        this._tlsApplyDbType();
        const portId = (document.getElementById('cfType')?.value === 'SID') ? 'cfPortSid' : 'cfPort';
        const portEl = document.getElementById(portId);
        if (portEl && !String(portEl.value || '').trim()) {
            const sp = this._standardPortFor(db);
            if (sp) portEl.value = sp;
        }
        this._tlsResetTestGate();
    },

    _onTypeChange() {
        const type = document.getElementById('cfType')?.value || 'SERVICE_NAME';
        ['ServiceName','Sid','Tns','Custom','Ldap'].forEach(t => {
            const el = document.getElementById('cfFields' + t);
            if (el) el.style.display = 'none';
        });
        const map = { SERVICE_NAME: 'ServiceName', SID: 'Sid', TNS: 'Tns', CUSTOM: 'Custom', LDAP: 'Ldap' };
        const target = document.getElementById('cfFields' + (map[type] || 'ServiceName'));
        if (target) target.style.display = 'block';
        this._testPassed = false;
        const saveBtn = document.getElementById('cfSaveBtn');
        if (saveBtn) saveBtn.disabled = true;
        const status = document.getElementById('cfTestStatus');
        if (status) { status.textContent = 'Test required before save'; status.style.color = 'var(--text-muted)'; }
    },

    _buildConnFromForm() {
        const type = document.getElementById('cfType').value;
        const conn = {
            name: document.getElementById('cfConn').value.trim(),
            description: document.getElementById('cfDesc').value.trim(),
            username: document.getElementById('cfUser').value.trim(),
            password: document.getElementById('cfPass').value,
            role: document.getElementById('cfRole').value,
            connectionType: type,
            dbType: document.getElementById('cfDbType')?.value || 'ORACLE'
        };
        if (type === 'SERVICE_NAME') {
            conn.hostname = document.getElementById('cfHost').value.trim();
            conn.port = parseInt(document.getElementById('cfPort').value) || 1521;
            conn.serviceName = document.getElementById('cfSvc').value.trim();
        } else if (type === 'SID') {
            conn.hostname = document.getElementById('cfHostSid').value.trim();
            conn.port = parseInt(document.getElementById('cfPortSid').value) || 1521;
            conn.sid = document.getElementById('cfSid').value.trim();
        } else if (type === 'TNS') {
            conn.tnsAlias = document.getElementById('cfTns').value.trim();
        } else if (type === 'CUSTOM') {
            conn.customUrl = document.getElementById('cfCustomUrl').value.trim();
        } else if (type === 'LDAP') {
            conn.customUrl = document.getElementById('cfLdapUrl').value.trim();
        }
        const schemasRaw = document.getElementById('cfSchemas')?.value || '';
        const schemas = schemasRaw.split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
        if (schemas.length) conn.schemas = schemas;
        // TLS / Security (camelCase keys per backend ConnectionConfig JSON binding)
        const sslMode = document.getElementById('cfSslMode')?.value || 'NONE';
        conn.sslMode = sslMode;
        conn.hostnameValidation = sslMode !== 'NONE' && !!document.getElementById('cfHostnameValidation')?.checked;
        conn.trustCertPath = (document.getElementById('cfTrustPath')?.value || '').trim();
        conn.clientCertPath = (document.getElementById('cfClientCertPath')?.value || '').trim();
        conn.clientKeyPath = (document.getElementById('cfClientKeyPath')?.value || '').trim();
        conn.truststorePath = (document.getElementById('cfTruststorePath')?.value || '').trim();
        conn.truststorePassword = document.getElementById('cfTruststorePassword')?.value || '';
        conn.walletPath = (document.getElementById('cfWalletPath')?.value || '').trim();
        return conn;
    },

    async _testConn() {
        const conn = this._buildConnFromForm();
        const btn = document.getElementById('cfTestBtn');
        const result = document.getElementById('cfTestResult');
        const status = document.getElementById('cfTestStatus');
        const saveBtn = document.getElementById('cfSaveBtn');
        btn.textContent = 'Testing...';
        btn.disabled = true;

        try {
            const res = await PA.api.testConnection(conn);
            if (res.success) {
                this._testPassed = true;
                saveBtn.disabled = false;
                // Certificate expiry rides along on the test result (certExpiresInDays /
                // certExpiryWarning, set server-side when a PEM trust/client cert is configured).
                // Values are numbers/booleans only, so this interpolation is safe.
                let certNote = '';
                if (typeof res.certExpiresInDays === 'number') {
                    const d = res.certExpiresInDays;
                    certNote = res.certExpiryWarning
                        ? ` <span style="color:var(--red)">&#9888; TLS cert expires in ${d} day(s)</span>`
                        : ` <span style="color:var(--text-muted)">TLS cert valid for ${d} more day(s)</span>`;
                }
                result.innerHTML = `<div class="cfg-test-ok">&#10003; ${this._esc(res.message)} — ${this._esc(res.version || '')}${certNote}</div>`;
                status.textContent = 'Test passed — ready to save';
                status.style.color = 'var(--green, var(--green))';
                if (Array.isArray(res.schemas) && res.schemas.length) {
                    const schemasInput = document.getElementById('cfSchemas');
                    if (schemasInput) schemasInput.value = res.schemas.join(', ');
                }
            } else {
                this._testPassed = false;
                saveBtn.disabled = true;
                result.innerHTML = `<div class="cfg-test-fail">&#10007; ${this._esc(res.message || res.error)}</div>`;
                status.textContent = 'Test failed — fix and retry';
                status.style.color = 'var(--red)';
            }
        } catch (e) {
            this._testPassed = false;
            saveBtn.disabled = true;
            result.innerHTML = `<div class="cfg-test-fail">&#10007; ${e.message}</div>`;
            status.textContent = 'Test error';
            status.style.color = 'var(--red)';
        }
        btn.innerHTML = '&#9889; Test Connection';
        btn.disabled = false;
    },

    async _saveConn(isEdit) {
        if (!this._testPassed) { alert('Test must pass before saving.'); return; }
        const conn = this._buildConnFromForm();
        conn.tested = true;
        // Scope is not part of the test payload (the test endpoint takes a ConnectionConfig), so it
        // is attached only here, at save time. Editing keeps the original scope unless the user moved
        // the toggle — `_formScope` was preset from the listing the connection came from.
        conn.scope = this._scopeValue();

        if (!conn.name) { alert('Connection name is required'); return; }
        if (!conn.username) { alert('Username is required'); return; }

        let res;
        if (isEdit) {
            res = await PA.api.updateConnection(this._selectedProject, this._selectedEnv, this._selectedConn, conn, conn.scope);
        } else {
            res = await PA.api.addConnection(this._selectedProject, this._selectedEnv, conn, conn.scope);
        }
        if (res.error) { alert(res.error); return; }

        await this.selectProject(this._selectedProject);
        this.selectEnv(this._selectedEnv);
    },

    /**
     * "Duplicate connection": copy an existing connection's NON-SECRET settings into a fresh New
     * Connection form. Typical use is the same host/service repeated across environments (PROD →
     * UAT), where re-typing everything is the pain point.
     *
     * <p>Secrets are deliberately dropped: `password` and `truststorePassword` are never readable
     * back over HTTP (see HttpSecretMaskingConfig), and a duplicated connection must be tested
     * before it can be saved anyway. TLS cert PATHS are non-secret and are copied, but note they
     * still point into the SOURCE connection's `certs/` directory (uploads are keyed by connection
     * name) — re-upload to retarget them.
     */
    duplicateConn(connName, scopeHint) {
        const project = this._projects.find(p => p.name === this._selectedProject);
        const env = project?.environments?.find(e => e.name === this._selectedEnv);
        let src = env?.connections?.find(c => c.name === connName && (!scopeHint || this._connScope(c) === scopeHint))
            || env?.connections?.find(c => c.name === connName);
        if (!src) return;
        const copy = Object.assign({}, src);
        delete copy.password;
        delete copy.truststorePassword;
        delete copy.lastTestedAt;
        copy.scope = this._connScope(src);
        copy.name = this._uniqueCopyName(src.name, env);
        copy.tested = false;
        this.showConnForm(null, copy);
    },

    /** A `_COPY`-suffixed name that does not already exist in this environment. */
    _uniqueCopyName(base, env) {
        const taken = new Set((env?.connections || []).map(c => (c.name || '').toLowerCase()));
        let name = base + '_COPY';
        let n = 2;
        while (taken.has(name.toLowerCase())) name = base + '_COPY' + (n++);
        return name;
    },

    /**
     * Delete a connection from the scope it lives in. The scope comes from the listing the entry was
     * rendered from; it is asked for only when the connection can't be resolved (ambiguous).
     */
    async deleteConn(connName, scopeHint) {
        const project = this._projects.find(p => p.name === this._selectedProject);
        const env = project?.environments?.find(e => e.name === this._selectedEnv);
        const conn = env?.connections?.find(c => c.name === connName && (!scopeHint || this._connScope(c) === scopeHint));
        let scope = conn ? this._connScope(conn) : null;
        if (!scope) {
            const ans = (prompt('Delete "' + connName + '" from which scope? Type "mine" (private) or "shared" (team):', 'mine') || '').trim().toLowerCase();
            if (!ans) return;
            scope = ans.startsWith('s') ? 'shared' : 'user';
        }
        if (!confirm('Delete connection "' + connName + '" (' + (scope === 'user' ? 'private' : 'team') + ')?')) return;
        await PA.api.deleteConnection(this._selectedProject, this._selectedEnv, connName, scope);
        await this.selectProject(this._selectedProject);
        this.selectEnv(this._selectedEnv);
    },

    showAddProject() {
        const name = prompt('Project name:');
        if (!name) return;
        const desc = prompt('Description (optional):') || '';
        this._doAddProject(name, desc);
    },

    async _doAddProject(name, desc) {
        const res = await PA.api.createProject({ name, description: desc });
        if (res.error) { alert(res.error); return; }
        await this.loadProjects();
        this.selectProject(name);
    },

    async deleteProject(name) {
        if (!confirm('Delete project "' + name + '" and all its environments?')) return;
        await PA.api.deleteProject(name);
        this._selectedProject = null;
        this._selectedEnv = null;
        await this.loadProjects();
    },

    showAddEnv() {
        const name = prompt('Environment name (e.g. PROD, UAT, DEV):');
        if (!name) return;
        const zone = prompt('Zone (optional, e.g. EU-CENTRAL):') || '';
        this._doAddEnv(name, zone);
    },

    async _doAddEnv(name, zone) {
        const res = await PA.api.createEnvironment(this._selectedProject, { name, zone });
        if (res.error) { alert(res.error); return; }
        await this.selectProject(this._selectedProject);
        this.selectEnv(name);
    },

    async deleteEnv(name) {
        if (!confirm('Delete environment "' + name + '" and all connections?')) return;
        await PA.api.deleteEnvironment(this._selectedProject, name);
        this._selectedEnv = null;
        await this.selectProject(this._selectedProject);
    },

    _esc(s) {
        if (!s) return '';
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }
};
