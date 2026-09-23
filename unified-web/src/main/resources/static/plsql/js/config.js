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
        await this.loadProjects();
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
        if (!this._projects.length) {
            el.innerHTML = '<div class="cfg-empty">No projects. Click "+ Add Project".</div>';
            document.getElementById('cfgEnvPanel').innerHTML = '';
            document.getElementById('cfgDetailPanel').innerHTML = '';
            return;
        }
        el.innerHTML = this._projects.map(p => `
            <div class="cfg-item ${this._selectedProject === p.name ? 'active' : ''}"
                 onclick="PA.config.selectProject('${this._esc(p.name)}')">
                <div class="cfg-item-name">${this._esc(p.name)}</div>
                <div class="cfg-item-desc">${this._esc(p.description || '')}</div>
                <div class="cfg-item-meta">${(p.environments || []).length} env(s)</div>
                <button class="cfg-del" onclick="event.stopPropagation();PA.config.deleteProject('${this._esc(p.name)}')" title="Delete">&#10005;</button>
            </div>
        `).join('');
    },

    async selectProject(name) {
        this._selectedProject = name;
        this._selectedEnv = null;
        this._selectedConn = null;
        this._renderProjects();
        const envs = await PA.api.listEnvironments(name);
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
        const env = project?.environments?.find(e => e.name === name);
        if (env) this._renderConnections(env);
    },

    _renderConnections(env) {
        const el = document.getElementById('cfgDetailPanel');
        let html = `<div class="cfg-section-head">
            <span>${this._esc(env.name)} — Connections</span>
            <button class="btn btn-sm btn-primary" onclick="PA.config.showConnForm(null)">+ New Connection</button>
        </div>`;
        if (!env.connections || !env.connections.length) {
            html += '<div class="cfg-empty">No connections. Click "+ New Connection" to add one (like SQL Developer).</div>';
        } else {
            html += '<div class="cfg-conn-list">' + env.connections.map(c => {
                const url = c.jdbcUrl || (c.hostname ? c.hostname + ':' + (c.port||1521) + '/' + (c.serviceName||c.sid||'') : 'N/A');
                return `
                <div class="cfg-conn-item" onclick="PA.config.showConnForm('${this._esc(c.name)}')">
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
                        <button class="cfg-del" onclick="event.stopPropagation();PA.config.deleteConn('${this._esc(c.name)}')" title="Delete">&#10005;</button>
                    </div>
                </div>`;
            }).join('') + '</div>';
        }
        el.innerHTML = html;
    },

    showConnForm(connName) {
        this._selectedConn = connName;
        this._testPassed = false;
        const project = this._projects.find(p => p.name === this._selectedProject);
        const env = project?.environments?.find(e => e.name === this._selectedEnv);
        const existing = connName ? env?.connections?.find(c => c.name === connName) : null;

        const el = document.getElementById('cfgDetailPanel');
        const c = existing || { connectionType: 'SERVICE_NAME', role: 'DEFAULT', port: 1521 };
        const isEdit = !!existing;

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
                    <input id="cfPort" type="number" value="${c.port || 1521}" class="form-input cfg-inp" style="width:100px"></div>
                <div class="cfg-row"><label>Service Name</label>
                    <input id="cfSvc" value="${this._esc(c.serviceName || '')}" class="form-input cfg-inp" placeholder="e.g. ORCL"></div>
            </div>
            <div id="cfFieldsSid" class="cfg-type-fields" style="display:none">
                <div class="cfg-row"><label>Hostname</label>
                    <input id="cfHostSid" value="${this._esc(c.hostname || '')}" class="form-input cfg-inp"></div>
                <div class="cfg-row"><label>Port</label>
                    <input id="cfPortSid" type="number" value="${c.port || 1521}" class="form-input cfg-inp" style="width:100px"></div>
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
        this._tlsDbType = ((c && c.dbType) || 'ORACLE').toUpperCase();
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
            connectionType: type
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
                result.innerHTML = `<div class="cfg-test-ok">&#10003; ${this._esc(res.message)} — ${this._esc(res.version || '')}</div>`;
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

        if (!conn.name) { alert('Connection name is required'); return; }
        if (!conn.username) { alert('Username is required'); return; }

        let res;
        if (isEdit) {
            res = await PA.api.updateConnection(this._selectedProject, this._selectedEnv, this._selectedConn, conn);
        } else {
            res = await PA.api.addConnection(this._selectedProject, this._selectedEnv, conn);
        }
        if (res.error) { alert(res.error); return; }

        await this.selectProject(this._selectedProject);
        this.selectEnv(this._selectedEnv);
    },

    async deleteConn(connName) {
        if (!confirm('Delete connection "' + connName + '"?')) return;
        await PA.api.deleteConnection(this._selectedProject, this._selectedEnv, connName);
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
