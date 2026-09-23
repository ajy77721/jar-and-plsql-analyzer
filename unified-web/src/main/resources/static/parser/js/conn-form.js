/* ── PA.connForm ────────────────────────────────────────────────────────────
   Connection form orchestrator.
   Composes: PA.dbSelector + auth section + dialect field module.

   Public API:
     PA.connForm.render(containerEl, existingConn, isEdit)
       — renders full form into containerEl, returns void
     PA.connForm.build()
       — reads all form values; returns merged connection object
     PA.connForm.currentDbType()
       — returns 'ORACLE' | 'MYSQL' | 'SQLSERVER'
────────────────────────────────────────────────────────────────────────── */
window.PA = window.PA || {};

PA.connForm = (function () {

    var _isEdit = false;

    var _FIELD_MODULES = {
        ORACLE:    function () { return PA.connFormFieldsOracle; },
        MYSQL:     function () { return PA.connFormFieldsMysql; },
        SQLSERVER: function () { return PA.connFormFieldsSqlServer; },
        DB2:       function () { return PA.connFormFieldsDb2; },
        POSTGRES:  function () { return PA.connFormFieldsPostgres; }
    };

    function _getFieldModule() {
        var db = PA.dbSelector.get();
        var factory = _FIELD_MODULES[db] || _FIELD_MODULES['ORACLE'];
        return factory();
    }

    // Role options depend on DB type
    function _roleOptions(dbType, currentRole) {
        var html = '';
        if (dbType === 'ORACLE') {
            html += '<option value="DEFAULT"' + (currentRole === 'DEFAULT' || !currentRole ? ' selected' : '') + '>Default</option>';
            html += '<option value="SYSDBA"'  + (currentRole === 'SYSDBA'  ? ' selected' : '') + '>SYSDBA</option>';
            html += '<option value="SYSOPER"' + (currentRole === 'SYSOPER' ? ' selected' : '') + '>SYSOPER</option>';
        }
        // MySQL and SQL Server handle auth/role inside their own field modules; this select is Oracle-only
        return html;
    }

    function _buildAuthSection(c, dbType) {
        var html = '<div class="conn-form-section-title">Authentication</div>';
        html += '<div class="conn-form-row"><label>Username</label>';
        html += '<input id="cfUser" value="' + PA.esc(c.username || '') + '" class="form-input conn-form-inp"';
        if (dbType === 'SQLSERVER' && c.role === 'WINDOWS') html += ' disabled';
        html += '></div>';
        html += '<div class="conn-form-row"><label>Password</label>';
        html += '<input id="cfPass" type="password" value="' + PA.esc(c.password || '') + '" class="form-input conn-form-inp"';
        if (dbType === 'SQLSERVER' && c.role === 'WINDOWS') html += ' disabled';
        html += '></div>';
        if (dbType === 'ORACLE') {
            html += '<div class="conn-form-row"><label>Role</label>';
            html += '<select id="cfRole" class="form-input conn-form-inp small">';
            html += _roleOptions(dbType, c.role);
            html += '</select></div>';
        }
        return html;
    }

    function _renderFields(dbType, c) {
        var mod = _FIELD_MODULES[dbType] ? _FIELD_MODULES[dbType]() : PA.connFormFieldsOracle;
        return mod.render(c);
    }

    function render(containerEl, c, isEdit, lockDb) {
        _isEdit = !!isEdit;
        c = c || { connectionType: 'SERVICE_NAME', role: 'DEFAULT', port: 0 };
        var dbType = (c.dbType || 'ORACLE').toUpperCase();

        // ── db selector placeholder (locked to the project's datasource when lockDb is set)
        var selectorDiv = document.createElement('div');
        selectorDiv.id = 'connFormDbSelectorWrap';
        containerEl.innerHTML = '';
        containerEl.appendChild(selectorDiv);
        PA.dbSelector.render(selectorDiv, dbType, lockDb);

        // ── form body
        var bodyDiv = document.createElement('div');
        bodyDiv.className = 'conn-form-wrap';
        bodyDiv.id = 'connFormBody';

        var inner = document.createElement('div');
        inner.className = 'conn-form-body';
        inner.id = 'connFormInner';
        inner.innerHTML = _buildBody(dbType, c, isEdit);
        bodyDiv.appendChild(inner);
        containerEl.appendChild(bodyDiv);

        // ── DB selector change → re-render fields section only
        PA.dbSelector.onChange(function (newDb) {
            var freshConn = { connectionType: 'SERVICE_NAME', role: 'DEFAULT', port: 0 };
            var fieldsEl = document.getElementById('connFormFields');
            if (fieldsEl) {
                fieldsEl.innerHTML = '<div class="conn-form-section-title">Connection Details</div>'
                    + _renderFields(newDb, freshConn);
                var mod = _FIELD_MODULES[newDb] ? _FIELD_MODULES[newDb]() : PA.connFormFieldsOracle;
                if (mod.activate) mod.activate();
            }
            // Oracle shows role row; others don't
            var roleRow = document.getElementById('connFormRoleRow');
            if (roleRow) roleRow.style.display = newDb === 'ORACLE' ? 'flex' : 'none';
            if (PA.connTest) PA.connTest.resetTestState();
        });

        // Activate correct sub-panels for the initial DB type
        var mod = _FIELD_MODULES[dbType] ? _FIELD_MODULES[dbType]() : PA.connFormFieldsOracle;
        if (mod.activate) mod.activate();
    }

    function _buildBody(dbType, c, isEdit) {
        var html = '';

        // Basics
        html += '<div class="conn-form-section-title">Connection Name</div>';
        html += '<div class="conn-form-row">';
        html += '<label>Name</label>';
        html += '<input id="cfConn" value="' + PA.esc(c.name || '') + '" class="form-input conn-form-inp"'
              + (isEdit ? ' readonly style="opacity:0.55"' : '') + ' placeholder="e.g. CORE_PROD">';
        html += '</div>';
        html += '<div class="conn-form-row"><label>Description</label>';
        html += '<input id="cfDesc" value="' + PA.esc(c.description || '') + '" class="form-input conn-form-inp" placeholder="Optional"></div>';

        // Auth section
        html += _buildAuthSection(c, dbType);

        // DB-type specific fields
        html += '<div id="connFormFields">';
        html += '<div class="conn-form-section-title">Connection Details</div>';
        html += _renderFields(dbType, c);
        html += '</div>';

        return html;
    }

    function build() {
        var dbType = PA.dbSelector.get();
        var mod    = _FIELD_MODULES[dbType] ? _FIELD_MODULES[dbType]() : PA.connFormFieldsOracle;

        var conn = {
            name:        _val('cfConn'),
            description: _val('cfDesc'),
            username:    _val('cfUser'),
            password:    (document.getElementById('cfPass') || {}).value || '',
            dbType:      dbType
        };

        // Merge dialect-specific fields
        var dialectFields = mod.collect();
        Object.assign(conn, dialectFields);

        // Oracle role (overridden by SQL Server auth via its module)
        if (dbType === 'ORACLE') {
            conn.role = _val('cfRole') || 'DEFAULT';
        }

        return conn;
    }

    function currentDbType() {
        return PA.dbSelector.get();
    }

    function _val(id) { return ((document.getElementById(id) || {}).value || '').trim(); }

    return { render: render, build: build, currentDbType: currentDbType };
}());
