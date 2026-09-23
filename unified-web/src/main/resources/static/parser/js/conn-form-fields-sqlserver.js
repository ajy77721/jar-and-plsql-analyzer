/* ── PA.connFormFieldsSqlServer ─────────────────────────────────────────────
   SQL Server-specific connection fields.
   Connection style: Host / Port / Database [/ Instance] + Auth type
   Auth: SQL Server auth (user+pass) | Windows Integrated (no creds)
────────────────────────────────────────────────────────────────────────── */
window.PA = window.PA || {};

PA.connFormFieldsSqlServer = (function () {

    function render(c) {
        var useCustom  = (c.connectionType === 'CUSTOM');
        var useWindows = (c.role === 'WINDOWS');
        var html = '';

        html += '<div class="conn-form-row"><label>Connection Type</label>';
        html += '<select id="cfType" class="form-input conn-form-inp" onchange="PA.connFormFieldsSqlServer.onTypeChange()">';
        html += '<option value="SERVICE_NAME"' + (!useCustom ? ' selected' : '') + '>Host / Port / Database</option>';
        html += '<option value="CUSTOM"'       + (useCustom  ? ' selected' : '') + '>Custom JDBC URL</option>';
        html += '</select></div>';

        // Standard panel
        html += '<div id="ssFieldsStd" class="conn-type-fields"' + (useCustom ? ' style="display:none"' : '') + '>';
        html += '<div class="conn-form-row"><label>Hostname</label><input id="cfHost" value="' + PA.esc(c.hostname || '') + '" class="form-input conn-form-inp" placeholder="e.g. sqlserver.company.com"></div>';
        html += '<div class="conn-form-row"><label>Port</label><input id="cfPort" type="number" value="' + (c.port || 1433) + '" class="form-input conn-form-inp small" placeholder="1433"></div>';
        html += '<div class="conn-form-row"><label>Database</label><input id="cfSvc" value="' + PA.esc(c.serviceName || '') + '" class="form-input conn-form-inp" placeholder="e.g. MyDatabase"></div>';
        html += '<div class="conn-form-row"><label>Instance</label><input id="cfSid" value="' + PA.esc(c.sid || '') + '" class="form-input conn-form-inp" placeholder="Optional, e.g. SQLEXPRESS"></div>';
        html += '</div>';

        // Custom JDBC URL panel
        html += '<div id="ssFieldsCustom" class="conn-type-fields"' + (!useCustom ? ' style="display:none"' : '') + '>';
        html += '<div class="conn-form-row"><label>JDBC URL</label><input id="cfCustomUrl" value="' + PA.esc(c.customUrl || '') + '" class="form-input conn-form-inp" placeholder="jdbc:sqlserver://host:1433;databaseName=..."></div>';
        html += '</div>';

        // Auth type note: role field is reused — WINDOWS = integrated security
        html += '<div class="conn-form-row" style="margin-top:4px"><label>Auth</label>';
        html += '<select id="cfSsAuth" class="form-input conn-form-inp" onchange="PA.connFormFieldsSqlServer.onAuthChange()">';
        html += '<option value="DEFAULT"' + (!useWindows ? ' selected' : '') + '>SQL Server Auth (username + password)</option>';
        html += '<option value="WINDOWS"' + (useWindows  ? ' selected' : '') + '>Windows Integrated Auth</option>';
        html += '</select></div>';

        html += '<div id="ssCredNote" class="conn-form-row" style="' + (useWindows ? '' : 'display:none') + '">';
        html += '<label></label><span style="font-size:11px;color:var(--text-muted)">Windows auth uses the OS identity — no username/password needed.</span>';
        html += '</div>';

        return html;
    }

    function onTypeChange() {
        var type   = (document.getElementById('cfType')   || {}).value || 'SERVICE_NAME';
        var std    = document.getElementById('ssFieldsStd');
        var custom = document.getElementById('ssFieldsCustom');
        if (std)    std.style.display    = type === 'CUSTOM' ? 'none'  : 'block';
        if (custom) custom.style.display = type === 'CUSTOM' ? 'block' : 'none';
        if (PA.connTest) PA.connTest.resetTestState();
    }

    function onAuthChange() {
        var auth = (document.getElementById('cfSsAuth') || {}).value || 'DEFAULT';
        var note = document.getElementById('ssCredNote');
        if (note) note.style.display = auth === 'WINDOWS' ? 'flex' : 'none';
        // Disable username/password inputs when Windows auth selected
        var userEl = document.getElementById('cfUser');
        var passEl = document.getElementById('cfPass');
        var isWindows = auth === 'WINDOWS';
        if (userEl) { userEl.disabled = isWindows; if (isWindows) userEl.value = ''; }
        if (passEl) { passEl.disabled = isWindows; if (isWindows) passEl.value = ''; }
        if (PA.connTest) PA.connTest.resetTestState();
    }

    function collect() {
        var type = (document.getElementById('cfType')   || {}).value || 'SERVICE_NAME';
        var auth = (document.getElementById('cfSsAuth') || {}).value || 'DEFAULT';
        var conn = { dbType: 'SQLSERVER', connectionType: type, role: auth };
        if (type === 'CUSTOM') {
            conn.customUrl = _val('cfCustomUrl');
        } else {
            conn.hostname    = _val('cfHost');
            conn.port        = _int('cfPort', 1433);
            conn.serviceName = _val('cfSvc');  // database name
            conn.sid         = _val('cfSid');  // instance name
        }
        return conn;
    }

    function _val(id) { return ((document.getElementById(id) || {}).value || '').trim(); }
    function _int(id, def) { return parseInt((document.getElementById(id) || {}).value) || def; }

    return { render: render, collect: collect, onTypeChange: onTypeChange, onAuthChange: onAuthChange, activate: function () {} };
}());
