/* ── PA.connFormFieldsMysql ─────────────────────────────────────────────────
   MySQL-specific connection fields.
   Connection style: Host / Port / Database [/ Custom JDBC URL]
────────────────────────────────────────────────────────────────────────── */
window.PA = window.PA || {};

PA.connFormFieldsMysql = (function () {

    function render(c) {
        var useCustom = (c.connectionType === 'CUSTOM');
        var html = '';

        html += '<div class="conn-form-row"><label>Connection Type</label>';
        html += '<select id="cfType" class="form-input conn-form-inp" onchange="PA.connFormFieldsMysql.onTypeChange()">';
        html += '<option value="SERVICE_NAME"' + (!useCustom ? ' selected' : '') + '>Host / Port / Database</option>';
        html += '<option value="CUSTOM"'       + (useCustom  ? ' selected' : '') + '>Custom JDBC URL</option>';
        html += '</select></div>';

        // Standard panel
        html += '<div id="myFieldsStd" class="conn-type-fields"' + (useCustom ? ' style="display:none"' : '') + '>';
        html += '<div class="conn-form-row"><label>Hostname</label><input id="cfHost" value="' + PA.esc(c.hostname || '') + '" class="form-input conn-form-inp" placeholder="e.g. localhost or db.company.com"></div>';
        html += '<div class="conn-form-row"><label>Port</label><input id="cfPort" type="number" value="' + (c.port || 3306) + '" class="form-input conn-form-inp small" placeholder="3306"></div>';
        html += '<div class="conn-form-row"><label>Database</label><input id="cfSvc" value="' + PA.esc(c.serviceName || '') + '" class="form-input conn-form-inp" placeholder="e.g. myapp_db"></div>';
        html += '<div class="conn-form-row"><label>SSL</label>';
        html += '<select id="cfMySsl" class="form-input conn-form-inp small">';
        html += '<option value="false"' + (c.sslMode !== 'true' ? ' selected' : '') + '>Disabled</option>';
        html += '<option value="true"'  + (c.sslMode === 'true' ? ' selected' : '') + '>Enabled</option>';
        html += '</select></div>';
        html += '</div>';

        // Custom JDBC URL panel
        html += '<div id="myFieldsCustom" class="conn-type-fields"' + (!useCustom ? ' style="display:none"' : '') + '>';
        html += '<div class="conn-form-row"><label>JDBC URL</label><input id="cfCustomUrl" value="' + PA.esc(c.customUrl || '') + '" class="form-input conn-form-inp" placeholder="jdbc:mysql://host:3306/db?useSSL=false"></div>';
        html += '</div>';

        return html;
    }

    function onTypeChange() {
        var type = (document.getElementById('cfType') || {}).value || 'SERVICE_NAME';
        var std    = document.getElementById('myFieldsStd');
        var custom = document.getElementById('myFieldsCustom');
        if (std)    std.style.display    = type === 'CUSTOM' ? 'none'  : 'block';
        if (custom) custom.style.display = type === 'CUSTOM' ? 'block' : 'none';
        if (PA.connTest) PA.connTest.resetTestState();
    }

    function collect() {
        var type = (document.getElementById('cfType') || {}).value || 'SERVICE_NAME';
        var conn = { dbType: 'MYSQL', connectionType: type };
        if (type === 'CUSTOM') {
            conn.customUrl = _val('cfCustomUrl');
        } else {
            conn.hostname    = _val('cfHost');
            conn.port        = _int('cfPort', 3306);
            conn.serviceName = _val('cfSvc');   // database name stored in serviceName field
            conn.sslMode     = _val('cfMySsl'); // extra hint for URL builder
        }
        return conn;
    }

    function _val(id) { return ((document.getElementById(id) || {}).value || '').trim(); }
    function _int(id, def) { return parseInt((document.getElementById(id) || {}).value) || def; }

    return { render: render, collect: collect, onTypeChange: onTypeChange, activate: function () {} };
}());
