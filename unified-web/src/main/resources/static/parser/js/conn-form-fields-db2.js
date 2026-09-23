/* ── PA.connFormFieldsDb2 ───────────────────────────────────────────────────
   IBM Db2-specific connection fields.
   Connection style: Host / Port / Database [/ Custom JDBC URL]
────────────────────────────────────────────────────────────────────────── */
window.PA = window.PA || {};

PA.connFormFieldsDb2 = (function () {

    function render(c) {
        var useCustom = (c.connectionType === 'CUSTOM');
        var html = '';

        html += '<div class="conn-form-row"><label>Connection Type</label>';
        html += '<select id="cfType" class="form-input conn-form-inp" onchange="PA.connFormFieldsDb2.onTypeChange()">';
        html += '<option value="SERVICE_NAME"' + (!useCustom ? ' selected' : '') + '>Host / Port / Database</option>';
        html += '<option value="CUSTOM"'       + (useCustom  ? ' selected' : '') + '>Custom JDBC URL</option>';
        html += '</select></div>';

        // Standard panel
        html += '<div id="db2FieldsStd" class="conn-type-fields"' + (useCustom ? ' style="display:none"' : '') + '>';
        html += '<div class="conn-form-row"><label>Hostname</label><input id="cfHost" value="' + PA.esc(c.hostname || '') + '" class="form-input conn-form-inp" placeholder="e.g. db2.company.com"></div>';
        html += '<div class="conn-form-row"><label>Port</label><input id="cfPort" type="number" value="' + (c.port || 50000) + '" class="form-input conn-form-inp small" placeholder="50000"></div>';
        html += '<div class="conn-form-row"><label>Database</label><input id="cfSvc" value="' + PA.esc(c.serviceName || '') + '" class="form-input conn-form-inp" placeholder="e.g. SAMPLE"></div>';
        html += '</div>';

        // Custom JDBC URL panel
        html += '<div id="db2FieldsCustom" class="conn-type-fields"' + (!useCustom ? ' style="display:none"' : '') + '>';
        html += '<div class="conn-form-row"><label>JDBC URL</label><input id="cfCustomUrl" value="' + PA.esc(c.customUrl || '') + '" class="form-input conn-form-inp" placeholder="jdbc:db2://host:50000/DBNAME"></div>';
        html += '</div>';

        return html;
    }

    function onTypeChange() {
        var type = (document.getElementById('cfType') || {}).value || 'SERVICE_NAME';
        var std    = document.getElementById('db2FieldsStd');
        var custom = document.getElementById('db2FieldsCustom');
        if (std)    std.style.display    = type === 'CUSTOM' ? 'none'  : 'block';
        if (custom) custom.style.display = type === 'CUSTOM' ? 'block' : 'none';
        if (PA.connTest) PA.connTest.resetTestState();
    }

    function collect() {
        var type = (document.getElementById('cfType') || {}).value || 'SERVICE_NAME';
        var conn = { dbType: 'DB2', connectionType: type };
        if (type === 'CUSTOM') {
            conn.customUrl = _val('cfCustomUrl');
        } else {
            conn.hostname    = _val('cfHost');
            conn.port        = _int('cfPort', 50000);
            conn.serviceName = _val('cfSvc');   // database name stored in serviceName field
        }
        return conn;
    }

    function _val(id) { return ((document.getElementById(id) || {}).value || '').trim(); }
    function _int(id, def) { return parseInt((document.getElementById(id) || {}).value) || def; }

    return { render: render, collect: collect, onTypeChange: onTypeChange, activate: function () {} };
}());
