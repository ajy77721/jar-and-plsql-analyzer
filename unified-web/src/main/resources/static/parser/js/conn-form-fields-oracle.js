/* ── PA.connFormFieldsOracle ────────────────────────────────────────────────
   Oracle-specific connection fields.
   Connection types: SERVICE_NAME | SID | TNS | CUSTOM
────────────────────────────────────────────────────────────────────────── */
window.PA = window.PA || {};

PA.connFormFieldsOracle = (function () {

    function render(c) {
        var connType = c.connectionType || 'SERVICE_NAME';
        var html = '';

        html += '<div class="conn-form-row">';
        html += '<label>Connection Type</label>';
        html += '<select id="cfType" class="form-input conn-form-inp" onchange="PA.connFormFieldsOracle.onTypeChange()">';
        html += '<option value="SERVICE_NAME"' + (connType === 'SERVICE_NAME' ? ' selected' : '') + '>Service Name</option>';
        html += '<option value="SID"'          + (connType === 'SID'          ? ' selected' : '') + '>SID</option>';
        html += '<option value="TNS"'          + (connType === 'TNS'          ? ' selected' : '') + '>TNS</option>';
        html += '<option value="CUSTOM"'       + (connType === 'CUSTOM'       ? ' selected' : '') + '>Custom JDBC URL</option>';
        html += '</select></div>';

        // SERVICE_NAME panel
        html += '<div id="oraFieldsSvcName" class="conn-type-fields">';
        html += '<div class="conn-form-row"><label>Hostname</label><input id="cfHost" value="' + PA.esc(c.hostname || '') + '" class="form-input conn-form-inp" placeholder="db.company.com"></div>';
        html += '<div class="conn-form-row"><label>Port</label><input id="cfPort" type="number" value="' + (c.port || 1521) + '" class="form-input conn-form-inp small"></div>';
        html += '<div class="conn-form-row"><label>Service Name</label><input id="cfSvc" value="' + PA.esc(c.serviceName || '') + '" class="form-input conn-form-inp" placeholder="e.g. ORCL, AGICPROD"></div>';
        html += '</div>';

        // SID panel
        html += '<div id="oraFieldsSid" class="conn-type-fields" style="display:none">';
        html += '<div class="conn-form-row"><label>Hostname</label><input id="cfHostSid" value="' + PA.esc(c.hostname || '') + '" class="form-input conn-form-inp"></div>';
        html += '<div class="conn-form-row"><label>Port</label><input id="cfPortSid" type="number" value="' + (c.port || 1521) + '" class="form-input conn-form-inp small"></div>';
        html += '<div class="conn-form-row"><label>SID</label><input id="cfSid" value="' + PA.esc(c.sid || '') + '" class="form-input conn-form-inp" placeholder="e.g. ORCL"></div>';
        html += '</div>';

        // TNS panel
        html += '<div id="oraFieldsTns" class="conn-type-fields" style="display:none">';
        html += '<div class="conn-form-row"><label>TNS Alias</label><input id="cfTns" value="' + PA.esc(c.tnsAlias || '') + '" class="form-input conn-form-inp" placeholder="e.g. MYDB_PROD"></div>';
        html += '</div>';

        // Custom panel
        html += '<div id="oraFieldsCustom" class="conn-type-fields" style="display:none">';
        html += '<div class="conn-form-row"><label>JDBC URL</label><input id="cfCustomUrl" value="' + PA.esc(c.customUrl || '') + '" class="form-input conn-form-inp" placeholder="jdbc:oracle:thin:@//..."></div>';
        html += '</div>';

        return html;
    }

    function onTypeChange() {
        var type = (document.getElementById('cfType') || {}).value || 'SERVICE_NAME';
        var panels = {
            SERVICE_NAME: 'oraFieldsSvcName',
            SID:          'oraFieldsSid',
            TNS:          'oraFieldsTns',
            CUSTOM:       'oraFieldsCustom'
        };
        Object.keys(panels).forEach(function (k) {
            var el = document.getElementById(panels[k]);
            if (el) el.style.display = (k === type) ? 'block' : 'none';
        });
        // Signal that form changed and re-test is required
        if (PA.connTest) PA.connTest.resetTestState();
    }

    function collect() {
        var type = (document.getElementById('cfType') || {}).value || 'SERVICE_NAME';
        var conn = { connectionType: type, dbType: 'ORACLE' };

        if (type === 'SERVICE_NAME') {
            conn.hostname    = _val('cfHost');
            conn.port        = _int('cfPort', 1521);
            conn.serviceName = _val('cfSvc');
        } else if (type === 'SID') {
            conn.hostname = _val('cfHostSid');
            conn.port     = _int('cfPortSid', 1521);
            conn.sid      = _val('cfSid');
        } else if (type === 'TNS') {
            conn.tnsAlias = _val('cfTns');
        } else if (type === 'CUSTOM') {
            conn.customUrl = _val('cfCustomUrl');
        }
        return conn;
    }

    function activateCorrectPanel() {
        onTypeChange();
    }

    function _val(id) {
        return ((document.getElementById(id) || {}).value || '').trim();
    }
    function _int(id, def) {
        return parseInt((document.getElementById(id) || {}).value) || def;
    }

    return { render: render, collect: collect, onTypeChange: onTypeChange, activate: activateCorrectPanel };
}());
