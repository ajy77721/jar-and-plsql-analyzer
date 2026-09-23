/* ── PA.connTest ────────────────────────────────────────────────────────────
   Test-connection + save button logic.
   Decoupled from the form rendering — works with any DB type.

   Public API:
     PA.connTest.init(opts)          — set up with { apiBase, onSave }
     PA.connTest.render(el)          — inject action bar HTML into el
     PA.connTest.resetTestState()    — called when form fields change
────────────────────────────────────────────────────────────────────────── */
window.PA = window.PA || {};

PA.connTest = (function () {

    var _apiBase  = '/api/plsql/config';
    var _onSave   = null;
    var _passed   = false;
    var _isEdit   = false;
    var _selectedProject = null;
    var _selectedEnv     = null;
    var _selectedConn    = null;

    function init(opts) {
        _apiBase         = (opts && opts.apiBase)  || '/api/plsql/config';
        _onSave          = (opts && opts.onSave)   || null;
        _isEdit          = !!(opts && opts.isEdit);
        _selectedProject = (opts && opts.project)  || null;
        _selectedEnv     = (opts && opts.env)      || null;
        _selectedConn    = (opts && opts.connName) || null;
        _passed          = false;
    }

    function render(el) {
        el.innerHTML = _actionBarHtml();
    }

    function _actionBarHtml() {
        return '<div class="conn-form-actions">'
            + '<button type="button" class="btn btn-sm" id="cfTestBtn" onclick="PA.connTest._doTest()" style="color:var(--green)">&#9889; Test Connection</button>'
            + '<button type="button" class="btn btn-primary btn-sm" id="cfSaveBtn" onclick="PA.connTest._doSave()" disabled>Save</button>'
            + '<span class="conn-form-status" id="cfStatusMsg">Test required before save</span>'
            + '</div>'
            + '<div id="cfTestResult"></div>';
    }

    function resetTestState() {
        _passed = false;
        var saveBtn = document.getElementById('cfSaveBtn');
        if (saveBtn) saveBtn.disabled = true;
        var status = document.getElementById('cfStatusMsg');
        if (status) { status.textContent = 'Test required before save'; status.className = 'conn-form-status'; }
        var result = document.getElementById('cfTestResult');
        if (result) result.innerHTML = '';
    }

    async function _doTest() {
        var conn = PA.connForm.build();
        var btn  = document.getElementById('cfTestBtn');
        var resultEl = document.getElementById('cfTestResult');
        var statusEl = document.getElementById('cfStatusMsg');
        var saveBtn  = document.getElementById('cfSaveBtn');

        btn.textContent = 'Testing…'; btn.disabled = true;
        resetTestState();

        try {
            var res  = await fetch(_apiBase + '/test-connection', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(conn)
            });
            if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
            var data = await res.json();
            if (data.success) {
                _passed = true;
                saveBtn.disabled = false;
                resultEl.innerHTML = '<div class="conn-test-result ok">&#10003; '
                    + PA.esc(data.message) + (data.version ? ' — ' + PA.esc(data.version) : '') + '</div>';
                statusEl.textContent = 'Test passed — ready to save';
                statusEl.className   = 'conn-form-status ok';
            } else {
                _passed = false; saveBtn.disabled = true;
                resultEl.innerHTML = '<div class="conn-test-result fail">&#10007; ' + PA.esc(data.message || data.error || 'Unknown error') + '</div>';
                statusEl.textContent = 'Test failed — fix and retry';
                statusEl.className   = 'conn-form-status fail';
            }
        } catch (e) {
            _passed = false; saveBtn.disabled = true;
            resultEl.innerHTML = '<div class="conn-test-result fail">&#10007; ' + PA.esc(e.message) + '</div>';
            statusEl.textContent = 'Test error'; statusEl.className = 'conn-form-status fail';
        }
        btn.innerHTML = '&#9889; Test Connection'; btn.disabled = false;
    }

    async function _doSave() {
        if (!_passed) { PA.toast && PA.toast('Run Test first', 'error'); return; }
        var conn = PA.connForm.build();
        conn.tested = true;

        if (!conn.name)     { PA.toast && PA.toast('Connection name is required', 'error'); return; }
        if (!conn.username && conn.role !== 'WINDOWS') {
            PA.toast && PA.toast('Username is required', 'error'); return;
        }

        var base = _apiBase + '/projects/' + encodeURIComponent(_selectedProject)
                 + '/environments/' + encodeURIComponent(_selectedEnv) + '/connections';
        try {
            var method = _isEdit ? 'PUT' : 'POST';
            var url    = _isEdit ? base + '/' + encodeURIComponent(_selectedConn) : base;
            var res    = await fetch(url, { method: method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(conn) });
            if (!res.ok) throw new Error(await res.text() || res.status + ' from ' + res.url);
            var data   = await res.json();
            if (data.error) { PA.toast && PA.toast(data.error, 'error'); return; }
            PA.toast && PA.toast('Connection saved', 'success');
            if (_onSave) _onSave(data);
        } catch (e) { PA.toast && PA.toast('Save failed: ' + e.message, 'error'); }
    }

    return { init: init, render: render, resetTestState: resetTestState, _doTest: _doTest, _doSave: _doSave };
}());
