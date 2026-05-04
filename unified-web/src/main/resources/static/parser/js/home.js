PA.home = (function() {
    var _projects = [];
    var _envs = [];
    var _owners = [];

    async function init() {
        await loadProjects();
    }

    async function loadProjects() {
        try {
            var res = await fetch('/api/plsql/config/projects');
            _projects = res.ok ? await res.json() : [];
        } catch (e) { _projects = []; }

        var sel = document.getElementById('homeProject');
        if (!sel) return;
        var html = '<option value="">Select project</option>';
        for (var i = 0; i < _projects.length; i++) {
            html += '<option value="' + PA.esc(_projects[i].name) + '">' + PA.esc(_projects[i].name) + '</option>';
        }
        sel.innerHTML = html;

        var legacy = _projects.length === 0;
        var legacyLabel = document.getElementById('homeLegacyLabel');
        if (legacyLabel) legacyLabel.style.display = legacy ? 'inline' : 'none';

        _envs = []; _owners = [];
        _renderEnvSelect(); _renderOwnerSelect();
    }

    function onProjectChange() {
        var sel = document.getElementById('homeProject');
        var projName = sel ? sel.value : '';
        _envs = []; _owners = [];
        if (projName) {
            var proj = _projects.find(function(p) { return p.name === projName; });
            if (proj && proj.environments) _envs = proj.environments;
        }
        _renderEnvSelect(); _renderOwnerSelect();
    }

    function _renderEnvSelect() {
        var sel = document.getElementById('homeEnv');
        if (!sel) return;
        var html = '<option value="">Select environment</option>';
        for (var i = 0; i < _envs.length; i++) {
            var env = _envs[i];
            html += '<option value="' + PA.esc(env.name) + '">' + PA.esc(env.name);
            if (env.zone) html += ' (' + PA.esc(env.zone) + ')';
            html += '</option>';
        }
        sel.innerHTML = html;
    }

    function onEnvChange() {
        var projSel = document.getElementById('homeProject');
        var envSel = document.getElementById('homeEnv');
        var projName = projSel ? projSel.value : '';
        var envName = envSel ? envSel.value : '';
        _owners = [];
        if (projName && envName) {
            var proj = _projects.find(function(p) { return p.name === projName; });
            if (proj) {
                var env = (proj.environments || []).find(function(e) { return e.name === envName; });
                if (env && env.connections) {
                    _owners = env.connections.map(function(c) { return c.username; }).filter(Boolean);
                }
            }
        }
        _renderOwnerSelect();
    }

    function _renderOwnerSelect() {
        var sel = document.getElementById('homeOwner');
        if (!sel) return;
        var html = '<option value="">Auto-detect (all schemas)</option>';
        var seen = {};
        for (var i = 0; i < _owners.length; i++) {
            var o = _owners[i].toUpperCase();
            if (seen[o]) continue;
            seen[o] = true;
            html += '<option value="' + PA.esc(o) + '">' + PA.esc(o) + '</option>';
        }
        sel.innerHTML = html;
    }

    function getFormData() {
        return {
            project: (document.getElementById('homeProject') || {}).value || '',
            environment: (document.getElementById('homeEnv') || {}).value || '',
            owner: (document.getElementById('homeOwner') || {}).value || '',
            objectType: (document.getElementById('homeObjType') || {}).value || 'PACKAGE',
            fastMode: !!(document.getElementById('homeFast') || {}).checked,
            entryPoint: (document.getElementById('homeObject') || {}).value || ''
        };
    }

    return {
        init: init, loadProjects: loadProjects,
        onProjectChange: onProjectChange, onEnvChange: onEnvChange,
        getFormData: getFormData
    };
})();
