window.PA = window.PA || {};

PA.esc = function(s) { return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); };
PA.escAttr = function(s) { return (s || '').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;'); };
PA.escJs = function(s) { return (s || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'"); };

PA.toast = function(msg, type) {
    var el = document.getElementById('toast');
    if (!el) return;
    el.textContent = msg;
    el.className = 'toast visible ' + (type || 'success');
    setTimeout(function() { el.className = 'toast'; }, 4000);
};

PA.getSchemaColor = function(schema) {
    var SCHEMA_COLORS = { 'DEFAULT': { bg: '#f1f5f9', fg: '#475569' } };
    if (!schema) return SCHEMA_COLORS.DEFAULT;
    var upper = schema.toUpperCase();
    if (SCHEMA_COLORS[upper]) return SCHEMA_COLORS[upper];

    var palettes = [
        { bg: '#dbeafe', fg: '#1d4ed8' },
        { bg: '#ccfbf1', fg: '#0f766e' },
        { bg: '#dcfce7', fg: '#15803d' },
        { bg: '#f3e8ff', fg: '#7e22ce' },
        { bg: '#fee2e2', fg: '#b91c1c' },
        { bg: '#fef3c7', fg: '#a16207' },
        { bg: '#e0e7ff', fg: '#3730a3' },
        { bg: '#fce7f3', fg: '#9d174d' }
    ];
    var hash = 0;
    for (var c = 0; c < upper.length; c++) {
        hash = ((hash << 5) - hash + upper.charCodeAt(c)) | 0;
    }
    return palettes[Math.abs(hash) % palettes.length];
};

PA.formatDuration = function(ms) {
    if (!ms || ms <= 0) return '';
    if (ms < 1000) return ms + 'ms';
    var totalSecs = Math.floor(ms / 1000);
    var remMs = ms % 1000;
    if (totalSecs < 60) return totalSecs + 's ' + remMs + 'ms';
    var totalMins = Math.floor(totalSecs / 60);
    var secs = totalSecs % 60;
    if (totalMins < 60) return totalMins + 'm ' + secs + 's';
    var totalHours = Math.floor(totalMins / 60);
    var mins = totalMins % 60;
    if (totalHours < 24) return totalHours + 'h ' + mins + 'm ' + secs + 's';
    var days = Math.floor(totalHours / 24);
    var hours = totalHours % 24;
    return days + 'd ' + hours + 'h ' + mins + 'm ' + secs + 's';
};

PA.formatDurationShort = function(ms) {
    if (!ms || ms <= 0) return '';
    if (ms < 1000) return ms + 'ms';
    var secs = ms / 1000;
    if (secs < 60) return secs.toFixed(1) + 's';
    var mins = Math.floor(secs / 60);
    var remSecs = Math.round(secs % 60);
    if (mins < 60) return mins + 'm ' + remSecs + 's';
    var hours = Math.floor(mins / 60);
    mins = mins % 60;
    return hours + 'h ' + mins + 'm';
};

PA.context = {
    procId: null,
    procDetail: null,
    scopedTables: [],
    callTreeNodeIds: new Set()
};

PA._collectTreeNodeIds = function(tree, set) {
    if (!tree) return set || new Set();
    if (!set) set = new Set();
    var id = (tree.id || tree.nodeId || '').toUpperCase();
    if (id) set.add(id);
    if (tree.children && !tree.circular) {
        for (var i = 0; i < tree.children.length; i++) {
            PA._collectTreeNodeIds(tree.children[i], set);
        }
    }
    return set;
};
