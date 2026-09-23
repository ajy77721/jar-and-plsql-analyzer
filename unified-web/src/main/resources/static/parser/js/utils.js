window.PA = window.PA || {};

// Delegated to shared/escape.js. These used to be three separate implementations, none of which
// escaped both quote characters — and 72 call sites interpolate them inside quoted attributes,
// where a stray `"` closes the attribute early. Kept as names because ~500 call sites use them;
// the behaviour, not the API, is what needed fixing.
PA.esc = function(s) { return Esc.html(s); };
PA.escAttr = function(s) { return Esc.attr(s); };
PA.escJs = function(s) { return Esc.js(s); };

PA.toast = function(msg, type) {
    var el = document.getElementById('toast');
    if (!el) return;
    el.textContent = msg;
    el.className = 'toast visible ' + (type || 'success');
    setTimeout(function() { el.className = 'toast'; }, 4000);
};

PA.getSchemaColor = function(schema) {
    var SCHEMA_COLORS = { 'DEFAULT': { bg: 'var(--bg)', fg: 'var(--text-secondary)' } };
    if (!schema) return SCHEMA_COLORS.DEFAULT;
    var upper = schema.toUpperCase();
    if (SCHEMA_COLORS[upper]) return SCHEMA_COLORS[upper];

    var palettes = [
        { bg: 'var(--badge-blue-bg)', fg: 'var(--badge-blue)' },
        { bg: 'var(--badge-teal-bg)', fg: 'var(--badge-teal)' },
        { bg: 'var(--badge-green-bg)', fg: 'var(--badge-green)' },
        { bg: 'var(--accent-subtle)', fg: 'var(--accent)' },
        { bg: 'var(--badge-red-bg)', fg: 'var(--badge-red)' },
        { bg: 'var(--badge-orange-bg)', fg: 'var(--badge-orange)' },
        { bg: 'var(--badge-yellow-bg)', fg: 'var(--badge-yellow)' },
        { bg: 'var(--badge-purple-bg)', fg: 'var(--badge-purple)' }
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

/**
 * UI-024 (§162) — async now: awaits `PA.nodeIndex.ensureLoaded()` before reading. §161 declined to
 * convert this on the assumption it had dozens of synchronous callers; traced for real this time —
 * its only real callers are `PA._collectTreeNodeIds` (which now always receives a pre-resolved
 * `idMap`, never builds one internally — see that function's own updated doc comment) and
 * `app.js`'s own `PA.showProcedure`, already `async`. No cascade beyond those.
 */
// Builds underscore→dollar mapping from package index nodes (detailFile encodes dollar id)
PA._buildNodeIdMap = async function() {
    var map = {};
    (await PA.nodeIndex.ensureLoaded()).forEach(function(n) {
        var underscoreId = (n.nodeId || '').toUpperCase();
        var detailFile = n.detailFile || '';
        // detailFile: "nodes/CUSTOMER$PKG$PROC.json" → dollar id = "CUSTOMER$PKG$PROC"
        var match = detailFile.match(/nodes\/(.+)\.json$/);
        if (match) {
            var dollarId = match[1].toUpperCase();
            if (dollarId !== underscoreId) map[underscoreId] = dollarId;
        }
    });
    return map;
};

/**
 * UI-024 (§162) — stays synchronous. `idMap` is only ever computed once, at the top of the
 * recursion, then threaded through every recursive call unchanged — so the async half
 * (`PA._buildNodeIdMap`) only has to happen once, at the caller, not on every node. The old
 * `if (idMap === undefined) idMap = PA._buildNodeIdMap()` fallback is gone: `_buildNodeIdMap` is
 * async now, and silently treating its Promise as a map would look up nothing and add no dollar-id
 * equivalents — a plausible-looking wrong answer, not a crash. Both real top-level callers
 * (`app.js`, ×2) now `await PA._buildNodeIdMap()` first and pass the result in explicitly; a
 * missing `idMap` here is a caller bug, not a runtime case to paper over, so it defaults to an
 * empty map and warns instead of guessing.
 */
PA._collectTreeNodeIds = function(tree, set, idMap) {
    if (!tree) return set || new Set();
    if (!set) set = new Set();
    if (idMap === undefined) {
        console.warn('[PA] _collectTreeNodeIds called without a resolved idMap — dollar-id '
            + 'equivalents will be skipped for this call. Callers must await PA._buildNodeIdMap() '
            + 'first.');
        idMap = {};
    }
    var rawId = (tree.id || tree.nodeId || '').toUpperCase();
    if (rawId) {
        set.add(rawId);
        // Also add the dollar-format equivalent for package analyses
        var dollId = idMap[rawId];
        if (dollId) set.add(dollId);
    }
    if (tree.children && !tree.circular) {
        for (var i = 0; i < tree.children.length; i++) {
            PA._collectTreeNodeIds(tree.children[i], set, idMap);
        }
    }
    return set;
};
