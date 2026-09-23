window.PA = window.PA || {};

PA._scope = 'direct';
PA._scopeListeners = [];

PA.setScope = function(mode) {
    PA._scope = mode;
    PA._renderScopeToggle();
    for (var i = 0; i < PA._scopeListeners.length; i++) {
        try { PA._scopeListeners[i](mode); } catch (e) { console.warn('[PA] scope listener error', e); }
    }
};

PA.onScopeChange = function(fn) {
    PA._scopeListeners.push(fn);
};

PA.renderScopeToggle = function(containerId) {
    var el = document.getElementById(containerId);
    if (!el) return;
    var mode = PA._scope;
    var html = '';
    var modes = [
        { key: 'direct', label: 'Direct', title: 'Direct: only THIS procedure/node — nothing it calls.' },
        { key: 'subtree', label: 'Subtree', title: 'Subtree: everything this node calls (all descendants), but NOT this node itself.' },
        { key: 'subflow', label: 'SubFlow', title: 'SubFlow: this node PLUS all of its descendants combined (the whole flow rooted here).' },
        { key: 'full', label: 'Full', title: 'Full: every node in the entire analysis, regardless of the current node.' }
    ];
    for (var i = 0; i < modes.length; i++) {
        var m = modes[i];
        html += '<button class="dh-scope-btn' + (mode === m.key ? ' active' : '') + '" data-scope="' + m.key + '" onclick="PA.setScope(\'' + m.key + '\')" title="' + m.title + '">' + m.label + '</button>';
    }
    el.innerHTML = html;
};

PA._renderScopeToggle = function() {
    PA.renderScopeToggle('dhScopeToggle');
};

PA._currentScopeId = function() {
    return PA.context.callTreeRootId || (PA.context.procId || '').toUpperCase();
};

PA._isPackageRoot = function() {
    return PA.context.callTreeRootId === '__PKG_ROOT__';
};

/**
 * UI-024 (§162) — async now: awaits `PA.nodeIndex.ensureLoaded()` before reading. This was the
 * "foundational, high-fan-in" utility §161 declined to convert on the assumption it had dozens of
 * synchronous callers; traced for real this time (`grep -rn "getScopedNodes(" *.js`) and found
 * exactly one real caller (`export.js`'s own `_gather`, already `async`) — the api.js/summary.js
 * hits were doc comments, not calls, matching §99's own earlier finding. No cascade needed.
 */
PA.getScopedNodes = async function() {
    var nodes = await PA.nodeIndex.ensureLoaded();
    var mode = PA._scope;
    var currentId = PA._currentScopeId();
    var treeIds = PA.context.callTreeNodeIds;

    if (mode === 'full' || PA._isPackageRoot()) return nodes.slice();
    if (mode === 'direct') {
        return nodes.filter(function(n) {
            var nid = (n.nodeId || n.name || '').toUpperCase();
            return nid === currentId || (treeIds && treeIds.has(nid) && treeIds.size === 1);
        });
    }
    if (mode === 'subtree') {
        return nodes.filter(function(n) {
            var nid = (n.nodeId || n.name || '').toUpperCase();
            return nid !== currentId && treeIds && treeIds.has(nid);
        });
    }
    return nodes.filter(function(n) {
        var nid = (n.nodeId || n.name || '').toUpperCase();
        return nid === currentId || (treeIds && treeIds.has(nid));
    });
};

PA.isInScope = function(nodeId) {
    var mode = PA._scope;
    if (mode === 'full' || PA._isPackageRoot()) return true;
    var nid = (nodeId || '').toUpperCase();
    var currentId = PA._currentScopeId();
    var treeIds = PA.context.callTreeNodeIds;
    if (mode === 'direct') return nid === currentId;
    if (mode === 'subtree') return nid !== currentId && treeIds && treeIds.has(nid);
    return nid === currentId || (treeIds && treeIds.has(nid));
};
