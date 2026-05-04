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
        { key: 'direct', label: 'Direct', title: 'Current procedure only' },
        { key: 'subtree', label: 'Subtree', title: 'All descendants, excluding current node' },
        { key: 'subflow', label: 'SubFlow', title: 'Current node + all descendants combined' },
        { key: 'full', label: 'Full', title: 'All nodes in the entire analysis' }
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

PA.getScopedNodes = function() {
    var nodes = PA.analysisData ? PA.analysisData.nodes : [];
    var mode = PA._scope;
    var currentId = (PA.context.procId || '').toUpperCase();
    var treeIds = PA.context.callTreeNodeIds;

    if (mode === 'full') return nodes.slice();
    if (mode === 'direct') {
        return nodes.filter(function(n) {
            return (n.nodeId || n.name || '').toUpperCase() === currentId;
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
    if (mode === 'full') return true;
    var nid = (nodeId || '').toUpperCase();
    var currentId = (PA.context.procId || '').toUpperCase();
    var treeIds = PA.context.callTreeNodeIds;
    if (mode === 'direct') return nid === currentId;
    if (mode === 'subtree') return nid !== currentId && treeIds && treeIds.has(nid);
    return nid === currentId || (treeIds && treeIds.has(nid));
};
