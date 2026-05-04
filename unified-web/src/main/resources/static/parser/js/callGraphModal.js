window.PA = window.PA || {};

PA.callGraphModal = {

    _getTypeLabel: function(objectType) {
        var t = (objectType || '').toUpperCase();
        if (t === 'FUNCTION') return 'F';
        if (t === 'TRIGGER') return 'T';
        if (t.indexOf('PACKAGE') !== -1) return 'PKG';
        return 'P';
    },

    _findNodeData: function(nodeId) {
        var nodes = PA.analysisData ? PA.analysisData.nodes : [];
        for (var i = 0; i < nodes.length; i++) {
            var nid = nodes[i].nodeId || nodes[i].name || '';
            if (nid === nodeId) return nodes[i];
        }
        return null;
    },

    _deriveCallersCallees: function(nodeId) {
        var edges = PA.callGraphViz._edges || [];
        var callers = [];
        var callees = [];
        for (var i = 0; i < edges.length; i++) {
            if (edges[i].to === nodeId) callers.push(edges[i].from);
            if (edges[i].from === nodeId) callees.push(edges[i].to);
        }
        return { callers: callers, callees: callees };
    },

    _findGraphNode: function(nodeId) {
        var nodes = PA.callGraphViz._nodes || [];
        for (var i = 0; i < nodes.length; i++) {
            if (nodes[i].id === nodeId) return nodes[i];
        }
        return null;
    },

    show: function(node) {
        if (!node) return;
        var overlay = document.getElementById('cgModalOverlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'cgModalOverlay';
            overlay.className = 'cg-modal-overlay';
            overlay.onclick = function(e) {
                if (e.target === overlay) PA.callGraphModal.close();
            };
            document.body.appendChild(overlay);
        }

        var detail = PA.callGraphModal._findNodeData(node.id);
        var rel = PA.callGraphModal._deriveCallersCallees(node.id);
        var typeLabel = PA.callGraphModal._getTypeLabel(node.objectType);

        var loc = node.loc || (detail ? (detail.linesOfCode || 0) : 0);
        var tables = detail ? (detail.tableCount || detail.tables || 0) : 0;
        var calls = node.outEdges || 0;

        var html = '';
        html += '<div class="cg-modal" onclick="event.stopPropagation()">';
        html += '<div class="cg-modal-header">';
        html += '<span class="lp-icon ' + typeLabel + '" style="display:inline-flex;width:20px;height:20px;font-size:9px">' + PA.esc(typeLabel) + '</span>';
        html += '<span class="cg-modal-title">' + PA.esc(node.label || node.id) + '</span>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphModal.close()" title="Close">&times;</button>';
        html += '</div>';

        html += '<div class="cg-modal-body">';

        html += '<div class="cg-modal-meta">';
        if (node.schema) html += '<span class="cg-meta-item"><span class="cg-meta-label">Schema</span>' + PA.esc(node.schema) + '</span>';
        html += '<span class="cg-meta-item"><span class="cg-meta-label">Type</span>' + PA.esc(node.objectType || 'PROCEDURE') + '</span>';
        html += '</div>';

        html += '<div class="cg-modal-stats">';
        html += '<div class="cg-stat"><span class="cg-stat-val">' + loc + '</span><span class="cg-stat-label">LOC</span></div>';
        html += '<div class="cg-stat"><span class="cg-stat-val">' + tables + '</span><span class="cg-stat-label">Tables</span></div>';
        html += '<div class="cg-stat"><span class="cg-stat-val">' + calls + '</span><span class="cg-stat-label">Calls</span></div>';
        html += '<div class="cg-stat"><span class="cg-stat-val">' + node.inEdges + '</span><span class="cg-stat-label">Called by</span></div>';
        html += '</div>';

        html += '<div class="cg-modal-lists">';
        html += PA.callGraphModal._buildList('Callers', rel.callers);
        html += PA.callGraphModal._buildList('Callees', rel.callees);
        html += '</div>';

        html += '<div class="cg-modal-actions">';
        html += '<button class="btn btn-sm cg-nav-btn" onclick="PA.callGraphModal.navigate(\'' + PA.escJs(node.id) + '\')">Navigate to Detail</button>';
        html += '</div>';

        html += '</div>';
        html += '</div>';

        overlay.innerHTML = html;
        overlay.style.display = '';
    },

    _buildList: function(title, ids) {
        var html = '<div class="cg-rel-section">';
        html += '<div class="cg-rel-title">' + PA.esc(title) + ' (' + ids.length + ')</div>';
        if (ids.length === 0) {
            html += '<div class="cg-rel-empty">None</div>';
        } else {
            html += '<div class="cg-rel-list">';
            for (var i = 0; i < ids.length; i++) {
                var gNode = PA.callGraphModal._findGraphNode(ids[i]);
                var label = gNode ? gNode.label : ids[i];
                var tl = gNode ? PA.callGraphModal._getTypeLabel(gNode.objectType) : 'P';
                html += '<span class="cg-rel-item" onclick="PA.callGraphModal.focusNode(\'' + PA.escJs(ids[i]) + '\')">';
                html += '<span class="lp-icon ' + tl + '" style="display:inline-flex;width:14px;height:14px;font-size:7px">' + PA.esc(tl) + '</span>';
                html += PA.esc(label);
                html += '</span>';
            }
            html += '</div>';
        }
        html += '</div>';
        return html;
    },

    close: function() {
        var overlay = document.getElementById('cgModalOverlay');
        if (overlay) overlay.style.display = 'none';
    },

    navigate: function(nodeId) {
        PA.callGraphModal.close();
        PA.showProcedure(nodeId);
    },

    focusNode: function(nodeId) {
        var gNode = PA.callGraphModal._findGraphNode(nodeId);
        if (gNode) {
            PA.callGraphModal.close();
            PA.callGraphModal.show(gNode);
        }
    }
};
