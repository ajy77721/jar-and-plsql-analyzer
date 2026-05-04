window.PA = window.PA || {};

PA.triggers = {
    showList: function(idx) {
        var s = PA.tf.state('to');
        if (!s) return;
        var t = s.filtered[idx];
        if (!t) return;
        var triggers = t.triggers || [];
        if (!triggers.length) return;

        var existing = document.getElementById('triggerPopup');
        if (existing) existing.remove();

        var html = '<div class="trigger-popup" id="triggerPopup">';
        html += '<div class="trigger-popup-header">';
        html += '<span>Triggers on ' + PA.esc(t.tableName || '') + '</span>';
        html += '<button class="btn btn-sm" onclick="document.getElementById(\'triggerPopup\').remove()">&times;</button>';
        html += '</div>';
        for (var i = 0; i < triggers.length; i++) {
            var tr = triggers[i];
            html += '<div class="trigger-item">';
            if (tr.definition) {
                html += '<span class="trigger-name" data-tip="Click to view definition" onclick="event.stopPropagation();PA.triggers.showDef(' + idx + ',' + i + ')">' + PA.esc(tr.name || '') + '</span>';
            } else if (tr.nodeId) {
                html += '<span class="trigger-name" data-tip="Click to view source" onclick="PA.triggers.showNodeDetail(\'' + PA.escJs(tr.nodeId) + '\')">' + PA.esc(tr.name || '') + '</span>';
            } else {
                html += '<span class="trigger-name" style="cursor:default">' + PA.esc(tr.name || '') + '</span>';
            }
            if (tr.timing) html += '<span class="trigger-timing" data-tip="Trigger timing">' + PA.esc(tr.timing) + '</span>';
            if (tr.event) html += '<span class="trigger-event" data-tip="Trigger event">' + PA.esc(tr.event) + '</span>';
            if (tr.triggerType) html += '<span class="trigger-meta">' + PA.esc(tr.triggerType) + '</span>';
            if (tr.nodeId) html += '<span class="trigger-src-btn" data-tip="View trigger source" onclick="event.stopPropagation();PA.triggers.showNodeDetail(\'' + PA.escJs(tr.nodeId) + '\')" title="View Source">&#9998;</span>';
            if (tr.source === 'DATABASE') html += '<span class="trigger-meta" style="color:var(--badge-teal)">DB</span>';
            html += '</div>';
        }
        html += '</div>';
        document.body.insertAdjacentHTML('beforeend', html);
    },

    showNodeDetail: function(nodeId) {
        var existing = document.getElementById('triggerPopup');
        if (existing) existing.remove();
        if (nodeId) PA.sourceView.openModal(nodeId);
    },

    showDef: function(tableIdx, trigIdx, keepList) {
        var s = PA.tf.state('to');
        if (!s) return;
        var t = s.filtered[tableIdx];
        if (!t || !t.triggers) return;
        var tr = t.triggers[trigIdx];
        if (!tr) return;

        var existing = document.getElementById('triggerDefModal');
        if (existing) existing.remove();
        if (!keepList) {
            var popup = document.getElementById('triggerPopup');
            if (popup) popup.remove();
        }

        var html = '<div class="trigger-popup trigger-def-modal" id="triggerDefModal">';
        html += '<div class="trigger-popup-header">';
        html += '<span>' + PA.esc(tr.name || '') + '</span>';
        html += '<button class="btn btn-sm" onclick="document.getElementById(\'triggerDefModal\').remove()">&times;</button>';
        html += '</div>';

        html += '<div class="trigger-meta-bar">';
        html += '<span class="trigger-timing">' + PA.esc(tr.timing || '') + '</span>';
        html += '<span class="trigger-event">' + PA.esc(tr.event || '') + '</span>';
        html += '<span class="trigger-meta">ON ' + PA.esc(t.tableName || '') + '</span>';
        if (tr.triggerType) html += '<span class="trigger-meta">' + PA.esc(tr.triggerType) + '</span>';
        html += '</div>';

        if (tr.tableOps && tr.tableOps.length > 0) {
            html += '<div class="trigger-section">';
            html += '<div class="trigger-section-title">Table Operations in Trigger</div>';
            for (var i = 0; i < tr.tableOps.length; i++) {
                var op = tr.tableOps[i];
                html += '<span class="op-badge ' + PA.esc(op.operation || '') + '" style="margin:2px 4px 2px 0">' + PA.esc(op.operation || '') + '</span>';
                html += '<span class="trigger-table-ref">' + PA.esc(op.tableName || '') + '</span>';
                if (op.objectType) {
                    var ot = op.objectType;
                    var cls = ot === 'VIEW' ? 'view' : (ot === 'MATERIALIZED_VIEW' || ot === 'MATERIALIZED VIEW') ? 'mv' : 'table';
                    html += '<span class="to-type-badge ' + cls + '" style="margin-left:2px">' + PA.esc(ot.replace('_', ' ')) + '</span>';
                }
            }
            html += '</div>';
        }

        if (tr.definition) {
            html += '<div class="trigger-section">';
            html += '<div class="trigger-section-title">Source</div>';
            html += '<pre class="trigger-source">' + PA.esc(tr.definition) + '</pre>';
            html += '</div>';
        }

        html += '</div>';
        document.body.insertAdjacentHTML('beforeend', html);
    }
};

(function() {
    document.addEventListener('click', function(e) {
        var triggerPopup = document.getElementById('triggerPopup');
        if (triggerPopup && !triggerPopup.contains(e.target)) {
            triggerPopup.remove();
        }
        var defModal = document.getElementById('triggerDefModal');
        if (defModal && !defModal.contains(e.target)) {
            defModal.remove();
        }
    });
})();
