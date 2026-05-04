PA.switchLeftTab = function(tab) {
    document.querySelectorAll('.ltab').forEach(function(t) { t.classList.remove('active'); });
    var btn = document.querySelector('.ltab[data-tab="' + tab + '"]');
    if (btn) btn.classList.add('active');
    document.querySelectorAll('.left-panel-list').forEach(function(p) { p.style.display = 'none'; });
    var map = { procedures: 'lpProcedures', tables: 'lpTables' };
    var panel = document.getElementById(map[tab]);
    if (panel) panel.style.display = '';
};

PA.filterLeft = function() {
    var q = (document.getElementById('leftFilter').value || '').toLowerCase();
    var visible = document.querySelector('.left-panel-list:not([style*="none"])') ||
                  document.querySelector('.left-panel-list[style=""]');
    if (!visible) return;

    if (q) {
        PA._expandAllLeftItems();
    }

    if (!q) {
        visible.querySelectorAll('.lp-item, .tree-node').forEach(function(el) { el.style.display = ''; });
        visible.querySelectorAll('.tree-children').forEach(function(el) { el.style.display = ''; });
        return;
    }

    visible.querySelectorAll('.lp-item').forEach(function(item) {
        var txt = (item.dataset.filter || item.textContent || '').toLowerCase();
        item.style.display = txt.includes(q) ? '' : 'none';
    });
    visible.querySelectorAll('.tree-node').forEach(function(node) {
        var search = (node.dataset.search || '').toLowerCase();
        if (search.includes(q)) {
            node.style.display = '';
            var children = node.querySelector('.tree-children');
            if (children) children.style.display = '';
        }
    });
};

PA._expandAllLeftItems = function() {
    if (PA._leftProcRendered < PA._leftProcSorted.length) {
        var container = document.getElementById('lpProcItems');
        if (container) {
            var btn = container.querySelector('.lp-show-more');
            if (btn) btn.remove();
            var html = '';
            for (var i = PA._leftProcRendered; i < PA._leftProcSorted.length; i++) {
                html += PA._renderLeftProcItem(PA._leftProcSorted[i]);
            }
            container.insertAdjacentHTML('beforeend', html);
            PA._leftProcRendered = PA._leftProcSorted.length;
        }
    }
    if (PA._leftTableRendered < PA._leftTableSorted.length) {
        var tContainer = document.getElementById('lpTableItems');
        if (tContainer) {
            var tBtn = tContainer.querySelector('.lp-show-more');
            if (tBtn) tBtn.remove();
            var tHtml = '';
            for (var j = PA._leftTableRendered; j < PA._leftTableSorted.length; j++) {
                tHtml += PA._renderLeftTableItem(PA._leftTableSorted[j]);
            }
            tContainer.insertAdjacentHTML('beforeend', tHtml);
            PA._leftTableRendered = PA._leftTableSorted.length;
        }
    }
};

PA._leftProcBatchSize = 50;
PA._leftProcSorted = [];
PA._leftProcRendered = 0;

PA._renderLeftProcedures = function() {
    var panel = document.getElementById('lpProcedures');
    if (!PA.analysisData || !PA.analysisData.nodes || !PA.analysisData.nodes.length) {
        panel.innerHTML = '<div class="empty-msg">No procedures found</div>';
        return;
    }

    var nodes = PA.analysisData.nodes;
    PA._leftProcSorted = nodes.slice().sort(function(a, b) { return (a.depth || 0) - (b.depth || 0) || (a.name || '').localeCompare(b.name || ''); });
    PA._leftProcRendered = 0;

    var html = '<div style="padding:6px 10px;font-size:10px;color:var(--text-muted);border-bottom:1px solid var(--border)">';
    html += 'Procedures (' + nodes.length + ')</div>';
    html += '<div id="lpProcItems"></div>';
    panel.innerHTML = html;
    PA._renderLeftProcBatch();
};

PA._renderLeftProcItem = function(n) {
    var nodeId = n.nodeId || n.name;
    var name = n.name || nodeId || '?';
    var schema = n.schema || '';
    var objType = n.objectType || '';
    var typeLabel = objType === 'FUNCTION' ? 'F' : objType === 'TRIGGER' ? 'T' : 'P';
    var depth = n.depth != null ? n.depth : 0;
    var loc = n.linesOfCode || 0;
    var colorObj = PA.getSchemaColor(schema);

    var tip = name;
    if (schema) tip = schema + '.' + tip;
    if (n.lineStart) tip += ' | Lines ' + n.lineStart + '-' + (n.lineEnd || '?');
    if (loc) tip += ' | ' + loc + ' LOC';
    tip += ' | Depth ' + depth;

    var html = '<div class="lp-item" data-id="' + PA.escAttr(nodeId) + '" data-filter="' + PA.escAttr(nodeId + ' ' + name + ' ' + schema) + '"';
    html += ' onclick="PA.showProcedure(\'' + PA.escJs(nodeId) + '\')" title="' + PA.escAttr(tip) + '">';
    if (schema) {
        html += '<span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + ';font-size:7px;padding:0 4px;margin-right:2px" data-tip="Owner schema" title="Oracle schema that owns this object">' + PA.esc(schema.substring(0, 4)) + '</span>';
    }
    var nodeTypeTooltip = typeLabel === 'P' ? 'P=Procedure' : typeLabel === 'F' ? 'F=Function' : typeLabel === 'T' ? 'T=Trigger' : typeLabel;
    html += '<span class="lp-icon ' + typeLabel + '" style="width:16px;height:16px;font-size:8px" title="' + nodeTypeTooltip + '">' + typeLabel + '</span>';
    html += '<span class="lp-name" style="font-size:11px">' + PA.esc(name) + '</span>';
    html += '<span class="lp-badge" data-tip="Call depth level">d' + depth + '</span>';
    if (loc > 0) html += '<span class="lp-badge" data-tip="Lines of code" style="color:var(--text-muted)">' + loc + '</span>';
    html += '</div>';
    return html;
};

PA._renderLeftProcBatch = function() {
    var container = document.getElementById('lpProcItems');
    if (!container) return;
    var sorted = PA._leftProcSorted;
    var start = PA._leftProcRendered;
    var end = Math.min(start + PA._leftProcBatchSize, sorted.length);

    var html = '';
    for (var i = start; i < end; i++) {
        html += PA._renderLeftProcItem(sorted[i]);
    }

    var existingBtn = container.querySelector('.lp-show-more');
    if (existingBtn) existingBtn.remove();
    container.insertAdjacentHTML('beforeend', html);
    PA._leftProcRendered = end;

    if (end < sorted.length) {
        var remaining = sorted.length - end;
        container.insertAdjacentHTML('beforeend',
            '<div class="lp-show-more" onclick="PA._renderLeftProcBatch()" style="padding:6px 10px;font-size:11px;color:var(--accent);cursor:pointer;text-align:center;border-top:1px solid var(--border)">Show more (' + remaining + ' remaining)</div>');
    }
};

PA._leftTableBatchSize = 50;
PA._leftTableSorted = [];
PA._leftTableRendered = 0;

PA._renderLeftTables = async function() {
    var panel = document.getElementById('lpTables');
    if (!PA.analysisData) {
        panel.innerHTML = '<div class="empty-msg">No tables found</div>';
        return;
    }

    try {
        var tables = await PA.api.getTableOperations();
        if (!tables || tables.length === 0) {
            panel.innerHTML = '<div class="empty-msg">No tables found</div>';
            return;
        }

        PA._leftTableSorted = tables.slice().sort(function(a, b) { return (a.tableName || '').localeCompare(b.tableName || ''); });
        PA._leftTableRendered = 0;

        var html = '<div style="padding:6px 10px;font-size:10px;color:var(--text-muted);border-bottom:1px solid var(--border)">';
        html += 'Tables (' + PA._leftTableSorted.length + ')</div>';
        html += '<div id="lpTableItems"></div>';
        panel.innerHTML = html;
        PA._renderLeftTableBatch();
    } catch (e) {
        panel.innerHTML = '<div class="empty-msg">Failed to load tables</div>';
    }
};

PA._renderLeftTableItem = function(t) {
    var ops = (t.operations || []).map(function(op) {
        return '<span class="op-badge ' + op + '" style="font-size:7px;padding:0 3px">' + op[0] + '</span>';
    }).join('');
    var count = t.accessCount || 0;

    var html = '<div class="lp-item" data-filter="' + PA.escAttr(t.tableName || '') + '" onclick="PA.tableDetail.open(\'' + PA.escJs(t.tableName || '') + '\', \'\')" style="cursor:pointer">';
    html += '<span style="font-size:7px;font-weight:700;color:var(--text-muted);min-width:12px;text-align:center">T</span>';
    html += '<span class="lp-name" style="color:var(--teal,#0f766e);font-size:11px">' + PA.esc(t.tableName || '') + '</span>';
    html += '<span style="display:flex;gap:1px;align-items:center">' + ops + '</span>';
    html += '<span class="lp-badge">' + count + '</span>';
    html += '</div>';
    return html;
};

PA._renderLeftTableBatch = function() {
    var container = document.getElementById('lpTableItems');
    if (!container) return;
    var sorted = PA._leftTableSorted;
    var start = PA._leftTableRendered;
    var end = Math.min(start + PA._leftTableBatchSize, sorted.length);

    var html = '';
    for (var i = start; i < end; i++) {
        html += PA._renderLeftTableItem(sorted[i]);
    }

    var existingBtn = container.querySelector('.lp-show-more');
    if (existingBtn) existingBtn.remove();
    container.insertAdjacentHTML('beforeend', html);
    PA._leftTableRendered = end;

    if (end < sorted.length) {
        var remaining = sorted.length - end;
        container.insertAdjacentHTML('beforeend',
            '<div class="lp-show-more" onclick="PA._renderLeftTableBatch()" style="padding:6px 10px;font-size:11px;color:var(--accent);cursor:pointer;text-align:center;border-top:1px solid var(--border)">Show more (' + remaining + ' remaining)</div>');
    }
};
