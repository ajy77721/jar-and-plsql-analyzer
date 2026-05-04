PA._allHistory = [];
PA._historySearchQuery = '';

PA.loadHistory = async function() {
    try {
        var hist = await PA.api.listHistory();
        PA._allHistory = hist || [];
        PA._renderFilteredHistory();
        if (document.getElementById('homeTabPerfSummary') && document.getElementById('homeTabPerfSummary').style.display !== 'none') {
            PA.perfSummary.render();
        }
    } catch (e) {
        document.getElementById('historyList').innerHTML = '<div class="empty-msg">Could not load history</div>';
    }
};

PA.onHistorySearch = function(q) {
    PA._historySearchQuery = (q || '').toUpperCase();
    PA._renderFilteredHistory();
};

PA._renderFilteredHistory = function() {
    var container = document.getElementById('historyList');
    var countEl = document.getElementById('historyCount');
    var list = PA._allHistory || [];
    var q = PA._historySearchQuery;
    if (q) {
        list = list.filter(function(item) {
            var txt = ((item.entryPoint || '') + ' ' + (item.entrySchema || '') + ' ' + (item.name || '')).toUpperCase();
            return txt.includes(q);
        });
    }
    if (countEl) countEl.textContent = list.length;

    if (list.length === 0) {
        container.innerHTML = '<div class="empty-msg">No analyses found</div>';
        return;
    }

    var html = '';
    for (var i = 0; i < list.length; i++) {
        var item = list[i];
        var entry = item.entryPoint || item.name || '';
        var schema = item.entrySchema || '';
        var ts = item.timestamp ? new Date(item.timestamp).toLocaleString() : '';
        var procs = item.totalNodes || 0;
        var tables = item.totalTables || 0;
        var loc = item.totalLinesOfCode || 0;
        var errors = (item.errors && item.errors.length) || 0;
        var duration = PA.formatDurationShort(item.crawlTimeMs);

        html += '<div class="history-card" onclick="PA.loadAnalysis(\'' + PA.escJs(item.name) + '\')">';
        html += '<div class="hc-info">';
        html += '<div class="hc-title">' + PA.esc(entry);
        if (schema) html += ' <span style="color:var(--text-muted);font-size:11px">(' + PA.esc(schema) + ')</span>';
        html += '</div>';
        if (ts) html += '<div class="hc-meta">' + PA.esc(ts) + '</div>';
        html += '<div class="hc-stats">' + procs + ' procs | ' + tables + ' tables | ' + loc.toLocaleString() + ' LOC';
        if (duration) html += ' | ' + PA.esc(duration);
        if (errors > 0) html += ' | <span style="color:#ef4444">' + errors + ' errors</span>';
        html += '</div></div>';
        html += '<div class="hc-actions">';
        html += '<button class="btn btn-sm btn-primary" onclick="event.stopPropagation(); PA.loadAnalysis(\'' + PA.escJs(item.name) + '\')">Load</button>';
        html += '<button class="btn btn-sm btn-danger" onclick="event.stopPropagation(); PA.deleteAnalysis(\'' + PA.escJs(item.name) + '\')">Delete</button>';
        html += '</div></div>';
    }
    container.innerHTML = html;
};

PA.deleteAnalysis = async function(name) {
    if (!confirm('Delete analysis "' + name + '"?')) return;
    try {
        var res = await fetch('/api/analyses/' + encodeURIComponent(name), { method: 'DELETE' });
        if (res.ok) {
            PA.toast('Deleted: ' + name, 'success');
            await PA.loadHistory();
        } else {
            PA.toast('Could not delete', 'error');
        }
    } catch (e) {
        PA.toast('Delete failed: ' + (e.message || e), 'error');
    }
};

PA.startAnalysis = async function() {
    var formData = PA.home ? PA.home.getFormData() : {};
    var entryPoint = formData.entryPoint || '';
    if (!entryPoint) {
        var objectInput = document.getElementById('homeObject');
        entryPoint = objectInput ? objectInput.value.trim() : '';
    }
    if (!entryPoint) {
        PA.toast('Enter an object name (e.g. PKG_CUSTOMER or SCHEMA.PKG_NAME)', 'error');
        return;
    }

    var prog = document.getElementById('homeProgress');
    var progText = document.getElementById('homeProgressText');
    var progFill = document.getElementById('homeProgressFill');
    prog.style.display = 'block';
    progFill.style.width = '30%';
    progText.textContent = 'Analyzing ' + entryPoint + '...';

    try {
        var body = { entryPoint: entryPoint };
        if (formData.owner) body.owner = formData.owner;
        if (formData.objectType) body.objectType = formData.objectType;

        var resp = await PA.api.runAnalysis(entryPoint, body);
        if (resp && resp.error) { prog.style.display = 'none'; PA.toast(resp.error, 'error'); return; }
        progFill.style.width = '100%';
        progText.textContent = 'Complete!';
        setTimeout(function() { prog.style.display = 'none'; }, 2000);
        PA.toast('Analysis complete', 'success');
        await PA.loadHistory();
        if (resp && resp.name) PA.loadAnalysis(resp.name);
    } catch (e) {
        prog.style.display = 'none';
        PA.toast('Failed: ' + (e.message || e), 'error');
    }
};
