window.PA = window.PA || {};

/**
 * Details → Transactions sub-tab.
 * Renders the flow-wide transaction-control statements (COMMIT / ROLLBACK /
 * SAVEPOINT, + autonomous procs) from PA.analysisData.transactionStatements.
 * Clicking a row's line jumps to that line in the proc's source.
 */
PA.transactions = {
    _all: [],
    _q: '',

    load: function () {
        var d = PA.analysisData || {};
        PA.transactions._all = d.transactionStatements || [];
        PA.transactions._render();
    },

    onSearch: function () {
        var el = document.getElementById('txnSearch');
        PA.transactions._q = SearchQuery.normUpper(el ? el.value : '');
        PA.transactions._render();
    },
    clearFilter: function () {
        var el = document.getElementById('txnSearch');
        if (el) el.value = '';
        PA.transactions._q = '';
        PA.transactions._render();
    },

    /** Re-render on scope change (Direct / Subtree / SubFlow / Full) — consistent with the
     *  other Details sub-tabs; _render honours PA.isInScope per statement. */
    applyScope: function () {
        PA.transactions._render();
    },

    _render: function () {
        var c = document.getElementById('txnContainer');
        if (!c) return;
        var q = PA.transactions._q;
        var scopeFn = (typeof PA.isInScope === 'function') ? PA.isInScope : null;
        var list = PA.transactions._all.filter(function (s) {
            // Scope filter (Direct/Subtree/SubFlow/Full) — match the rest of the UI.
            if (scopeFn && s.nodeId && !PA.isInScope(s.nodeId)) return false;
            if (!q) return true;
            var t = ((s.proc || '') + ' ' + (s.type || '') + ' ' + (s.name || '') + ' ' + (s.schema || '')).toUpperCase();
            return t.indexOf(q) >= 0;
        });
        var cnt = document.getElementById('txnTotalCount');
        if (cnt) cnt.textContent = list.length + ' statements';

        if (!list.length) {
            c.innerHTML = '<div class="empty-msg">No transaction-control statements in this flow</div>';
            return;
        }
        var color = { COMMIT: 'var(--green)', ROLLBACK: 'var(--red)', SAVEPOINT: 'var(--accent-hover)' };
        var html = '<table class="to-table" style="width:100%"><thead><tr>'
            + '<th>Type</th><th>Procedure</th><th>Savepoint</th><th style="text-align:right">Line</th><th></th></tr></thead><tbody>';
        for (var i = 0; i < list.length; i++) {
            var s = list[i];
            var col = color[s.type] || 'var(--text-muted)';
            var jump = s.line
                ? 'onclick="PA.sourceView.openNodeAtLine(\'' + PA.escJs(s.nodeId || '') + '\', ' + s.line + ')" style="cursor:pointer"'
                : '';
            html += '<tr class="to-row" ' + jump + '>';
            html += '<td><span class="op-badge" style="background:' + col + ';color:var(--text-inverse)">' + PA.esc(s.type) + '</span>'
                + (s.autonomous ? ' <span class="op-badge" style="background:var(--orange);color:var(--text-inverse)">AUTO</span>' : '') + '</td>';
            html += '<td style="font-weight:600;font-size:11px">' + PA.esc((s.schema ? s.schema + '.' : '') + (s.proc || '')) + '</td>';
            html += '<td style="font-size:11px;color:var(--text-muted)">' + PA.esc(s.name || '') + '</td>';
            html += '<td style="text-align:right;font-family:var(--font-mono)">' + (s.line || '') + '</td>';
            html += '<td style="text-align:right"><span class="to-detail-line" ' + jump + '>' + (s.line ? 'L' + s.line : '') + '</span></td>';
            html += '</tr>';
        }
        html += '</tbody></table>';
        c.innerHTML = html;
    }
};
