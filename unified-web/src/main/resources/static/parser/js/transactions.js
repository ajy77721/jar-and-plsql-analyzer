window.PA = window.PA || {};

/**
 * Details → Transactions sub-tab.
 *
 * Renders the flow-wide transaction-control statements (COMMIT / ROLLBACK / SAVEPOINT, + autonomous
 * procs) from PA.analysisData.transactionStatements through the SHARED usage-table component — the
 * same paginated, filterable Name/Line/snippet table every other Details case list uses. It used to
 * be a flat, unpaginated <table>; a flow can carry enough savepoints/rollbacks that the flat dump
 * was the one list on the Details tab without a filter or pager.
 *
 * The row's name opens the procedure; its line jumps to that line in the source.
 */
PA.transactions = {
    _all: [],

    load: function () {
        var d = PA.analysisData || {};
        PA.transactions._all = d.transactionStatements || [];
        PA.transactions._render();
    },

    /** Re-render on scope change (Direct / Subtree / SubFlow / Full) — consistent with the
     *  other Details sub-tabs; _rows honours PA.isInScope per statement. */
    applyScope: function () {
        PA.transactions._render();
    },

    /** The statements in scope. The usage-table component owns the text filter. */
    _rows: function () {
        var scopeFn = (typeof PA.isInScope === 'function') ? PA.isInScope : null;
        return (PA.transactions._all || []).filter(function (s) {
            if (scopeFn && s.nodeId && !PA.isInScope(s.nodeId)) return false;
            return true;
        });
    },

    _render: function () {
        var c = document.getElementById('txnContainer');
        if (!c) return;
        var list = PA.transactions._rows();

        var cnt = document.getElementById('txnTotalCount');
        if (cnt) cnt.textContent = list.length + ' statements';

        if (!list.length) {
            c.innerHTML = '<div class="empty-msg">No transaction-control statements in this flow</div>';
            return;
        }

        var opts = {
            showTitle: false, nameLabel: 'Procedure', countLabel: 'statements', snippetLabel: 'Savepoint',
            name: function (s) { return (s.schema ? s.schema + '.' : '') + (s.proc || ''); },
            nameLink: function (s) {
                return s.nodeId ? "PA.showProcedure('" + PA.escJs(s.nodeId) + "')" : null;
            },
            line: function (s) { return s.line; },
            lineLink: function (s) {
                return s.line
                    ? "PA.sourceView.openNodeAtLine('" + PA.escJs(s.nodeId || '') + "'," + s.line + ")"
                    : null;
            },
            snippet: function (s) { return s.name || ''; },
            badges: function (s) { return [s.type, s.autonomous ? 'AUTO' : null]; },
            search: function (s) { return s.type || ''; }
        };
        c.innerHTML = PA.usageTable.render('ut-txn', list, opts);
        PA.usageTable.mount('ut-txn', list, opts);
    }
};
