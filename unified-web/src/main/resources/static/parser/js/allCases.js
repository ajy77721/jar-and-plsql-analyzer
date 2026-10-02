window.PA = window.PA || {};

/*
 * Case-count helpers for the Details tab.
 *
 * This module used to render a per-procedure "All cases (N)" accordion panel listing EVERY analysis
 * case captured for the current node. That panel duplicated the Details sub-tabs, so the panel is
 * retired: the same counts now live in the SUB-TAB LABELS ("Cursors (43)", "Dynamic SQL (32)", …),
 * and each sub-tab renders the cases itself in the shared paginated table format.
 *
 * What remains here is the pure, well-tested group/count logic the labels are built from:
 *   - `_groups(detail)`  — the applicable-only case groups (rows + their usage-table options).
 *   - `counts(detail)`   — the flat per-sub-tab count map the labels read, derived from `_groups`
 *                          plus the node-doc facets that were never accordion groups
 *                          (Transactions / Statements / Exception handlers).
 *   - `triggerCount(detail, cb)` — "Triggers fired": not on the node doc, so it is joined in
 *                          asynchronously from the tables index, exactly as the panel used to.
 *
 * The source is the procedure's own node doc (`PA.currentDetail`, fetched by `PA.showProcedure` /
 * `PA.loadNodeDetail`): cursors, dynamic SQL, table operations, joins, globals, calls and sequences
 * are all already on it.
 *
 * Applicable-only: a procedure with no cursors shows a zero Cursors count and no label suffix.
 */
PA.allCases = {
    /**
     * The synchronous groups, all read straight off the node doc. Retained from the retired panel
     * because the labels' counts are derived from these groups — the group logic (which cases count
     * and how they are shaped) is the single source of truth for both.
     */
    _groups: function(detail) {
        detail = detail || {};
        var g = [];
        var nodeId = detail.nodeId || '';
        var sourceFile = detail.sourceFile || '';
        var selfSource = function(line) { return { file: sourceFile, line: line }; };

        // Cursors
        var cursors = detail.cursors || [];
        if (cursors.length) g.push({
            title: 'Cursors', id: 'ac-cursors', rows: cursors,
            opts: {
                showTitle: false, nameLabel: 'Cursor', countLabel: 'cursors', snippetLabel: 'Query',
                name: function(c) { return c.name; },
                line: function(c) { return c.line; },
                snippet: function(c) { return c.query; },
                sub: function(c) { return { text: (c.tables || []).join(', ') }; },
                badges: function(c) {
                    var b = [];
                    if (c.forLoop) b.push('FOR_LOOP');
                    if (c.refCursor) b.push('REF_CURSOR');
                    (c.stmtOperations || []).forEach(function(o) { if (o.operation) b.push(o.operation); });
                    return b;
                },
                source: function(c) { return selfSource(c.line); },
                full: function(c) { return c.query ? '<pre>' + PA.esc(c.query) + '</pre>' : ''; }
            }
        });

        // Dynamic SQL
        var dyn = detail.dynamicSql || [];
        if (dyn.length) g.push({
            title: 'Dynamic SQL', id: 'ac-dynamic', rows: dyn,
            opts: {
                showTitle: false, nameLabel: 'Statement', countLabel: 'executions', snippetLabel: 'SQL',
                name: function(d) { return d.type || 'EXECUTE IMMEDIATE'; },
                line: function(d) { return d.line; },
                snippet: function(d) { return d.sqlExpression; },
                sub: function(d) { return (d.usingVariables && d.usingVariables.length) ? { text: 'USING ' + d.usingVariables.join(', ') } : null; },
                badges: function(d) { return [d.type || 'DYNAMIC']; },
                source: function(d) { return selfSource(d.line); },
                full: function(d) { return d.sqlExpression ? '<pre>' + PA.esc(d.sqlExpression) + '</pre>' : ''; }
            }
        });

        // Table operations
        var tables = detail.tables || detail.directTables || [];
        if (tables.length) g.push({
            title: 'Table operations', id: 'ac-tables', rows: tables,
            opts: {
                showTitle: false, nameLabel: 'Table', countLabel: 'tables', showSnippet: false,
                name: function(t) { return (t.schema ? t.schema + '.' : '') + (t.name || ''); },
                nameLink: function(t) { return "PA.tableDetail.open('" + PA.escJs(t.name || '') + "','" + PA.escJs(t.schema || '') + "')"; },
                sub: function(t) { return { text: t.objectType || 'TABLE' }; },
                line: function(t) { return PA.usageTable.firstLine({ operations: t.operations }); },
                badges: function(t) { return Object.keys(t.operations || {}); },
                search: function(t) { return t.objectType || ''; },
                source: function(t) { return selfSource(PA.usageTable.firstLine({ operations: t.operations })); },
                full: function(t) {
                    var ops = t.operations || {};
                    return '<pre>' + PA.esc(Object.keys(ops).map(function(op) {
                        return op + ' L' + (ops[op] || []).join(', L');
                    }).join('\n')) + '</pre>';
                }
            }
        });

        // Joins (inside the table operations)
        var joins = [];
        tables.forEach(function(t) {
            (t.joins || []).forEach(function(j) { joins.push(j); });
        });
        if (joins.length) g.push({
            title: 'Joins', id: 'ac-joins', rows: joins,
            opts: {
                showTitle: false, nameLabel: 'Join', countLabel: 'joins', snippetLabel: 'ON condition',
                name: function(j) {
                    var l = (j.leftTableAlias ? j.leftTableAlias + '.' : '') + (j.leftTable || '');
                    var r = (j.joinedTableAlias ? j.joinedTableAlias + '.' : '') + (j.joinedTable || '');
                    return l + ' → ' + r;
                },
                line: function(j) { return j.line; },
                snippet: function(j) { return j.condition; },
                badges: function(j) { return [j.joinType || 'JOIN']; },
                source: function(j) { return selfSource(j.line); }
            }
        });

        // Globals (package variable reads/writes) — the VARIABLE name leads.
        var globals = detail.externalPackageVarRefDetails || [];
        if (globals.length) g.push({
            title: 'Globals', id: 'ac-globals', rows: globals,
            opts: {
                showTitle: false, nameLabel: 'Variable', countLabel: 'usages',
                nameClass: 'est-var-name',
                name: function(x) { return x.variableName; },
                sub: function(x) { return { text: x.packageName || '' }; },
                line: function(x) { return x.line; },
                badges: function(x) { return [x.accessType || 'UNKNOWN']; },
                source: function(x) { return selfSource(x.line); }
            }
        });

        // Calls (out-bound)
        var calls = detail.calls || [];
        if (calls.length) g.push({
            title: 'Calls', id: 'ac-calls', rows: calls,
            opts: {
                showTitle: false, nameLabel: 'Callee', countLabel: 'calls', showSnippet: false,
                name: function(c) { return c.objectName || c.target || ''; },
                nameLink: function(c) {
                    var id = c.target || c.objectName || '';
                    return id ? "PA.showProcedure('" + PA.escJs(id) + "')" : null;
                },
                line: function(c) { return (c.lines && c.lines.length) ? c.lines[0] : null; },
                sub: function(c) { return { text: c.packageName || c.schema || '' }; },
                badges: function(c) { return [c.type, c.count ? c.count + 'x' : null]; },
                search: function(c) { return c.packageName || ''; },
                source: function(c) { return selfSource((c.lines && c.lines.length) ? c.lines[0] : null); }
            }
        });

        // Sequences
        var seqs = detail.sequences || [];
        if (seqs.length) g.push({
            title: 'Sequences', id: 'ac-sequences', rows: seqs,
            opts: {
                showTitle: false, nameLabel: 'Sequence', countLabel: 'sequences', showSnippet: false,
                name: function(s) { return (s.schema ? s.schema + '.' : '') + (s.name || ''); },
                line: function(s) { return PA.usageTable.firstLine({ operations: s.operations }); },
                badges: function(s) { return Object.keys(s.operations || {}); },
                source: function(s) { return selfSource(PA.usageTable.firstLine({ operations: s.operations })); }
            }
        });

        // Views referenced (a view is a table row whose objectType is VIEW / MATERIALIZED_VIEW).
        var views = tables.filter(function(t) {
            var ot = String(t.objectType || '').toUpperCase();
            return ot === 'VIEW' || ot === 'MATERIALIZED_VIEW';
        });
        if (views.length) g.push({
            title: 'Views referenced', id: 'ac-views', rows: views,
            opts: {
                showTitle: false, nameLabel: 'View', countLabel: 'views', showSnippet: false,
                name: function(v) { return v.name; },
                nameLink: function(v) { return "PA.estateViews.showView('" + PA.escJs(v.name || '') + "')"; },
                sub: function(v) { return { text: v.objectType || 'VIEW' }; },
                badges: function(v) { return Object.keys(v.operations || {}); }
            }
        });

        return g;
    },

    /**
     * The per-sub-tab case counts the Details labels render.
     *
     * The accordion-group counts come straight from `_groups` (so "which cases count" stays in one
     * place); the three facets that were never accordion groups are read off the node doc directly.
     * `triggers` is 0 here — it is not on the node doc and is filled in asynchronously by
     * {@link PA.allCases.triggerCount}.
     */
    counts: function(detail) {
        detail = detail || {};
        var byTitle = {};
        PA.allCases._groups(detail).forEach(function(g) { byTitle[g.title] = g.rows.length; });

        var stmts = detail.statementSummary || {};
        var stmtTotal = 0;
        Object.keys(stmts).forEach(function(k) { stmtTotal += (stmts[k] || 0); });

        var txn = detail.transactionScope || {};
        var txnCount = (txn.statements && txn.statements.length)
            || ((txn.commits || 0) + (txn.rollbacks || 0) + (txn.savepoints || 0));

        return {
            cursors: byTitle['Cursors'] || 0,
            dynamic: byTitle['Dynamic SQL'] || 0,
            tables: byTitle['Table operations'] || 0,
            joins: byTitle['Joins'] || 0,
            globals: byTitle['Globals'] || 0,
            calls: byTitle['Calls'] || 0,
            sequences: byTitle['Sequences'] || 0,
            views: byTitle['Views referenced'] || 0,
            statements: stmtTotal,
            transactions: txnCount,
            exHandlers: (detail.exceptionHandlers || []).length,
            triggers: 0
        };
    },

    /**
     * "Triggers fired": match this node's touched tables against the tables index's trigger list and
     * report the count to `cb`. Async and best-effort — a failure reports 0, never a guess.
     */
    triggerCount: function(detail, cb) {
        if (typeof cb !== 'function') return;
        if (!PA.api || !PA.api.getTableOperations || !detail) { cb(0); return; }
        var tables = detail.tables || detail.directTables || [];
        if (!tables.length) { cb(0); return; }
        var byName = {};
        tables.forEach(function(t) {
            var k = String(t.name || '').toUpperCase();
            if (k) byName[k] = t;
        });
        PA.api.getTableOperations().then(function(rows) {
            var fired = 0;
            (rows || []).forEach(function(r) {
                var k = String(r.tableName || '').toUpperCase();
                if (!byName[k]) return;
                fired += (r.triggers || []).length;
            });
            cb(fired);
        }).catch(function() { cb(0); });
    }
};
