window.PA = window.PA || {};

/*
 * All cases — one collapsible section on the procedure detail view that lists EVERY analysis case
 * captured for THIS node, each with its real example (line + escaped SQL snippet), so a reader can
 * verify each point against the actual code rather than trusting a count.
 *
 * The source is the procedure's own node doc (`PA.currentDetail`, fetched by `PA.showProcedure` /
 * `PA.loadNodeDetail`): cursors, dynamic SQL, table operations, joins, globals, calls and sequences
 * are all already on it. Two cases are not on the node doc and are joined in asynchronously:
 *   - "Triggers fired"  — the per-table trigger list lives in the tables index
 *                         (`PA.api.getTableOperations()`), matched here by table name.
 *   - "Callers"         — `PA.api.getCallerTree(nodeId)`.
 * "Views referenced" is the node doc's own tables whose objectType is VIEW/MATERIALIZED_VIEW; the
 * chain is one click away (the view name opens the Estate → Views detail, which renders the chain).
 *
 * Applicable-only: a procedure with no cursors shows no Cursors group.
 */
PA.allCases = {
    /** Renders (or refreshes) the section for `detail`. Safe to call with no DOM (tests). */
    render: function(detail) {
        if (typeof document === 'undefined' || !detail) return;
        var view = document.getElementById('viewDetails');
        if (!view) return;
        var host = document.getElementById('allCasesSection');
        if (!host) {
            host = document.createElement('div');
            host.id = 'allCasesSection';
            view.insertBefore(host, view.firstChild);
        }

        var groups = PA.allCases._groups(detail);
        var total = 0;
        var body = '';
        for (var i = 0; i < groups.length; i++) {
            var g = groups[i];
            if (!g.rows || !g.rows.length) continue;
            total += g.rows.length;
            body += '<details class="ac-group" open><summary>' + PA.esc(g.title)
                + ' <span class="ac-gcount">(' + g.rows.length + ')</span></summary>'
                + PA.usageTable.attach(g.id, g.rows, g.opts)
                + '</details>';
        }
        // Async groups append here once their fetch resolves.
        body += '<div id="acAsync"></div>';

        var html = '<div class="ac-wrap">';
        html += '<div class="ac-head" onclick="PA.allCases.toggle()">'
            + '<span class="ac-caret" id="acCaret">▾</span>'
            + '<span class="ac-title">All cases</span>'
            + ' <span class="ac-count" id="acTotal">(' + total + ')</span></div>';
        html += '<div class="ac-body" id="acBody">'
            + (total ? body : body + '<div class="ac-empty">No analysis cases captured for this procedure.</div>')
            + '</div></div>';
        host.innerHTML = html;

        PA.allCases._loadTriggers(detail);
        PA.allCases._loadCallers(detail);
    },

    toggle: function() {
        var body = document.getElementById('acBody');
        if (!body) return;
        var hidden = body.style.display === 'none';
        body.style.display = hidden ? '' : 'none';
        var caret = document.getElementById('acCaret');
        if (caret) caret.textContent = hidden ? '▾' : '▸';
    },

    /** Appends an async group and bumps the total. */
    _appendAsync: function(title, id, rows, opts, detail) {
        var wrap = document.getElementById('acAsync');
        if (!wrap || !rows || !rows.length) return;
        var el = document.createElement('details');
        el.className = 'ac-group';
        el.open = true;
        el.innerHTML = '<summary>' + PA.esc(title) + ' <span class="ac-gcount">(' + rows.length + ')</span></summary>'
            + PA.usageTable.render(id, rows, opts);
        wrap.appendChild(el);
        PA.usageTable.mount(id, rows, opts);
        var totalEl = document.getElementById('acTotal');
        if (totalEl) {
            var n = parseInt((totalEl.textContent || '').replace(/[^0-9]/g, ''), 10) || 0;
            totalEl.textContent = '(' + (n + rows.length) + ')';
        }
    },

    /** The synchronous groups, all read straight off the node doc. */
    _groups: function(detail) {
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

    /** "Triggers fired": match this node's touched tables against the tables index's trigger list. */
    _loadTriggers: function(detail) {
        if (!PA.api || !PA.api.getTableOperations) return;
        var tables = detail.tables || detail.directTables || [];
        if (!tables.length) return;
        var byName = {};
        tables.forEach(function(t) {
            var k = String(t.name || '').toUpperCase();
            if (k) byName[k] = t;
        });
        PA.api.getTableOperations().then(function(rows) {
            var fired = [];
            (rows || []).forEach(function(r) {
                var k = String(r.tableName || '').toUpperCase();
                if (!byName[k]) return;
                (r.triggers || []).forEach(function(tr) {
                    fired.push({ trigger: tr, table: r.tableName, schema: r.tableSchema });
                });
            });
            if (!fired.length) return;
            PA.allCases._appendAsync('Triggers fired', 'ac-triggers', fired, {
                showTitle: false, nameLabel: 'Trigger', countLabel: 'triggers', showSnippet: false,
                name: function(f) { return f.trigger.name; },
                nameLink: function(f) { return "PA.estateTriggers.showTrigger('" + PA.escJs(f.trigger.name || '') + "')"; },
                sub: function(f) { return { text: (f.schema ? f.schema + '.' : '') + f.table }; },
                badges: function(f) { return [f.trigger.timing, f.trigger.event, f.trigger.status]; }
            }, detail);
        }).catch(function() {});
    },

    /** "Callers": the inverse call tree for this node. */
    _loadCallers: function(detail) {
        if (!PA.api || !PA.api.getCallerTree || !detail.nodeId) return;
        PA.api.getCallerTree(detail.nodeId).then(function(data) {
            if (data == null || (PA.failed && PA.failed(data))) return;
            var children = Array.isArray(data) ? data : (data && data.children ? data.children : []);
            if (!children.length) return;
            PA.allCases._appendAsync('Callers', 'ac-callers', children, {
                showTitle: false, nameLabel: 'Caller', countLabel: 'callers', showSnippet: false,
                name: function(n) { return n.name || n.id || n.nodeId || ''; },
                nameLink: function(n) { return "PA.showProcedure('" + PA.escJs(n.id || n.nodeId || n.name || '') + "')"; },
                line: function(n) { return n.callLineNumber || (n.callLines && n.callLines[0]) || null; },
                sub: function(n) { return { text: n.schemaName || n.schema || '' }; },
                badges: function(n) { return [n.callType, n.unitType || n.objectType]; }
            }, detail);
        }).catch(function() {});
    }
};

/* Section styling — scoped under .ac-wrap, themed through the shared variables. */
(function() {
    if (typeof document === 'undefined' || !document.head || typeof document.getElementById !== 'function') return;
    function install() {
        if (document.getElementById('all-cases-style')) return;
        var st = document.createElement('style');
        st.id = 'all-cases-style';
        st.textContent = [
            '.ac-wrap{margin:0 0 8px;border:1px solid var(--border-subtle);border-radius:var(--radius);background:var(--bg-card)}',
            '.ac-head{display:flex;align-items:center;gap:6px;padding:5px 10px;cursor:pointer;user-select:none}',
            '.ac-head:hover{background:var(--bg-hover,rgba(128,128,128,.06))}',
            '.ac-title{font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:var(--text)}',
            '.ac-count{font-size:11px;font-weight:700;color:var(--accent)}',
            '.ac-caret{font-size:10px;color:var(--text-muted)}',
            '.ac-body{padding:0 10px 8px;max-height:340px;overflow:auto}',
            '.ac-group{border-top:1px solid var(--border-subtle);padding:4px 0}',
            '.ac-group>summary{cursor:pointer;font-size:10px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:var(--text-muted);padding:3px 0}',
            '.ac-gcount{font-weight:700;color:var(--accent)}',
            '.ac-empty{padding:8px 0;color:var(--text-muted);font-size:11px;font-style:italic}'
        ].join('');
        document.head.appendChild(st);
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', install);
    else install();
})();
