window.PA = window.PA || {};

/**
 * Dynamic SQL report — lists every dynamic execution across the analysis:
 * Oracle EXECUTE IMMEDIATE / OPEN FOR, SQL Server EXEC(@sql) / sp_executesql /
 * SET @sql = '...'. Dialect-agnostic (data comes from node-detail "dynamicSql").
 *
 * Server-paged (PLAN-08 §4.3 step 12), through `PA.pager`'s page-source hook. Every dynamic
 * statement in the analysis carries its full SQL text, so the whole report was one of the larger
 * responses on this screen; only a page of it is fetched now. The search box (`q`) and the
 * Direct/Subtree/SubFlow selector (`scope`) travel with the request — searching or scoping the
 * page in the browser would silently apply to twenty-five rows.
 */
PA.dynamicSql = {
    // Retained for callers that test "is this loaded"; the rows are no longer held client-side.
    data: [],
    _q: '',
    _src: null,
    _summary: null,

    async load() {
        var container = document.getElementById('dynContainer');
        if (container) container.innerHTML = '<div class="empty-msg">Loading...</div>';
        this._src = PA.api.facetPageSource('dynamic-sql', this._scopeParams());
        PA.pager.register('dynamicSql', function () { PA.dynamicSql.render(); });
        await this._loadSummary();
        this.render();
    },

    /** The scope selector as request parameters. */
    _scopeParams() {
        var ctx = PA.context || {};
        if (!ctx.procId || PA._isPackageRoot()) return { scope: 'full' };
        return { scope: PA._scope, procId: PA._currentScopeId() };
    },

    /** The unscoped and in-scope totals for the header chips, which used to be array lengths. */
    async _loadSummary() {
        this._summary = await PA.api.facetSummary('dynamic-sql', this._scopeParams());
    },

    _searchTimer: null,

    onSearch() {
        this._q = ((document.getElementById('dynSearch') || {}).value || '');
        if (!this._src) { PA.pager.reset('dynamicSql'); this.render(); return; }
        // Debounced: this fires per keystroke, which cost nothing while the filter ran over an
        // array in memory and would now be one request per character.
        clearTimeout(this._searchTimer);
        this._searchTimer = setTimeout(function () {
            PA.dynamicSql._src.setParam('q', PA.dynamicSql._q.trim());
            PA.pager.reset('dynamicSql');
            PA.dynamicSql.render();
        }, 200);
    },

    clearFilters() {
        var s = document.getElementById('dynSearch');
        if (s) s.value = '';
        this._q = '';
        if (this._src) this._src.setParam('q', '');
        PA.pager.reset('dynamicSql');
        this.render();
    },

    applyScope() {
        if (this._src) {
            var params = this._scopeParams();
            this._src.setParam('scope', params.scope);
            this._src.setParam('procId', params.procId || '');
            PA.pager.reset('dynamicSql');
            this._loadSummary().then(function () { PA.dynamicSql.render(); });
        }
        this.render();
    },

    /** Strip the N'...' / '...' string wrapper and de-indent so multi-line dynamic SQL
     *  renders compact and left-aligned instead of shifted far right with big gaps. */
    _normalize(s) {
        if (!s) return '';
        s = s.trim().replace(/^N?'/, '').replace(/'\s*$/, '').replace(/''/g, "'");
        var lines = s.split('\n');
        while (lines.length && !lines[0].trim()) lines.shift();
        while (lines.length && !lines[lines.length - 1].trim()) lines.pop();
        var min = Infinity;
        lines.forEach(function (l) {
            if (l.trim()) { var m = (l.match(/^\s*/) || [''])[0].length; if (m < min) min = m; }
        });
        if (min !== Infinity && min > 0) lines = lines.map(function (l) { return l.slice(min); });
        return lines.join('\n').trim();
    },

    render() {
        var container = document.getElementById('dynContainer');
        if (!container) return;
        var esc = PA.esc;

        // The scope and the search are applied by the endpoint now; `slice` returns the page the
        // server sent and fetches it if it is not held yet.
        var pg = PA.pager.slice('dynamicSql', this._src || this.data || []);
        var sum = this._summary;
        var allTotal = sum ? sum.total : (this.data || []).length;
        var scopedTotal = sum ? sum.scopedTotal : allTotal;
        // `pg.total` is the count matching the current filters, or -1 when the server could not
        // count cheaply — in which case no "n shown" chip is rendered rather than a guess.
        var matched = pg.total;

        var total = document.getElementById('dynTotalCount');
        if (total) total.textContent = allTotal + ' dynamic';
        var filtered = document.getElementById('dynFilteredCount');
        if (filtered) {
            if (matched >= 0 && matched !== scopedTotal) {
                filtered.style.display = ''; filtered.textContent = matched + ' shown';
            } else { filtered.style.display = 'none'; }
        }

        if (pg.pageItems.length === 0) {
            container.innerHTML = (matched === 0)
                ? '<div class="empty-msg">No dynamic SQL found in this analysis</div>'
                : '<div class="empty-msg">Loading...</div>';
            return;
        }

        var page = pg.pageItems;
        // Full (un-truncated) SQL text for each row on this page, so the copy button copies
        // the complete statement even when the cell shows a scrollable/clamped view.
        PA.dynamicSql._pageTexts = [];
        // Pagination on TOP as well as bottom (was only rendered below the table).
        var html = (pg.controls ? '<div class="dyn-pager-top">' + pg.controls + '</div>' : '');
        html += '<table class="to-table" style="width:100%"><thead><tr>'
            + '<th>Procedure</th><th>Type</th><th>Line</th><th>SQL / Expression</th></tr></thead><tbody>';
        for (var i = 0; i < page.length; i++) {
            var d = page[i];
            var proc = '<span class="to-detail-proc" onclick="PA.showProcedure(\''
                + PA.escJs(d.procedureId || d.procedureName || '') + '\')">'
                + esc(d.procedureName || '') + '</span>';
            var line = d.lineNumber
                ? '<span class="to-detail-line" data-tip="Open source at line" onclick="event.stopPropagation(); PA.sourceView.openAtLine(\''
                    + PA.escJs(d.sourceFile || '') + '\', ' + d.lineNumber + ')">L' + d.lineNumber + '</span>'
                : '';
            var rawExpr = PA.dynamicSql._normalize(d.sqlExpression || '');
            var expr;
            if (rawExpr) {
                PA.dynamicSql._pageTexts[i] = rawExpr;
                // Scrollable code block (preserves whitespace) + a copy button for the full SQL.
                expr = '<div class="dyn-sql-wrap">'
                    + '<button class="btn btn-sm dyn-sql-copy" title="Copy SQL" '
                    + 'onclick="event.stopPropagation();PA.dynamicSql.copyExpr(' + i + ',this)">Copy</button>'
                    + '<pre class="dyn-sql-code">' + esc(rawExpr) + '</pre></div>';
            } else {
                // Executor rows (sp_executesql / EXEC(@sql)) run a string built in a SET row
                // above — they carry no inline SQL. Show an explanation instead of a blank cell.
                var t = (d.type || '').toUpperCase();
                var note = t.indexOf('SP_EXECUTESQL') >= 0
                    ? 'Executes a dynamic string via sp_executesql (built in a SET @sql above)'
                    : (t.indexOf('EXEC') >= 0
                        ? 'Executes a dynamic string (@sql built above)'
                        : '(no inline SQL captured for this execution)');
                expr = '<span style="color:var(--text-muted);font-style:italic">' + esc(note) + '</span>';
            }
            var uv = (d.usingVariables && d.usingVariables.length)
                ? '<div style="color:var(--text-muted);font-size:10px;margin-top:2px">USING: ' + esc(d.usingVariables.join(', ')) + '</div>'
                : '';
            html += '<tr class="to-row"><td class="nowrap">' + proc + '</td>'
                + '<td class="nowrap"><span class="ct-call-badge DYNAMIC" title="Dynamic SQL execution">' + esc(d.type || 'DYNAMIC') + '</span></td>'
                + '<td class="nowrap">' + line + '</td>'
                + '<td><span class="sql-cell">' + expr + '</span>' + uv + '</td></tr>';
        }
        html += '</tbody></table>' + pg.controls;
        container.innerHTML = html;
    },

    /** Copy the full (un-truncated) dynamic SQL for a row on the current page. */
    copyExpr(idx, btn) {
        var txt = (PA.dynamicSql._pageTexts || [])[idx] || '';
        var done = function () {
            if (!btn) return;
            var old = btn.textContent;
            btn.textContent = 'Copied';
            setTimeout(function () { btn.textContent = old; }, 1200);
        };
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(txt).then(done, done);
        } else {
            var ta = document.createElement('textarea');
            ta.value = txt; document.body.appendChild(ta); ta.select();
            try { document.execCommand('copy'); } catch (e) {}
            document.body.removeChild(ta); done();
        }
    }
};
