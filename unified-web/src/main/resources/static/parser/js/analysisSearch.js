/**
 * PA.analysisSearch — one search across a whole analysis (§B13).
 *
 * <h2>The question this answers</h2>
 *
 * "Is there any UPDATE on this table for this field in this proc?" Before this, answering it meant
 * opening procedures one at a time and reading them, because nothing served carried the pairing of
 * a statement with the columns it touches. It is now one query:
 *
 *   op:UPDATE table:ERP_STOCK_LEVEL col:QTY* proc:USP_RESERVE
 *
 * <h2>Three things this is careful about</h2>
 *
 * <b>Saying what it understood.</b> The parsed query comes back from the server and is shown as
 * chips. A search that quietly reinterprets what someone typed and then reports "no results" gives
 * them no way to tell a real absence from a misread query.
 *
 * <b>Counting honestly.</b> The header shows the server's total over every match, never the number
 * of rows on screen. Showing 50 of 231 while the heading says "50" would be a lie by omission.
 *
 * <b>Never presenting silence as an answer.</b> Zero results is stated as zero, with the
 * interpretation beside it. An analysis with no statement index says so rather than answering
 * "none" to a question it cannot evaluate — the difference between "there is no UPDATE" and "this
 * analysis was never indexed" matters to whoever is about to change that table.
 */
window.PA = window.PA || {};

PA.analysisSearch = {

    _seq: 0,          // stale-response guard: the most prone panel here, per §B5
    _debounce: null,
    _last: null,

    /** Opens the modal and focuses the box. */
    open(prefill) {
        const overlay = document.getElementById('asModal');
        if (!overlay) return;
        overlay.style.display = '';
        const box = document.getElementById('asQuery');
        if (box) {
            if (prefill) box.value = prefill;
            box.focus();
            box.select();
        }
        if (prefill) PA.analysisSearch.run();
        else PA.analysisSearch._renderHelp();
    },

    close(event) {
        if (event && event.target !== event.currentTarget) return;
        const overlay = document.getElementById('asModal');
        // A closing panel must not keep a query it has stopped applying; see
        // SearchQuery.resetWithin in shared/ui-busy.js.
        if (window.SearchQuery) SearchQuery.resetWithin(overlay);
        if (overlay) overlay.style.display = 'none';
    },

    /** Debounced so a request is not issued per keystroke over a 5,000-row index. */
    onInput() {
        clearTimeout(PA.analysisSearch._debounce);
        PA.analysisSearch._debounce = setTimeout(PA.analysisSearch.run, 250);
    },

    async run() {
        const box = document.getElementById('asQuery');
        const body = document.getElementById('asBody');
        if (!box || !body) return;

        // Trimmed like every other search here; the padding marker tells the user their paste
        // carried spaces rather than leaving them to wonder.
        const q = SearchQuery.trim(box.value);
        SearchQuery.markPadding(box, box.value);
        if (!q) { PA.analysisSearch._renderHelp(); return; }

        const mine = ++PA.analysisSearch._seq;
        body.innerHTML = '<div class="as-msg">Searching…</div>';

        let res, data;
        try {
            res = await fetch(`/api/parser/analyses/${encodeURIComponent(PA.api._analysisName)}`
                            + `/search?q=${encodeURIComponent(q)}&limit=50`);
            data = await res.json();
        } catch (e) {
            if (mine !== PA.analysisSearch._seq) return;
            body.innerHTML = '<div class="as-msg as-err">Search failed: ' + PA.esc(String(e)) + '</div>';
            return;
        }
        // A slower earlier request must not overwrite a newer answer.
        if (mine !== PA.analysisSearch._seq) return;

        if (res.status === 400) { PA.analysisSearch._renderBadQuery(data); return; }
        PA.analysisSearch._last = data;
        PA.analysisSearch._render(data);
    },

    _renderHelp() {
        const body = document.getElementById('asBody');
        if (!body) return;
        body.innerHTML =
            '<div class="as-help">'
          + '<div class="as-help-title">Search every statement in this analysis</div>'
          + '<table class="as-help-tbl">'
          + '<tr><td><code>op:UPDATE table:ERP_STOCK_LEVEL col:QTY*</code></td>'
          +     '<td>is anything updating that column on that table?</td></tr>'
          + '<tr><td><code>proc:USP_RESERVE</code></td><td>everything one procedure touches</td></tr>'
          + '<tr><td><code>op:UPDATE op:DELETE</code></td><td>same qualifier repeats = either</td></tr>'
          + '<tr><td><code>table:ORDERS !schema:LEGACY</code></td><td><code>!</code> excludes</td></tr>'
          + '<tr><td><code>sql:"order by"</code></td><td>quote a phrase</td></tr>'
          + '</table>'
          + '<div class="as-help-note">Qualifiers: '
          + '<code>table</code> <code>proc</code> <code>col</code> <code>op</code> '
          + '<code>schema</code> <code>file</code> <code>sql</code> <code>line</code>. '
          + 'Bare words match any of them and combine with AND. '
          + 'Trailing <code>*</code> is a prefix match — there is no regex.</div>'
          + '</div>';
    },

    _renderBadQuery(d) {
        const body = document.getElementById('asBody');
        // A typo must never look like "no such data". This is the whole reason the API 400s.
        body.innerHTML =
            '<div class="as-msg as-err"><strong>That query did not parse.</strong><ul>'
          + (d.errors || []).map(e => '<li>' + PA.esc(e) + '</li>').join('')
          + '</ul><div class="as-help-note">Try: <code>' + PA.esc(d.example || '') + '</code></div></div>';
        const hdr = document.getElementById('asHeader');
        if (hdr) hdr.textContent = '';
    },

    _render(d) {
        const body = document.getElementById('asBody');
        const hdr = document.getElementById('asHeader');

        if (d.indexed === false) {
            // Not the same as zero. Answering "none" to a question we cannot evaluate would be a
            // confident wrong answer to whoever is about to change that table.
            hdr.innerHTML = '<span class="as-warn">not indexed</span>';
            body.innerHTML = '<div class="as-msg as-err">' + PA.esc(d.message || '')
                           + '<div class="as-help-note">The statement index is written when an '
                           + 'analysis runs. This one predates it, so this search cannot say '
                           + 'whether such statements exist — only that it does not know.</div></div>';
            return;
        }

        // Chips: the query as the ENGINE understood it, not as it was typed.
        const chips = (d.echo || []).map(e =>
            '<span class="as-chip">' + PA.esc(e) + '</span>').join('');

        const total = d.total || 0;
        hdr.innerHTML =
            '<span class="as-total">' + total + '</span> '
          + '<span class="as-total-lbl">' + (total === 1 ? 'statement' : 'statements') + '</span>'
          + (d.truncated ? '<span class="as-shown">showing first ' + d.returned + '</span>' : '')
          + '<span class="as-of">of ' + (d.totalStatements || 0) + ' in this analysis</span>'
          + '<span class="as-chips">' + chips + '</span>';

        if (total === 0) {
            // A stated zero, with the interpretation beside it, so the user can see whether the
            // question was understood before concluding the data is absent.
            let extra = '';
            const usedCol = (d.echo || []).some(e => e.startsWith('col '));
            if (usedCol && d.statementsWithColumns === 0) {
                extra = '<div class="as-help-note">No statement in this analysis has column detail, '
                      + 'so a <code>col:</code> query cannot match here — this is not evidence that '
                      + 'the column is unused.</div>';
            } else if (usedCol && d.statementsWithColumns < d.totalStatements) {
                extra = '<div class="as-help-note">Only ' + d.statementsWithColumns + ' of '
                      + d.totalStatements + ' statements in this analysis carry column detail, so a '
                      + '<code>col:</code> query can only see those.</div>';
            }
            body.innerHTML = '<div class="as-msg">No statement matches that query.' + extra + '</div>';
            return;
        }

        body.innerHTML =
            PA.analysisSearch._facetsHtml(d.facets)
          + '<table class="to-table as-table"><thead><tr>'
          + '<th>Operation</th><th>Table</th><th>Procedure</th><th>Line</th><th>Columns</th><th>Statement</th>'
          + '</tr></thead><tbody>'
          + (d.rows || []).map(PA.analysisSearch._rowHtml).join('')
          + '</tbody></table>'
          + (d.truncated
              ? '<div class="as-more">Showing ' + d.returned + ' of ' + total
                + ' — narrow the query to see the rest.</div>'
              : '');
    },

    _facetsHtml(f) {
        if (!f) return '';
        // Counted server-side over every match, so these stay true while the table shows a page.
        const group = (label, obj, max) => {
            const es = Object.entries(obj || {}).sort((a, b) => b[1] - a[1]).slice(0, max);
            if (!es.length) return '';
            return '<div class="as-facet"><span class="as-facet-lbl">' + label + '</span>'
                 + es.map(([k, v]) =>
                     '<span class="as-facet-pill">' + PA.esc(k)
                   + '<span class="as-facet-n">' + v + '</span></span>').join('')
                 + '</div>';
        };
        return '<div class="as-facets">'
             + group('operations', f.operation, 8)
             + group('tables', f.tableName, 6)
             + group('procedures', f.nodeName, 6)
             + '</div>';
    },

    _rowHtml(r) {
        const esc = PA.esc;
        const matched = r.matchedColumns || [];
        const cols = (r.columns || []);
        // The column that answered is marked. On a statement touching twenty columns, "this row
        // matched" is not by itself an answer to "which field".
        const colHtml = r.columnsKnown === false
            ? '<span class="as-unknown" title="This analysis predates column capture — not the '
              + 'same as having no columns">not captured</span>'
            : (cols.length === 0 ? '<span class="as-dim">—</span>'
               : cols.slice(0, 6).map(c =>
                   '<span class="as-col' + (matched.includes(c) ? ' as-col-hit' : '') + '">'
                 + esc(c) + '</span>').join('')
                 + (cols.length > 6 ? '<span class="as-dim"> +' + (cols.length - 6) + '</span>' : ''));

        const sql = (r.sqlText || '').replace(/\s+/g, ' ').trim();
        const sqlShort = sql.length > 110 ? sql.substring(0, 110) + ' …' : sql;

        return '<tr class="as-row">'
          + '<td><span class="op-badge ' + esc(r.operation || '') + '">' + esc(r.operation || '?') + '</span></td>'
          + '<td><span class="as-table-name" onclick="PA.tableDetail.open(\'' + PA.escJs(r.tableName || '')
              + '\',\'' + PA.escJs(r.schema || '') + '\')">' + esc(r.tableName || '') + '</span></td>'
          + '<td><span class="to-detail-proc" onclick="PA.analysisSearch._goProc(\''
              + PA.escJs(r.nodeId || '') + '\')">' + esc(r.nodeName || '') + '</span></td>'
          + '<td>' + (r.line
              ? '<span class="to-detail-line" onclick="PA.analysisSearch._goLine(\''
                + PA.escJs(r.nodeId || '') + '\',' + r.line + ')">L' + r.line + '</span>'
              : '<span class="as-dim">—</span>') + '</td>'
          + '<td class="as-cols">' + colHtml + '</td>'
          + '<td class="as-sql" title="' + PA.escAttr(sql) + '">' + esc(sqlShort)
          + (r.sqlTruncated ? '<span class="as-dim"> (truncated)</span>' : '') + '</td>'
          + '</tr>';
    },

    _goProc(nodeId) {
        PA.analysisSearch.close();
        PA.showProcedure(nodeId);
    },

    _goLine(nodeId, line) {
        PA.analysisSearch.close();
        if (PA.sourceView && PA.sourceView.openNodeAtLine) PA.sourceView.openNodeAtLine(nodeId, line);
        else PA.showProcedure(nodeId);
    }
};

// "/" focuses the search, the convention everywhere else. Ignored while typing in a field, so it
// does not swallow a slash someone is entering into another box.
//
// TASK-REGISTRY §277 — redirected to #globalSearch (Global Search) alongside the topbar button's
// own redirect; see that change's comment for the honest scope note (statement-text vs. object-name
// matching). The Escape-to-close branch is left as-is since #asModal (this feature's own overlay)
// can never be open anymore via either entry point, so it can never fire — dead, not broken.
document.addEventListener('keydown', function (e) {
    if (e.key === '/' && !/^(INPUT|TEXTAREA|SELECT)$/.test((e.target.tagName || ''))) {
        // #globalSearch no longer exists (topbar input replaced by a button that opens the find
        // modal directly) — open the modal itself instead of focusing a dead element.
        e.preventDefault();
        PA.search.open();
    } else if (e.key === 'Escape') {
        const overlay = document.getElementById('asModal');
        if (overlay && overlay.style.display !== 'none') PA.analysisSearch.close();
    }
});
