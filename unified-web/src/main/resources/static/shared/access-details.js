/**
 * AccessDetails — the "Access Details (N)" panel, as a real table.
 *
 * <h2>What was wrong</h2>
 *
 * It rendered as a flat, unbounded `<div>` list: one line per access, no header, no sort, no filter
 * and no cap. N reaches 1,385 on real analyses. Every row was built and inserted at once, so opening
 * a busy table locked the tab, and once open the list could not be narrowed — finding "the UPDATEs
 * from one procedure" meant scrolling past a thousand rows that were not.
 *
 * <h2>Why this is shared</h2>
 *
 * `parser/js/tableOps.js` and `plsql/js/tableOps.js` each had their own copy of the renderer, and
 * they had already drifted. The same panel is also needed by the table popup modal. A third copy
 * would guarantee they diverge further, so the row-building lives here once and both pages call it.
 *
 * Paging, sorting and search come from `PA.tf`, which already does exactly this at 22 other call
 * sites at 50 rows a page — this is a conversion, not an invention. Only `PA.tf`'s common API is
 * used (`init`/`setSearch`/`filter`/`sort`/`render`, and the `{id}-tbody` / `{id}-pager` element
 * contract), because the two pages' copies of the framework have themselves diverged: `parser`'s
 * has server-paging, `plsql`'s has column filters. Those signatures and that contract are identical
 * in both, and nothing outside them is touched here.
 *
 * <h2>Fields that exist, and one that does not</h2>
 *
 * Rows are synthesised in the browser from the served `usedBy[].operations` map (`parser/js/api.js`),
 * so each carries exactly: `operation`, `procedureName`, `procedureId`, `lineNumber`. Two branches
 * in the old renderer read fields that are never populated on this path — `whereFilters` (nothing
 * writes it) and `sourceFile` (always the empty string). They rendered for nobody. They are not
 * reproduced here; the line link uses `openNodeAtLine`, which is the branch that actually ran.
 */
window.AccessDetails = (function () {

    var PAGE_SIZE = 50;   // matches PA.tf's 22 other call sites

    /** Per-panel state, keyed by the PA.tf id. Kept so pills and search can re-filter. */
    var panels = {};

    function esc(v) {
        return (window.PA && PA.esc) ? PA.esc(v) : String(v == null ? '' : v);
    }
    function escJs(v) {
        return (window.PA && PA.escJs) ? PA.escJs(v) : String(v == null ? '' : v);
    }
    /** Attribute-safe. Local fallback for the same reason esc and escJs have one. */
    function escAttr(v) {
        return (window.PA && PA.escAttr)
            ? PA.escAttr(v)
            : String(v == null ? '' : v).replace(/"/g, '&quot;');
    }

    /** One column's search box. */
    function _colFilter(id, key, placeholder) {
        return '<input type="search" class="ad-cf" id="' + id + '-cf-' + key + '"'
             + ' placeholder="' + placeholder + '" onclick="event.stopPropagation()"'
             + ' oninput="AccessDetails.colFilter(\'' + escJs(id) + '\',\'' + key + '\',this.value)">';
    }

    /**
     * Builds the panel markup. Call {@link mount} once it is in the DOM.
     *
     * @param {string} id      unique PA.tf id for this panel
     * @param {Array}  details the access rows
     * @param {Object} opts    {fullCount, scoped} — fullCount is the pre-scope total, so a scoped
     *                         view can say so rather than looking like the whole truth
     * @returns {string} HTML
     */
    function render(id, details, opts) {
        opts = opts || {};
        details = details || [];
        if (details.length === 0) {
            return '<div style="padding:8px 0;color:var(--text-muted);font-size:11px">'
                 + 'No access details available</div>';
        }

        var ops = {};
        for (var i = 0; i < details.length; i++) {
            var o = details[i].operation || '?';
            ops[o] = (ops[o] || 0) + 1;
        }
        var opNames = Object.keys(ops).sort();

        // The count in the title is the honest one: how many rows this panel holds. When a scope
        // filter is active and hiding some, that is stated rather than left for the reader to
        // notice — an unqualified smaller number reads as "this is all of them".
        var scopeNote = (opts.scoped && opts.fullCount > details.length)
            ? ' <span style="color:var(--text-muted);font-weight:400">of ' + opts.fullCount
              + ' total (scoped)</span>'
            : '';

        var h = '<div class="to-detail-section ad-panel">';
        h += '<div class="to-detail-section-title">Access Details (' + details.length + ')'
           + scopeNote + '</div>';

        // Controls: free-text search plus one toggle per operation actually present. Pills are
        // generated from the data rather than a fixed list, so a dialect with an operation the
        // others lack still gets a filter for it.
        h += '<div class="ad-controls" onclick="event.stopPropagation()">';
        h += '<input type="search" class="ad-search" id="' + id + '-q" placeholder="Filter '
           + details.length + ' accesses — procedure, operation or line"'
           + ' oninput="AccessDetails.search(\'' + escJs(id) + '\', this.value)">';
        h += '<span class="ad-pills">';
        for (var p = 0; p < opNames.length; p++) {
            h += '<button type="button" class="ad-pill op-badge ' + esc(opNames[p]) + ' active"'
               + ' data-op="' + esc(opNames[p]) + '"'
               + ' onclick="AccessDetails.togglePill(\'' + escJs(id) + '\',\'' + escJs(opNames[p]) + '\',this)">'
               + esc(opNames[p]) + ' <span class="ad-pill-n">' + ops[opNames[p]] + '</span></button>';
        }
        h += '</span>';
        h += '<span class="ad-shown" id="' + id + '-shown"></span>';
        // Sep05-08: how much detail this dialect actually yielded, stated rather than left to be
        // inferred from blank cells. Measured coverage differs sharply between dialects (Oracle
        // 85%, PostgreSQL 72%, DB2 93%), so a reader who assumes "blank means the table is not
        // used that way" is wrong on one dialect and right on another. Filled in after enrichment.
        h += '<span class="ad-cov" id="' + id + '-cov"></span>';
        h += '</div>';

        h += '<table class="to-table ad-table" id="' + id + '">';
        h += '<thead><tr>'
           + '<th data-sort-col="0" onclick="event.stopPropagation();PA.tf.sort(\'' + escJs(id) + '\',0)">Operation</th>'
           + '<th data-sort-col="1" onclick="event.stopPropagation();PA.tf.sort(\'' + escJs(id) + '\',1)">Procedure</th>'
           + '<th data-sort-col="2" onclick="event.stopPropagation();PA.tf.sort(\'' + escJs(id) + '\',2)">Line</th>'
           + '<th>Columns</th>'
           + '<th>Predicate / value</th>'
           + '</tr>'
           // A search per column. One global box could not answer "SELECTs on QTY_* from this
           // proc" — the columns are different questions, so they get different boxes.
           + '<tr class="ad-filter-row">'
           + '<th>' + _colFilter(id, 'op', 'operation') + '</th>'
           + '<th>' + _colFilter(id, 'proc', 'procedure') + '</th>'
           + '<th>' + _colFilter(id, 'line', 'line') + '</th>'
           + '<th>' + _colFilter(id, 'cols', 'column') + '</th>'
           + '<th>' + _colFilter(id, 'pred', 'predicate') + '</th>'
           + '</tr></thead>';
        h += '<tbody id="' + id + '-tbody"></tbody></table>';
        // `pagination-bar`, not a class of our own: it is what the other 44 pagers in this app use
        // and it is styled on both pages. Inventing `tf-pager` here is exactly what caused the
        // overlap — a layout-critical element emitted with no rule anywhere.
        h += '<div id="' + id + '-pager" class="pagination-bar ad-pager"></div>';
        h += '</div>';
        return h;
    }

    /**
     * One access row.
     *
     * <p>The two pages differ in how a line opens — the parser page resolves the source from the
     * node id ({@code sourceView.openNodeAtLine}) because its per-row {@code sourceFile} is always
     * empty, while the PL/SQL page has a real {@code sourceFile} and uses {@code codeModal}. That
     * difference is passed in as {@code jumpFn} rather than guessed at here; picking one would have
     * silently broken the line links on the other page.
     *
     * <p>Claude-verified rows are marked as they were before. On the PL/SQL page these arrive with
     * {@code sourceFile === 'claude-verified'}, which is a marker and not a path, so they get the
     * badge and no jump.
     */
    function makeRowRenderer(id) {
        return function renderRow(d, idx, e) {
            var op = d.operation || '?';
            var isClaude = d.sourceFile === 'claude-verified';
            var st = panels[id] || {};

            var html = '<tr class="ad-row' + (isClaude ? ' claude-found' : '') + '">';
            html += '<td>';
            if (isClaude) html += '<span class="claude-badge" title="Discovered by Claude AI">AI</span> ';
            html += '<span class="op-badge ' + e(op) + '">' + e(op) + '</span>';
            // Sep05-03: what kind of access. A read inside a three-table join used to look
            // identical to a standalone read; this is derived from evidence on the operation and is
            // absent when there was none, rather than defaulting to something plausible.
            if (d._accessKind && d._accessKind !== op) {
                html += '<span class="ad-kind" title="How this table was used">'
                      + e(d._accessKind.replace(/_/g, ' ').toLowerCase()) + '</span>';
            }
            html += '</td>';
            html += '<td><span class="to-detail-proc" onclick="event.stopPropagation(); PA.showProcedure(\''
                  + escJs(d.procedureId || d.procedureName || '') + '\')">'
                  + e(d.procedureName || '') + '</span></td>';

            var jump = (!isClaude && st.jumpFn) ? st.jumpFn(d) : null;
            if (d.lineNumber && jump) {
                html += '<td><span class="to-detail-line" data-tip="Open source at this line"'
                      + ' onclick="event.stopPropagation(); ' + jump + '">L' + d.lineNumber + '</span></td>';
            } else {
                html += '<td style="color:var(--text-muted)">'
                      + (d.lineNumber ? 'L' + d.lineNumber : '-') + '</td>';
            }
            // Column provenance, joined in by enrich() from the statement index. Until that
            // resolves the cell reads "…" rather than "—": an empty dash would state that this
            // statement touches no columns, which is a claim we have not made yet.
            var cols = d._columns;
            if (cols === undefined) {
                html += '<td class="ad-cols"><span class="ad-dim">…</span></td>';
            } else if (cols === null) {
                html += '<td class="ad-cols"><span class="ad-unknown" '
                      + 'title="This analysis predates column capture — not the same as having no '
                      + 'columns">not captured</span></td>';
            } else if (cols.length === 0) {
                // An empty list is not automatically "no columns". columnsParsed === false means
                // ANSI parsing of that statement failed — common for Oracle MERGE, which came back
                // at 0% column coverage — so the honest cell is "not parsed", not a dash.
                html += d._columnsParsed === false
                    ? '<td class="ad-cols"><span class="ad-unknown" title="This statement could not '
                      + 'be parsed for column detail, so its columns are unknown — not absent">'
                      + 'not parsed</span></td>'
                    : '<td class="ad-cols"><span class="ad-dim">—</span></td>';
            } else {
                // Sep05-06: show what each column DID, not just its name.
                //
                // usage (SELECT/SET/INSERT), value provenance (LITERAL/BIND/SEQUENCE/EXPRESSION) and
                // the qualifier were all in the served payload already and rendered nowhere — so a
                // written column looked identical to a read one, and `id` on a three-table join was
                // unattributable. The detail map is keyed by name so a name with no detail still
                // renders as a plain chip rather than disappearing.
                var byName = {};
                (d._columnDetail || []).forEach(function (c) { if (c.name) byName[c.name.toUpperCase()] = c; });
                html += '<td class="ad-cols">'
                      + cols.slice(0, 5).map(function (c) {
                            var det = byName[String(c).toUpperCase()];
                            var cls = 'ad-col';
                            var tip = '';
                            if (det) {
                                if (det.usage === 'SET' || det.usage === 'INSERT') cls += ' ad-col-write';
                                tip = [det.qualifier ? 'alias ' + det.qualifier : '',
                                       det.usage ? 'usage ' + det.usage : '',
                                       det.valueSource ? 'value from ' + det.valueSource : '',
                                       det.expression ? '= ' + det.expression : '',
                                       det.into ? 'INTO ' + det.into : '']
                                      .filter(Boolean).join(' · ');
                            }
                            return '<span class="' + cls + '"'
                                 + (tip ? ' title="' + escAttr(tip) + '"' : '') + '>'
                                 + e(c)
                                 + (det && det.usage ? '<span class="ad-usage">' + e(det.usage) + '</span>' : '')
                                 + '</span>';
                        }).join('')
                      + (cols.length > 5 ? '<span class="ad-dim"> +' + (cols.length - 5) + '</span>' : '')
                      + '</td>';
            }
            // Sep05-01/03/04: what the access actually DID.
            //
            // A predicate column never becomes a ColumnUsage, so for a table that is mostly
            // filtered or joined on this cell carries the only column information there is — the
            // reason 475 accesses on ERP_PRODUCT showed an empty Columns cell.
            //
            // The scope is shown, not hidden: WhereFilter has no qualifier, so on a join the
            // predicate belongs to the statement rather than provably to this table. Saying
            // "statement" is the difference between informing and over-claiming.
            var det = [];
            var preds = d._predicates;
            if (preds && preds.length) {
                var shown = preds.slice(0, 3).map(function (p) {
                    return '<span class="ad-pred">' + e(p.column || '?')
                         + ' <span class="ad-op">' + e(p.operator || '') + '</span> '
                         + e(p.value == null ? '?' : p.value) + '</span>';
                }).join('');
                if (preds.length > 3) shown += '<span class="ad-dim"> +' + (preds.length - 3) + '</span>';
                if (d._predicateScope === 'statement') {
                    shown += '<span class="ad-scope" title="This statement joins several tables and '
                           + 'a predicate carries no table qualifier, so it belongs to the statement '
                           + 'rather than provably to this table">stmt</span>';
                }
                det.push(shown);
            }
            // The assignment / selected expression, and the INTO target.
            var cd = d._columnDetail;
            if (cd && cd.length) {
                var extra = cd.filter(function (c) { return c.expression || c.into; })
                              .slice(0, 2).map(function (c) {
                    // Sep05-10: a nameless entry is an expression select item (COUNT(*), NVL(...)),
                    // not a column assignment, so it reads as the expression itself. Rendering
                    // it as "= COUNT(*)" would imply something was assigned to a column.
                    var bits = c.name ? e(c.name) : '';
                    if (c.usage) bits += '<span class="ad-op">' + (bits ? ' ' : '') + e(c.usage) + '</span>';
                    if (c.expression) bits += (c.name ? ' = ' : ' ') + e(c.expression);
                    if (c.into) bits += '<span class="ad-into"> INTO ' + e(c.into) + '</span>';
                    return '<span class="ad-pred">' + bits + '</span>';
                }).join('');
                if (extra) det.push(extra);
            }
            // Sep05-02: the join and its ON condition — measured at zero rows before, across all
            // five dialects, because the producer never emitted op.joins at all.
            var jns = d._joins;
            if (jns && jns.length) {
                var jtxt = jns.slice(0, 2).map(function (j) {
                    var b = '<span class="ad-op">' + e(j.type || 'JOIN') + '</span> ';
                    if (j.right) b += e(j.right);
                    if (j.on) b += ' <span class="ad-dim">ON</span> ' + e(j.on);
                    return '<span class="ad-pred">' + b + '</span>';
                }).join('');
                if (jns.length > 2) jtxt += '<span class="ad-dim"> +' + (jns.length - 2) + '</span>';
                det.push(jtxt);
            }
            // Sep05-09: a CREATE TABLE names its columns; that is the whole detail for a DDL row.
            var ddl = d._ddl;
            if (ddl && ddl.length) {
                var dtxt = ddl.slice(0, 2).map(function (x) {
                    var b = '<span class="ad-op">' + e(x.action || 'DDL') + '</span> ';
                    if (x.columns && x.columns.length) {
                        b += e(x.columns.slice(0, 4).join(', '));
                        if (x.columns.length > 4) b += '<span class="ad-dim"> +' + (x.columns.length - 4) + '</span>';
                    } else if (x.objectName) { b += e(x.objectName); }
                    return '<span class="ad-pred">' + b + '</span>';
                }).join('');
                if (dtxt) det.push(dtxt);
            }
            html += '<td class="ad-det">'
                  + (det.length ? det.join('') : '<span class="ad-dim">—</span>')
                  + '</td>';
            html += '</tr>';
            return html;
        };
    }

    /**
     * Fills in each access row's columns from the statement index.
     *
     * <p>The rows themselves are synthesised in the browser from `usedBy[].operations`, which is a
     * `{OP: [lines]}` map and carries no column attribution at all — so the panel could show which
     * procedure writes a table but never which field. The statement index has that pairing, and is
     * joined here on (nodeId, operation, line), the three things both sides agree on.
     *
     * <p>Failure is silent by design but not misleading: on any error the cells fall back to
     * "not captured" rather than to an empty list, because "we could not find out" and "there are
     * no columns" are different statements and only one of them is true.
     */
    /**
     * Fetches the statement rows for one table, keyed by (nodeId, operation, line).
     *
     * <p>Exposed because two screens need the same join — the inline Access Details panel and the
     * table popup. Two copies of a join key would drift, and the two screens would then disagree
     * about the same statement, which is worse than one of them being sparse.
     *
     * @returns the keyed map, or null when the analysis has no statement index or the request failed
     */
    async function fetchDetail(analysisName, tableName) {
        if (!analysisName || !tableName) return null;
        try {
            var r = await fetch('/api/parser/analyses/' + encodeURIComponent(analysisName)
                              + '/search?q=' + encodeURIComponent('table:' + tableName) + '&limit=1000');
            if (!r.ok) return null;
            var d = await r.json();
            if (d.indexed === false) return null;
            var want = String(tableName).toUpperCase();
            var byKey = {};
            (d.rows || []).forEach(function (row) {
                // `table:` is a CONTAINS match server-side (StatementSearcher.field), so the
                // response for `table:ORDERS` also carries rows of ORDERS_ARCHIVE, CLIENT_ORDERS,
                // … The join below is keyed per (nodeId, operation, line) — and two tables read by
                // the same statement line (a JOIN) collide on exactly that key with
                // last-write-wins. So only an exact, case-insensitive name match may enter the
                // map: another table's row here is how this panel showed someone else's columns
                // and predicates. A row with no tableName at all is skipped for the same reason —
                // unattributable evidence is not evidence.
                var rowTable = String(row.tableName || '').toUpperCase();
                if (rowTable !== want) return;
                byKey[(row.nodeId || '') + '|' + (row.operation || '') + '|' + row.line] = row;
            });
            return byKey;
        } catch (e) {
            return null;
        }
    }

    /** Copies the enriched fields onto one access row. Shared so both screens agree. */
    function applyDetail(x, m) {
        x._columns = m ? (m.columnsKnown === false ? null : (m.columns || [])) : null;
        x._columnsParsed = m ? m.columnsParsed : undefined;
        x._predicates = m ? m.predicates : undefined;
        x._predicateScope = m ? m.predicateScope : undefined;
        x._columnDetail = m ? m.columnDetail : undefined;
        x._ddl = m ? m.ddl : undefined;
        x._joins = m ? m.joins : undefined;
        x._accessKind = m ? m.accessKind : undefined;
        x._sqlText = m ? m.sqlText : undefined;
    }

    /** The key both screens use to match an access row to its statement. */
    function detailKey(x) {
        return (x.procedureId || '') + '|' + (x.operation || '') + '|' + x.lineNumber;
    }

    /**
     * Whether the panel `id` still belongs to THESE detail rows. Detail-panel ids are derived
     * from the outer row's index (`ad-p-<idx>` / `ad-s-<idx>`), so a re-sort or page change can
     * reassign an id to a different table while this panel's fetch is still in flight — the same
     * id, different `PA.tf` state. Repainting then would render one table's panel over another's;
     * `mount` re-inits the state with the new `details` array, so identity of the state's own
     * `data` is the exact test. (The mutations above stay harmless either way: they only touch
     * THIS panel's row objects, which the new panel never shares.)
     */
    function _panelIsCurrent(id, details) {
        var s = PA.tf && PA.tf._state ? PA.tf._state[id] : null;
        return !!s && s.data === details;
    }

    async function enrich(id, details, analysisName, tableName) {
        if (!analysisName || !tableName || !details || !details.length) { _markUnknown(id, details); return; }
        var byKey = await fetchDetail(analysisName, tableName);
        if (!byKey) { _markUnknown(id, details); return; }
        try {
            details.forEach(function (x) {
                applyDetail(x, byKey[detailKey(x)]);
            });
        } catch (e) {
            _markUnknown(id, details);
            return;
        }
        _updateCoverage(id, details);
        // Re-render only if this panel is still open AND still the panel these rows belong to;
        // the user may have collapsed the row, or the id may now belong to a different table.
        if (document.getElementById(id + '-tbody') && _panelIsCurrent(id, details)) PA.tf.render(id);
    }

    function _markUnknown(id, details) {
        (details || []).forEach(function (x) { x._columns = null; });
        _updateCoverage(id, details, true);
        if (document.getElementById(id + '-tbody') && _panelIsCurrent(id, details)) PA.tf.render(id);
    }

    /**
     * Sep05-08 — states how much of this table's detail was actually captured.
     *
     * <p>The three buckets are deliberately kept apart, because collapsing them is the confusion
     * this whole series set out to remove:
     *
     * <ul>
     *   <li><b>with detail</b> — something is on the row.</li>
     *   <li><b>not parsed</b> — the SQL did not parse, so we do not know. A blank cell here means
     *       our failure, and saying so is the difference between informing and misleading.</li>
     *   <li><b>nothing to show</b> — it parsed and genuinely carries no column, predicate or join.
     *       A blank cell here is a truthful answer about the code.</li>
     * </ul>
     *
     * <p>It is computed from the rows in hand rather than from a server-side counter, so the number
     * cannot disagree with what the reader is looking at.
     */
    function _updateCoverage(id, details, unknown) {
        var el = document.getElementById(id + '-cov');
        if (!el) return;
        var list = details || [];
        if (unknown || !list.length) {
            el.textContent = list.length ? 'detail unavailable' : '';
            el.className = 'ad-cov ad-cov-warn';
            return;
        }
        var withDetail = 0, notParsed = 0, quiet = 0, notAnalysed = 0;
        list.forEach(function (d) {
            // `_accessKind` is deliberately NOT counted. It is *derived* from the operation, so
            // every row has one, and including it made this line read "500 of 500 (100%)" on a
            // table whose predicates were captured on 72 rows out of 500. A coverage figure that
            // is always 100% is not reassurance, it is a broken gauge — and hiding a gap behind a
            // full bar is the exact failure this series exists to correct.
            //
            // What is counted is what the two cells can actually show: a column, a predicate, a
            // join, a DDL definition, or an expression/INTO from Sep05-10.
            var cd = d._columnDetail || [];
            var hasExpr = cd.some(function (c) { return c.expression || c.into; });
            var has = (d._predicates && d._predicates.length)
                   || (d._joins && d._joins.length)
                   || (d._ddl && d._ddl.length)
                   || (d._columns && d._columns.length)
                   || hasExpr;
            if (has) withDetail++;
            else if (d._columnsParsed === false) notParsed++;
            else if (d._columnsParsed === true) quiet++;
            // else: `columnsParsed` is absent — see notAnalysed below.
            else notAnalysed++;
        });
        var pct = Math.round((withDetail / list.length) * 100);
        var txt = 'detail on ' + withDetail + ' of ' + list.length + ' (' + pct + '%)';
        var bits = [];
        if (notParsed) bits.push(notParsed + ' not parsed');
        // `columnsParsed` has three states and the absent one is NOT "nothing to show".
        //
        // The first version of this line derived the quiet count by subtraction, which folded
        // "we never analysed this statement" into "this statement genuinely references nothing".
        // Measured, that mislabelled 235 SQL Server rows and 33 Oracle rows — and on SQL Server
        // that was its entire remaining gap, reported to the reader as a fact about their code
        // rather than a limit of ours. Claiming a table is unused where we simply did not look is
        // the worst answer this panel can give, so the two are now counted separately.
        if (notAnalysed) bits.push(notAnalysed + ' not analysed');
        if (quiet) bits.push(quiet + ' nothing to show');
        if (bits.length) txt += ' · ' + bits.join(' · ');
        el.textContent = txt;
        el.className = 'ad-cov' + ((notParsed || notAnalysed) ? ' ad-cov-warn' : '');
        el.title = 'How much detail was captured for these accesses. "not parsed" means the SQL '
                 + 'could not be analysed and "not analysed" means no SQL reached the analyser — '
                 + 'both are our gaps, so a blank cell there says nothing about your code. '
                 + '"nothing to show" is the only one that is a finding: the statement parsed and '
                 + 'genuinely references no column, predicate or join.';
    }

    /**
     * Initialises PA.tf over markup already in the document.
     *
     * <p>Must run after the HTML is inserted. `PA.tf.toggleDetail` assigns `innerHTML` immediately
     * after calling the detail renderer, so callers schedule this on a zero timeout from inside that
     * renderer: the assignment has completed by the time the task runs.
     */
    function mount(id, details, opts) {
        if (!document.getElementById(id + '-tbody')) return;   // panel was closed again first
        opts = opts || {};
        panels[id] = { active: new Set(), colFilters: {}, details: details || [],
                       jumpFn: opts.jumpFn || null };

        PA.tf.init(id, details || [], PAGE_SIZE, makeRowRenderer(id), {
            sortKeys: {
                0: { fn: function (d) { return d.operation || ''; } },
                1: { fn: function (d) { return (d.procedureName || '').toUpperCase(); } },
                2: { fn: function (d) { return d.lineNumber || 0; } }
            },
            // Only the three fields a reader can actually see are searched. PA.tf's default
            // stringifies every property, which here would match on procedureId — an opaque node id
            // that is not on screen, so a hit against it looks like a false positive.
            searchFn: function (d, q) {
                return String(d.operation || '').toUpperCase().includes(q)
                    || String(d.procedureName || '').toUpperCase().includes(q)
                    || ('L' + (d.lineNumber || '')).toUpperCase().includes(q);
            },
            extraFilter: function (d) {
                var st = panels[id];
                if (!st) return true;
                if (st.active.size > 0 && !st.active.has(d.operation || '?')) return false;

                var cf = st.colFilters || {};
                if (cf.op   && String(d.operation || '').toUpperCase().indexOf(cf.op) === -1) return false;
                if (cf.proc && String(d.procedureName || '').toUpperCase().indexOf(cf.proc) === -1) return false;
                if (cf.line && ('L' + (d.lineNumber || '')).toUpperCase().indexOf(cf.line) === -1) return false;
                if (cf.pred) {
                    var hay = '';
                    (d._predicates || []).forEach(function (p) {
                        hay += ' ' + (p.column || '') + ' ' + (p.operator || '') + ' ' + (p.value || '');
                    });
                    (d._columnDetail || []).forEach(function (c) {
                        hay += ' ' + (c.name || '') + ' ' + (c.expression || '') + ' ' + (c.into || '');
                    });
                    (d._joins || []).forEach(function (j) {
                        hay += ' ' + (j.type || '') + ' ' + (j.right || '') + ' ' + (j.on || '');
                    });
                    hay += ' ' + (d._accessKind || '');
                    if (hay.toUpperCase().indexOf(cf.pred) === -1) return false;
                }
                if (cf.cols) {
                    // A row whose columns were never captured cannot satisfy a column filter, and
                    // must not be silently kept as if it had matched.
                    var cols = d._columns;
                    if (!cols || !cols.some(function (c) { return c.indexOf(cf.cols) !== -1; })) return false;
                }
                return true;
            },
            onFilter: function () { _updateShown(id); }
        });
        PA.tf.render(id);
        _updateShown(id);

        // Asynchronous on purpose: the panel is usable immediately and the column cells fill in.
        if (opts.analysisName && opts.tableName) {
            enrich(id, details, opts.analysisName, opts.tableName);
        }
    }

    /** Reports how many rows the current filter leaves, so a narrowed list is never mistaken for all. */
    function _updateShown(id) {
        var el = document.getElementById(id + '-shown');
        var s = PA.tf._state && PA.tf._state[id];
        if (!el || !s) return;
        var total = (s.data || []).length;
        var shown = (s.filtered || []).length;
        el.textContent = shown === total ? '' : shown + ' of ' + total;
    }

    function search(id, value) {
        // SearchQuery.norm trims, so a query pasted with a trailing space still matches; PA.tf
        // upper-cases internally, hence normUpper here rather than norm.
        PA.tf.setSearch(id, window.SearchQuery ? SearchQuery.normUpper(value) : String(value || '').trim());
        if (window.SearchQuery) SearchQuery.markPadding(document.getElementById(id + '-q'), value);
        PA.tf.filter(id);
        _updateShown(id);
    }

    /** Applies one column's search box. Trimmed like every other search here. */
    function colFilter(id, key, value) {
        var st = panels[id];
        if (!st) return;
        st.colFilters = st.colFilters || {};
        st.colFilters[key] = window.SearchQuery ? SearchQuery.normUpper(value)
                                                : String(value || '').trim().toUpperCase();
        var el = document.getElementById(id + '-cf-' + key);
        if (window.SearchQuery) SearchQuery.markPadding(el, value);
        PA.tf.filter(id);
        _updateShown(id);
    }

    function togglePill(id, op, btn) {
        var st = panels[id];
        if (!st) return;
        if (st.active.has(op)) { st.active.delete(op); btn.classList.remove('active'); }
        else { st.active.add(op); btn.classList.add('active'); }
        // With nothing selected the panel shows everything, so every pill reads as active again.
        var allOff = st.active.size === 0;
        var wrap = btn.parentElement;
        if (wrap && allOff) {
            wrap.querySelectorAll('.ad-pill').forEach(function (b) { b.classList.add('active'); });
        }
        PA.tf.filter(id);
        _updateShown(id);
    }

    /* The panel's own styling.
     *
     * Injected rather than added to a stylesheet because the two pages keep their table-ops CSS in
     * different files (parser/css/table-ops.css and plsql/css/style.css). Two copies would drift
     * apart exactly as the two renderers did, which is what this module exists to stop. Everything
     * here is scoped under .ad-panel and uses the existing theme variables, so it inherits both
     * light and dark themes without knowing about either. */
    function installStyle() {
        if (document.getElementById('access-details-style')) return;
        var st = document.createElement('style');
        st.id = 'access-details-style';
        st.textContent = [
            '.ad-panel .ad-controls{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin:6px 0}',
            '.ad-panel .ad-search{flex:1 1 240px;min-width:180px;padding:3px 8px;font-size:11px;',
            '  border:1px solid var(--border);border-radius:4px;background:var(--bg);color:var(--text)}',
            '.ad-panel .ad-pills{display:flex;gap:4px;flex-wrap:wrap}',
            '.ad-panel .ad-pill{cursor:pointer;border:1px solid transparent;opacity:.38;',
            '  font-size:10px;padding:1px 6px;border-radius:10px}',
            '.ad-panel .ad-pill.active{opacity:1;border-color:var(--border)}',
            '.ad-panel .ad-pill-n{font-weight:400;opacity:.75}',
            '.ad-panel .ad-shown{font-size:10px;color:var(--text-muted);margin-left:auto}',
            // Sep05-08. `margin-left:auto` already belongs to .ad-shown, so this sits after it
            // rather than claiming the free space itself and pushing .ad-shown back left.
            '.ad-panel .ad-cov{font-size:10px;color:var(--text-muted);margin-left:10px;',
            '  white-space:nowrap}',
            '.ad-panel .ad-cov-warn{color:var(--warn,#b58900)}',
            '.ad-panel .ad-table{width:100%;font-size:11px}',
            '.ad-panel .ad-table th{cursor:pointer;text-align:left;font-size:10px;',
            '  color:var(--text-muted);font-weight:600;padding:3px 6px;border-bottom:1px solid var(--border)}',
            '.ad-panel .ad-table td{padding:2px 6px;vertical-align:top}',
            '.ad-panel .ad-row:hover{background:var(--hover, rgba(128,128,128,.08))}',
            /* Nested-context adjustments only — the base rules come from the shared
             * .pagination-bar stylesheet that every other pager on both pages uses. Inside an
             * expanded row this sits above its table, so the border belongs on the bottom. */
            '.ad-panel .ad-pager{border-top:none;border-bottom:1px solid var(--border);',
            '  background:transparent;padding:4px 0;margin-bottom:4px;flex-wrap:wrap}',
            /* Per-column search boxes. */
            '.ad-panel .ad-filter-row th{padding:2px 4px 4px;border-bottom:1px solid var(--border)}',
            '.ad-panel .ad-cf{width:100%;box-sizing:border-box;font-size:10px;padding:2px 5px;',
            '  border:1px solid var(--border);border-radius:3px;background:var(--bg);color:var(--text)}',
            '.ad-panel .ad-cf::placeholder{opacity:.5}',
            /* The nested table must NOT have a sticky header.
             *
             * Both pages style `.to-table thead` as `position:sticky; top:0; z-index:1` — correct
             * for a full-page table, wrong for one nested inside an expanded row. Two sticky theads
             * in the same scroll container collide: the outer table's header overlaps the inner
             * one, so the Operation/Procedure/Line labels disappear behind it and the panel looks
             * like it has no header at all. Reported from live use as "head filter missing" and,
             * earlier, as things overlapping. */
            '.ad-panel .ad-table thead{position:static;top:auto;z-index:auto;background:transparent}',
            '.ad-panel .ad-table th{position:static}',
            '.ad-panel .ad-table{table-layout:fixed}',
            '.ad-panel .ad-table th:nth-child(1){width:15%}',
            '.ad-panel .ad-table th:nth-child(2){width:34%}',
            '.ad-panel .ad-table th:nth-child(3){width:11%}',
            '.ad-panel .ad-table th:nth-child(4){width:22%}',
            '.ad-panel .ad-table th:nth-child(5){width:32%}',
            '.ad-panel .ad-det{font-size:10px;line-height:1.7}',
            '.ad-panel .ad-pred{display:inline-block;margin:0 4px 2px 0;padding:0 4px;border-radius:3px;',
            '  background:var(--bg-hover,rgba(128,128,128,.10));font-family:var(--mono,monospace)}',
            '.ad-panel .ad-op{opacity:.7;font-weight:600}',
            '.ad-panel .ad-into{color:var(--badge-teal,#3aa);font-weight:600}',
            /* A written column must be distinguishable from a read one at a glance — that is the
             * difference between "this table is used here" and "this table is CHANGED here". */
            '.ad-panel .ad-col-write{background:var(--badge-orange-bg,rgba(208,135,0,.18));',
            '  color:var(--badge-orange,#d08700)}',
            '.ad-panel .ad-usage{margin-left:3px;font-size:8px;opacity:.75;font-weight:700}',
            '.ad-panel .ad-kind{margin-left:5px;font-size:9px;padding:0 4px;border-radius:8px;',
            '  background:var(--accent-subtle);color:var(--accent);white-space:nowrap}',
            '.ad-panel .ad-scope{margin-left:4px;font-size:9px;padding:0 3px;border-radius:3px;',
            '  border:1px solid var(--border);color:var(--text-muted);cursor:help}',
            '.ad-panel .ad-cols{max-width:230px}',
            '.ad-panel .ad-col{display:inline-block;font-size:9px;padding:0 4px;margin:0 2px 2px 0;',
            '  border-radius:3px;background:var(--bg-hover,rgba(128,128,128,.10));color:var(--text-muted)}',
            '.ad-panel .ad-dim{color:var(--text-muted);opacity:.6}',
            '.ad-panel .ad-unknown{font-size:9px;color:var(--badge-orange,#d08700);font-style:italic}'
        ].join('');
        document.head.appendChild(st);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', installStyle);
    } else {
        installStyle();
    }

    return { render: render, mount: mount, search: search, togglePill: togglePill,
             colFilter: colFilter, enrich: enrich,
             fetchDetail: fetchDetail, applyDetail: applyDetail, detailKey: detailKey,
             PAGE_SIZE: PAGE_SIZE };
})();
