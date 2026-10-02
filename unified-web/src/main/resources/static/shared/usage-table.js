/**
 * UsageTable — ONE reusable "usage list" table for the whole app.
 *
 * <h2>Why this exists</h2>
 *
 * Everywhere a row-click opens a list of usages below it, the list must answer the same four
 * questions at a glance: WHO/WHAT (procedure or object name), WHERE (line), WHAT IT DOES (a SQL
 * snippet, so the reader can decide whether to open the full detail) and HOW (the operation/event
 * badges). And it must stay usable when the list is long: paged, filterable, and — because the SQL
 * text is database-sourced DATA — escaped, never emitted as markup.
 *
 * Before this, each screen hand-built a flat `<div>` list (`.est-usedby`, `.to-detail-item`,
 * `.js-pred-item`, …): no header, no paging, no filter, and a snippet only where a caller happened
 * to remember one. They drifted. This is the one place the row is built.
 *
 * <h2>How it is built</h2>
 *
 * On `PA.tf`, the app's existing table framework (pagination, search, sorting, expandable detail
 * rows), so the pager markup and styling are exactly what every other table on the page uses. No
 * framework, no new pager. The pure helpers below (`paginate`, `matchRow`, `filterRows`,
 * `snippetHtml`, `makeRowRenderer`) carry the logic and are unit-tested without a DOM.
 *
 * <h2>Usage</h2>
 *
 *   var html = PA.usageTable.attach('ut-usedby-3', rows, {
 *       title: 'Used By', countLabel: 'usages',
 *       name: function (r) { return r.nodeName; },
 *       nameLink: function (r) { return "PA.showProcedure('" + PA.escJs(r.nodeId) + "')"; },
 *       snippet: function (r) { return r.sqlExpression; },
 *       source: PA.usageTable.nodeSource   // lazily read the source line when no inline SQL
 *   });
 *
 * `attach` returns the markup AND schedules `mount` on the next task (the DOM has to exist first —
 * the same contract `shared/access-details.js` uses). A caller that already has the DOM mounted
 * can call `render` + `mount` directly.
 *
 * Options (all optional):
 *   title, countLabel, nameLabel, snippetLabel, pageSize (default 25), empty,
 *   showTitle, showSnippet, snippetLimit, nameClass,
 *   name(row), nameLink(row)->jsString, line(row), lineLink(row)->jsString,
 *   sub(row)->{text,link}, snippet(row), badges(row)->[string|{text,cls}],
 *   full(row)->htmlString (enables expand-to-full), search(row), source(row)->{file,line}
 */
window.PA = window.PA || {};
(function () {
    'use strict';

    var DEFAULT_PAGE = 25;
    var SNIPPET_LIMIT = 160;
    var panels = {};          // id -> { kinds:Set, opts, rows }
    var srcCache = {};        // sourceFile -> Promise<string content>

    function esc(v) {
        return (window.PA && PA.esc) ? PA.esc(v) : String(v == null ? '' : v);
    }
    function escJs(v) {
        return (window.PA && PA.escJs) ? PA.escJs(v)
            : String(v == null ? '' : v).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    }

    /* ============================ pure helpers (unit-tested) ============================ */

    /**
     * Page math. Clamps the page into `[0, pages-1]` and returns the slice bounds, so a filter that
     * shrinks the list can never leave the reader on a page past the end showing nothing.
     */
    function paginate(total, size, page) {
        total = Math.max(0, parseInt(total, 10) || 0);
        size = Math.max(1, parseInt(size, 10) || DEFAULT_PAGE);
        page = parseInt(page, 10) || 0;
        var pages = Math.max(1, Math.ceil(total / size));
        if (page < 0) page = 0;
        if (page > pages - 1) page = pages - 1;
        var start = page * size;
        var end = Math.min(start + size, total);
        return { page: page, pages: pages, size: size, total: total, start: start, end: end };
    }

    function nameOf(row, opts) {
        if (opts && opts.name) { var v = opts.name(row); return v == null ? '' : String(v); }
        if (row == null) return '';
        return String(row.name || row.procedureName || row.nodeName || row.variable
            || row.cursorName || row.triggerName || row.tableName || row.objectName
            || row.job || row.package || '');
    }

    function firstLine(row, opts) {
        if (opts && opts.line) { var v = opts.line(row); return v == null ? null : v; }
        if (row == null) return null;
        if (row.lineNumber != null) return row.lineNumber;
        if (row.line != null) return row.line;
        var ops = row.operations || row.matchingOperations;
        if (ops && typeof ops === 'object') {
            var best = null;
            Object.keys(ops).forEach(function (k) {
                var lines = ops[k] || [];
                for (var i = 0; i < lines.length; i++) {
                    if (lines[i] != null && (best == null || lines[i] < best)) best = lines[i];
                }
            });
            if (best != null) return best;
        }
        if (row.lines && row.lines.length) return row.lines[0];
        return null;
    }

    function snippetOf(row, opts) {
        if (opts && opts.snippet) { var v = opts.snippet(row); return v == null ? '' : String(v); }
        if (row == null) return '';
        if (row._snippet) return String(row._snippet);
        return String(row.snippet || row.sqlExpression || row.queryText || row.sqlText
            || row.condition || row.predicate || row.onPredicate || row.on || '');
    }

    var BADGE_CLS = { select: 'select', insert: 'insert', update: 'update', delete: 'delete', read: 'read', write: 'write' };

    function badgeCls(kind) {
        var k = String(kind || '').toLowerCase();
        if (BADGE_CLS[k]) return BADGE_CLS[k];
        if (/^insert/.test(k)) return 'insert';
        if (/^update/.test(k)) return 'update';
        if (/^delete/.test(k)) return 'delete';
        if (/^select/.test(k)) return 'select';
        if (/^read/.test(k)) return 'read';
        if (/^write/.test(k)) return 'write';
        return '';
    }

    function normalizeBadges(list) {
        var seen = {}, out = [];
        (list || []).forEach(function (b) {
            var kind, cls, text;
            if (b && typeof b === 'object') {
                kind = b.kind != null ? b.kind : (b.text || '');
                text = b.text != null ? b.text : (b.kind || '');
                cls = b.cls != null ? b.cls : badgeCls(kind);
            } else {
                kind = String(b); text = String(b); cls = badgeCls(b);
            }
            var key = (String(kind) + '|' + String(text)).toUpperCase();
            if (!text || seen[key]) return;
            seen[key] = true;
            out.push({ kind: kind, text: text, cls: cls });
        });
        return out;
    }

    /** The badges for a row, derived from whatever operation/event/type fields it carries. */
    function badgesOf(row, opts) {
        if (opts && opts.badges) return normalizeBadges(opts.badges(row));
        var out = [];
        function add(v) {
            if (v == null || v === '') return;
            String(v).split(/[\s,|]+/).forEach(function (x) { if (x) out.push(x); });
        }
        if (row == null) return [];
        add(row.operation); add(row.accessType); add(row.op);
        add(row.event); add(row.timing);
        if (row.type && /DYNAMIC|EXECUTE|IMMEDIATE/i.test(String(row.type))) add(row.type);
        ['operations', 'matchingOperations'].forEach(function (f) {
            var m = row[f];
            if (m && typeof m === 'object') Object.keys(m).forEach(add);
        });
        if (row.nodeType) add(row.nodeType);
        return normalizeBadges(out);
    }

    function badgesHtml(badges, escFn) {
        if (!badges || !badges.length) return '<span class="ut-dim">—</span>';
        return badges.map(function (b) {
            return '<span class="est-op-badge' + (b.cls ? ' ' + b.cls : '') + '">' + escFn(b.text) + '</span>';
        }).join(' ');
    }

    function rowSearchText(row, opts) {
        var parts = [nameOf(row, opts), snippetOf(row, opts)];
        if (opts && opts.procedure) parts.push(String(opts.procedure(row) || ''));
        badgesOf(row, opts).forEach(function (b) { parts.push(b.text); });
        var ln = firstLine(row, opts);
        if (ln != null) parts.push('L' + ln);
        if (opts && opts.search) parts.push(String(opts.search(row) || ''));
        return parts.join(' ').toUpperCase();
    }

    function matchRow(row, q, opts) {
        if (!q) return true;
        return rowSearchText(row, opts).indexOf(String(q).toUpperCase()) !== -1;
    }

    function filterRows(rows, q, opts) {
        rows = rows || [];
        if (!q) return rows.slice();
        return rows.filter(function (r) { return matchRow(r, q, opts); });
    }

    /**
     * Escaped, single-line, truncated SQL snippet.
     *
     * <p>The text is database-sourced DATA. It is escaped before it reaches the DOM, so a snippet
     * containing `<script>` or a quote renders as characters and cannot inject — the same rule the
     * whole app applies to DB text (see `shared/escape.js`).
     */
    function snippetHtml(text, escFn, limit) {
        limit = limit || SNIPPET_LIMIT;
        if (text == null || text === '') return '<span class="ut-dim">—</span>';
        var s = String(text).replace(/\s+/g, ' ').trim();
        var cut = s.length > limit;
        var shown = cut ? s.slice(0, limit) + '…' : s;
        return '<code class="ut-snippet"' + (cut ? ' title="Click the row to expand"' : '') + '>'
            + escFn(shown) + '</code>';
    }

    /* ============================ rendering ============================ */

    function colLayout(opts) {
        var showSnippet = opts.showSnippet !== false;
        var hasProc = !!opts.procedure;
        var c = 0;
        var nameCol = c++;
        var procCol = hasProc ? c++ : -1;
        var lineCol = c++;
        var snippetCol = showSnippet ? c++ : -1;
        var kindCol = c;
        return { showSnippet: showSnippet, hasProc: hasProc, nameCol: nameCol, procCol: procCol,
                 lineCol: lineCol, snippetCol: snippetCol, kindCol: kindCol };
    }

    function lineHtml(ln, link) {
        if (ln == null || ln === '') return '<span class="ut-dim">—</span>';
        if (link) return '<span class="to-detail-line ut-linelink" onclick="event.stopPropagation();' + link + '">L' + ln + '</span>';
        return '<span class="ut-line-no">L' + ln + '</span>';
    }

    /** Builds the row renderer. Exposed so the escaping is unit-testable without a DOM. */
    function makeRowRenderer(id, opts) {
        opts = opts || {};
        var layout = colLayout(opts);
        return function (row, idx, escFn) {
            var e = escFn || esc;
            var nm = nameOf(row, opts);
            var ln = firstLine(row, opts);
            var link = opts.nameLink ? opts.nameLink(row) : null;
            var lineLink = opts.lineLink ? opts.lineLink(row) : null;
            var sub = opts.sub ? opts.sub(row) : null;
            var hasFull = !!opts.full;

            var html = '<tr class="ut-row' + (hasFull ? ' ut-expandable' : '') + '"'
                + (hasFull ? ' onclick="PA.tf.toggleDetail(\'' + escJs(id) + '\',' + idx + ')"' : '') + '>';
            html += '<td class="ut-name ' + (opts.nameClass || '') + '">';
            html += link ? '<span class="ut-link" onclick="event.stopPropagation();' + link + '">' + e(nm) + '</span>' : e(nm);
            if (sub) {
                html += '<span class="ut-sub">'
                    + (sub.link ? '<span class="ut-sublink" onclick="event.stopPropagation();' + sub.link + '">' + e(sub.text) + '</span>' : e(sub.text))
                    + '</span>';
            }
            html += '</td>';
            if (layout.hasProc) {
                var proc = opts.procedure(row);
                var plink = opts.procedureLink ? opts.procedureLink(row) : null;
                proc = (proc == null || proc === '') ? '' : String(proc);
                html += '<td class="ut-proc">';
                html += proc
                    ? (plink ? '<span class="ut-link" onclick="event.stopPropagation();' + plink + '">' + e(proc) + '</span>' : e(proc))
                    : '<span class="ut-dim">—</span>';
                html += '</td>';
            }
            html += '<td class="ut-line">' + lineHtml(ln, lineLink) + '</td>';
            if (layout.showSnippet) html += '<td class="ut-sql">' + snippetHtml(snippetOf(row, opts), e, opts.snippetLimit) + '</td>';
            html += '<td class="ut-kind">' + badgesHtml(badgesOf(row, opts), e) + '</td>';
            html += '</tr>';
            return html;
        };
    }

    /**
     * Builds the panel markup. Call {@link mount} once it is in the DOM.
     * @returns {string} HTML
     */
    function render(id, rows, opts) {
        opts = opts || {};
        rows = rows || [];
        var e = esc;
        var layout = colLayout(opts);

        var kinds = {}, order = [];
        rows.forEach(function (r) {
            badgesOf(r, opts).forEach(function (b) {
                var k = String(b.text).toUpperCase();
                if (!kinds[k]) { kinds[k] = { text: b.text, cls: b.cls, n: 0 }; order.push(k); }
                kinds[k].n++;
            });
        });

        var h = '<div class="ut-panel">';
        h += '<div class="ut-head">';
        if (opts.showTitle !== false) h += '<span class="ut-title">' + e(opts.title || 'Usages') + '</span>';
        h += '<span class="ut-count" id="' + e(id) + '-count"></span></div>';

        h += '<div class="ut-controls">';
        h += '<input type="search" class="ut-search" id="' + e(id) + '-q" placeholder="Filter '
            + rows.length + ' ' + e(opts.countLabel || 'usages') + '…"'
            + ' onclick="event.stopPropagation()" oninput="PA.usageTable.search(\'' + escJs(id) + '\',this.value)">';
        if (order.length > 1) {
            h += '<span class="ut-pills">';
            order.forEach(function (k) {
                var b = kinds[k];
                h += '<button type="button" class="est-op-badge ut-pill active' + (b.cls ? ' ' + b.cls : '') + '"'
                    + ' data-kind="' + e(k) + '"'
                    + ' onclick="event.stopPropagation();PA.usageTable.toggleKind(\'' + escJs(id) + '\',\'' + escJs(k) + '\',this)">'
                    + e(b.text) + ' <span class="ut-pill-n">' + b.n + '</span></button>';
            });
            h += '</span>';
        }
        h += '<span class="ut-shown" id="' + e(id) + '-shown"></span>';
        h += '</div>';

        h += '<table class="to-table ut-table" id="' + e(id) + '"><thead><tr>';
        h += '<th data-sort-col="' + layout.nameCol + '" onclick="PA.usageTable.sort(\'' + escJs(id) + '\',' + layout.nameCol + ')">'
            + e(opts.nameLabel || 'Name') + '</th>';
        if (layout.hasProc) {
            h += '<th data-sort-col="' + layout.procCol + '" onclick="PA.usageTable.sort(\'' + escJs(id) + '\',' + layout.procCol + ')">'
                + e(opts.procedureLabel || 'Procedure') + '</th>';
        }
        h += '<th data-sort-col="' + layout.lineCol + '" onclick="PA.usageTable.sort(\'' + escJs(id) + '\',' + layout.lineCol + ')">Line</th>';
        if (layout.showSnippet) h += '<th>' + e(opts.snippetLabel || 'SQL') + '</th>';
        h += '<th>Kind</th>';
        // The first page is rendered inline (no DOM needed), so the panel is populated before
        // `mount` runs and the row-building stays testable without a browser. `PA.tf` re-renders
        // the same page on mount and takes over paging/filtering from there.
        var first = paginate(rows.length, opts.pageSize || DEFAULT_PAGE, 0);
        var rowFn = makeRowRenderer(id, opts);
        var body = '';
        for (var ri = first.start; ri < first.end; ri++) {
            body += rowFn(rows[ri], ri, e);
            if (opts.full) {
                body += '<tr class="to-detail-row" id="' + e(id) + '-detail-' + ri
                    + '" style="display:none" data-lazy="1"><td colspan="20"></td></tr>';
            }
        }
        h += '</tr></thead><tbody id="' + e(id) + '-tbody">' + body + '</tbody></table>';
        h += '<div id="' + e(id) + '-pager" class="pagination-bar ut-pager"></div>';
        h += '</div>';
        return h;
    }

    function updateCount(id, opts) {
        if (typeof document === 'undefined') return;
        var st = PA.tf && PA.tf.state ? PA.tf.state(id) : null;
        var rows = (panels[id] && panels[id].rows) || [];
        var total = rows.length;
        var shown = st && st.filtered ? st.filtered.length : total;
        var label = (opts && opts.countLabel) || 'usages';
        var countEl = document.getElementById(id + '-count');
        if (countEl) countEl.textContent = total + ' ' + label;
        var shownEl = document.getElementById(id + '-shown');
        if (shownEl) shownEl.textContent = shown === total ? '' : shown + ' of ' + total;
    }

    /** Initialises PA.tf over markup already in the document. */
    function mount(id, rows, opts) {
        opts = opts || {};
        rows = rows || [];
        if (typeof document === 'undefined' || !window.PA || !PA.tf || !document.getElementById(id + '-tbody')) return;
        var layout = colLayout(opts);
        panels[id] = { kinds: new Set(), opts: opts, rows: rows };

        var sortKeys = {};
        sortKeys[layout.nameCol] = { fn: function (r) { return nameOf(r, opts).toUpperCase(); } };
        sortKeys[layout.lineCol] = { fn: function (r) { var l = firstLine(r, opts); return l == null ? 0 : l; } };
        if (layout.hasProc) {
            sortKeys[layout.procCol] = { fn: function (r) { return String(opts.procedure(r) || '').toUpperCase(); } };
        }

        PA.tf.init(id, rows, opts.pageSize || DEFAULT_PAGE, makeRowRenderer(id, opts), {
            sortKeys: sortKeys,
            renderDetail: opts.full ? function (row, idx, e) { return '<div class="ut-full">' + opts.full(row) + '</div>'; } : null,
            searchFn: function (row, q) { return matchRow(row, q, opts); },
            extraFilter: function (row) {
                var st = panels[id];
                if (!st || !st.kinds.size) return true;
                return badgesOf(row, opts).some(function (b) { return st.kinds.has(String(b.text).toUpperCase()); });
            },
            onFilter: function () { updateCount(id, opts); }
        });
        PA.tf.render(id);
        updateCount(id, opts);
        if (opts.source) enrich(id, rows, opts);
    }

    /** Renders and schedules the mount once the markup is in the DOM. */
    function attach(id, rows, opts) {
        var html = render(id, rows, opts);
        if (typeof document !== 'undefined') setTimeout(function () { mount(id, rows, opts); }, 0);
        return html;
    }

    function search(id, value) {
        if (!window.PA || !PA.tf) return;
        PA.tf.setSearch(id, String(value == null ? '' : value));
        PA.tf.filter(id);
    }

    function toggleKind(id, kind, btn) {
        var st = panels[id];
        if (!st) return;
        kind = String(kind).toUpperCase();
        if (st.kinds.has(kind)) {
            st.kinds.delete(kind);
            if (btn) btn.classList.remove('active');
        } else {
            st.kinds.add(kind);
            if (btn) btn.classList.add('active');
        }
        // With nothing selected the panel shows everything, so every pill reads active again.
        if (!st.kinds.size && btn && btn.parentElement) {
            btn.parentElement.querySelectorAll('.ut-pill').forEach(function (b) { b.classList.add('active'); });
        }
        PA.tf.filter(id);
    }

    function sort(id, col) {
        if (window.PA && PA.tf) PA.tf.sort(id, col);
    }

    /** The default source resolver: an explicit sourceFile, else the node id resolved via nodeIndex. */
    function nodeSource(row) {
        if (!row) return null;
        var file = row.sourceFile || '';
        var id = row.nodeId || row.procedureId;
        if (!file && id && window.PA && PA.nodeIndex) {
            var n = (PA.nodeIndex.byId && PA.nodeIndex.byId(id))
                || (PA.nodeIndex.byIdIgnoringCase && PA.nodeIndex.byIdIgnoringCase(id));
            if (n && n.sourceFile) file = n.sourceFile;
        }
        return { file: file, line: firstLine(row) };
    }

    function getSourceCached(file) {
        if (srcCache[file]) return srcCache[file];
        srcCache[file] = PA.api.getSource(file).then(function (d) {
            if (d == null) return '';
            return d.content != null ? d.content : (d.source != null ? d.source : '');
        });
        return srcCache[file];
    }

    /**
     * Lazily fills a snippet from the source file for rows that carry no inline SQL, then re-renders.
     * Failure is silent: a row with no snippet shows "—", never a fabricated one.
     */
    function enrich(id, rows, opts) {
        if (typeof document === 'undefined' || !window.PA || !PA.api || !PA.api.getSource) return;
        var byFile = {};
        (rows || []).forEach(function (r) {
            if (snippetOf(r, opts)) return;
            var s = opts.source(r);
            if (!s || !s.file || s.line == null) return;
            (byFile[s.file] = byFile[s.file] || []).push({ row: r, line: s.line });
        });
        var files = Object.keys(byFile);
        if (!files.length) return;
        var pending = files.length;
        function done() {
            if (--pending > 0) return;
            if (document.getElementById(id + '-tbody') && PA.tf._state[id]) { PA.tf.render(id); updateCount(id, opts); }
        }
        files.forEach(function (file) {
            getSourceCached(file).then(function (content) {
                var lines = content ? String(content).split('\n') : [];
                byFile[file].forEach(function (it) {
                    var text = lines[it.line - 1];
                    if (text != null) it.row._snippet = String(text).trim();
                });
                done();
            }).catch(done);
        });
    }

    /* ============================ styling (self-contained, themed) ============================ */

    function installStyle() {
        if (typeof document === 'undefined' || !document.head
            || typeof document.getElementById !== 'function') return;
        if (document.getElementById('usage-table-style')) return;
        var st = document.createElement('style');
        st.id = 'usage-table-style';
        st.textContent = [
            '.ut-panel{margin:4px 0 2px}',
            '.ut-head{display:flex;align-items:baseline;gap:8px;margin-bottom:3px}',
            '.ut-title{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--text-muted)}',
            '.ut-count{font-size:10px;font-weight:700;color:var(--accent)}',
            '.ut-controls{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin:4px 0}',
            '.ut-search{flex:1 1 220px;min-width:160px;padding:3px 8px;font-size:11px;',
            '  border:1px solid var(--border-subtle);border-radius:var(--radius-sm);background:var(--bg);color:var(--text)}',
            '.ut-pills{display:flex;gap:4px;flex-wrap:wrap}',
            '.ut-pill{cursor:pointer;border:1px solid transparent;opacity:.4}',
            '.ut-pill.active{opacity:1;border-color:var(--border-subtle)}',
            '.ut-pill-n{font-weight:400;opacity:.75}',
            '.ut-shown{font-size:10px;color:var(--text-muted);margin-left:auto}',
            '.ut-table{width:100%;table-layout:auto}',
            '.ut-table thead{position:static}',
            '.ut-table th{cursor:pointer;text-align:left;font-size:9px;text-transform:uppercase;',
            '  letter-spacing:.05em;color:var(--text-muted);font-weight:700;padding:3px 6px;border-bottom:1px solid var(--border-subtle)}',
            '.ut-table td{padding:3px 6px;vertical-align:top;font-size:11px;border-bottom:1px solid var(--border-subtle)}',
            '.ut-table th:nth-child(1),.ut-table td:nth-child(1){width:26%}',
            '.ut-proc{font-family:var(--font-mono);word-break:break-word}',
            '.ut-row:hover{background:var(--bg-hover,rgba(128,128,128,.08))}',
            '.ut-row.ut-expandable{cursor:pointer}',
            '.ut-name{font-family:var(--font-mono);font-weight:600;color:var(--text);word-break:break-word}',
            '.ut-link{color:var(--accent-hover);cursor:pointer}.ut-link:hover{text-decoration:underline}',
            '.ut-sub{display:block;font-size:10px;font-weight:400;color:var(--text-muted);font-family:var(--font-mono)}',
            '.ut-sublink{color:var(--accent-hover);cursor:pointer}.ut-sublink:hover{text-decoration:underline}',
            '.ut-line{white-space:nowrap}',
            '.ut-line-no{font-size:10px;font-family:var(--font-mono);color:var(--text-muted)}',
            '.ut-sql{max-width:0}',
            '.ut-snippet{display:block;font-family:var(--font-mono);font-size:10px;color:var(--text-secondary);',
            '  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%}',
            '.ut-kind{white-space:nowrap}',
            '.ut-dim{color:var(--text-muted);opacity:.6}',
            '.ut-full{padding:2px 0}',
            '.ut-full pre{font-family:var(--font-mono);font-size:11px;background:var(--bg-inset);',
            '  border:1px solid var(--border-subtle);border-radius:var(--radius-sm);padding:6px 8px;',
            '  margin:2px 0;white-space:pre-wrap;word-break:break-word;max-height:220px;overflow:auto}',
            '.ut-pager{border-top:none;border-bottom:1px solid var(--border-subtle);background:transparent;padding:4px 0;margin-bottom:4px;flex-wrap:wrap}'
        ].join('');
        document.head.appendChild(st);
    }

    if (typeof document !== 'undefined' && typeof document.addEventListener === 'function') {
        if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', installStyle);
        else installStyle();
    }

    PA.usageTable = {
        PAGE: DEFAULT_PAGE,
        paginate: paginate,
        matchRow: matchRow,
        filterRows: filterRows,
        snippetHtml: snippetHtml,
        makeRowRenderer: makeRowRenderer,
        badgesOf: badgesOf,
        nameOf: nameOf,
        firstLine: firstLine,
        nodeSource: nodeSource,
        render: render,
        mount: mount,
        attach: attach,
        search: search,
        toggleKind: toggleKind,
        sort: sort,
        enrich: enrich
    };
    window.UsageTable = PA.usageTable;
})();
