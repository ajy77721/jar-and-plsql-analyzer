/**
 * PA.search — the REAL cross-facet Global Search results screen (UI-045..UI-050, §256), ported to
 * the Parser app.
 *
 * TASK-REGISTRY §280 — this file already existed for `plsql/js/search.js` (a near-identical
 * frontend that shares the same tab-switch/show-procedure function names as this app), but was
 * NEVER copied here. Parser's own `#globalSearch` input was still wired to the old `PA.globalSearch`
 * (`app.js`) — an 11-line, network-free, same-tab DOM substring filter — the entire time. The two
 * apps' Global Search share this same code, adapted only where their `PA.api` shapes differ (see
 * the `_analysisName` note below).
 *
 * Backend contract (`ParserAnalysisController.getSearch`/`getSearchSummary`, `ParserFacetSpecs.
 * SEARCH`) — identical to plsql/js/search.js's, since both apps hit the same controller:
 *   GET /api/parser/analyses/{name}/global-search?q=...&limit=...&after=...&sort=...&cf=...
 *     -> the standard page envelope ({items, page}), each item shaped
 *        {type, name, schema, matchedField, _idx, raw}
 *   GET /api/parser/analyses/{name}/global-search/summary?q=...
 *     -> {columnValues: {"0": [...types], "1": [...schemas]}, ...}
 *
 * `cf` column-index contract — THE ONE THING THIS FILE MUST KEEP IN SYNC WITH
 * `ParserFacetSpecs.SEARCH`: "0" = type, "1" = schema.
 *
 * Difference from plsql/js/search.js: that file gates on the `_parserName` property of `PA.api`
 * (called as a function) because PL/SQL's
 * app also serves live-crawl (non-parser) analyses with no such endpoint. Parser's own analyses are
 * ALWAYS parser-backed — there is no live-crawl mode here — so this uses the simpler
 * `PA.api._analysisName` (a plain string property, not a distinguishing gate) and has no
 * old-filter fallback branch to fall back to.
 *
 * UI redesign (this revision) — the topbar `#globalSearch` live-filter input is gone; the existing
 * "Search" button now opens this modal directly (was `openFromButton()`, which used to just focus
 * that input). Inside the modal: one text box plus three Notepad++/VS Code-style find toggles
 * (Match case / Whole word / Regex). Matching those toggles happens CLIENT-SIDE on top of the
 * server's own broader literal-substring recall — see plsql/js/search.js's identical header comment
 * for the full rationale and the one known trade-off (a fetched page can render sparser than 50
 * rows once a strict toggle trims some out; the pager's total still reflects the server's broader,
 * pre-toggle count).
 */
window.PA = window.PA || {};

PA.search = {
    _src: null,
    _lastQuery: '',
    _matcher: null,
    _opts: { cs: false, whole: false, regex: false },

    /** Entry point — the topbar "Search" button's onclick. Opens the modal directly. */
    open() {
        PA.search._loadOpts();
        PA.search._ensureOverlay();
        document.getElementById('searchOverlay').style.display = 'flex';
        PA.search._syncToggleUI();
        const input = document.getElementById('srchQueryInput');
        if (input) {
            input.value = PA.search._lastQuery || '';
            input.focus();
            input.select();
        }
        if (PA.search._lastQuery && PA.search._lastQuery.length >= 2) {
            PA.search._compileMatcher();
            PA.search._initTable(PA.search._lastQuery);
        } else {
            PA.search._showHint();
        }
    },

    /** The modal's own input's oninput. */
    _onInput(query) {
        const q = (query || '').trim();
        clearTimeout(PA.search._debounce);
        if (q.length < 2) {
            PA.search._showHint();
            return;
        }
        PA.search._debounce = setTimeout(() => {
            PA.search._lastQuery = q;
            PA.search._compileMatcher();
            PA.search._initTable(q);
        }, 300);
    },

    _showHint() {
        const c = document.getElementById('srchContainer');
        const p = document.getElementById('srch-pager');
        if (c) c.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text-muted)">Type at least 2 characters to search&hellip;</div>';
        if (p) p.innerHTML = '';
    },

    close() {
        const el = document.getElementById('searchOverlay');
        if (el) el.style.display = 'none';
    },

    /* ---- Find toggles (Match case / Whole word / Regex) ------------------------------------- */

    _loadOpts() {
        try {
            const raw = localStorage.getItem('mdb-search-opts:parser');
            if (raw) Object.assign(PA.search._opts, JSON.parse(raw));
        } catch (e) { /* ignore malformed storage */ }
    },

    _saveOpts() {
        try { localStorage.setItem('mdb-search-opts:parser', JSON.stringify(PA.search._opts)); } catch (e) { /* ignore */ }
    },

    _toggleOpt(key) {
        PA.search._opts[key] = !PA.search._opts[key];
        PA.search._saveOpts();
        PA.search._syncToggleUI();
        PA.search._compileMatcher();
        if (PA.tf.state('srch')) PA.tf.render('srch');
    },

    _syncToggleUI() {
        const map = { cs: 'srchToggleCase', whole: 'srchToggleWord', regex: 'srchToggleRegex' };
        Object.keys(map).forEach(k => {
            const el = document.getElementById(map[k]);
            if (el) el.classList.toggle('active', !!PA.search._opts[k]);
        });
    },

    _serverQueryFor(q) {
        if (!PA.search._opts.regex && !PA.search._opts.whole) return q;
        const parts = q.split(/[.*+?^${}()|[\]\\]/).filter(Boolean);
        parts.sort((a, b) => b.length - a.length);
        return parts[0] || q.replace(/[^a-zA-Z0-9_]/g, '') || q;
    },

    _compileMatcher() {
        const q = PA.search._lastQuery || '';
        const o = PA.search._opts;
        if (!q || (!o.regex && !o.whole)) { PA.search._matcher = null; return; }
        try {
            if (o.regex) {
                PA.search._matcher = new RegExp(q, o.cs ? '' : 'i');
            } else {
                const esc = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                PA.search._matcher = new RegExp('\\b' + esc + '\\b', o.cs ? '' : 'i');
            }
        } catch (e) {
            PA.search._matcher = null;
        }
    },

    _rowMatches(r) {
        const q = PA.search._lastQuery || '';
        if (!q) return true;
        const o = PA.search._opts;
        // Plain, no-toggle mode: trust the server's own match decision completely — see
        // plsql/js/search.js's identical comment for the full rationale (matchedField is a field
        // LABEL, not the matched text, so re-checking it client-side dropped real server matches).
        if (!o.regex && !o.whole && !o.cs) return true;
        let hay;
        try { hay = (r.name || '') + ' ' + (r.schema || '') + ' ' + JSON.stringify(r.raw || {}); }
        catch (e) { hay = (r.name || '') + ' ' + (r.schema || ''); }
        if (o.regex || o.whole) return PA.search._matcher ? PA.search._matcher.test(hay) : true;
        return hay.includes(q);
    },

    /** See plsql/js/search.js's identical helper for the full rationale — explicit
     *  user-confirmed 4-segment layout (mockup-approved): search textbar, pagination bar as its
     *  own row, then the results table in its own scroll region below. */
    _ensureOverlay() {
        if (document.getElementById('searchOverlay')) return;
        const html = `
<div id="searchOverlay" style="display:none;position:fixed;inset:0;z-index:9000;background:rgba(0,0,0,.45);align-items:flex-start;justify-content:center">
  <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;margin-top:48px;width:min(1100px,94vw);max-height:82vh;display:flex;flex-direction:column;box-shadow:var(--shadow-lg)">
    <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--border)">
      <div style="font-size:.95rem;font-weight:600">Search</div>
      <button class="btn btn-sm" onclick="PA.search.close()" title="Close">&times;</button>
    </div>
    <div style="display:flex;gap:8px;align-items:center;padding:10px 16px;border-bottom:1px solid var(--border)">
      <input type="text" id="srchQueryInput" class="form-input" placeholder="Find..." style="flex:1" oninput="PA.search._onInput(this.value)">
      <button class="srch-toggle-btn" id="srchToggleCase" title="Match case" onclick="PA.search._toggleOpt('cs')">Aa</button>
      <button class="srch-toggle-btn" id="srchToggleWord" title="Match whole word" onclick="PA.search._toggleOpt('whole')">ab</button>
      <button class="srch-toggle-btn" id="srchToggleRegex" title="Use regular expression" onclick="PA.search._toggleOpt('regex')">.*</button>
    </div>
    <div id="srch-pager" class="pager srch-pagerbar" data-moved-top="1" style="padding:8px 16px;border-bottom:1px solid var(--border);flex-shrink:0"></div>
    <div style="padding:10px 16px;overflow:auto;flex:1">
      <div id="srchContainer"></div>
    </div>
  </div>
</div>`;
        // Real bug found live (see page-envelope.js's mountOverlay doc comment): raw
        // insertAdjacentHTML with a fixed id stacks a duplicate overlay on every re-open, and
        // close only removes the first. mountOverlay clears any existing #searchOverlay first.
        PA.mountOverlay(html);
        document.getElementById('searchOverlay').addEventListener('click', (e) => {
            if (e.target.id === 'searchOverlay') PA.search.close();
        });
        document.addEventListener('keydown', (e) => {
            const overlay = document.getElementById('searchOverlay');
            if (e.key === 'Escape' && overlay && overlay.style.display !== 'none') PA.search.close();
        });
    },

    _initTable(q) {
        const n = PA.api._analysisName;
        const serverQ = PA.search._serverQueryFor(q);
        const container = document.getElementById('srchContainer');
        container.innerHTML = '<table class="srch-table">'
            + '<colgroup><col class="srch-col-type"><col class="srch-col-name">'
            + '<col class="srch-col-schema"><col class="srch-col-line"><col class="srch-col-matched"></colgroup>'
            + '<thead><tr>'
            + '<th data-sort-col="0" onclick="PA.tf.sort(\'srch\',0)">Type</th>'
            + '<th data-sort-col="2" onclick="PA.tf.sort(\'srch\',2)">Name</th>'
            + '<th data-sort-col="1" onclick="PA.tf.sort(\'srch\',1)">Schema</th>'
            + '<th>Line</th>'
            + '<th data-sort-col="3">Matched</th>'
            + '</tr></thead><tbody id="srch-tbody"></tbody></table>';

        PA.search._src = PA.pageSource(
            `/api/parser/analyses/${encodeURIComponent(n)}/global-search`, { q: serverQ });

        PA.tf.init('srch', PA.search._src, 50, PA.search._renderRow, {
            serverSorts: {
                0: { asc: 'TYPE_ASC', desc: 'TYPE_DESC' },
                2: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                1: { asc: 'SCHEMA_ASC', desc: 'SCHEMA_DESC' }
            }
        });

        const srchCols = {
            0: { label: 'Type', valueFn: r => r.type },
            1: { label: 'Schema', valueFn: r => r.schema || '-' },
            2: { label: 'Name', valueFn: r => r.name || '', mode: 'text' },
            3: { label: 'Matched', valueFn: r => r.matchedField || '', mode: 'text' }
        };
        PA.tf.pendingColFilters('srch', srchCols);
        fetch(`/api/parser/analyses/${encodeURIComponent(n)}/global-search/summary?q=${encodeURIComponent(serverQ)}`)
            .then(res => res.ok ? res.json() : null)
            .then(summary => {
                if (!summary || !summary.columnValues) {
                    PA.tf.colFiltersFailed('srch', srchCols, 'no filter values returned');
                    return;
                }
                PA.tf.initColFilters('srch', srchCols, {
                    '0': summary.columnValues['0'] || [],
                    '1': summary.columnValues['1'] || []
                });
            })
            .catch(() => {
                PA.tf.colFiltersFailed('srch', srchCols, 'request failed');
            });
    },

    /** See plsql/js/search.js's identical helper for the full rationale: a cursor/variable's own
     *  `name` is very often a generic, reused local identifier ("c1" in every procedure), so
     *  distinct rows can render visually identical without this. */
    _rowName(r) {
        const raw = r.raw || {};
        const proc = (Array.isArray(raw.accessDetails) && raw.accessDetails[0] && raw.accessDetails[0].procedureName)
            || raw.procedureName || raw.procedureId;
        if (proc && r.name && String(proc).toLowerCase() !== String(r.name).toLowerCase()) {
            return r.name + ' (' + proc + ')';
        }
        return r.name;
    },

    /** See plsql/js/search.js's identical helper for the full rationale — the parser stores
     *  queryText/sqlText with ALL whitespace stripped; this re-inserts spacing around common SQL
     *  keywords/punctuation purely for display readability. */
    _SQL_KEYWORDS: new Set(['SELECT', 'FROM', 'WHERE', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER',
        'JOIN', 'ON', 'AND', 'OR', 'AS', 'GROUP', 'ORDER', 'BY', 'HAVING', 'UNION', 'SET', 'VALUES',
        'INTO', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'UPDATE', 'DELETE', 'INSERT', 'MERGE', 'TRUNCATE', 'CREATE', 'DROP', 'ALTER', 'EXEC',
        'EXECUTE', 'DECLARE', 'COMMIT', 'ROLLBACK', 'TOP', 'EXISTS']),

    /** See plsql/js/search.js's identical helper for the full rationale — a real tokenizer, not a
     *  keyword regex (which matched keyword TEXT anywhere, corrupting identifiers that merely
     *  contain the same letters, e.g. "component_id" -> "comp on ent_id"; confirmed via a live
     *  screenshot, and strictly worse than the original unreadable-but-correct jammed text). */
    _SQL_KEYWORDS_BY_LEN: null,
    _sqlKeywordsByLen() {
        if (!PA.search._SQL_KEYWORDS_BY_LEN) {
            PA.search._SQL_KEYWORDS_BY_LEN = Array.from(PA.search._SQL_KEYWORDS).sort((a, b) => b.length - a.length);
        }
        return PA.search._SQL_KEYWORDS_BY_LEN;
    },

    /** See plsql/js/search.js's identical helper for the full rationale — case-based scan (this
     *  data's keywords are always uppercase, identifiers always lowercase), not a boundary-based
     *  tokenizer, since "SELECTp.product_id" has no character boundary at all between "SELECT"
     *  and "p" for a boundary-based scan to find. */
    /** See plsql/js/search.js's identical helper for the full rationale — a single forward scan
     *  that only cuts at an EXACT keyword match, absorbing everything else (including PascalCase
     *  aliases like "OrderId") into one blob, since the previous version isolated every lone
     *  capital letter and shredded those into "O rder I d". */
    _splitRun(token) {
        const kws = PA.search._sqlKeywordsByLen();
        const parts = [];
        let pos = 0;
        let blobStart = 0;
        const n = token.length;
        while (pos < n) {
            let matched = null;
            if (token[pos] >= 'A' && token[pos] <= 'Z') {
                for (let k = 0; k < kws.length; k++) {
                    if (token.startsWith(kws[k], pos)) { matched = kws[k]; break; }
                }
            }
            if (matched) {
                if (pos > blobStart) parts.push(token.slice(blobStart, pos));
                parts.push(matched);
                pos += matched.length;
                blobStart = pos;
            } else {
                pos++;
            }
        }
        if (blobStart < n) parts.push(token.slice(blobStart, n));
        return parts;
    },

    _prettifySql(text) {
        if (!text) return text;
        let out = '';
        let i = 0;
        const n = text.length;
        while (i < n) {
            const c = text[i];
            if (/[A-Za-z0-9_]/.test(c)) {
                let j = i;
                while (j < n && /[A-Za-z0-9_]/.test(text[j])) j++;
                const parts = PA.search._splitRun(text.slice(i, j));
                if (out.length && /[A-Za-z0-9_)]$/.test(out)) out += ' ';
                out += parts.join(' ');
                i = j;
            } else if (c === ',') {
                out += ', ';
                i++;
                while (i < n && text[i] === ' ') i++;
            } else {
                out += c;
                i++;
            }
        }
        return out.replace(/\s{2,}/g, ' ').trim();
    },

    /** See plsql/js/search.js's identical helper for the full rationale — DYNAMIC_SQL's real field
     *  is `sqlExpression`, not `expression`, and JOIN's useful detail (`onPredicate`) lives inside
     *  `raw.accessDetails[0]`, not at the top level. */
    _rowMatchedPreview(r) {
        const raw = r.raw || {};
        const firstDetail = Array.isArray(raw.accessDetails) && raw.accessDetails[0] ? raw.accessDetails[0] : null;
        const firstPredicate = Array.isArray(raw.predicates) && raw.predicates[0] ? raw.predicates[0] : null;
        const raw_text = raw.queryText || raw.sqlText || raw.content || raw.sqlExpression || raw.expression
            || (firstDetail && (firstDetail.onPredicate || firstDetail.queryText))
            || (firstPredicate && (firstPredicate.columnName + ' ' + firstPredicate.operator + ' ' + firstPredicate.value))
            || '';
        const text = PA.search._prettifySql(raw_text);
        if (text) return text.length > 80 ? text.slice(0, 80) + '…' : text;
        return r.matchedField || '';
    },

    /** See plsql/js/search.js's identical helper for the full rationale — a real Line column is a
     *  more universal, useful signal than the "Matched" text guess, especially for facets (like
     *  PROCEDURE) that have no extra snippet to show at all. */
    _rowLine(r) {
        const raw = r.raw || {};
        if (raw.lineNumber) return raw.lineNumber;
        if (raw.lineStart) return raw.lineStart;
        const firstDetail = Array.isArray(raw.accessDetails) && raw.accessDetails[0] ? raw.accessDetails[0] : null;
        if (firstDetail && firstDetail.lineNumber) return firstDetail.lineNumber;
        const firstPredicate = Array.isArray(raw.predicates) && raw.predicates[0] ? raw.predicates[0] : null;
        if (firstPredicate && firstPredicate.lineNumber) return firstPredicate.lineNumber;
        return '';
    },

    /** See plsql/js/search.js's identical helper for the full rationale. */
    _kindBadge(r, esc) {
        if (r.type !== 'TABLE') return '';
        const kind = PA.classifyTableKind(r.name);
        return kind.code === 'TABLE' ? '' : ` <span class="kind-badge ${kind.cls}">${esc(kind.label)}</span>`;
    },

    _renderRow(r, idx, esc) {
        if (!PA.search._rowMatches(r)) return '';
        return `<tr class="srch-row" onclick="PA.search.openRow(${idx})" data-srch-idx="${idx}">`
            + `<td><span class="lp-type-badge ${esc(r.type)}">${esc(r.type)}</span></td>`
            + `<td>${esc(PA.search._rowName(r))}${PA.search._kindBadge(r, esc)}</td>`
            + `<td>${esc(r.schema || '-')}</td>`
            + `<td style="color:var(--text-muted)">${esc(String(PA.search._rowLine(r) || '-'))}</td>`
            + `<td style="color:var(--text-muted);font-size:.8em">${esc(PA.search._rowMatchedPreview(r))}</td>`
            + `</tr>`;
    },

    _focusGrid(tableId, inputId, query) {
        PA.tf.setSearch(tableId, query);
        PA.tf.filter(tableId);
        const input = document.getElementById(inputId);
        if (input) input.value = query;
    },

    openRow(idx) {
        const s = PA.tf.state('srch');
        if (!s) return;
        const item = s.src ? s.filtered[idx - s.page * s.pageSize] : s.filtered[idx];
        if (!item) return;
        const raw = item.raw || {};

        switch (item.type) {
            case 'PROCEDURE': {
                const nodeId = raw.nodeId || item.name;
                PA.search.close();
                PA.showProcedure(nodeId);
                return;
            }
            case 'TABLE': {
                PA.search.close();
                PA.switchLeftTab('tables');
                PA.tableOps.focusTable(item.name);
                return;
            }
            case 'JOIN': {
                PA.search.close();
                PA.switchRightTab('joins');
                setTimeout(() => PA.search._focusGrid('join', 'joinSearch', raw.leftTable || item.name), 0);
                return;
            }
            case 'CURSOR': {
                PA.search.close();
                PA.switchRightTab('details');
                PA.detailsTab.switchSub('cursors');
                setTimeout(() => PA.search._focusGrid('cur', 'curSearch', item.name), 0);
                return;
            }
            case 'PREDICATE': {
                PA.search.close();
                PA.switchRightTab('predicates');
                setTimeout(() => PA.search._focusGrid('pred', 'predSearch', item.name), 0);
                return;
            }
            case 'SEQUENCE':
            case 'DYNAMIC_SQL':
            default: {
                const pid = raw.procedureId
                    || (Array.isArray(raw.accessDetails) && raw.accessDetails[0]
                        && (raw.accessDetails[0].procedureId || raw.accessDetails[0].procedureName));
                PA.search.close();
                if (pid) PA.showProcedure(pid);
                return;
            }
        }
    }
};
