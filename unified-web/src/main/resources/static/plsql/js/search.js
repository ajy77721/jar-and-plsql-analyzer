/**
 * PA.search — the REAL cross-facet Global Search results screen (UI-045..UI-050, §256).
 *
 * Replaces `#globalSearch`'s old `PA.globalSearch` (`app.js`) — an 11-line, network-free, same-tab
 * DOM substring filter — with a genuine server-side search across all 7 real facets (tables,
 * procedures, sequences, cursors, joins, dynamic SQL, predicates), rendered as ONE unified,
 * paginated, column-filterable results table with a `Type` column (per the user's explicit
 * sign-off in §255 — not grouped per-facet sections).
 *
 * Backend contract (`ParserAnalysisController.getSearch`/`getSearchSummary`, `ParserFacetSpecs.
 * SEARCH`). Path is `global-search`, not `search` — `PlsqlSearchController` already owns
 * `/analyses/{name}/search` for a pre-existing, unrelated feature (§B13's cross-analysis STATEMENT
 * search, a `op:UPDATE table:X` query language over individual SQL statements); reusing `/search`
 * threw Spring's "Ambiguous mapping" at boot (confirmed on this feature's own first restart), so
 * this one is `global-search` throughout, both sides:
 *   GET /api/parser/analyses/{name}/global-search?q=...&limit=...&after=...&sort=...&cf=...
 *     -> the standard page envelope ({items, page}), each item shaped
 *        {type, name, schema, matchedField, _idx, raw}
 *   GET /api/parser/analyses/{name}/global-search/summary?q=...
 *     -> {columnValues: {"0": [...types], "1": [...schemas]}, ...}
 *
 * `cf` column-index contract — THE ONE THING THIS FILE MUST KEEP IN SYNC WITH
 * `ParserFacetSpecs.SEARCH` (see that class's own doc comment, which restates the same numbers):
 *   "0" = type, "1" = schema.
 *
 * Only wired for a parser-backed analysis (`PA.api._parserName()`), because the new backend
 * endpoint only exists on `ParserAnalysisController` — the plsql live-crawl backend
 * (`AnalyzerController`/`PlsqlFacetSpecs`) has no such endpoint yet. On a live-crawl analysis, the
 * modal's own input falls back to the old same-tab substring filter (`PA.globalSearch` in
 * `app.js`) instead of hitting this endpoint — a deliberate, scoped decision, not an oversight.
 *
 * UI redesign (this revision) — the topbar `#globalSearch` live-filter input is gone; a single
 * "Search" button now opens this modal directly. Inside the modal: one text box plus three
 * Notepad++/VS Code-style find toggles (Match case / Whole word / Regex) instead of typing straight
 * into the topbar. Matching those toggles happens CLIENT-SIDE, on top of the server's own
 * broader substring recall (see `_serverQueryFor`/`_rowMatches`) — the server still narrows the
 * dataset with a plain literal-substring `q` (cheap, cache-friendly, unchanged contract), and the
 * three toggles apply the stricter real rule to what actually renders. One side effect worth
 * knowing: a fetched page of 50 rows can render fewer than 50 after a strict toggle (regex/whole
 * word/case) trims some out — the pager's total reflects the server's broader count, not the
 * post-toggle count, so pages can look sparse. Full backend-side regex support would need a new
 * query parameter across three controllers; out of scope for this pass.
 */
window.PA = window.PA || {};

PA.search = {
    _src: null,
    _lastQuery: '',
    _matcher: null,
    _opts: { cs: false, whole: false, regex: false },

    /** Entry point — the topbar "Search" button's onclick. Opens the modal directly. */
    open() {
        if (!(PA.api && PA.api._parserName && PA.api._parserName())) {
            // Not a parser-backed analysis — no cross-facet endpoint exists server-side for this
            // backend yet (see file header). Old same-tab filter is the only thing that works here;
            // focus it directly rather than opening a modal that can't fetch anything.
            const legacyInput = document.getElementById('globalSearch');
            if (legacyInput) { legacyInput.focus(); return; }
            return;
        }
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
            const raw = localStorage.getItem('mdb-search-opts:plsql');
            if (raw) Object.assign(PA.search._opts, JSON.parse(raw));
        } catch (e) { /* ignore malformed storage */ }
    },

    _saveOpts() {
        try { localStorage.setItem('mdb-search-opts:plsql', JSON.stringify(PA.search._opts)); } catch (e) { /* ignore */ }
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

    /** Longest literal (non-metacharacter) run in a regex/whole-word query — used as the actual
     *  server-side `q` so the server's plain substring recall still returns a usable candidate set
     *  for the client-side regex/whole-word pass to narrow further. See file header note. */
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
            // Invalid regex mid-typing (e.g. an unclosed bracket) — fail OPEN (show the server's
            // unfiltered candidate set) rather than showing an error wall for every keystroke.
            PA.search._matcher = null;
        }
    },

    _rowMatches(r) {
        const q = PA.search._lastQuery || '';
        if (!q) return true;
        const o = PA.search._opts;
        // Plain, no-toggle mode: trust the server's own match decision completely. A row's
        // name/schema/matchedField do not reliably contain the actual matched TEXT — matchedField
        // is a FIELD LABEL (e.g. "content"), not the matched substring — so re-checking just those
        // three fields client-side incorrectly rejected real, server-confirmed matches (confirmed
        // live: a cursor matched via its embedded SQL text was dropped to 0 rendered rows despite
        // the server genuinely returning it). Only the opt-in toggles below re-filter at all.
        if (!o.regex && !o.whole && !o.cs) return true;
        // Regex/whole-word/case-sensitive toggles need a real haystack to test against — broaden
        // beyond name/schema to the full raw payload so there is an actual chance of testing the
        // matched content, not just field labels.
        let hay;
        try { hay = (r.name || '') + ' ' + (r.schema || '') + ' ' + JSON.stringify(r.raw || {}); }
        catch (e) { hay = (r.name || '') + ' ' + (r.schema || ''); }
        if (o.regex || o.whole) return PA.search._matcher ? PA.search._matcher.test(hay) : true;
        return hay.includes(q);
    },

    /**
     * Explicit user-confirmed 4-segment layout (agreed via mockup before building):
     *   1. Header + full-width search textbar (with the Aa/ab/.* toggles)
     *   2. Pagination detail bar — its OWN row, directly below the textbar, never sharing a
     *      scroll container with the table (that's what let it look like it was "sitting on top of"
     *      or overlapping the table before)
     *   3+4. The results table itself, in its own scrollable region below the pager, with a real
     *      fixed column layout (.srch-table, shared/mongodb-theme.css) so Matched gets the most
     *      room and Type/Name/Schema/Line never grow/shrink based on content length.
     */
    _ensureOverlay() {
        if (document.getElementById('searchOverlay')) return;
        const html = `
<div id="searchOverlay" style="display:none;position:fixed;inset:0;z-index:9000;background:rgba(0,0,0,.45);align-items:flex-start;justify-content:center">
  <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;margin-top:48px;width:min(1100px,94vw);max-height:82vh;display:flex;flex-direction:column;box-shadow:var(--shadow-lg)">
    <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--border)">
      <div style="font-size:.95rem;font-weight:600">Search</div>
      <button class="btn btn-sm" onclick="PA.search.close()" title="Close">&times;</button>
    </div>
    <!-- Segment 1: search textbar -->
    <div style="display:flex;gap:8px;align-items:center;padding:10px 16px;border-bottom:1px solid var(--border)">
      <input type="text" id="srchQueryInput" class="form-input" placeholder="Find..." style="flex:1" oninput="PA.search._onInput(this.value)">
      <button class="srch-toggle-btn" id="srchToggleCase" title="Match case" onclick="PA.search._toggleOpt('cs')">Aa</button>
      <button class="srch-toggle-btn" id="srchToggleWord" title="Match whole word" onclick="PA.search._toggleOpt('whole')">ab</button>
      <button class="srch-toggle-btn" id="srchToggleRegex" title="Use regular expression" onclick="PA.search._toggleOpt('regex')">.*</button>
    </div>
    <!-- Segment 2: pagination detail bar, own row, own scroll boundary from the table below -->
    <div id="srch-pager" class="pager srch-pagerbar" data-moved-top="1" style="padding:8px 16px;border-bottom:1px solid var(--border);flex-shrink:0"></div>
    <!-- Segment 3+4: the results table, its own scrollable region -->
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
        const n = PA.api._parserName();
        const serverQ = PA.search._serverQueryFor(q);
        const container = document.getElementById('srchContainer');
        // .srch-table (shared/mongodb-theme.css) — a REAL fixed column layout (table-layout:fixed
        // + <colgroup> percentages), confirmed via mockup: Type/Name/Schema/Line stay narrow and
        // fixed regardless of content length, Matched gets the majority of the width. Before this,
        // "table-layout:auto" (the .to-table default) let one long cell's content push every other
        // column's width around row to row, which is what actually caused the "column not aligned
        // with header" look — not a CSS load/cache problem.
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

    /**
     * A cursor/variable/dynamic-SQL row's own `name` is very often a generic, reused local
     * identifier (e.g. every procedure's first explicit cursor is conventionally named "c1") —
     * confirmed live: a 96-row "prod" result set was ALL genuinely different objects (different
     * owning procedures: erp_usp_generate_invoice, erp_usp_init_mrp_run, ...) but rendered as 96
     * visually IDENTICAL rows because Name was always "c1" and Matched was always the generic field
     * label "content". Appending the owning procedure disambiguates rows that are otherwise
     * indistinguishable in the grid, without changing what object each row actually points to.
     */
    _rowName(r) {
        const raw = r.raw || {};
        const proc = (Array.isArray(raw.accessDetails) && raw.accessDetails[0] && raw.accessDetails[0].procedureName)
            || raw.procedureName || raw.procedureId;
        if (proc && r.name && String(proc).toLowerCase() !== String(r.name).toLowerCase()) {
            return r.name + ' (' + proc + ')';
        }
        return r.name;
    },

    /** `matchedField` from the server is a FIELD LABEL ("content", "queryText", ...), not the
     *  matched text itself — genuinely useless for telling two same-named rows apart. Show an
     *  actual snippet of the real content instead wherever one is available on the raw payload. */
    /**
     * The parser stores queryText/sqlText with ALL whitespace stripped (e.g. "SELECTp.product_id,
     * p.sku,COALESCE(..." — no space anywhere, not even between SELECT and the column list) —
     * confirmed live via the raw API response. That's a legitimate storage-side normalization
     * this file has no business changing, but showing it verbatim in a search results column is
     * genuinely unreadable ("too rocky" per explicit user feedback comparing it to JAR's cleaner
     * search results). This re-inserts spacing around common SQL keywords/punctuation purely for
     * display — heuristic, not a real SQL formatter, but turns "SELECTp.x,p.yFROMt" into
     * "SELECT p.x, p.y FROM t", which is the actual readability gap being fixed.
     */
    _SQL_KEYWORDS: new Set(['SELECT', 'FROM', 'WHERE', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER',
        'JOIN', 'ON', 'AND', 'OR', 'AS', 'GROUP', 'ORDER', 'BY', 'HAVING', 'UNION', 'SET', 'VALUES',
        'INTO', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'UPDATE', 'DELETE', 'INSERT', 'MERGE', 'TRUNCATE', 'CREATE', 'DROP', 'ALTER', 'EXEC',
        'EXECUTE', 'DECLARE', 'COMMIT', 'ROLLBACK', 'TOP', 'EXISTS']),

    /**
     * A real tokenizer, not regex-guessing — the first version of this (a plain keyword regex with
     * lookahead/lookbehind) matched keyword TEXT anywhere it appeared, including inside identifiers
     * that merely happen to contain the same letters: "component_id" has "on" in it, so that regex
     * turned it into "comp on ent_id"; "inventory" became "in vent or y"; "JOIN" (already a real,
     * correctly-spelled keyword) got split into "JO IN" because "IN" is also a keyword and the regex
     * matched it as a separate substring inside "JOIN" on a second pass. All three are real
     * corruptions confirmed in a live screenshot — this was strictly worse than the original
     * unreadable-but-at-least-not-WRONG jammed text.
     *
     * This version walks the string once, grouping each maximal run of [A-Za-z0-9_] into ONE token
     * (so "component_id" is a single token, never inspected for a "on" substring inside it) and only
     * decides a run is a keyword by an EXACT, WHOLE-token match against `_SQL_KEYWORDS` — the same
     * reason a real tokenizer never has this bug and a regex almost always does. A separating space
     * is inserted between two directly-touching word tokens only when at least one of them is a real
     * keyword (two real identifiers never touch with zero separator in valid SQL — only a keyword
     * concatenated onto an identifier does, which is the actual bug being fixed: "SELECTp.x",
     * "ONi.id", etc.).
     */
    // Longest-first so a prefix match (e.g. "IN") never wins over a longer real keyword that
    // starts the same way ("INTO", "INNER") when scanning greedily below.
    _SQL_KEYWORDS_BY_LEN: null,
    _sqlKeywordsByLen() {
        if (!PA.search._SQL_KEYWORDS_BY_LEN) {
            PA.search._SQL_KEYWORDS_BY_LEN = Array.from(PA.search._SQL_KEYWORDS).sort((a, b) => b.length - a.length);
        }
        return PA.search._SQL_KEYWORDS_BY_LEN;
    },

    /**
     * A plain "maximal alnum run = one token" tokenizer (this function's previous revision)
     * cannot fix this data at all: "SELECTp.product_id" has NO character-level boundary between
     * "SELECT" and "p" — both are ordinary letters with nothing between them — so the whole
     * "SELECTp" is unavoidably read as a single, uninterrupted run by any boundary-based scan.
     * Confirmed live: that revision left "SELECTp.product_id" completely unchanged.
     *
     * The actual signal available here is CASE, not boundaries: every real keyword in this data
     * is spelled in caps ("SELECT", "FROM", "AS", "ON", ...) while every identifier is lowercase
     * snake_case ("product_id", "component_id", "erp_product") — confirmed against every example
     * seen so far. So within one continuous alnum run, this scans left to right and, at any
     * position starting with an uppercase letter, greedily tries the longest known keyword as an
     * exact, case-sensitive prefix match ("LEFTJOIN" -> "LEFT" then, at the new position, "JOIN" —
     * two real keywords concatenated with no separator between them, both recovered). Lowercase
     * runs (identifiers — "component_id", "inventory", ...) never contain an uppercase letter at
     * all, so this scan never enters keyword-matching for them and they pass through completely
     * unmodified — which is exactly what the previous, regex-based revision got wrong (it matched
     * keyword TEXT anywhere, lowercase or not, corrupting "component_id" into "comp on ent_id").
     */
    _splitRun(token) {
        // Real bug, found live on SQL Server dynamic SQL text: this function's first version
        // (below, in spirit) isolated every lone uppercase letter that wasn't itself a keyword
        // match as its own token — meant to catch a genuine all-caps constant/alias, but PascalCase
        // column aliases ("OrderId", "CustomerName" — completely normal in this data) got shredded
        // into "O rder I d" / "C ustomer N ame", i.e. a space forced in front of every capital
        // letter. Confirmed corrupting real content in a live screenshot.
        //
        // Rewritten as a single forward scan instead: keep extending ONE running "everything else"
        // blob character by character, and only cut it — flushing the blob as its own token, then
        // emitting the keyword as a separate token — at a position where an EXACT, case-sensitive
        // keyword match starts. A capital letter that is not the start of a real keyword is simply
        // absorbed into the blob like any other character, so "OrderId" is never touched at all
        // (no substring of it matches a keyword), while "SELECTp"/"LEFTJOIN" still split correctly
        // (their prefixes DO exactly match real keywords).
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
                // Only force a separating space when the run touches the PREVIOUS run with zero
                // separator at all (out ends in a word char, e.g. after ")0)" this run starts a
                // fresh word — no, ")" is punctuation, so no space forced there; only a case like
                // "SELECT" immediately followed by this alnum run, which can't happen since this
                // whole branch only fires once per run anyway). Punctuation already in `out`
                // (".", "(", ",", a space) must NOT get an extra space glued on before an
                // identifier — "erp.erp_product" must stay "erp.erp_product", not "erp. erp_product".
                if (out.length && /[A-Za-z0-9_)]$/.test(out)) out += ' ';
                out += parts.join(' ');
                i = j;
            } else if (c === ',') {
                out += ', ';
                i++;
                while (i < n && text[i] === ' ') i++; // avoid a double space if one already existed
            } else {
                out += c;
                i++;
            }
        }
        return out.replace(/\s{2,}/g, ' ').trim();
    },

    /**
     * Real gap found live (multiple facet types, not just cursors): the field-name guesses here
     * were wrong for several types, so their rows fell through to the generic field-LABEL fallback
     * ("name", "content") instead of real content — confirmed via the actual API response:
     * DYNAMIC_SQL's real field is `sqlExpression` (this checked `expression`, which doesn't exist
     * on this payload at all), and JOIN's useful detail (`onPredicate`) lives one level down, inside
     * `raw.accessDetails[0]`, not at the top level. Both are added below alongside the existing
     * queryText/sqlText/content chain (cursors/tables), so every facet type gets a real snippet
     * instead of a label whenever the underlying data actually has one.
     */
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

    /**
     * Explicit user direction: a real Line column (this data already carries real line numbers
     * for almost every facet) is a much more useful, universal signal than the "Matched" text
     * guessing game — especially for PROCEDURE/TABLE rows, which have no extra snippet to show at
     * all (the row itself IS the match) but do have a concrete location. Same fallback-chain
     * pattern as `_rowMatchedPreview`: check the facet's own top-level `lineNumber` first, then
     * the first `accessDetails`/`predicates` entry one level down.
     */
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

    /** Explicit user direction: tag TABLE rows as actual table vs. SQL Server temp table/table
     *  variable — see PA.classifyTableKind's doc comment (shared/tableFramework.js). */
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
                PA.switchRightTab('cursors');
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
