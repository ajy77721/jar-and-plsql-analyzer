# Frontend Audit

**Date:** 2026-09-05
**Scope:** `unified-web/src/main/resources/static/`
**Companion document:** [`TASKS-FRONTEND.md`](TASKS-FRONTEND.md) — the improvement task list derived from this audit.

## Purpose and boundaries

This audit answers one question: **which frontend features are actually live, and what is wrong with them.**

Per the user's instruction, **disabled and dead components are out of scope.** They are recorded here as a
bare list only — name, page, one line of evidence — so that a later decision to delete or revive them is
possible. They receive no findings and no tasks.

A third list, **reachability unresolved**, holds features where liveness could not be established with
evidence. They are excluded from the task list. A wrong "live" wastes work; a wrong "dead" silently drops
a feature the user still relies on, so neither guess is made.

## Method

A feature is counted **LIVE** only when all of the following hold:

1. Its script is actually loaded by a page (a `<script src>` tag that is not commented out).
2. There is a reachable entry point — a nav item, tab, route, button or handler a user can actually trigger.
   *A file being loaded is not the same as a feature being reachable.*
3. It is not behind a flag that is off, or a guard that is never true.
4. If it calls an API, that endpoint still exists in the Java source under `unified-web/src/main/java/`.

Failing any one of these puts the feature in the dead list. Being unable to determine one of them puts it in
the unresolved list.

Every claim below cites `file:line`. Counts are computed, not estimated; where a number is an estimate it is
labelled as such.

## Measured baseline

| Metric | Value | How measured |
|---|---|---|
| HTML pages | 7 | `find static -name '*.html'` |
| JS files | 141 | `find static -name '*.js'`: shared 14, parser 57, jar 41, plsql 29 |
| JS lines of code | 62,417 | `wc -l` over all `.js` excluding the vendored `xlsx*` bundles |
| Largest JS file | `plsql/js/app.js`, 4,005 lines | `wc -l` |
| `fetch(` call sites | 314 across 45 files | `grep -rn 'fetch(' --include='*.js'` |
| Files calling `fetch` with no `catch` anywhere in the file | 4 | `shared/chat-api.js`, `shared/page-envelope.js`, `parser/js/api.js`, `plsql/js/api.js` |
| `innerHTML` assignments | 807 | `grep -rn 'innerHTML' --include='*.js'` |
| `aria-*` attributes across all JS and HTML | 13 | `grep -rn 'aria-' --include='*.js' --include='*.html'` |
| Frontend test files | 0 | no `package.json` exists anywhere in the repo; `unified-web/src/test` contains only `java` |
| Largest analysis document on disk | 47 MB (`data/jar/unified-web-1.0.0-SNAPSHOT/analysis.json`) | `du -h` |
| Largest single endpoint artifact | 7.2 MB (`.../endpoints/277_AnalyzerController.claudeCorrect/call-tree.json`) | `du -h` |

Scale targets the frontend must eventually survive, per the user: **8,000 procedures, 1,000,000 DB
operations, a 100M-line Java codebase.** Today's corpus is 15,824 statements. Findings marked `[SCALE]`
are judged against the target, not against today's corpus.

## Page map

Measured from the `<script src>` tags in each page.

| Page | Lines | Page-specific JS | Notes |
|---|---|---|---|
| `static/index.html` | 1,070 | none (inline, from line 348) | Home / launcher. Loads only `shared/*`. |
| `static/login.html` | 46 | none | Static stub, see dead list. |
| `static/jar/index.html` | 267 | 40 of 41 in `jar/js/` | One tag commented out at `jar/index.html:219`. |
| `static/parser/index.html` | 866 | 57 in `parser/js/` | Script block at `parser/index.html:762-836`. |
| `static/plsql/index.html` | 767 | 29 in `plsql/js/` | Script block at `plsql/index.html:716-747`. |
| `static/war/index.html` | 220 | 2 in `war/js/` + 33 borrowed from `/jar/js/` | Largely a reskin of the JAR page. |
| `static/conversion/index.html` | 851 | none (all inline, from line 331) | Loads only `shared/*`. |

## Cross-cutting measurement: forked modules

This is measured, not inferred. Files sharing a basename across page directories were compared with `md5`
and `diff`. **No pair is identical** — every one has diverged, which means a fix applied to one copy does
not reach the others.

Core modules, one copy per page, all diverged:

| Module | jar | war | parser | plsql |
|---|---|---|---|---|
| `api.js` | 505 | 335 | 379 | 582 |
| `app.js` | 1,612 | — | 1,125 | 4,005 |
| `callGraph.js` | 566 | — | 494 | 421 |
| `summary.js` | 665 | — | 234 | 278 |
| `utils.js` | 445 | — | 109 | — |
| `tableFramework.js` | — | — | 752 | 457 |
| `tableOps.js` | — | — | 368 | 405 |
| `export.js` | — | — | 1,405 | 283 |
| `sourceView.js` | — | — | 1,803 | 460 |
| `joins.js` | — | — | 460 | 277 |
| `cursors.js` | — | — | 301 | 293 |
| `config.js` | — | — | 313 | 362 |
| `logViewer.js` | — | — | 108 | 116 |

Line counts are `wc -l`. All md5 sums differ.

The `summary-*` family is forked between `jar/js/` and `plsql/js/` in 13 modules, and the divergence is
large rather than cosmetic — `diff | grep -c '^[<>]'` differing-line counts:

| Module | jar lines | plsql lines | differing lines |
|---|---|---|---|
| `summary-tables.js` | 1,392 | 564 | 1,740 |
| `summary-export-excel.js` | 868 | 430 | 1,116 |
| `summary-claude.js` | 919 | 390 | 1,093 |
| `summary-helpers.js` | 1,103 | 645 | 1,086 |
| `summary-analyzer.js` | 556 | 296 | 738 |
| `summary-corrections.js` | 419 | 170 | 523 |
| `summary-explorer.js` | 495 | 192 | 503 |
| `summary-trace.js` | 398 | 150 | 458 |
| `summary-export-modal.js` | 453 | 191 | 454 |
| `summary-export.js` | 131 | 77 | 162 |
| `summary-corr-logbrowser.js` | 130 | 38 | 126 |
| `summary-export-style.js` | 194 | 160 | 50 |
| `summary-col-filter.js` | 189 | 178 | 35 |

One concrete hazard found while measuring: `parser/index.html:773` and `plsql/index.html:723` both load
`js/tableOps.js?v=1788504053` — **the same cache-busting version stamp on two different files** (parser 368
lines, plsql 405 lines, different md5). The version stamps are therefore not a reliable signal of which
copies are in sync, and cannot be used to reason about the fork.

## Cross-cutting measurement: HTML construction and escaping

Also measured directly, and it cuts across every live feature on every page.

The UI is built almost entirely by assigning HTML strings to `innerHTML`, and click behaviour is attached by
writing `onclick="..."` into those strings rather than by binding listeners.

| Directory | `innerHTML` sites | `escapeHtml` references |
|---|---|---|
| `parser/js` | 297 | **0** |
| `jar/js` | 242 | 171 |
| `plsql/js` | 215 | **0** |
| `shared` | 53 | 0 (uses local `_esc`/`esc` helpers instead) |
| `war/js` | 0 | 0 |

There is exactly **one** `escapeHtml` implementation in the codebase, at `jar/js/utils.js:104`, exposed as
`JA.utils.escapeHtml`. The `parser/` and `plsql/` trees do not reference it and define no equivalent, so
their 512 combined `innerHTML` sites interpolate values with no escaping at all. `shared/access-details.js:39`
defines a third, private `esc()`; `shared/chat.js:881` and `shared/user-manual.js:301` each define a fourth
and fifth via the `div.textContent`/`div.innerHTML` round-trip.

Values interpolated into these templates are database and source identifiers — table names, column names,
procedure names, SQL fragments, exception messages — which are attacker-influenced only insofar as the
analysed corpus is, but are very much able to contain quotes and angle brackets. An identifier containing a
single quote breaks out of the generated `onclick="doThing('NAME')"` attribute and silently disables the
control; one containing `<` truncates the rendered row.

Related keyboard/accessibility measurement across all HTML and JS:

| Metric | Count |
|---|---|
| `onclick=` written from JS template strings | 1,108 |
| ...of those, attached to a `<div>` or `<span>` | 319 |
| `onclick=` inline in HTML | 270 (plsql 85, parser 116, jar 20, war 19, home 16, conversion 14) |
| `role=` attributes | 5 |
| `tabindex` attributes | 3 |
| `aria-*` attributes | 13 |
| `keydown`/`keyup` handlers | 34 |

A `<div onclick=...>` with no `role`, no `tabindex` and no key handler is not focusable and cannot be
activated from the keyboard. 319 such controls is not a rounding error; it means large parts of the UI are
mouse-only. This is stated once here as a systemic finding rather than repeated per feature.

---

# Part 1 — LIVE feature inventory

Finding IDs use an area prefix so they can be referenced from the task list without renumbering:
`X` cross-cutting, `S` shared/home/conversion, `J` jar/war, `P` parser, `Q` plsql.

## Area S — shared, home page, conversion workbench

| # | Feature | Page(s) | Entry point | Implemented by | API endpoints | What the user does |
|---|---|---|---|---|---|---|
| S-1 | Tool cards + corpus stats | home | `index.html:165,174,183`; auto-load `index.html:599` | inline `index.html:433-518` | `GET /api/jar/jars`, `/api/war/wars`, `/api/parser/analyses` | Sees per-analyzer totals and clicks through to an analyzer. |
| S-2 | Claude API statistics bar | home | `index.html:269` `toggleClaudeStats` | inline `index.html:363-427` | `GET /api/sessions/claude-stats` | Expands a per-analyzer call/prompt/session breakdown. |
| S-3 | Progress report panel | home | Refresh `index.html:319`; initial `index.html:599` | inline `index.html:521-596` | `/api/jar/jars`, `/api/parser/analyses`, `GET /api/parser/claude/sessions` | Reads enrichment/verification progress bars. |
| S-4 | Claude sessions table | home | Refresh `index.html:337`; poll `index.html:658` | inline `index.html:601-653` | `GET /api/sessions` | Lists every Claude session with status and duration. |
| S-5 | Analysis queue (bar, modal, cancel, reorder) | home | `index.html:283` `openQueueModal`; Esc `index.html:778` | inline `index.html:770-1038` | `GET /api/queue`, `POST /api/queue/{id}/cancel`, `POST /api/queue/reorder` | Views running/queued/history, cancels a job, reorders pending jobs. |
| S-6 | Log FAB viewer | home, jar, war, parser, plsql | FAB `shared/log-fab.js:127`; init e.g. `index.html:1045` | `shared/log-fab.js` (`LogTailSource` :21) | `/api/plsql/logs`, `/api/jar/logs` | Opens a tailing log overlay with level and text filter. |
| S-7 | Classic chat panel | home, jar, war, parser, plsql | FAB `shared/chat.js:37`; init `index.html:1052` | `shared/chat.js`, `shared/chat-api.js` | `/api/chat/sessions*`, `/api/chat/files/read`, `/write` | Browses sessions, reads a paged transcript, sends messages, attaches files, approves actions. |
| S-8 | Chatbox (new chat) + mode toggle | home, jar, war, parser, plsql | FAB `shared/chatbox.js:107`; switch `shared/chat-toggle.js:107` | `shared/chatbox.js`, `shared/chat-toggle.js`, `shared/chat-api.js` | `/api/chat/config`, `/api/chat/sessions*` | Uses a floating AI assistant; the toggle picks Classic vs New. |
| S-9 | Help overlay | home, jar, war, parser | `?` button `shared/help.js:11` | `shared/help.js` (static content) | none | Opens a Business/Technical guide modal. |
| S-10 | Theme toggle + token resolver | all pages | injected button `shared/theme-toggle.js:73` | `theme-toggle.js`, `theme-colors.js` | none | Switches light/dark; persisted in localStorage. |
| S-11 | Poll interval config | home, jar, war, parser, plsql, conversion | auto-run `shared/poll-config.js:82` | `poll-config.js` | `GET /api/config/polling` | Invisible; supplies poll intervals to every other feature. |
| S-12 | Page envelope shim + API-failure banner | all pages | auto-run `shared/page-envelope.js:15` | `page-envelope.js` | none directly | Normalises paged vs bare-array responses; shows a top-of-page failure banner. |
| S-13 | UIBusy in-flight guard + SearchQuery | jar, war, parser, plsql, conversion (loaded); used only by jar page JS | `shared/ui-busy.js:119,143` | `ui-busy.js` | none | Disables a button for the life of its promise; normalises search input. |
| S-14 | Access Details table | parser, plsql | `AccessDetails.mount` `shared/access-details.js:474` | `access-details.js` | `GET /api/parser/analyses/{name}/search?q=table:…&limit=500` | Reads a paged, sortable table of table-access operations with column and predicate enrichment. |
| S-15 | Conversion workbench | conversion | Convert/Validate/Fix `conversion/index.html:214-216`; tabs `:232-235` | all inline `conversion/index.html:344-849` | 11 endpoints on `/api/conversion/*` (`ConversionController.java:105-331`) | Runs a Claude conversion job and watches streamed output and the report. |

Endpoint existence for every row above was verified by grep against `unified-web/src/main/java`.
`/api/parser/claude/sessions` resolves via `@RequestMapping("/api/parser")` plus
`@GetMapping("/claude/sessions")` in `ClaudeController.java:34,632`.

**Note on S-15 reachability:** the conversion page has no nav link — the home-page card that would
link to it is commented out at `index.html:192-200`. It is reachable only by typing `/conversion/`.
Its page and all 11 of its endpoints are real and functioning, so it is audited as live, but see the
"reachability unresolved" list.

---

# Part 2 — Findings

Ranked within each area by user impact. Severity is `high` / `med` / `low`.
Where a finding says "at the target scale", the behaviour is **estimated from the code path**, not measured;
the code fact itself (the line, the missing limit, the missing guard) is measured.

## Area X — cross-cutting

These apply to many live features at once and are stated here rather than repeated per feature.

| ID | Kind | Sev | Where | Finding |
|---|---|---|---|---|
| X-1 | CONSISTENCY | high | table in "Forked modules" above | 13 core modules and 13 `summary-*` modules are forked per page with **no identical pair**. A correctness or scale fix has to be written 2–4 times or it silently does not apply to the other pages. This is the single largest multiplier on every other finding in this document. |
| X-2 | CORRECTNESS | high | `parser/js` (297 sites), `plsql/js` (215 sites) | 512 `innerHTML` assignments with **zero** escaping — neither tree references `JA.utils.escapeHtml` (`jar/js/utils.js:104`) nor defines an equivalent. Table, column, procedure names and SQL fragments are interpolated raw. An identifier containing `'` breaks the generated `onclick="fn('NAME')"` attribute and silently disables that row's control; one containing `<` truncates the row. |
| X-3 | CONSISTENCY | med | `jar/js/utils.js:104`, `shared/access-details.js:39`, `shared/chat.js:881`, `shared/user-manual.js:301`, `shared/chatbox.js:385` | Five separate HTML-escaping implementations, three of them the `div.textContent`→`innerHTML` round-trip. There is no single shared escape utility, which is why parser and plsql have none at all. |
| X-4 | A11Y | med | 319 sites, measured | 319 `<div>`/`<span>` elements are given `onclick` from JS template strings. Against these there are 5 `role=`, 3 `tabindex` and 13 `aria-*` attributes in the entire frontend. These controls are not focusable and cannot be activated from a keyboard, so large parts of the UI are mouse-only. `shared/theme-toggle.js:61-63` shows the correct pattern already exists in the codebase. |
| X-5 | CONSISTENCY | med | `parser/index.html:773`, `plsql/index.html:723` | Both load `js/tableOps.js?v=1788504053` — the same cache-busting stamp on two **different** files (368 vs 405 lines, different md5). Version stamps cannot be used to reason about which forks are in sync, and a stale stamp will serve a stale file after an edit. |
| X-6 | CORRECTNESS | med | repo-wide | There are **no frontend tests**: no `package.json` exists anywhere in the repo and `unified-web/src/test` contains only `java`. Every finding below is therefore unguarded against regression, and no task in the companion plan can be proven done without first standing up a harness. |

## Area S — shared, home page, conversion workbench

| ID | Kind | Sev | Feature | Where | Finding and impact |
|---|---|---|---|---|---|
| S-F1 | CORRECTNESS | high | User Manual | `shared/user-manual.js:117` | `SearchQuery.norm(filter)` is an unqualified reference to a symbol defined only in `shared/ui-busy.js:143`, which `index.html` does **not** load (its scripts are at `index.html:12-15,1044-1067`). `_renderNav` runs from `_buildModal` (`:105`) during `UserManual.init()` (`index.html:1068`) and again from `open()` (`:61`), so clicking "User Manual & Documentation" (`index.html:161`) throws a ReferenceError and shows an empty modal. Home is the only page loading `user-manual.js`, so the feature is broken everywhere it exists. |
| S-F2 | SCALE | high | Analysis queue | `index.html:990`; render `index.html:859-872` | `fetch('/api/queue')` requests the entire queued list and history with no limit although the server already accepts `queuedLimit`/`historyLimit` (`QueueController.java:39-46`), and `renderQueue()` rebuilds every row by string concat into `innerHTML` on every poll tick — unconditionally, whether or not the modal is open (`index.html:993`). At the 8,000-procedure target a batch submit means an 8,000-element payload and an 8,000-row DOM rebuild every 10 s. |
| S-F3 | SCALE | high | Claude sessions table | `index.html:634-646`; poll `index.html:658` | `GET /api/sessions` (`GlobalSessionsController.java:41-43`) has no paging and returns everything; the client renders one `<tr>` per session, uncapped and unvirtualised, every 30 s. One session per enriched procedure means thousands of rows re-serialised per tick. `/api/sessions/summary` (`:104`) already exists and is unused. |
| S-F4 | SCALE | high | Tool cards, progress report | `index.html:433-453`; sums at `:467,487,505,575` | Three full list endpoints are fetched purely to compute four scalar sums client-side. The home page downloads the whole analysis index to print four numbers; no server-side aggregate is requested. |
| S-F5 | SCALE | med | Chatbox session load | `shared/chatbox.js:79-80`; re-render `:326-346` | Uses `ChatAPI.getSession(id)`, which returns the whole transcript, holds all of it in `_messages`, and re-renders every message on each send. `ChatAPI.getMessages` (`shared/chat-api.js:38`) already does cursor paging and `ChatPanel` uses it (`shared/chat.js:216`); the chatbox does not. A long-lived GLOBAL session grows unbounded. |
| S-F6 | SCALE | med | Access Details | `shared/access-details.js:339` | Hardcoded `&limit=500` on the statement search. A table touched by more than 500 statements — plausible at 1,000,000 DB operations — silently enriches only the first 500 rows; the rest fall through `applyDetail(x, undefined)` (`:376`) and render as unknown-columns, **indistinguishable from genuinely unparsed statements**. The user is not told the cap was hit. |
| S-F7 | SCALE | med | Log FAB | `shared/log-fab.js:233-238`, `:55-63`, `:200` | Every filter change resets the seq cursor and refetches `FIRST=1000` lines (`:22`), and the whole buffer is re-assigned as one string into `content.textContent`. Typing a 10-character query issues a 1,000-line request per 250 ms debounce boundary. O(buffer) per keystroke group, no virtualisation. |
| S-F8 | SCALE | med | Conversion live stream | `conversion/index.html:534-537` | `_pullStream` recurses while `env.page.hasMore`, 500 lines per round trip, appending each batch to the DOM, with no recursion cap and no yield. Reconnecting to a job with a large backlog locks the tab while it drains. |
| S-F9 | CORRECTNESS | med | Home page fetches | `index.html:435-451` | `fetchJars`/`fetchWars`/`fetchParserAnalyses` call `res.json()` with **no `res.ok` check**. A 500 returning an HTML error page throws a parse error into the generic catch and renders `-` (`:478`) with no explanation. `index.html:365` and `:557` do check `r.ok`, so the page is inconsistent with itself. |
| S-F10 | CORRECTNESS | med | Home refresh / caching | `index.html:319`, `:429-458` | The Refresh button has no in-flight guard; repeated clicks start overlapping `loadStats()`+`loadProgress()`+`loadClaudeStats()`. `_cachedJars` is only populated after the await (`:436`), so N concurrent calls issue N identical requests, and later responses can overwrite earlier ones with no sequencing. `UIBusy.run`/`UIBusy.seq` exists for exactly this (`shared/ui-busy.js:119`) but `index.html` does not load `ui-busy.js`. |
| S-F11 | CORRECTNESS | med | Queue cancel / reorder | `index.html:926-936`, `:949-969` | `cancelJob` returns without re-polling, so the row survives until the next tick (up to 30 s on an idle queue, `index.html:1015`) and users click Cancel repeatedly. `applyOrder` disables its button, but `renderQueue()` at `:968` immediately regenerates the markup and discards the disabled state, so a second Apply can race the first. |
| S-F12 | ERRORS | med | Conversion job list | `conversion/index.html:402-408` | `_loadJobs` has an empty `.catch(e) {}` and no `res.ok` check. If `/api/conversion/jobs` fails, the list shows "No jobs yet." (`:646`) — indistinguishable from a genuinely empty list. Same shape at `:395-399` for `claude-status`, which at least degrades to "Claude status unknown". |
| S-F13 | CONSISTENCY | med | Markdown rendering | `conversion/index.html:757-834`, `shared/user-manual.js:186-296`, `shared/chatbox.js:350-359` | Three independent hand-rolled Markdown renderers differing in escaping order, table handling and list handling, so the same report text renders differently depending on where it is shown. `user-manual.js:222` applies inline-code substitution *after* escaping while `:217` escapes inside the callback — inconsistent within one file. |
| S-F14 | CONSISTENCY | med | In-flight guards | `shared/ui-busy.js`; `conversion/index.html:429,443,454-456` | `UIBusy.run`/`UIBusy.seq` is loaded by jar/war/parser/plsql/conversion but conversion uses neither (no `UIBusy` reference in the file); it rolls its own `btn.disabled` and its own `st0.busy` re-entrancy flag. `index.html` does not load the file at all. Three different answers to one problem. |
| S-F15 | CORRECTNESS | low | Home queue rendering | `index.html:804`, `:895` | `esc(...).substring(0,40)` and `esc(...).substring(0,30)` truncate the **escaped** string, so a name containing `&`, `<` or `>` near the boundary can be cut mid-entity (`&am`) and injected into `innerHTML`. Escape after truncating. |
| S-F16 | ERRORS | low | Claude sessions table | `index.html:651` | `'Error loading sessions: ' + e.message` is assigned via `innerHTML` without `esc()`, unlike every other value in the same function. Server-controlled text reaches the DOM as markup. |
| S-F17 | A11Y | low | Queue modal, conversion tabs | `index.html:294`, `:778`; `conversion/index.html:232-235`, `:670` | The queue modal has Escape but no focus trap and no `role="dialog"`/`aria-modal`. The conversion Jobs/Live/Report/Prompt tabs are `<div onclick>` with no `role="tab"`, no `tabindex` and no key handler, so the workbench's primary navigation is mouse-only; job cards (`:670`) likewise. |
| S-F18 | A11Y | low | Access Details sorting | `shared/access-details.js:120-122` | `<th onclick=…>` with no `tabindex`, no `role="button"` and no `aria-sort`: sorting is unreachable by keyboard and the current sort order is not announced. |
| S-F19 | ERRORS | low | Claude stats, progress report | `index.html:398-421`, `:521-596` | Empty corpus renders a four-column grid of zeros and `0%` bars rather than an empty state. The rest of the home page handles `[]` properly (`:475,493,513,609,811`), so this is an inconsistency rather than a gap. Cosmetic; listed for completeness. |

---

# Part 3 — Analyzer pages: inventory and findings

Areas J, P and Q are large enough that keeping each page's inventory next to its own findings is more
useful than splitting them. The structure is otherwise identical to Areas X and S above.

## Area Q — PL/SQL page (`plsql/index.html`, 29 JS files)

### Q inventory — live features

| # | Feature | Entry point | Implemented by | API endpoints | What the user does |
|---|---|---|---|---|---|
| Q-1 | Analyze form | `plsql/index.html:98` `PA.startAnalysis()` | `js/app.js:1193-1330` | `POST /api/plsql/analysis/analyze`, `/analyze-fast` (`AnalyzerController.java:62,67`); `GET /api/queue/events` (`QueueController.java:86`) | Submits a schema + object for a PL/SQL crawl, with SSE and poll progress. |
| Q-2 | Right tabs: History / Perf / Queue | `index.html:135,138,141` | `js/app.js:391,1057,3772` | `GET /api/parser/analyses` (`ParserAnalysisController.java:62`), `GET /api/queue` | Browses past analyses, queued jobs and perf stats. |
| Q-3 | Header menu overlay (queue/history/perf) | `index.html:31,34,37` | `js/app.js:3772,3897` | as Q-2 | Reaches the same lists from the header. |
| Q-4 | Load analysis | history cards → `PA.loadAnalysis` (`js/app.js:391,398,540`) | `js/app.js:540-588`, `js/api.js:64-94` | `GET /api/parser/analyses/{n}/index` (`:232`), fallback `GET /api/plsql/analysis/history/{n}` (`AnalyzerController.java:82`) | Opens an analysis into the analysis screen. |
| Q-5 | Left panel: call-flow tree | `index.html:198` | `js/app.js:1475-1620` | `/node/{id}`, `/call-tree/{id}`, `/procedures` (`:241,473,299`) | Navigates the call flow from the entry point. |
| Q-6 | Left panel: tables list | `index.html:199` | `js/app.js:1536,1740+` | `/api/parser/analyses/{n}/tables` (`:252`) or `/api/plsql/analysis/tables` (`:122`) | Browses accessed tables. |
| Q-7 | Left filter box | `leftFilter` → `js/app.js:1426-1473` | `js/app.js` | none | Filters the sidebar tree client-side. |
| Q-8 | Explore (call trace) | `index.html:237` | `js/callGraph.js:76-330` | `/call-tree/{id}` (`:112`) | Drills down an interactive call tree with breadcrumbs. |
| Q-9 | Trace (flat) | `index.html:238` | `js/app.js:3213-3316` | reuses loaded callTree | Reads a flat sequential call list with depth bars. |
| Q-10 | References | `index.html:239` → `js/app.js:1806` | `js/app.js:3419-3470` | `/call-tree/{id}/callers` (`:117`) | Looks up calls-made and called-by. |
| Q-11 | Table Operations | `index.html:240`; loaded `js/app.js:555` | `js/tableOps.js`, `js/tableFramework.js` | `/api/plsql/analysis/tables` or parser `/tables` | Pages a table of DB tables with op pills, scope, triggers. |
| Q-12 | Joins | `index.html:241` → `js/app.js:1831` | `js/joins.js` | `/api/plsql/analysis/joins` (`:147`) / parser `/joins` (`:324`) | Inspects JOIN relationships per table. |
| Q-13 | Cursors | `index.html:242` → `js/app.js:1836` | `js/cursors.js` | `/api/plsql/analysis/cursors` (`:152`) / parser `/cursors` (`:349`) | Inspects cursor declarations and usage. |
| Q-14 | Predicates | `index.html:243` → `js/app.js:1841` | `js/predicates.js:14` | none — derived client-side from `/tables` | Inspects WHERE-clause predicates per table. |
| Q-15 | Source view | `index.html:244` → `js/app.js:1818` | `js/sourceView.js` | `/api/plsql/analysis/source/{owner}/{obj}` (`:167`) | Reads focused PL/SQL source. |
| Q-16 | Code modal | `index.html:513-539` | `js/app.js` `PA.codeModal` | same source endpoint | Reads fullscreen source with back/forward, find and copy. |
| Q-17 | Claude AI verification | `index.html:245,451-487`, `:184` | `js/claude.js` (1,439 lines) | 9 endpoints on `/api/plsql/claude/*` (`ClaudeController.java:68-312`) | Runs and monitors Claude verification of table ops; browses chunks and sessions. |
| Q-18 | Summary dashboard | `index.html:246` → `js/app.js:1795` | `js/summary.js` + 13 `summary-*.js` | `/call-graph`, then `/detail/{id}` per node | Reads the aggregated report: Procedures, Static tabs, Claude tabs. |
| Q-19 | Summary → Trace / Explore drill-in | `js/summary-tables.js:130,131`; `js/summary-helpers.js:524,525`; `js/summary-claude.js:109,110` | `summary-trace.js`, `summary-explorer.js` | cached reports | Jumps from a summary row to that procedure's trace or call tree. |
| Q-20 | Summary export modal | `js/summary-claude.js:111` `showExportModal` | `summary-export-modal.js`, `-excel.js`, `-style.js` | `GET /api/plsql/config/export-config` (`ConfigController.java:49`) | Exports styled multi-sheet .xlsx or JSON. |
| Q-21 | Top-bar Export | `index.html:187` | `js/export.js` | `/procedures`, `/tables` | Exports call tree, table ops, procedures and Claude data. |
| Q-22 | Logs viewer | `index.html:188` | `js/logViewer.js` | `GET /api/plsql/logs` (`LogController.java:58`) | Tails the server log. |
| Q-23 | Parse errors | `index.html:395`; `js/app.js:467,681` | `js/app.js:467` | `/api/parser/analyses/{n}/errors` (`:122`) | Reviews parse failures for the analysis. |
| Q-24 | Table Info modal | `index.html:622-629` | `js/app.js` `PA.tableInfo` | `/tables/{t}/metadata` (`:132`), `/api/plsql/db/query` (`DatabaseController.java:643`) | Reads columns, constraints, indexes and SQL for a table. |
| Q-25 | Trigger modal | `index.html:648` | `js/triggerModal.js` | `/tables/{t}/triggers` (`:137`) | Reads trigger details for a table. |
| Q-26 | Connections config | `index.html:56,698` | `js/config.js` | `/api/plsql/config/projects…`, `/test-connection`, `/resolve` (`ConfigController.java:98-318`) | Manages projects, environments and DB connections. |
| Q-27 | Version banner + revert | `js/app.js:548,553` | `js/app.js` `PA.version`, `js/api.js` | `/api/plsql/analysis/versions` (`:97`), `/api/plsql/claude/versions/revert` (`ClaudeController.java:312`) | Switches between static and Claude analysis versions. |

### Q findings

| ID | Kind | Sev | Feature | Where | Finding and impact |
|---|---|---|---|---|---|
| Q-F1 | SCALE | high | Summary dashboard | `js/summary.js:68-103`, retention at `:65` | `_render` walks every call-graph node and issues one `PA.api.getProcDetail(procId)` per node **serially** (`await` inside a `for` inside `processBatch`), 15 per animation frame. At the 8,000-procedure target that is 8,000 sequential round-trips before anything renders — at ~20 ms each, roughly 3 minutes of blocking fetches (estimated) — and every `procReports` entry is retained in memory simultaneously. Measured context: `analysis.json` is 47 MB and per-endpoint `nodes.json` reaches 18 MB. |
| Q-F2 | CORRECTNESS | high | Summary dashboard | `js/app.js:1795-1797`; `js/summary.js:145-153` | `switchRightTab('summary')` calls `PA.summary.render(...)` unconditionally on **every** click, and `render`/`_render` have no in-flight or generation guard (`dataset.rendered` at `js/summary.js:243,272` only guards sub-tabs). Two clicks start two concurrent 8,000-request loops writing the same container and the same `this._procReports`; the loser's rows land in the winner's report. `js/callGraph.js:77,91,103` already uses `UIBusy.seq` correctly. |
| Q-F3 | SCALE | high | Trace tab | `js/app.js:3233-3290` | `PA.trace.render` builds one HTML string for **all** `flatNodes` — no cap, no paging, no virtualisation — then a single `innerHTML`. `js/callGraph.js:30-33` documents a measured real trace of 1,250 nodes at depth 71 and records that this exact pattern was the "still loading, nothing comes" bug fixed in Explore by lazy depth (`_initialDepth: 1`, `js/callGraph.js:35`). Trace never received the fix. |
| Q-F4 | SCALE | high | Trace search | `js/app.js:3298-3312` | `search()` re-queries `#traceContainer .trace-row` and per row runs two further `querySelector` calls plus a style write, on **every keystroke** — O(rows × 3 DOM queries) per character over the unbounded row set from Q-F3. |
| Q-F5 | SCALE / CONSISTENCY | high | Table Ops, Joins, Cursors, Predicates | `js/tableFramework.js:33-153` vs `parser/js/tableFramework.js:27-157` | The two forks differ by **429 lines**. The parser fork has a whole server-paging layer the plsql fork lacks — `PA.pageSource`, `isServerPaged`, `_fetch`, `pageCache`, `serverTotal`, `serverSorts`, cursor paging. The plsql fork materialises the whole array (`:37-38`), filters and sorts it client-side on every interaction (`:89-153`, `[...list].sort` at `:139`) and only slices for display (`:202`). All four callers pass the full dataset (`tableOps.js:39`, `joins.js:145`, `cursors.js:146`, `predicates.js:187`). At 1,000,000 DB operations the entire array must sit in the browser and is re-sorted on every column click. |
| Q-F6 | SCALE | high | Table Operations page size | `js/tableFramework.js:439` | The page-size dropdown offers `All` (`value = total`), rendering every filtered row into one `innerHTML`. Combined with Q-F5 this is an unbounded render of the full table-operations set, one click away. |
| Q-F7 | SCALE | med | Predicates tab | `js/predicates.js:14-15` | `load()` fetches the entire table-operations document via `PA.api.getTableOperations()` and derives predicates client-side; there is no `/api/plsql/analysis/predicates` endpoint (checked `AnalyzerController.java:62-197`). Every predicate row is computed in the browser over the full accessDetails set. |
| Q-F8 | SCALE | med | Excel export | `js/summary-export-excel.js:32-48`; `js/export.js:109-139` | Both build the workbook fully in-browser from `_procReports` / `_procedures` + `_tables` with **no row cap**. The only bound is a 32,000-char per-cell truncation (`js/export.js:11-19`, `js/summary-export-style.js:11-18`), which limits cell size, not row count. Exporting 8,000 procedures with their table ops will exhaust browser memory before `XLSX.writeFile`. |
| Q-F9 | CORRECTNESS | med | Analyze button | `index.html:98`; `js/app.js:1193,1225,1242` | `btnStartAnalysis` is never disabled (grep in `js/app.js` returns 0 hits) and `PA.startAnalysis` has no in-flight flag. A double-click submits two jobs; the second overwrites `PA.jobs.currentJobId` and opens a second `EventSource('/api/queue/events')` that the first path never closes. |
| Q-F10 | CORRECTNESS | med | Table Ops / Joins / Cursors / Predicates loads | `js/tableOps.js:12-19`, `js/joins.js:12-13`, `js/cursors.js:12-13`, `js/predicates.js:14-15` | None use `UIBusy.seq`, unlike `js/callGraph.js:77`. `PA.tableOps.load()` fires from `PA.loadAnalysis` (`js/app.js:555`), so loading analysis A then B before A returns leaves A's rows in `PA.tableOps.data` under B's header. `UIBusy` is available (`shared/ui-busy.js:119`) and used in only 5 of the 29 plsql files. |
| Q-F11 | CORRECTNESS | med | Analysis load | `js/app.js:552-561`, toast at `:565` | `Promise.allSettled` swallows all four failures silently, so the "Loaded: … (N procs)" success toast fires even when `loadSidebarData` and `tableOps.load` both rejected. The user sees a success message over an empty UI. |
| Q-F12 | ERRORS | med | API layer | `js/api.js:14-19,22-25,265-270,312-317` | Measured: 73 `fetch(` calls in `api.js`; across all plsql JS there are 61 response-status checks against 85 `await fetch(`. `analyze`, `getLatestResult`, `testDbConnection` and `startClaudeVerification` call `res.json()` directly, so a 500 returning an HTML error page throws a raw `SyntaxError` that surfaces to the user as `Failed: Unexpected token '<'` via `js/app.js:584`. |
| Q-F13 | CONSISTENCY | med | Summary modules | `plsql/js/summary-*.js` vs `jar/js/summary-*.js` | 14 of 15 share a name with a jar file and **none are identical**; the jar copies are consistently 1.5–3× larger. These are divergent forks of one design, not independent modules — the plsql side reads as a trimmed copy that has fallen behind. See the cross-cutting table for the per-file differing-line counts. |
| Q-F14 | CORRECTNESS | med | Export data cache | `js/export.js:42-43,115-116,141-142` | `PA.export._procedures` / `_tables` are module-level caches cleared only at the end of `_run`. Opening Export for analysis A, closing without exporting, then loading B and reopening Export shows **A's** counts (`js/export.js:45-46`). `PA.loadAnalysis` (`js/app.js:540`) does not reset them, unlike `PA.joins._loaded` etc. at `js/app.js:559-561`. |
| Q-F15 | A11Y | med | whole page | `plsql/index.html` (767 lines) | Measured: **0** `aria-`, **0** `role=`, **0** `tabindex`, against 85 `onclick` attributes. Every tab strip (`:237-246`, `:198-199`, `:626-629`) is plain buttons with no `role="tab"`/`aria-selected`, and all seven modals (`:513,600,622,648,670,683,698`) have no `role="dialog"`, no focus trap and no focus restore. |
| Q-F16 | A11Y | med | clickable non-buttons | `index.html:254,338,361,392,423,487`; `js/app.js:3254` | `Clear` and breadcrumb `Root` are `<span onclick>` with no `tabindex` and no key handler, as are all generated row handlers. Only 3 `keydown` handlers exist across all `plsql/js/*.js` plus `index.html`, so the table and trace grids are entirely mouse-only. |
| Q-F17 | ERRORS | low | Summary per-procedure failures | `js/summary.js:80-82`, progress text `:91` | A failed `getProcDetail` silently pushes `_minimalReport(node)`. With 8,000 procedures a partial backend outage yields a dashboard that looks complete but is mostly stubs; the progress text reports `(N with data)` without ever flagging the shortfall as an error. |
| Q-F18 | ERRORS | low | Trace expand/collapse | `index.html:273,274` → `js/app.js:3314,3315` | "Expand All" and "Collapse All" are wired to explicit `/* flat view — no-op */` functions. The buttons render and are clickable but do nothing and give no feedback. |
| Q-F19 | SCALE | low | Sidebar flow tree | `js/app.js:1594`; cap at `:1546-1560` | The "show all" path sets `PA._flowNodeBudget = Number.MAX_SAFE_INTEGER` and repaints the whole tree. The cap mechanism exists but its escape hatch is unbounded. |
| Q-F20 | SCALE | low | Home queue/session lists | `js/app.js:445,3406,3772` | Hard-coded `slice(0,50)`, `slice(0,200)`, `slice(0,30)` truncate with no "showing N of M" indicator and no paging, so a long queue silently appears short. |

## Area P — Parser page (`parser/index.html`, 57 JS files)

All 57 files in `parser/js/` are loaded — verified by comparing the 57 `src="js/…"` tags at
`parser/index.html:762-836` against `ls parser/js/`. Being loaded is not being reachable, and seven
loaded modules turned out to have no live entry point; they are in the dead list.

### P inventory — live features

| # | Feature | Entry point | Implemented by | API endpoints | What the user does |
|---|---|---|---|---|---|
| P-1 | Analysis history list | `parser/index.html:147,153` | `js/history.js` | `GET /api/parser/analyses`, `/analyses/facets` | Browses a server-paged, facet-filtered list of past analyses. |
| P-2 | Performance summary | `parser/index.html:148` | `js/perfSummary.js` | `/analyses/{n}/errors`, `/api/parser/claude/stats` | Reads a per-analysis timing and error rollup. |
| P-3 | Queue analysis | `parser/index.html:149` | `js/home.js` | `/api/queue`, `/api/queue/events` | Watches the live queue of running analyses. |
| P-4 | Start analysis | `parser/index.html:134` | `js/home.js`, `js/app.js:18` | `POST /api/parser/analyze` | Parses an Oracle object into an analysis. |
| P-5 | Connection manager | `parser/index.html:74` | `js/config.js`, `js/conn-form*.js`, `js/db-selector.js`, `js/conn-test.js` | `/api/plsql/config`, `/config/projects`, `POST /api/plsql/db/test` | Creates and edits DB connections per project and environment. |
| P-6 | Cache manager | `parser/index.html:76` | `js/cacheManager.js` | `/api/parser/cache/by-connection`, `/cache/clear`, `/cache/clear-connection` | Clears cached analyses per connection. |
| P-7 | Prompt manager | `parser/index.html:77,194` | `js/promptManager.js` | `/api/prompt-config` | Edits Claude prompt templates. |
| P-8 | Log viewer | `parser/index.html:75,211` | `js/logViewer.js` | `/api/plsql/logs` | Tails application logs. |
| P-9 | Left panel: procedures / tables | `parser/index.html:219-223` | `js/leftPanel.js` | `/analyses/{n}/tables` | Navigates batched, schema-grouped lists with a filter. |
| P-10 | Explore ▸ Hierarchy | `parser/index.html:266` | `js/callGraph.js` (`PA.callTrace`) | `/analyses/{n}/call-tree/{nodeId}` | Expands a call tree from the selected procedure. |
| P-11 | Explore ▸ Trace | `parser/index.html:267` | `js/callGraph.js` (`PA.trace`) | same call-tree data | Reads a flat call trace with type filter and unique-collapse. |
| P-12 | Explore ▸ References | `parser/index.html:268` | `js/refs.js` | index nodes, client-derived | Sees the calls made by the selected procedure. |
| P-13 | Explore ▸ Transactions | `parser/index.html:269,332-334` | `js/txnTrace.js` | index `transactionStatements` | Inspects commit/rollback/savepoint segments as table or graph. |
| P-14 | Table Ops | `parser/index.html:253` | `js/tableOps.js`, `js/triggers.js` | `/analyses/{n}/tables` | Reviews SELECT/INSERT/UPDATE/DELETE per table. |
| P-15 | Table detail drill-down | `js/leftPanel.js` table item onclick; `js/tableOps.js` | `js/tableDetail.js`, `js/triggerDetail.js`, `shared/access-details.js` | `/analyses/{n}/tables`, `/api/plsql/db/table-info/{t}` | Inspects per-table accesses, columns and triggers. |
| P-16 | Ad-hoc query runner | `parser/index.html:619` | `js/queryRunner.js` | `POST /api/plsql/db/query` | Runs a SELECT against the live DB for a table. |
| P-17 | Details ▸ Sequences | `parser/index.html:359` | `js/sequences.js` | `/analyses/{n}/sequences` | Reviews NEXTVAL/CURRVAL usage. |
| P-18 | Details ▸ Join Summary | `parser/index.html:360` | `js/joinSummary.js` (+ helpers in `js/joins.js`) | `/analyses/{n}/joins`, `/api/parser/config/join-complexity` | Ranks join queries by complexity. |
| P-19 | Details ▸ Dynamic SQL | `parser/index.html:361` | `js/dynamicSql.js` | `/analyses/{n}/dynamic-sql` (server-paged) | Finds EXECUTE IMMEDIATE and ref-cursor sites. |
| P-20 | Details ▸ Transactions | `parser/index.html:362` | `js/transactions.js` | index data | Reads the transaction statement table. |
| P-21 | Summary ▸ Dashboard | `parser/index.html:257,476` | `js/summary.js` | index only | Reads aggregate stats for the current scope. |
| P-22 | Summary ▸ Claude Insights | `parser/index.html:477` | `js/claudeInsights.js` | `/analyses/{n}/claude/result`, `/claude/verification-summary` | Reads Claude findings for the analysis. |
| P-23 | Summary ▸ Claude Corrections | `parser/index.html:478` | `js/claudeCorrections.js` | `/analyses/{n}/claude/review`, `/claude/apply` | Accepts or rejects Claude corrections. |
| P-24 | Complexity | `parser/index.html:258` | `js/complexity.js` | `/api/parser/config/complexity` | Reads LOC/tables/calls risk ratings and tunes the weights. |
| P-25 | Call graph (canvas) | `parser/index.html:259` | `js/callGraphViz.js`, `js/callGraphFullscreen.js`, `js/callGraphModal.js` | `/analyses/{n}/call-graph` | Explores a force-directed dependency diagram. |
| P-26 | Source view + source modal | `parser/index.html:260,657-733` | `js/sourceView.js` | `/analyses/{n}/source/{file}`, `/analyses/{n}/node/{file}` | Reads highlighted PL/SQL with a symbol sidebar. |
| P-27 | Analysis search | `parser/index.html:183` | `js/analysisSearch.js` | `GET /analyses/{n}/search?q=&limit=50` | Runs a qualified statement search (`op:`, `table:`, `col:`…). |
| P-28 | Global left-panel search | `parser/index.html:209` | `js/app.js:1036` | none | Types in the topbar box to hide non-matching left-panel rows. |
| P-29 | Claude enrichment run | `parser/index.html:194,199,201` | `js/claudeEnrichment.js`, `js/chunkViewer.js` | `/claude/verify`, `/claude/progress`, `/claude/chunks*`, `/claude/versions*` | Starts, monitors and kills Claude verification. |
| P-30 | Claude sessions | `parser/index.html:88,202` | `js/claudeSessions.js` | `/api/parser/claude/sessions`, `/claude/stats`, `/sessions/kill-all` | Reviews a session, token and cost table. |
| P-31 | Export | `parser/index.html:210` | `js/export.js`, `js/xlsx.full.min.js` | reuses tables/joins/cursors/sequences/call-graph | Downloads a JSON/CSV/XLSX workbook of the analysis. |
| P-32 | Pull / Refresh | `parser/index.html:207-208` | `js/app.js:1047,1080` | `/analyses/{n}/index` | Re-fetches the index or reloads the current tab. |
| P-33 | Scope toggle (direct/subtree/full) | rendered into `dhScopeToggle`, `parser/index.html:242` | `js/scope.js` | none | Re-filters every table to the selected call scope. |
| P-34 | Routing / deep links | `js/router.js:52` | `js/router.js` | — | Uses `#/analysis/{name}[/proc/{id}]` hash routes and shareable links. |
| P-35 | Detail header | `parser/index.html:233-248` | `js/detailHeader.js` | — | Sees the selected-object banner, stats and nav. |
| P-36 | Tooltips / error modal | `js/tooltips.js`, `js/errorModal.js` | — | `/analyses/{n}/errors` | Gets hover help and a parse-error dialog. |

### P findings

| ID | Kind | Sev | Feature | Where | Finding and impact |
|---|---|---|---|---|---|
| P-F1 | SCALE | high | Table Ops, left-panel tables, Export | `js/api.js:97-122` | `getTableOperations()` downloads the whole `/analyses/{n}/tables` document, then `flatMap`s `usedBy[].operations{op:[lines]}` into one flat `accessDetails` object **per access line**, plus a nested `reduce` over the same structure for `accessCount`. At the 1,000,000-DB-operation target this materialises roughly 1M JS objects of 5 fields each on the main thread, per call. Measured: no cap, no paging, no `limit` parameter anywhere in the function. |
| P-F2 | SCALE | high | Left panel tables list | `js/leftPanel.js:229` | `_renderLeftTables()` awaits the full `getTableOperations()` (P-F1) purely to render a name, an op-badge row and `accessCount` per table — every `accessDetails` object it builds is then discarded. This runs on **every** analysis load (`js/app.js:39`) and again on every `PA.pullData()` (`js/app.js:1060`). A cheap summary fetch already exists and is used for the topbar count (`js/api.js:127`), so the precedent is there and unused. |
| P-F3 | SCALE | high | Call graph | `js/callGraphViz.js:290,297-306` | The force simulation is an all-pairs O(n²) repulsion loop run for `maxIter = 200` frames over `PA.analysisData.nodes` with **no node cap** (grep for `MAX`/`limit`/`slice(0` returns only `:171`, an unrelated path slice). At 8,000 procedures that is ~32M distance computations per frame × 200 frames ≈ 6.4B operations and the tab will hang the browser. Estimated from the loop bounds; the absence of a cap is measured. |
| P-F4 | SCALE | high | Table Ops, Sequences, Complexity, References, Join Summary | `js/tableOps.js:31`, `js/sequences.js:97`, `js/complexity.js:263`, `js/refs.js:188`, `js/joinSummary.js:362` | All five call `PA.tf.init(id, <materialised array>, 50, …)`. `PA.tf` supports a server page source (`js/tableFramework.js:42-51,219`) and exactly **one** parser call site uses it (`js/cursors.js:142`). The framework's own answer to this problem is in the same file and applied to 1 of 6 tables. Sorting and search in the other five run over the entire in-memory array with no virtualisation. |
| P-F5 | SCALE | high | Export (XLSX) | `js/export.js:1006`, `:668-686`, `:145-160` | Inside `arr.forEach(function(t,i){…})` the code runs `opCols.forEach(op => cts[op] = dets.filter(a => a.operation===op).length)` — a full scan of that table's `accessDetails` once per operation column. Combined with the nested `t.accessDetails.forEach` inside `tablesData.forEach` and the gather of joins + tables + cursors + sequences + call graph in full, an export at the 1M-operation target is unbounded in both memory and time. No row cap exists in the file. |
| P-F6 | SCALE | med | References | `js/refs.js:41-64`; guard at `:27` | `_buildAllProcs()` rebuilds a 13-field object for every node in `PA.analysisData.nodes` into a `Map`, then `Array.from(map.values())` and `_assignCallTypes` over the result, **each time a new procedure is selected**. At 8,000 procedures that is 8,000 allocations per selection click; the guard only skips a repeat of the same procId. |
| P-F7 | SCALE | med | Summary dashboard | `js/summary.js:8,18-25` | `_computeStats` walks all scoped nodes and rebuilds the entire dashboard HTML string on every scope change (`applyScope` → `_render`). Linear, but re-run wholesale with `innerHTML` replacement, so at 8,000 nodes each scope toggle re-parses the whole panel. |
| P-F8 | CORRECTNESS | high | Global search box | `js/app.js:1036-1045`; `js/leftPanel.js:34,126` | `PA.globalSearch` walks `document.querySelectorAll('.left-panel-list .lp-item')` and toggles `display` — but `leftPanel.js` only renders `PA._LP_BATCH = 50` items per schema group. The topbar search therefore searches **only the currently rendered batch** and silently reports no match for anything beyond it. This is the exact bug the leftPanel docstring says was already fixed for the *other* search box (`js/leftPanel.js:8-13`). |
| P-F9 | CORRECTNESS | high | Left panel, two search boxes | `js/app.js:1042` vs `js/leftPanel.js:62-71`, count at `:110-112` | Two search boxes act on the same list with incompatible mechanisms: `PA.filterLeft` narrows `st.shown` and re-renders, while `PA.globalSearch` sets inline `display:none` on DOM nodes. Typing in one wipes the other's effect — a re-render drops the inline styles, and a DOM hide is invisible to `st.shown`. The "showing N of M" count is only ever correct for `filterLeft`. |
| P-F10 | CORRECTNESS | high | Right-tab switching | `js/app.js:783-796,815-851` | `switchRightTab` synchronously tears down the previous tab (`cel.innerHTML=''`, `delete PA.tf._state[tfKey]`, `_rightTabLoaded[prev]=false`) while that tab's `load()` may still be in flight; none of the eight `load()` calls is awaited, sequenced with a token, or `.catch()`ed. Rapid tab switching lets a stale response render into a torn-down container **and** re-fires the load on return because the flag was reset. `js/sourceView.js:53-64` already implements the `_reqSeq`/`_isCurrent` pattern this needs. |
| P-F11 | ERRORS | high | every table view | `shared/page-envelope.js:200,229`; `js/api.js:87,99,150,158,166` | `page-envelope.js` defines `FAILED` as a frozen empty array and `apiFail` returns it; `api.js` propagates it. Only **four** sites in all of `parser/js/` ever call `PA.failed()` to distinguish it — `js/sourceView.js:407,1618`, `js/perfSummary.js:405,410`. So a failed `/tables`, `/joins`, `/cursors` or `/sequences` request renders as "No data", indistinguishable from a genuinely empty analysis. |
| P-F12 | CONSISTENCY | high | table framework fork | `parser/js/tableFramework.js` (752) vs `plsql/js/tableFramework.js` (457) | Measured: 527-line unified diff, 338 added / 43 removed. Structural, not cosmetic — the parser fork adds server-page-source support (`src`, `serverSorts`, `pageCache`, `highestPage`, `serverTotal`, `totalIsExact`, `hasMore`, `loading`, `loadError` at `parser/js/tableFramework.js:43-51`) and server-forwarded search at `:219`. The plsql copy has none of it and is permanently client-side. Every scale fix landed in the parser copy is invisible to plsql. (Same fact as Q-F5, recorded from both sides.) |
| P-F13 | CONSISTENCY | med | search boxes | 13 sites, listed below | Thirteen independent `onSearch` implementations: `js/complexity.js:389`, `js/cursors.js:163`, `js/dynamicSql.js:44`, `js/joins.js:116`, `js/joinSummary.js:1048` and `:1054`, `js/sequences.js:147`, `js/refs.js:283`, `js/tableOps.js:132`, `js/txnTrace.js:26`, `js/transactions.js:19`, plus `js/leftPanel.js:62` and `js/app.js:1036`. They split three ways on semantics — server-side, client-array (`PA.tf.setSearch`), and DOM-walk. **Only** `js/analysisSearch.js:59-61` debounces and only it carries a stale-response guard (`_seq`, `:30`). |
| P-F14 | CORRECTNESS | med | Complexity, Sequences, Table Ops | `js/complexity.js:94`, `js/sequences.js:21`, `js/tableOps.js:7`; callers `js/app.js:816,850,743` | None of the three `load()` bodies has a `try`/`catch`, and all are invoked un-awaited. Any throw becomes an unhandled rejection with the container still showing the previous tab's markup. |
| P-F15 | ERRORS | med | Analysis search | `js/analysisSearch.js:78-81`, `:90` | `res.json()` is called before `res.ok` is checked, so a 500 returning an HTML error page throws inside the `try` and reaches the user as `Search failed: SyntaxError: Unexpected token '<'`. The 400 path is handled, but only after the parse has already had to succeed. |
| P-F16 | A11Y | med | whole page | `parser/index.html` (866 lines) | Measured: **0** occurrences of `role=`, `aria-*` or `tabindex`, against 116 `onclick=` attributes. The tab strips at `:252-260`, `:266-269`, `:359-362`, `:476-478` are `<button>` groups with no `role="tablist"`/`aria-selected`, so a screen reader gets no notion of which view is active — the `.active` class is the only state signal. |
| P-F17 | A11Y | med | table and list rows | 33 sites; e.g. `js/leftPanel.js:118,143` | 33 `<div … onclick=` constructs across `parser/js/*.js` are the page's **primary navigation** controls (`lp-group-head`, `lp-show-more`, proc and table items) and are unreachable by keyboard: no `tabindex`, no `role="button"`, no `keydown` handler. |
| P-F18 | A11Y | low | modals | `parser/index.html:657,670,738` | Overlays close on a backdrop `onclick` but have no focus trap, no `aria-modal` and no `role="dialog"`. Only the source modal advertises an Esc binding, and it does so in title text rather than as a real binding. |

## Area J — JAR page and WAR page (`jar/index.html` + 40 JS, `war/index.html` + 2 JS)

WAR is largely a reskin of JAR: `war/index.html:163-197` loads 33 files from `/jar/js/`, and overrides only
`war/js/api.js` and `war/js/app-overrides.js`. That reskin is the source of two high-severity findings below.

### J inventory — live features

| # | Feature | Page(s) | Entry point | Implemented by | API endpoints | What the user does |
|---|---|---|---|---|---|---|
| J-1 | Artifact list sidebar (group, select, delete) | both | `jar/index.html:45`, `war/index.html:36`; handlers `jar/js/sidebar.js:22-41` | `sidebar.js`, `app.js:112` | `GET`/`DELETE /api/jar/jars`, `/api/war/wars` | Picks or deletes a previously analysed artifact. |
| J-2 | Upload & analyze | both | `jar/index.html:28`, `war/index.html:27` | `app.js:362-524` | `POST /api/jar/jars`, `GET /api/queue/{id}`, SSE `/api/jar/progress`, `/api/war/progress` (`ProgressController.java:21,26`) | Uploads a JAR/WAR and watches queued analysis progress. |
| J-3 | Targeted analysis (dev) | jar only | `jar/index.html:36` | `targeted.js:183,226` | `POST /api/jar/jars/analyze-targeted` (`AnalyzerController.java:414`), `GET /api/queue/{id}` | Analyses a server-local JAR from chosen entry points. |
| J-4 | Home dashboard | both | `#dashboard-content` `jar/index.html:82`; render `app.js:112` | `dashboard.js:57-61,74-125` | `/api/jar/jars/{name}/claude-stats` | Sees landing cards per artifact plus Claude usage stats. |
| J-5 | Code Structure tab (package/project/visual views, code panel, jump history) | both | `jar/index.html:104,115-139`; `war/index.html:92,99-123` | `codeTree.js`, `codeTreeViews.js`, `codeTreePanel.js`, `nav.js` | `/classes/tree`, `/classes/by-index/{i}`, `/bundled-jars`, `/bundled-jars/content` (jar only) | Browses classes and methods and views decompiled source. |
| J-6 | Class/method search + expand/collapse all | both | `jar/index.html:128,130,131` → `app.js:1607-1609` | `codeTree.js:467-537`, `codeTreeViews.js:359-432` | none | Filters the class tree in-browser. |
| J-7 | Endpoint Flows tab | both | `jar/index.html:105,151-165` | `endpointList.js`, `callGraph.js:66,74` | `/endpoints/by-index/{i}/call-tree` | Picks an endpoint and walks its call tree. |
| J-8 | Implementation picker | both | `nav.js:57` via inline `JA.nav.click` | `nav.js:84-143` | none | Clicks an interface call and chooses which implementation to jump to. |
| J-9 | Summary tab, 11 live sub-tabs | both | `jar/index.html:106`; defs `summary.js:347-405`; dispatch `:626-664`; `_lazyTabs` `:470-501` | `summary.js` + 20 `summary-*.js` | `/summary`, `/summary/header(s)`, `/summary/external-calls`, `/summary/dynamic-flows`, `/summary/aggregation-flows`, `/summary/beans`, `/collections/call-paths` (jar only) | Works through the migration-analysis workbench. |
| J-10 | Column filters / sort / client paging in summary tables | both | `summary-tables.js:652,665,1075,1160,1239` | `summary-col-filter.js`, `summary-helpers.js:928,954` | none | Filters and sorts each summary table per column. |
| J-11 | Collection-op call-path drilldown | jar only | `summary-popups.js:483` | `summary-popups.js:350-410`, `summary-explorer.js:326` | `/collections/call-paths` | Traces which endpoints hit a collection with a given op. |
| J-12 | Code viewer modal from summary | both | `summary-codeview.js:842,877,909,920,1189` | `summary-codeview.js` | `/classes/by-index/{i}` | Opens source for a class referenced in a summary row. |
| J-13 | Excel / backend export | both | `summary.js:336` | `summary-export-modal.js`, `-excel.js`, `-export.js`, `-style.js` | export POST at `summary-export-excel.js:72` | Exports selected summary sheets to xlsx. |
| J-14 | Claude enrichment / correction + progress | both | Claude Insights and Claude Corrections sub-tabs (`summary.js:396,401`) | `summary-claude.js`, `summary-corrections.js`, `summary-corr-logbrowser.js` | `/claude-enrich-single`, `/claude-rescan`, `/claude-correct`, `/claude-correct-single`, `/claude-full-scan`, `/claude-progress`, `/corrections`, `/correction-logs`, `/versions`, `/revert-claude` | Runs and reviews AI enrichment and corrections. |
| J-15 | Sessions overlay | both | `jar/index.html:52`, `war/index.html:43` | `sessions.js` | `/api/jar/jars/sessions`, `/api/sessions/running` (`GlobalSessionsController.java:67`), `/sessions/{id}/kill` | Lists and kills running Claude sessions. |
| J-16 | Log viewer | both | `jar/index.html:56`, `war/index.html:47` | `logviewer.js:65-113` | `/run-logs`, `/run-logs/{name}` | Tails application logs with a cursor. |
| J-17 | Version switching | both | `app.js:1345-1356` | `app.js` | `/versions`, `/summary?version=` | Views an earlier analysis version. |
| J-18 | Connection & domain-config badges | jar only | `jar/index.html:90,91` | `app.js:788,897` | `/connections`, `/domain-config` | Sees the Mongo connection and domain config in the header. |

### J findings

| ID | Kind | Sev | Feature | Where | Finding and impact |
|---|---|---|---|---|---|
| J-F1 | SCALE | high | Summary tab | `jar/js/summary.js:217-247` | The entire Summary is computed in the browser: `_analyzeEndpoint` runs over every entry of `analysis.endpoints` in 30-item `requestAnimationFrame` batches. The whole analysis document — **47 MB measured on disk today** — must be in JS memory before the first table appears. At the 8,000-procedure / 1M-operation target this is a multi-hundred-MB heap and a tab that never finishes. |
| J-F2 | CORRECTNESS | high | Code Structure on WAR | `jar/js/codeTree.js:86,87,1005`; `war/js/api.js:7` | `codeTree.js` calls `JA.api.classTreePages(...)` / `JA.api.bundledJarPages(...)`, but `war/js/api.js` replaces `JA.api` **wholesale** and defines neither. Confirmed missing on WAR: `classTreePages`, `bundledJarPages`, `getBundledJarContent`, `getCollectionCallPaths`, `getConnections`, `getDomainConfig`, `getSummaryHeader`. The guard at `codeTree.js:87` checks the *product* (`source.fetchPage`), not the factory, so with `PA.pageSource` present the call throws `TypeError` before the fallback — **the Code Structure tab dies on WAR**. |
| J-F3 | CONSISTENCY | high | jar vs war API surface | `war/js/api.js:7`; `WarAnalyzerController.java` | WAR implements 36 of the `JA.api` methods jar code calls and omits the 7 in J-F2. The server side confirms the gap is real (0 hits for `call-paths`, 0 for `bundled-jars` in `WarAnalyzerController.java`). WAR therefore silently loses collection call-path drilldown (`summary-popups.js:357`), bundled-JAR browsing, and the connection/domain-config badges — **with no UI indication that anything is missing**. |
| J-F4 | CONSISTENCY | high | domain config on WAR | `jar/js/summary.js:64,66` | `fetch('domain-config.json')` is a **relative** URL. `jar/domain-config.json` exists; `war/domain-config.json` does not. On WAR it 404s, the catch sets `this._config = {}`, and every domain/module/view classification in the Summary silently degrades to defaults with only a `console.warn`. |
| J-F5 | SCALE | high | Claude enrich / correct completion | `jar/js/summary-claude.js:559`; `jar/js/summary-corrections.js:408` | On poll completion both call `JA.api.getSummary(jarId)` with no header variant — refetching the whole 47 MB analysis document just to re-render tabs, then re-running J-F1 end to end. |
| J-F6 | SCALE | high | Excel export | `jar/js/summary-export-excel.js:57-62`, `:21-46`; modal `summary-export-modal.js:247` | `JSON.parse(JSON.stringify(...))` deep-clones `_epReports`, `_vertReport`, `_distReport`, `_batchReport`, `_viewsReport` with **no row cap**, synchronously on the main thread, then POSTs them. The frontend path additionally builds the whole workbook client-side. Fine at 500 rows; freezes and OOMs at 500,000. |
| J-F7 | SCALE | high | Code Structure tree | `jar/js/codeTree.js:97,300,340` | The full class tree is concatenated into one string and assigned to `container.innerHTML`, with no virtualisation. `_fetchAllClassPages` caps at 500 pages, so up to 100,000 classes can be pushed into a single DOM write. |
| J-F8 | SCALE | high | Code Structure tree render | `jar/js/codeTree.js:805`; `codeTreeViews.js:128,236` | `this._classes.indexOf(cls)` inside the per-class render loop makes a whole-tree render **O(n²)**. At 8,000+ classes this is the dominant render cost; at a 100M-line codebase it is fatal. |
| J-F9 | SCALE | high | Summary tables | `summary-tables.js:40,617,640,1067,1152,1231`; `summary-vertical.js:160,286`; `summary-dynamic.js:106`; `summary-aggregation.js:112`; `summary-helpers.js:928,954` | `_initPage(id, fullArray, 25, …)` paginates **display only**; `_applyFilter` and `_applySortInternal` run over the complete client-side array on every keystroke and every header click. There is no server-side pagination or sort anywhere in the summary. |
| J-F10 | SCALE | high | Summary progress | `jar/js/summary.js:237,249-262` | `_updateLiveStats(epReports)` is called once per 30-endpoint batch and re-iterates *every* report accumulated so far plus every collection in each — O(n²/30). Tolerable today; at 8,000 endpoints it dominates load time. |
| J-F11 | CORRECTNESS | high | Flow analysis fetches | `jar/js/flow-analysis.js:188,377,443,500,581,628,654,979` | 8 `fetch` calls, **0** `res.ok` checks (measured by `grep -c`); every one goes straight to `.json()`, so a 500 surfaces as a JSON parse error. Currently masked because the tab definition is commented out at `jar/js/summary.js:406` — but the script still loads at `jar/index.html:218` and the server routes are live, so this must be fixed **before** the tab is ever re-enabled. Recorded as a live-code finding for that reason; the feature itself is in the dead list. |
| J-F12 | SCALE | med | Class-tree search | `jar/js/codeTree.js:478-537`, `:509-521`; input `jar/index.html:128` | `oninput` walks every `.tree-node[data-class-name]` in the DOM and does a nested parent walk per match, **with no debounce** — O(n·depth) per keystroke over the whole corpus. |
| J-F13 | SCALE | med | Project / Visual views | `jar/js/codeTreeViews.js:75-79,88` | `_countClasses` recurses the entire subtree and is called from the recursive `_renderFolder`, so counts are recomputed per folder per render. |
| J-F14 | SCALE | med | Expand / collapse all | `jar/js/codeTree.js:467-476`; buttons `jar/index.html:130-131` | `querySelectorAll` over every toggle in the tree, unbounded by corpus size, on a single button press. |
| J-F15 | SCALE | med | Code viewer jumps | `jar/js/summary-codeview.js:35-39` | On a class-index cache miss, `showClassCode()` linearly scans `Object.keys(this._classIdx)` with `endsWith`. Called from dozens of onclick sites (`summary-codeview.js:842,877`, `summary-aggregation.js:189`, `summary-dynamic.js:177`). O(n) per unresolved jump against a 100M-line target. |
| J-F16 | SCALE | med | Call graph | `jar/js/callGraph.js:12,146-152,376` | `_flattenOps` recurses the full call tree with **no depth or node-count cap**; only the render indentation is capped. |
| J-F17 | SCALE | med | Paged row render | `jar/js/summary-helpers.js:658` | `s.data.indexOf(item)` fallback runs per visible row — O(n) each — whenever `_origIdx` is missing. |
| J-F18 | CORRECTNESS | med | Resource file open | `jar/js/codeTree.js:190-212` | No in-flight or stale-response guard; two rapid clicks let the slower response overwrite the panel. `_openBundledContent` (`codeTree.js:1117`) and `endpointList.select` (`endpointList.js:139`) both use `UIBusy.seq` for exactly this — this path was missed. |
| J-F19 | CORRECTNESS | med | Class member load | `jar/js/codeTree.js:373-405`, re-check at `:397` | Only an identity re-check on arrival, no request dedupe; rapid expand/collapse of the same class issues overlapping fetches. |
| J-F20 | CORRECTNESS | med | Session id injection | `jar/js/sessions.js:98` vs `:73` | `s.id.replace(/'/g,"\\'")` is interpolated into `onclick="...('${safeId}')"` and escapes only single quotes; `"` and `<` are not defused — even though `esc()` is used on the same object 25 lines earlier. |
| J-F21 | CONSISTENCY | med | Flow-analysis poll not scoped to UI | `jar/js/flow-analysis.js:619,648,735` | `_pollTimer` is module-scoped and neither `switchTab` (`app.js:1572-1606`) nor `JA.summary._clearSubTabData` (`summary.js:37`) clears it, so polling continues after the user navigates away. Every other poller in scope self-stops (`sessions.js:143-156`, `summary-claude.js:533`, `summary-corrections.js:391`). |
| J-F22 | CONSISTENCY | med | Modal a11y baseline split | `app.js:872,875,954,957` vs `targeted.js:18-53`, `sessions.js:16-39`, `logviewer.js:21-52`, `dashboard.js:137-222` | The first group uses `role="dialog" aria-modal="true" aria-label`; the second group uses none. Two conventions in one application. |
| J-F23 | ERRORS | med | Silent load failures | `jar/js/codeTree.js:1017,1032` vs `:66` | Bundled-JAR and jar-dep-map load failures produce only `console.warn` — no toast, unlike `:66`. The user sees an empty panel indistinguishable from "nothing here". |
| J-F24 | ERRORS | med | Claude stats card | `jar/js/dashboard.js:124` | All failures collapse to `wrap.innerHTML = ''`, so a failed stats fetch looks identical to "Claude has never run". |
| J-F25 | ERRORS | low | Code panel | `jar/js/codeTreePanel.js:41` | `if (!cls) return;` leaves the previous class, or the loading spinner, on screen with no "class not found" message. |
| J-F26 | CORRECTNESS | low | Dead poll parameter | `jar/js/targeted.js:245`; `shared/poll-config.js:62` | Passes a literal `2000` as a third argument to `PollConfig.every(key, fn)`, which takes two. The interval is silently `jobStatusMs`, not 2 s. |
| J-F27 | A11Y | med | whole area | measured | `jar/index.html`: 20 `onclick=`, **0** `aria-`, **0** `role=`. `war/index.html`: 19 / 0 / 0. Across `jar/js/*.js`: 411 `onclick=` against 5 `aria-`. The three main tabs (`jar/index.html:104-106`) and all summary sub-tabs (`summary.js:413-419`) lack `role="tab"`/`aria-selected`. |
| J-F28 | A11Y | med | div-as-button throughout | `codeTree.js:358,407,447,970`; `codeTreeViews.js:94,137,251`; `endpointList.js:94`; `callGraph.js:303,383`; `summary-claude.js:57`; `summary-codeview.js:191,641,657,769`; `summary-dynamic.js:177,179`; `summary-explorer.js:326,356`; `summary-popups.js:108,574` | None have `tabindex` or a key handler, so none are keyboard-reachable. The only keyboard-usable controls found in the tree and graph modules are the real `<button>`s at `callGraph.js:513,550`. |
| J-F29 | A11Y | med | No Escape on modals | `summary-explorer.js:274`, `summary-popups.js:81`, `summary-export-modal.js:69`, `summary-corrections.js:346`, `summary-claude.js:883`, `summary-corr-logbrowser.js:54`, `nav.js:129,138`, `logviewer.js:50-52` | The `onclick="if(event.target===this)this.remove()"` backdrop pattern has no keydown handler anywhere. `JA.utils.confirm` (`utils.js:152,156`) *does* handle Escape and focus — the pattern exists in the codebase and simply was not applied. |
| J-F30 | SCALE | low | Memory retention | `jar/js/codeTree.js:12,214-215,829` | `_classes` holds the whole class index and is mutated with per-class `methods`/`fields` as the user expands, with no eviction over a long session. |
| J-F31 | SCALE | low | Home dashboard | `jar/js/dashboard.js:57-61` | One card per artifact into a single `innerHTML` string, with no pagination ceiling. |

### J — explicitly checked and clean

Recording these so the absence of a finding is visible as a result rather than an omission:
all summary-module fetch sites are inside `try`/`catch` or have a chained catch (`summary-claude.js:243,389,399,457,799,812,852`;
`summary-codeview.js:89`; `summary-corrections.js:58`; `summary-trace.js:214,386`); the upload SSE/poll race
is guarded (`app.js:402-524`, `_uploading` at `:131,362,524`); the log viewer uses a cursor-based tail and
stops on close (`logviewer.js:65-113`); `loadMoreCollOpPaths` has a single-in-flight guard
(`summary-popups.js:404-406`); the endpoint list caps at 200 with a "Show all" escape hatch
(`endpointList.js:48,50`).

---

# Part 4 — Disabled / dead (bare list, out of scope)

Out of scope by instruction: no findings, no analysis, no tasks. Listed only so a later delete-or-revive
decision is possible. One line of evidence each.

**Shared / home**
1. `shared/auth.js` — no page — referenced by no HTML file (checked all 7).
2. `login.html` — itself — static "no sign-in required" stub; its own comment block records that `/api/auth/login` does not exist and `ApiKeyAuthInterceptor` is not registered.
3. Quick Submit panel — home — the whole panel is inside `style="display:none"` at `index.html:205` and nothing sets it visible, so `switchQsTab` (`:682`), `submitParserJobs` (`:733`) and `submitJarLocal` (`:690`) are unreachable.
4. `UserManual._highlightText` — home — `shared/user-manual.js:182-184` is an empty stub called from `_onSearch`.

**JAR / WAR**
5. Cross-Reference — jar — script tag commented out at `jar/index.html:219`; no `data-tab="crossref"` button exists, so `jar/js/app.js:1595-1597` is unreachable; the panel at `jar/index.html:169-175` shows a "no longer supported" note.
6. `jar/js/dash-panel.js` — no page — loaded by no HTML (see comment at `jar/index.html:224`); the `JA.dashPanel` guards at `app.js:23,72,117,701` are never true.
7. Flow Analysis (`jar/js/flow-analysis.js`, 987 lines) — jar — the tab definition is commented out at `jar/js/summary.js:406`, so `switchSubTab` can never dispatch `'flow'`. Note: the script still loads (`jar/index.html:218`) and `FlowAnalysisController.java:115-495` is live, so finding J-F11 applies if it is ever revived.
8. Collection lifecycle flows (`jar/js/summary-lifecycle.js`) — jar — `_renderLifecycleTab` (`:20`) has zero references outside its own file and there is no `'lifecycle'` key in `_lazyTabs` (`summary.js:470-501`).
9. `jar/js/chatbox.js`, `jar/js/chat-toggle.js` — no page — every HTML file loads `shared/chatbox.js` / `shared/chat-toggle.js` instead (`jar/index.html:250-251`, `war/index.html:212-213`). Measured as stale forks: 333 and 65 differing lines against the shared copies.

**Parser**
10. Sync JAR & Parser (`parser/js/sync.js`, 282 lines) — parser — sole entry point commented out at `parser/index.html:206`; `PA.sync` referenced nowhere else.
11. Statements tab (`parser/js/statements.js`, 368 lines) — parser — rtab commented out at `parser/index.html:255`; no `switchRightTab('statements')` call site exists. Only `PA.statements._getTypeColor` survives, used from `js/summary.js:210`.
12. Exception Handlers tab (`parser/js/exHandlers.js`, 242 lines) — parser — rtab commented out at `parser/index.html:256`; no caller.
13. Raw joins table (`parser/js/joins.js:77` `PA.tf.init('join', …)`) — parser — no Details subtab maps to it (`parser/index.html:359-362` offers only sequences/joinQuery/dynamic/transactions). The rest of `joins.js` survives as a helper library for `joinSummary.js`.
14. Join Summary "by-join" table (`parser/js/joinSummary.js:22`, `:235`) — parser — `js/app.js:747` only ever calls `loadQueryOnly()`, which builds the `'jq'` table.
15. Claude Overview (`PA.claude.openOverview`) — parser — button commented out at `parser/index.html:195`.
16. References ▸ "Called By" and ▸ "Tables" sub-tabs — parser — commented out at `parser/index.html:311` and `:312`; `PA.refs.switchTab` (`js/refs.js:16`) is only reachable with `'calls'`.

**PL/SQL**
17. `plsql/js/summary-corr-logbrowser.js` — plsql — zero callers; the body is a stub that toasts "not yet available for PL/SQL" (`:12,20`).
18. `PA.summary.exportAllReports` / `exportJSON` / `importJSON` (`plsql/js/summary-export.js:10,23,47`) — plsql — no call site; the reachable export path is `showExportModal`.
19. `PA.api.search` (`plsql/js/api.js:251`) — plsql — 0 callers outside `api.js`.
20. `PA.api.findObject` (`plsql/js/api.js:298`) — plsql — 0 callers.
21. `PA.api.getSequenceOperations` (`plsql/js/api.js:176`) — plsql — 0 callers; there is no Sequences tab (`plsql/index.html:237-246`).
22. `PA.api.testDbConnection` (`plsql/js/api.js:264`) — plsql — 0 callers.
23. `PA.api.getReferences` (`plsql/js/api.js:242`) — plsql — 0 callers; the References tab uses `getCallerTree` (`js/app.js:3445`).
24. `PA.api.getParseErrors` (`plsql/js/api.js:215`) — plsql — 0 callers; `PA.showErrors` (`js/app.js:467`) uses the parser errors endpoint.

Note on item 24 and Q-F18: `PA.trace.expandAll` / `collapseAll` (`plsql/js/app.js:3314,3315`) are explicit
no-ops, but unlike the entries above they are **wired to visible buttons** at `plsql/index.html:273,274`.
They are therefore recorded as a live-feature defect (Q-F18), not as dead code.

---

# Part 5 — Reachability unresolved

Not counted live, not counted dead, and deliberately **excluded from the task list**. Each entry states what
was checked and what remained inconclusive. Resolving these needs a running instance with real data — a
static read cannot settle them.

| # | Item | What was checked | Why it is inconclusive |
|---|---|---|---|
| U-1 | Conversion workbench (`conversion/index.html`) | Every `*.html` for a link — only the commented-out card at `index.html:193`. Grepped Java for a `/conversion` view mapping: none, it is served as a static page. | The page and all 11 of its endpoints are real and working, but it has no nav entry and is reachable only by typing the URL. Whether "URL-only, no nav" counts as a live entry point is a product decision. **Audited as live** (Area S-15) on the basis that the feature works; flagged here because the call could go the other way. |
| U-2 | Bundled-JAR browsing on WAR | `jar/js/codeTree.js:995-1019,1112-1142`; `war/js/api.js` (no `bundledJarPages`/`getBundledJarContent`); `WarAnalyzerController.java` (0 hits for `bundled-jars`) | Whether the bundled-JAR UI branch is entered at all on WAR depends on runtime `_bundledJars` state that cannot be evaluated statically. |
| U-3 | Parser `js/triggers.js` / `js/triggerDetail.js` | Not referenced in `parser/index.html`; invoked from `js/tableOps.js` and `js/tableDetail.js` respectively. | Reaching them requires a table row carrying a non-empty `triggers[]`, which depends on runtime data. |
| U-4 | Parser `js/claudeCorrections.js` | Sub-tab exists at `parser/index.html:478` and `js/app.js` `summaryTab._loadSub` dispatches to it, so the **tab** is reachable. | Whether its content is ever non-empty needs a completed Claude review artifact. |
| U-5 | Parser `js/callGraphModal.js` / `js/callGraphFullscreen.js` | Not in `index.html`; referenced only from `js/callGraphViz.js` and each other. | The fullscreen/modal entry is created by `callGraphViz._renderControls()` at runtime; it was not confirmed that the control is emitted unconditionally. |
| U-6 | PL/SQL Summary "Claude Corrections" tab | Reachable button at `js/summary.js:197`; `_renderCorrectionsTab` exists. | Its data comes from `PA.claude.result` (`js/summary-corrections.js:14`), populated only after a Claude verification run; the corrections payload shape could not be confirmed without executing a run. |
| U-7 | PL/SQL Static/Claude header toggle | `plsql/index.html:228`; `js/app.js:1870-1876` — displayed only when `PA.claude.hasData`. | Could not determine statically whether `hasData` is ever set on a plain, non-verified analysis. |

## Counts

- **LIVE features: 96** — Area S 15, Area Q 27, Area P 36, Area J 18.
- **Dead / disabled: 24** (bare list above).
- **Reachability unresolved: 7.**
- **Findings so far: 94** — X 6, S 19, Q 20, P 18, J 31. Revised by the supplements in Parts 6 and 7; see the final count at the end of Part 7. Separately, 20 code paths were explicitly checked and found clean (recorded under Area J).

---

# Part 6 — Area J supplement (second pass)

The JAR/WAR area was re-examined and produced findings not covered above. They are appended rather than
merged so the ID sequence stays stable. One of them **revises** an earlier "clean" verdict.

| ID | Kind | Sev | Feature | Where | Finding and impact |
|---|---|---|---|---|---|
| J-F32 | CORRECTNESS | med | Endpoint list "Show all" | `jar/js/endpointList.js:48,89,127` | **Revises the earlier clean verdict on `endpointList.js:48,50`.** The 200-row cap (`_cap: 200`) exists precisely because, per the comment at `endpointList.js:40`, "the codebases this tool is aimed at have thousands" — but `_showAll` bypasses it entirely and renders `filtered` in full into `innerHTML` at `:127`. The escape hatch reintroduces the exact problem the cap solves. The cap is real; the finding is that its bypass is unbounded. |
| J-F33 | CORRECTNESS | med | Claude progress | `jar/js/summary-claude.js:523-526,533`; `jar/js/app.js:533` | Starting a full scan launches **two** independent pollers against the same `/claude-progress` endpoint — `this._startClaudePoll()` and `JA.app._startClaudePolling(jarId)`. This doubles the request rate and gives the same progress bar two sources of truth. |
| J-F34 | SCALE | med | Home dashboard | `jar/js/dashboard.js:80-82`; caller `app.js:112` | `Promise.allSettled(jars.map(j => JA.api.getClaudeStats(...)))` fires one request per analysed artifact with **no concurrency limit**, on every `JA.dashboard.render` — i.e. on every return to the home screen. Linear fan-out in artifact count. |
| J-F35 | CORRECTNESS | med | Upload / analyze and targeted analysis | `jar/js/app.js:420`; `jar/js/targeted.js:226,229` | `await fetch('/api/queue/' + jobId).then(r => r.json())` with no `res.ok` check. A 500 or an HTML error page becomes a JSON parse throw inside the poll callback rather than a handled state; `targeted.js:229` handles only the "job not found" case. |
| J-F36 | CONSISTENCY | med | Filter / search interactions | `summary-helpers.js:69,75,955`, `summary-tables.js:159,297`; `jar/index.html:128`; `endpointList.js:89` | Three different behaviours for one interaction ("type to narrow a large list"): summary tables debounce, Code Structure search does not, and the endpoint list filters synchronously. The debounce helper already exists and is used in one of the three. |
| J-F37 | ERRORS | low | Summary empty states | `jar/js/summary.js:490-495` | Sub-tab renderers fall back to strings like `'Scheduled jobs loading...'` when their module is absent. On WAR, where `summary-lifecycle.js` is not loaded and modules can legitimately be missing, a permanently-missing module renders a permanent "loading" message rather than an error or an empty state. |
| J-F38 | A11Y | low | Implementation picker | `jar/js/nav.js:123,137-142` | Dismissed only by an outside *click* — no Escape handler, no focus trap, no focus return. Separately, `:123` reads `picker.offsetHeight` before the element has laid out, so the vertical clamp computes against 0. |
| J-F39 | SCALE | low | Artifact sidebar | `jar/js/sidebar.js:20,34,57-66` | The full artifact list re-renders via `innerHTML` on every group expand/collapse, and the `_groupJars` sort runs per render. Fine at tens of artifacts; noted for completeness. |

Two further reachability questions were raised and are added to Part 5:

| # | Item | What was checked | Why it is inconclusive |
|---|---|---|---|
| U-8 | Visual view mode (`jar/js/codeTreeViews.js`) | `jar/index.html:123` wires `setViewMode('visual')` and the file is loaded. | Whether the renderer produces output for a corrected-mode analysis could not be confirmed without running it. |
| U-9 | WAR modules loaded but possibly inert | WAR loads the `jar/js/*` set **minus** `flow-analysis.js`, `targeted.js` and `summary-lifecycle.js`; `war/js/app-overrides.js` is 16 lines and only repoints `EventSource` from `/api/jar/progress` to `/api/war/progress`. | Which JAR summary sub-tabs actually produce content on WAR depends on the 7 missing `JA.api` methods (J-F2/J-F3) and on runtime data; a static read cannot enumerate the degradation per sub-tab. |

**Revised counts after this supplement:** findings **102** (X 6, S 19, Q 20, P 18, J 39); reachability
unresolved **9**. Live-feature and dead-item counts are unchanged at 96 and 24.

---

# Part 7 — Area J supplement 2

A third pass over JAR/WAR surfaced findings distinct from Parts 3 and 6. Appended, again, rather than
merged, so IDs stay stable.

| ID | Kind | Sev | Feature | Where | Finding and impact |
|---|---|---|---|---|---|
| J-F40 | SCALE | high | Load / select analysis | `jar/js/app.js:493,510,557,752,1319,1352`; `jar/js/targeted.js:202` | Seven separate call sites repeat the same pattern: `getSummaryHeaders(...)` with `catch → getSummary(...)`. A single failure of the cheap header endpoint therefore **silently degrades to downloading the full 47 MB analysis document**, at seven different places, with no user-visible signal that it happened. The fallback should be a visible degraded mode, not a silent one, and it should exist once rather than seven times. |
| J-F41 | CONSISTENCY | med | Collection drill-down on WAR | `war/index.html:189` loads `jar/js/summary-popups.js:357-361`; `WarAnalyzerController.java` has 0 hits for `call-paths` | WAR renders the collection-op drill-down affordance even though its backing route does not exist. The user is given a clickable control that **always** errors. This is J-F3 seen from the UI side: the missing method does not hide the control. |
| J-F42 | CONSISTENCY | med | Header badges on WAR | `jar/index.html:90-91` vs `war/index.html`; `jar/js/app.js:786` | `war/index.html` has no `#jar-conn-info` or `#jar-domain-config-info` element, so `app.js:786` early-returns. Two pages share one app module but have divergent header markup with no shared definition of what that module requires. |
| J-F43 | CONSISTENCY | low | Dead code still shipped | `jar/index.html:218`, `:206`, `:219`; `jar/js/summary.js:406-411` | Dead code is retained in three different styles — script tag commented out, tab definition commented out, and file simply orphaned. Measured by `wc -l`: `cross-ref.js` 2,713 + `flow-analysis.js` 987 + `dash-panel.js` 134 + `summary-lifecycle.js` 156 = **3,990 lines** of unreachable JS. Of these, `flow-analysis.js` and `summary-lifecycle.js` are still **downloaded and parsed on every jar page load**. Deleting them is out of scope per the user's instruction; the shipping cost is recorded because it affects live page load. |
| J-F44 | CORRECTNESS | low | Call-tree fetch | `jar/js/api.js:249-257` | The `format=dag` call-tree request is issued and, on failure, a second unpaged request is made to the same URL — two full call-tree downloads on the slow path. |
| J-F45 | ERRORS | low | Domain config failure | `jar/js/summary.js:66` | The only signal that domain config failed to load is a `console.warn` — no toast and no degraded-mode banner. Per J-F4 this is the **permanent** state on WAR, so the entire Summary silently runs with default classification and the user is never told. |

Also recorded from this pass, strengthening existing findings rather than adding new ones:
`jar/js/api.js` was measured at **43 fetches with 43 `res.ok` checks** — the discipline exists and is
complete in that file, which is what makes its absence at `jar/js/app.js:420` and `jar/js/targeted.js:226`
(J-F35) an inconsistency rather than a house style. Additional `div`-as-button sites for J-F28:
`jar/js/sidebar.js:75,113` (artifact row and version toggle) and `jar/js/summary.js:337-341` (stat chips).

Two further unresolved entries for Part 5:

| # | Item | What was checked | Why it is inconclusive |
|---|---|---|---|
| U-10 | `jar/js/summary-explorer.js`, `jar/js/summary-codeview.js` | Grepped `jar/js/` for `Explorer`, `explorer`, `CodeView`, `codeview` — only a load-order comment at `summary.js:6`. | Both `Object.assign` onto `JA.summary`, so their methods are reachable under names not enumerated. Part 3 lists concrete onclick entry points (`summary-codeview.js:842,877,909,920,1189`; `summary-explorer.js:326,356`), so they are **audited as live**; the uncertainty is over which of their exports are reachable, not whether the modules are. |
| U-11 | `jar/js/summary-scheduled.js` | `summary.js:490` calls `this._renderScheduledTab` behind a truthiness check; the tab button exists (`summary.js:373`). | It was not confirmed that the file actually defines that name. If it does not, the tab renders a permanent "loading" string — which is exactly the failure mode described in J-F37. |

**Final counts:** live features **96**; dead/disabled **24**; reachability unresolved **11**;
findings **108** (X 6, S 19, Q 20, P 18, J 45).

The counts stated at the end of Part 5 (94) and Part 6 (102) are running subtotals taken before the
supplements were appended. This line is the authoritative total.

