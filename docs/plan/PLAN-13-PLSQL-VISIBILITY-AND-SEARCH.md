# PLAN-13 — PL/SQL visibility and cross-analysis search

Plan only. Read-only investigation, 2026-09-04. Every number measured from `data/plsql-parse/*` on
this machine, not estimated.

## 0. Corrections to the stated ground truth — read first

Three of the assumptions handed to this investigation were wrong, and one invalidates the naive
version of features 1, 2 and 5.

### 0.1 Confirmed

| Claim | Verdict |
|---|---|
| `Access Details (N)` is a flat unbounded `<div>` list | Confirmed. `plsql/js/tableOps.js:199`, `parser/js/tableOps.js:197`, both in `_renderDetail`. No cap, pager or filter. |
| N reaches 475 | Confirmed for ORACLE/`ERP_PRODUCT`, but **understated** — real max is **1385** (MYSQL/`erp_audit_log`). |
| `PA.tf` at 24 sites, page size 50 | Confirmed; 2 of the 24 are doc comments, so **22 real sites**, all 50. |
| `cross-ref.js` disclosed-cap pattern | Confirmed at `:279-289` and again `:399-416`. |
| `TableDetailController`'s 4 routes uncalled | Confirmed in `docs/api-render-map.json`. 42 unused / 247 used. |
| Flow tree 500-node budget | Confirmed, `plsql/js/app.js:1559`. |
| `ArtifactStore` + ratchet-down gate | Confirmed. A class doing **less** I/O than its budget also fails. |

### 0.2 Wrong — and it changes the plan

**(a) `tables.json` has no `accessDetails`, and `ddl[]` is empty in all five dialects.**
It is a map keyed `SCHEMA.TABLE` with keys `schema, tableName, objectType, operations, lines,
referencedBy, columns`. `ddl` is plumbed (`SchemaTableInfo.java:25`, folded at
`DependencyCrawler:1587`) but **zero** tables in **any** dialect have a non-empty `ddl` — the demo
sources contain no in-procedure DDL. Do not build UI on `ddl[]` until a source with DDL exists.

**(b) The `accessDetails` the UI renders are synthesised in the browser — and one branch is dead.**
`parser/js/api.js:97-122` builds them from `api/tables/index.json` →
`tables[].usedBy[].operations{OP:[line,...]}`, emitting
`{operation, procedureId, procedureName, lineNumber, sourceFile: ''}`. Therefore:

- the `whereFilters` branch at `tableOps.js:214-217` is **dead on this path** — measured 0
  accessDetails with `whereFilters` in every dialect;
- because `sourceFile` is hardcoded `''`, the `L{n}` click-to-source at `:208` **never renders**
  (its guard is `d.sourceFile && d.lineNumber`). **A live bug, and the cheapest win in this plan.**

**(c) Per-statement `columnUsages` is NOT persisted in any served artifact.**
`api/nodes/*.json` `tables[]` entries carry only `schema, name, objectType, operations, joins`.
The writer (`ChunkedFlowWriter.java:819-852`) **groups by table name and drops `sqlText` and
`columnUsages`**. That detail survives only in `<analysis>/.node-cache/<HASH>.json`, which is a
content-hash **cache**, not an artifact — SQL Server has **1** cache file for 57 nodes. So
statement-granularity provenance is unavailable to the server today.

> This does **not** contradict the verified table-level result: `tables.json` `columns[]` is real,
> and the per-dialect counts (ORACLE 30, MYSQL 161, DB2 75, POSTGRES 97, SQLSERVER 7) are correct.
> Two different grains — the folded per-column summary exists; the per-statement rows do not.

**(d) The stale-response guard exists only in `parser/js/sourceView.js`.** `plsql/js/sourceView.js`
has **none** and needs one.

### 0.3 An existing silent truncation to fix

`DependencyCrawler:1594` `MAX_COLUMN_LINES = 25` caps `columns[].lines` with **no total recorded**.
MySQL and SQL Server samples both sit at exactly 25, i.e. truncated. This breaks the project's own
never-truncate-silently rule **at the producer**, and it was introduced by the very change that
added `columns[]`. Fix: emit `lineCount` (true total) beside `lines`. Additive.

### 0.4 The thing that makes all of this cheap

A **server-side paged-facet framework already exists** and is the right vehicle for features 1, 2
and 5: `unified-web/src/main/java/com/plsqlanalyzer/web/page/` — `FacetPageResponder`, `FacetSpec`,
`FacetSort`, `JsonFacetPager`, `JsonArrayRows`.

- `Params(limit, after, sort, q, types, cf, accept)` — cursor paging, text search, column filters.
- Emits a **true total** (`:261-262` `total` + `totalIsExact`) and an `X-Analyzer-Truncated` header.
- **Opt-in by `Accept: application/vnd.analyzer.page+json`** (`wantsEnvelope()`, `:66-75`), so the
  default response stays byte-identical — the API-compatibility constraint is satisfied for free.
- `TableDetailController.serve()` is a 30-line template to copy.

**Nothing in features 1–5 should invent pagination, totals or filtering. All of it exists.**

## 1. Measured data

### 1.1 Access details per table — the real distribution

| Dialect | Tables shown | Total access rows | Max (table) | >50 | >100 | >200 | Median |
|---|---|---|---|---|---|---|---|
| ORACLE | 41 | 2,435 | **475** (`ERP_PRODUCT`) | 11 | 8 | 2 | 32 |
| MYSQL | 31 | 4,614 | **1385** (`erp_audit_log`) | 18 | 8 | 6 | 66 |
| DB2 | 35 | 3,933 | **1144** (`ERP_AUDIT_LOG`) | 22 | 10 | 5 | 53 |
| POSTGRES | 30 | 2,336 | **571** (`erp_product`) | 11 | 5 | 4 | 3 |
| SQLSERVER | 26 | 563 | **184** (`Inventory`) | 2 | 1 | 0 | 13 |

Max distinct procs on one table: POSTGRES **84**, MYSQL 46, SQLSERVER 40, DB2 38, ORACLE 18.

**1385 rows in an unfiltered `<div>` list, median 3–66. Page size 50 plus a filter is exactly
right; virtualisation is not needed.**

### 1.2 `columns[]` in `tables.json`

| Dialect | Tables | With `columns[]` | Column rows | Max cols/table | With `ddl[]` |
|---|---|---|---|---|---|
| ORACLE | 42 | 30 | 60 | 5 | **0** |
| MYSQL | 232 | 161 | 421 | 71 | **0** |
| DB2 | 100 | 75 | 283 | 55 | **0** |
| POSTGRES | 114 | 97 | 236 | 8 | **0** |
| SQLSERVER | 85 | 7 | 13 | 4 | **0** |

**Table-count mismatch:** `tables.json` has 232 MySQL tables; `api/tables/index.json` (what the UI
shows) has 31 after filtering — the former includes temp/session/derived tables. Any join must key
on `UPPER(schema + '.' + tableName)` and tolerate misses **both** ways.

### 1.3 Per-statement detail — where it actually is (`.node-cache`)

| Dialect | Cache files | Statement rows | With `columnUsages` | With `sqlText` | With `whereFilters` | Bytes |
|---|---|---|---|---|---|---|
| ORACLE | 48 | 2,436 | 2,322 | 2,398 | **0** | 3.6 MB |
| MYSQL | 63 | 5,015 | 4,549 | 5,015 | **0** | 7.0 MB |
| DB2 | 57 | 4,063 | 1,331 | 1,331 | **0** | 3.9 MB |
| POSTGRES | 104 | 2,662 | 268 | 270 | **0** | 1.6 MB |
| SQLSERVER | **1** | 291 | 17 | 58 | **0** | 0.2 MB |

`whereFilters` is **zero everywhere**. The SQL Server row is why the cache cannot be the source of
truth: one file, the rest were cache misses.

### 1.4 Left panel / trace sizes

| Dialect | Proc list | Unique edges | Rendered tree nodes | Depth |
|---|---|---|---|---|
| ORACLE | 52 | 218 | 275 | 14 |
| MYSQL | 69 | 567 | **968** | 27 |
| DB2 | 64 | 406 | 491 | 18 |
| POSTGRES | **114** | 847 | **1248** | **71** |
| SQLSERVER | 57 | 337 | 389 | 27 |

**At most 114 procs and 1248 tree nodes — neither justifies virtualisation.** The real problems are
depth 71 and that `PA.filterLeft` (`app.js:1413`) sets `display` on matches but never re-expands
their collapsed ancestors, so a deep match is "shown" yet invisible.

## 2. Feature 1 — Access Details as a real table

### 2.1 "Magnification filter" — stated interpretation

Read as two things the request conflates, and both are adopted:

1. **Filter** — text box plus per-column facet dropdowns = `PA.tf` search + `initColFilters`.
2. **Magnification** — a per-row expand that zooms into one access: its `sqlText`, column list with
   value provenance, and predicates, without navigating away = `PA.tf`'s nested `renderDetail`.

**Not** interpreted as a CSS zoom or font slider. See open question Q1.

### 2.2 Files

| File | Change |
|---|---|
| `plsql/js/accessDetails.js` | **New** (~220 lines). `PA.accessDetails.mount(hostElId, tableId, rows, opts)` — the one shared renderer. |
| `parser/js/accessDetails.js` | Byte-identical copy; the repo already duplicates `tableOps`/`tableFramework`/`sourceView` this way. |
| `plsql/js/tableOps.js:187-247`, `parser/js/tableOps.js:185-245` | Replace the flat loop with a host div + `mount(...)`. |
| both `tableFramework.js` | Add **one** option, `onDetailRendered(item, idx)`, called at the end of `toggleDetail` (`:226-247`) — needed because `renderDetail` returns a string and `PA.tf.init` needs its `-tbody` in the DOM first. 3 lines. |
| `parser/js/api.js:97-122` | **Bug fix**: stop hardcoding `sourceFile: ''`; resolve from `index.nodes[]`. This is what makes `L{n}` clickable. |

### 2.3 Row shape and degradation

Columns: `AI?` · Operation · Procedure · Line · Columns (first 3 + `+N`) · Value sources ·
Predicates. Sortable on operation/procedure/line/column count; facets on operation, procedure,
column, value source.

**Degradation contract:** when `columnUsages` is absent the cells render `—` and their facets do not
appear. SQL Server (7 tables, 17 statements) is the test case. **Never render "0 columns" for
"unknown columns".**

### 2.4 Server side

`ChunkedFlowWriter` must stop dropping `columnUsages`/`sqlText`. Two options; **B recommended**:

- **A** — enrich `api/nodes/*.json` with `statements[]`. MySQL's node payload grows 3.1 MB → ~10 MB,
  and it is loaded per node.
- **B** — a new artifact `api/statements/index.json`, one `ArtifactKey` written by
  `ChunkedFlowWriter` beside `api/tables/index.json` (`:744`). 1.6–7.0 MB. Never loaded whole by the
  browser; served only through facet endpoints with `limit`.

New endpoints mirroring `TableDetailController`:

```
GET /api/parser/analyses/{name}/table-accesses?table=&limit=&after=&sort=&q=&cf=&scope=&procId=
GET /api/parser/analyses/{name}/table-accesses/summary?table=&scope=&procId=
```

All I/O via `ArtifactStore.readTree(ArtifactKey)`, so **no class gains a gate budget**.

**Interim step, zero server change — ship first:** the new table with page-50, filters, true counts
and clickable source using today's synthesised rows, plus the *table-level* `columns[]` shown in the
zoom row **labelled as table-level, not statement-level**.

## 3. Feature 2 — the same detail in the popup modal

**The actual path:** `PA.tableInfo.open(tableName, schemaName)` — defined at
**`plsql/js/claude.js:1075`**, markup `plsql/index.html:612-640`. Tabs: columns, constraints,
indexes, definition, query.

**It is DB-metadata driven, not parse-driven** — its columns come from the live database
(`columnId, dataType, nullable, …`) and it shows **nothing** about how the code uses the table.
That is the real gap. It also rolls its own filter machinery (`_tiFilter:1157`, `_tiFilterRow:1183`)
— an unpaged duplicate of `PA.tf` with a count from `array.length`. Do not extend it.

Plan: add a **Usage tab** whose body is one call to `PA.accessDetails.mount(...)` — the renderer is
not duplicated. Distinct `PA.tf` id (`tiUse`) because `_state` is a single global map keyed by id;
`close()` must delete it. `renderUsage` is async, so it **needs** the request-token guard
(`_reqSeq`/`_isCurrent` from `parser/js/sourceView.js:53,64`) — without it, clicking two tables
quickly paints the first table's usage under the second's title, reproducible today.

Optional, 20 lines added and 50 deleted: migrate the existing tabs to `PA.tf` and delete
`_tiFilter`/`_tiFilterRow`.

## 4. Feature 3 — left panel for large traces

What is actually wrong, at a measured ceiling of 114 procs / 1248 nodes / depth 71:

1. `PA.renderProcedureList` (`app.js:1656`) is one flat alphabetical list, though `p.id` is
   `SCHEMA$PACKAGE.PROC` and `getSchemaColor` (`:3271`) already exists.
2. **`PA.filterLeft` (`:1413`) is broken for the tree** — never re-expands matched nodes'
   ancestors, so a depth-40 match is invisible. This is the actual "does not cope".
3. No match count; `All Procedures (N)` comes from `array.length`.
4. `PA.globalSearch` (`:3287`) duplicates `filterLeft`, worse.

Plan: fix `filterLeft` first (~25 lines, walk the parent chain and force ancestors open, show
`M of N match`) — that alone addresses most of the complaint. Then group by schema/package with the
existing toggle, add a sticky ancestor breadcrumb, cap indentation at ~12 levels showing `·{depth}`
beyond, and delete `globalSearch`. **Do not virtualise.**

## 5. Feature 4 — trace readability

Files: `plsql/js/summary-trace.js:96`, `parser/js/txnTrace.js`, `plsql/js/app.js:1514/1568`.
No framework change, no dependency, no build step:

1. Sticky depth/step column (`position:sticky; left:0`).
2. Reuse the existing `op-badge` classes so `UPDATE` is one colour everywhere.
3. Depth banding, indent capped at 12 (solves depth 71).
4. **Collapse runs of identical consecutive steps** — `INSERT ERP_AUDIT_LOG ×37 (L322…L1158)` with a
   disclosed expander in the cross-ref phrasing. DB2 has 1144 near-identical audit inserts, MySQL
   1385. Never drop rows without the count.
5. Draw COMMIT/ROLLBACK boundaries from the existing `transactionScope` / `transactionSummary`.
   Highest-value readability change, and it needs no new data.

Not doing: a graph/mini-map (`callGraphViz.js` already exists), theming, animation.

## 6. Feature 5 — cross-analysis search

The unit of an answer is a **statement–column pair** — precisely the data §0.2(c) shows is not
persisted. **Feature 5 is blocked on `api/statements/index.json` (§2.4 B).** There is no way around
it: served artifacts carry grouped `operations{OP:[lines]}` with no column attribution, and the
`.node-cache` is a cache.

Row: `{stmtId, nodeId, nodeName, sourceFile, schema, tableName, objectType, operation, line,
sqlText, columns[], predicates[], joins[]}`. Expect 291 (SQLSERVER) – 5,015 (MYSQL) rows.

Built **at crawl time** in `ChunkedFlowWriter` (the data is already in memory — a flatten, not a
re-parse), stored as one `ArtifactKey`, chunked per PLAN-06 if it grows. **No persisted inverted
index**: build a `Map<upperToken, IntList>` lazily, memoised like `TableDetailFacetReader:150-152`.
At ≤5,015 rows a linear scan is sub-millisecond; the memo avoids re-parsing JSON, not slow matching.

Grammar — deliberately small, ~60 lines JS / ~80 Java:

```
term := qualifier ':' value | bare
qualifier := table | proc | col | op | src | file | line
```

`op:UPDATE table:ERP_INVENTORY col:QTY_ALLOCATED proc:USP_ALLOCATE` is the motivating question.
Bare words AND; same-qualifier repeats OR; `!` negates; `"…"` quotes a phrase. Unknown qualifier →
**400 listing the valid ones**, never a silently empty page. Trailing `*` only — **no regex**, which
over 5,000 `sqlText` blobs from a URL param is a DoS surface for no gain.

API: `GET …/search?q=…` and `…/search/summary?q=…` in a new `PlsqlSearchController` structurally
copying `TableDetailController`. Envelope carries `page.total` (server-provided, **never
`rows.length`**) and `facets`, so the modal can truthfully say "42 UPDATEs across 9 procs" before a
row is drawn. Add `PA.utils.trueCount` (PL/SQL has no twin of `JA.utils.trueCount`) which reads
`page.total` first and **marks the result approximate** on fallback so the UI can render `~N`.

UI: topbar box, `/` to focus, 250 ms debounce, `limit=50`, **request-token guard mandatory** (the
most stale-prone panel here). Results modal shows the true total, the parsed query echoed back as
removable chips (so the user sees how their text was interpreted), facet pills, then
`PA.accessDetails.mount(...)` **a third time**. Zero results is a stated zero with the parsed
interpretation, not a blank box.

## 7. Ordering

```
Phase 0 — independent, no server change
  0a  parser/js/api.js sourceFile bug -> L{n} clickable            1-2 h
  0b  PA.filterLeft ancestor-expand + match count                 2-3 h
  0c  PA.tf onDetailRendered hook (3 lines, both copies)           30 min
  0d  DependencyCrawler lineCount beside the 25-line cap (0.3)     1 h
Phase 1  needs 0c   accessDetails.js + tableOps swap (F1, interim) 1-2 d
Phase 2  needs 1    tableInfo Usage tab + guard (F2)               0.5-1 d
Phase 3  parallel   left panel (F3) / trace pass (F4)              1-2 d each
Phase 4  the gate   ChunkedFlowWriter -> api/statements/index.json 2-3 d
Phase 5  needs 4    table-accesses endpoint (F1/F2 gain real cols) 1-2 d
                    search endpoint + grammar + modal (F5)         3-5 d
```

Independent: 0a, 0b, 0d, 3, 4. All column provenance funnels through Phase 4. Phases 5+ need no new
renderer — the payoff for factoring `accessDetails.js` early.

## 8. Risks and the cheapest proof

| # | Risk | Cheapest proof, before building |
|---|---|---|
| R1 | `api/statements/index.json` too large / breaks the 16 MB rule | Already bounded: `.node-cache` is 0.2–7.0 MB and is a **superset**. Flatten one MySQL cache file with a 20-line script and read the byte count. |
| R2 | Nested `PA.tf` inside an expanded row breaks (id collisions, `initColFilters` resolving the **outer** `<table>`) | 30-line spike: hardcode one nested table into `_renderDetail` for `ERP_PRODUCT`. **Do this before Phase 1.** |
| R3 | `tables.json` `columns[]` cannot be joined to `api/tables/index.json` (232 vs 31 tables) | Script the `UPPER(schema.table)` join across all 5 dialects, print unmatched both ways. 15 min. Decides whether the interim step ships. |
| R4 | Left panel really needs virtualisation on customer data | Synthesise 10,000 edges from POSTGRES, measure `renderFlowTree` wall time. Under 100 ms ⇒ no. |
| R5 | Linear search scan too slow | Time a bare-word scan over MySQL's 5,015 rows; assert < 20 ms. |
| R6 | New fields change an existing response | Snapshot every `/api/parser/analyses/{name}/**` response for all 5 analyses before/after and diff. The `Accept` envelope means it should be a no-op — prove it. |
| R7 | SQL Server degrades to an empty-looking feature | Make SQLSERVER the **first** manual test, not the last. Every empty cell reads `—`; no count reads `0` where the truth is `unknown`. |
| R8 | `DirectFileIoGateTest` breaks | It will not if new I/O is `ArtifactStore` in an already-allowlisted class. Trap: a class doing **less** I/O than its budget also fails. |

## 9. Not worth doing

- **Persisted inverted index for search** — 291–5,015 rows. Revisit at ~100k statements.
- **Virtualised left panel** — ceiling 114 procs / 1248 nodes. Would add ~200 lines of scroll maths,
  break `filterLeft`, break the three `querySelectorAll('.lp-item')` callers, and break Ctrl-F.
- **Any UI on `ddl[]`** — zero tables in any dialect have one. Building against zero test data.
- **Designing around `whereFilters` on the parser path** — measured 0 populated everywhere. Keep the
  column (it costs nothing, the legacy path fills it) but do not promise predicate search yet.
- **Unifying the two model trees** (`com.plsql.parser.model` vs `com.plsqlanalyzer.parser.model`) —
  a separate plan, not a prerequisite for any of these five.
- **Reusing the 4 uncalled `table-joins`/`table-columns` endpoints for feature 1** —
  `getTableColumnRefs` answers a different question (`%TYPE`/`%ROWTYPE` and parameter type refs,
  kinds `TYPE_REF`/`PARAMETER`). Worth wiring as a separate tab; **must not be conflated** with
  access detail. Copy its structure, not its semantics.

## 10. Open questions

1. **"Magnification filter"** — is §2.1 right (facet filters + per-row zoom), or did you mean a
   literal magnifier over the source snippet?
2. **Re-crawl acceptable?** §2.4/§6 need the crawl re-run to emit the new artifact. Old analyses will
   have no column detail and must degrade like SQL Server. Confirm, or a backfill from `.node-cache`
   is needed — and that is incomplete (1 file for SQL Server).
3. **Search scope** — one analysis (planned), or across analyses of a project/env (materially bigger)?
4. **New artifact or enrich `api/nodes/*.json`?** §2.4 recommends the separate artifact.
5. **Page-size selector?** All 22 `PA.tf` sites hardcode 50. The 1385-row list wants a selector
   (`setPageSize` exists at `tableFramework.js:181`) — is breaking the "always 50" invariant fine?
