# API Error Contract Audit

**Scope:** how the HTTP layer handles failure, and what the user is told.
**Date:** 2026-09-04. **Method:** source read + live probes against a real server on port 18095
(`unified-web-1.0.0-SNAPSHOT.jar`, `data/` freshly wiped, so every "missing analysis" path was
genuinely exercised). Every status code in this document was observed, not inferred.
`_migration-backup-*` and `target/` excluded throughout.

---

## Correction to the premise

The brief stated "~51 sites of `if (!res.ok) return [];` in `plsql/js/api.js` and ~21 in
`parser/js/api.js`". The actual figures:

| File | `!res.ok` sites | `return []` | `return null` | `return {…}` | `throw` |
|---|---|---|---|---|---|
| `plsql/js/api.js` | 54 | **27** | 24 | 3 | **0** |
| `parser/js/api.js` | 38 | **9** | 12 | 0 | 17 |
| `jar/js/api.js` | 42 | 0 | 4 | 3 | **34** |
| `war/js/api.js` | 35 | 0 | 3 | 1 | **30** |
| **Total** | **169** | **36** | **43** | **7** | **81** |

The "~72 call sites" figure is correct under a different reading: `return []` + `return null`
across `plsql` + `parser` is exactly **36 + 36 = 72**. That is the real population.

Two things the premise got wrong, and both change the recommendation:

1. **`jar/js/api.js` and `war/js/api.js` are already correct** — 64 of their 77 `!res.ok` sites
   throw with the server's message. The good pattern already exists in this repo; it does not need
   to be invented, only propagated. `plsql/js/api.js` is the sole file with **zero** throw sites.
2. **`getParseErrors()` has no call site.** It is dead code. The "parse was clean" defect is real
   but it lives elsewhere and is **a server-side defect, not a client one** — see A-1 and C-1.

---

## A. Server side

### A-0. There is no global exception handling. At all.

`grep -r "@ControllerAdvice|@RestControllerAdvice|@ExceptionHandler"` over
`unified-web/src/main/java` returns **zero matches**. (The only `ExceptionHandler` string hits are
`ComplexityConfig.getExceptionHandlers()`, an unrelated complexity-weight property.)

Consequence: every uncaught exception falls through to Spring Boot's default `BasicErrorController`
and reaches the browser as `{"timestamp","status":500,"error":"Internal Server Error","path":…}`.
Observed shape, live:

```
GET /api/jar/jars/NOPE  →  404  {"timestamp":"2026-09-04T07:57:57.817+00:00","status":404,"error":"Not Found","path":"/api/jar/jars/NOPE"}
```

Every error shape in the application is hand-rolled per controller. The only structured mechanism
in use is `ResponseStatusException` (~80 throw sites, 22 of them in `WarAnalyzerController` alone —
the best-behaved controller in the codebase).

`ExternalApiLinkController.java:31,36` declares `throws IOException` with nothing to catch it, so a
missing analysis file there produces a raw whitelabel 500 — unlike every other jar endpoint.

### A-1. Missing-resource behaviour is inconsistent *within a single controller*

The clearest demonstration, both branches from the same class
(`detail/TableQueryService.java`), both observed live with no analysis loaded:

| Endpoint | Observed | Source |
|---|---|---|
| `GET /api/plsql/analysis/tables` | **404** | `TableQueryService.java:27` `if (result == null) return ResponseEntity.notFound().build();` |
| `GET /api/plsql/analysis/sequences` | **200 `[]`** | `TableQueryService.java:50` `if (result == null …) return ResponseEntity.ok(Collections.emptyList());` |
| `GET /api/plsql/analysis/joins` | **200 `[]`** | `TableQueryService.java:56` (same shape) |
| `GET /api/plsql/analysis/cursors` | **200 `[]`** | same class |

Same condition (`result == null`), same file, two different answers.

**Live probe results — `/api/plsql/analysis/*` with no analysis loaded:**

| Endpoint | Status | Body |
|---|---|---|
| `/api/plsql/analysis` | 200 | `{"status":"no_analysis"}` |
| `/api/plsql/analysis/tables` | 404 | — |
| `/api/plsql/analysis/procedures` | 404 | — |
| `/api/plsql/analysis/call-graph` | 404 | — |
| **`/api/plsql/analysis/errors`** | **200** | **`[]`** ← the server asserts "no parse errors" |
| `/api/plsql/analysis/joins` | 200 | `[]` |
| `/api/plsql/analysis/cursors` | 200 | `[]` |
| `/api/plsql/analysis/sequences` | 200 | `[]` |
| `/api/plsql/analysis/versions` | 200 | `[]` |
| `/api/plsql/analysis/references/NOPE` | 200 | `[]` |
| `/api/plsql/analysis/search?q=x` | 200 | `[]` |
| `/api/plsql/analysis/history/NOPE` | 404 | — |
| `/api/plsql/analysis/tables/NOPE` | 404 | — |
| `/api/plsql/analysis/detail/NOPE` | 404 | — |

**Live probe results — `/api/parser/analyses/NOPE/*` (analysis does not exist):**

| Endpoint | Status | Body |
|---|---|---|
| `/index`, `/procedures`, `/tables`, `/tables/summary`, `/call-graph`, `/errors`, `/node/NOPE`, `/call-tree/NOPE` | 404 | — |
| **`/joins`, `/cursors`, `/sequences`, `/dynamic-sql`** | **200** | **`[]`** |
| `/joins/summary` | 200 | `{"total":0,"scopedTotal":0,…}` |

So for an analysis name that does not exist, the parser API reports both "not found" **and** "exists
and has zero joins", depending on which tab you open.

**Live probe results — jar / war / conversion / queue:**

| Endpoint | Status | Body |
|---|---|---|
| `/api/jar/jars/NOPE`, `/summary`, `/summary/header`, `/classes/tree`, `/endpoints/by-index/999/call-tree` | 404 | Spring default body |
| `/api/jar/jars/NOPE/connections` | 200 | `{"available":false}` |
| `/api/jar/jars/NOPE/summary/endpoints?limit=abc` | 400 | Spring default body |
| `/api/war/wars/NOPE`, `/summary` | 404 | Spring default body |
| `/api/conversion/jobs/NOPE` | 404 | — |
| **`/api/queue/NOPE`** | **200** | **`{"error":"Job not found: NOPE"}`** |
| `/api/plsql/db/table-info/NOPE` | 200 | `{"tableName":"NOPE","found":false}` ← *good* explicit-flag pattern |
| `/api/plsql/db/objects/NOSUCHUSER` | 404 | — |

**Eleven distinct cross-controller inconsistencies** were catalogued. The load-bearing ones:

1. **"That analysis does not exist" has four answers.** jar/war → 404 (`AnalyzerController:1076`,
   `WarAnalyzerController:905`); parser → 404 but via a blanket `catch (Exception)`;
   `PlsqlSearchController:103-112` → 200 `{"indexed":false}`;
   `PlsqlEntityQueryController:65-80` → 200 `{"count":0}`.
2. **`GET /{id}` is 404 but `GET /{id}/collections` is 200 `[]`** for the same nonexistent id —
   `AnalyzerController.java:1382` and `WarAnalyzerController.java:577`. Within one controller the
   same missing resource is both absent and empty.
3. **`/{id}/catalog`** returns 200 `status:"none"` (`AnalyzerController:1179`,
   `WarAnalyzerController:555`) where every sibling by-id endpoint returns 404. The client cannot
   distinguish "no catalog" from "no such jar".
4. **`DatabaseController` contradicts itself.** `GET /objects` returns **502 `{"error"}`** on the
   non-Oracle path (:91) but **200 `[]`** on the Oracle path (:106) for the same connection failure.
   `GET /packages/{username}` returns 200 `[]` (:247) where the structurally identical
   `GET /objects/{username}` returns 502 (:121).
5. **"No analysis loaded" has four answers under one URL prefix.**
   `web/controller/ClaudeController.java:97` → 400; `TableQueryService.java:27` → 404;
   `HistoryQueryService.java:33` → 200 `{"status":"no_analysis"}`; `MiscQueryService.java:101` → 200 `[]`.
6. **Two `ClaudeController`s, two conventions.** `web/controller/ClaudeController` → 400 + `{"error"}`;
   `web/parser/controller/ClaudeController` → 500 + `{"error"}`, for essentially the same failures.
7. **`GET /{id}/bundled-jars`** (`AnalyzerController:1736`) has two missing-resource contracts on one
   URL: 204 for envelope requests (:1754), unchecked 200 for plain ones (:1746).

### A-2. Endpoints that return 200 with a body meaning "error" — the worst case

Twelve confirmed. Ranked by damage:

| # | Endpoint | Code | Body at 200 |
|---|---|---|---|
| 1 | `POST /api/jar/jars/{id}/flow/run` | `FlowAnalysisController.java:123` | `return Map.of("error", "Failed to create run directory: " + e.getMessage());` — bare `Map` return type, so **200** |
| 2 | `POST /api/plsql/db/query` | `DatabaseController.java:741-746` | any SQL failure → `success:false` + `error`, at 200 |
| 3 | `GET /api/conversion/jobs/{jobId}/report` | `ConversionController.java:299,314,317,323` | four separate `ResponseEntity.ok(Map.of("found", false, "error", …))` |
| 4 | `GET /api/queue/{jobId}` | `QueueController` | `{"error":"Job not found: NOPE"}` at 200 — **live-verified** |
| 5 | `POST /api/jar/jars/{id}/flow/test-connection` | `FlowAnalysisController.java:501,539` | `success:false` at 200 |
| 6 | `GET /api/jar|war/…/{id}/catalog` | `AnalyzerController:1179`, `WarAnalyzerController:555` | `status:"none"` at 200 for a nonexistent id |
| 7 | `GET /api/plsql/analysis` | `HistoryQueryService.java:32-34` | `{"status":"no_analysis"}` at 200 |
| 8 | `GET /api/parser/…/{name}/search` | `PlsqlSearchController.java:103-112` | `{"indexed":false}` at 200 — a typo'd name is indistinguishable from an un-indexed analysis |
| 9 | `POST /api/plsql/db/test`, `POST /api/plsql/config/test-connection` | `DatabaseController:59-70`, `ConfigController:398,411` | `success:false` at 200 |
| 10 | `GET /api/plsql/config/…/connections` | `ConfigController.java:300-302` | `{"success":false,"message":"No connections configured"}` |
| 11 | `GET …/crossref/claude/progress` | `CrossRefController.java:166-170` | returns the service map verbatim at 200; siblings at :180/:217/:230 prove it can carry `"error"` |
| 12 | conversion/flow job status | `ConversionController:182`, `FlowRunStore:345` | `error` field inside a 200 document (defensible for async jobs, but it is the *only* failure signal) |

`/api/plsql/db/table-info` (`{"found":false}`) is the one place this is done *right* — an explicit
boolean the client must read, not an error string smuggled into a success.

### A-3. Catch blocks that convert an exception into 200-empty

Seven confirmed. These are the server-side twin of the client defect: no client fix can help,
because the client correctly sees a 200.

```java
// MiscQueryService.java:93-96 — GET /api/plsql/analysis/references/{objectName}
} catch (Exception e) {
    log.error("Failed to get references: {}", e.getMessage());
    return ResponseEntity.ok(Collections.emptyList());
}
```
A dead database renders as "this object is referenced by nothing."

```java
// DatabaseController.java:104-107 — GET /api/plsql/db/objects
} catch (Exception e) {
    log.error("listObjectsBySchema failed for {}: {}", schema, e.getMessage());
    return ResponseEntity.ok(new ArrayList<>());
}
```
A dead database looks exactly like an empty schema. The non-Oracle branch of the *same method*
(:90-92) correctly returns 502.

Also: `DatabaseController.java:244-248` (`/packages/{username}`, 200 `[]`);
`DocsController.java:54-57` (200 `[]`, exception not even logged);
`FlowAnalysisController.java:484-487` (200 `[]`, not logged — a corrupt jar reports "no environment
profiles"); `AnalyzerController.java:1697-1701` (an unreadable resource is rendered as a zero-byte
one); `AnalyzerController.java:1064-1070` (store failure degraded to a cache miss — benign, but
hides outages).

**Related, same defect class:** `ParserAnalysisController` wraps **14 endpoints** in
`catch (Exception e) { return ResponseEntity.notFound().build(); }` (:237, :248, :257, :273, :282,
:295, :304, :439, :463, :480, :491, :502), as does `TableDetailController:196-199`. Disk error,
malformed JSON and OOM are all reported to the client as "does not exist".

---

## B. Client side

### B-1. Counts

Of the **169** `!res.ok` sites, **81 throw** (correct) and **88 do not**. The 88 break down:

| Verdict | Count | Notes |
|---|---|---|
| **HARMFUL** — an error renders as data the user will believe | **17 functions** | 10 in `plsql/js/api.js`, 7 in `parser/js/api.js` |
| **BENIGN** — dropdown / config surface, user notices | **4 functions** | `listJobs`, `listProjects`, `listEnvironments`, `getDbUsers` (all plsql) |
| **DEAD** — no call site found; not classified | **10 functions** | 8 plsql (incl. `getParseErrors`), 2 parser |
| **Correct already** | 81 sites | all of `jar/`, all of `war/`, 17 of `parser/` |

Two client sites already implement the right contract and prove the team knows it:
`parser/js/api.js:203` (`facetSummary` — `if (res.status === 204) return null;` "204 means the
analysis has no such facet at all — a different answer from 'zero rows matched'") and
`parser/js/api.js:281` (`claudeVerificationSummary` — "Returns null for 204 and throws for a real
failure, so a caller can tell 'no data' from 'broken' — conflating the two is what hid a permanently
empty grid"). `shared/page-envelope.js:105-117` does the same for paged sources.

### B-2. The critical fact about the existing `try/catch` blocks

**Every `try/catch` and `.catch(() => [])` at these call sites is dead for HTTP-error cases.**
Because `api.js` *resolves successfully* with `[]`, the catch never runs. Only network-level and
JSON-parse exceptions reach it. Several screens have a correct, carefully written error state that
can never be displayed:

- `plsql/js/app.js:334` — a `"Could not load history"` state that never fires.
- `parser/js/perfSummary.js:399` — a `.catch` carrying the comment *"Distinguished from 'no errors
  recorded', which is a different fact"* and a `"Could not load messages"` state. Defeated one file
  away by `parser/js/api.js:61` returning `[]`.
- `plsql/js/triggerModal.js:52` — renders `Error: …`, unreachable for 500s.
- `parser/js/sourceView.js:402` — `catch(e) {}`, both empty *and* unreachable.
- `parser/js/export.js:148` — logs `'[Export] tables fetch failed'`, never runs.

`PA.toast(msg, 'error')` exists and works (`plsql/js/app.js:12`). **No** `!res.ok → []` path reaches
it. `plsql/js/config.js:287-344` also uses raw `alert()`.

### B-3. HARMFUL sites, with the false claim the user reads

| Function | Call site | What the screen says | What the user falsely concludes |
|---|---|---|---|
| `parser` `analysisErrors` | `parser/js/perfSummary.js:398` | **"No messages recorded"** | "This analysis logged no errors" — while looking at the error badge they just clicked |
| `parser` `getCallerTree` | `parser/js/sourceView.js:400, 1605` | **"None"** under Called By | "Nothing calls this procedure" — the exact fact the panel exists to answer |
| `plsql` `getTableOperations` | `plsql/js/tableOps.js:13`, `predicates.js:15`, `app.js:1479,1523`, `export.js:43,116` | **"No tables found"** / **"No data found"** | "This code touches no tables"; "this code has no WHERE predicates" |
| `parser` `getTableOperations` | `parser/js/leftPanel.js:240`, `tableOps.js:8`, `tableDetail.js:194`, `export.js:148` | **"No tables found"** | same; `tableDetail` silently loses its cross-reference |
| `plsql`/`parser` `getJoinOperations` | `plsql/js/joins.js:13`; `parser/js/joins.js:22`, `joinSummary.js:26,40`, `export.js:145` | **"No data found"** | "The analysed SQL contains no joins" |
| `plsql` `getProcedures` | `plsql/js/app.js:1472,1476,1516,1520,2931`, `export.js:42,115` | **"No procedures found"** | "This package has no procedures" |
| `plsql` `getTableTriggers` | `plsql/js/triggerModal.js:37` | **"Trigger not found in analysis data"** | Actively false — the modal was opened *from* a trigger row that exists |
| `plsql`/`parser` `getCursorOperations` | `plsql/js/cursors.js:13`; `parser/js/export.js:152` | **"No data found"** | "No cursors in this code" |
| `parser` `getSequenceOperations` | `parser/js/sequences.js:22`, `export.js:156` | **"No data found"** | "No sequences" — *more* believable when wrong, since real analyses hold 0–3 |
| `plsql`/`parser` `listHistory` | `plsql/js/app.js:320`; `parser/js/history.js:105` | **"No analyses found"** | "My prior analyses were deleted" |
| `plsql` `listClaudeSessions` / `listClaudeChunks` | `plsql/js/claude.js:889, 1010` | **"No Claude sessions found"** / **"No chunks found"** | Verification never ran |
| `plsql` `listClaudeChunkSummaries` | `plsql/js/summary-progress.js:65` | zero chunks complete | Borderline: siblings in the same `Promise.all` usually also blank on a total outage |

**Aggravating factor — the exports.** `plsql/js/export.js:43,116` and `parser/js/export.js:145-156`
feed these same functions into the JSON/XLSX writers. A failed fetch produces a silently truncated
artifact, and then `PA.toast('JSON exported: ' + filename, 'success')` (`plsql/js/export.js:180`) /
`'Excel exported: '` (`:277`) confirms it green. The user files that spreadsheet.

### B-4. `conversion/index.html` (inline, 7 fetch sites)

Not covered by any `api.js`. Three of seven check `.ok`; four call `.then(x => x.json())` with no
check. `_loadJobs()` (:403) is `catch(e) {}` — a 500 leaves the job list silently empty or stale.
`/report` (:741) uses an explicit `{found, error}` contract and renders the error — correct.
`_loadInstruction` (:586) correctly distinguishes 204. Lower severity than plsql/parser: the user is
watching a live job and will retry.

---

## C. Highest-value subset — ranked

Ranked by *wrongness of the decision the user makes*, not by frequency.

**1. `GET /api/plsql/analysis/errors` → 200 `[]` when no analysis exists.**
`MiscQueryService.java:40-42`:
```java
AnalysisResult result = analysisService.getLatestResult();
if (result == null || result.getUnits() == null) return ResponseEntity.ok(Collections.emptyList());
```
`plsql/js/app.js:467-471` (`PA.showErrors`) fetches this directly — it does not use
`PA.api.getParseErrors()`, and it never checks `res.ok`:
```js
const errors = await res.json();
if (!errors || !errors.length) { PA.toast('No parse errors', 'success'); return; }
```
The user gets a **green success toast asserting a fact about their code**, produced by a state where
the server has nothing to say. Live-verified: `GET /api/plsql/analysis/errors` → `200 []`.
This is a *server* defect; fixing the client cannot help. **Worst single defect found.**
(Mitigation, for precision: `app.js:506` does have a working `catch` that toasts
`'Failed to load errors'`, so a 404 with an empty body *is* reported. The 200-`[]` path bypasses it.)

**2. `parser` `analysisErrors` → "No messages recorded" (`perfSummary.js:398`).**
This panel opens *only* when `hasErr` is true (`perfSummary.js:393`). It therefore contradicts the
error badge the user clicked one second earlier, and it defeats a distinction the author explicitly
wrote a comment to preserve.

**3. `parser` `getCallerTree` → "Called By: None" (`sourceView.js:400, 1605`).**
"Nothing calls this procedure" is the single most consequential false claim this tool can make — it
is the answer people use to decide something is dead code and delete it.

**4. `getTableOperations` (both apps) → "No tables found", and a green-confirmed truncated export.**
Six plsql + four parser call sites. "This procedure touches no tables" is the product's core claim.
The export path turns a transient 500 into a durable artifact the user forwards to someone else.

**5. Server-side catch-to-200-empty on live DB queries.**
`MiscQueryService.java:93-96` (`/references/{obj}`) and `DatabaseController.java:104-107`
(`/db/objects`). A database outage is rendered as "this object is referenced by nothing" and "this
schema is empty". Again unfixable from the client — it is a genuine 200.

Runner-up: `getTableTriggers` → `"Trigger not found in analysis data"` (`triggerModal.js:37`), which
is worse than a blank because it makes a confident negative assertion about a row the user is
looking at.

---

## D. Recommended pattern

### Constraint

72 call sites in two files; ~40 rendering screens; the failure must become **visible**, and no screen
may be rewritten. Anything that changes the *type* returned (throwing, or returning a sentinel object
instead of an array) forces every consumer to change or crash. Anything that only logs is invisible.

### The pattern: a frozen empty array as a sentinel, plus one global banner

`shared/page-envelope.js` is already loaded first on **all five** entry points
(`index.html`, `jar/`, `parser/`, `plsql/`, `war/` — verified). It is the only file that needs new
logic. Add ~30 lines:

```js
/** The empty array returned when a request FAILED, as opposed to genuinely returning no rows.
 *  It IS an array, so every existing render path keeps working unchanged; it is frozen and
 *  identity-comparable, so a screen that wants to say something better can ask. */
PA.FAILED = Object.freeze([]);

PA.failed = function (x) { return x === PA.FAILED; };

var _lastFailAt = 0;
/** Records a failed request, shows one throttled banner, and returns the sentinel. */
PA.apiFail = function (res, url) {
    console.warn('[api] ' + res.status + ' ' + (url || res.url));
    if (Date.now() - _lastFailAt > 4000) {          // one banner per burst, not one per request
        _lastFailAt = Date.now();
        (window.PA && PA.toast ? PA.toast : function (m) { console.error(m); })(
            'Request failed (HTTP ' + res.status + '). Data on screen may be incomplete.', 'error');
    }
    return PA.FAILED;
};
```

Then a mechanical, reviewable substitution in the two offending files:

```js
if (!res.ok) return [];      →   if (!res.ok) return PA.apiFail(res, url);
if (!res.ok) return null;    →   if (!res.ok) { PA.apiFail(res, url); return null; }
```

### Why this and not the alternatives

- **Throwing** (the `jar/`/`war/` pattern) is the *correct* contract, but here it would need a
  `catch` added to ~40 screens, and any screen missed goes from "wrong data" to "blank page and a
  console error" — a regression while the migration is half done. It remains the right end state
  for new code; it is not the right first move for 72 existing sites.
- **A sentinel object** (`{__failed: true}`) breaks `.length`, `.map`, `.filter` at every call site.
- **Logging only** does not satisfy "make the failure visible to the user".

`PA.FAILED` is the smallest change with the property that matters: **nothing breaks, and the failure
becomes visible immediately at all 72 sites at once.**

### Then upgrade the ranked screens, one line each

With `PA.failed()` available, the C-list screens improve their empty state individually and
independently:

```js
// parser/js/sourceView.js:427
list.innerHTML = PA.failed(callers) ? 'Could not load callers — retry'
               : callers.length     ? render(callers)
               : 'None';
```

Ten such edits cover every item in section C.

### Files changed and size

| File | Change | ~Lines |
|---|---|---|
| `static/shared/page-envelope.js` | add `PA.FAILED` / `PA.failed` / `PA.apiFail` | +30 |
| `static/plsql/js/api.js` | 54 one-line substitutions | 54 |
| `static/parser/js/api.js` | 21 one-line substitutions | 21 |
| 10 ranked screens (§C) | one-line empty-state upgrade each | 10 |
| **Client total** | | **~115, no screen rewritten** |

### The server-side half — 5 lines, and the highest value per line in this audit

The client fix cannot touch items C-1 and C-5, because those are honest 200s. Five one-line changes:

| File:line | Change |
|---|---|
| `detail/MiscQueryService.java:42` | `ok(emptyList())` → `notFound().build()` (no analysis ≠ no parse errors) |
| `detail/MiscQueryService.java:95` | `ok(emptyList())` → `status(502).body(…)` in the catch |
| `DatabaseController.java:106` | `ok(new ArrayList<>())` → `status(502)`, matching the sibling branch at :91 |
| `DatabaseController.java:247` | same |
| `detail/TableQueryService.java:50,56,62` | `ok(emptyList())` → `notFound().build()`, matching `:27` in the same class |

Do these **first**. They cost five lines, they remove the worst defect in the report, and every one
of them makes a controller self-consistent rather than introducing a new convention.

Two follow-ups worth scheduling but out of scope for a minimal change: a single
`@RestControllerAdvice` (there is currently none — §A-0), and replacing the 14 blanket
`catch (Exception e) → notFound()` handlers in `ParserAnalysisController`, which currently report
disk and parse failures as "does not exist".
