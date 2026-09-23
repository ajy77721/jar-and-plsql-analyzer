# No-op guards — checks that detect a problem but do not prevent it

Date: 2026-09-04. Audit only; no code was changed.

Scope: Java, JavaScript, shell, `.bat`, and Maven/XML across all modules. Paths containing
`_migration-backup`, `target/` and `node_modules` were excluded.

The defect class: **the check runs, the check is correct, the check reports — and then execution
continues into exactly the thing the check was protecting against.** An `echo`, a `log.warn`, a
discarded boolean, and an assertion that never executes are all the same bug.

Every finding below was confirmed by reading the surrounding code. The
"[Looks wrong, is fine](#looks-wrong-but-is-fine)" list at the end is part of the deliverable.

---

## HIGH — wrong data or corruption reaches the user

### H1. The fat-jar rebuild guard exists, is correct, and is wired into nothing

Three files, one defect.

`scripts/safe-build.sh:1-34` is a textbook guard. It pgreps for a running packaged instance and
`exit 1`s. Its own header comment explains why it exists:

```sh
# This has happened twice in this repo. The second time there WAS a check, but it only echoed a
# warning and let the build proceed on the next line. A guard that does not exit is not a guard,
# so this one exits non-zero and does nothing else.
```

**It is referenced by nothing.** A repo-wide grep for `safe-build` across `*.sh`, `*.bat`, `*.xml`,
`*.md` and `*.json` returns zero hits outside the file itself.

Meanwhile the two entry points that actually rebuild have no check at all:

- `setup.sh:86-98` — runs `mvn clean install -DskipTests` at line 89. The `pkill -f
  'unified-web-1.0.0-SNAPSHOT.jar'` is at **line 103**, fourteen lines *after* the build. The jar is
  overwritten while the old JVM still has it open, and only then is that JVM killed.
- `start-mvn-build.bat:117` — `call mvn clean package -DskipTests` with no instance/port check
  anywhere in the file (`grep -i 'taskkill|netstat|wmic'` → nothing).

And the fix the incident report prescribed was never implemented either.
`docs/audit/LOGBACK-NOCLASSDEF.md` closes with:

> If a guard is wanted anyway, the right place is `start.sh` / `start.bat` (kill any existing
> instance before launching), not the Maven build.

`start.sh:29-55` checks only that the jar exists, then `exec java`. `start.bat` likewise. Neither
kills a prior instance.

**What it fails to prevent:** replacing a Spring Boot fat jar under a live JVM. Per
`LOGBACK-NOCLASSDEF.md`, Boot resolves nested classes lazily, so every not-yet-loaded class becomes
unresolvable — *including the logback classes on the error-reporting path*, so the process then
fails silently in every other way too.

**Scenario:** developer runs `./setup.sh --skip-db` against a running app. Line 89 corrupts the live
process; line 103 kills it 14 lines later, masking the damage. Or on Windows,
`start-mvn-build.bat` with the app running in another console: the running app is not killed at
all, and it degrades into unlogged `NoClassDefFoundError`s. This has already happened twice.

**Severity: HIGH.** Documented, recurred twice, remediation written three separate times and wired
in zero times.

---

### H2. `setup-databases.sh` reports every seed failure and then exits 0

`db-setup/setup-databases.sh:22` — `err()` prints to stderr and returns 0. It is then used as the
*terminal statement* of five seed functions and as the `||` handler in `main`:

```sh
# :145, :184, :228, :280, :326 — last line of each setup_X()
[[ $pg_ok -eq 1 ]] || err "PostgreSQL seed created no routines (check $SEED_DIR/postgres.sql)."
}

# :367-372
oracle)    setup_oracle    || err "Oracle setup failed." ;;
mysql)     setup_mysql     || err "MySQL setup failed." ;;
...
```

Because `err` succeeds, `[[ ... ]] || err ...` succeeds, so the function's exit status is 0. Even a
genuine `return 1` from earlier in the function is neutralised by `|| err` in `main`. `set -e` at
line 13 never trips. The loop continues to the next database and `main` returns 0.

`print_summary()` then unconditionally prints a **hardcoded static table** of five containers and
ends with:

```sh
ok "All requested databases are up and seeded. Point the analyzer's demo projects at the rows above."
```

That claim is printed whether zero or five databases seeded.

Downstream, `setup.sh:61`:
```sh
bash "$SCRIPT_DIR/db-setup/setup-databases.sh" || { err "database setup failed"; exit 1; }
```
never fires, because the child always exits 0.

**Scenario:** Oracle's container comes up but `oracle.sql` fails after all 6 retries. `[FAIL]` scrolls
past, the green summary claims success, `setup.sh` proceeds to build, start, and analyse; step 6
reports `OracleDemo  0 nodes` and `setup.sh:157` still prints `Analyzer ready ... (5 datasources
configured + analyzed)` and exits 0. The user opens the UI and sees an empty analysis with no
indication anything failed.

**Severity: HIGH.**

Related, same file/flow, lower:
- `setup.sh:47` — `*) warn "unknown flag: $a" ;;` then proceeds with the full destructive default
  (recreate + reseed all 5 DBs). A typo'd `--skip-db` is a warning, not a stop. MEDIUM.
- `setup.sh:77,80` — `warn "... cross-schema Oracle demo will be partial"`, then analysis runs and
  the partial result is presented as the result. MEDIUM.
- `setup.sh:153-157` — prints per-datasource node counts (possibly all `0`) and then unconditionally
  declares success. MEDIUM.

---

### H3. Claude CLI failure is logged, its partial stdout is used, and the endpoint is checkpointed complete

Three no-op guards compounding into permanent data loss.

**(a)** `jar-analyzer-core/src/main/java/com/jaranalyzer/service/ClaudeProcessRunner.java:223`
```java
if (exitCode != 0) {
    String stderr = stderrBuf.toString().trim();
    log.warn("Claude CLI exited with code {} | stderr: {}", exitCode,
            stderr.isEmpty() ? "(empty)" : stderr.substring(0, Math.min(500, stderr.length())));
}

String result = stdoutBuf.toString();
return result;
```
No return, no throw. A crashed / rate-limited / killed CLI returns its truncated stdout as a normal
result.

**(b)** `jar-analyzer-core/.../ClaudeResultMerger.java:35` returns `boolean`, and explicitly returns
`false` on "no JSON found in Claude output". All four call sites discard it:
`ClaudeAnalysisService.java:237`, `:349`, `:504`, `:637`, e.g.
```java
if (result != null && !result.isBlank()) {
    fragmentStore.writeFragment(workDir, fragmentName + "_output.json", result);
    resultMerger.mergeClaudeResult(ep, result);      // boolean dropped
}
```

**(c)** Immediately after, the endpoint is marked done and persisted to the resume checkpoint
(`tracker.markEndpointComplete(...)`, consumed by `completedKeys` at `ClaudeAnalysisService.java:398`).

**Scenario:** Claude hits a rate limit mid-endpoint and exits 1 after emitting half a JSON object.
(a) hands the fragment back as a result; (b) fails to merge and says so to nobody; (c) records the
endpoint COMPLETE. Re-running the analysis **skips that endpoint forever**. The user sees a
"complete" enriched analysis in which that endpoint silently has no Claude data.

**Severity: HIGH** — silent, persistent, and self-concealing on retry.

Related: `ClaudeResultMerger.java:57` computes a `missing` field list, logs it, and proceeds to
overwrite static-analysis fields anyway (MEDIUM). Two sibling runners
(`unified-web/.../parser/service/ClaudeProcessRunner.java:124`,
`unified-web/.../web/service/ClaudeProcessRunner.java:156`) return `null` only when stdout is
*empty* — a nonzero exit **with** partial output still passes through (MEDIUM).

---

### H4. PL/SQL metadata resolution fails, defaults are persisted as if they were resolved

`plsql-analyzer-core/src/main/java/com/plsqlanalyzer/analyzer/service/AnalysisService.java:310-333`
```java
Map<String, String> ownerMap = Collections.emptyMap();
Map<String, String> typeMap  = Collections.emptyMap();
try { ownerMap = ownerFuture.get(60, TimeUnit.SECONDS); }
catch (Exception e) {
    log.warn("Parallel owner resolve failed, retrying on main: {}", e.getMessage());
    try { ownerMap = dictionaryService.bulkResolveTableOwners(conn, tableNamesToResolve, allSchemas); }
    catch (Exception e2) { log.error("Bulk owner resolve failed (retry): {}", e2.getMessage()); }
}
... same for typeMap ...

for (TableOperationSummary summary : tableOps.values()) {
    if (summary.getSchemaName() == null) {
        summary.setSchemaName(ownerMap.get(summary.getTableName().toUpperCase()));   // null
    }
    summary.setExternal(!entrySchema.equalsIgnoreCase(summary.getSchemaName()));      // -> true
    String type = typeMap.get(summary.getTableName().toUpperCase());
    summary.setTableType(type != null ? type : "TABLE");                              // -> "TABLE"
}
```
The retry is a real mitigation; the second failure is not mitigated at all. The loop runs
unconditionally against the empty maps.

**Scenario:** a transient dictionary timeout (`ORA-01013`) during a large analysis. Every VIEW in the
persisted report becomes `type=TABLE`, and every table becomes `external=true` because
`entrySchema.equalsIgnoreCase(null)` is false. The user's dependency report asserts cross-schema
access that does not exist, and a migration is planned against it. Nothing in the artifact records
that resolution failed.

Same shape, same file, sequential branch: `:346`, `:353`.

Same shape, no retry at all:
- `AnalysisService.java:479-505` (8 catches) — `allColumns/allConstraints/allIndexes/allViewDefs`
  stay empty, `TableMetadata` is assembled from `getOrDefault(key, List.of())`. A table with an
  unread PK/FK set renders identically to a table that genuinely has none.
- `FastAnalysisService.java:396` — the catch wraps the whole owner+type block *including*
  `applyTableInfo`, so no table gets an owner or type. No retry.
- `FastAnalysisService.java:531` — assembly loop is *inside* the try, so some tables get metadata
  and the rest silently get none.

**Severity: HIGH** for the cluster.

---

### H5. `Future.get(timeout)` treated as a completion signal

`plsql-analyzer-core/.../FastAnalysisService.java:320` and `:461`:
```java
try { f.get(30, TimeUnit.SECONDS); } catch (Exception e) { /* skip */ }
```
`TimeoutException` does **not** cancel the task, and `pool.shutdown()` is non-blocking. The method
returns `units` while parse workers are still writing to it.

**Scenario:** a large package body takes 35s to parse. Its unit is silently absent from the result,
every call edge into it disappears from the call graph, and the list itself was read under a data
race. Same shape at `AnalysisService.java:166` and `:237`, which then log an "N sources found" count
as if it were complete (MEDIUM there — per-object inner catches at `:160`/`:197` do log).

**Severity: HIGH.**

---

### H6. Truncated PL/SQL source is detected, kept, and parsed as complete

`plsql-parser/src/main/java/com/plsql/parser/flow/SourceDownloader.java:564`
```java
if (owner != null && looksLikeTruncated(source)) {
    String fullSource = tryDbmsMetadata(conn, owner, objectName, objectType);
    if (fullSource != null && (!looksLikeTruncated(fullSource) || fullSource.length() > source.length())) {
        ... source = fullSource;
    } else if (fullSource != null) {
        log.warn("[SourceDownloader] DBMS_METADATA also appears truncated for {} {} ...");
    }
}
...
return source;      // the truncated body
```
Both retrieval paths have failed and the code says so — then returns the fragment to
`PlSqlParserEngine` / `DependencyCrawler` as if it were the object. Note the `fullSource == null`
branch (DBMS_METADATA returned nothing) has no log line at all.

**Scenario:** a large package body clipped by `ALL_SOURCE`. ANTLR happily parses the fragment; the
tail — its INSERTs, UPDATEs and downstream calls — is simply absent. `tables/index.json` and the
call graph under-report, and the UI presents that as the analysis.

**Severity: HIGH.**

---

### H7. Persisted run counters silently become zero

`unified-web/src/main/java/com/jaranalyzer/controller/FlowAnalysisController.java:402`
```java
int high = 0, med = 0, low = 0, datasets = 0;
try {
    ... tally loop over the whole report ...
} catch (Exception ignored) {}

meta.highCount    = high;
meta.medCount     = med;
meta.lowCount     = low;
meta.datasetCount = datasets;
```
The catch wraps the entire loop, so a `ClassCastException` on one malformed entry discards every
count tallied so far — and the zeros are then **written to `status.json`** as the run's result.

**Scenario:** a run finds 9 HIGH findings; the 3rd result has `optimizations` as an object rather
than a list. The history row reads `0 HIGH / 0 MEDIUM / 0 LOW`. The user closes the run as clean.

**Severity: HIGH.**

---

### H8. Trigger resolution truncates on first error, with no marker

`plsql-analyzer-core/.../TriggerResolver.java:82` (and `:155`) — the catch wraps the **whole
per-trigger loop**:
```java
for (TriggerRecord trigger : triggers) {
    ... detail.setCalledProcedures(calledProcs);
    summary.getTriggers().add(detail);
    }
} catch (Exception e) {
    log.warn("Failed to resolve triggers for table {}: {}", tableName, e.getMessage());
}
```
**Scenario:** `ORDERS` has 5 triggers; the 2nd has a null `triggeringEvent` causing an NPE. The
report says the table has 1 trigger. Three cascading procedures are omitted from the migration plan
with no "partial" flag on the summary. `:108`/`:178` do the same at `log.debug` level for a
trigger's called-procedure list.

**Severity: HIGH.**

---

### H9. `TraceReadabilityGateTest` has a blind spot, and the defect it guards is present today

`unified-web/src/test/java/com/analyzer/arch/TraceReadabilityGateTest.java:44`
```java
private static final List<String> RENDERERS =
    List.of("parser/js/callGraph.js", "plsql/js/callGraph.js");
```
There is a **third** trace renderer, and it is the one that violates the rule.

`unified-web/src/main/resources/static/jar/js/callGraph.js:369`:
```js
const indent = op.depth * 20;
```
Unbounded — the exact defect. It also has **zero** `ct-depth-badge` occurrences (the parser version
has one), so the mitigation the gate calls "what makes the cap safe" is absent too. The file is
live: `static/jar/index.html:194` loads it. `_MAX_INDENT_LEVELS: 12` exists only in
`parser/js/callGraph.js:146`.

Secondarily the gate's check is an exact string match —
`assertFalse(src.contains("const indent = depth * 20;"))` — so `op.depth * 20` (what `jar/` uses),
different spacing, or a renamed variable all evade it.

**Scenario:** a deep call tree on the JAR summary page indents right off the viewport. The gate that
exists to prevent this is green.

**Severity: HIGH** — the only gate blind spot in the suite with a confirmed live violation behind it.

---

### H10. Flow-poc: failed Mongo call recorded as an observed result; unparseable filter becomes "match everything"

- `flow-poc/.../layer2/UniversalMongoInterceptor.java:117, 222, 262` —
  `try { result = superCall.call(); } catch (Exception ignored) {}`. `result` stays null/stale and
  the shadow-comparison layer records that as the observed behaviour.
- `flow-poc/.../engine/MongoFilterParser.java:29`,
  `flow-poc/.../engine/AggregationPipelineParser.java:41`,
  `flow-poc/.../layer2/ShadowAggregationExecutor.java:52, 408` — malformed JSON becomes an **empty
  filter** or an **empty pipeline**. An empty filter matches everything.

**Scenario:** a query with a typo'd `$match` silently becomes an unfiltered full-collection read
whose results are then reported as "matching".

**Severity: HIGH within flow-poc** — scope by how production-facing that module is.

---

### H11. The UI converts every HTTP error into an empty result and renders it as the truth

This is the single most widespread instance of the defect class in the repo.

**(a) The two parser/PL-SQL API layers swallow status codes wholesale.**

`unified-web/src/main/resources/static/plsql/js/api.js` — **51** occurrences of
`if (!res.ok) return [];` / `return null;`, e.g. `:161`, `:165`, `:180`, `:184`, `:193`, `:197`,
`:206`, `:210`, `:217`:
```js
async getTableOperations() {
    ...
    const res = await fetch('/api/plsql/analysis/tables');
    if (!res.ok) return [];
    return res.json();
},
```
`static/parser/js/api.js` — 21 more (`:18, 61, 86, 100, 148, 156, 164` → `[]`;
`:37, 54, 79, 93, 130, 137` → `null`).

The consumers cannot tell the difference. `parser/js/tableOps.js:8`:
```js
async load() {
    PA.tableOps.data = await PA.api.getTableOperations();   // [] on HTTP 500
    PA.tableOps._initOpPills();
    PA.tableOps._initTable();       // renders headers + zero rows
    PA.tableOps.applyScope();
}
```
Same at `parser/js/joins.js:22`, `plsql/js/app.js:1479` and `:1523` (`PA.loadSidebarData` →
`PA.renderTableList([])` → empty sidebar section).

**Scenario:** the analysis JSON on disk is corrupt so `/api/plsql/analysis/tables` 500s. The Tables
tab paints its header row and nothing else — visually identical to a procedure that performs no
DML. **`getParseErrors()` returning `[]` on error is the worst of the set: the user concludes the
parse was clean.**

**(b) Four `r.ok ? r.json() : []` sites in `static/jar/js/cross-ref.js`.**
`:354-372` (`_loadAvailableProcs`) also sets `this._procsLoaded = true` on both the error branch and
the `.catch(() => {})`, so **it never retries**: the procedure picker shows `(0)` forever and the
user concludes there is nothing to link. `:571-580` (`_loadSavedList`) renders "no saved links" on a
500 — the `.catch` does show "Failed to load.", but only for network/parse errors, so an HTTP error
looks like data loss. `:108-123` (`_loadAnalyzedPaths`) silently drops the "analyzed" tags, so the
user re-runs already-completed analyses.

**(c) `await fetch(...)` with no `res.ok` check before `.json()`.**
`plsql/js/api.js:19` (`analyze`) and `:24` (`getLatestResult`):
```js
const res = await fetch(endpoint, { method:'POST', ... });
return res.json();          // no res.ok
```
A 500 returning Spring's HTML error page throws `SyntaxError: Unexpected token '<'`, surfacing as
`PA.toast('Failed: Unexpected token <...')`. Same at `parser/js/api.js:68-73` (`runAnalysis`),
`jar/js/flow-analysis.js:188` , `:443`, `:651`, and `jar/js/targeted.js:226`.

`jar/js/flow-analysis.js:976-982` is the worst variant — it downloads without checking:
```js
const resp = await fetch(`/api/jar/jars/${...}/flow/report/${runId}`);
const text = await resp.text();
const blob = new Blob([text], { type: 'text/markdown' });
```
A 500 saves the **HTML error page** to disk as `<jar>-flow-analysis.md`.

**Severity: HIGH.** Highest-leverage fix in the UI: make the two `api.js` layers throw on `!res.ok`
and give the ~10 `load()` call sites a `catch → error banner in the panel`. That closes (a) and much
of (c) at once.

---

### H12. Call-trace drill-down fails silently; the breadcrumb then lies

`static/plsql/js/callGraph.js:60-68`
```js
async drillDown(procId) {
    const isCurrent = UIBusy.seq(PA.callTrace, '_gen');
    const data = await PA.api.getCallTree(procId);
    if (!isCurrent()) return;
    if (!data || !data.id) return;      // getCallTree returns null on !res.ok
    ...
}
```
The user clicks a child procedure and **nothing happens at all** — no spinner change, no toast, no
console line; the previous procedure's tree stays on screen. Twin at
`static/parser/js/callGraph.js:102-110`.

`static/plsql/js/callGraph.js:76-85` is worse:
```js
if (!isCurrent()) return;
PA.callTrace.breadcrumbStack = PA.callTrace.breadcrumbStack.slice(0, index + 1);
if (data) {
    PA.callTrace.treeData = data;
    PA.callTrace.render(data);
}
```
The stale-response guard here is correct and carries a comment explaining a previous fix. The
remaining bug is different: the truncation sits **outside** the `if (data)` block. When `data` is
null the breadcrumb shortens to "A > B" while the tree below it still renders the deep node D. The
parser twin at `parser/js/callGraph.js:120` does it correctly — the `slice()` is *inside*
`if (data)`. A one-sided fix of a duplicated pair.

**Scenario:** `/call-tree/{proc}` 404s for a proc whose node file was never written. The user clicks
a breadcrumb, the trail visibly shortens, and they read the tree still on screen as belonging to the
node the breadcrumb now names.

**Severity: HIGH** — wrong data presented as truth. `parser/js/callGraphViz.js:20-36` is the correct
template: it renders "No call graph data available" and "Failed to load call graph" as *distinct*
states.

---

## MEDIUM — silent degradation

### M0. UI features that silently vanish on error

| Location | Code | What the user concludes |
|---|---|---|
| `static/jar/js/codeTree.js:113-122`, `:997-1001`, `:1022-1031` | `if (!res.ok) return;` … `catch (e) { console.warn(...) }` | The Code Tree has no "Resources" / "Bundled JARs" / "Dependency Map" section at all — the JAR appears to have no `application.yml` and no bundled jars. `console.warn` is not user-visible. |
| `static/jar/js/summary-tables.js:702-766` | `.catch(() => {})` on `getCatalog` | Every collection stuck on "Unverified" — and *not even* the "No MongoDB catalog — connect DB" hint, because that hint only renders on the success path. |
| `static/parser/js/claudeInsights.js:73`, `:161` | `try { result = await PA.api.claudeResult(name); } catch (e) {}` then "No Claude verification data. Run a scan…" | The user pays for a second full Claude scan that had already completed. The poll catch at `:161` also freezes the progress bar. |
| `static/parser/js/home.js:21-33` | `_projects = res.ok ? await res.json() : []` then `var legacy = _projects.length === 0` | The home form silently drops into "legacy" mode with no project/env selectors — **the user may run the analysis against the wrong database.** |
| `static/jar/js/dashboard.js:74-125` | `catch (e) { wrap.innerHTML = ''; }` | The "Loading Claude stats…" line vanishes and the usage strip disappears — looks like the feature is switched off. |
| `static/jar/js/flow-analysis.js:188-194` | no `resp.ok`, `catch → _renderConnSection` | The environment-profile dropdown is gone, replaced by a manual URI box the user can typo. |
| `static/parser/js/tableDetail.js:194`, `:213` | `catch(e) { tableIndex = []; }` / `catch(e) {}` | The table-detail modal opens with empty Operations / Columns / Usage tabs. |
| `static/shared/access-details.js:218-227` | `catch (e) { _markUnknown(id, details); return; }` | Every row's columns become `null` and render blank — indistinguishable from "the parser could not determine columns". |
| `static/parser/js/export.js:145-160` | three `catch(e){ console.warn(...); result.X = [] }` | **An export file downloads successfully with the Joins/Tables sheets empty.** No banner, no partial-export warning — and unlike the on-screen cases this artifact leaves the tool and gets shared. |
| `static/jar/js/summary-explorer.js:26-30` | `catch (e) { /* fall through */ }` then `toast.warn('No call tree available…')` | LOW — there *is* a toast, but its text asserts absence when the cause was a failed request. |

### M1. `PlsqlEntityMapperTest.mapsTheWholeCorpus` never runs

`analyzer-storage/src/test/java/com/analyzer/storage/entity/PlsqlEntityMapperTest.java:204,214`
```java
assumeTrue(root != null, "no data/plsql-parse on this machine…");
assumeTrue(!files.isEmpty(), …);
```
`data/plsql-parse` **does not exist** and `data/` is gitignored (`.gitignore:27`). The test carries
seven real invariants (`assertNotNull(n.nodeId())`, `assertFalse(u.tableKey().contains("null"))`,
case-normalisation of `tableKey`, `assertEquals(files.size(), nodes)`) and its own Javadoc says:

> This is the test that would have caught each of the six hazards PLAN-11 found, because it is the
> only one that sees data nobody wrote for it.

None of those assertions ever execute. Same pattern, same cause:
- `MongoPlsqlEntityIndexTest.java:66,68` — `assumeTrue` in `@BeforeEach`, skips the **whole class**.
- `SummaryStatsTest.java:259` — `assumeTrue(!docs.isEmpty(), "no analyses in data/jar…")`.
- `RealSourceCoverageTest.java:44` — skips, *and* has no assertion even when it runs (see M2).
- `CallTreeFlattenRoundTripTest.java:235` — guards on
  `../data_old_1784577715/jar/web-1.0.0-SNAPSHOT/analysis.json`. That timestamped one-off backup
  directory happens to exist on this machine, so the test runs *here* and nowhere else, ever.

**Severity: MEDIUM** (HIGH in intent — these are the repo's only real-data tests).

### M2. Five parser tests print their findings instead of asserting them

- `db2-parser/.../Db2RoutinesHarnessTest.java:23`
- `mysql-parser/.../MySqlRoutinesHarnessTest.java:23`
- `postgres-parser/.../PostgresRoutinesHarnessTest.java:23`
- `sqlserver-parser/.../FlowsParseHarnessTest.java:28`
- `plsql-parser/.../service/RealSourceCoverageTest.java:43`

Each parses a whole corpus, computes `withErrors` / `coverage`, and only `System.out.printf`s it.
They also early-`return` if the fixture is missing:
```java
if (in == null) { System.out.println("db2-routines.txt not found"); return; }
```
A grammar regression that makes **every** routine in a dialect fail to parse is green. These are the
only whole-corpus tests those four dialects have, and `FlowsParseHarnessTest` already runs a full
`CoverageCheck` cross-check — one `assertEquals(0, withErrors)` would convert ~130 lines of existing
analysis into a real gate. Highest value-per-line fix in the repo.

### M3. `MongoTaggingTest.c24_gridFsStore` — `assertTrue(true)`

`jar-analyzer-core/src/test/java/com/jaranalyzer/service/MongoTaggingTest.java:479-500`
```java
if (!results.isEmpty()) {
    assertTrue(results.stream().anyMatch(dc -> "GRIDFS".equals(dc.source())), ...);
} else {
    assertTrue(true, "GridFS store with filename arg correctly produces no collection detection");
}
```
Both branches pass by construction. If `MongoMethodDetector` stops detecting GridFS entirely, the
test takes the `else` branch and is green. The only literal-true assertion in the tree.

### M4. Assertions inside conditionals that can be false

- `plsql-parser/.../edgecase/AliasAndScopeGapTest.java:51` — `if (sub != null) { assertTrue(foundEmp, ...) }`.
  `sub == null` is *exactly* the failure the test exists to catch (the SQL uses non-Oracle
  `FROM employees AS emp`). Needs `assertNotNull(sub)` first.
- `plsql-parser/.../TriggerDiscovererErrorTest.java:124` — `for (Map op : ops) assertNotEquals("DUAL", ...)`.
  In the expected-pass case `ops` is **empty**, so the loop body never runs. Vacuous today, not just
  at risk.

### M5. Tests that swallow their own failure

`plsql-parser/.../service/SqlParserCapabilityTest.java:63` (`selectIntoVariable`) and `:86` (`ddl`) —
each is entirely `try { parse; println("ok") } catch (Exception e) { println("NOT parseable") }`. A
JSqlParser upgrade that loses `SELECT ... INTO` support is a stdout line nobody reads. The sibling
methods in the same class assert properly.

### M6. Eleven test classes excluded from compilation and from surefire

`jar-analyzer-core/pom.xml:61-104` — `MessagingTaggingTest`, `SecurityTaggingTest`,
`CallTreeVisibilityTest`, `DispatchResolutionEdgeCaseTest`, `EndpointTaggingTest`,
`ExcelExportServiceTest`, `TransactionBoundaryTest`, `AsyncBoundaryTest`,
`CollectionAggregationEdgeCaseTest`, `FalsePositiveDetectionTest`, `HttpClientTaggingTest` are in
both `<testExcludes>` and surefire `<excludes>`.

This is **honestly documented** — the comment explains `MessagingTaggingTest` targets a method absent
from main, and that the rest are "stale against signatures that moved". It is listed here because
the features are live (`ExcelExportService.java` exists; endpoint/security tagging exist) and the
files still read as coverage on disk. Not a hidden defect; an acknowledged and unclosed gap.

### M7. `@Disabled` parking a known live NPE

`jar-analyzer-core/.../DispatchResolutionEdgeCaseTest.java:755` — `nullOwnerClassNoNpe` has a real
body and is disabled with a named unfixed bug: *"AggregationDetector.detect() does not guard against
null ownerClass (NPE at AggregationDetector.java:212)"*, plus the one-line fix. The test documents
the bug instead of failing on it.

(Also: `:738` and `CallTreeVisibilityTest.java:727` are `@Disabled` methods with **empty bodies** —
comments wearing a `@Test` annotation. LOW.)

### M8. Two gate tests return green when their scan root is missing

- `SearchQueryTrimGateTest.java:50,150-152` — `Path.of("src/main/resources/static")` is relative to
  CWD; `scan()` opens with `if (!Files.isDirectory(root)) return out;` and `everyNeedleIsTrimmed()`
  then does `if (!violations.isEmpty()) fail(...)`. **Run from the reactor root instead of
  `unified-web/`, it passes having scanned zero files.**
- `RenamedResponseFieldGateTest.java:75` — `if (!Files.isDirectory(STATIC_ROOT)) return;   // module
  layout changed; nothing to check`.

The suite disagrees with itself on this: `StaleGuardAndSharedScriptGateTest.staticRoot():328`
deliberately **throws** in the same situation, and `RenamedResponseFieldGateTest`'s own neighbour
calls a silently-skipped gate "worse than none".

### M9. Gate-test blind spots (no live violation today, but the gate would not see one)

| Gate | Blind spot |
|---|---|
| `PollingConfigGateTest.java:51` | `POLLING_DIRS = List.of("jar","parser","plsql","conversion","shared")` — **`war/` and loose files under `static/` are never scanned.** `static/index.html:658` has a `setInterval`, and `:1025` a **recursive `setTimeout` poller** — both invisible. The regex `",\\s*(\\d{3,})\\s*$"` also requires the literal to be the last argument, so `setInterval(fn, 3000, arg)`, `\|\| 30000`, `5e3` and `60*1000` all evade. `poll-exempt` is an unaudited opt-out. |
| `ApiPathCoverageTest.java:39` | `['\"`](/api/[a-zA-Z0-9/_.$-]*)` captures only up to the first string break. Every real call is concatenated (`'/api/jar/jars/' + id + '/summary/endpoints'`), so it validates roughly the **first two path segments** and nothing after a path variable. A typo in `/summary/endpoints` is a 404 the gate cannot see. |
| `SearchQueryTrimGateTest.java:67,155` | `.js` only — inline `<script>` in the 61 KB `static/index.html` is unscanned. Receiver whitelist is `[Ii]nput\|[Ee]l\|[Ff]ield\|[Bb]ox\|[Ss]earch\|[Ff]ilter`, so `q`, `txt`, `node` evade. Only matches `.value` immediately followed by case-folding — `const q = el.value; ... q.toLowerCase()` (the most natural spelling) evades. |
| `AccessDetailsPanelGateTest.java:46,58,98` | Raw `html.indexOf`, **comment-blind** (unlike `StaleGuard...:276` which strips `<!-- -->`). Commenting out the `access-details.js` tag keeps it green while the page throws `ReferenceError`. `PAGE_SIZE = 500` passes the `contains("PAGE_SIZE = 50")` check — a 10× regression is invisible. |
| `LeftPanelScaleGateTest.java:84-97` | Slices a **fixed 3000-char window** after `indexOf("PA.filterLeft")` in a 3988-line file; the assertions can match code from neighbouring functions. `.reverse()` matches anywhere in the window. `node.style.display = 'none'` is exact-spelling. `jar/js/sidebar.js` is not checked at all. |
| `AnalysisSearchGateTest.java:48,34` | Assertions are `contains("_seq")`, `contains("d.total")` — `_seq` is satisfied by any identifier containing it, and none verify the value is *checked*. At `:34`, if `api.js` is absent, `-1 < X` is **true** — a false pass. |
| `StaleGuardAndSharedScriptGateTest.java:73,183` | Best of the set (walks all of static, throws on missing root, `assertTrue(real >= 20)` anti-vacuity). Residual: `PROVIDED_GLOBALS` has only `UIBusy` and `PollConfig` — `AccessDetails`, `SearchQuery`, `LogFab`, `ThemeToggle`, `ChatApi` are unregistered. `SearchQueryTrimGateTest:99` explicitly delegates `SearchQuery` coverage to this registry, which does not track it — a **cross-gate hole**. `SEQ_INCREMENT` requires prefix `++`; postfix exists today at `parser/js/sourceView.js:84` and `jar/js/app.js:224` (both currently benign). |
| `DirectFileIoGateTest.java:41,44,138` | Strongest anti-vacuity design (the RATCHET loop fails if counts drop). Blind to method references — `Files::isRegularFile` at `ConfigController.java:80` and `PlSqlParserMain.java:166` is uncounted (both classes already allowlisted). Omits `.exists()`, `.delete()`, `.length()`, `.isDirectory()` from `File methods`. Identity is `module + SimpleName`, so two same-named classes in one module share one budget. |
| `RenamedResponseFieldGateTest.java:39,93` | Judges **1 rename × 2 files**. Detection is `src.contains("." + old + " \|\|")` only — `cls.methods ?? []`, `(cls.methods) \|\| []`, `cls['methods']`, or a bare `cls.methods` all evade. `usesNew` is file-global: one mention anywhere, even in dead code, exonerates the file. |

**Cross-cutting:** six gates hardcode the relative path `src/main/resources/static`; only
`StaleGuardAndSharedScriptGateTest`, `ApiPathCoverageTest`, `DirectFileIoGateTest` and
`PollingConfigGateTest` use a `repoRoot()` walk-up.

### M10. Other confirmed detect-but-proceed sites

| Location | Code | Fails to prevent |
|---|---|---|
| `plsql-parser/.../flow/TriggerDiscoverer.java:129` | `System.err.println("WARNING: No body for trigger …")` then `triggers.add(mapRowFromMeta(meta, sourceBody, …))` — no `continue` | A body-less trigger enters the flow output; downstream analysis finds no side effects and reports "analyzed, none" rather than "unreadable" |
| `unified-web/.../web/service/PersistenceService.java:1062` | `try { n.setUnitType(PlsqlUnitType.valueOf(nd.unitType)); } catch (Exception ignored) {}` | After an enum rename, reloading an old snapshot yields call-graph nodes with `unitType == null` but every other field set; UI grouping silently drops them |
| `unified-web/.../controller/FlowAnalysisController.java:660` | `} catch (Exception ignored) {}` per Mongo-profile section | The connection inventory shown to the user omits real profiles with no trace |
| `unified-web/.../controller/AnalyzerController.java:1550,1552` | `catch (IOException ignored) {}` → returns empty list | "No correction logs" and "log store unreachable" render identically |
| `jar-analyzer-core/.../ExcelExportService.java:787` | `catch (Exception e) { return 0; }` | `"1,024"` or `"12.0"` becomes `0` in the exported spreadsheet, uncommented |
| `unified-web/.../promptconfig/PromptExtrasBridge.java:49,58` | `catch (Exception ignored) { }` | A broken prompt config yields empty extras, so the LLM runs **without** the user's configured context and the analysis differs with no indication why |
| `jar-analyzer-core/.../CorrectionPersistence.java:106` | `catch → return false` from `hasData()` | Store unreachable reads as "this JAR has no corrections"; the UI hides the tab |

### M11. The auth gate is fully built and switched off, but the login page still exists

`unified-web/src/main/java/com/analyzer/config/WebConfig.java:19-23`
```java
public void addInterceptors(InterceptorRegistry registry) {
    // Auth disabled: API-key login gate removed for local use.
    // registry.addInterceptor(authInterceptor);
}
```
`ApiKeyAuthInterceptor` is a correct guard (it returns `false` and 401s — see the legitimate list).
It is a `@Component`, `AuthController` is live, `static/login.html` still exists and still POSTs to
`/api/auth/login` then redirects. The intent ("local use") is documented, but the artefact of a
login screen remains and implies a gate that is not installed. If this app is ever bound beyond
localhost, every endpoint is open. **MEDIUM as configured; HIGH if exposed.**

---

## LOW

- `plsql-parser/src/test/java/com/plsql/parser/QuickParseTool.java` (renamed from `QuickParseTest`, 2026-09-04) — not a JUnit test at all: no
  `@Test`, no JUnit import, just `public static void main(String[] args)` printing an 84-line parse
  dump and requiring `args[0]`. Surefire's `**/*Test.java` matches the filename, JUnit 5 finds no
  test methods, the class is silently ignored. It neither runs nor fails; it is dead weight in
  `src/test/java` that reads as coverage. (The module does have real tests, so this is presentation,
  not a coverage hole.) Move it to a `tools/` source set or delete it.
- `jar-analyzer-core/.../ClaudeCallLogger.java:208,213` — `catch → return 0L` in token/cost
  accounting; the total shown to the user is under-reported rather than flagged.
- `DispatchResolutionEdgeCaseTest.java:738`, `CallTreeVisibilityTest.java:727` — `@Disabled` methods
  with empty bodies.
- `MessagingTaggingTest.java:39` — class comment claims "gaps marked @Disabled"; no method in the
  file carries the annotation. Stale comment.

---

## Looks wrong, but is fine

Listed because these match the pattern textually and should not be re-flagged.

**Guards that actually guard**
- `scripts/safe-build.sh:17-31` — the model implementation. Detects and `exit 1`s. Its only problem
  is that nothing calls it (H1).
- `unified-web/.../auth/ApiKeyAuthInterceptor.java:32-39` — 401s or redirects and `return false`.
  Correct; merely unregistered (M11).
- `start.sh:29-35`, `start.bat:52`, `start-mvn-build.bat:27,53,118,139` — every one of these
  `echo`-then-`exit /b 1` / `exit 1`. Real guards.
- `setup.sh:52,88,93,98,114` — `err ...; exit 1` in each. The `err`-without-exit problem is confined
  to `db-setup/setup-databases.sh`.
- `start-mvn-build.bat:79-105` — Claude CLI missing is a warn-and-continue, and that is **correct**:
  the feature is genuinely optional, the degradation is enumerated, and it is re-stated in the
  launch banner (`Claude: NOT AVAILABLE — static analysis only`). This is what a legitimate
  detect-and-degrade looks like.
- `db-setup/setup-databases.sh:130-131,167,213,264,308` — `warn ...; wait_for_port ... || { err ...;
  return 1; }`. These *do* return non-zero. It is the seed-verification lines that do not.

**`if (bad) { log }` where the body does more than log**
- `DependencyCrawler.java:533` — warns *and* sets `node.setMessage(...)`, restricts to package-level
  ops, and records the key in `knownMissingObjects`.
- `PlSqlParserEngine.java:188` — warns that >10% of source was stripped then returns the trimmed
  content; the comment above states this is deliberate and the precondition (a complete first unit +
  a genuine second `CREATE`) was verified.
- `ClaudeProcessRunner.java:129`, `TreeChunker.java:349` — warn about an oversize prompt then
  truncate. **The truncation is the prevention.**
- `ClaudeAnalysisService.java:338,625` — `if (!executor.awaitTermination(...)) { log.warn;
  executor.shutdownNow(); }`. The shutdown is the corrective action.
- `ConfigDirService.java:216`, `ClaudeController.java:332` — warn then assign a documented fallback.
- `CallGraphService.java:159,391` — warn about unresolved scope targets; the resolved subset is
  genuinely valid output and the request explicitly permits partial roots.
- `S3ArtifactStore.java:255`, `MongoArtifactStore.java:534` — warn about a partial delete, then
  return an **accurate** count. The caller receives the truth.
- `summary.js:589` — the warn follows an intentional, commented identity→array-position fallback
  that is bounds-checked (`if (idx == null || idx < 0 || idx >= eps.length) continue;`).
- `plsql-analyzer-core/.../AnalysisService.java:476,481,486,491` — `if (allX.isEmpty())` then a
  **retry** on the main connection. The body performs recovery. (The catches *around* those retries
  are H4.)
- `ParserValidationExecutor.java:83`, `ParserAnalysisExecutor.java:86` — the warn is in the `else` of
  a skip decision; nothing dangerous follows.

**Catch blocks that are genuinely recoverable**
- `jar-analyzer-core/.../PersistenceService.java:218,230,396` — best-effort scratch/staging cleanup,
  documented. A leftover inert file must not fail a complete analysis.
- `PersistenceService.java:1048` — `catch (IOException ignore) { /* cache miss next time, not fatal */ }`;
  the sidecar is a pure cache and the recovered entry is still used.
- `analyzer-storage/.../LocalArtifactStore.java:177` — entry vanished between listing and stat;
  documented and correct. `:149` — best-effort delete, logged at warn.
- `plsql-parser/.../SqlAnalyzer.java:176,237` — both carry a comment explaining that the JSqlParser
  API throws rather than returning null; the `"SUBQUERY"` fallback is the correct semantic.
- `plsql-parser/.../flow/SourceDownloader.java:136,162` — cache-lookup miss; the caller falls through
  to the DB fetch and the real error is recorded by `connManager.recordFailure` at `:118`.
- `postgres-parser/.../PostgresAnalysisVisitor.java:745`, `db2-parser/.../Db2AnalysisVisitor.java:718`
  — `return null; // text is a bonus; never fail the parse for it`. Genuinely optional field.
- `jar-analyzer-core/.../ProgressService.java:65,78` — SSE emitter already closed by the client.
- `analyzer-storage/.../mongo/MongoAnalysisRepository.java:221,426` — index-ensure is idempotent;
  the discard is swept later and the log says so.
- `AnalyzerController.java:625,646,853,872` — cache read/write around a value recomputed on miss.
- `AnalyzerController.java:961`, `ConversionController.java:449` — async gzip of an already-written
  cache, and a stderr drain thread.
- `flow-poc/.../SqlPredicateExtractor.java:35`, `DynamicFlowExecutor.java:74,96,104`,
  `BeanFactoryLoader.java:52,73,85` — documented best-effort fallbacks and optional bean lookup by
  alternate name (`bean` is null-checked downstream).
- `ClaudeCallLogger.java:199` — per-line JSONL skip; malformed-line tolerance is the point of JSONL.
- `plsql-config/.../ConfigLoader.java:41` — `loadDefault()` is the documented default path (a
  `log.debug` would be an improvement).
- All `if (...) { try { ... } catch (Exception e) { log.warn(...); } }` in `JarAnalysisExecutor`
  (95/104/191), `WarAnalysisExecutor` (84/93/173), `JarTargetedAnalysisExecutor:100`,
  `AnalysisQueueService:299`, `PromptConfigService:208`, `DependencyCrawler:728` — the log is inside
  a catch for an explicitly non-critical enrichment step, not the guard of the dangerous operation.

**Return values that are not actually ignored**
- There are **no** `File.mkdirs()/mkdir()/renameTo()/createNewFile()/setWritable()` calls in
  non-test main source — the project uses `Files.createDirectories` plus an `ArtifactStore`
  abstraction. No bare `in.read(byte[]);` statement-expressions. No submitted-and-dropped `Future`s:
  every `submit()` result is stored and later joined, or returned as a `QueueJob` handle.
- `.setReadable(...)` hits in `DependencyCrawler` are `FlowNode` model setters, not `java.io.File`.
- `store.delete(...)` / `gridFs.delete(...)` statement-expressions are `void`-or-idempotent store
  APIs, not `File.delete()`.
- `ConversionService.validateRequired` / `validateServiceOrController` are `void` and **throw**;
  there is no return value to ignore.

**Tests that look assertion-free but are not**
- `S3ArtifactStoreTest.java:520` — `verifyNoInteractions(s3)` *is* the assertion.
- `WholeBodyLossGateTest.java:92` — aggregate-then-`fail(...)`; `losses` comes from a real sweep and
  can genuinely be non-empty. Its `catch (RuntimeException ex) { return ops; }` at `:141` is
  deliberate and correctly compensated: the empty set makes the caller's diff non-empty, so the test
  still fails.
- `TableDetailFacetSpecsTest.java:204` — the branch `fail(...)` is the negative-case guard after an
  expected rejection.
- `CacheTaggingTest:134/248/270`, `EndpointTaggingTest:134/356`, `MessagingTaggingTest:182`,
  `SecurityTaggingTest:224`, `SqlExtractionEdgeCaseTest:296`, `flow-poc/.../ShadowEngineTest:108-217`
  — flagged by brace-counting confused by multi-line `@DisplayName` strings. Manually confirmed to
  have real assertions.
- `assertEquals` calls in `ArtifactStoreConformanceTest`, `S3ArtifactStoreWireTest`,
  `MongoArtifactStoreTest`, `JsonFacetPagerTest`, `HistoryFacetTest`, `CredentialGuardStoreTest`,
  `InvocationPoolTest` that appear nested — all inside try-with-resources or loops over fixed
  non-empty collections. They always execute.
- `assertNotNull` in `ExcelExportServiceTest:107/110/155/160` and `PlsqlEntityMapperTest` — on values
  returned by the code under test, not on freshly-constructed objects.
- `MongoTestSupport.clientOrSkip:73`, `MongoHousekeepingServiceTest.mongoOrSkip:211` —
  `assumeTrue(false, "No MongoDB…")` is legitimate integration-test practice. **Worth confirming CI
  provisions Mongo**; if it does not, seven Mongo suites are silent no-ops.
- `catch (RuntimeException ignored)` in `MongoAnalysisRepositoryTest:79`, `MongoIngestFailureTest:104/109`,
  `MongoOverflowSweepTest:87`, `MongoRepositoryConcurrencyTest:97`, `MongoPlsqlEntityIndexTest:86`,
  `MongoHousekeepingServiceTest:180/207`, `FakeS3Server:130` — all in `@AfterEach`/`finally`
  teardown (`db.drop()`, `client.close()`), not around assertions. `HttpClientTaggingTest:101`
  rethrows.
- `no-local-writes.sh:52-63` — the S3 acceptance check. Judges every change against an allowlist and
  `return 1`s on any unexpected file. A correct gate.
- `ApiPathCoverageTest.java:67-69` — `if (mappedPrefixes.isEmpty()) fail(...)`, and
  `DirectFileIoGateTest`'s RATCHET loop — both explicitly defend against their own vacuous pass.
  These are the pattern done right.

**JS early returns that are correct**

These match `if (!x) return;` textually and were each read and cleared.
- Not-on-this-page guards for modules loaded by several pages: `jar/js/dashboard.js:11`, `:76`;
  `jar/js/summary.js:94`; `plsql/js/summary.js:29` (the last two then handle "no analysis" with an
  explicit empty-state message); `parser/js/callGraphViz.js:20`.
- Optional DOM sub-elements: `parser/js/tableOps.js:17`, `parser/js/joins.js:20` inside
  `_initTable` / `_initTypePills`.
- Panel-not-open guards on functions also driven by a poll timer: `jar/js/sessions.js:49`.
- Cache-not-yet-populated guard where opening an empty modal would be worse:
  `jar/js/dashboard.js:130`.
- User cancelled a confirm dialog: `jar/js/dashboard.js:311`, `:322`; `jar/js/sessions.js:132`.
- Idempotent init / re-entry guards: `jar/js/summary.js:62` (`if (this._config) return;`),
  `parser/js/refs.js:27`.
- Intentional debounce: `plsql/js/app.js:1478`.
- **Stale-response guards — these are correct and load-bearing**: `if (!isCurrent()) return;` /
  `entry.fqn !== fqn` at `jar/js/codeTreePanel.js:70`, `jar/js/summary-codeview.js:93`,
  `jar/js/endpointList.js:150`, `:159`, `jar/js/summary-trace.js:339`, and the
  `myGen !== PA.callTrace._gen` check at `parser/js/callGraph.js:118`. Do not confuse these with
  H12 — the bug there is the *null-data* path, not the staleness path, which is right in both files.
- Bounds checks: `parser/js/callGraph.js:114`; `jar/js/summary.js:589`
  (`if (idx == null || idx < 0 || idx >= eps.length) continue;`).
- Formatter guards on missing timestamps, cosmetic: `jar/js/sessions.js:169`, `:175`, `:189`;
  `jar/js/dashboard.js:337`.
- `try { es.close(); } catch(e){}` EventSource teardown: `parser/js/history.js:365`, `:425`;
  `plsql/js/claude.js:303`, `:407`; `jar/js/app.js:409`.
- Clipboard failures, cosmetic: `parser/js/dynamicSql.js:187`, `parser/js/tableDetail.js:665`.

---

## Suggested order of work

1. Wire `scripts/safe-build.sh` into `setup.sh` and `start-mvn-build.bat`; add an
   instance-kill to `start.sh` / `start.bat` as `LOGBACK-NOCLASSDEF.md` already prescribes. (H1)
2. Make `err()` in `db-setup/setup-databases.sh` return 1, or add explicit `return 1` after the five
   seed checks; make `print_summary` reflect actual per-database status. (H2)
3. Check `mergeClaudeResult`'s return before `markEndpointComplete`, and make
   `ClaudeProcessRunner:223` return null / throw on a nonzero exit. (H3)
4. Record a `metadataResolved=false` flag on the analysis artifact when the H4 catches fire, and
   surface it in the UI instead of persisting `"TABLE"` / `external=true` silently.
5. Make `plsql/js/api.js` and `parser/js/api.js` throw on `!res.ok` instead of returning `[]`/`null`,
   and give the ~10 `load()` call sites a `catch → error banner in the panel`. One change closes
   most of H11. (H11)
6. Add `assertEquals(0, withErrors)` to the four harness tests and `assertNotNull(in)` in place of
   the early returns — the analysis is already written, only the gate is missing. (M2)
7. Move the `slice()` in `plsql/js/callGraph.js:82` inside the `if (data)` block to match the parser
   twin, and surface a distinct "failed to load" state in `drillDown`. (H12)
8. Add `jar/js/callGraph.js` to `TraceReadabilityGateTest.RENDERERS`, and cap the indent at
   `jar/js/callGraph.js:369`. (H9)
