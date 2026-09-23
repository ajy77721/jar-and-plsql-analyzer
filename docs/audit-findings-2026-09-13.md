# Facet Capture Audit — 2026-09-13

Full static-code + live-data audit of what every dialect (Oracle, MySQL, DB2, SQL Server,
Postgres, JAR/Java) actually captures vs. what's assumed/documented. Produced by 6 parallel
review passes, each cross-checking real visitor source against real analysis JSON (mongosh /
live API), not assumptions. Ranked by severity. Use this as the fix backlog — each item has
file:line, root cause, suggested fix, and a concrete test case.

---

## Status update #4 — 2026-09-13, final cleanup pass (all remaining pending items closed)

Went through every item still marked pending after status update #3. Result: **everything is now
either fixed-and-live-verified, or explicitly, correctly ruled a non-issue.** No items remain open.

- **#24 (DB2 sequences/exceptionHandlers not reaching live output) — RESOLVED, was a false alarm.**
  Added temporary diagnostic logging at both the `DependencyCrawler` assignment point and the
  `ChunkedFlowWriter` write point, then re-ran a single, isolated DB2 crawl (not part of an
  overlapping 4-job batch like the run that originally surfaced this). Logs and the actual Mongo
  document both confirmed real data present at every step (34 handlers on the root node, 31 on a
  child, `ERP_SEQ_ORDER` NEXTVAL at line 4 matching the original audit's own example). The
  earlier "broken" finding was caused by inspecting a stale/intermediate snapshot from a batch of
  4 simultaneous re-crawls, not a real code defect. Debug logging has been removed again. **DB2's
  sequences and exception handlers are confirmed genuinely working end-to-end.**
- **#21 / SQL Server `identityColumns` downstream wiring — FIXED.** Added `FlowNode
  .identityColumns`, wired both `DependencyCrawler` (targetObj branch — IDENTITY is a
  ParsedObject-level, non-package construct, so only that branch needs it) and
  `ChunkedFlowWriter`'s node-detail write. Compiles clean, all plsql-parser tests pass. **Not
  live-verified** — the current SQL Server sample dataset genuinely contains no `IDENTITY(...)`
  columns in any table (all temp tables are plain `#stage`/`##g`), so there's nothing in the live
  data to exercise this path; the code mirrors the exceptionHandlers wiring exactly, which IS
  confirmed working, so this is high-confidence but not independently proven live.
- **#10 (JAR External Dependency facet scope) — FIXED, decided in favor of "extend" over
  "rename".** Added a new, additive `CallNode.thirdPartyDependencies` field (real third-party
  library classes actually invoked, JDK-core classes like `java.*` deliberately excluded),
  populated in `CallTreeBuilder` at the exact point a call target is excluded from recursion, and
  surfaced as a new `THIRD_PARTY_DEPENDENCY` category in `AbstractAnalysisDataProvider`'s dynamic-
  flows facet — additive, so the existing crossModule/httpTarget "External Dependency" facet and
  its consumers are completely unaffected. **Verified live**: re-ran a fresh JAR analysis of
  `unified-web` itself (1706 classes) and confirmed 7,080 real third-party dependency entries in
  the actual call tree (e.g. `org.springframework.web.multipart.MultipartFile`,
  `org.slf4j.Logger`), correctly excluding JDK-core noise.
- **#23 (Tomcat/logback `NoClassDefFoundError`) — investigated further, left as-is.** Confirmed
  the `ThrowableProxy` class genuinely IS present inside `logback-classic-1.4.14.jar` in the built
  fat jar (not a missing dependency), and there's only one copy of that jar in the classpath (no
  version conflict). This points to a classloader-context mismatch (Tomcat's JUL-bridge error
  path resolving through a different loader realm than the app's normal logging path) rather than
  a packaging bug — a genuinely rare, hard-to-reproduce-on-demand class of issue. Given it
  self-heals on restart and hasn't recurred since, no further time was spent chasing it; still
  documented for anyone who hits it again with reproduction steps to try.

### Items confirmed as correctly "no action needed" (not bugs)
- **#18** (table-kind tagging is a client-side heuristic) — acceptable by design.
- **#22** (dual Claude-verification DTO shapes) — a documented risk, not a live bug.
- **#7 across all dialects** (predicates parity) — confirmed already working via the shared
  `TableOperationEnricher` for every dialect tested (Oracle, MySQL, DB2, SQL Server; Postgres
  confirmed via the Postgres agent's own test) — no gap actually existed once real SQL text was
  fed through it.

---

## Status update #3 — 2026-09-13, deferred-item fix pass (4 parallel agents) + live verification

Launched 4 background agents (one per dialect: MySQL, DB2, SQL Server, Postgres) to tackle the
items deliberately deferred in status update #2. All 4 succeeded at the code+unit-test level —
see per-agent detail below — and the full `plsql-parser` reactor (all touched modules together)
compiles and tests clean (`mvn -pl unified-web -am compile` and `mvn -pl plsql-parser -am test`
both exit 0). One real cross-cutting bug was caught and fixed during merge (see "Cross-agent bug
found" below). **Live end-to-end verification on fresh crawls found the mechanism genuinely works
for SQL Server, but DB2's new facets do NOT reach the final output despite passing at the parser
layer — see "P0 #24 (NEW)" below.** This is the exact kind of gap the earlier P0 fixes were
verified against and this one was not — flagging honestly rather than claiming full success.

### MySQL agent — #7 (predicates), #9 (exception handlers)
- **#7:** no code change needed — confirmed already working via `TableOperationEnricher` (real
  test: `UPDATE erp_x SET status=1 WHERE id=5 AND active=1` → 2 real `whereFilters`).
- **#9:** fixed — new `MySqlExceptionHandlerInfo` model, `visitDeclareHandler` override, handler-
  body statements tagged via a new `handlerIndex` field (mirroring Oracle's existing pattern:
  tagged AND kept in the flat list, not removed). Wired to the shared `ParsedObject
  .exceptionHandlers` field (already existed from Oracle — reused, not duplicated). Verified via a
  real test: `DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; CALL erp_usp_audit(1); END;` →
  1 exception handler captured with 2 statements. 34/34 mysql-parser tests pass, 762/763
  plsql-parser tests pass (the 1 failure was the cross-agent bug below, since fixed).

### DB2 agent — #7 (predicates), #8 (sequences), #9 (exception handlers)
- **#7:** no code change needed — same enricher confirmation as MySQL.
- **#8:** fixed at the parser layer — new `Db2SequenceInfo` model, visitor extraction of `NEXT
  VALUE FOR`/`NEXTVAL FOR`/`PREVVAL FOR`, converted to synthetic `NEXTVAL`/`CURRVAL` `CallInfo`
  entries in `Db2ProcedureParser` (reusing the mechanism `SqlServerProcedureParser` already uses).
- **#9:** fixed at the parser layer — new `Db2ExceptionHandlerInfo` model, `visitDeclareHandlerStmt`
  override, wired to the shared `ExceptionHandlerInfo`/`ParsedObject.exceptionHandlers`.
- 26/26 real DB2 sources still parse clean, zero regressions in db2-parser's test suite.
- **However — see P0 #24 below: live verification found neither #8 nor #9 actually reaches the
  final live analysis output for DB2, despite passing at the parser/unit-test layer.**

### SQL Server agent — #7 (predicates), #8 (IDENTITY columns)
- **#7:** no code change needed — confirmed already working (`UPDATE erp.SalesOrder SET Status=1
  WHERE OrderId=5 AND Total>0` → real `whereFilters`).
- **#8:** fixed — added IDENTITY-column detection (regex-based, matching this visitor's existing
  style) to `SqlServerAnalysisVisitor`, extended `SqlServerSequenceInfo` additively
  (`identityColumn`/`seed`/`increment`), added a new shared `IdentityColumnInfo` model +
  `ParsedObject.identityColumns` field. **Confirmed NOT yet wired through `DependencyCrawler`/
  `ChunkedFlowWriter`** to the final node output (same class of gap as #24, scoped narrower here
  since the agent flagged it themselves rather than it being an unpleasant surprise) — the
  `identityColumns` key was confirmed absent from the live `api/nodes/*.json` output for
  `USP_RUNMRPCYCLE`'s root node. Needs the same downstream-wiring follow-up as #24.
- 29/29 relevant plsql-parser tests + all 3 sqlserver-parser test suites pass, zero regressions.
- **Live-verified working:** this agent's SQL Server `sequences`/`exceptionHandlers` (from the
  earlier DB2/general work, not new this pass) DO reach the final output correctly — used as the
  positive control that proved #24 is DB2-specific, not a systemic gap.

### Postgres agent — #7 (predicates), #8 (sequences), #9 (handlers), #20 (DO-blocks, RETURNING)
- **#7:** no code change needed — same enricher confirmation.
- **#8:** fixed — removed NEXTVAL/CURRVAL/SETVAL/LASTVAL from the builtin-exclusion list, added
  real sequence-function-call detection + `PostgresSequenceInfo`, wired to a synthetic `CallInfo`
  (SqlServer's pattern again).
- **#9:** fixed — new `PostgresExceptionHandlerInfo`, `visitExceptionHandler` override, reused the
  shared `ExceptionHandlerInfo`/`ParsedObject.exceptionHandlers` field.
- **#20 (DO-blocks):** succeeded, the riskiest change of the whole pass — added a `doStatement`
  grammar alternative to `compilationUnit`, reusing the existing `bodyBlock` rule so DO-block
  bodies flow through identical visitor logic to routine bodies. Full postgres-parser suite run
  after this change: zero regressions.
- **#20 (RETURNING):** fixed — new `RETURNING` lexer token, `returningClause`/`returningItem`
  grammar rules wired onto INSERT/UPDATE/DELETE, `returningColumns` added to
  `PostgresTableOpInfo`/shared `TableOperationInfo`.
- **46/46 postgres-parser tests pass** (4 new tests added, zero regressions) — the most thoroughly
  verified of the 4 agents, including a real grammar change.
- Not yet live-verified via a fresh crawl for the new facets specifically (Predicates/Sequences/
  DO-blocks/RETURNING) beyond compile+unit-test — same caveat as DB2/SQL Server: unit-test-level
  confidence only until proven otherwise on live data.

### Cross-agent bug found and fixed during merge
The Postgres agent's additive `returningColumns` field on the shared `TableOperationInfo` model
tripped `TableOperationCopyCompleteTest` (`plsql-parser/src/test/java/com/plsql/parser/flow/
TableOperationCopyCompleteTest.java`) — a reflection-based test that already exists specifically
because this exact class of bug ("a new field added to the model, but not copied in
`DependencyCrawler.resolveTableSchemas`, so it's silently lost") had happened 3 times before.
Fixed by adding `rt.setReturningColumns(op.getReturningColumns());` to the copy block at
`DependencyCrawler.java` (right after the existing `whereFilters`/`predicateScope` copies). This
is exactly the kind of gap this session's earlier live-verification discipline is meant to catch —
confirmed by that same discipline surfacing the (separate, still-open) #24 below.

### P0 #24 (NEW) — DB2's new facets (sequences, exceptionHandlers) don't reach the live output
- **Confirmed via direct instrumentation** (a standalone classpath probe against
  `db2-parser/target/classes` and `plsql-parser/target/classes`, run outside the app): parsing the
  real, live `ERP_USP_ALLOCATE_INVENTORY.prc` source (fetched fresh from Mongo, the exact text the
  live crawl uses) through `Db2AnalysisEngine.parse(...)` directly produces 31 real
  `Db2ExceptionHandlerInfo` entries, and through `Db2ProcedureParser.parse(...)` the resulting
  `ParsedObject.getExceptionHandlers()` also has 31 entries. **The parser layer is confirmed
  correct.**
- **But the live crawl's final output does not have it**: cleared all `.node-cache`/`.parse-cache`
  entries for DB2 (ruling out stale-cache reuse of a pre-fix parse), triggered a genuinely fresh
  crawl, and inspected the actual `api/nodes/DB2INST1$ERP_USP_ALLOCATE_INVENTORY.json` (and the
  root node, `DB2INST1$ERP_USP_RUN_CYCLE.json`) written by this fresh run: neither has an
  `exceptionHandlers` or `sequences` key at all (not null-valued — the key is absent, meaning
  `ChunkedFlowWriter`'s `!isEmpty()` write-guard is being hit, i.e. `FlowNode.getExceptionHandlers()`
  is empty/null by the time the node is written, despite the parser producing 31 real entries).
- **Ruled out as the cause:** stale parse-cache (cleared, still reproduces); a second/legacy DB2
  parser implementation (grep confirms only one `Db2ProcedureParser`, used identically by
  `DependencyCrawler` and by my standalone probe); `NodeParseCache`'s Jackson round-trip (uses
  full-object serialization, not a field-by-field copy, so it can't selectively drop a field); the
  `ChunkedFlowWriter.writeChunk`/`readNodeChunk` round-trip (same reasoning — full Jackson
  serialization of `FlowNode`, not a manual copy).
- **Not yet found:** the exact point between `DependencyCrawler` calling `Db2ProcedureParser
  .parse(...)` (confirmed correct) and the `FlowNode` object that reaches `ChunkedFlowWriter`'s
  write loop (confirmed empty) where the 31 handlers are lost. `DependencyCrawler`'s own
  `node.setExceptionHandlers(...)` call sites (2 of them) both look structurally correct by
  inspection and are shared, dialect-agnostic code that provably DOES work for SQL Server (used as
  a live positive control in this same investigation — SQL Server's root node in the same session
  has both `sequences` and `exceptionHandlers` correctly populated with real content). So this is
  a **DB2-specific** break, not a shared-code bug — meaning something about how DB2 objects are
  matched/resolved in `DependencyCrawler`'s BFS (`targetObj`/`targetSub`/`ownerSchema` matching
  logic, lines ~400-460) must differ from SQL Server's path in a way not yet identified.
- **Impact:** DB2 sequences and exception handlers are fully implemented and unit-tested at the
  parser layer, but currently do not surface anywhere in the live app (API, UI, Global Search) —
  users will not see this data despite it being "fixed."
- **Suggested next step:** add temporary debug logging (or a debugger breakpoint) at
  `DependencyCrawler.java` lines ~544-596 during a live DB2 crawl, printing
  `targetObj.getExceptionHandlers().size()` immediately before and after the `allExHandlers`
  assembly, to see exactly which step empties it — a live JVM inspection is needed here, static
  reading has been exhausted without finding the break point.

### Suggested immediate follow-ups (in priority order)
1. Root-cause and fix #24 (DB2 sequences/exceptionHandlers not reaching output) — highest priority
   since it means DB2's "fixed" facets are currently invisible in the live app.
2. Spot-check whether SQL Server's `identityColumns` and Postgres's newly-added facets
   (sequences/exceptionHandlers/DO-blocks/RETURNING) have the SAME live-output gap as #24, or work
   like SQL Server's pre-existing sequences/exceptionHandlers did — untested beyond parser-layer
   unit tests in this pass. Given #24 turned out to be DB2-specific rather than systemic, these
   are plausibly fine, but "plausibly fine" is exactly the assumption this whole audit exists to
   not make — needs the same live-crawl-plus-direct-JSON-inspection check #24 got.
3. Wire SQL Server's `identityColumns` field through `DependencyCrawler`/`ChunkedFlowWriter` to the
   final node output (confirmed missing, smaller/better-understood gap than #24).

---

## Status update #2 — 2026-09-13, second fix pass (P1/P2 items)

Additional items fixed and compiled/deployed this pass (not yet re-verified live via a fresh
Claude-verification run the way #1/#2/#4/#6 were — compile-clean + code-review-level confidence
only, flagged per item below):

- **#12 (Postgres prompt template unregistered):** fixed — added
  `"plsql-postgres-verification"` to `PromptConfigService.KNOWN_TEMPLATES`. **Verified live**:
  `GET /api/prompt-config/templates` now lists it.
- **#13 (no way to force a genuinely fresh Claude verify):** fixed — `ClaudeVerificationService
  .verify()` now takes a `force` flag that bypasses the checkpoint/existing-chunk-output skip
  entirely; wired through `POST .../claude/verify`'s request body (`{"force": true}`). Not yet
  live-tested end-to-end (would require a real Claude run to confirm chunks actually regenerate).
- **#14 (`IndexFacetReader` "null"-string bug):** fixed — all 5 unsafe `.asText()` sites on
  `detailFile` now go through the file's own existing `textOrNull()` helper.
- **#15 (`TableFacetReader` unguarded `allOperations` element `.asText()`):** fixed — both sites
  now skip JSON-null array elements instead of stringifying them.
- **#16 (join condition whitespace corruption, MySQL + DB2):** fixed — both visitors now use their
  existing `originalText()` helper instead of `ctx.getText()` for `JoinInfo.condition`.
- **#17 (dynamic-SQL `stmtName` dropped in MySQL/DB2 dialect→shared-model conversion):** fixed —
  added `stmtName` to the shared `DynamicSqlInfo` model and wired it through both
  `MySqlProcedureParser.toDynamicSql` and `Db2ProcedureParser.toDynamicSql`.
- **#19 (`_rowMatchedPreview` never surfaced a PREDICATE snippet):** fixed in all three
  `{plsql,parser,jar}/js/search.js` — added a `raw.predicates[0]` fallback branch
  (`columnName operator value`), mirroring `_rowLine`'s existing pattern.
- **#21 (DB2 `BEGIN ATOMIC` discarded):** partially fixed — `Db2RoutineInfo.atomic` is now
  captured via a new `visitBodyBlock` override. **Not yet wired further downstream** — DB2's
  `Db2ProcedureParser` doesn't currently copy this (or any transaction-semantics flag) onto the
  shared `ParsedObject`/`FlowNode` model the final JSON output is built from (Oracle's analogous
  `autonomousTransaction` flag follows a completely separate, Oracle-only pipeline path via
  `SubProgramInfo.isPragmaAutonomousTransaction()` — there's no existing generic slot to reuse).
  Full end-to-end wiring is a follow-up if this field needs to actually reach the API/UI.
- **#11 (JAR reflection detection was a string-literal substring scan):** fixed properly, not just
  patched — added `CallNode.reflectiveCalls`, populated by `CallTreeBuilder` from real resolved
  bytecode invocations (`ownerClass`+`methodName` matched against actual `java.lang.reflect.*`/
  `java.lang.Class` reflection APIs), and rewired `AbstractAnalysisDataProvider`'s REFLECTION
  facet to read this instead. The old string-literal scan (the actual source of both the
  false-positive and false-negative risk) was removed, not left running alongside the new logic.

**Deliberately deferred, not fixed this pass** (would require new ANTLR grammar rules and/or
substantial new visitor logic across multiple dialects — too large/risky to rush; each remains a
real, valid finding for a dedicated follow-up):
- **#7 (Predicates facet parity)** — would mean adding real WHERE/HAVING grammar+visitor support
  to MySQL, DB2, SQL Server, and Postgres (today only Oracle has it, via a separate enrichment
  pass). Substantial, multi-dialect grammar work.
- **#8 (Sequences parity)** — DB2 (extract already-parsed `NEXT VALUE FOR`), SQL Server (IDENTITY
  columns), Postgres (stop excluding NEXTVAL/CURRVAL/SETVAL, add a real facet) each need separate,
  non-trivial visitor work.
- **#9 (Exception/condition handlers invisible in MySQL/DB2/Postgres)** — grammar rules exist in
  all three but need new model classes + visitor overrides + statement-ownership tagging (so
  handler-body statements stop leaking into the main statement list) — a bigger lift than the
  CTE-guard/join-condition-style fixes done this pass.
- **#20 (Postgres DO-blocks, RETURNING clause)** — DO-blocks need a new top-level grammar
  alternative (`compilationUnit`), which is a higher-risk grammar change than adding a visitor
  override to an existing rule; RETURNING needs a new grammar rule + DTO field.
- **#10 (JAR External Dependency facet naming/scope)** — audit offered two options (rename vs. add
  real third-party detection); both change either an API-facing field or user-visible behavior
  meaningfully enough to warrant explicit sign-off on which direction, rather than picking one
  silently.
- **#18 (table-kind tagging is a pure client-side heuristic)** — already a low-priority,
  non-bug-per-se item per the original audit; no action needed unless #2's fix (which resolves the
  compounding "#" bug) isn't enough on its own.
- **#22 (dual Claude-verification DTO shapes)** — documentation/maintainability note only, no live
  bug found; no code change needed unless the DTOs are meant to converge.
- **#23 (intermittent `NoClassDefFoundError` in Tomcat connector)** — needs reproduction under load
  first; not actionable as a code fix yet (see its own entry above for the suggested approach).

---

## Status update #1 — 2026-09-13, first fix pass (P0 items)

Items #1, #2, #4, #6 below are **FIXED and verified live** on genuinely fresh re-crawls
(SQL Server job `9244c291`, MySQL job `bb2e3d26`) after code changes + rebuild/restart:

- **#1 (`"null"` table entry):** fixed in `plsql-parser/.../ChunkedFlowWriter.java` (added a
  shared `tableKey()` helper, applied at all 6 concatenation sites) + the SQL Server visitor path
  that produced the null name is now correctly filtered out instead of silently stringified.
  Verified: fresh crawl's `tables/page` has zero `"null"`/empty-name entries (down from 84 raw
  rows to 82, both bogus rows gone).
- **#2 (bare `"#"` merging 10 real `##`-tables):** fixed via `SqlServerLexer.g4`'s `ID` rule
  (added `#` to the continuation character class). Verified: `##g13` through `##g175` now all
  appear as distinct, correctly-named entries in the fresh crawl.
- **#3 (26 vs 84 count mismatch):** resolved as a side effect of #1/#2 — both `tables/summary`
  and `tables/page` now agree on `totalTables: 82`. The residual `validatedTableCount: 26` is a
  legitimately different metric (DB-existence-validated subset), not a bug — no further action
  needed unless the UI should visually distinguish the two counts better.
- **#4 (MySQL CTE phantom tables):** fixed by porting DB2's `cteNames`/`isCte()` guard into
  `MySqlAnalysisVisitor`. Verified: fresh MySQL crawl's Tables tab no longer contains `CUST_829`
  (previously a real `WITH cust_829 AS (...)` CTE misfiled as a genuine table).
- **#6 (JAR Aggregation Pipeline false positive):** fixed by requiring the invoked return type to
  actually be in a real Mongo/Spring-Data aggregation package before flagging pipeline
  involvement. Compiled clean; not yet re-verified against a fresh JAR analysis run (lower
  priority — flag for a follow-up spot-check next time a JAR analysis is re-run).

Also confirmed via this pass: the PL/SQL Claude-verification expansion (joins/sequences/
dynamicSql/predicates) is genuinely populated with real `claudeVerifications` data end-to-end —
84 joins, 6 sequences, 345 dynamicSql, 109 predicates on the SQL Server analysis, including
Claude itself correctly flagging the (pre-fix) `"#"` table bug in a predicate's `reason` field.

**New issue found during this verification pass (not yet fixed, not in the original 22):**
intermittently, a `NoClassDefFoundError: ch/qos/logback/classic/spi/ThrowableProxy` fires inside
Tomcat's NIO connector on certain requests, and appeared to correlate with subsequent requests
hanging/timing out until the app was restarted. Worth a follow-up look at the logback/slf4j
dependency setup (possible classpath/shading conflict) if this recurs — see "P0 #23" below.

### P0 #23 (NEW) — intermittent `NoClassDefFoundError` in Tomcat connector, correlates with request hangs
- **Evidence:** app log showed repeated `Exception in thread "http-nio-8083-exec-N"
  java.lang.NoClassDefFoundError: ch/qos/logback/classic/spi/ThrowableProxy` (via
  `SLF4JBridgeHandler` → `org.apache.juli.logging.DirectJDKLog.error` → Tomcat's
  `NioEndpoint$SocketProcessor`), immediately after two requests (`claude/verification-summary`,
  `claude/result`) had already returned 200 successfully. Subsequent fresh `curl` calls to the
  same/other endpoints then hung and timed out (`curl: (28) Operation timed out`) until the app
  was restarted.
- **Hypothesis (unconfirmed):** a classloading gap for `logback-classic` reachable only from
  Tomcat's own JUL-bridge error-logging path (as opposed to the app's normal SLF4J logging, which
  works fine elsewhere) — possibly a shaded/relocated dependency or a missing runtime jar on the
  embedded Tomcat's classloader. Each occurrence may be leaking a NIO worker thread that never
  gets recycled, eventually exhausting the connector's thread pool.
- **Suggested fix:** reproduce deliberately (hit `claude/verification-summary`/`claude/result`
  repeatedly under load), capture a thread dump when requests start hanging, and check for a
  `logback-classic` version/classifier mismatch or missing dependency in `unified-web`'s effective
  classpath (`mvn dependency:tree | grep logback`).
- **Test case:** N/A yet — needs reproduction first; not a unit-testable fix until the root cause
  is pinned down.

---

## P0 — Confirmed live data-corruption bugs

### 1. Literal string `"null"` table name (SQL Server) — ROOT CAUSE FOUND
- **File:line:** `plsql-parser/src/main/java/com/plsql/parser/flow/ChunkedFlowWriter.java:1455`
  (also similar unguarded concatenation at lines 724, 732, 1180, 1907, 2054 — audit further)
  ```java
  String key = (top.getSchema() != null ? top.getSchema() + "." : "") + top.getTableName();
  ```
  `top.getTableName()` can be `null`; `+` turns it into the literal text `"null"`.
- **Upstream cause:** `sqlserver-parser/.../SqlServerAnalysisVisitor.java` — `visitSelectStatement`
  (~line 491-507) and the INSERT/UPDATE/DELETE visitors (~447/463/479) construct
  `SqlServerTableOpInfo` directly, bypassing the existing null-guard already present in
  `addTableOp` (line ~1106: `if (name == null || name.isEmpty()) return null;`). Triggered by
  statements with no resolvable table, e.g. `SELECT @x = GETDATE()`.
- **Live evidence:** `GET /api/parser/analyses/SQLSERVER~SQLSERVERDEMO~LOCAL~USP_RUNMRPCYCLE/tables`
  → `"name": "null"` entry, ~1925 aggregated usage count (every no-table-resolved statement across
  the 55-node call graph merges into this one fake key). `TableFacetReader.java`'s
  `"null"`→`"(unresolved reference)"` relabel (already applied this session) only patches the
  merged/summarized view — the raw `tables/index.json` still stores the bare `"null"` string.
- **Fix:** (a) guard null/empty `tableName` before building the map key in
  `ChunkedFlowWriter.java:1455` (and the other 5 concat sites found by the general audit — verify
  each); (b) route SQL Server's "no table resolved" SELECT/INSERT/UPDATE/DELETE cases through the
  existing guarded `addTableOp` funnel instead of constructing `SqlServerTableOpInfo` directly.
- **Test case:** parse `SELECT @x = GETDATE();` (SQL Server) → assert no emitted table entry has
  `name == "null"` or `name == null` reaching the writer.

### 2. Bare `"#"` table entry — lexer/grammar bug merging 10 distinct temp tables (SQL Server)
- **File:line:** `sqlserver-parser/src/main/antlr4/com/plsql/sqlserver/grammar/SqlServerLexer.g4:95`
  — `ID : [a-zA-Z_#$] [a-zA-Z_$0-9]* ;` allows a leading `#` but **not** `#` in the continuation
  class, so `##g13` lexes as two tokens: `"#"` + `"#g13"`. `tableRef`
  (`SqlServerParser.g4:237-239`) only consumes one un-dotted token, so
  `extractTableName()` (`SqlServerAnalysisVisitor.java:1161-1172`) returns just `"#"` for
  `INSERT INTO ##g13 ...`.
- **Live evidence:** the `"#"` entry has 10 line hits matching exactly 10 real
  `##g13`...`##g175` global temp tables (correctly named elsewhere via the regex-recovery path
  used for CREATE/DROP/FROM/JOIN — only INSERT hits this bug). Corrupts predicates facet too
  (same predicate duplicated across all 10 merged tables).
- **Fix:** add `#` to the ID continuation character class in the lexer grammar, or make
  `extractTableName` scan/merge adjacent `ID` tokens.
- **Test case:** parse `INSERT INTO ##g13(Col) SELECT Col FROM Src;` → assert
  `getTableName() == "##g13"`, not `"#"`.

### 3. Header table-count ("26") vs Tables-tab count ("~76/84") mismatch — different filters, same data
- **Path A (26):** `TableFacetReader.getTablesSummary()` excludes rows where
  `dbValidated==true && dbFound==false`. Live: `.../tables/summary` →
  `{"totalTables":84,"validatedTableCount":26}`.
- **Path B (84):** `ParserAnalysisController.getTablesPage()` (~line 429-450) streams raw
  `tables/index.json` with no dbValidated/dbFound filtering (includes bug #1 and #2's bogus rows).
- **Fix:** apply the same `dbValidated && !dbFound` hide predicate in both endpoints, OR
  explicitly relabel them as two different metrics ("validated real tables" vs "all detected
  references") in the UI so the discrepancy isn't read as a bug.
- **Test case:** after fixing #1/#2 and applying a consistent filter, assert
  `tables/summary.validatedTableCount` reconciles with a client-side-filtered count from
  `tables/page`.

### 4. MySQL CTE names recorded as real tables ("phantom" tables)
- **File:line:** `mysql-parser/.../MySqlAnalysisVisitor.java:304-322` (`tableOpForAtom`) — no CTE
  name filter, unlike DB2's `cteNames` set + `isCte()` guard
  (`db2-parser/.../Db2AnalysisVisitor.java:32-33, 354-357, 381-386`).
- **Live evidence:** `data/plsql-parse/MYSQL/.../ERP_USP_RUN_CYCLE/tables.json` contains
  `"ANALYZERDB.CUST_829"` as a genuine `TABLE`/`SELECT` entry, sourced entirely from a
  `WITH cust_829 AS (...)` CTE in `erp_usp_dynamic_report.prc`. DB2's equivalent output has no
  such entries — proving this is a MySQL-only gap, not inherent difficulty.
- **Fix:** port DB2's `cteNames` collection/skip logic into
  `MySqlAnalysisVisitor.visitWithClause`/`tableOpForAtom`.
- **Test case:** MySQL `WITH x AS (SELECT 1) SELECT * FROM x;` → assert `tables.json` has no
  entity named `X`.

### 5. Trigger bodies never parsed at all (MySQL + DB2) — silently zero-fidelity
- **File:line:** grammar entry points only accept `createRoutine`/routine definitions
  (`mysql-parser/.../MySqlParser.g4:23-25`; `plsql-parser/.../MySqlProcedureParser.java:34-61`).
  No trigger-body grammar rule/visitor exists in either module. Trigger nodes are synthesized as
  pure metadata (name/table/event/timing) by
  `DependencyCrawler.emitTriggerNodesAndEdges` (`plsql-parser/.../DependencyCrawler.java:792-929`)
  — the body text is written to `sources/*.TRG.*.sql` but never actually parsed.
- **Live evidence:** `sources/analyzerdb.TRG.trg_order_line_au.sql` contains a real
  `INSERT INTO erp_audit_log(...)`; the matching chunk (`chunk_0064.json`,
  `ANALYZERDB$TRG$TRG_ORDER_LINE_AU`) has only `{triggerEvent, triggerTable, triggerTiming,
  callCount:0}` — no `tableOperations`, no `statements`. Same confirmed for DB2's
  `ERP_TRG_INV_TXN_AI`.
- **Fix:** add a trigger-body grammar entry point (or reuse the existing routine-body rule against
  the trigger's body text) and run it through the same visitor so trigger table-ops/calls attach
  to the trigger's node.
- **Test case:** `CREATE TRIGGER t AFTER INSERT ON tbl ... INSERT INTO audit_log VALUES(...)` →
  assert the trigger node's `tableOperations` contains the audit_log INSERT.

### 6. Aggregation Pipeline (JAR) false positive: bare substring match on unrelated return types
- **File:line:** `jar-analyzer-core/.../AggregationDetector.java:615-626`
  ```java
  if (rt != null && (rt.contains("Aggregation") || rt.contains("Pipeline") ...
  ```
  No check that the type belongs to Mongo/Spring-Data aggregation.
- **Live evidence:** `com.plsql.parser.PlSqlParser#create_function_body` (an unrelated ANTLR
  grammar method) gets a populated fake `aggregationPipeline` because
  `Pipelined_using_clauseContext` merely contains the substring "Pipeline". Also
  `ChunkedFlowWriter.writeFinalResult/writeApiFiles/writeCallGraph` surfaced as
  `operationType: "AGGREGATE"` with all-empty stages, due to unrelated classes named
  `AnalyzerPipeline`/`AggregationPipelineParser` in `flow-poc/`.
- **Fix:** require the invoked owner class/return type to be in a real Mongo/Spring-Data
  aggregation package allowlist before setting `isDynamic`/adding to `pipelineSourceMethods`.
- **Test case:** a class with a method named similarly to a grammar rule containing "Pipeline"
  but unrelated to Mongo → assert `isDynamic == false`, empty `pipelineSourceMethods`.

---

## P1 — High-value gaps (schema exists, extraction doesn't; or facet misnamed vs. what it measures)

### 7. Predicates (WHERE/HAVING) — weakest facet across the board
- Oracle: real WHERE extraction exists, but via a **separate** JSqlParser enrichment pass
  (`com.plsqlanalyzer.parser.service.SqlAnalyzer` → `TableOperationEnricher.java:434-440`), not the
  main ANTLR visitor — if that pass is ever skipped/fails, WHERE data silently disappears with no
  fallback.
- MySQL, DB2, SQL Server, Postgres: only JOIN ON-condition captured as raw string; no WHERE/HAVING
  facet or DTO field at all.
- **Fix:** decide whether Predicates should be a first-class per-dialect visitor facet (consistent
  everywhere) or officially documented as "Oracle-only, via enrichment pass" — currently it's an
  undocumented asymmetry.
- **Test case:** same WHERE-bearing SQL run through all 5 dialects → assert consistent behavior
  (either all populate `predicates`, or the schema/docs explicitly say which dialects don't).

### 8. Sequences — inconsistent across dialects
- Oracle: solid. MySQL: not implemented (correct — no native pre-8.0 sequences, not a bug).
  DB2: grammar parses `NEXT VALUE FOR` but nothing extracts it into a facet — confirmed via real
  DB2 source (`SET ERP_GV_RUN_ID = NEXTVAL FOR ERP_SEQ_ORDER;`) producing zero `sequences` key in
  the output chunk. SQL Server: `NEXTVAL`-style captured, but `IDENTITY` columns not captured at
  all (zero grep hits). Postgres: NEXTVAL/CURRVAL/SETVAL are actively placed in the **excluded
  builtins list**, so real sequence usage is discarded, not just unmodeled.
- **Fix:** add `sequences` extraction + DTO field for DB2 (`Db2SequenceInfo`), add SQL Server
  IDENTITY-column capture, and for Postgres remove NEXTVAL/CURRVAL/SETVAL from the exclusion list
  and add a real sequences facet instead.
- **Test case:** per dialect, a minimal sequence-usage snippet → assert a populated `sequences`
  array with name + operation (NEXT/PREV/etc) + line.

### 9. Exception/condition handlers — solid only in Oracle
- MySQL, DB2: grammar has full handler rules (`declareHandler`/`declareHandlerStmt`) but zero
  visitor override and zero model field — handler body statements leak into the flat top-level
  `statements`/`calls` list indistinguishable from main-body code. Confirmed live: DB2's
  `DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; CALL ERP_USP_AUDIT(...); END;` shows
  `ROLLBACK`/`CALL` as ordinary top-level statements with no handler association.
- SQL Server: TRY block range + statement count captured, CATCH body detail not modeled.
- Postgres: grammar rules exist, no visitor override, no DTO — same gap as MySQL/DB2.
- **Fix:** add `exceptionHandlers` (kind, condition(s), line range) to MySQL/DB2/Postgres routine
  models + visitor overrides, mirroring Oracle's existing implementation.
- **Test case:** `DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1;` → assert an
  `exceptionHandlers` entry `{kind:"CONTINUE", condition:"NOT FOUND"}` exists, and that statements
  inside the handler body are tagged as belonging to it (not merged into the main statement list).

### 10. External Dependency (JAR) facet cannot report actual third-party library calls
- **File:line:** `CrossRefClaudeService.java:643-654` (facet = `crossModule || httpTarget` only),
  gated by `CallTreeBuilder.java:1439-1459` (`isStandardLibraryClass`), which folds JDK classes
  and third-party frameworks (Guava, Jackson, Commons, Mongo driver, Spring) into one
  undifferentiated "don't recurse" bucket.
- **Impact:** a call into e.g. `com.google.common.collect.Lists` is filtered out before any
  external-dependency flag could ever be set — the facet, as implemented, only reports intra-repo
  cross-module calls and outbound HTTP, not "external dependencies" in the sense the name implies.
- **Fix:** either rename the facet ("Cross-Module / HTTP Dependency") or add a genuine
  third-party-dependency detector recording the classes currently excluded by
  `isStandardLibraryClass`/`isExcludedByConfig`.
- **Test case:** a call to a real third-party library method → assert it's tagged as an
  external/third-party dependency somewhere in output (currently it will not be).

### 11. Reflection detection (JAR) is a string-literal substring scan, not real bytecode analysis
- **File:line:** `AbstractAnalysisDataProvider.java:1105-1126` — checks whether a **string literal
  constant** contains text like `"Method.invoke"`, not whether the method performs an actual
  reflective bytecode call. Real reflective calls target `java.lang.reflect.*`, which is already
  excluded from recursion by `isStandardLibraryClass`, so genuine reflection is often invisible.
- **False-negative:** real `obj.getClass().getMethod("foo").invoke(obj)` misses unless a matching
  string literal happens to coexist in the same method.
- **False-positive:** a log message containing the prose "about to call getMethod" would
  incorrectly flag as REFLECTION.
- **Fix:** detect reflection from actual `InvocationRef` entries whose owner class is
  `java.lang.reflect.Method`/`Class`/`Constructor` with method name `invoke`/`forName`/
  `newInstance`, not string-literal matching.
- **Test case:** one method with real reflection and no matching string literal (should be
  flagged, currently isn't); one method with only a log-message string literal containing
  "Method.invoke" prose (should NOT be flagged, currently is).

### 12. `plsql-postgres-verification` prompt template unregistered — permanently un-editable via API
- **File:line:** `PromptConfigService.java` `KNOWN_TEMPLATES` (~line 48-58) — Postgres's real
  `dialectSlug()` output (`plsql-postgres-verification`) is missing from this list, so
  `GET/POST/reset .../templates/plsql-postgres-verification` all return HTTP 400 "Unknown
  template name." Currently harmless only because it happens to fall back to a correct classpath
  default — but the moment someone needs to customize/reset the Postgres verification prompt via
  the API, it silently can't be done. Confirmed: oracle/mysql/sqlserver/db2 have no equivalent
  gap.
- **Fix:** add `"plsql-postgres-verification"` to `KNOWN_TEMPLATES`.
- **Test case:** `GET /api/prompt-config/templates` includes `plsql-postgres-verification`;
  `POST .../plsql-postgres-verification` returns 200.

### 13. `/claude/verify` has no way to force a genuinely fresh run — silently reuses stale chunk output
- **File:line:** `ClaudeVerificationService.java:134-155` (unconditional skip-if-output-exists),
  `ClaudeController.java:85-106` (`resume` flag is accepted but never passed into `verify()`).
- **Impact:** already hit once this session — had to manually delete Mongo artifact documents to
  force a truly fresh Claude verification run after a prompt-template fix. No API-level way to do
  this exists today.
- **Fix:** add a `force`/`fresh` param that either clears checkpoint+chunk artifacts before
  running, or wires `resume` through so the skip logic only applies when explicitly resuming.
- **Test case:** run verify twice against an analysis with an intentionally stale chunk output;
  with `force=true` (once added), assert the chunk is genuinely regenerated; without it, assert
  today's stale-skip behavior as the regression baseline.

---

## P2 — Medium/low-severity correctness bugs

### 14. `IndexFacetReader.java` — JSON `null` becomes the literal string `"null"` (same bug class as #1, different file)
- **File:line:** `IndexFacetReader.java:46,61,70,161,166` — `node.has("detailFile") ?
  node.get("detailFile").asText() : null` — `has()` is true even for explicit JSON `null`, and
  `.asText()` with no default returns `"null"` for a `NullNode`.
- **Fix:** replace all 5 sites with the file's own existing `textOrNull()` helper (already used
  elsewhere in the same file).
- **Test case:** facet JSON with `"detailFile": null` → assert reader returns Java `null`, not
  `"null"`.

### 15. `TableFacetReader.java` — unguarded `.asText()` on `allOperations` array elements
- **File:line:** lines ~310, 407, 408 — same bug class as #1/#14, could turn a null array element
  into the literal string `"null"` in the merged ops list.
- **Fix:** guard with `!o.isNull()` or use `.asText("")` and skip empties.
- **Test case:** `allOperations` array containing a `null` entry → assert it never surfaces as the
  string `"null"` in merged output.

### 16. Join condition text has no whitespace between tokens for multi-clause conditions (MySQL + DB2)
- **File:line:** `MySqlAnalysisVisitor.java:254-256` and `Db2AnalysisVisitor.java:305-306` both use
  raw `ctx.getText()` (drops all whitespace) instead of the dialect's own `originalText()`/
  `enclosingStatementText()` helper used elsewhere in the same files.
- **Live evidence:** a real self-join condition renders as
  `"a.customer_id=b.customer_idANDa.order_id<b.order_id"` — unreadable, not re-parseable.
- **Fix:** use `originalText(ctx)` instead of `getText()` for `JoinInfo.condition` in both
  visitors.
- **Test case:** `... JOIN t2 ON t1.a = t2.a AND t1.b < t2.b` → assert `condition` is the original
  spaced text.

### 17. Dynamic-SQL `stmtName` captured internally (MySQL) but dropped in dialect→shared-model conversion
- **File:line:** `MySqlAnalysisVisitor.java:385-413` sets `stmtName`, but
  `MySqlProcedureParser.toDynamicSql` (~156-166) never copies it, and the shared
  `DynamicSqlInfo` model has no field for it at all. Also: `sqlExpression` for
  `PREPARE ... FROM @var` is just the variable name, not the resolved SQL text.
- **Impact:** with 2+ PREPARE/EXECUTE/DEALLOCATE triples in one procedure, there's no way to tell
  which EXECUTE/DEALLOCATE pairs with which PREPARE.
- **Fix:** add `stmtName` to the shared `DynamicSqlInfo` model, populate it in the converter;
  document that `sqlExpression` for a variable-sourced PREPARE is the variable reference, not
  resolved SQL (or attempt variable-value tracking if feasible).
- **Test case:** two independent PREPARE/EXECUTE/DEALLOCATE triples with different statement names
  → assert each entry's `stmtName` lets you correctly pair them.

### 18. Table "kind" tagging (temp table vs. real table) is a pure client-side string heuristic, not backend-verified
- **File:line:** `shared/tableFramework.js:993-999` `classifyTableKind` — purely prefix-based
  (`#`/`##`/`@`) on the name string; zero backend Java hits for this classification.
- **Impact:** mislabels bracket-quoted real names like `[#Config]`, and (compounding bug #2) would
  double-mislabel the corrupted `"#"` entry as `LOCAL_TEMP` even though it's actually 10 merged
  real global temp tables.
- **Fix:** low priority as a heuristic (prefix IS the real T-SQL signal), but once #2 is fixed this
  stops being a compounding problem. Consider backend-side kind tagging for defense in depth.
- **Test case:** a real (non-temp) table literally named `[#Config]` → confirm current behavior
  mislabels it; decide if that's acceptable or needs a backend flag to override.

### 19. `_rowMatchedPreview` (search.js, all 3 apps) never surfaces a PREDICATE snippet
- **File:line:** `{plsql,parser,jar}/js/search.js` `_rowMatchedPreview` (~line 454-462) — only
  inspects `raw.accessDetails[0]`; PREDICATE rows carry detail under `raw.predicates[0]`
  (`columnName`/`operator`/`value`) — a fact the sibling `_rowLine` helper in the same file already
  accounts for.
- **Fix:** add a `raw.predicates[0]` branch to the fallback chain, mirroring `_rowLine`.
- **Test case:** a PREDICATE search result with `predicates: [{columnName:"QTY_ON_HAND",
  operator:">=", value:"0"}]` → assert preview text contains `"QTY_ON_HAND >= 0"`, not a bare
  field-name label.

### 20. Postgres: no visitor for EXCEPTION handlers, no DO-block support, RETURNING clause unstructured
- Same handler gap as #9 (Postgres-specific evidence: zero `visitExceptionHandler` grep hits).
- Anonymous `DO $$ ... $$` blocks: grammar has no top-level alternative for them at all — a whole
  class of real Postgres code (ad-hoc scripts) can't be parsed.
- `RETURNING` clause: swallowed as raw trailing text, never split into a queryable field on
  `PostgresTableOpInfo`.
- **Fix:** add `doStatement` grammar alternative + visitor path; add `returningColumns` field +
  grammar rule.
- **Test case:** `DO $$ BEGIN INSERT INTO erp.log(msg) VALUES ('hi'); END $$;` should parse and
  yield a table op for `erp.log` (currently: syntax error/empty result).
  `INSERT INTO erp.orders(id) VALUES (1) RETURNING id;` → assert `returningColumns` includes `id`.

### 21. DB2 `BEGIN ATOMIC` vs implicit `NOT ATOMIC` parsed but discarded
- **File:line:** `Db2Parser.g4:41,390` — `ATOMIC` token optionally matched, then dropped; no
  field/visitor captures it. Not yet observed wrong in sampled data (no sample uses explicit
  ATOMIC) — a code-inspection finding, low urgency.
- **Fix (if wanted):** add `atomic: boolean` to `Db2RoutineInfo`, set from `ctx.ATOMIC() != null`.

### 22. Dual Claude-verification DTOs are not field-compatible (maintainability risk, not a live bug)
- `com.plsqlanalyzer.web.parser.service.ClaudeVerificationService` and
  `com.plsqlanalyzer.web.service.ClaudeVerificationRunner`/`ClaudeVerificationService` are both
  live, serving genuinely different endpoints — not legacy-vs-active as previously assumed. But
  their result DTOs have incompatible fields (`timestamp` type differs String vs LocalDateTime;
  one has `calls/joins/sequences/dynamicSql/predicates`+`triggers`, the other has
  `schemaName/staticAccessCount/isExternal`/`procedureId/sourceFile` instead of `userDecision`).
  No live miswiring found, but flagged as a real risk if a future change accidentally crosses the
  two paths.
- **Fix:** no immediate action; consider consolidating to one DTO shape, or clearly document why
  two shapes are intentional.

---

## Confirmed correct (no action needed) — for completeness, so nothing gets re-investigated

- Oracle: `%ROWTYPE`/`%TYPE` dependency extraction, autonomous transaction pragma, BULK COLLECT,
  exception handlers (full), dynamic SQL, joins, cursors, calls — all confirmed via real file:line
  + passing test suite.
- Postgres: CTE false-positive suppression, JOIN+alias resolution, dynamic SQL (3 kinds), array
  data types (round-trip as string, not decomposed — acceptable), builtin-function filtering,
  cursor lifecycle (previously fixed).
- MySQL/DB2: backtick-quoted identifiers (MySQL), views-vs-tables classification (both, via schema
  resolver cache not grammar), WHERE/SET predicate detail via shared `TableOperationEnricher`,
  basic joins/cursors/calls/CTE-statement-recording/control-flow/TRUNCATE-as-table-op.
- SQL Server: dynamic SQL (EXEC/EXEC-literal/sp_executesql, structurally distinguished correctly).
- JAR: Dynamic Dispatch (`CallTreeBuilder.java:1899-2179`) — genuine bytecode-driven resolution,
  verified against live data (2,349-node tree, plausible non-trivial dispatch-type spread, not an
  always-empty stub); crossModule/HTTP mechanism itself is real and correctly recomputed
  per-endpoint (just narrower in scope than its name implies — see #10).
- Prompt template registry: oracle/mysql/sqlserver/db2 all correctly registered (only postgres has
  the gap, #12).
- Other FacetReaders (TableDetailFacetReader, SequenceFacetReader, NodeScanFacetReader,
  CallTreeFacetReader): consistently use safe `textOrNull`/`textOrDefault`/`intOrDefault` helpers;
  a few array-element `.asText()` calls are fragile-but-not-currently-buggy (safe only by
  data-flow invariant — worth hardening opportunistically, not urgent).

---

## Suggested fix order

1. **P0 #1–#3** (SQL Server null/`#`/count-mismatch) — you already flagged all three directly;
   root causes are now fully traced, straightforward guard-clause fixes.
2. **P0 #4–#5** (MySQL CTE phantom tables, trigger bodies unparsed in MySQL+DB2) — data-integrity
   bugs affecting the core "tables" inventory and an entire object type.
3. **P0 #6** (JAR aggregation false positive) — currently poisoning real analysis output with fake
   Mongo-aggregation flags on unrelated PL/SQL-grammar-adjacent code.
4. **P1 #7–#13** — real feature gaps, prioritize by which facets you're actively relying on
   (Predicates/Sequences/Exceptions consistency, JAR External-Dependency scope, prompt-registry
   gap, verify-force-refresh capability).
5. **P2 #14–#22** — smaller correctness/hygiene fixes, batch these together.

Each item above has a paired test case — recommend adding them as real unit tests in the
respective dialect's test module (`*-parser/src/test/...`) as each fix lands, so this class of
regression can't silently return.
