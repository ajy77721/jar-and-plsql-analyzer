# PL/SQL → Spring Boot/MongoDB Full-Project Conversion — via MCP "plsql-parse" + Skills

**Status:** DRAFT v2 — for lead review
**Date:** 2026-10-07
**Repo:** amp-analyzer (multi-module Maven, Spring Boot 3.2.5, Java 17)

---

## 1. Goal

The analyzer already parses PL/SQL (all 5 dialects: Oracle, MySQL, PostgreSQL, Db2, SQL Server) into rich, uniform JSON artifacts. This feature uses that output to convert **whole projects** to **Java + MongoDB**:

1. **MCP server** ("plsql-parse") exposing the parse output as queryable tools — Claude Code pulls exact context (statements, call tree, sources, tables) instead of fetching/re-parsing raw database sources.
2. **A full-project conversion pipeline** — first a human-reviewed **project blueprint** (entities, repositories, service interfaces with frozen method signatures, DTOs, common beans, module layout), then per-procedure conversion **into that pre-agreed skeleton** — never re-inventing structure.
3. A **construct-strategy catalog** — for every hard construct (cursors, sequences, and DB features MongoDB cannot express directly: FOR UPDATE, triggers, CONNECT BY, analytic functions, MERGE, dynamic SQL…) a decided pattern (direct-translate / pattern-translate) or an explicit advisory flag. Never silently approximated.

**Decisions agreed with the requester:**
- Full-project conversion is the target; per-procedure conversion happens INTO a reviewed skeleton; the same contracts make whole-analysis batch a loop later.
- Generated code: **Spring Boot + Spring Data MongoDB**.

## 2. What already exists (verified in the repo)

| Asset | Where | Relevance |
|---|---|---|
| Parse output per analysis | `parser/plsql-parse/<DIALECT>/<NAME>/<ENV>/<ENTRY>/` (S3 via ArtifactStore) | The conversion fuel — see §4 |
| Conversion POC | `ConversionController`/`ConversionService` (`/api/conversion/*`), `static/conversion/index.html` | Already streams `claude -p --output-format stream-json` (parses tool_use events); encodes rules **G01–G26** (Mongo repository patterns, cursor→List, dynamic SQL→Criteria, autonomous txn→REQUIRES_NEW…) in 11/8/7-phase markdown. Prompts currently tell Claude to fetch source ITSELF — the gap MCP tools fill |
| Readers to reuse | `AnalysisViewStore` (19 sidecar views), `StatementsOffsetIndexReader` (paged statements), `AnalysisReaderFactory`, `ArtifactStore` (S3/local/mongo + per-user SSO routing) | MCP tools are thin wrappers over these — the <200ms API bar comes free |
| Claude CLI invocation patterns | 4 existing (`claude -p` + stdin + `--allowedTools`; ProcessBuilder, session ids, process-tree kill) | The conversion loop keeps using the proven one |
| MCP | **Nothing yet** | New build |

## 3. READ FIRST — how the system knows "new" vs "already converted" (the migration ledger)

As beans/classes/services grow across many conversion runs, dedup and coverage do NOT rely on memory or naming conventions — they rely on a **conversion ledger**: a machine-readable `.claude/conversion/state.json` in the target project, derived from the frozen blueprint.

- **One entry per SOURCE object**: every table, procedure/function/trigger, package, cursor, sequence, job → its target artifact (entity class, repository, service interface + method, DTO, common bean) → status: `PENDING → PLANNED (in blueprint) → SKELETON (stub generated) → CONVERTED → VALIDATED`, plus the source identity (`analysisName + nodeId` + artifact version) so a re-parse can't double-convert and a changed source is detected.
- **Checked BEFORE every action** (the convert skill's mandatory first steps):
  1. **Procedure first-time check**: is this proc already CONVERTED in the ledger? If YES → **no reconversion — only "plug in"**: wire the caller's call to the existing service method using the frozen signature. If first-time → full conversion. These two modes are distinct from the very first step of the skill.
  2. **Table/entity check**: does the table's entity already exist? Reuse it (G05) — never a second entity. New table → create entity + repository + ledger entry.
  3. **DTO possibility**: before creating a DTO, match the cursor-row shape against existing DTOs (field-shape comparison) — reuse or explicitly extend.
  4. **Repository check**: the (operation, predicate) already has a declared method? Reuse; only genuinely new access patterns add methods.
- **The ledger is updated in the SAME step as every code write** (write code + update state.json atomically in the skill's checklist) — coverage is always queryable: "X of Y procedures converted, which tables still pending"; a resumed/interrupted run knows exactly where to continue.
- This is also what makes BATCH safe later: batch = iterate ledger entries with status `PENDING`, nothing else.

## 4. The conversion fuel — parse output per analysis

Uniform shapes across all 5 dialects (minor deltas: PostgreSQL resultSets, statement-type vocabulary):

| Artifact | Contents |
|---|---|
| `api/manifest.json` | index of everything: counts, artifact list, 19 sidecar view paths |
| `api/nodes/<SCHEMA$OBJECT>.json` | per proc/func/trigger: parameters[{name,dataType,direction}], variables, tables{operations{OP:[lines]}, joins}, cursors[{name,query,tables,resultSets}], calls[{target,lines}], dynamicSql, exceptionHandlers, statementSummary, transactionScope, sourceFile |
| `api/statements/index.json` (+`.offsets.json`) | per statement: {nodeId, sourceFile, tableName, operation, line, sqlText, joins[], columns[], columnDetail[], predicates[]} — 1.7k–5k rows/analysis |
| `api/tables/index.json` | per table: usedBy[{nodeId, operations, predicates}], dbColumns/dbConstraints/dbIndexes, dbValidated |
| `api/call_graph.json` + `edges/` | call edges {fromNodeId, toNodeId, line, depth} |
| `sources/<SCHEMA>.<object>.prc` | raw PL/SQL source per routine |
| `views/**` | 19 precomputed sidecars: complexity, call-tree/<nodeId>, callers, cursors, dynamic-sql, sequences, joins-summary, table-joins, table-columns, views/triggers/globals details, jobs, summary-stats, search |

## 5. Architecture

```
┌ MCP server "plsql-parse" (13 tools, inside unified-web) ┐
│                                                          │
│ STAGE 1  plsql-blueprint skill → blueprint.md + blueprint.json (human-edited, FROZEN)
│ STAGE 2  skeleton generator     → Maven skeleton (deterministic Java, from blueprint.json)
│ STAGE 3  plsql-convert skill    → per-object conversion against frozen signatures
│ STAGE 4  validate               → logic-coverage findings JSON (per object + project rollup)
└──────────────────────────────────────────────────────────┘
```

### 5.1 MCP server
Java MCP server **inside `unified-web`** (new package `com.plsqlanalyzer.web.mcp` — no new module; all readers already live there). Official **MCP Java SDK** (`io.modelcontextprotocol.sdk:mcp`, version pinned in the parent pom) — not Spring AI's starter (Spring AI 1.0 targets Boot 3.4; this repo pins 3.2.5). Transport: **Streamable HTTP/SSE** at `/mcp/plsql-parse` (Claude Code connects with `claude mcp add --transport http`) + a stdio bootstrap for local dev/tests. Auth: **bearer token** (`PLSQL_PARSE_MCP_TOKEN` env) guarding only `/mcp/**`, mapping to a service-account context behind a `McpAnalysisContext` interface (per-user tokens are a later seam). Same pod, same port, no deployment change. A Node/Python MCP server was considered and rejected: it would re-implement offset seeking, gzip-twin fallback, staleness checks and S3 credentials.

### 5.2 Tool surface (13 tools — each a thin wrapper over an existing reader; nothing scans a whole JSON)

| Tool | Backing reader | Notes |
|---|---|---|
| `list-analyses` | analyses listing | name, dialect, entry, counts |
| `get-index` | `api/index.json` | totals, transaction summary, cycles |
| `get-manifest` | `api/manifest.json` | artifact paths, counts |
| `get-node` | `api/nodes/*.json` | full node doc |
| **`get-statements`** | `StatementsOffsetIndexReader` | node-filtered + paged; ordered rows with sqlText/line/operation/table — **the converter's primary tool** |
| `get-statement` | offset reader | one row by (node, line) |
| `search-statements` | sidecar + filter | q, operation, table |
| `get-tables` | `api/tables/index.json` + per-table splits | usedBy, operations, predicates, columns |
| `get-call-tree` | `views/call-tree/<nodeId>` | recursive tree |
| `get-callers` | `views/callers.json` | reverse-call map |
| `get-sidecar` | `AnalysisViewStore` | any of the 19 views |
| `get-source` | sources/ + line range | 200-line cap default |
| **`get-procedure-signatures`** | call_graph + node docs, **memoized as derived sidecar `views/blueprint-input.json`** (`AnalysisViewStore.readOrCompute`) | per-node params/cursors/resultSets/exceptionHandlers/transactionScope/calledByCount — the blueprint stage's evidence doc; hot path stays <200ms after first compute |

### 5.3 Blueprint stage (STAGE 1) — every decision mapped to an artifact field
Output: human-reviewable `docs/conversion/blueprint.md` + machine-**frozen** `.claude/conversion/blueprint.json` in the target project. The convert skill **refuses** an absent/unfrozen blueprint and must "propose a blueprint edit, never diverge silently".

| Blueprint decision | Driven by (artifact field) | Rule |
|---|---|---|
| One `@Document` per table | `api/tables/index.json` rows | `dbValidated:true` → db-validated entity; `false` → "inferred" entity from statement `columnDetail` only, flagged in blueprint.md |
| Field names/types | `dbColumns[]{columnName,dataType,nullable}` | NUMBER→BigDecimal/Long, VARCHAR2/CHAR→String, DATE/TIMESTAMP→Instant, CLOB→String (>16MB → GridFS advisory), BLOB/RAW→Binary; `nullable:false` → wrapper + `@NonNull` |
| `@Id` / indexes | `dbConstraints[]` PK columns, `dbIndexes[]` | composite PK → natural-key `@CompoundIndex(unique)` + surrogate note |
| **Embedded vs referenced** | node `tables[].joins[]{joinType,condition,leftTable}` + `usedBy[].predicates[]` + `views/table-joins.json` | child joined ONLY from one parent on parent key → **embed as array**; N:1 → **ref by id**; many-to-many → own collection; **every decision records its join evidence** so a human can overrule in one pass |
| Views / MVs | `views/views-index.json` (definition, columns, base tables) | read-only model fed by an `@Aggregation` pipeline over base entities; never a written collection |
| **Repository inventory** | per table `usedBy[].operations{OP:[lines]}` + `predicates[]{columnName,operator}` | ONE repository method per distinct (operation, predicate column+operator) — e.g. `{col,"<"}` SELECT → `findByColLessThan`; joins/aggregations → `@Aggregation` methods; MERGE → upsert. Declared ONCE; conversions reuse, never invent |
| **Shared service interfaces** | `api/call_graph.json` → **in-degree**; node `parameters[]`; `cursors[].resultSets` | callee with **in-degree ≥ 2 → shared service interface**, signature frozen (IN→args, OUT/IN OUT→holder DTO, REF CURSOR→`List<T>` return); in-degree ≤ 1 → private impl in the owning service |
| **Common beans** | transactionScope on many nodes, `sequences.json`, exceptionHandlers shapes, `jobs-index.json` | `TransactionHelper`, `SequenceService`, typed exception hierarchy, `@Scheduled` config — generated once in `common` |
| **DTO inventory** | cursor queries + statement `columnDetail[]` + IN OUT params | one DTO per cursor row shape and per IN OUT record |
| Skeleton shape | counts (tables / schemas / shared services) + join co-occurrence | decision framework §5.4 |

**Blueprint flow:** deterministic rules (skill reference data — not model judgment) → Claude drafts both files → **STOP for human review/edit** → freeze (`frozen: true` in the JSON). Re-runs are deterministic: same artifacts + same human edits → same output.

### 5.4 Skeleton decision framework (STAGE 2 input)
- **Single-module** when < ~20 tables AND one schema AND < 5 shared services → `src/main/java/<basepkg>/{entity,repository,service,service/impl,dto,common,config}`.
- **Multi-module** when multiple schemas or join co-occurrence clusters (tables repeatedly joined together, from `table-joins.json`) split into domains → parent pom + `<domain>-domain` (entity+repo+dto), `<domain>-service`, `common`, `web`.
- Always generated: `pom.xml` (spring-boot-starter-data-mongodb, spring-boot-starter-web), `application.yml` (Mongo URI placeholder), `MongoConfig` with `MongoTransactionManager`, `CLAUDE.md` + `CONVERSION_GUIDELINES.md` (the G01–G26 rules), `.claude/conversion/blueprint.json`, and the seeded migration ledger `state.json`.

### 5.5 Construct-strategy catalog (26 strategies — ships as skill reference data)
Classification: **DT** direct-translate · **PT** pattern-translate · **ADV** advisory (cannot translate — flagged in findings, human decides).

| # | Construct | Triggered by (artifact field) | Strategy | Class |
|---|---|---|---|---|
| 1 | Cursor, simple FOR loop | `cursors[]` + SELECT rows, no DML on same table in loop | repo method → `List<T>`, enhanced-for | PT |
| 2 | Cursor + row-by-row DML in loop | SELECT lines then INSERT/UPDATE on same table in loop | loop over `List<T>` + save per row; >~1000 rows → bulk flag | PT |
| 3 | Explicit OPEN/FETCH cursor | explicit open/fetch/close | same as 1 (List + index) | PT |
| 4 | **Cursor FOR UPDATE** | `sqlText` FOR UPDATE | **no row locks in Mongo** → `@Version` optimistic or `findAndModify` atomic block + flag | ADV |
| 5 | REF CURSOR / OUT result set | OUT cursor params; `resultSets` | interface returns `List<T>`/`Stream<T>` (signature frozen) | PT |
| 6 | BULK COLLECT | `sqlText` | List-returning repo method | PT |
| 7 | FORALL | `sqlText` | `BulkOperations` / `insertAll` | PT |
| 8 | **Sequences** | `views/sequences.json` | `counters` collection + `findAndModify $inc` in common `SequenceService` | PT |
| 9 | COMMIT / ROLLBACK | `transactionScope` | `@Transactional` + `MongoTransactionManager` ONLY where transactionScope non-empty | PT |
| 10 | PRAGMA AUTONOMOUS_TRANSACTION | source/sqlText | separate bean method, `Propagation.REQUIRES_NEW` | PT |
| 11 | Package globals | `views/globals-index.json` | pass-as-params (preferred) or `@Component` state holder + thread-safety advisory | PT+ADV |
| 12 | **Row-level triggers** | `views/triggers-index.json` | **NOT auto-converted** — app-layer event / CDC (Debezium) advisory; write nothing | ADV |
| 13 | INSTEAD OF / statement triggers | `trigger-detail/` | same as 12 | ADV |
| 14 | DBMS_SCHEDULER / jobs | `views/jobs-index.json` | `@Scheduled(cron=…)` bean calling the converted routine | PT |
| 15 | DBMS_OUTPUT | PUT_LINE | `log.info` with sanitized args (G03) | DT |
| 16 | Exception handlers | `exceptionHandlers[]` | Optional/empty-check for NO_DATA_FOUND; typed hierarchy; OTHERS → catch-all + log preserving semantics | PT |
| 17 | RAISE_APPLICATION_ERROR | sqlText/handlers | custom `AppException(code,msg)` + codes table | DT |
| 18 | MERGE | operation MERGE | upsert (save with version / MongoTemplate) | PT |
| 19 | CONNECT BY | CONNECT BY PRIOR | `$graphLookup` | PT |
| 20 | Analytic/window functions | OVER/PARTITION BY | `$setWindowFields`; too complex → app-side + ADV | PT |
| 21 | Dynamic SQL | `views/dynamic-sql.json` | reconstruct as `Criteria`/`Query` builders; un-parseable text → ADV flag with raw text | PT/ADV |
| 22 | NVL/DECODE/COALESCE/ADD_MONTHS… | sqlText, columnDetail | Java idioms / `java.time` (fixed table) | DT |
| 23 | LOB columns | CLOB/BLOB | String/Binary; >16MB → GridFS + advisory | PT |
| 24 | Views / MVs | views-index | read-only `@Aggregation`; MV refresh → `@Scheduled` recompute | PT |
| 25 | FROM DUAL / seq.NEXTVAL | statements | covered by #8 | PT |
| 26 | Pipelined/table functions | objectType/sqlText | manual List builder + complexity flag | ADV |

Strategy ids (`S1`–`S26`) are cited in blueprint.json AND in the stage-4 validation findings — full traceability from construct → generated code → validation result.

## 6. Conversion order — static/utils first, then main procedures (operations attached)

1. **Static / utility layer first**: pure helpers, shared functions with NO table access (from the call graph: nodes called by others, zero tables) → `common`/`util` classes.
2. **Common beans**: `SequenceService`, `TransactionHelper`, exception hierarchy, `@Scheduled` config.
3. **Entities + repositories** — ALL tables declared before any body conversion.
4. **Shared service INTERFACES** (in-degree ≥ 2) — signatures frozen.
5. **Main procedures** — leaf-first within each call-tree branch, **each conversion carrying its operations attached**: every DML/table line maps to the pre-declared repository method (the statement's line/sqlText cited in the `PROCEDUREREF` comment), every cursor to its declared strategy, transaction scope to the helper. A conversion needing an operation nobody declared → STOP, propose a blueprint edit (never silently add a repository method).
6. **Entry points last** → project integration validation (rollup findings).

## 7. Wiring into the existing conversion screen

- Both entry points run the SAME skill: a human in Claude Code (skill installed in the target project), or the existing conversion screen driving `claude -p`.
- `ConversionController` keeps its `--verbose --output-format stream-json` streaming untouched; it adds a generated `.mcp.json` + `mcp__plsql-parse__*` to the child CLI's `--allowedTools`, so the CLI connects to the MCP server for context (no more Oracle connectivity or sql-cache needed).
- `ConversionService`'s 11/8/7-phase markdown is **superseded behind a flag** (`conversion.use-skill`, default off during rollout) — prompts become short "execute skill X per blueprint.json" stubs.

## 8. Phases (each independently testable)

| Phase | Deliverable | Tests / verification |
|---|---|---|
| **1. MCP server + 13 tools** | `McpServerConfig`, `PlsqlParseToolRegistry`, `McpAnalysisFacade` (single seam), `McpAnalysisContext` + token impl, `McpAuthFilter`, config keys | Golden test per tool vs real demo artifacts (ORACLEDEMO + one non-Oracle dialect); statements paging + offsets-absent fallback; gzip-twin equivalence; auth filter; blueprint-input memoization (2nd call = no re-reads); curl handshake + MCP Inspector; full reactor green |
| **2. `plsql-blueprint` skill + schema** | SKILL.md + decisions.md + strategies.md + blueprint-schema.md; installer endpoint; **ledger seeding** (`state.json`, one PENDING entry per source object) | Filled ORACLEDEMO example blueprint as test resource; grep gates (every rule cites its artifact field, every strategy has an id); human runs the skill on ORACLEDEMO → reviews blueprint.md |
| **3. Skeleton generator** | `SkeletonGenerator` + `POST /api/conversion/skeleton {projectPath}` — deterministic Java (NOT skill-driven) reads frozen blueprint.json → exact Maven tree | Golden file-tree test per branch (single vs multi-module); entity annotations, repo method names, package layout asserted |
| **4. `plsql-convert` skill + wiring** | SKILL.md + mapping.md + validate.md + tools.md; installer; prompt swap behind flag; CLI `.mcp.json` + allowlist; UI context-source indicator | Prompt golden (skill name, blueprint path, NO "Fetch Oracle source"); **ledger idempotency** (converting the same procedure twice → second run is a no-op plug-in); existing job tests green; live E2E: blueprint ORACLEDEMO → skeleton → convert one leaf + one shared procedure → target `mvn compile` → findings JSON valid → signatures match blueprint exactly |
| **5. Batch mode (later)** | `BatchConversionService` loops stage 3 over `get-procedure-signatures` ordering (shared-first, in-degree order), existing worker pool (`claude.max-workers: 3`), per-object findings + project rollup | Batch smoke: 3 procedures unattended + rollup report |

## 9. Risks and mitigations

| Risk | Mitigation |
|---|---|
| MCP SDK is 0.x (young) | Pin exact version in parent pom; confine all SDK types to `McpServerConfig`/`PlsqlParseToolRegistry` — an upgrade touches 2 files |
| **Blueprint drift** (conversion invents structure) | Frozen `blueprint.json` contract + "propose blueprint edit, never diverge" rule hard-wired into the convert skill |
| Embedded-vs-referenced judgment | Every decision carries its join/usedBy evidence in blueprint.md — one-pass human audit |
| Volume growth / duplicate classes | The migration ledger: checked before every action, updated atomically with every write |
| Statements node-window contiguity | Asserted in golden tests — node-filtered paging cannot silently degrade to 5k-row scans |
| Dialect deltas (PG resultSets, statement vocab) | blueprint-input carries the analysis's actual operation vocab; goldens include a non-Oracle analysis |
| Auth | Bearer token from env only (never committed); filter scoped to `/mcp/**`; existing SSO routes untouched |

## 10. Out of scope (later)
Batch orchestrator implementation (phase 5), per-user MCP tokens, Claude writing the analyzer's own repo (it only ever writes the TARGET project), any change to the analyzer's parse/crawl behavior.
