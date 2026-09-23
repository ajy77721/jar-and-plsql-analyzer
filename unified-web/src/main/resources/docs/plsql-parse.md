# PL/SQL Analysis — Internal Flow

> **Version:** 1.0
> **Last Updated:** 2026-05-17

Technical reference for the full pipeline that turns a PL/SQL object name + Oracle connection into a persisted analysis result. Covers dependency discovery, source fetching, ANTLR4 parsing, call graph construction, table operation collection, and optional Claude enrichment.

---

## Table of Contents

1. [Flow Diagram](#1-flow-diagram)
2. [Phase 0 — Request & Queue](#2-phase-0--request--queue)
3. [Phase 1 — Dependency Discovery](#3-phase-1--dependency-discovery)
4. [Phase 2 — Source Fetch (ALL_SOURCE / DBMS_METADATA)](#4-phase-2--source-fetch-all_source--dbms_metadata)
5. [Phase 3 — ANTLR4 Parsing](#5-phase-3--antlr4-parsing)
6. [Phase 4 — Call Graph Construction](#6-phase-4--call-graph-construction)
7. [Phase 5 — Table & Operation Collection](#7-phase-5--table--operation-collection)
8. [Phase 6 — Trigger & Metadata Resolution](#8-phase-6--trigger--metadata-resolution)
9. [Complexity Scoring](#9-complexity-scoring)
10. [Optional: Claude Enrichment](#10-optional-claude-enrichment)
11. [Persistence & Output Layout](#11-persistence--output-layout)
12. [Two Analysis Engines](#12-two-analysis-engines)
13. [Tips](#13-tips)

---

## 1. Flow Diagram

<div class="um-flow">

<div class="um-flow-start">User submits analysis request — Project / Environment / Owner / Object name + type</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-upload">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">0</span>
    POST /api/parser/analyze — Request &amp; Queue
  </div>
  <div class="um-flow-phase-body">
    <ul>
      <li>Validates <code>entryPoint</code> is present</li>
      <li>Submits <strong>QueueJob (Type=PARSER_ANALYSIS)</strong></li>
      <li>Returns <code>{"status":"queued", "jobId":…, "entryPoint":…}</code> immediately</li>
      <li>Browser polls SSE for progress — dispatches to <strong>FastAnalysisService</strong> (default) or <strong>AnalysisService</strong> (legacy)</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-db">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">1</span>
    Dependency Discovery — OracleDictionaryService
  </div>
  <div class="um-flow-phase-body">
    <strong>getRecursiveDependencies()</strong> — queries <code>ALL_DEPENDENCIES</code> using BFS up to depth 20:
    <ul>
      <li>Only PL/SQL types followed: <code>PACKAGE</code>, <code>PACKAGE BODY</code>, <code>PROCEDURE</code>, <code>FUNCTION</code>, <code>TRIGGER</code></li>
      <li>Non-PL/SQL deps (tables, views, sequences) recorded but not expanded</li>
      <li>FastAnalysisService: bulk query per BFS level — AnalysisService: recursive per-object</li>
      <li>Returns <code>Map&lt;objectKey, List&lt;DependencyRecord&gt;&gt;</code></li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-source">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">2</span>
    Source Fetch — ALL_SOURCE / DBMS_METADATA
  </div>
  <div class="um-flow-phase-body">
    For each discovered object — <strong>fetchSource(conn, owner, name, type)</strong>:
    <ul>
      <li>Primary: <code>SELECT TEXT FROM ALL_SOURCE WHERE OWNER=? AND NAME=? AND TYPE=? ORDER BY LINE</code></li>
      <li>Fallback: <code>DBMS_METADATA.GET_DDL</code> for packages/triggers not in ALL_SOURCE</li>
      <li>Type hint map avoids retrying all 5 types when type is already known</li>
      <li><strong>Parallel fetch</strong> — up to 8 threads via connection pool (configurable via <code>sourceFetchThreads</code>)</li>
      <li><strong>Wrapped source detection</strong> — <code>isWrappedSource()</code> checks first 300 chars for Oracle WRAPPED keyword; wrapped bodies skipped with warning</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-antlr">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">3</span>
    ANTLR4 Parsing — PlSqlAnalyzerParserService
  </div>
  <div class="um-flow-phase-body">
    Grammar: custom <code>PlSqlAnalyzer.g4</code> compiled at build time → <code>PlSqlAnalyzerLexer / PlSqlAnalyzerParser</code>
    <ul>
      <li><strong>Two-pass prediction:</strong> SLL (fast) first; fall back to full LL on ParseCancellationException</li>
      <li><strong>PlSqlAnalyzerVisitorImpl</strong> — single-pass AST walk collects per <em>PlsqlUnit</em>:</li>
      <li>Per <em>PlsqlProcedure</em>: name, type, startLine/endLine, parameters, procedureCalls, tableReferences, variables, cursors, exceptionHandlers, dynamicSql (EXECUTE IMMEDIATE)</li>
      <li>PlsqlUnit-level: sequences, joinInfo (JSqlParser), parseErrors</li>
      <li><strong>PlSqlAnalysisVisitor</strong> (second grammar) — detailed flow analysis: CTE alias exclusion, MERGE target tracking, loop nesting depth, BUILTIN package classification</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-calltree">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">4</span>
    Call Graph Construction — CallGraphBuilder
  </div>
  <div class="um-flow-phase-body">
    <strong>buildGraph(List&lt;PlsqlUnit&gt;)</strong> — two passes:
    <ul>
      <li><strong>Pass 1 — register nodes:</strong> one CallGraphNode per PlsqlProcedure; ID format: <code>{SCHEMA}.{PACKAGE}.{PROC}</code>; overloaded procs get param signature suffix</li>
      <li><strong>Pass 2 — resolve edges:</strong> match ProcedureCall callees by name in node registry; cross-schema via packageSchemaMap; INTERNAL calls resolved first; unresolved → placeholder nodes</li>
      <li>Edge types: <code>INTERNAL</code> (same package), <code>EXTERNAL</code> (different package/schema), <code>BUILTIN</code> (DBMS_*, UTL_*), <code>UNRESOLVED</code></li>
      <li><code>getRootNodes()</code> returns nodes with no incoming edges — entry points for UI call flow tree</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-db">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">5</span>
    Table &amp; Operation Collection
  </div>
  <div class="um-flow-phase-body">
    <strong>TableOperationCollector.collect(units)</strong> — builds from TableReference objects:
    <ul>
      <li>Each reference carries: tableName, schemaName, operation (SELECT/INSERT/UPDATE/DELETE/MERGE/TRUNCATE), lineNumber</li>
      <li>Dedup key: <code>TABLE_NAME|PROC_NAME|OPERATION|LINE</code> (uppercased)</li>
      <li><strong>JSqlParser</strong> — secondary pass catches complex SQL patterns (CTEs, subquery joins) missed by PL/SQL grammar</li>
      <li><strong>SequenceOperationCollector</strong> — NEXTVAL/CURRVAL references</li>
      <li><strong>JoinOperationCollector</strong> — JOIN types and joined table pairs</li>
      <li><strong>CursorOperationCollector</strong> — explicit cursor definitions and OPEN/FETCH loops</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-index">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">6</span>
    Trigger &amp; Metadata Resolution
  </div>
  <div class="um-flow-phase-body">
    <strong>TriggerResolver.resolve()</strong>:
    <ul>
      <li>Queries <code>ALL_TRIGGERS</code> for every table touched by the call graph</li>
      <li>Fetches trigger source, parses it, adds trigger nodes/edges to the call graph</li>
      <li>A trigger fired by an INSERT in procedure A appears as a downstream node in A's call tree</li>
      <li>Parallel: up to 4 trigger threads</li>
    </ul>
    <strong>Bulk table metadata</strong> — fetched in 2–3 SQL queries against <code>ALL_TAB_COLUMNS</code>, <code>ALL_CONSTRAINTS</code>, <code>ALL_INDEXES</code>; attached to <code>AnalysisResult.tableMetadata</code> (powers the Table Detail modal)
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-complexity">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">★</span>
    Complexity Scoring
  </div>
  <div class="um-flow-phase-body">
    Computed per procedure from parsing information:
    <ul>
      <li><strong>Cyclomatic-style branches</strong> — IF, CASE, WHEN clauses</li>
      <li><strong>Loop nesting depth</strong> — nested FOR/WHILE/LOOP constructs</li>
      <li><strong>Exception handlers</strong> — each WHEN clause in EXCEPTION block</li>
      <li><strong>Dynamic SQL</strong> — EXECUTE IMMEDIATE statements</li>
      <li><strong>Cursor operations</strong> — explicit cursor OPEN/FETCH loops</li>
      <li><strong>External calls</strong> — EXTERNAL call edges leaving the package</li>
    </ul>
    Scores stored on CallGraphNode — surfaced in the Complexity tab of the UI
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-persist">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">7</span>
    Persistence — data/plsql-parse/{analysisName}/
  </div>
  <div class="um-flow-phase-body">
    All output written under <code>data/plsql-parse/{analysisName}/</code> (configurable via <code>app.data-dir</code>):
    <ul>
      <li><code>index.json</code> — summary: entry point, analysisMode, timestamps, stats</li>
      <li><code>graph.json</code> — CallGraph: all nodes and edges</li>
      <li><code>tables.json</code> — TableOperationSummary map (table → ops + access details)</li>
      <li><code>sequences.json</code> / <code>joins.json</code> / <code>cursors.json</code></li>
      <li><code>source/</code> — raw source text per object (<code>schema.name → .sql</code>)</li>
      <li><code>nodes/</code> — per-node detail JSON (one file per procedure/function; lazy-loaded by UI)</li>
      <li><code>chat/history.jsonl</code> — analysis-scoped chat history (append-only)</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-optional">
  <div class="um-flow-optional-label">Optional — Claude enrichment requested</div>
  <strong>Claude Enrichment</strong> — AnalysisResult summary serialised into prompt → Claude CLI invoked via subprocess → corrections merged into stored analysis JSON
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-output">
  <div class="um-flow-output-title">✓ Analysis Complete — Output Files</div>
  <ul>
    <li><code>index.json</code> — entry point, mode, timestamps, stats</li>
    <li><code>graph.json</code> — full call graph (nodes + edges)</li>
    <li><code>tables.json</code> — table → operations + access details</li>
    <li><code>nodes/{id}.json</code> — per-procedure detail (lazy-loaded)</li>
    <li><code>source/{schema.name}.sql</code> — raw PL/SQL source</li>
    <li><code>chat/history.jsonl</code> — analysis-scoped chat history</li>
  </ul>
</div>

</div>

---

## 2. Phase 0 — Request & Queue

**Controller:** `ParserAnalysisController` (`POST /api/parser/analyze`)

| Request Field | Required | Description |
|---|---|---|
| `entryPoint` | Yes | Object name (e.g., `PKG_ORDER` or `PROC_PROCESS_ORDER`) |
| `owner` | No | Schema/owner override (default from connection config) |
| `objectType` | No | `PACKAGE`, `PROCEDURE`, `FUNCTION`, `TRIGGER` |
| `project` | No | Project name from `plsql-config.yaml` |
| `environment` | No | Environment name (maps to a connection) |

The controller submits a `QueueJob (Type=PARSER_ANALYSIS)` and returns immediately with a job ID. The browser polls SSE for progress. The queue dispatches to `FastAnalysisService` (default) or `AnalysisService` (legacy).

---

## 3. Phase 1 — Dependency Discovery

**Service:** `OracleDictionaryService.getRecursiveDependencies()`

Queries `ALL_DEPENDENCIES` (Oracle's dependency catalog view) using BFS up to depth 20. Only PL/SQL object types are followed: `PACKAGE`, `PACKAGE BODY`, `PROCEDURE`, `FUNCTION`, `TRIGGER`. Non-PL/SQL dependencies (tables, views, sequences) are recorded but not expanded further.

`FastAnalysisService` performs this in bulk (one query per BFS level), while `AnalysisService` performs it recursively per object. The fast path is used by default.

---

## 4. Phase 2 — Source Fetch (ALL_SOURCE / DBMS_METADATA)

**Service:** `OracleDictionaryService.fetchSource()`

```sql
SELECT TEXT FROM ALL_SOURCE
WHERE OWNER = ? AND NAME = ? AND TYPE = ?
ORDER BY LINE
```

Lines are concatenated into a single source string. If `ALL_SOURCE` returns nothing (e.g., for system packages or wrapped objects), `DBMS_METADATA.GET_DDL` is attempted as a fallback.

**Wrapped source detection:** `PlSqlParserEngine.isWrappedSource()` checks the first 300 characters for the Oracle `WRAPPED` keyword or long hex strings. Wrapped bodies are skipped with a warning — they cannot be parsed.

**Parallel fetch:** `FastAnalysisService` uses a connection pool (up to 8 threads, configurable via `sourceFetchThreads`). Type hints from the dependency graph reduce trial-and-error fetching.

---

## 5. Phase 3 — ANTLR4 Parsing

**Services:** `PlSqlAnalyzerParserService` → `PlSqlAnalyzerVisitorImpl`  
**Grammar:** Custom `PlSqlAnalyzer.g4` (compiled at build time by `antlr4-maven-plugin` into `PlSqlAnalyzerLexer` / `PlSqlAnalyzerParser`)

The grammar covers:
- Package and subprogram declarations (`CREATE [OR REPLACE] PACKAGE [BODY]`)
- Procedure/function signatures and bodies
- DML statements: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`
- Cursor declarations and OPEN/FETCH/CLOSE
- `EXECUTE IMMEDIATE` (dynamic SQL)
- Exception handlers (`EXCEPTION WHEN … THEN`)
- Sequence references (`seq.NEXTVAL`, `seq.CURRVAL`)
- Loop constructs (FOR, WHILE, LOOP) for complexity tracking

**Two-pass prediction:** SLL prediction mode is tried first for speed. If a `ParseCancellationException` occurs, the parser resets and retries with full LL mode. This handles ambiguous grammar rules in complex PL/SQL without sacrificing performance on common cases.

**Standalone procedure deduplication:** `PlSqlParserEngine.trimDuplicateDefinition()` strips the spec section from standalone procedure source to avoid parsing the same signature twice.

The second grammar (`PlSqlParser` / `PlSqlAnalysisVisitor` in `com.plsql.parser`) handles the more detailed flow analysis used by the Flow POC tab.

---

## 6. Phase 4 — Call Graph Construction

**Service:** `CallGraphBuilder.buildGraph()`

Nodes represent individual procedures/functions. Node ID format: `{SCHEMA}.{PACKAGE}.{PROC}` (or `{SCHEMA}.{PROC}` for standalone objects). Overloaded procedures (same name, different parameters) get IDs suffixed with a parameter signature hash.

Edge types:

| Type | Meaning |
|---|---|
| `INTERNAL` | Callee is in the same package |
| `EXTERNAL` | Callee is in a different package or schema |
| `BUILTIN` | Callee is an Oracle built-in (`DBMS_*`, `UTL_*`, etc.) |
| `UNRESOLVED` | Callee could not be matched to any fetched unit |

`CallGraph.getRootNodes()` returns nodes with no incoming edges — these are the entry points for display in the UI's call flow tree.

---

## 7. Phase 5 — Table & Operation Collection

**Service:** `TableOperationCollector`

Built from `TableReference` objects emitted by the ANTLR visitor. Each reference carries:
- `tableName`, `schemaName` — resolved from the AST
- `operation` — `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`
- `lineNumber` — source line of the DML statement

`TableOperationSummary` aggregates per table: the set of operations performed and a list of `TableAccessDetail` (which procedure, which line). JSqlParser is used as a secondary pass to catch complex SQL patterns (CTEs, subquery joins) that the PL/SQL grammar may not have caught.

Deduplication key: `TABLE_NAME|PROC_NAME|OPERATION|LINE` (uppercased) prevents double-counting when both ANTLR and JSqlParser find the same statement.

---

## 8. Phase 6 — Trigger & Metadata Resolution

**Service:** `TriggerResolver`

After table operations are collected, `TriggerResolver` queries `ALL_TRIGGERS` for each table in the result set. Discovered trigger source is fetched, parsed, and added as additional nodes/edges in the call graph. This means a trigger fired by an `INSERT` in procedure A will appear as a downstream node in A's call tree.

Bulk table metadata (column names, data types, primary key columns, indexes) is fetched in 2-3 SQL queries against `ALL_TAB_COLUMNS`, `ALL_CONSTRAINTS`, and `ALL_INDEXES`, and attached to `AnalysisResult.tableMetadata`. This powers the Table Detail modal in the UI.

---

## 9. Complexity Scoring

Complexity is computed per procedure from information gathered during parsing:

| Factor | Contribution |
|---|---|
| Cyclomatic-style branches | `IF`, `CASE`, `WHEN` clauses |
| Loop nesting depth | Nested `FOR`/`WHILE`/`LOOP` constructs |
| Exception handlers | Each `WHEN` clause in `EXCEPTION` block |
| Dynamic SQL | `EXECUTE IMMEDIATE` statements |
| Cursor operations | Explicit cursor OPEN/FETCH loops |
| External calls | `EXTERNAL` call edges leaving the package |

Scores are stored on `CallGraphNode` and surfaced in the Complexity tab of the UI.

---

## 10. Optional: Claude Enrichment

If Claude enrichment is requested for a PL/SQL analysis:
1. The `AnalysisResult` summary (entry point, call graph summary, table operations) is serialised into a prompt.
2. Claude CLI is invoked via subprocess; the response is parsed for corrections or business-context annotations.
3. Corrections are merged into the stored analysis JSON.

---

## 11. Persistence & Output Layout

All output is written under `data/plsql-parse/{analysisName}/` (configurable via `app.data-dir`):

```
data/plsql-parse/
└── {analysisName}/
    ├── index.json          — summary: entry point, analysisMode, timestamps, stats
    ├── graph.json          — CallGraph: all nodes and edges
    ├── tables.json         — TableOperationSummary map (table → ops + access details)
    ├── sequences.json      — sequence references
    ├── joins.json          — join operation summaries
    ├── cursors.json        — cursor operation summaries
    ├── source/             — raw source text per object (schema.name → .sql)
    ├── nodes/              — per-node detail JSON (one file per procedure/function)
    └── chat/
        └── history.jsonl   — analysis-scoped chat history
```

The UI loads `index.json` first, then lazily fetches individual node files on demand. This avoids loading the full graph into memory for large analyses with hundreds of procedures.

---

## 12. Two Analysis Engines

The system contains two analysis service implementations:

| Engine | Class | Strategy |
|---|---|---|
| **FastAnalysisService** | `com.plsqlanalyzer.analyzer.service.FastAnalysisService` | BFS bulk dependency discovery (1 query/level), bulk source fetch (1-2 queries), parallel ANTLR parse. Default path. |
| **AnalysisService** | `com.plsqlanalyzer.analyzer.service.AnalysisService` | Recursive per-object dependency fetch, sequential or parallel source fetch. Used as fallback or for special cases. |

Both share the same `CallGraphBuilder`, `TableOperationCollector`, and `TriggerResolver` — only the orchestration and DB query strategy differ.

`FastAnalysisService` also performs iterative refinement: after the initial parse, any new call targets discovered in parsed code (not in `ALL_DEPENDENCIES`) are fetched and parsed in subsequent iterations (up to `MAX_ITERATIONS=5`).

---

## 13. Tips

- **Wrapped source:** If the analysis shows zero procedures for a package, the body is likely Oracle-wrapped. Check the progress log for `"Skipping wrapped source"` messages.
- **Deep dependency trees:** The BFS cap is `MAX_DEPTH=20`. If a call chain is longer (rare), the deepest objects will appear as `UNRESOLVED` edges. Adjust `MAX_DEPENDENCY_DEPTH` in `AnalysisService` if needed.
- **Schema/owner:** Always specify the correct `owner`. If `owner` is omitted, the connection's default schema is used, which may miss objects in other schemas.
- **Parallel connections:** `FastAnalysisService` creates up to 8 concurrent Oracle connections for source fetching. Ensure the Oracle user has sufficient session quota (`CREATE SESSION` with `SESSIONS_PER_USER` set appropriately in the profile).
- **ALL_SOURCE vs DBMS_METADATA:** `ALL_SOURCE` is always tried first (it's faster). `DBMS_METADATA.GET_DDL` is used as a fallback for objects that exist in `ALL_OBJECTS` but not in `ALL_SOURCE` (e.g., objects owned by SYS).
- **Triggers:** `TriggerResolver` only fetches triggers on tables that appear in the current analysis. Triggers on tables outside the call graph are not included.
- **Re-analysis:** Re-submitting the same entry point overwrites the previous output folder. Chat history is not overwritten — it is append-only.
- **JSqlParser and complex SQL:** JSqlParser parses SQL embedded in PL/SQL as standard SQL. It may misparse PL/SQL-specific syntax extensions (e.g., `BULK COLLECT INTO`). The ANTLR visitor result takes precedence; JSqlParser adds supplementary detail only.
