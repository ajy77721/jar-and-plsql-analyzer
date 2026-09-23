# PLAN-02 — PL/SQL Analyzer JSON Artifact Inventory (all 5 dialects)

Evidence base:

- Code: `plsql-parser/src/main/java/com/plsql/parser/flow/**`, `plsql-parser/src/main/java/com/plsql/parser/model/**`,
  `unified-web/src/main/java/com/plsqlanalyzer/web/parser/**`, `analyzer-storage/src/main/java/com/analyzer/storage/**`.
- Real data: `../data-backup-20260902-054631/` — 10 analysis directories under `plsql-parse/`, which are
  **5 unique analyses × 2** (each has an identical `_20260723_HHMMSS` archive twin). All 5 dialects present,
  identified from `api/index.json.jdbcUrl`:

| Analysis dir | Dialect | project / env | nodes | edges | tables |
|---|---|---|---|---|---|
| `ERP_USP_RUN_CYCLE__WL2ZTC` | **Oracle** (`jdbc:oracle:thin:@//localhost:1521/FREEPDB1`) | OracleDemo / local | 52 | 2258 | 42 |
| `ERP_USP_RUN_CYCLE__ROS0F8` | **Postgres** (`jdbc:postgresql://…/analyzerdb`) | PostgresDemo / local | 114 | 3295 | 114 |
| `ERP_USP_RUN_CYCLE__RQZVED` | **DB2** (`jdbc:db2://…/anlzdb`) | Db2Demo / local | 64 | 428 | 100 |
| `ERP_USP_RUN_CYCLE__JZFCKN` | **MySQL** (`jdbc:mysql://localhost:3307/analyzerdb`) | MysqlDemo / local | 69 | 652 | 232 |
| `USP_RUNMRPCYCLE__CIJ65E` | **SQL Server** (`jdbc:sqlserver://…;databaseName=analyzer-db`) | SqlServerDemo / local | 57 | 337 | 85 |

The dialect above is read from `api/index.json.jdbcUrl` (authoritative). It was independently
cross-checked by fingerprinting each analysis's `statementSummary` vocabulary (§4.3) — the two methods
agree on all five. Note there is **no `dbType`/`dialect` field anywhere inside the JSON**; the only
in-artifact discriminators are `jdbcUrl`/`project` (present because `AnalysisService.patchConnectionMeta`
writes them, `AnalysisService.java:918-941`) and, in the current layout, the DBTYPE path segment.
**A database schema must store `db_type` explicitly.**

> NOTE ON THE LIVE TREE: `jar-plsql-analyzer/data/plsql/` exists but is **empty** — no
> `<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>` nesting is materialised on this machine right now. The nested
> layout is therefore confirmed only from code (`AnalysisPaths.java:13-15,138-144`), not from data.

> NOTE ON THE BACKUP LAYOUT: the backup directories are **flat** (`plsql-parse/<OBJECT>__<HASH>/…`), i.e. the
> legacy pre-nesting layout that `AnalysisPaths` explicitly still supports
> (`AnalysisPaths.java:25-26`: *"Scopeless runs … fall back to a bare OBJECT name and a flat folder,
> preserving backwards compatibility with older data"*). The **current** layout is
> `plsql-parse/<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>/` (`AnalysisPaths.java:13-15`). The per-analysis
> *file set* is identical either way — only the prefix differs. The 5 analyses above carry
> `project`/`environment` inside `api/index.json`, so they were run with scope but written flat.

---

## 0. Identity ↔ path mapping (critical for schema design)

`unified-web/.../service/AnalysisPaths.java` is the single source of truth.

| Concept | Code | Value |
|---|---|---|
| Delimiter | `AnalysisPaths.java:31` | `public static final char SEP = '~'` |
| Segment sanitiser | `AnalysisPaths.java:36-44` | uppercase; `.`,`/`,`\`,`~` → `_`; then `[^A-Za-z0-9_\-]` → `_` |
| Identity name | `AnalysisPaths.java:50-58` | `name(dbType, project, env, object)` → `DB2~Db2Demo~local~ERP_USP_RUN_CYCLE` (empty context parts dropped; object defaults to `UNNAMED`) |
| Cache scope | `AnalysisPaths.java:61-68` | `scope(dbType, project, env)` → `DB2~Db2Demo~local` (no object) |
| Object leaf | `AnalysisPaths.java:71-75` | substring after last `~` |
| Parse-tree root | `AnalysisPaths.java:132` | `PARSE_ROOT = "plsql-parse"` |
| Claude sidecar root | `AnalysisPaths.java:135` | `CLAUDE_ROOT = "plsql"` |
| Identity → dir key | `AnalysisPaths.java:138-144` | `dirKey(name)`: `split('~')`, each token becomes one key segment under `plsql-parse/` |
| File in analysis | `AnalysisPaths.java:154-160` | `resolveKey(name, "api/index.json")` |
| Dir key → identity | `AnalysisPaths.java:182-186` | `nameOfKey`: drop leading `plsql-parse`, rejoin with `~` |

So the mapping is a **pure, reversible, lossless 1:1**:

```
identity   DB2 ~ Db2Demo ~ local ~ ERP_USP_RUN_CYCLE
path       plsql-parse / DB2 / Db2Demo / local / ERP_USP_RUN_CYCLE /
```

`'~'` was chosen because it is RFC-3986 unreserved (safe in `@PathVariable` and `encodeURIComponent`) and
the sanitiser guarantees it never appears inside a segment (`AnalysisPaths.java:19-23`).

**Schema-design consequence:** the natural composite primary key of an analysis is
`(db_type, project, environment, object_name)`. The `~` string is only a URL/serialisation form of that
tuple — store the four columns, derive the string. Note it is **not** globally unique over time: reruns
overwrite in place, and archived reruns get a `_yyyyMMdd_HHmmss` suffix appended to the OBJECT segment
(observed: `ERP_USP_RUN_CYCLE__WL2ZTC_20260723_153725`), which means the archive timestamp currently
lives *inside* the object segment. A real schema should add an explicit `run_id` / `archived_at` column
rather than reproduce that.

The schema-resolver cache mirrors the same nesting minus the object
(`SchemaResolver.java:56-65` splits `cacheScope` on `~`): `cache/schema-resolver/<DBTYPE>/<PROJECT>/<ENV>/`.

The Claude sidecar does **not** nest: `plsql/<full-identity-name>/…` (`AnalysisPaths.java:163-165`).

---

## 1. Summary table — every artifact

Sizes are **observed across the 5 real analyses**. Worst case is justified in each section.

| # | Key (relative to data root) | Written by | Cardinality | Observed size | Worst case | Bounded? | Dialect-specific? |
|---|---|---|---|---|---|---|---|
| 1 | `plsql-parse/…/<OBJ>/progress.json` | `ChunkedFlowWriter.java:1423-1425` | 1 / analysis | 1.5–4 KB | ~2 MB | ✗ (grows with node count) | no |
| 2 | `plsql-parse/…/<OBJ>/tables.json` | `ChunkedFlowWriter.java:136-138` | 1 / analysis | 36–106 KB | ~50 MB | ✗ | no |
| 3 | `plsql-parse/…/<OBJ>/chunks/chunk_NNNN.json` | `ChunkedFlowWriter.java:124-128` | 1 / node | 314 B – 217 KB (avg 12–46 KB) | ~5 MB / chunk | ✗ per chunk, but small | no |
| 4 | `plsql-parse/…/<OBJ>/edges/edges_NNNN.json` | `ChunkedFlowWriter.java:130-134` | 1 / chunk-with-edges | 210 B – 57 KB | ~2 MB | ✗ | no |
| 5 | `plsql-parse/…/<OBJ>/edges.json` | `ChunkedFlowWriter.java:173` | 1 / analysis | 75–826 KB | **~200 MB** | ✗✗ | no |
| 6 | `plsql-parse/…/<OBJ>/flow.json` | `ChunkedFlowWriter.java:206-207`, `:214-216` | 1 / analysis | **2.27–3.30 MB** | **~300 MB** | ✗✗ | no |
| 7 | `plsql-parse/…/<OBJ>/api/index.json` | `ChunkedFlowWriter.java:523`; patched `TableValidationService.java:745`, `AnalysisService.java:934` | 1 / analysis | 72–152 KB | ~30 MB | ✗ | Oracle-only `totalTables` patch |
| 8 | `plsql-parse/…/<OBJ>/api/nodes/<safeId>.json` | `ChunkedFlowWriter.java:715` | 1 / node | 282 B – **370 KB** (avg 42–78 KB) | ~40 MB / node | ✗ | no |
| 9 | `plsql-parse/…/<OBJ>/api/tables/index.json` | `ChunkedFlowWriter.java:1119`; **rewritten** by the 5 enrichers | 1 / analysis | 152–307 KB | **~500 MB** | ✗✗ | **YES — biggest divergence** |
| 10 | `plsql-parse/…/<OBJ>/api/call_graph.json` | `ChunkedFlowWriter.java:1206` | 1 / analysis | 84–263 KB | ~60 MB | ✗ | no |
| 11 | `plsql-parse/…/<OBJ>/api/node_id_map.json` | `ChunkedFlowWriter.java:1231` | 1 / analysis | ~9 KB | ~5 MB | ✗ | no |
| 12 | `plsql-parse/…/<OBJ>/resolver/q1_object_resolution.json` | `ChunkedFlowWriter.java:1411` | 1 / analysis | ≤ 60 KB (dir total) | ~20 MB | ✗ | **Oracle only populated** |
| 13 | `…/resolver/q4_direct_dependencies.json` | `ChunkedFlowWriter.java:1412` | 1 / analysis | (in 60 KB) | ~50 MB | ✗ | **Oracle only** |
| 14 | `…/resolver/q3_reverse_dependencies.json` | `ChunkedFlowWriter.java:1413` | 1 / analysis | `[]` observed | ~50 MB | ✗ | **Oracle only** |
| 15 | `…/resolver/q1_ambiguous_objects.json` | `ChunkedFlowWriter.java:1414` | 1 / analysis | small | ~10 MB | ✗ | **Oracle only** |
| 16 | `plsql-parse/…/<OBJ>/sources/<file>` (**not JSON**) | `ChunkedFlowWriter.java:140-142` | 1 / source object | 36 B – 94 KB; 712 KB–1.18 MB total | ~10 MB / file | ✗ | ext differs: `.prc/.fnc/.pkb/.sql` |
| 17 | `plsql-parse/…/<OBJ>/.parse-cache/<md5>.json` | `ParseResultCache.java:57-58` | 1 / distinct source text | 1.6 MB total | ~100 MB total | ✗ | no — **derived/cacheable** |
| 18 | `plsql-parse/…/<OBJ>/.node-cache/<NAME>_<md5>.json` | `NodeParseCache.java:61-62` | 1 / visited node | 2.4 MB total | ~200 MB total | ✗ | no — **derived/cacheable** |
| 19 | `cache/schema-resolver/<DB>/<PROJ>/<ENV>/*.tsv` (**TSV**) | `SchemaResolver.java:1001-1034` | 6 files / connection scope | 40 KB / scope | ~200 MB | ✗ | **Oracle only populated** |
| 20 | `plsql/<identity>/metadata.json` | Claude sidecar (`AnalysisPaths.java:168-174`) | 1 / analysis | ~250 B | ~1 KB | ✓ | no |
| 21 | `plsql/<identity>/analysis_claude.json` | Claude sidecar | 1 / analysis | 138 KB | ~50 MB | ✗ | no |
| 22 | `plsql-env-config.yaml` (**YAML, local disk only, credentials**) | `AnalysisService.java:903` | 1 / run | small | small | ✓ | no |

**Legend:** ✗✗ = already multi-MB on a 52-node toy analysis; guaranteed to break the 16 MB BSON limit on
a real workload. ✗ = unbounded in principle but small today.

---

## 2. The chunking scheme `ChunkedFlowWriter` already uses

This is the boundary the database design should reuse verbatim.

### 2.1 Constructor: fixed key set (`ChunkedFlowWriter.java:39-57`)

```java
this.chunksKey    = buildKey.resolve("chunks");        // :42
this.sourcesKey   = buildKey.resolve("sources");       // :43
this.edgesDirKey  = buildKey.resolve("edges");         // :44
this.progressKey  = buildKey.resolve("progress.json"); // :45
this.edgesFileKey = buildKey.resolve("edges.json");    // :46
this.tablesKey    = buildKey.resolve("tables.json");   // :47
writeString(tablesKey, "{}");                          // :55  — truncate on start
writeProgress();                                       // :56
```

`buildKey` is the analysis directory key. Everything is `ArtifactKey`-addressed, so the same code writes
to local disk or S3 with no branching (`ChunkedFlowWriter.java:17-21`).

### 2.2 The chunk boundary is **one node per chunk** — NOT a byte/size budget

```java
public void writeChunk(FlowNode node) throws IOException {   // :124
    int idx = chunkCounter.incrementAndGet();                // :125  1-based, monotonic
    String fileName = String.format("chunk_%04d.json", idx); // :126  4-digit zero-pad → 9999 max
    writeJson(chunksKey.resolve(fileName), node);            // :127
}
```

Confirmed in data: `chunks/` file count == `flowNodes` count == `api/nodes/` count, exactly, in all 5
analyses (52/52/52, 114/114/114, 64/64/64, 69/69/69, 57/57/57).

**The chunk index is the crawl-visit order, and it is the same ordinal as `FlowNode.order`.**
`chunk_%04d` caps the scheme at 9999 nodes — a real hard limit worth recording.

### 2.3 The edge boundary is **one file per chunk index** (sparse)

```java
public void appendEdges(List<FlowEdge> newEdges, int chunkIdx) throws IOException { // :130
    if (newEdges == null || newEdges.isEmpty()) return;      // :131  — sparse: no file if no edges
    String fileName = String.format("edges_%04d.json", chunkIdx);  // :132
    writeJson(edgesDirKey.resolve(fileName), newEdges);      // :133  whole-file overwrite, not append
}
```

Despite the name `appendEdges`, this is a **whole-file write**. Observed: 32 edge files for 52 chunks
(Oracle), 96/114 (Postgres), 49/64 (DB2), 56/69 (MySQL), 45/57 (SQL Server) — i.e. the numbering is
sparse and gapped (`edges_0001, 0026, 0034, 0035, 0036, 0042…0053`).

`edges_NNNN.json` is a **bare JSON array** of `FlowEdge`, not an object.

### 2.4 The merge step (`mergeChunks`, `ChunkedFlowWriter.java:153-212`)

Reads every `chunk_*` and `edges_*` back, name-sorted (`listSorted`, `:85-93`), and materialises:

- `result.flowNodes` = all chunks concatenated (`:162-166`)
- `result.callGraph` = all edge files concatenated (`:168-172`)
- `edges.json` = the same flat edge array written out whole (`:173`)
- `result.allTables` = `tables.json`'s map values (`:175-182`)
- rollup counters recomputed over all nodes (`:184-204`)
- `flow.json` written (`:206-207`)

**This is where boundedness dies.** The chunk scheme exists to keep the *writer's* memory flat, but
`mergeChunks` then re-materialises everything into one 2–3 MB `flow.json` on a toy dataset.

### 2.5 Recommended reuse

The database should adopt exactly these boundaries and simply **not perform the merge**:

| Chunk-writer boundary | Mongo collection / SQL table |
|---|---|
| `chunk_NNNN.json` (1 FlowNode) | one `flow_node` document, `_id = (analysis_id, order)` |
| `edges_NNNN.json` (edges emitted at chunk NNNN) | `flow_edge` rows, `chunk_idx = NNNN` |
| `api/nodes/<safeId>.json` (1 node detail) | one `node_detail` document, `_id = (analysis_id, node_id)` |
| `tables.json` map entry | one `table_usage` row per `SCHEMA.TABLE` |
| `api/tables/index.json` `tables[i]` | one `table_meta` document + child `column`/`index`/`constraint`/`trigger` rows |
| `flow.json` / `edges.json` | **do not store** — derived views (§8) |

---

## 3. Per-artifact detail

### 3.1 `plsql-parse/<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>/progress.json`

- **Writer:** `ChunkedFlowWriter.writeProgress()` — `ChunkedFlowWriter.java:1423-1425` (`writeJson(progressKey, progressMap)`).
- **Purpose:** live per-node crawl status so the UI can show progress while the crawl runs.
- **Cardinality:** 1 per analysis.
- **Shape:** a flat `Map<String,String>` — **no wrapper object**.

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `<visitKey>` (dynamic key) | string | no | Visit key. Observed forms: `ERP_USP_RUN_CYCLE` (entry point, unqualified) and `ANALYZER$ERP_USP_WRITE_AUDIT_TRAIL` (`SCHEMA$OBJECT`). Package members use `SCHEMA$PKG$OBJ`. |
| value | string enum-ish | no | One of `PENDING` (`:105`), `PROCESSING` (`:109`), `DONE` (`:114`), or `"FAILED: " + error` (`:120`) — note **FAILED is a prefix, not a bare token**, and carries the free-text error message inline. |

- **Write pattern:** **rewritten in full on every state transition** (`markPending`/`markProcessing`/`markDone`/`markFailed` each call `writeProgress()`, `:104-122`). O(n²) writes over a crawl.
- **Identity:** `(analysis_id, visit_key)`. `visit_key` → `flow_node.nodeId` (same `SCHEMA$PKG$OBJ` convention as `ChunkedFlowWriter.safeNodeId`, `:1427-1434`) but is **not guaranteed equal** — the entry point appears unqualified.
- **Size:** 1.5–4 KB observed (52–114 entries). Worst case ≈ 100 bytes × node count; 20 000 nodes → ~2 MB.
- **Read by:** no `ParserAnalysisController` endpoint reads it. **UNKNOWN — needs runtime check** whether a progress-polling endpoint exists outside `ParserAnalysisController`.
- **Source of truth?** No — derived from crawl state; discardable after completion.

---

### 3.2 `…/<OBJECT>/tables.json`

- **Writer:** `ChunkedFlowWriter.updateTables()` — `ChunkedFlowWriter.java:136-138`. Initialised to `"{}"` at `:55`.
- **Purpose:** running accumulator of every schema object touched, keyed `SCHEMA.TABLE`; the raw input to `flow.json.allTables`.
- **Cardinality:** 1 per analysis.
- **Shape:** `Map<String, SchemaTableInfo>` (`SchemaTableInfo.java:8-13`), keys like `ANALYZER.ERP_SALES_ORDER`.

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `<SCHEMA.TABLE>` | object | — | map key, schema-qualified |
| `.schema` | string | yes | owning schema (uppercase) |
| `.tableName` | string | no | object name |
| `.objectType` | string | yes | `TABLE` / `VIEW` / `MATERIALIZED_VIEW` / `SYNONYM` |
| `.operations` | array&lt;string&gt; | no (may be `[]`) | distinct DML/DQL verbs — observed `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CURSOR_REF`. Java type is `Set<String>` so order is insertion-order and **differs between `tables.json` and `flow.json.allTables`** (observed `["SELECT","UPDATE"]` vs `["UPDATE","SELECT"]`) |
| `.lines` | array&lt;int&gt; | no | source line numbers. **Contains duplicates** — Java type is `List<Integer>`, not a Set (observed `147` appearing twice) |
| `.referencedBy` | array&lt;string&gt; | no | node IDs that touch it (`Set<String>`, deduped) |

- **Write pattern:** truncated to `{}` at construction (`:55`), then **fully rewritten** on each `updateTables` call.
- **Identity:** `(analysis_id, schema, table_name)`. `referencedBy[]` → `flow_node.nodeId` (FK).
- **Size:** 36 KB (Oracle, 42 tables) → 106 KB (MySQL, 232 tables). `.lines` is the dominant term and is **quadratic in call-site count**. Worst case: 5000 tables × 2000 lines × 6 bytes ≈ 60 MB.
- **Read by:** `ChunkedFlowWriter` itself (`:175`, `:932-935`). No web endpoint reads it directly.
- **Duplication:** 100% duplicated into `flow.json.allTables` (`:181`), and partially into `api/tables/index.json`.

---

### 3.3 `…/<OBJECT>/chunks/chunk_NNNN.json`

- **Writer:** `ChunkedFlowWriter.writeChunk()` — `ChunkedFlowWriter.java:124-128`.
- **Purpose:** one fully-parsed `FlowNode` per crawled routine, streamed out so the crawler never holds the whole graph.
- **Cardinality:** **1 per node**, index = crawl order, 1-based, `%04d`.
- **Shape:** a serialised `FlowNode` (`plsql-parser/.../model/FlowNode.java:8-36`), Jackson `@JsonInclude(NON_EMPTY)` (`FlowNode.java:6`) so **empty collections / null / 0-length strings are omitted entirely**.

| Field | Type | Nullable/optional | Meaning |
|---|---|---|---|
| `nodeId` | string | optional | `SCHEMA$OBJ`, `SCHEMA$PKG$OBJ`, or `…$2`/`…$3` when deduped (`:236-249`) |
| `order` | int | omitted if 0 | crawl visit ordinal == chunk index |
| `depth` | int | omitted if 0 | recursion depth from entry point |
| `schema` | string | optional | owning schema |
| `packageName` | string | optional | present only for package members (9/52 in Oracle sample; **0 in all four non-Oracle samples**) |
| `objectName` | string | optional | routine name |
| `objectType` | string | optional | observed `PROCEDURE`, `FUNCTION`, `TRIGGER`, `TABLE` |
| `lineStart` / `lineEnd` | int | optional | span in `sources/<file>` |
| `linesOfCode` | int | optional | `lineEnd - lineStart` derived |
| `callCount` | int | optional | number of call sites |
| `readable` | boolean | default `true` (`FlowNode.java:30`) | false when the source could not be fetched |
| `message` | string | optional | error/skip reason when `readable=false` (2/52 Oracle) |
| `overloadParamCount` | int | default `-1` (`FlowNode.java:35`) | ≥0 only for overloaded package members |
| `autonomousTransaction` | boolean | default false | `PRAGMA AUTONOMOUS_TRANSACTION` — **Oracle-only semantics** (`FlowNode.java:36`) |
| `triggerEvent` / `triggerTable` / `triggerTiming` | string | optional | present only for `objectType=TRIGGER` |
| `statementSummary` | object&lt;string,int&gt; | optional | statement-type histogram, e.g. `{"SELECT":12,"IF":3}` |
| `calledBy` | array&lt;string&gt; | optional | caller node IDs |
| `externalPackageVarRefs` | array&lt;string&gt; | optional | `PKG.VAR` refs (Oracle package-variable concept) |
| `parameters[]` | array&lt;object&gt; | optional | `ParameterInfo.java:7-11`: `{name:string, dataType:string, direction:string, defaultValue:string?, line:int}` |
| `localVariables[]` | array&lt;object&gt; | optional | `VariableInfo.java:7-13`: `{name, dataType, scope, defaultValue?, constant:bool, line:int, sourcePackage?}` |
| `statements[]` | array&lt;object&gt; | optional | `StatementInfo.java:8-21`: `{type:string, line:int, lineEnd:int, sqlText:string, nestingDepth:int, indexVariable?, collectionName?, boundsExpression?, limitValue?, dmlType?, errorCode:int?, errorMessage?, savepointName?}` — **`sqlText` is the single largest text field in the whole system** |
| `calls[]` | array&lt;object&gt; | optional | `CallInfo.java:7-12`: `{type:string, packageName?, name:string, schema?, line:int, arguments:int}`. `type` ∈ `INTERNAL`/`EXTERNAL`/`BUILTIN` |
| `cursors[]` | array&lt;object&gt; | optional | `CursorInfo.java:8-15`: `{name, query:string, line:int, lineEnd:int, forLoop:bool, refCursor:bool, parameters:[ParameterInfo], tables:[string]}` |
| `dynamicSql[]` | array&lt;object&gt; | optional | `DynamicSqlInfo.java:8-25`: `{type, sqlExpression:string, line:int, unresolvedVarName?, crossProcSourceHint?, inferredOperationType?}` |
| `tableOperations[]` | array&lt;object&gt; | optional | `TableOperationInfo.java:8-17`: `{tableName, schema?, operation, line:int, objectType?, alias?, subqueryContext?, dbLink?, mergeClause?, joins:[JoinInfo]}` |
| `tableOperations[].joins[]` | array&lt;object&gt; | optional | `JoinInfo.java:7-18`: `{joinType, joinedTable, joinedTableAlias?, leftTable?, leftTableAlias?, condition:string, line:int, queryId:int}` |
| `exceptionHandlers[]` | array&lt;object&gt; | optional | `ExceptionHandlerInfo.java:8-11`: `{exceptionName?, line:int, lineEnd:int, statementsCount:int}` |

- **Write pattern:** written **once** per node, never revisited.
- **Identity:** `(analysis_id, chunk_index)`; also `(analysis_id, nodeId)` — but note `nodeId` uniqueness is only enforced later, in `writeApiFiles` (`:236-249`), so **within `chunks/` two nodes CAN share a `nodeId`**.
- **Size:** min 314 B, max **216 512 B**, avg 12–46 KB. Sum 1.4–2.4 MB per analysis. Worst case: one very long package body node — 5 MB is realistic (`statements[].sqlText` dominates).
- **Read by:** only `ChunkedFlowWriter.mergeChunks` (`:163-165`). Not exposed via HTTP.
- **Source of truth?** **Yes** — this is the primary parser output. `flow.json`, `api/nodes/*`, `api/index.json` are all derived from it.

---

### 3.4 `…/<OBJECT>/edges/edges_NNNN.json`

- **Writer:** `ChunkedFlowWriter.appendEdges()` — `ChunkedFlowWriter.java:130-134`.
- **Purpose:** the call edges discovered while processing chunk `NNNN`.
- **Cardinality:** ≤1 per chunk (sparse — skipped when the chunk emitted no edges, `:131`).
- **Shape:** a **bare JSON array** of `FlowEdge` (`FlowEdge.java:7-20`).

| Element field | Type | Nullable | Meaning |
|---|---|---|---|
| `fromNodeId` | string | yes | caller node ID; may be a **partial ID** without params (see `findMatchingNodeId`, `:1236-1242`) |
| `fromSchema` | string | yes | caller schema |
| `fromPackage` | string | yes (omitted) | caller package — **null for every non-Oracle sample** |
| `fromObject` | string | no | caller routine |
| `toNodeId` | string | yes | callee node ID (partial) |
| `toSchema` | string | yes | callee schema |
| `toPackage` | string | yes (omitted) | callee package |
| `toObject` | string | no | callee routine |
| `line` | int | no | call-site line in the caller |
| `depth` | int | no | recursion depth at which the edge was found |
| `edgeType` | string | yes (omitted) | only `FIRES_TRIGGER` / `TRIGGER_FN`; **plain call edges omit it** (`ChunkedFlowWriter.java:1166-1168`) |

- **Write pattern:** written once, whole-file (not appended despite the method name).
- **Identity:** no natural key — an edge is `(analysis_id, chunk_idx, from_node_id, to_node_id, line)`. FKs: `from_node_id`/`to_node_id` → `flow_node.nodeId` (**loose**: resolution is by prefix match, `:1236-1242`).
- **Size:** 210 B – 57 KB per file. Total 75–826 KB per analysis.
- **Read by:** `mergeChunks` (`:169-171`) only.
- **Source of truth?** Yes.

---

### 3.5 `…/<OBJECT>/edges.json`

- **Writer:** `ChunkedFlowWriter.mergeChunks()` — `ChunkedFlowWriter.java:173` (`writeJson(edgesFileKey, allEdges)`).
- **Purpose:** every `edges/edges_*.json` concatenated into one flat array.
- **Cardinality:** 1 per analysis.
- **Shape:** identical element shape to §3.4 (bare array of `FlowEdge`).
- **Write pattern:** written once at merge time; **fully rewritten** on rerun.
- **Size:** 75 349 B (SQL Server, 337 edges) → **825 959 B** (Postgres, 3295 edges). ≈ **250 bytes per edge**.
- **Worst case:** edges are the fastest-growing array (Postgres already has 29 edges/node). 20 000 nodes × 30 edges × 250 B ≈ **150–200 MB**. **Cannot be one Mongo document.**
- **Read by:** nothing in `unified-web` — `CoreFileReader` and `ParserAnalysisReader` both read `api/call_graph.json`, not `edges.json`.
- **Source of truth?** **No** — 100% derived, byte-for-byte, from `edges/edges_*.json`. **Pure duplication. Do not store.**

---

### 3.6 `…/<OBJECT>/flow.json`

- **Writer:** `ChunkedFlowWriter.mergeChunks()` — `ChunkedFlowWriter.java:206-207`; overwritten by `writeFinalResult()` — `:214-216`.
- **Purpose:** the complete `FlowResult` — every node, every edge, every table, plus rollup counters — in one file.
- **Cardinality:** 1 per analysis.
- **Shape:** `FlowResult` (`FlowResult.java:8-23`).

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `entryPoint` | string | yes | e.g. `ERP_USP_RUN_CYCLE` |
| `entrySchema` | string | yes | e.g. `ANALYZER`, `ERP`, `DB2INST1`, `ANALYZERDB` |
| `crawlTimeMs` | long | no | wall-clock crawl duration |
| `totalObjectsCrawled` | int | no | `= flowNodes.size()` (`:184`) |
| `uniqueObjectsCrawled` | int | no | distinct `nodeId` count (`:186-199`) |
| `totalProcedures` | int | no | standalone procedures (`:193-194`) |
| `totalFunctions` | int | no | standalone functions (`:195-196`) |
| `totalPackageProcedures` | int | no | package procedures — **0 for all non-Oracle dialects** |
| `totalPackageFunctions` | int | no | package functions — **0 for all non-Oracle dialects** |
| `totalLinesOfCode` | int | no | Σ node LOC (`:190`) |
| `maxDepthReached` | int | no | |
| `dbCallCount` | int | no | JDBC round trips during the crawl |
| `flowNodes[]` | array&lt;FlowNode&gt; | no | **full node shape as §3.3** — one element per chunk |
| `callGraph[]` | array&lt;FlowEdge&gt; | no | **full edge shape as §3.4** |
| `allTables[]` | array&lt;SchemaTableInfo&gt; | no | **full shape as §3.2** but as a *list*, having lost the `SCHEMA.TABLE` map key (`:181`) |
| `errors[]` | array&lt;string&gt; | no | crawl errors; `[]` in all 5 samples |

- **Write pattern:** written twice per run (merge, then final) — **full rewrite each time**.
- **Identity:** `analysis_id`.
- **Size:** **2 268 864 B (MySQL) → 3 298 349 B (Oracle)** on 52–114 nodes. Note the *Oracle* one is the biggest despite having the *fewest* nodes — driven by `statements[].sqlText`.
- **Worst case:** 3.3 MB / 52 nodes ≈ **63 KB per node**. A 5000-node enterprise package → **~315 MB**. **Categorically impossible as one BSON document.**
- **Read by:** nothing in `unified-web`. Every reader path goes through `api/`.
- **Source of truth?** **No** — fully derived from `chunks/` + `edges/` + `tables.json`. **Do not store; it is the single largest piece of pure duplication in the system.**

---

### 3.7 `…/<OBJECT>/api/index.json`

- **Writer:** `ChunkedFlowWriter.writeApiFiles()` — `ChunkedFlowWriter.java:523`.
  **Then patched twice:** `TableValidationService.patchMainIndex` (`TableValidationService.java:737-751`, Oracle only)
  and `AnalysisService.patchConnectionMeta` (`AnalysisService.java:918-941`).
- **Purpose:** the lightweight per-analysis manifest driving the sidebar/tree, summary tiles and the transactions sub-tab — everything the UI needs without loading a single node detail.
- **Cardinality:** 1 per analysis.

**Root fields** (`ChunkedFlowWriter.java:436-521` + patches):

| Field | Type | Nullable | Source line | Meaning |
|---|---|---|---|---|
| `entryPoint` | string | yes | `:437` | |
| `entrySchema` | string | yes | `:438` | |
| `totalNodes` | int | no | `:439` | `nodes.size()` |
| `maxDepth` | int | no | `:440` | |
| `totalLinesOfCode` | int | no | `:441` | Σ over all nodes |
| `totalFlowLinesOfCode` | int | no | `:442`, computed `:408-417` | entry node LOC + all its descendants' LOC |
| `totalTables` | int | no | `:443`; **overwritten** by `TableValidationService.java:745` with the DB-validated count | |
| `totalEdges` | int | no | `:444` | raw edge count |
| `uniqueEdges` | int | no | `:460`, computed `:446-459` | deduped `from -> to` pairs, nodeId-qualified |
| `totalCycles` | int | no | `:461` | observed 2–3 in every dialect |
| `transactionSummary` | object | no | `:489-494` | `{commits:int, rollbacks:int, savepoints:int, autonomousProcs:int}` |
| `transactionStatements[]` | array&lt;object&gt; | no (may be `[]`) | `:466-495` | element: `{nodeId:string, proc:string, schema:string, type:"COMMIT"\|"ROLLBACK"\|"SAVEPOINT", line:int, name:string?, autonomous:bool}` — `name` present only for named savepoints (`:484`) |
| `cycles[]` | array&lt;object&gt; | **omitted entirely when no cycles** (`:496`) | `:497-516` | element: `{nodeIds:[string], path:[string] (readable `PKG.OBJ` names), length:int}` |
| `errors[]` | array&lt;string&gt; | no | `:518` | |
| `crawlTimeMs` | long | no | `:519` | |
| `dbCallCount` | int | no | `:520` | |
| `nodes[]` | array&lt;object&gt; | no | `:521` | see below |
| `project` | string | optional | `AnalysisService.java:918-941` | patched post-hoc, e.g. `OracleDemo` |
| `environment` | string | optional | ditto | e.g. `local` |
| `connectionName` | string | optional | ditto | |
| `jdbcUrl` | string | optional | ditto | **the only reliable dialect discriminator in the artifact set** |
| `totalDurationMs` | long | optional | ditto | end-to-end incl. enrichment |
| `totalTablesRaw` | int | optional | `TableValidationService.java:350` | **Oracle only** — found + not-found |
| `dbValidated` | boolean | optional | `TableValidationService.java:351` | **Oracle only** at root level |

**`nodes[]` element** (`ChunkedFlowWriter.java:291-405`):

| Field | Type | Nullable | Line | Meaning |
|---|---|---|---|---|
| `nodeId` | string | no | `:299` | deduped short ID |
| `order` | int | no | `:300` | |
| `depth` | int | no | `:301` | |
| `schema` | string | yes | `:302` | |
| `packageName` | string | **explicitly null** when empty | `:303` | |
| `objectName` | string | no | `:304` | |
| `name` | string | no | `:305` | `pkg.obj` or `obj` (`:297`) |
| `objectType` | string | yes | `:306` | |
| `triggerTable` / `triggerEvent` / `triggerTiming` | string | **key omitted when null** | `:309-311` | trigger-only |
| `lineStart` / `lineEnd` / `linesOfCode` | int | no | `:312-314` | |
| `readable` | boolean | no | `:315` | |
| `message` | string | **key omitted when null** | `:316` | |
| `parentNodeId` | string | **explicitly null** for the root | `:317` | first caller found in edge order (`:272-274`) |
| `callCount` | int | no | `:318` | |
| `sourceFile` | string | **key omitted when null** | `:401` | `SCHEMA.PKG.OBJ.sql` or `SCHEMA.OBJ.sql` (`:1436-1444`) |
| `detailFile` | string | no | `:402` | `nodes/<safeFileName(nodeId)>.json` |
| `counts` | object | no | `:397` | see below |

**`nodes[].counts`** (`ChunkedFlowWriter.java:356-397`, `:433`):

| Field | Type | Line | Meaning |
|---|---|---|---|
| `tables` | int | `:357` | distinct schema-qualified tables (`countDistinct`, `:1479-1485`) |
| `callsOut` | int | `:358` | distinct non-BUILTIN call targets |
| `internalCalls` | int | `:359` | of which `type=INTERNAL` |
| `externalCalls` | int | `:360` | of which not INTERNAL |
| `variables` | int | `:361` | |
| `cursors` | int | `:362` | |
| `parameters` | int | `:363` | |
| `exceptionHandlers` | int | `:364` | |
| `sequences` | int | `:365`, computed `:343-354` | distinct sequence names inferred from `NEXTVAL`/`CURRVAL` calls |
| `statements` | object&lt;string,int&gt; | `:366` | the node's `statementSummary` histogram, **nested whole** |
| `subtreeTablesCount` | int | `:386` | distinct tables in this node + all descendants |
| `subtreeNodesCount` | int | `:387` | descendant count |
| `flowLinesOfCode` | int | `:395` | node LOC + descendant LOC |
| `inCycle` | boolean | `:433` | node participates in a detected cycle |

- **Write pattern:** written once by the writer, then **read-modify-rewritten** up to twice more (Oracle validation, connection metadata). Rerun overwrites.
- **Identity:** `analysis_id`; `nodes[].nodeId` → node PK; `parentNodeId` → self-FK; `detailFile`/`sourceFile` → file refs.
- **Size:** 72 136 – 151 749 B. ≈ **1.3 KB per node element**. Worst case 20 000 nodes ≈ **26 MB** — **exceeds 16 MB BSON.** The `nodes[]` array must be split out.
- **Read by:** `GET /api/parser/analyses/{name}/index` and `GET …/procedures` (`ParserAnalysisController`), via `IndexFacetReader:33` (and `:55` for package-merge) and `ParserAnalysisReader:75`. UI: `parser/js/api.js:35`, `plsql/js/api.js:66`; procedures at `api.js:42`, `plsql/js/api.js:225`. It also feeds the call-tree endpoints via `CallTreeFacetReader`.
- **Source of truth?** No — derived, but it is the **hot read path**, so it is the natural thing to materialise.

---

### 3.8 `…/<OBJECT>/api/nodes/<safeFileName(nodeId)>.json`

- **Writer:** `ChunkedFlowWriter.writeApiFiles()` — `ChunkedFlowWriter.java:715`.
- **Purpose:** the full expanded detail for one routine, loaded on demand when the user opens a node.
- **Cardinality:** **1 per node** (confirmed: `api/nodes/` count == `flowNodes` count in all 5 analyses).
- **Filename rule:** `safeFileName` (`:1446-1458`) — `[^A-Za-z0-9_$]` → `_`; if >150 chars, truncate to 140 + `_` + 8-hex MD5 prefix.

**Root fields** (`:528-715`):

| Field | Type | Nullable | Line | Meaning |
|---|---|---|---|---|
| `nodeId` | string | no | `:530` | |
| `name` | string | no | `:533` | `pkg.obj` or `obj` |
| `schema` | string | yes | `:534` | |
| `objectType` | string | yes | `:535` | |
| `depth` | int | no | `:536` | |
| `lineStart` / `lineEnd` / `linesOfCode` | int | no | `:537-539` | |
| `triggerTable` / `triggerEvent` / `triggerTiming` | string | key omitted if null | `:542-544` | trigger-only |
| `parameters[]` | array | **key omitted if empty** | `:547-558` | `{name, dataType, direction, default?, line:int}` — note the key is `default`, **renamed** from `defaultValue` (`:554`) |
| `variables` | object | key omitted if empty | `:562-563` | see below |
| `tables[]` | array | key omitted if empty | `:568` | see below |
| `directTables[]` | array | key omitted if empty | `:569` | **byte-identical duplicate of `tables`** — `groupTableOps` is called twice with the same input (`:568-569`) |
| `subtreeTables[]` | array | **always present** | `:575` | see below — may be `[]` |
| `cursors[]` | array | key omitted if empty | `:607-625` | see below |
| `sequences[]` | array | key omitted if empty/absent | `:630-633` | `{name:string, schema:string?, operations:{NEXTVAL?:[int], CURRVAL?:[int]}}` (`:905-925`) |
| `calls[]` | array | key omitted if empty | `:638` | see below |
| `dynamicSql[]` | array | key omitted if empty | `:642-653` | `{type:string, line:int, sqlExpression:string, usingVariables:[string]?}` |
| `exceptionHandlers[]` | array | key omitted if empty | `:657-667` | `{exception:string?, line:int, lineEnd:int, statementsCount:int}` — key **renamed** from `exceptionName` (`:661`) |
| `statementSummary` | object&lt;string,int&gt; | always present | `:671` | |
| `transactionScope` | object | key omitted when no txn stmts and not autonomous (`:695`) | `:696-702` | `{commits:int, rollbacks:int, savepoints:int, autonomous:bool, statements:[{type, line:int, name:string?}]}` |
| `externalPackageVarRefs[]` | array&lt;string&gt; | key omitted if empty | `:707` | |
| `sourceFile` | string | key omitted if null | `:712` | |

**`variables`** (`categorizeVariables`, `:731-778`) — a bucketed object, **not** a flat array:

| Field | Type | Line | Meaning |
|---|---|---|---|
| `total` | int | `:768` | |
| `typeRefCount` / `rowtypeRefCount` / `collectionCount` / `plainCount` | int | `:769-772` | bucket sizes |
| `typeRefs[]` | array | omitted if empty, `:773` | `%TYPE` vars: `{name, dataType, default?, line:int, constant:bool, refColumn:string, refTable?:string, refField?:string}` (`:749-757`) |
| `rowtypeRefs[]` | array | omitted if empty, `:774` | `%ROWTYPE` vars: `{…, refTable:string}` (`:746-748`) |
| `collections[]` | array | omitted if empty, `:775` | matched on `TABLE OF`/`VARRAY`/`ASSOCIATIVE`/`INDEX BY`/`NESTED TABLE` (`:758-761`) |
| `plain[]` | array | omitted if empty, `:776` | everything else |

> **Dialect note:** the `%TYPE`/`%ROWTYPE`/`VARRAY`/`INDEX BY` classification (`:746-761`) is
> **pure Oracle PL/SQL syntax**. For MySQL/SQL Server/DB2/Postgres every variable falls into
> `plain[]`, so `typeRefCount`/`rowtypeRefCount`/`collectionCount` are structurally 0.
> (Observed Oracle sample: `typeRefs` + `plain` present, no `rowtypeRefs`/`collections`.)

**`tables[]` / `directTables[]` element** (`groupTableOps`, `:781-835`):

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `schema` | string | yes | |
| `name` | string | no | |
| `objectType` | string | no | defaults to `"TABLE"` (`:793`) |
| `operations` | object&lt;string, array&lt;int&gt;&gt; | no | verb → **deduplicated, sorted** line numbers (`:824-832`) |
| `joins[]` | array&lt;object&gt; | no (may be `[]`) | `{joinType, joinedTable, joinedTableAlias, leftTable, leftTableAlias, condition, line:int, queryId:int}` (`:806-818`) — `leftTable`/`leftTableAlias` may be null (`:810-812`) |

**`subtreeTables[]` element** (`buildSubtreeTables`, `:1320-1406`):

| Field | Type | Meaning |
|---|---|---|
| `schema` / `name` / `objectType` | string | |
| `totalUsageCount` | int | operation count across the whole subtree (`:1390`) |
| `directUsageCount` | int | operation count from this node alone (`:1392`) |
| `operations` | object&lt;string, array&lt;int&gt;&gt; | verbs → merged, deduped, sorted lines across the subtree |
| `usedByNodes[]` | array&lt;object&gt; | `{nodeId:string, nodeName:string, depth:int, operations:{verb:[int]}}` (`:1396-1401`) |

**`cursors[]` element** (`:607-625`): `{name, line:int, lineEnd:int, forLoop:bool, refCursor:bool, query:string?, tables:[string]?, stmtOperations:[{operation:"OPEN"|"FETCH"|"BULK_COLLECT"|"CLOSE", line:int}]?}` — `stmtOperations` is reconstructed by **regex-scraping the cursor name out of `statement.sqlText`** (`extractCursorNameFromSql`, `:1460-1477`), so it is best-effort.

**`calls[]` element** (`groupCalls`, `:838-902`): `{target:string, packageName:string|null, objectName:string, type:string, schema:string|null, argCount:int?, lines:[int], count:int}` — `argCount` present **only for overloaded targets** (`:893`); `target` gets a ` (N args)` suffix in the same case (`:872`).

- **Write pattern:** written once per node per run; overwritten on rerun. **Stale node files from a previous, larger run are not deleted** — no cleanup pass exists in `writeApiFiles`.
- **Identity:** `(analysis_id, nodeId)`. FKs: `subtreeTables[].usedByNodes[].nodeId` → node; `calls[]` → the callee node (by name, **not** by nodeId — resolution is deferred to `node_id_map.json`).
- **Size:** min 282 B, max **370 379 B**, avg 42–78 KB. `subtreeTables` is the dominant term for shallow nodes (entry node's subtree contains every table in the analysis, with every usage line).
- **Worst case:** the entry node's `subtreeTables` is O(total tables × total call sites). 5000 tables × 2000 lines → **~40 MB for a single node document.** This is the *second* thing that must be split.
- **Read by:** `GET /api/parser/analyses/{name}/node/{file}` → `ParserAnalysisReader:82`, `IndexFacetReader:100` (subfolder fallback `:111`), `CoreFileReader:139-168` (listing). Also scanned in bulk by `SequenceFacetReader` (`/sequences`, `/dynamic-sql`) and `NodeScanFacetReader` (`/joins`, `/cursors`). UI: `parser/js/api.js:49`, `plsql/js/api.js:123`.
- **Source of truth?** No — derived from `chunks/`, but it is the primary UI read unit.

---

### 3.9 `…/<OBJECT>/api/tables/index.json`

- **Writers (two-phase):**
  1. `ChunkedFlowWriter.writeTableIndex()` — `ChunkedFlowWriter.java:1119` — writes the *static* view (from parsed source only).
  2. **Rewritten in place, per dialect,** with live catalog metadata:
     - Oracle: `TableValidationService.java:353` (+ `:60`, `:246`)
     - MySQL: `MySqlTableEnricher.java:100`
     - SQL Server: `SqlServerTableEnricher.java:93`
     - DB2: `Db2TableEnricher.java:100`
     - Postgres: `PostgresTableEnricher.java:96`
- **Purpose:** every schema object the flow touches, who touches it and how, enriched with real DB columns/indexes/constraints/triggers.
- **Cardinality:** 1 per analysis (but its `tables[]` is per-table → the split candidate).

**Root:**

| Field | Type | Nullable | Line | Meaning |
|---|---|---|---|---|
| `totalTables` | int | no | `:1108`; **overwritten** with the found-count by `TableValidationService.java:349` | |
| `operationSummary` | object&lt;string,int&gt; | no | `:1116`, computed `:1111-1115` | verb → number of tables using it |
| `tables[]` | array&lt;object&gt; | no | `:1117` | sorted by `schema.name` (`:1101-1105`) |
| `totalTablesRaw` | int | optional | `TableValidationService.java:350` | **Oracle only** (found + not-found) |
| `dbValidated` | boolean | optional | `TableValidationService.java:351` | present in all 5 dialects at the table level; at the **root** level only Oracle sets it |

**`tables[]` element — static part** (`:965-1052`):

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `schema` | string | **absent when the table key had no dot** (observed: 1/85 SQL Server entries lack it) | |
| `name` | string | no | |
| `objectType` | string | no | `"TABLE"` default (`:975`), or the type from `tables.json`, or overwritten by the enricher's `dbResolvedType` |
| `allOperations[]` | array&lt;string&gt; | no | union of verbs; `CURSOR_REF` added for cursor-only tables (`:1014`) |
| `usedBy[]` | array&lt;object&gt; | no | `{nodeId, nodeName, nodeDepth:int, operations:{verb:[int]}}` (`:992-997`). **`nodeDepth` is omitted for cursor-derived entries** (`:1016-1021`) |
| `triggers[]` | array&lt;object&gt; | **key omitted when the table has none** | two provenances: (a) parsed trigger nodes → `{nodeId, name, schema, event, timing}` (`:1042-1048`); (b) DB-discovered → `{name, schema, event, timing, triggerType, status, definition?, tableOps?, sequences?, source:"DATABASE"}` (`:1076-1087`). **The two shapes differ** — (a) has `nodeId`, (b) has `source`/`status`/`triggerType`. |

**`tables[]` element — enriched part** (added by the dialect enricher):

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `dbFound` | boolean | no | object exists in the live catalog |
| `dbValidated` | boolean | no | always `true` once an enricher ran |
| `dbResolvedType` | string | no | Oracle: real catalog type, spaces→underscores (`TableValidationService.java:271`, e.g. `MATERIALIZED_VIEW`). Other four: only `TABLE`/`VIEW`/`UNKNOWN`. |
| `dbColumns[]` | array | **key removed when `dbFound=false`** (`TableValidationService.java:337`) | see matrix below |
| `dbConstraints[]` | array | ditto | see matrix below |
| `dbIndexes[]` | array | ditto | `{indexName:string, uniqueness:"UNIQUE"\|"NONUNIQUE", columnName:string, position:int}` — **same 4 fields in all 5 dialects** (verified in real data) |
| `dbTriggers[]` | array | always refreshed when found (`:328-334`) | `{triggerName, owner, triggerType, triggeringEvent, status}` — **same 5 fields in all 5 dialects** (verified) |
| `viewDefinition` | string | key present only for views/mviews | Oracle caps at 32 000 chars + `[truncated]` marker (`TableValidationService.capViewDef:727-732`); **the other four never truncate** |

**`dbColumns[]` — DIALECT MATRIX (verified against real data, not just code):**

| Field | Oracle | MySQL | SQL Server | DB2 | Postgres |
|---|---|---|---|---|---|
| `columnName` (string) | ✔ | ✔ | ✔ | ✔ | ✔ |
| `dataType` (string) | ✔ | ✔ | ✔ | ✔ | ✔ |
| `columnId` (int) | ✔ | ✔ | ✔ | ✔ (`COLNO+1`, 0-based corrected) | ✔ |
| `nullable` (bool) | ✔ | ✔ | ✔ | ✔ | ✔ |
| `dataLength` (int) | ✔ always | ✔ conditional | ✔ always (**raw bytes**, not chars) | ✔ always | ✗ **absent in real data** |
| `dataDefault` (string) | ✔ | ✔ | ✔ (from default-constraint text) | ✔ | ✔ |
| `dataPrecision` (int) | ✔ | ✔ | ✔ | ✗ never emitted | ✗ never emitted |
| `dataScale` (int) | ✔ | ✔ | ✔ | ✔ | ✔ (code) |
| `computed` (bool) | ✗ | ✔ (from `EXTRA`) | ✔ (from `is_computed`) | ✗ | ✗ |

Real observed key sets: Oracle `[columnId, columnName, dataDefault, dataLength, dataType, nullable]`;
MySQL `[…, computed, dataLength, dataPrecision]`; SQL Server `[columnId, columnName, dataLength, dataPrecision, dataType, nullable]`;
DB2 `[…, dataLength, dataScale]`; Postgres `[columnId, columnName, dataDefault, dataType, nullable]`.

**`dbConstraints[]` — DIALECT MATRIX:**

| Field | Oracle | MySQL | SQL Server | DB2 | Postgres |
|---|---|---|---|---|---|
| `constraintName` (string) | ✔ | ✔ | ✔ | ✔ | ✔ |
| `constraintType` (string) | raw Oracle code `P`/`U`/`R`/`C` | mapped → `P`/`U`/`R`/`C` | only `P`/`U`/`R` (**no CHECK**) | only `P`/`R`/`U` (**no CHECK**) | only `P`/`R`/`U` (**no CHECK**) |
| `columnName` (string) | ✔ | ✔ | ✔ | ✔ | ✔ |
| `position` (int) | ✔ | ✔ | ✔ | ✔ | ✔ |
| `refConstraint` (string) | **referenced constraint NAME** | referenced `table.column` | referenced `schema.table` | referenced `schema.table` | referenced `schema.table` |
| `searchCondition` (string) | ✔ **Oracle only** (CHECK text) | ✗ | ✗ | ✗ | ✗ |

> `refConstraint` has **three incompatible meanings** across dialects. A unified schema must either
> normalise it into `(ref_schema, ref_table, ref_column, ref_constraint_name)` with nullable parts,
> or keep the dialect tag alongside it.

**Additional Oracle-only behaviours** (`TableValidationService.java`): synonym following before metadata
lookup (`resolveSynonyms:667-713`), CLOB-safe view/mview text (`:562-624`), materialized-view text from
`ALL_MVIEWS.query` separately from `ALL_VIEWS.text`, and patching `api/index.json.totalTables`
(`patchMainIndex:735-751`). **None of the four generic enrichers do any of these.**

**Postgres-only risk:** `PostgresTableEnricher.java:242-256` derives `dbIndexes[].columnName` and
`uniqueness` by **string-parsing the `pg_indexes.indexdef` DDL text** (split on comma inside the
parenthesised column list) and synthesises `position` from a counter. Expression indexes, quoted
identifiers, or commas inside function calls will corrupt these rows.

- **Write pattern:** written once by `ChunkedFlowWriter`, then **read-modify-rewrite** by the enricher (and re-runnable — the enricher skips re-attaching metadata that is already present, `:288`/`:296`/`:304`, but always refreshes `dbTriggers`, `:328`).
- **Identity:** `(analysis_id, schema, name)`. FKs: `usedBy[].nodeId` → node; `triggers[].nodeId` → node.
- **Size:** 152 152 B (Oracle, 42 tables) → 307 401 B (MySQL, 232 tables). ≈ **1.3–3.6 KB per table**, dominated by `dbColumns` and `usedBy[].operations` line arrays.
- **Worst case:** 5000 tables × (200 columns + 2000 usage lines) → **~500 MB**. **The single largest artifact at enterprise scale.** Must be split per table, and per table into `columns` / `indexes` / `constraints` / `triggers` / `usage` child collections.
- **Read by:** `GET /api/parser/analyses/{name}/tables` → `TableFacetReader:34` (package merge `:56-114`), `ParserAnalysisReader:86`. UI: `parser/js/api.js:56,84`, `parser/js/txnTrace.js:49`, `plsql/js/api.js:160`.
- **Source of truth?** **Partly.** The static half is derived from `chunks/`; the `db*` half is a **snapshot of live catalog state at run time and exists nowhere else** — that half IS source of truth and must be preserved with a timestamp.

---

### 3.10 `…/<OBJECT>/api/call_graph.json`

- **Writer:** `ChunkedFlowWriter.writeCallGraph()` — `ChunkedFlowWriter.java:1206`.
- **Purpose:** deduplicated call edges with occurrence lines, cycle back-edge marking, for graph rendering.
- **Cardinality:** 1 per analysis.

| Field | Type | Nullable | Line | Meaning |
|---|---|---|---|---|
| `totalEdges` | int | no | `:1201` | raw count |
| `uniqueEdges` | int | no | `:1202` | after dedup |
| `backEdges` | int | no | `:1203` | count of edges flagged `backEdge` |
| `edges[]` | array&lt;object&gt; | no | `:1204` | see below |

**`edges[]` element** (`:1155-1168`, `:1172-1192`):

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `fromNodeId` | string | yes | |
| `fromSchema` | string | yes | |
| `from` | string | no | `pkg.obj` or `obj` |
| `toNodeId` | string | yes | |
| `toSchema` | string | yes | |
| `to` | string | no | |
| `lines[]` | array&lt;int&gt; | no | **every** call-site line — observed one edge with **165 entries** |
| `count` | int | no | `lines.length` |
| `minDepth` | int | no | shallowest depth this edge was seen at |
| `backEdge` | boolean | no | true if it closes a detected cycle (`:1183-1192`) |
| `edgeType` | string | **key omitted for plain call edges** (`:1166-1168`) | `FIRES_TRIGGER` / `TRIGGER_FN` |

- **Dedup key** (`:1138-1151`): `fromKey + " -> " + toKey + "|" + fromNodeId + "->" + toNodeId`, where a
  key is `pkg.obj` when a package exists, else `schema$obj` — so same-named cross-schema routines and
  arity-distinguished overloads stay separate.
- **Write pattern:** once per run, full rewrite.
- **Identity:** `(analysis_id, from_node_id, to_node_id)` — the dedup key. FKs to node on both ends.
- **Size:** 84 365 – 263 021 B. ≈ **80 bytes per unique edge**, but the `lines[]` array makes hot edges very fat.
- **Worst case:** 20 000 nodes × 30 unique edges × (80 B + lines) → **~60 MB**. Unbounded; split per edge.
- **Read by:** `GET …/call-graph` → `CoreFileReader:66` (package merge `:83-91`), `ParserAnalysisReader:90`. Also the backing data for `GET …/call-tree/{nodeId}` and `…/callers` via `CallTreeFacetReader`. UI: `parser/js/api.js:149` (callGraph.js / callGraphViz.js), `plsql/js/api.js:111`; call-tree at `api.js:135,142`, `plsql/js/api.js:135,147`.
- **Source of truth?** No — derived from `edges/`. But note it carries information `edges/` does not: `backEdge`, `minDepth`, `count`, and the cycle analysis.

---

### 3.11 `…/<OBJECT>/api/node_id_map.json`

- **Writer:** `ChunkedFlowWriter.writeNodeIdMap()` — `ChunkedFlowWriter.java:1231`.
- **Purpose:** resolve the compact node ID used in URLs/edges to the fully-parameterised ID, and back.
- **Cardinality:** 1 per analysis.

| Field | Type | Nullable | Line | Meaning |
|---|---|---|---|---|
| `shortToFull` | object&lt;string,string&gt; | no | `:1228` | compact ID → `FlowNode.buildFullId()`. Key name is load-bearing: *"must match `ParserAnalysisReader.loadIdMap`"* (`:1228`) |
| `fullToId` | object&lt;string,string&gt; | no | `:1229` | exact inverse |

Observed: for non-overloaded nodes the two are identical (`ANALYZER$ERP_USP_RUN_CYCLE` → itself).

- **Write pattern:** once per run; also **merged** across procs for package analyses by `AnalysisService.mergePackageNodeIdMap` (`AnalysisService.java:841-843`).
- **Identity:** `analysis_id`; both maps are just an index over `flow_node`. **Fully redundant** with a `(node_id, full_id)` column pair on the node table.
- **Size:** ~9 KB.
- **Worst case:** 20 000 × ~250 B ≈ 5 MB. Bounded in practice but redundant.
- **Read by:** `ParserAnalysisReader:474` (`loadIdMap`), consumed by the `/call-tree/{nodeId}` and `/call-tree/{nodeId}/callers` endpoints. Not fetched directly by the UI.
- **Source of truth?** No — pure index. **Do not store; make it two columns.**

---

### 3.12–3.15 `…/<OBJECT>/resolver/*.json`

All four written together by `ChunkedFlowWriter.writeResolverData()` — `ChunkedFlowWriter.java:1408-1417`.
All are **Oracle-only in practice**: `SchemaResolver`'s constructor early-returns for MySQL (`:123-135`),
SQL Server (`:139-151`), DB2 (`:153-167`) and Postgres (`:169-183`) after seeding just the entry point,
so the four exports are empty or near-empty for those dialects.

| File | Writer line | Java export | Shape |
|---|---|---|---|
| `q1_object_resolution.json` | `:1411` | `SchemaResolver.exportObjectResolution():869-882` | **array** of `{objectName:string, schema:string, objectType:string}` (`objectType` defaults to `"UNKNOWN"`, `:875`) |
| `q4_direct_dependencies.json` | `:1412` | `exportDirectDependencies():884-890` | **object** map `"SCHEMA.OBJECT"` → array&lt;string&gt; of dependency names. Observed: `{"ANALYZER.ERP_FN_BOM_COMPONENT_COUNT": ["ERP_BOM_COMPONENT","ERP_BOM_HEADER","STANDARD","SYS_STUB_FOR_PURITY_ANALYSIS"]}` — note Oracle pseudo-objects leak through |
| `q3_reverse_dependencies.json` | `:1413` | `exportReverseDependencies():892-894` | **array** of `Map<String,String>` rows. **Observed `[]` even for Oracle** in the sample data |
| `q1_ambiguous_objects.json` | `:1414` | `exportAmbiguousObjects():896-914` | **array** of `{objectName:string, candidates:[{schema:string, objectType:string}]}`. **Observed bug:** candidates are not deduplicated — one entry had `ERP_AUDIT_LOG` with 9 identical `{ANALYZER, TABLE}` candidates |

**Observed population across all 10 dirs (confirms the Oracle-only claim empirically):**

| File | Oracle | MySQL | SQL Server | DB2 | Postgres |
|---|---|---|---|---|---|
| `q1_object_resolution.json` | 85 entries | 1 | 1 | 1 | 1 |
| `q1_ambiguous_objects.json` | 47 entries | `[]` | `[]` | `[]` | `[]` |
| `q4_direct_dependencies.json` | 44 keys | `{}` | `{}` | `{}` | `{}` |
| `q3_reverse_dependencies.json` | **empty** | empty | empty | empty | empty |

The non-Oracle "1 entry" in `q1_object_resolution.json` is exactly the entry-point seed written by the
early-return branches (`SchemaResolver.java:123-183`) — direct confirmation of the code reading.
`q3_reverse_dependencies.json` is **empty in all 10 analyses including Oracle**, so the reverse-dependency
feature is effectively unpopulated today. (Serialised form is `[]` for the Oracle dir read directly here;
an empty `{}` was also reported — the `exportReverseDependencies` return type is `List<Map<String,String>>`
(`SchemaResolver.java:892`), so `[]` is the correct expectation. Either way: **no data**. Do not design
a table around it without a runtime confirmation.)

- **Cardinality:** 1 each per analysis.
- **Write pattern:** written once per run, full rewrite.
- **Identity:** `(analysis_id, object_name)` for q1/q1amb; `(analysis_id, from_object, to_object)` for q4/q3.
- **Size:** the whole `resolver/` directory is 60 KB in every sample.
- **Worst case:** `q4` is O(objects × dependencies) — a full Oracle `ALL_DEPENDENCIES` crawl of a large schema could reach 50 MB. Unbounded.
- **Read by:** `GET /api/parser/analyses/{name}/resolver/{type}` → `ParserAnalysisReader:110-117`. **No UI `fetch()` calls this endpoint** — currently orphaned.
- **Source of truth?** Snapshot of Oracle `ALL_DEPENDENCIES` at run time — source of truth for that snapshot, but duplicated by the TSV cache (§3.19).

---

### 3.16 `…/<OBJECT>/sources/<file>` — NOT JSON

- **Writer:** `ChunkedFlowWriter.writeSourceFile()` — `ChunkedFlowWriter.java:140-142` (raw UTF-8 string, `writeString`).
- **Purpose:** the verbatim DDL text every line number in every other artifact refers to.
- **Cardinality:** 1 per distinct source object.
- **Content:** plain SQL text, no structure.
- **Filename:** two conventions coexist. The *reference* written into `index.json`/`nodes/*.json` is
  `buildSourceFileName` (`:1436-1444`): `SCHEMA.PKG.OBJ.sql` or `SCHEMA.OBJ.sql`. But the *actual files
  on disk* carry dialect/type extensions — observed across all 10 dirs: **582 `.prc`, 74 `.sql`, 50 `.fnc`,
  2 `.pkb`** (e.g. `ERP_REF.fn_RegionName.prc`, `erp_ref.TRG.trg_RefRegion.sql`, `ANALYZER.ERP_PKG_PLANNER.SET_CONTEXT.sql`).
  Both `ParserAnalysisReader:93-107` and `CoreFileReader:97-136` implement an **extension-fallback search**
  precisely because of this mismatch. Casing is also inconsistent (`ERP_REF.` vs `erp_ref.`).
- **Size:** 36 B – 94 331 B per file; 711 867 – 1 183 802 B total per analysis.
- **Worst case:** a large package body is easily 5–10 MB.
- **Read by:** `GET …/source/{file}` → `ParserAnalysisReader:93-107`, `CoreFileReader:97-136`; also read raw by `SequenceFacetReader` for dynamic-SQL scanning. UI: `parser/js/api.js:156`.
- **Source of truth?** **Yes** — this is the only copy of the analysed text. Store as GridFS / BLOB / object-store reference, never inline.

---

### 3.17 `…/<OBJECT>/.parse-cache/<md5-of-source-text>.json`

- **Writer:** `ParseResultCache.java:57-58`. Path: `ParseResultCache.java:34` (`outputKey.resolve(".parse-cache")`) + `:65` (`md5(sourceText) + ".json"`).
- **Purpose:** skip ANTLR re-parsing when the same source text is seen again (e.g. every subprogram in one package body).
- **Cardinality:** 1 per distinct source text.
- **Shape:** a serialised `ParseResult` (`ParseResult.java:8-11`): `{fileName:string, parseTimeMs:long, errors:[string], objects:[ParsedObject]}`.
  `ParsedObject` (`ParsedObject.java:8-51`) is a superset of `FlowNode`'s payload:
  `{type, name, schema, lineStart, lineEnd, parameters[], globalVariables[], packageVariables[], subprograms[], statements[], calls[], cursors[], dynamicSql[], tableOperations[], externalPackageVarRefs[], exceptionHandlers[], dependencies, triggerEvent, triggerTable, triggerTiming, viewQuery, pipelined, pragmaAutonomousTransaction}`.
  `subprograms[]` is `SubprogramInfo` (`SubprogramInfo.java:8-26`) — a **recursive nesting level** carrying its own `parameters/localVariables/statements/calls/cursors/dynamicSql/tableOperations/exceptionHandlers/externalPackageVarRefs` plus `returnType`, `pragmaAutonomousTransaction`, `pipelined`, `forwardDeclaration`.
  `dependencies` is `DependencySummary` (`DependencySummary.java:8-14`): `{tables:[], views:[], materializedViews:[], packages:[], sequences:[], dbLinks:[], types:[]}` — all `Set<String>`, all Oracle-flavoured categories (`dbLinks`, `materializedViews` are Oracle concepts).
  Observed real keys: `[fileName, parseTimeMs, objects]`, object keys `[type, name, lineStart, lineEnd, parameters, globalVariables, statements, tableOperations, exceptionHandlers, dependencies, pipelined, pragmaAutonomousTransaction]`.
- **Write pattern:** write-once, content-addressed (MD5 of the source), never invalidated.
- **Identity:** `(analysis_id, md5_of_source)` — content-addressed, so genuinely reusable across analyses.
- **Size:** 1.6 MB total per analysis.
- **Read by:** nothing in `unified-web`.
- **Source of truth?** **No — pure cache.** Safe to drop entirely from the database design (or keep as a
  content-addressed blob keyed on source MD5, shared across analyses, which is what it already is).

---

### 3.18 `…/<OBJECT>/.node-cache/<SAFENAME>_<md5>.json`

- **Writer:** `NodeParseCache.java:61-62`. Path: `:38` (`flowOutputKey.resolve(".node-cache")`) + `:76` (`safeName + "_" + hash + ".json"`).
- **Purpose:** skip the whole ANTLR + resolution pipeline for a node already analysed in a prior run.
- **Cardinality:** 1 per visited node.
- **Shape:** a serialised `FlowNode` — **identical to `chunks/chunk_NNNN.json` (§3.3)**. Observed key set matches exactly.
- **Filename observed:** `ANALYZER_ERP_USP_EXPLODE_BOM_568a824d…json`, `ERP_PKG_PLANNER.COMPUTE_TOTAL_1_d5028860…json`.
- **Write pattern:** write-once per visit key + hash.
- **Size:** 2.4 MB total per analysis.
- **Read by:** nothing in `unified-web`.
- **Source of truth?** **No — pure cache, 100% duplicated with `chunks/`.** Drop.

---

### 3.19 `cache/schema-resolver/<DBTYPE>/<PROJECT>/<ENV>/*.tsv` — NOT JSON

- **Writer:** `SchemaResolver.saveToDisk()` — `SchemaResolver.java:1001-1034`. Key: `resolverCacheKey()` — `SchemaResolver.java:56-65`, which splits `cacheScope` on `~` and sanitises each token with `[^A-Za-z0-9_~-]` → `_`.
- **Purpose:** cache the Oracle `ALL_OBJECTS`/`ALL_DEPENDENCIES` crawl **per connection scope**, shared by every analysis on that connection.
- **Cardinality:** 6 files per `(dbtype, project, env)` scope — **not per analysis.**
- **Files** (all TSV, tab-separated, no header observed):
  | File | Line | Content |
  |---|---|---|
  | `objects.tsv` | `:1004` | `objectName → schema` |
  | `object_types.tsv` | `:1005` | `objectName → objectType` |
  | `type_qualified.tsv` | `:1006` | `TYPE:objectName → schema` |
  | `ambiguous.tsv` | `:1008` | objects with multiple candidate schemas |
  | `dependencies.tsv` | `:1020` | forward dependency edges |
  | `reverse_deps.tsv` | `:1024` | reverse dependency edges |
- **Write pattern:** rewritten whole on crawl completion; deleted wholesale by `--clear-cache` (`FlowAnalysisMain.java:195-200`, `store.deletePrefix`).
- **Identity:** `(db_type, project, environment)` — **shared across analyses**, unlike everything else in this document. This is the one artifact whose grain is the *connection*, not the *analysis*.
- **Size:** 40 KB per scope observed (Oracle only; the other four scopes produce a near-empty cache).
- **Read by:** `SchemaResolver.loadFromDisk` (`:1060-1095`); the `POST /api/parser/cache/clear-connection` endpoint deletes it (UI: `parser/js/cacheManager.js:41,124,146`).
- **Source of truth?** No — cache. But it duplicates `resolver/q*.json` (§3.12-3.15) in a different format at a different grain.

---

### 3.20–3.21 Claude sidecar: `plsql/<identity>/metadata.json` and `analysis_claude.json`

Addressed by `AnalysisPaths.claudeDirKey`/`claudeFileKey` (`AnalysisPaths.java:163-174`) — a **flat**
`plsql/<full-identity-name>/` folder, *not* nested by DBTYPE/PROJECT/ENV. Written by the Claude
verification pipeline, not by `ChunkedFlowWriter`.

`metadata.json` (~250 B, bounded) — observed exactly:
`{name:string, mode:"claude", claudeIteration:int, lastUpdated:ISO-8601 string, confirmedCount:int, removedCount:int, newCount:int, totalTables:int, totalChunks:int, errorChunks:int}`.

`analysis_claude.json` (138 KB observed) — top-level keys:
`{analysisName:string, timestamp, tables, calls, confirmedCount:int, removedCount:int, newCount:int, callConfirmedCount:int, callRemovedCount:int, callNewCount:int, totalChunks:int, errorChunks:int, claudeCallCount:int, claudeTimeMs:long, error}`.
`tables` and `calls` are the unbounded arrays here. **UNKNOWN — needs runtime check** for their exact
element shapes; the writing class is outside the `parser/**` package scope of this document.

Archival: observed `plsql/ERP_USP_RUN_CYCLE__WL2ZTC`, `…_20260722_231526`, `…_20260723_001120`,
`…_20260723_153725` — i.e. the **same timestamp-suffix archival pattern** as `plsql-parse/`.

---

### 3.22 `plsql-env-config.yaml` — NOT JSON, NOT in the artifact store

- **Writer:** `AnalysisService.java:903` (`Files.writeString`), built by `buildEnvConfigFile`.
- **Purpose:** hands DB credentials to the parser subprocess.
- **Critical:** it is **deliberately excluded from the shared artifact store** — `CREDENTIAL_FILES`
  (`AnalysisService.java:71`) and `rejectCredentialFile` (`:85-90`) enforce this. It is local-disk only.
- **Schema-design consequence:** **must never be modelled as an artifact.** Credentials belong in a
  secrets store, referenced by ID from the connection record.

---

## 4. Dialect differences — consolidated

### 4.1 Fields that exist only for some databases

| Field / behaviour | Oracle | MySQL | SQL Server | DB2 | Postgres | Where |
|---|---|---|---|---|---|---|
| `dbColumns[].dataPrecision` | ✔ | ✔ | ✔ | ✗ | ✗ | enrichers |
| `dbColumns[].dataLength` | ✔ | ✔ | ✔ (raw bytes) | ✔ | ✗ (observed) | enrichers |
| `dbColumns[].computed` | ✗ | ✔ | ✔ | ✗ | ✗ | enrichers |
| `dbColumns[].dataScale` | ✔ | ✔ | ✔ | ✔ | ✔ (code) | enrichers |
| `dbConstraints[].searchCondition` | ✔ | ✗ | ✗ | ✗ | ✗ | `TableValidationService.java:511-517` |
| CHECK constraints represented at all | ✔ | ✔ | ✗ | ✗ | ✗ | enrichers |
| `dbConstraints[].refConstraint` semantics | constraint name | `table.column` | `schema.table` | `schema.table` | `schema.table` | enrichers |
| `dbTriggers[].status` reflects reality | ✔ | ✗ hardcoded `"ENABLED"` | ✔ | ✗ hardcoded | ✗ hardcoded | enrichers |
| `viewDefinition` truncated at 32 000 chars | ✔ | ✗ | ✗ | ✗ | ✗ | `TableValidationService.capViewDef:727-732` |
| Materialized-view type resolvable | ✔ | n/a | n/a | ✗ enricher / ✔ `batchResolveObjectTypes` | ✗ enricher / ✔ `batchResolveObjectTypes` | `SchemaResolver.java:755-765` |
| Synonym following | ✔ | ✗ | ✗ | ✗ | ✗ | `TableValidationService.resolveSynonyms:667-713` |
| `totalTables`/`totalTablesRaw`/root `dbValidated` patched | ✔ | ✗ | ✗ | ✗ | ✗ | `TableValidationService.java:349-351,745` |
| `resolver/q*.json` populated | ✔ | ✗ empty | ✗ empty | ✗ empty | ✗ empty | `SchemaResolver.java:123-183` |
| `flow.json.totalPackageProcedures/Functions` non-zero | ✔ | ✗ | ✗ | ✗ | ✗ | packages are an Oracle concept here |
| `FlowNode.packageName` / edge `fromPackage`/`toPackage` | ✔ | ✗ null | ✗ null | ✗ null | ✗ null | observed in all 5 datasets |
| `FlowNode.autonomousTransaction` (`PRAGMA`) | ✔ | ✗ | ✗ | ✗ | ✗ | `FlowNode.java:36` |
| `variables.typeRefs`/`rowtypeRefs`/`collections` (`%TYPE`, `%ROWTYPE`, `VARRAY`) | ✔ | ✗ | ✗ | ✗ | ✗ | `ChunkedFlowWriter.java:746-761` |
| `DependencySummary.dbLinks` / `.materializedViews` | ✔ | ✗ | ✗ | ✗ | ✗ | `DependencySummary.java:10,13` |
| `TableOperationInfo.dbLink` | ✔ | ✗ | ✗ | ✗ | ✗ | `TableOperationInfo.java:16` |
| Index metadata from catalog vs parsed DDL text | catalog | catalog | catalog | catalog | **parsed from `indexdef`** | `PostgresTableEnricher.java:242-256` |
| Enricher filters to one schema | multi (via `ALL_OBJECTS`) | ✔ one | ✗ all schemas | ✔ one | ✗ all non-system | enrichers |
| Dedicated sequence model in the parser module | n/a | ✗ | ✔ `SqlServerSequenceInfo` | ✗ | ✗ | `sqlserver-parser/.../model/SqlServerSequenceInfo.java` |

### 4.3 `statementSummary` — the dialect vocabulary (the biggest *value*-level divergence)

`statementSummary` / `counts.statements` is an **open-ended `Map<String,Integer>`** whose key set is
entirely dialect-determined. Observed key sets in the real data:

| Dialect | `statementSummary` keys observed |
|---|---|
| **Oracle** | `ASSIGNMENT, CASE, CLOSE_CURSOR, COMMIT, CTE, CURSOR_ATTRIBUTE, CURSOR_FOR_LOOP, DELETE, EXECUTE_IMMEDIATE, EXIT, FETCH, FOR_LOOP, FORALL, IF, INSERT, LOOP, MERGE, OPEN_CURSOR, PIPE_ROW, RAISE, RETURN, ROLLBACK, SAVEPOINT, SELECT, SUBQUERY, UPDATE, WHILE_LOOP` (27 — richest) |
| **SQL Server** | `BEGIN_TRAN, BLOCK, CASE, CLOSE, COMMIT, CREATE, CTE, DEALLOCATE, DECLARE, DROP, EXEC, FETCH, IF, INSERT, MERGE, OPEN, OTHER, RETURN, ROLLBACK, SAVEPOINT, SELECT, SET, SUBQUERY, THROW, TRUNCATE, TRY_CATCH, UPDATE, WHILE_LOOP` (28) |
| **DB2** | `CASE, COMMIT, CTE, FOR_LOOP, IF, LOOP, MERGE, REPEAT, ROLLBACK, SAVEPOINT, SUBQUERY, WHILE_LOOP` (12) |
| **Postgres** | `CASE, COMMIT, CTE, FOR_LOOP, IF, LOOP, MERGE, ROLLBACK, SUBQUERY, UPSERT, WHILE_LOOP` (11) |
| **MySQL** | `CASE, COMMIT, CTE, IF, LOOP, REPEAT, ROLLBACK, SAVEPOINT, SUBQUERY, UPSERT, WHILE_LOOP` (11) |

Dialect-exclusive tokens: `EXECUTE_IMMEDIATE`, `FORALL`, `PIPE_ROW`, `CURSOR_FOR_LOOP`, `CURSOR_ATTRIBUTE`,
`RAISE`, `OPEN_CURSOR`/`CLOSE_CURSOR` (Oracle); `TRY_CATCH`, `THROW`, `BEGIN_TRAN`, `EXEC`, `DEALLOCATE`,
`TRUNCATE`, `DECLARE`, `BLOCK`, `OTHER` (SQL Server); `REPEAT` (MySQL + DB2); `UPSERT` (MySQL + Postgres);
`FOR_LOOP` (Oracle, Postgres, DB2 — absent for MySQL and SQL Server); `MERGE` (all but MySQL).

The same open-endedness applies to `allOperations[]` / `operationSummary` on tables: `CREATE`, `DROP`,
`TRUNCATE` appear only in the SQL Server run; `DYNAMIC` only in Oracle.

**Schema-design consequence:** never model these as an enum or as fixed columns. Store as a
`(analysis_id, node_id, statement_type, count)` child table (relational) or a free-form sub-document
(Mongo). The set will grow with every new dialect.

### 4.2 What is identical across all five

- The **file set** and **directory layout** are byte-for-byte identical in shape for all five dialects
  (verified: all 10 dirs contain the same `flow.json`, `edges.json`, `tables.json`, `progress.json`,
  `chunks/`, `edges/`, `api/{index,call_graph,node_id_map}.json`, `api/nodes/`, `api/tables/index.json`,
  `resolver/q{1,1amb,3,4}.json`, `sources/`, `.parse-cache/`, `.node-cache/`).
- `ChunkedFlowWriter`, `ParseResultCache`, `NodeParseCache`, `FlowAnalysisMain` contain **zero dialect
  branching** — the divergence is entirely inside `SchemaResolver`'s constructor and the five enrichers.
- `dbIndexes[]` (4 fields) and `dbTriggers[]` (5 fields) have the **same key set in all five dialects**
  in real data — only the *fidelity* of the values differs.
- The four `mysql-parser`/`db2-parser`/`postgres-parser`/`sqlserver-parser` modules perform **no JSON
  serialisation at all** (no `ObjectMapper`/`writeValue`/`toJson`/`JSONObject` in any of their `src/main`).
  They emit POJOs that are mapped into the common `FlowNode`/`ParseResult` shapes in `plsql-parser`.

**Schema-design conclusion:** one schema comfortably covers all five if every dialect-specific column is
nullable and a `db_type` discriminator is stored on the analysis. The only genuine modelling hazard is
`dbConstraints.refConstraint`, whose meaning changes per dialect — split it into
`(ref_schema, ref_table, ref_column, ref_constraint_name)`.

---

## 5. Bounded vs unbounded — the 16 MB BSON question

**Bounded (safe as a single embedded document, forever):**

| Artifact | Ceiling |
|---|---|
| `plsql/<id>/metadata.json` | ~1 KB, 10 fixed scalar fields |
| `api/index.json` **root scalars only** (excluding `nodes[]`, `cycles[]`, `transactionStatements[]`) | ~1 KB |
| `flow.json` **root counters only** (excluding the three arrays) | ~200 B, 12 scalars |
| `api/tables/index.json` root (`totalTables`, `operationSummary`, `dbValidated`) | ~1 KB |
| `api/call_graph.json` root counters | ~100 B |
| One `dbColumns[]` / `dbIndexes[]` / `dbConstraints[]` / `dbTriggers[]` element | ~200 B each |

**Unbounded — can never be one Mongo document:**

| Artifact | Observed | Growth driver | Projected at 5k nodes |
|---|---|---|---|
| `flow.json` | **3.3 MB @ 52 nodes** | everything | **~300 MB** |
| `api/tables/index.json` | 307 KB @ 232 tables | tables × columns × usage lines | **~500 MB** |
| `edges.json` | 826 KB @ 3295 edges | edges | **~200 MB** |
| `api/call_graph.json` | 263 KB @ 3295 edges | unique edges + `lines[]` | ~60 MB |
| `api/index.json` | 152 KB @ 114 nodes | `nodes[]` | ~26 MB |
| single `api/nodes/*.json` | **370 KB** | `subtreeTables` | ~40 MB |
| single `chunks/chunk_*.json` | 217 KB | `statements[].sqlText` | ~5 MB |
| `tables.json` | 106 KB @ 232 tables | `lines[]` (with duplicates) | ~60 MB |
| `resolver/q4_direct_dependencies.json` | small | Oracle `ALL_DEPENDENCIES` | ~50 MB |
| `.node-cache/` + `.parse-cache/` | 4.0 MB combined | node count | ~400 MB |

**Borderline — depends on workload:** `progress.json` (~2 MB), `api/node_id_map.json` (~5 MB),
`analysis_claude.json` (~50 MB).

---

## 6. Large embedded arrays — the split candidates

Element counts and average element sizes measured on the real data.

| Array | Location | Observed count | Avg element size | Verdict |
|---|---|---|---|---|
| `flowNodes[]` | `flow.json` | 52 / 57 / 64 / 69 / 114 | **~30 KB** | **Split → one doc per node** |
| `callGraph[]` | `flow.json` | 337 / 428 / 652 / 2258 / 3295 | ~230 B | **Split → one row per edge** |
| `allTables[]` | `flow.json` | 42 / 85 / 100 / 114 / 232 | ~700 B (dominated by `lines[]`) | Split → one row per table |
| `edges[]` (raw) | `edges.json` | same as `callGraph` | **~250 B** | Split (or drop — duplicate) |
| `edges[]` (deduped) | `api/call_graph.json` | ≤ raw count | ~80 B + `lines[]` (up to **165 ints on one edge**) | Split; `lines[]` → child table |
| `nodes[]` | `api/index.json` | == node count | **~1.3 KB** | **Split → node summary collection** |
| `transactionStatements[]` | `api/index.json` | commits+rollbacks+savepoints across all nodes | ~120 B | Split if large |
| `cycles[]` | `api/index.json` | 2–3 observed | ~200 B | Keep embedded |
| `tables[]` | `api/tables/index.json` | 42 / 85 / 100 / 114 / 232 | **1.3–3.6 KB** | **Split → one doc per table** |
| `tables[].dbColumns[]` | `api/tables/index.json` | up to ~50/table observed | ~110 B | Child collection |
| `tables[].dbIndexes[]` | `api/tables/index.json` | 1–20/table | ~90 B | Child collection |
| `tables[].dbConstraints[]` | `api/tables/index.json` | 1–20/table | ~100 B | Child collection |
| `tables[].usedBy[]` | `api/tables/index.json` | up to node count | ~200 B + line arrays | Child collection |
| `subtreeTables[]` | `api/nodes/*.json` | **42 on the entry node** (== every table in the analysis) | up to **8 KB** each | **The worst offender.** Derived — recompute, don't store |
| `subtreeTables[].usedByNodes[]` | `api/nodes/*.json` | up to node count | ~250 B | Derived |
| `tables[]` / `directTables[]` | `api/nodes/*.json` | 30 observed | ~1 KB | Child collection; **drop `directTables` (exact duplicate)** |
| `cursors[]` | `api/nodes/*.json` | **43 on one node** | ~500 B (`query` text) | Child collection |
| `exceptionHandlers[]` | `api/nodes/*.json` | **86 on one node** | ~80 B | Child collection |
| `dynamicSql[]` | `api/nodes/*.json` | 32 | ~300 B | Child collection |
| `calls[]` | `api/nodes/*.json` | 21 | ~200 B + `lines[]` | Child collection |
| `statements[]` | `chunks/*.json` | hundreds–thousands | **variable, `sqlText` unbounded** | Child collection; consider not storing `sqlText` (it's in `sources/`) |
| `tableOperations[].joins[]` | `chunks/*.json`, `api/nodes/*.json` | variable | ~200 B | Child collection |
| `operations.<verb>` line arrays | everywhere | up to **165 ints** on one edge, hundreds per table | 6 B/int | Child rows, or a compressed range encoding |
| `shortToFull` / `fullToId` | `api/node_id_map.json` | == node count ×2 | ~130 B | **Drop — two columns on the node table** |

---

## 7. Duplication across artifacts

Ranked by bytes wasted:

1. **`flow.json` duplicates everything.** `flowNodes[]` == the whole of `chunks/`; `callGraph[]` == the
   whole of `edges/`; `allTables[]` == `tables.json`'s values (`ChunkedFlowWriter.java:162-182`).
   ~3 MB of 100% redundancy per analysis, and nothing in `unified-web` reads it.
2. **`edges.json` == concatenation of `edges/edges_*.json`** (`:168-173`). Byte-for-byte redundant, and
   also unread by the web layer.
3. **`.node-cache/*.json` == `chunks/chunk_*.json`** — the same `FlowNode` serialised under two names
   (2.4 MB per analysis).
4. **`api/nodes/*.json` `directTables` == `tables`** — literally the same call twice
   (`ChunkedFlowWriter.java:568-569`).
5. **`subtreeTables[]` re-embeds the whole table usage graph in every node** — the entry node's copy
   alone reproduces the full `api/tables/index.json` usage data.
6. **`api/node_id_map.json` `shortToFull` and `fullToId` are exact inverses** of each other, and both are
   an index over data already in `api/index.json`.
7. **`resolver/q*.json` vs `cache/schema-resolver/*.tsv`** — the same Oracle dependency crawl in two
   formats at two grains (per-analysis JSON vs per-connection TSV).
8. **`tables.json` vs `flow.json.allTables` vs `api/tables/index.json`** — three representations of the
   same table-usage facts, differing only in key structure and enrichment.
9. **Archive twins** — every `<OBJECT>_yyyyMMdd_HHmmss` directory is a full copy of the analysis
   (`AnalysisService.pruneOldArchives:346-372` bounds how many are kept). In the sample this doubles
   the on-disk footprint exactly.
10. **`statements[].sqlText`** duplicates text that already exists verbatim in `sources/<file>` at the
    recorded `line`/`lineEnd`.

---

## 8. Derived vs source-of-truth

**Source of truth (must be stored, cannot be recomputed without re-running against a live DB):**

| Artifact | Why |
|---|---|
| `sources/<file>` | the only copy of the analysed DDL text |
| `chunks/chunk_NNNN.json` | primary ANTLR parse output |
| `edges/edges_NNNN.json` | primary call-graph output |
| `api/tables/index.json` — the `db*` fields | a **live catalog snapshot** at run time; needs its own `captured_at` |
| `resolver/q*.json` (Oracle) | a live `ALL_DEPENDENCIES` snapshot |
| `api/index.json` — `project`/`environment`/`connectionName`/`jdbcUrl`/`totalDurationMs` | run provenance, written nowhere else (`AnalysisService.java:918-941`) |
| `analysis_claude.json` | LLM verification output, non-reproducible |

**Derived / recomputable (should NOT be stored, or stored only as a materialised read cache):**

| Artifact | Derivable from |
|---|---|
| `flow.json` | `chunks/` + `edges/` + `tables.json` |
| `edges.json` | `edges/` |
| `tables.json` | `chunks/[].tableOperations` |
| `api/index.json` — `nodes[]`, counts, cycles, transactions | `chunks/` + `edges/` |
| `api/nodes/*.json` | `chunks/` + the subtree walk |
| `api/tables/index.json` — the static half | `chunks/` |
| `api/call_graph.json` | `edges/` + cycle detection |
| `api/node_id_map.json` | node table columns |
| `progress.json` | crawl state, worthless after completion |
| `.parse-cache/`, `.node-cache/` | pure caches |
| `cache/schema-resolver/*.tsv` | pure cache of the Oracle crawl |

**Rough saving:** of ~13 MB per analysis on disk today, roughly **8–9 MB is derived or duplicated**.

---

## 9. Write patterns

| Pattern | Artifacts |
|---|---|
| **Write-once, never touched again** | `chunks/chunk_NNNN.json`, `edges/edges_NNNN.json`, `sources/<file>`, `.parse-cache/*`, `.node-cache/*` |
| **Written once at end of run, full rewrite on rerun** | `flow.json` (twice: `:206`, `:216`), `edges.json`, `api/nodes/*.json`, `api/call_graph.json`, `api/node_id_map.json`, `resolver/q*.json` |
| **Rewritten in full on every state change** | `progress.json` (once per node state transition — `:104-122`), `tables.json` (once per `updateTables` call) |
| **Read-modify-rewrite by a later stage** | `api/tables/index.json` (writer → dialect enricher), `api/index.json` (writer → `TableValidationService.patchMainIndex:745` → `AnalysisService.patchConnectionMeta:934`) |
| **Merged across sub-analyses (package mode)** | `api/index.json` (`AnalysisService.java:734-736`), `api/tables/index.json` (`:802-803`), `api/node_id_map.json` (`:841-843`) |
| **Archived with a timestamp suffix** | the whole analysis directory → `<OBJECT>_yyyyMMdd_HHmmss`; pruned by `AnalysisService.pruneOldArchives:346-372` |
| **Nothing is ever appended** | despite the name, `appendEdges` (`:130`) does a whole-file write |

### 10.1 No manifests exist

There is **no `chunks/index.json`, `chunks/manifest.json`, `edges/index.json`** or any equivalent —
confirmed absent in all 10 analysis directories. Consumers must `list()` the prefix and sort by leaf
name, which is exactly what `ChunkedFlowWriter.listSorted` (`:85-93`) does. A database design gets this
for free (an index on `chunk_idx`), but any intermediate file-based migration tooling must replicate the
name-sort, and must tolerate the **sparse, gapped numbering** of `edges/` (observed for MySQL:
`edges_0001, 0003…0013, 0016, …` — 2, 14, 15 absent).

### 10.2 Archived twins are RE-RUNS, not copies

The `_20260723_HHMMSS` directories are **not byte-identical archives** of their unsuffixed siblings —
they are independent re-executions of the same crawl:

- `tables.json` and `progress.json`: identical.
- `flow.json`: always differs, in `crawlTimeMs` and (Oracle/WL2ZTC) `dbCallCount` — **259 vs 263**, i.e.
  the JDBC round-trip count is not deterministic.
- `.parse-cache/*` and `.node-cache/*`: differ only in `parseTimeMs` (timing telemetry embedded in the
  cached payload — `ParseResult.parseTimeMs`, `ParseResult.java:9`).
- `edges.json` / `api/*` / `chunks/*`: byte-identical **except for the SQL Server run (CIJ65E)**, whose two
  executions produced genuinely different node numbering and edge-file counts (`edges_0057.json` vs
  `edges_0059.json`, not aligned 1:1). **The crawl is not fully deterministic.**

**Schema-design consequences:**
1. Content hashing cannot be used to dedupe runs — embedded timing fields (`crawlTimeMs`, `parseTimeMs`,
   `totalDurationMs`, `dbCallCount`) guarantee every run differs. Timing must be split into a separate
   `run_metrics` record so the analytical payload *can* be hashed and deduped.
2. Node ordinals (`order`, `chunk_idx`) are **run-scoped, not stable across runs**. They are safe as a
   within-run key but must never be used as a cross-run identifier — use `(schema, package_name, object_name)`
   for that.

### 10.3 Data anomaly worth flagging

The SQL Server analysis (`USP_RUNMRPCYCLE__CIJ65E`) has only **1** file in `.node-cache/` versus 41–104
for the other four dialects, despite having 57 nodes. Either the node cache was mostly not written for
that run or it was cleared. This is a cache directory only (§3.18), so it has no correctness impact on
the artifacts — but it is a signal that `.node-cache/` population is unreliable and reinforces the
recommendation not to model it.

---

## 10. UI / API read map

Base path `@RequestMapping("/api/parser")` — `ParserAnalysisController.java:18`.

| Endpoint | Artifact | Reader | UI |
|---|---|---|---|
| `GET /analyses` | scan for `api/index.json` | `AnalysisService.indexKeysByName` | `parser/js/sync.js:85`, `api.js:17`, `plsql/js/api.js:59` |
| `POST /analyze` | produces the whole tree | `AnalysisService` → `FlowAnalysisMain.run` | `parser/js/api.js:25` |
| `DELETE /analyses/{name}` | deletes the tree | `AnalysisService:335-339` | `parser/js/history.js:230` |
| `GET /analyses/{name}/index` | `api/index.json` | `IndexFacetReader:33`, `ParserAnalysisReader:75` | `api.js:35`, `plsql/js/api.js:66` |
| `GET /analyses/{name}/procedures` | `api/index.json` → `nodes[]` | `IndexFacetReader` | `api.js:42`, `plsql/js/api.js:225` |
| `GET /analyses/{name}/node/{file}` | `api/nodes/{file}` | `ParserAnalysisReader:82`, `IndexFacetReader:100,111` | `api.js:49`, `plsql/js/api.js:123` |
| `GET /analyses/{name}/tables` | `api/tables/index.json` | `TableFacetReader:34,56-114`, `ParserAnalysisReader:86` | `api.js:56,84`, `txnTrace.js:49`, `plsql/js/api.js:160` |
| `GET /analyses/{name}/call-graph` | `api/call_graph.json` | `CoreFileReader:66,83-91`, `ParserAnalysisReader:90` | `api.js:149`, `plsql/js/api.js:111` |
| `GET /analyses/{name}/source/{file}` | `sources/{file}` (ext fallback) | `ParserAnalysisReader:93-107`, `CoreFileReader:97-136` | `api.js:156` |
| `GET /analyses/{name}/joins` | derived from `api/nodes/*` | `NodeScanFacetReader` | `api.js:95`, `plsql/js/api.js:192` |
| `GET /analyses/{name}/cursors` | derived from `api/nodes/*` | `NodeScanFacetReader` | `api.js:103`, `plsql/js/api.js:205` |
| `GET /analyses/{name}/sequences` | derived from `api/nodes/*` | `SequenceFacetReader` | `api.js:111`, `plsql/js/api.js:179` |
| `GET /analyses/{name}/dynamic-sql` | `api/nodes/*` + `sources/*` | `SequenceFacetReader` | `api.js:119` (no `plsql/js` twin) |
| `GET /analyses/{name}/call-tree/{nodeId}` | `api/node_id_map.json` + `api/call_graph.json` (+ `PackageTreeBuilder` for packages) | `CallTreeFacetReader` | `api.js:135`, `plsql/js/api.js:135` |
| `GET /analyses/{name}/call-tree/{nodeId}/callers` | same, reversed | `CallTreeFacetReader` | `api.js:142`, `plsql/js/api.js:147` |
| `GET /analyses/{name}/resolver/{type}` | `resolver/q{1_object_resolution,3_reverse_dependencies,4_direct_dependencies,1_ambiguous_objects}.json` | `ParserAnalysisReader:110-117` | **no UI caller — orphaned** |
| `POST /cache/clear` | in-memory only | | `api.js:11` |
| `GET /cache/by-connection`, `POST /cache/clear-connection` | `cache/schema-resolver/**` | `SchemaResolver` | `parser/js/cacheManager.js:41,124,146` |

**Never read over HTTP:** `flow.json`, `edges.json`, `edges/`, `chunks/`, `tables.json`, `progress.json`,
`.parse-cache/`, `.node-cache/`.

Facet endpoints emit derived shapes not present on disk:

- `/sequences` → `[{sequenceName, sequenceSchema, operations[], accessCount, accessDetails[]}]`
- `/dynamic-sql` → `[{procedureId, procedureName, schema, sourceFile, type, lineNumber, sqlExpression, usingVariables?}]`
- `/joins` → `[{leftTable, rightTable, joinTypes[], accessCount, accessDetails[]}]`
- `/cursors` → `[{cursorName, cursorType, queryText, operations[], accessCount, accessDetails[]}]`

These are **computed by scanning every `api/nodes/*.json`** on each request (in-memory cached by
`name::joins` / `name::cursors` / `name::sequences`) — i.e. they are exactly the queries a relational
index would make trivial, and are strong evidence for normalising joins/cursors/sequences into their own
tables.

---

## 11. Recommended grain for the database design

Reusing `ChunkedFlowWriter`'s existing boundaries:

| Collection / table | Grain | Fed by | Bounded? |
|---|---|---|---|
| `analysis` | `(db_type, project, environment, object_name, run_id)` | `flow.json` counters + `api/index.json` provenance patch | ✓ |
| `flow_node` | `(analysis_id, order)` / `(analysis_id, node_id)` | one `chunks/chunk_NNNN.json` + its `api/nodes/*.json` summary | ✓ per doc if `statements[]` is split out |
| `flow_node_statement` | `(analysis_id, node_id, seq)` | `chunks[].statements[]` | ✓ |
| `flow_node_call` | `(analysis_id, node_id, seq)` | `chunks[].calls[]` / `api/nodes[].calls[]` | ✓ |
| `flow_node_cursor` / `_variable` / `_parameter` / `_exception_handler` / `_dynamic_sql` | `(analysis_id, node_id, seq)` | corresponding arrays | ✓ |
| `flow_edge` | `(analysis_id, from_node_id, to_node_id, line)` | `edges/edges_NNNN.json`, keep `chunk_idx` | ✓ |
| `flow_edge_dedup` (optional materialised) | `(analysis_id, from_node_id, to_node_id)` | `api/call_graph.json` (`count`, `minDepth`, `backEdge`, `lines[]`) | ✓ |
| `table_ref` | `(analysis_id, schema, table_name)` | `tables.json` + `api/tables/index.json` static half | ✓ |
| `table_usage` | `(analysis_id, schema, table_name, node_id, operation, line)` | `usedBy[].operations` | ✓ |
| `table_join` | `(analysis_id, node_id, query_id, seq)` | `tableOperations[].joins[]` | ✓ |
| `db_column` / `db_index` / `db_constraint` / `db_trigger` | `(analysis_id, schema, table_name, …)` + `db_type` discriminator, all dialect-specific fields nullable | enricher output | ✓ |
| `source_file` | `(analysis_id, file_name)`, body as GridFS/blob ref | `sources/` | body unbounded → external |
| `resolver_object` / `resolver_dependency` / `resolver_ambiguity` | `(analysis_id, object_name, …)` | `resolver/q*.json` (Oracle only) | ✓ |
| `connection_scope_cache` | `(db_type, project, environment)` | `cache/schema-resolver/*.tsv` — **shared, not per-analysis** | ✓ |

**Do not persist:** `flow.json`, `edges.json`, `api/node_id_map.json`, `subtreeTables`, `directTables`,
`progress.json` (after completion), `.parse-cache/`, `.node-cache/`, `plsql-env-config.yaml`.

---

## 12. Open items

- **`analysis_claude.json`'s `tables` / `calls` element shapes** — UNKNOWN, needs the Claude writer class
  (outside `parser/**`).
- **Whether any endpoint serves `progress.json`** — UNKNOWN; not present in `ParserAnalysisController`.
  Needs a runtime check of the SSE/polling path.
- **`resolver/q3_reverse_dependencies.json` is empty in all 10 analyses, including Oracle.** Whether that
  is a bug or genuinely empty for this dataset — UNKNOWN, needs a runtime check with a schema that has
  reverse dependencies. Do not model it until confirmed.
- **Why the crawl is non-deterministic** (§10.2, SQL Server run produced different node counts across two
  executions) — UNKNOWN. Likely `HashSet`/`HashMap` iteration order in `DependencyCrawler`, but not
  verified. This matters: it means re-running an analysis is not idempotent.
- **The `.node-cache` shortfall on the SQL Server run** (1 file vs 57 nodes) — UNKNOWN, needs a runtime check.
- **The nested `<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>` layout has no on-disk sample** — `data/plsql/` is empty
  and the backup is flat. The nesting is confirmed from `AnalysisPaths.java` only. Verify against a real
  scoped run before finalising the key scheme.
- **The `chunk_%04d` / `edges_%04d` format caps at 9999** (`ChunkedFlowWriter.java:126,132`). Whether the
  crawler enforces a lower node cap — UNKNOWN, needs a check in `DependencyCrawler`.
- The 5 sample analyses are **all toy datasets (52–114 nodes)**. Every worst-case figure here is a linear
  extrapolation from per-node/per-table/per-edge averages; the `subtreeTables` and `lines[]` terms are
  super-linear, so real worst cases will be **larger** than projected, not smaller.
