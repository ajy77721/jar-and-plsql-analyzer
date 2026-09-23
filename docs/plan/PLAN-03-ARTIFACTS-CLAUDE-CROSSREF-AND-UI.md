# PLAN-03 — Remaining JSON Artifacts (Claude / Cross-Ref / Chat / Conversion / Prompts / Config) and the UI Data Contract

Scope: everything **not** covered by the core JAR-analysis and PL/SQL-parse artifact inventories — i.e. the Claude enrichment sidecar, cross-reference, chat, conversion, flow-analysis, prompt and config artifacts — plus a complete **UI data contract**: exactly what every screen fetches and which fields it renders.

Evidence base:

- Java: `unified-web/src/main/java/**` (all `File.java:LINE` citations below are relative to that root unless the path says otherwise).
- JS: `unified-web/src/main/resources/static/**`.
- Real data: `../data-backup-20260902-054631/**` (PL/SQL side, 2026-09-02 snapshot) and `data_old_1784577715/jar/**` (JAR side, the only surviving JAR dataset — `data-backup-.../jar` is empty).

**Snapshot caveat.** In `data-backup-20260902-054631` the directories `jar/`, `conversion/`, `claude-chatbot/`, `cache-plsql/` are all **0 bytes / empty**, and there is no `crossref/` or `flowanalysis/` tree anywhere on disk. Sizes for cross-ref, chat, conversion and flow-analysis artifacts are therefore **UNKNOWN — needs runtime check**; only their shapes (from code) are authoritative here. Sizes for the Claude PL/SQL sidecar, prompts, config and the JAR artifacts are measured.

---

# 1. Access-pattern table — what the database must serve

This is the contract the schema will be judged against. "Init" = fires on initial page load of that screen (not on a user click). "Poll" = repeats on a timer forever/until a terminal state.

## 1.1 Hot paths (polled — these are the ones that matter most)

| # | Entity | Filter | Sort | Expected result size | Init? | Frequency | Cite |
|---|---|---|---|---|---|---|---|
| P1 | Claude/analysis **session** | all sessions (global) | none (client filters) | tens–hundreds of rows | **yes, every JAR page** | **30 s, forever** | `jar/js/app.js:99`, `api.js:167` |
| P2 | Claude/analysis **session** | all | none | as P1 | no (modal) | 30 s while open | `jar/js/sessions.js:145` |
| P3 | **Claude progress** for one jarId | `jarId` | — | 1 doc, ~6 scalars + `errors[]` | no | 30 s while RUNNING (**two independent timers**) | `jar/js/app.js:537`, `jar/js/summary-claude.js:532` |
| P4 | **Corrections** for one jarId **+** session list | `jarId` | — | corrections map (all endpoints) + full session list | no | 30 s, **2 requests/tick** | `jar/js/summary-corrections.js:394,403,404` |
| P5 | **Log tail** | `lines=10000` | chronological | up to 10 000 lines (**MBs**) | no | 10 s when "Auto" ticked | `jar/js/logviewer.js:112`, `shared/log-fab.js:137` |
| P6 | **Log tail** | `lines=50`, then client `.slice(-20)` | chronological | 50 lines | no | **10 s hard-coded** | `jar/js/dash-panel.js:37,81,86` |
| P7 | **Queue** (global job list) | none | none | current + queued[] + history[] | **yes, home page** | **3 s** | `index.html:995` |
| P8 | **Queue** (global) | none, client-filters to PL/SQL types | none | as P7 | no | **2 s hard-coded** | `parser/js/perfSummary.js:416,430-446` |
| P9 | **Queue job** by id | `jobId` | — | 1 doc | no | 2 s (SSE fallback) | `parser/js/history.js:364,373`; `jar/js/app.js:419`; `jar/js/targeted.js:224` |
| P10 | **Claude verify progress** (parser) | `analysisName` | — | 7 scalars | no | **2 s hard-coded** (config says 30 s) | `parser/js/claudeInsights.js:164` |
| P11 | **Claude call-log stats + session list** (parser) | global | — | **entire persisted call log**, growing | no | **5 s hard-coded**, 2 requests/tick | `parser/js/claudeSessions.js:35,52-53` |
| P12 | **Log tail** (parser) | `lines=10000` | chronological | MBs | no | **3 s hard-coded** | `parser/js/logViewer.js:105` |
| P13 | **Conversion job** by id | `jobId` | — | whole job incl. `log[]` + `liveLines[]` (≤2000) + `instructionMd` | no | **1.5 s hard-coded** | `conversion/index.html:398,404` |
| P14 | **Flow-analysis run status** | `jarId`,`runId` | — | status doc + last 30 log lines + (on done) whole `results.json` | no | **1.5 s** chained `setTimeout` | `jar/js/flow-analysis.js:646,651` |
| P15 | **PL/SQL Claude progress** | `analysisName` | — | 6 scalars | no | 30 s (PollConfig — correct) | `plsql/js/app.js:815`, `claude.js:383`, `summary-claude.js:353`, `summary-progress.js:235` |
| P16 | **PL/SQL job status** | `jobId` | — | 1 doc | no | 10 s (PollConfig) | `plsql/js/app.js:1073,1359` |
| P17 | **Polling config** | none | — | 5 ints | **yes, every page** | once | `shared/poll-config.js:12` |

## 1.2 Initial-load reads

| # | Entity | Filter | Sort | Expected result size | Init? | Cite |
|---|---|---|---|---|---|---|
| I1 | **JAR list** | none | client: CORRECTED-before-STATIC, then `analyzedAt` desc, grouped by de-timestamped name | 14 fields × N jars | yes | `jar/js/api.js:9`; sort `jar/js/sidebar.js:44-67` |
| I2 | **JAR summary headers** | `jarId` [+`version`] | — | `_summary_static.json`-shaped: **430 KB measured** (84 endpoints, 119 classes) | yes, per JAR click | `jar/js/api.js:49`, `app.js:733` |
| I3 | **JAR versions** | `jarId` | — | 3 booleans/ints | yes, per JAR click | `jar/js/api.js:205` |
| I4 | **Connections** | `jarId` | — | ~130 B measured | yes, per JAR click (**fetched twice** — badge + modal) | `jar/js/api.js:63`, `app.js:773,798` |
| I5 | **Domain config** | `jarId` | — | 7 818 B measured | yes, per JAR click (**twice**) | `jar/js/api.js:69`, `app.js:882,897` |
| I6 | **Claude progress** | `jarId` | — | small | yes, per JAR click | `jar/js/app.js:1290` |
| I7 | **Claude call-log stats** | `jarName` | — | small per JAR, but **one request per JAR** (`Promise.allSettled` fan-out) | yes, dashboard | `jar/js/dashboard.js:69,80-82` |
| I8 | **Chat config** | none | — | 2 booleans | yes, every page | `shared/chat-toggle.js:19` |
| I9 | **PL/SQL analysis index** | `analysisName` | — | `api/index.json` = **87 KB measured** (52 nodes) | yes | `parser/js/api.js:35` |
| I10 | **PL/SQL tables index** | `analysisName` | — | `api/tables/index.json` = **307 KB measured worst case** — fetched **only to set one integer** | yes | `parser/js/app.js:74-81`; file `plsql-parse/ERP_USP_RUN_CYCLE__JZFCKN/api/tables/index.json` |
| I11 | **Analysis history** | none | client-side facet filters | N cards, full `errors[]` per row | yes | `parser/js/history.js:95-105`, `api.js:17` |
| I12 | **Parser Claude stats** | global | — | whole doc → 6 badges | yes, and again on every `goHome()` | `parser/js/home.js:213`; `router.js:45` |
| I13 | **Project/env config** | none | — | small | yes | `parser/js/home.js:23` |
| I14 | **Conversion job list** | none, capped 50 server-side | client: `startedAt` desc | ≤50 jobs, `liveLines` stripped | yes | `conversion/index.html:352`; server cap `ConversionController.java:93-96` |
| I15 | **Conversion Claude status** | none | — | 2 fields | yes | `conversion/index.html:344` |

## 1.3 Lazy reads (tab switch / row click / modal)

| # | Entity | Filter | Sort | Expected result size | Init? | Cite |
|---|---|---|---|---|---|---|
| L1 | **Class tree** | `jarId` [+`version`] | client: by sourceJar, then package, then name | full class list (8 000+ classes contemplated) | no | `jar/js/api.js:109`; `codeTree.js:1-7,293-305` |
| L2 | Resources / bundled-jars / jar-dep-map listing | `jarId` | — | `bundled-jars.json` = **3.03 MB measured** | no (fired together with L1) | `jar/js/codeTree.js:55-57,827,842` |
| L3 | **Call tree by endpoint index** | `jarId`,`endpointIdx`[,`version`] | — | **max 178 KB, avg 13.8 KB** measured over 84 endpoints | no, cached on the endpoint object | `jar/js/api.js:117`; `endpointList.js:95,101` |
| L4 | **Call trees, fan-out** | all endpoints touching collection X | — | **N × L3 in parallel `Promise.all`** | no | `jar/js/summary-popups.js:382-389` |
| L5 | **Endpoint nodes** (pre-flattened) | `jarId`,`endpointName` | — | **max 175 KB, avg 14.4 KB** measured | no | `jar/js/summary-trace.js:150` |
| L6 | **Single node source** | `jarId`,`endpointName`,`nodeIdx` | — | **max 6.2 KB** measured (560 node files) | no, only when `sourceCode` missing | `jar/js/summary-trace.js:306` |
| L7 | **Summary slices** (`external-calls`, `dynamic-flows`, `aggregation-flows`, `beans`) | `jarId`[,`version`] | positional by `endpointIdx` | one array element per endpoint | no; **discarded on tab-away, refetched on return** | `jar/js/summary.js:460-466,478-484,497-514`; drop at `summary.js:35-55` |
| L8 | **Decompiled class** | `jarId`,`fqn` | — | one class source | no | `jar/js/codeTreePanel.js:61`, `summary-codeview.js:89` |
| L9 | **DB catalog** | `jarId` | — | collections[] + views[] | no, only if some row has `verification == null` | `jar/js/api.js:262`, `summary-tables.js:691,696` |
| L10 | **Claude fragment listing** | `jarId` | — | `[{name,size}]`, one per chunk file | no, called from 4 sites | `jar/js/summary-claude.js:239,385,453,798` |
| L11 | **Claude fragment file** | `jarId`,`name` | — | ≤ **29.7 KB** measured (largest `chunk_*_input.json`) | no | `jar/js/summary-claude.js:395,851` |
| L12 | **Run logs / correction logs** listing + file | `jarId`[,`name`] | — | text | no | `jar/js/api.js:237,243,249,255` |
| L13 | **Claude session detail** | `jarName`,`sessionId` | — | small | no, memoised on row expand | `jar/js/api.js:288`, `summary-claude.js:748,763` |
| L14 | **Cross-ref history** | `jarId` | server: newest-first | one `meta.json` map per run | no; **auto-opens first result** → immediately triggers L15 | `jar/js/cross-ref.js:571,580`; server `CrossRefStore.java:113` |
| L15 | **Cross-ref result** | `jarId`,`crossRefId` | — | whole `result.json` (all `procLinks[]`) | no; **refetched in full after every Claude completion** | `jar/js/cross-ref.js:675,1069` |
| L16 | **Cross-ref Claude chunk** | `jarId`,`crossRefId`,`chunkId` | — | `prompt` + `response` + verdict | no | `jar/js/cross-ref.js:1156,2075` |
| L17 | **Cross-ref source (java / oracle / oracle-tree)** | `nodeId` / `procRef` | — | source text / tree | no | `jar/js/cross-ref.js:1301,1401,1571` |
| L18 | **Available Oracle procs** | `jarId` | client: substring filter, client: count of `analyzed` | full list | no | `jar/js/cross-ref.js:354,361,375-379` |
| L19 | **Flow-analysis history** | `jarId` | client: by `mode` | one status doc per run | no; **also fetched by cross-ref purely to build a Set of analyzed endpoint paths** | `jar/js/flow-analysis.js:377`; `cross-ref.js:108,111-113` |
| L20 | **PL/SQL tables document** | `analysisName` | — | **307 KB measured**; fetched **≥5 times per session** (topbar, tableOps, leftPanel, tableDetail, txnTrace) | no | `parser/js/app.js:74`, `tableOps.js:8`, `leftPanel.js:178`, `tableDetail.js:127`, `txnTrace.js:49` |
| L21 | **PL/SQL node detail** | `analysisName`,`nodeFile` | — | **max 370 KB measured** (`api/nodes/ERP$USP_RUNMRPCYCLE.json`) | no | `parser/js/api.js:49` |
| L22 | **PL/SQL node detail, fan-out ×2** | every node in the table's call-tree scope | — | **N × L21 in `Promise.all`, twice per table modal** | no | `parser/js/tableDetail.js:294-296,439-441` |
| L23 | **PL/SQL call graph** | `analysisName` | — | `api/call_graph.json` = **84 KB measured** (218 edges) | no | `parser/js/api.js:149`, `callGraphViz.js:26` |
| L24 | **PL/SQL joins / cursors / sequences / dynamic-sql** | `analysisName` | — | per-section documents | no; **all five refetched by the XLSX export** | `parser/js/api.js:95,103,111,119`; export `parser/js/export.js:145-160` |
| L25 | **PL/SQL source file** | `analysisName`,`sourceFile` | — | whole file text; client slices `lineStart..lineEnd` | no | `parser/js/api.js:156`; slicing `triggerDetail.js:156-165` |
| L26 | **Claude verify result** | `analysisName` | — | `_result.json` = **138 301 B measured** (34 tables, 445 ops, 139 calls) | no; fetched by ≥5 screens to show 3–7 scalars | `parser/js/api.js:185`; file `plsql/ERP_USP_RUN_CYCLE__WL2ZTC/claude/_result.json` |
| L27 | **Claude chunk summary list** | `analysisName` | — | 25 rows measured | no | `parser/js/api.js:191` |
| L28 | **Claude chunk detail** | `analysisName`,`chunkId` | — | input+output; **input up to 29 729 B measured** | no, memoised | `parser/js/api.js:197`; `chunkViewer.js:140-151` |
| L29 | **Chat session list** | `scope`,`chatType` | — | id/name/messageCount/updatedAt per session | no, on panel open | `shared/chat-api.js:14-21` |
| L30 | **Chat session** | `sessionId` | — | **entire transcript**, unbounded | no | `shared/chat-api.js:25`; render `chat.js:220-257` |
| L31 | **Conversion report** | `jobId` | — | markdown file | no, on Report tab | `conversion/index.html:426,567` |
| L32 | **Prompt templates / extras / per-analysis** | `name` | — | 14 templates; largest measured **5 579 B** (`java-both-correction.txt`) | no | `parser/js/promptManager.js:18,33,39` |

## 1.4 Writes

| # | Entity | Trigger | Cite |
|---|---|---|---|
| W1 | Claude review decision (`userDecision`) | per-row and bulk | `parser/js/tableDetail.js:1512,1527`; `claudeCorrections.js:316,357,370` |
| W2 | Apply Claude changes | user | `parser/js/claudeEnrichment.js:490,671`; server `ClaudePersistenceService.java:405-409` |
| W3 | Version load/revert (static / claude / prev) | user | `parser/js/api.js:217,225,233,251` |
| W4 | Chat message | per user turn (rewrites whole session doc) | `ChatService.java:165,175,177,278-289` |
| W5 | Conversion job start + every milestone | 6 sites | `ConversionController.java:152,169,173,179,198,203` |
| W6 | Cross-ref run + per-chunk result flush | per chunk | `CrossRefClaudeService.java:1306` |
| W7 | Flow-analysis status | ~10 state changes/run | `FlowAnalysisController.java:131-260` |
| W8 | Flow-analysis log line | per event | `FlowRunStore.java:134-147` |
| W9 | Claude call-log line | per CLI call incl. retries | `ClaudeCallLogger.java:96-104` |
| W10 | Prompt template / extras / per-analysis override | user | `PromptConfigService.java:153,218,254` |

## 1.5 What this table implies for the schema

1. **P1/P2/P4 are the permanent load.** A session list is fetched every 30 s on every open page for the lifetime of the session, and P4 fetches it a second time. Both consumers only want a boolean (`sessions.some(s => s.status === 'RUNNING')`, `jar/js/app.js:86`, `summary-corrections.js:405`). The schema needs a cheap "any running session" predicate — a partial index on `status='RUNNING'`, or a counter.
2. **P11/P13/L26 fetch whole growing documents on a timer.** P11 re-reads the entire Claude call log every 5 s; P13 re-reads the whole conversion job (log + up to 2000 live lines + the immutable `instructionMd`) every 1.5 s. These need **cursor-based incremental reads** (`?sinceLine=`), which in turn means the append-only artifacts must be modelled as *rows*, not as an array inside one document.
3. **No endpoint anywhere accepts `page`, `offset`, `limit`, or a cursor.** Every list in every screen is fetched whole and paged in the browser (`jar` side: `summary-helpers.js:532`; `parser` side: `pagination.js:33-40`, `tableFramework.js:33-56`). If the schema is designed around server pagination, the UI must be changed in lockstep — otherwise pagination buys nothing.
4. **Positional coupling.** `L7` merges summary slices back into the header response **by array index** (`jar/js/summary.js:497-514`). Any schema that does not guarantee identical endpoint ordering across two queries silently corrupts the UI. Endpoints need a stable id.
5. **L22 and L4 are unbounded fan-outs.** One table-detail modal issues two `Promise.all` batches of *N* node-detail fetches (`parser/js/tableDetail.js:294-296,439-441`); one collection popup issues *N* call-tree fetches (`jar/js/summary-popups.js:382-389`). The schema must be able to answer "joins/columns for table T" and "call paths touching collection C" as single queries, or these stay N+1.

---

# 2. PART A — Artifact inventory

## 2.1 Path vocabulary

All writes go through `com.analyzer.storage.ArtifactStore`, rooted at `app.data-dir.base`. Keys below are relative to that root. `ArtifactStore.write` is **atomic-on-close** (temp file then move) and has **no append operation** — documented at `ClaudeCallLogger.java:87-95` and again at `FlowRunStore.java:137-138`. This is the single most consequential fact in this document (see §2.9).

PL/SQL key resolver — `parser/service/AnalysisPaths.java`:

- `PARSE_ROOT = "plsql-parse"` (`:132`), `CLAUDE_ROOT = "plsql"` (`:135`)
- `resolveKey(name, rel)` → `plsql-parse/<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>/<rel>` — splits `name` on `~` (`:138-144,154-160`)
- `claudeFileKey(name, rel)` → `plsql/<name>/<rel>` — **flat, `~` NOT split** (`:163-174`)

So one identity maps to a *nested* parse tree and a *flat* Claude sidecar. Any schema must not assume one canonical decomposition of the analysis name.

JAR key root — `jar-analyzer-core/.../JarDataPaths.java:104,115-125` → `jar`; `WarDataPaths` overrides to `jar/war`.

## 2.2 Claude PL/SQL enrichment sidecar — `plsql/{name}/**`

Measured from `data-backup-20260902-054631/plsql/ERP_USP_RUN_CYCLE__WL2ZTC/`: **1.1 MB per analysis, 79 files in `claude/`**, 4 sibling run directories present (same content, timestamp-suffixed names).

### 2.2.1 `plsql/{name}/analysis_claude.json` and `.../claude/_result.json`

Writer: `ClaudePersistenceService.java:70,78-80` (sidecar) and `parser/service/ClaudeVerificationService.java:183` (`_result.json`).
**Measured: 138 301 B each, and byte-identical** (`cmp` returns equal) — the same document is stored twice per run.

Content measured: 34 tables, 445 total `claudeVerifications` entries, 139 calls, 25 chunks.

Full field tree (DTO `parser/service/ClaudeVerificationModels.java:125-148`; `INDENT_OUTPUT` on at `ClaudePersistenceService.java:43`, no `@JsonInclude`, so **nulls are emitted**):

```
analysisName        String   nullable   the ~-joined identity name
timestamp           String   nullable   LocalDateTime.now().toString() — local, NO ZONE
tables              List<TableVerificationResult>   non-null, default []
  tableName            String   nullable   may be "SCHEMA.TABLE"; split on first '.' at ClaudePersistenceService.java:361-365
  overallStatus        String   nullable   CONFIRMED | PARTIAL | EXPANDED | MODIFIED | UNVERIFIED
  staticOperations     List<String>   non-null []   op names from static analysis
  claudeVerifications  List<OperationVerification>   non-null []
    operation      String  nullable  SELECT|INSERT|UPDATE|DELETE|MERGE|DYNAMIC; upper-cased on apply (:487)
    status         String  nullable  CONFIRMED | REMOVED | NEW
    procedureName  String  nullable  fuzzily matched vs static nodeName (:588-593)
    lineNumber     int               0 = unknown / whole-op
    reason         String  nullable  Claude's justification (free text, ~50-70 chars observed)
    userDecision   String  NULLABLE  null | ACCEPTED | REJECTED — set by review, drives applyAcceptedChanges (:370)
  triggers          List<Map<String,Object>>   NULLABLE (no default) — filled by TriggerReAnalysisService, free-form
calls               List<CallVerification>   non-null []
  fromProcedure  String  nullable
  toProcedure    String  nullable
  status         String  nullable  CONFIRMED | REMOVED | NEW
  lineNumber     int
  reason         String  nullable
  userDecision   String  nullable
confirmedCount      int    measured 438
removedCount        int    measured 0
newCount            int    measured 7
callConfirmedCount  int    measured 122
callRemovedCount    int    measured 16
callNewCount        int    measured 1
totalChunks         int    measured 25
errorChunks         int    measured 0
claudeCallCount     int    measured 25
claudeTimeMs        long   measured 312569
error               String nullable — ALSO a non-error status carrier:
                           "Partial result (3/10 chunks completed)" (ClaudeVerificationService.java:233)
```

- **Cardinality**: one per analysis name — **not** one per run. Runs overwrite.
- **Write pattern**: **rotated then rewritten**. `store.move(claudeFile → claudePrev)` at `:73`, then `mapper.writeValue` at `:79`. Depth-1 history only; the old `_prev` is destroyed. `updateClaudeResult` (`:100-110`) rewrites **in place without rotation** — deliberately, so a user review decision does not consume the history slot. `revertClaude` (`:159-171`) moves prev→current, destroying prev.
- **Size**: observed 138 KB for 34 tables / 445 ops. Worst case scales linearly with `tables × claudeVerifications` and with `calls`; the biggest measured PL/SQL analysis has 42 tables and 218 unique edges, so a same-shape run is **UNKNOWN but plausibly ~1 MB**; unbounded in principle.
- **Identity**: `analysisName`. `tables[].tableName` joins to `plsql-parse/.../api/tables/index.json`; `claudeVerifications[].procedureName` joins to static `usedBy[].nodeName`.

### 2.2.2 `plsql/{name}/analysis_claude_prev.json`

Byte-identical shape; it *is* the previous file, moved. `ClaudePersistenceService.java:71-73,146-149`. One per analysis, depth 1.

### 2.2.3 `plsql/{name}/metadata.json`

Writer: `ClaudePersistenceService.java:598-630` (fields built `:613-624`). Read-modify-write, so unknown pre-existing keys survive. **Measured 267 B.** Real content:

```
name             String  identity name              measured "ERP_USP_RUN_CYCLE__WL2ZTC"
mode             String  literal "claude" once written (:614); getVersionInfo defaults "static" when absent (:213)
claudeIteration  int     prev+1, starts at 1 (:615-617) — the ONLY run counter that survives
lastUpdated      String  LocalDateTime.now().toString()
confirmedCount   int     measured 438
removedCount     int     measured 0
newCount         int     measured 7
totalTables      int     measured 34 (result.tables.size(), 0 if null, :622)
totalChunks      int     measured 25
errorChunks      int     measured 0
```

Write pattern: **rewritten in place** on every save. One per analysis.

**File-existence dependency:** `listClaudeEnriched` (`:242-269`) discovers analyses purely by the presence of `metadata.json` or `analysis_claude.json` at exactly depth 3 (`plsql/<name>/<file>`, `:251`).

### 2.2.4 `plsql/{name}/claude/_index.json`

Writer: `parser/service/ClaudeVerificationService.java:1037-1051` (via `:135`). **Measured 4 642 B, 25 chunks.**

```
chunks   List<Map>
  id          String  e.g. "chunk_1"
  name        String  e.g. "ERP_USP_RUN_CYCLE [L1-501] -> 29 tables"
  nodeIds     List<String>   e.g. ["ANALYZER$ERP_USP_RUN_CYCLE"]   <-- parser stack
  tableCount  int     ChunkingUtils.countDistinctTables() — DISTINCT, not raw op count
timestamp  String  LocalDateTime.now().toString()
```

Write pattern: written once per run **and overwrites the previous run's index** — that is how resume works. One per analysis.

**Divergence:** the legacy stack writes the same filename with `procedures` + `procedureCount` instead of `nodeIds`, and `tableCount` = raw map size, not distinct (`service/ClaudeFragmentWriter.java:54-69`, esp. `:61-63`). `ClaudeChunkReader.listChunkSummaries` reads `procedures` (`service/ClaudeChunkReader.java:183`) and therefore returns `null` against a parser-written index.

### 2.2.5 `plsql/{name}/claude/_checkpoint.json`

Writer: `parser/service/ClaudeVerificationService.java:1064` via `:501`. **Measured 293 B.** Shape: a **bare JSON array of chunkId strings**, no wrapper object — measured `[ "chunk_7", "chunk_6", ... ]`, 25 entries, unordered (written from a `synchronized` `Set`, `:416`).

Write pattern: **rewritten in full after every chunk completion**, from concurrent chunk threads. Logically append-only, physically N full rewrites per run.

### 2.2.6 `plsql/{name}/claude/{chunkId}_input.json`

Writer: `parser/service/ClaudeVerificationService.java:460-468`. **Measured: 25 files, max 29 729 B, sum 1 955 652 B over 4 runs (100 files).**

```
chunkId       String   "chunk_1"
name          String   chunk label (same as _index.json chunks[].name)
nodeIds       List<String>
tableCount    int      distinct tables
promptLength  int      characters
timestamp     String   LocalDateTime.now().toString()
prompt        String   THE FULL PROMPT TEXT — measured 28 079 chars; this is the size hog
```

Legacy twin writes `procedures` instead of `nodeIds` (`service/ClaudeVerificationRunner.java:127-133`). One per chunk, written once.

### 2.2.7 `plsql/{name}/claude/{chunkId}_output.json`

Writer: `parser/service/ClaudeVerificationService.java:495`. **Measured: max 16 202 B, 25 per run.**

**The `.json` extension lies.** `saveFragment(dir, id + "_output.json", response)` passes the raw CLI string, and the `data instanceof String` branch (`:1022-1026`) writes raw UTF-8. Verified on real data: `chunk_1_output.json` begins with a markdown fence — its literal first bytes are ` ```json ` followed by a newline. Every reader defends against this (parse-then-fall-back-to-raw at `:364-388`, `:1107-1108`; `ClaudeChunkReader.java:134` even falls back to a regex `"tableName"\s*:\s*"([^"]+)"`). The chunk viewer strips the fence **client-side** (`parser/js/chunkViewer.js:199-206`).

Expected inner shape when it parses (`service/ClaudeResponseParser.java:50-77`):

```
tables      List
  tableName   String
  operations  List
    operation      String
    status         String
    procedureName  String
    lineNumber     Number → int, 0 if absent/non-numeric (:68)
    reason         String
summary     String
```

Everything else in the model payload is ignored.

### 2.2.8 `plsql/{name}/claude/{chunkId}_summary.txt` and `{chunkId}_error.txt`

Plain UTF-8, written only when present. Summary measured 280–589 B (25 files). Error body is the literal `"No response after 2 attempts"` (`parser/service/ClaudeVerificationService.java:491`) / `"No response from Claude CLI after 2 attempts"` (legacy, `ClaudeVerificationRunner.java:151`).

### 2.2.9 ⚠️ APPEND-ONLY — `plsql/{name}/claude/_call_log.jsonl`

Writer: `parser/service/ClaudeCallLogger.java`; key built literally at `:170-172` (`CLAUDE_ROOT="plsql"` `:35`, `CALL_LOG_FILE="_call_log.jsonl"` `:33`).
**Measured 6 880 B / 25 lines** for a 25-chunk run.

One JSON object per line (`:54-65`), all keys verified present in the real file:

```
ts             String   LocalDateTime.now().toString() — local, no zone. measured "2026-07-21T22:50:18.561829"
sessionId      String   nullable  UUID minted per chunk (ClaudeVerificationService.java:473)
sessionType    String   nullable  "VERIFY" from the parser verifier (:482)
endpoint       String   nullable  MISNOMER — holds the CHUNK NAME, not a URL (:482)
chunkIndex     int      the 1-based `processed` counter, NOT the chunk id
promptBytes    long     MISNOMER — prompt.length() in CHARS (:483)
responseBytes  long     MISNOMER — response.length() in CHARS (:484)
durationMs     long     wall clock of one CLI invocation. measured 30292
success        boolean
error          String   NULLABLE — null on success; "empty/failed response (attempt N)" otherwise (:485)
```

**Write pattern — logically append, physically full rewrite** (`:96-104`):

```java
boolean existed = store.exists(key);
try (OutputStream out = store.write(key)) {
    if (existed) { try (InputStream in = store.read(key)) { in.transferTo(out); } }
    out.write(line.getBytes(StandardCharsets.UTF_8));
}
```

Cost is **O(n²) bytes written** over a run. Guarded by `synchronized (this)` (`:67`) — process-local only; two JVMs against one store would lose lines. Never rotated, never truncated. All exceptions swallowed (`:70-72`).

Cardinality: one file per analysis; **one line per Claude CLI call including retries**, so a failed-then-retried chunk yields two lines with the same `sessionId`.

Join problem: there is **no `chunkId` field**, so joining a log line to `{chunkId}_output.json` requires going via `endpoint` (the chunk name).

`getGlobalStats` (`:107-118`) discovers analyses by **listing directories under `plsql/`** and using `dir.leaf()` as the analysis name — another file-layout dependency.

### 2.2.10 `plsql-parse/{…name…}/claude/tables_merged.json`

Writer: `ClaudePersistenceService.java:405-409`; read `:118-125`; **deleted on every fresh verification save** (`:84`) so a stale merge cannot shadow new results. Note it uses `resolveKey` (the nested `plsql-parse` tree), unlike everything else in that class.

It is a **mutated deep copy of the static `api/tables/index.json`** (read `:343-345`), so it inherits every field of that file. Guaranteed/mutated fields:

```
totalTables       int    overwritten :575
operationSummary  Map<String,Integer>  op -> count of tables having it, overwritten :576-585
tables            List<Map<String,Object>>
  schema         String  omitted when empty for newly created entries (:539-541)
  name           String
  objectType     String  "TABLE" for created entries (:543)
  allOperations  List<String>  recomputed from usedBy (:560-569)
  usedBy         List<Map<String,Object>>   empty-operation entries pruned (:553-556);
                                            tables with empty usedBy pruned entirely (:396-399)
    nodeId      String  created entries: procedureName with '.'→'$' (:504)
    nodeName    String
    nodeDepth   int     0 for created entries (:507)
    operations  Map<String,List<Integer>>  OP -> sorted line numbers (:518-531)
```

Base file measured at **307 401 B worst case** (`plsql-parse/ERP_USP_RUN_CYCLE__JZFCKN/api/tables/index.json`), so the merged copy is the same order.

### 2.2.11 `plsql/improvement/run_{safeName}_{timestamp}.md` — accumulating, never pruned

Writer: `service/ClaudeRunReporter.java:34-39,101-104`. Markdown, not JSON. The only artifact written outside a per-analysis folder, and the only one that **accumulates by name**:

```java
ArtifactKey improvDir = ArtifactKey.of(AnalysisPaths.CLAUDE_ROOT, "improvement");
String timestamp = LocalDateTime.now().toString().replace(":", "-").substring(0, 19);
String safeName = (vr.analysisName != null ? vr.analysisName : "unknown").replaceAll("[^a-zA-Z0-9_-]", "_");
ArtifactKey reportFile = improvDir.resolve("run_" + safeName + "_" + timestamp + ".md");
```

- `safeName` maps `~` → `_`, so it is **not reversible** to the identity name: `DB2~X~local~P` and `DB2_X_local_P` collide.
- `improvement` is a sibling of every analysis under `plsql/`, i.e. a **pseudo-analysis name** in that namespace — it would appear in `ClaudeCallLogger.getGlobalStats`'s directory scan.
- Sections: header bullets (`:51-54`), a Results Summary table (`:56-63`), "New Findings" when `newCount > 0` (`:66-81`), "Removed Findings" when `removedCount > 0` (`:84-99`).
- The three table counts are derived at `:42-47`: `UNVERIFIED`→unverified, `MODIFIED`/`PARTIAL`→modified, everything else **including null**→verified.
- Write-once, second-resolution filename, **no retention or pruning anywhere** — the directory grows one file per run forever.
- **Only the legacy stack calls it** (`ClaudeVerificationRunner.java:213`); the parser stack produces no run reports at all.

### 2.2.12 Read-only helpers

- `service/ClaudeResultLoader.java` — writes nothing. Reads `_result.json` (`:59-62`), `_checkpoint.json` (`:167-177`), `_index.json` for `chunks.size()` only (`:102-110`), and directory listings for `_result.json` / `*_output.json` presence (`:72-74`). `getProgress` (`:120-165`) returns `completedChunks, totalChunks, errorChunks, percentComplete, hasResult, hasPartialResult, isComplete`; the parser twin (`parser/service/ClaudeVerificationService.java:262-270`) additionally returns `claudeCallCount` and `claudeTimeMs`.
- `service/ClaudeChunkReader.java` — writes nothing. `getChunkFragment` → `{chunkId, input, output, error}` (`:58-101`); `getTableChunkMapping` → `Map<UPPERCASE tableName, List<chunkId>>` (`:103-144`); `listChunkSummaries` → `{id, name, procedures, tableCount, status, error?}` where status is COMPLETE / ERROR / PENDING **decided by file existence** (`:188-195`).
- `service/ClaudeResponseParser.java` — writes nothing. `extractJson` (`:86-122`) tolerates, in order: bare `{…}`, a ```` ```json ```` fence, a bare ``` ``` ``` fence, then first-`{`-to-last-`}`. Failure sets `cr.error` (`:41-45`, `:79`) which is **never persisted to `_error.txt`** — it surfaces only via `_result.json`'s `errorChunks`.

### 2.2.13 The two-stack divergence (schema risk)

Both stacks write the **same key space** with **different field names and different DTOs**:

| Aspect | parser stack (`parser/service/`) | legacy stack (`service/`) |
|---|---|---|
| `_index.json` chunk node field | `nodeIds` (`ClaudeVerificationService.java:1044`) | `procedures` + `procedureCount` (`ClaudeFragmentWriter.java:61-62`) |
| `{chunkId}_input.json` node field | `nodeIds` (`:463`) | `procedures` (`ClaudeVerificationRunner.java:129`) |
| `tableCount` meaning | distinct tables | raw `chunk.tables.size()` |
| `VerificationResult.timestamp` | `String` | real `LocalDateTime` (`ClaudeVerificationService.java:178-188`) |
| Fields only in parser | `calls[]`, `call*Count`, `userDecision`, `triggers`, `claudeCallCount`, `claudeTimeMs` | — |
| Fields only in legacy | — | `schemaName`, `staticAccessCount`, `isExternal`, `procedureId`, `sourceFile` |

`FAIL_ON_UNKNOWN_PROPERTIES` is disabled on both (`ClaudeResultLoader.java:59`), so a cross-read **silently drops** fields rather than erroring — including `userDecision`, i.e. user review decisions can be discarded without a signal. A unified schema must pick one representation and migrate both.

## 2.3 Cross-reference — `jar/crossref/{jarId}/{crossRefId}/**`

Run dir template: `CrossRefStore.java:47,62-64`; keys sanitised by `safe()` (`:217-219`, `[^a-zA-Z0-9._-]` → `_`). `crossRefId = String.valueOf(System.currentTimeMillis())` (`CrossRefService.java:139`).

**Size: UNKNOWN — needs runtime check.** No `crossref/` tree exists in either data snapshot.

### 2.3.1 `meta.json`

Writer `CrossRefStore.java:86-101` (write at `:97`), called once per run (`CrossRefService.java:153`). Written once; a re-run is impossible (ms-timestamp id).

| field | type | null? | meaning |
|---|---|---|---|
| `crossRefId` | String | no | epoch-ms run id; equals the dir segment |
| `jarId` | String | no | equals the dir segment |
| `endpointPath` | String | no | HTTP endpoint analysed |
| `procName` | String | no | Oracle package/proc requested |
| `schema` | String | **yes** | null when the user omitted it |
| `mode` | String | no | `STATIC_INFERENCE` \| `FULL_COMPARISON` |
| `timestamp` | long | no | `System.currentTimeMillis()` at write (`:96`) |

### 2.3.2 `result.json` — the whole `CrossRefResult`

Writer `CrossRefStore.writeResult` `:117-123` (write at `:119`). **Rewritten in place repeatedly during Claude enrichment**: `CrossRefClaudeService.java:259` (final merge), `:287` (mergeOnly), and **`:1306` per chunk under a per-crossRefId lock**. So one run performs O(chunks) full rewrites of a document whose size grows with `procLinks`.

Top level (`crossref/model/CrossRefResult.java`):

```
crossRefId               String   non-null  (:16)
jarId                    String   (:19)
endpointPath             String   (:22)
procName                 String   (:25)
schema                   String   nullable  (:28)
mode                     String   STATIC_INFERENCE | FULL_COMPARISON  (:35)
timestamp                long     (:38)
plsqlSessionId           String   NULL in STATIC_INFERENCE (:41) — FK to plsql-parse/{sessionId}/
summary                  CrossRefSummary  nullable (:44)
procLinks                List<CrossRefProcLink>  default [] (:47)
routingNodes             List<RoutingNode>       (:54)
javaToOracleMap          Map<String,List<String>>  javaKey -> oracle procRefs (:60)
oracleToJavaMap          Map<String,List<String>>  (:66)
infraNodes               List<InfraNode>         (:72)
oracleNodes              List<OracleNode>        (:79)
tableToJavaMap           Map<String,List<String>>  (:85)
javaToTableMap           Map<String,List<String>>  (:90)
javaOnlyProcs            List<JavaOnlyProc>      (:95)
oracleOnlyProcs          List<OracleOnlyProc>    (:98)
operationGaps            List<OperationGap>      (:101)
lowConfidenceHints       List<CrossRefProcLink>  (:104)
claudeOverallVerdict     String  null until enrichment (:109)
claudeOverallConfidence  String  nullable (:112)
claudeOverallReason      String  nullable (:115)
claudeOverallGaps        List<String>  (:118)
claudeConversionPct      int     default 0 (:121)
claudeOverallChunkId     String  nullable (:124) — FK to claude/{chunkId}.json
```

`summary` — `CrossRefSummary.java:6-26`, **all `int`, never null once set**:
`totalJavaNodes, highConfidenceLinks, mediumConfidenceLinks, lowConfidenceLinks, unmatchedJavaNodes, plsqlNodesFound, totalJavaCollections, matchedTables, javaOnlyCollections, oracleOnlyTables, operationMismatches, parameterMatchCount, parameterInferredCount, javaOnlyProcCount, oracleOnlyProcCount, operationGapCount, lowConfidenceHintCount`.

`procLinks[]` / `lowConfidenceHints[]` — `CrossRefProcLink.java:17-130`:

```
javaClass          String  (:17)
javaMethod         String  (:20)
javaStereotype     String  (:23)
javaOperationType  String  (:26)
javaNodeId         String  nullable (:29)
oracleProcRef      String  nullable (:38)
inferredProcRef    String  (:45)
resolvedProcRef    String  (:48)
matchConfidence    String  HIGH | MEDIUM | LOW (:57)
plsqlNodeFound     boolean (:60)
zoneSource         String  e.g. "PASS_B" (:70)
hasLogParameters   boolean (:73)
javaCollections    List<CrossRefCollectionEntry> (:78)
    collection  String
    operation   String
    sequence    String  nullable                     [CrossRefCollectionEntry.java:6-8]
oracleTables       List<CrossRefOracleTableEntry> (:84)
    tableName   String
    schema      String
    operation   String  SELECT|INSERT|UPDATE|DELETE|MERGE
    sourceLine  Integer nullable                     [CrossRefOracleTableEntry.java:6-9]
tableMatches       List<CrossRefTableMatch> (:87)
    mongoCollection  String  nullable
    oracleTable      String  nullable
    javaOperation    String  nullable
    oracleOperation  String  nullable
    operationMatch   Boolean nullable
    matchStatus      String
    oracleSequence   String  nullable                [CrossRefTableMatch.java:10-39]
parameterMatches   List<CrossRefParamMatch> (:90)
    javaLiteral  String
    oracleParam  String  nullable
    matched      Boolean nullable
    oracleMode   String  nullable                    [CrossRefParamMatch.java:11-23]
cursors            List<Map<String,Object>>  FREE-FORM (:96)
subCalls           List<Map<String,Object>>  FREE-FORM (:102)
javaOnlyCollections List<String> (:107)
oracleOnlyTables   List<String> (:110)
claudeChunkId      String  nullable (:115) — null on disk unless a chunk was applied,
                           see the comment at CrossRefClaudeService.java:471-472
claudeVerdict      String  CONVERTED | PARTIAL | NOT_CONVERTED (:118)
claudeConfidence   String  (:121)
claudeReason       String  (:124)
claudeGaps         List<String> (:127)
claudeDone         boolean (:130)
```

`routingNodes[]` — `RoutingNode` (`:133-140`): `javaClass String, javaMethod String, javaStereotype String, calledOraclePackages List<String>, directProcCallers List<String>`.

`oracleNodes[]` — `OracleNode` (`:165-174`): `nodeId String (SCHEMA$PACKAGE$PROC), packageName String, objectName String, schema String, objectType String (PROCEDURE|FUNCTION), depth int, parentNodeId String nullable, javaCallers List<String>, javaTableCallers List<String>`.

`javaOnlyProcs[]` — `JavaOnlyProc` (`:198-210`): `javaClass String, javaMethod String, stereotype String, operationType String, tables List<String>, reason String, claudeChunkId String nullable, claudeVerdict String (NOT_CONVERTED|HAS_ORACLE_EQUIVALENT|INFRA_ONLY|UNKNOWN), claudeConfidence String, claudeReason String, claudeDone boolean`.

`oracleOnlyProcs[]` — `OracleOnlyProc` (`:244-255`): `procRef String (PKG.PROC), packageName String, objectName String, depth int, tables List<String>, claudeChunkId String nullable, claudeVerdict String (NOT_IN_JAVA|HAS_JAVA_EQUIVALENT|INTERNAL_ONLY|UNKNOWN), claudeConfidence String, claudeReason String, claudeDone boolean`.

`operationGaps[]` — `OperationGap` (`:286-292`): `table String, javaOperation String nullable (READ|WRITE), oracleOperation String nullable (SELECT|INSERT|UPDATE|DELETE), javaNode String, oracleProc String, gapType String (JAVA_ONLY_TABLE|ORACLE_ONLY_TABLE|OP_MISMATCH)`.

`infraNodes[]` — `InfraNode` (`:315-318`): `javaClass String, javaMethod String, reason String (LOCK_MANAGEMENT|LOGGING|BATCH_INFRA|SEQUENCE_SERVICE)`.

### 2.3.3 `claude/{chunkId}.json`

Writer `CrossRefClaudeService.writeChunkResult` `:1163-1172` (write at `:1167`), from `processChunk` `:407`. **Written once per chunk**, skipped on resume when already in the checkpoint (`:209`).

`chunkId = stableHash(prefix)` where prefix is one of four literals: `"LINK|"+javaKey+"|"+oracleRef+"|"+plsqlSessionId` (`:149`), `"JAVA_ONLY|"+class#method+"|"+sessionId` (`:157`), `"ORACLE_ONLY|"+procRef+"|"+sessionId` (`:165`), `"OVERALL|"+crossRefId+"|"+sessionId` (`:171`). **Content-addressed** — this is the join key from `procLink.claudeChunkId` etc.

Fields (built `:394-405`):

```
chunkId        String    non-null (:395)
type           String    PROC_LINK | JAVA_ONLY | ORACLE_ONLY | OVERALL (:396, enum :364)
prompt         String    full Claude prompt text (:397)
response       String    raw Claude stdout (:398)
durationMs     long      (:399)
verdict        JsonNode  String; MissingNode when absent (:400)
confidence     JsonNode  String (:401)
reason         JsonNode  String (:402)
gaps           JsonNode  array of String (:403)
conversionPct  JsonNode  int — only for OVERALL / when the model returned it (:404)
tableAnalysis  JsonNode  free-form, model-supplied, optional (:405)
```

Cardinality: one per procLink + one per javaOnlyProc + one per oracleOnlyProc + exactly one OVERALL, per run.

### 2.3.4 `claude/checkpoint.json`

Writer `addToCheckpoint` `:1150-1160` (write `:1155`); read `:1138-1148`. A **bare JSON array of chunkId strings** (`Set<String>` serialised, `:1154`). Logically append-only, **physically rewritten in full after every completed chunk**. One per run.

### 2.3.5 `report.md`

Non-JSON, written once per run: `CrossRefStore.java:149-155` (write `:151`), from `CrossRefService.java:155` via `CrossRefReportGenerator.generate`.

## 2.4 Flow analysis — `jar/flowanalysis/{jarId}/{runId}/**`

Base `dataRoot.resolve("flowanalysis")` (`FlowRunStore.java:49`); `dataRoot = jar` (`FlowAnalysisController.java:89`). Run dir `:64-66`, `safe()` `:318-321`. `runId` = in-memory counter (`FlowAnalysisController.java:117`) — **not durable across restarts**.

**Size: UNKNOWN — needs runtime check.** No `flowanalysis/` tree in either snapshot.

### 2.4.1 `status.json`

Writer `writeStatus` `:106-113` (write `:109`); map built by `buildStatusMap` `:334-353`. **Rewritten in place on every state change** — 10 call sites: `FlowAnalysisController.java:131,138,164,174,180,187,196,239,255,260`.

| field | type | null? | line |
|---|---|---|---|
| `runId` | String | no | `:336` |
| `status` | String | no | `running`\|`done`\|`error` (`:337`; `RunMeta` `:364,401,407`) |
| `phase` | String | no | `:338` |
| `totalEndpoints` | int | no | `:339` |
| `doneEndpoints` | int | no | AtomicInteger snapshot, `:340` |
| `totalClaudeNodes` | int | no | `:341` |
| `doneClaudeNodes` | int | no | `:342` |
| `currentEndpoint` | String | no (`""`) | `:343` |
| `currentClaudeNode` | String | no (`""`) | `:344` |
| `error` | String | **yes** | `:345` |
| `timestamp` | long | no | run start epoch ms, `:346` |
| `mode` | String | yes | e.g. `optimize`, `:347` |
| `endpoints` | List\<String\> | yes | selected endpoint paths, `:348` |
| `highCount` | int | no | `:349` |
| `medCount` | int | no | `:350` |
| `lowCount` | int | no | `:351` |
| `datasetCount` | int | no | `:352` |

`listRunStatuses` (`:279-307`) injects a synthetic `runId` when absent (`:289`).

### 2.4.2 ⚠️ APPEND-ONLY — `log.jsonl`

`appendLog` `:134-147`. **One JSON-encoded string per line** (`"message"\n`, `:136`) — not an object, so there is no schema to key on. Physically a read-all + rewrite (`:139-143`) because `ArtifactStore` has no append (comment at `:137-138`): **unbounded O(n²) write cost**. `replaceLastLog` `:154-180` rewrites the whole file to substitute the last matching line (`:172`). Read is tail-only: `readLogTail` `:186-208`, `LOG_TAIL_LINES = 30` (`:37`). Callers: `FlowAnalysisController.java:163,170,186,192,256`. Never rotated or truncated.

### 2.4.3 `results.json`

`writeResults` `:212-218` (write `:214`) — writes a **pre-serialised JSON string** produced by the flow analyzer (`FlowResult[]`), so `FlowRunStore` imposes no schema; read back untyped (`:228`). Written once on success (`FlowAnalysisController.java:245`). **Shape: UNKNOWN — needs runtime check** (defined by the analyzer, not by the store).

### 2.4.4 `report.md`

Non-JSON, written once (`FlowAnalysisController.java:249`).

## 2.5 Conversion — `conversion/{jobId}.json`

Key: `JOBS = ArtifactKey.of("conversion")` (`ConversionController.java:62`) → `JOBS.resolve(state.jobId + ".json")` (`:444`). `jobId = String.valueOf(counter.getAndIncrement())` (`:149`), counter restored from disk on boot (`:462-463`).

Writer `persistJob` `:443-449`. **Rewritten in place at every milestone**: `:152, :169, :173, :179, :198, :203`. One per job. Jobs are **never deleted**; the whole directory is reloaded into memory at boot (`:451-478`).

**Size: UNKNOWN — needs runtime check** (`data-backup-.../conversion` is empty). Worst case is bounded below by `liveLines` (2 000 lines) plus an **uncapped** `log[]` plus the full `instructionMd`.

Fields from `JobState.toMap(true)` `:522-539`:

| field | type | null? | meaning |
|---|---|---|---|
| `jobId` | String | no | `:524` |
| `action` | String | no | `convert`\|`validate`\|`fix` (`:525`; endpoints `:142-144`) |
| `procedure` | String | no (`""`) | `:526` |
| `module` | String | no (`""`) | `:527` |
| `projectPath` | String | no (`""`) | local Maven checkout path, **outside the data root** | `:528` |
| `status` | String | no | `running`\|`done`\|`failed`\|`interrupted` (`:529`, `:465-467`) |
| `phase` | String | no | `:530`, mapped at `:407-439` |
| `log` | List\<String\> | no | ⚠️ **append-only, UNCAPPED** (`:531`; `addLog` `:508`) |
| `startedAt` | long | no | epoch ms, `:532` |
| `elapsedMs` | long | no | computed at serialize time, `:533` |
| `liveLines` | List\<String\> | only when `includeLive` | ⚠️ **append-only, ROTATED**: FIFO capped at `MAX_LIVE_LINES = 2000` (`:534`, `appendLiveLine` `:510-515`, cap `:42`) |
| `reportPath` | String | key omitted when null | path of the Claude-written report, **on the local filesystem** (`:535`) |
| `instructionMd` | String | key omitted when null | full generated instruction markdown (`:536`) — immutable, but shipped on every poll |
| `error` | String | key omitted when null | `:537` |

**Fields silently dropped on reload:** `service`, `controller`, `owner`, `jarId`, `batch` of `ConversionRequest` (record `:482-485`) are never persisted; `fromMap` (`:542-561`) reconstructs the request with nulls/false. Consequence: **there is no durable link from a conversion job back to a JAR.**

## 2.6 ⚠️ APPEND-ONLY — Chat: `claude-chatbot/{chatType}/{sessionId}.json`

`CHAT_ROOT = "claude-chatbot"` (`ChatService.java:31`), `chatDir` `:52-54`, `sessionKey` `:56-58` → `claude-chatbot/{classic|new}/{uuid}.json`. Legacy fallback read path `claude-chatbot/{uuid}.json` (`:136`).

Writer `saveSession` `:278-289` (write `:284-285`, pretty-printed). Called on create (`:75`) **and after every message exchange** (`:177`). **Whole-file rewrite each time**, while `messages` is strictly append-only in content (`:165` user, `:175` assistant). The file grows monotonically and is never truncated. Only the *prompt* is windowed to the last 20 messages (`MAX_CONTEXT_MESSAGES` `:26`, applied `:238`) — the stored document is not.

**Size: UNKNOWN — needs runtime check** (`data-backup-.../claude-chatbot` is empty). Unbounded by construction.

`ChatSession` (`ChatSession.java:10-18`, verified by reading the class):

```
id              String   non-null  UUID.randomUUID() (:23); equals the filename stem
name            String   non-null  required by the controller (ChatController.java:78)
scope           String   JAR | PLSQL | GLOBAL (:12); uppercased at ChatController.java:82
chatType        String   classic | new, default "classic" (:26); equals the directory segment
scopeContext    Map<String,String>  NULLABLE (:14)
                  known keys: "analysisName" (PLSQL, ChatService.java:256), "jarName" (JAR, :260)
claudeSessionId String   non-null, a second UUID (:28); written but not used for resume
createdAt       Instant  ISO-8601 string (JavaTimeModule, WRITE_DATES_AS_TIMESTAMPS disabled,
                         ChatService.java:44-46)
updatedAt       Instant  refreshed per message (:176)
messages        List<ChatMessage>  default [] (:18)
  role       String   "user" | "assistant"      [ChatMessage.java:6-8]
  content    String   free text, unbounded
  timestamp  Instant  ISO-8601
```

Cardinality: one file per session; **two array elements per message round-trip**.

References: `scopeContext.jarName` → `jar/{jarId}/…`; `scopeContext.analysisName` → the PL/SQL analysis id and `prompt-analysis/{analysisName}.json`.

**Dead key:** `JarDataPaths.chatHistoryKey` → `jar/{jarId}/chat/history.jsonl` (`JarDataPaths.java:143`) has **no caller anywhere in the source tree**.

## 2.7 Prompts and config

### 2.7.1 `prompts/{name}.txt` (not JSON)

Key via `PromptTemplates.templateKey(name)` (`PromptConfigService.java:185-190`), root `PROMPTS_ROOT = "prompts"` (`:65`). Seeded once at boot if absent (`:87-106`, write `:97-98`); rewritten by `saveTemplate` (`:153-157` → `writeText` `:297-303`) and `resetTemplate` (`:160-164`). `name` restricted to 14 `KNOWN_TEMPLATES` (`:48-58`). Global, one per template name.

**Measured** (`data-backup-.../prompts`, 14 files, 60 KB total): largest `java-both-correction.txt` **5 579 B**, smallest `java-mongo-analysis.txt` **588 B**. A second copy exists under `config/prompts/` (72 KB dir) — note `config/prompts` contains `plsql-postgres-verification.txt` which `prompts/` does not, and `prompts/` contains four `plsql-*-verification.txt` files that `config/prompts/` does not. **The two directories are not in sync.**

### 2.7.2 `prompt-extras.json`

Key `ArtifactKey.of(EXTRAS_FILE)` where `EXTRAS_FILE = "prompt-extras.json"` (`:69`) → **root-level**. Writer `saveExtras` `:218-223` (via `writeJson` `:291-295`). Rewritten in place, full replace. **Exactly one globally.**

```
globalAppend   String  never null (nz, :313) — text appended to every prompt
globalContext  String  never null — context prefixed to every prompt
```

Not present in the snapshot (no writes have occurred).

### 2.7.3 `prompt-analysis/{safeName}.json`

Key `analysisKey` `:265-267` → `ArtifactKey.of("prompt-analysis", safeName(name) + ".json")`; `safeName` `:276-282` (`[^A-Za-z0-9._-]`→`_`, `..` stripped, 200-char cap). Writer `saveAnalysis` `:254-263` (write `:261`). Rewritten in place, full replace. One per analysis id.

```
context    String                never null (:256)
append     String                never null (:257)
templates  Map<String,String>    never null, may be empty (:258-260)
             key   = one of KNOWN_TEMPLATES; unknown keys silently dropped (:259)
             value = full override template text
```

**Measured 58 B** (`prompt-analysis/ERP_USP_RUN_CYCLE__WL2ZTC.json`), real content `{"context":"","append":"","templates":{}}` — i.e. the empty default is persisted.

Precedence at read: `effectiveTemplate` `:167-179` → per-analysis override → `prompts/{name}.txt` → classpath `prompts/{name}.txt` (`:192-200`).

### 2.7.4 `config/**` (read-mostly)

Measured in `data-backup-.../config` (72 KB):

- `domain-config.json` — **7 818 B**. Keys used by the UI (`jar/js/summary.js:69-86`): `base_packages[]`, `collection_pattern` (a regex string), `module_types.{Module}.{prefixes[],description}`, `snapshot_buffer_domain_map`, `view_detection.{contains,starts_with}`, `operation_types`, `jar_name_ignore_segments`, `endpoint_classification.*.keywords`, `size_thresholds`, `performance_thresholds`, `complexity_rules`. Note the UI fetches this as a **static file at a relative URL** (`jar/js/summary.js:62`), and an identical copy is also persisted per-JAR (`jar/web-1.0.0-SNAPSHOT/domain-config.json`, 7 818 B) and served as `/api/jar/jars/{id}/domain-config`.
- `plsql-export-config.json` — **4 620 B**. Full tree verified: `_description`, `summary_metrics.{show_loc_total,show_cross_schema,show_transaction_count,show_batch_count,show_external_deps,show_schema_slices}` (all bool), `object_type_counts.{tables,views,materialized_views,sequences}.{label,tableType}`, `proc_counts.{procedures,functions}.{label,unitTypes[]}`, `complexity_rating.{loc_thresholds.ranges[{label,min_loc,max_loc,score}], ops_thresholds.ranges[{label,min_ops,max_ops,score}], join_score{per_join,max_contribution}, cursor_score{per_cursor,max_contribution}, cross_schema_bonus{bonus}, trigger_bonus{per_trigger,max_contribution}, final_thresholds{high_min,medium_min}, reasons{high,medium,low}}`, `detail_subtable.columns[{key,label,width}]` (14 columns).
- `plsql-config.yaml` — 4 533 B (YAML, not JSON).

### 2.7.5 `verification/*.json` — dialect verification reports

Measured, 24 KB total, 6 files: `_summary.json` (1 161 B) plus one per dialect (`oracle` 2 284 B, `sqlserver` 2 235 B, `db2` 1 976 B, `postgres` 1 917 B, `mysql` 1 916 B). Small, write-once, global (not per analysis).

`_summary.json`: `generatedAt String`, `dialects.{dialect}.{defined,capturedNodes,reached,depth,parseErrors,trueOrphans,potentialMissed:int, potentialMissedList:List<String>}`.

`{dialect}_verification.json`: `dialect`, `generatedAt`, `analysis` (the analysis name), `entryPoint`, `reachability.{definedRoutines,capturedNodes,reachedRoutines,maxDepth,parseErrors,unreachedCount:int, trueOrphans:List<String>, potentialMissed:List}`, `capturedTotals.{totalTables,totalEdges,uniqueEdges,totalCycles,totalLinesOfCode,dbCallCount:int, transactionSummary.{commits,rollbacks,savepoints,autonomousProcs:int}}`, `capturedOperations.{tables,callsOut,cursors,sequences,variables,parameters,exceptionHandlers:int}`, `capturedStatements.{SELECT,UPDATE,LOOP,FETCH,EXIT,CURSOR_ATTRIBUTE,CLOSE_CURSOR,IF,CURSOR_FOR_LOOP,CASE,ASSIGNMENT,EXECUTE_IMMEDIATE,OPEN_CURSOR,MERGE,…:int}` (a sparse, open-ended map — the statement-type set is not a closed enum).

### 2.7.6 `cache/schema-resolver/{key}/*.tsv`

Measured 40 KB, 6 TSV files (**not JSON**): `dependencies.tsv` 12 903 B, `ambiguous.tsv` 9 374 B, `type_qualified.tsv` 3 056 B, `objects.tsv` 2 396 B, `object_types.tsv` 2 271 B, `reverse_deps.tsv` **0 B**. Directory key is `__WL2ZTC` — note the empty leading segments, i.e. the same `~`-splitting convention with empty project/env.

## 2.8 `external-links` — `jar/{jarId}/external-links{,-pending}.json`

Keys from `JarDataPaths.java:144-145`. Both written together by `ExternalApiLinkService.writeResult` `:271-294`. **Rewritten in place (full replace) on every resolve**, triggered by `resolve()` `:73-76`, by `reresolvePending()` `:83-104` **for every other JAR after any analysis finishes**, and from `JarAnalysisExecutor.java:34,59`. One pair per JAR. Not present in the snapshot.

`external-links.json` (write `:281-283`):

```
jarId     String  non-null (:276)
resolved  List<Object>  non-null, may be empty (:277)   [callSite :214-220 + targetOf :259-267]
  callerEndpoint             String  NULLABLE (from nodes.json `endpoint`, :197)
  callerClassName            String  nullable (:216)
  callerMethodName           String  nullable (:217)
  httpMethod                 String  non-null (:218)
  path                       String  non-null, raw httpTargetPath (:219)
  moduleHint                 String  KEY OMITTED ENTIRELY when null (:220)
  targetJarId                String  (:261)
  targetControllerSimpleName String  nullable (:262)
  targetMethodName           String  nullable (:263)
  targetHttpMethod           String  (:264)
  targetPath                 String  (:265)
ambiguous List<Object>  non-null (:278)   same callSite fields, NO target* fields, plus:
  candidates List<Object>  each with the five target* fields (:237)
```

`external-links-pending.json` (write `:288-290`): `jarId String`, `pending List<Object>` — callSite objects only.

When the file is absent the service returns empty defaults (`:342-353`); **note the empty node lacks `jarId`** — a shape inconsistency the UI must tolerate.

## 2.9 Append-only artifacts — the schema's hardest problem

Six artifacts are append-only in content. Every one of them is implemented as a **read-everything-then-rewrite-everything**, because `ArtifactStore` has no append primitive.

| Artifact | Grain | Rewrite cost | Rotation | Cite |
|---|---|---|---|---|
| `plsql/{name}/claude/_call_log.jsonl` | one line per Claude CLI call (incl. retries) | **O(n²) bytes** — full copy per line | none | `ClaudeCallLogger.java:96-104` |
| `jar/flowanalysis/{jarId}/{runId}/log.jsonl` | one line per run event | **O(n²) bytes** | none | `FlowRunStore.java:134-147` |
| `conversion/{jobId}.json` → `log[]` | one element per event | O(n²) — whole job doc rewritten | **none, uncapped** | `ConversionController.java:508,531` |
| `conversion/{jobId}.json` → `liveLines[]` | one element per CLI output line | O(n²) | **FIFO capped at 2 000** | `ConversionController.java:42,510-515` |
| `claude-chatbot/{type}/{id}.json` → `messages[]` | 2 elements per turn | O(n²) — whole transcript rewritten per message | none (prompt windowed to 20, storage not) | `ChatService.java:26,165,175,177,278-289` |
| `.../claude/checkpoint.json` and `.../claude/_checkpoint.json` | one chunkId per completion | O(n²), from concurrent threads | none | `CrossRefClaudeService.java:1150-1160`; `parser/service/ClaudeVerificationService.java:501,1064` |

Plus one artifact that accumulates **as files rather than lines**: `plsql/improvement/run_*.md`, one per run, forever, never pruned (`ClaudeRunReporter.java:34-39`).

**Why this matters for the schema.** These are exactly the shapes that fit neither object storage (no append) nor a single-document model (the document is rewritten in full and grows without bound, and MongoDB has a 16 MB document ceiling). They are also the hot read paths — see P11 (whole call log every 5 s) and P13 (whole job doc every 1.5 s). The schema must model each of these as **one row/document per line**, keyed `(parentId, seq)`, with:

- a monotonic `seq` so the UI can fetch `?sinceSeq=` incrementally (this is what removes P11/P13/P14 from the hot path entirely);
- a bounded/TTL retention policy for `log[]`, `liveLines[]`, `_call_log.jsonl` and `improvement/run_*.md`, none of which have one today;
- a concurrency story for `_checkpoint.json`, which today is a whole-file rewrite from concurrent chunk threads guarded only by a process-local `synchronized`.

Chat history is the one case where the parent document is also read whole (`L30`), so it needs both per-message rows **and** a `?limit=/?before=` read path — the UI currently has neither.

---

# 3. PART B — UI contract, per screen

## 3.0 Polling configuration and the three ways it is broken

`shared/poll-config.js` (25 lines, quoted in full below because everything depends on it):

```js
var PollConfig = {
    sessionPollMs: 30000, claudeProgressMs: 30000, jobStatusMs: 10000,
    logRefreshMs: 10000, correctionMs: 30000, _loaded: false,
    async load() {
        if (this._loaded) return;
        try {
            var res = await fetch('/api/config/polling');
            if (res.ok) { var cfg = await res.json();
                if (cfg.sessionPollMs) this.sessionPollMs = cfg.sessionPollMs;
                if (cfg.claudeProgressMs) this.claudeProgressMs = cfg.claudeProgressMs;
                if (cfg.jobStatusMs) this.jobStatusMs = cfg.jobStatusMs;
                if (cfg.logRefreshMs) this.logRefreshMs = cfg.logRefreshMs;
                if (cfg.correctionMs) this.correctionMs = cfg.correctionMs; }
        } catch (e) { /* use defaults */ }
        this._loaded = true;
    }
};
PollConfig.load();
```

Server side: `com/analyzer/config/PollingConfigController.java`, `@GetMapping("/polling")` under `@RequestMapping("/api/config")`, returning the five ints from `app.poll.{session,claude-progress,job-status,log-refresh,correction}-ms`.

Three defects, all of which mean **server-side poll tuning does not work today**:

1. **`load()` is never awaited** (`poll-config.js:25`). Timers registered during initial script execution capture the hard-coded defaults, not the server values.
2. **Nothing under `parser/**` uses `PollConfig` at all** — every parser interval is hard-coded, and every one is faster than the configured value: 2 s vs 30 s (`claudeInsights.js:164`), 5 s vs 30 s (`claudeSessions.js:35`), 3 s vs 10 s (`logViewer.js:105`), 2 s vs 10 s (`perfSummary.js:416`, `history.js:364,373`).
3. **`parser/index.html:727-728` loads `shared/log-fab.js` without `poll-config.js`** (grep for `poll-config` matches only `jar/index.html:173`, `index.html:998`, `plsql/index.html:709`, `war/index.html:157`). So `log-fab.js:137` throws `ReferenceError: PollConfig is not defined` and the shared log FAB's Auto checkbox is dead on the parser page. `conversion/index.html` loads neither.

Also: `PollConfig.jobStatusMs` is fetched and defined but **has zero readers on the JAR side**.

The largest single response any screen requires, from real data:

| Rank | Response | Measured size | Screen |
|---|---|---|---|
| 1 | `GET /api/jar/logs?lines=10000` / `/api/plsql/logs?lines=10000` | up to **9.36 MB** (`logs/unified-analyzer.2026-07-23.0.log`); the endpoint caps at 10 000 lines, so the practical ceiling is **UNKNOWN but multi-MB** | log viewer (P5, P12) — **polled** |
| 2 | `bundled-jars.json` | **3 028 400 B** | Code Structure tab (L2) |
| 3 | `analysis.json` (whole) | **3 903 765 B**; `jac-demo_targeted` is **5 799 921 B** | not served whole to the browser — see §3.12 |
| 4 | `_summary_static.json` | **429 782 B** | every JAR page load (I2) |
| 5 | `api/tables/index.json` | **307 401 B** | parser topbar (I10) — for one integer |
| 6 | `api/nodes/{node}.json` | **370 379 B** max | table detail fan-out (L22) |
| 7 | `endpoints/*/call-tree.json` | **178 078 B** max, 13 775 B avg | endpoint flows (L3) |
| 8 | `endpoints/*/nodes.json` | **175 196 B** max, 14 372 B avg | node navigator (L5) |
| 9 | `_result.json` | **138 301 B** | five different Claude screens (L26) |
| 10 | `api/index.json` | **87 015 B** | parser initial load (I9) |

## 3.1 JAR — sidebar and boot

**Calls.** `GET /api/jar/jars` on init (`jar/js/api.js:9`, via `app.js:109` in `init()` at `app.js:21`), re-called after upload/delete/Claude-complete (`app.js:490,506,546`). `GET /api/jar/jars/sessions` polled 30 s. `GET /api/config/polling` once. `GET /api/chat/config` once (`shared/chat-toggle.js:19`, duplicated at `jar/js/chat-toggle.js:18`).

**Fields rendered** from `/api/jar/jars` (`sidebar.js:88-110`, `dashboard.js:31-36,238-279`, `app.js:342,716-718,1039`): `id`, `jarName`, `projectName`, `jarSize`, `analyzedAt`, `totalClasses`, `totalEndpoints`, `claudeStatus`, `claudeIteration`, `claudeCompleted`, `claudeTotal`, `analysisMode`, `basePackage`, `hasConnections`. **Nothing else.**

From `/sessions` (`sessions.js:67-101`, `app.js:86`): `id`, `status`, `type`, `jarName`, `detail`, `startedAt`, `completedAt`, `error`.

**Pagination**: none. **Client-side derivation**: duplicate-JAR grouping by stripping a 13-digit timestamp suffix + a two-key sort (`sidebar.js:44-67`), recomputed on every render (`render()` recurses for expand toggles, `sidebar.js:34`).

## 3.2 JAR — Dashboard

**Calls.** Cards render from the already-fetched JAR array — no fetch (`dashboard.js:9-17`). Then a **fan-out**: `setTimeout(...,100)` (`dashboard.js:69`) → `Promise.allSettled(jars.map(getClaudeStats))` (`:80-82`) — **one HTTP request per JAR** to render a one-line strip.

**Fields** (`dashboard.js:92-100,185-204`): `totalCalls`, `sessionCount`, `totalPromptSizeKb`, `totalResponseSizeKb`, `errorCount`, `avgDurationMs`, `sessions[].{sessionId,sessionType,callCount,durationMs,startedAt,endedAt}`.

**Client-side, should be precomputed:** cross-JAR totals (`:29-37`), Claude usage rollup (`:84-105`), and the sessions table is built by **merging every JAR's `sessions[]` into one array and sorting by `startedAt` desc** (`:99,169`). `avgDurMs` at `:105` is an unweighted mean of per-JAR means — statistically wrong; a server aggregate fixes it. **No pagination** on that table (`:183-206`).

## 3.3 JAR — analysis header

**Six requests fire on every JAR click** (`selectJar`, `app.js:697+`): `summary/headers` (`:733`), `versions` (`:1221`), `connections` (`:773`), `domain-config` (`:882`), `claude-progress` (`:1290`), and on any `/headers` error a **silent full `getSummary`** (`:735`). `connections` and `domain-config` are each fetched **a second time** if the user opens the corresponding badge modal (`:798`, `:897`).

**Fields** — from `summary/headers` (`app.js:1027-1105`, `index.html:245-249`): `jarName`, `projectName`, `analyzedAt`, `jarSize`, `totalClasses`, `totalEndpoints`, `analysisMode`, `basePackage`, `correctionAppliedAt`, `correctionCount`, `claudeIteration`, `endpoints[]`, plus `classIndex[]`/`classes[]`, `entityCollMap`, `repoCollMap`, `scheduledCount`, `scheduledJobs[]`, `enableSchedulingClasses[]` consumed by the Summary tab. From `versions` (`:1084-1101`): `hasCorrected`, `hasCorrectedPrev`, `claudeIteration`. From `connections` (`:774-790`): `available`, `mongodb.{uri,database}`, `oracle.jdbcUrl`, `postgres.jdbcUrl`, `extractedAt`. From `claude-progress` (`:542,594-596,1290-1300`): `status`, `completedEndpoints`, `totalEndpoints`, `errors[]`.

**Whole document for a summary:** the `getSummary` fallback is used at `app.js:494,511,558,631,656,735`, `summary-claude.js:559`, `summary-corrections.js:412` — in most cases purely to force a re-render after a Claude scan. `summary-corrections.js:412` proves the payload is not needed: its own catch branch just increments `_dataVersion`.

## 3.4 JAR — Code Structure tab

All lazy, on first tab switch (`app.js:1024`, guarded by `_loaded` at `codeTree.js:43`): `classes/tree` (`:49`), then unconditionally `resources` (`:55,66`), `bundled-jars` (`:56,827`) and `jar-dep-map` (`:57,842`). On click: `decompile?class={fqn}` (`codeTreePanel.js:61`), `resources/{filename}` returning `text()` (`codeTree.js:148`).

**Fields** (`codeTree.js:296-320`, `nav.js:24-33`): `fullyQualifiedName`, `simpleName`, `packageName`, `sourceJar`, `stereotype`, `interfaces[]`, `superClass`, `isInterface`, `isAbstract`, `annotations[]`, `fields[]`, `methods[].{name,returnType,parameters|parameterTypes,annotations[],invocations[]}`. Resources: `filename`, `size`. Decompile: `classData`, `source`.

**Lazy loading present:** package + class headers render first; class bodies render on expand and are memoised via `children.dataset.rendered` (`:320-327`); method invocations render on expand (`:343-353`); `expandAll()` deliberately only expands already-rendered nodes (`:360`). Header comment `:1-7` states this targets 8 000+ classes. **But there is no pagination and no virtual scroller** — the whole tree array still arrives in one response and is held in `_classes` (`:12`), and `bundled-jars.json` alone is **3.03 MB measured**.

**File/dir existence:** `resources`, `bundled-jars`, `jar-dep-map` are directory listings, and all three treat `!res.ok` as "feature absent" (`:67,829,844`).

## 3.5 JAR — Endpoint Flows tab

List renders from `analysis.endpoints` in memory — no fetch (`app.js:1122`). On click: call-tree by index, lazy and cached on `endpoint.callTree` (`endpointList.js:95,101`).

**Fields** — endpoint (`endpointList.js:31,54-64`): `controllerSimpleName`, `controllerClass`, `httpMethod`, `fullPath`, `methodName`, `parameters[].type`, `returnType`. Call-tree node (`callGraph.js:146-472`): `className`, `simpleClassName`, `methodName`, `parameterTypes[]`, `returnType`, `stereotype`, `children[]`, `dispatchType` (`AMBIGUOUS_IMPL|RECURSIVE|QUALIFIED|HEURISTIC|DYNAMIC_DISPATCH|PRIMARY|INTERFACE_FALLBACK|LIST_INJECT|DEFAULT_METHOD`), `recursive`, `resolvedFrom`, `qualifierHint`, `sourceJar`.

**No pagination.** The whole list is one HTML string (`endpointList.js:41-76`); the operations report flattens the entire call tree with no depth cap (`callGraph.js:143-149`).

**Client-side, should be server-side:** `_detectCollections` (`callGraph.js:170-198,200-212`) walks every bean's class data, every annotation, every method annotation and every attribute value, regex-scanning for `SB_*` / `*_VI_*` / `*_V_*`. That is analysis logic in the browser, and the server already computes the equivalent for the Summary tab.

## 3.6 JAR — Summary tab

**Shell.** One fetch: `domain-config.json` as a **static relative-URL file**, cached in `_config` (`summary.js:60,62`).

**The heaviest client-side work in the app.** `_render` (`summary.js:101-213`) runs `_analyzeEndpoint()` over every endpoint in `requestAnimationFrame` batches of 30 with a live progress bar (`:182,187-212`) — an explicit admission the work is too slow for the main thread. Before that it builds `classIdx` (`:111-117`), and — **only if the server did not send them** — `entityCollMap` by scanning every `@Document` annotation (`:120-131`) and `repoCollMap` by an O(repos × entities) name-stem join plus regex over method annotations (`:134-169`). `analysis.entityCollMap` / `repoCollMap` / `scheduledCount` are the existing server-authoritative escape hatches (`:120,134,258`); the fallbacks should be deleted. `_finishRender` (`:234-457`) then computes ten headline aggregates (`:236-255,286`) and **recomputes all ten on every filter change** (`:403-420`).

**Sub-tabs** render on first `switchSubTab`, cached via `tab.dataset.rendered` + `renderedVersion` (`:538-576`). Slices per `_SLICE_DEPS` (`:460-466`): `external-calls` (`:478`), `dynamic-flows` (`:480`), `aggregation-flows` (`:482`), `beans` (`:484`, no tab wired). Slices are merged back **by array index** (`:497-514`) and **dropped when the user leaves the Summary tab** (`:35-55` via `app.js:1017`), so they are refetched on every round-trip.

**Pagination — present, but entirely client-side.** `_initPage(id, data, pageSize, ...)` (`summary-helpers.js:478-500`), default page size **25**, selector `[10,25,50,100]` (`:570`), slicing `s.filtered.slice(start, start + s.pageSize)` (`:532`). **No `offset`/`limit` query param exists anywhere in the codebase.** Twelve paginated tables at size 25: `summary-tables.js:40,610,633,1060,1145,1224`, `summary-aggregation.js:112`, `summary-dynamic.js:106`, `summary-claude.js:44`, `summary-vertical.js:131,257`. Search is client-side too — `_buildSearchText` flattens every scalar of every row into a lowercase string at init (`summary-helpers.js:503-524`). **Not paginated:** corrections cards (`summary-corrections.js:107-113`), dashboard sessions, endpoint list, code tree.

**Whole-document-for-a-summary, worst case:** `summary-popups.js:382-389` fetches call trees for **all relevant endpoints in parallel** (`Promise.all`) just to list which call paths touch one collection. With a measured max call-tree of 178 KB, this is an unbounded multi-MB fan-out to render a popup table.

**Good lazy pattern to preserve:** `summary-trace.js:150` fetches the pre-flattened `nodes` list (falling back to a client `_flattenTree` at `:156-161`), and per-node source is fetched only when `node.sourceCode` is missing (`:306`).

**Catalog join in the browser:** `summary-tables.js:691,696,706-724` fetches `/catalog` when any collection has `verification == null`, builds a lowercase `Set`, and joins it against every row to set `VERIFIED`/`NOT_IN_DB`.

**Export round-trip:** `summary-export-excel.js:56-72` **POSTs the entire client-computed model back to the server** (`epReports`, `vertReport`, `distReport`, `batchReport`, `viewsReport`, `extReport`, `vertVerReport`, each deep-cloned) to `POST /api/jar/jars/export-excel`. This exists only because the aggregations live client-side.

## 3.7 JAR — Claude Insights and Corrections sub-tabs

**Insights** (`summary-claude.js`): `claude-progress` on open (`:51,211`) then polled 30 s (`:532`); `claude-fragments` **directory listing** from four sites (`:239,385,453,798`); `claude-fragments/_index.json` (`:811`) and `claude-fragments/{name}` (`:395,851`); `run-logs` listing (`:598`) and file (`:633`); `claude-stats/{sessionId}` lazy on row expand, memoised via `dataset.loaded` (`:748,763`).

**Severe file-existence dependence.** "Does Claude data exist?" is `fetch(claude-fragments)` → count array length → `> 0` means ENRICHED (`:239-247`); `!resp.ok` means "static analysis only" (`:253`). Endpoint↔log correlation is **string matching on filenames**: `f.name.includes(epSafe)` (`:387-388`), `traceId + '_'` prefix matching (`:825-845`), and `name.includes('input')/('output')/('error')` to classify a file's role (`:857-859`); files beginning with `_` are skipped as metadata (`:830`). This is on-disk layout leaking into UI logic — a `GET .../claude-logs?endpoint=X` returning typed records removes all of it.

**Corrections** (`summary-corrections.js`): `corrections` on open (`:44,58`) and polled 30 s (`:394,403`), with a **second** request per tick to `/sessions` (`:404`) only to test `some(status === 'RUNNING')`. `!res.ok` → "no correction data" (`:59,73`) — 404 as state again. Fields: `totalCorrections`, `corrections` (a **map keyed by endpoint name**), `corrections[].collections.{added[],removed[],verified[]}` (`:70-88`). `totalAdded/totalRemoved/totalVerified` are summed by iterating the whole nested map **on every poll tick** (`:80-88`) to render three numbers.

`getCorrectionForEndpoint` (`api.js:225`) is defined and never called. Other dead API methods: `getAnalysis` (`:29`), `claudeRescan` (`:157`), `getClaudeStatus` (`:128`).

## 3.8 JAR — Flow Analysis and Cross-Reference

**Flow Analysis** (`flow-analysis.js`), all lazy from `summary.js:451-455` → `_init()` (`:174`): `flow/env-profiles` (`:188`), `jars/claude-status` (`:500` — note a **different path** from `api.js:130`'s `/jars/settings/claude-status`), `flow/history` (`:377`), `POST flow/test-connection` (`:581`, reads `success,latencyMs,database,collectionCount,sampleCollections[],message`), `POST flow/run` (`:628`, reads `runId,error`), `flow/status/{runId}` **polled 1 500 ms via a chained `setTimeout`** while `status === 'running'` (`:646,651`, reads `status,phase,log[],mode,error`), `flow/report/{runId}` as `text()` → `.md` blob (`:976`). History is filtered by `e.mode` client-side (`:392`) — a `?mode=` param would do.

**Cross-Reference** (`cross-ref.js`), rendered on first tab switch and on JAR change (`app.js:1035`, `cross-ref.js:43`):

- `flow/history` (`:108`) fetched **only to build a Set of already-analyzed endpoint paths** for green tagging (`:111-113`) — a whole run-history document for a membership test.
- `cross-ref/available-procs` lazy with a `_procsLoaded` guard (`:354`); reads `procName`, `schema`, `analyzed`, `sessionId`; **client-side count of `p.analyzed`** (`:361`) and client-side substring filter over the full list (`:375-379`).
- `POST cross-ref/link` `{endpointPath, procName, schema, endpointIndex}` (`:505`).
- `cross-ref/history` full list, filtered client-side (`:571,591-600`), and it **auto-opens the first result** (`:580`) — so a list fetch immediately triggers a detail fetch.
- `cross-ref/result/{id}` (`:675`) — reads `crossRefId`, `endpointPath`, `procName`, `mode`, `summary{}`, `procLinks[]`, routing/infra nodes (`:2102,2117`). `_reloadResultAndRefresh` (`:1068-1069`) **refetches the entire result document** just to re-render one sub-panel after every Claude completion.
- Claude: `POST .../claude/run` with `Accept: text/event-stream`, consumed via `fetch` + `ReadableStream` with manual `data:` line parsing (`:1000-1030`; note the dead `new EventSource(url)` immediately `.close()`d at `:995-997`). Progress fields `status`, `completed`, `total`. `DELETE .../claude/cancel` (`:1039`), `POST .../claude/merge` (`:1047`), `GET .../claude/chunk/{chunkId}` (`:1156,2075`) reading `verdict, confidence, durationMs, reason, gaps[], prompt, response`.
- Source viewers: `source/java?nodeId=` (`:1301,1691,1921`), `source/oracle-tree` (`:1401`), `source/oracle?procRef=` (`:1571,1931`).

## 3.9 PL/SQL explorer (`parser/**`)

Tabs declared at `parser/index.html:243-251`: `explore`, `tableOps`, `details`, `summary`, `complexity`, `callGraphViz`, `source`. (`statements` and `exHandlers` are commented out at `:246-247` but `statements.js` and `exHandlers.js` are still shipped — dead payload.) All endpoints funnel through `parser/js/api.js` (277 lines).

**Home** — `plsql/config/projects` (`home.js:23`), `parser/claude/stats` (`:213`, **re-fetched on every `goHome()`**, `router.js:45`), `parser/analyses` (`:18`). Reads only six numbers from the stats document: `totalCalls`, `sessions.total`, `totalPromptBytes`, `totalResponseBytes`, `totalDurationMs`, `errors` (`:217-238`) — and `:218` bails (`if (calls === 0) return;`) **after** parsing the whole document.

**History** — `parser/analyses` on init/tab switch/after each analysis; `DELETE /api/parser/analyses/{name}` (`history.js:230`). Fields (`:95-105`): `entryPoint`, `name`, `entrySchema`, `timestamp`, `totalNodes`, `totalTables`, `totalLinesOfCode`, `errors[].length`, `totalDurationMs`/`crawlTimeMs`, `project`, `environment`, `connectionName`, `jdbcUrl`, `dbType`. **No pagination** (`:92-107`); filtering client-side (`:164-185`). `dbType` is **inferred by substring-scanning `jdbcUrl`** in two duplicated places (`history.js:101-109` and `perfSummary.js:13-22`) — should be a persisted field. `errors[]` ships in full but only `.length` is used.

**Queue tab** — `GET /api/queue` polled **2 000 ms** (`perfSummary.js:416`, self-cancels when hidden `:417-421`); the **whole global queue** is fetched then filtered client-side to PL/SQL types via a hard-coded `Set` (`:430-446`). No pagination.

**Performance Summary** — no fetch of its own, reuses `PA._allHistory` (`:52`), client page size 10 (`:6`). Sums `totalTime/totalDb/totalSuccess/totalErrors/totalProcs/totalTables/totalLoc` over the entire history **on every filter keystroke** (`:77-95`). Its detail modal calls `GET /api/analyses/{name}/claude/result` (`:366`) — **no controller maps `/api/analyses`** (only `/api/analysis`, `/api/parser`, `/api/plsql`); the correct path is `/api/parser/analyses/{name}/claude/result` (`api.js:185`). **This call always 404s**, so the Claude Verification grid permanently shows "No verification data" (`:371`). Intended fields: `confirmedCount, removedCount, newCount, totalChunks, errorChunks, claudeCallCount, claudeTimeMs` — 7 scalars from a **138 KB** document.

**Loading an analysis** (`app.js:18-73`) — `analyses/{name}/index`, then immediately `analyses/{name}/tables` via `_ensureTablesIndex()` (`:74`) **solely to correct one topbar integer**: `app.js:78-81` sets `analysisData.totalTables = d.tables.length`. Measured cost: **307 401 B for one number.** Topbar fields (`:89-101`): `entryPoint, name, totalNodes, totalTables, uniqueEdges|totalEdges, totalLinesOfCode, maxDepth, errors[].length`. `api.js:59-79` re-shapes the same tables document and computes `accessCount` by a nested `reduce` over `usedBy[].operations{op:[lines]}`; `api.js:88` filters `dbValidated !== true || dbFound !== false` client-side.

**File-path coupling:** node identity is recovered by **regex-parsing file paths** — `utils.js:83` `detailFile.match(/nodes\/(.+)\.json$/)` builds the underscore→dollar nodeId map; node details are addressed as `nodeId + '.json'` (`tableDetail.js:294,439`; `sourceView.js:283,1498`; `triggerDetail.js:150` strips `^nodes/`).

**Table Ops** — `analyses/{name}/tables` again (`tableOps.js:8`), and a fourth time from `leftPanel.js:178`. `PA.tf.init('to', data, 50, …)` (`:31`) — client-side, page size 50, all rows in memory (`tableFramework.js:33-56`).

**Table Detail modal** (1 580 lines, the worst offender): `getTableOperations()` (`:127`), `getTablesIndex()` (`:143,828`), then **two N-node `Promise.all` fan-outs** — `getNodeDetail(jn.nodeId + '.json')` for every node in scope at `:294-296` (join edges) and again at `:439-441` (column refs), pulling documents of up to **370 KB each** for a handful of fields (`name, nodeId, sourceFile, tables[], variables[], parameters[]`). `claudeResult(name)` attached post-render and cached (`:330-336`), with the whole 138 KB document scanned client-side to find one table (`:314-317`). `GET /api/plsql/db/table-info/{table}?schema=` is a **fallback only** — `_ensureMetadata` first tries `dbColumns`/`dbConstraints`/`dbIndexes`/`viewDefinition` off the tables JSON (`:826-854`). Pagination: `PA.pager.slice('td-claude-matched', …)` (`:1358`), `'td-claude-new'` (`:1445`), plus six `PA.tf.init(..., 50, ...)` sub-tables (`:719,907,971,1033,1120,1192`).

**Details sub-tabs** (lazy, `app.js:723-735`): sequences (`sequences.js:8`), joins + join-complexity config (`joinSummary.js:14,26,40`), dynamic-sql (`dynamicSql.js:16`), transactions (no fetch, `summary.js:24`), txn-trace (`txnTrace.js:49` — a fifth fetch of the tables document). `_detailsSubLoaded` guards re-fetch (`app.js:642,723-735`), **but** `switchRightTab` discards heavy tabs' DOM and `PA.tf` state on tab-away (`:773-779`), so leaving and returning refetches everything. `txnTrace.js:49-76` rebuilds `_writesByProc`/`_writesByNode` by triple-nested iteration over `tables[].usedBy[].operations{op:[lines]}`, filtering to DML and sorting each bucket — a "DML writes by procedure" projection endpoint eliminates it.

**Summary → Dashboard** (`parser/js/summary.js`) — **zero fetches**; `_computeStats` (`:28+`) does a full O(nodes) pass on every scope change (`:8-10`), accumulating `totalLoc/totalTables/totalCalls/totalCursors/totalParams/totalVars/totalExHandlers/totalStmts` and building `schemaMap`/`typeMap`/`stmtTypeMap`. Fields read per node: `counts.{tables,internalCalls,externalCalls,cursors,parameters,variables,exceptionHandlers,statements}`, `linesOfCode`, `schema`, `objectType`, `depth`.

**Claude Insights** (`claudeInsights.js`) — `claude/progress` on open (300 ms deferred, `:16,62`) and **polled 2 000 ms hard-coded** (`:164`), started `:139-142`, stopping on `progress.isComplete && !progress.hasRunningSession` (`:157-162`) or analysis-name change (`:143-148`). **15× the configured 30 s.** Progress fields (`:103-107`): `percent, completedChunks, totalChunks, failedChunks, status, hasRunningSession, isComplete`. `claude/result` lazy, only if no running session (`:73`); reads `confirmedCount, removedCount, newCount, tables[], claudeTimeMs, claudeCallCount, errorChunks` and per table `tableName, overallStatus, claudeVerifications[].{status,operation,procedureName,lineNumber,reason}` (`:173-190`). `_checkStatus` (`:77-82`) downloads the whole 138 KB document to print one line. `_renderTableCard` (`:213-219`) re-counts CONFIRMED/REMOVED/NEW per table although the top-level counts exist. **No pagination** (`:194-197`).

**Claude Enrichment / Overview** (`claudeEnrichment.js`, 685 lines) — `parser/claude/status` (`:30`), `POST claude/verify` (`:148`), **SSE** `EventSource /api/parser/analyses/{name}/claude/progress/stream` with `progress`/`complete`/`error` events (`:98-119`), `POST claude/sessions/{sid}/kill` (`:166`), `claude/result` (`:252,445,530`), `claude/versions` (`:463`), version load/revert POSTs (`:77,84,93`), `POST claude/apply` (`:490,671`). `_restoreMode` (`:438`) is a **500 ms × 40 busy-wait** (20 s) polling for an analysis to appear (`:184`). `renderResult` (`:288-321`) walks every `tables[].claudeVerifications[]` to compute confirmed/removed/new plus a per-operation 3-way matrix `byOp{SELECT,INSERT,UPDATE,DELETE,MERGE}` — **and then only shows a toast** (`:321`); the whole aggregation is discarded. The Overview modal (`:487+`) reads `timestamp, claudeTimeMs, errorChunks, error, claudeCallCount, totalChunks, confirmedCount, removedCount, newCount, tables[]` and then **re-derives per table** `tConf/tRem/tNew/tAccepted/tRejected/tPending` from `claudeVerifications[].status` and `.userDecision` (`:518-540`); paginated at 50 (`:558`), state deleted on close (`:470`).

**Claude Sessions modal** (`claudeSessions.js`) — **polled 5 000 ms hard-coded** (`:35`), **two parallel requests per tick**: `/api/parser/claude/sessions` and `/api/parser/claude/stats` (`:52-53`). Fields: from `/sessions` → `sessions[]` (or a bare array), `summary.{running,complete,failed,killed}`, per session `id, analysisName, type, status, detail, durationFormatted` (`:160-174`); from `/stats` → `log.{totalCalls,sessionCount,totalPromptBytes,totalResponseBytes,avgDurationMs,totalDurationMs,errors,sessions[]}` with top-level fallbacks (`:86-97`), per log session `sessionId, sessionType, callCount, promptBytes, responseBytes, durationMs, errorCount` (`:114-120`). **No pagination on either table** (`:111,160`) — so **the entire append-only Claude call log is re-fetched and re-rendered every 5 seconds to show seven totals.**

**Chunk Viewer** (`chunkViewer.js`) — `claude/chunks/summary` on open (`:45`); `claude/chunks/{chunkId}` **lazy on row click, memoised** in `_detailCache` (`:140-151`) — the best pattern in the app. Summary fields: `chunks[], completeCount, errorCount, pendingCount, total`, per chunk `chunkId|id, name, status, tableCount, nodeIds[]` (`:57-60,96-109`). Detail: `output` (string or object; **markdown fence stripped and `JSON.parse`d client-side** at `:199-206` — the artifact-level lie from §2.2.7 surfacing in the UI), `input.{prompt,timestamp,promptLength,tableCount,nodeIds}`, `error` (`:263-283`). `_renderStats` (`:62-74`) re-walks the whole `_result.tables[].claudeVerifications[]` for three badges that already exist as top-level counts; same again per chunk (`:226-233`). **No pagination** (`:95-111`); the filter is a DOM `style.display` walk (`:116-124`). The prompt tab renders the **entire prompt** (measured up to 28 079 chars) into `<pre>` blocks after a regex split (`:289-310`), with no truncation.

**Claude Corrections** (`claudeCorrections.js`) — `claude/result` (`:70`), `POST claude/review` per item (`:316`) and bulk `{bulk:'ACCEPTED'|'REJECTED'}` (`:357,370`), `POST claude/apply` (`:384`). **No polling** (contrast the JAR side).

**Call Graph viz** (`callGraphViz.js`, 947 lines) — `analyses/{name}/call-graph` lazy on tab switch (`app.js:822-824`, `:26`); `node/{nodeFile}` on node click (`:891`). Reads `data.edges[].{fromNodeId|from, toNodeId|to, fromSchema, toSchema}` (`:62-78`) and joins node attributes from the in-memory `index.json` nodes (`nodeId, name, schema, objectType, depth, linesOfCode`, `:48-56`). **The graph is joined in the browser** — `_buildGraph` (`:41-80`) merges the edge list against the node list and synthesises placeholders for edge endpoints missing from the index (`:64-79`), then counts `inEdges`/`outEdges` per node. No node cap, no viewport fetching; empty state decided by `!data.edges.length` (`:27`). Measured payload 84 365 B / 218 edges.

**Complexity tab** (`complexity.js`) — `parser/config/complexity` once, cached (`:97`). `_computeMetrics` (`:117+`) **recomputes every procedure's complexity score and risk band in JS** from `counts.{statements,tables,callsOut,cursors,exceptionHandlers}` and `linesOfCode`, applying weights `loc/tables/callsOut/cursors/dynamicSql/depth` and thresholds `medium/high` (`:118-130`), deriving `dynSql = EXECUTE_IMMEDIATE + DBMS_SQL` (`:135-136`). Config merged from `localStorage` per analysis (`:26,81-83`). Defensible for live weight tuning; the **default-weight pass** should still be precomputed. `PA.tf.init('cx', data, 50, …)` (`:263`).

**Source view** (`sourceView.js`, 1 779 lines) — `analyses/{name}/source/{sourceFile}` returns **raw text**, wrapped as `{content, owner:'', objectName}` (`api.js:154-160`); called at `:672,694,809,1706,1718`. `getNodeDetail` at `:283,1498`; `getCallTree` at `:376,1572`; `getCallerTree` at `:400,1596`. **`triggerDetail.js:156-165` fetches an entire source file and then slices `lines.slice(lineStart-1, lineEnd)` in the browser** to show one trigger body — a `?fromLine=&toLine=` parameter would avoid the transfer. `sourceView.js:694` probes for a companion `.pkb` by name convention, and `triggerDetail.js:173` falls back to `getSource(trigger.nodeId)` treating a nodeId as a filename: a 404-probe chain at `:150 → :156 → :173`.

**Log viewer** — `GET /api/plsql/logs?lines=10000` (`logViewer.js:62`), **polled 3 000 ms hard-coded** (`:105`). Fields `content, lineCount, fileSize, truncated` (`:65-71`). Every 3 s the client downloads up to 10 000 log lines and **filters them in the browser** (`:91-97` — `split('\n')` + `Array.filter` on level and substring). The parser page runs this **and** the shared FAB at a nominal 10 s, i.e. two log pollers disagreeing by 3.3× (except the FAB's is dead, §3.0).

**Config / connections** (`config.js`, base `/api/plsql/config` at `:9`) — `projects` on modal open (`:25`), `projects/{name}/environments` lazy on click (`:61`), `POST projects` (`:250`), `DELETE projects/{name}` (`:262`), `POST .../environments` (`:282`), `DELETE .../environments/{n}` (`:296`), `DELETE` connection (`:186`), `POST {apiBase}/test-connection` (`conn-test.js:66`), save (`:108`). Fields `name, description, dbType, environments[], connections[]` (`:41-51,70-80`). Env count is `(p.environments || []).length` (`:48`) — the nested array must ship for a count, and `selectProject` re-fetches environments anyway (`:61-64`). No polling, no pagination.

**Cache Manager** (`cacheManager.js`) — `parser/cache/by-connection` on open (`:41`), fields `dbType, project, environment, connectionName, analysisCount, sizeBytes` (`:105-112`); `POST parser/cache/clear-connection?project=&environment=&connName=` (`:124`). `_clearAll` (`:133-151`) issues **one POST per connection sequentially in a `for` loop** (`:142-148`) and sums `evicted` client-side — a bulk endpoint is warranted. No pagination; facets computed client-side (`:67-82`).

**Export** (`export.js`) — on click, serially fetches joins, tables, cursors, sequences and the whole call graph (`:145,148,152,156,160`), each in a `try/catch`, then filters by scope in JS. **Five full-document downloads for one XLSX.**

**Sync screen** (`sync.js:72-86`) — `GET /api/jar/jars` and `GET /api/parser/analyses` in parallel, then the JAR↔parser correlation is **joined client-side**.

**Prompt Manager** (`promptManager.js`) — base `/api/prompt-config` (`:18`), generic `_get`/`_post` (`:33,39`). Its header comment (`:6-15`) documents `/templates`, `/templates/{name}`, `/templates/{name}/reset`, `/extras`, `/analysis/{name}` and explicitly states **"Everything degrades gracefully if an endpoint 404s"** — feature detection by HTTP status.

## 3.10 PL/SQL legacy UI (`plsql/**`)

`plsql/index.html:709` loads `poll-config.js` **before** all app scripts — the only page that gets the ordering right. `plsql/js/api.js` (578 lines) has a **dual-backend switch**: `_parserName()` (`:100-102`) returns the analysis name only when `PA.analysisData._isParser`, and every method branches between `/api/parser/analyses/{name}/…` and the legacy singleton `/api/plsql/analysis/…` — call-graph `:111`/`:115`, node detail `:123`/`:127`, call-tree `:135,147`/`:139,151`, tables `:160`/`:164`, sequences `:179`/`:183`, joins `:192`/`:196`, cursors `:205`/`:209`, procedures `:225`/`:229`. **Two URL spaces for one entity** — the schema must serve both or the switch must be removed first.

Initial load (`app.js:3898` DOMContentLoaded): `listProjects()` + `getDbUsers()` in parallel (`:178-179`), `getClaudeStatus()` (`:284`), then `listHistory()` + `listClaudeSessions()` in parallel (`:320-321`). On analysis load (`:1451-1505`): `getCallTree(pkgId)` → `getProcedures()` → `getTableOperations()`, three **sequential** full-document fetches; `getProcedures()` is invoked on up to three branches (`:1454,1458,1498,1502`) and memoised only later as `PA._cachedProcedures` (`:2870`).

Polling here is the **good example** — everything is PollConfig-driven: Claude progress at `claudeProgressMs` (`app.js:815`, `claude.js:383`, `summary-claude.js:353`, `summary-progress.js:235`, the last skipping ticks when the tab is hidden at `:233-234`), job status at `jobStatusMs` (`app.js:1073,1359`), log refresh at `logRefreshMs` (`logViewer.js:111`). SSE: `/api/plsql/claude/progress/stream` with a 3-error auto-close (`api.js:355-363`) and `/api/queue/events` (`claude.js:302`) — **but `claude.js:307` re-fetches the whole `/api/queue` on every event whose payload lacks `state`**, which amplifies a burst of job events into a burst of full-queue fetches.

Progress fields (`claude.js:344-359`): `percentComplete, completedChunks, totalChunks, errorChunks, running, analysisName`. Queue panel (`claude.js:258-286`): `current, queued[], history[]`, per job `type, status, displayName, currentStep, progressPercent, id, resultName` — and `:259-261` filters to `PLSQL_CLAUDE_VERIFY`/`PARSER_ANALYSIS` and slices `history` to **3** client-side, i.e. the whole queue is transferred to show at most three rows.

Metadata path mirrors parser: cached `/api/plsql/analysis/tables/{t}/metadata` (`api.js:393`) and `/triggers` (`:399`) first, live `/api/plsql/db/table-info/{t}?schema=` (`:408`) as the slow fallback — the comments at `:390,406` say exactly this. `api.js:46,52` use `cache:'no-store'` and return `{available:false}` on any non-OK — availability inferred from an HTTP failure rather than a status field.

## 3.11 Conversion screen (`conversion/index.html`, 677 lines, single-file `const CV`)

| Endpoint | Mode | Cite |
|---|---|---|
| `GET /api/conversion/claude-status` | initial (`init()` `:335`) | `:344` |
| `GET /api/conversion/jobs` | initial | `:352` |
| `POST /api/conversion/{convert\|validate\|fix}` body `{procedure, module, service, controller, owner, projectPath, batch}` | user | `:379-383` |
| `GET /api/conversion/jobs/{jobId}` | **POLLED 1 500 ms** | `:398,404` |
| `GET /api/conversion/jobs/{jobId}/report` | lazy, on Report tab (`tab()` `:426`) or job re-open | `:567` |

**This page does not load `poll-config.js` at all** — the fastest poll in the suite (1.5 s) is the only one with no server override. A second timer runs the elapsed counter at 1 000 ms (`:445-451`), self-cancelling on non-running (`:449`). The job poll starts only for `status === 'running'` (`:437`) and clears when status leaves `running` (`:409`).

Fields rendered (`_renderDetail` `:534-590`, `_renderList` `:456-479`): `jobId, action, procedure, module, status, phase, startedAt, reportPath, error, log[], liveLines[], instructionMd`. Report: `found, error, path, content` (`:568-577`).

**The biggest whole-document-on-a-timer in the app.** `_pollOnce` re-fetches the **entire job object every 1.5 s** — including the full `log[]`, the up-to-2 000-element `liveLines[]`, and the immutable `instructionMd` (`:473`) — and `_renderDetail` re-renders all of it from scratch (`:516-517` `logEl.innerHTML = (j.log||[]).map(...)`, `:525-534` the same for `liveLines`), re-classifying every line with five regexes per line (`:526-532`). For a long Claude run this is O(n²) traffic **and** O(n) DOM rebuild per tick. A `?sinceLine=` cursor plus append fixes both. Client-side: jobs sorted by `startedAt` on every tick (`:458`), `d-live-count` = `lines.length` (`:527`), `_elapsed(j)` from `startedAt` (`:472,543`).

**File-existence dependence:** the report response carries `found` and `path`, and the UI renders "❌ File not found" with the raw path (`:571-576`); `reportPath` presence on the job is the **sole gate** for enabling the Report tab (`:433,459,551`). Markdown is rendered by hand-rolled regex (`_renderMd` `:596+`). **No pagination** anywhere — full job list, full log, full live output. `sessionStorage.cv_sel` persists the selected job across reloads (`:338,432`).

## 3.12 Chat (shared, all pages)

`shared/chat-api.js` (67 lines) — thin REST, **no polling**: `POST /api/chat/sessions` `{name, scope, scopeContext, chatType}` default `'classic'` (`:6-11`); `GET /api/chat/sessions?scope=&chatType=`, params omitted when falsy (`:14-21`); `GET /api/chat/sessions/{id}` (`:25`); `DELETE` (`:31`); `POST /api/chat/sessions/{id}/messages` `{message, pageContext?}` (`:38-42`); `POST /api/chat/files/read` `{sessionId, path}` (`:47-51`); `POST /api/chat/files/write` `{sessionId, path, content}` (`:56-60`); `GET /api/chat/sessions/{id}/report` as an href (`:65`).

`listSessions` returns `[]` on non-OK and `getSession` returns `null` (`:20,26`) — **failure and emptiness are indistinguishable**.

`shared/chat.js` (772 lines, `ChatPanel`) — `listSessions(scope,'classic')` on open (`:170`), `getSession(id)` (`:203`), `createSession` (`:193`), `deleteSession` (`:212`), `sendMessage` (`:296`, auto-greeting `:678`), `readFile` (`:410`), `writeFile` (`:469`). **No polling, no SSE, no streaming** — `sendMessage` is a single blocking POST with a "Claude is thinking" placeholder (`:290`) and the whole reply arrives at once (`res.content`, `:303`); long turns show no incremental output. Session list fields (`:177-183`): `id, name, messageCount, updatedAt|createdAt`. Message fields (`:729-733`): `role, content, timestamp`. `getSession(id)` returns the **entire transcript** and `renderChat` re-renders all of it (`:220-257`) — **no windowing, no `?limit=`/`?before=`, no pagination anywhere in chat.** `_buildPageContext()` (`:708-724`) assembles URL, document.title, the caller's context entries and the hash route into a newline string sent with **every** message.

`shared/chatbox.js` (414 lines, `chatType:'new'`) — `listSessions(_scope,'new')` (`:76`) then `getSession(_sessionId)` (`:79`), **two round-trips** where the list could carry the last session inline. `createSession` (`:87`), `sendMessage` (`:216`, greeting `:284`), `deleteSession` (`:233`). `_buildPageContext()` is duplicated byte-for-byte at `:158-174`.

`shared/chat-toggle.js:19` — `GET /api/chat/config` decides which chat to show, but **both** `chat.js` and `chatbox.js` plus both CSS files are unconditionally downloaded on parser (`parser/index.html:806-819`) and plsql (`plsql/index.html:748-757`) regardless of the answer — ~1 200 lines of dead JS per page load.

Server side, `POST /api/chat/files/read` and `/write` (`ChatController.java:179-180,219-220`) are **arbitrary read/write under the data root, unauthenticated**, with a 512 KB read cap (`:150`). That is a security finding, not a schema one, but any storage redesign inherits it.

---

# 4. Specific answers to the three "also report" questions

## 4.1 Any endpoint that streams a whole multi-GB document to the browser?

**No — but two get close, and one is polled.**

- Nothing streams a multi-GB document. There is **no** `StreamingResponseBody`, `InputStreamResource` or `FileSystemResource` anywhere in `unified-web/src/main/java` (grep returns only comments in `ConfigDirService.java:152,240,242` and `AnalyzerController.java:857`). `stored.jar` (**70 489 772 B measured**) is never served.
- The largest thing the browser actually pulls, and the only large one on a timer, is the **log endpoint at `?lines=10000`** (`jar/js/logviewer.js:70`, `parser/js/logViewer.js:62`, `shared/log-fab.js:98`). The source files are measured at **9 362 680 B** and **4 088 564 B**; the 10 000-line cap means the served slice is smaller, but it is still multi-MB, it is re-fetched every 3–10 s, and it is filtered client-side.
- Second largest: **`bundled-jars.json` at 3 028 400 B**, fetched unconditionally on the Code Structure tab (`jar/js/codeTree.js:56,827`).
- `analysis.json` (**3 903 765 B**, and **5 799 921 B** for `jac-demo_targeted`) is deliberately *not* served whole — `LazyAnalysisResult` exists precisely to avoid "loading the entire 27-45MB analysis.json at once" (`plsqlanalyzer/web/service/LazyAnalysisResult.java:16-20`), and `JA.api.getAnalysis` (`jar/js/api.js:29`) has **zero callers**. That mitigation must survive the schema change.
- `flow.json` (**3 298 349 B** max measured) is a parse intermediate; no UI route fetches it.

## 4.2 Places the UI depends on file/directory existence rather than data

These all become awkward queries — each one is a `LIST` or a 404-probe standing in for a boolean column.

| # | What is probed | Consequence in the UI | Cite |
|---|---|---|---|
| 1 | `GET .../claude-fragments` array length > 0 | the **entire** "is this JAR Claude-enriched?" decision | `jar/js/summary-claude.js:239-247`; `!ok` → static-only `:253` |
| 2 | Fragment **filenames** | endpoint↔log correlation by `f.name.includes(epSafe)`, `traceId+'_'` prefixes, and `includes('input'/'output'/'error')` to type a file; `_`-prefix means metadata | `summary-claude.js:387-388,825-845,857-859,830` |
| 3 | `GET .../corrections` returns 404 | "no correction data" state | `jar/js/summary-corrections.js:59,73` |
| 4 | `/resources`, `/bundled-jars`, `/jar-dep-map` non-2xx | feature silently absent | `jar/js/codeTree.js:67,829,844` |
| 5 | `/api/plsql/logs` then `/api/jar/logs` probed in order | **which application am I** is decided by HTTP status | `shared/log-fab.js:12,96-111` |
| 6 | `reportPath` present on a conversion job | Report tab enabled/disabled; response carries `found` + raw `path` | `conversion/index.html:433,459,551,571-576` |
| 7 | Any prompt-config endpoint 404s | feature degrades — stated as design intent in the header comment | `parser/js/promptManager.js:6-15` |
| 8 | `detailFile` regex `/nodes\/(.+)\.json$/` | node **identity** recovered from a file path | `parser/js/utils.js:83`; addressing `tableDetail.js:294,439`, `sourceView.js:283,1498`, `triggerDetail.js:150` |
| 9 | Companion `.pkb` by naming convention, then `getSource(nodeId)` | 3-deep 404-probe chain for a trigger body | `parser/js/sourceView.js:694`; `triggerDetail.js:150,156,173` |
| 10 | `getSession` → `null` / `listSessions` → `[]` on non-OK | failure indistinguishable from emptiness | `shared/chat-api.js:20,26` |
| 11 | `api.js:46,52` non-OK → `{available:false}` | Claude availability inferred from an HTTP failure | `plsql/js/api.js:46,52` |
| 12 | Server-side: `listClaudeEnriched` scans for `metadata.json`/`analysis_claude.json` at depth 3 | analysis discovery is a directory walk | `ClaudePersistenceService.java:242-269`, esp. `:251` |
| 13 | Server-side: `getGlobalStats` lists directories under `plsql/` and uses `dir.leaf()` as the analysis name | ditto, and `plsql/improvement/` pollutes the namespace | `ClaudeCallLogger.java:107-118`; `ClaudeRunReporter.java:34` |
| 14 | Server-side: chunk status COMPLETE/ERROR/PENDING decided by which of `{id}_output.json` / `{id}_error.txt` exists | chunk state is not stored, it is inferred | `ClaudeChunkReader.java:188-195` |
| 15 | Server-side: `hasResult` / error-chunk counts from directory listings | progress is inferred from file presence | `ClaudeResultLoader.java:72-74,147,151-155` |

Every one of these becomes a column: `claudeEnriched:boolean`, `chunk.status:enum`, `job.hasReport:boolean`, `analysis.dbType:string`, etc.

## 4.3 Fields the UI derives client-side that should be precomputed

Ordered by impact.

| # | Derived value | Where | Why it should move |
|---|---|---|---|
| 1 | **"Is any session running?"** | `jar/js/app.js:86`; `summary-corrections.js:405` | The full session list is fetched every 30 s (twice per tick in Corrections) to compute one boolean. This is the single most wasteful recurring call in the suite. |
| 2 | **Conversion log / live-output tail** | `conversion/index.html:404,516-534` | Whole job re-fetched every 1.5 s. Needs `?sinceLine=` — which requires per-line rows (§2.9). |
| 3 | **Log level/text filtering** | `jar/js/logviewer.js:97-103`; `parser/js/logViewer.js:91-97`; `log-fab.js:123-129` | 10 000 lines shipped, then filtered in JS on every keystroke. Needs `?level=&q=&since=`. |
| 4 | **CONFIRMED/REMOVED/NEW counts** | `parser/js/claudeInsights.js:213-219`; `chunkViewer.js:62-74,226-233`; `claudeEnrichment.js:288-321,518-540` | The same aggregation reimplemented **four** times over a 138 KB document that already carries `confirmedCount`/`removedCount`/`newCount`. `claudeEnrichment.js:288-321` computes a full per-operation matrix and then only shows a toast. |
| 5 | **Per-table joins and column references** | `parser/js/tableDetail.js:294-296,439-441` | Two N-node `Promise.all` fan-outs (documents up to 370 KB each) per table modal. Needs a per-table projection. |
| 6 | **"Which call paths touch collection C"** | `jar/js/summary-popups.js:382-389` | Unbounded parallel call-tree fetch to render one popup table. |
| 7 | **Summary headline aggregates** (10 stats) | `jar/js/summary.js:236-255,286`, recomputed on every filter at `:403-420` | The `requestAnimationFrame` batching with a progress bar at `:187-212` is the smell. |
| 8 | **`entityCollMap` / `repoCollMap`** | `jar/js/summary.js:120-131,134-169` | An O(repos × entities) join in the browser. The server-supplied variants already exist as an opt-in — make them mandatory and delete the fallbacks. |
| 9 | **PL/SQL dashboard node stats** | `parser/js/summary.js:28+`, full O(nodes) pass on every scope change | Also `statements.js:14-59` (statement-type rollup with per-bucket sorting) and `perfSummary.js:77-95` (whole-history totals per keystroke). |
| 10 | **DML writes by procedure/node** | `parser/js/txnTrace.js:49-76` | Triple-nested iteration over the 307 KB tables document to build two maps. |
| 11 | **Call-graph node degrees and node↔edge join** | `parser/js/callGraphViz.js:41-80` | The server should return one graph payload with node attributes and `inEdges`/`outEdges` attached, instead of the client merging two documents and synthesising placeholders. |
| 12 | **Complexity score and risk band** | `parser/js/complexity.js:117-136` | The default-weight pass should be a stored column; keep the client path only for live weight tuning. |
| 13 | **`dbType` from `jdbcUrl`** | `parser/js/history.js:101-109` ≡ `perfSummary.js:13-22` (duplicated) | Should be a persisted field on the analysis. |
| 14 | **Validated table count** | `parser/js/app.js:74-81` | 307 401 B fetched to set one integer. Put `validatedTableCount` in `index.json`. |
| 15 | **Catalog verification status** | `jar/js/summary-tables.js:691-724` | A lowercase-`Set` join against every row, in the browser. |
| 16 | **Duplicate-JAR grouping and CORRECTED-first sort** | `jar/js/sidebar.js:44-67` | A 13-digit-suffix strip plus a two-key sort on every sidebar render. |
| 17 | **Cross-JAR Claude rollup** | `jar/js/dashboard.js:29-37,84-105` | N+1 fan-out (one request per JAR) merged and sorted client-side; `avgDurMs` at `:105` is a statistically wrong mean-of-means. |
| 18 | **Collection detection by regex** | `jar/js/callGraph.js:170-212` | Analysis logic in the browser, duplicating what the server does for Summary. |
| 19 | **Session/queue counts and filters** | `jar/js/sessions.js:109-112` (four full `filter().length` passes); `parser/js/perfSummary.js:430-446`; `plsql/js/claude.js:259-261` (whole queue shipped to show ≤3 rows); `jar/js/cross-ref.js:361` | All are `WHERE … COUNT(*)` / `WHERE type IN (…) LIMIT 3`. |
| 20 | **Analyzed-endpoint membership Set** | `jar/js/cross-ref.js:108,111-113` | A whole flow-run-history document fetched to test set membership. |
| 21 | **Markdown-fence stripping on chunk output** | `parser/js/chunkViewer.js:199-206` | The artifact should be stored as parsed JSON plus the raw text, not as a fenced blob with a `.json` extension (§2.2.7). |

---

# 5. Known defects found while auditing (fix before or with the migration)

1. `parser/js/perfSummary.js:366` calls `GET /api/analyses/{name}/claude/result`. **No controller maps `/api/analyses`** — only `/api/analysis`, `/api/parser`, `/api/plsql`. The correct path is `/api/parser/analyses/{name}/claude/result` (`parser/js/api.js:185`). The Performance-Summary detail modal therefore always reports "No verification data" (`:371`).
2. `parser/index.html:727-728` loads `shared/log-fab.js` without `shared/poll-config.js` → `ReferenceError: PollConfig is not defined` at `log-fab.js:137`; the FAB's auto-refresh is dead on the parser page.
3. `PollConfig.load()` is not awaited (`shared/poll-config.js:25`), so timers registered during initial script execution use the hard-coded defaults. Server-side poll tuning is therefore only partially effective, and not effective at all under `parser/**` or `conversion/`.
4. `plsql/js/claude.js:307` re-fetches the whole `/api/queue` on every SSE event lacking `state`, amplifying job-event bursts into full-queue fetch bursts.
5. `jar/js/cross-ref.js:995-997` constructs an `EventSource` and immediately `.close()`s it — dead code beside the working `fetch`+`ReadableStream` SSE reader.
6. `ClaudeRunReporter.java:70,88` iterates `t.claudeVerifications` unguarded while guarding `vr.tables`; a null `claudeVerifications` NPEs. The surrounding `catch (Exception)` at `:106-108` degrades it to a warning and no report, so failures are silent.
7. `plsql/{name}/analysis_claude.json` and `plsql/{name}/claude/_result.json` are **byte-identical duplicates** (verified with `cmp` on real data, 138 301 B each) written by two different code paths.
8. `data-backup-.../prompts` and `data-backup-.../config/prompts` hold **overlapping but non-identical** template sets (`config/prompts` has `plsql-postgres-verification.txt`; `prompts` has four other `plsql-*-verification.txt` files the other lacks).
9. `JarDataPaths.chatHistoryKey` → `jar/{jarId}/chat/history.jsonl` (`JarDataPaths.java:143`) has no caller — a dead key that would otherwise look like the chat storage location.
10. `ConversionController.java:482-485,542-561` never persists `jarId` (nor `service`/`controller`/`owner`/`batch`), so there is **no durable link from a conversion job back to a JAR**.
11. `ChatController.java:179-180,219-220` expose unauthenticated arbitrary read/write under the data root (512 KB read cap at `:150`).

---

# 6. Open items requiring a runtime check

- Real sizes and cardinalities for: `jar/crossref/**` (`meta.json`, `result.json`, `claude/{chunkId}.json`, `checkpoint.json`, `report.md`), `jar/flowanalysis/**` (`status.json`, `log.jsonl`, `results.json`, `report.md`), `conversion/{jobId}.json`, `claude-chatbot/{type}/{id}.json`, `jar/{jarId}/external-links{,-pending}.json`, `prompt-extras.json`. **None of these exist in either snapshot.**
- The shape of `flowanalysis/{runId}/results.json` — `FlowRunStore.java:212-218` writes an opaque pre-serialised string and reads it back untyped; the schema is owned by the flow analyzer, not the store.
- The actual byte size served by `GET /api/{jar,plsql}/logs?lines=10000` against the 9.36 MB log file, and whether `truncated` is set.
- Worst-case `_result.json` size for the largest PL/SQL analysis (the measured 138 KB is for 34 tables / 445 ops; the largest parse has 42 tables / 218 unique edges).
- Whether `plsql/improvement/` has ever accumulated in a long-lived environment, and how fast (it is absent from both snapshots).
