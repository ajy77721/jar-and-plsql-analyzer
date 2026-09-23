# PLAN-08 — UI data contract, queries, indexes, pagination

Owner of constraint **C3 — the UI must not break** (PLAN-00). This document turns PLAN-03's
access-pattern table into concrete queries against PLAN-05's DDL, names the index that serves each
one, and defines the pagination, polling and precomputation changes the UI must make in lockstep so
that the storage work actually produces a latency win.

**The blocking problem, restated.** PLAN-03 §1.5 #3: *no endpoint anywhere accepts `page`, `offset`,
`limit` or a cursor.* All 20+ tables fetch a full array and slice in the browser
(`jar/js/summary-helpers.js:532`; `parser/js/pagination.js:33-40`, `tableFramework.js:33-56`).
PLAN-07 §2.2 designs a paged `AnalysisRepository` — but a paged repository behind an unpaged
controller behind a client-side slicer buys **zero** page-load improvement. Sections 3 and 4 below
are therefore not optional polish; they are the part of the plan that converts schema work into
observable speed.

**Scope of change.** This document extends the `AnalysisRepository` sketched in PLAN-07 §2.2 (it
does not propose a rival interface), adds a small number of controller-level query parameters, and
sequences the UI conversion. It changes no `ArtifactStore` signature (C1).

---

# 1. The query / index table

## 1.0 How to read it

- **Q-id** carries PLAN-03's own id (`P*` polled, `I*` initial load, `L*` lazy) so every row is
  traceable back to its evidence.
- **Repository method** is either a method already in PLAN-07 §2.2 or an **extension** proposed in
  §1.5 below; extensions are marked **[new]**.
- **Covered?** — "covered" means the index supplies every column the query filters, sorts and
  projects, so the engine never touches the heap/document. "index-seek" means the index locates the
  rows but a small bounded fetch follows (acceptable). "SCAN" is a defect and is flagged in §2.4.
- SQL is written against PLAN-05's PostgreSQL DDL. The MongoDB equivalent is given in §2.5 — the
  index shapes are the same because PLAN-05's tables map one-to-one onto collections.

## 1.1 Hot paths (polled)

| Q | PLAN-03 | Entity / table | Filter | Sort | Projection | Result size | Init? | Polled? | Repository method | Index | Covered? |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Q1 | P1, P2 | `claude_session` | `status='RUNNING'` — **existence only** | — | `EXISTS` | **1 boolean** (was: hundreds of rows) | yes, every page | 30 s → **SSE** (§5.1) | `boolean anySessionRunning(Scope)` | `ix_session_running` (partial, `WHERE status='RUNNING'`) | **covered** — partial-index `LIMIT 1` |
| Q2 | P2 | `claude_session` | `scope`, optional `status` | `started_at DESC` | `session_id,status,session_type,subject_id,started_at,finished_at,call_count,total_duration_ms` | ≤ page | no (modal) | 30 s while open | `Page<SessionSummary> sessions(Scope, SessionFilter, PageRequest)` **[new]** | `ix_session_scope_started` **[new]** `(subject_kind, subject_id, started_at DESC)` | index-seek |
| Q3 | P3 | `analysis_run` + `claude_verification` | `subject_id=:jarId AND kind='CLAUDE_VERIFY' AND is_current` | — | `status, completed, total, error` | 1 row | no | 30 s → **SSE** | `Optional<ProgressSnapshot> claudeProgress(AnalysisId)` **[new]** | `uq_run_current (subject_kind, subject_id, kind) WHERE is_current` | **covered** |
| Q4 | P4 | `endpoint_correction` | `analysis_id=:id` | `endpoint_id` | `endpoint_id, added[], removed[], verified[]` + 3 rollup counts | ≤ page | no | 30 s → ETag (§5.4) | `Page<CorrectionRow> corrections(AnalysisId, PageRequest)` **[new]** | `ix_correction_analysis` **[new]** `(analysis_id, endpoint_id)` | index-seek |
| Q4b | P4 | — | — | — | `totalCorrections, totalAdded, totalRemoved, totalVerified` | 4 ints | no | 30 s | `CorrectionTotals correctionTotals(AnalysisId)` **[new]** | same, aggregated; or `jar_analysis.correction_count` column | **covered** |
| Q5 | P5, P12 | log artifact (not a table) | `scope`, `sinceSeq`/byte offset, `level`, `q` | `seq ASC` | `seq, ts, level, message` | ≤ `limit` (default 200) | no | 3–10 s → `seq` cursor | `Page<LogLine> logLines(LogScope, LogFilter, PageRequest)` | ranged `ArtifactStore.read(key, offset, -1)` (local/s3); `(scope_id, seq)` PK (mongodb) | index-seek |
| Q6 | P6 | as Q5, `limit=20` | as Q5 | `seq DESC` | as Q5 | 20 rows | no | 10 s | same, `PageRequest.first(20)` with `Sort.SEQ_DESC` | same | index-seek |
| Q7 | P7, P8 | `job_queue_entry` | `state IN (:states)` [+ `kind IN (:kinds)`] | `enqueued_at ASC` (queued), `finished_at DESC` (history, `LIMIT 3`) | `job_id,kind,subject_id,state,progress,enqueued_at` | current + `limit` queued + `limit` history | yes, home | 2–3 s → **SSE** (`/api/queue/events` exists) | `QueueSnapshot queue(QueueFilter, int queuedLimit, int historyLimit)` **[new]** | `ix_queue_state (state, enqueued_at)`; add `ix_queue_kind_state` **[new]** `(kind, state, enqueued_at)` for P8's type filter | index-seek |
| Q8 | P9 | `job_queue_entry` | `job_id=?` | — | whole row | 1 row | no | 2 s (SSE fallback only) | `Optional<QueueJob> queueJob(String jobId)` **[new]** | PK | **covered** |
| Q9 | P10, P15 | `analysis_run` + `claude_chunk` | `run_id=:current run for analysis` | — | `percent, completedChunks, totalChunks, failedChunks, status, hasRunningSession, isComplete` | 7 scalars | no | 2 s → **SSE** (stream already exists) | `claudeProgress(AnalysisId)` (Q3) | `uq_run_current`; chunk counts from `claude_verification.total_chunks/error_chunks` columns, **not** a chunk scan | **covered** |
| Q10 | P11 | `claude_call_log_entry` | `scope`, `seq > :sinceSeq` | `seq ASC` | full row | delta only (typically 0–3 rows) | no | 5 s → `sinceSeq` cursor | `Page<LogLine> logLines(...)` with `LogScope.CLAUDE_CALLS` | PK `(scope_kind, scope_id, seq)` | **covered** — range scan on PK prefix |
| Q10b | P11 | `claude_call_log_entry` | `scope` | — | `SUM(prompt_chars), SUM(response_chars), SUM(duration_ms), COUNT(*), COUNT(*) FILTER (WHERE NOT success), COUNT(DISTINCT session_id)` | 7 scalars | no | 5 s → 30 s + ETag | `CallLogStats callLogStats(Scope)` **[new]** | see §2.4 — **needs a maintained rollup**, aggregate over an unbounded log is a scan | **SCAN today → rollup table** |
| Q11 | P13 | `conversion_job` | `job_id=?` | — | `status, phase, elapsed_ms, error, report_ref IS NOT NULL AS hasReport` — **no** `log[]`, **no** `liveLines[]`, **no** `instructionMd` | ~6 scalars (was: whole job) | no | 1.5 s | `Optional<ConversionJobSummary> conversionJob(JobId)` **[new]** | PK | **covered** |
| Q11b | P13 | `conversion_job_log_entry` | `job_id=?, stream=?, seq > :sinceSeq` | `seq ASC` | `seq, ts, message` | delta only | no | 1.5 s | `logLines(LogScope.CONVERSION(job, stream), …)` | PK `(job_id, stream, seq)` | **covered** |
| Q12 | P14 | `flow_analysis_run` (+ `flow_analysis_log_entry`) | `flow_run_id=?`; log `seq > :sinceSeq` | `seq ASC` | `status, phase, mode, error` + log delta | small | no | 1.5 s → **SSE** | `Optional<FlowRunStatus> flowRun(RunId)` **[new]** + `logLines` | PK; log PK `(flow_run_id, seq)` | **covered** |
| Q13 | P16 | `job_queue_entry` | `job_id=?` | — | 1 row | 1 row | no | 10 s | Q8 | PK | **covered** |
| Q14 | P17 | polling config | — | — | 5 ints | 5 ints | yes, every page | once | *(not a repository call — `PollingConfigController`)* | n/a | n/a |

## 1.2 Initial-load reads

| Q | PLAN-03 | Entity / table | Filter | Sort | Projection | Result size | Init? | Repository method | Index | Covered? |
|---|---|---|---|---|---|---|---|---|---|---|
| Q20 | I1 | `jar_artifact ⋈ jar_analysis` | `version='CORRECTED' OR 'STATIC'`, current only | **server-side**: `analysis_mode='CORRECTED'` first, then `analyzed_at DESC`, grouped by `jar_artifact.jar_id` | the exact 14 fields the sidebar renders (PLAN-03 §3.1) | 14 × page | yes | `Page<AnalysisSummary> analyses(AnalysisFilter, PageRequest)` | `ix_jar_analysis_current (jar_id, version, analyzed_at DESC)` + `ix_jar_sidebar` **[new]** `(analysis_mode DESC, analyzed_at DESC) INCLUDE (jar_id, …)` | index-seek; sort is index-ordered |
| Q21 | I2 | `jar_analysis` | `analysis_id` [+`version`] | — | header scalars only: `jar_name, project_name, analyzed_at, jar_size, total_classes, total_endpoints, analysis_mode, base_package, correction_applied_at, correction_count, claude_iteration` | **~400 B** (was **430 KB**) | yes | `Optional<AnalysisSummary> analysis(AnalysisId)` | PK | **covered** |
| Q21b | I2 | `endpoint` | `analysis_id=?` | `ordinal` | endpoint list columns (see Q30) | page of 25–100 | yes (first page only) | `Page<EndpointSummary> endpoints(AnalysisId, EndpointFilter, PageRequest)` | `ix_endpoint_analysis_ord` **[new]** `(analysis_id, ordinal)` (uq_endpoint_ordinal already provides it) | index-seek |
| Q22 | I3 | `jar_analysis` | `jar_id=?` | — | `bool_or(version='CORRECTED') AS hasCorrected, bool_or(version='CORRECTED_PREV'), max(claude_iteration)` | 3 scalars | yes | folded into Q21 — **one request, not two** | `ix_jar_analysis_current` | **covered** |
| Q23 | I4 | `connection_info` | `analysis_id=?` | — | `available, mongodb.*, oracle.*, postgres.*, extracted_at` | ~130 B | yes (fetch **once**, not twice) | `Optional<ConnectionInfo> connections(AnalysisId)` **[new]** | PK/FK on `analysis_id` | **covered** |
| Q24 | I5 | `domain_config` | `analysis_id=?` | — | whole row | 7.8 KB | yes (fetch **once**) | `Optional<DomainConfig> domainConfig(AnalysisId)` **[new]** | FK index | **covered** |
| Q25 | I6 | as Q3 | | | | | yes | `claudeProgress` | `uq_run_current` | **covered** |
| Q26 | I7 | `claude_call_log_rollup` (§2.4) | `scope_id IN (:all jars)` | — | 6 aggregate columns per jar | **1 request** (was: N requests, one per JAR) | yes, dashboard | `Map<Scope,CallLogStats> callLogStatsBatch(List<Scope>)` **[new]** | `pk (scope_kind, scope_id)` on the rollup | **covered** — kills the `Promise.allSettled` fan-out |
| Q27 | I8 | chat config | — | — | 2 booleans | tiny | yes | *(controller constant; should be inlined into the page, see §6.3)* | n/a | n/a |
| Q28 | I9 | `plsql_analysis` | `analysis_id=?` | — | the ~20 rollup scalars **incl. `validated_table_count`** | **~600 B** (was **87 KB**) | yes | `analysis(AnalysisId)` | PK | **covered** |
| Q29 | I9 | `flow_node` | `analysis_id=?` | `node_uid` | `node_id, name, schema, object_type, depth, lines_of_code, counts.*` | page of 100 | yes (first page) | `Page<FlowNodeSummary> flowNodes(AnalysisId, NodeFilter, PageRequest)` | `ix_flownode_type (analysis_id, object_type)`; add `ix_flownode_page` **[new]** `(analysis_id, node_uid)` | index-seek |
| Q30 | I10 | **deleted** | — | — | — | **0 B** (was **307 KB for one integer**) | — | `plsql_analysis.validated_table_count` is already a PLAN-05 column | n/a | n/a |
| Q31 | I11 | `plsql_analysis ⋈ plsql_analysis_key ⋈ analysis_run` | facet filters **server-side** (`project, environment, db_type, connection_name`) | `started_at DESC` | history-card fields + `array_length(errors,1) AS errorCount` (**not** `errors[]`) | page of 25 | yes | `analyses(AnalysisFilter, PageRequest)` | `ix_run_subject (subject_kind, subject_id, started_at DESC)`; add `ix_plsql_facets` **[new]** `(db_type, project, environment, started_at DESC)` | index-seek |
| Q32 | I12 | as Q10b | global scope | — | 6 numbers | 6 numbers | yes (and on every `goHome()` — **memoise**) | `callLogStats(Scope.GLOBAL)` | rollup PK | **covered** |
| Q33 | I13 | project/env config | — | — | small | small | yes | `connection_scope` listing | PK | **covered** |
| Q34 | I14 | `conversion_job` | none | `started_at DESC` | list columns, **`liveLines` already stripped** server-side | page of 25 (was: server cap 50) | yes | `Page<ConversionJobSummary> conversionJobs(PageRequest)` **[new]** | `ix_conversion_started` **[new]** `(started_at DESC)` | index-seek |
| Q35 | I15 | conversion Claude status | — | — | 2 fields | tiny | yes | *(controller)* | n/a | n/a |

## 1.3 Lazy reads (tab switch / row click / modal)

| Q | PLAN-03 | Entity / table | Filter | Sort | Projection | Result size | Repository method | Index | Covered? |
|---|---|---|---|---|---|---|---|---|---|
| Q40 | L1 | `java_class` | `analysis_id=?` [+ `source_jar`, `package_name` prefix] | `source_jar, package_name, simple_name` — **server-side** | tree-header fields; **bodies lazy** | page of 200 | `Page<ClassSummary> classes(AnalysisId, ClassFilter, PageRequest)` **[new]** | `ix_class_pkg (analysis_id, package_name, simple_name)` — *authored for exactly this sort*; `ix_class_sjar` for grouping | index-seek |
| Q41 | L2 | `bundled_jar` / `bundled_jar_resource` / `jar_dep_edge` | `analysis_id=?` | `name` | `name, size, resource_count` | page of 100 (was **3.03 MB**) | `Page<BundledJarRef> bundledJars(AnalysisId, PageRequest)` **[new]** | FK + `(analysis_id, name)` **[new]** | index-seek |
| Q42 | L3 | `call_node` | `endpoint_id=?` | `parent_ordinal, ordinal` | tree node fields | page of 500, depth-bounded | `Page<CallNodeSummary> callTreeNodes(EndpointId, PageRequest)` | `ix_node_tree (endpoint_id, parent_ordinal, ordinal)` | index-seek |
| Q43 | **L4** | `endpoint_collection` | `collection_name=?` [+ analysis scope] | `endpoint_id` | `endpoint_id, full_path, http_method, controller_simple_name` | page of 50 | `Page<EndpointRef> endpointsUsingTable(TableSelector, PageRequest)` | `ix_epcoll_name (collection_name, endpoint_id)` | **covered** — *this single query replaces an unbounded `Promise.all` of N call-tree fetches* |
| Q43b | L4 | `call_node_collection` | `collection_name=?` | — | `node_uid` → path | page of 50 | `Page<NodeRef> nodesUsingCollection(TableSelector, PageRequest)` **[new]** | `ix_nodecoll_name (collection_name)` | index-seek |
| Q44 | L5 | `call_node` | `endpoint_id=?` | `ordinal` | flattened node list | page of 500 | `callTreeNodes` (same as Q42) | `ix_node_tree` | index-seek |
| Q45 | L6 | `storage_ref` via `call_node.source_ref` | `node_id=?` | — | an `ArtifactKey`, then bytes | ≤ 6.2 KB | `Optional<ArtifactKey> blobRef(BlobRef)` → `ArtifactStore.read` | PK | **covered** (key arithmetic, no I/O) |
| Q46 | L7 | `endpoint` | `analysis_id=?`, `endpoint_id IN (:page)` | — | one jsonb column per slice (`external_calls`, `dynamic_flows`, `aggregation_flows`, `beans`) | one array per **visible** endpoint | `Page<EndpointSlice> endpointSlices(AnalysisId, SliceKind, List<EndpointId>)` **[new]** | PK on `endpoint_id` | **covered** — **and it is keyed by `endpoint_id`, closing PLAN-03 §1.5 #4's positional coupling** |
| Q47 | L8 | `java_class.source_ref` | `analysis_id, fqn` | — | key → bytes | one class | `blobRef` → `ArtifactStore.read` | `uq_class (analysis_id, fqn)` | **covered** |
| Q48 | L9 | `db_object` | `analysis_id=?` | `name` | `name, kind, verified` | page | `Page<TableRef> tables(AnalysisId, TableFilter, PageRequest)` | `ix_dbobject_name (name)` | index-seek — **and `verified` becomes a column, so the browser-side `Set` join (PLAN-03 §4.3 #15) disappears** |
| Q49 | L10, L11 | `claude_chunk` | `run_id=?` [+ `endpoint`/`table` link] | `chunk_id` | `chunk_id, name, status, table_count, node_ids[]` | page of 50 | `Page<ChunkSummary> chunks(AnalysisId, ChunkFilter, PageRequest)` **[new]** | `ix_chunk_run` **[new]** `(run_id, chunk_id)` | index-seek — **kills the filename-substring correlation of PLAN-03 §4.2 rows 1–2** |
| Q50 | L13 | `claude_session` | `session_id=?` | — | detail | 1 row | `Optional<SessionSummary> session(SessionId)` **[new]** | PK | **covered** |
| Q51 | L14 | `cross_ref_run` | `jar_id=?` | `started_at DESC` | meta fields | page of 25 | `Page<CrossRefRunSummary> crossRefRuns(AnalysisId, PageRequest)` **[new]** | `ix_run_subject` | index-seek |
| Q52 | L15 | `cross_ref_proc_link` | `cross_ref_run_id=?` | `seq` | link row | page of 50 (was: the whole `result.json`) | `Page<ProcLinkRow> procLinks(CrossRefRunId, PageRequest)` **[new]** | `(cross_ref_run_id, seq)` PK | **covered** |
| Q53 | L16 | `claude_chunk` | `run_id, chunk_id` | — | `verdict, confidence, duration_ms, reason, gaps[]`, `prompt_ref`, `response_ref` | 1 row + 2 blob keys | `Optional<ChunkDetail> chunk(ChunkId)` **[new]** | `ix_chunk_run` | index-seek |
| Q54 | L17 | source blobs | `node_id` / `proc_ref` | — | keys → bytes | text | `blobRef` | PK | **covered** |
| Q55 | L18 | `db_object` / `resolver_object` | `scope_id=?`, `name ILIKE :q%` — **server-side filter**, and `COUNT(*) FILTER (WHERE analyzed)` | `name` | `proc_name, schema, analyzed, session_id` | page of 50 | `Page<ProcRef> availableProcs(ScopeId, String q, PageRequest)` **[new]** | `ix_resolver_name` **[new]** `(scope_id, name text_pattern_ops)` | index-seek |
| Q56 | L19 | `flow_analysis_run` | `jar_id=?` [+ **`mode=?` server-side**] | `started_at DESC` | status doc | page of 25 | `Page<FlowRunSummary> flowRuns(AnalysisId, FlowRunFilter, PageRequest)` **[new]** | `ix_run_subject` | index-seek |
| Q56b | L19 | `endpoint` ⋈ `flow_analysis_run` | `jar_id=?` | — | `full_path` **only** — the membership Set | one string array | `Set<String> analyzedEndpointPaths(AnalysisId)` **[new]** | `ix_run_subject` | **covered** — replaces PLAN-03 §4.3 #20's whole-history fetch for a membership test |
| Q57 | L20 | `db_object` | `analysis_id=?` | `name` | table row | page of 50 (was **307 KB × 5 per session**) | `tables(AnalysisId, TableFilter, PageRequest)` | `ix_dbobject_name` | index-seek — **memoise once per analysis in the client** |
| Q58 | L21 | `flow_node` | `node_uid=?` | — | full detail (children paged separately) | ≤ page | `Optional<FlowNodeDetail> flowNode(NodeId)` | PK | **covered** |
| Q59 | **L22** | `table_join` | `left_table=? OR joined_table=?` [+ analysis scope] | `left_table` | join rows | page of 50 | `Page<JoinRef> tableJoins(TableSelector, PageRequest)` | `ix_join_tables (joined_table, left_table)` + **[new]** `ix_join_left (left_table, joined_table)` (the OR needs both arms) | index-seek — *replaces the first N-node `Promise.all`* |
| Q59b | L22 | `db_column` ⋈ `db_object_usage` | `object_id=?` | `column_name` | column refs | page of 100 | `Page<ColumnRef> tableColumns(TableSelector, PageRequest)` | `(object_id, column_name)` **[new]** | index-seek — *replaces the second `Promise.all`* |
| Q60 | L23 | `flow_edge_dedup` (+ node attrs) | `analysis_id=?` | `from_node_id` | `from,to,fromSchema,toSchema` **joined to node attrs and `inEdges`/`outEdges` server-side** | page of 500 | `Page<GraphEdge> callGraph(AnalysisId, GraphFilter, PageRequest)` **[new]** | `ix_edge_from (analysis_id, from_node_id)`, `ix_edge_to` | index-seek — **the browser-side node↔edge join (PLAN-03 §4.3 #11) moves to the server** |
| Q61 | L24 | facet tables | `analysis_id=?`, `kind` | facet-specific | facet rows | page of 100 | `Page<FacetRow> facet(AnalysisId, FacetKind, PageRequest)` | `ix_cursor_name`, `ix_stmt_type`, `ix_join_tables`, dynamic-sql FK | index-seek — *these are the four "scan every node" endpoints PLAN-07 §2.1 names* |
| Q62 | L25 | `plsql_source_file` | `analysis_id, reference_name` [+ **`fromLine`/`toLine`**] | — | key → **ranged** bytes | one slice (was: whole file) | `blobRef` → `ArtifactStore.read(key, offset, length)` | `uq_source_ref` | **covered** — the line-range slice PLAN-03 §3.9 asks for |
| Q63 | L26 | `claude_verification` | current for analysis | — | the 7 scalars *only* | **~80 B** (was **138 KB × 5 screens**) | `Optional<VerificationSummary> verification(AnalysisId)` **[new]** | `(plsql_key_id, claude_iteration DESC) WHERE superseded_by IS NULL` **[new]** | **covered** — *this is also §7's broken-screen fix* |
| Q63b | L26 | `claude_table_verification` ⋈ `claude_operation_verification` | `verification_id=?` [+ `status`, `user_decision IS NULL`] | `table_name, seq` | verification rows | page of 50 | `Page<TableVerificationRow> verificationTables(VerificationId, VerificationFilter, PageRequest)` **[new]** | `uq_claude_table`, `ix_claude_op_status`, `ix_claude_op_undecided` (partial) | index-seek |
| Q64 | L27 | `claude_chunk` | `run_id=?` | `chunk_id` | summary + `completeCount/errorCount/pendingCount` from columns | 25 rows | `chunks(...)` (Q49) | `ix_chunk_run` | index-seek |
| Q65 | L28 | as Q53 | | | | | `chunk(ChunkId)` | `ix_chunk_run` | index-seek |
| Q66 | L29 | `chat_session` | `scope`, `chat_type` | `updated_at DESC` | `id, name, message_count, updated_at` | page of 25 | `Page<ChatSessionRef> chatSessions(ChatFilter, PageRequest)` **[new]** | `ix_chat_scope` **[new]** `(scope, chat_type, updated_at DESC)` | **covered** — `message_count` is a column (PLAN-05 §4.7) |
| Q67 | **L30** | `chat_message` | `session_id=?`, `seq < :before` | `seq DESC` | `role, content, ts` | page of 50 (was: **entire transcript**) | `Page<ChatMessage> chatMessages(ChatSessionId, PageRequest)` **[new]** | PK `(session_id, seq)` | **covered** — backward paging on the PK |
| Q68 | L31 | `conversion_job.report_ref` | `job_id=?` | — | key → bytes | markdown | `blobRef` | PK | **covered** |
| Q69 | L32 | `prompt_template` / `prompt_override` | `name` / scope | `name` | body via `body_ref` | ≤ 5.6 KB | `Page<PromptRef> prompts(PromptFilter, PageRequest)` **[new]** | PKs | **covered** |

## 1.4 Writes

Writes are unchanged in shape; they are listed only because §3's cursor stability depends on them.

| W | PLAN-03 | Table(s) | Effect on read cursors |
|---|---|---|---|
| W1 | review decision | `claude_operation_verification.user_decision` | **update in place** — no key change, cursors stable |
| W2 | apply changes | `claude_verification` (new iteration, `superseded_by` set) | new rows appear at the *head* of `claude_iteration DESC`; a cursor already issued is unaffected (§3.4) |
| W4 | chat message | `chat_message` insert at `seq+1` | append-only; backward cursors stable by construction |
| W5 | conversion milestones | `conversion_job` update + `conversion_job_log_entry` insert | append-only log; `seq` cursor stable |
| W6 | cross-ref chunk flush | `claude_chunk` update | in-place |
| W7/W8 | flow status + log | `flow_analysis_run` update, `flow_analysis_log_entry` insert | append-only |
| W9 | Claude call-log line | `claude_call_log_entry` insert + **rollup upsert** (§2.4) | append-only |
| W10 | prompt edits | `prompt_template` / `prompt_override` | in-place |

## 1.5 `AnalysisRepository` extensions

These extend PLAN-07 §2.2 — same package, same `Page`/`PageRequest` types, same rules (cursor
paging, natural-key identity, no bytes). Nothing here replaces an existing method.

```java
public interface AnalysisRepository {

    // ─────────── (everything in PLAN-07 §2.2 stays exactly as written) ───────────

    // ── cheap predicates and snapshots that today cost a whole document ──────────
    /** Q3/Q9/P3/P10/P15. One row, from uq_run_current. Never a chunk scan. */
    Optional<ProgressSnapshot> claudeProgress(AnalysisId id);

    /** Q63/L26. The 7 scalars five screens fetch a 138 KB document to read. */
    Optional<VerificationSummary> verification(AnalysisId id);

    /** Q10b/Q32/I12. Served from the maintained rollup (§2.4), never by scanning the log. */
    CallLogStats callLogStats(Scope scope);
    /** Q26/I7. Replaces the dashboard's one-request-per-JAR Promise.allSettled fan-out. */
    Map<Scope, CallLogStats> callLogStatsBatch(List<Scope> scopes);

    /** Q7/P7/P8. current + bounded queued + bounded history, filtered server-side. */
    QueueSnapshot queue(QueueFilter filter, int queuedLimit, int historyLimit);
    Optional<QueueJob> queueJob(String jobId);

    // ── paged listings the UI currently slices client-side ───────────────────────
    Page<SessionSummary>        sessions(Scope scope, SessionFilter f, PageRequest p);
    Optional<SessionSummary>    session(SessionId id);
    Page<ClassSummary>          classes(AnalysisId id, ClassFilter f, PageRequest p);
    Page<BundledJarRef>         bundledJars(AnalysisId id, PageRequest p);
    Page<CorrectionRow>         corrections(AnalysisId id, PageRequest p);
    CorrectionTotals            correctionTotals(AnalysisId id);
    Page<ChunkSummary>          chunks(AnalysisId id, ChunkFilter f, PageRequest p);
    Optional<ChunkDetail>       chunk(ChunkId id);
    Page<TableVerificationRow>  verificationTables(VerificationId v, VerificationFilter f, PageRequest p);
    Page<GraphEdge>             callGraph(AnalysisId id, GraphFilter f, PageRequest p);
    Page<NodeRef>               nodesUsingCollection(TableSelector t, PageRequest p);
    Page<EndpointSlice>         endpointSlices(AnalysisId id, SliceKind kind, List<EndpointId> visible);

    // ── cross-ref, flow, conversion, chat, prompts ───────────────────────────────
    Page<CrossRefRunSummary>    crossRefRuns(AnalysisId id, PageRequest p);
    Page<ProcLinkRow>           procLinks(CrossRefRunId id, PageRequest p);
    Page<ProcRef>               availableProcs(ScopeId scope, String query, PageRequest p);
    Page<FlowRunSummary>        flowRuns(AnalysisId id, FlowRunFilter f, PageRequest p);
    Optional<FlowRunStatus>     flowRun(FlowRunId id);
    Set<String>                 analyzedEndpointPaths(AnalysisId id);
    Page<ConversionJobSummary>  conversionJobs(PageRequest p);
    Optional<ConversionJobSummary> conversionJob(ConversionJobId id);
    Page<ChatSessionRef>        chatSessions(ChatFilter f, PageRequest p);
    Page<ChatMessage>           chatMessages(ChatSessionId id, PageRequest p);
    Page<PromptRef>             prompts(PromptFilter f, PageRequest p);

    // ── connections / domain config (fetched twice per JAR click today) ──────────
    Optional<ConnectionInfo>    connections(AnalysisId id);
    Optional<DomainConfig>      domainConfig(AnalysisId id);
}
```

Two supporting records that the polling design (§5) depends on:

```java
/** Q3/Q9. Everything four different progress screens read, in one row. */
public record ProgressSnapshot(String status, int completed, int total, int failed,
                               boolean hasRunningSession, boolean complete,
                               List<String> errors, String etag) {}

/** §5.4. Every snapshot type carries an etag so the controller can answer 304. */
public interface Etagged { String etag(); }
```

> **OPEN QUESTION Q-A — does `Page.total` stay `-1` for the scan implementation?**
> PLAN-07 §2.2 allows `total = -1` when counting is not cheap. The client-side pagers render
> "Page 3 of 17", which needs a total.
> Options: (a) render "Page 3, more →" when `total < 0`; (b) make the scan implementation count by
> doing the work twice on the first page only and cache it.
> **Recommendation: (a).** It is honest, it is a one-line UI change in `_initPage`, and it removes
> the temptation for the Mongo implementation to fabricate a count on a 500 MB facet. Add
> `Page.totalIsExact()` so the pager can choose.

---

# 2. Indexes

## 2.1 Indexes PLAN-05 already declares that serve a hot query

These need no change. The DDL author already had the access pattern in view — each row below is an
index whose comment in PLAN-05 names the query it exists for.

| Index (PLAN-05) | Serves | Why it is enough |
|---|---|---|
| `ix_session_running ON claude_session (started_at DESC) WHERE status='RUNNING'` | **Q1** | Partial index. `EXISTS` is a single index probe against an index that is *usually empty*. This is the fix for "the single most wasteful recurring call in the suite". |
| `ix_run_running ON analysis_run (kind, started_at DESC) WHERE status='RUNNING'` | Q1 (run flavour), Q7 | same shape |
| `uq_run_current ON analysis_run (subject_kind, subject_id, kind) WHERE is_current` | **Q3, Q9, Q25** | unique partial → at most one row, no sort |
| `ix_class_pkg ON java_class (analysis_id, package_name, simple_name)` | **Q40** | PLAN-05 annotates it "L1 sort order" — the index *is* the sort, so the code-tree page is an ordered range scan |
| `ix_class_sjar ON java_class (analysis_id, source_jar)` | Q40 grouping | |
| `ix_node_tree ON call_node (endpoint_id, parent_ordinal, ordinal)` | **Q42, Q44** | annotated "L3/L5 tree walk"; a depth-bounded page is a prefix range |
| `ix_epcoll_name ON endpoint_collection (collection_name, endpoint_id)` | **Q43** | the single query that replaces L4's unbounded fan-out |
| `ix_nodecoll_name ON call_node_collection (collection_name)` | Q43b | annotated "L4: who touches C?" |
| `ix_join_tables ON table_join (joined_table, left_table)` | **Q59** (right arm) | see §2.3 — the left arm needs a second index |
| `ix_cursor_name ON flow_node_cursor (name)` | Q61 | annotated "backs GET /cursors without a full node scan" |
| `ix_stmt_type ON flow_node_statement (node_uid, type)` | Q61 | |
| `ix_edge_from / ix_edge_to ON flow_edge (analysis_id, …)` | **Q60** | both directions, so `inEdges`/`outEdges` are countable server-side |
| `ix_claude_op_status`, `ix_claude_op_undecided … WHERE user_decision IS NULL` | Q63b | the partial index makes "show me what still needs review" free |
| `ix_queue_state ON job_queue_entry (state, enqueued_at)` | Q7, Q8, Q13 | |
| `ix_dbobject_name ON db_object (name)` | Q48, Q57 | cross-analysis "who else uses this table" |
| `ix_jar_analysis_current ON jar_analysis (jar_id, version, analyzed_at DESC)` | Q20, Q22 | |
| `ix_run_subject ON analysis_run (subject_kind, subject_id, started_at DESC)` | Q31, Q51, Q56 | |
| `ix_calllog_ts ON claude_call_log_entry (ts DESC)` | (reporting only) | **not** what Q10 uses — Q10 uses the PK prefix `(scope_kind, scope_id, seq)` |
| PK `(scope_kind, scope_id, seq)`, `(job_id, stream, seq)`, `(flow_run_id, seq)`, `(session_id, seq)` | **Q10, Q11b, Q12, Q67** | every append-only cursor read is a PK range scan. This is the whole point of PLAN-05 §1.8's one-row-per-entry decision. |

## 2.2 New indexes this document requires

```sql
-- Q2: session modal, scoped and time-ordered
CREATE INDEX ix_session_scope_started
    ON claude_session (subject_kind, subject_id, started_at DESC);

-- Q4: corrections list, paged by endpoint
CREATE INDEX ix_correction_analysis
    ON endpoint_correction (analysis_id, endpoint_id);

-- Q7 (P8): the parser Queue tab filters to PL/SQL job kinds client-side today
CREATE INDEX ix_queue_kind_state
    ON job_queue_entry (kind, state, enqueued_at);

-- Q20: the sidebar's two-key sort (CORRECTED before STATIC, then analyzed_at desc),
--      done in the index instead of in sidebar.js:44-67 on every render
CREATE INDEX ix_jar_sidebar
    ON jar_analysis ((analysis_mode = 'CORRECTED') DESC, analyzed_at DESC)
    INCLUDE (jar_id, version, total_classes, total_endpoints, claude_iteration);

-- Q29: stable, index-ordered paging of flow nodes
CREATE INDEX ix_flownode_page ON flow_node (analysis_id, node_uid);

-- Q31: the history facet filters, applied server-side
CREATE INDEX ix_plsql_facets
    ON analysis_run (subject_kind, subject_id, started_at DESC)
    INCLUDE (status);
CREATE INDEX ix_plsql_key_facets
    ON plsql_analysis_key (db_type, project, environment, object_name);

-- Q34: conversion job list
CREATE INDEX ix_conversion_started ON conversion_job (started_at DESC);

-- Q41: bundled jars, paged by name
CREATE INDEX ix_bundled_name ON bundled_jar (analysis_id, name);

-- Q49/Q53/Q64: chunk listing and detail
CREATE INDEX ix_chunk_run ON claude_chunk (run_id, chunk_id);

-- Q55: available-procs substring filter, server-side (prefix-anchored; see §2.4)
CREATE INDEX ix_resolver_name
    ON resolver_object (scope_id, name text_pattern_ops);

-- Q59: table_join's OR needs BOTH arms indexed
CREATE INDEX ix_join_left ON table_join (left_table, joined_table);

-- Q59b: per-table columns
CREATE INDEX ix_dbcolumn_object ON db_column (object_id, column_name);

-- Q63: current (non-superseded) verification for a subject
CREATE INDEX ix_verification_current
    ON claude_verification (plsql_key_id, claude_iteration DESC)
    WHERE superseded_by IS NULL;
CREATE INDEX ix_verification_current_jar
    ON claude_verification (jar_id, claude_iteration DESC)
    WHERE superseded_by IS NULL;

-- Q66: chat session list
CREATE INDEX ix_chat_scope ON chat_session (scope, chat_type, updated_at DESC);
```

## 2.3 Coverage proof, per hot path

"Hot path" = anything in §1.1 (polled) or marked `Init? yes` in §1.2. For each, the plan a correct
planner must produce:

| Q | Plan | Rows touched | Proof it is not a scan |
|---|---|---|---|
| Q1 | `Index Only Scan using ix_session_running … Limit 1` | ≤ 1 | The index contains only `RUNNING` rows; when nothing runs it has **zero entries** and the probe is a root-page hit. |
| Q3 / Q9 / Q25 | `Index Scan using uq_run_current` | 1 | unique partial index; the equality covers all three key columns |
| Q7 | `Index Scan using ix_queue_state` ×3 (current / queued / history), each `LIMIT n` | ≤ `1 + queuedLimit + historyLimit` | `state` equality + `enqueued_at` ordering both come from the index; the `LIMIT 3` on history is applied **in the engine**, not in `plsql/js/claude.js:259-261` |
| Q8 / Q13 | `Index Scan using job_queue_entry_pkey` | 1 | PK |
| Q10 / Q11b / Q12 | `Index Scan using <pk>` with `seq > :since` | delta only | PK prefix equality + range on the trailing key column — the canonical covered range scan |
| Q11 | `Index Scan using conversion_job_pkey`, **projection excludes `report_ref`/`instruction_md_ref` bodies** | 1 | the blobs are refs, so they are physically not in the row |
| Q20 | `Index Only Scan using ix_jar_sidebar` | page | the `INCLUDE` columns make it index-only; the two-key sort is the index order |
| Q21 / Q22 / Q28 | `Index Scan using <pk>` | 1 | |
| Q23 / Q24 | `Index Scan` on the FK | 1 | |
| Q26 | `Index Scan using claude_call_log_rollup_pkey … = ANY(:scopes)` | N jars, **one round trip** | rollup is one row per scope (§2.4) |
| Q29 | `Index Scan using ix_flownode_page` | page | |
| Q31 | `Index Scan using ix_plsql_key_facets` → nested loop to `ix_plsql_facets` | page | both facet columns and the sort key are indexed |
| Q34 | `Index Scan using ix_conversion_started` | page | |
| Q63 | `Index Scan using ix_verification_current … Limit 1` | 1 | partial index excludes superseded iterations entirely |

## 2.4 Queries that cannot be served without a scan — and what changes

Four, and each one gets a concrete remedy.

**S1 — `callLogStats` (Q10b, Q32, I7, P11).** `SUM`/`COUNT` over `claude_call_log_entry` is a scan
of an append-only table that grows forever. It is polled every 5 s *and* fetched once per JAR on the
dashboard. No index fixes an aggregate over all rows.

*Change:* add a maintained rollup, upserted in the same transaction as W9.

```sql
CREATE TABLE claude_call_log_rollup (
    scope_kind      text   NOT NULL,
    scope_id        uuid   NOT NULL,
    last_seq        bigint NOT NULL,      -- also the ETag / cursor watermark for §5.3
    total_calls     bigint NOT NULL DEFAULT 0,
    session_count   integer NOT NULL DEFAULT 0,
    total_prompt_chars   bigint NOT NULL DEFAULT 0,
    total_response_chars bigint NOT NULL DEFAULT 0,
    total_duration_ms    bigint NOT NULL DEFAULT 0,
    error_count     bigint NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL,
    PRIMARY KEY (scope_kind, scope_id)
);
```

This also fixes PLAN-03 §4.3 #17's *statistically wrong* mean-of-means: `avgDurationMs` becomes
`total_duration_ms / total_calls` computed from true totals, not a client-side mean of per-JAR means
(`jar/js/dashboard.js:105`).

**S2 — `availableProcs` substring filter (Q55, L18).** `cross-ref.js:375-379` filters with an
unanchored substring. `ILIKE '%q%'` cannot use a b-tree.

*Change:* prefix-anchor the default (`name ILIKE :q || '%'`, served by `ix_resolver_name`), and add a
trigram index only if a runtime check shows users rely on infix matching:

```sql
-- only if infix search is required
CREATE INDEX ix_resolver_name_trgm ON resolver_object USING gin (name gin_trgm_ops);
```

> **OPEN QUESTION Q-B — is infix proc search actually used?** Options: (a) prefix-only, no extra
> index; (b) add the trigram index (≈ 3–5× the index size of the b-tree).
> **Recommendation: (a) first**, with a client-side hint ("starts with"). Ship (b) only on a
> complaint; the proc list is a few hundred rows per scope, so a scan is survivable in the interim.

**S3 — free-text log search (Q5's `q` parameter).** `message ILIKE '%q%'` over
`conversion_job_log_entry` / `flow_analysis_log_entry` is a scan.

*Change:* **always bound it by the cursor.** `logLines` requires either `sinceSeq` or a
`seq BETWEEN` window, so the text predicate is applied to a bounded range (the tail the user is
looking at), never to the whole log. The `level` filter is a cheap equality on a new
`level text` column (add it to `conversion_job_log_entry` and `flow_analysis_log_entry`; today the
level is re-derived from five regexes per line in the browser, `conversion/index.html:526-532`).
Full-history search is an explicitly separate, non-polled endpoint that may be slow.

**S4 — cross-analysis `endpointsUsingTable` with `Scope = AllAnalyses` (Q43).**
`ix_epcoll_name` starts with `collection_name`, so scoping to *one* analysis after the fact means
filtering the global match set. For a widely used collection this can be large.

*Change:* `ix_epcoll_name` stays as the global index (it is the headline cross-analysis query PLAN-00
calls out), and add the analysis-scoped variant:

```sql
CREATE INDEX ix_epcoll_scoped ON endpoint_collection (collection_name, analysis_id, endpoint_id);
```

`TableSelector.Scope` (PLAN-07 §2.2) already distinguishes `AllAnalyses` from `OneAnalysis`, so the
repository picks the index from the sealed type — no heuristics.

## 2.5 The MongoDB equivalents

`mongodb` mode uses the same shapes; PLAN-05's tables are collections and its indexes are compound
indexes in the same column order. The three that matter:

```js
// Q1 — the partial index that makes "is anything running?" free
db.claude_session.createIndex({ startedAt: -1 },
    { name: "ix_session_running", partialFilterExpression: { status: "RUNNING" } });

// Q10/Q11b/Q12/Q67 — every append-only cursor read
db.claude_call_log_entry.createIndex({ scopeKind: 1, scopeId: 1, seq: 1 }, { unique: true });
db.conversion_job_log_entry.createIndex({ jobId: 1, stream: 1, seq: 1 }, { unique: true });
db.chat_message.createIndex({ sessionId: 1, seq: -1 });

// Q43 — the headline reverse lookup
db.endpoint_collection.createIndex({ collectionName: 1, endpointId: 1 });
db.endpoint_collection.createIndex({ collectionName: 1, analysisId: 1, endpointId: 1 });
```

Coverage is asserted the same way: `explain("executionStats")` must report
`totalDocsExamined <= nReturned` for every §2.3 row, and `stage: "IXSCAN"` (never `COLLSCAN`) —
which is a test, not an inspection (§8.3).

---

# 3. Pagination design

## 3.1 Why cursors, not offsets — and the reason is specific to this data

The generic argument (`skip(n)` is O(n)) is true but secondary. The decisive reasons here:

1. **Ordinals are run-scoped and the crawl is non-deterministic.** PLAN-00 finding #5 and PLAN-02:
   re-running an analysis produces *different node numbering*, and `db_call_count` was observed as
   259 on one run and 263 on another. An offset is a promise that row *k* means the same thing on
   two requests. This data cannot keep that promise across a re-analysis, and a re-analysis can
   land between page 1 and page 2 of a user's browsing session. A cursor encodes *the last row I
   actually saw*, which stays meaningful (or fails loudly) instead of silently skipping or
   duplicating rows.
2. **The append-only tables move under the reader by design.** `claude_call_log_entry`,
   `conversion_job_log_entry`, `chat_message` all grow while a page is open — that is the point of
   polling them. Offset paging over a growing list shifts every row on every insert. A `seq` cursor
   is *defined* against a monotonic key and is immune.
3. **The scan implementation cannot skip cheaply either.** PLAN-07 §2.3's
   `ArtifactScanAnalysisRepository` serves `local` and `s3`. For it, a cursor can encode a *file
   name + byte offset*, which is O(1); an offset would mean re-reading everything before it. Cursor
   paging is the only design where both implementations are honest.
4. **The 500 MB facet.** PLAN-07 §2.1: `api/tables/index.json` extrapolates to ~500 MB. Offset
   paging over it is, in PLAN-07's words, "a performance trap that only shows up in production".

## 3.2 The wire contract

**Request** — three parameters, all optional, on any converted endpoint:

| Param | Type | Default | Meaning |
|---|---|---|---|
| `limit` | int, 1–1000 | endpoint-specific (25 for tables, 200 for trees, 200 for logs) | max items to return |
| `after` | opaque string | absent = start of the sort order | resume strictly after this cursor |
| `sort` | enum name | endpoint's natural order | one of a **closed, per-endpoint set** — never a free-form column name |

For append-only streams two aliases exist because they read better and because the UI already
speaks this language in PLAN-03's recommendations:

| Param | Equivalent to | Used by |
|---|---|---|
| `sinceSeq` | `after` = cursor encoding `{seq}` | Q10, Q11b, Q12 (forward tail) |
| `before` | `after` under `sort=SEQ_DESC` | Q67 (chat, backward scroll) |

**Response envelope** — one shape everywhere:

```json
{
  "items": [ ... ],
  "page": {
    "nextCursor": "eyJrIjpbIkNPUlJFQ1RFRCIsIjIwMjYtMDgtMTJUMTA6MTE6MDlaIl0sImkiOiJmM2E5In0",
    "hasMore": true,
    "total": 1743,
    "totalIsExact": true,
    "limit": 25,
    "sort": "SIDEBAR"
  }
}
```

- `nextCursor` is `null` iff `hasMore` is false.
- `total` is `-1` and `totalIsExact` is `false` when the implementation cannot count cheaply
  (PLAN-07 §2.2's rule, surfaced honestly to the UI — see OPEN QUESTION Q-A).
- Nothing else is added. No `page`, no `offset`, no `pageCount` — offering them would invite the UI
  to compute a cursor, which must never happen.

## 3.3 Cursor encoding

A cursor is `base64url(json)` of the **sort key values of the last returned row, plus a tiebreaker
primary key, plus a sort discriminator**:

```json
{ "s": "SIDEBAR", "k": ["CORRECTED", "2026-08-12T10:11:09Z"], "i": "f3a9…" , "v": 1 }
```

| Field | Purpose |
|---|---|
| `s` | the sort the cursor was minted under. A request whose `sort` differs → **HTTP 400**, never a silently wrong page. |
| `k` | the sort key tuple, so the resume predicate is a plain row comparison (`(mode, analyzed_at) < (…)`), which is exactly what the index supports |
| `i` | the row's stable primary key as the final tiebreaker — **never an ordinal** (PLAN-00 #5) |
| `v` | envelope version, so a cursor minted by an older deploy can be rejected cleanly |

Rules:

- **Opaque to the client.** The UI stores and returns it; it never parses or constructs one. The
  base64 is not security, it is a discouragement.
- **Not signed, not encrypted.** The contents are already-visible sort keys; signing would add key
  management for no threat. (If the deployment ever becomes multi-tenant, revisit — the scope is
  *not* in the cursor, it comes from the request path.)
- **Not a database cursor.** No server-side state, no timeout, no session affinity. A cursor is
  valid until the sort discriminator or envelope version changes.

## 3.4 Stability semantics — what a cursor does and does not promise

Stated explicitly because the crawl is non-deterministic and this is where silent corruption lives:

| Situation | Behaviour |
|---|---|
| Rows inserted **before** the cursor position | not returned. Correct for a log tail; for a `DESC`-sorted list the client must refetch page 1 to see them (the poll already does). |
| Rows inserted **after** the cursor position | returned normally |
| The row the cursor points at is **deleted** | paging continues from the key tuple, not from the row — deletion is harmless |
| A **re-analysis** replaces the whole result set mid-scroll | the cursor's `i` no longer exists and its `k` may be from a superseded run. The repository detects this via `analysis_id` mismatch and returns **HTTP 409 with `{"reason":"RESULT_SET_REPLACED"}`**; the UI resets to page 1 with a toast. This is the case offset paging would have silently corrupted. |
| `sort` changed by the user | client discards the cursor and starts over. Sending an old cursor with a new sort is a 400. |

## 3.5 One endpoint at a time

The contract is designed so that adoption is per endpoint and per screen, with no coordinated
deploy:

1. **Absent parameters mean today's behaviour.** An endpoint that has been converted but is called
   with no `limit`/`after` returns the full array in the legacy shape. Converted-but-not-yet-called
   is a no-op.
2. **`Accept` negotiation for the envelope.** A converted endpoint returns the bare array unless the
   caller opts in — either by sending any paging parameter, or by
   `Accept: application/vnd.analyzer.page+json`. So an old script and a new screen can hit the same
   URL on the same deploy.
3. **A hard cap with a warning, not a truncation.** The legacy full-array path gets a server-side
   cap (`app.api.max-unpaged-items`, default 10 000) and a `X-Analyzer-Truncated: true` header when
   it bites. This is the pressure valve that stops an unconverted screen from re-introducing a
   500 MB response after the schema change.
4. **Rollback is per endpoint.** Reverting one controller to the unpaged path does not touch any
   other, because the shim (§4.2) means the client tolerates both shapes indefinitely.

---

# 4. The UI migration

## 4.1 What has to change, mechanically

There are exactly two client-side pagers in the codebase, and every one of the 20+ tables goes
through one of them:

| Pager | File | Where the slice happens | Tables using it |
|---|---|---|---|
| JAR side | `jar/js/summary-helpers.js` — `_initPage(id, data, pageSize, …)` `:478-500`, slice at `:532`, size selector `[10,25,50,100]` at `:570` | `s.filtered.slice(start, start + s.pageSize)` | 12 (`summary-tables.js:40,610,633,1060,1145,1224`, `summary-aggregation.js:112`, `summary-dynamic.js:106`, `summary-claude.js:44`, `summary-vertical.js:131,257`) |
| Parser side | `parser/js/pagination.js:33-40` (`PA.pager.slice`) and `parser/js/tableFramework.js:33-56` (`PA.tf.init`) | same | ~10 (`tableOps.js:31`, `tableDetail.js:719,907,971,1033,1120,1192,1358,1445`, `complexity.js:263`, `claudeEnrichment.js:558`) |

**This is the leverage.** Both pagers take *a fully materialised array* and slice it. Teaching each
one to accept an optional **page source** — an async function `(cursor, limit) → {items, page}` —
converts every table that uses it, one call site at a time, with no change to the rendering code.

```js
// summary-helpers.js / pagination.js — the ONE change, made twice
// Legacy call (unchanged, still works):      _initPage(id, arrayOfRows, 25, renderFn)
// Paged call:                                _initPage(id, PA.pageSource(url), 25, renderFn)
PA.pageSource = function (url, params) {
    var cursor = null, exhausted = false;
    return {
        __pageSource: true,
        async fetchPage(limit) {
            if (exhausted) return { items: [], page: { hasMore: false } };
            var q = new URLSearchParams(params || {});
            q.set('limit', limit);
            if (cursor) q.set('after', cursor);
            var res = await fetch(url + '?' + q, {
                headers: { 'Accept': 'application/vnd.analyzer.page+json' } });
            if (!res.ok) throw new Error('page fetch ' + res.status);
            var body = await res.json();
            var env = PA.normalizePage(body);          // §4.2 — tolerates both shapes
            cursor = env.page.nextCursor;
            exhausted = !env.page.hasMore;
            return env;
        },
        reset() { cursor = null; exhausted = false; }
    };
};
```

Three consequences worth naming:

- **Search must move with pagination.** `_buildSearchText` (`summary-helpers.js:503-524`) flattens
  every scalar of every row into a lowercase string *at init*. Once only one page is in memory,
  client-side search silently searches one page. Every converted table therefore passes its search
  box through as a server-side `q` parameter in the same commit. A table converted without moving
  its search is a **regression**, and §8 tests for it.
- **Sort must move with pagination**, for the same reason, using the closed `sort` enum of §3.2.
- **Totals.** The pager's "Page 3 of 17" reads `page.total`; when `totalIsExact` is false it renders
  "Page 3" plus a next arrow (OPEN QUESTION Q-A).

## 4.2 The shim — an unconverted screen must not break

Every screen keeps working while its neighbours convert, because of one normaliser installed once,
before anything else:

```js
// shared/page-envelope.js — loaded on every page, ~20 lines
PA.normalizePage = function (body) {
    if (Array.isArray(body)) {                       // legacy: an unpaged endpoint
        return { items: body,
                 page: { nextCursor: null, hasMore: false,
                         total: body.length, totalIsExact: true, limit: body.length } };
    }
    if (body && Array.isArray(body.items) && body.page) return body;   // new envelope
    if (body && Array.isArray(body.items)) {         // half-converted: items, no page block
        return { items: body.items,
                 page: { nextCursor: null, hasMore: false,
                         total: body.items.length, totalIsExact: true } };
    }
    return { items: [], page: { nextCursor: null, hasMore: false, total: 0, totalIsExact: true } };
};
```

The four properties that make this safe:

1. **Both directions are covered.** An *unconverted client* hitting a *converted endpoint* sends no
   paging parameters and gets the legacy bare array (§3.5 rule 1) — it never sees the envelope. A
   *converted client* hitting an *unconverted endpoint* gets a bare array, which `normalizePage`
   wraps into a single exhausted page — so it renders correctly, just without server paging. There
   is no combination that breaks.
2. **Client-side slicing still works on a wrapped legacy array**, because the shim reports
   `hasMore: false` and hands the pager the complete array — exactly the input it takes today.
3. **`normalizePage` ships in step 0**, before any endpoint is converted. It is inert until it is
   needed, so it cannot be the cause of a regression.
4. **It is the only place that knows about two shapes.** No call site branches on response type.

## 4.3 Conversion order

Ranked by (traffic × bytes saved) ÷ effort. Each step is independently shippable and revertible.

| Step | Screen / table | PLAN-03 refs | Why here | Bytes / requests saved | Effort |
|---|---|---|---|---|---|
| **0** | `shared/page-envelope.js` + the two pager `pageSource` hooks + `PollConfig` fix (§5.2) | §1.5 #3, §3.0 | pure enablement; nothing user-visible changes | 0 (but unblocks all of it) | S |
| **1** | **"Is any session running?"** → `/api/sessions/running` boolean, then SSE | P1, P2, P4, §4.3 #1 | fires on **every open page every 30 s forever**, twice per tick in Corrections; the highest-frequency call in the suite and the smallest change | full session list × 2 → 1 boolean, ×N tabs | **XS** |
| **2** | **Conversion job poll** → Q11 + `sinceSeq` deltas | P13, §4.3 #2 | fastest poll (1.5 s) and the largest per-tick payload; also removes an O(n) DOM rebuild per tick | whole job (log + ≤2000 liveLines + immutable `instructionMd`) → ~6 scalars + delta | S |
| **3** | **Log viewers** → `?level=&q=&sinceSeq=&limit=200` | P5, P6, P12, §4.3 #3 | `?lines=10000` every 3–10 s is the single largest response in the app (multi-MB, measured source files 9.36 MB / 4.09 MB) | multi-MB → a bounded tail | S |
| **4** | **Claude Sessions modal** → Q10 delta + Q10b rollup at 30 s | P11, §4.3 #4 | the entire append-only call log re-fetched every 5 s to show seven totals | whole growing log → 7 scalars + 0–3 rows | S |
| **5** | **Queue** → Q7 server-side filter + bounded history, then the existing SSE | P7, P8, §4.3 #19 | 2–3 s polls of the whole global queue; `plsql/js/claude.js:259-261` ships the entire queue to render **≤3 rows** | whole queue → current + n + 3 | S |
| **6** | **PL/SQL topbar** — delete the `tables` fetch, read `validated_table_count` | I10, §4.3 #14 | **307 401 B for one integer**, on every analysis load. A deletion, not a conversion. | 307 KB → 0 | **XS** |
| **7** | **Claude verification screens** → Q63 (7 scalars) | L26, §4.3 #4, **§7** | a 138 KB document fetched by ≥5 screens to show 3–7 scalars; ships with the §7 broken-endpoint fix | 138 KB × 5 → ~80 B × 5 | S |
| **8** | **JAR summary header** → Q21 + paged Q21b, slices by `endpoint_id` (Q46) | I2, L7, §1.5 #4 | 430 KB on every JAR click; **and it closes the positional-coupling hazard** — the highest-severity latent bug in the UI | 430 KB → ~400 B + one page | **M** |
| **9** | **Summary-tab tables (12)** via the JAR pager | §3.6 | one hook, twelve call sites; needs server-side search+sort per table | large, proportional to endpoint count | M |
| **10** | **Table Detail modal** → Q59 / Q59b | L22, §4.3 #5 | replaces two N-node `Promise.all` fan-outs of ≤370 KB documents. High value, but the modal is 1 580 lines — the risk sits in the modal, not the query | N × 370 KB → 2 paged queries | **L** |
| **11** | **Collection popup** → Q43 | L4, §4.3 #6 | unbounded parallel call-tree fetch → one indexed query | N × ≤178 KB → one page | M |
| **12** | **Parser tables (~10)** via `PA.tf` / `PA.pager` | §3.9 | mechanical once step 0 lands | moderate | M |
| **13** | **Code Structure tab** → Q40 + Q41 | L1, L2 | `bundled-jars.json` is **3.03 MB**; the tree already lazy-renders, so only the fetch changes | 3.03 MB → a page | M |
| **14** | **Chat transcript** → Q67 backward paging | L30 | unbounded transcript, but **not polled** — real but lower-frequency | unbounded → 50 messages | M |
| **15** | **History / Performance Summary facets** → Q31 server-side | I11, §3.9 | removes whole-history recomputation per keystroke; also drops `errors[]` in favour of a count | moderate | M |
| **16** | **Cross-ref result** → Q52 | L15 | `_reloadResultAndRefresh` refetches the whole result after **every** Claude completion | whole `result.json` → a page | M |
| **17** | **Call-graph viz** → Q60 | L23, §4.3 #11 | server-side node↔edge join; needs viewport-driven fetching to be worth it | 84 KB → viewport | L |

> ### MEASURED CORRECTION — step 9 is a regression at current analysis sizes. Do not do it.
>
> Step 9 was planned on the assumption that paging the twelve summary tables would shrink the JAR
> click. Measured on real data (`web-1.0.0-SNAPSHOT`, 84 endpoints), it does the opposite:
>
> | | bytes |
> |---|---|
> | the single source fetch (`/summary/headers`) that produces **all twelve** tables | **120,272** |
> | twelve paged first pages (25 rows each) | **214,167** |
>
> A **1.78× regression**, plus twelve extra round trips. `sum-ep` + `sum-claude` alone (50 rows)
> is 135,376 B — already more than the whole source. The bytes live *inside* each derived row, so
> bounding the row count barely moves them. This is the same shape as the `joins` result (438 KB
> for twelve rows).
>
> Two further reasons it is not the change it looks like:
>
> - **It cannot ship one table at a time.** The source fetch can only be dropped when *all twelve*
>   move, which contradicts this section's "independently shippable" premise.
> - **Ten of the twelve aggregate over the whole endpoint set** (`sum-vertc`/`sum-vertm` rows carry
>   sets whose `.size` is a sort key), so a page of endpoints cannot produce a page of rows. The
>   server would need a Java port of the ~550-line client report derivation — a rewrite, not a
>   conversion.
>
> **Crossover:** the source costs ~1.43 KB/endpoint while pages are fixed, so above roughly
> **150 endpoints** converting all twelve *together* would start to win. Below that it loses. Revisit
> only with a measurement on an analysis that large.
>
> Consequence for step 8: its endpoint-list byte win does not materialise, and a JAR click stays at
> ~120 KB. What step 8 did deliver stands on its own — the header is 410 B with a 304, `/versions` is
> folded in, and the positional-coupling hazard is closed.

Steps 1–7 are the ones that produce a measurable improvement on their own. Steps 8+ are where the
byte counts are large but the per-user frequency is lower.

> **OPEN QUESTION Q-C — does the Code Structure tree need a virtual scroller?**
> Its header comment (`jar/js/codeTree.js:1-7`) targets 8 000+ classes and it already memoises
> rendered bodies, but the whole array lives in `_classes` (`:12`).
> Options: (a) paged fetch + "load more" at the package level; (b) a real virtual scroller.
> **Recommendation: (a).** The tree is already collapsed by package, so a page boundary is
> invisible; a virtual scroller is a much larger change for a tab that is not on the hot path.

---

# 5. The polling problem

PLAN-03 §3.0 found 17 polled patterns and a configuration mechanism that does not work. The fixes
split into four techniques; every polled path gets exactly one.

## 5.1 The decision, per polled path

| P | What it polls | Today | Decision | Why this and not something else |
|---|---|---|---|---|
| P1, P2 | session list (global) | 30 s, forever, **every open page** | **SSE** (`sessionState` event) + a boolean fallback endpoint | It is a state *change* notification, not data. It fires forever even when nothing is running — the only fix that reaches zero traffic at idle is push. Fallback: `GET /api/sessions/running` → `{"running":false}` for browsers without SSE. |
| P3 | Claude progress (JAR), **two independent timers** | 30 s | **SSE** — reuse `claude/progress/stream` | The pattern already exists (`parser/js/claudeEnrichment.js:98-119`, `plsql/js/api.js:355-363`) with `progress`/`complete`/`error` events. Also: **collapse the two timers into one subscription**, which halves the traffic before SSE even helps. |
| P4 | corrections + a second session request per tick | 30 s × 2 requests | **`If-None-Match`/304** for the corrections body; the session request is **deleted** (subsumed by P1's SSE) | Corrections change only on a Claude write, so 304 is almost always the answer. The second request exists purely to evaluate one boolean — step 1 of §4.3 removes it outright. |
| P5, P6, P12 | log tail, `lines=10000` / `lines=50` | 3–10 s | **`seq` cursor** (`?sinceSeq=&level=&q=&limit=200`) | Logs are append-only and the client already only ever appends. SSE is tempting but a log can burst faster than a browser renders; a cursor lets the client set its own pace and survive a disconnect by replaying from `sinceSeq`. Server-side `level`/`q` also removes the per-keystroke JS filter. |
| P7, P8 | global queue | 3 s / 2 s | **SSE** — `/api/queue/events` **already exists** | It is already there and already consumed by `plsql/js/claude.js:302`. Also fix defect 4: `claude.js:307` re-fetches the whole `/api/queue` on any event lacking `state`, amplifying a job burst into a fetch burst — the event must carry the state, and the client must never fetch in response to an event. |
| P9, P16 | one queue job | 2 s / 10 s | **keep polling, at `jobStatusMs`** — it is already the SSE fallback | A single row by PK is genuinely cheap (Q8). The bug is only the hard-coded 2 s. |
| P10, P15 | Claude verify progress | **2 s hard-coded** (config says 30 s) | **SSE** — the stream exists for this exact screen | `parser/js/claudeEnrichment.js:98-119` already subscribes to `claude/progress/stream`; `claudeInsights.js:164` polls the same data at 2 s beside it. Subscribe once, share. |
| P11 | Claude call log + session list | **5 s**, 2 requests/tick | **`seq` cursor** for the log delta + **rollup at 30 s with ETag** for the totals | The log is append-only, so a delta is exact. The seven totals come from `claude_call_log_rollup` (§2.4), so they are a single row — not an aggregate over a growing table. |
| P13 | whole conversion job | **1.5 s** | **Q11 scalars + `sinceSeq` deltas on both streams** | The immutable `instructionMd` and the full `log[]`/`liveLines[]` must simply stop being in the response — `conversion_job` stores them as refs (PLAN-05 §1.8), so this is a projection change. SSE is a reasonable follow-up but the payload fix is the whole win. |
| P14 | flow-run status + log | 1.5 s chained `setTimeout` | **`sinceSeq` for the log** now, **SSE** later | Same shape as P13. Note it also fetches the whole `results.json` on completion — that becomes a paged read. |
| P17 | polling config | once | **fix it** (§5.2) | |

## 5.2 Fixing `PollConfig` — three defects, three fixes

All three must land in step 0 of §4.3, because every other polling change depends on the interval
actually being configurable.

**Defect 1 — `load()` is never awaited** (`shared/poll-config.js:25`). Timers registered during
initial script execution capture the hard-coded defaults.

*Fix:* expose the promise and make every consumer await it. Ten lines:

```js
var PollConfig = {
    sessionPollMs: 30000, claudeProgressMs: 30000, jobStatusMs: 10000,
    logRefreshMs: 10000, correctionMs: 30000,
    _ready: null,
    ready() { return this._ready || (this._ready = this._load()); },
    async _load() { /* …existing body… */ return this; },
    /** The only correct way to start a timer. Awaits config, then schedules. */
    async every(key, fn) {
        await this.ready();
        fn();                                    // fire once immediately, as today
        return setInterval(fn, this[key]);
    }
};
PollConfig.ready();                              // start the fetch, do not block
```

Every `setInterval(fn, 2000)` becomes `await PollConfig.every('claudeProgressMs', fn)`. That single
call site shape makes defect 2 grep-detectable.

**Defect 2 — nothing under `parser/**` uses `PollConfig`.** Every parser interval is hard-coded and
every one is faster than configured: 2 s vs 30 s (`claudeInsights.js:164`), 5 s vs 30 s
(`claudeSessions.js:35`), 3 s vs 10 s (`logViewer.js:105`), 2 s vs 10 s (`perfSummary.js:416`,
`history.js:364,373`). `conversion/index.html:398` is 1.5 s with no config at all.

*Fix:* convert all six to `PollConfig.every(...)`, and add a lint gate:

```bash
# fails the build: no literal-ms timers in polling code
grep -rnE 'set(Interval|Timeout)\([^,]+,\s*[0-9]{3,}\)' \
     unified-web/src/main/resources/static/{jar,parser,plsql,conversion,shared}/ \
  | grep -v 'poll-config.js' | grep -v '// poll-exempt'
```

**Defect 3 — `parser/index.html:727-728` loads `shared/log-fab.js` without `shared/poll-config.js`**,
so `log-fab.js:137` throws `ReferenceError: PollConfig is not defined` and the FAB's Auto checkbox
is dead. `conversion/index.html` loads neither.

*Fix:* add the `<script src="/shared/poll-config.js">` tag to `parser/index.html` and
`conversion/index.html` **before** all app scripts — `plsql/index.html:709` is the one page that
already gets the ordering right, so copy it. Then add a smoke assertion: every page that loads
`log-fab.js` must also load `poll-config.js` (a five-line HTML grep test).

Also: `PollConfig.jobStatusMs` is fetched and defined but has **zero readers on the JAR side** —
after this change P9's `jar/js/app.js:419` and `targeted.js:224` become its first consumers.

## 5.3 The `seq`-cursor contract for append-only streams

Applies to P5/P6/P12 (application logs), P11 (Claude call log), P13 (conversion), P14 (flow).

```
GET /api/{scope}/log?sinceSeq=1843&limit=200&level=WARN&q=timeout
→ 200 { "items":[{"seq":1844,"ts":"…","level":"INFO","message":"…"}, …],
        "page": { "nextCursor": null, "hasMore": false, "total": -1,
                  "totalIsExact": false, "limit": 200 },
        "watermark": 1861 }
```

- `watermark` is the highest `seq` that exists, so the client can render "17 new lines" without
  fetching them, and can detect that it fell behind.
- `sinceSeq` absent = **the tail**, i.e. the last `limit` lines in `seq DESC`, reversed — that is
  today's `?lines=N` behaviour, preserved for the first paint.
- If `sinceSeq` is older than a retained floor (rotated logs), the server returns
  `{"reset": true, "fromSeq": …}` and the client clears its buffer. Never a silent gap.
- The client **appends** to the DOM instead of `innerHTML = …`. This is the half of the P13 fix that
  is not about bytes: `conversion/index.html:516-534` rebuilds the entire log and live-output DOM
  every 1.5 s and re-runs five regexes per line. With deltas it appends *k* nodes, and the `level`
  now arrives as a column so the regexes disappear entirely.

## 5.4 The `If-None-Match` contract

Applies to P4 (corrections) and to any snapshot that changes rarely but is polled: Q10b's rollup,
Q22's version flags, Q63's verification summary.

- Every snapshot record implements `Etagged` (§1.5). The ETag is a cheap monotonic value already in
  the schema — `analysis_run.finished_at` + `claude_iteration` for verification,
  `claude_call_log_rollup.last_seq` for the call-log stats, `endpoint_correction` max `updated_at`
  for corrections. **Never a hash of the body**, which would mean computing the body to decide not
  to send it.
- Response carries `ETag:` and `Cache-Control: no-cache` (revalidate always, never serve stale).
- Client sends `If-None-Match:`; a 304 is ~200 bytes and the render is skipped entirely.
- This is deliberately the *cheapest* of the four techniques and the right default for anything not
  worth an SSE channel.

## 5.5 The SSE pattern to reuse

`POST .../claude/run` with `Accept: text/event-stream` consumed by `fetch` + `ReadableStream`
(`jar/js/cross-ref.js:1000-1030`) and `EventSource /api/parser/analyses/{name}/claude/progress/stream`
(`parser/js/claudeEnrichment.js:98-119`) already work, with `progress` / `complete` / `error` events
and a 3-error auto-close (`plsql/js/api.js:355-363`). Standardise on that shape:

```js
// shared/sse.js — one implementation, replacing three hand-rolled ones
PA.subscribe = function (url, handlers) { /* EventSource + 3-error auto-close + poll fallback */ };
```

Rules, all of which the existing code already implies:

1. **One subscription per page per stream**, shared by every consumer. P3's *two independent timers*
   for the same data become one subscription with two listeners.
2. **Events carry state; they never mean "go fetch".** This is defect 4 stated as a rule
   (`plsql/js/claude.js:307`).
3. **Every SSE consumer keeps a polling fallback** at the *configured* interval, activated on
   3 consecutive errors. The fallback is the existing endpoint — which is why P9 keeps polling and
   why nothing regresses if SSE is unavailable behind a proxy.
4. **Delete the dead `EventSource`** at `jar/js/cross-ref.js:995-997` (constructed and immediately
   `.close()`d) as part of this work — defect 5.

---

# 6. Precomputation

PLAN-03 §4.3 lists 21 client-derived fields. Each becomes one of three things: a **column** (stored,
maintained on write), a **lightweight endpoint** (computed on read from an index, no document
fetched), or **stays client-side** (with a reason).

## 6.1 The disposition table

| # | Derived value | PLAN-03 cite | Becomes | Concretely |
|---|---|---|---|---|
| 1 | **"Is any session running?"** | `jar/js/app.js:86`, `summary-corrections.js:405` | **lightweight endpoint** (then SSE) | `GET /api/sessions/running → {"running":false}`, backed by `ix_session_running`. **Not** a column: it is a global predicate over a table, and a column would need invalidation on every session transition. This is the headline item — a full session list every 30 s forever, on every open page, twice per tick in Corrections, for one boolean. |
| 2 | Conversion log / live-output tail | `conversion/index.html:404,516-534` | **rows + cursor** | `conversion_job_log_entry (job_id, stream, seq)` (PLAN-05 §1.8) + §5.3 |
| 3 | Log level/text filtering | `logviewer.js:97-103`, `logViewer.js:91-97`, `log-fab.js:123-129` | **column + query params** | add `level` to the log-entry tables; `?level=&q=&sinceSeq=` |
| 4 | CONFIRMED/REMOVED/NEW counts | `claudeInsights.js:213-219`, `chunkViewer.js:62-74,226-233`, `claudeEnrichment.js:288-321,518-540` | **columns** (they already exist) | `claude_verification.{confirmed,removed,new,call_confirmed,call_removed,call_new}_count`. The aggregation is reimplemented **four times** over a document that already carries the totals — delete all four, read Q63. Per-table counts become columns on `claude_table_verification` too, since `_renderTableCard` re-counts per table. |
| 5 | Per-table joins and column references | `tableDetail.js:294-296,439-441` | **queries** | Q59 / Q59b |
| 6 | "Which call paths touch collection C" | `summary-popups.js:382-389` | **query** | Q43, via `ix_epcoll_name` |
| 7 | Summary headline aggregates (10 stats) | `summary.js:236-255,286,403-420` | **columns on `jar_analysis`** + a filtered-aggregate endpoint | The 10 unfiltered stats become columns computed at analysis time. Recomputing all ten on **every filter change** becomes `GET .../summary/stats?<filter>` returning 10 ints from indexed aggregates. The `requestAnimationFrame` batching with a progress bar (`:187-212`) is deleted — it is the smell that proves the work does not belong in the browser. |
| 8 | `entityCollMap` / `repoCollMap` | `summary.js:120-131,134-169` | **columns (mandatory)** | The server-supplied variants already exist as an opt-in (`:120,134`). Make them non-null and **delete the client fallbacks**, including the O(repos × entities) name-stem join. |
| 9 | PL/SQL dashboard node stats | `parser/js/summary.js:28+`, `statements.js:14-59`, `perfSummary.js:77-95` | **columns + a scoped-aggregate endpoint** | `flow_node.count_*` and `subtree_*_count` already exist (PLAN-05 §4.2, 12 counters). Scope-filtered rollups become `GET .../summary/stats?scope=`; `flow_node_statement_summary` already backs the statement-type rollup. |
| 10 | DML writes by procedure/node | `txnTrace.js:49-76` | **query** | `db_object_usage` filtered to DML operations, using `ix_usage_op (operation)` + `ix_usage_node`. Replaces a triple-nested iteration over the 307 KB tables document. |
| 11 | Call-graph node degrees and node↔edge join | `callGraphViz.js:41-80` | **columns + one query** | `in_edge_count` / `out_edge_count` as columns on `flow_node`; Q60 returns edges already joined to node attributes, so the browser stops synthesising placeholders for missing endpoints. |
| 12 | Complexity score and risk band | `complexity.js:117-136` | **column, client path retained** | Store the default-weight score and band as columns on `flow_node`. Keep the JS recompute **only** for live weight tuning — that is a genuine interactive feature, and `localStorage` per-analysis overrides stay. |
| 13 | `dbType` from `jdbcUrl` | `history.js:101-109` ≡ `perfSummary.js:13-22` | **column** | `plsql_analysis_key.db_type` already exists in PLAN-05 and is part of the identity. Delete both duplicated substring scanners. |
| 14 | Validated table count | `parser/js/app.js:74-81` | **column** | `plsql_analysis.validated_table_count` — already in PLAN-05, annotated *"today the UI pulls 307,401 B to set this int"*. §4.3 step 6. |
| 15 | Catalog verification status | `summary-tables.js:691-724` | **column** | `verified` on the collection/table row, set at analysis time. Removes the lowercase-`Set` join against every row in the browser and the conditional `/catalog` fetch. |
| 16 | Duplicate-JAR grouping and CORRECTED-first sort | `sidebar.js:44-67` | **column + index-ordered sort** | Persist the de-timestamped grouping key as a column on `jar_artifact` (the 13-digit-suffix strip becomes a write-time normalisation, consistent with `JarNameUtil.normalizeKey`); the two-key sort becomes `ix_jar_sidebar` (§2.2). |
| 17 | Cross-JAR Claude rollup | `dashboard.js:29-37,84-105` | **rollup table + batch endpoint** | `claude_call_log_rollup` (§2.4) + Q26. Kills the one-request-per-JAR `Promise.allSettled`, **and** fixes the statistically wrong mean-of-means at `:105`. |
| 18 | Collection detection by regex | `callGraph.js:170-212` | **column** | `call_node_collection` / `endpoint_collection` already hold what `_detectCollections` regex-scans for (`SB_*`, `*_VI_*`, `*_V_*`). The server already computes the equivalent for the Summary tab; delete the browser copy. |
| 19 | Session/queue counts and filters | `sessions.js:109-112`, `perfSummary.js:430-446`, `plsql/js/claude.js:259-261`, `cross-ref.js:361` | **query params + counts** | All four are `WHERE … COUNT(*)` or `WHERE kind IN (…) LIMIT 3`. Q7 takes `kind`, `state`, `historyLimit`; Q2 returns status counts; Q55 returns the `analyzed` count as a field. |
| 20 | Analyzed-endpoint membership Set | `cross-ref.js:108,111-113` | **lightweight endpoint** | Q56b `analyzedEndpointPaths(AnalysisId)` — a string array, instead of a whole flow-run-history document fetched for a membership test. |
| 21 | Markdown-fence stripping on chunk output | `chunkViewer.js:199-206` | **column** | Store parsed JSON **and** the raw text (PLAN-03 §2.2.7's "artifact-level lie"). `claude_chunk` gets `output_json jsonb` alongside `output_ref`. The client stops parsing a fenced blob that has a `.json` extension. |

## 6.2 Columns vs endpoints — the rule applied

- **A column** when the value is a function of one row's own analysis and changes only when that
  analysis is rewritten: #4, #7 (unfiltered), #8, #9 (per node), #11, #12, #13, #14, #15, #16, #18, #21.
  These are computed once at write time and are free forever after.
- **A lightweight endpoint** when the value is a predicate or aggregate over *many* rows and changes
  outside any single analysis's lifecycle: #1, #19, #20, #7/#9 in their *filtered* forms. A column
  here would need invalidation on every unrelated write.
- **A rollup table** for #17 and the call-log stats — an aggregate over an append-only stream, which
  is neither cheap to compute on read nor derivable from a single row. Maintained transactionally
  with the append (§2.4, W9).
- **Stays client-side:** #12's live weight tuning only, and nothing else.

## 6.3 Two small deletions worth naming

- `parser/index.html:806-819` and `plsql/index.html:748-757` unconditionally download **both**
  `chat.js` and `chatbox.js` plus both CSS files — ~1 200 lines of dead JS per page load — even
  though `GET /api/chat/config` (I8/Q27) decides which one is used. Inline the two booleans into the
  page and load one. This removes a request *and* a payload from every page in the app.
- `parser/index.html` still ships `statements.js` and `exHandlers.js` for tabs commented out at
  `:246-247` — dead payload.

Also delete the dead API methods PLAN-03 found while auditing: `getCorrectionForEndpoint`
(`jar/js/api.js:225`), `getAnalysis` (`:29`), `claudeRescan` (`:157`), `getClaudeStatus` (`:128`).
`getAnalysis` matters especially — it is the only client-side route to the whole `analysis.json`
(3.9–5.8 MB measured) and it has zero callers. Deleting it makes `LazyAnalysisResult`'s mitigation
(PLAN-03 §4.1) permanent rather than accidental.

---

# 7. The broken screen — Claude Verification grid

## 7.1 The defect

`parser/js/perfSummary.js:366` — the Performance Summary detail modal:

```js
const res = await fetch(`/api/analyses/${name}/claude/result`);
```

**No controller maps `/api/analyses`.** The controllers under `unified-web` map `/api/analysis`,
`/api/parser` and `/api/plsql`. The correct path is
`/api/parser/analyses/{name}/claude/result`, which is exactly what `parser/js/api.js:185` already
uses.

So the call has **always** 404'd. The catch branch at `perfSummary.js:371` renders "No verification
data", which is indistinguishable from a genuinely unverified analysis — which is why nobody
noticed. The grid is intended to show seven scalars: `confirmedCount`, `removedCount`, `newCount`,
`totalChunks`, `errorChunks`, `claudeCallCount`, `claudeTimeMs`.

This is PLAN-03 §5 defect 1, and it is the clearest instance of PLAN-03 §4.2's broader pattern:
**HTTP status used as application state**, so a wiring bug and an empty result look the same.

## 7.2 The fix

Three parts. The first is one line; the other two stop it recurring.

**(a) Correct the path — and route it through the shared API layer.** Do not just fix the string;
the reason the string was wrong is that this one call site bypassed `parser/js/api.js`, which is the
only file that knows the URL prefixes.

```js
// parser/js/perfSummary.js:366 — before
const res = await fetch(`/api/analyses/${name}/claude/result`);

// after — the same call every other screen makes
const result = await PA.api.claudeResult(name);        // parser/js/api.js:185
```

**(b) Make it the cheap query while we are here.** Five screens fetch this **138 301 B** document to
render 3–7 scalars (L26). Point the fixed call at Q63 instead:

```
GET /api/parser/analyses/{name}/claude/verification-summary
→ 200 { "status":"COMPLETE", "confirmedCount":438, "removedCount":0, "newCount":7,
        "totalChunks":25, "errorChunks":0, "claudeCallCount":25, "claudeTimeMs":312569,
        "verifiedAt":"…" }
→ 204 No Content        // genuinely never verified — NOT a 404
```

Backed by `ix_verification_current` (§2.2): a single indexed row, ~80 bytes on the wire, instead of
138 KB. This ships as §4.3 step 7.

**(c) Distinguish "absent" from "broken", permanently.** `204 No Content` means *this analysis has
no verification*; `404` means *you asked for something that does not exist* and must surface as an
error, not as an empty grid:

```js
if (res.status === 204) { renderEmpty('Not yet verified'); return; }
if (!res.ok) { renderError('Verification unavailable (' + res.status + ')'); console.error(...); return; }
```

**(d) A test that would have caught it.** Every fetch URL in the static assets must resolve to a
mapped controller route — a build-time check, not a runtime hope:

```bash
# extract every literal /api/... path from the JS and assert each has a @RequestMapping prefix
grep -rhoE "'/api/[a-z-]+" unified-web/src/main/resources/static/ | sort -u \
  | while read -r p; do grep -rq "${p#\'}" unified-web/src/main/java/ || echo "UNMAPPED: $p"; done
```

Run it in CI. It is cheap, it finds the whole class of defect, and PLAN-03 §4.2 shows there are
almost certainly more of them hiding behind graceful-degradation catch blocks — `promptManager.js`
states 404-tolerance as *design intent* (`:6-15`), which means a genuine typo there is invisible
too.

---

# 8. Acceptance — how we prove C3

C3 (PLAN-00): *every screen keeps working, at equal or better latency; PLAN-03's access-pattern table
is the acceptance criterion.* Three modes: `local`, `s3`, `mongodb`.

## 8.1 The rule

For every screen: **same rendered content, no new console errors, and p95 time-to-interactive no
worse than the `local`-mode baseline measured before the migration.** "Equal or better" is measured,
not asserted — which means the baseline must be captured **before** step 0 of §4.3 lands.

## 8.2 Per-screen measurable checks

| Screen | PLAN-03 | Measurable check | Baseline to beat |
|---|---|---|---|
| JAR sidebar / boot | §3.1, I1 | first paint of the JAR list; sidebar order identical to baseline (CORRECTED before STATIC, then `analyzedAt` desc) — **diffed as a list of ids**, since the sort moved to the server | current p95 |
| JAR dashboard | §3.2, I7 | **request count on load = 1** for Claude stats (was N, one per JAR); the sessions table renders the same rows in the same order; `avgDurationMs` differs from baseline **by design** (mean-of-means fixed) — assert against the true aggregate, not the old value | N requests → 1 |
| JAR analysis header | §3.3, I2–I6 | **request count on JAR click ≤ 3** (was 6, with `connections` and `domain-config` each fetched twice); header bytes < 2 KB (was **429 782 B**) | 430 KB → < 2 KB |
| Summary tab | §3.6, L7 | ten headline aggregates equal the baseline values exactly; **the `requestAnimationFrame` progress bar never appears** (the work is gone); slices join by `endpoint_id` — assert a slice/endpoint mismatch is impossible by shuffling the endpoint page order in a test and checking the render is unchanged | positional-coupling test must pass |
| Summary tables (12) | §3.6 | for each: page 1 content identical to baseline page 1; **search and sort still work** (server-side) — the specific regression §4.1 warns about; `total` matches baseline array length where exact | per-table |
| Code Structure | §3.4, L1/L2 | tree contents identical; bytes on tab open < 200 KB (was **3 028 400 B** for `bundled-jars.json` alone) | 3.03 MB → < 200 KB |
| Endpoint Flows | §3.5, L3 | call tree renders identically for the largest endpoint (**178 078 B** measured) | equal or better |
| Claude Insights / Corrections | §3.7, P3/P4 | requests per poll tick: **1** (was 2); a tick with no change is a **304** | 2 → 1, mostly 304 |
| Flow Analysis | §3.8, P14 | per-tick payload is a log delta, not the whole status + `results.json`; steady-state tick < 2 KB | 1.5 s × whole doc |
| Cross-Reference | §3.8, L14–L19 | result grid identical; the analyzed-endpoint green tags identical (Q56b vs the whole-history fetch); no dead `EventSource` in the console | equal |
| PL/SQL home / history | §3.9, I11/I12 | facet filtering returns the same rows with the filter applied server-side; `errors[]` no longer on the wire, only the count | equal |
| PL/SQL analysis load | §3.9, I9/I10 | **the `tables` request is gone**; topbar `totalTables` still correct from `validated_table_count` | 307 401 B → 0 |
| Table Ops / Table Detail | §3.9, L20/L22 | modal opens with **2 queries**, not two N-node `Promise.all` fan-outs; join and column lists identical to baseline | N × 370 KB → 2 pages |
| Claude Sessions modal | §3.9, P11 | steady-state tick payload < 1 KB (was the entire append-only call log); the seven totals equal a fresh aggregate over the log — **assert rollup ≡ recomputed sum** | whole log → < 1 KB |
| Chunk Viewer | §3.9, L27/L28 | identical; output no longer fence-stripped client-side | equal |
| Complexity | §3.9 | default-weight scores from the column equal the JS computation **exactly** — a golden-value test over a real analysis | bit-identical |
| Call Graph viz | §3.9, L23 | same nodes, same edges, same `inEdges`/`outEdges`; no synthesised placeholders (every edge endpoint resolves) | equal |
| **Performance Summary** | §3.9, **§7** | **the Claude Verification grid shows the seven scalars** — it has never rendered them. This is the one screen where "same as baseline" is the wrong test: the baseline is broken. Assert the seven values against `_result.json` for a known analysis. | broken → correct |
| Log viewers (3) | P5/P6/P12 | steady-state tick < 20 KB (was up to 10 000 lines); level/text filter returns the same lines as the old client-side filter over the same window | multi-MB → < 20 KB |
| Conversion | §3.11, P13 | steady-state tick < 2 KB; `instructionMd` appears **once**, not every 1.5 s; DOM node count grows by the delta, not rebuilt (assert via a MutationObserver count) | whole job → < 2 KB |
| Chat | §3.12, L30 | last 50 messages render on open; scroll-back loads older pages; a 1 000-message session opens in the same time as a 20-message one | O(n) → O(1) |
| Prompt Manager | §3.9, L32 | unchanged (small documents; no conversion needed) | equal |

## 8.3 The gates

1. **Three-mode screen matrix.** Every row of §8.2 runs against `local`, `s3` and `mongodb`, with
   the same seeded dataset. `AnalysisRepository.indexed()` differs (`false` for local/s3, `true` for
   mongodb) but **rendered output must be byte-identical**; only latency may differ. This is the
   direct test of C3 and of PLAN-07 §2.3's "no mode loses functionality".
2. **No `COLLSCAN` / no `Seq Scan` on a hot path.** For every §2.3 row, assert the plan. In Mongo,
   `explain("executionStats").stage == "IXSCAN"` and `totalDocsExamined <= nReturned * 1.1`. In
   Postgres, `EXPLAIN` output must not contain `Seq Scan` for those queries. Automated, run against
   a dataset large enough that the planner would prefer a scan on a small table — this is the
   standard way such a test lies.
3. **Payload budgets.** A test asserts a maximum response size per endpoint on the hot paths
   (the numbers in §8.2's last two columns). It fails on regression, which is the only durable
   defence against the next 307 KB-for-one-integer.
4. **Request-count budgets.** Per screen load: a headless run counts XHRs. JAR click ≤ 3, dashboard
   Claude stats = 1, poll tick = 1.
5. **The unmapped-URL check** (§7.2d) in CI.
6. **The hard-coded-interval lint** (§5.2 defect 2) in CI.
7. **The unpaged cap.** With `app.api.max-unpaged-items` set to 10, every unconverted screen must
   still render (truncated, with the `X-Analyzer-Truncated` header) rather than error. This proves
   the shim (§4.2) genuinely tolerates both shapes rather than tolerating them by luck.
8. **The 16 MB assertion** (PLAN-00 gate 4) and **`/storage-check` green** (gate 1) are unaffected by
   this document but are re-run, because §2.4's rollup table and the new `level` columns are schema
   changes.

## 8.4 What "equal or better latency" means when a screen gets slower

One honest case: a screen converted to server paging makes *more* requests over a full browsing
session (one per page turn) than the single fat request it replaces. Time-to-interactive improves;
total bytes over a long session may not, if the user pages through everything.

The acceptance criterion is **p95 time-to-interactive on first paint**, plus **p95 page-turn
latency < 200 ms**. A user who pages through all 84 endpoints does more round trips than before, and
that is the correct trade: the common case is looking at page 1.

> **OPEN QUESTION Q-D — prefetch the next page?**
> Options: (a) no prefetch; (b) prefetch page *n+1* when page *n* renders.
> **Recommendation: (a) initially.** Prefetching restores some of the traffic we are removing and
> hides paging bugs behind a warm cache. Revisit if page-turn p95 exceeds 200 ms.

---

# 9. Dependencies and open questions

## 9.1 What this document depends on

| Depends on | For |
|---|---|
| PLAN-05 §1.8 (append-only as one row per entry) | every `seq` cursor in §5.3. Without it, P11/P13/P14 cannot be fixed at all. |
| PLAN-05 partial indexes `ix_session_running`, `ix_run_running` | §6.1 #1, the highest-value single fix |
| PLAN-05 `endpoint.endpoint_id` as a stable uuid | §1.3 Q46 and the positional-coupling fix in §8.2 |
| PLAN-05 `plsql_analysis.validated_table_count`, `jar_analysis.claude_enriched` | §4.3 step 6 and §6.1 #14/#15 |
| PLAN-06 (chunking, append-only family) | the row-per-entry decision the cursors rest on |
| PLAN-07 §2.2 `AnalysisRepository`, `Page`, `PageRequest` | §1.5 extends it; §3 is its wire form |
| PLAN-07 §2.3 `ArtifactScanAnalysisRepository` | §3.1 reason 3 — cursors must work for local/s3 too |
| PLAN-09 (migration) | the baseline capture in §8.1 must happen before any cutover |

## 9.2 Open questions, collected

| # | Question | Options | Recommendation |
|---|---|---|---|
| Q-A | Does `Page.total` stay `-1` for the scan implementation, and what does the pager render? | (a) render "Page 3, more →"; (b) count by doing the work twice, cached | **(a)**, plus a `totalIsExact` flag |
| Q-B | Is infix (not prefix) proc search actually used? | (a) prefix-only; (b) add a trigram index | **(a)** first; ship (b) on a complaint |
| Q-C | Does the Code Structure tree need a virtual scroller? | (a) package-level paged fetch; (b) virtual scroller | **(a)** — the tree is already collapsed by package |
| Q-D | Prefetch the next page? | (a) no; (b) prefetch *n+1* | **(a)** initially |
| Q-E | Should SSE replace polling for P13/P14, or is the payload fix enough? | (a) payload fix only; (b) payload fix then SSE | **(a) first.** The payload fix is ~95 % of the win and is testable in isolation; SSE adds proxy-compatibility risk for a 1.5 s poll of ~2 KB. Revisit after §8's measurements. |
| Q-F | Does the legacy `plsql/**` UI get converted, or frozen? | (a) convert both URL spaces; (b) freeze `plsql/**` on the unpaged shim and convert only `parser/**` | **(b).** PLAN-03 §3.10 documents a **dual-backend switch** — two URL spaces for one entity — and converting both doubles the work for a UI that is already superseded. The shim (§4.2) keeps it working indefinitely. Needs a product decision, not a technical one. |
| Q-G | Where does the `level` column on log entries come from for *existing* rows? | (a) backfill by running the five client regexes once at migration; (b) leave NULL and fall back to client classification for old rows | **(a).** It is a one-pass backfill over rows that are never rewritten, and it lets the client-side regexes be deleted outright rather than kept as a fallback. |

