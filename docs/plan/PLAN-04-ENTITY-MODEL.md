# PLAN-04 — The Entity Model

What the entities *are*, before any question of tables, documents or indexes. PLAN-05 turns this into
DDL; this document decides what exists, what identifies it, what owns it, and how many of it there are.

Everything here is derived from PLAN-01 (JAR artifacts), PLAN-02 (PL/SQL artifacts, all 5 dialects) and
PLAN-03 (Claude / cross-ref / chat / conversion artifacts + the authoritative access-pattern table),
cross-checked against real data in `data_old_1784577715/jar/web-1.0.0-SNAPSHOT` and
`../data-backup-20260902-054631/plsql-parse/ERP_USP_RUN_CYCLE__WL2ZTC`.

---

## 0. Summary table — every entity

Row counts are for the two reference workloads: **J** = one 1,597-class JAR (~1,000 endpoints, the ~1 GB
`analysis.json` run of PLAN-01 §2.1); **P** = one 5,000-node PL/SQL crawl. Both are *per run*.
"—" means the entity does not exist on that side.

| # | Entity | Grain | Rows @ J | Rows @ P | Source | Own §|
|---|---|---|---|---|---|---|
| **Spine** ||||||
| E1 | `analysis_run` | one execution of any analyser | 1 | 1 | derived (new) | §2.1 |
| E2 | `storage_ref` | a pointer into `ArtifactStore` | ~1.6k | ~5.1k | new | §2.2 |
| **JAR side** ||||||
| E3 | `jar_artifact` | one uploaded JAR/WAR file | 1 | — | PLAN-01 §2.14 | §3.1 |
| E4 | `jar_analysis` | (jar, run, version) | 1–3 | — | PLAN-01 §2.1/2.2/2.3 | §3.2 |
| E5 | `java_class` | one parsed class | 1,597 | — | PLAN-01 §2.1 `classes[]` | §3.3 |
| E6 | `java_method` | one method of a class | ~16,000 | — | `ClassInfo.methods[]` | §3.4 |
| E7 | `java_invocation` | one bytecode call site | ~130,000 | — | `MethodInfo.invocations[]` | §3.5 |
| E8 | `java_field` | one declared field | ~11,000 | — | `ClassInfo.fields[]` | §3.6 |
| E9 | `endpoint` | one HTTP handler | ~1,000 | — | PLAN-01 §2.1/2.12.1 | §3.7 |
| E10 | `call_node` | one node of one endpoint's call tree | 67k–5M | — | PLAN-01 §2.12.3/2.12.4 | §3.8 |
| E11 | `call_node_collection` | (node, collection) access fact | ~10k–500k | — | `CallNode.collectionsAccessed` | §3.9 |
| E12 | `endpoint_collection` | (endpoint, collection) rollup **[denorm]** | ~4,000 | — | `aggregatedCollections` | §3.10 |
| E13 | `call_fragment` | overflow children of a fan-out node | 0..N | — | PLAN-01 §2.12.5 | §3.11 |
| E14 | `bundled_jar` | one dependency jar inside the uber-jar | ~300 | — | PLAN-01 §2.8 | §3.12 |
| E15 | `bundled_jar_resource` | one text resource in a bundled jar | ~1,500 | — | `bundled-jars.json[].resources` | §3.12 |
| E16 | `jar_dep_edge` | (dependency jar, referencing class) | ~2,000 | — | PLAN-01 §2.9 | §3.13 |
| E17 | `jar_resource` | one extracted config/DDL file | ~9 | — | PLAN-01 §2.13 | §3.14 |
| E18 | `connection_info` | DB connection strings seen in a jar | 1 | — | PLAN-01 §2.6 | §3.15 |
| E19 | `domain_config` | classification rules a run used | 1 | — | PLAN-01 §2.7 | §3.16 |
| E20 | `external_link` | Java→Java cross-jar HTTP link | 0..N | — | PLAN-01 §2.11 / PLAN-03 §2.8 | §3.17 |
| E21 | `endpoint_correction` | Claude verdict for one endpoint | ≤1,000 | — | PLAN-01 §2.17 | §3.18 |
| E22 | `node_correction` | Claude verdict for one node | ~5,000 | — | `CorrectionResult.corrections[]` | §3.18 |
| **PL/SQL side** ||||||
| E23 | `plsql_analysis` | (db_type, project, env, object, run) | — | 1 | PLAN-02 §0, §3.7 | §4.1 |
| E24 | `flow_node` | one crawled routine | — | 5,000 | `chunks/chunk_NNNN.json` | §4.2 |
| E25 | `flow_node_statement` | one parsed statement | — | ~368,000 | `chunks[].statements[]` | §4.3 |
| E26 | `flow_node_call` | one grouped call target | — | ~249,000 | `chunks[].calls[]` | §4.4 |
| E27 | `flow_node_cursor` | one cursor | — | ~46,000 | `chunks[].cursors[]` | §4.5 |
| E28 | `flow_node_variable` | one local/package variable | — | ~204,000 | `localVariables[]` | §4.6 |
| E29 | `flow_node_parameter` | one formal parameter | — | ~4,400 | `parameters[]` | §4.7 |
| E30 | `flow_node_exception_handler` | one handler | — | ~91,000 | `exceptionHandlers[]` | §4.8 |
| E31 | `flow_node_dynamic_sql` | one dynamic-SQL site | — | ~3,000 | `dynamicSql[]` | §4.9 |
| E32 | `flow_node_statement_summary` | (node, statement_type, count) | — | ~40,000 | `statementSummary` map | §4.10 |
| E33 | `flow_edge` | one raw call edge | — | ~217,000 | `edges/edges_NNNN.json` | §4.11 |
| E34 | `flow_edge_dedup` | one unique edge **[denorm]** | — | ~21,000 | `api/call_graph.json` | §4.12 |
| E35 | `flow_cycle` / `flow_cycle_member` | one detected cycle | — | ~10 / ~40 | `api/index.json.cycles[]` | §4.13 |
| E36 | `db_object` | one schema object the flow touches | — | ~4,000 | `tables.json` + tables index | §4.14 |
| E37 | `db_object_usage` | (object, node, operation) | — | ~40,000 | `usedBy[].operations` | §4.15 |
| E38 | `db_object_usage_line` | one source line of a usage | — | ~234,000 | `operations.<verb>[]` | §4.15 |
| E39 | `table_join` | one parsed join | — | ~94,000 | `tableOperations[].joins[]` | §4.16 |
| E40 | `db_column` | catalog column snapshot | — | ~15,000 | enrichers | §4.17 |
| E41 | `db_constraint` | catalog constraint snapshot | — | ~10,000 | enrichers | §4.18 |
| E42 | `db_index_column` | catalog index column snapshot | — | ~4,500 | enrichers | §4.19 |
| E43 | `db_trigger` | catalog trigger snapshot | — | ~500 | enrichers | §4.20 |
| E44 | `plsql_source_file` | one DDL text file (ref only) | — | ~4,600 | `sources/<file>` | §4.21 |
| E45 | `resolver_object` / `_dependency` / `_ambiguity` | Oracle `ALL_DEPENDENCIES` snapshot | — | 85 / 44 / 47 | `resolver/q*.json` | §4.22 |
| E46 | `connection_scope` | (db_type, project, env) — **not per run** | — | 1 (shared) | `cache/schema-resolver/**` | §4.23 |
| **Claude / LLM** ||||||
| E47 | `claude_verification` | one verification result per analysis | 0–1 | 0–1 | PLAN-03 §2.2.1 | §5.1 |
| E48 | `claude_table_verification` | one verified table | — | 34 obs / ~4,000 | `tables[]` | §5.1 |
| E49 | `claude_operation_verification` | one verified operation + `userDecision` | — | 445 obs / ~50,000 | `claudeVerifications[]` | §5.1 |
| E50 | `claude_call_verification` | one verified call | — | 139 obs / ~20,000 | `calls[]` | §5.1 |
| E51 | `claude_chunk` | one LLM chunk (prompt+response) | ~1,000 | 25 obs / ~2,000 | PLAN-03 §2.2.4/6/7 | §5.2 |
| E52 | `claude_session` | one enrichment session | tens | tens | PLAN-03 P1/P2 | §5.3 |
| E53 | `claude_call_log_entry` | **one CLI call** — append-only | ~2,000 | ~2,000 | PLAN-03 §2.2.9 | §6 |
| **Cross-cutting** ||||||
| E54 | `cross_ref_run` | one Java↔Oracle comparison | 0..N | — | PLAN-03 §2.3 | §5.4 |
| E55 | `cross_ref_proc_link` | one Java-method ↔ Oracle-proc link | ~100s | — | `procLinks[]` | §5.4 |
| E56 | `cross_ref_chunk` | one LLM chunk of a cross-ref run | ~100s | — | PLAN-03 §2.3.3 | §5.4 |
| E57 | `flow_analysis_run` | one optimise/analyse run | 0..N | — | PLAN-03 §2.4 | §5.5 |
| E58 | `flow_analysis_log_entry` | **one log line** — append-only | ~1,000s | — | PLAN-03 §2.4.2 | §6 |
| E59 | `conversion_job` | one PL/SQL→Java conversion job | 0..N | — | PLAN-03 §2.5 | §5.6 |
| E60 | `conversion_job_log_entry` | **one line** — append-only | ~1,000s | — | `log[]` / `liveLines[]` | §6 |
| E61 | `chat_session` | one chat conversation | 0..N | — | PLAN-03 §2.6 | §5.7 |
| E62 | `chat_message` | **one message** — append-only | unbounded | — | `messages[]` | §6 |
| E63 | `checkpoint_entry` | one completed chunk id — append-only | per run | per run | PLAN-03 §2.2.5/2.3.4 | §6 |
| E64 | `prompt_template` / `prompt_override` | one template (global / per analysis) | 14 global | + 1/analysis | PLAN-03 §2.7 | §5.8 |
| E65 | `job_queue_entry` | one queued/running analyser job | global | global | PLAN-03 P7–P9 | §5.9 |

**Not entities** — see §1.2 and §1.3: decompiled source, `stored.jar`, `sources/<file>` bodies,
`resources/`, prompt bodies, report markdown, exports, `_class-tree.json`, `mongo-catalog.json`,
`flow.json`, `edges.json`, `api/node_id_map.json`, `subtreeTables`, `directTables`, `.parse-cache/`,
`.node-cache/`, `progress.json`, `plsql-env-config.yaml`.

---

## 1. The five rules this model obeys

### 1.1 Rule: decompiled source, stored JARs and exports are artifacts, not entities

PLAN-01 §2.1 measured `CallNode.sourceCode` at **2,050 B/node average** (n=560 real node files) and
identified it as the mechanism by which a real `analysis.json` reached **912 MB and was still growing**
(PLAN-00 C2). `JarAnalysisExecutor.java:215` enriches trees with decompiled source *before* the write at
`:220-225`, so every one of the four copies of every call tree (PLAN-01 §6.2) carries it.

The rule, applied uniformly below:

> Any field whose value is a **program text body** — decompiled Java, PL/SQL DDL, an LLM prompt, an LLM
> raw response, a generated markdown report, a `.jar` binary, an extracted resource file, an XLSX export —
> is stored in `ArtifactStore` under its existing key and referenced from the entity by a `*_key` column.
> The entity carries the *metadata* (length, hash, language, line span), never the bytes.

This is stated per-field in every entity below as **`→ ArtifactStore`**. It costs one extra fetch on the
handful of access patterns that actually want the text (L6, L8, L25, L11, L28, L16, L17) and removes
~2 KB × node-count from every list, tree and summary query — which is every other access pattern in
PLAN-03 §1.

Two nuances, because a blanket rule would be wrong:

- **`statements[].sqlText`** (PLAN-02 §3.3) is program text and is the single largest field in the PL/SQL
  parse — but it is also 100 % duplicated from `sources/<file>` at `line..lineEnd` (PLAN-02 §7.10).
  It is therefore *neither* an entity field *nor* a separate artifact: it is a **line span into
  `plsql_source_file`** (E44). See §4.3 and the OPEN QUESTION there.
- **`CallNode.sqlStatements`** (a string array, e.g. the 19-line `SELECT … FROM ENTERPRISE.ORDERS` on
  node 2 of `001_ReportController.monthlySales`) has no `sources/` equivalent on the JAR side — it is
  reconstructed from bytecode string literals. It stays inline, capped, because it is the *only* copy
  and it is what the cross-ref matcher joins on.

### 1.2 Rule: do not model dead artifacts

- **`_class-tree.json` / `_class-tree_<v>.json`** — key defined at `JarDataPaths.java:148-153`, deleted at
  `PersistenceService.java:356-366`, **no writer and no reader anywhere** (PLAN-01 §2.5). The Code
  Structure tab (L1) streams `analysis.json` live via `GET /{id}/classes/tree`. Not modelled. `java_class`
  (E5) makes the tree a cheap query, which is what it wanted to be.
- **`mongo-catalog.json`** — `MongoCatalogService.fetchCatalog:413` returns null ("live MongoDB
  connections are disabled"), so it is **never written**; absent from all five on-disk analyses
  (PLAN-01 §2.10). Not modelled. Access pattern L9 (`GET /{id}/catalog`) is a real route, but it is
  gated on `verification == null` and therefore currently never fires with data. **Recommendation:**
  keep the route returning `{collections:[],views:[]}`; if live catalog fetching is ever re-enabled it
  belongs on `connection_scope` (E46), not per-jar — it is connection-grained, exactly like the PL/SQL
  schema-resolver cache.
- **`JarDataPaths.chatHistoryKey` → `jar/{id}/chat/history.jsonl`** — no caller anywhere
  (PLAN-03 §5.9, defect 9). Chat lives under `claude-chatbot/` (E61/E62). The `jar/.../chat/` key is dead.
- **`resolver/q3_reverse_dependencies.json`** — empty in **all 10** analysis directories including Oracle
  (PLAN-02 §3.12-3.15). Modelled as a *column shape* on E45 but with an explicit OPEN QUESTION; do not
  build a read path for it until a runtime check produces data.

### 1.3 Rule: node ordinals are run-scoped; nothing keys on them

PLAN-02 §10.2 proved this empirically: the two executions of the SQL Server crawl (`USP_RUNMRPCYCLE__CIJ65E`)
produced **different node numbering and different edge-file counts** (`edges_0057.json` vs `edges_0059.json`,
not aligned 1:1), and `dbCallCount` differed 259 vs 263 on the Oracle pair. The crawl is not deterministic.

Consequences, enforced throughout:

- `FlowNode.order` / `chunk_idx` / `edges_NNNN` index → a **within-run attribute** on E24/E33, never part
  of any key that survives the run. Cross-run identity of a routine is
  `(db_type, project, environment, schema, package_name, object_name, overload_param_count)`.
- The JAR endpoint folder ordinal `%03d` (`EndpointOutputWriter.java:40-42`) → **not** part of E9's key.
  Cross-run endpoint identity is `(jar_id, http_method, full_path, controller_class, method_name)`.
  PLAN-01 §7 already says this; PLAN-03 §1.5.4 makes it load-bearing, because `summary.js:497-514` merges
  slice responses back by array index and will silently corrupt the UI if two queries disagree on order.
  E9 therefore carries **both** a stable `endpoint_id` and a run-scoped `ordinal` used only for
  presentation ordering and for reproducing the legacy folder name.
- `%04d` caps the current scheme at **9,999 nodes** (`ChunkedFlowWriter.java:126,132`). The new model does
  not inherit it: `order` is an integer with no width, and no key derives from the zero-padded string.

### 1.4 Rule: identity is `(db_type, project, environment, object)` **plus an explicit `run_id`**

Today the archive timestamp is buried inside the object segment: `ERP_USP_RUN_CYCLE__WL2ZTC_20260723_153725`
is a *directory name*, and the `_yyyyMMdd_HHmmss` suffix is the only record that it is an older run
(PLAN-02 §0). `AnalysisPaths.name()` will happily round-trip that string, so the archive timestamp becomes
part of the "object name" and every downstream join sees a different object.

The model separates these:

```
analysis_key   = (db_type, project, environment, object_name)     -- stable, business identity
run            = analysis_key + run_id                            -- one execution
current run    = the run with is_current = true for that key      -- at most one
archived runs  = the rest, ordered by started_at desc
```

The same shape applies to the JAR side, where the equivalent hidden state is the version discriminator
(`""` / `corrected` / `corrected_prev`, PLAN-01 §2.2) and `claudeIteration`. `analysis_run` (E1) is the
single place a "which execution?" question is answered on either side.

`run_id` is a **UUIDv7** (time-ordered, so `ORDER BY run_id` is chronological without a second index and
concurrent runs from different processes cannot collide). It is *not* the epoch-ms scheme used by
`crossRefId` (`CrossRefService.java:139`) or the in-memory counter used by flow-analysis `runId`
(`FlowAnalysisController.java:117`, explicitly **not durable across restarts** — PLAN-03 §2.4). Both of
those become `legacy_id` columns during migration.

### 1.5 Rule: one schema, five dialects — nullable columns + a `db_type` discriminator

PLAN-02 §4.2 established that `ChunkedFlowWriter`, `ParseResultCache`, `NodeParseCache` and
`FlowAnalysisMain` contain **zero dialect branching**; the file set is byte-for-byte identical in shape
across all five. Divergence lives entirely in `SchemaResolver`'s constructor and the five enrichers.
So: one set of entities, `db_type ∈ {ORACLE, MYSQL, SQLSERVER, DB2, POSTGRES}` on `plsql_analysis` (E23)
and denormalised onto the catalog-snapshot entities (E40–E43) so a dialect-scoped catalog query needs no
join. Every dialect-specific column is nullable, and its nullability is documented as *structural*
(the dialect never emits it) rather than *incidental*.

**Trap 1 — `dbConstraints.refConstraint` means three different things** (PLAN-02 §3.9, §4.1):

| dialect | raw value | actual meaning |
|---|---|---|
| Oracle | `SYS_C0010522` | the **name** of the referenced constraint |
| MySQL | `orders.order_id` | referenced **`table.column`** |
| SQL Server / DB2 / Postgres | `analyzer.orders` | referenced **`schema.table`** |

Verified in real data: `ERP_BOM_COMPONENT.FK_BC_BOM` carries `"refConstraint": "SYS_C0010522"`.

Modelling the raw string and a dialect tag "works" and is what a lazy port would do. It is wrong, because
every consumer then re-implements the three-way parse — and the one question anyone asks
("what does this FK point at?") stays unanswerable in SQL. E41 therefore stores the **semantics**:

```
ref_schema           text NULL   -- Oracle: NULL (must be resolved via ref_constraint_name)
ref_table            text NULL   -- MySQL: parsed from before the '.'
ref_column           text NULL   -- MySQL only
ref_constraint_name  text NULL   -- Oracle only
ref_raw              text NULL   -- the original string, kept verbatim for audit/round-trip
```

with a resolution pass that, for Oracle, joins `ref_constraint_name` back to the `db_constraint` row of
type `P`/`U` on the same analysis to fill `ref_schema`/`ref_table`. That pass is the *only* dialect-aware
code in the whole model, it runs once at ingest, and `ref_raw` means it is always reversible.

**Trap 2 — `statementSummary` must never be an enum.** PLAN-02 §4.3 measured **27 / 28 / 12 / 11 / 11**
distinct keys for Oracle / SQL Server / DB2 / Postgres / MySQL, with dialect-exclusive tokens
(`EXECUTE_IMMEDIATE`, `FORALL`, `PIPE_ROW` — Oracle; `TRY_CATCH`, `THROW`, `BEGIN_TRAN`, `DEALLOCATE`,
`OTHER` — SQL Server; `UPSERT` — MySQL+Postgres; `REPEAT` — MySQL+DB2). The same open-endedness applies to
`allOperations[]` / `operationSummary` (`CREATE`, `DROP`, `TRUNCATE` only in SQL Server; `DYNAMIC` only in
Oracle) and to `capturedStatements` in the verification reports (PLAN-03 §2.7.5).

E32 `flow_node_statement_summary` is therefore a **child table `(analysis_id, node_id, statement_type TEXT, count INT)`**
— not columns, not an enum, not a check constraint. Any new dialect adds rows, never a migration.
The UI's statement-type rollup (`parser/js/statements.js:14-59`, PLAN-03 §4.3.9) becomes
`GROUP BY statement_type` instead of an O(nodes) client pass.

---

## 2. The spine

### 2.1 E1 — `analysis_run`

**Represents:** one execution of one analyser, of any kind. The thing that today is either an
untracked overwrite (JAR), a directory-name timestamp suffix (PL/SQL, PLAN-02 §0), an epoch-ms string
(`crossRefId`, PLAN-03 §2.3), or a non-durable in-memory counter (flow-analysis `runId`,
`FlowAnalysisController.java:117`).

| Field | Type | Null | Meaning |
|---|---|---|---|
| `run_id` | uuid (v7) | no | **surrogate PK**. Time-ordered. |
| `kind` | text | no | `JAR` \| `WAR` \| `PLSQL` \| `CROSSREF` \| `FLOW_ANALYSIS` \| `CONVERSION` \| `CLAUDE_VERIFY` \| `CLAUDE_CORRECT` |
| `subject_kind` / `subject_id` | text / text | no | polymorphic owner: `jar_artifact.jar_id` or `plsql_analysis_key.key_id` |
| `status` | text | no | `RUNNING` \| `DONE` \| `FAILED` \| `INTERRUPTED`. Today inferred from file existence (PLAN-03 §4.2 rows 14–15). |
| `is_current` | boolean | no | exactly one true per subject per kind; partial-unique enforced |
| `started_at` | timestamptz | no | |
| `finished_at` | timestamptz | yes | null while `RUNNING` |
| `crawl_time_ms` | bigint | yes | `flow.json.crawlTimeMs` — 4,872 observed (Oracle) |
| `total_duration_ms` | bigint | yes | `api/index.json.totalDurationMs` — 5,598 observed; incl. enrichment |
| `db_call_count` | int | yes | 259 observed. **Non-deterministic** (259 vs 263 across two runs) |
| `error` | text | yes | also a non-error status carrier: `"Partial result (3/10 chunks completed)"` (`ClaudeVerificationService.java:233`) |
| `legacy_id` | text | yes | the epoch-ms `crossRefId`, the flow-analysis counter, or the `_yyyyMMdd_HHmmss` archive suffix |
| `archived_at` | timestamptz | yes | set when superseded; drives `pruneOldArchives` (`AnalysisService.java:346-372`) |

**Natural key:** `(subject_kind, subject_id, kind, started_at)`. **Surrogate:** `run_id`.
**Cardinality:** 1 per execution; N per subject. **Rows:** 1 per J or P run; tens–hundreds per subject
over a project's lifetime (PLAN-02 §7.9 observed 4 archive twins per PL/SQL analysis, PLAN-03 §2.2
observed 4 sibling Claude run directories).

**Why timing lives here and nowhere else.** PLAN-02 §10.2 consequence 1: `crawlTimeMs`, `parseTimeMs`,
`totalDurationMs` and `dbCallCount` guarantee that two runs of the same crawl differ byte-wise, which
defeats content hashing for dedupe. Isolating every timing field on `analysis_run` makes the *analytical*
payload hashable, so a re-run that produced identical results can be detected and collapsed.

### 2.2 E2 — `storage_ref`

**Represents:** one blob in `ArtifactStore`, addressed by its existing key. This is the join between the
normalised model and the frozen blob interface (PLAN-00 C1). Nothing about `ArtifactStore` changes; this
entity is a *catalogue* of what has been put there.

| Field | Type | Null | Meaning |
|---|---|---|---|
| `ref_id` | uuid | no | surrogate PK |
| `store_key` | text | no | the literal `ArtifactKey` string, e.g. `jar/web-1.0.0-SNAPSHOT/stored.jar` |
| `run_id` | uuid | yes | owning run; null for connection-scoped and global blobs |
| `role` | text | no | `STORED_JAR` \| `DECOMPILED_SOURCE` \| `PLSQL_SOURCE` \| `LLM_PROMPT` \| `LLM_RESPONSE` \| `REPORT_MD` \| `RESOURCE` \| `EXPORT` \| `LOG` |
| `byte_length` | bigint | no | so `size()` is never called to render a list |
| `content_sha256` | bytea | yes | enables cross-run dedupe of identical decompiled methods and DDL |
| `content_type` | text | yes | |
| `created_at` | timestamptz | no | |

**Natural key:** `store_key`. **Rows @ J:** ~1,600 (1 stored jar + ~1,000 endpoint-level + resources +
LLM payloads); **@ P:** ~5,100 (4,600 source files + ~500 LLM payloads).

`content_sha256` is not decoration. PLAN-01 §6.6 notes decompiled source is stored 4× *and* re-derivable
from `stored.jar`; PLAN-02 §3.17 notes `.parse-cache/` is already content-addressed by MD5 of the source
text. Hashing at the `storage_ref` level makes both dedupes fall out for free and lets a re-run reuse the
previous run's blobs when the source did not change.

---

## 3. JAR-side entities

### 3.1 E3 — `jar_artifact`

**Represents:** one uploaded JAR or WAR file — the thing the user picked, independent of how many times
it has been analysed.

| Field | Type | Null | Meaning |
|---|---|---|---|
| `jar_id` | text | no | `JarNameUtil.normalizeKey(jarName)` (`JarDataPaths.java:120-125`) |
| `jar_name` | text | no | original filename, e.g. `web-1.0.0-SNAPSHOT.jar` |
| `kind` | text | no | `JAR` \| `WAR` (`WarDataPaths` roots at `jar/war`) |
| `jar_size` | bigint | no | **70,489,772** observed |
| `project_name` | text | yes | omitted when null at `PersistenceService.java:193` |
| `base_package` | text | yes | e.g. `com.enterprise` |
| `stored_jar_ref` | uuid → E2 | yes | **→ ArtifactStore** (`jar/<id>/stored.jar`, 433 KB – 70.5 MB, PLAN-01 §2.14). The bytes are never a row. |
| `first_seen_at` / `last_analyzed_at` | timestamptz | no / yes | |

**Natural key:** `jar_id`. **Rows:** 1 per uploaded artifact; 5 in the sample dataset.

The sidebar's de-timestamping and CORRECTED-first grouping (`jar/js/sidebar.js:44-67`, PLAN-03 §4.3.16 —
"a 13-digit-suffix strip plus a two-key sort on every sidebar render") becomes a query over
`jar_artifact ⋈ jar_analysis` once `jar_id` and the version discriminator are separate columns.

### 3.2 E4 — `jar_analysis`

**Represents:** the result of analysing one jar once, in one mode. Replaces `analysis.json` /
`analysis_corrected.json` / `analysis_corrected_prev.json` (PLAN-01 §2.1, §2.2) *and* `_header.json`
(§2.3), which is a pure cache of these very fields.

| Field | Type | Null | Meaning |
|---|---|---|---|
| `analysis_id` | uuid | no | surrogate PK |
| `jar_id` | text → E3 | no | |
| `run_id` | uuid → E1 | no | |
| `version` | text | no | `STATIC` \| `CORRECTED` \| `CORRECTED_PREV` — was the filename suffix |
| `analysis_mode` | text | no | `STATIC` \| `CORRECTED` (`PersistenceService.java:198`) |
| `analyzed_at` | timestamp | no | **`2026-07-19T19:45:20.171790`** observed — *local, no zone*; see OPEN QUESTION T1 |
| `total_classes` | int | no | **119** observed |
| `total_endpoints` | int | no | **84** observed |
| `scoped` | boolean | no | true only when `scopeTarget` set (`:201`) |
| `scope_target` | text | yes | |
| `correction_applied_at` | timestamp | yes | `JarAnalysis.java:9`, `@JsonInclude(NON_NULL)` |
| `correction_count` | int | no (0) | |
| `claude_iteration` | int | no (0) | |
| `claude_enriched` | boolean | no | **new column** — today this entire decision is "does the fragment listing return a non-empty array" (PLAN-03 §4.2 row 1) |

**Natural key:** `(jar_id, run_id, version)`. **Cardinality:** 1–3 per jar.
**Rows @ J:** 1–3. Real header (191 B): `{"jarName":"web-1.0.0-SNAPSHOT.jar","jarSize":70489772,
"analyzedAt":"2026-07-19T19:45:20.171790","totalClasses":119,"totalEndpoints":84,
"analysisMode":"STATIC","basePackage":"com.enterprise"}`.

`_summary_static.json` / `_summary.json` (430 KB, gz 17.7 KB) are **not** an entity — PLAN-01 §5 calls
them "derived, entirely; cacheable, never source of truth". They are the *projection* this model serves
directly from E5+E9+E12; see PLAN-05 for the materialised-view decision, which access pattern **I2**
forces.

### 3.3 E5 — `java_class`

**Represents:** one class parsed out of the jar or one of its bundled jars.

| Field | Type | Null | Meaning |
|---|---|---|---|
| `class_id` | uuid | no | surrogate |
| `analysis_id` | uuid → E4 | no | |
| `fqn` | text | no | `fullyQualifiedName` — natural key within an analysis |
| `package_name` / `simple_name` | text | yes | |
| `super_class` | text | yes | FQN; self-reference by name, not by id (may point outside the jar) |
| `stereotype` | text | yes | `REST_CONTROLLER` \| `SERVICE` \| `REPOSITORY` \| `ENTITY` \| `OTHER` … open-ended, **not an enum** |
| `access_flags` | int | no | JVM bitmask |
| `is_interface` / `is_abstract` / `is_enum` | boolean | no | |
| `source_jar` | text | yes | → E14 `bundled_jar.name`; null for the main app |
| `source_path` | text | yes | `src/main/java/com/enterprise/oracle/web/controller/ReportController.java` |
| `repository_entity_type` | text | yes | entity type param of a Spring Data repo |
| `interfaces` | text[] | no (`[]`) | small, unordered, never filtered on individually → stays inline |
| `annotations` | jsonb | no (`{}`) | `AnnotationInfo{name, attributes}`; `attributes` is arbitrarily nested |

**Natural key:** `(analysis_id, fqn)`. **Cardinality:** N per analysis.
**Rows @ J: 1,597** (119 observed; `classes[]` = 2,601,008 B, avg 21.9 KB/class, max 162 KB).

`annotations` stays `jsonb` deliberately: `AnnotationInfo.attributes` is "arbitrary nested values"
(PLAN-01 §2.1) and no access pattern filters on an attribute value. `interfaces` stays an array for the
same reason. Both are bounded per class.

### 3.4 E6 — `java_method`

| Field | Type | Null | Meaning |
|---|---|---|---|
| `method_id` | uuid | no | surrogate |
| `class_id` | uuid → E5 | no | |
| `name` | text | no | `<init>` for constructors |
| `descriptor` | text | no | **required for uniqueness** — overloads share `name` |
| `return_type` | text | yes | |
| `access_flags` | int | no | |
| `http_method` / `path` | text | yes | non-null exactly when this method is an endpoint handler → E9 |
| `start_line` / `end_line` | int | no | |
| `parameters` | jsonb | no (`[]`) | `ParameterInfo{name,type,annotations}` — bounded, always read with the method |
| `annotations` | jsonb | no (`[]`) | |
| `string_literals` | text[] | no (`[]`) | LDC constants; the input to collection/SQL detection |
| `local_variables` | jsonb | no (`{}`) | name → type |
| `field_accesses` | jsonb | no (`[]`) | `FieldAccessInfo{ownerClass,fieldName,fieldType,accessType,lineNumber}` |

**Natural key:** `(class_id, name, descriptor)`. **Cardinality:** 0..65 observed per class.
**Rows @ J: ~16,000** (10/class × 1,597; the largest observed class has 65).

### 3.5 E7 — `java_invocation`

**Represents:** one bytecode call site inside one method. PLAN-01 §2.1 names `invocations[]` the
"largest single contributor to class size".

| Field | Type | Null | Meaning |
|---|---|---|---|
| `method_id` | uuid → E6 | no | |
| `seq` | int | no | position within the method — run-scoped, presentational |
| `owner_class` / `method_name` / `descriptor` / `return_type` | text | yes | the callee |
| `opcode` | smallint | no | JVM invoke opcode |
| `line_number` | int | no | |
| `receiver_field_name` | text | yes | the DI field holding the receiver — how call resolution works |
| `lambda_invocation` | boolean | no | |
| `lambda_target_class` / `_method` / `_descriptor` | text | yes | |
| `recent_string_args` | text[] | **yes — nullable, no default init** | PLAN-01 §2.1 is explicit: this is `null`, not `[]` |
| `dynamic_arg_sources` | jsonb | yes | array of `{kind,name,ownerClass}`; also nullable, not `[]` |

**Natural key:** `(method_id, seq)`. **Cardinality:** 1..1000s per method, ~200 B each.
**Rows @ J: ~130,000** (8 per method × 16,000). This is the largest JAR-side table that is *not* the
call tree.

### 3.6 E8 — `java_field`

`(class_id, name)`; `type`, `access_flags`, `constant_value`, `mongo_field_name` (from
`@Field`/`@BsonProperty`/`@JsonProperty`), `id_field` boolean (`@Id`/`@BsonId`), `jpa_column_name`,
`generic_element_type`, `annotations jsonb`. **Rows @ J: ~11,000** (7/class).

`effectiveMongoName()` / `effectiveJpaColumnName()` are methods, **not serialised** (PLAN-01 §2.1) — but
they are exactly what `entityCollMap` / `repoCollMap` need, and PLAN-03 §4.3.8 flags that join as
"O(repos × entities) in the browser". Materialise them as generated columns
(`COALESCE(mongo_field_name, name)`) so the mapping is a server-side join.

### 3.7 E9 — `endpoint`

**Represents:** one HTTP handler and its whole-tree rollups. Unifies `analysis.json.endpoints[i]`,
`endpoints/<NNN>/endpoint.json` and `_summary_static.json.endpoints[i]` — PLAN-01 §6.1 shows these are
three near-identical copies of the same fields.

| Field | Type | Null | Meaning | Real value (001_ReportController.monthlySales) |
|---|---|---|---|---|
| `endpoint_id` | uuid | no | surrogate | |
| `analysis_id` | uuid → E4 | no | | |
| `http_method` | text | yes | | `GET` |
| `full_path` | text | yes | resolved class+method mapping | `/api/v1/reports/sales/monthly/{year}` |
| `controller_class` | text | yes | → E5 by fqn | `com.enterprise.oracle.web.controller.ReportController` |
| `controller_simple_name` | text | yes | used in the legacy folder name | `ReportController` |
| `method_name` | text | yes | | `monthlySales` |
| `return_type` | text | yes | | `org.springframework.http.ResponseEntity` |
| `parameters` | jsonb | no (`[]`) | bounded | `[{name:"year",type:"int",annotations:[{name:"PathVariable"}]}]` |
| `ordinal` | int | no | **run-scoped**; reproduces `%03d` (§1.3) | `1` |
| `source_jar` | text | yes | `""` when absent (`EndpointOutputWriter.java:55`) | `""` |
| `source_path` | text | yes | | `src/main/java/.../ReportController.java` |
| `derived_operation_type` | text | yes | highest-priority op across the tree | `READ` |
| `total_db_operations` | int | no (0) | pre-computed so summaries skip the tree | `1` |
| `total_methods` | int | no (0) | | `15` |
| `total_loc` | int | no (0) | | `59` |
| `in_scope_calls` / `external_scope_calls` | int | no (0) | | `1` / `14` |
| `modules` | text[] | no (`[]`) | | `["main","service-1.0.0-SNAPSHOT.jar","repository-1.0.0-SNAPSHOT.jar"]` |
| `operation_types` | text[] | no (`[]`) | open-ended | `["READ"]` |
| `has_call_tree` | boolean | no | `callTree` is genuinely nullable (PLAN-01 §2.1) | true |
| `external_calls` | jsonb | no (`[]`) | capped 100 (`AbstractAnalysisDataProvider.java:18-26`) | 14 elements |
| `http_calls` | jsonb | no (`[]`) | capped 50 | |
| `beans` | jsonb | no (`[]`) | capped 100 | |
| `proc_names` | jsonb | no (`[]`) | Oracle proc names from `@LogParameters` — **the JAR↔PL/SQL bridge**, see §7 | |
| `dynamic_flows` / `aggregation_flows` | jsonb | no (`[]`) | capped 200 each | |
| `dynamic_collection_flows` | jsonb | no (`[]`) | capped 200 | |
| `collection_lifecycle_flows` | jsonb | no (`[]`) | capped 100 | |

**Natural key:** `(analysis_id, http_method, full_path, controller_class, method_name)` — explicitly
**not** the ordinal (§1.3). **Surrogate:** `endpoint_id`, which is also what fixes PLAN-03 §1.5.4's
positional-coupling hazard. **Cardinality:** N per analysis.
**Rows @ J: ~1,000** (84 observed for 119 classes; `endpoints[]` = 1,302,544 B, avg 15.5 KB, max 166 KB).

The seven `*_flows` / `*_calls` arrays stay `jsonb`: they are already server-capped, they are always
fetched with the endpoint (access pattern L7 fetches them as *slices of the same array positions*), and
nothing filters inside them. Promoting them to tables would add seven joins to the hottest read for no
query benefit. This is recorded as a deliberate denormalisation in PLAN-05.

`aggregatedCollectionVerification` / `aggregatedCollectionSources` are named in `@JsonPropertyOrder` but
**have no fields or getters in `EndpointInfo.java`** (PLAN-01 §2.1 ⚠️) — they exist only in summary
output, computed at `AbstractAnalysisDataProvider.java:578-586`. They are therefore **not** endpoint
columns; they are derived from E11 at query time.

### 3.8 E10 — `call_node`

**Represents:** one node of one endpoint's resolved call tree. This is the entity that decides whether
the whole design works: PLAN-01 §6.2 shows the same tree stored **four times**
(`analysis.json.endpoints[i].callTree` + `call-tree.json` + `nodes.json` + `nodes/node_*.json`), and
PLAN-00 finding 6 identifies embedded source as the ~1 GB driver.

| Field | Type | Null | Meaning | Real value (node 2) |
|---|---|---|---|---|
| `node_id` | uuid | no | surrogate | |
| `endpoint_id` | uuid → E9 | no | | |
| `ordinal` | int | no | DFS ordinal — **run-scoped** (`nodeId` in `nodes.json`) | `2` |
| `parent_ordinal` | int | yes | **new, explicit**. Today topology is implicit in DFS order + `childCount` (PLAN-01 §2.12.3) | `1` |
| `depth` | int | no | | `2` |
| `child_count` | int | no | | `1` |
| `signature` | text | no | `TreeChunker.nodeSignature` | `…OrderJdbcRepository.getMonthlySalesReport(int):List` |
| `method_ref` | text | no | `CallNode.id`; **repeats across a tree** — not a key | `…#getMonthlySalesReport(I)Ljava/util/List;` |
| `class_name` / `simple_class_name` / `method_name` | text | yes | → E5/E6 by name | `…OrderJdbcRepository` / `getMonthlySalesReport` |
| `parameter_types` | text[] | no (`[]`) | | |
| `return_type` | text | yes | | |
| `stereotype` | text | yes | | `REPOSITORY` |
| `call_type` | text | yes | `APPLICATION` \| `CROSS_MODULE` \| `HTTP_CALL` \| `FEIGN` … open | `CROSS_MODULE` |
| `dispatch_type` | text | yes | `DIRECT`/`DYNAMIC_DISPATCH`/`QUALIFIED`/`HEURISTIC`/`INTERFACE_FALLBACK` | null |
| `resolved_from` / `qualifier_hint` | text | yes | how the callee was resolved | null |
| `recursive` / `cross_module` | boolean | no | | false / true |
| `module` / `domain` | text | yes | | `Repository` / `Repository` |
| `source_jar` | text | yes | | `repository-1.0.0-SNAPSHOT.jar` |
| `source_path` | text | yes | | `src/main/java/.../OrderJdbcRepository.java` |
| `line_count` | int | no | | `33` |
| **`source_ref`** | uuid → E2 | yes | **→ ArtifactStore.** The decompiled body (1,681 chars on this node; **2,050 B avg over 560 real nodes**). *Never inline.* | |
| `operation_type` | text | yes | | `READ` |
| `operation_types` | text[] | no (`[]`) | from a `Set` | `["READ"]` |
| `has_db_interaction` | boolean | no | | `true` |
| `sql_statements` | text[] | **yes (nullable, not `[]`)** | stays inline: only copy, and the cross-ref join key (§1.1) | 1 statement, 19 lines, `FROM ENTERPRISE.ORDERS` |
| `oracle_proc_ref` | text | yes | `PROC:PKG.NAME` / `FUNC:PKG.NAME` — **the JAR↔PL/SQL bridge**, §7 | null |
| `oracle_schema` | text | yes | | null |
| `entity_field_mappings` | jsonb | yes (nullable) | | |
| `aggregation_pipeline` | jsonb | yes (nullable) | deeply nested | |
| `cache_operations` | jsonb | yes (nullable) | | |
| `dynamic_collection_refs` | jsonb | no (`[]`) | `{variableName,varKind,pattern,source,operationType}` | |
| `lifecycle_stage_tags` | text[] | no (`[]`) | | |
| `annotations` | text[] | no (`[]`) | | `[]` |
| `annotation_details` | jsonb | no (`[]`) | `{name, attributes}` | |
| `string_literals` | text[] | no (`[]`) | | 1 element |
| `fragment_ref` | text | yes | → E13 | null |
| `truncated_child_count` | int | yes | | null |
| `http_target_method` / `_path` / `_module_hint` | text | yes | → another jar's endpoint, via E20 | null |

**Natural key:** `(endpoint_id, ordinal)` — run-scoped by construction, which is correct because a node
*is* a position in a tree produced by one run. There is no stable cross-run node identity and this model
does not pretend otherwise; `signature` is the cross-run *correlation* key and is indexed as such.
**Surrogate:** `node_id`.

**Rows @ J:** two figures, because the sample and the pathological case differ by two orders of magnitude.
- Measured: **560 nodes / 84 endpoints = 6.7 avg**, max ~90 → at 1,000 endpoints, **~6,700 rows**.
- PLAN-01 §2.1's ~1 GB scenario: 5,000-node trees × 100 such endpoints → **~500,000–5,000,000 rows**.
Design for the second. At ~800 B/row without source, 5 M rows ≈ 4 GB of *rows* against ~10 GB of
decompiled source that is now in `ArtifactStore` instead of in every document four times over.

`clearTransient()` (`CallNode.java:289-301`) nulls exactly `sourceCode`, `stringLiterals`,
`annotationDetails`, `entityFieldMappings`, `cacheOperations`, `aggregationPipeline` — "the fields that
are derived and re-derivable, and exactly what makes the difference between a 50 MB and a 1 GB
`analysis.json`". Those six are the pruning set: `source_ref` is already external, and the other five are
nullable and may be dropped on archived runs without loss of correctness.

### 3.9 E11 — `call_node_collection`

**Represents:** one (node, collection/table) data-access fact — the atom that PLAN-03 access patterns
**L4** ("which call paths touch collection C" — an unbounded parallel call-tree fan-out today) and
**L9/§4.3.15** ("catalog verification status — a lowercase-Set join against every row, in the browser")
both need and neither can express.

| Field | Type | Null | Meaning | Real value |
|---|---|---|---|---|
| `node_id` | uuid → E10 | no | | node 2 |
| `collection_name` | text | no | | `ENTERPRISE.ORDERS` |
| `source` | text | yes | `REPOSITORY_MAPPING`/`DOCUMENT_ANNOTATION`/`STRING_LITERAL`/`FIELD_CONSTANT`/`QUERY_ANNOTATION`/`PIPELINE_ANNOTATION`/`JDBC_TEMPLATE` — **open-ended** | `JDBC_TEMPLATE` |
| `domain` | text | yes | | `Other` |
| `operation` | text | yes | | `READ` |
| `verification` | text | yes | `VERIFIED`/`NOT_IN_DB`/`NO_CATALOG`; **nullable map today**, and its nullness is what gates L9 | null |

**Natural key:** `(node_id, collection_name)`. **Cardinality:** 0..N per node; 2 on the worked node.
**Rows @ J:** ~1.5 per db-touching node → **~10,000** at the measured shape, **~500,000** at the
pathological shape.

This entity replaces four parallel maps on `CallNode` (`collectionsAccessed`, `collectionDomains`,
`collectionSources`, `collectionVerification`) that are keyed by the same string and were only separate
because JSON has no row.

### 3.10 E12 — `endpoint_collection` — **deliberate denormalisation**

`(endpoint_id, collection_name, domain, operation)` — the endpoint-level rollup of E11 over the whole
tree. Reproduces `aggregatedCollections` / `aggregatedCollectionDomains` / `aggregatedOperations`
(real values on the worked endpoint: `["ORDER_DATE","ENTERPRISE.ORDERS"]`, both `Other`, both `READ`).

Strictly derivable, therefore not 3NF. Kept because access pattern **I2** (JAR summary headers, 430 KB,
fires on **every JAR click**) renders these for all ~1,000 endpoints at once, and computing them would
mean a recursive descent over up to 5 M `call_node` rows per page load. Write cost and invalidation are
quantified in PLAN-05 §4.

**Rows @ J: ~4,000** (4 collections/endpoint × 1,000).

### 3.11 E13 — `call_fragment`

`(endpoint_id, fragment_key)` → the overflow children of a node whose fan-out exceeded the per-node cap
(`fragments/<fragmentKey>.json`, PLAN-01 §2.12.5). **Not present in any sample data.** Modelled because
`CallNode.fragmentRef` + `truncatedChildCount` are live fields, but with an OPEN QUESTION:

> **OPEN QUESTION F1 — do fragments need to exist at all?**
> `EndpointInfo.fragments` is a spill mechanism for a *document-size* problem that only exists because a
> tree is one JSON blob. With one row per node (E10) there is no fan-out cap, so fragments are structurally
> unnecessary.
> **Options:** (a) model `call_fragment` and keep `fragment_ref` populated for round-trip fidelity;
> (b) flatten fragments into ordinary `call_node` rows at ingest and leave `fragment_ref` as a
> read-only legacy field.
> **Recommendation: (b).** Fragments are an artefact of the storage format, not of the analysis. Keep
> `fragment_ref` / `truncated_child_count` as nullable columns so the old JSON can be regenerated
> byte-compatibly, but do not create a second node-storage path.

### 3.12 E14/E15 — `bundled_jar`, `bundled_jar_resource`

`bundled_jar` = `(analysis_id, name)`; `size bigint`; `manifest_ref → E2` (the whole `MANIFEST.MF` text).
`bundled_jar_resource` = `(bundled_jar_id, file_name)`; `content_ref → E2`.

`bundled-jars.json` is a **bare JSON array**, 94 elements, **3,028,400 B** for one jar — ~32 KB/element,
dominated by the `resources` map which embeds `pom.xml` and friends verbatim (PLAN-01 §2.8). It is
fetched **unconditionally** on the Code Structure tab (**L2**), making it the second-largest thing the
browser pulls after the log endpoint (PLAN-03 §4.1).

Splitting the resource *bodies* into `ArtifactStore` turns a 3 MB unconditional fetch into a ~30 KB list
of `(name, size, resource_count)`, with bodies fetched on click. **Rows @ J: ~300 bundled jars,
~1,500 resources.**

### 3.13 E16 — `jar_dep_edge`

`(analysis_id, dependency_jar_name, app_class_simple_name)`. `jar-dep-map.json` is an object mapping
dependency jar → array of **simple** app class names (1,100 B, 11 keys observed). **Rows @ J: ~2,000.**

Note the values are *simple* names, so the FK to E5 is lossy under package collision. Keep the string;
resolve to `class_id` opportunistically and leave it null when ambiguous.

### 3.14 E17 — `jar_resource`

`(analysis_id, file_name)` + `content_ref → E2`. Extracted config/DDL (`application.yml`,
`V1..V8__*.sql`; 9 files, 1.8–19.8 KB each). **The bodies are artifacts, not entities** — they are raw
extracted bytes with no schema. **Rows @ J: ~9.**

### 3.15 E18 — `connection_info`

`(analysis_id)` — 1:1, so it could be columns on E4; kept separate because it is the natural join to
`connection_scope` (E46) and hence to the PL/SQL side (§7). Fields: `extracted_at`, `mongodb_uri`
(**credential-masked** by `MongoCatalogService.maskUri:494-497`), `mongodb_database`, `oracle_jdbc_url`,
`postgres_jdbc_url`. Real sample (130 B):
`{"extractedAt":"2026-07-19T19:45:19.964025","oracle":{"jdbcUrl":"jdbc:oracle:thin:@//localhost:1521/XEPDB1"}}`.

**Constraint C4 applies.** These URLs are already masked at write time. The model must not add an
unmasked variant, and `CredentialGuardStore` continues to wrap the backend.

### 3.16 E19 — `domain_config`

`(analysis_id)` → `config_ref → E2` + the handful of fields the UI actually reads.

`domain-config.json` is 7,818 B of user-authored config with 15 top-level keys, copied into **every** jar
folder (`JarAnalysisExecutor.java:196`, PLAN-01 §6.7) — and an identical copy also lives at
`config/domain-config.json` and is fetched by the UI as a *static file at a relative URL*
(`jar/js/summary.js:62`, PLAN-03 §2.7.4). Three copies of the same 7.8 KB.

The per-analysis copy is an **audit snapshot** — it records the rules a run used, which genuinely matters
and must not be deduped away against the current global config. Store it as a `storage_ref` with a
`content_sha256`, so N runs with unchanged config share one blob. **Rows @ J: 1.**

### 3.17 E20 — `external_link`

`(analysis_id, caller_endpoint, caller_class_name, caller_method_name, path)`; `http_method`,
`module_hint` (**key omitted entirely when null** — `ExternalApiLinkService.java:220`),
`state ∈ {RESOLVED, AMBIGUOUS, PENDING}`, `target_jar_id`, `target_controller_simple_name`,
`target_method_name`, `target_http_method`, `target_path`, and for `AMBIGUOUS` a `candidates jsonb`.

Merges `external-links.json` and `external-links-pending.json`, which today are two files holding three
states of the same fact. **Cross-document write dependency worth modelling explicitly** (PLAN-01 §2.11):
`reresolvePending()` rewrites **another jar's file** after any analysis finishes
(`ExternalApiLinkService.java:83-104`). As rows, that becomes an `UPDATE … WHERE state='PENDING' AND path=…`
touching only the rows that changed, instead of a full rewrite of every other jar's document.
**Rows @ J: 0..N**; no sample on disk.

### 3.18 E21/E22 — `endpoint_correction`, `node_correction`

`endpoint_correction` = `(analysis_id, endpoint_id)`; `primary_operation`, `confidence double`,
`all_collections text[]`, `cross_module_calls text[]`, `applied_at`.
`node_correction` = `(endpoint_correction_id, node_method_ref)` — note the FK is to `CallNode.id`
(a method signature string), **not** to `node_id`, because `CorrectionResult.NodeCorrection.nodeId`
carries the signature (PLAN-01 §2.17). Fields: `operation_type`, `operation_type_reason`,
`collections_added jsonb` (`{name,source,operation}`), `collections_removed text[]`,
`collections_verified text[]`.

`corrections/_summary.json` is a caller-supplied `Map<String,Object>` whose key set is
**UNKNOWN — needs runtime check** (PLAN-01 §8). Not modelled; it is a rollup of E21 and should be a query.

**Rows @ J:** ≤1,000 endpoint corrections, ~5,000 node corrections.

---

## 4. PL/SQL-side entities

### 4.1 E23 — `plsql_analysis`

**Represents:** one crawl of one database object. Split into a stable **key** and a per-**run** row, per §1.4.

`plsql_analysis_key`:

| Field | Type | Null | Meaning | Real value |
|---|---|---|---|---|
| `key_id` | uuid | no | surrogate | |
| `db_type` | text | no | **the discriminator (§1.5)**. Absent from every artifact today — derivable only from `jdbcUrl`, and the UI parses it client-side in two duplicated places (`history.js:101-109` ≡ `perfSummary.js:13-22`, PLAN-03 §4.3.13) | `ORACLE` |
| `project` | text | no | | `OracleDemo` |
| `environment` | text | no | | `local` |
| `object_name` | text | no | **without** any `_yyyyMMdd_HHmmss` archive suffix (§1.4) | `ERP_USP_RUN_CYCLE` |
| `identity_name` | text | no | generated: `~`-joined, `AnalysisPaths.name():50-58` | `ORACLE~OracleDemo~local~ERP_USP_RUN_CYCLE` |

`plsql_analysis` (one per run):

| Field | Type | Null | Meaning | Real value |
|---|---|---|---|---|
| `analysis_id` | uuid | no | surrogate | |
| `key_id` / `run_id` | uuid | no | | |
| `entry_point` / `entry_schema` | text | yes | | `ERP_USP_RUN_CYCLE` / `ANALYZER` |
| `connection_name` | text | yes | | `oracle-free-conn` |
| `jdbc_url` | text | yes | credential-masked; **the only dialect discriminator in the artifacts today** | `jdbc:oracle:thin:@//localhost:1521/FREEPDB1` |
| `total_nodes` | int | no | | `52` |
| `max_depth` | int | no | | `11` |
| `total_lines_of_code` | int | no | | `18829` |
| `total_flow_lines_of_code` | int | no | | `18824` |
| `total_tables` | int | no | Oracle: **overwritten** with the DB-validated count (`TableValidationService.java:745`) | `41` |
| `total_tables_raw` | int | yes | **Oracle only** (found + not-found) | `42` |
| `db_validated` | boolean | yes | **Oracle only** at root level | `true` |
| `total_edges` / `unique_edges` | int | no | | `2258` / `218` |
| `total_cycles` | int | no | | `3` |
| `total_procedures` / `total_functions` | int | no | | |
| `total_package_procedures` / `_functions` | int | no | **structurally 0 for all four non-Oracle dialects** | |
| `commits` / `rollbacks` / `savepoints` / `autonomous_procs` | int | no | `transactionSummary` | |
| `validated_table_count` | int | yes | **new column.** PLAN-03 §4.3.14: today the UI fetches the whole **307,401 B** tables document *to set one integer* (`parser/js/app.js:74-81`) | |
| `errors` | text[] | no (`[]`) | `[]` in all 5 samples | `[]` |

**Natural key:** `(db_type, project, environment, object_name, run_id)`. This is PLAN-02 §0's conclusion,
plus the `run_id` it recommends: *"a real schema should add an explicit `run_id`/`archived_at` column
rather than reproduce that"*. **Rows @ P: 1.**

Note the *flat* backup layout (`plsql-parse/<OBJECT>__<HASH>/`) and the *nested* current layout
(`plsql-parse/<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>/`) both map onto exactly these four columns — the
`AnalysisPaths` mapping is a "pure, reversible, lossless 1:1" (PLAN-02 §0), so migration from either
layout is mechanical. But note the **Claude sidecar does not nest** (`plsql/<full-identity-name>/`,
`AnalysisPaths.java:163-174`), so nothing may assume one canonical path decomposition (PLAN-03 §2.1).

### 4.2 E24 — `flow_node`

**Represents:** one crawled routine. Fed by `chunks/chunk_NNNN.json` (source of truth, PLAN-02 §3.3) and
its `api/nodes/<safeId>.json` summary (derived, but the primary UI read unit — **L21**, max 370 KB).

| Field | Type | Null | Meaning | Real value (`ANALYZER$ERP_USP_EXPLODE_BOM`) |
|---|---|---|---|---|
| `node_uid` | uuid | no | surrogate | |
| `analysis_id` | uuid → E23 | no | | |
| `node_id` | text | no | `SCHEMA$OBJ`/`SCHEMA$PKG$OBJ`, `…$2` when deduped (`:236-249`) | `ANALYZER$ERP_USP_EXPLODE_BOM` |
| `full_id` | text | yes | `FlowNode.buildFullId()`. **This one column + `node_id` replaces `api/node_id_map.json` entirely** (PLAN-02 §3.11: "do not store; make it two columns") | `ANALYZER$ERP_USP_EXPLODE_BOM` |
| `order` | int | no | crawl visit ordinal == chunk index. **Run-scoped (§1.3)** | `34` |
| `depth` | int | no | | `3` |
| `parent_node_id` | text | yes | first caller in edge order; **explicitly null** for the root | `ANALYZER$ERP_USP_VALIDATE_MASTER_DATA` |
| `schema` | text | yes | | `ANALYZER` |
| `package_name` | text | yes | **null for every non-Oracle sample** (structural) | null |
| `object_name` | text | no | | `ERP_USP_EXPLODE_BOM` |
| `display_name` | text | no | `pkg.obj` or `obj` | `ERP_USP_EXPLODE_BOM` |
| `object_type` | text | yes | `PROCEDURE`/`FUNCTION`/`TRIGGER`/`TABLE` — open | `PROCEDURE` |
| `line_start` / `line_end` / `lines_of_code` | int | no | | `1` / `33` / `33` |
| `call_count` | int | no | note: `2` in `api/index.json` but `0` in `chunks/chunk_0034.json` — see OPEN QUESTION N1 | `2` |
| `readable` | boolean | no (true) | | `true` |
| `message` | text | yes | skip/error reason when `readable=false` (2/52 Oracle) | null |
| `overload_param_count` | int | no (**-1**) | ≥0 only for overloaded package members | `-1` |
| `autonomous_transaction` | boolean | no (false) | `PRAGMA AUTONOMOUS_TRANSACTION` — **Oracle-only semantics** | `false` |
| `trigger_event` / `trigger_table` / `trigger_timing` | text | yes | only when `object_type='TRIGGER'` | null |
| `external_package_var_refs` | text[] | no (`[]`) | `PKG.VAR` — Oracle package-variable concept | `[]` |
| `source_file_id` | uuid → E44 | yes | **→ ArtifactStore** for the body | `ANALYZER.ERP_USP_EXPLODE_BOM.sql` |
| `in_cycle` | boolean | no | | `true` |
| `subtree_tables_count` / `subtree_nodes_count` / `flow_lines_of_code` | int | no | **[denorm]** rollups; see §4.14 note | `3` / `1` / `57` |
| `count_tables` / `_calls_out` / `_internal_calls` / `_external_calls` / `_variables` / `_cursors` / `_parameters` / `_exception_handlers` / `_sequences` | int | no | **[denorm]** `api/index.json.nodes[].counts` — what the sidebar/tree renders (**I9**) | `2`/`2`/`0`/`2`/`1`/`1`/`3`/`1`/`0` |

**Natural key within a run:** `(analysis_id, node_id)`. **Note the caveat** (PLAN-02 §3.3): nodeId
uniqueness is only enforced later, in `writeApiFiles`, so **inside `chunks/` two nodes can share a
`nodeId`** — the ingest must apply the same `…$2` dedup or the unique index will reject a real crawl.
`(analysis_id, order)` is also unique and is the chunk boundary PLAN-00 finding 2 says to reuse verbatim.
**Cross-run identity:** `(key_id, schema, package_name, object_name, overload_param_count)`.

**Rows @ P: 5,000.** Confirmed 1:1:1 with `chunks/` and `api/nodes/` in all five real analyses
(52/52/52, 114/114/114, 64/64/64, 69/69/69, 57/57/57).

> **OPEN QUESTION N1 — `callCount` disagrees between the two writers.** Real data:
> `api/index.json.nodes[].callCount = 2` but `chunks/chunk_0034.json.callCount = 0` for the same node,
> while `calls[]` on the node detail has two entries totalling 4 call sites.
> **Options:** (a) store both as `call_count_parsed` and `call_count_api`; (b) drop both and derive
> `COUNT(*) FROM flow_node_call`; (c) store one and pick the `api/` value for UI compatibility.
> **Recommendation: (b) with (c) as a materialised column.** Derive it, expose it under the `api/` name so
> the UI is unchanged, and record the discrepancy as a parser bug to file separately.

### 4.3 E25 — `flow_node_statement`

`(analysis_id, node_id, seq)`; `type text` (open-ended, §1.5 trap 2), `line`, `line_end`,
`nesting_depth`, `dml_type`, `index_variable`, `collection_name`, `bounds_expression`, `limit_value`,
`error_code`, `error_message`, `savepoint_name`.

**`sqlText` is deliberately absent.** PLAN-02 §3.3 calls it "the single largest text field in the whole
system" and §7.10 shows it duplicates `sources/<file>` verbatim at `line..lineEnd`. The row carries the
span; the text is fetched from the source blob.

> **OPEN QUESTION S1 — is the `line..lineEnd` span always sufficient to recover `sqlText`?**
> The duplication claim is structural, not verified line-by-line, and `sources/` filenames are
> inconsistent (`buildSourceFileName` emits `SCHEMA.OBJ.sql` while disk carries `.prc`/`.fnc`/`.pkb`,
> which is why two readers implement extension-fallback searches — PLAN-02 §3.16).
> **Options:** (a) span only; (b) span + a `sql_text_ref` to a deduped blob; (c) span + inline text for
> statements under N bytes.
> **Recommendation: (a), gated on a verification pass** that reconstructs `sqlText` from the span for
> every statement in all five sample analyses and reports the mismatch rate. If it is non-zero, fall back
> to (b) — never (c), which reintroduces the exact unbounded-inline-text failure mode of `analysis.json`.

**Rows @ P: ~368,000** (3,832 measured over 52 nodes; ×96).

### 4.4 E26 — `flow_node_call`

`(analysis_id, node_id, seq)`; `target`, `package_name`, `object_name`, `schema`,
`type ∈ {INTERNAL, EXTERNAL, BUILTIN}`, `arg_count` (**present only for overloaded targets** —
`ChunkedFlowWriter.java:893`), `call_site_count`, plus child `flow_node_call_line(call_id, line)`.

Real: `{target:"ERP_USP_WRITE_AUDIT_TRAIL", type:"EXTERNAL", schema:"ANALYZER", lines:[11,27,31], count:3}`.
Note `target` gets a `" (N args)"` suffix for overloads (`:872`) — store the clean `object_name` and the
`arg_count`, and derive the display string.

**Rows @ P: ~249,000** (2,586 × 96).

### 4.5–4.9 E27–E31 — the other per-node child arrays

All `(analysis_id, node_id, seq)`, all fed from `chunks/` and re-exposed by `api/nodes/`:

| Entity | Fields | Real example | Rows @ P |
|---|---|---|---|
| E27 `flow_node_cursor` | `name`, `line`, `line_end`, `for_loop`, `ref_cursor`, `query_ref → E2` **or** span, `tables text[]`, `stmt_operations jsonb` | `{name:"(FOR R)", line:16, lineEnd:21, forLoop:true, tables:["ERP_BOM_HEADER","ERP_BOM_COMPONENT"]}` | ~46,000 |
| E28 `flow_node_variable` | `name`, `data_type`, `scope`, `default_value`, `constant`, `line`, `source_package`, **`bucket`** ∈ `TYPE_REF`/`ROWTYPE_REF`/`COLLECTION`/`PLAIN`, `ref_table`, `ref_column`, `ref_field` | `{name:"V_CHILDREN", dataType:"NUMBER", default:":= 0", line:8, constant:false, bucket:PLAIN}` | ~204,000 |
| E29 `flow_node_parameter` | `name`, `data_type`, `direction`, `default_value`, `line` | `{name:"P_DEPTH", dataType:"NUMBER", direction:"IN", default:"DEFAULT 0", line:3}` | ~4,400 |
| E30 `flow_node_exception_handler` | `exception_name`, `line`, `line_end`, `statements_count` | `{exception:"OTHERS", line:30, lineEnd:32, statementsCount:1}` | ~91,000 |
| E31 `flow_node_dynamic_sql` | `type`, `line`, `sql_expression`, `using_variables text[]`, `unresolved_var_name`, `cross_proc_source_hint`, `inferred_operation_type` | — | ~3,000 |

Two normalisation notes:

- **E28's `bucket` collapses `variables.{typeRefs,rowtypeRefs,collections,plain}`** into one table with a
  discriminator. Those four buckets exist because the `%TYPE`/`%ROWTYPE`/`VARRAY`/`INDEX BY` classification
  (`ChunkedFlowWriter.java:746-761`) is **pure Oracle syntax**; for the other four dialects every variable
  is `PLAIN` and `typeRefCount`/`rowtypeRefCount`/`collectionCount` are structurally 0 (PLAN-02 §3.8).
  Four nullable columns and one discriminator, exactly per §1.5 — not four tables, three of which are
  always empty for 4/5 dialects.
- **Two field renames must be normalised at ingest**, or the same fact has two names:
  `parameters[].defaultValue` (chunks) → `default` (api/nodes, `:554`);
  `exceptionHandlers[].exceptionName` (chunks) → `exception` (api/nodes, `:661`).

### 4.10 E32 — `flow_node_statement_summary`

`(analysis_id, node_id, statement_type, count)`. Real: 4 rows for the worked node —
`IF=1, RETURN=1, CURSOR_FOR_LOOP=1, ASSIGNMENT=1`.

**Never an enum** (§1.5 trap 2). **Rows @ P: ~40,000** (~8 distinct types × 5,000 nodes).

### 4.11 E33 — `flow_edge`

**Represents:** one raw call edge, as emitted by `edges/edges_NNNN.json` (source of truth, PLAN-02 §3.4).

`(analysis_id, chunk_idx, from_node_id, to_node_id, line)`; `from_schema`, `from_package`, `from_object`,
`to_schema`, `to_package`, `to_object`, `depth`,
`edge_type` (**omitted for plain call edges**; only `FIRES_TRIGGER` / `TRIGGER_FN`).

Two honest caveats carried from PLAN-02 §3.4: node ids here **may be partial** (no params) and resolution
to `flow_node` is by **prefix match** (`findMatchingNodeId:1236-1242`) — so the FK is *loose* and must be
nullable with the raw string retained. `chunk_idx` is **run-scoped** and the numbering is sparse and
gapped (observed `edges_0001, 0026, 0034, 0035, 0036, 0042…`).

**Rows @ P: ~217,000** (2,258 measured over 52 nodes; ~250 B/edge on disk → `edges.json` reaches 826 KB at
3,295 edges and extrapolates to ~200 MB).

### 4.12 E34 — `flow_edge_dedup` — **deliberate denormalisation**

`(analysis_id, from_node_id, to_node_id)`; `count`, `min_depth`, `back_edge boolean`, `edge_type`,
plus child `flow_edge_dedup_line(edge_id, line)`.

Real: `ANALYZER$ERP_USP_EXPLODE_BOM → ANALYZER$ERP_USP_WRITE_AUDIT_TRAIL`, `lines:[11,27,31]`, `count:3`,
`minDepth:3`, `backEdge:false`. Analysis-level: **2,258 raw → 218 unique → 1 back edge**.

Mostly derivable from E33 — **but not entirely**: `backEdge` and `minDepth` carry cycle-detection results
that `edges/` does not have (PLAN-02 §3.10). So this is part denormalisation, part genuine new
information. Kept because it backs **L23** (call graph), the `/call-tree/{nodeId}` and `/callers`
endpoints, and the graph viewer — a 10× row reduction on the hottest graph read. Note the dedup key is
`fromKey + " -> " + toKey + "|" + fromNodeId + "->" + toNodeId` (`:1138-1151`), which deliberately keeps
same-named cross-schema routines and arity-distinguished overloads separate.

**Rows @ P: ~21,000.**

### 4.13 E35 — `flow_cycle` / `flow_cycle_member`

`(analysis_id, cycle_seq)` + `length`; members `(cycle_id, position, node_id, readable_name)`.
`cycles[]` is **omitted entirely when there are none** (`ChunkedFlowWriter.java:496`); 2–3 observed in
every dialect, 3 in the Oracle sample. **Rows @ P: ~10 cycles, ~40 members.**
`flow_node.in_cycle` is the denormalised boolean the tree view renders.

### 4.14 E36 — `db_object`

**Represents:** one schema object the flow touches. Consolidates `tables.json`, `flow.json.allTables` and
the static half of `api/tables/index.json` — PLAN-02 §7.8: "three representations of the same table-usage
facts, differing only in key structure and enrichment".

| Field | Type | Null | Meaning | Real value |
|---|---|---|---|---|
| `object_id` | uuid | no | surrogate | |
| `analysis_id` | uuid → E23 | no | | |
| `schema` | text | **yes** | **absent when the table key had no dot** — observed 1/85 SQL Server entries | `ANALYZER` |
| `name` | text | no | | `ERP_BOM_COMPONENT` |
| `object_type` | text | no | `TABLE`/`VIEW`/`MATERIALIZED_VIEW`/`SYNONYM`, default `TABLE` | `TABLE` |
| `all_operations` | text[] | no | union of verbs; `CURSOR_REF` added for cursor-only tables. **Open-ended** | `["SELECT"]` |
| `db_found` | boolean | yes | object exists in the live catalog | `true` |
| `db_validated` | boolean | yes | true once an enricher ran | `true` |
| `db_resolved_type` | text | yes | Oracle: real catalog type with spaces→underscores; others only `TABLE`/`VIEW`/`UNKNOWN` | `TABLE` |
| `catalog_captured_at` | timestamptz | yes | **new, required.** The `db*` half is a live-catalog snapshot that "exists nowhere else" (PLAN-02 §3.9) and needs its own timestamp |  |
| `view_definition_ref` | uuid → E2 | yes | **→ ArtifactStore.** Oracle caps at 32,000 chars + `[truncated]`; **the other four never truncate** — so this field is unbounded for 4/5 dialects |  |

**Natural key:** `(analysis_id, schema, name)`. **Rows @ P: ~4,000** (42 observed at 52 nodes; sublinear
because tables are shared, so this is an upper bound rather than 42×96).

**`subtreeTables[]` is not modelled.** PLAN-02 §6 calls it "the worst offender": the entry node's copy
alone reproduces the entire table-usage graph, up to 8 KB per element, O(total tables × total call sites)
→ ~40 MB for a single node document at scale. It is 100 % derivable by a recursive walk over E33 + E37.
The `subtree_*_count` scalars on E24 are kept (§4.2) because the tree view renders them; the array is not.

### 4.15 E37/E38 — `db_object_usage`, `db_object_usage_line`

`db_object_usage` = `(object_id, node_id, operation)` + `node_depth` (**omitted for cursor-derived
entries** — `:1016-1021`, so nullable). `db_object_usage_line` = `(usage_id, line)`.

Real, for `ERP_BOM_COMPONENT`: 10 `usedBy` entries. `ANALYZER$ERP_USP_RUN_CYCLE` at depth 0 has
`SELECT: [137,409,414,430,467,468,717,989,994,1010,1047,1048,1297,1569,1574,1590,1627,1628]` — 18 lines;
`ANALYZER$ERP_USP_EXPLODE_BOM` at depth 3 has `SELECT: [19]`.

Measured over the whole Oracle analysis: **42 tables, 411 usedBy entries, 2,436 usage lines**.
**Rows @ P: ~40,000 usage rows, ~234,000 usage-line rows.**

Splitting lines into their own table is not pedantry: `tables.json`'s `.lines` is a `List<Integer>`, **not
a Set**, and demonstrably **contains duplicates** (line 147 observed twice, PLAN-02 §3.2), while
`api/tables/index.json`'s equivalent is deduplicated and sorted (`:824-832`). One row per
`(usage, line)` with a unique constraint makes the two agree by construction and is what backs
**L20**/**L22** and `txnTrace.js`'s "triple-nested iteration over the 307 KB tables document"
(PLAN-03 §4.3.10).

> **OPEN QUESTION U1 — line-array storage.** 234,000 single-integer rows is a lot of rows for a small
> amount of information.
> **Options:** (a) child table, one row per line; (b) `int[]` column on `db_object_usage`;
> (c) a compressed range encoding.
> **Recommendation: (b) `int[]` for MongoDB, (a) for a relational target.** The only query that reaches
> inside is "show me the lines", which always fetches the whole set for one usage. (a) buys nothing
> queryable and costs 234 K rows; (b) keeps it one document-shaped value. (c) is premature.
> This is the one place where the relational and document targets should legitimately differ.

### 4.16 E39 — `table_join`

`(analysis_id, node_id, query_id, seq)`; `join_type`, `joined_table`, `joined_table_alias`, `left_table`,
`left_table_alias` (**both left fields nullable**, `:810-812`), `condition`, `line`.

Real: `{joinType:"INNER", joinedTable:"ERP_BOM_COMPONENT", joinedTableAlias:"BC",
leftTable:"ERP_BOM_HEADER", leftTableAlias:"BH", condition:"ON bc.bom_id = bh.bom_id", line:19, queryId:18}`.

**Rows @ P: ~94,000** (981 measured × 96). Its own entity because `GET …/joins` currently **scans every
`api/nodes/*.json` on each request** (`NodeScanFacetReader`, PLAN-02 §10) — PLAN-02's own words: "exactly
the queries a relational index would make trivial". Same argument for E27 (`/cursors`) and for the
sequence rollup.

### 4.17 E40 — `db_column` — the dialect matrix, as one table

`(analysis_id, object_id, column_name)`; `db_type` denormalised for dialect-scoped queries.

| Field | Type | Null | Oracle | MySQL | SQLSrv | DB2 | PG |
|---|---|---|---|---|---|---|---|
| `column_id` | int | no | ✔ | ✔ | ✔ | ✔ (`COLNO+1`) | ✔ |
| `data_type` | text | no | ✔ | ✔ | ✔ | ✔ | ✔ |
| `nullable` | boolean | no | ✔ | ✔ | ✔ | ✔ | ✔ |
| `data_length` | int | **yes** | ✔ | conditional | ✔ (**raw bytes**, not chars) | ✔ | ✗ structural |
| `data_precision` | int | **yes** | ✔ | ✔ | ✔ | ✗ | ✗ |
| `data_scale` | int | **yes** | ✔ | ✔ | ✔ | ✔ | ✔ |
| `data_default` | text | **yes** | ✔ | ✔ | ✔ (from default-constraint text) | ✔ | ✔ |
| `computed` | boolean | **yes** | ✗ | ✔ (`EXTRA`) | ✔ (`is_computed`) | ✗ | ✗ |

Real: `{"columnName":"QTY_PER","dataType":"NUMBER","dataLength":22,"nullable":true,"columnId":4,"dataDefault":"1"}`.
Note `data_length: 22` is Oracle's internal NUMBER width, and SQL Server's is raw *bytes* — the units
differ per dialect. Store the raw value; do not normalise units. **Rows @ P: ~15,000** (160 measured).

### 4.18 E41 — `db_constraint`

`(analysis_id, object_id, constraint_name, column_name, position)`; `constraint_type ∈ {P,U,R,C}`
(**Oracle emits the raw code; MySQL is mapped to it; SQL Server, DB2 and Postgres never emit `C` at all**),
`search_condition` (**Oracle only** — CHECK text), and the five `ref_*` columns of §1.5 trap 1.

Real, `ERP_BOM_COMPONENT`: `SYS_C0010526` type `P` on `BOM_COMP_ID`; `FK_BC_BOM` type `R` on `BOM_ID` with
`refConstraint:"SYS_C0010522"` → `ref_constraint_name='SYS_C0010522'`, `ref_raw='SYS_C0010522'`,
`ref_schema`/`ref_table` filled by the Oracle resolution pass; `SYS_C0010524` type `C` with
`searchCondition:"\"BOM_ID\" IS NOT NULL"`.

**Rows @ P: ~10,000** (108 measured).

### 4.19 E42 — `db_index_column`

`(analysis_id, object_id, index_name, position)`; `column_name`, `uniqueness ∈ {UNIQUE, NONUNIQUE}`.
**Same 4 fields in all five dialects** (verified in real data). Real: `IX_BC_BOM` NONUNIQUE on `BOM_ID`
pos 1; `SYS_C0010526` UNIQUE on `BOM_COMP_ID` pos 1. **Rows @ P: ~4,500** (47 measured).

⚠️ **Postgres-only fidelity warning** carried into the model as a column, not a footnote: add
`source ∈ {CATALOG, PARSED_DDL}`. `PostgresTableEnricher.java:242-256` derives `columnName`,
`uniqueness` and a synthetic `position` by **string-parsing `pg_indexes.indexdef`**, splitting on commas
inside the parenthesised column list — expression indexes, quoted identifiers or commas inside function
calls will corrupt these rows (PLAN-02 §3.9). Consumers deserve to know which rows are trustworthy.

### 4.20 E43 — `db_trigger`

`(analysis_id, object_id, trigger_name)`; `owner`, `trigger_type`, `triggering_event`, `status`,
`definition_ref → E2`, and **`provenance ∈ {PARSED, DATABASE}`**.

The provenance column is required because `triggers[]` on a table has **two structurally different
shapes** (PLAN-02 §3.9): parsed trigger nodes give `{nodeId, name, schema, event, timing}`; DB-discovered
ones give `{name, schema, event, timing, triggerType, status, definition?, source:"DATABASE"}`. One has a
node FK, the other has a status. One table, one discriminator, both FKs nullable.

⚠️ `status` **is hardcoded `"ENABLED"` for MySQL, DB2 and Postgres** and only reflects reality for Oracle
and SQL Server. Add `status_is_authoritative boolean` (derived from `db_type`) rather than letting three
dialects lie. **Rows @ P: ~500.**

### 4.21 E44 — `plsql_source_file`

`(analysis_id, file_name)`; `content_ref → E2`, `byte_length`, `line_count`, `content_sha256`.

**The body is an artifact, never a column** (§1.1) — 36 B to 94,331 B per file, 712 KB–1.18 MB per
analysis, and "a large package body is easily 5–10 MB". It is also genuine source of truth: the only copy
of the analysed text.

Model the **filename mismatch explicitly**, because two readers already work around it:
`reference_name` = what `buildSourceFileName:1436-1444` writes into `index.json`/`nodes/*.json`
(`SCHEMA.PKG.OBJ.sql`), `stored_name` = what is actually on disk (observed across the 10 dirs:
**582 `.prc`, 74 `.sql`, 50 `.fnc`, 2 `.pkb`**, with inconsistent casing — `ERP_REF.` vs `erp_ref.`).
Two columns turn an extension-fallback search (`ParserAnalysisReader:93-107`, `CoreFileReader:97-136`)
into an equality lookup. **Rows @ P: ~4,600.**

### 4.22 E45 — resolver snapshot (Oracle only)

- `resolver_object` `(analysis_id, object_name)` → `schema`, `object_type` (default `UNKNOWN`). **85 rows** Oracle, **1** for each other dialect (the entry-point seed written by `SchemaResolver`'s early-return branches, `:123-183` — direct empirical confirmation of the code reading).
- `resolver_dependency` `(analysis_id, from_object, to_object)`. **44 keys** Oracle, `{}` elsewhere. Note Oracle pseudo-objects leak through (`STANDARD`, `SYS_STUB_FOR_PURITY_ANALYSIS`).
- `resolver_ambiguity` `(analysis_id, object_name, seq)` → `schema`, `object_type`. **47 entries** Oracle. ⚠️ candidates are **not deduplicated** — one entry had `ERP_AUDIT_LOG` with 9 identical `{ANALYZER, TABLE}` candidates. Add a unique constraint at ingest and count the collapse.
- **Reverse dependencies: not modelled.** `q3_reverse_dependencies.json` is empty in **all 10** analyses including Oracle. **OPEN QUESTION R1 — bug or genuinely empty?** *Options:* (a) model the table now; (b) omit until a runtime check produces data. **Recommendation: (b)**, and note that the reverse direction is anyway derivable from `resolver_dependency` by swapping the columns, so the table may never be needed.

These endpoints (`GET …/resolver/{type}`) have **no UI caller — currently orphaned** (PLAN-02 §10), which
is an argument for keeping them cheap, not for dropping them: they are a live `ALL_DEPENDENCIES` snapshot
and cannot be recomputed after the fact.

### 4.23 E46 — `connection_scope`

`(db_type, project, environment)` — **the one entity whose grain is the connection, not the analysis**
(PLAN-02 §3.19). Holds the schema-resolver cache (6 TSV files, 40 KB per scope, Oracle-populated only:
`objects.tsv`, `object_types.tsv`, `type_qualified.tsv`, `ambiguous.tsv`, `dependencies.tsv`,
`reverse_deps.tsv` — the last measured **0 B**), plus `last_crawled_at` and a `credential_ref` that is a
**secrets-store id, never a value** (C4; `plsql-env-config.yaml` is deliberately excluded from the shared
store by `CREDENTIAL_FILES`/`rejectCredentialFile`, `AnalysisService.java:71,85-90`).

`POST /api/parser/cache/clear-connection` becomes `DELETE FROM connection_scope_entry WHERE scope_id = ?`.
**Rows: 1 per distinct connection scope, shared by every analysis on it.**

---

## 5. Claude, cross-ref and job entities

### 5.1 E47–E50 — Claude verification

`claude_verification` `(analysis_id)` — or `(key_id, iteration)`; **cardinality is one per analysis
*name*, not per run: runs overwrite** (PLAN-03 §2.2.1). Fields: `claude_iteration` (prev+1, starts at 1 —
**the only run counter that survives today**), `timestamp` (`LocalDateTime.now().toString()` — *local, no
zone*), `confirmed_count` **438**, `removed_count` **0**, `new_count` **7**, `call_confirmed_count` **122**,
`call_removed_count` **16**, `call_new_count` **1**, `total_chunks` **25**, `error_chunks` **0**,
`claude_call_count` **25**, `claude_time_ms` **312569**, `total_tables` **34**, `error` (nullable, and
**also a non-error status carrier**: `"Partial result (3/10 chunks completed)"`).

Children:
- `claude_table_verification` `(verification_id, table_name)` → `overall_status ∈ {CONFIRMED, PARTIAL, EXPANDED, MODIFIED, UNVERIFIED}`, `static_operations text[]`, `schema`/`table` split on the first `.` (`ClaudePersistenceService.java:361-365`), `triggers jsonb` (**nullable, no default**, free-form).
- `claude_operation_verification` `(table_verification_id, seq)` → `operation`, `status ∈ {CONFIRMED, REMOVED, NEW}`, `procedure_name` (**fuzzily matched** against the static node name, `:588-593`), `line_number int` (0 = unknown/whole-op), `reason`, **`user_decision`** (nullable: `ACCEPTED`/`REJECTED`) — the field that drives `applyAcceptedChanges` and the field a cross-stack read **silently drops** (see below). **445 measured.**
- `claude_call_verification` `(verification_id, seq)` → `from_procedure`, `to_procedure`, `status`, `line_number`, `reason`, `user_decision`. **139 measured.**

**`analysis_claude.json` and `claude/_result.json` are byte-identical** — measured **138,301 B each**,
`cmp` returns equal (PLAN-03 §2.2.1, defect 7). One entity, one row set, written once. Quantified in
PLAN-05 §5.

**`analysis_claude_prev.json`** is not a separate entity: it is depth-1 history implemented as a file
move. Model it as `superseded_by` on the row + `analysis_run.is_current` (§2.1), which gives arbitrary
depth for free and stops `revertClaude` from destroying history.

**`tables_merged.json`** is not an entity either: it is "a mutated deep copy of the static
`api/tables/index.json`" (PLAN-03 §2.2.10), deleted on every fresh save. Applying accepted changes should
`UPDATE` E37/E38 and stamp `applied_from_verification_id`, not materialise a second copy of a 307 KB
document.

> **OPEN QUESTION C1 — the two-stack divergence.** `parser/service/` and `service/` write the *same key
> space* with *different field names and DTOs*: `nodeIds` vs `procedures`+`procedureCount`; `tableCount`
> = distinct vs raw; `timestamp` String vs `LocalDateTime`; and the parser stack alone has `calls[]`,
> `userDecision`, `triggers`, `claudeCallCount`, `claudeTimeMs`. `FAIL_ON_UNKNOWN_PROPERTIES` is disabled
> on both, so a cross-read **silently discards user review decisions** (PLAN-03 §2.2.13).
> **Options:** (a) model the union with a `stack` discriminator; (b) pick the parser representation and
> migrate legacy rows; (c) two entity sets.
> **Recommendation: (b).** The parser representation is a strict superset in every field that carries
> user intent. Migrate `procedures` → `nodeIds`, recompute `tableCount` as distinct, and *log* every
> legacy record that had no `userDecision` column so the silent-drop bug becomes visible. (c) is wrong —
> it institutionalises the divergence.

### 5.2 E51 — `claude_chunk`

`(run_id, chunk_id)`; `name` (e.g. `ERP_USP_RUN_CYCLE [L1-501] -> 29 tables`), `node_ids text[]`
(the parser-stack field), `table_count` (**distinct**, `ChunkingUtils.countDistinctTables()`),
`prompt_length int` (chars), `prompt_ref → E2`, `response_ref → E2`, `parsed_response jsonb`,
`summary_text`, `error_text`, `duration_ms`,
**`status ∈ {PENDING, COMPLETE, ERROR}` as a column** — today it is *decided by which files exist*
(`ClaudeChunkReader.java:188-195`, PLAN-03 §4.2 row 14).

**The `.json` extension on `{chunkId}_output.json` lies** (PLAN-03 §2.2.7): `saveFragment` writes the raw
CLI string, and real data begins with a markdown fence. Every reader defends against it, and the chunk
viewer strips the fence **client-side** (`chunkViewer.js:199-206`, PLAN-03 §4.3.21). The model stores
**both**: `response_ref` (the raw bytes, verbatim, for audit) and `parsed_response` (the extracted JSON,
or null with `error_text` set). Parsing happens once at ingest, not on every render.

Measured: 25 chunks/run, `_input` max **29,729 B**, `_output` max **16,202 B**, `_index.json` 4,642 B,
`_checkpoint.json` 293 B. **Rows: ~25 @ P sample, ~2,000 at scale; ~1,000 @ J** (one per endpoint).

`_index.json` is not an entity — it is `SELECT chunk_id, name, node_ids, table_count FROM claude_chunk`.

### 5.3 E52 — `claude_session`

`(session_id)`; `jar_id` / `analysis_id`, `session_type` (`VERIFY`, `CORRECT`, …), `status`,
`started_at`, `finished_at`, `call_count`, `total_duration_ms`.

**This is P1 and P2 — the permanent load.** The full session list is fetched every 30 s on every open
page, and P4 fetches it a *second* time per tick, and both consumers only want the boolean
`sessions.some(s => s.status === 'RUNNING')` (`jar/js/app.js:86`, `summary-corrections.js:405`).
PLAN-03 §1.5.1 and §4.3.1 both call this the single most wasteful recurring call in the suite.

The entity therefore exists primarily so that `status` is an indexed column and
`EXISTS(SELECT 1 FROM claude_session WHERE status='RUNNING')` is a partial-index probe. PLAN-05 adds the
index; PLAN-08 owns the endpoint change.

### 5.4 E54–E56 — cross-reference

`cross_ref_run` `(jar_id, run_id)`; `endpoint_path`, `proc_name`, `schema` (nullable — null when the user
omitted it), `mode ∈ {STATIC_INFERENCE, FULL_COMPARISON}`, `plsql_session_id` (**null in
STATIC_INFERENCE** — an FK to a PL/SQL analysis, §7), the 17 `int` counters of `CrossRefSummary`
(all non-null once set), and the five `claude_overall_*` fields.

`cross_ref_proc_link` `(cross_ref_run_id, seq)`; `java_class`, `java_method`, `java_stereotype`,
`java_operation_type`, `java_node_id`, `oracle_proc_ref`, `inferred_proc_ref`, `resolved_proc_ref`,
`match_confidence ∈ {HIGH, MEDIUM, LOW}`, `plsql_node_found`, `zone_source`, `has_log_parameters`,
`is_low_confidence_hint boolean` (`lowConfidenceHints[]` is the same DTO in a second array — one table,
one flag), `claude_chunk_id`, `claude_verdict ∈ {CONVERTED, PARTIAL, NOT_CONVERTED}`, `claude_confidence`,
`claude_reason`, `claude_gaps text[]`, `claude_done`.
Children: `_collection` `{collection, operation, sequence}`, `_oracle_table`
`{table_name, schema, operation, source_line}`, `_table_match`, `_param_match`. `cursors` and `subCalls`
are `List<Map<String,Object>>` — **free-form, stay `jsonb`**.

The four map fields (`javaToOracleMap`, `oracleToJavaMap`, `tableToJavaMap`, `javaToTableMap`) are
**indexes over `cross_ref_proc_link`**, not data. Not modelled — they are `GROUP BY` results, exactly like
`node_id_map.json` (§4.2).

`cross_ref_chunk` `(cross_ref_run_id, chunk_id)`; `type ∈ {PROC_LINK, JAVA_ONLY, ORACLE_ONLY, OVERALL}`,
`prompt_ref → E2`, `response_ref → E2`, `duration_ms`, `verdict`, `confidence`, `reason`, `gaps text[]`,
`conversion_pct`, `table_analysis jsonb`. `chunkId = stableHash(prefix)` — **content-addressed**, which is
the join key from `procLink.claudeChunkId`.

`result.json` is **rewritten in full per chunk under a lock** (`CrossRefClaudeService.java:1306`) — O(chunks)
full rewrites of a document that grows with `procLinks`. As rows this becomes one `INSERT` per chunk plus
one `UPDATE` of the affected `cross_ref_proc_link`. **All sizes UNKNOWN — no `crossref/` tree exists in
either snapshot** (PLAN-03 §2.3, §6).

### 5.5 E57 — `flow_analysis_run`

`(jar_id, run_id)`; `status ∈ {running, done, error}`, `phase`, `total_endpoints`, `done_endpoints`,
`total_claude_nodes`, `done_claude_nodes`, `current_endpoint`, `current_claude_node` (both `""` not null),
`error`, `mode`, `endpoints text[]`, `high_count`, `med_count`, `low_count`, `dataset_count`,
`results_ref → E2`, `report_ref → E2`.

`status.json` is **rewritten in place on every state change — 10 call sites**. As a row that is 10
column updates. `results.json` holds a **pre-serialised opaque string** whose schema is owned by the flow
analyzer, not the store (**UNKNOWN — needs runtime check**) — so it stays a `storage_ref` until someone
establishes its shape. `runId` is a non-durable in-memory counter today; it becomes `legacy_id` (§1.4).

### 5.6 E59 — `conversion_job`

`(job_id)`; `action ∈ {convert, validate, fix}`, `procedure`, `module`, `project_path` (a local Maven
checkout path, **outside the data root**), `status ∈ {running, done, failed, interrupted}`, `phase`,
`started_at`, `elapsed_ms`, `report_ref`, `instruction_md_ref → E2`, `error`.
`log[]` and `liveLines[]` become E60 (§6).

⚠️ **`jarId` is never persisted** (`ConversionController.java:482-485,542-561`) — along with `service`,
`controller`, `owner`, `batch` — so **there is no durable link from a conversion job back to a JAR**
(PLAN-03 defect 10). The model adds `jar_id uuid NULL → E3` and `subject_kind/subject_id`; existing rows
migrate with null and the defect is fixed by persisting it going forward.

`instructionMd` is immutable and is **shipped on every 1.5 s poll** (P13). Making it a `storage_ref`
removes it from the poll response entirely.

### 5.7 E61 — `chat_session`

`(session_id)`; `name`, `scope ∈ {JAR, PLSQL, GLOBAL}`, `chat_type ∈ {classic, new}` (equals the directory
segment), `scope_context jsonb` (nullable; known keys `analysisName` → E23, `jarName` → E3),
`claude_session_id` (a second UUID, written but not used for resume), `created_at`, `updated_at`,
`message_count` (**new denormalised counter** — L29 renders it per session and today it means reading
every transcript). Messages are E62 (§6).

### 5.8 E64 — prompts

`prompt_template` `(name)` — global, restricted to the 14 `KNOWN_TEMPLATES`; `body_ref → E2`
(**text, not a column**: largest measured `java-both-correction.txt` **5,579 B**, smallest 588 B),
`is_default boolean`, `updated_at`.
`prompt_override` `(scope_kind, scope_id, template_name)` → per-analysis override body; plus a global
singleton row for `prompt-extras.json` (`globalAppend`, `globalContext`, both never null).

Precedence at read is per-analysis override → `prompts/{name}.txt` → classpath default
(`PromptConfigService.java:167-200`) — three levels, which is `ORDER BY scope_kind` with a `LIMIT 1`.

⚠️ Two directories hold **overlapping but non-identical** template sets: `config/prompts/` has
`plsql-postgres-verification.txt`; `prompts/` has four other `plsql-*-verification.txt` files it lacks
(PLAN-03 defect 8). Migration must union them and flag the conflicts, not pick one directory.

Real content of `prompt-analysis/ERP_USP_RUN_CYCLE__WL2ZTC.json` (58 B): `{"context":"","append":"","templates":{}}`
— i.e. the empty default is persisted. The model should **not** insert rows for empty overrides.

### 5.9 E65 — `job_queue_entry`

`(job_id)`; `kind`, `subject_id`, `state`, `enqueued_at`, `started_at`, `finished_at`, `progress jsonb`.
Backs **P7** (3 s, home page, always on), **P8** (2 s hard-coded), **P9** (2 s per job). Today the whole
queue — current + queued[] + history[] — is shipped every 2–3 s and filtered client-side
(`perfSummary.js:430-446`; `plsql/js/claude.js:259-261` ships the whole queue "to show ≤3 rows").
As rows these are `WHERE state IN (…) LIMIT 3`.

---

## 6. The six append-only artifacts — one document per entry

PLAN-03 §2.9 is unambiguous: six artifacts are append-only in content and **every one is implemented as
read-everything-then-rewrite-everything**, because `ArtifactStore` has no append primitive (documented at
`ClaudeCallLogger.java:87-95` and again at `FlowRunStore.java:137-138`). Cost is **O(n²) bytes written**
per run. Worse, four of them are also **hot read paths**: P11 re-reads the entire Claude call log every
5 s; P13 re-reads the whole conversion job every 1.5 s.

One entity per entry, with an explicit, monotonic entry key:

| Entity | Parent | **Entry key** | Ordering field | Rotation today | Rotation proposed |
|---|---|---|---|---|---|
| E53 `claude_call_log_entry` | `(scope_kind, scope_id)` — analysis or jar | **`(scope_id, seq)`**, `seq` a per-scope monotonic bigint | `seq`, tiebreak `ts` | **none** | TTL 90 d + `MAX_ENTRIES` per scope |
| E58 `flow_analysis_log_entry` | `flow_analysis_run` | **`(run_id, seq)`** | `seq` | **none** | keep last 10,000/run |
| E60 `conversion_job_log_entry` | `conversion_job` | **`(job_id, stream, seq)`**, `stream ∈ {LOG, LIVE}` | `seq` | `LOG` **uncapped**; `LIVE` FIFO 2,000 | keep both, cap `LOG` at 50,000 |
| E62 `chat_message` | `chat_session` | **`(session_id, seq)`** | `seq` | none (prompt windowed to 20, storage not) | none — transcripts are user data |
| E63 `checkpoint_entry` | run | **`(run_id, chunk_id)`** — *not* a seq; it is a set | insertion order irrelevant | none | delete with the run |
| — `plsql/improvement/run_*.md` | run | `(run_id)` → `report_ref → E2` | — | **none, forever** | TTL, `report_ref` only |

**Why `seq` and not the timestamp.** `ts` is `LocalDateTime.now().toString()` — *local, no zone*
(measured `"2026-07-21T22:50:18.561829"`), it is not unique under concurrency, and `_checkpoint.json` is
written **from concurrent chunk threads** guarded only by a process-local `synchronized`. A monotonic
per-parent `seq` allocated at insert is the only thing that gives a total order and therefore the only
thing that makes `?sinceSeq=` correct. That cursor is what removes **P11, P13 and P14** from the hot path
entirely (PLAN-03 §2.9).

**Why `checkpoint_entry` is keyed differently.** It is not a log, it is a *set of completed chunk ids*
(a bare JSON array, measured 293 B / 25 entries, "unordered — written from a `synchronized` Set"). The
key is `(run_id, chunk_id)` with a unique constraint; an insert is idempotent, which is exactly the
resume semantics the current code approximates with a full rewrite. Arguably it should not exist at all —
it is `SELECT chunk_id FROM claude_chunk WHERE run_id=? AND status='COMPLETE'`. **Recommendation:** derive
it, and keep the table only if a runtime check shows a chunk can be checkpointed without a `COMPLETE`
status row.

`claude_call_log_entry` fields (all measured present in the real 6,880 B / 25-line file), with the
misnomers fixed and the old names kept as aliases so PLAN-07 can serve the legacy JSON unchanged:
`ts`, `session_id` (nullable UUID, minted per chunk — **one line per CLI call including retries**, so a
failed-then-retried chunk yields two lines with the same `session_id`), `session_type` (`VERIFY`),
`chunk_name` (was `endpoint` — **a misnomer; it holds the chunk name, not a URL**),
`chunk_index` (the 1-based `processed` counter, **not** the chunk id), `prompt_chars` (was `promptBytes` —
chars, not bytes), `response_chars` (was `responseBytes`), `duration_ms` (measured 30,292),
`success boolean`, `error` (nullable).
**Add `chunk_id`** — its absence is why joining a log line to `{chunkId}_output.json` currently has to go
via the chunk *name* (PLAN-03 §2.2.9, "join problem").

`chat_message`: `(session_id, seq)`, `role ∈ {user, assistant}`, `content` (free text, unbounded →
`content_ref → E2` above a threshold), `timestamp`. Two rows per turn. Chat is the one case where the
parent is also read whole (**L30**), so it needs per-message rows **and** a `?limit=/?before=` read path;
the UI has neither today.

---

## 7. Where the two sides genuinely unify — and where they must not

This is the question the model has to answer honestly, because the temptation is to build one grand
"database object" entity and pretend the JAR analyser and the PL/SQL crawler are describing the same
thing. Mostly they are not.

### 7.1 Genuine unification #1 — `analysis_run` and `storage_ref`

Provenance and blob-addressing are the same problem on both sides: a run has a start, an end, a status, a
duration, an archive timestamp and a "which one is current" question, and a blob has a key, a length and
a hash. Both sides implement all of this today and both implement it badly and differently (PL/SQL: a
directory-name suffix; cross-ref: epoch-ms; flow-analysis: a counter that resets on restart). One spine,
five kinds. **Unify.**

### 7.2 Genuine unification #2 — a DB table referenced from Java *and* analysed by the crawler

This is the real one, and it is not hypothetical — it is visible in the sample data:

- **JAR side:** endpoint `GET /api/v1/reports/sales/monthly/{year}`, `call_node` ordinal 2
  (`OrderJdbcRepository.getMonthlySalesReport`) has `collectionsAccessed = ["ORDER_DATE","ENTERPRISE.ORDERS"]`,
  `operationType = READ`, `source = JDBC_TEMPLATE`, and an inline SQL statement whose text is
  `… FROM ENTERPRISE.ORDERS WHERE …`. `connection_info` for that jar records
  `jdbc:oracle:thin:@//localhost:1521/XEPDB1`.
- **PL/SQL side:** `db_object` `(ANALYZER, ERP_BOM_COMPONENT)` in the Oracle analysis, with 10 `usedBy`
  entries, 4 catalog columns, 5 constraints and 2 indexes, on
  `jdbc:oracle:thin:@//localhost:1521/FREEPDB1`.

These are the **same kind of thing**: a named object in a named schema on a named connection. The correct
unification is a **`schema_object_ref`** — `(connection_scope_id, schema, object_name)` — that both
`call_node_collection.collection_name` and `db_object` resolve to, *when the connection matches*.

Two things make it work and both are already in the model:

1. `connection_scope` (E46) is keyed `(db_type, project, environment)` and `connection_info` (E18) records
   the jar's JDBC URLs, so the match is `jdbc_url` → scope, not a string guess.
2. `call_node.oracle_proc_ref` (`PROC:PKG.NAME` / `FUNC:PKG.NAME`, from `@LogParameters`) and
   `endpoint.proc_names` give a **direct Java-method → PL/SQL-routine** edge, which is exactly the join
   `cross_ref_proc_link` (E55) is doing by hand today — `resolvedProcRef` matched against
   `plsqlSessionId`'s node set. Cross-ref stops being a bespoke pipeline that re-reads two artifact trees
   and becomes a query over E10/E11 ⋈ E24/E37.

**Caveat, stated because it is load-bearing:** the two sides' names do not always match. The JAR side
emits `ENTERPRISE.ORDERS` (schema-qualified) *and* `ORDER_DATE` (a bare column name, mis-detected as a
collection) in the same node. `schema_object_ref` resolution must therefore be **best-effort and
recorded**: a `match_kind ∈ {EXACT, SCHEMA_INFERRED, NAME_ONLY, UNMATCHED}` column, not a hard FK. The
`ORDER_DATE` row is `UNMATCHED` and should stay visible as such rather than being silently dropped or
silently joined to something.

### 7.3 Where forcing unification would be wrong

| Tempting unification | Why it is wrong |
|---|---|
| `java_class` + `flow_node` → one "code unit" | Their child sets barely overlap: `java_method`/`java_invocation`/`java_field` vs `statements`/`cursors`/`variables`/`parameters`/`exception_handlers`/`table_operations`. A unified table would be ~40 columns of which ~35 are null on any given row. The only shared fields are name, source location and LOC. |
| `call_node` + `flow_node` → one "call graph node" | Different topology and different identity. `call_node` is a **position in a materialised tree**, keyed `(endpoint_id, ordinal)`, and the *same method appears many times* under different callers (PLAN-01 §2.1: "children is a tree, not a DAG"). `flow_node` is a **vertex in a DAG**, keyed `(analysis_id, node_id)`, at most once. Merging them means one of the two loses its identity model. |
| `endpoint` + `flow_node(entry point)` | An endpoint is an HTTP contract; an entry point is a routine. They connect through `oracle_proc_ref` (§7.2), which is a *relationship*, not an identity. |
| `db_column`/`db_constraint` shared with the JAR side | The JAR side has no catalog snapshot at all — `mongo-catalog.json` is dead (§1.2). There is nothing to unify with. |
| `claude_chunk` (PL/SQL verify) + `cross_ref_chunk` | Superficially identical (prompt, response, verdict, content-addressed id) — but the *content* of `verdict` differs (`CONFIRMED/REMOVED/NEW` vs `CONVERTED/PARTIAL/NOT_CONVERTED`), the parent differs, and the chunk-id hash inputs differ. **Borderline.** *Recommendation:* keep separate tables but give them the same column names and the same `prompt_ref`/`response_ref` convention, so a future union view is trivial and neither is contorted now. |
| One `analysis` table for E4 and E23 | Their natural keys are genuinely different — `jar_id` vs a 4-tuple — and their counters share almost nothing (`totalClasses`/`totalEndpoints` vs `totalNodes`/`totalEdges`/`totalTables`/`maxDepth`). `analysis_run` already gives them the shared spine; a shared *analysis* table would give them only shared nulls. |

The honest summary: **the two sides unify at the run, at the blob, and at the database object — and
nowhere else.** Everything between those three points is genuinely different subject matter, and the
schema is clearer for saying so.

---

## 8. Open questions, collected

| # | Question | Options | Recommendation |
|---|---|---|---|
| F1 | Do call-tree fragments need to exist? (§3.11) | (a) model them (b) flatten at ingest | **(b)** — they are a storage artefact; keep `fragment_ref` for round-trip only |
| N1 | `callCount` = 2 (`api/`) vs 0 (`chunks/`) for the same node (§4.2) | (a) both (b) derive (c) pick one | **(b)** + expose under the `api/` name; file the parser discrepancy |
| S1 | Is `line..lineEnd` always enough to recover `sqlText`? (§4.3) | (a) span only (b) span + blob ref (c) inline under N bytes | **(a) gated on a reconstruction pass** over all 5 samples; fall back to (b); never (c) |
| U1 | Usage line arrays: rows or `int[]`? (§4.15) | (a) child rows (b) `int[]` (c) range encoding | **(b) for Mongo, (a) for SQL** — the only legitimate divergence between the two targets |
| R1 | Model reverse dependencies? (§4.22) | (a) now (b) after a runtime check | **(b)** — empty in all 10 analyses; derivable from the forward table anyway |
| C1 | Two Claude stacks, two field vocabularies (§5.1) | (a) union + discriminator (b) adopt parser, migrate legacy (c) keep both | **(b)** — parser is a strict superset for user intent; log every silently-dropped `userDecision` |
| T1 | Every timestamp in the system is `LocalDateTime.now().toString()` — local, **no zone** (`analyzedAt`, `timestamp`, `ts`, `lastUpdated`) | (a) store as `timestamp` and preserve ambiguity (b) store `timestamptz`, assuming server-local at migration (c) store both | **(b) + a `tz_assumed boolean`** on migrated rows. Ambiguous timestamps in a shared database are a correctness bug, not a cosmetic one; but flag which rows were guessed. |
| D1 | Should `subtree_*_count` (§4.2) and `endpoint_collection` (§3.10) be maintained on write, or computed on read? | (a) triggers (b) recompute at end of run (c) read-time | **(b)** — the run is the only writer, and it already computes them. PLAN-05 §4 quantifies the cost. |
| X1 | Sizes for `crossref/**`, `flowanalysis/**`, `conversion/*`, `claude-chatbot/**`, `external-links*` are **UNKNOWN** — none exist in either snapshot (PLAN-03 §6) | — | **Runtime check before PLAN-06 sets chunk boundaries.** Their *shapes* are code-derived and reliable; only the cardinalities are guesses. |

---

## 9. What this model deletes

Stated plainly, because it is the point. Not modelled, and why:

| Artifact | Reason | Bytes recovered (per analysis) |
|---|---|---|
| `flow.json` | 100 % derived from `chunks/`+`edges/`+`tables.json`, **and read by nothing** | **2.27–3.30 MB** |
| `edges.json` | byte-for-byte concatenation of `edges/`, read by nothing | 75–826 KB |
| `.node-cache/` | identical `FlowNode` under a second name | 2.4 MB |
| `.parse-cache/` | pure content-addressed cache | 1.6 MB |
| `api/node_id_map.json` | two columns on `flow_node` | ~9 KB |
| `subtreeTables[]` / `directTables[]` | derived; `directTables` is literally the same call twice (`:568-569`) | up to 40 MB/node at scale |
| `progress.json` | crawl state, worthless after completion; rewritten in full on **every node transition** | 1.5–4 KB × N writes |
| `_class-tree.json`, `mongo-catalog.json` | dead (§1.2) | — |
| `analysis_claude.json` **or** `claude/_result.json` | byte-identical duplicates | **138,301 B** |
| `tables_merged.json` | mutated copy of a 307 KB document | ~307 KB |
| `_header.json`, `_summary*.json` | pure caches of E4/E5/E9/E12 | 191 B + 430 KB |
| `endpoints/**/{endpoint,call-tree,nodes,nodes/*}.json` | four copies of one tree (PLAN-01 §6.2) | 5.4 MB observed; the ~1 GB driver at scale |
| `CallNode.sourceCode` inline | → `ArtifactStore` by ref | **2,050 B × node count**, ×4 copies |

PLAN-05 §5 turns these into a single quantified savings table and decides, for each, whether the saving
comes from normalisation, from a deliberate retention choice, or simply from the artifact being dead.
