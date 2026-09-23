# PLAN-09 — Migration and rollout

How we get from "JSON blobs in `local`/`s3`" to "entities in MongoDB" without losing data and without
downtime, and how we get back if it goes wrong.

This document owns **C5** — existing local and S3 data stays readable throughout, and rollback is a
property change, not a restore from backup. It also owns the honest list of data that **cannot** be
migrated, because pretending otherwise is how a migration silently loses information.

Two numbers shape every decision here and should be read before the increment table:

- **~0.5 MB/s** measured uplink to the real bucket (`amp-analyzer`). One 70 MB `stored.jar` is
  **9+ minutes** of upload. Any design that re-uploads existing artifacts is dead on arrival.
- **912 MB** — a real `analysis.json`, still growing (PLAN-01). Nothing may be parsed whole.

Together they force the central design decision of this plan:

> **The backfill moves *structure*, not *bytes*.** Blob artifacts (`stored.jar`, decompiled source,
> raw `sources/*.sql`, exports) are **left exactly where they are** and referenced by their existing
> `ArtifactKey`. Only the queryable entities — the things PLAN-04/05 model as rows — are read,
> parsed and inserted into Mongo. A backfill of a 912 MB analysis therefore transfers **tens of MB of
> JSON structure down**, and **zero bytes up**.

---

## 1 — The increments (read this first)

Each increment is independently shippable, independently valuable, and independently revertible.
"Delivers to a user" is the acceptance question; an increment that only improves the codebase is
folded into the one that uses it.

| # | ships | delivers to a user | revert |
|---|---|---|---|
| **A** | **UI pagination contract** (PLAN-08): `page`/`size`/`cursor` on the 20+ list endpoints, served from the *existing* JSON, plus paged tables in the UI | First real page-load win: the JAR class list and PL/SQL table list stop shipping whole arrays. **No database involved.** | revert the commit; endpoints ignore the params |
| **B** | Drop the two dead artifacts (`_class-tree*.json`, `mongo-catalog.json`) and stop *reading* `flow.json` / `edges.json`; keep writing them | Faster analyses, smaller output trees. ~500 MB/analysis avoided at worst case | re-enable readers |
| **C** | `AnalysisRepository` interface + the **local/s3 implementation over today's JSON** | Nothing visible. But every caller that will later query Mongo is now on the new interface, in production, against unchanged data | interface unused; delete |
| **D** | `mongodb` storage mode: `MongoArtifactStore` + `MongoAnalysisRepository`, **new analyses only**, behind `app.storage.mode=mongodb` | A user can run a *new* analysis into Mongo and get sub-second class/endpoint/table lists at any size | set `app.storage.mode` back to `s3` |
| **E** | **Backfill migrator** (`migrate` CLI), structure-only, restartable | Historical analyses become queryable at the same speed as new ones. Old JSON untouched | drop the Mongo database; nothing else changed |
| **F** | **Dual-read**: `app.storage.mode=s3` + `app.storage.repository=mongo-then-artifact-scan` | Users stop noticing which analyses are migrated — every screen is fast where data is migrated and correct where it is not. Writes still go to S3, so this is reversible with zero loss | set `repository=artifact-scan` |
| **G** | Cutover per environment + append-only collections with retention (TTL) | Chat history and Claude call logs stop being O(n²) rewrites; unbounded run logs get a policy | flip mode back |
| **H** | Retire legacy writes; drop `flow.json` / `edges.json` writers; delete migrated JSON **only after the retention window** | Storage cost drops; the duplication PLAN-01 measured is finally gone | **this is the point of no return** — see §6 |

> ### VERIFIED CORRECTION — Increment B as written is now wrong on three of its four items
>
> Checked against the code after Increment A, not against the inventory that predated it.
>
> | B's item | actual state |
> |---|---|
> | drop `_class-tree*.json` (dead) | **NO LONGER DEAD.** `AnalyzerController:940-947` now *writes* it as the paging cache for `/classes/tree`, so page 2 does not re-stream ~3.9 MB of `analysis.json`. Dropping it would undo a working optimisation. `PersistenceService:356` still deletes it on invalidation, which is correct. |
> | drop `mongo-catalog.json` (dead) | still never written (`MongoCatalogService.fetchCatalog` returns null). Correct — but there is nothing to *stop*, so the saving is zero. It only means "do not model it". |
> | stop reading `edges.json` | **IT IS READ.** `unified-web/.../web/service/PersistenceService:1027` loads call-graph edges from it, reachable via the `api/index.json` + `section == GRAPH` branch. PLAN-02's "nothing in unified-web reads it" is wrong. Stopping the read would silently empty the call graph for api-export-format analyses. |
> | stop reading `flow.json` | **correct, and it is the only real item.** Written by `ChunkedFlowWriter.mergeChunks` (via `DependencyCrawler:760`); no reader anywhere. 3.3 MB observed, ~300 MB extrapolated. |
>
> **So Increment B reduces to one action:** stop *writing* `flow.json`. B deliberately kept writing
> it for revertibility, but for an artifact nothing reads, writing it is pure cost — the honest
> version is a property (default off) so the write can be restored without a code change, plus a
> logged counter if anything ever reads it.
>
> Lesson for the remaining increments: **re-verify each one against the code before executing it.**
> Increment A changed the codebase underneath this plan in ways the plan cannot know.

**Ordering constraint stated plainly:** **A is a prerequisite for everything.** PLAN-03 found *no*
endpoint accepts `page`, `offset`, `limit` or a cursor — all 20+ tables fetch a full array and slice
client-side. Until that changes, a paged Mongo query is converted straight back into a full array by
the controller and the user's page load is **exactly as slow as today**. D–H reduce storage cost and
write cost; only A reduces page-load cost. Shipping D before A produces a migration that is correctly
built and delivers nothing anyone can see — the fastest way to lose sponsorship for the rest.

The other non-database prerequisites:

- **A live-bucket smoke test for `s3` mode** is still unrun (storage-check OPEN-ITEMS: real SigV4,
  throttling, multi-GB objects, `_prev` rotation under concurrency all unverified). The migrator
  reads from that bucket at volume; it will be the first thing to exercise those paths. Run the
  `s3-mode` skill §5 checklist **before** E, not during it.
- **A MongoDB instance with a replica set decision.** PLAN-06 deliberately avoids multi-document
  transactions and uses the run-marker scheme instead, so a standalone server is *sufficient* — but
  change streams, if we ever want them for divergence detection, are not.
- **Operator runbook + a `migrate` entry point that is not the web app.** Backfill must not run
  inside the request path.

---

> ### VERIFIED CORRECTION — Increment E cannot be executed as written
>
> E says: read the existing JSON, **map it to entities**, insert, restartable. Checked against the
> code after Increments A–D:
>
> - **No entity mapper exists.** Nothing implements PLAN-04's 65 entities or PLAN-05's 71 tables.
> - **`MongoAnalysisRepository` stores each row as an opaque JSON payload**, deliberately: Increment
>   C's acceptance criterion was that no response change byte for byte, and a round trip through
>   typed records is exactly where field order and absent-vs-null shift.
> - **Ingest is lazy, on first touch, from the scan implementation** — so "backfill" in the sense of
>   moving bytes is already unnecessary.
>
> So there is nothing for a migrator to map *to*. Writing one now would move opaque payloads from
> one place to another and add no queryability.
>
> **What E actually is:** implement the entity mapper — PLAN-04/05 and PLAN-11's field rules — as
> real collections with real indexes. That is the step that makes MongoDB worth having over S3,
> because today it answers `document(key)` and cannot answer *"which endpoints call table X"*.
>
> The pre-warm idea (eagerly ingest existing analyses so the first query is not slow) is real but
> minor, and belongs after the mapper, not instead of it.
>
> This is the sixth plan step invalidated by verifying against the code first. The plan was written
> from a static inventory; A–D moved the ground under it.

> ### VERIFIED CORRECTION — Increment F is already built; only one combination needed proving
>
> F proposes `repository=mongo-then-artifact-scan` as a new dual-read mode. Checked against the code:
> **`MongoAnalysisRepository` already takes the scan implementation as its `source`** and answers
> from it for any run not yet ingested, writing rows to Mongo as they stream past. Dual-read is the
> construction, not a mode — a third enum value would be a second spelling of the same behaviour.
>
> What genuinely needed proving was the combination PLAN-07 §5.5 calls the shape people actually
> want: **blobs in one store, entities in another**. Increment D noted the code path existed but had
> never been run.
>
> **Verified** (`mode=local` + `repository=mongo`, real MongoDB, real analysis):
>
> | | result |
> |---|---|
> | `artifacts` collection in Mongo | **0** — blobs stayed on local disk |
> | `table_usage` / `flow_node` in Mongo | **7,861 / 114** — entities went to Mongo |
> | entity query | HTTP 200 |
> | `\bERROR\b` in the log | 0 |
>
> The store and the repository are independent beans, so this is a config combination rather than a
> code path — which is why it worked first time.
>
> **Still unproven: `mode=s3` + `repository=mongo`.** Not because of the wiring — that is the same two
> beans — but because the AWS STS credentials expired mid-session and could not be renewed here. The
> S3 store itself is separately proven (25/25 conformance; a real bucket round trip with a 47.84 MB
> object). What has never run is the two together. It needs one live smoke test with valid
> credentials.
>
> Seventh plan step invalidated by verifying against the code first.

## 2 — Backfill

### 2.1 What is in scope

Only what PLAN-06's decision table marks **SPLIT** or **DOC**. Everything marked **ARTIFACT** is
*already* in an `ArtifactStore` and stays there; everything marked **DROP** is not migrated at all.

| PLAN-06 decision | backfill action |
|---|---|
| **DROP (dead)** — `_class-tree*.json`, `mongo-catalog.json` | ignore. Do not read, do not count as a discrepancy |
| **DROP (duplicate)** — `flow.json`, `edges.json`, `endpoints/*/call-tree.json`, `endpoints/*/nodes.json` | ignore. These are the ~500 MB that make backfill cheap. Their content is reconstructed from `chunks/` + `edges/` + `nodes/node_*.json`, which *are* migrated |
| **DROP (derived)** — `_summary*.json`, `.parse-cache/*`, `.node-cache/*` | ignore. Recomputed by query |
| **ARTIFACT** — `stored.jar`, `sources/*`, embedded `sourceCode`, exports, resolver TSVs | **not moved.** The entity row stores the existing `ArtifactKey` verbatim. In `s3` deployments the object never leaves the bucket |
| **SPLIT / DOC** | read, map, insert |

The one nuance: embedded `sourceCode` inside `analysis.json` is *not* yet an artifact — it lives
inline in the JSON. Backfill must **extract** it. It is extracted to
`jar/<id>/src/<sha256>` (PLAN-06's content-addressed scheme) — and because content addressing
deduplicates identical method bodies, generated code collapses hard. Crucially, in `s3` mode this
extraction is a **new upload**, which is the *only* place backfill puts bytes on the 0.5 MB/s wire.
See §2.5 for why that is still bounded, and the OPEN QUESTION about deferring it.

### 2.2 How it reads — streaming, never whole

A 912 MB `analysis.json` cannot be `objectMapper.readValue(...)`-ed into a tree. The migrator uses
**Jackson streaming (`JsonParser`) with a state machine**, not `readTree`, not `readValue` on the
root:

1. `ArtifactStore.read(key)` gives an `InputStream`. For S3 that is a streaming GET; for local a
   file stream. **Never** `materialize()` a 912 MB object to a temp file first — that is a full
   download plus a full disk write for no benefit.
2. Advance to a known top-level field (`classes`, `endpoints`, `nodes`), enter the array, and then
   `readValueAs(ClassDto.class)` **one element at a time**. Peak heap is one element, not one file.
   Measured element sizes make this safe: `node_<n>.json` averages 2.05 KB / max 6.2 KB;
   `api/nodes/<safeId>.json` maxes at 370 KB.
3. Elements accumulate into a batch buffer sized by *bytes*, not count — target ~4 MB per bulk write,
   flushed early if the buffer would exceed it. A count-based batch of 1,000 works fine for 2 KB
   nodes and blows up on 370 KB nodes.
4. The **12 MB soft limit guard** from PLAN-06 §"size guard" applies on the write path, so the
   migrator inherits the oversized-element diversion rule for free — it does not reimplement it.
5. Per-node/per-chunk artifacts (`chunks/chunk_NNNN.json`, `edges/edges_NNNN.json`) are already one
   element per file and are read whole; the only care needed is that `list(prefix)` is consumed
   **lazily** and sorted by leaf name, and that it tolerates **sparse, gapped numbering** — PLAN-02
   observed MySQL `edges_0001, 0003…0013, 0016, …` with 2, 14, 15 absent. A migrator that assumes
   contiguous ordinals will silently stop at the first gap.

**Hard rule, enforced by review and by a test:** the migrator module may not call
`readTree`, `readValue(InputStream, X.class)` on an analysis root, `readAllBytes`, or `materialize()`
on an analysis JSON key. A unit test running the migrator against a synthetic 2 GB stream under a
constrained heap (`-Xmx512m`) is the assertion.

### 2.3 How it maps to entities

The migrator is a **second producer** for the same repository write path the analyzer uses — not a
parallel, hand-rolled insert path. This matters: if the migrator has its own mapping code, the
migrated rows and the freshly-analysed rows will diverge in exactly the ways nobody tests.

```
ArtifactStore (unchanged)  ->  streaming reader  ->  DTOs (existing model classes)
                                                        |
                                       the SAME mapper the analyzer uses
                                                        |
                                              AnalysisRepository.write*   (12 MB guard, bulk, run marker)
```

**Identity is PLAN-05's scheme, unchanged.** Each migrated analysis becomes one `analysis_run` row
with a **UUIDv7** `run_id` under an existing `analysis_key` — for PL/SQL,
`(db_type, project, environment, object_name)`, e.g.
`identity_name = 'ORACLE~OracleDemo~local~ERP_USP_RUN_CYCLE'`; for JAR, `subject_kind='JAR_ARTIFACT'`
with `subject_id = jar_id`. The migrator does **not** invent an identity scheme.

Two migration-specific rules on top:

- **The `run_id` is allocated once and recorded in the ledger before any child row is written.** A
  restart reuses the ledger's `run_id` rather than minting a new one, so restart is idempotent
  without needing a content-derived id. (A deterministic hash would not work anyway: PLAN-02 shows
  embedded timing fields — `crawlTimeMs`, `parseTimeMs`, `totalDurationMs`, `dbCallCount` — make
  every run's bytes differ, which is why those fields are split into `run_metrics` in the first
  place.)
- **`is_current` is set last, and only once.** The run marker in this schema is the partial unique
  index `uq_run_current ON analysis_run (subject_kind, subject_id, kind) WHERE is_current`. The
  migrator inserts with `is_current=false`, and promotes with the atomic cutover UPDATE pair
  (§7.2 of PLAN-05) only after the count checks in §7.2 below pass. **A partially migrated analysis
  can never be the current one** — this is the same guarantee PLAN-06's `status=IN_PROGRESS` marker
  gives, expressed in the schema that actually exists.

The archived `_20260723_HHMMSS` twins are migrated as **separate runs** with `is_current=false` and
`legacy_id='20260723_153725'`, not as copies, because PLAN-02 §10.2 established they are independent
re-executions (`dbCallCount` **259 vs 263** for the Oracle `WL2ZTC` pair). Marking them as archives of
their siblings would assert an equivalence that is false. The `_20260723_153725` suffix is a *run*,
not part of the object name, and must not leak into `object_name`.

Legacy identifiers that are not stable keys are preserved as `legacy_id` columns rather than
discarded: the epoch-ms `crossRefId` (`CrossRefService.java:139`) and the in-memory flow-analysis
`runId` (`FlowAnalysisController.java:117`, not durable across restarts). Anything a user could have
bookmarked stays resolvable.

Timing telemetry (`crawlTimeMs`, `parseTimeMs`, `totalDurationMs`, `dbCallCount`) is split into a
separate `run_metrics` record, per PLAN-02's schema consequence #1, so the analytical payload can be
hashed and deduped.

### 2.4 Restartability — failing at 80%

The requirement is that a migrator killed at 80% resumes at 80%, not at 0%, and never double-inserts.
Three mechanisms, all of which already exist in the design:

**(a) The run marker is the unit of atomicity.** PLAN-06 already defines it: insert the `run` document
with `status=IN_PROGRESS`, flip to `COMPLETE` after the last child is acknowledged, and **every read
filters on `status=COMPLETE`**. A migrator killed mid-analysis leaves an `IN_PROGRESS` run that is
invisible to the UI — not a half-populated analysis that looks complete. This is strictly better than
what object storage does today, where a half-written analysis is indistinguishable from a whole one.

**(b) A `migration_ledger` collection, one document per source analysis.**

| field | meaning |
|---|---|
| `source_key` | the `ArtifactKey` prefix of the source analysis — the primary key |
| `source_mode` | `local` \| `s3` |
| `source_size`, `source_last_modified` | change detection |
| `run_id` | the deterministic id from §2.3 |
| `state` | `PENDING` → `IN_PROGRESS` → `COMPLETE` \| `FAILED` \| `SKIPPED` |
| `cursor` | last completed *sub-unit* (see (c)) |
| `counts` | per-collection inserted counts, for §7 reconciliation |
| `error`, `attempts` | why it failed and how often |

Restart = "select where `state != COMPLETE`". Resume is analysis-granular by default, which for the
common case (thousands of small analyses) is entirely sufficient.

**(c) Sub-analysis cursors for the big ones.** A single 912 MB analysis is itself hours of work, so
analysis-granular resume is not enough for the outliers. The migrator records a `cursor` naming the
last completed **phase and element ordinal** (`classes:4102`, `endpoints:87/nodes:2311`). On resume it
re-enters the stream and **skips forward** to the cursor. Re-reading the skipped prefix of the stream
is cheap relative to re-inserting it; the download is the same either way.

Because writes are **upserts keyed by `(analysis_id, run_id, natural_key)`** and `run_id` is
deterministic, overshooting the cursor is harmless — a replayed element overwrites itself. **The
migrator is idempotent by construction, not by careful bookkeeping.** That is the property to protect
in review.

**(d) The sweeper.** PLAN-06's sweeper that deletes `IN_PROGRESS` runs older than a threshold must
**exempt runs referenced by a `migration_ledger` row in `IN_PROGRESS`**, or a migration paused
overnight will have its partial work garbage-collected underneath it.

### 2.5 Throughput expectations

The measured uplink is **~0.5 MB/s**. Downlink is typically better but should not be assumed to be;
plan on the same order.

| direction | what moves | why |
|---|---|---|
| **down** (S3 → migrator) | the JSON structure of SPLIT/DOC artifacts | must be parsed |
| **down** | *nothing* for `stored.jar`, `sources/*`, exports | referenced by key, never read |
| **up** (migrator → S3) | **only** extracted `sourceCode` blobs, deduped by sha256 | new artifacts that did not exist as separate objects |
| **up** (migrator → Mongo) | the entity documents | local/VPC link, not the 0.5 MB/s path |

Worked estimate for one large analysis, using PLAN-01/06 measured figures:

- Source `analysis.json`: 912 MB — but `~2,050 B/node × node count` of that is embedded source. On a
  1,597-class / ~450k-node-scale run the embedded source dominates. Call it **~75% source, ~25%
  structure**.
- **Download**: 912 MB at 0.5 MB/s ≈ **30 hours** for that one file if it must be read in full — and
  it must, because the source is interleaved with the structure in a single stream.
- **Upload of extracted source**: ~680 MB pre-dedup at 0.5 MB/s ≈ **~380 hours**. Unacceptable.

That single line is the reason for the following rule, and it should be stated as a decision rather
than discovered during the run:

> **Source extraction is deferred, not skipped.** In increment E the entity row stores a
> `source_ref` of `{ inline_offset_key: <the original analysis.json key>, sha256, size }` — i.e. the
> source stays **inside the original JSON**, and the source-view path reads it with a **ranged GET**
> (`ArtifactStore.read(key, offset, length)`, which every backend already implements) at the byte
> offsets the streaming parser recorded on the way past. Zero bytes uploaded. Extraction to
> `jar/<id>/src/<sha256>` happens only for **new** analyses (increment D), where the source is in
> memory anyway and the upload is part of the analysis it was always part of.

With that rule, backfill upload traffic is **zero** and the cost is download-bound only.

**Practical throughput planning:**

- Bound the whole job by download: `total_bytes_of_SPLIT/DOC_artifacts / 0.5 MB/s`. Get
  `total_bytes` for free from **`statPrefix`** — one listing per analysis, not a HEAD per file (an S3
  HEAD is ~25 ms, so a stat-per-file loop is 2,500× slower; the `s3-mode` skill calls this out
  explicitly). Print the estimate before starting and refuse to start without an operator ack if it
  exceeds a configured wall-clock budget.
- **Parallelise across analyses, never within one.** N concurrent analyses × one stream each. S3
  throughput per connection is the limit, not aggregate — this is the one lever that actually helps.
  Default `--parallelism=4`, tunable; watch for 503/throttling (§8).
- **Migrate newest-first.** Users look at recent analyses. Newest-first means the perceived migration
  finishes in hours even if the tail takes a week.
- **Skip-list the outliers.** Any analysis whose `statPrefix` total exceeds a threshold
  (default 200 MB) is marked `SKIPPED` with a reason and migrated in a deliberate, scheduled, single
  job. It stays fully readable from legacy the whole time (§4). One 912 MB file must not sit at the
  head of the queue blocking 2,000 small ones.
- **Run it where the bandwidth is.** 0.5 MB/s is a *local uplink* measurement. The right answer for
  the bulk job is to run the migrator **inside the same region/VPC as the bucket**, where the same
  transfer is minutes rather than days. This should be the recommended deployment for E and the
  laptop path treated as the fallback for small estates.

---

## 3 — Data that cannot be backfilled

This section exists because the honest answer to "did we migrate everything?" is **no**, and the
failure mode is discovering that after the legacy JSON is deleted.

### 3.1 A re-run is not a substitute for a migration

The tempting shortcut — "just re-analyse everything into Mongo" — **does not work**, and PLAN-02
proves it rather than suspecting it:

- The crawl is **not deterministic**. The SQL Server run (`USP_RUNMRPCYCLE__CIJ65E`) produced
  genuinely different node numbering and edge-file counts across two executions
  (`edges_0057.json` vs `edges_0059.json`, not aligned 1:1). Oracle/WL2ZTC differed in `dbCallCount`
  (259 vs 263).
- Node ordinals (`order`, `chunk_idx`) are **run-scoped**, so a re-run renumbers everything. Anything
  a user bookmarked, linked or cited by ordinal breaks.
- A re-run requires **live database connectivity** to the original source schema, with the original
  credentials, in the original state. For a historical analysis of a system that has since changed —
  which is most of the value of keeping historical analyses — that connectivity no longer describes
  the same thing.

**Consequence:** re-analysis is a *last resort*, offered to the user as an explicit action with a
warning that it produces a different run, never triggered automatically by the migrator.

### 3.2 Artifact families with no snapshot — unknown cardinality

PLAN-04 §8 (X1) and PLAN-05 §9 (Q10) say it in identical words: sizes for **`crossref/**`,
`flowanalysis/**`, `conversion/*`, `claude-chatbot/**` and `external-links*`** are **UNKNOWN — none
exist in either snapshot**. That is **five** trees, not four; `external-links*` is routinely forgotten
because it is a single file rather than a directory, and PLAN-06 nonetheless marks it **SPLIT**
(one document per link) on a cardinality nobody has measured. Their *shapes* are code-derived and
reliable; only cardinality is a guess.

Three specific hazards inside them, all already documented, all of which a migrator will hit:

- **`crossref/result.json` is rewritten in full per chunk under a lock** (`CrossRefClaudeService.java:1306`).
  A migration reading it concurrently with a running cross-ref job can read a torn file.
- **`flow_analysis_run.results.json` holds a pre-serialised opaque string** whose schema is owned by
  the flow analyzer, not the store — PLAN-04 §5.5 marks it UNKNOWN and keeps it a `storage_ref`.
  There is nothing to normalise until someone establishes its shape. `status.json` is rewritten in
  place on every state change from **10 call sites**.
- **Conversion jobs have no durable link back to a JAR** — `jarId` is never persisted
  (`ConversionController.java:482-485,542-561`, PLAN-03 defect 10). The entity model adds a nullable
  `jar_id`, but **the migrator cannot populate it**, because the information was never written. Those
  rows migrate with `jar_id=NULL` and the link is lost for all historical jobs. This is a real,
  permanent, pre-existing data loss and it should be reported as such rather than absorbed silently.

Similarly, the `jar/.../chat/` key is **dead** (PLAN-03 defect 9); chat lives under `claude-chatbot/`.
A migrator that walks `jar/**` looking for chat finds nothing and must not report that as an error.

Designing a normalised schema against a shape we have never seen populated is exactly the kind of
guess that produces a silent key/path divergence (§8). So:

> **These five families are migrated as ARTIFACTs, byte-for-byte, not modelled as entities in the
> first pass.** They keep their existing `ArtifactKey`, they are read exactly as today, and every
> screen that uses them behaves identically. They are entered in the entity model as an
> `unmodelled_artifact` row carrying `key`, `size`, `sha256`, `family` — enough to inventory them,
> not enough to query their contents.

This is a deliberate trade: those screens get **no** speed-up from the migration, and we say so
rather than promising one. In exchange we do not ship a schema for data we have not measured.

**The unblocking step is measurement, not design.** PLAN-04/05's own recommendation is a *"runtime
check before PLAN-06 sets chunk boundaries"*, which has not happened. Increment E's migrator emits a
**cardinality report** for these five families from the real estate — file counts, size histogram,
depth, field frequency sampled from the first N documents of each family. That report is the input to
a follow-up chunk (PLAN-04 revision) that models them properly. Until it exists, any schema we write
for them is fiction.

### 3.3 Things genuinely discarded, and why that is acceptable

| dropped | why it is safe |
|---|---|
| `_class-tree*.json`, `mongo-catalog.json` | **dead**: no writer exists for the first; `fetchCatalog:413` returns null for the second. Nothing can regress |
| `flow.json`, `edges.json` | **duplicates**: re-embed `chunks/` + `edges/` + `tables.json`. Every byte is recoverable from data we *do* migrate. See the OPEN QUESTION on external consumers |
| `endpoints/*/call-tree.json`, `endpoints/*/nodes.json` | duplicates; call trees are stored 4× |
| `_summary*.json`, `.parse-cache/*`, `.node-cache/*` | **derived caches**. `.node-cache/` is additionally known-unreliable — 1 file for a 57-node SQL Server run vs 41–104 elsewhere |

None of these are deleted from the legacy store by the migrator. They are simply not read. Deletion
is increment **H**, after the retention window, and is a separate operator decision.

### 3.4 Partial and corrupt sources

Legacy object storage cannot distinguish a complete analysis from an abandoned one — that is the
problem PLAN-06's run marker fixes going forward, but it does not fix the past. The migrator will
encounter analyses missing `api/index.json`, with truncated JSON, or with a `progress.json` that never
reached completion.

Policy: **fail the analysis, do not fail the job.** Mark the ledger row `FAILED` with the parse
position and the exception, leave no `COMPLETE` run marker (so nothing partial becomes visible), and
continue. A final report lists every `FAILED` row. A migration that aborts at the first bad input
will never finish against a real estate.

---

## 4 — Dual-write

### 4.1 The recommendation: **dual-read, not dual-write**

Do not write both. Write to one store; **read from both**.

The asymmetry is the point. The problem dual-write solves is "the new store might be wrong, so keep
the old one current." But in this system:

- **Legacy data is never mutated in place.** An analysis is produced once. Re-analysis produces a new
  output (or rotates to `_prev`), it does not edit the old one. So legacy JSON that has been migrated
  **cannot drift** — there is nothing to keep in sync.
- **The expensive half is the write.** At 0.5 MB/s, dual-writing means every new analysis pays the
  full legacy upload cost *plus* the Mongo write. The 9-minute upload for one JAR does not become
  free because we also wrote to Mongo — it becomes mandatory again, having been the thing we set out
  to remove.
- **Dual-write's hard case is partial failure**, and there is no atomicity across an S3 bucket and a
  Mongo database. Every design for it ends in a reconciliation job — which is the migrator we already
  have.

So the transition state is:

| kind of data | written to | read from |
|---|---|---|
| analyses produced **before** cutover | legacy only (already there) | Mongo if a `COMPLETE` run exists, else legacy |
| analyses produced **after** cutover | Mongo (+ `ArtifactStore` for blobs, as always) | Mongo |
| blob artifacts, all eras | `ArtifactStore` — **unchanged in every mode** | `ArtifactStore` |

This is implemented as a **composite `AnalysisRepository`** decorating the two implementations
PLAN-07 §2.3 already defines — `ArtifactScanAnalysisRepository` (which serves *both* `local` and `s3`,
because it is written against `ArtifactStore`) and `MongoAnalysisRepository`. Increment C exists
precisely so the composite has a legacy delegate. It is selected by PLAN-07's own property rather
than a new one:

```
app.storage.repository = auto | artifact-scan | mongodb | mongo-then-artifact-scan
```

`mongo-then-artifact-scan` checks Mongo for a **current, complete** `analysis_run` for the requested
subject (one indexed lookup against `uq_run_current`, not a scan); a miss falls through to
`ArtifactScanAnalysisRepository`. Because `AnalysisRepository` is cursor-paged
(`PageRequest(limit, after, sort)` → `Page(items, nextCursor, total)`, with `total == -1` permitted),
the composite never has to reconcile offsets between two backends — it serves any one subject
entirely from one delegate. That is the property that makes the composite about 100 lines rather than
a distributed-query engine, and it is worth preserving deliberately: **never split a single page
across both delegates.**

PLAN-07 §5.5 also requires a startup WARN naming properties that are populated but inert (e.g.
`app.storage.s3.*` under `mode=mongodb`). Keep it — during a migration, a silently-ignored property is
exactly how an operator concludes they rolled back when they did not.

### 4.2 The narrow case where we *do* dual-write

There is one family where the legacy artifact **is** mutated after creation: the **append-only** six
(chat history, Claude call logs, correction run logs, improvement runs). Those are read-everything,
append, rewrite-everything today. A user chatting during the transition window writes new entries.

For those, and only those, during the window between D and G:

- Write the entry to Mongo (one document per entry, `(scope_id, seq)`) **and** append to the legacy
  artifact.
- **Mongo is the authority.** If the Mongo insert fails, the operation fails and the user sees an
  error — no silent divergence.
- If the **legacy** append fails, log a `WARN` with the `(scope_id, seq)`, increment a
  `migration.legacy_append_failures` counter, and **continue**. The legacy copy is a rollback safety
  net, not the record of truth. Its being stale by a few entries is exactly the bounded loss defined
  in §6.
- The window is **short by design** — see §4.4 — because this is the one path where the O(n²)
  rewrite cost we are trying to eliminate is still being paid.

### 4.3 Detecting divergence

Because there is no general dual-write, "divergence" means one of two specific things:

1. **A migrated analysis whose Mongo entities do not match its source JSON.** Detected by the
   reconciliation checks in §7 — row counts per collection against counts derived from the source
   artifacts, recorded in `migration_ledger.counts` at migration time and re-verified by a separate
   `verify` pass that re-reads the source. Run `verify` over a **sampled 5%** continuously and over
   100% before increment H.
2. **Append-only divergence.** `seq_max` in Mongo vs entry count in the legacy artifact, per scope.
   A scheduled job compares them and reports; a non-zero delta is expected only in the direction
   Mongo-ahead-of-legacy, and Mongo-behind-legacy is an **alert**, because it means someone wrote
   through a path that bypasses the composite repository.

Both feed one metric surface with three numbers a human can read: analyses migrated / analyses
verified / divergences open.

### 4.4 How long the window lasts

| phase | duration | ends when |
|---|---|---|
| D → E (Mongo for new analyses, legacy untouched) | as long as needed; safe indefinitely | someone decides to backfill |
| E (backfill running) | download-bound; see §2.5. Days for a laptop-run estate, hours in-region | ledger has no `PENDING`/`FAILED` rows except deliberate `SKIPPED` |
| F (`mongo-then-legacy`) | **minimum 2 weeks per environment** | 100% `verify` pass and zero open divergences |
| G → H (retention window before legacy deletion) | **90 days, default** | explicit operator decision, per environment |

The 90 days is not arithmetic; it is the period during which rollback (§6) remains free. Shortening it
shortens the rollback guarantee, and that trade should be made explicitly rather than by an eager
cleanup script.

---

## 5 — Cutover

### 5.1 The flip

**Two properties, and only the first is the real flip.** `StorageConfig` binds `app.storage.mode`
(`StorageMode.parse`, `analyzer-storage/.../StorageMode.java`) and `StorageFactory.settingsFor(...)`
resolves it with **system property beating env var** (`StorageFactory.env(envVar, sysProp, fallback)`),
so the flip is a command-line argument, not a redeploy:

```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
     --app.storage.mode=mongodb \
     --app.storage.mongodb.uri=<credential-free uri> \
     --app.storage.mongodb.database=analyzer \
     --app.storage.repository=auto
```

The second property is PLAN-07's own recommendation (§5.5 / OPEN QUESTION 5):

```
app.storage.repository = auto | artifact-scan | mongodb      # default: auto
```

`auto` means "`mongodb` iff `mode=mongodb`". Splitting it out is what makes the migration safe,
because it separates **where bytes live** from **where queries are answered**:

| combination | meaning | used in |
|---|---|---|
| `mode=s3`, `repository=auto`(→`artifact-scan`) | today | pre-migration, and full rollback |
| `mode=s3`, `repository=mongodb` | **artifacts stay in the bucket, queries answered from Mongo** | the whole of increment F — this is the read-side canary, and it needs no write-side change at all |
| `mode=mongodb`, `repository=auto`(→`mongodb`) | full cutover | increment G onward |
| `mode=mongodb`, `repository=artifact-scan` | diagnostic only | isolating a repository bug from a store bug |

The `mongo-then-legacy` fallback of §4.1 is implemented as a **composite `AnalysisRepository`**
selected by a third value, `repository=mongo-then-artifact-scan`, decorating the two existing
implementations. It is a decorator over `AnalysisRepository`, the same shape `CredentialGuardStore`
already is over `ArtifactStore` — no new architectural concept.

**Startup validation.** `StorageFactory.Settings.validate()` already refuses impossible combinations
before any analysis starts (*"Storage mode is s3 but no bucket was given."*). PLAN-07's MONGODB cases
must all be live before cutover: blank URI → *"Set APP_STORAGE_MONGODB_URI"*; a **userinfo-bearing URI**
(regex `//[^/@]*@`) sourced from a property → refuse to boot, because that is a credential in a
property file and C4 forbids it; blank database; `inline-threshold-bytes` outside `(0, 8 MiB]`. Add
one more: `repository=mongodb` with an unreachable server must fail at startup, not on the first
request.

**Indexes before data.** Per PLAN-07, create indexes *before* importing a backfilled deployment and
boot with `app.storage.mongodb.index-ensure=never` thereafter. Building indexes after a multi-million-row
backfill is both slower and the classic way to make a cutover window unbounded.

`CredentialGuardStore` still wraps the Mongo backend — `StorageFactory.create` applies it
unconditionally, before the mode switch, so it cannot be bypassed. A test asserts it blocks
`plsql-config.yaml` / `plsql-env-config.yaml` through the Mongo path exactly as through the S3 path.

**The property that makes rollback free.** `app.storage.mongodb.prefix` defaults to `data` — the same
value as `app.storage.s3.prefix` and the same layout as the local data root. **Keys are byte-identical
across all three modes.** Keep it that way. It is the reason an `ArtifactKey` recorded by the migrator
still resolves after a rollback to `s3`, and it is the concrete mechanism behind C5's "not a restore".

### 5.2 Per-environment sequence

Identical steps, escalating scrutiny.

**Local / developer** — the loop, not a gate.
1. `mvn -o -q -DskipTests install && mvn -o test` — full suite green.
2. Boot in `local` mode. **Boot it.** (§8, risk 1.)
3. Boot in `mongodb` mode against a container. Boot with a deliberately broken URI and confirm it
   refuses at startup.
4. `ArtifactStoreConformanceTest` green against the Mongo backend — the same contract the local and
   S3 backends pass.

**CI** — every commit from increment C onward.
5. `python3 .claude/skills/storage-check/scripts/scan.py` — 0 unclassified ops.
6. `mvn -o -pl unified-web -Dtest=DirectFileIoGateTest test` — gate green, no RATCHET.
7. The 16 MB synthetic-scale test (2,000 classes / 6,000 nodes) — every document under the 12 MB soft
   limit.
8. `no-local-writes.sh` under `mongodb` mode with an in-memory/containerised store.

**Staging** — the first environment with real data. Everything above, plus:
9. Backfill a **copy** of a production estate. Compare `migration_ledger.counts` to source
   `statPrefix` counts (§7.1).
10. The two end-to-end spot checks (§7.2) against real identifiers.
11. Every PLAN-03 screen exercised in `mongodb` mode. C3 is the acceptance criterion, not a hope.
12. Soak for the §4.4 window at `mongo-then-legacy` with real traffic.

**Production** — no new verification invented here; the same list, plus:
13. Backfill runs **before** the mode flip, against the live bucket, read-only. This is the key
    property: **production backfill changes nothing a user can see.** The application is still in
    `s3` mode throughout. If it fails, nobody notices.
14. `verify` at 100% over the migrated estate.
15. Flip to `mongodb` + `mongo-then-legacy` — one restart, rolling, no downtime, because the legacy
    store is still authoritative for anything not migrated.
16. Watch the three numbers from §4.3 plus p95 page load, for the window.
17. Only then consider `read-preference=mongo` (increment G) and, much later, H.

### 5.3 What is verified before the flip — the checklist

The flip is refused unless all of these are true, and each is a command, not an opinion:

- [ ] `migration_ledger`: zero `PENDING`, zero `FAILED`, `SKIPPED` rows all explained
- [ ] `verify` pass at 100%, zero count discrepancies
- [ ] both §7.2 spot checks pass against real identifiers
- [ ] `no-local-writes.sh verify` → PASS in `mongodb` mode
- [ ] `scan.py` → 0 unclassified; `DirectFileIoGateTest` green
- [ ] `ArtifactStoreConformanceTest` green against the Mongo backend
- [ ] the app **boots** in `mongodb` mode and serves a real page
- [ ] `ArtifactStore`'s method signatures diff clean against the pre-migration tag (C1's definition of
      done)
- [ ] the rollback command in §6 has been **executed once in staging** and the environment came back

---

## 6 — Rollback

C5 requires better than "restore from backup", and the design already gives it: **the legacy JSON is
never modified and never deleted before increment H.** Rollback is therefore a property change plus a
bounded, enumerable loss — not a restore.

### 6.1 The command

```bash
# full rollback: back to the pre-migration mode
--app.storage.mode=s3                        # or local
--app.storage.repository=artifact-scan
```

One restart. Rolling, so no downtime. Nothing is copied, nothing is restored, nothing is un-migrated.
It works because `app.storage.mongodb.prefix` and `app.storage.s3.prefix` are both `data`, so every
`ArtifactKey` in the system resolves identically on both sides of the flip.

There is a **partial rollback** that is usually the right first move, and it does not even change the
storage mode:

```bash
--app.storage.repository=artifact-scan
```

This keeps `mongodb` mode writing new analyses to Mongo while all *reads* come from legacy. It is the
correct response to "Mongo reads look wrong" and it loses nothing at all. **Try this before the full
rollback.** Because it is a read-path-only change it can be flipped back and forth freely, which makes
it a genuine diagnostic tool rather than a one-way door.

### 6.2 What is reverted, what is kept, what is lost

| category | on rollback |
|---|---|
| Legacy JSON in `local`/`s3` for pre-migration analyses | **untouched and authoritative.** Nothing to revert |
| Blob artifacts (`stored.jar`, `sources/*`, exports) | **untouched.** They never moved — the whole point of §2.5's structure-only design. This is why rollback is cheap |
| Mongo entity documents for migrated analyses | **kept, not deleted.** They are a derived copy. Leaving them means a re-attempt does not re-download at 0.5 MB/s |
| `migration_ledger` | kept. A second attempt resumes rather than restarts |
| Append-only entries written during the window | **preserved in both** — that is what §4.2's dual-write buys |
| **Analyses produced *only* in `mongodb` mode after cutover** | **THIS IS THE LOSS.** See below |

### 6.3 The exact loss, and its maximum window

Only one class of data exists solely in Mongo: **analyses run after the cutover flip and before the
rollback.** Their entity documents survive in Mongo but the application, back in `s3` mode, cannot
read them — there is no legacy JSON for them because nothing wrote it.

**Maximum window of loss = time between the cutover flip and the rollback.** Bounded by the operator,
not by the system. Concretely, with the §5.2 sequence and monitoring on the three numbers in §4.3, the
realistic detection time is **under one hour** and the contractual bound we commit to is
**24 hours** — anything undetected for longer than that has passed a full business-day monitoring
cycle and is not a cutover failure.

Three mitigations shrink it further, and at least the first two should ship with increment G:

1. **The blobs are already safe.** A post-cutover analysis's `stored.jar`, decompiled source and raw
   SQL went to `ArtifactStore` as always. What is stranded is the *structure*, not the input. The
   analysis can be **re-derived from artifacts already in the bucket, with no re-upload** — and for
   the JAR side that derivation is deterministic (unlike the PL/SQL crawl, §3.1).
2. **An export path.** `migrate --export --since <cutover-time>` walks Mongo runs with no ledger row
   and writes legacy-shaped JSON back into the `ArtifactStore`. This converts the loss into a
   *download plus upload*, which at 0.5 MB/s is slow but finite and, critically, only applies to
   however many analyses were produced in the window — typically a handful. **This is the reason the
   loss is bounded rather than permanent.**
3. **Announce a freeze.** For the first 24 hours after a production flip, new analyses are gated
   behind an operator-visible banner. Cheap, and it makes the window an explicit decision.

**What is *not* recoverable:** chat messages and Claude call-log entries written after cutover, in the
case where §4.2's legacy append also failed (the `WARN` path). That is a handful of entries, they are
named in the log by `(scope_id, seq)`, and the count is on a metric. It is bounded, enumerable, and
reportable to the user — which is the standard C5 sets, as opposed to "some data may be missing".

### 6.4 The one thing that can silently break rollback: blob reclamation

PLAN-05 §7.1 specifies `storage_ref` reclamation as **mark-and-sweep with a 7-day grace**
(`… AND created_at < now() - interval '7 days'`, then `ArtifactStore.delete(store_key)`). Blobs are
shared by `content_sha256` and are *never* cascaded from content.

That sweeper is the only component in the entire design with permission to delete from the
`ArtifactStore` — which is to say, it is the only thing that can destroy the data rollback depends on.
Its correctness assumption is that Mongo's `storage_ref` table knows every live reference. **During
the migration that assumption is false**, because the legacy JSON also references those blobs, and the
legacy JSON is not in `storage_ref`.

Concretely: an analysis migrated on day 1, rolled back on day 9, could find its `stored.jar` deleted
by a sweep on day 8 that saw no live Mongo reference to it.

**Therefore: the reclamation sweeper is DISABLED for the entire migration programme and is not
enabled until after increment H.** Not "configured with a longer grace" — disabled, with a startup
log line saying so, and re-enabling it is an explicit item on H's checklist. A 7-day grace is a
sensible steady-state policy and a catastrophic transition-state one, and the difference is invisible
until someone rolls back in week two.

### 6.5 The point of no return

Increment **H** — deleting migrated legacy JSON — is the only step that makes rollback impossible. It
is therefore a separate, per-environment, explicitly-acknowledged operation, gated on:

- the full §4.4 retention window elapsed (90 days default), **and**
- a 100% `verify` pass **re-run at deletion time**, not reused from cutover, **and**
- a per-analysis check that a current, complete `analysis_run` exists before *that analysis's* JSON is
  deleted, **and**
- re-enabling the `storage_ref` reclamation sweeper (§6.4) as a deliberate, separately-logged step.

Deletion is per-analysis and driven off the ledger, so it is interruptible and partial deletion leaves
a consistent state. It should be scheduled to trail the migration by the full window rather than run
as a "cleanup" at the end of E — the eagerness there is exactly the shape of mistake that turns a
recoverable rollout into an incident.

---

## 7 — Verification at each step

Reuse first. Three things already exist and already work, and the migration adds checks *around* them
rather than replacing them.

### 7.1 What already exists

| tool | where | what it proves | when |
|---|---|---|---|
| **`/storage-check`** | `.claude/skills/storage-check/scripts/scan.py` + gate `DirectFileIoGateTest` | 0 UNCLASSIFIED direct-I/O ops (baseline **217 classified**), no NOT-ALLOWED / BUDGET / RATCHET failure, suite green (**1165 tests, 0 failures**), app boots and serves real data | every commit, and as the precondition to every increment |
| **`no-local-writes.sh`** | `.claude/skills/s3-mode/scripts/no-local-writes.sh` | observes the *filesystem* during a real run — described in its own header as *"the only test that can actually fail the migration"*, because everything else reasons about source code | before each cutover, in `mongodb` mode |
| **`ArtifactStoreConformanceTest`** | `analyzer-storage/src/test/java/com/analyzer/storage/ArtifactStoreConformanceTest.java` — **25 `@Test` methods**; `S3ArtifactStoreConformanceTest` re-runs all of them against S3 over real HTTP | "callers cannot tell the backends apart" — asserted, not claimed | CI, against the Mongo backend from increment D |

Exact commands:

```bash
mvn -o -q -DskipTests install && mvn -o test
mvn -o -pl unified-web -Dtest=DirectFileIoGateTest test
python3 .claude/skills/storage-check/scripts/scan.py                 # exits non-zero on any UNCLASSIFIED
mvn -o -pl analyzer-storage -Dtest='*ArtifactStoreConformanceTest' test
git diff --exit-code -- .../ArtifactStoreConformanceTest.java        # the contract itself must not move
.claude/skills/s3-mode/scripts/no-local-writes.sh snapshot /tmp/before
#   ... run a real analysis in mongodb mode ...
.claude/skills/s3-mode/scripts/no-local-writes.sh verify /tmp/before /tmp/after
```

Three rules about reusing these, each of which someone will be tempted to break:

1. **`MongoArtifactStoreConformanceTest` overrides `newStore(Path root)` and an `@AfterEach` teardown,
   and nothing else.** If it needs to relax a case, that case is a real behavioural difference and a
   finding to report, not a test to edit. The `git diff --exit-code` above enforces this.
2. **`no-local-writes.sh` needs no change for Mongo mode.** Its allowlist (`data/logs/**`,
   `data/config/**`, `**/plsql-env-config.yaml`) is about *what is legitimately local*, which is
   unchanged. `plsql-env-config.yaml` still holds plaintext production passwords and must stay local —
   C4, correct by design, not a bug to fix. It records **path + size**, so an in-place rewrite is
   caught, not just a new path.
3. **No new `ALLOWED` entry and no raised budget** in `DirectFileIoGateTest`. Per PLAN-07, the Mongo
   backend's only direct I/O is `materialize()`'s temp file, which is category `temps` and already
   accounted for. A migration that needs the budget raised has a call site writing where it should not.
   *"Never raise a budget to go green"* is the storage-check skill's own rule.

### 7.2 Migration-specific checks

**(a) Row counts vs source artifacts.** For every migrated analysis, before flipping its ledger row to
`COMPLETE`:

| collection | must equal | source of truth |
|---|---|---|
| chunk/node docs | `statPrefix("…/chunks/")` file count | one listing, not a HEAD per file |
| edge docs | `statPrefix("…/edges/")` file count — **allowing for gaps**, so count files, never `max(ordinal)` | PLAN-02: MySQL had 2, 14, 15 absent |
| `api/nodes` docs | same count as chunk docs | PLAN-02 verified chunk count == node count == `api/nodes/` count across **all five** analyses. This is a real, already-established invariant — assert it |
| class docs | `classes[]` length counted by the streaming parser as it passes | |
| endpoint docs | `endpoints[]` length, likewise | |
| table docs | `tables.json` entry count | |

Any mismatch → `FAILED`, no `COMPLETE` marker, analysis stays served from legacy. **Counts are
recorded in the ledger at migration time and independently recomputed by the `verify` pass**, so a
migrator bug that miscounts on the way in is caught by a reader that recounts from source.

**(b) Two end-to-end spot checks, against real identifiers.** Counts prove nothing about *content*.
Reuse PLAN-05 §8's worked example **verbatim** rather than inventing test data — it already carries
real values, so the assertions can be literals.

**Check 1 — a real Java endpoint.** Source
`jar/web-1.0.0-SNAPSHOT/endpoints/001_ReportController.monthlySales/`. After migration, assert:

| assertion | expected |
|---|---|
| `jar_artifact` | `jar_id='web-1.0.0-SNAPSHOT'`, `jar_size=70489772`, `base_package='com.enterprise'` |
| `jar_analysis` | `total_classes=119`, `total_endpoints=84`, `version='STATIC'` — these six fields *are* the 191 B `_header.json` |
| `endpoint` | `GET /api/v1/reports/sales/monthly/{year}`, `controller_class='com.enterprise.oracle.web.controller.ReportController'`, `method_name='monthlySales'`, `total_methods=15`, `in_scope_calls=1`, `external_scope_calls=14` |
| `call_node` count | **exactly 15** (`nodes.json.totalNodes`), ordinals 0..14 |
| node ordinal 2 | `OrderJdbcRepository.getMonthlySalesReport`, `stereotype='REPOSITORY'`, `cross_module=true`, `source_jar='repository-1.0.0-SNAPSHOT.jar'`, `line_count=33`, `has_db_interaction=true` |
| `call_node_collection` | present on **ordinal 2 only**: `ENTERPRISE.ORDERS` and `ORDER_DATE`, both `READ` |
| **source resolution** | `source_ref` for ordinal 2 resolves through `ArtifactStore` to **1,681 bytes** of decompiled source, byte-identical to the `sourceCode` field in the legacy `nodes/node_2.json` |

The last row is the important one: it exercises §2.5's ranged-GET `source_ref`, the most novel
machinery in this plan, end to end. Note `ORDER_DATE` is a **column** mis-detected as a collection and
resolves to `match_kind='UNMATCHED'` — the migration must reproduce the existing defect, not quietly
fix it, or the diff against legacy will never be clean.

**Check 2 — a real PL/SQL node.** Source `plsql-parse/ERP_USP_RUN_CYCLE__WL2ZTC` (Oracle), node
`ANALYZER$ERP_USP_EXPLODE_BOM`, chunk **34**. Assert:

| assertion | expected |
|---|---|
| `plsql_analysis_key.identity_name` | `ORACLE~OracleDemo~local~ERP_USP_RUN_CYCLE` |
| `plsql_analysis` | `total_nodes=52`, `max_depth=11`, `total_tables=41`, `total_tables_raw=42`, `total_edges=2258`, `unique_edges=218`, `total_cycles=3` |
| `flow_node` | `node_id='ANALYZER$ERP_USP_EXPLODE_BOM'`, `"order"=34`, `depth=3`, `parent_node_id='ANALYZER$ERP_USP_VALIDATE_MASTER_DATA'`, `line_end=33`, `in_cycle=true` |
| denormalised counters | `count_tables=2 count_calls_out=2 count_variables=1 count_cursors=1 count_parameters=3 count_exception_handlers=1 subtree_tables_count=3 flow_lines_of_code=57` |
| children | 3 parameters (`P_PRODUCT_ID`, `P_DEPTH`, `P_MAX_DEPTH`), 1 variable (`V_CHILDREN`), 1 cursor (lines 16–21), 1 handler (`OTHERS`, line 30) |
| calls | `ERP_USP_WRITE_AUDIT_TRAIL` `call_site_count=3` lines {11,27,31}; self-recursive `ERP_USP_EXPLODE_BOM` `call_site_count=1` line 24 |
| edge dedup | 2,258 raw → **218** dedup (10.4×) across the analysis |
| catalog | `ANALYZER.ERP_BOM_COMPONENT`: **4** `db_column`, **5** `db_constraint`, **2** `db_index_column` |
| whole-analysis usage | **42** tables, **411** usage rows, **2,436** usage lines |

Two of these are deliberately adversarial. `count_calls_out=2` is **PLAN-05 OPEN QUESTION N1** —
`api/` says 2 and `chunks/` says 0 for the same node. The migrator must pick a source, and the test
must pin which one, or the two readers will disagree after cutover exactly as the artifacts do now.
And the **edge dedup key must reproduce `ChunkedFlowWriter.java:1138-1151` exactly**; 2,258 → 218 is
the assertion that it does. A dedup key that is subtly wrong still produces a plausible number.

**Check 3 — the non-deterministic one.** Add the SQL Server analysis `USP_RUNMRPCYCLE__CIJ65E`. It is
the run PLAN-02 found produced genuinely different node numbering and edge-file counts across two
executions (`edges_0057.json` vs `edges_0059.json`, not aligned 1:1), *and* has the anomalous
`.node-cache/` (1 file for 57 nodes vs 41–104 elsewhere). If any real input breaks the migrator, it is
this one. Assert only that both its runs migrate as **two independent runs** with different node
counts and that neither is silently deduped into the other.

All three are automated against checked-in fixtures in CI, and re-run in staging against the real
estate.

**(c) Per-increment gate.**

| increment | additionally verified |
|---|---|
| A | a list endpoint returns page 2 with a stable total; UI renders it; no endpoint's default behaviour changed for a client that sends no params |
| B | full suite green **and the app boots**; nothing 404s that previously rendered |
| C | the legacy repository returns byte-identical results to the code paths it replaces |
| D | conformance test green on Mongo; 16 MB synthetic-scale test; `no-local-writes.sh` PASS; startup refuses a bad URI |
| E | §7.2(a) counts; `verify` at 5% sample; the §3.2 cardinality report actually produced |
| F | both §7.2(b) spot checks; p95 page load not worse than legacy for every PLAN-03 screen |
| G | append-only `seq_max` reconciliation; TTL index present and its window is the configured one |
| H | 100% `verify` re-run *at deletion time*; per-analysis `COMPLETE` marker checked before each delete |

---

## 8 — Risks

Only risks this codebase has already demonstrated. Each has a mitigation and, more importantly, an
**early-warning signal** — something observable before the damage, not the damage itself.

### R1 — A green test suite hiding a startup failure

**Demonstrated:** the `s3-mode` skill records it as a known trap in this repo: *"A green suite has
hidden a startup failure here before. Always boot the app."* The `mongodb` mode adds a new
`StorageFactory` branch, a new `Settings` variant and a new external dependency — three of the exact
ingredients that produce a context-initialisation failure no unit test observes. The AWS SDK version
clash (`NoClassDefFoundError: BusinessMetricFeatureId`, from 2.25.60 and 2.29.24 both in `~/.m2`) is
the same class of failure and the Mongo driver will bring its own transitive graph.

**Mitigation.** Booting the app is part of the definition of done for every increment, not a manual
step. A smoke stage starts the real jar in each mode, hits a real page, and kills it. Pin the Mongo
driver via `dependencyManagement` and prove it with
`mvn -o dependency:tree -pl analyzer-storage | grep -i mongo | sort -u`, exactly as the SDK was pinned.

**Early warning.** The smoke stage failing while `mvn test` is green. If that combination ever appears
and someone re-runs the suite instead of reading the boot log, the signal has been wasted.

### R2 — Key/path divergence that broke WAR storage silently

**Demonstrated:** provider `Path` constructors and `getFilePath()`/`getSummaryCachePath()` diverged
from the key scheme, breaking WAR storage with no error. It was closed by giving
`AbstractAnalysisDataProvider` and both concrete providers an `ArtifactKey` and adding the
`getXDocumentKey()` twins. The migration reintroduces the hazard in a new form: the **migrator
computes keys for entities that the analyzer also computes keys for**, and if the two disagree, reads
return empty rather than throwing.

**Mitigation.** §2.3's rule — the migrator uses the **same mapper and the same repository write path**
as the analyzer, never its own. No key-construction code exists in the migrator module; a test asserts
the module contains no string concatenation producing an `ArtifactKey`.

**Early warning.** §7.2(a)'s count checks, and specifically the *zero* case: a collection with zero
rows where the source has files is the signature of a key mismatch, not of an empty analysis. Alert on
zero, not only on mismatch — an equality check passes trivially when both sides are wrong in the same
direction, and "empty result" is precisely how this bug presented last time.

### R3 — 0.5 MB/s: a 9-minute upload for one JAR

**Demonstrated:** measured. One 70 MB `stored.jar` takes 9+ minutes. A naive migration that
re-uploads artifacts is not slow, it is **infeasible** — §2.5 computes ~380 hours for the extracted
source of a single large analysis.

**Mitigation.** The structure-only design: zero uploads during backfill, ranged-GET `source_ref`
instead of source extraction, `statPrefix` instead of per-file HEADs, newest-first ordering,
size-threshold skip-listing, parallelism across analyses, and the recommendation to run the bulk job
in-region.

**Early warning.** The migrator prints an estimated wall clock from `statPrefix` totals **before it
starts** and refuses to proceed past a configured budget without an ack. During the run, an
**observed-vs-estimated throughput ratio** is logged per analysis; a sustained drop below 50% of
estimate means throttling or a hidden re-upload path and should page. A single non-zero
`bytes_uploaded` counter during backfill is by itself a bug — it should be **asserted zero**, not
merely monitored, since the design says nothing uploads.

### R4 — Agents (and long jobs) losing work to machine sleep

**Demonstrated:** two earlier agents lost all their work by leaving writing until the end; work has
been lost to machine sleep. A backfill measured in days on a laptop is the same failure with a bigger
blast radius.

**Mitigation.** Everything in §2.4: the ledger is written **continuously**, cursors are recorded
per-phase, writes are idempotent, and killing the process at any instant costs at most one in-flight
batch. Nothing is held only in memory and flushed at the end — which is the same lesson as writing the
document early. Operationally, run the bulk job with `caffeinate` on macOS or, better, not on a laptop
at all (§2.5).

**Early warning.** A ledger heartbeat: `migration_ledger` rows in `IN_PROGRESS` whose `updated_at` is
older than a few minutes mean the job is dead or wedged. Because §2.4(d) exempts these from the
sweeper, a dead job leaves a clear, persistent trace rather than being tidied away.

### R5 — Unverified S3 paths meeting real volume for the first time

**Demonstrated by absence:** storage-check OPEN-ITEMS lists real SigV4, IAM/bucket policy/KMS,
throttling and 503 retries, socket resets mid-multipart, eventual listing visibility, `_prev` rotation
under concurrency, multi-GB objects, and the 5 GiB `CopyObject` ceiling as **not verified**. The
migrator will be the heaviest S3 client this application has ever had.

**Mitigation.** Run the `s3-mode` skill §5 live smoke checklist before increment E, as a prerequisite
and not as part of the run. Give the migrator explicit backoff on 503/throttling and a per-analysis
retry with the ledger's `attempts` counter as the circuit breaker.

**Early warning.** Any `FAILED` ledger row whose exception is a 503, a socket reset, or a signature
error — as distinct from a parse error. Those are infrastructure findings, not data findings, and
should stop the job rather than being retried into a rate-limit spiral.

### R6 — Promising a page-load improvement that does not arrive

**Demonstrated:** PLAN-03 found no endpoint accepts pagination parameters, and PLAN-06 states plainly
that without PLAN-08 this plan "reduces storage cost but not page-load cost." The risk is
organisational: ship D–H, measure no user-visible change, and lose the mandate to finish.

**Mitigation.** Increment **A** ships first and delivers a measurable win with **no database work at
all**. Every later increment states its user-visible delivery in the §1 table or is not shipped
separately.

**Early warning.** p95 page load for the PLAN-03 screens is tracked from increment A onward. If it is
flat after A, the pagination is not actually reaching the client and everything downstream is built on
a false premise.

---

## 9 — OPEN QUESTIONS

1. **Does anything outside `unified-web` read `flow.json` or `edges.json`?** PLAN-02 found no reader
   in `unified-web`, but an external consumer (a script, a customer integration) would break silently
   when B stops reading and H stops writing.
   *Options:* (a) drop now; (b) keep writing as an ARTIFACT for one release with a read counter, then
   drop; (c) keep forever.
   **Recommendation: (b)** — it is PLAN-06's own recommendation, it costs one release of storage, and
   it converts a guess into a measurement. The counter is the deciding evidence for H.

2. **Ranged-GET `source_ref` vs eventual source extraction.** §2.5 defers extraction to avoid ~380
   hours of upload, at the cost of keeping the 912 MB `analysis.json` alive as a byte-range container
   forever — which means increment H can never delete it for backfilled analyses.
   *Options:* (a) permanent ranged-GET; (b) extract lazily on first source view, writing
   `jar/<id>/src/<sha256>` then, so cost follows actual use; (c) a scheduled in-region extraction job
   once, where bandwidth is cheap.
   **Recommendation: (b) then (c)** — lazy extraction makes the common case free and self-limiting,
   and a later in-region sweep finishes the tail so H can eventually delete. (a) alone quietly makes H
   impossible, which should not happen by default.

3. **Where does the bulk backfill run?** A laptop at 0.5 MB/s versus an in-region job.
   *Options:* (a) operator laptop; (b) a one-off task in the bucket's region; (c) both, with (a) for
   small estates.
   **Recommendation: (c)** — make in-region the documented path for any estate over ~10 GB and keep
   the laptop path for development and small deployments. The migrator code is identical; only the
   `--parallelism` default and the wall-clock budget differ.

4. **Do the five unmodelled families (`crossref/`, `flowanalysis/`, `conversion/`, `claude-chatbot/`,
   `external-links*`) get modelled in a follow-up chunk, or stay artifacts?** §3.2 defers on the
   grounds that we have never seen them populated.
   *Options:* (a) stay artifacts permanently; (b) model after E's cardinality report; (c) model now
   from the writers.
   **Recommendation: (b)** — (c) is the guess that produced R2 last time. The report is a deliverable
   of E precisely so this question becomes answerable with data.

5. **How long is the retention window before H?** §4.4 proposes 90 days.
   *Options:* 30 / 90 / 180 days, or per-environment.
   **Recommendation: 90 days, per-environment overridable, with production never shorter than
   staging.** The number is the duration of the free-rollback guarantee, so it should be set by
   whoever owns that guarantee rather than by disk cost.

6. **Does the archived-twin `_20260723_HHMMSS` convention survive?** PLAN-06 notes re-analysis becomes
   insert-new-run plus retire-old-run, giving history for free.
   *Options:* (a) migrate twins as separate runs and stop creating new twins at D; (b) keep the folder
   convention alongside runs.
   **Recommendation: (a)** — §2.3 already migrates them as separate runs because PLAN-02 proved they
   are re-executions rather than copies; continuing to write timestamped folders after D would be two
   history mechanisms disagreeing.

7. **How many tables are there actually?** PLAN-05 §0.1 states **68** (26 JAR / 27 PL/SQL / 15
   shared), but §1 contains **71** `CREATE TABLE` statements, and three PLAN-04 entities have no DDL
   at all: `call_fragment` (E13), `db_object_usage_line` (E38 — folded into an `int[]` per Q1 and
   marked `[MONGO DIVERGES]`), and `cross_ref_chunk` (E56). This is not pedantry: the migrator's
   completeness check is *"every collection in the schema received the rows its source artifact
   implies"*, and that check cannot be written against an ambiguous schema. Three entities with no
   DDL means three families whose migration nobody will notice is missing.
   *Options:* (a) reconcile the count in PLAN-05 and add the three missing DDLs; (b) declare the three
   deliberately unmodelled and list them in §3.2 alongside the other five.
   **Recommendation: (a) for `call_fragment` and `cross_ref_chunk`** (PLAN-06 explicitly models
   `endpoints/*/fragments/*.json` as DOC-per-fragment, so `call_fragment` is required, not optional),
   **and (b) for `db_object_usage_line`**, whose `[MONGO DIVERGES]` marking is a deliberate decision
   already made. Resolve before increment E, because the count check is what catches a silent
   key/path divergence (R2) and it must be able to enumerate every target collection.

---

## 10 — What this document commits to

Three claims, each falsifiable:

1. **Backfill uploads zero bytes.** Asserted by a counter, not monitored. If it is non-zero, the
   structure-only design has been violated somewhere and the 0.5 MB/s uplink will make the job
   unfinishable.
2. **Rollback is `--app.storage.repository=artifact-scan` and one rolling restart**, losing at most
   the analyses produced in the cutover window (bounded at 24 hours, realistically under one hour),
   all of which are re-derivable from artifacts already in the bucket.
3. **Increment A ships first and is not database work.** Without it the rest of the programme is
   correct, expensive, and invisible to users.
