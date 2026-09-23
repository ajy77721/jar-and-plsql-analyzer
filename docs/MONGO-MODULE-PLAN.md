# MongoDB Persistence Module — Implementation Plan

Planning document. **No module created, no Java written, no DDL run, nothing inserted.**

Data model reference: [`RELATIONAL-MODEL-PLAN.md`](RELATIONAL-MODEL-PLAN.md) — §5 (logical
schema), §13 (Mongo architecture), §17 (uniform decomposition), §18 (ER diagrams). This
document plans *how to build* the module that writes that model; it does not restate it.

## Environment — measured, design against these

| fact | value | consequence |
|---|---|---|
| mongod | **6.0.26**, localhost:27017 | `$jsonSchema`, partial indexes, `bulkWrite` all available |
| topology | **STANDALONE — `NoReplicationEnabled`** | **multi-document transactions are NOT available** |
| `maxBsonObjectSize` | 16,777,216 | §17.6 ceiling analysis applies |
| databases present | `admin`, `config`, `local` | nothing of ours exists yet |
| `mongosh` | on PATH | usable for verification scripts |
| build | Maven, Spring Boot 3.2.5 parent, **Java 17** | |
| coordinates | groupId `com.codeanalyzer`, version `1.0.0-SNAPSHOT` | |
| driver | `mongodb-driver-sync` already used by `jar-analyzer-core`, `flow-poc` | reuse; do not add a second client stack |

**The standalone constraint is the single most design-shaping fact here.** Every "wrap it in a
transaction" answer is unavailable. §5 and §9 below are written around that.

## Contents

- [x] 1. Module identity and boundaries — placement, dependencies, the I/O-gate constraint
- [x] 2. **What already exists** — reuse inventory, the collection-name collision, run-marker precedent
- [x] 3. Identity — the deterministic `_id` scheme in full (incl. the measured 1,025-byte `_id`)
- [x] 4. Package and class structure
- [x] 5. **Every RDBMS guarantee, and the mechanism that replaces it** (PK, FK, cascade, unique,
      not-null, check/enum, default, transactions, isolation, sequences, validation level)
- [x] 6. **Reconciliation with the existing PLAN-00…PLAN-13 series**
- [x] 7. **Every cardinality, handled explicitly** (1:1, 1:N, N:1, M:N, self-tree, self-DAG)
- [x] 8. Ingest pipeline — stages, ordering, batching, idempotency, resumability, runtime
- [x] 9. Verification and integrity — orphan refs, count reconciliation, **lossless round-trip proof**
- [x] 10. Schema versioning, migration and coexistence
- [x] 11. Failure modes and operability
- [x] 12. Testing strategy
- [x] 13. Phasing — M1…M7, first increment vertical
- [x] 14. Open questions and risks

> **Two findings reshaped this plan and are worth reading first.**
> (a) A MongoDB layer **already exists** in `analyzer-storage`, and one of its live collections is
> already called `analysis_run` — see §2.3 before naming anything.
> (b) `docs/plan/` already contains **14 planning documents, 11,208 lines**, including a 65-entity
> model and a 71-table schema. §6 reconciles with them; this plan defers to them where they decided
> first, and fills the `_id` gap they left open.

---

## 1. Module identity and boundaries

### 1.1 The finding that reshapes this plan

**A MongoDB persistence layer already exists in `analyzer-storage`.** Before proposing a new
module I inventoried it:

```
analyzer-storage/src/main/java/com/analyzer/storage/mongo/
  MongoConnection.java  MongoArtifactStore.java  MongoAnalysisRepository.java
  MongoDocumentGuard.java  MongoPlsqlEntityIndex.java  GridFsRolloverOutputStream.java
analyzer-storage/src/main/java/com/analyzer/storage/entity/
  PlsqlEntityMapper.java  FlowNodeRow.java  FlowEdgeRow.java  TableUsageRow.java
  OrdinalValues.java  EntityShapeException.java
```
with tests at `analyzer-storage/src/test/java/com/analyzer/storage/mongo/` (9 test classes
including `MongoIngestFailureTest`, `MongoOverflowSweepTest`, `MongoRepositoryConcurrencyTest`,
`MongoDocumentGuardTest`).

`analyzer-storage/pom.xml:77-84` already declares `mongodb-driver-sync` **4.11.2**, with a
comment stating the pin is deliberate:

> "Pinned to the version already in jar-analyzer-core/pom.xml:48-51 so the two modules cannot
> land two different drivers on one classpath — the same clash the aws.sdk.version property
> above exists to prevent, which fails at RUNTIME rather than in the build. mongodb-driver-sync
> is the blocking driver: the async/reactive one would pull a second Netty onto a Spring Boot
> classpath that already has one."

**Consequence: do not introduce a new Mongo client stack, and do not re-pin the driver.** Any
new module inherits 4.11.2 transitively from `analyzer-storage` and must not declare its own
`<version>`. A second driver version on the classpath is a runtime failure, not a build failure
— the worst kind.

### 1.2 The I/O gate is the deciding constraint on module placement

`unified-web/src/test/java/com/analyzer/arch/DirectFileIoGateTest.java` fails the build when a
class outside an allowlist touches the filesystem. Two facts from it:

- **`analyzer-storage` is wholly exempt** — `DirectFileIoGateTest.java:137`
  (`private static final String STORAGE_MODULE = "analyzer-storage";`) and the scan loop skips
  it (`if (module.equals(STORAGE_MODULE)) continue;`). It is the designated I/O module.
- Every other module needs an explicit `ALLOWED.put("module/Class", budget)` entry, and the
  budgets **ratchet down only** — the class comment at lines 30-32: *"The counts are budgets,
  not targets: they may only ever go down. A class that does less direct I/O than its budget
  also fails, with a note to lower the number."*

The ingest module must read ~122,641 JSON files. Doing that with `Files.walk`/`Files.newInputStream`
in a **new** module means adding allowlist entries and permanently owning a budget that can only
shrink. Reading through `ArtifactStore` instead costs nothing against the gate, because the
implementation lives in the exempt module.

**Recommendation: read exclusively through `ArtifactStore`.** Its SPI
(`analyzer-storage/src/main/java/com/analyzer/storage/ArtifactStore.java`) already provides
everything the ingest needs — `read(ArtifactKey)` (line 40), `list(ArtifactKey, boolean recursive)`
(line 69), `exists` (51), `size` (54), and the default `statPrefix` (105-108) which returns a
whole prefix's metadata in one call and is exactly what the `sourceDigest` computation
(RELATIONAL §13.5) needs.

**What that costs, stated honestly:**
- `ArtifactStore` has no "walk a directory tree cheaply" primitive beyond `list(prefix, true)`.
  For 321 endpoint directories × ~4,930 files this returns ~1.58 M `ArtifactEntry` objects if
  called at the jar root. **Mitigation:** the ingest never lists the endpoint tree — RELATIONAL
  §17.6/§12.2 establishes those files map to nothing, so ingest lists only the ~13 top-level
  jar documents and the 5 PL/SQL analysis directories.
- There is no streaming-JSON convenience; `read()` returns an `InputStream`, which is exactly
  what Jackson's `JsonParser` wants. No loss.
- Range reads exist (`read(key, offset, length)`, line 49) but are not needed.

**The judgement call:** an alternative is to put the ingest *inside* `analyzer-storage` and
avoid the SPI question entirely. I recommend **against** it — `analyzer-storage`'s own pom
comment (lines ~95-100) states it "depends on NO other project module", and ingest needs to
understand analysis-domain shapes. Putting domain knowledge in the storage module would break
the rule that makes it reusable. A separate module depending on `analyzer-storage` keeps both
properties.

### 1.3 Proposed module

| property | value |
|---|---|
| artifactId | **`analysis-mongo-ingest`** |
| groupId | `com.codeanalyzer` (matches the reactor) |
| version | `1.0.0-SNAPSHOT` |
| base package | `com.codeanalyzer.ingest.mongo` |
| Java | 17 (inherited from the Spring Boot 3.2.5 parent) |
| packaging | `jar` |

**Reactor position** — insert after `analyzer-storage`, before the modules that would consume
it. `pom.xml` `<modules>` becomes:

```xml
<module>analyzer-storage</module>
<module>analysis-mongo-ingest</module>   <!-- NEW: depends only on analyzer-storage -->
<module>plsql-config</module>
... unchanged ...
<module>unified-web</module>
<module>flow-poc</module>
```
Second position because its only project dependency is `analyzer-storage`. Maven would resolve
the order regardless, but declaration order that matches the dependency order is how the rest
of this file reads.

### 1.4 Dependencies — and what is deliberately excluded

**Depends on:**

| dependency | scope | why |
|---|---|---|
| `com.codeanalyzer:analyzer-storage` | compile | `ArtifactStore` (file reads), `MongoConnection`, `MongoDocumentGuard`, the `mongo`/`entity` packages, and the driver transitively |
| `com.fasterxml.jackson.core:jackson-databind` | compile | streaming `JsonParser` for the 49.3 MB `analysis.json`; already on every consumer's classpath via `spring-boot-starter-json`, and `analyzer-storage/pom.xml` already declares it for the same reason |
| `org.mongodb:mongodb-driver-sync` | **compile, NO `<version>`** | inherited at 4.11.2; declaring a version here re-creates the clash the pom comment warns about |
| `org.junit.jupiter:junit-jupiter` | test | |
| `org.assertj:assertj-core` | test | |

**Must NOT depend on:**

| excluded | why |
|---|---|
| `unified-web` | the web layer consumes ingest, never the reverse; depending on it would make the reactor cyclic |
| `plsql-parser`, `jar-analyzer-core`, the five dialect parsers | ingest reads *analysis output already on disk*. Depending on the producers would couple the persistence schema to parser internals and drag ANTLR onto the ingest classpath for no gain |
| `spring-boot-starter-web` / any Spring starter | this is a library and a CLI entry point, not a service. Keeping Spring out means the integration tests start in milliseconds |
| `mongodb-driver-reactivestreams`, `spring-data-mongodb` | both pull a second driver stack; `spring-data-mongodb` additionally imposes its own mapping and `_id` conventions, which conflict with the deterministic composite `_id` scheme (§3) |
| a second JSON library | Jackson is already there |

**Spring is excluded deliberately and this is a judgement call.** `spring-data-mongodb` would
give repositories and mapping for free. It is the wrong trade here: the `_id` scheme is
deterministic composite strings (RELATIONAL §13.1), the write path is `bulkWrite` of
pre-built `Document`s, and the constraint enforcement is `$jsonSchema` + unique indexes managed
explicitly (§5). Spring Data would be a layer to fight rather than use. The module exposes a
plain `MongoIngestService` that `unified-web` can wrap in a `@Bean` if it wants to.

### 1.5 What the module owns, and what it does not

**Owns:** the Mongo document schema for analysis output (collection names, `_id` recipes,
validators, indexes), the JSON→document mapping, the write path, the integrity checker, and the
ingest CLI.

**Does not own:** reading files (delegated to `ArtifactStore`), producing analyses (the parser
modules), serving queries (a future read-side; §8 covers coexistence), or the connection
mechanism if `MongoConnection` already provides one — §2 finalises that once the existing API is
confirmed.

---

## 2. What already exists — reuse inventory

**This section supersedes any impression from §1 that the module starts from nothing.** It does
not. The repo already contains a working Mongo layer, a run-marker protocol, a size guard, a
streaming mapper and a test harness — plus **fourteen prior planning documents**
(`docs/plan/PLAN-00`…`PLAN-13`, 11,208 lines) that this module must conform to rather than
re-decide. Building without reusing these would duplicate solved problems and, worse, would
collide with live collection names.

### 2.1 Existing components and the verdict on each

| component | file | verdict |
|---|---|---|
| `MongoConnection` | `.../storage/mongo/MongoConnection.java` | **REUSE as-is.** One pooled `MongoClient`, `applicationName("jar-plsql-analyzer")`, pings at open (:86) and fails fast naming `app.storage.mongodb.uri`. `unconfigured()` is a first-class state (:24), so the module degrades cleanly when Mongo is absent. |
| `MongoDocumentGuard` | `.../storage/mongo/MongoDocumentGuard.java` | **REUSE as-is.** Soft limit **12 MiB of UTF-8 bytes** (:66, :131), 25% under the BSON ceiling and deliberately above `MongoArtifactStore.MAX_INLINE_THRESHOLD` (8 MiB) "so the store's guard and this one cannot disagree". Over-limit payloads divert to `ArtifactStore` at `repo-overflow/<aa>/<sha256>` — ":39 Nothing is ever silently truncated", which is exactly the lossless rule of RELATIONAL §17.4. |
| `ArtifactStore` + `StorageFactory` | `.../storage/` | **REUSE.** §1.2's I/O-gate argument stands. `StorageFactory.settingsFor` reads system property then env var (:174). |
| `PlsqlEntityMapper` + `RowSink` | `.../storage/entity/PlsqlEntityMapper.java` | **REUSE the pattern, extend the coverage.** Streaming Jackson, one array element at a time (:287), `ARTIFACT = "api/nodes/*.json"`, fails loudly via `EntityShapeException` on unobserved shapes (:156). |
| `OrdinalValues` | `.../storage/entity/OrdinalValues.java` | **REUSE — and this is the one to notice.** It normalises absent/scalar/array into ordinal rows and is *written and tested but used by nothing* (`MongoPlsqlEntityIndex.java:78-80`). It is precisely the primitive the JAR spine needs for `parameterTypes`, `childIds`, `stringLiterals`, `interfaces`. |
| `MongoTestSupport` | `.../test/.../mongo/MongoTestSupport.java` | **REUSE.** §10 builds on it. |
| `MongoPlsqlEntityIndex` | `.../storage/mongo/MongoPlsqlEntityIndex.java` | **DO NOT EXTEND — see §2.3.** |
| `MongoAnalysisRepository` | `.../storage/mongo/MongoAnalysisRepository.java` | **DO NOT EXTEND.** Stores rows as opaque JSON strings; its own comment (:113) says "this is a document store used as an indexed row store, not yet as a query engine." |

### 2.2 The run-marker protocol is already invented — adopt it verbatim

RELATIONAL §13.5 proposed a `committedAt` marker because standalone mongod has no
transactions. **That pattern already exists in this codebase, twice**, with the same reasoning:

> `MongoPlsqlEntityIndex.java:92` — "The run marker again, not a transaction … MongoDB 6.0.26
> standalone; multi-document transactions need a replica set, and PLAN-06 rejects them anyway"

> `MongoAnalysisRepository.java:90` — "A multi-document transaction is rejected deliberately,
> per PLAN-06: it requires a replica set, holds resources for the whole crawl, and risks the
> 16 MB oplog entry limit"

The mechanism, from `MongoPlsqlEntityIndex.java:435-478`: insert marker `IN_PROGRESS` → write
children unordered → single-document `updateOne` flipping to `COMPLETE`, which is the
**arbitration point**; a loser discards children-first. A failed ingest deliberately leaves
`IN_PROGRESS` (:413) so a sweeper can find it (`sweepIncompleteEntityRuns`, :506).

Publication is enforced by a **partial unique index**, not by convention:
`entity_run_one_complete` on `{analysis:1}` with `partialFilterExpression {status:"COMPLETE"}`.
That is the same construct RELATIONAL §5.2 specified for "one current run per entry point",
already proven in this codebase. **Adopt it; do not reinvent `committedAt`.** The module's
marker field is `status ∈ {IN_PROGRESS, COMPLETE}`, matching the existing vocabulary.

Two corrections to RELATIONAL §13.5 follow from this:
- **`committedAt: null` is replaced by `status: "IN_PROGRESS"`.** A partial index cannot filter
  on `null` as cleanly as on an enum value, and the codebase already reads `status`.
- **Zero references to `startSession`, `ClientSession` or `TransactionOptions` exist anywhere in
  the repo.** The "use a 2-document transaction for the version flip" suggestion in RELATIONAL
  §13.5 step 3 is therefore **withdrawn** — see §5.8 for the standalone-safe replacement.

### 2.3 The collection-name collision — a live defect if unaddressed

`MongoAnalysisRepository` already owns a collection literally named **`analysis_run`**
(`MongoAnalysisRepository.java:123-125`, alongside `analysis_facet` and `analysis_row`), and
`MongoPlsqlEntityIndex` owns `entity_run`, `flow_node`, `flow_edge`, `table_usage`.

RELATIONAL §13.4 and §18 name the new spine collection **`analysis_run`** too — with an entirely
different document shape (deterministic string `_id`, `dialect`, `project`, `totals`, …) versus
the existing `{_id: UUID, dataset, analysis, version, table, status}`. Writing both into one
database would interleave two schemas in one collection, and the existing partial unique index
`run_one_complete` on `{dataset, analysis, version, table}` would apply to documents that have
none of those fields.

**This is not a naming quibble; it is a data-corruption path.** `MongoPlsqlEntityIndex.java:99`
already anticipated the hazard for its own case: *"It is a separate marker collection from
`analysis_run`, not a reuse of it."*

**Recommendation: namespace every new collection with a short prefix, `am_` (analysis model).**

| RELATIONAL name | module collection | collides with |
|---|---|---|
| `analysis_run` | **`am_run`** | existing `analysis_run` (`MongoAnalysisRepository`) |
| `proc_node` | `am_proc_node` | — |
| `statement` | `am_statement` | — |
| `db_object` | `am_db_object` | — |
| `dag_node` | `am_dag_node` | — |
| `java_class` / `java_method` | `am_java_class` / `am_java_method` | — |
| …all ~47 | `am_*` | — |

The prefix costs six characters and buys three things: no collision today, a one-line
`db.getCollectionNames().filter(n => n.startsWith("am_"))` for drop/inspect/migrate, and an
unambiguous answer to "is this collection the new model or the old index?". The alternative — a
**separate database** (e.g. `analyzer_model`) — is also viable and arguably cleaner, but
`MongoConnection` hands out a single `MongoDatabase` (:112) and threading a second database name
through `StorageConfig` touches shared config for little gain. **Recommend the prefix; revisit
the separate database if the model ever needs different backup or retention policy.**

### 2.4 Relationship to `MongoPlsqlEntityIndex` — additive, not a replacement

`MongoPlsqlEntityIndex` implements **3 of the 65 entities PLAN-04 models** (its own comment,
:64: *"PLAN-04 models 65 entities and PLAN-05 71 tables. Three are implemented here"*). It is a
**derived, discardable read index**, built lazily by re-reading `api/nodes/*.json` when a query
first needs it — not a write path. Its own framing (:56): *"It is additive and it changes
nothing … drop all four collections and the application still answers every question."*

What it does **not** cover, per its own enumeration (:62-87) and confirmed against
RELATIONAL §18: statements at row level (only `statementSummary` counts), predicates, joins,
`columnDetail`, cursors, dynamic SQL, sequences, DDL actions, the `table_column_usage` rows
recovered in RELATIONAL §17.4, the whole JAR spine (`java_class`, `java_method`, `invocation`,
`endpoint`, `dag_node`), and any `$jsonSchema` validation.

**The new module is a superset with different identity semantics**, and the identity difference
is why it cannot simply extend the existing collections:

| | `MongoPlsqlEntityIndex` | this module |
|---|---|---|
| `_id` for runs | `UUID.randomUUID()` (:387) | deterministic composite string (§3) |
| `_id` for children | driver-generated `ObjectId` | deterministic composite string |
| write trigger | lazy, on first query | explicit ingest |
| relationship to files | derived index, discardable | intended system of record (eventually) |
| re-ingest | invalidate + rebuild | idempotent upsert by `_id` |

A random `_id` requires a read-back to learn a child's parent key — the exact thing the user's
constraint forbids ("don't believe relation auto resolve"). **The two can coexist**: they share
`MongoConnection`, `MongoDocumentGuard` and `ArtifactStore`, and touch disjoint collections.
Whether the old one is eventually retired is a §8 migration question, not a §2 one.

### 2.5 `$jsonSchema` would be new to this codebase

Grep confirms **zero occurrences of `jsonSchema`, `createCollection` or `ValidationOptions`**
anywhere in the repo. All existing constraint enforcement is client-side guards plus unique and
partial indexes. RELATIONAL §13.6 leans heavily on `$jsonSchema` validators to recover
`CHECK`/`NOT NULL`/`ENUM`. That remains the right mechanism (§5), but it must be introduced as a
**new, justified capability** rather than presented as house style — and §5.9 addresses the
operational cost the existing code implicitly avoided by not using it.

---

## 3. Identity — the deterministic `_id` scheme in full

(Placed before §4 because the class structure follows from it.)

The requirement (RELATIONAL §13.1): every `_id` computable from natural keys **before** insert,
so parent and children can be written in any order by any number of writers with no read-back.

### 3.1 The measured problem: a real `_id` is 1,025 bytes

I generated the canonical `_id` for every entity against the live data. Worst cases:

| collection | recipe | **max `_id` length (measured)** |
|---|---|---|
| `method_invocation` | `{methodRef}~{invocationRef}` | **1,084 B** |
| **`java_method`** | `{runId}~{fqn}#{name}{descriptor}` | **1,025 B** |
| `invocation` (unhashed) | `{runId}~{owner}~{name}~{desc}~{opcode}~{line}` | 641 B |
| `java_field` | `{runId}~{fqn}~{name}` | 139 B |
| `endpoint` | `{runId}~{METHOD}~{fullPath}` | 127 B |
| `java_class` | `{runId}~{fqn}` | 119 B |
| `dag_node` | `{runId}~{contentHash}` | 54 B |

The 1,025-byte case is a real constructor:
`...AnalyzerController#<init>(Lcom/jaranalyzer/service/JarParserService;Lcom/jaranalyzer/service/CallGraphService;…)V`
— a Spring controller with many injected dependencies, whose JVM descriptor spells out every
parameter's fully-qualified type. This is **not** a generated-code edge case; it is ordinary
constructor injection, and it will get longer as dependencies are added.

**Does the 1,024-byte index-key limit actually bite?** I checked the live server rather than
assuming: `db.version()` = **6.0.26**, `featureCompatibilityVersion` = **6.0**, and
`getParameter: failIndexKeyTooLong` returns `MongoServerError: no option found to get` — the
parameter no longer exists. MongoDB removed the index key length limit in **4.2** for FCV ≥ 4.2,
so on this server a 1,025-byte `_id` **will insert and index successfully**.

**It is still the wrong design**, for three reasons that survive the limit's removal:
1. The `_id` index is the one index every collection has and every lookup uses. At 111,455
   `method_invocation` documents *(measured)* a ~1 KB key is ~110 MB of index for a collection
   whose payload is ~12 MB.
2. It is version- and configuration-dependent. Downgrading FCV, or a future engine, reinstates
   the limit — and it would reinstate it as *write failures on the largest classes*, which is
   the worst possible time to discover it.
3. Every ref field (`methodRef`, `invocationRef`) carries a copy of that string, so a 1 KB `_id`
   is paid again in each child document and each secondary index on a ref.

### 3.2 Recommendation: hybrid — readable `_id` with a hashed form above a threshold

| rule | applies to |
|---|---|
| **`_id` = the canonical string**, when it is ≤ **256 bytes** (UTF-8) | `analysis_run`, `proc_node`, `db_object`, `statement`, `dag_node`, `endpoint`, `java_class`, `java_field`, `sequence_obj`, `call_edge`, and every node-scoped child |
| **`_id` = `{runId}~#{sha256(canonical)[:32]}`**, when the canonical string exceeds 256 bytes | `java_method`, `method_invocation`, `invocation`, `method_field_access`, `method_string_literal` |
| **always** store the full canonical string in a `_key` field, indexed | every collection using the hashed form |

The `~#` marker after `runId` makes a hashed `_id` self-identifying: anything containing `~#` is
a digest and its plaintext is in `_key`. `runId` stays in the clear so the prefix-range delete of
a superseded run (RELATIONAL §13.5) still works on `_id` alone.

**Why 256 bytes.** Long enough that every PL/SQL collection and most JAR ones keep readable ids
(measured maxima: `java_class` 119, `endpoint` 127, `java_field` 139), short enough that the
worst `_id` is bounded at `len(runId) + 34` ≈ 70 bytes. Applied to the measured data, **only 5
of ~47 collections use the hashed form**, and the two that were over 1 KB drop to ~70 B.

**Why 32 hex chars (128 bits) of SHA-256.** Birthday collision probability at the measured
33,407 methods is ~10⁻²⁹; even at 10⁹ documents it is ~10⁻²⁰. Truncating to 24 chars (96 bits,
as RELATIONAL §13.1 suggested for `sql_text`) is also safe at these volumes but 32 costs 8 bytes
and removes the argument entirely. **`sql_text` keeps 24** — it is content-addressed on the body
hash and its `_key` would be the SQL itself, which is already stored in `body`.

**The trade, stated plainly.** Hashing costs debuggability: `db.java_method.find({_id: "..."})`
stops being something you can type from knowing the method name. Three compensations:
`_key` holds the plaintext and is uniquely indexed, so `find({_key: "com.Foo#bar()V"})` works
identically; the module exposes `IdCodec.forMethod(...)` so tooling computes the id rather than
guessing; and the hashed set is small and enumerable (5 collections, all JAR-side).

**Rejected alternative:** hash everything, for uniformity. Rejected because it makes the 90% of
collections that have short natural keys unreadable to buy consistency in a document store where
`_id`s are routinely eyeballed. Uniformity is not worth blinding the common case.

### 3.3 Escaping — measured

Canonical strings join components with `~`. Two questions: does `~` occur in real components,
and what about the other structural characters?

Measured across all five PL/SQL analyses and the whole jar analysis:

| character | occurrences in natural-key components | example |
|---|---|---|
| `~` | **0** | — |
| `/` | 31,473 in `descriptor`, 319 in `fullPath` | `(Lcom/jaranalyzer/service/WarParserService;…)` |
| `$` | 14,746 in `nodeId`, 12,615 in `descriptor`, 1,357 in `fqn`, 479 in `methodName` | `ANALYZER$ERP_USP_RUN_CYCLE`, `ApiLoggingFilter$CappedResponseWrapper`, `lambda$listRunLogs$11` |
| `.` | 1,633 in `fqn`, 6 in `fullPath` | `com.waranalyzer.controller.WarAnalyzerController` |
| `#` | **720 in `tableName`**, 21 in `objectName` | `#stage6` — and one object named literally `#` |
| `\0` | 0 | — |

So `~` is unused today. **That is not a licence to skip escaping.** Oracle and SQL Server both
permit `~` inside a quoted identifier (`CREATE TABLE "ODD~NAME"`), the PL/SQL side already
produces objects named `#`, and the analysis-name format `DBTYPE~PROJECT~ENV~ENTRYPOINT` is
user-supplied at the project/environment level. An unescaped scheme is one oddly-named table
away from two different entities sharing an `_id` — a silent overwrite, the exact failure mode
RELATIONAL §5.3 already caught once.

**Escaping rule** — applied to each component *before* joining:

```
~   →  ~1        (tilde: the separator)
#   →  ~2        (hash: the java_method class/member separator and the hashed-id marker)
\0  →  ~0        (defensive; not observed)
```
`~` is the only escape introducer, so the transform is prefix-free and reversible: unescape by
scanning for `~` and mapping the next character back. **`.`, `/` and `$` are deliberately not
escaped** — they occur in 45,000+ components, they are never used as separators in any recipe,
and escaping them would inflate every JAR `_id` by ~30% for no disambiguation. This is a
judgement call and the reason is that escaping must cover *separators*, not *punctuation*.

Worked example, a real value:
```
component  : ANALYZER$ERP_USP_RUN_CYCLE        → unchanged (no ~ or #)
component  : #stage6                            → ~2stage6
component  : ODD~NAME                           → ODD~1NAME
_id        : ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE~v1~ANALYZER.~2stage6
```

**Collision argument.** With every component escaped so that no component can contain a bare
`~`, the joined string is uniquely decomposable into its components. Two entities therefore
share an `_id` if and only if they share every natural-key component — which is the definition
of the natural key. For the hashed form, add the SHA-256 collision probability above. The
uniqueness of each underlying natural key is established in RELATIONAL §5.3 (the five-part
statement key, measured unique in all five dialects) and §17.2 (`(class, name, descriptor)`,
guaranteed by the JVM).

### 3.4 `IdCodec` — the single source of truth

One class, `com.codeanalyzer.ingest.mongo.id.IdCodec`, is the **only** place an `_id` is
constructed. Not a utility that mappers may use — the mappers receive ids from it and have no
string concatenation of their own. Contract:

```
String runId(Dialect d, String project, String env, String entryPoint, int version)
String runIdJar(String jarId, int version)
String procNode(String runId, String nodeId)
String dbObject(String runId, String schema, String objectName)
String statement(String runId, String nodeId, String op, int line, String schema, String table)
String javaClass(String runId, String fqn)
String javaMethod(String runId, String fqn, String name, String descriptor)   // may hash
String dagNode(String runId, String contentHash)
... one method per collection ...

record Id(String id, String key)      // key non-null only when hashed
static String escape(String component)
static List<String> decompose(String canonical)   // inverse; used by the round-trip proof (§7)
```

`decompose` exists so the verification step can prove escaping is reversible on real data rather
than on the three examples above — a property test over every natural key in the corpus
(§10). An id scheme whose inverse is untested is an id scheme whose collisions are undiscovered.

---

## 4. Package and class structure

```
analysis-mongo-ingest/src/main/java/com/codeanalyzer/ingest/mongo/
├── id/
│   ├── IdCodec.java              // §3.4 — the ONLY place an _id is built
│   ├── Ids.java                  // record Id(String id, String key)
│   └── Escaping.java             // escape/unescape + decompose; property-tested (§10)
├── schema/
│   ├── Collections.java          // the am_* name constants; one place, no literals elsewhere
│   ├── CollectionSpec.java       // record(name, validator, List<IndexSpec>, sizeBudget)
│   ├── IndexSpec.java            // record(name, keys, unique, partialFilter, background)
│   ├── Validators.java           // $jsonSchema documents per collection (§5.4-5.6)
│   └── SchemaInstaller.java      // createCollection + ensureIndexes, idempotent, drift-tolerant
├── model/
│   ├── plsql/   RunDoc, ProcNodeDoc, StatementDoc, DbObjectDoc, CallEdgeDoc,
│   │            CursorDoc, DynamicSqlDoc, ExceptionHandlerDoc, TxnStatementDoc,
│   │            SequenceDoc, SubtreeTableDoc, NodeTableAccessDoc, TableColumnUsageDoc,
│   │            SqlTextDoc, SourceFileDoc, ChunkStatementDoc
│   └── jar/     JavaClassDoc, JavaMethodDoc, JavaFieldDoc, FieldConstantDoc,
│                InvocationDoc, MethodInvocationDoc, DagNodeDoc, EndpointDoc,
│                EndpointPoolDoc, EndpointPoolLinkDoc, EndpointReachableDoc,
│                BundledJarDoc, BundledJarResourceDoc, AnnotationDoc, ExternalLinkDoc
├── map/
│   ├── ArtifactMapper.java       // interface: ARTIFACT constant + map(ctx, InputStream, DocSink)
│   ├── DocSink.java              // void emit(String collection, Document doc)
│   ├── plsql/  IndexJsonMapper, StatementsMapper, TablesIndexMapper, NodeDetailMapper,
│   │           EdgesMapper, CallGraphMapper, TablesJsonMapper, ChunkMapper, ResolverMapper
│   └── jar/    AnalysisJsonMapper, BundledJarsMapper, JarDepMapMapper, ExternalLinksMapper
├── write/
│   ├── BulkWriter.java           // batching, upsert-by-_id, byte-aware flush
│   ├── RunMarker.java            // IN_PROGRESS → COMPLETE protocol (§2.2)
│   └── WriteStats.java           // per-collection counters for §7 reconciliation
├── verify/
│   ├── OrphanRefChecker.java     // §7.1
│   ├── CountReconciler.java      // §7.2
│   └── RoundTripProver.java      // §7.3 — the lossless proof
├── MongoIngestService.java       // the façade: ingest(source) / verify(runId) / drop(runId)
└── cli/IngestMain.java           // main(); the only entry point
```

### 4.1 Contracts of the load-bearing classes

**`IdCodec`** — §3.4. One method per collection, returns `Id(id, key)`. No mapper concatenates
strings. Rationale: an `_id` scheme scattered across 30 mappers is an `_id` scheme with 30
subtly different escaping bugs.

**`ArtifactMapper`** — the extension point, following the convention
`MongoPlsqlEntityIndex.java:78-82` already states for the JAR spine: a new artifact gets *"a
separate class with its own `ARTIFACT` constant"*, not a branch in an existing mapper.

```java
public interface ArtifactMapper {
    String artifact();                       // e.g. "api/statements/index.json"
    void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException;
}
```
`map` **does not close the stream** — matching `PlsqlEntityMapper.map`'s existing contract
(:92). `IngestContext` carries `runId`, the `IdCodec`, and the lookup maps (`nodeId → _id`,
`objectKey → _id`) that make refs resolvable without read-back.

**`DocSink`** decouples mapping from writing, so a mapper can be unit-tested by collecting into
a list with no Mongo present — which is what makes §10's unit tier possible at all.

**`BulkWriter`** — batches `ReplaceOneModel` with `upsert(true)` keyed on `_id`. Flush policy
copies the existing precedent in `MongoAnalysisRepository.java:138-139`: **`BATCH_TARGET_BYTES =
4 MiB`, `BATCH_MAX_ROWS = 2_000`**, whichever trips first, `unordered`. Reusing those exact
numbers rather than inventing new ones means one tuning story for the codebase.

**`SchemaInstaller`** — idempotent. Creates each collection with its validator if absent,
applies it with `collMod` if present, then ensures indexes. Must tolerate index-spec drift the
way `MongoPlsqlEntityIndex.java:231-243` does (catching error codes **85** `IndexOptionsConflict`
and **86** `IndexKeySpecsConflict`, then drop-and-recreate) — otherwise a changed index
definition breaks every subsequent run.

### 4.2 Why mapping is separated from writing

The measured corpus is 82.9 MB of PL/SQL JSON and a 49.3 MB `analysis.json`. Mappers stream
(Jackson `JsonParser`, one element at a time, as `PlsqlEntityMapper.java:287` already does) and
emit to `DocSink`; `BulkWriter` batches. Nothing accumulates a full document set in memory.

The one deliberate exception: the **`IngestContext` lookup maps**. Measured sizes are 298
`nodeId`s, 570 `(schema,table)` keys, 3,784 DAG hashes, 1,633 class FQNs — tens of thousands of
short strings, a few MB. Holding them is what allows every ref to be computed at write time with
no read-back, which is the user's central constraint. This is the memory/purity trade and it is
worth making explicitly.

---

## 5. Every RDBMS guarantee, and the mechanism that replaces it

The user's requirement: *"primary key, foreign key, and all other constraint and feature oracle
or any other db provide by default so make sure handle properly."* Each guarantee is named, given
a mechanism, then given its residual gap — several cannot be fully replaced, and saying so is
more useful than claiming otherwise.

### 5.1 PRIMARY KEY, then `_id`

Native. `_id` is unique, immutable, always indexed; a duplicate insert fails with error **11000**
`DuplicateKey`. Section 3's deterministic composite string makes the primary key **be** the natural
key, so a duplicate is structurally impossible rather than merely detected.

The strongest instance is the one RELATIONAL §5.3 measured: `(nodeId, operation, line)` is **not**
unique (ORACLE 2,436 rows to 2,373 triples; POSTGRES 2,581 to 2,500; DB2 4,063 to 3,999), but the
five-part key including `(schema, tableName)` is unique in all five dialects. Encoding the
five-part key into `am_statement._id` makes the collision that currently corrupts
`access-details.js` **unrepresentable**.

**Residual gap:** none.

### 5.2 FOREIGN KEY, then ref field plus three-layer enforcement

Mongo has no `REFERENCES` and will never reject a dangling ref.

**Layer 1 — prevention at write time (primary control).** `IngestContext` holds the `_id` sets it
has emitted (measured: 298 nodeIds, 570 object keys, 3,784 DAG hashes, 1,633 class FQNs — small
enough to hold). Every ref is asserted present in its target set before being written. A violation
aborts the run, which is safe because the marker is still `IN_PROGRESS` and the run is invisible.

This works **only** because ids are deterministic: the writer knows a child's ref without having
written the parent. With `ObjectId`s it is impossible — the concrete reason §2.4 rejects extending
`MongoPlsqlEntityIndex`.

**Layer 2 — detection after commit.** `OrphanRefChecker` (§7.1) runs `$lookup` per ref field.
`$lookup` is banned from the *hot query path* (RELATIONAL §13.3 R3), not from a batch integrity
job; the distinction is stated so nobody "fixes" it later.

**Layer 3 — shape.** `$jsonSchema` enforces that a ref is a string matching its target's `_id`
pattern. It cannot check existence — no Mongo mechanism can — but it catches null, numeric or
malformed refs.

**Residual gap: referential integrity is not enforced by the database.** A direct
`db.am_statement.insertOne(...)` bypassing the module writes a dangling ref and nothing stops it.
This is the single largest thing lost versus Oracle.

### 5.3 ON DELETE CASCADE / RESTRICT, then prefix-range delete

Every `_id` begins with `runId`, so retiring a run is an indexed range delete per collection:

```js
const HI = String.fromCharCode(0x7f);   // DEL: above every character the escaper can emit
db.am_statement.deleteMany({ _id: { $gte: RUN + "~", $lt: RUN + HI } })
```
The upper bound is safe because §3.3's escaping never emits `0x7f`. This is the same technique
`MongoArtifactStore.java:254-256` already uses for prefix scans (there `[p+"/", p+"0")`, exploiting
`'0'` = 0x30 succeeding `'/'` = 0x2F).

Belt and braces: every document **also** carries a plain indexed `runId` field, so the delete can
equally be `deleteMany({runId: RUN})`. Two independent routes, which matters because range-delete
correctness depends on the escaping being right.

**Order:** children first, marker last — the reverse of write order, and the discipline
`MongoPlsqlEntityIndex.java:435-478` already uses when a losing writer discards. A crash mid-delete
leaves a `COMPLETE` marker with missing rows; §7.2 detects it and re-running is idempotent.

This matches PLAN-05 §7.1 cascade class 1 (owned content trees). PLAN-05 class 2
(`ON DELETE SET NULL` for independently-existing things) maps to leaving the ref string in place
and letting §7.1 report it — Mongo cannot null it automatically.

**ON DELETE RESTRICT:** PLAN-05 §7.1 already concluded *"nothing. There is no entity whose deletion
should be blocked by a child."* That resolves RELATIONAL §13.7's proposed RESTRICT on
`endpoint.rootRef` — **drop the requirement**, rely on §7.1 detection.

### 5.4 UNIQUE, including partial, then unique indexes

| SQL | Mongo |
|---|---|
| `UNIQUE (a, b)` | `createIndex({a:1,b:1}, {unique:true})` |
| `UNIQUE ... WHERE is_current` | `createIndex({...}, {unique:true, partialFilterExpression:{status:"COMPLETE"}})` |
| `UNIQUE (upper(name))` | store a canonicalised `nameUpper` field, index that |

The partial form is exactly `entity_run_one_complete` in `MongoPlsqlEntityIndex.java:169-217`, and
it is PLAN-05's `uq_run_current` (`ON analysis_run (subject_kind, subject_id, kind) WHERE is_current`):

```js
db.am_run.createIndex(
  { kind:1, dialect:1, project:1, environment:1, entryPoint:1 },
  { unique:true, partialFilterExpression:{ status:"COMPLETE" }, name:"am_run_one_current" })
```
Two `IN_PROGRESS` runs for one entry point are allowed; two `COMPLETE` are not. **The index is the
concurrency control** — §5.9.

**Expression indexes do not exist in Mongo.** Every `upper(object_name)` in RELATIONAL §7.2 becomes
a stored pre-canonicalised field written by the mapper.

**A trap worth an assertion:** a unique index over a *missing* field treats all missing values as
one `null` and collides. Every uniquely-indexed field must be `required` in the validator, or the
index must be partial on its existence. `SchemaInstaller` should assert the pairing, not trust it.

### 5.5 NOT NULL, then `required` plus a null-excluding `bsonType`

`required` means **the key is present**; it does not exclude an explicit `null`. True NOT NULL
needs both:

```js
required: ["_id","runId","nodeRef","objectRef","operation","accessKind","line"],
properties: { operation: { bsonType: "string" } }   // bsonType excludes null
```

**Interaction with RELATIONAL §17.1 Rule 1 (omit null/false/empty on write).** These pull opposite
ways: Rule 1 saves a measured 24-26% of bytes by omitting absent values; `required` demands
presence. Resolution: **`required` lists only fields that always have a value**, decided by the
measured non-null census. `callType` (2,285 of 2,285) is required; `dispatchType` (2,284 of 2,285)
is not. This is what RELATIONAL §2.2's census was for.

This also satisfies PLAN-11 obligation 4: *"Emit nothing for null — absent and null must not become
a zero row, or counts drift."*

### 5.6 CHECK / ENUM / domain, then `$jsonSchema` — narrowed

**PLAN-04 §1.5 Trap 2 and PLAN-05:66-69 warn against enumerating parsed vocabularies**, and the
warning is better evidenced than RELATIONAL §5.1's proposal: *"CHECK constraints only where the
value set is closed by construction, never on a parsed vocabulary"* — because `statementSummary`
keys were measured at 27/28/12/11/11 distinct across dialects with dialect-exclusive tokens, and
*"every enum in this system has been observed to grow"* (PLAN-02 §4.3).

**Reconciliation — split the vocabularies in two:**

| closed **by construction**, so `enum` | parsed vocabulary, so `bsonType:"string"` only |
|---|---|
| `status` (IN_PROGRESS/COMPLETE/SUPERSEDED) — we mint it | `operation`, `accessKind`, `objectType` — parser output |
| `kind` (PLSQL/JAR) — we mint it | `stereotype`, `callType`, `dispatchType`, `module`, `domain` |
| `poolType` (BEAN/EXTERNAL_CALL/DYNAMIC_FLOW) — we mint it | `valueSource`, `dynamicSql.kind` (1 observed value each) |
| `join.kind` after canonicalisation — we mint the canonical form | `exception_name` (open by construction) |

**Recommendation: enum only what this module mints.** RELATIONAL §5.1 typed `operation` and
`accessKind` as enums on the strength of five dialects; PLAN-02's longitudinal evidence is stronger.
A sixth dialect must not fail ingest. Record the observed domain in a comment, not a constraint.

Structural constraints stay, because they are about shape rather than vocabulary:
```js
line:  { bsonType: "int", minimum: 1 },
joins: { bsonType:"array", items:{ bsonType:"object", required:["kind","rightRef","line"] } }
```

**Multi-field CHECKs are not expressible.** `CHECK ((kind='PLSQL') = (dialect IS NOT NULL))` has no
`$jsonSchema` form. **Recommendation: assert in the mapper**, because the failure should name the
source file, which a server-side validator error cannot.

### 5.7 DEFAULT, then mapper-applied and written explicitly

Mongo has no column defaults. `edgeType` defaults to `"CALLS"` (the JSON omits it for 2,257 of 2,258
edges, emitting `FIRES_TRIGGER` for 1); `isResolved` to `false`; `fromCatalog` to `false`.

**Rule: write defaults explicitly; do not omit them under Rule 1.** Rule 1 is for values genuinely
absent in the source; a default is a value the model asserts. Omitting it makes "absent because
defaulted" indistinguishable from "absent because unknown" — the ambiguity the lossless rule exists
to prevent.

### 5.8 Transactions and atomicity, then the run marker

**Unavailable**: mongod 6.0.26 standalone, `NoReplicationEnabled`, verified. Also rejected on design
grounds — PLAN-06:206-208:

> "A multi-document transaction is rejected deliberately: it requires a replica set, holds resources
> for the whole crawl (minutes), and risks exceeding the 16 MB oplog entry limit on large batches.
> The run marker gives the same visible atomicity at a fraction of the cost, and it degrades
> gracefully on a standalone server."

```
1. insert  am_run { _id: runId, status:"IN_PROGRESS", startedAt, sourceDigest }
2. write   all children, unordered bulkWrite, any order, any parallelism
3. verify  7.1 orphan check + 7.2 counts        <- still invisible if this fails
4. update  previous COMPLETE run -> status:"SUPERSEDED"
5. update  am_run -> status:"COMPLETE"          <- single-doc, atomic; the publish point
```

**Step order 4-before-5 is deliberate and reverses RELATIONAL §13.5 step 3**, which assumed a
transaction. If 5 ran first, the partial unique index of §5.4 would reject it while the old run is
still `COMPLETE`. Marking the old one `SUPERSEDED` first means a crash between 4 and 5 leaves *zero*
current runs — recoverable and detectable — rather than a uniqueness violation that cannot resolve.

**What is observable mid-ingest.** Children of an `IN_PROGRESS` run **are** visible to anything
querying those collections directly. The contract is therefore: **every reader resolves `runId`
through `am_run` where `status="COMPLETE"` first**, then queries children. That is discipline, not
enforcement, and it is the honest statement of the gap.

Alternatives rejected: staging collections renamed on commit (`renameCollection` is atomic per
collection, so 47 renames are not atomic together); per-run collection name suffixes (explodes the
collection count).

### 5.9 Isolation and concurrent runs

Two ingests of the same analysis race. Both write children under **different** `runId`s (different
versions), both attempt the `COMPLETE` flip, and the partial unique index lets exactly one succeed;
the loser gets 11000 and discards its children by prefix-delete. This is
`MongoPlsqlEntityIndex.java:435-478` exactly.

Two ingests of different analyses never conflict: disjoint `_id` prefixes.

**Version allocation is the one racy step.** `version = 1 + max(existing)` read-then-write is not
atomic. **Recommendation: `findOneAndUpdate` with `$inc` on a per-entry-point counter document** —
one atomic single-document operation, no transaction needed.

This corrects RELATIONAL §13's claim that sequences are unnecessary: **they are necessary in exactly
this one place**, and Mongo supplies the atomicity as a single-document update.

### 5.10 Validation level and action

**Recommendation: `validationLevel: "strict"`, `validationAction: "error"` — but only with §5.6's
narrowed enums.**

- `strict` over `moderate`: documents are written once and replaced wholesale by `_id`; there is no
  legacy population that `moderate` exists to protect.
- `error` over `warn`: a violating document is a mapper bug. Failing surfaces it at the point of
  cause with the run still invisible. `warn` writes it to the log *and the database*, which is how a
  corrupt document reaches a UI.

**The cost, stated:** no existing collection in this repo uses validators (§2.5), so this introduces
an operational concept the team has not managed before; and `strict`/`error` means a mapper meeting
a new dialect **fails the ingest**. That is acceptable only because §5.6 removes parsed vocabularies
from the enums. With RELATIONAL §5.1's original enum list, `strict`/`error` would be the wrong
choice — a sixth dialect would take ingest down. **The two decisions are coupled and must move
together.**

### 5.11 Summary

| RDBMS feature | mechanism | fully replaced? |
|---|---|---|
| PRIMARY KEY | deterministic composite `_id` | **yes** |
| UNIQUE / partial UNIQUE | unique + partial indexes | **yes** |
| NOT NULL | `required` + null-excluding `bsonType` | **yes** |
| CHECK (single-field), ENUM | `$jsonSchema`, **minted vocabularies only** | partly — parsed vocabularies stay open by design |
| DEFAULT | mapper, written explicitly | yes (by convention) |
| SEQUENCE / AUTO-INCREMENT | not needed except run version, then `findOneAndUpdate($inc)` | **yes** |
| CHECK (multi-field) | mapper assertion | partly — not server-enforced |
| FOREIGN KEY | write-time assertion + post-commit checker + shape validation | **no** — not server-enforced |
| ON DELETE CASCADE | `_id` prefix range delete + `runId` index | yes (application-driven) |
| ON DELETE SET NULL | ref left in place, reported by checker | **no** |
| ON DELETE RESTRICT | none; PLAN-05 concluded none is needed | n/a |
| Multi-statement transaction | run marker, single-doc publish flip | **no** — standalone mongod |
| Read isolation mid-write | reader contract via `am_run.status` | **no** — discipline |

---

## 6. Reconciliation with the existing PLAN-00…PLAN-13 series

**`docs/plan/` contains 14 documents, 11,208 lines, and it already covers most of this ground.**
Any new module plan that ignores them would re-decide settled questions and contradict shipped
code. This section states where this plan **conforms**, where it **fills a gap**, and where it
**diverges and why**. It also corrects two things in `RELATIONAL-MODEL-PLAN.md`.

### 6.1 Status of the series (PLAN-00:100-112)

| plan | subject | status |
|---|---|---|
| 00 | overview, constraints C1-C5 | index |
| 01/02/03 | artifact inventories (JAR / PL/SQL / Claude+UI) | **done** |
| 04 | entity model — **65 entities, E1-E65** | **done** |
| 05 | relationships and normalization — **71 `CREATE TABLE`** | **done** |
| 06 | Mongo 16 MB chunking | **done** |
| 07 | storage mode mongodb | **done** |
| 08 | UI contract and queries | in progress |
| 09 | migration and rollout — increments A-H | in progress |
| 10 | workflow and gates | done |
| 11 | field mapping — 7,715 field paths | (not in index) |
| 12 | analysis size | (not in index) |
| 13 | PL/SQL visibility and search | (not in index) |

### 6.2 Hard constraints this module inherits (PLAN-00:9-45)

> "A design that violates one of them is wrong, however elegant."

- **C1 — `ArtifactStore`'s public surface is FROZEN.** 74 classes depend on it; a new backend is a
  new implementation plus one branch in `StorageFactory`. **§1.2's decision to read through
  `ArtifactStore` conforms**, and C1 also forbids widening the SPI to suit ingest.
- **C2 — 16 MB BSON.** *"Nothing may assume 'one analysis, one document'."* RELATIONAL §17.6's
  per-collection ceiling analysis is the detailed form of this.
- **C3 — the UI must not break.** PLAN-03's access-pattern table is the acceptance criterion.
- **C4 — credentials never enter shared storage.** `CredentialGuardStore` wraps every backend
  (`StorageFactory.java:204`). **This bears directly on RELATIONAL §18(a)**, which put
  `connectionName` and `jdbcUrl` on `analysis_run` marked "sensitive". **Under C4 that is
  probably not allowed.** Flagged in §12.
- **C5 — no data loss; rollback must not be "restore from backup".**

### 6.3 Where this plan fills a genuine gap

**The `_id` scheme.** PLAN-05 is explicitly PostgreSQL DDL with **UUIDv7 surrogate PKs**
(PLAN-05:66-69) and defers the Mongo mapping to PLAN-06 (PLAN-05:7-8: *"PLAN-06 maps it to MongoDB
collections; the mapping is mechanical except in the places flagged [MONGO DIVERGES]"*). **PLAN-06
contains zero occurrences of `_id` or `ObjectId`.**

So the `_id` scheme is **undefined in the entire series**, and §3 fills it rather than contradicting
it. That said, the difference is real and must be named:

| | PLAN-05 (Postgres) | this plan (Mongo) |
|---|---|---|
| primary key | UUIDv7 surrogate | deterministic composite string |
| natural key | separate `uq_*` UNIQUE constraint | **is** the `_id` |
| child needs parent's key | read it back, or generate parent first | computes it |

UUIDv7 is right for Postgres, where a FK is enforced and a surrogate keeps indexes narrow. It is
wrong for the user's stated Mongo constraint — *"don't believe relation auto resolve"* — because a
random id **must** be read back before a child can reference it. **Recommendation: keep §3's scheme
for Mongo and record it as the `[MONGO DIVERGES]` case PLAN-05 anticipated.** PLAN-05 already has
precedent for a deterministic string PK: `jar_artifact.jar_id text PRIMARY KEY` via
`JarNameUtil.normalizeKey` (PLAN-05:124).

### 6.4 Where this plan conforms — adopt, do not re-derive

| PLAN decision | where | effect here |
|---|---|---|
| **Run marker, not transaction** | PLAN-06:197-208 | §5.8 adopts verbatim |
| **12 MB soft limit, estimate before insert** | PLAN-06:117-137 | reuse `MongoDocumentGuard.SOFT_LIMIT_BYTES`; §7 asserts it |
| **Overflow order: field-to-artifact, then split deeper, then whole-element artifact** | PLAN-06:127-132 | supersedes RELATIONAL §17.6's GridFS suggestion for `bundled_jar_resource` |
| **No GridFS for model data** | PLAN-06:158-161 | *"Adding GridFS would mean two blob mechanisms."* **RELATIONAL §17.6's GridFS recommendation is withdrawn** — use `ArtifactStore`, which `MongoDocumentGuard` already does |
| **Never key on ordinals, depth, or array position** | PLAN-04 §1.3, PLAN-11 obligation 5 | confirms RELATIONAL §17.4's decision to derive `nodeId`/`depth`/`childCount` rather than store them |
| **Split boundary = one document per FlowNode** | PLAN-06:102-116 | matches RELATIONAL §17.3 |
| **Append-only families: one doc per entry, key `(scope_id, seq)`** | PLAN-06:88-100 | applies to logs, outside this module's scope |
| **snake_case, singular, unprefixed names** | PLAN-05 | §2.3's `am_` prefix **diverges** — see §6.6 |
| **Cascade classes 1/2/3** | PLAN-05 §7.1 | §5.3 adopts |
| **Run versioning: new run, then atomic cutover, then prune** | PLAN-05 §7.2 | §5.8, adapted for no transactions |

**PLAN-11's headline rule is now a design constraint on §4:**

> "**Never map by field name across artifacts. Map by (artifact, path).** Two of the six hazards are
> name collisions where the same identifier means a count in one document and a collection in
> another. Name-keyed reconciliation … produces a wrong schema in exactly those cases, and nothing
> detects it later."

§4's `ArtifactMapper` interface carries an `artifact()` constant precisely for this, and the six
mapper obligations (PLAN-11:122-133) become the mapper acceptance criteria in §11.

### 6.5 Corrections to `RELATIONAL-MODEL-PLAN.md`

Reading the series surfaced four items to fix in the companion document:

1. **GridFS recommendation withdrawn** (RELATIONAL §17.6, `bundled_jar_resource`). PLAN-06:158-161
   rejects GridFS for model data; `ArtifactStore` + `MongoDocumentGuard` is the house mechanism and
   already content-addresses at `repo-overflow/<aa>/<sha256>`.
2. **`sequence` claim corrected.** RELATIONAL §13 said sequences are unnecessary; §5.9 shows run
   versioning needs exactly one atomic counter.
3. **The two-document transaction in RELATIONAL §13.5 step 3 is impossible here** — no replica set.
   §5.8 replaces it with ordered single-document updates.
4. **`analysis_run` name collides** with a live collection (§2.3).

And two figures differ between the documents because they measure **different runs of a moving
target**, which is worth stating rather than silently reconciling:

| quantity | RELATIONAL (measured by me) | PLAN-12 (measured earlier) |
|---|---|---|
| `analysis.json` size | **49.3 MB** | 77.0 MB at caps 30/30/50k; **53.1 MB** with ANTLR excluded; 40.9 MB after pooling |
| `callTreeNodes` | **3,784** | 4,089 |
| classes / endpoints | **1,633 / 321** | 1,623 / 317 |

PLAN-12 also records that the same document once reached **~69 GB** uncapped, and that
*"1,219 of the 1,623 analysed classes are `com.plsql.parser.*`"* — the bundled ANTLR parser, where
`PlSqlParser` alone has 1,245 methods and 47,585 invocations. **This independently confirms
RELATIONAL §17.2's finding** that the 5.5 MB `java_class` outlier is generated code, and it
strengthens the argument: the tail is not merely large, it has been observed to explode.

**PLAN-12's ruled-out list should not be re-tested** (PLAN-12:50-60): recursion, depth
(30 to 45 costs 1.3%), unbounded class scanning, embedded decompiled source (0.0-4.0%, mean 322 B
per node), and the storage backend. Note this **contradicts** the "sourceCode is the size driver"
intuition; PLAN-00:86-94 marks that theory *"MEASURED AND WRONG"* and identifies
`methods.invocations` (33-85% of the document) as the real driver — consistent with the 111,455
`method_invocation` rows RELATIONAL §8.2 measured.

### 6.6 Where this plan diverges, and why

**Divergence 1 — collection naming (`am_` prefix).** PLAN-05 uses unprefixed snake_case
(`analysis_run`, `flow_node`, `java_class`). But `MongoAnalysisRepository.java:123-125` **already
owns `analysis_run`** with an incompatible shape, and `MongoPlsqlEntityIndex` owns `flow_node`.
Following PLAN-05's naming literally would write two schemas into one collection.

**Recommendation: keep the `am_` prefix**, and record it as a deliberate departure. The alternative
— a separate database — is cleaner in principle but `MongoConnection.database()` hands out exactly
one `MongoDatabase` (:112), so it means threading a second name through `StorageConfig`. Revisit if
the model ever needs separate backup or retention.

**Divergence 2 — `$jsonSchema` validators.** Absent from PLAN-04/05/06 and from the codebase (§2.5).
This plan introduces them (§5.5-5.10) as the mechanism for NOT NULL and CHECK. **Narrowed by
PLAN-04 §1.5 Trap 2** so that parsed vocabularies are never enumerated.

**Divergence 3 — testing against a real local mongod.** PLAN-07 §6 decided to build a
`FakeMongoServer` because the measured environment had no Docker, no Testcontainers and no `mongod`.
**That environment has changed**: mongod 6.0.26 is running on localhost:27017, and
`MongoTestSupport.java:15-31` already documents the reversal — *"There is no fake server here, and
that is a deliberate reversal of PLAN-07 §6 … A skipped test is honest; a green test against a fake
seek is not."* §10 follows the code, not the superseded plan.

### 6.7 Entity-numbering: map to E-numbers, do not invent a parallel vocabulary

PLAN-04 defines **E1-E65** and both PLAN-04 and PLAN-05 are cited throughout the codebase by those
ids. RELATIONAL §18 named ~47 collections independently. **Recommendation: every collection in this
module carries its E-number in the `Collections` constant's javadoc**, e.g.

```java
/** E24 flow_node (PLAN-04 §2.2) / RELATIONAL §18(a) PROC_NODE. */
public static final String PROC_NODE = "am_proc_node";
```
so the three vocabularies stay tied together. Where RELATIONAL discovered an entity PLAN-04 lacks —
notably **`table_column_usage`** from `tables.json` (RELATIONAL §17.4 Reversal 3, 132 measured rows)
— it is a genuine addition and should be proposed as a new E-number rather than quietly added.

Conversely, PLAN-04 models **entity families this module does not touch**: the Claude/LLM group
(E47-E53), cross-reference (E54-E56), conversion jobs, chat, prompts and the job queue (E57-E65).
This module's scope is the JAR and PL/SQL analysis output only. **Scope boundary stated so the
module is not later blamed for gaps it never claimed.**

---

## 7. Every cardinality, handled explicitly

The user asked for *"one to many, many to many, many to one, one to one"* each handled properly.
Each pattern below uses a **real** relationship from RELATIONAL §18, and states five things:
representation, who writes the link and when, both read paths, the index making each direction one
seek, and the failure mode when the counterpart is missing.

Universal rule, from which most of the table follows: **the child stores the parent's `_id`, and
the child's own `_id` is prefixed by it.** So the "up" direction needs no index at all — the ref is
already in hand — and the "down" direction needs one index per ref field.

### 7.1 One-to-many (1:N) — `proc_node` to `statement`

Measured: 15,824 statements over 298 distinct nodes (53:1).

- **Representation.** `am_statement.nodeRef` holds `am_proc_node._id`. No array on the parent —
  53 average and 235 worst *(measured, DB2)* is beyond what belongs in a parent document, and
  statements are searched independently (`PlsqlSearchController`).
- **Who writes it, when.** The statement mapper, at the moment it builds the statement. It computes
  `nodeRef = IdCodec.procNode(runId, nodeId)` from the `nodeId` **already present in the statement
  row** — no lookup, no read-back. Parent may not exist yet.
- **Read down** (node to its statements): `find({nodeRef: X})`.
- **Read up** (statement to its node): the ref is a field; one `findOne({_id: doc.nodeRef})`, or
  zero queries if the caller only needs `nodeName`, which is denormalised onto the statement
  (RELATIONAL §13.3).
- **Index.** `{runId:1, nodeRef:1, line:1}` — one seek down, ordered by line, which is how the node
  detail screen renders.
- **Failure mode.** A statement whose `nodeRef` names a missing node. Prevented by §5.2 layer 1,
  detected by §7.1's checker. The statement still renders, because `nodeName` is denormalised —
  degraded, not broken.

Same shape for: `proc_node`→`cursor_decl`/`dynamic_sql`/`exception_handler`/`txn_statement`
(1,421/1,618/963/793 measured), `java_class`→`java_method` (33,407 over 1,633),
`java_class`→`java_field`, `db_object`→`table_column_usage`, `bundled_jar`→`bundled_jar_resource`.

### 7.2 Many-to-one (N:1) — `statement` to `db_object`

Measured: 15,824 statements referencing 570 distinct objects (27.8:1).

Structurally identical to 1:N — it is the same edge read from the other end — but the **write-side
concern differs**: the mapper must not create a `db_object` per statement. `IngestContext` holds
`objectKey -> _id` and the object documents are emitted once, from `api/tables/index.json`, before
statements are mapped.

- **Read down** (object to statements using it): `find({objectRef: X})` — index
  `{runId:1, objectRef:1, operation:1, line:1}`. **This is the hottest query in the product**
  (`access-details.js:338`, `table:<name>` capped at 500).
- **Read up:** `schema`, `tableName`, `objectType` are denormalised onto the statement, so the
  common render needs **zero** queries.
- **Failure mode.** Measured and real: 17 SQLSERVER node documents carry
  `{"schema":null,"name":null}` (RELATIONAL §17.0c). Those produce **no `objectRef` at all** — the
  field is omitted and `unresolvedName` carries the raw text. The model must not invent an object
  to satisfy a ref.

### 7.3 Many-to-many (M:N) — `java_method` to `invocation`

Measured: **111,455 refs over 64,446 distinct invocations** — genuinely M:N, since one invocation
is referenced by several methods.

- **Representation: a junction collection**, `am_method_invocation`, with
  `_id = hash(methodRef ~ invocationRef)` (§3.2 — the unhashed form measures 1,084 B).
  Fields: `runId`, `methodRef`, `invocationRef`.
- **Why not an array on the method.** `invocationRefs` measures max 6,132 B and mean 23 B — it
  *would* fit. Rejected because §17.6 splits it out anyway on code-governed grounds, and because a
  junction gives the reverse index for free. **This is a judgement call**: an array on the method
  plus a multikey index would also work and would halve the document count. **Recommendation:
  junction**, because "which methods call this invocation?" is a real question and a multikey index
  on a 33,407-document collection answers it less directly than a 111,455-document junction with
  two single-field indexes.
- **Write.** The `analysis.json` mapper emits junction rows as it walks `classes[].methods[]`. Both
  refs are computable: `methodRef` from the enclosing class and method, `invocationRef` from the
  pool index it is resolving. The pool is mapped first so the index-to-`_id` array is in hand.
- **Read down** (method to its invocations): `find({methodRef: X})`, index `{methodRef:1}`.
- **Read up** (invocation to its callers): `find({invocationRef: Y})`, index `{invocationRef:1}`.
- **Failure mode.** A junction row referencing a missing method or invocation is invisible in both
  directions — it silently narrows results rather than erroring. This is the M:N-specific hazard and
  the reason §7.1's checker must cover **both** ref fields of every junction, not just one.

**M:N by embedded ref array**, the alternative used for the pools: `am_endpoint.beanRefs[]`,
`externalCallRefs[]`, `dynamicFlowRefs[]` (measured 106/2,027/364 for endpoint 001, pools of
220/2,080/1,635). RELATIONAL §17.6 splits these to `endpoint_pool_link` for size. Both forms appear
in the model, and the rule distinguishing them is: **embed the ref array when the parent is small
and the array is bounded by the parent's own attributes; use a junction when either side is
independently queried or the array is code-governed.**

### 7.4 One-to-one (1:1) — `java_field` to `java_field_constant`

Measured: 52 of 8,649 fields have an oversized `constantValue` (max 21,845 B).

- **Representation.** `am_java_field_constant._id = "{fieldRef}~const"` — derived from the parent's
  `_id`, so the child's key **is** the relationship. `am_java_field.constantRef` points back.
- **Why a separate document at all.** Pure size: the parent's worst case falls from 131,146 B to
  ~1.2 KB *(estimated)*, and the constant is read by nothing on a hot path.
- **Read down:** `findOne({_id: fieldRef + "~const"})` — **no index needed**, it is the `_id`. This
  is the payoff of deriving a 1:1 child's key from its parent.
- **Read up:** strip the suffix, or read `runId`/`fieldRef` from the child.
- **Failure mode.** `constantRef` set but the child missing: the UI shows a field with no constant.
  Detectable, and mild. The reverse — child present, parent gone — is caught by the same checker.
- **Optionality.** 1:1 here is really 1:0..1. The model has **no mandatory 1:1**; every such pair
  would be a single document instead. That is worth stating, because a mandatory 1:1 split across
  two documents in a store with no transactions is a torn write waiting to happen.

Same shape: `analysis_run` to its counter document (§5.9).

### 7.5 Self-referencing tree — `proc_node.parentRef`

Measured: 356 nodes, `parentNodeId` null at root.

- **Representation.** `am_proc_node.parentRef` holds another `am_proc_node._id`, or is **omitted**
  at the root (not null — Rule 1).
- **Write.** Computable from `parentNodeId` in the same JSON object. **This is where deterministic
  ids earn their place most visibly:** RELATIONAL §9.2 needed a *two-pass* ingest under FK
  constraints because a parent might not exist yet. Here there is one pass and no ordering
  constraint at all.
- **Read down** (children): `find({parentRef: X})`, index `{runId:1, parentRef:1}`.
- **Read up:** the ref is in hand. Walking to the root is N single-`_id` lookups, N = `depth`,
  measured max 11 — bounded and cheap.
- **Failure mode.** A cycle would loop a naive walker. PL/SQL call graphs **do** contain cycles
  (3 measured in the ORACLE run), but those are in `call_edge`, not the parent tree. Any walker must
  still carry a visited set and a depth cap; §7.6 states why.

### 7.6 Self-referencing DAG — `dag_node.childRefs[]`

Measured: 3,784 nodes, 7,281 edges, and **20,300,167,285 logical nodes if expanded**.

- **Representation.** An **ordered array of child `_id`s embedded on the parent**. The content
  hashes already in `childIds[]` *are* the ids (§3, RELATIONAL §13.1), so the edge is materialised
  first-hand with no read-back and no resolution pass. This is the single closest fit between the
  user's constraint and the data as it already exists.
- **Why an array and not a junction**, unlike §7.3: order is semantic (call order), the array is
  small (mean 46 B, max 3,384 B), and the parent is always read when the edge is traversed.
- **Read down:** the array is in the document — **zero queries**; then a batched
  `find({_id: {$in: childRefs}})`.
- **Read up** ("who calls this?"): `find({childRefs: X})` on the **multikey index `{childRefs:1}`**.
- **Failure mode, and the one that matters most.** A traversal that does not carry a visited set
  does not fail — it **hangs**, because 48 nodes are marked `recursive` and the expansion is
  2.03 × 10¹⁰. `$graphLookup` is **not** an option: 100 MB memory cap, and its `maxDepth` is not
  cycle protection. **Traversal must be application-side BFS with an explicit visited set**, and
  §10 must include a test asserting that traversal of endpoint 001 terminates.

### 7.7 Summary

| cardinality | example | representation | down index | up cost |
|---|---|---|---|---|
| 1:N | `proc_node` to `statement` (1:53) | child ref field | `{runId:1,nodeRef:1,line:1}` | free (ref in hand) |
| N:1 | `statement` to `db_object` (27.8:1) | child ref field | `{runId:1,objectRef:1,operation:1,line:1}` | free (denormalised) |
| M:N | `java_method` to `invocation` (111,455/64,446) | junction collection | `{methodRef:1}` and `{invocationRef:1}` | one seek |
| M:N (small, ordered) | `endpoint` to pools | ref array or junction | `{endpointRef:1}` | one seek |
| 1:0..1 | `java_field` to constant | child `_id` derived from parent `_id` | none — it is `_id` | free |
| self, tree | `proc_node.parentRef` | ref field, omitted at root | `{runId:1,parentRef:1}` | free, depth ≤ 11 |
| self, DAG | `dag_node.childRefs[]` | ordered embedded array | multikey `{childRefs:1}` | free |

**No relationship in the model requires a read-back before a write.** That is the property the
user asked for, and it holds because every `_id` is computed (§3) and every ref is a copy of a
computed `_id`.

---

## 8. Ingest pipeline

### 8.1 Stages

```
S0  resolve source     ArtifactStore.statPrefix(prefix) -> file list + sizes + mtimes
S1  compute digest     sha256 over sorted (relPath, size, sha256); short-circuit if unchanged
S2  allocate version   findOneAndUpdate($inc) on the counter doc  (§5.9)
S3  install schema     SchemaInstaller: collections + validators + indexes (idempotent)
S4  write marker       am_run { status: IN_PROGRESS }
S5  map + write        per artifact, in the order of §8.2, streaming -> DocSink -> BulkWriter
S6  verify             §9.1 orphan refs, §9.2 counts, §9.3 round-trip sample
S7  supersede          previous COMPLETE run -> SUPERSEDED
S8  publish            am_run -> COMPLETE
S9  report             WriteStats to stdout and to am_run.stats
```

S6 sits **before** S7/S8 deliberately: a run that fails integrity never becomes visible, and the
operator is left with an `IN_PROGRESS` run to inspect rather than a published bad one.

### 8.2 Ordering — and why there barely is one

Under FK constraints RELATIONAL §9.2 needed a 19-step topological order. Here **the only ordering
constraint is S4 before S5** (the marker must exist so a sweeper can find an abandoned run).
Everything in S5 can run in any order, because every ref is computed rather than looked up.

There is one *efficiency* ordering, not a correctness one — the pooled/shared entities are mapped
first so `IngestContext`'s lookup sets are populated for the write-time FK assertion of §5.2:

```
PL/SQL:  api/tables/index.json   -> db_object, db_column        (570 objects; fills objectKey set)
         tables.json             -> table_column_usage          (132 rows, RELATIONAL §17.4)
         api/index.json          -> am_run scalars, proc_node stubs (fills nodeId set)
         api/nodes/*.json        -> proc_node + node-scoped children
         chunks/chunk_*.json     -> chunk_statement             (fidelity source, §3.4)
         api/statements/*.json   -> statement (+ embedded arrays)
         edges.json              -> call_edge
         api/call_graph.json     -> call_edge_unique
JAR:     analysis.json           -> invocation pool, endpoint pools, dag_node,
                                    java_class, java_method, java_field, endpoint  (one pass)
         bundled-jars.json       -> bundled_jar (+ resources)
         jar-dep-map.json        -> jar_dependency
         external-links*.json    -> external_link
```

If the assertion set is incomplete when a ref is written, the module has two options: fail, or defer
the assertion to S6. **Recommendation: defer to S6 and log at S5.** Failing on ordering makes the
"any order" property a lie, and S6 catches the same problem before anything is visible.

### 8.3 Batching

Reuse the existing constants rather than invent new ones —
`MongoAnalysisRepository.java:138-139`: **`BATCH_TARGET_BYTES = 4 MiB`, `BATCH_MAX_ROWS = 2,000`**,
whichever trips first, **unordered** `bulkWrite` of `ReplaceOneModel` with `upsert(true)`.

- Unordered so one rejected document does not abort the batch; the driver reports per-item errors
  and `BulkWriter` collects them.
- `ReplaceOneModel` + upsert, not `insertOne`, is what makes re-running idempotent (§8.4).
- Byte-aware flushing matters because document sizes vary by 3 orders of magnitude — `am_statement`
  at ~754 B *(measured max)* versus `am_bundled_jar_resource` at ~465 KB.

### 8.4 Idempotency

Three layers, strongest first:

1. **`sourceDigest` short-circuit (S1).** If a `COMPLETE` run exists with the same identity and the
   same digest, exit having written nothing.
2. **Upsert by deterministic `_id`.** Re-running over unchanged input rewrites identical documents
   to identical `_id`s. Converges; no duplicates possible.
3. **Version isolation.** A re-run under a new version writes under a different `_id` prefix and
   **cannot collide with the previous version in any collection** (RELATIONAL §13.5).

Note layers 2 and 3 are alternatives, not cumulative: **resuming** a failed run reuses its version
(layer 2 does the work); a **deliberate re-ingest** takes a new version (layer 3). §8.5 decides
which applies.

### 8.5 Resumability after a crash

On startup, if an `IN_PROGRESS` run exists for this identity with a **matching `sourceDigest`**:
resume it — same `runId`, same version, re-run S5 from the beginning. Upserts make the already-written
documents no-ops, so the cost is re-mapping, which §8.7 shows is cheap. If the digest **differs**,
the source changed underneath a failed run: abandon it (prefix-delete, §5.3) and start a new version.

Finer-grained resumption — per-artifact checkpoints — is deliberately **not** proposed. It would need
a checkpoint document per artifact and the recovery logic to trust it. At the runtimes in §8.7 the
whole ingest is cheaper than the bookkeeping. PLAN-05's `checkpoint_entry` (E63) exists for the
Claude crawl, which is minutes-to-hours; this is not that.

### 8.6 Progress reporting and partial failure

`WriteStats` counts per collection: attempted, upserted, modified, failed, bytes. Emitted to stdout
every N seconds and stored on `am_run.stats` at S9, which makes §9.2's reconciliation a comparison
between two numbers already in the database.

On partial failure the run stays `IN_PROGRESS` — deliberately, matching
`MongoPlsqlEntityIndex.java:413` — so a sweeper can find it. **Nothing is rolled back**, because
there is nothing to roll back to and the documents are invisible. The operator sees a non-zero exit,
the failing collection and `_id`, and the source artifact path.

### 8.7 Runtime estimate

**Measured inputs.** JSON parsing is not the bottleneck: `analysis.json` (49.3 MB) parses in
**0.43 s** and walking all 33,407 methods and 111,455 invocation refs takes **0.01 s** (Python
`json`; Jackson streaming should be comparable or better, and uses far less memory). All five PL/SQL
statement indexes parse in **0.11 s** combined.

So runtime is dominated by **Mongo round trips**. Total documents, from RELATIONAL §8:

| side | documents |
|---|---|
| PL/SQL, 5 analyses | ~95,000 |
| JAR, 1 jar | ~303,000 |
| **total** | **~400,000** |

At `BATCH_MAX_ROWS = 2,000` that is ~200 `bulkWrite` round trips, plus ~47 collection/index
setup calls.

**Estimate: 30-90 seconds for the full corpus on localhost** *(estimated, not measured — no ingest
has been run)*. Reasoning: unordered `bulkWrite` of ~2,000 small documents to a local mongod is
typically 50-200 ms; 200 batches gives 10-40 s, and index maintenance on ~47 collections with
~2 indexes each adds to it. The 465 KB `bundled_jar_resource` documents and the 12 MB guard checks
are individually slower but few.

**The estimate is uncertain in one direction that matters:** `$jsonSchema` `strict` validation runs
per document on the server, and no measurement of its cost on this corpus exists. If validation
proves expensive, the mitigation is to validate on a sample during bulk load and run a full
validation pass in S6 — but that weakens §5.10, so measure before deciding.

**What is explicitly not in the runtime:** the 321 endpoint directories (~1.58 M files, 1.17 GB).
They map to nothing (RELATIONAL §12.2, §17.6) and are never opened. Reading them would dominate
everything else — this is the single largest performance decision in the design and it was made in
the data model, not here.

---

## 9. Verification and integrity

Three checkers, run at S6 before publication and re-runnable afterwards via
`MongoIngestService.verify(runId)`. All three write their findings to `am_run.verification`, so a
published run carries the evidence it was checked.

### 9.1 Orphan reference checker

For every ref field in every collection (the list is derived from `Collections`, not hand-written,
so a new collection cannot be forgotten):

```js
db.am_statement.aggregate([
  { $match: { runId: RUN } },
  { $lookup: { from:"am_proc_node", localField:"nodeRef", foreignField:"_id", as:"p" } },
  { $match: { p: { $size: 0 } } },
  { $project: { _id:1, nodeRef:1 } },
  { $limit: 100 }
])
```

**Both ref fields of every junction must be checked**, not just one — §7.3's failure mode is a
junction row that is invisible from both ends and silently narrows results.

**Nullable refs are expected, not orphans.** The checker needs an allowlist of refs that may be
absent, with the measured justification: `dag_node.methodRef` (2,080 of 2,285 absent — third-party
methods have no `java_class`), `endpoint.handlerMethodRef` (overload ambiguity),
`node_table_access.objectRef` and `subtree_table.objectRef` (17 SQLSERVER rows with
`{"schema":null,"name":null}`), `statement.sqlTextRef` (186 of 2,436 statements have no SQL text).
**A checker that flags these is a checker nobody runs** — the allowlist is what keeps the signal.

### 9.2 Count reconciliation

Compare document counts against counts taken from the source JSON during mapping (`WriteStats`) and
against RELATIONAL §8's measured figures, which act as regression baselines:

| collection | expected | source |
|---|---|---|
| `am_statement` | **15,824** | measured, 5 analyses |
| `am_proc_node` | **356** | `api/nodes/*.json` file count |
| `am_db_object` | **571** | `api/tables/index.json` rows |
| `am_call_edge` | **6,970** | `edges.json` array lengths |
| `am_dag_node` | **3,784** | `analysis.json.callTreeNodes` |
| `am_java_method` | **33,407** | flattened `classes[].methods` |
| `am_invocation` | **64,446** | `invocationPool` |
| `am_method_invocation` | **111,455** | sum of `invocationRefs` lengths |
| `am_table_column_usage` | **132** | `tables.json`, and cross-checks `columnCoverage.columnRows: 132` |

The last row is worth its place: it reconciles two independent sources, so a mismatch means the
mapper is wrong rather than the baseline being stale.

**Count files, never `max(ordinal)`** — PLAN-09:801-873's rule, because ordinal gaps exist and
PLAN-04 §1.3 forbids keying on ordinals at all.

**These are baselines, not invariants.** A new analysis legitimately changes them. The check is
"counts match what *this ingest* mapped", with the measured figures as a secondary signal that the
corpus is the one the plan was written against.

### 9.3 Lossless round-trip proof

This is how RELATIONAL §17.4's *"don't lose any field"* requirement becomes mechanical rather than
promised.

**Method.** For a sampled set of source documents, reconstruct the original JSON from Mongo and
diff it against the file:

```
1. pick a source artifact, e.g. api/nodes/ANALYZER$ERP_USP_RUN_CYCLE.json
2. read the corresponding am_proc_node + its children (cursors, dynamicSql, ...)
3. run the inverse mapper: documents -> JsonNode
4. compare with mapper.readTree(originalBytes), field by field
5. report every path present in one and absent in the other
```

**The comparison must be field-path aware, not string equality.** Key ordering, whitespace and
number formatting all differ legitimately. Comparing `JsonNode.equals` is the right granularity —
the same technique `SidecarCompactnessTest` already uses in this repo
(`readTree(compact).equals(readTree(pretty))`, per PLAN-10).

**Three classes of expected difference**, which the prover must encode rather than ignore:
- **Derived fields not stored** (RELATIONAL §17.5): `nodes.json`'s `nodeId`, `depth`, `childCount`.
  The prover computes them and asserts they match — that is what "derivable" claimed, so it should
  be tested, not assumed.
- **Deduplicated storage**: `directTables` is served from the same rows as `tables` (§17.4
  Reversal 2). The prover must request both names and get byte-equal arrays.
- **Omitted null/false/empty keys** (Rule 1): absent in Mongo, present-as-null in the source. The
  prover normalises both sides by dropping null/false/empty before comparing — and this is the one
  place the proof is weaker than it looks, because it cannot distinguish "correctly omitted" from
  "wrongly dropped". **Mitigation: a separate assertion that the set of omitted paths is exactly
  the set whose source value was null/false/empty.**

**Coverage.** Full round-trip over all 1,050 PL/SQL files is feasible (82.9 MB); over the JAR side
it is `analysis.json` only, since the endpoint tree is not ingested. **Recommendation: prove
100% of PL/SQL artifacts and 100% of `analysis.json`'s top-level arrays in CI**, rather than
sampling — the corpus is small enough that sampling buys nothing but doubt.

### 9.4 Document size assertion

PLAN-00:121-131 requires *"no document exceeds 16 MB, asserted by a test rather than by
inspection"*, and PLAN-06:135-137 asks for a synthetic scale test at **2,000 classes / 6,000 nodes**.

Two checks:
- **Real corpus:** after ingest, `db.<coll>.aggregate([{$project:{sz:{$bsonSize:"$$ROOT"}}}, {$sort:{sz:-1}}, {$limit:1}])`
  per collection, asserted under the **1 MB ingest ceiling** of RELATIONAL §17.8 — 16× stricter than
  the BSON limit, and currently expected to pass on everything (largest predicted:
  `am_bundled_jar_resource` at ~465 KB).
- **Synthetic scale:** the PLAN-06 test, asserting the 12 MB `MongoDocumentGuard` limit and that an
  oversized element is **diverted to `ArtifactStore`, not truncated or rejected**.

---

## 10. Schema versioning, migration and coexistence

### 10.1 Collection naming and schema version

All collections are `am_*` (§2.3, §6.6). A `am_meta` collection holds one document:

```js
{ _id: "schema", schemaVersion: 1, installedAt: ISODate(), collections: [ ... ] }
```
`SchemaInstaller` compares `schemaVersion` at startup and refuses to run against a **newer** schema
than it knows — the cheap guard against an old binary writing documents a new reader will
misinterpret.

### 10.2 Rolling out a model change

Three kinds, with different costs:

| change | mechanism | needs re-ingest? |
|---|---|---|
| **additive field** | write it; readers tolerate absence | no — old runs simply lack it |
| **new collection** | `SchemaInstaller` creates it | only if it must be populated for old runs |
| **changed field meaning or removed field** | new `schemaVersion`; re-ingest affected runs | **yes** |

**Re-ingest is the migration mechanism, not `updateMany`.** The source JSON is still on disk and
ingest is idempotent and cheap (§8.7, 30-90 s estimated for the whole corpus). Writing migration
scripts that transform documents in place would mean maintaining two code paths that must agree —
the mapper and the migrator — with no way to test that they do. **Recommendation: never
`updateMany` the model; delete the run and re-ingest.** This is only affordable because the files
remain the system of record; §10.4 is where that stops being true.

Validator changes are the exception that needs care: `collMod` applies a new validator to a
collection that may hold documents written under the old one. With `validationLevel: strict`, those
documents are not re-validated at rest, but the **next** write to them fails. Since the module
replaces documents wholesale rather than updating them, and re-ingest rewrites all of them, this is
consistent — but it must be sequenced: `collMod` **then** re-ingest, never the reverse.

### 10.3 Index build strategy

Follow the existing precedent: `background(true)` on every index
(`MongoPlsqlEntityIndex.java:169-217`), and tolerate spec drift by catching error codes **85**
(`IndexOptionsConflict`) and **86** (`IndexKeySpecsConflict`) then drop-and-recreate (:231-243).

One divergence from RELATIONAL §9.4's advice to "drop and rebuild indexes around the load": at
~400,000 documents the build cost is small, and dropping indexes would remove the **unique** ones
that §5.4 relies on for concurrency control during the load. **Recommendation: keep indexes in
place during ingest.** The advice was written for a Postgres bulk load and does not transfer.

### 10.4 Coexistence with the file store

PLAN-09 already decided this and the decision is well-argued (PLAN-09:436-483):

> "**The recommendation: dual-read, not dual-write.** Do not write both. Write to one store; **read
> from both**."

with three reasons: legacy data is never mutated in place so migrated JSON *cannot* drift; at the
measured 0.5 MB/s uplink dual-write reimposes a 9-minute upload; and there is no atomicity across
S3 and Mongo, so every dual-write design "ends in a reconciliation job — which is the migrator we
already have."

**This module is an importer, and that is the right shape.** It reads analysis output that already
exists on disk and writes it to Mongo. It is **not** a write path from the analysers, so the
dual-write question does not arise for it at all.

The existing feature flags (PLAN-09:538-594) already express the coexistence:
```
app.storage.mode       = local | s3 | mongodb          # where bytes live
app.storage.repository = auto | artifact-scan | mongodb | mongo-then-artifact-scan
```
**Recommendation: this module ships behind neither flag initially.** It is a CLI (`IngestMain`) that
an operator runs; nothing reads its collections yet. That makes increments 1-4 of §11 completely
safe — they cannot affect a running system, because no code path reaches them.

A read side is a **separate, later** piece of work that would implement `AnalysisRepository` over
`am_*` and slot into `app.storage.repository`. Naming it here as out of scope is deliberate: PLAN-00
C3 ("the UI must not break") and PLAN-08's contract work both bear on it, and conflating import with
serving is how an importer acquires a UI contract it cannot meet.

**The point of no return** is PLAN-09's increment H — deleting migrated JSON. This module must not
approach it. As long as the files exist, §10.2's "delete and re-ingest" migration works and a bad
schema decision costs minutes. **Recommendation: state as a module invariant that the files remain
the system of record until a read side has been in production for a full retention window.**

---

## 11. Failure modes and operability

### 11.1 mongod unavailable

| when | behaviour |
|---|---|
| **at startup** | `MongoConnection.open` pings and throws `IllegalStateException` naming `app.storage.mongodb.uri` (`MongoConnection.java:86`). Fail fast, before any file is read. Its own comment: *"an unreachable server lets the context start and fails on the first artifact write instead"* — the anti-pattern it exists to prevent |
| **mid-ingest** | the current `bulkWrite` throws; `BulkWriter` propagates; the run stays `IN_PROGRESS`; exit non-zero. **Nothing is rolled back and nothing needs to be** — the documents are invisible |
| **during verification (S6)** | same; the run stays `IN_PROGRESS`, so it never publishes |
| **between S7 and S8** | the dangerous window: the old run is `SUPERSEDED`, the new one not yet `COMPLETE`, so **zero current runs** for that entry point. Detectable by §11.3, recoverable by re-running S8 |

The S7/S8 window is the residual cost of having no transaction, and §5.8 chose it deliberately over
the alternative (a uniqueness violation that cannot resolve itself). **Its duration is one
single-document update** — milliseconds — but it is not zero and the plan should not pretend it is.

### 11.2 Half-written runs — detection and cleanup

A sweeper, modelled on `MongoPlsqlEntityIndex.sweepIncompleteEntityRuns` (:506):

```
find am_run where status = "IN_PROGRESS" and startedAt < now - threshold
  -> for each: prefix-delete children (§5.3), then delete the marker
```

**The age threshold is not a nicety.** `MongoAnalysisRepository.java:536` states it exactly:
*"The age threshold is not a nicety; it is the only thing protecting an ingest in flight."* A
sweeper with no threshold deletes the run currently being written. **Recommendation: 6 hours**,
against an estimated 30-90 s ingest — three orders of magnitude of headroom, so a pathologically
slow run is never mistaken for a dead one.

Cleanup order is children-then-marker (§5.3): if it dies partway, the marker is still
`IN_PROGRESS` and the next sweep resumes. Marker-first would strand children with nothing pointing
at them and nothing to find them by — except the `runId` index, which is why §5.3 keeps it.

### 11.3 What the operator sees

`IngestMain` exit codes: `0` published, `1` mapping/validation failure, `2` verification failure,
`3` Mongo unavailable, `4` source unreadable.

A health command reports, per identity:
- runs by status, with age — `IN_PROGRESS` older than the threshold is the actionable line
- **entry points with zero `COMPLETE` runs** — catches the §11.1 S7/S8 window
- orphan-ref counts from the last verification
- largest document per collection against the 1 MB ceiling (§9.4)

The second item deserves its own alert rather than being buried in a table: it is the only state
where data exists but the application cannot see it, and it is silent otherwise.

### 11.4 Validator rejection

With `validationAction: "error"` (§5.10) a non-conforming document fails its write. The driver
reports `WriteError` code **121** `DocumentValidationFailure`, which — unhelpfully — does not say
*which* rule failed.

**Mitigation, because this will otherwise cost hours the first time it happens:** on a 121, the
`BulkWriter` re-runs the offending document through a **client-side** copy of the same schema and
reports the specific failing path and value, plus the source artifact and JSON pointer. That
duplicates the schema in two places, which is a real cost; the alternative is an error message that
says only "document failed validation" against a 400,000-document ingest.

### 11.5 Disk and memory

**Mongo storage:** estimated **under 120 MB** for all six analyses (RELATIONAL §8.4) — negligible.

**Ingest memory:** bounded by the batch (4 MiB) plus `IngestContext`'s lookup sets (tens of
thousands of short strings, a few MB) plus Jackson's streaming buffer. **The one risk is
`analysis.json`**: at 49.3 MB, a DOM parse is ~300-500 MB of Java heap *(estimated)*. §4.2 requires
streaming for exactly this reason, and it is the difference between a module that runs in a default
JVM and one that needs `-Xmx2g`.

PLAN-12 records that this document has been **69 GB** in a pathological configuration. Streaming is
not optional here; it is the only thing that makes the module robust to the input growing again.

---

## 12. Testing strategy

### 12.1 The repo's conventions, which this follows

Three, all load-bearing:

1. **A test that cannot fail is worthless.** `MongoTestSupport.java:15-31`: *"A skipped test is
   honest; a green test against a fake seek is not."* PLAN-10 on `no-local-writes.sh`: *"The harness
   is self-tested: it was checked to fail when a file is planted, because a verifier that cannot
   fail proves nothing."*
2. **Gates ratchet, never relax.** `DirectFileIoGateTest` budgets *"may only ever go down"*;
   PLAN-09 §7.1: *"Never raise a budget to go green."*
3. **Conformance suites are not edited to pass.** PLAN-09:834-838 enforces
   `git diff --exit-code` on the conformance test file: *"If it needs to relax a case, that case is
   a real behavioural difference and a finding to report, not a test to edit."*

### 12.2 Local mongod, not Testcontainers

**Recommendation: reuse `MongoTestSupport` exactly as it is.** It connects to
`mongodb://localhost:27017` (overridable with `-Dtest.mongodb.uri`), uses scratch DB
`analyzer_d_test`, pings with a 2-second server-selection timeout and calls
`assumeTrue(false, "No MongoDB at ... — skipping")` when absent.

This **contradicts PLAN-07 §6**, which specified a `FakeMongoServer` (~800-1,200 lines) because the
environment then had no Docker, no Testcontainers and no `mongod`. That environment has changed —
mongod 6.0.26 is running — and the code already made the reversal deliberately. **Following the code
over the superseded plan is right here**, and §6.6 records it.

Not Testcontainers: no Docker in the measured environment, and it would add a dependency to get a
server that is already running. Not a fake: this module leans on `$jsonSchema` validation, partial
unique indexes, `$bsonSize` and `bulkWrite` error semantics — precisely the behaviours a fake models
wrongly and silently.

**The cost, stated:** tests skip on a machine without mongod, so CI must run one or the Mongo tier
is silently absent. A green build that skipped every Mongo test looks identical to one that ran
them. **Recommendation: a test that asserts the Mongo tier actually ran** — count executed tests and
fail if zero — so "skipped everywhere" cannot masquerade as passing.

### 12.3 The tiers

**Unit — no Mongo, the bulk of the suite.**
- `IdCodecTest`: recipe per collection; escaping round-trips; the measured worst cases
  (`java_method` at 1,025 B unhashed → hashed form ≤ 70 B).
- **`EscapingPropertyTest`**: for every natural-key component in the real corpus (~45,000 measured
  values containing `.`/`/`/`$`/`#`), assert `decompose(join(escape(c) for c)) == components`.
  This is the collision argument of §3.3 turned into a test, and it must run over real data rather
  than generated strings — the awkward values (`#`, `lambda$listRunLogs$11`) are real.
- Mapper tests: feed a real artifact, collect into a list `DocSink`, assert documents. No Mongo, so
  these are milliseconds and cover the PLAN-11 obligations.

**Integration — real mongod.**
- `SchemaInstallerTest`: idempotent re-install; index spec drift (create with wrong options, assert
  drop-and-recreate); validator applied via `collMod`.
- `ConstraintTest`, one per row of §5.11: duplicate `_id` → 11000; two `COMPLETE` runs → 11000 on
  the partial index; a document violating `required` → 121; a null in a `bsonType:"string"` field
  → 121. **Each must be shown to fail when the constraint is removed** — that is convention 1.
- `RunMarkerTest`: kill after S5, assert nothing visible via the `status="COMPLETE"` contract;
  resume, assert convergence.
- `ConcurrentIngestTest`: two ingests of the same analysis; assert exactly one publishes and the
  loser leaves no children. Modelled on `MongoRepositoryConcurrencyTest`.
- `PrefixDeleteTest`: ingest two versions, delete v1 by range, assert v2 is untouched — the
  correctness of §5.3's range bound, which depends on the escaping.

**Golden-data — the acceptance tier.**
Ingest the real ORACLE analysis (`data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE`,
2,436 statements measured) and assert the §9.2 counts exactly. Using real data rather than fixtures
follows PLAN-09:801-873, which reuses *"real identifiers … verbatim from PLAN-05 §8 rather than
invented test data"*.

**The round-trip proof, automated (§9.3).** For every PL/SQL artifact: ingest, reconstruct, compare
`JsonNode`s, assert no path is lost. This is the mechanical form of the lossless requirement and it
is the single most valuable test in the plan — it is the only one that would have caught the
`tables.json` omission that RELATIONAL §17.4 found by hand.

**Scale test (PLAN-06:135-137).** Synthetic 2,000 classes / 6,000 nodes; assert every document
under the 12 MB guard and that an oversized element diverts to `ArtifactStore` rather than being
truncated or rejected.

### 12.4 What is deliberately not tested

Driver behaviour (4.11.2 is a dependency, not our code), and Mongo's own index semantics beyond the
one `PrefixDeleteTest` that depends on collation ordering. Testing those tests MongoDB.

### 12.5 Gates this module must not trip

- **`DirectFileIoGateTest`** — the module reads via `ArtifactStore`, so it should add **zero**
  `ALLOWED` entries. If a reviewer finds one being added, §1.2's decision was abandoned somewhere.
- **`/storage-check` (`scan.py`)** — exits non-zero on any unclassified direct I/O op.
- **`no-local-writes.sh`** — ingest writes to Mongo only; a local write is a defect.
- **`ArtifactStoreConformanceTest`** — untouched; this module adds no `ArtifactStore` implementation.

That last point is worth stating plainly: **this module changes no existing file except
`pom.xml`'s `<modules>` and one new `<dependency>`.** If a diff shows otherwise, the boundary in
§1.5 has leaked.

---

## 13. Phasing

Following PLAN-09's convention: lettered increments, each *"independently shippable, independently
valuable, and independently revertible"*, each with a definition of done that is **a command, not
an opinion**. Numbered M1-M7 to avoid colliding with PLAN-09's A-H.

**The first increment is end-to-end for one small entity chain, not a horizontal layer.** A
horizontal "build all the mappers" phase produces nothing testable until the last one lands, and
hides every integration problem until then.

### M1 — end-to-end vertical slice: `am_run` + `am_db_object` + `am_table_column_usage`

The smallest chain that exercises every mechanism: a run marker, a 1:N relationship, an embedded
array (`dbColumns`), a deterministic `_id`, a unique index, a validator, and the round-trip proof.
Chosen over the obvious `proc_node`/`statement` slice because it is ~700 documents rather than
16,000, and because `table_column_usage` is the entity RELATIONAL §17.4 nearly lost — proving it
first means the hardest-won finding is the first one locked down.

**Done when:** `mvn -pl analysis-mongo-ingest test` green; ingesting the ORACLE analysis yields
exactly **42** `am_db_object` and **132** `am_table_column_usage` documents; the round-trip proof
passes for `api/tables/index.json` and `tables.json`; re-running changes nothing (idempotency);
`DirectFileIoGateTest` and `/storage-check` unchanged; the module builds in the reactor.

### M2 — the PL/SQL node chain: `am_proc_node` + node-scoped children

Adds the self-referencing tree (§7.5), five 1:N children, and `subtree_table`.

**Done when:** 356 `am_proc_node`, 1,421 cursors, 1,618 dynamic SQL, 963 exception handlers,
793 txn statements, 70 sequence uses, 11,411 subtree rows; `parentRef` resolves for all 356 with
exactly the roots absent; round-trip passes for all `api/nodes/*.json`; **`directTables` and
`tables` both return byte-equal arrays** (§9.3).

### M3 — `am_statement` and the embedded arrays

The volume increment: 15,824 documents with 60,711 embedded children.

**Done when:** counts match §9.2 for all five dialects; the **five-part `_id`** is proven unique by
ingesting all five analyses with zero upsert-overwrites (assert `upserted == attempted`); the hot
query `{runId, objectRef, operation}` is served by an index (`explain` asserts `IXSCAN`, not
`COLLSCAN`); round-trip passes for `api/statements/index.json`.

The upsert-overwrite assertion is the important one: it is the mechanical proof that RELATIONAL
§5.3's collision finding was handled, and it would fail loudly against the three-part key.

### M4 — `am_call_edge`, `am_call_edge_unique`, cycles

**Done when:** 6,970 edges; `call_edge_unique` carries `count`, `lines[]`, `minDepth`, `backEdge`
(the six fields RELATIONAL §17.4 promoted from derivable to stored); 3 cycles for ORACLE; a
traversal test over the call graph **terminates** in the presence of those cycles.

### M5 — the JAR spine: classes, methods, fields, invocations, junction

The M:N increment. 1,633 + 33,407 + 8,649 + 64,446 + 111,455 documents.

**Done when:** counts match; **hashed `_id`s** are exercised (the 1,025 B constructor is in the
corpus, so this is automatic — assert its `_key` round-trips); both junction directions are indexed
and `explain`-verified; `am_java_class` worst document is **≤ 1 KB** (measured 605 B, versus
5,544,022 B embedded — the §17.2 claim, now asserted).

### M6 — the DAG and endpoints

**Done when:** 3,784 `am_dag_node` and 7,281 edges as `childRefs[]`; the multikey reverse index
answers "who calls this"; **a traversal of endpoint 001 terminates** with a visited set (the
2.03 × 10¹⁰ expansion is the reason this is a named criterion and not an assumption); the four
endpoint-only fields (`analyzedAt`, `jarName`, `sourceJar`, `sourcePath`) are present, since they
exist in no other artifact.

### M7 — verification, sweeper, CLI, operability

**Done when:** all three checkers run in CI over the full corpus; the sweeper is shown to delete an
abandoned run **and shown not to delete a run in flight** (convention 1 — the test must fail when
the age threshold is removed); exit codes behave; the health command reports the zero-current-runs
state of §11.3; the scale test (PLAN-06) passes.

### Ordering constraints

M1 before everything. M2 before M3 (statements reference nodes). M5 before M6 (`dag_node.methodRef`
targets methods). M4 and M5 are independent. M7 is last but its checkers should be written
incrementally alongside M1-M6 rather than deferred — a checker written after the fact tends to
codify whatever the mappers happen to do.

### What is explicitly out of scope

A read side implementing `AnalysisRepository` over `am_*`; wiring into `app.storage.repository`;
retiring `MongoPlsqlEntityIndex`; the Claude/cross-ref/chat/job entity families (PLAN-04 E47-E65);
and deleting any JSON (PLAN-09 increment H).

---

## 14. Open questions and risks

### 14.1 Needing a product decision

| # | question | why it cannot be decided here |
|---|---|---|
| 1 | **Does `am_run` store `jdbcUrl` and `connectionName`?** RELATIONAL §18(a) includes them as "sensitive". **PLAN-00 C4** says credentials never enter shared storage and `CredentialGuardStore` wraps every backend. A JDBC URL is not a credential but it names a production host. | Security policy, not a modelling choice. **Recommendation: store `connectionName`, omit `jdbcUrl`** — the name is the join key the UI needs; the URL is reconstructible from config. |
| 2 | **Prefix `am_` or a separate database?** (§2.3, §6.6) | Affects backup and retention policy. **Recommendation: prefix now**, revisit if the model needs its own lifecycle. |
| 3 | **Does the `_id` scheme diverge from PLAN-05's UUIDv7, permanently?** (§6.3) | If a Postgres target is ever real, two id schemes must be reconciled. **Recommendation: yes, diverge, and record it as `[MONGO DIVERGES]`.** |
| 4 | **Is `constantValue` truncation acceptable?** §17.4 reversed to storing it in full via a child document. That is more storage for 52 ANTLR automata nobody reads. | **Recommendation: keep full** — losslessness was an explicit user requirement and 52 documents is nothing. |
| 5 | **Retention: how many superseded runs?** RELATIONAL §5.2 deferred it; PLAN-05 §7.2 says "prune to $keep newest". | Operational. |
| 6 | **`connections.json` — never read** (RELATIONAL §12.2). If it holds credentials it must never be ingested. | Needs someone to look. |

### 14.2 Risks

| risk | severity | mitigation |
|---|---|---|
| **`$jsonSchema` validation cost is unmeasured** on a 400,000-document ingest (§8.7) | medium | measure in M1 on ~700 documents and extrapolate before M3 commits to it |
| **Validator error 121 does not name the failing rule** (§11.4) | medium | client-side re-validation for diagnostics; accepted schema duplication |
| **The S7→S8 window leaves zero current runs** (§11.1) | low | milliseconds wide; §11.3 alerts on it; the alternative was worse |
| **No FK enforcement** (§5.2) | **high, inherent** | the module is the only writer; three-layer defence; §9.1 detection. **Cannot be eliminated on Mongo** |
| **Readers can see `IN_PROGRESS` children** (§5.8) | medium | the `status="COMPLETE"` reader contract — discipline, not enforcement. Worth a lint or a shared query helper once a read side exists |
| **The corpus moves** — PLAN-12 measured 4,089 DAG nodes and 1,623 classes where I measure 3,784 and 1,633 (§6.5) | low | §9.2 baselines are secondary signals, not invariants |
| **Duplicating the model across three documents** (PLAN-04/05, RELATIONAL, this) | **medium and growing** | §6.7's E-number cross-referencing. A fourth vocabulary would be worse than any technical debt here |
| **`analysis.json` DOM parse would need ~500 MB heap** (§11.5) | medium | streaming is mandatory in §4.2; PLAN-12 records this file at 69 GB in one configuration |
| **Schema churn** — the JAR writer is mid-migration (inline *and* pooled forms coexist, RELATIONAL §10.3) | medium | `schemaVersion` + delete-and-re-ingest (§10.2), affordable only while files remain the system of record |

### 14.3 The largest single risk

**That this module is built and nothing reads it.** `MongoPlsqlEntityIndex` is already a working,
tested Mongo entity layer serving three of 65 entities, and it is *"additive … drop all four
collections and the application still answers every question"* (:56). A second, larger, unread
entity layer is a bigger version of the same position.

PLAN-09 anticipated exactly this and made it an ordering rule (PLAN-09:62-67): *"Shipping D before
A produces a migration that is correctly built and delivers nothing anyone can see — the fastest
way to lose sponsorship for the rest."*

**Recommendation: before M3 (the volume increment), commit to a specific first reader** — one screen,
one query, served from `am_*`. The natural candidate is the measured hot path,
`table:<name>` at limit 500 (`access-details.js:338`), because §7.2 already shows it is a single
indexed query and because fixing the `(nodeId, operation, line)` collision (RELATIONAL §5.3) is a
**user-visible correctness win** that the file-based store cannot deliver at all. That gives the
module a reason to exist beyond fidelity.
