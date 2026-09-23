# Coding Standards — `analysis-mongo-ingest` and the layers it touches

Rules, not platitudes. Every convention below is either **extracted from what this repository
already does** (cited) or justified against a specific problem in these plans.

Companions: [`TASK-REGISTRY.md`](TASK-REGISTRY.md), [`TEST-STRATEGY.md`](TEST-STRATEGY.md),
[`SCALE-ARCHITECTURE-PLAN.md`](SCALE-ARCHITECTURE-PLAN.md).

---

## 1. Three kinds of object, never merged

The single most important rule, because merging them is what makes a schema change break the UI.

| kind | package | crosses | mutable | knows about |
|---|---|---|---|---|
| **Document** | `…ingest.mongo.model` | the driver boundary | no | BSON field names, `_id` recipes |
| **Domain entity** | `…ingest.mongo.domain` | nothing — internal | no | business meaning only |
| **DTO** | `…web.dto` (in `unified-web`) | the HTTP boundary | no | JSON field names the UI reads |

**Why they must not be the same class.** A shared class means the BSON field name, the domain
concept and the API contract change together. `analyzer-storage/pom.xml:95-100` already reasons
this way about `AnalysisRepository` trading in `JsonNode`:

> "Increment C's acceptance criterion is that no endpoint's response changes byte for byte, and
> today's responses ARE these JSON documents — mapping them through records and back is exactly
> where a field order or an absent-vs-null distinction changes."

That is the hazard stated precisely. Renaming a Mongo field must not be able to change an HTTP
response; only an explicit mapper edit may do that.

**When each is used**
- **Document** — inside `map/` and `write/` only. Never returned from a public service method.
- **Domain entity** — only where behaviour exists (e.g. `ContentHash`, `RunVersion`). If it has no
  behaviour, do not create it; a document plus a mapper is enough. **Do not build a domain layer
  for symmetry.**
- **DTO** — one per response shape, not one per entity. A list DTO carries only the fields the list
  renders (`SCALE §C.5`).

**Records by default, classes by exception.** All three kinds are `record`s unless they need
inheritance or lazy fields. Immutability is not stylistic here: §B.1 content-addressing means a
document's identity **is** its content, so a mutable document is an object whose `_id` can silently
stop matching its own hash.

**The one deliberate exception:** the ingest hot path builds `org.bson.Document` directly rather
than a record it then converts. At 235M documents per run *(extrapolated)* the intermediate
allocation is real cost. Records are used for everything a human reads; `Document` for the write
path. State this in the class comment so it does not look like an oversight.

---

## 2. SOLID, applied to this code

Not definitions — the specific place each principle changes a decision here.

### S — Single responsibility
**`IdCodec` is the only place an `_id` is constructed** (`MONGO §3.4`). Not "a utility mappers may
use" — mappers receive ids and have no string concatenation of their own.

The reason is concrete: §3.3's escaping rule must hold for the prefix-range delete in §5.3 to be
correct. An `_id` built in 30 mappers is an escaping bug in 30 places, and the failure mode is a
`deleteMany` that removes the wrong run.

### O — Open/closed
**`ArtifactMapper` is the extension point.** A new artifact is a new class with its own `artifact()`
constant, never a branch in an existing mapper. This is already the house rule —
`MongoPlsqlEntityIndex.java:78-82` states it for the JAR spine — and PLAN-11 explains why:

> "**Never map by field name across artifacts. Map by (artifact, path).** Two of the six hazards are
> name collisions where the same identifier means a count in one document and a collection in
> another."

A `switch` on artifact name inside one mapper is exactly the name-keyed coupling that rule forbids.

### L — Liskov
**Every `ArtifactStore` implementation must be substitutable**, which is what
`ArtifactStoreConformanceTest` enforces — and PLAN-09:834-838 protects the suite itself:

> "If it needs to relax a case, that case is a real behavioural difference and a finding to report,
> not a test to edit."

Applied here: `DocSink` has exactly two implementations (`BulkWriter`, and a list collector for
tests) and they must be indistinguishable to a mapper. If a mapper ever needs to know which one it
has, the seam is wrong.

### I — Interface segregation
**`DocSink` is one method.** A mapper needs "emit a document"; it does not need connections,
transactions or flush control. This is what lets the entire unit tier run with no Mongo
(`TEST-STRATEGY.md` §2.1) — the payoff is measured in test runtime, not in tidiness.

Counter-example to avoid: do **not** give mappers the `MongoDatabase`. The moment one does its own
query, the unit tier dies.

### D — Dependency inversion
**Ingest depends on `ArtifactStore`, not on the filesystem.** This is enforced by a build gate, not
by intention: `DirectFileIoGateTest` fails any class outside the allowlist that touches `Files.*`,
and budgets *"may only ever go down"* (`:30-32`). `analyzer-storage` is the one exempt module
(`:137`).

Consequence for this module: **zero new allowlist entries** (`INF-002`). If a reviewer sees one
being added, the inversion was abandoned somewhere.

---

## 3. Patterns that are warranted, and where

| pattern | where | why here specifically |
|---|---|---|
| **Repository** | `AnalysisRepository` over `am_*` | the seam already exists (`com.analyzer.storage.repo.AnalysisQuery`); it is what lets `MID-*` swap file scans for indexed queries one endpoint at a time |
| **Mapper** | `ArtifactMapper` per artifact | PLAN-11's (artifact, path) rule; also the unit-test seam |
| **Strategy** | per-dialect behaviour | five dialects with measured divergence — call-edge density 5.9-43.4/node, and only SQLSERVER produces `{"schema":null,"name":null}` rows. A conditional chain over `dialect` would grow with each new dialect |
| **Builder** | `CollectionSpec` / `IndexSpec` | many optional fields (partial filter, unique, collation); a 7-arg constructor is unreadable and mis-ordering two booleans is silent |
| **Factory** | `StorageFactory` (existing) | already the single branch point for backends; extend, do not add a second |
| **Template method** | ingest stage sequence (§8.1) | the stage order is fixed; only the mapping varies |

### Patterns to **not** use, and why

| pattern | why not |
|---|---|
| **Generic `AbstractBaseMapper<T>` hierarchy** | 30 artifacts with genuinely different shapes; the shared superclass becomes a bag of `protected` helpers and every subclass overrides half of it. Prefer composition and small helpers |
| **Visitor over document types** | adding a collection would touch every visitor. Collections are added often here (47 and counting) |
| **Unit of Work / identity map** | there is nothing to track: writes are idempotent upserts by deterministic `_id` and there are no transactions (standalone mongod, `MONGO §5.8`) |
| **Spring Data repositories** | imposes its own `_id` and mapping conventions, which fight the composite content-addressed `_id`. `MONGO §1.4` rejects it with the reasoning |
| **Active Record** (documents that save themselves) | couples the model to the connection and kills the no-Mongo unit tier |
| **A DI container inside the module** | it is a library and a CLI; constructor wiring in `IngestMain` is ~20 lines and starts in milliseconds |
| **Event bus / observers for progress** | a `WriteStats` counter object passed down is simpler and testable; an event bus makes ordering non-obvious in a pipeline whose ordering is the point |
| **Premature `interface` on every class** | one implementation plus one test double does not need an interface unless the seam is load-bearing (`ArtifactMapper`, `DocSink`, `ArtifactStore` qualify; `BulkWriter` does not) |

**The general rule:** a pattern is justified when it removes a conditional that would otherwise grow
with the data, or creates a seam a test needs. Otherwise it is indirection.

---

## 4. House conventions to follow (extracted, not invented)

These are the repository's existing rules. Following them matters more than any preference below.

**4.1 Comments explain *why*, not *what*.** The codebase is unusually good at this and the standard
is set by examples like `analyzer-storage/pom.xml:77-84` (why the driver version is pinned) and
`MongoAnalysisRepository.java:536`:
> *"The age threshold is not a nicety; it is the only thing protecting an ingest in flight."*

A comment restating the code is noise; a comment recording a measurement or a rejected alternative
is the most valuable thing in the file.

**4.2 State what the evidence does not cover.** `parser/js/leftPanel.js:29-31`:
> *"the largest analysis has 114 procedures and 85 tables… it is not evidence about the
> thousand-procedure case, which no analysis here exercises."*

Do this. Every performance claim in this module names the corpus it was measured on.

**4.3 A gate is calibrated by deliberately breaking it.** PLAN-10 on `no-local-writes.sh`:
*"The harness is self-tested: it was checked to fail when a file is planted, because a verifier that
cannot fail proves nothing."* Every gate in `TASK-REGISTRY.md` (DB-014, MID-022, UI-027, VER-006)
ships with the planted-failure test.

**4.4 Budgets ratchet down only.** `DirectFileIoGateTest:30-32`; PLAN-09 §7.1: *"Never raise a
budget to go green."*

**4.5 All I/O through `ArtifactStore`.** §2 D above.

**4.6 Never hard-validate a vocabulary that comes from analysed source.** PLAN-04 §1.5 Trap 2:
*"CHECK constraints only where the value set is closed by construction, never on a parsed
vocabulary"* — measured, `statementSummary` keys vary 27/28/12/11/11 across dialects. Applied:
`$jsonSchema` enums cover only values **this module mints** (`MONGO §5.6`).

**4.7 A skipped test is honest; a fake-backed green test is not.** `MongoTestSupport.java:15-31`.

**4.8 Count files, never `max(ordinal)`.** PLAN-09:801-873 — ordinal gaps exist, and PLAN-04 §1.3
forbids keying on ordinals at all.

---

## 5. Error handling

**5.1 Fail fast at the boundary, with the input named.** `MongoConnection.java:86` pings at open and
throws naming `app.storage.mongodb.uri`, because *"an unreachable server lets the context start and
fails on the first artifact write instead."*

**5.2 Every ingest exception names the source artifact and the JSON pointer.** A mapper failure that
says only "unexpected token" against 122,641 files is unusable. `EntityShapeException`
(`analyzer-storage/.../entity/EntityShapeException.java`) is the existing precedent — extend it.

**5.3 Unknown shapes fail loudly; unknown *values* pass.** `PlsqlEntityMapper.java:156` fails on an
unobserved shape. The distinction is 4.6: a new **field layout** is a bug; a new **enum value** is a
sixth dialect.

**5.4 Never catch and continue in the write path.** A failed batch aborts the run, which is safe
because the marker is `IN_PROGRESS` and the run is invisible (`MONGO §5.8`). Partial success is the
one outcome that must not exist.

**5.5 Validator error 121 gets client-side re-validation for diagnostics** (`MONGO §11.4`) — the
server error does not say which rule failed.

---

## 6. Null policy

**6.1 No `null` returns from public methods.** `Optional<T>` for absence; empty collections for
empty. Internally, `null` is permitted in document building where absent means absent.

**6.2 Absent and null are the same fact in storage, and different in the source.** Rule 1
(`RELATIONAL §17.1`) omits null/false/empty on write, saving a measured 24-26%. But `VER-005`
proves the omitted set is **exactly** the null/false/empty set — omission must be provable, not
assumed.

**6.3 A nullable ref is a documented state with a measured count.** `dag_node.methodRef` is absent
for 2,080 of 2,285 nodes; that is in the allowlist (`VER-002`) with the number. An undocumented
nullable ref is a bug.

**6.4 `@Nullable`/`@NonNull` on every public parameter and return.** Records make the default
non-null; annotate the exceptions.

---

## 7. Logging

| level | use | example |
|---|---|---|
| `ERROR` | the run failed | validator rejection with the artifact path |
| `WARN` | degraded but continuing | an unresolvable ref added to the report |
| `INFO` | run lifecycle, one line per stage | `"Ingested am_statement: 15,824 docs in 4.2s"` |
| `DEBUG` | per-artifact | `"Mapping api/nodes/X.json → 42 docs"` |
| `TRACE` | never per document | — |

**Rules.** No logging inside a per-document loop — at 235M documents a single `INFO` is a denial of
service against the log. Progress is a periodic `WriteStats` summary (`MONGO §8.6`), not a line per
row. Never log document content: `RELATIONAL §10.3` flags `jdbcUrl` as sensitive and PLAN-00 C4
forbids credentials in shared storage.

---

## 8. Naming

| thing | convention | example |
|---|---|---|
| collection constant | `Collections.PROC_NODE` → `"am_proc_node"` | prefix `am_` (`MONGO §2.3` — `analysis_run` is already taken) |
| document class | `<Entity>Doc` | `ProcNodeDoc` |
| DTO | `<Entity><View>Dto` | `StatementRowDto`, `StatementDetailDto` |
| mapper | `<Artifact>Mapper` | `StatementsMapper` |
| ref field | `<target>Ref` | `nodeRef`, `objectRef`, `parentRef` |
| business key | `entityKey` | run-independent (`SCALE §B.1`) |
| test | `<Unit>Test`; gates end `GateTest` | `EscapingPropertyTest`, `NoArraysGateTest` |
| index | `<coll>_<fields>[_partial]` | `am_run_one_current` |

**Ref fields are always `<target>Ref` and always hold an `_id` string.** Not `nodeId`, which in this
data already means the *source's* identifier (`ANALYZER$ERP_USP_RUN_CYCLE`) rather than a document
key. Conflating the two is the mistake `SCALE §B.1` exists to prevent.

---

## 9. Concurrency

**9.1 Ingest is parallel by construction and needs no locks** — every `_id` is computed, so writers
never coordinate (`MONGO §7.7`).

**9.2 Shared mutable state is forbidden in the mapping layer.** `IngestContext`'s lookup maps are
built in a single-threaded phase and **published immutable**. This is also the suspected shape of
a non-determinism bug that was reported as `ANA-001` — join attribution appearing to vary run to run — had the
signature of a shared mutable accumulator or `HashMap` iteration order.

**9.3 Determinism is a testable property, not a hope.** `DeterminismTest` (re-filed as `DB-901`) runs the same
input twice and asserts byte-equality. Without it, content-addressed dedup — worth ~90% of storage
— silently degrades.

**9.4 One ingest per analysis, enforced by the partial unique index**, not by a lock
(`MONGO §5.4`). The index is the semaphore and it works across processes.

> **Correction to §9.2–9.3.** The non-determinism these sections cite as motivation was **not
> real**. The `51 / 11 / 51` reading came from four runs, not three, and the outlier is the oldest
> — a different build, before the join-attribution fix. Across the three runs from one build, 0 of
> 18 nodes differ.
>
> The *guidance* stands: determinism is worth asserting with a test, because content-addressed
> dedup depends on it and nothing else would catch a regression. But it is being asserted to
> protect a property we currently **have**, not to fix a defect we currently suffer — and a
> standards document that cites a phantom defect teaches people to distrust the rest of it.
