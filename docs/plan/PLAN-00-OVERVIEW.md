# PLAN-00 — Overview, constraints, and chunk map

Adds a third storage mode (`mongodb`) alongside `local` and `s3`, and turns the analysis output
from opaque JSON blobs into queryable, normalised entities the UI can page through — **without
changing a single method the rest of the application calls.**

## The hard constraints

These are not preferences. A design that violates one of them is wrong, however elegant.

### C1 — `ArtifactStore`'s public surface is FROZEN

74 classes outside `analyzer-storage` depend on it. The exposed methods stay exactly as they are:

```
write(key) / write(key, contentLength) / read(key) / read(key, offset, length)
exists / size / lastModified / delete / deletePrefix
list(prefix, recursive) / statPrefix(prefix) / copy / move / materialize
```

Only the *internals* change. A new backend is a new implementation behind this interface plus one
branch in `StorageFactory` — nothing else in the codebase moves. This is the property that made the
S3 backend a single class, and it is worth more than any individual optimisation.

### C2 — 16 MB BSON limit

Measured, not theoretical. A real `analysis.json` reached **912 MB and was still growing**
(PLAN-01); `flow.json` extrapolates to ~300 MB and `api/tables/index.json` to ~500 MB (PLAN-02).
Nothing may assume "one analysis, one document".

### C3 — the UI must not break

Every screen keeps working, at equal or better latency. PLAN-03's access-pattern table is the
acceptance criterion.

### C4 — credentials never enter shared storage

`plsql-config.yaml` and `plsql-env-config.yaml` hold plaintext production passwords.
`CredentialGuardStore` wraps every backend in `StorageFactory.create`; a Mongo backend inherits that
protection automatically and must not bypass it.

### C5 — no data loss on migration

Existing local and S3 data must remain readable throughout, with a rollback that is not "restore
from backup".

## The architectural consequence of C1, stated plainly

`ArtifactStore` is a **blob** interface: opaque bytes addressed by key. That is exactly right for
what it does, and it is why S3 slotted in cleanly.

But "normalised entities with relationships the UI can query" is a **different capability**, not a
different backend. Implementing `ArtifactStore` over MongoDB gives you *blobs stored in MongoDB* —
GridFS handles the 16 MB limit fine — and it delivers C1 perfectly. What it does not give you is
`find all endpoints calling table X`, because a blob store cannot answer that whatever is behind it.

So the plan is **additive, not a modification**:

| layer | role | changes? |
|---|---|---|
| `ArtifactStore` | opaque artifacts: stored JARs, decompiled source, exports, logs | **frozen** (C1) |
| `AnalysisRepository` *(new)* | normalised entities, relationships, paged queries | new interface, additive |

Both live in `analyzer-storage`. `mongodb` mode wires both; `local` and `s3` wire `ArtifactStore`
and a repository implementation that reads the same artifacts as today, so **no caller is forced to
change and no mode loses functionality**. A caller that wants a query uses the repository; a caller
that wants bytes keeps using the store. Nothing that exists today is rewritten.

If instead we forced entity queries through `ArtifactStore`, we would have to widen its interface —
breaking C1 across 74 classes — or fake queries by scanning blobs, which is what makes the UI slow
today. Neither is acceptable, so the second interface is the honest answer.

## What the inventories already tell us

Findings from PLAN-01/02 that shape everything downstream:

1. **Duplication is the dominant cost.** Call trees are stored 4× on the JAR side; ~8-9 MB of every
   13 MB PL/SQL analysis is duplication, and its largest component (`flow.json`) **is never read**.
   Normalising removes more bytes than any compression.
2. **The chunk boundary already exists.** `ChunkedFlowWriter` writes one chunk per node. Reuse that
   boundary; do not invent a second one that disagrees with the reader.
3. **Two artifacts are dead** (`_class-tree.json`, `mongo-catalog.json`) — do not model them.
4. **`%04d` caps the system at 9,999 nodes** — an accidental limit the new design should not inherit.
5. **The crawl is not deterministic**: re-running produces different node numbering. Node ordinals
   are run-scoped and can never be stable keys.
6. ~~**Embedded decompiled source (~2,050 B/node) is what makes `analysis.json` enormous.**~~
   **MEASURED AND WRONG.** Across every real analysis, `sourceCode` is **0.0-4.0%** of
   `analysis.json`; the mean per node is **322 bytes**, not 2,050 (`SourceEnrichmentService:148`
   caps each node at 8,192 chars, so 2,050 was a near-worst-case node read as a mean).
   `jar-analyzer-core`'s analysis contains **zero** source and is still 5.2 MB.
   **The real driver is `methods.invocations`: 33-85% of the document** — 8-21x all source
   combined. Extracting source to blobs nets ~0.1% after key overhead and was rejected
   (`PLAN-09` Increment E-source). Source *should* still live in `ArtifactStore` when it is
   convenient, but it is not the size lever and nothing should be justified by it.

## Chunk map

Each document is independently actionable. Later ones depend on earlier ones only for facts.

| # | file | question it answers | status |
|---|---|---|---|
| 00 | `PLAN-00-OVERVIEW.md` | constraints, architecture, sequencing | this file |
| 01 | `PLAN-01-ARTIFACTS-JAR.md` | every JAR/WAR artifact, field by field | **done** |
| 02 | `PLAN-02-ARTIFACTS-PLSQL.md` | every PL/SQL artifact, all 5 dialects | **done** |
| 03 | `PLAN-03-ARTIFACTS-CLAUDE-CROSSREF-AND-UI.md` | remaining artifacts + UI access patterns | **done** |
| 04 | `PLAN-04-ENTITY-MODEL.md` | the entities, their fields, identity and ownership | **done** |
| 05 | `PLAN-05-RELATIONSHIPS-NORMALIZATION.md` | relational schema, FKs, what stays denormalised and why | **done** |
| 06 | `PLAN-06-MONGO-16MB-CHUNKING.md` | how each unbounded artifact is split; GridFS boundary | **done** |
| 07 | `PLAN-07-STORAGE-MODE-MONGODB.md` | `mongodb` mode behind the frozen interface + the new repository | **done** |
| 08 | `PLAN-08-UI-CONTRACT-AND-QUERIES.md` | required queries, indexes, pagination | in progress |
| 09 | `PLAN-09-MIGRATION-AND-ROLLOUT.md` | backfill, dual-write, cutover, rollback | in progress |
| 10 | `PLAN-10-WORKFLOW.md` | all three skills: `/storage-check`, `/s3-mode`, `/db-mode` | **done** (the file was missing until now — the index claimed done while nothing existed; written from the skills as they are on disk) |

## Sequencing

04 and 05 need 03's access patterns — a normalisation that cannot serve the UI cheaply is worthless.
06 needs 04/05. 07 needs 06. 08 validates 05 against 03. 09 and 10 need 07.

Parallelisable: 04+05 together, then 06+08 together, then 07+09+10.

## How progress is judged

Not by documents written. By these, in order:

1. `/storage-check` stays green — 0 unclassified direct I/O, gate passing.
2. `ArtifactStore`'s method signatures are byte-for-byte unchanged. A diff proving it is part of the
   definition of done.
3. Every UI screen in PLAN-03 works in all three modes.
4. No document exceeds 16 MB, asserted by a test rather than by inspection.
5. The zero-local-writes acceptance test still passes in `s3` and `mongodb` modes.
