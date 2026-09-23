# PLAN-06 — Staying under the 16 MB BSON limit

How every artifact is stored in MongoDB such that **no document can exceed 16 MB**, derived from the
measured sizes in PLAN-01/02/03 rather than from theory.

## The problem, in measured numbers

| artifact | observed | worst case | over the limit by |
|---|---|---|---|
| `jar/<id>/analysis.json` | 3.90 MB (119 classes) | **~1 GB** (1,597 classes, live run) | **62×** |
| `api/tables/index.json` | 152–307 KB | **~500 MB** | 31× |
| `plsql-parse/.../flow.json` | 2.27–3.30 MB | **~300 MB** | 19× |
| `plsql-parse/.../edges.json` | 75–826 KB | **~200 MB** | 12× |
| `bundled-jars.json` | **3.03 MB** (94 jars) | 100s of MB | ~10× |
| `api/nodes/<id>.json` | avg 42–78 KB, max **370 KB** | ~40 MB | 2.5× |
| `_summary_static.json` | **430 KB** | ~35 MB | 2× |

Seven artifact families breach the limit. "One analysis, one document" is not available at any size
of input we care about, so the design question is *where to cut*, not *whether*.

Two facts do most of the work:

1. ~~**~2,050 B/node of embedded decompiled source** is what drove `analysis.json` to 912 MB.~~
   **MEASURED AND WRONG — see PLAN-00 §6.** `sourceCode` is 0.0-4.0% of `analysis.json` (mean 322
   B/node); `methods.invocations` is **33-85%**. Source still belongs in `ArtifactStore` because it
   is opaque bytes rather than queryable data, but moving it removes ~4% at best and ~0.1% net, so
   no decision below should rest on it. **The lever worth costing out is `invocations`**, which
   `AbstractAnalysisDataProvider:302-320` reads on exactly one path — to compute a single
   `hasOutboundRestCall` boolean — and skips everywhere else.
2. **The largest PL/SQL artifacts are duplicates that nothing reads.** `flow.json` re-embeds
   `chunks/` + `edges/` + `tables.json`, and no code in `unified-web` reads it. Same for
   `edges.json`. Not storing them is both correct and the single biggest saving.

## Decision table

Rule applied, in order:

- **DROP** — dead (no writer or no reader) or purely derived and cheaply recomputed.
- **ARTIFACT** — opaque bytes, no queries against its content → `ArtifactStore`, referenced by key.
- **SPLIT** — unbounded collection → one document per natural element.
- **DOC** — bounded, provably < 16 MB → a single document.

### Java JAR/WAR

| artifact | decision | rationale |
|---|---|---|
| `analysis.json` | **SPLIT + ARTIFACT** | the root document holds scalars only; `classes[]` → one doc per class; `endpoints[]` → one doc per endpoint; every `sourceCode` → ARTIFACT keyed `jar/<id>/src/<sha256>` |
| `analysis_corrected.json` | SPLIT + ARTIFACT | same shape, discriminated by `variant` |
| `analysis_corrected_prev.json` | SPLIT + ARTIFACT | same, `variant=prev`; rotation becomes an update of `variant`, not a copy |
| `_header.json` | **DOC** | 191 B measured, fixed field set |
| `_summary_static.json` / `_summary.json` | **DROP (derive)** | a cache of data now indexed; the query replaces it. ~35 MB at scale, and 430 KB on *every* page load today |
| `_class-tree*.json` | **DROP (dead)** | no writer exists; only deleted. PLAN-01 §1 |
| `mongo-catalog.json` | **DROP (dead)** | `fetchCatalog:413` returns null; never written |
| `connections.json` | DOC | 130 B |
| `domain-config.json` | DOC | 7.8 KB |
| `bundled-jars.json` | **SPLIT** | 3.03 MB for 94 jars; one doc per bundled jar. Embedded text resources → ARTIFACT |
| `jar-dep-map.json` | SPLIT | one doc per dependency edge; grows as deps × classes |
| `external-links*.json` | SPLIT | one doc per link |
| `endpoints/*/endpoint.json` | DOC | 9.9 KB max observed, bounded |
| `endpoints/*/call-tree.json` | **DROP (duplicate)** | same tree as `nodes/node_*.json`; PLAN-01 records call trees stored **4×** |
| `endpoints/*/nodes.json` | **DROP (duplicate)** | ditto |
| `endpoints/*/nodes/node_<n>.json` | **DOC, one per node** | avg 2.05 KB, max 6.2 KB — already the right grain |
| `endpoints/*/fragments/*.json` | DOC per fragment | unbounded in count, bounded per item |
| `stored.jar` | **ARTIFACT** | 70 MB binary; never queried |

### PL/SQL (all five dialects)

| artifact | decision | rationale |
|---|---|---|
| `chunks/chunk_NNNN.json` | **DOC, one per node** | already one node per file — the boundary exists; reuse it |
| `edges/edges_NNNN.json` | **DOC, one per edge** | finer than per-chunk; makes graph queries possible |
| `flow.json` | **DROP (duplicate, unread)** | re-embeds chunks+edges+tables; **nothing in unified-web reads it**. ~300 MB avoided |
| `edges.json` | **DROP (duplicate)** | merged view of `edges/`; ~200 MB avoided |
| `tables.json` | SPLIT | one doc per table |
| `api/index.json` | SPLIT | root scalars ~1 KB as DOC; `nodes[]`, `cycles[]`, `transactionStatements[]` split |
| `api/nodes/<safeId>.json` | **DOC per node** | max 370 KB observed — under the limit, but see the guard below |
| `api/tables/index.json` | **SPLIT** | ~500 MB worst case; one doc per table, `operationSummary` as root scalars |
| `api/call_graph.json` | SPLIT | one doc per edge; reuse the `edges/` collection |
| `api/node_id_map.json` | DOC | ~9 KB |
| `resolver/q*.json` | SPLIT | Oracle-only in practice; one doc per resolved object |
| `sources/<file>` | **ARTIFACT** | raw SQL text, not JSON, never queried by content |
| `.parse-cache/*`, `.node-cache/*` | **DROP (derived)** | byte-equivalent to `chunks/*`; recomputable |
| `cache/schema-resolver/*.tsv` | ARTIFACT | derived cache, opaque |
| `progress.json` | DOC | transient run state |
| `plsql/<id>/metadata.json` | DOC | ~250 B |
| `plsql/<id>/analysis_claude.json` | SPLIT | 138 KB now, ~50 MB worst case. **Byte-identical to `claude/_result.json`** (PLAN-03) — store once |

### Append-only (the awkward family)

Six artifacts are appended: chat history, Claude call logs, correction run logs, improvement runs.
Today each is **read-everything-then-rewrite-everything**, because `ArtifactStore` has no append —
O(n²) in entry count, and four have no retention policy at all.

**One document per entry.** Key `(scope_id, seq)` with `seq` monotonic per scope; append is a single
insert, read is a range query with a limit. This removes the O(n²) write cost *and* the 16 MB
ceiling in one move, and it is the case where the document store is strictly better than either
files or objects.

Retention becomes a policy rather than an accident: a TTL index on `created_at`, or a capped count
per scope. PLAN-03 notes `plsql/improvement/run_*.md` accumulates one file per run forever.

## Where the split boundary goes

**Reuse `ChunkedFlowWriter`'s existing boundary: one chunk per `FlowNode`** (`:124-128`). It is
already what the writer produces and what `api/nodes/` mirrors — verified in PLAN-02 as
chunk count == node count == `api/nodes/` count across all five analyses. Inventing a second
boundary would put the database out of step with the reader for no gain.

**But do not inherit `chunk_%04d`.** That format caps the system at **9,999 nodes** — an accidental
limit nobody chose. Documents are keyed by `(analysis_id, run_id, node_id)`, with no ordinal in the
key at all. This matters beyond formatting: PLAN-02 established the crawl is **not deterministic**,
so re-running renumbers nodes. Ordinals are run-scoped and can never be stable identifiers.

For the JAR side the natural grains are **class** and **endpoint**, and below an endpoint, **node** —
`node_<n>.json` already averages 2.05 KB, which is the right size.

## The size guard

A document that grows past 16 MB is a hard write failure in production, and the inputs that cause it
(a 1,597-class JAR, a 5,000-node crawl) are exactly the ones customers bring. So this is enforced,
not hoped for.

**Estimate before insert.** Compute the BSON size of each document before writing and refuse at a
**12 MB soft limit**, leaving 25% headroom for driver overhead and later field additions. A single
place — the repository write path — does this for every collection; no caller can bypass it.

**When an element would breach the limit**, in order of preference:
1. If the bulk is one large field (source, a long SQL body), move that field to `ArtifactStore` and
   store the key. This is the common case and it is the right answer.
2. If the bulk is a nested array, split one level deeper (endpoint → node, table → column).
3. If a single indivisible element still exceeds 12 MB, store it as an ARTIFACT and keep only its
   metadata in the document. Nothing is ever silently truncated.

**Tested, not asserted.** A test generates a synthetic analysis scaled past the largest real input
(2,000 classes, 6,000 nodes — beyond the 1,597 and 5,000 seen) and asserts that every produced
document is under the soft limit. The guard itself gets a unit test that a deliberately oversized
element is diverted to `ArtifactStore` rather than rejected or truncated.

## Documents vs `ArtifactStore` — the boundary

The rule: **if you would never filter, sort or aggregate on it, it is an artifact.**

Into `ArtifactStore`, referenced by key, never inline:

- decompiled `sourceCode` (~2,050 B/node — the single largest contributor to the 912 MB)
- `stored.jar` / `stored.war` (70 MB)
- `sources/*.sql|prc|fnc|pkb` raw text
- Excel/markdown exports and reports
- rolled log archives (already handled by `LogArchiveService`)
- resolver TSV caches

A document references an artifact by storing its `ArtifactKey` string plus `size` and a `sha256`.
Content-addressing the source (`jar/<id>/src/<sha256>`) also **deduplicates identical method bodies
across nodes**, which is common in generated code.

**GridFS is not used.** It exists to store large blobs in MongoDB by chunking them — but we already
have an interface that stores large blobs, `ArtifactStore`, with three working backends. Adding
GridFS would mean two blob mechanisms with different semantics and two places to get retention
wrong. The exception would be a deployment where Mongo is the *only* available storage; if that is
ever a requirement, GridFS becomes the `MongoArtifactStore` implementation detail (PLAN-07) — which
keeps it behind the frozen interface rather than exposed as a second concept.

## Read patterns

The failure mode to design out is **reassembling a 300 MB document to render a summary**, which is
close to what happens today: PLAN-03 measured `_summary_static.json` at 430 KB fetched on *every*
JAR page load, and the log endpoint at 9.36 MB *polled*.

| screen need | today | with documents |
|---|---|---|
| JAR list | read every `_header.json` | one query, projected fields |
| class list | parse whole `analysis.json` | paged query on classes |
| endpoint detail | read `endpoint.json` + `call-tree.json` | endpoint doc + paged node docs |
| call graph | whole `call_graph.json` | edge query by node, expanded on demand |
| table list | whole `api/tables/index.json` (~500 MB worst) | paged query |
| source view | read file | `ArtifactStore.read(key)` — unchanged |

Nothing is reassembled to serve a list. Full reassembly is needed only for export, which is a
streaming job, not a request path.

**A dependency that must be stated plainly:** PLAN-03 found **no endpoint currently accepts `page`,
`offset`, `limit` or a cursor** — all 20+ tables fetch a full array and slice client-side. Server-side
pagination therefore delivers nothing until the UI changes in lockstep. PLAN-08 owns that; without
it, this plan reduces storage cost but not page-load cost.

## Write patterns

- **Bulk insert** for the produce-once collections (classes, endpoints, nodes, edges, tables). Use
  unordered bulk writes in batches so one bad document does not abort the batch.
- **Upsert on re-analysis**, keyed by `(analysis_id, run_id, natural_key)`. Because a re-run is a new
  `run_id`, this is really insert-new-run plus retire-old-run, which is cheaper and gives history for
  free — replacing today's copy-the-folder-with-a-timestamp archive.
- **Append-only** as above: one document per entry, never rewrite.

## Failure and partial writes

A 5,000-node crawl writing 5,000 documents can fail at 3,000. Object storage has the same exposure
today and handles it badly — a half-written analysis looks complete.

**Use a run marker, not a transaction.** Insert a `run` document with `status=IN_PROGRESS` first;
flip it to `COMPLETE` only after the last child document is acknowledged. Every read filters on
`status=COMPLETE`, so a partial run is invisible rather than corrupt. A sweeper deletes
`IN_PROGRESS` runs older than a threshold.

A multi-document transaction is rejected deliberately: it requires a replica set, holds resources for
the whole crawl (minutes), and risks exceeding the 16 MB oplog entry limit on large batches. The run
marker gives the same visible atomicity at a fraction of the cost, and it degrades gracefully on a
standalone server.

## OPEN QUESTIONS

1. **Is `bundled-jars.json` worth keeping at all?** 3.03 MB for 94 jars, and it embeds every bundled
   jar's text resources. *Recommendation:* split per jar and move resources to artifacts; revisit
   whether anything reads the resource bodies.
2. **Should `variant` (static / corrected / prev) be a field or separate collections?** *Recommendation:*
   a field, indexed — the queries are identical and rotation becomes a field update.
3. **Retention for append-only entries.** No policy exists today. *Recommendation:* a TTL index with a
   configurable window, defaulting to keeping everything, so nothing disappears without an explicit
   decision.
4. **Does anything still read `flow.json` outside `unified-web`?** PLAN-02 found no reader there, but
   an external consumer would break. *Recommendation:* keep writing it as an ARTIFACT for one
   release, log every read, then drop it.
