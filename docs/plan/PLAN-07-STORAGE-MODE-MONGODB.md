# PLAN-07 — `app.storage.mode=mongodb` behind the frozen interface, plus the additive repository

How the third storage mode is built: one new `ArtifactStore` implementation over MongoDB
(GridFS-backed above a threshold), and one **new, additive** `AnalysisRepository` interface that
gives the UI the entity queries a blob store can never answer.

Depends on: PLAN-00 (C1–C5), PLAN-01 §7 and §3, PLAN-02 §0, §2, §10, §11,
**PLAN-03 §1.5, §4.1–4.3** (the access patterns), **PLAN-06** (the 16 MB chunking decisions and the
GridFS ruling — reconciled explicitly in §1.11).

---

## 0. The diff surface, up front

### What changes

| File | Change |
|---|---|
| `analyzer-storage/.../StorageMode.java` | **one** enum constant + **one** `parse` case |
| `analyzer-storage/.../StorageFactory.java` | **one** component on `Settings`, **one** static factory `Settings.mongodb(...)`, **one** `validate()` case, **one** `createBackend()` case, **one** `settingsFor()` case, **one** private `buildMongoClient(...)` |
| `analyzer-storage/.../StorageConfig.java` | six new `@Value` params + one `switch` arm |
| `analyzer-storage/.../mongo/MongoArtifactStore.java` | **new** — the whole backend, one class (~500 lines), mirroring `s3/S3ArtifactStore` |
| `analyzer-storage/.../mongo/GridFsRolloverOutputStream.java` | **new** — mirrors `s3/MultipartUploadOutputStream` |
| `analyzer-storage/.../AnalysisRepository.java` + `repo/*` records | **new**, additive interface + value types |
| `analyzer-storage/.../repo/ArtifactScanAnalysisRepository.java` | **new** — the `local`/`s3` implementation, reads today's artifacts |
| `analyzer-storage/.../mongo/MongoAnalysisRepository.java` | **new** — the `mongodb` implementation |
| `analyzer-storage/pom.xml` | `org.mongodb:mongodb-driver-sync` (already at **4.11.2** in `jar-analyzer-core/pom.xml:48-51`, and present in `~/.m2`) |
| `unified-web/.../application.properties:150-157` | commented `app.storage.mongodb.*` block |
| new tests | `MongoArtifactStoreConformanceTest`, `FakeMongoServer`, repository conformance suite |

### What does NOT change

- **`ArtifactStore.java` — not one byte.** No new method, no widened signature, no new default.
  This is C1 and it is the acceptance criterion (§7).
- `ArtifactKey`, `ArtifactEntry`, `LocalHandle`, `CredentialGuardStore`, `LocalArtifactStore`,
  `InMemoryArtifactStore`, `S3ArtifactStore` — untouched.
- `ArtifactStoreConformanceTest.java` — untouched. The Mongo backend subclasses it and overrides
  only `newStore(Path)`, exactly as `S3ArtifactStoreConformanceTest` does.
- **All 74 downstream classes** — zero edits. They keep calling `store.write(key)`.
- The three external `StorageFactory` call sites
  (`FlowAnalysisMain.java:163`, `PocRunner.java:225`, `CredentialGuardStoreTest.java:26`) all go
  through `settingsFor(...)` / `Settings.local(...)`; none constructs the `Settings` record
  positionally, so adding a component to it is source-compatible. Verified by grep.
- `CredentialGuardStore` still wraps every backend in `StorageFactory.create` — the Mongo backend
  inherits C4 protection for free and must not be constructed anywhere else.

### The one honest caveat

`StorageFactory.Settings` gains a component. It is not part of the frozen surface (C1 names
`ArtifactStore` and only `ArtifactStore`), and no caller breaks — but say so out loud rather than
claim "nothing changed".

---

## 1. `MongoArtifactStore` — the frozen interface over MongoDB

### 1.1 Storage layout

Two namespaces per configured database:

```
<db>.<artifacts-collection>        default: analyzer.artifacts     — the manifest, one doc per artifact
<db>.<gridfs-bucket>.files         default: analyzer.artifacts_fs.files
<db>.<gridfs-bucket>.chunks        default: analyzer.artifacts_fs.chunks
```

The manifest document:

```javascript
{
  _id:      "data/jar/demo/analysis.json",   // key.s3Key(prefix) — reused verbatim, see 1.2
  parent:   "data/jar/demo",                 // for non-recursive listing
  leaf:     "analysis.json",
  len:      912345678,                       // NumberLong
  mtime:    ISODate("2026-09-01T12:00:00Z"),
  inline:   BinData(0, "...")   // present iff len <= inlineThreshold
  gridfsId: ObjectId("...")     // present iff len >  inlineThreshold
}
```

Exactly one of `inline` / `gridfsId` is present. **The manifest document is the only thing that
makes an artifact visible.** GridFS chunks written but not yet referenced by a manifest are
invisible and are garbage. That single rule is what buys atomic-on-close (§1.4).

Why hybrid rather than GridFS for everything: PLAN-01 §1 shows `_header.json` at **191 B**,
`connections.json` at **130 B**, and 560 `nodes/node_<n>.json` files averaging **2,050 B**. Putting
those in GridFS costs two documents and two round trips each. Threshold default
**1 MiB** — comfortably under the 16 MB BSON limit even with BSON overhead, and above every
artifact PLAN-01 §3 classifies as "safe as one document".

### 1.2 Key mapping — reuse `ArtifactKey.s3Key(prefix)`

No new mapping method on `ArtifactKey` (that would be a change to the frozen package's public
surface for no gain). `ArtifactKey.s3Key(String rootPrefix)` already produces exactly the
`'/'`-joined, prefix-rooted string we want, and `ArtifactKey`'s validator already guarantees no
segment contains `/`, `\`, NUL, `.` or `..` — so the string is unambiguous and reversible.

```java
String docId(ArtifactKey key)      { return key.s3Key(prefix); }          // "data/jar/x/a.json"
String listPrefix(ArtifactKey key) { return docId(key) + "/"; }           // "data/jar/x/"
```

Prefix scans are a **pure `_id` index range**, no regex:

```java
// '/' is 0x2F; '0' is 0x30 — the immediate successor. So [p+"/", p+"0") is exactly the subtree.
Bson underPrefix(String p) { return and(gte("_id", p + "/"), lt("_id", p + "0")); }
```

This uses the free unique `_id` index as a bounded range scan. An anchored `$regex` would also be
index-eligible but is slower and easy to get wrong with metacharacters in a key segment (which
`ArtifactKey` permits — `$`, `[`, `(` are all legal segment characters). Range bounds have no
escaping problem at all.

> **OPEN QUESTION — long keys.** MongoDB's index key limit is 1024 bytes. PL/SQL identities nest
> five segments (`plsql-parse/<DB>/<PROJECT>/<ENV>/<OBJECT>/api/nodes/<safeFileName(nodeId)>.json`,
> PLAN-02 §0) and `safeFileName` can be long. A key over ~1000 bytes makes the insert **fail**,
> where `LocalArtifactStore` and S3 both accept it.
> Options: (a) reject at construction — changes `ArtifactKey`, violates the spirit of C1;
> (b) `_id` = SHA-256 hex of the key when the key exceeds 900 bytes, with the full key in a `key`
> field — but then the range scan breaks for those keys; (c) always `_id` = SHA-256 hex, plus an
> indexed `parent` + `leaf` and an indexed `key` for prefix ranges.
> **Recommendation: (c).** It costs one extra index but removes the cliff entirely, and `parent`
> is wanted for non-recursive listing anyway. Keep the human-readable key in the document for
> operability. Measure the longest key produced by a real run before finalising.

### 1.3 Method-by-method mapping

| Method | Mongo mapping | Round trips | Awkward? |
|---|---|---|---|
| `write(key)` | `GridFsRolloverOutputStream` (§1.4) | 1 (small) / N+1 (large) | atomicity is the whole design, see §1.4 |
| `write(key, len)` | same, with the hint | 1 if `0 <= len <= threshold` | hint is advisory; a wrong hint just rolls over |
| `read(key)` | `find(_id).projection(include inline, gridfsId, len)`; inline → `ByteArrayInputStream`, else `gridFsBucket.openDownloadStream(gridfsId)` | 1 (inline) / 2 (GridFS) | fine |
| `read(key, off, len)` | inline → slice; GridFS → `openDownloadStream` + `skip(off)` + bound | 1 / 2 | **awkward**, see §1.5 |
| `exists(key)` | `find(_id).projection({_id:1}).first() != null` | 1 | covered by `_id` index, ~0.3 ms |
| `size(key)` | `find(_id).projection({len:1})` → `len`, else `-1` | 1 | fine |
| `lastModified(key)` | same projection → `mtime.getTime()`, else `-1` | 1 | ms resolution matches |
| `delete(key)` | `findOneAndDelete(_id)`; if it returned a `gridfsId`, `gridFsBucket.delete(id)` | 1–2 | orphan window, see §5.1 |
| `deletePrefix(prefix)` | `find(range).projection({_id:1,gridfsId:1})` → collect ids → `deleteMany(range)` → GridFS deletes in batches | 2 + ⌈n/batch⌉ | not atomic; count from `deletedCount` |
| `list(prefix, true)` | `find(range).projection(EXCLUDE inline).sort(_id)` streamed lazily off the cursor | 1 + getMores | fine |
| `list(prefix, false)` | files: `find(and(range, eq(parent, p)))`; dirs: covered aggregation `$group` on the first extra segment | 2 | **awkward**, see §1.6 |
| `statPrefix(prefix)` | ONE `find(range).projection({_id:1,len:1,mtime:1})` drained into a `LinkedHashMap` | 1 + getMores | the reason the method exists; see §1.7 |
| `copy(from, to)` | inline: server-side `$merge` aggregation; GridFS: chunk-copy aggregation or client stream | 1–2 / N | **awkward**, see §1.8 |
| `move(from, to)` | manifest-only re-`_id`: insert new doc pointing at the **same** `gridfsId`, then delete old | 2 | genuinely *better* than S3 — zero byte movement |
| `materialize(key)` | `Files.createTempFile` + `Files.copy(read(key), temp)`; `temporary() == true` | as `read` | identical to `S3ArtifactStore:380-405` |

Every `IOException`-declaring method wraps `MongoException` in `IOException`; `exists`/`size`/
`lastModified`/`delete` do not declare it, so they must swallow-and-log exactly as
`LocalArtifactStore:114-137` does and return `-1`/`false`. Silently returning `-1` because Mongo is
*down* rather than because the artifact is *absent* is a real hazard — log at WARN with the key,
and see §5.2.

#### The projection rule (non-obvious, load-bearing)

`list`, `statPrefix`, `exists`, `size`, `lastModified` **must** project `inline` away. Without it,
a `statPrefix` over `jar/<id>/endpoints/` drags every inline payload across the wire — for the
observed 812-file / 5.4 MB endpoint tree that is 5.4 MB per stat call, and for a 1 MiB threshold
the worst case is gigabytes. `S3ArtifactStore` cannot make this mistake because S3 metadata and
body are separate operations; Mongo puts them in one document, so the discipline has to be
explicit. Assert it in a test (§6.2).

### 1.4 Atomic-on-close — the hardest requirement, and how it is met

The contract (`ArtifactStore:22-27`) and three conformance tests
(`abandonedWriteLeavesPreviousContentIntact`, `abandonedWriteOfNewKeyLeavesNothingVisible`,
`closeIsIdempotent`) require: `close()` is the commit point; abandoning the stream leaves the
previous object intact; a never-committed write is invisible.

`GridFsRolloverOutputStream` mirrors `MultipartUploadOutputStream`'s rollover shape:

1. Buffer into a heap `ByteArrayOutputStream` while total ≤ `inlineThreshold`.
2. **On the first byte past the threshold**, roll over: open
   `gridFsBucket.openUploadStream(<key>, options)`, flush the buffered bytes into it, drop the
   heap buffer, and stream everything after that straight through. The GridFS file gets a fresh
   `ObjectId` and *nothing points at it yet*.
3. `close()`:
   - inline path — one `replaceOne({_id}, {…inline…}, upsert(true))`;
   - GridFS path — close the upload stream (chunks now durable but unreferenced), then one
     `findOneAndReplace({_id}, {…gridfsId…}, upsert(true), returnDocument=BEFORE)`; if the returned
     previous document carried a different `gridfsId`, delete that old GridFS file.
   Either way **one single-document write flips visibility**, and single-document writes in
   MongoDB are atomic. No transaction needed.
4. Abandoning the stream: nothing was ever replaced, so the previous manifest still stands and
   still points at the previous payload. A rolled-over-but-abandoned GridFS file is an orphan —
   see §5.1.
5. `close()` is idempotent via a `closed` flag, same as `LocalArtifactStore:62,73-74`.

Consequence worth stating plainly: **atomicity here is per-key only.** So is
`LocalArtifactStore`'s (`Files.move` with `ATOMIC_MOVE`) and S3's. `deletePrefix`, `copy` and a
multi-artifact analysis run are *not* atomic in any of the three backends. Mongo could offer
multi-document transactions and deliberately does not (§5.4).

### 1.5 Ranged reads — where Mongo is weakest

`LocalArtifactStore:93-105` does `skipNBytes(offset)` then bounds; S3 issues an HTTP `Range`.
Mongo offers neither a server-side byte range on a BinData field nor a range on a GridFS download.

- **Inline** (≤ 1 MiB): the whole payload comes back, then is sliced client-side. Correct, and
  bounded by the threshold — but it is *not* a bandwidth saving, which is what a ranged read is
  usually for. Acceptable at 1 MiB.
- **GridFS**: `GridFSDownloadStream.skip(long)` is chunk-aware — the driver recomputes the chunk
  index and re-issues the cursor from there, so an offset deep into a 900 MB file does **not**
  transfer the preceding bytes. It does over-read up to one chunk (255 KB default) at the front.
  Then wrap in the same bounded stream shape `LocalArtifactStore` uses.

Contract corners that must be reproduced by hand:

| Conformance test | Requirement | Mongo |
|---|---|---|
| `negativeOffsetIsRejected` | `IllegalArgumentException` | explicit `if (offset < 0) throw` — copy `S3ArtifactStore:179` |
| `rangeLongerThanObjectIsClamped` | clamp to EOF | natural for both paths |
| `rangedReadToEndOfObject` (`length == -1`) | read to end | natural |
| `length == 0` | immediately-empty stream that still closes the underlying resource | needs the `EmptyOnTop` wrapper from `S3ArtifactStore:408-415` |
| offset past EOF | `LocalArtifactStore` throws `EOFException` (`skipNBytes`), S3 translates 416 to `EOFException` (`S3ArtifactStore:163-170`) | GridFS `skip` past the end just returns fewer bytes — **must** compare `offset` to `len` from the manifest and throw `EOFException` explicitly. Not currently asserted by the conformance suite; add it there so all three backends are pinned. |

### 1.6 Prefix listing with a delimiter — the second-weakest spot

Recursive listing is easy: the `[p+"/", p+"0")` range, sorted by `_id`, streamed lazily off the
cursor (`MongoCursor` → `Spliterator` → `Stream`, closed by the caller as
`S3ArtifactStore.list` does). Genuinely lazy, one getMore per batch.

Non-recursive is where S3's `delimiter="/"` has no Mongo equivalent. Two queries:

```java
// (a) immediate file children — indexed equality on `parent`
find(eq("parent", listPrefixWithoutTrailingSlash)).projection(exclude("inline"))

// (b) immediate pseudo-directories — covered aggregation, never touches a document
aggregate([
  { $match:   { _id: { $gte: p + "/", $lt: p + "0" } } },
  { $project: { d: { $arrayElemAt: [ { $split: [ { $substrBytes: ["$_id", plen, -1] }, "/" ] }, 0 ] } } },
  { $match:   { d: { $ne: null } } },
  { $group:   { _id: "$d" } }
])
```

Honest cost: `$group` scans **every key under the prefix**, not just the immediate children. It is
an index-covered scan (only `_id` is read), so it is cheap per key, but it is O(subtree) where S3's
delimiter is O(children). For `jar/<id>/endpoints/` (812 keys) that is nothing; for a
`plsql-parse/` root with millions it is not.

> **OPEN QUESTION.** (a) accept the covered scan; (b) materialise explicit directory-marker
> documents on write, giving O(children) at the cost of write amplification and a consistency
> problem on delete; (c) cap it — refuse a non-recursive listing above N keys.
> **Recommendation: (a) for v1**, with a WARN log above a configurable
> `app.storage.mongodb.list-scan-warn-threshold` (default 100_000) so we find out from production
> whether it is real. Non-recursive listing is used by the UI for shallow directories only.

Directory entries are reported as `new ArtifactEntry(key, 0, -1, true)`, identical to
`S3ArtifactStore:310`, so `listNonRecursiveReturnsImmediateChildren` (which expects `["a.json",
"sub"]`) passes unchanged.

### 1.7 `statPrefix` — one round trip, as designed

`ArtifactStore:90-105` exists because a loop of per-key HEADs collapses on object storage. The same
argument applies to Mongo, with a smaller constant (~0.3 ms per `find` on a warm local replica set
vs ~25 ms for an S3 HEAD, but still 300× a local `Files.exists`). So `statPrefix` is a single
range `find` with `{_id:1, len:1, mtime:1}` projection, drained into a `LinkedHashMap` — the exact
shape of `S3ArtifactStore:339-353`. Do **not** inherit the interface default, which calls `list`
and would return `inline` payloads unless the projection is right.

### 1.8 `copy` — no server-side copy exists

MongoDB has no `CopyObject`. Three cases:

- **inline source**: server-side, bytes never reach this process —
  `aggregate([{$match:{_id:from}}, {$set:{_id:to, parent:…, leaf:…, mtime:$$NOW}},
  {$merge:{into:"artifacts", whenMatched:"replace"}}])`. One round trip.
- **GridFS source**: the same trick on `<bucket>.chunks` (`$match` `files_id`, `$set` a new
  `files_id`, `$merge` back) plus a copied `.files` document, then a new manifest. Server-side, but
  it **doubles storage** and is not atomic — a crash mid-copy leaves partial chunks under an
  unreferenced `files_id`, which the GC (§5.1) removes.
- **fallback**: stream `read(from)` into `write(to)` through this process. Correct, always works,
  costs 2× the bytes over the wire.

> **OPEN QUESTION.** Implement the GridFS server-side chunk copy, or ship the streaming fallback?
> `S3ArtifactStore:363-366` chose to *fail loudly* above 5 GiB rather than implement multipart copy,
> on the grounds that `copy` is only used for small sidecars today.
> **Recommendation: ship the inline `$merge` fast path and the streaming fallback for GridFS.**
> Add the chunk-copy aggregation only if profiling shows `copy` on a large artifact in a hot path.
> `move` is the one that matters (`PersistenceService.rotateCorrected:454-460` moves a
> potentially ~1 GB `analysis_corrected.json`), and `move` needs no byte copy at all.

### 1.9 Objects over 16 MB

C2 is satisfied structurally, not by a size check: any artifact above the inline threshold lives in
GridFS, which shards it into 255 KB chunk documents. The 16 MB BSON limit therefore applies only to
the manifest document, which is bounded at `inlineThreshold + ~200 B` by construction.

Explicit guards, asserted by test (PLAN-00 "how progress is judged" #4):

- `inlineThreshold` is validated at startup to be `> 0` and `<= 8 MiB`. Refuse to boot otherwise.
- The rollover stream asserts it never buffers more than `inlineThreshold` bytes.
- A test writes an artifact at `threshold - 1`, `threshold`, and `threshold + 1` bytes and asserts
  the storage path (inline / inline / GridFS) and byte-for-byte round trip.

PLAN-01 §3's "never one document" list (`analysis.json` up to ~1 GB, `stored.jar` up to 70.5 MB,
`bundled-jars.json` 3 MB, per-endpoint `call-tree.json`/`nodes.json`) all land in GridFS
automatically, with no caller knowing.

### 1.10 Where Mongo cannot match `LocalArtifactStore` exactly

Stated so a reviewer does not have to find them:

1. **Ranged read on an inline artifact transfers the whole payload** (≤ 1 MiB). Semantically
   identical, not performance-identical.
2. **Non-recursive listing is O(subtree), not O(children)** (§1.6).
3. **Key length cliff at the index key limit** unless the hashed-`_id` option is taken (§1.2).
4. **`copy` costs bytes** where local uses `Files.copy` and S3 uses `CopyObject` (§1.8).
5. **Orphaned GridFS chunks** after an abandoned large write or a crash between manifest delete and
   chunk delete. Local has no equivalent; the temp file is removed by `Files.deleteIfExists`
   (`LocalArtifactStore:80`). Requires a GC (§5.1).
6. **`exists`/`size`/`lastModified` cannot distinguish "absent" from "Mongo unreachable"** and both
   return `-1`/`false`. Local can only fail this way if the disk is gone; S3 has the same problem
   and lives with it (`S3ArtifactStore:187-199` treats 404 as an answer). Mitigated by a WARN log
   and a health indicator (§5.2), not by the interface.
7. **No `lastModified` on a directory** — `-1`, same as S3.

None of these is asserted by `ArtifactStoreConformanceTest`, and none needs to be. Point 6 is the
only one that is a genuine behavioural difference visible to a caller, and it is shared with S3.

### 1.11 Reconciling with PLAN-06's "GridFS is not used"

PLAN-06 states, correctly: *"GridFS is not used. It exists to store large blobs in MongoDB by
chunking them — but we already have an interface that stores large blobs, `ArtifactStore`… Adding
GridFS would mean two blob mechanisms with different semantics."* It then names the exact
exception this document occupies: *"if that is ever a requirement, GridFS becomes the
`MongoArtifactStore` implementation detail (PLAN-07) — which keeps it behind the frozen interface
rather than exposed as a second concept."*

The two documents agree, and the boundary is worth stating precisely because it is easy to misread:

| layer | uses GridFS? |
|---|---|
| **Entity documents** (PLAN-06's SPLIT/DOC decisions, `MongoAnalysisRepository`) | **No — never.** A document that needs bytes stores an `ArtifactKey` string + `size` + `sha256`, exactly as PLAN-06 §"Documents vs ArtifactStore" specifies. No repository code ever touches a `GridFSBucket`. |
| **`MongoArtifactStore`** (this document) | **Yes — internally, invisibly.** It is *the* implementation of `ArtifactStore` for a Mongo-only deployment. GridFS is how one backend stores bytes, the same way multipart upload is how the S3 backend stores bytes. |

So there is still exactly **one** blob mechanism in the design — `ArtifactStore` — and exactly one
concept for a caller to learn. `MongoArtifactStore` is only reachable when
`app.storage.mode=mongodb`, i.e. precisely PLAN-06's "Mongo is the only available storage" case.
Under the recommended production shape of §5.5 (`mode=s3` + `repository=mongodb`) **GridFS is not
used at all** — S3 holds the blobs and Mongo holds only entity documents, which is PLAN-06's
preferred world unchanged.

Three further PLAN-06 rulings that constrain this document and are honoured:

- **The 12 MB soft limit** on entity documents belongs to the repository write path, not to the
  store. `MongoArtifactStore`'s manifest is bounded independently at `inlineThreshold + ~200 B`
  (§1.9) and its default 1 MiB threshold sits well inside that. The 8 MiB validation ceiling in
  §4 is deliberately below PLAN-06's 12 MB soft limit so the two guards cannot disagree.
- **The run marker, not a transaction.** PLAN-06 reaches the same conclusion as §5.4 by the same
  reasoning (replica set required, minutes-long crawl, 16 MB oplog entry). `MongoAnalysisRepository`
  writes a `run` document `status=IN_PROGRESS` first and flips it to `COMPLETE` last; every
  repository read filters on `status=COMPLETE`. §5.4's "idempotent upsert keyed by the natural key"
  is the same mechanism described from the write side.
- **No ordinal in any key.** PLAN-06 rejects `chunk_%04d` (the 9,999-node cap) and PLAN-00 finding
  #5 rejects run-scoped node numbering. `AnalysisRepository`'s identity types (§2.2) carry no
  ordinal, and `MongoArtifactStore` never parses one out of a key — it treats `chunk_0007.json` as
  an opaque leaf, exactly as the local and S3 backends do.

---

## 2. `AnalysisRepository` — the additive interface

### 2.1 Why it exists

PLAN-00 makes the argument; PLAN-02 §10 and PLAN-03 supply the evidence. Four endpoints —
`/joins`, `/cursors`, `/sequences`, `/dynamic-sql` — are computed by **scanning every
`api/nodes/*.json` on each request** and memoising the result under `name::joins` etc. That is
exactly the query an index makes trivial. The reverse lookup "which endpoints touch table X" is
served today only by `api/tables/index.json`'s `tables[].usedBy[]` array (PLAN-02 §3.9) — a
152–307 KB blob today, extrapolated to **~500 MB**.

### 2.1.1 The access patterns the interface must serve (PLAN-03)

PLAN-03 §1.5 and §4.3 name the specific costs. These are the acceptance criteria for §2.2, not
background:

| PLAN-03 finding | Cost today | Repository method |
|---|---|---|
| §1.5 P1 / §4.3 #1 — **"is any session running?"** fetched every 30 s on every open page, twice per tick in Corrections, to compute one boolean. *"The single most wasteful recurring call in the suite."* | a full session list per 30 s per page | `boolean anySessionRunning(Scope)` — a dedicated predicate, not a list the caller reduces |
| §1.5 P11/P13, §4.3 #2/#3 — the whole Claude call log re-fetched every **5 s** to show seven totals; the whole conversion job re-fetched every **1.5 s**; 10,000 log lines shipped and filtered in JS per keystroke | multi-MB on a timer | `Page<LogLine> logLines(LogScope, LogFilter, PageRequest)` with a **`since` cursor** — requires append-only artifacts modelled as rows (PLAN-06 "Append-only"), which is exactly what that plan does |
| §1.5 L22 / §4.3 #5 — one table-detail modal issues **two `Promise.all` fan-outs of N node fetches** (documents up to 370 KB each) | N+1, unbounded | `tableJoins(TableSelector, …)` and `tableColumns(TableSelector, …)` as single queries |
| §1.5 L5 / §4.3 #6 — "which call paths touch collection C" issues an unbounded parallel call-tree fetch to render one popup | N+1, unbounded | `endpointsUsingTable(TableSelector, …)` — a Mongo collection name and a SQL table are the same `TableSelector` here |
| §4.3 #4 — CONFIRMED/REMOVED/NEW counts reimplemented **four times** over a 138 KB document that already carries the totals | 138 KB × 4 | counts are fields on `AnalysisSummary`, never recomputed |
| §4.2 (15 rows) — `claudeEnriched`, chunk `status`, `hasReport`, `dbType` are all decided by **404-probing or directory listing** | a LIST or a 404 per boolean | fields on `AnalysisSummary` / `ChunkSummary`. `dbType` in particular is substring-scanned out of `jdbcUrl` in two duplicated places today |
| §1.5 #4 — summary slices merged back into the header **by array index**; any ordering change silently corrupts the UI | latent corruption | every returned row carries a stable id (`EndpointId`, §2.2); no caller ever joins by position |
| §4.1 — `LazyAnalysisResult` exists specifically to avoid "loading the entire 27-45MB analysis.json at once" | — | **that mitigation must survive.** No repository method returns a whole analysis; `blobRef` + a ranged `ArtifactStore.read` is the escape hatch |

> **The dependency PLAN-06 also flags, restated because it governs whether any of this pays off.**
> PLAN-03 §1.5 #3: **no endpoint anywhere accepts `page`, `offset`, `limit` or a cursor** — all 20+
> tables fetch a full array and slice in the browser. So server-side paging in this interface buys
> **nothing** until the UI changes in lockstep. PLAN-08 owns that change. The interface is still
> designed paged from day one, because retrofitting paging onto a returns-everything method is a
> breaking change and adding it now is free — but do not claim a latency win from paging until the
> UI consumes it.

### 2.2 The interface

Deliberately small. Every method below maps to a real controller in PLAN-02 §10 or a real identity
in PLAN-01 §7. Nothing speculative.

```java
package com.analyzer.storage;

/**
 * Queries over normalised analysis entities. ADDITIVE — {@link ArtifactStore} is unchanged and
 * every existing caller keeps working without touching this. A caller that wants bytes uses the
 * store; a caller that wants an answer uses this.
 *
 * <p>Every listing method is paged. Nothing here returns an unbounded collection, because the
 * artifacts behind these entities are unbounded (PLAN-01 §3, PLAN-02 §5).
 */
public interface AnalysisRepository {

    // ── analyses ────────────────────────────────────────────────────────────
    Page<AnalysisSummary> analyses(AnalysisFilter filter, PageRequest page);
    Optional<AnalysisSummary> analysis(AnalysisId id);

    // ── JAR side (PLAN-01 §7) ───────────────────────────────────────────────
    Page<EndpointSummary> endpoints(AnalysisId id, EndpointFilter filter, PageRequest page);
    Optional<EndpointSummary> endpoint(EndpointId id);
    Page<CallNodeSummary> callTreeNodes(EndpointId endpoint, PageRequest page);

    // ── PL/SQL side (PLAN-02 §11) ───────────────────────────────────────────
    Page<FlowNodeSummary> flowNodes(AnalysisId id, NodeFilter filter, PageRequest page);
    Optional<FlowNodeDetail> flowNode(NodeId id);
    Page<EdgeRef> callees(NodeId from, PageRequest page);
    Page<EdgeRef> callers(NodeId to, PageRequest page);

    // ── tables, and the reverse lookups that justify the mode ───────────────
    Page<TableRef>  tables(AnalysisId id, TableFilter filter, PageRequest page);
    Page<TableUsage> tableUsages(TableSelector table, PageRequest page);
    /** "Which endpoints touch table X" — the headline query. Scope may span analyses. */
    Page<EndpointRef> endpointsUsingTable(TableSelector table, PageRequest page);
    /** The PL/SQL twin: which flow nodes touch table X. */
    Page<NodeRef> nodesUsingTable(TableSelector table, PageRequest page);
    /** PLAN-03 §4.3 #5: replaces an N-node Promise.all fan-out per table-detail modal. */
    Page<JoinRef>   tableJoins(TableSelector table, PageRequest page);
    Page<ColumnRef> tableColumns(TableSelector table, PageRequest page);

    // ── the four scan-every-node facets (PLAN-02 §10) ───────────────────────
    Page<FacetRow> facet(AnalysisId id, FacetKind kind, PageRequest page);

    // ── append-only streams: incremental reads, never re-fetch the whole thing ──
    /**
     * PLAN-03 §1.5 P11/P13 and §4.3 #2/#3. Logs, Claude call logs, conversion output and chat
     * transcripts are append-only; today each is re-fetched whole on a 1.5–5 s timer. Reading
     * from a cursor with a server-side level/text filter is the entire point.
     *
     * <p>Requires the append-only artifacts to be rows, not an array in one document — which is
     * PLAN-06's "Append-only (the awkward family)" decision, one document per entry keyed
     * {@code (scope_id, seq)}.
     */
    Page<LogLine> logLines(LogScope scope, LogFilter filter, PageRequest page);

    // ── cheap predicates that today cost a whole list ───────────────────────
    /**
     * PLAN-03 §4.3 #1 — the single most wasteful recurring call in the suite: the full session
     * list fetched every 30 s on every open page (twice per tick in Corrections) purely to
     * evaluate {@code sessions.some(s => s.status === 'RUNNING')}. A predicate, not a list.
     * Backed by a partial index on {@code status:'RUNNING'} in mongodb mode.
     */
    boolean anySessionRunning(Scope scope);

    // ── bridge back to bytes ────────────────────────────────────────────────
    /**
     * Location of the blob for an entity that has one — decompiled source, a stored jar, a PL/SQL
     * source file. Returned as an {@link ArtifactKey} so the caller reads it through the
     * (credential-guarded) {@link ArtifactStore}. The repository NEVER returns bytes.
     */
    Optional<ArtifactKey> blobRef(BlobRef ref);

    /** True when this implementation answers queries from an index rather than by scanning. */
    boolean indexed();
}
```

Supporting types (records, all in `com.analyzer.storage.repo`):

```java
/** Cursor-based paging. `after` is an opaque token from the previous Page; null starts at the top. */
public record PageRequest(int limit, String after, Sort sort) {
    public static PageRequest first(int limit) { return new PageRequest(limit, null, Sort.NATURAL); }
    public PageRequest next(Page<?> p)         { return new PageRequest(limit, p.nextCursor(), sort); }
}

/**
 * @param total -1 when the implementation cannot count cheaply. Callers must render "many"
 *              rather than assume a number — the scan-based implementation cannot count a
 *              500 MB facet without doing the work twice.
 */
public record Page<T>(List<T> items, String nextCursor, long total) {
    public boolean hasMore() { return nextCursor != null; }
}

public record AnalysisId(Kind kind, String value) {          // JAR: jarId; PLSQL: the '~' identity
    public enum Kind { JAR, PLSQL }
}
public record EndpointId(AnalysisId analysis, String controllerClass, String methodName,
                         String httpMethod, String fullPath) {}   // PLAN-01 §7 — NOT the NNN ordinal
public record NodeId(AnalysisId analysis, String nodeId) {}       // PLAN-02: SCHEMA$PKG$OBJ
public record TableSelector(Scope scope, String schema, String tableName) {
    public sealed interface Scope permits AllAnalyses, OneAnalysis {}
}
public enum FacetKind { JOIN, CURSOR, SEQUENCE, DYNAMIC_SQL }
```

Design rules baked in:

- **Cursor paging, not offset.** `skip(n)` is O(n) in Mongo and O(n) in a scan; a cursor token
  (`_id` of the last row, base64-encoded with the sort key) is O(1) for Mongo and can encode a
  file/offset position for the scanning implementation. Offset paging over a 500 MB facet is a
  performance trap that only shows up in production.
- **Identity is the natural key, never the ordinal.** PLAN-01 §7 and PLAN-00 finding #5 both say
  node/endpoint ordinals are run-scoped and not stable. `EndpointId` uses the five-field natural
  key; the `NNN` folder prefix never appears.
- **No bytes.** `blobRef` returns a key, so PLAN-00 finding #6 (decompiled source is an artifact,
  not an entity) is enforced by the type system, and C4 is preserved because the read still goes
  through `CredentialGuardStore`.
- **`total` may be -1.** Better an honest sentinel than a number one implementation fabricates.

### 2.3 Implementation per mode — no mode loses functionality

| mode | `ArtifactStore` | `AnalysisRepository` |
|---|---|---|
| `local` | `LocalArtifactStore` | `ArtifactScanAnalysisRepository` over the same store |
| `s3` | `S3ArtifactStore` | `ArtifactScanAnalysisRepository` over the same store |
| `mongodb` | `MongoArtifactStore` | `MongoAnalysisRepository` |

**One** scan-based implementation serves both `local` and `s3`, because it is written against
`ArtifactStore` and therefore cannot tell them apart — the same property that made S3 a single
class. It reads exactly the artifacts the controllers read today:

| Repository method | Artifact read | Same as today? |
|---|---|---|
| `analyses` | scan for `api/index.json` / `jar/*/_header.json` | yes — `AnalysisService.indexKeysByName`; `_header.json` exists precisely so the picker never opens the huge analysis (PLAN-01 §2.3) |
| `endpoints` | `jar/<id>/endpoints/*/endpoint.json` via **`statPrefix`**, one round trip | today's readers loop per file; this is strictly faster on S3 |
| `callTreeNodes` | `endpoints/<ep>/nodes/node_<n>.json` | yes |
| `flowNodes` / `flowNode` | `api/index.json`, `api/nodes/<safe>.json` | yes |
| `callees` / `callers` | `api/call_graph.json` + `api/node_id_map.json` | yes — `CallTreeFacetReader` |
| `tables` / `tableUsages` / `nodesUsingTable` | `api/tables/index.json` → `tables[].usedBy[]` | yes — `TableFacetReader` |
| `endpointsUsingTable` | `_summary_static.json` `endpoints[]` + `EndpointInfo.aggregatedCollections` | yes |
| `tableJoins` / `tableColumns` | `api/tables/index.json` + `api/nodes/*.json` | yes — but as **one** server-side pass instead of the browser's N-node `Promise.all`, so even the scan implementation removes PLAN-03 §4.3 #5's fan-out |
| `facet` | scan every `api/nodes/*.json` | yes — verbatim today's behaviour, including the `name::joins` memo cache |
| `logLines` | ranged `ArtifactStore.read(key, offset, -1)` on the `.jsonl` artifact | **better than today.** The `since` cursor is a byte offset, so a tail is a ranged read rather than a whole-file fetch. This is the one place the scan implementation beats today's code outright, and it is why `read(key, offset, length)` was worth having on the frozen interface |
| `anySessionRunning` | `statPrefix` + read only the session index | yes, but one round trip instead of a full list transfer |
| `blobRef` | pure key arithmetic, no I/O | n/a |

So the scan implementation is **not a regression and not a stub**: it is today's code moved behind
an interface, with `statPrefix` used where a per-file loop exists. `indexed()` returns `false`, and
a UI that wants to warn "this may take a moment" can check it. Paging is applied after
materialisation for the facet methods — identical to today, where the whole facet is computed and
returned in one response.

**No existing caller is forced to change.** The repository is a new bean; the controllers keep
their `ArtifactStore` and their current readers. Migrating a controller to the repository is a
separate, opt-in change per endpoint (PLAN-08's job), and each migration is independently
revertible.

> **OPEN QUESTION — who owns the repository interface's home?** It sits in `analyzer-storage`
> alongside `ArtifactStore`, which keeps the module's "depends on no other project module" rule
> intact but means the entity records duplicate shapes that live in `jar-analyzer-core`
> (`ClassInfo`, `EndpointInfo`, `CallNode`).
> Options: (a) thin summary records in `analyzer-storage`, mapped at the edge — duplication, but
> the module stays dependency-free; (b) a new `analyzer-model` module both depend on.
> **Recommendation: (a).** The repository returns *summaries* for listing, not full domain objects;
> the full object still comes from the artifact. Revisit if the summaries start growing toward the
> domain types.

---

## 3. Configuration

### 3.1 Properties

```properties
# ── Storage backend ───────────────────────────────────────────────────
# local   = files on disk under app.data-dir.base (default; today's behaviour)
# s3      = objects under s3://{bucket}/{prefix}/ — same layout, same file names
# mongodb = manifest documents + GridFS under {database}, same keys, same file names
app.storage.mode=local

# mongodb mode only. See the credentials note below — this URI must be credential-free.
app.storage.mongodb.uri=mongodb://localhost:27017/?replicaSet=rs0
app.storage.mongodb.database=analyzer
app.storage.mongodb.prefix=data
app.storage.mongodb.artifacts-collection=artifacts
app.storage.mongodb.gridfs-bucket=artifacts_fs
app.storage.mongodb.inline-threshold-bytes=1048576
app.storage.mongodb.max-pool-size=50
app.storage.mongodb.min-pool-size=0
app.storage.mongodb.connect-timeout-ms=10000
app.storage.mongodb.server-selection-timeout-ms=15000
app.storage.mongodb.socket-timeout-ms=0
app.storage.mongodb.wait-queue-timeout-ms=10000
app.storage.mongodb.write-concern=majority
app.storage.mongodb.read-preference=primary
app.storage.mongodb.retry-writes=true
app.storage.mongodb.index-ensure=on-startup
app.storage.mongodb.list-scan-warn-threshold=100000
```

Environment-variable twins follow the existing convention in `StorageFactory.env(...)`:
`APP_STORAGE_MONGODB_URI`, `APP_STORAGE_MONGODB_DATABASE`, `APP_STORAGE_MONGODB_PREFIX`, with the
system property winning over the environment variable.

`prefix=data` keeps the key layout identical across all three modes, which is C5's precondition:
`data/jar/<id>/analysis.json` is the local path, the S3 key, and the Mongo `_id`.

### 3.2 Credentials — never in a config file this app serves

S3 mode's rule is stated at `StorageConfig.java:40-41` and `StorageFactory.java:145-147`:
credentials come from `DefaultCredentialsProvider` and there is **deliberately no property for an
access key**. MongoDB has no equivalent default chain, so the rule is enforced instead:

1. **`app.storage.mongodb.uri` must not contain userinfo.** At startup, if the URI resolved from a
   property or `application.properties` matches `//[^/@]*@`, **refuse to boot** with a message
   naming the property. This is a hard gate, not a warning — a `mongodb://user:pw@host/` in
   `application.properties` is exactly the C4 failure the guard exists to prevent, and
   `unified-web` serves its own config tree.
2. **The credentialed connection string comes from the environment**, `APP_STORAGE_MONGODB_URI`,
   injected by the deployment platform (ECS task secret, Kubernetes secret, systemd
   `EnvironmentFile`). Never checked in, never in the data tree, never in the store.
3. **Preferred: no secret at all.** For Atlas use `authMechanism=MONGODB-AWS`, which reuses the
   same AWS credential chain S3 mode already uses; for self-hosted use x.509
   (`authMechanism=MONGODB-X509` + a mounted keystore path, not a password). Either way the
   connection string in config is credential-free and the gate in (1) is satisfied by
   construction. **Recommendation: `MONGODB-AWS` in AWS environments** — it is the one option that
   makes the two remote modes share a single credential story.
4. **Never log the URI.** `StorageFactory` logs a redacted form —
   `mongodb://***@host:27017/analyzer` — built by a `redact(String uri)` helper, and the
   `MongoClientSettings` are never `toString()`d into a log line.
5. **`plsql-config.yaml` / `plsql-env-config.yaml` still cannot get in.** PLAN-02 §3.22 confirms
   they are not artifacts, and `CredentialGuardStore` blocks them by leaf name at the boundary.

### 3.3 `CredentialGuardStore` keeps wrapping everything

`StorageFactory.create` (`:123-125`) is the only public construction path and it does
`new CredentialGuardStore(createBackend(...))` unconditionally, before the mode switch. Adding a
mode therefore inherits C4 with **zero new code** — which is the entire reason the guard was put
there rather than in each backend (`StorageFactory:119-122`).

Two things must hold and should be asserted:

- `MongoArtifactStore`'s constructor is not called anywhere except `StorageFactory.createBackend`
  and its own tests. Enforceable by an ArchUnit-style test or a grep in the gate.
- `CredentialGuardStoreTest` gains a `mode=mongodb` case asserting
  `StorageFactory.create(Settings.mongodb(...))` returns a `CredentialGuardStore`, so a future
  fourth mode cannot skip it.

The repository needs no equivalent guard: it never models a credential file (nothing in PLAN-01/02
maps `plsql-config.yaml` to an entity) and it returns `ArtifactKey`s, not bytes, so every actual
read goes back through the guarded store.

---

## 4. `StorageFactory` and `StorageMode` — the exact diff

### `StorageMode.java`

```java
    /** S3 bucket under the {@code data/} prefix. Object storage is the source of truth. */
    S3,

+   /** MongoDB: manifest documents + GridFS, same keys and same layout. */
+   MONGODB;

    public static StorageMode parse(String s) {
        if (s == null || s.isBlank()) return LOCAL;
        return switch (s.trim().toLowerCase()) {
            case "local", "internal" -> LOCAL;
            case "s3", "external"    -> S3;
+           case "mongodb", "mongo"  -> MONGODB;
            default -> throw new IllegalArgumentException(
-                   "Unknown app.storage.mode '" + s + "'. Expected: local | s3");
+                   "Unknown app.storage.mode '" + s + "'. Expected: local | s3 | mongodb");
        };
    }
```

### `StorageFactory.Settings` — one component, one factory, one `validate` case

```java
+   /** MONGODB only. Null in other modes. Credentials are NOT a field here — see §3.2. */
+   public record Mongo(String uri, String database, String prefix, String collection,
+                       String gridFsBucket, long inlineThresholdBytes, MongoPool pool) {}
+   public record MongoPool(int maxSize, int minSize, int connectTimeoutMs,
+                           int serverSelectionTimeoutMs, int socketTimeoutMs,
+                           int waitQueueTimeoutMs, String writeConcern, String readPreference,
+                           boolean retryWrites) {}

    public record Settings(StorageMode mode, Path dataRoot,
-                          String bucket, String prefix, String region, String endpoint) {
+                          String bucket, String prefix, String region, String endpoint,
+                          Mongo mongo) {

        public static Settings local(Path dataRoot) {
-           return new Settings(StorageMode.LOCAL, dataRoot, null, null, null, null);
+           return new Settings(StorageMode.LOCAL, dataRoot, null, null, null, null, null);
        }

        public static Settings s3(String bucket, String prefix, String region, String endpoint) {
-           return new Settings(StorageMode.S3, null, bucket, prefix, region, endpoint);
+           return new Settings(StorageMode.S3, null, bucket, prefix, region, endpoint, null);
        }

+       public static Settings mongodb(Mongo mongo) {
+           return new Settings(StorageMode.MONGODB, null, null, mongo.prefix(), null, null, mongo);
+       }

        public Settings validate() {
            switch (mode) {
                case LOCAL -> { ... }
                case S3    -> { ... }
+               case MONGODB -> {
+                   if (mongo == null || mongo.uri() == null || mongo.uri().isBlank()) {
+                       throw new IllegalArgumentException(
+                               "Storage mode is mongodb but no connection string was given. "
+                               + "Set APP_STORAGE_MONGODB_URI (see PLAN-07 §3.2).");
+                   }
+                   if (HAS_USERINFO.matcher(mongo.uri()).find() && fromServedConfig) {
+                       throw new IllegalArgumentException(
+                               "app.storage.mongodb.uri must not contain credentials. Supply the "
+                               + "credentialed URI via APP_STORAGE_MONGODB_URI, or use "
+                               + "authMechanism=MONGODB-AWS / MONGODB-X509.");
+                   }
+                   if (mongo.database() == null || mongo.database().isBlank()) {
+                       throw new IllegalArgumentException(
+                               "Set app.storage.mongodb.database for mode=mongodb.");
+                   }
+                   if (mongo.inlineThresholdBytes() <= 0
+                           || mongo.inlineThresholdBytes() > 8L * 1024 * 1024) {
+                       throw new IllegalArgumentException(
+                               "app.storage.mongodb.inline-threshold-bytes must be in (0, 8 MiB]; "
+                               + "larger values risk the 16 MB BSON limit.");
+                   }
+               }
            }
            return this;
        }
    }
```

`settingsFor` (the non-Spring entry point used by `FlowAnalysisMain:163` and `PocRunner:225`) gains
one arm, following the existing `env(...)` pattern exactly:

```java
        return switch (mode) {
            case LOCAL -> Settings.local(localRoot);
            case S3    -> Settings.s3(...);
+           case MONGODB -> Settings.mongodb(new Settings.Mongo(
+                   env("APP_STORAGE_MONGODB_URI",      "app.storage.mongodb.uri", null),
+                   env("APP_STORAGE_MONGODB_DATABASE", "app.storage.mongodb.database", "analyzer"),
+                   env("APP_STORAGE_MONGODB_PREFIX",   "app.storage.mongodb.prefix", "data"),
+                   "artifacts", "artifacts_fs", 1L << 20, MongoPool.defaults()));
        };
```

`createBackend` gains one arm, symmetrical with S3:

```java
+           case MONGODB -> {
+               Settings.Mongo m = settings.mongo();
+               log.info("Storage mode: MONGODB — {}/{} prefix={}",
+                        redact(m.uri()), m.database(), settings.normalisedPrefix());
+               yield new MongoArtifactStore(buildMongoClient(m), m.database(),
+                                            m.collection(), m.gridFsBucket(),
+                                            settings.normalisedPrefix(), m.inlineThresholdBytes());
+           }
```

and one private builder, mirroring `buildClient(Settings)`:

```java
+   private static MongoClient buildMongoClient(Settings.Mongo m) {
+       MongoClientSettings.Builder b = MongoClientSettings.builder()
+               .applyConnectionString(new ConnectionString(m.uri()))
+               .applyToConnectionPoolSettings(p -> p
+                       .maxSize(m.pool().maxSize()).minSize(m.pool().minSize())
+                       .maxWaitTime(m.pool().waitQueueTimeoutMs(), MILLISECONDS))
+               .applyToSocketSettings(s -> s
+                       .connectTimeout(m.pool().connectTimeoutMs(), MILLISECONDS)
+                       .readTimeout(m.pool().socketTimeoutMs(), MILLISECONDS))
+               .applyToClusterSettings(c -> c
+                       .serverSelectionTimeout(m.pool().serverSelectionTimeoutMs(), MILLISECONDS))
+               .writeConcern(WriteConcern.valueOf(m.pool().writeConcern()))
+               .readPreference(ReadPreference.valueOf(m.pool().readPreference()))
+               .retryWrites(m.pool().retryWrites());
+       return MongoClients.create(b.build());
+   }
```

### `StorageConfig.java`

```java
            @Value("${app.storage.s3.endpoint:}") String endpoint,
+           @Value("${app.storage.mongodb.uri:}") String mongoUri,
+           @Value("${app.storage.mongodb.database:analyzer}") String mongoDb,
+           @Value("${app.storage.mongodb.prefix:data}") String mongoPrefix,
+           @Value("${app.storage.mongodb.artifacts-collection:artifacts}") String mongoColl,
+           @Value("${app.storage.mongodb.gridfs-bucket:artifacts_fs}") String mongoBucket,
+           @Value("${app.storage.mongodb.inline-threshold-bytes:1048576}") long inlineThreshold) {
        ...
        return StorageFactory.create(switch (mode) {
            case LOCAL -> StorageFactory.Settings.local(Path.of(dataDir));
            case S3    -> StorageFactory.Settings.s3(bucket, prefix, region, endpoint);
+           case MONGODB -> StorageFactory.Settings.mongodb(new StorageFactory.Settings.Mongo(
+                   resolveMongoUri(mongoUri), mongoDb, mongoPrefix, mongoColl, mongoBucket,
+                   inlineThreshold, MongoPool.fromEnvironment()));
        });
```

`resolveMongoUri` prefers `APP_STORAGE_MONGODB_URI` from the environment and only falls back to the
property, applying the userinfo gate of §3.2 to the property-sourced value.

Plus a second, separate bean for the repository:

```java
+   @Bean
+   public AnalysisRepository analysisRepository(ArtifactStore store,
+           @Value("${app.storage.mode:local}") String modeProp) {
+       return StorageMode.parse(modeProp) == StorageMode.MONGODB
+               ? new MongoAnalysisRepository(/* same client, same db */)
+               : new ArtifactScanAnalysisRepository(store);
+   }
```

That is the whole diff: **one enum constant, one branch, one settings factory** — plus the new
backend class, exactly as promised.

---

## 5. Operational reality

### 5.1 Indexes and GridFS garbage

Indexes on `artifacts`:

| Index | Serves | Notes |
|---|---|---|
| `{_id: 1}` | every point lookup, every prefix range | free, unique |
| `{parent: 1}` | non-recursive listing, file children | |
| `{gridfsId: 1}`, sparse | GC: find the manifest referencing a `files_id` | sparse — only large artifacts have it |

Plus the driver's own `{files_id: 1, n: 1}` on `<bucket>.chunks` and `{filename: 1, uploadDate: 1}`
on `<bucket>.files`, created automatically by `GridFSBucket` on first upload.

Indexes the **repository** needs, one per access pattern in §2.1.1 (owned by PLAN-08, listed here
so the startup `createIndexes` call site is complete):

| Index | Serves |
|---|---|
| `{analysis_id: 1, status: 1}` | the run marker — every read filters `status=COMPLETE` (§1.11) |
| `{status: 1}` **partial**, `{status: 'RUNNING'}` | `anySessionRunning` — the partial index is tiny and the predicate is an index-only count |
| `{schema: 1, table_name: 1, analysis_id: 1}` on `table_usage` | `endpointsUsingTable` / `nodesUsingTable` / `tableJoins` — the headline query |
| `{analysis_id: 1, node_id: 1}` on `flow_node` and its children | node detail, facets |
| `{analysis_id: 1, from_node_id: 1}` and `{analysis_id: 1, to_node_id: 1}` on `flow_edge` | `callees` / `callers` |
| `{scope_id: 1, seq: 1}` on the append-only entry collection | `logLines` cursor reads (PLAN-06 "Append-only") |
| TTL on `{created_at: 1}` for append-only entries | PLAN-06 OPEN QUESTION 3 — retention, defaulting to keep-everything |

**When:** `createIndexes` at startup, guarded by `app.storage.mongodb.index-ensure=on-startup`
(alternative: `never`, for a deployment whose DBA owns DDL). `createIndexes` is idempotent, so this
is safe to run on every boot. On an empty collection it is instant; on a large existing collection
MongoDB ≥4.2 builds indexes with only a brief exclusive lock at start and end, but the build still
takes minutes and consumes I/O — so for a backfilled deployment, create the indexes **before**
importing, and set `index-ensure=never` at boot. Log the elapsed time either way. Startup must not
*block* on index creation beyond a bounded timeout; if it exceeds it, log an error and continue —
queries will be slow, but the app boots.

**GridFS GC.** Orphaned chunks arise from (a) an abandoned rolled-over write, (b) a crash between
manifest delete and `gridFsBucket.delete`, (c) a failed server-side chunk copy. A scheduled sweep
(daily, off-peak) lists `<bucket>.files` older than a safety window (24 h — must exceed the longest
possible upload) and deletes any whose `_id` has no manifest with a matching `gridfsId`. The
sparse index makes the check a point lookup. The safety window is what stops the GC racing an
in-flight upload; without it, a 40-minute 1 GB upload would be collected mid-write. Report the
reclaimed byte count as a metric — a rising number means a bug, not just garbage.

### 5.2 What happens if Mongo goes down mid-analysis

Compare honestly against S3, which has the same failure mode:

- **Writes in flight**: the driver retries once (retryable writes), then throws
  `MongoTimeoutException` after `serverSelectionTimeoutMs` (15 s). `MongoArtifactStore` wraps it in
  `IOException`, which every `write`/`close` caller already handles — the analysis run fails with
  an error, as it does today when the disk fills.
- **No partial artifacts.** Because visibility is one manifest write (§1.4), a killed upload leaves
  the previous artifact intact and the new one absent. This is *stronger* than a naive
  file-per-write and equal to `LocalArtifactStore`'s `ATOMIC_MOVE`.
- **Partial analysis trees, though.** An analysis writes dozens of artifacts; a failure halfway
  leaves some present and some missing. Identical to `local` and `s3`. The existing recovery is a
  re-run, which overwrites in place (PLAN-02 §10.2: reruns overwrite, archives get a timestamped
  object segment). Nothing new is needed, but do **not** claim mid-analysis atomicity.
- **Reads**: `exists`/`size`/`lastModified` return `-1`/`false` on a `MongoException` and log WARN.
  This is the one place a Mongo outage can look like "data is gone" to a caller (§1.10 point 6).
  Mitigation: a Spring `HealthIndicator` that pings the cluster and a metric on the swallow count,
  so an outage is visible as an outage rather than as mass 404s.
- **Replica-set failover** (10–30 s election): with `retryWrites=true` and
  `serverSelectionTimeout=15s`, a single failover is usually invisible. A standalone server has no
  such cover, which is one reason to require a replica set (§5.4).

### 5.3 Connection limits

`maxPoolSize=50` per JVM by default. The relevant pressure is not query count but **held streams**:
an open `GridFSDownloadStream` holds a *cursor*, and a connection is checked out only for the
duration of each `getMore` — so a hundred slow readers do not pin a hundred connections. The real
risks are:

- **A leaked `Stream<ArtifactEntry>` from `list`** leaves a server-side cursor open until the
  10-minute timeout. The interface already says "the caller must close the stream"
  (`ArtifactStore:66-68`); with Mongo the consequence is a real server resource, not just a file
  handle. Wire `MongoCursor.close()` into the stream's `onClose`, and add a leaked-cursor counter.
- **Parallel analysis workers.** Size the pool at `maxPoolSize >= workers × 2`. Above
  `maxPoolSize`, checkout blocks for `waitQueueTimeoutMs` (10 s) then throws — a bounded, visible
  failure rather than an unbounded queue, which is the right default.
- **Mongo server-side `maxIncomingConnections`** (default 65,536, but far lower on shared Atlas
  tiers — an M10 caps at 1,500). With multiple app instances, `instances × maxPoolSize` must stay
  under it. Document this in the runbook; it is the classic way a Mongo migration falls over on
  day one.

### 5.4 Transactions — used, and at what cost

**Recommendation: no transactions in `MongoArtifactStore`. None.**

- The atomicity the interface requires is per-key, and a single-document write already provides it
  (§1.4). Adding a transaction would buy nothing.
- Transactions require a **replica set or sharded cluster** — a standalone `mongod` cannot start
  one. Depending on them would make a single-node dev setup impossible.
- Default transaction lifetime is **60 s** (`transactionLifetimeLimitSeconds`). A 1 GB GridFS
  upload cannot fit; PLAN-01 measures `analysis.json` at ~1 GB.
- A transaction's oplog entry is itself subject to the 16 MB limit unless split, and holds a
  snapshot open, causing WiredTiger cache pressure and `WriteConflict` retries under concurrency.

Where they might earn their keep is **`MongoAnalysisRepository`'s bulk ingest** — writing several
thousand `flow_node` documents for one analysis. Even there the recommendation is **no**, and
PLAN-06 ("Failure and partial writes") reaches the identical conclusion independently: make the
ingest idempotent (`replaceOne(upsert)` keyed by the natural key from PLAN-01 §7 / PLAN-02 §11) and
re-runnable, insert a `run` document with `status=IN_PROGRESS` first, and flip it to `COMPLETE`
only after the last child document is acknowledged. Every read filters on `status=COMPLETE`, so a
partial run is **invisible rather than corrupt**, and a sweeper deletes stale `IN_PROGRESS` runs.
Same visible atomicity, none of the cost, and it degrades gracefully on a standalone server.
A transaction is a reasonable v2 option only if a partial ingest turns out to be user-visible in
practice.

A replica set is still **recommended** — for retryable writes, majority write concern and
failover — just not *required* by the storage design.

### 5.5 Mongo and S3 both configured

`app.storage.mode` is single-valued, so `mongodb` wins and the `app.storage.s3.*` properties are
inert. That is quiet in a bad way: someone who sets a bucket and gets no objects will spend an
afternoon on it. So: **at startup, log WARN naming every populated property that the active mode
ignores.** Cheap, and it turns a mystery into a log line.

The more interesting case is the one people will actually want:

> **OPEN QUESTION — blobs in S3, entities in Mongo.** This is the likely production shape: a 70 MB
> `stored.jar` and a 1 GB `analysis.json` belong in S3, while the queries belong in Mongo. Storing
> a 1 GB artifact in GridFS works but costs ~4,000 chunk documents and puts blob I/O on the same
> cluster as the queries.
> Options: (a) a fourth mode `hybrid`; (b) **a second, independent property** —
> `app.storage.repository=auto|artifact-scan|mongodb`, defaulting to `auto` (mongodb iff
> `mode=mongodb`), so `mode=s3` + `repository=mongodb` is expressible; (c) defer.
> **Recommendation: (b).** It costs one property and one condition in the repository bean, needs no
> new `StorageMode` constant, and it falls straight out of having made the repository a separate
> bean. Ship `mongodb` mode first, then enable the combination — the design already permits it.

---

## 6. Test strategy that works offline

### 6.1 The constraint, measured

Checked in this environment:

- `~/.m2/repository/org/mongodb/` has `mongodb-driver-sync` **4.11.2** (already a dependency of
  `jar-analyzer-core`), `mongodb-driver-core`, `bson`. **The driver is available offline.**
- `~/.m2/repository/de/` contains only `siegmar` — **no flapdoodle embedded MongoDB.**
- `~/.m2/repository/org/testcontainers/` contains only `testcontainers-bom` — **no Testcontainers
  jars, no `mongodb` module.**
- No Docker, no network, no `mongod`.

So the offline strategy cannot rely on any server, embedded or containerised. There are two ways
out:

- **(a) `FakeMongoServer`** — a wire-protocol fake, exactly the shape `FakeS3Server` already takes
  for S3 (558 lines, `com.sun.net.httpserver.HttpServer`, in-memory bucket, `maxKeys(3)` to force
  pagination). For Mongo it is a `ServerSocket` speaking `OP_MSG` with BSON bodies, backed by
  in-memory maps. Bigger than the S3 fake — estimate 800–1,200 lines — but bounded, because the
  store issues a *small, closed* set of commands: `hello`, `buildInfo`, `ping`, `find`, `getMore`,
  `killCursors`, `insert`, `update`, `delete`, `findAndModify`, `aggregate`, `createIndexes`,
  `endSessions`. Crucially, **GridFS is entirely client-side** — the driver implements it over
  `fs.files`/`fs.chunks` with ordinary CRUD — so the fake needs no GridFS support at all.
- **(b) A `MongoOps` seam** in production code with an in-memory test double. Rejected: it adds an
  abstraction to shipped code purely for tests, and it tests our own double rather than the driver
  — precisely the class of bug (driver semantics) the conformance suite exists to catch.

**Recommendation: (a).** Scope it hard: implement only the commands listed, and make an unknown
command **fail loudly** with `CommandNotSupported` so the fake can never silently pass a test by
ignoring an operation. Configure a tiny batch size (the analogue of `FakeS3Server.maxKeys(3)`) so
every listing exercises the `getMore` path.

### 6.2 Provable offline

| # | Test | How |
|---|---|---|
| 1 | **The full `ArtifactStoreConformanceTest`, unmodified** | `MongoArtifactStoreConformanceTest extends ArtifactStoreConformanceTest`, overriding only `newStore(Path)` to spin a `FakeMongoServer` + `MongoClient` per test — byte-for-byte the shape of `S3ArtifactStoreConformanceTest`. This is the single highest-value test and the C1 proof. |
| 2 | Key ↔ `_id` mapping | Pure unit, no server. Round-trip `ArtifactKey` → `_id` → `ArtifactKey` over adversarial segments (`$`, `[`, `(`, unicode, 900-byte). |
| 3 | Prefix range arithmetic | Pure unit. Assert `[p+"/", p+"0")` includes `p/a`, `p/a/b`, and **excludes** the sibling `p-x` and the prefix document `p` itself — the `relativeKey` edge case S3 handles at `:326`. |
| 4 | Inline ↔ GridFS rollover | Write at `threshold-1`, `threshold`, `threshold+1`, and 4 MiB. Assert storage path and byte-identical round trip. |
| 5 | 16 MB guard | Assert no `artifacts` document exceeds 16 MB for any input, including a 4 MiB write (which must have gone to GridFS). Satisfies PLAN-00 judging criterion #4 **by test, not inspection**. |
| 6 | **Projection excludes `inline`** | Fake records every command; assert no `find` issued by `list`/`statPrefix`/`exists`/`size`/`lastModified` returns an `inline` field. Guards the §1.3 perf trap, which no functional test would catch. |
| 7 | Round-trip budget | Assert `statPrefix` over 500 keys issues **one** `find` plus getMores, never 500 `find`s. The Mongo analogue of the 4,345-file / 3.6-minute disaster in `ArtifactStore:96-98`. |
| 8 | `deletePrefix` batching + GridFS cleanup | Assert both manifests and chunks are gone, and the count matches. |
| 9 | Orphan GC | Simulate an abandoned rolled-over write; assert the sweep reclaims it and that the safety window protects an in-flight upload. |
| 10 | Config validation | `Settings.mongodb(...).validate()` rejects: blank URI, userinfo in a property-sourced URI, blank database, threshold ≤ 0 or > 8 MiB. Pure unit. |
| 11 | URI redaction | `redact()` never emits a password; assert over a table of URI shapes. Pure unit. |
| 12 | `CredentialGuardStore` still wraps | `StorageFactory.create(Settings.mongodb(...))` returns a `CredentialGuardStore`; `plsql-env-config.yaml` is blocked in mongodb mode. Extends the existing `CredentialGuardStoreTest`. |
| 13 | Repository conformance | A new `AnalysisRepositoryConformanceTest` — same pattern as the store's — run against **both** `ArtifactScanAnalysisRepository` (over `InMemoryArtifactStore` seeded with real fixture artifacts) and `MongoAnalysisRepository` (over the fake). Same fixtures, same expected answers. This is what proves "no mode loses functionality". |
| 14 | Cursor paging | Page through a 1,000-row facet at limit 7; assert no duplicates, no gaps, and stability across an interleaved insert. |
| 15 | Cursor-leak guard | Assert `list(...)` closes the `MongoCursor` on stream close and on early termination (`findFirst`). |

### 6.3 Genuinely requires a live server — the untested surface

An honest list. None of these is covered by anything above, and shipping without them means
shipping with known unknowns:

1. **Index selection.** `explain()` output — that the prefix range really is an `IXSCAN` and the
   `$group` really is `PROJECTION_COVERED`. A fake cannot have a query planner.
2. **Index build time and impact** on a large, populated collection.
3. **Real 16 MB server-side rejection.** The fake will happily accept an oversized document; test 5
   asserts our own client-side guard, not the server's. A real server is the only way to confirm
   the guard fires first.
4. **GridFS at scale** — a 1 GB upload, ~4,000 chunks, upload throughput, and whether
   `GridFSDownloadStream.skip` into the middle of it really avoids the preceding transfer. The
   *semantics* are tested; the *performance claim in §1.5 is not*.
5. **Auth**: TLS, `MONGODB-AWS`, `MONGODB-X509`. Untestable without a real handshake — and §3.2's
   recommendation depends on it working.
6. **Replica-set behaviour**: retryable writes across a real failover, `w:majority` latency,
   read-preference and read-concern semantics, election windows (§5.2).
7. **Connection-pool saturation** under real latency, `waitQueueTimeoutMs` firing, and the
   `instances × maxPoolSize` ceiling of §5.3.
8. **Server-side `$merge` copy** (§1.8) — whether it behaves as described and whether it is
   actually faster than streaming.
9. **The `_id` index key limit** (§1.2 OPEN QUESTION). A fake has no such limit, so the failure
   mode is invisible offline. **This is the most likely production surprise in this document.**
10. **Wire compression** (`zstd`/`snappy`) and its effect on large-artifact throughput.
11. **Real query latency**, i.e. whether the repository actually beats the scan implementation
    it exists to replace. The entire justification for the mode (PLAN-00, C3) is unverified until
    a live server runs PLAN-03's access-pattern table (§2.1.1). Note that even a live server only
    proves the *server* side: PLAN-03 §1.5 #3 means the UI still fetches whole lists, so an
    end-to-end page-load win is not measurable until PLAN-08 lands.
12. **Disk/oplog growth** with GridFS chunks; sharding, if it ever becomes relevant.

Mitigation for the gap: gate the mode behind an explicit opt-in and treat the first live
environment as the real test. Items 1, 3, 9 and 11 should be a required checklist before
`mongodb` is recommended to anyone, not a follow-up ticket.

---

## 7. Proving C1 held

Three independent proofs. Any one of them failing means the design was violated.

### 7.1 Signature diff — mechanical, not by eye

```bash
# 1. Nothing in the file changed at all.
git diff --exit-code -- analyzer-storage/src/main/java/com/analyzer/storage/ArtifactStore.java

# 2. Stronger: the compiled signature is identical, javadoc and formatting aside.
git stash && mvn -o -q -pl analyzer-storage -DskipTests compile \
  && javap -p -cp analyzer-storage/target/classes com.analyzer.storage.ArtifactStore > /tmp/before.txt
git stash pop && mvn -o -q -pl analyzer-storage -DskipTests compile \
  && javap -p -cp analyzer-storage/target/classes com.analyzer.storage.ArtifactStore > /tmp/after.txt
diff /tmp/before.txt /tmp/after.txt   # must be empty
```

Expected `javap` output, pinned here so a reviewer can check without running anything — 14 methods,
unchanged:

```
public interface com.analyzer.storage.ArtifactStore {
  public abstract java.io.OutputStream write(com.analyzer.storage.ArtifactKey) throws java.io.IOException;
  public abstract java.io.OutputStream write(com.analyzer.storage.ArtifactKey, long) throws java.io.IOException;
  public abstract java.io.InputStream read(com.analyzer.storage.ArtifactKey) throws java.io.IOException;
  public abstract java.io.InputStream read(com.analyzer.storage.ArtifactKey, long, long) throws java.io.IOException;
  public abstract boolean exists(com.analyzer.storage.ArtifactKey);
  public abstract long size(com.analyzer.storage.ArtifactKey);
  public abstract long lastModified(com.analyzer.storage.ArtifactKey);
  public abstract boolean delete(com.analyzer.storage.ArtifactKey);
  public abstract int deletePrefix(com.analyzer.storage.ArtifactKey) throws java.io.IOException;
  public abstract java.util.stream.Stream<com.analyzer.storage.ArtifactEntry> list(com.analyzer.storage.ArtifactKey, boolean) throws java.io.IOException;
  public abstract void copy(com.analyzer.storage.ArtifactKey, com.analyzer.storage.ArtifactKey) throws java.io.IOException;
  public abstract void move(com.analyzer.storage.ArtifactKey, com.analyzer.storage.ArtifactKey) throws java.io.IOException;
  public abstract com.analyzer.storage.LocalHandle materialize(com.analyzer.storage.ArtifactKey) throws java.io.IOException;
  public java.util.Map<com.analyzer.storage.ArtifactKey, com.analyzer.storage.ArtifactEntry> statPrefix(com.analyzer.storage.ArtifactKey) throws java.io.IOException;
}
```

Make this a build step, not a ritual: an `ArtifactStoreSignatureTest` that reflects over the
interface and asserts against a checked-in golden list. Then C1 is enforced by CI rather than by
reviewer discipline, and a future contributor who widens the interface finds out in 30 seconds.

Also assert the blast radius is zero:

```bash
git diff --stat -- analyzer-storage/src/main/java/com/analyzer/storage/{ArtifactKey,ArtifactEntry,LocalHandle,CredentialGuardStore,LocalArtifactStore,InMemoryArtifactStore}.java
# must be empty
```

### 7.2 The conformance suite, unmodified

```bash
git diff --exit-code -- analyzer-storage/src/test/java/com/analyzer/storage/ArtifactStoreConformanceTest.java
mvn -o -pl analyzer-storage -Dtest='*ArtifactStoreConformanceTest' test
```

Three classes must run the same 24 tests: the base (`LocalArtifactStore`),
`S3ArtifactStoreConformanceTest`, and the new `MongoArtifactStoreConformanceTest`. The new subclass
is allowed to contain exactly two things — an override of `newStore(Path)` and an `@AfterEach`
teardown — matching `S3ArtifactStoreConformanceTest` line for line in structure. If the Mongo
backend needs the suite relaxed, the backend is wrong, not the suite. (The one permitted addition
is a *new* assertion added to the **base** class and therefore applied to all three backends — the
offset-past-EOF case in §1.5 is the known candidate.)

### 7.3 `/storage-check` stays green

Run the skill end to end in `mongodb` mode and report before/after:

1. `mvn -q -o -DskipTests install && mvn -o test` — baseline **1071 tests, 0 failures**, plus the
   new tests. No existing test may be modified to accommodate the mode.
2. `mvn -o -pl unified-web -Dtest=DirectFileIoGateTest test` — green, **with no new `ALLOWED`
   entries and no raised budget**. `MongoArtifactStore` does direct file I/O in exactly one place,
   `materialize`'s `Files.createTempFile`/`Files.copy`, which is category **`temps`** — the same
   entry `S3ArtifactStore` already holds, for the same reason.
3. `python3 .claude/skills/storage-check/scripts/scan.py` — **0 UNCLASSIFIED**.
4. Boot and exercise with the mode on:
   ```bash
   java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar --server.port=8099 \
        --app.storage.mode=mongodb --app.storage.mongodb.database=analyzer
   ```
   then require **200 with real content** from `/api/jar/jars`, `/api/parser/analyses`, `/api/docs`,
   `/api/conversion/jobs`, and for a real analysis name from that list:
   `/index`, `/call-graph`, `/tables`, `/source/{file}`, `/node/{file}`.
   Note the caveats the skill already records: the on-disk layout is **flat**
   (`ERP_USP_RUN_CYCLE__RQZVED`), source files may be `.prc`, and `/node/{file}` needs the `.json`
   extension. Do not invent a name.
5. The zero-local-writes acceptance test passes in `mongodb` mode as it does in `s3`: with the
   data directory made read-only, a full analysis completes. Only `temps`, `scratch`, `config` and
   `credentials` writes may appear, and every stored artifact must be reached via
   `store.materialize(key)` rather than an assumed data-tree path.

Steps 4 and 5 require a live server (§6.3) and therefore cannot run in this environment. That is a
gap in the *verification*, not in the design — say so in the PR rather than reporting the check as
passed.

---

## 8. Open questions, collected

| # | Question | Options | Recommendation |
|---|---|---|---|
| 1 | Long keys vs the 1024-byte index key limit (§1.2) | reject / conditional hash / always hash `_id` | **always hash `_id`**, keep the readable key indexed; measure real key lengths first |
| 2 | Non-recursive listing cost, O(subtree) (§1.6) | covered scan / directory markers / cap | **covered scan + WARN threshold** for v1 |
| 3 | GridFS `copy`: server-side chunk aggregation or client stream (§1.8) | `$merge` chunks / stream / fail loud like S3 | **inline `$merge` + streaming fallback**; `move` needs no copy at all |
| 4 | Where the repository's value types live (§2.3) | thin records in `analyzer-storage` / new `analyzer-model` module | **thin records**, keep the module dependency-free |
| 5 | Blobs in S3 + entities in Mongo (§5.5) | fourth `hybrid` mode / separate `app.storage.repository` property / defer | **separate property**, `auto` by default |
| 6 | Offline test vehicle (§6.1) | wire-protocol `FakeMongoServer` / a `MongoOps` seam | **`FakeMongoServer`**, closed command set, loud on anything unknown |
| 7 | Transactions for repository bulk ingest (§5.4) | none + idempotent upserts / per-analysis transaction | **none**; revisit only if a partial ingest proves user-visible |
| 8 | Paging model (§2.2) | offset / cursor | **cursor**; offset over a 500 MB facet is a production trap |

## 9. Dependencies on other chunks

- **PLAN-03** is the acceptance criterion for C3. Its §1.5 and §4.3 findings are folded into
  §2.1.1 and drove four methods onto the interface (`anySessionRunning`, `logLines`, `tableJoins`,
  `tableColumns`). If a screen needs a query that is not in §2.2, the interface is wrong, not the
  screen. Its §1.5 #3 (no endpoint accepts paging today) is the reason §2.2's paging is
  *forward-looking*, not an immediate win.
- **PLAN-06** is reconciled in §1.11. Its "GridFS is not used" ruling holds for entity documents;
  GridFS appears only inside `MongoArtifactStore`, which is the exception PLAN-06 itself names.
  Its 12 MB soft limit governs the repository write path, §1.9's threshold governs the store, and
  the two are deliberately non-overlapping. PLAN-02 §2.5's rule binds both: reuse
  `ChunkedFlowWriter`'s existing one-node-per-chunk boundary, **do not invent a second one that
  disagrees with the reader**, and do not perform the `mergeChunks` step that re-materialises
  everything.
- **PLAN-04/05** define the collections `MongoAnalysisRepository` queries. §2.2 constrains them
  from the read side: every method above must be answerable by one indexed query, and §5.1 lists
  the indexes that implies.
- **PLAN-09** owns backfill and rollback. §1.2's shared key layout (`prefix=data` in all three
  modes) is the precondition that makes a rollback something other than a restore (C5).
