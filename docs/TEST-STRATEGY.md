# Test Strategy

> *"tomorrow we do some change we can check are we did any mistake or might break"*

That is the purpose: **regression safety**. Every rule below serves it, and the design follows the
repository's existing conventions rather than inventing new ones.

Companions: [`TASK-REGISTRY.md`](TASK-REGISTRY.md), [`CODING-STANDARDS.md`](CODING-STANDARDS.md),
[`SCALE-ARCHITECTURE-PLAN.md`](SCALE-ARCHITECTURE-PLAN.md).

---

## 1. The rule that matters most

**Every task in the registry ships its tests in the same increment.** There is no testing phase at
the end.

The reason is not discipline for its own sake — it is that a testing phase is the first thing cut
under time pressure, and the cut is invisible until a regression lands. Each `TASK-REGISTRY.md` row
has a **Tests** column, and a task whose tests are not written is `WIP`, not `DONE`.

Corollary: **a task's definition of done is a command with an expected result.** "Ingest works" is
not a DoD; `ORACLE ingest → exactly 42 am_db_object docs` is.

---

## 2. The tiers

### 2.1 Unit — no Mongo, the bulk of the suite

Runs in milliseconds, on any machine, with no server. This tier exists because `DocSink`
(`CODING-STANDARDS.md` §2 I) lets a mapper be tested by collecting into a list.

| what | example |
|---|---|
| `IdCodec` recipes and hashing thresholds | `IdCodecTest` — asserts the measured 1,025 B worst case hashes to ≤ 70 B |
| every `ArtifactMapper` | feed a real artifact, assert the documents |
| document builders, Rule-1 omission | `OmissionTest` |
| `WriteStats`, batching arithmetic | `BulkWriterTest` with a fake sink |

**Use real artifacts as fixtures, not invented JSON.** PLAN-09:801-873 reuses *"real identifiers …
verbatim from PLAN-05 §8 rather than invented test data"*, and the awkward values here are real:
`#stage6`, `lambda$listRunLogs$11`, `{"schema":null,"name":null}`. Invented fixtures would not
contain them.

### 2.2 Property tests — the escaping round-trip is the model

`EscapingPropertyTest` (`DB-001`) is the template for this tier: for **every natural-key component
in the live corpus** (~45,000 measured values containing `.`, `/`, `$`, `#`), assert
`decompose(join(escape(c))) == c`.

Run over **real data, not generated strings** — the point is that no `~` appears in the corpus today
(measured: zero) while `$` appears 14,746 times, so a generator drawing from a plausible alphabet
would miss exactly the cases that matter. Supplement with generated adversarial inputs (`~`, `~1`,
`~~`, empty, NUL) which the corpus does *not* contain.

Other property tests: ordinal density (`0..n-1`, no gaps), content-hash stability across the 4
archived runs, and `_id` uniqueness over the whole corpus.

### 2.3 Integration — real local mongod, skip-if-absent, **plus the anti-skip guard**

Reuse `MongoTestSupport` (`analyzer-storage/src/test/java/com/analyzer/storage/mongo/`) exactly:
`mongodb://localhost:27017`, scratch DB, 2-second server-selection timeout,
`assumeTrue(false, "No MongoDB at … — skipping")`.

Not Testcontainers (no Docker in the measured environment) and not a fake — this tier exists to test
`$jsonSchema` rejection, partial unique indexes, `$bsonSize` and `bulkWrite` error semantics,
which are precisely what a fake models wrongly and silently. `MongoTestSupport.java:15-31` already
records this as *"a deliberate reversal of PLAN-07 §6 … A skipped test is honest; a green test
against a fake seek is not."*

**The anti-skip guard.** A build where every Mongo test skipped looks identical to one where they
all passed. So:

```
MongoTierRanTest — asserts the count of executed Mongo integration tests is > 0.
                   Fails the build when the tier is silently absent.
```
CI must run mongod. Locally, skipping is fine — but CI green must mean the tier ran.

### 2.4 Invariant gates — the regression net

These are the tests that make tomorrow's change safe. Each scans the whole system for a violated
rule, and **each is calibrated by deliberately planting a violation** (`CODING-STANDARDS.md` §4.3).

| gate | asserts | task |
|---|---|---|
| `NoArraysGateTest` | no stored document contains an array of objects | DB-014 |
| `RelationChainGateTest` | every collection reaches its run in one hop; every doc has an indexed `runId` | VER-007 |
| `OrphanRefGateTest` | every ref resolves (minus the measured nullable allowlist) | VER-001/002 |
| `DocSizeGateTest` | no document exceeds the 1 MB ingest ceiling | VER-006 |
| `NoCollectionLiteralsTest` | no `"am_…"` string outside `Collections.java` — catches a collection added without a mapper | DB-006 |
| `UnboundedResponseGateTest` | no endpoint returns an unpaged collection | MID-022 |
| `EagerRenderGateTest` | no client-side full-dataset render remains | UI-027 |
| `DirectFileIoGateTest` (existing) | this module adds **zero** allowlist entries | INF-002 |

A gate that has never failed has never been tested. Each ships with a companion test that plants a
violation and asserts the gate catches it.

### 2.5 Golden-data acceptance

Ingest the real ORACLE analysis and assert the measured counts exactly: 42 `am_db_object`,
132 `am_table_column_usage`, 356 `am_proc_node`, 2,436 statements (15,824 across five dialects),
6,970 call edges, 3,784 DAG nodes, 33,407 methods, 64,446 invocations, 111,455 junction rows.

These are **regression baselines, not invariants** — a new corpus legitimately changes them. The
check is "counts match what this ingest mapped", with the measured figures as a secondary signal
that the corpus is the one the plan was written against (`MONGO §9.2`).

### 2.6 The lossless round-trip proof — permanent, not a one-off

**This is the most valuable test in the plan.** It is the only one that would have caught the
`tables.json` omission that three manual reviews missed (`RELATIONAL §17.4`).

```
for each source artifact:
    ingest → reconstruct the original JSON from Mongo → diff field-path by field-path
```
Compare with `JsonNode.equals`, not strings — key order, whitespace and number formatting differ
legitimately (the technique `SidecarCompactnessTest` already uses).

Three classes of expected difference must be **encoded, not ignored**:
- **derived fields** (`nodeId`, `depth`, `childCount`) — the prover *computes* them and asserts they
  match, which tests the "derivable" claim rather than assuming it
- **deduplicated storage** — `directTables` and `tables` must both return byte-equal arrays
- **Rule-1 omissions** — normalise both sides, **and** assert the omitted set is exactly the
  null/false/empty set (`VER-005`), or the proof is weaker than it looks

**Coverage: 100% of PL/SQL artifacts and 100% of `analysis.json`'s top-level arrays, in CI.** The
corpus is small enough (82.9 MB) that sampling buys nothing but doubt.

---

## 3. Load testing — the whole point is scale

Correctness on a 15,824-statement sample says nothing about 1,000,000. This tier is what makes the
scale claims checkable.

### 3.1 The corpus generator (`TEST-L01`)

Synthetic, parameterised, and shaped by the **measured** ratios so it stresses the real thing:

| property | measured value to reproduce |
|---|---|
| child rows per statement | **3.837** (37,290 cd + 6,173 pred + 16,756 joins + 492 ddl / 15,824) |
| node-side children per node | **57.9** |
| call-edge density | parameterised **5.9 – 43.4**/node (the measured range), default at the **worst** |
| most-called node fan-in | up to **2,534** (measured, POSTGRES) |
| statements per node | ~44 |
| distinct/total ratios | 570 objects / 15,824 uses; 4,810 distinct SQL texts / 15,417 |

Default target: **8,000 procedures, 1,000,000 DB operations**. The generator must also produce a
**second run with a configurable change rate** so `DB-103`'s dedup can be measured at c = 0,
c = 0.05 and c = 1.

### 3.2 What the load tests assert

| test | asserts | budget |
|---|---|---|
| `TEST-L02` ingest throughput | 1M-operation corpus ingests; record wall time, docs/s, peak heap | complete without OOM; heap bounded by batch size, **not** corpus size |
| `TEST-L03` index size | measured `db.stats()` index bytes vs the §B.5 estimate | PL/SQL ~0.6 GB/run *(extrapolated)* — the test **records** and flags deviation > 2× |
| `TEST-L04` query latency | hot path `table:<name>` at p95 | page 1 **and** page 1,000 < 100 ms, and **page 1,000 is not slower than page 1** — this is what proves keyset paging rather than `skip` |
| `TEST-L05` call-edge worst case | 347,200 edges (43.4/node × 8,000) ingest and traverse | no timeout |
| `TEST-L06` Java realistic ratio | **blocked** on §E.4's real-codebase measurement | explicitly deferred, not forgotten |

**`TEST-L04`'s second assertion is the important one.** A `skip`-based pager passes "page 1 is fast"
and fails at page 1,000; comparing the two catches the regression that a single-page test cannot.

### 3.3 Traversal safety

`TraversalTerminatesTest` (`ING-114`): BFS from endpoint 001 with a visited set **terminates**. The
measured logical expansion is **20,300,167,285** nodes and 48 nodes are flagged recursive, so a
traversal without a visited set does not fail — it hangs, which no ordinary test detects. This test
must have a hard timeout so a hang is a failure rather than a stuck build.

### 3.4 Where load tests run

Not on every commit — they need a generated corpus and minutes of runtime. **Nightly, plus before
any release**, with results recorded over time so a slow regression is visible as a trend rather
than discovered at a threshold.

---

## 4. What is deliberately not tested

- **The MongoDB driver and MongoDB's own index semantics** — 4.11.2 is a dependency. The one
  exception is `PrefixDeleteTest`, which depends on collation ordering *we* rely on (§B.5's range
  delete) and so must be verified.
- **Third-party analysis output correctness** — this module persists what the analyser produced; it
  does not judge it. The one exception is `ANA-001`'s determinism test, because dedup depends on it.
- **UI pixel rendering** — the gates assert *what is loaded*, not how it looks.

---

## 5. Coverage expectations

Not a percentage target — a shape:

- **Every mapper** has a unit test with a real artifact.
- **Every collection** has a count assertion and appears in the round-trip proof.
- **Every constraint in `MONGO §5.11`** has a test proving it rejects, and that test must fail if
  the constraint is removed.
- **Every gate** has a planted-violation test.
- **Every task** in the registry has at least one test named in its row.

A line-coverage number would be satisfied by exercising getters. The list above is what actually
catches tomorrow's mistake.
