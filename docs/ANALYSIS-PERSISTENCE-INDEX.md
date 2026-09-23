# Analysis Persistence — document index

Five planning documents, written in this order, each building on the last. **Read them in
dependency order; where they conflict, the later one wins and says so explicitly.**

| # | document | what it decides | status |
|---|---|---|---|
| 1 | [`RELATIONAL-MODEL-PLAN.md`](RELATIONAL-MODEL-PLAN.md) | the **data model** — every document kind on disk, entities, keys, constraints, and the MongoDB mapping. Includes the field-level completeness proof (1,123 paths) | complete |
| 2 | [`MONGO-MODULE-PLAN.md`](MONGO-MODULE-PLAN.md) | the **ingest module** — identity scheme, every RDBMS guarantee and its replacement, every cardinality, ingest pipeline, verification | complete |
| 3 | [`SCALE-ARCHITECTURE-PLAN.md`](SCALE-ARCHITECTURE-PLAN.md) | **scale** — where each layer breaks at 63× (PL/SQL) and ~1,025× (Java), and the DB-first redesign | complete |
| 4 | [`TASK-REGISTRY.md`](TASK-REGISTRY.md) | **the work** — atomic tasks with IDs, dependencies, and executable definitions of done | complete |
| 5 | [`CODING-STANDARDS.md`](CODING-STANDARDS.md) · [`TEST-STRATEGY.md`](TEST-STRATEGY.md) | **how to build it** — layering, SOLID, patterns; test tiers, gates, load tests | complete |

Prior art these build on, not replace: **`docs/plan/PLAN-00`…`PLAN-13`** (14 documents, 11,208
lines) — a 65-entity model, a 71-table schema, and the 16 MB/transaction decisions.
`MONGO-MODULE-PLAN.md` §6 reconciles with them section by section.

---

## Where each document supersedes an earlier one

| decision | first said | now |
|---|---|---|
| embed statement child arrays | RELATIONAL §13.2 | **split** — SCALE §B.2 (no-arrays rule) |
| embed `dag_node.childRefs[]` | RELATIONAL §13.7 | **split** with a covering index — SCALE §B.2 |
| run-scoped `_id` | MONGO §3 | **content-addressed** three-level identity — SCALE §B.1 |
| GridFS for large resources | RELATIONAL §17.6 | **`ArtifactStore`** — MONGO §6.4 (PLAN-06 rejects GridFS) |
| two-document transaction for cutover | RELATIONAL §13.5 | **ordered single-doc updates** — MONGO §5.8 (standalone mongod) |
| enum `operation`/`accessKind` | RELATIONAL §5.1 | **open vocabularies** — MONGO §5.6 (PLAN-04 Trap 2) |
| sequences unnecessary | RELATIONAL §13 | **needed once**, for run version — MONGO §5.9 |
| truncate `constantValue` | SCALE-era §17.1 | **store whole** in a child doc — RELATIONAL §17.4 |
| sizing from the sample corpus | all of 1-2 | **rejected** — SCALE targets 63× / ~1,025× |

---

## The findings that most changed the design

Each was measured, not assumed, and each reversed a decision:

1. **`tables.json` is not redundant** — it holds 132 per-column usage rows found in no other
   artifact. Three manual reviews missed it; a mechanical field-path diff caught it.
   (RELATIONAL §17.4)
2. **`(nodeId, operation, line)` is not unique** — 63/81/64 collisions measured in
   ORACLE/POSTGRES/DB2. The live UI joins on it (`access-details.js:345`), so it silently attributes
   data to the wrong row. The five-part `_id` makes this unrepresentable. (RELATIONAL §5.3)
3. **A MongoDB layer already exists**, and one of its live collections is already called
   `analysis_run`. (MONGO §2.3)
4. **The analyser is non-deterministic** — identical sources produced 51 / 11 / 51 joins for one
   table across three runs. This is a correctness bug *and* it defeats the dedup lever.
   (SCALE §A.2)
5. **Cross-run dedup is worth ~90%** — 3 of 5 dialects re-run byte-identically across 4 archived
   runs. (SCALE §A.1)
6. **Indexes, not documents, are the wall** — 65.4 GB of index for one Java run; 654 GB at 10 runs
   without dedup. (SCALE §B.5)
7. **The analyser breaks before MongoDB does** — `CallGraphIndex.ResolutionContext` holds 22
   unbounded maps that must be complete before any tree is walked. No storage change fixes it.
   (SCALE §A.3.1, §E.1)
8. **The extrapolation basis is unrepresentative** — the measured jar is 78% generated ANTLR
   classes and 85% generated methods, so the Java ratios may be wrong by 2×. (SCALE §A0.3)

---

## Open decisions blocking work

Listed in full in [`TASK-REGISTRY.md`](TASK-REGISTRY.md) §10. The two that matter most:

- **O-1** — is the Java requirement "index 100M lines" or "answer questions about a 100M-line
  codebase"? The first is not achievable in this architecture; the second is. This blocks the whole
  JAR scale effort.
- **O-6** — measure one real 5-10M-line codebase before sizing a cluster. Costs a day, requires no
  code, and is the highest-value action available. (SCALE §E.4)

---

## Suggested reading order by role

- **Deciding whether to build this:** this index, then SCALE §A0, §B.8, §E.
- **Building it:** TASK-REGISTRY, then CODING-STANDARDS, then MONGO §3, §5, §7.
- **Reviewing the data model:** RELATIONAL §17, §18 (five ER diagrams).
- **Writing tests:** TEST-STRATEGY, then the Tests column of every registry row.
