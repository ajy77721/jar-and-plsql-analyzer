# Scale-First Architecture Plan — DB, then Middle Layer, then UI

> **Collection naming, decided 2026-09-07.** The names in this document are the ones `Splitter`
> actually produces: `am_parent__field`, derived mechanically from the parent collection and the
> field being split out. They previously read `am_column_usage`, `am_db_column` and so on — names
> this plan invented and the ingest module never used, so a middle-layer query written from the
> plan would have returned zero rows against a fully populated database. That is the
> `am_txn_statement` failure mode: every integrity check passes and the answer is empty.
>
> The plan changed rather than the code because a derived name cannot drift from the data. A
> hand-written name per collection needs an override map in `Splitter`, maintained for every new
> split field forever — which is the class of gap that produced this discrepancy in the first
> place. See TASK-REGISTRY §17.

**Plan only. No code, no schema, no DDL.**

Companion documents: [`RELATIONAL-MODEL-PLAN.md`](RELATIONAL-MODEL-PLAN.md) (data model),
[`MONGO-MODULE-PLAN.md`](MONGO-MODULE-PLAN.md) (ingest module), `docs/plan/PLAN-00…13`.
**Where those conflict with this document, this one wins on sizing** — they were written against
the sample corpus, which the user has correctly rejected as a basis.

## The rejection, and what it means

> "ui and backend, db nothing is able to handle such load, saving, ui render — anything is not
> ready to handle. so make sure plan such way it handles everything. first fix and make db best
> way so we can try to fix ui and best on midlayer"

Everything measured so far describes a **sample**: 5 PL/SQL analyses (15,824 statements) and one
jar (97,607 lines of Java). The real targets are **63×** and **~1,025×** those. Order of work is
the user's: **database first**, then middle layer, then UI.

## Targets

| | measured sample | target | multiplier |
|---|---|---|---|
| PL/SQL procedures | 356 nodes | **8,000** | 22× |
| PL/SQL DB operations | 15,824 statements | **1,000,000** | **63×** |
| Java | 97,607 lines | **100,000,000** | **~1,025×** |

## Contents

- [x] **A0. Corrections to the supplied sizing basis** — three figures re-measured, one changes the
      Java estimate materially
- [x] **A. Where each layer breaks — measured** · A.1 cross-run duplication (75% dedup, measured)
      · **A.2 the analyser is non-deterministic** · A.3 backend (the hard ceiling) · A.4 UI
- [x] **B. The database at target scale** — the priority section · B.1 three-level identity
      · B.2 no arrays · B.3 cross-run dedup · B.4 sharding · **B.5 index budget** · B.6 query
      patterns and paging · B.7 engine settings · B.8 verdict
- [x] C. Middle layer
- [x] D. UI
- [x] E. What not to do, and the honest limits

**Companion documents** (this plan's decisions become work there):

| document | purpose |
|---|---|
| [`TASK-REGISTRY.md`](TASK-REGISTRY.md) | every atomic task, with IDs, dependencies, and a command as the definition of done |
| [`CODING-STANDARDS.md`](CODING-STANDARDS.md) | layering, DTO/entity/document separation, SOLID applied to this code, patterns to use and to avoid |
| [`TEST-STRATEGY.md`](TEST-STRATEGY.md) | tiers, invariant gates, the lossless round-trip proof, and the load test at target scale |

## Figures inherited for checking

Extrapolations supplied to me, to be verified in §A/§B rather than assumed:

- 3.84 child rows per statement (measured: 37,290 columnDetail + 6,173 predicates + 16,756 joins
  + 492 ddl over 15,824 statements)
- PL/SQL at target: 4,836,641 statement-side docs · 469,438 node-side · 231,228 call edges →
  **~5.5M docs / ~2 GB per run**
- Java at target: 1,673,036 classes · 34,228,078 methods · 8,861,045 fields →
  **~45M docs / ~18 GB per run**
- 10 runs: 55M docs / 20 GB (PL/SQL), 447M docs / 180 GB (all)
- **Flagged for re-measurement:** the invocation count returned 0 in the supplied run.

---

## A0. Corrections to the supplied sizing basis

I was asked to flag figures that look wrong. Three do, and one of them changes the shape of the
Java estimate materially.

### A0.1 The invocation count is not zero — the original figures stand

Re-measured against `data/jar/unified-web-1.0.0-SNAPSHOT/analysis.json`:

- `invocationPool` **is** present at top level: **64,446 entries, all 64,446 distinct**
- invocation refs live on `classes[].methods[].invocationRefs` (confirmed method key set:
  `accessFlags, annotations, descriptor, endLine, fieldAccesses, httpMethod, invocationRefs,
  localVariables, name, parameters, path, returnType, startLine, stringLiterals`)
- **sum of `invocationRefs` lengths = 111,455**

The supplied zero was a lookup in the wrong place. **Use 111,455 refs / 64,446 distinct** — the
figures in `RELATIONAL-MODEL-PLAN.md` §8.2 were right.

At the Java target this is the largest single collection:
**111,455 × 1,025 ≈ 114,241,375 junction documents per run** *(extrapolated)*. That alone exceeds
the entire supplied "~45M docs per run" estimate, which appears not to have included it.

### A0.2 Call-edge density is not 29 per node — it ranges 5.9 to 43.4, and this is the real risk

The supplied 29 edges/node is the POSTGRES figure. Measured across all five live analyses:

| dialect | nodes | edges | **edges/node** | most-called node (times) | statements |
|---|---|---|---|---|---|
| SQLSERVER | 57 | 337 | **5.9** | 37 | 1,729 |
| DB2 | 64 | 428 | **6.7** | 43 | 4,063 |
| MYSQL | 69 | 652 | **9.4** | 132 | 5,015 |
| POSTGRES | 114 | 3,295 | **28.9** | **2,534** | 2,581 |
| ORACLE | 52 | 2,258 | **43.4** | **1,819** | 2,436 |
| **total** | **356** | **6,970** | **19.6** | — | 15,824 |

A **7.4× spread**, and the average (19.6) is not the figure to plan with. At 8,000 nodes:

| basis | edges at target |
|---|---|
| corpus average 19.6 | 156,800 |
| supplied 29 (POSTGRES) | 231,228 |
| **worst observed 43.4 (ORACLE)** | **347,200** |

Worse: the density is **not** correlated with node count in the sample (ORACLE has the fewest nodes
and the highest density), so it is a property of coding style — recursion, package-internal calls,
trigger chains — not of size. **We cannot extrapolate it safely from five samples of 52-114 nodes**,
and a call graph's edge count is bounded above by O(n²), not O(n).

The `maxToNode` column is the concrete warning: one PL/SQL node is a call target **2,534 times** in
a 114-node analysis. A single hot procedure in an 8,000-node system could plausibly be a target
tens of thousands of times.

**Recommendation: plan `am_call_edge` for 500,000 documents per run at target and design it so the
count does not matter** — §B's sharding and the absence of any per-parent array mean an edge
collection that grows superlinearly is a capacity question, not a correctness one. This is the
supplied verdict ("the uncertainty is call-edge density, not volume") confirmed with numbers.

### A0.3 The Java ratios are dominated by generated code — the extrapolation is probably wrong

This is the significant correction. Composition of the measured jar:

| segment | classes | methods |
|---|---|---|
| `com.plsql.parser.*` (**bundled ANTLR-generated parser**) | **1,274** (78%) | **28,558** (85%) |
| everything else | 359 | 4,849 |
| own-jar classes (`sourceJar` absent) | **215** | **2,392** |
| **total** | **1,633** | **33,407** |

So the "20.5 methods per class" ratio driving the 34,228,078-method extrapolation is measured on a
corpus that is **85% generated parser code**. Own-jar code measures **11.1 methods/class** — 1.8×
lower. This is the same ANTLR concentration `docs/plan/PLAN-12` already recorded (*"1,219 of the
1,623 analysed classes are `com.plsql.parser.*`"*), independently reproduced.

The LOC basis is also ambiguous. Measured three ways:

| basis | value |
|---|---|
| sum of all method line-spans | **131,009** |
| sum of `max(endLine)` per class, own-jar only | **55,226** |
| `max(endpoints[].totalLoc)` | **100,739** |
| supplied basis | 97,607 |

`sum(class.lineCount)` is **0** — classes carry no `lineCount` field, so any extrapolation using one
is reading an absent value.

**Consequence.** The 1,025× multiplier is derived from a denominator that is neither clearly defined
nor representative. Depending on basis and composition, the Java target could be anywhere from
**0.5× to 2× the supplied document counts**. The supplied figures are the right order of magnitude
and I will use them, but:

**Recommendation: do not size hardware or shard counts from the 1,025× extrapolation.** Instead,
(a) design so cost is **per-entity and incremental** rather than per-run-total (§B.3 cross-run
dedup, §F incremental analysis), and (b) obtain **one real large codebase** and measure it before
committing to a cluster shape. A ratio measured on a generated parser is not a ratio.

### A0.4 Figures that check out

| supplied | verified |
|---|---|
| 3.84 child rows per statement | **3.837** measured (37,290 + 6,173 + 16,756 + 492 = 60,711 over 15,824) ✓ |
| 4,836,641 statement-side docs at 1M statements | ✓ consistent with 3.837 |
| ~58 node-side children per node | **57.9** measured (20,607 over 356) ✓ |
| `analysis.json` = 49.3 MB, 3,784 DAG nodes, 1,633 classes, 321 endpoints | ✓ all reproduced |

---

## A. Where each layer breaks — measured

### A.1 Cross-run duplication is real, and it is the largest single lever

The repo contains **15 archived re-runs** (`*_2026*` directories) — the same analyses, re-run. That
is a natural experiment for the "every run writes a complete copy" cost, so I measured it rather
than assuming it. Content-hashing every node document and every statement across 4 runs of each
analysis:

| dialect | runs | node docs | distinct | **dedup** | statements | distinct | **dedup** |
|---|---|---|---|---|---|---|---|
| DB2 | 4 | 256 | 64 | **75.0%** | 16,252 | 4,063 | **75.0%** |
| MYSQL | 4 | 276 | 69 | **75.0%** | 20,060 | 5,015 | **75.0%** |
| POSTGRES | 4 | 456 | 114 | **75.0%** | 10,324 | 2,581 | **75.0%** |
| ORACLE | 4 | 208 | 67 | 67.8% | 9,744 | 3,539 | 63.7% |

75.0% is the arithmetic maximum for 4 runs — **three of four dialects re-run byte-identically**.
Content-addressed storage would store one copy instead of four.

Generalising: with R runs and a genuine change rate c, storage is `1 + (R-1)·c` instead of `R`.
At the measured c ≈ 0 and the target of 10 runs, **dedup saves ~90% of everything**. That single
change is worth more than every other optimisation in this document combined, and §B.3 builds on it.

### A.2 The analyser is non-deterministic — this is a correctness bug *and* it defeats dedup

ORACLE did not dedup cleanly, so I looked at why. It is not ordering, and it is not real change.

**The sources are byte-identical across runs** — aggregate SHA-256 over all 50 files in
`sources/` is `b46531ff786f` for run0, run1 and run2 alike. Yet for one node,
`api/nodes/ANALYZER$ERP_USP_COLLECT_DEMAND.json`, the joins attributed to table `ERP_PRODUCT` are:

| run | joins on `ERP_PRODUCT` | first joined table | lines |
|---|---|---|---|
| run0 (live) | **51** | `ERP_PURCHASE_ORDER_LINE` | 35, 57, 157, 239, 254, 257… |
| run1 (`_20260905_024149`) | **11** | `ERP_BOM_COMPONENT` | 239, 254, 257, 572, 819, 834… |
| run2 (`_20260905_025907`) | **51** | `ERP_PURCHASE_ORDER_LINE` | 35, 57, 157, 239, 254, 257… |

Identical input, different output, intermittently. 15 of 52 node documents differ between run0 and
run1, and **the only fields that differ are `tables` and `directTables`** — specifically the nested
`joins` arrays. 1,103 of 2,436 statement rows differ likewise while the *count* stays at 2,436.

Two consequences, and the second is the one this document cares about:

1. **A user sees different join data depending on which run they open** — 51 joins or 11, for the
   same procedure and the same table. That is a live correctness defect, independent of scale, and
   it should be raised separately.
2. **It silently defeats content-addressed dedup.** §A.1's 90% saving assumes unchanged code
   produces identical content. A non-deterministic analyser makes unchanged code look changed, so
   dedup degrades toward zero exactly when it matters most.

The signature — join attribution varying while counts hold — points at iteration over a
non-ordered collection (`HashMap`/`HashSet`) or a shared mutable accumulator being attributed to
whichever table is processed first. I have **not** located the defect in source; that is a
follow-up, not a claim.

**Recommendation: fixing analyser determinism is a prerequisite for the dedup lever, and therefore
belongs in phase 1 of §E, not in a backlog.** It is also cheap to verify: run the same analysis
twice and diff the output. The fact that the archives make this a one-command check, and it has not
been run, is itself worth noting.



### A.3 Backend — the analyser hits a hard ceiling long before MongoDB is involved

The coordinator's framing was that `analysis.json` being read whole is the first failure. That is
right in outcome but wrong in location, and the correction matters for §E's ordering: **the write
path is already streamed; it is the in-memory index and the read path that break.**

**A.3.1 — The hard ceiling: `CallGraphIndex.ResolutionContext`.**
`jar-analyzer-core/src/main/java/com/jaranalyzer/service/CallGraphIndex.java:498-574` holds the
whole-JAR bytecode index in **22 unbounded collections simultaneously**:
```java
static class ResolutionContext {
    final Map<String, IndexedClass> classMap = new HashMap<>();
    final Map<String, List<String>> interfaceImplMap = new HashMap<>();
    final Map<String, String> superClassMap = new HashMap<>();
    final List<IndexedEndpoint> endpoints = new ArrayList<>();
```
plus `entityCollectionMap`, `repoCollectionMap`, `collectionFieldMappings` (`:516`),
`enumConstantsMap` (`:537`), `fieldConstantsMap` (`:544`) and a dozen more. Each `IndexedClass`
(`:29-127`) holds its own `methods` map, `allStringConstants` (`:103`) and `injectedFields` (`:127`);
each `IndexedMethod` (`:384-389`) holds `invocations`, `stringLiterals`, `staticFieldRefOwners`.

**There is no eviction and no spill, and `classMap` must be complete before any tree can be walked.**
This is the true ceiling: **100M lines of bytecode index cannot fit in a JVM heap at any setting.**
No database change helps. §F returns to this.

**A.3.2 — The write path is already streamed, and already has a 69 GB scar.**
`PersistenceService.java:178-200` streams `analysis.json` and spills the node dictionary to
`_calltree-nodes.spill.jsonl` (`:199`). Its comment (`:190-196`) records the exact failure mode:
> *"serialising it nested re-emits each shared subtree once per path and the file grows with the
> number of **PATHS** rather than the number of **METHODS**. That is what produced a **69 GB
> analysis.json** for unified-web that never finished writing."*

So the streaming write already exists and works. **But the POJO save path does not:**
`writeFlattened` (`:738-786`) accumulates `dict` (every distinct call-tree node as `JsonNode`) plus
`savedTrees`, `savedFragments` and `sidecars` — roughly **3× the document in heap**. That is the path
the Claude correction pipeline uses.

**A.3.3 — The read path is the coordinator's 50 GB problem, and it is worse than stated.**
`PersistenceService.java:662-671`:
```java
public JarAnalysis load(String jarName) throws IOException {
    ...
    return rehydrate(objectMapper.readValue(in, JarAnalysis.class));
```
Full document into a POJO graph — and then `rehydrate()` (`:693-731`) **re-expands the DAG into
nested trees** (`:706-709` `inflater.toTree(...)` per endpoint). So the read path reintroduces
precisely the path-proportional explosion the write path was rewritten to avoid.
`JarAnalysis.java` is a flat POJO with `classes` (`:71`), `endpoints` (`:72`), `callTreeNodes`
(`:41`), `invocationPool` (`:50`) all eagerly held.

Eight callers do a full `load()`, including `ChatboxService.java:33` and seven sites in
`ClaudeJobExecutor`.

**A.3.4 — The 300-million-node measurement, already in the code.**
`AnalyzerController.java:505-509`:
> *"Measured on endpoint 271 of this very jar: the expanded tree is **300,167,238** nodes — it ran
> past 3.5 minutes and produced 120 MB with a 200,000-node cap, which is **0.07% of the answer**."*

and `CallTreeBuilder.java:1425-1429`:
> *"com.plsql.parser.PlSqlParser alone has 1,245 methods and 47,585 invocations, and walking the
> grammar uncapped took analysis.json to 69 GB."*

The only mitigation is an opt-in exclusion list that is **empty by default**
(`CallTreeBuilder.java:1364`). This independently confirms §A0.3: the ANTLR parser is both the
size driver and the reason the extrapolation ratios are unrepresentative.

**A.3.5 — PL/SQL endpoints are the least protected.** Every one of these returns a whole document
with **no paging** (`ParserAnalysisController.java`): `/index` (`:232`), `/call-graph` (`:277`),
`/tables` (`:252`), `/procedures` (`:299`, returns the entire node array),
`/resolver/{type}` (`:495`). And they are cached whole:
`ParserAnalysisReader.java:81` keeps `MAX_CACHE_ENTRIES = 30` **fully parsed documents** resident,
with no byte budget.

Several readers do an N-file full scan per request — `ParserAnalysisReader.java:158` (`getJoins`)
reads **every** node file and accumulates into a `LinkedHashMap`; same shape at `:238`, `:319`,
`TableDetailFacetReader.java:76`, `SequenceFacetReader.java:45`, `NodeScanFacetReader.java:44`.
At 8,000 procedures that is 8,000 file reads and a full accumulation on a single HTTP request.

`PlsqlSearchController.java:171-181` reads the whole statements index and memoises it, admitting at
`:178` it *"holds 3.6 MB of parsed JSON for the largest analysis measured here"* — at the 63× target
that is ~227 MB per analysis, per cache entry, up to 30 entries.

**A.3.6 — `ChunkedFlowWriter` un-chunks itself.** `ChunkedFlowWriter.java:165-176` carefully writes
per-node chunks and then reads them **all** back into `List<FlowNode> allNodes` and
`List<FlowEdge> allEdges` for the merge; `:485` builds the entire statements index as one in-memory
`List<Map<String,Object>>`. The chunking is undone at exactly the point it would have paid off.
`:56` also enables `INDENT_OUTPUT` — pretty-printing every artifact.

**A.3.7 — PL/SQL holds every source line in memory.**
`plsql-analyzer-core/.../model/AnalysisResult.java:20`:
```java
private Map<String, List<String>> sourceMap = new HashMap<>(); // OWNER.NAME -> source lines
```
Every line of every analysed unit as a `String`. At the PL/SQL target this is tens of GB of `String`
overhead on its own.

**A.3.8 — What already exists and should be built on, not rebuilt.** This is substantial and the
plan should not ignore it:
- **Cursor paging**: `analyzer-storage/.../page/PageRequest.java` with `MAX_LIMIT = 1000` (`:21`),
  strict/lenient modes (`:64`, `:80`), and an n+1 has-next probe (`:99` `fetchSize()`).
  `Cursor` is opaque and **keyed on sort-field values, not array positions** — `AnalyzerController.java:1000-1003`
  notes *"a cursor survives the rebuild anyway because it is keyed on the class's package and name
  rather than on an array position."* That is exactly the property §C needs.
- **`FacetPageResponder`** (`unified-web/.../page/FacetPageResponder.java:255-263`) streams a `page`
  envelope from a closeable row `Stream` and never materialises the collection.
- **Already-paged endpoints**: `/summary/endpoints` (`AnalyzerController.java:752-830`, default 25),
  `/classes/tree` (`:1009-1059`, plus a `?members=false` projection dropping *"93.8% of this
  response"*), `/bundled-jars` (`:1752-1781`).
- **Streaming primitives**: `JarParserService.streamClasses` (`:518-530`), `InvocationPool` JSONL
  (`:98, 163, 250`), `JsonArrayRows.java:101` (per-row over a JSON array),
  `MongoAnalysisRepository.java:701` (cursor-based per-row).
- **The repository seam**: `com.analyzer.storage.repo.AnalysisQuery` / `ArtifactScanRepository` with
  `Dataset.JAR_CLASS_TREE` already exists — the place to swap file scans for indexed queries.

### A.4 UI — no virtualisation exists anywhere, and paging is client-side

Searched all of `unified-web/src/main/resources/static/`. **There is no windowing, no
`IntersectionObserver`, no `DocumentFragment` batching, no infinite scroll — anywhere.** The only
mitigations are fixed caps and "show more" buttons.

**The pager is client-side at ~34 of 36 call sites.** `parser/js/tableFramework.js` (`PA.tf`) is
dual-mode; the deciding line is `:42`:
```js
const src = (window.PA && PA.isPageSource && PA.isPageSource(data)) ? data : null;
const rows = src ? [] : (data || []);          // :43  an array means the WHOLE dataset is retained
filtered: src ? [] : [...rows],                // :47  and then fully copied
```
Server-paging plumbing exists (`parser/js/pagination.js:54-107`) but is wired at only **2 of 24**
`PA.tf` sites (`parser/js/cursors.js:142`, `parser/js/tableDetail.js:1055`) and **0 of 12** JAR
`_initPage` sites. Default page size `:49` is 50; the JAR copy uses 25.

**The 1,000,000-operation array is built in the browser.** `parser/js/api.js:111-121` fetches the
whole tables index and flattens every `usedBy × operation × line` into one flat array:
```js
accessDetails: (t.usedBy || []).flatMap(u => Object.entries(u.operations || {})
    .flatMap(([op, lines]) => (lines || []).map(line => ({...}))))
```
`shared/access-details.js:480` then hands that entire array to `PA.tf.init`. **This is exactly the
user's 1M DB-operation target, materialised client-side as JavaScript objects.** At 63× it is
~1M objects in a browser tab before a single row is drawn.

**Whole-dataset fetches with no limit** (`parser/js/api.js`): `/index` (every node), `/tables`,
`/joins`, `/cursors`, `/sequences`, `/dynamic-sql`, `/call-graph`, `/call-tree/{nodeId}`. On the JAR
side (`jar/js/api.js`) the docstrings measure their own problem: `:138` says `/classes/tree` is
**"11.8 MB with members and ~760 KB without"**; `:50` measures `/summary/headers` at **130,420 B**;
`:63` notes *"Absent `limit`/`after` returns the whole array, as before."*

**Paging that is drained in a loop** — `jar/js/codeTree.js:97-101`:
```js
for (let page = 0; page < 500; page++) {
    const env = await source.fetchPage(this._pageSize);
    for (const cls of env.items) classes.push(cls);
```
200/page × 500 pages accumulates up to 100,000 classes into one array. **The paging exists and is
then defeated**, falling back to the 11.8 MB unpaged endpoint at `:92`. At the Java target
(1,673,036 classes) this loop caps at 100,000 and silently truncates.

**Call-tree rendering is the worst case, and it has the DAG shape the coordinator identified.**
`parser/js/callGraph.js`:
- `:207` `_indexNodes(data, null)` — a **full recursive walk of the entire tree** stamping every
  node into `_nodeMap`, before anything renders
- `:209-210` `html += renderNode(data, 0, null); container.innerHTML = html;`
- Lazy expansion **does** exist (`_initialDepth: 1` at `:143`, `_renderChildrenLazy` at `:347-359`)
- …and is **defeated by two paths**: `expandAll()` `:399-408` loops `for (let pass = 0; pass < 200; pass++)`
  materialising every remaining child; and `search(query)` `:458` **calls `expandAll()` first**, so
  typing in the search box renders the whole tree. The only guard is a `window.confirm` above
  `_EXPAND_ALL_WARN: 400` (`:149`), sized against *"the 1,250-node trace measured here"*.

`jar/js/summary-trace.js:95` renders the whole JAR call tree into one HTML string with only a
`depth > 25` guard (`:102`) — no node-count cap — and its own comment at `:43` admits the tree
*"can reach 120 MB"*.

**Client-side sort/filter over the full dataset**, on every keystroke:
- `parser/js/tableFramework.js:238` stringifies **every property of every row** to filter; `:274`
  copies the array and `localeCompare`s per comparison
- `parser/js/tableOps.js:41` allocates `new Set((t.accessDetails || []).map(...))` **per sort
  comparison** — with 1M access details this is O(n log n) set allocations
- `jar/js/summary.js:217-240` runs `_analyzeEndpoint` over every endpoint in batches of 30,
  building whole-JAR reports in the browser

**The honest note already in the code** — `parser/js/leftPanel.js:29-31`:
> *"the largest analysis has 114 procedures and 85 tables… it is not evidence about the
> thousand-procedure case, which no analysis here exercises."*

That is precisely the case the user is now asking for.

---

## B. The database at target scale

### B.1 Identity — three different things, currently conflated

The user's instruction is *"ids and relation we have to manage in best unique way"*. At scale that
requires separating three notions the current model merges into one:

| level | question it answers | lifetime | example |
|---|---|---|---|
| **entity identity** | "which procedure/class is this, across all time?" | forever, across runs | `ORACLE:ANALYZER.ERP_USP_RUN_CYCLE` |
| **content identity** | "is this the same *version* of it?" | changes when code changes | `sha256(canonical content)` |
| **occurrence identity** | "which *appearance* of it is this?" | within one run | node X reached via path P at depth D |

`MONGO-MODULE-PLAN.md` §3 gave every document a single run-scoped `_id`, which conflates all three.
That is correct at sample scale and **wrong at target scale**, because it makes cross-run dedup
(§B.3, worth ~90%) impossible: the same unchanged procedure gets a different `_id` in every run.

**The three-key scheme.** Every entity collection carries:

```
_id        surrogate   content-addressed:  "{entityKey}@{contentHash12}"
entityKey  business     run-independent:    "ORACLE:ANALYZER.ERP_USP_RUN_CYCLE"
contentHash            sha256(canonical document minus run-scoped fields)[:12]
```
and a **separate membership collection** records which runs contain which entity versions:

```
am_run_member { _id: "{runId}~{entityKey}", runId, entityKey, entityRef, ordinal? }
```

`_id` is content-addressed, so an unchanged procedure re-ingested in run 7 **upserts onto the same
document** and costs nothing but the membership row. This is the generalisation of the call-tree
DAG's existing technique (§B.3).

**Occurrence identity is deliberately not stored.** The same node recurs many times within a run —
measured: one POSTGRES node is a call target **2,534 times** (§A0.2). Materialising occurrences is
the 20,300,167,285-node expansion `RELATIONAL-MODEL-PLAN.md` §8.3 measured and rejected. Occurrence
is a *path*, computed at read time by traversing `am_call_edge`, never a stored row.

**Which level each collection uses** — this must be explicit per collection or the model drifts:

| collection | identity level | why |
|---|---|---|
| `am_java_class`, `am_java_method`, `am_java_field` | **content** | overwhelmingly unchanged between runs; the dedup prize |
| `am_proc_node`, `am_db_object`, `am_sql_text` | **content** | same |
| `am_invocation`, `am_dag_node` | **content** | already content-addressed in the source data |
| `am_statement` and its children | **content**, keyed on the owning node's content | a statement belongs to a procedure version |
| `am_call_edge` | **content** (`fromEntity`+`toEntity`+line) | edges are a property of the caller's version |
| `am_run` | **run** | it *is* the run |
| `am_run_member` | **run × entity** | the join that makes dedup work |
| `am_subtree_table`, `am_endpoint_reachable` | **run** | genuinely run-derived rollups |

**The trade, stated.** Content-addressing costs a hop: "give me the statements of node N in run R"
becomes membership lookup then statement query, instead of one query. That is one extra indexed
seek on a collection with 8,000 rows per run. **Recommendation: accept it.** The alternative is
storing 10× the data to save one seek, and §B.5 shows the index budget cannot afford it.

**Prerequisite, flagged:** content-addressing is only sound if the analyser is deterministic. §A.2
shows it currently is not. That dependency is why §E puts determinism in phase 1.

### B.2 No arrays — the rule, and where it bites

The user's rule is absolute: **every array becomes its own collection with `parentRef` + `ordinal`.**
This reverses `RELATIONAL-MODEL-PLAN.md` §13.2/§17.6, which embedded `columnDetail`, `predicates`,
`joins`, `ddl` in `am_statement` and `childRefs[]` in `am_dag_node`.

At sample scale embedding was right (2.4 children per statement, measured). At target scale the
rule is right for reasons the sample could not show:

1. **Unbounded growth is unbounded.** `ERP_PRODUCT.joins` measured **51** entries for one table in
   one node (§A.2). Nothing bounds it. An embedded array whose length is data is a document whose
   size is data.
2. **Update amplification.** Rewriting a 16 MB document to change one join is 16 MB of I/O and
   oplog. With arrays split out it is one small document.
3. **Independent queryability.** "every join on `ERP_PRODUCT` across the estate" is a collection
   scan of an array-of-arrays if embedded; an indexed lookup if not.
4. **Projection.** §C's rule that no endpoint returns a whole document is unenforceable when the
   payload *is* an array inside the document.

**Order is load-bearing and must survive.** `ordinal` is mandatory, not decorative:

| array | why order matters |
|---|---|
| `columnDetail[].into` | **INTO targets are positional** — `SELECT a,b INTO x,y` binds by position. Losing order corrupts meaning |
| `dag_node.childRefs[]` | call order |
| `method.parameters[]` | JVM signature order |
| `dynamicSql.usingVariables[]` | bind position fills `?` in order |
| `cycle.path[]` | the cycle is a sequence |

`ordinal` is `int32`, dense from 0, and is part of the business key
(`{parentRef}#{ordinal}`) — but **never part of `_id` alone**, per `PLAN-04 §1.3`'s existing rule
against keying on ordinals.

**The one exception I recommend keeping, and why.** `am_dag_node.childRefs[]` is the call-tree DAG:
7,281 edges over 3,784 nodes, mean 46 bytes, max 3,384 bytes *(measured)*. Splitting it to
`am_dag_edge` is 7,281 documents instead of an array — cheap — **but** the array is the thing that
makes the 2.03 × 10¹⁰ expansion tractable, because traversal reads the parent and gets its children
for free with zero queries. Splitting turns each traversal step into an indexed lookup.

**Recommendation: split it anyway, and add a covering index.** `{parentRef:1, ordinal:1}` with
`childRef` included makes a step one index-only read — no document fetch. That preserves the
traversal cost while honouring the rule, and it makes reverse traversal ("who calls this?") a
first-class indexed query rather than a multikey scan. The user's rule is right here and my earlier
embedding was the sample-scale answer.

**Consequence for document counts.** Splitting the four statement children was already counted
(3.837/statement). Splitting the remaining arrays adds, at PL/SQL target *(extrapolated from
measured per-node counts)*: `am_node_table_access` ~97,000, `am_subtree_table__used_by_nodes` ~257,000,
`am_cursor_object` ~32,000, `am_java_method__parameters` ~22M on the Java side. **The no-array rule roughly
doubles the Java document count** and is the main reason §B.5's index budget is what it is.

### B.3 Cross-run dedup — the highest-value change in this document

**Measured basis (§A.1):** across 4 real re-runs, three of five dialects produced **byte-identical**
output — 75.0% dedup, the arithmetic maximum for 4 runs. The prize is not speculative.

**The technique already exists in this codebase.** `analysis.json.callTreeNodes` is a
content-addressed map keyed by a 20-hex hash, and `childIds[]` references those hashes. It stores
3,784 physical nodes for 20,300,167,285 logical ones *(measured)*. §B.3 generalises that from the
call tree to every entity.

**Mechanism.** `_id = "{entityKey}@{contentHash12}"` (§B.1). Ingest computes the hash from the
canonical document with run-scoped fields removed, then `updateOne(upsert)`. An unchanged entity
resolves to an existing `_id` and the write is a no-op; only the `am_run_member` row is new.

**What is excluded from the hash** — get this wrong and dedup silently fails:
- `runId`, `analyzedAt`, `crawlTimeMs`, `totalDurationMs`, and any counter
- ordering that is not semantically meaningful (arrays sorted canonically before hashing, except
  the order-bearing ones listed in §B.2)
- **anything non-deterministic** — which today includes `tables[].joins` (§A.2)

**Quantified saving** at the Java target, using the measured document counts and a change rate `c`
(fraction of entities changed between runs):

| runs | c = 0 (measured for 3 of 5 dialects) | c = 0.05 (realistic) | c = 1 (today) |
|---|---|---|---|
| 1 | 1.0× | 1.0× | 1.0× |
| 5 | 1.0× | 1.2× | 5.0× |
| **10** | **1.0×** | **1.45×** | **10.0×** |

Against §B.5's 65.4 GB of index per run: 10 runs cost **~654 GB** without dedup, **~95 GB** at
c = 0.05, **~65 GB** at c = 0. **Dedup is the difference between a cluster and a server.**

**Retention becomes cheap too.** Dropping run 1 today means deleting 45M documents. With dedup it
means deleting 45M `am_run_member` rows and then garbage-collecting entities no membership row
references — and the GC can be lazy, off-peak, or never (an unreferenced entity costs storage but
breaks nothing).

**The two costs, stated.**
1. **A hop on every read** (§B.1). Mitigated by membership being small and indexed.
2. **Determinism becomes load-bearing.** Today's non-determinism (§A.2) would make `c` ≈ 0.3
   spuriously — 15 of 52 node docs differed between two identical-input runs. **Dedup without a
   determinism fix delivers a fraction of the prize and hides the bug.**

**Recommendation: implement dedup and determinism together as one increment**, with the acceptance
test being "ingest the same analysis twice; the second ingest writes zero entity documents." That
test fails today, and it is the single clearest measure of whether this lever works.

### B.4 Sharding

**Standalone mongod is not a viable target shape** — 447M docs / 180 GB unsharded and unreplicated,
with no failover. But sharding is a *later* increment than it appears, and the reason matters:
**with §B.3 dedup and §F's incremental analysis, one replica set may well suffice.** Sharding a
dataset you have not yet deduplicated is optimising the wrong number.

**Recommendation: replica set first (3 nodes), shard only when a measured collection exceeds ~2 TB
or write throughput saturates.** A replica set also unlocks multi-document transactions, which
`MONGO-MODULE-PLAN.md` §5.8 had to design around.

When sharding does come, the key per collection:

| collection | shard key | reasoning |
|---|---|---|
| `am_java_method` | **`{classRef: hashed}`** | co-locates a class's methods on one shard, so "methods of class X" is single-shard; hashed avoids the monotonic-prefix hotspot |
| `am_method_invocation` | **`{methodRef: hashed}`** | the 114M-doc collection; every query is "invocations of method X", so single-shard |
| `am_invocation` | `{_id: hashed}` | content-addressed, accessed by `_id`, no locality to preserve |
| `am_statement` | **`{nodeRef: hashed}`** | "statements of procedure X" single-shard; the table-search query (§B.6) is scatter-gather by design |
| `am_statement__columns`, `am_statement__predicates`, `am_statement__joins` | `{parentRef: hashed}` | follow the parent |
| `am_call_edge` | `{fromRef: hashed}` | forward traversal single-shard; reverse is scatter-gather |
| `am_dag_edge` | `{parentRef: hashed}` | same |
| `am_run_member` | `{runId: 1, entityKey: 1}` | ranged, because "all members of run R" must be one contiguous scan |

**The obvious-but-wrong choices, and what goes wrong:**

- **`{runId: hashed}` on everything.** Tempting — every query is run-scoped. It is wrong because
  runs are added monotonically and one run is one shard: a single ingest writes 45M documents to
  one shard while the others idle, and the newest run (the one everyone queries) is a hotspot.
- **`{_id: 1}` ranged, on content-addressed collections.** `_id` begins with `entityKey`, which for
  Java is a package-sorted FQN. `com.foo.*` all land together, so one large package is one shard.
  Hashed fixes it; ranged does not.
- **`{tableName: 1}` on `am_statement`.** Serves the hot query beautifully and fails on skew:
  measured, one table is referenced by a large share of statements, and 570 distinct tables over
  15,824 statements means low cardinality. Low-cardinality shard keys produce jumbo chunks that
  cannot split.
- **Anything monotonically increasing** (timestamp, ordinal, `_id` with a run prefix) — the classic
  hotspot; all writes go to the highest chunk.

**Zone sharding is worth considering for PL/SQL vs Java.** They have different sizes, query shapes
and retention needs; the PL/SQL side is ~0.5 GB of index against Java's 65 GB (§B.5). Pinning
PL/SQL collections to a small zone keeps them off the shards doing Java work. **Judgement call:
defer until there is a measured reason.**

### B.5 Index budget — this, not document count, is the wall

Documents are cheap; indexes must be in RAM to be fast. Estimated per-collection index size at
target, using measured `_id` lengths (`MONGO-MODULE-PLAN.md` §3.1) and ~20 bytes of WiredTiger
per-entry overhead. **`_id idx` is mandatory; `+2 sec` assumes two secondary indexes**, which is
conservative — most collections need more.

| collection | docs at target | `_id` len | `_id` index | +2 secondary | note |
|---|---|---|---|---|---|
| `am_method_invocation` | **114,241,375** | 70 | **10.3 GB** | 20.6 GB | the 111,455×1,025 junction |
| `am_invocation` | **66,057,150** | 70 | **6.0 GB** | 11.9 GB | |
| `am_java_method` | 34,228,078 | 70 | 3.1 GB | 6.2 GB | hashed `_id` |
| `am_java_field` | 8,861,045 | 139 | 1.4 GB | 2.8 GB | |
| `am_dag_node` | 3,878,600 | 54 | 0.3 GB | 0.6 GB | |
| `am_statement__columns` | 2,356,547 | 78 | 0.23 GB | 0.46 GB | |
| `am_java_class` | 1,673,036 | 119 | 0.23 GB | 0.47 GB | |
| `am_statement` | 1,260,526 | 70 | 0.11 GB | 0.23 GB | |
| `am_statement__joins` | 1,058,897 | 74 | 0.10 GB | 0.20 GB | |
| `am_call_edge` | 500,000 | 84 | 0.05 GB | 0.10 GB | §A0.2 worst case |
| `am_statement__predicates` | 390,103 | 76 | 0.04 GB | 0.07 GB | |
| `am_subtree_table` | 256,426 | 80 | 0.03 GB | 0.05 GB | |
| `am_proc_node` | 8,000 | 62 | ~0 | ~0 | |
| **total, one run** | **~235M** | | **21.8 GB** | **43.6 GB** | **65.4 GB of index** |

*(All extrapolated from measured per-document sizes; no ingest has been run.)*

**Four conclusions.**

1. **65.4 GB of index for one run**, against a working set that must be in RAM. Ten runs without
   dedup is **~654 GB of index** — unservable on anything reasonable. With §B.3 at c ≈ 0.05 it is
   **~95 GB**, which fits a large single machine.
2. **The whole problem is Java invocations.** `am_method_invocation` + `am_invocation` are
   **180M of the 235M documents and 48 GB of the 65 GB**. Everything else combined is a rounding
   error. **This is where §F's "should we serve 100M lines at all" question actually lands.**
3. **The PL/SQL side is a non-problem.** Its entire index budget is **~0.6 GB per run**. The user's
   PL/SQL target (63×) is comfortably servable today; the crisis is Java-only, and the plan should
   say so rather than treating both as equally at risk.
4. **`_id` hashing is mandatory, as the coordinator said.** At the unhashed 1,025-byte `java_method`
   `_id` *(measured)*, that collection's `_id` index alone would be
   34,228,078 × 1,045 B ≈ **35.8 GB** instead of 3.1 GB — a 11.5× penalty on one collection.

**Recommendation on index discipline:** every index must be justified by a query in §B.6, reviewed
at each increment, and measured with `$indexStats` to find unused ones. At these sizes an unused
index is not tidiness, it is gigabytes of RAM.

### B.6 Query patterns and paging — never scan, never skip

Every screen's query must be expressible as **an index seek plus a bounded scan**. The three rules:

1. **No `skip`.** `skip(n)` is O(n) server-side. At page 1,000 of a 1M-row result it reads and
   discards 50,000 documents per request. Use **keyset (range) pagination** on the sort key.
2. **No unbounded `$lookup` on a hot path.** Denormalise the fields a screen renders (§B.6.4).
3. **Every query starts with an equality match on an indexed, run- or parent-scoped field**, so the
   candidate set is bounded before any sort.

The good news: **the repository already has the right primitive.**
`analyzer-storage/.../page/PageRequest.java` has `MAX_LIMIT = 1000` (`:21`) and an n+1 has-next
probe (`:99`), and its `Cursor` is *"keyed on the class's package and name rather than on an array
position"* (`AnalyzerController.java:1000-1003`) — that is keyset paging, already built and already
cursor-stable across re-ingest. §C extends it rather than inventing it.

**B.6.1 — The hot query: statements for one table.** Measured as the product's most-used path
(`shared/access-details.js:338`, `table:<name>` at limit 500).

```js
// page 1
db.am_statement.find({ runId: R, objectRef: O })
                .sort({ line: 1, _id: 1 }).limit(51)
// page N — keyset, not skip
db.am_statement.find({ runId: R, objectRef: O,
                       $or: [ { line: { $gt: lastLine } },
                              { line: lastLine, _id: { $gt: lastId } } ] })
                .sort({ line: 1, _id: 1 }).limit(51)
```
Index `{runId:1, objectRef:1, line:1, _id:1}` — the sort is satisfied by the index, so no in-memory
sort and no 32 MB sort-memory limit. `_id` is the tiebreaker because `line` is not unique
(measured: 63 ORACLE / 81 POSTGRES / 64 DB2 collisions on `(nodeId, operation, line)`).

**B.6.2 — Faceted counts must not be computed per request.** `StatementSearcher` currently facets on
`operation`, `tableName` and `nodeName` by scanning the whole in-memory array
(`PlsqlSearchController.java:130`). At 1M statements that is a full scan per keystroke.

**Recommendation: precompute facet counts at ingest** into `am_facet_count`:
```
{ _id: "{runId}~statement~operation~SELECT", runId, facet:"operation", value:"SELECT", n: 812443 }
```
Cardinality is tiny — measured 6 operations, 6 access kinds, 570 tables, 298 nodes — so this is a
few thousand documents per run and turns every facet panel into a single indexed range read. The
alternative, `$group` over 1M documents on each request, is the thing that makes the screen slow.

**B.6.3 — Graph traversal stays application-side BFS.** Not `$graphLookup`: 100 MB memory cap, and
its `maxDepth` is not cycle protection. With `am_dag_edge` split out (§B.2) and a covering index
`{parentRef:1, ordinal:1}` including `childRef`, one BFS level is one index-only range read.
A visited set is mandatory — the expansion is 20,300,167,285 nodes *(measured)* and 48 nodes are
flagged recursive.

**Depth and breadth caps must be server-side**, returning a `truncated: true` marker rather than
silently trimming. `AnalyzerController.java:505-509` already measured what happens without one:
200,000-node cap = *"0.07% of the answer"*. A cap that returns 0.07% without saying so is worse
than an error.

**B.6.4 — Denormalise exactly what a row renders, and no more.** From the measured render code
(`shared/access-details.js:120-129`), a statement row shows operation, procedure name, line and
access kind. So `am_statement` carries `nodeName`, `schema`, `tableName`, `objectType` inline
*(as `RELATIONAL-MODEL-PLAN.md` §13.3 established)* — five extra fields, ~325 bytes/row, ~325 MB at
1M statements *(extrapolated)*. That buys zero joins on the hottest path.

**This is safe only because entities are write-once** (§B.1 content-addressing makes a changed
entity a *new document*, not an update). A denormalised copy can never go stale because nothing is
ever updated in place. That property is worth more here than the bytes it costs.

**B.6.5 — The queries that must never exist.** Named so they can be caught in review:

| forbidden | why | replacement |
|---|---|---|
| `find({})` with no run/parent scope | full collection scan of 114M docs | always scope first |
| `.skip(n)` | O(n) | keyset (§B.6.1) |
| `$regex` unanchored (`/foo/`) | cannot use an index | anchored `^foo` prefix, or a dedicated text/trigram field |
| `$group` over a whole collection at request time | 1M-doc scan | precomputed `am_facet_count` (§B.6.2) |
| `sort` on an unindexed field | 32 MB memory-limit failure | index it or don't offer the sort |
| `$lookup` on a request path | unbounded | denormalise (§B.6.4) |
| `count()` without a filter | scans | maintain counters at ingest |

**B.6.6 — Total counts.** The UI wants "42 UPDATEs" while showing 50 (`PlsqlSearchController.java:38`).
An exact count over 1M documents is a full index scan per request. **Recommendation: serve exact
counts from `am_facet_count` for the precomputed facets, and for arbitrary filter combinations
return a bounded count (`countDocuments` with `limit: 10000`) rendered as "10,000+".** This is a
visible product change and needs sign-off — but an exact count on an arbitrary predicate over 1M
rows cannot be made fast, and pretending otherwise is how the screen ends up timing out.

### B.7 Storage engine settings that matter at this size

- **Compression: `zstd` over the default `snappy`** for the large collections. These documents are
  short repetitive JSON-ish strings; measured PL/SQL text deduplicates 73% by content
  (`RELATIONAL-MODEL-PLAN.md` §3.2), which is exactly what zstd exploits. Trade: more CPU per write.
- **The WiredTiger cache must hold the working-set index**, not the data. §B.5's budget (65.4 GB of
  index for one run) is the sizing input, not document bytes.
- **Do not enable `journal.compressor` changes or fiddle with `eviction` without measurement** —
  named because it is the usual reflex and it is not where the wins are here.
- **`readConcern: "local"`, `writeConcern: w:1`** on a single node; revisit to `majority` when the
  replica set of §B.4 lands. Ingest is idempotent and re-runnable (`MONGO-MODULE-PLAN.md` §8.4), so
  durability-per-write is not worth the latency during bulk load.

### B.8 Summary — is MongoDB the right store at this scale?

**For the PL/SQL target: comfortably yes.** ~0.6 GB of index per run, ~5.5M documents. This is not a
hard problem and the current file-based approach is the only reason it is one today.

**For the Java target: yes, but only with §B.3 dedup and §F's scope reduction.** 65.4 GB of index
per run is servable; 654 GB across 10 runs is not. Dedup takes it to ~95 GB. The document count
(235M/run, dominated by 180M invocation rows) is within MongoDB's competence on a replica set with
adequate RAM — but it is **not** within the competence of the analyser that produces it (§A.3.1),
and that is the binding constraint.

**The honest conclusion: the database is not the bottleneck, and fixing it first — as the user
instructed — is still right**, because every other layer's fix depends on the data being queryable
in pages rather than read whole. But no amount of database design makes a 100M-line whole-program
analysis fit in `CallGraphIndex.ResolutionContext`. §F addresses that directly.

---

## C. Middle layer

The seam already exists — `com.analyzer.storage.repo.AnalysisQuery` / `ArtifactScanRepository` with
`Dataset.JAR_CLASS_TREE`. The work is to route reads through it and delete the whole-document paths.

### C.1 Four rules

1. **No endpoint returns a whole document.** Every list endpoint is cursor-paged; every detail
   endpoint is projected.
2. **No request loads a file whole.** Reads stream (`JsonArrayRows.java:101` is the primitive) or
   query Mongo with a limit.
3. **Nothing unbounded is cached.** `ParserAnalysisReader.java:81` caches 30 whole parsed documents
   with no byte budget — at the 63× target that is ~7 GB of heap in a cache.
4. **Every response is bounded before it is built**, not truncated after.

### C.2 Endpoints that must change

**PL/SQL — the least protected surface** (`ParserAnalysisController.java`). All currently return
whole documents with no paging:

| endpoint | line | change |
|---|---|---|
| `/analyses/{name}/procedures` | `:299` | cursor-paged over `am_proc_node`; today returns the entire node array |
| `/analyses/{name}/index` | `:232` | split: scalars+totals stay; `nodes[]` moves to `/procedures` |
| `/analyses/{name}/tables` | `:252` | cursor-paged over `am_db_object`, projected (no `usedBy`) |
| `/analyses/{name}/call-graph` | `:277` | **delete**. Replace with `/call-graph/edges?from=&limit=` — nobody can consume 500k edges |
| `/analyses/{name}/resolver/{type}` | `:495` | cursor-paged |
| `/analyses/{name}/call-tree/{nodeId}` | `:473` | depth+breadth capped, `truncated` marker, lazy children by node id |
| `/analyses/{name}/search` | `PlsqlSearchController:130` | query `am_statement` by index; facets from `am_facet_count` (§B.6.2) |
| `/analyses/{name}/source/{file}` | `:286` | range-read + paginate; currently `readAllBytes` into a `String` |
| `/analyses` | `:62` | paged; currently reads **every** `api/index.json` in the tree |

**JAR:**

| endpoint | line | change |
|---|---|---|
| `GET /{id}` | `AnalyzerController:489` | already token-streamed, but unbounded. Add required `limit`; return the header + links, never the body |
| `GET /{id}/collections` | `:1395` and `WarAnalyzerController:574` | **worst offender**: full `load()` + full response. Query `am_endpoint_collection` paged |
| `GET /{id}/endpoints/.../call-tree` | `:516` | make `format=dag` the **default**; expanded form requires an explicit cap |
| `/correction-logs`, `/run-logs`, `/catalog` | `:1573`, `:1632`, `:1192` | paged |

**The eight `PersistenceService.load()` callers** (`ChatboxService.java:33`, seven in
`ClaudeJobExecutor`) must move to targeted queries. `load()` itself should be **deleted**, not
deprecated — while it exists, something will call it.

### C.3 Streaming ingest

`ChunkedFlowWriter.java:165-176` reads every chunk back into `List<FlowNode> allNodes` for the merge
(§A.3.6). The fix is that **the merge step should not exist**: chunks become Mongo documents
directly, and `writeStatementIndex`/`writeTableIndex` become aggregation queries rather than
in-memory passes over the full list.

For the JAR side, `writeFlattened` (`PersistenceService.java:738`) holds ~3× the document. Ingest
should read the **streamed** artifact (`:178`, which already works) rather than the POJO path.

`AnalysisResult.sourceMap` (`AnalysisResult.java:20`) — every source line as `String` — must become
`ArtifactStore` reads by key. This is a change in the analyser, not the web layer, and it is on the
critical path for the PL/SQL target.

### C.4 Cursor pagination — extend, don't invent

`PageRequest` (`MAX_LIMIT = 1000`, n+1 probe at `:99`) and the value-keyed `Cursor` are already
right. Three additions:

- **Cursor encodes the sort tuple plus `_id`**, matching §B.6.1's tiebreaker.
- **Cursors carry `runId`**, so a cursor from a superseded run fails loudly rather than silently
  paging across two runs.
- **`FacetPageResponder`'s legacy escape hatch closes.** Today `params.wantsEnvelope()` lets a
  caller with no `Accept` header get the old unpaged response — the back-compat hole that keeps
  every unpaged path alive. Retire it per endpoint as each UI caller is migrated.

### C.5 Projection

`/classes/tree?members=false` already drops *"93.8% of this response"* (`AnalyzerController.java:1009-1059`)
— the pattern works and should be the default rather than an option.

**Rule: a list endpoint returns only the fields its list view renders.** Detail comes from a
detail endpoint keyed by `_id`. §B.6.4's denormalised fields on `am_statement` exist precisely so
that projection is a field subset, not a join.

### C.6 Caching and invalidation

Replace the unbounded `TtlCache<JsonNode>` (`ParserAnalysisReader.java:81`) with:

- **Cache keys include `runId`** — which is content-addressed and immutable (§B.1), so
  **invalidation is not required**: a new run has new keys and old entries expire. This is the
  quiet payoff of content-addressing, and it removes the hardest part of caching.
- **A byte budget, not an entry count.** 30 entries of unbounded size is not a cache, it is a leak.
- **Cache page results and facet counts**, not documents. Those are bounded by construction.

### C.7 Back-pressure

- **Reject rather than queue.** A request whose estimated result exceeds the cap returns `400` with
  the cap named, not a truncated body.
- **One concurrent ingest per analysis**, enforced by the partial unique index already specified in
  `MONGO-MODULE-PLAN.md` §5.4 — the index is the semaphore.
- **Bounded ingest concurrency** with a fixed pool; the batch sizes (`BATCH_TARGET_BYTES = 4 MiB`,
  `BATCH_MAX_ROWS = 2000`, `MongoAnalysisRepository.java:138-139`) already bound per-batch memory.
- **Server-side timeouts** (`maxTimeMS`) on every query, so a missing index degrades to an error
  naming the query instead of an occupied connection.

---

## D. UI

§A.4 established there is **no virtualisation anywhere** and the pager is client-side at ~34 of 36
call sites. Three changes, in order of value.

### D.1 Stop building datasets in the browser

`parser/js/api.js:111-121` flat-maps every `usedBy × operation × line` into one array — **the user's
1,000,000-operation target, as JavaScript objects, before a row is drawn.** This must become a
paged server query against `am_statement` (§B.6.1). It is the single highest-value UI change.

Similarly: `jar/js/codeTree.js:97-101` drains 500 pages into one array, defeating its own paging and
silently truncating at 100,000 classes against a 1,673,036-class target.

### D.2 Server-side paging at every `PA.tf` site

The framework is already dual-mode; `parser/js/tableFramework.js:42` decides. The work is converting
**34 call sites** from "pass an array" to "pass a `PA.pageSource`" — mechanical, individually
testable, and enumerated as tasks in the registry.

This also removes the client-side filter/sort chain (`:229-279`), which stringifies every property
of every row per keystroke, and the pathological
`parser/js/tableOps.js:41` (`new Set(...)` allocated **per sort comparison**).

### D.3 Virtualise the two tree views

The call tree has the DAG shape the coordinator identified, and it is worse than the sample
suggests: `parser/js/callGraph.js:207` walks the entire tree to build `_nodeMap` before rendering,
and lazy expansion is defeated by `expandAll()` (`:399`, 200 passes) and by `search()` (`:458`),
which calls `expandAll()` first — **typing in the search box materialises the whole tree**.

Fixes: search moves server-side (return matching node ids, expand only those paths); `expandAll` is
**removed**, not warned about; children load per level from a paged endpoint; rows are windowed.

`jar/js/summary-trace.js:95` renders the whole JAR tree into one HTML string with only a depth
guard, and its own comment concedes the tree *"can reach 120 MB"*.

**What else has this shape** — anything currently recursive over analysis output with no node cap:
`jar/js/callGraph.js:146` `_flattenOps` (no guard at all), `jar/js/codeTree.js:266` (package tree),
and `jar/js/summary.js:217-240`, which runs whole-JAR analysis in the browser in batches of 30.

### D.4 What must stop being loaded eagerly

| surface | today | change |
|---|---|---|
| access details | whole array client-side | paged query |
| procedures / tables left panel | whole arrays, batched at 50 (`leftPanel.js:35`) | server search + paged |
| class tree | 11.8 MB, or 500 drained pages | paged, lazy packages |
| call tree | whole tree + full pre-walk | per-level lazy, server search |
| endpoint list | cap 200 with a `showAll()` escape (`endpointList.js:48,53`) | paged; remove `showAll` |
| summary tables | 12 `_initPage` sites, all plain arrays | paged |

### D.5 The honest note already in the code

`parser/js/leftPanel.js:29-31`:
> *"the largest analysis has 114 procedures and 85 tables… it is not evidence about the
> thousand-procedure case, which no analysis here exercises."*

That case is now the requirement. The comment is a good example of the house convention this plan
should keep: state what the evidence does **not** cover.

---

## E. What we should not do, and the honest limits

### E.1 The Java target needs a different approach, not a bigger machine

**100M lines of whole-program analysis is not achievable by scaling this design**, and the reason is
not the database. It is `CallGraphIndex.ResolutionContext` (§A.3.1): 22 unbounded maps that must be
**complete before any call tree can be walked**, with no eviction and no spill. That is a hard heap
ceiling no storage change addresses.

The evidence that it is already at its limit at **0.1%** of the target, all from the code itself:
- one endpoint expands to **300,167,238** nodes; a 200,000-node cap yields *"0.07% of the answer"*
  (`AnalyzerController.java:505-509`)
- uncapped walking produced a **69 GB** `analysis.json` that *"never finished writing"*
  (`PersistenceService.java:190-196`)
- one generated class has **47,585 invocations** (`CallTreeBuilder.java:1425-1429`)

**Recommendation: reframe the Java target from whole-program to incremental/partial analysis.**

1. **Per-module, not per-jar.** Analyse each module independently, store per-module results, and
   resolve cross-module edges lazily as references rather than by expansion. The index then bounds
   at the largest module, not the whole program.
2. **Analyse on demand from a root.** The product's real question is "what does this endpoint
   touch?", not "index everything". A bounded BFS from one endpoint, with results cached and
   content-addressed, never needs the whole-program index.
3. **Exclude generated code by default.** `CallTreeBuilder.java:1364`'s exclusion list is empty by
   default; measured, `com.plsql.parser.*` is **78% of classes and 85% of methods** in the sample.
   Excluding generated parsers is not a workaround — nobody asks questions about ANTLR internals.
4. **Incremental re-analysis.** With content-addressing (§B.3) and a per-file hash, a re-run
   re-analyses only changed compilation units. Measured: 3 of 5 dialects re-run **byte-identically**,
   so the incremental case is the normal case.

**This is a product decision, not an engineering one**, and it should be made explicitly: is the
requirement "index 100M lines" or "answer questions about a 100M-line codebase"? The second is
achievable; the first, in this architecture, is not.

### E.2 Things not to do

| don't | why |
|---|---|
| **Shard before deduplicating** | 10× the data because every run is a full copy (§A.1); dedup first, then measure |
| **Raise the JVM heap to fix `ResolutionContext`** | linear headroom against a 1,025× target; buys one doubling |
| **Cap the call tree and call it solved** | already done, already measured at 0.07% of the answer |
| **Move to a graph database for the call graph** | the traversal problem is a 2.03 × 10¹⁰ expansion; a graph DB traverses it faster and still cannot return it. The fix is not returning it |
| **Keep both the file store and Mongo as sources of truth** | `PLAN-09:436-483` already decided dual-read, not dual-write, and the reasoning holds |
| **Client-side paging with a bigger page** | the array is already in the browser; the cost is the array, not the page |
| **`$graphLookup` for traversal** | 100 MB cap, `maxDepth` is not cycle protection, and there are 48 recursive nodes |
| **Rewrite the pager** | `PageRequest`/`Cursor`/`FacetPageResponder` are already correct; the work is wiring, not design |
| **Pretty-print stored artifacts** | `ChunkedFlowWriter.java:56` enables `INDENT_OUTPUT` on every artifact |
| **Exact counts on arbitrary predicates** | unfixable at 1M rows; §B.6.6 recommends bounded counts and says it needs sign-off |

### E.3 Where the limits honestly sit

| target | verdict |
|---|---|
| PL/SQL 8,000 procs / 1M operations | **achievable.** ~0.6 GB index/run, ~5.5M docs. The DB work is the easy part; the analyser's `sourceMap` (§A.3.7) and the unpaged endpoints (§A.3.5) are the real work |
| Java 100M lines, whole-program, per run | **not achievable** in this architecture (§E.1) |
| Java 100M lines, incremental + per-module + generated-code excluded | **plausible**, but unproven — no codebase near this size has been measured |
| 10 retained runs | **achievable with dedup** (~95 GB index), **not without** (~654 GB) |

### E.4 The measurement that should happen before any of this is built

Every Java figure here is extrapolated from a **single 97,607-line jar that is 85% generated parser
code** (§A0.3). The ratios may be wrong by 2× in either direction.

**Recommendation: analyse one real large codebase — 5-10M lines — and re-derive the ratios before
committing to a cluster shape or a sharding plan.** The cost is a day; the alternative is sizing a
system on a generated grammar. This is the single highest-value action in the whole document and it
requires no code.
