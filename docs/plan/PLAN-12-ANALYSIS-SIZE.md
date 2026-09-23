# PLAN-12 — Making a large JAR analysis storable, without truncating it

Everything below is measured on `unified-web-1.0.0-SNAPSHOT.jar` (84 MB, 1,623 classes,
317 endpoints) on this machine. No estimates.

## 1. What we actually know

| run | `analysis.json` | note |
|---|---|---|
| uncapped (our default) | **~69 GB**, aborted | never completed on local, S3 or MongoDB |
| caps 30 / 30 / 50,000 | **77,031,534 B** | **first successful completion ever** |
| caps 45 / 30 / 50,000 | **78,058,032 B** | +1.3% for 50% more depth |

Largest analysis previously completed on this machine: **5.53 MB**. This JAR had never finished, so
nothing regressed — no input this large had been run before.

### 1.1 The node pool the user asked for already exists

```
callTreeFormat: "dag-v1"
callTreeNodes:  { "<id>": { className, methodName, …, childIds: [ … ] }, … }   // 4,089 nodes
endpoints[].callTreeRootId: "<id>"
```

Each node is stored **once**, children referenced by id, no nested `children`. 12,650,914 B for the
entire call graph serving 317 endpoints. **The call tree is not the size problem.**

An earlier diagnosis in this session blamed `CallTreeBuilder`'s memo shallow-copy
(`node.setChildren(cached.getChildren())`) for serialising shared subtrees repeatedly. That is a real
property of the in-memory object graph, but `dag-v1` already supersedes it on the write path. The
diagnosis was wrong for the current code and is recorded here so it is not repeated.

### 1.2 Where the 77 MB actually is

| field | bytes | share | in the DAG pool? |
|---|---|---|---|
| `classes[].methods[].invocations` | **23,877,407** | 31.0% | no |
| `endpoints[].externalCalls` | **16,295,438** | 21.2% | no |
| `callTreeNodes` (the whole call graph) | 12,650,914 | 16.4% | — |
| `classes[].fields` | 4,575,941 | 5.9% | no |
| `classes[].methods[].fieldAccesses` | 4,371,205 | 5.7% | no |
| `endpoints[].dynamicFlows` | 4,213,611 | 5.5% | no |

Plus, outside the document: `endpoints/` = **288 MB**, and `_summary_static.json` = 6.5 MB.

**The two dominant costs are per-endpoint and per-method aggregates that sit outside the node pool.**
`externalCalls` is the clearest case: the same external call reachable from 50 endpoints is written
50 times, because the aggregate is materialised per endpoint rather than referenced.

### 1.3 What is NOT the cause — each ruled out by measurement

- **Recursion.** Cut correctly by `callStack.contains(nodeId)`.
- **Depth.** 30 → 45 costs 1.3%.
- **Unbounded class scanning.** `basePackage` already filters Phase 2:
  `JarParserService:179-195` skips any `BOOT-INF/lib` JAR containing no class under the base
  package. This run scanned 1,623 classes, not the whole fat jar.
- **Embedded decompiled source.** 0.0-4.0% of the document (mean 322 B/node). An earlier claim of
  "~2,050 B/node is what drove it to 912 MB" was wrong; see PLAN-00 §6.
- **The storage backend.** Local produced 69 GB, MongoDB ~80 GB, S3 912 MB before abort. Same
  document, three backends.

### 1.4 RESOLVED — the cause was bundled generated parser code, not fan-out

**The fan-out hypothesis below was tested and is WRONG.** Running with children unlimited produced a
*smaller* document (76,909,451 B vs 77,031,534 B). Fan-out is not the driver; it is recorded here so
the test is not repeated.

The actual cause: `unified-web.jar` bundles its own ANTLR-generated PL/SQL parser. **1,219 of the
1,623 analysed classes are `com.plsql.parser.*`**, and `PlSqlParser` alone has 1,245 methods and
47,585 invocations. ANTLR rule methods are mutually recursive with very high fan-out, and because a
`dag-v1` id is a hash over a node's `childIds`, every distinct *path* through the grammar hashes to a
distinct node. Dedup cannot collapse them — they genuinely are different subtrees.

**Fix** (`CallTreeBuilder`): `org.antlr.` / `antlr.` added to `isStandardLibraryClass`, plus a
configurable `calltree.exclude-packages` (empty by default). Both are checked at the **recursion
decision**, not at render time, so an excluded class still appears as a leaf node while its callees
are not expanded — the tree stays honest about what it saw.

| run | `analysis.json` | |
|---|---|---|
| uncapped, generated code walked | ~69,000,000,000 B | aborted, never completed |
| caps 30 / 30 / 50,000 | 77,031,534 B | first completion |
| **uncapped, generated code excluded** | **53,050,432 B** | **25 s, all caps still off** |

Merkle split collapsed 1.15× → **1.03×** (1,894 ids for 1,841 distinct methods) and `endpoints`
shrank 3.8× (22,140,316 → 5,763,734 B). `classes` is now 89% of the document, dominated by
`invocations` — which makes **Step 4 the only remaining step that matters**.

Acceptance §6.1 is met: uncapped completes, well under 1 GB.

## 2. The design goal

> Never truncate the crawl. Make the *output* size proportional to distinct facts, not to paths.

The crawler must keep walking everything — a truncated tree silently under-reports collections and
operations, which is a wrong answer that looks right. That was the reason the caps were removed and
it still holds. The fix belongs in what gets **written**, never in what gets **walked**.

## 3. The work, in order

Each step is independently shippable, measurable, and revertible. Sizes are the measured
contribution at caps 30/30/50,000; the uncapped saving is larger.

### Step 1 — ~~Isolate fan-out~~ DONE, and it was not fan-out
See §1.4. Fan-out ruled out by measurement; the cause was bundled generated parser code and is
fixed. **Step 5 is therefore not needed** — the document completes uncapped at 53 MB.

### Step 2 — Pool the endpoint-level arrays — RE-MEASURED, the −16.3 MB below is stale

That figure came from the 77 MB document. The ANTLR fix shrank `endpoints` 3.8×, so re-measured on
the current document:

| field | entries | distinct | inline | saving |
|---|---|---|---|---|
| `externalCalls` | 5,958 | 277 (4.6%) | 2.25 MB | **2.11 MB** |
| `dynamicFlows` | 3,456 | 1,357 (39.3%) | 2.14 MB | 1.33 MB |
| `beans` | 2,818 | 177 (6.3%) | 0.78 MB | 0.72 MB |
| `aggregationFlows` | 16 | 16 (100%) | 0.01 MB | none |
| `httpCalls` | 1 | 1 (100%) | — | none |

**DONE.** `EndpointArrayPool`, measured on an identical subject JAR analysed by both builds in
isolated data dirs:

```
44,937,916 -> 40,936,321 B    -4,001,595 B (-8.9%)
12,232 references over 1,811 distinct entries across 3 fields
```

**323 responses compared: 320 byte-identical, 3 differing only in `analyzedAt`** (two separate
runs, identical lengths). `aggregationFlows` and `httpCalls` are excluded — 100% unique, so pooling
them would cost bytes.

### Four traps, every one of them silent

This step earned its "highest risk" label. None of these would have thrown:

1. **Format detection.** `checkHasPrecomputedAggregates` decides "new format" by finding a literal
   `externalCalls` field on the first endpoint. Pooling renames it, so the check would have
   returned false and the summary would have **recomputed aggregates from the call tree** —
   different collections and domains, no error. Now short-circuits on the pool object.
2. **POJO type clash.** `EndpointInfo.externalCalls` is `List<Map<String,String>>`; binding
   `[0,3,7]` **throws**, so `load()` would have failed on every pooled analysis. Hence the ordinals
   live under a `*Refs` key, with `*Refs` fields on the model and resolution in `rehydrate`.
3. **The skip list stopped matching.** The summary generator first stored inflated values under the
   `*Refs` key, so `HEADERS_SKIP_FIELDS` — which drops these three fields by name — no longer
   matched and emitted them in full: `/summary/headers` grew **6.3×**, 928,083 → 5,892,425 bytes.
   Inflation now restores the original key.
4. **A leaked marker and leaked ordinals.** `endpointPoolFormat` appeared in `/summary/header`
   (+40 bytes of internal storage detail in a public response), and `/summary/endpoints` — which
   streams `analysis.json` directly rather than the summary cache — emitted the raw ordinal arrays,
   +51,029 bytes of numbers the UI cannot use. The marker is gone entirely (the pool object's
   presence says the same thing), and that route now inflates.

Only the byte-for-byte comparison caught 3 and 4; the suite was green throughout.

#### (original reasoning, superseded)
*Move `externalCalls` into the node pool  (−16.3 MB, 21%)*
It is per-endpoint today. Store each distinct external call once, reference it by id from the nodes
that make it, and let the endpoint aggregate be derived on read. Same shape out of the API.
- **Risk:** the endpoint aggregate is what the UI renders; deriving it must produce identical bytes.
- **Test:** `cmp` `/summary/headers` and the four slice endpoints before and after.

### Step 3 — The `endpoints/` duplicates — PARTLY DONE, and the diagnosis was wrong

Measured on one analysis directory (323 MB total, `endpoints/` = **191 MB**):

| file | count | size |
|---|---|---|
| `nodes.json` | 317 | 53.64 MB |
| `call-tree.json` | 317 | 36.07 MB |
| `endpoint.json` | 317 | 6.48 MB |
| `node_*.json` | ~2,000 | remainder |

**The cause was not path duplication — it was pretty-printing.** `call-tree.json` is already the
flattened dictionary form, so it never duplicated subtrees; both large files were simply written
through `prettyWriter`, and indentation is most of their weight.

**Done:** `call-tree.json` is now written **compact**, priced on identical content by
re-deriving Jackson's pretty form from the compact files:

```
317 files    compact 29.55 MB    pretty 35.79 MB    saved 6.24 MB (17%) per analysis
```

This is free. Its only route, `GET /{id}/endpoints/{endpoint}/call-tree`, does
`readTree(file) -> expandSidecar -> writeValueAsString`, and whitespace cannot survive `readTree`;
`SidecarCompactnessTest` asserts `readTree(compact).equals(readTree(pretty))`. Verified live: 317/317
call-tree routes serve from compact storage, and 120 captured routes on a pre-existing
pretty-printed analysis stayed **byte-identical**.

> **Two measurement mistakes were made getting here, both worth remembering.** First, the file total
> was compared across *different subject JARs* (36.07 MB vs 21.92 MB) and read as a 14 MB win; it
> was mostly a different input. Second, both of those totals were sampled **while the endpoint
> writer was still running** — `Analysis saved` precedes endpoint output, so a count taken at that
> log line is short (268 of 317 files, read as a regression that did not exist). A size delta is
> only evidence when the content is identical and the write has finished.

**Not done, and it is a judgement call:** `nodes.json` (53.6 MB) is served **verbatim** by
`GET /{id}/endpoints/{endpoint}/nodes`, as are the `node_*.json` files. Compacting them would change
those response bytes — whitespace only, identical parsed content, and the consumers are JSON
parsers. That is a deliberate decision to leave with the owner rather than take unilaterally under a
strict byte-identity requirement.

**Deleting them outright is a bigger change than this plan implied.** Five consumers read them
directly — `DecompileController` (2 routes), `CrossRefService` (3 sites), `ExternalApiLinkService`,
and `EndpointOutputWriter`'s own listing optimisation — and all are keyed by *endpoint folder name*,
a mapping that does not exist in `analysis.json`. Serving them from the node pool would require
inventing that resolution, and cross-ref has no test data on this machine to verify against.

#### (original reasoning, superseded)
*Stop writing the `endpoints/` duplicates  (−288 MB on disk)*
`endpoints/*/call-tree.json` and `nodes.json` are the same tree already in `callTreeNodes`
(PLAN-01 recorded call trees stored 4×). `node_*.json` is the right grain and stays.
- **Risk:** `DecompileController:152,164,178` reads these files directly.
- **Test:** those three routes must still serve identical bytes, assembled from the pool.

### Step 4 — Reference `invocations` instead of inlining — MEASURED, and it does not pay

Now measured on the 53 MB uncapped document rather than reasoned from the 77 MB field table:

```
invocations total : 110,234
distinct          :  63,301   (57.4% unique)
inline bytes      : 22,297,106
pooled + refs     : 16,208,235
saving            : 6.1 MB  = 27% of the field, 11% of the document
```

**At 57.4% unique there is little duplication left to remove.** 11% of the document is the payoff
for the change this plan itself rates highest-risk — it touches `DecompileController:210`'s
whole-class passthrough, which feeds 8 live `.invocations` reads in `summary-codeview.js`, against a
standing requirement that every API response stay byte-identical. Acceptance §6.1 is already met
without it (uncapped completes at 53 MB).

**Built anyway, on request, with byte-identity proof — and it beat the estimate.**

`InvocationPool` pools by **integer ordinal** rather than content hash, precisely because at 110k
references a 20-char hash id would cost 2.4 MB and eat 40% of the saving. The pool is written before
`classes` (forward-only readers) and built in a separate scan pass, exactly as
`CallTreeFlattener` does for call-tree nodes. `PersistenceService.rehydrate` resolves it on POJO
load, so nothing downstream knows pooling exists.

Measured on an **identical subject JAR** analysed by both builds in isolated data dirs — the only
valid comparison, since analysing a rebuilt jar changes the subject:

```
subject.jar (84,531,867 B) analysed twice
  pre-pooling : 52,750,850 B
  pooled      : 44,937,916 B      −7,812,934 B  (−14.8%)
  pool        : 110,234 references over 63,301 distinct
```

**804 API responses compared byte-for-byte: 800 identical, 4 explained.** Three differ only in
`analyzedAt` (two of those are 404s differing in the error timestamp); the fourth is
`/summary/headers`, differing in `analyzedAt` and in the additive `advice` field. All 317 call-trees
and all 317 endpoint objects in `/summary/headers` were identical, as were 400 `decompile?class=`
responses — the whole-class passthrough that feeds the 8 `.invocations` reads. **Zero differences
attributable to pooling.**

### Three real defects this uncovered

1. **A lone surrogate aborted the write.** `Incomplete surrogate pair: first char 0xDF00` —
   decompiled string constants can contain an unpaired surrogate, and `JsonNode.toString()` emits
   it as a raw char that the UTF-8 encoder rejects. Fixed by writing pooled entries and rewritten
   classes through the `JsonGenerator`, which applies the same escaping that wrote the source
   JSONL. Regression-tested.

2. **A failed write leaves a corrupt `analysis.json` in place.** When the above crashed mid-write,
   three previously-good analyses were truncated (`Unexpected end-of-input`, ~23.7 MB of 53 MB) and
   `reanalyze-all` then found **0** valid JARs — the metadata it needs is inside the file it just
   destroyed. **Still open:** the local write path is not atomic. It should write to a temp key and
   commit on success, so a failure leaves the previous analysis intact.

3. **Two streaming readers would have silently truncated.** `SummaryStats` and one loop in
   `AbstractAnalysisDataProvider` break on any `END_OBJECT` and had no `skipChildren` default, so
   descending into the new pool array would have ended the pass at the first pooled invocation's
   closing brace — returning stats and summaries for a fraction of the document, with no error.
   Both now skip the pool as a unit. A codebase-wide audit found only these two.

#### (original reasoning, superseded)

#### (original reasoning, superseded)
The largest single field. **Removal was already costed and rejected** — it has 8 live consumers in
`summary-codeview.js` reached via `DecompileController:210`'s whole-class passthrough. So do not
remove it; store each distinct invocation once in a pool and reference it, exactly as the call tree
already does.
- Null suppression on `MethodCallInfo` has already taken 26.9% off this field's real weight; the
  remaining cost is duplication across methods that call the same target.
- **Risk:** highest of the four. Do it last.

### Step 5 — A loud backstop, only if Step 1 confirms fan-out
Not a silent cap. If a tree exceeds a very high node budget, finish the analysis, mark it
`truncated: true` with the reason, and log loudly. The failure mode to avoid is 69 GB written with no
warning; the failure mode to avoid *equally* is a quietly incomplete answer.

## 4. Two correctness gaps found while investigating

Neither is about size, both are worth fixing.

### 4.1 AOP and interceptors are not detected at all — FIXED

**First, a correction to the diagnosis above.** It claimed the annotations being absent from
`SpringAnnotations.ANNOTATION_MAP` made them invisible. That is wrong:
`BytecodeClassParser.parseAnnotationNodes` does **not** filter by the map — an unmapped annotation
falls through to a simple-name branch, so `@Aspect` was always captured in `annotationDetails`. The
map only supplies canonical names. The real gap was that nothing ever *recognised* advice as advice
or related it to the flows it cuts.

**What was built.** `AdviceDetector` + `AdviceInfo`, emitted as an additive top-level `advice`
array, ordered before `classes` for forward-only readers and omitted entirely when a JAR has no
advice, so no existing consumer sees a new field. It is a post-pass over already-parsed
`ClassInfo` — every input it needs (annotations, `interfaces`, `recentStringArgs`) was already
captured, so it adds no parsing cost. It streams: on `unified-web` it retains **2 of 1,623
classes**. `@Aspect` also now yields an `ASPECT` stereotype, tested before the stereotype loop
because that loop returns on the first annotation in *bytecode* order.

**The field that matters is `registered`, not the detection.** Implementing `HandlerInterceptor`
does nothing until something registers it. Spring Boot auto-registers a `Filter` bean and
auto-proxies an `@Aspect` bean, but a `@Component HandlerInterceptor` with no
`WebMvcConfigurer.addInterceptors` call is **inert** — so `AUTO_REGISTERED_KINDS` is deliberately
narrow, and treating "is a bean" as "is registered" would report exactly the inert interceptors as
active.

**It immediately found a real one in this repository:**

```json
{"kind":"HANDLER_INTERCEPTOR","className":"com.analyzer.auth.ApiKeyAuthInterceptor",
 "stereotype":"COMPONENT","registered":false,
 "methods":[{"name":"preHandle","advice":"preHandle"}]}
```

`ApiKeyAuthInterceptor` is complete and 401s every `/api/` path, but
`WebConfig.addInterceptors` has its `registry.addInterceptor(authInterceptor)` line commented out
(`WebConfig.java:22`). **Every `/api/` route is unauthenticated while the codebase appears to have
an auth gate** — which independently confirms the open item about
`POST /api/chat/files/{read,write}`: those routes are not specially exposed, nothing is protected.

**One heuristic was built, measured as wrong, and removed.** Collecting path constants from the
advice body reported `excludedPathPatterns: ["/api/"]` — the paths the interceptor *protects* —
as exempt from it. `startsWith("/api/")` (a guard) and `equals("/login")` (an exemption) are both
just a `String` call on a constant; separating them needs branch-outcome dataflow. Only
registration-derived patterns are recorded now, where the framework defines which list is which.
A test pins the removal, since it reads as a missing feature.

**Not done: synthetic call-tree edges.** Placing advice into each intercepted flow needs a real
AspectJ pointcut matcher; a partial one would mis-attribute silently, which is worse than a
reported gap. The pointcut is stored verbatim so a reader can do the join.

### 4.3 An unregistered auth interceptor — investigated, unchanged

Found by §4.1's detector and then verified against a running instance rather than by reading code.

`ApiKeyAuthInterceptor` is the **only** auth mechanism in the codebase — there is no Spring
Security, and the only registered filter is `ApiLoggingFilter` (logging). Its registration is
commented out (`WebConfig.java:22`), so all ~315 mapped routes (219 GET, 81 POST, 11 DELETE, 3 PUT,
1 PATCH) are unauthenticated.

**What holds**, probed with no session:

| probe | result |
|---|---|
| `../../../../etc/passwd` | `400` rejected |
| `/etc/passwd` | confined to the data root |
| read `config/plsql-config.yaml` | blocked by `CredentialGuardStore` |
| overwrite `config/plsql-config.yaml` | blocked; file intact, 4,533 B |

`ChatController.keyInsideDataRoot` is sound: it rejects `\0` and `.`/`..` segments and rebuilds the
path as an `ArtifactKey`, so an absolute path cannot escape the store namespace. The credential
guard fired on a file that **exists**, so this is a real result and not absence.

**What is exposed:** unauthenticated read *and write* of any artifact in the data root
(`data/probe-unauth/hello.txt` was created and removed to confirm), so analysis results can be
silently falsified; and 11 DELETE routes, of which
`DELETE /api/plsql/config/projects/{p}/environments/{e}/connections/{c}` is the most damaging — it
removes a datasource definition, after which `ConfigDirService` re-seeds classpath defaults and the
analyses cannot connect.

Minor: overwriting a credential file returns **500**, not a deliberate 4xx.

**Left as-is.** The comment reads "Auth disabled: API-key login gate removed for local use", so this
is intentional for a local tool, and on loopback the risk is bounded. It stops being acceptable the
moment the app binds a non-loopback interface.

### 4.4 The Code Structure tree never rendered a single method — FIXED

`AbstractAnalysisDataProvider.streamClassTree` renames `classes[].methods` to `methodSummaries` on
the way out. The browser was never updated: `codeTree.js:755` read `cls.methods`, got `undefined`,
fell back to `[]`, and the **Methods section of the Code Structure tree was empty for every class**.
Measured against the live payload:

```
classes 1623    methods rendered before: 0    after: 33,250
```

Nothing ever failed. `[]` is a legitimate value for a class with no methods, so this was a silently
empty panel that only a person looking at the screen could catch.

`codeTreeViews.js` had the same defect in four places — member counts, the search index, and
`totalMethods` were all computed from the missing field. Both files now read
`cls.methodSummaries || cls.methods || []`, keeping the old name as a fallback for stored payloads.

`RenamedResponseFieldGateTest` locks it, **scoped to the two files that consume that payload**.
Scope is the whole difficulty here: `.methods` appears on three different payloads and only one is
renamed. Written unscoped, the gate flagged five files of which one was a real bug:

| file | payload | verdict |
|---|---|---|
| `codeTree.js`, `codeTreeViews.js` | `/classes/tree` | **renamed — was broken, fixed** |
| `summary-codeview.js` | `/decompile` (whole class node, no rename) | correct; "fixing" it would have been the mistake |
| `callGraph.js`, `summary-analyzer.js`, `summary-scheduled.js` | `JA.app.currentAnalysis.classes` | carries **neither** field — see 4.5 |

### 4.5 Three summary consumers loop over data the summary never contained

`callGraph.js:184`, `summary-analyzer.js:249,291` and `summary-scheduled.js:34` iterate
`cls.methods` on `JA.app.currentAnalysis.classes`. That payload's class entries carry class-level
metadata only — verified live, the keys are `annotations, fullyQualifiedName, interfaces,
isAbstract, isEnum, isInterface, jarName, packageName, simpleName`. **Neither `methods` nor
`methodSummaries` is present**, so those loops have always been no-ops.

**Deliberately not "fixed" by renaming**, which would have papered over it. At least
`summary-scheduled.js` is a legacy fallback whose primary path uses the precomputed
`scheduledJobs`/`scheduledCount`, so the dead branch may be harmless. Resolving it properly means
deciding, per consumer, whether the summary should carry method-level annotation data at all — that
grows the summary for every request, so it is a product decision, not a patch.

### 4.6 Counts were silently capped — FIXED

Verified after the pooling work by reconciling every count-bearing surface against the stored
document, with pools resolved by hand. Everything agreed:

| surface | value | matches document |
|---|---|---|
| `header.totalClasses` / `totalEndpoints` | 1,623 / 317 | yes |
| `classes/tree` classes | 1,623 | yes |
| `classes/tree` `methodSummaries` | 33,250 | yes |
| `classes/tree` `methodCount` sum | 33,250 | yes |
| `/summary` endpoints | 317 | yes |
| `dynamicFlows`, `beans` | 3,456 / 2,818 | yes |

**One did not:** `externalCalls` came to 3,104 against a stored 5,958. Pooling was NOT the cause —
the unpooled document reported the same 3,104. `CAPPED_ARRAY_FIELDS` caps these arrays in the
summary (100 for `externalCalls`/`beans`, 200 for the flows, 50 for `httpCalls`), which is
deliberate and necessary: uncapped, one real analysis produced a **650 MB** summary. 23 endpoints
were truncated, several from 224–225 entries down to 100.

The defect was that the truncation was **silent**. Anything counting the array got the cap instead
of the truth. The cap stays; a `<field>Total` is now emitted alongside a truncated array, so a
reader can say "100 of 224" rather than quietly saying "100". Written only when truncation actually
occurred, so untruncated responses are byte-for-byte unchanged. Verified: the reported totals now
sum to **5,958**, exactly the stored figure.

### 4.7 Stale responses could overwrite newer ones — one confirmed case fixed

The risk: two requests for the same panel, the slower earlier one lands last and repaints stale
data. The PL/SQL side already solves this (`PA.sourceModal._reqSeq` / `_isCurrent`, commented "so no
panel can show the previous procedure's data"); the JAR side had no equivalent.

Checked, not assumed:

- **`summary-codeview.js` class-code modal — already safe.** Its callback re-checks
  `entry.fqn !== fqn` and bails with "navigated away".
- **`summary-trace.js` `_nodeNavGo` — a real race, now fixed.** It set `s.currentIdx = newIdx`
  inside the fetch callback with no guard, so clicking through nodes quickly could snap the panel
  back to a node you had already moved past. It now takes a monotonic token and applies the result
  only if it is still the newest navigation *and* the panel state object is unchanged — the second
  check matters because you can close the panel and open a different endpoint mid-flight.
  The counter is explicitly initialised to 0: `++undefined` is `NaN`, `NaN !== NaN`, and an
  uninitialised token would have made the guard reject **every** response, so the panel would never
  update at all.

**Not a clean bill of health.** There are ~117 async call sites in `jar/js`; a sample was audited,
not all of them. Any new fetch that writes shared state needs the same token pattern.

**A separate staleness vector worth knowing:** the summary cache (`_summary_static.json`) is served
in preference to regenerating. Changing the summary's shape — as §4.5 did — has no effect until it
is invalidated; the new field appeared only after deleting the cached artifacts.

### 4.2 An internal library outside the base package is skipped entirely
Phase 2 scans only `BOOT-INF/lib` JARs containing at least one class under the detected base package
(`JarParserService:179-195`). An interceptor — or any code — in a differently-named internal library
is not scanned, so its body and everything it calls is missing.

**Recommendation: accept a list of base packages, not one detected prefix.** `detectBasePackage`
guesses from the main package set; a second internal group (`com.enterprise.*` plus
`com.acme.platform.*`) cannot be expressed today.

## 5. What "handles 12 GB" means after this

Honestly: **no single step makes a 12 GB document comfortable, and that is the wrong target.** The
right target is that the document stops growing with paths. Steps 2-4 move the three largest fields
into pools where each fact is stored once, at which point size tracks distinct
methods/calls/endpoints — all bounded by the JAR — rather than the number of ways through the graph.

For this JAR that is 77 MB → an estimated 25-35 MB at the same caps, and, more importantly, a shape
that does not explode when the caps come off. That estimate is arithmetic from §1.2, not a
measurement, and should be re-measured after Step 2.

## 6. Acceptance

1. `unified-web.jar` completes **uncapped** and the result is under 1 GB.
2. Every affected endpoint is byte-identical, proven with `cmp` on a small JAR first.
3. An analysis stored in the old inline format still reads — there is no migration step.
4. 1,509 tests, four gates, conformance 25/25 × three backends: unchanged.
5. `df` never moves more than a few GB during a run.
