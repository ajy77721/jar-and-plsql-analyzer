# PLAN-01 — JAR/WAR Analyzer JSON Artifact Inventory

Evidence base:
- Code: `jar-analyzer-core/src/main/java/com/jaranalyzer/**`, `unified-web/src/main/java/com/jaranalyzer/**`
- Real data read from disk: `jar-plsql-analyzer/data_old_1784577715/jar/**` (5 analyses; `web-1.0.0-SNAPSHOT` is the only complete one, with `endpoints/`, `resources/`, `bundled-jars.json`).
  `../data-backup-20260902-054631/jar/` exists but is **empty** (0 bytes) — no analyses there.
  Caveat: the on-disk sample was produced by an older build (its `analysis.json` contains only **1** occurrence of `"sourceCode"`), whereas current code enriches call trees with decompiled source *before* writing `analysis.json` (`JarAnalysisExecutor.java:215` then `:220-225`). Size projections below account for that.

All artifact reads/writes go through `com.analyzer.storage.ArtifactStore` keyed by `JarDataPaths` (`JarDataPaths.java:104-153`); `<jarId>` = `JarNameUtil.normalizeKey(jarName)` (`JarDataPaths.java:120-125`). WAR uses the same code with key root `jar/war` (`JarDataPaths.java:115-117` + `WarDataPaths`).

---

## 1. Summary table

| # | Key | Written by | Cardinality | Typical size (observed) | Worst case | Bounded? |
|---|-----|-----------|-------------|--------------------------|-----------|----------|
| 1 | `jar/<id>/analysis.json` | `PersistenceService.writeAnalysisStreamingTo` :186-239 | 1 / jar | 19 KB – 5.8 MB (119-class jar: **3.90 MB**) | **~1 GB** (1,597-class jar, live run) | **UNBOUNDED** (classes[] × methods[] × invocations[], endpoints[].callTree with `sourceCode`) |
| 2 | `jar/<id>/analysis_corrected.json` | same method via `:170`; `save`/`saveCorrected` :298-325 | 0–1 / jar | ≈ analysis.json | ≈ analysis.json | UNBOUNDED |
| 3 | `jar/<id>/analysis_corrected_prev.json` | `PersistenceService.rotateCorrected` :454-460 (`store.move`) | 0–1 / jar | ≈ analysis.json | ≈ analysis.json | UNBOUNDED |
| 4 | `jar/<id>/_header.json` | `PersistenceService.writeHeaderSidecar` :270-294 | 1 / jar | **191 B** (154–204 B observed) | < 1 KB | **BOUNDED** |
| 5 | `jar/<id>/_summary_static.json` (+ `.gz`) | `PersistenceService.preGenerateSummaryCache` :374-406 | 1 / jar | **430 KB** (3.8 KB–430 KB); gz **17.7 KB** | 10s of MB | unbounded but capped arrays → grows ~O(classes+endpoints) |
| 6 | `jar/<id>/_summary.json` (+ `.gz`) | same :374-406 (Claude/corrected mode) | 0–1 / jar | not present on disk | as #5 | as #5 |
| 7 | `jar/<id>/_class-tree.json`, `_class-tree_<v>.json` | **no writer exists** — only deleted (`PersistenceService.java:356-366`) | — | — | — | **DEAD KEY** |
| 8 | `jar/<id>/connections.json` | `PersistenceService.storeConnections` :93-95 (built by `MongoCatalogService.extractAllConnectionInfo` :377-405) | 1 / jar | **130 B** | < 2 KB | BOUNDED |
| 9 | `jar/<id>/domain-config.json` | `PersistenceService.storeDomainConfig` :73-81 / snapshot copy `JarAnalysisExecutor.java:196` | 1 / jar | **7.8 KB** | ~50 KB | BOUNDED |
| 10 | `jar/<id>/bundled-jars.json` | `PersistenceService.storeBundledJarInfo` :674-686 | 1 / jar | **3.03 MB** (94 jars) | 100s of MB (embeds every bundled jar's text resources) | **UNBOUNDED** |
| 11 | `jar/<id>/jar-dep-map.json` | `PersistenceService.storeJarDepsMap` :702-713 | 1 / jar | **1.1 KB** (11 entries) | few MB | grows with (#dep jars × #app classes) |
| 12 | `jar/<id>/mongo-catalog.json` | `MongoCatalogService.storeCatalog` :420-431 | 0–1 / jar | none on disk (live fetch **disabled**, `fetchCatalog` :413 returns null) | < 1 MB | BOUNDED |
| 13 | `jar/<id>/external-links.json` | `ExternalApiLinkService.writeResult` :281-283 | 0–1 / jar | none on disk | MBs | grows with #HTTP call sites |
| 14 | `jar/<id>/external-links-pending.json` | same :288-290 | 0–1 / jar | none on disk | MBs | same |
| 15 | `jar/<id>/endpoints/<NNN_Ctrl.method>/endpoint.json` | `EndpointOutputWriter.writeEndpointOutput` :79-80 | 1 / endpoint | **9.9 KB** (avg over 84 eps: ~6 KB) | ~1 MB | grows with tree aggregates |
| 16 | `.../call-tree.json` | same :82-83 | 1 / endpoint | **avg 31 KB, max 178 KB** (84 eps, 5.4 MB total dir) | 100s of MB / endpoint with `sourceCode` | **UNBOUNDED** |
| 17 | `.../nodes.json` | same :101-106 | 1 / endpoint | **avg 31 KB, max 175 KB** | as call-tree.json | **UNBOUNDED** |
| 18 | `.../nodes/node_<n>.json` | same :108-113 | 1 / node (560 files for 84 eps) | **avg 2.05 KB, max 6.2 KB** | ~1 MB (huge decompiled method) | BOUNDED per doc |
| 19 | `.../fragments/<fragmentKey>.json` | same :90-94 | 0..N / endpoint | none on disk | as call-tree | unbounded |
| 20 | `jar/<id>/resources/<name>` | `PersistenceService.storeResourceFiles` :632-648 | N / jar (9 observed) | 1.8 KB – 19.8 KB | MBs | per-file bounded |
| 21 | `jar/<id>/stored.jar` (binary) | `PersistenceService.storeJar` :57-65 | 1 / jar | **433 KB – 70.5 MB** | GBs | UNBOUNDED (blob) |
| 22 | `jar/<id>/chat/history.jsonl` | `ChatHistoryService.append` :41-67 | 1 / jar | none on disk | **hard-capped 10 MB** (`MAX_HISTORY_BYTES`, :55-58) | BOUNDED by cap |
| 23 | `jar/<id>/claude/_tracker.json` | `ClaudeEnrichmentTracker.persistState` :160-170 | 1 / jar | small | ~1 MB (errors[] + endpoints{}) | grows O(#endpoints) |
| 24 | `jar/<id>/claude/_meta.json` | `FragmentStore.writeMetaSafe` :96-109 | 1 / jar | < 1 KB | < 1 KB | BOUNDED |
| 25 | `jar/<id>/claude/_index.json` | `FragmentStore.writeIndex` :63-74 | 1 / jar | small | O(#fragments) | grows |
| 26 | `jar/<id>/claude/_call_log.jsonl` | `ClaudeCallLogger.logCall` :55-88 | 1 / jar | — | O(#Claude calls) | unbounded (no cap found) |
| 27 | `jar/<id>/claude/_checkpoint.json`, `_cluster_plan.json`, `_cluster_<n>_context.json` | `ClaudeAnalysisService` :397, `SwarmClusterer` :325, `ClaudeAnalysisService` :434 | 1 / jar (cluster ctx: 1 / cluster) | — | MBs | unbounded |
| 28 | `jar/<id>/claude/<frag>_input.json` / `_output.json` / `_assembled.json` | `ClaudeAnalysisService` :212,236,272,321,474-640 | 2+ / endpoint (+ per chunk) | — | MBs each (LLM prompt/response payloads) | unbounded |
| 29 | `jar/<id>/claude/<frag>_chunk_plan.json` | `TreeChunker.writeChunkPlan` :363-386 | 1 / endpoint | small | O(#chunks) | bounded-ish |
| 30 | `jar/<id>/corrections/<endpoint>_correction.json` | `CorrectionPersistence.saveCorrection` :46-52 | 1 / endpoint | — | ~1 MB | grows with #nodes corrected |
| 31 | `jar/<id>/corrections/_summary.json` | `CorrectionPersistence.saveSummary` :83-89 | 1 / jar | — | MBs | grows |
| 32 | `jar/<id>/corrections/<...>_corr_chunk<n>_output.json`, `_corr_merged.json`, `_corr_output.json` | `ClaudeCorrectionService` :447, :496, :547 | per endpoint / per chunk | — | MBs | unbounded |
| 33 | `jar/<id>/improvement/run_<jar>_<ts>.md` | `ClaudeCorrectionService.generateImprovementNotes` :244-... | 1 / correction run | — | MBs | **Markdown, not JSON** |
| — | `${java.io.tmpdir}/subtree-cache-*.jsonl`, `jar-classes-*.jsonl` | `SubtreeCache` :45,53-67; `JarParserService` | ephemeral / run | — | GBs | **Not persisted** (deleted in `SubtreeCache.close()` :113-127) |

**16 MB BSON verdict:** only #4, #8, #9, #11, #12, #18, #24 are safe as single Mongo documents unconditionally. #1/#2/#3, #10, #16, #17, #19, #21, #28 can *never* be one document. #5/#6, #15, #22, #23, #30 are "usually fine, occasionally not" and need a split plan.

---

## 2. Detailed artifact specifications

### 2.1 `jar/<jarId>/analysis.json` — the source-of-truth document

- **Writer:** `PersistenceService.writeAnalysisStreamingTo(...)`, `PersistenceService.java:186-239` (streamed with a Jackson `JsonGenerator`, never materialised in RAM). Entry points `writeAnalysisStreaming` :159 (→ `analysis.json`) and :170 (→ `analysis_corrected.json`). A second, non-streaming writer exists: `save(JarAnalysis)` :298-310 (`objectMapper.writeValue`), used for small/whole-object saves.
- **Purpose:** the complete static analysis of one JAR — every parsed class and every discovered HTTP endpoint with its full call tree.
- **Cardinality:** exactly 1 per analysed JAR (plus a corrected twin).
- **Write pattern:** rewritten wholesale, once per analysis run, **after** source decompilation (`JarAnalysisExecutor.java:219-225`: "Write analysis.json ONCE — after decompilation so sourceCode is included").
- **Identity:** `jarName` (== `<jarId>` after `JarNameUtil.normalizeKey`). Everything else in the tree hangs off it.

#### Top-level fields (written literally at `PersistenceService.java:191-237`)

| Field | JSON type | Optional | Meaning |
|---|---|---|---|
| `jarName` | string | no (:192) | Original jar file name; primary key |
| `projectName` | string | **yes** — omitted when null (:193) | Logical project/app name |
| `jarSize` | number (long) | no (:194) | Uploaded jar size in bytes |
| `analyzedAt` | string (ISO-8601 local date-time) | no (:195) | Run timestamp |
| `totalClasses` | number | no (:196) | Count of `classes` |
| `totalEndpoints` | number | no (:197) | Count of `endpoints` |
| `analysisMode` | string | no (:198), defaults `"STATIC"` | `STATIC` \| `CORRECTED` |
| `basePackage` | string | yes — omitted when null/blank (:199) | Package filter applied at upload |
| `scoped` | boolean `true` | yes — only when `scopeTarget` set (:201) | Targeted (single-endpoint/controller) analysis |
| `scopeTarget` | string | yes (:202) | What the run was scoped to |
| `classes` | array\<ClassInfo\> | no (:205-221) — may be `[]` | Raw class lines streamed verbatim from the parser's JSONL temp file (`gen.writeRawValue(line)` :212) |
| `endpoints` | array\<EndpointInfo\> | no (:224-234) | Serialised `EndpointInfo` objects |

The `JarAnalysis` POJO (`model/JarAnalysis.java:9`, `@JsonInclude(NON_NULL)`) adds three more fields that appear only on the `save()`/corrected path: `correctionAppliedAt` (string, nullable), `correctionCount` (number, default 0), `claudeIteration` (number, default 0). Observed in `_header.json` recovery too (`PersistenceService.listJarsFromStore` :553-555).

#### `classes[]` element — `ClassInfo` (`model/ClassInfo.java:6`)

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `fullyQualifiedName` | string | yes | FQN — the natural key within a jar |
| `packageName` | string | yes | Java package |
| `simpleName` | string | yes | Simple name |
| `superClass` | string | yes | Superclass FQN |
| `interfaces` | array\<string\> | no (`[]`) | Implemented interface FQNs |
| `annotations` | array\<AnnotationInfo\> | no (`[]`) | Class-level annotations |
| `fields` | array\<FieldInfo\> | no (`[]`) | Declared fields |
| `methods` | array\<MethodInfo\> | no (`[]`) | **Dominant sub-array** |
| `stereotype` | string | yes | `REST_CONTROLLER` \| `SERVICE` \| `REPOSITORY` \| `ENTITY` \| `OTHER` … |
| `accessFlags` | number | no | JVM access-flag bitmask |
| `isInterface` / `isAbstract` / `isEnum` | boolean | no | Class kind |
| `sourceJar` | string \| null | yes | Bundled jar the class came from (null for main app) |
| `jarName` | string | yes | Owning analysis jar (denormalised) |
| `sourcePath` | string | yes | Reconstructed `src/main/java/...` path |
| `repositoryEntityType` | string \| null | yes | Entity type param for Spring Data repos |

`AnnotationInfo` (`model/AnnotationInfo.java:6`): `name` (string, nullable), `attributes` (object, `{}` default; arbitrary nested values).

`FieldInfo` (`model/FieldInfo.java:6`): `name`, `type` (strings, nullable); `annotations` (array\<AnnotationInfo\>, `[]`); `accessFlags` (number); `constantValue` (string, nullable); `mongoFieldName` (string, nullable — from `@Field`/`@BsonProperty`/`@JsonProperty`); `idField` (boolean — `@Id`/`@BsonId`); `jpaColumnName` (string, nullable); `genericElementType` (string, nullable — element type of a collection field). `effectiveMongoName()`/`effectiveJpaColumnName()` are plain methods and are **not** serialised.

`MethodInfo` (`model/MethodInfo.java:8`):

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `name` | string | yes | Method name (`<init>` for ctors) |
| `descriptor` | string | yes | JVM descriptor — needed with `name` for uniqueness |
| `returnType` | string | yes | Return type FQN |
| `parameters` | array\<ParameterInfo\> | no (`[]`) | |
| `annotations` | array\<AnnotationInfo\> | no (`[]`) | |
| `invocations` | array\<MethodCallInfo\> | no (`[]`) | **Largest single contributor to class size** |
| `accessFlags` | number | no | |
| `httpMethod` | string | yes | Set when the method is an endpoint handler |
| `path` | string | yes | Handler path mapping |
| `stringLiterals` | array\<string\> | no (`[]`) | LDC constants (used for collection/SQL detection) |
| `fieldAccesses` | array\<FieldAccessInfo\> | no (`[]`) | |
| `localVariables` | object\<string,string\> | no (`{}`) | local var name → type |
| `startLine` / `endLine` | number | no | Source line range |

`ParameterInfo` (`model/ParameterInfo.java:6`): `name`, `type` (strings), `annotations` (array\<AnnotationInfo\>).
`FieldAccessInfo` (`model/FieldAccessInfo.java:7`): `ownerClass`, `fieldName`, `fieldType`, `accessType` (`GET`/`PUT`/`GET_STATIC`/`PUT_STATIC`), `lineNumber` (number).
`MethodCallInfo` (`model/MethodCallInfo.java:3`): `ownerClass`, `methodName`, `descriptor`, `returnType` (strings, nullable); `opcode` (number, JVM invoke opcode); `lineNumber` (number); `recentStringArgs` (array\<string\> **or null** — no default init); `receiverFieldName` (string, nullable — DI field holding the receiver); `lambdaInvocation` (boolean, from `isLambdaInvocation`); `lambdaTargetClass`/`lambdaTargetMethod`/`lambdaTargetDescriptor` (strings, nullable); `dynamicArgSources` (array **or null**) of record `DynamicArgSource{kind, name, ownerClass}` (all strings).

Real element sizes (`data_old_1784577715/jar/web-1.0.0-SNAPSHOT/analysis.json`, 119 classes): `classes` array = **2,601,008 bytes**, avg **21.9 KB/class**, max **162 KB/class**; largest class has **65 methods**.

#### `endpoints[]` element — `EndpointInfo` (`model/EndpointInfo.java:17`, `@JsonPropertyOrder` — `callTree` is deliberately ordered late so streaming readers can skip it)

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `httpMethod` | string | yes | GET/POST/… |
| `fullPath` | string | yes | Fully-resolved request path (class + method mapping) |
| `controllerClass` | string | yes | FQN of controller |
| `controllerSimpleName` | string | yes | Simple name — used in the endpoint folder name |
| `methodName` | string | yes | Handler method |
| `returnType` | string | yes | |
| `parameters` | array\<ParameterInfo\> | no (`[]`) | |
| `callTree` | CallNode object | **yes (null possible)** | Root of the resolved call graph (recursive) |
| `fragments` | object\<string, array\<CallNode\>\> | no (`{}`) | Overflow children keyed by `fragmentRef` when a node exceeded the max-children cap |
| `aggregatedCollections` | array\<string\> | no (`[]`) | All collections/tables touched by the whole tree |
| `aggregatedCollectionDomains` | object\<string,string\> | no (`{}`) | collection → business domain |
| `aggregatedOperations` | object\<string,string\> | no (`{}`) | collection → highest-priority operation |
| `derivedOperationType` | string | yes | Highest-priority op across the tree (`READ`/`WRITE`/…) |
| `totalDbOperations`, `totalMethods`, `totalLoc`, `inScopeCalls`, `externalScopeCalls` | number | no (0) | Pre-computed roll-ups (so summary generation can skip the tree) |
| `modules` | array\<string\> | no (`[]`) | Distinct modules/jars traversed |
| `externalCalls` | array\<object\> | no (`[]`) | Observed element keys: `className`, `simpleClassName`, `methodName`, `stereotype`, `sourceJar`, `jarName`, `sourcePath`, `module`, `domain` (all strings) |
| `httpCalls` | array\<object\<string,string\>\> | no (`[]`) | Outbound HTTP/Feign call sites |
| `beans` | array\<object\<string,string\>\> | no (`[]`) | Distinct beans visited |
| `operationTypesList` | array\<string\> | no (`[]`) | Distinct operation types |
| `procNames` | array\<object\> | no (`[]`) | Oracle stored-proc names from `@LogParameters` |
| `dynamicFlows` | array\<object\> | no (`[]`) | Reflection/dynamic-dispatch/dynamic-query flows |
| `aggregationFlows` | array\<object\> | no (`[]`) | Aggregation-pipeline flows (deeply nested maps) |
| `dynamicCollectionFlows` | array\<object\> | no (`[]`) | Call sites with variable (unresolved) collection names |
| `collectionLifecycleFlows` | array\<object\> | no (`[]`) | Multi-step lifecycle workflows (e.g. blue-green swap) |

⚠️ `@JsonPropertyOrder` also names `aggregatedCollectionVerification` and `aggregatedCollectionSources`, but **no such fields or getters exist in `EndpointInfo.java`** — they are produced only by the streaming aggregate computation in `AbstractAnalysisDataProvider` (:583-586 region) and appear in *summary* documents, not in `EndpointInfo` serialisation. Treat as summary-only, optional.

Real element sizes: `endpoints` array = **1,302,544 bytes**, avg **15.5 KB/endpoint**, max **166 KB**; embedded `callTree`s total **933 KB** (avg 11.1 KB, max 135 KB) — *in the old build without embedded `sourceCode`*.

#### `CallNode` (`model/CallNode.java:10`) — recursive, the growth engine

Identity/topology: `id` (string; e.g. `com.x.Y#m(I)Lz;`), `className`, `simpleClassName`, `methodName`, `returnType`, `parameterTypes` (array\<string\>), `stereotype`, `annotations` (array\<string\>), `children` (**array\<CallNode\>, recursive**), `recursive` (boolean), `crossModule` (boolean), `module`, `domain`, `callType` (e.g. `APPLICATION`, `HTTP_CALL`, `FEIGN`), `dispatchType` (`DIRECT`/`DYNAMIC_DISPATCH`/`QUALIFIED`/`HEURISTIC`/`INTERFACE_FALLBACK`, nullable), `resolvedFrom` (nullable), `qualifierHint` (nullable).

Source identity: `sourceJar` (nullable), `jarName`, `sourcePath`, `lineCount` (number), **`sourceCode`** (string, nullable — CFR-decompiled method body; observed ~200 B–2 KB per node, no upper bound).

Data-access facts: `collectionsAccessed` (array\<string\>), `operationType` (string, nullable), `operationTypes` (array\<string\>, from a `Set`), `collectionDomains` (object\<string,string\>), `collectionSources` (object\<string,string\>: `REPOSITORY_MAPPING`/`DOCUMENT_ANNOTATION`/`STRING_LITERAL`/`FIELD_CONSTANT`/`QUERY_ANNOTATION`/`PIPELINE_ANNOTATION`), `collectionVerification` (object\<string,string\> **or null** — `VERIFIED`/`NOT_IN_DB`/`NO_CATALOG`), `hasDbInteraction` (boolean), `entityFieldMappings` (object\<string, object\<string,string\>\> **or null**), `aggregationPipeline` (object **or null**, deeply nested), `sqlStatements` (array\<string\> **or null**), `cacheOperations` (array\<object\> **or null**), `oracleProcRef` (string, nullable — `PROC:PKG.NAME`/`FUNC:PKG.NAME`), `oracleSchema` (string, nullable), `dynamicCollectionRefs` (array\<object\>: `variableName`, `varKind`, `pattern`, `source`, `operationType`), `lifecycleStageTags` (array\<string\>).

Detail/telemetry: `annotationDetails` (array\<object\> with `name` + `attributes`), `stringLiterals` (array\<string\>).

Fragmentation & HTTP linking: `fragmentRef` (string, nullable — points into `EndpointInfo.fragments`), `truncatedChildCount` (number, nullable), `httpTargetMethod`, `httpTargetPath`, `httpTargetModuleHint` (strings, nullable).

`clearTransient()` (`CallNode.java:289-301`) nulls `sourceCode`, `stringLiterals`, `annotationDetails`, `entityFieldMappings`, `cacheOperations`, `aggregationPipeline` recursively — i.e. exactly the fields that are *derived and re-derivable*, and exactly what makes the difference between a 50 MB and a 1 GB `analysis.json`.

#### Size / worst case

Observed `analysis.json`: 19,646 B (demo-app) · 2.64 MB · 3.90 MB · 5.27 MB · 5.80 MB. The 3.90 MB file = 119 classes + 84 endpoints.

Growth drivers, in order:
1. **`endpoints[].callTree[*].sourceCode`** — decompiled Java source embedded at every node. Since `JarAnalysisExecutor.java:215` runs `enrichCallTreesWithSource` *before* the write at :220-225, the current build embeds it. Per-node source averages ~2 KB (measured from `nodes/node_*.json`: n=560, avg 2,050 B). A tree with 5,000 nodes therefore carries ~10 MB of source **per endpoint**; 100 such endpoints = ~1 GB. This is the mechanism behind the reported ~1 GB for a 1,597-class JAR.
2. **`classes[].methods[].invocations[]`** — one object per bytecode call site; 21.9 KB/class average → 1,597 classes ≈ 35 MB before any source.
3. Duplicate subtrees: `children` is a materialised tree, not a DAG, so a shared service method is re-serialised under every caller (`SubtreeCache` exists precisely to bound *memory*, not output).

**Bounded?** No. Must be sharded for MongoDB — one document per class and one per endpoint (and per call-tree node, or trees stored as GridFS/S3 blobs).

#### Readers

- `GET /api/jar/jars/{id}` → `AnalyzerController.getAnalysis` (`AnalyzerController.java:476-488`), streams via `provider.streamAnalysis`. No UI caller found.
- `GET /api/jar/jars/{id}/endpoints/by-index/{idx}/call-tree` → `AnalyzerController.java:490-503` (`streamCallTree`) — **UI: `api.js:113-119`**, used by `endpointList.js:101` (Endpoint Flows tab) and the Summary-tab popups (`summary-explorer.js`, `summary-tables.js`, `summary-trace.js`, `summary-popups.js`, `summary-vert-popup.js`, `summary-export.js`).
- `GET /{id}/classes/tree` and `GET /{id}/classes/by-index/{idx}` → `AnalyzerController.java:652-679` — these stream **from `analysis.json`**, not from `_class-tree.json`. UI: `api.js:99-107` → `codeTree.js:49` (Code Structure tab).
- `DecompileController.extractClassData` (`DecompileController.java:190-205`) streams it for per-class decompile views.

### 2.2 `analysis_corrected.json` / `analysis_corrected_prev.json`

Same schema as 2.1 with `analysisMode="CORRECTED"` and the three correction header fields populated. Written by `PersistenceService.writeAnalysisStreamingTo` via :170 and `saveCorrected` :312-325; the previous generation is **rotated** (`store.move`) by `rotateCorrected` :454-460. Provider selection prefers `analysis_corrected.json` when present (`DecompileController.java:191-193`; `resolveProvider(id, version)` in `AnalyzerController`). Cardinality 0–1 each per jar. Identity: `<jarId>` + version discriminator (`""` | `"corrected"` | `"corrected_prev"`).

### 2.3 `_header.json`

- **Writer:** `PersistenceService.writeHeaderSidecar` :270-294; regenerated lazily on listing if missing (:553-555).
- **Purpose:** cheap jar-list row so the jar picker never opens the multi-hundred-MB analysis.
- **Fields:** `jarName` (string), `projectName` (string, optional :277), `jarSize` (number), `analyzedAt` (string), `totalClasses` (number), `totalEndpoints` (number), `analysisMode` (string), `basePackage` (string, optional :283), `scoped` (boolean, optional), `scopeTarget` (string, optional). On the recovery path it may also carry `correctionAppliedAt`, `correctionCount`, `claudeIteration`.
- **Real sample** (`web-1.0.0-SNAPSHOT/_header.json`, 191 B): `{"jarName":"web-1.0.0-SNAPSHOT.jar","jarSize":70489772,"analyzedAt":"2026-07-19T19:45:20.171790","totalClasses":119,"totalEndpoints":84,"analysisMode":"STATIC","basePackage":"com.enterprise"}`
- **Cardinality:** 1/jar. **Write pattern:** rewritten wholesale. **Bounded:** yes (<1 KB).
- **Derived?** Yes — 100 % derivable from `analysis.json`'s header fields. Pure cache.
- **Reader:** jar list (`AnalyzerController.listJarsFromStore` :553) and `/summary/headers` slice (`AbstractAnalysisDataProvider.java:1191-1248`).

### 2.4 `_summary_static.json` / `_summary.json` (+ `.gz` twins)

- **Writer:** `PersistenceService.preGenerateSummaryCache` :374-406 — obtains a `JsonParser` over `analysis.json` and runs `provider.streamSummary`; writes plain bytes (:394) and a gzip copy (:396-399). `StaticAnalysisProvider` → `_summary_static.json`; `ClaudeAnalysisProvider` (corrected) → `_summary.json`. Both implementations inherit `AbstractAnalysisDataProvider.streamSummary` :378-620.
- **Purpose:** a call-tree-free projection of the whole analysis that the Summary tab can load in one request.
- **Cardinality:** 1 per jar per mode. **Write pattern:** rewritten wholesale; invalidated/deleted by `invalidateSummaryCache` :349-368.
- **Derived?** **Yes — entirely.** Cacheable, never source of truth.

Field tree (verified against the 429,782-byte real file and against `AbstractAnalysisDataProvider.java:502-515`):

| Field | Type | Meaning |
|---|---|---|
| `jarName`, `jarSize`, `analyzedAt`, `totalClasses`, `totalEndpoints`, `analysisMode`, `basePackage` | copied verbatim from analysis header | |
| `classIndex` | array (n=119, 61,121 B) | per-class stub, keys at :409-438: `fullyQualifiedName`, `simpleName`, `packageName`, `stereotype`, `sourceJar` (nullable), `jarName`, `sourcePath`, `isInterface`, `isAbstract`, `isEnum`, `annotations[]` (`{name, attributes?}`), `interfaces[]` (strings), `superClass` |
| `entityCollMap` | object\<string,string\> | entity class → collection/table (empty in sample) |
| `repoCollMap` | object\<string,string\> | repository class → collection/table |
| `scheduledJobs` | array\<object\> | keys at :475-483: `className`, `simpleName`, `methodName`, `stereotype`, `sourceJar`, `jarName`, `sourcePath`, `attributes` |
| `scheduledCount` | number (:510) | |
| `enableSchedulingClasses` | array\<string\> (:511) | classes annotated `@EnableScheduling` |
| `endpoints` | array (n=84, avg 4,384 B, max 30,823 B) | every `EndpointInfo` field **except `callTree`, `fragments`, `sourceCode`** — those are `skipChildren()`-ed at :528-546. Observed keys: `httpMethod, fullPath, controllerClass, controllerSimpleName, methodName, returnType, parameters, aggregatedCollections, aggregatedCollectionDomains, aggregatedOperations, derivedOperationType, totalDbOperations, totalMethods, totalLoc, inScopeCalls, externalScopeCalls, modules, externalCalls, httpCalls, beans, operationTypesList, procNames, dynamicFlows, aggregationFlows`. Newer runs may add `aggregatedCollectionVerification` / `aggregatedCollectionSources` (computed at :578-586). |

**Bounding:** per-endpoint arrays are capped (`CAPPED_ARRAY_FIELDS`, `AbstractAnalysisDataProvider.java:18-26`): `beans` 100, `externalCalls` 100, `httpCalls` 50, `dynamicFlows` 200, `aggregationFlows` 200, `dynamicCollectionFlows` 200, `collectionLifecycleFlows` 100. So each endpoint entry is roughly bounded (~30 KB observed max), but the *document* still grows linearly with #classes + #endpoints. Observed 3.8 KB → 430 KB (gzip 17.7 KB, 24×). A 1,597-class jar with ~1,000 endpoints extrapolates to ~35 MB — **over the 16 MB BSON limit**; treat as splittable.

**Readers:** `GET /{id}/summary` plus slice routes `/summary/headers`, `/summary/external-calls`, `/summary/dynamic-flows`, `/summary/aggregation-flows`, `/summary/beans` (`AnalyzerController.java:507-650`, slice extraction `streamSummarySlice` `AbstractAnalysisDataProvider.java:1191`). UI: `api.js:39-96` → `app.js`, `summary.js:478-484`, `targeted.js:201`, `summary-claude.js:558`, `summary-corrections.js:412` — **Summary tab**.

### 2.5 `_class-tree.json` / `_class-tree_<version>.json` — **dead key**

`JarDataPaths.java:148-153` and `:257-269` define the key/path, and `PersistenceService.invalidateSummaryCache` :356-366 deletes it (including `_class-tree.json.tmp` and every `_class-tree_*` variant). **No writer and no reader exists in `jar-analyzer-core/src/main` or `unified-web/src/main`** (grep for `classTreeCache`/`class-tree` returns only `JarDataPaths`, the deletion code, and tests). The Code Structure tab is served by `GET /{id}/classes/tree`, which streams `analysis.json` live (`AnalyzerController.java:652-664`). Conclusion: a planned cache that was never wired up — do **not** model it, but do keep it in mind as the natural home for a derived class tree once analysis.json is split.

### 2.6 `connections.json`

- **Writer:** `PersistenceService.storeConnections` :93-95; content built by `MongoCatalogService.extractAllConnectionInfo` :377-405.
- **Fields:** `extractedAt` (string); `mongodb` (object, optional: `uri` — **credential-masked** by `maskUri` :494-497, `database` optional); `oracle` (object, optional: `jdbcUrl`); `postgres` (object, optional: `jdbcUrl`).
- **Real sample (130 B):** `{"extractedAt":"2026-07-19T19:45:19.964025","oracle":{"jdbcUrl":"jdbc:oracle:thin:@//localhost:1521/XEPDB1"}}`
- 1/jar, rewritten wholesale, bounded (<2 KB). Reader: `GET /{id}/connections` (`AnalyzerController.java:1096-1102`) → `api.js:63-66`, `app.js:773,798`.

### 2.7 `domain-config.json`

- **Writer:** `PersistenceService.storeDomainConfig` :73-81 (per-jar upload), or a server-side copy of the global config (`JarAnalysisExecutor.java:196`, `store.copy`).
- **Purpose:** the classification rules the analysis ran with (audit snapshot + editable override).
- **Top-level keys (real file, 7,818 B):** `base_packages` (array), `collection_pattern` (string), `module_types` (object), `view_detection` (object), `snapshot_buffer_domain_map` (object), `operation_types` (object), `jar_name_ignore_segments` (array), `endpoint_classification` (object), `http_client_classes` (array), `repository_suffixes` (array), `query_annotations` (array), `size_thresholds` (object), `complexity_rules` (object), `performance_thresholds` (array), `implicit_collections` (object). Inner shapes are user-authored config (see `DomainConfigLoader.java`, which only *reads* it — `StageConfig`/`WorkflowConfig` records at :50,:53).
- 1/jar, rewritten on upload, bounded (~8 KB). Reader: `GET/POST /{id}/domain-config` (`AnalyzerController.java:270,297-336`) → `api.js:69-72`, `app.js:882,897`.

### 2.8 `bundled-jars.json` — an unbounded array at top level

- **Writer:** `PersistenceService.storeBundledJarInfo` :674-686; elements are `JarParserService.BundledJarInfo` (`JarParserService.java:351-355`).
- **Shape:** a **bare JSON array** (not an object) of:
  - `name` — string, bundled jar file name
  - `size` — number, bytes
  - `manifest` — string, the entire `MANIFEST.MF` text
  - `resources` — object\<string,string\>, **file name → full file content** (e.g. `pom.xml` embedded verbatim)
- **Real data:** 94 elements, **3,028,400 bytes** total (~32 KB/element average) for one jar. The `resources` map is what makes this large.
- 1/jar, rewritten wholesale. **Unbounded** — scales with the number of bundled jars × size of their text resources. A fat Spring Boot uber-jar with 300 dependencies would be ~10 MB+, so this is at genuine risk of the 16 MB BSON limit.
- Identity: `<jarId>` + `name`. Reader: `GET /{id}/bundled-jars` (`AnalyzerController.java:1343-1346`) → `codeTree.js:827,856-916` (Code Structure tab).

### 2.9 `jar-dep-map.json`

- **Writer:** `PersistenceService.storeJarDepsMap` :702-713.
- **Shape:** object mapping *dependency jar file name* → array\<string\> of **simple** app class names that reference it. Real file 1,100 B, 11 keys.
- 1/jar, rewritten wholesale. Grows as O(#dependency jars × #referencing classes) — a few MB worst case, still one document. Reader: `GET /{id}/jar-dep-map` (`AnalyzerController.java:1348-1351`) → `codeTree.js:842,950-982`.

### 2.10 `mongo-catalog.json`

- **Writer:** `MongoCatalogService.storeCatalog` :420-431 (only caller: :474-476).
- **Shape:** record `MongoCatalog` (`MongoCatalogService.java:505-513`): `database` (string), `maskedUri` (string, credentials redacted), `collections` (array\<string\>), `views` (array\<string\>), `totalCollections` (number), `totalViews` (number), `fetchedAt` (string).
- **Status:** `fetchCatalog` (:413) currently returns null — "Live MongoDB connections are disabled" — so in practice this file is **never written today**. Confirmed absent from all five on-disk analyses.
- 0–1/jar, rewritten wholesale, bounded (<1 MB). Used to fill `CallNode.collectionVerification`. Reader: `GET /{id}/catalog`, `POST /{id}/fetch-catalog` (`AnalyzerController.java:751-813`) → `api.js:259-267`, `summary-tables.js:696`.

### 2.11 `external-links.json` and `external-links-pending.json`

- **Writer:** `ExternalApiLinkService.writeResult` (`unified-web/.../extapi/ExternalApiLinkService.java:274-291`); invoked from `JarAnalysisExecutor.java:234-236` (`resolve` + `reresolvePending`).
- **Purpose:** cross-JAR Java→Java links from `HTTP_CALL`/`FEIGN` leaf nodes in this jar to endpoints in *another* analysed jar (link, not splice).
- **`external-links.json`:** `{ jarId: string, resolved: array<callSite+target>, ambiguous: array<callSite + candidates[]> }` (:276-278).
- **`external-links-pending.json`:** `{ jarId: string, pending: array<callSite> }` (:286-287).
- **callSite element** (:215-220): `callerEndpoint` (string), `callerClassName`, `callerMethodName`, `httpMethod`, `path`, `moduleHint` (optional).
- **target fields** merged into resolved entries (:261-265): `targetJarId`, `targetControllerSimpleName`, `targetMethodName`, `targetHttpMethod`, `targetPath`. `ambiguous` entries instead carry `candidates` = array of those same target objects (:237).
- Cardinality 0–1 each per jar; rewritten wholesale on every resolve pass — note this means **another jar's analysis rewrites this jar's file** (`reresolvePending`), a cross-document write dependency worth modelling explicitly.
- Identity: `<jarId>`; foreign keys → `targetJarId` + endpoint identity. Reader: `ExternalApiLinkController.java:30-51` (`/api/jar/jars/{id}/...`). **No UI caller found.**

### 2.12 `endpoints/<NNN>_<Controller>.<method>/…` — the per-endpoint tree

Folder name: `String.format("%03d_%s", index, sanitize(controllerSimpleName + "." + methodName))` (`EndpointOutputWriter.java:40-42`). The 3-digit index is a **1-based ordinal within the run** — it is *not* stable across re-analyses, which makes it a poor primary key; the stable identity is `(jarId, controllerClass, methodName, httpMethod, fullPath)`.

Real data: 84 endpoint folders, **5.4 MB total**, 812 JSON files.

#### 2.12.1 `endpoint.json` — 1 per endpoint

Writer `EndpointOutputWriter.java:45-80`. Keys, in write order: `httpMethod`, `fullPath`, `controllerClass`, `controllerSimpleName`, `methodName`, `returnType`, `parameters` (array\<ParameterInfo\>), `sourceJar` (string, `""` when absent :55), `jarName` (:56), `sourcePath` (:57), `analyzedAt` (string, `LocalDateTime.now()` :58), `aggregatedCollections`, `aggregatedCollectionDomains`, `aggregatedOperations`, `derivedOperationType`, `totalDbOperations`, `totalMethods`, `totalLoc`, `inScopeCalls`, `externalScopeCalls`, `modules`, `externalCalls`, `httpCalls`, `beans`, `operationTypesList`, `procNames`, `dynamicFlows`, `aggregationFlows`, `dynamicCollectionFlows`, `collectionLifecycleFlows` (types as in §2.1's `EndpointInfo`). Note **no `callTree`** and **no `fragments`**.

Real size: 9,945 B for `001_ReportController.monthlySales`. Written once (pretty-printed). **Duplicate** of the corresponding `analysis.json` `endpoints[]` element minus the tree, plus `analyzedAt`/`sourceJar`/`jarName`/`sourcePath`. Read server-side by `ExternalApiLinkService.java:133` and `CrossRefService.java:249`; no direct HTTP route.

#### 2.12.2 `call-tree.json` — 1 per endpoint

Writer `EndpointOutputWriter.java:82-83`: the raw `CallNode` root, pretty-printed — i.e. exactly the §2.1 `CallNode` field tree, recursively. Real sizes: avg **31 KB**, max **178 KB** (`062_AnalysisController.runComprehensive`). Contains `sourceCode` at every node. **Unbounded** (depth × breadth × source). Reader: `GET /api/jar/jars/{id}/endpoints/{endpoint}/call-tree` (`DecompileController.java:177-188`) — the UI instead uses the by-index route that streams `analysis.json` (§2.1), so this file is currently API-only.

#### 2.12.3 `nodes.json` — 1 per endpoint (flattened tree)

Writer `EndpointOutputWriter.java:101-106`. Shape: `{ endpoint: string ("<Controller>.<method>"), totalNodes: number, nodes: array<FlatNode> }`.

`FlatNode` keys (written at :128-181, verified against real `nodes.json`): `nodeId` (number, DFS ordinal), `depth` (number), `id`, `signature` (string, from `TreeChunker.nodeSignature` — e.g. `com.x.Y.m(int):ResponseEntity`), `className`, `simpleClassName`, `methodName`, `parameterTypes` (array\<string\>), `returnType`, `lineCount` (number), `stereotype`, `annotations` (array\<string\>), `annotationDetails` (array\<object\>), `sourceJar` (nullable), `jarName`, `sourcePath`, `sourceCode` (string, nullable), `callType`, `recursive` (boolean), `crossModule` (boolean), `module`, `domain`, `childCount` (number), `dispatchType`/`resolvedFrom`/`qualifierHint` (nullable), `httpTargetMethod`/`httpTargetPath`/`httpTargetModuleHint` (nullable), `operationType`, `operationTypes` (array), `collectionsAccessed` (array), `collectionDomains` (object), `collectionSources` (object), `collectionVerification` (object, nullable), `hasDbInteraction` (boolean), `dynamicCollectionRefs` (array), `lifecycleStageTags` (array), `entityFieldMappings` (object, nullable), `aggregationPipeline` (object, nullable), `cacheOperations` (array, nullable), `sqlStatements` (array, nullable), `oracleProcRef`/`oracleSchema` (nullable), `stringLiterals` (array), `fragmentRef` (nullable), `truncatedChildCount` (number, nullable).

Real sizes: avg **31 KB**, max **175 KB**; a 15-node endpoint produced 31,373 B. **Unbounded**. Identity of an element: `(jarId, endpointFolder, nodeId)`; `childCount`+`depth` reconstruct topology (parent link is implicit in DFS order — worth making explicit if relationalised). Reader: `GET /{id}/endpoints/{endpoint}/nodes` (`DecompileController.java:152-161`) → `summary-trace.js:150,306` (Summary tab trace viewer).

#### 2.12.4 `nodes/node_<nodeId>.json` — 1 per node

Writer `EndpointOutputWriter.java:108-113`: the same `FlatNode` map, one file each. Real data: **560 files** across 84 endpoints, avg **2,050 B**, max **6,177 B**, total 1.15 MB. Each is individually **bounded** (only a single decompiled method) — the natural Mongo document grain. Reader: `GET /{id}/endpoints/{endpoint}/nodes/{nodeId}` (`DecompileController.java:164-176`). **100 % duplicated** inside `nodes.json`.

#### 2.12.5 `fragments/<fragmentKey>.json` — 0..N per endpoint

Writer `EndpointOutputWriter.java:90-94`. Shape: `{ "children": array<CallNode> }` — the overflow children of a node whose fan-out exceeded the per-node cap; referenced by `CallNode.fragmentRef` + `truncatedChildCount`. None present in the sample data. Unbounded, same content model as `call-tree.json`.

### 2.13 `resources/<filename>`

Writer `PersistenceService.storeResourceFiles` :632-648 (raw bytes, `..` traversal guarded). Not JSON — extracted config/DDL (`application.yml`, `V1..V8__*.sql` in the sample; 1.8 KB–19.8 KB each, 9 files). N per jar, written once. Reader: `GET /{id}/resources[/{filename}]` (`AnalyzerController.java:1311-1341`) → `codeTree.js:148`.

### 2.14 `stored.jar`

Writer `PersistenceService.storeJar` :57-65. Binary blob; the original upload, kept because the decompiler and `JarFile` readers need a real local file (`JarAnalysisExecutor.java:209-213` uses `store.materialize`). Observed **433 KB – 70.5 MB**; dominates on-disk footprint (161 MB total for 5 analyses, of which ~142 MB is `stored.jar`). 1/jar, written once. **Never a Mongo document** — GridFS/object storage.

### 2.15 `chat/history.jsonl`

Writer `ChatHistoryService.append` :41-67. **JSONL**, one object per line: `{ "role": string, "content": string, "ts": string(ISO instant) }` (:44-47). Read back by `load` :69-92 (which drops `ts`). Cardinality 1/jar; logically appended, but physically a **read-modify-write of the entire file** (:49-66) because object stores cannot append. Hard cap `MAX_HISTORY_BYTES` = 10 MB, oldest lines evicted (:55-58) — so **bounded by construction**, but 10 MB is uncomfortably close to 16 MB BSON with overhead; model as one document per message. Reader: `ChatboxController.java:59-73` (`/api/jars/{id}/chat…`) → `chatbox.js:26,203` (global chat widget).

### 2.16 Claude enrichment artifacts (`jar/<id>/claude/**`)

| Key | Writer | Shape | Pattern |
|---|---|---|---|
| `_tracker.json` | `ClaudeEnrichmentTracker.persistState` :160-170 (map built by `stateToMap` :133-150) | `status` (`IDLE`/`RUNNING`/`COMPLETE`/`FAILED`), `totalEndpoints` (number), `completedEndpoints` (number), `startedAt`/`completedAt`/`lastUpdatedAt` (strings), `endpoints` (object\<string, `PENDING`/`PROCESSING`/`DONE`/`ERROR`\>), `errors` (array\<string\>) | rewritten on every state transition |
| `_meta.json` | `FragmentStore.writeMetaSafe` :96-109 | `jarName`, `mode`, `totalEndpoints`, `processedEndpoints`, `timestamp` | rewritten repeatedly |
| `_index.json` | `FragmentStore.writeIndex` :63-74 | `generatedAt`, `totalFragments`, `fragments` (object traceId → caller-supplied detail map + `file`, `traceId`) | written once per index build |
| `_call_log.jsonl` | `ClaudeCallLogger.logCall` :55-88 | per line: `ts`, `sessionId`, `sessionType`, `endpoint`, `chunkIndex`, `promptSizeKb`, `responseSizeKb`, `durationMs`, `success`, `error` | in-memory list re-serialised in full each call (no cap found) |
| `_checkpoint.json` | `ClaudeAnalysisService.java:397` | resume state | rewritten |
| `_cluster_plan.json` | `SwarmClusterer.java:325` | cluster decomposition | once |
| `_cluster_<n>_context.json` | `ClaudeAnalysisService.java:434` | per-cluster prompt context | once per cluster |
| `<trace>_input.json` / `<trace>_output.json` / `<trace>_assembled.json` | `ClaudeAnalysisService.java:212,236,272,321,474-640` | raw LLM prompt / response payloads | once per endpoint (and per chunk) |
| `<frag>_chunk_plan.json` | `TreeChunker.writeChunkPlan` :363-386 | `endpoint`, `totalChunks`, `chunks[]` (`chunk`, `type`, `label`, `chars`, `nodes`) | once per endpoint |

All of these are **operational/audit** data, not analysis truth. The `_input`/`_output` fragments are unbounded (they contain serialised call-tree slices) and are the second-largest disk consumer after `analysis.json` in Claude runs. Reader: `DecompileController.java:124-145` serves individual fragments; no UI caller found.

### 2.17 Correction artifacts (`jar/<id>/corrections/**`)

- **`<sanitizedEndpointKey>_correction.json`** — writer `CorrectionPersistence.saveCorrection` :46-52; serialises `CorrectionResult` (`model/CorrectionResult.java:10`), 1 per endpoint, overwritten on rerun.
  - `endpointName` — string
  - `corrections` — array **or null** of `NodeCorrection` (`:26`): `nodeId` (string → FK to `CallNode.id`), `collections` (`CollectionCorrections` or null), `operationType` (string), `operationTypeReason` (string)
    - `CollectionCorrections` (`:45`): `added` (array\<CollectionEntry\> or null), `removed` (array\<string\> or null), `verified` (array\<string\> or null)
    - `CollectionEntry` (`:61`): `name`, `source`, `operation` (strings)
  - `endpointSummary` — object or null (`:77`): `allCollections` (array\<string\>), `primaryOperation` (string), `crossModuleCalls` (array\<string\>), `confidence` (number, double)
- **`_summary.json`** — writer `CorrectionPersistence.saveSummary` :83-89; a caller-supplied `Map<String,Object>` roll-up. 1/jar, rewritten wholesale. **UNKNOWN — needs runtime check** for its exact key set (constructed at the call site, not in `CorrectionPersistence`).
- **`<method>_corr_chunk<n>_output.json` / `_corr_merged.json` / `_corr_output.json`** — `ClaudeCorrectionService.java:447,496,547`; raw LLM correction payloads, per endpoint/chunk.
- Readers: `/{id}/corrections`, `/{id}/corrections/{endpoint}`, `/{id}/correction-logs*`, `/{id}/run-logs*` (`AnalyzerController.java:1104-1269`) → `api.js:207-254`, `summary-corrections.js:339`, `summary-corr-logbrowser.js:18,118` (Summary tab correction-log viewer).

### 2.18 `improvement/run_<jar>_<ts>.md`

Writer `ClaudeCorrectionService.generateImprovementNotes` :244-... — **Markdown**, not JSON; a human-readable diff of what corrections changed. No API route and no UI reference found anywhere. 1 per correction run; safe to leave on object storage.

### 2.19 Non-persisted temp files (do not model)

- `SubtreeCache` — `Files.createTempFile("subtree-cache-", ".jsonl")` (`SubtreeCache.java:45`), appended via `RandomAccessFile` (:53-67), deleted in `close()` (:113-127). Purely a RAM-relief spill file for call-tree construction.
- `JarParserService` / `WarParserService` / `RepositoryStereotypeResolver` — `jar-classes-*.jsonl`, `war-classes-*.jsonl`, `jar-classes-repo-patched-*.jsonl`. The first of these **is** the byte-for-byte source of `analysis.json`'s `classes[]` array (streamed through with `writeRawValue`, `PersistenceService.java:212`).
- `CallGraphIndex`, `ProgressService`, `DomainConfigLoader` write nothing.

---

## 3. Bounded vs unbounded (16 MB BSON decision table)

**Never one document:**
`analysis.json`, `analysis_corrected*.json` (up to ~1 GB), `stored.jar` (up to 70 MB observed), `bundled-jars.json` (3 MB observed, 10s of MB plausible), per-endpoint `call-tree.json` / `nodes.json` / `fragments/*.json`, Claude `_input`/`_output` fragments.

**Safe as one document (hard-bounded):**
`_header.json` (<1 KB), `connections.json` (<2 KB), `domain-config.json` (~8 KB), `mongo-catalog.json`, `claude/_meta.json`, individual `nodes/node_<n>.json` (max 6.2 KB observed), individual `corrections/<ep>_correction.json`, individual chat messages.

**Grows with input but usually fits — needs a cap or split plan:**
`_summary_static.json` (430 KB at 119 classes / 84 endpoints → ~35 MB extrapolated at 1,597 classes / 1,000 endpoints), `jar-dep-map.json`, `external-links*.json`, `claude/_tracker.json` (O(#endpoints)), `chat/history.jsonl` (capped at 10 MB), `claude/_call_log.jsonl` (uncapped).

## 4. Large embedded arrays — the split candidates

| Parent | Array | Observed count | Observed avg element | Observed total | Recommended grain |
|---|---|---|---|---|---|
| `analysis.json` | `classes[]` | 119 | **21.9 KB** (max 162 KB) | 2.60 MB | one doc per class |
| `ClassInfo` | `methods[]` | up to 65 per class | — | — | embed (bounded per class) unless a class is pathological |
| `MethodInfo` | `invocations[]` | 1..1000s | ~200 B each | — | embed in method |
| `analysis.json` | `endpoints[]` | 84 | **15.5 KB** (max 166 KB) | 1.30 MB | one doc per endpoint |
| `EndpointInfo` | `callTree` (recursive `children[]`) | 84 trees | **11.1 KB** (max 135 KB) *without* embedded source | 933 KB | **one doc per call-tree node** (this is the ~1 GB driver once `sourceCode` is embedded) |
| `EndpointInfo` | `fragments{}` | 0 in sample | — | — | with the node docs |
| `nodes.json` | `nodes[]` | 560 nodes / 84 eps (avg 6.7, max ~90) | **2.05 KB** (max 6.2 KB) | 1.15 MB | one doc per node — already materialised as `nodes/node_*.json` |
| `bundled-jars.json` | root array | 94 | **32 KB** | 3.03 MB | one doc per bundled jar; `resources` map itself may need splitting |
| `_summary_static.json` | `classIndex[]` | 119 | 514 B | 61 KB | one doc per class stub, or keep embedded |
| `_summary_static.json` | `endpoints[]` | 84 | **4.4 KB** (max 30.8 KB) | 368 KB | one doc per endpoint summary |
| `jar-dep-map.json` | values | 11 keys | ~100 B | 1.1 KB | embed |

## 5. Derived / cacheable (not source of truth)

| Artifact | Derived from | Regeneration cost |
|---|---|---|
| `_header.json` | `analysis.json` header fields | trivial |
| `_summary_static.json`, `_summary.json` (+ `.gz`) | full streaming pass over `analysis.json` (`AbstractAnalysisDataProvider.streamSummary` :378) | one full read of a multi-hundred-MB file |
| `_class-tree*.json` | would be derived — **never written today** (§2.5) | — |
| `endpoints/**/endpoint.json` | `analysis.json` `endpoints[]` (minus tree) | cheap |
| `endpoints/**/call-tree.json` | `analysis.json` `endpoints[i].callTree` | cheap |
| `endpoints/**/nodes.json` | flatten of the same tree (`flattenCallTree` :121) | cheap |
| `endpoints/**/nodes/node_*.json` | split of `nodes.json` | cheap |
| `CallNode.sourceCode`, `stringLiterals`, `annotationDetails`, `entityFieldMappings`, `cacheOperations`, `aggregationPipeline` | re-derivable from `stored.jar` (decompiler) — the fields `clearTransient()` :289-301 discards | expensive but possible |
| `improvement/*.md` | `corrections/*` | cheap |
| `claude/_meta.json`, `_tracker.json`, `_call_log.jsonl` | run telemetry, no downstream truth | n/a |

**Genuine sources of truth:** `stored.jar`, `analysis.json` (as the only place the raw parse survives), `domain-config.json`, `corrections/*_correction.json` (LLM verdicts), `chat/history.jsonl`, `external-links*.json` (cross-jar resolution results), `connections.json`.

## 6. Duplication map

1. **Endpoint metadata** appears in three places: `analysis.json.endpoints[i]`, `endpoints/<NNN>/endpoint.json`, and `_summary_static.json.endpoints[i]` — near-identical field sets (§2.1, §2.12.1, §2.4). Only `endpoint.json` adds `analyzedAt`/`sourceJar`/`jarName`/`sourcePath`; only the summary caps its arrays.
2. **Call trees** appear four times: `analysis.json.endpoints[i].callTree`, `endpoints/<NNN>/call-tree.json` (same tree), `endpoints/<NNN>/nodes.json` (flattened), `endpoints/<NNN>/nodes/node_*.json` (per node). In the sample: 933 KB embedded + 2.6 MB in `call-tree.json` + 2.6 MB in `nodes.json` + 1.15 MB in `nodes/` — the same information stored ~4×.
3. **Class metadata**: `analysis.json.classes[i]` (full) vs `_summary_static.json.classIndex[i]` (stub) — the stub is a strict subset.
4. **Analysis header**: `analysis.json` top-level == `_header.json` == `_summary_static.json` top-level.
5. **`analysis.json` vs `analysis_corrected.json`**: whole-document duplication of everything the correction did *not* change, and `_prev` makes it three copies.
6. **Decompiled source**: `CallNode.sourceCode` stored in `analysis.json`, `call-tree.json`, `nodes.json` and `node_*.json` — and re-derivable from `stored.jar`, which is also kept.
7. **Domain config**: the global config is copied into every jar folder (`JarAnalysisExecutor.java:196`).
8. Within a single call tree, a shared method is re-serialised under every caller (`children` is a tree, not a DAG).

## 7. Identity / foreign-key summary

| Entity | Natural key | References |
|---|---|---|
| Analysis | `jarId` (= `JarNameUtil.normalizeKey(jarName)`) + version (`""`/`corrected`/`corrected_prev`) | — |
| Class | (`jarId`, `fullyQualifiedName`) | `sourceJar` → bundled jar `name`; `superClass`/`interfaces[]` → other classes |
| Method | (`jarId`, `fullyQualifiedName`, `name`, `descriptor`) | `invocations[].ownerClass` → class |
| Endpoint | (`jarId`, `controllerClass`, `methodName`, `httpMethod`, `fullPath`) — the on-disk folder ordinal `NNN` is **run-order, not stable** | `controllerClass` → class |
| Call-tree node | (`jarId`, endpoint identity, `nodeId`) — `CallNode.id` is a method signature and repeats across a tree | `className`/`methodName` → method; `fragmentRef` → fragment; `httpTarget*` → another jar's endpoint |
| Correction | (`jarId`, `endpointName`), then `corrections[].nodeId` → `CallNode.id` | |
| Bundled jar | (`jarId`, `name`) | ← `ClassInfo.sourceJar`, `jar-dep-map` keys |
| External link | (`jarId`, callerEndpoint, callerClassName, callerMethodName, path) | `targetJarId` + target endpoint identity |
| Chat message | (`jarId`, `ts`) — no id field; ordering is file order | |

## 8. Open items

- `corrections/_summary.json` exact key set — **UNKNOWN — needs runtime check** (built by the caller of `CorrectionPersistence.saveSummary` :83).
- `_summary.json` (Claude mode) real shape — no sample on disk; assumed identical to `_summary_static.json` plus `aggregatedCollectionVerification`/`aggregatedCollectionSources` because both providers share `AbstractAnalysisDataProvider.streamSummary`. **Needs a corrected-mode run to confirm.**
- `external-links*.json`, `mongo-catalog.json`, `chat/history.jsonl`, all `claude/**` and `corrections/**` artifacts: **no real samples exist on disk** — field lists above are code-derived only.
- Exact node count and `sourceCode` volume of the 1,597-class / ~1 GB run — **UNKNOWN — needs runtime check**; the mechanism is established (§2.1) but the per-node figures above come from an 84-endpoint / 560-node sample.
