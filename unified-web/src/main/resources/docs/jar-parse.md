# JAR Analysis — Internal Flow

> **Version:** 1.0
> **Last Updated:** 2026-05-17

Technical reference for the full pipeline that turns an uploaded `.jar` or `.war` into an `analysis.json` on disk. Covers every phase from HTTP upload to optional Claude enrichment.

---

## Table of Contents

1. [Flow Diagram](#1-flow-diagram)
2. [Phase 0 — Upload & Queue](#2-phase-0--upload--queue)
3. [Phase 1 — JAR Parsing (ASM)](#3-phase-1--jar-parsing-asm)
4. [Phase 2 — Config & Catalog Extraction](#4-phase-2--config--catalog-extraction)
5. [Phase 3 — Call Graph Index & Endpoint Detection](#5-phase-3--call-graph-index--endpoint-detection)
6. [Phase 4 — Enriched Call Trees](#6-phase-4--enriched-call-trees)
7. [Phase 5 — Persistence](#7-phase-5--persistence)
8. [Phase 5.6 — Source Enrichment (CFR)](#8-phase-56--source-enrichment-cfr)
9. [Optional: Claude Enrichment](#9-optional-claude-enrichment)
10. [Output File Layout](#10-output-file-layout)
11. [analysis.json Structure](#11-analysisjson-structure)
12. [Tips](#12-tips)

---

## 1. Flow Diagram

<div class="um-flow">

<div class="um-flow-start">User uploads .jar / .war</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-upload">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">0</span>
    POST /api/analyzer — Upload &amp; Queue
  </div>
  <div class="um-flow-phase-body">
    <ul>
      <li>Validates <code>.jar</code> / <code>.war</code> extension (up to 2 GB)</li>
      <li>Streams file bytes to a temp path — HTTP thread is never blocked</li>
      <li>Optional per-JAR <code>domain-config.json</code> saved alongside</li>
      <li>Submits <strong>QueueJob (Type=JAR_UPLOAD)</strong> and returns <code>{"status":"queued","jobId":…}</code> immediately</li>
      <li>Browser polls SSE <code>/api/queue/{jobId}/progress</code> for live updates</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-parse">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">1</span>
    JAR Parsing — BytecodeClassParser (ASM Tree API)
  </div>
  <div class="um-flow-phase-body">
    For each <code>.class</code> entry in the JAR (<code>BOOT-INF/classes/</code> for fat JARs):
    <ul>
      <li><strong>ClassInfo</strong> — FQN, package, superClass, interfaces, accessFlags, stereotype, annotations</li>
      <li><strong>FieldInfo</strong> — name, type, annotations (<code>@Autowired</code>, <code>@Value</code>, <code>@Field</code>), genericElementType, constantValue</li>
      <li><strong>MethodInfo</strong> — name, descriptor, returnType, parameters, HTTP mapping annotations, all invocation call sites</li>
      <li>Nested JARs in <code>BOOT-INF/lib/</code> parsed only if they contain base-package classes (avoids library noise)</li>
      <li>Each ClassInfo serialised as a <strong>JSON line</strong> to a temp <code>.jsonl</code> file — heap never holds all classes at once</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-db">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">2</span>
    Config &amp; Catalog Extraction
  </div>
  <div class="um-flow-phase-body">
    <ul>
      <li>Scans JAR entries for <code>application.properties</code> / <code>application.yml</code> / <code>bootstrap.yml</code></li>
      <li><strong>MongoCatalogService</strong> — parses MongoDB URI, connects, calls <code>listCollectionNames()</code> → stored as <code>mongo-catalog.json</code></li>
      <li>Connection info (MongoDB URI, Oracle JDBC) → <code>connections.json</code></li>
      <li>Catalog results passed to CallGraphService for collection cross-checking in Phase 4</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-index">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">3</span>
    Call Graph Index &amp; Endpoint Detection
  </div>
  <div class="um-flow-phase-body">
    <strong>CallGraphService.buildIndex()</strong> — streams all classes from <code>.jsonl</code>, populates:
    <ul>
      <li><code>classMap</code> — FQN → ClassInfo</li>
      <li><code>interfaceImplMap</code> — interface FQN → concrete implementors (BFS-expanded)</li>
      <li><code>fieldTypeMap</code> — ownerClass → fieldName → declared type (for @Autowired resolution)</li>
    </ul>
    <strong>buildEndpointsFromIndex()</strong> — finds all CONTROLLER-stereotype classes, inspects each method for <code>@GetMapping / @PostMapping / @PutMapping / @DeleteMapping / @PatchMapping</code>, combines class-level base path with method path → <strong>EndpointInfo</strong> list
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-graph">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">4</span>
    Enriched Call Trees — CollectionResolver &amp; Domain Classification
  </div>
  <div class="um-flow-phase-body">
    <strong>CallTreeBuilder.buildCallTree()</strong> — BFS/DFS per endpoint, limits: maxDepth=30, maxChildren=30, maxNodes=2000
    <ul>
      <li>Each invocation owner resolved: field type lookup → interface impl → superclass chain</li>
      <li><strong>CollectionResolver</strong> — detects MongoDB collections at each CallNode via: repository method names, MongoTemplate calls, <code>@Query</code> values, constant String fields; cross-checked against <code>mongo-catalog.json</code></li>
      <li><strong>Domain classification</strong> — <code>domain-config.json</code> prefix rules assign domain label per collection</li>
      <li><strong>AggregationDetector</strong> — finds Aggregation pipeline construction patterns</li>
      <li><strong>JpaMethodDetector / MongoMethodDetector</strong> — supplementary query pattern detection</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-source">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">5.6</span>
    Source Enrichment — CFR Decompilation
  </div>
  <div class="um-flow-phase-body">
    <strong>SourceEnrichmentService</strong> runs before writing <code>analysis.json</code>:
    <ul>
      <li>Walks every CallNode in every endpoint tree</li>
      <li><strong>DecompilerService</strong> extracts raw <code>.class</code> bytes from stored JAR → runs CFR <code>CfrDriver</code> API → captures Java source</li>
      <li>Individual method source extracted by scanning for method signature in full class source</li>
      <li>LRU cache (max 1500 entries) avoids re-decompiling the same class</li>
      <li>Fixed thread pool: min(4, availableProcessors) with 30s per-class timeout</li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-phase um-phase-persist">
  <div class="um-flow-phase-header">
    <span class="um-flow-phase-num">5</span>
    Persistence — PersistenceService
  </div>
  <div class="um-flow-phase-body">
    All files written under <code>data/{jarName}/</code>:
    <ul>
      <li><code>storeJar()</code> → <code>stored.jar</code></li>
      <li><code>storeDomainConfig()</code> → <code>domain-config.json</code></li>
      <li><code>writeAnalysisStreaming()</code> → <code>analysis.json</code> (JsonGenerator streaming — no full object in heap)</li>
      <li><code>writeEndpointOutputs()</code> → <code>endpoints/{ep}/</code></li>
      <li><code>extractResourceFiles()</code> → <code>resources/</code></li>
      <li><code>extractBundledJarInfo()</code> → <code>bundled-jars.json</code></li>
      <li><code>buildJarDependencyMap()</code> → <code>jar-deps.json</code></li>
      <li><code>storeConnections()</code> → <code>connections.json</code></li>
    </ul>
  </div>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-optional">
  <div class="um-flow-optional-label">Optional — mode=claude</div>
  <strong>Claude Enrichment</strong> — follow-up <code>CLAUDE_ENRICH</code> job enqueued automatically
  <ul>
    <li>Chunks endpoints into batches, writes prompt files to <code>endpoints/{ep}/</code></li>
    <li>Invokes Claude CLI subprocess via <code>ClaudeProcessRunner</code></li>
    <li><code>CorrectionMerger</code> applies corrections → <code>analysis_corrected.json</code> (original preserved)</li>
  </ul>
</div>

<div class="um-flow-arrow"></div>

<div class="um-flow-output">
  <div class="um-flow-output-title">✓ Analysis Complete — Output Files</div>
  <ul>
    <li><code>analysis.json</code> — full static analysis (classes + endpoints + call trees)</li>
    <li><code>analysis_corrected.json</code> — Claude-enriched version (if Claude mode)</li>
    <li><code>domain-config.json</code> — snapshot of config used for this run</li>
    <li><code>mongo-catalog.json</code> — live collection names from MongoDB</li>
    <li><code>connections.json</code> — MongoDB/Oracle connection info</li>
    <li><code>bundled-jars.json</code> / <code>jar-deps.json</code> — nested JAR inventory</li>
    <li><code>endpoints/{ep}/</code> — per-endpoint detail for Claude</li>
    <li><code>chat/history.jsonl</code> — JAR-scoped chat history</li>
  </ul>
</div>

</div>

---

## 2. Phase 0 — Upload & Queue

**Controller:** `AnalyzerController` (`POST /api/analyzer`)

| Parameter | Type | Description |
|---|---|---|
| `file` | `MultipartFile` | `.jar` or `.war`, up to 2 GB |
| `mode` | `String` | `static` (default) or `claude` |
| `projectPath` | `String` | Optional path to source project (used by Claude) |
| `basePackage` | `String` | Optional filter — only parse classes under this prefix |
| `configFile` | `MultipartFile` | Optional per-JAR `domain-config.json` override |
| `renameSuffix` | `String` | Optional suffix appended before extension for disambiguation |

The controller saves the uploaded bytes to a temp file immediately, then submits a `QueueJob` and returns `{"status":"queued","jobId":…}`. The actual analysis runs asynchronously on a queue thread. WAR files are detected by extension and routed through `WarParserService`, which extracts app classes from `WEB-INF/classes/` and treats `WEB-INF/lib/*.jar` as bundled libraries.

---

## 3. Phase 1 — JAR Parsing (ASM)

**Service:** `JarParserService` + `BytecodeClassParser`

`JarParserService.parseJarToFile()` opens the JAR with `java.util.jar.JarFile` and visits every `.class` entry. Each class is handed to `BytecodeClassParser.parseClass(byte[])`, which uses the ASM **tree API** (`ClassNode`, `MethodNode`, `FieldNode`) to build a `ClassInfo` model object.

### What BytecodeClassParser collects

| Collected Item | Source ASM node | Notes |
|---|---|---|
| FQN, package, simpleName | `ClassNode.name` | Slash-separated → dot-separated |
| Superclass | `ClassNode.superName` | |
| Interfaces | `ClassNode.interfaces` | |
| `isInterface/Abstract/Enum` | `ClassNode.access` flags | |
| Class annotations | `ClassNode.visibleAnnotations` | Includes `@Controller`, `@Service`, etc. |
| Stereotype | Derived from annotations + interfaces | `CONTROLLER`, `SERVICE`, `REPOSITORY`, `COMPONENT`, `ENTITY`, `CONFIG` |
| Repository entity type | `ClassNode.signature` generic | Extracted from `MongoRepository<T,K>` signature |
| Class-level `@RequestMapping` base path | `ClassNode.visibleAnnotations` | Stored as `basePath` for method path combination |
| Fields | `ClassNode.fields` → `FieldNode` | Type, annotations, generic element type, compile-time constant value |
| Methods + annotations | `ClassNode.methods` → `MethodNode` | Name, descriptor, return type, parameter types, HTTP mapping annotations |
| Method invocations | `MethodNode.instructions` (INVOKEVIRTUAL/STATIC/INTERFACE/DYNAMIC) | `{ownerClass, methodName, descriptor}` — the raw call graph edges |

**Memory strategy:** each `ClassInfo` is serialised as a JSON line to a temp `.jsonl` file before moving to the next class. The heap never holds all classes simultaneously.

### Base package detection

If no `basePackage` is specified at upload time, the parser scans all top-level class packages, finds the most common 2-segment prefix (e.g., `com.example`), and uses that to decide which nested JARs in `BOOT-INF/lib/` to also parse (only those containing matching classes).

---

## 4. Phase 2 — Config & Catalog Extraction

**Services:** `JarParserService.extractConfigFiles()`, `MongoCatalogService`

- Scans the JAR for `application.properties`, `application.yml`, `bootstrap.yml`, etc.
- `MongoCatalogService.parseAllProperties()` merges all config files into a flat property map.
- If a MongoDB URI is found, `fetchAndStore()` connects and calls `listCollectionNames()` → stored as `mongo-catalog.json`.
- `extractAllConnectionInfo()` also captures Oracle JDBC URLs and stores them in `connections.json`.

These catalog results are passed to `CallGraphService.setMongoCatalog()` and `setConfigProperties()` so the call tree builder can use them for collection cross-checking.

---

## 5. Phase 3 — Call Graph Index & Endpoint Detection

**Services:** `CallGraphService` → `CallTreeBuilder` → `CallGraphIndex`

`buildIndex()` creates a fresh `ResolutionContext` (synchronized — prevents concurrent upload corruption). Then `addToIndex()` is called for each class streamed from the `.jsonl` file, populating:

- `classMap` — FQN → `ClassInfo`
- `interfaceImplMap` — interface FQN → concrete implementors (transitively expanded by BFS)
- `fieldTypeMap` — ownerClass → field name → declared field type (used for `@Autowired` injection resolution)

`buildEndpointsFromIndex()` then finds all `CONTROLLER`-stereotype classes, inspects each method for Spring HTTP mapping annotations, and constructs `EndpointInfo` objects with the resolved HTTP method and full path.

---

## 6. Phase 4 — Enriched Call Trees

**Service:** `CallTreeBuilder.buildCallTree()`

For each endpoint, a recursive tree walk resolves each invocation to a concrete target:

1. Look up the invocation owner class in `classMap`.
2. If the owner is an interface, look up concrete impls in `interfaceImplMap`.
3. If unresolved, try superclass chain via `childClassMap`.
4. At each `CallNode`, `CollectionResolver` inspects the class for MongoDB collection references (repository method names, `MongoTemplate` calls, `@Query` values, constant String fields).
5. Each collection reference is matched against `domain-config.json` prefix rules to assign a domain label.

Tree limits (`maxDepth=30`, `maxChildrenPerNode=30`, `maxNodesPerTree=2000`) prevent runaway traversal on circular or deeply nested call graphs.

---

## 7. Phase 5 — Persistence

**Service:** `PersistenceService` + `JarDataPaths`

All files are written under `data/{jarName}/` (configurable via `app.data-dir`). The JAR name is normalised by `JarNameUtil.normalizeKey()` (lowercased, special characters replaced) to produce the folder name.

`writeAnalysisStreaming()` uses a `JsonGenerator` to stream the `analysis.json` output directly to disk — the full list of `ClassInfo` objects is never held in memory at the same time as the endpoints.

---

## 8. Phase 5.6 — Source Enrichment (CFR)

**Services:** `SourceEnrichmentService` → `DecompilerService`

After the call trees are built but **before** `analysis.json` is written, CFR decompilation is run on all classes referenced in endpoint call trees:

- `SourceEnrichmentService.enrichCallTreesWithSource()` walks every `CallNode` in every endpoint tree.
- For each unique class encountered, `DecompilerService.decompile(jarPath, className)` extracts the raw `.class` bytes from the stored JAR, writes them to a temp dir, runs the CFR `CfrDriver` API, and captures the output.
- Individual method source is extracted from the full class source by scanning for the method signature.
- The source string is set on the `CallNode.sourceCode` field.
- Class source is cached (LRU, max 1500 entries) to avoid re-decompiling the same class for multiple endpoints.
- CFR runs on a fixed thread pool (min(4, availableProcessors)) with a 30-second per-class timeout.

---

## 9. Optional: Claude Enrichment

When `mode=claude`, after static analysis completes, `JarAnalysisExecutor` enqueues a follow-up `CLAUDE_ENRICH` job. `ClaudeAnalysisService` then:

1. Chunks endpoints into batches (controlled by `claudeAnalysisService.getMaxEndpoints()`).
2. Writes per-endpoint prompt files to `data/{jarName}/endpoints/{ep}/`.
3. Invokes the Claude CLI subprocess via `ClaudeProcessRunner`.
4. `CorrectionMerger` applies Claude's JSON corrections into a second file `analysis_corrected.json`, preserving the original `analysis.json`.
5. Each correction round increments `correctionCount` and updates `correctionAppliedAt` in the analysis metadata.

---

## 10. Output File Layout

```
data/
└── {jarName}/
    ├── analysis.json          — full static analysis (classes + endpoints + call trees)
    ├── analysis_corrected.json— Claude-enriched version (only present after Claude scan)
    ├── stored.jar             — copy of the uploaded file
    ├── domain-config.json     — snapshot of domain config used for this analysis
    ├── connections.json       — MongoDB/Oracle connection info found in the JAR's config
    ├── mongo-catalog.json     — live collection names from MongoDB (if reachable)
    ├── bundled-jars.json      — metadata on nested JARs inside the upload
    ├── jar-deps.json          — which bundled JARs are used by which app classes
    ├── _summary.json          — cached summary tab data
    ├── resources/             — non-class resource files from the JAR
    ├── endpoints/
    │   └── {ep}/              — per-endpoint JSON inputs/outputs for Claude
    ├── corrections/           — raw correction JSON payloads from each Claude run
    ├── claude/                — Claude session transcripts
    └── chat/
        └── history.jsonl      — JAR-scoped chat history
```

---

## 11. analysis.json Structure

```json
{
  "jarName": "my-service.jar",
  "projectName": "My Service",
  "jarSize": 45234567,
  "analyzedAt": "2026-05-17T10:30:00",
  "totalClasses": 342,
  "totalEndpoints": 28,
  "analysisMode": "STATIC",
  "basePackage": "com.example",
  "correctionAppliedAt": null,
  "correctionCount": 0,
  "claudeIteration": 0,
  "classes": [
    {
      "fullyQualifiedName": "com.example.controller.OrderController",
      "simpleName": "OrderController",
      "packageName": "com.example.controller",
      "stereotype": "CONTROLLER",
      "annotations": [{"desc": "Lorg/springframework/web/bind/annotation/RestController;"}],
      "fields": [...],
      "methods": [...]
    }
  ],
  "endpoints": [
    {
      "httpMethod": "GET",
      "path": "/api/orders",
      "controllerClass": "com.example.controller.OrderController",
      "methodName": "getOrders",
      "callTree": { ... }
    }
  ]
}
```

The `classes` array contains every parsed `ClassInfo`. The `endpoints` array contains every detected endpoint with its fully resolved `callTree` (nested `CallNode` objects). After a Claude scan, `analysisMode` becomes `"CORRECTED"` in `analysis_corrected.json`.

---

## 12. Tips

- **Large JARs (>200 MB):** The JSONL streaming strategy keeps heap usage bounded, but CFR decompilation of many large classes can still spike memory. The JVM is configured with `-Xmx4g` by default — increase via `spring-boot-maven-plugin` JVM args if needed.
- **Base package filter:** If a JAR includes heavily instrumented framework classes (Hibernate bytecode proxies, etc.), specifying a `basePackage` at upload time dramatically reduces analysis time and noise in the call trees.
- **WAR files:** `WarParserService` handles the WEB-INF layout automatically. The file extension (`.war`) triggers the WAR path; no user action is required.
- **Per-JAR domain config:** Upload a `domain-config.json` alongside the JAR to override the global config for this analysis only. This is useful when a service uses non-standard collection prefixes.
- **Re-analysis:** Deleting and re-uploading a JAR with the same name replaces all output files. The stored `domain-config.json` from the previous run is re-applied automatically unless a new one is uploaded.
- **CFR timeout:** If a class cannot be decompiled within 30 seconds, `DecompilerService` returns `null` for that class — the `CallNode.sourceCode` will be absent but the call tree structure is unaffected.
- **Call tree limits:** `calltree.max-depth`, `calltree.max-children-per-node`, and `calltree.max-nodes-per-tree` are configurable in `application.properties` (defaults: 30 / 30 / 2000).
