# Technology Stack & Architecture Reference

## Module Map

```
plsql-jar-analyzer (parent pom)
├── plsql-config          — Config model + YAML loader
├── plsql-parser          — ANTLR4 Oracle PL/SQL grammar, flow crawler, source downloader,
│                            + per-dialect adapters (multidb/{MySql,Db2,SqlServer,Postgres}ProcedureParser)
├── mysql-parser          — ANTLR4 MySQL grammar + MySqlAnalysisVisitor
├── sqlserver-parser      — ANTLR4 Microsoft SQL Server (T-SQL) grammar + SqlServerAnalysisVisitor
├── db2-parser            — ANTLR4 IBM Db2 (SQL PL) grammar + Db2AnalysisVisitor
├── postgres-parser       — ANTLR4 PostgreSQL (PL/pgSQL) grammar + PostgresAnalysisVisitor
├── plsql-analyzer-core   — Multi-dialect multi-file analysis orchestration
├── jar-analyzer-core     — JAR/WAR bytecode analysis (ASM + CFR) + Excel export — shared by
│                            both the JAR (`/api/jar/jars`) and WAR (`/api/war/wars`) controllers
├── flow-poc              — Dynamic flow executor POC (MongoDB + Oracle)
└── unified-web           — Spring Boot fat-JAR, REST API, frontend SPA
```

**Supported databases:** Oracle PL/SQL, MySQL, Microsoft SQL Server (T-SQL), IBM Db2 (SQL PL),
PostgreSQL (PL/pgSQL). Each dialect is a self-contained ANTLR4 grammar + analysis visitor
emitting the same shared analysis model, so downstream features (call graphs, table ops,
cursors, dynamic SQL, joins, complexity) work identically across all five — though facet
*completeness* (sequences, exception handlers, predicates) still varies by dialect; see
`docs/audit-findings-2026-09-13.md` for the current per-dialect status.

---

---

# 1. PL/SQL Parser (`plsql-parser`)

## Libraries

| Library | Version | Role |
|---------|---------|------|
| **ANTLR4 Runtime** | 4.13.1 | Lexer + parser for Oracle PL/SQL — `PlSqlLexer` / `PlSqlParser` generated from `.g4` grammar. Drives the full AST walk. |
| **ANTLR4 Maven Plugin** | 4.13.1 | Build-time code generation: compiles `.g4` grammar files into Java source at `generate-sources` phase. |
| **JSqlParser** | 5.0 | Parses SQL statements found inside PL/SQL bodies — extracts DML (`SELECT`, `INSERT`, `UPDATE`, `DELETE`), table names, and column refs from static SQL strings. |
| **ojdbc11** | 23.3.0.23.09 | Oracle JDBC driver. Used by `DbConnectionManager` to connect to `ALL_SOURCE` and `DBMS_METADATA.GET_DDL` for live source download. **Not on Maven Central** — must be installed locally. |
| **SnakeYAML** | Boot-managed | Reads `plsql-config.yaml` inside `DbConnectionManager` when building the connection pool from project/environment YAML. |
| **Jackson Databind** | Boot-managed | Serialises `FlowNode`, `ParseResult`, and chunked output JSON written to `data/plsql-parse/…/chunks/`. |
| **Jackson Dataformat YAML** | Boot-managed | YAML → POJO deserialization in `ConfigLoader` for `PlsqlConfig` / `ProjectConfig` / `EnvironmentConfig`. |
| **SLF4J API** | Boot-managed | Logging throughout all parser classes. |
| **JUnit 5** | Boot-managed (test) | Unit tests for grammar edge-cases, visitor output, utility methods. |
| **Mockito** | Boot-managed (test) | Mock `DbConnectionManager`, `SourceDownloader` in parser tests. |

## Process Flow

```
AnalysisService.runAnalysis(entryPoint, owner, objectType, project, environment)
│
├─ 1. buildEnvConfigFile(envConfig, project, environment)
│       Writes a plain-YAML temp file:
│         jdbc_url: "…"
│         db_users: [{username, password, description}, …]
│         source_paths: ["./plsql-sources"]
│       Returns Path to temp file (deleted on JVM exit).
│
├─ 2. FlowAnalysisMain.run(args)   ← CLI entry point
│       Parses --config, --entry, --object-type, --output-dir flags.
│
├─ 3. DbConnectionManager(configPath)
│       Loads jdbc_url + db_users list from temp YAML.
│       Registers Oracle JDBC driver; creates one Connection per db_user.
│       Exposes getAnyConnection() + getAvailableSchemas().
│
├─ 4. SchemaResolver(connManager, entryPoints)
│       Q1: resolve seed owners (e.g. CUSTOMER, OPUS_CORE, …)
│       Q2: CONNECT BY — transitive object dependencies per seed schema
│       Q3: reverse dependencies (objects that call the seed)
│       Q4: direct BFS validation for each entry-point owner
│       → Builds schema→object map used by resolveSchema() later.
│
├─ 5. SourceDownloader(connManager)
│       Provides downloadSource(name, type):
│         a. In-memory sourceCache hit → return immediately.
│         b. Local file cache (sources/ dir, built as filename index on first use).
│         c. DB query: SELECT OWNER,LINE,TEXT FROM ALL_SOURCE ORDER BY OWNER,LINE
│              → assembles lines into "CREATE OR REPLACE …" string.
│         d. looksLikeTruncated() check → if true, fallback to DBMS_METADATA.GET_DDL:
│              - SET_TRANSFORM_PARAM PRETTY=FALSE, SQLTERMINATOR=FALSE
│              - Normalises EDITIONABLE header and quoted schema.object names
│              - stripTrailingDdl() removes any second CREATE block appended by DBMS_METADATA
│         e. Wrapped/encrypted detection → marks object in wrappedObjects set.
│
└─ 6. DependencyCrawler(engine, downloader, schemaResolver)
         crawl(entryPoints)  [BFS, max-depth guard, cycle prevention via visitedKeys]
         │
         For each queued item:
         │
         ├─ 6a. SourceDownloader.downloadSource(name, type)
         │       → source text (or null if unavailable)
         │
         ├─ 6b. NodeParseCache check (disk cache, keyed by visitKey + MD5(source))
         │       Cache HIT  → skip re-parse, reuse FlowNode.
         │       Cache MISS → proceed to parse.
         │
         ├─ 6c. PlSqlParserEngine.parseContent(source, filename)
         │       Stage 1 — SLL prediction (fast):
         │         ANTLRInputStream → PlSqlLexer → CommonTokenStream → PlSqlParser
         │         BailErrorStrategy: throws ParseCancellationException on first error.
         │       Stage 2 — LL prediction (accurate, on SLL failure):
         │         DefaultErrorStrategy: recovers and collects error list.
         │       PlSqlAnalysisVisitor.visit(tree):
         │         Extracts: subprograms, call sites, table DML ops, cursor decls,
         │                   variable declarations, dynamic SQL (EXECUTE IMMEDIATE),
         │                   exception handlers, INTO targets.
         │       Returns ParseResult{FlowNode, errors[]}.
         │
         ├─ 6d. SchemaResolver.resolveSchema(callName, ownerHint)
         │       Maps unqualified call names to schema owners.
         │
         ├─ 6e. ChunkedFlowWriter.writeChunk(FlowNode)
         │       Writes to:
         │         data/plsql-parse/{folderName}/chunks/chunk_NNNN.json
         │         data/plsql-parse/{folderName}/sources/*.pkb|prc|fnc
         │
         └─ 6f. Enqueue discovered external calls → BFS continues
                  Writes on completion:
                    api/index.json   (summary + project/env/conn metadata)
                    edges.json       (call graph edges)
                    progress.json    (run statistics)
```

### Key classes

| Class | Responsibility |
|-------|---------------|
| `FlowAnalysisMain` | CLI entry; wires all components together |
| `DbConnectionManager` | JDBC connection pool, schema list |
| `SchemaResolver` | Oracle CONNECT BY transitive dependency resolution |
| `SourceDownloader` | ALL_SOURCE + DBMS_METADATA download, local file cache |
| `DependencyCrawler` | BFS orchestrator, cycle guard, error dedup |
| `PlSqlParserEngine` | Two-stage ANTLR4 parse (SLL → LL fallback) |
| `PlSqlAnalysisVisitor` | ANTLR4 visitor — extracts calls, DML, cursors, vars |
| `NodeParseCache` | Disk cache keyed by visitKey + MD5(source) |
| `ChunkedFlowWriter` | Writes chunk JSON + source files to output dir |

---

---

# 2. JAR Analyzer (`jar-analyzer-core`)

> **Also powers the WAR Analyzer.** WAR uploads go through the same `jar-analyzer-core`
> engine (`CallGraphService`, `CallTreeBuilder`, `PersistenceService`, `CallNode` model) via a
> separate `WarAnalyzerController` (`/api/war/wars`) and its own `/war/` frontend page — not a
> mode of `AnalyzerController`/`/jar/`. `CallNode` carries two additive, bytecode-derived
> facets consumed by the frontend's Dynamic Flows tab: `reflectiveCalls` (real
> `java.lang.reflect.*`/`Class` invocations, not a string-literal scan) and
> `thirdPartyDependencies` (real third-party library classes called, JDK-core packages
> excluded).

## Libraries

| Library | Version | Role |
|---------|---------|------|
| **OW2 ASM** | 9.7 | Core bytecode reader. Visits `.class` file bytes at instruction level using `ClassVisitor` / `MethodVisitor`. Zero external deps, minimal overhead per class. |
| **OW2 ASM Tree** | 9.7 | Tree API layer on top of ASM. Builds `ClassNode` / `MethodNode` / `InsnList` objects that can be walked multiple times, enabling multi-pass analysis. |
| **CFR** | 0.152 | Java decompiler used in the optional source-enrichment phase. Given a `.class` byte array it produces readable Java source — used to add `decompiled` field to `ClassInfo`. |
| **Apache POI OOXML** | 5.2.5 | Writes multi-sheet `.xlsx` Excel reports. Each sheet = one analysis category (controllers, repositories, SQL calls, HTTP calls, Mongo collections). |
| **MongoDB Driver Sync** | 4.11.2 | During config-scan phase reads `application.yml`/`application.properties` for a MongoDB URI and calls `listCollectionNames()` to build the Mongo catalog. |
| **Jackson Databind** | Boot-managed | JSON serialisation of `ClassInfo`, `EndpointInfo`, `MethodCallInfo`, and the final `analysis.json` written to disk. |
| **Spring Boot Starter Web** | 3.2.5 | Embeds Tomcat + Jackson for the REST layer (`AnalyzerController`). Provides `@RestController`, `MultipartFile`, async task infrastructure. |

## Process Flow

```
POST /api/jar/jars   (AnalyzerController)
│    Body: multipart JAR/WAR file + metadata (appName, version, …)
│
├─ 1. queueService.submit(JAR_UPLOAD, metadata)
│       Persists job record; returns jobId to caller immediately.
│
└─ 2. [async worker] JarAnalysisExecutor.execute(job)
        │
        ├─ 2a. JarParserService.parseJarToFile(jarFile)
        │       │
        │       ├─ Open JAR with ZipFile; detect layout:
        │       │    Spring Boot fat-JAR  → classes under BOOT-INF/classes/
        │       │    Standard JAR/WAR     → classes at root or WEB-INF/classes/
        │       │
        │       ├─ For each .class entry:
        │       │    BytecodeClassParser.parseClass(classBytes)
        │       │      ├─ ASM ClassReader → ClassNode (fields, methods, annotations)
        │       │      ├─ Read class-level annotations:
        │       │      │    @RestController, @Controller  → stereotype = CONTROLLER
        │       │      │    @Repository, @Mapper          → stereotype = REPOSITORY
        │       │      │    @Service                      → stereotype = SERVICE
        │       │      │    @SpringBootApplication        → marks entry point
        │       │      ├─ Walk each MethodNode.instructions:
        │       │      │    LdcInsnNode     → string constants (SQL fragments, URLs)
        │       │      │    FieldInsnNode   → field references
        │       │      │    MethodInsnNode  → regular method calls
        │       │      │    InvokeDynamicInsnNode → lambda / string concat targets
        │       │      │         Detects:
        │       │      │           @GetMapping / @PostMapping / @RequestMapping → EndpointInfo
        │       │      │           JdbcTemplate.query/update/execute             → SQL call
        │       │      │           SQL-keyword string constants                  → raw SQL
        │       │      │           MongoTemplate / MongoRepository calls         → Mongo op
        │       │      │           RestTemplate / WebClient / HttpClient calls   → HTTP call
        │       │      └─ Returns ClassInfo{name, stereotype, fields, methods,
        │       │                           endpoints[], sqlCalls[], mongoCalls[], httpCalls[]}
        │       │
        │       └─ Streams ClassInfo objects as JSONL to bounded temp file
        │               (avoids holding all classes in memory simultaneously)
        │
        ├─ 2b. JarParserService.extractConfigFiles()
        │       Reads application.yml / application.properties from the JAR.
        │       MongoCatalogService.fetchAndStore():
        │         Parses spring.data.mongodb.uri from config.
        │         Connects via MongoDB Driver Sync → listCollectionNames().
        │         Stores collection list in analysis metadata.
        │
        ├─ 2c. CallGraphService.buildIndex()
        │       Reads JSONL temp file; indexes all classes by internal name.
        │       Indexes all method signatures → MethodNode map.
        │
        │       CallGraphService.buildEndpointsFromIndex()
        │         For each REST endpoint discovered:
        │           Performs BFS/DFS call tree walk from controller method.
        │           At each node: records DB calls, HTTP calls, Mongo ops.
        │           Annotates each node with depth + call path.
        │         → Produces List<EndpointInfo> with full call trees.
        │
        ├─ 2d. SourceEnrichmentService.enrich()   [optional]
        │       For classes flagged for enrichment (controllers, repos):
        │         CFR.decompile(classBytes) → Java source string.
        │         Attached to ClassInfo.decompiledSource field.
        │       Skipped if CFR is unavailable or class is excluded.
        │
        └─ 2e. PersistenceService.save()
                Writes analysis.json to:
                  data/jar-analysis/{appName}-{version}/analysis.json
                Writes Excel report (.xlsx) with sheets:
                  • Endpoints      (controller → HTTP method, path, return type)
                  • SQL Calls      (method → raw SQL fragment, operation type)
                  • Mongo Calls    (method → collection, operation)
                  • HTTP Calls     (method → target URL, client library)
                  • Class Summary  (all classes with stereotype + metrics)
```

### Key classes

| Class | Responsibility |
|-------|---------------|
| `AnalyzerController` | REST endpoint, multipart upload, job submission |
| `JarAnalysisExecutor` | Async worker — orchestrates the full analysis pipeline |
| `JarParserService` | JAR entry enumeration, fat-JAR layout detection |
| `BytecodeClassParser` | ASM ClassNode visitor — extracts all metadata from `.class` bytes |
| `CallGraphService` | Indexes all classes/methods; builds endpoint call trees |
| `SourceEnrichmentService` | Optional CFR decompilation phase |
| `MongoCatalogService` | Reads MongoDB URI from config; fetches collection catalog |
| `PersistenceService` | Writes `analysis.json` + Excel `.xlsx` to output directory |

---

---

## Shared Config (`plsql-config`)

| Library | Version | Role |
|---------|---------|------|
| Jackson Databind | Boot-managed | Deserializes `PlsqlConfig` / `ProjectConfig` / `EnvironmentConfig` |
| Jackson Dataformat YAML | Boot-managed | YAML → POJO mapping in `ConfigLoader` |

Key models: `PlsqlConfig`, `ProjectConfig`, `EnvironmentConfig`, `ConnectionConfig`, `DbUserConfig`

---

## Flow POC (`flow-poc`)

| Library | Version | Role |
|---------|---------|------|
| MongoDB Driver Sync | 4.11.2 | Live DB fetch; `MongoDataFetcher`, shadow engine |
| Byte Buddy | 1.14.17 | Dynamic proxy for `MongoTemplate` interception (no source change) |
| JSqlParser | 5.0 | SQL predicate extraction from static call trees |
| Jackson Databind | Boot-managed | Reads `analysis.json` for static flow data |
| ojdbc11 | 23.3.0.23.09 | Oracle predicate-fetch against real DB (optional, runtime scope) |
| Spring JDBC | Boot-managed | Oracle JDBC template |

---

## AI Enrichment

| Component | Notes |
|-----------|-------|
| Claude CLI | External process via `ProcessBuilder` (`claude` binary; `npm install -g @anthropic-ai/claude-code`) |
| Claude Bedrock | AWS Bedrock fallback; `claude.bedrock.enabled=true`, profile `ClaudeCode`, region `eu-central-1` |
| HTTP API model | `claude-haiku-4-5-20251001` used in `ClaudeFlowAnalyzer` when no CLI |

---

## Frontend

| Technology | Notes |
|------------|-------|
| Vanilla JavaScript | IIFE module pattern; no build step, no framework |
| HTML/CSS | Single-page app served from `src/main/resources/static/parser/index.html` |
| Modules | `app.js`, `home.js`, `history.js`, `queryRunner.js`, `chunkViewer.js`, `sync.js`, `tooltips.js` |

---

## Build & Run

**Build:**
```bash
mvn clean package -pl plsql-config,plsql-parser,unified-web -am
```

**Run:**
```bash
java -Xms512m -Xmx4g -XX:+UseG1GC -XX:+UseStringDeduplication \
  -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --app.config-dir=/path/to/config \
  --app.data-dir=/path/to/data
```

**Oracle JDBC (not on Maven Central — install once):**
```bash
mvn install:install-file \
  -Dfile=ojdbc11.jar \
  -DgroupId=com.oracle.database.jdbc \
  -DartifactId=ojdbc11 \
  -Dversion=23.3.0.23.09 \
  -Dpackaging=jar
```
