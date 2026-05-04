# Unified Analyzer -- Architecture Manual

> **Version:** 1.0.0-SNAPSHOT  
> **Runtime:** Java 17, Spring Boot 3.2.5  
> **Group ID:** `com.codeanalyzer`  
> **Artifact:** `plsql-jar-analyzer`

---

## Table of Contents

1. [Overview](#1-overview)
2. [Module Structure](#2-module-structure)
3. [Build & Deploy](#3-build--deploy)
4. [Configuration](#4-configuration)
5. [Data Flow Pipeline](#5-data-flow-pipeline)
6. [Queue System](#6-queue-system)
7. [Claude AI Integration](#7-claude-ai-integration)
8. [REST API Map](#8-rest-api-map)
9. [Storage Layout](#9-storage-layout)
10. [Frontend Architecture](#10-frontend-architecture)
11. [Security Considerations](#11-security-considerations)

---

## 1. Overview

The Unified Analyzer is a multi-module Maven project that combines two complementary
static analysis engines into a single deployable Spring Boot application:

- **JAR Analyzer** -- Inspects compiled Java bytecode (`.jar` files) using ASM 9.7 and
  CFR 0.152. Extracts REST endpoints, class hierarchies, call graphs, complexity metrics,
  and JPA/MongoDB data-access patterns. Optionally enriches results with Claude AI
  natural-language descriptions.

- **PL/SQL Parser** -- Parses Oracle PL/SQL source code (8i through 23c) using ANTLR4
  4.13.1. Crawls dependency trees via Oracle data dictionary views, extracts control flow,
  table operations, triggers, cursors, joins, and exception handlers. Produces structured
  JSON for downstream consumption.

Both engines share a unified queue system that serializes analysis jobs on a single
worker thread, a common configuration subsystem (`ConfigDirService`), Server-Sent Events
(SSE) for real-time progress streaming, and a Claude AI integration layer for
AI-assisted enrichment and verification.

The application runs as a single fat JAR on a configurable port (default `8083`) and
serves three static web UIs alongside the REST API layer. No external database is
required for core functionality; all data persists to the local filesystem. MongoDB
connectivity is optional and used only for the JAR Analyzer's catalog feature.

### Technology Stack Summary

| Concern               | Technology            | Version        |
|-----------------------|-----------------------|----------------|
| Language              | Java (JDK)            | 17             |
| Framework             | Spring Boot           | 3.2.5          |
| PL/SQL Parsing        | ANTLR4                | 4.13.1         |
| SQL Normalization     | JSqlParser            | 5.0            |
| Bytecode Analysis     | OW2 ASM               | 9.7            |
| Java Decompilation    | CFR                   | 0.152          |
| Excel Export          | Apache POI            | 5.2.5          |
| Catalog Storage       | MongoDB Driver Sync   | 4.11.2         |
| Oracle Connectivity   | ojdbc11               | 23.3.0.23.09   |
| Serialization         | Jackson (YAML + JSON) | Managed by Boot|
| AI Enrichment         | Claude CLI (external)  | Any            |
| Frontend              | Vanilla JS (IIFE)     | No build step  |

---

## 2. Module Structure

The project is organized as a Maven multi-module build with a parent POM that inherits
from `spring-boot-starter-parent:3.2.5`. The parent POM declares five child modules and
manages cross-module dependency versions centrally.

```
plsql-jar-analyzer/                     (parent POM, packaging=pom)
  plsql-config/                         Module 1 -- Shared configuration
  plsql-parser/                         Module 2 -- ANTLR4 PL/SQL parser
  plsql-analyzer-core/                  Module 3 -- Legacy PL/SQL analyzer
  jar-analyzer-core/                    Module 4 -- Bytecode analysis engine
  unified-web/                          Module 5 -- Spring Boot web application
  docs/                                 Documentation
  config/                               Runtime configuration files
  data/                                 Runtime data directory
  start.bat / start.sh                  Launch scripts
  dist/ / deploy/                       Distribution packaging
```

### 2.1 plsql-config

**Purpose:** Shared configuration model and YAML/JSON loader.

**Package:** `com.plsqlanalyzer.config`

**Key Classes:**
- `PlsqlConfig` -- Root configuration POJO (deserialized from `plsql-config.yaml`)
- `ConnectionConfig` -- Oracle JDBC connection parameters (host, port, SID/service, credentials)
- `ProjectConfig` -- Named project with multiple environments
- `EnvironmentConfig` -- Named environment with connection list
- `DbUserConfig` -- Per-user database credentials
- `SchemaMapping` -- Schema alias resolution mappings
- `ConfigLoader` -- YAML/JSON deserialization utility

**Dependencies:** Jackson Databind, Jackson YAML format.

**Consumers:** All other modules depend on `plsql-config` for configuration types.

### 2.2 plsql-parser

**Purpose:** ANTLR4-based PL/SQL flow analysis engine. This is the most test-intensive
module with 395 tests (zero failures), including an 88-test grammar hardening suite
covering Oracle syntax from 8i through 23c.

**Packages:**

```
com.plsql.parser
  flow/           Orchestration layer
  model/          Data classes (FlowResult, FlowNode, FlowEdge, ParseResult, etc.)
  visitor/        PlSqlAnalysisVisitor -- main ANTLR visitor
  (root)          PlSqlParserEngine, PlSqlParserMain, PlSqlLexerBase, PlSqlParserBase

com.plsqlanalyzer.parser
  grammar/        PlSqlAnalyzer.g4 (combined grammar for legacy path)
  model/          PlsqlProcedure, PlsqlUnit, DependencyGraph, SqlAnalysisResult
  service/        Parsing services for the legacy analyzer model
  cache/          Grammar and parse-tree caching
  visitor/        Visitor for the legacy analyzer grammar
```

**ANTLR4 Grammars (three files):**
- `PlSqlLexer.g4` -- Tokenizer for Oracle PL/SQL (split lexer grammar)
- `PlSqlParser.g4` -- Full parser grammar (DDL, DML, PL/SQL blocks, Oracle-specific syntax)
- `PlSqlAnalyzer.g4` -- Combined grammar for the legacy analyzer code path

**Key Classes:**
- `FlowAnalysisMain` -- Entry point coordinating the full analysis pipeline
- `DependencyCrawler` -- Walks the dependency tree, downloads source from Oracle,
  recursively parses called objects
- `SchemaResolver` -- Resolves unqualified object names to schemas using Oracle data
  dictionary views (`ALL_OBJECTS`, `ALL_SYNONYMS`, `ALL_DEPENDENCIES`). Uses
  `CONNECT BY` transitive dependency queries. Results are cached to disk as TSV files
  in `data/cache-plsql/`
- `TriggerDiscoverer` -- Identifies triggers attached to tables referenced by the
  analyzed procedure
- `PlSqlAnalysisVisitor` -- ANTLR visitor extracting flow nodes, table operations,
  calls, cursors, variables, exception handlers, and dynamic SQL
- `PlSqlParserEngine` -- Wraps ANTLR lexer/parser setup with error handling and
  parse-tree caching
- `ChunkedFlowWriter` -- Writes large analysis results in chunked JSON to avoid memory
  issues

**Dependencies:** ANTLR4 Runtime, JSqlParser, ojdbc11, Jackson, SnakeYAML, plsql-config.

### 2.3 plsql-analyzer-core

**Purpose:** Legacy PL/SQL analyzer service layer. Provides higher-level analysis
constructs (call graphs, table operation summaries, join/cursor/sequence summaries)
built on top of the parser output.

**Package:** `com.plsqlanalyzer.analyzer`

**Key Classes:**
- `AnalysisService` -- Orchestrates full PL/SQL analysis (delegates to parser, builds
  call graphs, collects summaries)
- `FastAnalysisService` -- Lightweight analysis mode for quick results
- `CallGraphBuilder` / `CallGraph` -- Builds and queries PL/SQL call graphs
- `TriggerResolver` -- Resolves trigger metadata for referenced tables
- `TableOperationCollector` -- Aggregates table operations across the dependency tree
- `JoinOperationCollector` -- Extracts and summarizes join operations
- `CursorOperationCollector` -- Extracts and summarizes cursor usage
- `SequenceOperationCollector` -- Extracts and summarizes sequence references

**Models:** `AnalysisResult`, `CallGraphNode`, `CallEdge`, `TableOperationSummary`,
`JoinOperationSummary`, `CursorOperationSummary`, `SequenceOperationSummary`.

**Dependencies:** plsql-config, plsql-parser, SLF4J.

### 2.4 jar-analyzer-core

#### Complete Service Inventory

| Service | Purpose |
|---|---|
| `JarParserService` | End-to-end JAR analysis orchestration |
| `WarParserService` | WAR file analysis — extracts `WEB-INF/classes/` and `WEB-INF/lib/` |
| `BytecodeClassParser` | ASM visitor — extracts `ClassInfo`, `MethodInfo`, `FieldInfo`, invocations, annotations |
| `CallGraphService` | Builds the flat method-level call graph from invocation data |
| `CallGraphIndex` | Indexed call graph for O(1) lookup by class+method key |
| `CallTreeBuilder` | Constructs per-endpoint call trees with Spring DI dispatch resolution |
| `DecompilerService` | CFR wrapper for on-demand source decompilation |
| `AggregationDetector` | Detects MongoDB aggregation pipeline stages in bytecode |
| `CollectionResolver` | Resolves MongoDB collection names from repository annotations, string literals, and method patterns |
| `MongoMethodDetector` | Identifies MongoDB driver method calls (`find`, `aggregate`, `insertOne`, etc.) |
| `JpaMethodDetector` | Identifies JPA/Hibernate method calls and query patterns |
| `DomainConfigLoader` | Loads and applies `domain-config.json` rules for domain assignment |
| `SwarmClusterer` | Groups related endpoints into clusters for coherent Claude analysis |
| `TreeChunker` | Splits large call trees into Claude-sized chunks respecting char/node/depth limits |
| `ClaudeAnalysisService` | Master Claude enrichment orchestrator (chunking, verification, merging) |
| `ClaudeProcessRunner` | Spawns Claude CLI as external subprocess with stdin piping and timeout |
| `ClaudeResultMerger` | Extracts and merges JSON from Claude response text |
| `ClaudeCorrectionService` | Applies Claude correction output to analysis data |
| `CorrectionMerger` | Merges per-endpoint corrections into the full analysis JSON |
| `CorrectionPersistence` | Reads/writes correction files and manages the three-version lifecycle |
| `ClaudeSessionManager` | Tracks active Claude sessions; supports kill by session or bulk kill |
| `ClaudeEnrichmentTracker` | Per-JAR enrichment progress tracking |
| `ClaudeCallLogger` | Logs every Claude CLI invocation to a structured JSONL log file |
| `FragmentStore` | Caches individual Claude response fragments to disk |
| `ChatboxService` | JAR-scoped chatbox conversation handler |
| `ChatHistoryService` | Classic chat session persistence and management |
| `MongoCatalogService` | Fetches MongoDB collection metadata and verifies collection existence; also detects MongoDB, Oracle, and PostgreSQL connection details from bundled config files |
| `ExcelExportService` | Multi-sheet XLSX export using Apache POI |
| `ExcelStyleHelper` | Reusable cell styling (colors, borders, fonts) for Excel output |
| `PersistenceService` | Reads/writes `analysis.json` and version files; exposes `storeResourceFiles()`, `listResourceFiles()`, and `loadResourceFile()` for resource file management; exposes `storeBundledJarInfo()`, `loadBundledJarInfo()`, `storeJarDepsMap()`, and `loadJarDepsMap()` for bundled-library and dependency-map persistence |
| `ProgressService` | Tracks analysis progress percentages for SSE streaming |
| `SubtreeCache` | Caches built subtrees to avoid redundant re-traversal across endpoints |
| `SourceEnrichmentService` | Correlates decompiled bytecode with original source code |
| `EndpointOutputWriter` | Writes per-endpoint breakdown JSON files |
| `JarDataPaths` | Resolves all file paths for a given JAR key; includes `resourcesDir()` for the extracted resource files directory, `bundledJarsFile()` for `bundled-jars.json`, and `jarDepMapFile()` for `jar-dep-map.json` |
| `WarDataPaths` | Resolves all file paths for a given WAR key |
| `JarNameUtil` | Normalizes JAR/WAR names to filesystem-safe keys |
| `PromptTemplates` | Loads Claude prompt templates from `config/prompts/` |
| `StaticAnalysisProvider` | Data provider that loads static analysis only (no Claude data) |
| `AnalysisDataProviderFactory` | Factory that selects the appropriate data provider based on version request |
| `AnalysisNotFoundException` | Exception thrown when requested analysis does not exist on disk |
| `WarPersistenceService` | WAR-specific persistence (identical structure to JAR persistence) |



**Purpose:** Java bytecode analysis engine. Extracts structural metadata, REST endpoints,
call graphs, class hierarchies, and complexity metrics from compiled JAR files. Includes
the full Claude AI enrichment pipeline for JAR analysis.

**Package:** `com.jaranalyzer`

```
com.jaranalyzer
  model/        ClassInfo, MethodInfo, EndpointInfo, CallNode, FieldInfo,
                AnnotationInfo, JarAnalysis, ParameterInfo, etc.
  service/      56 service classes covering analysis, Claude AI, and export
  util/         SpringAnnotations, SqlStatementParser, TypeUtils
```

**Key Services:**
- `JarParserService` -- Orchestrates end-to-end JAR analysis
- `BytecodeClassParser` -- ASM-based visitor extracting class/method/field metadata
- `DecompilerService` -- CFR wrapper for on-demand source decompilation
- `CallGraphService` / `CallGraphIndex` / `CallTreeBuilder` -- Method-level call
  graph construction and querying
- `ExcelExportService` -- Multi-sheet XLSX reports with styled output
- `MongoCatalogService` -- Persists analysis results to MongoDB (optional)
- `ProgressService` -- Tracks analysis progress for SSE streaming

**Claude AI Services (within jar-analyzer-core):**
- `ClaudeAnalysisService` -- Manages enrichment sessions (chunking, verification, merging)
- `ClaudeProcessRunner` -- Spring `@Component("jarClaudeProcessRunner")` that invokes
  Claude CLI as an external process
- `ClaudeResultMerger` / `CorrectionMerger` -- Merges AI-generated corrections into
  analysis data
- `ClaudeSessionManager` / `ClaudeEnrichmentTracker` -- Session lifecycle and progress
- `TreeChunker` / `SwarmClusterer` -- Splits large call trees into Claude-friendly chunks
- `FragmentStore` / `CorrectionPersistence` -- Caches fragments and persists corrections

**Dependencies:** Spring Boot Web, ASM 9.7, CFR 0.152, Apache POI 5.2.5,
MongoDB Driver Sync 4.11.2.

### 2.5 unified-web

**Purpose:** Spring Boot web application providing a single deployment for both tools.
Serves REST APIs, hosts three static web UIs, manages the unified queue system, and
handles SSE progress streaming.

**Packages:**

```
com.analyzer
  chat/           ChatController, ChatService, ChatSession, ChatConfig
  config/         WebConfig, GlobalSessionsController, PollingConfigController
  queue/          AnalysisQueueService, QueueController, QueueJob,
                  JarAnalysisExecutor, PlsqlAnalysisExecutor,
                  FastAnalysisExecutor, ParserAnalysisExecutor,
                  ClaudeJobExecutor

com.jaranalyzer.controller
                  AnalyzerController, DecompileController, ChatboxController,
                  ProgressController, LogController

com.plsqlanalyzer.web
  config/         AppConfig, ConfigDirService, ApiLoggingFilter
  controller/     AnalyzerController, DatabaseController, SourceController,
                  ConfigController, ClaudeController, DocsController,
                  LogController, ProgressController
  parser/
    config/       ComplexityConfig, JoinComplexityConfig
    controller/   ParserAnalysisController, ParserConfigController, ClaudeController
    model/        AnalysisInfo, ParserAnalysisInfo
    service/      AnalysisService, ClaudeProcessRunner, ClaudeSessionManager,
                  ClaudePersistenceService, ClaudePromptBuilder, ChunkingUtils
```

**Dependencies:** Spring Boot Web, Spring Boot JDBC, jar-analyzer-core,
plsql-analyzer-core, plsql-parser, ojdbc11, Jackson YAML, Jackson JSR-310.

### Module Dependency Graph

```
plsql-config
    ^
    |
    +-- plsql-parser
    |       ^
    |       |
    +-- plsql-analyzer-core
    |       ^
    |       |
    +-- jar-analyzer-core (independent, no PL/SQL deps)
    |       ^
    |       |
    +-- unified-web (depends on all four modules above)
```

---

## 3. Build & Deploy

### 3.1 Prerequisites

- **Java 17+** -- Required for compilation and runtime
- **Maven 3.8+** -- Multi-module build tool
- **Claude CLI** (optional) -- Required only for AI enrichment features.
  Install via: `npm install -g @anthropic-ai/claude-code`
- **Oracle Database** (optional) -- Required only for PL/SQL source fetching

### 3.2 Building

```bash
# Full build (all modules, with tests)
mvn clean package

# Full build, skip tests
mvn clean package -DskipTests

# Build only unified-web and its dependencies
mvn clean package -pl unified-web -am

# Build a specific module
mvn clean install -pl plsql-parser
mvn clean install -pl jar-analyzer-core
```

The build produces a Spring Boot fat JAR at:
```
unified-web/target/unified-web-1.0.0-SNAPSHOT.jar
```

### 3.3 Running

**Direct launch:**
```bash
java -Xms512m -Xmx4g -XX:+UseG1GC -XX:+UseStringDeduplication \
     -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
     --server.port=8083 \
     --app.config-dir=config \
     --app.data-dir=data
```

**Using start scripts:**
```bash
# Windows
start.bat

# Linux
start.sh
```

The `start.bat` script performs pre-flight checks before launch:
1. Validates Java is installed and in PATH
2. Verifies the built JAR file exists
3. Checks for Claude CLI availability (warns if missing, does not block)
4. Creates required directories (`config/`, `data/`, subdirectories)
5. Sets JVM tuning flags
6. Launches with configured port, config-dir, and data-dir

### 3.4 JVM Tuning

The recommended JVM configuration (set by `start.bat` and the Maven plugin):

| Flag                           | Purpose                                       |
|--------------------------------|-----------------------------------------------|
| `-Xms512m`                     | Initial heap size                             |
| `-Xmx4g`                       | Maximum heap size                             |
| `-XX:MaxMetaspaceSize=256m`    | Metaspace ceiling                             |
| `-XX:+UseG1GC`                 | G1 garbage collector                          |
| `-XX:G1HeapRegionSize=4m`      | G1 region size                                |
| `-XX:+UseStringDeduplication`  | Deduplicate identical strings (saves memory)  |
| `-XX:ParallelGCThreads=4`     | Parallel GC threads                           |
| `-XX:ConcGCThreads=2`         | Concurrent GC threads                         |
| `-XX:+HeapDumpOnOutOfMemoryError` | Dump heap on OOM                           |

### 3.5 Verifying the Deployment

After startup, the following URLs should be accessible:

| URL                           | Description                    |
|-------------------------------|--------------------------------|
| `http://localhost:8083/`      | Home page (launcher)           |
| `http://localhost:8083/jar/`  | JAR Analyzer UI                |
| `http://localhost:8083/parser/` | PL/SQL Parser UI             |
| `http://localhost:8083/api/queue` | Queue state (JSON)          |

---

## 4. Configuration

### 4.1 ConfigDirService

`ConfigDirService` is the central directory manager for all configuration and data
paths. It is a Spring `@Component` that reads two properties at startup:

| Property          | Default   | Description                                   |
|-------------------|-----------|-----------------------------------------------|
| `app.config-dir`  | `config`  | Root directory for configuration files         |
| `app.data-dir`    | `data`    | Root directory for analysis data and logs      |

Both are fully dynamic -- pass any absolute or relative path at startup:

```bash
java -jar app.jar --app.config-dir=/etc/analyzer/config --app.data-dir=/var/data/analyzer
```

At startup, `ConfigDirService` performs the following initialization:

1. Resolves both paths to absolute form
2. Creates directory trees if they do not exist
3. Migrates legacy config files from old locations
4. Seeds missing config files from classpath resources (built into the JAR)
5. Seeds prompt templates into `config/prompts/`
6. Wires `PromptTemplates` to check the external config dir first
7. Runs data layout migration (flat layout to per-analysis layout)

### 4.2 application.properties

The file `unified-web/src/main/resources/application.properties` is the central
configuration file. All values are overridable via command-line flags (`--key=value`).

**Server:**

| Property                                | Default   | Description                          |
|-----------------------------------------|-----------|--------------------------------------|
| `server.port`                           | `8083`    | HTTP port                            |
| `spring.application.name`              | `unified-analyzer` | Application name             |
| `server.tomcat.threads.max`            | `50`      | Max Tomcat threads                   |
| `server.tomcat.threads.min-spare`      | `4`       | Min idle threads                     |
| `server.tomcat.connection-timeout`     | `60s`     | Connection timeout                   |
| `spring.servlet.multipart.max-file-size` | `2GB`   | Max upload file size                 |
| `spring.mvc.async.request-timeout`     | `-1`      | Async timeout (infinite for SSE)     |

**Directories:**

| Property                   | Description                                              |
|----------------------------|----------------------------------------------------------|
| `app.config-dir`           | Config files root (plsql-config.yaml, domain-config.json)|
| `app.data-dir`             | Data files root (jar/, plsql/, logs)                     |
| `app.parser-config-path`   | Absolute path to `plsql-config.yaml` for parser DB conn  |

**Claude AI:**

| Property                              | Default    | Description                        |
|---------------------------------------|------------|------------------------------------|
| `claude.binary`                       | `claude`   | Path to Claude CLI binary          |
| `claude.analysis.max-endpoints`       | `-1`       | Max endpoints to enrich (-1 = all) |
| `claude.analysis.parallel-chunks`     | `5`        | Concurrent chunk processing        |
| `claude.allowed-tools`                | `Read,Grep,Glob,Bash,Write,Edit` | Tools Claude may use |
| `claude.timeout.per-endpoint`         | `7200`     | Per-endpoint timeout (seconds)     |
| `claude.timeout.version-check`        | `30`       | CLI version check timeout (seconds)|
| `claude.timeout.stream-drain`         | `3000`     | Stream drain timeout (seconds)     |
| `claude.timeout.executor-shutdown`    | `60`       | Executor shutdown timeout (seconds)|
| `claude.chunking.max-chunk-chars`     | `50000`    | Max characters per chunk           |
| `claude.chunking.max-tree-nodes`      | `500`      | Max nodes per call tree chunk      |
| `claude.chunking.max-chunk-depth`     | `3`        | Max depth per chunk                |
| `claude.chunking.max-prompt-chars`    | `180000`   | Max total prompt size              |

**Call Tree Limits:**

| Property                     | Default | Description                             |
|------------------------------|---------|-----------------------------------------|
| `calltree.max-depth`        | `20`    | Maximum call tree depth                  |
| `calltree.max-children-per-node` | `30` | Max children per node                 |
| `calltree.max-nodes-per-tree`| `2000`  | Max nodes per tree                      |

**PL/SQL Threading:**

| Property                        | Default | Description                          |
|---------------------------------|---------|--------------------------------------|
| `plsql.threads.source-fetch`   | `8`     | Parallel source download threads     |
| `plsql.threads.trigger-resolve`| `4`     | Parallel trigger resolution threads  |
| `plsql.threads.metadata`       | `4`     | Parallel metadata fetch threads      |
| `plsql.threads.claude-parallel`| `5`     | Parallel Claude verification threads |
| `plsql.tree.max-depth`         | `50`    | Max dependency tree depth            |
| `plsql.tree.max-nodes`         | `2000`  | Max nodes in dependency tree         |

**Chat:**

| Property               | Default | Description                                |
|-------------------------|---------|--------------------------------------------|
| `chat.classic.enabled`  | `true`  | Enable session-based classic chat panel    |
| `chat.chatbox.enabled`  | `true`  | Enable floating AI chatbox (JAR-specific)  |

**MongoDB (optional):**

| Property                          | Default | Description                     |
|-----------------------------------|---------|---------------------------------|
| `mongo.timeout.connect`          | `30`    | Connection timeout (seconds)     |
| `mongo.timeout.server-selection` | `30`    | Server selection timeout (seconds)|

**Logging:**

| Property                                    | Default                      |
|---------------------------------------------|------------------------------|
| `logging.file.name`                        | `{data-dir}/unified-analyzer.log` |
| `logging.logback.rollingpolicy.max-file-size` | `10MB`                    |
| `logging.logback.rollingpolicy.max-history`   | `10`                      |
| `logging.logback.rollingpolicy.total-size-cap`| `500MB`                   |

**DataSource:** Auto-configuration is excluded (`DataSourceAutoConfiguration`).
PL/SQL manages Oracle connections manually via `DbConnectionManager` using
parameters from `plsql-config.yaml`.

### 4.3 plsql-config.yaml

Located at `{config-dir}/plsql-config.yaml`. Defines Oracle database connections
organized by project and environment:

```yaml
projects:
  - name: MyProject
    environments:
      - name: DEV
        connections:
          - name: primary
            host: db-host
            port: 1521
            service: ORCL
            username: app_user
            password: "..."
            schemas:
              - APP_SCHEMA
              - COMMON_SCHEMA
```

Managed at runtime via the REST API (`/api/plsql/config/projects/...`).

### 4.4 domain-config.json

Located at `{config-dir}/domain-config.json`. Defines domain-specific clustering
rules for the JAR Analyzer. Used to group endpoints and classes into business domains
for more meaningful analysis output.

### 4.5 Prompt Templates

Located at `{config-dir}/prompts/*.txt`. These files define the prompt templates
sent to Claude CLI during enrichment and verification. Seeded from classpath on
first startup. Templates include:

| Template File                    | Purpose                                     |
|----------------------------------|---------------------------------------------|
| `plsql-verification.txt`        | PL/SQL analysis verification prompt         |
| `java-mongo-analysis.txt`       | JAR analysis (MongoDB context)              |
| `java-mongo-chunk-analysis.txt` | JAR chunk analysis (MongoDB context)         |
| `java-mongo-correction.txt`     | JAR correction prompt (MongoDB context)      |
| `java-both-analysis.txt`        | JAR analysis (both DB types)                 |
| `java-both-chunk-analysis.txt`  | JAR chunk analysis (both DB types)           |
| `java-both-correction.txt`      | JAR correction prompt (both DB types)        |
| `java-oracle-analysis.txt`      | JAR analysis (Oracle context)                |
| `java-oracle-chunk-analysis.txt`| JAR chunk analysis (Oracle context)          |
| `java-oracle-correction.txt`    | JAR correction prompt (Oracle context)       |

---

## 5. Data Flow Pipeline

### 5.0 WAR Analysis Pipeline

```
User uploads .war file
  |
  v
AnalyzerController detects .war extension
  |
  v
QueueJob(JAR_UPLOAD) submitted (shared job type)
  |
  v
JarAnalysisExecutor detects WAR → delegates to WarParserService.analyze():
  |
  +-- Extract WEB-INF/classes/ entries → parse application classes via BytecodeClassParser
  |
  +-- Extract WEB-INF/lib/*.jar entries:
  |     +-- detectBasePackage() checks which bundled JARs contain application classes
  |     +-- containsBasePackageClasses() filters out framework/library JARs
  |     +-- Application-origin bundled JARs are parsed; pure library JARs are skipped
  |
  +-- Build unified ClassInfo list (application + relevant bundled classes)
  |
  +-- Feed into standard CallGraphService / CallTreeBuilder pipeline
  |     (identical to JAR analysis from this point)
  |
  +-- Write analysis.json to data/jar/{normalizedKey}/
  |
  v
Browser receives "job-complete" SSE event
```

Key difference from JAR: the WAR parser must distinguish application classes (under
`WEB-INF/classes/`) from library JARs (under `WEB-INF/lib/`). It uses base-package
detection to decide which bundled JARs belong to the application vs. third-party
dependencies, so only relevant classes are included in the call graph.

After the call graph step, `WarParserService` runs the same three supplementary
extraction steps as `JarParserService` (see section 5.1 for details):
- `extractResourceFiles()` — reads from `WEB-INF/classes/` only
- `extractBundledJarInfo()` — reads from `WEB-INF/lib/`; writes `bundled-jars.json`
- `buildJarDependencyMap()` — cross-references invocations against the bundled-JAR
  class index; writes `jar-dep-map.json`

Both JAR and WAR executors produce identical storage layouts under
`data/jar/{normalizedKey}/`.

### 5.1 JAR Analysis Pipeline

```
User uploads .jar file
  |
  v
AnalyzerController receives multipart upload
  |
  v
QueueJob(JAR_UPLOAD) submitted to AnalysisQueueService
  |
  v
JarAnalysisExecutor.execute():
  |
  +-- Store uploaded JAR to data/jar/{normalizedKey}/stored.jar
  |
  +-- JarParserService.analyze():
  |     +-- BytecodeClassParser extracts ClassInfo/MethodInfo via ASM visitors
  |     +-- CallGraphService builds method-level call graphs
  |     +-- CallTreeBuilder constructs per-endpoint call trees
  |     +-- EndpointDetector identifies REST endpoints (Spring MVC annotations)
  |     +-- ProgressService.update() --> SSE events to browser
  |
  +-- Write analysis.json to data/jar/{normalizedKey}/
  |
  +-- Resource file extraction (in try-catch, non-fatal):
  |     +-- JarParserService.extractResourceFiles() scans the JAR entries:
  |     |     +-- Includes: .properties, .yml, .yaml, .json, .xml, .sql, .conf, .txt,
  |     |     |   JavaScript, HTML, and any non-binary resource
  |     |     +-- Excludes: .class files, META-INF/maven/ paths, binary content
  |     |     +-- Limits: max 50 files, max 200 KB per file
  |     +-- PersistenceService.storeResourceFiles() writes each file to
  |           data/jar/{normalizedKey}/resources/{filename}
  |
  +-- Bundled JAR info extraction (in try-catch, non-fatal):
  |     +-- JarParserService.extractBundledJarInfo() scans BOOT-INF/lib/ entries:
  |     |     +-- For each bundled JAR: reads MANIFEST.MF (if present) and up to
  |     |     |   10 resource files of the types listed above
  |     |     +-- Limits: max 100 bundled JARs
  |     +-- PersistenceService.storeBundledJarInfo() writes result to
  |           data/jar/{normalizedKey}/bundled-jars.json
  |
  +-- JAR dependency map build (in try-catch, non-fatal):
  |     +-- JarParserService.buildJarDependencyMap():
  |     |     +-- Pass 1 (index build): for each bundled JAR, maps every class name
  |     |     |   it contains to that JAR's filename → class→jar index
  |     |     +-- Pass 2 (cross-reference): for each invocation in the application's
  |     |     |   own classes, looks up the invoked class in the index; if found,
  |     |     |   records the calling app class under the bundled JAR entry
  |     |     +-- Only JARs with at least one application-class reference are emitted
  |     +-- PersistenceService.storeJarDepsMap() writes result to
  |           data/jar/{normalizedKey}/jar-dep-map.json
  |
  +-- (Optional) MongoCatalogService fetches MongoDB collection metadata
  |
  v
Browser receives "job-complete" SSE event
```

The same three supplementary extraction steps (resource files, bundled JAR info, JAR dependency map) are applied for WAR files via the equivalent methods in `WarParserService`. For WAR archives:
- `extractResourceFiles()` reads only from `WEB-INF/classes/` (nested JARs in `WEB-INF/lib/` are not included here)
- `extractBundledJarInfo()` reads from `WEB-INF/lib/`
- `buildJarDependencyMap()` uses the same cross-reference logic against the `WEB-INF/lib/` class index

### 5.2 PL/SQL Parser Analysis Pipeline

```
User selects entry points (procedures/packages) via UI
  |
  v
ParserAnalysisController receives analysis request
  |
  v
QueueJob(PARSER_ANALYSIS) submitted to AnalysisQueueService
  |
  v
ParserAnalysisExecutor.execute():
  |
  +-- FlowAnalysisMain orchestrates:
  |     +-- SchemaResolver resolves object names to schemas via Oracle queries
  |     |     (cached to data/cache-plsql/ as TSV)
  |     +-- DependencyCrawler walks the dependency tree:
  |     |     +-- Downloads PL/SQL source from Oracle (ALL_SOURCE)
  |     |     +-- Parses each object via PlSqlParserEngine
  |     |     +-- PlSqlAnalysisVisitor extracts flow nodes, table ops, calls
  |     |     +-- Recursively crawls called objects
  |     +-- TriggerDiscoverer finds triggers on referenced tables
  |     +-- ChunkedFlowWriter writes results as chunked JSON
  |
  +-- Write output to data/plsql-parse/{analysisName}/
  |
  v
Browser receives "job-complete" SSE event
```

### 5.3 Legacy PL/SQL Analysis Pipeline

```
User submits PL/SQL analysis via legacy UI
  |
  v
AnalyzerController (/api/plsql/analysis/analyze)
  |
  v
QueueJob(PLSQL_ANALYSIS) submitted
  |
  v
PlsqlAnalysisExecutor.execute():
  |
  +-- AnalysisService orchestrates:
  |     +-- Parser engine processes source code
  |     +-- CallGraphBuilder constructs call graph
  |     +-- TableOperationCollector / JoinOperationCollector /
  |     |   CursorOperationCollector / SequenceOperationCollector
  |     |   aggregate summary data
  |     +-- TriggerResolver resolves trigger metadata
  |
  +-- Write AnalysisResult to data/plsql/{analysisName}/
  |
  v
Browser receives "job-complete" SSE event
```

### 5.4 Claude AI Enrichment Pipeline

```
User clicks "Enrich with Claude" in UI
  |
  v
QueueJob(CLAUDE_ENRICH | CLAUDE_FULL_SCAN | ...) submitted
  |
  v
ClaudeJobExecutor.execute():
  |
  +-- Load analysis data from disk
  |
  +-- TreeChunker splits call trees into manageable chunks
  |     (respects max-chunk-chars, max-tree-nodes, max-chunk-depth)
  |
  +-- For each chunk:
  |     +-- ClaudePromptBuilder assembles prompt from template + chunk data
  |     +-- ClaudeProcessRunner spawns Claude CLI process:
  |     |     +-- Prompt piped via stdin (avoids Windows 32KB CLI arg limit)
  |     |     +-- stdout/stderr read in separate threads
  |     |     +-- Configurable timeout per endpoint
  |     +-- ClaudeResultMerger extracts JSON from Claude response
  |     +-- FragmentStore caches intermediate results
  |
  +-- CorrectionMerger applies corrections to analysis data
  |
  +-- CorrectionPersistence saves corrected version to disk
  |
  v
Browser receives "job-complete" SSE event
```

### 5.5 Connection Detection Pipeline

`MongoCatalogService` is responsible for scanning the uploaded JAR/WAR's bundled
configuration files and extracting database connection details for display in the
**Connections** modal. It detects three database types:

| Database | Detection Logic | Config Property Keys / Patterns |
|---|---|---|
| **MongoDB** | Looks for `spring.data.mongodb.uri`, `spring.data.mongodb.host`, or MongoClient URI strings in `.properties` and `.yaml` config files | `spring.data.mongodb.*`, `mongodb.uri`, `mongo.uri` |
| **Oracle** | Looks for JDBC URLs containing `oracle` or TNS-style connection descriptors | `spring.datasource.url` with `jdbc:oracle:*`, `oracle.jdbc.url` |
| **PostgreSQL** | Looks for JDBC URLs containing `postgresql` or `postgres` | `spring.datasource.url` with `jdbc:postgresql:*`; also checks `spring.datasource.driver-class-name` for `org.postgresql.Driver` |

Both `.properties` and `.yaml` (`.yml`) config files inside the archive are scanned.
Passwords found in any connection string or property value are masked as `****` before
the results are returned to the frontend.

The detection runs on demand when the user clicks the **Connections** button in the JAR
header. It does not run automatically during analysis. Results are not cached to disk;
the archive is re-scanned on each request.

---

## 6. Queue System

### 6.1 Architecture

The `AnalysisQueueService` is a Spring `@Service` that provides a unified, thread-safe
job queue for all analysis types. It enforces sequential execution -- only one analysis
job runs at a time -- preventing concurrent heavy workloads from exhausting memory or
CPU.

**Key design decisions:**
- **Single worker thread** -- A daemon `ExecutorService` with exactly one thread
  (`Executors.newSingleThreadExecutor`). Jobs execute sequentially in FIFO order.
- **Linked list queue** -- Pending jobs are stored in a `LinkedList<QueueJob>` guarded
  by an explicit `Object lock`.
- **History retention** -- Completed, failed, and cancelled jobs are kept in a bounded
  history list (`MAX_HISTORY = 50`).
- **SSE broadcasting** -- All state changes (queued, started, completed, failed,
  cancelled) are broadcast to connected SSE clients via `CopyOnWriteArrayList<SseEmitter>`.

### 6.2 Job Types

```java
public enum Type {
    JAR_UPLOAD,              // Upload + analyze a JAR file
    PLSQL_ANALYSIS,          // Full PL/SQL analysis (legacy path)
    PLSQL_FAST_ANALYSIS,     // Lightweight PL/SQL analysis
    PARSER_ANALYSIS,         // PL/SQL Parser flow analysis
    CLAUDE_ENRICH,           // Claude AI enrichment (batch)
    CLAUDE_ENRICH_SINGLE,    // Claude AI enrichment (single endpoint)
    CLAUDE_RESCAN,           // Re-scan with Claude
    CLAUDE_FULL_SCAN,        // Full Claude scan
    CLAUDE_CORRECT,          // Claude correction (batch)
    CLAUDE_CORRECT_SINGLE,   // Claude correction (single endpoint)
    PLSQL_CLAUDE_VERIFY      // PL/SQL Claude verification
}
```

### 6.3 Job Lifecycle

```
QUEUED  -->  RUNNING  -->  COMPLETE
                |
                +--->  FAILED
                |
                +--->  CANCELLED
```

**Status fields tracked per job:**
- `id` -- UUID-based short identifier (8 chars)
- `type` -- Job type enum
- `displayName` -- Human-readable name
- `status` -- Current lifecycle status
- `currentStep` -- Description of what is happening now
- `stepNumber` / `totalSteps` / `progressPercent` -- Progress tracking
- `submittedAt` / `startedAt` / `completedAt` -- Timestamps
- `error` -- Error message (if failed)
- `log` -- Synchronized list of log messages (last 50 exposed via API)
- `metadata` -- Arbitrary key-value pairs for job-specific parameters
- `followUpJob` -- Optional chained job to submit on completion

### 6.4 Job Executors

Each job type is handled by a dedicated executor class:

| Executor                  | Job Types                                          |
|---------------------------|----------------------------------------------------|
| `JarAnalysisExecutor`     | `JAR_UPLOAD`                                       |
| `PlsqlAnalysisExecutor`   | `PLSQL_ANALYSIS`                                   |
| `FastAnalysisExecutor`    | `PLSQL_FAST_ANALYSIS`                              |
| `ParserAnalysisExecutor`  | `PARSER_ANALYSIS`                                  |
| `ClaudeJobExecutor`       | `CLAUDE_ENRICH`, `CLAUDE_ENRICH_SINGLE`,           |
|                           | `CLAUDE_RESCAN`, `CLAUDE_FULL_SCAN`,               |
|                           | `CLAUDE_CORRECT`, `CLAUDE_CORRECT_SINGLE`,         |
|                           | `PLSQL_CLAUDE_VERIFY`                              |

### 6.5 Cancellation

Jobs can be cancelled in two ways:
- **Pending jobs** -- Removed from the queue immediately, status set to `CANCELLED`.
- **Running jobs** -- Status set to `CANCELLED`, worker thread interrupted via
  `Thread.interrupt()`. The executor must check for interruption and clean up.

### 6.6 Reordering

Clients can reorder pending jobs by sending an ordered list of job IDs to
`POST /api/queue/reorder`. The queue rebuilds itself in the specified order, appending
any unmentioned jobs at the end.

### 6.7 Follow-up Jobs

A job can specify a `followUpJob` map in its metadata. When the job completes
successfully, `AnalysisQueueService` automatically submits the follow-up job. This
enables chaining (e.g., run analysis, then enrich with Claude).

---

## 7. Claude AI Integration

### 7.1 Architecture Overview

Claude AI integration exists in **two separate implementations**, one for each
analysis domain:

| Location                                          | Bean Name                    | Type         | Domain       |
|---------------------------------------------------|------------------------------|--------------|--------------|
| `jar-analyzer-core/.../ClaudeProcessRunner.java`  | `jarClaudeProcessRunner`     | `@Component` | JAR analysis |
| `unified-web/.../parser/service/ClaudeProcessRunner.java` | `parserClaudeProcessRunner` | `@Service` | PL/SQL Parser|
| `unified-web/.../web/service/ClaudeProcessRunner.java` | (legacy PL/SQL)          | Plain POJO   | Legacy PL/SQL|

Both implementations share the same core approach:
1. Spawn Claude CLI as an external process via `ProcessBuilder`
2. Pipe prompts via stdin (avoiding the Windows 32KB command-line argument limit)
3. Read stdout/stderr in separate threads to prevent deadlocks
4. Track active processes per thread ID for targeted kill support
5. Enforce configurable timeouts

### 7.2 Process Execution Model

```
Java Application
  |
  +-- ProcessBuilder("claude", "--allowedTools", "Read,Grep,...", "-p", ...)
  |
  +-- Prompt piped to stdin (buffered, 8KB chunks, 64KB buffer)
  |
  +-- stdout reader thread --> StringBuilder
  |
  +-- stderr reader thread --> StringBuilder (logged)
  |
  +-- waitFor(timeoutSeconds) --> result or TimeoutException
  |
  +-- Process tree destroyed on timeout/cancellation
```

**Key implementation details:**

- **Stdin piping** uses a dedicated daemon thread with 64KB `BufferedOutputStream`.
  The thread writes in 8KB chunks. This avoids the Windows 32KB CLI argument limit
  that would cause error 206 if the prompt were passed as a command-line argument.

- **Process tracking** uses a `ConcurrentHashMap<Long, Process>` keyed by thread ID.
  This supports both targeted kills (single session) and bulk kills (all sessions for
  a JAR).

- **Prompt truncation** is applied when stdin content exceeds `MAX_PROMPT_CHARS`
  (configurable, default 180KB). A truncation notice is appended.

### 7.3 Environment Variables

The PL/SQL Parser's `ClaudeProcessRunner` pre-configures AWS Bedrock environment
variables for Claude:

| Variable                     | Default Value  |
|------------------------------|----------------|
| `CLAUDE_CODE_USE_BEDROCK`    | `1`            |
| `AWS_PROFILE`                | `ClaudeCode`   |
| `AWS_REGION`                 | `eu-central-1` |

These can be overridden at runtime via the `setEnv()` method.

### 7.4 Availability Detection

Both implementations check for Claude CLI availability by running `claude --version`.
The parser-side implementation caches the result for 60 seconds to avoid repeated
subprocess spawns.

### 7.5 Chunking Strategy

For large analysis results, the enrichment pipeline chunks the data before sending
to Claude:

- `TreeChunker` splits call trees based on configurable limits:
  - `max-chunk-chars` (default 50,000) -- Character budget per chunk
  - `max-tree-nodes` (default 500) -- Node count budget per chunk
  - `max-chunk-depth` (default 3) -- Depth budget per chunk
  - `max-prompt-chars` (default 180,000) -- Total prompt size ceiling

- `SwarmClusterer` groups related endpoints/methods for coherent analysis

### 7.6 Session Management

- `ClaudeSessionManager` tracks active Claude sessions per analysis
- Sessions can be individually killed (`POST .../sessions/{id}/kill`)
  or bulk-killed (`POST .../sessions/kill-all`)
- Each running Claude process is tracked by its thread ID for precise teardown

### 7.7 Persistence and Versioning

- `FragmentStore` caches individual Claude response fragments to disk
- `CorrectionPersistence` saves corrected analysis versions alongside originals
- Version management supports loading different analysis versions:
  - Static (original, no Claude data)
  - Claude-enriched
  - Previous Claude version
  - Revert to static

---

## 7a. Spring DI Dispatch Resolution

### Overview

When the call tree builder encounters a method call on a Spring-injected field, it
determines a **dispatch type** describing how confidently the target implementation
was identified. This drives the colored badges shown in the UI and the navigation
links in the decompile code view.

### Dispatch Types

| Dispatch Type | Meaning | UI Color |
|---|---|---|
| `DIRECT` | Single implementation or concrete class call — no ambiguity | (no badge) |
| `QUALIFIED` | Resolved via `@Qualifier`, `@Named`, or custom qualifier annotation | Green |
| `HEURISTIC` | Resolved via field-name substring match against implementation class names | Indigo |
| `AMBIGUOUS_IMPL` | Multiple implementations exist; none of the narrowing strategies succeeded | Orange |
| `LIST_INJECT` | Field is `List<Interface>` — all implementations are injected and run | Cyan |
| `RECURSIVE` | Method calls itself (directly or indirectly); traversal stops here | Red |
| `INTERFACE_FALLBACK` | Interface found but no concrete implementation exists in the analyzed classes | Red |
| `PRIMARY` | Resolved using Spring's `@Primary` bean | Purple |
| `DEFAULT_METHOD` | Java 8+ default method on an interface (no concrete override) | Grey |

### Resolution Algorithm (CallTreeBuilder)

For each injected field whose type is an interface or abstract class, the builder:

1. **Collects all implementations** from `interfaceImplMap` (built during class parsing).

2. **Single-impl shortcut**: If exactly one implementation exists → `DIRECT`.

3. **Strategy 1 — `@Qualifier` / `@Named`**: If the field has a `@Qualifier` or `@Named`
   annotation whose value matches exactly one implementation (by bean name, simple name,
   or FQN suffix) → `QUALIFIED`.

4. **Strategy 2 — Custom qualifier annotation**: If the field has any non-standard
   annotation (not in the Spring/Jakarta standard set) with an `attributes.value` that
   is a substring of exactly one implementation's simple name → `QUALIFIED`.
   Example: `@AnalysisType("trend")` on a field matches `TrendAnalysisStrategy`.

5. **Strategy 3 — Field name heuristic**: Split the field name at camelCase boundaries
   (e.g., `trendAnalysisStrategy` → `["trend", "analysis", "strategy"]`). If any
   segment (longer than 3 chars) matches exactly one implementation's simple name as
   a substring → `HEURISTIC`.

6. **`@Primary` fallback**: If one implementation is annotated `@Primary` → `PRIMARY`.

7. **`List<Interface>` detection**: If the field type is `List<T>` where `T` is an
   interface → all implementations → `LIST_INJECT`.

8. **Exhausted strategies**: → `AMBIGUOUS_IMPL`.

### Recursive Back-Edge Detection

When `CallTreeBuilder` encounters a call to a method already present in the current
call stack (via `visitedSet`):
- Sets `callType = "RECURSIVE"` and `recursive = true` on the child node.
- Sets `dispatchType = "RECURSIVE"` on the child node.
- Does NOT recurse further (prevents infinite loops).
- The `isRecursive()` guard in the builder prevents subsequent `setDispatchType()`
  calls from overwriting the `RECURSIVE` label.
- `rewriteCrossModuleRecursive` also skips nodes with `callType = "RECURSIVE"` to
  preserve the back-edge marker.

### Frontend Field-Dispatch Narrowing (summary-codeview.js)

The decompile code view independently re-runs the narrowing logic client-side to
render navigation links:

1. For each injected field of interface/abstract type, builds a `fieldDispatch` map
   using the same three strategies (Qualifier, custom annotation, field-name heuristic).
2. Uses `receiverFieldName` from bytecode invocation metadata to match a given line's
   method call to its specific field.
3. If the field resolved to one implementation → renders a colored clickable link.
4. If AMBIGUOUS → renders a picker span; clicking it opens an implementation chooser.
5. Only adds a method name to `fieldAwareMethods` (which removes it from the generic
   navTargets map) when the invocation's owner class is an interface or abstract class.
   This preserves navigation for concrete class methods (e.g.,
   `orchestrator.runAllStrategiesOnMetric()`) even when they have a `receiverFieldName`.

#### Bug Fix — Dispatch Links in Summary Modal Context

When the decompile popup is opened from the Summary tab (rather than from the Code
Structure tab), two initialization problems previously caused dispatch navigation links
to be absent from the modal output:

- **`_classIdx` lazy initialization** — `_classIdx` (the class-name-to-index lookup map
  built in `JA.nav`) was not re-initialized inside `_buildDecompCode` and
  `_buildFallbackCode` when those functions were called from the Summary modal context.
  The fix adds a lazy-init guard at the entry point of both functions: if `_classIdx` is
  null or empty, it is rebuilt from `JA.app.currentAnalysis.classes` before proceeding.

- **`_implMap` stale empty guard** — `_implMap` (the interface-to-implementations map)
  was initialized to an empty object early in the module load. If the analysis data
  arrived after this early init, `_implMap` remained empty and dispatch links were never
  rendered. The fix adds a re-initialization check: if `_implMap` has no keys at the
  time `_buildDecompCode` runs but `JA.nav._implMap` is populated, the local reference
  is refreshed before the narrowing logic executes.

After these fixes, the Summary tab's decompile modal renders the same colored
"→ N implementations: …" dispatch navigation links as the Code Structure tab's
decompile panel.

---

## 7b. Chatbox and Chat History Architecture

### Overview

Two chat systems are available:

| System | Bean | Storage | Scope |
|---|---|---|---|
| **Floating Chatbox** | `ChatboxController` + `ChatboxService` | `data/jar/{key}/chat/` | Per-JAR |
| **Classic Chat Panel** | `ChatController` + `ChatService` | `data/claude-chatbot/{sessionId}/` | Cross-JAR session |

### Floating Chatbox

- Each JAR has its own conversation thread. Switching JARs switches context.
- `ChatboxService` maintains in-memory conversation state, serialized to disk on each
  message for persistence across restarts.
- The AI has access to the full analysis JSON for the selected JAR via context injection.
- History stored at: `data/jar/{normalizedKey}/chat/history.json`
- REST API: `POST /api/jars/{id}/chat`, `GET /api/jars/{id}/chat/history`,
  `DELETE /api/jars/{id}/chat/history`

### Classic Chat Sessions

- `ChatHistoryService` manages session lifecycle (create, list, delete, get).
- Each session is a directory under `data/claude-chatbot/{sessionId}/`.
- Sessions support multi-turn conversation with full context retention.
- A session can be exported as a structured report via `GET /api/chat/sessions/{id}/report`.
- `ChatService` handles message routing and Claude process invocation.

---

## 7c. Correction Versioning

The JAR analyzer maintains up to three versions of analysis data per JAR:

| File | Description |
|---|---|
| `analysis.json` | Raw static analysis (always the original, never overwritten by Claude) |
| `analysis_corrected.json` | Latest Claude-enriched version (overwritten on each new correction run) |
| `analysis_corrected_prev.json` | The prior Claude version (one round back) |

### Version Loading Flow

```
User clicks "View Corrected" → loads analysis_corrected.json
User clicks "View Static"    → loads analysis.json
User clicks "View Previous"  → loads analysis_corrected_prev.json
User clicks "Revert"         → deletes _corrected.json, returns to static
```

### Correction Merge Process

`CorrectionMerger` applies Claude's output on top of the static analysis:
1. Reads static `analysis.json` as the base.
2. Applies each correction from `corrections/{endpointName}_correction.json`.
3. Writes the merged result to `analysis_corrected.json`.
4. Before writing, moves the current `_corrected.json` to `_corrected_prev.json`.

`CorrectionPersistence` manages reading/writing these files and exposes version
availability so the frontend can enable/disable the View Previous button.

---

## 8. REST API Map

### 8.1 Queue Management (`/api/queue`)

| Method | Path                    | Description                         |
|--------|-------------------------|-------------------------------------|
| GET    | `/api/queue`            | Get full queue state (current, pending, history) |
| GET    | `/api/queue/{id}`       | Get single job details              |
| POST   | `/api/queue/{id}/cancel`| Cancel a queued or running job      |
| POST   | `/api/queue/reorder`    | Reorder pending jobs                |
| GET    | `/api/queue/events`     | SSE stream of queue state changes   |

### 8.2 JAR Analyzer (`/api/jar/*`)

**Analysis & Data** (`/api/jar/jars`):

| Method | Path                                          | Description                              |
|--------|-----------------------------------------------|------------------------------------------|
| POST   | `/api/jar/jars`                               | Upload and analyze a JAR file            |
| POST   | `/api/jar/jars/analyze-local`                 | Analyze a JAR from local filesystem path |
| GET    | `/api/jar/jars`                               | List all analyzed JARs                   |
| GET    | `/api/jar/jars/{id}`                          | Get analysis result for a specific JAR   |
| DELETE | `/api/jar/jars/{id}`                          | Delete a JAR analysis                    |
| GET    | `/api/jar/jars/{id}/summary`                  | Endpoint summary with enrichment data    |
| GET    | `/api/jar/jars/{id}/summary/headers`          | Summary column headers                   |
| GET    | `/api/jar/jars/{id}/summary/external-calls`   | External call summary                    |
| GET    | `/api/jar/jars/{id}/summary/dynamic-flows`    | Dynamic flow summary                     |
| GET    | `/api/jar/jars/{id}/summary/aggregation-flows`| Aggregation flow summary                 |
| GET    | `/api/jar/jars/{id}/summary/beans`            | Spring bean summary                      |
| GET    | `/api/jar/jars/{id}/classes/tree`             | Class tree structure                     |
| GET    | `/api/jar/jars/{id}/classes/by-index/{idx}`   | Get class by index                       |
| GET    | `/api/jar/jars/{id}/endpoints/by-index/{idx}/call-tree` | Call tree for endpoint      |
| GET    | `/api/jar/jars/{id}/versions`                 | List analysis versions                   |
| GET    | `/api/jar/jars/{id}/collections`              | MongoDB collections for this JAR         |
| POST   | `/api/jar/jars/export-excel`                  | Export analysis to XLSX                  |
| POST   | `/api/jar/jars/reanalyze-all`                 | Re-analyze all stored JARs               |
| POST   | `/api/jar/jars/{id}/reanalyze`                | Re-analyze a specific JAR                |

**Decompilation & Claude** (`/api/jar/jars`):

| Method | Path                                                   | Description                         |
|--------|--------------------------------------------------------|-------------------------------------|
| GET    | `/api/jar/jars/{id}/decompile`                         | Decompile a class via CFR           |
| GET    | `/api/jar/jars/settings/claude-status`                 | Check Claude CLI availability       |
| GET    | `/api/jar/jars/settings/claude-test`                   | Run Claude stdin pipe integration test |
| GET    | `/api/jar/jars/{id}/claude-progress`                   | Get Claude enrichment progress      |
| GET    | `/api/jar/jars/{id}/claude-fragments`                  | List Claude response fragments      |
| GET    | `/api/jar/jars/{id}/claude-fragments/{filename}`       | Get a specific fragment             |
| GET    | `/api/jar/jars/{id}/endpoints`                         | List endpoints for a JAR            |
| GET    | `/api/jar/jars/{id}/endpoints/{endpoint}/nodes`        | Get call tree nodes                 |
| GET    | `/api/jar/jars/{id}/endpoints/{endpoint}/nodes/{nodeId}` | Get specific node              |
| GET    | `/api/jar/jars/{id}/endpoints/{endpoint}/call-tree`    | Get call tree for endpoint          |
| POST   | `/api/jar/jars/{id}/claude-enrich-single`              | Enrich single endpoint with Claude  |
| POST   | `/api/jar/jars/{id}/claude-rescan`                     | Re-scan with Claude                 |
| POST   | `/api/jar/jars/{id}/claude-full-scan`                  | Full Claude scan                    |
| POST   | `/api/jar/jars/{id}/claude-correct`                    | Run Claude corrections (batch)      |
| POST   | `/api/jar/jars/{id}/claude-correct-single`             | Run Claude correction (single)      |
| POST   | `/api/jar/jars/{id}/revert-claude`                     | Revert to pre-Claude version        |
| GET    | `/api/jar/jars/{id}/corrections`                       | List correction summaries           |
| GET    | `/api/jar/jars/{id}/corrections/{endpoint}`            | Get correction for endpoint         |
| GET    | `/api/jar/jars/{id}/correction-logs`                   | List correction log files           |
| GET    | `/api/jar/jars/{id}/correction-logs/{endpointName}`    | Correction log for endpoint         |
| GET    | `/api/jar/jars/{id}/correction-logs/file/{fileName}`   | Read correction log file            |
| GET    | `/api/jar/jars/{id}/run-logs`                          | List run log files                  |
| GET    | `/api/jar/jars/{id}/run-logs/{logName}`                | Read run log file                   |
| GET    | `/api/jar/jars/{name}/claude-stats`                    | Claude enrichment statistics        |
| GET    | `/api/jar/jars/{name}/claude-stats/{sessionId}`        | Stats for specific session          |
| POST   | `/api/jar/jars/{id}/kill-sessions`                     | Kill all Claude sessions for JAR    |
| GET    | `/api/jar/jars/sessions`                               | List all active Claude sessions     |
| POST   | `/api/jar/jars/sessions/{sessionId}/kill`              | Kill specific Claude session        |
| POST   | `/api/jar/jars/{id}/fetch-catalog`                     | Fetch MongoDB catalog               |
| GET    | `/api/jar/jars/{id}/catalog`                           | Get cached MongoDB catalog          |
| GET    | `/api/jar/jars/{id}/resources`                         | List extracted resource files       |
| GET    | `/api/jar/jars/{id}/resources/{filename:.+}`           | Fetch content of a resource file    |
| GET    | `/api/jar/jars/{id}/bundled-jars`                      | List bundled JARs with manifest and resource file entries |
| GET    | `/api/jar/jars/{id}/jar-dep-map`                       | Get map of bundled JAR → app classes that call into it |

**Progress & Logs:**

| Method | Path                    | Description                         |
|--------|-------------------------|-------------------------------------|
| GET    | `/api/jar/progress`     | SSE stream of JAR analysis progress |
| GET    | `/api/jar/logs`         | Tail the application log            |

**Chatbox** (`/api/jars`):

| Method | Path                            | Description                     |
|--------|---------------------------------|---------------------------------|
| POST   | `/api/jars/{id}/chat`           | Send message to chatbot         |
| GET    | `/api/jars/{id}/chat/history`   | Get chat history                |
| DELETE | `/api/jars/{id}/chat/history`   | Clear chat history              |

### 8.3 PL/SQL Parser (`/api/parser/*`)

**Analysis:**

| Method | Path                                        | Description                         |
|--------|---------------------------------------------|-------------------------------------|
| GET    | `/api/parser/analyses`                      | List all parser analyses            |
| POST   | `/api/parser/analyze`                       | Start a new parser analysis         |
| GET    | `/api/parser/analyses/{name}/index`         | Get analysis index                  |
| GET    | `/api/parser/analyses/{name}/node/{file}`   | Get a specific parsed node          |
| GET    | `/api/parser/analyses/{name}/tables`        | Table operations summary            |
| GET    | `/api/parser/analyses/{name}/call-graph`    | Call graph data                     |
| GET    | `/api/parser/analyses/{name}/source/{file}` | Get PL/SQL source file              |
| GET    | `/api/parser/analyses/{name}/procedures`    | List parsed procedures              |
| GET    | `/api/parser/analyses/{name}/joins`         | Join operations summary             |
| GET    | `/api/parser/analyses/{name}/cursors`       | Cursor usage summary                |
| GET    | `/api/parser/analyses/{name}/sequences`     | Sequence references                 |
| GET    | `/api/parser/analyses/{name}/call-tree/{nodeId}` | Call tree for node            |
| GET    | `/api/parser/analyses/{name}/call-tree/{nodeId}/callers` | Reverse call tree    |
| GET    | `/api/parser/analyses/{name}/resolver/{type}` | Schema resolver results by type  |

**Claude Enrichment for Parser:**

| Method | Path                                                      | Description                     |
|--------|-----------------------------------------------------------|---------------------------------|
| POST   | `/api/parser/analyses/{name}/claude/verify`               | Start Claude verification       |
| GET    | `/api/parser/analyses/{name}/claude/progress`             | Get verification progress       |
| GET    | `/api/parser/analyses/{name}/claude/progress/stream`      | SSE stream of progress          |
| GET    | `/api/parser/analyses/{name}/claude/result`               | Get verification result         |
| GET    | `/api/parser/analyses/{name}/claude/chunks`               | List prompt chunks              |
| GET    | `/api/parser/analyses/{name}/claude/chunks/summary`       | Chunk summary statistics        |
| GET    | `/api/parser/analyses/{name}/claude/chunks/{id}`          | Get specific chunk              |
| GET    | `/api/parser/analyses/{name}/claude/table-chunks`         | Table-oriented chunks           |
| POST   | `/api/parser/analyses/{name}/claude/review`               | Submit manual review            |
| POST   | `/api/parser/analyses/{name}/claude/apply`                | Apply corrections               |
| POST   | `/api/parser/analyses/{name}/claude/sessions/{id}/kill`   | Kill session                    |
| GET    | `/api/parser/analyses/{name}/claude/versions`             | List analysis versions          |
| POST   | `/api/parser/analyses/{name}/claude/versions/load-static` | Load static version             |
| POST   | `/api/parser/analyses/{name}/claude/versions/load-claude` | Load Claude-enriched version    |
| POST   | `/api/parser/analyses/{name}/claude/versions/load-prev`   | Load previous version           |
| POST   | `/api/parser/analyses/{name}/claude/versions/revert`      | Revert to static                |
| GET    | `/api/parser/claude/status`                               | Claude CLI status               |
| GET    | `/api/parser/claude/sessions`                             | List all active sessions        |
| POST   | `/api/parser/claude/sessions/kill-all`                    | Kill all active sessions        |

**Configuration:**

| Method | Path                       | Description                    |
|--------|----------------------------|--------------------------------|
| GET    | `/api/parser/config`       | Get parser configuration       |

### 8.4 Legacy PL/SQL Analyzer (`/api/plsql/*`)

**Analysis** (`/api/plsql/analysis`):

| Method | Path                                             | Description                    |
|--------|--------------------------------------------------|--------------------------------|
| POST   | `/api/plsql/analysis/analyze`                    | Start full analysis            |
| POST   | `/api/plsql/analysis/analyze-fast`               | Start fast analysis            |
| GET    | `/api/plsql/analysis`                            | List analyses                  |
| GET    | `/api/plsql/analysis/history`                    | Analysis history               |
| GET    | `/api/plsql/analysis/history/{name}`             | Get specific history entry     |
| DELETE | `/api/plsql/analysis/history/{name}`             | Delete history entry           |
| GET    | `/api/plsql/analysis/versions`                   | List analysis versions         |
| GET    | `/api/plsql/analysis/call-graph`                 | Get call graph                 |
| GET    | `/api/plsql/analysis/detail/{procName}`          | Procedure detail               |
| GET    | `/api/plsql/analysis/call-tree/{procName}`       | Call tree for procedure        |
| GET    | `/api/plsql/analysis/call-tree/{procName}/callers` | Reverse call tree           |
| GET    | `/api/plsql/analysis/tables`                     | Table operations               |
| GET    | `/api/plsql/analysis/tables/{tableName}`         | Operations on specific table   |
| GET    | `/api/plsql/analysis/tables/{tableName}/metadata`| Table metadata                 |
| GET    | `/api/plsql/analysis/tables/{tableName}/triggers`| Table triggers                 |
| GET    | `/api/plsql/analysis/sequences`                  | Sequence references            |
| GET    | `/api/plsql/analysis/joins`                      | Join operations                |
| GET    | `/api/plsql/analysis/cursors`                    | Cursor usage                   |
| GET    | `/api/plsql/analysis/errors`                     | Parse errors                   |
| GET    | `/api/plsql/analysis/procedures`                 | List all procedures            |
| GET    | `/api/plsql/analysis/source/{owner}/{objectName}`| PL/SQL source                  |
| GET    | `/api/plsql/analysis/references/{objectName}`    | Object references              |
| GET    | `/api/plsql/analysis/search`                     | Search procedures/objects      |
| GET    | `/api/plsql/analysis/jobs`                       | Queue jobs (legacy)            |
| GET    | `/api/plsql/analysis/jobs/{jobId}`               | Get job (legacy)               |
| POST   | `/api/plsql/analysis/jobs/{jobId}/cancel`        | Cancel job (legacy)            |

**Claude Verification** (`/api/plsql/claude`):

| Method | Path                                        | Description                     |
|--------|---------------------------------------------|---------------------------------|
| GET    | `/api/plsql/claude/status`                  | Claude CLI status               |
| POST   | `/api/plsql/claude/verify`                  | Start verification              |
| GET    | `/api/plsql/claude/progress`                | Get progress                    |
| GET    | `/api/plsql/claude/progress/stream`         | SSE progress stream             |
| GET    | `/api/plsql/claude/result`                  | Get latest result               |
| GET    | `/api/plsql/claude/result/{analysisName}`   | Get result by analysis name     |
| GET    | `/api/plsql/claude/chunks`                  | List chunks                     |
| GET    | `/api/plsql/claude/chunks/summary`          | Chunk summary                   |
| GET    | `/api/plsql/claude/chunks/{chunkId}`        | Get specific chunk              |
| GET    | `/api/plsql/claude/table-chunks`            | Table-oriented chunks           |
| GET    | `/api/plsql/claude/sessions`                | Active sessions                 |
| POST   | `/api/plsql/claude/sessions/{id}/kill`      | Kill session                    |
| POST   | `/api/plsql/claude/sessions/kill-all`       | Kill all sessions               |
| GET    | `/api/plsql/claude/versions`                | List versions                   |
| POST   | `/api/plsql/claude/versions/load-static`    | Load static version             |
| POST   | `/api/plsql/claude/versions/load-claude`    | Load Claude version             |
| POST   | `/api/plsql/claude/versions/load-prev`      | Load previous version           |
| POST   | `/api/plsql/claude/versions/revert`         | Revert to static                |

**Database** (`/api/plsql/db`):

| Method | Path                                               | Description                     |
|--------|----------------------------------------------------|---------------------------------|
| GET    | `/api/plsql/db/users`                              | List Oracle DB users/schemas    |
| POST   | `/api/plsql/db/test`                               | Test database connection        |
| GET    | `/api/plsql/db/objects/{username}`                 | List objects for a schema       |
| GET    | `/api/plsql/db/source/{username}/{objectName}`     | Download PL/SQL source          |
| GET    | `/api/plsql/db/cached-source`                      | Get cached source files         |
| GET    | `/api/plsql/db/packages/{username}`                | List packages in schema         |
| GET    | `/api/plsql/db/package/{username}/{packageName}`   | Package details                 |
| GET    | `/api/plsql/db/find/{objectInput}`                 | Find object across schemas      |
| GET    | `/api/plsql/db/table-info/{tableName}`             | Table metadata from Oracle      |
| POST   | `/api/plsql/db/query`                              | Run ad-hoc SQL query            |

**Configuration** (`/api/plsql/config`):

| Method | Path                                                                     | Description              |
|--------|--------------------------------------------------------------------------|--------------------------|
| GET    | `/api/plsql/config`                                                      | Get current config       |
| GET    | `/api/plsql/config/dir`                                                  | Get config/data dirs     |
| GET    | `/api/plsql/config/schemas`                                              | List configured schemas  |
| GET    | `/api/plsql/config/projects`                                             | List projects            |
| POST   | `/api/plsql/config/projects`                                             | Create project           |
| PUT    | `/api/plsql/config/projects/{name}`                                      | Update project           |
| DELETE | `/api/plsql/config/projects/{name}`                                      | Delete project           |
| GET    | `/api/plsql/config/projects/{project}/environments`                      | List environments        |
| POST   | `/api/plsql/config/projects/{project}/environments`                      | Create environment       |
| PUT    | `/api/plsql/config/projects/{project}/environments/{env}`                | Update environment       |
| DELETE | `/api/plsql/config/projects/{project}/environments/{env}`                | Delete environment       |
| GET    | `/api/plsql/config/projects/{project}/environments/{env}/connections`    | List connections         |
| POST   | `/api/plsql/config/projects/{project}/environments/{env}/connections`    | Create connection        |
| POST   | `/api/plsql/config/projects/{project}/environments/{env}/connections/test` | Test connection        |
| PUT    | `/api/plsql/config/projects/{project}/environments/{env}/connections/{conn}` | Update connection  |
| DELETE | `/api/plsql/config/projects/{project}/environments/{env}/connections/{conn}` | Delete connection  |
| POST   | `/api/plsql/config/test-connection`                                      | Test ad-hoc connection   |
| GET    | `/api/plsql/config/resolve`                                              | Resolve config path      |

**Source, Logs, Progress:**

| Method | Path                       | Description                           |
|--------|----------------------------|---------------------------------------|
| GET    | `/api/plsql/source/file`   | Read a PL/SQL source file from disk   |
| GET    | `/api/plsql/logs`          | Tail the application log              |
| GET    | `/api/plsql/progress`      | SSE stream of PL/SQL analysis progress|

### 8.5 Shared APIs

| Method | Path                         | Description                          |
|--------|------------------------------|--------------------------------------|
| GET    | `/api/config/polling`        | Get polling configuration            |
| GET    | `/api/sessions`              | Global session overview (all tools)  |
| GET    | `/api/sessions/summary`      | Session summary statistics           |
| GET    | `/api/chat/config`           | Chat feature configuration           |
| POST   | `/api/chat/sessions`         | Create new chat session              |
| GET    | `/api/chat/sessions`         | List chat sessions                   |
| GET    | `/api/chat/sessions/{id}`    | Get chat session                     |
| DELETE | `/api/chat/sessions/{id}`    | Delete chat session                  |
| POST   | `/api/chat/sessions/{id}/messages` | Send message in session        |
| GET    | `/api/chat/sessions/{id}/report`   | Generate chat report           |
| POST   | `/api/chat/files/read`       | Read file for chat context           |
| POST   | `/api/chat/files/write`      | Write file from chat                 |
| GET    | `/api/docs`                  | List documentation pages             |
| GET    | `/api/docs/{id}`             | Get documentation page               |

---

## 9. Storage Layout

### 9.1 Configuration Directory (`config/`)

```
config/
  plsql-config.yaml              Oracle DB connections (projects/environments)
  domain-config.json             Domain clustering rules for JAR analysis
  prompts/
    plsql-verification.txt       PL/SQL verification prompt template
    java-mongo-analysis.txt      JAR analysis prompt (MongoDB)
    java-mongo-chunk-analysis.txt
    java-mongo-correction.txt
    java-both-analysis.txt       JAR analysis prompt (both DBs)
    java-both-chunk-analysis.txt
    java-both-correction.txt
    java-oracle-analysis.txt     JAR analysis prompt (Oracle)
    java-oracle-chunk-analysis.txt
    java-oracle-correction.txt
```

### 9.2 Data Directory (`data/`)

```
data/
  unified-analyzer.log           Main application log (rolling, 10MB max)
  unified-analyzer.*.log.gz      Rotated compressed logs

  jar/                           JAR Analyzer data
    {normalizedKey}/             Per-JAR analysis directory
      stored.jar                 Uploaded JAR file
      analysis.json              Raw analysis result (100-400MB for large JARs)
      analysis_corrected.json    Claude-enriched version
      analysis_corrected_prev.json  Previous Claude version
      mongo-catalog.json         MongoDB collection metadata
      claude/                    Claude enrichment fragments
      corrections/               Claude correction files
      endpoints/                 Per-endpoint breakdown files
      chat/                      Chatbot conversation history
      resources/                 Extracted resource files (.properties, .yml, .yaml,
                                 .json, .xml, .sql, .conf, .txt, JS, HTML, etc.) —
                                 max 50 files, 200 KB each; populated during analysis
      bundled-jars.json          Bundled library metadata: list of JARs found in
                                 BOOT-INF/lib/ (JAR) or WEB-INF/lib/ (WAR), each with
                                 MANIFEST.MF content and up to 10 resource file entries;
                                 max 100 JARs; written by extractBundledJarInfo()
      jar-dep-map.json           JAR dependency map: bundled JAR filename → list of
                                 application class names that invoke methods from that
                                 JAR; only JARs with at least one reference appear;
                                 written by buildJarDependencyMap()

  plsql/                         Legacy PL/SQL Analyzer data
    {analysisName}/              Per-analysis directory
      claude/                    Claude verification results

  plsql-parse/                   PL/SQL Parser data
    {analysisName}/              Per-analysis directory
      index.json                 Analysis index (procedure list, metadata)
      *.json                     Chunked flow analysis results per object
      source/                    Downloaded PL/SQL source files
      call-graph.json            Call graph data
      tables.json                Table operations summary
      claude/                    Claude enrichment results

  cache-plsql/                   SchemaResolver disk cache
    *.tsv                        Cached Oracle query results (tab-separated)

  claude-chatbot/                Classic chat session data
    {sessionId}/                 Per-session directory
```

### 9.3 Data Layout Migration

`ConfigDirService` automatically migrates data from legacy flat layouts to
per-analysis layouts on startup. The migration is idempotent (safe to run
multiple times):

- **JAR data:** Flat directories (`data/jar/analysis/`, `data/jar/jars/`,
  `data/jar/claude/`, etc.) are consolidated into per-JAR directories
  (`data/jar/{normalizedKey}/`).

- **PL/SQL data:** Claude data from `data/plsql/claude/{name}/` is moved into
  `data/plsql/{name}/claude/`.

---

## 10. Frontend Architecture

### 10.1 Technology

The frontend is built with vanilla JavaScript using the IIFE (Immediately Invoked
Function Expression) module pattern. There is no build step, no bundler, no
transpilation. CSS and JS files are served as static resources directly by Spring Boot.

The visual design uses a Catppuccin-like dark theme across all three UIs, providing
a consistent low-contrast color palette with accent colors for interactive elements.

### 10.2 Static Resource Structure

```
unified-web/src/main/resources/static/
  index.html                     Home page (launcher for both tools)

  jar/                           JAR Analyzer UI
    index.html                   Main page
    style.css                    Primary stylesheet
    css/
      chatbox.css                Chatbox styles
    js/
      api.js                     API client
      dashboard.js               Dashboard view
      sidebar.js                 Navigation sidebar
      summary.js                 Endpoint summary table
      summary-claude.js          Claude enrichment columns
      summary-corrections.js     Correction overlay
      summary-explorer.js        Tree explorer
      summary-trace.js           Call trace view
      summary-export.js          Export functionality
      summary-export-excel.js    Excel export
      summary-export-modal.js    Export modal dialog
      summary-export-style.js    Export styling
      summary-col-filter.js      Column filter
      summary-corr-logbrowser.js Correction log viewer
      summary-popups.js          Popup overlays
      summary-vert-popup.js      Vertical popup
      summary-codeview.js        Decompiled code viewer
      summary-helpers.js         Shared utilities
      summary-aggregation.js     Aggregation flow view
      summary-dynamic.js         Dynamic flow view
      summary-scheduled.js       Scheduled task view
      summary-vertical.js        Vertical layout view
      codeTree.js                Class tree browser
      codeTreePanel.js           Code tree panel
      codeTreeViews.js           Code tree views
      callGraph.js               Call graph visualization
      endpointList.js            Endpoint listing
      nav.js                     Navigation
      sessions.js                Session management
      chatbox.js                 AI chatbox
      chat-toggle.js             Chat panel toggle
      logviewer.js               Log viewer
      utils.js                   Utilities

  plsql/                         Legacy PL/SQL Analyzer UI
    index.html                   Main page
    css/
      style.css                  Primary stylesheet
      summary-views.css          Summary view styles
    js/
      api.js                     API client
      app.js                     Application entry
      summary.js                 Summary table
      summary-analyzer.js        Analyzer view
      summary-tables.js          Table operations view
      summary-claude.js          Claude enrichment
      summary-corrections.js     Corrections view
      summary-explorer.js        Tree explorer
      summary-trace.js           Call trace
      summary-export.js          Export
      summary-export-excel.js    Excel export
      summary-export-modal.js    Export modal
      summary-export-style.js    Export styling
      summary-col-filter.js      Column filter
      summary-corr-logbrowser.js Correction log viewer
      summary-helpers.js         Shared utilities
      summary-progress.js        Progress tracking
      claude.js                  Claude integration
      callGraph.js               Call graph
      config.js                  Configuration
      cursors.js                 Cursor view
      export.js                  Export utilities
      joins.js                   Join view
      logViewer.js               Log viewer
      predicates.js              Predicate analysis
      sourceView.js              Source code viewer
      tableFramework.js          Table framework
      tableOps.js                Table operations
      triggerModal.js            Trigger detail modal

  parser/                        PL/SQL Parser UI
    index.html                   Main page
    css/
      base.css                   Base styles and variables
      buttons.css                Button styles
      sidebar.css                Sidebar navigation
      topbar.css                 Top bar
      forms.css                  Form elements
      scrollbar.css              Custom scrollbars
      variables.css              CSS custom properties
      summary.css                Summary table
      table-ops.css              Table operations
      table-detail.css           Table detail view
      call-graph-viz.css         Call graph visualization
      call-trace.css             Call trace
      chunk-viewer.css           Chunk viewer
      claude-enrichment.css      Claude enrichment panel
      claude-overview.css        Claude overview
      claude-insights.css        Claude insights
      column-filter.css          Column filter
      complexity.css             Complexity metrics
      config.css                 Configuration panel
      detail-header.css          Detail header
      error-modal.css            Error modal
      ex-handlers.css            Exception handlers
      export.css                 Export
      home.css                   Home view
      join-detail.css            Join detail
      join-summary.css           Join summary
      log-viewer.css             Log viewer
      pagination.css             Pagination controls
      perf-summary.css           Performance summary
      query-runner.css           Query runner
      refs.css                   References view
      right-panel.css            Right panel
      sequences.css              Sequences view
      source-view.css            Source code view
      statements.css             Statement view
      sync.css                   Sync indicators
      toast.css                  Toast notifications
      tooltips.css               Tooltips
      trace.css                  Trace view
      trigger-detail.css         Trigger detail
      triggers.css               Triggers list
    js/
      home.js                    Home/landing view
      router.js                  Client-side routing
      config.js                  Configuration
      history.js                 Analysis history
      leftPanel.js               Left navigation panel
      summary.js                 Summary table
      tableFramework.js          Table framework
      tableOps.js                Table operations
      callGraph.js               Call graph
      callGraphViz.js            Call graph visualization
      callGraphModal.js          Call graph modal
      callGraphFullscreen.js     Fullscreen graph
      chunkViewer.js             Chunk viewer
      claudeEnrichment.js        Claude enrichment panel
      claudeInsights.js          Claude insights panel
      complexity.js              Complexity metrics
      cursors.js                 Cursor view
      detailHeader.js            Detail header
      errorModal.js              Error modal
      exHandlers.js              Exception handlers
      export.js                  Export
      joinSummary.js             Join summary
      joins.js                   Join detail
      perfSummary.js             Performance summary
      refs.js                    Object references
      scope.js                   Scope analysis
      sequences.js               Sequence references
      sourceView.js              Source code viewer
      statements.js              Statement analysis
      tooltips.js                Tooltips
      triggerDetail.js           Trigger detail
      triggers.js                Trigger list
      utils.js                   Utilities

  shared/                        Shared assets
    chat.js                      Classic chat widget
    chat.css                     Classic chat styles
    chatbox.js                   Floating chatbox widget
    chatbox.css                  Floating chatbox styles
    chat-api.js                  Chat API client
    log-fab.js                   Floating log button
    log-fab.css                  Log FAB styles
    poll-config.js               Polling configuration
    help.css                     Help panel styles
    user-manual.css              User manual styles
```

### 10.3 JS Module Pattern and State Management

The JAR Analyzer frontend uses a **shared namespace pattern** (`window.JA`) rather
than IIFEs. All modules extend a single global object using `Object.assign`:

```javascript
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {
    _traceState: null,          // private state (underscore prefix = internal)
    showCallTrace(idx) { ... }, // public method
    _renderCallTrace() { ... }  // internal method
});
```

**Key namespaces:**

| Namespace | File(s) | Responsibility |
|---|---|---|
| `JA.app` | `app.js` | Global app state: `currentJarId`, `currentAnalysis`, tab switching |
| `JA.api` | `api.js` | All REST calls: `getCallTree()`, `getSummary()`, `decompile()` |
| `JA.nav` | `nav.js` | Class navigation history, `_implMap` (interface → implementations), `_classIdx` |
| `JA.summary` | `summary.js` + `summary-*.js` | All Summary tab logic (11 sub-tabs, trace views, export, dispatch) |
| `JA.codeTree` | `codeTree.js` + `codeTreePanel.js` | Code Structure tab tree and right panel; manages `_resources` (extracted resource file list) and `_resourcesLoaded` (lazy-load guard) for the Resource Files section |
| `JA.callGraph` | `callGraph.js` | Endpoint Flows tab and call chain table |
| `JA.endpointList` | `endpointList.js` | Left pane of Endpoint Flows |
| `JA.toast` | `utils.js` | Toast notification helper |
| `JA.utils` | `utils.js` | `escapeHtml()`, formatting helpers |

**Module load order** (as declared in `index.html`):
`utils.js` → `api.js` → `nav.js` → `app.js` → sidebar/dashboard → codeTree* →
callGraph → endpointList → summary.js → summary-helpers → summary-tables →
summary-explorer → summary-trace → summary-codeview → (other summary-* files)

**State flow:**
1. User selects JAR → `JA.app.loadJar(jarId)` fetches analysis → sets `JA.app.currentAnalysis`
2. `JA.nav.init()` builds `_implMap` and `_classIdx` from the analysis class list
3. Each tab module reads from `JA.app.currentAnalysis` when rendering
4. `JA.summary._epReports` holds the per-endpoint report array used by all Summary sub-tabs
5. When the Code Structure tab is first opened or a JAR is (re)selected, `JA.codeTree` checks `_resourcesLoaded`; if false, it fetches `GET /api/jar/jars/{id}/resources`, stores the result in `_resources`, and renders the Resource Files section at the bottom of the left panel. Resource file content is loaded individually on demand via `GET /api/jar/jars/{id}/resources/{filename:.+}`

Files are loaded via `<script>` tags in the HTML pages. Order matters — shared
utilities load first, then domain-specific modules.

### 10.4 SSE Integration

The frontend uses `EventSource` for real-time updates:

```javascript
const source = new EventSource('/api/queue/events');
source.addEventListener('job-started', (e) => { ... });
source.addEventListener('job-complete', (e) => { ... });
source.addEventListener('job-failed', (e) => { ... });
source.addEventListener('job-cancelled', (e) => { ... });
source.addEventListener('state', (e) => { ... });
```

SSE emitters are configured with a 30-minute timeout (`1,800,000ms`). Dead emitters
(disconnected clients) are automatically cleaned up on send failure.

### 10.5 Resource Caching

In the default development configuration, static resource caching is disabled:

```properties
spring.web.resources.cache.period=0
spring.web.resources.cache.cachecontrol.no-cache=true
spring.web.resources.cache.cachecontrol.no-store=true
```

For production, these should be changed to enable browser caching.

---

## 11. Security Considerations

### 10.6 Frontend JS File Reference (JAR Analyzer)

| File | Purpose |
|---|---|
| `api.js` | All REST calls wrapped in `JA.api.*` methods |
| `app.js` | Entry point — JAR selection, tab switching, upload control; renders the styled Connections modal (replaces browser `alert()`) |
| `callGraph.js` | Endpoint Flows tab — operation flow table with dispatch badges |
| `chat-toggle.js` | Switches between New (chatbox) and Classic chat modes |
| `chatbox.js` | Floating AI chatbox FAB and popover |
| `codeTree.js` | Code Structure tab — package/project/visual tree views; manages `_resources` / `_resourcesLoaded` state for the Resource Files section |
| `codeTreePanel.js` | Right code panel container in Code Structure tab |
| `codeTreeViews.js` | View mode renderers (hierarchical, card grid) for code tree |
| `dash-panel.js` | Statistics panels on the dashboard |
| `dashboard.js` | Empty-state dashboard shown before any JAR is selected |
| `endpointList.js` | Left pane of Endpoint Flows — grouped endpoint listing |
| `logviewer.js` | Application log tail viewer (FAB, bottom-left) |
| `nav.js` | Class navigation history stack; builds `_implMap` and `_classIdx` |
| `sessions.js` | Claude session overlay (list, view history) |
| `sidebar.js` | Persistent left sidebar — JAR list, upload, Claude progress |
| `summary-aggregation.js` | Aggregation Flows sub-tab |
| `summary-claude.js` | Claude Insights columns in the endpoint summary |
| `summary-codeview.js` | Decompiled code modal with dispatch-aware navigation links; `_classIdx` lazy-init and `_implMap` stale-guard fixes ensure dispatch links render correctly when the modal is opened from the Summary tab |
| `summary-col-filter.js` | Advanced filter panel (range sliders, multi-select pills) |
| `summary-corrections.js` | Claude Corrections sub-tab |
| `summary-corr-logbrowser.js` | Correction log file browser |
| `summary-dynamic.js` | Dynamic Flows sub-tab |
| `summary-explorer.js` | Interactive drill-down call trace (breadcrumb navigator) |
| `summary-export*.js` | Export dialog, XLSX generation, styling |
| `summary-helpers.js` | Shared metric calculations, formatting, domain utilities |
| `summary-popups.js` | Modal/popup overlays |
| `summary-scheduled.js` | Scheduled Jobs sub-tab |
| `summary-tables.js` | Collection Analysis sub-tab |
| `summary-trace.js` | Flat trace overlay and node-by-node navigator |
| `summary-vert-popup.js` | Verticalisation popup detail |
| `summary-vertical.js` | Verticalisation sub-tab (bean crossing, data crossing) |
| `summary.js` | Summary tab orchestrator — state, render entry point, sub-tab routing |
| `utils.js` | `escapeHtml()`, `JA.toast`, formatting utilities |

---

### 11.1 No Authentication

The application does not include authentication or authorization. It is designed for
use within a trusted network or behind a corporate proxy that handles access control.
All REST endpoints are open.

### 11.2 Database Credentials

Oracle database credentials are stored in `plsql-config.yaml` within the
`config/` directory. Passwords are stored in plain text. Recommendations:

- Restrict filesystem permissions on the config directory
- Use a secrets management solution in production environments
- The JDBC auto-configuration is disabled; the application manages connections manually

### 11.3 File System Access

The application reads and writes to the filesystem extensively:

- `data/` directory contains all analysis outputs (potentially large JSON files)
- `config/` directory contains configuration including database credentials
- Claude CLI has access to tools (Read, Grep, Glob, Bash, Write, Edit) within its
  allowed toolset -- this means Claude can read and modify files on the filesystem
  within its working directory scope

### 11.4 File Upload

JAR file uploads are accepted up to 2GB (`spring.servlet.multipart.max-file-size=2GB`).
There is no virus scanning or content validation beyond basic JAR structure verification.
Uploaded JARs are stored persistently in `data/jar/{key}/stored.jar`.

### 11.5 SQL Injection Protection

- The legacy PL/SQL database controller exposes `POST /api/plsql/db/query` which
  accepts ad-hoc SQL queries. This is intended for authorized analyst use only.
- The SchemaResolver uses `PreparedStatement` with parameterized queries for all
  Oracle data dictionary queries.

### 11.6 Claude CLI Security

- Claude processes run as external OS processes with the same privileges as the
  application
- The `--allowedTools` flag restricts which tools Claude can use
- Environment variables for AWS Bedrock authentication are injected into the
  process environment
- Active processes are tracked per thread and can be forcibly killed via the API

### 11.7 CORS

No explicit CORS configuration is present. The application serves its own frontend
from the same origin, so cross-origin requests are not needed in normal operation.

### 11.8 Compression

JSON response compression is enabled for responses over 1KB:

```properties
server.compression.enabled=true
server.compression.mime-types=application/json
server.compression.min-response-size=1024
```

### 11.9 Recommendations for Production Deployment

1. Place behind a reverse proxy (nginx, Apache) with TLS termination
2. Add authentication (Spring Security, OAuth2, or proxy-level)
3. Encrypt database credentials in `plsql-config.yaml`
4. Restrict the ad-hoc query endpoint or disable it entirely
5. Set appropriate file upload size limits
6. Enable static resource caching
7. Review and restrict Claude CLI allowed tools
8. Configure firewall rules to restrict access to the configured port
9. Set `logging.level.root=WARN` for production (reduce log volume)
10. Monitor heap usage -- large JAR analyses can produce 100-400MB JSON files
