# Unified Web

Spring Boot web application that provides a single deployment combining the JAR Analyzer
and PL/SQL Parser tools. Serves REST APIs for both analyzers and hosts three static
web UIs behind a unified queue system with SSE progress streaming.

## Runtime

| Property | Value |
|----------|-------|
| Port | 8083 |
| Framework | Spring Boot 3.2.5 |
| Java | 17 |
| JVM flags | -Xms512m -Xmx4g -XX:+UseG1GC -XX:+UseStringDeduplication |

## Web UIs

| Path | Description |
|------|-------------|
| `/` | Home page -- launcher for both tools |
| `/jar/` | JAR Analyzer UI -- upload JARs, browse results, view call trees |
| `/parser/` | PL/SQL Parser UI -- run flow analysis, browse parsed procedures |

Shared assets (chat widget, log FAB, polling config) live under `/shared/`.

## REST API Structure

### JAR Analyzer (`/api/jar/*`)
Handled by controllers in `com.jaranalyzer.controller`:
- **AnalyzerController** -- Upload, analyze, list analyses, export Excel
- **DecompileController** -- On-demand class decompilation
- **ChatboxController** -- Claude AI chatbot for JAR analysis context
- **ProgressController** -- SSE endpoint for JAR analysis progress
- **LogController** -- Tail analysis logs

### PL/SQL Parser (`/api/parser/*`)
Handled by controllers in `com.plsqlanalyzer.web.parser.controller`:
- **ParserAnalysisController** -- Run analysis, list results, view flow JSON
- **ParserConfigController** -- Complexity and join-complexity thresholds
- **ClaudeController** -- Claude AI enrichment for PL/SQL analysis

### PL/SQL Database (`/api/plsql/db/*`)
Handled by controllers in `com.plsqlanalyzer.web.controller`:
- **DatabaseController** -- Test Oracle DB connectivity, list schemas
- **SourceController** -- Download PL/SQL source from Oracle
- **ConfigController** -- Manage DB connection and analysis configuration

### Queue (`/api/queue`)
- **QueueController** -- Unified analysis queue (submit, cancel, status, SSE progress)

## Package Structure

```
com.analyzer
  chat/           ChatController, ChatService, ChatSession, ChatConfig
  config/         WebConfig, GlobalSessionsController, PollingConfigController
  queue/          AnalysisQueueService, QueueController, QueueJob,
                  JarAnalysisExecutor, ParserAnalysisExecutor, ClaudeJobExecutor

com.jaranalyzer.controller
                  AnalyzerController, DecompileController, ChatboxController,
                  ProgressController, LogController

com.plsqlanalyzer.web
  config/         AppConfig, ConfigDirService, ApiLoggingFilter
  controller/     AnalyzerController, DatabaseController, SourceController, etc.
  parser/
    config/       ComplexityConfig, JoinComplexityConfig
    controller/   ParserAnalysisController, ParserConfigController, ClaudeController
    model/        AnalysisInfo, ParserAnalysisInfo
    service/      AnalysisService, ClaudeProcessRunner, ClaudeSessionManager,
                  ClaudePersistenceService, ClaudePromptBuilder, ChunkingUtils
```

## Key Components

- **AnalysisQueueService** -- Thread-safe queue that serializes JAR and PL/SQL analysis
  jobs, preventing concurrent heavy workloads. Supports job cancellation and priority.
- **SSE Progress Streaming** -- Both analyzers push real-time progress events to the
  browser via Server-Sent Events.
- **Claude AI Integration** -- Chat and enrichment endpoints for both tools, with
  session management, chunked prompts, and verification/persistence.

## Build and Run

```bash
# Build the full project (all modules)
mvn clean package -pl unified-web -am

# Run
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar
```

The application is also launchable via `start.bat` (Windows) or `start.sh` (Linux)
from the parent directory.
