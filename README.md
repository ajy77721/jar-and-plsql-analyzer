# Unified PL/SQL + JAR Analyzer

Multi-module Maven project that provides a unified web platform for static analysis
of Java JAR files and Oracle PL/SQL database code. Combines bytecode inspection,
ANTLR4-based PL/SQL parsing, dependency crawling, and optional Claude AI enrichment
into a single deployable Spring Boot application.

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Java (JDK) | 17+ | Required |
| Maven | 3.8+ | Required for building |
| Claude CLI | latest | Optional -- enables AI enrichment, chatbot, and verification |

To install Claude CLI (optional):
```bash
npm install -g @anthropic-ai/claude-code
```

## Quick Start

```bash
# Build all modules (skip tests for faster first build)
mvn clean package -DskipTests

# Start the application
start.bat          # Windows
./start.sh         # Linux / macOS
```

The start scripts validate Java, the built JAR, and Claude CLI availability before
launching with tuned JVM flags (`-Xms512m -Xmx4g -XX:+UseG1GC`).

## Web UIs

| URL | Description |
|-----|-------------|
| http://localhost:8083/ | Home page -- launcher for both tools |
| http://localhost:8083/jar/ | JAR Analyzer -- upload JARs, browse call trees, export Excel |
| http://localhost:8083/parser/ | PL/SQL Parser -- run flow analysis, browse parsed procedures |

## Modules

```
plsql-jar-analyzer/
  plsql-config/          Shared YAML configuration management (Oracle connections, schemas)
  plsql-parser/          ANTLR4 PL/SQL parser with flow analysis and dependency crawling
  plsql-analyzer-core/   PL/SQL analysis service layer (call graphs, table ops, triggers)
  jar-analyzer-core/     Java bytecode analysis engine (ASM, CFR decompiler, Excel export)
  unified-web/           Spring Boot web app combining both tools with REST APIs and UIs
```

## Configuration

The application uses two directories configured via command-line properties:

| Property | Default | Contents |
|----------|---------|----------|
| `app.config-dir` | `config/` | `plsql-config.yaml`, `domain-config.json`, `prompts/` |
| `app.data-dir` | `data/` | Analysis output: `jar/`, `plsql/`, `plsql-parse/`, `cache-plsql/`, `claude-chatbot/` |

### plsql-config.yaml

Defines Oracle database connections in a project/environment hierarchy:

```yaml
projects:
  - name: "MY_PROJECT"
    environments:
      - name: "PROD"
        connections:
          - name: "SCHEMA_PROD"
            hostname: "db-host"
            port: 1521
            service_name: "ORCL"
            username: "SCHEMA_USER"
            password: "..."
```

Additional Spring Boot properties can be set in `application.properties` or via
command-line `--property=value` arguments passed to the start script.

## Claude AI Features

When Claude CLI is available on PATH, the platform enables:

- AI enrichment for JAR analysis (natural-language descriptions, correction suggestions)
- AI verification for PL/SQL analysis results
- Interactive chatbot conversations scoped to analysis context
- Chunked prompt generation for large call trees

Static analysis works fully without Claude CLI installed.

## Build

```bash
# Full build with tests
mvn clean install

# Build a specific module
mvn clean install -pl plsql-parser

# Build unified-web and all its dependencies
mvn clean package -pl unified-web -am
```

## Technology Stack

| Concern | Library | Version |
|---------|---------|---------|
| Runtime | Spring Boot | 3.2.5 |
| PL/SQL parsing | ANTLR4 | 4.13.1 |
| SQL normalization | JSqlParser | 5.0 |
| Bytecode analysis | OW2 ASM | 9.7 |
| Decompilation | CFR | 0.152 |
| Excel export | Apache POI | 5.2.5 |
| Oracle connectivity | ojdbc11 | 23.3.0.23.09 |
| Java | JDK | 17 |
