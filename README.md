# Unified PL/SQL + JAR Analyzer

Multi-module Maven project that provides a unified web platform for static analysis
of Java JAR/WAR files and Oracle PL/SQL database code. Combines bytecode inspection,
ANTLR4-based PL/SQL parsing, dependency crawling, and optional Claude AI enrichment
into a single deployable Spring Boot application.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Build](#build)
4. [Running the Application](#running-the-application)
5. [Web UIs](#web-uis)
6. [Modules](#modules)
7. [Dependencies](#dependencies)
8. [Configuration](#configuration)
9. [WAR File Support](#war-file-support)
10. [Claude AI Features](#claude-ai-features)
11. [Technology Stack](#technology-stack)
12. [Troubleshooting](#troubleshooting)

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Java (JDK) | **17+** | Required for compilation and runtime. Must be on `PATH`. |
| Maven | **3.8+** | Required for building all modules. |
| Claude CLI | latest | **Optional** — enables AI enrichment, chatbot, and verification features. |
| Oracle Database | any | **Optional** — required only for PL/SQL source fetching via JDBC. |
| MongoDB | any | **Optional** — required only for collection catalog verification. |

### Installing Java

Download from [https://adoptium.net/](https://adoptium.net/) (Eclipse Temurin recommended).

Verify installation:
```bash
java -version
# Expected: openjdk version "17.x.x" or higher
```

### Installing Maven

Download from [https://maven.apache.org/download.cgi](https://maven.apache.org/download.cgi)
or use `sdk install maven` if you have SDKMAN.

Verify installation:
```bash
mvn -version
# Expected: Apache Maven 3.8.x or higher
```

### Installing Claude CLI (Optional)

```bash
npm install -g @anthropic-ai/claude-code
claude --version   # verify installation
```

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/ajy77721/jar-and-plsql-analyzer.git
cd jar-and-plsql-analyzer

# 2. Build all modules (skip tests for a faster first build)
mvn clean package -DskipTests

# 3. Start the application
./start.sh         # Linux / macOS
start.bat          # Windows
```

Open your browser at **http://localhost:8083/**

---

## Build

### Full Build (All Modules, With Tests)

```bash
mvn clean install
```

### Fast Build (Skip Tests)

```bash
mvn clean package -DskipTests
```

### Build a Specific Module

```bash
# Build only the JAR analyzer core
mvn clean install -pl jar-analyzer-core -DskipTests

# Build only the PL/SQL parser
mvn clean install -pl plsql-parser -DskipTests

# Build the web app and all its dependencies
mvn clean package -pl unified-web -am -DskipTests
```

### Build Output

The deployable artifact is produced at:
```
unified-web/target/unified-web-1.0.0-SNAPSHOT.jar
```

This is a Spring Boot fat JAR (~150–200 MB) containing all modules, dependencies,
and static frontend assets.

---

## Running the Application

### Using the Start Scripts (Recommended)

The start scripts perform pre-flight checks (Java, JAR file, Claude CLI), create
required directories, and apply JVM tuning flags automatically.

```bash
# Linux / macOS
./start.sh

# Windows
start.bat
```

#### Environment Variables (Linux/macOS)

Override defaults without editing the script:

```bash
PORT=9090 CONFIG_DIR=/etc/analyzer/config DATA_DIR=/var/data/analyzer ./start.sh
```

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8083` | HTTP port |
| `CONFIG_DIR` | `config` | Path to config directory |
| `DATA_DIR` | `data` | Path to data/output directory |

### Direct Java Launch

```bash
java \
  -Xms512m -Xmx4g \
  -XX:MaxMetaspaceSize=256m \
  -XX:+UseG1GC \
  -XX:G1HeapRegionSize=4m \
  -XX:+UseStringDeduplication \
  -XX:HeapDumpOnOutOfMemoryError \
  -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --server.port=8083 \
  --app.config-dir=config \
  --app.data-dir=data
```

### Custom Port and Data Paths

Any `application.properties` value can be overridden on the command line:

```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --server.port=9090 \
  --app.config-dir=/opt/analyzer/config \
  --app.data-dir=/opt/analyzer/data \
  --claude.analysis.parallel-chunks=10
```

### Verifying Startup

After launch, the following endpoints should respond:

| URL | Expected Response |
|-----|-------------------|
| `http://localhost:8083/` | Home page (HTML) |
| `http://localhost:8083/jar/` | JAR Analyzer UI (HTML) |
| `http://localhost:8083/parser/` | PL/SQL Parser UI (HTML) |
| `http://localhost:8083/api/queue` | Queue state (JSON) |

---

## Web UIs

| URL | Description |
|-----|-------------|
| `http://localhost:8083/` | Home Dashboard — launcher for both tools, queue monitor, Claude session overview |
| `http://localhost:8083/jar/` | JAR / WAR Analyzer — upload, browse call trees, dispatch analysis, export |
| `http://localhost:8083/parser/` | PL/SQL Parser — run flow analysis, browse parsed procedures |

### JAR / WAR Analyzer Highlights

- **Connections modal** — the **Connections** button on any analyzed JAR/WAR opens a styled modal popup (not a browser alert) that lists every database connection detected in the archive's config files. Each database type is shown in its own color-coded section: MongoDB (green), Oracle (red), and PostgreSQL (blue). Passwords are masked. The modal shows which config file each entry was extracted from and can be closed via the × button, by clicking outside the panel, or by pressing Escape.

- **Resource Files** — after analysis completes, the Code Structure tab's left panel includes a **Resource Files** section. It lists up to 50 resource files extracted from the JAR or WAR, grouped by type with file-type icons (📄 `.properties`, 📋 `.yml`/`.yaml`, 📊 `.json`, 📝 `.xml`, 🗄️ `.sql`, 📃 others). For JAR files, resources are extracted from the root and resource paths; for WAR files, only resources from `WEB-INF/classes/` are included. Clicking any file loads its content in the right panel on demand.

- **Bundled Libraries** — the Code Structure tab's left panel includes a **Bundled Libraries** section listing every JAR bundled inside the analyzed archive (`BOOT-INF/lib/` for Spring Boot fat JARs, `WEB-INF/lib/` for WARs). Each bundled JAR is a collapsible node that exposes its `MANIFEST.MF` (if present) and up to 10 resource files. Up to 100 bundled JARs are listed. Available for both JAR and WAR analysis.

- **JAR Dependencies** — the Code Structure tab's left panel includes a **JAR Dependencies** section showing which bundled JARs the application's own code actually calls into. Dependencies are identified by cross-referencing class invocations against a class-to-JAR index built from the bundled library set. Each entry shows the bundled JAR name alongside the application classes that call into it. Clicking an application class name navigates directly to that class in the tree. The section header shows the count of libraries actually used (e.g., "🔗 JAR Dependencies (12 libs used)"). Available for both JAR and WAR analysis.

---

## Modules

```
plsql-jar-analyzer/                   (parent POM, packaging=pom)
  plsql-config/                       Shared YAML config model (Oracle connections, schemas)
  plsql-parser/                       ANTLR4 PL/SQL parser — flow analysis, dependency crawling
  plsql-analyzer-core/                PL/SQL analysis service layer (call graphs, table ops, triggers)
  jar-analyzer-core/                  Java bytecode analysis engine (ASM, CFR, dispatch resolution)
  unified-web/                        Spring Boot web app — REST APIs, queue, static UIs
  docs/                               User manuals and architecture documentation
  config/                             Runtime configuration (prompts, domain rules, Oracle connections)
  data/                               Runtime data directory (analysis output, logs)
  start.bat / start.sh                Launch scripts with pre-flight checks
```

### Module Dependency Graph

```
plsql-config
    ├── plsql-parser        (depends on plsql-config)
    ├── plsql-analyzer-core (depends on plsql-config + plsql-parser)
    ├── jar-analyzer-core   (independent — no PL/SQL dependencies)
    └── unified-web         (depends on all four modules above)
```

---

## Dependencies

### Core Framework

| Library | Version | Purpose |
|---------|---------|---------|
| Spring Boot | 3.2.5 | Web framework, DI container, embedded Tomcat |
| Spring Boot Starter Web | 3.2.5 | REST controllers, MVC, async/SSE |
| Spring Boot Starter JDBC | 3.2.5 | JDBC template (used for Oracle connectivity) |

### Bytecode & Decompilation

| Library | Version | Purpose |
|---------|---------|---------|
| OW2 ASM | 9.7 | Bytecode class visitor — extracts class/method/field metadata, invocations |
| CFR | 0.152 | Java decompiler — on-demand source decompilation for code view |

### PL/SQL Parsing

| Library | Version | Purpose |
|---------|---------|---------|
| ANTLR4 Runtime | 4.13.1 | Grammar-based PL/SQL lexer and parser |
| JSqlParser | 5.0 | SQL statement normalization and parsing |
| ojdbc11 | 23.3.0.23.09 | Oracle JDBC driver for source fetching and schema queries |

### Export & Data

| Library | Version | Purpose |
|---------|---------|---------|
| Apache POI | 5.2.5 | Multi-sheet XLSX Excel export with styling |
| MongoDB Driver Sync | 4.11.2 | Optional — collection catalog verification |
| Jackson Databind | (Boot managed) | JSON serialization/deserialization |
| Jackson YAML | (Boot managed) | YAML config loading (`plsql-config.yaml`) |
| Jackson JSR-310 | (Boot managed) | Java 8 date/time type support |

### AI Enrichment

| Component | Notes |
|-----------|-------|
| Claude CLI | External process — spawned via `ProcessBuilder`. Install separately via `npm install -g @anthropic-ai/claude-code` |

### Testing

| Library | Version | Purpose |
|---------|---------|---------|
| JUnit 5 | (Boot managed) | Unit and integration tests |
| Spring Boot Test | (Boot managed) | Spring context tests |

---

## Configuration

### Directory Layout

The application uses two configurable directories:

| Property | Default | Contents |
|----------|---------|----------|
| `app.config-dir` | `config/` | `plsql-config.yaml` — Oracle DB connections<br>`domain-config.json` — domain clustering rules<br>`prompts/*.txt` — Claude AI prompt templates (auto-seeded on first run) |
| `app.data-dir` | `data/` | `jar/` — JAR/WAR analysis output<br>`plsql/` — legacy PL/SQL analysis<br>`plsql-parse/` — PL/SQL parser results<br>`cache-plsql/` — Oracle schema resolver cache<br>`claude-chatbot/` — classic chat sessions<br>`unified-analyzer.log` — rolling application log |

Both directories are created automatically on first startup if they do not exist.

### Oracle Database Connection (`config/plsql-config.yaml`)

Required only for PL/SQL source fetching. Edit this file to add your Oracle connections:

```yaml
projects:
  - name: "MY_PROJECT"
    environments:
      - name: "PROD"
        connections:
          - name: "APP_SCHEMA"
            hostname: "db-host.example.com"
            port: 1521
            service_name: "ORCL"
            username: "APP_USER"
            password: "secret"
            schemas:
              - APP_SCHEMA
              - COMMON_SCHEMA
```

Connections can also be managed via the REST API or the PL/SQL Parser UI
without editing the file directly.

### Key `application.properties` Overrides

| Property | Default | Common Override |
|----------|---------|----------------|
| `server.port` | `8083` | `--server.port=9090` |
| `calltree.max-depth` | `20` | Increase for deeply nested frameworks |
| `calltree.max-nodes-per-tree` | `2000` | Increase for very large JARs |
| `claude.analysis.parallel-chunks` | `5` | Increase for faster enrichment |
| `claude.chunking.max-chunk-chars` | `50000` | Decrease if hitting Claude context limits |
| `spring.servlet.multipart.max-file-size` | `2GB` | Increase for very large WARs |

Full properties reference: [`docs/manual-properties.md`](docs/manual-properties.md)

---

## WAR File Support

The analyzer supports `.war` files in addition to `.jar` files. Upload a WAR file
exactly the same way as a JAR — the analyzer auto-detects the extension.

WAR files are processed by `WarParserService` which:
1. Extracts application classes from `WEB-INF/classes/`
2. Detects the application's base package
3. Filters `WEB-INF/lib/` — includes only app-origin bundled JARs, skips pure libraries
4. Feeds into the standard call graph and endpoint detection pipeline

All analysis features work identically for WAR and JAR files.
See [`docs/manual-jar-analyzer.md`](docs/manual-jar-analyzer.md#4-war-file-support) for full details.

---

## Claude AI Features

When Claude CLI is available on `PATH`, the platform enables:

- **JAR/WAR enrichment** — natural-language business descriptions, correction suggestions, risk flags per endpoint
- **PL/SQL verification** — AI-assisted review of parsed procedure analysis results
- **Chatbot** — conversational interface scoped to the currently selected JAR/WAR analysis
- **Chunked prompts** — large call trees are automatically split into manageable chunks

Static analysis (bytecode parsing, call trees, dispatch resolution, resource file extraction, export) works
fully without Claude CLI installed.

---

## Technology Stack

| Concern | Library | Version |
|---------|---------|---------|
| Runtime | Spring Boot | 3.2.5 |
| PL/SQL parsing | ANTLR4 | 4.13.1 |
| SQL normalization | JSqlParser | 5.0 |
| Bytecode analysis | OW2 ASM | 9.7 |
| Decompilation | CFR | 0.152 |
| Excel export | Apache POI | 5.2.5 |
| Oracle JDBC | ojdbc11 | 23.3.0.23.09 |
| MongoDB (optional) | MongoDB Driver Sync | 4.11.2 |
| Java | JDK | 17 |
| Frontend | Vanilla JS (no build step) | — |

---

## Troubleshooting

### Build Fails — `ojdbc11` Not Found in Maven Central

Oracle JDBC driver is not published to Maven Central. Add the Oracle Maven repository
to your `~/.m2/settings.xml`:

```xml
<repositories>
  <repository>
    <id>maven.oracle.com</id>
    <url>https://maven.oracle.com</url>
  </repository>
</repositories>
```

Or install it locally:
```bash
mvn install:install-file \
  -Dfile=ojdbc11.jar \
  -DgroupId=com.oracle.database.jdbc \
  -DartifactId=ojdbc11 \
  -Dversion=23.3.0.23.09 \
  -Dpackaging=jar
```

### Application Starts But Claude Features Don't Work

1. Verify Claude CLI is installed: `claude --version`
2. Check Claude is on `PATH`: `which claude` (Linux/macOS) or `where claude` (Windows)
3. If using AWS Bedrock, ensure `AWS_PROFILE` and `AWS_REGION` environment variables are set
4. Check the in-app log viewer (`Logs` button) for Claude initialization errors

### OutOfMemoryError on Large JARs

Increase the heap size in the start script or command line:
```bash
java -Xmx8g -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar ...
```

Large enterprise JARs (400+ classes) can produce analysis JSON files of 200–400 MB.
The heap needs room for Jackson to deserialize and process these.

### Port Already in Use

```bash
# Linux/macOS — find what's using port 8083
lsof -i :8083

# Use a different port
./start.sh --server.port=9090
```

### Analysis JSON Not Loading in Browser

Static resource caching is disabled by default (development mode). If the browser
shows a cached empty state after re-analyzing:
1. Hard refresh: `Cmd+Shift+R` (macOS) or `Ctrl+Shift+R` (Windows/Linux)
2. Open DevTools → Application → Clear storage
