# Unified PL/SQL + JAR Analyzer

Multi-module Maven project providing a unified web platform for static analysis of Java JAR/WAR files and stored database code across **five SQL dialects — Oracle PL/SQL, MySQL, Microsoft SQL Server (T-SQL), IBM Db2 SQL PL, and PostgreSQL PL/pgSQL**. Combines bytecode inspection, ANTLR4-based multi-dialect parsing, dependency crawling, and optional Claude AI enrichment into a single deployable Spring Boot application.

> **Supported databases:** Oracle, MySQL, Microsoft SQL Server, IBM Db2, PostgreSQL. Each dialect has its own ANTLR4 grammar + analysis visitor (`plsql-parser`, `mysql-parser`, `sqlserver-parser`, `db2-parser`, `postgres-parser`) that feed a common analysis model, so procedures/functions, table operations, cursors, dynamic SQL, joins, and call graphs are extracted uniformly regardless of source database.

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
| Oracle Database | any | **Optional** — required only for Oracle PL/SQL source fetching via JDBC. |
| MySQL | 5.7 / 8.x | **Optional** — required only for fetching MySQL routine source via JDBC. |
| Microsoft SQL Server | 2016+ | **Optional** — required only for fetching T-SQL procedure/function source via JDBC. |
| IBM Db2 | 11.x+ | **Optional** — required only for fetching Db2 SQL PL routine source via JDBC. |
| PostgreSQL | 12+ | **Optional** — required only for fetching PL/pgSQL routine source via JDBC. |
| MongoDB | any | **Optional** — required only for collection catalog verification in JAR analyzer. |

### Installing Java

Download from [https://adoptium.net/](https://adoptium.net/) (Eclipse Temurin recommended).

```bash
java -version
# Expected: openjdk version "17.x.x" or higher
```

### Installing Maven

Download from [https://maven.apache.org/download.cgi](https://maven.apache.org/download.cgi) or use `sdk install maven` if you have SDKMAN.

```bash
mvn -version
# Expected: Apache Maven 3.8.x or higher
```

### Installing Claude CLI (Optional)

```bash
npm install -g @anthropic-ai/claude-code
claude --version
```

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/ajy77721/jar-and-plsql-analyzer.git
cd jar-and-plsql-analyzer

# 2. Build all modules (skip tests for faster first build)
mvn clean package -DskipTests

# 3. Start the application
./start.sh         # Linux / macOS
start.bat          # Windows
```

Open your browser at **http://localhost:8083/**

---

## Build

### Full Build

```bash
mvn clean install
```

### Fast Build (Skip Tests)

```bash
mvn clean package -DskipTests
```

### Build a Specific Module

```bash
# Build only the web app and its dependencies
mvn clean package -pl unified-web -am -DskipTests
```

### Build Output

```
unified-web/target/unified-web-1.0.0-SNAPSHOT.jar
```

Spring Boot fat JAR (~150–200 MB) containing all modules, dependencies, and static frontend assets.

---

## Running the Application

### Using Start Scripts (Recommended)

```bash
./start.sh          # Linux / macOS
start.bat           # Windows
```

#### Environment Variables (Linux/macOS)

```bash
PORT=9090 CONFIG_DIR=/etc/analyzer/config DATA_DIR=/var/data/analyzer ./start.sh
```

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8083` | HTTP port |
| `DATA_DIR` | `data` | Single runtime tree — config, logs, cache, and output all live under here |

### Direct Java Launch

```bash
java \
  -Xms512m -Xmx4g \
  -XX:MaxMetaspaceSize=256m \
  -XX:+UseG1GC \
  -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --server.port=8083 \
  --app.data-dir=data
```

> Only `--app.data-dir` is needed — the config dir (`{data}/config`) and log dir (`{data}/logs`) are derived from it automatically.

### Running with Docker

A published image is available on GitHub Container Registry and is updated automatically by CI ([`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml), modeled on the hello-flask Drone pipeline):

- **push to `master`** → runs tests, then publishes `git-<sha>` + `latest`
- **tag push (`v1.2.3`)** → runs tests, then publishes `git-<sha>` + `<version>`

```bash
# Latest master build
docker run -d -p 8083:8083 \
  -v analyzer-data:/data \
  ghcr.io/modernization-factory/amp-analyzer:latest

# A released version
docker run -d -p 8083:8083 \
  -v analyzer-data:/data \
  ghcr.io/modernization-factory/amp-analyzer:v1.2.3
```

All analysis state (config, logs, results, caches) persists in the `analyzer-data` volume at `/data`. Override the port or JVM options with `PORT` / `JAVA_OPTS`:

```bash
docker run -d -p 9090:9090 -e PORT=9090 -e JAVA_OPTS="-Xmx8g -XX:+UseG1GC" \
  -v analyzer-data:/data ghcr.io/modernization-factory/amp-analyzer:latest
```

To build the image locally instead:

```bash
docker build -t unified-analyzer .
docker run -d -p 8083:8083 -v analyzer-data:/data unified-analyzer
```

### Storage Modes — Build + Run (Local / MongoDB / S3 / Hybrid)

Analysis artifacts (JAR/WAR/PL-SQL results) can be stored in one of four interchangeable backends,
selected by a single property — **`app.storage.mode`** = `local` | `s3` | `mongodb` | `hybrid`.
Same jar, no code change; every layer (per-user SSO routing, write-behind, caches, entity ingest)
coheres around that one choice. `config/`, `logs/`, `prompts/`, `claude-chatbot/` always stay on
local disk in every mode; only `data/jar` and `data/plsql` follow the selected mode (they are never
created locally in `mongodb`/`s3`/`hybrid` mode).

> **Full guide: [`docs/modes.md`](docs/modes.md)** — the mode matrix, every property with its
> default, the auth behaviour per mode, the `am_*` entity-ingest vs `repository=mongo` distinction,
> the startup requirements block, and the "I want X → set Y" decision table.

**Authentication is mode-aware.** `app.auth.required` is auto-derived when unset: `s3`/`hybrid` →
`true` (SSO page-lock, per-user buckets); `local`/`mongodb` → `false` (**zero verification** — every
`/api/**` is open with no cookie). An explicit `app.auth.required=true|false` always wins. The SSO
endpoints stay reachable in every mode, so an optional login is still possible.

| Mode | Store | Auth (auto) | Needs |
|------|-------|-------------|-------|
| `local` | files under `{data-dir}` | off | — |
| `mongodb` | manifest doc + GridFS | off | `app.storage.mongodb.uri`, `.database` |
| `s3` | `s3://{bucket}/{prefix}` | **on** | `app.storage.s3.bucket`, `.region` |
| `hybrid` | small → Mongo, large → S3 | **on** | bucket + Mongo uri/database |

**Build once, for any mode:**
```bash
python3 scripts/stamp-assets.py
mvn -pl unified-web -am package -DskipTests -q
```

**Local mode** (default, no external dependency, no login):
```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --server.port=8083 --app.config-dir=config --app.data-dir=data
```

**MongoDB mode** (requires a reachable MongoDB):
```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --server.port=8083 --app.config-dir=config --app.data-dir=data \
  --app.storage.mode=mongodb \
  --app.storage.mongodb.uri=mongodb://localhost:27017 \
  --app.storage.mongodb.database=analyzer \
  --app.storage.entity-ingest.enabled=true \
  --app.storage.entity-read.enabled=true
```

**S3 mode** (requires AWS credentials via the default provider chain; SSO login is required).
Accounts with no AWS Identity Center can instead connect classic IAM access keys via
`POST /api/auth/aws/iam-connect`; if a `sessionToken` is supplied the credentials are temporary STS
credentials and expire after **~1 hour** — the response's `expiresAt` / `expiresInSeconds` fields
carry that window (both `null` for long-lived IAM user keys). Re-run `iam-connect` with fresh keys
when it lapses:
```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --app.storage.mode=s3 \
  --app.storage.s3.bucket=<your-bucket> \
  --app.storage.s3.prefix=data \
  --app.storage.s3.region=<region> \
  --app.storage.sso.start-url='https://d-xxxxxxxx.awsapps.com/start/#'
```

**Hybrid mode** (small artifacts in Mongo, large ones in S3):
```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --app.storage.mode=hybrid \
  --app.storage.s3.bucket=<your-bucket> --app.storage.s3.region=<region> \
  --app.storage.mongodb.uri=mongodb://localhost:27017 --app.storage.mongodb.database=analyzer \
  --app.storage.hybrid.big-threshold=1048576 \
  --app.storage.hybrid.s3-always-paths=sources
```

On boot the app prints a `[storage] mode check` block listing each requirement with `✓`/`MISSING`.
See the in-app **Properties** manual (User Manual → Properties) for the full property reference.

### Verifying Startup

| URL | Expected Response |
|-----|-------------------|
| `http://localhost:8083/` | Home Dashboard (HTML) |
| `http://localhost:8083/jar/` | JAR Analyzer UI (HTML) |
| `http://localhost:8083/war/` | WAR Analyzer UI (HTML) — own page, shares most JS with `/jar/` |
| `http://localhost:8083/parser/` | PL/SQL Parser UI (HTML) |
| `http://localhost:8083/api/queue` | Queue state (JSON) |

---

## Web UIs

| URL | Description |
|-----|-------------|
| `http://localhost:8083/` | Home Dashboard — launcher, **Analysis History** (loaded on open), Performance Summary, queue monitor, Claude session overview |
| `http://localhost:8083/jar/` | JAR Analyzer — upload, browse call trees, Summary reports, export |
| `http://localhost:8083/war/` | WAR Analyzer — its own page/tool card, separate REST endpoints (`/api/war/wars`), but reuses almost all of the JAR page's JS (Code Structure, Endpoint Flows, Summary tabs) |
| `http://localhost:8083/parser/` | Multi-dialect analysis — run analysis and browse a procedure's views (see below) |

### Analysis views (`/parser/`)

Once an analysis is loaded, each procedure exposes these tabs:

- **Explore** — Hierarchy (call tree), Trace (flat, filterable, with a Unique toggle), References, and **Transactions**.
- **Transactions** — transaction boundaries across the call trace, tagged **T1/T2/T3…**. Each row shows the start (first DML write), savepoints, and its two mutually-exclusive exits — the success **COMMIT** and the error-handler **ROLLBACK** (they are one transaction, not two). Table and Graph views, plus a per-transaction **Details** popup: the full call tree of that transaction laid out as a 3-per-row snake (5 in full screen) with each proc's operations and boundary points, read-only calls pruned, nested transactions tagged, and a plain-language ACID/dialect internals summary.
- **Table Ops** — tables touched and the INSERT/UPDATE/DELETE/MERGE/SELECT operations per procedure.
- **Complexity** — per-procedure risk scoring with a **configurable algorithm** (Linear / Logarithmic / Exponential / Balanced), adjustable weights and thresholds; the active algorithm is shown next to the ⚙ Scoring button, saved per-analysis or as a global default.
- **Summary / Graph / Source** — dashboard, call-graph visualization, and source viewer.
- **Claude** enrichment layer (optional) — AI verification and corrections over the parsed results.

---

## Modules

```
plsql-jar-analyzer/                   (parent POM, packaging=pom)
  plsql-config/                       Shared YAML config model (DB connections, schemas)
  plsql-parser/                       ANTLR4 Oracle PL/SQL parser — flow analysis, dependency crawling
  mysql-parser/                       ANTLR4 MySQL parser — analysis visitor for MySQL routines
  sqlserver-parser/                   ANTLR4 Microsoft SQL Server (T-SQL) parser — analysis visitor
  db2-parser/                         ANTLR4 IBM Db2 (SQL PL) parser — analysis visitor
  postgres-parser/                    ANTLR4 PostgreSQL (PL/pgSQL) parser — analysis visitor
  plsql-analyzer-core/                Multi-dialect analysis service layer (call graphs, table ops, triggers)
  jar-analyzer-core/                  Java bytecode analysis engine (ASM, CFR, dispatch resolution)
  unified-web/                        Spring Boot web app — REST APIs, queue, static UIs
  docs/                               User manuals and architecture documentation
  data/                               Single runtime tree — ALL mutable state (see Configuration):
                                        config/  (connections, prompts, domain rules — seeded from the jar)
                                        logs/    (rolling application log)
                                        jar/, plsql-parse/, cache-plsql/, claude-chatbot/
  start.bat / start.sh                Launch scripts with pre-flight checks
```

> **Note:** The pristine config/prompt masters ship **inside the jar** (`resources/`) and are seeded into `data/config/` on first run. There is no committed top-level `config/` directory — everything mutable lives under the single `data/` tree, so relocating the product is just pointing `--app.data-dir` at one folder.

The four sibling dialect parsers (`mysql-parser`, `sqlserver-parser`, `db2-parser`, `postgres-parser`) mirror the Oracle `plsql-parser` design: an ANTLR4 grammar plus an analysis visitor that emits the same shared model (procedures, table operations, cursors, dynamic SQL, joins, calls). `plsql-parser` also hosts the per-dialect adapters (`multidb/{MySql,Db2,SqlServer,Postgres}ProcedureParser`) that map each dialect's output into the unified model consumed by the web layer.

### Module Dependency Graph

```
plsql-config
    ├── plsql-parser         (depends on plsql-config)
    ├── mysql-parser         (depends on plsql-config)
    ├── sqlserver-parser     (depends on plsql-config)
    ├── db2-parser           (depends on plsql-config)
    ├── postgres-parser      (depends on plsql-config)
    ├── plsql-analyzer-core  (depends on plsql-config + plsql-parser)
    ├── jar-analyzer-core    (independent; shared by both the JAR and WAR controllers in unified-web)
    └── unified-web          (depends on all modules above)
```

---

## Dependencies

### Core Framework

| Library | Version | Purpose |
|---------|---------|---------|
| Spring Boot | 3.2.5 | Web framework, DI container, embedded Tomcat |
| Spring Boot Starter Web | 3.2.5 | REST controllers, MVC, async/SSE |
| Spring Boot Starter JDBC | 3.2.5 | JDBC template for Oracle connectivity |

### Bytecode & Decompilation

| Library | Version | Purpose |
|---------|---------|---------|
| OW2 ASM | 9.7 | Bytecode class visitor — extracts class/method/field metadata and invocations |
| CFR | 0.152 | Java decompiler — on-demand source decompilation for code view |

### Multi-Dialect SQL Parsing

| Library | Version | Purpose |
|---------|---------|---------|
| ANTLR4 Runtime | 4.13.1 | Grammar-based lexer/parser for all five dialects (Oracle PL/SQL, MySQL, T-SQL, Db2 SQL PL, PL/pgSQL) |
| JSqlParser | 5.0 | SQL statement normalization and parsing |
| ojdbc11 | 23.3.0.23.09 | Oracle JDBC driver for source fetching and schema queries |
| MySQL Connector/J | (managed) | MySQL JDBC driver for routine source fetching |
| mssql-jdbc | (managed) | Microsoft SQL Server JDBC driver for procedure/function source fetching |
| Db2 JCC (`jcc`) | (managed) | IBM Db2 JDBC driver for routine source fetching |
| PostgreSQL JDBC (`postgresql`) | 42.7.3 | PostgreSQL JDBC driver for routine source fetching — on Maven Central |

### Export & Data

| Library | Version | Purpose |
|---------|---------|---------|
| Apache POI | 5.2.5 | Multi-sheet XLSX Excel export with styling |
| MongoDB Driver Sync | 4.11.2 | Optional — live collection catalog verification |
| Jackson Databind | (Boot managed) | JSON serialization/deserialization |
| Jackson YAML | (Boot managed) | YAML config loading |
| Jackson JSR-310 | (Boot managed) | Java 8 date/time type support |

### AI Enrichment

| Component | Notes |
|-----------|-------|
| Claude CLI | External process — spawned via `ProcessBuilder`. Install separately via `npm install -g @anthropic-ai/claude-code` |

---

## Configuration

### Directory Layout

All mutable state lives under a **single `app.data-dir`** tree. The config, log, and cache locations are **derived automatically** from it by `DirPropertyResolver` (you normally set only `--app.data-dir`):

| Path (under `app.data-dir`, default `data/`) | Contents |
|----------|----------|
| `config/` | `plsql-config.yaml` — DB connections (Oracle / MySQL / SQL Server / Db2)<br>`domain-config.json` — domain clustering rules<br>`prompts/*.txt` — Claude AI prompt templates<br>*(all seeded from the jar's `resources/` on first run; edits are saved back here)* |
| `logs/` | `unified-analyzer.log` — rolling application log (what the in-app **Logs** viewer reads) |
| `jar/` | JAR/WAR analysis output |
| `plsql-parse/` | PL/SQL & multi-dialect parser results |
| `cache-plsql/` | schema-resolver cache |
| `claude-chatbot/` | Claude chat sessions |

> `app.config-dir` resolves to `{data}/config` and the log dir (`LOG_PATH`) to `{data}/logs` automatically — you don't set them separately.

### Database Connections (`{data}/config/plsql-config.yaml`)

Each connection declares a `dbType` of `ORACLE`, `MYSQL`, `SQLSERVER`, `DB2`, or `POSTGRES`; the analyzer uses it to pick the matching JDBC driver and dialect parser. Example (Oracle shown):


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

### Key `application.properties` Overrides

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8083` | HTTP port |
| `calltree.max-depth` | `20` | Max call tree depth for JAR analysis |
| `calltree.max-nodes-per-tree` | `2000` | Max nodes per endpoint call tree |
| `claude.analysis.parallel-chunks` | `5` | Parallel Claude chunks during enrichment |
| `claude.chunking.max-chunk-chars` | `50000` | Max chars per Claude chunk |
| `spring.servlet.multipart.max-file-size` | `2GB` | Max upload size |

Full properties reference: [`docs/manual-properties.md`](docs/manual-properties.md)

Per-database setup, connection, and dialect notes:
[Oracle](docs/db-oracle.md) · [MySQL](docs/db-mysql.md) ·
[SQL Server](docs/db-sqlserver.md) · [Db2](docs/db-db2.md) · [PostgreSQL](docs/db-postgres.md) —
overview in [`docs/databases.md`](docs/databases.md).

---

## WAR File Support

WAR files have their own dedicated page (`http://localhost:8083/war/`), a separate tool card on the Home Dashboard, and their own REST API (`/api/war/wars`, a sibling to `/api/jar/jars`) — not a mode of the JAR page. `WarParserService` extracts application classes from `WEB-INF/classes/` and bundled libraries from `WEB-INF/lib/`; the shared `jar-analyzer-core` module and almost all of the JAR page's frontend JS are reused as-is, so analysis features work identically for WAR and JAR files. WAR data is stored one key-root level below JAR data (`data/jar/war/{name}/` vs `data/jar/{name}/`) so the two analysis lists stay separate.

See [`docs/manual-jar-analyzer.md`](docs/manual-jar-analyzer.md#4-war-file-support) for full details.

---

## Claude AI Features

When Claude CLI is available on `PATH`:

- **JAR/WAR enrichment** — natural-language business descriptions, correction suggestions, risk flags per endpoint
- **PL/SQL verification** — AI-assisted review of parsed procedure analysis results
- **Chatbot** — conversational interface scoped to the currently selected analysis
- **Chunked prompts** — large call trees split into manageable chunks automatically

Static analysis works fully without Claude CLI installed.

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
| PostgreSQL JDBC | postgresql | 42.7.3 |
| MongoDB (optional) | MongoDB Driver Sync | 4.11.2 |
| Java | JDK | 17 |
| Frontend | Vanilla JS (no build step) | — |

---

## Troubleshooting

### Build Fails — `ojdbc11` Not Found in Maven Central

Oracle JDBC is not published to Maven Central. Install it locally:

```bash
mvn install:install-file \
  -Dfile=ojdbc11.jar \
  -DgroupId=com.oracle.database.jdbc \
  -DartifactId=ojdbc11 \
  -Dversion=23.3.0.23.09 \
  -Dpackaging=jar
```

### Claude Features Don't Work

1. Verify: `claude --version`
2. Check PATH: `which claude` (Linux/macOS) or `where claude` (Windows)
3. If using AWS Bedrock, set `AWS_PROFILE` and `AWS_REGION` environment variables
4. Check the in-app log viewer (`Logs` button) for Claude initialization errors

### OutOfMemoryError on Large JARs

```bash
java -Xmx8g -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar
```

Large enterprise JARs (400+ classes) can produce analysis JSON files of 200–400 MB.

### Port Already in Use

```bash
# Find what's using port 8083
lsof -i :8083          # Linux/macOS

# Use a different port
./start.sh --server.port=9090
```

### Corrupted JAR After Interrupted Build

If Maven was interrupted during a build, the output JAR may be an invalid ZIP. Fix with:

```bash
mvn clean -pl plsql-parser --no-transfer-progress
mvn clean package -pl unified-web -am -DskipTests --no-transfer-progress
```
