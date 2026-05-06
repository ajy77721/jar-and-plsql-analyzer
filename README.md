# Unified PL/SQL + JAR Analyzer

> **Version:** 1.0.0-SNAPSHOT &nbsp;|&nbsp; **Default Port:** 8083 &nbsp;|&nbsp; **Java 17** &nbsp;|&nbsp; **Spring Boot 3.2.5**

A production-grade static analysis platform that combines two complementary engines into a single deployable Spring Boot fat JAR. No external database required — all data persists to the local filesystem.

- **JAR / WAR Analyzer** — Inspects compiled Java bytecode using ASM 9.7 + CFR 0.152. Extracts REST endpoints, async entry points (RabbitMQ, Kafka, WebSocket, scheduled tasks, event listeners), class hierarchies, call graphs, MongoDB and JPA/JDBC data-access patterns, aggregation pipelines, cross-domain coupling, and complexity metrics. Optionally enriches everything with Claude AI natural-language analysis.
- **PL/SQL Parser** — Parses Oracle PL/SQL (8i → 23c) using a full ANTLR4 4.13.1 grammar. Crawls dependency trees live from Oracle data dictionary, extracts control flow, table operations, triggers, cursors, joins, sequences, exception handlers, and dynamic SQL. 395 unit tests, zero failures.

Both tools share a **unified queue**, **SSE real-time progress streaming**, a **Claude AI integration layer**, and a **Catppuccin dark-themed web UI**.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Web UIs](#web-uis)
4. [Feature Groups](#feature-groups)
   - [Home Dashboard](#1-home-dashboard)
   - [JAR / WAR Analyzer — Code Structure](#2-jar--war-analyzer--code-structure-tab)
   - [JAR / WAR Analyzer — Endpoint Flows](#3-jar--war-analyzer--endpoint-flows-tab)
   - [JAR / WAR Analyzer — Summary (11 sub-tabs)](#4-jar--war-analyzer--summary-tab-11-sub-tabs)
   - [PL/SQL Parser — Home Screen](#5-plsql-parser--home-screen)
   - [PL/SQL Parser — Analysis Screen](#6-plsql-parser--analysis-screen)
   - [PL/SQL Parser — Right Panel Tabs](#7-plsql-parser--right-panel-tabs)
   - [Claude AI Integration](#8-claude-ai-integration)
   - [Analysis Queue System](#9-analysis-queue-system)
   - [Chat System](#10-chat-system)
   - [Export](#11-export)
5. [Analysis Engine Internals](#analysis-engine-internals)
   - [What the JAR Engine Extracts](#what-the-jar-engine-extracts)
   - [Collection & Table Detection](#collection--table-detection)
   - [Call Graph Resolution](#call-graph-resolution)
   - [PL/SQL Flow Engine](#plsql-flow-engine)
6. [Modules](#modules)
7. [Configuration](#configuration)
8. [Build](#build)
9. [Technology Stack](#technology-stack)
10. [REST API Reference](#rest-api-reference)
11. [Storage Layout](#storage-layout)
12. [Screenshots](#screenshots)
13. [Documentation](#documentation)

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Java (JDK) | 17+ | Required |
| Maven | 3.8+ | Required for building |
| Claude CLI | latest | Optional — AI enrichment, verification, chatbot |
| Oracle Database | any | Optional — required for PL/SQL source fetching |

```bash
# Install Claude CLI (optional)
npm install -g @anthropic-ai/claude-code
```

---

## Quick Start

```bash
# Build all modules (skip tests for a faster first build)
mvn clean package -DskipTests

# Start — Linux / macOS
./start.sh

# Start — Windows
start.bat
```

The start scripts run pre-flight checks (Java, JAR, Claude CLI), create required directories, apply JVM tuning flags (`-Xms512m -Xmx4g -XX:+UseG1GC`), then launch.

---

## Web UIs

| URL | Description |
|-----|-------------|
| `http://localhost:8083/` | **Home Dashboard** — stats, quick submit, queue monitor, Claude sessions |
| `http://localhost:8083/jar/` | **JAR / WAR Analyzer** — upload, code browse, endpoint flows, AI enrichment |
| `http://localhost:8083/parser/` | **PL/SQL Parser Analyzer** — connect to Oracle, flow analysis, procedure explorer |
| `http://localhost:8083/plsql/` | **Legacy PL/SQL Analyzer** — alternative PL/SQL analysis path |

---

## Feature Groups

### 1. Home Dashboard

| Feature | Description |
|---------|-------------|
| **JAR Analyzer card** | Live stats: JARs analyzed, endpoints discovered, classes cataloged, AI-enriched endpoints |
| **PL/SQL Parser card** | Live stats: analyses completed, procedures parsed, tables referenced, lines of code |
| **Quick Submit — PL/SQL tab** | Queue procedure names directly (formats: `PKG.PROC`, `SCHEMA.PKG.PROC`, `PROC_NAME`). Existing records auto-archived before re-queue. Success/error banner auto-hides after 6 s |
| **Quick Submit — JAR tab** | Queue a JAR from a local filesystem path; choose Static or Claude mode |
| **Analysis Queue bar** | One-line summary `N running, N queued, N done` with expandable modal |
| **Queue modal — Running section** | Blue-pulse indicator, type badge, job name, elapsed time, progress bar + percentage, current step text, Cancel button |
| **Queue modal — Queued section** | Position numbers, type badges, Move Up / Move Down reordering, Cancel per job, Apply Order / Discard buttons |
| **Queue modal — History section** | Last 20 completed/failed/cancelled jobs with status icon, type badge, job name, result name, error text, elapsed time |
| **Progress Report — JAR panel** | Total JARs, endpoints, classes; AI Enriched (green), AI Running (blue), AI Pending (orange), Not Enriched; progress bar |
| **Progress Report — PL/SQL panel** | Analyses, procedures, tables, LOC; AI Verified (green), Sessions Running (blue), Sessions Failed (red); progress bar |
| **Claude Sessions table** | All sessions across both tools: Analyzer badge (JAR indigo / Parser green), ID, Status, Type, Detail (truncated with tooltip), Duration |
| **Keyboard shortcuts** | `Escape` — close any open modal |
| **Log FAB** | Bottom-left floating button → live application log panel |
| **Chat FAB** | Bottom-right floating button → AI chatbox |
| **Help / Docs button** | Business Guide (non-technical) or Technical Guide (developer) |

---

### 2. JAR / WAR Analyzer — Code Structure Tab

#### Sidebar
| Feature | Description |
|---------|-------------|
| **JAR list** | Per-JAR entry: class count badge, endpoint count badge, AI badge (IDLE / RUNNING / COMPLETE / FAILED), correction badge `C` or `C(N)`, relative upload date, delete (×) button |
| **Upload** | File picker — accepts `.jar` and `.war` up to 2 GB; Static or Claude mode toggle; Source Project Path field (Claude mode only) |
| **Analyze from local path** | Submit a local filesystem path instead of uploading |
| **Claude progress indicator** | Per-JAR progress bar during active enrichment scans |

#### Top Bar
| Button | Action |
|--------|--------|
| **Sessions** | Full-screen overlay of all Claude sessions (status, JAR, start time, duration) |
| **Logs** | Application log viewer panel |
| **Analysis Status** | Toggle real-time progress log panel (streams decompiling/endpoint-detecting/building messages) |
| **Chat Toggle** | Switch between New (floating chatbox) and Classic (session panel) chat modes |

#### JAR Header (when a JAR is selected)
| Element | Description |
|---------|-------------|
| **Stats badges** | Classes, endpoints, file size, upload date |
| **Mode badge** | STATIC / CORRECTED / Previous |
| **View Corrected** | Switch to Claude-enriched view |
| **View Static** | Switch back to raw static analysis |
| **View Previous** | Load prior Claude correction round for before/after comparison |
| **Enable Claude / Re-scan Claude** | Start or restart AI enrichment; hidden and replaced with progress bar while running |

#### Code Structure — 3 View Modes
| View | Description |
|------|-------------|
| **Package View** | Classes grouped by Java package; main app + internal dependency classes separated; collapsible groups |
| **Project View** | IntelliJ-style `src/main/java` folder hierarchy; single-child path collapsing |
| **Visual Overview** | Card grid grouped by Spring stereotype: `@RestController`, `@Service`, `@Repository`, `@Component` |

#### Class Detail (expanded inside any view)
| Section | Contents |
|---------|----------|
| **Fields** | Annotations (e.g. `@Autowired`, `@Value`), Java type, MongoDB `@Field`/`@BsonProperty` mapping, JPA `@Column` mapping |
| **Methods** | Full signature, parameter names + types, return type, method-level annotations (`@GetMapping`, `@Transactional`, etc.) |
| **Invocations** | Lazy-loaded outgoing method calls per method — target class + method name; click to navigate |

#### Code Structure Toolbar
| Control | Description |
|---------|-------------|
| **Search box** | Real-time filter across class names, method names, annotations (case-insensitive) |
| **Expand All / Collapse All** | Expand or collapse every node |
| **Back / Forward** | Browser-style navigation history |
| **History dropdown** | Jump to any previously viewed class |
| **JAR Source Filter Bar** | Show only classes from specific bundled dependency JARs |

#### Code Panel (right side)
- CFR-decompiled Java source with syntax highlighting
- Scrolls to and highlights selected method
- Read-only view

---

### 3. JAR / WAR Analyzer — Endpoint Flows Tab

#### Endpoint List (left pane)
| Feature | Description |
|---------|-------------|
| **Grouping** | Endpoints grouped under their controller class with count badge; collapsible |
| **Search** | Filter by HTTP method, URL path, method name, controller class (real-time) |
| **HTTP method badges** | GET = green, POST = blue, PUT = orange, DELETE = red |
| **Entry types** | REST (`GET /api/...`), AMQP (`@RabbitListener`), Kafka (`@KafkaListener`), WebSocket (`@MessageMapping`), Scheduled (`@Scheduled`) |
| **Entry rows** | Full path, Java method signature, return type |

#### Endpoint Detail (right pane)
| Feature | Description |
|---------|-------------|
| **Call chain diagram** | Visual flowchart Controller → Service → Repository with stereotype chips and directional arrows |
| **Collections** | MongoDB collections accessed: DATA or VIEW badge per collection |
| **Operation Flow Table** | Step-by-step execution with: step #, stereotype badge, clickable `Class.method`, description, params, return type, dispatch badge, recursive indicator |
| **Dispatch badges** | QUALIFIED · DYNAMIC · HEURISTIC · IFACE ONLY · @PRIMARY · DYNAMIC_DISPATCH · INTERFACE_FALLBACK |
| **Breadcrumb nav** | Back / Next / Root buttons |
| **Cache operations** | `@Cacheable`, `@CacheEvict`, `@CachePut` indicators where present |
| **SQL statements** | Raw SQL literals detected in the call chain |
| **JPA stored procedures** | `SimpleJdbcCall` / `Connection.prepareCall` / `@Procedure` mappings |

---

### 4. JAR / WAR Analyzer — Summary Tab (11 sub-tabs)

#### 4.1 Endpoint Report
- Cards with: HTTP method, path, method signature, collections count, views count, DB ops count, internal calls, external calls, LOC, size (S/M/L/XL), domain badge, performance badge (No Impact / Low / Medium / High based on DB op thresholds)
- **Sort** by: Path, Domain, Collections, DB Ops, External, LOC, Size
- **Domain pill filters** — quick toggle by business domain
- **Advanced Filters panel**: range sliders (Collections, Views, DB Ops, Internal Calls, External Calls, Methods, LOC), HTTP method pills, Size pills, Performance pills, Operations pills (READ / WRITE / UPDATE / DELETE / AGGREGATE / COUNT)
- **Pagination**: 25 per page

#### 4.2 Collection Analysis
| Column | Description |
|--------|-------------|
| Collection Name | DATA or VIEW badge |
| Domain | Business domain (Claims, Accounting, Underwriting, Policy, etc.) |
| Read Ops / Write Ops / Agg Ops | Operation counts |
| Detected Via | REPOSITORY_MAPPING · DOCUMENT_ANNOTATION · STRING_LITERAL · FIELD_CONSTANT · PIPELINE_ANNOTATION |
| Complexity | Weighted score (endpoints×1.0, writes×1.5, aggregation×2.0, cross-domain×3.0) |
| Verification | IN_DB · NOT_IN_DB · NEED_REVIEW |
- **Hierarchical view** (grouped by domain) or **Flat table view** toggle

#### 4.3 External Dependencies
- Module name, domain(s), call count, calling classes/methods (expandable), dependency type (Library / Internal / External HTTP)
- HTTP calls with extracted URLs listed separately

#### 4.4 Distributed Transactions
- Endpoints classified as REQUIRED / ADVISORY / NONE based on multi-datasource access patterns

#### 4.5 Batch Jobs
- Batch name, primary domain, cross-domain touches (badge pills), collections, method count, size (S/M/L/XL)

#### 4.6 Scheduled Jobs
- Method name, class, schedule expression (cron / fixedRate / fixedDelay), domain, collections, method count
- Grouped by execution pattern type

#### 4.7 Aggregation Flows
| Column | Description |
|--------|-------------|
| Pipeline ID | Unique identifier |
| Collections | All collections including `$lookup` join targets |
| Pipeline Stages | Stage count + types ($match, $group, $project, $lookup, $unwind, $sort, $out, $merge, $graphLookup, $unionWith, $facet, etc.) |
| Called From | Method + class executing this pipeline |
| Complexity | Low / Medium / High |
| $Lookup Joins | Number of join stages |
| Dynamic | Yes/No — pipeline built conditionally at runtime |
- **Expand row** → full stage list with per-stage parameters (match predicates, group accumulators, projection fields, sort fields)
- Cross-domain `$lookup` stages highlighted in **red**
- Filters by collection, pipeline stage type, complexity

#### 4.8 Dynamic Flows
| Column | Description |
|--------|-------------|
| Dispatch Type | DYNAMIC_DISPATCH · REFLECTION · DYNAMIC_QUERY · HEURISTIC · INTERFACE_FALLBACK |
| Target Class | Declared interface or abstract class |
| Implementations Found | Concrete implementations resolved in JAR |
| Confidence | Resolution confidence |
| Called From | Calling method |
| Collections Touched | Possible collections depending on implementation |
| Risk Level | Low / Medium / High |

#### 4.9 Verticalisation
**Bean Crossing** — Spring beans called across domain boundaries:
- Target bean, stereotype, source module, source domain, call count, caller endpoints, recommendation (REST_API)

**Data Crossing** — MongoDB collections accessed by multiple domains:
- Collection, type (DATA/VIEW), owner domain, accessed-by domains, access type, endpoints, recommendation

#### 4.10 Claude Insights *(requires Claude enrichment)*
- Business process description, risk flags (missing error handling / data inconsistency / perf bottlenecks / security), migration difficulty score (1–5), key insights, recommendations

#### 4.11 Claude Corrections *(requires Claude enrichment)*
- Per endpoint: added count, removed count, verified count, verified % vs static analysis
- Side-by-side diff showing what Claude changed from static results

---

### 5. PL/SQL Parser — Home Screen

| Feature | Description |
|---------|-------------|
| **Step 1 — Connection** | Project dropdown + Environment dropdown (populated from `plsql-config.yaml`) |
| **Step 2 — Owner** | Optional schema owner or Auto-detect across all accessible schemas |
| **Step 3 — Target** | Object name input (e.g. `PKG_CUSTOMER` or `PKG.PROC`), object type dropdown (PACKAGE / PROCEDURE / FUNCTION / TRIGGER), Analyze button |
| **Progress phases** | Starting → Resolving dependencies → Downloading source → Parsing → Building output → Complete |
| **Analysis History tab** | Searchable table: Name, Date, Procs, Tables, LOC; click any row to reload without re-running |
| **Performance Summary tab** | Phase timing breakdown for past runs |
| **Connection Manager** | Gear button → 3-column modal: Projects list (Add Project) + Environments panel + Detail panel (JDBC URL, username, password, Test Connection button, Save, Delete) |
| **Logs button** | Real-time server log viewer |

---

### 6. PL/SQL Parser — Analysis Screen

#### Top Bar Actions
| Button | Description |
|--------|-------------|
| **Pull** | Re-fetch source from Oracle database and re-analyze |
| **Refresh** | Reload cached analysis from disk |
| **Search** | Global search across procedures, tables, and code |
| **Export** | Open export modal (JSON / CSV) |
| **Logs** | Open log viewer |
| **Verify** | Trigger Claude AI verification *(Claude mode)* |
| **Kill** | Abort running Claude session *(Claude mode)* |
| **Chunks** | View prompt chunks sent to Claude *(Claude mode)* |
| **Sessions** | Past Claude enrichment sessions *(Claude mode)* |
| **Claude mode toggle** | Switch between Static (parser-only) and Claude (AI-enriched) results |

#### Stats bar
Procs · Tables · Edges · LOC · Max Depth · Errors

#### Left Panel — Call Flow Tab
- All discovered procedures ordered by call depth
- Per row: schema badge (color-coded per schema), type icon (P=Procedure / F=Function / T=Trigger), procedure name, depth badge, LOC badge
- Filter box + lazy-load 50 at a time (scroll to load more)

#### Left Panel — Tables Tab
- All referenced tables with operation badges: S (SELECT) · I (INSERT) · U (UPDATE) · D (DELETE) · M (MERGE) · C (CREATE) · T (TRUNCATE)
- Access count per table; click to open Table Detail Modal

---

### 7. PL/SQL Parser — Right Panel Tabs

#### Explore — Hierarchy sub-tab
- Depth-indented expandable call tree
- Per row: depth indicator (L0/L1/L2…), step counter, schema badge, clickable procedure name, clickable source line link, call type badge (INTERNAL / EXTERNAL / TRIGGER / DYNAMIC), LOC, complexity risk (L=green / M=orange / H=red)
- Toolbar: Expand All, Collapse All, breadcrumb nav, search

#### Explore — Trace sub-tab
- Flat list of all procedures, same columns, no indent; 200 per page with "Show more"

#### Explore — References sub-tab
- Calls made by selected procedure; toggle INT / EXT scope filter
- Columns: Name, call type, count, line numbers list

#### Table Ops Tab
- Full cross-analysis table operations grid
- Filter pills: SELECT / INSERT / UPDATE / DELETE / MERGE / CREATE / TRUNCATE / DROP
- Search box + column-level dropdown filters + sortable columns
- Columns: Table Name (clickable), Type (TABLE/VIEW/MATERIALIZED VIEW), Operation badges, Count, Triggers count (clickable), Procedures count, Usage
- **Row expansion**: access details (op, procedure link, line link, WHERE clause), tables via triggers, Claude verification badges per access

#### Details — Sequences sub-tab
- Oracle sequences with NEXTVAL/CURRVAL operation badges, usage count, procedure count
- Filter pills: NEXTVAL / CURRVAL; sortable; expandable rows (operation, procedure, line number)

#### Details — Join Summary sub-tab
- All table joins: Left Table, Right Table, join type badges (INNER=blue / LEFT=green / RIGHT=orange / CROSS=red / FULL=purple), usage count, ON predicate preview
- Filter by join type pills + search + column filters
- **Row expansion**: per-occurrence details with "Show SQL" button

#### Summary — Dashboard sub-tab
- Object type distribution chart + schema distribution chart
- Highlights: Most LOC, Deepest, Most Tables, Most Calls (all clickable)

#### Summary — Claude Insights sub-tab
AI-generated summaries, risk assessments, architectural observations

#### Summary — Claude Corrections sub-tab
Static parser vs Claude-verified side-by-side comparison

#### Complexity Tab
- Risk filter pills: Low (green) / Medium (orange) / High (red)
- Summary cards (count + % per risk level — click to filter)
- Table: Procedure, Schema, LOC, Tables, Dependencies, Dynamic SQL, Depth, Score, Risk badge; lock icon for encrypted/wrapped procedures
- All columns sortable + dropdown filters
- **Row expansion — Score Breakdown**: LOC, Tables, Calls, Cursors, Dynamic SQL, Depth — each showing value, weight, contribution, bar chart

#### Graph Tab
- Interactive draggable call graph diagram with pan + zoom
- Nodes = procedures, directed edges = calls

#### Source Tab
- Full PL/SQL source viewer with syntax highlighting (keywords, strings, comments, identifiers)
- Line numbers (clickable), in-source search with match count + Prev/Next navigation
- Back / Forward history, Copy to clipboard, Fullscreen toggle
- **Sidebar panels** (collapsible, each filterable):
  - Parameters — declared params with type and mode (IN/OUT/IN OUT)
  - Variables — locally declared variables with types
  - Tables — tables accessed; click to open Table Detail Modal
  - Cursors — cursor declarations
  - Calls Made — procedures called; click to navigate
  - Called By — procedures that call this one; click to navigate

#### Scope Controls
**Direct** (selected procedure only) · **Subtree** (procedure + direct children) · **SubFlow** (procedure + all descendants) · **Full** (entire analysis)
Changing scope updates all right-panel tabs instantly.

#### Table Detail Modal (7 tabs)
| Tab | Contents |
|-----|---------|
| **Accesses** | Op (SELECT/INSERT/…), clickable procedure, clickable line; filter by op type pills; Claude verification column when enrichment present |
| **Columns** | Column # (ordinal), column name (PK icon), data type, nullable, default value; sortable + filterable |
| **Indexes** | Index name, uniqueness (UNIQUE/NONUNIQUE), columns; sortable + filterable |
| **Constraints** | Constraint name, type badge (P=blue/U=green/R=orange/C=gray), columns, references; sortable + filterable |
| **Joins** | Join type badge, joined table, alias, ON condition, procedure (clickable), line (clickable); sortable + filterable |
| **Triggers** | Schema badge, trigger name (clickable to source), BEFORE/AFTER, event (INSERT/UPDATE/DELETE), row/statement level, source badge (DB/SRC); View Source button, Definition button |
| **Claude** *(when enriched)* | Op, procedure, line, Static badge, Claude Status (confirmed/corrected/new/removed), Reason, Accept/Reject review buttons; summary counts (Confirmed / Removed / New / Unverified) |

#### Query Runner
- Pre-filled `SELECT * FROM <table> WHERE ROWNUM <= 20`, editable SQL textarea
- Run button (Ctrl+Enter shortcut), Clear button, Max Rows dropdown (20/50/100/200/500)
- Results: status bar (row count, column count, execution time), results table with row numbers, NULL highlighting, truncation warning

#### Export Modal (PL/SQL Parser)
- Format: JSON or CSV
- Sections: Procedures, Table Operations, Joins, Cursors, Call Graph, Summary Stats

---

### 8. Claude AI Integration

| Capability | Detail |
|------------|--------|
| **JAR endpoint enrichment** | Business process descriptions, risk flags, migration difficulty (1–5), key insights, recommendations |
| **JAR full scan** | Enriches all endpoints; configurable max-endpoints limit (-1 = all) |
| **JAR corrections** | Claude verifies static call chains — adds missed calls, removes false positives, corrects operation types |
| **JAR correction resume** | Skips already-corrected endpoints — safe to re-run after failure |
| **Swarm clustering** | Endpoints grouped by shared dependencies before sending to Claude for coherent multi-endpoint context |
| **PL/SQL verification** | Claude verifies parser-extracted table operations and call relationships |
| **Version management** | Toggle between Static, Claude-enriched, Previous Claude version; full revert to static |
| **Chunking** | Call trees split by configurable limits: max 50 K chars, 500 nodes, depth 3 per chunk |
| **Parallel chunks** | Up to N chunks analyzed simultaneously (default 4 per endpoint) |
| **Parallel endpoints** | Up to N endpoints corrected simultaneously (default 3) |
| **Fragment caching** | Intermediate responses cached to disk — resume on failure |
| **Session management** | Per-tool session tracking with kill-single and kill-all |
| **Call logging** | Every Claude interaction logged: prompt, response, token counts, latency, success/failure |
| **Run logs** | Timestamped per-run log files with per-endpoint statistics |
| **SSE progress** | Real-time enrichment/verification progress streamed to browser |
| **Process model** | Claude spawned as external OS process via ProcessBuilder; prompt piped via stdin (avoids Windows 32KB CLI arg limit) |
| **Timeout management** | Per-endpoint timeout (default 7200 s), stream drain timeout (3000 s), executor shutdown timeout (60 s) |
| **AWS Bedrock support** | Pre-configured env vars: `CLAUDE_CODE_USE_BEDROCK=1`, `AWS_PROFILE=ClaudeCode`, `AWS_REGION=eu-central-1` |
| **Process kill** | Active Claude processes tracked per thread ID — precise kill or bulk kill via API |

---

### 9. Analysis Queue System

| Feature | Detail |
|---------|--------|
| **Single worker thread** | Prevents concurrent heavy analyses from exhausting memory/CPU |
| **Job types** | JAR_UPLOAD · PLSQL_ANALYSIS · PLSQL_FAST_ANALYSIS · PARSER_ANALYSIS · CLAUDE_ENRICH · CLAUDE_ENRICH_SINGLE · CLAUDE_RESCAN · CLAUDE_FULL_SCAN · CLAUDE_CORRECT · CLAUDE_CORRECT_SINGLE · PLSQL_CLAUDE_VERIFY |
| **Job lifecycle** | QUEUED → RUNNING → COMPLETE / FAILED / CANCELLED |
| **Per-job tracking** | UUID-based ID, type, display name, status, current step text, step #/total, progress %, timestamps (submitted/started/completed), error message, last 50 log lines, metadata key-value pairs |
| **Reordering** | Move pending jobs up or down; Apply Order / Discard |
| **Cancellation** | Pending: removed immediately; Running: Thread.interrupt() with clean executor shutdown |
| **Follow-up chaining** | A completed job can automatically queue a follow-up job (e.g. analyze then auto-enrich) |
| **History** | Last 50 completed/failed/cancelled jobs retained |
| **SSE broadcast** | Every state change pushed to all connected browser tabs via `/api/queue/events` |

---

### 10. Chat System

| Mode | Description |
|------|-------------|
| **Floating chatbox** | FAB (bottom-right); natural-language questions about current analysis; minimize back to FAB |
| **Classic panel** | Session-based side panel with full conversation history and session persistence |
| **Chat toggle** | Switch modes from top bar; preference persists across page reloads |
| **Chat sessions** | Create, list, continue, delete sessions; generate session report |
| **File context** | Chat API supports reading/writing files for additional context |
| **Log FAB** | Separate bottom-left FAB → live rolling application log panel |
| **Help button** | Business Guide (non-technical) or Technical Guide (developer-oriented) |

---

### 11. Export

#### JAR Analyzer (Excel / JSON)
| Sheet / Key | Contents |
|-------------|----------|
| Summary | Key metrics: endpoints, collections, views, domains, cross-module endpoints, batch jobs |
| Endpoints | HTTP method, path, name, collections, views, DB ops, methods, LOC, scope calls, operations, size, performance |
| Endpoint-Collections | Per-endpoint → collection mapping with op type and detection source |
| Collections | Per-collection metrics: endpoints accessing, domains, usage count, verification |
| Collection Summary | Aggregated with operation breakdown |
| Collection Usage Detail | Detailed cross-reference |
| Transactions | Transaction requirement classification per endpoint |
| Batch | Scheduled/batch jobs |
| Views | MongoDB views |
| External | Cross-module dependency details |
| Extended Calls Detail | External call breadcrumb chains |
| Vertical Method Calls | Method invocation patterns |
| Vertical Cross-Domain | Cross-domain dependency chains |

Excel styling: conditional formatting (XL=red, L=orange), header/title/section rows, alternating data rows, color-coded fills, column freeze, auto-filters.

#### PL/SQL Parser (JSON / CSV)
Procedures, Table Operations, Joins, Cursors, Call Graph, Summary Stats

---

## Analysis Engine Internals

### What the JAR Engine Extracts

**From bytecode (ASM 9.7):**
- Class metadata: FQN, package, superclass, interfaces, annotations, access flags, stereotypes
- Fields: name, type, annotations, constant values, `@Field`/`@BsonProperty`/`@Column` DB field mappings
- Methods: name, descriptor, return type, parameters (extracted from LocalVariableTable), annotations
- Method invocations: target class, method name, descriptor, opcode, receiver field tracking
- Field accesses: GETFIELD/PUTFIELD/GETSTATIC/PUTSTATIC with line numbers
- String literals: LDC constants filtered by length and SQL/collection name patterns
- Lambda / method references: InvokeDynamic instruction analysis
- Local variable table: type-annotated variable map

**Entry point types detected:**
- REST: `@RequestMapping`, `@GetMapping`, `@PostMapping`, `@PutMapping`, `@DeleteMapping`, `@PatchMapping`
- Async: `@RabbitListener` (AMQP), `@KafkaListener` (KAFKA), `@MessageMapping` (WS)
- Scheduled: `@Scheduled` (cron/fixedRate/fixedDelay)
- Event-driven: `@EventListener`, `@TransactionalEventListener`

---

### Collection & Table Detection

**MongoDB collection detection — 5 source types (priority order):**
1. **REPOSITORY_MAPPING** — generic signature from `MongoRepository<Entity, Id>` via ASM signature parsing
2. **DOCUMENT_ANNOTATION** — `@Document(collection = "...")` on entity class
3. **STRING_LITERAL** — string LDC constants matching collection name pattern (configurable regex)
4. **FIELD_CONSTANT** — static final String fields on repository class
5. **PIPELINE_ANNOTATION** — `@Aggregation` annotation pipeline references

**Name derivation fallback chain:**
- Generic entity type → CamelCase → `UPPER_SNAKE` (e.g. `ClaimTransaction` → `CLAIM_TRANSACTION`)
- Stem matching: `ClaimRepository` → `Claim` → `CLAIM_*` prefix search
- Implicit hard-coded patterns: `SaveOperationValidatorImpl` → `USER_CONSTRAINTS`, `SequenceService` → `SEQUENCES`

**JPA / JDBC table detection:**
- `JdbcTemplate` / `NamedParameterJdbcTemplate` SQL string extraction
- `EntityManager`: `createQuery` (JPQL), `createNativeQuery`, `createStoredProcedureQuery`, `createNamedQuery`
- `CriteriaBuilder.createQuery(Entity.class)`
- Spring Data JPA methods → operation type mapping
- `SimpleJdbcCall.withProcedureName` / `Connection.prepareCall("{call ...}")`
- `@Query`, `@Procedure`, `@NamedQuery`, `@NamedNativeQuery` SQL extraction

---

### Call Graph Resolution

**Dispatch modes (per call node):**
- **QUALIFIED** — resolved with full certainty from explicit type info
- **DYNAMIC** / **DYNAMIC_DISPATCH** — interface/abstract class polymorphism
- **HEURISTIC** — naming-convention inference (single-implementation or stem match)
- **IFACE ONLY** / **INTERFACE_FALLBACK** — only interface declaration found; no concrete implementation
- **@PRIMARY** — Spring `@Primary` annotation used to select among multiple candidates

**Index structures built at analysis time:**
- `classMap` — FQN → class data
- `interfaceImplMap` — interface FQN → all concrete implementors (transitive expansion)
- `childClassMap` / `superClassMap` — full class hierarchy
- `entityCollectionMap` / `entityTableMap` — entity → DB name
- `repoCollectionMap` / `repoTableMap` — repository → DB name
- `collectionFieldMappings` — collection → Java field → DB column
- `beanNameToImplMap` — Spring bean name → implementation class
- `enumConstantsMap` — enum class → string constant values
- `namedQueryMap` — `@NamedQuery` name → JPQL/SQL text
- Event listener registry: event type → listener method

**Call tree limits (configurable):**

| Property | Default |
|----------|---------|
| `calltree.max-depth` | 20 |
| `calltree.max-children-per-node` | 30 |
| `calltree.max-nodes-per-tree` | 2000 |

---

### PL/SQL Flow Engine

**From each PL/SQL object, the ANTLR4 visitor extracts:**
- Flow nodes (calls, DML statements, conditional branches, loops)
- Table operations: SELECT / INSERT / UPDATE / DELETE / MERGE / TRUNCATE with table name, schema, WHERE predicate
- Cursor declarations and OPEN/FETCH/CLOSE operations
- Dynamic SQL: `EXECUTE IMMEDIATE`, `DBMS_SQL`, concatenated strings
- Exception handlers: exception name, handler body, re-raise detection
- JOIN expressions: left/right table, join type, ON predicate, line number
- Sequence references: `NEXTVAL` / `CURRVAL` usage with line numbers
- Variable declarations with type and initial value
- Subprogram (nested package procedures) extraction
- FORALL / BULK COLLECT patterns

**Dependency crawling:**
- BFS traversal of Oracle data dictionary (`ALL_OBJECTS`, `ALL_SYNONYMS`, `ALL_DEPENDENCIES`, `CONNECT BY` transitive queries)
- Schema resolution cached to `data/cache-plsql/*.tsv`; cache clearable via `--clear-cache`
- Source downloaded from `ALL_SOURCE` view
- Non-parseable types skipped gracefully: TYPE, SYNONYM, SEQUENCE, JAVA CLASS
- Configurable: `maxDepth` and per-entry timeout (default 120 s)
- Parallel downloads: 8 threads for source, 4 for triggers, 4 for metadata

---

## Modules

```
plsql-jar-analyzer/                     Parent POM (Spring Boot 3.2.5)
  plsql-config/                         Shared YAML/JSON config model — Oracle connections, schema mappings
  plsql-parser/                         ANTLR4 PL/SQL parser — Oracle 8i–23c, 395 tests, zero failures
  plsql-analyzer-core/                  PL/SQL service layer — call graphs, table/join/cursor/sequence aggregation
  jar-analyzer-core/                    Java bytecode engine — ASM, CFR, Excel export, Claude AI pipeline
  unified-web/                          Spring Boot web app — REST APIs, 4 UIs, queue, SSE, chat (planned)
  docs/                                 Architecture + user manuals
  config/                               Runtime config (plsql-config.yaml, domain-config.json, prompts/)
  start.bat / start.sh                  Launch scripts with pre-flight checks and JVM tuning
```

### Module Dependency Graph

```
plsql-config (no dependencies)
     ↑
plsql-parser (depends on plsql-config)
     ↑
plsql-analyzer-core (depends on plsql-parser, plsql-config)
     ↑
jar-analyzer-core (independent — no PL/SQL module dependencies)
     ↑
unified-web (depends on all four above)
```

---

## Configuration

### Runtime Directories

| Property | Default | Contents |
|----------|---------|----------|
| `app.config-dir` | `config/` | `plsql-config.yaml`, `domain-config.json`, `prompts/` |
| `app.data-dir` | `data/` | `jar/`, `plsql/`, `plsql-parse/`, `cache-plsql/`, `claude-chatbot/`, log files |

Override at startup:
```bash
java -jar unified-web.jar --app.config-dir=/etc/analyzer/config --app.data-dir=/var/data/analyzer
```

### plsql-config.yaml

Oracle connections organized by project → environment hierarchy:

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

Managed at runtime via `GET/POST/PUT/DELETE /api/plsql/config/projects/...`

### domain-config.json

Defines the 12 business domains (Claims, Accounting, Underwriting, Policy, CounterParty, Reinsurance, Common, SystemAdmin, Distribution, EInvoice, Infrastructure, Treaty) with their collection prefixes, operation type keywords, size thresholds, and complexity scoring weights.

### Key application.properties

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8083` | HTTP port |
| `spring.servlet.multipart.max-file-size` | `2GB` | Max JAR/WAR upload size |
| `calltree.max-depth` | `20` | Call tree depth limit |
| `calltree.max-nodes-per-tree` | `2000` | Call tree node limit |
| `claude.analysis.parallel-chunks` | `4` | Parallel Claude chunk workers |
| `claude.analysis.parallel-endpoints` | `3` | Parallel Claude correction workers |
| `claude.analysis.max-endpoints` | `-1` | Max endpoints to enrich (-1 = unlimited) |
| `claude.chunking.max-chunk-chars` | `50000` | Characters per Claude chunk |
| `claude.chunking.max-prompt-chars` | `180000` | Total prompt size ceiling |
| `claude.timeout.per-endpoint` | `7200` | Per-endpoint AI timeout (seconds) |
| `plsql.threads.source-fetch` | `8` | Parallel PL/SQL source download threads |
| `plsql.tree.max-depth` | `50` | PL/SQL dependency tree depth limit |
| `plsql.tree.max-nodes` | `2000` | PL/SQL dependency tree node limit |

All properties are overridable via `--property=value` at startup. Full reference: [`docs/manual-properties.md`](docs/manual-properties.md)

---

## Build

```bash
# Full build with tests
mvn clean package

# Skip tests (faster)
mvn clean package -DskipTests

# Build unified-web and all its dependencies
mvn clean package -pl unified-web -am

# Build a specific module
mvn clean install -pl plsql-parser
mvn clean install -pl jar-analyzer-core
```

Output: `unified-web/target/unified-web-1.0.0-SNAPSHOT.jar`

### JVM Tuning (applied by start scripts)

| Flag | Value | Purpose |
|------|-------|---------|
| `-Xms` | `512m` | Initial heap |
| `-Xmx` | `4g` | Max heap (large JAR analyses can generate 100–400 MB JSON) |
| `-XX:MaxMetaspaceSize` | `256m` | Metaspace ceiling |
| `-XX:+UseG1GC` | — | G1 garbage collector |
| `-XX:G1HeapRegionSize` | `4m` | G1 region size |
| `-XX:+UseStringDeduplication` | — | Deduplicate identical strings |
| `-XX:ParallelGCThreads` | `4` | Parallel GC threads |
| `-XX:ConcGCThreads` | `2` | Concurrent GC threads |
| `-XX:+HeapDumpOnOutOfMemoryError` | — | Dump heap on OOM |

---

## Technology Stack

| Concern | Library | Version |
|---------|---------|---------|
| Runtime framework | Spring Boot | 3.2.5 |
| PL/SQL parsing | ANTLR4 | 4.13.1 |
| SQL normalization | JSqlParser | 5.0 |
| Bytecode analysis | OW2 ASM | 9.7 |
| Java decompilation | CFR | 0.152 |
| Excel export | Apache POI | 5.2.5 |
| Oracle connectivity | ojdbc11 | 23.3.0.23.09 |
| MongoDB catalog | MongoDB Driver Sync | 4.11.2 |
| Serialization | Jackson (YAML + JSON) | Boot-managed |
| AI enrichment | Claude CLI (external process) | any |
| Frontend | Vanilla JS (IIFE modules) | no build step |
| Java | JDK | 17 |

---

## REST API Reference

### Queue (`/api/queue`)
`GET /api/queue` · `GET /api/queue/{id}` · `POST /api/queue/{id}/cancel` · `POST /api/queue/reorder` · `GET /api/queue/events` (SSE stream)

### JAR Analyzer (`/api/jar/jars`)
Upload, analyze-local, list, get, delete, summary, summary slices (headers/external-calls/dynamic-flows/aggregation-flows/beans), class tree, class by index, endpoint call tree, endpoint nodes, decompile, Claude enrich/enrich-single/rescan/full-scan/correct/correct-single/revert, versions, corrections, correction logs, run logs, claude-stats, claude-fragments, kill-sessions, fetch-catalog, catalog, sessions, progress SSE, logs, export Excel, reanalyze.

### JAR Chatbox (`/api/jars/{id}/chat`)
POST message · GET history · DELETE history

### PL/SQL Parser (`/api/parser/analyses`)
List, analyze, index, node, tables, call-graph, source, procedures, joins, cursors, sequences, call-tree (forward + reverse callers), resolver. Claude: verify, progress, progress-stream, result, chunks, chunks-summary, chunk by id, table-chunks, review, apply, kill session, versions (load-static/load-claude/load-prev/revert). Config: GET config. Status: `/api/parser/claude/status` · sessions · kill-all.

### Legacy PL/SQL (`/api/plsql`)
Full/fast analysis, history, versions, call-graph, procedure detail, call-tree, tables, table metadata, triggers, sequences, joins, cursors, errors, source, references, search, jobs. Claude: verify, progress-stream, result, chunks, sessions, kill, versions. Database: users, test, objects, source, cached-source, packages, find, table-info, query. Config: full CRUD for projects/environments/connections.

### Shared
`/api/sessions` · `/api/sessions/summary` · `/api/chat/*` · `/api/config/polling` · `/api/docs` · `/api/docs/{id}`

> Full endpoint reference: [`docs/manual-architecture.md`](docs/manual-architecture.md) — Section 8 REST API Map

---

## Storage Layout

```
config/
  plsql-config.yaml               Oracle DB connections (project/environment hierarchy)
  domain-config.json              12 business domains + operation types + complexity rules
  prompts/                        10 Claude prompt templates
    plsql-verification.txt
    java-mongo-analysis.txt       (+ chunk + correction variants)
    java-oracle-analysis.txt      (+ chunk + correction variants)
    java-both-analysis.txt        (+ chunk + correction variants)

data/
  unified-analyzer.log            Rolling log — 10 MB per file, max 500 MB total
  jar/{normalizedKey}/
    stored.jar                    Uploaded JAR/WAR
    analysis.json                 Static analysis (100–400 MB for large JARs)
    analysis_corrected.json       Claude-enriched version
    analysis_corrected_prev.json  Previous Claude round
    _header.json                  Lightweight sidecar for fast listing
    mongo-catalog.json            MongoDB collection metadata
    claude/                       Claude response fragments + meta.json
    corrections/                  Per-endpoint correction files
    endpoints/                    Per-endpoint breakdown JSON files
    chat/                         Chatbot conversation history
  plsql/
    {analysisName}/claude/        Legacy PL/SQL Claude verification results
  plsql-parse/{analysisName}/
    index.json                    Procedure list + analysis metadata
    call-graph.json               Call graph data
    tables.json                   Table operations summary
    source/                       Downloaded PL/SQL source files
    *.json                        Chunked flow results per object
    claude/                       Claude verification results + versions
  cache-plsql/
    *.tsv                         SchemaResolver Oracle query cache (tab-separated)
  claude-chatbot/{sessionId}/     Classic chat session storage
```

---

## Screenshots

> Place screenshots in `docs/screenshots/` and replace each row below with `![alt](docs/screenshots/filename.png)`.

| Screen | Target file |
|--------|------------|
| Home Dashboard — stats cards + queue bar | `docs/screenshots/home-dashboard.png` |
| Home Dashboard — Queue modal (running + pending + history) | `docs/screenshots/home-queue-modal.png` |
| Home Dashboard — Progress Report panels | `docs/screenshots/home-progress-report.png` |
| Home Dashboard — Claude Sessions table | `docs/screenshots/home-claude-sessions.png` |
| JAR Analyzer — sidebar + Code Structure Package View | `docs/screenshots/jar-code-structure-package.png` |
| JAR Analyzer — Code Structure Project View + code panel | `docs/screenshots/jar-code-structure-project.png` |
| JAR Analyzer — Code Structure Visual Overview (stereotypes grid) | `docs/screenshots/jar-code-visual-overview.png` |
| JAR Analyzer — Endpoint Flows call chain + Operation Flow Table | `docs/screenshots/jar-endpoint-flows.png` |
| JAR Analyzer — Summary Endpoint Report with advanced filters | `docs/screenshots/jar-summary-endpoint-report.png` |
| JAR Analyzer — Summary Collection Analysis (hierarchical view) | `docs/screenshots/jar-summary-collections.png` |
| JAR Analyzer — Summary Aggregation Flows with stage expansion | `docs/screenshots/jar-summary-aggregation-flows.png` |
| JAR Analyzer — Summary Verticalisation (bean + data crossing) | `docs/screenshots/jar-summary-verticalisation.png` |
| JAR Analyzer — Summary Dynamic Flows | `docs/screenshots/jar-summary-dynamic-flows.png` |
| JAR Analyzer — Summary Distributed Transactions | `docs/screenshots/jar-summary-transactions.png` |
| JAR Analyzer — Summary Batch + Scheduled Jobs | `docs/screenshots/jar-summary-batch-scheduled.png` |
| JAR Analyzer — Claude Insights tab | `docs/screenshots/jar-claude-insights.png` |
| JAR Analyzer — Claude Corrections tab | `docs/screenshots/jar-claude-corrections.png` |
| PL/SQL Parser — Home screen 3-step wizard | `docs/screenshots/parser-home.png` |
| PL/SQL Parser — Analysis screen overview (left + right panels) | `docs/screenshots/parser-analysis-overview.png` |
| PL/SQL Parser — Explore Hierarchy call tree | `docs/screenshots/parser-explore-hierarchy.png` |
| PL/SQL Parser — Table Ops tab with filters | `docs/screenshots/parser-table-ops.png` |
| PL/SQL Parser — Complexity tab with score breakdown | `docs/screenshots/parser-complexity.png` |
| PL/SQL Parser — Graph tab (interactive call graph) | `docs/screenshots/parser-call-graph.png` |
| PL/SQL Parser — Source tab + sidebar panels | `docs/screenshots/parser-source-view.png` |
| PL/SQL Parser — Table Detail Modal (7 tabs) | `docs/screenshots/parser-table-detail-modal.png` |
| PL/SQL Parser — Query Runner | `docs/screenshots/parser-query-runner.png` |
| PL/SQL Parser — Connection Manager Modal | `docs/screenshots/parser-connection-manager.png` |
| Chat — Floating chatbox | `docs/screenshots/chat-floating.png` |
| Chat — Classic chat panel | `docs/screenshots/chat-classic.png` |

---

## Documentation

| Document | Description |
|----------|-------------|
| [`docs/manual-architecture.md`](docs/manual-architecture.md) | Architecture, modules, queue system, Claude AI integration, full REST API map, storage layout, frontend architecture, security |
| [`docs/manual-home.md`](docs/manual-home.md) | Home Dashboard complete user manual |
| [`docs/manual-jar-analyzer.md`](docs/manual-jar-analyzer.md) | JAR Analyzer complete user manual |
| [`docs/manual-parser.md`](docs/manual-parser.md) | PL/SQL Parser Analyzer complete user manual |
| [`docs/manual-properties.md`](docs/manual-properties.md) | Full `application.properties` reference |


<img width="1728" height="968" alt="Screenshot 2026-05-05 at 10 40 39 AM" src="https://github.com/user-attachments/assets/e59433d7-a68c-4db9-993c-a4a3800f82ea" />


<img width="1718" height="960" alt="Screenshot 2026-05-05 at 10 40 56 AM" src="https://github.com/user-attachments/assets/467371aa-8bab-4349-bdd3-db32caafe7d9" />

<img width="1722" height="940" alt="Screenshot 2026-05-05 at 10 41 17 AM" src="https://github.com/user-attachments/assets/8323af61-334e-4d0f-9a35-60a3cdbe7794" />

<img width="1718" height="934" alt="Screenshot 2026-05-05 at 10 41 28 AM" src="https://github.com/user-attachments/assets/a815a265-bc31-49d1-910e-b43c1bd1b58a" />

