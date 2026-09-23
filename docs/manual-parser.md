# Parse Analyzer — Complete User Manual

> **Version:** 3.0
> **Last Updated:** 2026-07-19
> **Application Port:** 8083
> **Databases supported:** Oracle PL/SQL · MySQL · Microsoft SQL Server (T-SQL) · IBM Db2 (SQL PL)

---

## Table of Contents

1. [How It Works](#1-how-it-works)
   - [Architecture Overview](#architecture-overview)
   - [Step-by-Step Analysis Flow](#step-by-step-analysis-flow)
   - [Source Fetching Strategy](#source-fetching-strategy)
   - [What Gets Extracted](#what-gets-extracted)
2. [Configuration](#2-configuration)
   - [plsql-config.yaml Structure](#plsql-configyaml-structure)
   - [Projects, Environments, Connections](#projects-environments-connections)
   - [Owner / Schema](#owner--schema)
3. [Home Screen](#3-home-screen)
   - [Step 1 — Connection](#step-1--connection)
   - [Step 2 — Owner](#step-2--owner)
   - [Step 3 — Target and Analysis](#step-3--target-and-analysis)
   - [Progress Bar](#progress-bar)
   - [Manage Connections](#manage-connections)
   - [Logs](#logs)
   - [Analysis History Tab](#analysis-history-tab)
   - [Performance Summary Tab](#performance-summary-tab)
4. [Analysis Screen Layout](#4-analysis-screen-layout)
   - [Top Bar](#top-bar)
5. [Left Panel](#5-left-panel)
   - [Call Flow Tab](#call-flow-tab)
   - [Tables Tab](#tables-tab)
6. [Right Panel — Explore Tab](#6-right-panel--explore-tab)
   - [Hierarchy Sub-Tab](#hierarchy-sub-tab)
   - [Trace Sub-Tab](#trace-sub-tab)
   - [References Sub-Tab](#references-sub-tab)
7. [Right Panel — Table Ops Tab](#7-right-panel--table-ops-tab)
8. [Right Panel — Details Tab](#8-right-panel--details-tab)
   - [Sequences Sub-Tab](#sequences-sub-tab)
   - [Join Summary Sub-Tab](#join-summary-sub-tab)
9. [Right Panel — Summary Tab](#9-right-panel--summary-tab)
10. [Right Panel — Complexity Tab](#10-right-panel--complexity-tab)
11. [Right Panel — Graph Tab](#11-right-panel--graph-tab)
12. [Right Panel — Source Tab](#12-right-panel--source-tab)
13. [Source Code Modal](#13-source-code-modal)
14. [Table Detail Modal](#14-table-detail-modal)
15. [Query Runner](#15-query-runner)
16. [Export Modal](#16-export-modal)
17. [Connection Manager Modal](#17-connection-manager-modal)
18. [Scope Controls](#18-scope-controls)
19. [Chat](#19-chat)
20. [Understanding Output Files](#20-understanding-output-files)
21. [Special Behaviors & Limits](#21-special-behaviors--limits)
22. [Tips & Best Practices](#22-tips--best-practices)

---

## 1. How It Works

### Architecture Overview

The Parse Analyzer is a Spring Boot web application that performs **static analysis** of stored database code across **five SQL dialects — Oracle PL/SQL, MySQL, Microsoft SQL Server (T-SQL), IBM Db2 (SQL PL), and PostgreSQL (PL/pgSQL)**. It reads source from the database, parses it with the ANTLR4 grammar for the selected dialect, and builds a complete picture of the call graph, database access patterns, and code structure — without executing any code.

```
Browser UI  →  REST API  →  Analysis Engine  →  Source DB (Oracle / MySQL / SQL Server / Db2)
                                ↓
                  ANTLR4 Parser for the chosen dialect
             (plsql-parser | mysql-parser | sqlserver-parser | db2-parser)
                                ↓
                  Unified analysis model (shared)
                                ↓
                      JSON output files (data/plsql-parse/)
                                ↓
                         Browser UI reads results
```

> **Databases supported:** Oracle, MySQL, Microsoft SQL Server, IBM Db2. You pick the database on the Home screen; the correct dialect grammar and source-fetch strategy are selected automatically. All downstream features (call flow, table ops, cursors, dynamic SQL, joins, complexity) behave the same regardless of source database.

### Step-by-Step Analysis Flow

**1. User submits request**
- Selects Project → Environment → Owner → Object name + type
- Clicks **Analyze**

**2. Request queued**
- Backend receives `POST /api/parser/analyze`
- Job queued; UI shows live status via SSE (server-sent events)

**3. Entry point resolved**
- If entry is `PACKAGE.PROC`, treats as a specific package subprogram
- If bare `PACKAGE` name + type `PACKAGE`, queries `ALL_PROCEDURES` to discover all members
- Owner/schema resolved from config if not explicitly specified

**4. Dependency crawler starts (BFS)**
- Breadth-first search from the entry point
- Pre-warms dependency cache from Oracle `DBA_DEPENDENCIES`
- For each procedure: downloads source → parses → enqueues its callees
- Tracks visited set to prevent infinite loops
- Tracks depth — deeper calls get higher depth numbers

**5. Source downloaded**
- Checks local file cache first (`sources/` folder)
- Falls back to Oracle `ALL_SOURCE` (line-by-line query)
- If `ALL_SOURCE` appears truncated, attempts `DBMS_METADATA.GET_DDL()` fallback
- Wrapped/encrypted objects are marked unreadable and stored as stub nodes

**6. ANTLR4 parsing**
- Each source file parsed by the PL/SQL grammar
- Extracts: parameters, variables, cursors, table operations, calls, dynamic SQL, exception handlers, sequences, joins
- Records exact source line numbers for every construct

**7. Results written**
- `ChunkedFlowWriter` streams results to `data/plsql-parse/{ANALYSIS_NAME}/`
- Individual node JSONs written per procedure
- Index, call graph, tables aggregated at the end
- `progress.json` updated continuously (shows DONE / FAILED per object)

**8. UI loads results**
- `GET /api/parser/analyses/{name}/index` loads root metadata
- Individual node detail loaded on demand when user clicks a procedure
- Source files served from `sources/` folder

### Source Fetching Strategy

| Priority | Source | When Used |
|----------|--------|-----------|
| 1 | **Local file cache** | `sources/` folder already has the file |
| 2 | **Oracle ALL_SOURCE** | Default — queries row-by-row and concatenates |
| 3 | **DBMS_METADATA.GET_DDL** | When ALL_SOURCE source appears truncated (doesn't end with `/` or `END name;`) |

The DBMS_METADATA fallback returns the full DDL as a CLOB, which handles cases where Oracle's `ALL_SOURCE` view has incomplete rows for very large packages.

### What Gets Extracted

For each procedure/function the parser extracts:

| Category | Details |
|----------|---------|
| **Location** | lineStart, lineEnd, linesOfCode |
| **Parameters** | Name, data type, direction (IN / OUT / IN OUT), line number |
| **Variables** | Name, data type, default value, constant flag, line number |
| **Cursors** | Name, query text, whether a REF CURSOR, tables it accesses, line range |
| **Table operations** | Table name, schema, operations (SELECT/INSERT/UPDATE/DELETE/MERGE), line numbers |
| **Calls out** | Package, procedure/function name, schema, line numbers, argument count |
| **Dynamic SQL** | EXECUTE IMMEDIATE / OPEN FOR text, line numbers |
| **Sequences** | NEXTVAL / CURRVAL references, sequence name, line numbers |
| **Joins** | Left/right table, join type, ON condition, line number |
| **Exception handlers** | WHEN clause names, line numbers |
| **Statement counts** | Breakdown by type (IF, LOOP, SELECT, INSERT, UPDATE, DELETE, COMMIT, RETURN…) |

---

## 2. Configuration

### plsql-config.yaml Structure

The application is configured via `unified-web/config/plsql-config.yaml`. This file defines all database connections, projects, and environments.

```yaml
source_paths:
  - "./plsql-sources"         # Optional: local .sql/.pkb files as source fallback

schema_mappings:
  customer:
    schema: "CUSTOMER"
    description: "Customer schema"

projects:
  - name: "OPUS"
    description: "OPUS Insurance"
    environments:
      - name: "PROD"
        zone: "EU-CENTRAL"
        connections:
          - name: "CUSTOMER_PROD"
            username: "CUSTOMER"
            password: "..."
            hostname: "db.example.com"
            port: 1521
            service_name: "AGICPROD"
            connection_type: "SERVICE_NAME"
            role: "DEFAULT"
            jdbc_url: "jdbc:oracle:thin:@//db.example.com:1521/AGICPROD"
            description: "Customer schema"
```

### Projects, Environments, Connections

| Concept | Description |
|---------|-------------|
| **Project** | Top-level logical group (e.g., "OPUS Insurance"). Can contain multiple environments. |
| **Environment** | A named deployment stage within a project (e.g., PROD, UAT, DEV). Holds a set of connections. |
| **Connection** | One Oracle credential — a username/password pointing at a specific host:port/service. Each schema in an environment needs its own connection entry. |
| **Zone** | Optional label on an environment (e.g., "EU-CENTRAL"). Displayed in the UI for clarity. |

**Multiple connections per environment** are required when the PL/SQL you are analyzing spans multiple Oracle schemas. For example, if `CUSTOMER` calls objects owned by `OPUS_CORE`, both schemas need a connection entry in the same environment.

### Owner / Schema

The **Owner** dropdown on the home screen is populated from the `username` fields of all connections in the selected environment.

| Selection | Behaviour |
|-----------|-----------|
| **Auto-detect (all schemas)** | Searches `ALL_SOURCE` across every configured schema, returns first match |
| **Specific schema** | Restricts DB queries to that schema only; slightly faster |

> **Note:** If two connections share the same username (e.g., you add a DEV connection with the same username as PROD), the owner appears only once in the dropdown (deduplicated). To analyze against a different database with the same schema name, select the correct environment first.

---

## 3. Home Screen

The Home Screen is the entry point for every analysis session. It guides you through selecting a **database type**, a connection, and a target object.

### Step 1 — Database & Connection

| Control | Description |
|---------|-------------|
| **Database dropdown** | Select the source database dialect: **Oracle**, **MySQL**, **SQL Server**, or **IBM Db2**. This drives everything else — the connection list, the Owner/Connection label, the object types offered, and the Target example pattern. |
| **Project dropdown** | Select the project that contains your target database. Projects are filtered to those that have a connection of the chosen database type. |
| **Environment dropdown** | Select the environment within the chosen project (e.g., DEV, UAT, PROD). Each environment maps to a JDBC connection defined in `plsql-config.yaml`. |

The dropdowns cascade: choosing a database filters projects, which re-derives environments and connections. The `?` **Help** button (top toolbar and next to **Analyze**) opens a per-dialect guide describing exactly what to enter for the selected database.

### Step 2 — Owner / Connection

| Control | Description |
|---------|-------------|
| **Owner / Connection dropdown** | The label adapts to the database. For **Oracle** and **Db2** it is the schema **Owner** (with an *Auto-detect (all schemas)* default). For **MySQL** and **SQL Server** it is the **Connection** (database), with an *All connections* default. |

When a specific owner is selected, the analyzer queries only that schema. Auto-detect is recommended when you are unsure which schema owns the target object.

### Step 3 — Target and Analysis

| Control | Description |
|---------|-------------|
| **Object name input** | Enter the name of the object to analyze. The placeholder shows a **dialect-specific example** that changes with the selected database (see below). |
| **Object type dropdown** | Select the type of the target object. Available types depend on the database. |
| **Analyze button** | Starts the analysis. Disabled until connection and object name are provided. |
| **? Help button** | Opens the datasource-aware guide for what to select and type. |

**Target example patterns by database** (shown in the input placeholder):

| Database | Example | Notes |
|----------|---------|-------|
| Oracle | `PKG_CUSTOMER` or `PKG.PROC_NAME` | Package, procedure or function; qualified names supported. |
| MySQL | `film_in_stock` or `get_total` | Stored procedure or function; no package concept. |
| SQL Server | `usp_GetOrders` or `ufn_Total` | Procedure or function; bracketed names accepted. |
| IBM Db2 | `ERP_USP_RUN_CYCLE` | Procedure or function. |

Supported object types:

| Type | Description | Databases |
|------|-------------|-----------|
| `PACKAGE` | Package — all procedures and functions in the body | Oracle only |
| `PROCEDURE` | Standalone stored procedure | All |
| `FUNCTION` | Standalone stored function | All |
| `TRIGGER` | Database trigger | Oracle (and where supported) |

### Progress Bar

Once the **Analyze** button is clicked, a progress bar appears showing the current phase:

| Phase | Description |
|-------|-------------|
| **Starting** | Initializing the analysis session |
| **Resolving dependencies** | Walking the dependency tree to discover all referenced objects |
| **Downloading source** | Retrieving PL/SQL source code from the database |
| **Parsing** | Running the grammar-based parser over all source files |
| **Building output** | Assembling the final analysis model (calls, tables, graphs) |
| **Complete** | Analysis finished; screen transitions to the Analysis view |

### Manage Connections

The **gear button** next to the connection dropdowns opens the Connection Manager modal. See [Connection Manager Modal](#17-connection-manager-modal) for full details.

### Logs

The **Logs button** opens a log viewer window that displays real-time server-side log output. Useful for debugging connection issues, parser errors, or performance problems.

### Analysis History Tab

Located below the connection controls, the **Analysis History** tab lists all previously completed analyses.

| Column | Description |
|--------|-------------|
| **Name** | The analyzed object name |
| **Date** | Timestamp of when the analysis was performed |
| **Procs** | Total number of procedures discovered |
| **Tables** | Total number of tables referenced |
| **LOC** | Total lines of code across all procedures |

- Click any row to **reload** that analysis into the Analysis Screen without re-running it.
- A **search box** at the top of the list lets you filter history entries by name or date.

### Performance Summary Tab

The **Performance Summary** tab shows timing breakdowns for past analysis runs. Use this to identify slow phases or compare performance across different target objects and environments.

---

## 4. Analysis Screen Layout

After an analysis completes (or a history entry is loaded), the screen transitions to the Analysis Screen. This is a two-panel layout with a top bar.

### Top Bar

| Element | Description |
|---------|-------------|
| **Analysis name** | The name of the currently loaded analysis |
| **Stats row** | Quick summary metrics displayed inline |
| **Claude mode toggle** | Switch between `Static` (parser-only) and `Claude` (AI-enriched) |
| **Action buttons** | Pull, Refresh, Search, Export, Logs |
| **Claude control buttons** | Verify, Kill, Chunks, Sessions — visible when Claude mode is active |
| **Claude status indicator** | Shows current state of Claude enrichment (idle, running, complete, error) |

**Stats displayed in the top bar:**

| Stat | Description |
|------|-------------|
| **Procs** | Total procedures/functions discovered |
| **Tables** | Total distinct tables referenced |
| **Edges** | Total call-graph edges (unique caller→callee pairs) |
| **LOC** | Total lines of code |
| **Depth** | Deepest level in the call hierarchy |
| **Errors** | Number of parse errors or unresolved references |

**Action buttons:**

| Button | Description |
|--------|-------------|
| **Pull** | Re-fetch source from the database and re-analyze (use when code has changed) |
| **Refresh** | Reload the current analysis from server cache without re-fetching source |
| **Search** | Opens the **Global Search** overlay to find procedures, tables, or code across the analysis. Fixed 4-segment layout: search textbar (case / whole-word / regex toggles), a dedicated pagination bar, and a fixed-header/scrollable results table, all styled with dedicated `.srch-*` CSS classes. Table results carry the same [kind badge](#tables-tab) as the Tables tab. |
| **Export** | Opens the Export modal — see [Export Modal](#16-export-modal) |
| **Logs** | Opens the log viewer |

**Claude control buttons (Claude mode only):**

| Button | Description |
|--------|-------------|
| **Verify** | Trigger Claude AI verification |
| **Kill** | Abort a running Claude enrichment session |
| **Chunks** | View the chunk breakdown used for Claude API calls |
| **Sessions** | View past Claude enrichment sessions and their results |

---

## 5. Left Panel

The left panel provides navigation across all discovered procedures and tables. It contains two tabs: **Call Flow** and **Tables**.

Both tabs feature:
- A **filter box** at the top for instant text filtering
- **Lazy-loaded batches** of 50 items — scroll to the bottom to load the next batch

### Call Flow Tab

Lists all procedures discovered during the analysis, ordered by call depth.

Each row displays:

| Element | Description |
|---------|-------------|
| **Schema badge** | Color-coded badge showing the owning schema |
| **Type icon** | Single-letter icon: **P** = Procedure, **F** = Function, **T** = Trigger |
| **Procedure name** | Full name of the procedure or function |
| **Depth badge** | Numeric badge showing the call depth (0 = entry point) |
| **LOC badge** | Lines of code for this specific procedure |

**Click** any row to load that procedure's details into the right panel.

### Tables Tab

Lists all tables referenced anywhere in the analysis.

| Element | Description |
|---------|-------------|
| **Table name** | Color-coded by schema |
| **Kind badge** | Shown next to the name for anything other than a real base table: `Local Temp` (SQL Server `#name`), `Global Temp` (`##name`), or `Table Var` (`@name`). Computed client-side from the name prefix (`PA.classifyTableKind` in `shared/tableFramework.js`) — the same helper drives the matching badge in Global Search results. |
| **Operation badges** | Single-letter badges for operations performed |
| **Access count** | Total number of times accessed across all procedures |

Operation badge legend:

| Badge | Operation |
|-------|-----------|
| **S** | SELECT |
| **I** | INSERT |
| **U** | UPDATE |
| **D** | DELETE |
| **M** | MERGE |
| **C** | CREATE |
| **T** | TRUNCATE |

**Click** any table row to open the [Table Detail Modal](#14-table-detail-modal).

---

## 6. Right Panel — Explore Tab

The **Explore** tab contains four sub-tabs: **Hierarchy**, **Trace**, **References**, and **Transactions**.

### Hierarchy Sub-Tab

The default view. Displays a depth-indented call tree with expand/collapse toggles.

Each row contains:

| Element | Description |
|---------|-------------|
| **Depth indicator** | Label showing the level: L0, L1, L2, etc. |
| **Step counter** | Sequential step number in the execution flow |
| **Schema badge** | Color-coded schema identifier |
| **Procedure name** | Clickable — navigates to the source view for that procedure |
| **Source line link** | Clickable line number that jumps directly to the calling line in source |
| **Call type badge** | Categorization of the call |
| **LOC** | Lines of code for this procedure |
| **Complexity risk** | Risk level indicator: **L** (Low), **M** (Medium), **H** (High) |

Call type badges:

| Badge | Meaning |
|-------|---------|
| `INTERNAL` | Call to a procedure within the same package or analysis scope |
| `EXTERNAL` | Call to a procedure outside the current package/scope |
| `TRIGGER` | Invocation through a database trigger |
| `DYNAMIC` | Call constructed via dynamic SQL (EXECUTE IMMEDIATE, etc.) |

**Toolbar actions:**

| Action | Description |
|--------|-------------|
| **Expand All** | Expand every node in the tree |
| **Collapse All** | Collapse all nodes to root level |
| **Breadcrumb nav** | Shows path to currently focused node; click any breadcrumb to jump back |
| **Search** | Filter the tree by procedure name |

### Trace Sub-Tab

A flat, non-hierarchical list of every procedure in the analysis. Same columns as Hierarchy but without indentation.

- **Paginated:** Displays 200 items per batch
- **"Show more" button:** Load next batch of 200

### References Sub-Tab

Shows what the currently selected procedure calls.

| Element | Description |
|---------|-------------|
| **Scope filter pills** | Toggle between **INT** (internal calls) and **EXT** (external calls) |

Columns:

| Column | Description |
|--------|-------------|
| **Name** | Name of the called procedure |
| **Call type** | INTERNAL or EXTERNAL |
| **Count** | Number of times this call occurs |
| **Line numbers** | Source line numbers where the call is made |

---

## 7. Right Panel — Table Ops Tab

Provides a comprehensive view of all table operations across the entire analysis.

### Filters

| Filter | Description |
|--------|-------------|
| **Operation filter pills** | Multi-toggle: SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE, TRUNCATE, DROP |
| **Search box** | Free-text search across all columns |
| **Column-level dropdowns** | Precise per-column filtering |

### Table Columns

| Column | Description |
|--------|-------------|
| **Table Name** | Clickable — opens the Table Detail Modal |
| **Type** | `TABLE`, `VIEW`, or `MATERIALIZED VIEW` |
| **Operations** | Operation badges (S/I/U/D/M/C/T) |
| **Count** | Total number of accesses |
| **Triggers** | Count of triggers on this table (clickable) |
| **Procedures** | Number of distinct procedures that access this table |
| **Usage** | Summary of how the table is used |

All columns are **sortable**.

### Row Expansion

Click the expand arrow on any row to reveal:

| Section | Description |
|---------|-------------|
| **Access Details** | Per-access breakdown: operation badge, procedure name, line number, WHERE clause preview |
| **Tables via Triggers** | Tables indirectly affected through triggers |
| **Claude verification badges** | Verification status per access (if Claude enrichment run) |

---

## 8. Right Panel — Details Tab

Contains four sub-tabs: **Sequences**, **Join Summary**, **Dynamic SQL**, and **Transactions**.

> **Facet completeness varies by dialect.** Joins/predicates are populated for all five dialects via the shared `TableOperationEnricher`. Sequences and exception handlers are solid for Oracle, SQL Server, DB2, and Postgres; MySQL has exception handlers but no native sequence concept (pre-8.0). See `docs/audit-findings-2026-09-13.md` for the definitive, dialect-by-dialect status if a facet looks unexpectedly empty for a given dialect.

### Sequences Sub-Tab

Lists all Oracle sequences referenced in the analysis.

| Column | Description |
|--------|-------------|
| **Sequence Name** | Name of the sequence |
| **Schema** | Color-coded schema badge |
| **Operations** | Badges: `NEXTVAL` and/or `CURRVAL` |
| **Usage Count** | Total number of references |
| **Procedure Count** | Number of distinct procedures that reference this sequence |

**Row expansion:** Shows individual usage — operation, procedure, line number.

### Join Summary Sub-Tab

Lists all table joins discovered across the analysis (all five dialects).

Both sides of every join are **resolved from the aliases used in the `ON` condition**, not defaulted to the FROM base table. In a chain such as `A JOIN B ON … JOIN C ON c.x = b.y`, the `C` join is correctly recorded against **B** (its real left side), and every explicit join — including the first one after `FROM` — is captured with its `joinType` and `ON` condition. Joins are grouped by the **originating query/statement** (a `queryId` carried end-to-end), so joins from different SELECTs in the same procedure are not mixed and per-query counts are accurate.

| Column | Description |
|--------|-------------|
| **Left Table** | Left-hand table, resolved from the `ON`-condition alias (not the FROM base) |
| **Right Table** | Right-hand (newly joined) table |
| **Join Types** | Colored badges for each join type used |
| **Usage Count** | Number of times this join pair appears |
| **ON Predicate** | Preview of the join condition |

A **Query view** groups joins per originating SELECT statement, showing the correct join count and table count for each query, and **View Full Query** renders the original statement text with whitespace preserved.

Join type badge colors:

| Badge | Color |
|-------|-------|
| `INNER` | Blue |
| `LEFT` | Green |
| `RIGHT` | Orange |
| `CROSS` | Red |
| `FULL` | Purple |

**Row expansion:** Shows each individual occurrence — type, procedure, line, and a **Show SQL** button to view the full statement.

---

## 9. Right Panel — Summary Tab

Contains three sub-tabs: **Dashboard**, **Claude Insights**, and **Claude Corrections**.

### Dashboard Sub-Tab

**Charts:**

| Chart | Description |
|-------|-------------|
| **Object Types** | Distribution of object types (packages, procedures, functions, etc.) |
| **Schemas** | Distribution of objects across schemas |

**Highlights:**

| Highlight | Description |
|-----------|-------------|
| **Most LOC** | Procedure with highest line count (clickable) |
| **Deepest** | Procedure at maximum call depth (clickable) |
| **Most Tables** | Procedure that references the most tables (clickable) |
| **Most Calls** | Procedure that makes the most outgoing calls (clickable) |

### Claude Insights Sub-Tab

Displays AI-generated insights after a Claude enrichment session: natural-language summaries, risk assessments, and architectural observations.

### Claude Corrections Sub-Tab

Side-by-side comparison of static parser results vs Claude-verified results. Shows where Claude agreed, corrected, or discovered new information.

---

## 10. Right Panel — Complexity Tab

Provides a risk-scored view of every procedure in the analysis.

### Risk Filter Pills

| Level | Color | Description |
|-------|-------|-------------|
| **Low** | Green | Low complexity, minimal risk |
| **Medium** | Orange | Moderate complexity, review recommended |
| **High** | Red | High complexity, requires careful review |

### Risk Summary Cards

One card per risk level showing count, percentage, and clickable to filter the table.

### Table Columns

| Column | Description |
|--------|-------------|
| **Procedure** | Procedure name (lock icon = encrypted/wrapped) |
| **Schema** | Schema name |
| **LOC** | Lines of code |
| **Tables** | Number of tables accessed |
| **Dependencies** | Calls made + calls received |
| **Dynamic SQL** | Count of dynamic SQL statements (highlighted if > 0) |
| **Depth** | Call depth in the hierarchy |
| **Score** | Weighted complexity score |
| **Risk** | Risk level badge: **L** / **M** / **H** |

All columns are **sortable** and have **dropdown filters**.

### Row Expansion — Score Breakdown

Shows how the score was calculated across factors: LOC, Tables, Calls, Cursors, Dynamic SQL, Depth — each with its value, weight, and contribution displayed as a bar chart. The breakdown reflects the currently selected scoring algorithm and also opens the source in a popup via **View Source**.

### Configurable Scoring Algorithm (⚙ Scoring)

The **⚙ Scoring** button in the Complexity toolbar opens a configuration panel where you can tune how the score is computed:

- **Algorithm** — choose how each metric value is transformed before it is weighted:
  | Algorithm | Formula |
  |-----------|---------|
  | Linear (weighted sum) | `score = Σ(weight × value)` |
  | Logarithmic (diminishing) | `score = Σ(weight × log₂(1 + value))` |
  | Exponential (compounding) | `score = Σ(weight × ((1 + value)^p − 1))`, with a configurable exponent `p` |
  | Balanced (√ dampening) | `score = Σ(weight × √value)` |
- **Weights** — per-metric weights (LOC, Tables, Outgoing Calls, Cursors, Dynamic SQL, Call Depth).
- **Risk Thresholds** — the Medium and High cut-offs used to label each row.
- **Preview** — recomputes all scores/risks live without saving.
- **Save for this analysis** — persists an override scoped to the current analysis.
- **Save as default (all)** — persists a default applied to every analysis.
- **Reset to built-in** — clears overrides and restores the server defaults.

Effective configuration resolves as **per-analysis override → default-for-all override → built-in server defaults**. Overrides are stored locally in the browser, so changes apply immediately without restarting the application.

---

## 11. Right Panel — Graph Tab

Renders a visual call graph diagram.

- **Nodes** represent procedures and functions
- **Directed edges** represent calls from one procedure to another
- **Interactive:** Nodes can be dragged; view can be panned and zoomed

---

## 12. Right Panel — Source Tab

A full-featured source code viewer with PL/SQL syntax highlighting embedded directly in the analysis screen.

### Main Viewer

| Feature | Description |
|---------|-------------|
| **Syntax highlighting** | PL/SQL keywords, strings, comments, numbers color-coded |
| **Line numbers** | Clickable gutter on the left |
| **Focused view** | When a procedure has known line boundaries, shows only that procedure's body with a "Show Full Source" button |
| **Truncation warning** | Orange warning shown when source was truncated (Oracle ALL_SOURCE issue) |
| **Find** | In-source search (Ctrl+F); match count + Previous/Next navigation |
| **Copy Proc** | Copy only the focused procedure's source lines to clipboard |
| **Copy All** | Copy the entire source file to clipboard |
| **Clickable call links** | Procedure names on call lines are underlined/highlighted — click to open that procedure's source in the modal |

### Detail Bar

Shown above the source, summarizing the currently viewed procedure: type icon, full qualified name, package name, LOC, table count, call count, cursor count, depth.

### Sidebar Panels

The source tab has a right sidebar with collapsible sections, each independently scrollable (vertical) with horizontal scroll for long names:

| Panel | Description |
|-------|-------------|
| **Parameters** | Declared parameters: mode (IN/OUT/IN OUT), name, type, line. Click to highlight line in source. |
| **Variables** | Locally declared variables: name, type, line. Click to highlight line. |
| **Tables** | Tables accessed by this procedure. Click to open Table Detail Modal. |
| **Calls Made** | Procedures/functions called by this procedure. Click to open in source modal. |
| **Called By** | Procedures that call this procedure. Click to open in source modal. |

Each section has a **filter box** to search within that panel.

---

## 13. Source Code Modal

The **Source Code Modal** opens when you click a procedure name in the sidebar panels or a clickable call link in the source. It provides full source viewing in an overlay without leaving the current tab.

### Modal Header

| Element | Description |
|---------|-------------|
| **Title** | Procedure name and line range (e.g., `FN_CALC_NCD (Lines 45-120)`) |
| **Back / Forward** | Navigate history of opened procedures |
| **Search** | Search within modal source (Ctrl+F) |
| **Fullscreen** | Toggle fullscreen view |
| **Copy** | Copy visible source to clipboard |
| **Close** | Press Escape or click the overlay |

### Info Bar

Shows type badge (PROCEDURE / FUNCTION / TRIGGER), scope badge (INTERNAL / EXTERNAL), fully qualified name, and line count.

### Breadcrumb

When you navigate from one procedure to another within the modal, a breadcrumb trail shows your path. Click any breadcrumb item to jump back to that point.

### Sidebar Panels

Same panels as the Source Tab sidebar (Parameters, Variables, Tables, Cursors, Calls Made, Called By, Claude) — each independently scrollable.

### Clickable Call Links

Procedure names on lines that are known call sites are rendered as clickable underlined links. Clicking them opens that procedure in the modal and pushes it onto the navigation history.

---

## 14. Table Detail Modal

Opens when you click any table name anywhere in the application.

### Modal Header

| Element | Description |
|---------|-------------|
| **Table name** | Full name of the table |
| **Schema** | The owning schema |
| **Access count** | Total number of accesses across the analysis |
| **Join count** | Number of joins involving this table |
| **Trigger count** | Number of triggers defined on this table |
| **Query button** | Opens the Query Runner pre-filled for this table |

### Accesses Tab

| Column | Description |
|--------|-------------|
| **Operation** | DML operation: SELECT, INSERT, UPDATE, DELETE, MERGE |
| **Procedure** | Clickable — navigates to that procedure in the source viewer |
| **Line** | Clickable — jumps to the specific source line |

Operation filter pills at the top. Claude verification column appears if enrichment has been run.

### Columns Tab

Displays the table's column metadata from the database.

| Column | Description |
|--------|-------------|
| **#** | Column position |
| **Column Name** | Name (PK icon for primary key columns) |
| **Data Type** | Formatted data type (e.g., `VARCHAR2(100)`, `NUMBER(10,2)`) |
| **Nullable** | `YES` or `NOT NULL` |
| **Default** | Default value expression, if defined |

### Indexes Tab

| Column | Description |
|--------|-------------|
| **Index Name** | Name of the index |
| **Uniqueness** | `UNIQUE` or `NONUNIQUE` |
| **Columns** | Comma-separated list of indexed columns |

### Constraints Tab

| Column | Description |
|--------|-------------|
| **Constraint Name** | Name of the constraint |
| **Type** | Constraint type badge |
| **Columns** | Columns involved |
| **References** | For foreign keys: referenced table and column(s) |

Constraint type badges:

| Badge | Type | Color |
|-------|------|-------|
| **P** | Primary Key | Blue |
| **U** | Unique | Green |
| **R** | Foreign Key | Orange |
| **C** | Check | Gray |

### Joins Tab

| Column | Description |
|--------|-------------|
| **Join Type** | Colored badge (INNER/LEFT/RIGHT/CROSS/FULL) |
| **Joined Table** | The other table in the join |
| **Alias** | Table alias used in the SQL |
| **Condition** | The ON clause |
| **Procedure** | Clickable — navigates to the procedure |
| **Line** | Clickable — jumps to source line |

### Triggers Tab

| Column | Description |
|--------|-------------|
| **Trigger Name** | Clickable — navigates to trigger source |
| **Timing** | `BEFORE` or `AFTER` |
| **Event** | `INSERT`, `UPDATE`, `DELETE`, or compound |
| **Type** | Row-level or statement-level |
| **Source** | **DB** (database metadata) or **SRC** (parsed source) |

### Claude Tab

Available only after Claude enrichment. Shows Claude's verification status per access:

| Metric | Description |
|--------|-------------|
| **Confirmed** | Accesses Claude verified as correct |
| **Removed** | Accesses Claude determined are false positives |
| **New** | Accesses Claude discovered that the static parser missed |
| **Unverified** | Accesses not yet reviewed by Claude |

---

## 15. Query Runner

Opens from the **Query** button in the Table Detail Modal header.

| Control | Description |
|---------|-------------|
| **SQL textarea** | Pre-filled with `SELECT * FROM <table> WHERE ROWNUM <= 20` |
| **Run button** | Execute the query (keyboard: **Ctrl+Enter**) |
| **Clear button** | Clear textarea and results |
| **Max rows dropdown** | Limit returned rows: 20, 50, 100, 200, 500 |

Results show row number, all data columns, NULL highlighting, and a truncation warning when the row limit is hit.

---

## 16. Export Modal

Opens from the **Export** button in the top bar.

### Format

| Format | Description |
|--------|-------------|
| **JSON** | Structured JSON suitable for programmatic use |
| **CSV** | Flat comma-separated file for spreadsheets |
| **Excel (.xlsx)** | Multi-sheet workbook with full styling via Apache POI |

### Scope

| Scope | Description |
|-------|-------------|
| **Full** | All procedures in the analysis |
| **Subtree** | Selected procedure and its direct children |
| **SubFlow** | Selected procedure and all descendants |
| **Direct** | Selected procedure only |

### Exportable Sections

| Section | Description |
|---------|-------------|
| **Procedures** | All discovered procedures with metadata (LOC, depth, type, schema) |
| **Table Operations** | All table access records (table name, operation, procedure, line) |
| **Joins** | All join relationships (left/right table, join type, condition, procedure) |
| **Cursors** | All cursor declarations (name, query, procedure) |
| **Sequences** | All sequence references (name, operation, procedure) |
| **Call Graph** | Full procedure-to-procedure call graph (caller, callee, call count, line) |
| **Summary Stats** | Aggregated statistics: total procs, tables, LOC, depth, errors |

Click **Download** to generate and save the file.

---

## 17. Connection Manager Modal

Opens from the **gear button** on the Home Screen.

### Layout

| Column | Description |
|--------|-------------|
| **Projects list** | Left column. Lists all configured projects. Includes **Add Project** button. |
| **Environments panel** | Middle column. Shows all environments for the selected project. |
| **Detail panel** | Right column. Shows and edits connection details for the selected environment. |

### Connection Fields

| Field | Description |
|-------|-------------|
| **Name** | Display name for this connection |
| **Description** | Optional description |
| **Username** | Oracle schema username |
| **Password** | Database password (masked) |
| **Hostname** | Oracle DB host |
| **Port** | Oracle listener port (default 1521) |
| **Service Name / SID** | Oracle service name (recommended) or SID |
| **Connection Type** | `SERVICE_NAME` or `SID` |
| **Role** | `DEFAULT`, `SYSDBA`, or `SYSOPER` |
| **JDBC URL** | Auto-constructed or manually overridden |
| **Test Connection** | Verifies credentials and connectivity |

---

## 18. Scope Controls

**Scope Controls** determine which portion of the analysis is shown in the right panel.

| Scope | Description |
|-------|-------------|
| **Direct** | Show data for the currently selected procedure only |
| **Subtree** | Show data for the selected procedure and its direct children |
| **SubFlow** | Show data for the selected procedure and all descendants (all levels) |
| **Full** | Show data for the entire analysis regardless of selection |

Changing scope updates Table Ops, Details, Complexity, and all other right-panel tabs.

---

## 19. Chat

| Element | Description |
|---------|-------------|
| **Floating FAB** | Floating action button at bottom-right corner |
| **Classic chat panel** | Opens a chat panel for conversational questions about the analysis |
| **Topbar chat toggle** | Additional toggle in the top bar actions area |

Use Chat to ask natural language questions about procedure behavior, table relationships, or code patterns in the current analysis.

---

## 20. Understanding Output Files

All analysis output is stored under `data/plsql-parse/{ANALYSIS_NAME}/`. This directory is self-contained and portable.

```
data/plsql-parse/{ANALYSIS_NAME}/
├── api/
│   ├── index.json               ← Root metadata + all node summaries
│   ├── call_graph.json          ← All edges with line numbers and call counts
│   ├── node_id_map.json         ← Name → NodeId lookup
│   ├── nodes/
│   │   └── {SCHEMA$PKG$PROC}.json   ← Individual procedure detail (loaded on demand)
│   └── tables/
│       └── {TABLE_NAME}.json    ← Per-table operation details
├── sources/                      ← Downloaded source files (local cache for re-runs)
├── progress.json                ← "PACKAGE.PROC" → "DONE" | "FAILED: reason"
├── edges.json                   ← Complete raw edge list
├── flow.json                    ← Complete flow structure (large)
├── tables.json                  ← Aggregated table usage
└── chunks/                      ← Claude enrichment chunks (if run)
```

### index.json Key Fields

| Field | Description |
|-------|-------------|
| `entryPoint` | The object name you analyzed |
| `entrySchema` | Oracle schema of the entry point |
| `totalNodes` | Number of distinct procedures/functions found |
| `totalTables` | Number of distinct tables referenced |
| `totalEdges` | Total call edges (including repeated calls) |
| `uniqueEdges` | Distinct caller→callee pairs |
| `maxDepth` | Deepest level in the call tree |
| `totalLinesOfCode` | Sum of LOC across all nodes |
| `errors` | List of parse errors or warnings |
| `crawlTimeMs` | Total analysis time in milliseconds |
| `dbCallCount` | Number of DB queries made during analysis |
| `nodes` | Array of all node summaries |

### progress.json

Shows the status of every object the crawler attempted:

```json
{
  "PG_RNGE_POL_CALC.FN_CALC_NCD": "DONE",
  "PG_UTIL_LOG_ERROR.PC_INS_LOG_TRACE": "DONE",
  "CUSTOMER.SOME_PROC": "FAILED: no source found"
}
```

Check this file first when diagnosing why a procedure is missing from the results.

### Node ID Format

Node IDs follow the pattern `SCHEMA$PACKAGE$PROCEDURE` (e.g., `CUSTOMER$PG_RNGE_POL_CALC$FN_CALC_NCD`). For standalone procedures: `SCHEMA$PROCEDURE_NAME`.

---

## 21. Special Behaviors & Limits

### Truncated Source (Oracle ALL_SOURCE)

Some large packages have incomplete rows in Oracle's `ALL_SOURCE` view. The analyzer detects this by checking whether the last non-empty line ends with `/` or `END name;`. If truncated:

1. Automatically retries using `DBMS_METADATA.GET_DDL()`
2. If DBMS_METADATA also fails, a warning is added to `errors` in `index.json`
3. The procedure's `lineStart` will be 1 (meaning parser could not locate boundaries)
4. The source view shows a **⚠ Truncated source — showing full package** warning

**Resolution:** Ensure the DB user has `EXECUTE` permission on `DBMS_METADATA`.

### Wrapped / Encrypted Packages

If Oracle source contains `wrapped` keyword or hex-encoded lines:

- Object is marked `readable: false`
- No parameters, variables, or table operations extracted
- Appears with a **lock icon** in all views
- Still appears in the call tree (the analyzer knows it exists via `ALL_PROCEDURES`)
- Cannot be navigated to in the source viewer

### Circular Dependencies (Cycles)

When procedure A calls B which eventually calls A again:

- Both nodes are flagged `inCycle: true`
- The cycle is reported in `index.json` under `cycles`
- A **cycle** badge appears in the Hierarchy view
- The crawler does not re-traverse already-visited nodes, so no infinite loop occurs

### Depth Limit

By default there is no depth limit. Very deep call trees (depth 10+) can significantly increase analysis time. If an object exceeds the timeout per entry, it is added to `errors` with a "max time exceeded" message.

### Call Count vs Unique Edges

- **totalEdges** counts every call instance including repeated calls in loops (a procedure called 500 times in a loop = 500 edges)
- **uniqueEdges** counts distinct caller→callee pairs (that same loop = 1 unique edge)
- The `count` field on each edge in `call_graph.json` shows how many times that pair occurs

### Chunked Source Rendering

For large source files (2,000+ lines), the source viewer uses **chunked rendering**: only the visible window of lines is rendered at any time. Content expands automatically as you scroll. This prevents browser hangs on packages with thousands of lines.

### Large Analyses

Analyses with 100+ nodes may take several minutes. The progress bar and `progress.json` file update continuously so you can monitor progress. Do not close the browser tab while analysis is running — use the **Logs** button to confirm activity.

---

## 22. Tips & Best Practices

### Connection Setup

- Add one connection per Oracle schema per environment
- Use **Service Name** (not SID) for better resilience to DB failover
- Always click **Test Connection** before running analysis to confirm credentials
- If a schema's objects are inaccessible, check that the user has `SELECT ON ALL_SOURCE` privilege

### Entry Point Specification

| Goal | How to specify |
|------|----------------|
| Analyze one specific function in a package | `PKG_NAME.FUNCTION_NAME` |
| Analyze the entire package | `PKG_NAME` + type `PACKAGE` |
| Analyze a standalone procedure | `PROCEDURE_NAME` |
| Analyze across multiple entry points | Run separate analyses and compare history |

- Always trim trailing whitespace from the object name (the backend auto-trims, but be aware)
- Names are case-insensitive — `fn_calc_ncd` and `FN_CALC_NCD` both work

### Choosing the Right Owner

- If you are certain of the schema, select it — avoids cross-schema ambiguity
- If the procedure is part of a package that spans schemas (e.g., `CUSTOMER` package calls `OPUS_CORE` objects), add both schemas to the environment's connections
- "Auto-detect" uses the **first schema** in `ALL_SOURCE` that has a matching object — this can be unpredictable if multiple schemas have identically named packages

### Reading the Hierarchy

- **Depth 0** = your entry point
- **Depth 1** = procedures directly called by your entry point
- **High depth** (6+) means complex nested calling chains — trace back using the breadcrumb in the source modal
- **callCount** on a node shows how many times it is called from its immediate parent; high counts (100+) usually indicate a loop

### Working with Large Analyses

- Use the **Scope Controls** (Direct / Subtree / SubFlow / Full) to narrow down what is shown — "Full" on a 200-node analysis can be slow to render
- Use the **filter box** in the left panel to find a specific procedure quickly
- Export to CSV/JSON for offline analysis of large data sets
- The **Complexity tab** is the fastest way to identify the highest-risk procedures

### Interpreting Errors

| Error message | Meaning | Action |
|--------------|---------|--------|
| `Source appears TRUNCATED` | ALL_SOURCE returned incomplete source | Ensure DBMS_METADATA privilege; re-analyze |
| `FAILED: no source found` | Object not found in any configured schema | Check schema, check object name spelling |
| `FAILED: max time exceeded` | Analysis of one object took too long | May indicate very deep recursion or DB slowness |
| `WRAPPED/ENCRYPTED` | Source is wrapped — cannot be parsed | Ask DBA for unwrapped source |

### Re-analyzing After Code Changes

1. Click **Pull** in the top bar — this clears the cached source and re-fetches from the database
2. Alternatively, delete the `data/plsql-parse/{ANALYSIS_NAME}/` folder and re-analyze from the home screen
3. The `sources/` subfolder acts as a local cache — deleting it forces fresh DB fetches

### Claude AI Enrichment

- Run Claude enrichment **after** static analysis completes (not during)
- Start with a small scope (one procedure) to test before enriching the full analysis
- Claude results persist in `chunks/` and can be reloaded without re-running
- Use **Claude Corrections** tab to review and accept/reject AI findings before trusting them

### Source Navigation Tips

- In the **Source Tab**, procedure names on call lines are **clickable** — they open the called procedure in the Source Modal
- Use **Ctrl+F** in both the Source Tab and Source Modal to search within code
- Click any **line number** in the sidebar panels (Parameters, Variables, etc.) to jump to that line in the source
- Use **Back/Forward** arrows in the Source Modal to retrace your navigation path through the call chain
- **Breadcrumbs** in the Source Modal show your full navigation path — click any crumb to jump back

---

*End of User Manual*
