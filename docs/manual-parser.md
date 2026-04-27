# PL/SQL Parse Analyzer -- Complete User Manual

> **Version:** 1.0  
> **Last Updated:** 2026-04-27  
> **Application Port:** 8083

---

## Table of Contents

1. [Home Screen](#1-home-screen)
   - [Step 1 -- Connection](#step-1--connection)
   - [Step 2 -- Owner](#step-2--owner)
   - [Step 3 -- Target and Analysis](#step-3--target-and-analysis)
   - [Progress Bar](#progress-bar)
   - [Manage Connections](#manage-connections)
   - [Logs](#logs)
   - [Analysis History Tab](#analysis-history-tab)
   - [Performance Summary Tab](#performance-summary-tab)
2. [Analysis Screen Layout](#2-analysis-screen-layout)
   - [Top Bar](#top-bar)
3. [Left Panel](#3-left-panel)
   - [Call Flow Tab](#call-flow-tab)
   - [Tables Tab](#tables-tab)
4. [Right Panel -- Explore Tab](#4-right-panel--explore-tab)
   - [Hierarchy Sub-Tab](#hierarchy-sub-tab)
   - [Trace Sub-Tab](#trace-sub-tab)
   - [References Sub-Tab](#references-sub-tab)
5. [Right Panel -- Table Ops Tab](#5-right-panel--table-ops-tab)
6. [Right Panel -- Details Tab](#6-right-panel--details-tab)
   - [Sequences Sub-Tab](#sequences-sub-tab)
   - [Join Summary Sub-Tab](#join-summary-sub-tab)
7. [Right Panel -- Summary Tab](#7-right-panel--summary-tab)
8. [Right Panel -- Complexity Tab](#8-right-panel--complexity-tab)
9. [Right Panel -- Graph Tab](#9-right-panel--graph-tab)
10. [Right Panel -- Source Tab](#10-right-panel--source-tab)
11. [Table Detail Modal](#11-table-detail-modal)
12. [Query Runner](#12-query-runner)
13. [Export Modal](#13-export-modal)
14. [Connection Manager Modal](#14-connection-manager-modal)
15. [Scope Controls](#15-scope-controls)
16. [Chat](#16-chat)

---

## What the Parser Engine Extracts

The ANTLR4 grammar-based engine extracts the following from every PL/SQL object in the dependency tree:

| Category | Details |
|----------|---------|
| **Flow nodes** | Calls, DML statements, conditional branches, loops, exception handlers |
| **Table operations** | SELECT / INSERT / UPDATE / DELETE / MERGE / TRUNCATE with table name, schema, WHERE predicate, line number |
| **Cursor declarations** | OPEN / FETCH / CLOSE operations with cursor names and result set types |
| **Dynamic SQL** | `EXECUTE IMMEDIATE` statements, `DBMS_SQL` usage, concatenated string patterns |
| **Exception handlers** | Exception name, handler body, re-raise detection |
| **JOIN expressions** | Left/right table, join type (INNER/LEFT/RIGHT/FULL/CROSS), ON predicate, line number |
| **Sequence references** | `NEXTVAL` / `CURRVAL` usage with sequence name and line number |
| **Variable declarations** | Name, type, initial value, scope |
| **Parameters** | Name, type, mode (IN / OUT / IN OUT) |
| **Subprograms** | Nested procedures and functions inside package bodies |
| **FORALL / BULK COLLECT** | Bulk DML patterns with collection variable binding |
| **Non-parseable types** | TYPE, SYNONYM, SEQUENCE, JAVA CLASS — detected and skipped gracefully |

Dependency crawling uses BFS traversal with configurable depth and timeout limits. Schema resolution is cached to `data/cache-plsql/*.tsv` for faster repeated analysis.

---

## CLI Mode (FlowAnalysisMain)

The parser engine can also be invoked directly from the command line without the web UI:

```bash
java -cp unified-web.jar com.plsql.parser.flow.FlowAnalysisMain \
  --config config/plsql-config.yaml \
  --entry "PKG_CUSTOMER.PROC_GET_ACCOUNT" \
  --entry "PKG_ORDERS.PROC_CREATE_ORDER" \
  --output-dir /tmp/analysis-output \
  --max-depth 30 \
  --pretty
```

| Argument | Required | Description |
|----------|----------|-------------|
| `--config <path>` | Yes | Path to `plsql-config.yaml` containing Oracle connection details |
| `--entry "SCHEMA.PKG.PROC"` | Yes | Entry point(s) to analyze. Repeat `--entry` for multiple entry points |
| `--output-dir <dir>` | No | Output directory for chunked JSON files (defaults to current directory) |
| `--max-depth <N>` | No | Maximum BFS recursion depth; `-1` = unlimited |
| `--pretty` | No | Pretty-print JSON output |
| `--clear-cache` | No | Delete cached schema resolver TSV files before running |

Supported entry-point name formats:
- `PKG.PROC` — package-qualified
- `SCHEMA.PKG.PROC` — fully qualified
- `PROCEDURE_NAME` — standalone

---

## 1. Home Screen

The Home Screen is the entry point for every analysis session. It guides you through a three-step process to select a database connection and target object.

### Step 1 -- Connection

| Control             | Description                                                                                      |
|---------------------|--------------------------------------------------------------------------------------------------|
| **Project dropdown**    | Select the project that contains your target database. Projects are configured in `plsql-config.yaml`. |
| **Environment dropdown** | Select the environment within the chosen project (e.g., DEV, UAT, PROD). Each environment maps to a specific Oracle JDBC connection defined in `plsql-config.yaml`. |

Both dropdowns must be set before proceeding. The available environments update dynamically based on the selected project.

### Step 2 -- Owner

| Control              | Description                                                                                  |
|----------------------|----------------------------------------------------------------------------------------------|
| **Owner dropdown**       | Optional. Select a specific schema owner to narrow the analysis scope.                       |
| **Auto-detect (all schemas)** | Default option. The analyzer will resolve the object across all accessible schemas automatically. |

When a specific owner is selected, the analyzer queries only that schema. Using auto-detect is recommended when you are unsure which schema owns the target object.

### Step 3 -- Target and Analysis

| Control              | Description                                                                                                        |
|----------------------|--------------------------------------------------------------------------------------------------------------------|
| **Object name input**    | Enter the name of the PL/SQL object to analyze. Accepts simple names like `PKG_CUSTOMER` or qualified names like `PKG.PROC_NAME`. |
| **Object type dropdown** | Select the type of the target object.                                                                              |
| **Analyze button**       | Starts the analysis. Disabled until connection and object name are provided.                                        |

Supported object types:

| Type          | Description                          |
|---------------|--------------------------------------|
| `PACKAGE`     | PL/SQL package (spec + body)         |
| `PROCEDURE`   | Standalone stored procedure          |
| `FUNCTION`    | Standalone stored function           |
| `TRIGGER`     | Database trigger                     |

### Progress Bar

Once the **Analyze** button is clicked, a progress bar appears showing the current phase:

| Phase                      | Description                                                    |
|----------------------------|----------------------------------------------------------------|
| **Starting**               | Initializing the analysis session.                             |
| **Resolving dependencies** | Walking the dependency tree to discover all referenced objects. |
| **Downloading source**     | Retrieving PL/SQL source code from the database.               |
| **Parsing**                | Running the grammar-based parser over all source files.         |
| **Building output**        | Assembling the final analysis model (calls, tables, graphs).   |
| **Complete**               | Analysis finished. The screen transitions to the Analysis view. |

### Manage Connections

The **gear button** next to the connection dropdowns opens the Connection Manager modal. See [Connection Manager Modal](#14-connection-manager-modal) for full details.

### Schema Cache

The parser caches Oracle schema resolver query results to `data/cache-plsql/*.tsv` to speed up repeated analyses. If the database schema has changed significantly (new synonyms, relocated objects), you can force a fresh resolve by passing `--clear-cache` via the CLI mode or using the **Clear Cache** option in the UI settings panel.

### Logs

The **Logs button** opens a log viewer window that displays real-time server-side log output. Useful for debugging connection issues, parser errors, or performance problems.

### Analysis History Tab

Located below the connection controls, the **Analysis History** tab lists all previously completed analyses.

| Column     | Description                                    |
|------------|------------------------------------------------|
| **Name**   | The analyzed object name.                      |
| **Date**   | Timestamp of when the analysis was performed.  |
| **Procs**  | Total number of procedures discovered.         |
| **Tables** | Total number of tables referenced.             |
| **LOC**    | Total lines of code across all procedures.     |

- Click any row to **reload** that analysis into the Analysis Screen without re-running it.
- A **search box** at the top of the list lets you filter history entries by name or date.

### Performance Summary Tab

The **Performance Summary** tab (next to Analysis History) shows timing breakdowns for past analysis runs. Use this to identify slow phases or compare performance across different target objects and environments.

---

## 2. Analysis Screen Layout

After an analysis completes (or a history entry is loaded), the screen transitions to the Analysis Screen. This is a two-panel layout with a top bar.

### Top Bar

The top bar spans the full width and contains:

| Element                        | Description                                                                                                  |
|--------------------------------|--------------------------------------------------------------------------------------------------------------|
| **Analysis name**              | The name of the currently loaded analysis (object name + environment).                                       |
| **Stats row**                  | Quick summary metrics displayed inline.                                                                      |
| **Claude mode toggle**         | Switch between `Static` (parser-only results) and `Claude` (AI-enriched results).                            |
| **Action buttons**             | Pull, Refresh, Search, Export, Logs.                                                                         |
| **Claude control buttons**     | Verify, Kill, Chunks, Sessions -- visible when Claude mode is active.                                        |
| **Claude status indicator**    | Shows current state of Claude enrichment (idle, running, complete, error).                                   |

**Stats displayed in the top bar:**

| Stat         | Description                                          |
|--------------|------------------------------------------------------|
| **Procs**    | Total procedures/functions discovered.               |
| **Tables**   | Total distinct tables referenced.                    |
| **Edges**    | Total call-graph edges (procedure-to-procedure).     |
| **LOC**      | Total lines of code.                                 |
| **Max Depth**| Deepest level in the call hierarchy.                 |
| **Errors**   | Number of parse errors or unresolved references.     |

**Action buttons:**

| Button       | Description                                                                                 |
|--------------|---------------------------------------------------------------------------------------------|
| **Pull**     | Re-fetch source code from the database and re-analyze (useful if code changed).             |
| **Refresh**  | Reload the current analysis from the server cache without re-fetching source.               |
| **Search**   | Opens a global search dialog to find procedures, tables, or code across the analysis.       |
| **Export**   | Opens the Export modal. See [Export Modal](#13-export-modal).                                |
| **Logs**     | Opens the log viewer.                                                                       |

**Claude control buttons (visible in Claude mode):**

| Button        | Description                                                              |
|---------------|--------------------------------------------------------------------------|
| **Verify**    | Trigger Claude AI verification of the current analysis.                  |
| **Kill**      | Abort a running Claude enrichment session.                               |
| **Chunks**    | View the chunk breakdown used for Claude API calls.                      |
| **Sessions**  | View past Claude enrichment sessions and their results.                  |

---

## 3. Left Panel

The left panel provides navigation across all discovered procedures and tables. It contains two tabs: **Call Flow** and **Tables**.

Both tabs feature:
- A **filter box** at the top for instant text filtering.
- **Lazy-loaded batches** of 50 items. Scroll to the bottom to load the next batch automatically.

### Call Flow Tab

Lists all procedures discovered during the analysis, ordered by call depth.

Each row displays:

| Element            | Description                                                                                          |
|--------------------|------------------------------------------------------------------------------------------------------|
| **Schema badge**   | Color-coded badge showing the owning schema. Each schema is assigned a consistent color.             |
| **Type icon**      | Single-letter icon indicating the object type: **P** = Procedure, **F** = Function, **T** = Trigger. |
| **Procedure name** | Full name of the procedure or function.                                                              |
| **Depth badge**    | Numeric badge showing the call depth (0 = entry point, 1 = direct call, etc.).                       |
| **LOC badge**      | Lines of code for this specific procedure.                                                           |

**Click** any row to load that procedure's details into the right panel.

### Tables Tab

Lists all tables referenced anywhere in the analysis.

Each row displays:

| Element              | Description                                                                                                             |
|----------------------|-------------------------------------------------------------------------------------------------------------------------|
| **Table name**       | Color-coded by schema.                                                                                                  |
| **Operation badges** | One or more single-letter badges indicating the types of operations performed on this table.                            |
| **Access count**     | Total number of times this table is accessed across all procedures.                                                      |

Operation badge legend:

| Badge | Operation    |
|-------|-------------|
| **S** | SELECT       |
| **I** | INSERT       |
| **U** | UPDATE       |
| **D** | DELETE       |
| **M** | MERGE        |
| **C** | CREATE       |
| **T** | TRUNCATE     |

**Click** any table row to open the [Table Detail Modal](#11-table-detail-modal).

---

## 4. Right Panel -- Explore Tab

The **Explore** tab is the first tab in the right panel and contains three sub-tabs: **Hierarchy**, **Trace**, and **References**.

### Hierarchy Sub-Tab

The default view. Displays a depth-indented call tree with expand/collapse toggles for each node that has children.

Each row contains:

| Element                | Description                                                                                   |
|------------------------|-----------------------------------------------------------------------------------------------|
| **Depth indicator**    | Label showing the level: L0, L1, L2, etc.                                                    |
| **Step counter**       | Sequential step number in the execution flow.                                                 |
| **Schema badge**       | Color-coded schema identifier.                                                                |
| **Procedure name**     | Clickable -- navigates to the source view for that procedure.                                 |
| **Source line link**   | Clickable line number that jumps directly to the calling line in the source viewer.            |
| **Call type badge**    | Categorization of the call.                                                                   |
| **LOC**                | Lines of code for this procedure.                                                             |
| **Complexity risk**    | Risk level indicator: **L** (Low), **M** (Medium), **H** (High).                             |

Call type badges:

| Badge        | Meaning                                                                |
|--------------|------------------------------------------------------------------------|
| `INTERNAL`   | Call to a procedure within the same package or analysis scope.         |
| `EXTERNAL`   | Call to a procedure outside the current package/scope.                 |
| `TRIGGER`    | Invocation through a database trigger.                                 |
| `DYNAMIC`    | Call constructed and executed via dynamic SQL (EXECUTE IMMEDIATE, etc.).|

**Toolbar actions:**

| Action              | Description                                          |
|---------------------|------------------------------------------------------|
| **Expand All**      | Expand every node in the tree.                       |
| **Collapse All**    | Collapse all nodes to show only the root level.      |
| **Breadcrumb nav**  | Shows the path to the currently focused node. Click any breadcrumb to jump back. |
| **Search**          | Filter the tree by procedure name.                   |

### Trace Sub-Tab

A flat, non-hierarchical list of every procedure in the analysis. Displays the same columns as the Hierarchy sub-tab but without indentation or expand/collapse controls.

- **Paginated:** Displays 200 items per batch.
- **"Show more" button:** Click to load the next batch of 200.

### References Sub-Tab

Shows what the currently selected procedure calls. Contains a **Calls** section.

| Element              | Description                                                        |
|----------------------|--------------------------------------------------------------------|
| **Scope filter pills** | Toggle between **INT** (internal calls) and **EXT** (external calls). |

Columns:

| Column         | Description                                                  |
|----------------|--------------------------------------------------------------|
| **Name**       | Name of the called procedure.                                |
| **Call type**  | INTERNAL or EXTERNAL.                                        |
| **Count**      | Number of times this call occurs.                            |
| **Line numbers** | List of source line numbers where the call is made.        |

---

## 5. Right Panel -- Table Ops Tab

The **Table Ops** tab provides a comprehensive view of all table operations across the entire analysis.

### Filters

| Filter                   | Description                                                                      |
|--------------------------|----------------------------------------------------------------------------------|
| **Operation filter pills** | Multi-toggle buttons: SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE, TRUNCATE, DROP. Click one or more to filter the table to only those operation types. |
| **Search box**           | Free-text search across all columns.                                             |
| **Column-level dropdowns** | Each column header has a dropdown filter for precise filtering.                 |

### Table Columns

| Column          | Description                                                                                                       |
|-----------------|-------------------------------------------------------------------------------------------------------------------|
| **Table Name**  | Clickable -- opens the [Table Detail Modal](#11-table-detail-modal).                                              |
| **Type**        | The object type: `TABLE`, `VIEW`, or `MATERIALIZED VIEW`.                                                         |
| **Operations**  | Row of operation badges (S/I/U/D/M/C/T) showing which operations are performed on this table.                     |
| **Count**       | Total number of accesses.                                                                                         |
| **Triggers**    | Count of triggers on this table. Clickable -- shows trigger details.                                              |
| **Procedures**  | Number of distinct procedures that access this table.                                                             |
| **Usage**       | Summary of how the table is used in the analysis.                                                                 |

All columns are **sortable** by clicking the column header.

### Row Expansion

Click the expand arrow on any row to reveal additional details:

| Section                    | Description                                                                                              |
|----------------------------|----------------------------------------------------------------------------------------------------------|
| **Access Details**         | Per-access breakdown showing: operation badge, procedure name (clickable), line number (clickable to source), WHERE clause text. |
| **Tables via Triggers**    | Lists any tables that are indirectly affected through triggers on this table.                             |
| **Claude verification badges** | If Claude enrichment has been run, shows verification status per access (confirmed, removed, new, unverified). |

---

## 6. Right Panel -- Details Tab

The **Details** tab contains two sub-tabs: **Sequences** and **Join Summary**.

### Sequences Sub-Tab

Lists all Oracle sequences referenced in the analysis.

**Columns:**

| Column              | Description                                                          |
|---------------------|----------------------------------------------------------------------|
| **Sequence Name**   | Name of the sequence.                                                |
| **Schema**          | Color-coded schema badge.                                            |
| **Operations**      | Badges showing usage type: `NEXTVAL` and/or `CURRVAL`.              |
| **Usage Count**     | Total number of references to this sequence.                         |
| **Procedure Count** | Number of distinct procedures that reference this sequence.          |

**Filters:**
- **Operation pills:** Toggle NEXTVAL / CURRVAL to filter.
- **Column-level filters:** Dropdown filters on each column header.

All columns are **sortable**.

**Row expansion:** Expands to show individual usage details:

| Detail         | Description                                           |
|----------------|-------------------------------------------------------|
| **Operation**  | NEXTVAL or CURRVAL.                                   |
| **Procedure**  | The procedure that references the sequence.           |
| **Line number**| Source line where the reference occurs.                |

### Join Summary Sub-Tab

Lists all table joins discovered across the analysis.

**Columns:**

| Column              | Description                                                                    |
|---------------------|--------------------------------------------------------------------------------|
| **Left Table**      | The left-hand table in the join.                                               |
| **Right Table**     | The right-hand table in the join.                                              |
| **Join Types**      | Colored badges for each join type used between these two tables.               |
| **Usage Count**     | Number of times this join pair appears.                                        |
| **ON Predicate**    | Preview of the join condition (truncated for long predicates).                 |

Join type badge colors:

| Badge       | Color   |
|-------------|---------|
| `INNER`     | Blue    |
| `LEFT`      | Green   |
| `RIGHT`     | Orange  |
| `CROSS`     | Red     |
| `FULL`      | Purple  |

**Filters:**
- **Join type pills:** Toggle one or more join types.
- **Search box:** Free-text search.
- **Column-level filters:** Dropdown filters on each column header.

All columns are **sortable**.

**Row expansion:** Shows each individual occurrence of this join:

| Detail             | Description                                                        |
|--------------------|--------------------------------------------------------------------|
| **Type badge**     | The specific join type for this occurrence.                        |
| **Procedure**      | The procedure containing this join.                                |
| **Line**           | Source line number.                                                |
| **"Show SQL" button** | Click to view the full SQL statement containing this join.      |

---

## 7. Right Panel -- Summary Tab

The **Summary** tab contains three sub-tabs: **Dashboard**, **Claude Insights**, and **Claude Corrections**.

### Dashboard Sub-Tab

Provides a visual overview of the analysis.

**Charts:**

| Chart               | Description                                                   |
|----------------------|---------------------------------------------------------------|
| **Object Types**     | Distribution of object types (packages, procedures, etc.).    |
| **Schemas**          | Distribution of objects across schemas.                       |

**Highlights section:**

| Highlight         | Description                                                                       |
|-------------------|-----------------------------------------------------------------------------------|
| **Most LOC**      | The procedure with the highest line count. Clickable -- navigates to that procedure. |
| **Deepest**       | The procedure at the maximum call depth. Clickable.                               |
| **Most Tables**   | The procedure that references the most tables. Clickable.                         |
| **Most Calls**    | The procedure that makes the most outgoing calls. Clickable.                      |

### Claude Insights Sub-Tab

Displays AI-generated insights after a Claude enrichment session has been completed. Content includes natural-language summaries, risk assessments, and architectural observations produced by Claude.

### Claude Corrections Sub-Tab

Shows a side-by-side comparison of static parser results versus Claude-verified results. Highlights where Claude agreed with the parser, where it corrected mistakes, and where it discovered additional information.

---

## 8. Right Panel -- Complexity Tab

The **Complexity** tab provides a risk-scored view of every procedure in the analysis.

### Risk Filter Pills

Three toggle buttons at the top:

| Level      | Color   | Description                              |
|------------|---------|------------------------------------------|
| **Low**    | Green   | Low complexity, minimal risk.            |
| **Medium** | Orange  | Moderate complexity, review recommended. |
| **High**   | Red     | High complexity, requires careful review.|

### Risk Summary Cards

Displayed at the top of the tab. One card per risk level showing:
- **Count** of procedures at that level.
- **Percentage** of total procedures.
- **Clickable** -- clicking a card filters the table to that risk level.

### Table Columns

| Column            | Description                                                                                                   |
|-------------------|---------------------------------------------------------------------------------------------------------------|
| **Procedure**     | Procedure name. A **lock icon** appears next to encrypted (wrapped) procedures.                               |
| **Schema**        | Schema name.                                                                                                  |
| **LOC**           | Lines of code.                                                                                                |
| **Tables**        | Number of tables accessed.                                                                                    |
| **Dependencies**  | Number of dependencies (calls made + calls received).                                                         |
| **Dynamic SQL**   | Count of dynamic SQL statements. Highlighted (bold/colored) if greater than 0.                                |
| **Depth**         | Call depth in the hierarchy.                                                                                  |
| **Score**         | Weighted complexity score (numeric).                                                                          |
| **Risk**          | Risk level badge: **L** (green), **M** (orange), **H** (red).                                                |

All columns are **sortable**. All columns have **dropdown filters**.

### Row Expansion -- Score Breakdown

Expanding a row reveals the **Score Breakdown** grid showing how the final score was calculated:

| Factor           | Value | Weight | Contribution | Bar Chart |
|------------------|-------|--------|--------------|-----------|
| **LOC**          | Actual LOC count | Weight multiplier | Weighted value | Visual bar |
| **Tables**       | Table count | Weight multiplier | Weighted value | Visual bar |
| **Calls**        | Call count | Weight multiplier | Weighted value | Visual bar |
| **Cursors**      | Cursor count | Weight multiplier | Weighted value | Visual bar |
| **Dynamic SQL**  | Dynamic SQL count | Weight multiplier | Weighted value | Visual bar |
| **Depth**        | Call depth | Weight multiplier | Weighted value | Visual bar |

---

## 9. Right Panel -- Graph Tab

The **Graph** tab renders a visual call graph diagram.

- **Nodes** represent procedures and functions.
- **Directed edges** represent calls from one procedure to another.
- The graph is interactive: nodes can be dragged, and the view can be panned and zoomed.

---

## 10. Right Panel -- Source Tab

The **Source** tab is a full-featured source code viewer with syntax highlighting for PL/SQL.

### Main Viewer

| Feature                  | Description                                                              |
|--------------------------|--------------------------------------------------------------------------|
| **Syntax highlighting**  | PL/SQL keywords, strings, comments, and identifiers are color-coded.     |
| **Line numbers**         | Displayed in a gutter on the left. Clickable for navigation/reference.   |
| **Search in source**     | Search bar with match count display, **Previous** and **Next** navigation buttons to cycle through matches. |

### Navigation

| Control                | Description                                               |
|------------------------|-----------------------------------------------------------|
| **Back / Forward**     | Navigate through your source viewing history.             |
| **Copy**               | Copy the entire source code to the clipboard.             |
| **Fullscreen toggle**  | Expand the source viewer to fill the entire screen.       |

### Sidebar Panels

The source viewer includes collapsible sidebar panels that provide contextual information about the currently viewed procedure:

| Panel          | Description                                                                                      |
|----------------|--------------------------------------------------------------------------------------------------|
| **Parameters** | Lists all declared parameters with their types and modes (IN/OUT/IN OUT). Filterable.            |
| **Variables**  | Lists all locally declared variables and their types. Filterable.                                |
| **Tables**     | Lists all tables accessed by this procedure. Filterable. Clickable to open Table Detail Modal.   |
| **Cursors**    | Lists all cursor declarations. Filterable.                                                       |
| **Calls Made** | Lists all procedures/functions called by this procedure. Filterable. Clickable to navigate.      |
| **Called By**   | Lists all procedures/functions that call this procedure. Filterable. Clickable to navigate.      |

---

## 11. Table Detail Modal

The **Table Detail Modal** opens when you click any table name anywhere in the application (left panel Tables tab, Table Ops tab, Source sidebar, etc.).

### Modal Header

| Element              | Description                                                        |
|----------------------|--------------------------------------------------------------------|
| **Table name**       | Full name of the table.                                            |
| **Schema**           | The owning schema.                                                 |
| **Access count**     | Total number of accesses across the analysis.                      |
| **Join count**       | Number of joins involving this table.                              |
| **Trigger count**    | Number of triggers defined on this table.                          |
| **Query button**     | Opens the [Query Runner](#12-query-runner) pre-filled for this table. |

The modal contains six tabs (seven if Claude enrichment is available):

### Accesses Tab

| Column         | Description                                                                       |
|----------------|-----------------------------------------------------------------------------------|
| **Operation**  | The DML operation: SELECT, INSERT, UPDATE, DELETE, or MERGE.                      |
| **Procedure**  | Clickable -- navigates to that procedure in the source viewer.                    |
| **Line**       | Clickable -- jumps to the specific line in the source viewer.                     |

- **Operation filter pills** at the top let you filter by specific operations.
- If Claude enrichment is available, a **Claude verification column** appears showing the verification status of each access.

### Columns Tab

Displays the table's column metadata as retrieved from the database.

| Column           | Description                                                              |
|------------------|--------------------------------------------------------------------------|
| **#**            | Column position (ordinal).                                               |
| **Column Name**  | Name of the column. A **PK icon** appears next to primary key columns.   |
| **Data Type**    | Formatted data type (e.g., `VARCHAR2(100)`, `NUMBER(10,2)`).            |
| **Nullable**     | `YES` if the column allows nulls, `NOT NULL` otherwise.                 |
| **Default**      | Default value expression, if defined.                                    |

All columns are **sortable** and **filterable**.

### Indexes Tab

Displays all indexes defined on the table, grouped by index name.

| Column           | Description                                            |
|------------------|--------------------------------------------------------|
| **Index Name**   | Name of the index.                                     |
| **Uniqueness**   | `UNIQUE` or `NONUNIQUE`.                               |
| **Columns**      | Comma-separated list of indexed columns.               |

All columns are **sortable** and **filterable**.

### Constraints Tab

Displays all constraints defined on the table.

| Column              | Description                                                                  |
|---------------------|------------------------------------------------------------------------------|
| **Constraint Name** | Name of the constraint.                                                      |
| **Type**            | Constraint type badge (color-coded).                                         |
| **Columns**         | The column(s) involved in the constraint.                                    |
| **References**      | For foreign keys: the referenced table and column(s).                        |

Constraint type badges:

| Badge | Type            | Color  |
|-------|-----------------|--------|
| **P** | Primary Key     | Blue   |
| **U** | Unique          | Green  |
| **R** | Foreign Key     | Orange |
| **C** | Check           | Gray   |

All columns are **sortable** and **filterable**.

### Joins Tab

Lists all joins involving this table.

| Column           | Description                                                               |
|------------------|---------------------------------------------------------------------------|
| **Join Type**    | Colored badge (INNER, LEFT, RIGHT, CROSS, FULL).                         |
| **Joined Table** | The other table in the join.                                              |
| **Alias**        | Table alias used in the SQL statement.                                    |
| **Condition**    | The ON clause or join condition.                                          |
| **Procedure**    | Clickable -- navigates to the procedure containing this join.             |
| **Line**         | Clickable -- jumps to the source line.                                    |

All columns are **sortable** and **filterable**.

### Triggers Tab

Lists all triggers defined on this table.

| Column           | Description                                                                              |
|------------------|------------------------------------------------------------------------------------------|
| **Schema badge** | Color-coded schema identifier.                                                           |
| **Trigger Name** | Clickable -- navigates to the trigger's source code.                                     |
| **Timing**       | `BEFORE` or `AFTER`.                                                                     |
| **Event**        | The DML event: `INSERT`, `UPDATE`, `DELETE`, or compound.                                |
| **Type**         | Row-level or statement-level.                                                            |
| **Source**       | Badge indicating origin: **DB** (from database metadata) or **SRC** (from parsed source). |

**Actions per row:**

| Action              | Description                                                    |
|---------------------|----------------------------------------------------------------|
| **View Source**     | Opens the trigger's PL/SQL source in the source viewer.        |
| **Definition**      | Shows the trigger's DDL definition.                            |

All columns are **sortable** and **filterable**.

### Claude Tab

Available only after Claude enrichment has been run. Displays a side-by-side verification view.

| Column             | Description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| **Operation**      | The DML operation.                                                          |
| **Procedure**      | The procedure performing the operation.                                     |
| **Line**           | Source line number.                                                         |
| **Static badge**   | What the static parser determined.                                          |
| **Claude Status**  | Claude's verification result (confirmed, corrected, new, removed).          |
| **Reason**         | Claude's explanation for any discrepancy.                                   |
| **Review buttons** | Accept or reject Claude's suggestion.                                       |

**Summary counts** at the top of the tab:

| Metric           | Description                                                   |
|------------------|---------------------------------------------------------------|
| **Confirmed**    | Accesses that Claude verified as correct.                     |
| **Removed**      | Accesses that Claude determined are false positives.          |
| **New**          | Accesses that Claude discovered but the static parser missed. |
| **Unverified**   | Accesses not yet reviewed by Claude.                          |

---

## 12. Query Runner

The **Query Runner** opens from the **Query** button in the Table Detail Modal header. It provides a lightweight SQL execution environment.

### Controls

| Control              | Description                                                                              |
|----------------------|------------------------------------------------------------------------------------------|
| **SQL textarea**     | Editable text area pre-filled with `SELECT * FROM <table> WHERE ROWNUM <= 20`.           |
| **Run button**       | Execute the query. Keyboard shortcut: **Ctrl+Enter**.                                    |
| **Clear button**     | Clear the SQL textarea and results.                                                      |
| **Max rows dropdown**| Limit the number of returned rows. Options: 20, 50, 100, 200, 500.                      |

### Results

| Element                | Description                                                                     |
|------------------------|---------------------------------------------------------------------------------|
| **Status bar**         | Shows row count returned, column count, and execution time.                     |
| **Results table**      | Columns: **#** (row number) followed by all data columns from the query result. |
| **NULL highlighting**  | NULL values are visually highlighted (typically in italic or a distinct color).  |
| **Truncation warning** | Displayed when the result set was capped by the max rows limit.                 |

---

## 13. Export Modal

The **Export Modal** opens from the **Export** button in the top bar.

### Options

| Option       | Description                                                |
|--------------|------------------------------------------------------------|
| **Format**   | Choose the export format: **JSON** or **CSV**.             |

### Exportable Sections

Select one or more sections to include in the export:

| Section             | Description                                             |
|---------------------|---------------------------------------------------------|
| **Procedures**      | All discovered procedures with their metadata.          |
| **Table Operations**| All table access records.                               |
| **Joins**           | All join relationships.                                 |
| **Cursors**         | All cursor declarations.                                |
| **Call Graph**      | The full procedure-to-procedure call graph.             |
| **Summary Stats**   | Aggregated statistics for the analysis.                 |

Click the **Download** button to generate and save the file.

---

## 14. Connection Manager Modal

The **Connection Manager Modal** opens from the **gear button** on the Home Screen. It provides a three-column layout for managing database connections.

### Layout

| Column                | Description                                                                          |
|-----------------------|--------------------------------------------------------------------------------------|
| **Projects list**     | Left column. Lists all configured projects. Includes an **Add Project** button.      |
| **Environments panel**| Middle column. Shows all environments for the selected project (e.g., DEV, UAT, PROD). |
| **Detail panel**      | Right column. Shows and edits the connection details for the selected environment.    |

### Project Management

| Action            | Description                                        |
|-------------------|----------------------------------------------------|
| **Add Project**   | Creates a new project entry.                       |
| **Select Project**| Click a project in the list to view its environments. |

### Environment Management

Each project can contain multiple environments. Per environment:

| Field                  | Description                                                                |
|------------------------|----------------------------------------------------------------------------|
| **JDBC URL**           | The Oracle JDBC connection string (e.g., `jdbc:oracle:thin:@host:port:sid`). |
| **Username**           | Database username.                                                         |
| **Password**           | Database password (masked input).                                          |
| **Test Connection**    | Button to verify that the connection parameters are valid.                 |

### Actions

| Button      | Description                                               |
|-------------|-----------------------------------------------------------|
| **Save**    | Persist changes to the project/environment configuration. |
| **Delete**  | Remove the selected project or environment.               |

---

## 15. Scope Controls

**Scope Controls** are toggle buttons that determine which portion of the analysis is reflected in the right panel views. They appear in the right panel toolbar.

| Scope        | Description                                                                                  |
|--------------|----------------------------------------------------------------------------------------------|
| **Direct**   | Show data for the currently selected procedure only.                                         |
| **Subtree**  | Show data for the selected procedure and its direct children (one level down).               |
| **SubFlow**  | Show data for the selected procedure and all descendants (recursive, all levels).            |
| **Full**     | Show data for the entire analysis regardless of which procedure is selected.                 |

Changing the scope updates all tabs in the right panel (Table Ops, Details, Complexity, etc.) to reflect only the data within the chosen scope.

---

## 16. Chat

The **Chat** feature provides an interactive conversational interface.

| Element                  | Description                                                                           |
|--------------------------|---------------------------------------------------------------------------------------|
| **Floating FAB**         | A floating action button positioned at the bottom-right corner of the screen.         |
| **Classic chat panel**   | Opens a chat panel where you can type questions and receive responses.                 |
| **Topbar chat toggle**   | An additional toggle in the top bar actions area to open/close the chat panel.         |

The chat interface allows you to ask questions about the current analysis, request explanations of specific procedures or table relationships, and interact with the tool in a conversational manner.

---

*End of User Manual*
