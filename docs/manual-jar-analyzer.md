# JAR / WAR Analyzer -- Complete User Manual

This document is the authoritative reference for every piece of UI functionality in the JAR / WAR Analyzer tool. It covers the sidebar, top bar, JAR header controls, all three main tabs (Code Structure, Endpoint Flows, Summary), export capabilities, and the AI chat system.

The analyzer accepts both `.jar` and `.war` files. It detects not just REST HTTP endpoints but also asynchronous entry points: RabbitMQ listeners, Kafka consumers, WebSocket message handlers, `@Scheduled` tasks, and Spring event listeners.

---

## Table of Contents

1. [Sidebar](#1-sidebar)
   - [JAR List](#11-jar-list)
   - [Upload Section](#12-upload-section)
   - [Claude Progress Indicator](#13-claude-progress-indicator)
2. [Top Bar](#2-top-bar)
3. [JAR Header (When a JAR Is Selected)](#3-jar-header-when-a-jar-is-selected)
4. [Tab 1: Code Structure](#4-tab-1-code-structure)
   - [Package View](#41-package-view)
   - [Project View](#42-project-view)
   - [Visual Overview](#43-visual-overview)
   - [Class Detail (Expanded)](#44-class-detail-expanded)
   - [Code Structure Toolbar](#45-code-structure-toolbar)
   - [Code Panel (Right Side)](#46-code-panel-right-side)
5. [Tab 2: Endpoint Flows](#5-tab-2-endpoint-flows)
   - [Left Pane -- Endpoint List](#51-left-pane----endpoint-list)
   - [Right Pane -- Endpoint Detail](#52-right-pane----endpoint-detail)
6. [Tab 3: Summary](#6-tab-3-summary)
   - [3.1 Endpoint Report](#61-endpoint-report)
   - [3.2 Collection Analysis](#62-collection-analysis)
   - [3.3 External Dependencies](#63-external-dependencies)
   - [3.4 Distributed Transactions](#64-distributed-transactions)
   - [3.5 Batch Jobs](#65-batch-jobs)
   - [3.6 Scheduled Jobs](#66-scheduled-jobs)
   - [3.7 Aggregation Flows](#67-aggregation-flows)
   - [3.8 Dynamic Flows](#68-dynamic-flows)
   - [3.9 Verticalisation](#69-verticalisation)
   - [3.10 Claude Insights](#610-claude-insights)
   - [3.11 Claude Corrections](#611-claude-corrections)
7. [Export](#7-export)
8. [Chat](#8-chat)

---

## 1. Sidebar

The sidebar is the persistent left panel visible at all times. It contains the list of uploaded JARs, the upload controls, and the Claude progress indicator.

### 1.1 JAR List

Each entry in the JAR list represents one uploaded and analyzed JAR file. Entries are displayed as rows with the following information:

| Element | Description |
|---|---|
| **Project / JAR Name** | The display name of the uploaded JAR. Clicking the name selects this JAR and loads its analysis into the main content area. |
| **Class Count** | Numeric badge showing the total number of Java classes found in the JAR. |
| **Endpoint Count** | Numeric badge showing the total number of entry points detected. This includes HTTP REST endpoints, RabbitMQ listeners (`@RabbitListener`), Kafka consumers (`@KafkaListener`), WebSocket handlers (`@MessageMapping`), scheduled tasks (`@Scheduled`), and Spring event listeners (`@EventListener`, `@TransactionalEventListener`). |
| **AI Badge** | Status indicator for Claude enrichment. Possible values: `IDLE` (no Claude scan started), `RUNNING` (Claude scan in progress), `COMPLETE` (Claude scan finished successfully), `FAILED` (Claude scan encountered an error). |
| **Corrected Badge** | Shows `C` when Claude corrections have been applied once, or `C(N)` (e.g., `C(2)`) when multiple correction rounds have been applied. Only visible after a Claude scan has produced corrections. |
| **Relative Date** | Displays how long ago the JAR was uploaded (e.g., "2 hours ago", "3 days ago"). |
| **Delete Button (x)** | A small "x" button at the far right of the entry. Clicking it removes the JAR and all its associated analysis data permanently. A confirmation may be required. |

### 1.2 Upload Section

The upload section sits below the JAR list and provides controls for adding new JARs to the analyzer.

| Control | Description |
|---|---|
| **Upload Button** | Opens a file input dialog. Accepts `.jar` and `.war` files up to **2 GB** in size. After selecting a file, analysis begins automatically based on the selected mode. |
| **Analyze from Local Path** | A text field for submitting a JAR or WAR already on the server's local filesystem by its absolute path (e.g. `C:\builds\app-0.0.1-SNAPSHOT.jar`). No file transfer needed. |
| **Analysis Mode Toggle** | A two-option toggle switch: **Static** and **Claude**. In **Static** mode, only static bytecode analysis is performed (decompilation, class inspection, endpoint detection). In **Claude** mode, static analysis is performed first, followed by AI-powered enrichment that adds business context, corrections, and insights. |
| **Source Project Path** | A text input field that appears **only when Claude mode is selected**. Enter the absolute filesystem path to the original source project. This helps Claude correlate decompiled bytecode with the original source code for higher-quality analysis. |

### 1.3 Claude Progress Indicator

When one or more Claude scans are active, a progress section appears in the sidebar below the upload controls.

- Displays the name of each JAR currently being scanned by Claude.
- Each active scan shows a **progress bar** indicating completion percentage.
- Multiple concurrent scans are listed individually.
- The indicator disappears when no scans are running.

---

## 2. Top Bar

The top bar spans the full width of the main content area and provides global controls.

| Button | Description |
|---|---|
| **Sessions** | Opens a full-screen overlay listing all Claude sessions (both currently running and previously completed). Each session entry shows its status, the associated JAR, start time, and duration. Use this to monitor or review Claude enrichment activity across all JARs. |
| **Logs** | Opens a log viewer panel that displays application-level log messages. Useful for debugging upload failures, analysis errors, or Claude communication issues. |
| **Analysis Status** | Toggles the visibility of the progress log panel. When visible, this panel streams real-time progress messages during JAR analysis (e.g., "Decompiling classes...", "Detecting endpoints...", "Building call chains..."). Click again to hide. |
| **Chat Toggle** | Switches the chat interface between **New** (floating chatbox) and **Classic** (session-based panel) modes. See the [Chat](#8-chat) section for details. |

---

## 3. JAR Header (When a JAR Is Selected)

When you click a JAR in the sidebar, a header section appears at the top of the main content area with the following elements.

### Title and Stats Badges

| Element | Description |
|---|---|
| **JAR Title** | The name of the selected JAR displayed prominently. |
| **Classes Badge** | Total number of Java classes in the JAR. |
| **Endpoints Badge** | Total number of HTTP endpoints detected. |
| **File Size Badge** | The size of the uploaded JAR file (e.g., "45.2 MB"). |
| **Date Badge** | The date and time the JAR was uploaded. |

### Mode Badge

A label indicating which data view is currently active:

| Badge | Meaning |
|---|---|
| **STATIC** | Showing results from static bytecode analysis only. |
| **CORRECTED** | Showing results that include Claude's corrections and enrichments merged on top of the static analysis. |
| **Previous** | Showing results from an earlier Claude scan (before the most recent correction round). |

### Mode Toggle Buttons

| Button | Action |
|---|---|
| **View Corrected** | Switches to the corrected data view. Only enabled when Claude corrections exist for this JAR. |
| **View Static** | Switches back to the raw static analysis data, ignoring any Claude corrections. |
| **View Previous** | Switches to data from a prior Claude correction round (useful for comparing before/after). |

### Claude Button

| State | Display | Action |
|---|---|---|
| No Claude scan has ever run | **Enable Claude** button | Initiates the first Claude enrichment scan for this JAR. |
| A previous Claude scan completed | **Re-scan Claude** button | Starts a new Claude enrichment scan (useful after corrections or to improve results). |
| Claude scan currently running | Button is **hidden**; a **progress bar** is shown instead | No action available -- wait for the scan to complete. |

---

## 4. Tab 1: Code Structure

The Code Structure tab provides a navigable view of all classes, fields, methods, and invocations found in the JAR. It has three distinct view modes selectable via sub-tabs at the top.

### 4.1 Package View

This is the **default view mode**. Classes are grouped by their Java package.

- **Main Application Classes** appear first, grouped under their respective packages.
- **Internal Dependencies** appear below in a separate section, organized by dependency JAR module name. These are classes pulled in from libraries bundled inside the analyzed JAR.
- Each package group is collapsible. Click a package name to expand or collapse its class list.
- Each class entry shows its fully qualified name and annotations (e.g., `@RestController`, `@Service`).

### 4.2 Project View

An IntelliJ-style folder hierarchy that mirrors the expected source project structure.

- The tree starts from `src/main/java` and descends through the package directory structure (e.g., `src/main/java/com/example/domain/ClassName.java`).
- **Single-child path collapsing**: When a directory has only one subdirectory, the path is collapsed into a single node (e.g., `com/example/domain` instead of three separate levels). This reduces visual clutter.
- Clicking a `.java` leaf node selects that class and opens it in the code panel.

### 4.3 Visual Overview

A card-based grid layout that groups classes by Spring stereotype annotation.

| Group | Contents |
|---|---|
| **@RestController** | All classes annotated with `@RestController` or `@Controller`. |
| **@Service** | All classes annotated with `@Service`. |
| **@Repository** | All classes annotated with `@Repository`. |
| **@Component** | All classes annotated with `@Component` or other Spring-managed beans. |

Each card displays the class name and a brief summary (annotation, package). Clicking a card selects the class.

### 4.4 Class Detail (Expanded)

When a class is expanded (in Package or Project view), the following sections are displayed:

#### Fields

Each field shows:

| Column | Description |
|---|---|
| **Annotations** | Any annotations on the field (e.g., `@Autowired`, `@Value("${...}")`). |
| **Type** | The Java type of the field (e.g., `String`, `List<Order>`, `OrderRepository`). |

#### Methods

Each method shows:

| Column | Description |
|---|---|
| **Signature** | The full method signature (name + parameter list). |
| **Parameters** | Each parameter's type and name. |
| **Return Type** | The return type of the method. |
| **Annotations** | Method-level annotations (e.g., `@GetMapping("/api/orders")`, `@Transactional`). |

#### Invocations (Lazy-Loaded)

Method invocations are loaded on demand (lazy) to keep the initial render fast.

- Expand a method to see all **outgoing method calls** made within that method body.
- Each invocation shows the target class and method name.
- Invocations are the building blocks of the call chains shown in the Endpoint Flows tab.

### 4.5 Code Structure Toolbar

The toolbar sits above the tree/grid area and provides the following controls:

| Control | Description |
|---|---|
| **Search Box** | A text input that filters the visible classes, methods, and annotations in real time. Type a class name, method name, or annotation to narrow the view. Matching is case-insensitive and applies across all three view modes. |
| **Expand All** | Expands every collapsible node in the current view, showing all packages, classes, fields, methods, and invocations. |
| **Collapse All** | Collapses every node back to the top-level grouping. |
| **Back Button** | Navigates to the previously viewed class (browser-style history). |
| **Forward Button** | Navigates forward in the history stack (available after using Back). |
| **History** | Opens a dropdown showing the full navigation history. Click any entry to jump directly to that class. |
| **JAR Source Filter Bar** | Filters the tree to show only classes from specific source JARs (relevant when the analyzed JAR bundles multiple dependency JARs). Select one or more source JARs to narrow the view. |

### 4.6 Code Panel (Right Side)

The right side of the Code Structure tab displays a read-only code viewer.

- When a **class** is selected, the full decompiled Java source code for that class is displayed.
- When a **method** is selected, the code panel scrolls to and highlights the relevant method within the class source.
- Syntax highlighting is applied for Java keywords, types, strings, annotations, and comments.

---

## 5. Tab 2: Endpoint Flows

The Endpoint Flows tab provides a detailed view of every detected HTTP endpoint and its downstream call chain.

### 5.1 Left Pane -- Endpoint List

The left pane shows all endpoints grouped by their controller class.

#### Grouping

- Endpoints are grouped under their parent controller class name.
- Each group header shows the controller name and the **count** of endpoints in that controller.
- Groups are collapsible.

#### Supported Entry Point Types

The analyzer detects all of the following entry types, not just REST endpoints:

| HTTP Method Label | Source Annotation | Description |
|---|---|---|
| `GET`, `POST`, `PUT`, `DELETE`, `PATCH` | `@RequestMapping` / `@GetMapping` / etc. | Standard HTTP REST endpoints |
| `AMQP` | `@RabbitListener` | RabbitMQ message consumers |
| `KAFKA` | `@KafkaListener` | Apache Kafka consumers |
| `WS` | `@MessageMapping` | WebSocket message handlers |
| `SCHEDULED` | `@Scheduled` | Timed tasks (cron / fixedRate / fixedDelay) |
| `EVENT` | `@EventListener` / `@TransactionalEventListener` | Spring application event handlers |

#### Filter Search

A search input at the top of the left pane filters across:

- Entry type (GET, POST, AMQP, KAFKA, WS, SCHEDULED, EVENT)
- URL path or queue/topic name
- Method name
- Controller or listener class name

Filtering is case-insensitive and updates the visible list in real time.

#### Endpoint Entry

Each endpoint entry displays:

| Element | Description |
|---|---|
| **HTTP Method Badge** | A colored badge indicating the HTTP method. Color coding: `GET` = green, `POST` = blue, `PUT` = orange, `DELETE` = red. Other methods use neutral colors. |
| **Full Path** | The complete URL path for the endpoint (e.g., `/api/v1/orders/{id}`). |
| **Method Signature** | The Java method name and parameter types (e.g., `getOrderById(Long id)`). |
| **Return Type** | The Java return type (e.g., `ResponseEntity<OrderDTO>`). |

Click an endpoint to load its detail in the right pane.

### 5.2 Right Pane -- Endpoint Detail

When an endpoint is selected, the right pane displays a comprehensive breakdown of its execution flow.

#### Header

| Element | Description |
|---|---|
| **HTTP Method** | The HTTP method (same colored badge as the list). |
| **Full Path** | The complete URL path. |
| **Method Signature** | The full Java method signature. |

#### Call Chain

A visual flowchart showing the execution path from the controller entry point through services down to repositories.

- Displayed as a horizontal or vertical chain of connected nodes.
- Each node shows the class name with a **stereotype chip** (e.g., `@RestController`, `@Service`, `@Repository`).
- Nodes are connected by **arrows** indicating the direction of the call.
- The chain follows the pattern: **Controller --> Service --> Repository** (with as many intermediate layers as the actual code has).

#### Collections

Lists the MongoDB collections accessed during this endpoint's execution.

| Badge | Meaning |
|---|---|
| **DATA** | The collection is used as a primary data store (reads/writes business entities). |
| **VIEW** | The collection is a MongoDB view (read-only, derived from another collection). |

#### Operation Flow Table

A detailed step-by-step table of every method call in the endpoint's execution chain.

| Column | Description |
|---|---|
| **#** | Step number in the execution sequence (1, 2, 3, ...). |
| **Stereotype Label** | The Spring stereotype of the class containing this method, shown as a badge (e.g., `[CONTROLLER]`, `[SERVICE]`, `[REPOSITORY]`). |
| **ClassName.methodName** | The fully qualified method reference. **Clickable** -- clicking navigates to the Code Structure tab and highlights that method. |
| **Description** | A human-readable description of what this step does (generated by static analysis or Claude enrichment). |
| **Params** | The parameters accepted by this method. |
| **Returns** | The return type of this method. |
| **Dispatch Badge** | Indicates how the method call was resolved during static analysis. See the table below. |
| **Recursive Indicator** | Shown when the method call is detected as recursive (calls itself directly or indirectly). |

##### Dispatch Badge Values

| Badge | Meaning |
|---|---|
| **QUALIFIED** | The call target was resolved with full certainty from explicit type information. |
| **DYNAMIC** | The call target was resolved through dynamic dispatch (e.g., interface polymorphism, runtime binding). Multiple implementations may exist. |
| **DYNAMIC_DISPATCH** | Dynamic dispatch variant tracked at the bytecode level with implementations found. |
| **HEURISTIC** | The call target was resolved using heuristic matching (e.g., naming conventions, single-implementation inference). |
| **IFACE ONLY** | Only the interface declaration was found; no concrete implementation was located in the analyzed JAR. |
| **INTERFACE_FALLBACK** | Call resolved to the interface/abstract definition because no concrete implementation was found via index traversal. |
| **@PRIMARY** | The call target was resolved using Spring's `@Primary` annotation to select among multiple candidates. |

##### Additional Call Node Indicators

| Indicator | Meaning |
|---|---|
| **Recursive** | The method call is detected as directly or indirectly recursive. |
| **Cache op** | Method is annotated with `@Cacheable`, `@CacheEvict`, or `@CachePut`. |
| **SQL** | Raw SQL string literals were detected inside this method's bytecode. |
| **Stored proc** | A stored procedure call was detected via `SimpleJdbcCall`, `Connection.prepareCall`, `@Procedure`, or `EntityManager.createStoredProcedureQuery`. |
| **Aggregation pipeline** | MongoDB aggregation pipeline code was detected in this node. |

#### Breadcrumb Navigation

| Button | Action |
|---|---|
| **Back** | Returns to the previous endpoint detail viewed. |
| **Next** | Advances to the next endpoint in the navigation history. |
| **Root** | Jumps back to the root endpoint (the first one viewed in the current navigation chain). |

---

## 6. Tab 3: Summary

The Summary tab contains **11 sub-tabs**, each providing a different analytical perspective on the JAR's contents. Click a sub-tab name to switch views.

### 6.1 Endpoint Report

**Sub-tab label:** Endpoint Report (default sub-tab when Summary is opened)

A card-based list of all HTTP endpoints with rich metadata.

#### Card Contents

Each endpoint card displays:

| Element | Description |
|---|---|
| **HTTP Method Badge** | Colored badge (GET/POST/PUT/DELETE). |
| **Path** | The full URL path for the endpoint. |
| **Method Signature** | The Java method name and parameters. |
| **Collections** | Number of MongoDB collections accessed. |
| **Views** | Number of MongoDB views accessed. |
| **DB Ops** | Total number of database operations performed. |
| **Internal Calls** | Number of calls to methods within the same JAR. |
| **External Calls** | Number of calls to methods in external libraries. |
| **LOC** | Lines of code in the endpoint's full call chain. |
| **Size** | A categorical size label: `S` (Small), `M` (Medium), `L` (Large), `XL` (Extra Large), based on the combined complexity metrics. |
| **Domain Badge** | The business domain this endpoint belongs to (e.g., "Orders", "Payments", "Users"). |
| **Performance Badge** | An estimated performance classification based on the number of DB operations, external calls, and chain depth. |

#### Sorting

Click any sort option to reorder the endpoint cards. Available sort fields:

| Sort Field | Description |
|---|---|
| **Path** | Alphabetical by URL path. |
| **Domain** | Grouped by business domain. |
| **Collections** | By number of collections accessed (descending). |
| **DB Ops** | By number of database operations (descending). |
| **External** | By number of external calls (descending). |
| **LOC** | By lines of code (descending). |
| **Size** | By size category (XL > L > M > S). |

#### Filters

Two levels of filtering are available:

**Domain Pills**: Quick-filter buttons at the top of the card list. Each pill represents a detected domain. Click one or more to show only endpoints in those domains. Click again to deselect.

**Advanced Filters Panel**: Expand this panel to access granular filtering controls.

| Filter Type | Controls |
|---|---|
| **Range Sliders** | Adjustable min/max sliders for: Collections, Views, DB Ops, Internal Calls, External Calls, Methods, LOC. Drag handles to set the desired range. |
| **HTTP Method Pills** | Toggle pills for GET, POST, PUT, DELETE, PATCH. Select one or more to filter by HTTP method. |
| **Size Pills** | Toggle pills for S, M, L, XL. Select one or more to filter by endpoint size. |
| **Performance Pills** | Toggle pills for performance categories. |
| **Operations Filter** | Toggle pills for database operation types: `READ`, `WRITE`, `UPDATE`, `DELETE`, `AGGREGATE`. Select to show only endpoints that perform the chosen operation types. |

#### Pagination

- Results are paginated at **25 endpoints per page**.
- Page navigation controls appear at the bottom of the card list (Previous / Next / page numbers).

### 6.2 Collection Analysis

A tabular view of all MongoDB collections and views detected across the JAR.

#### Table Columns

| Column | Description |
|---|---|
| **Collection Name** | The MongoDB collection name. Accompanied by a badge: `DATA` for standard collections, `VIEW` for MongoDB views. |
| **Domain** | The business domain this collection belongs to (e.g., Claims, Accounting, Underwriting, Policy). |
| **Type** | The collection type (standard collection, capped collection, or view). |
| **Read Ops** | Number of read operations detected against this collection. |
| **Write Ops** | Number of write operations detected against this collection. |
| **Agg Ops** | Number of aggregation pipeline operations detected against this collection. |
| **Detected Via** | How the collection reference was discovered. Possible values: `REPOSITORY_MAPPING` (from generic `MongoRepository<Entity, Id>` signature), `DOCUMENT_ANNOTATION` (from `@Document(collection="...")` on entity), `STRING_LITERAL` (from string constant matching collection-name pattern), `FIELD_CONSTANT` (from `static final String` field on repository class), `PIPELINE_ANNOTATION` (from `@Aggregation` pipeline reference). |
| **Complexity** | Weighted complexity score based on: endpoint count (×1.0), write ops (×1.5), aggregation pipelines (×2.0), cross-domain access (×3.0). Classified as Low (≤4), Medium (≤10), or High (>10). |
| **Verification** | Whether the collection was confirmed to exist in the database. Values: `IN_DB` (confirmed present), `NOT_IN_DB` (not found in the database), `NEED_REVIEW` (could not be automatically verified), `CLAUDE_*` (status assigned by Claude enrichment). |

#### View Toggle

- **Hierarchical View**: Groups collections under their owning domain, with expandable domain sections.
- **Flat Table View**: Shows all collections in a single flat table without grouping.

Toggle between these two layouts using the view switch control above the table.

### 6.3 External Dependencies

A table listing all external library and module dependencies detected in the endpoint call chains.

#### Table Columns

| Column | Description |
|---|---|
| **Module Name** | The name of the external module or library (e.g., `spring-data-mongodb`, `commons-lang3`). |
| **Domain** | The business domain(s) that use this dependency. |
| **Call Count** | Total number of method calls made to this module across all endpoints. |
| **Called By** | The classes that invoke methods on this module. **Expandable** -- click to see the full list of calling classes and methods. |
| **Type** | Classification of the dependency: `Library` (third-party library), `Internal` (another module within the same organization), `External` (external service call). |

#### Module Expansion

Click the expand arrow on any module row to see a detailed list of all methods called within that module, along with the callers for each method.

### 6.4 Distributed Transactions

A table identifying endpoints that participate in or require distributed transaction handling.

#### Table Columns

| Column | Description |
|---|---|
| **Endpoint Name** | The endpoint's path and method. **Clickable** -- navigates to the Endpoint Flows detail for this endpoint. |
| **Domain** | The business domain of the endpoint. |
| **Transaction Requirement** | The assessed transaction need: `REQUIRED` (the endpoint modifies multiple data sources and requires distributed transaction coordination), `ADVISORY` (the endpoint touches multiple sources but may not strictly require distributed transactions), `NONE` (no distributed transaction concern detected). |

### 6.5 Batch Jobs

A table of detected batch processing jobs (e.g., Spring Batch jobs).

#### Table Columns

| Column | Description |
|---|---|
| **Batch Name** | The name or identifier of the batch job. |
| **Primary Domain** | The main business domain this batch job operates in. |
| **Touches Domains** | Badge pills showing all business domains this batch job interacts with. Multiple badges indicate cross-domain batch processing. |
| **Collections** | The MongoDB collections accessed by this batch job. |
| **Methods** | The number of methods involved in the batch job's execution chain. |
| **Size** | Categorical size: `S` (Small), `M` (Medium), `L` (Large), `XL` (Extra Large). |

### 6.6 Scheduled Jobs

A table of methods annotated with scheduling annotations (`@Scheduled`, etc.).

#### Table Columns

| Column | Description |
|---|---|
| **Method Name** | The scheduled method's name. |
| **Class** | The class containing the scheduled method. |
| **Schedule Expression** | The scheduling configuration. This can be a **cron** expression (e.g., `0 0 2 * * ?` for 2 AM daily), a **fixedRate** value (e.g., `fixedRate=60000` for every 60 seconds), or a **fixedDelay** value (e.g., `fixedDelay=30000` for 30 seconds after completion). |
| **Domain** | The business domain this job belongs to. |
| **Collections** | MongoDB collections accessed during execution. |
| **Methods** | Number of methods in the execution chain. |

#### Grouping

Scheduled jobs are grouped by their **execution pattern** (e.g., all cron-based jobs together, all fixedRate jobs together, all fixedDelay jobs together) for easier scanning.

### 6.7 Aggregation Flows

A table of MongoDB aggregation pipelines detected in the codebase.

#### Table Columns

| Column | Description |
|---|---|
| **Pipeline ID** | A unique identifier for the aggregation pipeline. |
| **Collections** | The MongoDB collections involved, including any collections joined via `$lookup` stages. |
| **Pipeline Stages** | The total count of stages in the pipeline, plus the types of stages used (e.g., `$match`, `$group`, `$project`, `$lookup`, `$unwind`). |
| **Called From** | The method and class that executes this aggregation pipeline. |
| **Complexity** | An assessed complexity level for the pipeline. |
| **$Lookup Joins** | The number of `$lookup` (join) stages in the pipeline. |
| **Dynamic** | `Yes` if the pipeline is built dynamically at runtime (e.g., conditional stages), `No` if it is static. |

#### Pipeline Expansion

Click the expand arrow on any row to see the **full pipeline structure** -- every stage listed in execution order with its parameters.

- **Cross-domain `$lookup` stages** are highlighted in **red** to flag potential domain boundary violations.

#### Filters

| Filter | Description |
|---|---|
| **Collection** | Filter pipelines by the collections they access. |
| **Pipeline Stage** | Filter pipelines that include specific stage types (e.g., show only pipelines using `$lookup`). |
| **Complexity** | Filter by complexity level. |

### 6.8 Dynamic Flows

A table of method calls resolved through dynamic dispatch (polymorphism, interfaces, abstract classes).

#### Table Columns

| Column | Description |
|---|---|
| **Dispatch Type** | How the dynamic call was resolved (e.g., interface polymorphism, abstract class, factory pattern). |
| **Target Class** | The declared type (interface or abstract class) of the call target. |
| **Implementations Found** | The number of concrete implementations discovered in the JAR for this target type. |
| **Confidence** | The confidence level of the resolution (how certain the analyzer is that it identified the correct implementation). |
| **Called From** | The method and class that makes this dynamic call. |
| **Collections Touched** | MongoDB collections that may be accessed depending on which implementation is invoked. |
| **Risk Level** | An assessed risk level for this dynamic dispatch point. Higher risk indicates more uncertainty about runtime behavior. |

### 6.9 Verticalisation

The Verticalisation sub-tab analyzes cross-domain coupling to support domain-driven design and microservice extraction. It is divided into two sections.

#### Section A: Bean Crossing

Identifies Spring beans that are called across domain boundaries.

| Column | Description |
|---|---|
| **Target Bean** | The Spring bean being called from another domain. |
| **Stereotype** | The Spring stereotype of the target bean (e.g., `@Service`, `@Repository`). |
| **Source Module** | The module where the calling code resides. |
| **Source Domain** | The business domain of the caller. |
| **Call Count** | How many times this cross-domain call occurs. |
| **Caller Endpoints** | The HTTP endpoints whose call chains include this cross-domain invocation. |
| **Recommendation** | The suggested refactoring approach. Typically `REST_API` -- recommending that the cross-domain call be replaced with a REST API call to maintain domain isolation. |

#### Section B: Data Crossing

Identifies MongoDB collections that are accessed by multiple domains.

| Column | Description |
|---|---|
| **Collection** | The MongoDB collection name. |
| **Type** | The collection type (DATA or VIEW). |
| **Owner Domain** | The primary domain that owns this collection. |
| **Accessed By Domains** | All domains that access this collection (including the owner). |
| **Access Type** | The type of access performed by non-owner domains (e.g., read-only, read-write). |
| **Endpoints** | The HTTP endpoints that access this collection across domain boundaries. |
| **Recommendation** | The suggested approach for resolving the data coupling (e.g., replicate data, create an API, introduce an event). |

### 6.10 Claude Insights

Available only after a Claude enrichment scan has completed. Displays AI-generated business analysis for each endpoint.

#### Table Columns

| Column | Description |
|---|---|
| **Endpoint Name** | The endpoint path and method. |
| **Business Process** | A Claude-generated description of the business process this endpoint implements (e.g., "Order placement with inventory reservation and payment processing"). |
| **Risk Flags** | Identified risks such as missing error handling, potential data inconsistency, performance bottlenecks, or security concerns. |
| **Migration Difficulty** | A numeric score from **1** (trivial) to **5** (extremely complex) assessing how difficult it would be to migrate this endpoint to a new architecture. |
| **Key Insights** | Notable observations about the endpoint's implementation, patterns, or anti-patterns. |
| **Recommendations** | Suggested improvements, refactoring opportunities, or migration strategies. |

### 6.11 Claude Corrections

Available only after a Claude enrichment scan has completed. Shows a per-endpoint summary of what Claude changed compared to the static analysis.

#### Table Columns

| Column | Description |
|---|---|
| **Endpoint Name** | The endpoint path and method. |
| **Added Count** | Number of items (call chain steps, collections, etc.) that Claude added to the endpoint's analysis that were missed by static analysis. |
| **Removed Count** | Number of items that Claude removed from the endpoint's analysis (false positives from static analysis). |
| **Verified Count** | Number of items from the static analysis that Claude confirmed as correct. |
| **Verified %** | The percentage of static analysis items that Claude verified as accurate. A higher percentage indicates the static analysis was largely correct for this endpoint. |

---

## 7. Export

The Export feature allows you to download the analysis results for offline use or integration with other tools.

### Accessing Export

- The **Export** button is located within the **Summary tab** toolbar area.
- Clicking it opens an export dialog.

### Available Formats

| Format | Description |
|---|---|
| **JSON** | Exports the analysis data as a structured JSON file. Suitable for programmatic consumption and integration with other tools. |
| **Excel** | Exports the analysis data as an `.xlsx` Excel workbook with multiple sheets. Suitable for manual review, sharing with stakeholders, and reporting. |

### Exportable Sections

The Excel workbook contains **13 sheets**. The JSON export includes equivalent top-level keys.

| Sheet | Contents |
|---|---|
| **Summary** | Key metrics: total endpoints, collections, views, business domains, cross-module endpoints, batch jobs. |
| **Endpoints** | HTTP method, path, name, collections count, views count, DB ops, methods, LOC, scope calls, operation types, size (S/M/L/XL), performance classification. |
| **Endpoint-Collections** | Per-endpoint → collection mapping with operation type and detection source per row. |
| **Collections** | Per-collection metrics: number of endpoints, domains, usage count, verification status. |
| **Collection Summary** | Aggregated collection view with read/write/aggregation op breakdown. |
| **Collection Usage Detail** | Detailed cross-reference of every endpoint accessing every collection. |
| **Transactions** | Transaction requirement classification per endpoint (REQUIRED / ADVISORY / NONE). |
| **Batch** | Scheduled and batch jobs with domains, collections, and sizing. |
| **Views** | MongoDB views detected with their source collections. |
| **External** | Cross-module dependency details with call counts. |
| **Extended Calls Detail** | External call breadcrumb chains (full call path per external invocation). |
| **Vertical Method Calls** | Method invocation patterns across domain boundaries. |
| **Vertical Cross-Domain** | Cross-domain dependency chains with refactoring recommendations. |

Excel formatting includes: conditional color fills (XL = red, L = orange), header/title/section row styles, alternating data row shading, column freeze, and auto-filters on all data sheets.

---

## 8. Chat

The Chat feature provides an AI-powered conversational interface for asking questions about the analyzed JAR.

### Floating Chatbox (New Mode)

- A **floating action button (FAB)** appears in the **bottom-right corner** of the screen.
- Clicking the FAB opens a chat popover window.
- Type questions about the current JAR's analysis in natural language (e.g., "Which endpoints write to the orders collection?", "What are the most complex endpoints?", "Explain the call chain for POST /api/orders").
- The AI responds with answers drawn from the analysis data.
- The chatbox can be minimized back to the FAB by clicking the close/minimize button.

### Classic Chat Panel (Classic Mode)

- The Classic chat panel is a **session-based** chat interface that opens as a dedicated side panel.
- Each chat session is persisted and can be revisited.
- Supports longer, more detailed conversations with full context retention within a session.
- Previous sessions can be viewed and continued.

### Chat Toggle

- Use the **Chat toggle** in the **top bar** to switch between **New** (floating chatbox) and **Classic** (session-based panel) modes.
- Your preference persists across page reloads.

---

*End of JAR Analyzer User Manual.*
