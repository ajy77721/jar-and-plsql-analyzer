# PL/SQL Analyzer - Core

Service layer that orchestrates PL/SQL analysis from Oracle database sources.
Bridges the ANTLR4 parsing engine in `plsql-parser` with the web layer in
`unified-web` by coordinating dependency discovery, parallel source fetching,
call graph construction, table/trigger resolution, and metadata collection.

## Technology Stack

| Concern | Library | Version |
|---------|---------|---------|
| PL/SQL parsing | plsql-parser (internal) | 1.0.0-SNAPSHOT |
| Configuration | plsql-config (internal) | 1.0.0-SNAPSHOT |
| Logging | SLF4J API | (managed by Spring Boot 3.2.5) |
| Java | JDK | 17 |

## Package Structure

```
com.plsqlanalyzer.analyzer
  graph/
    CallGraph                Directed graph of procedure calls with forward/reverse
                             tree traversal, search, stats, and depth/node guards

  model/
    AnalysisResult           Top-level result: call graph, table ops, sequences,
                             joins, cursors, metadata, source map, Claude state
    CallGraphNode            Node representing a procedure/function/trigger
    CallEdge                 Directed edge between two call graph nodes
    TableOperationSummary    Per-table summary: operations, schema, triggers, metadata
    SequenceOperationSummary Per-sequence usage summary
    JoinOperationSummary     Per-join summary with table pairs and join types
    CursorOperationSummary   Per-cursor summary with open/fetch/close tracking

  service/
    AnalysisService          Full 6-step pipeline: dependencies -> source fetch ->
                             parse -> call graph -> tables/triggers/metadata -> finalize
    FastAnalysisService      Optimized 5-phase pipeline with BFS discovery, bulk SQL
                             queries, iterative call-target resolution
    CallGraphBuilder         Builds call graph from parsed units with overload
                             disambiguation and cross-schema resolution
    TableOperationCollector  Aggregates table operations across all parsed units
    SequenceOperationCollector Aggregates sequence usage across all parsed units
    JoinOperationCollector   Aggregates join operations across all parsed units
    CursorOperationCollector Aggregates cursor operations across all parsed units
    TriggerResolver          Fetches trigger source from Oracle, parses it, and
                             extracts called procedures for each referenced table
```

## Key Services

- **AnalysisService** -- Primary analysis orchestrator. Accepts a JDBC connection,
  schema, object name, and type. Executes a 6-step pipeline: fetch dependencies via
  `ALL_DEPENDENCIES`, fetch source from `ALL_SOURCE`, parse with ANTLR4, build a call
  graph, collect table/sequence/join/cursor operations with trigger resolution, and
  assemble the final `AnalysisResult`. Supports parallel source fetching and metadata
  queries via a configurable connection supplier.

- **FastAnalysisService** -- Optimized alternative that uses bulk SQL queries (one
  query per object type instead of per-object) and iterative BFS discovery. Discovers
  new call targets from parsed code and re-fetches in up to 5 iterations.

- **CallGraphBuilder** -- Two-pass graph builder: first registers all procedure nodes
  (handling overloaded procedures via parameter signatures), then resolves call edges
  with cross-schema and within-package fallback. Creates synthetic package-level root
  nodes for package-scoped analysis.

- **TriggerResolver** -- For each table touched during analysis, fetches trigger
  records and source in bulk, parses trigger bodies, and extracts called procedures.
  Only attaches triggers whose events (INSERT/UPDATE/DELETE) match the actual DML
  operations performed on the table.

## Build

```bash
mvn clean install -pl plsql-analyzer-core
```

This module has no standalone entry point. It is consumed as a dependency by
`unified-web`, which exposes its services through REST APIs.
