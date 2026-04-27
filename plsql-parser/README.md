# PL/SQL Parser

ANTLR4-based PL/SQL flow analysis parser that reads Oracle PL/SQL source code and
produces structured JSON representations of control flow, dependencies, table operations,
and call graphs. Supports Oracle syntax from version 8i through 23c.

## Technology Stack

| Concern | Library | Version |
|---------|---------|---------|
| Grammar / parsing | ANTLR4 | 4.13.1 |
| SQL normalization | JSqlParser | 5.0 |
| Oracle connectivity | ojdbc11 | 23.3.0.23.09 |
| Serialization | Jackson | (managed by Spring Boot 3.2.5) |
| Configuration | SnakeYAML | (managed by parent) |
| Java | JDK | 17 |

## Package Structure

```
com.plsql.parser
  flow/           Orchestration: FlowAnalysisMain, DependencyCrawler,
                  SchemaResolver, TriggerDiscoverer, SourceDownloader,
                  DbConnectionManager, ChunkedFlowWriter
  model/          Data classes: FlowResult, FlowNode, FlowEdge, ParseResult,
                  CallInfo, TableOperationInfo, StatementInfo, CursorInfo,
                  DynamicSqlInfo, ExceptionHandlerInfo, SubprogramInfo, etc.
  visitor/        PlSqlAnalysisVisitor -- main ANTLR visitor implementation
  (root)          PlSqlParserEngine, PlSqlParserMain, PlSqlLexerBase, PlSqlParserBase

com.plsqlanalyzer.parser
  grammar/        PlSqlAnalyzer.g4 (combined grammar for legacy analyzer path)
  model/          PlsqlProcedure, PlsqlUnit, DependencyGraph, SqlAnalysisResult
  service/        Parsing services for the legacy analyzer model
  cache/          Grammar and parse-tree caching
  visitor/        Visitor for the legacy analyzer grammar
```

## ANTLR4 Grammars

Three grammar files drive parsing:

- `PlSqlLexer.g4` -- Tokenizer for Oracle PL/SQL (split lexer grammar).
- `PlSqlParser.g4` -- Full parser grammar covering DDL, DML, PL/SQL blocks, and Oracle-specific syntax.
- `PlSqlAnalyzer.g4` -- Combined grammar used by the legacy analyzer code path.

## Key Classes

- **FlowAnalysisMain** -- Entry point that coordinates the full analysis pipeline.
- **DependencyCrawler** -- Walks the dependency tree, downloading source from Oracle and recursively parsing called objects.
- **SchemaResolver** -- Resolves unqualified object names to schemas using Oracle data dictionary views (ALL_OBJECTS, ALL_SYNONYMS).
- **TriggerDiscoverer** -- Identifies triggers attached to tables referenced by the analyzed procedure.
- **PlSqlAnalysisVisitor** -- ANTLR visitor that extracts flow nodes, table operations, calls, cursors, variables, exception handlers, and dynamic SQL.
- **PlSqlParserEngine** -- Wraps ANTLR lexer/parser setup with error handling and parse-tree caching.
- **ChunkedFlowWriter** -- Writes large analysis results in chunked JSON format to avoid memory issues.

## Key Features

- Dependency crawling across packages, procedures, functions, and triggers
- Schema resolution via Oracle data dictionary (ALL_OBJECTS, ALL_SYNONYMS, ALL_SOURCE)
- Table operation tracking (SELECT, INSERT, UPDATE, DELETE, MERGE) with line numbers
- Trigger discovery for all referenced tables
- Call graph construction with cross-package resolution
- Cursor and dynamic SQL detection
- Exception handler mapping
- Subprogram (nested procedure/function) extraction

## Testing

The module includes 395 tests with zero failures, covering:

- Oracle syntax from 8i through 23c (88-test grammar hardening suite)
- Flashback query syntax
- Complex PL/SQL constructs (bulk collect, forall, pipelined functions, etc.)

```bash
mvn test -pl plsql-parser
```

## Build

```bash
mvn clean install -pl plsql-parser
```

This module is consumed as a dependency by `unified-web`. It can also be used
programmatically by calling `FlowAnalysisMain` or `PlSqlParserEngine` directly.
