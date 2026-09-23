# Oracle (PL/SQL) — Database Support

The analyzer's original and reference dialect. Oracle stored procedures, functions,
packages, and triggers are parsed grammar-first via ANTLR and crawled into a full
call/flow graph.

## Module & grammar
- **Parser module:** `plsql-parser`
- **Grammar:** `plsql-parser/src/main/antlr4/.../PlSqlParser.g4` + `PlSqlLexer.g4`
- **Engine:** `PlSqlParserEngine` — two-stage ANTLR parsing (SLL → LL fallback). The
  shared decision (DFA) cache is kept **warm** across procedures (cleared only every
  200 parses) so large packages parse fast.
- **Visitor:** `PlSqlAnalysisVisitor` — extracts calls, table operations, joins,
  cursors, sequences, dynamic SQL, exception handlers, and **package-level variables**
  (`packageVariables` / `globalVariables` / `externalPackageVarRefs` binding).
- **JDBC driver:** `ojdbc` (Oracle thin).

## Connection (demo)
| Field | Value |
|---|---|
| Project / Env | `OracleDemo` / `local` |
| Connection | `oracle-free-conn` |
| JDBC URL | `jdbc:oracle:thin:@//localhost:1521/FREEPDB1` |
| User / Schema | `analyzer` (schema `ANALYZER`) |
| db_type | `ORACLE` |

Local container: `gvenzl/oracle-free:23-slim`, service `FREEPDB1`.

## How source & metadata are fetched
- **Source:** `ALL_SOURCE` (one query resolves the object type — PACKAGE BODY /
  PROCEDURE / FUNCTION — instead of probing each type). Truncation fallback via
  `DBMS_METADATA`; wrapped/encrypted objects are detected and flagged.
- **Table metadata (enrichment):** bulk `ALL_TAB_COLUMNS`, `ALL_INDEXES` /
  `ALL_IND_COLUMNS`, `ALL_CONSTRAINTS` / `ALL_CONS_COLUMNS`, `ALL_TRIGGERS`,
  view/mview definitions — batched with `WHERE name IN (...)` (not per-table).
- **Object resolution:** `ALL_OBJECTS`, synonyms followed one level.

## Dialect notes
- Schema/owner names are upper-cased (Oracle default), which matches the catalog.
- Package bodies, standalone procedures/functions, and triggers are all supported.
- Cursor `FOR` loops, `EXECUTE IMMEDIATE`, `MERGE`, `BULK COLLECT` / `FORALL`,
  sequences (`NEXTVAL`), and recursive CTEs are extracted.

## Reference flow (demo)
`ERP_USP_RUN_CYCLE` — 21 nodes / 149 unique edges / 33 tables / 6,587 LOC / depth 11 / 0 errors.

## Quick start
1. Home → Database = **Oracle**, Project `OracleDemo`, Env `local`.
2. Owner = `ANALYZER` (or auto-detect).
3. Target = `PKG_CUSTOMER` or `PKG.PROC_NAME`, Type = PROCEDURE/FUNCTION/PACKAGE → **Analyze**.
