# IBM Db2 (SQL PL) — Database Support

Db2 SQL PL procedures and functions are parsed grammar-first (ANTLR) and crawled into
the unified call/flow graph.

## Module & grammar
- **Parser module:** `db2-parser`
- **Grammar:** `db2-parser/src/main/antlr4/.../Db2Parser.g4` (+ lexer)
- **Visitor:** `Db2AnalysisVisitor` — calls, table ops, joins, cursors, dynamic SQL.
- **Adapter:** `plsql-parser/.../multidb/Db2ProcedureParser`.
- **JDBC driver:** `jcc` (Db2 Type 4).

## Connection (demo)
| Field | Value |
|---|---|
| Project / Env | `Db2Demo` / `local` |
| Connection | `db2-local-conn` |
| JDBC URL | `jdbc:db2://localhost:50000/anlzdb` |
| User / Schema | `db2inst1` (schema `DB2INST1`) |
| db_type | `DB2` |

Local container: `icr.io/db2_community/db2` run with `--platform linux/amd64`
(amd64-emulated on Apple Silicon). Note: Db2 database names are **max 8 chars** — the
demo db is `anlzdb`.

## How source & metadata are fetched
- **Source:** `SYSCAT.ROUTINES` / catalog text (loaded with the `@` statement
  terminator).
- **Table metadata:** `SYSCAT.*` (COLUMNS, INDEXES/INDEXCOLUSE, keys/references, TRIGGERS,
  view definitions).

## Grammar coverage (fixed this project)
- `DECLARE <cursor> CURSOR FOR <prepared-statement-name>` (in addition to `FOR SELECT …`).
- `CAST(expr AS type)` inside `CALL` arguments (e.g. `CALL p(CAST(v AS INTEGER))`).
- Structured `SELECT` (no regex fallback); optional function parameter modes;
  recursive CTE, `MERGE`, `NEXT VALUE FOR`.
- Truncation detection uses **balanced BEGIN/END** (Db2 DDL ends in bare `END`), so
  genuine parse errors surface instead of being masked as "truncated".

## Reference flow (demo)
`ERP_USP_RUN_CYCLE` — 26 nodes / 100 edges / 37 tables / 5,849 LOC / depth 12 / 0 errors.

## Quick start
1. Home → Database = **IBM Db2**, Project `Db2Demo`, Env `local`.
2. Owner/Schema = `DB2INST1`.
3. Target = e.g. `ERP_USP_RUN_CYCLE`, Type = PROCEDURE/FUNCTION → **Analyze**.
