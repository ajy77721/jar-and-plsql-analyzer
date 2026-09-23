# MySQL — Database Support

MySQL stored procedures and functions are parsed grammar-first (ANTLR) and crawled
into the same call/flow graph model as Oracle.

## Module & grammar
- **Parser module:** `mysql-parser`
- **Grammar:** `mysql-parser/src/main/antlr4/.../MySqlParser.g4` + `MySqlLexer.g4`
- **Visitor:** `MySqlAnalysisVisitor` — calls, table ops, joins, cursors, dynamic SQL.
- **Adapter:** `plsql-parser/.../multidb/MySqlProcedureParser` maps MySQL results into
  the unified model.
- **JDBC driver:** `mysql-connector-j`.

## Connection (demo)
| Field | Value |
|---|---|
| Project / Env | `MysqlDemo` / `local` |
| Connection | `mysql-local-conn` |
| JDBC URL | `jdbc:mysql://localhost:3307/analyzerdb?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC` |
| User | `root` |
| Database | `analyzerdb` (**case-sensitive** on Linux) |
| db_type | `MYSQL` |

Local container: `mysql:8`, host port `3307`.

## How source & metadata are fetched
- **Source:** `SHOW CREATE PROCEDURE` / `SHOW CREATE FUNCTION`.
- **Table metadata:** `information_schema` (columns / statistics / key_column_usage /
  table_constraints / triggers / views).
- **Case sensitivity:** MySQL database/table names are case-sensitive on Linux. The
  Query Runner normalizes a schema qualifier to the connection's real catalog case
  (e.g. `ANALYZERDB.` → `analyzerdb.`) so schema-qualified queries resolve.

## Grammar coverage (fixed this project)
- Multi-table **`UPDATE t1 JOIN t2 ON … SET …`** (reuses the SELECT join rules;
  qualified `SET table.column` supported).
- `SELECT … INTO … FROM …`, `CAST(expr AS type)` / `CONVERT`, function calls in
  expressions, cursor `DECLARE … HANDLER … LOOP/FETCH`, `PREPARE`/`EXECUTE`.
- Truncation detection: MySQL DDL legitimately ends in bare `END`, so completeness is
  decided by **balanced BEGIN/END** (not the trailing token) — real parse errors surface.

## Reference flow (demo)
`ERP_USP_RUN_CYCLE` — 28 nodes / 205 edges / 92 tables / 8,855 LOC / depth 12 / 0 errors.

## Quick start
1. Home → Database = **MySQL**, Project `MysqlDemo`, Env `local`.
2. Owner/Connection = `mysql-local-conn` (database `analyzerdb`).
3. Target = e.g. `get_total`, Type = PROCEDURE/FUNCTION → **Analyze**.
