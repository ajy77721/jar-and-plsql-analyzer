# Microsoft SQL Server (T-SQL) — Database Support

T-SQL stored procedures and functions are parsed and crawled into the unified
call/flow graph.

## Module & grammar
- **Parser module:** `sqlserver-parser`
- **Grammar:** `sqlserver-parser/src/main/antlr4/.../` (T-SQL grammar)
- **Visitor:** `SqlServerAnalysisVisitor` — calls, table ops, joins, cursors, dynamic SQL.
- **Adapter:** `plsql-parser/.../multidb/SqlServerProcedureParser`.
- **JDBC driver:** `mssql-jdbc`.

## Connection (demo)
| Field | Value |
|---|---|
| Project / Env | `SqlServerDemo` / `local` |
| Connection | `analyzer-db-conn` |
| JDBC URL | `jdbc:sqlserver://localhost:1433;encrypt=false;trustServerCertificate=true;databaseName=analyzer-db` |
| User | `sa` |
| Schema | `ERP` (and `dbo`) |
| db_type | `SQLSERVER` |

Local container: `mssql/server:2019`.

## How source & metadata are fetched
- **Source:** `sys.sql_modules` / `OBJECT_DEFINITION` (schema-qualified fetch;
  standalone objects cached under `SCHEMA.NAME`).
- **Table metadata:** `sys.*` catalog views (columns, indexes, constraints, triggers)
  and `information_schema`.

## Dialect notes
- `SELECT TOP n`, `MERGE`, `OUTPUT`, table variables, and `EXEC`/`sp_executesql`
  dynamic SQL are handled.
- Cross-schema calls resolve as INTERNAL/EXTERNAL based on the object's schema.
- Object names/types are resolved so overloaded / same-named routines don't collide.

## Reference flow (demo, reference metrics)
`USP_RUNMRPCYCLE` — 18 nodes / 73 edges / 52 tables / 6,974 LOC / depth 11 / 0 errors.
This is the reference complex flow the other dialects are scaled against.

## Quick start
1. Home → Database = **SQL Server**, Project `SqlServerDemo`, Env `local`.
2. Owner/Schema = `ERP`.
3. Target = e.g. `usp_GetOrders` or `USP_RUNMRPCYCLE`, Type = PROCEDURE/FUNCTION → **Analyze**.
