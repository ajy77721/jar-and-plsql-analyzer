# Supported Databases

The analyzer parses stored procedures / functions / packages **grammar-first** (ANTLR,
per dialect) and crawls them into a unified call/flow graph, table-operation, join,
cursor, sequence, and dynamic-SQL model. Five SQL dialects plus JVM bytecode are
supported.

| Dialect | Module | db_type | Per-DB doc |
|---|---|---|---|
| Oracle PL/SQL | `plsql-parser` | `ORACLE` | [db-oracle.md](db-oracle.md) |
| MySQL | `mysql-parser` | `MYSQL` | [db-mysql.md](db-mysql.md) |
| Microsoft SQL Server (T-SQL) | `sqlserver-parser` | `SQLSERVER` | [db-sqlserver.md](db-sqlserver.md) |
| IBM Db2 (SQL PL) | `db2-parser` | `DB2` | [db-db2.md](db-db2.md) |
| PostgreSQL (PL/pgSQL) | `postgres-parser` | `POSTGRES` | [db-postgres.md](db-postgres.md) |
| Java JAR / WAR (bytecode) | `jar-analyzer-core` | — | [manual-jar-analyzer.md](manual-jar-analyzer.md), [jar-parse.md](jar-parse.md) |

Non-Oracle dialects map into the unified model via `plsql-parser/.../multidb/*ProcedureParser`.

## What's captured (all dialects)
- Call tree / flow graph (INTERNAL vs EXTERNAL calls, depth, cycles)
- Table operations (INSERT/UPDATE/DELETE/SELECT/MERGE) with line numbers
- Joins with ON-condition alias resolution (correct left/right tables), grouped per query
- Cursors, sequences, dynamic SQL, exception handlers
- Table metadata: columns, indexes, constraints, triggers, view/mview definitions
- Package-level variables and their per-procedure bindings (Oracle)

## Reference complex flow (per dialect, 0 errors)
| Dialect | Entry | Nodes | Edges | Tables | LOC | Depth |
|---|---|---|---|---|---|---|
| SQL Server (reference) | USP_RUNMRPCYCLE | 18 | 73 | 52 | 6,974 | 11 |
| Oracle | ERP_USP_RUN_CYCLE | 21 | 149 | 33 | 6,587 | 11 |
| MySQL | ERP_USP_RUN_CYCLE | 28 | 205 | 92 | 8,855 | 12 |
| Db2 | ERP_USP_RUN_CYCLE | 26 | 100 | 37 | 5,849 | 12 |

## Choosing a database in the UI
On the Home screen pick the **Database** dropdown; the Owner/Connection label, the
Target example, and the object-type list adapt to the selected dialect. Use the **?**
button next to Analyze for per-dialect guidance on what to enter.
