# PostgreSQL (PL/pgSQL) — Database Support

PostgreSQL PL/pgSQL procedures and functions are parsed grammar-first (ANTLR) and crawled into
the unified call/flow graph.

## Module & grammar
- **Parser module:** `postgres-parser`
- **Grammar:** `postgres-parser/src/main/antlr4/com/plsql/postgres/grammar/PostgresLexer.g4` +
  `PostgresParser.g4`
- **Visitor:** `com.plsql.postgres.visitor.PostgresAnalysisVisitor` — calls, table ops, joins,
  cursors, dynamic SQL, sequences, exception handlers.
- **Adapter:** `plsql-parser/.../multidb/PostgresProcedureParser` (maps Postgres output into the
  unified model consumed by the web layer).
- **JDBC driver:** `org.postgresql:postgresql` (42.7.3, on Maven Central — no manual install
  needed, unlike Oracle's `ojdbc11`).

## Connection
| Field | Value |
|---|---|
| db_type | `POSTGRES` |
| Database dropdown | Home screen → parser's connection form (`js/conn-form-fields-postgres.js`) |

## How source & metadata are fetched
- **Source:** routine definitions from PostgreSQL's catalog (`pg_proc`/`pg_get_functiondef`-style
  text), loaded via the standard JDBC source-fetch path shared with the other dialects.
- **Table metadata:** PostgreSQL catalog views (columns, indexes, constraints, triggers, view
  definitions), fetched the same way as the other dialects' metadata enrichment phase.

## Grammar & facet coverage (recent additions)
- `DECLARE <cursor> CURSOR FOR <prepared-statement-name>` (in addition to `FOR SELECT …`).
- `CAST(expr AS type)` inside `CALL` arguments (e.g. `CALL p(CAST(v AS INTEGER))`).
- Structured `SELECT` (no regex fallback); optional function parameter modes;
  recursive CTE, `MERGE`.
- Sequences: `NEXTVAL`/`CURRVAL`/`SETVAL`/`LASTVAL` are now extracted into a real `sequences`
  facet (previously excluded as builtins and silently discarded).
- Exception handlers: real `PostgresExceptionHandlerInfo` model + visitor, wired to the shared
  `ParsedObject.exceptionHandlers` field (same shape as Oracle's).
- `DO $$ ... $$` anonymous blocks: parse via a dedicated `doStatement` grammar alternative,
  reusing the same body-block visitor logic as routine bodies.
- `RETURNING` clause on INSERT/UPDATE/DELETE: captured as a structured `returningColumns` field
  on the table-operation model, not left as unstructured trailing text.
- Predicates (WHERE/HAVING) are populated via the shared `TableOperationEnricher`, same as the
  other non-Oracle dialects.
- Truncation detection uses **balanced BEGIN/END** (PostgreSQL DDL ends in bare `END`), so
  genuine parse errors surface instead of being masked as "truncated".

See `docs/audit-findings-2026-09-13.md` for the full verification detail behind each of the
items above (parser-layer vs. live-output confidence, remaining caveats).

## Quick start
1. Home → Database = **PostgreSQL**, select project/environment/connection as configured in
   `plsql-config.yaml` (`dbType: POSTGRES`).
2. Enter the target procedure/function name and Type = PROCEDURE/FUNCTION → **Analyze**.
