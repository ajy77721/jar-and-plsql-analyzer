package com.plsql.parser.multidb;

import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tasks #26 + #27 — the result-set marker and the TVF/SETOF return shape must cross the multidb
 * bridge into the shared {@link ParsedObject}. Each adapter owns the only conversion the artifact
 * writer (and therefore Mongo ingest and the UI) ever sees, so a dialect-model field that is not
 * copied here is data that exists but is unreachable.
 */
class ResultSetAndReturnShapeBridgeTest {

    private final PostgresProcedureParser postgres = new PostgresProcedureParser();
    private final SqlServerProcedureParser sqlServer = new SqlServerProcedureParser();
    private final Db2ProcedureParser db2 = new Db2ProcedureParser();

    private static ParsedObject single(ParseResult r) {
        assertEquals(1, r.getObjects().size(), "expected exactly one parsed object");
        return r.getObjects().get(0);
    }

    // ── #25 Postgres DO block becomes an analyzable unit ───────────────────────

    @Test
    @DisplayName("#25 Postgres DO block is a DO_BLOCK unit whose table ops are captured")
    void postgresDoBlockIsAnalyzableUnit() {
        String src = "DO $$ BEGIN INSERT INTO erp.log(msg) VALUES ('hi'); END $$;";
        ParsedObject obj = single(postgres.parse(src, "migrate_1", "erp"));
        assertEquals("DO_BLOCK", obj.getType(),
                "a DO block must not masquerade as a PROCEDURE — it has no routine identity");
        assertEquals(1, obj.getTableOperations().size());
        assertEquals("log", obj.getTableOperations().get(0).getTableName());
    }

    // ── #26 Postgres: refcursor / SETOF return a result set ─────────────────────

    @Test
    @DisplayName("#26 Postgres RETURNS TABLE is a result-set-returning function")
    void postgresReturnsTableIsResultSet() {
        String src = """
                CREATE FUNCTION get_orders() RETURNS TABLE(order_id integer, total numeric) AS $$
                BEGIN
                  RETURN QUERY SELECT id, amount FROM orders;
                END;
                $$ LANGUAGE plpgsql;
                """;
        ParsedObject obj = single(postgres.parse(src, "get_orders", "sales"));
        assertTrue(obj.isReturnsResultSet(), "a RETURNS TABLE function hands a result set back");
        assertEquals(1, obj.getResultSets());
        assertEquals("TABLE", obj.getReturnType());
        assertEquals(List.of("order_id", "total"), obj.getReturnsColumns());
    }

    @Test
    @DisplayName("#26 Postgres RETURNS SETOF is a result-set-returning function")
    void postgresReturnsSetofIsResultSet() {
        String src = """
                CREATE FUNCTION all_ids() RETURNS SETOF integer AS $$
                BEGIN
                  RETURN QUERY SELECT id FROM t;
                END;
                $$ LANGUAGE plpgsql;
                """;
        ParsedObject obj = single(postgres.parse(src, "all_ids", null));
        assertTrue(obj.isReturnsResultSet());
        assertEquals("SETOF integer", obj.getReturnType());
        assertTrue(obj.getReturnsColumns().isEmpty(), "a SETOF scalar has no named columns");
    }

    @Test
    @DisplayName("#26 Postgres RETURNS refcursor is a result-set-returning function")
    void postgresReturnsRefcursorIsResultSet() {
        String src = """
                CREATE FUNCTION open_cur() RETURNS refcursor AS $$
                DECLARE c refcursor;
                BEGIN
                  OPEN c FOR SELECT id FROM t;
                  RETURN c;
                END;
                $$ LANGUAGE plpgsql;
                """;
        ParsedObject obj = single(postgres.parse(src, "open_cur", null));
        assertTrue(obj.isReturnsResultSet());
        assertEquals("refcursor", obj.getReturnType());
    }

    @Test
    @DisplayName("#26 Postgres a plain procedure returns no result set")
    void postgresPlainProcedureHasNoResultSet() {
        String src = """
                CREATE PROCEDURE do_work() AS $$
                BEGIN
                  INSERT INTO t(id) VALUES (1);
                END;
                $$ LANGUAGE plpgsql;
                """;
        ParsedObject obj = single(postgres.parse(src, "do_work", null));
        assertFalse(obj.isReturnsResultSet());
        assertEquals(0, obj.getResultSets());
        assertNull(obj.getReturnType());
    }

    // ── #26 SQL Server: count of statement-level bare SELECTs ───────────────────

    @Test
    @DisplayName("#26 SQL Server counts top-level bare SELECTs, ignoring the @var= assignment form")
    void sqlServerCountsBareSelects() {
        String src = """
                CREATE PROCEDURE dbo.report AS
                BEGIN
                  SELECT 1 AS a;
                  SELECT id FROM dbo.orders;
                  SELECT @x = 2;
                END
                """;
        ParsedObject obj = single(sqlServer.parse(src, "report", "dbo"));
        assertTrue(obj.isReturnsResultSet());
        assertEquals(2, obj.getResultSets(),
                "two bare SELECTs return result sets; SELECT @x = 2 is an assignment, not a result set");
    }

    @Test
    @DisplayName("#26 SQL Server a proc with no bare SELECT returns no result set")
    void sqlServerNoSelectHasNoResultSet() {
        String src = """
                CREATE PROCEDURE dbo.audit AS
                BEGIN
                  INSERT INTO dbo.log(msg) VALUES ('x');
                END
                """;
        ParsedObject obj = single(sqlServer.parse(src, "audit", "dbo"));
        assertFalse(obj.isReturnsResultSet());
        assertEquals(0, obj.getResultSets());
    }

    // ── #26 DB2: a cursor declared WITH RETURN ─────────────────────────────────

    @Test
    @DisplayName("#26 DB2 DECLARE ... CURSOR WITH RETURN is a result-set hand-back")
    void db2WithReturnCursorIsResultSet() {
        String src = """
                CREATE PROCEDURE list_orders()
                LANGUAGE SQL
                BEGIN
                  DECLARE c_orders CURSOR WITH RETURN TO CALLER FOR SELECT id FROM orders;
                  OPEN c_orders;
                END
                """;
        ParsedObject obj = single(db2.parse(src, "list_orders", null));
        assertTrue(obj.isReturnsResultSet(), "WITH RETURN hands the cursor's result set to the caller");
        assertEquals(1, obj.getResultSets());
    }

    @Test
    @DisplayName("#26 DB2 a plain cursor (no WITH RETURN) is not a result set")
    void db2PlainCursorIsNotResultSet() {
        String src = """
                CREATE PROCEDURE scan_orders()
                LANGUAGE SQL
                BEGIN
                  DECLARE c_orders CURSOR FOR SELECT id FROM orders;
                  OPEN c_orders;
                END
                """;
        ParsedObject obj = single(db2.parse(src, "scan_orders", null));
        assertFalse(obj.isReturnsResultSet());
    }

    // ── #27 SQL Server TVF return shape ────────────────────────────────────────

    @Test
    @DisplayName("#27 SQL Server multi-statement TVF persists RETURNS @t TABLE(...) columns")
    void sqlServerMultiStatementTvfReturnShape() {
        String src = """
                CREATE FUNCTION dbo.active_users() RETURNS @result TABLE(id INT, name VARCHAR(50), amount DECIMAL(18,2)) AS
                BEGIN
                  INSERT INTO @result SELECT id, name, amount FROM users WHERE active = 1;
                  RETURN;
                END
                """;
        ParsedObject obj = single(sqlServer.parse(src, "active_users", "dbo"));
        assertNotNull(obj.getReturnType());
        assertTrue(obj.getReturnType().startsWith("TABLE"), "returnType was: " + obj.getReturnType());
        assertEquals(List.of("id", "name", "amount"), obj.getReturnsColumns(),
                "the declared table variable's columns, in order (commas inside DECIMAL(18,2) must not split)");
    }

    @Test
    @DisplayName("#27 SQL Server inline TVF keeps returnType TABLE with no declared columns")
    void sqlServerInlineTvfReturnShape() {
        String src = """
                CREATE FUNCTION dbo.recent_orders() RETURNS TABLE AS
                RETURN (SELECT id, total FROM orders WHERE created_at > '2020-01-01')
                """;
        ParsedObject obj = single(sqlServer.parse(src, "recent_orders", "dbo"));
        assertEquals("TABLE", obj.getReturnType());
        assertTrue(obj.getReturnsColumns().isEmpty(),
                "an inline TVF's columns come from the RETURN projection, which is not a declared list");
    }

    // ── #27 Postgres return shape (already asserted above) ─────────────────────

    @Test
    @DisplayName("#27 Postgres RETURNS TABLE columns survive the bridge in declaration order")
    void postgresReturnsColumnsOrderPreserved() {
        String src = """
                CREATE FUNCTION f() RETURNS TABLE(b text, a integer, c numeric) AS $$
                BEGIN
                  RETURN QUERY SELECT b, a, c FROM t;
                END;
                $$ LANGUAGE plpgsql;
                """;
        ParsedObject obj = single(postgres.parse(src, "f", null));
        assertEquals(List.of("b", "a", "c"), obj.getReturnsColumns());
    }
}
