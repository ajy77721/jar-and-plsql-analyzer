package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlServerAnalysisEngineTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    // ── Routine type & name ───────────────────────────────────────────────────

    @Test
    void procedureName() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE usp_hire @p_name NVARCHAR(100)\nAS\nBEGIN\n" +
                "  INSERT INTO employees (name) VALUES (@p_name);\nEND");
        assertEquals("usp_hire", r.getName());
        assertEquals(SqlServerRoutineInfo.RoutineType.PROCEDURE, r.getType());
    }

    @Test
    void functionName() {
        SqlServerRoutineInfo r = parse(
                "CREATE FUNCTION fn_get_tax(@amount DECIMAL(10,2))\nRETURNS DECIMAL(10,2)\nAS\nBEGIN\n" +
                "  RETURN @amount * 0.1;\nEND");
        assertEquals("fn_get_tax", r.getName());
        assertEquals(SqlServerRoutineInfo.RoutineType.FUNCTION, r.getType());
    }

    @Test
    void orAlterParsed() {
        SqlServerRoutineInfo r = parse(
                "CREATE OR ALTER PROCEDURE usp_sync\nAS\nBEGIN\n  SELECT 1;\nEND");
        assertEquals("usp_sync", r.getName());
    }

    @Test
    void schemaQualifiedName() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE dbo.usp_get_orders\nAS\nBEGIN\n  SELECT * FROM orders;\nEND");
        assertEquals("usp_get_orders", r.getName());
        assertEquals("dbo", r.getSchema());
    }

    // ── Parameters ────────────────────────────────────────────────────────────

    @Test
    void inParameter() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE usp_find\n@p_id INT\nAS\nBEGIN\n  SELECT * FROM emp WHERE id = @p_id;\nEND");
        assertEquals(1, r.getParams().size());
        assertEquals("@p_id", r.getParams().get(0).getName());
        assertEquals(SqlServerParamInfo.Direction.IN, r.getParams().get(0).getDirection());
    }

    @Test
    void outputParameter() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE usp_count\n@p_total INT OUTPUT\nAS\nBEGIN\n" +
                "  SELECT @p_total = COUNT(*) FROM employees;\nEND");
        assertEquals(1, r.getParams().size());
        assertEquals(SqlServerParamInfo.Direction.OUT, r.getParams().get(0).getDirection());
    }

    @Test
    void multipleParameters() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE usp_transfer\n" +
                "@p_from INT, @p_to INT, @p_amount DECIMAL(10,2), @p_status NVARCHAR(20) OUTPUT\n" +
                "AS\nBEGIN\n  SELECT 1;\nEND");
        assertEquals(4, r.getParams().size());
    }

    // ── EXEC calls ────────────────────────────────────────────────────────────

    @Test
    void execCallDetected() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE daily_sync\nAS\nBEGIN\n  EXEC process_orders;\nEND");
        assertEquals(1, r.getCalls().size());
        assertEquals("process_orders", r.getCalls().get(0).getName());
    }

    @Test
    void executeCallDetected() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE daily_sync\nAS\nBEGIN\n  EXECUTE update_inventory;\nEND");
        assertTrue(r.getCalls().stream().anyMatch(c -> "update_inventory".equals(c.getName())));
    }

    @Test
    void schemaQualifiedExec() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE test\nAS\nBEGIN\n  EXEC dbo.some_proc;\nEND");
        assertTrue(r.getCalls().stream().anyMatch(c ->
                "some_proc".equals(c.getName()) && "dbo".equals(c.getSchema())));
    }

    @Test
    void multipleExecCalls() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE full_sync\nAS\nBEGIN\n" +
                "  EXEC process_orders;\n  EXEC update_inventory;\n  EXEC log_audit;\n" +
                "END");
        assertEquals(3, r.getCalls().size());
    }

    // ── DML table operations ──────────────────────────────────────────────────

    @Test
    void insertTableOp() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE usp_hire\nAS\nBEGIN\n" +
                "  INSERT INTO employees (name) VALUES ('Alice');\nEND");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT
                && "employees".equals(o.getTableName())));
    }

    @Test
    void updateTableOp() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE usp_update\nAS\nBEGIN\n" +
                "  UPDATE employees SET status = 'ACTIVE' WHERE id = 1;\nEND");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE
                && "employees".equals(o.getTableName())));
    }

    @Test
    void deleteTableOp() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE cleanup\nAS\nBEGIN\n" +
                "  DELETE FROM audit_log WHERE log_date < GETDATE();\nEND");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == SqlServerTableOpInfo.DmlType.DELETE
                && "audit_log".equals(o.getTableName())));
    }

    @Test
    void selectTableOp() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE list_orders\nAS\nBEGIN\n" +
                "  SELECT order_id FROM orders WHERE status = 'OPEN';\nEND");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT));
    }

    // ── DECLARE @variable ─────────────────────────────────────────────────────

    @Test
    void declareVariableExtracted() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE test\nAS\nBEGIN\n" +
                "  DECLARE @v_count INT;\n  DECLARE @v_name NVARCHAR(100);\n" +
                "  SELECT @v_count = 1;\nEND");
        assertTrue(r.getVariables().stream().anyMatch(v -> "@v_count".equals(v.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "@v_name".equals(v.getName())));
    }

    // ── Dynamic SQL ───────────────────────────────────────────────────────────

    @Test
    void dynamicSql_setSqlDetected() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE dyn_test\nAS\nBEGIN\n" +
                "  DECLARE @v_sql NVARCHAR(MAX);\n" +
                "  SET @v_sql = 'UPDATE employees SET salary = salary * 1.1 WHERE dept_id = 1';\n" +
                "  EXEC sp_executesql @v_sql;\nEND");
        assertFalse(r.getDynamicSql().isEmpty(), "expected dynamic SQL entry");
        assertTrue(r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.SET_SQL));
    }

    @Test
    void dynamicSql_spExecuteSqlDetected() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE dyn_test\nAS\nBEGIN\n" +
                "  DECLARE @v_sql NVARCHAR(MAX);\n" +
                "  SET @v_sql = N'SELECT 1';\n" +
                "  EXECUTE sp_executesql @v_sql;\nEND");
        assertTrue(r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_SP_EXECUTESQL));
    }

    /** Bug regression: EXEC(@sql) and EXEC sp_executesql @sql are structurally different
     *  mechanisms (plain string execution vs parameterized) and must not share a type. */
    @Test
    void dynamicSql_execStringAndSpExecuteSqlHaveDifferentTypes() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE dyn_test\nAS\nBEGIN\n" +
                "  DECLARE @v_sql NVARCHAR(MAX);\n" +
                "  SET @v_sql = 'SELECT 1';\n" +
                "  EXEC(@v_sql);\n" +
                "  EXEC sp_executesql @v_sql;\nEND");

        assertTrue(r.getDynamicSql().stream()
                        .anyMatch(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_STRING),
                "expected an EXEC_STRING entry for EXEC(@v_sql): " + r.getDynamicSql());
        assertTrue(r.getDynamicSql().stream()
                        .anyMatch(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_SP_EXECUTESQL),
                "expected an EXEC_SP_EXECUTESQL entry for EXEC sp_executesql: " + r.getDynamicSql());
        assertTrue(r.getDynamicSql().stream()
                        .noneMatch(d -> d.getKind() == SqlServerDynamicSqlInfo.Kind.EXEC_STRING
                                && d.getLine() != 6),
                "EXEC_STRING should be captured at the EXEC(@v_sql) line");
    }

    // ── Cursors ───────────────────────────────────────────────────────────────

    /** Bug regression: every cursor lifecycle statement (OPEN/FETCH/CLOSE/DEALLOCATE), not just
     *  DECLARE, must be tracked against the matching cursor by name. */
    @Test
    void cursor_fullLifecycleTracked() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE cursor_test\nAS\nBEGIN\n" +
                "  DECLARE @v_id INT;\n" +
                "  DECLARE cur5 CURSOR FOR SELECT id FROM employees;\n" +
                "  OPEN cur5;\n" +
                "  FETCH NEXT FROM cur5 INTO @v_id;\n" +
                "  FETCH NEXT FROM cur5 INTO @v_id;\n" +
                "  CLOSE cur5;\n" +
                "  DEALLOCATE cur5;\n" +
                "END");

        assertEquals(1, r.getCursors().size());
        SqlServerCursorInfo cur = r.getCursors().get(0);
        assertEquals("cur5", cur.getName());
        assertEquals(5, cur.getLine(), "DECLARE itself is tracked via the cursor's own line");

        List<String> ops = cur.getStmtOperations().stream()
                .map(SqlServerCursorInfo.CursorStmtOp::getOperation)
                .collect(java.util.stream.Collectors.toList());

        assertTrue(ops.contains("OPEN"), "expected OPEN: " + ops);
        assertTrue(ops.contains("FETCH"), "expected FETCH: " + ops);
        assertTrue(ops.contains("CLOSE"), "expected CLOSE: " + ops);
        assertTrue(ops.contains("DEALLOCATE"), "expected DEALLOCATE: " + ops);

        // Two FETCH NEXT statements against the same cursor → two FETCH entries.
        long fetchCount = ops.stream().filter("FETCH"::equals).count();
        assertEquals(2, fetchCount, "expected 2 FETCH entries: " + ops);

        // Lines should be in source order: DECLARE(5) < OPEN(6) < FETCH(7) < FETCH(8) < CLOSE(9) < DEALLOCATE(10)
        List<Integer> lines = cur.getStmtOperations().stream()
                .map(SqlServerCursorInfo.CursorStmtOp::getLine)
                .collect(java.util.stream.Collectors.toList());
        for (int i = 1; i < lines.size(); i++) {
            assertTrue(lines.get(i) >= lines.get(i - 1),
                    "expected non-decreasing line order: " + lines);
        }
    }

    // ── Body extraction ───────────────────────────────────────────────────────

    @Test
    void extractBody_returnsContentBetweenBeginEnd() {
        String src = "CREATE PROCEDURE p\nAS\nBEGIN\n  SELECT 1;\nEND";
        String body = engine.extractBody(src);
        assertNotNull(body);
        assertTrue(body.contains("SELECT 1"));
    }

    @Test
    void extractBody_noAS_returnsNull() {
        assertNull(engine.extractBody("SELECT 1;"));
    }

    // ── Smoke test ────────────────────────────────────────────────────────────

    @Test
    void complexProc_noEngineErrors() {
        String src =
                "CREATE PROCEDURE usp_daily_sync\n" +
                "@p_mode NVARCHAR(20)\n" +
                "AS\nBEGIN\n" +
                "  DECLARE @v_mode NVARCHAR(20);\n" +
                "  SET @v_mode = UPPER(LTRIM(RTRIM(ISNULL(@p_mode, 'FULL'))));\n" +
                "  EXEC log_audit 'START', 'SYSTEM', NULL;\n" +
                "  IF @v_mode IN ('FULL', 'ORDERS')\n" +
                "  BEGIN\n" +
                "    EXEC process_all_orders;\n" +
                "  END\n" +
                "  IF @v_mode = 'FULL'\n" +
                "  BEGIN\n" +
                "    EXEC generate_full_report;\n" +
                "  END\n" +
                "  EXEC log_audit 'DONE', 'SYSTEM', NULL;\n" +
                "END";

        SqlServerRoutineInfo r = engine.parse(src, "usp_daily_sync", null);
        assertEquals("usp_daily_sync", r.getName());
        assertEquals(1, r.getParams().size());
        assertTrue(r.getCalls().size() >= 3);
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── Transaction control (COMMIT / ROLLBACK / SAVEPOINT) ────────────────────

    @Test
    void transactionControlStatements() {
        SqlServerRoutineInfo r = parse(
                "CREATE PROCEDURE usp_txn\nAS\nBEGIN\n"
              + "  BEGIN TRANSACTION;\n"
              + "  SAVE TRANSACTION sp1;\n"
              + "  UPDATE accounts SET bal = bal - 1;\n"
              + "  ROLLBACK TRANSACTION sp1;\n"
              + "  COMMIT TRANSACTION;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        java.util.List<SqlServerStatementInfo> txn = r.getStatements();
        assertTrue(txn.stream().anyMatch(s -> "COMMIT".equals(s.getType())), "expected COMMIT");
        assertTrue(txn.stream().anyMatch(s ->
                "ROLLBACK".equals(s.getType()) && "sp1".equals(s.getSavepointName())),
                "expected ROLLBACK TRANSACTION sp1");
        assertTrue(txn.stream().anyMatch(s ->
                "SAVEPOINT".equals(s.getType()) && "sp1".equals(s.getSavepointName())),
                "expected SAVE TRANSACTION sp1");
    }
}
