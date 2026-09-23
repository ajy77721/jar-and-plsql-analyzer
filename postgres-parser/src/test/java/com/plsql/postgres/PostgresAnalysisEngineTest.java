package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PostgresAnalysisEngineTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    // ── Routine type & name ───────────────────────────────────────────────────

    @Test
    void procedureName() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE emp_hire(IN p_name VARCHAR(100))\nLANGUAGE SQL\nBEGIN END");
        assertEquals("emp_hire", r.getName());
        assertEquals(PostgresRoutineInfo.RoutineType.PROCEDURE, r.getType());
    }

    @Test
    void functionName() {
        PostgresRoutineInfo r = parse(
                "CREATE FUNCTION calc_tax(IN amount DECIMAL(10,2))\nRETURNS DECIMAL(10,2)\n" +
                "LANGUAGE SQL\nBEGIN ATOMIC\n  RETURN amount * 0.1;\nEND");
        assertEquals("calc_tax", r.getName());
        assertEquals(PostgresRoutineInfo.RoutineType.FUNCTION, r.getType());
    }

    @Test
    void orReplaceParsed() {
        PostgresRoutineInfo r = parse(
                "CREATE OR REPLACE PROCEDURE update_emp(IN p_id INT)\nLANGUAGE SQL\nBEGIN END");
        assertEquals("update_emp", r.getName());
    }

    @Test
    void schemaQualifiedName() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE hrschema.emp_promote(IN p_id INT)\nLANGUAGE SQL\nBEGIN END");
        assertEquals("emp_promote", r.getName());
        assertEquals("hrschema", r.getSchema());
    }

    @Test
    void fallbackName_whenSignatureAbsent() {
        PostgresRoutineInfo r = engine.parse("-- nothing here\nSELECT 1 FROM SYSIBM.SYSDUMMY1;",
                "my_fallback", null);
        assertEquals("my_fallback", r.getName());
    }

    // ── Parameters ────────────────────────────────────────────────────────────

    @Test
    void inParameter() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE find_emp(IN p_id INT)\nLANGUAGE SQL\n" +
                "BEGIN\n  SELECT * FROM employees WHERE emp_id = p_id;\nEND");
        assertEquals(1, r.getParams().size());
        assertEquals("p_id", r.getParams().get(0).getName());
        assertEquals(PostgresParamInfo.Direction.IN, r.getParams().get(0).getDirection());
    }

    @Test
    void outParameter() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE get_count(OUT p_total INT)\nLANGUAGE SQL\n" +
                "BEGIN\n  SELECT COUNT(*) INTO p_total FROM employees;\nEND");
        assertEquals(1, r.getParams().size());
        assertEquals(PostgresParamInfo.Direction.OUT, r.getParams().get(0).getDirection());
    }

    @Test
    void inoutParameter() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE adjust(INOUT p_amount DECIMAL(10,2))\nLANGUAGE SQL\n" +
                "BEGIN\n  SET p_amount = p_amount * 1.1;\nEND");
        assertEquals(PostgresParamInfo.Direction.INOUT, r.getParams().get(0).getDirection());
    }

    @Test
    void multipleParameters() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE transfer(\n" +
                "  IN p_from INT, IN p_to INT, INOUT p_amount DECIMAL(10,2), OUT p_status VARCHAR(20))\n" +
                "LANGUAGE SQL\nBEGIN END");
        assertEquals(4, r.getParams().size());
    }

    // ── CALL statements ───────────────────────────────────────────────────────

    @Test
    void callDetected() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE daily_sync()\nLANGUAGE SQL\nBEGIN\n" +
                "  CALL process_orders();\nEND");
        assertEquals(1, r.getCalls().size());
        assertEquals("process_orders", r.getCalls().get(0).getName());
    }

    @Test
    void multipleCallsDetected() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE full_sync()\nLANGUAGE SQL\nBEGIN\n" +
                "  CALL process_orders();\n  CALL update_inventory();\n  CALL log_audit('SYNC');\n" +
                "END");
        assertEquals(3, r.getCalls().size());
    }

    @Test
    void schemaQualifiedCall() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE SQL\nBEGIN\n" +
                "  CALL hrschema.emp_promote(1);\nEND");
        assertEquals(1, r.getCalls().size());
        assertEquals("emp_promote", r.getCalls().get(0).getName());
        assertEquals("hrschema", r.getCalls().get(0).getSchema());
    }

    @Test
    void callInsideIF() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test(IN p_mode VARCHAR(10))\nLANGUAGE SQL\nBEGIN\n" +
                "  IF p_mode = 'FULL' THEN\n    CALL run_full_sync();\n  END IF;\nEND");
        assertTrue(r.getCalls().stream().anyMatch(c -> "run_full_sync".equals(c.getName())));
    }

    // ── DML table operations ──────────────────────────────────────────────────

    @Test
    void insertTableOp() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE emp_hire(IN p_name VARCHAR(100))\nLANGUAGE SQL\nBEGIN\n" +
                "  INSERT INTO employees (emp_name) VALUES (p_name);\nEND");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == PostgresTableOpInfo.DmlType.INSERT && "employees".equals(o.getTableName())));
    }

    @Test
    void updateTableOp() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE emp_terminate(IN p_id INT)\nLANGUAGE SQL\nBEGIN\n" +
                "  UPDATE employees SET status = 'TERM' WHERE emp_id = p_id;\nEND");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == PostgresTableOpInfo.DmlType.UPDATE && "employees".equals(o.getTableName())));
    }

    @Test
    void deleteTableOp() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE cleanup()\nLANGUAGE SQL\nBEGIN\n" +
                "  DELETE FROM audit_log WHERE log_date < CURRENT DATE;\nEND");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == PostgresTableOpInfo.DmlType.DELETE && "audit_log".equals(o.getTableName())));
    }

    @Test
    void selectTableOp() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE list_orders()\nLANGUAGE SQL\nBEGIN\n" +
                "  SELECT order_id FROM orders WHERE status = 'OPEN';\nEND");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == PostgresTableOpInfo.DmlType.SELECT));
    }

    @Test
    void joinDetectsTables() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE join_test()\nLANGUAGE SQL\nBEGIN\n" +
                "  SELECT o.order_id, e.emp_name FROM orders o " +
                "JOIN employees e ON e.emp_id = o.emp_id;\nEND");
        assertFalse(r.getTableOps().isEmpty(), "expected at least one SELECT op");
    }

    // ── DECLARE variable / cursor ─────────────────────────────────────────────

    @Test
    void declareVariableExtracted() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE v_count INT DEFAULT 0;\n  DECLARE v_name VARCHAR(100);\nEND");
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_count".equals(v.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_name".equals(v.getName())));
    }

    @Test
    void declareVariableWithDefault() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE v_status VARCHAR(20) DEFAULT 'PENDING';\nEND");
        assertTrue(r.getVariables().stream()
                .anyMatch(v -> "v_status".equals(v.getName()) && v.getDefaultValue() != null));
    }

    @Test
    void declareCursorExtracted() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE cursor_test()\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE cur_emps CURSOR FOR SELECT emp_id FROM employees;\n" +
                "  OPEN cur_emps;\n  CLOSE cur_emps;\nEND");
        assertEquals(1, r.getCursors().size());
        assertEquals("cur_emps", r.getCursors().get(0).getName());
    }

    @Test
    void declareCursorWithHold() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE cur CURSOR WITH HOLD FOR SELECT id FROM t;\nEND");
        assertTrue(r.getCursors().stream().anyMatch(c -> "cur".equals(c.getName()) && c.isWithHold()));
    }

    @Test
    void declareCursorForPreparedStmt_noErrors() {
        // POSTGRES: DECLARE c CURSOR FOR <prepared-statement-name> (from PREPARE)
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE prep_cursor(IN p_dept INT)\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE v_sql VARCHAR(500);\n" +
                "  SET v_sql = 'SELECT emp_id FROM employees WHERE dept_id = ?';\n" +
                "  PREPARE my_prepared_stmt FROM v_sql;\n" +
                "  DECLARE c CURSOR FOR my_prepared_stmt;\n" +
                "  OPEN c;\n  CLOSE c;\nEND");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        assertTrue(r.getCursors().stream().anyMatch(cu -> "c".equals(cu.getName())));
    }

    // ── CAST in CALL arguments ────────────────────────────────────────────────

    @Test
    void castInCallArgs_noErrors() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE cast_call(IN v VARCHAR(10), IN x DECIMAL(10,2))\nLANGUAGE SQL\nBEGIN\n" +
                "  CALL foo(CAST(v AS INTEGER), CAST(x AS DECIMAL(10,2)));\nEND");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        assertTrue(r.getCalls().stream().anyMatch(c -> "foo".equals(c.getName())));
    }

    @Test
    void castInSetAndInsert_stillWorks() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE cast_reg(IN p VARCHAR(10))\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE v_n INT;\n" +
                "  SET v_n = CAST(p AS INTEGER);\n" +
                "  INSERT INTO t (n) VALUES (CAST(p AS INTEGER));\nEND");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── Dynamic SQL ───────────────────────────────────────────────────────────

    @Test
    void prepareAndExecute_detected() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE dyn_test(IN p_dept INT)\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE v_sql VARCHAR(500);\n" +
                "  SET v_sql = 'UPDATE employees SET salary = salary * 1.1 WHERE dept_id = 1';\n" +
                "  PREPARE stmt1 FROM v_sql;\n  EXECUTE stmt1;\nEND");
        assertTrue(r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == PostgresDynamicSqlInfo.Kind.PREPARE));
        assertTrue(r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == PostgresDynamicSqlInfo.Kind.EXECUTE));
    }

    @Test
    void executeImmediate_detected() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE dyn_imm()\nLANGUAGE SQL\nBEGIN\n" +
                "  EXECUTE IMMEDIATE 'DELETE FROM temp_data WHERE expired = 1';\nEND");
        assertTrue(r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == PostgresDynamicSqlInfo.Kind.EXECUTE_IMMEDIATE));
    }

    // ── Bug regression: EXECUTE's dynamic SQL text/expression must not be dropped ─────────────

    @Test
    void executeFormat_capturesSqlExpressionText() {
        // Mirrors erp.erp_usp_stage_01's real EXECUTE format('SELECT count(*) FROM erp.%I', v_tab)
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE stage_01()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE v_tab VARCHAR(100);\n  DECLARE v_cnt INT;\n" +
                "  EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;\nEND");
        PostgresDynamicSqlInfo exec = r.getDynamicSql().stream()
                .filter(d -> d.getKind() == PostgresDynamicSqlInfo.Kind.EXECUTE)
                .findFirst().orElseThrow(() -> new AssertionError("no EXECUTE captured"));
        assertNotNull(exec.getSqlSource(), "sqlSource must not be null");
        assertFalse(exec.getSqlSource().isBlank(), "sqlSource must not be blank");
        assertTrue(exec.getSqlSource().contains("format"), "expected the format(...) call text, got: " + exec.getSqlSource());
        assertTrue(exec.getSqlSource().contains("v_tab"), "expected the bound variable name, got: " + exec.getSqlSource());
    }

    @Test
    void executeWithUsing_capturesSqlAndUsingExprs() {
        // Mirrors erp.erp_usp_stage_01's real EXECUTE '...' USING 0
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE stage_02()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE status = $1' USING 0;\nEND");
        PostgresDynamicSqlInfo exec = r.getDynamicSql().stream()
                .filter(d -> d.getKind() == PostgresDynamicSqlInfo.Kind.EXECUTE)
                .findFirst().orElseThrow(() -> new AssertionError("no EXECUTE captured"));
        assertNotNull(exec.getSqlSource());
        assertTrue(exec.getSqlSource().contains("SELECT count(*) FROM erp.erp_demand"),
                "expected the real SQL text, got: " + exec.getSqlSource());
        assertEquals(1, exec.getUsingExprs().size());
        assertEquals("0", exec.getUsingExprs().get(0));
    }

    // ── Bug regression: cursor lifecycle must record OPEN/FETCH/CLOSE, not just DECLARE ──────

    @Test
    void cursorLifecycle_recordsOpenFetchClose() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE stage_01()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE v_rec RECORD;\n" +
                "  DECLARE c1 CURSOR FOR SELECT id FROM erp.erp_product;\n" +
                "  OPEN c1;\n" +
                "  FETCH c1 INTO v_rec;\n" +
                "  CLOSE c1;\nEND");
        PostgresCursorInfo c1 = r.getCursors().stream()
                .filter(c -> "c1".equalsIgnoreCase(c.getName()))
                .findFirst().orElseThrow(() -> new AssertionError("cursor c1 not captured"));

        List<String> kinds = c1.getOperations().stream()
                .map(PostgresCursorInfo.Op::getKind).toList();
        assertTrue(kinds.contains("OPEN"), "expected OPEN, got: " + kinds);
        assertTrue(kinds.contains("FETCH"), "expected FETCH, got: " + kinds);
        assertTrue(kinds.contains("CLOSE"), "expected CLOSE, got: " + kinds);

        for (PostgresCursorInfo.Op op : c1.getOperations()) {
            assertTrue(op.getLine() > c1.getLine(),
                    "expected " + op.getKind() + " line (" + op.getLine()
                            + ") after DECLARE line (" + c1.getLine() + ")");
        }
    }

    // ── Nested BEGIN ATOMIC ───────────────────────────────────────────────────

    @Test
    void nestedBeginAtomic_callsExtracted() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE nested()\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE v_id INT;\n" +
                "  BEGIN ATOMIC\n" +
                "    INSERT INTO orders (status) VALUES ('NEW');\n" +
                "    CALL log_event('created');\n" +
                "  END;\nEND");
        assertTrue(r.getCalls().stream().anyMatch(c -> "log_event".equals(c.getName())));
    }

    // ── Body extraction ───────────────────────────────────────────────────────

    @Test
    void extractBody_contentBetweenBeginEnd() {
        String src = "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN\n  SELECT 1 FROM SYSIBM.SYSDUMMY1;\nEND";
        String body = engine.extractBody(src);
        assertNotNull(body);
        assertTrue(body.contains("SELECT 1"));
    }

    @Test
    void extractBody_noBegin_returnsNull() {
        assertNull(engine.extractBody("SELECT 1 FROM SYSIBM.SYSDUMMY1;"));
    }

    // ── No errors ─────────────────────────────────────────────────────────────

    @Test
    void complexProc_noEngineErrors() {
        String src =
                "CREATE PROCEDURE proc_daily_sync(IN p_mode VARCHAR(20))\nLANGUAGE SQL\nBEGIN\n" +
                "  DECLARE v_mode VARCHAR(20);\n" +
                "  DECLARE v_start TIMESTAMP;\n" +
                "  SET v_mode = UPPER(TRIM(COALESCE(p_mode, 'FULL')));\n" +
                "  SET v_start = CURRENT TIMESTAMP;\n" +
                "  CALL log_audit('START', 'SYSTEM', NULL);\n" +
                "  IF v_mode IN ('FULL', 'ORDERS') THEN\n" +
                "    CALL process_all_orders();\n" +
                "  END IF;\n" +
                "  IF v_mode = 'FULL' THEN\n" +
                "    CALL generate_full_report();\n" +
                "  END IF;\n" +
                "  CALL log_audit('DONE', 'SYSTEM', NULL);\n" +
                "END";

        PostgresRoutineInfo r = engine.parse(src, "proc_daily_sync", null);
        assertEquals("proc_daily_sync", r.getName());
        assertEquals(1, r.getParams().size());
        assertTrue(r.getCalls().size() >= 3);
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── Transaction control (COMMIT / ROLLBACK / SAVEPOINT) ────────────────────

    @Test
    void transactionControlStatements() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE txn_demo()\nLANGUAGE SQL\nBEGIN\n"
              + "  SAVEPOINT sp1 ON ROLLBACK RETAIN CURSORS;\n"
              + "  UPDATE accounts SET bal = bal - 1;\n"
              + "  ROLLBACK TO SAVEPOINT sp1;\n"
              + "  COMMIT;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        List<PostgresStatementInfo> txn = r.getStatements();
        assertTrue(txn.stream().anyMatch(s -> "COMMIT".equals(s.getType())), "expected COMMIT");
        assertTrue(txn.stream().anyMatch(s ->
                "ROLLBACK".equals(s.getType()) && "sp1".equals(s.getSavepointName())),
                "expected ROLLBACK TO SAVEPOINT sp1");
        assertTrue(txn.stream().anyMatch(s ->
                "SAVEPOINT".equals(s.getType()) && "sp1".equals(s.getSavepointName())),
                "expected SAVEPOINT sp1");
    }

    // ── Sequences (audit #8) ────────────────────────────────────────────────────

    @Test
    void nextvalSequenceCaptured() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE bump_order()\nLANGUAGE SQL\nBEGIN\n"
              + "  SELECT nextval('erp.order_seq');\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        assertEquals(1, r.getSequences().size());
        PostgresSequenceInfo seq = r.getSequences().get(0);
        assertEquals("erp.order_seq", seq.getSequenceName());
        assertEquals(PostgresSequenceInfo.Operation.NEXTVAL, seq.getOperation());
        assertTrue(seq.getLine() > 0);
    }

    // ── Exception handlers (audit #9) ───────────────────────────────────────────

    @Test
    void exceptionHandlerCaptured() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE p1()\nLANGUAGE SQL\nBEGIN\n"
              + "  INSERT INTO t1 VALUES (1);\n"
              + "EXCEPTION WHEN division_by_zero THEN\n"
              + "  ROLLBACK;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        assertEquals(1, r.getExceptionHandlers().size());
        PostgresExceptionHandlerInfo eh = r.getExceptionHandlers().get(0);
        assertEquals("division_by_zero", eh.getExceptionName());
        assertEquals(1, eh.getStatementsCount());
    }

    // ── DO-blocks (audit #20) ────────────────────────────────────────────────────

    @Test
    void doBlockParsesAndCapturesTableOp() {
        PostgresRoutineInfo r = parse(
                "DO $$ BEGIN INSERT INTO erp.log(msg) VALUES ('hi'); END $$;");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        assertEquals(1, r.getTableOps().size());
        PostgresTableOpInfo op = r.getTableOps().get(0);
        assertEquals("log", op.getTableName());
        assertEquals("erp", op.getSchemaName());
        assertEquals(PostgresTableOpInfo.DmlType.INSERT, op.getDmlType());
    }

    // ── RETURNING clause (audit #20) ────────────────────────────────────────────

    @Test
    void insertReturningColumnCaptured() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE add_order()\nLANGUAGE SQL\nBEGIN\n"
              + "  INSERT INTO erp.orders(id) VALUES (1) RETURNING id;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
        assertEquals(1, r.getTableOps().size());
        PostgresTableOpInfo op = r.getTableOps().get(0);
        assertEquals(PostgresTableOpInfo.DmlType.INSERT, op.getDmlType());
        assertEquals(List.of("id"), op.getReturningColumns());
    }
}
