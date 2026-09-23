package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MySqlGrammarEngineTest {

    private final MySqlGrammarEngine engine = new MySqlGrammarEngine();

    private MySqlRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    // ── Routine type & name ───────────────────────────────────────────────────

    @Test
    void procedureName() {
        MySqlRoutineInfo r = parse("CREATE PROCEDURE emp_hire(IN p_name VARCHAR(100)) BEGIN END");
        assertEquals("emp_hire", r.getName());
        assertEquals(MySqlRoutineInfo.RoutineType.PROCEDURE, r.getType());
    }

    @Test
    void functionName() {
        MySqlRoutineInfo r = parse(
            "CREATE FUNCTION emp_get_name(p_id INT) RETURNS VARCHAR(100) READS SQL DATA BEGIN END");
        assertEquals("emp_get_name", r.getName());
        assertEquals(MySqlRoutineInfo.RoutineType.FUNCTION, r.getType());
    }

    @Test
    void definerStripped() {
        MySqlRoutineInfo r = parse(
            "CREATE DEFINER=`root`@`localhost` PROCEDURE utils_log() BEGIN END");
        assertEquals("utils_log", r.getName());
    }

    @Test
    void schemaQualifiedName() {
        MySqlRoutineInfo r = parse("CREATE PROCEDURE testdb.proc_daily_sync() BEGIN END");
        assertEquals("proc_daily_sync", r.getName());
        assertEquals("testdb", r.getSchema());
    }

    // ── Parameters ────────────────────────────────────────────────────────────

    @Test
    void inParameter() {
        MySqlRoutineInfo r = parse("CREATE PROCEDURE emp_hire(IN p_name VARCHAR(100)) BEGIN END");
        assertEquals(1, r.getParams().size());
        assertEquals("p_name", r.getParams().get(0).getName());
        assertEquals(MySqlParamInfo.Direction.IN, r.getParams().get(0).getDirection());
    }

    @Test
    void outParameter() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE emp_hire(IN p_name VARCHAR(100), OUT p_id INT) BEGIN END");
        assertEquals(2, r.getParams().size());
        assertEquals(MySqlParamInfo.Direction.OUT, r.getParams().get(1).getDirection());
        assertEquals("p_id", r.getParams().get(1).getName());
    }

    @Test
    void inoutParameter() {
        MySqlRoutineInfo r = parse("CREATE PROCEDURE test(INOUT p_val DECIMAL(10,2)) BEGIN END");
        assertEquals(MySqlParamInfo.Direction.INOUT, r.getParams().get(0).getDirection());
        assertEquals("p_val", r.getParams().get(0).getName());
    }

    @Test
    void multipleParameters() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE ord_create(" +
            "  IN p_emp_id INT, IN p_product_id INT, IN p_qty INT, OUT p_order_id INT" +
            ") BEGIN END");
        assertEquals(4, r.getParams().size());
    }

    // ── CALL statements ───────────────────────────────────────────────────────

    @Test
    void callDetected() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE proc_daily_sync() BEGIN " +
            "  CALL ord_process_all_pending(); " +
            "END");
        assertEquals(1, r.getCalls().size());
        assertEquals("ord_process_all_pending", r.getCalls().get(0).getName());
    }

    @Test
    void multipleCallsDetected() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE rpt_generate_full() BEGIN " +
            "  CALL ord_process_all_pending(); " +
            "  CALL inv_low_stock_report(); " +
            "  CALL utils_log_audit('RPT', 'SYSTEM', NULL, 'done'); " +
            "END");
        assertEquals(3, r.getCalls().size());
    }

    @Test
    void callInsideIfDetected() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE proc_daily_sync(IN p_mode VARCHAR(20)) BEGIN " +
            "  IF p_mode = 'FULL' THEN " +
            "    CALL rpt_generate_full(); " +
            "  END IF; " +
            "END");
        assertEquals(1, r.getCalls().size());
        assertEquals("rpt_generate_full", r.getCalls().get(0).getName());
    }

    @Test
    void callInsideWhileDetected() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE loop_test() BEGIN " +
            "  DECLARE v_done INT DEFAULT FALSE; " +
            "  WHILE NOT v_done DO " +
            "    CALL emp_process_department(1); " +
            "    SET v_done = TRUE; " +
            "  END WHILE; " +
            "END");
        assertEquals(1, r.getCalls().size());
        assertEquals("emp_process_department", r.getCalls().get(0).getName());
    }

    @Test
    void schemaQualifiedCall() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE test() BEGIN CALL mydb.some_proc(); END");
        assertEquals(1, r.getCalls().size());
        assertEquals("some_proc", r.getCalls().get(0).getName());
        assertEquals("mydb", r.getCalls().get(0).getSchema());
    }

    // ── DML table operations ──────────────────────────────────────────────────

    @Test
    void insertTableOp() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE emp_hire() BEGIN " +
            "  INSERT INTO employees (emp_name, dept_id) VALUES ('Alice', 10); " +
            "END");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == MySqlTableOpInfo.DmlType.INSERT
                && "employees".equals(o.getTableName())));
    }

    @Test
    void updateTableOp() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE emp_terminate(IN p_id INT) BEGIN " +
            "  UPDATE employees SET status = 'TERMINATED' WHERE emp_id = p_id; " +
            "END");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == MySqlTableOpInfo.DmlType.UPDATE
                && "employees".equals(o.getTableName())));
    }

    @Test
    void updateJoinMultiTableParsesAndExtractsTables() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE emp_sync() BEGIN " +
            "  UPDATE employees e JOIN departments d ON d.dept_id = e.dept_id " +
            "    SET e.dept_name = d.dept_name WHERE e.active = 1; " +
            "END");
        assertTrue(r.getErrors().isEmpty(),
                "unexpected parse errors: " + r.getErrors());
        // Target table captured as UPDATE
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == MySqlTableOpInfo.DmlType.UPDATE
                && "employees".equals(o.getTableName())),
                "expected employees as UPDATE target");
        // Joined table captured (as a source op)
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                "departments".equals(o.getTableName())),
                "expected joined departments table captured");
    }

    @Test
    void singleTableUpdateStillParses() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE emp_terminate(IN p_id INT) BEGIN " +
            "  UPDATE employees SET status = 'TERMINATED' WHERE emp_id = p_id; " +
            "END");
        assertTrue(r.getErrors().isEmpty(),
                "unexpected parse errors: " + r.getErrors());
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == MySqlTableOpInfo.DmlType.UPDATE
                && "employees".equals(o.getTableName())));
    }

    @Test
    void deleteTableOp() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE cleanup() BEGIN " +
            "  DELETE FROM audit_log WHERE changed_at < NOW(); " +
            "END");
        assertTrue(r.getTableOps().stream().anyMatch(o ->
                o.getDmlType() == MySqlTableOpInfo.DmlType.DELETE
                && "audit_log".equals(o.getTableName())));
    }

    @Test
    void selectCursorExtractsTables() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE emp_process_department(IN p_dept INT) BEGIN " +
            "  DECLARE cur_active CURSOR FOR " +
            "    SELECT e.emp_id, d.dept_name " +
            "    FROM employees e JOIN departments d ON d.dept_id = e.dept_id " +
            "    WHERE e.dept_id = p_dept; " +
            "END");
        long selectOps = r.getTableOps().stream()
                .filter(o -> o.getDmlType() == MySqlTableOpInfo.DmlType.SELECT)
                .count();
        assertTrue(selectOps >= 1);
    }

    @Test
    void joinDetectsBothTables() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE test() BEGIN " +
            "  SELECT o.order_id, e.emp_name " +
            "  FROM orders o JOIN employees e ON e.emp_id = o.emp_id; " +
            "END");
        List<MySqlTableOpInfo> ops = r.getTableOps().stream()
                .filter(o -> o.getDmlType() == MySqlTableOpInfo.DmlType.SELECT).toList();
        assertTrue(ops.size() >= 2,
                "expected orders + employees in FROM, got " + ops.size());
    }

    // ── DECLARE variables & cursors ───────────────────────────────────────────

    @Test
    void declareVariableExtracted() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE test() BEGIN " +
            "  DECLARE v_count INT DEFAULT 0; " +
            "  DECLARE v_name VARCHAR(100); " +
            "END");
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_count".equals(v.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_name".equals(v.getName())));
    }

    @Test
    void declareCursorExtracted() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE test() BEGIN " +
            "  DECLARE cur_emps CURSOR FOR SELECT emp_id FROM employees; " +
            "END");
        assertEquals(1, r.getCursors().size());
        assertEquals("cur_emps", r.getCursors().get(0).getName());
    }

    // ── Dynamic SQL ───────────────────────────────────────────────────────────

    @Test
    void prepareAndExecuteDetected() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE emp_raise_salary_bulk(IN p_dept INT) BEGIN " +
            "  SET @dyn_sql = 'UPDATE employees SET salary = salary * 1.1 WHERE dept_id = 1'; " +
            "  PREPARE stmt FROM @dyn_sql; " +
            "  EXECUTE stmt; " +
            "  DEALLOCATE PREPARE stmt; " +
            "END");
        assertTrue(r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == MySqlDynamicSqlInfo.Kind.PREPARE));
        assertTrue(r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == MySqlDynamicSqlInfo.Kind.EXECUTE));
        assertTrue(r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == MySqlDynamicSqlInfo.Kind.DEALLOCATE));
    }

    // ── Nested compound blocks ────────────────────────────────────────────────

    @Test
    void nestedBeginEndParsed() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE ord_process_all_pending() BEGIN " +
            "  DECLARE v_id INT; " +
            "  BEGIN " +
            "    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN " +
            "      CALL utils_log_audit('ERROR', 'orders', NULL, 'failed'); " +
            "    END; " +
            "    CALL ord_fulfill(v_id); " +
            "  END; " +
            "END");
        assertTrue(r.getCalls().stream().anyMatch(c -> "utils_log_audit".equals(c.getName())));
        assertTrue(r.getCalls().stream().anyMatch(c -> "ord_fulfill".equals(c.getName())));
    }

    // ── Real test-data proc (smoke) ───────────────────────────────────────────

    @Test
    void realProcDailySyncSmoke() {
        String src =
            "CREATE PROCEDURE proc_daily_sync(IN p_run_mode VARCHAR(20)) BEGIN\n" +
            "  DECLARE v_mode VARCHAR(20);\n" +
            "  DECLARE v_start DATETIME;\n" +
            "  SET v_mode = UPPER(TRIM(IFNULL(p_run_mode, 'FULL')));\n" +
            "  SET v_start = NOW();\n" +
            "  CALL utils_log_audit('START', 'SYSTEM', NULL, CONCAT('mode=', v_mode));\n" +
            "  IF v_mode IN ('FULL', 'ORDERS') THEN\n" +
            "    CALL ord_process_all_pending();\n" +
            "  END IF;\n" +
            "  IF v_mode IN ('FULL', 'AUDIT') THEN\n" +
            "    CALL utils_purge_old_audit(90);\n" +
            "  END IF;\n" +
            "  IF v_mode = 'FULL' THEN\n" +
            "    CALL rpt_generate_full();\n" +
            "  END IF;\n" +
            "  CALL utils_log_audit('COMPLETE', 'SYSTEM', NULL, 'done');\n" +
            "END";

        MySqlRoutineInfo r = parse(src);
        assertEquals("proc_daily_sync", r.getName());
        assertEquals(1, r.getParams().size());
        assertEquals(5, r.getCalls().size());
        assertTrue(r.getErrors().isEmpty(),
                "unexpected parse errors: " + r.getErrors());
    }

    // ── Transaction control (COMMIT / ROLLBACK / SAVEPOINT) ────────────────────

    @Test
    void transactionControlStatements() {
        MySqlRoutineInfo r = parse(
            "CREATE PROCEDURE txn_demo() BEGIN "
          + "  SAVEPOINT sp1; "
          + "  UPDATE accounts SET bal = bal - 1; "
          + "  ROLLBACK TO SAVEPOINT sp1; "
          + "  COMMIT; "
          + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        List<MySqlStatementInfo> txn = r.getStatements();
        assertTrue(txn.stream().anyMatch(s -> "COMMIT".equals(s.getType())), "expected COMMIT");
        assertTrue(txn.stream().anyMatch(s ->
                "ROLLBACK".equals(s.getType()) && "sp1".equals(s.getSavepointName())),
                "expected ROLLBACK TO SAVEPOINT sp1");
        assertTrue(txn.stream().anyMatch(s ->
                "SAVEPOINT".equals(s.getType()) && "sp1".equals(s.getSavepointName())),
                "expected SAVEPOINT sp1");
    }
}
