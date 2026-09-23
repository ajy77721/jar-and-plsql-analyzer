package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlCursorInfo;
import com.plsql.mysql.model.MySqlExceptionHandlerInfo;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * READ-ONLY AUDIT (2026-09-17) — MySQL-unique concepts audit (items 10, 11, 12) for
 * mysql-parser, run against the real MySqlGrammarEngine / MySqlAnalysisVisitor. No hard
 * assertions on documented gaps or unsupported constructs — this test exists to print real
 * captured evidence, per the invoking audit's method (mirrors MySqlDmlCaptureGapAuditTest /
 * MySqlDynamicCursorAuditTest style).
 */
public class MySqlUniqueConceptsAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    // ── Item 10: stored FUNCTION with RETURNS <type> + DETERMINISTIC / READS SQL DATA ──────

    @Test
    @DisplayName("10: FUNCTION with RETURNS <type>, DETERMINISTIC, READS SQL DATA characteristics")
    void functionWithReturnsAndCharacteristics() {
        String sql = """
                CREATE FUNCTION calc_order_total(p_order_id INT)
                RETURNS DECIMAL(10,2)
                DETERMINISTIC
                READS SQL DATA
                BEGIN
                  DECLARE v_total DECIMAL(10,2);
                  SELECT SUM(amount) INTO v_total FROM order_items WHERE order_id = p_order_id;
                  RETURN v_total;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "calc_order_total", null);
        System.out.println("[10] type: " + r.getType());
        System.out.println("[10] name: " + r.getName());
        System.out.println("[10] params: " + r.getParams().stream()
                .map(p -> p.getDirection() + " " + p.getName() + " " + p.getDataType()).toList());
        System.out.println("[10] tableOps: " + tableNames(r));
        System.out.println("[10] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[10] errors: " + r.getErrors());
        // NOTE: MySqlRoutineInfo has no returnType field at all (confirmed by reading
        // model/MySqlRoutineInfo.java) - there is nowhere for a captured "DECIMAL(10,2)"
        // return type to even be stored, regardless of whether returnsClause is visited.
    }

    // ── Item 11: TRIGGER with OLD/NEW row references ───────────────────────────────────────

    @Test
    @DisplayName("11: CREATE TRIGGER ... BEFORE UPDATE ... referencing NEW/OLD")
    void triggerWithOldNewRowReferences() {
        String sql = """
                CREATE TRIGGER trg_orders_status_audit
                BEFORE UPDATE ON orders
                FOR EACH ROW
                BEGIN
                  IF NEW.status <> OLD.status THEN
                    INSERT INTO order_status_audit (order_id, old_status, new_status)
                    VALUES (OLD.id, OLD.status, NEW.status);
                  END IF;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "trg_orders_status_audit", null);
        System.out.println("[11] type: " + r.getType());
        System.out.println("[11] name: " + r.getName());
        System.out.println("[11] tableOps: " + tableNames(r));
        System.out.println("[11] errors: " + r.getErrors());
        // CREATE TRIGGER is now a first-class grammar construct (createTrigger, wired as an
        // alternative of compilationUnit). It must parse cleanly, expose its trigger metadata,
        // and — crucially — have its body walked by the same visitor logic as a procedure body,
        // so the INSERT nested inside the IF is captured.
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE TRIGGER must parse cleanly, got: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                MySqlRoutineInfo.RoutineType.TRIGGER, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("trg_orders_status_audit", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getTriggerTable());
        org.junit.jupiter.api.Assertions.assertEquals("BEFORE", r.getTriggerTiming());
        org.junit.jupiter.api.Assertions.assertEquals("UPDATE", r.getTriggerEvent());
        org.junit.jupiter.api.Assertions.assertTrue(
                tableNames(r).contains("order_status_audit"),
                "trigger body INSERT target not captured, got: " + tableNames(r));
    }

    // ── Item 12: HANDLER declarations with cursor + LOOP inside a realistic procedure ──────

    @Test
    @DisplayName("12: DECLARE CONTINUE HANDLER FOR NOT FOUND + cursor + LOOP/FETCH/LEAVE")
    void continueHandlerWithCursorLoop() {
        String sql = """
                CREATE PROCEDURE proc_process_open_orders()
                BEGIN
                  DECLARE done INT DEFAULT FALSE;
                  DECLARE v_order_id INT;
                  DECLARE v_amount DECIMAL(10,2);
                  DECLARE cur CURSOR FOR SELECT id, amount FROM orders WHERE status = 'OPEN';
                  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

                  OPEN cur;
                  read_loop: LOOP
                    FETCH cur INTO v_order_id, v_amount;
                    IF done THEN
                      LEAVE read_loop;
                    END IF;
                    INSERT INTO processed_orders (order_id, amount) VALUES (v_order_id, v_amount);
                    UPDATE orders SET status = 'PROCESSED' WHERE id = v_order_id;
                  END LOOP;
                  CLOSE cur;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_process_open_orders", null);
        System.out.println("[12] cursors: " + r.getCursors().stream()
                .map(MySqlCursorInfo::getName).toList());
        System.out.println("[12] exceptionHandlers: " + r.getExceptionHandlers().stream()
                .map(h -> h.getKind() + " conditions=" + h.getConditions() + "@L" + h.getLine()).toList());
        System.out.println("[12] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[12] errors: " + r.getErrors());

        boolean cursorCaptured = r.getCursors().stream().anyMatch(c -> "cur".equalsIgnoreCase(c.getName()));
        boolean insertCaptured = r.getTableOps().stream().anyMatch(t ->
                t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT
                        && "processed_orders".equalsIgnoreCase(t.getTableName()));
        boolean updateCaptured = r.getTableOps().stream().anyMatch(t ->
                t.getDmlType() == MySqlTableOpInfo.DmlType.UPDATE
                        && "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("[12] cursorCaptured=" + cursorCaptured
                + " insertInsideLoopCaptured=" + insertCaptured
                + " updateInsideLoopCaptured=" + updateCaptured);
    }
}
