package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CREATE TRIGGER / CREATE VIEW support in mysql-parser, exercised against the real
 * MySqlGrammarEngine / MySqlAnalysisVisitor / MySqlParser.g4.
 *
 * <p>Originally a read-only audit that documented both constructs as unsupported: the
 * grammar entry point was {@code compilationUnit : createRoutine EOF ;} and
 * {@code createRoutine} only accepted PROCEDURE/FUNCTION, so CREATE TRIGGER and
 * CREATE VIEW were rejected at the first token after CREATE, every time, and never
 * reached the visitor at all.
 *
 * <p>The grammar now has {@code createTrigger} (and {@code createView}) rules wired as
 * alternatives of {@code compilationUnit}, so these assertions are the real thing: zero
 * parse errors, correct identity/metadata, and body DML captured by the same visitor
 * logic that handles procedure bodies.
 */
public class MySqlTriggerAndViewAuditTest {

    // ───────────────────────────── CREATE TRIGGER ─────────────────────────────

    @Test
    @DisplayName("TRIGGER 1: BEFORE INSERT ... FOR EACH ROW referencing NEW.col")
    void triggerBeforeInsertNewRow() {
        String sql = """
                CREATE TRIGGER trg_before_insert
                BEFORE INSERT ON orders
                FOR EACH ROW
                BEGIN
                  SET NEW.created_at = NOW();
                END;
                """;
        MySqlRoutineInfo r = runOk("TRIGGER-1 (BEFORE INSERT, NEW.col)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.TRIGGER, r.getType());
        assertEquals("trg_before_insert", r.getName());
        assertEquals("orders", r.getTriggerTable());
        assertEquals("BEFORE", r.getTriggerTiming());
        assertEquals("INSERT", r.getTriggerEvent());
    }

    @Test
    @DisplayName("TRIGGER 2: AFTER UPDATE ... referencing OLD/NEW")
    void triggerAfterUpdateOldNew() {
        String sql = """
                CREATE TRIGGER trg_after_update
                AFTER UPDATE ON orders
                FOR EACH ROW
                BEGIN
                  INSERT INTO order_audit (order_id, old_status, new_status)
                  VALUES (OLD.id, OLD.status, NEW.status);
                END;
                """;
        MySqlRoutineInfo r = runOk("TRIGGER-2 (AFTER UPDATE, OLD/NEW)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.TRIGGER, r.getType());
        assertEquals("trg_after_update", r.getName());
        assertEquals("orders", r.getTriggerTable());
        assertEquals("AFTER", r.getTriggerTiming());
        assertEquals("UPDATE", r.getTriggerEvent());
        // The body's INSERT must be captured by the ordinary procedure-body visitor logic.
        assertTrue(tableNames(r).contains("order_audit"),
                "trigger body INSERT target not captured, got: " + tableNames(r));
        assertTrue(r.getTableOps().stream().anyMatch(
                        t -> "order_audit".equals(t.getTableName())
                                && t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT),
                "expected an INSERT op on order_audit, got: " + r.getTableOps().stream()
                        .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
    }

    @Test
    @DisplayName("TRIGGER 3: MySQL 5.7+ ordering with FOLLOWS")
    void triggerWithFollows() {
        String sql = """
                CREATE TRIGGER trg_before_insert_2
                BEFORE INSERT ON orders
                FOR EACH ROW
                FOLLOWS trg_before_insert
                BEGIN
                  SET NEW.updated_at = NOW();
                END;
                """;
        MySqlRoutineInfo r = runOk("TRIGGER-3 (FOLLOWS ordering)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.TRIGGER, r.getType());
        assertEquals("trg_before_insert_2", r.getName());
        assertEquals("orders", r.getTriggerTable());
        assertEquals("BEFORE", r.getTriggerTiming());
        assertEquals("INSERT", r.getTriggerEvent());
    }

    @Test
    @DisplayName("TRIGGER 4: MySQL 5.7+ ordering with PRECEDES")
    void triggerWithPrecedes() {
        String sql = """
                CREATE TRIGGER trg_before_insert_0
                BEFORE INSERT ON orders
                FOR EACH ROW
                PRECEDES trg_before_insert
                BEGIN
                  SET NEW.id = NEW.id;
                END;
                """;
        MySqlRoutineInfo r = runOk("TRIGGER-4 (PRECEDES ordering)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.TRIGGER, r.getType());
        assertEquals("trg_before_insert_0", r.getName());
        assertEquals("orders", r.getTriggerTable());
        assertEquals("BEFORE", r.getTriggerTiming());
        assertEquals("INSERT", r.getTriggerEvent());
    }

    // ───────────────────────────── CREATE VIEW ─────────────────────────────

    @Test
    @DisplayName("VIEW 1: plain CREATE VIEW ... AS SELECT ... JOIN")
    void plainCreateView() {
        String sql = """
                CREATE VIEW v_orders AS
                SELECT o.*, c.name
                FROM orders o
                JOIN customers c ON o.cust_id = c.id;
                """;
        MySqlRoutineInfo r = runOk("VIEW-1 (plain CREATE VIEW ... JOIN)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.VIEW, r.getType());
        assertEquals("v_orders", r.getName());
        assertTrue(tableNames(r).containsAll(List.of("orders", "customers")),
                "both underlying tables of the defining SELECT must be captured, got: "
                        + tableNames(r));
    }

    @Test
    @DisplayName("VIEW 2: CREATE OR REPLACE VIEW ... WITH CHECK OPTION")
    void createOrReplaceViewWithCheckOption() {
        String sql = """
                CREATE OR REPLACE VIEW v_open_orders AS
                SELECT * FROM orders WHERE status = 'OPEN'
                WITH CHECK OPTION;
                """;
        MySqlRoutineInfo r = runOk("VIEW-2 (CREATE OR REPLACE ... WITH CHECK OPTION)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.VIEW, r.getType());
        assertEquals("v_open_orders", r.getName());
        assertTrue(tableNames(r).contains("orders"),
                "underlying table not captured, got: " + tableNames(r));
    }

    @Test
    @DisplayName("VIEW 3: CREATE ALGORITHM=MERGE VIEW ...")
    void createAlgorithmMergeView() {
        String sql = """
                CREATE ALGORITHM=MERGE VIEW v_customer_orders AS
                SELECT c.id AS customer_id, o.id AS order_id
                FROM customers c
                JOIN orders o ON o.cust_id = c.id;
                """;
        MySqlRoutineInfo r = runOk("VIEW-3 (ALGORITHM=MERGE VIEW)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.VIEW, r.getType());
        assertEquals("v_customer_orders", r.getName());
        assertTrue(tableNames(r).containsAll(List.of("customers", "orders")),
                "both underlying tables of the defining SELECT must be captured, got: "
                        + tableNames(r));
    }

    // ───────────────────────────── shared harness ─────────────────────────────

    /**
     * Parses the snippet with the real MySqlGrammarEngine, prints what was captured, and
     * asserts the parse was clean. Every construct exercised here is now supported by the
     * grammar, so any reported syntax error is a genuine regression.
     */
    private MySqlRoutineInfo runOk(String label, String sql) {
        MySqlRoutineInfo r = new MySqlGrammarEngine().parse(sql, label, null);
        System.out.println("[" + label + "] type=" + r.getType()
                + " name=" + r.getName()
                + " triggerTable=" + r.getTriggerTable()
                + " timing=" + r.getTriggerTiming()
                + " event=" + r.getTriggerEvent()
                + " tableOps=" + r.getTableOps().stream()
                        .map(t -> t.getDmlType() + ":" + t.getTableName()).toList()
                + " errors=" + r.getErrors());
        assertTrue(r.getErrors().isEmpty(),
                "[" + label + "] expected a clean parse, got errors: " + r.getErrors());
        return r;
    }

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }
}
