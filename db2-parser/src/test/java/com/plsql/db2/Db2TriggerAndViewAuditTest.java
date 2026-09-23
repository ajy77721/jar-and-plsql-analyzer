package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CREATE TRIGGER and CREATE VIEW support in the DB2 grammar/visitor.
 *
 * Originally written as a read-only audit (2026-09-18) documenting that both constructs were
 * entirely unsupported: Db2Lexer.g4 had no TRIGGER/VIEW token, Db2Parser.g4's entry point was
 * {@code compilationUnit : routineDefinition? EOF ;} with routineDefinition demanding
 * PROCEDURE or FUNCTION right after CREATE, and Db2AnalysisVisitor.java had zero matches for
 * "trigger" or "view" — so the token after CREATE was a syntax error and the whole unit yielded
 * nothing (no target table, no body DML, no calls).
 *
 * The gap has since been closed: {@code createTrigger} / {@code createView} are real
 * compilationUnit alternatives, and {@code visitCreateTrigger} / {@code visitCreateView} capture
 * the DDL metadata while reusing the existing bodyBlock/selectStatement rules so body DML, CALLs
 * and FROM/JOIN tables are captured by the same logic as procedures. The evidence dumps are kept
 * (they are useful when a case regresses) and the findings are now hard assertions.
 */
class Db2TriggerAndViewAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("errors=" + r.getErrors());
        System.out.println("tableOps.size()=" + r.getTableOps().size());
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable() + " alias=" + t.getAlias());
        }
        System.out.println("statements.size()=" + r.getStatements().size());
        for (Db2StatementInfo s : r.getStatements()) {
            System.out.println("  " + s.getType() + " line=" + s.getLine());
        }
        System.out.println("calls.size()=" + r.getCalls().size());
        for (Db2CallInfo c : r.getCalls()) {
            System.out.println("  " + c.getSchema() + "." + c.getName());
        }
    }

    // ── Concept 1: CREATE TRIGGER — BEFORE INSERT, row-level, WHEN condition ──

    private static final String TRIGGER_BEFORE_INSERT_SRC =
        "CREATE TRIGGER trg1 NO CASCADE BEFORE INSERT ON orders\n"
      + "REFERENCING NEW AS n\n"
      + "FOR EACH ROW\n"
      + "WHEN (n.amount > 1000)\n"
      + "BEGIN\n"
      + "  CALL log_big_order(n.order_id, n.amount);\n"
      + "  INSERT INTO order_audit (order_id, amount) VALUES (n.order_id, n.amount);\n"
      + "END";

    @Test
    @DisplayName("Concept 1a: CREATE TRIGGER ... NO CASCADE BEFORE INSERT ... REFERENCING NEW AS n ... FOR EACH ROW WHEN (...) BEGIN...END")
    void triggerBeforeInsertRowLevel() {
        Db2RoutineInfo r = engine.parse(TRIGGER_BEFORE_INSERT_SRC, "trg1", null);
        dump("Trigger-BeforeInsert", r);

        boolean parsedWithoutError = r.getErrors().isEmpty();
        boolean targetTableCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean bodyCallCaptured = r.getCalls().stream()
                .anyMatch(c -> "log_big_order".equalsIgnoreCase(c.getName()));
        boolean bodyInsertCaptured = r.getTableOps().stream()
                .anyMatch(t -> "order_audit".equalsIgnoreCase(t.getTableName()));

        System.out.println(">>> parsedWithoutError=" + parsedWithoutError
                + " targetTableCaptured(orders)=" + targetTableCaptured
                + " bodyCallCaptured(log_big_order)=" + bodyCallCaptured
                + " bodyInsertCaptured(order_audit)=" + bodyInsertCaptured);

        assertTrue(parsedWithoutError, "trigger DDL must parse cleanly: " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.TRIGGER, r.getType());
        assertTrue("trg1".equalsIgnoreCase(r.getName()), "name=" + r.getName());
        assertEquals("BEFORE", r.getTriggerTiming());
        assertEquals("INSERT", r.getTriggerEvent());
        assertEquals("ROW", r.getTriggerGranularity());
        assertTrue("orders".equalsIgnoreCase(r.getTriggerTable()), "triggerTable=" + r.getTriggerTable());
        assertEquals(1, r.getTriggerReferencing().size());
        assertTrue("NEW AS n".equalsIgnoreCase(r.getTriggerReferencing().get(0)),
                "referencing=" + r.getTriggerReferencing());
        assertTrue(targetTableCaptured, "subject table 'orders' must be captured as a table op");
        assertTrue(bodyCallCaptured, "CALL log_big_order inside the trigger body must be captured");
        assertTrue(bodyInsertCaptured, "INSERT INTO order_audit in the trigger body must be captured");
    }

    // ── Concept 1b: CREATE TRIGGER — AFTER UPDATE, statement-level, OLD+NEW aliases ──

    private static final String TRIGGER_AFTER_UPDATE_SRC =
        "CREATE TRIGGER trg2 AFTER UPDATE ON orders\n"
      + "REFERENCING OLD AS o NEW AS n\n"
      + "FOR EACH STATEMENT\n"
      + "BEGIN\n"
      + "  CALL sync_order_status(n.order_id, n.status);\n"
      + "  UPDATE order_history SET last_status = n.status WHERE order_id = n.order_id;\n"
      + "END";

    @Test
    @DisplayName("Concept 1b: CREATE TRIGGER ... AFTER UPDATE ... REFERENCING OLD AS o NEW AS n ... FOR EACH STATEMENT")
    void triggerAfterUpdateStatementLevel() {
        Db2RoutineInfo r = engine.parse(TRIGGER_AFTER_UPDATE_SRC, "trg2", null);
        dump("Trigger-AfterUpdate", r);

        boolean parsedWithoutError = r.getErrors().isEmpty();
        boolean targetTableCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean bodyCallCaptured = r.getCalls().stream()
                .anyMatch(c -> "sync_order_status".equalsIgnoreCase(c.getName()));
        boolean bodyUpdateCaptured = r.getTableOps().stream()
                .anyMatch(t -> "order_history".equalsIgnoreCase(t.getTableName()));

        System.out.println(">>> parsedWithoutError=" + parsedWithoutError
                + " targetTableCaptured(orders)=" + targetTableCaptured
                + " bodyCallCaptured(sync_order_status)=" + bodyCallCaptured
                + " bodyUpdateCaptured(order_history)=" + bodyUpdateCaptured);

        assertTrue(parsedWithoutError, "trigger DDL must parse cleanly: " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.TRIGGER, r.getType());
        assertEquals("AFTER", r.getTriggerTiming());
        assertEquals("UPDATE", r.getTriggerEvent());
        assertEquals("STATEMENT", r.getTriggerGranularity());
        assertTrue("orders".equalsIgnoreCase(r.getTriggerTable()), "triggerTable=" + r.getTriggerTable());
        assertEquals(2, r.getTriggerReferencing().size(), "referencing=" + r.getTriggerReferencing());
        assertTrue("OLD AS o".equalsIgnoreCase(r.getTriggerReferencing().get(0)));
        assertTrue("NEW AS n".equalsIgnoreCase(r.getTriggerReferencing().get(1)));
        assertTrue(targetTableCaptured, "subject table 'orders' must be captured as a table op");
        assertTrue(bodyCallCaptured, "CALL sync_order_status in the trigger body must be captured");
        assertTrue(bodyUpdateCaptured, "UPDATE order_history in the trigger body must be captured");
    }

    // ── Concept 1c: CREATE TRIGGER — INSTEAD OF DELETE, no body BEGIN..END keywords needed check ──

    private static final String TRIGGER_INSTEAD_OF_SRC =
        "CREATE TRIGGER trg3 INSTEAD OF DELETE ON orders_view\n"
      + "REFERENCING OLD AS o\n"
      + "FOR EACH ROW\n"
      + "BEGIN\n"
      + "  DELETE FROM orders WHERE order_id = o.order_id;\n"
      + "END";

    @Test
    @DisplayName("Concept 1c: CREATE TRIGGER ... INSTEAD OF DELETE ON orders_view ... REFERENCING OLD AS o")
    void triggerInsteadOfDelete() {
        Db2RoutineInfo r = engine.parse(TRIGGER_INSTEAD_OF_SRC, "trg3", null);
        dump("Trigger-InsteadOfDelete", r);

        boolean parsedWithoutError = r.getErrors().isEmpty();
        boolean targetViewCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders_view".equalsIgnoreCase(t.getTableName()));
        boolean bodyDeleteCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.DELETE);

        System.out.println(">>> parsedWithoutError=" + parsedWithoutError
                + " targetViewCaptured(orders_view)=" + targetViewCaptured
                + " bodyDeleteCaptured(orders,DELETE)=" + bodyDeleteCaptured);

        assertTrue(parsedWithoutError, "trigger DDL must parse cleanly: " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.TRIGGER, r.getType());
        assertEquals("INSTEAD OF", r.getTriggerTiming());
        assertEquals("DELETE", r.getTriggerEvent());
        assertEquals("ROW", r.getTriggerGranularity());
        assertTrue("orders_view".equalsIgnoreCase(r.getTriggerTable()), "triggerTable=" + r.getTriggerTable());
        assertTrue(targetViewCaptured, "subject view 'orders_view' must be captured as a table op");
        assertTrue(bodyDeleteCaptured, "DELETE FROM orders in the trigger body must be captured");
    }

    // ── Concept 2a: CREATE VIEW — simple JOIN ──

    private static final String VIEW_SIMPLE_SRC =
        "CREATE VIEW v_orders AS\n"
      + "SELECT o.*, c.name\n"
      + "FROM orders o\n"
      + "JOIN customers c ON o.cust_id = c.id";

    @Test
    @DisplayName("Concept 2a: CREATE VIEW v_orders AS SELECT ... FROM orders o JOIN customers c ON ...")
    void viewSimpleJoin() {
        Db2RoutineInfo r = engine.parse(VIEW_SIMPLE_SRC, "v_orders", null);
        dump("View-Simple", r);

        boolean parsedWithoutError = r.getErrors().isEmpty();
        boolean baseOrdersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean baseCustomersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));

        System.out.println(">>> parsedWithoutError=" + parsedWithoutError
                + " baseOrdersCaptured=" + baseOrdersCaptured
                + " baseCustomersCaptured=" + baseCustomersCaptured);

        assertTrue(parsedWithoutError, "view DDL must parse cleanly: " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.VIEW, r.getType());
        assertTrue("v_orders".equalsIgnoreCase(r.getName()), "name=" + r.getName());
        assertNotNull(r.getViewQuery());
        assertTrue(r.getViewQuery().toUpperCase().startsWith("SELECT"), "viewQuery=" + r.getViewQuery());
        assertTrue(baseOrdersCaptured, "base table 'orders' must be captured from the defining SELECT");
        assertTrue(baseCustomersCaptured, "joined table 'customers' must be captured from the defining SELECT");
    }

    // ── Concept 2b: CREATE VIEW ... WITH CHECK OPTION ──

    private static final String VIEW_CHECK_OPTION_SRC =
        "CREATE VIEW v_active_orders AS\n"
      + "SELECT o.order_id, o.status\n"
      + "FROM orders o\n"
      + "WHERE o.status = 'ACTIVE'\n"
      + "WITH CHECK OPTION";

    @Test
    @DisplayName("Concept 2b: CREATE VIEW ... WHERE ... WITH CHECK OPTION")
    void viewWithCheckOption() {
        Db2RoutineInfo r = engine.parse(VIEW_CHECK_OPTION_SRC, "v_active_orders", null);
        dump("View-CheckOption", r);

        boolean parsedWithoutError = r.getErrors().isEmpty();
        boolean baseOrdersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));

        System.out.println(">>> parsedWithoutError=" + parsedWithoutError
                + " baseOrdersCaptured=" + baseOrdersCaptured);

        assertTrue(parsedWithoutError, "view DDL must parse cleanly: " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.VIEW, r.getType());
        assertTrue("v_active_orders".equalsIgnoreCase(r.getName()), "name=" + r.getName());
        assertTrue(baseOrdersCaptured, "base table 'orders' must be captured despite WITH CHECK OPTION");
    }
}
