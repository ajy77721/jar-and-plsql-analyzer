package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT (read-only, evidence-gathering) test file. Not a bug-fix.
 *
 * Per source inspection: sqlserver-parser's grammar DOES have structural support for both
 * CREATE TRIGGER (triggerDefinition, SqlServerParser.g4:26-32) and CREATE VIEW
 * (viewDefinition, SqlServerParser.g4:35-41). Both headers (triggerHeader / viewHeader) are
 * permissive "~AS+" token-eaters, so the target table / timing / events for a trigger, and
 * WITH SCHEMABINDING / WITH CHECK OPTION for a view, are NOT captured by the grammar as
 * structured nodes — they are recovered by regex in SqlServerAnalysisVisitor
 * (visitTriggerDefinition / visitViewDefinition) against the raw header text, the same
 * regex-over-raw-text approach used for SELECT/FROM/JOIN extraction elsewhere in this dialect.
 *
 * Notably, `inserted` / `deleted` (the trigger pseudo-tables) are NOT in any skip-list anywhere
 * in the visitor (grep for "inserted"/"deleted"/"pseudo" found nothing besides a code comment
 * mentioning pseudo-tables in the abstract) — unlike CTE names, which ARE tracked and filtered
 * (see cteNames / collectCteNames). This test empirically confirms whether that gap manifests.
 */
@DisplayName("AUDIT: SQL Server CREATE TRIGGER / CREATE VIEW extraction")
public class SqlServerTriggerAndViewAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_trigger_or_view", null);
    }

    // ------------------------------------------------------------------
    // TRIGGER
    // ------------------------------------------------------------------

    @Test
    @DisplayName("AUDIT: basic AFTER INSERT, UPDATE trigger on orders — target table / timing / events")
    void auditBasicCombinedEventTrigger() {
        String sql = "CREATE TRIGGER trg_orders ON orders AFTER INSERT, UPDATE AS\n" +
                "BEGIN\n" +
                "  INSERT INTO audit_log (msg) SELECT 'changed' FROM inserted;\n" +
                "END;";

        SqlServerRoutineInfo r = parse(sql);
        System.out.println("[AUDIT] type=" + r.getType()
                + " triggerTable=" + r.getTriggerTable()
                + " triggerTiming=" + r.getTriggerTiming()
                + " triggerEvent=" + r.getTriggerEvent()
                + " tableOps=" + tableNames(r)
                + " errors=" + r.getErrors());

        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "AUDIT: basic combined-event trigger should parse without error: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.TRIGGER, r.getType());
        assertEquals("orders", r.getTriggerTable(), "AUDIT: trigger target table must be 'orders'");
        assertEquals("AFTER", r.getTriggerTiming());
        assertNotNull(r.getTriggerEvent());
        assertTrue(r.getTriggerEvent().contains("INSERT") && r.getTriggerEvent().contains("UPDATE"),
                "AUDIT: combined events INSERT and UPDATE must both be captured: " + r.getTriggerEvent());

        // FIXED: `inserted` is a pseudo-table (the trigger's in-memory delta table), not a real
        // base table — it must not be recorded as an ordinary table op. The real INSERT target
        // (audit_log) must still be captured.
        assertFalse(tableNames(r).contains("INSERTED"),
                "AUDIT: 'inserted' pseudo-table must NOT be recorded as a real table op");
        assertTrue(tableNames(r).contains("AUDIT_LOG"),
                "AUDIT: real INSERT target audit_log must still be captured");
    }

    @Test
    @DisplayName("AUDIT: INSTEAD OF DELETE trigger referencing 'deleted' pseudo-table and calling a proc")
    void auditInsteadOfDeleteTriggerWithProcCall() {
        String sql = "CREATE TRIGGER trg_orders_del ON orders INSTEAD OF DELETE AS\n" +
                "BEGIN\n" +
                "  DECLARE @id INT;\n" +
                "  SELECT @id = order_id FROM deleted;\n" +
                "  EXEC dbo.usp_archive_order @id;\n" +
                "  DELETE FROM orders WHERE order_id = @id;\n" +
                "END;";

        SqlServerRoutineInfo r = parse(sql);
        List<String> tableNames = tableNames(r);
        List<String> callNames = r.getCalls().stream()
                .map(SqlServerCallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .toList();

        System.out.println("[AUDIT] type=" + r.getType()
                + " triggerTable=" + r.getTriggerTable()
                + " triggerTiming=" + r.getTriggerTiming()
                + " triggerEvent=" + r.getTriggerEvent()
                + " tableOps=" + tableNames
                + " calls=" + callNames
                + " errors=" + r.getErrors());

        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "AUDIT: INSTEAD OF DELETE trigger should parse without error: " + r.getErrors());
        assertEquals("orders", r.getTriggerTable());
        assertEquals("INSTEAD OF", r.getTriggerTiming());
        assertEquals("DELETE", r.getTriggerEvent());

        boolean foundProcCall = callNames.stream().anyMatch(n -> n.toLowerCase().contains("usp_archive_order"));
        assertTrue(foundProcCall, "AUDIT: EXEC dbo.usp_archive_order inside trigger body must be captured as a call");

        boolean foundRealDelete = tableNames.contains("ORDERS");
        assertTrue(foundRealDelete, "AUDIT: DELETE FROM orders inside the trigger body must be captured");

        // FIXED: 'deleted' pseudo-table must not be treated as a real table op, same as 'inserted'.
        assertFalse(tableNames.contains("DELETED"),
                "AUDIT: 'deleted' pseudo-table must NOT be recorded as a real table op");
    }

    @Test
    @DisplayName("AUDIT: FOR (synonym for AFTER) trigger with single event, no pseudo-table reference")
    void auditForSynonymTiming() {
        String sql = "CREATE TRIGGER trg_simple ON customers FOR INSERT AS\n" +
                "BEGIN\n" +
                "  UPDATE stats SET cnt = cnt + 1;\n" +
                "END;";

        SqlServerRoutineInfo r = parse(sql);
        System.out.println("[AUDIT] type=" + r.getType()
                + " triggerTable=" + r.getTriggerTable()
                + " triggerTiming=" + r.getTriggerTiming()
                + " triggerEvent=" + r.getTriggerEvent()
                + " tableOps=" + tableNames(r)
                + " errors=" + r.getErrors());

        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "AUDIT: FOR-timing trigger should parse without error: " + r.getErrors());
        assertEquals("customers", r.getTriggerTable());
        // NOTE: FOR is a legitimate synonym of AFTER in T-SQL trigger syntax; verifying the regex
        // reports it as "FOR" (its own literal token) rather than misclassifying/dropping it.
        assertEquals("FOR", r.getTriggerTiming());
        assertEquals("INSERT", r.getTriggerEvent());
        assertTrue(tableNames(r).contains("STATS"), "AUDIT: UPDATE stats inside body must be captured");
    }

    // ------------------------------------------------------------------
    // VIEW
    // ------------------------------------------------------------------

    @Test
    @DisplayName("AUDIT: basic CREATE VIEW with JOIN — source tables captured, viewQuery populated")
    void auditBasicViewWithJoin() {
        String sql = "CREATE VIEW v_orders AS\n" +
                "SELECT o.*, c.name FROM orders o JOIN customers c ON o.cust_id = c.id;";

        SqlServerRoutineInfo r = parse(sql);
        List<String> tableNames = tableNames(r);

        System.out.println("[AUDIT] type=" + r.getType()
                + " tableOps=" + tableNames
                + " viewQuery=" + r.getViewQuery()
                + " errors=" + r.getErrors());

        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "AUDIT: basic view with join should parse without error: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.VIEW, r.getType());
        assertTrue(tableNames.contains("ORDERS"), "AUDIT: source table ORDERS must be captured");
        assertTrue(tableNames.contains("CUSTOMERS"), "AUDIT: source table CUSTOMERS must be captured");
        assertNotNull(r.getViewQuery(), "AUDIT: view defining query text should be captured");
    }

    @Test
    @DisplayName("AUDIT: CREATE VIEW WITH SCHEMABINDING AS SELECT ...")
    void auditViewWithSchemabinding() {
        String sql = "CREATE VIEW v_orders_sb WITH SCHEMABINDING AS\n" +
                "SELECT o.id, o.total FROM dbo.orders o;";

        SqlServerRoutineInfo r = parse(sql);
        List<String> tableNames = tableNames(r);

        System.out.println("[AUDIT] type=" + r.getType()
                + " tableOps=" + tableNames
                + " errors=" + r.getErrors());

        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "AUDIT: WITH SCHEMABINDING view should parse without error: " + r.getErrors());
        assertEquals(SqlServerRoutineInfo.RoutineType.VIEW, r.getType());
        assertTrue(tableNames.contains("ORDERS"), "AUDIT: source table orders must be captured despite WITH SCHEMABINDING header");
    }

    @Test
    @DisplayName("AUDIT: CREATE VIEW ... AS SELECT ... WITH CHECK OPTION (trailing clause after routineBody)")
    void auditViewWithCheckOption() {
        String sql = "CREATE VIEW v_active_orders AS\n" +
                "SELECT * FROM orders WHERE status = 'ACTIVE'\n" +
                "WITH CHECK OPTION;";

        SqlServerRoutineInfo r = parse(sql);
        List<String> tableNames = tableNames(r);

        System.out.println("[AUDIT] type=" + r.getType()
                + " tableOps=" + tableNames
                + " viewQuery=" + r.getViewQuery()
                + " errors=" + r.getErrors());

        // WITH CHECK OPTION trails AFTER the SELECT, unlike SCHEMABINDING which precedes AS.
        // routineBody's tolerant catch-all rules are exercised here to see if the trailing
        // WITH CHECK OPTION clause parses cleanly or corrupts table extraction.
        assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "AUDIT: WITH CHECK OPTION view should parse without error: " + r.getErrors());
        assertTrue(tableNames.contains("ORDERS"), "AUDIT: source table orders must still be captured with trailing WITH CHECK OPTION");
    }

    private static List<String> tableNames(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
    }
}
