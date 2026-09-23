package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Read-only audit (2026-09-18): CREATE TRIGGER and CREATE VIEW support probe against the real
 * ANTLR grammar/visitor. This is a diagnostic + permanent evidence file — it prints actual
 * captured data for each scenario; some assertions are strict where the expected behavior is
 * unambiguous (e.g. "the trigger table must be captured"), others are exploratory prints only.
 */
public class OracleTriggerAndViewAuditTest extends ParserTestBase {

    private ParsedObject findObject(ParseResult result, String type, String name) {
        for (ParsedObject po : result.getObjects()) {
            if (type.equalsIgnoreCase(po.getType())
                    && (name == null || name.equalsIgnoreCase(po.getName()))) {
                return po;
            }
        }
        return null;
    }

    private void dumpObject(String label, ParsedObject po) {
        System.out.println("=== " + label + " ===");
        if (po == null) {
            System.out.println("  (object NOT FOUND)");
            return;
        }
        System.out.println("  name=" + po.getName() + " type=" + po.getType()
                + " triggerTiming=" + po.getTriggerTiming()
                + " triggerEvent=" + po.getTriggerEvent()
                + " triggerTable=" + po.getTriggerTable());
        List<TableOperationInfo> ops = po.getTableOperations();
        if (ops == null || ops.isEmpty()) {
            System.out.println("  (no table operations captured)");
        } else {
            for (TableOperationInfo t : ops) {
                System.out.println("  table=" + t.getTableName() + " op=" + t.getOperation());
            }
        }
        List<CallInfo> calls = po.getCalls();
        if (calls != null) {
            for (CallInfo c : calls) {
                System.out.println("  call=" + c.getName());
            }
        }
        if (po.getDependencies() != null && po.getDependencies().getTables() != null) {
            System.out.println("  dependencies.tables=" + po.getDependencies().getTables());
        }
    }

    // 1. BEFORE INSERT FOR EACH ROW with :NEW pseudo-record assignment
    @Test
    @DisplayName("1: BEFORE INSERT ... FOR EACH ROW with :NEW assignment")
    void beforeInsertForEachRow() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_orders_bi
                BEFORE INSERT ON orders
                FOR EACH ROW
                BEGIN
                  :NEW.status := 'ACTIVE';
                  INSERT INTO audit_log(action, tbl) VALUES ('INSERT', 'ORDERS');
                END;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "BEFORE INSERT trigger");
        ParsedObject po = findObject(result, "TRIGGER", "TRG_ORDERS_BI");
        dumpObject("1: BEFORE INSERT FOR EACH ROW", po);
    }

    // 2. AFTER UPDATE FOR EACH ROW, :OLD/:NEW compare, plus a call inside body
    @Test
    @DisplayName("2: AFTER UPDATE ... FOR EACH ROW with :OLD/:NEW compare + inner call")
    void afterUpdateForEachRow() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_orders_au
                AFTER UPDATE ON orders
                FOR EACH ROW
                BEGIN
                  IF :OLD.status != :NEW.status THEN
                    log_status_change(:OLD.status, :NEW.status);
                  END IF;
                END;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "AFTER UPDATE trigger");
        ParsedObject po = findObject(result, "TRIGGER", "TRG_ORDERS_AU");
        dumpObject("2: AFTER UPDATE FOR EACH ROW", po);
    }

    // 3. INSTEAD OF DELETE ON a view
    @Test
    @DisplayName("3: INSTEAD OF DELETE ON orders_view")
    void insteadOfDelete() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_orders_view_iod
                INSTEAD OF DELETE ON orders_view
                FOR EACH ROW
                BEGIN
                  DELETE FROM orders WHERE id = :OLD.id;
                END;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "INSTEAD OF DELETE trigger");
        ParsedObject po = findObject(result, "TRIGGER", "TRG_ORDERS_VIEW_IOD");
        dumpObject("3: INSTEAD OF DELETE", po);
    }

    // 4. Combined events (BEFORE INSERT OR UPDATE) + WHEN clause conditional firing
    @Test
    @DisplayName("4: BEFORE INSERT OR UPDATE ... WHEN (NEW.status = 'X')")
    void combinedEventsWithWhenClause() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_orders_biu
                BEFORE INSERT OR UPDATE ON orders
                FOR EACH ROW
                WHEN (NEW.status = 'X')
                BEGIN
                  :NEW.last_modified := SYSDATE;
                END;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "combined-event WHEN-clause trigger");
        ParsedObject po = findObject(result, "TRIGGER", "TRG_ORDERS_BIU");
        dumpObject("4: combined events + WHEN clause", po);
    }

    // 5. CREATE VIEW with a two-table JOIN
    @Test
    @DisplayName("5: CREATE VIEW v_orders AS SELECT ... JOIN customers")
    void createViewWithJoin() {
        String sql = """
                CREATE VIEW v_orders AS
                SELECT o.*, c.name
                FROM orders o
                JOIN customers c ON o.cust_id = c.id;
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CREATE VIEW with JOIN");
        ParsedObject po = findObject(result, "VIEW", "V_ORDERS");
        dumpObject("5: CREATE VIEW with JOIN", po);

        // Bug 1 fix: "o.*" in the select list must not create a phantom table op named "O".
        org.junit.jupiter.api.Assertions.assertNotNull(po);
        boolean phantomAlias = po.getTableOperations().stream()
                .anyMatch(t -> "O".equalsIgnoreCase(t.getTableName()));
        org.junit.jupiter.api.Assertions.assertFalse(phantomAlias,
                "phantom alias table 'O' must not be captured");
        // Bug 2 fix: no duplicate table ops for a plain CREATE VIEW with a JOIN either.
        long ordersCount = po.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .count();
        long customersCount = po.getTableOperations().stream()
                .filter(t -> "CUSTOMERS".equalsIgnoreCase(t.getTableName()))
                .count();
        org.junit.jupiter.api.Assertions.assertEquals(1, ordersCount,
                "ORDERS must be captured exactly once, not duplicated");
        org.junit.jupiter.api.Assertions.assertEquals(1, customersCount,
                "CUSTOMERS must be captured exactly once, not duplicated");
    }

    // 5b. Same "alias.*" pattern in a plain procedure (not a view) to make sure the fix
    // isn't view-specific.
    @Test
    @DisplayName("5b: alias.* in a plain procedure's SELECT must not create a phantom table")
    void aliasStarInProcedure() {
        String sql = """
                CREATE OR REPLACE PROCEDURE proc_orders IS
                  CURSOR c1 IS
                    SELECT o.*, c.name
                    FROM orders o
                    JOIN customers c ON o.cust_id = c.id;
                BEGIN
                  NULL;
                END;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "alias.* in plain procedure");
        ParsedObject po = findObject(result, "PROCEDURE", "PROC_ORDERS");
        dumpObject("5b: alias.* in plain procedure", po);

        org.junit.jupiter.api.Assertions.assertNotNull(po);
        boolean phantomAlias = po.getTableOperations().stream()
                .anyMatch(t -> "O".equalsIgnoreCase(t.getTableName()));
        org.junit.jupiter.api.Assertions.assertFalse(phantomAlias,
                "phantom alias table 'O' must not be captured");
    }

    // 6. CREATE VIEW ... WITH CHECK OPTION
    @Test
    @DisplayName("6: CREATE VIEW v_x AS SELECT * FROM orders WHERE status='X' WITH CHECK OPTION")
    void createViewWithCheckOption() {
        String sql = """
                CREATE VIEW v_x AS
                SELECT * FROM orders WHERE status='X' WITH CHECK OPTION;
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CREATE VIEW WITH CHECK OPTION");
        ParsedObject po = findObject(result, "VIEW", "V_X");
        dumpObject("6: CREATE VIEW WITH CHECK OPTION", po);

        // Bug 2 fix: exactly ONE ORDERS entry in po.getTableOperations(), not a duplicate.
        org.junit.jupiter.api.Assertions.assertNotNull(po);
        long ordersCount = po.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .count();
        org.junit.jupiter.api.Assertions.assertEquals(1, ordersCount,
                "ORDERS must be captured exactly once, not duplicated");
    }

    // 7. CREATE OR REPLACE FORCE VIEW
    @Test
    @DisplayName("7: CREATE OR REPLACE FORCE VIEW v_y AS ...")
    void createOrReplaceForceView() {
        String sql = """
                CREATE OR REPLACE FORCE VIEW v_y AS
                SELECT o.id, c.name
                FROM orders o, customers c
                WHERE o.cust_id = c.id;
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CREATE OR REPLACE FORCE VIEW");
        ParsedObject po = findObject(result, "VIEW", "V_Y");
        dumpObject("7: CREATE OR REPLACE FORCE VIEW", po);

        // Bug 2 fix: no duplicate table ops for a FORCE VIEW either.
        org.junit.jupiter.api.Assertions.assertNotNull(po);
        long ordersCount = po.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .count();
        long customersCount = po.getTableOperations().stream()
                .filter(t -> "CUSTOMERS".equalsIgnoreCase(t.getTableName()))
                .count();
        org.junit.jupiter.api.Assertions.assertEquals(1, ordersCount,
                "ORDERS must be captured exactly once, not duplicated");
        org.junit.jupiter.api.Assertions.assertEquals(1, customersCount,
                "CUSTOMERS must be captured exactly once, not duplicated");
    }

    // 8. CREATE MATERIALIZED VIEW must not double-count FROM-clause tables either.
    @Test
    @DisplayName("8: CREATE MATERIALIZED VIEW mv_orders AS SELECT * FROM orders")
    void createMaterializedViewNoDuplicate() {
        String sql = """
                CREATE MATERIALIZED VIEW mv_orders AS
                SELECT * FROM orders WHERE status = 'X';
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CREATE MATERIALIZED VIEW");
        ParsedObject po = findObject(result, "MATERIALIZED_VIEW", "MV_ORDERS");
        dumpObject("8: CREATE MATERIALIZED VIEW", po);

        org.junit.jupiter.api.Assertions.assertNotNull(po);
        long ordersCount = po.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .count();
        org.junit.jupiter.api.Assertions.assertEquals(1, ordersCount,
                "ORDERS must be captured exactly once, not duplicated");
    }
}
