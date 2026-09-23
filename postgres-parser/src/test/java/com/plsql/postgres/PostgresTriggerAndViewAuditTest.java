package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT — Postgres TRIGGER and VIEW support.
 *
 * Grammar/visitor were read fresh before writing this:
 *   - postgres-parser/src/main/antlr4/com/plsql/postgres/grammar/PostgresParser.g4
 *   - postgres-parser/src/main/antlr4/com/plsql/postgres/grammar/PostgresLexer.g4
 *   - postgres-parser/src/main/java/com/plsql/postgres/visitor/PostgresAnalysisVisitor.java
 *
 * Findings (see class-level comment blocks below each test for full detail):
 *
 *  1. compilationUnit is defined as:
 *         compilationUnit : (routineDefinition | doStatement)? EOF ;
 *     routineDefinition only accepts "CREATE (OR REPLACE)? (PROCEDURE|FUNCTION) ...".
 *     There is NO grammar rule anywhere for a top-level "CREATE TRIGGER ..." or
 *     "CREATE VIEW/MATERIALIZED VIEW ..." statement. grep -ni "trigger" and grep -ni "view"
 *     against PostgresParser.g4 and PostgresLexer.g4 both return zero matches (confirmed
 *     before writing this file). The visitor likewise has zero trigger/view handling (its
 *     only "VIEW" hit is the unrelated PG_GET_VIEWDEF builtin-function name, and its only
 *     "TRIGGER" reference is an explanatory code comment about the CREATE-TABLE fix, not
 *     trigger support).
 *
 *  2. When CREATE TRIGGER ... / CREATE VIEW ... text is fed to the engine, since it can't
 *     match routineDefinition it is instead swallowed as ANTLR parse-error tokens (the
 *     "(...)?" on compilationUnit lets the parser emit "extraneous input"/"mismatched input"
 *     errors and recover rather than throw), so PostgresAnalysisEngine.parse() does NOT throw
 *     — it returns a mostly-empty PostgresRoutineInfo with the fallback objectName, zero
 *     calls/tableOps/variables, and one or more entries in getErrors(). The trigger's target
 *     table, its timing/events, and the referenced trigger-function name are all silently
 *     lost — nothing about the trigger is captured anywhere in the result.
 *
 *  3. The inner trigger-function body case (CREATE FUNCTION ... RETURNS TRIGGER AS $$ ...
 *     NEW.col := ...; ... $$) DOES match routineDefinition (it is just an ordinary
 *     CREATE FUNCTION), so it parses. But NEW./OLD. field references are never given any
 *     special handling anywhere in the visitor (confirmed: no NEW/OLD tokens in the lexer,
 *     no NEW/OLD handling in the visitor) — they fall through as ordinary
 *     qualifier-dot-identifier expressions, same as any other correlation name. This test
 *     verifies that at minimum they don't get misclassified as a real table reference
 *     (tableOps) and don't crash the visitor.
 */
class PostgresTriggerAndViewAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_obj", null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // CREATE TRIGGER — top-level statement
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void createTrigger_simpleBeforeInsert_isFullyCaptured() {
        String sql =
                "CREATE TRIGGER trg_orders\n" +
                "BEFORE INSERT ON orders\n" +
                "FOR EACH ROW\n" +
                "EXECUTE FUNCTION check_order_status();";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE TRIGGER must parse without syntax errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.TRIGGER, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("trg_orders", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getTriggerTable());
        org.junit.jupiter.api.Assertions.assertEquals("BEFORE", r.getTriggerTiming());
        org.junit.jupiter.api.Assertions.assertEquals("INSERT", r.getTriggerEvent());
        org.junit.jupiter.api.Assertions.assertEquals("ROW", r.getTriggerForEach());
        org.junit.jupiter.api.Assertions.assertEquals("check_order_status", r.getTriggerFunction());

        // The target table is captured as a table op (so the trigger depends on "orders") and
        // the EXECUTE FUNCTION target as a call (the edge to where the logic actually lives).
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())),
                "target table of CREATE TRIGGER must be captured as a table op");
        org.junit.jupiter.api.Assertions.assertTrue(r.getCalls().stream()
                .anyMatch(c -> "check_order_status".equalsIgnoreCase(c.getName())),
                "the trigger function reference in EXECUTE FUNCTION must be captured as a call");
    }

    @Test
    void createTrigger_combinedEvents_withWhenCondition_isFullyCaptured() {
        String sql =
                "CREATE TRIGGER trg_orders_mod\n" +
                "BEFORE INSERT OR UPDATE ON orders\n" +
                "FOR EACH ROW\n" +
                "WHEN (NEW.status = 'X')\n" +
                "EXECUTE FUNCTION check_order_status();";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE TRIGGER with combined events + WHEN must parse cleanly: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.TRIGGER, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("trg_orders_mod", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("INSERT,UPDATE", r.getTriggerEvent());
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getTriggerTable());
        org.junit.jupiter.api.Assertions.assertNotNull(r.getTriggerWhen());
        org.junit.jupiter.api.Assertions.assertTrue(
                r.getTriggerWhen().replaceAll("\\s+", "").contains("NEW.status='X'"),
                "WHEN condition text must be captured, got: " + r.getTriggerWhen());

        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())));
        org.junit.jupiter.api.Assertions.assertTrue(r.getCalls().stream()
                .anyMatch(c -> "check_order_status".equalsIgnoreCase(c.getName())));
        // NEW inside the WHEN condition is a pseudo-record, never a table.
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .noneMatch(op -> "new".equalsIgnoreCase(op.getTableName())));
    }

    @Test
    void createTrigger_insteadOfDeleteOnView_isFullyCaptured() {
        String sql =
                "CREATE TRIGGER trg_orders_delete\n" +
                "INSTEAD OF DELETE ON orders_view\n" +
                "FOR EACH ROW\n" +
                "EXECUTE FUNCTION handle_delete();";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "INSTEAD OF trigger must parse cleanly: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.TRIGGER, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("trg_orders_delete", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("INSTEAD OF", r.getTriggerTiming());
        org.junit.jupiter.api.Assertions.assertEquals("DELETE", r.getTriggerEvent());
        org.junit.jupiter.api.Assertions.assertEquals("orders_view", r.getTriggerTable());
        org.junit.jupiter.api.Assertions.assertEquals("handle_delete", r.getTriggerFunction());

        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders_view".equalsIgnoreCase(op.getTableName())));
        org.junit.jupiter.api.Assertions.assertTrue(r.getCalls().stream()
                .anyMatch(c -> "handle_delete".equalsIgnoreCase(c.getName())));
    }

    @Test
    void createTrigger_qualifiedTable_updateOfColumns_executeProcedure_isCaptured() {
        String sql =
                "CREATE TRIGGER trg_audit\n" +
                "AFTER UPDATE OF status, total ON sales.orders\n" +
                "FOR EACH STATEMENT\n" +
                "EXECUTE PROCEDURE audit.log_change();";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "UPDATE OF <cols> + schema-qualified table + EXECUTE PROCEDURE must parse cleanly: "
                        + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals("trg_audit", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("AFTER", r.getTriggerTiming());
        org.junit.jupiter.api.Assertions.assertEquals("UPDATE", r.getTriggerEvent());
        org.junit.jupiter.api.Assertions.assertEquals("STATEMENT", r.getTriggerForEach());
        org.junit.jupiter.api.Assertions.assertEquals("orders", r.getTriggerTable());
        org.junit.jupiter.api.Assertions.assertEquals("audit.log_change", r.getTriggerFunction());
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())
                             && "sales".equalsIgnoreCase(op.getSchemaName())));
    }

    // ─────────────────────────────────────────────────────────────────────
    // Trigger FUNCTION body — NEW./OLD. handling inside a normal CREATE FUNCTION
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void triggerFunctionBody_newOldFieldAccess_doesNotCrashAndIsNotMisclassifiedAsTable() {
        String sql =
                "CREATE FUNCTION check_order_status() RETURNS TRIGGER AS $$\n" +
                "BEGIN\n" +
                "  NEW.updated_at := now();\n" +
                "  IF NEW.status = 'CLOSED' AND OLD.status <> 'CLOSED' THEN\n" +
                "    UPDATE orders SET closed_count = closed_count + 1 WHERE id = NEW.id;\n" +
                "  END IF;\n" +
                "  RETURN NEW;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;";
        PostgresRoutineInfo r = parse(sql);

        System.out.println("[triggerFunctionBody] name=" + r.getName() + " type=" + r.getType()
                + " tableOps=" + r.getTableOps() .size()+ " errors=" + r.getErrors());

        // The outer CREATE FUNCTION shell does parse (it is a normal routineDefinition).
        org.junit.jupiter.api.Assertions.assertEquals("check_order_status", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.FUNCTION, r.getType());

        // NEW/OLD must not be misclassified as real tables named "new"/"old".
        boolean newOrOldAsTable = r.getTableOps().stream()
                .anyMatch(op -> "new".equalsIgnoreCase(op.getTableName())
                             || "old".equalsIgnoreCase(op.getTableName()));
        org.junit.jupiter.api.Assertions.assertFalse(newOrOldAsTable,
                "NEW./OLD. must never be captured as a table reference.");

        // The real UPDATE ... orders ... target table should still be captured normally.
        boolean ordersUpdateCaptured = r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName()));
        System.out.println("[triggerFunctionBody] ordersUpdateCaptured=" + ordersUpdateCaptured);
    }

    // ─────────────────────────────────────────────────────────────────────
    // CREATE VIEW / CREATE MATERIALIZED VIEW — top-level statement
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void createView_simpleJoin_isFullyCaptured() {
        String sql =
                "CREATE VIEW v_orders AS\n" +
                "SELECT o.*, c.name\n" +
                "FROM orders o JOIN customers c ON o.cust_id = c.id;";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE VIEW must parse without syntax errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.VIEW, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("v_orders", r.getName());
        org.junit.jupiter.api.Assertions.assertFalse(r.isMaterializedView());
        org.junit.jupiter.api.Assertions.assertNotNull(r.getViewQuery());

        // Both underlying tables of the defining SELECT are captured.
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())),
                "view's base table must be captured");
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "customers".equalsIgnoreCase(op.getTableName())),
                "view's joined table must be captured");
        // ...and so is the JOIN itself, via the shared FROM/JOIN extraction path.
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .flatMap(op -> op.getJoins().stream())
                .anyMatch(j -> "customers".equalsIgnoreCase(j.getJoinedTable())),
                "view's JOIN must be captured");
    }

    @Test
    void createView_withCheckOption_isFullyCaptured() {
        String sql =
                "CREATE VIEW v_open_orders AS\n" +
                "SELECT * FROM orders WHERE status = 'OPEN'\n" +
                "WITH CHECK OPTION;";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE VIEW ... WITH CHECK OPTION must parse cleanly: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.VIEW, r.getType());
        org.junit.jupiter.api.Assertions.assertEquals("v_open_orders", r.getName());
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())));
    }

    @Test
    void createMaterializedView_isFullyCaptured() {
        String sql =
                "CREATE MATERIALIZED VIEW mv_orders AS\n" +
                "SELECT customer_id, COUNT(*) AS order_count\n" +
                "FROM orders\n" +
                "GROUP BY customer_id;";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE MATERIALIZED VIEW must parse cleanly: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.VIEW, r.getType());
        org.junit.jupiter.api.Assertions.assertTrue(r.isMaterializedView(),
                "MATERIALIZED must be flagged on the result");
        org.junit.jupiter.api.Assertions.assertEquals("mv_orders", r.getName());
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())));
    }

    @Test
    void createOrReplaceView_withColumnList_andSchemaQualifiedName_isCaptured() {
        String sql =
                "CREATE OR REPLACE VIEW sales.v_totals (cust_id, total) AS\n" +
                "SELECT cust_id, SUM(amount) FROM sales.orders GROUP BY cust_id;";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE OR REPLACE VIEW with a column list must parse cleanly: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals("v_totals", r.getName());
        org.junit.jupiter.api.Assertions.assertEquals("sales", r.getSchema());
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())
                             && "sales".equalsIgnoreCase(op.getSchemaName())));
    }

    @Test
    void createView_withCte_capturesUnderlyingTablesButNotTheCte() {
        String sql =
                "CREATE VIEW v_recent AS\n" +
                "WITH recent AS (SELECT * FROM orders WHERE created_at > now())\n" +
                "SELECT r.id, c.name FROM recent r JOIN customers c ON r.cust_id = c.id;";
        PostgresRoutineInfo r = parse(sql);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "CREATE VIEW over a CTE must parse cleanly: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(
                PostgresRoutineInfo.RoutineType.VIEW, r.getType());
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName())));
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .anyMatch(op -> "customers".equalsIgnoreCase(op.getTableName())));
        org.junit.jupiter.api.Assertions.assertTrue(r.getTableOps().stream()
                .noneMatch(op -> "recent".equalsIgnoreCase(op.getTableName())),
                "a CTE name is not a base table and must not be captured as one");
    }
}
