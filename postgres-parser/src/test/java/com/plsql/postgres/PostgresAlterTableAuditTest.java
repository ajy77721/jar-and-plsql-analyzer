package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Postgres ALTER TABLE support — regression tests for the gap this file originally audited.
 *
 * ORIGINAL AUDIT FINDING (kept for context): ALTER/TABLE/ADD/CONSTRAINT/FOREIGN/REFERENCES were
 * not lexer tokens at all, there was no alterTable rule, and compilationUnit had no ALTER
 * alternative. Consequences were:
 *   (a) a standalone top-level "ALTER TABLE ...;" could not match compilationUnit -> parse errors;
 *   (b) far worse, an ALTER TABLE embedded INSIDE a routine body matched `otherStatement`'s
 *       catch-all and was consumed token-by-token with ZERO reported errors and ZERO capture —
 *       so an FK dependency between two tables vanished with no signal at all.
 *
 * FIXED: `alterTable` (PostgresParser.g4) is now both a compilationUnit alternative and a
 * `statement` alternative placed ahead of `otherStatement`, and `visitAlterTable`
 * (PostgresAnalysisVisitor) records the altered table plus, for FOREIGN KEY constraints, the
 * referenced table as a real table op / lineage edge on both paths. These tests assert that.
 */
class PostgresAlterTableAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_obj", null);
    }

    private static PostgresRoutineInfo dump(String label, PostgresRoutineInfo r) {
        System.out.println("[" + label + "] name=" + r.getName() + " type=" + r.getType()
                + " tableOps=" + r.getTableOps() + " alterActions=" + r.getAlterActions()
                + " fkRefs=" + r.getAlterForeignKeyRefs() + " errors=" + r.getErrors());
        return r;
    }

    private static boolean hasTable(PostgresRoutineInfo r, String name) {
        return r.getTableOps().stream().anyMatch(op -> name.equalsIgnoreCase(op.getTableName()));
    }

    // ─────────────────────────────────────────────────────────────────────
    // 1. ADD COLUMN, with and without the COLUMN keyword — standalone top-level
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void addColumn_withKeyword_topLevel() {
        PostgresRoutineInfo r = dump("addColumn_withKeyword",
                parse("ALTER TABLE orders ADD COLUMN priority INT;"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(PostgresRoutineInfo.RoutineType.ALTER_TABLE, r.getType());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertEquals(java.util.List.of("ADD COLUMN"), r.getAlterActions());
    }

    @Test
    void addColumn_withoutKeyword_topLevel() {
        PostgresRoutineInfo r = dump("addColumn_withoutKeyword",
                parse("ALTER TABLE orders ADD priority INT;"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertEquals(java.util.List.of("ADD COLUMN"), r.getAlterActions());
    }

    // ─────────────────────────────────────────────────────────────────────
    // 2. THE KEY CASE — ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ...
    //    The FK edge (orders -> customers) must be captured.
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void addForeignKeyConstraint_topLevel_bothTablesCaptured() {
        String sql = "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) "
                + "REFERENCES customers(id);";
        PostgresRoutineInfo r = dump("addForeignKeyConstraint", parse(sql));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertTrue(hasTable(r, "customers"),
                "referenced table captured — the FK lineage edge survives: " + r.getTableOps());
        assertEquals(java.util.List.of("orders->customers"), r.getAlterForeignKeyRefs());

        // The string above names the two tables; the constraint is the column pairing, which
        // is in the parse tree and must be kept rather than re-guessed downstream.
        assertEquals(1, r.getForeignKeys().size(), "structured FK recorded: " + r.getForeignKeys());
        var fk = r.getForeignKeys().get(0);
        assertEquals("fk_customer", fk.getConstraintName());
        assertEquals("orders", fk.getTable());
        assertEquals("customers", fk.getReferencedTable());
        assertEquals(java.util.List.of("customer_id"), fk.getColumns());
        assertEquals(java.util.List.of("id"), fk.getReferencedColumns());
        assertTrue(fk.getLine() > 0, "line number makes the constraint navigable");
    }

    @Test
    void addForeignKeyConstraint_composite_keepsEveryColumnInOrder() {
        PostgresRoutineInfo r = dump("addForeignKeyConstraint_composite", parse(
                "ALTER TABLE order_lines ADD CONSTRAINT fk_ol FOREIGN KEY (order_id, line_no) "
                        + "REFERENCES sales.orders(id, line_no);"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(1, r.getForeignKeys().size());
        var fk = r.getForeignKeys().get(0);
        assertEquals(java.util.List.of("order_id", "line_no"), fk.getColumns());
        assertEquals(java.util.List.of("id", "line_no"), fk.getReferencedColumns());
        assertEquals("sales", fk.getReferencedSchema(),
                "the qualified parent keeps its schema — 'orders' alone is ambiguous");
    }

    @Test
    void addForeignKeyConstraint_withoutReferencedColumns_leavesThemEmpty() {
        // Omitting the parent column list means "the parent's primary key", which this
        // statement does not state. An empty list is the honest answer; inventing one is not.
        PostgresRoutineInfo r = dump("addForeignKeyConstraint_noRefCols", parse(
                "ALTER TABLE orders ADD FOREIGN KEY (customer_id) REFERENCES customers;"));

        assertEquals(1, r.getForeignKeys().size(), "FK still recorded: " + r.getForeignKeys());
        var fk = r.getForeignKeys().get(0);
        assertEquals(java.util.List.of("customer_id"), fk.getColumns());
        assertTrue(fk.getReferencedColumns().isEmpty());
        assertNull(fk.getConstraintName(), "an unnamed constraint is still a constraint");
    }

    @Test
    void addForeignKeyConstraint_unnamed_withOnDeleteTail() {
        PostgresRoutineInfo r = dump("addForeignKeyConstraint_unnamed", parse(
                "ALTER TABLE orders ADD FOREIGN KEY (customer_id) "
                        + "REFERENCES sales.customers(id) ON DELETE CASCADE;"));

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(hasTable(r, "customers"), "referenced table captured: " + r.getTableOps());
        assertEquals(java.util.List.of("orders->customers"), r.getAlterForeignKeyRefs());

        assertEquals(1, r.getForeignKeys().size());
        var fk = r.getForeignKeys().get(0);
        assertNull(fk.getConstraintName(), "unnamed constraint");
        assertEquals("sales", fk.getReferencedSchema());
        assertEquals(java.util.List.of("customer_id"), fk.getColumns());
        assertEquals(java.util.List.of("id"), fk.getReferencedColumns(),
                "an ON DELETE tail must not swallow the column pairing");
    }

    // ─────────────────────────────────────────────────────────────────────
    // 3. Other constraint kinds — PRIMARY KEY / CHECK / UNIQUE
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void addPrimaryKeyConstraint_topLevel_captured() {
        PostgresRoutineInfo r = dump("addPrimaryKeyConstraint",
                parse("ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (id);"));
        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertEquals(java.util.List.of("ADD PRIMARY KEY"), r.getAlterActions());
    }

    @Test
    void addCheckConstraint_topLevel_captured() {
        PostgresRoutineInfo r = dump("addCheckConstraint",
                parse("ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0);"));
        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertEquals(java.util.List.of("ADD CHECK"), r.getAlterActions());
    }

    @Test
    void addUniqueConstraint_topLevel_captured() {
        PostgresRoutineInfo r = dump("addUniqueConstraint",
                parse("ALTER TABLE orders ADD CONSTRAINT uq_code UNIQUE (code);"));
        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertEquals(java.util.List.of("ADD UNIQUE"), r.getAlterActions());
    }

    // ─────────────────────────────────────────────────────────────────────
    // 4. DROP COLUMN / DROP CONSTRAINT
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void dropColumn_topLevel_captured() {
        PostgresRoutineInfo r = dump("dropColumn", parse("ALTER TABLE orders DROP COLUMN priority;"));
        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertEquals(java.util.List.of("DROP COLUMN"), r.getAlterActions());
    }

    @Test
    void dropConstraint_topLevel_captured() {
        PostgresRoutineInfo r = dump("dropConstraint",
                parse("ALTER TABLE orders DROP CONSTRAINT fk_customer;"));
        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertEquals(java.util.List.of("DROP CONSTRAINT"), r.getAlterActions());
    }

    // ─────────────────────────────────────────────────────────────────────
    // 5. Multiple comma-separated actions in one statement.
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void multipleActions_allCaptured() {
        PostgresRoutineInfo r = dump("multipleActions", parse(
                "ALTER TABLE orders ADD COLUMN priority INT NOT NULL DEFAULT 0, "
                        + "ADD CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES customers(id), "
                        + "DROP COLUMN legacy_flag;"));
        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(java.util.List.of("ADD COLUMN", "ADD FOREIGN KEY", "DROP COLUMN"),
                r.getAlterActions());
        assertTrue(hasTable(r, "customers"), "FK target captured: " + r.getTableOps());
    }

    // ─────────────────────────────────────────────────────────────────────
    // 6. THE MOST IMPORTANT CASE — same ALTER TABLE ADD CONSTRAINT FK, embedded inside a
    //    routine BODY. This used to be swallowed by otherStatement with zero errors and zero
    //    capture; both tables must now be captured while the enclosing routine is unaffected.
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void addForeignKeyConstraint_insideRoutineBody_captured() {
        String sql =
                "CREATE PROCEDURE add_fk() AS $$\n"
                        + "BEGIN\n"
                        + "  ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY "
                        + "(customer_id) REFERENCES customers(id);\n"
                        + "END;\n"
                        + "$$ LANGUAGE plpgsql;";
        PostgresRoutineInfo r = dump("addForeignKeyConstraint_insideBody", parse(sql));

        assertEquals("add_fk", r.getName(), "enclosing routine identity is unchanged");
        assertEquals(PostgresRoutineInfo.RoutineType.PROCEDURE, r.getType(),
                "an embedded ALTER TABLE must not hijack the routine's own type");
        assertTrue(hasTable(r, "orders"),
                "altered table captured inside a routine body: " + r.getTableOps());
        assertTrue(hasTable(r, "customers"),
                "referenced table captured inside a routine body — the FK edge no longer "
                        + "vanishes silently: " + r.getTableOps());
        assertEquals(java.util.List.of("orders->customers"), r.getAlterForeignKeyRefs());
        assertEquals(1, r.getForeignKeys().size(),
                "an embedded ALTER TABLE records the structured FK too: " + r.getForeignKeys());
        assertEquals(java.util.List.of("customer_id"), r.getForeignKeys().get(0).getColumns());
    }

    @Test
    void alterTableInsideRoutineBody_alongsideOtherStatements() {
        String sql =
                "CREATE PROCEDURE mixed() AS $$\n"
                        + "BEGIN\n"
                        + "  INSERT INTO audit_log (msg) VALUES ('start');\n"
                        + "  ALTER TABLE orders ADD COLUMN priority INT;\n"
                        + "  SELECT count(*) FROM orders;\n"
                        + "END;\n"
                        + "$$ LANGUAGE plpgsql;";
        PostgresRoutineInfo r = dump("alterTableInBody_mixed", parse(sql));

        assertEquals("mixed", r.getName());
        assertTrue(hasTable(r, "audit_log"), "surrounding DML still captured: " + r.getTableOps());
        assertTrue(hasTable(r, "orders"), "altered table captured: " + r.getTableOps());
        assertTrue(r.getAlterActions().contains("ADD COLUMN"), r.getAlterActions().toString());
    }

    // ─────────────────────────────────────────────────────────────────────
    // 7. Less-common sub-forms must degrade permissively (no parse error), not hard-fail.
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void unrecognizedSubForm_degradesPermissively() {
        PostgresRoutineInfo r = dump("alterColumnType",
                parse("ALTER TABLE orders ALTER COLUMN qty TYPE BIGINT;"));
        assertTrue(r.getErrors().isEmpty(), "permissive tail must absorb it: " + r.getErrors());
        assertTrue(hasTable(r, "orders"), "altered table still captured: " + r.getTableOps());
        assertEquals(java.util.List.of("OTHER"), r.getAlterActions());
    }
}
