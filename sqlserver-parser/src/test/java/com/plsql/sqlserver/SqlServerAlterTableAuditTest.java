package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerForeignKeyInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone (top-level, outside any routine body) {@code ALTER TABLE} statements and
 * constraint clauses — lineage capture in the SQL Server dialect.
 *
 * HISTORY (2026-09-19 audit -> fix): compilationUnit had no alterTable alternative, so a bare
 * "ALTER TABLE ..." was only half-recovered by SqlServerAnalysisVisitor's KEYWORD_OP_PATTERN
 * text scan, which captures the name after ALTER TABLE but knows nothing about REFERENCES —
 * the foreign key's referenced table (the whole point of the lineage) was never recorded.
 *
 * FIXED by: CONSTRAINT / FOREIGN / REFERENCES lexer keywords (re-admitted as ordinary
 * identifiers via exprAtom), an {@code alterTable} parser rule in compilationUnit that parses
 * ADD ... FOREIGN KEY (cols) REFERENCES t (cols) structurally and eats every other sub-form
 * permissively, a {@code visitAlterTable} capture recording the altered table as an ALTER op
 * and the referenced table as a SELECT (read-through dependency) op, and matching
 * {@code alterTableName / alterAction / alterConstraintName} fields plus a structured
 * {@code foreignKeys} list on SqlServerRoutineInfo.
 *
 * HISTORY (follow-up fix): the foreign key was first captured into single-valued
 * {@code alterFkReferencedTable / alterFkColumns / alterFkReferencedColumns} fields. One
 * ALTER TABLE may add SEVERAL foreign keys in one comma-separated ADD list, and those fields
 * could hold only one — the grammar swallowed the rest into the permissive tail and they were
 * lost. They are replaced by {@code List<SqlServerForeignKeyInfo> foreignKeys}; see
 * {@link #alterTableAddTwoForeignKeyConstraints()}.
 *
 * These are now hard pass/fail assertions.
 */
@DisplayName("SQL Server standalone ALTER TABLE / constraint clause lineage audit")
public class SqlServerAlterTableAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_alter_table", null);
    }

    private void dump(String label, String sql, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("SQL: " + sql);
        System.out.println("type=" + r.getType() + " name=" + r.getName() + " schema=" + r.getSchema());
        System.out.println("errors=" + r.getErrors());
        System.out.println("tableOps=" + r.getTableOps().size());
        for (SqlServerTableOpInfo op : r.getTableOps()) {
            System.out.println("  op: dml=" + op.getDmlType()
                    + " table=" + op.getTableName()
                    + " alias=" + op.getAlias()
                    + " line=" + op.getLine()
                    + " sqlText=" + op.getSqlText());
        }
        System.out.println();
    }

    @Test
    @DisplayName("1. ALTER TABLE orders ADD priority INT; (no COLUMN keyword) — is 'orders' captured?")
    void alterTableAddColumnNoColumnKeyword() {
        String sql = "ALTER TABLE orders ADD priority INT;";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 1: ADD column without COLUMN keyword", sql, r);
        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals("orders", r.getAlterTableName());
        assertEquals("ADD", r.getAlterAction());
        assertTrue(r.getTableOps().stream().anyMatch(op ->
                        "orders".equalsIgnoreCase(op.getTableName())
                                && op.getDmlType() == SqlServerTableOpInfo.DmlType.ALTER),
                "altered table must be captured as an ALTER op");
    }

    @Test
    @DisplayName("2. KEY CASE: ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY ... REFERENCES customers(id); "
            + "does it capture BOTH orders and customers?")
    void alterTableAddForeignKeyConstraint() {
        String sql = "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id);";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 2: ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES customers", sql, r);

        boolean sawOrders = r.getTableOps().stream()
                .anyMatch(op -> "orders".equalsIgnoreCase(op.getTableName()));
        boolean sawCustomers = r.getTableOps().stream()
                .anyMatch(op -> "customers".equalsIgnoreCase(op.getTableName()));
        System.out.println("case 2 result: sawOrders(altered)=" + sawOrders
                + " sawCustomers(referenced)=" + sawCustomers);

        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertTrue(sawOrders, "the ALTERED table must be captured");
        assertTrue(sawCustomers, "KEY FIX: the REFERENCED table must be captured too");
        assertTrue(r.getTableOps().stream().anyMatch(op ->
                        "orders".equalsIgnoreCase(op.getTableName())
                                && op.getDmlType() == SqlServerTableOpInfo.DmlType.ALTER),
                "orders is the ALTER target");
        assertTrue(r.getTableOps().stream().anyMatch(op ->
                        "customers".equalsIgnoreCase(op.getTableName())
                                && op.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT),
                "customers is a read-through FK dependency");
        assertEquals("orders", r.getAlterTableName());
        assertEquals("ADD", r.getAlterAction());
        assertEquals("fk_customer", r.getAlterConstraintName());

        assertEquals(1, r.getForeignKeys().size());
        SqlServerForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals("fk_customer", fk.getConstraintName());
        assertEquals("orders", fk.getTable());
        assertEquals("customers", fk.getReferencedTable());
        assertEquals(java.util.List.of("customer_id"), fk.getColumns());
        assertEquals(java.util.List.of("id"), fk.getReferencedColumns());
        assertTrue(fk.getLine() > 0, "a line number makes the constraint navigable in the source");
    }

    /**
     * THE BUG THIS REPLACED: two FOREIGN KEY constraints in ONE ALTER TABLE. With the old
     * single-valued alterFk* fields — and the old grammar rule, which matched exactly one
     * constraint and let the permissive tail eat the rest — only fk_order survived. Both must
     * now be captured, with their own column pairings and both referenced tables as ops.
     */
    @Test
    @DisplayName("2b. KEY FIX: TWO FOREIGN KEY constraints in one ALTER TABLE — both captured")
    void alterTableAddTwoForeignKeyConstraints() {
        String sql = "ALTER TABLE order_lines "
                + "ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id), "
                + "CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id);";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 2b: two FK constraints in one ALTER TABLE", sql, r);

        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals("order_lines", r.getAlterTableName());
        assertEquals("ADD", r.getAlterAction());

        assertEquals(2, r.getForeignKeys().size(),
                "BOTH constraints must survive — the second was silently dropped before");

        SqlServerForeignKeyInfo first = r.getForeignKeys().get(0);
        assertEquals("fk_order", first.getConstraintName());
        assertEquals("order_lines", first.getTable());
        assertEquals("orders", first.getReferencedTable());
        assertEquals(java.util.List.of("order_id"), first.getColumns());
        assertEquals(java.util.List.of("id"), first.getReferencedColumns());

        SqlServerForeignKeyInfo second = r.getForeignKeys().get(1);
        assertEquals("fk_product", second.getConstraintName());
        assertEquals("order_lines", second.getTable());
        assertEquals("products", second.getReferencedTable());
        assertEquals(java.util.List.of("product_id"), second.getColumns());
        assertEquals(java.util.List.of("id"), second.getReferencedColumns());

        assertTrue(r.getTableOps().stream().anyMatch(op ->
                        "orders".equalsIgnoreCase(op.getTableName())
                                && op.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT),
                "orders is a read-through FK dependency");
        assertTrue(r.getTableOps().stream().anyMatch(op ->
                        "products".equalsIgnoreCase(op.getTableName())
                                && op.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT),
                "products is a read-through FK dependency — the one that used to vanish");
    }

    @Test
    @DisplayName("2b-ii. KEY FIX: referential actions between comma-separated FK constraints")
    void alterTableAddTwoForeignKeysWithReferentialActions() {
        String sql = "ALTER TABLE order_lines "
                + "ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE, "
                + "CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id);";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 2b-ii: ON DELETE CASCADE between two FK constraints", sql, r);

        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals("order_lines", r.getAlterTableName());
        assertEquals("ADD", r.getAlterAction());

        assertEquals(2, r.getForeignKeys().size(),
                "ON DELETE CASCADE must not let the greedy tail swallow the second constraint");

        SqlServerForeignKeyInfo first = r.getForeignKeys().get(0);
        assertEquals("fk_order", first.getConstraintName());
        assertEquals("order_lines", first.getTable());
        assertEquals("orders", first.getReferencedTable());
        assertEquals(java.util.List.of("order_id"), first.getColumns());
        assertEquals(java.util.List.of("id"), first.getReferencedColumns());

        SqlServerForeignKeyInfo second = r.getForeignKeys().get(1);
        assertEquals("fk_product", second.getConstraintName());
        assertEquals("order_lines", second.getTable());
        assertEquals("products", second.getReferencedTable());
        assertEquals(java.util.List.of("product_id"), second.getColumns());
        assertEquals(java.util.List.of("id"), second.getReferencedColumns());

        assertTrue(r.getTableOps().stream().anyMatch(op ->
                        "products".equalsIgnoreCase(op.getTableName())
                                && op.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT),
                "products is a read-through FK dependency");
    }

    @Test
    @DisplayName("2b-iii. ON DELETE + ON UPDATE on both comma-separated FK constraints")
    void alterTableAddTwoForeignKeysWithBothReferentialActions() {
        String sql = "ALTER TABLE order_lines "
                + "ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id) "
                + "ON DELETE CASCADE ON UPDATE NO ACTION, "
                + "CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id) "
                + "ON UPDATE SET NULL ON DELETE SET DEFAULT;";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 2b-iii: ON DELETE + ON UPDATE on both constraints", sql, r);

        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals(2, r.getForeignKeys().size(), "both constraints must survive");
        assertEquals("fk_order", r.getForeignKeys().get(0).getConstraintName());
        assertEquals("orders", r.getForeignKeys().get(0).getReferencedTable());
        assertEquals("fk_product", r.getForeignKeys().get(1).getConstraintName());
        assertEquals("products", r.getForeignKeys().get(1).getReferencedTable());
    }

    @Test
    @DisplayName("2c. composite FK keeps every column, in declaration order")
    void alterTableAddCompositeForeignKey() {
        String sql = "ALTER TABLE order_lines "
                + "ADD FOREIGN KEY (order_id, line_no) REFERENCES orders(id, line_no);";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 2c: composite FK", sql, r);

        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals(1, r.getForeignKeys().size());
        SqlServerForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertNull(fk.getConstraintName(), "an unnamed constraint is still a constraint");
        assertEquals(java.util.List.of("order_id", "line_no"), fk.getColumns());
        assertEquals(java.util.List.of("id", "line_no"), fk.getReferencedColumns());
    }

    @Test
    @DisplayName("3a. ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (id);")
    void alterTableAddPrimaryKeyConstraint() {
        String sql = "ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (id);";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 3a: ADD CONSTRAINT ... PRIMARY KEY", sql, r);
        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals("orders", r.getAlterTableName());
        assertEquals("ADD", r.getAlterAction());
        assertEquals("pk_orders", r.getAlterConstraintName());
        assertTrue(r.getForeignKeys().isEmpty(), "not a foreign key");
    }

    @Test
    @DisplayName("3b. ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0);")
    void alterTableAddCheckConstraint() {
        String sql = "ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0);";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 3b: ADD CONSTRAINT ... CHECK", sql, r);
        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals("orders", r.getAlterTableName());
        assertEquals("ADD", r.getAlterAction());
        assertEquals("chk_qty", r.getAlterConstraintName());
        assertTrue(r.getForeignKeys().isEmpty(), "not a foreign key");
    }

    @Test
    @DisplayName("3c. ALTER TABLE orders ADD CONSTRAINT uq_code UNIQUE (code);")
    void alterTableAddUniqueConstraint() {
        String sql = "ALTER TABLE orders ADD CONSTRAINT uq_code UNIQUE (code);";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 3c: ADD CONSTRAINT ... UNIQUE", sql, r);
        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals("orders", r.getAlterTableName());
        assertEquals("ADD", r.getAlterAction());
        assertEquals("uq_code", r.getAlterConstraintName());
        assertTrue(r.getForeignKeys().isEmpty(), "not a foreign key");
    }

    @Test
    @DisplayName("4a. ALTER TABLE orders DROP COLUMN priority;")
    void alterTableDropColumn() {
        String sql = "ALTER TABLE orders DROP COLUMN priority;";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 4a: DROP COLUMN", sql, r);
        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals("orders", r.getAlterTableName());
        assertEquals("DROP", r.getAlterAction());
    }

    @Test
    @DisplayName("4b. ALTER TABLE orders DROP CONSTRAINT fk_customer;")
    void alterTableDropConstraint() {
        String sql = "ALTER TABLE orders DROP CONSTRAINT fk_customer;";
        SqlServerRoutineInfo r = parse(sql);
        dump("case 4b: DROP CONSTRAINT", sql, r);
        assertTrue(r.getErrors().isEmpty(), "must parse cleanly: " + r.getErrors());
        assertEquals("orders", r.getAlterTableName());
        assertEquals("DROP", r.getAlterAction());
        assertEquals("fk_customer", r.getAlterConstraintName());
    }
}
