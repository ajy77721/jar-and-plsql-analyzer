package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2ForeignKeyInfo;
import com.plsql.db2.model.Db2RoutineInfo;
import com.plsql.db2.model.Db2TableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone ALTER TABLE support in db2-parser (added 2026-09-19; this file began life as a
 * read-only audit recording the gap and is now the regression suite for the fix).
 *
 * Before: compilationUnit was
 *     (routineDefinition | createTrigger | createView | createSequence | createIndex)? EOF
 * with no ALTER rule and no ALTER/TABLE/ADD/DROP/CONSTRAINT/FOREIGN/KEY/PRIMARY/CHECK/
 * REFERENCES lexer tokens at all, so every statement below died at the first token and
 * produced zero table ops — losing both the altered table and, worse, the FK's referenced
 * table (a real lineage edge).
 *
 * After: Db2Parser.g4 has an `alterTable` rule wired into compilationUnit, and
 * Db2AnalysisVisitor.visitAlterTable records
 *   - the altered table as an UPDATE-flavoured table op (the statement mutates it),
 *   - every ADD ... FOREIGN KEY ... REFERENCES target as a SELECT-flavoured table op,
 *   - a descriptor per action in Db2RoutineInfo.getAlterActions().
 */
class Db2AlterTableAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("type=" + r.getType());
        System.out.println("name=" + r.getName());
        System.out.println("errors=" + r.getErrors());
        System.out.println("alterTableName=" + r.getAlterTableName());
        System.out.println("alterActions=" + r.getAlterActions());
        System.out.println("tableOps.size()=" + r.getTableOps().size());
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable() + " alias=" + t.getAlias());
        }
    }

    private static boolean hasOp(Db2RoutineInfo r, Db2TableOpInfo.DmlType dml, String table) {
        return r.getTableOps().stream()
                .anyMatch(t -> t.getDmlType() == dml && table.equalsIgnoreCase(t.getTableName()));
    }

    /** Every ALTER TABLE parses cleanly, is typed ALTER_TABLE, and captures the altered table. */
    private Db2RoutineInfo parseOk(String sql, String label) {
        Db2RoutineInfo r = engine.parse(sql, label, null);
        dump(label, r);
        assertTrue(r.getErrors().isEmpty(), "expected a clean parse, got " + r.getErrors());
        assertEquals(Db2RoutineInfo.RoutineType.ALTER_TABLE, r.getType());
        assertEquals("ORDERS", r.getAlterTableName());
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.UPDATE, "orders"),
                "altered table 'orders' must be captured as an UPDATE-flavoured table op");
        return r;
    }

    // ── 1. Plain ADD COLUMN / DB2-style ADD (no COLUMN keyword) ────────────────

    @Test
    @DisplayName("ALTER TABLE orders ADD COLUMN priority INT")
    void alterTableAddColumnWithKeyword() {
        Db2RoutineInfo r = parseOk("ALTER TABLE orders ADD COLUMN priority INT;", "AddColumnWithKeyword");
        assertEquals(1, r.getTableOps().size());
        assertEquals(1, r.getAlterAddedColumns().size());
        assertEquals("priority", r.getAlterAddedColumns().get(0));
        assertEquals("ADD COLUMN PRIORITY INT", r.getAlterActions().get(0).toUpperCase());
    }

    @Test
    @DisplayName("ALTER TABLE orders ADD priority INT (DB2 syntax without COLUMN keyword)")
    void alterTableAddColumnWithoutKeyword() {
        Db2RoutineInfo r = parseOk("ALTER TABLE orders ADD priority INT;", "AddColumnWithoutKeyword");
        assertEquals(1, r.getTableOps().size());
        assertEquals("priority", r.getAlterAddedColumns().get(0));
    }

    // ── 2. THE KEY CASE: ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ────────

    @Test
    @DisplayName("ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id)")
    void alterTableAddForeignKeyConstraint() {
        Db2RoutineInfo r = parseOk(
                "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) "
                        + "REFERENCES customers(id);", "AddForeignKeyConstraint");

        // BOTH ends of the lineage edge are now captured.
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.UPDATE, "orders"), "altered table captured");
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.SELECT, "customers"),
                "referenced table 'customers' must be captured as a real relationship");
        assertEquals(2, r.getTableOps().size());
        assertEquals(1, r.getAlterReferencedTables().size());
        assertEquals("CUSTOMERS", r.getAlterReferencedTables().get(0));

        String action = r.getAlterActions().get(0).toUpperCase();
        assertTrue(action.contains("FK_CUSTOMER"), action);
        assertTrue(action.contains("FOREIGN KEY"), action);
        assertTrue(action.contains("REFERENCES CUSTOMERS"), action);

        // The structured form: the descriptor string above is prose, and the referenced
        // column list ("id") appears nowhere in it. The pairing is the constraint.
        assertEquals(1, r.getForeignKeys().size());
        Db2ForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals("fk_customer", fk.getConstraintName());
        assertEquals("ORDERS", fk.getTable());
        assertEquals("CUSTOMERS", fk.getReferencedTable());
        assertEquals(List.of("customer_id"), fk.getColumns());
        assertEquals(List.of("id"), fk.getReferencedColumns());
        assertTrue(fk.getLine() > 0);
    }

    @Test
    @DisplayName("Composite FK keeps every column on both sides, in declaration order")
    void alterTableAddCompositeForeignKey() {
        Db2RoutineInfo r = parseOk(
                "ALTER TABLE orders ADD CONSTRAINT fk_ol FOREIGN KEY (order_id, line_no) "
                        + "REFERENCES order_lines (id, line_no);", "AddCompositeForeignKey");

        assertEquals(1, r.getForeignKeys().size());
        Db2ForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals(List.of("order_id", "line_no"), fk.getColumns());
        assertEquals(List.of("id", "line_no"), fk.getReferencedColumns(),
                "order is the pairing — a set would lose which column matches which");
    }

    @Test
    @DisplayName("FK with no referenced column list (defaults to the parent PK) is still a FK")
    void alterTableAddForeignKeyWithoutReferencedColumns() {
        Db2RoutineInfo r = parseOk(
                "ALTER TABLE orders ADD FOREIGN KEY (customer_id) REFERENCES customers;",
                "AddForeignKeyNoRefCols");

        assertEquals(1, r.getForeignKeys().size());
        Db2ForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertNull(fk.getConstraintName(), "an unnamed constraint is still a constraint");
        assertEquals(List.of("customer_id"), fk.getColumns());
        assertTrue(fk.getReferencedColumns().isEmpty(),
                "omitting the parent columns is legal DB2, not missing data");
    }

    @Test
    @DisplayName("Schema-qualified FK: ALTER TABLE app.orders ... REFERENCES ref.customers(id)")
    void alterTableAddForeignKeyQualified() {
        Db2RoutineInfo r = engine.parse(
                "ALTER TABLE app.orders ADD CONSTRAINT fk_c FOREIGN KEY (customer_id) "
                        + "REFERENCES ref.customers(id);", "qualified_fk", null);
        dump("AddForeignKeyQualified", r);
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals("APP.ORDERS", r.getAlterTableName());
        assertEquals("REF.CUSTOMERS", r.getAlterReferencedTables().get(0));
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.SELECT, "customers"));

        Db2ForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals("APP", fk.getSchema());
        assertEquals("ORDERS", fk.getTable());
        assertEquals("REF", fk.getReferencedSchema(),
                "the parent's schema is what makes the edge resolvable across schemas");
        assertEquals("CUSTOMERS", fk.getReferencedTable());
    }

    @Test
    @DisplayName("FK with ON DELETE CASCADE trailing referential action still parses")
    void alterTableAddForeignKeyWithOnDelete() {
        Db2RoutineInfo r = parseOk(
                "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) "
                        + "REFERENCES customers(id) ON DELETE CASCADE;", "AddForeignKeyOnDelete");
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.SELECT, "customers"));
        assertEquals(1, r.getForeignKeys().size(),
                "a trailing referential action must not hide the constraint");
        assertEquals(List.of("id"), r.getForeignKeys().get(0).getReferencedColumns());
    }

    @Test
    @DisplayName("Column-level FK: ALTER TABLE order_lines ADD COLUMN customer_id INT REFERENCES customers(id)")
    void alterTableAddColumnWithInlineReferences() {
        Db2RoutineInfo r = engine.parse(
                "ALTER TABLE order_lines ADD COLUMN customer_id INT REFERENCES customers(id);",
                "inline_column_fk", null);
        dump("AddColumnInlineReferences", r);
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals("ORDER_LINES", r.getAlterTableName());
        assertEquals(List.of("customer_id"), r.getAlterAddedColumns());

        // Both ends of the edge: the inline spelling is the same relationship as the
        // table-level ADD CONSTRAINT … FOREIGN KEY form, so it lands in the same list.
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.UPDATE, "order_lines"));
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.SELECT, "customers"),
                "the referenced table must be captured even in the column-level spelling");
        assertEquals(List.of("CUSTOMERS"), r.getAlterReferencedTables());

        assertEquals(1, r.getForeignKeys().size());
        Db2ForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals("ORDER_LINES", fk.getTable());
        assertEquals("CUSTOMERS", fk.getReferencedTable());
        assertEquals(List.of("customer_id"), fk.getColumns(),
                "the column being added is itself the single source column");
        assertEquals(List.of("id"), fk.getReferencedColumns());
        assertTrue(fk.getLine() > 0);
    }

    @Test
    @DisplayName("Column-level FK with no parent column list, NOT NULL and DEFAULT still captured")
    void alterTableAddColumnInlineReferencesNoParentColumns() {
        Db2RoutineInfo r = engine.parse(
                "ALTER TABLE order_lines ADD COLUMN customer_id INT NOT NULL DEFAULT 0 "
                        + "REFERENCES customers;", "inline_column_fk_no_cols", null);
        dump("AddColumnInlineReferencesNoCols", r);
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(1, r.getForeignKeys().size());
        Db2ForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals(List.of("customer_id"), fk.getColumns());
        assertTrue(fk.getReferencedColumns().isEmpty(),
                "omitting the parent columns is legal DB2, not missing data");
    }

    // ── 3. Other ADD CONSTRAINT variants: PRIMARY KEY / CHECK / UNIQUE ────────

    @Test
    @DisplayName("ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (id)")
    void alterTableAddPrimaryKeyConstraint() {
        Db2RoutineInfo r = parseOk("ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (id);",
                "AddPrimaryKeyConstraint");
        assertEquals(1, r.getTableOps().size());
        String action = r.getAlterActions().get(0).toUpperCase();
        assertTrue(action.contains("PK_ORDERS"), action);
        assertTrue(action.contains("PRIMARY KEY"), action);
    }

    @Test
    @DisplayName("ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0)")
    void alterTableAddCheckConstraint() {
        Db2RoutineInfo r = parseOk("ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0);",
                "AddCheckConstraint");
        String action = r.getAlterActions().get(0).toUpperCase();
        assertTrue(action.contains("CHK_QTY"), action);
        assertTrue(action.contains("CHECK"), action);
        assertTrue(action.contains("QTY > 0"), action);
    }

    @Test
    @DisplayName("ALTER TABLE orders ADD CONSTRAINT uq_code UNIQUE (code)")
    void alterTableAddUniqueConstraint() {
        Db2RoutineInfo r = parseOk("ALTER TABLE orders ADD CONSTRAINT uq_code UNIQUE (code);",
                "AddUniqueConstraint");
        String action = r.getAlterActions().get(0).toUpperCase();
        assertTrue(action.contains("UQ_CODE"), action);
        assertTrue(action.contains("UNIQUE"), action);
    }

    // ── 4. DROP COLUMN / DROP CONSTRAINT ───────────────────────────────────────

    @Test
    @DisplayName("ALTER TABLE orders DROP COLUMN priority")
    void alterTableDropColumn() {
        Db2RoutineInfo r = parseOk("ALTER TABLE orders DROP COLUMN priority;", "DropColumn");
        assertEquals(1, r.getAlterDroppedColumns().size());
        assertEquals("priority", r.getAlterDroppedColumns().get(0));
        assertEquals("DROP COLUMN PRIORITY", r.getAlterActions().get(0).toUpperCase());
    }

    @Test
    @DisplayName("ALTER TABLE orders DROP CONSTRAINT fk_customer")
    void alterTableDropConstraint() {
        Db2RoutineInfo r = parseOk("ALTER TABLE orders DROP CONSTRAINT fk_customer;", "DropConstraint");
        // DROP CONSTRAINT must win over the generic DROP [COLUMN] col form, which would
        // otherwise read CONSTRAINT itself as the column name (CONSTRAINT is a legal `id`).
        assertTrue(r.getAlterDroppedColumns().isEmpty(), "not a column drop");
        assertEquals("DROP CONSTRAINT FK_CUSTOMER", r.getAlterActions().get(0).toUpperCase());
    }

    // ── 5. Multiple actions in one statement ──────────────────────────────────

    @Test
    @DisplayName("ALTER TABLE orders ADD COLUMN a INT ADD CONSTRAINT fk FOREIGN KEY (b) REFERENCES c (id)")
    void alterTableMultipleActions() {
        Db2RoutineInfo r = parseOk(
                "ALTER TABLE orders ADD COLUMN a INT "
                        + "ADD CONSTRAINT fk_c FOREIGN KEY (b) REFERENCES customers (id);",
                "MultipleActions");
        assertEquals(2, r.getAlterActions().size());
        assertTrue(hasOp(r, Db2TableOpInfo.DmlType.SELECT, "customers"));
        assertEquals("a", r.getAlterAddedColumns().get(0));
    }

    // ── 6. Keywords-as-identifiers must still work after the new lexer tokens ──

    @Test
    @DisplayName("New keyword tokens (KEY/CHECK/TABLE/COLUMN/TYPE…) remain usable as identifiers")
    void newKeywordsStillUsableAsIdentifiers() {
        Db2RoutineInfo r = engine.parse(
                "ALTER TABLE orders ADD COLUMN key INT;", "keyword_col", null);
        dump("KeywordAsIdentifier", r);
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals("key", r.getAlterAddedColumns().get(0));
    }
}
