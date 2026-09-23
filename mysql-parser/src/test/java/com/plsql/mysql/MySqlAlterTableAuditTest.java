package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlForeignKeyInfo;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ALTER TABLE / constraint-clause lineage capture for mysql-parser.
 *
 * <p>History: this file began (2026-09-19) as a read-only audit asserting a documented GAP —
 * every form of ALTER TABLE fell into the {@code unknownStatement} wildcard catch-all, which
 * recorded the statement on {@code errors} as "unsupported statement skipped" and captured
 * nothing. The worst case was
 * {@code ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (customer_id) REFERENCES customers(id)}:
 * the orders → customers dependency edge that statement exists to declare was invisible on BOTH
 * sides, not merely one-sided.
 *
 * <p>The gap is now closed by the {@code alterTable} grammar rule (MySqlParser.g4) and
 * {@code MySqlAnalysisVisitor.visitAlterTable}, so every assertion below has been flipped from
 * "confirm it is lost" to "confirm it is captured".
 */
public class MySqlAlterTableAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    private static boolean hasOp(MySqlRoutineInfo r, MySqlTableOpInfo.DmlType dml, String table) {
        return r.getTableOps().stream()
                .anyMatch(t -> t.getDmlType() == dml && table.equalsIgnoreCase(t.getTableName()));
    }

    private static MySqlRoutineInfo run(String label, String alterSql) {
        String sql = """
                CREATE PROCEDURE proc_alter_%s()
                BEGIN
                  %s
                END
                """.formatted(label, alterSql);
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_alter_" + label, null);
        System.out.println("[" + label + "] sql: " + alterSql.trim());
        System.out.println("[" + label + "] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[" + label + "] foreignKeys: " + r.getForeignKeys());
        System.out.println("[" + label + "] errors: " + r.getErrors());
        return r;
    }

    /** Every case must parse as a real rule now — never as a skipped/unsupported statement. */
    private static void assertNotSkipped(MySqlRoutineInfo r) {
        assertTrue(r.getErrors().stream().noneMatch(e -> e.contains("unsupported statement skipped")),
                "ALTER TABLE must be parsed by the alterTable rule, not the catch-all: " + r.getErrors());
    }

    // ── Case 1: ADD COLUMN with and without the COLUMN keyword ─────────────────────────

    @Test
    @DisplayName("Case1a: ALTER TABLE orders ADD COLUMN priority INT — orders captured as an ALTER op")
    void addColumnWithKeywordCaptured() {
        MySqlRoutineInfo r = run("addcol_kw", "ALTER TABLE orders ADD COLUMN priority INT;");

        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"),
                "the altered table must be captured as an ALTER table op: " + tableNames(r));
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case1b: ALTER TABLE orders ADD priority INT (no COLUMN keyword) — orders captured")
    void addColumnWithoutKeywordCaptured() {
        MySqlRoutineInfo r = run("addcol_nokw", "ALTER TABLE orders ADD priority INT;");

        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"),
                "MySQL's COLUMN-keyword-optional ADD form must capture the table too: " + tableNames(r));
        assertNotSkipped(r);
    }

    // ── Case 2: THE KEY CASE — ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ───────────

    @Test
    @DisplayName("Case2: ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) "
            + "REFERENCES customers(id) — BOTH ends of the dependency edge are captured")
    void foreignKeyConstraintDependencyEdgeCaptured() {
        MySqlRoutineInfo r = run("fk",
                "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id);");

        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"),
                "altered table 'orders' must be captured: " + tableNames(r));
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.REFERENCE, "customers"),
                "referenced table 'customers' must be captured as a REFERENCE op, so plain "
                        + "table-op lineage sees the edge: " + tableNames(r));

        assertEquals(1, r.getForeignKeys().size(), "exactly one FK relationship expected");
        MySqlForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals("fk_customer", fk.getConstraintName());
        assertEquals("orders", fk.getTable());
        assertEquals("customers", fk.getReferencedTable());
        assertEquals(List.of("customer_id"), fk.getColumns());
        assertEquals(List.of("id"), fk.getReferencedColumns());
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case2b: multi-column FK, no CONSTRAINT keyword, with ON DELETE CASCADE")
    void unnamedMultiColumnForeignKeyWithReferenceOptionCaptured() {
        MySqlRoutineInfo r = run("fk_multi",
                "ALTER TABLE order_lines ADD FOREIGN KEY (order_id, tenant_id) "
                        + "REFERENCES orders(id, tenant_id) ON DELETE CASCADE ON UPDATE NO ACTION;");

        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "order_lines"));
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.REFERENCE, "orders"));

        assertEquals(1, r.getForeignKeys().size());
        MySqlForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertNull(fk.getConstraintName(), "no CONSTRAINT name was given");
        assertEquals("order_lines", fk.getTable());
        assertEquals("orders", fk.getReferencedTable());
        assertEquals(List.of("order_id", "tenant_id"), fk.getColumns());
        assertEquals(List.of("id", "tenant_id"), fk.getReferencedColumns());
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case2c: column-level inline REFERENCES (ADD COLUMN c INT REFERENCES t(c)) — same edge")
    void inlineColumnReferencesCaptured() {
        MySqlRoutineInfo r = run("fk_inline",
                "ALTER TABLE orders ADD COLUMN customer_id INT REFERENCES customers(id);");

        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.REFERENCE, "customers"));

        assertEquals(1, r.getForeignKeys().size());
        MySqlForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals("orders", fk.getTable());
        assertEquals("customers", fk.getReferencedTable());
        assertEquals(List.of("customer_id"), fk.getColumns());
        assertEquals(List.of("id"), fk.getReferencedColumns());
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case2d: schema-qualified FK — both schemas preserved")
    void schemaQualifiedForeignKeyCaptured() {
        MySqlRoutineInfo r = run("fk_schema",
                "ALTER TABLE sales.orders ADD CONSTRAINT fk_c FOREIGN KEY (cid) REFERENCES crm.customers(id);");

        assertEquals(1, r.getForeignKeys().size());
        MySqlForeignKeyInfo fk = r.getForeignKeys().get(0);
        assertEquals("orders", fk.getTable());
        assertEquals("sales", fk.getSchema());
        assertEquals("customers", fk.getReferencedTable());
        assertEquals("crm", fk.getReferencedSchema());
        assertNotSkipped(r);
    }

    // ── Case 3: other ADD CONSTRAINT forms — PRIMARY KEY / CHECK / UNIQUE ──────────────

    @Test
    @DisplayName("Case3a: ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (id) — orders captured")
    void addPrimaryKeyConstraintCaptured() {
        MySqlRoutineInfo r = run("pk", "ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (id);");
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertTrue(r.getForeignKeys().isEmpty(), "a PRIMARY KEY declares no cross-table edge");
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case3b: ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0) — orders captured")
    void addCheckConstraintCaptured() {
        MySqlRoutineInfo r = run("chk", "ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0);");
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertTrue(r.getForeignKeys().isEmpty());
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case3c: ALTER TABLE orders ADD CONSTRAINT uq_code UNIQUE (code) — orders captured")
    void addUniqueConstraintCaptured() {
        MySqlRoutineInfo r = run("uq", "ALTER TABLE orders ADD CONSTRAINT uq_code UNIQUE (code);");
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertTrue(r.getForeignKeys().isEmpty());
        assertNotSkipped(r);
    }

    // ── Case 4: DROP COLUMN / DROP CONSTRAINT / DROP FOREIGN KEY ───────────────────────

    @Test
    @DisplayName("Case4a: ALTER TABLE orders DROP COLUMN priority — orders captured")
    void dropColumnCaptured() {
        MySqlRoutineInfo r = run("dropcol", "ALTER TABLE orders DROP COLUMN priority;");
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case4b: ALTER TABLE orders DROP CONSTRAINT fk_customer — orders captured")
    void dropConstraintCaptured() {
        MySqlRoutineInfo r = run("dropcons", "ALTER TABLE orders DROP CONSTRAINT fk_customer;");
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case4c: ALTER TABLE orders DROP FOREIGN KEY fk_customer (MySQL form) — orders captured")
    void dropForeignKeyMySqlFormCaptured() {
        MySqlRoutineInfo r = run("dropfk", "ALTER TABLE orders DROP FOREIGN KEY fk_customer;");
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case4d: ALTER TABLE orders DROP PRIMARY KEY — 'primary' is not read as a column")
    void dropPrimaryKeyCaptured() {
        MySqlRoutineInfo r = run("droppk", "ALTER TABLE orders DROP PRIMARY KEY;");
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertNotSkipped(r);
    }

    // ── Case 5: multi-action, neighbour survival, unmodelled sub-forms, standalone DDL ──

    @Test
    @DisplayName("Case5a: comma-separated actions in one statement — every FK in the list is captured")
    void multipleActionsInOneStatementCaptured() {
        MySqlRoutineInfo r = run("multi",
                "ALTER TABLE orders ADD COLUMN priority INT, "
                        + "ADD CONSTRAINT fk_c FOREIGN KEY (cid) REFERENCES customers(id), "
                        + "ADD CONSTRAINT fk_s FOREIGN KEY (sid) REFERENCES suppliers(id), "
                        + "DROP COLUMN legacy_flag;");

        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertEquals(2, r.getForeignKeys().size(), "both FKs in the action list must be captured");
        assertEquals(List.of("customers", "suppliers"),
                r.getForeignKeys().stream().map(MySqlForeignKeyInfo::getReferencedTable).toList());
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case5b: an unmodelled sub-form (MODIFY / ENGINE=) still yields the table and "
            + "never fails its neighbouring statements")
    void unmodelledSubFormStillCapturesTableAndSpsNeighbours() {
        String sql = """
                CREATE PROCEDURE proc_alter_mixed()
                BEGIN
                  INSERT INTO audit_log(msg) VALUES ('before');
                  ALTER TABLE orders MODIFY COLUMN priority BIGINT NOT NULL;
                  ALTER TABLE orders ENGINE = InnoDB;
                  INSERT INTO audit_log(msg) VALUES ('after');
                END
                """;
        MySqlRoutineInfo r = new MySqlGrammarEngine().parse(sql, "proc_alter_mixed", null);
        System.out.println("[mixed] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[mixed] errors: " + r.getErrors());

        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"),
                "permissive tail forms must still contribute the altered table");
        assertEquals(2, r.getTableOps().stream()
                        .filter(t -> t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT
                                && "audit_log".equalsIgnoreCase(t.getTableName())).count(),
                "both neighbouring INSERTs must survive: " + tableNames(r));
        assertNotSkipped(r);
    }

    @Test
    @DisplayName("Case5c: standalone ALTER TABLE script (no enclosing routine) parses at the top level")
    void standaloneAlterTableScriptParses() {
        MySqlRoutineInfo r = new MySqlGrammarEngine().parse(
                "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) "
                        + "REFERENCES customers(id);",
                "alter_orders", null);
        System.out.println("[standalone] type=" + r.getType() + " name=" + r.getName()
                + " fks=" + r.getForeignKeys() + " errors=" + r.getErrors());

        assertEquals(MySqlRoutineInfo.RoutineType.ALTER_TABLE, r.getType());
        assertEquals("orders", r.getName());
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.ALTER, "orders"));
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.REFERENCE, "customers"));
        assertEquals(1, r.getForeignKeys().size());
        assertTrue(r.getErrors().isEmpty(), "standalone DDL must parse clean: " + r.getErrors());
    }

    // ── Case 6: no regression for identifiers that are now keywords ────────────────────

    @Test
    @DisplayName("Case6: columns/variables named `add`, `column`, `constraint`, `references`, "
            + "`foreign`, `alter` still parse (reservedAsId)")
    void newKeywordsStillUsableAsIdentifiers() {
        String sql = """
                CREATE PROCEDURE proc_kw_ids()
                BEGIN
                  DECLARE alter_v INT DEFAULT 0;
                  SELECT `add`, `column`, constraint_id, references_id, foreign_id
                    INTO alter_v, alter_v, alter_v, alter_v, alter_v
                    FROM legacy_keywords;
                  UPDATE legacy_keywords SET constraint_id = 1 WHERE references_id = 2;
                END
                """;
        MySqlRoutineInfo r = new MySqlGrammarEngine().parse(sql, "proc_kw_ids", null);
        System.out.println("[kwids] tableOps: " + tableNames(r) + " errors: " + r.getErrors());

        assertTrue(r.getErrors().isEmpty(),
                "newly-tokenized keywords must remain usable as identifiers: " + r.getErrors());
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.SELECT, "legacy_keywords"));
        assertTrue(hasOp(r, MySqlTableOpInfo.DmlType.UPDATE, "legacy_keywords"));
    }
}
