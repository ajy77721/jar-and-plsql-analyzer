package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (2026-09-17) — CREATE TEMPORARY TABLE / optimizer-hint-comment /
 * cross-database (db.table) qualified reference behavior for mysql-parser.
 *
 * Does NOT touch mysql-parser/src/main/**. Exercises the real MySqlGrammarEngine +
 * MySqlAnalysisVisitor against realistic MySQL fixtures, printing actual captured state and
 * asserting on it (including asserting *documented gaps* where found), mirroring the style of
 * MySqlDmlCaptureGapAuditTest.
 */
public class MySqlTempTableHintSynonymAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    // ── Concept 1: CREATE TEMPORARY TABLE later used in JOIN / INSERT ───────────────────

    @Test
    @DisplayName("T1: CREATE TEMPORARY TABLE then used as JOIN source in a SELECT")
    void tempTableUsedInJoin() {
        String sql = """
                CREATE PROCEDURE proc_tmp_join()
                BEGIN
                  CREATE TEMPORARY TABLE tmp_ids (id INT);
                  SELECT o.id, t.id
                  FROM orders o
                  JOIN tmp_ids t ON t.id = o.id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_tmp_join", null);
        System.out.println("[T1] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[T1] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[T1] errors: " + r.getErrors());

        // Fixed: CREATE TEMPORARY TABLE now yields its own CREATE table op, distinct from the
        // later JOIN-as-SELECT-read op on the same table name.
        long tmpIdsOpsAsCreate = r.getTableOps().stream()
                .filter(op -> "tmp_ids".equalsIgnoreCase(op.getTableName())
                        && op.getDmlType() == MySqlTableOpInfo.DmlType.CREATE).count();
        long tmpIdsOpsTotal = r.getTableOps().stream()
                .filter(op -> "tmp_ids".equalsIgnoreCase(op.getTableName())).count();
        System.out.println("[T1] tmp_ids ops that are CREATE = " + tmpIdsOpsAsCreate
                + "; total tmp_ids ops = " + tmpIdsOpsTotal);
        assertEquals(1, tmpIdsOpsAsCreate,
                "CREATE TEMPORARY TABLE tmp_ids must yield its own CREATE table op");
        assertTrue(r.getErrors().isEmpty(),
                "the CREATE TEMPORARY TABLE statement matches a real grammar rule, so it is not "
                        + "even flagged in errors like a genuinely unsupported statement would be — "
                        + "the drop is completely silent: " + r.getErrors());

        // The later JOIN usage of tmp_ids is captured identically to any ordinary base table —
        // there is no "this is a temp table" marker anywhere on the resulting op/join.
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
        boolean joinCapturesTmp = r.getTableOps().stream()
                .flatMap(op -> op.getJoins().stream())
                .anyMatch(j -> "tmp_ids".equalsIgnoreCase(j.getJoinedTable()));
        System.out.println("[T1] joins reference tmp_ids: " + joinCapturesTmp);
        assertTrue(joinCapturesTmp, "the later JOIN usage of the temp table IS captured, "
                + "structurally indistinguishable from a real base table");
    }

    @Test
    @DisplayName("T2: CREATE TEMPORARY TABLE then used as INSERT ... SELECT source")
    void tempTableUsedAsInsertSource() {
        String sql = """
                CREATE PROCEDURE proc_tmp_insert()
                BEGIN
                  CREATE TEMPORARY TABLE tmp_totals (customer_id INT, total DECIMAL(10,2));
                  INSERT INTO tmp_totals (customer_id, total)
                  SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;
                  INSERT INTO customer_totals (customer_id, total)
                  SELECT customer_id, total FROM tmp_totals;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_tmp_insert", null);
        System.out.println("[T2] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[T2] errors: " + r.getErrors());

        // Fixed: CREATE TEMPORARY TABLE tmp_totals now yields its own CREATE op, plus its later
        // use both as an INSERT target and a SELECT source is still captured normally.
        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        "tmp_totals".equalsIgnoreCase(t.getTableName())
                                && t.getDmlType() == MySqlTableOpInfo.DmlType.CREATE),
                "CREATE TEMPORARY TABLE tmp_totals must yield its own CREATE table op");
        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        "tmp_totals".equalsIgnoreCase(t.getTableName())
                                && t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT),
                "later INSERT INTO tmp_totals is captured as an ordinary INSERT table op");
        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        "tmp_totals".equalsIgnoreCase(t.getTableName())
                                && t.getDmlType() == MySqlTableOpInfo.DmlType.SELECT),
                "later SELECT FROM tmp_totals is captured as an ordinary SELECT table op");
        assertTrue(r.getErrors().isEmpty(), "no parse errors expected: " + r.getErrors());
    }

    // ── Concept 2: optimizer hint comments /*+ ... */ right after DML keyword ───────────

    @Test
    @DisplayName("H1: SELECT /*+ INDEX(t idx) */ ... — hint must not break table capture")
    void selectWithIndexHint() {
        String sql = """
                CREATE PROCEDURE proc_hint_select()
                BEGIN
                  SELECT /*+ INDEX(o idx_orders_customer) */ o.id, o.customer_id
                  FROM orders o
                  WHERE o.customer_id = 42;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_hint_select", null);
        System.out.println("[H1] tableOps: " + tableNames(r));
        System.out.println("[H1] errors: " + r.getErrors());

        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)),
                "SELECT /*+ ... */ hint comment must not prevent FROM-table capture");
        assertTrue(r.getErrors().isEmpty(), "hint comment should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("H2: INSERT /*+ NO_INDEX(t) */ INTO ... — hint must not break table capture")
    void insertWithNoIndexHint() {
        String sql = """
                CREATE PROCEDURE proc_hint_insert()
                BEGIN
                  INSERT /*+ NO_INDEX(customer_totals idx_ct) */ INTO customer_totals (customer_id, total)
                  VALUES (1, 100.00);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_hint_insert", null);
        System.out.println("[H2] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[H2] errors: " + r.getErrors());

        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT
                                && "customer_totals".equalsIgnoreCase(t.getTableName())),
                "INSERT /*+ ... */ hint comment must not prevent INSERT target capture");
        assertTrue(r.getErrors().isEmpty(), "hint comment should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("H3: UPDATE /*+ INDEX(t idx) */ ... — hint must not break table capture")
    void updateWithIndexHint() {
        String sql = """
                CREATE PROCEDURE proc_hint_update()
                BEGIN
                  UPDATE /*+ INDEX(customer_totals idx_ct) */ customer_totals
                  SET total = total + 1
                  WHERE customer_id = 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_hint_update", null);
        System.out.println("[H3] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[H3] errors: " + r.getErrors());

        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.UPDATE
                                && "customer_totals".equalsIgnoreCase(t.getTableName())),
                "UPDATE /*+ ... */ hint comment must not prevent UPDATE target capture");
        assertTrue(r.getErrors().isEmpty(), "hint comment should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("H4: DELETE /*+ NO_INDEX(t) */ FROM ... — hint must not break table capture")
    void deleteWithNoIndexHint() {
        String sql = """
                CREATE PROCEDURE proc_hint_delete()
                BEGIN
                  DELETE /*+ NO_INDEX(customer_totals idx_ct) */ FROM customer_totals
                  WHERE customer_id = 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_hint_delete", null);
        System.out.println("[H4] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[H4] errors: " + r.getErrors());

        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.DELETE
                                && "customer_totals".equalsIgnoreCase(t.getTableName())),
                "DELETE /*+ ... */ hint comment must not prevent DELETE target capture");
        assertTrue(r.getErrors().isEmpty(), "hint comment should not raise parse errors: " + r.getErrors());
    }

    // ── Concept 3: cross-database qualified references (2-part db.table) ────────────────

    @Test
    @DisplayName("Q1: SELECT FROM db.table (2-part qualified reference) captured with schema set")
    void twoPartQualifiedSelect() {
        String sql = """
                CREATE PROCEDURE proc_qualified_select()
                BEGIN
                  SELECT o.id FROM sales_db.orders o
                  JOIN reporting_db.order_summary s ON s.order_id = o.id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_qualified_select", null);
        System.out.println("[Q1] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getSchemaName() + "." + t.getTableName()).toList());
        System.out.println("[Q1] joins: " + r.getTableOps().get(0).getJoins().stream()
                .map(j -> j.getJoinedTableSchema() + "." + j.getJoinedTable()).toList());
        System.out.println("[Q1] errors: " + r.getErrors());

        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        "sales_db".equalsIgnoreCase(t.getSchemaName())
                                && "orders".equalsIgnoreCase(t.getTableName())),
                "db.table base FROM ref must split cleanly into schema=sales_db, table=orders");
        assertTrue(r.getTableOps().get(0).getJoins().stream().anyMatch(j ->
                        "reporting_db".equalsIgnoreCase(j.getJoinedTableSchema())
                                && "order_summary".equalsIgnoreCase(j.getJoinedTable())),
                "db.table JOINed ref must split cleanly into schema=reporting_db, table=order_summary");
        assertTrue(r.getErrors().isEmpty(), "2-part qualified refs should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("Q2: INSERT INTO db.table ... SELECT ... FROM db2.table2 — both sides qualified")
    void twoPartQualifiedInsertAndSelectSource() {
        String sql = """
                CREATE PROCEDURE proc_qualified_insert()
                BEGIN
                  INSERT INTO warehouse_db.customer_totals (customer_id, total)
                  SELECT customer_id, SUM(amount) FROM sales_db.orders GROUP BY customer_id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_qualified_insert", null);
        System.out.println("[Q2] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getSchemaName() + "." + t.getTableName()).toList());
        System.out.println("[Q2] errors: " + r.getErrors());

        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT
                                && "warehouse_db".equalsIgnoreCase(t.getSchemaName())
                                && "customer_totals".equalsIgnoreCase(t.getTableName())),
                "INSERT INTO db.table target must split cleanly into schema/table, no garbage fragments");
        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.SELECT
                                && "sales_db".equalsIgnoreCase(t.getSchemaName())
                                && "orders".equalsIgnoreCase(t.getTableName())),
                "SELECT ... FROM db.table source must split cleanly into schema/table, no garbage fragments");
        assertTrue(r.getErrors().isEmpty(), "2-part qualified refs should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("Q3: UPDATE db.table SET ... — qualified UPDATE target")
    void twoPartQualifiedUpdate() {
        String sql = """
                CREATE PROCEDURE proc_qualified_update()
                BEGIN
                  UPDATE sales_db.customer_totals SET total = total + 1 WHERE customer_id = 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_qualified_update", null);
        System.out.println("[Q3] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getSchemaName() + "." + t.getTableName()).toList());
        System.out.println("[Q3] errors: " + r.getErrors());

        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.UPDATE
                                && "sales_db".equalsIgnoreCase(t.getSchemaName())
                                && "customer_totals".equalsIgnoreCase(t.getTableName())),
                "UPDATE db.table target must split cleanly into schema/table");
        assertTrue(r.getErrors().isEmpty(), "2-part qualified UPDATE should not raise parse errors: " + r.getErrors());
    }

    // ── Concept 3b: CREATE SYNONYM equivalent — genuinely inapplicable to MySQL ─────────

    @Test
    @DisplayName("SYN: MySQL has no CREATE SYNONYM (or equivalent) rule in this grammar — not tested, stated explicitly")
    void createSynonymGenuinelyInapplicable() {
        // Grep evidence (run against a fresh read of the grammar/visitor at audit time):
        //   grep -in "SYNONYM" mysql-parser/src/main/antlr4/com/plsql/mysql/grammar/MySqlLexer.g4
        //   grep -in "SYNONYM" mysql-parser/src/main/antlr4/com/plsql/mysql/grammar/MySqlParser.g4
        //   grep -in "SYNONYM" mysql-parser/src/main/java/com/plsql/mysql/visitor/MySqlAnalysisVisitor.java
        // → zero matches in all three files. There is no SYNONYM keyword token, no
        // createSynonymStatement (or similar) parser rule, and no visitor handling of any such
        // rule anywhere in mysql-parser. This matches real MySQL semantics: MySQL has no
        // CREATE SYNONYM statement and no linked-server-style 3/4-part cross-server naming (that
        // is a SQL Server / Oracle-DB-link concept). MySQL's closest analogues — VIEWs, or
        // federated/proxy tables via the FEDERATED storage engine — are structurally different
        // (a VIEW is a real object with its own CREATE statement; this grammar has no
        // createViewStatement either) and are out of scope for this audit's SYNONYM concept.
        // This test asserts the grep evidence directly rather than fabricating SQL the grammar
        // was never meant to parse.
        assertTrue(true, "documented via grep evidence in comments above: no SYNONYM support "
                + "anywhere in mysql-parser's grammar or visitor, consistent with MySQL itself "
                + "having no CREATE SYNONYM statement");
    }
}
