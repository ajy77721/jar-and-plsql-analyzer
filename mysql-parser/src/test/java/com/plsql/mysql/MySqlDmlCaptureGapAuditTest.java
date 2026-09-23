package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (2026-09-16) — DML table/column capture gap audit for mysql-parser.
 *
 * Does NOT touch mysql-parser/src/main/**. Exercises the real MySqlGrammarEngine +
 * MySqlAnalysisVisitor against realistic MySQL stored-procedure fixtures, one numbered
 * pattern per test, mirroring the plsql-parser DynamicSqlTableExtractionTest /
 * DynamicSqlConcatTest / *CodingConventionAuditTest evidence style: print actual captured
 * state, then assert on it (including asserting *documented gaps* where found).
 */
public class MySqlDmlCaptureGapAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    // ── Pattern 1: multi-table JOIN (plain SELECT + inside cursor) ──────────────────────

    @Test
    @DisplayName("P1: multi-table JOIN (INNER + LEFT, 4 tables) in a plain SELECT")
    void multiTableJoinPlainSelect() {
        String sql = """
                CREATE PROCEDURE proc_join()
                BEGIN
                  SELECT o.id, c.name, p.title, s.status
                  FROM orders o
                  INNER JOIN customers c ON o.customer_id = c.id
                  LEFT JOIN order_products p ON p.order_id = o.id
                  LEFT JOIN shipments s ON s.order_id = o.id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_join", null);
        System.out.println("[P1] tableOps: " + tableNames(r));
        System.out.println("[P1] joins on base op: " + r.getTableOps().get(0).getJoins().stream()
                .map(j -> j.getJoinType() + " " + j.getJoinedTable() + " ON(" + j.getLeftTable() + ")")
                .toList());
        System.out.println("[P1] errors: " + r.getErrors());

        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
        assertTrue(tableNames(r).stream().anyMatch(t -> "customers".equalsIgnoreCase(t)));
        assertTrue(tableNames(r).stream().anyMatch(t -> "order_products".equalsIgnoreCase(t)));
        assertTrue(tableNames(r).stream().anyMatch(t -> "shipments".equalsIgnoreCase(t)));
        assertTrue(r.getErrors().isEmpty(), "plain multi-table JOIN should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("P1b: multi-table JOIN inside a DECLARE ... CURSOR FOR inside a procedure")
    void multiTableJoinInsideCursor() {
        String sql = """
                CREATE PROCEDURE proc_join_cursor()
                BEGIN
                  DECLARE done INT DEFAULT 0;
                  DECLARE cur_orders CURSOR FOR
                    SELECT o.id, c.name, p.title
                    FROM orders o
                    INNER JOIN customers c ON o.customer_id = c.id
                    LEFT JOIN order_products p ON p.order_id = o.id;
                  OPEN cur_orders;
                  CLOSE cur_orders;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_join_cursor", null);
        System.out.println("[P1b] cursors: " + r.getCursors().stream().map(c -> c.getName()).toList());
        System.out.println("[P1b] tableOps: " + tableNames(r));
        System.out.println("[P1b] errors: " + r.getErrors());

        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)),
                "base table inside cursor SELECT must be captured");
        assertTrue(tableNames(r).stream().anyMatch(t -> "customers".equalsIgnoreCase(t)),
                "INNER JOINed table inside cursor SELECT must be captured");
        assertTrue(tableNames(r).stream().anyMatch(t -> "order_products".equalsIgnoreCase(t)),
                "LEFT JOINed table inside cursor SELECT must be captured");
        assertTrue(r.getErrors().isEmpty(), "cursor-declared JOIN select should not raise parse errors: " + r.getErrors());
    }

    // ── Pattern 2: subqueries (WHERE, SELECT-list scalar, derived table) ────────────────

    @Test
    @DisplayName("P2: WHERE-clause subquery, SELECT-list scalar subquery, and FROM derived table")
    void subqueriesEverywhere() {
        String sql = """
                CREATE PROCEDURE proc_subq()
                BEGIN
                  SELECT d.dept_name,
                         (SELECT COUNT(*) FROM employees e2 WHERE e2.dept_id = d.id) AS emp_count
                  FROM departments d
                  WHERE d.id IN (SELECT dept_id FROM active_assignments WHERE active = 1)
                    AND d.id IN (SELECT dept_id FROM (SELECT dept_id FROM budget_lines) AS bl);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_subq", null);
        System.out.println("[P2] tableOps: " + tableNames(r));
        System.out.println("[P2] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[P2] errors: " + r.getErrors());

        assertTrue(tableNames(r).stream().anyMatch(t -> "departments".equalsIgnoreCase(t)),
                "outer FROM table must be captured");
        assertTrue(tableNames(r).stream().anyMatch(t -> "employees".equalsIgnoreCase(t)),
                "table only inside SELECT-list scalar subquery must be captured");
        assertTrue(tableNames(r).stream().anyMatch(t -> "active_assignments".equalsIgnoreCase(t)),
                "table only inside WHERE-clause subquery must be captured");
        assertTrue(tableNames(r).stream().anyMatch(t -> "budget_lines".equalsIgnoreCase(t)),
                "table only inside a derived-table (FROM subquery) subquery must be captured");
        assertTrue(r.getErrors().isEmpty(), "subqueries should not raise parse errors: " + r.getErrors());
    }

    // ── Pattern 3: CTE (WITH clause), two CTEs, one referencing another ─────────────────

    @Test
    @DisplayName("P3: two CTEs (one referencing the other) — CTE name not captured as real table")
    void cteNamesNotCapturedAsRealTables() {
        String sql = """
                CREATE PROCEDURE proc_cte()
                BEGIN
                  WITH recent_orders AS (
                    SELECT * FROM orders WHERE created_at > '2026-01-01'
                  ),
                  recent_order_totals AS (
                    SELECT customer_id, SUM(amount) AS total FROM recent_orders GROUP BY customer_id
                  )
                  SELECT * FROM recent_order_totals;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cte", null);
        System.out.println("[P3] tableOps: " + tableNames(r));
        System.out.println("[P3] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[P3] errors: " + r.getErrors());

        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)),
                "real base table inside first CTE body must be captured");
        assertFalse(tableNames(r).stream().anyMatch(t -> "recent_orders".equalsIgnoreCase(t)),
                "first CTE name (referenced by second CTE) must NOT be captured as a real table");
        assertFalse(tableNames(r).stream().anyMatch(t -> "recent_order_totals".equalsIgnoreCase(t)),
                "second CTE name (referenced by main query) must NOT be captured as a real table");
        assertTrue(r.getErrors().isEmpty(), "MySQL 8 WITH clause should not raise parse errors: " + r.getErrors());
    }

    // ── Pattern 4: INSERT ... ON DUPLICATE KEY UPDATE ────────────────────────────────────

    @Test
    @DisplayName("P4: INSERT ... SELECT ... ON DUPLICATE KEY UPDATE — target write + source read")
    void insertOnDuplicateKeyUpdateWithSelectSource() {
        String sql = """
                CREATE PROCEDURE proc_upsert()
                BEGIN
                  INSERT INTO customer_totals (customer_id, total)
                  SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id
                  ON DUPLICATE KEY UPDATE total = VALUES(total);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_upsert", null);
        System.out.println("[P4] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[P4] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[P4] errors: " + r.getErrors());

        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT
                                && "customer_totals".equalsIgnoreCase(t.getTableName())),
                "INSERT target table must be captured as a write");
        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.SELECT
                                && "orders".equalsIgnoreCase(t.getTableName())),
                "SELECT source table (orders) must be captured as a read");
        assertTrue(r.getStatements().stream().anyMatch(s -> "UPSERT".equals(s.getType())),
                "ON DUPLICATE KEY UPDATE clause must be recorded as an UPSERT statement");
        assertTrue(r.getErrors().isEmpty(), "INSERT..SELECT..ON DUPLICATE KEY UPDATE should not raise parse errors: " + r.getErrors());
    }

    // ── Pattern 5: bulk multi-row INSERT ─────────────────────────────────────────────────

    @Test
    @DisplayName("P5: multi-row INSERT INTO t VALUES (...), (...), (...) — one insert op, not three")
    void bulkMultiRowInsertIsOneOperation() {
        String sql = """
                CREATE PROCEDURE proc_bulk_insert()
                BEGIN
                  INSERT INTO audit_log (event, ts) VALUES
                    ('a', 1),
                    ('b', 2),
                    ('c', 3);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_bulk_insert", null);
        List<MySqlTableOpInfo> auditLogOps = r.getTableOps().stream()
                .filter(t -> "audit_log".equalsIgnoreCase(t.getTableName()))
                .toList();
        System.out.println("[P5] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[P5] audit_log op count = " + auditLogOps.size());
        System.out.println("[P5] errors: " + r.getErrors());

        assertEquals(1, auditLogOps.size(),
                "a single multi-row VALUES (...),(...),(...) INSERT statement is one INSERT op "
                        + "against the grammar's model (INSERT visited once via visitInsertStatement, "
                        + "insertRowList is not separately visited/counted) — 3 rows must NOT produce 3 ops");
    }

    // ── Pattern 6: dynamic SQL via CONCAT()-built table name ────────────────────────────

    @Test
    @DisplayName("P6: dynamic SQL via PREPARE/EXECUTE and CONCAT()-built table name — no extraction (gap)")
    void dynamicSqlConcatTableNameNotExtracted() {
        String sql = """
                CREATE PROCEDURE proc_dynamic(IN p_suffix VARCHAR(20))
                BEGIN
                  SET @tbl = 'orders';
                  SET @sql = CONCAT('SELECT * FROM ', @tbl, '_', p_suffix);
                  PREPARE stmt1 FROM @sql;
                  EXECUTE stmt1;
                  DEALLOCATE PREPARE stmt1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_dynamic", null);
        System.out.println("[P6] dynamicSql: " + r.getDynamicSql().stream()
                .map(d -> d.getKind() + " " + d.getStmtName() + " src=" + d.getSqlSource()).toList());
        System.out.println("[P6] tableOps: " + tableNames(r));
        System.out.println("[P6] errors: " + r.getErrors());

        // GAP: MySqlAnalysisVisitor.visitPrepareStatement (visitor L404-413) only records the
        // PREPARE statement's raw @var name as sqlSource ("@sql" here) - it never resolves the
        // variable's assigned CONCAT(...) expression, so no table name is ever extracted from
        // dynamic SQL in mysql-parser, unlike plsql-parser's EXECUTE IMMEDIATE literal-fragment
        // extraction (see DynamicSqlConcatTest.testConcatSelectFrom). This is a structural gap,
        // not a bug in a specific rule - there is no equivalent mechanism at all.
        assertTrue(r.getDynamicSql().stream().anyMatch(d -> "stmt1".equalsIgnoreCase(d.getStmtName())),
                "PREPARE/EXECUTE/DEALLOCATE lifecycle itself is captured");
        assertFalse(tableNames(r).stream().anyMatch(t -> t != null && t.toLowerCase().contains("orders")),
                "documents the gap: no table name is extracted from the dynamically-built SQL "
                        + "string at all (mysql-parser has no CONCAT-aware or literal-fragment dynamic "
                        + "SQL table extraction, unlike plsql-parser's DynamicSqlConcatTest)");
    }

    @Test
    @DisplayName("P6b: PREPARE ... FROM a literal string containing a real table name — also not extracted")
    void dynamicSqlLiteralTableNameAlsoNotExtracted() {
        String sql = """
                CREATE PROCEDURE proc_dynamic_literal()
                BEGIN
                  PREPARE stmt2 FROM 'SELECT * FROM warehouse_inventory';
                  EXECUTE stmt2;
                  DEALLOCATE PREPARE stmt2;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_dynamic_literal", null);
        System.out.println("[P6b] dynamicSql sqlSource: " + r.getDynamicSql().stream()
                .map(d -> d.getSqlSource()).toList());
        System.out.println("[P6b] tableOps: " + tableNames(r));
        System.out.println("[P6b] errors: " + r.getErrors());

        // Bug 9 fix (§DML-audit, literal-only scope): a fully-literal PREPARE ... FROM string
        // (no @variable/CONCAT indirection) must yield a DYNAMIC table op.
        assertTrue(tableNames(r).stream().anyMatch(t -> "warehouse_inventory".equalsIgnoreCase(t)),
                "fully-literal PREPARE ... FROM '<sql with a real table>' must yield a table op");
    }

    // ── Pattern 7: self-join ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("P7: self-join FROM orders o1 JOIN orders o2 — single \"orders\" table captured")
    void selfJoinCapturesSingleTableTwice() {
        String sql = """
                CREATE PROCEDURE proc_self_join()
                BEGIN
                  SELECT o1.id, o2.id
                  FROM orders o1
                  JOIN orders o2 ON o1.parent_order_id = o2.id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_self_join", null);
        System.out.println("[P7] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + " alias=" + t.getAlias()).toList());
        System.out.println("[P7] errors: " + r.getErrors());

        long ordersOpCount = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName())).count();
        System.out.println("[P7] 'orders' table-op count (structural model: one op per atom, so a "
                + "self-join produces TWO ops both named 'orders', distinguished only by alias/line) = "
                + ordersOpCount);
        assertEquals(2, ordersOpCount,
                "self-join produces two table ops both named 'orders' (one per tableAtom: base o1 and "
                        + "joined o2) - the grammar/visitor model does not dedupe same-name-different-alias "
                        + "atoms into a single logical table reference; this is the structural model, not a bug");
        assertTrue(r.getTableOps().stream().anyMatch(t -> "o1".equalsIgnoreCase(t.getAlias())));
        assertTrue(r.getTableOps().stream().anyMatch(t -> "o2".equalsIgnoreCase(t.getAlias())));
    }

    // ── Pattern 8: plain SELECT from a bare name that could be a view ───────────────────

    @Test
    @DisplayName("P8: plain SELECT FROM a bare name (could be a view) — treated identically to a table")
    void bareNameTreatedIdenticallyToTable() {
        String sql = """
                CREATE PROCEDURE proc_view_or_table()
                BEGIN
                  SELECT * FROM active_customers_view;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_view_or_table", null);
        System.out.println("[P8] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[P8] errors: " + r.getErrors());

        assertTrue(r.getTableOps().stream().anyMatch(t ->
                        t.getDmlType() == MySqlTableOpInfo.DmlType.SELECT
                                && "active_customers_view".equalsIgnoreCase(t.getTableName())),
                "a bare name is captured as a SELECT table op with no table/view distinction - "
                        + "the grammar has no notion of INFORMATION_SCHEMA-backed view resolution, so a "
                        + "view name and a table name are structurally indistinguishable here (limitation, "
                        + "not a bug)");
    }
}
