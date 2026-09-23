package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-16) of postgres-parser's DML table/column capture across 8
 * realistic PL/pgSQL patterns (multi-join, subqueries, CTE, ON CONFLICT upsert, bulk
 * INSERT...SELECT...RETURNING, dynamic SQL, self-join, view-as-table). Prints real captured
 * output for each pattern; asserts only where there is a clear expectation. Does not modify
 * any file under src/main.
 */
class PostgresDmlCaptureGapAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private void dump(String label, PostgresRoutineInfo r) {
        System.out.println("========== " + label + " ==========");
        for (PostgresTableOpInfo t : r.getTableOps()) {
            System.out.println("  tableOp: " + t.getDmlType() + " table=" + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
            for (PostgresJoinInfo j : t.getJoins()) {
                System.out.println("    join: " + j.getJoinType() + " " + j.getJoinedTable()
                        + " left=" + j.getLeftTable() + " cond=" + j.getCondition());
            }
        }
        if (r.getDynamicSql() != null) {
            for (PostgresDynamicSqlInfo d : r.getDynamicSql()) {
                System.out.println("  dynSql: kind=" + d.getKind() + " line=" + d.getLine()
                        + " source=" + d.getSqlSource());
            }
        }
        System.out.println("  errors=" + r.getErrors());
    }

    // ── 1. Multi-table JOIN (plain SELECT + inside a cursor declaration) ──────────────────
    @Test
    @DisplayName("1: multi-table JOIN in plain SELECT and inside a cursor declaration")
    void multiTableJoin() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_join_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id, c.name, p.sku, s.status\n" +
                "    FROM orders o\n" +
                "    JOIN customers c ON o.customer_id = c.id\n" +
                "    LEFT JOIN products p ON o.product_id = p.id\n" +
                "    LEFT JOIN shipments s ON o.id = s.order_id;\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT count(*) INTO v_cnt\n" +
                "    FROM orders o2\n" +
                "    JOIN customers c2 ON o2.customer_id = c2.id\n" +
                "    LEFT JOIN products p2 ON o2.product_id = p2.id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_join_probe", null);
        dump("multiTableJoin", r);
        r.getCursors().forEach(c -> System.out.println("  cursor=" + c.getName() + " selectText=" + c.getSelectText()));
        long ordersCount = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName())).count();
        long customersCount = r.getTableOps().stream()
                .filter(t -> "customers".equalsIgnoreCase(t.getTableName())).count();
        System.out.println("  >>> orders table-op occurrences: " + ordersCount + " (expect 2: cursor SELECT + plain SELECT)");
        System.out.println("  >>> customers table-op occurrences: " + customersCount + " (expect 2)");
        boolean hasShipments = r.getTableOps().stream()
                .anyMatch(t -> "shipments".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> shipments captured (cursor SELECT, 4th join): " + hasShipments);

        // Bug 5 fix (§DML-audit): the cursor's joined SELECT must be descended into for table
        // capture, not silently discarded because its dataType text is "CURSOR".
        org.junit.jupiter.api.Assertions.assertEquals(2, ordersCount,
                "orders must be captured from both the cursor SELECT and the plain SELECT");
        org.junit.jupiter.api.Assertions.assertEquals(2, customersCount,
                "customers must be captured from both the cursor SELECT and the plain SELECT");
        org.junit.jupiter.api.Assertions.assertTrue(hasShipments,
                "shipments (only referenced inside the cursor's 4th join) must be captured");
    }

    // ── 2. Subqueries: WHERE, SELECT-list scalar, derived table (FROM) ────────────────────
    @Test
    @DisplayName("2: subqueries only inside WHERE / SELECT-list / FROM are captured")
    void subqueries() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_subquery_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT d.name,\n" +
                "         (SELECT max(salary) FROM employee_salaries WHERE dept_id = d.id) AS max_sal\n" +
                "    INTO v_cnt\n" +
                "    FROM departments d\n" +
                "   WHERE d.id IN (SELECT dept_id FROM active_departments)\n" +
                "     AND EXISTS (SELECT 1 FROM (SELECT dept_id FROM dept_review) rv WHERE rv.dept_id = d.id);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_subquery_probe", null);
        dump("subqueries", r);
        for (String tbl : new String[]{"departments", "employee_salaries", "active_departments", "dept_review"}) {
            boolean found = r.getTableOps().stream().anyMatch(t -> tbl.equalsIgnoreCase(t.getTableName()));
            System.out.println("  >>> " + tbl + " captured: " + found);
        }
    }

    // ── 3. CTE: two CTEs (one referencing another), main query selects from CTE name ─────
    @Test
    @DisplayName("3: CTE name not captured as real table; base tables inside CTE bodies are")
    void ctes() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_cte_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  WITH recent_orders AS (\n" +
                "    SELECT * FROM orders WHERE created_at > now() - interval '7 days'\n" +
                "  ), recent_customers AS (\n" +
                "    SELECT c.* FROM customers c JOIN recent_orders ro ON ro.customer_id = c.id\n" +
                "  )\n" +
                "  SELECT count(*) INTO v_cnt FROM recent_customers;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_cte_probe", null);
        dump("ctes", r);
        boolean recentOrdersAsTable = r.getTableOps().stream()
                .anyMatch(t -> "recent_orders".equalsIgnoreCase(t.getTableName()));
        boolean recentCustomersAsTable = r.getTableOps().stream()
                .anyMatch(t -> "recent_customers".equalsIgnoreCase(t.getTableName()));
        boolean ordersReal = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean customersReal = r.getTableOps().stream()
                .anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> CTE 'recent_orders' misclassified as real table: " + recentOrdersAsTable + " (expect false)");
        System.out.println("  >>> CTE 'recent_customers' misclassified as real table: " + recentCustomersAsTable + " (expect false)");
        System.out.println("  >>> base table 'orders' inside CTE captured: " + ordersReal + " (expect true)");
        System.out.println("  >>> base table 'customers' inside CTE captured: " + customersReal + " (expect true)");
        org.junit.jupiter.api.Assertions.assertFalse(recentOrdersAsTable);
        org.junit.jupiter.api.Assertions.assertFalse(recentCustomersAsTable);
        org.junit.jupiter.api.Assertions.assertTrue(ordersReal);
        org.junit.jupiter.api.Assertions.assertTrue(customersReal);
    }

    // ── 3b. Bug 6 (§DML-audit): a *second* CTE in the chain whose own body is a JOIN against
    // the *first* CTE (as the JOIN's base) and a real table (as the JOIN's partner) — the real
    // table on the join side of the second CTE must still be captured. ────────────────────────
    @Test
    @DisplayName("3b: second CTE's own join against a real table is captured")
    void cteChainSecondJoinCaptured() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_cte_chain_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_cnt INT;\n" +
                "BEGIN\n" +
                "  WITH a AS (\n" +
                "    SELECT o.id FROM orders o\n" +
                "  ), b AS (\n" +
                "    SELECT a.id, c.name FROM a JOIN customers c ON a.id = c.id\n" +
                "  )\n" +
                "  SELECT count(*) INTO v_cnt FROM b;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_cte_chain_probe", null);
        dump("cteChainSecondJoinCaptured", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean customersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured, "first CTE's table (orders) must be captured");
        org.junit.jupiter.api.Assertions.assertTrue(customersCaptured,
                "second CTE's own joined table (customers) must be captured");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── 4. INSERT ... ON CONFLICT ... DO UPDATE SET, source is a SELECT from another table ──
    @Test
    @DisplayName("4: ON CONFLICT DO UPDATE — target write, INSERT...SELECT source read")
    void onConflictUpsert() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_upsert_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT INTO customer_summary (customer_id, total)\n" +
                "    SELECT customer_id, sum(amount) FROM orders GROUP BY customer_id\n" +
                "  ON CONFLICT (customer_id) DO UPDATE SET total = EXCLUDED.total;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_upsert_probe", null);
        dump("onConflictUpsert", r);
        boolean targetWrite = r.getTableOps().stream()
                .anyMatch(t -> "customer_summary".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.INSERT);
        boolean sourceRead = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean upsertStatementRecorded = r.getStatements().stream()
                .anyMatch(s -> "UPSERT".equals(s.getType()));
        System.out.println("  >>> customer_summary captured as INSERT (write): " + targetWrite + " (expect true)");
        System.out.println("  >>> orders captured as SELECT (read source): " + sourceRead + " (expect true)");
        System.out.println("  >>> UPSERT statement record present: " + upsertStatementRecorded + " (expect true)");

        // Bug 7 fix (§DML-audit): no phantom table named SET, and UPSERT record present.
        org.junit.jupiter.api.Assertions.assertTrue(targetWrite);
        org.junit.jupiter.api.Assertions.assertTrue(sourceRead);
        org.junit.jupiter.api.Assertions.assertTrue(upsertStatementRecorded, "UPSERT statement record must be present");
        boolean phantomSet = r.getTableOps().stream().anyMatch(t -> "SET".equalsIgnoreCase(t.getTableName()));
        org.junit.jupiter.api.Assertions.assertFalse(phantomSet,
                "ON CONFLICT DO UPDATE SET must not fabricate a phantom table named SET");
        org.junit.jupiter.api.Assertions.assertEquals(2, r.getTableOps().size(),
                "expected exactly 2 table ops (INSERT target + SELECT source), got: "
                        + r.getTableOps().stream().map(PostgresTableOpInfo::getTableName).toList());
    }

    // ── 5. Bulk INSERT INTO t (...) SELECT ... FROM source RETURNING ... ──────────────────
    @Test
    @DisplayName("5: INSERT...SELECT...RETURNING — one statement, target+source, no RETURNING miscount")
    void bulkInsertSelectReturning() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_bulk_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT INTO archived_orders (id, customer_id, amount)\n" +
                "    SELECT id, customer_id, amount FROM orders WHERE status = 'CLOSED'\n" +
                "  RETURNING id, customer_id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_bulk_probe", null);
        dump("bulkInsertSelectReturning", r);
        long insertOps = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == PostgresTableOpInfo.DmlType.INSERT
                        && "archived_orders".equalsIgnoreCase(t.getTableName())).count();
        long selectOps = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == PostgresTableOpInfo.DmlType.SELECT
                        && "orders".equalsIgnoreCase(t.getTableName())).count();
        System.out.println("  >>> archived_orders INSERT ops: " + insertOps + " (expect 1)");
        System.out.println("  >>> orders SELECT ops (source): " + selectOps + " (expect 1)");
        PostgresTableOpInfo insertOp = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == PostgresTableOpInfo.DmlType.INSERT).findFirst().orElse(null);
        System.out.println("  >>> RETURNING columns on INSERT op: "
                + (insertOp == null ? "N/A" : insertOp.getReturningColumns()) + " (expect [id, customer_id])");
        System.out.println("  >>> total table-op count: " + r.getTableOps().size() + " (expect 2, no RETURNING miscount)");

        // Bug 8 fix (§DML-audit): RETURNING must survive on INSERT...SELECT...RETURNING.
        org.junit.jupiter.api.Assertions.assertEquals(1, insertOps);
        org.junit.jupiter.api.Assertions.assertEquals(1, selectOps);
        org.junit.jupiter.api.Assertions.assertNotNull(insertOp);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("id", "customer_id"),
                insertOp.getReturningColumns(), "RETURNING columns must be populated on INSERT...SELECT...RETURNING");
        org.junit.jupiter.api.Assertions.assertEquals(2, r.getTableOps().size());
    }

    // ── 6. Dynamic SQL: EXECUTE format(...) / EXECUTE '...' || var — table extraction gap ─
    @Test
    @DisplayName("6: dynamic SQL via EXECUTE format()/concat — does postgres-parser extract table names?")
    void dynamicSqlTableExtractionGap() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_dynsql_probe(p_table_name TEXT, p_prefix TEXT)\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_result INT;\n" +
                "BEGIN\n" +
                "  EXECUTE format('SELECT * FROM %I', p_table_name);\n" +
                "  EXECUTE 'SELECT count(*) FROM ' || p_prefix || '_orders' INTO v_result;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_dynsql_probe", null);
        dump("dynamicSqlTableExtractionGap", r);
        System.out.println("  >>> dynamicSql entries captured (raw source, no table parsing): "
                + (r.getDynamicSql() == null ? 0 : r.getDynamicSql().size()) + " (expect 2)");
        // PostgresTableOpInfo.DmlType has no DYNAMIC value at all (see
        // postgres-parser/src/main/java/com/plsql/postgres/model/PostgresTableOpInfo.java:8),
        // and visitExecuteStatement/visitExecuteImmStmt never parse the string/format() contents
        // for embedded table names (unlike plsql-parser's EXECUTE IMMEDIATE handling — see
        // DynamicSqlTableExtractionTest/DynamicSqlConcatTest). So no table op should appear here.
        boolean anyDynamicTableOp = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        || (t.getTableName() != null && t.getTableName().toLowerCase().contains("table")));
        System.out.println("  >>> any table op derived from dynamic SQL text: " + anyDynamicTableOp
                + " (plsql-parser equivalent extracts these; postgres-parser does not)");
    }

    // ── 6b. Bug 9 fix (§DML-audit, lower priority, literal-only scope): a fully-literal
    // EXECUTE '<sql>' (no ||/format() indirection) must still extract the table name. ──────
    @Test
    @DisplayName("6b: EXECUTE '<literal sql>' with no concatenation extracts the table name")
    void dynamicSqlLiteralExtracted() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_dynsql_literal_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  EXECUTE 'DELETE FROM temp_staging WHERE created < 30';\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_dynsql_literal_probe", null);
        dump("dynamicSqlLiteralExtracted", r);
        boolean tempStagingCaptured = r.getTableOps().stream()
                .anyMatch(t -> "temp_staging".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.DYNAMIC);
        org.junit.jupiter.api.Assertions.assertTrue(tempStagingCaptured,
                "fully-literal EXECUTE '...' dynamic SQL must yield a DYNAMIC table op");
    }

    // ── 7. Self-join: FROM orders o1 JOIN orders o2 ── single "orders" table captured ─────
    @Test
    @DisplayName("7: self-join FROM orders o1 JOIN orders o2 — single base table name")
    void selfJoin() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_selfjoin_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT count(*) INTO v_cnt\n" +
                "    FROM orders o1\n" +
                "    JOIN orders o2 ON o1.parent_order_id = o2.id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_selfjoin_probe", null);
        dump("selfJoin", r);
        long ordersOps = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName())).count();
        System.out.println("  >>> distinct table-op records naming 'orders': " + ordersOps
                + " (expect 2: base o1 + joined o2, both resolving to table name 'orders')");
        r.getTableOps().forEach(t -> System.out.println("      alias=" + t.getAlias() + " table=" + t.getTableName()));
    }

    // ── 8. Plain SELECT from a bare name that could be a view ─────────────────────────────
    @Test
    @DisplayName("8: bare-name SELECT (could be a view) treated identically to a table")
    void bareNameCouldBeView() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_view_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT count(*) INTO v_cnt FROM active_customer_view;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_view_probe", null);
        dump("bareNameCouldBeView", r);
        boolean captured = r.getTableOps().stream()
                .anyMatch(t -> "active_customer_view".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.SELECT);
        System.out.println("  >>> active_customer_view captured as SELECT table op (no table/view distinction exists): "
                + captured + " (expect true — structural limitation, not a bug)");
    }
}
