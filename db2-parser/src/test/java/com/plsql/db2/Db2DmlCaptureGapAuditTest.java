package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY (read-only investigation, 2026-09-16) — printed evidence for 8 DML/table-capture
 * patterns against {@code Db2AnalysisEngine}, mirroring the existing pattern in
 * {@code Db2CodingConventionAuditTest} / plsql-parser's {@code VarVsCallVerificationTest} and
 * {@code DynamicSqlTableExtractionTest}. Each routine below is a single, self-contained
 * CREATE PROCEDURE body (the grammar's {@code compilationUnit} accepts exactly one routine —
 * see the class comment on {@code Db2CodingConventionAuditTest}).
 */
class Db2DmlCaptureGapAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " errors === " + r.getErrors());
        System.out.println("--- tableOps ---");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
            for (Db2JoinInfo j : t.getJoins()) {
                System.out.println("      JOIN " + j.getJoinType() + " " + j.getJoinedTable()
                        + " alias=" + j.getJoinedTableAlias() + " on=" + j.getCondition());
            }
        }
        System.out.println("--- statements ---");
        for (Db2StatementInfo s : r.getStatements()) {
            System.out.println("  " + s.getType() + " line=" + s.getLine());
        }
        System.out.println("--- dynamicSql ---");
        for (Db2DynamicSqlInfo d : r.getDynamicSql()) {
            System.out.println("  kind=" + d.getKind() + " stmtName=" + d.getStmtName()
                    + " sqlSource=" + d.getSqlSource() + " line=" + d.getLine());
        }
        System.out.println("--- calls ---");
        for (Db2CallInfo c : r.getCalls()) {
            System.out.println("  " + c.getSchema() + "." + c.getName() + " line=" + c.getLine());
        }
    }

    // ── Pattern 1: multi-table JOIN, plain SELECT + inside a cursor declaration ──

    private static final String MULTI_JOIN_SRC =
        "CREATE PROCEDURE p_multi_join(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER DEFAULT 0;\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id, c.name, s.qty, p.price\n"
      + "    FROM orders o\n"
      + "    INNER JOIN customers c ON o.cust_id = c.cust_id\n"
      + "    LEFT JOIN shipments s ON o.order_id = s.order_id\n"
      + "    LEFT JOIN products p ON s.product_id = p.product_id\n"
      + "    WHERE c.region = p_region;\n"
      + "  SELECT COUNT(*) INTO v_cnt\n"
      + "    FROM orders o2\n"
      + "    INNER JOIN customers c2 ON o2.cust_id = c2.cust_id\n"
      + "    LEFT JOIN shipments s2 ON o2.order_id = s2.order_id;\n"
      + "END";

    @Test
    @DisplayName("Pattern 1: multi-table JOIN in plain SELECT and inside a cursor")
    void multiTableJoin() {
        Db2RoutineInfo r = engine.parse(MULTI_JOIN_SRC, "p_multi_join", null);
        dump("Pattern1", r);
        boolean cursorHasAllFour = r.getTableOps().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()) && "o".equalsIgnoreCase(t.getAlias())
                        && t.getJoins().size() == 3);
        boolean plainSelectCaptured = r.getTableOps().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()) && "o2".equalsIgnoreCase(t.getAlias()));
        System.out.println(">>> cursorHasAllFourJoinedTables=" + cursorHasAllFour
                + " plainSelectJoinCaptured=" + plainSelectCaptured);
    }

    // ── Pattern 2: subqueries — WHERE clause, SELECT-list scalar, FROM derived table ──

    private static final String SUBQUERY_SRC =
        "CREATE PROCEDURE p_subqueries(IN p_min INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT COUNT(*) INTO v_cnt\n"
      + "  FROM orders o\n"
      + "  WHERE o.cust_id IN (SELECT c.cust_id FROM customers c WHERE c.active = 1);\n"
      + "  SELECT o.order_id, (SELECT MAX(p.price) FROM products p WHERE p.product_id = o.product_id) AS max_price\n"
      + "  INTO v_cnt, v_cnt\n"
      + "  FROM orders o\n"
      + "  WHERE o.order_id = p_min;\n"
      + "  SELECT d.total INTO v_cnt\n"
      + "  FROM (SELECT s.order_id, SUM(s.qty) AS total FROM shipments s GROUP BY s.order_id) d\n"
      + "  WHERE d.order_id = p_min;\n"
      + "END";

    @Test
    @DisplayName("Pattern 2: WHERE subquery, SELECT-list scalar subquery, FROM derived table — only tables inside subqueries captured")
    void subqueries() {
        Db2RoutineInfo r = engine.parse(SUBQUERY_SRC, "p_subqueries", null);
        dump("Pattern2", r);
        boolean sawCustomersInWhereSubquery = r.getTableOps().stream()
                .anyMatch(t -> "CUSTOMERS".equalsIgnoreCase(t.getTableName()));
        boolean sawProductsInScalarSubquery = r.getTableOps().stream()
                .anyMatch(t -> "PRODUCTS".equalsIgnoreCase(t.getTableName()));
        boolean sawShipmentsInDerivedTable = r.getTableOps().stream()
                .anyMatch(t -> "SHIPMENTS".equalsIgnoreCase(t.getTableName()));
        boolean subqueryStatementRecorded = r.getStatements().stream()
                .anyMatch(s -> "SUBQUERY".equals(s.getType()));
        System.out.println(">>> customersInWhereSubquery=" + sawCustomersInWhereSubquery
                + " productsInScalarSubquery=" + sawProductsInScalarSubquery
                + " shipmentsInDerivedTable=" + sawShipmentsInDerivedTable
                + " subqueryStatementRecorded=" + subqueryStatementRecorded);
    }

    // ── Pattern 3: CTE (WITH clause), one referencing another, main query selects from CTE name ──

    private static final String CTE_SRC =
        "CREATE PROCEDURE p_cte_demo(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  WITH regional_orders AS (\n"
      + "    SELECT o.order_id, o.cust_id FROM orders o WHERE o.region = p_region\n"
      + "  ),\n"
      + "  regional_customers AS (\n"
      + "    SELECT c.cust_id, c.name FROM customers c, regional_orders r WHERE c.cust_id = r.cust_id\n"
      + "  )\n"
      + "  SELECT COUNT(*) INTO v_cnt FROM regional_customers;\n"
      + "END";

    @Test
    @DisplayName("Pattern 3: CTE name not captured as real table; base tables inside CTE bodies ARE captured")
    void cte() {
        Db2RoutineInfo r = engine.parse(CTE_SRC, "p_cte_demo", null);
        dump("Pattern3", r);
        boolean cteNameCapturedAsTable = r.getTableOps().stream()
                .anyMatch(t -> "regional_orders".equalsIgnoreCase(t.getTableName())
                        || "regional_customers".equalsIgnoreCase(t.getTableName()));
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        boolean customersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "CUSTOMERS".equalsIgnoreCase(t.getTableName()));
        boolean cteStatementsRecorded = r.getStatements().stream()
                .filter(s -> "CTE".equals(s.getType())).count() == 2;
        System.out.println(">>> cteNameCapturedAsTable(shouldBeFalse)=" + cteNameCapturedAsTable
                + " ordersCaptured=" + ordersCaptured + " customersCaptured=" + customersCaptured
                + " cteStatementsRecorded(2 expected)=" + cteStatementsRecorded);
    }

    // ── Pattern 4: MERGE INTO target USING source ── target write, source read ──

    private static final String MERGE_SRC =
        "CREATE PROCEDURE p_merge_demo()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  MERGE INTO orders_target t\n"
      + "  USING orders_staging s\n"
      + "  ON t.order_id = s.order_id\n"
      + "  WHEN MATCHED THEN UPDATE SET t.status = s.status\n"
      + "  WHEN NOT MATCHED THEN INSERT (order_id, status) VALUES (s.order_id, s.status);\n"
      + "END";

    @Test
    @DisplayName("Pattern 4: MERGE INTO target USING source — target write, source read")
    void mergeStatement() {
        Db2RoutineInfo r = engine.parse(MERGE_SRC, "p_merge_demo", null);
        dump("Pattern4", r);
        boolean mergeStatementRecorded = r.getStatements().stream()
                .anyMatch(s -> "MERGE".equals(s.getType()));
        boolean targetCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders_target".equalsIgnoreCase(t.getTableName()));
        boolean sourceCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders_staging".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> mergeStatementRecorded=" + mergeStatementRecorded
                + " targetCapturedAsTableOp=" + targetCaptured
                + " sourceCapturedAsTableOp=" + sourceCaptured);

        // Bug 4 fix (§DML-audit): MERGE must capture both target (write) and source (read).
        assertTrue(mergeStatementRecorded, "MERGE statement record must be present");
        assertTrue(targetCaptured, "MERGE INTO target (orders_target) must be captured");
        assertTrue(sourceCaptured, "MERGE USING source (orders_staging) must be captured");
        Db2TableOpInfo targetOp = r.getTableOps().stream()
                .filter(t -> "orders_target".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        Db2TableOpInfo sourceOp = r.getTableOps().stream()
                .filter(t -> "orders_staging".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(targetOp);
        assertNotNull(sourceOp);
        assertEquals(Db2TableOpInfo.DmlType.MERGE, targetOp.getDmlType(), "target must be dmlType MERGE");
        assertEquals(Db2TableOpInfo.DmlType.SELECT, sourceOp.getDmlType(), "source must be dmlType SELECT (read)");
    }

    // ── Pattern 5: bulk ops — multi-row VALUES, and INSERT ... SELECT ──

    private static final String BULK_SRC =
        "CREATE PROCEDURE p_bulk_ops()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  INSERT INTO orders_archive (order_id, status) VALUES (1, 'A'), (2, 'B'), (3, 'C');\n"
      + "  INSERT INTO orders_summary (order_id, total)\n"
      + "    SELECT o.order_id, SUM(s.qty) FROM orders o INNER JOIN shipments s ON o.order_id = s.order_id\n"
      + "    GROUP BY o.order_id;\n"
      + "END";

    @Test
    @DisplayName("Pattern 5: multi-row INSERT VALUES and INSERT...SELECT captured as one statement-level op each")
    void bulkOps() {
        Db2RoutineInfo r = engine.parse(BULK_SRC, "p_bulk_ops", null);
        dump("Pattern5", r);
        long archiveInsertCount = r.getTableOps().stream()
                .filter(t -> t.getDmlType() == Db2TableOpInfo.DmlType.INSERT
                        && "orders_archive".equalsIgnoreCase(t.getTableName())).count();
        boolean summaryInsertCaptured = r.getTableOps().stream()
                .anyMatch(t -> t.getDmlType() == Db2TableOpInfo.DmlType.INSERT
                        && "orders_summary".equalsIgnoreCase(t.getTableName()));
        boolean sourceOrdersReadCaptured = r.getTableOps().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        boolean sourceShipmentsReadCaptured = r.getTableOps().stream()
                .anyMatch(t -> "SHIPMENTS".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> archiveInsertCount(expect 1, not 3)=" + archiveInsertCount
                + " summaryInsertCaptured=" + summaryInsertCaptured
                + " sourceOrdersReadCaptured=" + sourceOrdersReadCaptured
                + " sourceShipmentsReadCaptured=" + sourceShipmentsReadCaptured);
    }

    // ── Pattern 6: dynamic SQL, table name built via string concatenation ──

    private static final String DYNAMIC_CONCAT_SRC =
        "CREATE PROCEDURE p_dynamic_concat(IN v_table_prefix VARCHAR(30))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_sql VARCHAR(500);\n"
      + "  SET v_sql = 'SELECT * FROM ' || v_table_prefix || '_orders';\n"
      + "  EXECUTE IMMEDIATE v_sql;\n"
      + "END";

    private static final String DYNAMIC_LITERAL_SRC =
        "CREATE PROCEDURE p_dynamic_literal()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  EXECUTE IMMEDIATE 'DELETE FROM temp_staging WHERE created < 30';\n"
      + "END";

    @Test
    @DisplayName("Pattern 6: EXECUTE IMMEDIATE with table name via string concat (and a plain literal EXECUTE IMMEDIATE) — table extraction check")
    void dynamicSqlConcatTableExtraction() {
        Db2RoutineInfo r1 = engine.parse(DYNAMIC_CONCAT_SRC, "p_dynamic_concat", null);
        dump("Pattern6 (concat via variable)", r1);
        boolean anyTableOpFromDynamicConcat = !r1.getTableOps().isEmpty();
        boolean dynamicSqlEntryRecorded = !r1.getDynamicSql().isEmpty();
        System.out.println(">>> [concat case] tableOpsCaptured=" + anyTableOpFromDynamicConcat
                + " dynamicSqlEntryRecorded=" + dynamicSqlEntryRecorded
                + " sqlSourceStoredAsVariableNameOnly="
                + (dynamicSqlEntryRecorded ? r1.getDynamicSql().get(0).getSqlSource() : "N/A"));

        Db2RoutineInfo r2 = engine.parse(DYNAMIC_LITERAL_SRC, "p_dynamic_literal", null);
        dump("Pattern6 (plain literal, no concat)", r2);
        boolean anyTableOpFromDynamicLiteral = !r2.getTableOps().isEmpty();
        boolean literalStoredVerbatim = !r2.getDynamicSql().isEmpty()
                && r2.getDynamicSql().get(0).getSqlSource() != null
                && r2.getDynamicSql().get(0).getSqlSource().toUpperCase().contains("TEMP_STAGING");
        System.out.println(">>> [literal case] tableOpsCaptured=" + anyTableOpFromDynamicLiteral
                + " rawSqlSourceContainsTableTextVerbatim=" + literalStoredVerbatim);

        // Bug 9 fix (§DML-audit, literal-only scope): fully-literal EXECUTE IMMEDIATE must
        // yield a DYNAMIC table op; the variable-built case remains out of scope (documented gap).
        assertTrue(anyTableOpFromDynamicLiteral, "fully-literal EXECUTE IMMEDIATE must yield a table op");
        boolean tempStagingCaptured = r2.getTableOps().stream()
                .anyMatch(t -> "temp_staging".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.DYNAMIC);
        assertTrue(tempStagingCaptured, "expected temp_staging captured as DYNAMIC table op");
    }

    // ── Pattern 7: self-join — FROM orders o1 JOIN orders o2 ── single "orders" table captured ──

    private static final String SELF_JOIN_SRC =
        "CREATE PROCEDURE p_self_join()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT COUNT(*) INTO v_cnt\n"
      + "  FROM orders o1\n"
      + "  INNER JOIN orders o2 ON o1.parent_order_id = o2.order_id;\n"
      + "END";

    @Test
    @DisplayName("Pattern 7: self-join FROM orders o1 JOIN orders o2 — single 'orders' table name captured (twice, once per alias)")
    void selfJoin() {
        Db2RoutineInfo r = engine.parse(SELF_JOIN_SRC, "p_self_join", null);
        dump("Pattern7", r);
        long ordersOccurrences = r.getTableOps().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName())).count();
        boolean bothAliasesSeen = r.getTableOps().stream().anyMatch(t -> "o1".equalsIgnoreCase(t.getAlias()))
                && r.getTableOps().stream().anyMatch(t -> "o2".equalsIgnoreCase(t.getAlias()));
        System.out.println(">>> ordersTableOpOccurrences=" + ordersOccurrences + " bothAliasesSeen=" + bothAliasesSeen);
    }

    // ── Pattern 8: plain SELECT from a bare name that could be a view ──

    private static final String VIEW_LIKE_SRC =
        "CREATE PROCEDURE p_view_like_select()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER;\n"
      + "  SELECT COUNT(*) INTO v_cnt FROM active_customers_view;\n"
      + "END";

    @Test
    @DisplayName("Pattern 8: SELECT from bare name that could be a view — treated identically to a table (structural limitation)")
    void bareNameMayBeView() {
        Db2RoutineInfo r = engine.parse(VIEW_LIKE_SRC, "p_view_like_select", null);
        dump("Pattern8", r);
        boolean capturedAsSelectTableOp = r.getTableOps().stream()
                .anyMatch(t -> "active_customers_view".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.SELECT);
        System.out.println(">>> viewLikeNameCapturedAsPlainSelectTableOp(noViewDistinction, byDesign)=" + capturedAsSelectTableOp);
    }
}
