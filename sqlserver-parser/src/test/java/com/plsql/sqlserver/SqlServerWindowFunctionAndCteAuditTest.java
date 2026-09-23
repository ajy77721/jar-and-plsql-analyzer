package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep17 audit — window/analytic functions (OVER(...)) and multi-CTE chains, SQL Server dialect.
 *
 * <h2>Method</h2>
 * Runs real snippets through {@link SqlServerAnalysisEngine} (actual grammar + actual
 * {@code SqlServerAnalysisVisitor} regex extraction — no mocks) and records the actual observed
 * table-ops/joins/calls list, compared against the correct expected result.
 *
 * <h2>Why this dialect is structurally different from the Postgres OVER(...) bug</h2>
 * SqlServerParser.g4's {@code stmtTail}/{@code stmtToken} catch-all consumes EVERYTHING inside a
 * SELECT — including {@code OVER}, {@code PARTITION}, {@code ORDER BY}, parens, commas — as plain
 * tokens with no dedicated grammar rule for window functions at all (confirmed: grep of
 * SqlServerLexer.g4/SqlServerParser.g4 shows OVER/PARTITION are lexer tokens only, matched by the
 * generic {@code LPAREN stmtToken* RPAREN} / bare-token alternatives of {@code stmtToken}). So
 * there is no grammar-level parse tree branch that can desync on OVER(...) the way Postgres's
 * dedicated (but too-narrow) rule did. All table/join capture for SELECTs happens by
 * {@link java.util.regex.Pattern} scanning of {@code ctx.stmtTail()}'s raw original text
 * (TABLE_REF_PATTERN / JOIN_PATTERN in SqlServerAnalysisVisitor) — so the only way an OVER(...)
 * clause could desync capture here is if it confuses one of those *regexes* specifically (e.g. by
 * containing a stray FROM/JOIN-shaped substring, or by breaking JOIN_PATTERN's bounded ON-condition
 * capture or its boundary-keyword lookahead if OVER(...) sits inside/after an ON clause).
 */
class SqlServerWindowFunctionAndCteAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private List<String> tableNames(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
    }

    private String dump(SqlServerRoutineInfo r) {
        StringBuilder sb = new StringBuilder();
        sb.append("tableOps=");
        for (SqlServerTableOpInfo t : r.getTableOps()) {
            sb.append("[").append(t.getDmlType()).append(" ")
              .append(t.getSchemaName() != null ? t.getSchemaName() + "." : "")
              .append(t.getTableName()).append(" alias=").append(t.getAlias())
              .append(" joins=").append(t.getJoins() == null ? 0 : t.getJoins().size()).append("] ");
            if (t.getJoins() != null) {
                for (SqlServerJoinInfo j : t.getJoins()) {
                    sb.append("  JOIN[").append(j.getJoinType()).append(" ")
                      .append(j.getJoinedTableSchema() != null ? j.getJoinedTableSchema() + "." : "")
                      .append(j.getJoinedTable()).append(" alias=").append(j.getJoinedTableAlias())
                      .append(" leftTable=").append(j.getLeftTable())
                      .append(" cond=").append(j.getCondition()).append("] ");
                }
            }
        }
        sb.append(" calls=");
        for (SqlServerCallInfo c : r.getCalls()) {
            sb.append(c.getSchema() != null ? c.getSchema() + "." : "").append(c.getName()).append(" ");
        }
        return sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────────
    // 1. Window / analytic functions — standalone
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("1a: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) does not desync FROM capture")
    void rowNumberOverStandalone() {
        String sql =
                "CREATE PROCEDURE usp_RankedEmployees\nAS\nBEGIN\n"
              + "  SELECT EmployeeId, DeptId, Salary,\n"
              + "    ROW_NUMBER() OVER (PARTITION BY DeptId ORDER BY Salary DESC) AS rn\n"
              + "  FROM hr.Employee;\n"
              + "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> tables = tableNames(r);
        System.out.println("1a: " + dump(r));
        assertTrue(tables.contains("EMPLOYEE"),
                "EXPECTED hr.Employee captured as source table; ACTUAL tables=" + tables + " | " + dump(r));
        assertFalse(tables.contains("DEPTID"), "phantom table from PARTITION BY column name");
        assertFalse(tables.contains("SALARY"), "phantom table from ORDER BY column name");
    }

    @Test
    @DisplayName("1b: RANK()/DENSE_RANK()/LAG/LEAD/NTILE/SUM OVER, standalone SELECT, no phantom tables/calls")
    void multipleWindowFunctionsStandalone() {
        String sql =
                "CREATE PROCEDURE usp_WindowMix\nAS\nBEGIN\n"
              + "  SELECT\n"
              + "    OrderId, CustomerId, Amount, OrderDate,\n"
              + "    RANK() OVER (PARTITION BY CustomerId ORDER BY Amount DESC) AS rnk,\n"
              + "    DENSE_RANK() OVER (ORDER BY Amount DESC) AS drnk,\n"
              + "    LAG(Amount, 1) OVER (ORDER BY OrderDate) AS prevAmt,\n"
              + "    LEAD(Amount, 1) OVER (ORDER BY OrderDate) AS nextAmt,\n"
              + "    NTILE(4) OVER (ORDER BY Amount) AS quartile,\n"
              + "    SUM(Amount) OVER (PARTITION BY CustomerId) AS custTotal\n"
              + "  FROM sales.Orders;\n"
              + "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> tables = tableNames(r);
        System.out.println("1b: " + dump(r));
        assertEquals(1, tables.size(), "EXPECTED exactly 1 table op (sales.Orders); ACTUAL=" + tables + " | " + dump(r));
        assertTrue(tables.contains("ORDERS"));
        assertTrue(r.getCalls().isEmpty(),
                "EXPECTED no phantom outbound calls from RANK/LAG/LEAD/NTILE/SUM window fns; ACTUAL calls="
                        + r.getCalls().stream().map(c -> c.getName()).collect(Collectors.toList()));
    }

    @Test
    @DisplayName("1c: window function combined with a real JOIN in the same query")
    void windowFunctionWithRealJoin() {
        String sql =
                "CREATE PROCEDURE usp_WindowedJoin\nAS\nBEGIN\n"
              + "  SELECT e.EmployeeId, d.DeptName,\n"
              + "    ROW_NUMBER() OVER (PARTITION BY e.DeptId ORDER BY e.Salary DESC) AS rn\n"
              + "  FROM hr.Employee e\n"
              + "  INNER JOIN hr.Department d ON e.DeptId = d.DeptId\n"
              + "  WHERE e.Active = 1;\n"
              + "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> tables = tableNames(r);
        System.out.println("1c: " + dump(r));
        assertTrue(tables.contains("EMPLOYEE"), "ACTUAL=" + dump(r));
        assertTrue(tables.contains("DEPARTMENT"), "EXPECTED joined table hr.Department captured; ACTUAL=" + dump(r));
        SqlServerTableOpInfo empOp = r.getTableOps().stream()
                .filter(t -> "EMPLOYEE".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(empOp);
        assertEquals(1, empOp.getJoins().size(), "EXPECTED exactly 1 join edge on Employee; ACTUAL=" + dump(r));
        assertEquals("Department", empOp.getJoins().get(0).getJoinedTable());
    }

    @Test
    @DisplayName("1d: window function whose OVER() PARTITION BY list has multiple comma-separated columns, plus a JOIN after it")
    void windowFunctionMultiColumnPartitionWithTrailingJoin() {
        // Stress the comma-inside-PARTITION-BY scenario explicitly, mirroring the Postgres bug shape.
        String sql =
                "CREATE PROCEDURE usp_MultiPartition\nAS\nBEGIN\n"
              + "  SELECT o.OrderId, o.CustomerId, o.Region,\n"
              + "    SUM(o.Amount) OVER (PARTITION BY o.Region, o.CustomerId ORDER BY o.OrderDate) AS runningTotal\n"
              + "  FROM sales.Orders o\n"
              + "  JOIN sales.Customer c ON o.CustomerId = c.CustomerId;\n"
              + "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> tables = tableNames(r);
        System.out.println("1d: " + dump(r));
        assertTrue(tables.contains("ORDERS"), "ACTUAL=" + dump(r));
        assertTrue(tables.contains("CUSTOMER"), "EXPECTED bare-JOIN'd Customer table captured despite comma inside PARTITION BY; ACTUAL=" + dump(r));
        assertFalse(tables.contains("REGION"), "phantom table from PARTITION BY column");
    }

    // ─────────────────────────────────────────────────────────────────────
    // 2. Multiple chained CTEs (3+, one referencing a prior CTE)
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("2a: three chained CTEs — real tables captured, CTE names suppressed")
    void threeChainedCtes() {
        String sql =
                "CREATE PROCEDURE usp_ChainedCtes\nAS\nBEGIN\n"
              + "  WITH cte_a AS (\n"
              + "    SELECT CustomerId, Region FROM sales.Customer\n"
              + "  ),\n"
              + "  cte_b AS (\n"
              + "    SELECT a.CustomerId, o.OrderId FROM cte_a a JOIN sales.Orders o ON a.CustomerId = o.CustomerId\n"
              + "  ),\n"
              + "  cte_c AS (\n"
              + "    SELECT b.OrderId, p.ProductName FROM cte_b b JOIN sales.Product p ON b.OrderId = p.OrderId\n"
              + "  )\n"
              + "  SELECT * FROM cte_c;\n"
              + "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> tables = tableNames(r);
        System.out.println("2a: " + dump(r));
        assertTrue(tables.contains("CUSTOMER"), "ACTUAL=" + dump(r));
        assertTrue(tables.contains("ORDERS"), "ACTUAL=" + dump(r));
        assertTrue(tables.contains("PRODUCT"), "ACTUAL=" + dump(r));
        assertFalse(tables.contains("CTE_A"), "cte_a leaked as a phantom external table; ACTUAL=" + dump(r));
        assertFalse(tables.contains("CTE_B"), "cte_b leaked as a phantom external table; ACTUAL=" + dump(r));
        assertFalse(tables.contains("CTE_C"), "cte_c leaked as a phantom external table (final SELECT FROM cte_c); ACTUAL=" + dump(r));

        // FIXED: cte_b's "JOIN sales.Orders o ON ..." and cte_c's "JOIN sales.Product p ON ..."
        // are both real joins whose base resolves to a prior CTE name (cte_a / cte_b). Rather than
        // silently dropping the edge, attachJoins() now attributes it to the joined table's own op
        // (sales.Orders, sales.Product respectively), with leftTable recording the CTE name as the
        // structural base — so both join edges are now recorded (2 total), and no table is lost.
        long totalJoins = r.getTableOps().stream().mapToLong(t -> t.getJoins() == null ? 0 : t.getJoins().size()).sum();
        assertEquals(2, totalJoins,
                "FIXED: both chained-CTE JOINs (cte_b's onto Orders, cte_c's onto Product) are now "
              + "recorded as edges attributed to the joined table's own op; ACTUAL=" + dump(r));
    }

    @Test
    @DisplayName("2b: window function INSIDE a CTE definition + final SELECT filtering on it")
    void windowFunctionInsideCte() {
        String sql =
                "CREATE PROCEDURE usp_TopEarnerPerDept\nAS\nBEGIN\n"
              + "  WITH ranked AS (\n"
              + "    SELECT EmployeeId, DeptId, Salary,\n"
              + "      ROW_NUMBER() OVER (PARTITION BY DeptId ORDER BY Salary DESC) AS rn\n"
              + "    FROM hr.Employee\n"
              + "  )\n"
              + "  SELECT * FROM ranked WHERE rn = 1;\n"
              + "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> tables = tableNames(r);
        System.out.println("2b: " + dump(r));
        assertTrue(tables.contains("EMPLOYEE"),
                "EXPECTED hr.Employee (inside the CTE body) captured; ACTUAL=" + dump(r));
        assertFalse(tables.contains("RANKED"), "ranked CTE name leaked as phantom table; ACTUAL=" + dump(r));
        assertFalse(tables.contains("DEPTID"), "phantom table from PARTITION BY column inside CTE");
    }

    @Test
    @DisplayName("2c: chained CTE + window function inside one CTE + real JOIN in a later CTE")
    void chainedCteWithWindowFunctionAndJoin() {
        String sql =
                "CREATE PROCEDURE usp_TopOrdersPerRegion\nAS\nBEGIN\n"
              + "  WITH ranked_orders AS (\n"
              + "    SELECT o.OrderId, o.CustomerId, o.Region, o.Amount,\n"
              + "      RANK() OVER (PARTITION BY o.Region ORDER BY o.Amount DESC) AS rnk\n"
              + "    FROM sales.Orders o\n"
              + "  ),\n"
              + "  top_orders AS (\n"
              + "    SELECT ro.OrderId, ro.CustomerId, c.CustomerName\n"
              + "    FROM ranked_orders ro\n"
              + "    JOIN sales.Customer c ON ro.CustomerId = c.CustomerId\n"
              + "    WHERE ro.rnk <= 3\n"
              + "  )\n"
              + "  SELECT * FROM top_orders;\n"
              + "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> tables = tableNames(r);
        System.out.println("2c: " + dump(r));
        assertTrue(tables.contains("ORDERS"), "ACTUAL=" + dump(r));
        assertTrue(tables.contains("CUSTOMER"), "EXPECTED sales.Customer joined in top_orders captured; ACTUAL=" + dump(r));
        assertFalse(tables.contains("RANKED_ORDERS"), "ranked_orders CTE leaked; ACTUAL=" + dump(r));
        assertFalse(tables.contains("TOP_ORDERS"), "top_orders CTE leaked (final SELECT FROM top_orders); ACTUAL=" + dump(r));
        assertFalse(tables.contains("REGION"), "phantom table from PARTITION BY column");

        // Same FIX as 2a: "JOIN sales.Customer c ON ..." inside top_orders has a base
        // (ranked_orders) that is a CTE name, so attachJoins() now attributes the edge to
        // sales.Customer's own op instead of dropping it.
        long totalJoins = r.getTableOps().stream().mapToLong(t -> t.getJoins() == null ? 0 : t.getJoins().size()).sum();
        assertEquals(1, totalJoins,
                "FIXED: same as test 2a — the JOIN inside top_orders (base = ranked_orders, a CTE "
              + "name) is now recorded, attributed to sales.Customer's own op; ACTUAL=" + dump(r));
    }
}
