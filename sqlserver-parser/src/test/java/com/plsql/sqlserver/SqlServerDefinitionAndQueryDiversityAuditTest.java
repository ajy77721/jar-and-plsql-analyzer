package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) — procedure/function DEFINITION style diversity and
 * query-writing diversity for T-SQL, mirroring SqlServerCommentStyleAuditTest /
 * SqlServerDmlCaptureGapAuditTest. SqlServerAnalysisEngine#parse parses exactly one routine
 * per call, so overloading (item A1) and forward declarations (item A4) are N/A at this
 * engine's granularity (T-SQL itself has no package/spec+body split either — genuinely N/A
 * for the dialect, not just the harness). Prints evidence; asserts only where confident.
 */
class SqlServerDefinitionAndQueryDiversityAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("  errors=" + r.getErrors());
        for (SqlServerParamInfo p : r.getParams()) {
            System.out.println("  param name=" + p.getName() + " type=" + p.getDataType()
                    + " dir=" + p.getDirection());
        }
        System.out.println("  tableOps=" + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()
                        + " alias=" + t.getAlias()).toList());
        System.out.println("  calls=" + r.getCalls().stream()
                .map(c -> c.getName() + "@" + c.getLine()).toList());
        System.out.println("  dynamicSql=" + r.getDynamicSql().stream()
                .map(SqlServerDynamicSqlInfo::getSqlSource).toList());
        System.out.println("  lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
    }

    // A2: default param with complex expression
    @Test
    @DisplayName("A2: default param with complex expression (DATEADD)")
    void complexDefaultParam() {
        String sql = """
                CREATE PROCEDURE proc_defaults
                    @p_date DATE = DATEADD(DAY, -30, GETDATE()),
                    @p_status VARCHAR(20) = 'ACTIVE'
                AS
                BEGIN
                    UPDATE orders SET status = @p_status WHERE order_date >= @p_date;
                END
                """;
        SqlServerRoutineInfo r = parse(sql);
        dump("A2: complex default param (DATEADD)", r);
    }

    // A5: no-params without vs with empty parens
    @Test
    @DisplayName("A5: no-parameter procedure without vs with empty parens")
    void noParamsParenVariants() {
        String noParens = """
                CREATE PROCEDURE proc_noparen
                AS
                BEGIN
                    UPDATE orders SET status = 'SWEEP';
                END
                """;
        String emptyParens = """
                CREATE PROCEDURE proc_emptyparen ()
                AS
                BEGIN
                    UPDATE orders SET status = 'SWEEP';
                END
                """;
        dump("A5: no parens", parse(noParens));
        dump("A5: empty parens ()", parse(emptyParens));
    }

    // A6: table-valued function used in FROM
    @Test
    @DisplayName("A6: table-valued function used in FROM clause")
    void tableValuedFunctionInFrom() {
        String sql = """
                CREATE PROCEDURE proc_use_tvf
                AS
                BEGIN
                    SELECT * FROM dbo.get_active_orders(1) AS t;
                END
                """;
        dump("A6: table-valued function in FROM", parse(sql));
    }

    // B7: alias without AS vs with AS
    @Test
    @DisplayName("B7: table alias without AS vs with AS")
    void aliasWithWithoutAs() {
        String sql = """
                CREATE PROCEDURE proc_alias_mix
                AS
                BEGIN
                    SELECT * FROM orders o
                    JOIN customers AS c ON c.id = o.customer_id;
                END
                """;
        SqlServerRoutineInfo r = parse(sql);
        dump("B7: alias with/without AS", r);
        // REGRESSION (fixed bug): table alias must be captured for FROM/JOIN table ops,
        // whether or not the AS keyword is present.
        org.junit.jupiter.api.Assertions.assertEquals(2, r.getTableOps().size());
        org.junit.jupiter.api.Assertions.assertEquals("o",
                r.getTableOps().stream().filter(t -> "orders".equalsIgnoreCase(t.getTableName()))
                        .findFirst().orElseThrow().getAlias());
        org.junit.jupiter.api.Assertions.assertEquals("c",
                r.getTableOps().stream().filter(t -> "customers".equalsIgnoreCase(t.getTableName()))
                        .findFirst().orElseThrow().getAlias());
    }

    // B9: UNION/UNION ALL/INTERSECT/EXCEPT with different tables per branch
    @Test
    @DisplayName("B9: set operators with different tables per branch")
    void setOperatorsDifferentTables() {
        String sql = """
                CREATE PROCEDURE proc_setops
                AS
                BEGIN
                    SELECT id FROM online_orders
                    UNION
                    SELECT id FROM phone_orders
                    UNION ALL
                    SELECT id FROM store_orders
                    INTERSECT
                    SELECT id FROM flagged_orders
                    EXCEPT
                    SELECT id FROM cancelled_orders;
                END
                """;
        SqlServerRoutineInfo r = parse(sql);
        dump("B9: set operators, different tables per branch", r);
        // REGRESSION (fixed bug): INTERSECT/EXCEPT must not be misread as a bare table alias
        // by the FROM/JOIN alias-recovery regex.
        org.junit.jupiter.api.Assertions.assertEquals(5, r.getTableOps().size());
        for (SqlServerTableOpInfo t : r.getTableOps()) {
            org.junit.jupiter.api.Assertions.assertNull(t.getAlias(),
                    "table alias must not be corrupted by INTERSECT/EXCEPT keywords, table="
                            + t.getTableName() + " alias=" + t.getAlias());
        }
    }

    // B10: large realistic query
    @Test
    @DisplayName("B10: large realistic query with joins+CTE+window fn+CASE")
    void largeRealisticQuery() {
        String sql = """
                CREATE PROCEDURE proc_large_query
                AS
                BEGIN
                    WITH region_totals AS (
                      SELECT o.region, SUM(o.amount) AS total_amount
                      FROM orders o
                      JOIN customers c ON c.id = o.customer_id
                      WHERE o.status = 'CLOSED'
                      GROUP BY o.region
                    ),
                    ranked_customers AS (
                      SELECT c.id, c.name, c.region,
                             ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY c.lifetime_value DESC) AS rn
                      FROM customers c
                      JOIN region_totals rt ON rt.region = c.region
                    )
                    SELECT rc.id, rc.name,
                           CASE WHEN rc.rn = 1 THEN 'TOP'
                                WHEN rc.rn <= 5 THEN 'HIGH'
                                ELSE 'STANDARD' END AS tier,
                           s.warehouse_code, p.category
                    FROM ranked_customers rc
                    LEFT JOIN shipments s ON s.customer_id = rc.id
                    LEFT JOIN order_items oi ON oi.customer_id = rc.id
                    LEFT JOIN products p ON p.id = oi.product_id
                    WHERE rc.rn <= 10
                    ORDER BY rc.region, rc.rn;
                END
                """;
        dump("B10: large realistic query", parse(sql));
    }

    // B11: semicolon-optional final statement
    @Test
    @DisplayName("B11: final statement missing trailing semicolon vs having it")
    void semicolonOptionalFinalStatement() {
        String withSemi = """
                CREATE PROCEDURE proc_semi_yes
                AS
                BEGIN
                    UPDATE orders SET status = 'X' WHERE id = 1;
                END
                """;
        String noSemi = """
                CREATE PROCEDURE proc_semi_no
                AS
                BEGIN
                    UPDATE orders SET status = 'X' WHERE id = 1
                END
                """;
        dump("B11: with semicolon", parse(withSemi));
        dump("B11: without semicolon", parse(noSemi));
    }

    // B12: CASE expression building dynamic SQL table name
    @Test
    @DisplayName("B12: CASE expression inside dynamic SQL table-name construction")
    void caseBuiltDynamicTableName() {
        String sql = """
                CREATE PROCEDURE proc_case_dyn
                    @v_region VARCHAR(10)
                AS
                BEGIN
                    DECLARE @sql NVARCHAR(200);
                    SET @sql = 'SELECT COUNT(*) FROM ' +
                        CASE WHEN @v_region = 'US' THEN 'us_orders' ELSE 'intl_orders' END;
                    EXEC sp_executesql @sql;
                END
                """;
        dump("B12: CASE-built dynamic SQL table name", parse(sql));
    }
}
