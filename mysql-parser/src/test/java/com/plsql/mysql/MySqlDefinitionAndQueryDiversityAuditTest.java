package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) — procedure/function DEFINITION style diversity and
 * query-writing diversity for MySQL, mirroring MySqlCommentStyleAuditTest /
 * MySqlDmlCaptureGapAuditTest. MySqlGrammarEngine#parse handles exactly one routine per call
 * (no package spec/body split), so A1 (overloads) and A4 (forward declaration) are N/A at the
 * dialect level. A2 (default parameter values) is ALSO dialect-level N/A: native MySQL
 * CREATE PROCEDURE/FUNCTION syntax has no DEFAULT clause for parameters at all — confirmed by
 * grammar: MySqlParser.g4's `param` rule is `direction=(IN|OUT|INOUT)? paramName dataType`
 * with no DEFAULT alternative. Prints evidence; asserts only where confident.
 */
public class MySqlDefinitionAndQueryDiversityAuditTest {

    private final MySqlGrammarEngine engine = new MySqlGrammarEngine();

    private MySqlRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, MySqlRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("  errors=" + r.getErrors());
        for (MySqlParamInfo p : r.getParams()) {
            System.out.println("  param name=" + p.getName() + " type=" + p.getDataType()
                    + " dir=" + p.getDirection());
        }
        System.out.println("  tableOps=" + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@" + t.getLine()
                        + " alias=" + t.getAlias()).toList());
        System.out.println("  calls=" + r.getCalls().stream()
                .map(c -> c.getName() + "@" + c.getLine()).toList());
        System.out.println("  dynamicSql=" + r.getDynamicSql().stream()
                .map(MySqlDynamicSqlInfo::getSqlSource).toList());
        System.out.println("  lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
    }

    // A2: native MySQL has NO default-param syntax at all; probing what happens if written anyway.
    @Test
    @DisplayName("A2 (N/A probe): DEFAULT clause on a param is not native MySQL syntax")
    void defaultParamNotSupported() {
        String sql = """
                CREATE PROCEDURE proc_defaults(IN p_status VARCHAR(20) DEFAULT 'ACTIVE')
                BEGIN
                    UPDATE orders SET status = p_status;
                END
                """;
        dump("A2 (N/A probe): DEFAULT on param", parse(sql));
    }

    // A5: no-params without vs with empty parens (MySQL requires parens either way)
    @Test
    @DisplayName("A5: no-parameter procedure - MySQL always requires parens")
    void noParamsParenVariants() {
        String emptyParens = """
                CREATE PROCEDURE proc_emptyparen()
                BEGIN
                    UPDATE orders SET status = 'SWEEP';
                END
                """;
        dump("A5: empty parens (native MySQL form)", parse(emptyParens));
    }

    // A6: table function used in FROM -- MySQL (pre-8.0/no genuine SRF like Postgres) doesn't
    // support a native "table function in FROM" construct the way Oracle/Postgres do; a stored
    // FUNCTION cannot be placed inside FROM at all. Probing what happens if written anyway
    // (calling a scalar function inside a derived-table-free FROM is simply not legal MySQL).
    @Test
    @DisplayName("A6 (N/A probe): MySQL has no table-function-in-FROM equivalent")
    void tableFunctionInFromNotSupported() {
        String sql = """
                CREATE PROCEDURE proc_use_tablefn()
                BEGIN
                    SELECT * FROM get_active_orders();
                END
                """;
        dump("A6 (N/A probe): function call in FROM", parse(sql));
    }

    // B7: alias without AS vs with AS
    @Test
    @DisplayName("B7: table alias without AS vs with AS")
    void aliasWithWithoutAs() {
        String sql = """
                CREATE PROCEDURE proc_alias_mix()
                BEGIN
                    SELECT * FROM orders o
                    JOIN customers AS c ON c.id = o.customer_id;
                END
                """;
        dump("B7: alias with/without AS", parse(sql));
    }

    // B9: set operators - MySQL (pre-8.0.31) only has UNION/UNION ALL, no native INTERSECT/EXCEPT
    @Test
    @DisplayName("B9: UNION/UNION ALL with different tables per branch")
    void setOperatorsDifferentTables() {
        String sql = """
                CREATE PROCEDURE proc_setops()
                BEGIN
                    SELECT id FROM online_orders
                    UNION
                    SELECT id FROM phone_orders
                    UNION ALL
                    SELECT id FROM store_orders;
                END
                """;
        dump("B9: UNION/UNION ALL, different tables per branch", parse(sql));
    }

    // B10: large realistic query
    @Test
    @DisplayName("B10: large realistic query with joins+CTE+window fn+CASE")
    void largeRealisticQuery() {
        String sql = """
                CREATE PROCEDURE proc_large_query()
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
                    WHERE rc.rn <= 10;
                END
                """;
        dump("B10: large realistic query", parse(sql));
    }

    // B11: semicolon-optional final statement
    @Test
    @DisplayName("B11: final statement missing trailing semicolon vs having it")
    void semicolonOptionalFinalStatement() {
        String withSemi = """
                CREATE PROCEDURE proc_semi_yes()
                BEGIN
                    UPDATE orders SET status = 'X' WHERE id = 1;
                END
                """;
        String noSemi = """
                CREATE PROCEDURE proc_semi_no()
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
                CREATE PROCEDURE proc_case_dyn(IN p_region VARCHAR(10))
                BEGIN
                    SET @sql = CONCAT('SELECT COUNT(*) FROM ',
                        CASE WHEN p_region = 'US' THEN 'us_orders' ELSE 'intl_orders' END);
                    PREPARE stmt FROM @sql;
                    EXECUTE stmt;
                    DEALLOCATE PREPARE stmt;
                END
                """;
        MySqlRoutineInfo r = parse(sql);
        dump("B12: CASE-built dynamic SQL table name", r);
        // REGRESSION (fixed bug): EXECUTE stmt / DEALLOCATE PREPARE stmt must carry the SQL
        // source already known from the matching PREPARE, instead of leaving sqlSource null.
        org.junit.jupiter.api.Assertions.assertEquals(3, r.getDynamicSql().size());
        for (MySqlDynamicSqlInfo d : r.getDynamicSql()) {
            org.junit.jupiter.api.Assertions.assertNotNull(d.getSqlSource(),
                    "dynamic SQL record's sqlSource must be resolved from the matching PREPARE, not left null; kind="
                            + d.getKind());
        }
    }
}
