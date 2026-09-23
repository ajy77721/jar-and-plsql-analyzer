package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) — procedure/function DEFINITION style diversity and
 * query-writing diversity for PL/pgSQL, mirroring PostgresCommentStyleAuditTest /
 * PostgresDmlCaptureGapAuditTest. PostgresAnalysisEngine#parse handles exactly one routine per
 * call (no package spec/body split in Postgres), so A1 (overloads) and A4 (forward
 * declaration) are N/A at the dialect/harness level (Postgres DOES support overloading at the
 * catalog level, but each overload is a fully separate CREATE FUNCTION statement, not visible
 * inside one parse() call — noted in the report). Prints evidence; asserts only where
 * confident.
 */
class PostgresDefinitionAndQueryDiversityAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, PostgresRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("  errors=" + r.getErrors());
        for (PostgresParamInfo p : r.getParams()) {
            System.out.println("  param name=" + p.getName() + " type=" + p.getDataType()
                    + " dir=" + p.getDirection());
        }
        System.out.println("  tableOps=" + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@" + t.getLine()
                        + " alias=" + t.getAlias()).toList());
        System.out.println("  calls=" + r.getCalls().stream()
                .map(c -> c.getName() + "@" + c.getLine()).toList());
        System.out.println("  dynamicSql=" + r.getDynamicSql().stream()
                .map(PostgresDynamicSqlInfo::getSqlSource).toList());
        System.out.println("  lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
    }

    // A2: default param with complex expression -- PostgresParser.g4 defaultVal is
    // `literal | id | MINUS INT_NUM` only, same restriction as DB2. Probing actual behavior for
    // a real PL/pgSQL idiom `now() - interval '30 days'`.
    @Test
    @DisplayName("A2: default param with complex expression (now() - interval)")
    void complexDefaultParam() {
        String sql = """
                CREATE OR REPLACE FUNCTION proc_defaults(
                    p_date date DEFAULT (now() - interval '30 days'),
                    p_status varchar DEFAULT 'ACTIVE'
                ) RETURNS void AS $$
                BEGIN
                    UPDATE orders SET status = p_status WHERE order_date >= p_date;
                END;
                $$ LANGUAGE plpgsql;
                """;
        dump("A2: complex default param (now()-interval)", parse(sql));
    }

    // A2b: simple literal default -- grammar-supported case; check if captured in model at all.
    @Test
    @DisplayName("A2b: default param with simple literal (grammar-supported case)")
    void simpleDefaultParam() {
        String sql = """
                CREATE OR REPLACE FUNCTION proc_simple_default(
                    p_limit integer DEFAULT 10
                ) RETURNS void AS $$
                BEGIN
                    UPDATE orders SET flagged = 'Y' WHERE id <= p_limit;
                END;
                $$ LANGUAGE plpgsql;
                """;
        dump("A2b: simple literal default", parse(sql));
    }

    // A5: no-params without vs with empty parens
    @Test
    @DisplayName("A5: no-parameter function without vs with empty parens")
    void noParamsParenVariants() {
        String noParens = """
                CREATE OR REPLACE FUNCTION proc_noparen RETURNS void AS $$
                BEGIN
                    UPDATE orders SET status = 'SWEEP';
                END;
                $$ LANGUAGE plpgsql;
                """;
        String emptyParens = """
                CREATE OR REPLACE FUNCTION proc_emptyparen() RETURNS void AS $$
                BEGIN
                    UPDATE orders SET status = 'SWEEP';
                END;
                $$ LANGUAGE plpgsql;
                """;
        dump("A5: no parens", parse(noParens));
        dump("A5: empty parens ()", parse(emptyParens));
    }

    // A6: set-returning function used in FROM (native Postgres idiom)
    @Test
    @DisplayName("A6: set-returning function used in FROM clause")
    void setReturningFunctionInFrom() {
        String sql = """
                CREATE OR REPLACE FUNCTION proc_use_srf() RETURNS void AS $$
                DECLARE
                    v_cnt integer;
                BEGIN
                    SELECT count(*) INTO v_cnt FROM get_active_orders(1) AS t;
                END;
                $$ LANGUAGE plpgsql;
                """;
        dump("A6: set-returning function in FROM", parse(sql));
    }

    // B7: alias without AS vs with AS
    @Test
    @DisplayName("B7: table alias without AS vs with AS")
    void aliasWithWithoutAs() {
        String sql = """
                CREATE OR REPLACE FUNCTION proc_alias_mix() RETURNS void AS $$
                DECLARE
                    v_cnt integer;
                BEGIN
                    SELECT count(*) INTO v_cnt
                    FROM orders o
                    JOIN customers AS c ON c.id = o.customer_id;
                END;
                $$ LANGUAGE plpgsql;
                """;
        dump("B7: alias with/without AS", parse(sql));
    }

    // B9: set operators with different tables per branch
    @Test
    @DisplayName("B9: set operators with different tables per branch")
    void setOperatorsDifferentTables() {
        String sql = """
                CREATE OR REPLACE FUNCTION proc_setops() RETURNS void AS $$
                DECLARE
                    v_cnt integer;
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
                END;
                $$ LANGUAGE plpgsql;
                """;
        PostgresRoutineInfo r = parse(sql);
        dump("B9: set operators, different tables per branch", r);
        // REGRESSION (fixed bug): INTERSECT/EXCEPT must be recognized as real set operators,
        // not misparsed as a bare table alias on the preceding branch's table.
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "expected no parse errors, got: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(5, r.getTableOps().size(),
                "expected all 5 SELECT branches captured as table operations");
        for (PostgresTableOpInfo t : r.getTableOps()) {
            org.junit.jupiter.api.Assertions.assertNull(t.getAlias(),
                    "table alias must not be corrupted by INTERSECT/EXCEPT set-operator keywords, table="
                            + t.getTableName() + " alias=" + t.getAlias());
        }
        java.util.List<String> tables = r.getTableOps().stream().map(PostgresTableOpInfo::getTableName).toList();
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("online_orders", "phone_orders", "store_orders", "flagged_orders", "cancelled_orders"),
                tables);
    }

    // B10: large realistic query
    @Test
    @DisplayName("B10: large realistic query with joins+CTE+window fn+CASE")
    void largeRealisticQuery() {
        String sql = """
                CREATE OR REPLACE FUNCTION proc_large_query() RETURNS void AS $$
                DECLARE
                    v_cnt integer;
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
                    INTO v_cnt
                    FROM ranked_customers rc
                    LEFT JOIN shipments s ON s.customer_id = rc.id
                    LEFT JOIN order_items oi ON oi.customer_id = rc.id
                    LEFT JOIN products p ON p.id = oi.product_id
                    WHERE rc.rn <= 10;
                END;
                $$ LANGUAGE plpgsql;
                """;
        dump("B10: large realistic query", parse(sql));
    }

    // B11: semicolon-optional final statement
    @Test
    @DisplayName("B11: final statement missing trailing semicolon vs having it")
    void semicolonOptionalFinalStatement() {
        String withSemi = """
                CREATE OR REPLACE FUNCTION proc_semi_yes() RETURNS void AS $$
                BEGIN
                    UPDATE orders SET status = 'X' WHERE id = 1;
                END;
                $$ LANGUAGE plpgsql;
                """;
        String noSemi = """
                CREATE OR REPLACE FUNCTION proc_semi_no() RETURNS void AS $$
                BEGIN
                    UPDATE orders SET status = 'X' WHERE id = 1
                END;
                $$ LANGUAGE plpgsql;
                """;
        dump("B11: with semicolon", parse(withSemi));
        dump("B11: without semicolon", parse(noSemi));
    }

    // B12: CASE expression building dynamic SQL table name
    @Test
    @DisplayName("B12: CASE expression inside dynamic SQL table-name construction")
    void caseBuiltDynamicTableName() {
        String sql = """
                CREATE OR REPLACE FUNCTION proc_case_dyn(p_region varchar) RETURNS void AS $$
                DECLARE
                    v_sql text;
                BEGIN
                    v_sql := 'SELECT COUNT(*) FROM ' ||
                        CASE WHEN p_region = 'US' THEN 'us_orders' ELSE 'intl_orders' END;
                    EXECUTE v_sql;
                END;
                $$ LANGUAGE plpgsql;
                """;
        dump("B12: CASE-built dynamic SQL table name", parse(sql));
    }
}
