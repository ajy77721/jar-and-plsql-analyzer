package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) — procedure/function DEFINITION style diversity and
 * query-writing diversity for DB2 SQL PL, mirroring Db2CommentStyleAuditTest /
 * Db2DmlCaptureGapAuditTest. Db2AnalysisEngine#parse handles exactly one routine per call
 * (no package spec/body split in DB2), so overloading (A1) and forward declaration (A4)
 * are N/A at the dialect level. Prints evidence; asserts only where confident.
 */
class Db2DefinitionAndQueryDiversityAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("  errors=" + r.getErrors());
        for (Db2ParamInfo p : r.getParams()) {
            System.out.println("  param name=" + p.getName() + " type=" + p.getDataType()
                    + " dir=" + p.getDirection());
        }
        System.out.println("  tableOps=" + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()
                        + " alias=" + t.getAlias()).toList());
        System.out.println("  calls=" + r.getCalls().stream()
                .map(c -> c.getName() + "@" + c.getLine()).toList());
        System.out.println("  dynamicSql=" + r.getDynamicSql().stream()
                .map(Db2DynamicSqlInfo::getSqlSource).toList());
        System.out.println("  lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
    }

    // A2: default param with complex expression -- DB2 grammar's defaultVal is
    // `literal | id | MINUS INT_NUM` only (see Db2Parser.g4 ~line 78); a function-call
    // expression should either fail this alternative or be misparsed. Probing actual behavior.
    @Test
    @DisplayName("A2: default param with complex expression (CURRENT DATE - 30 DAYS)")
    void complexDefaultParam() {
        String sql = """
                CREATE PROCEDURE proc_defaults (
                    IN p_date DATE DEFAULT CURRENT DATE - 30 DAYS,
                    IN p_status VARCHAR(20) DEFAULT 'ACTIVE'
                )
                BEGIN
                    UPDATE orders SET status = p_status WHERE order_date >= p_date;
                END
                """;
        dump("A2: complex default param (CURRENT DATE - 30 DAYS)", parse(sql));
    }

    // A2b: simple literal default -- to check whether even the SIMPLE case (grammar-supported)
    // makes it into Db2ParamInfo at all (there is no getDefaultValue() on Db2ParamInfo).
    @Test
    @DisplayName("A2b: default param with simple literal (grammar-supported case)")
    void simpleDefaultParam() {
        String sql = """
                CREATE PROCEDURE proc_simple_default (
                    IN p_limit INTEGER DEFAULT 10
                )
                BEGIN
                    UPDATE orders SET flagged = 'Y' WHERE id <= p_limit;
                END
                """;
        dump("A2b: simple literal default", parse(sql));
    }

    // A5: no-params without vs with empty parens
    @Test
    @DisplayName("A5: no-parameter procedure without vs with empty parens")
    void noParamsParenVariants() {
        String noParens = """
                CREATE PROCEDURE proc_noparen
                BEGIN
                    UPDATE orders SET status = 'SWEEP';
                END
                """;
        String emptyParens = """
                CREATE PROCEDURE proc_emptyparen ()
                BEGIN
                    UPDATE orders SET status = 'SWEEP';
                END
                """;
        dump("A5: no parens", parse(noParens));
        dump("A5: empty parens ()", parse(emptyParens));
    }

    // A6: table function used in FROM (DB2 supports TABLE(func(...)) syntax)
    @Test
    @DisplayName("A6: table function used via TABLE() in FROM clause")
    void tableFunctionInFrom() {
        String sql = """
                CREATE PROCEDURE proc_use_tablefn()
                BEGIN
                    DECLARE v_cnt INTEGER;
                    SELECT COUNT(*) INTO v_cnt FROM TABLE(get_active_orders()) AS t;
                END
                """;
        dump("A6: table function in FROM", parse(sql));
    }

    // B7: alias without AS vs with AS
    @Test
    @DisplayName("B7: table alias without AS vs with AS")
    void aliasWithWithoutAs() {
        String sql = """
                CREATE PROCEDURE proc_alias_mix()
                BEGIN
                    DECLARE v_cnt INTEGER;
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o
                    JOIN customers AS c ON c.id = o.customer_id;
                END
                """;
        dump("B7: alias with/without AS", parse(sql));
    }

    // B9: set operators (UNION/UNION ALL/INTERSECT/EXCEPT) different tables per branch
    @Test
    @DisplayName("B9: set operators with different tables per branch")
    void setOperatorsDifferentTables() {
        String sql = """
                CREATE PROCEDURE proc_setops()
                BEGIN
                    DECLARE v_cnt INTEGER;
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
        Db2RoutineInfo r = parse(sql);
        dump("B9: set operators, different tables per branch", r);
        // REGRESSION (fixed bug): INTERSECT/EXCEPT must be recognized as real set operators,
        // not misparsed as a bare table alias on the preceding branch's table.
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "expected no parse errors, got: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertEquals(5, r.getTableOps().size(),
                "expected all 5 SELECT branches captured as table operations");
        for (Db2TableOpInfo t : r.getTableOps()) {
            org.junit.jupiter.api.Assertions.assertNull(t.getAlias(),
                    "table alias must not be corrupted by INTERSECT/EXCEPT set-operator keywords, table="
                            + t.qualifiedTable() + " alias=" + t.getAlias());
        }
        java.util.List<String> tables = r.getTableOps().stream().map(Db2TableOpInfo::qualifiedTable).toList();
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("ONLINE_ORDERS", "PHONE_ORDERS", "STORE_ORDERS", "FLAGGED_ORDERS", "CANCELLED_ORDERS"),
                tables);
    }

    // B10: large realistic query
    @Test
    @DisplayName("B10: large realistic query with joins+CTE+window fn+CASE")
    void largeRealisticQuery() {
        String sql = """
                CREATE PROCEDURE proc_large_query()
                BEGIN
                    DECLARE v_cnt INTEGER;
                    SELECT rc.id, rc.name,
                           CASE WHEN rc.rn = 1 THEN 'TOP'
                                WHEN rc.rn <= 5 THEN 'HIGH'
                                ELSE 'STANDARD' END AS tier,
                           s.warehouse_code, p.category
                    INTO v_cnt
                    FROM (
                        SELECT c.id, c.name, c.region,
                               ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY c.lifetime_value DESC) AS rn
                        FROM customers c
                        JOIN orders o ON o.customer_id = c.id
                        WHERE o.status = 'CLOSED'
                    ) rc
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
                CREATE PROCEDURE proc_case_dyn (
                    IN p_region VARCHAR(10)
                )
                BEGIN
                    DECLARE v_sql VARCHAR(200);
                    DECLARE v_stmt STATEMENT;
                    SET v_sql = 'SELECT COUNT(*) FROM ' ||
                        CASE WHEN p_region = 'US' THEN 'us_orders' ELSE 'intl_orders' END;
                    PREPARE v_stmt FROM v_sql;
                    EXECUTE v_stmt;
                END
                """;
        Db2RoutineInfo r = parse(sql);
        dump("B12: CASE-built dynamic SQL table name", r);
        // REGRESSION (fixed bug): EXECUTE <stmtName> must carry the SQL text already known from
        // the matching PREPARE, instead of leaving sqlSource null.
        org.junit.jupiter.api.Assertions.assertEquals(2, r.getDynamicSql().size());
        org.junit.jupiter.api.Assertions.assertNotNull(r.getDynamicSql().get(1).getSqlSource(),
                "EXECUTE record's sqlSource must be resolved from the matching PREPARE, not left null");
    }
}
