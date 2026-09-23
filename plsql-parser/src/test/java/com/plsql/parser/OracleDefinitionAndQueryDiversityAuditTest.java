package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * READ-ONLY AUDIT (2026-09-17): probes procedure/function DEFINITION style diversity
 * (overloads, complex default expressions, single-line vs multi-line bodies, forward
 * declarations, param-less parens, table-functions in FROM) and further query-writing
 * diversity (alias styles, qualification depth, set operators across differing tables,
 * a large realistic query, semicolon-optional bodies, dynamic-SQL table names built via
 * CASE) for Oracle PL/SQL — mirroring OracleDmlCaptureGapAuditTest / OracleCommentStyleAuditTest.
 * Prints evidence; only asserts on high-confidence points.
 */
public class OracleDefinitionAndQueryDiversityAuditTest extends ParserTestBase {

    private static void dumpSub(String label, SubprogramInfo sub) {
        System.out.println("=== " + label + " ===");
        if (sub == null) {
            System.out.println("  (subprogram not found)");
            return;
        }
        System.out.println("  name=" + sub.getName() + " type=" + sub.getType()
                + " lineStart=" + sub.getLineStart() + " lineEnd=" + sub.getLineEnd()
                + " forwardDeclaration=" + sub.isForwardDeclaration()
                + " pipelined=" + sub.isPipelined());
        for (ParameterInfo p : sub.getParameters()) {
            System.out.println("  param name=" + p.getName() + " type=" + p.getDataType()
                    + " dir=" + p.getDirection() + " default=[" + p.getDefaultValue() + "]");
        }
        for (TableOperationInfo t : sub.getTableOperations()) {
            System.out.println("  table=" + t.getTableName() + " op=" + t.getOperation()
                    + " alias=" + t.getAlias() + " objectType=" + t.getObjectType());
        }
        for (CallInfo c : sub.getCalls()) {
            System.out.println("  call=" + c.getPackageName() + "." + c.getName() + " line=" + c.getLine());
        }
        if (sub.getDynamicSql() != null) {
            for (DynamicSqlInfo d : sub.getDynamicSql()) {
                System.out.println("  dynamicSql=[" + d.getSqlExpression() + "]");
            }
        }
    }

    private List<SubprogramInfo> findAll(ParseResult result, String name) {
        return result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .filter(s -> s.getName() != null && s.getName().equalsIgnoreCase(name))
                .collect(Collectors.toList());
    }

    private Set<String> tableNames(SubprogramInfo sub) {
        return sub.getTableOperations().stream()
                .map(t -> t.getTableName() == null ? "" : t.getTableName().toUpperCase())
                .collect(Collectors.toSet());
    }

    // ── A1: overloaded procedures in one package ──────────────────────────────
    @Test
    @DisplayName("A1: overloaded procedures same name different signature")
    void overloadedProcedures() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE do_work (p_id NUMBER) IS
                  BEGIN
                    UPDATE orders SET status = 'DONE' WHERE id = p_id;
                  END do_work;

                  PROCEDURE do_work (p_id NUMBER, p_reason VARCHAR2) IS
                  BEGIN
                    UPDATE orders SET status = 'DONE', reason = p_reason WHERE id = p_id;
                    INSERT INTO order_audit (id, reason) VALUES (p_id, p_reason);
                  END do_work;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "overloaded procedures");
        List<SubprogramInfo> subs = findAll(result, "DO_WORK");
        System.out.println("=== A1: overloads, count found = " + subs.size() + " ===");
        for (SubprogramInfo s : subs) dumpSub("A1 overload variant", s);
    }

    // ── A2: default parameter with complex expressions ─────────────────────────
    @Test
    @DisplayName("A2: default params with complex expressions (TRUNC/CASE)")
    void complexDefaultParams() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_defaults (
                    p_date IN DATE DEFAULT TRUNC(SYSDATE) - 30,
                    p_status IN VARCHAR2 DEFAULT CASE WHEN 1 = 1 THEN 'ACTIVE' ELSE 'INACTIVE' END
                  ) IS
                  BEGIN
                    UPDATE orders SET status = p_status WHERE order_date >= p_date;
                  END proc_defaults;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "complex default params");
        SubprogramInfo sub = findSub(result, "PROC_DEFAULTS");
        dumpSub("A2: complex default params", sub);
    }

    // ── A3: single-line body vs multi-line body, same logic ────────────────────
    @Test
    @DisplayName("A3: single-line body vs 20-line spread body")
    void singleLineVsMultiLineBody() {
        String sqlOneLine = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_oneline (p_id NUMBER) IS BEGIN UPDATE orders SET status = 'X' WHERE id = p_id; INSERT INTO order_audit(id) VALUES (p_id); END proc_oneline;
                END test_pkg;
                /
                """;
        String sqlMultiLine = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_multiline (
                    p_id NUMBER
                  )
                  IS
                  BEGIN

                    UPDATE orders
                       SET status = 'X'
                     WHERE id = p_id;

                    INSERT INTO order_audit
                      (id)
                    VALUES
                      (p_id);

                  END proc_multiline;
                END test_pkg;
                /
                """;
        ParseResult r1 = parse(sqlOneLine);
        ParseResult r2 = parse(sqlMultiLine);
        assertNoParsErrors(r1, "single-line body");
        assertNoParsErrors(r2, "multi-line body");
        SubprogramInfo s1 = findSub(r1, "PROC_ONELINE");
        SubprogramInfo s2 = findSub(r2, "PROC_MULTILINE");
        dumpSub("A3: single-line body", s1);
        dumpSub("A3: multi-line body", s2);
        System.out.println("  single-line table set=" + (s1 == null ? null : tableNames(s1)));
        System.out.println("  multi-line table set=" + (s2 == null ? null : tableNames(s2)));
    }

    // ── A4: forward declaration in spec, body implements later with different param name/default ──
    @Test
    @DisplayName("A4: forward declaration (spec) vs body implementation with different param name/default")
    void forwardDeclaration() {
        String sql = """
                CREATE OR REPLACE PACKAGE test_pkg AS
                  PROCEDURE proc_fwd (p_limit IN NUMBER DEFAULT 10);
                END test_pkg;
                /
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_fwd (p_max_rows IN NUMBER DEFAULT 20) IS
                  BEGIN
                    UPDATE orders SET flagged = 'Y' WHERE ROWNUM <= p_max_rows;
                  END proc_fwd;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "forward declaration");
        List<SubprogramInfo> subs = findAll(result, "PROC_FWD");
        System.out.println("=== A4: forward decl, count found = " + subs.size() + " ===");
        for (SubprogramInfo s : subs) dumpSub("A4 variant", s);
    }

    // ── A5: no-params, without vs with empty parens ─────────────────────────────
    @Test
    @DisplayName("A5: no-parameter procedure without vs with empty parens")
    void noParamsParenVariants() {
        String sqlNoParens = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_noparen IS
                  BEGIN
                    UPDATE orders SET status = 'SWEEP';
                  END proc_noparen;
                END test_pkg;
                /
                """;
        String sqlEmptyParens = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_emptyparen () IS
                  BEGIN
                    UPDATE orders SET status = 'SWEEP';
                  END proc_emptyparen;
                END test_pkg;
                /
                """;
        ParseResult r1 = parse(sqlNoParens);
        ParseResult r2 = parse(sqlEmptyParens);
        System.out.println("=== A5: no-parens errors=" + r1.getErrors() + " ===");
        System.out.println("=== A5: empty-parens errors=" + r2.getErrors() + " ===");
        dumpSub("A5: no parens", findSub(r1, "PROC_NOPAREN"));
        dumpSub("A5: empty parens", findSub(r2, "PROC_EMPTYPAREN"));
        // REGRESSION (fixed bug): `PROCEDURE name () IS ...` (empty, explicit parameter parens)
        // is legal Oracle PL/SQL syntax and must parse cleanly with the subprogram still found.
        assertNoParsErrors(r2, "empty parens procedure");
        SubprogramInfo emptyParenSub = findSub(r2, "PROC_EMPTYPAREN");
        org.junit.jupiter.api.Assertions.assertNotNull(emptyParenSub,
                "PROC_EMPTYPAREN must be found after fixing the empty-parens grammar gap");
        org.junit.jupiter.api.Assertions.assertTrue(emptyParenSub.getParameters().isEmpty());
    }

    // ── A6: pipelined table function used via TABLE() in FROM ──────────────────
    @Test
    @DisplayName("A6: pipelined function used as TABLE() in FROM clause")
    void tableFunctionInFrom() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  FUNCTION get_active_orders RETURN SYS.ODCINUMBERLIST PIPELINED IS
                  BEGIN
                    RETURN;
                  END get_active_orders;

                  PROCEDURE proc_use_tablefn IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM TABLE(get_active_orders());
                  END proc_use_tablefn;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "table function in FROM");
        SubprogramInfo fn = findSub(result, "GET_ACTIVE_ORDERS");
        SubprogramInfo proc = findSub(result, "PROC_USE_TABLEFN");
        dumpSub("A6: pipelined function decl", fn);
        dumpSub("A6: proc calling TABLE(fn())", proc);
    }

    // ── B7: table alias with vs without AS ──────────────────────────────────────
    @Test
    @DisplayName("B7: table alias without AS vs with AS")
    void aliasWithWithoutAs() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_alias_mix IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o
                    JOIN customers AS c ON c.id = o.customer_id;
                  END proc_alias_mix;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "alias with/without AS");
        SubprogramInfo sub = findSub(result, "PROC_ALIAS_MIX");
        dumpSub("B7: alias with/without AS", sub);
    }

    // ── B8: column ref qualification depth (schema.table.column vs table.column vs bare) ──
    @Test
    @DisplayName("B8: 3-part vs 2-part vs bare column qualification")
    void columnQualificationDepth() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_qual IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM app_schema.orders o
                    WHERE app_schema.orders.status = 'OPEN'
                      AND o.region = 'US'
                      AND status <> 'CANCELLED';
                  END proc_qual;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "column qualification depth");
        SubprogramInfo sub = findSub(result, "PROC_QUAL");
        dumpSub("B8: 3-part/2-part/bare column qualification", sub);
    }

    // ── B9: UNION/UNION ALL/INTERSECT/MINUS with DIFFERENT tables per branch ────
    @Test
    @DisplayName("B9: set operators combining SELECTs with different tables per branch")
    void setOperatorsDifferentTables() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_setops IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT id FROM online_orders
                    UNION
                    SELECT id FROM phone_orders
                    UNION ALL
                    SELECT id FROM store_orders
                    INTERSECT
                    SELECT id FROM flagged_orders
                    MINUS
                    SELECT id FROM cancelled_orders;
                  END proc_setops;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "set operators different tables");
        SubprogramInfo sub = findSub(result, "PROC_SETOPS");
        dumpSub("B9: set operators, different tables per branch", sub);
        System.out.println("  distinct tables captured=" + tableNames(sub));
    }

    // ── B10: large realistic 30+ line query (joins, CTEs, window fn, CASE) ─────
    @Test
    @DisplayName("B10: large realistic query with joins+CTEs+window fn+CASE")
    void largeRealisticQuery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_large_query IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH region_totals AS (
                      SELECT o.region,
                             SUM(o.amount) AS total_amount
                      FROM orders o
                      JOIN customers c ON c.id = o.customer_id
                      WHERE o.status = 'CLOSED'
                      GROUP BY o.region
                    ),
                    ranked_customers AS (
                      SELECT c.id,
                             c.name,
                             c.region,
                             ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY c.lifetime_value DESC) AS rn
                      FROM customers c
                      JOIN region_totals rt ON rt.region = c.region
                    )
                    SELECT rc.id,
                           rc.name,
                           CASE
                             WHEN rc.rn = 1 THEN 'TOP'
                             WHEN rc.rn <= 5 THEN 'HIGH'
                             ELSE 'STANDARD'
                           END AS tier,
                           s.warehouse_code,
                           p.category
                    INTO v_cnt
                    FROM ranked_customers rc
                    LEFT JOIN shipments s ON s.customer_id = rc.id
                    LEFT JOIN order_items oi ON oi.customer_id = rc.id
                    LEFT JOIN products p ON p.id = oi.product_id
                    WHERE rc.rn <= 10
                      AND EXISTS (
                        SELECT 1 FROM loyalty_flags lf WHERE lf.customer_id = rc.id
                      )
                    ORDER BY rc.region, rc.rn;
                  END proc_large_query;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "large realistic query");
        SubprogramInfo sub = findSub(result, "PROC_LARGE_QUERY");
        dumpSub("B10: large realistic query", sub);
        System.out.println("  distinct tables captured=" + tableNames(sub));
    }

    // ── B11: semicolon-optional final statement before END ─────────────────────
    @Test
    @DisplayName("B11: final statement missing trailing semicolon vs having it")
    void semicolonOptionalFinalStatement() {
        String sqlWithSemi = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_semi_yes IS
                  BEGIN
                    UPDATE orders SET status = 'X' WHERE id = 1;
                  END proc_semi_yes;
                END test_pkg;
                /
                """;
        // Oracle PL/SQL grammar requires a semicolon terminator on statements; a missing
        // semicolon before END is NOT legal Oracle syntax (unlike some other dialects/tools).
        // We still probe it to document actual parser behavior (error vs silent recovery).
        String sqlNoSemi = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_semi_no IS
                  BEGIN
                    UPDATE orders SET status = 'X' WHERE id = 1
                  END proc_semi_no;
                END test_pkg;
                /
                """;
        ParseResult r1 = parse(sqlWithSemi);
        ParseResult r2 = parse(sqlNoSemi);
        System.out.println("=== B11: with semicolon errors=" + r1.getErrors() + " ===");
        System.out.println("=== B11: without semicolon errors=" + r2.getErrors() + " ===");
        dumpSub("B11: with semicolon", findSub(r1, "PROC_SEMI_YES"));
        dumpSub("B11: without semicolon", findSub(r2, "PROC_SEMI_NO"));
    }

    // ── B12: CASE expression building a dynamic SQL table name ─────────────────
    @Test
    @DisplayName("B12: CASE expression inside dynamic SQL table-name construction")
    void caseBuiltDynamicTableName() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_case_dyn (v_region VARCHAR2) IS
                    v_cnt NUMBER;
                  BEGIN
                    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' ||
                      CASE WHEN v_region = 'US' THEN 'us_orders' ELSE 'intl_orders' END
                      INTO v_cnt;
                  END proc_case_dyn;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CASE-built dynamic table name");
        SubprogramInfo sub = findSub(result, "PROC_CASE_DYN");
        dumpSub("B12: CASE-built dynamic SQL table name", sub);
        Set<String> names = tableNames(sub);
        System.out.println("  captured table names=" + names);
        System.out.println("  contains US_ORDERS literal (would be wrong, region unresolved)? "
                + names.contains("US_ORDERS"));
        System.out.println("  contains UNRESOLVABLE_DYNAMIC marker? "
                + names.contains("UNRESOLVABLE_DYNAMIC"));
    }
}
