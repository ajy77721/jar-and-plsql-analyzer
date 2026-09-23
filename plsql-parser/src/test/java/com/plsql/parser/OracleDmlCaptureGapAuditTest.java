package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Read-only audit (2026-09-16): probes 8 realistic Oracle DML capture patterns against the real
 * parser/visitor and PRINTS what was captured (tables, operations). No assertions are meant to
 * fail the build hard for exploratory cases; each test prints evidence and only asserts on the
 * few points explicitly requested. This is a diagnostic harness, not a permanent regression
 * suite — see report from the invoking task for pass/gap verdicts per pattern.
 */
public class OracleDmlCaptureGapAuditTest extends ParserTestBase {

    private static void dump(String label, SubprogramInfo sub) {
        System.out.println("=== " + label + " ===");
        if (sub == null) {
            System.out.println("  (subprogram not found)");
            return;
        }
        List<TableOperationInfo> ops = sub.getTableOperations();
        if (ops == null || ops.isEmpty()) {
            System.out.println("  (no table operations captured)");
        }
        for (TableOperationInfo t : ops) {
            System.out.println("  table=" + t.getTableName()
                    + " op=" + t.getOperation()
                    + " alias=" + t.getAlias()
                    + " subqueryContext=" + t.getSubqueryContext()
                    + " objectType=" + t.getObjectType());
        }
        if (sub.getCursors() != null) {
            for (CursorInfo c : sub.getCursors()) {
                System.out.println("  cursor=" + c.getName());
            }
        }
    }

    private Set<String> tableNames(SubprogramInfo sub) {
        return sub.getTableOperations().stream()
                .map(t -> t.getTableName() == null ? "" : t.getTableName().toUpperCase())
                .collect(Collectors.toSet());
    }

    // 1. Multi-table JOIN in plain SELECT + inside a CURSOR
    @Test
    @DisplayName("1: multi-table JOIN (plain SELECT) and inside a CURSOR")
    void multiTableJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_join IS
                    CURSOR cur_join IS
                      SELECT o.id, c.name, s.status, p.price
                      FROM orders o
                      INNER JOIN customers c ON c.id = o.customer_id
                      LEFT JOIN shipments s ON s.order_id = o.id
                      LEFT JOIN products p ON p.id = o.product_id;
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o
                    INNER JOIN customers c ON c.id = o.customer_id
                    LEFT JOIN shipments s ON s.order_id = o.id
                    LEFT JOIN products p ON p.id = o.product_id;
                  END proc_join;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "multi-table JOIN");
        SubprogramInfo sub = findSub(result, "PROC_JOIN");
        dump("1: multi-table JOIN", sub);
    }

    // 2. Subqueries: WHERE, SELECT-list scalar subquery, derived table
    @Test
    @DisplayName("2: subqueries (WHERE, scalar SELECT-list, derived table in FROM)")
    void subqueries() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_subq IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT o.id,
                           (SELECT MAX(p.price) FROM price_history p WHERE p.order_id = o.id) AS max_price
                    INTO v_cnt
                    FROM orders o,
                         (SELECT cust_id, region FROM customer_region_map) crm
                    WHERE o.customer_id IN (SELECT id FROM active_customers WHERE active = 'Y')
                      AND o.region = crm.region;
                  END proc_subq;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "subqueries");
        SubprogramInfo sub = findSub(result, "PROC_SUBQ");
        dump("2: subqueries", sub);
    }

    // 3. CTE where one CTE references another
    @Test
    @DisplayName("3: CTE chain, one referencing another, main query selects from CTE name")
    void cteChain() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_cte IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH base AS (
                      SELECT id, dept_id FROM employees WHERE active = 'Y'
                    ),
                    dept_summary AS (
                      SELECT b.dept_id, COUNT(*) cnt FROM base b JOIN departments d ON d.id = b.dept_id
                      GROUP BY b.dept_id
                    )
                    SELECT COUNT(*) INTO v_cnt FROM dept_summary;
                  END proc_cte;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CTE chain");
        SubprogramInfo sub = findSub(result, "PROC_CTE");
        dump("3: CTE chain", sub);
    }

    // 4. MERGE INTO ... USING ...
    @Test
    @DisplayName("4: MERGE INTO target USING source subquery")
    void mergeIntoUsing() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_merge IS
                  BEGIN
                    MERGE INTO account_balances t
                    USING (SELECT acct_id, delta FROM pending_transactions WHERE processed = 'N') s
                    ON (t.acct_id = s.acct_id)
                    WHEN MATCHED THEN UPDATE SET t.balance = t.balance + s.delta
                    WHEN NOT MATCHED THEN INSERT (acct_id, balance) VALUES (s.acct_id, s.delta);
                  END proc_merge;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "MERGE INTO USING");
        SubprogramInfo sub = findSub(result, "PROC_MERGE");
        dump("4: MERGE INTO USING", sub);
    }

    // 5. FORALL ... INSERT and BULK COLLECT INTO
    @Test
    @DisplayName("5: FORALL INSERT and BULK COLLECT INTO from SELECT")
    void bulkOps() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_bulk IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    v_ids t_ids;
                  BEGIN
                    SELECT id BULK COLLECT INTO v_ids FROM staging_orders WHERE status = 'NEW';

                    FORALL i IN 1 .. v_ids.COUNT
                      INSERT INTO processed_orders (id) VALUES (v_ids(i));
                  END proc_bulk;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "bulk ops");
        SubprogramInfo sub = findSub(result, "PROC_BULK");
        dump("5: FORALL/BULK COLLECT", sub);
    }

    // 6. Dynamic SQL with string concatenation
    @Test
    @DisplayName("6: EXECUTE IMMEDIATE with concatenated table-name fragment")
    void dynamicSqlConcatTableName() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_dyn (v_table_prefix VARCHAR2) IS
                    v_cnt NUMBER;
                  BEGIN
                    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table_prefix || '_orders' INTO v_cnt;
                  END proc_dyn;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "dynamic SQL concat table name");
        SubprogramInfo sub = findSub(result, "PROC_DYN");
        dump("6: dynamic SQL concat (table name itself built dynamically)", sub);

        // Bug 1 fix (§DML-audit): must NOT fabricate a bogus literal table name ("_ORDERS") by
        // bridging the space left by the interrupting variable; must instead record it as
        // unresolvable dynamic SQL.
        Set<String> names = tableNames(sub);
        org.junit.jupiter.api.Assertions.assertFalse(names.contains("_ORDERS"),
                "must not fabricate a table name from the literal fragment adjacent to a variable");
        org.junit.jupiter.api.Assertions.assertTrue(names.contains("UNRESOLVABLE_DYNAMIC"),
                "must mark the variable-built table name as unresolvable dynamic SQL");
        boolean sawDynamic = sub.getTableOperations().stream()
                .anyMatch(t -> "UNRESOLVABLE_DYNAMIC".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation()));
        org.junit.jupiter.api.Assertions.assertTrue(sawDynamic, "unresolvable entry must carry op=DYNAMIC");
    }

    // 6b. Fully-literal dynamic SQL (no variables at all) must still resolve the real table name.
    @Test
    @DisplayName("6b: EXECUTE IMMEDIATE with fully-literal SQL still resolves the table name")
    void dynamicSqlFullyLiteral() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_dyn_lit IS
                    v_cnt NUMBER;
                  BEGIN
                    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM real_orders' INTO v_cnt;
                  END proc_dyn_lit;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "dynamic SQL fully literal");
        SubprogramInfo sub = findSub(result, "PROC_DYN_LIT");
        dump("6b: dynamic SQL fully literal", sub);
        Set<String> names = tableNames(sub);
        org.junit.jupiter.api.Assertions.assertTrue(names.contains("REAL_ORDERS"),
                "fully-literal dynamic SQL must still resolve the real table name");
        org.junit.jupiter.api.Assertions.assertFalse(names.contains("UNRESOLVABLE_DYNAMIC"),
                "fully-literal dynamic SQL must not be marked unresolvable");
    }

    // 7. Self-join
    @Test
    @DisplayName("7: self-join FROM orders o1 JOIN orders o2")
    void selfJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_self_join IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o1
                    JOIN orders o2 ON o2.customer_id = o1.customer_id AND o2.id <> o1.id;
                  END proc_self_join;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "self-join");
        SubprogramInfo sub = findSub(result, "PROC_SELF_JOIN");
        dump("7: self-join", sub);
        if (sub != null) {
            Set<String> names = tableNames(sub);
            System.out.println("  distinct table names: " + names);
        }
    }

    // 8. Plain name that could be a view
    @Test
    @DisplayName("8: SELECT from a plain name that could be a view (no syntactic distinction)")
    void viewLikeReference() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_view_like IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM active_customer_view WHERE region = 'EU';
                  END proc_view_like;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "view-like reference");
        SubprogramInfo sub = findSub(result, "PROC_VIEW_LIKE");
        dump("8: view-like reference", sub);
    }
}
