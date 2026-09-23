package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Complex SQL: CTE, UNION, Subqueries, JOINs, CONNECT BY, DECODE")
public class ComplexSqlPatternTest extends ParserTestBase {

    @Test
    @DisplayName("Nested subqueries in FROM clause")
    void testNestedSubqueries() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_total NUMBER;
                  BEGIN
                    SELECT total INTO v_total
                      FROM (SELECT SUM(amount) AS total
                              FROM (SELECT amount FROM orders WHERE status = 'PAID'));
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Nested subqueries");
    }

    @Test
    @DisplayName("CASE expression in SELECT")
    void testCaseExpression() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_label VARCHAR2(10);
                  BEGIN
                    SELECT CASE WHEN amount > 1000 THEN 'HIGH'
                                WHEN amount > 100 THEN 'MED'
                                ELSE 'LOW' END
                      INTO v_label
                      FROM orders WHERE id = 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CASE in SELECT");
    }

    @Test
    @DisplayName("WITH clause (CTE) in SELECT INTO")
    void testWithClauseCTE() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_total NUMBER;
                  BEGIN
                    WITH active_emp AS (
                      SELECT id, salary FROM employees WHERE status = 'ACTIVE'
                    ),
                    dept_totals AS (
                      SELECT dept_id, SUM(salary) AS total
                        FROM active_emp e JOIN departments d ON e.id = d.emp_id
                       GROUP BY dept_id
                    )
                    SELECT SUM(total) INTO v_total FROM dept_totals;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "WITH clause CTE");
    }

    @Test
    @DisplayName("UNION ALL in subquery")
    void testUnionAll() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                      FROM (SELECT id FROM active_orders
                            UNION ALL
                            SELECT id FROM archived_orders);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "UNION ALL");
    }

    @Test
    @DisplayName("CONNECT BY / START WITH hierarchical query")
    void testConnectByStartWith() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR rec IN (
                      SELECT LEVEL, emp_id, mgr_id
                        FROM org_chart
                       START WITH mgr_id IS NULL
                     CONNECT BY PRIOR emp_id = mgr_id
                       ORDER SIBLINGS BY emp_id
                    ) LOOP
                      DBMS_OUTPUT.PUT_LINE(rec.emp_id);
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CONNECT BY START WITH");
    }

    @Test
    @DisplayName("EXISTS and NOT IN subquery patterns")
    void testExistsAndNotInSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                      FROM orders o
                     WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id)
                       AND o.status NOT IN (SELECT code FROM inactive_statuses)
                       AND o.customer_id IN (SELECT id FROM active_customers);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXISTS + NOT IN subqueries");
    }

    @Test
    @DisplayName("DECODE and NVL in SELECT")
    void testDecodeAndNvl() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_val VARCHAR2(10);
                  BEGIN
                    SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown'),
                           NVL(description, 'N/A'),
                           NVL2(phone, 'Has Phone', 'No Phone'),
                           COALESCE(email, alt_email, 'none')
                      INTO v_val, v_val, v_val, v_val
                      FROM contacts WHERE id = 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "DECODE/NVL/COALESCE");
    }

    @Test
    @DisplayName("GROUP BY with HAVING clause")
    void testGroupByHaving() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_dept NUMBER;
                  BEGIN
                    SELECT dept_id INTO v_dept
                      FROM employees
                     WHERE status = 'A'
                     GROUP BY dept_id
                    HAVING COUNT(*) > 5
                     ORDER BY dept_id
                     FETCH FIRST 1 ROWS ONLY;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "GROUP BY HAVING");
    }

    @Test
    @DisplayName("Multi-table LEFT/RIGHT/FULL OUTER JOIN")
    void testMultiTableJoins() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_name VARCHAR2(100);
                  BEGIN
                    SELECT e.name INTO v_name
                      FROM employees e
                      LEFT OUTER JOIN departments d ON e.dept_id = d.id
                     RIGHT JOIN locations l ON d.loc_id = l.id
                      FULL OUTER JOIN regions r ON l.region_id = r.id
                     INNER JOIN countries c ON r.country_id = c.id
                     WHERE e.id = 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multi-table JOINs");
    }
}
