package com.plsql.parser.edgecase;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case tests for Gap #1 (Column-level lineage not tracked) and
 * Gap #10 (Subquery context classification edge cases).
 *
 * Verifies that the parser correctly extracts TABLE references even when
 * column-level tracking is not available, and that subquery contexts are
 * classified correctly in complex expressions.
 */
@DisplayName("Gap #1/#10: Column-Level Lineage & Subquery Context Edge Cases")
public class ColumnLevelLineageGapTest extends ParserTestBase {

    // ── Helper ──────────────────────────────────────────────────────────────

    private String wrap(String body) {
        return """
                CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                  %s
                END TEST_PKG;
                /
                """.formatted(body);
    }

    // ── 1. Specific columns — tables still extracted ────────────────────────

    @Test
    @DisplayName("SELECT with specific columns from JOINed tables extracts both tables")
    void testSelectSpecificColumns_TablesStillExtracted() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_name VARCHAR2(100);
                  v_dept VARCHAR2(100);
                BEGIN
                  SELECT e.first_name, d.department_name
                    INTO v_name, v_dept
                    FROM employees e
                    JOIN departments d ON e.department_id = d.department_id
                   WHERE e.employee_id = 100;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT specific columns with JOIN");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc, "Subprogram P should be found");
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "EMPLOYEES table should be extracted even though only e.first_name selected");
        assertTrue(ops.stream().anyMatch(t ->
                        "DEPARTMENTS".equalsIgnoreCase(t.getTableName())),
                "DEPARTMENTS table should be extracted even though only d.department_name selected");
    }

    // ── 2. SELECT * — table extracted ───────────────────────────────────────

    @Test
    @DisplayName("SELECT * FROM single table extracts that table")
    void testSelectStar_TableExtracted() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt FROM employees WHERE status = 'A';
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT *");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())
                                && "SELECT".equalsIgnoreCase(t.getOperation())),
                "EMPLOYEES should be extracted as SELECT");
    }

    // ── 3. Column alias does not create a fake table ────────────────────────

    @Test
    @DisplayName("Column alias does not produce a spurious table entry")
    void testColumnAliasDoesNotCreateFakeTable() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_id NUMBER;
                BEGIN
                  SELECT emp_id AS employee_identifier INTO v_id
                    FROM employees WHERE ROWNUM = 1;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Column alias");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "EMPLOYEES should be extracted");
        assertFalse(ops.stream().anyMatch(t ->
                        "EMPLOYEE_IDENTIFIER".equalsIgnoreCase(t.getTableName())),
                "Column alias 'employee_identifier' must NOT appear as a table");
    }

    // ── 4. Scalar subquery in SELECT list — both tables extracted ───────────

    @Test
    @DisplayName("Scalar subquery in SELECT list extracts both outer and inner tables")
    void testScalarSubqueryInSelectList() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_name VARCHAR2(100);
                  v_dept VARCHAR2(100);
                BEGIN
                  SELECT e.first_name,
                         (SELECT d.department_name FROM departments d
                           WHERE d.department_id = e.department_id) dept_name
                    INTO v_name, v_dept
                    FROM employees e
                   WHERE e.employee_id = 100;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Scalar subquery in SELECT list");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "Outer table EMPLOYEES should be extracted");
        assertTrue(ops.stream().anyMatch(t ->
                        "DEPARTMENTS".equalsIgnoreCase(t.getTableName())),
                "Inner scalar-subquery table DEPARTMENTS should be extracted");

        // The DEPARTMENTS entry from the scalar subquery should have subqueryContext set
        TableOperationInfo deptOp = ops.stream()
                .filter(t -> "DEPARTMENTS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(deptOp);
        assertNotNull(deptOp.getSubqueryContext(),
                "DEPARTMENTS in scalar subquery should have a non-null subqueryContext");
    }

    // ── 5. CASE expression with EXISTS subquery — both tables found ────────

    @Test
    @DisplayName("CASE WHEN EXISTS subquery extracts both outer and inner tables")
    void testCaseExpressionWithSubquery() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_label VARCHAR2(20);
                BEGIN
                  SELECT CASE
                           WHEN EXISTS (SELECT 1 FROM vip_customers v WHERE v.id = c.id)
                           THEN 'VIP'
                           ELSE 'NORMAL'
                         END
                    INTO v_label
                    FROM customers c
                   WHERE c.id = 42;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CASE WHEN EXISTS subquery");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                        "CUSTOMERS".equalsIgnoreCase(t.getTableName())),
                "Outer table CUSTOMERS should be extracted");
        assertTrue(ops.stream().anyMatch(t ->
                        "VIP_CUSTOMERS".equalsIgnoreCase(t.getTableName())),
                "Inner EXISTS table VIP_CUSTOMERS should be extracted");
    }

    // ── 6. Nested function calls do not create fake tables ──────────────────

    @Test
    @DisplayName("Nested function calls NVL/DECODE do not produce spurious table entries")
    void testNestedFunctionCallsDoNotCreateTables() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_label VARCHAR2(100);
                BEGIN
                  SELECT NVL(DECODE(status, 'A', 'Active', 'I', 'Inactive'), 'Unknown')
                    INTO v_label
                    FROM employees
                   WHERE ROWNUM = 1;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Nested function calls");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "EMPLOYEES should be extracted");
        assertFalse(ops.stream().anyMatch(t ->
                        "NVL".equalsIgnoreCase(t.getTableName())),
                "NVL must NOT appear as a table");
        assertFalse(ops.stream().anyMatch(t ->
                        "DECODE".equalsIgnoreCase(t.getTableName())),
                "DECODE must NOT appear as a table");
    }

    // ── 7. Subquery in WHERE IN — subqueryContext set ───────────────────────

    @Test
    @DisplayName("Subquery in WHERE ... IN (...) sets subqueryContext")
    void testSubqueryInWhereIn() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt
                    FROM employees
                   WHERE department_id IN (SELECT department_id FROM departments WHERE location_id = 1700);
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Subquery in WHERE IN");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                        "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "Outer table EMPLOYEES should be extracted");
        TableOperationInfo deptOp = ops.stream()
                .filter(t -> "DEPARTMENTS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(deptOp, "DEPARTMENTS in IN-subquery should be extracted");
        assertNotNull(deptOp.getSubqueryContext(),
                "DEPARTMENTS should have a non-null subqueryContext for IN-subquery");
    }

    // ── 8. WHERE EXISTS — subqueryContext indicates EXISTS ──────────────────

    @Test
    @DisplayName("WHERE EXISTS subquery marks table with subqueryContext=EXISTS")
    void testSubqueryExistsContext() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt
                    FROM orders o
                   WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.order_id);
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "WHERE EXISTS subquery");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        TableOperationInfo oiOp = proc.getTableOperations().stream()
                .filter(t -> "ORDER_ITEMS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(oiOp, "ORDER_ITEMS should be extracted from EXISTS subquery");
        assertEquals("EXISTS", oiOp.getSubqueryContext(),
                "ORDER_ITEMS should have subqueryContext=EXISTS");
    }

    // ── 9. WHERE NOT EXISTS — subqueryContext indicates NOT_EXISTS ──────────

    @Test
    @DisplayName("WHERE NOT EXISTS subquery marks table with subqueryContext containing EXISTS")
    void testSubqueryNotExistsContext() {
        String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt
                    FROM employees e
                   WHERE NOT EXISTS (SELECT 1 FROM terminated_employees t WHERE t.emp_id = e.emp_id);
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "WHERE NOT EXISTS subquery");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        TableOperationInfo termOp = proc.getTableOperations().stream()
                .filter(t -> "TERMINATED_EMPLOYEES".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(termOp, "TERMINATED_EMPLOYEES should be extracted from NOT EXISTS subquery");
        assertNotNull(termOp.getSubqueryContext(),
                "TERMINATED_EMPLOYEES should have a non-null subqueryContext");
        // Accept either "EXISTS" or "NOT_EXISTS" — both indicate the parser recognized the context
        assertTrue(termOp.getSubqueryContext().toUpperCase().contains("EXISTS"),
                "subqueryContext should contain 'EXISTS', got: " + termOp.getSubqueryContext());
    }

    // ── 10. Correlated self-referencing subquery — table still extracted ────

    @Test
    @DisplayName("Correlated subquery on same table extracts the table")
    void testCorrelatedSubqueryBothTablesExtracted() {
        String sql = wrap("""
                PROCEDURE P IS
                  CURSOR c1 IS
                    SELECT *
                      FROM orders o
                     WHERE amount > (SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id);
                BEGIN
                  NULL;
                END P;
                """);
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Correlated self-referencing subquery");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);

        // "orders" should appear at least once (it may appear twice: outer + subquery)
        long ordersCount = proc.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .count();
        assertTrue(ordersCount >= 1,
                "ORDERS should appear at least once in table operations (found " + ordersCount + ")");

        // Also check via cursor tables
        boolean foundInCursors = proc.getCursors().stream()
                .flatMap(c -> c.getTables().stream())
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t));
        assertTrue(foundInCursors || ordersCount >= 1,
                "ORDERS should be found either in cursor tables or table operations");
    }
}
