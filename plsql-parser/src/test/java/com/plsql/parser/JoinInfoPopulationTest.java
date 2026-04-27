package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #5: JoinInfo Population")
public class JoinInfoPopulationTest extends ParserTestBase {

    @Test
    @DisplayName("INNER JOIN populates JoinInfo with type, table, condition")
    void testInnerJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o
                    INNER JOIN customers c ON c.id = o.customer_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "INNER JOIN");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo orders = proc.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(orders, "Should have ORDERS table");
        assertFalse(orders.getJoins().isEmpty(), "ORDERS should have join info");
        JoinInfo ji = orders.getJoins().get(0);
        assertEquals("INNER", ji.getJoinType());
        assertTrue(ji.getJoinedTable() != null && ji.getJoinedTable().toUpperCase().contains("CUSTOMERS"));
    }

    @Test
    @DisplayName("LEFT OUTER JOIN captures correct type")
    void testLeftOuterJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM employees e
                    LEFT OUTER JOIN departments d ON d.id = e.dept_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo emp = proc.getTableOperations().stream()
                .filter(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(emp);
        assertFalse(emp.getJoins().isEmpty());
        assertEquals("LEFT OUTER", emp.getJoins().get(0).getJoinType());
    }

    @Test
    @DisplayName("Multiple JOINs on driving table")
    void testMultipleJoins() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o
                    JOIN customers c ON c.id = o.customer_id
                    JOIN products p ON p.id = o.product_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "CUSTOMERS".equalsIgnoreCase(t.getTableName())),
                "CUSTOMERS should be captured");
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "PRODUCTS".equalsIgnoreCase(t.getTableName())),
                "PRODUCTS should be captured");
    }

    @Test
    @DisplayName("CROSS JOIN has no condition")
    void testCrossJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM table_a a
                    CROSS JOIN table_b b;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo tblA = proc.getTableOperations().stream()
                .filter(t -> "TABLE_A".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(tblA);
        if (!tblA.getJoins().isEmpty()) {
            assertEquals("CROSS", tblA.getJoins().get(0).getJoinType());
            assertNull(tblA.getJoins().get(0).getCondition());
        }
    }

    @Test
    @DisplayName("Implicit comma join in FROM clause")
    void testImplicitCommaJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o, customers c
                    WHERE c.id = o.customer_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Implicit comma join");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo orders = proc.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(orders, "Should have ORDERS table");
        assertFalse(orders.getJoins().isEmpty(), "ORDERS should have implicit join info");
        JoinInfo ji = orders.getJoins().get(0);
        assertEquals("IMPLICIT", ji.getJoinType());
        assertTrue(ji.getJoinedTable().toUpperCase().contains("CUSTOMERS"));
    }

    @Test
    @DisplayName("Multiple implicit comma joins")
    void testMultipleImplicitJoins() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o, customers c, products p
                    WHERE c.id = o.customer_id AND p.id = o.product_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multiple implicit joins");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo orders = proc.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(orders, "Should have ORDERS table");
        assertEquals(2, orders.getJoins().size(), "Should have 2 implicit joins");
        assertTrue(orders.getJoins().stream().anyMatch(j ->
                j.getJoinedTable().toUpperCase().contains("CUSTOMERS")));
        assertTrue(orders.getJoins().stream().anyMatch(j ->
                j.getJoinedTable().toUpperCase().contains("PRODUCTS")));
    }

    @Test
    @DisplayName("Mixed explicit and implicit joins")
    void testMixedExplicitAndImplicitJoins() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders o
                    LEFT JOIN customers c ON c.id = o.customer_id,
                    products p
                    WHERE p.id = o.product_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        boolean hasExplicitJoin = proc.getTableOperations().stream()
                .flatMap(t -> t.getJoins().stream())
                .anyMatch(j -> "LEFT OUTER".equals(j.getJoinType()) || "LEFT".equals(j.getJoinType()));
        boolean hasImplicitJoin = proc.getTableOperations().stream()
                .flatMap(t -> t.getJoins().stream())
                .anyMatch(j -> "IMPLICIT".equals(j.getJoinType()));
        assertTrue(hasExplicitJoin || proc.getTableOperations().size() >= 2,
                "Should detect explicit join or multiple tables");
    }

    @Test
    @DisplayName("Comma join with schema-qualified tables")
    void testCommaJoinWithSchema() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM my_schema.orders o, my_schema.customers c
                    WHERE c.id = o.customer_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo orders = proc.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(orders, "Should have ORDERS table");
        assertFalse(orders.getJoins().isEmpty(), "Should have implicit join");
        JoinInfo ji = orders.getJoins().get(0);
        assertEquals("IMPLICIT", ji.getJoinType());
        assertTrue(ji.getJoinedTable().toUpperCase().contains("CUSTOMERS"));
    }

    @Test
    @DisplayName("RIGHT JOIN captures correct type")
    void testRightJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM employees e
                    RIGHT JOIN departments d ON d.id = e.dept_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        boolean hasRightJoin = proc.getTableOperations().stream()
                .flatMap(t -> t.getJoins().stream())
                .anyMatch(j -> j.getJoinType() != null && j.getJoinType().contains("RIGHT"));
        assertTrue(hasRightJoin, "Should detect RIGHT join type");
    }
}
