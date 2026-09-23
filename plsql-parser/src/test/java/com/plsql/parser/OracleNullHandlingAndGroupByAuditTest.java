package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT (read-only, no fixes): Oracle NULL-handling functions (NVL/COALESCE) interacting
 * with dynamic SQL and CASE expressions, plus GROUP BY ROLLUP/CUBE/GROUPING SETS extensions.
 *
 * This test documents actual observed behavior of the current grammar/visitor. Some assertions
 * are EXPECTED TO FAIL if a bug exists — that failure is the audit evidence, do not "fix" it here.
 */
@DisplayName("Oracle Audit: NVL/COALESCE + Dynamic SQL/CASE, and GROUP BY ROLLUP/CUBE/GROUPING SETS")
public class OracleNullHandlingAndGroupByAuditTest extends ParserTestBase {

    // ---------- 1(a) NVL/COALESCE embedded inside EXECUTE IMMEDIATE dynamic SQL string ----------

    @Test
    @DisplayName("1a. EXECUTE IMMEDIATE with NVL(...) inside dynamic SQL string built via concatenation")
    void testNvlInsideDynamicSqlConcatenation() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_id NUMBER) IS
                    v_sql VARCHAR2(4000);
                  BEGIN
                    v_sql := 'SELECT NVL(status, ''X'') FROM orders WHERE id = ' || p_id;
                    EXECUTE IMMEDIATE v_sql;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE with NVL in dynamic SQL");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 1a: TableOperations found in PROC (NVL in dynamic SQL via variable) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        // NOTE: v_sql is a variable, not a literal passed directly to EXECUTE IMMEDIATE,
        // so per existing convention (see DynamicSqlTableExtractionTest#testExecuteImmediateWithConcatenation)
        // extraction of variable-held dynamic SQL is not guaranteed. This assertion documents whether
        // the analyzer follows the variable assignment back to the literal concatenation.
        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        assertTrue(foundOrders, "Should extract ORDERS from dynamic SQL string despite embedded NVL(...) call");
    }

    @Test
    @DisplayName("1a-direct. EXECUTE IMMEDIATE literal string with NVL(...) (no intermediate variable)")
    void testNvlInsideDynamicSqlLiteralDirect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'SELECT NVL(status, ''X'') FROM orders WHERE id = 1';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE literal with NVL");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 1a-direct: TableOperations found in PROC (NVL in dynamic SQL literal) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation()));
        assertTrue(foundOrders, "Should extract ORDERS as DYNAMIC table op from literal EXECUTE IMMEDIATE containing NVL(...)");
    }

    @Test
    @DisplayName("1a-coalesce. EXECUTE IMMEDIATE literal string with COALESCE(...)")
    void testCoalesceInsideDynamicSqlLiteral() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'SELECT COALESCE(status, ''X'') FROM orders WHERE id = 1';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE literal with COALESCE");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 1a-coalesce: TableOperations found in PROC (COALESCE in dynamic SQL literal) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation()));
        assertTrue(foundOrders, "Should extract ORDERS as DYNAMIC table op from literal EXECUTE IMMEDIATE containing COALESCE(...)");
    }

    // ---------- 1(b) NVL/COALESCE nested inside CASE WHEN in a REAL (non-dynamic) query ----------

    @Test
    @DisplayName("1b. NVL(a,b) inside CASE WHEN in real SELECT ... FROM WHERE — known bug pattern hunt")
    void testNvlInsideCaseWhenRealQuery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM orders
                    WHERE CASE WHEN NVL(a, b) = 1 THEN 1 ELSE 0 END = 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "NVL inside CASE WHEN in real query");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 1b: TableOperations found in PROC (NVL inside CASE WHEN, real query) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        assertTrue(foundOrders, "Should capture ORDERS table even with NVL(a,b) nested inside CASE WHEN in WHERE clause");
    }

    @Test
    @DisplayName("1b-coalesce. COALESCE(a,b) inside CASE WHEN in real cursor SELECT")
    void testCoalesceInsideCaseWhenRealQuery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS
                      SELECT *
                      FROM orders
                      WHERE CASE WHEN COALESCE(a, b) = 1 THEN 1 ELSE 0 END = 1;
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "COALESCE inside CASE WHEN in real cursor query");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 1b-coalesce: TableOperations found in PROC (COALESCE inside CASE WHEN, cursor) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        assertTrue(foundOrders, "Should capture ORDERS table even with COALESCE(a,b) nested inside CASE WHEN in cursor WHERE clause");
    }

    // ---------- 2. GROUP BY ROLLUP / CUBE / GROUPING SETS ----------

    @Test
    @DisplayName("2a. GROUP BY ROLLUP(a,b) still captures FROM-clause table")
    void testGroupByRollup() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS
                      SELECT a, b, SUM(c)
                      FROM orders
                      GROUP BY ROLLUP(a, b);
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "GROUP BY ROLLUP");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 2a: TableOperations found in PROC (GROUP BY ROLLUP) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        assertTrue(foundOrders, "Should capture ORDERS table with GROUP BY ROLLUP(a,b)");
    }

    @Test
    @DisplayName("2b. GROUP BY CUBE(a,b) still captures FROM-clause table")
    void testGroupByCube() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS
                      SELECT a, b, SUM(c)
                      FROM orders
                      GROUP BY CUBE(a, b);
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "GROUP BY CUBE");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 2b: TableOperations found in PROC (GROUP BY CUBE) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        assertTrue(foundOrders, "Should capture ORDERS table with GROUP BY CUBE(a,b)");
    }

    @Test
    @DisplayName("2c. GROUP BY GROUPING SETS ((a),(b),(a,b)) still captures FROM-clause table")
    void testGroupByGroupingSets() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS
                      SELECT a, b, SUM(c)
                      FROM orders
                      GROUP BY GROUPING SETS ((a), (b), (a, b));
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "GROUP BY GROUPING SETS");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 2c: TableOperations found in PROC (GROUP BY GROUPING SETS) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        assertTrue(foundOrders, "Should capture ORDERS table with GROUP BY GROUPING SETS ((a),(b),(a,b))");
    }

    @Test
    @DisplayName("2d. GROUP BY ROLLUP in a direct SELECT ... INTO (non-cursor) still captures table")
    void testGroupByRollupSelectInto() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_a NUMBER;
                    v_sum NUMBER;
                  BEGIN
                    SELECT a, SUM(c)
                    INTO v_a, v_sum
                    FROM orders
                    GROUP BY ROLLUP(a);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "GROUP BY ROLLUP SELECT INTO");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "Should find PROC");

        System.out.println("=== 2d: TableOperations found in PROC (GROUP BY ROLLUP, SELECT INTO) ===");
        for (TableOperationInfo t : proc.getTableOperations()) {
            System.out.println("  Table: " + t.getTableName() + " schema=" + t.getSchema()
                    + " op=" + t.getOperation());
        }

        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        assertTrue(foundOrders, "Should capture ORDERS table with GROUP BY ROLLUP(a) in SELECT INTO statement");
    }
}
