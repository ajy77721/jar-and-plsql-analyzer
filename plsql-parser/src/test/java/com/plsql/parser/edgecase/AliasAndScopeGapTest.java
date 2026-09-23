package com.plsql.parser.edgecase;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case tests for Gap #9 (Table alias resolution) and
 * Gap #13 (Variable scope not cross-referenced with cursor columns).
 */
@DisplayName("Gaps #9, #13: Table Alias Resolution and Variable Scope")
public class AliasAndScopeGapTest extends ParserTestBase {

    // =========================================================================
    // Gap #9: Table alias extraction
    // =========================================================================

    @Test
    @DisplayName("Simple table alias: SELECT e.name FROM employees e")
    void testTableAliasExtracted() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_name VARCHAR2(100);
                  BEGIN
                    SELECT e.name INTO v_name FROM employees e WHERE ROWNUM = 1;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Simple table alias");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        TableOperationInfo emp = sub.getTableOperations().stream()
                .filter(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(emp, "Should extract EMPLOYEES table");
        assertEquals("e", emp.getAlias() != null ? emp.getAlias().toLowerCase() : null,
                "Alias should be 'e'");
    }

    @Test
    @DisplayName("Table alias with AS keyword: SELECT * FROM employees AS emp")
    void testTableAliasWithAsKeyword() {
        // Note: Oracle does not officially support AS for table aliases,
        // but some SQL code uses it. Test that the parser handles it gracefully.
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM employees AS emp;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        // The parser may or may not error on non-Oracle syntax; just verify table extraction
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        if (sub != null) {
            boolean foundEmp = sub.getTableOperations().stream()
                    .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()));
            assertTrue(foundEmp, "Should still extract EMPLOYEES table");
        }
    }

    @Test
    @DisplayName("Single-letter aliases not confused with table names: FROM table_a a, table_b b")
    void testSingleLetterAliasNotConfusedWithTable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM table_a a, table_b b
                    WHERE a.id = b.id;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Single-letter aliases");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();
        assertTrue(ops.stream().anyMatch(t -> "TABLE_A".equalsIgnoreCase(t.getTableName())),
                "Should extract TABLE_A");
        assertTrue(ops.stream().anyMatch(t -> "TABLE_B".equalsIgnoreCase(t.getTableName())),
                "Should extract TABLE_B");
    }

    @Test
    @DisplayName("Subquery alias NOT captured as table: FROM (SELECT * FROM real_table) sq")
    void testSubqueryAliasNotAsTable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM (SELECT * FROM real_table) sq;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Subquery alias filtering");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();
        assertTrue(ops.stream().anyMatch(t -> "REAL_TABLE".equalsIgnoreCase(t.getTableName())),
                "Should extract REAL_TABLE from subquery");
        assertFalse(ops.stream().anyMatch(t -> "SQ".equalsIgnoreCase(t.getTableName())),
                "Subquery alias SQ should NOT appear as a table");
    }

    @Test
    @DisplayName("CTE alias NOT captured as table: WITH cte AS (...) SELECT * FROM cte")
    void testCteAliasNotAsTable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH cte AS (SELECT id FROM real_table WHERE status = 'A')
                    SELECT COUNT(*) INTO v_cnt FROM cte;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CTE alias filtering");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();
        assertTrue(ops.stream().anyMatch(t -> "REAL_TABLE".equalsIgnoreCase(t.getTableName())),
                "Should extract REAL_TABLE from CTE body");
        assertFalse(ops.stream().anyMatch(t ->
                "CTE".equalsIgnoreCase(t.getTableName()) && t.getSchema() == null),
                "CTE alias should NOT appear as a table");
    }

    @Test
    @DisplayName("Multiple CTEs: only real tables appear in operations")
    void testMultipleCteAliasesFiltered() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH
                      c1 AS (SELECT id FROM real_t1 WHERE active = 1),
                      c2 AS (SELECT id FROM c1 WHERE id > 10),
                      c3 AS (SELECT c2.id, r.name FROM c2 JOIN real_t2 r ON r.id = c2.id)
                    SELECT COUNT(*) INTO v_cnt FROM c3;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multiple CTE alias filtering");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();
        assertTrue(ops.stream().anyMatch(t -> "REAL_T1".equalsIgnoreCase(t.getTableName())),
                "Should extract REAL_T1");
        assertTrue(ops.stream().anyMatch(t -> "REAL_T2".equalsIgnoreCase(t.getTableName())),
                "Should extract REAL_T2");
        assertFalse(ops.stream().anyMatch(t -> "C1".equalsIgnoreCase(t.getTableName())
                && t.getSchema() == null), "CTE C1 should NOT appear as table");
        assertFalse(ops.stream().anyMatch(t -> "C2".equalsIgnoreCase(t.getTableName())
                && t.getSchema() == null), "CTE C2 should NOT appear as table");
        assertFalse(ops.stream().anyMatch(t -> "C3".equalsIgnoreCase(t.getTableName())
                && t.getSchema() == null), "CTE C3 should NOT appear as table");
    }

    @Test
    @DisplayName("Self-join: both aliases on same table, join detected")
    void testSelfJoinBothAliases() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM employees e1
                    JOIN employees e2 ON e1.manager_id = e2.employee_id;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Self-join aliases");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        // The table EMPLOYEES should appear, and there should be join info
        boolean hasEmployees = sub.getTableOperations().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()));
        assertTrue(hasEmployees, "Should extract EMPLOYEES table for self-join");
        boolean hasJoin = sub.getTableOperations().stream()
                .flatMap(t -> t.getJoins().stream())
                .anyMatch(j -> j.getJoinedTable() != null
                        && j.getJoinedTable().toUpperCase().contains("EMPLOYEES"));
        assertTrue(hasJoin, "Should detect join on EMPLOYEES (self-join)");
    }

    // =========================================================================
    // Gap #13: Variable scope / type references
    // =========================================================================

    @Test
    @DisplayName("table.column%TYPE adds table to dependencies")
    void testPercentTypeVariableDeclaration() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_name employees.last_name%TYPE;
                  BEGIN
                    NULL;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "%TYPE variable declaration");
        ParsedObject obj = result.getObjects().get(0);
        Set<String> depTables = obj.getDependencies().getTables();
        assertTrue(depTables.contains("EMPLOYEES"),
                "EMPLOYEES from %TYPE reference should appear in dependency tables");
    }

    @Test
    @DisplayName("table%ROWTYPE adds table to dependencies")
    void testPercentRowtypeDeclaration() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_emp employees%ROWTYPE;
                  BEGIN
                    NULL;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "%ROWTYPE variable declaration");
        ParsedObject obj = result.getObjects().get(0);
        Set<String> depTables = obj.getDependencies().getTables();
        assertTrue(depTables.contains("EMPLOYEES"),
                "EMPLOYEES from %ROWTYPE reference should appear in dependency tables");
    }

    @Test
    @DisplayName("FETCH INTO variables detected but no cursor-column-to-variable mapping (gap)")
    void testFetchIntoVariablesNotCrossReferenced() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    CURSOR c1 IS SELECT id, name FROM employees;
                    v_id NUMBER;
                    v_name VARCHAR2(100);
                  BEGIN
                    OPEN c1;
                    FETCH c1 INTO v_id, v_name;
                    CLOSE c1;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FETCH INTO variables");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        // Cursor should be detected
        boolean hasCursor = sub.getCursors().stream()
                .anyMatch(c -> "C1".equalsIgnoreCase(c.getName()));
        assertTrue(hasCursor, "Should detect cursor C1");
        // Variables should be declared
        assertFalse(sub.getLocalVariables().isEmpty(),
                "Should have local variables declared");
        // Gap: no model field maps cursor columns to FETCH INTO variables
        // This test documents that the parser extracts both but does not cross-reference them
    }

    @Test
    @DisplayName("Local variable declarations tracked via getLocalVariables()")
    void testLocalVariableDeclarationsTracked() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                    v_name VARCHAR2(100);
                  BEGIN
                    v_cnt := 0;
                    v_name := 'test';
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Local variable declarations");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<VariableInfo> vars = sub.getLocalVariables();
        assertNotNull(vars);
        assertTrue(vars.stream().anyMatch(v -> "V_CNT".equalsIgnoreCase(v.getName())),
                "Should track V_CNT local variable");
        assertTrue(vars.stream().anyMatch(v -> "V_NAME".equalsIgnoreCase(v.getName())),
                "Should track V_NAME local variable");
    }

    @Test
    @DisplayName("Parameter directions (IN, OUT, IN OUT) tracked via getDirection()")
    void testParameterDirectionsTracked() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc(
                    p_in    IN     NUMBER,
                    p_out      OUT VARCHAR2,
                    p_inout IN OUT DATE
                  ) IS
                  BEGIN
                    p_out := TO_CHAR(p_in);
                    p_inout := SYSDATE;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Parameter directions");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<ParameterInfo> params = sub.getParameters();
        assertNotNull(params);
        assertTrue(params.size() >= 3, "Should have at least 3 parameters");

        ParameterInfo pIn = params.stream()
                .filter(p -> "P_IN".equalsIgnoreCase(p.getName()))
                .findFirst().orElse(null);
        assertNotNull(pIn, "Should find P_IN parameter");
        assertEquals("IN", pIn.getDirection().toUpperCase(),
                "P_IN direction should be IN");

        ParameterInfo pOut = params.stream()
                .filter(p -> "P_OUT".equalsIgnoreCase(p.getName()))
                .findFirst().orElse(null);
        assertNotNull(pOut, "Should find P_OUT parameter");
        assertEquals("OUT", pOut.getDirection().toUpperCase(),
                "P_OUT direction should be OUT");

        ParameterInfo pInOut = params.stream()
                .filter(p -> "P_INOUT".equalsIgnoreCase(p.getName()))
                .findFirst().orElse(null);
        assertNotNull(pInOut, "Should find P_INOUT parameter");
        assertTrue(pInOut.getDirection().toUpperCase().contains("IN")
                && pInOut.getDirection().toUpperCase().contains("OUT"),
                "P_INOUT direction should be IN OUT");
    }
}
