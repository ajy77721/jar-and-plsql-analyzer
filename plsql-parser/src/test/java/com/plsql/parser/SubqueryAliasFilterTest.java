package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Bug Fix #8: CTE & Inline View Aliases Not Treated as Tables")
public class SubqueryAliasFilterTest extends ParserTestBase {

    @Test
    @DisplayName("CTE alias (WITH T AS ...) is NOT captured as a table")
    void testCteAliasFiltered() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  FUNCTION FN_TEST RETURN NUMBER IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM (
                      WITH T AS (SELECT id, status FROM real_table WHERE status = 'A')
                      SELECT * FROM T WHERE id > 100
                    );
                    RETURN v_cnt;
                  END FN_TEST;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CTE alias");
        SubprogramInfo fn = findSub(result, "FN_TEST");
        assertNotNull(fn);
        List<TableOperationInfo> ops = fn.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                "REAL_TABLE".equalsIgnoreCase(t.getTableName())),
                "Should capture REAL_TABLE");
        assertFalse(ops.stream().anyMatch(t ->
                "T".equalsIgnoreCase(t.getTableName()) && t.getSchema() == null),
                "CTE alias T should NOT be captured as a table");
    }

    @Test
    @DisplayName("Inline view alias (FROM (SELECT ...) alias) is NOT captured as a table")
    void testInlineViewAliasFiltered() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM (SELECT id, name FROM employees WHERE dept = 'IT') sub_view
                    WHERE sub_view.id > 0;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Inline view alias");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "Should capture EMPLOYEES");
        assertFalse(ops.stream().anyMatch(t ->
                "SUB_VIEW".equalsIgnoreCase(t.getTableName())),
                "Inline view alias SUB_VIEW should NOT be captured as a table");
    }

    @Test
    @DisplayName("Nested inline view aliases are all filtered")
    void testNestedInlineViewAliasesFiltered() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM (
                      SELECT tab.*, ROWNUM rn
                      FROM (
                        SELECT t.*, SYSDATE dt
                        FROM (SELECT id, amt FROM transactions WHERE yr = 2024) t
                      ) tab
                    ) calctab
                    WHERE calctab.rn <= 100;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Nested inline views");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                "TRANSACTIONS".equalsIgnoreCase(t.getTableName())),
                "Should capture TRANSACTIONS (the real table)");
        assertFalse(ops.stream().anyMatch(t ->
                "T".equalsIgnoreCase(t.getTableName()) && t.getSchema() == null),
                "Inline view alias T should NOT be captured");
        assertFalse(ops.stream().anyMatch(t ->
                "TAB".equalsIgnoreCase(t.getTableName()) && t.getSchema() == null),
                "Inline view alias TAB should NOT be captured");
        assertFalse(ops.stream().anyMatch(t ->
                "CALCTAB".equalsIgnoreCase(t.getTableName())),
                "Inline view alias CALCTAB should NOT be captured");
    }

    @Test
    @DisplayName("Schema-prefixed table with same name as CTE is still captured")
    void testSchemaTableNotFilteredByCte() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM (
                      WITH T AS (SELECT id FROM other_table)
                      SELECT * FROM CUSTOMER.T
                    );
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                "T".equalsIgnoreCase(t.getTableName())
                        && "CUSTOMER".equalsIgnoreCase(t.getSchema())),
                "Schema-prefixed CUSTOMER.T should still be captured even if T is a CTE name");
    }

    @Test
    @DisplayName("Multiple CTEs — all aliases filtered, all real tables captured")
    void testMultipleCtes() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH
                      active_policies AS (SELECT id FROM policy_header WHERE status = 'A'),
                      claims AS (SELECT claim_id, pol_id FROM claim_master WHERE yr = 2024)
                    SELECT COUNT(*) INTO v_cnt
                    FROM active_policies ap
                    JOIN claims c ON c.pol_id = ap.id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                "POLICY_HEADER".equalsIgnoreCase(t.getTableName())),
                "Should capture POLICY_HEADER");
        assertTrue(ops.stream().anyMatch(t ->
                "CLAIM_MASTER".equalsIgnoreCase(t.getTableName())),
                "Should capture CLAIM_MASTER");
        assertFalse(ops.stream().anyMatch(t ->
                "ACTIVE_POLICIES".equalsIgnoreCase(t.getTableName())),
                "CTE alias ACTIVE_POLICIES should NOT be captured");
        assertFalse(ops.stream().anyMatch(t ->
                "CLAIMS".equalsIgnoreCase(t.getTableName())),
                "CTE alias CLAIMS should NOT be captured");
    }

    @Test
    @DisplayName("Real table named same as common alias but used directly — still captured")
    void testRealTableWithCommonName() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM tab_master;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "TAB_MASTER".equalsIgnoreCase(t.getTableName())),
                "Real table TAB_MASTER should be captured normally");
    }
}
