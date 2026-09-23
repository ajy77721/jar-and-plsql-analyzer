package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Bug Fix #4: Subquery Tables Captured When No Parent DML Context")
public class SubqueryNullContextTest extends ParserTestBase {

    @Test
    @DisplayName("Scalar subquery in assignment captures table as SELECT")
    void testScalarSubqueryInAssignment() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    v_cnt := (SELECT COUNT(*) FROM audit_log WHERE status = 'A');
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Scalar subquery in assignment");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "AUDIT_LOG".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "AUDIT_LOG in scalar subquery should be SELECT");
    }

    @Test
    @DisplayName("EXISTS subquery in IF condition captures table as SELECT")
    void testExistsSubqueryInIfCondition() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_exists BOOLEAN;
                  BEGIN
                    IF EXISTS (SELECT 1 FROM pending_tasks WHERE owner = 'ME') THEN
                      NULL;
                    END IF;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        boolean found = proc.getTableOperations().stream().anyMatch(t ->
                "PENDING_TASKS".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation()));
        assertTrue(found, "PENDING_TASKS in EXISTS subquery should be SELECT");
    }

    @Test
    @DisplayName("Subquery in CASE expression captures table as SELECT")
    void testSubqueryInCaseExpression() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_label VARCHAR2(10);
                  BEGIN
                    v_label := CASE
                      WHEN (SELECT COUNT(*) FROM errors) > 0 THEN 'BAD'
                      ELSE 'OK'
                    END;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        boolean found = proc.getTableOperations().stream().anyMatch(t ->
                "ERRORS".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation()));
        assertTrue(found, "ERRORS table in CASE subquery should be SELECT");
    }
}
