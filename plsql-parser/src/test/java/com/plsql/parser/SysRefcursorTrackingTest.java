package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #7: SYS_REFCURSOR / OPEN FOR SELECT Tracking")
public class SysRefcursorTrackingTest extends ParserTestBase {

    @Test
    @DisplayName("OPEN cursor FOR SELECT captures tables as SELECT")
    void testOpenForSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_cur OUT SYS_REFCURSOR) IS
                  BEGIN
                    OPEN p_cur FOR SELECT id, name FROM employees WHERE active = 'Y';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "OPEN FOR SELECT");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "EMPLOYEES".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "OPEN FOR SELECT should capture EMPLOYEES as SELECT operation");
    }

    @Test
    @DisplayName("OPEN FOR dynamic expression creates DynamicSqlInfo")
    void testOpenForDynamic() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_cur OUT SYS_REFCURSOR, p_table VARCHAR2) IS
                  BEGIN
                    OPEN p_cur FOR 'SELECT * FROM ' || p_table;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "OPEN FOR dynamic");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getDynamicSql().stream().anyMatch(d ->
                "OPEN_FOR".equals(d.getType())),
                "Should have OPEN_FOR DynamicSqlInfo");
    }

    @Test
    @DisplayName("OPEN FOR SELECT with JOINs captures all tables")
    void testOpenForSelectWithJoins() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_cur OUT SYS_REFCURSOR) IS
                  BEGIN
                    OPEN p_cur FOR
                      SELECT e.name, d.dept_name
                      FROM employees e
                      JOIN departments d ON d.id = e.dept_id
                      WHERE e.active = 'Y';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "Should capture EMPLOYEES from OPEN FOR SELECT");
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "DEPARTMENTS".equalsIgnoreCase(t.getTableName())),
                "Should capture DEPARTMENTS from OPEN FOR SELECT JOIN");
    }

    @Test
    @DisplayName("OPEN FOR dynamic string literal extracts table")
    void testOpenForDynamicLiteral() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_cur OUT SYS_REFCURSOR) IS
                  BEGIN
                    OPEN p_cur FOR 'SELECT id FROM batch_queue WHERE status = ''PENDING''';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "BATCH_QUEUE".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "Should extract BATCH_QUEUE from OPEN FOR dynamic literal");
    }
}
