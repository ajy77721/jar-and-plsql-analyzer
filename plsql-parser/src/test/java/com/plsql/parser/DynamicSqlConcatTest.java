package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #1: Dynamic SQL Concatenation Fragment Extraction")
public class DynamicSqlConcatTest extends ParserTestBase {

    @Test
    @DisplayName("Concatenated SELECT FROM extracts table from literal fragment")
    void testConcatSelectFrom() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_id NUMBER) IS
                    v_cnt NUMBER;
                  BEGIN
                    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM employees WHERE id = ' || p_id INTO v_cnt;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Concat SELECT FROM");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "EMPLOYEES".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "Should extract EMPLOYEES from concatenated dynamic SQL fragment");
    }

    @Test
    @DisplayName("Concatenated INSERT with SELECT extracts source table")
    void testConcatInsertSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_table VARCHAR2) IS
                  BEGIN
                    EXECUTE IMMEDIATE 'INSERT INTO ' || p_table || ' SELECT * FROM source_data WHERE status = ''A''';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "SOURCE_DATA".equalsIgnoreCase(t.getTableName())),
                "Should extract SOURCE_DATA from literal fragment after concatenation");
    }

    @Test
    @DisplayName("Multiple literal fragments each with table references")
    void testMultipleFragments() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'DELETE FROM audit_trail WHERE id IN (SELECT id FROM archive_data)';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "AUDIT_TRAIL".equalsIgnoreCase(t.getTableName())),
                "Should extract AUDIT_TRAIL");
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "ARCHIVE_DATA".equalsIgnoreCase(t.getTableName())),
                "Should extract ARCHIVE_DATA from subquery in dynamic SQL");
    }
}
