package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Bug Fix #6: Dynamic SQL Table Extraction from String Literals")
public class DynamicSqlTableExtractionTest extends ParserTestBase {

    @Test
    @DisplayName("EXECUTE IMMEDIATE 'SELECT FROM table' extracts table as DYNAMIC")
    void testExecuteImmediateSelectFrom() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM audit_transactions' INTO v_cnt;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE SELECT");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "AUDIT_TRANSACTIONS".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "Should extract AUDIT_TRANSACTIONS from dynamic SQL string");
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE 'DELETE FROM table' extracts table")
    void testExecuteImmediateDeleteFrom() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'DELETE FROM temp_staging WHERE created < SYSDATE - 30';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "TEMP_STAGING".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "Should extract TEMP_STAGING from dynamic DELETE");
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE 'INSERT INTO table' extracts table")
    void testExecuteImmediateInsertInto() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'INSERT INTO log_archive SELECT * FROM log_current';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                "LOG_ARCHIVE".equalsIgnoreCase(t.getTableName())),
                "Should extract LOG_ARCHIVE from dynamic INSERT");
        assertTrue(ops.stream().anyMatch(t ->
                "LOG_CURRENT".equalsIgnoreCase(t.getTableName())),
                "Should extract LOG_CURRENT from dynamic SELECT");
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE with schema.table extracts both parts")
    void testExecuteImmediateWithSchema() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMER.staging_data';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "STAGING_DATA".equalsIgnoreCase(t.getTableName())
                        && "CUSTOMER".equalsIgnoreCase(t.getSchema())),
                "Should extract schema-prefixed table from dynamic TRUNCATE");
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE with concatenation — extracts literal parts")
    void testExecuteImmediateWithConcatenation() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_table VARCHAR2) IS
                  BEGIN
                    EXECUTE IMMEDIATE 'DELETE FROM ' || p_table || ' WHERE status = ''OLD''';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE with concat");
        // Variable part can't be extracted — only literal parts are analyzed
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE with UPDATE extracts table")
    void testExecuteImmediateUpdate() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'UPDATE batch_control SET status = ''DONE'' WHERE id = :1' USING 42;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "BATCH_CONTROL".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "Should extract BATCH_CONTROL from dynamic UPDATE");
    }
}
