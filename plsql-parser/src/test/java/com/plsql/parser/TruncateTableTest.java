package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Bug Fix #5: TRUNCATE TABLE Captured")
public class TruncateTableTest extends ParserTestBase {

    @Test
    @DisplayName("TRUNCATE TABLE captures table with TRUNCATE operation")
    void testTruncateTable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_data';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "TRUNCATE TABLE");
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE TRUNCATE TABLE extracts table via dynamic SQL")
    void testExecuteImmediateTruncate() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE CLEAN_UP IS
                  BEGIN
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE temp_staging';
                  END CLEAN_UP;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE TRUNCATE");
        SubprogramInfo proc = findSub(result, "CLEAN_UP");
        assertNotNull(proc, "Should find CLEAN_UP subprogram");
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "TEMP_STAGING".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "Should extract TEMP_STAGING from EXECUTE IMMEDIATE TRUNCATE");
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE TRUNCATE TABLE with schema.table")
    void testExecuteImmediateTruncateWithSchema() {
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
        assertNoParsErrors(result, "EXECUTE IMMEDIATE TRUNCATE with schema");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "STAGING_DATA".equalsIgnoreCase(t.getTableName())
                        && "CUSTOMER".equalsIgnoreCase(t.getSchema())),
                "Should extract schema-prefixed table from EXECUTE IMMEDIATE TRUNCATE");
    }
}
