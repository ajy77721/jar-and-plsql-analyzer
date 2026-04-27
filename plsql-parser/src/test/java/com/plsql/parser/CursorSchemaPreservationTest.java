package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Bug Fix #7: Cursor Table Schema Prefix Preserved")
public class CursorSchemaPreservationTest extends ParserTestBase {

    @Test
    @DisplayName("Cursor FOR loop with schema.table preserves schema in directTables")
    void testCursorForLoopSchemaPreserved() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR rec IN (SELECT id FROM CUSTOMER.policy_header WHERE status = 'A') LOOP
                      NULL;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Cursor FOR loop with schema");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        boolean found = proc.getTableOperations().stream().anyMatch(t ->
                "POLICY_HEADER".equalsIgnoreCase(t.getTableName())
                        && "CUSTOMER".equalsIgnoreCase(t.getSchema())
                        && "SELECT".equalsIgnoreCase(t.getOperation()));
        assertTrue(found, "Schema-prefixed table in cursor FOR loop should preserve CUSTOMER schema");
    }

    @Test
    @DisplayName("Declared cursor with schema.table preserves schema in directTables")
    void testDeclaredCursorSchemaPreserved() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS SELECT id FROM OPUS_CORE.transaction_log WHERE type = 'X';
                    v_rec c1%ROWTYPE;
                  BEGIN
                    OPEN c1;
                    FETCH c1 INTO v_rec;
                    CLOSE c1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Declared cursor with schema");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        boolean found = proc.getTableOperations().stream().anyMatch(t ->
                "TRANSACTION_LOG".equalsIgnoreCase(t.getTableName())
                        && "OPUS_CORE".equalsIgnoreCase(t.getSchema())
                        && "SELECT".equalsIgnoreCase(t.getOperation()));
        assertTrue(found, "Schema-prefixed table in declared cursor should preserve OPUS_CORE schema");
    }
}
