package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #3: Exception Handler Body Analysis")
public class ExceptionHandlerBodyTest extends ParserTestBase {

    @Test
    @DisplayName("Exception handler with INSERT tracks table operation")
    void testExceptionHandlerInsert() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN OTHERS THEN
                      INSERT INTO error_log (msg) VALUES (SQLERRM);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Exception handler INSERT");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        ExceptionHandlerInfo eh = proc.getExceptionHandlers().stream()
                .filter(e -> "OTHERS".equals(e.getExceptionName()))
                .findFirst().orElse(null);
        assertNotNull(eh);
        assertTrue(eh.getTableOperations().stream().anyMatch(t ->
                "ERROR_LOG".equalsIgnoreCase(t.getTableName())
                        && "INSERT".equalsIgnoreCase(t.getOperation())),
                "Exception handler should track INSERT into ERROR_LOG");
    }

    @Test
    @DisplayName("Exception handler with procedure call tracks call")
    void testExceptionHandlerCall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE LOG_ERROR(p_msg VARCHAR2) IS BEGIN NULL; END;
                  PROCEDURE PROC IS
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN OTHERS THEN
                      LOG_ERROR(SQLERRM);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Exception handler call");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        ExceptionHandlerInfo eh = proc.getExceptionHandlers().stream()
                .filter(e -> "OTHERS".equals(e.getExceptionName()))
                .findFirst().orElse(null);
        assertNotNull(eh);
        assertTrue(eh.getCalls().stream().anyMatch(c ->
                "LOG_ERROR".equalsIgnoreCase(c.getName())),
                "Exception handler should track call to LOG_ERROR");
    }

    @Test
    @DisplayName("Multiple WHEN branches each get their own ops")
    void testMultipleWhenBranches() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                      INSERT INTO missing_data (info) VALUES ('not found');
                    WHEN OTHERS THEN
                      INSERT INTO error_log (msg) VALUES (SQLERRM);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertEquals(2, proc.getExceptionHandlers().size());

        ExceptionHandlerInfo ndf = proc.getExceptionHandlers().stream()
                .filter(e -> e.getExceptionName() != null && e.getExceptionName().contains("NO_DATA_FOUND"))
                .findFirst().orElse(null);
        assertNotNull(ndf);
        assertTrue(ndf.getTableOperations().stream().anyMatch(t ->
                "MISSING_DATA".equalsIgnoreCase(t.getTableName())));

        ExceptionHandlerInfo others = proc.getExceptionHandlers().stream()
                .filter(e -> "OTHERS".equals(e.getExceptionName()))
                .findFirst().orElse(null);
        assertNotNull(others);
        assertTrue(others.getTableOperations().stream().anyMatch(t ->
                "ERROR_LOG".equalsIgnoreCase(t.getTableName())));
    }

    @Test
    @DisplayName("WHEN OTHERS with RAISE only — empty table/call lists")
    void testWhenOthersRaiseOnly() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN OTHERS THEN
                      RAISE;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        ExceptionHandlerInfo eh = proc.getExceptionHandlers().get(0);
        assertTrue(eh.getTableOperations().isEmpty());
        assertTrue(eh.getCalls().isEmpty());
    }

    @Test
    @DisplayName("Exception handler ops also appear in parent subprogram (backward compat)")
    void testBackwardCompatibility() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    SELECT 1 FROM main_table WHERE 1=0;
                  EXCEPTION
                    WHEN OTHERS THEN
                      INSERT INTO error_log (msg) VALUES (SQLERRM);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "ERROR_LOG".equalsIgnoreCase(t.getTableName())),
                "ERROR_LOG should still appear in parent SubprogramInfo.tableOperations");
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "MAIN_TABLE".equalsIgnoreCase(t.getTableName())),
                "MAIN_TABLE from main body should also be in SubprogramInfo");
    }
}
