package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #6: DBMS_SQL.PARSE Table Extraction")
public class DbmsSqlParseTest extends ParserTestBase {

    @Test
    @DisplayName("DBMS_SQL.PARSE with string literal extracts table")
    void testDbmsSqlParseStringLiteral() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    l_cursor INTEGER;
                  BEGIN
                    l_cursor := DBMS_SQL.OPEN_CURSOR;
                    DBMS_SQL.PARSE(l_cursor, 'SELECT id, name FROM customer_master WHERE status = ''A''', DBMS_SQL.NATIVE);
                    DBMS_SQL.CLOSE_CURSOR(l_cursor);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "DBMS_SQL.PARSE");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "CUSTOMER_MASTER".equalsIgnoreCase(t.getTableName())
                        && "DYNAMIC".equalsIgnoreCase(t.getOperation())),
                "Should extract CUSTOMER_MASTER from DBMS_SQL.PARSE SQL text");
    }

    @Test
    @DisplayName("DBMS_SQL.PARSE with variable — no table extractable, but DynamicSqlInfo recorded")
    void testDbmsSqlParseVariable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    l_cursor INTEGER;
                    l_sql VARCHAR2(200);
                  BEGIN
                    l_cursor := DBMS_SQL.OPEN_CURSOR;
                    DBMS_SQL.PARSE(l_cursor, l_sql, DBMS_SQL.NATIVE);
                    DBMS_SQL.CLOSE_CURSOR(l_cursor);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "DBMS_SQL.PARSE variable");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        // DBMS_SQL calls should be detected as BUILTIN calls
        assertTrue(proc.getCalls().stream().anyMatch(c ->
                "DBMS_SQL".equalsIgnoreCase(c.getPackageName())),
                "Should detect DBMS_SQL calls");
        assertTrue(proc.getDynamicSql().stream().anyMatch(d ->
                "DBMS_SQL".equals(d.getType())),
                "Should record DynamicSqlInfo for DBMS_SQL");
    }

    @Test
    @DisplayName("DBMS_SQL.PARSE DynamicSqlInfo contains SQL expression")
    void testDbmsSqlParseSqlExpression() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    l_cursor INTEGER;
                  BEGIN
                    l_cursor := DBMS_SQL.OPEN_CURSOR;
                    DBMS_SQL.PARSE(l_cursor, 'UPDATE batch_ctrl SET done = 1', DBMS_SQL.NATIVE);
                    DBMS_SQL.CLOSE_CURSOR(l_cursor);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        DynamicSqlInfo dsi = proc.getDynamicSql().stream()
                .filter(d -> "DBMS_SQL".equals(d.getType()) && d.getSqlExpression() != null
                        && d.getSqlExpression().contains("UPDATE"))
                .findFirst().orElse(null);
        assertNotNull(dsi, "Should have DBMS_SQL DynamicSqlInfo with SQL expression text");
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "BATCH_CTRL".equalsIgnoreCase(t.getTableName())),
                "Should extract BATCH_CTRL from DBMS_SQL.PARSE");
    }
}
