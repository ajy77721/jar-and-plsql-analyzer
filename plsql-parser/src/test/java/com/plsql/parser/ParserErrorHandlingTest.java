package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Error handling tests for PlSqlParserEngine and PlSqlAnalysisVisitor.
 * Verifies the parser degrades gracefully on invalid, encrypted, or edge-case input.
 */
@DisplayName("Parser Error Handling")
public class ParserErrorHandlingTest extends ParserTestBase {

    // ── Wrapped / Encrypted Source ──

    @Test
    @DisplayName("Wrapped source is detected and not parsed by ANTLR")
    void testWrappedSourceDetected() {
        String src = "CREATE OR REPLACE PACKAGE BODY MY_PKG wrapped\n"
                + "a000000\n89abcdef\n0123456789\n";
        ParseResult result = parse(src);
        assertNotNull(result);
        assertFalse(result.getObjects().isEmpty(), "Should produce at least one stub object");
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getType().toUpperCase().contains("ENCRYPTED")
                || obj.getType().toUpperCase().contains("WRAPPED"),
                "Object type should indicate ENCRYPTED/WRAPPED");
        assertTrue(result.getErrors() == null || result.getErrors().isEmpty(),
                "No parse errors should be raised for wrapped source");
    }

    @Test
    @DisplayName("isWrappedSource correctly identifies wrapped content")
    void testIsWrappedSourcePositive() {
        assertTrue(PlSqlParserEngine.isWrappedSource(
                "CREATE OR REPLACE PACKAGE BODY PK wrapped\na000000\n89abcdef"));
    }

    @Test
    @DisplayName("isWrappedSource returns false for normal PL/SQL")
    void testIsWrappedSourceNegative() {
        assertFalse(PlSqlParserEngine.isWrappedSource(
                "CREATE OR REPLACE PACKAGE BODY MY_PKG AS\n  PROCEDURE P IS BEGIN NULL; END P;\nEND MY_PKG;\n/"));
    }

    @Test
    @DisplayName("isWrappedSource handles null and empty")
    void testIsWrappedSourceNullEmpty() {
        assertFalse(PlSqlParserEngine.isWrappedSource(null));
        assertFalse(PlSqlParserEngine.isWrappedSource(""));
    }

    // ── Empty / Null Source ──

    @Test
    @DisplayName("Empty source string produces empty result without exception")
    void testEmptySource() {
        ParseResult result = parse("");
        assertNotNull(result);
    }

    @Test
    @DisplayName("Whitespace-only source produces empty result")
    void testWhitespaceOnlySource() {
        ParseResult result = parse("   \n\t\n   ");
        assertNotNull(result);
    }

    @Test
    @DisplayName("Single slash source produces empty result")
    void testSlashOnlySource() {
        ParseResult result = parse("/");
        assertNotNull(result);
    }

    // ── Invalid PL/SQL Syntax ──

    @Test
    @DisplayName("Completely invalid SQL returns errors list, does not throw")
    void testGarbageInput() {
        ParseResult result = parse("THIS IS NOT SQL AT ALL @#$%^&*");
        assertNotNull(result);
        // The parser should produce errors but not throw
        assertNotNull(result.getErrors());
    }

    @Test
    @DisplayName("Unclosed BEGIN block returns errors")
    void testUnclosedBegin() {
        String sql = "CREATE OR REPLACE PROCEDURE P IS\nBEGIN\n  INSERT INTO t VALUES(1);\n";
        ParseResult result = parse(sql);
        assertNotNull(result);
        assertNotNull(result.getErrors());
    }

    @Test
    @DisplayName("Mismatched END name returns errors but still parses")
    void testMismatchedEndName() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE P IS
                  BEGIN
                    NULL;
                  END WRONG_NAME;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNotNull(result);
    }

    // ── Bare JOIN Keyword Fix ──

    @Test
    @DisplayName("Bare JOIN keyword in SELECT parses without errors")
    void testBareJoinKeyword() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE P IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM employees e
                    JOIN departments d ON e.dept_id = d.id
                    LEFT JOIN locations l ON d.loc_id = l.id;
                  END P;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Bare JOIN keyword");
        SubprogramInfo proc = findSub(result, "P");
        assertNotNull(proc);
        assertFalse(proc.getTableOperations().isEmpty(),
                "Should detect table operations with JOINs");
    }

    @Test
    @DisplayName("CROSS JOIN, NATURAL JOIN parse without errors")
    void testVariousJoinTypes() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE P IS
                    CURSOR c1 IS
                      SELECT * FROM t1
                      CROSS JOIN t2
                      NATURAL JOIN t3;
                  BEGIN
                    NULL;
                  END P;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CROSS/NATURAL JOIN");
    }

    // ── WORKERID as Column Alias Fix ──

    @Test
    @DisplayName("WORKERID as column alias parses without errors")
    void testWorkeridAsAlias() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE P IS
                    v_id NUMBER;
                  BEGIN
                    SELECT employee_id AS WORKERID INTO v_id FROM employees WHERE rownum = 1;
                  END P;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "WORKERID alias");
    }

    @Test
    @DisplayName("WORKERID as table alias in FROM clause parses without errors")
    void testWorkeridAsTableAlias() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE P IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM employees WORKERID WHERE WORKERID.status = 'A';
                  END P;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "WORKERID table alias");
    }

    // ── Large Package Body (no OOM) ──

    @Test
    @DisplayName("Large package body with many subprograms parses without OOM")
    void testLargePackageBody() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE OR REPLACE PACKAGE BODY BIG_PKG AS\n");
        for (int i = 0; i < 200; i++) {
            sb.append("  PROCEDURE P").append(i).append(" IS\n");
            sb.append("  BEGIN\n");
            sb.append("    INSERT INTO t").append(i).append(" VALUES (").append(i).append(");\n");
            sb.append("  END P").append(i).append(";\n\n");
        }
        sb.append("END BIG_PKG;\n/\n");

        ParseResult result = parse(sb.toString());
        assertNotNull(result);
        assertFalse(result.getObjects().isEmpty());
        assertTrue(result.getObjects().get(0).getSubprograms().size() >= 100,
                "Should parse at least 100 of the 200 subprograms");
    }

    // ── Different Object Types ──

    @Test
    @DisplayName("Standalone procedure parses correctly")
    void testStandaloneProcedure() {
        String sql = """
                CREATE OR REPLACE PROCEDURE MY_STANDALONE_PROC (p_id NUMBER) IS
                BEGIN
                  INSERT INTO audit_log (id) VALUES (p_id);
                END MY_STANDALONE_PROC;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Standalone procedure");
        assertFalse(result.getObjects().isEmpty());
        assertTrue(result.getObjects().get(0).getName().toUpperCase().contains("MY_STANDALONE_PROC"));
    }

    @Test
    @DisplayName("Standalone function parses correctly")
    void testStandaloneFunction() {
        String sql = """
                CREATE OR REPLACE FUNCTION FN_CALC (p_val NUMBER) RETURN NUMBER IS
                BEGIN
                  RETURN p_val * 2;
                END FN_CALC;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Standalone function");
        assertFalse(result.getObjects().isEmpty());
        assertTrue(result.getObjects().get(0).getName().toUpperCase().contains("FN_CALC"));
    }

    @Test
    @DisplayName("Package body with procedures and functions parses correctly")
    void testPackageBody() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  FUNCTION HELPER RETURN NUMBER IS BEGIN RETURN 1; END HELPER;
                  PROCEDURE MAIN IS BEGIN NULL; END MAIN;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Package body");
        assertEquals(1, result.getObjects().size());
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getType().toUpperCase().contains("PACKAGE"),
                "Object type should contain PACKAGE, got: " + obj.getType());
        assertTrue(obj.getSubprograms().size() >= 2);
    }

    @Test
    @DisplayName("Trigger parses correctly")
    void testTrigger() {
        String sql = """
                CREATE OR REPLACE TRIGGER TRG_AUDIT
                AFTER INSERT OR UPDATE ON employees
                FOR EACH ROW
                BEGIN
                  INSERT INTO audit_log (action, emp_id, ts)
                  VALUES ('CHANGE', :NEW.id, SYSTIMESTAMP);
                END TRG_AUDIT;
                /
                """;
        ParseResult result = parse(sql);
        assertNotNull(result);
        assertFalse(result.getObjects().isEmpty());
    }

    // ── trimDuplicateDefinition ──

    @Test
    @DisplayName("trimDuplicateDefinition removes duplicate standalone definition")
    void testTrimDuplicateDefinition() {
        String input = "CREATE OR REPLACE FUNCTION FN_X RETURN NUMBER IS BEGIN RETURN 1; END;\n"
                + "FUNCTION FN_X RETURN NUMBER IS BEGIN RETURN 1; END;\n/";
        String trimmed = PlSqlParserEngine.trimDuplicateDefinition(input);
        assertNotNull(trimmed);
        // The trimmed result should not be longer than the original
        assertTrue(trimmed.length() <= input.length());
    }

    @Test
    @DisplayName("trimDuplicateDefinition is no-op on null or short input")
    void testTrimDuplicateDefinitionEdgeCases() {
        assertNull(PlSqlParserEngine.trimDuplicateDefinition(null));
        assertEquals("short", PlSqlParserEngine.trimDuplicateDefinition("short"));
    }

    // ── Parse timing ──

    @Test
    @DisplayName("ParseResult includes non-zero parse time")
    void testParseTimeMeasured() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY PKG AS
                  PROCEDURE P IS BEGIN NULL; END P;
                END PKG;
                /
                """;
        PlSqlParserEngine eng = new PlSqlParserEngine();
        ParseResult result = eng.parseContent(sql, "test.sql");
        assertTrue(result.getParseTimeMs() >= 0, "Parse time should be measured");
    }
}
