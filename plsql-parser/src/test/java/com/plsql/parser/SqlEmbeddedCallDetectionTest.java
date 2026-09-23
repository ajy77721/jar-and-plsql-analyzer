package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SQL-Embedded Function Call Detection")
public class SqlEmbeddedCallDetectionTest extends ParserTestBase {

    @Test
    @DisplayName("Function call inside AND clause of CASE WHEN expression in cursor SQL")
    void testFunctionCallInAndCaseWhen() {
        String sql = "CREATE OR REPLACE PACKAGE BODY pg_uwpl_ci_upload AS\n"
            + "  FUNCTION fn_chk_number(p_val VARCHAR2) RETURN NUMBER IS\n"
            + "  BEGIN\n"
            + "    RETURN 0;\n"
            + "  END fn_chk_number;\n"
            + "\n"
            + "  FUNCTION fn_chk_tab(p_tab VARCHAR2, p_col VARCHAR2, p_val VARCHAR2) RETURN NUMBER IS\n"
            + "  BEGIN\n"
            + "    RETURN 0;\n"
            + "  END fn_chk_tab;\n"
            + "\n"
            + "  PROCEDURE prc_validate_data IS\n"
            + "    CURSOR cur_data IS\n"
            + "      SELECT empno, datavalue,\n"
            + "             CASE WHEN valid_type = 'N'\n"
            + "                       AND pg_uwpl_ci_upload.fn_chk_number(datavalue) = 0\n"
            + "                  THEN 'INVALID_NUMBER'\n"
            + "                  WHEN valid_type = 'T'\n"
            + "                       AND pg_uwpl_ci_upload.fn_chk_tab(valid_tab, valid_tab_column, datavalue) = 0\n"
            + "                  THEN 'INVALID_TAB'\n"
            + "                  ELSE 'OK'\n"
            + "             END AS validation_status\n"
            + "      FROM upload_staging;\n"
            + "  BEGIN\n"
            + "    NULL;\n"
            + "  END prc_validate_data;\n"
            + "END pg_uwpl_ci_upload;\n"
            + "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CASE WHEN AND function call");
        SubprogramInfo proc = findSub(result, "PRC_VALIDATE_DATA");
        assertNotNull(proc, "Should find PRC_VALIDATE_DATA");

        System.out.println("=== Calls found in PRC_VALIDATE_DATA ===");
        for (CallInfo call : proc.getCalls()) {
            System.out.println("  Call: name=" + call.getName() + " pkg=" + call.getPackageName()
                + " type=" + call.getType() + " args=" + call.getArguments());
        }

        boolean foundFnChkNumber = proc.getCalls().stream()
            .anyMatch(c -> "FN_CHK_NUMBER".equalsIgnoreCase(c.getName()));
        boolean foundFnChkTab = proc.getCalls().stream()
            .anyMatch(c -> "FN_CHK_TAB".equalsIgnoreCase(c.getName()));

        assertTrue(foundFnChkNumber, "Should detect pg_uwpl_ci_upload.fn_chk_number() call in CASE WHEN AND");
        assertTrue(foundFnChkTab, "Should detect pg_uwpl_ci_upload.fn_chk_tab() call in CASE WHEN AND");
    }

    @Test
    @DisplayName("Function call in WHERE clause with AND should be detected")
    void testFunctionCallInWhereAndClause() {
        String sql = "CREATE OR REPLACE PACKAGE BODY my_pkg AS\n"
            + "  PROCEDURE proc1 IS\n"
            + "    v_count NUMBER;\n"
            + "  BEGIN\n"
            + "    SELECT COUNT(*) INTO v_count\n"
            + "    FROM my_table\n"
            + "    WHERE status = 'A'\n"
            + "      AND other_pkg.check_valid(col1) = 1\n"
            + "      AND other_pkg.check_format(col2, 'UPPER') = 'Y';\n"
            + "  END proc1;\n"
            + "END my_pkg;\n"
            + "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "WHERE AND function call");
        SubprogramInfo proc = findSub(result, "PROC1");
        assertNotNull(proc);

        System.out.println("=== Calls found in PROC1 ===");
        for (CallInfo call : proc.getCalls()) {
            System.out.println("  Call: name=" + call.getName() + " pkg=" + call.getPackageName()
                + " type=" + call.getType() + " args=" + call.getArguments());
        }

        boolean foundCheckValid = proc.getCalls().stream()
            .anyMatch(c -> "CHECK_VALID".equalsIgnoreCase(c.getName()));
        boolean foundCheckFormat = proc.getCalls().stream()
            .anyMatch(c -> "CHECK_FORMAT".equalsIgnoreCase(c.getName()));

        assertTrue(foundCheckValid, "Should detect other_pkg.check_valid()");
        assertTrue(foundCheckFormat, "Should detect other_pkg.check_format()");
    }

    @Test
    @DisplayName("Function call in SELECT column list with complex expressions")
    void testFunctionCallInSelectColumnsComplex() {
        String sql = "CREATE OR REPLACE PACKAGE BODY my_pkg AS\n"
            + "  PROCEDURE proc2 IS\n"
            + "    CURSOR c1 IS\n"
            + "      SELECT col1,\n"
            + "             NVL(util_pkg.format_name(first_name, last_name), 'UNKNOWN') AS full_name,\n"
            + "             DECODE(util_pkg.get_status(emp_id), 1, 'ACTIVE', 'INACTIVE') AS emp_status\n"
            + "      FROM employees;\n"
            + "  BEGIN\n"
            + "    NULL;\n"
            + "  END proc2;\n"
            + "END my_pkg;\n"
            + "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT column complex function call");
        SubprogramInfo proc = findSub(result, "PROC2");
        assertNotNull(proc);

        System.out.println("=== Calls found in PROC2 ===");
        for (CallInfo call : proc.getCalls()) {
            System.out.println("  Call: name=" + call.getName() + " pkg=" + call.getPackageName()
                + " type=" + call.getType() + " args=" + call.getArguments());
        }

        boolean foundFormatName = proc.getCalls().stream()
            .anyMatch(c -> "FORMAT_NAME".equalsIgnoreCase(c.getName()));
        boolean foundGetStatus = proc.getCalls().stream()
            .anyMatch(c -> "GET_STATUS".equalsIgnoreCase(c.getName()));

        assertTrue(foundFormatName, "Should detect util_pkg.format_name() in NVL wrapper");
        assertTrue(foundGetStatus, "Should detect util_pkg.get_status() in DECODE wrapper");
    }

    @Test
    @DisplayName("Function calls in cursor FOR loop with CASE WHEN")
    void testFunctionCallInCursorForLoop() {
        String sql = "CREATE OR REPLACE PACKAGE BODY my_pkg AS\n"
            + "  PROCEDURE proc3 IS\n"
            + "  BEGIN\n"
            + "    FOR rec IN (SELECT col1,\n"
            + "                       CASE WHEN flag = 'Y'\n"
            + "                            THEN validate_pkg.check_rule(col1, col2)\n"
            + "                            ELSE 0\n"
            + "                       END AS result\n"
            + "                FROM data_table\n"
            + "                WHERE validate_pkg.is_active(col1) = 1)\n"
            + "    LOOP\n"
            + "      NULL;\n"
            + "    END LOOP;\n"
            + "  END proc3;\n"
            + "END my_pkg;\n"
            + "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Cursor FOR loop with CASE WHEN function call");
        SubprogramInfo proc = findSub(result, "PROC3");
        assertNotNull(proc);

        System.out.println("=== Calls found in PROC3 ===");
        for (CallInfo call : proc.getCalls()) {
            System.out.println("  Call: name=" + call.getName() + " pkg=" + call.getPackageName()
                + " type=" + call.getType() + " args=" + call.getArguments());
        }

        boolean foundCheckRule = proc.getCalls().stream()
            .anyMatch(c -> "CHECK_RULE".equalsIgnoreCase(c.getName()));
        boolean foundIsActive = proc.getCalls().stream()
            .anyMatch(c -> "IS_ACTIVE".equalsIgnoreCase(c.getName()));

        assertTrue(foundCheckRule, "Should detect validate_pkg.check_rule() in CASE WHEN");
        assertTrue(foundIsActive, "Should detect validate_pkg.is_active() in WHERE clause");
    }

    @Test
    @DisplayName("collection(index).field should NOT be detected as a call (no regression)")
    void testCollectionAccessNotDetectedAsCall() {
        String sql = "CREATE OR REPLACE PACKAGE BODY my_pkg AS\n"
            + "  PROCEDURE proc_coll IS\n"
            + "    TYPE rec_type IS RECORD (name VARCHAR2(100));\n"
            + "    TYPE tab_type IS TABLE OF rec_type INDEX BY BINARY_INTEGER;\n"
            + "    v_tab tab_type;\n"
            + "    v_name VARCHAR2(100);\n"
            + "  BEGIN\n"
            + "    v_name := v_tab(1).name;\n"
            + "  END proc_coll;\n"
            + "END my_pkg;\n"
            + "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Collection access");
        SubprogramInfo proc = findSub(result, "PROC_COLL");
        assertNotNull(proc);

        System.out.println("=== Calls found in PROC_COLL (should not include collection access as pkg call) ===");
        for (CallInfo call : proc.getCalls()) {
            System.out.println("  Call: name=" + call.getName() + " pkg=" + call.getPackageName()
                + " type=" + call.getType());
        }

        // v_tab(1).name should not be detected as a package.function call
        boolean hasFalseCall = proc.getCalls().stream()
            .anyMatch(c -> "NAME".equalsIgnoreCase(c.getName())
                         && "V_TAB".equalsIgnoreCase(c.getPackageName()));
        assertFalse(hasFalseCall, "collection(index).field should NOT be a package.function call");
    }

    @Test
    @DisplayName("Cross-package function call in SQL from standalone procedure")
    void testCrossPackageCallFromStandaloneProc() {
        String sql = "CREATE OR REPLACE PROCEDURE validate_upload IS\n"
            + "  CURSOR cur_data IS\n"
            + "    SELECT empno,\n"
            + "           CASE WHEN valid_type = 'N'\n"
            + "                     AND pg_uwpl_ci_upload.fn_chk_number(datavalue) = 0\n"
            + "                THEN 'INVALID'\n"
            + "                ELSE 'OK'\n"
            + "           END AS status\n"
            + "    FROM upload_staging;\n"
            + "BEGIN\n"
            + "  NULL;\n"
            + "END validate_upload;\n"
            + "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Standalone proc with cursor CASE WHEN");

        ParsedObject po = result.getObjects().get(0);
        System.out.println("=== Calls found in standalone VALIDATE_UPLOAD ===");
        for (CallInfo call : po.getCalls()) {
            System.out.println("  Call: name=" + call.getName() + " pkg=" + call.getPackageName()
                + " type=" + call.getType() + " args=" + call.getArguments());
        }

        boolean found = po.getCalls().stream()
            .anyMatch(c -> "FN_CHK_NUMBER".equalsIgnoreCase(c.getName()));
        assertTrue(found, "Should detect pg_uwpl_ci_upload.fn_chk_number() in standalone proc cursor");
    }

    @Test
    @DisplayName("Nested CASE WHEN with multiple function calls should all be detected")
    void testNestedCaseWhenMultipleFunctions() {
        // Test: function call in SELECT INTO inside BEGIN/END (not cursor)
        String sql = "CREATE OR REPLACE PACKAGE BODY my_pkg AS\n"
            + "  PROCEDURE prc_validate IS\n"
            + "    v_status VARCHAR2(20);\n"
            + "  BEGIN\n"
            + "    SELECT CASE WHEN valid_type = 'N'\n"
            + "                     AND ext_pkg.fn_chk_number(datavalue) = 0\n"
            + "                THEN 'INVALID_NUMBER'\n"
            + "                WHEN valid_type = 'T'\n"
            + "                     AND ext_pkg.fn_chk_tab(valid_tab, col_name, datavalue) = 0\n"
            + "                THEN 'INVALID_TAB'\n"
            + "                ELSE 'OK'\n"
            + "           END\n"
            + "    INTO v_status\n"
            + "    FROM upload_staging\n"
            + "    WHERE rownum = 1;\n"
            + "  END prc_validate;\n"
            + "END my_pkg;\n"
            + "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT INTO with CASE WHEN function calls");
        SubprogramInfo proc = findSub(result, "PRC_VALIDATE");
        assertNotNull(proc, "Should find PRC_VALIDATE subprogram");

        System.out.println("=== Calls found in PRC_VALIDATE (SELECT INTO) ===");
        for (CallInfo call : proc.getCalls()) {
            System.out.println("  Call: name=" + call.getName() + " pkg=" + call.getPackageName()
                + " type=" + call.getType() + " args=" + call.getArguments());
        }

        assertTrue(proc.getCalls().stream()
            .anyMatch(c -> "FN_CHK_NUMBER".equalsIgnoreCase(c.getName())),
            "Should detect ext_pkg.fn_chk_number() in SELECT INTO CASE WHEN");
        assertTrue(proc.getCalls().stream()
            .anyMatch(c -> "FN_CHK_TAB".equalsIgnoreCase(c.getName())),
            "Should detect ext_pkg.fn_chk_tab() in SELECT INTO CASE WHEN");
    }

    @Test
    @DisplayName("Fallback detects calls when keyword identifiers cause parse errors")
    void testFallbackDetectsCallsOnParseError() {
        // date_format is a keyword (DATE_FORMAT token), causing parse errors.
        // The fallback regex scan should still detect the function call.
        String sql = "CREATE OR REPLACE PACKAGE BODY my_pkg AS\n"
            + "  PROCEDURE prc_validate IS\n"
            + "    CURSOR c_data IS\n"
            + "      SELECT empno,\n"
            + "             CASE WHEN ext_pkg.fn_chk(col1, date_format) = 0\n"
            + "                  THEN 'ERR'\n"
            + "                  ELSE 'OK'\n"
            + "             END AS status\n"
            + "      FROM staging;\n"
            + "  BEGIN\n"
            + "    NULL;\n"
            + "  END prc_validate;\n"
            + "END my_pkg;\n"
            + "/\n";
        ParseResult result = parse(sql);
        // We expect parse errors due to date_format keyword
        // but the fallback should still detect the function call

        // Try to find the procedure -- it may or may not be extracted depending on error recovery
        SubprogramInfo proc = findSub(result, "PRC_VALIDATE");
        ParsedObject po = result.getObjects().isEmpty() ? null : result.getObjects().get(0);

        boolean foundCall = false;
        if (proc != null) {
            foundCall = proc.getCalls().stream()
                .anyMatch(c -> "FN_CHK".equalsIgnoreCase(c.getName())
                             && "EXT_PKG".equalsIgnoreCase(c.getPackageName()));
        }
        if (!foundCall && po != null) {
            foundCall = po.getCalls().stream()
                .anyMatch(c -> "FN_CHK".equalsIgnoreCase(c.getName())
                             && "EXT_PKG".equalsIgnoreCase(c.getPackageName()));
        }

        System.out.println("=== Parse errors: " + result.getErrors());
        System.out.println("=== Calls found (fallback test) ===");
        if (proc != null) {
            for (CallInfo call : proc.getCalls()) {
                System.out.println("  Sub call: name=" + call.getName() + " pkg=" + call.getPackageName());
            }
        }
        if (po != null) {
            for (CallInfo call : po.getCalls()) {
                System.out.println("  Obj call: name=" + call.getName() + " pkg=" + call.getPackageName());
            }
        }

        assertTrue(foundCall, "Fallback should detect ext_pkg.fn_chk() even when parse errors occur");
    }

    @Test
    @DisplayName("Three-part schema.pkg.fn call inside SQL expression")
    void testThreePartCallInSqlExpression() {
        String sql = "CREATE OR REPLACE PACKAGE BODY my_pkg AS\n"
            + "  PROCEDURE proc1 IS\n"
            + "    CURSOR c1 IS\n"
            + "      SELECT col1\n"
            + "      FROM my_table\n"
            + "      WHERE app_schema.util_pkg.validate(col1) = 1;\n"
            + "  BEGIN\n"
            + "    NULL;\n"
            + "  END proc1;\n"
            + "END my_pkg;\n"
            + "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Three-part name in SQL");
        SubprogramInfo proc = findSub(result, "PROC1");
        assertNotNull(proc);

        System.out.println("=== Calls found in PROC1 (three-part) ===");
        for (CallInfo call : proc.getCalls()) {
            System.out.println("  Call: name=" + call.getName() + " pkg=" + call.getPackageName()
                + " schema=" + call.getSchema() + " type=" + call.getType() + " args=" + call.getArguments());
        }

        boolean found = proc.getCalls().stream()
            .anyMatch(c -> "VALIDATE".equalsIgnoreCase(c.getName())
                         && "UTIL_PKG".equalsIgnoreCase(c.getPackageName()));
        assertTrue(found, "Should detect app_schema.util_pkg.validate() in WHERE clause");
    }
}
