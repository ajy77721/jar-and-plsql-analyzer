package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Call Classification: Internal / External / Builtin")
public class CallClassificationTest extends ParserTestBase {

    @Test
    @DisplayName("Bare function call to sibling subprogram classified as INTERNAL")
    void testBareCallClassifiedAsInternal() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  FUNCTION HELPER RETURN NUMBER IS
                  BEGIN
                    RETURN 1;
                  END HELPER;

                  PROCEDURE MAIN_PROC IS
                    v_val NUMBER;
                  BEGIN
                    v_val := HELPER();
                  END MAIN_PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo main = findSub(result, "MAIN_PROC");
        assertNotNull(main);
        boolean foundInternal = false;
        if (main.getCalls() != null) {
            for (CallInfo call : main.getCalls()) {
                if ("HELPER".equalsIgnoreCase(call.getName())) {
                    assertEquals("INTERNAL", call.getType());
                    foundInternal = true;
                }
            }
        }
        assertTrue(foundInternal, "Should find HELPER call as INTERNAL");
    }

    @Test
    @DisplayName("Package-prefixed call classified as EXTERNAL")
    void testPackagePrefixedCallExternal() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE MAIN_PROC IS
                  BEGIN
                    OTHER_PKG.DO_SOMETHING(1);
                  END MAIN_PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo main = findSub(result, "MAIN_PROC");
        assertNotNull(main);
        boolean found = false;
        if (main.getCalls() != null) {
            for (CallInfo call : main.getCalls()) {
                if ("DO_SOMETHING".equalsIgnoreCase(call.getName())) {
                    assertEquals("EXTERNAL", call.getType());
                    assertEquals("OTHER_PKG", call.getPackageName().toUpperCase());
                    found = true;
                }
            }
        }
        assertTrue(found, "Should find OTHER_PKG.DO_SOMETHING as EXTERNAL");
    }

    @Test
    @DisplayName("Three-part name call: schema.package.procedure")
    void testThreePartNameCall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    CUSTOMER.OTHER_PKG.DO_WORK(1, 'A');
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Three-part name");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertFalse(proc.getCalls().isEmpty());
    }

    @Test
    @DisplayName("DBMS_OUTPUT and UTL_FILE calls classified as BUILTIN")
    void testBuiltinPackageCalls() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_file UTL_FILE.FILE_TYPE;
                  BEGIN
                    DBMS_OUTPUT.PUT_LINE('start');
                    v_file := UTL_FILE.FOPEN('/tmp', 'out.txt', 'w');
                    UTL_FILE.PUT_LINE(v_file, 'data');
                    UTL_FILE.FCLOSE(v_file);
                    DBMS_LOB.CREATETEMPORARY(NULL, TRUE);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "DBMS/UTL calls");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getCalls().stream()
                .anyMatch(c -> "BUILTIN".equalsIgnoreCase(c.getType())));
    }

    @Test
    @DisplayName("Large package: mixed INTERNAL and EXTERNAL calls detected")
    void testMixedInternalExternalCalls() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY BIG_PKG AS
                  PROCEDURE P1 IS BEGIN INSERT INTO t1 VALUES (1); END P1;
                  PROCEDURE P2 (p_id NUMBER) IS BEGIN UPDATE t1 SET v = p_id; END P2;
                  FUNCTION F1 RETURN NUMBER IS BEGIN RETURN 1; END F1;
                  FUNCTION F2 (p_name VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN p_name; END F2;
                  PROCEDURE P3 IS
                    v_x NUMBER;
                  BEGIN
                    v_x := F1();
                    P2(v_x);
                    OTHER_PKG.REMOTE_PROC(v_x);
                    SELECT COUNT(*) INTO v_x FROM employees;
                    DELETE FROM temp_data WHERE id < v_x;
                  END P3;
                END BIG_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Large package body");
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getSubprograms().size() >= 5);

        SubprogramInfo p3 = findSub(result, "P3");
        assertNotNull(p3);
        assertTrue(p3.getCalls().size() >= 3);
        assertTrue(p3.getTableOperations().size() >= 2);
        assertTrue(p3.getCalls().stream().anyMatch(c -> "INTERNAL".equalsIgnoreCase(c.getType())));
        assertTrue(p3.getCalls().stream().anyMatch(c -> "EXTERNAL".equalsIgnoreCase(c.getType())));
    }
}
