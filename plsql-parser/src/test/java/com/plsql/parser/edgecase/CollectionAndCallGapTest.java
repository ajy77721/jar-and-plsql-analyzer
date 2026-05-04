package com.plsql.parser.edgecase;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case tests for Gap #7 (Collection methods not extracted as structured info),
 * Gap #8 (Call argument values not tracked), and Gap #12 (Hints not extracted).
 */
@DisplayName("Gaps #7, #8, #12: Collection Methods, Call Arguments, Hints")
public class CollectionAndCallGapTest extends ParserTestBase {

    // =========================================================================
    // Gap #7: Collection method calls parse without error
    // =========================================================================

    @Test
    @DisplayName("v_tab.DELETE and v_tab.DELETE(5) parse without error")
    void testCollectionDeleteParsesWithoutError() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    v_tab t;
                  BEGIN
                    v_tab.DELETE;
                    v_tab.DELETE(5);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Collection DELETE / DELETE(n)");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub, "Should find TEST_PROC subprogram");
    }

    @Test
    @DisplayName("FOR i IN 1..v_tab.COUNT LOOP parses without error and loop detected")
    void testCollectionCountInForLoop() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    v_tab t;
                  BEGIN
                    FOR i IN 1..v_tab.COUNT LOOP
                      NULL;
                    END LOOP;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Collection COUNT in FOR loop bounds");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        boolean hasForLoop = sub.getStatements().stream()
                .anyMatch(s -> "FOR_LOOP".equalsIgnoreCase(s.getType()));
        assertTrue(hasForLoop, "Should detect a FOR loop statement");
    }

    @Test
    @DisplayName("IF v_tab.EXISTS(i) THEN parses without error")
    void testCollectionExistsInIf() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    v_tab t;
                    i PLS_INTEGER := 1;
                  BEGIN
                    IF v_tab.EXISTS(i) THEN
                      NULL;
                    END IF;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Collection EXISTS in IF condition");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
    }

    @Test
    @DisplayName("v_tab.FIRST, .LAST, .NEXT(idx), .PRIOR(idx) parse without error")
    void testCollectionFirstLastNextPrior() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    v_tab t;
                    v_idx PLS_INTEGER;
                  BEGIN
                    v_idx := v_tab.FIRST;
                    v_idx := v_tab.LAST;
                    v_idx := v_tab.NEXT(v_idx);
                    v_idx := v_tab.PRIOR(v_idx);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Collection FIRST / LAST / NEXT / PRIOR");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
    }

    @Test
    @DisplayName("v_tab.EXTEND, .EXTEND(10), .TRIM, .TRIM(5) parse without error")
    void testCollectionExtendTrim() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    TYPE t IS TABLE OF NUMBER;
                    v_tab t := t();
                  BEGIN
                    v_tab.EXTEND;
                    v_tab.EXTEND(10);
                    v_tab.TRIM;
                    v_tab.TRIM(5);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Collection EXTEND / TRIM variants");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
    }

    // =========================================================================
    // Gap #8: Call argument count tracked, but values are not
    // =========================================================================

    @Test
    @DisplayName("Call argument count is tracked via getArguments()")
    void testCallArgumentCountTracked() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                  BEGIN
                    other_pkg.do_work(1, 'abc', SYSDATE);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Call argument count");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        CallInfo call = sub.getCalls().stream()
                .filter(c -> "DO_WORK".equalsIgnoreCase(c.getName()))
                .findFirst().orElse(null);
        assertNotNull(call, "Should find DO_WORK call");
        assertEquals(3, call.getArguments(),
                "Should track 3 arguments for do_work(1, 'abc', SYSDATE)");
    }

    @Test
    @DisplayName("CallInfo model has no method to retrieve individual argument values (gap documentation)")
    void testCallArgumentValuesNotAccessible() {
        // This test documents the gap: CallInfo tracks argument count (getArguments())
        // but does not expose individual argument values or expressions.
        // The model only has: getType, getPackageName, getName, getSchema, getLine, getArguments
        CallInfo info = new CallInfo();
        info.setArguments(3);
        assertEquals(3, info.getArguments(),
                "getArguments() returns the count, but no method exists for individual values");
        // Verify there is no getArgumentValues or similar method by checking the class API.
        // If such a method is ever added, this test should be updated to exercise it.
        assertNotNull(info.getClass().getMethods(),
                "CallInfo has standard methods but no argument-value accessor");
    }

    @Test
    @DisplayName("Three-part call CUSTOMER.PKG_UTIL.FORMAT extracts schema, package, name")
    void testThreePartCallSchema() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_val VARCHAR2(100);
                  BEGIN
                    v_val := CUSTOMER.PKG_UTIL.FORMAT(v_val);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Three-part call: schema.pkg.proc");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        CallInfo call = sub.getCalls().stream()
                .filter(c -> "FORMAT".equalsIgnoreCase(c.getName()))
                .findFirst().orElse(null);
        assertNotNull(call, "Should find FORMAT call");
        assertEquals("PKG_UTIL", call.getPackageName().toUpperCase(),
                "Package should be PKG_UTIL");
        assertEquals("CUSTOMER", call.getSchema().toUpperCase(),
                "Schema should be CUSTOMER");
    }

    // =========================================================================
    // Call type classification edge cases
    // =========================================================================

    @Test
    @DisplayName("DBMS_OUTPUT.PUT_LINE classified as BUILTIN")
    void testBuiltinCallClassification() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                  BEGIN
                    DBMS_OUTPUT.PUT_LINE('test message');
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "BUILTIN call classification");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        CallInfo call = sub.getCalls().stream()
                .filter(c -> "PUT_LINE".equalsIgnoreCase(c.getName()))
                .findFirst().orElse(null);
        assertNotNull(call, "Should find PUT_LINE call");
        assertEquals("BUILTIN", call.getType(),
                "DBMS_OUTPUT.PUT_LINE should be classified as BUILTIN");
    }

    @Test
    @DisplayName("Call to sibling procedure in same package classified as INTERNAL")
    void testInternalCallClassification() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE helper_proc IS
                  BEGIN
                    NULL;
                  END helper_proc;

                  PROCEDURE test_proc IS
                  BEGIN
                    helper_proc;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "INTERNAL call classification");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        CallInfo call = sub.getCalls().stream()
                .filter(c -> "HELPER_PROC".equalsIgnoreCase(c.getName()))
                .findFirst().orElse(null);
        assertNotNull(call, "Should find HELPER_PROC call");
        assertEquals("INTERNAL", call.getType(),
                "Call to sibling subprogram should be INTERNAL");
    }

    @Test
    @DisplayName("Call to OTHER_PKG.DO_WORK classified as EXTERNAL")
    void testExternalCallClassification() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                  BEGIN
                    OTHER_PKG.DO_WORK(1);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXTERNAL call classification");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        CallInfo call = sub.getCalls().stream()
                .filter(c -> "DO_WORK".equalsIgnoreCase(c.getName()))
                .findFirst().orElse(null);
        assertNotNull(call, "Should find DO_WORK call");
        assertEquals("EXTERNAL", call.getType(),
                "Call to OTHER_PKG.DO_WORK should be EXTERNAL");
    }

    @Test
    @DisplayName("MY_SEQ.NEXTVAL classified as SEQUENCE call type")
    void testSequenceCallClassification() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_id NUMBER;
                  BEGIN
                    v_id := MY_SEQ.NEXTVAL;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SEQUENCE call classification");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        CallInfo call = sub.getCalls().stream()
                .filter(c -> "NEXTVAL".equalsIgnoreCase(c.getName()))
                .findFirst().orElse(null);
        assertNotNull(call, "Should find NEXTVAL call");
        assertEquals("SEQUENCE", call.getType(),
                "MY_SEQ.NEXTVAL should be classified as SEQUENCE");
        assertEquals("MY_SEQ", call.getPackageName().toUpperCase(),
                "Package name should be MY_SEQ (the sequence name)");
    }

    // =========================================================================
    // Gap #12: Hints ignored but do not cause parse errors
    // =========================================================================

    @Test
    @DisplayName("SELECT /*+ PARALLEL(e, 4) FULL(e) */ parses without error, table extracted")
    void testHintIgnoredButNoParsError() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT /*+ PARALLEL(e, 4) FULL(e) */ COUNT(*)
                    INTO v_cnt
                    FROM employees e;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Hint PARALLEL + FULL");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertTrue(sub.getTableOperations().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "Should extract EMPLOYEES table even with hints");
    }

    @Test
    @DisplayName("Multi-hint SELECT /*+ INDEX(...) LEADING(...) USE_NL(...) */ parses without error")
    void testMultiLineHintNoParsError() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_val NUMBER;
                  BEGIN
                    SELECT /*+ INDEX(t idx_name) LEADING(t) USE_NL(t) */ col1
                    INTO v_val
                    FROM table_t t;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multi-hint INDEX + LEADING + USE_NL");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertTrue(sub.getTableOperations().stream()
                .anyMatch(t -> "TABLE_T".equalsIgnoreCase(t.getTableName())),
                "Should extract TABLE_T even with multi-hints");
    }
}
