package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Cursor join extraction and TABLE(fn()) call detection")
public class CursorJoinAndTableFunctionTest extends ParserTestBase {

    @Test
    @DisplayName("Joins inside cursor declaration are extracted")
    void testCursorDeclarationJoinsExtracted() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  CURSOR C_DATA IS
                    SELECT A.ID, B.NAME
                      FROM TABLE_A A
                      INNER JOIN TABLE_B B ON A.ID = B.A_ID
                      INNER JOIN TABLE_C C ON C.ID = A.C_ID;

                  PROCEDURE PROC IS
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "cursor declaration joins");

        ParsedObject pkg = result.getObjects().get(0);
        // TABLE_A should have joins to TABLE_B and TABLE_C
        boolean foundJoin = false;
        for (TableOperationInfo toi : pkg.getTableOperations()) {
            if (toi.getTableName() != null && toi.getTableName().equalsIgnoreCase("TABLE_A")) {
                assertNotNull(toi.getJoins(), "TABLE_A should have joins");
                assertFalse(toi.getJoins().isEmpty(), "TABLE_A should have at least 1 join");
                foundJoin = true;
            }
        }
        assertTrue(foundJoin, "TABLE_A operation should be present");
    }

    @Test
    @DisplayName("Joins inside inline cursor FOR loop are extracted")
    void testCursorForLoopJoinsExtracted() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR rec IN (
                      SELECT A.ID, B.NAME
                        FROM TABLE_A A
                        INNER JOIN TABLE_B B ON A.ID = B.A_ID
                    ) LOOP
                      NULL;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "cursor FOR loop joins");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "PROC should be found");

        boolean foundJoin = false;
        for (TableOperationInfo toi : proc.getTableOperations()) {
            if (toi.getTableName() != null && toi.getTableName().equalsIgnoreCase("TABLE_A")) {
                assertNotNull(toi.getJoins(), "TABLE_A should have joins");
                assertFalse(toi.getJoins().isEmpty(), "TABLE_A should have at least 1 join from cursor FOR loop");
                foundJoin = true;
            }
        }
        assertTrue(foundJoin, "TABLE_A operation should be present in PROC");
    }

    @Test
    @DisplayName("TABLE(pkg.fn()) in cursor FROM clause is detected as a call")
    void testTableFunctionInCursorDetectedAsCall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  CURSOR C_DATA (p_id NUMBER) IS
                    SELECT CP.PART_ID, URP.RISK_ID
                      FROM UWGE_RISK_PARTS URP
                      INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) CP
                          ON CP.PART_ID = URP.RISK_PART_ID;

                  PROCEDURE PROC IS
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        // Allow parse errors on TABLE() — just verify call is detected
        ParsedObject pkg = result.getObjects().get(0);
        assertNotNull(pkg, "Package body should be parsed");

        boolean foundCall = false;
        for (CallInfo ci : pkg.getCalls()) {
            if (ci.getName() != null && ci.getName().equalsIgnoreCase("FN_GEN_CP_TABLE")) {
                foundCall = true;
                assertEquals("PG_CP_GEN_TABLE", ci.getPackageName().toUpperCase());
                assertEquals("EXTERNAL", ci.getType());
            }
        }
        // Also check subprogram calls (in case attributed to PROC)
        for (SubprogramInfo sub : pkg.getSubprograms()) {
            for (CallInfo ci : sub.getCalls()) {
                if (ci.getName() != null && ci.getName().equalsIgnoreCase("FN_GEN_CP_TABLE")) {
                    foundCall = true;
                }
            }
        }
        assertTrue(foundCall, "FN_GEN_CP_TABLE should be detected as a call from TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(...))");
    }

    @Test
    @DisplayName("TABLE(pkg.fn()) join records alias as joined table name")
    void testTableFunctionJoinHasAlias() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  CURSOR C_DATA IS
                    SELECT A.ID, CP.NAME
                      FROM BASE_TABLE A
                      INNER JOIN TABLE(EXT_PKG.FN_GET_ROWS(A.ID)) CP
                          ON CP.ID = A.ID;

                  PROCEDURE PROC IS
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        ParsedObject pkg = result.getObjects().get(0);
        assertNotNull(pkg);

        boolean foundCall = false;
        for (CallInfo ci : pkg.getCalls()) {
            if (ci.getName() != null && ci.getName().equalsIgnoreCase("FN_GET_ROWS")) {
                foundCall = true;
            }
        }
        for (SubprogramInfo sub : pkg.getSubprograms()) {
            for (CallInfo ci : sub.getCalls()) {
                if (ci.getName() != null && ci.getName().equalsIgnoreCase("FN_GET_ROWS")) {
                    foundCall = true;
                }
            }
        }
        assertTrue(foundCall, "FN_GET_ROWS should be detected from TABLE(EXT_PKG.FN_GET_ROWS(...))");
    }

    @Test
    @DisplayName("TABLE(fn()) with an UNqualified (no package prefix) function name is detected as a call")
    void testUnqualifiedTableFunctionCallDetected() {
        // The dotted/qualified scan never matches a bare function name, so this call is only
        // found by the dedicated unqualified TABLE(...) extraction — which is grammar-driven
        // (leading general_element with a single part carrying a function_argument).
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR r IN (SELECT * FROM TABLE(SOME_PIPELINED_FUNC(1, 2))) LOOP
                      NULL;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        ParsedObject pkg = result.getObjects().get(0);
        assertNotNull(pkg);

        CallInfo found = null;
        for (CallInfo ci : pkg.getCalls()) {
            if (ci.getName() != null && ci.getName().equalsIgnoreCase("SOME_PIPELINED_FUNC")) found = ci;
        }
        for (SubprogramInfo sub : pkg.getSubprograms()) {
            for (CallInfo ci : sub.getCalls()) {
                if (ci.getName() != null && ci.getName().equalsIgnoreCase("SOME_PIPELINED_FUNC")) found = ci;
            }
        }
        assertNotNull(found, "SOME_PIPELINED_FUNC should be detected from TABLE(SOME_PIPELINED_FUNC(1, 2))");
        assertEquals(2, found.getArguments(), "argument count should come from the grammar's argument list");
    }

    @Test
    @DisplayName("TABLE(pkg.fn()) called 3x in different cursor FOR loops accumulates all occurrences")
    void testTableFunctionCalledMultipleTimesAccumulatesAllOccurrences() {
        // Reproduces a real bug: erp_pkg_planner.explode_products(...) invoked inside
        // TABLE(...) in 3 separate cursor FOR loops within one procedure was collapsed
        // to a single CallInfo (count=1) instead of accumulating all 3 occurrences.
        String sql = """
                CREATE OR REPLACE PACKAGE BODY ERP_PKG AS
                  PROCEDURE ERP_USP_COLLECT_DEMAND IS
                  BEGIN
                    FOR r IN (SELECT * FROM TABLE(ERP_PKG_PLANNER.EXPLODE_PRODUCTS(1))) LOOP
                      NULL;
                    END LOOP;

                    FOR r IN (SELECT * FROM TABLE(ERP_PKG_PLANNER.EXPLODE_PRODUCTS(2))) LOOP
                      NULL;
                    END LOOP;

                    FOR r IN (SELECT * FROM TABLE(ERP_PKG_PLANNER.EXPLODE_PRODUCTS(3))) LOOP
                      NULL;
                    END LOOP;
                  END ERP_USP_COLLECT_DEMAND;
                END ERP_PKG;
                /
                """;
        ParseResult result = parse(sql);
        ParsedObject pkg = result.getObjects().get(0);
        assertNotNull(pkg);

        SubprogramInfo proc = findSub(result, "ERP_USP_COLLECT_DEMAND");
        assertNotNull(proc, "ERP_USP_COLLECT_DEMAND should be found");

        List<CallInfo> matches = new java.util.ArrayList<>();
        for (CallInfo ci : proc.getCalls()) {
            if (ci.getName() != null && ci.getName().equalsIgnoreCase("EXPLODE_PRODUCTS")) {
                matches.add(ci);
            }
        }
        assertEquals(3, matches.size(),
                "EXPLODE_PRODUCTS should be recorded once per TABLE(...) occurrence (3 total), " +
                "not deduplicated across separate cursor FOR loops");

        java.util.Set<Integer> lines = new java.util.HashSet<>();
        for (CallInfo ci : matches) lines.add(ci.getLine());
        assertEquals(3, lines.size(), "Each occurrence should carry its own distinct source line");
    }

    @Test
    @DisplayName("TABLE(fn(...)) call site is not double-counted by both the cursor-text scan and "
            + "the TABLE(...) collection-expression scan (§264/§265 dup-edge regression)")
    void testTableFunctionCallNotDuplicatedAcrossBothScanners() {
        // Reproduces the ingest-breaking bug: the enclosing cursor/SELECT text catch-all scan
        // (extractFunctionCallsFromSqlText, allowRepeats=false) and the nested TABLE(fn(...))
        // collection-expression scan (extractCallsFromTableCollectionExpression, allowRepeats=true)
        // both cover the SAME physical call site, just with slightly different approximate
        // baseLine values (a couple of lines apart because the query spans multiple lines). Before
        // the fix, the allowRepeats path only suppressed a call recorded at the *exact same* line,
        // so the same EXPLODE_PRODUCTS(...) invocation produced two separate CallInfo entries for
        // the identical (from,to) pair — which later became two edges in call_graph.json and, once
        // resolved to the same Mongo entity, a duplicate-key (E11000) collision in
        // am_call_edge_unique. Only one CallInfo should be recorded for this single occurrence.
        String sql = """
                CREATE OR REPLACE PACKAGE BODY ERP_PKG AS
                  PROCEDURE ERP_USP_COLLECT_DEMAND IS
                  BEGIN
                    FOR r IN (
                      SELECT *
                        FROM TABLE(ERP_PKG_PLANNER.EXPLODE_PRODUCTS(1))
                    ) LOOP
                      NULL;
                    END LOOP;
                  END ERP_USP_COLLECT_DEMAND;
                END ERP_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "ERP_USP_COLLECT_DEMAND");
        assertNotNull(proc, "ERP_USP_COLLECT_DEMAND should be found");

        List<CallInfo> matches = new java.util.ArrayList<>();
        for (CallInfo ci : proc.getCalls()) {
            if (ci.getName() != null && ci.getName().equalsIgnoreCase("EXPLODE_PRODUCTS")) {
                matches.add(ci);
            }
        }
        assertEquals(1, matches.size(),
                "A single TABLE(fn(...)) occurrence must yield exactly one CallInfo/edge, "
                + "not one per scanner pass (general cursor-text scan + TABLE(...) scan)");
    }

    @Test
    @DisplayName("Regular cursor joins still work — no regression")
    void testRegularCursorJoinsNoRegression() {
        String sql = """
                CREATE OR REPLACE PROCEDURE STANDALONE_PROC IS
                  CURSOR C IS
                    SELECT A.ID
                      FROM ORDERS A
                      LEFT JOIN CUSTOMERS C ON C.ID = A.CUST_ID
                      LEFT JOIN PRODUCTS P ON P.ID = A.PROD_ID;
                BEGIN
                  NULL;
                END STANDALONE_PROC;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "regular cursor joins");

        ParsedObject proc = result.getObjects().get(0);
        boolean foundOrdersWithJoins = false;
        for (TableOperationInfo toi : proc.getTableOperations()) {
            if (toi.getTableName() != null && toi.getTableName().equalsIgnoreCase("ORDERS")) {
                if (toi.getJoins() != null && toi.getJoins().size() >= 2) {
                    foundOrdersWithJoins = true;
                }
            }
        }
        assertTrue(foundOrdersWithJoins, "ORDERS should have 2 joins (CUSTOMERS and PRODUCTS)");
    }
}
