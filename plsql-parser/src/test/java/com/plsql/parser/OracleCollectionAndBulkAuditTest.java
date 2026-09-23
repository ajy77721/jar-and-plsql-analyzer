package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * READ-ONLY AUDIT (no source fixes). Evidence-collecting test for Oracle
 * collection types (VARRAY / TABLE OF ... INDEX BY), collection methods
 * (.COUNT, .EXTEND, .DELETE, .EXISTS), BULK COLLECT INTO, FORALL (with
 * INDICES OF), and Oracle's lack of multi-row VALUES syntax.
 *
 * This test intentionally does not use strict assertions everywhere —
 * many methods print observed vs. expected output to stdout so a human
 * (or CI log) can see actual behavior even where the "expected" outcome
 * legitimately fails. Some assertions are loose (assertTrue/assertFalse)
 * specifically so a bug shows as a failing test rather than being masked.
 */
public class OracleCollectionAndBulkAuditTest extends ParserTestBase {

    private static void dump(String label, StatementInfo si) {
        if (si == null) {
            System.out.println("[" + label + "] statement NOT FOUND");
            return;
        }
        System.out.println("[" + label + "] type=" + si.getType()
                + " collectionName=" + si.getCollectionName()
                + " limitValue=" + si.getLimitValue()
                + " dmlType=" + si.getDmlType()
                + " indexVariable=" + si.getIndexVariable()
                + " boundsExpression=" + si.getBoundsExpression()
                + " sqlText=" + si.getSqlText());
    }

    private static void dumpTableOps(String label, SubprogramInfo sub) {
        List<TableOperationInfo> ops = sub.getTableOperations();
        System.out.println("[" + label + "] tableOperations=" + ops.size());
        for (TableOperationInfo t : ops) {
            System.out.println("    table=" + t.getTableName() + " op=" + t.getOperation()
                    + " line=" + t.getLine());
        }
    }

    private static void dumpCalls(String label, SubprogramInfo sub) {
        List<CallInfo> calls = sub.getCalls();
        System.out.println("[" + label + "] calls=" + calls.size());
        for (CallInfo c : calls) {
            System.out.println("    pkg=" + c.getPackageName() + " name=" + c.getName());
        }
    }

    // =====================================================================
    // Concept 1: Collection types + .COUNT / .DELETE / .EXISTS classification
    // =====================================================================

    @Test
    @DisplayName("1a. VARRAY declaration + .COUNT usage — must not be a call/table")
    void testVarrayCount() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_arr IS VARRAY(10) OF NUMBER;
                    v_arr t_arr := t_arr(1,2,3);
                    v_cnt NUMBER;
                  BEGIN
                    v_cnt := v_arr.COUNT;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "VARRAY .COUNT");
        SubprogramInfo proc = findSub(result, "PROC");
        dumpCalls("VARRAY.COUNT", proc);
        dumpTableOps("VARRAY.COUNT", proc);
        boolean misclassifiedAsCall = proc.getCalls().stream()
                .anyMatch(c -> "COUNT".equalsIgnoreCase(c.getName()) || "V_ARR".equalsIgnoreCase(c.getName()));
        boolean misclassifiedAsTable = proc.getTableOperations().stream()
                .anyMatch(t -> "V_ARR".equalsIgnoreCase(t.getTableName()));
        System.out.println("VARRAY.COUNT misclassifiedAsCall=" + misclassifiedAsCall
                + " misclassifiedAsTable=" + misclassifiedAsTable);
        org.junit.jupiter.api.Assertions.assertFalse(misclassifiedAsCall,
                "v_arr.COUNT should not be recorded as a CallInfo (external call)");
        org.junit.jupiter.api.Assertions.assertFalse(misclassifiedAsTable,
                "v_arr.COUNT should not be recorded as a TableOperationInfo");
    }

    @Test
    @DisplayName("1b. Associative array (INDEX BY) .DELETE / .EXISTS classification")
    void testAssocArrayDeleteExists() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_map IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    v_map t_map;
                    v_found BOOLEAN;
                  BEGIN
                    v_map(1) := 100;
                    IF v_map.EXISTS(1) THEN
                      v_found := TRUE;
                    END IF;
                    v_map.DELETE(1);
                    v_map.DELETE;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "assoc-array .EXISTS/.DELETE");
        SubprogramInfo proc = findSub(result, "PROC");
        dumpCalls("ASSOC.DELETE/EXISTS", proc);
        dumpTableOps("ASSOC.DELETE/EXISTS", proc);
        boolean misclassifiedAsCall = proc.getCalls().stream()
                .anyMatch(c -> "V_MAP".equalsIgnoreCase(c.getPackageName())
                        || "V_MAP".equalsIgnoreCase(c.getName()));
        boolean misclassifiedAsTable = proc.getTableOperations().stream()
                .anyMatch(t -> "V_MAP".equalsIgnoreCase(t.getTableName()));
        System.out.println("ASSOC.DELETE/EXISTS misclassifiedAsCall=" + misclassifiedAsCall
                + " misclassifiedAsTable=" + misclassifiedAsTable);
        org.junit.jupiter.api.Assertions.assertFalse(misclassifiedAsCall,
                "v_map.EXISTS/DELETE should not be recorded as an external CallInfo");
        org.junit.jupiter.api.Assertions.assertFalse(misclassifiedAsTable,
                "v_map.EXISTS/DELETE should not be recorded as a TableOperationInfo");
    }

    @Test
    @DisplayName("1c. Nested table .EXTEND regression check (reportedly already fixed)")
    void testNestedTableExtend() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    v_ids t_ids := t_ids();
                  BEGIN
                    v_ids.EXTEND;
                    v_ids.EXTEND(5);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "nested table .EXTEND");
        SubprogramInfo proc = findSub(result, "PROC");
        dumpCalls("EXTEND", proc);
        boolean misclassifiedAsCall = proc.getCalls().stream()
                .anyMatch(c -> "V_IDS".equalsIgnoreCase(c.getPackageName())
                        || "V_IDS".equalsIgnoreCase(c.getName()));
        System.out.println("EXTEND misclassifiedAsCall=" + misclassifiedAsCall);
        org.junit.jupiter.api.Assertions.assertFalse(misclassifiedAsCall,
                "v_ids.EXTEND should not be recorded as an external call (regression)");
    }

    @Test
    @DisplayName("1d. Package-level collection global referenced from a DIFFERENT procedure in the same package body")
    void testPackageLevelCollectionCrossProcedure() {
        String sql = """
                CREATE OR REPLACE PACKAGE MY_PKG AS
                  PROCEDURE LOAD_IDS;
                  PROCEDURE USE_IDS;
                END MY_PKG;
                /
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  TYPE t_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                  g_ids t_ids;

                  PROCEDURE LOAD_IDS IS
                  BEGIN
                    g_ids(1) := 10;
                    g_ids(2) := 20;
                  END LOAD_IDS;

                  PROCEDURE USE_IDS IS
                    v_cnt NUMBER;
                  BEGIN
                    v_cnt := g_ids.COUNT;
                    IF g_ids.EXISTS(1) THEN
                      g_ids.DELETE(1);
                    END IF;
                  END USE_IDS;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "package-level collection global cross-procedure");
        SubprogramInfo useIds = findSub(result, "USE_IDS");
        org.junit.jupiter.api.Assertions.assertNotNull(useIds, "USE_IDS should be found");
        dumpCalls("USE_IDS (cross-proc g_ids)", useIds);
        dumpTableOps("USE_IDS (cross-proc g_ids)", useIds);
        boolean misclassifiedAsCall = useIds.getCalls().stream()
                .anyMatch(c -> "G_IDS".equalsIgnoreCase(c.getPackageName())
                        || "G_IDS".equalsIgnoreCase(c.getName()));
        boolean misclassifiedAsTable = useIds.getTableOperations().stream()
                .anyMatch(t -> "G_IDS".equalsIgnoreCase(t.getTableName()));
        System.out.println("cross-proc g_ids misclassifiedAsCall=" + misclassifiedAsCall
                + " misclassifiedAsTable=" + misclassifiedAsTable);
        org.junit.jupiter.api.Assertions.assertFalse(misclassifiedAsCall,
                "g_ids.COUNT/EXISTS/DELETE used from a different procedure than the one it was "
                        + "first written in should still resolve as the package-level collection global, "
                        + "not an external call");
        org.junit.jupiter.api.Assertions.assertFalse(misclassifiedAsTable,
                "g_ids used from a different procedure should not become a table operation");
    }

    // =====================================================================
    // Concept 2: BULK COLLECT INTO / FORALL
    // =====================================================================

    @Test
    @DisplayName("2a. SELECT ... BULK COLLECT INTO captures real table + collection")
    void testSelectBulkCollectInto() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    v_ids t_ids;
                  BEGIN
                    SELECT id BULK COLLECT INTO v_ids FROM tab;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT BULK COLLECT INTO");
        SubprogramInfo proc = findSub(result, "PROC");
        StatementInfo bulkStmt = proc.getStatements().stream()
                .filter(s -> "BULK_COLLECT".equals(s.getType()) || (s.getSqlText() != null && s.getSqlText().contains("BULK COLLECT")))
                .findFirst().orElse(null);
        dump("SELECT-BULK-COLLECT stmt", bulkStmt);
        dumpTableOps("SELECT-BULK-COLLECT", proc);
        boolean tabCaptured = proc.getTableOperations().stream()
                .anyMatch(t -> "TAB".equalsIgnoreCase(t.getTableName()));
        System.out.println("SELECT-BULK-COLLECT tabCaptured=" + tabCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(tabCaptured,
                "The real table TAB in 'SELECT ... BULK COLLECT INTO v_ids FROM tab' must be "
                        + "recorded as a table operation");
    }

    @Test
    @DisplayName("2b. FETCH ... BULK COLLECT INTO with cursor — collection name captured, no false table")
    void testFetchBulkCollectInto() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS SELECT id FROM real_table;
                    TYPE t_ids IS TABLE OF NUMBER;
                    v_ids t_ids;
                  BEGIN
                    OPEN c1;
                    FETCH c1 BULK COLLECT INTO v_ids;
                    CLOSE c1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FETCH BULK COLLECT INTO");
        SubprogramInfo proc = findSub(result, "PROC");
        StatementInfo bulk = proc.getStatements().stream()
                .filter(s -> "BULK_COLLECT".equals(s.getType()))
                .findFirst().orElse(null);
        dump("FETCH-BULK-COLLECT", bulk);
        dumpTableOps("FETCH-BULK-COLLECT", proc);
        org.junit.jupiter.api.Assertions.assertNotNull(bulk);
        org.junit.jupiter.api.Assertions.assertEquals("V_IDS", bulk.getCollectionName() == null ? null
                : bulk.getCollectionName().toUpperCase());
        boolean collectionMisclassifiedAsTable = proc.getTableOperations().stream()
                .anyMatch(t -> "V_IDS".equalsIgnoreCase(t.getTableName()));
        org.junit.jupiter.api.Assertions.assertFalse(collectionMisclassifiedAsTable,
                "v_ids collection must not appear as a table operation");
    }

    @Test
    @DisplayName("2c. FORALL i IN 1..arr.COUNT INSERT — real table captured, arr.COUNT not a table/call")
    void testForallCountBound() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    v_ids t_ids := t_ids(1,2,3);
                  BEGIN
                    FORALL i IN 1..v_ids.COUNT
                      INSERT INTO real_tab VALUES (v_ids(i));
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FORALL 1..arr.COUNT INSERT");
        SubprogramInfo proc = findSub(result, "PROC");
        dumpTableOps("FORALL-COUNT-BOUND", proc);
        dumpCalls("FORALL-COUNT-BOUND", proc);
        boolean realTableCaptured = proc.getTableOperations().stream()
                .anyMatch(t -> "REAL_TAB".equalsIgnoreCase(t.getTableName()) && "INSERT".equalsIgnoreCase(t.getOperation()));
        boolean countMisclassifiedAsTable = proc.getTableOperations().stream()
                .anyMatch(t -> "V_IDS".equalsIgnoreCase(t.getTableName()));
        boolean countMisclassifiedAsCall = proc.getCalls().stream()
                .anyMatch(c -> "V_IDS".equalsIgnoreCase(c.getPackageName()) || "V_IDS".equalsIgnoreCase(c.getName()));
        System.out.println("realTableCaptured=" + realTableCaptured
                + " countMisclassifiedAsTable=" + countMisclassifiedAsTable
                + " countMisclassifiedAsCall=" + countMisclassifiedAsCall);
        org.junit.jupiter.api.Assertions.assertTrue(realTableCaptured,
                "real_tab INSERT inside FORALL must be captured as a table operation");
        org.junit.jupiter.api.Assertions.assertFalse(countMisclassifiedAsTable,
                "v_ids.COUNT bound expression must not be recorded as a table operation");
        org.junit.jupiter.api.Assertions.assertFalse(countMisclassifiedAsCall,
                "v_ids.COUNT bound expression must not be recorded as an external call");
    }

    @Test
    @DisplayName("2d. FORALL i IN INDICES OF arr SAVE EXCEPTIONS INSERT — real table captured")
    void testForallIndicesOfSaveExceptions() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    v_ids t_ids;
                  BEGIN
                    v_ids(1) := 10;
                    v_ids(5) := 50;
                    FORALL i IN INDICES OF v_ids SAVE EXCEPTIONS
                      INSERT INTO real_tab2 VALUES (v_ids(i));
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FORALL INDICES OF ... SAVE EXCEPTIONS");
        SubprogramInfo proc = findSub(result, "PROC");
        dumpTableOps("FORALL-INDICES-OF", proc);
        StatementInfo forall = proc.getStatements().stream()
                .filter(s -> "FORALL".equals(s.getType()))
                .findFirst().orElse(null);
        dump("FORALL-INDICES-OF stmt", forall);
        boolean realTableCaptured = proc.getTableOperations().stream()
                .anyMatch(t -> "REAL_TAB2".equalsIgnoreCase(t.getTableName()) && "INSERT".equalsIgnoreCase(t.getOperation()));
        boolean indicesMisclassifiedAsTable = proc.getTableOperations().stream()
                .anyMatch(t -> "V_IDS".equalsIgnoreCase(t.getTableName()));
        System.out.println("realTableCaptured=" + realTableCaptured
                + " indicesMisclassifiedAsTable=" + indicesMisclassifiedAsTable);
        org.junit.jupiter.api.Assertions.assertNotNull(forall, "FORALL statement should be recorded");
        org.junit.jupiter.api.Assertions.assertEquals("INSERT", forall == null ? null : forall.getDmlType());
        org.junit.jupiter.api.Assertions.assertTrue(realTableCaptured,
                "real_tab2 INSERT inside FORALL ... INDICES OF must be captured as a table operation");
        org.junit.jupiter.api.Assertions.assertFalse(indicesMisclassifiedAsTable,
                "v_ids in INDICES OF v_ids must not be recorded as a table operation");
    }

    // =====================================================================
    // Concept 3: Oracle multi-row VALUES batch insert — grammar support check
    // =====================================================================

    @Test
    @DisplayName("3. Grammar does not support multi-VALUES INSERT INTO tab VALUES (1,'a'),(2,'b') — documented, not fabricated")
    void testMultiRowValuesNotSupportedByGrammar() {
        // values_clause in PlSqlParser.g4 is defined as:
        //   values_clause : VALUES (REGULAR_ID | '(' expressions_ ')' | collection_expression) ;
        // There is no repetition/comma-separated group of parenthesized tuples, so the grammar
        // has no rule that accepts INSERT INTO tab VALUES (1,'a'), (2,'b'); at all (this matches
        // real Oracle SQL, which likewise does not support multi-row VALUES the way MySQL/Postgres do).
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    INSERT INTO tab (id, name) VALUES (1, 'a'), (2, 'b');
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        List<String> errors = result.getErrors();
        System.out.println("Multi-VALUES INSERT parse errors: " + errors);
        // We expect this to fail/produce parse errors since the grammar has no rule for it.
        // This is not a bug: Oracle PL/SQL genuinely does not support this syntax either.
        org.junit.jupiter.api.Assertions.assertTrue(errors != null && !errors.isEmpty(),
                "Multi-row VALUES (...),(...) is not valid Oracle SQL and the grammar has no "
                        + "values_clause alternative for it either — expected parse errors, confirming "
                        + "concept 3 does not apply to this grammar (correctly, since Oracle doesn't support it).");
    }
}
