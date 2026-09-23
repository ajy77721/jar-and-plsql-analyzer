package com.plsql.parser.edgecase;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Accuracy Stress Tests: Deep Nesting, Many Joins, Large Procedures")
public class AccuracyStressTest extends ParserTestBase {

    // ---------------------------------------------------------------
    // Helper: collect all table names (upper-cased) from a subprogram
    // ---------------------------------------------------------------
    private Set<String> tableNames(SubprogramInfo sub) {
        return sub.getTableOperations().stream()
                .map(t -> t.getTableName().toUpperCase())
                .collect(Collectors.toSet());
    }

    // ---------------------------------------------------------------
    // Helper: collect all JoinInfo entries from a subprogram
    // ---------------------------------------------------------------
    private List<JoinInfo> allJoins(SubprogramInfo sub) {
        return sub.getTableOperations().stream()
                .flatMap(t -> t.getJoins().stream())
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------
    // Helper: collect all sequence names from the dependency summary
    // ---------------------------------------------------------------
    private Set<String> getSequences(ParseResult result) {
        Set<String> seqs = new java.util.LinkedHashSet<>();
        for (ParsedObject obj : result.getObjects()) {
            seqs.addAll(obj.getDependencies().getSequences());
        }
        return seqs;
    }

    // ---------------------------------------------------------------
    // Helper: collect sequence CallInfo from a subprogram
    // ---------------------------------------------------------------
    private List<CallInfo> getSequenceCalls(SubprogramInfo sub) {
        return sub.getCalls().stream()
                .filter(c -> "NEXTVAL".equalsIgnoreCase(c.getName())
                        || "CURRVAL".equalsIgnoreCase(c.getName()))
                .collect(Collectors.toList());
    }

    @Test
    @DisplayName("10-table JOIN chain -- all tables found, 9 JoinInfo entries")
    void test10TableJoinChain() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_10join IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM t1
                    JOIN t2 ON t2.id = t1.t2_id
                    JOIN t3 ON t3.id = t2.t3_id
                    JOIN t4 ON t4.id = t3.t4_id
                    JOIN t5 ON t5.id = t4.t5_id
                    JOIN t6 ON t6.id = t5.t6_id
                    JOIN t7 ON t7.id = t6.t7_id
                    LEFT JOIN t8 ON t8.id = t7.t8_id
                    LEFT JOIN t9 ON t9.id = t8.t9_id
                    LEFT JOIN t10 ON t10.id = t9.t10_id;
                  END proc_10join;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "10-table JOIN chain");
        SubprogramInfo sub = findSub(result, "PROC_10JOIN");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        for (int i = 1; i <= 10; i++) {
            assertTrue(names.contains("T" + i),
                    "T" + i + " should be found in table operations");
        }

        List<JoinInfo> joins = allJoins(sub);
        assertTrue(joins.size() >= 9,
                "Should have at least 9 JoinInfo entries, got: " + joins.size());
    }

    @Test
    @DisplayName("5-level nested subquery -- base table found, aliases filtered")
    void test5LevelNestedSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_nested IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM (
                      SELECT * FROM (
                        SELECT * FROM (
                          SELECT * FROM (
                            SELECT id, name FROM base_tbl WHERE active = 'Y'
                          ) l4
                        ) l3
                      ) l2
                    ) l1;
                  END proc_nested;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "5-level nested subquery");
        SubprogramInfo sub = findSub(result, "PROC_NESTED");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        assertTrue(names.contains("BASE_TBL"),
                "BASE_TBL should be found at the deepest level");
        assertFalse(names.contains("L1"), "Inline view alias L1 should NOT appear");
        assertFalse(names.contains("L2"), "Inline view alias L2 should NOT appear");
        assertFalse(names.contains("L3"), "Inline view alias L3 should NOT appear");
        assertFalse(names.contains("L4"), "Inline view alias L4 should NOT appear");
    }

    @Test
    @DisplayName("UNION ALL of 5 queries -- all tables found")
    void testUnionOf5Queries() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_union IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM (
                      SELECT id, name FROM t1
                      UNION ALL
                      SELECT id, name FROM t2
                      UNION ALL
                      SELECT id, name FROM t3
                      UNION ALL
                      SELECT id, name FROM t4
                      UNION ALL
                      SELECT id, name FROM t5
                    );
                  END proc_union;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "UNION ALL of 5 queries");
        SubprogramInfo sub = findSub(result, "PROC_UNION");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        for (int i = 1; i <= 5; i++) {
            assertTrue(names.contains("T" + i),
                    "T" + i + " should be found in UNION ALL");
        }
    }

    @Test
    @DisplayName("SELECT with deeply nested functions -- only real table captured")
    void testSelectWithManyFunctions() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_funcs IS
                    v_name VARCHAR2(200);
                    v_date VARCHAR2(20);
                    v_sal NUMBER;
                  BEGIN
                    SELECT
                      NVL(DECODE(TRIM(UPPER(e.name)), 'X', SUBSTR(e.alt_name, 1, 10)), 'UNKNOWN'),
                      TO_CHAR(e.hire_date, 'YYYY-MM-DD'),
                      ROUND(e.salary * 1.1, 2)
                    INTO v_name, v_date, v_sal
                    FROM employees e
                    WHERE e.dept_id = 10;
                  END proc_funcs;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT with many nested functions");
        SubprogramInfo sub = findSub(result, "PROC_FUNCS");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        assertTrue(names.contains("EMPLOYEES"),
                "EMPLOYEES should be captured as the only real table");
        // Function names should NOT appear as tables
        assertFalse(names.contains("NVL"), "NVL should not appear as a table");
        assertFalse(names.contains("DECODE"), "DECODE should not appear as a table");
        assertFalse(names.contains("TRIM"), "TRIM should not appear as a table");
        assertFalse(names.contains("UPPER"), "UPPER should not appear as a table");
        assertFalse(names.contains("SUBSTR"), "SUBSTR should not appear as a table");
        assertFalse(names.contains("TO_CHAR"), "TO_CHAR should not appear as a table");
        assertFalse(names.contains("ROUND"), "ROUND should not appear as a table");
    }

    @Test
    @DisplayName("MERGE with CTE, JOIN, and EXISTS subquery -- all real tables found")
    void testMergeWithCteAndJoinAndSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_merge_complex IS
                  BEGIN
                    MERGE INTO target_table t
                    USING (
                      WITH src AS (
                        SELECT a.id, b.val
                        FROM table_a a
                        JOIN table_b b ON b.id = a.b_id
                      )
                      SELECT s.id, s.val
                      FROM src s
                      WHERE EXISTS (SELECT 1 FROM table_c c WHERE c.id = s.id)
                    ) s ON (t.id = s.id)
                    WHEN MATCHED THEN UPDATE SET t.val = s.val
                    WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
                  END proc_merge_complex;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "MERGE with CTE, JOIN, EXISTS");
        SubprogramInfo sub = findSub(result, "PROC_MERGE_COMPLEX");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        assertTrue(names.contains("TARGET_TABLE"),
                "TARGET_TABLE (MERGE target) should be found");
        assertTrue(names.contains("TABLE_A"),
                "TABLE_A (inside CTE JOIN) should be found");
        assertTrue(names.contains("TABLE_B"),
                "TABLE_B (inside CTE JOIN) should be found");
        assertTrue(names.contains("TABLE_C"),
                "TABLE_C (inside EXISTS subquery) should be found");
        assertFalse(names.contains("SRC"),
                "CTE alias SRC should NOT appear as a table");
    }

    @Test
    @DisplayName("Large package body with 20 procedures each touching a different table")
    void testLargePackageBody20Procedures() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE OR REPLACE PACKAGE BODY test_pkg AS\n");
        for (int i = 1; i <= 20; i++) {
            sb.append("  PROCEDURE proc_").append(i).append(" IS\n");
            sb.append("    v_cnt NUMBER;\n");
            sb.append("  BEGIN\n");
            sb.append("    SELECT COUNT(*) INTO v_cnt FROM table_").append(i).append(";\n");
            sb.append("  END proc_").append(i).append(";\n");
        }
        sb.append("END test_pkg;\n/\n");

        ParseResult result = parse(sb.toString());
        assertNoParsErrors(result, "20-procedure package body");

        for (int i = 1; i <= 20; i++) {
            SubprogramInfo sub = findSub(result, "PROC_" + i);
            assertNotNull(sub, "PROC_" + i + " should be found");
            Set<String> names = tableNames(sub);
            assertTrue(names.contains("TABLE_" + i),
                    "TABLE_" + i + " should be found in PROC_" + i);
        }
    }

    @Test
    @DisplayName("4 levels of nested BEGIN/EXCEPTION/END -- all handlers found")
    void testDeeplyNestedExceptionHandlers() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_deep_exc IS
                  BEGIN
                    BEGIN
                      BEGIN
                        BEGIN
                          INSERT INTO level4_table (id) VALUES (1);
                        EXCEPTION
                          WHEN NO_DATA_FOUND THEN
                            INSERT INTO exc_log (lvl, msg) VALUES (4, 'NDF at level 4');
                        END;
                      EXCEPTION
                        WHEN DUP_VAL_ON_INDEX THEN
                          INSERT INTO exc_log (lvl, msg) VALUES (3, 'DUP at level 3');
                      END;
                    EXCEPTION
                      WHEN TOO_MANY_ROWS THEN
                        INSERT INTO exc_log (lvl, msg) VALUES (2, 'TMR at level 2');
                    END;
                  EXCEPTION
                    WHEN OTHERS THEN
                      INSERT INTO exc_log (lvl, msg) VALUES (1, 'OTHERS at level 1');
                  END proc_deep_exc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "4-level nested exception handlers");
        SubprogramInfo sub = findSub(result, "PROC_DEEP_EXC");
        assertNotNull(sub);

        List<ExceptionHandlerInfo> handlers = sub.getExceptionHandlers();
        assertTrue(handlers.size() >= 4,
                "Should have at least 4 exception handlers, got: " + handlers.size());

        // Verify the expected exception types are present
        Set<String> excNames = handlers.stream()
                .map(ExceptionHandlerInfo::getExceptionName)
                .collect(Collectors.toSet());
        assertTrue(excNames.contains("NO_DATA_FOUND") || excNames.contains("OTHERS"),
                "Should have at least NO_DATA_FOUND or OTHERS handlers");
    }

    @Test
    @DisplayName("Procedure with 50 different table operations -- all captured")
    void testProcedureWith50TableOperations() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE OR REPLACE PACKAGE BODY test_pkg AS\n");
        sb.append("  PROCEDURE proc_50ops IS\n");
        sb.append("    v_cnt NUMBER;\n");
        sb.append("  BEGIN\n");
        for (int i = 1; i <= 15; i++) {
            sb.append("    SELECT COUNT(*) INTO v_cnt FROM sel_tbl_").append(i).append(";\n");
        }
        for (int i = 1; i <= 15; i++) {
            sb.append("    INSERT INTO ins_tbl_").append(i).append(" (id) VALUES (").append(i).append(");\n");
        }
        for (int i = 1; i <= 10; i++) {
            sb.append("    UPDATE upd_tbl_").append(i).append(" SET val = ").append(i).append(" WHERE id = 1;\n");
        }
        for (int i = 1; i <= 10; i++) {
            sb.append("    DELETE FROM del_tbl_").append(i).append(" WHERE id = ").append(i).append(";\n");
        }
        sb.append("  END proc_50ops;\n");
        sb.append("END test_pkg;\n/\n");

        ParseResult result = parse(sb.toString());
        assertNoParsErrors(result, "50 table operations");
        SubprogramInfo sub = findSub(result, "PROC_50OPS");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        int foundCount = 0;
        for (int i = 1; i <= 15; i++) {
            if (names.contains("SEL_TBL_" + i)) foundCount++;
            if (names.contains("INS_TBL_" + i)) foundCount++;
        }
        for (int i = 1; i <= 10; i++) {
            if (names.contains("UPD_TBL_" + i)) foundCount++;
            if (names.contains("DEL_TBL_" + i)) foundCount++;
        }
        assertTrue(foundCount >= 50,
                "Should capture all 50 table operations, got: " + foundCount);
    }

    @Test
    @DisplayName("Multiple sequences in one procedure -- all detected")
    void testMultipleSequencesInOneProcedure() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_multi_seq IS
                    v1 NUMBER;
                    v2 NUMBER;
                    v3 NUMBER;
                    v4 NUMBER;
                  BEGIN
                    v1 := SEQ1.NEXTVAL;
                    v2 := SEQ2.NEXTVAL;
                    v3 := SEQ3.CURRVAL;
                    v4 := MY_SCHEMA.SEQ4.NEXTVAL;
                  END proc_multi_seq;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multiple sequences");
        SubprogramInfo sub = findSub(result, "PROC_MULTI_SEQ");
        assertNotNull(sub);

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("SEQ1"), "SEQ1 should be in dependencies");
        assertTrue(seqs.contains("SEQ2"), "SEQ2 should be in dependencies");
        assertTrue(seqs.contains("SEQ3"), "SEQ3 should be in dependencies");
        assertTrue(seqs.contains("SEQ4"), "SEQ4 should be in dependencies");

        List<CallInfo> seqCalls = getSequenceCalls(sub);
        assertTrue(seqCalls.size() >= 4,
                "Should have at least 4 sequence calls, got: " + seqCalls.size());

        // Verify schema-qualified sequence has schema set
        boolean hasSchemaSeq = seqCalls.stream()
                .anyMatch(c -> "SEQ4".equalsIgnoreCase(c.getPackageName())
                        && "MY_SCHEMA".equalsIgnoreCase(c.getSchema()));
        assertTrue(hasSchemaSeq,
                "SEQ4 should have MY_SCHEMA as its schema");
    }

    @Test
    @DisplayName("FORALL inside EXCEPTION WHEN OTHERS handler")
    void testForallInsideExceptionHandler() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_forall_exc IS
                    TYPE t_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    l_failed_ids t_ids;
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN OTHERS THEN
                      l_failed_ids(1) := 100;
                      l_failed_ids(2) := 200;
                      FORALL i IN 1..l_failed_ids.COUNT
                        INSERT INTO failed_records (id, err_msg)
                        VALUES (l_failed_ids(i), SQLERRM);
                  END proc_forall_exc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FORALL in exception handler");
        SubprogramInfo sub = findSub(result, "PROC_FORALL_EXC");
        assertNotNull(sub);

        // Check that the exception handler has the table operation
        ExceptionHandlerInfo eh = sub.getExceptionHandlers().stream()
                .filter(e -> "OTHERS".equals(e.getExceptionName()))
                .findFirst().orElse(null);
        assertNotNull(eh, "Should have WHEN OTHERS handler");

        boolean hasFailedRecords = eh.getTableOperations().stream()
                .anyMatch(t -> "FAILED_RECORDS".equalsIgnoreCase(t.getTableName())
                        && "INSERT".equalsIgnoreCase(t.getOperation()));
        assertTrue(hasFailedRecords,
                "FAILED_RECORDS INSERT should be tracked in exception handler");

        // Also verify FORALL statement is detected
        boolean hasForall = sub.getStatements().stream()
                .anyMatch(s -> "FORALL".equalsIgnoreCase(s.getType()));
        assertTrue(hasForall,
                "FORALL statement should be detected even inside exception handler");
    }
}
