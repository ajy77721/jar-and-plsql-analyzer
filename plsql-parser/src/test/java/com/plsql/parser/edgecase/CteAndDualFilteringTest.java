package com.plsql.parser.edgecase;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gaps #15-#17: CTE Alias Filtering, Subquery Operation Type, DUAL Filtering")
public class CteAndDualFilteringTest extends ParserTestBase {

    // ---------------------------------------------------------------
    // Helper: collect all table names (upper-cased) from a subprogram
    // ---------------------------------------------------------------
    private Set<String> tableNames(SubprogramInfo sub) {
        return sub.getTableOperations().stream()
                .map(t -> t.getTableName().toUpperCase())
                .collect(Collectors.toSet());
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

    // =================================================================
    // CTE Alias Filtering Edge Cases (Gap #15)
    // =================================================================

    @Test
    @DisplayName("Single CTE alias is filtered out, real table is kept")
    void testSingleCteFilteredOut() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_cte IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH cte AS (SELECT id FROM real_table WHERE status = 'A')
                    SELECT COUNT(*) INTO v_cnt FROM cte;
                  END proc_cte;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Single CTE");
        SubprogramInfo sub = findSub(result, "PROC_CTE");
        assertNotNull(sub, "Subprogram PROC_CTE should exist");
        Set<String> names = tableNames(sub);
        assertTrue(names.contains("REAL_TABLE"),
                "REAL_TABLE must appear in table operations");
        assertFalse(names.contains("CTE"),
                "CTE alias must NOT appear as a table operation");
    }

    @Test
    @DisplayName("Five chained CTEs all filtered, only real tables kept")
    void testFiveCtesChainingFiltered() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_chain IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH
                      c1 AS (SELECT id, col1 FROM t1 WHERE active = 'Y'),
                      c2 AS (SELECT c1.id, t2.val FROM c1 JOIN t2 ON t2.id = c1.id),
                      c3 AS (SELECT * FROM c2 WHERE val > 0),
                      c4 AS (SELECT c3.id, t3.name FROM c3 JOIN t3 ON t3.id = c3.id),
                      c5 AS (SELECT * FROM c4)
                    SELECT COUNT(*) INTO v_cnt FROM c5;
                  END proc_chain;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Five chained CTEs");
        SubprogramInfo sub = findSub(result, "PROC_CHAIN");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        assertTrue(names.contains("T1"), "T1 must be captured");
        assertTrue(names.contains("T2"), "T2 must be captured");
        assertTrue(names.contains("T3"), "T3 must be captured");

        assertFalse(names.contains("C1"), "CTE alias C1 must NOT appear");
        assertFalse(names.contains("C2"), "CTE alias C2 must NOT appear");
        assertFalse(names.contains("C3"), "CTE alias C3 must NOT appear");
        assertFalse(names.contains("C4"), "CTE alias C4 must NOT appear");
        assertFalse(names.contains("C5"), "CTE alias C5 must NOT appear");
    }

    @Test
    @DisplayName("CTE with same name as a real table -- CTE should win")
    void testCteSameNameAsRealTable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_shadow IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH employees AS (SELECT id, name FROM hr_data WHERE dept = 10)
                    SELECT COUNT(*) INTO v_cnt FROM employees;
                  END proc_shadow;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CTE shadows real table name");
        SubprogramInfo sub = findSub(result, "PROC_SHADOW");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        assertTrue(names.contains("HR_DATA"),
                "HR_DATA (the real table inside CTE definition) must be captured");
        assertFalse(names.contains("EMPLOYEES"),
                "EMPLOYEES used as CTE alias should NOT appear as a table operation");
    }

    @Test
    @DisplayName("CTE used in JOIN position is filtered, joined real table is kept")
    void testCteUsedInJoinNotAsTable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_cte_join IS
                    v_cnt NUMBER;
                  BEGIN
                    WITH summary AS (SELECT dept_id, SUM(sal) total_sal FROM payroll GROUP BY dept_id)
                    SELECT COUNT(*) INTO v_cnt
                    FROM summary s
                    JOIN real_table r ON r.dept_id = s.dept_id;
                  END proc_cte_join;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CTE in JOIN");
        SubprogramInfo sub = findSub(result, "PROC_CTE_JOIN");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        assertFalse(names.contains("SUMMARY"),
                "CTE alias SUMMARY must NOT appear as a table operation");
        assertTrue(names.contains("PAYROLL"),
                "PAYROLL (real table inside CTE) must be captured");
        assertTrue(names.contains("REAL_TABLE"),
                "REAL_TABLE (joined) must be captured");
    }

    @Test
    @DisplayName("CTE in INSERT...SELECT via subquery -- only real tables captured")
    void testCteInInsertSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_cte_ins IS
                  BEGIN
                    INSERT INTO target_table (id, val)
                    SELECT id, val FROM (
                      WITH src AS (SELECT id, val FROM source_table WHERE active = 'Y')
                      SELECT id, val FROM src
                    );
                  END proc_cte_ins;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CTE in INSERT...SELECT");
        SubprogramInfo sub = findSub(result, "PROC_CTE_INS");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        assertTrue(names.contains("SOURCE_TABLE"),
                "SOURCE_TABLE must be captured");
        assertTrue(names.contains("TARGET_TABLE"),
                "TARGET_TABLE must be captured as INSERT target");
        assertFalse(names.contains("SRC"),
                "CTE alias SRC must NOT appear as a table operation");
    }

    @Test
    @DisplayName("CTE in MERGE USING subquery -- CTE filtered, target is real")
    void testCteInMergeUsing() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_cte_merge IS
                  BEGIN
                    MERGE INTO target_table t
                    USING (
                      WITH staged AS (
                        SELECT id, val FROM staging_area WHERE processed = 'N'
                      )
                      SELECT id, val FROM staged
                    ) s ON (t.id = s.id)
                    WHEN MATCHED THEN UPDATE SET t.val = s.val
                    WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
                  END proc_cte_merge;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "CTE in MERGE USING");
        SubprogramInfo sub = findSub(result, "PROC_CTE_MERGE");
        assertNotNull(sub);
        Set<String> names = tableNames(sub);

        assertTrue(names.contains("TARGET_TABLE"),
                "TARGET_TABLE (MERGE target) must be captured");
        assertTrue(names.contains("STAGING_AREA"),
                "STAGING_AREA (real table inside CTE) must be captured");
        assertFalse(names.contains("STAGED"),
                "CTE alias STAGED must NOT appear as a table operation");
    }

    // =================================================================
    // Subquery Operation Type (Gap #16)
    // =================================================================

    @Test
    @DisplayName("INSERT subquery tables are SELECT, not INSERT")
    void testSubqueryTablesAreSelectNotParentDml() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_ins_subq IS
                  BEGIN
                    INSERT INTO target_table (id)
                    SELECT id FROM source_table
                    WHERE id IN (SELECT id FROM filter_table WHERE active = 'Y');
                  END proc_ins_subq;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "INSERT with subquery");
        SubprogramInfo sub = findSub(result, "PROC_INS_SUBQ");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "TARGET_TABLE".equalsIgnoreCase(t.getTableName())
                        && "INSERT".equalsIgnoreCase(t.getOperation())),
                "TARGET_TABLE should be INSERT");
        assertTrue(ops.stream().anyMatch(t ->
                "FILTER_TABLE".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "FILTER_TABLE in subquery should be SELECT, not INSERT");
        assertFalse(ops.stream().anyMatch(t ->
                "FILTER_TABLE".equalsIgnoreCase(t.getTableName())
                        && "INSERT".equalsIgnoreCase(t.getOperation())),
                "FILTER_TABLE must NOT be marked as INSERT");
    }

    @Test
    @DisplayName("DELETE subquery tables are SELECT, not DELETE")
    void testDeleteSubqueryTablesAreSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_del_subq IS
                  BEGIN
                    DELETE FROM orders
                    WHERE cust_id IN (SELECT id FROM inactive_customers WHERE purge_flag = 'Y');
                  END proc_del_subq;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "DELETE with subquery");
        SubprogramInfo sub = findSub(result, "PROC_DEL_SUBQ");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "ORDERS".equalsIgnoreCase(t.getTableName())
                        && "DELETE".equalsIgnoreCase(t.getOperation())),
                "ORDERS should be DELETE");
        assertTrue(ops.stream().anyMatch(t ->
                "INACTIVE_CUSTOMERS".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "INACTIVE_CUSTOMERS in subquery should be SELECT, not DELETE");
    }

    @Test
    @DisplayName("UPDATE correlated subquery table is SELECT")
    void testUpdateCorrelatedSubqueryIsSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_upd_corr IS
                  BEGIN
                    UPDATE emp e
                    SET e.sal = (SELECT AVG(e2.sal) FROM emp e2 WHERE e2.dept_id = e.dept_id)
                    WHERE e.dept_id IS NOT NULL;
                  END proc_upd_corr;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "UPDATE correlated subquery");
        SubprogramInfo sub = findSub(result, "PROC_UPD_CORR");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "EMP".equalsIgnoreCase(t.getTableName())
                        && "UPDATE".equalsIgnoreCase(t.getOperation())),
                "EMP should have UPDATE operation for the outer statement");
        // The subquery reference to emp should be SELECT
        long selectEmpCount = ops.stream().filter(t ->
                "EMP".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())).count();
        assertTrue(selectEmpCount >= 1,
                "EMP in correlated subquery should also appear as SELECT");
    }

    @Test
    @DisplayName("MERGE USING subquery tables are SELECT")
    void testMergeUsingSubqueryIsSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_merge_subq IS
                  BEGIN
                    MERGE INTO target_table t
                    USING (SELECT id, val FROM source_table WHERE active = 'Y') s
                    ON (t.id = s.id)
                    WHEN MATCHED THEN UPDATE SET t.val = s.val
                    WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
                  END proc_merge_subq;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "MERGE USING subquery");
        SubprogramInfo sub = findSub(result, "PROC_MERGE_SUBQ");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "SOURCE_TABLE".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "SOURCE_TABLE in MERGE USING subquery should be SELECT");
        assertFalse(ops.stream().anyMatch(t ->
                "SOURCE_TABLE".equalsIgnoreCase(t.getTableName())
                        && "MERGE".equalsIgnoreCase(t.getOperation())),
                "SOURCE_TABLE must NOT be marked as MERGE");
    }

    // =================================================================
    // DUAL Filtering (Gap #17)
    // =================================================================

    @Test
    @DisplayName("SELECT SYSDATE FROM DUAL -- verify DUAL handling")
    void testDualNotInTableOperations() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_dual IS
                    v_dt DATE;
                  BEGIN
                    SELECT SYSDATE INTO v_dt FROM DUAL;
                  END proc_dual;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT FROM DUAL");
        SubprogramInfo sub = findSub(result, "PROC_DUAL");
        assertNotNull(sub);
        // DUAL should ideally not be tracked as a real table dependency.
        // If the parser does track it, at least verify no crash.
        boolean hasDual = sub.getTableOperations().stream()
                .anyMatch(t -> "DUAL".equalsIgnoreCase(t.getTableName()));
        // Preferred behavior: DUAL is filtered out
        assertFalse(hasDual,
                "DUAL should not appear as a tracked table operation");
    }

    @Test
    @DisplayName("SELECT seq.NEXTVAL FROM DUAL -- sequence tracked, DUAL filtered")
    void testDualWithSequence() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_seq_dual IS
                    v_id NUMBER;
                  BEGIN
                    SELECT my_seq.NEXTVAL INTO v_id FROM DUAL;
                  END proc_seq_dual;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Sequence from DUAL");
        SubprogramInfo sub = findSub(result, "PROC_SEQ_DUAL");
        assertNotNull(sub);

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("MY_SEQ"),
                "MY_SEQ should be detected as a sequence dependency");

        boolean hasDual = sub.getTableOperations().stream()
                .anyMatch(t -> "DUAL".equalsIgnoreCase(t.getTableName()));
        assertFalse(hasDual,
                "DUAL should not appear as a tracked table operation");
    }

    @Test
    @DisplayName("INSERT INTO log_table with seq -- only log_table as INSERT, no DUAL")
    void testDualInInsert() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_ins_seq IS
                  BEGIN
                    INSERT INTO log_table (id, created)
                    VALUES (log_seq.NEXTVAL, SYSDATE);
                  END proc_ins_seq;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "INSERT with sequence");
        SubprogramInfo sub = findSub(result, "PROC_INS_SEQ");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "LOG_TABLE".equalsIgnoreCase(t.getTableName())
                        && "INSERT".equalsIgnoreCase(t.getOperation())),
                "LOG_TABLE should be captured as INSERT");

        boolean hasDual = ops.stream()
                .anyMatch(t -> "DUAL".equalsIgnoreCase(t.getTableName()));
        assertFalse(hasDual,
                "DUAL should not appear as a table operation in INSERT context");

        Set<String> seqs = getSequences(result);
        assertTrue(seqs.contains("LOG_SEQ"),
                "LOG_SEQ should be detected as a sequence dependency");
    }
}
