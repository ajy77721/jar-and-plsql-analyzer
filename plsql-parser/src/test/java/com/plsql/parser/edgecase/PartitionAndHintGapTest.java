package com.plsql.parser.edgecase;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case tests for Gap #11 (Advanced partitioning grammar) and
 * Gap #12 (Hints not extracted as structured info).
 */
@DisplayName("Gaps #11, #12: Partition Syntax and Hint Handling")
public class PartitionAndHintGapTest extends ParserTestBase {

    // =========================================================================
    // Gap #11: Partition and subpartition syntax
    // =========================================================================

    @Test
    @DisplayName("SELECT FROM table PARTITION(name) parses without error, table extracted")
    void testPartitionQueryNoParsError() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM sales PARTITION (sales_q1_2024);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT from PARTITION");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertTrue(sub.getTableOperations().stream()
                .anyMatch(t -> "SALES".equalsIgnoreCase(t.getTableName())),
                "Should extract SALES table from PARTITION query");
    }

    @Test
    @DisplayName("SELECT FROM table SUBPARTITION(name) parses without error")
    void testSubpartitionQueryNoParsError() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM sales SUBPARTITION (sub_q1);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SELECT from SUBPARTITION");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertTrue(sub.getTableOperations().stream()
                .anyMatch(t -> "SALES".equalsIgnoreCase(t.getTableName())),
                "Should extract SALES table from SUBPARTITION query");
    }

    @Test
    @DisplayName("ALTER TABLE ADD PARTITION: document whether DDL partitioning parses")
    void testPartitionByRangeInDdl() {
        // DDL inside PL/SQL must use EXECUTE IMMEDIATE, so wrapping it that way
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                  BEGIN
                    EXECUTE IMMEDIATE
                      'ALTER TABLE sales ADD PARTITION sales_q2_2024 VALUES LESS THAN (TO_DATE(''2024-07-01'',''YYYY-MM-DD''))';
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "ALTER TABLE ADD PARTITION via EXECUTE IMMEDIATE");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        // Dynamic SQL should be detected
        assertFalse(sub.getDynamicSql().isEmpty(),
                "Should detect EXECUTE IMMEDIATE as dynamic SQL");
    }

    @Test
    @DisplayName("INSERT INTO table PARTITION(name) VALUES(...) — table extracted")
    void testInsertIntoPartition() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                  BEGIN
                    INSERT INTO sales PARTITION (sales_q1_2024)
                    VALUES (1, SYSDATE, 100.00);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "INSERT INTO PARTITION");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertTrue(sub.getTableOperations().stream()
                .anyMatch(t -> "SALES".equalsIgnoreCase(t.getTableName())),
                "Should extract SALES table from INSERT INTO PARTITION");
    }

    @Test
    @DisplayName("SELECT FROM PARTITION with JOIN: both tables and join detected")
    void testSelectFromPartitionWithJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt
                    FROM sales PARTITION (p1) s
                    JOIN products p ON s.prod_id = p.id;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "PARTITION with JOIN");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();
        assertTrue(ops.stream().anyMatch(t -> "SALES".equalsIgnoreCase(t.getTableName())),
                "Should extract SALES table");
        assertTrue(ops.stream().anyMatch(t -> "PRODUCTS".equalsIgnoreCase(t.getTableName())),
                "Should extract PRODUCTS table");
        boolean hasJoin = ops.stream()
                .flatMap(t -> t.getJoins().stream())
                .anyMatch(j -> j.getJoinedTable() != null);
        assertTrue(hasJoin, "Should detect the JOIN between SALES and PRODUCTS");
    }

    // =========================================================================
    // Gap #12: Hint syntax handling
    // =========================================================================

    @Test
    @DisplayName("SELECT /*+ INDEX(emp idx_emp_dept) */ parses without error")
    void testSimpleHintNoParsError() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT /*+ INDEX(emp idx_emp_dept) */ COUNT(*)
                    INTO v_cnt
                    FROM employees emp;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "INDEX hint");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertTrue(sub.getTableOperations().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "Should extract EMPLOYEES with INDEX hint");
    }

    @Test
    @DisplayName("SELECT /*+ PARALLEL(4) */ COUNT(*) parses without error")
    void testParallelHintNoParsError() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT /*+ PARALLEL(4) */ COUNT(*)
                    INTO v_cnt
                    FROM large_table;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "PARALLEL hint");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertTrue(sub.getTableOperations().stream()
                .anyMatch(t -> "LARGE_TABLE".equalsIgnoreCase(t.getTableName())),
                "Should extract LARGE_TABLE with PARALLEL hint");
    }

    @Test
    @DisplayName("Multi-hint combination: LEADING + USE_NL + INDEX, both tables + join extracted")
    void testMultiHintCombination() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_id NUMBER;
                  BEGIN
                    SELECT /*+ LEADING(a b) USE_NL(b) INDEX(a idx1) */ a.id
                    INTO v_id
                    FROM t1 a
                    JOIN t2 b ON a.id = b.id
                    WHERE ROWNUM = 1;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multi-hint LEADING + USE_NL + INDEX");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();
        assertTrue(ops.stream().anyMatch(t -> "T1".equalsIgnoreCase(t.getTableName())),
                "Should extract T1");
        assertTrue(ops.stream().anyMatch(t -> "T2".equalsIgnoreCase(t.getTableName())),
                "Should extract T2");
        boolean hasJoin = ops.stream()
                .flatMap(t -> t.getJoins().stream())
                .anyMatch(j -> j.getJoinedTable() != null);
        assertTrue(hasJoin, "Should detect JOIN between T1 and T2 despite hints");
    }

    @Test
    @DisplayName("SELECT /*+ FULL(e) NO_INDEX(e) */ parses without error")
    void testHintWithFullSyntax() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT /*+ FULL(e) NO_INDEX(e) */ COUNT(*)
                    INTO v_cnt
                    FROM employees e;
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FULL + NO_INDEX hints");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertTrue(sub.getTableOperations().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName())),
                "Should extract EMPLOYEES with FULL + NO_INDEX hints");
    }

    @Test
    @DisplayName("MERGE /*+ APPEND */ INTO target USING source ON... parses without error")
    void testMergeWithHint() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE test_proc IS
                  BEGIN
                    MERGE /*+ APPEND */ INTO target_table t
                    USING source_table s ON (t.id = s.id)
                    WHEN MATCHED THEN
                      UPDATE SET t.val = s.val
                    WHEN NOT MATCHED THEN
                      INSERT (id, val) VALUES (s.id, s.val);
                  END test_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "MERGE with APPEND hint");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        List<TableOperationInfo> ops = sub.getTableOperations();
        assertTrue(ops.stream().anyMatch(t -> "TARGET_TABLE".equalsIgnoreCase(t.getTableName())),
                "Should extract TARGET_TABLE from MERGE");
        assertTrue(ops.stream().anyMatch(t -> "SOURCE_TABLE".equalsIgnoreCase(t.getTableName())),
                "Should extract SOURCE_TABLE from MERGE");
    }
}
