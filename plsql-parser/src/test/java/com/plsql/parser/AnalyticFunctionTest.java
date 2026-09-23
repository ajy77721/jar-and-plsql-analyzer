package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Analytic / Window Functions")
public class AnalyticFunctionTest extends ParserTestBase {

    @Test
    @DisplayName("MAX() OVER (PARTITION BY) inside FROM subquery")
    void testMaxOverPartitionInSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                  PROCEDURE TEST_PROC IS
                    v_status VARCHAR2(10);
                  BEGIN
                    SELECT jpj_status INTO v_status
                      FROM (SELECT jpj_reply_id, jpj_status,
                                   MAX(jpj_reply_id) OVER (PARTITION BY cnote_no) latest
                              FROM cnge_note_mtjpj_reply
                             WHERE cnote_no = 'X' AND doc_type NOT IN ('3'))
                     WHERE JPJ_REPLY_ID = LATEST;
                  END TEST_PROC;
                END TEST_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "MAX OVER PARTITION BY");
        SubprogramInfo sub = findSub(result, "TEST_PROC");
        assertNotNull(sub);
        assertFalse(sub.getTableOperations().isEmpty());
    }

    @Test
    @DisplayName("SUM() OVER (ORDER BY) in top-level SELECT INTO")
    void testSumOverOrderBy() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                  PROCEDURE TEST_PROC IS
                    v_total NUMBER;
                  BEGIN
                    SELECT SUM(amount) OVER (ORDER BY created_date) INTO v_total
                      FROM transactions WHERE id = 1;
                  END TEST_PROC;
                END TEST_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SUM OVER ORDER BY");
    }

    @Test
    @DisplayName("ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)")
    void testRowNumberOverInSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                  PROCEDURE TEST_PROC IS
                    v_id NUMBER;
                  BEGIN
                    SELECT id INTO v_id
                      FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                              FROM employees)
                     WHERE rn = 1;
                  END TEST_PROC;
                END TEST_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "ROW_NUMBER OVER");
    }

    @Test
    @DisplayName("LEAD/LAG analytic functions")
    void testLeadLagInSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                  PROCEDURE TEST_PROC IS
                    v_delta NUMBER;
                  BEGIN
                    SELECT delta INTO v_delta
                      FROM (SELECT id, amount - LAG(amount) OVER (ORDER BY id) AS delta
                              FROM transactions)
                     WHERE id = 5;
                  END TEST_PROC;
                END TEST_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "LEAD/LAG OVER");
    }

    @Test
    @DisplayName("LATEST as column alias in subquery")
    void testLatestAsColumnAlias() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                  PROCEDURE TEST_PROC IS
                    v_id NUMBER;
                  BEGIN
                    SELECT id INTO v_id
                      FROM (SELECT id, MAX(id) latest FROM t1)
                     WHERE id = 1;
                  END TEST_PROC;
                END TEST_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "LATEST as alias");
    }

    @Test
    @DisplayName("DENSE_RANK, RANK, NTILE analytic functions")
    void testRankFunctions() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_rank NUMBER;
                  BEGIN
                    SELECT rnk INTO v_rank
                      FROM (SELECT id,
                                   DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk,
                                   RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rnk,
                                   NTILE(4) OVER (ORDER BY salary) AS quartile
                              FROM employees)
                     WHERE id = 100;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "DENSE_RANK/RANK/NTILE");
    }

    @Test
    @DisplayName("LISTAGG analytic function")
    void testListagg() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_names VARCHAR2(4000);
                  BEGIN
                    SELECT LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name)
                      INTO v_names
                      FROM employees
                     WHERE dept_id = 10;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "LISTAGG");
    }

    @Test
    @DisplayName("FIRST_VALUE / LAST_VALUE with windowing clause")
    void testFirstValueLastValue() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_first NUMBER;
                  BEGIN
                    SELECT val INTO v_first
                      FROM (SELECT id,
                                   FIRST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY hire_date) AS val,
                                   LAST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY hire_date
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_val
                              FROM employees)
                     WHERE id = 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FIRST_VALUE/LAST_VALUE with windowing");
    }

    @Test
    @DisplayName("Multiple OVER clauses in same SELECT")
    void testMultipleOverClauses() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_id NUMBER;
                  BEGIN
                    SELECT id INTO v_id
                      FROM (SELECT id,
                                   ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn,
                                   SUM(salary) OVER (PARTITION BY dept) AS dept_total,
                                   AVG(salary) OVER () AS global_avg,
                                   COUNT(*) OVER (ORDER BY hire_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling
                              FROM employees)
                     WHERE rn = 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multiple OVER clauses");
    }
}
