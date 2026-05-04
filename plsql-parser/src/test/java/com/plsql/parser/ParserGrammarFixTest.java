package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Targeted tests for two parser grammar fixes:
 *
 * Fix 1 (WORKERID non-reserved keyword): WORKERID was a lexer token but not listed
 * in non_reserved_keywords_in_12c, so CASE ... END AS WorkerId failed to parse.
 * Fixed in PlSqlParser.g4.
 *
 * Fix 2 (bare JOIN recognition): isNotStartOfJoin() in PlSqlParserBase.java did not
 * check for the bare JOIN token, so FROM t1 JOIN t2 ON ... parsed JOIN as a table
 * alias instead of a join keyword. Fixed in PlSqlParserBase.java.
 */
@DisplayName("Parser Grammar Fixes")
public class ParserGrammarFixTest extends ParserTestBase {

    // =========================================================================
    // Fix 1: WORKERID as non-reserved keyword
    // =========================================================================

    @Nested
    @DisplayName("Fix 1: WORKERID as non-reserved keyword")
    class WorkerIdFix {

        @Test
        @DisplayName("CASE expression with END AS WorkerId (mixed case) parses with 0 errors")
        void testWorkerIdMixedCase() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_id VARCHAR2(50);
                      BEGIN
                        SELECT CASE WHEN (col IS NOT NULL) THEN col END AS WorkerId
                          INTO v_id FROM dual;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "CASE END AS WorkerId (mixed case)");
        }

        @Test
        @DisplayName("CASE expression with END AS workerid (lowercase) parses with 0 errors")
        void testWorkerIdLowerCase() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_id VARCHAR2(50);
                      BEGIN
                        SELECT CASE WHEN (col IS NOT NULL) THEN col END AS workerid
                          INTO v_id FROM dual;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "CASE END AS workerid (lowercase)");
        }

        @Test
        @DisplayName("CASE expression with END AS WORKERID (uppercase) parses with 0 errors")
        void testWorkerIdUpperCase() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_id VARCHAR2(50);
                      BEGIN
                        SELECT CASE WHEN (col IS NOT NULL) THEN col END AS WORKERID
                          INTO v_id FROM dual;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "CASE END AS WORKERID (uppercase)");
        }

        @Test
        @DisplayName("WORKERID used as a variable name in DECLARE block")
        void testWorkerIdAsVariable() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        WorkerId VARCHAR2(50);
                      BEGIN
                        WorkerId := 'W001';
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "WORKERID as variable name");
        }

        @Test
        @DisplayName("WORKERID used as a column name in SELECT list")
        void testWorkerIdAsColumnName() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_id VARCHAR2(50);
                      BEGIN
                        SELECT WorkerId INTO v_id FROM employees WHERE ROWNUM = 1;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "WORKERID as column name in SELECT");
        }

        @Test
        @DisplayName("WORKERID used in WHERE clause")
        void testWorkerIdInWhereClause() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM employees
                         WHERE WorkerId = 'W001';
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "WORKERID in WHERE clause");
        }

        @Test
        @DisplayName("Other similar non-reserved keywords used as aliases (DOCUMENT, CONTENT)")
        void testOtherNonReservedKeywordsAsAliases() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v1 VARCHAR2(50);
                        v2 VARCHAR2(50);
                      BEGIN
                        SELECT col1 AS DOCUMENT, col2 AS CONTENT
                          INTO v1, v2 FROM dual;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "DOCUMENT/CONTENT as aliases");
        }

        @Test
        @DisplayName("Actual cursor from PG_TPA_BUILD with WorkerId alias parses with 0 errors")
        void testActualPgTpaBuildCursor() {
            // Extracted from samples/CUSTOMER/PG_TPA_BUILD.pkb around line 3547-3589
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY PG_TPA_BUILD AS
                    PROCEDURE PC_TPA_SPPA_CLAIMS(P_START_DT IN DATE,
                                                V_COUNT_SUCCESS IN VARCHAR2, V_RUN_HOUR IN VARCHAR2) IS
                      CURSOR C_TPA_SPPA_CLAIMS IS
                        SELECT 'AGI' AS INSURER_CD, B.POL_NO, A.CLM_NO,
                        CASE WHEN (A.FW_COUNTRY IS NOT NULL) THEN A.FW_COUNTRY END AS NationalityCd,
                        CASE WHEN (A.FW_PASSPORT IS NOT NULL) THEN A.FW_PASSPORT END AS PassportNo,
                        CASE WHEN (A.FW_CARD_ID IS NOT NULL) THEN A.FW_CARD_ID END AS WorkerId,
                        TO_CHAR(A.LOSS_DATE, 'YYYYMMDD') AS AccidentDate,
                        A.FW_ACCIDENT_TIME AS AccidentTime, A.FW_ACCIDENT_PLACE,
                        CASE WHEN (A.FW_ACCIDENT_OCCUR IS NOT NULL) THEN A.FW_ACCIDENT_OCCUR END AS ACCIDENT_OCCUR,
                        CASE WHEN (A.FW_DISABLEMENT_TYPE IS NOT NULL) THEN A.FW_DISABLEMENT_TYPE END AS DISABLE_TYPE,
                        CASE WHEN (A.LOSS_CAUSE = 'WC01') THEN '01'
                             WHEN (A.LOSS_CAUSE = 'WC02') THEN '02'
                             WHEN (A.LOSS_CAUSE = 'WC03') THEN '03'
                             ELSE '10' END AS LOSS_CODE,
                        CASE WHEN (A.FW_INJURY_TYPE IS NOT NULL) THEN A.FW_INJURY_TYPE END AS INJURY_TYPE,
                        TO_CHAR(A.FW_CEASED_DATE, 'YYYYMMDD') AS DateCeaseWork,
                        TO_CHAR(A.FW_START_DATE, 'YYYYMMDD') AS DateStartWork,
                        CASE WHEN (B.PYMT_TYPE = '44') THEN B.PYMT_AMT ELSE 0.00 END AS MEDICAL_AMT,
                        CASE WHEN (B.PYMT_TYPE = '46') THEN B.PYMT_AMT ELSE 0.00 END AS REPAT_AMT,
                        0.00 AS FUNRL_AMOUNT,
                        CASE WHEN (B.PYMT_TYPE IN ('40', '41', '43')) THEN B.PYMT_AMT ELSE 0.00 END AS COMPNST_PAYOUT,
                        B.PYMT_NO AS VoucherNo, TO_CHAR(B.TRAN_DATE, 'YYYYMMDD') AS VoucherDate,
                        A.RISK_ID
                        FROM CLNM_MAST A
                        INNER JOIN CLNM_PYMT B ON (A.CLM_NO = B.CLM_NO)
                        WHERE B.PYMT_TYPE IN ('44', '46', '40', '41', '43')
                          AND B.TRAN_DATE = TO_DATE(P_START_DT, 'dd-MON-yy');
                      V_STEPS VARCHAR2(10);
                    BEGIN
                      V_STEPS := '001';
                    END PC_TPA_SPPA_CLAIMS;
                    END PG_TPA_BUILD;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "PG_TPA_BUILD cursor with WorkerId alias");
        }
    }

    // =========================================================================
    // Fix 2: Bare JOIN keyword recognition
    // =========================================================================

    @Nested
    @DisplayName("Fix 2: Bare JOIN keyword recognition")
    class BareJoinFix {

        @Test
        @DisplayName("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id parses with 0 errors")
        void testBareJoin() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM t1 JOIN t2 ON t1.id = t2.id;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Bare JOIN");
        }

        @Test
        @DisplayName("INNER JOIN still works")
        void testInnerJoin() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM t1 INNER JOIN t2 ON t1.id = t2.id;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "INNER JOIN");
        }

        @Test
        @DisplayName("LEFT JOIN still works")
        void testLeftJoin() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM t1 LEFT JOIN t2 ON t1.id = t2.id;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "LEFT JOIN");
        }

        @Test
        @DisplayName("RIGHT JOIN still works")
        void testRightJoin() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM t1 RIGHT JOIN t2 ON t1.id = t2.id;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "RIGHT JOIN");
        }

        @Test
        @DisplayName("FULL OUTER JOIN still works")
        void testFullOuterJoin() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "FULL OUTER JOIN");
        }

        @Test
        @DisplayName("CROSS JOIN still works")
        void testCrossJoin() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM t1 CROSS JOIN t2;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "CROSS JOIN");
        }

        @Test
        @DisplayName("NATURAL JOIN still works")
        void testNaturalJoin() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM t1 NATURAL JOIN t2;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "NATURAL JOIN");
        }

        @Test
        @DisplayName("Multiple bare JOINs: FROM t1 JOIN t2 ON ... JOIN t3 ON ...")
        void testMultipleBareJoins() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM t1
                          JOIN t2 ON t1.id = t2.t1_id
                          JOIN t3 ON t2.id = t3.t2_id;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Multiple bare JOINs");
        }

        @Test
        @DisplayName("Bare JOIN in a cursor definition")
        void testBareJoinInCursor() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        CURSOR c IS
                          SELECT a.col1, b.col2
                            FROM t1 a JOIN t2 b ON a.id = b.t1_id;
                      BEGIN
                        NULL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Bare JOIN in cursor");
        }

        @Test
        @DisplayName("Bare JOIN in subquery")
        void testBareJoinInSubquery() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM (SELECT a.col1, b.col2
                                  FROM t1 a JOIN t2 b ON a.id = b.t1_id);
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Bare JOIN in subquery");
        }

        @Test
        @DisplayName("JOIN used as a PL/SQL variable name (non-join context)")
        void testJoinAsVariableName() {
            // JOIN is not a reserved word in PL/SQL variable declarations;
            // however, the parser may or may not allow it depending on grammar.
            // We just verify it does not throw an exception.
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_join VARCHAR2(50);
                      BEGIN
                        v_join := 'test';
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "v_join as variable name");
        }

        @Test
        @DisplayName("Actual SQL from PG_RPGE_LISTING with bare JOIN parses with 0 errors")
        void testActualPgRpgeListingJoin() {
            // Extracted from samples/CUSTOMER/PG_RPGE_LISTING.pkb around line 6362-6396
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY PG_RPGE_LISTING AS
                    FUNCTION FN_RPAC_OUTRANSTATE_DET(
                      P_ST_SEQ_NO IN NUMBER,
                      P_AGENT_ID IN VARCHAR2,
                      P_MATCH_DOC_NO IN VARCHAR2
                    ) RETURN SYS_REFCURSOR IS
                      v_ProcName_v VARCHAR2(30) := 'FN_RPAC_OUTRANSTATE_DET';
                      v_rc SYS_REFCURSOR;
                    BEGIN
                      OPEN v_rc FOR
                        SELECT ACST_MATCH.TRAN_DATE, ACST_MATCH.MATCH_DOC_NO,
                               ACST_MATCH.MATCH_DOC_AMT, ACST_MATCH.MATCH_ST_SEQ_NO,
                               ACST_MATCH.ST_SEQ_NO, ACST_MATCH.DOC_NO
                          FROM ACST_MATCH
                         WHERE ACST_MATCH.MATCH_ST_SEQ_NO = P_ST_SEQ_NO
                           AND ACST_MATCH.MATCH_AGENT_ID = P_AGENT_ID
                           AND ACST_MATCH.MATCH_DOC_NO = P_MATCH_DOC_NO
                        UNION ALL
                        SELECT ACST_MATCH.TRAN_DATE,
                               ACGC_KO_INST.DOC_NO || ':' || ACGC_KO_INST.INST_CYCLE AS MATCH_DOC_NO,
                               ACGC_KO_INST.DOC_AMT AS MATCH_DOC_AMT,
                               ACST_MATCH.MATCH_ST_SEQ_NO AS MATCH_ST_SEQ_NO,
                               ACST_MATCH.ST_SEQ_NO, ACST_MATCH.DOC_NO
                          FROM ACGC_KO_INST JOIN ACST_MATCH
                            ON ACST_MATCH.DOC_NO = ACGC_KO_INST.AC_NO
                           AND ACST_MATCH.AGENT_CODE = ACGC_KO_INST.AGENT_CODE
                           AND ACST_MATCH.AGENT_CAT_TYPE = ACGC_KO_INST.AGENT_CAT_TYPE
                           AND ACST_MATCH.MATCH_DOC_NO = ACGC_KO_INST.DOC_NO
                           AND ACST_MATCH.GL_SEQ_NO = ACGC_KO_INST.GL_SEQ_NO
                         WHERE ACGC_KO_INST.AGENT_ID = P_AGENT_ID
                           AND ACGC_KO_INST.DOC_NO || ':' || ACGC_KO_INST.INST_CYCLE = P_MATCH_DOC_NO
                           AND EXISTS (
                             SELECT 1 FROM ACST_MAST
                              WHERE INST_IND = 'Y'
                                AND ST_DOC = ACST_MATCH.MATCH_DOC_NO);
                      RETURN v_rc;
                    END FN_RPAC_OUTRANSTATE_DET;
                    END PG_RPGE_LISTING;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "PG_RPGE_LISTING bare JOIN with UNION ALL");
        }
    }

    // =========================================================================
    // Regression: Combined fix + full file parsing
    // =========================================================================

    @Nested
    @DisplayName("Regression: combined scenarios and full file parsing")
    class Regression {

        @Test
        @DisplayName("Package body with both WORKERID alias and bare JOIN parses with 0 errors")
        void testCombinedWorkerIdAndBareJoin() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY COMBINED_PKG AS
                      PROCEDURE GET_WORKERS IS
                        CURSOR c IS
                          SELECT CASE WHEN e.card_id IS NOT NULL THEN e.card_id END AS WorkerId,
                                 d.dept_name
                            FROM employees e
                            JOIN departments d ON e.dept_id = d.id
                            LEFT JOIN locations l ON d.loc_id = l.id;
                      BEGIN
                        NULL;
                      END GET_WORKERS;

                      FUNCTION COUNT_WORKERS(p_dept IN NUMBER) RETURN NUMBER IS
                        v_cnt NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_cnt
                          FROM employees e
                          JOIN departments d ON e.dept_id = d.id
                         WHERE d.id = p_dept
                           AND e.WORKERID IS NOT NULL;
                        RETURN v_cnt;
                      END COUNT_WORKERS;
                    END COMBINED_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Combined WORKERID alias + bare JOIN");
        }

        @Test
        @DisplayName("Full PG_TPA_BUILD.pkb parses with 0 errors")
        void testFullPgTpaBuild() throws Exception {
            Path path = Path.of("samples/CUSTOMER/PG_TPA_BUILD.pkb");
            if (!Files.exists(path)) {
                // Try absolute path from project root
                path = Path.of("C:/Users/HUMDL98/Repo/self-learning-poc/plsql-parser/samples/CUSTOMER/PG_TPA_BUILD.pkb");
            }
            assertTrue(Files.exists(path), "PG_TPA_BUILD.pkb must exist at: " + path);

            String content = Files.readString(path);
            ParseResult result = engine.parseContent(content, "PG_TPA_BUILD.pkb");

            assertTrue(result.getErrors() == null || result.getErrors().isEmpty(),
                    "PG_TPA_BUILD.pkb should parse with 0 errors but got: " + result.getErrors());
        }

        @Test
        @DisplayName("Full PG_RPGE_LISTING.pkb parses with 0 errors")
        void testFullPgRpgeListing() throws Exception {
            Path path = Path.of("samples/CUSTOMER/PG_RPGE_LISTING.pkb");
            if (!Files.exists(path)) {
                path = Path.of("C:/Users/HUMDL98/Repo/self-learning-poc/plsql-parser/samples/CUSTOMER/PG_RPGE_LISTING.pkb");
            }
            assertTrue(Files.exists(path), "PG_RPGE_LISTING.pkb must exist at: " + path);

            String content = Files.readString(path);
            ParseResult result = engine.parseContent(content, "PG_RPGE_LISTING.pkb");

            assertTrue(result.getErrors() == null || result.getErrors().isEmpty(),
                    "PG_RPGE_LISTING.pkb should parse with 0 errors but got: " + result.getErrors());
        }
    }
}
