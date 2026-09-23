package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 3-part (schema.package.function) pipelined table function references in FROM clause.
 *
 * Grammar fix: tableview_name changed from ('.' id_expression)? to ('.' id_expression)*
 * so that 3-part names like customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY are valid.
 *
 * Trigger case: PG_RNGE_POL_CALC_TEST.FN_RECAL_RNGE_PREMIUM lines 7440/7442:
 *   FROM customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY(p_pol, p_ren) claimHist
 */
@DisplayName("3-Part Tableview Name Grammar Fix")
public class ThreePartTableviewNameTest extends ParserTestBase {

    @Nested
    @DisplayName("Basic 3-part pipelined function in FROM clause")
    class BasicThreePartFromClause {

        @Test
        @DisplayName("schema.package.function() in FROM — simple SELECT")
        void threePartFunctionInFrom() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_count NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_count
                          FROM customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY(1, 2) claimHist;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "3-part function in FROM");
        }

        @Test
        @DisplayName("schema.package.function() with column selection")
        void threePartFunctionWithColumns() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_no VARCHAR2(20);
                        v_year NUMBER;
                      BEGIN
                        SELECT claimHist.clm_no, claimHist.year_renewal
                          INTO v_no, v_year
                          FROM customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY('POL001', 1) claimHist
                         WHERE ROWNUM = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "3-part function with column select");
        }

        @Test
        @DisplayName("Two-part schema.function() still works (regression)")
        void twoPartFunctionNotBroken() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_count NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_count
                          FROM schema_name.MY_TABLE_FN(1) tbl;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "2-part function in FROM regression");
        }

        @Test
        @DisplayName("Plain table name still works (regression)")
        void plainTableNotBroken() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_count NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_count FROM employees;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "plain table regression");
        }

        @Test
        @DisplayName("schema.table 2-part name still works (regression)")
        void twoPartTableNotBroken() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_count NUMBER;
                      BEGIN
                        SELECT COUNT(*) INTO v_count FROM hr.employees;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "2-part table regression");
        }
    }

    @Nested
    @DisplayName("3-part name in XMLAGG subquery context (real-world pattern)")
    class XmlAggWithThreePartFunction {

        @Test
        @DisplayName("3-part function inside XMLAGG subquery FROM clause")
        void threePartInXmlAggSubquery() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_xml CLOB;
                      BEGIN
                        SELECT XMLSERIALIZE(DOCUMENT
                                 XMLELEMENT("claimHistoryList",
                                   (SELECT XMLAGG(XMLELEMENT("claimHistory",
                                              XMLFOREST(claimHistMain.year_renewal AS "yearRenew",
                                                        claimHistMain.gross_prem AS "grossPremium")))
                                      FROM customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY('POL1', 1) claimHistMain))
                               AS CLOB)
                          INTO v_xml
                          FROM DUAL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "3-part function inside XMLAGG subquery");
        }

        @Test
        @DisplayName("Nested 3-part function: inner claimHist inside outer claimHistMain subquery")
        void nestedThreePartInXmlElement() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_xml CLOB;
                      BEGIN
                        SELECT XMLSERIALIZE(DOCUMENT
                                 XMLELEMENT("claimHistoryList",
                                   (SELECT XMLAGG(XMLELEMENT("claimHistory",
                                              XMLFOREST(claimHistMain.year_renewal AS "yearRenew"),
                                              XMLELEMENT("claimList",
                                                (SELECT XMLAGG(XMLELEMENT("claim",
                                                           XMLFOREST(claimHist.clm_no AS "claimNo")))
                                                   FROM customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY('POL1', 1) claimHist))))
                                      FROM customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY('POL1', 1) claimHistMain))
                               AS CLOB)
                          INTO v_xml
                          FROM DUAL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "nested 3-part function in XMLELEMENT");
        }

        @Test
        @DisplayName("Full PG_RNGE_POL_CALC_TEST claimHistory pattern — exact reproduction")
        void fullClaimHistoryPattern() {
            // Paren structure:
            //   XMLSERIALIZE(DOCUMENT XMLELEMENT("policy", XMLELEMENT("claimHistoryList",
            //     (SELECT XMLAGG(XMLELEMENT("claimHistory", XMLFOREST(...),
            //        XMLELEMENT("claimList", (SELECT XMLAGG(XMLELEMENT("claim", XMLFOREST(...)))
            //          FROM 3part claimHist))))
            //       FROM 3part claimHistMain)
            //   )) AS CLOB INDENT) AS XML
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC(p_pol IN VARCHAR2, p_ren IN NUMBER) IS
                        v_xml CLOB;
                      BEGIN
                        SELECT REPLACE(XML, '', '') XML
                          INTO v_xml
                          FROM (SELECT XMLSERIALIZE(
                                         DOCUMENT XMLELEMENT(
                                                    "policy",
                                                    XMLELEMENT(
                                                       "claimHistoryList",
                                                       (SELECT XMLAGG(
                                                                  XMLELEMENT(
                                                                     "claimHistory",
                                                                     XMLFOREST(
                                                                        claimHistMain.year_renewal AS "yearRenew",
                                                                        claimHistMain.gross_prem AS "grossPremium"),
                                                                     XMLELEMENT(
                                                                        "claimList",
                                                                        (SELECT XMLAGG(
                                                                                   XMLELEMENT(
                                                                                      "claim",
                                                                                      XMLFOREST(
                                                                                         claimHist.clm_no AS "claimNo",
                                                                                         claimHist.resv_grp AS "reserveGroup")))
                                                                           FROM customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY(p_pol, p_ren) claimHist)
                                                                     )
                                                                  )
                                                               )
                                                          FROM customer.PG_CLAIM_HISTORY.FN_CLAIM_HISTORY(p_pol, p_ren) claimHistMain)
                                                    )
                                                 )
                                        AS CLOB INDENT) AS XML
                               FROM DUAL);
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "full claim history 3-part function pattern");
        }
    }

    @Nested
    @DisplayName("DB link still works with multi-part names (regression)")
    class DbLinkRegression {

        @Test
        @DisplayName("schema.table@dblink still parses correctly")
        void twoPartTableWithDbLink() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM remote_schema.employees@MY_LINK WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "schema.table@dblink regression");
        }
    }
}
