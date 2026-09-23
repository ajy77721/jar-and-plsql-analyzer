package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for XML function grammar rules:
 *
 *  Rule 1 — XMLELEMENT with double-quoted element name (DELIMITED_ID):
 *    XMLELEMENT("tagName", expr)
 *    This exercises the (NAME|EVALNAME)? expression path in the XMLELEMENT rule
 *    where the element name is a DELIMITED_ID rather than a plain identifier.
 *
 *  Rule 2 — XMLFOREST with double-quoted column aliases (DELIMITED_ID):
 *    XMLFOREST(v_col AS "alias")
 *    This exercises the xml_multiuse_expression_element rule's AS? id_expression
 *    path where id_expression resolves to DELIMITED_ID.
 *
 *  Rule 3 — XMLSERIALIZE with bare INDENT (no SIZE clause):
 *    XMLSERIALIZE(DOCUMENT expr AS CLOB INDENT)
 *    This exercises xmlserialize_param_ident_part where INDENT appears without
 *    an optional SIZE = N clause.
 *
 *  Rule 4 — Combined real-world pattern from PG_RNGE_POL_CALC package body:
 *    SELECT REPLACE(...XML...) XML INTO v_xml FROM (
 *      SELECT XMLSERIALIZE(DOCUMENT XMLELEMENT("coverNote", XMLFOREST(... AS "alias"...)) AS CLOB INDENT) AS XML
 *      FROM DUAL)
 */
@DisplayName("XML Function Grammar Tests")
public class XmlFunctionGrammarTest extends ParserTestBase {

    // =========================================================================
    // Rule 1: XMLELEMENT with double-quoted element name
    // =========================================================================

    @Nested
    @DisplayName("Rule 1 — XMLELEMENT with double-quoted element name")
    class XmlElementDoubleQuotedName {

        @Test
        @DisplayName("XMLELEMENT(\"tagName\", expr) with simple expression body")
        void xmlElementDoubleQuotedNameSimple() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                      BEGIN
                        SELECT XMLELEMENT("coverNote", 'value')
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLELEMENT with double-quoted element name (simple)");
        }

        @Test
        @DisplayName("XMLELEMENT(\"tagName\", column_expr) with column reference body")
        void xmlElementDoubleQuotedNameWithColumn() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                        v_region VARCHAR2(10);
                      BEGIN
                        SELECT XMLELEMENT("region", v_region)
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLELEMENT with double-quoted name and column ref");
        }

        @Test
        @DisplayName("XMLELEMENT(NAME tagName, expr) — explicit NAME keyword still works")
        void xmlElementExplicitNameKeyword() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                      BEGIN
                        SELECT XMLELEMENT(NAME coverNote, 'value')
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLELEMENT with NAME keyword");
        }

        @Test
        @DisplayName("XMLELEMENT(\"coverNote\", XMLFOREST(...)) — nested XMLFOREST in XMLELEMENT")
        void xmlElementWithNestedXmlForest() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                        v_region VARCHAR2(10) := 'MY';
                        v_date DATE := SYSDATE;
                      BEGIN
                        SELECT XMLELEMENT(
                                   "coverNote",
                                   XMLFOREST(v_region AS "region", v_date AS "issueDate"))
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLELEMENT with double-quoted name and nested XMLFOREST");
        }

        @Test
        @DisplayName("XMLELEMENT(\"coverNote\", xmlattributes_clause, expr) with attributes")
        void xmlElementWithAttributes() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                      BEGIN
                        SELECT XMLELEMENT("item",
                                          XMLATTRIBUTES('active' AS "status"),
                                          'content')
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLELEMENT with attributes");
        }
    }

    // =========================================================================
    // Rule 2: XMLFOREST with double-quoted aliases
    // =========================================================================

    @Nested
    @DisplayName("Rule 2 — XMLFOREST with double-quoted aliases")
    class XmlForestDoubleQuotedAlias {

        @Test
        @DisplayName("XMLFOREST(v_col AS \"alias\") — single double-quoted alias")
        void xmlForestSingleDoubleQuotedAlias() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                        v_region VARCHAR2(10) := 'MY';
                      BEGIN
                        SELECT XMLFOREST(v_region AS "region")
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLFOREST with single double-quoted alias");
        }

        @Test
        @DisplayName("XMLFOREST(a AS \"alias1\", b AS \"alias2\") — multiple double-quoted aliases")
        void xmlForestMultipleDoubleQuotedAliases() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                        v_region VARCHAR2(10) := 'MY';
                        v_date DATE := SYSDATE;
                        v_amount NUMBER := 100;
                      BEGIN
                        SELECT XMLFOREST(
                                   v_region AS "region",
                                   v_date   AS "issueDate",
                                   v_amount AS "sumInsured")
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLFOREST with multiple double-quoted aliases");
        }

        @Test
        @DisplayName("XMLFOREST with mixed: double-quoted, plain, and expression aliases")
        void xmlForestMixedAliases() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                        v_a VARCHAR2(10) := 'A';
                        v_b VARCHAR2(10) := 'B';
                        v_c VARCHAR2(10) := 'C';
                      BEGIN
                        SELECT XMLFOREST(
                                   v_a AS "camelCaseAlias",
                                   v_b AS plainAlias,
                                   v_c AS "another-alias")
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLFOREST with mixed alias types");
        }

        @Test
        @DisplayName("XMLFOREST with literal string value and double-quoted alias")
        void xmlForestLiteralWithDoubleQuotedAlias() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                      BEGIN
                        SELECT XMLFOREST(
                                   ''          AS "bankclientType",
                                   SYSDATE     AS "issueDate",
                                   1           AS "version")
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLFOREST with literal values and double-quoted aliases");
        }

        @Test
        @DisplayName("XMLFOREST with values on separate lines from aliases (pkb file format)")
        void xmlForestSeparateLineFormat() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                        v_region VARCHAR2(10) := 'MY';
                        v_date DATE := SYSDATE;
                        v_amount NUMBER := 100;
                      BEGIN
                        SELECT XMLFOREST(
                                   v_region
                                       AS "region",
                                   v_date
                                       AS "issueDate",
                                   v_amount
                                       AS "sumInsured")
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLFOREST with values/aliases on separate lines");
        }
    }

    // =========================================================================
    // Rule 3: XMLSERIALIZE with bare INDENT
    // =========================================================================

    @Nested
    @DisplayName("Rule 3 — XMLSERIALIZE with bare INDENT keyword")
    class XmlSerializeIndent {

        @Test
        @DisplayName("XMLSERIALIZE(DOCUMENT expr AS CLOB INDENT) — bare INDENT without SIZE")
        void xmlSerializeBareIndent() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml CLOB;
                      BEGIN
                        SELECT XMLSERIALIZE(DOCUMENT XMLELEMENT("root", 'content') AS CLOB INDENT)
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLSERIALIZE with bare INDENT");
        }

        @Test
        @DisplayName("XMLSERIALIZE(CONTENT expr AS CLOB INDENT) — CONTENT variant with INDENT")
        void xmlSerializeContentWithIndent() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml CLOB;
                      BEGIN
                        SELECT XMLSERIALIZE(CONTENT XMLELEMENT("root", 'content') AS CLOB INDENT)
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLSERIALIZE(CONTENT) with bare INDENT");
        }

        @Test
        @DisplayName("XMLSERIALIZE(DOCUMENT expr AS CLOB NO INDENT)")
        void xmlSerializeNoIndent() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml CLOB;
                      BEGIN
                        SELECT XMLSERIALIZE(DOCUMENT XMLELEMENT("root", 'content') AS CLOB NO INDENT)
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLSERIALIZE with NO INDENT");
        }

        @Test
        @DisplayName("XMLSERIALIZE without INDENT (just AS CLOB)")
        void xmlSerializeNoIndentClause() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml CLOB;
                      BEGIN
                        SELECT XMLSERIALIZE(DOCUMENT XMLELEMENT("root", 'content') AS CLOB)
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLSERIALIZE without INDENT");
        }

        @Test
        @DisplayName("XMLSERIALIZE(DOCUMENT expr AS CLOB INDENT SIZE = 2)")
        void xmlSerializeIndentWithSize() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml CLOB;
                      BEGIN
                        SELECT XMLSERIALIZE(DOCUMENT XMLELEMENT("root", 'content') AS CLOB INDENT SIZE = 2)
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLSERIALIZE with INDENT SIZE = 2");
        }
    }

    // =========================================================================
    // Rule 4: Combined real-world pattern from PG_RNGE_POL_CALC
    // =========================================================================

    @Nested
    @DisplayName("Rule 4 — Combined real-world XML pattern (PG_RNGE_POL_CALC style)")
    class CombinedXmlPattern {

        @Test
        @DisplayName("SELECT REPLACE(...XML...) XML INTO v_xml FROM (SELECT XMLSERIALIZE(...) AS XML FROM DUAL)")
        void combinedPatternCompact() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY PG_RNGE_POL_CALC AS
                      PROCEDURE TEST_PROC IS
                        V_XML CLOB;
                        v_region VARCHAR2(50);
                        v_inceptionDate DATE;
                        v_expiryDate DATE;
                        v_sumInsured NUMBER;
                      BEGIN
                        SELECT REPLACE(REPLACE(REPLACE(REPLACE(XML,'<NAMEDDRVRS>',''),'</NAMEDDRVRS>',''),'<NAMEDDRVRS/>',''),'NAMEDDRVR','namedDriverList') XML
                          INTO V_XML
                          FROM (SELECT XMLSERIALIZE(
                                           DOCUMENT XMLELEMENT(
                                               "coverNote",
                                               XMLFOREST(
                                                   v_region        AS "region",
                                                   ''              AS "bankclientType",
                                                   SYSDATE         AS "issueDate",
                                                   v_inceptionDate AS "inceptionDate",
                                                   v_expiryDate    AS "expiryDate",
                                                   v_sumInsured    AS "sumInsured"
                                               )
                                           ) AS CLOB INDENT
                                       ) AS XML
                                FROM DUAL);
                      END TEST_PROC;
                    END PG_RNGE_POL_CALC;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Combined XML pattern (compact)");
        }

        @Test
        @DisplayName("Same pattern with values on separate lines from aliases (actual pkb format)")
        void combinedPatternWithSeparateLines() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY PG_RNGE_POL_CALC AS
                      PROCEDURE TEST_PROC IS
                        V_XML CLOB;
                        v_region VARCHAR2(50);
                        v_inceptionDate DATE;
                        v_expiryDate DATE;
                        v_sumInsured NUMBER;
                      BEGIN
                        SELECT REPLACE (
                                   REPLACE (
                                       REPLACE (
                                           REPLACE (XML, '<NAMEDDRVRS>', ''),
                                           '</NAMEDDRVRS>',
                                           ''),
                                       '<NAMEDDRVRS/>',
                                       ''),
                                   'NAMEDDRVR',
                                   'namedDriverList') XML
                          INTO V_XML
                          FROM (SELECT XMLSERIALIZE (
                                           DOCUMENT XMLELEMENT (
                                                        "coverNote",
                                                        XMLFOREST (
                                                            v_region
                                                                AS "region",
                                                            ''
                                                                AS "bankclientType",
                                                            SYSDATE
                                                                AS "issueDate",
                                                            v_inceptionDate
                                                                AS "inceptionDate",
                                                            v_expiryDate
                                                                AS "expiryDate",
                                                            v_sumInsured
                                                                AS "sumInsured"
                                                        )
                                                    ) AS CLOB INDENT
                                       ) AS XML
                                FROM DUAL);
                      END TEST_PROC;
                    END PG_RNGE_POL_CALC;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Combined XML pattern (pkb multiline format)");
        }

        @Test
        @DisplayName("XMLELEMENT with double-quoted name inside XMLSERIALIZE inside subquery")
        void xmlElementInXmlSerializeInSubquery() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_doc CLOB;
                        v_val VARCHAR2(100) := 'hello';
                      BEGIN
                        SELECT XMLSERIALIZE(DOCUMENT XMLELEMENT("myTag", v_val) AS CLOB INDENT)
                          INTO v_doc
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLELEMENT in XMLSERIALIZE in subquery");
        }

        @Test
        @DisplayName("XMLCOLATTVAL with double-quoted alias")
        void xmlColAttValDoubleQuotedAlias() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                        v_region VARCHAR2(10) := 'MY';
                      BEGIN
                        SELECT XMLCOLATTVAL(v_region AS "region", SYSDATE AS "date")
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLCOLATTVAL with double-quoted aliases");
        }

        @Test
        @DisplayName("Full PG_RNGE_POL_CALC-style procedure with complex XML generation")
        void fullPgRngePolCalcStyle() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY PG_RNGE_POL_CALC AS
                      PROCEDURE PC_RN_POL_CALC_MT IS
                        V_XML CLOB;
                        v_region VARCHAR2(10) := 'MY';
                        v_inceptionDate DATE := SYSDATE;
                        v_expiryDate DATE := SYSDATE + 365;
                        v_sumInsured NUMBER(18,2) := 50000;
                        v_basicAnnualPremium NUMBER(18,2) := 1200;
                        v_type VARCHAR2(20) := 'IND';
                        v_subchannel VARCHAR2(20);
                        v_channel VARCHAR2(20);
                      BEGIN
                        -- Simple SELECT INTO
                        SELECT SUBCHANNEL, CHANNEL
                          INTO v_subchannel, v_channel
                          FROM dmag_agents
                         WHERE INT_ID = 1;

                        -- XML generation with REPLACE, XMLSERIALIZE, XMLELEMENT, XMLFOREST
                        SELECT REPLACE (
                                   REPLACE (
                                       REPLACE (
                                           REPLACE (XML, '<NAMEDDRVRS>', ''),
                                           '</NAMEDDRVRS>',
                                           ''),
                                       '<NAMEDDRVRS/>',
                                       ''),
                                   'NAMEDDRVR',
                                   'namedDriverList') XML
                          INTO V_XML
                          FROM (SELECT XMLSERIALIZE (
                                           DOCUMENT XMLELEMENT (
                                                        "coverNote",
                                                        XMLFOREST (
                                                            v_region
                                                                AS "region",
                                                            ''
                                                                AS "bankclientType",
                                                            SYSDATE
                                                                AS "issueDate",
                                                            v_inceptionDate
                                                                AS "inceptionDate",
                                                            v_expiryDate
                                                                AS "expiryDate",
                                                            v_sumInsured
                                                                AS "sumInsured",
                                                            v_basicAnnualPremium
                                                                AS "basicAnnualPremium",
                                                            v_type
                                                                AS "clientType"
                                                        )
                                                    ) AS CLOB INDENT
                                       ) AS XML
                                FROM DUAL);
                      END PC_RN_POL_CALC_MT;
                    END PG_RNGE_POL_CALC;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "Full PG_RNGE_POL_CALC style procedure");
        }
    }

    // =========================================================================
    // Regression: other XML functions unaffected
    // =========================================================================

    @Nested
    @DisplayName("Regression — other XML functions still parse correctly")
    class XmlRegressionTests {

        @Test
        @DisplayName("XMLAGG with ORDER BY still works")
        void xmlAggStillWorks() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                      BEGIN
                        SELECT XMLAGG(XMLELEMENT("item", col) ORDER BY col)
                          INTO v_xml
                          FROM dual;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLAGG regression");
        }

        @Test
        @DisplayName("XMLROOT still works")
        void xmlRootStillWorks() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                      BEGIN
                        SELECT XMLROOT(XMLELEMENT("root", 'value'), VERSION '1.0')
                          INTO v_xml
                          FROM dual;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLROOT regression");
        }

        @Test
        @DisplayName("XMLPARSE still works")
        void xmlParseStillWorks() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml XMLTYPE;
                      BEGIN
                        SELECT XMLPARSE(DOCUMENT '<root>value</root>')
                          INTO v_xml
                          FROM dual;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XMLPARSE regression");
        }

        @Test
        @DisplayName("XML used as column alias (keyword as identifier) still works")
        void xmlAsColumnAliasStillWorks() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
                      PROCEDURE P IS
                        v_xml CLOB;
                      BEGIN
                        SELECT XMLSERIALIZE(DOCUMENT XMLELEMENT("root", 'val') AS CLOB) AS XML
                          INTO v_xml
                          FROM DUAL;
                      END P;
                    END TEST_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "XML as column alias");
        }
    }
}
