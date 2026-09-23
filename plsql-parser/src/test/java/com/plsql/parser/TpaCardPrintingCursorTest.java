package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PG_TPA_BUILD PC_TPA_CARD_PRINTING_POL_ENDT — real cursor reproduction")
public class TpaCardPrintingCursorTest extends ParserTestBase {

    // Exact structure of the real cursor including scalar subqueries in SELECT list
    private static final String SOURCE = """
            CREATE OR REPLACE PACKAGE BODY PG_TPA_BUILD AS
              PROCEDURE PC_TPA_CARD_PRINTING_POL_ENDT (P_START_DT IN VARCHAR2) IS
                CURSOR C_TPA_CARD_PRINTING IS
                  SELECT (CASE WHEN UPV.VERSION_NO = 1 THEN 'PL' ELSE 'EN' END)
                             AS TRANSACTION_TYPE,
                         UPC.PRODUCT_CONFIG_CODE,
                         (SELECT CODE_DESC
                            FROM CMGE_CODE CC
                           WHERE CC.CODE_CD = UPC.PRODUCT_CONFIG_CODE
                             AND CC.CAT_CODE = UPC.LOB || '_PRODUCT')
                             AS PRODUCT_DESC,
                         CP.NAME_EXT,
                         (SELECT PC_VALUE_DESCRIPTION
                            FROM PDC_V_PROD_ATTRIBUTE_VALUES
                           WHERE VALUE = URP.INSURED_TYPE
                             AND ATTRIBUTE = 'INSURED_TYPE'
                             AND PRODUCT_CODE = UPC.PRODUCT_CONFIG_CODE)
                             AS INSURED_TYPE,
                         (CASE
                              WHEN UPV.VERSION_NO = 1 THEN OPB.POLICY_REF
                              ELSE UPV.ENDT_NO
                          END) AS POL_NO,
                         UPLC.PLAN_CODE,
                         (SELECT CODE_DESC
                            FROM CMGE_CODE CC
                           WHERE CC.CAT_CODE = 'MS_PLAN'
                             AND CC.CODE_CD = UPLC.PLAN_CODE)
                             AS PLAN_DESC,
                         UPB.ISSUE_OFFICE,
                         (SELECT COUNT (1)
                            FROM UWGE_RISK TUR
                           WHERE TUR.CONTRACT_ID = UPV.CONTRACT_ID
                             AND TUR.VERSION_NO = UPV.VERSION_NO)
                             AS ISSHOWRISK,
                         UPV.VERSION_NO AS POLICY_VERSION,
                         UPV.CONTRACT_ID
                    FROM UWGE_POLICY_VERSIONS UPV
                         INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
                             ON UPV.CONTRACT_ID = UPCD.CONTRACT_ID
                            AND UPV.VERSION_NO = UPCD.VERSION_NO
                         INNER JOIN UWGE_POLICY_CONTRACTS UPC
                             ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
                         INNER JOIN OCP_POLICY_BASES OPB
                             ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
                         INNER JOIN UWGE_POLICY_BASES UPB
                             ON UPB.CONTRACT_ID = UPV.CONTRACT_ID
                            AND UPB.VERSION_NO = UPV.VERSION_NO
                         INNER JOIN SB_UWGE_RISK UR
                             ON UR.CONTRACT_ID = UPV.CONTRACT_ID
                            AND UR.POLICY_VERSION = UPV.VERSION_NO
                         INNER JOIN UWGE_COVER UCOV
                             ON UCOV.CONTRACT_ID = UPV.CONTRACT_ID
                            AND UCOV.VERSION_NO = UPV.VERSION_NO
                            AND UR.RISK_ID = UCOV.RISK_ID
                         INNER JOIN SB_UWPL_COVER UPLC
                             ON UPLC.CONTRACT_ID = UCOV.CONTRACT_ID
                            AND UPLC.COV_ID = UCOV.COV_ID
                         INNER JOIN SB_UWPL_RISK_PERSON URP
                             ON URP.CONTRACT_ID = UPV.CONTRACT_ID
                            AND URP.POLICY_VERSION = UPV.VERSION_NO
                            AND UR.RISK_ID = URP.RISK_ID
                         INNER JOIN
                         TABLE (
                             CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (
                                 URP.RISK_PART_ID,
                                 URP.RISK_PART_VER)) CP
                             ON CP.PART_ID = URP.RISK_PART_ID
                            AND CP.VERSION = URP.RISK_PART_VER
                   WHERE UPCD.DLOAD_STATUS = 'P'
                ORDER BY TRUNC (UCOV.COV_SEQ_REF);
              BEGIN
                FOR rec IN C_TPA_CARD_PRINTING LOOP
                  NULL;
                END LOOP;
              END PC_TPA_CARD_PRINTING_POL_ENDT;
            END PG_TPA_BUILD;
            /
            """;

    @Test
    @DisplayName("UWGE_POLICY_VERSIONS has all INNER JOINs — with scalar subqueries in SELECT list")
    void testCursorJoinsWithScalarSubqueriesInSelectList() {
        ParseResult result = parse(SOURCE);
        SubprogramInfo proc = findSub(result, "PC_TPA_CARD_PRINTING_POL_ENDT");
        assertNotNull(proc, "PC_TPA_CARD_PRINTING_POL_ENDT should be found");

        System.out.println("=== All table ops in proc ===");
        for (TableOperationInfo toi : proc.getTableOperations()) {
            System.out.println("  " + toi.getTableName()
                    + " line=" + toi.getLine()
                    + " joins=" + (toi.getJoins() != null ? toi.getJoins().size() : 0));
        }

        TableOperationInfo upvOp = proc.getTableOperations().stream()
                .filter(t -> "UWGE_POLICY_VERSIONS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(upvOp, "UWGE_POLICY_VERSIONS must be present");
        assertNotNull(upvOp.getJoins(), "Must have joins list");

        System.out.println("=== Joins on UWGE_POLICY_VERSIONS ===");
        for (JoinInfo ji : upvOp.getJoins()) {
            System.out.println("  JOIN " + ji.getJoinType() + " -> " + ji.getJoinedTable()
                    + " alias=" + ji.getJoinedTableAlias());
        }

        assertTrue(upvOp.getJoins().size() >= 8,
                "Expected >= 8 joins, got " + upvOp.getJoins().size());
    }

    @Test
    @DisplayName("FN_GEN_CP_TABLE detected in cursor with scalar subqueries")
    void testTableFunctionCallDetected() {
        ParseResult result = parse(SOURCE);
        ParsedObject pkg = result.getObjects().get(0);
        boolean found = false;
        for (CallInfo ci : pkg.getCalls()) {
            if ("FN_GEN_CP_TABLE".equalsIgnoreCase(ci.getName())) { found = true; break; }
        }
        for (SubprogramInfo sub : pkg.getSubprograms()) {
            for (CallInfo ci : sub.getCalls()) {
                if ("FN_GEN_CP_TABLE".equalsIgnoreCase(ci.getName())) { found = true; break; }
            }
        }
        assertTrue(found, "FN_GEN_CP_TABLE must be detected as a call");
    }
}
