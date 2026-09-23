package com.plsqlanalyzer.web.controller.detail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * UI-030b — {@link ProcedureCrossReportBuilder}, the Java port of
 * {@code plsql/js/summary-analyzer.js}'s four cross-procedure report builders.
 *
 * <p>Fixtures are built directly as the {@code procReports} shape {@link ProcedureReportBuilder}
 * itself produces (flowTables as a map of name → {schemaName, operations[]}), rather than a
 * separately-invented shape, so these tests exercise the real field names the two classes agree on.
 */
class ProcedureCrossReportBuilderTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static JsonNode json(String s) throws Exception {
        return M.readTree(s);
    }

    /**
     * Calibrated by changing the multi-table-single-schema branch's requirement string from
     * "RECOMMENDED..." to "REQUIRED..." (matching the cross-schema branch) — confirmed the test
     * failed on the exact string before restoring.
     */
    @Test
    void transactionReportPicksTheRightRequirementByWriteSchemaCount() throws Exception {
        ArrayNode reports = (ArrayNode) json("["
                + "{\"name\":\"P_SINGLE\",\"id\":\"P1\",\"schemaName\":\"ERP\",\"packageName\":\"\",\"unitType\":\"PROCEDURE\","
                + " \"tableCount\":1,\"loc\":10,\"complexity\":\"Low\","
                + " \"flowTables\":{\"A\":{\"schemaName\":\"ERP\",\"operations\":[\"UPDATE\"]}}},"
                + "{\"name\":\"P_MULTI_SAME_SCHEMA\",\"id\":\"P2\",\"schemaName\":\"ERP\",\"packageName\":\"\",\"unitType\":\"PROCEDURE\","
                + " \"tableCount\":2,\"loc\":20,\"complexity\":\"Medium\","
                + " \"flowTables\":{\"A\":{\"schemaName\":\"ERP\",\"operations\":[\"UPDATE\"]},"
                + "                  \"B\":{\"schemaName\":\"ERP\",\"operations\":[\"INSERT\"]}}},"
                + "{\"name\":\"P_CROSS_SCHEMA\",\"id\":\"P3\",\"schemaName\":\"ERP\",\"packageName\":\"\",\"unitType\":\"PROCEDURE\","
                + " \"tableCount\":2,\"loc\":30,\"complexity\":\"High\","
                + " \"flowTables\":{\"A\":{\"schemaName\":\"ERP\",\"operations\":[\"UPDATE\"]},"
                + "                  \"B\":{\"schemaName\":\"BILLING\",\"operations\":[\"INSERT\"]}}},"
                + "{\"name\":\"P_READONLY\",\"id\":\"P4\",\"schemaName\":\"ERP\",\"packageName\":\"\",\"unitType\":\"PROCEDURE\","
                + " \"tableCount\":1,\"loc\":5,\"complexity\":\"Low\","
                + " \"flowTables\":{\"A\":{\"schemaName\":\"ERP\",\"operations\":[\"SELECT\"]}}}"
                + "]");
        ArrayNode results = ProcedureCrossReportBuilder.buildTransactionReport(M, reports);
        assertEquals(4, results.size());
        assertEquals("SINGLE TABLE", results.get(0).get("transactionRequirement").asText());
        assertEquals("RECOMMENDED - multi-table DML in single schema", results.get(1).get("transactionRequirement").asText());
        assertTrue(results.get(2).get("transactionRequirement").asText().startsWith("REQUIRED - cross-schema"));
        assertEquals("READ ONLY", results.get(3).get("transactionRequirement").asText());
    }

    /**
     * Calibrated by changing the schema-slice sort comparator from descending to ascending —
     * confirmed the order flipped (ERP, with the lower callCount, moved first) before restoring.
     */
    @Test
    void schemaSliceReportAggregatesPerSchemaAndSortsByCallCountDescending() throws Exception {
        ArrayNode reports = (ArrayNode) json("["
                + "{\"name\":\"P1\",\"schemas\":[\"ERP\"],"
                + " \"flowTables\":{\"A\":{\"schemaName\":\"ERP\",\"operations\":[\"SELECT\"]}}},"
                + "{\"name\":\"P2\",\"schemas\":[\"BILLING\"],"
                + " \"flowTables\":{\"B\":{\"schemaName\":\"BILLING\",\"operations\":[\"INSERT\"]}}},"
                + "{\"name\":\"P3\",\"schemas\":[\"BILLING\"],"
                + " \"flowTables\":{\"B\":{\"schemaName\":\"BILLING\",\"operations\":[\"UPDATE\"]}}}"
                + "]");
        ArrayNode results = ProcedureCrossReportBuilder.buildSchemaSliceReport(M, reports);
        assertEquals(2, results.size());
        assertEquals("BILLING", results.get(0).get("schema").asText(), "BILLING has callCount 2, must sort first");
        assertEquals(2, results.get(0).get("callCount").asInt());
        assertEquals("ERP", results.get(1).get("schema").asText());
    }

    /**
     * Calibrated by changing the EXTERNAL callType filter to also accept "INTERNAL" — confirmed a
     * non-external call started polluting the dependency map before restoring.
     */
    @Test
    void externalDepsReportOnlyCountsExternalCalls() throws Exception {
        ArrayNode reports = (ArrayNode) json("["
                + "{\"name\":\"P1\",\"calls\":["
                + "  {\"name\":\"EXT_PROC\",\"callType\":\"EXTERNAL\",\"schemaName\":\"BILLING\",\"packageName\":\"PKG1\"},"
                + "  {\"name\":\"LOCAL_PROC\",\"callType\":\"INTERNAL\",\"schemaName\":\"ERP\"}"
                + "]}"
                + "]");
        ArrayNode results = ProcedureCrossReportBuilder.buildExternalDepsReport(M, reports);
        assertEquals(1, results.size());
        assertEquals("BILLING", results.get(0).get("schema").asText());
        assertEquals(1, results.get(0).get("count").asInt());
    }

    /**
     * Calibrated by removing the "or check calls" fallback branch — confirmed a procedure whose own
     * name/package don't match but whose call list does (DBMS_SCHEDULER) dropped out of the results
     * before restoring.
     */
    @Test
    void batchReportMatchesOwnNameOrAnyCallsKeyword() throws Exception {
        ArrayNode reports = (ArrayNode) json("["
                + "{\"name\":\"NIGHTLY_RECON\",\"packageName\":\"\",\"calls\":[]},"
                + "{\"name\":\"RUN_STEP\",\"packageName\":\"\","
                + " \"calls\":[{\"name\":\"DBMS_SCHEDULER.CREATE_JOB\",\"packageName\":\"\"}]},"
                + "{\"name\":\"NORMAL_PROC\",\"packageName\":\"\",\"calls\":[]}"
                + "]");
        ArrayNode results = ProcedureCrossReportBuilder.buildBatchReport(M, reports);
        assertEquals(2, results.size());
        assertEquals("NIGHTLY_RECON", results.get(0).get("procName").asText());
        assertEquals("RUN_STEP", results.get(1).get("procName").asText());
        assertEquals("DBMS_SCHEDULER", results.get(1).get("matchedKeyword").asText());
    }
}
