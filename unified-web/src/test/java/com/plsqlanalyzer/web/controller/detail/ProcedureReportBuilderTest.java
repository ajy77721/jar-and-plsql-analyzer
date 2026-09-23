package com.plsqlanalyzer.web.controller.detail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * UI-030a — {@link ProcedureReportBuilder}, the Java port of
 * {@code plsql/js/summary-analyzer.js:_analyzeProcedure}, targeting the live-crawl backend's
 * {@code ProcedureDetailService} response shape.
 *
 * <p>Each test below was calibrated per this session's own discipline: written against the real
 * behavior, then the guarded line was deliberately broken and the test confirmed to fail, then
 * restored. See individual test comments for what was broken.
 */
class ProcedureReportBuilderTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static JsonNode detail(String json) throws Exception {
        return M.readTree(json);
    }

    /**
     * Calibrated by temporarily reading {@code t.get("name")} instead of {@code t.get("tableName")}
     * in the table-collection loop — confirmed the test failed (tableCount became 0) before
     * restoring. This is exactly the field-name mismatch that makes UI-030-BUG break the parser
     * backend; this test locks the live-crawl backend's correct field name in place.
     */
    @Test
    void buildsTableCountAndOpsFromDetailTablesOnly() throws Exception {
        JsonNode d = detail("{"
                + "\"id\":\"P1\",\"name\":\"ERP_USP_RUN\",\"schemaName\":\"ERP\",\"packageName\":\"\","
                + "\"startLine\":10,\"endLine\":60,"
                + "\"tables\":["
                + "  {\"tableName\":\"ACCOUNTS\",\"schemaName\":\"ERP\",\"operations\":[\"SELECT\",\"UPDATE\"]},"
                + "  {\"tableName\":\"LEDGER\",\"schemaName\":\"ERP\",\"operations\":[\"INSERT\"]}"
                + "],"
                + "\"callTree\":{\"id\":\"P1\",\"schemaName\":\"ERP\",\"callType\":\"INTERNAL\",\"children\":[]},"
                + "\"calls\":[]"
                + "}");
        ObjectNode report = ProcedureReportBuilder.build(M, d, "ERP");
        assertEquals(2, report.get("tableCount").asInt());
        assertEquals(2, report.get("writeTableCount").asInt(), "ACCOUNTS(UPDATE) and LEDGER(INSERT) both count");
        assertEquals(1, report.get("readTableCount").asInt(), "only ACCOUNTS has SELECT");
        assertTrue(report.get("directTables").has("ACCOUNTS"));
        assertTrue(report.get("flowTables").has("ACCOUNTS"), "flowTables must equal directTables (dead call-tree table walk)");
    }

    /**
     * Calibrated by changing the cross-schema condition's {@code !=} to {@code ==} — confirmed
     * crossSchemaCalls flipped from 1 to 0 (wrong direction) before restoring.
     */
    @Test
    void callTreeWalkCountsCrossSchemaCallsAndDepth() throws Exception {
        JsonNode d = detail("{"
                + "\"id\":\"P1\",\"name\":\"P1\",\"schemaName\":\"ERP\",\"startLine\":1,\"endLine\":5,"
                + "\"tables\":[],"
                + "\"callTree\":{\"id\":\"P1\",\"schemaName\":\"ERP\",\"callType\":\"INTERNAL\",\"children\":["
                + "  {\"id\":\"P2\",\"schemaName\":\"ERP\",\"callType\":\"INTERNAL\",\"children\":["
                + "    {\"id\":\"P3\",\"schemaName\":\"BILLING\",\"callType\":\"INTERNAL\",\"children\":[]}"
                + "  ]}"
                + "]},"
                + "\"calls\":[]"
                + "}");
        ObjectNode report = ProcedureReportBuilder.build(M, d, "ERP");
        assertEquals(3, report.get("totalCalls").asInt(), "root + 2 descendants");
        assertEquals(2, report.get("callDepth").asInt(), "root=0, P2=1, P3=2");
        assertEquals(1, report.get("crossSchemaCalls").asInt(), "only P3 (BILLING) crosses the ERP entry schema");
    }

    /**
     * Calibrated by swapping the {@code loc <= 200} threshold to {@code loc <= 20} — confirmed a
     * 60-line procedure flipped from "M" to "L" before restoring.
     */
    @Test
    void sizeCategoryMatchesJsThresholdsExactly() {
        assertEquals("S", ProcedureReportBuilder.sizeCategory(50));
        assertEquals("M", ProcedureReportBuilder.sizeCategory(51));
        assertEquals("M", ProcedureReportBuilder.sizeCategory(200));
        assertEquals("L", ProcedureReportBuilder.sizeCategory(201));
        assertEquals("L", ProcedureReportBuilder.sizeCategory(500));
        assertEquals("XL", ProcedureReportBuilder.sizeCategory(501));
    }

    /**
     * Calibrated by changing the writeTableCount weight from {@code * 2} to {@code * 1} — confirmed
     * the score dropped from 11 (High) to 9 (Medium) for the fixture below before restoring.
     */
    @Test
    void complexityScoreMatchesJsFormulaExactly() {
        double[] score = new double[1];
        // tableCount=3(*1=3) + writeTableCount=2(*2=4) + crossSchemaCalls=1(*3=3) + loc>200(+0, loc=100)
        // + callDepth>5(+0,depth=2) + totalCalls>20(+0,total=5) = 10 -> Medium
        String c = ProcedureReportBuilder.calcComplexity(3, 2, 1, 100, 2, 5, score);
        assertEquals(10.0, score[0]);
        assertEquals("Medium", c);

        // push totalCalls>20 true: +1 = 11 -> High
        c = ProcedureReportBuilder.calcComplexity(3, 2, 1, 100, 2, 25, score);
        assertEquals(11.0, score[0]);
        assertEquals("High", c);
    }

    /**
     * Calibrated by removing the {@code internalCalls}/{@code externalCalls} split (counting
     * everything as internal) — confirmed externalCalls dropped from 1 to 0 before restoring.
     */
    @Test
    void internalAndExternalCallsComeFromTheFlatCallsListNotTheTree() throws Exception {
        JsonNode d = detail("{"
                + "\"id\":\"P1\",\"name\":\"P1\",\"schemaName\":\"ERP\",\"startLine\":1,\"endLine\":5,"
                + "\"tables\":[],"
                + "\"callTree\":{\"id\":\"P1\",\"schemaName\":\"ERP\",\"callType\":\"INTERNAL\",\"children\":[]},"
                + "\"calls\":["
                + "  {\"name\":\"P2\",\"callType\":\"INTERNAL\"},"
                + "  {\"name\":\"UTL_HTTP.REQUEST\",\"callType\":\"EXTERNAL\"}"
                + "]"
                + "}");
        ObjectNode report = ProcedureReportBuilder.build(M, d, "ERP");
        assertEquals(1, report.get("internalCalls").asInt());
        assertEquals(1, report.get("externalCalls").asInt());
    }
}
