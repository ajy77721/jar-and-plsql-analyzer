package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * UI-030-BUG — {@link ParserProcedureReportBuilder}, built against the parser backend's real
 * `/node/{file}` shape (confirmed field-for-field against a real file on disk, quoted below), not
 * a port of the client logic that was silently broken against it.
 *
 * <p>The fixture is the exact JSON read off
 * {@code data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE_20260905_025907/api/nodes/
 * ANALYZER$ERP_PKG_PLANNER$WALK_UP.json} while investigating this bug — a real node, not an
 * invented one.
 */
class ParserProcedureReportBuilderTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static final String WALK_UP_FIXTURE = "{\"nodeId\": \"ANALYZER$ERP_PKG_PLANNER$WALK_UP\", "
            + "\"name\": \"ERP_PKG_PLANNER.WALK_UP\", \"schema\": \"ANALYZER\", \"objectType\": \"PROCEDURE\", "
            + "\"depth\": 4, \"lineStart\": 89, \"lineEnd\": 98, \"linesOfCode\": 10, "
            + "\"parameters\": [{\"name\": \"P_DEPTH\", \"dataType\": \"NUMBER\", \"direction\": \"IN\", \"line\": 89}, "
            + "{\"name\": \"P_LIMIT\", \"dataType\": \"NUMBER\", \"direction\": \"IN\", \"line\": 89}], "
            + "\"subtreeTables\": [{\"schema\": \"ANALYZER\", \"name\": \"ERP_AUDIT_LOG\", \"objectType\": \"TABLE\", "
            + "\"totalUsageCount\": 3, \"directUsageCount\": 0, "
            + "\"operations\": {\"INSERT\": [11], \"SELECT\": [14], \"DELETE\": [16]}, "
            + "\"usedByNodes\": [{\"nodeId\": \"ANALYZER$ERP_USP_WRITE_AUDIT_TRAIL\", "
            + "\"nodeName\": \"ERP_USP_WRITE_AUDIT_TRAIL\", \"depth\": 1, "
            + "\"operations\": {\"INSERT\": [11], \"SELECT\": [14], \"DELETE\": [16]}}]}], "
            + "\"calls\": [{\"target\": \"ERP_USP_WRITE_AUDIT_TRAIL\", \"packageName\": null, "
            + "\"objectName\": \"ERP_USP_WRITE_AUDIT_TRAIL\", \"type\": \"EXTERNAL\", \"schema\": \"ANALYZER\", "
            + "\"lines\": [92, 95], \"count\": 2}, "
            + "{\"target\": \"ERP_PKG_PLANNER.WALK_DOWN\", \"packageName\": \"ERP_PKG_PLANNER\", "
            + "\"objectName\": \"WALK_DOWN\", \"type\": \"INTERNAL\", \"schema\": \"ANALYZER\", "
            + "\"lines\": [97], \"count\": 1}], "
            + "\"statementSummary\": {\"IF\": 1, \"RETURN\": 1}, "
            + "\"sourceFile\": \"ANALYZER.ERP_PKG_PLANNER.WALK_UP.sql\"}";

    /**
     * Calibrated by reading {@code t.get("name")} instead of what's actually there, then reverting
     * — this is the exact bug: {@code _analyzeProcedure} does `t.tableName`, which is undefined on
     * this shape (`t.name` is correct here), causing `(t.tableName || t).toUpperCase()` to call
     * `.toUpperCase()` on the whole object and throw. Confirmed the fix reads the right field by
     * checking the table actually appears in `flowTables`.
     */
    @Test
    void readsTheRealFieldNamesNotTheOnesAnalyzeProcedureBrokenlyExpected() throws Exception {
        JsonNode node = M.readTree(WALK_UP_FIXTURE);
        ObjectNode report = ParserProcedureReportBuilder.build(M, node, "ANALYZER");
        assertEquals(1, report.get("tableCount").asInt());
        assertTrue(report.get("flowTables").has("ERP_AUDIT_LOG"), "must key flowTables from the real `name` field, not `tableName`");
        assertEquals("WALK_UP", report.get("name").asText());
        assertEquals("ERP_PKG_PLANNER", report.get("packageName").asText(), "packageName must split out of the dotted `name` field");
    }

    /**
     * Calibrated by swapping `directUsageCount > 0` to `>= 0` (matching everything) — confirmed
     * `directTables` picked up the table with zero direct uses before restoring.
     */
    @Test
    void directTablesOnlyIncludesTablesWithDirectUsage() throws Exception {
        JsonNode node = M.readTree(WALK_UP_FIXTURE);
        ObjectNode report = ParserProcedureReportBuilder.build(M, node, "ANALYZER");
        assertEquals(0, report.get("directTables").size(), "ERP_AUDIT_LOG has directUsageCount 0 — it's a subtree-only access");
        assertEquals(1, report.get("flowTables").size(), "but it's still a real flow table");
    }

    /**
     * Calibrated by removing the EXTERNAL-callType branch of the cross-schema check (leaving only
     * the schema-mismatch branch) — confirmed crossSchemaCalls dropped from 1 to 0 (the one EXTERNAL
     * call here has the *same* schema as the entry) before restoring.
     */
    @Test
    void crossSchemaCallsCountsExternalCallTypeEvenWithinTheSameSchema() throws Exception {
        JsonNode node = M.readTree(WALK_UP_FIXTURE);
        ObjectNode report = ParserProcedureReportBuilder.build(M, node, "ANALYZER");
        assertEquals(2, report.get("totalCalls").asInt());
        assertEquals(1, report.get("internalCalls").asInt());
        assertEquals(1, report.get("externalCalls").asInt());
        assertEquals(1, report.get("crossSchemaCalls").asInt(), "the EXTERNAL-typed call must count even though its schema matches the entry schema");
    }

    /** Calibrated by hardcoding callDepth to 0 — confirmed the assertion below failed before restoring. */
    @Test
    void callDepthIsTheMaxUsedByNodesDepthAcrossSubtreeTables() throws Exception {
        JsonNode node = M.readTree(WALK_UP_FIXTURE);
        ObjectNode report = ParserProcedureReportBuilder.build(M, node, "ANALYZER");
        assertEquals(1, report.get("callDepth").asInt());
    }

    @Test
    void writeAndReadTableClassificationMatchesTheOperationsMap() throws Exception {
        JsonNode node = M.readTree(WALK_UP_FIXTURE);
        ObjectNode report = ParserProcedureReportBuilder.build(M, node, "ANALYZER");
        // ERP_AUDIT_LOG has INSERT+DELETE (write) and SELECT (read) — both classifications apply.
        assertEquals(1, report.get("writeTableCount").asInt());
        assertEquals(1, report.get("readTableCount").asInt());
    }
}
