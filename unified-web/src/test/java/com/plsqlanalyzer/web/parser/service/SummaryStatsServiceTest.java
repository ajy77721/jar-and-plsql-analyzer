package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * UI-024 — {@link SummaryStatsService}, the Java port of `summary.js`'s `_computeStats`. Unlike
 * `complexity.js` (§98), this genuinely collapses to a small aggregate — no row-per-procedure data
 * ever reaches the client.
 */
class SummaryStatsServiceTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static String indexJson(String nodesJson) {
        // The root's own totalTables is the DISTINCT count /analyses and /tables report; it is
        // deliberately different from the sum of the per-node counts below so the two fields can
        // be told apart.
        return "{\"totalTables\":2,\"nodes\":" + nodesJson + "}";
    }

    private static String node(String id, String schema, String objectType, int loc, int depth,
                               int tables, int intCalls, int extCalls, boolean readable) {
        return "{\"nodeId\":\"" + id + "\",\"name\":\"" + id + "\",\"schema\":\"" + schema + "\","
                + "\"objectType\":\"" + objectType + "\",\"linesOfCode\":" + loc + ",\"depth\":" + depth
                + ",\"readable\":" + readable + ",\"counts\":{\"tables\":" + tables
                + ",\"internalCalls\":" + intCalls + ",\"externalCalls\":" + extCalls
                + ",\"cursors\":1,\"parameters\":1,\"variables\":1,\"exceptionHandlers\":0,"
                + "\"statements\":{\"SELECT\":2}}}";
    }

    @Test
    void aggregatesTotalsAcrossEveryNodeWhenScopeIsFull() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("P1", "ERP", "PROCEDURE", 100, 2, 3, 1, 1, true)
                + "," + node("P2", "ERP", "FUNCTION", 50, 1, 1, 0, 1, true) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        SummaryStatsService svc = new SummaryStatsService(service, new ParserScopeResolver(service), M);
        ResponseEntity<Object> response = svc.getStats("A1", "full", null, "ERP");

        ObjectNode body = (ObjectNode) response.getBody();
        assertEquals(2, body.get("totalNodes").asInt());
        assertEquals(150, body.get("totalLoc").asInt());
        // totalTables is the DISTINCT count (the index root's own scalar), NOT the per-node sum;
        // the sum is totalTableReferences. 3 + 1 references vs 2 distinct tables.
        assertEquals(2, body.get("totalTables").asInt());
        assertEquals(4, body.get("totalTableReferences").asInt());
        assertEquals(3, body.get("totalCalls").asInt());
        assertEquals(100, body.get("maxLoc").get("value").asInt());
        assertEquals("P1", body.get("maxLoc").get("name").asText());
        assertEquals(2, body.get("types").get("PROCEDURE").asInt() + body.get("types").get("FUNCTION").asInt());
    }

    /**
     * Calibrated by removing the entrySchema comparison (always classifying as home schema) —
     * confirmed the BILLING node stopped being marked "External" before restoring.
     */
    @Test
    void classifiesOtherSchemasAsExternalRelativeToEntrySchema() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("P1", "ERP", "PROCEDURE", 10, 0, 0, 0, 0, true)
                + "," + node("P2", "BILLING", "PROCEDURE", 10, 0, 0, 0, 0, true) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        SummaryStatsService svc = new SummaryStatsService(service, new ParserScopeResolver(service), M);
        ObjectNode body = (ObjectNode) svc.getStats("A1", "full", null, "ERP").getBody();
        assertTrue(body.get("schemas").has("ERP"));
        assertTrue(body.get("schemas").has("External (BILLING)"), "a schema other than the entry schema must be classified External");
    }

    @Test
    void encryptedCountOnlyCountsExplicitlyUnreadableNodes() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("P1", "ERP", "PROCEDURE", 10, 0, 0, 0, 0, false)
                + "," + node("P2", "ERP", "PROCEDURE", 10, 0, 0, 0, 0, true) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        SummaryStatsService svc = new SummaryStatsService(service, new ParserScopeResolver(service), M);
        ObjectNode body = (ObjectNode) svc.getStats("A1", "full", null, "ERP").getBody();
        assertEquals(1, body.get("encryptedCount").asInt());
    }

    @Test
    void directScopeOnlyCountsTheOneSelectedNode() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("ERP$P1", "ERP", "PROCEDURE", 100, 0, 0, 0, 0, true)
                + "," + node("ERP$P2", "ERP", "PROCEDURE", 50, 0, 0, 0, 0, true) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));
        when(service.getCallTree("A1", "ERP$P1")).thenReturn(M.readTree("{\"id\":\"ERP$P1\",\"children\":[]}"));
        when(service.getProcedures("A1")).thenReturn(M.readTree("[]"));

        SummaryStatsService svc = new SummaryStatsService(service, new ParserScopeResolver(service), M);
        ObjectNode body = (ObjectNode) svc.getStats("A1", "direct", "ERP$P1", "ERP").getBody();
        assertEquals(1, body.get("totalNodes").asInt());
        assertEquals(100, body.get("totalLoc").asInt());
    }
}
