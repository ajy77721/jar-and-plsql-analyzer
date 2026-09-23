package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * UI-024 — {@link DetailHeaderCountsService}, collapsing `detailHeader.js`'s seven
 * `PA.nodeIndex.all()`-scanning functions (`_dhUniqueCallCount`/`_dhUniqueCallCountByType`/
 * `_dhUniqueTriggerCount`/`_dhUniqueCursorCount`/`_dhFullCallCountByType`/`_dhFullTriggerCount`/
 * `_dhFullCursorCount`) into one aggregation pass.
 */
class DetailHeaderCountsServiceTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static String indexJson(String nodesJson) {
        return "{\"entrySchema\":\"ERP\",\"nodes\":" + nodesJson + "}";
    }

    private static String node(String id, String objectType, int intCalls, int extCalls, int cursors) {
        return "{\"nodeId\":\"" + id + "\",\"schema\":\"ERP\",\"objectType\":\"" + objectType + "\","
                + "\"counts\":{\"internalCalls\":" + intCalls + ",\"externalCalls\":" + extCalls
                + ",\"cursors\":" + cursors + "}}";
    }

    @Test
    void fullScopeSumsAcrossEveryNode() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("ERP$P1", "PROCEDURE", 2, 1, 3)
                + "," + node("ERP$P2", "TRIGGER", 0, 1, 0) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        DetailHeaderCountsService svc = new DetailHeaderCountsService(service, new ParserScopeResolver(service), M);
        ResponseEntity<Object> response = svc.getCounts("A1", "full", null);

        assertEquals(200, response.getStatusCode().value());
        ObjectNode body = (ObjectNode) response.getBody();
        assertEquals(2, body.get("intCalls").asInt());
        assertEquals(2, body.get("extCalls").asInt());
        assertEquals(4, body.get("totalCalls").asInt());
        assertEquals(1, body.get("triggers").asInt(), "only P2 is a TRIGGER");
        assertEquals(3, body.get("cursors").asInt());
    }

    @Test
    void subtreeScopeExcludesTheRootItself() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("ERP$P1", "PROCEDURE", 5, 5, 5)
                + "," + node("ERP$P2", "PROCEDURE", 1, 0, 1) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));
        when(service.getCallTree("A1", "ERP$P1")).thenReturn(
                M.readTree("{\"id\":\"ERP$P1\",\"children\":[{\"id\":\"ERP$P2\",\"children\":[]}]}"));
        when(service.getProcedures("A1")).thenReturn(M.readTree("[]"));

        DetailHeaderCountsService svc = new DetailHeaderCountsService(service, new ParserScopeResolver(service), M);
        ResponseEntity<Object> response = svc.getCounts("A1", "subtree", "ERP$P1");

        ObjectNode body = (ObjectNode) response.getBody();
        assertEquals(1, body.get("intCalls").asInt(), "subtree excludes P1 itself, only counts P2");
    }

    /**
     * Calibrated by comparing against `subtree`'s own result for the identical fixture — confirmed
     * `subflow` (which includes the root) produces a strictly larger total than `subtree` (which
     * doesn't), by exactly the root's own contribution.
     */
    @Test
    void subflowScopeIncludesTheRootUnlikeSubtree() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("ERP$P1", "PROCEDURE", 5, 0, 0)
                + "," + node("ERP$P2", "PROCEDURE", 1, 0, 0) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));
        when(service.getCallTree("A1", "ERP$P1")).thenReturn(
                M.readTree("{\"id\":\"ERP$P1\",\"children\":[{\"id\":\"ERP$P2\",\"children\":[]}]}"));
        when(service.getProcedures("A1")).thenReturn(M.readTree("[]"));

        DetailHeaderCountsService svc = new DetailHeaderCountsService(service, new ParserScopeResolver(service), M);
        int subtreeCalls = ((ObjectNode) svc.getCounts("A1", "subtree", "ERP$P1").getBody()).get("intCalls").asInt();
        int subflowCalls = ((ObjectNode) svc.getCounts("A1", "subflow", "ERP$P1").getBody()).get("intCalls").asInt();

        assertEquals(1, subtreeCalls);
        assertEquals(6, subflowCalls, "subflow adds P1's own 5 internal calls on top of subtree's 1");
    }
}
