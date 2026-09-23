package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * UI-024 — {@link StatementsSummaryService}, the server-side aggregation replacing
 * `statements.js`'s two `PA.nodeIndex.all()` scans (`_aggregate`/`_aggregateScoped`).
 *
 * <p>{@link ParserScopeResolver} is the real object here, not mocked — it's the component whose
 * own scope semantics this row exists to reuse, not reimplement, so a fake would defeat the point
 * of the test. {@link AnalysisService} is mocked to supply a small, real-shaped index.
 */
class StatementsSummaryServiceTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static String indexJson(String nodesJson) {
        return "{\"entrySchema\":\"ERP\",\"nodes\":" + nodesJson + "}";
    }

    private static String node(String id, String name, java.util.Map<String, Integer> stmts) {
        StringBuilder sb = new StringBuilder("{\"nodeId\":\"" + id + "\",\"name\":\"" + name + "\",\"schema\":\"ERP\",");
        sb.append("\"sourceFile\":\"").append(id).append(".sql\",\"counts\":{\"statements\":{");
        boolean first = true;
        for (var e : stmts.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(e.getKey()).append("\":").append(e.getValue());
            first = false;
        }
        sb.append("}}}");
        return sb.toString();
    }

    @Test
    void aggregatesStatementTypesAcrossEveryNodeWhenScopeIsFull() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("ERP$P1", "P1", java.util.Map.of("SELECT", 3, "UPDATE", 1))
                + "," + node("ERP$P2", "P2", java.util.Map.of("SELECT", 2)) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        StatementsSummaryService svc = new StatementsSummaryService(service, new ParserScopeResolver(service), M);
        ResponseEntity<Object> response = svc.getSummary("A1", "full", null);

        assertEquals(200, response.getStatusCode().value());
        ArrayNode body = (ArrayNode) response.getBody();
        assertNotNull(body);
        ObjectNode select = findType(body, "SELECT");
        assertEquals(5, select.get("totalCount").asInt(), "3 from P1 + 2 from P2");
        assertEquals(2, select.get("procCount").asInt());
        ObjectNode update = findType(body, "UPDATE");
        assertEquals(1, update.get("totalCount").asInt());
        assertEquals(1, update.get("procCount").asInt());
    }

    @Test
    void directScopeOnlyCountsTheOneSelectedNode() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("ERP$P1", "P1", java.util.Map.of("SELECT", 3))
                + "," + node("ERP$P2", "P2", java.util.Map.of("SELECT", 2)) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));
        when(service.getCallTree("A1", "ERP$P1")).thenReturn(M.readTree("{\"id\":\"ERP$P1\",\"children\":[]}"));
        when(service.getProcedures("A1")).thenReturn(M.readTree("[]"));

        StatementsSummaryService svc = new StatementsSummaryService(service, new ParserScopeResolver(service), M);
        ResponseEntity<Object> response = svc.getSummary("A1", "direct", "ERP$P1");

        ArrayNode body = (ArrayNode) response.getBody();
        ObjectNode select = findType(body, "SELECT");
        assertEquals(3, select.get("totalCount").asInt(), "only P1's own SELECT count, not P2's");
        assertEquals(1, select.get("procCount").asInt());
    }

    /**
     * Calibrated by removing the `count <= 0` skip — confirmed a zero-count statement type entered
     * the map with an empty `procs` entry contributing nothing but noise, before restoring.
     */
    @Test
    void zeroCountStatementTypesAreSkipped() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("ERP$P1", "P1", java.util.Map.of("SELECT", 0, "UPDATE", 1)) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        StatementsSummaryService svc = new StatementsSummaryService(service, new ParserScopeResolver(service), M);
        ResponseEntity<Object> response = svc.getSummary("A1", "full", null);
        ArrayNode body = (ArrayNode) response.getBody();
        assertNull(findTypeOrNull(body, "SELECT"), "a zero count must not produce a SELECT entry");
        assertNotNull(findTypeOrNull(body, "UPDATE"));
    }

    @Test
    void procsWithinEachTypeAreSortedByCountDescending() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[" + node("ERP$LOW", "LOW", java.util.Map.of("SELECT", 1))
                + "," + node("ERP$HIGH", "HIGH", java.util.Map.of("SELECT", 9)) + "]";
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        StatementsSummaryService svc = new StatementsSummaryService(service, new ParserScopeResolver(service), M);
        ResponseEntity<Object> response = svc.getSummary("A1", "full", null);
        ArrayNode body = (ArrayNode) response.getBody();
        ArrayNode procs = (ArrayNode) findType(body, "SELECT").get("procs");
        assertEquals("HIGH", procs.get(0).get("name").asText());
        assertEquals("LOW", procs.get(1).get("name").asText());
    }

    private static ObjectNode findType(ArrayNode body, String type) {
        ObjectNode found = findTypeOrNull(body, type);
        assertNotNull(found, "expected a " + type + " entry");
        return found;
    }

    private static ObjectNode findTypeOrNull(ArrayNode body, String type) {
        for (var n : body) {
            if (type.equals(n.get("type").asText())) return (ObjectNode) n;
        }
        return null;
    }
}
