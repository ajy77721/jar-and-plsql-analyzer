package com.plsqlanalyzer.web.controller.detail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * UI-030c — {@link ProcedureReportsService}, which replaces the client's per-procedure
 * {@code getProcDetail} network loop with one server-side pass over {@code graph.getAllNodes()}.
 *
 * <p>{@link ProcedureDetailService} and {@link AnalysisService} are mocked here, deliberately — the
 * point of this test is the orchestration (iterate every node, skip ones with no body, assemble the
 * 4 cross-reports from the collected procReports), not re-proving {@code ProcedureDetailService}'s
 * own per-node computation, which is out of scope for this class and already exercised elsewhere.
 */
class ProcedureReportsServiceTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static Map<String, Object> detailFor(String name, String schema, String op) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("id", name);
        d.put("name", name);
        d.put("schemaName", schema);
        d.put("packageName", "");
        d.put("startLine", 1);
        d.put("endLine", 10);
        Map<String, Object> table = new LinkedHashMap<>();
        table.put("tableName", "T_" + name);
        table.put("schemaName", schema);
        table.put("operations", java.util.List.of(op));
        d.put("tables", java.util.List.of(table));
        d.put("callTree", Map.of("id", name, "schemaName", schema, "callType", "INTERNAL", "children", java.util.List.of()));
        d.put("calls", java.util.List.of());
        return d;
    }

    /**
     * Calibrated by changing the null-body skip to instead throw (removing the `continue`) —
     * confirmed the whole request failed instead of just omitting the one broken node, before
     * restoring.
     */
    @Test
    void assemblesProcReportsAndAllFourCrossReportsFromEveryGraphNode() {
        CallGraph graph = new CallGraph();
        graph.addNode(new CallGraphNode("ERP", "", "P1"));
        graph.addNode(new CallGraphNode("ERP", "", "P2"));

        AnalysisResult result = new AnalysisResult();
        result.setCallGraph(graph);
        result.setEntrySchema("ERP");

        AnalysisService analysisService = mock(AnalysisService.class);
        when(analysisService.getLatestResult()).thenReturn(result);

        ProcedureDetailService detailService = mock(ProcedureDetailService.class);
        when(detailService.getProcedureDetail("ERP.P1"))
                .thenReturn(ResponseEntity.ok(detailFor("P1", "ERP", "SELECT")));
        when(detailService.getProcedureDetail("ERP.P2"))
                .thenReturn(ResponseEntity.ok(detailFor("P2", "ERP", "UPDATE")));

        ProcedureReportsService service = new ProcedureReportsService(analysisService, detailService, M);
        ResponseEntity<Object> response = service.getProcedureReports();

        assertEquals(200, response.getStatusCode().value());
        ObjectNode body = (ObjectNode) response.getBody();
        assertNotNull(body);
        assertEquals(2, body.get("procReports").size(), "one report per graph node");
        assertTrue(body.has("txnReport"));
        assertTrue(body.has("schemaSliceReport"));
        assertTrue(body.has("extReport"));
        assertTrue(body.has("batchReport"));
        assertEquals(2, body.get("txnReport").size(), "one transaction row per procedure, not grouped");
        // P2 writes (UPDATE) -> SINGLE TABLE; P1 only reads -> READ ONLY
        boolean sawSingleTable = false, sawReadOnly = false;
        for (var row : body.get("txnReport")) {
            String req = row.get("transactionRequirement").asText();
            if (req.equals("SINGLE TABLE")) sawSingleTable = true;
            if (req.equals("READ ONLY")) sawReadOnly = true;
        }
        assertTrue(sawSingleTable && sawReadOnly);
    }

    @Test
    void returnsNotFoundWhenNoAnalysisIsLoaded() {
        AnalysisService analysisService = mock(AnalysisService.class);
        when(analysisService.getLatestResult()).thenReturn(null);
        ProcedureDetailService detailService = mock(ProcedureDetailService.class);

        ProcedureReportsService service = new ProcedureReportsService(analysisService, detailService, M);
        ResponseEntity<Object> response = service.getProcedureReports();
        assertEquals(404, response.getStatusCode().value());
    }

    /**
     * Calibrated by making the mock return a 404 (null body) for one node and confirming the
     * service tolerates it (skips that node) rather than throwing — the exact resilience the old
     * client loop's own try/catch relied on (§81/§82), now ported server-side.
     */
    @Test
    void skipsNodesWhoseDetailLookupReturnsNoBody() {
        CallGraph graph = new CallGraph();
        graph.addNode(new CallGraphNode("ERP", "", "P1"));
        graph.addNode(new CallGraphNode("ERP", "", "MISSING"));

        AnalysisResult result = new AnalysisResult();
        result.setCallGraph(graph);
        result.setEntrySchema("ERP");

        AnalysisService analysisService = mock(AnalysisService.class);
        when(analysisService.getLatestResult()).thenReturn(result);

        ProcedureDetailService detailService = mock(ProcedureDetailService.class);
        when(detailService.getProcedureDetail("ERP.P1"))
                .thenReturn(ResponseEntity.ok(detailFor("P1", "ERP", "SELECT")));
        when(detailService.getProcedureDetail("ERP.MISSING"))
                .thenReturn(ResponseEntity.notFound().build());

        ProcedureReportsService service = new ProcedureReportsService(analysisService, detailService, M);
        ResponseEntity<Object> response = service.getProcedureReports();
        ObjectNode body = (ObjectNode) response.getBody();
        assertEquals(1, body.get("procReports").size(), "the missing node must be skipped, not fail the whole request");
    }
}
