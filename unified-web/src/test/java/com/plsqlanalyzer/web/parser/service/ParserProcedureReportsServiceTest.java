package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * UI-030-BUG's fix, wired — {@link ParserProcedureReportsService}'s orchestration (enumerate every
 * node in the index, look up each one's detail, skip failures, assemble the cross-reports).
 * {@link AnalysisService} is mocked; the per-node report shape itself is
 * {@link ParserProcedureReportBuilder}'s own concern, already covered by its own tests.
 */
class ParserProcedureReportsServiceTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static String nodeJson(String id, String name, String schema) {
        return "{\"nodeId\":\"" + id + "\",\"name\":\"" + name + "\",\"schema\":\"" + schema + "\","
                + "\"objectType\":\"PROCEDURE\",\"lineStart\":1,\"lineEnd\":5,\"linesOfCode\":5,"
                + "\"subtreeTables\":[],\"calls\":[]}";
    }

    @Test
    void assemblesOneReportPerIndexNodeAndAllFourCrossReports() throws Exception {
        String index = "{\"entrySchema\":\"ERP\",\"nodes\":["
                + "{\"nodeId\":\"ERP$P1\"},{\"nodeId\":\"ERP$P2\"}]}";
        AnalysisService service = mock(AnalysisService.class);
        when(service.getIndex("A1")).thenReturn(M.readTree(index));
        when(service.getNodeDetail("A1", "ERP$P1.json")).thenReturn(M.readTree(nodeJson("ERP$P1", "P1", "ERP")));
        when(service.getNodeDetail("A1", "ERP$P2.json")).thenReturn(M.readTree(nodeJson("ERP$P2", "P2", "ERP")));

        ParserProcedureReportsService reportsService = new ParserProcedureReportsService(service, M);
        ResponseEntity<Object> response = reportsService.getProcedureReports("A1");

        assertEquals(200, response.getStatusCode().value());
        ObjectNode body = (ObjectNode) response.getBody();
        assertNotNull(body);
        assertEquals(2, body.get("procReports").size());
        assertTrue(body.has("txnReport"));
        assertTrue(body.has("schemaSliceReport"));
        assertTrue(body.has("extReport"));
        assertTrue(body.has("batchReport"));
    }

    @Test
    void returnsNotFoundWhenTheIndexHasNoNodesField() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        when(service.getIndex("A1")).thenReturn(M.readTree("{\"entrySchema\":\"ERP\"}"));
        ParserProcedureReportsService reportsService = new ParserProcedureReportsService(service, M);
        ResponseEntity<Object> response = reportsService.getProcedureReports("A1");
        assertEquals(404, response.getStatusCode().value());
    }

    /**
     * Calibrated by having `getNodeDetail` throw for one node and confirming the service tolerates
     * it (skips that node) rather than failing the whole request — the same resilience
     * {@link com.plsqlanalyzer.web.controller.detail.ProcedureReportsServiceTest} locks in for the
     * live-crawl side.
     */
    @Test
    void skipsNodesWhoseDetailLookupThrows() throws Exception {
        String index = "{\"entrySchema\":\"ERP\",\"nodes\":["
                + "{\"nodeId\":\"ERP$P1\"},{\"nodeId\":\"ERP$BROKEN\"}]}";
        AnalysisService service = mock(AnalysisService.class);
        when(service.getIndex("A1")).thenReturn(M.readTree(index));
        when(service.getNodeDetail("A1", "ERP$P1.json")).thenReturn(M.readTree(nodeJson("ERP$P1", "P1", "ERP")));
        when(service.getNodeDetail("A1", "ERP$BROKEN.json")).thenThrow(new RuntimeException("node file not found"));

        ParserProcedureReportsService reportsService = new ParserProcedureReportsService(service, M);
        ResponseEntity<Object> response = reportsService.getProcedureReports("A1");
        ObjectNode body = (ObjectNode) response.getBody();
        assertEquals(1, body.get("procReports").size(), "the broken node must be skipped, not fail the whole request");
    }
}
