package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * UI-024 — {@link ComplexityScoringService}, the Java port of `complexity.js`'s
 * `_metricContribution`/`scoreRaw`/`_computeMetrics`. Weights/thresholds/algorithm are the caller's
 * own resolved config (server has no `localStorage` to read), passed as plain parameters.
 */
class ComplexityScoringServiceTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static ComplexityScoringService.Weights defaultWeights() {
        return new ComplexityScoringService.Weights();
    }

    private static ComplexityScoringService.Thresholds defaultThresholds() {
        return new ComplexityScoringService.Thresholds();
    }

    private static String indexJson(String nodesJson) {
        return "{\"nodes\":" + nodesJson + "}";
    }

    private static String node(String id, int loc, int tables, int callsOut, int cursors, int depth) {
        return "{\"nodeId\":\"" + id + "\",\"name\":\"" + id + "\",\"schema\":\"ERP\",\"objectType\":\"PROCEDURE\","
                + "\"linesOfCode\":" + loc + ",\"depth\":" + depth + ","
                + "\"counts\":{\"tables\":" + tables + ",\"callsOut\":" + callsOut + ",\"cursors\":" + cursors
                + ",\"exceptionHandlers\":0,\"parameters\":0,\"variables\":0,\"sequences\":0,\"statements\":{}}}";
    }

    /** Linear formula: score = round(sum(weight * value)) — the default algorithm's simplest case. */
    @Test
    void linearScoreMatchesTheExactFormula() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson("[" + node("P1", 100, 2, 3, 1, 0) + "]")));

        ComplexityScoringService svc = new ComplexityScoringService(service, M);
        ComplexityScoringService.Weights w = defaultWeights(); // loc=0.3, tables=15, callsOut=10, cursors=8, dynamicSql=20, depth=5
        ResponseEntity<Object> response = svc.getScores("A1", w, defaultThresholds(), "linear", 1.3);

        ArrayNode body = (ArrayNode) response.getBody();
        // 0.3*100 + 15*2 + 10*3 + 8*1 + 20*0 + 5*0 = 30+30+30+8 = 98
        assertEquals(98, body.get(0).get("score").asInt());
        assertEquals("MEDIUM", body.get(0).get("risk").asText(), "98 is >=50 (medium) and <150 (high)");
    }

    /**
     * Calibrated by changing the logarithmic case to use natural log instead of log base 2 —
     * confirmed the computed score changed (a different formula) before restoring.
     */
    @Test
    void logarithmicUsesLogBaseTwoNotNaturalLog() {
        // metricContribution(value=3, weight=10, "logarithmic") = 10 * log2(4) = 10 * 2 = 20
        double result = ComplexityScoringService.metricContribution(3, 10, "logarithmic", 1.3);
        assertEquals(20.0, result, 0.0001);
    }

    @Test
    void exponentialAndBalancedFormulasMatch() {
        // exponential: w * ((1+v)^p - 1); with v=1, p=2, w=10 => 10 * (2^2 - 1) = 30
        assertEquals(30.0, ComplexityScoringService.metricContribution(1, 10, "exponential", 2), 0.0001);
        // balanced: w * sqrt(v); v=9, w=2 => 6
        assertEquals(6.0, ComplexityScoringService.metricContribution(9, 2, "balanced", 1.3), 0.0001);
    }

    @Test
    void riskThresholdsAreInclusiveAtTheBoundary() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        // loc=500 alone with weight 0.3 => score 150, exactly at the "high" boundary.
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson("[" + node("P1", 500, 0, 0, 0, 0) + "]")));

        ComplexityScoringService svc = new ComplexityScoringService(service, M);
        ResponseEntity<Object> response = svc.getScores("A1", defaultWeights(), defaultThresholds(), "linear", 1.3);
        ObjectNode row = (ObjectNode) ((ArrayNode) response.getBody()).get(0);
        assertEquals(150, row.get("score").asInt());
        assertEquals("HIGH", row.get("risk").asText(), "score == high threshold must count as HIGH, not MEDIUM");
    }

    @Test
    void readableDefaultsTrueUnlessExplicitlyFalse() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        String nodes = "[{\"nodeId\":\"P1\",\"counts\":{},\"readable\":false},"
                + "{\"nodeId\":\"P2\",\"counts\":{}}]"; // no readable field at all
        when(service.getIndex("A1")).thenReturn(M.readTree(indexJson(nodes)));

        ComplexityScoringService svc = new ComplexityScoringService(service, M);
        ArrayNode body = (ArrayNode) svc.getScores("A1", defaultWeights(), defaultThresholds(), "linear", 1.3).getBody();
        assertFalse(body.get(0).get("readable").asBoolean());
        assertTrue(body.get(1).get("readable").asBoolean(), "missing readable field must default to true");
    }
}
