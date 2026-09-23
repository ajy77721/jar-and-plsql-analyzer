package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.repo.AnalysisQuery;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.parser.config.JoinComplexityConfig;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import com.plsqlanalyzer.web.parser.service.ParserScopeResolver;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * §260 — regression coverage for the {@code procId}-alone correctness trap: {@code GET
 * .../cursors?procId=X} and {@code GET .../dynamic-sql?procId=X}, with no {@code scope} companion
 * and no {@code directProcId}, used to silently ignore {@code procId} entirely and return every
 * row in the whole analysis (a 200 OK with wrong data, not an error). Confirmed independently on
 * two live dialects (MySQL {@code ERP_USP_RUN_CYCLE}, SQL Server {@code USP_RUNMRPCYCLE}) before
 * this fix.
 *
 * <p>Fixed in {@link ParserScopeResolver#predicate} — {@code procId} alone (blank/absent
 * {@code scope}) now defaults to {@code direct}, matching the same "this node only" contract
 * {@code directProcId} already gives the caller one parameter over.
 */
class ParserAnalysisControllerProcIdScopeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String joinRow(String left, String right, String procedureId) {
        return "{\"leftTable\":\"" + left + "\",\"rightTable\":\"" + right + "\",\"joinTypes\":[\"INNER\"],"
                + "\"accessDetails\":[{\"procedureId\":\"" + procedureId + "\",\"onPredicate\":\"x=y\"}]}";
    }

    /** A single-node call tree — {@code procId} is a leaf procedure with nothing beneath it. */
    private static JsonNode singleNodeTree(String procId) throws Exception {
        return MAPPER.readTree("{\"id\":\"" + procId + "\",\"children\":[]}");
    }

    private static ParserAnalysisController build(AnalysisRepository repository,
                                                   AnalysisService analysisService) throws Exception {
        Constructor<?> ctor = ParserAnalysisController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = MAPPER;
            else if (AnalysisRepository.class.equals(type)) args[i] = repository;
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(MAPPER, 10000);
            else if (ParserScopeResolver.class.equals(type)) args[i] = new ParserScopeResolver(analysisService);
            else if (JoinComplexityConfig.class.equals(type)) args[i] = new JoinComplexityConfig();
            else if (type.isPrimitive()) args[i] = (type == boolean.class) ? false : 0;
            else args[i] = mock(type);
        }
        return (ParserAnalysisController) ctor.newInstance(args);
    }

    @Test
    void cursorsProcIdAloneScopesToThatProcedureOnly() throws Exception {
        ArrayNode rows = (ArrayNode) MAPPER.readTree("["
                + joinRow("ERP_ORDER", "ERP_CUSTOMER", "ERP$P1")
                + "," + joinRow("ERP_INVOICE", "ERP_PAYMENT", "ERP$P2")
                + "]");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(rows);

        AnalysisService service = mock(AnalysisService.class);
        when(service.getCallTree("A1", "ERP$P1")).thenReturn(singleNodeTree("ERP$P1"));

        ParserAnalysisController c = build(repo, service);
        // scope=null, directProcId=null, nodeIds=null — the naive, natural-looking call a caller
        // would make without knowing the undiscoverable directProcId/scope=direct workarounds.
        var response = c.getCursors("A1", 50, null, null, null, null, null,
                null, "ERP$P1", null, null, null, null);

        assertEquals(200, response.getStatusCode().value());
        JsonNode body = response.getBody();
        ArrayNode items = (ArrayNode) body.get("items");
        assertEquals(1, items.size(), "procId alone must scope to only that procedure's rows");
        assertEquals("ERP_ORDER", items.get(0).get("leftTable").asText());
    }

    /** Dynamic SQL rows are flat — {@code procedureId} lives on the row, not in an accessDetails[]. */
    private static String dynamicSqlRow(String procId, String type, String sqlExpression) {
        return "{\"procedureId\":\"" + procId + "\",\"procedureName\":\"" + procId + "\","
                + "\"type\":\"" + type + "\",\"lineNumber\":1,\"sqlExpression\":\"" + sqlExpression + "\"}";
    }

    @Test
    void dynamicSqlProcIdAloneScopesToThatProcedureOnly() throws Exception {
        ArrayNode rows = (ArrayNode) MAPPER.readTree("["
                + dynamicSqlRow("ERP$P1", "EXECUTE_IMMEDIATE", "SELECT A")
                + "," + dynamicSqlRow("ERP$P2", "EXECUTE_IMMEDIATE", "SELECT C")
                + "," + dynamicSqlRow("ERP$P3", "EXECUTE_IMMEDIATE", "SELECT E")
                + "]");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(rows);

        AnalysisService service = mock(AnalysisService.class);
        when(service.getCallTree("A1", "ERP$P2")).thenReturn(singleNodeTree("ERP$P2"));

        ParserAnalysisController c = build(repo, service);
        var response = c.getDynamicSql("A1", 50, null, null, null, null, null,
                null, "ERP$P2", null, null);

        JsonNode body = response.getBody();
        ArrayNode items = (ArrayNode) body.get("items");
        assertEquals(1, items.size(), "procId alone must scope to only that procedure's rows");
        assertEquals("ERP$P2", items.get(0).get("procedureId").asText());
    }

    @Test
    void joinsProcIdAloneScopesToThatProcedureOnlyTooSameContractAcrossFacets() throws Exception {
        ArrayNode rows = (ArrayNode) MAPPER.readTree("["
                + joinRow("ERP_ORDER", "ERP_CUSTOMER", "ERP$P1")
                + "," + joinRow("ERP_INVOICE", "ERP_PAYMENT", "ERP$P2")
                + "]");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(rows);

        AnalysisService service = mock(AnalysisService.class);
        when(service.getCallTree("A1", "ERP$P2")).thenReturn(singleNodeTree("ERP$P2"));

        ParserAnalysisController c = build(repo, service);
        var response = c.getJoins("A1", 50, null, null, null, null, null,
                null, "ERP$P2", null, null, null, null);

        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(1, items.size());
        assertEquals("ERP_INVOICE", items.get(0).get("leftTable").asText());
    }

    /**
     * Guards against a fix that only patches the entrypoint for {@code procId} without truly
     * scoping — i.e. this would still pass a broken "return everything anyway" fix unless it
     * actually asserts the row count shrank, which it does above; this test additionally confirms
     * a nonsense/unrelated {@code procId} yields zero rows rather than falling back to "everyone".
     */
    @Test
    void cursorsProcIdAloneWithNoMatchingRowsReturnsEmptyNotEverything() throws Exception {
        ArrayNode rows = (ArrayNode) MAPPER.readTree("["
                + joinRow("ERP_ORDER", "ERP_CUSTOMER", "ERP$P1")
                + "," + joinRow("ERP_INVOICE", "ERP_PAYMENT", "ERP$P2")
                + "]");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(rows);

        AnalysisService service = mock(AnalysisService.class);
        when(service.getCallTree("A1", "ERP$NOBODY")).thenReturn(singleNodeTree("ERP$NOBODY"));

        ParserAnalysisController c = build(repo, service);
        var response = c.getCursors("A1", 50, null, null, null, null, null,
                null, "ERP$NOBODY", null, null, null, null);

        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(0, items.size());
    }
}
