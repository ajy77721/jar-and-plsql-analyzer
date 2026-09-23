package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.AnalysisRepository;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * UI-024 (§112) — {@code /procedures/export}, `parser/js/export.js`'s own bulk endpoint: deliberately
 * unpaged, the same "legitimate whole-corpus consumer" shape already established for {@code /tables}/
 * {@code /joins}/{@code /cursors}. Returns exactly what {@link AnalysisService#getProcedures} does —
 * the full, unprojected node array {@code /procedures}'s paged facet also pages, not a new read path.
 */
class ParserAnalysisControllerProceduresExportTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ParserAnalysisController build(AnalysisService service) throws Exception {
        Constructor<?> ctor = ParserAnalysisController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = MAPPER;
            else if (AnalysisService.class.equals(type)) args[i] = service;
            else if (AnalysisRepository.class.equals(type)) args[i] = mock(AnalysisRepository.class);
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(MAPPER, 10000);
            else if (ParserScopeResolver.class.equals(type)) args[i] = new ParserScopeResolver(mock(AnalysisService.class));
            else if (JoinComplexityConfig.class.equals(type)) args[i] = new JoinComplexityConfig();
            else if (type.isPrimitive()) args[i] = (type == boolean.class) ? false : 0;
            else args[i] = mock(type);
        }
        return (ParserAnalysisController) ctor.newInstance(args);
    }

    @Test
    void returnsTheFullUnprojectedNodeArray() throws Exception {
        ArrayNode nodes = (ArrayNode) MAPPER.readTree("["
                + "{\"nodeId\":\"ERP$P1\",\"name\":\"P1\",\"schema\":\"ERP\",\"packageName\":\"\","
                + "\"objectType\":\"PROCEDURE\",\"depth\":0,\"linesOfCode\":120,\"callCount\":3,"
                + "\"sourceFile\":\"p1.sql\",\"counts\":{\"tables\":2,\"callsOut\":1,\"cursors\":0}}"
                + "]");
        AnalysisService service = mock(AnalysisService.class);
        when(service.getProcedures("A1")).thenReturn(nodes);

        ParserAnalysisController c = build(service);
        var response = c.getProceduresForExport("A1");

        assertEquals(200, response.getStatusCode().value());
        JsonNode body = response.getBody();
        assertTrue(body.isArray(), "must return the bare array, not a paged envelope — export means every row");
        assertEquals(1, body.size());
        assertEquals(120, body.get(0).get("linesOfCode").asInt(),
                "must carry the full node shape (linesOfCode), not the grid's 8-of-21 projection");
        assertEquals(2, body.get(0).get("counts").get("tables").asInt());
    }

    @Test
    void aMissingAnalysisIs404NotAnException() throws Exception {
        AnalysisService service = mock(AnalysisService.class);
        when(service.getProcedures("missing")).thenThrow(new java.io.IOException("not found"));

        ParserAnalysisController c = build(service);
        var response = c.getProceduresForExport("missing");

        assertEquals(404, response.getStatusCode().value());
    }
}
