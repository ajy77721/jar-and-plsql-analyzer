package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.queue.AnalysisQueueService;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * MID-021 (§107) — {@code ParserAnalysisController.getJoins}/{@code getCursors} gain
 * {@code directProcId}/{@code nodeIds}, `plsql/js/joins.js`/`cursors.js`'s own "all"/"scoped"
 * scope pair (already ported for the live-crawl backend as
 * {@code com.plsqlanalyzer.web.controller.PlsqlScopeFilter}), reused here because both facets'
 * parser-backend row shape already matches {@code accessDetails[]} — confirmed directly against
 * {@code ParserFacetSpecs.JOINS}/{@code CURSORS} before writing this, not assumed.
 */
class ParserAnalysisControllerPlsqlScopeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String joinRow(String left, String right, String... procedureIds) {
        StringBuilder details = new StringBuilder("[");
        for (int i = 0; i < procedureIds.length; i++) {
            if (i > 0) details.append(',');
            details.append("{\"procedureId\":\"").append(procedureIds[i]).append("\",\"onPredicate\":\"x=y\"}");
        }
        details.append(']');
        return "{\"leftTable\":\"" + left + "\",\"rightTable\":\"" + right + "\",\"joinTypes\":[\"INNER\"],"
                + "\"accessDetails\":" + details + "}";
    }

    private static ParserAnalysisController build(AnalysisRepository repository) throws Exception {
        Constructor<?> ctor = ParserAnalysisController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = MAPPER;
            else if (AnalysisRepository.class.equals(type)) args[i] = repository;
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(MAPPER, 10000);
            else if (ParserScopeResolver.class.equals(type)) args[i] = new ParserScopeResolver(mock(AnalysisService.class));
            else if (JoinComplexityConfig.class.equals(type)) args[i] = new JoinComplexityConfig();
            else if (type.isPrimitive()) args[i] = (type == boolean.class) ? false : 0;
            else args[i] = mock(type);
        }
        return (ParserAnalysisController) ctor.newInstance(args);
    }

    @Test
    void directProcIdKeepsOnlyThatProceduresAccessDetails() throws Exception {
        ArrayNode joins = (ArrayNode) MAPPER.readTree("["
                + joinRow("ERP_ORDER", "ERP_CUSTOMER", "ERP$P1")
                + "," + joinRow("ERP_INVOICE", "ERP_PAYMENT", "ERP$P2")
                + "]");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(joins);

        ParserAnalysisController c = build(repo);
        // limit forces the envelope path — a real caller (PA.pageSource) always sends it, and
        // wantsEnvelope() would otherwise route to the legacy bare-array response instead.
        var response = c.getJoins("A1", 50, null, null, null, null, null, null, null,
                "ERP$P1", null, null, null);

        assertEquals(200, response.getStatusCode().value());
        JsonNode body = response.getBody();
        ArrayNode items = (ArrayNode) body.get("items");
        assertEquals(1, items.size(), "only the row touching ERP$P1 must survive");
        assertEquals("ERP_ORDER", items.get(0).get("leftTable").asText());
    }

    @Test
    void nodeIdsKeepsRowsMatchingAnyOfTheGivenIds() throws Exception {
        ArrayNode joins = (ArrayNode) MAPPER.readTree("["
                + joinRow("ERP_ORDER", "ERP_CUSTOMER", "ERP$P1")
                + "," + joinRow("ERP_INVOICE", "ERP_PAYMENT", "ERP$P2")
                + "," + joinRow("ERP_SHIPMENT", "ERP_CARRIER", "ERP$P3")
                + "]");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(joins);

        ParserAnalysisController c = build(repo);
        var response = c.getCursors("A1", 50, null, null, null, null, null, null, null,
                null, List.of("ERP$P2", "ERP$P3"), null, null);

        JsonNode body = response.getBody();
        ArrayNode items = (ArrayNode) body.get("items");
        assertEquals(2, items.size(), "the two rows touching ERP$P2/ERP$P3 must survive, the one touching only ERP$P1 must not");
    }

    /**
     * Calibrated by having {@code directProcId} fall through to the plain {@code scope}/{@code
     * procId} path (as if the new parameter were never read) — confirmed the test failed because
     * every row survived unfiltered, before restoring.
     */
    @Test
    void directProcIdActuallyNarrowsRatherThanReturningEverything() throws Exception {
        ArrayNode joins = (ArrayNode) MAPPER.readTree("["
                + joinRow("A", "B", "ERP$P1")
                + "," + joinRow("C", "D", "ERP$P2")
                + "," + joinRow("E", "F", "ERP$P3")
                + "]");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(joins);

        ParserAnalysisController c = build(repo);
        var response = c.getJoins("A1", 50, null, null, null, null, null, null, null,
                "ERP$P2", null, null, null);
        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(1, items.size());
        assertEquals("C", items.get(0).get("leftTable").asText());
    }

    /**
     * MID-021 (§110) — {@code TABLES}' row shape is {@code usedBy[]}, not {@code accessDetails[]},
     * and the endpoint's document is {@code {tables:[...], ...}}, not a bare array — so this exercises
     * {@code directTables}/{@code subtreeTables}, not {@code direct}/{@code subtree}, through
     * {@code getTablesPage} itself.
     */
    private static String tableRow(String name, String... nodeIds) {
        StringBuilder usedBy = new StringBuilder("[");
        for (int i = 0; i < nodeIds.length; i++) {
            if (i > 0) usedBy.append(',');
            usedBy.append("{\"nodeId\":\"").append(nodeIds[i]).append("\",\"nodeName\":\"N\",")
                    .append("\"operations\":{\"SELECT\":[1,2]}}");
        }
        usedBy.append(']');
        return "{\"name\":\"" + name + "\",\"schema\":\"ERP\",\"objectType\":\"TABLE\","
                + "\"allOperations\":[\"SELECT\"],\"usedBy\":" + usedBy + "}";
    }

    @Test
    void directProcIdOnTablesTrimsUsedByNotAccessDetails() throws Exception {
        JsonNode doc = MAPPER.readTree("{\"totalTables\":2,\"tables\":["
                + tableRow("ERP_ORDER", "ERP$P1") + "," + tableRow("ERP_INVOICE", "ERP$P2") + "]}");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(doc);

        ParserAnalysisController c = build(repo);
        var response = c.getTablesPage("A1", 50, null, null, null, null, null, null, null,
                "ERP$P1", null, null, null);

        JsonNode body = response.getBody();
        ArrayNode items = (ArrayNode) body.get("items");
        assertEquals(1, items.size(), "only the table used by ERP$P1 must survive");
        assertEquals("ERP_ORDER", items.get(0).get("name").asText());
    }

    /**
     * Calibrated by short-circuiting the {@code arrayField != null} branch (forcing the plain
     * {@code direct}/{@code subtree} path onto the wrapper object instead) — confirmed it threw a
     * {@code ClassCastException}/returned nothing usable rather than trimming, before restoring.
     */
    @Test
    void nodeIdsOnTablesKeepsOnlyMatchingTables() throws Exception {
        JsonNode doc = MAPPER.readTree("{\"totalTables\":3,\"tables\":["
                + tableRow("A", "ERP$P1") + "," + tableRow("B", "ERP$P2") + ","
                + tableRow("C", "ERP$P3") + "]}");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(doc);

        ParserAnalysisController c = build(repo);
        var response = c.getTablesPage("A1", 50, null, null, null, null, null, null, null,
                null, List.of("ERP$P2", "ERP$P3"), null, null);

        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(2, items.size());
    }
}
