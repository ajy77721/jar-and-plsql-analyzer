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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * MID-021 (predicates.js's own paged-conversion project) — {@code ParserAnalysisController
 * .getPredicates}, the last of the four screens §106's own audit named. Not a shape reuse the way
 * `joins`/`cursors`/`tables` were (§107-110): the parser's raw {@code usedBy[]} tables document is
 * adapted ({@code ParserPredicatesAdapter}) into the live-crawl backend's own
 * {@code accessDetails[].whereFilters[]} shape first, then {@code PlsqlPredicatesFacet.build}
 * aggregates it exactly as it already does for that backend — confirmed by reading both classes
 * directly, not assumed compatible.
 */
class ParserAnalysisControllerPredicatesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    /** One table, one procedure, two access lines — only one carries a predicate. */
    private static String rawTable(String name, String schema) {
        return "{\"name\":\"" + name + "\",\"schema\":\"" + schema + "\",\"objectType\":\"TABLE\","
                + "\"allOperations\":[\"SELECT\"],"
                + "\"usedBy\":[{\"nodeId\":\"ERP$P1\",\"nodeName\":\"P1\","
                + "\"operations\":{\"SELECT\":[10,20]},"
                + "\"predicates\":[{\"columnName\":\"REGION_ID\",\"operator\":\"=\",\"value\":\"p_id\",\"lineNumber\":10}]}]}";
    }

    @Test
    void returnsAggregatedPredicateRowsFromTheRawUsedByShape() throws Exception {
        JsonNode doc = MAPPER.readTree("{\"totalTables\":1,\"tables\":[" + rawTable("REF_REGION", "ANALYZER_REF") + "]}");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(doc);

        ParserAnalysisController c = build(repo);
        var response = c.getPredicates("A1", 50, null, null, null, null, null, null, null, null, null);

        assertEquals(200, response.getStatusCode().value());
        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(1, items.size(), "only the table with a predicate on it should appear");
        JsonNode row = items.get(0);
        assertEquals("REF_REGION", row.get("tableName").asText());
        assertEquals("ANALYZER_REF", row.get("schemaName").asText());
        assertEquals(1, row.get("predicateCount").asInt(), "line 20 has no predicate, must not count it");
        assertEquals(1, row.get("uniqueColumns").asInt());
    }

    @Test
    void directProcIdNarrowsToOnlyThatProceduresPredicates() throws Exception {
        String tableA = "{\"name\":\"A\",\"schema\":\"S\",\"objectType\":\"TABLE\",\"allOperations\":[\"SELECT\"],"
                + "\"usedBy\":[{\"nodeId\":\"ERP$P1\",\"nodeName\":\"P1\",\"operations\":{\"SELECT\":[1]},"
                + "\"predicates\":[{\"columnName\":\"X\",\"operator\":\"=\",\"value\":\"1\",\"lineNumber\":1}]}]}";
        String tableB = "{\"name\":\"B\",\"schema\":\"S\",\"objectType\":\"TABLE\",\"allOperations\":[\"SELECT\"],"
                + "\"usedBy\":[{\"nodeId\":\"ERP$P2\",\"nodeName\":\"P2\",\"operations\":{\"SELECT\":[1]},"
                + "\"predicates\":[{\"columnName\":\"Y\",\"operator\":\"=\",\"value\":\"2\",\"lineNumber\":1}]}]}";
        JsonNode doc = MAPPER.readTree("{\"totalTables\":2,\"tables\":[" + tableA + "," + tableB + "]}");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(doc);

        ParserAnalysisController c = build(repo);
        var response = c.getPredicates("A1", 50, null, null, null, null, null, "ERP$P1", null, null, null);

        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(1, items.size(), "only table A (touched by ERP$P1) must survive");
        assertEquals("A", items.get(0).get("tableName").asText());
    }

    /**
     * Calibrated by having {@code directProcId} fall through unread (as if the scope trim never
     * ran) — confirmed the test failed because both tables survived, before restoring.
     */
    @Test
    void nodeIdsNarrowsToOnlyMatchingProcedures() throws Exception {
        String tableA = "{\"name\":\"A\",\"schema\":\"S\",\"objectType\":\"TABLE\",\"allOperations\":[\"SELECT\"],"
                + "\"usedBy\":[{\"nodeId\":\"ERP$P1\",\"nodeName\":\"P1\",\"operations\":{\"SELECT\":[1]},"
                + "\"predicates\":[{\"columnName\":\"X\",\"operator\":\"=\",\"value\":\"1\",\"lineNumber\":1}]}]}";
        String tableB = "{\"name\":\"B\",\"schema\":\"S\",\"objectType\":\"TABLE\",\"allOperations\":[\"SELECT\"],"
                + "\"usedBy\":[{\"nodeId\":\"ERP$P2\",\"nodeName\":\"P2\",\"operations\":{\"SELECT\":[1]},"
                + "\"predicates\":[{\"columnName\":\"Y\",\"operator\":\"=\",\"value\":\"2\",\"lineNumber\":1}]}]}";
        JsonNode doc = MAPPER.readTree("{\"totalTables\":2,\"tables\":[" + tableA + "," + tableB + "]}");
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.document(any(AnalysisQuery.class))).thenReturn(doc);

        ParserAnalysisController c = build(repo);
        var response = c.getPredicates("A1", 50, null, null, null, null, null, null, List.of("ERP$P2"), null, null);

        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(1, items.size());
        assertEquals("B", items.get(0).get("tableName").asText());
    }
}
