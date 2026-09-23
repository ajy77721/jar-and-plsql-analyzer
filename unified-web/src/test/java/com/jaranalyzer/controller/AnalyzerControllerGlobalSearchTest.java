package com.jaranalyzer.controller;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The JAR-side mirror of {@code ParserAnalysisController.getSearch}/{@code getSearchSummary} —
 * {@code AnalyzerController.getGlobalSearch}/{@code getGlobalSearchSummary}, normalizing classes
 * ({@code JAR_CLASS_TREE}) and dependency JARs ({@code JAR_BUNDLED_JARS}) into one
 * {@code {type, name, schema, matchedField, _idx, raw}} row set, paged via the same
 * {@code JarFacetSpecs.SEARCH}/{@code FacetPageResponder} machinery every other JAR facet uses.
 *
 * <p>Endpoints (the third real facet {@code buildSearchRows} scans) are deliberately not exercised
 * here: they come from {@code AnalysisDataProvider.streamSummaryEndpointPage}, which needs a real
 * backing document rather than a repository stub, and the endpoint scan is wrapped in its own
 * try/catch so a provider that cannot be resolved (as here, where {@code providerFactory} is a bare
 * mock) simply drops that facet rather than failing the whole request — confirmed by the fact these
 * tests see only CLASS/DEPENDENCY rows and still get a 200.
 */
class AnalyzerControllerGlobalSearchTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static AnalyzerController build(AnalysisRepository repository) throws Exception {
        Constructor<?> ctor = AnalyzerController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = MAPPER;
            else if (AnalysisRepository.class.equals(type)) args[i] = repository;
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(MAPPER, 10000);
            else args[i] = mock(type);
        }
        return (AnalyzerController) ctor.newInstance(args);
    }

    private static RowStream streamOf(List<JsonNode> rows) {
        Iterator<JsonNode> it = rows.iterator();
        RowStream rs = mock(RowStream.class);
        when(rs.hasNext()).thenAnswer(inv -> it.hasNext());
        when(rs.next()).thenAnswer(inv -> it.next());
        return rs;
    }

    private static AnalysisRepository repoWith(List<JsonNode> classes, List<JsonNode> deps) throws Exception {
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.rows(any(AnalysisQuery.class), any())).thenAnswer(inv -> {
            AnalysisQuery q = inv.getArgument(0);
            if (q.dataset() == AnalysisQuery.Dataset.JAR_CLASS_TREE) return streamOf(classes);
            if (q.dataset() == AnalysisQuery.Dataset.JAR_BUNDLED_JARS) return streamOf(deps);
            return streamOf(List.of());
        });
        return repo;
    }

    @Test
    void mergesClassesAndDependenciesIntoOneNormalizedRowSet() throws Exception {
        JsonNode classA = MAPPER.readTree(
                "{\"fullyQualifiedName\":\"com.acme.PaymentService\",\"simpleName\":\"PaymentService\","
                        + "\"packageName\":\"com.acme\"}");
        JsonNode classB = MAPPER.readTree(
                "{\"fullyQualifiedName\":\"com.acme.OrderController\",\"simpleName\":\"OrderController\","
                        + "\"packageName\":\"com.acme\"}");
        JsonNode dep = MAPPER.readTree("{\"name\":\"payment-lib-1.0.jar\"}");

        AnalyzerController c = build(repoWith(List.of(classA, classB), List.of(dep)));
        var response = c.getGlobalSearch("jar1", null, "payment", 50, null, null, null, null);

        assertEquals(200, response.getStatusCode().value());
        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(2, items.size(), "PaymentService class + payment-lib dependency, not OrderController");

        boolean sawClass = false;
        boolean sawDep = false;
        for (JsonNode row : items) {
            if ("CLASS".equals(row.path("type").asText())) {
                sawClass = true;
                assertEquals("com.acme.PaymentService", row.path("name").asText());
                assertEquals("com.acme", row.path("schema").asText());
            }
            if ("DEPENDENCY".equals(row.path("type").asText())) {
                sawDep = true;
                assertEquals("payment-lib-1.0.jar", row.path("name").asText());
            }
        }
        assertTrue(sawClass, "PaymentService class must be in the results");
        assertTrue(sawDep, "payment-lib-1.0.jar dependency must be in the results");
    }

    @Test
    void blankQueryIsRejectedWith400() throws Exception {
        AnalyzerController c = build(repoWith(List.of(), List.of()));

        var response = c.getGlobalSearch("jar1", null, "   ", 50, null, null, null, null);

        assertEquals(400, response.getStatusCode().value());
    }

    @Test
    void summaryEndpointReturnsTypeColumnValues() throws Exception {
        JsonNode classA = MAPPER.readTree(
                "{\"fullyQualifiedName\":\"com.acme.PaymentService\",\"simpleName\":\"PaymentService\","
                        + "\"packageName\":\"com.acme\"}");

        AnalyzerController c = build(repoWith(List.of(classA), List.of()));
        var response = c.getGlobalSearchSummary("jar1", null, "payment");

        assertEquals(200, response.getStatusCode().value());
        JsonNode columnValues = response.getBody().path("columnValues");
        assertTrue(columnValues.path("0").toString().contains("CLASS"),
                "type column filter values must include CLASS");
    }
}
