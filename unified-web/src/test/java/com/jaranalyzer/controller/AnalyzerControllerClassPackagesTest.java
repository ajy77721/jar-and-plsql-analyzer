package com.jaranalyzer.controller;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
 * UI-003 (§134) — {@code GET /classes/packages}, a genuine server-side aggregate (one row per
 * package, not per class) that lets `codeTree.js`'s package view stop fetching every class just to
 * render its own top level — see the endpoint's own doc comment for why this is the aggregate §118
 * found the full tree itself does not have.
 */
class AnalyzerControllerClassPackagesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static AnalyzerController build(AnalysisRepository repository) throws Exception {
        Constructor<?> ctor = AnalyzerController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = MAPPER;
            else if (AnalysisRepository.class.equals(type)) args[i] = repository;
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

    private static JsonNode cls(String pkg, String jar) throws Exception {
        return MAPPER.readTree("{\"packageName\":\"" + pkg + "\",\"sourceJar\":\"" + jar + "\"}");
    }

    @Test
    void countsClassesPerPackageWithoutReturningTheClassesThemselves() throws Exception {
        AnalysisRepository repo = mock(AnalysisRepository.class);
        // Built before the stub, not inline as .thenReturn(streamOf(...)) — streamOf's own mock()/
        // when() calls, evaluated as a Java argument, land while Mockito considers the outer
        // when(repo.rows(...)) stubbing still "open" and throws UnfinishedStubbingException. Not a
        // real defect in the endpoint, a Mockito ordering gotcha caught before it obscured a real
        // failure.
        RowStream stream = streamOf(List.of(
                cls("com.acme.service", ""),
                cls("com.acme.service", ""),
                cls("com.acme.web", ""),
                cls("com.acme.util", "lib-1.0.jar")
        ));
        when(repo.rows(any(AnalysisQuery.class), any())).thenReturn(stream);

        AnalyzerController c = build(repo);
        var response = c.getClassPackages("jar1", null);

        assertEquals(200, response.getStatusCode().value());
        ArrayNode body = (ArrayNode) response.getBody();
        assertTrue(body.isArray(), "must be a package-summary array, not the class tree itself");
        assertEquals(3, body.size(), "3 distinct package/jar pairs, not 4 classes");

        boolean foundService = false;
        for (JsonNode row : body) {
            assertTrue(!row.has("simpleName"), "must not carry per-class fields — this is a summary, not the tree");
            if ("com.acme.service".equals(row.path("packageName").asText())) {
                foundService = true;
                assertEquals(2, row.path("classCount").asLong());
            }
        }
        assertTrue(foundService, "com.acme.service must be present in the summary");
    }

    @Test
    void aMissingAnalysisIs404NotAnException() throws Exception {
        AnalysisRepository repo = mock(AnalysisRepository.class);
        when(repo.rows(any(AnalysisQuery.class), any())).thenThrow(new java.io.IOException("not found"));

        AnalyzerController c = build(repo);
        var response = c.getClassPackages("missing", null);

        assertEquals(404, response.getStatusCode().value());
    }
}
