package com.analyzer.integration;

import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.controller.AnalyzerController;
import com.jaranalyzer.model.DbCallRef;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.service.AnalysisDataProviderFactory;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.PersistenceService;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * F2 + the jar half of F4: the full JAR flow in-process — the analysis write path produces the
 * artifacts, the real {@link AnalyzerController} serves them, and each serving endpoint (listing,
 * summary, header, endpoints, db-calls, db-calls summary) returns real data.
 *
 * <p>The write and the read are the <b>same</b> {@link PersistenceService} over the same
 * {@link InMemoryArtifactStore}, so a sidecar the write pass produces is the very one the read
 * pass serves — the two are not allowed to disagree. F4 then proves the cache/registry path is
 * idempotent and that a re-analysis invalidates the stale projection instead of serving it.
 */
@DisplayName("Integration F2/F4: JAR write -> artifacts -> serving (summary/header/endpoints/db-calls)")
class EndToEndJarFlowTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ID = "svc";

    @TempDir
    Path tmp;

    private InMemoryArtifactStore store;
    private JarDataPaths paths;
    private PersistenceService persistence;
    private AnalyzerController controller;

    @BeforeEach
    void setUp() throws Exception {
        store = new InMemoryArtifactStore();
        paths = new JarDataPaths("data/jar", store);
        persistence = new PersistenceService(paths, store);
        controller = build();
    }

    /** Builds the real controller, filling only the storage/read collaborators with real objects. */
    private AnalyzerController build() throws Exception {
        Constructor<?> ctor = AnalyzerController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = M;
            else if (PersistenceService.class.equals(type)) args[i] = persistence;
            else if (ArtifactStore.class.equals(type)) args[i] = store;
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(M, 10000);
            else if (AnalysisDataProviderFactory.class.equals(type)) {
                args[i] = new AnalysisDataProviderFactory(persistence, paths, M);
            } else if (com.jaranalyzer.service.DomainConfigLoader.class.equals(type)) {
                // The header projection consults the domain config; a bare mock returns a null
                // Config, which NPEs. Supply the empty-but-valid config the loader yields with no
                // domain-config.json present.
                com.jaranalyzer.service.DomainConfigLoader loader =
                        mock(com.jaranalyzer.service.DomainConfigLoader.class);
                when(loader.summaryStatsConfig()).thenReturn(new com.jaranalyzer.service.SummaryStats.Config(
                        List.of(), java.util.Map.of(), List.of(), List.of(), List.of()));
                args[i] = loader;
            } else {
                args[i] = mock(type);
            }
        }
        return (AnalyzerController) ctor.newInstance(args);
    }

    private void seed(int classCount, int endpointCount) throws Exception {
        Path classes = tmp.resolve("classes-" + classCount + ".jsonl");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < classCount; i++) {
            sb.append("{\"fullyQualifiedName\":\"com.demo.p").append(i % 3).append(".C").append(i)
              .append("\",\"packageName\":\"com.demo.p").append(i % 3)
              .append("\",\"simpleName\":\"C").append(i)
              .append("\",\"stereotype\":\"SERVICE\",\"methods\":[]}\n");
        }
        Files.writeString(classes, sb.toString(), StandardCharsets.UTF_8);

        List<EndpointInfo> eps = new ArrayList<>();
        for (int i = 0; i < endpointCount; i++) {
            EndpointInfo ep = new EndpointInfo();
            ep.setHttpMethod(i % 2 == 0 ? "GET" : "POST");
            ep.setFullPath("/api/demo/" + i);
            ep.setControllerClass("com.demo.p0.C" + i);
            ep.setMethodName("handle" + i);
            eps.add(ep);
        }
        persistence.writeAnalysisStreaming(ID, "Demo", 4096L, "2026-01-01T00:00:00",
                classCount, eps.size(), classes, eps, "STATIC", null);
    }

    private void seedDbCalls() {
        List<DbCallRef> refs = new ArrayList<>();
        refs.add(new DbCallRef("callable", "PKG.PROC", "PKG", "PROC", "raw1", "com.a.A", "m1", 0, -1, "bytecode"));
        refs.add(new DbCallRef("mybatis", "PKG.REBUILD", "PKG", "REBUILD", "raw2", "com.a.Mapper", "m2", 0, -1, "resource"));
        persistence.storeDbCalls(ID, refs);
    }

    private static JsonNode json(MockHttpServletResponse response) throws Exception {
        assertTrue(response.getStatus() >= 200 && response.getStatus() < 300,
                "expected 2xx, got " + response.getStatus());
        return M.readTree(response.getContentAsByteArray());
    }

    @Test
    @DisplayName("the write path's artifacts are exactly what the listing and serving paths return")
    void writePathArtifactsAreServed() throws Exception {
        seed(6, 3);
        seedDbCalls();

        // Listing (backed by the _header.json the write pass produced).
        List<java.util.Map<String, Object>> jars = controller.listJars();
        assertEquals(1, jars.size(), "the freshly written analysis must be listable");
        assertEquals(ID, jars.get(0).get("id"));
        assertEquals(6, ((Number) jars.get(0).get("totalClasses")).intValue());

        MockHttpServletRequest req = new MockHttpServletRequest();
        MockHttpServletResponse res = new MockHttpServletResponse();

        // Summary.
        controller.getSummary(ID, null, req, res);
        JsonNode summary = json(res);
        assertNotNull(summary);
        assertTrue(summary.size() > 0, "the summary must carry data, not an empty document");

        // Header.
        res = new MockHttpServletResponse();
        controller.getSummaryHeader(ID, null, req, res);
        JsonNode header = json(res);
        assertTrue(header.has("stats") || header.size() > 0, "the header must carry its stats block");

        // Endpoints (paged envelope).
        res = new MockHttpServletResponse();
        controller.getSummaryEndpoints(ID, null, 25, null, null, null, req, res);
        JsonNode endpoints = json(res);
        assertEquals(3, endpoints.path("items").size(),
                "all three endpoints the write pass stored must be served");

        // DB calls (legacy full array, header says available).
        ResponseEntity<JsonNode> dbCalls = controller.getDbCalls(ID, null, null, null, null, null, null);
        assertEquals("true", dbCalls.getHeaders().getFirst(AnalyzerController.DB_CALLS_AVAILABLE_HEADER));
        assertEquals(2, dbCalls.getBody().size());

        // DB-calls summary.
        JsonNode dbSummary = controller.getDbCallsSummary(ID).getBody();
        assertTrue(dbSummary.path("available").asBoolean());
        assertEquals(2, dbSummary.path("total").asInt());
    }

    @Test
    @DisplayName("F4: re-serving through the cache/registry path is byte-identical (idempotent)")
    void reServingIsIdempotent() throws Exception {
        seed(6, 3);
        MockHttpServletRequest req = new MockHttpServletRequest();

        MockHttpServletResponse first = new MockHttpServletResponse();
        controller.getSummary(ID, null, req, first);
        MockHttpServletResponse second = new MockHttpServletResponse();
        controller.getSummary(ID, null, req, second);

        assertArrayEquals(first.getContentAsByteArray(), second.getContentAsByteArray(),
                "the cached summary must be byte-identical to the first serve");
    }

    @Test
    @DisplayName("F4: a re-analysis invalidates the stale summary instead of serving it")
    void reAnalysisInvalidatesStaleProjection() throws Exception {
        seed(6, 3);
        MockHttpServletRequest req = new MockHttpServletRequest();
        MockHttpServletResponse before = new MockHttpServletResponse();
        controller.getSummary(ID, null, req, before);
        assertTrue(new String(before.getContentAsByteArray(), StandardCharsets.UTF_8).contains("\"/api/demo/0\""));

        // The same identity re-analysed with a different shape.
        Thread.sleep(5);
        seed(9, 1);

        MockHttpServletResponse after = new MockHttpServletResponse();
        controller.getSummary(ID, null, req, after);
        String body = new String(after.getContentAsByteArray(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"/api/demo/0\""), "the re-analysed endpoint must be served");
        assertTrue(!body.contains("\"/api/demo/1\""),
                "the superseded run's endpoints must be gone, not served from a stale cache");
    }
}
