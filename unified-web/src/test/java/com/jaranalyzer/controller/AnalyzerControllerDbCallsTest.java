package com.jaranalyzer.controller;

import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.DbCallRef;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.PersistenceService;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * The DB-call endpoints at the controller boundary: the paged envelope shape, the legacy full-array
 * shape, the unavailable shape for a JAR analysed before the facet existed, and the summary.
 *
 * <p>Backed by a real {@link PersistenceService} over an in-memory store, so the sidecar the write
 * path produces is the very one the read path serves — the two are not allowed to disagree.
 */
class AnalyzerControllerDbCallsTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ID = "svc";

    private InMemoryArtifactStore store;
    private JarDataPaths paths;
    private PersistenceService persistence;
    private AnalyzerController controller;

    @BeforeEach
    void setUp() throws Exception {
        store = new InMemoryArtifactStore();
        paths = new JarDataPaths("data/jar", store);
        persistence = new PersistenceService(paths, store);
        controller = build(persistence);
    }

    private AnalyzerController build(PersistenceService p) throws Exception {
        Constructor<?> ctor = AnalyzerController.class.getConstructors()[0];
        Parameter[] params = ctor.getParameters();
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> type = params[i].getType();
            if (ObjectMapper.class.equals(type)) args[i] = M;
            else if (PersistenceService.class.equals(type)) args[i] = p;
            else if (ArtifactStore.class.equals(type)) args[i] = store;
            else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(M, 10000);
            else args[i] = mock(type);
        }
        return (AnalyzerController) ctor.newInstance(args);
    }

    private static DbCallRef ref(String kind, String obj, String cls, String method) {
        return new DbCallRef(kind, obj, obj.contains(".") ? obj.substring(0, obj.indexOf('.')) : null,
                obj.contains(".") ? obj.substring(obj.indexOf('.') + 1) : obj, "raw-" + obj,
                cls, method, 0, -1, "bytecode");
    }

    private void seed() {
        List<DbCallRef> refs = new ArrayList<>();
        refs.add(ref("callable", "PKG.PROC", "com.a.A", "m1"));
        refs.add(ref("callable", "PKG.PROC2", "com.a.A", "m2"));
        refs.add(ref("mybatis", "PKG.REBUILD", "com.a.Mapper", "m3"));
        refs.add(ref("jpa", "PKG.JPA_PROC", "com.b.B", "m4"));
        persistence.storeDbCalls(ID, refs);
    }

    @Test
    @DisplayName("a paging parameter yields the page envelope with a cursor")
    void pagedEnvelopeShape() {
        seed();
        ResponseEntity<JsonNode> resp = controller.getDbCalls(ID, 2, null, null, null, null, null);

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertEquals("true", resp.getHeaders().getFirst(AnalyzerController.DB_CALLS_AVAILABLE_HEADER));
        JsonNode body = resp.getBody();
        assertNotNull(body);
        assertEquals(2, body.path("items").size());
        JsonNode page = body.path("page");
        assertEquals(4, page.path("total").asInt());
        assertTrue(page.path("hasMore").asBoolean());
        assertNotNull(page.path("nextCursor").asText(null), "a non-final page must carry a cursor");
        assertEquals(2, page.path("limit").asInt());
        assertEquals("PACKAGE", page.path("sort").asText());
    }

    @Test
    @DisplayName("no paging parameter yields the whole array (legacy shape)")
    void legacyFullArray() {
        seed();
        ResponseEntity<JsonNode> resp = controller.getDbCalls(ID, null, null, null, null, null, null);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertTrue(resp.getBody().isArray());
        assertEquals(4, resp.getBody().size());
        assertEquals("true", resp.getHeaders().getFirst(AnalyzerController.DB_CALLS_AVAILABLE_HEADER));
    }

    @Test
    @DisplayName("the cursor chain returns every row exactly once")
    void cursorChainCoversEverything() {
        seed();
        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 10; guard++) {
            ResponseEntity<JsonNode> resp =
                    controller.getDbCalls(ID, 3, cursor, null, null, null, null);
            JsonNode body = resp.getBody();
            body.path("items").forEach(r -> seen.add(r.path("dbObject").asText()));
            if (!body.path("page").path("hasMore").asBoolean()) break;
            cursor = body.path("page").path("nextCursor").asText();
        }
        assertEquals(4, seen.size());
        assertEquals(4, seen.stream().distinct().count(), "no row may be repeated: " + seen);
    }

    @Test
    @DisplayName("a JAR analysed before the facet -> unavailable header and empty body")
    void unavailableWhenAbsent() {
        ResponseEntity<JsonNode> legacy = controller.getDbCalls(ID, null, null, null, null, null, null);
        assertEquals("false", legacy.getHeaders().getFirst(AnalyzerController.DB_CALLS_AVAILABLE_HEADER));
        assertTrue(legacy.getBody().isArray());
        assertEquals(0, legacy.getBody().size());

        ResponseEntity<JsonNode> paged = controller.getDbCalls(ID, 10, null, null, null, null, null);
        assertEquals("false", paged.getHeaders().getFirst(AnalyzerController.DB_CALLS_AVAILABLE_HEADER));
        assertFalse(paged.getBody().path("available").asBoolean());
        assertEquals(0, paged.getBody().path("page").path("total").asInt());
        assertNotNull(paged.getBody().path("reason").asText(null));
    }

    @Test
    @DisplayName("server-side search and the kind column filter both apply")
    void searchAndKindFilter() {
        seed();
        // q matches the class name of the mybatis row only.
        ResponseEntity<JsonNode> bySearch =
                controller.getDbCalls(ID, 10, null, null, "mapper", null, null);
        assertEquals(1, bySearch.getBody().path("page").path("total").asInt());

        // cf=0:callable keeps only the two callable rows.
        ResponseEntity<JsonNode> byKind =
                controller.getDbCalls(ID, 10, null, null, null, List.of("0:callable"), null);
        assertEquals(2, byKind.getBody().path("page").path("total").asInt());
    }

    @Test
    @DisplayName("an unknown sort is a 400, not a silent reorder")
    void unknownSortRejected() {
        seed();
        ResponseEntity<JsonNode> resp =
                controller.getDbCalls(ID, 10, null, "BY_WHATEVER", null, null, null);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
    }

    @Test
    @DisplayName("summary is available with per-kind counts; unavailable for an old analysis")
    void summaryShape() {
        seed();
        ResponseEntity<JsonNode> summary = controller.getDbCallsSummary(ID);
        assertTrue(summary.getBody().path("available").asBoolean());
        assertEquals(4, summary.getBody().path("total").asInt());
        assertEquals(2, summary.getBody().path("byKind").path("callable").asInt());
        assertEquals(1, summary.getBody().path("byKind").path("mybatis").asInt());
        assertTrue(summary.getBody().path("byProcedure").isArray());

        ResponseEntity<JsonNode> absent = controller.getDbCallsSummary("never-analysed");
        assertFalse(absent.getBody().path("available").asBoolean());
        assertEquals(0, absent.getBody().path("total").asInt());
    }

    @Test
    @DisplayName("a clean analysis (empty facet) is available with zero rows")
    void emptyFacetIsAvailable() {
        persistence.storeDbCalls(ID, List.of());
        ResponseEntity<JsonNode> resp = controller.getDbCalls(ID, null, null, null, null, null, null);
        assertEquals("true", resp.getHeaders().getFirst(AnalyzerController.DB_CALLS_AVAILABLE_HEADER));
        assertEquals(0, resp.getBody().size());
        assertTrue(controller.getDbCallsSummary(ID).getBody().path("available").asBoolean());
    }
}
