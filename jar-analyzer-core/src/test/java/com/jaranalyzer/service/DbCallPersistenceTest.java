package com.jaranalyzer.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.jaranalyzer.model.DbCallRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The DB-call facet's artifacts: {@code db-calls/index.json} plus the {@code views/db-calls.json}
 * summary sidecar, and the three distinct read answers the API depends on.
 *
 * <p>The distinction that matters is <b>present-but-empty</b> ("analysed, none found") versus
 * <b>absent</b> ("analysed before the facet existed — re-analyse"). Collapsing them would tell a
 * user of an old analysis that their JAR makes no DB calls, which is a different and false claim.
 */
class DbCallPersistenceTest {

    private static final String ID = "svc-1.0.0";

    private InMemoryArtifactStore store;
    private PersistenceService service;

    @BeforeEach
    void setUp() {
        store = new InMemoryArtifactStore();
        service = new PersistenceService(new JarDataPaths("data/jar", store), store);
    }

    private static DbCallRef ref(String kind, String obj, String cls, String method) {
        return new DbCallRef(kind, obj, obj.contains(".") ? obj.substring(0, obj.indexOf('.')) : null,
                obj.contains(".") ? obj.substring(obj.indexOf('.') + 1) : obj,
                "raw", cls, method, 0, -1, "bytecode");
    }

    @Test
    @DisplayName("storeDbCalls writes index.json and views/db-calls.json; both read back")
    void storesAndLoads() {
        service.storeDbCalls(ID, List.of(
                ref("callable", "PKG.PROC", "com.a.A", "m1"),
                ref("mybatis", "PKG.REBUILD", "com.a.M", "m2")));

        assertTrue(service.hasDbCalls(ID));
        assertTrue(store.exists(service.getPaths().dbCallsIndexKey(ID)));
        assertTrue(store.exists(service.getPaths().dbCallsViewKey(ID)));

        JsonNode index = service.loadDbCalls(ID);
        assertNotNull(index);
        assertTrue(index.isArray());
        assertEquals(2, index.size());

        JsonNode summary = service.loadDbCallsSummary(ID);
        assertNotNull(summary);
        assertTrue(summary.path("available").asBoolean());
        assertEquals(2, summary.path("total").asInt());
        assertEquals(1, summary.path("byKind").path("callable").asInt());
        assertEquals(1, summary.path("byKind").path("mybatis").asInt());
        assertTrue(summary.path("byProcedure").isArray());
    }

    @Test
    @DisplayName("an empty facet is still present — distinct from absent")
    void emptyButPresent() {
        service.storeDbCalls(ID, List.of());

        assertTrue(service.hasDbCalls(ID), "an empty facet must still be recorded as analysed");
        JsonNode index = service.loadDbCalls(ID);
        assertNotNull(index);
        assertTrue(index.isArray());
        assertEquals(0, index.size());

        JsonNode summary = service.loadDbCallsSummary(ID);
        assertNotNull(summary);
        assertTrue(summary.path("available").asBoolean());
        assertEquals(0, summary.path("total").asInt());
    }

    @Test
    @DisplayName("an analysis that predates the facet reads as absent/unavailable")
    void absentIsUnavailable() {
        assertFalse(service.hasDbCalls(ID));
        assertNull(service.loadDbCalls(ID));
        assertNull(service.loadDbCallsSummary(ID));
    }

    @Test
    @DisplayName("the summary falls back to the index when the views sidecar is missing")
    void summaryRebuiltFromIndex() throws Exception {
        service.storeDbCalls(ID, List.of(ref("jpa", "PKG.P", "com.a.A", "m")));
        // Simulate a correction that cleared views/ (invalidateSummaryCache deletes the whole dir).
        store.delete(service.getPaths().dbCallsViewKey(ID));

        JsonNode summary = service.loadDbCallsSummary(ID);
        assertNotNull(summary, "the summary must be rebuilt from the index when the view is gone");
        assertEquals(1, summary.path("total").asInt());
        assertEquals(1, summary.path("byKind").path("jpa").asInt());
    }
}
