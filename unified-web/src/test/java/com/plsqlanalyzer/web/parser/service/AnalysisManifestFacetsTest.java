package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-analysis manifest's {@code facets}/{@code counts} sections and the freshness rule that
 * gates them. A fresh manifest lets a read answer "this facet is empty" and "this is the count"
 * without touching the artifact; a stale one records nothing and every path falls back to the
 * legacy read (and its lazy backfill), unchanged.
 *
 * <p>Freshness is decided by {@link com.analyzer.storage.ArtifactStore#lastModified} — the storage
 * layer's write stamp — so a manifest and the index it describes that were written in the same
 * (backend-second) instant are still ordered exactly: equal is fresh, older is stale.
 */
class AnalysisManifestFacetsTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String NAME = "ORACLE~P~DEV~DEMO";

    private static ArtifactKey indexKey() {
        return AnalysisPaths.resolveKey(NAME, "api/index.json");
    }

    private static ArtifactKey manifestKey() {
        return AnalysisPaths.resolveKey(NAME, "api/manifest.json");
    }

    private static ArtifactKey viewsIndexKey() {
        return AnalysisPaths.resolveKey(NAME, "views/" + AnalysisViewStore.VIEWS_INDEX);
    }

    private static void put(InMemoryArtifactStore store, ArtifactKey key, String body) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** A minimal but valid manifest carrying counts and one facet row-count. */
    private static void writeManifest(InMemoryArtifactStore store, String facets, String counts) throws IOException {
        put(store, manifestKey(), "{\"manifestVersion\":1,\"counts\":" + counts + ",\"facets\":" + facets + "}");
    }

    @Test
    void aFreshManifestSuppliesTheFacetRowCountAndTheTopLevelCount() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{}");
        writeManifest(store, "{\"" + AnalysisViewStore.VIEWS_INDEX + "\":0}", "{\"statements\":42}");
        store.setLastModifiedForTest(manifestKey(), store.lastModified(indexKey()));

        AnalysisViewStore views = new AnalysisViewStore(store);
        assertEquals(0L, views.facetRows(NAME, AnalysisViewStore.VIEWS_INDEX));
        assertEquals(42L, views.manifestCount(NAME, "statements"));
        assertNull(views.manifestCount(NAME, "tables"), "an unrecorded count is unknown, not zero");
    }

    @Test
    void aStaleManifestRecordsNothing() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{}");
        writeManifest(store, "{\"" + AnalysisViewStore.VIEWS_INDEX + "\":0}", "{\"statements\":42}");
        // The index is newer than the manifest: the manifest describes a superseded run.
        store.setLastModifiedForTest(manifestKey(), store.lastModified(indexKey()) - 1000);

        AnalysisViewStore views = new AnalysisViewStore(store);
        assertNull(views.facetRows(NAME, AnalysisViewStore.VIEWS_INDEX));
        assertNull(views.manifestCount(NAME, "statements"));
    }

    @Test
    void anEqualWriteStampIsFresh() throws IOException {
        // The write stamp is millisecond-ordered, so a manifest and index written back to back are
        // distinguishable even when the backend's own timestamp has only second granularity. Equal
        // values are the same write pass and must be trusted.
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{}");
        writeManifest(store, "{}", "{\"nodes\":7}");
        long shared = 1_700_000_000_000L;
        store.setLastModifiedForTest(manifestKey(), shared);
        store.setLastModifiedForTest(indexKey(), shared);

        AnalysisViewStore views = new AnalysisViewStore(store);
        assertEquals(7L, views.manifestCount(NAME, "nodes"));
    }

    @Test
    void anEmptyRecordedFacetIsAnsweredWithoutRunningTheLegacyScan() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{}");
        writeManifest(store, "{\"" + AnalysisViewStore.VIEWS_INDEX + "\":0}", "{}");
        store.setLastModifiedForTest(manifestKey(), store.lastModified(indexKey()));

        AnalysisViewStore views = new AnalysisViewStore(store);
        AtomicInteger calls = new AtomicInteger();
        JsonNode result = views.readOrCompute(NAME, AnalysisViewStore.VIEWS_INDEX, () -> {
            calls.incrementAndGet();
            ArrayNode one = M.createArrayNode();
            one.add(M.createObjectNode());
            return one;
        });
        assertNotNull(result);
        assertTrue(result.isArray());
        assertEquals(0, result.size(), "the recorded-empty facet must answer with an empty array");
        assertEquals(0, calls.get(), "the legacy node scan must not run for a recorded-empty facet");
    }

    @Test
    void aStaleManifestStillBackfillsAndRecordsTheFacet() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{}");
        writeManifest(store, "{}", "{}");
        store.setLastModifiedForTest(manifestKey(), store.lastModified(indexKey()) - 1000);

        AnalysisViewStore views = new AnalysisViewStore(store);
        AtomicInteger calls = new AtomicInteger();
        JsonNode result = views.readOrCompute(NAME, AnalysisViewStore.VIEWS_INDEX, () -> {
            calls.incrementAndGet();
            ArrayNode two = M.createArrayNode();
            two.add(M.createObjectNode());
            two.add(M.createObjectNode());
            return two;
        });
        assertEquals(2, result.size());
        assertEquals(1, calls.get(), "a stale manifest must not suppress the lazy backfill");

        // The backfill updated the manifest's facets section and made it fresh, so a later read
        // serves the sidecar (and would answer an empty facet instantly).
        JsonNode facets = M.readTree(store.read(manifestKey())).get("facets");
        assertNotNull(facets);
        assertEquals(2L, facets.get(AnalysisViewStore.VIEWS_INDEX).asLong());
        assertTrue(store.lastModified(manifestKey()) >= store.lastModified(indexKey()),
                "recording a facet must not make the manifest stale");
    }

    @Test
    void anAbsentManifestLeavesEveryPathOnTheLegacyRead() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{}");

        AnalysisViewStore views = new AnalysisViewStore(store);
        assertNull(views.facetRows(NAME, AnalysisViewStore.VIEWS_INDEX));
        AtomicInteger calls = new AtomicInteger();
        JsonNode result = views.readOrCompute(NAME, AnalysisViewStore.VIEWS_INDEX, () -> {
            calls.incrementAndGet();
            return M.createArrayNode();
        });
        assertTrue(result.isArray());
        assertEquals(1, calls.get());
        assertFalse(store.exists(manifestKey()), "recording a facet must never create a manifest");
    }

    @Test
    void invalidateClearsTheFacetRecordsSoTheBackfillIsNotSuppressed() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{}");
        // The analysis had no views when the manifest was written...
        writeManifest(store, "{\"" + AnalysisViewStore.VIEWS_INDEX + "\":0}", "{}");
        store.setLastModifiedForTest(manifestKey(), store.lastModified(indexKey()));
        AnalysisViewStore views = new AnalysisViewStore(store);

        // ...a correction invalidates the sidecars, which must also drop the stale "empty" record.
        views.invalidate(NAME);
        JsonNode facets = M.readTree(store.read(manifestKey())).get("facets");
        assertNotNull(facets);
        assertTrue(facets.isEmpty(), "invalidate must clear the facet records");

        // With the record gone, the read recomputes (and backfills) instead of answering empty.
        AtomicInteger calls = new AtomicInteger();
        JsonNode result = views.readOrCompute(NAME, AnalysisViewStore.VIEWS_INDEX, () -> {
            calls.incrementAndGet();
            ArrayNode one = M.createArrayNode();
            one.add(M.createObjectNode());
            return one;
        });
        assertEquals(1, result.size());
        assertEquals(1, calls.get(), "the backfill must run once the facet record is cleared");
    }

    @Test
    void recordFacetUpdatesTheManifestInPlace() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{}");
        writeManifest(store, "{\"nodes\":1}", "{\"nodes\":1}");
        store.setLastModifiedForTest(manifestKey(), store.lastModified(indexKey()));

        AnalysisViewStore views = new AnalysisViewStore(store);
        ArrayNode rows = M.createArrayNode();
        rows.add(M.createObjectNode());
        views.recordFacet(NAME, AnalysisViewStore.GLOBALS_INDEX, rows);

        JsonNode doc = M.readTree(store.read(manifestKey()));
        assertEquals(1L, doc.get("facets").get("nodes").asLong(), "existing facets are preserved");
        assertEquals(1L, doc.get("facets").get(AnalysisViewStore.GLOBALS_INDEX).asLong());
    }

    /**
     * The table enrichers and the index patch rewrite {@code api/tables/index.json} and
     * {@code api/index.json} AFTER the crawler's write pass, so the lengths the manifest recorded at
     * crawl time go stale. {@code refreshArtifactLengths} re-records them from the store and
     * rewrites the manifest in place, keeping it the last artifact written (and therefore fresh
     * against the rewritten index).
     */
    @Test
    void refreshArtifactLengthsUpdatesRewrittenArtifactsAndStaysLast() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey idx = AnalysisPaths.resolveKey(NAME, "api/index.json");
        ArtifactKey tbl = AnalysisPaths.resolveKey(NAME, "api/tables/index.json");
        put(store, idx, "{\"nodes\":[{\"a\":1}]}");
        put(store, tbl, "{\"tables\":[{\"name\":\"T\"}]}");
        // A manifest carrying STALE lengths (1) for both rewritten artifacts.
        put(store, manifestKey(), "{\"manifestVersion\":1,\"artifacts\":["
                + "{\"key\":\"" + idx + "\",\"length\":1},"
                + "{\"key\":\"" + tbl + "\",\"length\":1}]}");
        store.setLastModifiedForTest(manifestKey(), store.lastModified(idx) - 1000);

        new AnalysisViewStore(store).refreshArtifactLengths(NAME,
                List.of("api/index.json", "api/tables/index.json"));

        JsonNode doc = M.readTree(store.read(manifestKey()));
        for (JsonNode a : doc.get("artifacts")) {
            String k = a.get("key").asText();
            ArtifactKey key = k.equals(idx.toString()) ? idx : tbl;
            assertEquals(store.size(key), a.get("length").asLong(), "length wrong for " + k);
        }
        assertTrue(store.lastModified(manifestKey()) >= store.lastModified(idx),
                "the refreshed manifest must not be older than the index it describes");
    }

    /** A missing manifest is a no-op — an analysis predating it stays on the legacy path. */
    @Test
    void refreshArtifactLengthsIsANoOpWithoutAManifest() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, AnalysisPaths.resolveKey(NAME, "api/index.json"), "{}");
        new AnalysisViewStore(store).refreshArtifactLengths(NAME, List.of("api/index.json"));
        assertFalse(store.exists(manifestKey()));
    }
}
