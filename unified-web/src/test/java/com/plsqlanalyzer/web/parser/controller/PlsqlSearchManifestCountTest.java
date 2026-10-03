package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import com.plsqlanalyzer.web.parser.service.AnalysisViewStore;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The empty-query {@code /search} hint: {@code totalStatements} comes from the fresh per-analysis
 * manifest's recorded count — no read of the statements index — and falls back to the index header
 * when the manifest is stale or absent.
 */
class PlsqlSearchManifestCountTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String NAME = "ORACLE~P~DEV~DEMO";

    private static void put(InMemoryArtifactStore store, String rel, String body) throws IOException {
        ArtifactKey key = AnalysisPaths.resolveKey(NAME, rel);
        try (OutputStream out = store.write(key)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static long totalStatements(InMemoryArtifactStore store) throws IOException {
        PlsqlSearchController controller = new PlsqlSearchController(store, M, new AnalysisViewStore(store));
        ResponseEntity<JsonNode> r = controller.search(NAME, null, null);
        assertEquals(200, r.getStatusCode().value());
        return r.getBody().get("totalStatements").asLong();
    }

    @Test
    void theFreshManifestCountIsPreferredOverTheStatementsIndexHeader() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, "api/index.json", "{}");
        put(store, "api/statements/index.json", "{\"totalStatements\":5,\"statements\":[]}");
        // The manifest records a DIFFERENT value: if the response reports it, the manifest was the
        // source. (In a real analysis the two agree; this makes the source observable.)
        put(store, "api/manifest.json", "{\"counts\":{\"statements\":99},\"facets\":{}}");
        store.setLastModifiedForTest(AnalysisPaths.resolveKey(NAME, "api/manifest.json"),
                store.lastModified(AnalysisPaths.resolveKey(NAME, "api/index.json")));

        assertEquals(99L, totalStatements(store), "the fresh manifest's count must be served");
    }

    @Test
    void aStaleManifestFallsBackToTheIndexHeader() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, "api/index.json", "{}");
        put(store, "api/statements/index.json", "{\"totalStatements\":5,\"statements\":[]}");
        put(store, "api/manifest.json", "{\"counts\":{\"statements\":99},\"facets\":{}}");
        ArtifactKey manifest = AnalysisPaths.resolveKey(NAME, "api/manifest.json");
        store.setLastModifiedForTest(manifest, store.lastModified(manifest) - 1000);

        assertEquals(5L, totalStatements(store), "a stale manifest must fall back to the index header");
    }

    @Test
    void noManifestFallsBackToTheIndexHeader() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, "api/index.json", "{}");
        put(store, "api/statements/index.json", "{\"totalStatements\":7,\"statements\":[]}");

        assertEquals(7L, totalStatements(store));
    }
}
