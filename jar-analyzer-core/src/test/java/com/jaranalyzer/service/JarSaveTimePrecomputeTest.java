package com.jaranalyzer.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.EndpointInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plan 2b-2d: a fresh JAR analysis must leave no first-hit penalty. The summary, the class tree
 * (monolithic and chunked) and the layout manifest are all produced at save time, so the first
 * visitor reads small sidecars instead of streaming the document.
 */
class JarSaveTimePrecomputeTest {

    @TempDir
    Path tmp;

    private static final String JAR = "demo.jar";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JarDataPaths paths(InMemoryArtifactStore store) {
        return new JarDataPaths(tmp.resolve("jar").toString(), store);
    }

    private Path classesJsonl(int classes) throws Exception {
        Path f = tmp.resolve("classes.jsonl");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < classes; i++) {
            sb.append("{\"fullyQualifiedName\":\"com.demo.p").append(i % 4).append(".C").append(i)
              .append("\",\"packageName\":\"com.demo.p").append(i % 4)
              .append("\",\"simpleName\":\"C").append(i)
              .append("\",\"stereotype\":\"SERVICE\",\"methods\":[]}\n");
        }
        Files.writeString(f, sb.toString(), StandardCharsets.UTF_8);
        return f;
    }

    private List<EndpointInfo> endpoints(int n) {
        List<EndpointInfo> eps = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            EndpointInfo ep = new EndpointInfo();
            ep.setHttpMethod("GET");
            ep.setFullPath("/api/demo/" + i);
            ep.setControllerClass("com.demo.p0.C" + i);
            ep.setMethodName("handle" + i);
            eps.add(ep);
        }
        return eps;
    }

    private void save(InMemoryArtifactStore store, JarDataPaths paths, int classes, int eps)
            throws Exception {
        new PersistenceService(paths, store).writeAnalysisStreaming(
                JAR, "Demo", 4096L, "2026-01-01T00:00:00", classes, eps,
                classesJsonl(classes), endpoints(eps), "STATIC", null);
    }

    @Test
    @DisplayName("summary and class tree are built at save time")
    void summaryAndClassTreeExistAtSave() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, 12, 2);

        assertTrue(store.exists(paths.summaryCacheStaticKey(JAR)),
                "the summary cache must be written at save, not on the first request");
        assertTrue(store.exists(paths.classTreeCacheKey(JAR, null)),
                "the class-tree cache must be written at save");

        JsonNode tree = MAPPER.readTree(store.read(paths.classTreeCacheKey(JAR, null)));
        assertTrue(tree.isArray());
        assertEquals(12, tree.size(), "one row per class");
        assertEquals("com.demo.p0.C0", tree.get(0).path("fullyQualifiedName").asText());
    }

    @Test
    @DisplayName("the manifest inventories artifacts, counts and sizes")
    void manifestIsComplete() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, 20, 3);

        assertTrue(store.exists(paths.manifestKey(JAR)), "manifest.json must be written");
        JsonNode m = MAPPER.readTree(store.read(paths.manifestKey(JAR)));

        assertEquals(1, m.path("manifestVersion").asInt());
        assertEquals(JAR, m.path("jarName").asText());
        assertEquals(20, m.path("counts").path("classes").asInt());
        assertEquals(3, m.path("counts").path("endpoints").asInt());
        assertEquals(4, m.path("counts").path("packages").asInt(), "distinct packages p0..p3");

        // analysis.json is listed with its real size.
        JsonNode analysisArtifact = null;
        for (JsonNode a : m.path("artifacts")) {
            if (a.path("key").asText().endsWith("/analysis.json")) analysisArtifact = a;
        }
        assertNotNull(analysisArtifact, "analysis.json must be inventoried");
        assertEquals(store.size(paths.analysisKey(JAR)), analysisArtifact.path("length").asLong());

        // offsets refs point at the sidecar, with the row counts.
        JsonNode off = m.path("offsets");
        assertTrue(off.path("classes").path("key").asText().endsWith("analysis.offsets.json"));
        assertEquals(20, off.path("classes").path("rowCount").asInt());
        assertEquals(3, off.path("endpoints").path("rowCount").asInt());

        // chunk section present.
        assertTrue(m.has("classTreeChunks"), "the chunked class tree must be inventoried");
        assertEquals(20, m.path("classTreeChunks").path("rowCount").asInt());
    }

    @Test
    @DisplayName("the class tree is chunked, and concatenating the chunks reproduces it")
    void classTreeChunksReproduceTheMonolith() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        int n = 1201;   // 3 chunks at 500 rows
        save(store, paths, n, 1);

        ClassTreeChunks.Index idx = ClassTreeChunks.loadIndex(store, paths.classTreeChunkIndexKey(JAR));
        assertNotNull(idx, "chunk index must be written");
        assertEquals(n, idx.rowCount());
        assertEquals(3, idx.chunkCount());
        assertEquals(500, idx.chunks().get(0).rowCount());
        assertEquals(201, idx.chunks().get(2).rowCount());

        // Every chunk file exists and the concatenation is the monolith, row for row.
        List<JsonNode> monolith = new ArrayList<>();
        for (JsonNode r : MAPPER.readTree(store.read(paths.classTreeCacheKey(JAR, null)))) {
            monolith.add(r);
        }
        List<JsonNode> fromChunks = new ArrayList<>();
        for (int c = 0; c < idx.chunkCount(); c++) {
            assertTrue(store.exists(paths.classTreeChunkKey(JAR, c)), "chunk " + c + " must exist");
            fromChunks.addAll(ClassTreeChunks.readChunk(store, paths.classTreeChunkKey(JAR, c)));
        }
        assertEquals(monolith, fromChunks, "chunks must concatenate to the monolithic array");
    }

    @Test
    @DisplayName("a chunk is seekable by its recorded bounds")
    void chunkIndexLocatesAKey() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        // One package, zero-padded names: document order is then fully-qualified-name order, which
        // is the precondition the recorded bounds (and chunkForFqn) rely on.
        Path f = tmp.resolve("ordered.jsonl");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1201; i++) {
            String n = String.format("C%04d", i);
            sb.append("{\"fullyQualifiedName\":\"com.demo.").append(n)
              .append("\",\"packageName\":\"com.demo\",\"simpleName\":\"").append(n)
              .append("\",\"stereotype\":\"SERVICE\",\"methods\":[]}\n");
        }
        Files.writeString(f, sb.toString(), StandardCharsets.UTF_8);
        new PersistenceService(paths, store).writeAnalysisStreaming(
                JAR, "Demo", 1L, "t", 1201, 0, f, List.of(), "STATIC", null);

        ClassTreeChunks.Index idx = ClassTreeChunks.loadIndex(store, paths.classTreeChunkIndexKey(JAR));
        // A name in the last chunk must resolve to the last chunk, not to the first.
        assertEquals(2, idx.chunkForFqn("com.demo.C1200"));
        assertEquals(0, idx.chunkForFqn("com.demo.C0000"));
    }

    @Test
    @DisplayName("invalidation drops the chunks, index and manifest too")
    void invalidateDropsDerivedChunks() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, 30, 1);
        var service = new PersistenceService(paths, store);
        assertTrue(store.exists(paths.manifestKey(JAR)));
        assertTrue(store.exists(paths.classTreeChunkKey(JAR, 0)));

        service.invalidateSummaryCache(JAR);

        assertFalse(store.exists(paths.classTreeChunkKey(JAR, 0)), "chunks must be dropped");
        assertFalse(store.exists(paths.classTreeChunkIndexKey(JAR)), "chunk index must be dropped");
        assertFalse(store.exists(paths.manifestKey(JAR)), "manifest must be dropped");
    }
}
