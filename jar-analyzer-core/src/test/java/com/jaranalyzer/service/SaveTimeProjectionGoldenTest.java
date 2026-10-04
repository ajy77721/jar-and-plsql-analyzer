package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.storage.LocalHandle;
import com.jaranalyzer.model.EndpointInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plans B1/B3 golden test: the summary and the class tree built DURING the document write (the
 * {@link SaveTimeProjections} accumulator) must be byte-for-byte identical to the same sidecars
 * built by re-reading the committed document ({@code preGenerateSummaryCache} /
 * {@code preGenerateClassTreeCache}).
 *
 * <p>The fixtures deliberately exercise every branch the projections have: a {@code @Document}
 * class (the entity-collection map), a {@code @Scheduled} method (the scheduled-jobs list), an
 * outbound HTTP-client invocation (the class-tree's {@code hasOutboundRestCall}), and an endpoint
 * whose {@code externalCalls} array exceeds the summary's cap (so the truncation marker is
 * emitted).
 */
class SaveTimeProjectionGoldenTest {

    @TempDir
    Path tmp;

    private static final String JAR = "golden.jar";

    private JarDataPaths paths(InMemoryArtifactStore store) {
        return new JarDataPaths(tmp.resolve("jar").toString(), store);
    }

    private Path classesJsonl() throws Exception {
        Path f = tmp.resolve("classes.jsonl");
        List<String> lines = new ArrayList<>();
        lines.add("{\"fullyQualifiedName\":\"com.demo.Order\",\"packageName\":\"com.demo\","
                + "\"simpleName\":\"Order\",\"stereotype\":\"ENTITY\",\"sourceJar\":\"golden.jar\","
                + "\"sourcePath\":\"Order.java\",\"isInterface\":false,\"isAbstract\":false,"
                + "\"isEnum\":false,\"superClass\":\"java.lang.Object\","
                + "\"interfaces\":[\"java.io.Serializable\"],"
                + "\"annotations\":[{\"name\":\"Document\",\"attributes\":{\"collection\":\"orders\"}}],"
                + "\"fields\":[{\"name\":\"id\",\"type\":\"String\",\"accessFlags\":2,"
                + "\"annotations\":[{\"name\":\"Id\"}]}],"
                + "\"methods\":[{\"name\":\"sweep\",\"returnType\":\"void\",\"accessFlags\":1,"
                + "\"startLine\":10,\"endLine\":20,"
                + "\"annotations\":[{\"name\":\"Scheduled\",\"attributes\":{\"cron\":\"0 0 * * *\"}}],"
                + "\"invocations\":[{\"ownerClass\":\"org.springframework.web.client.RestTemplate\","
                + "\"methodName\":\"getForObject\"}]}]}");
        lines.add("{\"fullyQualifiedName\":\"com.demo.Service\",\"packageName\":\"com.demo\","
                + "\"simpleName\":\"Service\",\"stereotype\":\"SERVICE\",\"sourceJar\":\"golden.jar\","
                + "\"methods\":[]}");
        Files.writeString(f, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        return f;
    }

    private List<EndpointInfo> endpoints() {
        List<EndpointInfo> eps = new ArrayList<>();
        EndpointInfo ep = new EndpointInfo();
        ep.setHttpMethod("GET");
        ep.setFullPath("/api/orders");
        ep.setControllerClass("com.demo.Service");
        ep.setControllerSimpleName("Service");
        ep.setMethodName("list");
        ep.setReturnType("java.util.List");
        ep.setTotalMethods(7);
        ep.setTotalLoc(123);
        ep.setInScopeCalls(4);
        ep.setExternalScopeCalls(2);
        ep.setModules(List.of("main", "orders"));
        // Over the summary cap of 100, so the truncation branch and its *Total marker both fire.
        List<Map<String, String>> ext = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            Map<String, String> m = new LinkedHashMap<>();
            m.put("ownerClass", "com.demo.External" + i);
            m.put("methodName", "call" + i);
            ext.add(m);
        }
        ep.setExternalCalls(ext);
        ep.setBeans(List.of(Map.of("beanType", "Repo", "fieldName", "repo")));
        List<Map<String, Object>> flows = new ArrayList<>();
        flows.add(Map.of("collection", "orders", "operation", "find"));
        ep.setDynamicFlows(flows);
        eps.add(ep);

        EndpointInfo ep2 = new EndpointInfo();
        ep2.setHttpMethod("POST");
        ep2.setFullPath("/api/orders");
        ep2.setControllerClass("com.demo.Service");
        ep2.setMethodName("create");
        eps.add(ep2);
        return eps;
    }

    private void save(ArtifactStore store, JarDataPaths paths, boolean corrected)
            throws Exception {
        PersistenceService service = new PersistenceService(paths, store);
        Path jsonl = classesJsonl();
        if (corrected) {
            service.writeAnalysisStreamingCorrected(JAR, "Golden", 2048L, "2026-01-01T00:00:00",
                    2, 2, jsonl, endpoints(), "CORRECTED");
        } else {
            service.writeAnalysisStreaming(JAR, "Golden", 2048L, "2026-01-01T00:00:00",
                    2, 2, jsonl, endpoints(), "STATIC", null);
        }
    }

    @Test
    @DisplayName("B1: summary built during the write == the re-read-built summary, byte for byte")
    void summaryAccumulatorMatchesReRead() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, false);

        byte[] fromWrite = store.read(paths.summaryCacheStaticKey(JAR)).readAllBytes();
        byte[] gzFromWrite = store.read(
                paths.summaryCacheStaticKey(JAR).parent()
                        .resolve(paths.summaryCacheStaticKey(JAR).leaf() + ".gz")).readAllBytes();

        // Rebuild by re-reading the committed document; this overwrites the same keys.
        new PersistenceService(paths, store).preGenerateSummaryCache(JAR, true);

        byte[] reRead = store.read(paths.summaryCacheStaticKey(JAR)).readAllBytes();
        byte[] gzReRead = store.read(
                paths.summaryCacheStaticKey(JAR).parent()
                        .resolve(paths.summaryCacheStaticKey(JAR).leaf() + ".gz")).readAllBytes();

        assertTrue(fromWrite.length > 0, "the summary must have been built");
        assertArrayEquals(reRead, fromWrite, "summary (plain) must be byte-identical");
        assertArrayEquals(gzReRead, gzFromWrite, "summary (gz) must be byte-identical");
    }

    @Test
    @DisplayName("B3: class tree built during the write == the re-read-built tree, byte for byte")
    void classTreeAccumulatorMatchesReRead() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, false);

        byte[] fromWrite = store.read(paths.classTreeCacheKey(JAR, null)).readAllBytes();

        new PersistenceService(paths, store).preGenerateClassTreeCache(JAR, true);

        byte[] reRead = store.read(paths.classTreeCacheKey(JAR, null)).readAllBytes();

        assertTrue(fromWrite.length > 0, "the class tree must have been built");
        assertArrayEquals(reRead, fromWrite, "class tree must be byte-identical");
    }

    @Test
    @DisplayName("B1/B3: the corrected variant's sidecars are byte-identical too")
    void correctedSidecarsMatchReRead() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, true);

        byte[] summaryFromWrite = store.read(paths.summaryCacheKey(JAR)).readAllBytes();
        byte[] treeFromWrite = store.read(paths.classTreeCacheKey(JAR, null)).readAllBytes();

        var service = new PersistenceService(paths, store);
        service.preGenerateSummaryCache(JAR, false);
        service.preGenerateClassTreeCache(JAR, false);

        assertArrayEquals(store.read(paths.summaryCacheKey(JAR)).readAllBytes(), summaryFromWrite,
                "corrected summary must be byte-identical");
        assertArrayEquals(store.read(paths.classTreeCacheKey(JAR, null)).readAllBytes(), treeFromWrite,
                "corrected class tree must be byte-identical");
    }

    @Test
    @DisplayName("the accumulator projects the rich fields, not just the header")
    void projectionsCarryTheFields() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, false);

        var summary = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(store.read(paths.summaryCacheStaticKey(JAR)));

        assertEquals("golden.jar", summary.path("jarName").asText());
        assertEquals(2, summary.path("classIndex").size());
        assertEquals("orders", summary.path("entityCollMap").path("com.demo.Order").asText());
        assertEquals(1, summary.path("scheduledCount").asInt());
        assertEquals("sweep", summary.path("scheduledJobs").get(0).path("methodName").asText());

        var ep0 = summary.path("endpoints").get(0);
        assertEquals(100, ep0.path("externalCalls").size(), "capped at 100");
        assertEquals(150, ep0.path("externalCallsTotal").asInt(), "real size stated");
        assertEquals("/api/orders", ep0.path("fullPath").asText());

        var tree = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(store.read(paths.classTreeCacheKey(JAR, null)));
        assertEquals(2, tree.size());
        var order = tree.get(0);
        assertEquals("com.demo.Order", order.path("fullyQualifiedName").asText());
        assertEquals(1, order.path("fieldCount").asInt());
        assertEquals(1, order.path("methodCount").asInt());
        assertTrue(order.path("methodSummaries").get(0).path("hasOutboundRestCall").asBoolean(),
                "the RestTemplate invocation must set hasOutboundRestCall");
    }

    @Test
    @DisplayName("the accumulator never reads the document it is writing")
    void accumulatorDoesNotReadTheDocument() throws Exception {
        var backing = new InMemoryArtifactStore();
        var counting = new CountingStore(backing);
        var paths = new JarDataPaths(tmp.resolve("jar").toString(), counting);
        save(counting, paths, false);

        assertEquals(0, counting.readsMatching("analysis.json"),
                "the summary/class-tree must be built from the write pass, not a re-read of analysis.json");
        assertEquals(0, counting.readsMatching("analysis_corrected.json"),
                "nor of the corrected document");
    }

    @Test
    @DisplayName("B3: the chunked class tree from the accumulator matches the re-read chunks")
    void chunksMatchReRead() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        Path f = tmp.resolve("many.jsonl");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1201; i++) {
            String n = String.format("C%04d", i);
            sb.append("{\"fullyQualifiedName\":\"com.demo.").append(n)
              .append("\",\"packageName\":\"com.demo\",\"simpleName\":\"").append(n)
              .append("\",\"stereotype\":\"SERVICE\",\"methods\":[]}\n");
        }
        Files.writeString(f, sb.toString(), StandardCharsets.UTF_8);
        var service = new PersistenceService(paths, store);
        service.writeAnalysisStreaming(JAR, "Many", 1L, "t", 1201, 0, f, List.of(), "STATIC", null);

        byte[] monoFromWrite = store.read(paths.classTreeCacheKey(JAR, null)).readAllBytes();
        byte[] chunk0FromWrite = store.read(paths.classTreeChunkKey(JAR, 0)).readAllBytes();
        ClassTreeChunks.Index idxFromWrite =
                ClassTreeChunks.loadIndex(store, paths.classTreeChunkIndexKey(JAR));
        assertEquals(1201, idxFromWrite.rowCount());
        assertEquals(3, idxFromWrite.chunkCount());

        service.preGenerateClassTreeCache(JAR, true);

        assertArrayEquals(store.read(paths.classTreeCacheKey(JAR, null)).readAllBytes(), monoFromWrite,
                "the monolithic class tree must be byte-identical");
        assertArrayEquals(store.read(paths.classTreeChunkKey(JAR, 0)).readAllBytes(), chunk0FromWrite,
                "the first chunk must be byte-identical");
        ClassTreeChunks.Index idxRe =
                ClassTreeChunks.loadIndex(store, paths.classTreeChunkIndexKey(JAR));
        assertEquals(idxFromWrite.chunkCount(), idxRe.chunkCount());
        assertEquals(idxFromWrite.rowCount(), idxRe.rowCount());
    }

    @Test
    @DisplayName("B2: the streaming corrected write emits its offsets sidecar")
    void correctedStreamingWriteEmitsOffsets() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, true);

        assertTrue(store.exists(paths.correctedOffsetsKey(JAR)),
                "analysis_corrected.json must be offset-addressable");
        var offsets = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(store.read(paths.correctedOffsetsKey(JAR)));
        assertEquals(2, offsets.path("classes").path("rowCount").asInt());
        assertEquals(2, offsets.path("endpoints").path("rowCount").asInt());
    }

    @Test
    @DisplayName("B2: rotating corrected -> previous moves the offsets sidecar with it")
    void rotationCarriesOffsetsToPrevious() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, true);
        byte[] before = store.read(paths.correctedOffsetsKey(JAR)).readAllBytes();

        var service = new PersistenceService(paths, store);
        service.rotateCorrected(JAR);

        assertTrue(store.exists(paths.correctedPrevKey(JAR)), "the document rotates");
        assertFalse(store.exists(paths.correctedOffsetsKey(JAR)), "the sidecar moves with it");
        assertTrue(store.exists(paths.correctedPrevOffsetsKey(JAR)), "the previous variant gets it");
        assertArrayEquals(before, store.read(paths.correctedPrevOffsetsKey(JAR)).readAllBytes(),
                "the moved sidecar is unchanged");

        // The previous view can now resolve its offsets sidecar instead of scanning the document.
        var prev = new ClaudeAnalysisProvider(paths.correctedPrevKey(JAR),
                new com.fasterxml.jackson.databind.ObjectMapper(), paths, JAR, "previous");
        assertEquals(paths.correctedPrevOffsetsKey(JAR), prev.offsetsKey());

        service.revertCorrected(JAR);
        assertTrue(store.exists(paths.correctedOffsetsKey(JAR)), "revert moves it back");
        assertFalse(store.exists(paths.correctedPrevOffsetsKey(JAR)));
    }

    /** Delegates every call, recording which keys were read — to prove the document is not. */
    private static final class CountingStore implements ArtifactStore {
        private final ArtifactStore delegate;
        private final List<String> reads = new ArrayList<>();

        CountingStore(ArtifactStore delegate) { this.delegate = delegate; }

        int readsMatching(String suffix) {
            return (int) reads.stream().filter(k -> k.endsWith(suffix)).count();
        }

        @Override public OutputStream write(ArtifactKey key) throws IOException { return delegate.write(key); }
        @Override public OutputStream write(ArtifactKey key, long n) throws IOException { return delegate.write(key, n); }
        @Override public InputStream read(ArtifactKey key) throws IOException {
            reads.add(key.toString());
            return delegate.read(key);
        }
        @Override public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
            reads.add(key.toString());
            return delegate.read(key, offset, length);
        }
        @Override public boolean exists(ArtifactKey key) { return delegate.exists(key); }
        @Override public long size(ArtifactKey key) { return delegate.size(key); }
        @Override public long lastModified(ArtifactKey key) { return delegate.lastModified(key); }
        @Override public boolean delete(ArtifactKey key) { return delegate.delete(key); }
        @Override public int deletePrefix(ArtifactKey prefix) throws IOException { return delegate.deletePrefix(prefix); }
        @Override public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
            return delegate.list(prefix, recursive);
        }
        @Override public void copy(ArtifactKey from, ArtifactKey to) throws IOException { delegate.copy(from, to); }
        @Override public void move(ArtifactKey from, ArtifactKey to) throws IOException { delegate.move(from, to); }
        @Override public LocalHandle materialize(ArtifactKey key) throws IOException { return delegate.materialize(key); }
    }
}
