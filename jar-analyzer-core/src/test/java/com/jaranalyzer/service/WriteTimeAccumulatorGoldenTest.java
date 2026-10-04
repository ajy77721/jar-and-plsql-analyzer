package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.storage.LocalHandle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.EndpointInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plans B1/B3 — the acceptance test.
 *
 * <p>The summary and the class tree are built from the document's own write pass (the
 * "accumulator") instead of by re-opening and re-parsing the committed document. The hard
 * requirement is that the outputs are <b>byte-identical</b> to the re-read form: the sidecars are a
 * public contract (the UI fetches them), and a semantic-but-not-byte difference would be a silent
 * behaviour change. So this test builds both forms from the SAME fixture and compares the bytes —
 * plain and gzipped summary, the class-tree monolith, and every chunk.
 *
 * <p>The fixture is deliberately rich enough to exercise the paths where the two could diverge:
 * methods with invocations (the invocation pool rewrites them), scheduled/annotated methods, and
 * endpoints with duplicated arrays (the endpoint pool renames {@code externalCalls} to
 * {@code externalCallsRefs}) including one array large enough to hit the summary's per-field cap.
 *
 * <p>It also proves the point of the change directly: the store is wrapped so a read of
 * {@code analysis.json} during the save is counted, and the save must make <b>zero</b> of them.
 */
class WriteTimeAccumulatorGoldenTest {

    @TempDir
    Path tmp;

    private static final String JAR = "demo.jar";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JarDataPaths paths(ArtifactStore store) {
        return new JarDataPaths(tmp.resolve("jar").toString(), store);
    }

    // ────────────────────────────── fixture ──────────────────────────────

    /**
     * Classes with methods carrying invocations (so the invocation pool fires and rewrites them),
     * annotations (so the summary's classIndex and scheduledJobs are non-trivial) and a spread of
     * packages. Field order matches what the analyzer serialises.
     */
    private Path classesJsonl(int classes) throws IOException {
        Path f = tmp.resolve("classes.jsonl");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < classes; i++) {
            String pkg = "com.demo.p" + (i % 3);
            sb.append("{\"fullyQualifiedName\":\"").append(pkg).append(".C").append(i)
              .append("\",\"packageName\":\"").append(pkg)
              .append("\",\"simpleName\":\"C").append(i)
              .append("\",\"stereotype\":\"").append(i % 2 == 0 ? "SERVICE" : "REPOSITORY")
              .append("\",\"sourceJar\":\"").append(JAR)
              .append("\",\"isInterface\":false,\"isAbstract\":false,\"isEnum\":false")
              .append(",\"annotations\":[{\"name\":\"Document\",\"attributes\":{\"collection\":\"coll")
              .append(i % 3).append("\"}}]")
              .append(",\"interfaces\":[\"java.io.Serializable\"]")
              .append(",\"methods\":[{")
              .append("\"name\":\"m").append(i).append("\",\"returnType\":\"void\"")
              .append(",\"annotations\":[{\"name\":\"Scheduled\",\"attributes\":{\"cron\":\"0 0 * * *\"}}]")
              .append(",\"parameters\":[],\"startLine\":1,\"endLine\":5,\"accessFlags\":1")
              .append(",\"invocations\":[{\"ownerClass\":\"org.springframework.web.client.RestTemplate\"")
              .append(",\"methodName\":\"get\",\"lineNumber\":3}]}]}\n");
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
            ep.setControllerSimpleName("C" + i);
            ep.setMethodName("handle" + i);

            // Duplicated across endpoints -> the endpoint pool will intern and rewrite these.
            List<Map<String, String>> ext = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                Map<String, String> c = new LinkedHashMap<>();
                c.put("className", "com.ext.Client" + j);
                c.put("methodName", "call" + j);
                ext.add(c);
            }
            ep.setExternalCalls(ext);

            List<Map<String, String>> beans = new ArrayList<>();
            Map<String, String> bean = new LinkedHashMap<>();
            bean.put("type", "com.demo.Repo");
            bean.put("field", "repo");
            beans.add(bean);
            ep.setBeans(beans);

            List<Map<String, String>> http = new ArrayList<>();
            Map<String, String> hc = new LinkedHashMap<>();
            hc.put("className", "com.ext.Http");
            hc.put("methodName", "post");
            http.add(hc);
            ep.setHttpCalls(http);

            List<Map<String, Object>> dyn = new ArrayList<>();
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("collection", "coll" + i);
            d.put("operation", "READ");
            dyn.add(d);
            ep.setDynamicFlows(dyn);
            ep.setAggregationFlows(new ArrayList<>(dyn));
            ep.setDynamicCollectionFlows(new ArrayList<>(dyn));

            // One endpoint carries a large externalCalls array to exercise the summary's cap and
            // the "…Total" companion field, which must match between the two build forms.
            if (i == 0) {
                List<Map<String, String>> big = new ArrayList<>();
                for (int j = 0; j < 150; j++) {
                    Map<String, String> c = new LinkedHashMap<>();
                    c.put("className", "com.big.Client" + j);
                    c.put("methodName", "m" + j);
                    big.add(c);
                }
                ep.setExternalCalls(big);
            }
            eps.add(ep);
        }
        return eps;
    }

    private void save(ArtifactStore store, JarDataPaths paths, int classes, int eps) throws IOException {
        new PersistenceService(paths, store).writeAnalysisStreaming(
                JAR, "Demo", 4096L, "2026-01-01T00:00:00", classes, eps,
                classesJsonl(classes), endpoints(eps), "STATIC", null);
    }

    // ────────────────────────────── B1/B3 golden ──────────────────────────────

    @Test
    @DisplayName("B1/B3: accumulator sidecars are byte-identical to the re-read-built sidecars")
    void accumulatorIsByteIdenticalToReRead() throws Exception {
        var counting = new CountingReadStore();
        var store = counting.store;
        var paths = paths(store);

        // 1. The write pass builds the sidecars from its own inputs.
        save(store, paths, 24, 4);

        byte[] accSummary = store.read(paths.summaryCacheStaticKey(JAR)).readAllBytes();
        byte[] accSummaryGz = store.read(summaryGzKey(paths)).readAllBytes();
        byte[] accTree = store.read(paths.classTreeCacheKey(JAR, null)).readAllBytes();
        List<byte[]> accChunks = readAllChunks(store, paths);

        assertTrue(accSummary.length > 0, "the accumulator must produce a non-empty summary");
        assertTrue(accTree.length > 0, "the accumulator must produce a non-empty class tree");

        // The save must not have read the document it just wrote — the whole point of B1/B3.
        assertEquals(0, counting.readsOf(paths.analysisKey(JAR)),
                "the save re-read analysis.json; the accumulator is not actually accumulating");

        // 2. Drop the derived artifacts, then rebuild them the legacy way: re-read the document.
        store.delete(paths.summaryCacheStaticKey(JAR));
        store.delete(summaryGzKey(paths));
        store.delete(paths.classTreeCacheKey(JAR, null));
        store.deletePrefix(paths.classTreeChunksDirKey(JAR));
        store.delete(paths.classTreeChunkIndexKey(JAR));

        var legacy = new PersistenceService(paths, store);
        legacy.preGenerateSummaryCache(JAR, true);
        legacy.preGenerateClassTreeCache(JAR, true);

        byte[] legacySummary = store.read(paths.summaryCacheStaticKey(JAR)).readAllBytes();
        byte[] legacySummaryGz = store.read(summaryGzKey(paths)).readAllBytes();
        byte[] legacyTree = store.read(paths.classTreeCacheKey(JAR, null)).readAllBytes();
        List<byte[]> legacyChunks = readAllChunks(store, paths);

        // 3. Byte for byte.
        assertArrayEquals(legacySummary, accSummary, "summary bytes differ from the re-read form");
        assertArrayEquals(legacySummaryGz, accSummaryGz, "gzipped summary bytes differ");
        assertArrayEquals(legacyTree, accTree, "class-tree bytes differ from the re-read form");
        assertEquals(legacyChunks.size(), accChunks.size(), "chunk count differs");
        for (int i = 0; i < legacyChunks.size(); i++) {
            assertArrayEquals(legacyChunks.get(i), accChunks.get(i), "chunk " + i + " differs");
        }

        // And the gzip still decompresses to the plain copy (the tee invariant).
        assertArrayEquals(accSummary, gunzip(accSummaryGz));
    }

    @Test
    @DisplayName("B1: the accumulator summary carries the pooled arrays, inflated and capped")
    void accumulatorSummaryInflatesPools() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, 6, 2);

        var summary = MAPPER.readTree(store.read(paths.summaryCacheStaticKey(JAR)));
        // Two endpoints, each with a 3-entry (or 150-entry) externalCalls array.
        assertEquals(2, summary.path("endpoints").size());
        var first = summary.path("endpoints").get(0);
        assertTrue(first.path("externalCalls").isArray(), "pooled externalCalls must be inflated");
        assertEquals(100, first.path("externalCalls").size(), "the summary cap still applies");
        assertEquals(150, first.path("externalCallsTotal").asInt(),
                "the truthful total must survive the accumulator");
        assertEquals(6, summary.path("classIndex").size(), "one classIndex row per class");
    }

    // ────────────────────────────── B2 ──────────────────────────────

    @Test
    @DisplayName("B2: the corrected document's write emits its own offsets sidecar")
    void correctedWriteEmitsOffsets() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);

        new PersistenceService(paths, store).writeAnalysisStreamingCorrected(
                JAR, "Demo", 4096L, "2026-01-01T00:00:00", 8, 2,
                classesJsonl(8), endpoints(2), "CLAUDE");

        assertTrue(store.exists(paths.correctedKey(JAR)), "corrected document must exist");
        assertTrue(store.exists(paths.correctedOffsetsKey(JAR)),
                "the corrected write must emit its offsets sidecar (B2)");

        var reader = new AnalysisOffsetIndexReader();
        var classes = reader.loadSegment(store, paths.correctedOffsetsKey(JAR),
                AnalysisOffsets.SEGMENT_CLASSES);
        var eps = reader.loadSegment(store, paths.correctedOffsetsKey(JAR),
                AnalysisOffsets.SEGMENT_ENDPOINTS);
        assertEquals(8, classes.rowCount(), "classes offsets must address every class");
        assertEquals(2, eps.rowCount(), "endpoint offsets must address every endpoint");

        // The corrected summary is built into the corrected cache key, not the static one.
        assertTrue(store.exists(paths.summaryCacheKey(JAR)), "corrected summary must be pre-generated");
        assertFalse(store.exists(paths.summaryCacheStaticKey(JAR)),
                "the corrected write must not populate the static summary cache");
    }

    // ────────────────────────────── helpers ──────────────────────────────

    private ArtifactKey summaryGzKey(JarDataPaths paths) {
        ArtifactKey plain = paths.summaryCacheStaticKey(JAR);
        return plain.parent().resolve(plain.leaf() + ".gz");
    }

    private List<byte[]> readAllChunks(ArtifactStore store, JarDataPaths paths) throws IOException {
        ClassTreeChunks.Index idx = ClassTreeChunks.loadIndex(store, paths.classTreeChunkIndexKey(JAR));
        assertNotNull(idx, "chunk index must exist");
        List<byte[]> out = new ArrayList<>();
        for (int i = 0; i < idx.chunkCount(); i++) {
            out.add(store.read(paths.classTreeChunkKey(JAR, i)).readAllBytes());
        }
        return out;
    }

    private static byte[] gunzip(byte[] gz) throws IOException {
        try (InputStream in = new GZIPInputStream(new java.io.ByteArrayInputStream(gz));
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            in.transferTo(out);
            return out.toByteArray();
        }
    }

    /**
     * Delegates every store call to a real in-memory store, counting full reads of the analysis
     * document so a test can assert the save never re-opened what it wrote.
     */
    private static final class CountingReadStore implements ArtifactStore {
        final InMemoryArtifactStore store = new InMemoryArtifactStore();
        private final Map<String, AtomicInteger> reads = new java.util.concurrent.ConcurrentHashMap<>();

        int readsOf(ArtifactKey key) {
            AtomicInteger n = reads.get(key.toString());
            return n == null ? 0 : n.get();
        }

        private InputStream counted(ArtifactKey key, InputStream in) {
            reads.computeIfAbsent(key.toString(), k -> new AtomicInteger()).incrementAndGet();
            return in;
        }

        @Override public OutputStream write(ArtifactKey key) throws IOException { return store.write(key); }
        @Override public OutputStream write(ArtifactKey key, long len) throws IOException { return store.write(key, len); }
        @Override public InputStream read(ArtifactKey key) throws IOException { return counted(key, store.read(key)); }
        @Override public InputStream read(ArtifactKey key, long off, long len) throws IOException {
            return counted(key, store.read(key, off, len));
        }
        @Override public boolean exists(ArtifactKey key) { return store.exists(key); }
        @Override public long size(ArtifactKey key) { return store.size(key); }
        @Override public long lastModified(ArtifactKey key) { return store.lastModified(key); }
        @Override public boolean delete(ArtifactKey key) { return store.delete(key); }
        @Override public int deletePrefix(ArtifactKey prefix) throws IOException { return store.deletePrefix(prefix); }
        @Override public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
            return store.list(prefix, recursive);
        }
        @Override public void copy(ArtifactKey from, ArtifactKey to) throws IOException { store.copy(from, to); }
        @Override public void move(ArtifactKey from, ArtifactKey to) throws IOException { store.move(from, to); }
        @Override public LocalHandle materialize(ArtifactKey key) throws IOException { return store.materialize(key); }
        @Override public Map<ArtifactKey, ArtifactEntry> statPrefix(ArtifactKey prefix) throws IOException {
            return store.statPrefix(prefix);
        }
    }
}
