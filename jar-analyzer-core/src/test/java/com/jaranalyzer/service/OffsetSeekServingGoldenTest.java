package com.jaranalyzer.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.EndpointInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The offset-seek serving paths (plan 2a adoption) must be <b>byte-identical</b> to the legacy
 * full-document scans they replace, for a fixture that has the offsets sidecar.
 *
 * <p>Two things are being adopted and they differ in what the slice needs:
 *
 * <ul>
 *   <li>{@code /classes/by-index/{idx}}: the class slice, plus — when the class carries pooled
 *       {@code invocationRefs} — the document-level {@code invocationPool}, read from the prefix
 *       before {@code classes}. A class with no refs needs no pool.</li>
 *   <li>{@code /summary/endpoints?page=N}: the {@code endpoints[]} elements only; the endpoint
 *       pool's contents never affect the headers-mode response, so it is not read at all.</li>
 * </ul>
 *
 * <p>The fixture deliberately contains a class <em>with</em> invocations (so pooling is exercised),
 * a class <em>without</em> any (the branch that reads no pool), and endpoints whose pooled heavy
 * arrays are duplicated so the endpoint pool is non-empty.
 */
@DisplayName("Offset-seek serving is byte-identical to the legacy scan")
class OffsetSeekServingGoldenTest {

    @TempDir
    Path tmp;

    private static final String JAR = "demo.jar";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String[] POOLABLE = {
            "{\"ownerClass\":\"com.demo.Repo\",\"methodName\":\"save\",\"descriptor\":\"()V\"}",
            "{\"ownerClass\":\"com.demo.Repo\",\"methodName\":\"find\",\"descriptor\":\"()V\"}"
    };

    private JarDataPaths paths(InMemoryArtifactStore store) {
        return new JarDataPaths(tmp.resolve("jar").toString(), store);
    }

    /** Half the classes make calls (so they pool), half have empty methods (no refs at all). */
    private Path classesJsonl(int classes) throws Exception {
        Path f = tmp.resolve("classes.jsonl");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < classes; i++) {
            if (i % 2 == 0) {
                sb.append("{\"fullyQualifiedName\":\"com.demo.p").append(i % 3).append(".C").append(i)
                  .append("\",\"simpleName\":\"C").append(i)
                  .append("\",\"packageName\":\"com.demo.p").append(i % 3)
                  .append("\",\"stereotype\":\"SERVICE\",\"methods\":[")
                  .append("{\"name\":\"one\",\"descriptor\":\"()V\",\"invocations\":[")
                  .append(POOLABLE[0]).append(",").append(POOLABLE[1]).append("],\"startLine\":1},")
                  .append("{\"name\":\"two\",\"descriptor\":\"()V\",\"invocations\":[")
                  .append(POOLABLE[0]).append("],\"startLine\":9}")
                  .append("]}\n");
            } else {
                sb.append("{\"fullyQualifiedName\":\"com.demo.p").append(i % 3).append(".C").append(i)
                  .append("\",\"simpleName\":\"C").append(i)
                  .append("\",\"packageName\":\"com.demo.p").append(i % 3)
                  .append("\",\"stereotype\":\"SERVICE\",\"methods\":[]}\n");
            }
        }
        Files.writeString(f, sb.toString(), StandardCharsets.UTF_8);
        return f;
    }

    /** Endpoints with a duplicated {@code externalCalls} entry so the endpoint pool is non-empty. */
    private List<EndpointInfo> endpoints(int n) {
        List<EndpointInfo> eps = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            EndpointInfo ep = new EndpointInfo();
            ep.setHttpMethod("GET");
            ep.setFullPath("/api/demo/" + i);
            ep.setControllerClass("com.demo.web.Ctrl" + (i % 4));
            ep.setControllerSimpleName("Ctrl" + (i % 4));
            ep.setMethodName("handle" + i);
            ep.setReturnType("String");
            Map<String, String> call = new LinkedHashMap<>();
            call.put("className", "com.demo.ext.Client");
            call.put("methodName", "call");
            ep.setExternalCalls(List.of(call, Map.of("className", "com.demo.ext.Other",
                    "methodName", "only" + i)));
            ep.setBeans(List.of(Map.of("className", "com.demo.svc.Svc" + (i % 2))));
            eps.add(ep);
        }
        return eps;
    }

    private StaticAnalysisProvider save(InMemoryArtifactStore store, JarDataPaths paths,
                                        int nClasses, int nEndpoints) throws Exception {
        new PersistenceService(paths, store).writeAnalysisStreaming(
                JAR, "Demo", 4096L, "2026-01-01T00:00:00", nClasses, nEndpoints,
                classesJsonl(nClasses), endpoints(nEndpoints), "STATIC", null);
        return new StaticAnalysisProvider(paths.analysisKey(JAR), MAPPER, paths, JAR);
    }

    @Test
    @DisplayName("the fixture is written WITH the offsets sidecar and both pools")
    void fixtureHasOffsetsAndPools() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        save(store, paths, 6, 5);

        assertTrue(store.exists(paths.analysisOffsetsKey(JAR)), "offsets sidecar must exist");
        var segs = new AnalysisOffsetIndexReader().load(store, paths.analysisOffsetsKey(JAR));
        assertNotNull(segs.get(AnalysisOffsets.SEGMENT_CLASSES));
        assertNotNull(segs.get(AnalysisOffsets.SEGMENT_ENDPOINTS));

        String doc = store.read(paths.analysisKey(JAR)).readAllBytes().length > 0
                ? new String(store.read(paths.analysisKey(JAR)).readAllBytes(), StandardCharsets.UTF_8)
                : "";
        assertTrue(doc.contains("\"invocationPool\""), "the class pool must be exercised");
        assertTrue(doc.contains("\"endpointPool\""), "the endpoint pool must be exercised");
    }

    @Test
    @DisplayName("every class served by seek is byte-identical to the legacy scan")
    void classByIndexSeekMatchesLegacy() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        var provider = save(store, paths, 7, 4);

        for (int idx = 0; idx <= 7; idx++) {   // 7 is out of range: both must answer null
            byte[] legacy = legacyClass(provider, idx);
            byte[] seek = seekClass(provider, idx);
            assertArrayEquals(legacy, seek, "class " + idx + " must be byte-identical");
        }
        // And the response really is the document's class (not an empty stand-in).
        JsonNode served = MAPPER.readTree(seekClass(provider, 0));
        assertEquals("com.demo.p0.C0", served.path("fullyQualifiedName").asText());
        assertTrue(served.path("methods").get(0).has("invocations"),
                "the seek path must inflate pooled invocations back");
    }

    @Test
    @DisplayName("every endpoint page served by seek is byte-identical to the legacy scan")
    void endpointPageSeekMatchesLegacy() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        var provider = save(store, paths, 4, 9);

        List<Object[]> cases = new ArrayList<>();
        cases.add(new Object[]{null, -1, null});          // whole array
        cases.add(new Object[]{null, 3, null});           // first page
        cases.add(new Object[]{null, 2, null});           // short page
        cases.add(new Object[]{null, 0, null});           // empty page, hasMore true
        cases.add(new Object[]{null, 100, null});         // limit beyond the array
        cases.add(new Object[]{null, 3, "Ctrl1"});        // filtered
        cases.add(new Object[]{null, -1, "handle"});      // filtered, all
        cases.add(new Object[]{null, 4, "no-such-thing"}); // filtered, empty
        // A cursor minted from the legacy first page, replayed on both paths.
        cases.add(new Object[]{"GET /api/demo/2 com.demo.web.Ctrl2#handle2", 3, null});
        cases.add(new Object[]{"GET /api/demo/2 com.demo.web.Ctrl2#handle2", 3, "Ctrl3"});
        // A cursor whose endpoint does not exist -> 409 signal on both paths.
        cases.add(new Object[]{"GET /api/demo/999 com.demo.web.Ctrl0#gone", 3, null});

        for (Object[] c : cases) {
            String after = (String) c[0];
            int limit = (Integer) c[1];
            String q = (String) c[2];
            Paged legacy = legacyPage(provider, after, limit, q);
            Paged seek = seekPage(provider, after, limit, q);

            assertArrayEquals(legacy.json().getBytes(StandardCharsets.UTF_8),
                    seek.json().getBytes(StandardCharsets.UTF_8),
                    "page bytes differ for after=" + after + " limit=" + limit + " q=" + q);
            assertEquals(legacy.mark().lastId(), seek.mark().lastId(), "lastId");
            assertEquals(legacy.mark().hasMore(), seek.mark().hasMore(), "hasMore");
            assertEquals(legacy.mark().matched(), seek.mark().matched(), "matched");
            assertEquals(legacy.mark().cursorResolved(), seek.mark().cursorResolved(),
                    "cursorResolved");
        }
    }

    @Test
    @DisplayName("absent offsets fall back cleanly: nothing written, legacy path intact")
    void absentOffsetsFallBack() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        var provider = save(store, paths, 4, 3);
        store.delete(paths.analysisOffsetsKey(JAR));

        try (var gen = MAPPER.getFactory().createGenerator(new ByteArrayOutputStream())) {
            assertFalse(provider.streamClassByIndexSeek(gen, 0),
                    "no sidecar means the caller must use the legacy scan");
            assertNull(provider.streamSummaryEndpointPageSeek(gen, null, 3, null));
        }

        // The legacy scan is untouched and still answers.
        assertNotNull(MAPPER.readTree(legacyClass(provider, 0)));
        assertFalse(legacyPage(provider, null, 3, null).json().isEmpty());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private record Paged(String json, AbstractAnalysisDataProvider.EndpointPageMark mark) {}

    private byte[] legacyClass(AnalysisDataProvider provider, int idx) throws Exception {
        try (var parser = MAPPER.getFactory().createParser(provider.openStream());
             var out = new ByteArrayOutputStream();
             var gen = MAPPER.getFactory().createGenerator(out)) {
            provider.streamClassByIndex(parser, gen, idx);
            gen.flush();
            return out.toByteArray();
        }
    }

    private byte[] seekClass(AnalysisDataProvider provider, int idx) throws Exception {
        try (var out = new ByteArrayOutputStream();
             var gen = MAPPER.getFactory().createGenerator(out)) {
            boolean served = provider.streamClassByIndexSeek(gen, idx);
            gen.flush();
            assertTrue(served, "the fixture has offsets; seek must serve class " + idx);
            return out.toByteArray();
        }
    }

    private Paged legacyPage(AnalysisDataProvider provider, String after, int limit, String q)
            throws Exception {
        try (var parser = MAPPER.getFactory().createParser(provider.openStream());
             var out = new ByteArrayOutputStream();
             var gen = MAPPER.getFactory().createGenerator(out)) {
            var mark = provider.streamSummaryEndpointPage(parser, gen, after, limit, q);
            gen.flush();
            return new Paged(out.toString(StandardCharsets.UTF_8), mark);
        }
    }

    private Paged seekPage(AnalysisDataProvider provider, String after, int limit, String q)
            throws Exception {
        try (var out = new ByteArrayOutputStream();
             var gen = MAPPER.getFactory().createGenerator(out)) {
            var mark = provider.streamSummaryEndpointPageSeek(gen, after, limit, q);
            gen.flush();
            assertNotNull(mark, "the fixture has offsets; page seek must serve");
            return new Paged(out.toString(StandardCharsets.UTF_8), mark);
        }
    }
}
