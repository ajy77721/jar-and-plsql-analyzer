package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The summary cache must be produced with memory that does not grow with the analysis.
 *
 * <p>It used to be built entirely in a {@code ByteArrayOutputStream} so it could be written twice —
 * once plain, once gzipped. The summary grows with class and endpoint count (430 KB measured at 119
 * classes, ~35 MB projected at 1,597), so on a very large analysis that buffer becomes the process's
 * peak memory and eventually an {@code OutOfMemoryError}. Everything else on this path streams; this
 * was the one place that did not.
 *
 * <p>These tests pin the two properties that matter: both artifacts are still produced and agree,
 * and the plain one is byte-identical to what the gzip decompresses to.
 */
class SummaryCacheStreamingTest {

    @TempDir
    Path tmp;

    private static final String JAR = "demo.jar";

    /**
     * Paths root at {root}/jar and keys carry an explicit "jar" segment; the store roots above it.
     *
     * <p>The store-AWARE constructor matters: the providers reach the store through
     * {@code JarDataPaths.store()}, and with the store-less one they NPE inside
     * {@code checkHasPrecomputedAggregates}. {@code preGenerateSummaryCache} catches Exception and
     * only logs, so that surfaces as two empty artifacts rather than a failure — which is exactly
     * how this test first failed.
     */
    private JarDataPaths paths(InMemoryArtifactStore store) {
        return new JarDataPaths(tmp.resolve("jar").toString(), store);
    }

    private PersistenceService service(InMemoryArtifactStore store) {
        return new PersistenceService(paths(store), store);
    }

    /** A minimal analysis document the summary streamer can walk. */
    private void seedAnalysis(InMemoryArtifactStore store, JarDataPaths paths, int classes)
            throws Exception {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("{\"jarName\":\"").append(JAR).append("\",\"projectName\":\"Demo\",")
          .append("\"totalClasses\":").append(classes).append(",\"totalEndpoints\":0,")
          .append("\"analysisMode\":\"STATIC\",\"classes\":[");
        for (int i = 0; i < classes; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"fullyQualifiedName\":\"com.demo.C").append(i)
              .append("\",\"simpleName\":\"C").append(i)
              .append("\",\"packageName\":\"com.demo\",\"stereotype\":\"SERVICE\"}");
        }
        sb.append("],\"endpoints\":[]}");
        try (OutputStream out = store.write(paths.analysisKey(JAR))) {
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("both the plain and gzip artifacts are written, and they agree")
    void bothArtifactsAgree() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        seedAnalysis(store, paths, 50);

        service(store).preGenerateSummaryCache(JAR, true);

        ArtifactKey plainKey = paths.summaryCacheStaticKey(JAR);
        ArtifactKey gzKey = plainKey.parent().resolve(plainKey.leaf() + ".gz");
        assertTrue(store.exists(plainKey), "plain summary must be written");
        assertTrue(store.exists(gzKey), "gzip summary must be written");

        byte[] plain = store.read(plainKey).readAllBytes();
        assertTrue(plain.length > 0, "summary must not be empty");

        // The tee is only correct if the gzip decompresses to exactly the plain bytes. Producing the
        // document once and writing it twice is the whole point; a divergence here would mean the UI
        // served different content depending on whether it asked for the compressed copy.
        byte[] unzipped;
        try (InputStream in = store.read(gzKey);
             GZIPInputStream gz = new GZIPInputStream(in);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            gz.transferTo(out);
            unzipped = out.toByteArray();
        }
        assertArrayEquals(plain, unzipped,
                "gzip copy must decompress to the plain copy byte for byte");
    }

    @Test
    @DisplayName("the summary is valid JSON carrying the analysis identity")
    void summaryIsValidJson() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        seedAnalysis(store, paths, 10);

        service(store).preGenerateSummaryCache(JAR, true);

        var node = new ObjectMapper().readTree(store.read(paths.summaryCacheStaticKey(JAR)));
        assertFalse(node.isMissingNode(), "summary must parse as JSON");
        assertEquals(JAR, node.path("jarName").asText(),
                "a streamed generator must still emit the header fields");
    }

    @Test
    @DisplayName("a large analysis still produces a correct summary")
    void largeAnalysisStillWorks() throws Exception {
        // 4,000 classes — 80x the other fixtures. The point is not a timing or memory number (a
        // heap-delta assertion in a JVM is not evidence of anything), but that the streamed path
        // produces a complete, valid summary at a size where the old buffer would have been large.
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        seedAnalysis(store, paths, 4000);

        service(store).preGenerateSummaryCache(JAR, true);

        long size = store.size(paths.summaryCacheStaticKey(JAR));
        assertTrue(size > 100_000, "expected a substantial summary, got " + size);
        var node = new ObjectMapper().readTree(store.read(paths.summaryCacheStaticKey(JAR)));
        assertEquals(JAR, node.path("jarName").asText());
    }

    @Test
    @DisplayName("the summary is never staged in a whole-output buffer")
    void noWholeOutputBuffer() throws Exception {
        // The structural guarantee, asserted structurally. This is the actual regression risk: the
        // method is easy to "simplify" back into building a byte[] and writing it twice, which is
        // correct on small inputs and an OutOfMemoryError on large ones. A behavioural test cannot
        // distinguish the two; reading the source can.
        String src = java.nio.file.Files.readString(java.nio.file.Path.of(
                "src/main/java/com/jaranalyzer/service/PersistenceService.java"));
        int from = src.indexOf("public void preGenerateSummaryCache");
        assertTrue(from > 0, "method not found — did it move?");
        int to = src.indexOf("\n    }", from);
        String body = src.substring(from, to);
        // Match the ALLOCATION, not the bare type name — the method's own comment explains why the
        // buffer was removed, and matching the name flags that comment. Three scanners in this
        // codebase have now been tripped by their own documentation.
        assertFalse(body.contains("new java.io.ByteArrayOutputStream")
                        || body.contains("new ByteArrayOutputStream"),
                "preGenerateSummaryCache must stream to both outputs, not buffer the whole summary");
        assertTrue(body.contains("TeeOutputStream"),
                "expected the tee that writes plain and gzip in one pass");
    }

    @Test
    @DisplayName("a missing analysis is a no-op, not an exception")
    void missingAnalysisIsNoOp() {
        var store = new InMemoryArtifactStore();
        assertDoesNotThrow(() -> service(store).preGenerateSummaryCache("absent.jar", true));
        assertEquals(0, store.objectCount());
    }
}
