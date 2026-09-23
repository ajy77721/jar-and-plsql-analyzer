package com.jaranalyzer.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The tracker must be able to say how many endpoints failed.
 *
 * <p>It recorded {@code EpStatus.ERROR} per endpoint and nothing ever asked. {@code ClaudeJobExecutor}
 * therefore set the analysis to {@code CORRECTED} and called {@code markComplete} unconditionally,
 * because {@code enrichFromAnalysis} returns void and swallows per-endpoint failures — so a rescan
 * in which every endpoint was rate-limited was presented to the user as a completed correction,
 * over data Claude never touched.
 *
 * <p>These two counts are what let the executor refuse that.
 */
class ClaudeErrorCountTest {

    /**
     * A tracker backed by an in-memory store.
     *
     * <p>An earlier version of this test passed nulls, on the assumption that only the counting
     * paths were exercised. They are not: every mark* call persists the tracker file, so the nulls
     * produced a NullPointerException. An in-memory store keeps this a unit test without pretending
     * the persistence is not there.
     */
    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path tmp;

    private ClaudeEnrichmentTracker tracker() {
        // The base dir must end in /jar with the store rooted at its parent, or JarDataPaths
        // refuses to construct — its guard exists so the Path accessors and the ArtifactStore
        // cannot address different locations. Same arrangement as PersistenceServiceCacheTest.
        var store = new com.analyzer.storage.LocalArtifactStore(tmp);
        return new ClaudeEnrichmentTracker(
                new JarDataPaths(tmp.resolve("jar").toString(), store), store);
    }

    @Test
    @DisplayName("counts only the endpoints that errored")
    void countsErrors() {
        var t = tracker();
        t.startTracking("app.jar", List.of("A.a", "B.b", "C.c", "D.d"));

        t.markEndpointComplete("app.jar", "A.a");
        t.markEndpointError("app.jar", "B.b", "rate limited");
        t.markEndpointError("app.jar", "C.c", "timeout");
        // D.d left PENDING — not an error, and must not be counted as one.

        assertEquals(2, t.errorCount("app.jar"));
        assertEquals(4, t.trackedCount("app.jar"));
    }

    @Test
    @DisplayName("all-failed is distinguishable from all-succeeded")
    void allFailedIsVisible() {
        var t = tracker();
        t.startTracking("app.jar", List.of("A.a", "B.b"));
        t.markEndpointError("app.jar", "A.a", "429");
        t.markEndpointError("app.jar", "B.b", "429");

        // Asserting equality alone would pass on 0 == 0 — which is exactly what happened while the
        // key-normalisation bug made both return zero. Pin the values.
        assertEquals(2, t.trackedCount("app.jar"));
        assertEquals(2, t.errorCount("app.jar"),
                "errors == tracked is the condition the executor refuses to relabel on");
    }

    @Test
    @DisplayName("an untracked jar reports zero rather than throwing")
    void untrackedIsZero() {
        var t = tracker();
        assertEquals(0, t.errorCount("never-seen.jar"));
        assertEquals(0, t.trackedCount("never-seen.jar"),
                "0 tracked also stops the all-failed guard firing on an empty run");
    }
}
