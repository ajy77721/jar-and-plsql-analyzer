package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The analysis-level idempotency registry: skip semantics, per-analysis scoping (NEVER global),
 * corrupt fail-safe, and concurrent-crawl safety.
 */
@DisplayName("AnalysisRegistry")
class AnalysisRegistryTest {

    private static final ArtifactKey OUT_A = ArtifactKey.of("plsql-parse", "ORACLE", "PROJ", "ENV", "A");
    private static final ArtifactKey OUT_B = ArtifactKey.of("plsql-parse", "ORACLE", "PROJ", "ENV", "B");

    @AfterEach
    void reset() {
        AnalysisRegistry.resetCommittedForTest(OUT_A);
        AnalysisRegistry.resetCommittedForTest(OUT_B);
    }

    @Test
    @DisplayName("record + save + reload: a complete, unchanged object is skippable")
    void roundTripSkip() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        AnalysisRegistry reg = new AnalysisRegistry(store, OUT_A);
        reg.record("PKG.SUB", "HR", "SUB", "PROCEDURE", "2026-01-01 00:00:00", 1, "sub.json");
        reg.save();

        // Fresh registry (drop the in-JVM snapshot) reads the persisted file.
        AnalysisRegistry.resetCommittedForTest(OUT_A);
        AnalysisRegistry reloaded = new AnalysisRegistry(store, OUT_A);
        assertTrue(reloaded.isSkippable("PKG.SUB", "2026-01-01 00:00:00"),
                "unchanged + complete => skip");
    }

    @Test
    @DisplayName("LAST_DDL change => NOT skippable (re-analyse)")
    void ddlChangeForcesReanalysis() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        AnalysisRegistry reg = new AnalysisRegistry(store, OUT_A);
        reg.record("PKG.SUB", "HR", "SUB", "PROCEDURE", "2026-01-01 00:00:00", 1, "sub.json");

        assertFalse(reg.isSkippable("PKG.SUB", "2026-02-02 00:00:00"),
                "a changed LAST_DDL must force a re-analysis");
        assertTrue(reg.isSkippable("PKG.SUB", "2026-01-01 00:00:00"));
    }

    @Test
    @DisplayName("missing DDL on either side => reuse (mirrors the source cache)")
    void missingDdlStillReuses() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        AnalysisRegistry reg = new AnalysisRegistry(store, OUT_A);
        reg.record("PKG.SUB", "HR", "SUB", "PROCEDURE", null, 1, "sub.json");
        assertTrue(reg.isSkippable("PKG.SUB", null));
        assertTrue(reg.isSkippable("PKG.SUB", "2026-01-01 00:00:00"));
    }

    @Test
    @DisplayName("partial run (0 nodes) => NOT skippable (never trust an incomplete run)")
    void partialRunNotSkippable() {
        AnalysisRegistry.Entry e = new AnalysisRegistry.Entry();
        e.identity = "PKG.SUB";
        e.nodesProduced = 0;
        e.lastDdl = "2026-01-01 00:00:00";
        e.nodeCacheLeaf = "sub.json";
        AnalysisRegistry reg = new AnalysisRegistry(new InMemoryArtifactStore(), OUT_A);
        assertFalse(reg.isCompleteAndUnchanged(e, "2026-01-01 00:00:00"));
    }

    @Test
    @DisplayName("no restorable node-cache leaf => NOT skippable")
    void noLeafNotSkippable() {
        AnalysisRegistry.Entry e = new AnalysisRegistry.Entry();
        e.identity = "PKG.SUB";
        e.nodesProduced = 1;
        e.lastDdl = "2026-01-01 00:00:00";
        e.nodeCacheLeaf = null;
        AnalysisRegistry reg = new AnalysisRegistry(new InMemoryArtifactStore(), OUT_A);
        assertFalse(reg.isCompleteAndUnchanged(e, "2026-01-01 00:00:00"));
    }

    @Test
    @DisplayName("corrupt registry => loads empty => everything re-analysed (fail-safe)")
    void corruptRegistryIsFailSafe() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        try (OutputStream out = store.write(OUT_A.resolve(".analysis-registry.json"))) {
            out.write("{ this is not valid json".getBytes(StandardCharsets.UTF_8));
        }
        AnalysisRegistry reg = new AnalysisRegistry(store, OUT_A);
        assertFalse(reg.isSkippable("PKG.SUB", "2026-01-01 00:00:00"));
    }

    @Test
    @DisplayName("PER-ANALYSIS SCOPE: two analyses/users have fully independent registries")
    void registriesAreIndependentPerScope() {
        InMemoryArtifactStore storeA = new InMemoryArtifactStore();
        InMemoryArtifactStore storeB = new InMemoryArtifactStore();

        AnalysisRegistry userA = new AnalysisRegistry(storeA, OUT_A);
        userA.record("PKG.SUB", "HR", "SUB", "PROCEDURE", "T1", 1, "sub.json");
        userA.save();

        // A different analysis scope (different store + different outputKey) must NOT skip based
        // on another scope's registry — there is no global registry to leak through.
        AnalysisRegistry userB = new AnalysisRegistry(storeB, OUT_B);
        assertFalse(userB.isSkippable("PKG.SUB", "T1"),
                "user B must not skip based on user A's registry");
        assertNull(userB.entryFor("PKG.SUB"));

        // Even on the SAME store, a different outputKey is a different registry file.
        AnalysisRegistry otherScope = new AnalysisRegistry(storeA, OUT_B);
        assertFalse(otherScope.isSkippable("PKG.SUB", "T1"));
    }

    @Test
    @DisplayName("registry file lives under the analysis outputKey (no global location)")
    void registryLivesUnderOutputKey() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        AnalysisRegistry reg = new AnalysisRegistry(store, OUT_A);
        assertEquals(OUT_A.resolve(".analysis-registry.json"), reg.fileKey());
        assertTrue(reg.fileKey().toString().startsWith(OUT_A.toString()),
                "the registry must live under the per-analysis scope, not a shared root");
    }

    @Test
    @DisplayName("concurrent crawls of one scope do not clobber each other's entries")
    void concurrentCommitsMerge() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        AnalysisRegistry.resetCommittedForTest(OUT_A);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < 2; i++) {
            final int n = i;
            pool.submit(() -> {
                try {
                    start.await();
                    AnalysisRegistry reg = new AnalysisRegistry(store, OUT_A);
                    reg.record("PKG.SUB" + n, "HR", "SUB" + n, "PROCEDURE", "T", 1, "leaf" + n + ".json");
                    reg.save();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        AnalysisRegistry.resetCommittedForTest(OUT_A);
        AnalysisRegistry merged = new AnalysisRegistry(store, OUT_A);
        assertTrue(merged.isSkippable("PKG.SUB0", "T"), "first crawl's entry must survive");
        assertTrue(merged.isSkippable("PKG.SUB1", "T"), "second crawl's entry must survive");
    }
}
