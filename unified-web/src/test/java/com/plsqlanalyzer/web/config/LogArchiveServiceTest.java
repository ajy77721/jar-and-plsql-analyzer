package com.plsqlanalyzer.web.config;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Rolled logs must reach the store and leave the local disk; the live log must not be touched.
 *
 * <p>Uses {@link InMemoryArtifactStore} rather than the local backend deliberately: with a local
 * store the archive key would resolve back into the same {@code logs/} tree, so a bug that uploaded
 * a file onto itself would still look like a pass.
 */
class LogArchiveServiceTest {

    @TempDir
    Path dataDir;

    private Path logs() throws Exception {
        Path l = dataDir.resolve("logs");
        Files.createDirectories(l);
        return l;
    }

    private LogArchiveService service(com.analyzer.storage.ArtifactStore store) throws Exception {
        // retain-days 0 disables pruning: these tests are about the shipping pass, and a
        // retention window would delete the very archives they assert on.
        return new LogArchiveService(store, logs().toString(), dataDir.toString(),
                "unified-analyzer.log", 0, 5);
    }

    /** The sweep skips a file the first time it sees it, so two passes settle a stable file. */
    private int settleAndRun(LogArchiveService s) {
        s.run();
        return s.run();
    }

    @Test
    @DisplayName("a rolled log is uploaded and deleted locally")
    void rolledLogIsShippedAndRemoved() throws Exception {
        var store = new InMemoryArtifactStore();
        Path rolled = logs().resolve("unified-analyzer.2026-09-02.0.log.gz");
        Files.write(rolled, "rolled-content".getBytes(StandardCharsets.UTF_8));

        assertEquals(1, settleAndRun(service(store)));

        ArtifactKey key = ArtifactKey.of("logs", "archive", rolled.getFileName().toString());
        assertTrue(store.exists(key), "the archive must be in the store");
        assertEquals("rolled-content",
                new String(store.read(key).readAllBytes(), StandardCharsets.UTF_8),
                "content must survive the trip byte-for-byte");
        assertFalse(Files.exists(rolled), "the local copy must be gone");
    }

    @Test
    @DisplayName("the live log file is never uploaded or deleted")
    void activeLogIsLeftAlone() throws Exception {
        var store = new InMemoryArtifactStore();
        Path active = logs().resolve("unified-analyzer.log");
        Files.write(active, "still being written".getBytes(StandardCharsets.UTF_8));

        assertEquals(0, settleAndRun(service(store)));

        assertTrue(Files.exists(active),
                "logback holds this open; deleting it would send logs to an unlinked inode");
        assertEquals(0, store.objectCount());
    }

    @Test
    @DisplayName("a file still growing is left until its size settles")
    void growingFileIsSkipped() throws Exception {
        var store = new InMemoryArtifactStore();
        Path rolled = logs().resolve("unified-analyzer.2026-09-02.1.log.gz");
        Files.write(rolled, "partial".getBytes(StandardCharsets.UTF_8));

        LogArchiveService s = service(store);
        assertEquals(0, s.run(), "first sighting must never ship");

        // logback compresses asynchronously, so the file can grow after it appears.
        Files.write(rolled, "partial-plus-more".getBytes(StandardCharsets.UTF_8));
        assertEquals(0, s.run(), "a changed size means it is still being written");
        assertTrue(Files.exists(rolled));

        assertEquals(1, s.run(), "stable size: now it ships");
        assertFalse(Files.exists(rolled));
    }

    @Test
    @DisplayName("the archive prefix is distinct from the live log directory")
    void archiveDoesNotCollideWithLiveLogs() throws Exception {
        var store = new InMemoryArtifactStore();
        Path rolled = logs().resolve("unified-analyzer.2026-09-02.2.log.gz");
        Files.write(rolled, "x".getBytes(StandardCharsets.UTF_8));
        settleAndRun(service(store));

        // logs/<name> would be the file itself under a local backend, so the upload-then-delete
        // would destroy what it had just written. The archive must sit somewhere else.
        assertTrue(store.keys().stream().allMatch(k -> k.startsWith("logs/archive/")),
                "archives must not be keyed at logs/<name>: " + store.keys());
    }

    @Test
    @DisplayName("a failed upload keeps the local file so nothing is lost")
    void failedUploadKeepsTheLocalCopy() throws Exception {
        // A store that accepts the write but reports the wrong size, standing in for a truncated
        // or interrupted upload. The service must notice and refuse to delete.
        var delegate = new InMemoryArtifactStore();
        var store = new com.analyzer.storage.ArtifactStore() {
            public java.io.OutputStream write(ArtifactKey k) throws java.io.IOException { return delegate.write(k); }
            public java.io.OutputStream write(ArtifactKey k, long n) throws java.io.IOException { return delegate.write(k, n); }
            public java.io.InputStream read(ArtifactKey k) throws java.io.IOException { return delegate.read(k); }
            public java.io.InputStream read(ArtifactKey k, long o, long n) throws java.io.IOException { return delegate.read(k, o, n); }
            public long size(ArtifactKey k) { return 1; }          // never matches the real file
            public long lastModified(ArtifactKey k) { return delegate.lastModified(k); }
            public boolean exists(ArtifactKey k) { return delegate.exists(k); }
            public boolean delete(ArtifactKey k) { return delegate.delete(k); }
            public int deletePrefix(ArtifactKey k) throws java.io.IOException { return delegate.deletePrefix(k); }
            public java.util.stream.Stream<com.analyzer.storage.ArtifactEntry> list(ArtifactKey k, boolean r) throws java.io.IOException { return delegate.list(k, r); }
            public void copy(ArtifactKey a, ArtifactKey b) throws java.io.IOException { delegate.copy(a, b); }
            public void move(ArtifactKey a, ArtifactKey b) throws java.io.IOException { delegate.move(a, b); }
            public com.analyzer.storage.LocalHandle materialize(ArtifactKey k) throws java.io.IOException { return delegate.materialize(k); }
        };
        Path rolled = logs().resolve("unified-analyzer.2026-09-02.3.log.gz");
        Files.write(rolled, "important log data".getBytes(StandardCharsets.UTF_8));

        assertEquals(0, settleAndRun(service(store)));
        assertTrue(Files.exists(rolled),
                "a partial upload must never be followed by a local delete");
    }

    @Test
    @DisplayName("a missing log directory is not an error")
    void missingLogDirectoryIsHarmless() {
        var store = new InMemoryArtifactStore();
        var s = new LogArchiveService(store, dataDir.resolve("nope").toString(),
                dataDir.toString(), "unified-analyzer.log", 0, 5);
        assertEquals(0, s.run());
    }
    /* ---- retention: shipped is not the same as kept forever ---- */

    @Test
    @DisplayName("archives older than the window are pruned")
    void oldArchivesArePruned() throws Exception {
        var store = new InMemoryArtifactStore();
        // 10 archives, all 40 days old. Local rolled logs were always bounded by logback, but these
        // shipped copies were never removed and grew for the life of the deployment.
        long old = System.currentTimeMillis() - 40L * 86_400_000L;
        for (int i = 0; i < 10; i++) seedArchive(store, "app-" + i + ".log.gz", old + i * 1000L);

        var s = new LogArchiveService(store, logs().toString(), dataDir.toString(),
                "unified-analyzer.log", 14, 5);
        int pruned = s.pruneOldArchives();

        assertEquals(5, pruned, "10 archives, all past the window, floor of 5 kept");
        assertEquals(5, countArchives(store), "the newest 5 must survive on the floor alone");
    }

    @Test
    @DisplayName("recent archives are never pruned")
    void recentArchivesSurvive() throws Exception {
        var store = new InMemoryArtifactStore();
        long recent = System.currentTimeMillis() - 2L * 86_400_000L;
        for (int i = 0; i < 8; i++) seedArchive(store, "app-" + i + ".log.gz", recent + i * 1000L);

        var s = new LogArchiveService(store, logs().toString(), dataDir.toString(),
                "unified-analyzer.log", 14, 5);
        assertEquals(0, s.pruneOldArchives(), "nothing is past a 14-day window");
        assertEquals(8, countArchives(store));
    }

    @Test
    @DisplayName("the floor protects the last few archives on a long-quiet system")
    void floorProtectsTheNewest() throws Exception {
        var store = new InMemoryArtifactStore();
        // Everything is ancient. Age alone would empty the archive — which is exactly when the last
        // few logs matter most.
        long ancient = System.currentTimeMillis() - 400L * 86_400_000L;
        for (int i = 0; i < 3; i++) seedArchive(store, "app-" + i + ".log.gz", ancient + i * 1000L);

        var s = new LogArchiveService(store, logs().toString(), dataDir.toString(),
                "unified-analyzer.log", 14, 5);
        assertEquals(0, s.pruneOldArchives());
        assertEquals(3, countArchives(store), "fewer archives than the floor: keep them all");
    }

    @Test
    @DisplayName("retain-days=0 disables pruning entirely")
    void zeroDaysKeepsEverything() throws Exception {
        var store = new InMemoryArtifactStore();
        long ancient = System.currentTimeMillis() - 400L * 86_400_000L;
        for (int i = 0; i < 9; i++) seedArchive(store, "app-" + i + ".log.gz", ancient);

        var s = new LogArchiveService(store, logs().toString(), dataDir.toString(),
                "unified-analyzer.log", 0, 5);
        assertEquals(0, s.pruneOldArchives(), "0 means keep everything");
        assertEquals(9, countArchives(store));
    }
    /* ---- helpers for the retention tests ---- */

    private static final com.analyzer.storage.ArtifactKey ARCHIVE =
            com.analyzer.storage.ArtifactKey.of("logs", "archive");

    /** Writes an archived log and backdates it, so a retention window can be exercised. */
    private void seedArchive(InMemoryArtifactStore store, String name, long whenMillis)
            throws Exception {
        var key = ARCHIVE.resolve(name);
        try (var out = store.write(key)) { out.write(("log " + name).getBytes()); }
        assertTrue(store.setLastModifiedForTest(key, whenMillis),
                "backdating failed — the object was not written");
    }

    private int countArchives(InMemoryArtifactStore store) throws Exception {
        try (var s = store.list(ARCHIVE, false)) {
            return (int) s.filter(e -> !e.directory()).count();
        }
    }
}
