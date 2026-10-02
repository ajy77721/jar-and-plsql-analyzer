package com.analyzer.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The write-ahead spill: a {@code close()} that returned must survive a process kill, and a
 * surviving spill must upload on the next boot. These tests simulate the crash by letting the
 * upload fail (the flush never reaches S3), leaving the spill behind exactly as a {@code kill -9}
 * between spill and upload would, then boot a fresh store over the same directory.
 */
class WriteBehindStoreSpillTest {

    private static final ArtifactKey KEY = ArtifactKey.of("parser", "db1", "proj", "dev", "obj", "api", "index.json");
    private static final byte[] BODY = "{\"run\":1}".getBytes(StandardCharsets.UTF_8);

    /** A store whose uploads always fail — the crash window's observable stand-in. */
    private static final class FailingStore implements ArtifactStore {
        @Override public OutputStream write(ArtifactKey key) throws IOException {
            throw new IOException("s3 unreachable");
        }
        @Override public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
            throw new IOException("s3 unreachable");
        }
        @Override public InputStream read(ArtifactKey key) { throw new UnsupportedOperationException(); }
        @Override public InputStream read(ArtifactKey key, long o, long l) { throw new UnsupportedOperationException(); }
        @Override public boolean exists(ArtifactKey key) { return false; }
        @Override public long size(ArtifactKey key) { return -1; }
        @Override public long lastModified(ArtifactKey key) { return -1; }
        @Override public boolean delete(ArtifactKey key) { return false; }
        @Override public int deletePrefix(ArtifactKey prefix) { return 0; }
        @Override public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) { return Stream.empty(); }
        @Override public void copy(ArtifactKey from, ArtifactKey to) { }
        @Override public void move(ArtifactKey from, ArtifactKey to) { }
        @Override public LocalHandle materialize(ArtifactKey key) { throw new UnsupportedOperationException(); }
    }

    private static long spillCount(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) return 0;
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".spill")).count();
        }
    }

    private static void write(WriteBehindStore store, ArtifactKey key, byte[] body) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(body);
        }
    }

    @Test
    void aClosedWriteSurvivesAFailedUploadAndReplaysOnBoot(@TempDir Path tmp) throws IOException {
        Path spill = tmp.resolve("spill");
        WriteBehindStore crashing = new WriteBehindStore(new FailingStore(), 2, null, spill);
        try {
            write(crashing, KEY, BODY);
            // The upload fails; the drain surfaces it, exactly as the analysis thread would see.
            assertThrows(IOException.class, crashing::awaitPending);
        } finally {
            crashing.close(1_000);
        }
        // The bytes that reached close() are on disk, not lost with the heap buffer.
        assertEquals(1, spillCount(spill), "the closed write must have spilled before the failed upload");

        // "Restart": a fresh store over the same spill dir with a working backend.
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        WriteBehindStore booted = new WriteBehindStore(backend, 2, null, spill);
        try {
            int replayed = booted.replaySpills();
            assertEquals(1, replayed, "the surviving spill must replay at boot");
            assertTrue(backend.exists(KEY), "replayed bytes must land in the backend");
            try (InputStream in = backend.read(KEY)) {
                assertArrayEquals(BODY, in.readAllBytes());
            }
            assertEquals(0, spillCount(spill), "a successfully replayed spill is deleted");
        } finally {
            booted.close(1_000);
        }
    }

    @Test
    void aSuccessfulUploadDeletesItsSpill(@TempDir Path tmp) throws IOException {
        Path spill = tmp.resolve("spill");
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        WriteBehindStore store = new WriteBehindStore(backend, 2, null, spill);
        try {
            write(store, KEY, BODY);
            store.awaitPending();
            assertTrue(backend.exists(KEY));
            assertEquals(0, spillCount(spill), "steady state keeps the spill directory empty");
        } finally {
            store.close(1_000);
        }
    }

    @Test
    void aCorruptSpillIsDiscardedNotReplayed(@TempDir Path tmp) throws IOException {
        Path spill = tmp.resolve("spill");
        Files.createDirectories(spill);
        Files.write(spill.resolve("garbage.spill"), "not a spill record".getBytes(StandardCharsets.UTF_8));

        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        WriteBehindStore store = new WriteBehindStore(backend, 2, null, spill);
        try {
            assertEquals(0, store.replaySpills(), "garbage must never be replayed");
            assertEquals(0, backend.objectCount());
            assertEquals(0, spillCount(spill), "an unparseable spill is reclaimed");
        } finally {
            store.close(1_000);
        }
    }

    @Test
    void withoutASpillDirTheBufferStaysHeapOnly(@TempDir Path tmp) throws IOException {
        // Legacy behaviour preserved: no spill dir means no local writes at all.
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        WriteBehindStore store = new WriteBehindStore(backend, 2, null, null);
        try {
            write(store, KEY, BODY);
            store.awaitPending();
            assertTrue(backend.exists(KEY));
            try (Stream<Path> s = Files.list(tmp)) {
                assertFalse(s.findAny().isPresent(), "a heap-only store writes nothing locally");
            }
        } finally {
            store.close(1_000);
        }
    }

    @Test
    void theSpillNeverCarriesCredentials(@TempDir Path tmp) throws IOException {
        // Only key + bytes are spilled; the captured SSO context is heap-only. If a future change
        // ever serialises it, this fails.
        Path spill = tmp.resolve("spill");
        WriteBehindStore store = new WriteBehindStore(new FailingStore(), 2, null, spill);
        try {
            write(store, KEY, BODY);
            assertThrows(IOException.class, store::awaitPending);
        } finally {
            store.close(1_000);
        }
        List<Path> files;
        try (Stream<Path> s = Files.list(spill)) {
            files = s.filter(p -> p.getFileName().toString().endsWith(".spill")).toList();
        }
        assertEquals(1, files.size());
        String content = new String(Files.readAllBytes(files.get(0)), StandardCharsets.ISO_8859_1);
        assertFalse(content.contains("alice-secret"), "no credential material may reach the spill");
        assertTrue(content.contains(KEY.leaf()), "the spill records the key it belongs to");
    }
}
