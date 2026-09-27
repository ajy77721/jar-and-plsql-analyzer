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

import static org.junit.jupiter.api.Assertions.*;

/**
 * The disk cache's own behaviours: mirroring on a miss, invalidation on a write/delete, listing
 * invalidation when a child appears or disappears, revalidation of an aged entry, and the size cap.
 *
 * <p>The full {@link ArtifactStore} contract is separately re-run against the cache by
 * {@link CachingArtifactStoreConformanceTest}; this class covers the parts that contract cannot see
 * — namely that there is a cache at all, and that it is dropped at the right moments.
 */
class CachingArtifactStoreTest {

    @TempDir Path tmp;

    private CachingArtifactStore cache(ArtifactStore delegate) {
        return new CachingArtifactStore("test", delegate, tmp.resolve("cache").resolve("store"),
                64L * 1024 * 1024);
    }

    private static void put(ArtifactStore store, ArtifactKey key, String content) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String get(ArtifactStore store, ArtifactKey key) throws IOException {
        try (InputStream in = store.read(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void readMissMirrorsToDiskAndSecondReadServesLocally() throws IOException {
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        CachingArtifactStore store = cache(backend);
        ArtifactKey k = ArtifactKey.of("jar", "demo", "analysis.json");
        put(store, k, "{\"a\":1}");

        Path mirror = store.cacheDir().resolve("jar").resolve("demo").resolve("analysis.json");
        assertFalse(Files.exists(mirror), "a write must not populate the cache");

        assertEquals("{\"a\":1}", get(store, k));
        assertTrue(Files.exists(mirror), "first read must mirror the artifact to disk");

        // Prove the second read is served from disk, not the backend: remove it from the backend.
        backend.delete(k);
        assertEquals("{\"a\":1}", get(store, k), "second read should be served from the cache");
    }

    @Test
    void writeInvalidatesTheCachedEntrySoNextReadRefetches() throws IOException {
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        CachingArtifactStore store = cache(backend);
        ArtifactKey k = ArtifactKey.of("jar", "demo", "analysis.json");

        put(store, k, "v1");
        assertEquals("v1", get(store, k));
        Path mirror = store.cacheDir().resolve("jar").resolve("demo").resolve("analysis.json");
        assertTrue(Files.exists(mirror));

        put(store, k, "v2");
        assertFalse(Files.exists(mirror), "a write must drop the cached entry");

        assertEquals("v2", get(store, k), "the next read must observe the new content");
    }

    @Test
    void deleteInvalidatesEntryAndMetadata() throws IOException {
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        CachingArtifactStore store = cache(backend);
        ArtifactKey k = ArtifactKey.of("a.json");
        put(store, k, "x");
        assertEquals("x", get(store, k));

        assertTrue(store.delete(k));
        assertFalse(store.exists(k));
        assertFalse(Files.exists(store.cacheDir().resolve("a.json")));
    }

    @Test
    void writingAChildInvalidatesTheParentsListing() throws IOException {
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        CachingArtifactStore store = cache(backend);
        ArtifactKey dir = ArtifactKey.of("jar", "demo");

        put(store, dir.resolve("a.json"), "1");
        assertEquals(List.of("a.json"), listLeavesList(store, dir));

        // A new child appears; the cached listing for `dir` must be dropped so it shows up.
        put(store, dir.resolve("b.json"), "2");
        assertEquals(List.of("a.json", "b.json"), listLeavesList(store, dir));
    }

    @Test
    void deletingAChildInvalidatesTheParentsListing() throws IOException {
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        CachingArtifactStore store = cache(backend);
        ArtifactKey dir = ArtifactKey.of("jar", "demo");
        put(store, dir.resolve("a.json"), "1");
        put(store, dir.resolve("b.json"), "2");
        assertEquals(List.of("a.json", "b.json"), listLeavesList(store, dir));

        store.delete(dir.resolve("b.json"));
        assertEquals(List.of("a.json"), listLeavesList(store, dir));
    }

    @Test
    void agedEntryIsRevalidatedAgainstTheBackend() throws IOException {
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        // revalidateAfterMs = 0 → every read must probe, so an out-of-band edit is caught at once.
        CachingArtifactStore store = new CachingArtifactStore("test", backend,
                tmp.resolve("cache").resolve("store"), 64L * 1024 * 1024, 0);
        ArtifactKey k = ArtifactKey.of("x.json");
        put(store, k, "small");
        assertEquals("small", get(store, k));

        // Out-of-band write straight to the backend: size differs, so revalidation must refetch.
        put(backend, k, "a much longer payload");
        assertEquals("a much longer payload", get(store, k));
    }

    @Test
    void clearReportsFreedBytesAndEmptiesTheCache() throws IOException {
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        CachingArtifactStore store = cache(backend);
        put(store, ArtifactKey.of("a.json"), "12345");
        get(store, ArtifactKey.of("a.json"));

        long freed = store.clear();
        assertTrue(freed >= 5, "expected at least the artifact's bytes freed, got " + freed);
        assertTrue(DiskCacheAdmin.sizeOf(store.cacheDir()) == 0);
    }

    @Test
    void sizeCapTrimsLeastRecentlyAccessed() throws IOException {
        InMemoryArtifactStore backend = new InMemoryArtifactStore();
        // 30-byte cap: two 20-byte artifacts cannot both stay.
        CachingArtifactStore store = new CachingArtifactStore("test", backend,
                tmp.resolve("cache").resolve("store"), 30, CachingArtifactStore.DEFAULT_REVALIDATE_MS);
        String twenty = "01234567890123456789";
        ArtifactKey first = ArtifactKey.of("first.json");
        ArtifactKey second = ArtifactKey.of("second.json");
        put(store, first, twenty);
        put(store, second, twenty);
        get(store, first);
        get(store, second);

        int deleted = store.trimNow();
        assertTrue(deleted >= 1, "trim should have removed at least one artifact");
        assertTrue(DiskCacheAdmin.sizeOf(store.cacheDir()) <= 30,
                "cache must be at or below the cap after trim");
    }

    private static List<String> listLeavesList(ArtifactStore store, ArtifactKey dir) throws IOException {
        try (var s = store.list(dir, false)) {
            return s.filter(e -> !e.directory()).map(e -> e.key().leaf()).sorted().toList();
        }
    }
}
