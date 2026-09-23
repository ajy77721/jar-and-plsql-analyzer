package com.analyzer.storage;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Behavioural contract for {@link ArtifactStore}.
 *
 * <p>Written against the interface, not the implementation. When the S3 backend lands it must
 * subclass this and pass unchanged — that is the point. Anything asserted here is a promise both
 * backends make; anything not asserted is free to differ.
 */
class ArtifactStoreConformanceTest {

    @TempDir Path tmp;
    private ArtifactStore store;

    /** Override in a subclass to run the same suite against another backend. */
    protected ArtifactStore newStore(Path root) {
        return new LocalArtifactStore(root);
    }

    @BeforeEach
    void setUp() {
        store = newStore(tmp);
    }

    private void put(ArtifactKey key, String content) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String get(ArtifactKey key) throws IOException {
        try (InputStream in = store.read(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // ── round trip ──────────────────────────────────────────────────────────

    @Test
    void writeThenReadReturnsSameBytes() throws IOException {
        ArtifactKey k = ArtifactKey.of("jar", "demo", "analysis.json");
        put(k, "{\"a\":1}");
        assertEquals("{\"a\":1}", get(k));
    }

    @Test
    void writeCreatesIntermediateDirectories() throws IOException {
        put(ArtifactKey.of("a", "b", "c", "d", "e.json"), "x");
        assertTrue(store.exists(ArtifactKey.of("a", "b", "c", "d", "e.json")));
    }

    @Test
    void overwriteReplacesContent() throws IOException {
        ArtifactKey k = ArtifactKey.of("f.json");
        put(k, "first");
        put(k, "second");
        assertEquals("second", get(k));
    }

    @Test
    void emptyArtifactIsValid() throws IOException {
        ArtifactKey k = ArtifactKey.of("empty.json");
        put(k, "");
        assertTrue(store.exists(k));
        assertEquals(0, store.size(k));
        assertEquals("", get(k));
    }

    @Test
    void largeArtifactRoundTripsWithoutCorruption() throws IOException {
        // Exercises the bulk-write path; a byte-at-a-time FilterOutputStream would be unusably
        // slow here, which is exactly the regression this guards.
        ArtifactKey k = ArtifactKey.of("big.bin");
        byte[] chunk = new byte[64 * 1024];
        for (int i = 0; i < chunk.length; i++) chunk[i] = (byte) (i % 251);
        try (OutputStream out = store.write(k)) {
            for (int i = 0; i < 64; i++) out.write(chunk);      // 4 MiB
        }
        assertEquals(64L * chunk.length, store.size(k));
        try (InputStream in = store.read(k)) {
            byte[] all = in.readAllBytes();
            assertEquals(64 * chunk.length, all.length);
            assertEquals((byte) (5 % 251), all[5]);
            assertEquals((byte) ((chunk.length - 1) % 251), all[chunk.length - 1]);
        }
    }

    // ── atomicity ───────────────────────────────────────────────────────────

    @Test
    void abandonedWriteLeavesPreviousContentIntact() throws IOException {
        ArtifactKey k = ArtifactKey.of("atomic.json");
        put(k, "original");
        OutputStream out = store.write(k);
        out.write("partial".getBytes(StandardCharsets.UTF_8));
        // deliberately not closed — simulates a crash mid-write
        assertEquals("original", get(k), "reader must not observe a partial write");
    }

    @Test
    void abandonedWriteOfNewKeyLeavesNothingVisible() throws IOException {
        ArtifactKey k = ArtifactKey.of("never-committed.json");
        OutputStream out = store.write(k);
        out.write("partial".getBytes(StandardCharsets.UTF_8));
        assertFalse(store.exists(k), "uncommitted write must not be visible");
    }

    @Test
    void closeIsIdempotent() throws IOException {
        ArtifactKey k = ArtifactKey.of("twice.json");
        OutputStream out = store.write(k);
        out.write("v".getBytes(StandardCharsets.UTF_8));
        out.close();
        assertDoesNotThrow(out::close);
        assertEquals("v", get(k));
    }

    // ── ranged reads ────────────────────────────────────────────────────────

    @Test
    void rangedReadReturnsRequestedWindow() throws IOException {
        ArtifactKey k = ArtifactKey.of("range.txt");
        put(k, "0123456789");
        try (InputStream in = store.read(k, 3, 4)) {
            assertEquals("3456", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void rangedReadToEndOfObject() throws IOException {
        ArtifactKey k = ArtifactKey.of("range2.txt");
        put(k, "0123456789");
        try (InputStream in = store.read(k, 7, -1)) {
            assertEquals("789", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void rangeLongerThanObjectIsClamped() throws IOException {
        ArtifactKey k = ArtifactKey.of("range3.txt");
        put(k, "abc");
        try (InputStream in = store.read(k, 1, 1000)) {
            assertEquals("bc", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void negativeOffsetIsRejected() throws IOException {
        ArtifactKey k = ArtifactKey.of("range4.txt");
        put(k, "abc");
        assertThrows(IllegalArgumentException.class, () -> store.read(k, -1, 1));
    }

    // ── metadata ────────────────────────────────────────────────────────────

    @Test
    void absentArtifactReportsConsistently() {
        ArtifactKey missing = ArtifactKey.of("nope.json");
        assertFalse(store.exists(missing));
        assertEquals(-1, store.size(missing));
        assertEquals(-1, store.lastModified(missing));
        assertFalse(store.delete(missing));
    }

    @Test
    void sizeAndLastModifiedArePopulated() throws IOException {
        ArtifactKey k = ArtifactKey.of("m.json");
        put(k, "12345");
        assertEquals(5, store.size(k));
        assertTrue(store.lastModified(k) > 0);
    }

    // ── delete / list ───────────────────────────────────────────────────────

    @Test
    void deleteRemovesArtifact() throws IOException {
        ArtifactKey k = ArtifactKey.of("d.json");
        put(k, "x");
        assertTrue(store.delete(k));
        assertFalse(store.exists(k));
    }

    @Test
    void deletePrefixRemovesSubtreeAndCounts() throws IOException {
        put(ArtifactKey.of("jar", "x", "a.json"), "1");
        put(ArtifactKey.of("jar", "x", "sub", "b.json"), "2");
        put(ArtifactKey.of("jar", "y", "c.json"), "3");

        int removed = store.deletePrefix(ArtifactKey.of("jar", "x"));
        assertTrue(removed >= 2, "expected at least the 2 files, got " + removed);
        assertFalse(store.exists(ArtifactKey.of("jar", "x", "a.json")));
        assertTrue(store.exists(ArtifactKey.of("jar", "y", "c.json")), "sibling must survive");
    }

    @Test
    void deletePrefixOnMissingPrefixIsZeroNotError() throws IOException {
        assertEquals(0, store.deletePrefix(ArtifactKey.of("ghost")));
    }

    @Test
    void listNonRecursiveReturnsImmediateChildren() throws IOException {
        put(ArtifactKey.of("p", "a.json"), "1");
        put(ArtifactKey.of("p", "sub", "b.json"), "2");
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("p"), false)) {
            List<String> leaves = s.map(e -> e.key().leaf()).sorted().toList();
            assertEquals(List.of("a.json", "sub"), leaves);
        }
    }

    @Test
    void listRecursiveReachesNestedArtifacts() throws IOException {
        put(ArtifactKey.of("q", "a.json"), "1");
        put(ArtifactKey.of("q", "sub", "b.json"), "2");
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("q"), true)) {
            List<String> leaves = s.filter(e -> !e.directory()).map(e -> e.key().leaf()).sorted().toList();
            assertEquals(List.of("a.json", "b.json"), leaves);
        }
    }

    @Test
    void listMissingPrefixIsEmptyNotError() throws IOException {
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("ghost"), true)) {
            assertEquals(0, s.count());
        }
    }

    // ── copy / move ─────────────────────────────────────────────────────────

    @Test
    void copyDuplicatesLeavingSourceInPlace() throws IOException {
        ArtifactKey from = ArtifactKey.of("c1.json");
        ArtifactKey to = ArtifactKey.of("nested", "c2.json");
        put(from, "payload");
        store.copy(from, to);
        assertEquals("payload", get(to));
        assertTrue(store.exists(from));
    }

    @Test
    void moveRelocatesAndRemovesSource() throws IOException {
        ArtifactKey from = ArtifactKey.of("m1.json");
        ArtifactKey to = ArtifactKey.of("nested", "m2.json");
        put(from, "payload");
        store.move(from, to);
        assertEquals("payload", get(to));
        assertFalse(store.exists(from));
    }

    // ── materialize ─────────────────────────────────────────────────────────

    @Test
    void materializeYieldsReadableLocalFile() throws IOException {
        ArtifactKey k = ArtifactKey.of("mat.json");
        put(k, "content");
        try (LocalHandle h = store.materialize(k)) {
            assertTrue(Files.exists(h.path()));
            assertEquals("content", Files.readString(h.path()));
        }
    }

    @Test
    void materializeOfMissingArtifactThrows() {
        assertThrows(IOException.class, () -> store.materialize(ArtifactKey.of("absent.json")));
    }

    @Test
    void materializedArtifactSurvivesCloseWhenNotTemporary() throws IOException {
        // Local backend hands back the real file, so close() must NOT delete it. Getting this
        // wrong would delete live analysis data.
        ArtifactKey k = ArtifactKey.of("keep.json");
        put(k, "content");
        LocalHandle h = store.materialize(k);
        boolean temporary = h.temporary();
        h.close();
        if (!temporary) {
            assertTrue(store.exists(k), "close() deleted a non-temporary artifact");
        }
    }
}
