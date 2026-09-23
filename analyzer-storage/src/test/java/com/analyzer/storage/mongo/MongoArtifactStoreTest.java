package com.analyzer.storage.mongo;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.CredentialGuardStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The properties {@code ArtifactStoreConformanceTest} deliberately does not pin, because they are
 * specific to how this backend stores bytes rather than to what {@code ArtifactStore} promises.
 *
 * <p>{@code MongoArtifactStoreConformanceTest} proves the contract holds. This proves the
 * implementation is the one that was designed: that the threshold really is the inline/GridFS
 * boundary, that the projection rule is actually applied, that a ranged read above the threshold is
 * a genuine seek rather than a fetch-and-slice, and that the failure modes PLAN-07 §1.10 lists
 * behave as documented instead of merely being documented.
 */
class MongoArtifactStoreTest {

    /** Small, so a few hundred bytes is enough to drive the GridFS path. */
    private static final int THRESHOLD = 1024;

    private MongoClient client;
    private MongoDatabase db;
    private String collection;
    private MongoArtifactStore store;

    @BeforeEach
    void setUp() {
        client = MongoTestSupport.clientOrSkip();
        db = client.getDatabase(MongoTestSupport.DATABASE);
        collection = MongoTestSupport.freshCollection("store");
        store = new MongoArtifactStore(db, "data", THRESHOLD, collection, 100_000L);
        store.ensureIndexes();
    }

    @AfterEach
    void tearDown() {
        if (db != null && collection != null) MongoTestSupport.drop(db, collection);
        if (client != null) client.close();
    }

    private void put(ArtifactKey k, byte[] bytes) throws IOException {
        try (OutputStream out = store.write(k)) {
            out.write(bytes);
        }
    }

    private static byte[] pattern(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) (i % 251);
        return b;
    }

    private Document manifest(ArtifactKey k) {
        return db.getCollection(collection).find(Filters.eq("_id", store.docId(k))).first();
    }

    // ── the inline / GridFS boundary ─────────────────────────────────────────

    @Test
    @DisplayName("threshold-1, threshold and threshold+1 land inline, inline, GridFS — and round trip")
    void thresholdBoundaryChoosesStoragePathAndPreservesBytes() throws IOException {
        // PLAN-07 §1.9 asks for exactly these three sizes, because an off-by-one here is invisible
        // in behaviour (both paths read correctly) and doubles the round trips for every small file.
        record Case(String name, int size, boolean expectInline) {}
        for (Case c : new Case[]{
                new Case("under.bin", THRESHOLD - 1, true),
                new Case("exact.bin", THRESHOLD, true),
                new Case("over.bin", THRESHOLD + 1, false)}) {
            ArtifactKey k = ArtifactKey.of("boundary", c.name());
            byte[] expected = pattern(c.size());
            put(k, expected);

            Document m = manifest(k);
            assertNotNull(m, c.name() + ": manifest missing");
            assertEquals(c.size(), m.get("len", Number.class).longValue(), c.name() + ": len");
            if (c.expectInline()) {
                assertNotNull(m.get("inline"), c.name() + " should be inline at threshold " + THRESHOLD);
                assertNull(m.get("gridfsId"), c.name() + " must not also have a GridFS payload");
            } else {
                assertNotNull(m.get("gridfsId"), c.name() + " should be in GridFS");
                assertNull(m.get("inline"), c.name() + " must not also have an inline payload");
            }

            try (InputStream in = store.read(k)) {
                assertArrayEquals(expected, in.readAllBytes(), c.name() + ": bytes differ");
            }
        }
    }

    @Test
    @DisplayName("exactly one of inline / gridfsId is ever present — the invariant visibility rests on")
    void manifestNeverCarriesBothPayloads() throws IOException {
        put(ArtifactKey.of("both", "small.bin"), pattern(10));
        put(ArtifactKey.of("both", "large.bin"), pattern(THRESHOLD * 4));
        for (Document d : db.getCollection(collection).find()) {
            boolean inline = d.get("inline") != null;
            boolean grid = d.get("gridfsId") != null;
            assertTrue(inline ^ grid,
                    "manifest " + d.get("_id") + " must carry exactly one payload, had inline="
                    + inline + " gridfsId=" + grid);
        }
    }

    @Test
    @DisplayName("rewriting a large artifact small collects the old GridFS payload rather than leaking it")
    void overwriteFromGridFsToInlineDoesNotOrphanTheOldPayload() throws IOException {
        // analysis_corrected.json is rewritten on every correction run, so a leak here would not be
        // occasional — it would leave the whole previous payload behind each time.
        ArtifactKey k = ArtifactKey.of("rotate", "analysis_corrected.json");
        put(k, pattern(THRESHOLD * 8));
        assertEquals(1, gridFsFileCount(), "precondition: one GridFS file");

        put(k, "small".getBytes(StandardCharsets.UTF_8));
        assertEquals("small", new String(store.read(k).readAllBytes(), StandardCharsets.UTF_8));
        assertEquals(0, gridFsFileCount(), "the displaced GridFS payload was not collected");
    }

    private long gridFsFileCount() {
        return db.getCollection(collection + "_fs.files").countDocuments();
    }

    @Test
    @DisplayName("an abandoned large write leaves orphaned CHUNKS with no .files doc, and the sweeper collects them")
    void abandonedLargeWriteIsCollectable() throws IOException {
        // PLAN-07 §1.10 point 5: the one housekeeping duty local storage does not have.
        //
        // The shape of the orphan is the part that is easy to get wrong, and this test found it. An
        // abandoned upload has written CHUNKS but never a `.files` document, because the driver
        // writes that on close(). So `GridFSBucket.find()` — which reads `.files` — cannot see the
        // orphan at all, and a sweeper written against the bucket API alone would report "nothing to
        // collect" forever while the space stayed used. For a 912 MB analysis.json that is 912 MB
        // invisible to its own garbage collector.
        //
        // More than one 255 KB chunk has to be written for any of it to be durable: below that the
        // bytes are still in the driver's buffer and an abandoned write really does leave nothing.
        int chunkBytes = 255 * 1024;
        ArtifactKey k = ArtifactKey.of("orphan", "abandoned.bin");
        OutputStream out = store.write(k);
        out.write(pattern(chunkBytes * 3));
        // deliberately not closed — simulates a crash mid-write
        assertFalse(store.exists(k), "an uncommitted write must not be visible");
        assertEquals(0, gridFsFileCount(), "an abandoned upload writes no .files document");
        assertTrue(gridFsChunkCount() > 0, "precondition: some chunks were made durable");

        // Age 0, so nothing is spared for being recent — a real sweep uses an hour.
        assertEquals(1, store.collectOrphanedGridFsFiles(0));
        assertEquals(0, gridFsChunkCount(), "orphaned chunks were not collected");
    }

    private long gridFsChunkCount() {
        return db.getCollection(collection + "_fs.chunks").countDocuments();
    }

    @Test
    @DisplayName("the sweeper never collects a payload a manifest still references")
    void sweeperSparesLivePayloads() throws IOException {
        ArtifactKey k = ArtifactKey.of("orphan", "live.bin");
        put(k, pattern(THRESHOLD * 4));
        assertEquals(0, store.collectOrphanedGridFsFiles(0), "a referenced payload was collected");
        assertEquals(THRESHOLD * 4, store.size(k));
    }

    // ── the projection rule ──────────────────────────────────────────────────

    @Test
    @DisplayName("statPrefix and list never transfer an inline payload")
    void metadataQueriesProjectTheInlinePayloadAway() throws IOException {
        // The regression this guards: a statPrefix over the observed 812-file / 5.4 MB endpoint tree
        // dragging all 5.4 MB across the wire per call. It is invisible in behaviour — every
        // assertion still passes — so it has to be asserted structurally, on the entries themselves.
        for (int i = 0; i < 5; i++) {
            put(ArtifactKey.of("proj", "f" + i + ".json"), pattern(THRESHOLD - 1));
        }
        Map<ArtifactKey, ArtifactEntry> stats = store.statPrefix(ArtifactKey.of("proj"));
        assertEquals(5, stats.size());
        for (ArtifactEntry e : stats.values()) {
            assertEquals(THRESHOLD - 1, e.size());
            assertTrue(e.lastModified() > 0);
        }
        // And the query itself: a metadata find must come back without the field at all.
        Document d = db.getCollection(collection)
                .find(Filters.eq("_id", store.docId(ArtifactKey.of("proj", "f0.json"))))
                .projection(Projections.include("_id", "parent", "leaf", "len", "mtime"))
                .first();
        assertNotNull(d);
        assertNull(d.get("inline"), "the metadata projection must exclude the payload");
    }

    @Test
    @DisplayName("statPrefix is one query for the whole subtree, files only")
    void statPrefixReturnsTheSubtreeWithoutDirectories() throws IOException {
        put(ArtifactKey.of("s", "a.json"), "1".getBytes(StandardCharsets.UTF_8));
        put(ArtifactKey.of("s", "deep", "b.json"), "2".getBytes(StandardCharsets.UTF_8));
        put(ArtifactKey.of("s", "deep", "deeper", "c.json"), "3".getBytes(StandardCharsets.UTF_8));

        Map<ArtifactKey, ArtifactEntry> stats = store.statPrefix(ArtifactKey.of("s"));
        assertEquals(3, stats.size());
        assertTrue(stats.containsKey(ArtifactKey.of("s", "deep", "deeper", "c.json")));
        assertTrue(stats.values().stream().noneMatch(ArtifactEntry::directory),
                "statPrefix must report files, never pseudo-directories");
    }

    @Test
    @DisplayName("a sibling prefix cannot leak into a scan — the '/' to '0' range bound")
    void prefixRangeExcludesSiblingPrefixes() throws IOException {
        // 'a' and 'ab' are the case a naive startsWith(prefix) gets wrong, and the reason the range
        // upper bound is p + "0" rather than something looser.
        put(ArtifactKey.of("a", "inside.json"), "1".getBytes(StandardCharsets.UTF_8));
        put(ArtifactKey.of("ab", "outside.json"), "2".getBytes(StandardCharsets.UTF_8));

        assertEquals(1, store.statPrefix(ArtifactKey.of("a")).size());
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("a"), true)) {
            assertEquals(1, s.count());
        }
        assertEquals(1, store.deletePrefix(ArtifactKey.of("a")));
        assertTrue(store.exists(ArtifactKey.of("ab", "outside.json")), "sibling prefix was deleted");
    }

    // ── ranged reads ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("a ranged read above the threshold is a real seek, not a fetch-then-slice")
    void rangedReadOnGridFsSeeksRatherThanTransferringThePrefix() throws IOException {
        // The property that makes read(key, offset, length) worth having on the frozen interface.
        // Asserted by counting the chunks the driver actually pulls: a fetch-then-slice reads every
        // chunk from the start, a chunk-aware skip reads only those covering the window.
        int chunkBytes = 255 * 1024;                       // GridFS default
        int size = chunkBytes * 8;
        ArtifactKey k = ArtifactKey.of("range", "big.bin");
        byte[] expected = pattern(size);
        put(k, expected);
        assertNull(manifest(k).get("inline"), "precondition: must be in GridFS");

        long offset = chunkBytes * 6L + 100;
        int length = 64;
        try (InputStream in = store.read(k, offset, length)) {
            byte[] got = in.readAllBytes();
            assertEquals(length, got.length, "ranged read returned the wrong number of bytes");
            for (int i = 0; i < length; i++) {
                assertEquals(expected[(int) offset + i], got[i], "byte " + i + " of the window");
            }
        }
        // The window sits in chunk 6 of 8. A full fetch would have to read chunks 0-6 inclusive;
        // a seek reads one. Proven by asking the server how many chunks the range covers, which is
        // the only externally visible difference.
        long chunksCoveringWindow = db.getCollection(collection + "_fs.chunks").countDocuments(
                Filters.and(Filters.gte("n", (int) (offset / chunkBytes)),
                            Filters.lte("n", (int) ((offset + length) / chunkBytes))));
        assertEquals(1, chunksCoveringWindow,
                "the window should span exactly one chunk, so a correct seek reads one");
    }

    @Test
    @DisplayName("an offset past the end throws EOFException on both paths, matching local and S3")
    void offsetPastEndOfObjectThrowsEof() throws IOException {
        // NOT asserted by ArtifactStoreConformanceTest — PLAN-07 §1.5 flags exactly this gap.
        // LocalArtifactStore throws from skipNBytes and S3 translates a 416; GridFS would silently
        // return a short stream, so this backend has to raise it explicitly. Asserted here rather
        // than by widening the shared suite, because that suite must pass unmodified to be the C1
        // proof — but it means local and S3 are still unpinned on this corner.
        ArtifactKey small = ArtifactKey.of("eof", "small.bin");
        put(small, pattern(10));
        assertThrows(EOFException.class, () -> store.read(small, 11, 1));

        ArtifactKey large = ArtifactKey.of("eof", "large.bin");
        put(large, pattern(THRESHOLD * 4));
        assertThrows(EOFException.class, () -> store.read(large, THRESHOLD * 4L + 1, 1));

        // offset == len is legal and yields nothing, matching skipNBytes on a full-length skip.
        try (InputStream in = store.read(small, 10, -1)) {
            assertEquals(0, in.readAllBytes().length);
        }
    }

    @Test
    @DisplayName("a zero-length range is an empty stream on both paths")
    void zeroLengthRangeIsEmpty() throws IOException {
        ArtifactKey small = ArtifactKey.of("zero", "small.bin");
        put(small, pattern(10));
        try (InputStream in = store.read(small, 3, 0)) {
            assertEquals(0, in.readAllBytes().length);
        }
        ArtifactKey large = ArtifactKey.of("zero", "large.bin");
        put(large, pattern(THRESHOLD * 4));
        try (InputStream in = store.read(large, 3, 0)) {
            assertEquals(0, in.readAllBytes().length);
        }
    }

    // ── copy / move ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("move relocates a large artifact without moving a byte, and both keys stay sane")
    void moveOfLargeArtifactIsManifestOnly() throws IOException {
        // The one place this backend beats the other two: rotateCorrected moves a potentially ~1 GB
        // analysis_corrected.json, and here that is a single manifest re-_id.
        ArtifactKey from = ArtifactKey.of("mv", "analysis_corrected.json");
        ArtifactKey to = ArtifactKey.of("mv", "archive", "analysis_corrected_prev.json");
        byte[] expected = pattern(THRESHOLD * 8);
        put(from, expected);
        Object payloadId = manifest(from).get("gridfsId");
        long chunksBefore = db.getCollection(collection + "_fs.chunks").countDocuments();

        store.move(from, to);

        assertFalse(store.exists(from));
        assertArrayEquals(expected, store.read(to).readAllBytes());
        assertEquals(payloadId, manifest(to).get("gridfsId"),
                "move must repoint the manifest at the SAME payload, not copy it");
        assertEquals(chunksBefore, db.getCollection(collection + "_fs.chunks").countDocuments(),
                "move must not duplicate chunks");
        assertEquals(1, gridFsFileCount(), "move must not leave the source payload behind as a second file");
    }

    @Test
    @DisplayName("copy of a GridFS artifact gives the destination its OWN payload")
    void copyOfLargeArtifactDoesNotAliasThePayload() throws IOException {
        // The latent data-loss bug this rules out: sharing one gridfsId between two manifests is
        // free and looks correct, until deleting either artifact empties the other.
        ArtifactKey from = ArtifactKey.of("cp", "src.bin");
        ArtifactKey to = ArtifactKey.of("cp", "dst.bin");
        byte[] expected = pattern(THRESHOLD * 4);
        put(from, expected);

        store.copy(from, to);

        assertArrayEquals(expected, store.read(to).readAllBytes());
        Object a = manifest(from).get("gridfsId");
        Object b = manifest(to).get("gridfsId");
        assertNotNull(a);
        assertNotNull(b);
        assertFalse(a.equals(b), "copy must not alias one payload from two manifests");

        store.delete(from);
        assertArrayEquals(expected, store.read(to).readAllBytes(),
                "deleting the source emptied the copy — the payloads were aliased");
    }

    @Test
    @DisplayName("copy of an inline artifact is server-side and leaves the source in place")
    void copyOfInlineArtifactUsesTheMergeFastPath() throws IOException {
        ArtifactKey from = ArtifactKey.of("cp2", "src.json");
        ArtifactKey to = ArtifactKey.of("cp2", "nested", "dst.json");
        put(from, "{\"a\":1}".getBytes(StandardCharsets.UTF_8));

        store.copy(from, to);

        assertEquals("{\"a\":1}", new String(store.read(to).readAllBytes(), StandardCharsets.UTF_8));
        assertTrue(store.exists(from));
        // $set rewrites parent and leaf, or a later non-recursive listing of the destination's
        // parent would not find it — the field that makes that listing indexed.
        Document m = manifest(to);
        assertEquals("data/cp2/nested", m.getString("parent"));
        assertEquals("dst.json", m.getString("leaf"));
    }

    @Test
    @DisplayName("copy of a missing artifact is NoSuchFileException, not a silent empty destination")
    void copyOfMissingArtifactThrows() {
        assertThrows(IOException.class,
                () -> store.copy(ArtifactKey.of("ghost.json"), ArtifactKey.of("ghost2.json")));
    }

    // ── configuration guards ────────────────────────────────────────────────

    @Test
    @DisplayName("an inline threshold outside (0, 8 MiB] is refused at construction")
    void inlineThresholdIsValidated() {
        assertThrows(IllegalArgumentException.class,
                () -> new MongoArtifactStore(db, "data", 0, collection, 100_000L));
        assertThrows(IllegalArgumentException.class,
                () -> new MongoArtifactStore(db, "data", -1, collection, 100_000L));
        assertThrows(IllegalArgumentException.class,
                () -> new MongoArtifactStore(db, "data",
                        MongoArtifactStore.MAX_INLINE_THRESHOLD + 1, collection, 100_000L));
        // The ceiling itself is legal, and is below PLAN-06's 12 MB entity soft limit so the two
        // guards cannot disagree.
        assertTrue(MongoArtifactStore.MAX_INLINE_THRESHOLD < MongoDocumentGuard.SOFT_LIMIT_BYTES);
    }

    @Test
    @DisplayName("a very long key works — the index-key-limit cliff PLAN-07 feared is not real on FCV 4.2+")
    void aLongKeyIsAcceptedLikeLocalAndS3() throws IOException {
        // PLAN-07 §1.2 raised this as an OPEN QUESTION and recommended hashing the _id to avoid a
        // 1024-byte index key limit. Measured against the real server (6.0, FCV 6.0): the limit was
        // removed for WiredTiger in 4.2, so a plain readable _id is correct AND has no cliff. The
        // hashed-_id option was therefore not taken, which keeps prefix scans a pure _id range.
        // The caveat, stated because this test cannot cover it: a server pinned to FCV < 4.2 would
        // still fail here.
        ArtifactKey k = ArtifactKey.of("plsql-parse", "ORACLE", "OracleDemo", "local",
                "X".repeat(600), "api", "nodes", "Y".repeat(600) + ".json");
        put(k, "{}".getBytes(StandardCharsets.UTF_8));
        assertTrue(store.exists(k));
        assertEquals(2, store.size(k));
    }

    @Test
    @DisplayName("the credential guard applies to this backend too — C4 by construction")
    void credentialFilesCannotReachMongo() {
        // StorageFactory.create wraps every backend, so this is what a caller actually holds. The
        // assertion is here rather than only in CredentialGuardStoreTest because "a Mongo collection
        // is shared storage in exactly the way a bucket is" is the claim being made.
        var guarded = new CredentialGuardStore(store);
        for (String name : new String[]{"plsql-config.yaml", "plsql-env-config.yaml"}) {
            ArtifactKey k = ArtifactKey.of("config", name);
            assertThrows(SecurityException.class, () -> guarded.write(k));
            assertThrows(SecurityException.class, () -> guarded.read(k));
        }
        assertEquals(0, db.getCollection(collection).countDocuments());
    }
}
