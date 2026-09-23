package com.analyzer.storage.s3;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.CredentialGuardStore;
import com.analyzer.storage.LocalHandle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for {@link S3ArtifactStore} against a real {@link S3Client} pointed at
 * {@link FakeS3Server} over real HTTP.
 *
 * <p>Complements the Mockito tests rather than replacing them. A mock proves which SDK method was
 * called; this proves the whole wire path — signing, aws-chunked framing, Range semantics,
 * ListObjectsV2 XML and continuation tokens, the three-request multipart dance — actually produces
 * the right bytes in the right places.
 *
 * <p><b>Not proven here:</b> real S3 authentication and IAM (the fake never checks a signature),
 * real network failure modes, request throttling, versioning, objects above the 5 GiB CopyObject
 * limit, or anything about latency and consistency. See the live smoke checklist.
 */
class S3ArtifactStoreWireTest {

    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "data";

    private FakeS3Server server;
    private S3Client client;
    private S3ArtifactStore store;

    /** Small enough that the genuine multipart path costs milliseconds, not megabytes. */
    private static final int TEST_PART_SIZE = 64 * 1024;

    @BeforeEach
    void setUp() throws IOException {
        server = new FakeS3Server(BUCKET);
        client = S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .endpointOverride(URI.create(server.endpoint()))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .build();
        store = new S3ArtifactStore(client, BUCKET, PREFIX) {
            @Override
            OutputStream newUpload(String objectKey, long contentLength) {
                return new MultipartUploadOutputStream(client(), BUCKET, objectKey,
                        contentLength, TEST_PART_SIZE);
            }
        };
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.close();
    }

    private void put(ArtifactKey k, String s) throws IOException {
        try (OutputStream out = store.write(k)) {
            out.write(s.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String get(ArtifactKey k) throws IOException {
        try (InputStream in = store.read(k)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // ── 1. round trip ───────────────────────────────────────────────────────

    @Test
    void writeThenReadRoundTripsTheExactBytesOverTheWire() throws IOException {
        ArtifactKey k = ArtifactKey.of("jar", "demo", "analysis.json");
        String json = "{\"classes\":[{\"n\":\"a\"},{\"n\":\"ü😀\"}]}";
        put(k, json);

        assertEquals(json, get(k));
        assertEquals(Set.of("data/jar/demo/analysis.json"), server.keys(),
                "the key mapping must land the object exactly here");
        assertTrue(store.exists(k));
        assertEquals(json.getBytes(StandardCharsets.UTF_8).length, store.size(k));
        assertTrue(store.lastModified(k) > 0);
    }

    @Test
    void allBinaryByteValuesSurviveTheUpload() throws IOException {
        byte[] bytes = new byte[256];
        for (int i = 0; i < 256; i++) bytes[i] = (byte) i;
        ArtifactKey k = ArtifactKey.of("jar", "raw.bin");
        try (OutputStream out = store.write(k)) {
            out.write(bytes);
        }
        try (InputStream in = store.read(k)) {
            assertArrayEquals(bytes, in.readAllBytes());
        }
    }

    @Test
    void missingObjectReadsAsNoSuchFile() {
        assertThrows(NoSuchFileException.class, () -> store.read(ArtifactKey.of("nope.json")));
        assertFalse(store.exists(ArtifactKey.of("nope.json")));
        assertEquals(-1, store.size(ArtifactKey.of("nope.json")));
        assertEquals(-1, store.lastModified(ArtifactKey.of("nope.json")));
    }

    // ── 2. real multipart ───────────────────────────────────────────────────

    @Test
    void aWriteLargerThanOnePartGoesMultipartAndReassemblesByteIdentical() throws IOException {
        byte[] payload = new byte[TEST_PART_SIZE * 3 + 1234];   // 3 full parts + a remainder
        new Random(42).nextBytes(payload);
        ArtifactKey k = ArtifactKey.of("jar", "big.bin");

        try (OutputStream out = store.write(k)) {
            // Written in awkward slices, so any off-by-one in the buffer/part boundary shows up.
            int pos = 0;
            int[] slices = {1, 7, 4095, TEST_PART_SIZE - 10, 1};
            int i = 0;
            while (pos < payload.length) {
                int n = Math.min(slices[i++ % slices.length], payload.length - pos);
                out.write(payload, pos, n);
                pos += n;
            }
        }

        assertTrue(server.uploadPartRequests.get() >= 4,
                "expected a real multipart upload, saw " + server.uploadPartRequests.get() + " parts");
        assertArrayEquals(payload, server.raw("data/jar/big.bin"),
                "reassembled object must be byte-identical");
        try (InputStream in = store.read(k)) {
            assertArrayEquals(payload, in.readAllBytes());
        }
        assertTrue(server.liveUploadIds().isEmpty(), "no upload may be left open");
        assertEquals(0, server.abortRequests.get());
    }

    @Test
    void aWriteSmallerThanOnePartUsesASinglePutAndCreatesNoUpload() throws IOException {
        put(ArtifactKey.of("small.json"), "{}");
        assertEquals(1, server.putRequests.get());
        assertEquals(0, server.uploadPartRequests.get());
        assertTrue(server.liveUploadIds().isEmpty());
    }

    // ── 3. ranged read ──────────────────────────────────────────────────────

    @Test
    void rangedReadReturnsTheCorrectSliceAndNotTheWholeObject() throws IOException {
        ArtifactKey k = ArtifactKey.of("a", "range.txt");
        put(k, "0123456789abcdef");

        try (InputStream in = store.read(k, 4, 3)) {
            assertEquals("456", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        try (InputStream in = store.read(k, 10, -1)) {
            assertEquals("abcdef", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        try (InputStream in = store.read(k, 0, 1)) {
            assertEquals("0", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        // Length beyond the end is clamped, exactly as a local bounded read would be.
        try (InputStream in = store.read(k, 14, 100)) {
            assertEquals("ef", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void anOffsetPastTheEndOfTheObjectIsAnEofNotASilentEmptyRead() throws IOException {
        ArtifactKey k = ArtifactKey.of("a", "range.txt");
        put(k, "short");
        assertThrows(java.io.EOFException.class, () -> store.read(k, 500, 10));
    }

    // ── 4. pagination ───────────────────────────────────────────────────────

    @Test
    void listPagesAcrossManyPagesAndReturnsEveryObjectExactlyOnce() throws IOException {
        server.maxKeys(2);                       // force many pages
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            String leaf = String.format("f%02d.json", i);
            put(ArtifactKey.of("tree", leaf), "v" + i);
            expected.add("tree/" + leaf);
        }
        server.resetCounters();

        List<String> seen;
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("tree"), true)) {
            seen = s.map(e -> e.key().toString()).sorted().toList();
        }
        assertEquals(expected, seen);
        assertTrue(server.listRequests.get() >= 5,
                "9 keys at 2 per page must take several requests, took " + server.listRequests.get());
        assertEquals(0, server.headRequests.get(), "listing must not HEAD anything");
    }

    @Test
    void listIsLazyAndStopsFetchingPagesWhenTheCallerStops() throws IOException {
        server.maxKeys(2);
        for (int i = 0; i < 9; i++) put(ArtifactKey.of("tree", "f" + i + ".json"), "v");
        server.resetCounters();

        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("tree"), true)) {
            assertTrue(s.findFirst().isPresent());
        }
        assertEquals(1, server.listRequests.get(),
                "a caller that reads one entry must not pull the whole listing");
    }

    @Test
    void nonRecursiveListRollsSubtreesIntoDirectoryEntries() throws IOException {
        server.maxKeys(100);
        put(ArtifactKey.of("tree", "top.json"), "t");
        put(ArtifactKey.of("tree", "sub", "a.json"), "a");
        put(ArtifactKey.of("tree", "sub", "b.json"), "b");
        put(ArtifactKey.of("tree", "other", "c.json"), "c");

        Map<String, Boolean> byKey;
        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("tree"), false)) {
            byKey = s.collect(Collectors.toMap(e -> e.key().toString(), ArtifactEntry::directory));
        }
        assertEquals(Map.of("tree/top.json", false, "tree/sub", true, "tree/other", true), byKey);
    }

    // ── 5. prefix boundary ──────────────────────────────────────────────────

    /** The bug reported in the parked sketch: {@code a/b} leaking {@code a/bc}. */
    @Test
    void listingPrefixAbDoesNotReturnAnythingUnderAbc() throws IOException {
        server.maxKeys(100);
        put(ArtifactKey.of("a", "b", "inside.json"), "in");
        put(ArtifactKey.of("a", "bc", "x.json"), "out");
        put(ArtifactKey.of("a", "b2.json"), "out");

        try (Stream<ArtifactEntry> s = store.list(ArtifactKey.of("a", "b"), true)) {
            assertEquals(List.of("a/b/inside.json"), s.map(e -> e.key().toString()).toList());
        }
        assertEquals(Set.of(ArtifactKey.of("a", "b", "inside.json")),
                store.statPrefix(ArtifactKey.of("a", "b")).keySet());
    }

    @Test
    void deletePrefixAbLeavesAbcAlone() throws IOException {
        server.maxKeys(100);
        put(ArtifactKey.of("a", "b", "inside.json"), "in");
        put(ArtifactKey.of("a", "b", "deep", "inside2.json"), "in");
        put(ArtifactKey.of("a", "bc", "x.json"), "out");

        assertEquals(2, store.deletePrefix(ArtifactKey.of("a", "b")));
        assertEquals(Set.of("data/a/bc/x.json"), server.keys());
    }

    // ── 6. statPrefix is one listing ────────────────────────────────────────

    @Test
    void statPrefixIsOneListingAndZeroHeads() throws IOException {
        server.maxKeys(1000);
        for (int i = 0; i < 25; i++) put(ArtifactKey.of("stat", "f" + i + ".json"), "x".repeat(i + 1));
        server.resetCounters();

        Map<ArtifactKey, ArtifactEntry> m = store.statPrefix(ArtifactKey.of("stat"));

        assertEquals(25, m.size());
        assertEquals(8, m.get(ArtifactKey.of("stat", "f7.json")).size());
        assertTrue(m.get(ArtifactKey.of("stat", "f7.json")).lastModified() > 0);
        assertEquals(1, server.listRequests.get(), "must be ONE listing");
        assertEquals(0, server.headRequests.get(), "must be ZERO HEADs");
        assertEquals(0, server.getRequests.get());
    }

    // ── 7/8. delete, move ───────────────────────────────────────────────────

    @Test
    void deletePrefixBatchesAndRemovesTheWholeSubtree() throws IOException {
        server.maxKeys(1000);
        for (int i = 0; i < 30; i++) put(ArtifactKey.of("gone", "f" + i + ".json"), "x");
        put(ArtifactKey.of("keep", "f.json"), "x");
        server.resetCounters();

        assertEquals(30, store.deletePrefix(ArtifactKey.of("gone")));
        assertEquals(Set.of("data/keep/f.json"), server.keys());
        assertEquals(1, server.batchDeleteRequests.get(),
                "30 keys must go in one DeleteObjects call, not 30 DeleteObject calls");
    }

    @Test
    void deleteReturnsTrueOnlyWhenSomethingWasThere() throws IOException {
        ArtifactKey k = ArtifactKey.of("one.json");
        put(k, "x");
        assertTrue(store.delete(k));
        assertFalse(store.delete(k));
        assertTrue(server.keys().isEmpty());
    }

    @Test
    void moveIsCopyThenDeleteWithAByteIdenticalDestination() throws IOException {
        ArtifactKey from = ArtifactKey.of("a", "src.json");
        ArtifactKey to = ArtifactKey.of("b", "dst.json");
        put(from, "{\"moved\":true}");

        store.move(from, to);

        assertFalse(store.exists(from), "source must be gone");
        assertEquals("{\"moved\":true}", get(to));
        assertEquals(Set.of("data/b/dst.json"), server.keys());
    }

    @Test
    void copyLeavesBothAndDoesNotRoundTripThroughThisProcess() throws IOException {
        ArtifactKey from = ArtifactKey.of("a", "src.json");
        ArtifactKey to = ArtifactKey.of("b", "dst.json");
        put(from, "payload");
        server.resetCounters();

        store.copy(from, to);

        assertEquals("payload", get(to));
        assertEquals("payload", get(from));
        assertEquals(0, server.putRequests.get(), "a server-side copy must not re-upload the bytes");
    }

    // ── 9. materialize ──────────────────────────────────────────────────────

    @Test
    void materializeWritesOutsideTheDataRootAndCloseDeletesIt() throws IOException {
        ArtifactKey k = ArtifactKey.of("jar", "demo.jar");
        byte[] payload = new byte[TEST_PART_SIZE * 2 + 5];      // large enough to be a real download
        new Random(7).nextBytes(payload);
        try (OutputStream out = store.write(k)) {
            out.write(payload);
        }

        Path p;
        try (LocalHandle h = store.materialize(k)) {
            p = h.path();
            assertTrue(h.temporary());
            assertArrayEquals(payload, Files.readAllBytes(p));
            assertTrue(p.getFileName().toString().endsWith("demo.jar"));
            assertTrue(p.toAbsolutePath().startsWith(
                    Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath()));
        }
        assertFalse(Files.exists(p), "close() must delete the temp copy");
    }

    @Test
    void materializeOfAMissingArtifactFailsAndLeavesNoTemp() {
        assertThrows(IOException.class, () -> store.materialize(ArtifactKey.of("nope.jar")));
    }

    // ── 10. abort on failure ────────────────────────────────────────────────

    @Test
    void aFailedPartUploadAbortsTheUploadOnTheServer() {
        server.failPart(2);
        byte[] payload = new byte[TEST_PART_SIZE * 4];
        ArtifactKey k = ArtifactKey.of("jar", "doomed.bin");

        assertThrows(IOException.class, () -> {
            try (OutputStream out = store.write(k)) {
                out.write(payload);
            }
        });

        assertEquals(1, server.abortRequests.get(),
                "the server must have received an AbortMultipartUpload");
        assertEquals(1, server.abortedUploadIds().size());
        assertTrue(server.liveUploadIds().isEmpty(), "no orphaned, billable parts may remain");
        assertFalse(server.keys().contains("data/jar/doomed.bin"),
                "a failed multipart must not publish a partial object");
    }

    @Test
    void abandoningAWriteWithoutClosingPublishesNothingAndLeavesThePreviousObject()
            throws IOException {
        ArtifactKey k = ArtifactKey.of("jar", "existing.json");
        put(k, "old");

        OutputStream abandoned = store.write(k);
        abandoned.write("new".getBytes(StandardCharsets.UTF_8));
        // deliberately never closed

        assertEquals("old", get(k), "an unclosed write must not be visible");
    }

    // ── 11. credential guard through the S3 path ────────────────────────────

    @Test
    void credentialFilesNeverReachTheBucketThroughTheS3Backend() throws IOException {
        ArtifactStore guarded = new CredentialGuardStore(store);
        for (String leaf : new String[]{"plsql-config.yaml", "plsql-env-config.yaml"}) {
            ArtifactKey k = ArtifactKey.of("config", leaf);
            assertThrows(SecurityException.class, () -> guarded.write(k), leaf);
            assertThrows(SecurityException.class, () -> guarded.write(k, 4), leaf);
        }
        // A non-credential file through the same wrapper still works, so the guard is not a
        // blanket refusal.
        try (OutputStream out = guarded.write(ArtifactKey.of("config", "settings.json"))) {
            out.write("{}".getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(Set.of("data/config/settings.json"), server.keys());
    }

    // ── empty root prefix ───────────────────────────────────────────────────

    @Test
    void anEmptyRootPrefixPutsArtifactsAtTheBucketRoot() throws IOException {
        S3ArtifactStore bare = new S3ArtifactStore(client, BUCKET, "");
        try (OutputStream out = bare.write(ArtifactKey.of("jar", "x.json"))) {
            out.write("v".getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(Set.of("jar/x.json"), server.keys());
        try (InputStream in = bare.read(ArtifactKey.of("jar", "x.json"))) {
            assertEquals("v", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertEquals(1, bare.statPrefix(ArtifactKey.of("jar")).size());
    }
}
