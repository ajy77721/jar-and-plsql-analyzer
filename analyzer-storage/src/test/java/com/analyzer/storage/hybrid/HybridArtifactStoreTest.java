package com.analyzer.storage.hybrid;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.mongo.MongoArtifactStore;
import com.analyzer.storage.mongo.MongoTestSupport;
import com.analyzer.storage.s3.FakeS3Server;
import com.analyzer.storage.s3.S3ArtifactStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip and routing tests for {@link HybridArtifactStore}.
 *
 * <p>Follows the same "real backend, not a mock" pattern {@code MongoArtifactStoreConformanceTest}
 * and {@code S3ArtifactStoreConformanceTest} already use: a real MongoDB (the test class SKIPS,
 * rather than passing against a stand-in, when none is reachable — see {@link MongoTestSupport})
 * and {@link FakeS3Server} speaking real HTTP with a real {@link S3Client}. No new test
 * infrastructure is introduced.
 *
 * <p>The Mongo-vs-S3 split threshold is a small, fixed 1 KiB, deliberately smaller than the Mongo
 * half's OWN inline-vs-GridFS threshold (512 B here) — the two are independent knobs, and keeping
 * them different in this suite is what proves neither one is accidentally reading the other.
 */
class HybridArtifactStoreTest {

    private static final long BIG_THRESHOLD = 1024;
    private static final int MONGO_INLINE_THRESHOLD = 512;

    private MongoClient mongoClient;
    private MongoDatabase db;
    private String collection;
    private MongoArtifactStore mongoStore;

    private FakeS3Server s3Server;
    private S3Client s3Client;
    private S3ArtifactStore s3Store;

    private HybridArtifactStore hybrid;

    @BeforeEach
    void setUp() throws IOException {
        mongoClient = MongoTestSupport.clientOrSkip();
        db = mongoClient.getDatabase(MongoTestSupport.DATABASE);
        collection = MongoTestSupport.freshCollection("hybrid");
        mongoStore = new MongoArtifactStore(db, "data", MONGO_INLINE_THRESHOLD, collection, 100_000L);
        mongoStore.ensureIndexes();

        s3Server = new FakeS3Server("hybrid-bucket");
        s3Client = S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .endpointOverride(URI.create(s3Server.endpoint()))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .build();
        s3Store = new S3ArtifactStore(s3Client, "hybrid-bucket", "data");

        hybrid = new HybridArtifactStore(mongoStore, s3Store, BIG_THRESHOLD);
    }

    @AfterEach
    void tearDown() {
        if (db != null && collection != null) MongoTestSupport.drop(db, collection);
        if (mongoClient != null) mongoClient.close();
        if (s3Client != null) s3Client.close();
        if (s3Server != null) s3Server.close();
    }

    private static byte[] bytesOf(int length, int seed) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) b[i] = (byte) ((i + seed) % 251);
        return b;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    @Test
    void smallArtifactRoundTripsThroughMongoOnly() throws IOException {
        ArtifactKey key = ArtifactKey.of("small.json");
        String content = "{\"hello\":\"world\"}";

        try (OutputStream out = hybrid.write(key)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }

        try (InputStream in = hybrid.read(key)) {
            assertEquals(content, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertTrue(mongoStore.exists(key),
                "small artifact should be reachable directly via the mongo half");
        assertFalse(s3Store.exists(key), "small artifact must not have been written to S3");
    }

    @Test
    void bigArtifactWithKnownLengthGoesStraightToS3() throws IOException {
        ArtifactKey key = ArtifactKey.of("big.bin");
        byte[] payload = bytesOf((int) BIG_THRESHOLD * 4, 7);

        try (OutputStream out = hybrid.write(key, payload.length)) {
            out.write(payload);
        }

        try (InputStream in = hybrid.read(key)) {
            assertArrayEquals(payload, in.readAllBytes());
        }
        assertTrue(s3Store.exists(key), "big artifact should be reachable directly via the s3 half");
        assertFalse(mongoStore.exists(key), "big artifact must not have been written to mongo");
    }

    @Test
    void writeThatCrossesThresholdMidStreamRollsOverToS3() throws IOException {
        ArtifactKey key = ArtifactKey.of("rollover.bin");
        byte[] underThreshold = bytesOf((int) BIG_THRESHOLD / 2, 1);
        byte[] moreBytes = bytesOf((int) BIG_THRESHOLD, 2);   // pushes the total past the threshold

        try (OutputStream out = hybrid.write(key)) {   // no content-length hint
            out.write(underThreshold);
            out.write(moreBytes);
        }

        byte[] expected = concat(underThreshold, moreBytes);
        try (InputStream in = hybrid.read(key)) {
            assertArrayEquals(expected, in.readAllBytes());
        }
        assertTrue(s3Store.exists(key), "rolled-over artifact should land in s3");
        assertFalse(mongoStore.exists(key), "rolled-over artifact must not be left behind in mongo");
    }

    @Test
    void overwriteAcrossTheThresholdCleansUpTheOtherBackend() throws Exception {
        ArtifactKey key = ArtifactKey.of("flip.bin");

        // First write: big, known length up front, lands in S3.
        byte[] big = bytesOf((int) BIG_THRESHOLD * 3, 3);
        try (OutputStream out = hybrid.write(key, big.length)) {
            out.write(big);
        }
        assertTrue(s3Store.exists(key));
        // The cross-backend cleanup runs on a background sweeper (off the write path — a remote
        // S3 DELETE per artifact was the save-phase bottleneck), so poll briefly instead of
        // asserting synchronously. The guarantee is "promptly", not "within close()".
        awaitSweep(() -> !mongoStore.exists(key));

        // Second write: small, must land in Mongo AND remove the stale S3 copy — otherwise read()
        // would still see the (correct-looking, but stale) backend it checks second.
        String small = "now small";
        try (OutputStream out = hybrid.write(key)) {
            out.write(small.getBytes(StandardCharsets.UTF_8));
        }

        assertTrue(mongoStore.exists(key));
        awaitSweep(() -> !s3Store.exists(key));
        assertFalse(s3Store.exists(key), "stale big copy in s3 must be cleaned up on overwrite");
        try (InputStream in = hybrid.read(key)) {
            assertEquals(small, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** Waits up to 5s for a background sweep to observe `condition` (see the async sweeper). */
    private static void awaitSweep(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }
}
