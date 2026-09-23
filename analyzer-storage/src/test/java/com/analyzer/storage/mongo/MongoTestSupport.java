package com.analyzer.storage.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Connects the Mongo tests to a <b>real</b> MongoDB.
 *
 * <p>There is no fake server here, and that is a deliberate reversal of PLAN-07 §6, which planned a
 * {@code FakeMongoServer} to mirror {@code FakeS3Server}. The two situations are not alike. S3 is
 * HTTP with a small, documented request surface, so a fake is a few hundred lines and its fidelity
 * is checkable by reading the spec. The parts of MongoDB this backend leans on are the parts a fake
 * would model wrongly and silently: {@code findOneAndReplace} returning the pre-image atomically,
 * {@code $merge} into the source collection, GridFS chunking, and above all
 * {@code GridFSDownloadStream.skip}'s chunk-aware seek — which is the entire justification for
 * calling {@code read(key, offset, length)} a genuine ranged read. A fake that returned the right
 * bytes by reading from the start would pass every assertion while removing the property under
 * test.
 *
 * <p>So these tests require a server and <b>skip</b> without one, rather than passing against a
 * stand-in. A skipped test is honest; a green test against a fake seek is not.
 *
 * <p>Point the suite elsewhere with {@code -Dtest.mongodb.uri=...}. Each store gets its own
 * collection name inside one scratch database, which gives the same per-test isolation
 * {@code @TempDir} gives locally and {@code FakeS3Server}'s fresh bucket gives for S3.
 */
public final class MongoTestSupport {

    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String DATABASE = "analyzer_d_test";

    /** Unique collection per store instance, so tests cannot see each other's artifacts. */
    private static final AtomicInteger SEQ = new AtomicInteger();

    private MongoTestSupport() {
    }

    public static String uri() {
        return System.getProperty("test.mongodb.uri", DEFAULT_URI);
    }

    /**
     * @return an open client, after skipping the test if no server answered
     */
    public static MongoClient clientOrSkip() {
        MongoClient client = null;
        try {
            client = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(uri()))
                    // Short, so a machine with no MongoDB skips in a second rather than stalling
                    // the build for the driver's 30-second default.
                    .applyToClusterSettings(b -> b.serverSelectionTimeout(2, TimeUnit.SECONDS))
                    .build());
            client.getDatabase(DATABASE).runCommand(new Document("ping", 1));
            return client;
        } catch (RuntimeException e) {
            if (client != null) {
                try {
                    client.close();
                } catch (RuntimeException ignored) {
                    // best effort
                }
            }
            assumeTrue(false, "No MongoDB at " + uri() + " — skipping. " + e.getMessage());
            throw new AssertionError("unreachable");
        }
    }

    public static String freshCollection(String label) {
        return label + "_" + SEQ.incrementAndGet() + "_" + Long.toString(System.nanoTime(), 36);
    }

    /** Drops the manifest collection and its GridFS buckets. */
    public static void drop(MongoDatabase db, String collection) {
        for (String name : new String[]{collection, collection + "_fs.files", collection + "_fs.chunks"}) {
            try {
                db.getCollection(name).drop();
            } catch (RuntimeException ignored) {
                // best effort teardown
            }
        }
    }
}
