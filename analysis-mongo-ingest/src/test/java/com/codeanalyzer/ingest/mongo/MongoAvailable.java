package com.codeanalyzer.ingest.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

/**
 * Detects a usable local mongod.
 *
 * <p>The repo convention (see {@code MongoTestSupport}) is to run integration tests against a real
 * local server and skip when absent. The danger with skip-if-absent is that a build where every
 * Mongo test skipped looks exactly like one where they all passed — so
 * {@code MongoIngestM1Test.mongoWasActuallyExercised} asserts the suite did not silently do
 * nothing, rather than trusting the skip.
 */
final class MongoAvailable {

    static final String URI = System.getProperty("mongo.uri", "mongodb://localhost:27017");
    static final String DB  = System.getProperty("mongo.testdb", "am_ingest_test");

    private MongoAvailable() {}

    static boolean present() {
        try (MongoClient c = MongoClients.create(URI)) {
            c.getDatabase("admin").runCommand(new Document("ping", 1));
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
