package com.codeanalyzer.ingest.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * VER-006 — no ingested document exceeds the 1 MB ceiling, and a planted oversized one is caught.
 *
 * <h2>Which limit this is, and why it is not the 16 MB BSON limit</h2>
 *
 * <p>{@code RELATIONAL-MODEL-PLAN.md} §17.8 sets 1 MB as an <b>ingest</b> ceiling — 16× stricter
 * than MongoDB's hard 16 MB document limit. The gap is deliberate headroom: a document under 1 MB
 * today has sixteen doublings of growth before it becomes a server-side failure, which is what
 * makes "under 1 MB" a useful early warning rather than a limit this codebase is already brushing
 * against. {@code MongoDocumentGuard} (in {@code analyzer-storage}) enforces the hard limit at
 * write time by diverting an oversized element to {@code ArtifactStore}; this gate is the softer,
 * earlier check the plan asks for on top of it.
 *
 * <h2>Real corpus, both ingest paths</h2>
 *
 * <p>Both a PL/SQL and a JAR analysis are ingested into the same database, so the scan covers
 * every collection either path writes — including {@code am_bundled_jar} and
 * {@code am_bundled_jar_resource} (ING-116), the two collections closest to this ceiling before
 * the split (worst case measured at 465,644 B embedded, 61,636 B split).
 */
class DocSizeGateTest {

    private static final long CEILING_BYTES = 1_000_000;

    private static final Path PLSQL =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(PLSQL), "ORACLE analysis not on disk");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");

        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_doc_size_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(PLSQL));
        svc.ingestJar("unified-web", new FileArtifactSource(jar));
    }

    @AfterAll
    static void tearDown() {
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    /** The largest document in {@code collection}, in bytes, or -1 if it is empty. */
    private long largestDocBytes(String collection) {
        List<Document> pipeline = List.of(
                new Document("$project", new Document("sz",
                        new Document("$bsonSize", "$$ROOT"))),
                new Document("$sort", new Document("sz", -1)),
                new Document("$limit", 1));
        for (Document d : db.getCollection(collection).aggregate(pipeline)) {
            return d.getInteger("sz");
        }
        return -1;
    }

    @Test
    void noCollectionExceedsOneMegabyte() {
        List<String> offenders = new ArrayList<>();
        long worstOverall = 0;
        String worstCollection = null;
        for (String name : db.listCollectionNames()) {
            if (!name.startsWith("am_")) continue;
            long sz = largestDocBytes(name);
            if (sz > CEILING_BYTES) offenders.add(name + " (" + sz + " B)");
            if (sz > worstOverall) {
                worstOverall = sz;
                worstCollection = name;
            }
        }
        assertTrue(worstOverall > 0, "no documents were scanned; the ingest produced nothing");
        assertTrue(offenders.isEmpty(),
                "these collections exceed the 1 MB ingest ceiling: " + offenders
                        + " (worst overall: " + worstCollection + " at " + worstOverall + " B)");
    }

    @Test
    void aPlantedOversizedDocumentIsCaught() {
        // Calibration in the honest direction: prove the scan can fail before trusting that it
        // passing means something. A 1.2 MB string field, comfortably over the ceiling and
        // comfortably under the 16 MB BSON hard limit, so this is testing the gate and not
        // accidentally testing the driver.
        String scratch = "am_doc_size_scratch";
        db.getCollection(scratch).drop();
        db.getCollection(scratch).insertOne(
                new Document("bloat", "x".repeat(1_200_000)));
        try {
            long sz = largestDocBytes(scratch);
            assertTrue(sz > CEILING_BYTES,
                    "the planted document measured " + sz + " B, which the gate should have "
                            + "flagged as over the 1 MB ceiling");
        } finally {
            db.getCollection(scratch).drop();
        }
    }

    @Test
    void aDocumentWellUnderTheCeilingIsNotFlagged() {
        // The other calibration direction: a gate that fires on everything is as useless as one
        // that fires on nothing.
        String scratch = "am_doc_size_scratch_small";
        db.getCollection(scratch).drop();
        db.getCollection(scratch).insertOne(new Document("small", "fine"));
        try {
            long sz = largestDocBytes(scratch);
            assertFalse(sz > CEILING_BYTES,
                    "a tiny document measured " + sz + " B and was flagged as oversized");
        } finally {
            db.getCollection(scratch).drop();
        }
    }
}
