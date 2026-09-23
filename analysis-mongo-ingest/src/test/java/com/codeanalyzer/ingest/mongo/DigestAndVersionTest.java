package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.write.RunMarker;
import com.codeanalyzer.ingest.mongo.write.SourceDigest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-004 (digest short-circuit) and ING-005 (version allocation).
 *
 * <p>Both exist to stop the same thing: a second ingest of work already done. ING-004 stops it when
 * the source is unchanged; ING-005 makes sure that when a run *is* new, two of them racing cannot
 * be handed the same version and collide on the unique index.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DigestAndVersionTest {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_digest_test");
        db.drop();
        new MongoIngestService(db).installSchema();
    }

    @AfterAll
    static void tearDown() {
        // Dropped here as well as in setUp: dropping only on start leaves every class's data
        // resident for the life of the mongod. Twenty-three classes accumulated 2.1 GB and the
        // OS started killing test runs.
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    @Test
    @Order(1)
    void reingestingUnchangedSourceWritesNothingAndReusesTheRun() {
        MongoIngestService svc = new MongoIngestService(db);
        var first = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS));
        assertFalse(first.skipped(), "the first ingest cannot be a skip");
        assertTrue(first.stats().snapshot().values().stream().mapToLong(Long::longValue).sum() > 0, "the first ingest wrote nothing");

        long docsAfterFirst = db.getCollection(Collections.PROC_NODE).countDocuments();
        long runsAfterFirst = db.getCollection(Collections.RUN).countDocuments();

        var second = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS));

        assertTrue(second.skipped(), "re-ingesting identical source was not short-circuited");
        assertEquals(0L, second.stats().snapshot().values().stream().mapToLong(Long::longValue).sum(), "the skipped run still wrote documents");
        assertEquals(first.runId(), second.runId(), "the skip returned a different run id");
        assertEquals(docsAfterFirst, db.getCollection(Collections.PROC_NODE).countDocuments(),
                "the skipped run changed the data");
        assertEquals(runsAfterFirst, db.getCollection(Collections.RUN).countDocuments(),
                "the skipped run created a second run document");
    }

    @Test
    @Order(2)
    void theSkipDoesNotBurnAVersionNumber() {
        // The digest is taken before the version is allocated. If it were taken after, every no-op
        // re-ingest would advance the counter and the sequence would climb with no run behind it.
        Document counterBefore = db.getCollection("am_counter")
                .find(Filters.eq("_id", "ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE")).first();
        int before = counterBefore == null ? 0 : counterBefore.getInteger("seq", 0);

        new MongoIngestService(db).ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS));

        Document counterAfter = db.getCollection("am_counter")
                .find(Filters.eq("_id", "ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE")).first();
        int after = counterAfter == null ? 0 : counterAfter.getInteger("seq", 0);
        assertEquals(before, after, "a skipped ingest advanced the version counter");
    }

    @Test
    @Order(3)
    void anUnpublishedRunIsNotAValidReasonToSkip() {
        // A digest may only be satisfied by a run that actually published. Matching an unfinished
        // or failed one would skip on the strength of work that never landed, leaving the content
        // permanently invisible with nothing scheduled to write it again.
        //
        // Uses FAILED rather than IN_PROGRESS: since MID-025 an IN_PROGRESS run for this entry
        // point means an ingest is running, and starting a second is refused outright — a stronger
        // guarantee, asserted separately below.
        String digest = db.getCollection(Collections.RUN)
                .find(Filters.eq("status", RunMarker.COMPLETE)).first().getString(SourceDigest.FIELD);
        assertNotNull(digest, "the completed run carries no source digest");

        db.getCollection(Collections.RUN).updateMany(
                Filters.eq("status", RunMarker.COMPLETE),
                new Document("$set", new Document("status", RunMarker.FAILED)));
        try {
            var r = new MongoIngestService(db).ingest("ORACLE", "ORACLEDEMO", "LOCAL",
                    "ERP_USP_RUN_CYCLE", new FileArtifactSource(ANALYSIS));
            assertFalse(r.skipped(),
                    "a run that never published satisfied the digest short-circuit");
        } finally {
            db.getCollection(Collections.RUN).updateMany(
                    Filters.eq("status", RunMarker.FAILED),
                    new Document("$set", new Document("status", RunMarker.COMPLETE)));
        }
    }

    @Test
    @Order(3)
    void aRunningIngestBlocksASecondOneEntirely() {
        // The MID-025 guarantee, stated here because it subsumes the case this test used to cover:
        // with an ingest in flight the question "does the digest match" is never reached.
        db.getCollection(Collections.RUN).updateMany(
                Filters.eq("status", RunMarker.COMPLETE),
                new Document("$set", new Document("status", RunMarker.IN_PROGRESS)));
        try {
            assertThrows(RunMarker.ConcurrentIngestException.class,
                    () -> new MongoIngestService(db).ingest("ORACLE", "ORACLEDEMO", "LOCAL",
                            "ERP_USP_RUN_CYCLE", new FileArtifactSource(ANALYSIS), true),
                    "a second ingest started while one was already in progress");
        } finally {
            db.getCollection(Collections.RUN).updateMany(
                    Filters.eq("status", RunMarker.IN_PROGRESS),
                    new Document("$set", new Document("status", RunMarker.COMPLETE)));
        }
    }

    @Test
    @Order(4)
    void theDigestIsIndependentOfArtifactListingOrder() {
        // ArtifactSource.list makes no ordering guarantee, and a local listing already differs from
        // an S3 key listing. An order-sensitive digest would vary by storage backend for identical
        // bytes, and the short-circuit would simply never fire in one of them.
        var src = new FileArtifactSource(ANALYSIS);
        List<String> paths = new java.util.ArrayList<>(List.of(
                "api/tables/index.json", "api/statements/index.json", "edges.json"));
        String forward = SourceDigest.of(src, paths);
        java.util.Collections.reverse(paths);
        String reversed = SourceDigest.of(src, paths);
        assertEquals(forward, reversed, "the digest depends on the order paths were listed in");
    }

    @Test
    @Order(5)
    void differentContentProducesADifferentDigest() {
        // The other half: a digest that never changes would short-circuit every real re-analysis.
        var src = new FileArtifactSource(ANALYSIS);
        String withStatements = SourceDigest.of(src,
                List.of("api/tables/index.json", "api/statements/index.json"));
        String withoutStatements = SourceDigest.of(src, List.of("api/tables/index.json"));
        assertNotEquals(withStatements, withoutStatements,
                "adding an artifact did not change the digest");
    }

    @Test
    @Order(6)
    void concurrentVersionAllocationsAreDenseAndUnique() {
        // ING-005. Ten threads, one counter, no transactions available on this server —
        // findOneAndUpdate($inc) is atomic on a single document, which is what makes it safe.
        RunMarker marker = new RunMarker(db);
        int n = 10;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Integer>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < n; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return marker.nextVersion("ORACLE", "CONC", "LOCAL", "RACE");
                }));
            }
            start.countDown();
            Set<Integer> seen = new java.util.TreeSet<>();
            for (Future<Integer> f : futures) seen.add(f.get(30, TimeUnit.SECONDS));

            assertEquals(n, seen.size(), "duplicate versions were handed out: " + seen);
            assertEquals(1, java.util.Collections.min(seen), "versions do not start at 1");
            assertEquals(n, java.util.Collections.max(seen), "versions are not dense: " + seen);
        } catch (Exception e) {
            throw new AssertionError("concurrent allocation failed", e);
        } finally {
            pool.shutdownNow();
        }
    }
}
