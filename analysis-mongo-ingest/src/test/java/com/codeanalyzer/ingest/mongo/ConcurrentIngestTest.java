package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.schema.SchemaInstaller;
import com.codeanalyzer.ingest.mongo.write.RunMarker;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-025 — one ingest per analysis, and one current run per entry point.
 *
 * <h2>What two concurrent ingests did</h2>
 *
 * <p>Both ran to completion. {@code complete()} demotes the previous current run before publishing,
 * so the second quietly superseded the first: one machine's entire ingest discarded, with nothing to
 * show for it but a {@code SUPERSEDED} marker timestamped seconds before the run that replaced it.
 * No error, no warning, and the wasted work is only visible to someone comparing timestamps.
 *
 * <p>The fix is a partial unique index on {@code IN_PROGRESS}, not a read-then-write check: a check
 * has a window between the read and the write, and a window is exactly what concurrency exploits.
 * The second ingest now fails on its <b>first</b> write rather than its last.
 *
 * <h2>Why this uses run documents rather than two real ingests</h2>
 *
 * <p>The property is about the marker and the index. Driving it with two full ingests would take
 * minutes, need both analyses resident at once, and conflate ingest correctness with publish
 * exclusivity — a failure could mean either. Run documents exercise exactly the guarantee.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConcurrentIngestTest {

    private static MongoClient client;
    private MongoDatabase db;
    private RunMarker marker;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        client = MongoClients.create(MongoAvailable.URI);
    }

    @AfterAll
    static void close() {
        if (client != null) {
            client.getDatabase("am_concurrent_ingest_test").drop();
            client.close();
        }
    }

    @BeforeEach
    void freshDb() {
        db = client.getDatabase("am_concurrent_ingest_test");
        db.drop();
        new SchemaInstaller(db).install();
        marker = new RunMarker(db);
    }

    private static Document run(String id, int version) {
        return new Document("_id", id)
                .append("kind", "PLSQL")
                .append("dialect", "ORACLE")
                .append("project", "DEMO")
                .append("environment", "LOCAL")
                .append("entryPoint", "ERP_USP_RUN_CYCLE")
                .append("version", version)
                .append("isCurrent", false);
    }

    @Test
    @Order(1)
    void aSecondIngestOfTheSameAnalysisIsRefusedAtItsFirstWrite() {
        marker.begin(run("run-1", 1));
        var e = assertThrows(RunMarker.ConcurrentIngestException.class,
                () -> marker.begin(run("run-2", 2)),
                "a second concurrent ingest was allowed to start; it would finish and silently "
                        + "supersede the first");
        assertTrue(e.getMessage().contains("ERP_USP_RUN_CYCLE"),
                "the refusal does not name the analysis: " + e.getMessage());
    }

    @Test
    @Order(2)
    void exactlyOneOfManyRacingIngestsStarts() {
        // The property under real contention rather than in sequence.
        int racers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        AtomicInteger started = new AtomicInteger();
        AtomicInteger refused = new AtomicInteger();
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < racers; i++) {
                final int n = i;
                futures.add(pool.submit(() -> {
                    go.await();
                    try {
                        marker.begin(run("run-" + n, n + 1));
                        started.incrementAndGet();
                    } catch (RunMarker.ConcurrentIngestException expected) {
                        refused.incrementAndGet();
                    }
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("the race did not complete", e);
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, started.get(),
                started.get() + " ingests started concurrently; all but one would have their work "
                        + "discarded at publish time");
        assertEquals(racers - 1, refused.get(), "some racers neither started nor were refused");
        assertEquals(1, db.getCollection(Collections.RUN)
                        .countDocuments(Filters.eq("status", RunMarker.IN_PROGRESS)),
                "more than one IN_PROGRESS run exists for one entry point");
    }

    @Test
    @Order(3)
    void aFinishedIngestReleasesTheSlot() {
        // The guard must not become a permanent lock. Completing frees the entry point.
        marker.begin(run("run-1", 1));
        marker.complete("run-1", "ORACLE", "DEMO", "LOCAL", "ERP_USP_RUN_CYCLE");
        assertDoesNotThrow(() -> marker.begin(run("run-2", 2)),
                "the entry point stayed locked after its ingest completed");
    }

    @Test
    @Order(4)
    void aFailedIngestAlsoReleasesTheSlot() {
        // Otherwise one crash makes an analysis permanently un-ingestable, recoverable only by
        // editing the database — a far worse failure than the one being prevented.
        marker.begin(run("run-1", 1));
        marker.fail("run-1", "boom");
        assertDoesNotThrow(() -> marker.begin(run("run-2", 2)),
                "a failed ingest left the entry point locked forever");
    }

    @Test
    @Order(5)
    void aCrashedIngestIsReclaimedRatherThanLockingTheAnalysisForever() {
        // fail() clears the marker on a graceful error. A hard kill does not — the IN_PROGRESS run
        // simply stays. Without reclamation the entry point is locked permanently and the only
        // recovery is editing the database, which is a worse failure than the work-loss this guard
        // prevents and arrives via an ordinary process kill.
        //
        // This case was missed by the first round of calibration: it only exercised fail().
        marker.begin(run("crashed-1", 1));
        java.time.Instant old = java.time.Instant.now()
                .minusMillis(RunMarker.ABANDONED_AFTER_MS + 60_000);
        db.getCollection(Collections.RUN).updateOne(Filters.eq("_id", "crashed-1"),
                new Document("$set", new Document("startedAt", old.toString())
                        .append("startedAtEpochMs", old.toEpochMilli())));

        assertDoesNotThrow(() -> marker.begin(run("run-2", 2)),
                "a crashed ingest locked the analysis permanently");
        assertEquals(RunMarker.FAILED, db.getCollection(Collections.RUN)
                        .find(Filters.eq("_id", "crashed-1")).first().getString("status"),
                "the abandoned run was not marked FAILED, so its documents have nothing to "
                        + "attribute them to");
    }

    @Test
    @Order(6)
    void aLiveIngestIsNotReclaimed() {
        // The other side of the threshold, and the dangerous one: reclaiming a running ingest
        // resurrects exactly the concurrent-ingest waste this guard exists to prevent.
        marker.begin(run("live-1", 1));
        java.time.Instant recent = java.time.Instant.now()
                .minusMillis(RunMarker.ABANDONED_AFTER_MS / 2);
        db.getCollection(Collections.RUN).updateOne(Filters.eq("_id", "live-1"),
                new Document("$set", new Document("startedAt", recent.toString())
                        .append("startedAtEpochMs", recent.toEpochMilli())));

        assertThrows(RunMarker.ConcurrentIngestException.class,
                () -> marker.begin(run("run-2", 2)),
                "a live ingest was reclaimed as abandoned");
    }

    @Test
    @Order(7)
    void ageIsJudgedNumericallyRatherThanByStringOrder() {
        // Instant.toString() omits the fraction when nanos are exactly zero, so
        // "…T10:00:00Z" sorts lexicographically AFTER "…T10:00:00.500Z" — a later instant
        // comparing as earlier. A reclamation driven by the string would misjudge runs rarely and
        // unrepeatably, which is the worst way for a guard to be wrong.
        marker.begin(run("edge-1", 1));
        Document stored = db.getCollection(Collections.RUN)
                .find(Filters.eq("_id", "edge-1")).first();
        assertNotNull(stored.get("startedAtEpochMs"),
                "the run carries no numeric start time, so its age can only be judged by a string "
                        + "whose ordering is not chronological");
        assertInstanceOf(Long.class, stored.get("startedAtEpochMs"),
                "the numeric start time is not stored as a number");

        // The defect itself, asserted on the values rather than through reclaimAbandoned.
        //
        // It cannot be triggered deterministically through the reclaim path: it needs nanos to be
        // exactly zero AND the same second as a cutoff two hours in the past — roughly one in a
        // billion, and unreproducible when it happens. So the property is checked where it is
        // decidable, and the reclaim path is covered by the two age tests above.
        java.time.Instant earlier = java.time.Instant.ofEpochSecond(1_800_000_000L);        // no fraction
        java.time.Instant later = earlier.plusMillis(500);                                   // 0.5s after

        assertTrue(earlier.isBefore(later), "fixture is wrong: earlier must precede later");
        assertTrue(earlier.toString().compareTo(later.toString()) > 0,
                "this assertion encodes the bug being avoided: as ISO strings the EARLIER instant "
                        + "must compare greater, because Instant.toString() drops the fraction when "
                        + "nanos are zero. If this ever fails, string comparison became safe and "
                        + "the numeric field is no longer load-bearing.");
        assertTrue(earlier.toEpochMilli() < later.toEpochMilli(),
                "epoch millis must order chronologically; that is the whole reason for the field");
    }

    @Test
    @Order(8)
    void adifferentAnalysisIsUnaffected() {
        // The guard is per entry point. Blocking unrelated analyses would serialise the whole
        // pipeline to one ingest at a time.
        marker.begin(run("run-1", 1));
        Document other = run("other-1", 1).append("entryPoint", "ERP_USP_OTHER");
        assertDoesNotThrow(() -> marker.begin(other),
                "an ingest of a different analysis was blocked");
    }

    @Test
    @Order(9)
    void onlyOneRunIsEverCurrent() {
        // The other half of the guarantee, and the one that was already indexed. Publishing a
        // second run must demote the first rather than leaving two current.
        marker.begin(run("run-1", 1));
        marker.complete("run-1", "ORACLE", "DEMO", "LOCAL", "ERP_USP_RUN_CYCLE");
        marker.begin(run("run-2", 2));
        marker.complete("run-2", "ORACLE", "DEMO", "LOCAL", "ERP_USP_RUN_CYCLE");

        assertEquals(1, db.getCollection(Collections.RUN).countDocuments(
                        Filters.and(Filters.eq("isCurrent", true),
                                Filters.eq("entryPoint", "ERP_USP_RUN_CYCLE"))),
                "more than one current run for one entry point");
        assertEquals(RunMarker.SUPERSEDED, db.getCollection(Collections.RUN)
                        .find(Filters.eq("_id", "run-1")).first().getString("status"),
                "the previous run was not demoted");
    }
}
