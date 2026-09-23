package com.analyzer.storage.mongo;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MongoDB going away in the middle of an ingest, and what a reader sees afterwards.
 *
 * <p>PLAN-06's failure section is written entirely about this case — "a 5,000-node crawl writing
 * 5,000 documents can fail at 3,000" — and it had never been made to happen. The sequential tests
 * cover a caller <em>abandoning</em> a stream, which is a cooperative stop: every Mongo call
 * succeeded. This covers the uncooperative one, where a write throws part way through and the
 * process that would have tidied up is the one that failed.
 *
 * <h2>How the failure is injected, and why not by killing the server</h2>
 *
 * <p>Two mechanisms, neither of which stops the real {@code mongod}:
 *
 * <ol>
 *   <li><b>Closing the client mid-ingest.</b> A closed {@link MongoClient} fails every subsequent
 *       operation from inside the driver, at the same call site a vanished server would. Used where
 *       the question is "what state is left behind", because it can be triggered at an exact row.
 *       Note the one place it is <em>not</em> faithful: the driver reports a call on a closed client
 *       as {@code IllegalStateException}, not as a {@code MongoException}, so a test asserting how a
 *       driver failure is wrapped must not use it.</li>
 *   <li><b>Pointing a connection at a port nothing is listening on</b>, which produces a real
 *       {@code MongoTimeoutException}. Used where the exception type is the assertion.</li>
 * </ol>
 *
 * <p>Killing and restarting the real server was deliberately not done. The MongoDB on this machine
 * is a shared development instance holding other databases; a test that stops a process it does not
 * own is a test that occasionally leaves a developer's machine broken, and it cannot run on CI at
 * all. What killing the process would add over closing the client is coverage of the driver's
 * reconnect and retry behaviour — driver behaviour, not this code's. The property under test is
 * "when a write throws, is a partial ingest visible", and a thrown write is a thrown write.
 *
 * <h2>What is asserted</h2>
 *
 * <p>That the failure is <b>reported</b> (not swallowed into a silently short answer), that the
 * partial run is <b>invisible</b> to every read because its marker never reached {@code COMPLETE},
 * that a later read <b>re-ingests correctly</b> rather than serving the fragment, and that the
 * fragment is <b>reclaimed</b> by the sweeper once the server is back.
 */
class MongoIngestFailureTest {

    @TempDir Path tmp;

    private MongoClient client;
    private MongoDatabase db;
    private String dbName;
    private ObjectMapper mapper;
    private ArtifactStore blobs;
    private FailingSource source;
    private MongoAnalysisRepository repo;

    @BeforeEach
    void setUp() {
        client = MongoTestSupport.clientOrSkip();
        dbName = MongoTestSupport.DATABASE + "_" + MongoTestSupport.freshCollection("fail");
        db = client.getDatabase(dbName);
        mapper = new ObjectMapper();
        blobs = new LocalArtifactStore(tmp);
        source = new FailingSource();
        repo = new MongoAnalysisRepository(db, blobs, mapper, source);
        repo.ensureIndexes();
    }

    @AfterEach
    void tearDown() {
        // The client under test may have been closed by the injection, so clean up with a fresh one.
        try (MongoClient cleanup = MongoTestSupport.clientOrSkip()) {
            cleanup.getDatabase(dbName).drop();
        } catch (RuntimeException ignored) {
            // best effort
        }
        try {
            if (client != null) client.close();
        } catch (RuntimeException ignored) {
            // already closed by the test
        }
    }

    private static AnalysisQuery q(String id) {
        return AnalysisQuery.ofJar(AnalysisQuery.Dataset.JAR_BUNDLED_JARS, id, null);
    }

    // ── the connection dies part way through a row ingest ────────────────────

    @Test
    @DisplayName("Mongo dies mid-ingest: the read fails loudly, and the partial run is invisible")
    void connectionLostMidIngestLeavesNothingVisible() throws IOException {
        AnalysisQuery query = q("midflight");
        source.rowCount = 5_000;
        // Small batches so several have been acknowledged before the failure — this must be a
        // genuinely PARTIAL ingest, not one that failed before writing anything. That is the state
        // PLAN-06 names and the only one where "invisible rather than corrupt" means something.
        source.killAfter = 2_500;
        source.onKill = () -> client.close();

        List<JsonNode> received = new ArrayList<>();
        // The failure is surfaced, not swallowed. A caller that silently received 2,500 of 5,000
        // rows and no error is the worst possible outcome — a truncated analysis that looks whole.
        assertThrows(RuntimeException.class, () -> {
            try (RowStream s = repo.rows(query, null)) {
                while (s.hasNext()) received.add(s.next());
            }
        });
        assertTrue(received.size() >= 1 && received.size() < 5_000,
                "the injection did not produce a partial ingest; it saw " + received.size());

        // Everything below runs on a NEW connection: the point is what a different reader, or this
        // process after a restart, sees in the database that was left behind.
        try (MongoClient reader = MongoTestSupport.clientOrSkip()) {
            MongoDatabase fresh = reader.getDatabase(dbName);
            assertEquals(0, fresh.getCollection(MongoAnalysisRepository.C_RUNS)
                            .countDocuments(Filters.eq("status", MongoAnalysisRepository.COMPLETE)),
                    "a partial ingest published a COMPLETE run");
            assertEquals(1, fresh.getCollection(MongoAnalysisRepository.C_RUNS)
                            .countDocuments(Filters.eq("status", MongoAnalysisRepository.IN_PROGRESS)),
                    "the marker should be left IN_PROGRESS, which is what makes the run invisible");
            long strandedRows = fresh.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments();
            assertTrue(strandedRows > 0, "precondition: rows were written before the failure");

            // The invisibility is a property of the READ path, so assert it through the read path
            // and not only by counting markers.
            FailingSource quiet = new FailingSource();
            quiet.rowCount = 5_000;
            MongoAnalysisRepository reopened =
                    new MongoAnalysisRepository(fresh, blobs, mapper, quiet);
            List<JsonNode> after = new ArrayList<>();
            try (RowStream s = reopened.rows(query, null)) {
                while (s.hasNext()) after.add(s.next());
            }
            assertEquals(5_000, after.size(), "the re-read served the fragment instead of re-ingesting");
            assertEquals(1, quiet.rowCalls, "the fragment was treated as a usable run");
            assertEquals(1, reopened.completeRunCount());
            // …and the fragment is gone: the completing ingest retired the abandoned run.
            assertEquals(5_000, fresh.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments(),
                    "the stranded rows from the failed ingest were not reclaimed");
        }
    }

    @Test
    @DisplayName("the stranded fragment is reclaimed by the sweeper even if nothing re-reads the query")
    void theSweeperReclaimsAFailedIngest() {
        AnalysisQuery query = q("stranded");
        // Above BATCH_MAX_ROWS (2,000), so at least one batch is acknowledged before the kill.
        // Below it nothing would have reached Mongo at all and the test would assert on an empty
        // database — which is itself worth knowing: a small failed ingest strands nothing.
        source.rowCount = 5_000;
        source.killAfter = 2_500;
        source.onKill = () -> client.close();

        assertThrows(RuntimeException.class, () -> {
            try (RowStream s = repo.rows(query, null)) {
                while (s.hasNext()) s.next();
            }
        });

        try (MongoClient reader = MongoTestSupport.clientOrSkip()) {
            MongoDatabase fresh = reader.getDatabase(dbName);
            MongoAnalysisRepository reopened =
                    new MongoAnalysisRepository(fresh, blobs, mapper, new FailingSource());
            assertTrue(fresh.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments() > 0);
            // Age zero: the sweeper is being asked to treat everything as old. In production the
            // threshold is an hour, which is what stops it collecting a write still in flight.
            assertTrue(reopened.sweepIncompleteRuns(0) > 0);
            assertEquals(0, fresh.getCollection(MongoAnalysisRepository.C_RUNS).countDocuments());
            assertEquals(0, fresh.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments());
        }
    }

    @Test
    @DisplayName("an age threshold protects an ingest that is merely slow, not dead")
    void theSweeperWillNotCollectAFreshRun() {
        // The inverse hazard and the more dangerous one: a sweeper that cannot tell "still being
        // written" from "abandoned" and guesses wrong deletes live data. With any non-zero
        // threshold, a run created moments ago is off limits.
        AnalysisQuery query = q("slowbutalive");
        source.rowCount = 5_000;
        source.killAfter = 2_500;
        source.onKill = () -> client.close();
        assertThrows(RuntimeException.class, () -> {
            try (RowStream s = repo.rows(query, null)) {
                while (s.hasNext()) s.next();
            }
        });

        try (MongoClient reader = MongoTestSupport.clientOrSkip()) {
            MongoDatabase fresh = reader.getDatabase(dbName);
            MongoAnalysisRepository reopened =
                    new MongoAnalysisRepository(fresh, blobs, mapper, new FailingSource());
            long before = fresh.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments();
            assertEquals(0, reopened.sweepIncompleteRuns(TimeUnit.HOURS.toMillis(1)),
                    "the sweeper collected a run created seconds ago");
            assertEquals(before, fresh.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments());
        }
    }

    // ── the document()/facet path ────────────────────────────────────────────

    @Test
    @DisplayName("a facet ingest against a dead server reports IOException and publishes nothing")
    void facetIngestFailureIsReported() {
        // Deliberately a genuinely unreachable server rather than a closed client. Closing the
        // client is the better simulation for a connection lost mid-stream, but it is the WRONG one
        // here: the driver answers a call on a closed client with IllegalStateException("state
        // should be: open"), which is a programming error, not a server failure. A dead server
        // answers with MongoTimeoutException — a MongoException — which is what ingest() wraps into
        // the IOException the AnalysisRepository interface declares. Asserting the wrapping is the
        // point: a caller must not have to catch a driver type to handle "Mongo is down".
        StubSource quiet = new StubSource("{\"a\":1}");
        try (MongoClient dead = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://127.0.0.1:1"))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1, TimeUnit.SECONDS))
                .build())) {
            MongoAnalysisRepository broken =
                    new MongoAnalysisRepository(dead.getDatabase("nope"), blobs, mapper, quiet);
            AnalysisQuery query =
                    AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_JOINS, "ORACLE~D~local~P");

            IOException e = assertThrows(IOException.class, () -> broken.document(query));
            assertTrue(e.getMessage().contains("ingest"), e.getMessage());
            assertNotNull(e.getCause(), "the driver failure must be preserved as the cause");
        }
        // Nothing landed in the real database either — the failure was not partially applied
        // somewhere else.
        assertEquals(0, db.getCollection(MongoAnalysisRepository.C_FACETS).countDocuments());
        assertEquals(0, db.getCollection(MongoAnalysisRepository.C_RUNS).countDocuments());
    }

    @Test
    @DisplayName("the connection dropping between the marker and the facet leaves no COMPLETE run")
    void facetIngestInterruptedLeavesNothingVisible() {
        // The other half: the marker WAS written to the real server, and then the world ended. The
        // exception type is the driver's business; what matters is that nothing is published and
        // the marker is left IN_PROGRESS for the sweeper.
        source.document = "{\"a\":1}";
        source.failDocumentIngest = true;
        AnalysisQuery query = AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_JOINS, "ORACLE~D~local~Q");

        assertThrows(RuntimeException.class, () -> repo.document(query));

        try (MongoClient reader = MongoTestSupport.clientOrSkip()) {
            MongoDatabase fresh = reader.getDatabase(dbName);
            assertEquals(0, fresh.getCollection(MongoAnalysisRepository.C_RUNS)
                    .countDocuments(Filters.eq("status", MongoAnalysisRepository.COMPLETE)),
                    "a failed facet ingest published a COMPLETE run");
            assertEquals(0, fresh.getCollection(MongoAnalysisRepository.C_FACETS).countDocuments());
        }
    }

    // ── unreachable before anything starts ───────────────────────────────────

    @Test
    @DisplayName("an unreachable server fails the read rather than answering emptily")
    void unreachableServerFailsLoudly() {
        // 1 is a privileged port nothing listens on; the driver's server selection gives up quickly
        // with the short timeout below rather than stalling the build.
        try (MongoClient dead = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://127.0.0.1:1"))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1, TimeUnit.SECONDS))
                .build())) {
            MongoAnalysisRepository broken = new MongoAnalysisRepository(
                    dead.getDatabase("nope"), blobs, mapper, new FailingSource());
            // ensureIndexes swallows and warns, deliberately — index creation is not the read path.
            broken.ensureIndexes();
            assertThrows(RuntimeException.class, () -> {
                try (RowStream s = broken.rows(q("dead"), null)) {
                    while (s.hasNext()) s.next();
                }
            });
        }
    }

    @Test
    @DisplayName("MongoConnection.open refuses an unreachable server at boot, naming the property to fix")
    void unreachableServerIsRefusedAtBoot() {
        // PLAN-09 §8 R1: a green startup must not hide a storage failure. Restated here beside the
        // mid-flight failures because the two together are the whole failure story — one is caught
        // before any user sees it, the other cannot be.
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> MongoConnection.open("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=800",
                        "nope"));
        assertTrue(e.getMessage().contains("app.storage.mongodb.uri"), e.getMessage());
    }

    // ── a partially-written artifact is not a partially-visible artifact ─────

    @Test
    @DisplayName("a GridFS write killed mid-stream leaves the previous artifact intact and the chunks collectable")
    void abandonedGridFsWriteIsInvisibleAndCollectable() throws IOException {
        // The store-level twin of the repository failure, and the one the author flagged: an
        // abandoned large upload writes CHUNKS with no .files document, so GridFSBucket.find()
        // cannot see it and a sweeper written against the bucket API alone would leave it forever.
        String collection = MongoTestSupport.freshCollection("failstore");
        MongoArtifactStore store = new MongoArtifactStore(db, "root", 1024, collection, 100_000L);
        com.analyzer.storage.ArtifactKey key = com.analyzer.storage.ArtifactKey.of("big.json");

        try (java.io.OutputStream o = store.write(key)) {
            o.write("first".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        // Now start a large write and never close it: past 1 KB it has rolled into GridFS and, past
        // 255 KB, has durable chunks under an ObjectId nothing references.
        java.io.OutputStream abandoned = store.write(key);
        abandoned.write(new byte[512 * 1024]);
        abandoned.flush();

        // Visibility is the manifest write in close(), so the previous content is untouched.
        try (java.io.InputStream in = store.read(key)) {
            assertEquals("first", new String(in.readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8));
        }
        assertEquals(5, store.size(key));

        long chunks = db.getCollection(collection + "_fs.chunks").countDocuments();
        assertTrue(chunks > 0, "precondition: the abandoned write left durable chunks");
        assertEquals(0, db.getCollection(collection + "_fs.files").countDocuments(),
                "precondition: no .files document, which is why bucket.find() cannot see this");

        // And the sweep finds them anyway, without touching the live artifact.
        assertEquals(1, store.collectOrphanedGridFsFiles(0));
        assertEquals(0, db.getCollection(collection + "_fs.chunks").countDocuments());
        try (java.io.InputStream in = store.read(key)) {
            assertEquals("first", new String(in.readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8));
        }
        MongoTestSupport.drop(db, collection);
    }

    /** A source with nothing to fail: used where the FAILURE is the server, not the source. */
    private final class StubSource implements AnalysisRepository {
        private final String document;

        StubSource(String document) {
            this.document = document;
        }

        @Override public boolean indexed() { return false; }

        @Override public List<AnalysisRef> analyses(AnalysisQuery.Dataset dataset) {
            return Collections.emptyList();
        }

        @Override public boolean exists(AnalysisQuery query) { return true; }

        @Override public JsonNode document(AnalysisQuery query) throws IOException {
            return mapper.readTree(document);
        }

        @Override public RowStream rows(AnalysisQuery query, Function<JsonNode, JsonNode> p) {
            return new RowStream() {
                @Override public boolean hasNext() { return false; }
                @Override public JsonNode next() { throw new NoSuchElementException(); }
                @Override public void close() { }
            };
        }
    }

    /**
     * A source that yields rows until told to break the world.
     *
     * <p>{@code onKill} runs once, after {@code killAfter} rows have been handed over — so the
     * ingest has genuinely written some and genuinely fails on the next flush, rather than failing
     * before it started.
     */
    private final class FailingSource implements AnalysisRepository {

        int rowCount;
        int killAfter = Integer.MAX_VALUE;
        Runnable onKill;
        String document;
        boolean failDocumentIngest;
        int rowCalls;

        @Override public boolean indexed() { return false; }

        @Override public List<AnalysisRef> analyses(AnalysisQuery.Dataset dataset) {
            return Collections.emptyList();
        }

        @Override public boolean exists(AnalysisQuery query) { return true; }

        @Override
        public JsonNode document(AnalysisQuery query) throws IOException {
            if (failDocumentIngest) {
                // Kill on the way OUT of the source, so ingest() has already inserted its marker
                // and the failure lands on the facet write.
                client.close();
            }
            return document == null ? null : mapper.readTree(document);
        }

        @Override
        public RowStream rows(AnalysisQuery query, Function<JsonNode, JsonNode> projection) {
            rowCalls++;
            return new RowStream() {
                int i;
                boolean killed;

                @Override public boolean hasNext() { return i < rowCount; }

                @Override
                public JsonNode next() {
                    if (i >= rowCount) throw new NoSuchElementException();
                    if (!killed && i >= killAfter && onKill != null) {
                        killed = true;
                        onKill.run();
                    }
                    try {
                        return mapper.readTree("{\"i\":" + (i++) + ",\"pad\":\""
                                + "y".repeat(128) + "\"}");
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }

                @Override public void close() { }
            };
        }
    }
}
