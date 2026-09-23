package com.analyzer.storage.mongo;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.CredentialGuardStore;
import com.analyzer.storage.LocalArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The diverted payloads under {@code repo-overflow/}, and the fact that until now nothing ever
 * deleted one.
 *
 * <p>{@link MongoDocumentGuard} diverts an element past the 12 MB soft limit to the
 * {@link ArtifactStore} and stores only its key. Every path that removes the referring document —
 * a retired superseded run, a swept incomplete run, the discarded loser of an ingest race — removes
 * the reference and leaves the artifact. There was no collector, so on a repeatedly re-analysed
 * codebase the overflow root grew monotonically, in the backend where it costs most: in
 * {@code mongodb} mode those bytes are GridFS chunk documents in the same database.
 *
 * <p>Content addressing is what makes the collection safe by reference rather than by ownership —
 * the key is the SHA-256 of the payload, so "no document mentions this key" is exactly "nothing can
 * read these bytes". The age threshold is the second guard, and the one that matters: a payload is
 * written before the document that references it, so there is a real window in which a live blob
 * looks unreferenced.
 */
class MongoOverflowSweepTest {

    /** Small enough to be fast, large enough that the diversion path is genuinely taken. */
    private static final int TINY_LIMIT = 256;

    @TempDir Path tmp;

    private MongoClient client;
    private MongoDatabase db;
    private ObjectMapper mapper;
    private ArtifactStore blobs;
    private StubSource source;
    private MongoAnalysisRepository repo;

    @BeforeEach
    void setUp() {
        client = MongoTestSupport.clientOrSkip();
        db = client.getDatabase(MongoTestSupport.DATABASE + "_" + MongoTestSupport.freshCollection("ovf"));
        mapper = new ObjectMapper();
        // Wrapped exactly as StorageFactory.create wraps it, so the sweep is proven to work through
        // the credential guard rather than around it (C4).
        blobs = new CredentialGuardStore(new LocalArtifactStore(tmp));
        source = new StubSource();
        repo = new MongoAnalysisRepository(db, mapper, source,
                new MongoDocumentGuard(blobs, TINY_LIMIT));
        repo.ensureIndexes();
    }

    @AfterEach
    void tearDown() {
        try {
            if (db != null) db.drop();
        } catch (RuntimeException ignored) {
            // best effort
        }
        if (client != null) client.close();
    }

    private static AnalysisQuery q(String id) {
        return AnalysisQuery.ofJar(AnalysisQuery.Dataset.JAR_BUNDLED_JARS, id, null);
    }

    private List<ArtifactKey> overflowBlobs() throws IOException {
        List<ArtifactKey> out = new ArrayList<>();
        try (Stream<ArtifactEntry> s = blobs.list(MongoAnalysisRepository.overflowRoot(), true)) {
            s.filter(e -> !e.directory()).forEach(e -> out.add(e.key()));
        }
        return out;
    }

    private void drain(AnalysisQuery query) throws IOException {
        try (RowStream s = repo.rows(query, null)) {
            while (s.hasNext()) s.next();
        }
    }

    private static String oversized(String marker) {
        return "{\"m\":\"" + marker + "\",\"pad\":\"" + "z".repeat(TINY_LIMIT * 2) + "\"}";
    }

    // ── the leak, and the collector ──────────────────────────────────────────

    @Test
    @DisplayName("a live run's diverted payloads are never collected, at any age")
    void referencedBlobsSurvive() throws IOException {
        source.rows = List.of(oversized("a"), oversized("b"));
        AnalysisQuery query = q("live");
        drain(query);

        assertEquals(2, overflowBlobs().size(), "precondition: both rows were diverted");
        // Age zero: the sweeper is being told to consider everything old enough. The only thing
        // saving these is the reference check, which is the point.
        assertEquals(0, repo.sweepUnreferencedOverflowBlobs(0));
        assertEquals(2, overflowBlobs().size());

        // And they are still readable, which is the assertion that actually matters — a sweep that
        // left the artifacts but broke the read would be worse than one that deleted them.
        List<JsonNode> served = new ArrayList<>();
        try (RowStream s = repo.rows(query, null)) {
            while (s.hasNext()) served.add(s.next());
        }
        assertEquals(2, served.size());
        assertEquals("a", served.get(0).get("m").asText());
    }

    @Test
    @DisplayName("retiring a superseded run strands its diverted payloads, and the sweeper reclaims them")
    void supersededRunsBlobsAreReclaimed() throws IOException {
        AnalysisQuery query = q("rerun");
        source.rows = List.of(oversized("v1"));
        drain(query);
        assertEquals(1, overflowBlobs().size());

        // Force a re-ingest with different content, the way a re-analysis would: drop the marker so
        // the next read ingests again, then serve new rows.
        db.getCollection(MongoAnalysisRepository.C_RUNS).drop();
        db.getCollection(MongoAnalysisRepository.C_ROWS).drop();
        source.rows = List.of(oversized("v2"));
        drain(query);

        // Both payloads on disk, only one referenced. This is the leak, reproduced.
        assertEquals(2, overflowBlobs().size(), "precondition: the old payload is still on disk");
        assertEquals(0, repo.sweepUnreferencedOverflowBlobs(TimeUnit.HOURS.toMillis(1)),
                "a payload written seconds ago must be off limits regardless of references");

        assertEquals(1, repo.sweepUnreferencedOverflowBlobs(0));
        List<ArtifactKey> left = overflowBlobs();
        assertEquals(1, left.size());

        // The survivor is the referenced one, not just "one of them".
        List<JsonNode> served = new ArrayList<>();
        try (RowStream s = repo.rows(query, null)) {
            while (s.hasNext()) served.add(s.next());
        }
        assertEquals("v2", served.get(0).get("m").asText());
    }

    @Test
    @DisplayName("an in-flight ingest's payloads are protected by AGE, not by the reference check — and that is load-bearing")
    void inFlightBlobsAreProtectedByTheAgeThresholdOnly() throws IOException {
        // This test documents the sharpest edge in the whole sweeper, found by writing it.
        //
        // A row's diverted payload is written to the ArtifactStore by guard.store() at the moment
        // the row streams past, but the row DOCUMENT sits in IngestingRowStream's batch buffer
        // until 4 MB or 2,000 rows have accumulated. So for the whole of a large ingest there are
        // payloads on disk that NO document in Mongo references yet — the reference set simply
        // cannot see them, because they are still in a client-side ArrayList.
        //
        // The reference check therefore does NOT protect an in-flight ingest. The age threshold is
        // the only thing that does, which is why MongoHousekeepingService refuses a non-positive
        // one and defaults to an hour: it must exceed the longest single ingest, not the longest
        // gap between two Mongo calls.
        source.rows = List.of(oversized("p0"), oversized("p1"), oversized("p2"));
        try (RowStream s = repo.rows(q("partial"), null)) {
            s.next();
            s.next();
        }
        assertEquals(0, repo.completeRunCount(), "precondition: the run was abandoned");
        int written = overflowBlobs().size();
        assertTrue(written >= 1, "precondition: payloads were diverted");
        assertEquals(0, db.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments(),
                "precondition: the rows are still in the batch buffer, unflushed — which is exactly "
                + "why the reference set cannot see them");

        // Production settings: nothing this young is eligible, so an ingest in flight is safe.
        assertEquals(0, repo.sweepUnreferencedOverflowBlobs(TimeUnit.HOURS.toMillis(1)),
                "the age threshold did not protect an in-flight ingest's payloads");
        assertEquals(written, overflowBlobs().size());

        // With the threshold disabled they ARE collected. Asserted rather than left implicit, so
        // that anyone tempted to set min-age-ms to zero can see what it costs.
        assertEquals(written, repo.sweepUnreferencedOverflowBlobs(0));
    }

    @Test
    @DisplayName("once the incomplete run is swept, its payloads become collectable")
    void sweepingTheRunThenTheBlobsReclaimsBoth() throws IOException {
        source.rows = List.of(oversized("g0"), oversized("g1"), oversized("g2"));
        try (RowStream s = repo.rows(q("garbage"), null)) {
            s.next();
            s.next();
        }
        int stranded = overflowBlobs().size();
        assertTrue(stranded > 0);

        // Order matters, and it is the order MongoHousekeepingService runs them in: the run sweep
        // removes the references, then the blob sweep removes what is no longer referenced. The
        // other order would leave the payloads for one whole interval.
        assertTrue(repo.sweepIncompleteRuns(0) > 0);
        assertEquals(stranded, repo.sweepUnreferencedOverflowBlobs(0));
        assertEquals(0, overflowBlobs().size());
    }

    @Test
    @DisplayName("deduplicated payloads are only collected when the LAST reference goes")
    void sharedPayloadsAreNotCollectedWhileOneRunStillNeedsThem() throws IOException {
        // Content addressing means two runs referencing identical bytes reference one artifact.
        // A per-run "delete my blobs" cleanup would delete the other run's data; a reference count
        // over the whole collection does not. This is why the collector is a sweeper and not a
        // cascade delete on retire.
        String shared = oversized("shared");
        source.rows = List.of(shared);
        drain(q("one"));
        drain(q("two"));
        assertEquals(1, overflowBlobs().size(), "precondition: the two runs share one artifact");

        db.getCollection(MongoAnalysisRepository.C_RUNS)
                .deleteMany(com.mongodb.client.model.Filters.eq("analysis", "one"));
        assertTrue(repo.sweepIncompleteRuns(0) >= 0);
        repo.sweepIncompleteRuns(0);   // removes run "one"'s now-orphaned rows

        assertEquals(0, repo.sweepUnreferencedOverflowBlobs(0),
                "the shared payload was collected while run 'two' still referenced it");
        assertEquals(1, overflowBlobs().size());
        List<JsonNode> served = new ArrayList<>();
        try (RowStream s = repo.rows(q("two"), null)) {
            while (s.hasNext()) served.add(s.next());
        }
        assertEquals("shared", served.get(0).get("m").asText());
    }

    @Test
    @DisplayName("the sweep does not touch anything outside repo-overflow/")
    void theSweepIsScopedToTheOverflowRoot() throws IOException {
        ArtifactKey elsewhere = ArtifactKey.of("plsql-parse", "some-analysis", "analysis.json");
        try (java.io.OutputStream o = blobs.write(elsewhere)) {
            o.write("not overflow".getBytes(StandardCharsets.UTF_8));
        }
        source.rows = List.of(oversized("x"));
        drain(q("scoped"));
        db.getCollection(MongoAnalysisRepository.C_ROWS).drop();

        assertEquals(1, repo.sweepUnreferencedOverflowBlobs(0));
        assertTrue(blobs.exists(elsewhere), "the sweep deleted an artifact that is not an overflow payload");
    }

    // ── the 12 MB guard, at the real boundary ────────────────────────────────

    @Test
    @DisplayName("the guard fires at exactly 12 MB + 1 byte and not at 12 MB, measured on the real limit")
    void theBoundaryIsExactlyWhereItIsDocumented() throws IOException {
        // The existing proof uses a 14 MB payload — comfortably over, so it cannot detect an
        // off-by-one, an inclusive/exclusive slip, or a limit that is really 12 MB minus framing.
        // These three payloads are built to the byte, in UTF-8, around the documented line.
        MongoDocumentGuard guard = new MongoDocumentGuard(blobs);
        int limit = guard.softLimitBytes();
        assertEquals(12 * 1024 * 1024, limit);

        String atLimit = "a".repeat(limit);
        assertEquals(limit, atLimit.getBytes(StandardCharsets.UTF_8).length);
        assertFalse(MongoDocumentGuard.diverted(guard.store(atLimit)),
                "exactly 12 MB is at the limit, not past it — the guard is documented as '<='");

        String justUnder = "a".repeat(limit - 1);
        assertFalse(MongoDocumentGuard.diverted(guard.store(justUnder)));

        String justOver = "a".repeat(limit + 1);
        var stored = guard.store(justOver);
        assertTrue(MongoDocumentGuard.diverted(stored),
                "one byte past the soft limit must divert");
        // Not truncated — the whole point. Byte-for-byte, through the store, at the real size.
        assertEquals(limit + 1L, stored.get(MongoDocumentGuard.F_LEN, Number.class).longValue());
        assertEquals(justOver, guard.load(stored));
        assertEquals(justOver.length(), guard.load(stored).length());
    }

    @Test
    @DisplayName("an oversized element written to a real Mongo row is diverted, and reads back whole")
    void oversizedRowSurvivesARealRoundTripThroughMongo() throws IOException {
        // The end-to-end version: the guard's boundary test proves the measurement, this proves the
        // document that gets inserted is small and the payload is still complete after a round trip
        // through the driver, BSON and the artifact store. A truncation would show up here as a
        // shorter string; a failure to divert would show up as a BSON size error from the server.
        MongoAnalysisRepository real = new MongoAnalysisRepository(db, mapper, source,
                new MongoDocumentGuard(blobs, MongoDocumentGuard.SOFT_LIMIT_BYTES));
        String huge = "{\"big\":\"" + "q".repeat(13 * 1024 * 1024) + "\"}";
        source.rows = List.of(huge);
        AnalysisQuery query = q("realboundary");

        try (RowStream s = real.rows(query, null)) {
            while (s.hasNext()) s.next();
        }

        var rowDoc = db.getCollection(MongoAnalysisRepository.C_ROWS).find().first();
        assertTrue(MongoDocumentGuard.diverted(rowDoc),
                "a 13 MB row was stored inline; the guard did not fire on the real path");
        // The stored document must be nowhere near the 16 MB BSON ceiling — that is what the
        // diversion is for.
        assertTrue(rowDoc.toJson().length() < 4096,
                "the row document is still large after diversion: " + rowDoc.toJson().length());

        List<JsonNode> served = new ArrayList<>();
        try (RowStream s = real.rows(query, null)) {
            while (s.hasNext()) served.add(s.next());
        }
        assertEquals(1, served.size());
        assertEquals(13 * 1024 * 1024, served.get(0).get("big").asText().length(),
                "the payload came back short — it was truncated somewhere");
    }

    private final class StubSource implements AnalysisRepository {

        List<String> rows = List.of();

        @Override public boolean indexed() { return false; }

        @Override public List<AnalysisRef> analyses(AnalysisQuery.Dataset dataset) {
            return Collections.emptyList();
        }

        @Override public boolean exists(AnalysisQuery query) { return true; }

        @Override public JsonNode document(AnalysisQuery query) { return null; }

        @Override
        public RowStream rows(AnalysisQuery query, Function<JsonNode, JsonNode> projection) {
            Iterator<String> it = rows.iterator();
            return new RowStream() {
                @Override public boolean hasNext() { return it.hasNext(); }

                @Override
                public JsonNode next() {
                    if (!it.hasNext()) throw new NoSuchElementException();
                    try {
                        JsonNode row = mapper.readTree(it.next());
                        return projection == null ? row : projection.apply(row);
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }

                @Override public void close() { }
            };
        }
    }
}
