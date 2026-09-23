package com.analyzer.storage.mongo;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simultaneous ingests of the same query — the case the run-marker scheme was designed for and the
 * one nothing had ever run.
 *
 * <p>The marker scheme's whole claim is that it gives the visible atomicity of a transaction on a
 * standalone MongoDB, which cannot offer one. That claim was argued in prose and tested
 * sequentially: one ingest, one abandonment, one re-ingest. Sequential tests cannot fail the way
 * this scheme fails. Two ingests of the same query starting before either finishes is the shape
 * that <b>did</b> break it — both inserted {@code IN_PROGRESS}, both flipped to {@code COMPLETE},
 * and for a window the database held two COMPLETE runs for one key, which is precisely the
 * ambiguity {@code status=COMPLETE} filtering is supposed to remove. Whichever one a reader's
 * {@code findOne} happened to hit is the answer it got.
 *
 * <p>What makes it correct now is a <b>partial unique index</b> on
 * {@code (dataset, analysis, version, table)} scoped to {@code status=COMPLETE}
 * ({@code run_one_complete}), which turns the marker flip into a single-document compare-and-set the
 * server arbitrates. Every assertion below is about that: exactly one COMPLETE run, every row
 * present exactly once, and the loser of the race returning normally rather than throwing at a user
 * who did nothing wrong.
 *
 * <p>These tests are the reason the constraint is not just documented. They fail — with two COMPLETE
 * runs and duplicated rows — against the index specification that shipped.
 */
class MongoRepositoryConcurrencyTest {

    /** Enough rows that the ingests genuinely overlap rather than serialising by luck. */
    private static final int ROWS = 400;
    private static final int THREADS = 6;

    @TempDir Path tmp;

    private MongoClient client;
    private MongoDatabase db;
    private ObjectMapper mapper;
    private ArtifactStore blobs;
    private SlowSource source;
    private MongoAnalysisRepository repo;

    @BeforeEach
    void setUp() {
        client = MongoTestSupport.clientOrSkip();
        db = client.getDatabase(MongoTestSupport.DATABASE + "_" + MongoTestSupport.freshCollection("conc"));
        mapper = new ObjectMapper();
        blobs = new LocalArtifactStore(tmp);
        source = new SlowSource();
        repo = new MongoAnalysisRepository(db, blobs, mapper, source);
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

    // ── the index itself ─────────────────────────────────────────────────────

    @Test
    @DisplayName("run_one_complete exists, is unique, and is scoped to COMPLETE")
    void theConstraintIsReal() {
        // Asserted on the index metadata, not inferred from behaviour, because "two COMPLETE runs
        // never happened in this test run" is exactly what the old code also demonstrated.
        Document spec = null;
        for (Document d : db.getCollection(MongoAnalysisRepository.C_RUNS).listIndexes()) {
            if ("run_one_complete".equals(d.getString("name"))) spec = d;
        }
        assertNotNull(spec, "the unique index that enforces one COMPLETE run per key is missing");
        assertEquals(Boolean.TRUE, spec.getBoolean("unique"),
                "the index exists but is not unique, so it enforces nothing");
        Document partial = spec.get("partialFilterExpression", Document.class);
        assertNotNull(partial, "without a partial filter the index would also forbid two "
                + "concurrent IN_PROGRESS runs, which are legal");
        assertEquals(MongoAnalysisRepository.COMPLETE, partial.getString("status"));
        assertEquals(new Document("dataset", 1).append("analysis", 1)
                        .append("version", 1).append("table", 1),
                spec.get("key", Document.class),
                "the unique key must be the run key without status");
    }

    @Test
    @DisplayName("the server refuses a second COMPLETE run for one key, directly")
    void serverRefusesASecondCompleteRun() {
        Document a = new Document("_id", "a").append("dataset", "JAR_BUNDLED_JARS")
                .append("analysis", "x").append("version", "").append("table", null)
                .append("status", MongoAnalysisRepository.COMPLETE);
        Document b = new Document(a).append("_id", "b");
        db.getCollection(MongoAnalysisRepository.C_RUNS).insertOne(a);
        assertThrows(com.mongodb.MongoWriteException.class,
                () -> db.getCollection(MongoAnalysisRepository.C_RUNS).insertOne(b),
                "the invariant is not enforced by the database");

        // …and any number of IN_PROGRESS runs for the same key are still fine, which is what the
        // partial filter buys and what concurrent ingests require.
        Document p1 = new Document(a).append("_id", "p1")
                .append("status", MongoAnalysisRepository.IN_PROGRESS);
        Document p2 = new Document(a).append("_id", "p2")
                .append("status", MongoAnalysisRepository.IN_PROGRESS);
        db.getCollection(MongoAnalysisRepository.C_RUNS).insertOne(p1);
        db.getCollection(MongoAnalysisRepository.C_RUNS).insertOne(p2);
        assertEquals(2, db.getCollection(MongoAnalysisRepository.C_RUNS)
                .countDocuments(Filters.eq("status", MongoAnalysisRepository.IN_PROGRESS)));
    }

    // ── rows(): the real thing, concurrently ─────────────────────────────────

    @Test
    @DisplayName("six simultaneous rows() ingests of one query: exactly one COMPLETE run, no lost or duplicated rows")
    void concurrentRowIngestsProduceExactlyOneCompleteRun() throws Exception {
        AnalysisQuery query = q("hot");
        List<List<JsonNode>> results = inParallel(THREADS, () -> {
            List<JsonNode> out = new ArrayList<>();
            try (RowStream s = repo.rows(query, null)) {
                while (s.hasNext()) out.add(s.next());
            }
            return out;
        });

        // 1. Every caller got the whole, correct answer. A race that is invisible to the user is the
        //    minimum bar: nobody should see a short read because someone else was also reading.
        for (List<JsonNode> result : results) {
            assertEquals(ROWS, result.size(), "a concurrent reader got a short answer");
            for (int i = 0; i < ROWS; i++) {
                assertEquals(i, result.get(i).get("i").asInt(), "rows arrived out of order");
            }
        }

        // 2. Exactly one COMPLETE run. This is the assertion that fails without the unique index.
        assertEquals(1, repo.completeRunCount(),
                "two COMPLETE runs for one key is the ambiguity the marker scheme exists to prevent");

        // 3. No lost and no duplicated rows: the surviving run holds each seq exactly once. Counting
        //    documents alone would pass on a run that held row 7 twice and row 8 never.
        assertEquals(ROWS, repo.storedRowCount(query));
        assertSeqsAreExactlyZeroTo(ROWS, soleCompleteRunId());

        // 4. No garbage left behind by the losers. Their rows are deleted by whoever notices first —
        //    the winner's retire, or the loser's own discard — so the collection holds one run's
        //    worth and not six.
        assertEquals(ROWS, db.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments(),
                "a losing ingest left its rows behind");
        assertEquals(1, db.getCollection(MongoAnalysisRepository.C_RUNS).countDocuments(),
                "a losing ingest left its marker behind");
    }

    @Test
    @DisplayName("after the race, the next read is served from the index and the source is not consulted again")
    void theSurvivingRunIsUsable() throws Exception {
        AnalysisQuery query = q("afterwards");
        inParallel(THREADS, () -> {
            try (RowStream s = repo.rows(query, null)) {
                while (s.hasNext()) s.next();
            }
            return null;
        });
        int scansDuringRace = source.rowCalls.get();
        assertTrue(scansDuringRace >= 2, "the ingests did not actually overlap; the test proves nothing");

        List<JsonNode> served = new ArrayList<>();
        try (RowStream s = repo.rows(query, null)) {
            while (s.hasNext()) served.add(s.next());
        }
        assertEquals(ROWS, served.size());
        assertEquals(scansDuringRace, source.rowCalls.get(),
                "the surviving run was not usable, so the query re-scanned");
    }

    @Test
    @DisplayName("the loser of a race does not throw — the user did not cause it and must not see it")
    void theLoserDoesNotSurfaceAnError() throws Exception {
        // inParallel rethrows, so this passing at all is the assertion. Stated as its own test
        // because "no exception" is the requirement, not a side effect: the losing ingest has
        // already handed its caller every row, byte-identically, and a duplicate-key error escaping
        // here would turn a correct read into a 500 for a race between two users.
        AnalysisQuery query = q("noerror");
        List<Integer> sizes = inParallel(THREADS, () -> {
            int n = 0;
            try (RowStream s = repo.rows(query, null)) {
                while (s.hasNext()) {
                    s.next();
                    n++;
                }
            }
            return n;
        });
        for (int n : sizes) assertEquals(ROWS, n);
        assertEquals(1, repo.completeRunCount());
    }

    // ── document(): the same race on the facet path ──────────────────────────

    @Test
    @DisplayName("simultaneous document() ingests: one COMPLETE run, one facet, identical bytes to every caller")
    void concurrentDocumentIngestsProduceExactlyOneCompleteRun() throws Exception {
        source.document = "{\"joins\":[{\"a\":1},{\"b\":2}],\"count\":2}";
        AnalysisQuery query = AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_JOINS, "ORACLE~D~local~PKG");

        List<String> seen = inParallel(THREADS, () -> mapper.writeValueAsString(repo.document(query)));

        for (String s : seen) {
            assertEquals(mapper.writeValueAsString(mapper.readTree(source.document)), s,
                    "a concurrent caller got different bytes");
        }
        assertEquals(1, repo.completeRunCount());
        assertEquals(1, db.getCollection(MongoAnalysisRepository.C_RUNS).countDocuments());
        assertEquals(1, db.getCollection(MongoAnalysisRepository.C_FACETS).countDocuments(),
                "a losing ingest left its facet document behind");
        // And the run that survived is the one the facet belongs to — not a marker pointing at
        // nothing, which would read as "this analysis has no such facet" for an analysis that does.
        assertNotNull(repo.document(query));
    }

    @Test
    @DisplayName("concurrent ingests of DIFFERENT queries all publish — the constraint is per key, not global")
    void differentQueriesDoNotContend() throws Exception {
        // The mirror-image failure: a unique index on too few fields would serialise unrelated
        // analyses and silently retire one behind the other. Six distinct ids, six COMPLETE runs.
        List<Integer> sizes = inParallelIndexed(THREADS, i -> {
            int n = 0;
            try (RowStream s = repo.rows(q("distinct-" + i), null)) {
                while (s.hasNext()) {
                    s.next();
                    n++;
                }
            }
            return n;
        });
        for (int n : sizes) assertEquals(ROWS, n);
        assertEquals(THREADS, repo.completeRunCount());
        assertEquals((long) THREADS * ROWS,
                db.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private String soleCompleteRunId() {
        Document d = db.getCollection(MongoAnalysisRepository.C_RUNS)
                .find(Filters.eq("status", MongoAnalysisRepository.COMPLETE)).first();
        assertNotNull(d);
        return d.getString("_id");
    }

    /** Every seq from 0..n-1 present exactly once: the "no lost, no duplicated rows" assertion. */
    private void assertSeqsAreExactlyZeroTo(int n, String runId) {
        Set<Long> seen = new HashSet<>();
        long count = 0;
        for (Document d : db.getCollection(MongoAnalysisRepository.C_ROWS)
                .find(Filters.eq("runId", runId))) {
            count++;
            assertTrue(seen.add(d.get("seq", Number.class).longValue()),
                    "duplicated row seq " + d.get("seq"));
        }
        assertEquals(n, count);
        for (long i = 0; i < n; i++) {
            assertTrue(seen.contains(i), "lost row seq " + i);
        }
    }

    private <T> List<T> inParallel(int threads, Callable<T> body) throws Exception {
        return inParallelIndexed(threads, i -> body.call());
    }

    /**
     * Starts {@code threads} tasks and releases them from one latch.
     *
     * <p>The latch matters: without it the first thread submitted would usually finish its whole
     * ingest before the last one started, and the test would pass against code with no constraint
     * at all. Any exception is rethrown, so "the loser does not throw" is enforced rather than
     * hoped for.
     */
    private <T> List<T> inParallelIndexed(int threads, IndexedCallable<T> body) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int index = i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return body.call(index);
                }));
            }
            assertTrue(ready.await(30, TimeUnit.SECONDS), "threads never started");
            go.countDown();
            List<T> out = new ArrayList<>();
            for (Future<T> f : futures) out.add(f.get(120, TimeUnit.SECONDS));
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    private interface IndexedCallable<T> {
        T call(int index) throws Exception;
    }

    /**
     * A source that yields rows slowly enough for the ingests to interleave.
     *
     * <p>Thread-safe and stateless per stream, unlike the sequential tests' stub: each call gets its
     * own iterator, because the point is several ingests reading the same content at once.
     */
    private final class SlowSource implements AnalysisRepository {

        String document;
        final AtomicInteger rowCalls = new AtomicInteger();
        final AtomicInteger documentCalls = new AtomicInteger();

        @Override public boolean indexed() { return false; }

        @Override public List<AnalysisRef> analyses(AnalysisQuery.Dataset dataset) {
            return Collections.emptyList();
        }

        @Override public boolean exists(AnalysisQuery query) { return true; }

        @Override
        public JsonNode document(AnalysisQuery query) throws IOException {
            documentCalls.incrementAndGet();
            // A pause on the read side, so several callers are inside document() at once.
            sleepABit();
            return document == null ? null : mapper.readTree(document);
        }

        @Override
        public RowStream rows(AnalysisQuery query, Function<JsonNode, JsonNode> projection) {
            rowCalls.incrementAndGet();
            Iterator<Integer> it = java.util.stream.IntStream.range(0, ROWS).boxed().iterator();
            return new RowStream() {
                @Override public boolean hasNext() { return it.hasNext(); }

                @Override
                public JsonNode next() {
                    if (!it.hasNext()) throw new NoSuchElementException();
                    int i = it.next();
                    if (i % 64 == 0) sleepABit();
                    try {
                        JsonNode row = mapper.readTree("{\"i\":" + i + ",\"pad\":\""
                                + "x".repeat(64) + "\"}");
                        return projection == null ? row : projection.apply(row);
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }

                @Override public void close() { }
            };
        }

        private void sleepABit() {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
