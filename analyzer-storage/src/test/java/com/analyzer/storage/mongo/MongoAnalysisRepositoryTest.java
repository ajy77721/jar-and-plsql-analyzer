package com.analyzer.storage.mongo;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactKey;
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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MongoAnalysisRepository}'s five methods, the run marker, and the property the whole
 * increment rests on: <b>an answer served from the index is byte-identical to the one the scan
 * implementation would have given</b>.
 *
 * <p>The source is a recording stub rather than the real {@code ArtifactScanRepository}, which lives
 * in {@code unified-web} and would drag the whole web module in here. What is being tested is not
 * the scan implementation — it has its own 17 tests — but the three things this class adds on top:
 * that the source is consulted exactly once per run, that what comes back afterwards is unchanged,
 * and that a failed or abandoned ingest stays invisible.
 */
class MongoAnalysisRepositoryTest {

    @TempDir Path tmp;

    private MongoClient client;
    private MongoDatabase db;
    private ObjectMapper mapper;
    private ArtifactStore blobs;
    private RecordingSource source;
    private MongoAnalysisRepository repo;

    /** Named per test run, so tests cannot see each other's runs. */
    private String suffix;

    @BeforeEach
    void setUp() {
        client = MongoTestSupport.clientOrSkip();
        suffix = MongoTestSupport.freshCollection("repo");
        db = client.getDatabase(MongoTestSupport.DATABASE + "_" + suffix);
        mapper = new ObjectMapper();
        blobs = new LocalArtifactStore(tmp);
        source = new RecordingSource();
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

    private static AnalysisQuery bundledJars(String jarId) {
        return AnalysisQuery.ofJar(AnalysisQuery.Dataset.JAR_BUNDLED_JARS, jarId, null);
    }

    private static AnalysisQuery joins(String analysis) {
        return AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_JOINS, analysis);
    }

    private List<JsonNode> drain(AnalysisQuery q, Function<JsonNode, JsonNode> projection)
            throws IOException {
        List<JsonNode> out = new ArrayList<>();
        try (RowStream s = repo.rows(q, projection)) {
            while (s.hasNext()) out.add(s.next());
        }
        return out;
    }

    // ── indexed() ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("indexed() is true — the one thing that distinguishes the two implementations")
    void indexedIsTrue() {
        assertTrue(repo.indexed());
        assertFalse(source.indexed(), "the source must be the scanning one");
    }

    // ── rows: ingest then serve ─────────────────────────────────────────────

    @Test
    @DisplayName("first rows() call ingests from the source; the second is served from the index")
    void rowsIngestOnceThenServeFromIndex() throws IOException {
        source.rows = List.of("{\"name\":\"a\",\"size\":1}", "{\"name\":\"b\",\"size\":2}");
        AnalysisQuery q = bundledJars("demo");

        List<JsonNode> first = drain(q, null);
        assertEquals(2, first.size());
        assertEquals(1, source.rowCalls.get(), "the source should have been read once");
        assertEquals(2, repo.storedRowCount(q), "the rows were not persisted");

        List<JsonNode> second = drain(q, null);
        assertEquals(1, source.rowCalls.get(), "the second call must NOT re-read the source");
        assertEquals(first, second, "the indexed answer differs from the scanned one");
    }

    @Test
    @DisplayName("the indexed rows are byte-identical to the source's, field order included")
    void indexedRowsAreByteIdentical() throws IOException {
        // The acceptance criterion Increment C established and Increment D must not spend. Field
        // ORDER is the part a BSON round trip would be most likely to change, so the assertion is on
        // the serialised string rather than on node equality — JsonNode.equals ignores order.
        String[] originals = {
                "{\"z\":1,\"a\":2,\"m\":{\"nested\":[1,2,3]}}",
                "{\"big\":123456789012345,\"float\":1.5,\"nul\":null,\"t\":true}",
                "{\"dotted.field\":\"v\",\"$dollar\":\"w\"}"
        };
        source.rows = List.of(originals);
        AnalysisQuery q = bundledJars("exact");

        drain(q, null);                                    // ingest
        List<JsonNode> served = drain(q, null);            // from the index

        assertEquals(originals.length, served.size());
        for (int i = 0; i < originals.length; i++) {
            assertEquals(originals[i], mapper.writeValueAsString(served.get(i)),
                    "row " + i + " changed on the way through MongoDB");
        }
    }

    @Test
    @DisplayName("a projection is applied on the way out, not baked into the stored row")
    void projectionIsNotStoredApplied() throws IOException {
        source.rows = List.of("{\"name\":\"a\",\"drop\":\"me\"}");
        AnalysisQuery q = bundledJars("proj");

        Function<JsonNode, JsonNode> nameOnly =
                n -> mapper.createObjectNode().put("name", n.get("name").asText());

        List<JsonNode> ingested = drain(q, nameOnly);
        assertEquals("{\"name\":\"a\"}", mapper.writeValueAsString(ingested.get(0)));

        // A different screen wants the whole row, and must get it from the index rather than
        // triggering a re-scan because the first screen happened to project.
        List<JsonNode> whole = drain(q, null);
        assertEquals(1, source.rowCalls.get());
        assertEquals("{\"name\":\"a\",\"drop\":\"me\"}", mapper.writeValueAsString(whole.get(0)));
    }

    @Test
    @DisplayName("a projection returning null drops the row, per the interface")
    void nullProjectionDropsRows() throws IOException {
        source.rows = List.of("{\"keep\":true}", "{\"keep\":false}", "{\"keep\":true}");
        AnalysisQuery q = bundledJars("drop");
        drain(q, null);

        List<JsonNode> kept = new ArrayList<>();
        try (RowStream s = repo.rows(q, n -> n.get("keep").asBoolean() ? n : null)) {
            while (s.hasNext()) {
                JsonNode row = s.next();
                if (row != null) kept.add(row);
            }
        }
        assertEquals(2, kept.size());
    }

    @Test
    @DisplayName("an empty source produces a COMPLETE run with zero rows, not a permanent re-scan")
    void emptySourceStillCompletesTheRun() throws IOException {
        source.rows = List.of();
        AnalysisQuery q = bundledJars("empty");

        assertEquals(0, drain(q, null).size());
        assertEquals(0, repo.storedRowCount(q), "storedRowCount should be 0, meaning a run exists");
        assertEquals(1, repo.completeRunCount());

        assertEquals(0, drain(q, null).size());
        assertEquals(1, source.rowCalls.get(), "an empty result must not be re-scanned every time");
    }

    // ── the run marker ───────────────────────────────────────────────────────

    @Test
    @DisplayName("an abandoned row stream leaves the run IN_PROGRESS, and therefore invisible")
    void abandonedIngestStaysInvisible() throws IOException {
        // The failure PLAN-06 names: a 5,000-node crawl that fails at 3,000 must not look complete.
        // Here the caller reads one row of three and stops, which is the same shape.
        source.rows = List.of("{\"i\":0}", "{\"i\":1}", "{\"i\":2}");
        AnalysisQuery q = bundledJars("partial");

        try (RowStream s = repo.rows(q, null)) {
            assertTrue(s.hasNext());
            s.next();                       // one row only, then close
        }

        assertEquals(0, repo.completeRunCount(), "a partial ingest must not publish a COMPLETE run");
        assertEquals(-1, repo.storedRowCount(q), "there must be no COMPLETE run to count rows for");
        assertEquals(1, db.getCollection(MongoAnalysisRepository.C_RUNS)
                .countDocuments(Filters.eq("status", MongoAnalysisRepository.IN_PROGRESS)));

        // And the next query re-ingests from the start rather than serving the partial rows.
        assertEquals(3, drain(q, null).size());
        assertEquals(2, source.rowCalls.get());
        assertEquals(1, repo.completeRunCount());
    }

    @Test
    @DisplayName("the sweeper removes abandoned runs and their orphaned rows")
    void sweeperRemovesIncompleteRuns() throws IOException {
        source.rows = List.of("{\"i\":0}", "{\"i\":1}");
        try (RowStream s = repo.rows(bundledJars("sweep"), null)) {
            s.next();
        }
        assertEquals(1, repo.sweepIncompleteRuns(0));
        assertEquals(0, db.getCollection(MongoAnalysisRepository.C_RUNS).countDocuments());
        assertEquals(0, db.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments());
    }

    @Test
    @DisplayName("the sweeper never touches a COMPLETE run")
    void sweeperSparesCompleteRuns() throws IOException {
        source.rows = List.of("{\"i\":0}");
        drain(bundledJars("keep"), null);
        assertEquals(0, repo.sweepIncompleteRuns(0));
        assertEquals(1, repo.completeRunCount());
    }

    @Test
    @DisplayName("a completed ingest retires the abandoned attempt's rows, leaving exactly one run")
    void completedIngestRetiresTheAbandonedAttempt() throws IOException {
        // The realistic re-ingest path: page 1 is abandoned, so a run is left IN_PROGRESS with some
        // rows; the next full read starts a second run. Once that one completes, the first must be
        // gone — otherwise every abandoned page-1 view leaks a partial copy of the analysis, and
        // those accumulate silently because an IN_PROGRESS run is invisible to every read.
        AnalysisQuery q = bundledJars("reingest");
        source.rows = List.of("{\"v\":1}", "{\"v\":2}", "{\"v\":3}");
        try (RowStream s = repo.rows(q, null)) {
            s.next();                      // abandon after one row
        }
        assertEquals(1, db.getCollection(MongoAnalysisRepository.C_RUNS).countDocuments());

        List<JsonNode> after = drain(q, null);

        assertEquals(3, after.size());
        assertEquals(1, repo.completeRunCount(), "two COMPLETE runs for one key is the ambiguity "
                + "the marker scheme exists to prevent");
        assertEquals(1, db.getCollection(MongoAnalysisRepository.C_RUNS).countDocuments(),
                "the abandoned run's marker was not retired");
        assertEquals(3, db.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments(),
                "the abandoned run's rows were not retired");
    }

    @Test
    @DisplayName("rows whose run marker is gone are collected — the second orphan class")
    void sweeperRemovesRowsWithNoRunMarker() throws IOException {
        // Unreachable through every read path, because rows are only ever found via a run id, so
        // without this they would sit there forever. Same shape as the orphaned GridFS chunks whose
        // collector had to be written against the chunks rather than the .files documents.
        AnalysisQuery q = bundledJars("orphanrows");
        source.rows = List.of("{\"v\":1}", "{\"v\":2}");
        drain(q, null);
        assertEquals(2, db.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments());

        db.getCollection(MongoAnalysisRepository.C_RUNS).deleteMany(new Document());

        assertTrue(repo.sweepIncompleteRuns(0) > 0);
        assertEquals(0, db.getCollection(MongoAnalysisRepository.C_ROWS).countDocuments());
    }

    // ── document() ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("document() ingests then serves, byte-identically")
    void documentIngestOnceThenServeFromIndex() throws IOException {
        source.document = "{\"joins\":[{\"a\":1}],\"count\":1}";
        AnalysisQuery q = joins("ORACLE~Demo~local~PKG");

        JsonNode first = repo.document(q);
        assertEquals(source.document, mapper.writeValueAsString(first));
        assertEquals(1, source.documentCalls.get());

        JsonNode second = repo.document(q);
        assertEquals(1, source.documentCalls.get(), "the second call must not re-read the source");
        assertEquals(source.document, mapper.writeValueAsString(second));
    }

    @Test
    @DisplayName("a facet the source does not have stays absent — not turned into an empty object")
    void absentDocumentIsPreservedAsAbsent() throws IOException {
        source.document = null;
        AnalysisQuery q = joins("ORACLE~Demo~local~NOTHING");

        assertNull(repo.document(q));
        assertEquals(1, repo.completeRunCount(), "the absence should still be recorded as a run");
        assertNull(repo.document(q), "the recorded absence must read back as an absence");
        assertEquals(1, source.documentCalls.get());
    }

    @Test
    @DisplayName("table-scoped facets for one analysis do not collide")
    void tableScopedFacetsAreKeyedSeparately() throws IOException {
        String analysis = "ORACLE~Demo~local~PKG";
        source.document = "{\"table\":\"EMP\"}";
        AnalysisQuery emp = AnalysisQuery.ofTable(
                AnalysisQuery.Dataset.PLSQL_TABLE_JOINS, analysis, "EMP");
        assertEquals("{\"table\":\"EMP\"}", mapper.writeValueAsString(repo.document(emp)));

        source.document = "{\"table\":\"DEPT\"}";
        AnalysisQuery dept = AnalysisQuery.ofTable(
                AnalysisQuery.Dataset.PLSQL_TABLE_JOINS, analysis, "DEPT");
        assertEquals("{\"table\":\"DEPT\"}", mapper.writeValueAsString(repo.document(dept)));

        // And EMP is unchanged — the second ingest must not have replaced it.
        assertEquals("{\"table\":\"EMP\"}", mapper.writeValueAsString(repo.document(emp)));
        assertEquals(2, source.documentCalls.get());
    }

    // ── the guard, through the repository ───────────────────────────────────

    @Test
    @DisplayName("an oversized row is diverted to blob storage and still reads back whole")
    void oversizedRowIsDivertedThroughTheRepository() throws IOException {
        // End to end: the guard is on the repository's write path, so no producer can bypass it.
        // A tiny limit stands in for 12 MB; MongoDocumentGuardTest exercises the real one.
        MongoAnalysisRepository small = new MongoAnalysisRepository(
                db, mapper, source, new MongoDocumentGuard(blobs, 256));
        String big = "{\"src\":\"" + "y".repeat(2_000) + "\"}";
        source.rows = List.of("{\"small\":1}", big);
        AnalysisQuery q = bundledJars("oversize");

        List<JsonNode> ingested = new ArrayList<>();
        try (RowStream s = small.rows(q, null)) {
            while (s.hasNext()) ingested.add(s.next());
        }
        assertEquals(2, ingested.size());

        // One row inline, one diverted — and the diverted one is a real artifact.
        List<Document> stored = new ArrayList<>();
        db.getCollection(MongoAnalysisRepository.C_ROWS).find().into(stored);
        assertEquals(2, stored.size());
        assertEquals(1, stored.stream().filter(MongoDocumentGuard::diverted).count(),
                "exactly one row should have been diverted");
        Document diverted = stored.stream().filter(MongoDocumentGuard::diverted).findFirst().orElseThrow();
        assertNotNull(diverted.getString(MongoDocumentGuard.F_BLOB_KEY));
        assertTrue(blobs.exists(MongoDocumentGuard.keyOf(
                diverted.getString(MongoDocumentGuard.F_BLOB_KEY))));

        // And it reads back whole, from the index, with the caller unable to tell.
        List<JsonNode> served = new ArrayList<>();
        try (RowStream s = small.rows(q, null)) {
            while (s.hasNext()) served.add(s.next());
        }
        assertEquals(big, mapper.writeValueAsString(served.get(1)));
    }

    // ── delegated methods ───────────────────────────────────────────────────

    @Test
    @DisplayName("analyses() is delegated — the store, not this index, is the authority on what exists")
    void analysesIsDelegated() throws IOException {
        source.analyses = List.of(
                new AnalysisRef("ORACLE~Demo~local~A", ArtifactKey.of("plsql-parse", "a"), 200L),
                new AnalysisRef("ORACLE~Demo~local~B", ArtifactKey.of("plsql-parse", "b"), 100L));
        List<AnalysisRef> refs = repo.analyses(AnalysisQuery.Dataset.PLSQL_ANALYSES);
        assertEquals(source.analyses, refs);
        assertEquals(1, source.analysesCalls.get());
    }

    @Test
    @DisplayName("exists() is delegated — 'no bundled JARs' and 'filter matched nothing' stay different answers")
    void existsIsDelegated() {
        source.exists = true;
        assertTrue(repo.exists(bundledJars("x")));
        source.exists = false;
        assertFalse(repo.exists(bundledJars("x")));
        assertEquals(2, source.existsCalls.get());
    }

    @Test
    @DisplayName("a source is required at construction — a repository that can only serve pre-loaded runs is a trap")
    void aSourceIsRequired() {
        assertThrows(IllegalArgumentException.class,
                () -> new MongoAnalysisRepository(db, blobs, mapper, null));
        assertThrows(IllegalArgumentException.class,
                () -> new MongoAnalysisRepository(null, blobs, mapper, source));
    }

    /**
     * A scan-implementation stand-in that records how often it was asked.
     *
     * <p>The call counts are the point: "served from the index" is not observable from the returned
     * data — that is the whole guarantee — so the only way to assert it is to prove the source was
     * not consulted.
     */
    private final class RecordingSource implements AnalysisRepository {

        List<String> rows = List.of();
        String document;
        List<AnalysisRef> analyses = List.of();
        boolean exists;

        final AtomicInteger rowCalls = new AtomicInteger();
        final AtomicInteger documentCalls = new AtomicInteger();
        final AtomicInteger analysesCalls = new AtomicInteger();
        final AtomicInteger existsCalls = new AtomicInteger();

        @Override
        public boolean indexed() {
            return false;
        }

        @Override
        public List<AnalysisRef> analyses(AnalysisQuery.Dataset dataset) {
            analysesCalls.incrementAndGet();
            return analyses;
        }

        @Override
        public boolean exists(AnalysisQuery query) {
            existsCalls.incrementAndGet();
            return exists;
        }

        @Override
        public JsonNode document(AnalysisQuery query) throws IOException {
            documentCalls.incrementAndGet();
            return document == null ? null : mapper.readTree(document);
        }

        @Override
        public RowStream rows(AnalysisQuery query, Function<JsonNode, JsonNode> projection) {
            rowCalls.incrementAndGet();
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
