package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.verify.RoundTripProver;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M1 acceptance: the smallest end-to-end chain, against real data and a real server.
 *
 * <p>Exercises every mechanism the module depends on — deterministic ids, a run marker, a 1:N
 * reference, an embedded array, a unique index, a validator, idempotency, and the lossless proof —
 * on ~700 documents rather than 16,000, so a failure is diagnosable.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoIngestM1Test {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;
    private static MongoIngestService svc;
    private static String runId;
    /** Proves the suite really ran, so an all-skipped build cannot masquerade as a green one. */
    private static boolean exercised = false;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS),
                "ORACLE analysis not on disk at " + ANALYSIS.toAbsolutePath());
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase(MongoAvailable.DB);
        svc = new MongoIngestService(db);
        db.drop();                       // a fresh namespace per run; this is a test database
        svc.installSchema();
    }

    @AfterAll
    static void tearDown() {
        // Dropped here as well as in setUp: dropping only on start leaves every class's data
        // resident for the life of the mongod. Twenty-three classes accumulated 2.1 GB and the
        // OS started killing test runs.
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    @Test @Order(1)
    @DisplayName("ingest yields exactly 42 db_object and 132 table_column_usage documents")
    void ingestCounts() {
        var result = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS));
        runId = result.runId();
        exercised = true;

        // Counted through membership, not by scanning the entity collections: an entity document
        // is shared by every run with matching content, so a raw count answers "how many distinct
        // objects have ever been seen", not "how many did this run contain".
        assertEquals(42, db.getCollection("am_run_member").countDocuments(
                        Filters.and(Filters.eq("runRef", runId),
                                Filters.eq("entityCollection", Collections.DB_OBJECT))),
                "db_object membership must match api/tables/index.json");
        assertEquals(132, db.getCollection("am_run_member").countDocuments(
                        Filters.and(Filters.eq("runRef", runId),
                                Filters.eq("entityCollection", Collections.TABLE_COLUMN_USAGE))),
                "table_column_usage must match the analyser's columnCoverage.columnRows: 132");
    }

    @Test @Order(2)
    @DisplayName("the run is published exactly once and is visible as current")
    void runMarker() {
        Document run = db.getCollection(Collections.RUN).find(Filters.eq("_id", runId)).first();
        assertNotNull(run, "no run marker written");
        assertEquals("COMPLETE", run.getString("status"));
        assertEquals(Boolean.TRUE, run.getBoolean("isCurrent"));
        assertNotNull(run.getString("committedAt"), "a COMPLETE run must carry its commit time");
    }

    @Test @Order(3)
    @DisplayName("every column-usage row resolves to a real parent — no orphans")
    void relationChainHolds() {
        List<String> refs = db.getCollection(Collections.TABLE_COLUMN_USAGE)
                .distinct("objectRef", String.class).into(new java.util.ArrayList<>());
        assertFalse(refs.isEmpty(), "no refs at all — the relationship was never written");
        // Refs point at entityKey, not at _id: a content hash changes when content changes, so a
        // reference to it would break on every edit.
        long missing = refs.stream()
                .filter(r -> db.getCollection(Collections.DB_OBJECT)
                        .countDocuments(Filters.eq("entityKey", r)) == 0)
                .count();
        assertEquals(0, missing, "orphaned objectRef values: the child points at no parent");
    }

    @Test @Order(4)
    @DisplayName("every entity reaches its run in one hop, through membership")
    void reachesRunInOneHop() {
        long members = db.getCollection("am_run_member").countDocuments(Filters.eq("runRef", runId));
        assertTrue(members > 0, "no membership rows: the entities are not joined to any run");
        // Every membership row must name an entity that exists.
        long dangling = db.getCollection("am_run_member")
                .find(Filters.eq("runRef", runId)).into(new java.util.ArrayList<>())
                .stream()
                .filter(m -> db.getCollection(m.getString("entityCollection"))
                        .countDocuments(Filters.eq("entityKey", m.getString("entityRef"))) == 0)
                .count();
        assertEquals(0, dangling, "membership rows pointing at entities that do not exist");
    }

    @Test @Order(5)
    @DisplayName("nothing from tables.json was lost")
    void losslessRoundTrip() throws Exception {
        JsonNode root = new ObjectMapper().readTree(ANALYSIS.resolve("tables.json").toFile());
        int checked = 0;
        List<RoundTripProver.Loss> allLosses = new java.util.ArrayList<>();
        var it = root.fields();
        while (it.hasNext()) {
            var e = it.next();
            JsonNode cols = e.getValue().path("columns");
            if (!cols.isArray()) continue;
            for (JsonNode col : cols) {
                String name = col.path("columnName").asText(null);
                if (name == null) continue;
                Document stored = db.getCollection(Collections.TABLE_COLUMN_USAGE)
                        .find(Filters.and(
                                Filters.eq("tableName", e.getValue().path("tableName").asText(null)),
                                Filters.eq("columnName", name))).first();
                assertNotNull(stored, "column row missing entirely: " + e.getKey() + "." + name);
                // Arrays live in their own collections now, so the stored document alone is only
                // part of the answer. Reassembly is what makes "nothing was lost" a claim about
                // data rather than about a document — comparing the stripped form would compare
                // less against more and call the difference acceptable.
                Document full = new com.codeanalyzer.ingest.mongo.verify.Reassembler(db)
                        .reassemble(Collections.TABLE_COLUMN_USAGE,
                                stored.getString("key") != null
                                        ? stored.getString("key")
                                        : storedKeyOf(stored),
                                stored, List.of());
                allLosses.addAll(RoundTripProver.compare(col, full));
                checked++;
            }
        }
        assertEquals(132, checked, "expected to verify all 132 column rows");
        assertTrue(allLosses.isEmpty(), "fields lost on the way into MongoDB:\n  "
                + allLosses.stream().map(Object::toString).limit(10).reduce((a, b) -> a + "\n  ").orElse(""));
    }

    @Test @Order(6)
    @DisplayName("re-ingesting changes nothing — deterministic ids make it idempotent")
    void idempotent() {
        long objectsBefore = db.getCollection(Collections.DB_OBJECT).countDocuments();
        long colsBefore = db.getCollection(Collections.TABLE_COLUMN_USAGE).countDocuments();

        // force=true: this test proves dedup by writing the same entities twice, which the
        // ING-004 digest short-circuit would otherwise skip. Skipping is right for an operator
        // re-running an unchanged analysis; it would make the property under test unreachable.
        svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS), true);

        // THE DEDUP PROPERTY, and the reason the whole identity model exists: re-ingesting
        // identical content adds a second run's membership rows but NOT a second copy of the
        // entities. Measured on the archived runs, three of five dialects re-run byte-identically,
        // so this is the difference between ~95 GB and ~654 GB of index at ten runs.
        assertEquals(objectsBefore, db.getCollection(Collections.DB_OBJECT).countDocuments(),
                "re-ingesting identical content duplicated the entity documents — dedup is not working");
        assertEquals(colsBefore, db.getCollection(Collections.TABLE_COLUMN_USAGE).countDocuments(),
                "re-ingesting identical content duplicated the column rows");

        // And the older run must have been demoted, or the partial unique index is not doing its job.
        Document old = db.getCollection(Collections.RUN).find(Filters.eq("_id", runId)).first();
        assertNotNull(old);
        assertEquals("SUPERSEDED", old.getString("status"),
                "publishing a new run must demote the previous one");
        assertEquals(1, db.getCollection(Collections.RUN)
                .countDocuments(Filters.and(Filters.eq("isCurrent", true),
                        Filters.eq("entryPoint", "ERP_USP_RUN_CYCLE"))),
                "exactly one current run per entry point");
    }

    /**
     * An entity document carries no run-scoped key, so its children's {@code parentRef} has to be
     * recomputed from the natural key rather than read off the parent.
     */
    private static String storedKeyOf(Document stored) {
        return com.codeanalyzer.ingest.mongo.id.IdCodec.tableColumnUsage(
                stored.getString("objectRef"), stored.getString("columnName")).key();
    }

    @AfterAll
    static void mongoWasActuallyExercised() {
        // Without this, a build where mongod was absent and every test skipped is indistinguishable
        // from one where they all passed.
        if (MongoAvailable.present() && Files.isDirectory(ANALYSIS)) {
            assertTrue(exercised, "mongod and data were available but nothing was ingested");
        }
    }
}
