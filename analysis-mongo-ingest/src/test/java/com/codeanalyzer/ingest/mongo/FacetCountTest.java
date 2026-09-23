package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.write.FacetCounter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB-106 — the facet panel is served by an indexed read, and the numbers are the real ones.
 *
 * <p>Two independent risks. A precomputed count that is <em>wrong</em> is worse than a slow correct
 * one, because nothing about the panel reveals it — so the counts are checked against the
 * {@code $group} they replace, on real data. And a count that is right but still read by scanning
 * has bought nothing, so the query plan is asserted too.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FacetCountTest {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;
    private static String runId;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_facet_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS)).runId();
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
    void ingestLeavesFacetCountsBehind() {
        long n = db.getCollection(Collections.FACET_COUNT)
                .countDocuments(Filters.eq("runId", runId));
        assertTrue(n > 0, "no facet counts were computed, so every panel still needs a $group");
    }

    @Test
    @Order(2)
    void theCountsAgreeWithTheGroupTheyReplace() {
        // The whole risk. A precomputed number that drifts from the data is invisible to the reader
        // and to every integrity check — only recomputing it can tell.
        for (String field : List.of("operation", "schemaName", "tableName")) {
            Map<String, Integer> live = new HashMap<>();
            for (Document g : db.getCollection(Collections.STATEMENT).aggregate(List.of(
                    new Document("$match", new Document("runId", runId)
                            .append(field, new Document("$ne", null))),
                    new Document("$group", new Document("_id", "$" + field)
                            .append("n", new Document("$sum", 1)))))) {
                if (g.get("_id") != null) live.put(String.valueOf(g.get("_id")), g.getInteger("n"));
            }

            Map<String, Integer> stored = new HashMap<>();
            for (Document d : new FacetCounter(db).read(runId, Collections.STATEMENT, field)) {
                stored.put(d.getString("value"), d.getInteger("count"));
            }

            assertEquals(live, stored,
                    "precomputed counts for " + field + " disagree with a live $group");
        }
    }

    @Test
    @Order(3)
    void everyDistinctValueIsKeptRatherThanTheCommonOnes() {
        // A threshold would make the panel a sample. A reader could not tell "no rows match" from
        // "too few rows to mention", which is a wrong answer wearing the clothes of a complete one.
        assertEquals(0, FacetCounter.MIN_COUNT, "facet counts are being truncated to popular values");

        long distinctOperations = db.getCollection(Collections.STATEMENT)
                .distinct("operation", Filters.eq("runId", runId), String.class)
                .into(new java.util.ArrayList<>()).size();
        long storedOperations = new FacetCounter(db)
                .read(runId, Collections.STATEMENT, "operation").size();
        assertEquals(distinctOperations, storedOperations,
                "some operation values are missing from the facet counts");
    }

    @Test
    @Order(4)
    void thePanelsReadIsAnIndexScanNotACollectionScan() {
        // Correct-but-scanning would have bought nothing. The index has to actually be chosen.
        Document explain = db.runCommand(new Document("explain",
                new Document("find", Collections.FACET_COUNT)
                        .append("filter", new Document("runId", runId)
                                .append("entityCollection", Collections.STATEMENT)
                                .append("field", "operation"))
                        .append("sort", new Document("count", -1)))
                .append("verbosity", "queryPlanner"));

        String plan = explain.toJson();
        assertTrue(plan.contains("IXSCAN"),
                "the facet read is not using an index:\n" + plan);
        assertFalse(plan.contains("\"stage\": \"COLLSCAN\""),
                "the facet read falls back to a collection scan:\n" + plan);
        // The sort must come from the index too, or the saving is undone by an in-memory sort.
        assertFalse(plan.contains("\"stage\": \"SORT\""),
                "the facet read sorts in memory rather than reading the index in order:\n" + plan);
    }

    @Test
    @Order(5)
    void countsAreScopedToTheirRun() {
        // Unlike a call edge, a facet count is a fact ABOUT a run — "this run saw 412 SELECTs".
        // Two runs that saw different numbers hold different facts, so these rows are per-run by
        // nature and must not collapse into one another.
        MongoIngestService svc = new MongoIngestService(db);
        String second = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS), true).runId();
        assertNotEquals(runId, second);

        long first = db.getCollection(Collections.FACET_COUNT)
                .countDocuments(Filters.eq("runId", runId));
        long other = db.getCollection(Collections.FACET_COUNT)
                .countDocuments(Filters.eq("runId", second));
        assertTrue(first > 0 && other > 0,
                "one run's facet counts overwrote the other's: " + first + " / " + other);
    }
}
