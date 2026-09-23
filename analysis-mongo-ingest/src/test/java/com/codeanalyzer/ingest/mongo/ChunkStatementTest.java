package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-022 — the chunk files are the fidelity source, and the api index is a projection of them.
 *
 * <p>Counts measured from the artifacts on disk: 3,832 statements across 52 chunk files against
 * 2,436 rows in {@code api/statements/index.json}. The gap is not detail — the api index has one
 * row per (statement, table) pair, so a statement touching no table is absent from it entirely.
 * Every {@code IF}, {@code FOR_LOOP}, {@code CASE} and {@code ASSIGNMENT} is in that category.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ChunkStatementTest {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    /** Measured, not planned. */
    private static final long EXPECTED_CHUNK_STATEMENTS = 3832;

    private static MongoClient client;
    private static MongoDatabase db;
    private static String runId;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_chunkstmt_test");
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
    void everyChunkStatementIsStored() {
        long n = db.getCollection(Collections.CHUNK_STATEMENT)
                .countDocuments(Filters.eq("runId", runId));
        assertEquals(EXPECTED_CHUNK_STATEMENTS, n,
                "the chunk files hold " + EXPECTED_CHUNK_STATEMENTS + " statements");
    }

    @Test
    @Order(2)
    void twoStatementsOnOneLineBothSurvive() {
        // The reason ordinal is part of the key. An assignment inside a single-line IF shares a
        // line with it; keyed on line alone one would upsert over the other and vanish silently.
        var byNodeAndLine = db.getCollection(Collections.CHUNK_STATEMENT).aggregate(List.of(
                new Document("$match", new Document("runId", runId)),
                new Document("$group", new Document("_id",
                        new Document("n", "$nodeRef").append("l", "$line"))
                        .append("c", new Document("$sum", 1))),
                new Document("$match", new Document("c", new Document("$gt", 1))),
                new Document("$count", "shared")));
        Document d = byNodeAndLine.first();
        long sharedLines = d == null ? 0 : d.getInteger("shared", 0);
        assertTrue(sharedLines > 0,
                "no line holds more than one statement, so this fixture cannot prove the ordinal "
                        + "is load-bearing — the assertion would pass with a line-only key");
    }

    @Test
    @Order(3)
    void theFieldsTheApiIndexDoesNotCarryArePresent() {
        // The point of the collection. Each of these is absent from api/statements/index.json.
        for (String field : List.of("statementType", "nestingDepth", "lineEnd")) {
            long n = db.getCollection(Collections.CHUNK_STATEMENT).countDocuments(
                    Filters.and(Filters.eq("runId", runId), Filters.exists(field, true)));
            assertTrue(n > 0, "no chunk statement carries " + field
                    + ", which exists only in the chunk files");
        }
    }

    @Test
    @Order(4)
    void controlFlowStatementsExistOnlyHere() {
        // These touch no table, so the api index — one row per (statement, table) — cannot hold
        // them. If they are missing, "which statements are inside this loop" is unanswerable.
        List<String> missing = new ArrayList<>();
        for (String type : List.of("IF", "FOR_LOOP", "ASSIGNMENT", "CASE")) {
            if (db.getCollection(Collections.CHUNK_STATEMENT).countDocuments(
                    Filters.and(Filters.eq("runId", runId),
                            Filters.eq("statementType", type))) == 0) {
                missing.add(type);
            }
        }
        assertEquals(List.of(), missing, "control-flow statement types absent from the store");
    }

    @Test
    @Order(5)
    void itHoldsStrictlyMoreThanTheApiProjection() {
        long chunks = db.getCollection(Collections.CHUNK_STATEMENT)
                .countDocuments(Filters.eq("runId", runId));
        long api = db.getCollection(Collections.STATEMENT)
                .countDocuments(Filters.eq("runId", runId));
        assertTrue(chunks > api,
                "the fidelity source holds " + chunks + " rows and the projection " + api
                        + "; the fidelity source is not more complete, so it is not one");
    }

    @Test
    @Order(6)
    void everyRowIsReachableFromItsProcedure() {
        long orphans = db.getCollection(Collections.CHUNK_STATEMENT).countDocuments(
                Filters.and(Filters.eq("runId", runId),
                        Filters.or(Filters.exists("nodeRef", false), Filters.eq("nodeRef", null))));
        assertEquals(0, orphans, "chunk statements with no nodeRef are unreachable");
    }

    @Test
    @Order(7)
    void nestedArraysWereSplitOutRatherThanStored() {
        // `tables` is an array on the chunk statement. The no-arrays invariant applies here as
        // everywhere else, and a collection added later is exactly where it tends to be forgotten.
        Document withArray = db.getCollection(Collections.CHUNK_STATEMENT)
                .find(Filters.and(Filters.eq("runId", runId),
                        Filters.type("tables", "array"))).first();
        assertNull(withArray, "a chunk statement still stores its tables array inline");
    }
}
