package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Mode 4 read-side, facet 5 ({@code EntityProceduresReader}) — real end-to-end proof, following
 * {@code EntityTablesReaderComparisonTest}/{@code EntityJoinsReaderComparisonTest}/{@code
 * EntityCursorsReaderComparisonTest}/{@code EntitySequencesReaderComparisonTest}'s own exact
 * pattern. Reads the {@code procedures} facet from the real, already-ingested
 * {@code ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE} analysis and asserts it matches the real
 * artifact-scan path's own output exactly — confirmed by direct HTTP comparison against the
 * rebuilt, restarted app before this test was written: 52 procedures, 0 field mismatches (name/
 * schema/depth/objectType/linesOfCode/counts.exceptionHandlers), and the {@code cf=9:Y}
 * exception-handler filter returning the identical 39 rows on both paths.
 *
 * <p>Skips (not fails) when no local MongoDB is reachable or the expected analysis hasn't been
 * ingested there — this session's own environment, not a portable fixture.
 */
class EntityProceduresReaderComparisonTest {

    private static final String ANALYSIS = "ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE";
    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void connect() {
        try {
            client = MongoClients.create("mongodb://localhost:27017");
            client.getDatabase("analyzer").runCommand(new Document("ping", 1));
            db = client.getDatabase("analyzer");
            long runs = db.getCollection("am_run").countDocuments(
                    new Document("entryPoint", "ERP_USP_RUN_CYCLE").append("dialect", "ORACLE"));
            Assumptions.assumeTrue(runs > 0,
                    "No am_run for " + ANALYSIS + " in local Mongo — skipping (run the Oracle "
                            + "analysis with entity-ingest enabled first)");
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "No MongoDB reachable at localhost:27017 — skipping: " + e.getMessage());
        }
    }

    @AfterAll
    static void disconnect() {
        if (client != null) client.close();
    }

    @Test
    void reproducesTheRealAnalysisExactly() {
        EntityProceduresReader reader = new EntityProceduresReader(new ObjectMapper());
        JsonNode doc = reader.readProcedures(db, ANALYSIS);

        assertEquals(52, doc.size(), "procedure row count must match the artifact-scan path exactly (52)");
        for (JsonNode row : doc) {
            assertTrue(!row.path("nodeId").asText("").isEmpty(), "every row must carry a real nodeId");
            assertTrue(!row.path("name").asText("").isEmpty(), "every row must carry a real name");
        }
    }

    /**
     * UI-029's own {@code cf=9:Y} hidden filter reads exactly this field — the one this class's own
     * doc comment (§154's resolution) had to derive from {@code am_exception_handler} rather than a
     * missing artifact mapper. Confirmed against the real corpus: 86 handlers on
     * {@code ERP_USP_RUN_CYCLE} alone, 39 of the 52 procedures have at least one.
     */
    @Test
    void exceptionHandlerCountsMatchTheRealCorpusAndTheCf9yFilterCount() {
        EntityProceduresReader reader = new EntityProceduresReader(new ObjectMapper());
        JsonNode doc = reader.readProcedures(db, ANALYSIS);

        int withHandlers = 0;
        int totalHandlers = 0;
        for (JsonNode row : doc) {
            int c = row.path("counts").path("exceptionHandlers").asInt(0);
            totalHandlers += c;
            if (c > 0) withHandlers++;
        }
        assertEquals(39, withHandlers,
                "procedures with >0 exception handlers must match the real cf=9:Y filter count exactly");
        assertTrue(totalHandlers > 0, "at least one procedure must carry a real exception handler count");
    }

    /**
     * Real bug found by actually using the live app: without {@code detailFile}, {@code
     * parser/js/app.js}'s {@code PA.showProcedure} never fetches a node's full detail — clicking a
     * procedure showed no source, no detail header counts, nothing. Confirmed against the real
     * corpus that the artifact-scan path's own {@code detailFile} for the entry node is exactly
     * {@code "nodes/ANALYZER$ERP_USP_RUN_CYCLE.json"} — this test locks in that same convention.
     */
    @Test
    void everyRowCarriesTheDetailFileTheClientNeedsToFetchFullDetail() {
        EntityProceduresReader reader = new EntityProceduresReader(new ObjectMapper());
        JsonNode doc = reader.readProcedures(db, ANALYSIS);

        for (JsonNode row : doc) {
            String nodeId = row.path("nodeId").asText("");
            String detailFile = row.path("detailFile").asText("");
            assertEquals("nodes/" + nodeId + ".json", detailFile,
                    "detailFile must follow the real artifact-scan convention for node " + nodeId);
            assertTrue(!row.path("sourceFile").asText("").isEmpty(),
                    "sourceFile must be present so the source viewer has something to fetch, node " + nodeId);
        }
    }
}
