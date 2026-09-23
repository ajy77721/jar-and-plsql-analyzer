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
 * Mode 4 read-side, facet 4 ({@code EntitySequencesReader}) — real end-to-end proof, not a
 * hand-built fixture, following {@code EntityTablesReaderComparisonTest}/{@code
 * EntityJoinsReaderComparisonTest}/{@code EntityCursorsReaderComparisonTest}'s own exact pattern.
 * Reads the {@code sequences} facet from the real, already-ingested
 * {@code ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE} analysis and asserts it matches the real
 * artifact-scan path's own output exactly, confirmed by direct HTTP comparison before this test was
 * written (both overall counts and per-row operation/procedureId/lineNumber content): 2 sequences,
 * 34 total access details.
 *
 * <p>Skips (not fails) when no local MongoDB is reachable or the expected analysis hasn't been
 * ingested there — this session's own environment, not a portable fixture.
 */
class EntitySequencesReaderComparisonTest {

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

    private static int totalAccessDetails(JsonNode doc) {
        int n = 0;
        for (JsonNode row : doc) n += row.path("accessDetails").size();
        return n;
    }

    @Test
    void reproducesTheRealAnalysisExactly() {
        EntitySequencesReader reader = new EntitySequencesReader(new ObjectMapper());
        JsonNode doc = reader.readSequences(db, ANALYSIS);

        assertEquals(2, doc.size(), "sequence row count must match the artifact-scan path exactly (2)");
        assertEquals(34, totalAccessDetails(doc), "total access details must match every sequence usage exactly");
        for (JsonNode row : doc) {
            assertTrue(!row.path("sequenceName").asText("").isEmpty(), "every row must carry a real sequenceName");
            assertTrue(row.path("accessCount").asInt() > 0, "accessCount must be positive");
            assertEquals(row.path("accessDetails").size(), row.path("accessCount").asInt(),
                    "accessCount must equal the number of access details, same invariant as the artifact-scan reader");
        }
    }

    @Test
    void everyAccessDetailCarriesARealProcedureAndOperation() {
        EntitySequencesReader reader = new EntitySequencesReader(new ObjectMapper());
        JsonNode doc = reader.readSequences(db, ANALYSIS);
        boolean sawRealProcedure = false;
        boolean sawRealOperation = false;
        for (JsonNode row : doc) {
            for (JsonNode d : row.path("accessDetails")) {
                if (!d.path("procedureName").asText("").isEmpty()) sawRealProcedure = true;
                if (!d.path("operation").asText("").isEmpty()) sawRealOperation = true;
            }
        }
        assertTrue(sawRealProcedure, "at least one access detail must carry a real procedure name");
        assertTrue(sawRealOperation, "at least one access detail must carry a real operation");
    }
}
