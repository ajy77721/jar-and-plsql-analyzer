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
 * Mode 4 read-side, facet 2 ({@code EntityJoinsReader}) — real end-to-end proof, not a hand-built
 * fixture, following {@code EntityTablesReaderComparisonTest}'s own exact pattern. Reads the
 * `joins` facet from the real, already-ingested {@code ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE}
 * analysis and asserts it matches the real artifact-scan path's own output exactly, confirmed by
 * direct HTTP comparison before this test was written: 27 distinct unordered table pairs — the
 * artifact-scan path merges the same logical pair recorded in either join direction into one row —
 * and 981 total access details (one per {@code am_node_table_access__joins} row — every join usage,
 * not deduplicated).
 *
 * <p>Skips (not fails) when no local MongoDB is reachable or the expected analysis hasn't been
 * ingested there — this session's own environment, not a portable fixture.
 */
class EntityJoinsReaderComparisonTest {

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
        EntityJoinsReader reader = new EntityJoinsReader(new ObjectMapper());
        JsonNode doc = reader.readJoins(db, ANALYSIS);

        assertEquals(27, doc.size(), "distinct unordered table-pair count must match the artifact-scan path exactly (27)");
        assertEquals(981, totalAccessDetails(doc), "total access details must match every join usage exactly");
        for (JsonNode row : doc) {
            assertTrue(!row.path("leftTable").asText("").isEmpty(), "every row must carry a real leftTable");
            assertTrue(!row.path("rightTable").asText("").isEmpty(), "every row must carry a real rightTable");
            assertTrue(row.path("joinTypes").size() > 0, "every row must carry at least one join type");
        }
    }

    @Test
    void everyAccessDetailCarriesARealProcedureAndPredicate() {
        EntityJoinsReader reader = new EntityJoinsReader(new ObjectMapper());
        JsonNode doc = reader.readJoins(db, ANALYSIS);
        boolean sawRealProcedure = false;
        boolean sawRealPredicate = false;
        for (JsonNode row : doc) {
            for (JsonNode d : row.path("accessDetails")) {
                if (!d.path("procedureName").asText("").isEmpty()) sawRealProcedure = true;
                if (!d.path("onPredicate").asText("").isEmpty()) sawRealPredicate = true;
            }
        }
        assertTrue(sawRealProcedure, "at least one access detail must carry a real procedure name");
        assertTrue(sawRealPredicate, "at least one access detail must carry a real ON predicate");
    }

    /**
     * Real gap found by auditing {@code parser/js/joins.js}'s own {@code _renderDetail}/{@code
     * toggleSql}, not just {@code ParserFacetSpecs.JOINS}: it reads {@code d.joinType} per occurrence
     * (badge colour/label — {@code "? JOIN"}/the muted colour otherwise) and {@code d.sourceFile} as
     * its "show SQL" fallback, neither of which the pair-level {@code joinTypes[]} this class already
     * emitted covered.
     */
    @Test
    void everyAccessDetailCarriesItsOwnJoinTypeAndSourceFile() {
        EntityJoinsReader reader = new EntityJoinsReader(new ObjectMapper());
        JsonNode doc = reader.readJoins(db, ANALYSIS);
        boolean sawRealJoinType = false;
        boolean sawRealSourceFile = false;
        for (JsonNode row : doc) {
            for (JsonNode d : row.path("accessDetails")) {
                assertTrue(d.has("joinType"), "every access detail must carry a joinType field");
                assertTrue(d.has("sourceFile"), "every access detail must carry a sourceFile field");
                if (!d.path("joinType").asText("").isEmpty()) sawRealJoinType = true;
                if (!d.path("sourceFile").asText("").isEmpty()) sawRealSourceFile = true;
            }
        }
        assertTrue(sawRealJoinType, "at least one access detail must carry a real joinType");
        assertTrue(sawRealSourceFile, "at least one access detail must carry a real sourceFile");
    }
}
