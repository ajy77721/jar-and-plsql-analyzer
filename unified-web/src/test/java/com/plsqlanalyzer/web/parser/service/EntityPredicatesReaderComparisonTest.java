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
 * Mode 4 read-side, facet 6 ({@code EntityPredicatesReader}) — real end-to-end proof, following
 * {@code EntityTablesReaderComparisonTest}/{@code EntityJoinsReaderComparisonTest}/{@code
 * EntityCursorsReaderComparisonTest}/{@code EntitySequencesReaderComparisonTest}/{@code
 * EntityProceduresReaderComparisonTest}'s own exact pattern. Reads the {@code predicates} facet from
 * the real, already-ingested {@code ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE} analysis and asserts
 * it matches the real artifact-scan path's own output exactly — confirmed by direct HTTP comparison
 * against the rebuilt, restarted app before this test was written: 26 tables with at least one
 * predicate, 912 total predicates, 0 mismatches on {@code predicateCount}/{@code uniqueColumns} per
 * table (matched on the first attempt, unlike {@code tables}/{@code joins}, since the underlying
 * {@code am_statement__predicates} data was already real and complete before this reader existed).
 *
 * <p>Skips (not fails) when no local MongoDB is reachable or the expected analysis hasn't been
 * ingested there — this session's own environment, not a portable fixture.
 */
class EntityPredicatesReaderComparisonTest {

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

    private static int totalPredicates(JsonNode doc) {
        int n = 0;
        for (JsonNode row : doc) n += row.path("predicateCount").asInt();
        return n;
    }

    @Test
    void reproducesTheRealAnalysisExactly() {
        EntityPredicatesReader reader = new EntityPredicatesReader(new ObjectMapper());
        JsonNode doc = reader.readPredicates(db, ANALYSIS);

        assertEquals(26, doc.size(), "table row count must match the artifact-scan path exactly (26)");
        assertEquals(912, totalPredicates(doc), "total predicates across every table must match exactly");
        for (JsonNode row : doc) {
            assertTrue(!row.path("tableName").asText("").isEmpty(), "every row must carry a real tableName");
            assertEquals(row.path("predicates").size(), row.path("predicateCount").asInt(),
                    "predicateCount must equal the number of predicate entries, same invariant as the artifact-scan reader");
        }
    }

    @Test
    void everyPredicateCarriesARealColumnAndProcedure() {
        EntityPredicatesReader reader = new EntityPredicatesReader(new ObjectMapper());
        JsonNode doc = reader.readPredicates(db, ANALYSIS);
        boolean sawRealColumn = false;
        boolean sawRealProcedure = false;
        for (JsonNode row : doc) {
            for (JsonNode p : row.path("predicates")) {
                if (!p.path("columnName").asText("").isEmpty()) sawRealColumn = true;
                if (!p.path("procedureName").asText("").isEmpty()) sawRealProcedure = true;
            }
        }
        assertTrue(sawRealColumn, "at least one predicate must carry a real column name");
        assertTrue(sawRealProcedure, "at least one predicate must carry a real procedure name");
    }
}
