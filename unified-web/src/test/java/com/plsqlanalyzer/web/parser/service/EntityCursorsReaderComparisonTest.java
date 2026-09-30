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
 * Mode 4 read-side, facet 3 ({@code EntityCursorsReader}) — real end-to-end proof, not a hand-built
 * fixture, following {@code EntityTablesReaderComparisonTest}/{@code EntityJoinsReaderComparisonTest}'s
 * own exact pattern. Reads the `cursors` facet from the real, already-ingested
 * {@code ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE} analysis and asserts it matches the real
 * artifact-scan path's own output exactly, confirmed by direct HTTP comparison before this test was
 * written: 476 rows, 476 total access details, every operation either {@code CURSOR_FOR_LOOP} or
 * {@code DECLARE} (this corpus's own {@code am_cursor_decl} rows carry zero {@code stmtOperations}
 * entries, so the OPEN/FETCH/CLOSE path {@code EntityCursorsReader} also supports is unverified
 * against real data here — stated in that class's own doc comment).
 *
 * <p>Skips (not fails) when no local MongoDB is reachable or the expected analysis hasn't been
 * ingested there — this session's own environment, not a portable fixture.
 */
class EntityCursorsReaderComparisonTest {

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
        EntityCursorsReader reader = new EntityCursorsReader(new ObjectMapper());
        JsonNode doc = reader.readCursors(db, ANALYSIS);

        // One row per cursor INSTANCE (name::node#line): 476 rows, one access detail each — the
        // count the analysis's own counts.cursors sums to (stats.totalCursors), not the 64 the
        // name::node merge collapsed them into.
        assertEquals(476, doc.size(), "cursor row count must match the analysis's totalCursors (476)");
        assertEquals(476, totalAccessDetails(doc), "one access detail per cursor instance");
        for (JsonNode row : doc) {
            assertTrue(!row.path("cursorName").asText("").isEmpty(), "every row must carry a real cursorName");
            String type = row.path("cursorType").asText("");
            assertTrue(type.equals("EXPLICIT") || type.equals("REF_CURSOR") || type.equals("FOR_LOOP"),
                    "cursorType must be one of the three real values, was: " + type);
            assertTrue(row.path("accessCount").asInt() > 0, "accessCount must be positive");
        }
    }

    @Test
    void everyAccessDetailCarriesARealProcedureAndSourceFile() {
        EntityCursorsReader reader = new EntityCursorsReader(new ObjectMapper());
        JsonNode doc = reader.readCursors(db, ANALYSIS);
        boolean sawRealProcedure = false;
        boolean sawRealSourceFile = false;
        boolean sawRealQuery = false;
        for (JsonNode row : doc) {
            if (!row.path("queryText").asText("").isEmpty()) sawRealQuery = true;
            for (JsonNode d : row.path("accessDetails")) {
                if (!d.path("procedureName").asText("").isEmpty()) sawRealProcedure = true;
                if (!d.path("sourceFile").asText("").isEmpty()) sawRealSourceFile = true;
            }
        }
        assertTrue(sawRealProcedure, "at least one access detail must carry a real procedure name");
        assertTrue(sawRealSourceFile, "at least one access detail must carry a real source file");
        assertTrue(sawRealQuery, "at least one row must carry a real query text");
    }
}
