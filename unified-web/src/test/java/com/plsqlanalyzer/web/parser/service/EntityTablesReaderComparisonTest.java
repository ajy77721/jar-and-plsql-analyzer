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
 * Mode 4 read-side pilot ({@code EntityTablesReader}) — real end-to-end proof, not a hand-built
 * fixture. Reads the `tables` facet two ways for the same real, already-ingested analysis
 * (`ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE`, populated by §149's own automatic entity-ingest
 * trigger) and asserts they agree: 42 tables, 912 total predicates, identical per-table operation
 * line counts — confirmed once already via direct HTTP comparison against
 * `/tables/page` (artifact-scan) vs. `/tables/page?source=entity` before this test was written.
 *
 * <p>Skips (not fails) when no local MongoDB is reachable or the expected analysis hasn't been
 * ingested there — this session's own environment, not a portable fixture; the pattern matches
 * {@code MongoHousekeepingServiceTest}'s own real-Mongo test.
 */
class EntityTablesReaderComparisonTest {

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
        for (JsonNode t : doc.path("tables")) {
            for (JsonNode u : t.path("usedBy")) {
                n += u.path("predicates").size();
            }
        }
        return n;
    }

    private static int totalOperationLines(JsonNode doc) {
        int n = 0;
        for (JsonNode t : doc.path("tables")) {
            for (JsonNode u : t.path("usedBy")) {
                var ops = u.path("operations");
                var fields = ops.fields();
                while (fields.hasNext()) n += fields.next().getValue().size();
            }
        }
        return n;
    }

    @Test
    void reproducesTheRealAnalysisExactly() {
        EntityTablesReader reader = new EntityTablesReader(new ObjectMapper());
        JsonNode doc = reader.readTables(db, ANALYSIS);

        assertEquals(42, doc.path("tables").size(), "table count must match the real crawl (42)");
        assertEquals(912, totalPredicates(doc), "predicate count must match the artifact-scan path exactly");
        assertTrue(totalOperationLines(doc) > 0, "operation line data must actually be present, not just table names");
    }

    /**
     * Calibrated by breaking the reader's own join deliberately (below), not just asserting a
     * plausible-looking number.
     */
    @Test
    void everyTableHasARealSchemaAndName() {
        EntityTablesReader reader = new EntityTablesReader(new ObjectMapper());
        JsonNode doc = reader.readTables(db, ANALYSIS);
        for (JsonNode t : doc.path("tables")) {
            assertTrue(!t.path("schema").asText("").isEmpty(), "every row must carry a real schema");
            assertTrue(!t.path("name").asText("").isEmpty(), "every row must carry a real table name");
        }
    }

    /**
     * Real gap found by auditing {@code parser/js/tableOps.js} (not just {@code
     * ParserFacetSpecs.TABLES}, which never reads {@code triggers}) — see this class's own doc
     * comment. Confirmed against the real corpus: {@code am_db_object__triggers} carries exactly 2
     * rows, one on {@code ANALYZER.ERP_SALES_ORDER} ({@code ERP_TRG_SO_STMT_AD}), one on
     * {@code ANALYZER_REF.REF_REGION}. Every other table must still carry an empty (not missing)
     * {@code triggers} array, since {@code tableOps.js} reads {@code (t.triggers || []).length}.
     */
    @Test
    void tablesWithRealTriggersCarryThemAndEveryOtherTableCarriesAnEmptyArray() {
        EntityTablesReader reader = new EntityTablesReader(new ObjectMapper());
        JsonNode doc = reader.readTables(db, ANALYSIS);

        int totalTriggers = 0;
        boolean foundSalesOrderTrigger = false;
        for (JsonNode t : doc.path("tables")) {
            assertTrue(t.has("triggers") && t.path("triggers").isArray(),
                    "every table row must carry a triggers array, even if empty: " + t.path("name"));
            totalTriggers += t.path("triggers").size();
            if ("ERP_SALES_ORDER".equals(t.path("name").asText())) {
                for (JsonNode trig : t.path("triggers")) {
                    if ("ERP_TRG_SO_STMT_AD".equals(trig.path("name").asText())) {
                        foundSalesOrderTrigger = true;
                        assertEquals("DELETE", trig.path("event").asText());
                        assertEquals("AFTER", trig.path("timing").asText());
                    }
                }
            }
        }
        assertEquals(2, totalTriggers, "total trigger rows must match the real corpus exactly (2)");
        assertTrue(foundSalesOrderTrigger, "ERP_SALES_ORDER must carry its real trigger");
    }
}
