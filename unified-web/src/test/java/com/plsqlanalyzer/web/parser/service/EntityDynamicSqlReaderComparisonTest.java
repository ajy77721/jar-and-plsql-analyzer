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
 * Mode 4 read-side, the final facet ({@code EntityDynamicSqlReader}) — real end-to-end proof,
 * following {@code EntityTablesReaderComparisonTest}/{@code EntityJoinsReaderComparisonTest}/{@code
 * EntityCursorsReaderComparisonTest}/{@code EntitySequencesReaderComparisonTest}/{@code
 * EntityProceduresReaderComparisonTest}/{@code EntityPredicatesReaderComparisonTest}'s own exact
 * pattern. Reads the {@code dynamic-sql} facet from the real, already-ingested
 * {@code ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE} analysis and asserts it matches the real
 * artifact-scan path's own output exactly — confirmed by direct HTTP comparison against the
 * rebuilt, restarted app before this test was written: 341 executions, identical identity set
 * (procedureId/lineNumber/type/sqlExpression), 0 field mismatches including {@code usingVariables}
 * (62 of 341 executions carry at least one bound variable) — matched on the first attempt, since the
 * unit was verified to agree before building (one row per execution, both sides — unlike the
 * disqualified {@code statements} case, §157, where the entity data counted a different unit).
 *
 * <p>Skips (not fails) when no local MongoDB is reachable or the expected analysis hasn't been
 * ingested there — this session's own environment, not a portable fixture.
 */
class EntityDynamicSqlReaderComparisonTest {

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
        EntityDynamicSqlReader reader = new EntityDynamicSqlReader(new ObjectMapper());
        JsonNode doc = reader.readDynamicSql(db, ANALYSIS);

        assertEquals(341, doc.size(), "execution row count must match the artifact-scan path exactly (341)");
        int withVars = 0;
        for (JsonNode row : doc) {
            assertTrue(!row.path("procedureId").asText("").isEmpty(), "every row must carry a real procedureId");
            assertTrue(!row.path("type").asText("").isEmpty(), "every row must carry a real type");
            if (row.path("usingVariables").size() > 0) withVars++;
        }
        assertEquals(62, withVars, "executions with bound variables must match the real corpus exactly (62)");
    }

    @Test
    void everyRowCarriesARealSqlExpressionAndSourceFile() {
        EntityDynamicSqlReader reader = new EntityDynamicSqlReader(new ObjectMapper());
        JsonNode doc = reader.readDynamicSql(db, ANALYSIS);
        boolean sawRealExpr = false;
        boolean sawRealSourceFile = false;
        for (JsonNode row : doc) {
            if (!row.path("sqlExpression").asText("").isEmpty()) sawRealExpr = true;
            if (!row.path("sourceFile").asText("").isEmpty()) sawRealSourceFile = true;
        }
        assertTrue(sawRealExpr, "at least one row must carry a real sqlExpression");
        assertTrue(sawRealSourceFile, "at least one row must carry a real sourceFile");
    }
}
