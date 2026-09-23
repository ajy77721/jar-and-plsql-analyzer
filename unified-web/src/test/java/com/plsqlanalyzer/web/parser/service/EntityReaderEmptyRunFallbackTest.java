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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * §267 regression — an {@code am_run} that is current but holds no rows must make the entity
 * readers return {@code null} (so the controller falls back to the artifact scan), NOT an empty
 * document (which the controller serves as the answer).
 *
 * <h2>The bug this pins</h2>
 *
 * <p>Live, on SQLSERVER only: {@code /analyses/SQLSERVER~SQLSERVERDEMO~LOCAL~USP_RUNMRPCYCLE/
 * procedures} returned {@code []} and {@code /tables/page} returned {@code {"tables":[]}} while the
 * same analysis's {@code /index} correctly reported 55 nodes and 68 tables, and
 * {@code ?source=artifact} returned all of them. Cause: two runs existed — {@code v1}
 * ({@code isCurrent: true, status: COMPLETE}) with <b>zero</b> {@code am_proc_node} and
 * {@code am_node_table_access} rows, and {@code v2} which held the real 55/68 but had died on an
 * {@code E11000} during ingest and so was never promoted. The readers found v1, found no rows, and
 * returned an empty document — which reads as a legitimate "this analysis has nothing" and
 * suppresses the fallback that exists for exactly this case.
 *
 * <p>The second test also pins the SQL-Server-specific global-temp-table naming ({@code ##g103})
 * through the reader, because that naming was the first hypothesis for a dialect-only bug and must
 * stay demonstrably harmless: a {@code #}/{@code ##} prefix is data, not syntax, at every layer
 * here.
 */
class EntityReaderEmptyRunFallbackTest {

    private static final String DB_NAME = "unifiedweb_entity_fallback_test";
    private static final String ANALYSIS = "SQLSERVER~SQLSERVERDEMO~LOCAL~USP_RUNMRPCYCLE";
    private static final String RUN_ID = "SQLSERVER~SqlServerDemo~local~USP_RUNMRPCYCLE~v1";

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void connect() {
        try {
            client = MongoClients.create("mongodb://localhost:27017");
            client.getDatabase("admin").runCommand(new Document("ping", 1));
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "No MongoDB at localhost:27017 — skipping: " + e);
        }
        db = client.getDatabase(DB_NAME);
        db.drop();
        // Deliberately the only document in the whole database: a current, COMPLETE run with no
        // entity rows behind it. This is the exact live shape, not an invented one.
        db.getCollection("am_run").insertOne(new Document("_id", RUN_ID)
                .append("key", RUN_ID)
                .append("dialect", "SQLSERVER")
                .append("project", "SqlServerDemo")
                .append("environment", "local")
                .append("entryPoint", "USP_RUNMRPCYCLE")
                .append("version", 1)
                .append("isCurrent", true)
                .append("status", "COMPLETE"));
    }

    @AfterAll
    static void disconnect() {
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    @Test
    @DisplayName("a current run with no rows yields null from every entity reader, not an empty answer")
    void emptyCurrentRunFallsBack() {
        ObjectMapper m = new ObjectMapper();
        assertNull(new EntityProceduresReader(m).readProcedures(db, ANALYSIS),
                "procedures: an empty result is served as the answer and blanks the grid — the "
                        + "reader must return null so the controller falls back to the artifact scan");
        assertNull(new EntityTablesReader(m).readTables(db, ANALYSIS), "tables");
        assertNull(new EntityJoinsReader(m).readJoins(db, ANALYSIS), "joins");
        assertNull(new EntityCursorsReader(m).readCursors(db, ANALYSIS), "cursors");
        assertNull(new EntitySequencesReader(m).readSequences(db, ANALYSIS), "sequences");
        assertNull(new EntityPredicatesReader(m).readPredicates(db, ANALYSIS), "predicates");
        assertNull(new EntityDynamicSqlReader(m).readDynamicSql(db, ANALYSIS), "dynamicSql");
    }

    @Test
    @DisplayName("a ## global temp table name survives the tables reader verbatim")
    void doubleHashTempTableNameIsData() {
        String nodeRef = "SQLSERVER~ERP$USP_RUNMRPCYCLE";
        db.getCollection("am_proc_node").insertOne(new Document()
                .append("runId", RUN_ID)
                .append("entityKey", nodeRef)
                .append("nodeId", "ERP$USP_RUNMRPCYCLE")
                .append("name", "USP_RUNMRPCYCLE")
                .append("schema", "ERP")
                .append("objectType", "PROCEDURE")
                .append("depth", 0));
        for (String name : List.of("##g103", "#local_tmp", "MRP_DEMAND")) {
            db.getCollection("am_node_table_access").insertOne(new Document()
                    .append("runId", RUN_ID)
                    .append("key", nodeRef + "~ERP~" + name)
                    .append("schema", "ERP")
                    .append("name", name)
                    .append("objectType", "TABLE")
                    .append("nodeRef", nodeRef)
                    .append("objectRef", "ERP~" + name));
        }
        try {
            JsonNode doc = new EntityTablesReader(new ObjectMapper()).readTables(db, ANALYSIS);
            assertNotNull(doc, "rows exist, so this must no longer fall back");
            assertEquals(3, doc.path("tables").size(), "no table may be dropped for its name");
            boolean sawDoubleHash = false, sawSingleHash = false;
            for (JsonNode t : doc.path("tables")) {
                if ("##g103".equals(t.path("name").asText())) sawDoubleHash = true;
                if ("#local_tmp".equals(t.path("name").asText())) sawSingleHash = true;
            }
            assertEquals(true, sawDoubleHash, "the ## global-temp name must come back byte-for-byte");
            assertEquals(true, sawSingleHash, "the # local-temp name must come back byte-for-byte");
        } finally {
            db.getCollection("am_node_table_access").drop();
            db.getCollection("am_proc_node").drop();
        }
    }
}
