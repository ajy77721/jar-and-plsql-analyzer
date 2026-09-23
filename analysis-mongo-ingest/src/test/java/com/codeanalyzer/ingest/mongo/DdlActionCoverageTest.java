package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-019/§144's own follow-up — is a dedicated {@code am_ddl_action} mapper actually needed, or
 * does {@code StatementsMapper} already capture a DDL-classified statement generically, the way it
 * captures every other operation?
 *
 * <p>{@code StatementsMapper.emit()} was read in full before writing this: nothing in it branches
 * on the {@code operation} field's value at all — {@code nodeId}/{@code operation}/{@code line}/
 * {@code schema}/{@code tableName}/{@code sqlText} are handled identically whichever string
 * {@code operation} holds. Checked {@code docs/plan/PLAN-04-ENTITY-MODEL.md} directly too: it has
 * no {@code am_ddl_action} entity at all — every "DDL" mention there is the source-text type, not a
 * collection name. So the premise this test checks is that a DDL statement is already an
 * {@code am_statement} row with {@code operation: "DDL"}, not a document that needs a new,
 * separate collection nobody's own plan ever specified.
 */
class DdlActionCoverageTest {

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_ddl_action_test");
        db.drop();
    }

    @AfterAll
    static void tearDown() {
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    /**
     * A minimal, hand-built fixture — one statement row shaped exactly the way
     * {@code ChunkedFlowWriter}'s own §122/§138 fix now writes a dynamic-SQL DDL statement:
     * {@code operation: "DDL"}, a real {@code sqlText}, no joins/columns (a bare {@code CREATE
     * TABLE}). Every other artifact this ingest reads is absent, and {@code MongoIngestService}'s
     * own loop skips an absent artifact rather than failing — confirmed by reading its `ingest()`
     * loop before relying on that.
     */
    @Test
    void aDdlClassifiedStatementIsCapturedByTheExistingStatementsMapperWithNoNewCode(@TempDir Path root)
            throws IOException {
        Path stmts = root.resolve("api/statements");
        Files.createDirectories(stmts);
        Files.writeString(stmts.resolve("index.json"), """
            {
              "statements": [
                {
                  "nodeId": "ANALYZER$ERP_USP_PROVISION_STAGING",
                  "nodeName": "ERP_USP_PROVISION_STAGING",
                  "schema": "ANALYZER",
                  "tableName": "ERP_STAGE_TMP",
                  "objectType": "TABLE",
                  "operation": "DDL",
                  "line": 42,
                  "sqlText": "CREATE TABLE erp_stage_tmp (id NUMBER)"
                }
              ]
            }
            """);
        writeNode(root, "ANALYZER$ERP_USP_PROVISION_STAGING", "ERP_USP_PROVISION_STAGING");

        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        String runId = svc.ingest("ORACLE", "DdlFixtureProj", "LOCAL", "ERP_USP_PROVISION_STAGING",
                new FileArtifactSource(root)).runId();

        Document ddlStatement = db.getCollection(Collections.STATEMENT)
                .find(Filters.and(Filters.eq("runId", runId), Filters.eq("operation", "DDL")))
                .first();
        assertNotNull(ddlStatement,
                "a DDL-operation row must land in am_statement like any other operation — "
                        + "StatementsMapper does not special-case operation values");
        assertEquals("ERP_STAGE_TMP", ddlStatement.getString("tableName"));
        assertEquals(42, ddlStatement.getInteger("line"));

        // sqlText moves to its own content-addressed collection, same as every other statement's.
        String sqlTextRef = ddlStatement.getString("sqlTextRef");
        assertNotNull(sqlTextRef, "DDL's sqlText must be content-addressed the same way DML's is");
        Document text = db.getCollection(Collections.SQL_TEXT)
                .find(Filters.eq("entityKey", sqlTextRef)).first();
        assertNotNull(text);
        assertEquals("CREATE TABLE erp_stage_tmp (id NUMBER)", text.getString("text"));

        // The negative half of the premise: no am_ddl_action collection is written at all, because
        // none is meant to exist — confirms this isn't accidentally passing by writing to both.
        assertEquals(0, db.listCollectionNames().into(new java.util.ArrayList<>()).stream()
                .filter(n -> n.equals("am_ddl_action")).count(),
                "no am_ddl_action collection should exist — DDL rows live in am_statement");
    }

    /**
     * Calibration for the assertion above: if {@code StatementsMapper} ever *did* special-case
     * {@code operation.equals("DDL")} to skip or divert the row, this test would need to fail
     * loudly rather than pass by accident. Proven by checking the row count is exactly 1 for this
     * single-statement fixture — a silently-dropped or silently-duplicated DDL row would show here.
     */
    @Test
    void exactlyOneStatementRowExistsForTheOneStatementInTheFixture(@TempDir Path root) throws IOException {
        Path stmts = root.resolve("api/statements");
        Files.createDirectories(stmts);
        Files.writeString(stmts.resolve("index.json"), """
            {
              "statements": [
                {
                  "nodeId": "ANALYZER$X", "nodeName": "X", "schema": "ANALYZER",
                  "tableName": "Y", "operation": "DDL", "line": 1,
                  "sqlText": "CREATE TABLE y (id NUMBER)"
                }
              ]
            }
            """);
        writeNode(root, "ANALYZER$X", "X");
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        String runId = svc.ingest("ORACLE", "DdlFixtureProj2", "LOCAL", "X",
                new FileArtifactSource(root)).runId();
        long n = db.getCollection(Collections.STATEMENT).countDocuments(Filters.eq("runId", runId));
        assertEquals(1, n, "one statement in, one am_statement row out — no dropping, no fan-out");
    }

    /** A minimal node file — just enough for {@code am_proc_node} to exist so the statement's
     *  {@code nodeRef} resolves under {@code assertReferencesResolve}'s check. */
    private static void writeNode(Path root, String nodeId, String name) throws IOException {
        Path nodes = root.resolve("api/nodes");
        Files.createDirectories(nodes);
        Files.writeString(nodes.resolve(name + ".json"), """
            { "nodeId": "%s", "name": "%s", "schema": "ANALYZER", "objectType": "PROCEDURE" }
            """.formatted(nodeId, name));
    }
}
