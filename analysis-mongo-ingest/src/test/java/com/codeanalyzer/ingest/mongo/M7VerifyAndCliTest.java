package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.cli.IngestMain;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.verify.IntegrityChecker;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M7 acceptance — the verifier and the CLI.
 *
 * <p>The verifier is the module's answer to everything MongoDB will not enforce, so the test that
 * matters is not "it reports clean on good data" but "it reports a problem on bad data". A checker
 * that cannot fail is worth nothing, and this one is deliberately broken to prove it can.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class M7VerifyAndCliTest {

    private static final Path ORACLE =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;
    private static String runId;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(ORACLE), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_m7_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ORACLE)).runId();
    }

    @AfterAll
    static void tearDown() {
        // Dropped here as well as in setUp: dropping only on start leaves every class's data
        // resident for the life of the mongod. Twenty-three classes accumulated 2.1 GB and the
        // OS started killing test runs.
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    @Test @Order(1)
    @DisplayName("a good ingest verifies clean")
    void cleanRunIsClean() {
        List<IntegrityChecker.Finding> findings = new IntegrityChecker(db).check(runId);
        // Dangling call-edge endpoints are legitimate — an external callee has no node of its own —
        // so those are reported and tolerated. Nothing else may appear.
        List<IntegrityChecker.Finding> unexpected = findings.stream()
                .filter(f -> !(f.kind().equals("dangling-ref") && f.collection().equals(Collections.CALL_EDGE)))
                .toList();
        assertTrue(unexpected.isEmpty(), "unexpected integrity findings:\n  " + unexpected);
    }

    @Test @Order(2)
    @DisplayName("the verifier notices a broken reference")
    void detectsDanglingReference() {
        // Calibration: break one reference and confirm the checker sees it. Without this the
        // "clean" result above proves only that the checker ran, not that it works.
        db.getCollection(Collections.TABLE_COLUMN_USAGE).updateOne(
                Filters.exists("objectRef"), Updates.set("objectRef", "JAR~does~not~exist"));
        List<IntegrityChecker.Finding> findings = new IntegrityChecker(db).check(runId);
        assertTrue(findings.stream().anyMatch(f -> f.kind().equals("dangling-ref")
                        && f.collection().equals(Collections.TABLE_COLUMN_USAGE)),
                "a deliberately broken objectRef was not reported: " + findings);
    }

    @Test @Order(3)
    @DisplayName("the verifier notices an incomplete run")
    void detectsIncompleteRun() {
        db.getCollection(Collections.RUN).updateOne(
                Filters.eq("_id", runId), Updates.set("status", "IN_PROGRESS"));
        assertTrue(new IntegrityChecker(db).check(runId).stream()
                        .anyMatch(f -> f.kind().equals("run-not-complete")),
                "an IN_PROGRESS run must be reported: readers would see nothing");
        db.getCollection(Collections.RUN).updateOne(
                Filters.eq("_id", runId), Updates.set("status", "COMPLETE"));
    }

    @Test @Order(4)
    @DisplayName("the CLI returns codes a script can act on")
    void cliExitCodes() {
        assertEquals(1, IngestMain.run(new String[]{}), "no args is a usage error");
        assertEquals(0, IngestMain.run(new String[]{"--help"}), "--help is success");
        assertEquals(1, IngestMain.run(new String[]{"nonsense"}), "unknown command is a usage error");
        assertEquals(2, IngestMain.run(new String[]{
                "plsql", "--source", "/nowhere/at/all", "--dialect", "ORACLE",
                "--project", "P", "--entry", "E",
                "--mongo", MongoAvailable.URI, "--db", "am_ingest_m7_cli"}),
                "a missing source directory must not look like success");
    }

    @Test @Order(5)
    @DisplayName("verification failure does not exit zero")
    void verificationFailureIsNotSuccess() {
        // The whole point of the exit codes: a script that trusts the code must not be told a
        // broken ingest succeeded.
        db.getCollection(Collections.STATEMENT).updateOne(
                Filters.exists("objectRef"), Updates.set("objectRef", "definitely~missing"));
        int code = IngestMain.run(new String[]{"verify", "--run", runId,
                "--mongo", MongoAvailable.URI, "--db", "am_ingest_m7_test"});
        assertEquals(4, code, "integrity findings must produce a non-zero exit");
    }
}
