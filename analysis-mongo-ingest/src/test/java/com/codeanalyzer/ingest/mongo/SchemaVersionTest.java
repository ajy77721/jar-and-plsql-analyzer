package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.SchemaInstaller;
import com.codeanalyzer.ingest.mongo.schema.SchemaMeta;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB-013 — a database records the schema it holds, and refuses an older writer.
 *
 * <h2>The scenario</h2>
 *
 * <p>Rolling back a deployment is enough to cause this. An older build pointed at a newer
 * database installs its own validators over the newer ones and ingests happily, writing documents
 * in an older shape alongside newer ones. Nothing crashes; the database simply ends up in two
 * shapes at once with no record of when it happened.
 *
 * <p>The rule is one-directional and the asymmetry is the point: refusing is reversible (redeploy),
 * writing is not.
 */
class SchemaVersionTest {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private MongoDatabase db;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        client = MongoClients.create(MongoAvailable.URI);
    }

    @AfterAll
    static void close() {
        // Dropped by name: this class uses a per-test database, so a static teardown cannot
        // reach the instance field. Dropping only on start left every class's data resident
        // for the life of the mongod — 23 classes accumulated 2.1 GB and the OS began
        // killing test runs.
        if (client != null) {
            client.getDatabase("am_ingest_schemaver_test").drop();
        }
        if (client != null) client.close();
    }

    @BeforeEach
    void freshDb() {
        db = client.getDatabase("am_ingest_schemaver_test");
        db.drop();
    }

    @Test
    void installingStampsTheVersion() {
        assertEquals(0, SchemaMeta.storedVersion(db), "a fresh database should report no version");
        new SchemaInstaller(db).install();
        assertEquals(SchemaMeta.CURRENT_VERSION, SchemaMeta.storedVersion(db),
                "install did not record the schema version");
    }

    @Test
    void installingTwiceIsStillFine() {
        SchemaInstaller installer = new SchemaInstaller(db);
        installer.install();
        assertDoesNotThrow(installer::install, "the version check broke idempotent installs");
        assertEquals(SchemaMeta.CURRENT_VERSION, SchemaMeta.storedVersion(db));
    }

    @Test
    void aNewerDatabaseRefusesThisWriter() {
        new SchemaInstaller(db).install();
        db.getCollection(SchemaMeta.COLLECTION).replaceOne(
                Filters.eq("_id", SchemaMeta.ID),
                new Document("_id", SchemaMeta.ID)
                        .append("schemaVersion", SchemaMeta.CURRENT_VERSION + 1),
                new ReplaceOptions().upsert(true));

        var e = assertThrows(SchemaMeta.SchemaTooNewException.class,
                () -> new SchemaInstaller(db).install(),
                "an older build was allowed to write to a newer database");
        assertTrue(e.getMessage().contains(String.valueOf(SchemaMeta.CURRENT_VERSION + 1)),
                "the error does not say which version was found");
    }

    @Test
    void anOlderDatabaseIsUpgradedRatherThanRefused() {
        // The other direction must NOT fail: that is the ordinary upgrade path, and refusing it
        // would mean every schema bump required manual intervention on every database.
        new SchemaInstaller(db).install();
        db.getCollection(SchemaMeta.COLLECTION).replaceOne(
                Filters.eq("_id", SchemaMeta.ID),
                new Document("_id", SchemaMeta.ID).append("schemaVersion", 0),
                new ReplaceOptions().upsert(true));

        assertDoesNotThrow(() -> new SchemaInstaller(db).install(),
                "an older database should be upgraded, not refused");
        assertEquals(SchemaMeta.CURRENT_VERSION, SchemaMeta.storedVersion(db));
    }

    @Test
    void theCheckRunsBeforeAnyValidatorIsInstalled() {
        // Ordering is the whole guarantee. A check that runs after installation has already
        // overwritten the newer build's validators by the time it reports the mismatch.
        db.getCollection(SchemaMeta.COLLECTION).replaceOne(
                Filters.eq("_id", SchemaMeta.ID),
                new Document("_id", SchemaMeta.ID)
                        .append("schemaVersion", SchemaMeta.CURRENT_VERSION + 5),
                new ReplaceOptions().upsert(true));

        assertThrows(SchemaMeta.SchemaTooNewException.class, () -> new SchemaInstaller(db).install());

        var names = new java.util.HashSet<String>();
        db.listCollectionNames().into(names);
        names.remove(SchemaMeta.COLLECTION);
        assertTrue(names.isEmpty(),
                "collections were created before the version check refused the build: " + names);
    }

    @Test
    void aRunRecordsTheSchemaThatWroteIt() {
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        String runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS)).runId();

        Document run = db.getCollection("am_run").find(Filters.eq("_id", runId)).first();
        assertNotNull(run);
        assertEquals(SchemaMeta.CURRENT_VERSION, run.getInteger("schemaVersion"),
                "the run does not record which schema wrote it, so a digest match cannot tell "
                        + "'same source, same mapper' from 'same source, mapper since fixed'");
    }

    @Test
    void aRunFromAnOlderSchemaDoesNotSatisfyTheDigestShortCircuit() {
        // The reason the run carries the version at all. Same bytes through a fixed mapper produce
        // different documents, so the fix must not be skipped over.
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        var first = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS));
        assertFalse(first.skipped());

        // Pretend that run was written by an earlier schema.
        db.getCollection("am_run").updateOne(Filters.eq("_id", first.runId()),
                new Document("$set", new Document("schemaVersion", SchemaMeta.CURRENT_VERSION - 1)));

        var second = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS));
        assertFalse(second.skipped(),
                "a run written by an older schema satisfied the digest short-circuit, so a mapper "
                        + "fix would never reach the database");
    }
}
