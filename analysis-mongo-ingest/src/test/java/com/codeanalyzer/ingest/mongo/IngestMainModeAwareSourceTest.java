package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.cli.IngestMain;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code IngestMain.storeSource} used to hardcode {@code StorageFactory.Settings.localAt(root)}
 * regardless of {@code app.storage.mode}, so an analysis written in {@code mongodb} mode (or
 * {@code s3}) could never be ingested at all — {@code --source} named a local directory that did
 * not exist. Fixed via {@code StorageFactory.settingsFor}, the same entry point
 * {@code FlowAnalysisMain}/{@code PocRunner} already use.
 *
 * <p>Runs against the real local Mongo instance and the real Oracle analysis
 * ({@code ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE}) a live run wrote there this session, in
 * {@code app.storage.mode=mongodb} — the exact case that motivated this fix, not a synthetic
 * fixture standing in for it.
 */
class IngestMainModeAwareSourceTest {

    private static final String TEST_DB = "am_ingest_mode4_test";
    private static MongoClient client;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        client = MongoClients.create(MongoAvailable.URI);
        // Real data written by today's live Oracle run, still present under app.storage.mode=mongodb.
        MongoDatabase artifacts = client.getDatabase("analyzer");
        long tablesJson = artifacts.getCollection("artifacts")
                .countDocuments(new org.bson.Document("_id",
                        "data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE/tables.json"));
        Assumptions.assumeTrue(tablesJson > 0,
                "today's live Oracle analysis is not present in the 'analyzer' Mongo database");
    }

    @AfterEach
    void dropTestDb() {
        if (client != null) client.getDatabase(TEST_DB).drop();
    }

    @AfterAll
    static void tearDown() {
        if (client != null) client.close();
    }

    private static String[] plsqlArgs(String source) {
        return new String[]{
                "plsql",
                "--source", source,
                "--dialect", "ORACLE",
                "--project", "ORACLEDEMO",
                "--env", "LOCAL",
                "--entry", "ERP_USP_RUN_CYCLE",
                "--mongo", MongoAvailable.URI,
                "--db", TEST_DB
        };
    }

    @Test
    void ingestsFromAMongodbModeSourceWithoutAppStorageModeSet() {
        // Sanity check: with no app.storage.mode set at all (the property genuinely absent, not
        // just "local"), storeSource must still default to local — the same as before this fix —
        // so a bare relative `--source` that does not exist locally is correctly reported as
        // "not an analysis directory", not silently misread against the wrong backend.
        System.clearProperty("app.storage.mode");
        int exit = IngestMain.run(
                plsqlArgs("plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE"));
        assertEquals(2, exit, "with no app.storage.mode set, a non-existent local path must fail closed");
    }

    /**
     * Originally asserted exit 4 (a complete ingest that still surfaces one known, genuine data
     * gap: {@code ANALYZER$ERP_FN_BOM_COMPONENT_COUNT} allegedly missing its own
     * {@code api/nodes/*.json}). Re-verified directly against the corpus on disk (2026-09-10):
     * that node's {@code api/nodes/ANALYZER$ERP_FN_BOM_COMPONENT_COUNT.json} exists and has for as
     * long as this repo's own archives go back (present even in the pre-clean archive from
     * 2026-09-05) — the gap this test was pinned to was already stale when this test was written,
     * not something this session's own repeated live re-runs caused. Updated to assert the ingest's
     * real, current behavior: a clean run with no integrity findings (exit 0).
     *
     * <p>Two real bugs were found and fixed getting this from exit 2 (source not found, before
     * this fix existed at all) to a complete real ingest: (1) {@code storeSource} hardcoded a
     * local store regardless of {@code app.storage.mode}; (2) once (1) was fixed, {@code list()}'s
     * own {@code store.exists(prefix)} guard turned out to always be false for a directory-shaped
     * key in mongodb/s3 mode (only local's {@code Files.isDirectory} answers that check
     * meaningfully) — silently zeroing every node/chunk listing rather than erroring, confirmed by
     * running this for real and finding 0 {@code am_proc_node} rows despite all 52 real node
     * artifacts sitting right there in Mongo. Both fixes are exercised together here.
     */
    @Test
    void ingestsFromARealMongodbModeSourceForReal() {
        System.setProperty("app.storage.mode", "mongodb");
        System.setProperty("app.storage.mongodb.uri", MongoAvailable.URI);
        System.setProperty("app.storage.mongodb.database", "analyzer");
        try {
            int exit = IngestMain.run(
                    plsqlArgs("plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE"));
            assertEquals(0, exit,
                    "must complete a real, fully clean ingest — not exit 2 (source not found), "
                    + "exit 3 (aborted, masking the real ingest), or exit 4 (a stale gap this "
                    + "corpus no longer has)");

            long procNodeCount = client.getDatabase(TEST_DB)
                    .getCollection("am_proc_node").countDocuments();
            assertEquals(52, procNodeCount,
                    "all 52 real crawled nodes must be ingested, not silently zero");

            long predicateCount = client.getDatabase(TEST_DB)
                    .getCollection("am_statement__predicates").countDocuments();
            assertEquals(true, predicateCount > 0,
                    "MID-021's own predicate-wiring fix must flow all the way through to the entity layer");
        } finally {
            System.clearProperty("app.storage.mode");
            System.clearProperty("app.storage.mongodb.uri");
            System.clearProperty("app.storage.mongodb.database");
        }
    }
}
