package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The two child collections that were declared, installed, indexed — and never written to.
 *
 * <h2>How they stayed empty</h2>
 *
 * <p>{@code NodeDetailMapper} fed {@code transactionScope} and {@code variables} to a helper whose
 * first line is {@code if (!arr.isArray()) return;}. Both are <b>objects</b> in the artifact — a
 * summary with the rows nested inside ({@code transactionScope.statements},
 * {@code variables.typeRefs} and three siblings) — so the helper returned immediately, every time,
 * and raised nothing. Two collections in {@code Collections} had no writer at all.
 *
 * <h2>Why no other check caught it</h2>
 *
 * <p>Nothing was lost, which is exactly what made it invisible. {@code Splitter} still split the
 * inner arrays out of the flattened {@code am_proc_node} document, so the rows existed — under
 * {@code am_proc_node__statements} and {@code am_proc_node__plain}, names derived from the source
 * field rather than the names PLAN-04 specifies. Round-trip passed, the no-arrays gate passed, and
 * the integrity checker found no dangling reference, because the data really was all there. Only a
 * count against the artifact shows the difference, which is what this test is.
 *
 * <p>It matters because the middle layer (P6) is specified against the planned names. Queries
 * written to {@code am_txn_statement} would have returned zero rows on a populated database.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ObjectShapedChildTest {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    /** Measured from {@code api/nodes/*.json}, not copied from a plan. */
    private static final long EXPECTED_TXN = 135;
    private static final long EXPECTED_VARS = 2121;
    private static final long EXPECTED_TYPE_REFS = 3;
    private static final long EXPECTED_ROWTYPE_REFS = 1;
    private static final long EXPECTED_PLAIN = 2117;

    private static MongoClient client;
    private static MongoDatabase db;
    private static String runId;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_objchild_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS)).runId();
    }

    @AfterAll
    static void tearDown() {
        // Dropped here as well as in setUp: dropping only on start leaves every class's data
        // resident for the life of the mongod. Twenty-three classes accumulated 2.1 GB and the
        // OS started killing test runs.
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    @Test
    @Order(1)
    void transactionStatementsReachTheirOwnCollection() {
        long n = db.getCollection(Collections.TXN_STATEMENT)
                .countDocuments(Filters.eq("runId", runId));
        assertEquals(EXPECTED_TXN, n,
                "am_txn_statement is declared and indexed but holds " + n + " rows; the artifact has "
                        + EXPECTED_TXN + " under transactionScope.statements");
    }

    @Test
    @Order(2)
    void everyVariableBucketReachesTheVariableCollection() {
        long n = db.getCollection(Collections.VARIABLE_TYPE_REF)
                .countDocuments(Filters.eq("runId", runId));
        assertEquals(EXPECTED_VARS, n,
                "am_variable_type_ref holds " + n + " rows; the artifact has " + EXPECTED_VARS
                        + " across typeRefs/rowtypeRefs/collections/plain");
    }

    @Test
    @Order(3)
    void theVariableKindIsRecordedSoTheFourBucketsStayDistinguishable() {
        // Merged into one collection because they are one entity — a declared variable — and
        // "every variable in this procedure" should not be a four-way union. That is only safe if
        // the bucket survives as a field, or "which variables reference a table column" is lost.
        var coll = db.getCollection(Collections.VARIABLE_TYPE_REF);
        assertEquals(EXPECTED_TYPE_REFS,
                coll.countDocuments(Filters.and(Filters.eq("runId", runId),
                        Filters.eq("varKind", "typeRefs"))),
                "typeRefs rows are not distinguishable by varKind");
        assertEquals(EXPECTED_ROWTYPE_REFS,
                coll.countDocuments(Filters.and(Filters.eq("runId", runId),
                        Filters.eq("varKind", "rowtypeRefs"))),
                "rowtypeRefs rows are not distinguishable by varKind");
        assertEquals(EXPECTED_PLAIN,
                coll.countDocuments(Filters.and(Filters.eq("runId", runId),
                        Filters.eq("varKind", "plain"))),
                "plain rows are not distinguishable by varKind");
    }

    @Test
    @Order(4)
    void everyRowPointsAtTheNodeItWasDeclaredIn() {
        // A child row with no parent reference is unreachable: it is in the database and no query
        // that starts from a procedure will ever find it.
        for (String c : new String[]{Collections.TXN_STATEMENT, Collections.VARIABLE_TYPE_REF}) {
            long orphans = db.getCollection(c).countDocuments(
                    Filters.and(Filters.eq("runId", runId),
                            Filters.or(Filters.exists("nodeRef", false), Filters.eq("nodeRef", null))));
            assertEquals(0, orphans, c + " has " + orphans + " rows with no nodeRef");
        }
    }

    @Test
    @Order(5)
    void noDeclaredCollectionIsLeftWithoutAWriter() {
        // The general form of this bug. A collection that is declared, created and indexed but never
        // written is indistinguishable at runtime from one whose source data happens to be absent —
        // so it is checked here against an analysis known to exercise the whole PL/SQL model.
        //
        // JAR-only collections are excluded: this run is PL/SQL, and their being empty is correct.
        String[] plsqlCollections = {
                Collections.RUN, Collections.DB_OBJECT, Collections.PROC_NODE,
                Collections.STATEMENT, Collections.CALL_EDGE, Collections.CURSOR_DECL,
                Collections.DYNAMIC_SQL, Collections.EXCEPTION_HANDLER,
                Collections.TXN_STATEMENT, Collections.VARIABLE_TYPE_REF,
                Collections.SEQUENCE_USE, Collections.TABLE_COLUMN_USAGE,
                Collections.NODE_TABLE_ACCESS, Collections.SQL_TEXT,
        };
        StringBuilder empty = new StringBuilder();
        for (String c : plsqlCollections) {
            if (db.getCollection(c).countDocuments() == 0) empty.append(c).append(' ');
        }
        assertEquals("", empty.toString().trim(),
                "declared PL/SQL collections with no rows after a full ingest: " + empty);
    }
}
