package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.write.RefAssertion;
import com.codeanalyzer.ingest.mongo.write.RunMarker;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-023 — a run that produced an unresolvable structural reference never becomes visible.
 *
 * <h2>What "structural" excludes, and why</h2>
 *
 * <p>Not every dangling reference is a defect. A call edge may name an external or unresolved
 * callee that has no node of its own; dropping that edge would lose a real dependency, so those are
 * reported by {@code IntegrityChecker} and are deliberately not fatal. Fatal is reserved for
 * references whose absence makes a row <em>unreachable</em> — a child whose parent does not exist
 * can never be found by a query that starts from the parent.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RefAssertionTest {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private MongoDatabase db;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
    }

    @AfterAll
    static void close() {
        // Dropped by name: this class uses a per-test database, so a static teardown cannot
        // reach the instance field. Dropping only on start left every class's data resident
        // for the life of the mongod — 23 classes accumulated 2.1 GB and the OS began
        // killing test runs.
        if (client != null) {
            client.getDatabase("am_ingest_refassert_test").drop();
        }
        if (client != null) client.close();
    }

    @BeforeEach
    void freshDb() {
        db = client.getDatabase("am_ingest_refassert_test");
        db.drop();
    }

    /** The real analysis, with one statement re-pointed at a procedure that does not exist. */
    private static final class OrphanStatementSource implements MongoIngestService.ArtifactSource {
        private final FileArtifactSource delegate = new FileArtifactSource(ANALYSIS);

        @Override
        public InputStream open(String artifactPath) throws IOException {
            InputStream in = delegate.open(artifactPath);
            if (in == null || !"api/statements/index.json".equals(artifactPath)) return in;
            String json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            in.close();
            // Re-point the first statement's nodeId at a procedure with no node file. The mapper
            // falls back to computing a key for an unknown node, which is exactly how a real
            // orphan arises — no node document is ever written for it.
            String broken = json.replaceFirst("\"nodeId\"\\s*:\\s*\"[^\"]*\"",
                    "\"nodeId\": \"GHOST\\$NO_SUCH_PROCEDURE\"");
            assertNotEquals(json, broken, "the fixture no longer contains a nodeId to re-point");
            return new ByteArrayInputStream(broken.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public List<String> list(String directory) throws IOException {
            return delegate.list(directory);
        }
    }

    @Test
    @Order(1)
    void aCleanIngestPassesTheAssertion() {
        // Calibration in the other direction: an assertion that fires on correct data would be
        // useless, and the real analysis is the only proof that it does not.
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        String runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS)).runId();

        List<RefAssertion.Broken> broken = new RefAssertion(db).check(runId);
        assertTrue(broken.isEmpty(), "the real analysis reports broken references: " + broken);

        Document run = db.getCollection(Collections.RUN).find(Filters.eq("_id", runId)).first();
        assertEquals(RunMarker.COMPLETE, run.getString("status"));
    }

    @Test
    @Order(2)
    void anOrphanedChildAbortsTheRun() {
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();

        var e = assertThrows(RefAssertion.BrokenReferencesException.class,
                () -> svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                        new OrphanStatementSource()),
                "a statement pointing at a non-existent procedure was published");

        assertFalse(e.broken().isEmpty());
        var first = e.broken().get(0);
        assertEquals(Collections.STATEMENT, first.collection(),
                "the failure does not name the collection that produced it");
        assertEquals("nodeRef", first.field(), "the failure does not name the field");
    }

    @Test
    @Order(3)
    void theAbortedRunIsNeverVisibleToAReader() {
        // The point of checking before publishing. Readers filter on COMPLETE, so a failed run's
        // documents are invisible rather than wrong — cleanable later, harmless meanwhile.
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        assertThrows(RefAssertion.BrokenReferencesException.class,
                () -> svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                        new OrphanStatementSource()));

        assertEquals(0, db.getCollection(Collections.RUN)
                        .countDocuments(Filters.eq("status", RunMarker.COMPLETE)),
                "a run with broken references was published");
        assertEquals(1, db.getCollection(Collections.RUN)
                        .countDocuments(Filters.eq("status", RunMarker.FAILED)),
                "the aborted run was not marked FAILED");
        assertEquals(0, db.getCollection(Collections.RUN)
                        .countDocuments(Filters.eq("isCurrent", true)),
                "an aborted run is current");
    }

    @Test
    @Order(4)
    void aFailedRunDoesNotDemoteTheGoodOneBeforeIt() {
        // Ordering: the previous run is demoted inside complete(), which the assertion runs before.
        // If the check ran after, a bad run would have already superseded a good one and the
        // database would be left with no current run at all.
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        String good = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS)).runId();

        assertThrows(RefAssertion.BrokenReferencesException.class,
                () -> svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                        new OrphanStatementSource(), true));

        Document stillGood = db.getCollection(Collections.RUN).find(Filters.eq("_id", good)).first();
        assertEquals(RunMarker.COMPLETE, stillGood.getString("status"),
                "a failed run demoted the last good one");
        assertTrue(stillGood.getBoolean("isCurrent", false),
                "the last good run is no longer current after a failed ingest");
    }

    @Test
    @Order(5)
    void aParentCarriedOverFromAnEarlierRunStillResolves() {
        // The reason the lookup is global rather than filtered to this run.
        //
        // Today every ingest rewrites every entity it encounters, stamping the current runId, so a
        // run-scoped lookup would happen to pass. That is a property of the current write path, not
        // of the model: entity documents are content-addressed and deduplicated precisely so that
        // an unchanged one need not be rewritten. The moment that optimisation is taken — or an
        // entity is reachable from a run whose source no longer contains it — a run-scoped lookup
        // reports a live parent as missing and fails a correct ingest.
        //
        // Simulated by stamping a parent with an earlier run's id, which is exactly the state a
        // skipped rewrite would leave behind.
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        String runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS)).runId();

        Document child = db.getCollection(Collections.STATEMENT)
                .find(Filters.and(Filters.eq("runId", runId), Filters.exists("nodeRef", true)))
                .first();
        assertNotNull(child, "no statement with a nodeRef to work from");
        String parentKey = child.getString("nodeRef");

        var updated = db.getCollection(Collections.PROC_NODE).updateOne(
                Filters.eq("entityKey", parentKey),
                new Document("$set", new Document("runId", "OLDER~RUN~FROM~BEFORE")));
        assertEquals(1, updated.getModifiedCount(), "the parent to re-stamp was not found");

        List<RefAssertion.Broken> broken = new RefAssertion(db).check(runId);
        assertTrue(broken.isEmpty(),
                "a parent carried over from an earlier run was reported as dangling: " + broken);
    }
}
