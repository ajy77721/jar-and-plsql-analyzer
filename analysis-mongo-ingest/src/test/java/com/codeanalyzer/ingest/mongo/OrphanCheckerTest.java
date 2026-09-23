package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.verify.OrphanRefChecker;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * VER-001 — every reference is checked, including the ones nobody declared.
 *
 * <h2>Why discovery is the requirement</h2>
 *
 * <p>{@code IntegrityChecker} checks a hand-written map of eight collections. The database has ref
 * fields in dozens — every split collection carries {@code parentRef}, and each new relation adds
 * its own. The gap between a declared list and the data is invisible: a reference nobody checks
 * looks exactly like one that is fine. So the fields are read off the documents, and a reference
 * whose target is undeclared is <b>reported</b> rather than skipped.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrphanCheckerTest {

    private static final Path PLSQL =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(PLSQL), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_orphan_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(PLSQL));
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        if (jar != null) svc.ingestJar("unified-web", new FileArtifactSource(jar));
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
    void itFindsFarMoreRefFieldsThanTheDeclaredList() {
        Map<String, Set<String>> found = new OrphanRefChecker(db).discoverRefFields();
        long fields = found.values().stream().mapToLong(Set::size).sum();
        assertTrue(found.size() > 20,
                "only " + found.size() + " collections have ref fields; discovery is not working");
        assertTrue(fields > 25,
                "found " + fields + " ref fields; the hand-written list covered 11 across 8 "
                        + "collections, so discovery must find substantially more");
    }

    @Test
    @Order(2)
    void bothSidesOfEveryJunctionAreCovered() {
        // A junction row is correct only if both references resolve. Discovery gets this for free,
        // which is the point: neither side can be checked while the other is forgotten.
        Map<String, Set<String>> found = new OrphanRefChecker(db).discoverRefFields();
        assertTrue(found.getOrDefault(Collections.DAG_EDGE, Set.of())
                        .containsAll(Set.of("parentRef", "childRef")),
                "am_dag_edge: both sides not discovered");
        assertTrue(found.getOrDefault(Collections.METHOD_INVOCATION, Set.of())
                        .containsAll(Set.of("methodRef", "invocationRef")),
                "am_method_invocation: both sides not discovered");
        assertTrue(found.getOrDefault(Collections.ENDPOINT_REACHABLE, Set.of())
                        .containsAll(Set.of("endpointRef", "nodeRef")),
                "am_endpoint_reachable: both sides not discovered");
    }

    @Test
    @Order(3)
    void everyDiscoveredRefHasADeclaredTarget() {
        // The failure mode this class exists to end: a reference nobody declared is a reference
        // nobody checks. It must be loud rather than absent.
        List<OrphanRefChecker.Orphan> undeclared = new OrphanRefChecker(db).check().stream()
                .filter(OrphanRefChecker.Orphan::unresolvableTarget).toList();
        assertEquals(List.of(), undeclared,
                "these reference fields resolve to no known collection, so they are unchecked");
    }

    @Test
    @Order(4)
    void theOnlyOrphansAreTheOneKnownUpstreamDefect() {
        // A checker that fires on correct data is unusable, so this is calibration in the honest
        // direction. But the answer on real data is not zero, and pinning it to zero would mean
        // either weakening the checker or hiding a genuine finding.
        //
        // THE FINDING: PL/SQL procedure overloads. ERP_PKG_PLANNER.COMPUTE_TOTAL is defined twice
        // and stored as ...COMPUTE_TOTAL__1 and __2, but two call edges name the bare
        // ...COMPUTE_TOTAL with no suffix — the analyser could not resolve which overload is
        // called. Those edges point at a procedure that does not exist, so "who calls
        // COMPUTE_TOTAL__1" silently answers with two callers missing.
        //
        // Upstream of ingest and filed as ANA-004. Pinned by exact count so the day it is fixed
        // this test fails and says so, rather than quietly passing with a stale allowance.
        //
        // A SECOND FINDING, added with ING-117: jar-dep-map.json names two jars —
        // flow-poc-1.0.0-SNAPSHOT.jar and jackson-datatype-jsr310-2.15.4.jar — as being depended
        // on by classes in this application, but neither appears in bundled-jars.json. Two source
        // artifacts describing the same jar set disagree; ingest reports what each one says rather
        // than silently reconciling them into a single answer neither file actually gives.
        List<OrphanRefChecker.Orphan> orphans = new OrphanRefChecker(db).check();

        List<OrphanRefChecker.Orphan> unexpected = orphans.stream()
                .filter(o -> !isKnownOverloadDefect(o) && !isKnownUncatalogedDependencyJar(o))
                .toList();
        assertEquals(List.of(), unexpected,
                "new dangling references beyond the two known upstream defects");

        long overloadRefs = orphans.stream().filter(OrphanCheckerTest::isKnownOverloadDefect)
                .mapToLong(OrphanRefChecker.Orphan::missing).sum();
        assertEquals(2, overloadRefs,
                "the known unresolved-overload references changed count (was 2, one per edge "
                        + "collection); if ANA-004 is fixed, delete this allowance");

        long uncatalogedJarRefs = orphans.stream()
                .filter(OrphanCheckerTest::isKnownUncatalogedDependencyJar)
                .mapToLong(OrphanRefChecker.Orphan::missing).sum();
        assertEquals(2, uncatalogedJarRefs,
                "the known uncataloged-dependency-jar count changed (was 2); if jar-dep-map.json "
                        + "and bundled-jars.json were reconciled upstream, delete this allowance");
    }

    /** The single documented upstream defect: an unresolved procedure overload. */
    private static boolean isKnownOverloadDefect(OrphanRefChecker.Orphan o) {
        return "fromRef".equals(o.field())
                && o.example() != null && o.example().endsWith("COMPUTE_TOTAL");
    }

    /**
     * A jar named in {@code jar-dep-map.json} that {@code bundled-jars.json} does not catalog.
     * Two source artifacts, not an ingest defect — see the finding recorded above.
     */
    private static boolean isKnownUncatalogedDependencyJar(OrphanRefChecker.Orphan o) {
        return com.codeanalyzer.ingest.mongo.schema.Collections.JAR_DEPENDENCY.equals(o.collection())
                && "parentRef".equals(o.field())
                && o.example() != null
                && (o.example().endsWith("flow-poc-1.0.0-SNAPSHOT.jar")
                        || o.example().endsWith("jackson-datatype-jsr310-2.15.4.jar"));
    }

    @Test
    @Order(5)
    void aPlantedDanglingParentRefIsDetectedAndNamed() {
        // The DoD, planted rather than simulated. Detection is not enough — the report has to say
        // which collection and which field, or it cannot be acted on.
        String coll = Collections.STATEMENT;
        Document victim = db.getCollection(coll).find(Filters.exists("nodeRef", true)).first();
        assertNotNull(victim, "no statement with a nodeRef to corrupt");
        String original = victim.getString("nodeRef");

        db.getCollection(coll).updateOne(Filters.eq("_id", victim.get("_id")),
                new Document("$set", new Document("nodeRef", "GHOST~NO~SUCH~NODE")));
        try {
            List<OrphanRefChecker.Orphan> orphans = new OrphanRefChecker(db).check();
            OrphanRefChecker.Orphan hit = orphans.stream()
                    .filter(o -> coll.equals(o.collection()) && "nodeRef".equals(o.field()))
                    .findFirst().orElse(null);
            assertNotNull(hit, "the planted dangling reference was not detected: " + orphans);
            assertEquals(1, hit.missing(), "the report miscounts the dangling references");
            assertEquals("GHOST~NO~SUCH~NODE", hit.example(),
                    "the report does not name the offending value");
            assertEquals(Collections.PROC_NODE, hit.target(),
                    "the report does not say which collection it should have resolved into");
        } finally {
            db.getCollection(coll).updateOne(Filters.eq("_id", victim.get("_id")),
                    new Document("$set", new Document("nodeRef", original)));
        }
    }

    @Test
    @Order(6)
    void aPlantedDanglingSplitChildIsDetected() {
        // parentRef targets are derived from the collection name rather than declared, so this is a
        // different code path from the one above and needs its own planting.
        String split = db.listCollectionNames().into(new java.util.ArrayList<>()).stream()
                .filter(n -> n.contains("__"))
                .filter(n -> db.getCollection(n).countDocuments() > 0)
                .findFirst().orElse(null);
        assertNotNull(split, "no populated split collection to corrupt");

        Document victim = db.getCollection(split).find().first();
        Object id = victim.get("_id");
        String original = victim.getString("parentRef");
        db.getCollection(split).updateOne(Filters.eq("_id", id),
                new Document("$set", new Document("parentRef", "GHOST~NO~SUCH~PARENT")));
        try {
            boolean found = new OrphanRefChecker(db).check().stream()
                    .anyMatch(o -> split.equals(o.collection()) && "parentRef".equals(o.field()));
            assertTrue(found, "an orphaned split child in " + split + " was not detected");
        } finally {
            db.getCollection(split).updateOne(Filters.eq("_id", id),
                    new Document("$set", new Document("parentRef", original)));
        }
    }

    @Test
    @Order(7)
    void nullableRefsAreJustifiedRatherThanIgnoredWholesale() {
        // VER-002. "Ignore nulls" would hide real defects; a list with a reason per field does not.
        for (String field : List.of("methodRef", "objectRef", "toRef", "nodeRef")) {
            assertTrue(OrphanRefChecker.nullAllowed(field), field + " is not on the allowlist");
            assertNotNull(OrphanRefChecker.nullReason(field),
                    field + " is allowlisted with no justification");
        }
        assertFalse(OrphanRefChecker.nullAllowed("parentRef"),
                "a split child with no parent is unreachable and must never be allowlisted");
    }
}
