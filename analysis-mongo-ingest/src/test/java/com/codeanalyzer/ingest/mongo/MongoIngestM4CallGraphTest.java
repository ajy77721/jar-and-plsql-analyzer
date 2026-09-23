package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M4 acceptance — the call graph, stored as occurrences AND as aggregated pairs.
 *
 * <p>ORACLE: 2,258 call occurrences over 218 distinct pairs. The ratio is the point — collapsing to
 * pairs would lose 2,040 call sites, and keeping only occurrences would lose the counts and cycle
 * flags the analyser computed. Both shapes are asserted here so neither can quietly disappear.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoIngestM4CallGraphTest {

    private static final Path ORACLE =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(ORACLE), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_m4_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ORACLE));
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
    @DisplayName("both shapes are stored: 2,258 occurrences and 218 aggregated pairs")
    void bothShapesStored() {
        assertEquals(2258, db.getCollection(Collections.CALL_EDGE).countDocuments(),
                "call occurrences must match edges.json");
        assertEquals(218, db.getCollection(Collections.CALL_EDGE_UNIQUE).countDocuments(),
                "aggregated pairs must match api/call_graph.json");
    }

    @Test @Order(2)
    @DisplayName("the aggregated rows carry the fields the raw edges do not")
    void aggregateKeepsItsExtraFields() {
        // These four are why call_graph.json is stored rather than derived. backEdge especially:
        // it is a cycle judgement made while walking the graph, and recomputing it here would risk
        // disagreeing with what the rest of the product displays.
        Document any = db.getCollection(Collections.CALL_EDGE_UNIQUE)
                .find(Filters.exists("count")).first();
        assertNotNull(any, "no aggregated edge carries `count` — the extra fields were dropped");
        for (String f : List.of("count", "minDepth")) {
            assertTrue(any.containsKey(f), "aggregated edge lost `" + f + "`");
        }
        assertTrue(db.getCollection(Collections.CALL_EDGE_UNIQUE)
                        .countDocuments(Filters.exists("backEdge")) > 0,
                "no edge carries backEdge, so cycle information was lost");
    }

    @Test @Order(3)
    @DisplayName("the graph is traversable in both directions by index")
    void traversableBothWays() {
        String hub = db.getCollection(Collections.CALL_EDGE).find().first().getString("toRef");
        assertNotNull(hub);
        // "who calls this" and "what does this call" are both real questions; an edge collection
        // indexed only one way answers half of them with a collection scan.
        for (String field : List.of("fromRef", "toRef")) {
            Document plan = db.getCollection(Collections.CALL_EDGE)
                    .find(Filters.eq(field, hub)).explain();
            assertTrue(plan.toJson().contains("IXSCAN"),
                    "traversal by " + field + " is not indexed");
        }
    }

    @Test @Order(4)
    @DisplayName("every edge endpoint names a node, and most resolve to a stored one")
    void endpointsResolve() {
        List<String> refs = db.getCollection(Collections.CALL_EDGE)
                .distinct("fromRef", String.class).into(new ArrayList<>());
        assertFalse(refs.isEmpty(), "no edges recorded a source node");
        long resolved = refs.stream()
                .filter(r -> db.getCollection(Collections.PROC_NODE)
                        .countDocuments(Filters.eq("entityKey", r)) > 0)
                .count();
        // Not all endpoints have a node file — an external or unresolved callee legitimately has
        // none, and the edge is kept rather than dropped so the dependency is not lost. What must
        // hold is that the graph is mostly connected to real nodes.
        assertTrue(resolved > refs.size() / 2,
                "most call sources should resolve to a stored node; " + resolved + " of " + refs.size());
    }
}
