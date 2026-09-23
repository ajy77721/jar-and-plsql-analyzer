package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M6 acceptance — the call-tree DAG and the endpoints.
 *
 * <p>The property that matters is that the DAG is stored <em>as a DAG</em>. Its sharing is what
 * makes it storable at all: 3,784 physical nodes stand in for 20.3 billion logical ones, and one
 * endpoint alone expands to 300,167,238. A test that only counted rows would pass while an
 * expansion bug quietly made the collection unbounded, so this asserts the sharing directly.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoIngestM6DagTest {

    private static MongoClient client;
    private static MongoDatabase db;
    private static Path jarDir;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        jarDir = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jarDir != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_m6_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingestJar("unified-web", new FileArtifactSource(jarDir));
    }

    @AfterAll
    static void tearDown() {
        // Dropped here as well as in setUp: dropping only on start leaves every class's data
        // resident for the life of the mongod. Twenty-three classes accumulated 2.1 GB and the
        // OS started killing test runs.
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    private JsonNode source() throws Exception {
        return new ObjectMapper().readTree(jarDir.resolve("analysis.json").toFile());
    }

    @Test @Order(1)
    @DisplayName("every DAG node and every edge is stored, once")
    void dagStored() throws Exception {
        JsonNode dag = source().path("callTreeNodes");
        int edges = 0;
        for (JsonNode n : dag) edges += n.path("childIds").size();

        assertEquals(dag.size(), db.getCollection(Collections.DAG_NODE).countDocuments(), "DAG nodes");
        assertEquals(edges, db.getCollection(Collections.DAG_EDGE).countDocuments(), "DAG edges");
    }

    @Test @Order(2)
    @DisplayName("the DAG is shared, not expanded")
    void dagIsShared() {
        long nodes = db.getCollection(Collections.DAG_NODE).countDocuments();
        long edges = db.getCollection(Collections.DAG_EDGE).countDocuments();
        // A node reachable by several paths must be ONE document. If storing had expanded the
        // tree, edges would vastly exceed nodes and the collection would grow without bound.
        assertTrue(edges < nodes * 4,
                "edges (" + edges + ") far exceed nodes (" + nodes + "): the tree is being expanded");
        long reused = db.getCollection(Collections.DAG_EDGE)
                .distinct("childRef", String.class).into(new ArrayList<>()).size();
        assertTrue(reused < edges,
                "no child is referenced twice, so the sharing that makes this storable is absent");
    }

    @Test @Order(3)
    @DisplayName("every edge names a node that exists")
    void edgesResolve() {
        Set<String> nodeKeys = new HashSet<>(db.getCollection(Collections.DAG_NODE)
                .distinct("entityKey", String.class).into(new ArrayList<>()));
        long dangling = db.getCollection(Collections.DAG_EDGE)
                .distinct("childRef", String.class).into(new ArrayList<>())
                .stream().filter(r -> !nodeKeys.contains(r)).count();
        assertEquals(0, dangling, "edges point at DAG nodes that were not stored");
    }

    @Test @Order(4)
    @DisplayName("endpoints and their shared pools are stored, and the roots resolve")
    void endpointsStored() throws Exception {
        JsonNode src = source();
        assertEquals(src.path("endpoints").size(),
                db.getCollection(Collections.ENDPOINT).countDocuments(), "endpoints");

        int pooled = 0;
        JsonNode pools = src.path("endpointPool");
        var names = pools.fieldNames();
        while (names.hasNext()) pooled += pools.path(names.next()).size();
        assertEquals(pooled, db.getCollection(Collections.ENDPOINT_POOL).countDocuments(),
                "pooled beans/externalCalls/dynamicFlows");

        Document ep = db.getCollection(Collections.ENDPOINT).find(Filters.exists("rootRef")).first();
        assertNotNull(ep, "no endpoint recorded a call-tree root");
        assertEquals(1, db.getCollection(Collections.DAG_NODE)
                        .countDocuments(Filters.eq("entityKey", ep.getString("rootRef"))),
                "an endpoint's call-tree root is not a stored DAG node");
    }

    @Test @Order(5)
    @DisplayName("the tree can be walked in both directions by index")
    void bothDirectionsIndexed() {
        for (String field : java.util.List.of("parentRef", "childRef")) {
            Document plan = db.getCollection(Collections.DAG_EDGE)
                    .find(Filters.eq(field, "x")).explain();
            assertTrue(plan.toJson().contains("IXSCAN"),
                    "DAG traversal by " + field + " is not indexed — 'how is this reached' would scan");
        }
    }
}
