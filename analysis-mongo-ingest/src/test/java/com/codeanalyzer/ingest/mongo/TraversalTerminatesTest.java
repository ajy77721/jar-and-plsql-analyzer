package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-114 — the traversal terminates, which is not a given.
 *
 * <h2>The measurement this exists for</h2>
 *
 * <p>Expanded as a tree, the largest endpoint in this analysis is <b>20,300,167,401</b> nodes and
 * the 321 endpoints together are <b>466,903,865,409</b>. Collapsed by a visited set the same
 * traversal produces <b>57,700</b> (endpoint, node) rows, the largest endpoint reaching 2,297
 * distinct nodes. The producer content-addresses subtrees, so identical subtrees are one node with
 * many parents; walking as a tree re-walks each shared subtree once per path that reaches it, and
 * it is the number of paths that explodes.
 *
 * <p>"Terminates" is therefore the assertion, and a time bound is the only honest way to state it:
 * a traversal that has not finished is indistinguishable from one that never will.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TraversalTerminatesTest {

    /** Measured from the analysis on disk. */
    private static final long EXPECTED_ROWS = 57700;
    private static final long EXPECTED_LARGEST_ENDPOINT = 2297;

    private static MongoClient client;
    private static MongoDatabase db;
    private static Path jarDir;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        jarDir = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jarDir != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_reach_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        // The DoD in one line: this call contains the traversal, and it has to come back.
        assertTimeoutPreemptively(Duration.ofMinutes(5),
                () -> svc.ingestJar("unified-web", new FileArtifactSource(jarDir)),
                "ingest did not terminate; the traversal is expanding paths rather than nodes");
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
    void theClosureIsTheCollapsedSizeNotTheExpandedOne() {
        long rows = db.getCollection(Collections.ENDPOINT_REACHABLE).countDocuments();
        assertEquals(EXPECTED_ROWS, rows,
                "reachability rows should be the visited-set count, not the path count");
    }

    @Test
    @Order(2)
    void theLargestEndpointReachesItsDistinctNodesOnce() {
        Document top = db.getCollection(Collections.ENDPOINT_REACHABLE).aggregate(List.of(
                new Document("$group", new Document("_id", "$endpointRef")
                        .append("n", new Document("$sum", 1))),
                new Document("$sort", new Document("n", -1)),
                new Document("$limit", 1))).first();
        assertNotNull(top, "no reachability rows at all");
        assertEquals(EXPECTED_LARGEST_ENDPOINT, top.getInteger("n").longValue(),
                "the largest endpoint's reachable set is the wrong size");
    }

    @Test
    @Order(3)
    void noEndpointNodePairIsRecordedTwice() {
        // A visited set that is consulted but not honoured still terminates and still duplicates.
        Document dup = db.getCollection(Collections.ENDPOINT_REACHABLE).aggregate(List.of(
                new Document("$group", new Document("_id",
                        new Document("e", "$endpointRef").append("n", "$nodeRef"))
                        .append("c", new Document("$sum", 1))),
                new Document("$match", new Document("c", new Document("$gt", 1))),
                new Document("$limit", 1))).first();
        assertNull(dup, "the same (endpoint, node) pair was recorded more than once");
    }

    @Test
    @Order(4)
    void depthIsTheShortestPathForEveryNodeNotJustOne() {
        // Checked across every row, not on a sample. The first version sampled one node and passed
        // when the traversal was switched to depth-first — with the visited check on enqueue, a
        // single node's depth can satisfy the invariant by coincidence. The property is global:
        // for every reachable node, depth must be exactly one more than the shallowest parent that
        // the same endpoint also reaches. Only BFS guarantees that; DFS records whichever path
        // happened to arrive first, which depends on child ordering rather than on the graph.
        Map<String, List<String>> parentsOf = new HashMap<>();
        for (Document e : db.getCollection(Collections.DAG_EDGE).find()
                .projection(new Document("parentRef", 1).append("childRef", 1))) {
            parentsOf.computeIfAbsent(e.getString("childRef"), k -> new ArrayList<>())
                    .add(e.getString("parentRef"));
        }

        // One endpoint's whole reachable set, so the comparison is within a single traversal.
        Document top = db.getCollection(Collections.ENDPOINT_REACHABLE).aggregate(List.of(
                new Document("$group", new Document("_id", "$endpointRef")
                        .append("n", new Document("$sum", 1))),
                new Document("$sort", new Document("n", -1)),
                new Document("$limit", 1))).first();
        assertNotNull(top);
        String endpoint = top.getString("_id");

        Map<String, Integer> depth = new HashMap<>();
        for (Document r : db.getCollection(Collections.ENDPOINT_REACHABLE)
                .find(Filters.eq("endpointRef", endpoint))) {
            depth.put(r.getString("nodeRef"), r.getInteger("depth"));
        }
        assertTrue(depth.size() > 1000, "the sampled endpoint is too small to prove anything");

        int checked = 0;
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, Integer> e : depth.entrySet()) {
            if (e.getValue() == 0) continue;
            int min = Integer.MAX_VALUE;
            for (String p : parentsOf.getOrDefault(e.getKey(), List.of())) {
                Integer pd = depth.get(p);
                if (pd != null) min = Math.min(min, pd);
            }
            if (min == Integer.MAX_VALUE) continue;     // parent outside this endpoint's reach
            checked++;
            if (e.getValue() != min + 1) {
                wrong.add(e.getKey() + " depth=" + e.getValue() + " shallowestParent=" + min);
            }
        }
        assertTrue(checked > 500, "too few nodes had a reachable parent to make this meaningful");
        assertEquals(List.of(), wrong.size() > 5 ? wrong.subList(0, 5) : wrong,
                wrong.size() + " of " + checked + " nodes are not at shortest-path depth");
    }

    @Test
    @Order(5)
    void everyEndpointWithARootHasAReachableSet() {
        long withRoot = db.getCollection(Collections.ENDPOINT)
                .countDocuments(Filters.exists("rootRef", true));
        long covered = db.getCollection(Collections.ENDPOINT_REACHABLE)
                .distinct("endpointRef", String.class).into(new ArrayList<>()).size();
        assertEquals(withRoot, covered,
                "some endpoints with a call tree have no reachability rows");
    }

    @Test
    @Order(6)
    void theRelationIsTraversableFromTheNodeSide() {
        // "Which endpoints reach this node" is the question an impact analysis asks, and it is the
        // direction a tree-shaped store cannot answer at all.
        Document row = db.getCollection(Collections.ENDPOINT_REACHABLE).find().first();
        assertNotNull(row);
        assertTrue(db.getCollection(Collections.ENDPOINT_REACHABLE)
                        .countDocuments(Filters.eq("nodeRef", row.getString("nodeRef"))) > 0,
                "cannot ask which endpoints reach a given node");
    }
}
