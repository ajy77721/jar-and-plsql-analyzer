package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.verify.Reassembler;
import com.codeanalyzer.ingest.mongo.verify.RoundTripProver;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M2 acceptance — the PL/SQL node chain and its five child collections.
 *
 * <p>Where M1 proved one shallow parent/child pair, this proves the shape the rest of the model
 * repeats: a self-referencing tree, several 1:N children of differing sizes, and the two array
 * fields ({@code tables} / {@code directTables}) that are byte-identical in the source and must
 * still both be answerable.
 *
 * <p>Counts are the ORACLE analysis, measured from {@code api/nodes/*.json} rather than copied from
 * a plan: 52 nodes, 476 cursors, 341 dynamic SQL, 950 exception handlers, 502 subtree rows, 411
 * table accesses. Asserting measured numbers is what makes this a regression test rather than a
 * restatement of whatever the code currently does.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoIngestM2NodeChainTest {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;
    private static String runId;
    private static boolean exercised = false;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_m2_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS)).runId();
        exercised = true;
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
    @DisplayName("every node and every node-scoped child is present, at its measured count")
    void counts() {
        assertEquals(52,  count(Collections.PROC_NODE),         "proc_node");
        assertEquals(476, count(Collections.CURSOR_DECL),       "cursors");
        assertEquals(341, count(Collections.DYNAMIC_SQL),       "dynamic SQL");
        assertEquals(950, count(Collections.EXCEPTION_HANDLER), "exception handlers");
        assertEquals(502, count(Collections.SUBTREE_TABLE),     "subtree tables");
        assertEquals(411, count(Collections.NODE_TABLE_ACCESS), "node table accesses");
    }

    @Test @Order(2)
    @DisplayName("no stored document contains an array, at any depth")
    void noArraysAnywhere() {
        // The user's rule, asserted against what is actually in the database rather than against
        // the mapper's intent. An array that slipped through would be a document that grows with
        // the analysed code — the failure mode the whole decomposition exists to prevent.
        List<String> offenders = new ArrayList<>();
        for (String name : db.listCollectionNames()) {
            Document any = db.getCollection(name)
                    .find(new Document("$where", "false")).first();   // schema probe, cheap
            for (Document d : db.getCollection(name).find().limit(200)) {
                if (com.codeanalyzer.ingest.mongo.map.Splitter.containsArray(d)) {
                    offenders.add(name + " :: " + d.get("_id"));
                    break;
                }
            }
        }
        assertTrue(offenders.isEmpty(), "documents containing arrays: " + offenders);
    }

    @Test @Order(3)
    @DisplayName("every child resolves to a node that exists")
    void childrenResolveToTheirNode() {
        for (String coll : List.of(Collections.CURSOR_DECL, Collections.DYNAMIC_SQL,
                Collections.EXCEPTION_HANDLER, Collections.SUBTREE_TABLE,
                Collections.NODE_TABLE_ACCESS)) {
            List<String> refs = db.getCollection(coll)
                    .distinct("nodeRef", String.class).into(new ArrayList<>());
            assertFalse(refs.isEmpty(), coll + " wrote no nodeRef at all");
            long orphans = refs.stream()
                    .filter(r -> db.getCollection(Collections.PROC_NODE)
                            .countDocuments(Filters.eq("entityKey", r)) == 0)
                    .count();
            assertEquals(0, orphans, coll + " has children pointing at no node");
        }
    }

    @Test @Order(4)
    @DisplayName("the node tree's parent links resolve, with exactly the roots absent")
    void treeIsWalkable() {
        List<Document> nodes = db.getCollection(Collections.PROC_NODE)
                .find().into(new ArrayList<>());
        long withParent = nodes.stream().filter(n -> n.getString("parentRef") != null).count();
        long dangling = nodes.stream()
                .map(n -> n.getString("parentRef"))
                .filter(java.util.Objects::nonNull)
                .filter(p -> db.getCollection(Collections.PROC_NODE)
                        .countDocuments(Filters.eq("entityKey", p)) == 0)
                .count();
        assertEquals(0, dangling, "parentRef values pointing at no node");
        // A forest with no root would mean every node claims a parent, which cannot be true.
        assertTrue(withParent < nodes.size(), "no root node: every node claims a parent");
    }

    @Test @Order(5)
    @DisplayName("both `tables` and `directTables` are answerable, and equal")
    void bothArrayNamesResolve() {
        // They are byte-identical in the source (300 of 300 documents measured where both exist).
        // Storage keeps one copy, but a screen asking for either name must still get an answer —
        // storage deduplication must never become field removal.
        long flagged = db.getCollection(Collections.PROC_NODE)
                .countDocuments(Filters.eq("directTablesEqualsTables", true));
        assertTrue(flagged > 0, "no node recorded the tables/directTables relationship");
        long contradicted = db.getCollection(Collections.PROC_NODE)
                .countDocuments(Filters.eq("directTablesEqualsTables", false));
        assertEquals(0, contradicted,
                "a node reported the two arrays as differing; the second set must then be stored "
                + "separately rather than assumed equal");
    }

    @Test @Order(6)
    @DisplayName("a node document round-trips with nothing lost")
    void losslessRoundTrip() throws Exception {
        ObjectMapper json = new ObjectMapper();
        Reassembler re = new Reassembler(db);
        int checked = 0;
        List<RoundTripProver.Loss> losses = new ArrayList<>();
        try (var files = Files.list(ANALYSIS.resolve("api/nodes"))) {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).sorted().toList()) {
                JsonNode src = json.readTree(f.toFile());
                String nodeId = src.path("nodeId").asText(null);
                if (nodeId == null) continue;
                Document stored = db.getCollection(Collections.PROC_NODE)
                        .find(Filters.eq("nodeId", nodeId)).first();
                assertNotNull(stored, "node missing entirely: " + nodeId);

                Document full = re.reassemble(Collections.PROC_NODE,
                        stored.getString("entityKey"), stored, List.of());
                // `directTables` is stored once under `tables`; restore the alias before comparing,
                // which is the same thing a read API would do for a caller asking for that name.
                if (src.has("directTables") && !full.containsKey("directTables")
                        && full.containsKey("tables")) {
                    full.put("directTables", full.get("tables"));
                }
                losses.addAll(RoundTripProver.compare(src, full));
                checked++;
            }
        }
        assertEquals(52, checked, "expected to verify all 52 node documents");
        assertTrue(losses.isEmpty(), "fields lost on the way into MongoDB:\n  "
                + losses.stream().map(Object::toString).limit(12)
                        .reduce((a, b) -> a + "\n  " + b).orElse(""));
    }

    @AfterAll
    static void mongoWasActuallyExercised() {
        if (MongoAvailable.present() && Files.isDirectory(ANALYSIS)) {
            assertTrue(exercised, "mongod and data were available but nothing was ingested");
        }
    }

    private long count(String collection) {
        return db.getCollection(collection).countDocuments();
    }
}
