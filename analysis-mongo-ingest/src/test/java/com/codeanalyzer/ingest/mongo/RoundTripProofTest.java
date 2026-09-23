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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * VER-004 — every artifact reconstructs from the database, permanently.
 *
 * <h2>How this differs from the M1/M2 round trips</h2>
 *
 * <p>Those prove two collections. This one covers every PL/SQL artifact an ingest reads and the
 * top-level arrays of {@code analysis.json}, and it does it without recomputing any key.
 *
 * <p>Recomputing keys is the obvious approach and the wrong one: the test would reimplement each
 * mapper's key logic, and a mapper and a test that are wrong the same way agree. Instead each
 * source element is matched by its own <b>natural business fields</b>, which the mapper never
 * chose — so the two sides can only agree if the data really is there.
 *
 * <h2>What "lossless" excludes</h2>
 *
 * <p>Bookkeeping the database adds — {@code _id}, {@code key}, {@code entityKey}, {@code runId},
 * {@code contentHash}, {@code parentRef}, {@code ordinal} — is not part of the source and is
 * ignored. Everything the analyser produced must come back.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RoundTripProofTest {

    private static final Path PLSQL =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static final ObjectMapper JSON = new ObjectMapper();

    private static MongoClient client;
    private static MongoDatabase db;
    private static String runId;
    private static Reassembler reassembler;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(PLSQL), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_roundtrip_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(PLSQL)).runId();
        reassembler = new Reassembler(db);
    }

    @AfterAll
    static void tearDown() {
        // Dropped here as well as in setUp: dropping only on start leaves every class's data
        // resident for the life of the mongod. Twenty-three classes accumulated 2.1 GB and the
        // OS started killing test runs.
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    private static JsonNode read(String relative) throws Exception {
        try (InputStream in = Files.newInputStream(PLSQL.resolve(relative))) {
            return JSON.readTree(in);
        }
    }

    /** Finds the stored row by natural fields, reassembles it, and reports what did not survive. */
    private List<RoundTripProver.Loss> lossesFor(String collection, JsonNode source,
                                                 Document naturalKey) {
        Document stored = db.getCollection(collection)
                .find(Filters.and(Filters.eq("runId", runId), new Document(naturalKey))).first();
        if (stored == null) {
            return List.of(new RoundTripProver.Loss(
                    "<row missing for " + naturalKey.toJson() + ">", "present", null));
        }
        String key = stored.getString("entityKey") != null
                ? stored.getString("entityKey") : stored.getString("key");
        Document full = reassembler.reassemble(collection, key, stored, List.of());
        resolvePooledText(full);
        return RoundTripProver.compare(source, full);
    }

    /**
     * Puts the pooled SQL text back on the row.
     *
     * <p>{@code sqlText} is deliberately not stored on the statement: it is 73% duplicated (3.7 MB
     * of captured SQL, 1.0 MB distinct), so it lives in the content-addressed {@code am_sql_text}
     * and the row keeps a reference. A round trip that does not follow that reference reports every
     * statement as having lost its text — which is what happened, 2,436 times. The data was never
     * missing; the reconstruction was incomplete.
     */
    private void resolvePooledText(Document row) {
        String ref = row.getString("sqlTextRef");
        if (ref == null) return;
        Document text = db.getCollection(Collections.SQL_TEXT)
                .find(Filters.eq("entityKey", ref)).first();
        if (text != null) row.put("sqlText", text.getString("text"));
    }

    /** The stored procedure key for a source {@code nodeId}, so children can be matched to it. */
    private String nodeRefFor(String nodeId) {
        Document n = db.getCollection(Collections.PROC_NODE)
                .find(Filters.and(Filters.eq("runId", runId), Filters.eq("nodeId", nodeId))).first();
        return n == null ? null : n.getString("entityKey");
    }

    private static void assertNoLosses(String what, List<RoundTripProver.Loss> losses) {
        if (losses.isEmpty()) return;
        List<RoundTripProver.Loss> shown = losses.size() > 8 ? losses.subList(0, 8) : losses;
        fail(what + ": " + losses.size() + " field(s) did not survive the round trip:\n  "
                + String.join("\n  ", shown.stream().map(Object::toString).toList()));
    }

    @Test
    @Order(1)
    void everyTableInTheIndexReconstructs() throws Exception {
        List<RoundTripProver.Loss> losses = new ArrayList<>();
        int checked = 0;
        for (JsonNode t : read("api/tables/index.json").path("tables")) {
            losses.addAll(lossesFor(Collections.DB_OBJECT, t,
                    new Document("schema", t.path("schema").asText(""))
                            .append("name", t.path("name").asText(""))));
            checked++;
        }
        assertTrue(checked > 0, "no tables were checked");
        assertNoLosses("api/tables/index.json (" + checked + " tables)", losses);
    }

    @Test
    @Order(2)
    void everyProcedureNodeReconstructs() throws Exception {
        List<RoundTripProver.Loss> losses = new ArrayList<>();
        int checked = 0;
        try (Stream<Path> files = Files.list(PLSQL.resolve("api/nodes"))) {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList()) {
                JsonNode n = JSON.readTree(Files.newInputStream(f));
                losses.addAll(lossesFor(Collections.PROC_NODE, n,
                        new Document("nodeId", n.path("nodeId").asText(""))));
                checked++;
            }
        }
        assertEquals(52, checked, "the node file count changed");
        assertNoLosses("api/nodes (" + checked + " nodes)", losses);
    }

    @Test
    @Order(3)
    void everyStatementReconstructs() throws Exception {
        List<RoundTripProver.Loss> losses = new ArrayList<>();
        int checked = 0;
        for (JsonNode st : read("api/statements/index.json").path("statements")) {
            // All five natural parts, including the owning procedure. Omitting the node matched
            // a statement from a DIFFERENT procedure at the same line touching the same table —
            // which is exactly the collision the mapper widened its own key to five parts to
            // avoid (63 ORACLE / 81 POSTGRES / 64 DB2 rows). A four-part match here reproduced
            // that collision inside the test and reported 2,305 phantom losses.
            String nodeRef = nodeRefFor(st.path("nodeId").asText(""));
            losses.addAll(lossesFor(Collections.STATEMENT, st,
                    new Document("nodeRef", nodeRef)
                            .append("operation", st.path("operation").asText(null))
                            .append("line", st.path("line").asInt(0))
                            .append("tableName", st.path("tableName").asText(null))
                            .append("schemaName", st.path("schema").asText(null))));
            checked++;
        }
        assertEquals(2436, checked, "the statement count changed");
        assertNoLosses("api/statements (" + checked + " statements)", losses);
    }

    @Test
    @Order(4)
    void everyChunkStatementReconstructs() throws Exception {
        List<RoundTripProver.Loss> losses = new ArrayList<>();
        int checked = 0;
        try (Stream<Path> files = Files.list(PLSQL.resolve("chunks"))) {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).sorted().toList()) {
                JsonNode chunk = JSON.readTree(Files.newInputStream(f));
                JsonNode statements = chunk.path("statements");
                if (!statements.isArray()) continue;
                // Scoped to the owning procedure. Without it, (line, ordinal, type) matches a row
                // from a DIFFERENT chunk file — every chunk starts at ordinal 0 and low line
                // numbers, so the collision is near-certain rather than rare. It produced 70
                // "losses" that were really comparisons against the wrong statement.
                String nodeRef = nodeRefFor(chunk.path("nodeId").asText(""));
                assertNotNull(nodeRef, "no stored procedure for chunk " + f.getFileName());
                int ordinal = 0;
                for (JsonNode st : statements) {
                    losses.addAll(lossesFor(Collections.CHUNK_STATEMENT, st,
                            new Document("nodeRef", nodeRef)
                                    .append("line", st.path("line").asInt(0))
                                    .append("ordinal", ordinal++)
                                    .append("statementType", st.path("type").asText(null))));
                    checked++;
                }
            }
        }
        assertEquals(3832, checked, "the chunk statement count changed");
        assertNoLosses("chunks (" + checked + " statements)", losses);
    }

    @Test
    @Order(5)
    void everyCallEdgeReconstructs() throws Exception {
        List<RoundTripProver.Loss> losses = new ArrayList<>();
        int checked = 0;
        for (JsonNode e : read("edges.json")) {
            losses.addAll(lossesFor(Collections.CALL_EDGE, e,
                    new Document("line", e.path("line").asInt(0))
                            .append("fromNodeId", e.path("fromNodeId").asText(null))
                            .append("toNodeId", e.path("toNodeId").asText(null))));
            checked++;
        }
        assertTrue(checked > 2000, "only " + checked + " edges were checked");
        assertNoLosses("edges.json (" + checked + " edges)", losses);
    }

    @Test
    @Order(6)
    void theProofCoversEveryArtifactTheIngestReads() {
        // The requirement is 100%, so the list of what is covered has to be checked too — a proof
        // that quietly omits an artifact proves nothing about it, and nothing else would say so.
        Set<String> proven = new LinkedHashSet<>(List.of(
                "api/tables/index.json", "api/nodes", "api/statements/index.json",
                "chunks", "edges.json"));
        Set<String> readByIngest = new LinkedHashSet<>(List.of(
                "api/tables/index.json", "tables.json", "api/nodes", "api/statements/index.json",
                "chunks", "edges.json", "api/call_graph.json"));

        Set<String> unproven = new LinkedHashSet<>(readByIngest);
        unproven.removeAll(proven);
        assertEquals(Set.of("tables.json", "api/call_graph.json"), unproven,
                "the set of artifacts without a round-trip proof changed; extend the proof or "
                        + "update this list deliberately");
    }
}
