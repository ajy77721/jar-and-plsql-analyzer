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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M3 acceptance — {@code am_statement}, the largest collection.
 *
 * <p>The load-bearing assertion is {@link #fivePartKeyIsUniqueInEveryDialect()}. Everything else
 * here would pass with the wrong key; only that one catches the defect the key exists to prevent.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoIngestM3StatementTest {

    private static final Path ORACLE =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;
    private static String runId;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(ORACLE), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_m3_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        runId = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ORACLE)).runId();
    }

    @AfterAll
    static void tearDown() {
        // Dropped here as well as in setUp: dropping only on start leaves every class's data
        // resident for the life of the mongod. Twenty-three classes accumulated 2.1 GB and the
        // OS started killing test runs.
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    /**
     * The five-part key must be unique in every dialect, not just the one we ingest here.
     *
     * <p>Checked against the JSON rather than by ingesting all five analyses: the risk is a key
     * <em>collision</em>, which is a property of the data, and reading it directly tests all five
     * in under a second where five ingests would add three minutes to every build. A slow
     * acceptance test gets skipped, and a skipped test protects nothing.
     *
     * <p>The three-part key is asserted to collide in the same pass, so this test also records
     * <em>why</em> the extra parts are there. If a future change makes three parts sufficient, this
     * fails and someone reads the reason rather than rediscovering it.
     */
    @Test @Order(1)
    @DisplayName("the five-part statement key is unique in all five dialects; three parts is not")
    void fivePartKeyIsUniqueInEveryDialect() throws IOException {
        ObjectMapper json = new ObjectMapper();
        int dialectsChecked = 0, totalThreePartCollisions = 0;

        for (String dialect : List.of("DB2", "MYSQL", "ORACLE", "SQLSERVER", "POSTGRES")) {
            Path index = liveStatementIndex(dialect);
            if (index == null) continue;
            dialectsChecked++;

            JsonNode rows = json.readTree(index.toFile()).path("statements");
            Set<String> five = new HashSet<>(), three = new HashSet<>();
            for (JsonNode r : rows) {
                String node = r.path("nodeId").asText("");
                String op = r.path("operation").asText("");
                String line = r.path("line").asText("");
                three.add(node + "|" + op + "|" + line);
                five.add(node + "|" + op + "|" + line + "|"
                        + r.path("schema").asText("") + "|" + r.path("tableName").asText(""));
            }
            assertEquals(rows.size(), five.size(),
                    dialect + ": the five-part key collides, so ingest would overwrite rows");
            totalThreePartCollisions += rows.size() - three.size();
        }
        assertEquals(5, dialectsChecked, "expected all five live analyses on disk");
        assertTrue(totalThreePartCollisions > 0,
                "the three-part key no longer collides anywhere — if that is genuinely true the "
                + "key could be shortened, but verify it before assuming; it collided on 208 rows "
                + "when this was written");
    }

    @Test @Order(2)
    @DisplayName("every statement is stored, and none was overwritten")
    void allStatementsStored() {
        assertEquals(2436, db.getCollection(Collections.STATEMENT).countDocuments(),
                "ORACLE statement count must match api/statements/index.json");
        // Uniqueness in the database, not just in the source: equal counts prove no upsert
        // collapsed two rows into one.
        long distinct = db.getCollection(Collections.STATEMENT)
                .distinct("entityKey", String.class).into(new ArrayList<>()).size();
        assertEquals(2436, distinct, "two statements shared a key and one overwrote the other");
    }

    @Test @Order(3)
    @DisplayName("SQL text is stored once and referenced, not copied per row")
    void sqlTextIsShared() {
        long texts = db.getCollection(Collections.SQL_TEXT).countDocuments();
        long refs = db.getCollection(Collections.STATEMENT)
                .countDocuments(Filters.exists("sqlTextRef"));
        assertTrue(texts > 0, "no SQL text was stored at all");
        assertTrue(texts < refs,
                "SQL text is not being shared: " + texts + " texts for " + refs + " references. "
                + "Measured duplication is 73%, so equality here means content-addressing broke.");
    }

    @Test @Order(4)
    @DisplayName("the hot query uses an index, not a collection scan")
    void hotQueryIsIndexed() {
        // The query the access-details screen makes: one table's statements, filtered by operation.
        // At 1,000,000 statements a COLLSCAN here is the difference between a page load and a
        // timeout, and nothing in the code would reveal it — only explain() does.
        Document explain = db.getCollection(Collections.STATEMENT)
                .find(Filters.and(Filters.eq("objectRef", "ORACLE~ANALYZER~ERP_PRODUCT"),
                                  Filters.eq("operation", "SELECT")))
                .explain();
        String plan = explain.toJson();
        assertTrue(plan.contains("IXSCAN"),
                "the hot query is not using an index; plan was:\n" + plan.substring(0, Math.min(1200, plan.length())));
        assertFalse(plan.contains("\"stage\": \"COLLSCAN\""),
                "the hot query falls back to a collection scan");
    }

    @Test @Order(5)
    @DisplayName("every statement reaches its run and its table")
    void relationsResolve() {
        long members = db.getCollection("am_run_member").countDocuments(
                Filters.and(Filters.eq("runRef", runId),
                            Filters.eq("entityCollection", Collections.STATEMENT)));
        assertEquals(2436, members, "membership rows must exist for every statement");

        List<String> refs = db.getCollection(Collections.STATEMENT)
                .distinct("objectRef", String.class).into(new ArrayList<>());
        assertFalse(refs.isEmpty(), "no statement resolved to a table at all");
        long orphans = refs.stream()
                .filter(r -> db.getCollection(Collections.DB_OBJECT)
                        .countDocuments(Filters.eq("entityKey", r)) == 0)
                .count();
        assertEquals(0, orphans, "statements pointing at a table that is not stored");
    }

    private static Path liveStatementIndex(String dialect) throws IOException {
        Path root = Path.of("../data/plsql-parse", dialect);
        if (!Files.isDirectory(root)) return null;
        try (Stream<Path> w = Files.walk(root)) {
            return w.filter(p -> p.endsWith(Path.of("api/statements/index.json")))
                    .filter(p -> !p.toString().contains("_2026"))   // archived previous runs
                    .findFirst().orElse(null);
        }
    }
}
