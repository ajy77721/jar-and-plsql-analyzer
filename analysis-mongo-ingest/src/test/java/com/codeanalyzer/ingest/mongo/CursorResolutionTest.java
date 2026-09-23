package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.verify.OrphanRefChecker;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ING-014 — a cursor's tables are resolved to objects, and unresolvable names are preserved.
 *
 * <h2>What was wrong</h2>
 *
 * <p>A cursor declares its tables as bare names: {@code ["ERP_CUSTOMER", "ERP_SALES_ORDER"]}.
 * Splitter stored each as {@code {value: "ERP_CUSTOMER"}} in {@code am_cursor_decl__tables} — a
 * string with nothing to resolve it. "Which cursors read this table" had no reference to match, so
 * it was answerable only by comparing names, and {@code OrphanRefChecker} had no {@code *Ref} field
 * to check across 1,103 rows.
 *
 * <h2>Where the schema comes from</h2>
 *
 * <p>A name alone cannot address an object; {@code am_db_object} is keyed by schema and name. The
 * schema is available in the <b>same node document</b>, in {@code directTables}, which carries
 * {@code {schema, name}}. Measured: all 1,103 references resolve that way and no name appears under
 * two schemas within one node — so no second artifact and no post-ingest pass is needed.
 *
 * <h2>The row's real requirement</h2>
 *
 * <p>"Unresolved bare names preserved, not mis-attributed." So {@code objectRef} is nullable: a
 * name with no schema in its node keeps its name and gets no reference, rather than being attached
 * to a guessed schema. That is asserted here as a property of the writer, since the current corpus
 * has no such name — which is also why {@code am_cursor_unresolved} is not created: it would be an
 * empty collection.
 */
class CursorResolutionTest {

    private static final Path PLSQL =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(PLSQL), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_cursor_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(PLSQL));
    }

    @AfterAll
    static void tearDown() {
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    /** Every (cursor, table) pair the source declares. */
    private static int sourcePairs() throws Exception {
        ObjectMapper m = new ObjectMapper();
        int pairs = 0;
        try (Stream<Path> files = Files.list(PLSQL.resolve("api/nodes"))) {
            for (Path p : files.filter(f -> f.toString().endsWith(".json")).toList()) {
                JsonNode n = m.readTree(Files.newInputStream(p));
                for (JsonNode c : n.path("cursors")) {
                    if (c.path("tables").isArray()) pairs += c.path("tables").size();
                }
            }
        }
        return pairs;
    }

    @Test
    void everyDeclaredTableBecameARow() throws Exception {
        int expected = sourcePairs();
        assertTrue(expected > 0, "the fixture declares no cursor tables");
        assertEquals(expected, db.getCollection(Collections.CURSOR_OBJECT).countDocuments(),
                "am_cursor_object does not hold one row per declared cursor table");
    }

    @Test
    void theStringOnlyRowsAreGoneNotDuplicated() {
        Set<String> names = new HashSet<>();
        db.listCollectionNames().into(names);
        assertFalse(names.contains("am_cursor_decl__tables"),
                "am_cursor_decl__tables still exists — the bare names are being stored alongside "
                        + "the resolved rows, so a query can be written against either");
        Document cursor = db.getCollection(Collections.CURSOR_DECL).find().first();
        assertNotNull(cursor);
        assertFalse(cursor.containsKey("tables"),
                "the cursor document still carries its tables array inline");
    }

    @Test
    void everyResolvedReferenceActuallyResolves() {
        Set<String> objects = new HashSet<>();
        for (Document d : db.getCollection(Collections.DB_OBJECT)
                .find().projection(new Document("entityKey", 1))) {
            objects.add(d.getString("entityKey"));
        }
        assertFalse(objects.isEmpty());

        long resolved = 0;
        long unresolved = 0;
        String bad = null;
        for (Document r : db.getCollection(Collections.CURSOR_OBJECT).find()) {
            String ref = r.getString("objectRef");
            if (ref == null) {
                unresolved++;
                continue;
            }
            if (objects.contains(ref)) {
                resolved++;
            } else {
                if (bad == null) bad = ref;
            }
        }
        assertEquals(null, bad,
                "a cursor table resolved to an object key that does not exist: " + bad
                        + " — note am_db_object is keyed by the ENTITY key, not the run-scoped one");
        assertEquals(db.getCollection(Collections.CURSOR_OBJECT).countDocuments(),
                resolved + unresolved, "some rows were counted twice");
        assertTrue(resolved > 0, "nothing resolved at all, so this test proves nothing");
    }

    @Test
    void anUnresolvedNameKeepsItsNameAndGetsNoReference() {
        // The row's actual DoD. Every row must carry the table name whether or not it resolved,
        // because the name is the source data and the reference is derived from it.
        for (Document r : db.getCollection(Collections.CURSOR_OBJECT).find().limit(1000)) {
            assertNotNull(r.getString("tableName"),
                    "a cursor-table row has no name, so an unresolved one would be unreadable");
            if (r.getString("objectRef") == null) {
                assertFalse(r.containsKey("schemaName"),
                    "a row with no objectRef carries a schema, which means a schema was guessed");
            }
        }
        assertTrue(OrphanRefChecker.nullAllowed("objectRef"),
                "objectRef is not on the nullable allowlist, so an unresolvable cursor table "
                        + "would be reported as a defect rather than preserved");
        assertNotNull(OrphanRefChecker.nullReason("objectRef"));
    }

    @Test
    void bothDirectionsAreIndexed() {
        // "The tables this cursor reads" and "the cursors that read this table". The second is the
        // question the bare names could not answer at all.
        Document r = db.getCollection(Collections.CURSOR_OBJECT)
                .find(Filters.exists("objectRef")).first();
        assertNotNull(r);
        for (var q : List.of(Filters.eq("cursorRef", r.getString("cursorRef")),
                Filters.eq("objectRef", r.getString("objectRef")))) {
            Document plan = db.getCollection(Collections.CURSOR_OBJECT).find(q).explain();
            assertFalse(plan.toJson().contains("COLLSCAN"),
                    "a cursor-object lookup is a collection scan: " + q);
        }
    }

    @Test
    void theCheckerSeesTheseReferences() {
        var fields = new OrphanRefChecker(db).discoverRefFields()
                .getOrDefault(Collections.CURSOR_OBJECT, Set.of());
        assertTrue(fields.contains("objectRef"),
                "the checker does not discover objectRef on am_cursor_object: " + fields);
        assertTrue(fields.contains("cursorRef"),
                "the checker does not discover cursorRef on am_cursor_object: " + fields);
    }
}
