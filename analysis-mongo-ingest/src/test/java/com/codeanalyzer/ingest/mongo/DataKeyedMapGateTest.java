package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB-902 — a map keyed by data is the same defect as an array, and was never checked.
 *
 * <h2>The blind spot</h2>
 *
 * <p>The no-arrays rule exists because an array grows without bound. A map keyed by data grows
 * without bound in exactly the same way, and `containsArray` returned false for every one of them
 * — <b>a map is not an array</b>. Two reached the database before the rule was extended:
 *
 * <ul>
 *   <li>an endpoint's `aggregatedCollectionDomains`, keyed by collection name — 69 of 155 names
 *       contain a dot (ING-113);</li>
 *   <li>a method's `localVariables`, keyed by variable name — 11,996 keys over 2,857 methods, the
 *       widest declaring 99;</li>
 *   <li>and a DAG node's `collectionDomains` / `collectionSources` / `collectionVerification`,
 *       found by this gate on its first run.</li>
 * </ul>
 *
 * <h2>Two harms, both silent</h2>
 *
 * <p>A key containing a dot is <b>unaddressable</b>: the query path is ambiguous with a nested
 * document. And "which methods declare `conn`" cannot use an index when the name is a field rather
 * than a value.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DataKeyedMapGateTest {

    @Test
    @Order(1)
    void aDottedKeyIsRejected() {
        Document doc = new Document("_id", "x").append("nested",
                new Document("information_schema.ROUTINES", "READ"));
        String found = Splitter.dataKeyedMap(doc);
        assertNotNull(found, "a key containing a dot was accepted");
        assertTrue(found.contains("dot"), "the reason does not name the problem: " + found);
    }

    @Test
    @Order(2)
    void aDollarPrefixedKeyIsRejected() {
        // '$' is legal in a Java identifier, so a local variable named $tmp is reachable input.
        Document doc = new Document("_id", "x").append("nested", new Document("$tmp", 1));
        assertNotNull(Splitter.dataKeyedMap(doc), "a key beginning with '$' was accepted");
    }

    @Test
    @Order(3)
    void anUnboundedKeySetIsRejected() {
        Document wide = new Document();
        for (int i = 0; i <= Splitter.MAX_SCHEMA_KEYS; i++) wide.append("var_" + i, i);
        String found = Splitter.dataKeyedMap(new Document("_id", "x").append("locals", wide));
        assertNotNull(found, "a map with an unbounded key set was accepted");
        assertTrue(found.contains("keys"), "the reason does not name the problem: " + found);
    }

    @Test
    @Order(4)
    void anOrdinarySchemaObjectIsAccepted() {
        // A rule that rejects legitimate documents is worse than no rule. Every stored nested
        // object is a fixed schema shape and must pass.
        Document doc = new Document("_id", "x")
                .append("nested", new Document("schema", "APP").append("name", "ORDERS")
                        .append("objectType", "TABLE"))
                .append("scalar", 5);
        assertNull(Splitter.dataKeyedMap(doc), "a plain schema object was rejected");
    }

    @Test
    @Order(5)
    void theTopLevelDocumentIsExemptFromTheWidthRule() {
        // Top-level field names are the schema. A wide row is a wide row; only NESTED objects can
        // be maps keyed by data.
        Document wide = new Document("_id", "x");
        for (int i = 0; i <= Splitter.MAX_SCHEMA_KEYS; i++) wide.append("field_" + i, i);
        assertNull(Splitter.dataKeyedMap(wide), "a wide top-level document was rejected");
    }

    @Test
    @Order(6)
    void theWriterRefusesADocumentCarryingADataKeyedMap() throws Exception {
        // The rule is only worth anything if the write path consults it. Removing the check from
        // BulkWriter left every other assertion here passing — they exercise the rule, and
        // assertDoesNotThrow on a clean ingest passes especially well when nothing can throw.
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        try (MongoClient client = MongoClients.create(MongoAvailable.URI)) {
            MongoDatabase db = client.getDatabase("am_datakeyed_writer_test");
            db.drop();
            new com.codeanalyzer.ingest.mongo.schema.SchemaInstaller(db).install();

            var stats = new com.codeanalyzer.ingest.mongo.write.WriteStats();
            try (var writer = new com.codeanalyzer.ingest.mongo.write.BulkWriter(db, stats)) {
                Document offender = new Document("_id", "probe").append("key", "probe")
                        .append("locals", new Document("information_schema.ROUTINES", "READ"));
                var e = assertThrows(IllegalStateException.class,
                        () -> writer.upsert(Collections.DAG_NODE, offender),
                        "the writer accepted a document whose field names come from data");
                assertTrue(e.getMessage().contains("dot"),
                        "the refusal does not say what is wrong: " + e.getMessage());
            }
            db.drop();
        }
    }

    @Test
    @Order(7)
    void aRealJarIngestProducesNoDataKeyedMaps() throws Exception {
        // The gate runs on every write, so a clean ingest is itself the assertion — but stated
        // explicitly, because this is the check that found the DAG-node maps.
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");

        try (MongoClient client = MongoClients.create(MongoAvailable.URI)) {
            MongoDatabase db = client.getDatabase("am_datakeyed_gate_test");
            db.drop();
            MongoIngestService svc = new MongoIngestService(db);
            svc.installSchema();
            assertDoesNotThrow(() -> svc.ingestJar("unified-web", new FileArtifactSource(jar)),
                    "a real JAR ingest still writes a map keyed by data");

            // And the rows those maps became are actually there.
            assertTrue(db.getCollection(Collections.JAVA_METHOD_LOCAL_VAR).countDocuments() > 10_000,
                    "local variables were dropped rather than split into rows");
            assertTrue(db.getCollection(Collections.DAG_NODE_COLLECTION).countDocuments() > 0,
                    "DAG node collections were dropped rather than split into rows");

            // The dotted names survive as values, which is the whole point.
            List<String> dotted = db.getCollection(Collections.DAG_NODE_COLLECTION)
                    .distinct("collectionName", Filters.regex("collectionName", "\\."), String.class)
                    .into(new ArrayList<>());
            assertFalse(dotted.isEmpty(),
                    "no dotted collection name survived; as field names they were unaddressable, "
                            + "as values they must be queryable");
            db.drop();
        }
    }
}
