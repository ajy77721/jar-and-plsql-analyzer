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
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ING-113 — three parallel structures become one relation.
 *
 * <h2>What was actually stored, and why it was worse than "not normalised"</h2>
 *
 * <p>An endpoint carried {@code aggregatedCollections} (an array of collection names) plus
 * {@code aggregatedCollectionDomains} and {@code aggregatedOperations} — two maps keyed by those
 * same names. The array was split out correctly; the two maps were embedded as-is, which makes
 * <b>field names out of data</b>. Two consequences, both silent:
 *
 * <ul>
 *   <li>The key set grows without bound — exactly the failure the no-arrays rule exists to prevent.
 *       It never fired, because a map is not an array.</li>
 *   <li><b>69 of the 155</b> collection names measured here contain a dot
 *       ({@code information_schema.ROUTINES}). A dotted query path onto such a field is ambiguous
 *       with a nested document, so those values could not be addressed at all.</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EndpointCollectionTest {

    /** Measured from the analysis on disk. */
    private static final long EXPECTED_PAIRS = 155;
    private static final long EXPECTED_DOTTED = 69;

    private static MongoClient client;
    private static MongoDatabase db;
    private static Path jarDir;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        jarDir = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jarDir != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_epcoll_test");
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

    @Test
    @Order(1)
    void everyEndpointCollectionPairBecomesARow() {
        assertEquals(EXPECTED_PAIRS,
                db.getCollection(Collections.ENDPOINT_COLLECTION).countDocuments(),
                "one row per (endpoint, collection) is the whole contract");
    }

    @Test
    @Order(2)
    void theTwoSideMapsAreJoinedOntoTheRow() {
        // If the join were dropped, the rows would exist and be useless: the collection name alone
        // does not say whether the endpoint reads or writes it.
        long withDomain = db.getCollection(Collections.ENDPOINT_COLLECTION)
                .countDocuments(Filters.exists("domain", true));
        long withOperation = db.getCollection(Collections.ENDPOINT_COLLECTION)
                .countDocuments(Filters.exists("operation", true));
        assertEquals(EXPECTED_PAIRS, withDomain, "domains were not joined onto the rows");
        assertTrue(withOperation > 0, "operations were not joined onto the rows");
    }

    @Test
    @Order(3)
    void theDataKeyedMapsAreGoneFromTheEndpoint() {
        // Leaving them would keep the unbounded-field-name problem while adding a second copy.
        assertEquals(0, db.getCollection(Collections.ENDPOINT)
                        .countDocuments(Filters.exists("aggregatedCollectionDomains", true)),
                "the domain map is still embedded, keyed by collection name");
        assertEquals(0, db.getCollection(Collections.ENDPOINT)
                        .countDocuments(Filters.exists("aggregatedOperations", true)),
                "the operation map is still embedded, keyed by collection name");
    }

    @Test
    @Order(4)
    void collectionNamesContainingDotsSurviveAsValues() {
        // The sharpest case. As a field name, "information_schema.ROUTINES" is unaddressable; as a
        // field VALUE it is ordinary text. That difference is most of the point of this task.
        long dotted = db.getCollection(Collections.ENDPOINT_COLLECTION)
                .countDocuments(Filters.regex("collectionName", "\\."));
        assertEquals(EXPECTED_DOTTED, dotted,
                "collection names containing dots did not survive the conversion");

        Document one = db.getCollection(Collections.ENDPOINT_COLLECTION)
                .find(Filters.eq("collectionName", "information_schema.ROUTINES")).first();
        assertNotNull(one, "a dotted collection name cannot be queried by equality");
    }

    @Test
    @Order(5)
    void theRelationIsTraversableInBothDirections() {
        // "Which collections does this endpoint touch" and "which endpoints touch this collection"
        // are both real questions; a relation indexed one way answers half of them.
        Document row = db.getCollection(Collections.ENDPOINT_COLLECTION).find().first();
        assertNotNull(row);
        assertTrue(db.getCollection(Collections.ENDPOINT_COLLECTION)
                        .countDocuments(Filters.eq("endpointRef", row.getString("endpointRef"))) > 0,
                "cannot look up an endpoint's collections");
        assertTrue(db.getCollection(Collections.ENDPOINT_COLLECTION)
                        .countDocuments(Filters.eq("collectionName", row.getString("collectionName"))) > 0,
                "cannot look up a collection's endpoints");

        Set<String> indexed = new HashSet<>();
        db.getCollection(Collections.ENDPOINT_COLLECTION).listIndexes()
                .forEach(i -> indexed.add(((Document) i.get("key")).keySet().iterator().next()));
        assertTrue(indexed.contains("endpointRef"), "no index from endpoint to collection");
        assertTrue(indexed.contains("collectionName"), "no index from collection to endpoint");
    }

    @Test
    @Order(6)
    void everyRowPointsAtAnEndpointThatExists() {
        for (Document r : db.getCollection(Collections.ENDPOINT_COLLECTION).find().limit(200)) {
            assertNotNull(db.getCollection(Collections.ENDPOINT)
                            .find(Filters.eq("entityKey", r.getString("endpointRef"))).first(),
                    "endpoint-collection row points at a missing endpoint: " + r.getString("key"));
        }
    }

    @Test
    @Order(7)
    void theSourceCountsAgreeWithWhatWasStored() throws Exception {
        // Read from the analysis actually ingested, so the expectation cannot drift from the data.
        JsonNode root = new ObjectMapper().readTree(
                Files.newInputStream(jarDir.resolve("analysis.json")));
        long pairs = 0;
        for (JsonNode e : root.path("endpoints")) {
            JsonNode names = e.path("aggregatedCollections");
            if (names.isArray()) pairs += names.size();
        }
        assertEquals(pairs, db.getCollection(Collections.ENDPOINT_COLLECTION).countDocuments(),
                "stored rows do not match the pairs in the analysis that was ingested");
    }
}
