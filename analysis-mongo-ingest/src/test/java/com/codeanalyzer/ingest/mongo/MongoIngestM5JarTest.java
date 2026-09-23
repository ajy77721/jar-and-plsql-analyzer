package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M5 acceptance — the Java model, and the document-size property that motivated all of it.
 *
 * <p>Measured source: 1,633 classes, 33,409 methods, 8,649 fields, 64,443 pooled invocation targets
 * referenced 111,467 times.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoIngestM5JarTest {

    private static MongoClient client;
    private static MongoDatabase db;
    private static Path jarDir;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        jarDir = newestJarAnalysis();
        Assumptions.assumeTrue(jarDir != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_ingest_m5_test");
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

    /** Counts read from the analysis actually ingested, so the two can never disagree. */
    private record SourceCounts(int classes, int methods, int fields, int pool, int refs) {}

    private static SourceCounts sourceCounts() throws Exception {
        com.fasterxml.jackson.databind.JsonNode root =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readTree(jarDir.resolve("analysis.json").toFile());
        int classes = 0, methods = 0, fields = 0, refs = 0;
        for (com.fasterxml.jackson.databind.JsonNode c : root.path("classes")) {
            classes++;
            methods += c.path("methods").size();
            fields += c.path("fields").size();
            for (com.fasterxml.jackson.databind.JsonNode m : c.path("methods")) {
                refs += m.path("invocationRefs").size();
            }
        }
        return new SourceCounts(classes, methods, fields, root.path("invocationPool").size(), refs);
    }

    @Test @Order(1)
    @DisplayName("classes, methods, fields and the invocation pool are all stored")
    void everythingStored() throws Exception {
        // Expectations are derived from the file under test rather than written as literals. A
        // literal plus a directory-mtime file selector previously manufactured a phantom "two
        // methods were dropped" defect — the count came from one analysis, the ingest from
        // another, and the mapper was correct all along.
        SourceCounts src = sourceCounts();
        assertEquals(src.classes(), db.getCollection(Collections.JAVA_CLASS).countDocuments(), "classes");
        assertEquals(src.methods(), db.getCollection(Collections.JAVA_METHOD).countDocuments(), "methods");
        assertEquals(src.fields(), db.getCollection(Collections.JAVA_FIELD).countDocuments(), "fields");
        assertEquals(src.pool(), db.getCollection(Collections.INVOCATION).countDocuments(),
                "pooled invocation targets");
    }

    @Test @Order(2)
    @DisplayName("the invocation junction keeps 111,467 references over 64,443 targets")
    void junctionPreservesSharing() throws Exception {
        long junction = db.getCollection(Collections.METHOD_INVOCATION).countDocuments();
        long targets = db.getCollection(Collections.INVOCATION).countDocuments();
        assertEquals(sourceCounts().refs(), junction,
                "every invocation reference must survive as a junction row");
        assertTrue(junction > targets,
                "the pool is not being shared: " + junction + " refs over " + targets + " targets");
    }

    @Test @Order(3)
    @DisplayName("no class document is anywhere near the BSON limit")
    void classDocumentsAreBounded() {
        // The reason methods and fields are referenced rather than embedded. Embedded, the worst
        // class measured 5,232,804 bytes — 31% of the 16 MB limit in one document, growing with a
        // code generator. The mean was 14 KB, which is exactly why the mean is the wrong statistic.
        Document worst = null;
        int worstSize = 0;
        for (Document d : db.getCollection(Collections.JAVA_CLASS).find()) {
            int size = d.toJson().length();
            if (size > worstSize) { worstSize = size; worst = d; }
        }
        assertNotNull(worst);
        assertTrue(worstSize < 64 * 1024,
                "largest class document is " + worstSize + " bytes ("
                        + worst.getString("fullyQualifiedName") + "); an array has crept back in");
    }

    @Test @Order(4)
    @DisplayName("no stored JAR document contains an array")
    void noArrays() {
        for (String coll : java.util.List.of(Collections.JAVA_CLASS, Collections.JAVA_METHOD,
                Collections.JAVA_FIELD, Collections.INVOCATION, Collections.METHOD_INVOCATION)) {
            for (Document d : db.getCollection(coll).find().limit(300)) {
                assertFalse(Splitter.containsArray(d),
                        coll + " document " + d.get("_id") + " contains an array");
            }
        }
    }

    @Test @Order(5)
    @DisplayName("every method resolves to a class, by index")
    void methodsResolveToClasses() {
        Document plan = db.getCollection(Collections.JAVA_METHOD)
                .find(Filters.eq("classRef", "x")).explain();
        assertTrue(plan.toJson().contains("IXSCAN"),
                "methods are not indexed by classRef; at 34M rows this is a full scan");

        Document m = db.getCollection(Collections.JAVA_METHOD).find().first();
        assertNotNull(m);
        assertEquals(1, db.getCollection(Collections.JAVA_CLASS)
                        .countDocuments(Filters.eq("entityKey", m.getString("classRef"))),
                "a method points at a class that is not stored");
    }

    /**
     * The most recent JAR analysis, chosen by the mtime of {@code analysis.json} itself.
     *
     * <p>Sorting by the <em>directory</em> mtime picked the wrong one: a directory is touched by
     * anything that writes inside it, so an older analysis whose folder was touched later won. The
     * test then ingested one file and asserted counts taken from another, which looked exactly
     * like the mapper silently dropping two methods and cost a long hunt to disprove.
     */
    static Path newestJarAnalysis() throws Exception {
        Path root = Path.of("../data/jar");
        if (!Files.isDirectory(root)) return null;
        try (Stream<Path> s = Files.list(root)) {
            return s.filter(Files::isDirectory)
                    .filter(p -> Files.isRegularFile(p.resolve("analysis.json")))
                    .max(Comparator.comparingLong(
                            p -> p.resolve("analysis.json").toFile().lastModified()))
                    .orElse(null);
        }
    }
}
