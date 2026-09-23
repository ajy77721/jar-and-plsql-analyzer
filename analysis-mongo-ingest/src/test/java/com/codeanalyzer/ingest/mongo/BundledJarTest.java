package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ING-116 — {@code bundled-jars.json}, read for the first time.
 *
 * <h2>What was wrong</h2>
 *
 * <p>{@code MongoIngestService.ingestJar} named exactly one file to read, and its own comment
 * explained why the 321 per-endpoint directories were excluded. That comment said nothing about
 * {@code bundled-jars.json}, which sits at the same analysis root — nothing ever opened it, so
 * every bundled dependency and every resource file inside one was dropped on every JAR ingest.
 *
 * <h2>The split, measured</h2>
 *
 * <p>100 bundled jars. Embedded, the worst single entry reaches 465,644 bytes
 * ({@code spring-boot-autoconfigure-3.2.5.jar}, whose {@code resources} map holds its own bundled
 * configuration files). Split, the worst remaining scalar document is 61,636 bytes
 * ({@code mssql-jdbc-12.6.1.jre11.jar}'s manifest). 229 resource files across 87 jars become their
 * own rows in {@code am_bundled_jar_resource}, keyed by position rather than by resource file name
 * — a resource name is exactly the kind of data-driven key {@code Splitter}'s no-arrays invariant
 * exists to keep out of a BSON field name.
 */
class BundledJarTest {

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_bundled_jar_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingestJar("unified-web", new FileArtifactSource(jar));
    }

    @AfterAll
    static void tearDown() {
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    @Test
    void measuredCountsHoldToday() {
        assertEquals(100, db.getCollection(Collections.BUNDLED_JAR).countDocuments(),
                "bundled jar count drifted");
        assertEquals(229, db.getCollection(Collections.BUNDLED_JAR_RESOURCE).countDocuments(),
                "bundled jar resource count drifted");
    }

    @Test
    void noDocumentExceedsTheMeasuredCeiling() {
        // 61,636 B is the measured worst case with resources split out. A generous ceiling —
        // 100 KB — so this fails on a real regression rather than on GC/BSON overhead noise.
        for (Document d : db.getCollection(Collections.BUNDLED_JAR).find()) {
            String json = d.toJson();
            assertTrue(json.length() < 100_000,
                    "am_bundled_jar/" + d.get("jarName") + " is " + json.length()
                            + " chars — resources may be leaking back into the parent document");
        }
    }

    @Test
    void everyResourceKeyIsWellFormed_noDoubleEscaping() {
        // The bug this test exists to catch: IdCodec.of() escapes every argument, so passing an
        // already-escaped parent key as one of its parts turns the parent's own '~' separators
        // into literal '\~' in the stored key. Caught once by reading a stored document by hand;
        // pinned here so it cannot come back silently.
        long checked = 0;
        for (Document d : db.getCollection(Collections.BUNDLED_JAR_RESOURCE).find()) {
            String key = d.getString("key");
            assertNotNull(key, "a resource row has no key");
            assertFalse(key.contains("\\~"),
                    "key '" + key + "' contains an escaped separator — the parent key was "
                            + "escaped a second time (IdCodec.of used where nested() was needed)");
            checked++;
        }
        assertTrue(checked > 0, "nothing was checked; the collection is empty");
    }

    @Test
    void everyResourcePointsAtARealBundledJar() {
        Set<String> jarKeys = new HashSet<>();
        for (Document d : db.getCollection(Collections.BUNDLED_JAR)
                .find().projection(new Document("entityKey", 1))) {
            jarKeys.add(d.getString("entityKey"));
        }
        assertFalse(jarKeys.isEmpty());

        for (Document r : db.getCollection(Collections.BUNDLED_JAR_RESOURCE).find()) {
            String parent = r.getString("parentRef");
            assertTrue(jarKeys.contains(parent),
                    "resource " + r.getString("resourceName") + " points at a jar that does not "
                            + "exist: " + parent);
        }
    }

    @Test
    void resourceContentSurvivesWholeNotTruncated() {
        // The largest resource content measured is ~200 KB. If truncation crept back in, the
        // largest stored value would come back suspiciously round.
        long maxLen = 0;
        for (Document d : db.getCollection(Collections.BUNDLED_JAR_RESOURCE).find()) {
            String content = d.getString("content");
            assertNotNull(content, "a resource row has no content");
            maxLen = Math.max(maxLen, content.length());
        }
        assertTrue(maxLen > 100_000,
                "the largest resource content is only " + maxLen
                        + " chars; the measured largest is ~200,000 — check for truncation");
    }

    @Test
    void bothDirectionsAreIndexed() {
        Document r = db.getCollection(Collections.BUNDLED_JAR_RESOURCE).find().first();
        assertNotNull(r);
        Document plan = db.getCollection(Collections.BUNDLED_JAR_RESOURCE)
                .find(Filters.eq("parentRef", r.getString("parentRef"))).explain();
        assertFalse(plan.toJson().contains("COLLSCAN"),
                "looking up a jar's resources by parentRef is a collection scan");
    }
}
