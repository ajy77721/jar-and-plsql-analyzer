package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ING-106 — a field's constant value is stored whole.
 *
 * <h2>Why this test exists at all</h2>
 *
 * <p>The registry row for ING-106 named {@code ConstantNotTruncatedTest} as its verification and
 * marked the row done. <b>The class did not exist.</b> The measurement in the row is real — 52
 * fields carry a constant and the largest is 21,845 bytes — but nothing held the property in place,
 * so a truncation added later would have passed every test and the row would still have read
 * "verified by ConstantNotTruncatedTest".
 *
 * <p>That is the same shape as the collections §17 found missing: evidence cited by name, never
 * checked against what is there.
 *
 * <h2>What actually needs guarding</h2>
 *
 * <p>The constant is the one field in the Java model with no natural bound. It is whatever the
 * source declares, and generated code declares enormous ones — the largest here is an ANTLR
 * serialised ATN segment. Truncating it is the tempting fix for a document-size problem, and it is
 * silent: a 21 KB string cut to 1 KB is still a string, still renders, and is simply wrong. So the
 * assertion is on the <b>exact byte length and content</b>, taken from the source file rather than
 * hard-coded.
 *
 * <p>No separate {@code am_java_field_constant} collection is created for this. The row's plan name
 * implied one, but the constant belongs to the field, is read with it, and has no independent
 * identity — a second collection holding a projection of one field would be the same fact in two
 * places, which is the defect the pool-link work had to undo. See registry §19.
 */
class ConstantNotTruncatedTest {

    private static MongoClient client;
    private static MongoDatabase db;
    private static JsonNode analysis;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");

        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_constant_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingestJar("unified-web", new FileArtifactSource(jar));
        analysis = new ObjectMapper().readTree(
                Files.newInputStream(jar.resolve("analysis.json")));
    }

    @AfterAll
    static void tearDown() {
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    /** The longest constant in the source, and the field that declares it. */
    private static String[] longestInSource() {
        String owner = null;
        String field = null;
        String value = null;
        for (JsonNode c : analysis.path("classes")) {
            for (JsonNode f : c.path("fields")) {
                String cv = f.path("constantValue").asText(null);
                if (cv == null || cv.isEmpty()) continue;
                // "className" here returned empty for every class and the lookup below silently
                // matched nothing — the field is fullyQualifiedName. A wrong field name reads as
                // "not ingested", which is the failure this class exists to detect, so it had to
                // be told apart from a real one.
                if (value == null || cv.length() > value.length()) {
                    value = cv;
                    field = f.path("name").asText("");
                    owner = c.path("fullyQualifiedName").asText("");
                }
            }
        }
        return new String[]{owner, field, value};
    }

    @Test
    void theLongestConstantIsStoredByteForByte() {
        String[] src = longestInSource();
        assertNotNull(src[2], "the fixture declares no constants at all");
        // Guards the guard: if the fixture's largest constant were small, storing a truncated
        // value would satisfy the comparison below and this test would prove nothing.
        assertTrue(src[2].length() > 10_000,
                "the largest constant in the fixture is only " + src[2].length()
                        + " bytes, which is too small to detect truncation");

        // Located by taking the longest stored constant rather than by rebuilding the field's key.
        // The first version matched on a regex over `key`, which couples this test to IdCodec's
        // escaping and hashing rules — and when it matched nothing the failure read "the field was
        // not ingested", which is precisely the real defect this class looks for. A test that
        // reports the thing it is hunting for when it is merely miswired is worse than no test.
        String value = null;
        String owner = null;
        for (Document d : db.getCollection(Collections.JAVA_FIELD)
                .find(Filters.exists("constantValue"))) {
            String v = d.getString("constantValue");
            if (v != null && (value == null || v.length() > value.length())) {
                value = v;
                owner = d.getString("name");
            }
        }
        assertNotNull(value, "no field in the database has a constant at all");
        assertEquals(src[1], owner,
                "the longest constant in the database belongs to a different field than in the "
                        + "source");
        assertEquals(src[2].length(), value.length(),
                "the constant was stored at " + value.length() + " of " + src[2].length()
                        + " bytes — something is truncating it");
        assertEquals(src[2], value, "the constant differs from the source byte for byte");
    }

    @Test
    void everyConstantInTheSourceSurvived() {
        int expected = 0;
        for (JsonNode c : analysis.path("classes")) {
            for (JsonNode f : c.path("fields")) {
                String cv = f.path("constantValue").asText(null);
                if (cv != null && !cv.isEmpty()) expected++;
            }
        }
        assertTrue(expected > 0);
        long stored = db.getCollection(Collections.JAVA_FIELD).countDocuments(
                Filters.and(Filters.exists("constantValue"), Filters.ne("constantValue", "")));
        assertEquals(expected, stored,
                "the source has " + expected + " fields with a constant and the database has "
                        + stored + "; constants are being lost, not truncated");
    }

    @Test
    void noConstantLooksLikeItWasCutOff() {
        // A stronger statement than "the longest one is intact": nothing anywhere sits suspiciously
        // at a round byte boundary, which is what a truncation would leave behind.
        for (Document d : db.getCollection(Collections.JAVA_FIELD)
                .find(Filters.exists("constantValue"))
                .sort(Sorts.descending("constantValue"))
                .limit(200)) {
            String v = d.getString("constantValue");
            if (v == null) continue;
            for (int boundary : new int[]{256, 512, 1024, 2048, 4096, 8192}) {
                assertTrue(v.length() != boundary,
                        "field " + d.getString("name") + " has a constant of exactly " + boundary
                                + " bytes, which is where a truncation would land");
            }
        }
    }
}
