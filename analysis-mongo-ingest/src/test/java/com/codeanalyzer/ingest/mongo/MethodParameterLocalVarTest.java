package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ING-104 — method parameters and local variables, verified against measured counts.
 *
 * <h2>The naming, again</h2>
 *
 * <p>The row names {@code am_method_parameter} and {@code am_method_local_var}. Neither exists: a
 * method's {@code parameters} array is a plain scalar array, so {@code Splitter} auto-splits it
 * into {@code am_java_method__parameters}; local variables are collected explicitly into
 * {@code am_java_method_local_var} by {@code AnalysisJsonMapper}. Same shape as the §17 audit —
 * a plan name the ingest module never produced, on data that is fully present under a different
 * one.
 *
 * <h2>The count</h2>
 *
 * <p>The row states 21,489 parameters. Measured here: <b>22,066</b>. Not a defect — every prior
 * measurement of this fixture in this session's work agrees at 22,066, so the row's figure is
 * simply stale, the same way §12's node and endpoint counts have drifted as fixtures changed.
 *
 * <h2>What "ordinals dense" actually means, checked rather than assumed</h2>
 *
 * <p>Both collections are written by iterating a source array with a counter that increments once
 * per element — {@code Splitter.emitArray} for parameters, an explicit loop for local vars — so
 * density is a property of the iteration, not something that could silently develop a gap under
 * today's code. Verified anyway, because "the code makes this true" and "this is true" are
 * different claims, and the second is the one a reader of this row can act on.
 */
class MethodParameterLocalVarTest {

    private static Path jar;
    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_method_param_test");
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
    void theMeasuredCountsHoldToday() {
        assertEquals(22066, db.getCollection("am_java_method__parameters").countDocuments(),
                "parameter count drifted from the last measured figure — re-measure before "
                        + "trusting this number elsewhere");
        assertEquals(11996, db.getCollection(Collections.JAVA_METHOD_LOCAL_VAR).countDocuments(),
                "local variable count drifted from the last measured figure");
    }

    @Test
    void parameterOrdinalsAreDensePerMethod() {
        assertDense("am_java_method__parameters", "parentRef");
    }

    @Test
    void localVarOrdinalsAreDensePerMethod() {
        // methodRef, not nodeRef — checked against a real stored document rather than assumed
        // from the parameter collection's field name. The two collections are written by
        // different code (auto-split vs. an explicit loop) and do not share a naming convention.
        assertDense(Collections.JAVA_METHOD_LOCAL_VAR, "methodRef");
    }

    /** Every group of rows sharing {@code groupField} has ordinals exactly {@code 0..n-1}, no gaps. */
    private void assertDense(String collection, String groupField) {
        Map<String, Set<Integer>> byParent = new HashMap<>();
        long total = 0;
        for (Document d : db.getCollection(collection).find()) {
            total++;
            String parent = d.getString(groupField);
            Integer ord = d.getInteger("ordinal");
            assertTrue(parent != null && ord != null,
                    collection + ": a row is missing " + groupField + " or ordinal: " + d.toJson());
            byParent.computeIfAbsent(parent, k -> new HashSet<>()).add(ord);
        }
        assertTrue(total > 0, collection + " is empty; this test proves nothing");

        for (var e : byParent.entrySet()) {
            Set<Integer> ords = e.getValue();
            int n = ords.size();
            for (int i = 0; i < n; i++) {
                assertTrue(ords.contains(i),
                        collection + ": " + e.getKey() + " has " + n + " rows but ordinal " + i
                                + " is missing — the sequence has a gap");
            }
        }
    }
}
