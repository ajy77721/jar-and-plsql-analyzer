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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ING-117 (part 1 of 3) — {@code jar-dep-map.json}, read for the first time.
 *
 * <h2>Not embedded, though the plan recommended it</h2>
 *
 * <p>{@code RELATIONAL-MODEL-PLAN.md} models this as {@code bundled_jar.dependsOn[]}. Split
 * anyway, for the reason recorded in {@code JarDependencyMapper}'s own javadoc: the no-arrays
 * invariant is structural in this module, and "bounded today" is a property of the data.
 *
 * <h2>The two rows this test does not expect to resolve</h2>
 *
 * <p>18 jars named in the dependency map, 16 of them also cataloged in {@code bundled-jars.json}.
 * The other two — {@code flow-poc-1.0.0-SNAPSHOT.jar} and
 * {@code jackson-datatype-jsr310-2.15.4.jar} — are named as depended-on but not separately
 * cataloged as bundled. That is a disagreement between two source artifacts, not an ingest defect,
 * and {@code OrphanCheckerTest} records it as the second known upstream finding.
 */
class JarDependencyTest {

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_jar_dependency_test");
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
    void measuredCountHoldsToday() {
        assertEquals(303, db.getCollection(Collections.JAR_DEPENDENCY).countDocuments(),
                "(jar, class) edge count drifted");
    }

    @Test
    void everyRowNamesBothSides() {
        long checked = 0;
        for (Document d : db.getCollection(Collections.JAR_DEPENDENCY).find()) {
            assertNotNull(d.getString("jarName"), "a row has no jar name: " + d.toJson());
            assertNotNull(d.getString("dependentClassName"),
                    "a row has no dependent class name: " + d.toJson());
            assertNotNull(d.getString("parentRef"), "a row has no parentRef: " + d.toJson());
            checked++;
        }
        assertTrue(checked > 0, "nothing was checked; the collection is empty");
    }

    @Test
    void mostReferencedJarHasSixtyThreeDependents() {
        long max = 0;
        for (Document d : db.getCollection(Collections.JAR_DEPENDENCY)
                .aggregate(java.util.List.of(
                        new Document("$group", new Document("_id", "$jarName")
                                .append("n", new Document("$sum", 1))),
                        new Document("$sort", new Document("n", -1)),
                        new Document("$limit", 1)))) {
            max = d.getInteger("n");
        }
        assertEquals(63, max, "the most-referenced jar's dependent count drifted");
    }
}
