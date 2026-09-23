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

/**
 * ING-117 (part 2 of 3) — {@code analysis.json}'s {@code advice} array, no longer silently dropped.
 *
 * <p>Before {@code DagAndEndpointMapper.readAdvice} existed, this field fell through both JAR
 * mappers' field switches to {@code default -> p.skipChildren()}: two rows lost on every ingest,
 * with nothing anywhere reporting a loss, because a skipped field is indistinguishable from an
 * absent one. See the mapper's javadoc for why "small and bounded" did not earn it an embedded
 * exception from the no-arrays invariant.
 */
class AnalysisAdviceTest {

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_analysis_advice_test");
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
        assertEquals(2, db.getCollection(Collections.ANALYSIS_ADVICE).countDocuments(),
                "advice count drifted from the measured figure");
    }

    @Test
    void everyRowCarriesItsKindAndClass() {
        for (Document d : db.getCollection(Collections.ANALYSIS_ADVICE).find()) {
            assertNotNull(d.getString("kind"), "an advice row has no kind: " + d.toJson());
            assertNotNull(d.getString("className"), "an advice row has no className: " + d.toJson());
        }
    }
}
