package com.codeanalyzer.ingest.mongo;

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
 * ING-118 — {@code am_class_interface}, stored as {@code am_java_class__interfaces}.
 *
 * <p>Same §17 shape as the other planned-name rows in this section: {@code interfaces} is a plain
 * array of fully-qualified names, so {@code Splitter} auto-splits it rather than a mapper writing
 * a collection called {@code am_class_interface}. Unlike annotations (ING-115), an interface name
 * is a bare string with no nested structure, so there is no attribute-array complication here —
 * the collection total <b>is</b> the entity count, and it matches the source exactly at 37.
 */
class ClassInterfaceSplitTest {

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_class_interface_test");
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
    void countMatchesTheSourceExactly() {
        assertEquals(37, db.getCollection("am_java_class__interfaces").countDocuments(),
                "interface-split count drifted from the measured figure");
    }

    @Test
    void everyRowCarriesAnInterfaceNameAndAParent() {
        long checked = 0;
        for (Document d : db.getCollection("am_java_class__interfaces").find()) {
            assertNotNull(d.getString("value"), "a row has no interface name: " + d.toJson());
            assertNotNull(d.getString("parentRef"), "a row has no owning class: " + d.toJson());
            checked++;
        }
        assertTrue(checked > 0, "nothing was checked; the collection is empty");
    }
}
