package com.codeanalyzer.ingest.mongo;

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

/**
 * ING-115 — Java annotations, entity count verified against the source.
 *
 * <h2>The "3 link collections" the row asks for already exist</h2>
 *
 * <p>{@code annotations} is a plain array on classes, methods and fields, so {@code Splitter}
 * auto-splits it into {@code am_java_class__annotations}, {@code am_java_method__annotations} and
 * {@code am_java_field__annotations}. No {@code am_annotation} entity collection exists or needs
 * to: an annotation has no identity outside where it is declared — two classes annotated
 * {@code @Deprecated} are not "the same annotation" in any sense a query would need — so a shared
 * entity plus three junctions would be a normalisation this data has no use for.
 *
 * <h2>Why the collection totals (183/668/31) are not the row's numbers, and are not wrong either</h2>
 *
 * <p>The row states 146/348/31. Measured collection sizes are 183/668/31 — larger for class and
 * method, identical for field. The difference is not loss; it is the no-arrays invariant applied
 * one level deeper. An annotation can carry an array-valued attribute
 * ({@code @ComponentScan(scanBasePackages = {...})}), and each element of that array becomes its
 * own row in the <b>same</b> collection, sharing the annotation's key as {@code parentRef} — the
 * same rule that keeps a nested array inside a split element from escaping to a collection of its
 * own. Field annotations in this corpus happen to carry no array attributes, which is why that
 * count alone matches directly; it is coincidence, not a different rule.
 *
 * <p>So a row is an annotation entity if it carries {@code name}, and an attribute-array element if
 * it carries {@code value} instead. Filtering to the first gives exactly 146 / 348 / 31 — the row's
 * numbers were the right target, they just needed the right filter to reach them, not a different
 * count.
 */
class AnnotationLinkTest {

    private static final String[] COLLECTIONS = {
            "am_java_class__annotations", "am_java_method__annotations", "am_java_field__annotations"};
    private static final int[] EXPECTED_ENTITIES = {146, 348, 31};

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");
        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_annotation_link_test");
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
    void entityCountsMatchTheSourceExactly() {
        for (int i = 0; i < COLLECTIONS.length; i++) {
            long entities = db.getCollection(COLLECTIONS[i])
                    .countDocuments(Filters.exists("name"));
            assertEquals(EXPECTED_ENTITIES[i], entities,
                    COLLECTIONS[i] + ": annotation-entity count (rows with a name) drifted");
        }
    }

    @Test
    void attributeArrayElementsAreNotMistakenForEntities() {
        // The other half of the same row that carries value instead of name — the split children
        // of an annotation's array-valued attributes. Every row must be one or the other, never
        // both and never neither, or "annotation count" becomes ambiguous depending on which field
        // a caller happens to check.
        for (String coll : COLLECTIONS) {
            for (Document d : db.getCollection(coll).find()) {
                boolean hasName = d.containsKey("name");
                boolean hasValue = d.containsKey("value");
                assertFalse(hasName && hasValue,
                        coll + ": a row has both name and value, so entity/attribute rows are "
                                + "not distinguishable: " + d.toJson());
                assertFalse(!hasName && !hasValue,
                        coll + ": a row has neither name nor value: " + d.toJson());
            }
        }
    }

    @Test
    void everyAttributeArrayElementPointsAtARealAnnotation() {
        for (String coll : COLLECTIONS) {
            Set<String> entityKeys = new HashSet<>();
            for (Document d : db.getCollection(coll).find(Filters.exists("name"))) {
                entityKeys.add(d.getString("key"));
            }
            for (Document d : db.getCollection(coll).find(Filters.exists("value"))) {
                String parent = d.getString("parentRef");
                assertNotNull(parent, coll + ": an attribute row has no parentRef");
                org.junit.jupiter.api.Assertions.assertTrue(entityKeys.contains(parent),
                        coll + ": attribute row points at an annotation that does not exist: "
                                + parent);
            }
        }
    }
}
