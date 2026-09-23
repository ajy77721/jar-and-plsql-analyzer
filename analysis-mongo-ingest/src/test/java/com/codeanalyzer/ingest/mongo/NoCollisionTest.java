package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.schema.Catalog;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-104 — the two Mongo models coexist, and cannot collide.
 *
 * <h2>The decision</h2>
 *
 * <p>{@code MongoPlsqlEntityIndex} is <b>not</b> retired (open question O-7, resolved: coexist).
 * It is a working index with live callers, and the normalised model does not yet serve every query
 * it does — the middle layer is unbuilt. Retiring it would mean either porting those queries first
 * or losing them, and neither is a prerequisite for the ingest work.
 *
 * <p>Coexistence is only safe if the two write to disjoint collections. They do, but by convention
 * rather than by construction: the new model prefixes everything {@code am_}, the old one does not.
 * A convention nobody checks is a convention that holds until someone adds a collection.
 *
 * <h2>Why the prefix rule is checked rather than the four names</h2>
 *
 * <p>Listing the old model's four collections and asserting the new model avoids them would pass
 * forever while saying nothing about the fifth. The invariant is that the new model owns exactly
 * the {@code am_} namespace and the old model owns none of it — which stays true as either grows.
 */
class NoCollisionTest {

    /** The collections {@code MongoPlsqlEntityIndex} owns. */
    private static final Set<String> LEGACY = Set.of(
            "entity_run", "flow_node", "flow_edge", "table_usage");

    private static List<String> declaredCollections() throws IllegalAccessException {
        List<String> names = new ArrayList<>();
        for (Field f : Collections.class.getDeclaredFields()) {
            if (!Modifier.isStatic(f.getModifiers()) || f.getType() != String.class) continue;
            names.add((String) f.get(null));
        }
        return names;
    }

    @Test
    void everyDeclaredCollectionIsInTheAmNamespace() throws Exception {
        List<String> stray = declaredCollections().stream()
                .filter(n -> !n.startsWith("am_")).toList();
        assertEquals(List.of(), stray,
                "these collections are outside the am_ namespace, so nothing keeps them clear of "
                        + "MongoPlsqlEntityIndex");
    }

    @Test
    void everyCatalogCollectionIsInTheAmNamespace() {
        List<String> stray = Catalog.all().stream()
                .map(spec -> spec.name())
                .filter(n -> !n.startsWith("am_"))
                .toList();
        assertEquals(List.of(), stray, "the schema installer creates collections outside am_");
    }

    @Test
    void noDeclaredCollectionCollidesWithTheLegacyIndex() throws Exception {
        List<String> collisions = declaredCollections().stream().filter(LEGACY::contains).toList();
        assertEquals(List.of(), collisions,
                "the normalised model would write into the legacy index's collections");
    }

    @Test
    void theLegacyIndexOwnsNothingInTheAmNamespace() {
        // The other direction, and the one a prefix rule on our side alone would miss.
        List<String> stray = LEGACY.stream().filter(n -> n.startsWith("am_")).toList();
        assertEquals(List.of(), stray, "the legacy index has taken a name in the am_ namespace");
    }

    @Test
    void splitCollectionsInheritThePrefix() {
        // Splitter derives a child collection's name from its parent, so a split of an am_ parent
        // is inside the namespace by construction. Asserted because "by construction" has been
        // wrong twice already in this module.
        String child = Splitter.childCollection(Collections.PROC_NODE, "subtreeTables");
        assertTrue(child.startsWith("am_"),
                "a split collection escaped the am_ namespace: " + child);
        assertTrue(child.startsWith(Collections.PROC_NODE + "__"),
                "a split collection is not named after its parent: " + child);
    }

    @Test
    void aRealIngestCreatesNothingOutsideTheNamespace() throws Exception {
        // The empirical check. Splitter names collections at write time from the data's shape, so
        // the only way to know what a run actually creates is to run one.
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path analysis = Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");
        Assumptions.assumeTrue(java.nio.file.Files.isDirectory(analysis), "no ORACLE analysis");

        try (MongoClient client = MongoClients.create(MongoAvailable.URI)) {
            MongoDatabase db = client.getDatabase("am_nocollision_test");
            db.drop();
            MongoIngestService svc = new MongoIngestService(db);
            svc.installSchema();
            svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                    new FileArtifactSource(analysis));

            Set<String> created = new TreeSet<>();
            db.listCollectionNames().into(created);
            List<String> stray = created.stream().filter(n -> !n.startsWith("am_")).toList();
            assertEquals(List.of(), stray,
                    "a real ingest created collections outside the am_ namespace: " + stray);
            db.drop();
        }
    }
}
