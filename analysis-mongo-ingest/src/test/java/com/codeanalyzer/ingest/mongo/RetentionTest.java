package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.write.Retention;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB-104 / DB-105 — dropping a run, and dropping a key range.
 *
 * <h2>The property that makes this hard</h2>
 *
 * <p>Entities are shared: four archived runs of one dialect produce 29,172 memberships over 7,293
 * entities. A drop must remove exactly the dropped run's claim on them and nothing else, and the
 * entity document itself carries a {@code runId} naming whichever run last wrote it — so deleting
 * by that field would be both too broad and unpredictable.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RetentionTest {

    private static final Path ANALYSIS =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private MongoDatabase db;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(ANALYSIS), "ORACLE analysis not on disk");
        client = MongoClients.create(MongoAvailable.URI);
    }

    @AfterAll
    static void close() {
        // Dropped by name: this class uses a per-test database, so a static teardown cannot
        // reach the instance field. Dropping only on start left every class's data resident
        // for the life of the mongod — 23 classes accumulated 2.1 GB and the OS began
        // killing test runs.
        if (client != null) {
            client.getDatabase("am_retention_test").drop();
            client.getDatabase("am_retention_prefix_test").drop();
        }
        if (client != null) client.close();
    }

    private String[] twoRuns() {
        db = client.getDatabase("am_retention_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        String v1 = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS), true).runId();
        String v2 = svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(ANALYSIS), true).runId();
        assertNotEquals(v1, v2, "the two ingests produced one run");
        return new String[]{v1, v2};
    }

    @Test
    @Order(1)
    void droppingOneRunLeavesTheOtherFullyIntact() {
        String[] runs = twoRuns();
        long entitiesBefore = db.getCollection(Collections.PROC_NODE).countDocuments();
        long v2MembersBefore = db.getCollection(RunMember.COLLECTION)
                .countDocuments(Filters.eq("runRef", runs[1]));

        Retention.Removed removed = new Retention(db).dropRun(runs[0]);

        assertTrue(removed.memberships() > 0, "nothing was removed");
        // Both runs saw identical content, so v2 still claims every entity: none may be collected.
        assertEquals(0, removed.entities(),
                "entities still referenced by the surviving run were collected");
        assertEquals(entitiesBefore, db.getCollection(Collections.PROC_NODE).countDocuments(),
                "the surviving run lost entity documents");
        assertEquals(v2MembersBefore, db.getCollection(RunMember.COLLECTION)
                        .countDocuments(Filters.eq("runRef", runs[1])),
                "the surviving run lost membership rows");
        assertEquals(0, db.getCollection(RunMember.COLLECTION)
                        .countDocuments(Filters.eq("runRef", runs[0])),
                "the dropped run still has membership rows");
        assertNull(db.getCollection(Collections.RUN).find(Filters.eq("_id", runs[0])).first(),
                "the dropped run document survived");
    }

    @Test
    @Order(2)
    void droppingTheLastRunCollectsTheEntities() {
        // The other half. If nothing is ever collected, "retention" is just a slower leak.
        String[] runs = twoRuns();
        Retention r = new Retention(db);
        r.dropRun(runs[0]);
        Retention.Removed second = r.dropRun(runs[1]);

        assertTrue(second.entities() > 0, "dropping the last run collected no entities");
        assertEquals(0, db.getCollection(Collections.PROC_NODE).countDocuments(),
                "procedure entities survived with no run referring to them");
        assertEquals(0, db.getCollection(RunMember.COLLECTION).countDocuments(),
                "membership rows survived every run being dropped");
    }

    @Test
    @Order(3)
    void splitChildrenGoWithTheirParent() {
        // They carry no membership of their own, so nothing else would ever collect them: no
        // reference points at them and no integrity check names them. They would accumulate
        // silently for the life of the database.
        String[] runs = twoRuns();
        List<String> splitCollections = new ArrayList<>();
        for (String n : db.listCollectionNames()) {
            if (n.contains("__") && db.getCollection(n).countDocuments() > 0) splitCollections.add(n);
        }
        assertFalse(splitCollections.isEmpty(), "no split collections to check");

        Retention r = new Retention(db);
        r.dropRun(runs[0]);
        Retention.Removed second = r.dropRun(runs[1]);

        assertTrue(second.splitChildren() > 0, "no split children were collected");
        for (String n : splitCollections) {
            assertEquals(0, db.getCollection(n).countDocuments(),
                    n + " still holds rows whose parent entity is gone");
        }
    }

    @Test
    @Order(4)
    void pooledSqlTextIsRetiredWhenNothingQuotesIt() {
        // am_sql_text has no membership — it is deduplicated by content across every run (73%
        // duplication measured), so only the absence of a referrer can retire a row.
        String[] runs = twoRuns();
        assertTrue(db.getCollection(Collections.SQL_TEXT).countDocuments() > 0,
                "no pooled SQL text to check");

        Retention r = new Retention(db);
        r.dropRun(runs[0]);
        r.dropRun(runs[1]);

        assertEquals(0, db.getCollection(Collections.SQL_TEXT).countDocuments(),
                "pooled SQL text survived with no statement referencing it");
    }

    @Test
    @Order(5)
    void aPrefixDeleteStopsAtTheSeparator() {
        // DB-105. The escaping contract is what makes this exact: an unescaped '~' appears only
        // between components, so "prefix then separator" cannot match halfway through a component.
        // Without requiring the separator, dropping ORACLE~DEMO would also take ORACLE~DEMO_ARCHIVE.
        db = client.getDatabase("am_retention_prefix_test");
        db.drop();
        var coll = db.getCollection("am_run_member");
        coll.insertMany(List.of(
                new Document("_id", "a").append("key", "ORACLE~DEMO~one"),
                new Document("_id", "b").append("key", "ORACLE~DEMO~two"),
                new Document("_id", "c").append("key", "ORACLE~DEMO_ARCHIVE~three"),
                new Document("_id", "d").append("key", "ORACLE~DEMOX~four"),
                new Document("_id", "e").append("key", "ORACLE~OTHER~five")));

        long removed = new Retention(db).deleteByKeyPrefix("am_run_member", "ORACLE~DEMO");

        assertEquals(2, removed, "the prefix delete did not remove exactly the two matching keys");
        var survivors = coll.distinct("_id", String.class).into(new ArrayList<>());
        assertTrue(survivors.contains("c"), "ORACLE~DEMO_ARCHIVE was deleted by a DEMO prefix");
        assertTrue(survivors.contains("d"), "ORACLE~DEMOX was deleted by a DEMO prefix");
        assertTrue(survivors.contains("e"), "an unrelated key was deleted");
    }

    @Test
    @Order(6)
    void aPrefixDeleteTreatsRegexMetacharactersAsText() {
        // Key components legitimately contain '$' (Oracle package separators) and '.', both regex
        // metacharacters. Unquoted, '.' matches any character and the delete over-reaches.
        db = client.getDatabase("am_retention_prefix_test");
        db.drop();
        var coll = db.getCollection("am_run_member");
        coll.insertMany(List.of(
                new Document("_id", "a").append("key", "PKG$A.B~one"),
                new Document("_id", "b").append("key", "PKGxAyB~two")));

        long removed = new Retention(db).deleteByKeyPrefix("am_run_member", "PKG$A.B");

        assertEquals(1, removed, "the prefix was treated as a regex pattern rather than as text");
        assertTrue(coll.distinct("_id", String.class).into(new ArrayList<>()).contains("b"),
                "a key matching only because '.' is a wildcard was deleted");
    }
}
