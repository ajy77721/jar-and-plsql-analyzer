package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.model.RunMember;
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
 * DB-103 — dedup measured on the archived runs, which is the number the identity model exists for.
 *
 * <h2>What is being measured</h2>
 *
 * <p>Every entity is written under a content-addressed {@code _id}, and its presence in a run is a
 * separate {@code am_run_member} row. So ingesting the same procedure four times should produce
 * <b>one</b> entity document and <b>four</b> membership rows. Dedup is therefore
 * {@code 1 - distinct entities / membership rows}, and for four runs of identical content the
 * ceiling is exactly 75%.
 *
 * <h2>Why the runs look different on disk and are not</h2>
 *
 * <p>A whole-directory checksum says all four runs of every dialect differ. Compared file by file,
 * <b>188 of 189 files are byte-identical</b> and the only difference is {@code api/index.json} —
 * run metadata: timestamps and the run id. The analysis output itself is unchanged, which is what
 * content addressing is meant to notice and a directory checksum cannot.
 *
 * <h2>force = true, deliberately</h2>
 *
 * <p>The ING-004 digest covers the artifacts that are actually mapped, and those are identical
 * across these runs — so an unforced second ingest would short-circuit and there would be nothing
 * to measure. Skipping is the right behaviour for an operator; here it would hide the property
 * under test.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ArchivedRunDedupTest {

    private static final Path BASE = Path.of("../data/plsql-parse");

    /** The DoD's threshold: the measured maximum for four runs of identical content. */
    private static final double REQUIRED = 0.75;

    private static MongoClient client;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        Assumptions.assumeTrue(Files.isDirectory(BASE), "archived runs not on disk");
        client = MongoClients.create(MongoAvailable.URI);
    }

    @AfterAll
    static void close() {
        // Dropped by name: this class uses a per-test database, so a static teardown cannot
        // reach the instance field. Dropping only on start left every class's data resident
        // for the life of the mongod — 23 classes accumulated 2.1 GB and the OS began
        // killing test runs.
        if (client != null) {
            client.getDatabase("am_dedup_db2").drop();
            client.getDatabase("am_dedup_mysql").drop();
            client.getDatabase("am_dedup_postgres").drop();
        }
        if (client != null) client.close();
    }

    private record Measurement(int runs, long members, long entities, double dedup) {}

    /** Ingests every archived run of one dialect into a fresh database and measures dedup. */
    private Measurement measure(String dialect, String project, String entryPoint) {
        Path dir = BASE.resolve(dialect).resolve(project).resolve("LOCAL");
        // BASE itself existing (checked in connect()) does not mean this dialect's own
        // subdirectory does: a "clean fresh start" leaves BASE present but empty, and
        // Files.list on a missing path throws NoSuchFileException rather than skipping —
        // exactly the state that turned an intended assumption-skip into a real failure.
        Assumptions.assumeTrue(Files.isDirectory(dir), dialect + " has no archived runs on disk");
        List<Path> runs = new ArrayList<>();
        try (var s = Files.list(dir)) {
            s.filter(Files::isDirectory).sorted().forEach(runs::add);
        } catch (Exception e) {
            throw new IllegalStateException("listing " + dir, e);
        }
        Assumptions.assumeTrue(runs.size() >= 2, dialect + " has fewer than two archived runs");

        MongoDatabase db = client.getDatabase("am_dedup_" + dialect.toLowerCase());
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        for (Path run : runs) {
            svc.ingest(dialect, project, "LOCAL", entryPoint, new FileArtifactSource(run), true);
        }

        long members = db.getCollection(RunMember.COLLECTION).countDocuments();
        long entities = db.getCollection(RunMember.COLLECTION)
                .distinct("entityRef", String.class).into(new ArrayList<>()).size();
        double dedup = members == 0 ? 0 : 1.0 - ((double) entities / members);
        return new Measurement(runs.size(), members, entities, dedup);
    }

    private void assertDedup(String dialect, String project, String entryPoint) {
        Measurement m = measure(dialect, project, entryPoint);
        System.out.printf("  %-10s runs=%d members=%,d entities=%,d dedup=%.1f%%%n",
                dialect, m.runs(), m.members(), m.entities(), m.dedup() * 100);
        assertTrue(m.dedup() >= REQUIRED, String.format(
                "%s dedup is %.1f%% over %d runs (%,d membership rows, %,d distinct entities); "
                        + "the identity model is not collapsing repeated content",
                dialect, m.dedup() * 100, m.runs(), m.members(), m.entities()));
    }

    @Test
    @Order(1)
    void db2() {
        assertDedup("DB2", "DB2DEMO", "ERP_USP_RUN_CYCLE");
    }

    @Test
    @Order(2)
    void mysql() {
        assertDedup("MYSQL", "MYSQLDEMO", "ERP_USP_RUN_CYCLE");
    }

    @Test
    @Order(3)
    void postgres() {
        assertDedup("POSTGRES", "POSTGRESDEMO", "ERP_USP_RUN_CYCLE");
    }

    @Test
    @Order(4)
    void everyRunIsStillIndividuallyAccountedFor() {
        // Dedup must not be achieved by losing runs. Four ingests must leave four run documents
        // and every entity must still be attributable to each run that saw it — otherwise the
        // "saving" is just missing data.
        //
        // Depends on db2() (Order 1) having actually ingested — @TestMethodOrder controls
        // sequence, not skip propagation, so an aborted db2() (no archived DB2 runs on disk)
        // would otherwise leave this querying an empty database and fail for real rather than
        // skip, the same false-failure shape measure()'s own guard fixes above.
        Assumptions.assumeTrue(Files.isDirectory(BASE.resolve("DB2").resolve("DB2DEMO").resolve("LOCAL")),
                "DB2 has no archived runs on disk");
        MongoDatabase db = client.getDatabase("am_dedup_db2");
        long runs = db.getCollection("am_run").countDocuments();
        assertEquals(4, runs, "not every archived run produced a run document");

        for (Document run : db.getCollection("am_run").find()) {
            String runId = run.getString("_id");
            long mine = db.getCollection(RunMember.COLLECTION)
                    .countDocuments(Filters.eq("runRef", runId));
            assertTrue(mine > 0, "run " + runId + " has no membership rows, so it is invisible");
        }
    }

    @Test
    @Order(5)
    void anEntitySeenInEveryRunIsStoredOnce() {
        // The property in its sharpest form, checked on one entity rather than in aggregate.
        // Same db2()-dependency guard as everyRunIsStillIndividuallyAccountedFor above.
        Assumptions.assumeTrue(Files.isDirectory(BASE.resolve("DB2").resolve("DB2DEMO").resolve("LOCAL")),
                "DB2 has no archived runs on disk");
        MongoDatabase db = client.getDatabase("am_dedup_db2");
        Document busiest = db.getCollection(RunMember.COLLECTION).aggregate(List.of(
                new Document("$group", new Document("_id", "$entityRef")
                        .append("runs", new Document("$addToSet", "$runRef"))),
                new Document("$project", new Document("n", new Document("$size", "$runs"))),
                new Document("$sort", new Document("n", -1)),
                new Document("$limit", 1))).first();
        assertNotNull(busiest, "no membership rows at all");
        assertEquals(4, busiest.getInteger("n"),
                "no entity is shared by all four runs, so identical content is producing "
                        + "distinct entities");
    }
}
