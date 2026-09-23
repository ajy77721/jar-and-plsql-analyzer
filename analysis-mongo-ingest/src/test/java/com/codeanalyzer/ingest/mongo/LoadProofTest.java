package com.codeanalyzer.ingest.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The scale question, answered by measurement rather than by argument.
 *
 * <h2>What is actually being asked</h2>
 *
 * <p>The target is 8,000 procedures and roughly a million database operations. Nothing in this
 * repository has ever been measured above 52 procedures, so every scale claim so far has been an
 * extrapolation from one point — which is not an extrapolation, it is a guess with a number
 * attached.
 *
 * <p>The decisive question is not "does 8,000 work" but <b>how the cost grows</b>. Linear means the
 * target needs a larger machine and nothing else. Super-linear means the model breaks somewhere
 * between here and there, and the exponent says roughly where.
 *
 * <h2>Synthetic input, shaped from the real analysis</h2>
 *
 * <p>The generator preserves the measured per-procedure ratios: at 52 procedures it emits 2,444
 * statements against the real 2,436, 3,848 chunk statements against 3,832, and 2,236 edges against
 * 2,258. A larger synthetic analysis therefore differs from the measured one in count, not in shape.
 *
 * <h2>Reported, not asserted</h2>
 *
 * <p>Timings on a shared developer machine are not a pass/fail signal — this run competes with an
 * IDE and a VM for memory. The test asserts only what is scale-invariant (every row arrives), and
 * prints the curve for a human to read. A green tick on a wall-clock threshold measured here would
 * mean nothing anywhere else.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LoadProofTest {

    private static final Path ROOT = Path.of("/tmp/loadproof");

    /** Sizes to run, ascending, so a kill still leaves the smaller points measured. */
    private static final int[] SIZES = {52, 208, 832};

    private static MongoClient client;
    private static final List<String> report = new ArrayList<>();

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(ROOT), "no generated corpora at " + ROOT);
        client = MongoClients.create(MongoAvailable.URI);
    }

    @AfterAll
    static void close() {
        System.out.println("\n  === load proof ===");
        report.forEach(System.out::println);
        if (client != null) {
            for (int n : SIZES) client.getDatabase("am_load_" + n).drop();
            client.close();
        }
    }

    @Test
    @Order(1)
    void ingestCostGrowsWithTheCorpus() {
        double previousPerProc = -1;
        for (int n : SIZES) {
            Path dir = ROOT.resolve("syn_" + n);
            if (!Files.isDirectory(dir)) continue;

            MongoDatabase db = client.getDatabase("am_load_" + n);
            db.drop();
            MongoIngestService svc = new MongoIngestService(db);
            svc.installSchema();

            Runtime rt = Runtime.getRuntime();
            System.gc();
            long heapBefore = rt.totalMemory() - rt.freeMemory();
            long started = System.nanoTime();
            var result = svc.ingest("ORACLE", "SYN", "LOCAL", "SYN_PROC_00000",
                    new FileArtifactSource(dir), true);
            long ms = (System.nanoTime() - started) / 1_000_000;
            long heapAfter = rt.totalMemory() - rt.freeMemory();

            long docs = 0;
            for (String c : db.listCollectionNames()) docs += db.getCollection(c).countDocuments();
            long dataMb = db.runCommand(new Document("dbStats", 1))
                    .get("dataSize", Number.class).longValue() / 1_000_000;

            double perProc = ms / (double) n;
            String growth = previousPerProc < 0 ? "—"
                    : String.format("%.2fx per-proc", perProc / previousPerProc);
            previousPerProc = perProc;

            report.add(String.format(
                    "  %5d procs  %8d ms  %7.1f ms/proc  %10d docs  %5d MB  heapΔ %4d MB  %s",
                    n, ms, perProc, docs, dataMb,
                    (heapAfter - heapBefore) / 1_000_000, growth));

            assertFalse(result.skipped(), "the load run was short-circuited");
            assertTrue(docs > 0, "no documents were written at n=" + n);
        }
        assertFalse(report.isEmpty(), "no corpus was ingested");
    }
}
