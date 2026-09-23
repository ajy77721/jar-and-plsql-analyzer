package com.analyzer.storage.mongo;

import com.analyzer.storage.LocalArtifactStore;
import com.analyzer.storage.entity.FlowEdgeRow;
import com.analyzer.storage.entity.FlowNodeRow;
import com.analyzer.storage.entity.PlsqlEntityMapper;
import com.analyzer.storage.entity.TableUsageRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * PLAN-09 Increment E's payoff, measured rather than asserted.
 *
 * <p>Three things are checked, in order of how easily each could be faked:
 *
 * <ol>
 *   <li><b>The index answers the same rows a scan does.</b> Correctness first; a fast wrong answer
 *       is not a win. The comparison is against a scan through the <em>same</em> mapper, so the two
 *       cannot disagree by having two parsers.</li>
 *   <li><b>The plan is a seek, not a collection scan.</b> Read out of {@code explain}, because "we
 *       created an index" and "the query uses it" are different claims and only the second one
 *       matters. An index dropped or a filter field renamed regresses this silently otherwise.</li>
 *   <li><b>It is faster, with numbers.</b> Printed, not asserted at a threshold — a timing assertion
 *       on shared CI hardware is a flake generator. The number goes in the report.</li>
 * </ol>
 *
 * <p>Against a real MongoDB, skipping without one — {@link MongoTestSupport} explains why there is
 * no fake.
 */
class MongoPlsqlEntityIndexTest {

    private static final String DATABASE = "analyzer_e_test";
    private static final ObjectMapper M = new ObjectMapper();

    private MongoClient client;
    private MongoDatabase db;
    private MongoPlsqlEntityIndex index;
    private LocalArtifactStore store;
    private String analysis;

    @BeforeEach
    void setUp() {
        Path corpus = corpusRoot();
        assumeTrue(corpus != null, "no data/plsql-parse on this machine — skipping");
        analysis = firstAnalysisWithNodes(corpus);
        assumeTrue(analysis != null, "data/plsql-parse has no analysis with node documents");

        client = MongoTestSupport.clientOrSkip();
        db = client.getDatabase(DATABASE);
        // dataRoot() is data/, so ArtifactKey.of("plsql-parse") resolves exactly as it does in the
        // running application. Rooting at plsql-parse/ instead would test a path this code never
        // takes.
        store = new LocalArtifactStore(corpus.getParent());
        index = new MongoPlsqlEntityIndex(db, store, M, "plsql-parse");
        index.ensureIndexes();
        index.invalidate(analysis);
    }

    @AfterEach
    void tearDown() {
        if (index != null && analysis != null) {
            try {
                index.invalidate(analysis);
            } catch (RuntimeException ignored) {
                // best effort; the database is dropped by the operator at the end of the increment
            }
        }
        if (client != null) client.close();
    }

    /* ─────────────────────────── 1. correctness ─────────────────────────── */

    @Test
    @DisplayName("the index returns exactly the rows a scan of the same artifacts returns")
    void indexAgreesWithTheScan() throws Exception {
        String table = busiestTable();
        assertNotNull(table, "the corpus must have at least one table usage");

        List<TableUsageRow> scanned = scanNodesUsingTable(table, null);
        List<TableUsageRow> indexed = index.nodesUsingTable(analysis, table, null);

        assertEquals(scanned.size(), indexed.size(),
                "row counts differ — the index is not a faithful projection of the artifacts");
        for (int i = 0; i < scanned.size(); i++) {
            assertEquals(scanned.get(i).nodeId(), indexed.get(i).nodeId());
            assertEquals(scanned.get(i).operation(), indexed.get(i).operation());
            assertEquals(scanned.get(i).usage(), indexed.get(i).usage());
            assertEquals(scanned.get(i).tableKey(), indexed.get(i).tableKey());
            assertEquals(scanned.get(i).lines(), indexed.get(i).lines());
        }
        assertFalse(indexed.isEmpty(), "the busiest table must have users");
    }

    @Test
    @DisplayName("a table can be asked for in any case, with or without its schema")
    void lookupIsCaseAndSchemaInsensitive() throws Exception {
        TableUsageRow any = index.nodesUsingTable(analysis, busiestTable(), null).get(0);
        String qualified = any.tableKey();
        String bare = qualified.substring(qualified.indexOf('.') + 1);

        int n = index.nodesUsingTable(analysis, qualified, null).size();
        assertEquals(n, index.nodesUsingTable(analysis, qualified.toLowerCase(), null).size());
        assertTrue(index.nodesUsingTable(analysis, bare, null).size() >= n,
                "the bare name must find at least the schema-qualified rows");
        assertTrue(index.nodesUsingTable(analysis, bare.toLowerCase(), null).size() >= n);
    }

    @Test
    @DisplayName("DIRECT and SUBTREE are separable — they are different questions")
    void usageDiscriminates() throws Exception {
        String table = busiestTable();
        int all = index.nodesUsingTable(analysis, table, null).size();
        int direct = index.nodesUsingTable(analysis, table, TableUsageRow.DIRECT).size();
        int subtree = index.nodesUsingTable(analysis, table, TableUsageRow.SUBTREE).size();
        assertEquals(all, direct + subtree);
        assertTrue(direct > 0, "some node must touch the busiest table in its own statements");
    }

    @Test
    @DisplayName("flowNode and edgesFrom answer from their own indexes")
    void nodeAndEdgeQueries() throws Exception {
        TableUsageRow any = index.nodesUsingTable(analysis, busiestTable(), null).get(0);
        Optional<FlowNodeRow> node = index.flowNode(analysis, any.nodeId());
        assertTrue(node.isPresent(), "the node that owns a usage row must exist as a flow_node");
        assertEquals(any.nodeId(), node.get().nodeId());

        List<FlowEdgeRow> edges = index.edgesFrom(analysis, any.nodeId());
        for (FlowEdgeRow e : edges) assertEquals(any.nodeId(), e.fromNodeId());

        assertTrue(index.flowNode(analysis, "NO$SUCH_NODE").isEmpty(),
                "an absent node is empty, not an invented row");
    }

    @Test
    @DisplayName("a second query does not re-ingest — the run marker is what makes it an index")
    void ingestHappensOnce() throws Exception {
        index.nodesUsingTable(analysis, busiestTable(), null);
        long after = index.entityRowCount(analysis);
        assertTrue(after > 0);
        index.nodesUsingTable(analysis, busiestTable(), null);
        index.flowNode(analysis, "anything");
        assertEquals(after, index.entityRowCount(analysis),
                "rows grew on a read — the analysis is being re-ingested every query");
    }

    /* ─────────────────────────── 2. the plan ─────────────────────────── */

    @Test
    @DisplayName("the hot path is an index seek, not a collection scan")
    void hotPathUsesTheIndex() throws Exception {
        TableUsageRow any = index.nodesUsingTable(analysis, busiestTable(), null).get(0);
        Document explain = index.explainNodesUsingTable(analysis, any.tableKey(),
                TableUsageRow.DIRECT);
        String plan = explain.toJson();

        assertTrue(plan.contains("IXSCAN"), "no index scan in the winning plan:\n" + plan);
        assertFalse(plan.contains("COLLSCAN"),
                "the hot path is doing a collection scan, which is the whole thing this "
                + "increment exists to remove:\n" + plan);
        assertTrue(plan.contains("tableusage_key"),
                "a different index won — tableusage_key is the one authored for this query:\n" + plan);
        // A blocking SORT stage would mean the index located the rows but the engine still had to
        // buffer and sort them, which is the cost the (runId, tableKey, usage, nodeId) key order
        // exists to avoid.
        assertFalse(plan.contains("\"stage\": \"SORT\"") || plan.contains("\"stage\":\"SORT\""),
                "the sort is not index-ordered:\n" + plan);
        System.out.println("[plan] " + winningStages(explain));
    }

    /* ─────────────────────────── 3. the measurement ─────────────────────────── */

    @Test
    @DisplayName("MEASURED: index seek versus a scan of every node document")
    void measured() throws Exception {
        String table = busiestTable();
        // Warm both sides. The FIRST entity query pays the ingest and is slower than the scan —
        // that is the honest cost of lazy first-touch ingest and it is stated in the report, not
        // hidden by measuring only the warm case. It is timed separately below.
        index.invalidate(analysis);
        long ingestNanos = time(() -> index.nodesUsingTable(analysis, table, null));
        scanNodesUsingTable(table, null);

        int reps = 20;
        long scanNanos = 0, seekNanos = 0;
        int scanRows = 0, seekRows = 0;
        for (int i = 0; i < reps; i++) {
            long t0 = System.nanoTime();
            scanRows = scanNodesUsingTable(table, null).size();
            scanNanos += System.nanoTime() - t0;

            long t1 = System.nanoTime();
            seekRows = index.nodesUsingTable(analysis, table, null).size();
            seekNanos += System.nanoTime() - t1;
        }
        assertEquals(scanRows, seekRows, "the two implementations disagree; the timing is moot");

        double scanMs = scanNanos / 1e6 / reps;
        double seekMs = seekNanos / 1e6 / reps;
        System.out.printf("[measure] analysis=%s table=%s rows=%d%n", analysis, table, seekRows);
        System.out.printf("[measure] first entity query (scan + ingest of the whole analysis): "
                          + "%.1f ms  <-- the honest cost, paid once%n", ingestNanos / 1e6);
        System.out.printf("[measure] artifact scan : %.2f ms/query (mean of %d)%n", scanMs, reps);
        System.out.printf("[measure] index seek    : %.2f ms/query (mean of %d)%n", seekMs, reps);
        System.out.printf("[measure] speedup       : %.1fx%n", scanMs / seekMs);
        System.out.printf("[measure] entity rows stored for this analysis: %d%n",
                index.entityRowCount(analysis));

        // Asserted only as an order-of-magnitude sanity floor, not as a benchmark: the scan opens
        // and parses every node document of the analysis and the seek reads one index range, so if
        // the seek is not comfortably faster something is structurally wrong (the index is not
        // being used, or the ingest is running again). A tight threshold here would be a flake.
        assertTrue(seekMs < scanMs,
                "the index is not faster than the scan: seek=" + seekMs + "ms scan=" + scanMs + "ms");
    }

    private static long time(ThrowingRunnable r) throws Exception {
        long t0 = System.nanoTime();
        r.run();
        return System.nanoTime() - t0;
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /* ─────────────────────────── the scan baseline ─────────────────────────── */

    /**
     * The same question, answered by reading every node document.
     *
     * <p>A local copy of what {@code ArtifactScanRepository#nodesUsingTable} does, because that class
     * lives in the web module and this one is in {@code analyzer-storage}. It goes through the same
     * {@link PlsqlEntityMapper} and the same {@link com.analyzer.storage.ArtifactStore}, so the
     * comparison is index-versus-scan and not this-parser-versus-that-one.
     */
    private List<TableUsageRow> scanNodesUsingTable(String table, String usage) throws IOException {
        String key = TableUsageRow.keyOf(null, table);
        boolean qualified = key.indexOf('.') >= 0;
        String suffix = "." + key;
        List<TableUsageRow> out = new ArrayList<>();
        PlsqlEntityMapper entityMapper = new PlsqlEntityMapper(M);
        for (Path f : nodeFiles()) {
            try (InputStream in = Files.newInputStream(f)) {
                entityMapper.map(analysis, in, PlsqlEntityMapper.sinkOf(null, null, row -> {
                    if (usage != null && !usage.equals(row.usage())) return;
                    String k = row.tableKey();
                    if (qualified ? k.equals(key) : (k.equals(key) || k.endsWith(suffix))) out.add(row);
                }));
            }
        }
        out.sort(Comparator.comparing(TableUsageRow::nodeId));
        return out;
    }

    private List<Path> nodeFiles() throws IOException {
        try (Stream<Path> s = Files.list(corpusRoot().resolve(analysis)
                .resolve("api").resolve("nodes"))) {
            return s.filter(p -> p.toString().endsWith(".json")).sorted().toList();
        }
    }

    /** The table with the most usage rows — the one a "which nodes touch X" screen would be opened
     *  on, and the one where a scan hurts most. */
    private String busiestTable() throws IOException {
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        PlsqlEntityMapper entityMapper = new PlsqlEntityMapper(M);
        for (Path f : nodeFiles()) {
            try (InputStream in = Files.newInputStream(f)) {
                entityMapper.map(analysis, in, PlsqlEntityMapper.sinkOf(null, null,
                        row -> counts.merge(row.tableKey(), 1, Integer::sum)));
            }
        }
        return counts.entrySet().stream().max(java.util.Map.Entry.comparingByValue())
                .map(java.util.Map.Entry::getKey).orElse(null);
    }

    static Path corpusRoot() {
        Path p = Path.of("").toAbsolutePath();
        for (int i = 0; i < 4 && p != null; i++, p = p.getParent()) {
            Path candidate = p.resolve("data").resolve("plsql-parse");
            if (Files.isDirectory(candidate)) return candidate;
        }
        return null;
    }

    private static String firstAnalysisWithNodes(Path corpus) {
        try (Stream<Path> s = Files.list(corpus)) {
            return s.filter(Files::isDirectory)
                    .filter(d -> Files.isDirectory(d.resolve("api").resolve("nodes")))
                    .map(d -> d.getFileName().toString())
                    .sorted()
                    .findFirst().orElse(null);
        } catch (IOException e) {
            return null;
        }
    }

    private static String winningStages(Document explain) {
        List<String> stages = new ArrayList<>();
        collectStages(explain, stages);
        return String.join(" <- ", stages);
    }

    @SuppressWarnings("unchecked")
    private static void collectStages(Object o, List<String> out) {
        if (o instanceof Document d) {
            Object stage = d.get("stage");
            if (stage instanceof String s) {
                out.add(d.containsKey("indexName") ? s + "(" + d.getString("indexName") + ")" : s);
            }
            d.values().forEach(v -> collectStages(v, out));
        } else if (o instanceof List<?> l) {
            l.forEach(v -> collectStages(v, out));
        }
    }
}
