package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.jar.AnalysisJsonMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * VER-003 — {@link CountReconciler}, checked both against synthetic maps (the pure comparison
 * logic) and against a real analysis file (the tally logic, the same real-data discipline
 * {@code AnalysisJsonMapperCountTest} already established for one collection).
 */
class CountReconcilerTest {

    // ==================== reconcile() — pure logic ====================

    @Test
    void matchingCountsProduceNoDiscrepancies() {
        Map<String, Long> a = Map.of("am_java_method", 100L, "am_java_class", 20L);
        Map<String, Long> b = Map.of("am_java_method", 100L, "am_java_class", 20L);
        assertTrue(CountReconciler.reconcile(a, b).isEmpty());
    }

    @Test
    void aMismatchedCollectionIsReportedWithBothCounts() {
        Map<String, Long> expected = Map.of("am_java_method", 100L);
        Map<String, Long> actual = Map.of("am_java_method", 97L);
        List<CountReconciler.Discrepancy> d = CountReconciler.reconcile(expected, actual);
        assertEquals(1, d.size());
        assertEquals("am_java_method", d.get(0).collection());
        assertEquals(100L, d.get(0).expected());
        assertEquals(97L, d.get(0).actual());
        assertEquals(-3L, d.get(0).difference());
    }

    @Test
    void aCollectionPresentOnlyInExpectedIsADiscrepancyNotASkip() {
        // The mapper stopped emitting a collection entirely — a real defect, not a case to ignore
        // because "actual" simply has no entry for it.
        Map<String, Long> expected = Map.of("am_db_trigger", 12L);
        Map<String, Long> actual = Map.of();
        List<CountReconciler.Discrepancy> d = CountReconciler.reconcile(expected, actual);
        assertEquals(1, d.size());
        assertEquals(12L, d.get(0).expected());
        assertEquals(0L, d.get(0).actual());
    }

    @Test
    void aCollectionPresentOnlyInActualIsADiscrepancyNotASkip() {
        // The writer wrote something the mapper never emitted — equally a real defect.
        Map<String, Long> expected = Map.of();
        Map<String, Long> actual = Map.of("am_ghost_collection", 5L);
        List<CountReconciler.Discrepancy> d = CountReconciler.reconcile(expected, actual);
        assertEquals(1, d.size());
        assertEquals(0L, d.get(0).expected());
        assertEquals(5L, d.get(0).actual());
    }

    @Test
    void discrepanciesAreSortedByCollectionNameForADeterministicReport() {
        Map<String, Long> expected = Map.of("z_coll", 1L, "a_coll", 1L, "m_coll", 1L);
        Map<String, Long> actual = Map.of("z_coll", 2L, "a_coll", 2L, "m_coll", 2L);
        List<CountReconciler.Discrepancy> d = CountReconciler.reconcile(expected, actual);
        assertEquals(List.of("a_coll", "m_coll", "z_coll"),
                d.stream().map(CountReconciler.Discrepancy::collection).toList());
    }

    // ==================== tally() — real data, no invented baseline ====================

    @Test
    @DisplayName("tally() reproduces AnalysisJsonMapperCountTest's own per-collection count, generalised")
    void tallyMatchesAManualCountOfTheSameRealFile() throws Exception {
        Path dir = newestJarAnalysis();
        Assumptions.assumeTrue(dir != null, "no JAR analysis on disk");

        IngestContext ctx = new IngestContext("run~v1", "JAR");
        Map<String, Long> tallied;
        try (var in = Files.newInputStream(dir.resolve("analysis.json"))) {
            tallied = CountReconciler.tally(new AnalysisJsonMapper(), ctx, in);
        }

        // Independently recount am_java_method by running the same mapper a second time with a
        // hand-rolled sink, and compare against tally()'s own number for that one collection —
        // proving tally() isn't silently mis-attributing counts to the wrong collection key.
        long[] manualCount = {0};
        IngestContext ctx2 = new IngestContext("run~v1", "JAR");
        try (var in = Files.newInputStream(dir.resolve("analysis.json"))) {
            new AnalysisJsonMapper().map(ctx2, in, (coll, doc) -> {
                if ("am_java_method".equals(coll)) manualCount[0]++;
            });
        }

        assertTrue(tallied.containsKey("am_java_method"), "the mapper must emit am_java_method");
        assertEquals(manualCount[0], tallied.get("am_java_method"),
                "tally() must count the same collection the same way a direct sink does");
        // A real file maps into more than one collection — proving this generalises past the one
        // collection AnalysisJsonMapperCountTest already checked, not just replicating it.
        assertTrue(tallied.size() > 1, "a real analysis.json must map into more than one collection");
    }

    @Test
    void reconcilingATallyAgainstItselfIsClean() throws Exception {
        // The baseline reconciled against is "the same source file, tallied again" — reproducible
        // by construction, unlike a hand-written number that can only ever describe one past run.
        Path dir = newestJarAnalysis();
        Assumptions.assumeTrue(dir != null, "no JAR analysis on disk");

        Map<String, Long> first;
        Map<String, Long> second;
        try (var in = Files.newInputStream(dir.resolve("analysis.json"))) {
            first = CountReconciler.tally(new AnalysisJsonMapper(), new IngestContext("r1", "JAR"), in);
        }
        try (var in = Files.newInputStream(dir.resolve("analysis.json"))) {
            second = CountReconciler.tally(new AnalysisJsonMapper(), new IngestContext("r2", "JAR"), in);
        }

        assertTrue(CountReconciler.reconcile(first, second).isEmpty(),
                "the same mapper run twice over the same file must tally identically");
    }

    static Path newestJarAnalysis() throws Exception {
        Path root = Path.of("../data/jar");
        if (!Files.isDirectory(root)) return null;
        try (Stream<Path> s = Files.list(root)) {
            return s.filter(Files::isDirectory)
                    .filter(p -> Files.isRegularFile(p.resolve("analysis.json")))
                    .max(Comparator.comparingLong(
                            p -> p.resolve("analysis.json").toFile().lastModified()))
                    .orElse(null);
        }
    }
}
