package com.jaranalyzer.report;

import com.analyzer.storage.page.InvalidPageRequestException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-013 — the collection report is built by streaming, not by loading the analysis.
 *
 * <h2>The ratio that decides the design</h2>
 *
 * <p>The endpoint called {@code persistenceService.load(id)} — all of {@code analysis.json},
 * measured at <b>49.4 MB</b> — to produce an aggregate measured at <b>3.8 KB</b>: 14 collections
 * over 155 (endpoint, collection) pairs. About <b>13,000×</b> more input than output, held in heap,
 * per request.
 *
 * <p>Paging a 14-row answer was never the point. Not reading 49 MB to compute it is.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CollectionsPagingTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static Path analysis;

    @BeforeAll
    static void findAnalysis() throws Exception {
        Path root = Path.of("../data/jar");
        if (!Files.isDirectory(root)) return;
        try (Stream<Path> s = Files.list(root)) {
            analysis = s.filter(Files::isDirectory)
                    .map(p -> p.resolve("analysis.json"))
                    .filter(Files::isRegularFile)
                    .max(Comparator.comparingLong(p -> p.toFile().lastModified()))
                    .orElse(null);
        }
    }

    private List<CollectionReportAggregator.CollectionRow> aggregate() throws Exception {
        try (InputStream in = Files.newInputStream(analysis);
             var parser = M.getFactory().createParser(in)) {
            return CollectionReportAggregator.aggregate(parser, M);
        }
    }

    @Test
    @Order(1)
    void theAggregateMatchesTheAnalysisItWasBuiltFrom() throws Exception {
        Assumptions.assumeTrue(analysis != null, "no JAR analysis on disk");
        // Expectations recomputed from the file rather than hard-coded, so the test cannot drift
        // away from the data it is checking.
        JsonNode root = M.readTree(Files.newInputStream(analysis));
        var expected = new java.util.TreeMap<String, Integer>();
        for (JsonNode ep : root.path("endpoints")) {
            for (JsonNode c : ep.path("aggregatedCollections")) {
                expected.merge(c.asText(), 1, Integer::sum);
            }
        }

        var rows = aggregate();
        assertEquals(expected.size(), rows.size(),
                "the streaming aggregate has a different number of collections than the file");
        for (var r : rows) {
            assertEquals(expected.get(r.collection()).intValue(), r.endpointCount(),
                    "endpoint count differs for " + r.collection());
        }
    }

    @Test
    @Order(2)
    void rowsAreSortedByEndpointCountDescending() throws Exception {
        Assumptions.assumeTrue(analysis != null, "no JAR analysis on disk");
        var rows = aggregate();
        assertFalse(rows.isEmpty());
        for (int i = 1; i < rows.size(); i++) {
            assertTrue(rows.get(i - 1).endpointCount() >= rows.get(i).endpointCount(),
                    "the report is no longer ordered by endpoint count — the order the original "
                            + "returned, and the one any existing caller relies on");
        }
    }

    @Test
    @Order(3)
    void aDottedCollectionNameSurvives() throws Exception {
        Assumptions.assumeTrue(analysis != null, "no JAR analysis on disk");
        // 69 of 155 collection names contain a dot. As map keys they were unaddressable (ING-113);
        // as row values they must be intact.
        var rows = aggregate();
        assertTrue(rows.stream().anyMatch(r -> r.collection().contains(".")),
                "no dotted collection name is present — either the data changed or names are "
                        + "being mangled");
    }

    @Test
    @Order(4)
    void anUnpagedRequestStillGetsABareArray() throws Exception {
        Assumptions.assumeTrue(analysis != null, "no JAR analysis on disk");
        // The endpoint is documented and has no in-application caller, so an external script
        // reading it must keep working.
        JsonNode out = CollectionReportPage.respond(M, aggregate(), null, null, null);
        assertTrue(out.isArray(), "an unpaged request no longer receives a bare array");
        assertTrue(out.size() > 0);
        assertTrue(out.get(0).has("endpointCount"), "the row shape changed");
    }

    @Test
    @Order(5)
    void pagingWalksEveryRowExactlyOnce() throws Exception {
        Assumptions.assumeTrue(analysis != null, "no JAR analysis on disk");
        var rows = aggregate();
        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 50; guard++) {
            JsonNode env = CollectionReportPage.respond(M, rows, 3, cursor, null);
            for (JsonNode item : env.path("items")) seen.add(item.path("collection").asText());
            JsonNode next = env.path("page").path("nextCursor");
            cursor = next.isNull() ? null : next.asText(null);
            if (cursor == null || cursor.isEmpty()) break;
        }
        assertEquals(rows.size(), seen.size(),
                "paging returned " + seen.size() + " of " + rows.size() + " rows");
        assertEquals(rows.size(), seen.stream().distinct().count(),
                "a collection was returned on two pages");
    }

    @Test
    @Order(6)
    void anOverCapLimitIsRefused() throws Exception {
        Assumptions.assumeTrue(analysis != null, "no JAR analysis on disk");
        var rows = aggregate();
        assertThrows(InvalidPageRequestException.class,
                () -> CollectionReportPage.respond(M, rows, 999_999, null, null),
                "an over-cap limit was accepted");
    }

    @Test
    @Order(7)
    void theLegacyPathIsCappedEvenWithoutALimit() {
        // "The aggregate is small" is a property of today's data, not a guarantee of the endpoint.
        List<CollectionReportAggregator.CollectionRow> many = new ArrayList<>();
        for (int i = 0; i < CollectionReportPage.MAX_ROWS + 500; i++) {
            many.add(new CollectionReportAggregator.CollectionRow(
                    "c" + i, "d", List.of("READ"), List.of(), 1));
        }
        JsonNode out = CollectionReportPage.respond(M, many, null, null, null);
        assertEquals(CollectionReportPage.MAX_ROWS, out.size(),
                "the unpaged path returns an unbounded array");
    }

    @Test
    @Order(8)
    void onlyTheEndpointsArrayIsRead() throws Exception {
        Assumptions.assumeTrue(analysis != null, "no JAR analysis on disk");
        // The whole point. Measured by heap rather than asserted structurally: a parser that walked
        // `classes` and `callTreeNodes` into memory would show it here, and nothing else would.
        Runtime rt = Runtime.getRuntime();
        System.gc();
        long before = rt.totalMemory() - rt.freeMemory();
        var rows = aggregate();
        long usedMb = (rt.totalMemory() - rt.freeMemory() - before) / 1_000_000;

        assertFalse(rows.isEmpty());
        assertTrue(usedMb < 40,
                "aggregating retained ~" + usedMb + " MB; the file is 49 MB, so this is reading "
                        + "more than the endpoints array");
    }
}
