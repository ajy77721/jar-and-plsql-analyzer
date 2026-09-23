package com.plsqlanalyzer.web.parser.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MID-007 (first half) — the statement search does not materialise the index.
 *
 * <h2>The measurement that justifies this</h2>
 *
 * <p>The controller called {@code readTree} on the whole file and cached the result. Measured on the
 * largest analysis (7.0 MB, 5,015 statements): the parsed tree retains <b>24.3 MB</b>, streaming
 * retains ~0. At the stated target of 8,000 procedures the file is roughly 1.1 GB, so no cache size
 * fixes it — the problem is holding one document.
 *
 * <h2>What is asserted, and why identity comes first</h2>
 *
 * <p>The streaming path and the array path are the same loop given rows from different places, so
 * the first thing to hold is that they agree. Two search implementations that disagree would
 * present as users finding different results for the same query depending on how the page was
 * served, which is far worse than a slow search.
 */
class StatementStreamSearchTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static Path index;

    @BeforeAll
    static void findIndex() throws Exception {
        // The largest statement index on disk, because a small one would not distinguish streaming
        // from materialising on any measurement below.
        Path root = Path.of("../data/plsql-parse");
        if (!Files.isDirectory(root)) return;
        try (Stream<Path> s = Files.walk(root)) {
            index = s.filter(p -> p.endsWith(Path.of("api/statements/index.json")))
                    .max(Comparator.comparingLong(p -> p.toFile().length()))
                    .orElse(null);
        }
    }

    /** Used heap after a collection, so the figure is what is still reachable. */
    private static long retainedBytes() {
        Runtime rt = Runtime.getRuntime();
        for (int i = 0; i < 3; i++) {
            System.gc();
            try {
                Thread.sleep(40);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return rt.totalMemory() - rt.freeMemory();
    }

    private static StatementIndexStream open() throws Exception {
        return new StatementIndexStream(M, Files.newInputStream(index));
    }

    @Test
    void streamingFindsExactlyWhatTheArrayFormFinds() throws Exception {
        Assumptions.assumeTrue(index != null, "no statement index on disk");
        JsonNode doc = M.readTree(Files.newInputStream(index));
        JsonNode statements = doc.get("statements");
        assertNotNull(statements, "the fixture has no statements array");

        // Several shapes of query, not one: a bare word, a qualifier, a negation and a prefix are
        // four different code paths through the matcher.
        for (String q : List.of("ERP", "op:UPDATE", "op:SELECT !table:ERP_PRODUCT", "table:ERP_*",
                "col:QTY", "nothing_matches_this_xyz")) {
            SearchQuery parsed = SearchQuery.parse(q);
            StatementSearcher.Result fromArray = StatementSearcher.search(statements, parsed, 25);
            StatementSearcher.Result fromStream;
            try (StatementIndexStream s = open()) {
                fromStream = StatementSearcher.search(s, parsed, 25);
            }

            assertEquals(fromArray.total(), fromStream.total(), "total differs for: " + q);
            assertEquals(fromArray.hits().size(), fromStream.hits().size(),
                    "page size differs for: " + q);
            assertEquals(fromArray.facets(), fromStream.facets(), "facets differ for: " + q);
            for (int i = 0; i < fromArray.hits().size(); i++) {
                assertEquals(fromArray.hits().get(i).row(), fromStream.hits().get(i).row(),
                        "hit " + i + " differs for: " + q);
                assertEquals(fromArray.hits().get(i).matchedColumns(),
                        fromStream.hits().get(i).matchedColumns(),
                        "matched columns differ for hit " + i + " of: " + q);
            }
        }
    }

    @Test
    void theHeaderScalarsAreCapturedWhilePassing() throws Exception {
        Assumptions.assumeTrue(index != null, "no statement index on disk");
        JsonNode doc = M.readTree(Files.newInputStream(index));
        try (StatementIndexStream s = open()) {
            int rows = 0;
            for (JsonNode ignored : s) rows++;
            assertEquals(doc.path("totalStatements").asInt(-1), s.totalStatements(),
                    "totalStatements was not picked up from the stream");
            assertEquals(doc.path("statementsWithColumns").asInt(-1), s.statementsWithColumns(),
                    "statementsWithColumns was not picked up from the stream");
            assertEquals(doc.get("statements").size(), rows,
                    "the stream yielded a different number of rows than the array holds");
        }
    }

    @Test
    void searchingRetainsFarLessThanTheDocument() throws Exception {
        Assumptions.assumeTrue(index != null, "no statement index on disk");
        assertTrue(Files.size(index) > 1_000_000,
                "the fixture is only " + Files.size(index) + " bytes, too small to tell "
                        + "streaming from materialising");

        // Retention, not allocation. The first version of this measured used-heap before and
        // after without collecting in between, and reported 12 MB — which is the churn of 5,015
        // row objects being created and dropped, exactly what streaming is supposed to do. It
        // failed the assertion while the code was behaving correctly. Collecting on both sides
        // measures what is still held, which is the claim.
        long before = retainedBytes();
        StatementSearcher.Result r;
        try (StatementIndexStream s = open()) {
            r = StatementSearcher.search(s, SearchQuery.parse("ERP"), 25);
        }
        long retainedMb = Math.max(0, retainedBytes() - before) / 1_000_000;

        assertTrue(r.total() > 0, "the probe query matched nothing, so it measured nothing");
        // The parsed document measures 24.3 MB. A generous ceiling: the point is the order of
        // magnitude, not a number that fails on a different GC schedule.
        assertTrue(retainedMb < 8,
                "searching retained ~" + retainedMb + " MB; the parsed document is ~24 MB, so this "
                        + "is materialising rather than streaming");
        // And the comparison that gives the number meaning, in the same run and on the same heap.
        long treeBefore = retainedBytes();
        JsonNode held = M.readTree(Files.newInputStream(index));
        long treeMb = Math.max(0, retainedBytes() - treeBefore) / 1_000_000;
        assertNotNull(held.get("statements"));
        assertTrue(treeMb > retainedMb,
                "materialising the document retained " + treeMb + " MB and streaming retained "
                        + retainedMb + " MB — if those are the same, this test is measuring noise");
    }

    @Test
    void theStreamIsSinglePassAndSaysSo() throws Exception {
        Assumptions.assumeTrue(index != null, "no statement index on disk");
        try (StatementIndexStream s = open()) {
            for (JsonNode ignored : s) { /* drain */ }
            // A second pass must fail loudly. Returning an empty iterator would present as "no
            // matches", which is a wrong answer rather than an error.
            assertThrows(IllegalStateException.class, s::iterator,
                    "a second pass returned quietly, so a re-search would find nothing");
        }
    }

    @Test
    void theScalarsAreFoundWhicheverOrderTheyAppearIn() throws Exception {
        // The corpus puts them before `statements`; nothing guarantees that, and depending on it
        // would be a decision about an artifact this class does not produce.
        String after = """
                {"statements":[{"operation":"SELECT","tableName":"T","nodeName":"P"}],
                 "totalStatements":7,"statementsWithColumns":3}
                """;
        try (StatementIndexStream s = new StatementIndexStream(M,
                new java.io.ByteArrayInputStream(after.getBytes(StandardCharsets.UTF_8)))) {
            List<JsonNode> rows = new ArrayList<>();
            for (JsonNode r : s) rows.add(r);
            assertEquals(1, rows.size());
            assertEquals(7, s.totalStatements(), "a trailing totalStatements was missed");
            assertEquals(3, s.statementsWithColumns(),
                    "a trailing statementsWithColumns was missed");
        }
    }

    @Test
    void unrelatedNestedStructureIsSkippedNotSearched() throws Exception {
        // skipChildren on everything else: a future sibling object must not become rows.
        String withNoise = """
                {"totalStatements":1,
                 "somethingElse":{"nested":[{"operation":"UPDATE","tableName":"X"}]},
                 "statements":[{"operation":"SELECT","tableName":"T","nodeName":"P"}]}
                """;
        try (StatementIndexStream s = new StatementIndexStream(M,
                new java.io.ByteArrayInputStream(withNoise.getBytes(StandardCharsets.UTF_8)))) {
            List<JsonNode> rows = new ArrayList<>();
            for (JsonNode r : s) rows.add(r);
            assertEquals(1, rows.size(), "a nested unrelated array was read as statements");
            assertEquals("SELECT", rows.get(0).path("operation").asText());
        }
    }

    @Test
    void anEmptyOrAbsentStatementsArrayIsNotAnError() throws Exception {
        try (StatementIndexStream s = new StatementIndexStream(M,
                new java.io.ByteArrayInputStream(
                        "{\"totalStatements\":0,\"statements\":[]}".getBytes(StandardCharsets.UTF_8)))) {
            StatementSearcher.Result r = StatementSearcher.search(s, SearchQuery.parse("x"), 10);
            assertEquals(0, r.total());
        }
        // And the array form's contract is unchanged: a missing node searches nothing.
        assertEquals(0, StatementSearcher.search((JsonNode) null, SearchQuery.parse("x"), 10).total());
    }
}
