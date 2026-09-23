package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.page.JsonFacetPager;
import com.plsqlanalyzer.web.parser.model.AnalysisInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The analysis-history projection, sorts and facets — Q31 / I11, step 15 of PLAN-08 §4.3.
 *
 * <p>Three things are checked, in rough order of how badly they would hurt if wrong: the derived
 * {@code dbType} (which replaces two disagreeing browser-side scanners, so it has to match what
 * both screens showed), the cursor chain under every one of the twenty declared sorts, and the
 * scoping of the facet summary — aggregates over the filter, dropdown values over everything.
 */
class HistoryFacetTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static AnalysisInfo info(String name, String jdbcUrl, String project,
                                     long timestamp, long duration, int errors) {
        AnalysisInfo a = new AnalysisInfo();
        a.setName(name);
        a.setEntryPoint("PKG_" + name);
        a.setEntrySchema("APP");
        a.setJdbcUrl(jdbcUrl);
        a.setProject(project);
        a.setEnvironment("DEV");
        a.setConnectionName("conn-" + project);
        a.setTimestamp(timestamp);
        a.setTotalDurationMs(duration);
        a.setCrawlTimeMs(duration / 2);
        a.setTotalNodes(3);
        a.setTotalTables(4);
        a.setTotalLinesOfCode(500);
        a.setDbCallCount(7);
        List<String> msgs = new ArrayList<>();
        for (int i = 0; i < errors; i++) msgs.add("boom " + i + ": " + "x".repeat(120));
        a.setErrors(msgs);
        return a;
    }

    /* ── the projection ──────────────────────────────────────────────────────────────────── */

    @Test
    @DisplayName("errors[] becomes errorCount, and jdbcUrl is gone")
    void projection() {
        ObjectNode row = HistoryFacet.row(M, info("a", "jdbc:oracle:thin:@//h:1521/X", "P",
                1000L, 500L, 3));
        assertEquals(3, row.path("errorCount").asInt());
        assertFalse(row.has("errors"), "the messages are fetched per row, not sent for all rows");
        assertFalse(row.has("jdbcUrl"),
                "its only readers on this list were the dialect scanners dbType replaces");
        // Everything both screens actually render is still there.
        for (String field : List.of("name", "entryPoint", "entrySchema", "totalNodes",
                "totalTables", "totalEdges", "totalLinesOfCode", "maxDepth", "crawlTimeMs",
                "totalDurationMs", "dbCallCount", "timestamp", "project", "environment",
                "connectionName", "dbType")) {
            assertTrue(row.has(field), "projection dropped a rendered field: " + field);
        }
    }

    @Test
    @DisplayName("the projection is smaller than the row it replaces")
    void projectionIsSmaller() {
        AnalysisInfo a = info("a", "jdbc:oracle:thin:@//host:1521/FREEPDB1", "P", 1L, 2L, 4);
        int before = M.valueToTree(a).toString().length();
        int after = HistoryFacet.row(M, a).toString().length();
        assertTrue(after < before, "expected a reduction, got " + before + " -> " + after);
    }

    @Test
    @DisplayName("dbType covers every dialect the two client scanners covered, PostgreSQL included")
    void dbTypeCoversEveryDialect() {
        assertEquals("ORACLE", dbType("jdbc:oracle:thin:@//localhost:1521/FREEPDB1"));
        assertEquals("SQLSERVER", dbType("jdbc:sqlserver://localhost:1433;databaseName=x"));
        assertEquals("MYSQL", dbType("jdbc:mysql://localhost:3307/anl"));
        assertEquals("DB2", dbType("jdbc:db2://localhost:50000/anl"));
        // AnalysisService.dialectOf omits this one and would have reported UNKNOWN for the two
        // PostgreSQL analyses in the measured data.
        assertEquals("POSTGRES", dbType("jdbc:postgresql://localhost:5432/anl"));
        assertEquals("", dbType("jdbc:h2:mem:test"), "unknown stays empty; the client labels it");
        assertEquals("", dbType(null));
    }

    private static String dbType(String url) {
        return HistoryFacet.dbType(info("a", url, "P", 1L, 1L, 0));
    }

    /* ── the sorts ───────────────────────────────────────────────────────────────────────── */

    @Test
    @DisplayName("the default sort is newest first, which is the order the list already showed")
    void defaultSortIsNewestFirst() {
        List<JsonNode> rows = rows();
        Page<JsonNode> p = page(rows, null, 3, null, null);
        assertEquals("TIME_DESC", p.page().sort());
        List<Long> stamps = p.items().stream().map(r -> r.path("timestamp").asLong()).toList();
        assertEquals(stamps.stream().sorted(java.util.Comparator.reverseOrder()).toList(), stamps);
    }

    @Test
    @DisplayName("DURATION falls back from totalDurationMs to crawlTimeMs, as the client did")
    void durationFallback() {
        AnalysisInfo old = info("legacy", "jdbc:oracle:x", "P", 5L, 0L, 0);
        old.setTotalDurationMs(0);
        old.setCrawlTimeMs(9_999);
        AnalysisInfo current = info("current", "jdbc:oracle:x", "P", 6L, 100L, 0);

        List<JsonNode> rows = List.of(HistoryFacet.row(M, old), HistoryFacet.row(M, current));
        Page<JsonNode> p = page(rows, "DURATION_DESC", 10, null, null);
        assertEquals("legacy", p.items().get(0).path("name").asText(),
                "a run recorded before totalDurationMs existed must not sort as zero");
    }

    @Test
    @DisplayName("every declared sort walks its whole chain, each analysis exactly once")
    void everySortChain() {
        List<JsonNode> rows = rows();
        for (String sort : HistoryFacet.SPEC.sorts().keySet()) {
            Set<String> seen = new LinkedHashSet<>();
            String cursor = null;
            for (int guard = 0; guard < 100; guard++) {
                Page<JsonNode> p = page(rows, sort, 4, null, cursor);
                assertEquals(rows.size(), p.page().total(), sort + ": total must be the whole set");
                for (JsonNode r : p.items()) {
                    assertTrue(seen.add(r.path("name").asText()),
                            sort + ": analysis returned twice: " + r.path("name").asText());
                }
                if (!p.page().hasMore()) {
                    assertNull(p.page().nextCursor(), sort + ": last page carried a cursor");
                    break;
                }
                cursor = p.page().nextCursor();
            }
            assertEquals(rows.size(), seen.size(), sort + ": the chain lost or repeated rows");
        }
    }

    @Test
    @DisplayName("an unknown sort is a 400-shaped rejection, not a different ordering")
    void unknownSort() {
        assertThrows(IllegalArgumentException.class, () -> page(rows(), "BY_VIBES", 5, null, null));
    }

    /* ── search and column filters ───────────────────────────────────────────────────────── */

    @Test
    @DisplayName("search covers the fields both screens searched")
    void search() {
        List<JsonNode> rows = rows();
        // Entry point, exactly one match.
        assertEquals(1, page(rows, null, 50, "pkg_a22", null).page().total());
        // Substring, so PKG_a1 and PKG_a10..PKG_a19 all match — the count is the real one, which
        // is the whole reason it moved off the browser.
        assertEquals(11, page(rows, null, 50, "pkg_a1", null).page().total());
        // Connection name: every third analysis is project Red.
        assertEquals(8, page(rows, null, 50, "conn-red", null).page().total());
        assertEquals(8, page(rows, null, 50, "red", null).page().total());
        assertEquals(0, page(rows, null, 50, "nothing here", null).page().total());
    }

    /* ── the facet summary ───────────────────────────────────────────────────────────────── */

    @Test
    @DisplayName("aggregates describe the filtered set; dropdown values describe every analysis")
    void summaryScoping() {
        List<ObjectNode> all = new ArrayList<>();
        all.add(HistoryFacet.row(M, info("a", "jdbc:oracle:x", "Red", 1L, 100L, 0)));
        all.add(HistoryFacet.row(M, info("b", "jdbc:mysql://x", "Blue", 2L, 200L, 2)));
        all.add(HistoryFacet.row(M, info("c", "jdbc:mysql://x", "Blue", 3L, 300L, 0)));
        List<ObjectNode> matched = List.of(all.get(1), all.get(2));   // the two MySQL ones

        ObjectNode s = HistoryFacet.summary(M, all, matched);

        assertEquals(2, s.path("total").asInt(), "the cards describe the filter");
        assertEquals(3, s.path("unfilteredTotal").asInt());
        assertEquals(1, s.path("withErrors").asInt());
        assertEquals(1, s.path("success").asInt());
        assertEquals(500, s.path("totalDurationMs").asLong());
        assertEquals(300, s.path("maxDurationMs").asLong(),
                "the time bar is a percentage of the longest run in the filtered set");
        assertEquals(14, s.path("dbCallCount").asLong());

        // A dropdown that only offered the values surviving its own filter could not be used to
        // change the filter — pick MySQL and Oracle would disappear.
        assertEquals(1, s.path("dsCounts").path("ORACLE").asInt());
        assertEquals(2, s.path("dsCounts").path("MYSQL").asInt());
        assertEquals("ORACLE", s.path("projectToDs").path("Red").asText());
        assertEquals("MYSQL", s.path("projectToDs").path("Blue").asText());
    }

    @Test
    @DisplayName("an empty history summarises to zeroes rather than failing")
    void emptySummary() {
        ObjectNode s = HistoryFacet.summary(M, List.of(), List.of());
        assertEquals(0, s.path("total").asInt());
        assertEquals(0, s.path("maxDurationMs").asLong());
        assertEquals(0, s.path("dsCounts").size());
    }

    /* ── helpers ─────────────────────────────────────────────────────────────────────────── */

    private static List<JsonNode> rows() {
        String[] urls = {"jdbc:oracle:thin:@//h/X", "jdbc:mysql://h/x",
                         "jdbc:postgresql://h/x", "jdbc:db2://h/x"};
        String[] projects = {"Red", "Blue", "Green"};
        List<JsonNode> out = new ArrayList<>();
        for (int i = 0; i < 23; i++) {
            out.add(HistoryFacet.row(M, info("a" + i, urls[i % 4], projects[i % 3],
                    // Deliberately colliding timestamps and durations, so the tiebreaker is used.
                    1000L + (i % 5), (i % 5) * 100L, i % 3)));
        }
        return out;
    }

    private static Page<JsonNode> page(List<JsonNode> rows, String sort, int limit, String q,
                                       String after) {
        var spec = HistoryFacet.SPEC;
        PageRequest req = PageRequest.of(limit, after, sort, q, spec.sorts().keySet(),
                spec.defaultSort(), spec.defaultLimit());
        return JsonFacetPager.page(rows.iterator(), req, spec.sorts(), spec.id(),
                spec.searchText(), null);
    }
}
