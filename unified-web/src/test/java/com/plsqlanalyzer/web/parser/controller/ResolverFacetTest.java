package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.JsonFacetPager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MID-005 — {@code /resolver/{type}/page} walks every row exactly once, for both row shapes.
 *
 * <p>Two shapes, both real: {@code q1_object_resolution.json} is a plain array, and
 * {@code q4_direct_dependencies.json} is a data-keyed map ({@code "SCHEMA.OBJECT": [...]}) that
 * {@link ResolverFacet#rowsFromMap} turns into rows. Read directly from disk rather than through
 * the controller, since the point under test is the row conversion and the pager, not Spring
 * wiring — {@code JsonFacetPager} is the same pager {@code FacetPageResponder} delegates to.
 */
class ResolverFacetTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final Path RESOLVER = Path.of(
            "../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE/resolver");

    private static JsonNode read(String file) throws Exception {
        return M.readTree(Files.newInputStream(RESOLVER.resolve(file)));
    }

    @Test
    void arrayShapedResolverDataPagesCompletely() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(RESOLVER), "no resolver fixture on disk");
        // Expected count comes from the raw source array, not from rowsFromArray's own output —
        // comparing the converted list against itself cannot see a bug inside the conversion.
        // Caught exactly this way while calibrating: a mutation that dropped the array's last
        // element left rows.size() and seen.size() agreeing with each other and both wrong.
        JsonNode source = read("q1_object_resolution.json");
        int expected = source.size();
        List<JsonNode> rows = ResolverFacet.rowsFromArray(source);
        assertEquals(expected, rows.size(),
                "rowsFromArray produced a different count than the source array");
        assertTrue(rows.size() > 0, "the fixture has no rows to walk");

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 50; guard++) {
            Page<JsonNode> page = page(rows, 3, cursor);
            page.items().forEach(r -> seen.add(r.path("objectName").asText()));
            cursor = page.page().nextCursor();
            if (cursor == null) break;
        }
        assertEquals(expected, seen.size(),
                "paging returned " + seen.size() + " of " + expected + " source rows");
        assertEquals(expected, seen.stream().distinct().count(),
                "a row was returned on two pages");
    }

    @Test
    void mapShapedResolverDataIsNormalisedIntoOneRowPerKey() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(RESOLVER), "no resolver fixture on disk");
        JsonNode map = read("q4_direct_dependencies.json");
        assertTrue(map.isObject(), "the fixture is not the data-keyed map this test expects");

        List<JsonNode> rows = ResolverFacet.rowsFromMap(M, map);
        assertEquals(map.size(), rows.size(),
                "row count does not match the number of keys in the source map");

        for (JsonNode row : rows) {
            String name = row.path("objectName").asText(null);
            assertNotNull(name, "a row has no objectName");
            assertTrue(map.has(name), "row names a key not present in the source map: " + name);
            assertEquals(map.get(name), row.get("dependencies"),
                    "the dependencies array for " + name + " does not match the source");
        }
    }

    @Test
    void mapShapedDataPagesCompletelyToo() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(RESOLVER), "no resolver fixture on disk");
        JsonNode source = read("q4_direct_dependencies.json");
        int expected = source.size();
        List<JsonNode> rows = ResolverFacet.rowsFromMap(M, source);
        assertTrue(expected > 0, "the fixture has no rows to walk");

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 50; guard++) {
            Page<JsonNode> page = page(rows, 5, cursor);
            page.items().forEach(r -> seen.add(r.path("objectName").asText()));
            cursor = page.page().nextCursor();
            if (cursor == null) break;
        }
        assertEquals(expected, seen.size(),
                "paging returned " + seen.size() + " of " + expected + " source map keys");
    }

    @Test
    void anEmptyArrayPagesToNothingWithoutError() throws Exception {
        // q3_reverse_dependencies.json is empty in every fixture measured for this row (§ MID-005
        // in the registry says so) — the boundary that matters here, not the happy path.
        List<JsonNode> rows = ResolverFacet.rowsFromArray(M.createArrayNode());
        Page<JsonNode> page = page(rows, 50, null);
        assertEquals(0, page.items().size());
        assertEquals(null, page.page().nextCursor());
    }

    private static Page<JsonNode> page(List<JsonNode> rows, int limit, String after) {
        var spec = ResolverFacet.SPEC;
        PageRequest req = PageRequest.of(limit, after, spec.defaultSort(), null,
                spec.sorts().keySet(), spec.defaultSort(), spec.defaultLimit());
        return JsonFacetPager.page(rows.iterator(), req, spec.sorts(), spec.id(),
                spec.searchText(), null);
    }

    @Test
    void aNonObjectIsNotMistakenForTheMapShape() {
        // rowsFromMap must not throw on the array/empty shapes the other three resolver types use.
        assertEquals(0, ResolverFacet.rowsFromMap(M, M.createArrayNode()).size());
        assertEquals(0, ResolverFacet.rowsFromArray(M.createObjectNode()).size());
    }
}
