package com.jaranalyzer.controller;

import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.JsonFacetPager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MID-017 — the correction-log and run-log listings page completely, and {@code /catalog} is
 * deliberately excluded (see {@link LogListFacets}'s own javadoc for why).
 */
class LogListFacetsTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static Map<String, Object> correctionRow(String name, String endpoint, long size) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("label", endpoint + " - prompt");
        m.put("type", "prompt");
        m.put("endpoint", endpoint);
        m.put("size", size);
        return m;
    }

    private static Map<String, Object> runLogRow(String name, long size) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("size", size);
        m.put("modified", "2026-09-07T00:00:00Z");
        return m;
    }

    private static Page<JsonNode> page(List<JsonNode> rows, com.plsqlanalyzer.web.page.FacetSpec spec,
                                       int limit, String after) {
        PageRequest req = PageRequest.of(limit, after, spec.defaultSort(), null,
                spec.sorts().keySet(), spec.defaultSort(), spec.defaultLimit());
        return JsonFacetPager.page(rows.iterator(), req, spec.sorts(), spec.id(),
                spec.searchText(), null);
    }

    @Test
    void correctionLogsConversionPreservesEveryField() {
        Map<String, Object> src = correctionRow("createUser_corr_input.txt", "createUser", 512);
        List<JsonNode> rows = LogListFacets.rows(M, List.of(src));
        JsonNode row = rows.get(0);
        assertEquals("createUser_corr_input.txt", row.path("name").asText());
        assertEquals("createUser", row.path("endpoint").asText());
        assertEquals(512, row.path("size").asLong());
        assertEquals("prompt", row.path("type").asText());
    }

    @Test
    void correctionLogsPageCompletely() {
        List<Map<String, Object>> maps = new ArrayList<>();
        for (int i = 0; i < 37; i++) {
            maps.add(correctionRow("ep" + i + "_corr_input.txt", "ep" + i, i * 100));
        }
        List<JsonNode> rows = LogListFacets.rows(M, maps);

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 20; guard++) {
            Page<JsonNode> p = page(rows, LogListFacets.CORRECTION_LOGS, 5, cursor);
            p.items().forEach(r -> seen.add(r.path("name").asText()));
            cursor = p.page().nextCursor();
            if (cursor == null) break;
        }
        assertEquals(maps.size(), seen.size(),
                "paging returned " + seen.size() + " of " + maps.size() + " correction logs");
        assertEquals(maps.size(), seen.stream().distinct().count(),
                "a correction log row was returned on two pages");
    }

    @Test
    void runLogsPageCompletelyAndDefaultNewestFirst() {
        List<Map<String, Object>> maps = new ArrayList<>();
        for (int i = 0; i < 23; i++) {
            maps.add(runLogRow("run_2026090" + (i % 10) + "_" + i + ".log", i * 10));
        }
        List<JsonNode> rows = LogListFacets.rows(M, maps);

        assertEquals("NAME_DESC", LogListFacets.RUN_LOGS.defaultSort(),
                "run logs must default to newest-first, matching the un-paged reader's own order");

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 20; guard++) {
            Page<JsonNode> p = page(rows, LogListFacets.RUN_LOGS, 4, cursor);
            p.items().forEach(r -> seen.add(r.path("name").asText()));
            cursor = p.page().nextCursor();
            if (cursor == null) break;
        }
        assertEquals(maps.size(), seen.size(),
                "paging returned " + seen.size() + " of " + maps.size() + " run logs");
    }

    @Test
    void emptyListsPageToNothingWithoutError() {
        for (var spec : List.of(LogListFacets.CORRECTION_LOGS, LogListFacets.RUN_LOGS)) {
            Page<JsonNode> p = page(List.of(), spec, 50, null);
            assertTrue(p.items().isEmpty());
            assertEquals(null, p.page().nextCursor());
        }
    }
}
