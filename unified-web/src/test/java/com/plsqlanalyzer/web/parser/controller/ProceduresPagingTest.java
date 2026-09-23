package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetSort;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-001 / MID-002 — the procedure list is a paged facet, not a field inside `/index`.
 *
 * <h2>The measurement</h2>
 *
 * <p>Across every analysis on disk the node list is <b>53.1%</b> of `/index`, on a request whose
 * other fields total 446 bytes — and `/index` is fetched on every analysis load. At the
 * 8,000-procedure target that is ~14 MB of node list per load.
 *
 * <p>The panel reads 8 of a node's 21 fields, so projection alone is 4.6× (90,901 bytes → 19,846)
 * before paging applies.
 */
class ProceduresPagingTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode node(String id, String name, String schema, int depth, int loc) {
        return mapper.createObjectNode()
                .put("nodeId", id).put("name", name).put("schema", schema)
                .put("objectType", "PROCEDURE").put("depth", depth).put("linesOfCode", loc);
    }

    @Test
    void theDefaultOrderIsTheOneTheSidebarAlreadyShowed() {
        // The panel sorted by depth then name client-side. A different default here would reorder
        // the sidebar on the day the endpoint is switched on — a change nobody asked for, arriving
        // as a side effect of a performance fix.
        assertEquals("DEPTH_ASC", ParserFacetSpecs.PROCEDURES.defaultSort());
    }

    @Test
    void everyDeclaredSortOrdersSomething() {
        // A sort naming a field the rows do not have is inert: it is accepted and returns insertion
        // order. Checked on the sort KEY, because comparator() tiebreaks on identity and so
        // produces a stable, plausible order even when every row scores the same.
        JsonNode a = node("S$A", "AAA", "AAA", 1, 10);
        JsonNode b = node("S$B", "ZZZ", "ZZZ", 9, 900);
        for (var e : ParserFacetSpecs.PROCEDURES.sorts().entrySet()) {
            String ka = e.getValue().key().apply(a);
            String kb = e.getValue().key().apply(b);
            assertNotEquals(ka, kb,
                    e.getKey() + " scores two entirely different rows identically, so it names a "
                            + "field a node does not have");
        }
    }

    @Test
    void identityIsTheCrawlersOwnNodeId() {
        // A non-unique cursor tiebreaker loses or repeats a row at a page boundary, and only for
        // data where the duplicate exists.
        var id = ParserFacetSpecs.PROCEDURES.id();
        assertEquals("PKG$A", id.apply(node("PKG$A", "A", "S", 1, 1)));
        assertNotEquals(id.apply(node("PKG$A", "A", "S", 1, 1)),
                id.apply(node("PKG$B", "A", "S", 1, 1)),
                "two nodes sharing a name collide on identity");
    }

    @Test
    void searchMatchesWhatTheSidebarFilterMatched() {
        // The panel's filter key was nodeId + name + schema, lower-cased. Narrowing it here would
        // silently change what the sidebar finds.
        String text = ParserFacetSpecs.PROCEDURES.searchText()
                .apply(node("ANALYZER$ERP_USP_RUN", "ERP_USP_RUN", "ANALYZER", 1, 10));
        assertTrue(text.contains("analyzer$erp_usp_run"), "nodeId is not searchable");
        assertTrue(text.contains("erp_usp_run"), "name is not searchable");
        assertTrue(text.contains("analyzer"), "schema is not searchable");
    }

    @Test
    void pagingTheWholeListReturnsEveryRowExactlyOnce() {
        // The property a paged sidebar depends on. Walked page by page and compared against the
        // input, because a boundary that drops or repeats a row is invisible on one page.
        List<JsonNode> rows = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            rows.add(node("S$P" + i, String.format("P_%03d", i), "S", i % 7, i));
        }
        var arr = mapper.createArrayNode();
        rows.forEach(arr::add);

        var responder = new com.plsqlanalyzer.web.page.FacetPageResponder(mapper, 10_000);
        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 100; guard++) {
            var res = responder.respond(arr, null, ParserFacetSpecs.PROCEDURES,
                    new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                            25, cursor, "NAME_ASC", null, null, null, "application/json"),
                    null);
            JsonNode body = res.getBody();
            for (JsonNode item : body.path("items")) seen.add(item.path("nodeId").asText());
            cursor = body.path("page").path("nextCursor").asText(null);
            if (cursor == null || cursor.isEmpty()) break;
        }
        assertEquals(250, seen.size(), "paging returned " + seen.size() + " of 250 rows");
        assertEquals(250, seen.stream().distinct().count(), "a row was returned on two pages");
    }

    @Test
    void exceptionHandlerFilterColumnMatchesOnlyProceduresWithHandlers() {
        // UI-013 groundwork: exHandlers.js aggregates PA.nodeIndex.all() client-side today to find
        // procedures with exceptionHandlers > 0. This filter column (index "9", a hidden preset —
        // not a user-facing popover) lets that same question be asked of /procedures?cf=9:Y
        // instead, once the client side is converted.
        var withHandlers = mapper.createObjectNode()
                .put("nodeId", "S$WITH").put("name", "WITH").put("schema", "S").put("depth", 1);
        withHandlers.putObject("counts").put("exceptionHandlers", 3);
        var withoutHandlers = mapper.createObjectNode()
                .put("nodeId", "S$WITHOUT").put("name", "WITHOUT").put("schema", "S").put("depth", 1);
        withoutHandlers.putObject("counts").put("exceptionHandlers", 0);
        var noCountsAtAll = mapper.createObjectNode()
                .put("nodeId", "S$NOCOUNTS").put("name", "NOCOUNTS").put("schema", "S").put("depth", 1);

        var arr = mapper.createArrayNode().add(withHandlers).add(withoutHandlers).add(noCountsAtAll);
        var responder = new com.plsqlanalyzer.web.page.FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, ParserFacetSpecs.PROCEDURES,
                new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                        null, null, null, null, null, List.of("9:Y"), "application/json"), null);

        List<String> names = new ArrayList<>();
        for (JsonNode item : res.getBody().path("items")) names.add(item.path("name").asText());
        assertEquals(List.of("WITH"), names,
                "cf=9:Y must match only the procedure whose exceptionHandlers count is > 0");
    }

    @Test
    void anUnpagedCallerStillGetsTheBareArray() {
        // Existing callers send no paging parameters and must be unaffected; conversion has to be
        // possible one screen at a time.
        var arr = mapper.createArrayNode();
        arr.add(node("S$A", "A", "S", 1, 1));
        var responder = new com.plsqlanalyzer.web.page.FacetPageResponder(mapper, 10_000);
        var res = responder.respond(arr, null, ParserFacetSpecs.PROCEDURES,
                new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                        null, null, null, null, null, null, null), null);
        assertTrue(res.getBody().isArray(),
                "an unpaged request no longer receives the bare array it always did");
    }
}
