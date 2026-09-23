package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.page.FacetSpec;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wire contract of the Table Detail lists — PLAN-08 §3.2 and §4.3 step 10.
 *
 * <p>Two properties matter more than the rest and are asserted first: <b>absent paging parameters
 * return today's exact response</b>, and <b>a full cursor walk returns every row exactly once</b>.
 * The second is not theoretical — a non-unique cursor tiebreaker loses a row at a page boundary,
 * only for data that contains the duplicate, and these rows are duplicate-prone by construction
 * (one procedure joins the same pair of tables on many lines).
 */
class TableDetailFacetSpecsTest {

    private static final ObjectMapper M = new ObjectMapper();

    private final FacetPageResponder responder = new FacetPageResponder(M, 10_000);

    /**
     * Rows shaped like the reader's output, and deliberately awkward: three procedures, repeated
     * join types, repeated joined tables and repeated lines — so every declared sort has ties
     * that only the row identity can break.
     *
     * <p>The three key fields vary independently, because the reader's de-duplication guarantees
     * that {@code direction|joinType|joinedTable|procedureId} is unique. A fixture that repeated
     * that tuple would be testing rows the scan cannot emit.
     */
    private static ArrayNode joins(int perProc) {
        String[] procs = {"S$A", "S$B", "S$C"};
        String[] types = {"JOIN", "LEFT", "INNER"};
        String[] tables = {"CUSTOMER", "INVOICE", "SHIPMENT"};
        ArrayNode a = M.createArrayNode();
        int seq = 0;
        for (String proc : procs) {
            for (int i = 0; i < perProc; i++) {
                ObjectNode o = a.addObject();
                o.put("direction", i % 2 == 0 ? "OUT" : "IN");
                o.put("joinType", types[i % types.length]);
                o.put("joinedTable", tables[(i / types.length) % tables.length]);
                o.put("joinedTableAlias", "x" + i);
                o.put("condition", "c" + i + ".id = o.id");
                o.put("line", 10 + (i % 4));
                o.put("procedureId", proc);
                o.put("procedureName", proc.substring(proc.indexOf('$') + 1));
                o.put("sourceFile", proc + ".sql");
                o.put("seq", seq++);
            }
        }
        return a;
    }

    private static FacetPageResponder.Params params(Integer limit, String after, String sort,
                                                    String q, List<String> cf) {
        return new FacetPageResponder.Params(limit, after, sort, q, null, cf,
                FacetPageResponder.PAGE_MEDIA_TYPE);
    }

    private static String id(JsonNode row) {
        return TableDetailFacetSpecs.TABLE_JOINS.id().apply(row);
    }

    @Test
    void absentParametersReturnTheDocumentUntouched() {
        ArrayNode doc = joins(4);
        ResponseEntity<JsonNode> res = responder.respond(doc, null,
                TableDetailFacetSpecs.TABLE_JOINS,
                new FacetPageResponder.Params(null, null, null, null, null, null, null), null);

        assertEquals(HttpStatus.OK, res.getStatusCode());
        assertSame(doc, res.getBody(), "the legacy path must not rebuild the document");
        assertNull(res.getHeaders().getFirst(FacetPageResponder.TRUNCATED_HEADER));
    }

    @Test
    void theDefaultSortIsTheScanOrderSoPageOneDoesNotChangeOnTheDayItIsConverted() {
        ArrayNode doc = joins(5);
        JsonNode page = responder.respond(doc, null, TableDetailFacetSpecs.TABLE_JOINS,
                params(1000, null, null, null, null), null).getBody();

        List<Integer> seqs = new ArrayList<>();
        page.path("items").forEach(r -> seqs.add(r.path("seq").asInt()));
        List<Integer> docSeqs = new ArrayList<>();
        doc.forEach(r -> docSeqs.add(r.path("seq").asInt()));
        assertEquals(docSeqs, seqs);
        assertEquals("SCAN", page.path("page").path("sort").asText());
    }

    @Test
    void aFullCursorWalkReturnsEveryRowExactlyOnceUnderEverySort() {
        ArrayNode doc = joins(9);
        for (String sort : TableDetailFacetSpecs.TABLE_JOINS.sorts().keySet()) {
            for (int limit : new int[]{1, 4, 27}) {
                Set<String> seen = new HashSet<>();
                int count = 0;
                String cursor = null;
                for (int guard = 0; guard < 200; guard++) {
                    JsonNode body = responder.respond(doc, null,
                            TableDetailFacetSpecs.TABLE_JOINS,
                            params(limit, cursor, sort, null, null), null).getBody();
                    for (JsonNode row : body.path("items")) {
                        assertTrue(seen.add(id(row)),
                                "row repeated across a page boundary: " + id(row)
                                        + " sort=" + sort + " limit=" + limit);
                        count++;
                    }
                    assertEquals(doc.size(), body.path("page").path("total").asInt());
                    assertTrue(body.path("page").path("totalIsExact").asBoolean());
                    if (!body.path("page").path("hasMore").asBoolean()) {
                        assertTrue(body.path("page").path("nextCursor").isNull());
                        break;
                    }
                    cursor = body.path("page").path("nextCursor").asText();
                    assertNotNull(cursor);
                }
                assertEquals(doc.size(), count,
                        "rows lost while paging, sort=" + sort + " limit=" + limit);
            }
        }
    }

    @Test
    void rowIdentityIsUniqueForRowsTheScanCanProduce() {
        // The scan de-duplicates on exactly this key, so a collision here would mean the reader
        // emitted two rows the cursor cannot tell apart.
        ArrayNode doc = joins(9);
        Set<String> ids = new HashSet<>();
        doc.forEach(r -> assertTrue(ids.add(id(r)), "duplicate row identity: " + id(r)));
    }

    @Test
    void searchIsAnsweredByTheEndpointOverTheWholeResultSet() {
        ArrayNode doc = joins(6);
        JsonNode body = responder.respond(doc, null, TableDetailFacetSpecs.TABLE_JOINS,
                params(2, null, null, "invoice", null), null).getBody();

        assertTrue(body.path("page").path("total").asInt() > 2,
                "the matched total must count the whole result set, not the page");
        for (JsonNode row : body.path("items")) {
            assertEquals("INVOICE", row.path("joinedTable").asText());
        }
    }

    @Test
    void searchAlsoMatchesTheOnPredicateAsTheModalDid() {
        ArrayNode doc = joins(6);
        JsonNode body = responder.respond(doc, null, TableDetailFacetSpecs.TABLE_JOINS,
                params(50, null, null, "c3.id", null), null).getBody();
        assertTrue(body.path("page").path("total").asInt() > 0,
                "the modal's searchFn searched the condition; dropping it would be a regression");
    }

    @Test
    void columnFiltersAreAnsweredByTheEndpoint() {
        ArrayNode doc = joins(6);
        JsonNode body = responder.respond(doc, null, TableDetailFacetSpecs.TABLE_JOINS,
                params(50, null, null, null, List.of("4:B")), null).getBody();

        assertTrue(body.path("page").path("total").asInt() > 0);
        for (JsonNode row : body.path("items")) {
            assertEquals("B", row.path("procedureName").asText());
        }
    }

    @Test
    void anUnfilterableColumnIsRefusedRatherThanIgnored() {
        ArrayNode doc = joins(2);
        // Column 3 is the Condition cell, which has no popover. Ignoring the filter would return
        // rows the user asked to exclude, with the filter icon lit.
        assertThrowsIllegalArgument(() -> responder.respond(doc, null,
                TableDetailFacetSpecs.TABLE_JOINS,
                params(50, null, null, null, List.of("3:x")), null));
    }

    @Test
    void anUnknownSortIsFourHundredNotASilentlyDifferentOrder() {
        ArrayNode doc = joins(2);
        assertThrowsIllegalArgument(() -> responder.respond(doc, null,
                TableDetailFacetSpecs.TABLE_JOINS, params(50, null, "CONDITION_ASC", null, null),
                null));
    }

    @Test
    void aCursorMintedUnderADifferentSortIsRejected() {
        ArrayNode doc = joins(6);
        JsonNode first = responder.respond(doc, null, TableDetailFacetSpecs.TABLE_JOINS,
                params(3, null, "SCAN", null, null), null).getBody();
        String cursor = first.path("page").path("nextCursor").asText();

        try {
            responder.respond(doc, null, TableDetailFacetSpecs.TABLE_JOINS,
                    params(3, cursor, "TABLE_ASC", null, null), null);
            org.junit.jupiter.api.Assertions.fail("a cursor from another sort must not be honoured");
        } catch (IllegalArgumentException | com.analyzer.storage.page.Cursor.InvalidCursorException e) {
            // expected — the controller turns this into a 400 with a reason
        }
    }

    @Test
    void theScopeSelectorFiltersServerSide() {
        ArrayNode doc = joins(4);
        FacetSpec spec = TableDetailFacetSpecs.TABLE_JOINS;
        JsonNode body = responder.respond(doc, null, spec, params(50, null, null, null, null),
                row -> spec.procedureIds().apply(row).contains("S$B")).getBody();

        assertEquals(4, body.path("page").path("total").asInt());
        for (JsonNode row : body.path("items")) {
            assertEquals("S$B", row.path("procedureId").asText());
        }
    }

    @Test
    void theSummarySuppliesTheCountsAndPopoverValuesTheArrayUsedToSupply() {
        ArrayNode doc = joins(4);
        FacetSpec spec = TableDetailFacetSpecs.TABLE_JOINS;
        JsonNode sum = responder.summary(doc, null, spec,
                row -> spec.procedureIds().apply(row).contains("S$B")).getBody();

        assertEquals(12, sum.path("total").asInt());
        assertEquals(4, sum.path("scopedTotal").asInt());
        // Collected inside the scope, because that is the set the popover offered when the client
        // built it from the scoped array it had been handed.
        assertEquals(1, sum.path("columnValues").path("4").size());
        assertEquals("B", sum.path("columnValues").path("4").get(0).asText());
        assertTrue(sum.path("sorts").size() > 1);
    }

    @Test
    void columnSpecDeclaresOnlyTheColumnsTheModalCanSortAndFilter() {
        FacetSpec spec = TableDetailFacetSpecs.TABLE_COLUMNS;
        assertEquals("SCAN", spec.defaultSort());
        assertTrue(spec.sorts().containsKey("COLUMN_ASC"));
        assertFalse(spec.sorts().containsKey("DATATYPE_ASC"));
    }

    private static void assertThrowsIllegalArgument(Runnable r) {
        try {
            r.run();
            org.junit.jupiter.api.Assertions.fail("expected a 400-worthy IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertNotNull(expected.getMessage());
        }
    }
}
