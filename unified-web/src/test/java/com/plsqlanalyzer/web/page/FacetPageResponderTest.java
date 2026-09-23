package com.plsqlanalyzer.web.page;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The compatibility rule that makes conversion possible one endpoint at a time, tested from both
 * directions: <b>absent paging parameters must return today's exact response</b>, and a caller that
 * asks for a page must get the envelope.
 *
 * <p>This is the test that would catch the worst plausible mistake in this work — routing the
 * legacy path through the pager, which would silently reorder every unconverted screen.
 */
class FacetPageResponderTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static final FacetSpec SPEC = new FacetSpec(
            "widgets",
            FacetSort.byName(FacetSort.text("NAME_ASC", "name", false),
                             FacetSort.text("NAME_DESC", "name", true)),
            "NAME_ASC",
            25,
            n -> n.path("name").asText("").toUpperCase(Locale.ROOT),
            n -> n.path("name").asText("").toLowerCase(Locale.ROOT),
            n -> List.of(n.path("kind").asText("")),
            n -> List.of(),
            Map.of("0", FacetSpec.scalarColumn("name", null),
                   "1", FacetSpec.scalarColumn("kind", null)));

    /** Same rows/sorts as {@link #SPEC}, plus a text-mode column ("2" = name, contains/regex). */
    private static final FacetSpec TEXT_SPEC = new FacetSpec(
            "widgets",
            SPEC.sorts(),
            SPEC.defaultSort(),
            SPEC.defaultLimit(),
            SPEC.id(),
            SPEC.searchText(),
            SPEC.typeValues(),
            SPEC.procedureIds(),
            SPEC.filterColumns(),
            Map.of("2", n -> n.path("name").asText("")));

    private final FacetPageResponder responder = new FacetPageResponder(M, 10_000);

    /** Deliberately NOT in sorted order, so a reorder on the legacy path is detectable. */
    private static ArrayNode unsorted() {
        ArrayNode a = M.createArrayNode();
        for (String n : new String[]{"ZETA", "ALPHA", "MIKE"}) {
            ObjectNode o = a.addObject();
            o.put("name", n);
            o.put("kind", n.startsWith("A") ? "FIRST" : "OTHER");
        }
        return a;
    }

    private static FacetPageResponder.Params none() {
        return new FacetPageResponder.Params(null, null, null, null, null, null, null);
    }

    @Test
    void absentParametersReturnTheDocumentUntouched() {
        ArrayNode doc = unsorted();
        ResponseEntity<JsonNode> res = responder.respond(doc, null, SPEC, none(), null);
        assertEquals(HttpStatus.OK, res.getStatusCode());
        assertSame(doc, res.getBody(), "the legacy path must not rebuild the document");
        assertEquals("ZETA", res.getBody().get(0).get("name").asText(),
                "the legacy path must not reorder — that would change every unconverted screen");
        assertNull(res.getHeaders().getFirst(FacetPageResponder.TRUNCATED_HEADER));
    }

    @Test
    void theEnvelopeMediaTypeOptsInWithoutAnyPagingParameter() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(null, null, null, null, null, null,
                        "application/vnd.analyzer.page+json"), null);
        JsonNode body = res.getBody();
        assertTrue(body.has("items"));
        assertTrue(body.has("page"));
        assertEquals("ALPHA", body.get("items").get(0).get("name").asText(),
                "the paged path applies the default sort");
        assertEquals(3, body.get("page").get("total").asLong());
        assertTrue(body.get("page").get("totalIsExact").asBoolean());
        assertFalse(body.get("page").get("hasMore").asBoolean());
        assertTrue(body.get("page").get("nextCursor").isNull());
        assertEquals("NAME_ASC", body.get("page").get("sort").asText());
    }

    @Test
    void aLimitAloneOptsIn() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(2, null, null, null, null, null, null), null);
        JsonNode body = res.getBody();
        assertEquals(2, body.get("items").size());
        assertTrue(body.get("page").get("hasMore").asBoolean());
        assertFalse(body.get("page").get("nextCursor").isNull());
    }

    @Test
    void anEmptySearchIsNotASearch() {
        // `q=` must mean "no filter", not "match the empty string", or clearing a search box
        // would look like a filter change and reload for nothing.
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(null, null, null, "", null, null, null), null);
        assertTrue(res.getBody().isArray(), "an empty q must stay on the legacy path");
    }

    @Test
    void noDataIsTwoHundredAndFourNotAnEmptyArray() {
        ResponseEntity<JsonNode> res = responder.respond(null, null, SPEC, none(), null);
        assertEquals(HttpStatus.NO_CONTENT, res.getStatusCode());
    }

    @Test
    void aDocumentWrappingItsArrayIsPagedTheSameWay() {
        ObjectNode doc = M.createObjectNode();
        doc.put("totalWidgets", 3);
        doc.set("widgets", unsorted());
        ResponseEntity<JsonNode> legacy = responder.respond(doc, "widgets", SPEC, none(), null);
        assertSame(doc, legacy.getBody());

        ResponseEntity<JsonNode> paged = responder.respond(doc, "widgets", SPEC,
                new FacetPageResponder.Params(1, null, null, null, null, null, null), null);
        assertEquals(1, paged.getBody().get("items").size());
        assertEquals(3, paged.getBody().get("page").get("total").asLong());
    }

    @Test
    void theUnpagedCapTruncatesVisiblyRatherThanFailing() {
        FacetPageResponder capped = new FacetPageResponder(M, 2);
        ResponseEntity<JsonNode> res = capped.respond(unsorted(), null, SPEC, none(), null);
        assertEquals(HttpStatus.OK, res.getStatusCode(), "an unconverted screen must still render");
        assertEquals(2, res.getBody().size());
        assertEquals("true", res.getHeaders().getFirst(FacetPageResponder.TRUNCATED_HEADER));
    }

    @Test
    void theCapKeepsTheSiblingScalarsOfAWrappedDocument() {
        FacetPageResponder capped = new FacetPageResponder(M, 1);
        ObjectNode doc = M.createObjectNode();
        doc.put("totalWidgets", 3);
        doc.set("widgets", unsorted());
        ResponseEntity<JsonNode> res = capped.respond(doc, "widgets", SPEC, none(), null);
        assertEquals(3, res.getBody().get("totalWidgets").asInt());
        assertEquals(1, res.getBody().get("widgets").size());
    }

    @Test
    void anUnknownSortIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(null, null, "SIDEWAYS", null, null, null, null),
                null));
    }

    @Test
    void anOutOfRangeLimitIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(0, null, null, null, null, null, null), null));
        assertThrows(IllegalArgumentException.class, () -> responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(5000, null, null, null, null, null, null), null));
    }

    @Test
    void aPillFilterMatchesAnySelectedValue() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(25, null, null, null, "FIRST", null, null), null);
        assertEquals(1, res.getBody().get("page").get("total").asLong());
        assertEquals("ALPHA", res.getBody().get("items").get(0).get("name").asText());
    }

    @Test
    void aColumnFilterNarrowsTheWholeResultSet() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("0:ZETA|MIKE"), null), null);
        assertEquals(2, res.getBody().get("page").get("total").asLong());
    }

    @Test
    void twoColumnFiltersAreAnded() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("0:ZETA|MIKE", "1:FIRST"), null), null);
        assertEquals(0, res.getBody().get("page").get("total").asLong());
    }

    @Test
    void anUndeclaredFilterColumnIsRejectedRatherThanIgnored() {
        // Ignoring it would return rows the user asked to exclude while the filter icon is lit.
        assertThrows(IllegalArgumentException.class, () -> responder.respond(unsorted(), null, SPEC,
                new FacetPageResponder.Params(25, null, null, null, null, List.of("9:X"), null),
                null));
    }

    @Test
    void theSummaryServesTheCountsAndTheFilterValueLists() {
        ResponseEntity<JsonNode> res = responder.summary(unsorted(), null, SPEC, null);
        JsonNode body = res.getBody();
        assertEquals(3, body.get("total").asLong());
        assertEquals(3, body.get("scopedTotal").asLong());
        assertEquals(List.of("FIRST", "OTHER"), toList(body.get("types")));
        assertEquals(List.of("ALPHA", "MIKE", "ZETA"), toList(body.get("columnValues").get("0")));
    }

    @Test
    void theSummaryAppliesTheScopeToTheScopedTotalOnly() {
        ResponseEntity<JsonNode> res = responder.summary(unsorted(), null, SPEC,
                row -> row.path("name").asText("").startsWith("A"));
        assertEquals(3, res.getBody().get("total").asLong(), "total is the unscoped size");
        assertEquals(1, res.getBody().get("scopedTotal").asLong());
        assertEquals(List.of("ALPHA"), toList(res.getBody().get("columnValues").get("0")),
                "a popover offers the values inside the current scope, as it did before");
    }

    // ==================== text-mode column filters (contains/regex) ====================

    @Test
    void aContainsFilterIsCaseSensitiveByDefault() {
        // "ETA" (uppercase) matches ZETA; case-sensitive is the default absent an explicit ":ci",
        // so a lowercase needle would not (see the next test).
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, TEXT_SPEC,
                new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("2:contains:ETA"), null), null);
        assertEquals(1, res.getBody().get("page").get("total").asLong());
        assertEquals("ZETA", res.getBody().get("items").get(0).get("name").asText());
    }

    @Test
    void aContainsFilterCanBeMadeCaseInsensitive() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, TEXT_SPEC,
                new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("2:contains:zeta:ci"), null), null);
        assertEquals(1, res.getBody().get("page").get("total").asLong());
        assertEquals("ZETA", res.getBody().get("items").get(0).get("name").asText());
    }

    @Test
    void aContainsFilterWithWrongCaseMatchesNothingWithoutCi() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, TEXT_SPEC,
                new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("2:contains:zeta"), null), null);
        assertEquals(0, res.getBody().get("page").get("total").asLong());
    }

    @Test
    void aRegexFilterMatchesAcrossTheWholeResultSet() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, TEXT_SPEC,
                new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("2:regex:^(ZETA|MIKE)$"), null), null);
        assertEquals(2, res.getBody().get("page").get("total").asLong());
    }

    @Test
    void aRegexFilterCanBeMadeCaseInsensitiveToo() {
        ResponseEntity<JsonNode> res = responder.respond(unsorted(), null, TEXT_SPEC,
                new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("2:regex:^zeta$:ci"), null), null);
        assertEquals(1, res.getBody().get("page").get("total").asLong());
    }

    @Test
    void anInvalidRegexIsRejectedCleanlyRatherThanFailingOrHanging() {
        assertThrows(IllegalArgumentException.class, () -> responder.respond(unsorted(), null,
                TEXT_SPEC, new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("2:regex:("), null), null));
    }

    @Test
    void anOversizedTextFilterIsRejected() {
        String tooLong = "a".repeat(300);
        assertThrows(IllegalArgumentException.class, () -> responder.respond(unsorted(), null,
                TEXT_SPEC, new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("2:contains:" + tooLong), null), null));
    }

    @Test
    void aMalformedTextTokenIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> responder.respond(unsorted(), null,
                TEXT_SPEC, new FacetPageResponder.Params(25, null, null, null, null,
                        List.of("2:notamode:ZETA"), null), null));
    }

    @Test
    void declaringTheSameColumnAsBothCheckboxAndTextIsRejectedAtConstruction() {
        assertThrows(IllegalStateException.class, () -> new FacetSpec(
                SPEC.label(), SPEC.sorts(), SPEC.defaultSort(), SPEC.defaultLimit(), SPEC.id(),
                SPEC.searchText(), SPEC.typeValues(), SPEC.procedureIds(), SPEC.filterColumns(),
                Map.of("0", n -> n.path("name").asText(""))));
    }

    private static List<String> toList(JsonNode arr) {
        List<String> out = new java.util.ArrayList<>();
        arr.forEach(n -> out.add(n.asText()));
        return out;
    }
}
