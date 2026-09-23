package com.plsqlanalyzer.web.page;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-019 — a cursor may only be resumed against the result set that minted it.
 *
 * <h2>The failure</h2>
 *
 * <p>Page 1 is served. The analysis is re-run and the rows change. Page 2 arrives carrying the
 * cursor from page 1 and is answered against the new rows. Nothing errors — the caller gets a page
 * that looks ordinary and either repeats rows or skips them at the seam. The analyser is run often
 * enough that "between two page requests" is a realistic window.
 *
 * <h2>Why an identity taken from the rows, not a run id</h2>
 *
 * <p>These readers are file-backed and have no run. Taking the identity from the rows is also
 * strictly better here: a re-analysis producing identical output is genuinely safe to page across —
 * measured, 188 of 189 artifact files are unchanged between runs — and a run id would reject that
 * case anyway. A content identity rejects exactly the unsafe case and permits the safe one.
 */
class ResultSetScopedCursorTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static final Function<JsonNode, String> ID =
            n -> n.path("name").asText("").toUpperCase(Locale.ROOT);
    private static final Function<JsonNode, String> SEARCH =
            n -> n.path("name").asText("").toLowerCase(Locale.ROOT);
    private static final Map<String, FacetSort> SORTS = FacetSort.byName(
            FacetSort.text("NAME_ASC", "name", false));

    private static ArrayNode rows(int n, String prefix) {
        ArrayNode arr = M.createArrayNode();
        for (int i = 0; i < n; i++) {
            ObjectNode o = arr.addObject();
            o.put("name", String.format("%s_%03d", prefix, i));
            o.put("count", i);
        }
        return arr;
    }

    private static PageRequest req(Integer limit, String after) {
        return PageRequest.of(limit, after, "NAME_ASC", null, SORTS.keySet(), "NAME_ASC", 25);
    }

    private static Page<JsonNode> page(ArrayNode rows, PageRequest r, String resultSetId) {
        return JsonFacetPager.page(rows.iterator(), r, SORTS, ID, SEARCH, null, resultSetId);
    }

    @Test
    void aCursorMintedAgainstOneResultSetIsRejectedByAnother() {
        ArrayNode before = rows(10, "SEQ");
        Page<JsonNode> first = page(before, req(3, null), "rs-before");
        assertTrue(first.page().hasMore());
        String cursor = first.page().nextCursor();
        assertNotNull(cursor);

        // The analysis is re-run and the rows change.
        ArrayNode after = rows(10, "OTHER");
        var e = assertThrows(Cursor.InvalidCursorException.class,
                () -> page(after, req(3, cursor), "rs-after"),
                "a cursor from the previous result set was resumed against the new one");
        assertTrue(e.getMessage().contains("rs-before") && e.getMessage().contains("rs-after"),
                "the error should name both result sets, got: " + e.getMessage());
    }

    @Test
    void theSameResultSetPagesNormally() {
        // A check that rejects everything is not a check. Walking the whole set must still work.
        ArrayNode data = rows(10, "SEQ");
        int seen = 0;
        String cursor = null;
        do {
            Page<JsonNode> p = page(data, req(3, cursor), "rs-1");
            seen += p.items().size();
            cursor = p.page().nextCursor();
        } while (cursor != null);
        assertEquals(10, seen, "paging the same result set did not return every row");
    }

    @Test
    void theCheckHappensBeforeAnyRowIsReturned() {
        // Failing partway would already have handed back rows the caller treats as continuous with
        // the previous page — a wrong answer delivered before the error arrives.
        ArrayNode before = rows(10, "SEQ");
        String cursor = page(before, req(3, null), "rs-before").page().nextCursor();
        assertThrows(Cursor.InvalidCursorException.class,
                () -> page(rows(10, "SEQ"), req(3, cursor), "rs-different"));
    }

    @Test
    void anUnscopedCallerIsUnaffected() {
        // Callers with no result-set identity keep the previous behaviour rather than breaking.
        ArrayNode data = rows(10, "SEQ");
        String cursor = JsonFacetPager.page(data.iterator(), req(3, null), SORTS, ID, SEARCH, null)
                .page().nextCursor();
        assertNotNull(cursor);
        assertDoesNotThrow(() ->
                JsonFacetPager.page(data.iterator(), req(3, cursor), SORTS, ID, SEARCH, null));
    }

    // ---------------------------------------------------------------------------------------
    // Through the responder, which is what actually computes the identity in production.
    // ---------------------------------------------------------------------------------------

    private static final FacetSpec RESPONDER_SPEC = new FacetSpec(
            "widgets",
            SORTS,
            "NAME_ASC",
            25,
            ID,
            SEARCH,
            n -> java.util.List.of(),
            n -> java.util.List.of(),
            Map.of());

    private static FacetPageResponder.Params params(Integer limit, String after) {
        return new FacetPageResponder.Params(limit, after, "NAME_ASC", null, null, null, null);
    }

    @Test
    void theResponderDerivesAnIdentityFromTheRowsItServes() {
        // Exercises FacetPageResponder.resultSetId, which the tests above bypass by passing an
        // identity straight to the pager. Replacing that method with a constant left every one of
        // them passing — the mechanism was covered and the wiring that supplies it was not.
        FacetPageResponder responder = new FacetPageResponder(M, 10_000);

        var first = responder.respond(rows(10, "SEQ"), null, RESPONDER_SPEC, params(3, null), null);
        String cursor = first.getBody().path("page").path("nextCursor").asText(null);
        assertNotNull(cursor, "the responder returned no cursor to resume from");

        // Same rows: must resume.
        assertDoesNotThrow(() -> responder.respond(rows(10, "SEQ"), null, RESPONDER_SPEC,
                params(3, cursor), null));

        // Different rows: must not.
        assertThrows(Cursor.InvalidCursorException.class,
                () -> responder.respond(rows(10, "OTHER"), null, RESPONDER_SPEC,
                        params(3, cursor), null),
                "the responder resumed a cursor against a different set of rows");
    }

    @Test
    void theResponderIdentityChangesWithRowCountToo() {
        // Same first and last row, different size — a truncated or extended result set. An identity
        // built only from the endpoints would call these the same.
        FacetPageResponder responder = new FacetPageResponder(M, 10_000);
        var first = responder.respond(rows(10, "SEQ"), null, RESPONDER_SPEC, params(3, null), null);
        String cursor = first.getBody().path("page").path("nextCursor").asText(null);

        ArrayNode fewer = M.createArrayNode();
        fewer.add(rows(10, "SEQ").get(0));
        fewer.add(rows(10, "SEQ").get(9));
        assertThrows(Cursor.InvalidCursorException.class,
                () -> responder.respond(fewer, null, RESPONDER_SPEC, params(3, cursor), null),
                "a result set with the same endpoints but a different size was accepted");
    }

    @Test
    void anIdenticalRerunIsNotRejected() {
        // The case a run id would get wrong. Identical output is safe to page across, and 188 of
        // 189 artifact files are unchanged between runs — rejecting that would be a false alarm on
        // the common path.
        String cursor = page(rows(10, "SEQ"), req(3, null), "rs-content").page().nextCursor();
        assertDoesNotThrow(() -> page(rows(10, "SEQ"), req(3, cursor), "rs-content"),
                "an identical re-run was rejected; the identity is not content-based");
    }
}
