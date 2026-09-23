package com.plsqlanalyzer.web.page;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The properties a paged table depends on, tested where they can actually be checked.
 *
 * <p>The interesting failures of cursor paging are not "does page 1 render" — they are a row that
 * appears twice, or vanishes, at a boundary, under a sort the test fixture happens not to exercise.
 * So most of these tests walk the whole result set page by page and compare it against the same
 * data sorted in one go.
 */
class JsonFacetPagerTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static final Function<JsonNode, String> ID =
            n -> n.path("name").asText("").toUpperCase(Locale.ROOT);
    private static final Function<JsonNode, String> SEARCH =
            n -> n.path("name").asText("").toLowerCase(Locale.ROOT);

    private static final Map<String, FacetSort> SORTS = FacetSort.byName(
            FacetSort.text("NAME_ASC", "name", false),
            FacetSort.text("NAME_DESC", "name", true),
            FacetSort.number("COUNT_ASC", "count", false),
            FacetSort.number("COUNT_DESC", "count", true));

    /** Rows with deliberately colliding counts, so the tiebreaker is exercised. */
    private static ArrayNode rows(int n) {
        ArrayNode arr = M.createArrayNode();
        for (int i = 0; i < n; i++) {
            ObjectNode o = arr.addObject();
            o.put("name", String.format("SEQ_%03d", i));
            o.put("count", i % 7);
        }
        return arr;
    }

    private static PageRequest req(Integer limit, String after, String sort, String q) {
        return PageRequest.of(limit, after, sort, q, SORTS.keySet(), "NAME_ASC", 25);
    }

    private static Page<JsonNode> page(ArrayNode rows, PageRequest r) {
        return JsonFacetPager.page(rows.iterator(), r, SORTS, ID, SEARCH, null);
    }

    /** Walks every page and returns the concatenated ids, plus how many requests it took. */
    private static List<String> walk(ArrayNode rows, String sort, int limit, String q) {
        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int guard = 0; guard < 1000; guard++) {
            Page<JsonNode> p = page(rows, req(limit, cursor, sort, q));
            p.items().forEach(n -> seen.add(ID.apply(n)));
            if (!p.hasMore()) return seen;
            cursor = p.page().nextCursor();
            assertNotNull(cursor, "hasMore=true must carry a cursor");
        }
        return fail("paging did not terminate");
    }

    @Test
    void pagingCoversEveryRowExactlyOnce() {
        ArrayNode rows = rows(103);
        for (String sort : SORTS.keySet()) {
            for (int limit : new int[]{1, 7, 25, 102, 103, 104}) {
                List<String> seen = walk(rows, sort, limit, null);
                assertEquals(103, seen.size(),
                        "sort=" + sort + " limit=" + limit + " lost or repeated rows");
                assertEquals(103, seen.stream().distinct().count(),
                        "sort=" + sort + " limit=" + limit + " returned a duplicate");
            }
        }
    }

    @Test
    void pagedOrderEqualsSortingTheWholeSetAtOnce() {
        ArrayNode rows = rows(60);
        for (String sort : SORTS.keySet()) {
            List<JsonNode> all = new ArrayList<>();
            rows.forEach(all::add);
            all.sort(SORTS.get(sort).comparator(ID));
            List<String> expected = all.stream().map(ID).toList();
            assertEquals(expected, walk(rows, sort, 7, null), "sort=" + sort);
        }
    }

    /**
     * The reason numeric keys are encoded rather than compared as text: with plain digits, "10"
     * sorts before "9", and the resume test then skips rows only once the data is big enough.
     */
    @Test
    void numericSortIsNumericNotLexicographic() {
        ArrayNode arr = M.createArrayNode();
        for (int v : new int[]{9, 10, 100, 2, 1000, -5, 0}) {
            ObjectNode o = arr.addObject();
            o.put("name", "N" + v);
            o.put("count", v);
        }
        List<String> asc = walk(arr, "COUNT_ASC", 2, null);
        assertEquals(List.of("N-5", "N0", "N2", "N9", "N10", "N100", "N1000"), asc);
        List<String> desc = walk(arr, "COUNT_DESC", 3, null);
        assertEquals(List.of("N1000", "N100", "N10", "N9", "N2", "N0", "N-5"), desc);
    }

    @Test
    void totalIsTheFilteredCountAndDoesNotShrinkWhilePaging() {
        ArrayNode rows = rows(103);
        String cursor = null;
        for (int i = 0; i < 5; i++) {
            Page<JsonNode> p = page(rows, req(10, cursor, "NAME_ASC", null));
            assertEquals(103, p.page().total(), "page " + i);
            assertTrue(p.page().totalIsExact());
            cursor = p.page().nextCursor();
        }
    }

    @Test
    void searchFiltersTheWholeSetNotThePage() {
        ArrayNode rows = rows(103);
        // SEQ_007, SEQ_017, … 070-079 → 10 + 9 + 1 within 0..102
        Page<JsonNode> p = page(rows, req(5, null, "NAME_ASC", "seq_07"));
        assertEquals(10, p.page().total(), "seq_07 matches SEQ_070..SEQ_079");
        assertEquals(5, p.items().size());
        assertEquals(10, walk(rows, "NAME_ASC", 5, "seq_07").size());
    }

    @Test
    void searchWithoutASearchFunctionIsRejectedRatherThanIgnored() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> JsonFacetPager.page(rows(5).iterator(), req(5, null, "NAME_ASC", "x"),
                        SORTS, ID, null, null));
        assertTrue(e.getMessage().contains("search"));
    }

    @Test
    void aCursorMintedUnderAnotherSortIsRejected() {
        Page<JsonNode> p = page(rows(50), req(10, null, "NAME_ASC", null));
        String cursor = p.page().nextCursor();
        assertThrows(Cursor.InvalidCursorException.class,
                () -> req(10, cursor, "COUNT_DESC", null),
                "a cursor is only meaningful inside one ordering");
    }

    @Test
    void unknownSortIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> req(10, null, "NOPE", null));
    }

    @Test
    void lastPageCarriesNoCursor() {
        Page<JsonNode> p = page(rows(10), req(50, null, "NAME_ASC", null));
        assertFalse(p.hasMore());
        assertNull(p.page().nextCursor());
        assertEquals(10, p.items().size());
    }

    @Test
    void emptyResultIsAnExactZeroNotAnUnknown() {
        Page<JsonNode> p = page(rows(10), req(5, null, "NAME_ASC", "nothing-matches-this"));
        assertTrue(p.items().isEmpty());
        assertEquals(0, p.page().total());
        assertTrue(p.page().totalIsExact());
        assertFalse(p.hasMore());
    }

    /**
     * Memory is the point of the bounded heap: a page of 10 out of 50 000 rows must not retain
     * anything proportional to the input. Asserted indirectly — the returned page is the only
     * thing held — but the row count is large enough that an accidental full copy would be a
     * visible regression in this test's own runtime.
     */
    @Test
    void largeFacetStillReturnsOnePage() {
        ArrayNode rows = rows(50_000);
        Page<JsonNode> p = page(rows, req(10, null, "COUNT_DESC", null));
        assertEquals(10, p.items().size());
        assertEquals(50_000, p.page().total());
        assertTrue(p.hasMore());
    }
}
