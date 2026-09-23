package com.analyzer.storage.page;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The paging contract, which the whole UI migration depends on being exactly right.
 *
 * <p>The failure mode these tests exist to prevent is not a crash — it is a silently wrong page.
 * A cursor accepted under the wrong sort, or a stale cursor read as if it were current, returns
 * plausible-looking rows from the wrong position, and nobody notices until data is missing from a
 * report.
 */
class PagingContractTest {

    private static final Set<String> SORTS = Set.of("SIDEBAR", "NAME_ASC", "SEQ_DESC");

    @Nested
    @DisplayName("cursor")
    class Cursors {

        @Test
        @DisplayName("round-trips the sort, keys and tiebreaker")
        void roundTrips() {
            Cursor c = Cursor.of("SIDEBAR", List.of("CORRECTED", "2026-08-12T10:11:09Z"), "f3a9");
            Cursor back = Cursor.decode(c.encode(), "SIDEBAR");
            assertEquals("SIDEBAR", back.sort());
            assertEquals(List.of("CORRECTED", "2026-08-12T10:11:09Z"), back.keys());
            assertEquals("f3a9", back.tiebreaker());
            assertEquals(Cursor.CURRENT_VERSION, back.version());
        }

        @Test
        @DisplayName("a cursor from another sort is REJECTED, never silently answered")
        void wrongSortIsRejected() {
            // The whole point of the discriminator: the same key tuple means a different position
            // under a different ordering, so answering it would return the wrong rows.
            String encoded = Cursor.of("NAME_ASC", List.of("alpha"), "id1").encode();
            var e = assertThrows(Cursor.InvalidCursorException.class,
                    () -> Cursor.decode(encoded, "SIDEBAR"));
            assertTrue(e.getMessage().contains("NAME_ASC"), e.getMessage());
        }

        @Test
        @DisplayName("a cursor from an older deploy is rejected cleanly")
        void staleVersionIsRejected() {
            String encoded = new Cursor("SIDEBAR", List.of("x"), "id1", null, 0).encode();
            assertThrows(Cursor.InvalidCursorException.class,
                    () -> Cursor.decode(encoded, "SIDEBAR"));
        }

        @Test
        @DisplayName("malformed input throws rather than producing a partial cursor")
        void malformedIsRejected() {
            assertThrows(Cursor.InvalidCursorException.class,
                    () -> Cursor.decode("!!!not-base64!!!", "SIDEBAR"));
            assertThrows(Cursor.InvalidCursorException.class,
                    () -> Cursor.decode(java.util.Base64.getUrlEncoder().withoutPadding()
                            .encodeToString("{}".getBytes()), "SIDEBAR"));
        }

        @Test
        @DisplayName("keys containing quotes and backslashes survive encoding")
        void escapingIsCorrect() {
            // Analysis names and SQL fragments genuinely contain both.
            Cursor c = Cursor.of("NAME_ASC", List.of("a\"b", "c\\d", "e\"\\f"), "id\"1");
            Cursor back = Cursor.decode(c.encode(), "NAME_ASC");
            assertEquals(List.of("a\"b", "c\\d", "e\"\\f"), back.keys());
            assertEquals("id\"1", back.tiebreaker());
        }

        @Test
        @DisplayName("whitespace around the colon does not break decoding")
        void whitespaceTolerant() {
            // The hand-rolled parser matched the literal `"s":"`, so `"s": "SIDEBAR"` made every
            // field invisible — and an unreadable `v` reads as version 0, i.e. the error said
            // "minted by a different version" about a perfectly well-formed cursor. Any future
            // pretty-printer or a cursor built by another tool would have hit this.
            String spaced = "{\"s\": \"SIDEBAR\", \"k\": [\"a\", \"b\"], \"i\": \"id1\", \"v\": 1}";
            String encoded = java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(spaced.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            Cursor c = Cursor.decode(encoded, "SIDEBAR");
            assertEquals("SIDEBAR", c.sort());
            assertEquals(List.of("a", "b"), c.keys());
            assertEquals("id1", c.tiebreaker());
            assertEquals(1, c.version());
        }

        @Test
        @DisplayName("a seq cursor carries the sequence number")
        void seqCursor() {
            Cursor c = Cursor.ofSeq("SEQ_DESC", 4711);
            assertEquals(4711, Cursor.decode(c.encode(), "SEQ_DESC").seq());
        }

        @Test
        @DisplayName("the encoding is URL-safe and unpadded")
        void encodingIsUrlSafe() {
            String enc = Cursor.of("SIDEBAR", List.of("a".repeat(40)), "id").encode();
            assertFalse(enc.contains("+") || enc.contains("/") || enc.contains("="),
                    "a cursor travels in a query string: " + enc);
        }
    }

    @Nested
    @DisplayName("page envelope")
    class Envelope {

        @Test
        @DisplayName("hasMore and nextCursor cannot disagree")
        void consistency() {
            assertThrows(IllegalArgumentException.class,
                    () -> new PageInfo("abc", false, 1, true, 25, "SIDEBAR"),
                    "a last page must not carry a cursor");
            assertThrows(IllegalArgumentException.class,
                    () -> new PageInfo(null, true, 1, true, 25, "SIDEBAR"),
                    "hasMore with no cursor leaves the client unable to continue");
        }

        @Test
        @DisplayName("an unknown total is reported as such, not estimated")
        void unknownTotalIsHonest() {
            PageInfo p = PageInfo.unknownTotal("c", true, 25, "SIDEBAR");
            assertEquals(-1, p.total());
            assertFalse(p.totalIsExact(), "a UI must be able to tell it cannot render a page count");
        }

        @Test
        @DisplayName("a full result set can be served as one page with the same contract")
        void singlePage() {
            Page<String> p = Page.single(List.of("a", "b"), "NAME_ASC");
            assertFalse(p.hasMore());
            assertNull(p.page().nextCursor());
            assertEquals(2, p.page().total());
            assertTrue(p.page().totalIsExact());
        }

        @Test
        @DisplayName("map keeps the paging information intact")
        void mapPreservesPaging() {
            Page<Integer> src = Page.more(List.of(1, 2), Cursor.of("NAME_ASC", List.of("2"), "2"),
                    "NAME_ASC", 2, 99, true);
            Page<String> out = src.map(String::valueOf);
            assertEquals(List.of("1", "2"), out.items());
            assertEquals(src.page(), out.page());
        }

        @Test
        @DisplayName("items are defensively copied")
        void itemsAreImmutable() {
            var mutable = new java.util.ArrayList<>(List.of("a"));
            Page<String> p = Page.single(mutable, "NAME_ASC");
            mutable.add("b");
            assertEquals(1, p.items().size());
            assertThrows(UnsupportedOperationException.class, () -> p.items().add("c"));
        }
    }

    @Nested
    @DisplayName("request parsing")
    class Requests {

        @Test
        @DisplayName("no parameters means legacy: today's bare array")
        void absentParamsMeanLegacy() {
            PageRequest r = PageRequest.of(null, null, null, null, SORTS, "SIDEBAR", 25);
            assertTrue(r.isLegacy(),
                    "this is what lets one endpoint convert without breaking its callers");
            assertEquals("SIDEBAR", r.sort());
        }

        @Test
        @DisplayName("any paging parameter opts in")
        void anyParamOptsIn() {
            assertFalse(PageRequest.of(10, null, null, null, SORTS, "SIDEBAR", 25).isLegacy());
            assertFalse(PageRequest.of(null, null, "NAME_ASC", null, SORTS, "SIDEBAR", 25).isLegacy());
        }

        @Test
        @DisplayName("an unknown sort is rejected — the set is closed")
        void unknownSortRejected() {
            // A free-form sort name would be both an injection surface and a way to request an
            // unindexed ordering, i.e. a full scan on a hot path.
            assertThrows(IllegalArgumentException.class,
                    () -> PageRequest.of(10, null, "DROP TABLE", null, SORTS, "SIDEBAR", 25));
        }

        @Test
        @DisplayName("limit is bounded on both sides")
        void limitBounded() {
            assertThrows(IllegalArgumentException.class,
                    () -> PageRequest.of(0, null, null, null, SORTS, "SIDEBAR", 25));
            assertThrows(IllegalArgumentException.class,
                    () -> PageRequest.of(PageRequest.MAX_LIMIT + 1, null, null, null, SORTS,
                            "SIDEBAR", 25));
            assertEquals(PageRequest.MAX_LIMIT,
                    PageRequest.of(PageRequest.MAX_LIMIT, null, null, null, SORTS, "SIDEBAR", 25)
                            .limit());
        }

        @Test
        @DisplayName("fetchSize is limit+1, to detect a further page in one read")
        void fetchSizeIsLimitPlusOne() {
            assertEquals(26, PageRequest.of(25, null, null, null, SORTS, "SIDEBAR", 25).fetchSize());
        }

        @Test
        @DisplayName("a cursor is validated against the requested sort at parse time")
        void cursorValidatedAgainstSort() {
            String wrongSort = Cursor.of("NAME_ASC", List.of("a"), "id").encode();
            assertThrows(Cursor.InvalidCursorException.class,
                    () -> PageRequest.of(25, wrongSort, "SIDEBAR", null, SORTS, "SIDEBAR", 25));
        }

        @Test
        @DisplayName("search text is carried server-side, blank treated as absent")
        void searchIsCarried() {
            // Search must move with pagination: with one page in memory, a client-side filter
            // would silently search that page only.
            assertEquals("orders", PageRequest.of(25, null, null, "orders", SORTS, "SIDEBAR", 25)
                    .query());
            assertFalse(PageRequest.of(25, null, null, "   ", SORTS, "SIDEBAR", 25).hasQuery());
        }
    }
}
