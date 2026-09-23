package com.analyzer.storage.page;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-024 — an over-cap request is refused, never quietly shortened.
 *
 * <h2>Why clamping is the worse failure</h2>
 *
 * <p>A clamped page is indistinguishable from the end of the data: the caller asked for 5,000 rows,
 * received 1,000, and has no field to tell it that 4,000 were withheld. It stops, believing it has
 * everything. An error naming the cap is the only answer that cannot be misread — the caller either
 * lowers the limit or pages, and both are correct.
 */
class BackPressureTest {

    private static final Set<String> SORTS = Set.of("NAME_ASC");

    @Test
    void anOverCapLimitIsRefusedOnThePagedPath() {
        var e = assertThrows(InvalidPageRequestException.class,
                () -> PageRequest.of(PageRequest.MAX_LIMIT + 1, null, "NAME_ASC", null,
                        SORTS, "NAME_ASC", 25));
        assertTrue(e.getMessage().contains(String.valueOf(PageRequest.MAX_LIMIT)),
                "the error does not name the cap: " + e.getMessage());
    }

    @Test
    void anOverCapLimitIsRefusedOnTheStreamPathToo() {
        // This path used to clamp with Math.min(limitParam, MAX_LIMIT) — success, fewer rows, no
        // signal. It is the same rule, so it must fail the same way.
        var e = assertThrows(InvalidPageRequestException.class,
                () -> PageRequest.sinceSeq(0L, PageRequest.MAX_LIMIT + 1, "SEQ", 25));
        assertTrue(e.getMessage().contains(String.valueOf(PageRequest.MAX_LIMIT)),
                "the error does not name the cap: " + e.getMessage());
    }

    @Test
    void theStreamPathDoesNotSilentlyShortenARequest() {
        // Stated as the property rather than as the exception: whatever a caller is given back, it
        // must never be fewer rows than asked for without being told.
        int asked = PageRequest.MAX_LIMIT + 500;
        try {
            PageRequest req = PageRequest.sinceSeq(0L, asked, "SEQ", 25);
            assertEquals(asked, req.limit(),
                    "the request was silently clamped from " + asked + " to " + req.limit()
                            + "; the caller cannot tell that rows were withheld");
        } catch (InvalidPageRequestException expected) {
            // Refusing is the intended behaviour.
        }
    }

    @Test
    void aZeroOrNegativeLimitIsRefusedOnBothPaths() {
        assertThrows(InvalidPageRequestException.class,
                () -> PageRequest.of(0, null, "NAME_ASC", null, SORTS, "NAME_ASC", 25));
        assertThrows(InvalidPageRequestException.class,
                () -> PageRequest.sinceSeq(0L, -1, "SEQ", 25));
    }

    @Test
    void theCapItselfIsAccepted() {
        // Off-by-one in the guard would reject a legal request. The boundary is inclusive.
        assertDoesNotThrow(() -> PageRequest.of(PageRequest.MAX_LIMIT, null, "NAME_ASC", null,
                SORTS, "NAME_ASC", 25));
        assertDoesNotThrow(() -> PageRequest.sinceSeq(0L, PageRequest.MAX_LIMIT, "SEQ", 25));
    }

    @Test
    void anAbsentLimitStillGetsTheEndpointDefault() {
        // Back-pressure must not turn "I did not specify" into an error.
        assertEquals(25, PageRequest.sinceSeq(0L, null, "SEQ", 25).limit());
        assertEquals(25, PageRequest.of(null, null, "NAME_ASC", null, SORTS, "NAME_ASC", 25).limit());
    }

    @Test
    void refusalsAreTypedSoTheyCanBecomeA400() {
        // A bare IllegalArgumentException cannot be mapped to 400 safely — the same type is thrown
        // across the JVM for real internal faults, and translating those would report server bugs
        // as client errors. The narrow type is what makes the advice safe.
        var e = assertThrows(InvalidPageRequestException.class,
                () -> PageRequest.of(999_999, null, "NAME_ASC", null, SORTS, "NAME_ASC", 25));
        assertInstanceOf(IllegalArgumentException.class, e,
                "existing catch(IllegalArgumentException) blocks must keep working");
    }

    @Test
    void anUnknownSortIsAlsoARefusalRatherThanASilentDefault() {
        // Falling back to the default sort would answer a different question than the one asked,
        // and the caller would have no way to notice.
        var e = assertThrows(InvalidPageRequestException.class,
                () -> PageRequest.of(10, null, "NO_SUCH_SORT", null, SORTS, "NAME_ASC", 25));
        assertTrue(e.getMessage().contains("NO_SUCH_SORT"), "the error does not name the bad sort");
    }
}
