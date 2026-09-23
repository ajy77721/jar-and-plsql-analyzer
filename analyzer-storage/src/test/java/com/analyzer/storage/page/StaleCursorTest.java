package com.analyzer.storage.page;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-019 / MID-020 — a cursor is only valid within the run and the ordering that minted it.
 *
 * <h2>The failure this prevents</h2>
 *
 * <p>Page 1 is served from run v1. A re-analysis publishes v2. Page 2 arrives carrying a cursor
 * minted against v1 and is answered from v2's rows. Nothing errors: the caller receives a page that
 * looks ordinary and either repeats rows or skips them at the seam, depending on which way the data
 * moved. A re-analysis between two page requests is all it takes, and the analyser is run often.
 *
 * <p>"Cannot be shown to be safe" is treated as unsafe: a cursor carrying no run at all is rejected
 * when a run is expected, because it cannot be shown to belong to the current one.
 */
class StaleCursorTest {

    @Test
    void aCursorFromASupersededRunIsRejectedAndNamesBothRuns() {
        Cursor minted = Cursor.of("USAGES_DESC", List.of("42"), "APP.ORDERS", "ORACLE~DEMO~v1");
        Cursor decoded = Cursor.decode(minted.encode(), "USAGES_DESC");

        var e = assertThrows(Cursor.InvalidCursorException.class,
                () -> decoded.requireRun("ORACLE~DEMO~v2"),
                "a cursor from a superseded run was accepted; paging would mix two runs");
        assertTrue(e.getMessage().contains("ORACLE~DEMO~v1"), "the error does not name the stale run");
        assertTrue(e.getMessage().contains("ORACLE~DEMO~v2"),
                "the error does not name the run that is now current");
    }

    @Test
    void aCursorFromTheSameRunIsAccepted() {
        // The other direction. A check that rejects everything is not a check.
        Cursor decoded = Cursor.decode(
                Cursor.of("NAME_ASC", List.of("ORDERS"), "APP.ORDERS", "ORACLE~DEMO~v1").encode(),
                "NAME_ASC");
        assertDoesNotThrow(() -> decoded.requireRun("ORACLE~DEMO~v1"));
    }

    @Test
    void theRunSurvivesTheEncodeDecodeRoundTrip() {
        // The field is only useful if it is actually carried. An encoder that dropped it would make
        // every cursor look like a pre-runId one, which the next assertion shows is rejected —
        // failing closed, but for the wrong reason and on every request.
        Cursor minted = Cursor.of("NAME_ASC", List.of("a", "b"), "id-1", "RUN~7");
        Cursor decoded = Cursor.decode(minted.encode(), "NAME_ASC");
        assertEquals("RUN~7", decoded.runId());
        assertEquals(List.of("a", "b"), decoded.keys());
        assertEquals("id-1", decoded.tiebreaker());
    }

    @Test
    void aCursorWithNoRunIsRejectedWhenARunIsExpected() {
        // Cursors minted before this field existed. They cannot be shown to belong to the current
        // run, and "unknown" must not be treated as "fine".
        Cursor legacy = Cursor.decode(
                Cursor.of("NAME_ASC", List.of("x"), "id-1").encode(), "NAME_ASC");
        assertNull(legacy.runId());
        assertThrows(Cursor.InvalidCursorException.class, () -> legacy.requireRun("RUN~7"),
                "a cursor with no run was accepted against a real run");
    }

    @Test
    void aStreamWithNoRunIsUnaffected() {
        // The append-only logs genuinely have no run. Requiring one there would break tailing.
        Cursor logCursor = Cursor.decode(Cursor.ofSeq("SEQ", 1234).encode(), "SEQ");
        assertNull(logCursor.runId());
        assertDoesNotThrow(() -> logCursor.requireRun(null));
        assertEquals(1234, logCursor.seq());
    }

    @Test
    void aCursorIsStillRejectedAcrossOrderings() {
        // MID-020's other half, kept alongside because the two are one guarantee: a cursor is a row
        // comparison, and a comparison only means something within the ordering it was made in.
        Cursor minted = Cursor.of("NAME_ASC", List.of("ORDERS"), "APP.ORDERS", "RUN~1");
        assertThrows(Cursor.InvalidCursorException.class,
                () -> Cursor.decode(minted.encode(), "USAGES_DESC"),
                "a cursor minted under one sort was accepted for another");
    }

    @Test
    void theTiebreakerIsCarriedSoCollidingSortKeysStayOrdered() {
        // MID-020. Sort keys collide in this data — 63 ORACLE / 81 POSTGRES / 64 DB2 statements
        // share a line — so the sort tuple alone cannot identify the last row returned. Without a
        // stable tiebreaker the next page restarts inside the collision group and rows repeat.
        Cursor a = Cursor.of("LINE_ASC", List.of("100"), "stmt-A", "RUN~1");
        Cursor b = Cursor.of("LINE_ASC", List.of("100"), "stmt-B", "RUN~1");
        assertNotEquals(a.encode(), b.encode(),
                "two rows with identical sort keys encode to the same cursor, so a page boundary "
                        + "inside a collision group cannot be resumed correctly");
        assertEquals("stmt-B", Cursor.decode(b.encode(), "LINE_ASC").tiebreaker());
    }
}
