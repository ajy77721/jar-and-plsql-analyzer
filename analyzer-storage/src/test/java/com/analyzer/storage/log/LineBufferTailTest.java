package com.analyzer.storage.log;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The conversion job's live output was the largest per-tick payload in the application, re-sent in
 * full every 1.5 s. Replacing it with a {@code seq} delta is only safe if three properties hold,
 * and each is a specific way this can silently corrupt a screen rather than fail.
 *
 * <p><b>A delta must be exactly the delta.</b> If the sequence number counted rows in the response
 * instead of lines in the stream, the client's next cursor would skip or repeat lines — and because
 * the client <em>appends</em>, a repeat is invisible in the response and visible only as duplicated
 * output on screen.
 *
 * <p><b>Eviction must not renumber.</b> The buffer is rolling. If {@code seq} were the index into
 * the surviving list, every eviction would shift the meaning of every cursor the client holds, and
 * a poll would re-append lines already rendered. This is the reason the counter is "lines ever
 * appended" and not {@code size()}.
 *
 * <p><b>Falling off the bottom must be loud.</b> A client whose cursor has been evicted must be
 * told to clear its buffer. Answering with "here is what is newest" and no flag produces a log that
 * never existed: new lines spliced onto a stale prefix, with the gap in the middle invisible.
 */
class LineBufferTailTest {

    /** Stands in for the conversion controller's classifier; only the routing matters here. */
    private static final Function<String, String> LEVEL = line ->
            line.startsWith("!") ? "ERROR" : line.startsWith("?") ? "WARN" : "";

    private static List<String> lines(int from, int to) {
        return IntStream.rangeClosed(from, to).mapToObj(i -> "line " + i).toList();
    }

    private static List<String> messages(LogTail tail) {
        List<String> out = new ArrayList<>();
        tail.page().items().forEach(l -> out.add(l.message()));
        return out;
    }

    @Test
    @DisplayName("no sinceSeq returns the tail, so a first paint shows what it showed before")
    void absentCursorReturnsTheTail() {
        LogTail t = LineBufferTail.of(lines(1, 50), 50, null, 10, null, null, LEVEL);

        assertEquals(10, t.page().items().size());
        assertEquals("line 41", t.page().items().get(0).message());
        assertEquals("line 50", t.page().items().get(9).message());
        assertEquals(50, t.watermark());
        assertFalse(t.page().hasMore(), "the tail is the end of the stream by definition");
        assertFalse(t.reset());
    }

    @Test
    @DisplayName("sinceSeq at the watermark returns nothing — the steady state this exists for")
    void caughtUpPollReturnsNothing() {
        LogTail t = LineBufferTail.of(lines(1, 50), 50, 50L, 200, null, null, LEVEL);

        assertTrue(t.page().items().isEmpty());
        assertEquals(50, t.watermark());
        assertFalse(t.reset(), "up to date is not a reset");
    }

    @Test
    @DisplayName("a delta is exactly the lines appended after the cursor, numbered by the stream")
    void deltaIsExact() {
        LogTail t = LineBufferTail.of(lines(1, 50), 50, 47L, 200, null, null, LEVEL);

        assertEquals(List.of("line 48", "line 49", "line 50"), messages(t));
        assertEquals(48, t.page().items().get(0).seq(), "seq counts the stream, not the response");
        assertEquals(50, t.page().items().get(2).seq());
    }

    @Test
    @DisplayName("eviction moves fromSeq forward and does not renumber the surviving lines")
    void evictionDoesNotRenumber() {
        // 2000 lines appended, only the last 500 retained: seqs 1501..2000.
        LogTail t = LineBufferTail.of(lines(1501, 2000), 2000, 1998L, 200, null, null, LEVEL);

        assertEquals(2000, t.watermark());
        assertEquals(1501, t.fromSeq(), "the retained floor, not 1");
        assertEquals(List.of("line 1999", "line 2000"), messages(t));
        assertEquals(1999, t.page().items().get(0).seq());
        assertFalse(t.reset(), "this cursor is still inside the buffer");
    }

    @Test
    @DisplayName("a cursor that fell off the bottom is a reset, refilled from the retained floor")
    void evictedCursorIsAReset() {
        LogTail t = LineBufferTail.of(lines(1501, 2000), 2000, 900L, 5, null, null, LEVEL);

        assertTrue(t.reset(), "the client must clear its buffer, not append onto a stale prefix");
        assertEquals(1501, t.fromSeq());
        assertEquals(1501, t.page().items().get(0).seq(), "refills from the floor, oldest first");
        assertTrue(t.page().hasMore(), "there is more than one page between the floor and now");
    }

    @Test
    @DisplayName("a cursor past the watermark is a reset, never a silent empty page")
    void cursorPastTheEndIsAReset() {
        LogTail t = LineBufferTail.of(lines(1, 10), 10, 99L, 200, null, null, LEVEL);

        assertTrue(t.reset());
        assertTrue(t.page().items().isEmpty());
        assertEquals(10, t.watermark());
    }

    @Test
    @DisplayName("an empty stream is empty, and a cursor into it is a reset")
    void emptyStream() {
        LogTail fresh = LineBufferTail.of(List.of(), 0, null, 200, null, null, LEVEL);
        assertTrue(fresh.page().items().isEmpty());
        assertEquals(0, fresh.watermark());
        assertFalse(fresh.reset(), "nothing has happened yet — that is not a reset");

        LogTail stale = LineBufferTail.of(List.of(), 0, 5L, 200, null, null, LEVEL);
        assertTrue(stale.reset(), "the client holds lines the stream does not have");
    }

    @Test
    @DisplayName("level and q filter server-side, and the cursor still counts unfiltered lines")
    void filtersRunOnTheServer() {
        List<String> buf = List.of("plain 1", "!boom 2", "plain 3", "?careful 4", "!boom 5");

        LogTail errors = LineBufferTail.of(buf, 5, null, 200, "error", null, LEVEL);
        assertEquals(List.of("!boom 2", "!boom 5"), messages(errors));
        assertEquals(2, errors.page().items().get(0).seq(), "seq is the position in the stream");
        assertEquals(5, errors.page().items().get(1).seq());

        LogTail q = LineBufferTail.of(buf, 5, null, 200, null, "CAREFUL", LEVEL);
        assertEquals(List.of("?careful 4"), messages(q), "q is case-insensitive");

        LogTail both = LineBufferTail.of(buf, 5, null, 200, "WARN", "plain", LEVEL);
        assertTrue(both.page().items().isEmpty(), "the filters are an AND, not an OR");
    }

    @Test
    @DisplayName("a client far behind is paged forward, and the cursor chain loses no line")
    void pagingForwardLosesNothing() {
        List<String> buf = lines(1, 1000);
        List<String> seen = new ArrayList<>();
        Long cursor = 0L;
        int guard = 0;
        while (guard++ < 50) {
            LogTail t = LineBufferTail.of(buf, 1000, cursor, 100, null, null, LEVEL);
            seen.addAll(messages(t));
            if (!t.page().hasMore()) break;
            cursor = t.page().items().get(t.page().items().size() - 1).seq();
        }
        assertEquals(1000, seen.size(), "every line, exactly once");
        assertEquals(buf, seen);
    }

    @Test
    @DisplayName("the total is honest: counting a stream means reading it, so it is not claimed")
    void totalIsNotFabricated() {
        LogTail t = LineBufferTail.of(lines(1, 1000), 1000, 0L, 10, null, null, LEVEL);

        assertTrue(t.page().hasMore());
        assertEquals(-1, t.page().page().total());
        assertFalse(t.page().page().totalIsExact());
        assertEquals(1000, t.watermark(), "watermark is the honest substitute for a total");
    }
}
