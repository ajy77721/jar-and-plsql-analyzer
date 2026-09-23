package com.analyzer.storage.log;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The log tail is the largest response in the application, and the one that runs on the fastest
 * timer. These tests pin the three properties that make replacing it safe.
 *
 * <p><b>Same lines.</b> The server-side {@code level} and {@code q} filters must select exactly the
 * lines the browser used to select for itself, or "convert the log viewer" quietly becomes "change
 * what the log viewer shows".
 *
 * <p><b>A delta is a delta.</b> A poll with {@code sinceSeq} at the watermark must return nothing,
 * which is the steady state the whole change exists to make cheap.
 *
 * <p><b>Rotation is loud.</b> The failure mode of a naive tail is to append post-rotation lines
 * onto pre-rotation ones and render a log that never existed. That must be a reported reset.
 */
class LogTailReaderTest {

    @TempDir
    Path dir;

    private Path write(String... lines) throws IOException {
        Path f = dir.resolve("unified-analyzer.log");
        Files.writeString(f, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        return f;
    }

    private void append(Path f, String... lines) throws IOException {
        Files.writeString(f, String.join("\n", lines) + "\n", StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);
    }

    private static String line(String level, String msg) {
        return "2026-09-01 10:11:09.123 " + String.format("%5s", level) + " [main] c.a.Thing - " + msg;
    }

    @Test
    @DisplayName("no parameters returns the tail, newest last, exactly as ?lines=N did")
    void unanchoredReadReturnsTheTail() throws IOException {
        Path f = write(IntStream.rangeClosed(1, 50).mapToObj(i -> line("INFO", "m" + i))
                                .toArray(String[]::new));
        LogTail tail = new LogTailReader(f).read(null, 10, null, null);

        List<LogLine> items = tail.page().items();
        assertEquals(10, items.size());
        assertEquals(41, items.get(0).seq(), "the tail starts 10 lines from the end");
        assertEquals(50, items.get(9).seq());
        assertTrue(items.get(9).message().endsWith("m50"));
        assertEquals(50, tail.watermark());
        assertFalse(tail.page().hasMore(), "the tail is the end of the log");
    }

    @Test
    @DisplayName("sinceSeq at the watermark returns nothing — the steady state of a poll")
    void deltaAtTheWatermarkIsEmpty() throws IOException {
        Path f = write(line("INFO", "a"), line("INFO", "b"));
        LogTailReader r = new LogTailReader(f);

        LogTail first = r.read(null, 200, null, null);
        assertEquals(2, first.watermark());

        LogTail nothing = r.read(first.watermark(), 200, null, null);
        assertTrue(nothing.page().items().isEmpty());
        assertEquals(2, nothing.watermark());
        assertFalse(nothing.reset());
    }

    @Test
    @DisplayName("sinceSeq returns only what was appended after it")
    void deltaReturnsOnlyNewLines() throws IOException {
        Path f = write(line("INFO", "a"), line("INFO", "b"));
        LogTailReader r = new LogTailReader(f);
        long seen = r.read(null, 200, null, null).watermark();

        append(f, line("WARN", "c"), line("INFO", "d"));
        LogTail delta = r.read(seen, 200, null, null);

        assertEquals(List.of(3L, 4L), delta.page().items().stream().map(LogLine::seq).toList());
        assertEquals(4, delta.watermark());
        assertEquals("WARN", delta.page().items().get(0).level());
    }

    @Test
    @DisplayName("level and text filters select the same lines the browser used to select")
    void serverSideFilterMatchesTheClientSidePredicate() throws IOException {
        String[] raw = {
                line("INFO", "starting up"),
                line("WARN", "connection timeout on orders"),
                line("ERROR", "boom"),
                line("INFO", "timeout mentioned but INFO"),
                line("WARN", "unrelated"),
        };
        Path f = write(raw);
        LogTailReader r = new LogTailReader(f);

        assertEquals(clientSide(raw, "WARN", null),
                     messages(r.read(null, 200, "WARN", null)),
                     "level filter");
        assertEquals(clientSide(raw, null, "timeout"),
                     messages(r.read(null, 200, null, "timeout")),
                     "text filter, case-insensitive");
        assertEquals(clientSide(raw, "WARN", "timeout"),
                     messages(r.read(null, 200, "WARN", "timeout")),
                     "both together");
        assertEquals(clientSide(raw, "ERROR", "no such text"),
                     messages(r.read(null, 200, "ERROR", "no such text")),
                     "a filter that matches nothing returns nothing, not everything");
    }

    @Test
    @DisplayName("a filtered delta still numbers by line, not by match")
    void filteredDeltaKeepsAbsoluteSequenceNumbers() throws IOException {
        Path f = write(line("INFO", "a"), line("WARN", "b"), line("INFO", "c"), line("WARN", "d"));
        LogTail warns = new LogTailReader(f).read(0L, 200, "WARN", null);

        assertEquals(List.of(2L, 4L), warns.page().items().stream().map(LogLine::seq).toList(),
                "seq is the position in the log; filtering must not renumber it, or the client's "
                + "next sinceSeq would skip lines");
        assertEquals(4, warns.watermark(), "the watermark counts lines, not matches");
    }

    @Test
    @DisplayName("truncation is reported as a reset, never as a silent gap")
    void truncationReportsReset() throws IOException {
        Path f = write(IntStream.rangeClosed(1, 30).mapToObj(i -> line("INFO", "m" + i))
                                .toArray(String[]::new));
        LogTailReader r = new LogTailReader(f);
        long seen = r.read(null, 200, null, null).watermark();
        assertEquals(30, seen);

        // Rotation, as logback performs it: the live file starts again from nothing.
        Files.writeString(f, line("INFO", "fresh") + "\n", StandardCharsets.UTF_8);

        LogTail after = r.read(seen, 200, null, null);
        assertTrue(after.reset(), "the client must clear its buffer rather than append onto stale lines");
        assertEquals(1, after.watermark());
        assertEquals(1, after.fromSeq(), "and restart from the retained floor");
    }

    @Test
    @DisplayName("a cursor beyond the end of the log is a reset, not an empty page")
    void cursorPastTheEndIsAReset() throws IOException {
        Path f = write(line("INFO", "a"));
        LogTail t = new LogTailReader(f).read(999L, 200, null, null);

        assertTrue(t.reset(), "an empty page here would read as 'nothing new', which is a lie");
        assertTrue(t.page().items().isEmpty());
    }

    @Test
    @DisplayName("a delta larger than the limit pages, with a cursor")
    void deltaLargerThanTheLimitPages() throws IOException {
        Path f = write(IntStream.rangeClosed(1, 100).mapToObj(i -> line("INFO", "m" + i))
                                .toArray(String[]::new));
        LogTail t = new LogTailReader(f).read(0L, 25, null, null);

        assertEquals(25, t.page().items().size());
        assertTrue(t.page().hasMore());
        assertNotNull(t.page().page().nextCursor());
        assertEquals(100, t.watermark(), "the watermark tells the client how far behind it is");
    }

    @Test
    @DisplayName("reads across a checkpoint boundary, so the byte index is actually correct")
    void readsAcrossCheckpointBoundaries() throws IOException {
        int n = LogTailReader.CHECKPOINT_STRIDE * 3 + 7;
        Path f = write(IntStream.rangeClosed(1, n).mapToObj(i -> line("INFO", "m" + i))
                                .toArray(String[]::new));
        LogTailReader r = new LogTailReader(f);

        // Straddles the second recorded offset: the seek lands before it, the scan must skip
        // forward to exactly the right line.
        long since = LogTailReader.CHECKPOINT_STRIDE * 2L + 3;
        LogTail t = r.read(since, 5, null, null);

        assertEquals(since + 1, t.page().items().get(0).seq());
        assertTrue(t.page().items().get(0).message().endsWith("m" + (since + 1)));
        assertEquals(n, t.watermark());
    }

    @Test
    @DisplayName("a missing log file is empty, not an error — it does not exist before first write")
    void missingFileIsEmpty() throws IOException {
        LogTail t = new LogTailReader(dir.resolve("never-written.log")).read(null, 200, null, null);
        assertTrue(t.page().items().isEmpty());
        assertEquals(0, t.watermark());
        assertEquals(0, t.fileSize());
    }

    @Test
    @DisplayName("a partially written final line is not served until its newline arrives")
    void partialLineIsNotServed() throws IOException {
        Path f = write(line("INFO", "complete"));
        Files.writeString(f, line("INFO", "half-writt"), StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);

        LogTail t = new LogTailReader(f).read(null, 200, null, null);
        assertEquals(1, t.watermark(), "an unterminated line has no seq yet");
        assertEquals(1, t.page().items().size());
        assertTrue(t.page().items().get(0).message().endsWith("complete"));
    }

    private static List<String> messages(LogTail t) {
        return t.page().items().stream().map(LogLine::message).toList();
    }

    /** The predicate the log viewers ran in the browser, verbatim. */
    private static List<String> clientSide(String[] lines, String level, String query) {
        String lvl = level == null ? null : " " + level + " ";
        String q = query == null ? null : query.toLowerCase();
        return java.util.Arrays.stream(lines)
                .filter(l -> lvl == null || l.contains(lvl))
                .filter(l -> q == null || l.toLowerCase().contains(q))
                .toList();
    }
}
