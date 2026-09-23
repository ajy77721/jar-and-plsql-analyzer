package com.analyzer.storage.log;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Serves a bounded, filtered, cursor-addressed tail of a local log file.
 *
 * <p><b>Why this exists.</b> The three log viewers and the shared log FAB all fetched
 * {@code ?lines=10000} every 3–10 s while their Auto box was ticked — the single largest response
 * in the application, measured against source logs of 9.36 MB and 4.09 MB — and then filtered by
 * level and text <em>in the browser</em>, re-running the filter over the whole buffer on every
 * keystroke. Every tick re-sent the entire window the client already had.
 *
 * <p><b>The design.</b> Lines are addressed by a 1-based ordinal, {@code seq}, which is a legitimate
 * monotonic key because a log is append-only. A client that has seen {@code seq} asks for
 * {@code sinceSeq=seq} and gets only what arrived since — normally nothing, occasionally a handful
 * of lines. Level and text filters are applied here, so a filtered view costs one request rather
 * than a full refetch plus a client-side sweep.
 *
 * <p><b>Why an index.</b> Answering {@code sinceSeq} means finding the byte offset of a line
 * number, and re-reading 9 MB to serve three lines would defeat the whole point. So byte offsets
 * are recorded every {@link #CHECKPOINT_STRIDE} lines and the index is extended incrementally over
 * the bytes appended since the last call — the steady-state cost of a poll is proportional to what
 * was actually logged. The first call pays one linear scan; at {@code CHECKPOINT_STRIDE} the index
 * for a 100 000-line log is about 200 longs, so it does not grow into a second problem.
 *
 * <p><b>Rotation.</b> Detected by the file's identity key changing or its length shrinking, either
 * of which invalidates every existing {@code seq}. The reader restarts its numbering and reports
 * {@link LogTail#reset()}, and the client clears its buffer. A poller that quietly appended
 * post-rotation lines to pre-rotation ones would render a log that never existed, so the reset is
 * explicit rather than best-effort.
 *
 * <p>Lives in {@code analyzer-storage} because it is seekable local-file access, which is what this
 * module is the boundary for. It is deliberately <em>not</em> built on {@code ArtifactStore}: the
 * live log is the one file that must keep working when the store is unreachable, since otherwise a
 * storage outage takes the diagnostics for that outage with it.
 */
public final class LogTailReader {

    /** Lines between recorded byte offsets. Small enough to seek near, few enough to keep. */
    static final int CHECKPOINT_STRIDE = 512;

    /**
     * How far back an unanchored (first-paint) request looks for matching lines.
     *
     * <p>A filtered tail cannot know how far back the last {@code limit} matches are without
     * looking, and "look until you find them" over an append-only file that grows forever is an
     * unbounded read waiting to happen. Bounded instead: a filter that matches nothing in the last
     * 20 000 lines returns nothing, which is both cheap and what the old client-side filter did to
     * its 10 000-line buffer anyway.
     */
    static final int TAIL_SCAN_LINES = 20_000;

    /** The {@code seq} sort. A cursor minted under it is rejected by any other ordering. */
    public static final String SORT_SEQ_ASC = "SEQ_ASC";

    /** {@code %d{yyyy-MM-dd HH:mm:ss.SSS} %5level} — the pattern logback-spring.xml declares. */
    private static final Pattern LINE_HEAD = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s+"
            + "(ERROR|WARN|INFO|DEBUG|TRACE)\\b");

    private final Path file;

    /** Byte offset of the first byte of line {@code i * CHECKPOINT_STRIDE + 1}. */
    private final List<Long> checkpoints = new ArrayList<>();

    private long indexedBytes;      // bytes consumed, always ending just after a newline
    private long lineCount;         // complete lines indexed
    private Object fileKey;         // inode-equivalent, for rotation detection
    private boolean rotatedSinceStart;

    public LogTailReader(Path file) {
        this.file = Objects.requireNonNull(file, "file");
    }

    /**
     * Reads a page of the log.
     *
     * @param sinceSeq resume strictly after this line; {@code null} means the tail, i.e. the last
     *                 {@code limit} matching lines — which is what {@code ?lines=N} did, preserved
     *                 so the first paint of a viewer is unchanged
     * @param limit    maximum lines to return, clamped by {@link PageRequest#MAX_LIMIT}
     * @param level    exact level to keep, or null for all
     * @param query    case-insensitive substring the line must contain, or null
     */
    public synchronized LogTail read(Long sinceSeq, Integer limit, String level, String query)
            throws IOException {

        PageRequest req = PageRequest.sinceSeq(sinceSeq, limit, SORT_SEQ_ASC, 200);
        int max = req.limit();

        if (!Files.exists(file)) {
            return new LogTail(Page.empty(SORT_SEQ_ASC, max), 0, false, 0, 0);
        }

        long size = refreshIndex();
        long watermark = lineCount;
        long floor = lineCount == 0 ? 0 : 1;

        // The caller's cursor points past the end of the log: it was rotated or truncated under
        // them. Say so, rather than returning an empty page they would read as "nothing new".
        if (sinceSeq != null && sinceSeq > watermark) {
            return new LogTail(Page.empty(SORT_SEQ_ASC, max), watermark, true, floor, size);
        }

        long from;      // first seq to consider, inclusive
        boolean keepLast;
        if (sinceSeq == null) {
            from = Math.max(1, lineCount - TAIL_SCAN_LINES + 1);
            keepLast = true;    // the newest matches, not the oldest in the scanned window
        } else {
            from = sinceSeq + 1;
            keepLast = false;
        }

        List<LogLine> hits = scanFrom(from, level, query, keepLast ? TAIL_SCAN_LINES : max + 1);

        boolean hasMore;
        if (keepLast) {
            hasMore = false;    // the tail is by definition the end of the log
            if (hits.size() > max) hits = new ArrayList<>(hits.subList(hits.size() - max, hits.size()));
        } else {
            hasMore = hits.size() > max;
            if (hasMore) hits = new ArrayList<>(hits.subList(0, max));
        }

        Page<LogLine> page = hasMore
                ? Page.more(hits, Cursor.ofSeq(SORT_SEQ_ASC, hits.get(hits.size() - 1).seq()),
                            SORT_SEQ_ASC, max, -1, false)
                : Page.last(hits, SORT_SEQ_ASC, max);

        boolean reset = sinceSeq != null && rotatedSinceStart;
        rotatedSinceStart = false;
        return new LogTail(page, watermark, reset, floor, size);
    }

    /** Total complete lines currently in the log. Exposed for the legacy caption. */
    public synchronized long lineCount() throws IOException {
        if (!Files.exists(file)) return 0;
        refreshIndex();
        return lineCount;
    }

    /**
     * Reads forward from line {@code from}, keeping matching lines until {@code want} of them are
     * collected.
     *
     * <p>The seek lands on the checkpoint at or before {@code from}, so the skipped prefix is never
     * read, and at most {@link #CHECKPOINT_STRIDE} lines before {@code from} are decoded and
     * discarded. Streamed rather than slurped: a client that has fallen a hundred thousand lines
     * behind must not turn its catch-up request into a hundred-megabyte allocation.
     */
    private List<LogLine> scanFrom(long from, String level, String query, int want)
            throws IOException {
        if (lineCount == 0 || from > lineCount) return new ArrayList<>();

        int checkpoint = (int) ((from - 1) / CHECKPOINT_STRIDE);
        if (checkpoint >= checkpoints.size()) checkpoint = checkpoints.size() - 1;
        long offset = checkpoints.get(checkpoint);
        long seq0 = (long) checkpoint * CHECKPOINT_STRIDE;   // seq of the line before `offset`

        List<LogLine> out = new ArrayList<>();
        String needle = (query == null || query.isBlank()) ? null : query.toLowerCase(Locale.ROOT);
        String levelNeedle = (level == null || level.isBlank())
                ? null : " " + level.trim().toUpperCase(Locale.ROOT) + " ";

        try (InputStream raw = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            skipFully(raw, offset);
            // Capped at indexedBytes, so a line still being written is never half-served and a
            // rotation mid-read cannot hand back bytes from two different generations.
            try (BufferedReader r = new BufferedReader(new InputStreamReader(
                    new BoundedInputStream(raw, indexedBytes - offset), StandardCharsets.UTF_8),
                    1 << 16)) {
                long seq = seq0;
                String line;
                while ((line = r.readLine()) != null) {
                    seq++;
                    if (seq < from) continue;
                    if (levelNeedle != null && !line.contains(levelNeedle)) continue;
                    if (needle != null && !line.toLowerCase(Locale.ROOT).contains(needle)) continue;

                    String ts = "", lvl = "";
                    Matcher m = LINE_HEAD.matcher(line);
                    if (m.find()) { ts = m.group(1); lvl = m.group(2); }
                    out.add(new LogLine(seq, ts, lvl, line));
                    if (out.size() >= want) break;
                }
            }
        }
        return out;
    }

    /**
     * Extends the line index over whatever has been appended since the last call, and restarts it
     * if the file was rotated.
     *
     * @return the current file size
     */
    private long refreshIndex() throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
        long size = attrs.size();
        Object key = attrs.fileKey();

        boolean rotated = (fileKey != null && !Objects.equals(key, fileKey)) || size < indexedBytes;
        if (rotated || checkpoints.isEmpty()) {
            if (rotated) rotatedSinceStart = true;
            checkpoints.clear();
            checkpoints.add(0L);
            indexedBytes = 0;
            lineCount = 0;
        }
        fileKey = key;
        if (size <= indexedBytes) return size;

        try (InputStream raw = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            skipFully(raw, indexedBytes);
            try (BufferedInputStream in = new BufferedInputStream(raw, 1 << 16)) {
                long at = indexedBytes;
                long lines = lineCount;
                int b;
                while (at < size && (b = in.read()) >= 0) {
                    at++;
                    if (b != '\n') continue;
                    lines++;
                    // Offsets are recorded, not lines, so the index stays ~200 longs for a
                    // 100 000-line log while still landing the seek within one stride.
                    if (lines % CHECKPOINT_STRIDE == 0) checkpoints.add(at);
                    indexedBytes = at;
                    lineCount = lines;
                }
            }
        }
        return size;
    }

    /**
     * Stops the reader at a byte ceiling.
     *
     * <p>Needed because the live log is being appended to while it is read: without a ceiling, a
     * {@code readLine()} can consume a line the indexer has not yet counted, and the sequence
     * numbers on the wire would then disagree with the watermark reported beside them.
     */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream in;
        private long left;

        BoundedInputStream(InputStream in, long limit) {
            this.in = in;
            this.left = Math.max(0, limit);
        }

        @Override public int read() throws IOException {
            if (left <= 0) return -1;
            int b = in.read();
            if (b >= 0) left--;
            return b;
        }

        @Override public int read(byte[] buf, int off, int len) throws IOException {
            if (left <= 0) return -1;
            int n = in.read(buf, off, (int) Math.min(len, left));
            if (n > 0) left -= n;
            return n;
        }
    }

    private static void skipFully(InputStream in, long n) throws IOException {
        long left = n;
        while (left > 0) {
            long skipped = in.skip(left);
            if (skipped <= 0) {
                if (in.read() < 0) return;
                left--;
            } else {
                left -= skipped;
            }
        }
    }
}
