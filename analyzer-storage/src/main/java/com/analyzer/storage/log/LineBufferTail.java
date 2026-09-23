package com.analyzer.storage.log;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * The {@code seq}-cursor read for an append-only stream that lives in memory rather than in a file.
 *
 * <p>{@link LogTailReader} answers the same contract for a log on disk, where the sequence number
 * is the line's position in the file. Several streams in this application are instead a bounded
 * in-process list — the conversion job's progress log and its live Claude output, the flow run's
 * log — and a poller wants exactly the same thing from them: "what has arrived since {@code seq}".
 * This is that read, so those endpoints do not each invent their own cursor semantics.
 *
 * <p><b>Why {@code totalAppended} and not {@code list.size()}.</b> The buffers are rolling: the
 * conversion job keeps the last 2 000 live lines and drops the oldest. If {@code seq} were the
 * index into the surviving list, every eviction would silently renumber every line the client
 * already holds, and the client would append lines it had already rendered. The sequence number is
 * therefore the count of lines ever appended, which only ever increases, and eviction shows up
 * honestly as {@link LogTail#fromSeq()} moving forward — with {@link LogTail#reset()} set for the
 * one client whose cursor fell off the bottom.
 */
public final class LineBufferTail {

    /** The only sort an append-only stream has. */
    public static final String SORT_SEQ_ASC = "SEQ_ASC";

    private LineBufferTail() {
    }

    /**
     * Reads a page of a rolling in-memory line buffer.
     *
     * @param lines         the surviving lines, oldest first. A snapshot: the caller must not
     *                      mutate it while this runs.
     * @param totalAppended lines ever appended to the stream, i.e. the sequence number of the last
     *                      element of {@code lines}. Must be {@code >= lines.size()}.
     * @param sinceSeq      resume strictly after this sequence number. {@code null} means the tail
     *                      — the last {@code limit} matching lines, which is what a first paint
     *                      wants and what the unpaged response gave.
     * @param limit         maximum lines to return, defaulted and clamped by {@link PageRequest}
     * @param level         exact level to keep (case-insensitive), or null for all. The classifier
     *                      supplies the level, so this filter runs here instead of in the browser.
     * @param query         case-insensitive substring the line must contain, or null
     * @param classifier    derives a level from a line. Applied only to lines that survive the
     *                      {@code seq} window, so a filtered poll does not classify the whole
     *                      buffer.
     */
    public static LogTail of(List<String> lines, long totalAppended, Long sinceSeq, Integer limit,
                             String level, String query, Function<String, String> classifier) {

        PageRequest req = PageRequest.sinceSeq(sinceSeq, limit, SORT_SEQ_ASC, 200);
        int max = req.limit();

        int size = lines.size();
        // Defensive: a caller that snapshots the list and the counter separately could observe a
        // count behind the list. Trusting it would make `floor` negative and the seq numbers wrong,
        // so the list wins — it is the thing whose contents are actually being served.
        long watermark = Math.max(totalAppended, size);
        // seq of lines.get(0); 0 when the buffer is empty, so "nothing retained" is not seq 1.
        long floor = size == 0 ? 0 : watermark - size + 1;

        if (size == 0) {
            // A cursor beyond the (empty) stream is a reset: the client holds lines that no longer
            // exist, which is not the same thing as "nothing new".
            boolean stale = sinceSeq != null && sinceSeq > watermark;
            return new LogTail(Page.empty(SORT_SEQ_ASC, max), watermark, stale, floor, 0);
        }

        // The caller's cursor points past the end of the stream — the buffer was replaced (a new
        // job reusing the id, or a restart). Never answer this with an empty page, which the client
        // would read as "you are up to date".
        if (sinceSeq != null && sinceSeq > watermark) {
            return new LogTail(Page.empty(SORT_SEQ_ASC, max), watermark, true, floor, 0);
        }

        // The next line the caller wants has already been evicted from the rolling buffer. Tell it
        // to clear its buffer and re-render from `floor`, rather than appending new lines onto a
        // stale prefix and showing a log that never existed.
        boolean reset = sinceSeq != null && sinceSeq + 1 < floor;

        long from;              // first seq to consider, inclusive
        boolean keepLast;       // keep the newest matches rather than the oldest
        if (sinceSeq == null || reset) {
            from = floor;
            keepLast = sinceSeq == null;
        } else {
            from = sinceSeq + 1;
            keepLast = false;
        }

        String needle = (query == null || query.isBlank()) ? null : query.toLowerCase(Locale.ROOT);
        String want = (level == null || level.isBlank())
                ? null : level.trim().toUpperCase(Locale.ROOT);

        List<LogLine> hits = new ArrayList<>();
        int startIdx = (int) Math.max(0, from - floor);
        for (int i = startIdx; i < size; i++) {
            String line = lines.get(i);
            if (line == null) line = "";
            if (needle != null && !line.toLowerCase(Locale.ROOT).contains(needle)) continue;
            String lvl = classifier == null ? "" : classifier.apply(line);
            if (lvl == null) lvl = "";
            if (want != null && !want.equals(lvl)) continue;
            hits.add(new LogLine(floor + i, "", lvl, line));
            // Forward paging stops one past the limit so `hasMore` is known without a second pass;
            // the tail keeps scanning because it wants the newest matches, not the oldest.
            if (!keepLast && hits.size() > max) break;
        }

        boolean hasMore;
        if (keepLast) {
            hasMore = false;    // the tail is, by definition, the end of the stream
            if (hits.size() > max) hits = new ArrayList<>(hits.subList(hits.size() - max, hits.size()));
        } else {
            hasMore = hits.size() > max;
            if (hasMore) hits = new ArrayList<>(hits.subList(0, max));
        }

        Page<LogLine> page = hasMore
                ? Page.more(hits, Cursor.ofSeq(SORT_SEQ_ASC, hits.get(hits.size() - 1).seq()),
                            SORT_SEQ_ASC, max, -1, false)
                : Page.last(hits, SORT_SEQ_ASC, max);

        return new LogTail(page, watermark, reset, floor, 0);
    }
}
