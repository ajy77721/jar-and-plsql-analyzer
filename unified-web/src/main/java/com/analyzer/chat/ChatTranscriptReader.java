package com.analyzer.chat;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageInfo;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.JsonArrayRows;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;

/**
 * A bounded, backward-paged window onto a chat transcript — Q67 / L30.
 *
 * <p><b>The problem.</b> Opening a chat session fetched the whole session document, transcript and
 * all, to render it. A transcript only grows: every message ever sent, plus every reply Claude
 * produced, in one array in one file. It is not polled, so the frequency is low — but it is a
 * genuinely unbounded read, and the one that gets worse the more the feature is used.
 *
 * <p><b>The shape.</b> The same shape the log tail already uses, because chat history is the same
 * kind of data: append-only, addressed by a monotonic {@code seq}. A message's {@code seq} is its
 * 1-based position, which is a legitimate stable key here precisely <em>because</em> the array is
 * append-only — a message is never inserted or reordered, so a seq once issued keeps meaning the
 * same message. (That is not true of the analysis artifacts, where re-running renumbers everything,
 * which is why those cursors carry natural keys instead.)
 *
 * <p><b>Backward, not forward.</b> A reader opens a chat at the <em>end</em> and scrolls up, so the
 * useful page is "the newest {@code limit} messages" and the useful cursor is "older than this one"
 * — {@code before}, which PLAN-08 §3.2 defines as {@code after} under {@code sort=SEQ_DESC}. Items
 * come back in that declared descending order rather than in reading order, because a page whose
 * items are not in the order the envelope names is a trap for the next caller.
 *
 * <p><b>Streamed.</b> The transcript is read with a {@link com.fasterxml.jackson.core.JsonParser}
 * one message at a time and selected into a deque of at most {@code limit + 1}, so the memory cost
 * is a page rather than a transcript. Deserialising the session to page it would have read the
 * whole thing into the heap to return fifty messages, which is the cost this is here to remove.
 */
final class ChatTranscriptReader {

    /** The one sort this endpoint offers. A cursor from any other ordering is rejected. */
    static final String SORT_SEQ_DESC = "SEQ_DESC";

    /** Messages per page when the caller does not say. */
    static final int DEFAULT_LIMIT = 50;

    private final ObjectMapper mapper;

    ChatTranscriptReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param open     opens the session document; called once
     * @param before   return only messages with {@code seq} strictly below this, or null for the
     *                 newest page
     * @param limit    maximum messages, clamped to {@link PageRequest#MAX_LIMIT}
     * @param query    case-insensitive substring the content or role must contain, or null. Search
     *                 belongs here rather than in the browser: with one page in memory a
     *                 client-side search would report matches from that page and call it the total.
     */
    Page<JsonNode> read(DocumentSource open, Long before, Integer limit, String query)
            throws IOException {
        PageRequest req = PageRequest.sinceSeq(before, limit, SORT_SEQ_DESC, DEFAULT_LIMIT);
        int max = req.limit();
        String needle = (query == null || query.isBlank())
                ? null : query.toLowerCase(Locale.ROOT);

        // Newest-first selection over a forward scan: keep the last `max + 1` matches below the
        // cursor. The extra one is how `hasMore` is known without a second pass.
        Deque<JsonNode> window = new ArrayDeque<>(max + 2);
        long matched = 0;
        long seq = 0;

        try (InputStream in = open.open();
             var parser = mapper.getFactory().createParser(in);
             var rows = JsonArrayRows.of(parser, mapper, "messages", null)) {
            while (rows.hasNext()) {
                JsonNode raw = rows.next();
                seq++;
                if (needle != null) {
                    String text = (raw.path("content").asText("") + ' '
                            + raw.path("role").asText("")).toLowerCase(Locale.ROOT);
                    if (!text.contains(needle)) continue;
                }
                matched++;
                if (before != null && seq >= before) continue;
                var out = ((com.fasterxml.jackson.databind.node.ObjectNode) raw).deepCopy();
                // Written into the row, not just used as a cursor: the client needs it to ask for
                // the next window, and a client that derived it from a rendered position would be
                // wrong the moment a filter was applied.
                out.put("seq", seq);
                window.addLast(out);
                if (window.size() > max + 1) window.removeFirst();
            }
        }

        boolean hasMore = window.size() > max;
        if (hasMore) window.removeFirst();

        // Reversed into the declared SEQ_DESC order; the scan collected them ascending.
        List<JsonNode> items = new ArrayList<>(window);
        java.util.Collections.reverse(items);

        if (items.isEmpty()) {
            return new Page<>(List.of(),
                    new PageInfo(null, false, matched, true, max, SORT_SEQ_DESC));
        }
        long oldest = items.get(items.size() - 1).path("seq").asLong();
        return hasMore
                ? Page.more(items, Cursor.ofSeq(SORT_SEQ_DESC, oldest), SORT_SEQ_DESC, max,
                            matched, true)
                : new Page<>(items, new PageInfo(null, false, matched, true, max, SORT_SEQ_DESC));
    }

    /** Opens the session document. A supplier that can throw, which {@link java.util.function.Supplier} cannot. */
    @FunctionalInterface
    interface DocumentSource {
        InputStream open() throws IOException;
    }
}
