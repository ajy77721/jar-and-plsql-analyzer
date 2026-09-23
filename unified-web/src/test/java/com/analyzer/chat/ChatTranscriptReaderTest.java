package com.analyzer.chat;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Backward paging over a chat transcript — Q67 / L30, step 14 of PLAN-08 §4.3.
 *
 * <p>The failure that matters is a message appearing twice or vanishing as the reader scrolls up,
 * so the central test walks the whole chain from the newest page to the oldest and checks that the
 * union is exactly the transcript, in order, once each.
 */
class ChatTranscriptReaderTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final ChatTranscriptReader READER = new ChatTranscriptReader(M);

    /** A session document of the shape ChatSession serialises to. */
    private static String session(int messages) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"id\":\"s1\",\"name\":\"Session\",\"scope\":\"GLOBAL\",")
          .append("\"chatType\":\"classic\",\"messages\":[");
        for (int i = 1; i <= messages; i++) {
            if (i > 1) sb.append(',');
            sb.append("{\"role\":\"").append(i % 2 == 1 ? "user" : "assistant")
              .append("\",\"content\":\"message ").append(i)
              .append("\",\"timestamp\":\"2026-09-01T00:00:0")
              .append(i % 10).append("Z\"}");
        }
        return sb.append("]}").toString();
    }

    private static Page<JsonNode> read(String doc, Long before, Integer limit, String q)
            throws Exception {
        return READER.read(
                () -> new ByteArrayInputStream(doc.getBytes(StandardCharsets.UTF_8)),
                before, limit, q);
    }

    private static List<Long> seqs(Page<JsonNode> p) {
        List<Long> out = new ArrayList<>();
        p.items().forEach(i -> out.add(i.path("seq").asLong()));
        return out;
    }

    @Test
    @DisplayName("the first page is the newest messages, in the declared SEQ_DESC order")
    void newestFirst() throws Exception {
        Page<JsonNode> p = read(session(10), null, 3, null);
        assertEquals(List.of(10L, 9L, 8L), seqs(p),
                "items must be in the order the envelope names, not reading order");
        assertEquals("SEQ_DESC", p.page().sort());
        assertTrue(p.page().hasMore());
        assertEquals(10, p.page().total());
        assertTrue(p.page().totalIsExact());
    }

    @Test
    @DisplayName("`before` returns the messages immediately older, exclusive of the cursor")
    void beforeIsExclusive() throws Exception {
        Page<JsonNode> p = read(session(10), 8L, 3, null);
        assertEquals(List.of(7L, 6L, 5L), seqs(p));
    }

    @Test
    @DisplayName("scrolling the whole way up yields every message exactly once")
    void fullChain() throws Exception {
        String doc = session(47);
        Set<Long> seen = new LinkedHashSet<>();
        Long before = null;
        long previousLowest = Long.MAX_VALUE;
        for (int guard = 0; guard < 100; guard++) {
            Page<JsonNode> p = read(doc, before, 5, null);
            assertEquals(47, p.page().total(), "total must be the transcript, not the page");
            for (Long s : seqs(p)) {
                assertTrue(seen.add(s), "message " + s + " returned twice");
                assertTrue(s < previousLowest, "pages must strictly descend");
            }
            if (!p.page().hasMore()) {
                assertNull(p.page().nextCursor(), "the last page must carry no cursor");
                break;
            }
            previousLowest = seqs(p).get(seqs(p).size() - 1);
            // The cursor is decoded the way the endpoint does, from the encoded form — not
            // reconstructed from the rows, which is what a client must never do.
            before = Cursor.decode(p.page().nextCursor(), "SEQ_DESC").seq();
        }
        assertEquals(47, seen.size(), "the chain lost messages");
        for (long i = 1; i <= 47; i++) assertTrue(seen.contains(i), "missing message " + i);
    }

    @Test
    @DisplayName("the last page reports hasMore=false and no cursor")
    void lastPage() throws Exception {
        Page<JsonNode> p = read(session(3), null, 10, null);
        assertEquals(List.of(3L, 2L, 1L), seqs(p));
        assertFalse(p.page().hasMore());
        assertNull(p.page().nextCursor());
    }

    @Test
    @DisplayName("an empty transcript is an empty page, not an error")
    void emptyTranscript() throws Exception {
        Page<JsonNode> p = read(session(0), null, 10, null);
        assertTrue(p.items().isEmpty());
        assertEquals(0, p.page().total());
        assertFalse(p.page().hasMore());
    }

    @Test
    @DisplayName("search runs over the whole transcript, and total is the match count")
    void searchIsServerSide() throws Exception {
        // "message 1" also matches 10..19, which is the point: the count must be the real one.
        Page<JsonNode> p = read(session(30), null, 5, "message 1");
        assertEquals(11, p.page().total(), "1 and 10-19 all contain 'message 1'");
        assertEquals(5, p.items().size());
        assertTrue(p.page().hasMore());
        // The newest matches, not the oldest — the same rule the log tail uses.
        assertEquals(List.of(19L, 18L, 17L, 16L, 15L), seqs(p));
    }

    @Test
    @DisplayName("search paging is consistent with search filtering — no match repeats or is lost")
    void searchChain() throws Exception {
        String doc = session(30);
        Set<Long> seen = new LinkedHashSet<>();
        Long before = null;
        for (int guard = 0; guard < 50; guard++) {
            Page<JsonNode> p = read(doc, before, 3, "message 1");
            for (Long s : seqs(p)) assertTrue(seen.add(s), "match " + s + " returned twice");
            if (!p.page().hasMore()) break;
            before = Cursor.decode(p.page().nextCursor(), "SEQ_DESC").seq();
        }
        assertEquals(11, seen.size());
    }

    @Test
    @DisplayName("search matches the role as well as the content")
    void searchMatchesRole() throws Exception {
        assertEquals(5, read(session(10), null, 50, "assistant").page().total());
        assertEquals(5, read(session(10), null, 50, "user").page().total());
    }

    @Test
    @DisplayName("seq is written into each message, so the client never derives it from a position")
    void seqIsOnTheRow() throws Exception {
        Page<JsonNode> p = read(session(5), null, 2, null);
        assertEquals(5, p.items().get(0).path("seq").asInt());
        assertEquals("message 5", p.items().get(0).path("content").asText());
        assertTrue(p.items().get(0).has("role"));
        assertTrue(p.items().get(0).has("timestamp"));
    }

    @Test
    @DisplayName("a session document with no messages array pages empty rather than throwing")
    void noMessagesField() throws Exception {
        Page<JsonNode> p = read("{\"id\":\"s1\",\"name\":\"x\"}", null, 10, null);
        assertTrue(p.items().isEmpty());
        assertEquals(0, p.page().total());
    }

    @Test
    @DisplayName("a `before` older than every message returns an empty page, not the newest one")
    void beforeBelowFloor() throws Exception {
        Page<JsonNode> p = read(session(10), 1L, 5, null);
        assertTrue(p.items().isEmpty(), "nothing is older than message 1");
        assertFalse(p.page().hasMore());
    }
}
