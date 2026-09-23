package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Expanding the call-tree DAG must terminate in bounded output, and say when it was cut.
 *
 * <p>The DAG stores one copy of a shared subtree; expanding it writes one copy per path that
 * reaches it, so output is exponential in sharing. That was documented as acceptable — "exactly
 * what the endpoint returned before" — which was true and still hung. Measured in the field on
 * {@code unified-web-1.0.0-SNAPSHOT} endpoint 271: past <b>295 seconds</b> with no response.
 *
 * <p>The chain below is the cheapest possible reproduction. Each level has two children pointing at
 * the same next level, so n levels expand to 2^n nodes: 40 levels is a trillion, which no timeout
 * saves you from. It completes here because the budget stops it.
 */
class CallTreeExpansionBudgetTest {

    private static final ObjectMapper M = new ObjectMapper();

    /** n levels, each with two edges to the same child — 2^n expanded nodes, n stored nodes. */
    private String sharedChain(int levels) throws Exception {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < levels; i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append("n").append(i).append("\":{\"method\":\"m").append(i).append("\",")
              .append("\"childIds\":[\"n").append(i + 1).append("\",\"n").append(i + 1).append("\"]}");
        }
        sb.append(",\"n").append(levels).append("\":{\"method\":\"leaf\",\"childIds\":[]}}");
        return sb.toString();
    }

    private JsonNode expand(String dict, String rootId) throws Exception {
        CallTreeInflater inf = CallTreeInflater.read(
                M.getFactory().createParser(dict).nextToken() == null ? null : reparse(dict), M);
        StringWriter w = new StringWriter();
        try (var gen = M.getFactory().createGenerator(w)) {
            inf.expand(rootId, gen);
        }
        return M.readTree(w.toString());
    }

    private com.fasterxml.jackson.core.JsonParser reparse(String dict) throws Exception {
        var p = M.getFactory().createParser(dict);
        p.nextToken();   // position on START_OBJECT, as streamCallTree does
        return p;
    }

    @Test
    @DisplayName("a heavily shared DAG completes instead of hanging")
    void sharedDagTerminates() throws Exception {
        // 40 levels = 2^40 nodes if expanded naively. This test finishing at all is the assertion.
        long start = System.currentTimeMillis();
        JsonNode out = expand(sharedChain(40), "n0");
        long ms = System.currentTimeMillis() - start;

        assertNotNull(out);
        assertTrue(ms < 30_000, "expansion took " + ms + " ms — the budget is not bounding it");
        assertEquals(countNodes(out) <= CallTreeInflater.MAX_NODES + 1, true,
                "wrote " + countNodes(out) + " nodes, budget is " + CallTreeInflater.MAX_NODES);
    }

    @Test
    @DisplayName("a truncated tree says so, rather than looking complete")
    void truncationIsDisclosed() throws Exception {
        JsonNode out = expand(sharedChain(40), "n0");
        assertTrue(hasTruncationFlag(out),
                "the tree was cut but no node carries childrenTruncated — a shortened call tree "
                        + "that does not admit it is a wrong answer about what the endpoint calls");
    }

    @Test
    @DisplayName("a small tree is untouched — no flags, every child present")
    void smallTreeIsUnchanged() throws Exception {
        String dict = "{\"a\":{\"method\":\"root\",\"childIds\":[\"b\",\"c\"]},"
                    + "\"b\":{\"method\":\"b\",\"childIds\":[]},"
                    + "\"c\":{\"method\":\"c\",\"childIds\":[]}}";
        JsonNode out = expand(dict, "a");
        assertEquals("root", out.get("method").asText());
        assertEquals(2, out.get("children").size());
        assertFalse(hasTruncationFlag(out), "a tree within budget must carry no truncation flag");
        assertFalse(out.has("omittedChildCount"));
    }

    private int countNodes(JsonNode n) {
        if (n == null || !n.isObject()) return 0;
        int c = 1;
        JsonNode kids = n.get("children");
        if (kids != null && kids.isArray()) for (JsonNode k : kids) c += countNodes(k);
        return c;
    }

    private boolean hasTruncationFlag(JsonNode n) {
        if (n == null || !n.isObject()) return false;
        if (n.path("childrenTruncated").asBoolean(false)) return true;
        JsonNode kids = n.get("children");
        if (kids != null && kids.isArray()) for (JsonNode k : kids) if (hasTruncationFlag(k)) return true;
        return false;
    }
}
