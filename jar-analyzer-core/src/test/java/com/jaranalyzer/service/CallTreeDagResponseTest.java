package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The DAG response must be bounded by distinct nodes, not by paths.
 *
 * <h2>The measurement that forced this</h2>
 *
 * <p>Endpoint 271 of {@code unified-web-1.0.0-SNAPSHOT}:
 *
 * <pre>
 *   expanded tree : 300,167,238 nodes   — 120 MB after 3.6 min, with a 200,000-node cap
 *   reachable DAG :       2,282 nodes   — 7.2 MB in 0.44 s
 * </pre>
 *
 * <p>The cap kept <b>0.07%</b> of the tree. A node budget can only choose how much of an unusable
 * answer to send; it cannot make the answer usable. This test pins the property that does: output
 * proportional to distinct methods, which is exactly what {@link CallTreeFlattener} stores and what
 * expansion was throwing away.
 */
class CallTreeDagResponseTest {

    private static final ObjectMapper M = new ObjectMapper();

    /**
     * n levels, two edges per level pointing at the same child: 2^n expanded, n+1 stored.
     * The cheapest possible model of the production shape.
     */
    private String sharedChain(int levels) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < levels; i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append("n").append(i).append("\":{\"method\":\"m").append(i)
              .append("\",\"childIds\":[\"n").append(i + 1).append("\",\"n").append(i + 1).append("\"]}");
        }
        sb.append(",\"n").append(levels).append("\":{\"method\":\"leaf\",\"childIds\":[]}}");
        return sb.toString();
    }

    private JsonNode dagOf(String dict, String rootId) throws Exception {
        var parser = M.getFactory().createParser(dict);
        parser.nextToken();
        CallTreeInflater inf = CallTreeInflater.read(parser, M);
        StringWriter w = new StringWriter();
        try (var gen = M.getFactory().createGenerator(w)) {
            inf.writeReachable(rootId, gen);
        }
        return M.readTree(w.toString());
    }

    @Test
    @DisplayName("a 2^40 tree ships as 41 nodes")
    void boundedByDistinctNodes() throws Exception {
        JsonNode out = dagOf(sharedChain(40), "n0");

        assertEquals("dag-v1", out.get("format").asText());
        assertEquals("n0", out.get("rootId").asText());
        // 41 stored nodes describe a tree of 2^40 (~1.1 trillion) expanded nodes.
        assertEquals(41, out.get("nodeCount").asInt(),
                "the response must be proportional to distinct nodes, not paths");
        assertEquals(41, out.get("nodes").size());
    }

    @Test
    @DisplayName("childIds are preserved, so the client can expand on demand")
    void childIdsSurvive() throws Exception {
        JsonNode out = dagOf(sharedChain(5), "n0");
        JsonNode n0 = out.get("nodes").get("n0");
        assertNotNull(n0, "root record missing");
        assertEquals(2, n0.get("childIds").size(),
                "without childIds the client cannot build the next level");
        assertEquals("m0", n0.get("method").asText(), "the record's own fields must be intact");
    }

    @Test
    @DisplayName("only the reachable subgraph is sent")
    void unreachableNodesAreExcluded() throws Exception {
        String dict = "{\"a\":{\"method\":\"a\",\"childIds\":[\"b\"]},"
                    + "\"b\":{\"method\":\"b\",\"childIds\":[]},"
                    + "\"orphan\":{\"method\":\"orphan\",\"childIds\":[]}}";
        JsonNode out = dagOf(dict, "a");
        assertEquals(2, out.get("nodeCount").asInt(), "orphan is not reachable from a");
        assertFalse(out.get("nodes").has("orphan"),
                "an endpoint must pay only for what it can reach");
    }

    @Test
    @DisplayName("an unknown root yields an empty, well-formed answer rather than an error")
    void unknownRootIsEmptyNotBroken() throws Exception {
        JsonNode out = dagOf("{\"a\":{\"method\":\"a\",\"childIds\":[]}}", "nope");
        assertEquals(0, out.get("nodeCount").asInt());
        assertEquals(0, out.get("nodes").size());
        assertEquals("dag-v1", out.get("format").asText(),
                "still a valid envelope, so the client can tell empty from failed");
    }

    @Test
    @DisplayName("a cycle terminates — the visited set is the guard")
    void cyclesTerminate() throws Exception {
        // Content-addressed ids cannot cycle in practice, but a hand-edited or legacy document can,
        // and an unbounded walk would hang the request rather than the render.
        String dict = "{\"a\":{\"method\":\"a\",\"childIds\":[\"b\"]},"
                    + "\"b\":{\"method\":\"b\",\"childIds\":[\"a\"]}}";
        JsonNode out = dagOf(dict, "a");
        assertEquals(2, out.get("nodeCount").asInt());
    }
}
