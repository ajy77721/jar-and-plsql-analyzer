package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-004 — the graph transfer is bounded, not just the render.
 *
 * <h2>The mismatch</h2>
 *
 * <p>FE-018 capped the renderer at 300 nodes because the repulsion step is ~6.4 billion operations
 * at 8,000. The transfer was never capped: the client fetched every edge and discarded almost all of
 * them. 847 edges today, <b>35,776 at 832 procedures</b> in the load proof, so ~344,000 at the
 * stated target — to draw 300 nodes.
 *
 * <p>Selection must be server-side because degree is a property of the whole graph: a client cannot
 * pick the top 300 without first holding all of it.
 */
class CallGraphTopNTest {

    private static final ObjectMapper M = new ObjectMapper();

    /** A star: HUB connects to n leaves, so HUB has degree n and each leaf has degree 1. */
    private static ObjectNode star(int leaves) {
        ObjectNode g = M.createObjectNode();
        ArrayNode edges = g.putArray("edges");
        for (int i = 0; i < leaves; i++) {
            edges.addObject().put("fromNodeId", "HUB").put("toNodeId", "LEAF" + i);
        }
        return g;
    }

    @Test
    void theHighestDegreeNodesAreKept() {
        JsonNode out = CallGraphTopN.select(M, star(100), 10);
        Set<String> nodes = new HashSet<>();
        for (JsonNode e : out.path("edges")) {
            nodes.add(e.path("fromNodeId").asText());
            nodes.add(e.path("toNodeId").asText());
        }
        assertTrue(nodes.contains("HUB"), "the most-connected node was dropped");
        assertEquals(10, out.path("nodeCount").asInt(), "the node cap was not applied");
        assertTrue(nodes.size() <= 10, "more nodes were returned than the cap allows");
    }

    @Test
    void anEdgeToADroppedNodeIsNotReturned() {
        // An edge whose other end is gone draws a line to nothing.
        JsonNode out = CallGraphTopN.select(M, star(100), 5);
        Set<String> nodes = new HashSet<>();
        for (JsonNode e : out.path("edges")) {
            nodes.add(e.path("fromNodeId").asText());
            nodes.add(e.path("toNodeId").asText());
        }
        for (JsonNode e : out.path("edges")) {
            assertTrue(nodes.contains(e.path("fromNodeId").asText())
                    && nodes.contains(e.path("toNodeId").asText()),
                    "an edge references a node outside the returned set");
        }
    }

    @Test
    void theResponseSaysItIsASlice() {
        // A subgraph presented as a graph is the failure FE-018's notice exists to prevent. Drop
        // these counts and the client cannot tell the reader that it is showing part of the data.
        JsonNode out = CallGraphTopN.select(M, star(100), 10);
        assertTrue(out.path("truncated").asBoolean(), "a truncated slice does not report itself");
        assertEquals(101, out.path("totalNodes").asInt(), "the true node count is not reported");
        assertEquals(100, out.path("totalEdges").asInt(), "the true edge count is not reported");
    }

    @Test
    void anUntruncatedGraphSaysSo() {
        // The other direction: a small graph must not claim to be a slice, or every reader sees a
        // warning that is never true and learns to ignore it.
        JsonNode out = CallGraphTopN.select(M, star(4), 300);
        assertFalse(out.path("truncated").asBoolean(), "a complete graph reported itself truncated");
        assertEquals(5, out.path("nodeCount").asInt());
    }

    @Test
    void theSliceDoesNotDependOnTheOrderEdgesArrivedIn() {
        // The property that is actually testable. The same logical graph, edges supplied in reverse,
        // must yield the same slice — otherwise re-running an analysis that emits edges in a
        // different order silently changes which subgraph a reader sees.
        //
        // Note on what this does NOT prove: dropping the explicit name tiebreaker leaves this
        // passing, because HashMap iteration order is stable for a given key set within a JVM. The
        // tiebreaker is kept regardless — HashMap ordering is not contractual, and relying on it
        // means the slice is only incidentally stable. That is a robustness argument, not one a
        // single-process test can demonstrate, and saying so is better than implying otherwise.
        String[] names = {"C", "A", "D", "B"};
        ObjectNode forward = M.createObjectNode();
        ArrayNode fe = forward.putArray("edges");
        for (String n : names) fe.addObject().put("fromNodeId", n).put("toNodeId", "SINK");

        ObjectNode reversed = M.createObjectNode();
        ArrayNode re = reversed.putArray("edges");
        for (int i = names.length - 1; i >= 0; i--) {
            re.addObject().put("fromNodeId", names[i]).put("toNodeId", "SINK");
        }

        Set<String> a = nodesOf(CallGraphTopN.select(M, forward, 3));
        Set<String> b = nodesOf(CallGraphTopN.select(M, reversed, 3));
        assertEquals(a, b, "the slice depends on the order edges were supplied in");
    }

    private static Set<String> nodesOf(JsonNode out) {
        Set<String> nodes = new HashSet<>();
        for (JsonNode e : out.path("edges")) {
            nodes.add(e.path("fromNodeId").asText());
            nodes.add(e.path("toNodeId").asText());
        }
        return nodes;
    }

    @Test
    void bothEdgeFieldSpellingsAreUnderstood() {
        // The artifact carries fromNodeId/toNodeId and also from/to; an endpoint reader that knows
        // only one silently computes every degree as zero and returns an arbitrary slice.
        ObjectNode g = M.createObjectNode();
        ArrayNode edges = g.putArray("edges");
        edges.addObject().put("from", "HUB").put("to", "LEAF1");
        edges.addObject().put("from", "HUB").put("to", "LEAF2");
        JsonNode out = CallGraphTopN.select(M, g, 2);
        assertEquals(3, out.path("totalNodes").asInt(),
                "the plain from/to spelling was not recognised, so degrees were all zero");
    }

    @Test
    void anEmptyOrAbsentGraphIsNotAnError() {
        assertEquals(0, CallGraphTopN.select(M, null, 300).path("totalEdges").asInt());
        assertEquals(0, CallGraphTopN.select(M, M.createObjectNode(), 300).path("totalEdges").asInt());
        assertFalse(CallGraphTopN.select(M, M.createObjectNode(), 300).path("truncated").asBoolean());
    }

    @Test
    void theSliceIsSmallerThanTheGraphItCameFrom() {
        // The point of the endpoint, stated as a size property rather than a node count.
        JsonNode full = star(2000);
        int fullBytes = full.toString().length();
        int sliceBytes = CallGraphTopN.select(M, full, 300).path("edges").toString().length();
        assertTrue(sliceBytes < fullBytes / 5,
                "the slice is " + sliceBytes + " bytes against " + fullBytes
                        + " — not a meaningful reduction");
    }
}
