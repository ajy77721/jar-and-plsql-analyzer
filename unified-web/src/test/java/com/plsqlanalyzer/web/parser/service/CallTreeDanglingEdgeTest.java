package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces the reported bug: a bare {@code identifier(args)} that is really a package-level
 * collection variable being indexed (e.g. {@code v_tbl(i)}) is syntactically identical to a call
 * to the ANTLR grammar. {@code PlSqlAnalysisVisitor.classifyCall} misclassifies it as an EXTERNAL
 * call with no package, and — before the DependencyCrawler fix — the package-aggregation crawl
 * path never ran {@code filterLocalVarCalls} against it, so a bogus {@code CallInfo} for the
 * variable's own name leaked into {@code call_graph.json} as an edge whose target was never
 * resolved to any real object (the crawler logs "No source found for: ..." and writes no
 * {@code FlowNode}/index entry for it at all).
 *
 * <p>{@code CallGraphNav.buildCallTreeNode} previously built a tree node for every edge
 * regardless, degrading gracefully to blank fields ("" / 0) when {@code nodeMetadata} had no
 * entry for the target — producing the reported broken row (no line range, no proper name, no
 * "Make Root", tagged INTERNAL). This test asserts the fix: an edge whose target has NO node
 * metadata entry at all is dropped from the tree entirely, while a sibling edge whose target DOES
 * have metadata (even a minimal non-parsable stub, e.g. a TABLE/VIEW/SYNONYM reference) still
 * renders normally. Same guard, same assertion shape, for the reverse "callers" direction.
 */
class CallTreeDanglingEdgeTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ANALYSIS = "dangling-edge-fixture";

    private static InMemoryArtifactStore fixture() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();

        ObjectNode index = M.createObjectNode();
        ArrayNode nodes = index.putArray("nodes");
        addNode(nodes, "APP$A");
        addNode(nodes, "APP$B");
        // Deliberately NO node entry for "APP$COV_STAX_BREAKDOWN_TBL3" — nothing was ever
        // written for it (the crawler's "No source found for: ..." case).
        write(store, "api/index.json", index);

        ObjectNode graph = M.createObjectNode();
        ArrayNode edges = graph.putArray("edges");

        // A real call, A -> B: must still render.
        ObjectNode realEdge = edges.addObject();
        realEdge.put("fromNodeId", "APP$A");
        realEdge.put("toNodeId", "APP$B");
        realEdge.put("fromSchema", "APP");
        realEdge.put("toSchema", "APP");
        realEdge.putArray("lines").add(10);

        // The bogus "call" to a package-level collection variable, A -> COV_STAX_BREAKDOWN_TBL3:
        // must be dropped, not rendered blank.
        ObjectNode danglingEdge = edges.addObject();
        danglingEdge.put("fromNodeId", "APP$A");
        danglingEdge.put("toNodeId", "APP$COV_STAX_BREAKDOWN_TBL3");
        danglingEdge.put("fromSchema", "APP");
        danglingEdge.putArray("lines").add(20);

        write(store, "api/call_graph.json", graph);
        return store;
    }

    private static void addNode(ArrayNode nodes, String id) {
        ObjectNode n = nodes.addObject();
        n.put("nodeId", id);
        n.put("name", id);
        n.put("schema", "APP");
    }

    private static void write(InMemoryArtifactStore store, String path, JsonNode content)
            throws Exception {
        var key = AnalysisPaths.resolveKey(ANALYSIS, path);
        try (var out = store.write(key)) {
            M.writeValue(out, content);
        }
    }

    @Test
    void danglingEdgeOmittedFromCallTreeRealEdgeKept() throws Exception {
        InMemoryArtifactStore store = fixture();
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode tree = reader.getCallTree(ANALYSIS, "APP$A");

        assertEquals(1, tree.path("children").size(),
                "Dangling edge to a target with no node metadata must be dropped, not rendered blank");
        JsonNode onlyChild = tree.path("children").get(0);
        assertEquals("APP$B", onlyChild.path("id").asText());
        assertTrue(onlyChild.path("startLine").asInt(-1) >= 0);
    }

    @Test
    void danglingEdgeOmittedFromCallersRealEdgeKept() throws Exception {
        InMemoryArtifactStore store = fixture();
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        // Callers of B: only A, and it must have real metadata attached (this direction has no
        // dangling case in this fixture, but exercises the same code path/guard).
        JsonNode callersOfB = reader.getCallers(ANALYSIS, "APP$B");
        assertEquals(1, callersOfB.size());
        assertEquals("APP$A", callersOfB.get(0).path("id").asText());

        // Callers of the dangling target itself: the caller (A) DOES have metadata, so it must
        // still be listed — the guard only drops entries with NO metadata for the far side of the
        // edge being rendered, never a legitimately-described node.
        JsonNode callersOfDangling = reader.getCallers(ANALYSIS, "APP$COV_STAX_BREAKDOWN_TBL3");
        assertEquals(1, callersOfDangling.size());
        assertEquals("APP$A", callersOfDangling.get(0).path("id").asText());
    }
}
