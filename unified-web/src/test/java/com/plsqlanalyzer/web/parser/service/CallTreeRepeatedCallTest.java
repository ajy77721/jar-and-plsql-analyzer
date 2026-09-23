package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Reproduces the reported bug: "a procedure called 10 separate times ... only shows up ONCE in
 * the Trace and Hierarchy UI screens". Root cause: {@code ChunkedFlowWriter#writeCallGraph}
 * legitimately deduplicates edges for the call-graph VIEWER (one visual A->B edge, with a "lines"
 * array recording every call-site line and a "count"). But {@code CallGraphNav.buildCallTreeNode}
 * — the live path behind {@code /call-tree/{nodeId}} which both the Trace and Hierarchy screens
 * read — was treating each deduped edge as a single call site too, building exactly one child node
 * per distinct (from,to) edge regardless of how many times it was actually called. Fixed by
 * expanding each edge into one child occurrence per recorded call-site line.
 */
class CallTreeRepeatedCallTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ANALYSIS = "repeated-call-fixture";

    private static InMemoryArtifactStore repeatedCallStore(int callCount) throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();

        ObjectNode index = M.createObjectNode();
        ArrayNode nodes = index.putArray("nodes");
        addNode(nodes, "A");
        addNode(nodes, "B");
        write(store, "api/index.json", index);

        // Mirror ChunkedFlowWriter#writeCallGraph's real output shape: ONE deduped edge entry for
        // A->B carrying a "lines" array with one entry per call site (B(1); B(2); ... B(10);).
        ObjectNode graph = M.createObjectNode();
        ArrayNode edges = graph.putArray("edges");
        ObjectNode edge = edges.addObject();
        edge.put("fromNodeId", "A");
        edge.put("toNodeId", "B");
        edge.put("fromSchema", "APP");
        edge.put("toSchema", "APP");
        ArrayNode lines = edge.putArray("lines");
        for (int i = 1; i <= callCount; i++) lines.add(i * 10);
        edge.put("count", callCount);
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
    void tenCallSitesProduceTenDistinctChildEdgesNotOne() throws Exception {
        InMemoryArtifactStore store = repeatedCallStore(10);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode tree = reader.getCallTree(ANALYSIS, "A");

        assertEquals(10, tree.path("children").size(),
                "B is called 10 times from A but only " + tree.path("children").size()
                        + " child occurrence(s) were built — the dedup-collapse bug");

        // Each occurrence should carry its own distinct call line, not all sharing line[0].
        java.util.Set<Integer> lines = new java.util.HashSet<>();
        for (JsonNode child : tree.path("children")) {
            lines.add(child.path("callLineNumber").asInt());
        }
        assertEquals(10, lines.size(), "all 10 occurrences collapsed onto the same call line");
    }
}
