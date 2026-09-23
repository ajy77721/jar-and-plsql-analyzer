package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression: a call tree's children must come back in true source/execution order — ascending
 * call-site line in the PARENT's source — across ALL children, regardless of which target each
 * call points at.
 *
 * <p>The bug: {@code call_graph.json} stores one deduped edge per (from,to) pair with every call
 * site folded into a {@code lines} array ({@code ChunkedFlowWriter#writeCallGraph}).
 * {@code CallGraphNav.buildCallTreeNode} expanded those edges back into per-line occurrences
 * edge-by-edge, so every call to the same callee came out contiguously and children were ordered
 * by callee NAME first, line second. On the real Postgres {@code ERP_USP_RUN_CYCLE} that pushed
 * {@code erp_fn_get_config} (line 7) to position 32, behind thirty {@code write_audit_trail} calls
 * from lines 34-263.
 *
 * <p>The fixture below is the interleaved case the grouping bug cannot survive: A@5, B@10, A@15,
 * C@20 — the repeated callee is deliberately NOT contiguous, so grouping-by-name yields
 * A,A,B,C while correct source order is A,B,A,C.
 */
class CallTreeChildOrderTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ANALYSIS = "child-order-fixture";

    @Test
    void repeatedCallInterleavedWithDistinctCallsStaysInSourceOrder() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();

        ObjectNode index = M.createObjectNode();
        ArrayNode nodes = index.putArray("nodes");
        for (String id : new String[]{"ROOT", "A", "B", "C"}) addNode(nodes, id);
        write(store, "api/index.json", index);

        ObjectNode graph = M.createObjectNode();
        ArrayNode edges = graph.putArray("edges");
        // Deduped edge shape: ROOT->A carries BOTH its call sites (5 and 15) in one entry.
        addEdge(edges, "A", 5, 15);
        addEdge(edges, "B", 10);
        addEdge(edges, "C", 20);
        write(store, "api/call_graph.json", graph);

        JsonNode tree = new StaticAnalysisReader(store).getCallTree(ANALYSIS, "ROOT");

        List<String> actual = new ArrayList<>();
        List<Integer> lines = new ArrayList<>();
        for (JsonNode child : tree.path("children")) {
            actual.add(child.path("name").asText());
            lines.add(child.path("callLineNumber").asInt());
        }

        assertEquals(List.of("A", "B", "A", "C"), actual,
                "children came back grouped by callee name instead of in call-site order");
        assertEquals(List.of(5, 10, 15, 20), lines,
                "call-site line numbers are not ascending");
    }

    /** Call sites with no recorded line sink to the end rather than claiming line 0 up front. */
    @Test
    void occurrencesWithNoRecordedLineSortLast() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();

        ObjectNode index = M.createObjectNode();
        ArrayNode nodes = index.putArray("nodes");
        for (String id : new String[]{"ROOT", "A", "B"}) addNode(nodes, id);
        write(store, "api/index.json", index);

        ObjectNode graph = M.createObjectNode();
        ArrayNode edges = graph.putArray("edges");
        addEdge(edges, "A");        // no lines at all — unknown call site
        addEdge(edges, "B", 12);
        write(store, "api/call_graph.json", graph);

        JsonNode tree = new StaticAnalysisReader(store).getCallTree(ANALYSIS, "ROOT");

        List<String> actual = new ArrayList<>();
        for (JsonNode child : tree.path("children")) actual.add(child.path("name").asText());
        assertEquals(List.of("B", "A"), actual,
                "an occurrence with no recorded line must not jump ahead of a real line-12 call");
    }

    private static void addEdge(ArrayNode edges, String to, int... callLines) {
        ObjectNode edge = edges.addObject();
        edge.put("fromNodeId", "ROOT");
        edge.put("toNodeId", to);
        edge.put("fromSchema", "APP");
        edge.put("toSchema", "APP");
        ArrayNode lines = edge.putArray("lines");
        for (int l : callLines) lines.add(l);
        edge.put("count", callLines.length);
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
}
