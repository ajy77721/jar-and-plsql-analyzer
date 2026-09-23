package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowEdge;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MID-026 — {@code writeCallGraph} used to accept a {@code Map<String, FlowNode> nodeIndex}
 * parameter it never read anywhere in its own body: every field it writes (`from`/`to`/schema/
 * nodeId/lines/count/backEdge) comes from the {@code FlowEdge} objects themselves. Removed rather
 * than converted to {@code LightNode} — this consumer needs no node data at all, full or
 * lightweight, a cheaper outcome than the bounded-conversion pattern {@code writeTableIndex}/
 * {@code writeStatementIndex} used.
 *
 * <p>This test locks the new, shorter signature in and proves the method's own real logic
 * (dedup by from/to pair, line accumulation, back-edge detection) still works correctly without
 * a node lookup — the thing a reflection-based caller passing the old 4-arg signature would have
 * caught as a compile error, but a test pins the *behavior*, not just the shape.
 */
class WriteCallGraphNoNodeIndexTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private FlowEdge edge(String fromNodeId, String fromObj, String toNodeId, String toObj, int line, int depth) {
        FlowEdge e = new FlowEdge();
        e.setFromNodeId(fromNodeId);
        e.setFromObject(fromObj);
        e.setFromSchema("APP");
        e.setToNodeId(toNodeId);
        e.setToObject(toObj);
        e.setToSchema("APP");
        e.setLine(line);
        e.setDepth(depth);
        return e;
    }

    @Test
    void writesADedupedGraphWithNoNodeIndexParameterAtAll() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, ArtifactKey.of("test-build"));
        ArtifactKey apiDir = ArtifactKey.of("test-build", "api");

        List<FlowEdge> edges = List.of(
                edge("A", "PROC_A", "B", "PROC_B", 10, 0),
                edge("A", "PROC_A", "B", "PROC_B", 20, 0), // same pair, second call site
                edge("B", "PROC_B", "C", "PROC_C", 5, 1),
                edge("C", "PROC_C", "A", "PROC_A", 1, 2)   // closes a cycle A->B->C->A
        );
        List<List<String>> cycles = List.of(List.of("A", "B", "C"));

        // MID-026: the real point of this test — writeCallGraph now takes exactly 3 args
        // (ArtifactKey, List<FlowEdge>, List<List<String>>), no node index at all.
        Method m = ChunkedFlowWriter.class.getDeclaredMethod(
                "writeCallGraph", ArtifactKey.class, List.class, List.class);
        m.setAccessible(true);
        m.invoke(writer, apiDir, edges, cycles);

        JsonNode graph = JSON.readTree(store.read(apiDir.resolve("call_graph.json")));
        assertEquals(4, graph.path("totalEdges").asInt(), "all 4 raw edges counted");
        assertEquals(3, graph.path("uniqueEdges").asInt(), "A->B deduped from 2 occurrences to 1");

        JsonNode edgesArr = graph.path("edges");
        boolean foundDedupedAB = false;
        boolean foundBackEdge = false;
        for (JsonNode e : edgesArr) {
            if ("A".equals(e.path("fromNodeId").asText()) && "B".equals(e.path("toNodeId").asText())) {
                foundDedupedAB = true;
                assertEquals(2, e.path("count").asInt(), "A->B occurred twice");
                assertEquals(2, e.path("lines").size(), "both line numbers kept");
            }
            if ("C".equals(e.path("fromNodeId").asText()) && "A".equals(e.path("toNodeId").asText())) {
                foundBackEdge = true;
                assertTrue(e.path("backEdge").asBoolean(), "C->A closes the cycle, must be flagged");
            }
        }
        assertTrue(foundDedupedAB, "the deduped A->B edge must be present");
        assertTrue(foundBackEdge, "the cycle-closing edge must be present and flagged");
    }
}
