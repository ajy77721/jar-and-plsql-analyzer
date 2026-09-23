package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowNode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MID-026's 4th conversion — {@code writeNodeIdMap} only ever reads {@link FlowNode#buildFullId()}'s
 * finished string, never the raw {@code parameters} list the method itself reads to build it. The
 * light projection ({@code LightNode.fullId}) now carries the precomputed answer instead of the
 * ingredients, so {@code writeNodeIdMap} takes {@code Map<String, LightNode>} rather than the full
 * {@code Map<String, FlowNode>}.
 *
 * <p>Verified byte-for-byte against the real analysis's own {@code node_id_map.json} before this
 * change shipped (8,427 bytes, identical after); this test locks the same behavior in with a
 * synthetic node so it survives independent of any real corpus.
 */
class WriteNodeIdMapLightNodeTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private FlowNode node(String nodeId, String schema, String pkg, String obj) {
        FlowNode n = new FlowNode();
        n.setNodeId(nodeId);
        n.setSchema(schema);
        n.setPackageName(pkg);
        n.setObjectName(obj);
        n.setDepth(0);
        n.setObjectType("PROCEDURE");
        return n;
    }

    @SuppressWarnings("unchecked")
    private static Object toLightNode(FlowNode n) throws Exception {
        Method m = ChunkedFlowWriter.class.getDeclaredMethod("toLightNode", FlowNode.class);
        m.setAccessible(true);
        return m.invoke(null, n);
    }

    @Test
    void writesTheSameFullIdBuildFullIdWouldHaveProduced() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, ArtifactKey.of("test-build"));
        ArtifactKey apiDir = ArtifactKey.of("test-build", "api");

        FlowNode standalone = node("ERP$USP_RUN_CYCLE", "ANALYZER", null, "USP_RUN_CYCLE");
        FlowNode packaged = node("ERP$PKG_PLANNER$COMPUTE_TOTAL", "ANALYZER", "ERP_PKG_PLANNER", "COMPUTE_TOTAL");

        // The real behavior under test: writeNodeIdMap's own output must equal what
        // FlowNode.buildFullId() itself would have produced for the same node — confirming the
        // precomputed LightNode.fullId field is not silently wrong.
        String expectedStandalone = standalone.buildFullId();
        String expectedPackaged = packaged.buildFullId();

        Object lightStandalone = toLightNode(standalone);
        Object lightPackaged = toLightNode(packaged);

        Map<String, Object> lightIndex = new LinkedHashMap<>();
        lightIndex.put("ERP$USP_RUN_CYCLE", lightStandalone);
        lightIndex.put("ERP$PKG_PLANNER$COMPUTE_TOTAL", lightPackaged);

        Method write = ChunkedFlowWriter.class.getDeclaredMethod(
                "writeNodeIdMap", ArtifactKey.class, Map.class);
        write.setAccessible(true);
        write.invoke(writer, apiDir, lightIndex);

        JsonNode map = JSON.readTree(store.read(apiDir.resolve("node_id_map.json")));
        assertEquals(expectedStandalone,
                map.path("shortToFull").path("ERP$USP_RUN_CYCLE").asText(),
                "standalone procedure's fullId must match FlowNode.buildFullId() exactly");
        assertEquals(expectedPackaged,
                map.path("shortToFull").path("ERP$PKG_PLANNER$COMPUTE_TOTAL").asText(),
                "package-body procedure's fullId must match FlowNode.buildFullId() exactly");
        assertEquals("ERP$USP_RUN_CYCLE",
                map.path("fullToId").path(expectedStandalone).asText(),
                "the reverse lookup must round-trip back to the same short id");
    }

    /**
     * Calibrated by having {@code toLightNode} skip the {@code buildFullId()} call and store an
     * empty string instead (simulating the exact class of bug this precomputation could introduce
     * — the light projection silently carrying a stale or missing answer) — confirmed the test
     * failed with the exact expected empty-string mismatch, restored.
     */
    @Test
    void differentNodesProduceDifferentFullIds() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, ArtifactKey.of("test-build"));
        ArtifactKey apiDir = ArtifactKey.of("test-build", "api");

        FlowNode a = node("A", "ANALYZER", null, "PROC_A");
        FlowNode b = node("B", "ANALYZER", null, "PROC_B");

        Map<String, Object> lightIndex = new LinkedHashMap<>();
        lightIndex.put("A", toLightNode(a));
        lightIndex.put("B", toLightNode(b));

        Method write = ChunkedFlowWriter.class.getDeclaredMethod(
                "writeNodeIdMap", ArtifactKey.class, Map.class);
        write.setAccessible(true);
        write.invoke(writer, apiDir, lightIndex);

        JsonNode map = JSON.readTree(store.read(apiDir.resolve("node_id_map.json")));
        String fullA = map.path("shortToFull").path("A").asText();
        String fullB = map.path("shortToFull").path("B").asText();
        assertEquals(a.buildFullId(), fullA);
        assertEquals(b.buildFullId(), fullB);
        org.junit.jupiter.api.Assertions.assertNotEquals(fullA, fullB, "distinct nodes must not collide");
    }
}
