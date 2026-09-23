package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UI-019 (§142) — {@code maxDepth} on {@code getCallTree}: a per-level slice instead of the whole
 * (node-count-budgeted) tree, so a client can fetch one level at a time.
 *
 * <p>Uses a genuine 3-level tree (ROOT → MID → LEAF) rather than {@code CallTreeCapTest}'s flat
 * fan-out — that fixture proves the node-count cap, not depth, and a fan-out has no third level to
 * assert against.
 */
class CallTreeMaxDepthTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ANALYSIS = "three-level-fixture";

    /** ROOT -> MID0..midCount-1 -> LEAF (one leaf per mid, so every mid has exactly one child). */
    private static InMemoryArtifactStore threeLevelStore(int midCount) throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();

        ObjectNode index = M.createObjectNode();
        ArrayNode nodes = index.putArray("nodes");
        addNode(nodes, "ROOT");
        for (int i = 0; i < midCount; i++) {
            addNode(nodes, "MID" + i);
            addNode(nodes, "LEAF" + i);
        }
        write(store, "api/index.json", index);

        ObjectNode graph = M.createObjectNode();
        ArrayNode edges = graph.putArray("edges");
        for (int i = 0; i < midCount; i++) {
            addEdge(edges, "ROOT", "MID" + i);
            addEdge(edges, "MID" + i, "LEAF" + i);
        }
        write(store, "api/call_graph.json", graph);

        return store;
    }

    private static void addNode(ArrayNode nodes, String id) {
        ObjectNode n = nodes.addObject();
        n.put("nodeId", id);
        n.put("name", id);
        n.put("schema", "APP");
    }

    private static void addEdge(ArrayNode edges, String from, String to) {
        ObjectNode edge = edges.addObject();
        edge.put("fromNodeId", from);
        edge.put("toNodeId", to);
        edge.put("fromSchema", "APP");
        edge.put("toSchema", "APP");
    }

    private static void write(InMemoryArtifactStore store, String path, JsonNode content)
            throws Exception {
        var key = AnalysisPaths.resolveKey(ANALYSIS, path);
        try (var out = store.write(key)) {
            M.writeValue(out, content);
        }
    }

    @Test
    void maxDepthZeroReturnsOnlyTheRootWithMoreChildrenFlagged() throws Exception {
        InMemoryArtifactStore store = threeLevelStore(3);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode tree = reader.getCallTree(ANALYSIS, "ROOT", 0);

        assertEquals(0, tree.path("children").size(),
                "maxDepth=0 must stop before building any children at all");
        assertTrue(tree.path("moreChildren").asBoolean(false),
                "the root has real children beyond the fetched depth — moreChildren must say so");
        assertEquals(3, tree.path("childCount").asInt(-1),
                "childCount must report how many children exist, even though none were built");
        assertFalse(tree.has("childrenTruncated"),
                "moreChildren (depth cut) must not be confused with childrenTruncated (budget cut)");
    }

    @Test
    void maxDepthOneReturnsRootAndChildrenButNotGrandchildren() throws Exception {
        InMemoryArtifactStore store = threeLevelStore(3);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode tree = reader.getCallTree(ANALYSIS, "ROOT", 1);

        assertEquals(3, tree.path("children").size(), "depth 1 (the MID nodes) must be built");
        assertFalse(tree.has("moreChildren"), "the root itself was fully expanded at this depth");
        for (JsonNode mid : tree.path("children")) {
            assertEquals(0, mid.path("children").size(),
                    "a MID node's own children (the LEAF) must not be built at maxDepth=1");
            assertTrue(mid.path("moreChildren").asBoolean(false),
                    "each MID node has a real LEAF child beyond this depth — must be flagged");
            assertEquals(1, mid.path("childCount").asInt(-1));
        }
    }

    @Test
    void nullMaxDepthIsUnchangedFromBeforeThisFeature() throws Exception {
        InMemoryArtifactStore store = threeLevelStore(3);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode withNull = reader.getCallTree(ANALYSIS, "ROOT", (Integer) null);
        JsonNode withoutParam = reader.getCallTree(ANALYSIS, "ROOT");

        assertEquals(withoutParam.toString(), withNull.toString(),
                "a null maxDepth must be byte-identical to the pre-existing no-arg call — "
                        + "every caller that doesn't know about this feature yet must see no change");
        // And the full tree really is there, three levels deep, unlike the two tests above.
        JsonNode firstMid = withNull.path("children").get(0);
        assertEquals(1, firstMid.path("children").size(),
                "an unbounded fetch must still reach the LEAF level");
    }

    @Test
    void aLeafNodeWithNoRealChildrenIsNotFalselyFlagged() throws Exception {
        InMemoryArtifactStore store = threeLevelStore(1);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        // Fetch just the LEAF node itself as a root, at maxDepth=0 — it has no children at all,
        // real or not, so moreChildren must be absent, not falsely true.
        JsonNode leaf = reader.getCallTree(ANALYSIS, "LEAF0", 0);
        assertEquals(0, leaf.path("children").size());
        assertFalse(leaf.has("moreChildren"),
                "a genuinely childless node must not be flagged moreChildren just because "
                        + "the depth cut was reached");
    }
}
