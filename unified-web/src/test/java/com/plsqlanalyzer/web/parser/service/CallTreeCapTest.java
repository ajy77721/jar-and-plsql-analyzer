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
 * MID-006 (UI-019/§141 — pointed at the real live path) — {@code /call-tree/{nodeId}} caps at a
 * node count and says so, instead of building an unbounded tree.
 *
 * <h2>This test used to pass against dead code</h2>
 *
 * <p>It originally exercised {@code ParserAnalysisReader}, which implements this exact cap
 * ({@code MAX_CALL_TREE_NODES}/{@code TreeBudget}) — but {@code ParserAnalysisReader} is never
 * wired into {@code AnalysisReaderFactory} (confirmed by grep: zero references outside its own
 * file and two stale comments). The real path every request actually reaches is {@code
 * AnalysisService.getCallTree} → {@code StaticAnalysisReader}/{@code ClaudeAnalysisReader} →
 * {@code CallTreeFacetReader} → {@code CallGraphNav.buildCallTreeNode}, which had no cap at all
 * until this fix — so this test suite was green while the live endpoint remained genuinely
 * unbounded. Repointed at {@code StaticAnalysisReader} (the real class) instead, and the cap logic
 * itself ported into {@code CallGraphNav}, the class actually on the live path.
 *
 * <h2>Why a synthetic fixture, not the real corpus</h2>
 *
 * <p>Today's PL/SQL corpus (52 procedures) tops out at 277 nodes for its worst call-tree root —
 * comfortably under any reasonable cap, so no real fixture can exercise the boundary. The mechanism
 * this guards against is real regardless: {@code buildCallTreeNode}'s cycle guard only breaks a
 * <b>direct</b> cycle ({@code visited} is added and removed around each recursive call), so the
 * same node reached by different paths is rebuilt in full each time — the identical shape to the
 * JAR call-tree explosion already measured elsewhere in this registry (one endpoint,
 * 300,167,238 nodes, from a source graph that looked equally harmless).
 *
 * <h2>The contract this fills in, not invents</h2>
 *
 * <p>{@code childrenTruncated} / {@code omittedChildCount} were already read by both call-tree
 * renderers ({@code parser/js/callGraph.js}, {@code plsql/js/callGraph.js}) before this fix
 * existed — the UI half of MID-006 was built, the live server half never emitted the fields it
 * renders a badge for. This test pins the names as much as the behaviour: a renamed field would
 * pass every assertion here and still leave the badge silently unrendered.
 */
class CallTreeCapTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ANALYSIS = "wide-graph-fixture";

    /** A root fanning out to {@code breadth} leaves — enough to blow past any small cap. */
    private static InMemoryArtifactStore wideGraphStore(int breadth) throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();

        ObjectNode index = M.createObjectNode();
        ArrayNode nodes = index.putArray("nodes");
        addNode(nodes, "ROOT");
        for (int i = 0; i < breadth; i++) addNode(nodes, "LEAF" + i);
        write(store, "api/index.json", index);

        ObjectNode graph = M.createObjectNode();
        ArrayNode edges = graph.putArray("edges");
        for (int i = 0; i < breadth; i++) {
            ObjectNode edge = edges.addObject();
            edge.put("fromNodeId", "ROOT");
            edge.put("toNodeId", "LEAF" + i);
            edge.put("fromSchema", "APP");
            edge.put("toSchema", "APP");
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

    private static void write(InMemoryArtifactStore store, String path, JsonNode content)
            throws Exception {
        var key = AnalysisPaths.resolveKey(ANALYSIS, path);
        try (var out = store.write(key)) {
            M.writeValue(out, content);
        }
    }

    @Test
    void theTreeStopsAtTheCeiling() throws Exception {
        InMemoryArtifactStore store = wideGraphStore(5000);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode tree = reader.getCallTree(ANALYSIS, "ROOT");

        int childCount = tree.path("children").size();
        assertTrue(childCount < 5000,
                "built " + childCount + " of 5000 children — the cap did not bite");
        assertTrue(childCount > 0, "the cap left nothing built at all");
    }

    @Test
    void theCutNodeIsMarkedWithTheFieldsTheUiAlreadyReads() throws Exception {
        InMemoryArtifactStore store = wideGraphStore(5000);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode tree = reader.getCallTree(ANALYSIS, "ROOT");

        assertTrue(tree.path("childrenTruncated").asBoolean(false),
                "the root's children were cut but childrenTruncated is not set — "
                        + "parser/js/callGraph.js and plsql/js/callGraph.js render a badge from "
                        + "exactly this field and would show nothing");
        int omitted = tree.path("omittedChildCount").asInt(-1);
        assertTrue(omitted > 0, "omittedChildCount is " + omitted + ", expected a positive count");
        assertEquals(5000, tree.path("children").size() + omitted,
                "built + omitted does not add up to the source breadth — some children vanished "
                        + "without being counted either way");
    }

    @Test
    void theRootReportsTruncatedTrue() throws Exception {
        InMemoryArtifactStore store = wideGraphStore(5000);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode tree = reader.getCallTree(ANALYSIS, "ROOT");
        assertTrue(tree.path("truncated").asBoolean(false),
                "the root-level truncated flag was not set even though the tree was cut");
    }

    @Test
    void aSmallTreeIsNotTruncatedAndCarriesNoStrayFields() throws Exception {
        // The honest direction: a cap that fires on everything is as useless as one that fires on
        // nothing, and a badge on an untruncated tree would be a false positive nobody trusts.
        InMemoryArtifactStore store = wideGraphStore(5);
        StaticAnalysisReader reader = new StaticAnalysisReader(store);

        JsonNode tree = reader.getCallTree(ANALYSIS, "ROOT");
        assertEquals(5, tree.path("children").size());
        assertFalse(tree.path("truncated").asBoolean(false),
                "a 5-child tree, far under the cap, was reported as truncated");
        assertFalse(tree.has("childrenTruncated"),
                "an untruncated node carries the truncation field anyway");
    }
}
