package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * One call edge must get ONE INTERNAL/EXTERNAL answer, whichever direction you read it from.
 *
 * <p>The downward call tree ({@code /call-tree/{id}}, behind Explore &gt; Hierarchy and Trace) and
 * the upward caller list ({@code /call-tree/{id}/callers}, behind Explore &gt; References "Called
 * By" and the source viewer's "Called By" panel) used to classify the same edge with two different
 * rules — the tree with {@code equalsIgnoreCase} plus metadata/nodeId-prefix fallbacks, the caller
 * list with case-sensitive {@code equals} and a mismatched from/to source. The Postgres and SQL
 * Server crawlers record {@code fromSchema:"ERP"} with {@code toSchema:"erp"} (835 of 1011 Postgres
 * edges and 273 of 337 SQL Server edges in the loaded demo analyses), so on those dialects every
 * such edge read INTERNAL going down and EXTERNAL coming back up. Both directions now share
 * {@link CallGraphNav#classifyCall}; this test is what keeps them sharing it.
 */
class CallTypeDirectionAgreementTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ANALYSIS = "calltype-direction-fixture";

    /**
     * @param fromSchema schema recorded on the edge's "from" side
     * @param toSchema   schema recorded on the edge's "to" side — differing only in CASE is the
     *                   real, extremely common shape this test exists for
     */
    private static StaticAnalysisReader reader(String fromSchema, String toSchema) throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();

        ObjectNode index = M.createObjectNode();
        ArrayNode nodes = index.putArray("nodes");
        addNode(nodes, "A", fromSchema);
        addNode(nodes, "B", toSchema);
        write(store, "api/index.json", index);

        ObjectNode graph = M.createObjectNode();
        ArrayNode edges = graph.putArray("edges");
        ObjectNode edge = edges.addObject();
        edge.put("fromNodeId", "A");
        edge.put("toNodeId", "B");
        edge.put("fromSchema", fromSchema);
        edge.put("toSchema", toSchema);
        edge.putArray("lines").add(7);
        edge.put("count", 1);
        write(store, "api/call_graph.json", graph);

        return new StaticAnalysisReader(store);
    }

    private static void addNode(ArrayNode nodes, String id, String schema) {
        ObjectNode n = nodes.addObject();
        n.put("nodeId", id);
        n.put("name", id);
        n.put("schema", schema);
    }

    private static void write(InMemoryArtifactStore store, String path, JsonNode content)
            throws Exception {
        var key = AnalysisPaths.resolveKey(ANALYSIS, path);
        try (var out = store.write(key)) {
            M.writeValue(out, content);
        }
    }

    /** The exact Postgres/SQL Server shape: same schema, recorded with different case. */
    @Test
    void caseOnlySchemaDifferenceIsInternalInBothDirections() throws Exception {
        StaticAnalysisReader r = reader("ERP", "erp");

        String downward = r.getCallTree(ANALYSIS, "A").path("children").get(0).path("callType").asText();
        String upward = r.getCallers(ANALYSIS, "B").get(0).path("callType").asText();

        assertEquals("INTERNAL", downward, "A->B is one schema calling itself; the call tree must say INTERNAL");
        assertEquals("INTERNAL", upward,
                "the caller list said " + upward + " for the SAME edge the call tree calls " + downward);
    }

    /** A genuinely cross-schema call must still read EXTERNAL from both directions. */
    @Test
    void realCrossSchemaCallIsExternalInBothDirections() throws Exception {
        StaticAnalysisReader r = reader("ERP", "OTHER_SCHEMA");

        String downward = r.getCallTree(ANALYSIS, "A").path("children").get(0).path("callType").asText();
        String upward = r.getCallers(ANALYSIS, "B").get(0).path("callType").asText();

        assertEquals("EXTERNAL", downward);
        assertEquals("EXTERNAL", upward, "cross-schema call must be EXTERNAL from both directions");
    }

    /** Whatever the schemas, the two directions must never disagree with each other. */
    @Test
    void directionsNeverDisagree() throws Exception {
        String[][] pairs = {
                {"ERP", "erp"}, {"ERP", "ERP"}, {"erp", "ERP"},
                {"ERP", "SALES"}, {"ANALYZER_REF", "ANALYZER_REF"}
        };
        for (String[] p : pairs) {
            StaticAnalysisReader r = reader(p[0], p[1]);
            String downward = r.getCallTree(ANALYSIS, "A").path("children").get(0).path("callType").asText();
            String upward = r.getCallers(ANALYSIS, "B").get(0).path("callType").asText();
            assertEquals(downward, upward,
                    "edge " + p[0] + "->" + p[1] + " classified as " + downward
                            + " downward but " + upward + " upward");
        }
    }
}
