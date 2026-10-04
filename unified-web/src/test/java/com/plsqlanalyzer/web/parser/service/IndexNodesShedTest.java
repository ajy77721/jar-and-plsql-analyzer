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
 * MID-002 — {@code /index} sheds {@code nodes[]} (measured at 53.1% of the response across every
 * analysis on disk, see {@link ParserFacetSpecs#PROCEDURES}'s javadoc); the sidebar and every other
 * consumer now fetch it from {@code /procedures} instead. The decision on how the sidebar's own
 * filter behaves once that split lands (threshold-based: instant client-side under 1,000 procedures,
 * server search at or above it) is a client-only concern with no server contract to test here — see
 * {@code leftPanel.js}.
 *
 * <h2>Why {@link #getIndex} keeps returning {@code nodes} unchanged</h2>
 *
 * {@link IndexFacetReader#getProcedures}, {@link ParserAnalysisReader#getCallTree}, and {@link
 * ParserAnalysisReader#getCallers} all read {@code index.get("nodes")} internally — this is not
 * dead weight to delete, it is the source {@code /procedures} itself reads from. Only the new
 * {@code /index} controller route needed to stop re-sending it.
 */
class IndexNodesShedTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ANALYSIS = "shed-nodes-fixture";

    private static InMemoryArtifactStore fixture(int nodeCount) throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ObjectNode index = M.createObjectNode();
        index.put("entryPoint", "PKG.ENTRY");
        index.put("entrySchema", "APP");
        index.put("totalNodes", nodeCount);
        ArrayNode nodes = index.putArray("nodes");
        for (int i = 0; i < nodeCount; i++) {
            ObjectNode n = nodes.addObject();
            n.put("nodeId", "N" + i);
            n.put("name", "PROC_" + i);
            n.put("schema", "APP");
        }
        var key = AnalysisPaths.resolveKey(ANALYSIS, "api/index.json");
        try (var out = store.write(key)) {
            M.writeValue(out, index);
        }
        return store;
    }

    @Test
    void indexWithoutNodesDropsOnlyTheArray() throws Exception {
        StaticAnalysisReader reader = new StaticAnalysisReader(fixture(50));

        JsonNode shed = reader.getIndexWithoutNodes(ANALYSIS);

        assertFalse(shed.has("nodes"), "nodes must be gone from the shed response");
        assertEquals("PKG.ENTRY", shed.path("entryPoint").asText());
        assertEquals("APP", shed.path("entrySchema").asText());
        assertEquals(50, shed.path("totalNodes").asInt(), "totalNodes is a scalar, not derived "
                + "from nodes.size() at read time, so dropping the array must not touch it");
    }

    @Test
    void getIndexItselfIsUnchangedForInternalConsumers() throws Exception {
        // getProcedures reads nodes off getIndex(), not getIndexWithoutNodes() — this pins that the
        // new method is additive, not a replacement that would silently break that internal read.
        StaticAnalysisReader reader = new StaticAnalysisReader(fixture(50));

        JsonNode full = reader.getIndex(ANALYSIS);
        assertTrue(full.has("nodes"), "getIndex must still carry nodes for internal callers");
        assertEquals(50, full.get("nodes").size());

        // This fixture predates objectType, so every node is a routine by the same default the
        // crawler applies; /procedures keeps them all. Entity separation for typed nodes is covered
        // by proceduresFacetKeepsOnlyRoutines below.
        JsonNode procedures = reader.getProcedures(ANALYSIS);
        assertEquals(50, procedures.size());
    }

    /** An index whose nodes carry the real entity types the crawler writes. */
    private static InMemoryArtifactStore typedFixture() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ObjectNode index = M.createObjectNode();
        index.put("entryPoint", "PKG.ENTRY");
        index.put("entrySchema", "APP");
        index.put("totalNodes", 5);
        ArrayNode nodes = index.putArray("nodes");
        nodes.addObject().put("nodeId", "S$P1").put("name", "P1").put("objectType", "PROCEDURE");
        nodes.addObject().put("nodeId", "S$P2").put("name", "P2").put("objectType", "PROCEDURE");
        nodes.addObject().put("nodeId", "S$F1").put("name", "F1").put("objectType", "FUNCTION");
        nodes.addObject().put("nodeId", "S$T1").put("name", "T1").put("objectType", "TRIGGER");
        nodes.addObject().put("nodeId", "S$TBL1").put("name", "TBL1").put("objectType", "TABLE");
        var key = AnalysisPaths.resolveKey(ANALYSIS, "api/index.json");
        try (var out = store.write(key)) {
            M.writeValue(out, index);
        }
        return store;
    }

    @Test
    void proceduresFacetKeepsOnlyRoutines() throws Exception {
        // Entity separation: TRIGGER and TABLE nodes belong to their own facets, never the
        // procedures list. A blank objectType is treated as a routine (crawler default).
        StaticAnalysisReader reader = new StaticAnalysisReader(typedFixture());

        JsonNode procedures = reader.getProcedures(ANALYSIS);
        assertEquals(3, procedures.size(), "PROCEDURE x2 + FUNCTION x1 only: " + procedures);
        for (JsonNode n : procedures) {
            String ot = n.path("objectType").asText();
            assertTrue("PROCEDURE".equals(ot) || "FUNCTION".equals(ot),
                    "a non-routine leaked into /procedures: " + ot);
        }
    }

    @Test
    void indexWithoutNodesCarriesComputedTypeCounts() throws Exception {
        // An analysis crawled before typeCounts existed still gets the truthful breakdown, computed
        // from the nodes array /index already had to load before shedding it.
        StaticAnalysisReader reader = new StaticAnalysisReader(typedFixture());

        JsonNode shed = reader.getIndexWithoutNodes(ANALYSIS);
        assertFalse(shed.has("nodes"));
        JsonNode tc = shed.path("typeCounts");
        assertEquals(2, tc.path("PROCEDURE").asInt());
        assertEquals(1, tc.path("FUNCTION").asInt());
        assertEquals(1, tc.path("TRIGGER").asInt());
        assertEquals(1, tc.path("TABLE").asInt());
    }

    @Test
    void shedResponseIsMaterialSmallerAtRealisticScale() throws Exception {
        // Not the 53.1% figure itself (that is measured against real analyses, recorded in
        // ParserFacetSpecs's javadoc) — just confirming the mechanism actually removes bytes rather
        // than, say, nulling the field and leaving `"nodes":null` in the output.
        StaticAnalysisReader reader = new StaticAnalysisReader(fixture(2000));

        byte[] full = M.writeValueAsBytes(reader.getIndex(ANALYSIS));
        byte[] shed = M.writeValueAsBytes(reader.getIndexWithoutNodes(ANALYSIS));

        assertTrue(shed.length < full.length / 2,
                "shed response (" + shed.length + " bytes) should be a small fraction of the full "
                        + "response (" + full.length + " bytes) at 2,000 nodes");
    }
}
