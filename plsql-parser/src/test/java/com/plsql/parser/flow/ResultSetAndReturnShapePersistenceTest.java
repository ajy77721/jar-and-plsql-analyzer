package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.CursorInfo;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tasks #26/#27 at the persistence boundary: a field that reaches {@link FlowNode} but is not
 * emitted by {@code ChunkedFlowWriter} reaches no artifact — and therefore no Mongo collection and
 * no screen. This pins the writer's own output for the result-set marker, the TVF/SETOF return
 * shape, and the cursors facet's "returns data" row flag.
 */
class ResultSetAndReturnShapePersistenceTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @DisplayName("#26/#27 result-set marker + return shape reach the index row and node detail")
    void resultSetAndReturnShapeArePersisted() throws Exception {
        FlowNode node = new FlowNode();
        node.setOrder(1);
        node.setDepth(0);
        node.setSchema("ERP");
        node.setObjectName("GET_ORDERS");
        node.setObjectType("FUNCTION");
        node.setReturnsResultSet(true);
        node.setResultSets(1);
        node.setReturnType("TABLE");
        node.setReturnsColumns(new ArrayList<>(List.of("order_id", "total")));
        node.generateNodeId();

        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, ArtifactKey.of("b-returns"));
        writer.writeChunk(node);
        FlowResult result = writer.mergeChunks("GET_ORDERS", "ERP", 100, 0, new ArrayList<>());
        writer.writeFinalResult(result, Map.of());

        JsonNode indexRow = readIndexRow(store, "b-returns");
        assertTrue(indexRow.path("returnsResultSet").asBoolean(), "index row must carry the marker");
        assertEquals(1, indexRow.path("resultSets").asInt());
        assertEquals("TABLE", indexRow.path("returnType").asText());
        assertEquals(List.of("order_id", "total"), toList(indexRow.path("returnsColumns")));

        JsonNode detail = readNodeDetail(store, "b-returns", indexRow.path("nodeId").asText());
        assertTrue(detail.path("returnsResultSet").asBoolean(), "node detail must carry the marker");
        assertEquals("TABLE", detail.path("returnType").asText());
        assertEquals(List.of("order_id", "total"), toList(detail.path("returnsColumns")));
    }

    @Test
    @DisplayName("#26 a cursor row carries the owning node's returnsData marker")
    void cursorRowCarriesReturnsData() throws Exception {
        FlowNode node = new FlowNode();
        node.setOrder(1);
        node.setDepth(0);
        node.setSchema("ERP");
        node.setObjectName("FETCH_ALL");
        node.setObjectType("PROCEDURE");
        node.setReturnsResultSet(true);
        node.setResultSets(1);
        CursorInfo cur = new CursorInfo();
        cur.setName("C_ALL");
        cur.setLine(3);
        node.setCursors(List.of(cur));
        node.generateNodeId();

        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, ArtifactKey.of("b-cursor"));
        writer.writeChunk(node);
        FlowResult result = writer.mergeChunks("FETCH_ALL", "ERP", 100, 0, new ArrayList<>());
        writer.writeFinalResult(result, Map.of());

        JsonNode indexRow = readIndexRow(store, "b-cursor");
        JsonNode detail = readNodeDetail(store, "b-cursor", indexRow.path("nodeId").asText());
        JsonNode cursor = detail.path("cursors").get(0);
        assertTrue(cursor.path("returnsData").asBoolean(),
                "the cursors facet reads this flag to surface 'returns data'");
        assertEquals(1, cursor.path("resultSets").asInt());
    }

    private static List<String> toList(JsonNode arr) {
        List<String> out = new ArrayList<>();
        if (arr != null && arr.isArray()) arr.forEach(n -> out.add(n.asText()));
        return out;
    }

    private static JsonNode readIndexRow(InMemoryArtifactStore store, String buildId) throws Exception {
        try (var in = store.read(ArtifactKey.of(buildId, "api", "index.json"))) {
            return JSON.readTree(in).get("nodes").get(0);
        }
    }

    private static JsonNode readNodeDetail(InMemoryArtifactStore store, String buildId, String nodeId)
            throws Exception {
        ArtifactKey nodesDir = ArtifactKey.of(buildId, "api", "nodes");
        try (var in = store.read(nodesDir.resolve(nodeId.replace('/', '_') + ".json"))) {
            return JSON.readTree(in);
        }
    }
}
