package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.CursorInfo;
import com.plsql.parser.model.FlowNode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MID-026 — calibration for the {@code LightNode} projection {@code writeTableIndex} now reads
 * (`ChunkedFlowWriter.toLightNode`'s own doc comment): confirms the {@code cursors} field survives
 * the projection, since a table referenced *only* through a cursor's own {@code tables[]} list
 * (never through a {@code TableOperationInfo}) is the one real case that field alone accounts for —
 * §168's own field enumeration missed it, caught only while implementing, not investigating.
 *
 * <p>Calibrated by dropping {@code cursors} from the projection (passing {@code null} in place of
 * {@code n.getCursors()}) and confirming this table silently disappears from {@code tables/
 * index.json} — the exact wrong output a lossy projection would produce — before restoring.
 */
class TableIndexCursorReferenceTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void tableReferencedOnlyThroughACursorStillAppearsInTheIndex() throws Exception {
        FlowNode node = new FlowNode();
        node.setNodeId("ERP$USP_TEST");
        node.setSchema("ERP");
        node.setObjectName("USP_TEST");
        node.setObjectType("PROCEDURE");
        node.setDepth(0);

        CursorInfo cursor = new CursorInfo();
        cursor.setName("c_orders");
        cursor.setLine(10);
        cursor.setTables(List.of("ERP_ORDER"));
        node.setCursors(List.of(cursor));
        // Deliberately no TableOperationInfo at all — ERP_ORDER is reachable only via the cursor.

        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey build = ArtifactKey.of("build1");
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, build);
        ArtifactKey apiDir = ArtifactKey.of("build1", "api");

        Method toLightNodes = ChunkedFlowWriter.class.getDeclaredMethod("toLightNodes", List.class);
        toLightNodes.setAccessible(true);
        Object lightNodes = toLightNodes.invoke(null, List.of(node));

        Method m = ChunkedFlowWriter.class.getDeclaredMethod(
                "writeTableIndex", ArtifactKey.class, List.class);
        m.setAccessible(true);
        m.invoke(writer, apiDir, lightNodes);

        JsonNode tables;
        try (var in = store.read(apiDir.resolve("tables", "index.json"))) {
            tables = JSON.readTree(in).get("tables");
        }

        assertEquals(1, tables.size(), "the cursor-only-referenced table must still appear");
        assertTrue(tables.get(0).get("name").asText().equalsIgnoreCase("ERP_ORDER"),
                "the table found via the cursor's own tables[] must be the one indexed");
    }
}
