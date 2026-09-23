package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.TableOperationInfo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@code operation -> [lines]} map every table emitter writes ({@code {"SELECT":[12,88],
 * "INSERT":[40]}}) cannot express the order the accesses actually execute in — a table read at 12,
 * written at 40 and read again at 88 reads back as SELECT(12), SELECT(88), INSERT(40) when walked
 * operation-by-operation. unified-web's {@code ParserPredicatesAdapter} had to reconstruct the true
 * order by merge-sorting that map after the fact; this pins the upstream fix, the sibling
 * {@code accessSequence} list every emitter of the grouped map now also writes.
 *
 * <p>The grouped {@code operations} map itself is unchanged and still asserted here — the UI's
 * per-table "operations" rendering depends on that shape.
 */
class TableOperationTrueOrderTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The proven interleaved pattern: SELECT@12, INSERT@40, SELECT@88 on one table. */
    private static List<TableOperationInfo> interleaved() {
        return List.of(op("ERP_STOCK_LEVEL", "SELECT", 12),
                op("ERP_STOCK_LEVEL", "INSERT", 40),
                op("ERP_STOCK_LEVEL", "SELECT", 88));
    }

    private static TableOperationInfo op(String table, String operation, int line) {
        TableOperationInfo t = new TableOperationInfo();
        t.setTableName(table);
        t.setSchema("ERP");
        t.setOperation(operation);
        t.setLine(line);
        t.setObjectType("TABLE");
        return t;
    }

    private static List<String> sequence(JsonNode holder) {
        JsonNode seq = holder.get("accessSequence");
        assertNotNull(seq, "accessSequence must be emitted alongside operations: " + holder);
        List<String> out = new ArrayList<>();
        for (JsonNode e : seq) out.add(e.get("operation").asText() + "@" + e.get("line").asInt());
        return out;
    }

    // ── writeTableIndex: usedBy[] — the artifact ParserPredicatesAdapter reads ──

    @Test
    void tableIndexUsedByCarriesTrueLineOrderAcrossOperationTypes() throws Exception {
        FlowNode node = new FlowNode();
        node.setObjectName("P_RESTOCK");
        node.setDepth(0);
        node.setTableOperations(interleaved());

        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey build = ArtifactKey.of("b-order-1");
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, build);
        ArtifactKey apiDir = ArtifactKey.of("b-order-1", "api");

        Method toLightNodes = ChunkedFlowWriter.class.getDeclaredMethod("toLightNodes", List.class);
        toLightNodes.setAccessible(true);
        Object lightNodes = toLightNodes.invoke(null, List.of(node));
        Method m = ChunkedFlowWriter.class.getDeclaredMethod(
                "writeTableIndex", ArtifactKey.class, List.class);
        m.setAccessible(true);
        m.invoke(writer, apiDir, lightNodes);

        JsonNode doc;
        try (var in = store.read(apiDir.resolve("tables", "index.json"))) {
            doc = JSON.readTree(in);
        }
        JsonNode usage = doc.get("tables").get(0).get("usedBy").get(0);

        // The grouped map is preserved exactly as it was — existing consumers depend on it.
        JsonNode ops = usage.get("operations");
        assertEquals(2, ops.get("SELECT").size());
        assertEquals(12, ops.get("SELECT").get(0).asInt());
        assertEquals(88, ops.get("SELECT").get(1).asInt());
        assertEquals(40, ops.get("INSERT").get(0).asInt());

        // ...and the true execution order is now derivable straight from the writer's output.
        assertEquals(List.of("SELECT@12", "INSERT@40", "SELECT@88"), sequence(usage));
    }

    // ── groupTableOps: node detail `tables` / `directTables` ──

    @Test
    @SuppressWarnings("unchecked")
    void groupTableOpsCarriesTrueLineOrder() throws Exception {
        ChunkedFlowWriter writer = new ChunkedFlowWriter(
                new InMemoryArtifactStore(), ArtifactKey.of("b-order-2"));
        Method m = ChunkedFlowWriter.class.getDeclaredMethod("groupTableOps", List.class);
        m.setAccessible(true);
        List<Map<String, Object>> grouped = (List<Map<String, Object>>) m.invoke(writer, interleaved());

        assertEquals(1, grouped.size());
        Map<String, Object> table = grouped.get(0);
        Map<String, List<Integer>> ops = (Map<String, List<Integer>>) table.get("operations");
        assertEquals(List.of(12, 88), ops.get("SELECT"));
        assertEquals(List.of(40), ops.get("INSERT"));

        assertEquals(List.of("SELECT@12", "INSERT@40", "SELECT@88"),
                sequence(JSON.valueToTree(table)));
    }

    // ── buildSubtreeTables: node detail `subtreeTables` ──

    @Test
    @SuppressWarnings("unchecked")
    void subtreeTablesCarryTrueLineOrderPerNodeAndAggregated() throws Exception {
        FlowNode caller = new FlowNode();
        caller.setObjectName("P_RESTOCK");
        caller.setDepth(0);
        caller.setTableOperations(interleaved());
        FlowNode callee = new FlowNode();
        callee.setObjectName("P_AUDIT");
        callee.setDepth(1);
        callee.setTableOperations(List.of(op("ERP_STOCK_LEVEL", "UPDATE", 50)));
        caller.generateNodeId();
        callee.generateNodeId();

        Method toLightNodes = ChunkedFlowWriter.class.getDeclaredMethod("toLightNodes", List.class);
        toLightNodes.setAccessible(true);
        List<Object> lightNodes = (List<Object>) toLightNodes.invoke(null, List.of(caller, callee));

        Method nodeIdOf = lightNodes.get(0).getClass().getDeclaredMethod("nodeId");
        nodeIdOf.setAccessible(true);
        java.util.Map<String, Object> nodeIndex = new java.util.LinkedHashMap<>();
        for (Object ln : lightNodes) nodeIndex.put((String) nodeIdOf.invoke(ln), ln);
        List<String> ids = new ArrayList<>(nodeIndex.keySet());

        ChunkedFlowWriter writer = new ChunkedFlowWriter(
                new InMemoryArtifactStore(), ArtifactKey.of("b-order-3"));
        Method m = ChunkedFlowWriter.class.getDeclaredMethod(
                "buildSubtreeTables", String.class, Set.class, Map.class);
        m.setAccessible(true);
        List<Map<String, Object>> tables = (List<Map<String, Object>>) m.invoke(
                writer, ids.get(0), Set.of(ids.get(1)), nodeIndex);

        assertEquals(1, tables.size());
        Map<String, Object> table = tables.get(0);
        // Aggregated across the subtree, in true line order across BOTH nodes and all op types.
        assertEquals(List.of("SELECT@12", "INSERT@40", "UPDATE@50", "SELECT@88"),
                sequence(JSON.valueToTree(table)));
        // Grouped shape still intact for the existing consumer.
        Map<String, List<Integer>> ops = (Map<String, List<Integer>>) table.get("operations");
        assertEquals(List.of(12, 88), ops.get("SELECT"));
        assertEquals(List.of(50), ops.get("UPDATE"));

        // ...and per contributing node.
        List<Map<String, Object>> usedByNodes = (List<Map<String, Object>>) table.get("usedByNodes");
        assertEquals(List.of("SELECT@12", "INSERT@40", "SELECT@88"),
                sequence(JSON.valueToTree(usedByNodes.get(0))));
    }

    @Test
    @SuppressWarnings("unchecked")
    void accessesSharingALineKeepTheGroupedMapsOperationOrder() throws Exception {
        ChunkedFlowWriter writer = new ChunkedFlowWriter(
                new InMemoryArtifactStore(), ArtifactKey.of("b-order-4"));
        Method m = ChunkedFlowWriter.class.getDeclaredMethod("groupTableOps", List.class);
        m.setAccessible(true);
        // A MERGE-style single line recorded as both a SELECT and an UPDATE at line 30.
        List<Map<String, Object>> grouped = (List<Map<String, Object>>) m.invoke(writer,
                List.of(op("ERP_STOCK_LEVEL", "SELECT", 30),
                        op("ERP_STOCK_LEVEL", "UPDATE", 30),
                        op("ERP_STOCK_LEVEL", "SELECT", 5)));
        assertEquals(List.of("SELECT@5", "SELECT@30", "UPDATE@30"),
                sequence(JSON.valueToTree(grouped.get(0))),
                "stable sort: ties keep the grouped map's own operation order");
    }

    @Test
    @SuppressWarnings("unchecked")
    void emptyTableOperationsProduceNoTables() throws Exception {
        ChunkedFlowWriter writer = new ChunkedFlowWriter(
                new InMemoryArtifactStore(), ArtifactKey.of("b-order-5"));
        Method m = ChunkedFlowWriter.class.getDeclaredMethod("groupTableOps", List.class);
        m.setAccessible(true);
        List<Map<String, Object>> grouped =
                (List<Map<String, Object>>) m.invoke(writer, Collections.emptyList());
        assertTrue(grouped.isEmpty());
    }
}
