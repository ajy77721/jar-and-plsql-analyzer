package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import com.plsql.parser.model.TableOperationInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The concurrent save pass ({@code ChunkedFlowWriter.writeApiFiles}) must not change a single output
 * byte. The node-detail files and the facet indexes are now written on a bounded pool, but the
 * layout manifest — whose insertion order IS part of its bytes — is registered only after every task
 * has joined, in the writer's original order. This pins that contract: two identical saves produce
 * the same artifacts, every node detail file is still emitted, and the manifest's {@code api} group
 * keeps the sequential order ({@code index.json}, {@code call_graph.json}, {@code node_id_map.json}).
 *
 * <p>{@code api/manifest.json} itself is excluded from the byte comparison because it carries a
 * wall-clock {@code generatedAt} and per-key {@code lastModified} values; its structure and ordering
 * are asserted instead.
 */
class ParallelSaveDeterminismTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ArtifactKey BUILD = ArtifactKey.of("plsql-parse", "DB", "PROJ", "ENV", "OBJ");
    private static final String MANIFEST_KEY = BUILD.resolve("api", "manifest.json").toString();

    private static FlowNode node(int i) {
        FlowNode n = new FlowNode();
        n.setNodeId("APP$PKG$P" + i);
        n.setOrder(i);
        n.setDepth(i % 4);
        n.setSchema("APP");
        n.setPackageName("PKG");
        n.setObjectName("P" + i);
        n.setObjectType("PROCEDURE");
        n.setLinesOfCode(3 + i);
        n.setReadable(true);
        TableOperationInfo op = new TableOperationInfo();
        op.setSchema("APP");
        op.setTableName("T" + (i % 5));
        op.setOperation("SELECT");
        op.setLine(i + 1);
        n.setTableOperations(List.of(op));
        return n;
    }

    /** One full save into a fresh store; returns every artifact's bytes keyed by path. */
    private static Map<String, byte[]> runSave(int nodeCount) throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, BUILD);
        for (int i = 0; i < nodeCount; i++) writer.writeChunk(node(i));

        FlowResult result = new FlowResult();
        result.setEntryPoint("APP.PKG.P0");
        result.setEntrySchema("APP");
        result.setCrawlTimeMs(123);
        writer.mergeChunks("APP.PKG.P0", "APP", 123, 3, new ArrayList<>());
        writer.writeFinalResult(result, Map.of());

        Map<String, byte[]> out = new TreeMap<>();
        for (String k : store.keys()) {
            try (InputStream in = store.read(ArtifactKey.of(k.split("/")))) {
                out.put(k, in.readAllBytes());
            }
        }
        return out;
    }

    @Test
    void concurrentSaveIsByteIdenticalAcrossRunsAndPreservesManifestOrder() throws Exception {
        int nodes = 25; // > LayoutManifest.MAX_GROUP, so the node detail family collapses
        Map<String, byte[]> first = runSave(nodes);
        Map<String, byte[]> second = runSave(nodes);

        assertEquals(first.keySet(), second.keySet(), "same artifact set");
        for (String k : first.keySet()) {
            if (k.equals(MANIFEST_KEY)) continue; // carries generatedAt/lastModified
            assertArrayEquals(first.get(k), second.get(k), "bytes differ for " + k);
        }

        // Every node detail file was emitted despite the concurrent pool.
        long details = first.keySet().stream()
                .filter(k -> k.startsWith(BUILD.resolve("api", "nodes").toString() + "/"))
                .filter(k -> k.endsWith(".json")).count();
        assertEquals(nodes, details, "one detail file per node");

        // The manifest's api-group listing keeps the sequential order — the invariant the pool
        // threads must not be allowed to reorder.
        JsonNode man = JSON.readTree(first.get(MANIFEST_KEY));
        List<String> apiKeys = new ArrayList<>();
        for (JsonNode a : man.get("artifacts")) {
            String k = a.get("key").asText();
            if (k.startsWith(BUILD.resolve("api").toString() + "/")
                    && !k.contains("/nodes/") && !k.contains("/tables/")
                    && !k.contains("/statements/") && !k.contains("/jobs/")) {
                apiKeys.add(k);
            }
        }
        assertEquals(List.of(
                BUILD.resolve("api", "index.json").toString(),
                BUILD.resolve("api", "call_graph.json").toString(),
                BUILD.resolve("api", "node_id_map.json").toString()), apiKeys);

        assertEquals(nodes, man.get("counts").get("nodes").asInt());
    }
}
