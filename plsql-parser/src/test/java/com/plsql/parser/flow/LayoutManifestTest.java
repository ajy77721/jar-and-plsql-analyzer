package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowEdge;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import com.plsql.parser.model.TableOperationInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plan 1f — the layout manifest must inventory every artifact the writer emitted, with the correct
 * length, built during the write pass and not by re-reading anything.
 */
class LayoutManifestTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final ArtifactKey BUILD =
            ArtifactKey.of("plsql-parse", "DB", "PROJ", "ENV", "OBJ");

    private static FlowNode node(String nodeId, int order) {
        FlowNode n = new FlowNode();
        n.setNodeId(nodeId);
        n.setOrder(order);
        n.setDepth(0);
        n.setSchema("APP");
        n.setPackageName("PKG");
        n.setObjectName(nodeId.substring(nodeId.lastIndexOf('$') + 1));
        n.setLinesOfCode(3);
        n.setReadable(true);
        return n;
    }

    private static long sizeOf(ArtifactStore store, String key) {
        return store.size(ArtifactKey.of(key.split("/")));
    }

    private static JsonNode manifest(InMemoryArtifactStore store) throws IOException {
        return JSON.readTree(store.read(BUILD.resolve("api", "manifest.json")));
    }

    @Test
    @DisplayName("every emitted artifact appears with its correct byte length")
    void everyEmittedArtifactIsInventoried() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, BUILD);

        // 20 chunks: more than MAX_GROUP, so the family must be summarised, not listed one by one.
        for (int i = 0; i < 20; i++) writer.writeChunk(node("APP$PKG$P" + i, i));
        writer.writeSourceFile("APP.PKG.P0.sql", "BEGIN NULL; END;");
        FlowEdge edge = new FlowEdge();
        edge.setFromNodeId("APP$PKG$P0");
        edge.setToNodeId("APP$PKG$P1");
        writer.appendEdges(List.of(edge), 1);
        writer.updateTables(Map.of());
        writer.writeManifest();

        JsonNode man = manifest(store);
        String manifestKey = BUILD.resolve("api", "manifest.json").toString();

        // Per-key entries carry a length that matches the store exactly.
        Set<String> listed = new HashSet<>();
        for (JsonNode a : man.get("artifacts")) {
            String k = a.get("key").asText();
            listed.add(k);
            assertEquals(sizeOf(store, k), a.get("length").asLong(), "length wrong for " + k);
        }

        // Collection aggregates must account for exactly the members under their prefix.
        Set<String> inCollections = new HashSet<>();
        for (JsonNode c : man.get("collections")) {
            String prefix = c.get("prefix").asText();
            int count = 0;
            long bytes = 0;
            for (String k : store.keys()) {
                if (k.startsWith(prefix)) {
                    count++;
                    bytes += sizeOf(store, k);
                    inCollections.add(k);
                }
            }
            assertEquals(count, c.get("count").asInt(), "count wrong for " + prefix);
            assertEquals(bytes, c.get("totalBytes").asLong(), "bytes wrong for " + prefix);
        }

        // Completeness: nothing the writer emitted is missing from the manifest.
        for (String k : store.keys()) {
            if (k.equals(manifestKey)) continue;
            assertTrue(listed.contains(k) || inCollections.contains(k),
                    "artifact written but not inventoried: " + k);
        }

        // The 20-chunk family collapsed into one aggregate (the manifest must stay KBs).
        JsonNode chunks = null;
        for (JsonNode c : man.get("collections")) {
            if (c.get("prefix").asText().endsWith("/chunks/")) chunks = c;
        }
        assertNotNull(chunks, "chunk family was not summarised");
        assertEquals(20, chunks.get("count").asInt());

        // The sidecar inventory is advertised, not probed.
        assertTrue(man.get("sidecars").size() >= 9, "expected sidecar list");
    }

    @Test
    @DisplayName("writeFinalResult writes a manifest last, carrying node/statement counts and the offsets file")
    void finalResultWritesManifestWithCounts() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, BUILD);

        FlowNode n = node("APP$PKG$P", 0);
        TableOperationInfo op = new TableOperationInfo();
        op.setTableName("ERP_PRODUCT");
        op.setSchema("APP");
        op.setOperation("SELECT");
        op.setLine(7);
        n.setTableOperations(List.of(op));
        writer.writeChunk(n);

        FlowResult result = new FlowResult();
        result.setEntryPoint("APP.PKG.P");
        result.setEntrySchema("APP");
        writer.writeFinalResult(result, Map.of());

        JsonNode man = manifest(store);
        assertEquals(1, man.get("counts").get("nodes").asInt());
        assertEquals(1, man.get("counts").get("statements").asInt());

        JsonNode indexEntry = null;
        for (JsonNode a : man.get("artifacts")) {
            if (a.get("key").asText().endsWith("api/index.json")) indexEntry = a;
        }
        assertNotNull(indexEntry, "api/index.json missing from the manifest");
        assertEquals(1, indexEntry.get("rows").asInt());
        assertEquals(sizeOf(store, BUILD.resolve("api", "index.json").toString()),
                indexEntry.get("length").asLong());

        // The offset index is written alongside the statements index.
        assertTrue(store.exists(BUILD.resolve("api", "statements", "index.offsets.json")));
    }

    @Test
    @DisplayName("the manifest carries the index root's listing scalars, with nodes[] skipped")
    void manifestCarriesListingScalars() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, BUILD);

        writer.writeChunk(node("APP$PKG$P", 0));
        FlowResult result = new FlowResult();
        result.setEntryPoint("APP.PKG.P");
        result.setEntrySchema("APP");
        result.setCrawlTimeMs(1234);
        result.setTotalLinesOfCode(42);
        result.setMaxDepthReached(2);
        result.setDbCallCount(3);
        result.setErrors(List.of("warn1"));
        writer.writeFinalResult(result, Map.of());

        JsonNode listing = manifest(store).get("listing");
        assertNotNull(listing, "the manifest must carry a listing object");
        assertEquals("APP.PKG.P", listing.get("entryPoint").asText());
        assertEquals("APP", listing.get("entrySchema").asText());
        assertEquals(1, listing.get("totalNodes").asInt());
        assertEquals(42, listing.get("totalLinesOfCode").asInt());
        assertEquals(2, listing.get("maxDepth").asInt());
        assertEquals(1234, listing.get("crawlTimeMs").asLong());
        assertEquals(3, listing.get("dbCallCount").asInt());
        assertEquals(1, listing.get("errors").size());
        // The listing is scalars only — the nodes[] array (53% of a large index) must not be in it.
        assertFalse(listing.has("nodes"), "the nodes[] array must not be copied into the listing");
    }
}
