package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Task #29 — the nested marker must survive all the way into {@code api/index.json}, which is
 * exactly what the procedures facet reads ({@code IndexFacetReader.getProcedures} returns the
 * index {@code nodes[]} array). Proves the visitor→FlowNode→index projection path, and that an
 * ordinary (non-nested) node's index row is unchanged.
 */
class NestedMarkerPersistenceTest {

    private static final ObjectMapper M = new ObjectMapper();

    @TempDir
    Path tempDir;

    private JsonNode readJson(LocalArtifactStore store, ArtifactKey key) throws Exception {
        try (InputStream in = store.read(key)) {
            return M.readTree(in);
        }
    }

    private JsonNode byObject(JsonNode nodes, String objectName) {
        for (JsonNode n : nodes) {
            if (objectName.equalsIgnoreCase(n.path("objectName").asText())) return n;
        }
        return null;
    }

    @Test
    @DisplayName("#29: nested=true/scopeDepth reach api/index.json (procedures facet source)")
    void nestedMarkerReachesIndexProjection() throws Exception {
        ArtifactKey key = ArtifactKey.of("test-build");
        LocalArtifactStore store = new LocalArtifactStore(tempDir);
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, key);

        FlowNode outer = new FlowNode();
        outer.setOrder(1);
        outer.setDepth(0);
        outer.setSchema("S");
        outer.setPackageName("PKG");
        outer.setObjectName("OUTER_PROC");
        outer.setObjectType("PROCEDURE");
        outer.setLineStart(3);
        outer.setLineEnd(10);
        outer.generateNodeId();

        FlowNode nested = new FlowNode();
        nested.setOrder(2);
        nested.setDepth(0);
        nested.setSchema("S");
        nested.setPackageName("PKG");
        nested.setObjectName("LOCAL_HELPER");
        nested.setObjectType("PROCEDURE");
        nested.setLineStart(4);
        nested.setLineEnd(6);
        nested.setNested(true);
        nested.setScopeDepth(1);
        nested.generateNodeId();

        writer.writeChunk(outer);
        writer.writeChunk(nested);
        FlowResult result = writer.mergeChunks("PKG", "S", 0, 0, List.of());
        // api/** (index.json) is written by writeFinalResult, not mergeChunks.
        writer.writeFinalResult(result, java.util.Map.of());

        JsonNode index = readJson(store, key.resolve("api", "index.json"));
        JsonNode nodes = index.get("nodes");
        assertNotNull(nodes, "index.json must carry nodes[]");

        JsonNode nestedRow = byObject(nodes, "LOCAL_HELPER");
        assertNotNull(nestedRow, "nested node present in index");
        assertTrue(nestedRow.path("nested").asBoolean(), "nested badge reaches the index");
        assertEquals(1, nestedRow.path("scopeDepth").asInt());

        JsonNode outerRow = byObject(nodes, "OUTER_PROC");
        assertNotNull(outerRow, "top-level node present in index");
        assertFalse(outerRow.has("nested"), "non-nested index row stays byte-identical (no badge)");
        assertFalse(outerRow.has("scopeDepth"));
    }
}
