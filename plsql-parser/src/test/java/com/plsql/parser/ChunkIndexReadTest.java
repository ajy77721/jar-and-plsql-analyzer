package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.plsql.parser.flow.ChunkedFlowWriter;
import com.plsql.parser.model.FlowNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-026 — proves {@code ChunkedFlowWriter.buildChunkIndex}/{@code readNodeChunk} (the per-chunk
 * re-read primitive the 2 remaining bounded-conversion pieces need) reproduce exactly what
 * {@code mergeChunks}'s own full read already produces for the same node, without requiring the
 * full node list to stay resident to look one up.
 *
 * <p>Package-private methods, called via reflection from this same package rather than widened —
 * this primitive has no other caller yet (deliberately: proving it in isolation before the 2
 * loops that will use it, per this session's own bounded-increment discipline).
 */
@DisplayName("ChunkedFlowWriter per-chunk re-read primitive")
public class ChunkIndexReadTest {

    private ChunkedFlowWriter writer;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup() throws IOException {
        writer = new ChunkedFlowWriter(new LocalArtifactStore(tempDir), ArtifactKey.of("test-build"));
    }

    private FlowNode node(String id, String objectName, int loc) {
        FlowNode n = new FlowNode();
        n.setOrder(1);
        n.setDepth(0);
        n.setObjectName(objectName);
        n.setNodeId(id);
        n.setLinesOfCode(loc);
        return n;
    }

    @SuppressWarnings("unchecked")
    private Map<String, ArtifactKey> buildChunkIndex() throws Exception {
        Method m = ChunkedFlowWriter.class.getDeclaredMethod("buildChunkIndex");
        m.setAccessible(true);
        return (Map<String, ArtifactKey>) m.invoke(writer);
    }

    private FlowNode readNodeChunk(String nodeId, Map<String, ArtifactKey> index) throws Exception {
        Method m = ChunkedFlowWriter.class.getDeclaredMethod("readNodeChunk", String.class, Map.class);
        m.setAccessible(true);
        return (FlowNode) m.invoke(writer, nodeId, index);
    }

    @Test
    @DisplayName("reads the same body mergeChunks' own full read produces, for a chosen node")
    void reproducesTheFullReadForOneNode() throws Exception {
        writer.writeChunk(node("ERP$P1", "P1", 100));
        writer.writeChunk(node("ERP$P2", "P2", 200));
        writer.writeChunk(node("ERP$P3", "P3", 300));

        var full = writer.mergeChunks("ERP$P1", "ERP", 0, 0, java.util.List.of());
        FlowNode expected = full.getFlowNodes().stream()
                .filter(n -> "ERP$P2".equals(n.getNodeId())).findFirst().orElseThrow();

        Map<String, ArtifactKey> index = buildChunkIndex();
        FlowNode actual = readNodeChunk("ERP$P2", index);

        assertNotNull(actual);
        assertEquals(expected.getNodeId(), actual.getNodeId());
        assertEquals(expected.getObjectName(), actual.getObjectName());
        assertEquals(expected.getLinesOfCode(), actual.getLinesOfCode());
    }

    @Test
    @DisplayName("index covers every written chunk, keyed by its own nodeId")
    void indexCoversEveryChunk() throws Exception {
        writer.writeChunk(node("A", "OA", 1));
        writer.writeChunk(node("B", "OB", 2));

        Map<String, ArtifactKey> index = buildChunkIndex();
        assertEquals(2, index.size());
        assertTrue(index.containsKey("A"));
        assertTrue(index.containsKey("B"));
    }

    @Test
    @DisplayName("a dangling id returns null, not an exception")
    void danglingIdReturnsNull() throws Exception {
        writer.writeChunk(node("A", "OA", 1));
        Map<String, ArtifactKey> index = buildChunkIndex();
        assertNull(readNodeChunk("NONEXISTENT", index));
    }
}
