package com.plsql.parser;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.flow.ChunkedFlowWriter;
import com.plsql.parser.model.ExceptionHandlerInfo;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression test for the total-data-loss bug: a crawled routine with a NESTED exception handler
 * (one handler's protected block physically inside another handler's EXCEPTION section) set
 * {@code ExceptionHandlerInfo.parentHandler}, whose only serializable trace was the getter-only
 * derived property {@code getParentExceptionName()}. Jackson's default bean introspection
 * serialized that getter into chunk JSON despite it having no backing field, and
 * {@code ChunkedFlowWriter.mergeChunks}'s plain {@code ObjectMapper} (default
 * {@code FAIL_ON_UNKNOWN_PROPERTIES=true}) then threw {@code UnrecognizedPropertyException} on
 * read-back — with NO per-chunk try/catch, aborting the entire merge and silently discarding every
 * node already accumulated (a real crawl of 244 tables ended with "Objects crawled: 0",
 * "Call graph edges: 0", "Errors: 0").
 *
 * <p>Fixed by: (1) {@code @JsonIgnore} on {@code getParentExceptionName()} itself so it is never
 * serialized in the first place, (2) {@code @JsonIgnoreProperties(ignoreUnknown = true)} on
 * {@code FlowNode}/{@code ExceptionHandlerInfo} as defense in depth for any future unknown field,
 * and (3) a per-chunk try/catch in {@code mergeChunks} so one bad chunk degrades to "missing that
 * one node" instead of "missing everything".
 */
@DisplayName("Nested exception handler chunk round-trip (data-loss regression)")
public class ExceptionHandlerChunkRoundTripTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private FlowNode nodeWithNestedHandler(String nodeId, String objectName) {
        ExceptionHandlerInfo outer = new ExceptionHandlerInfo();
        outer.setExceptionName("OTHERS");
        outer.setLine(10);
        outer.setLineEnd(30);

        // The nested handler: its protected block sits inside the outer handler's EXCEPTION
        // section, so parentHandler is non-null — exactly the shape that used to leak
        // parentExceptionName into chunk JSON.
        ExceptionHandlerInfo nested = new ExceptionHandlerInfo();
        nested.setExceptionName("NO_DATA_FOUND");
        nested.setLine(15);
        nested.setLineEnd(20);
        nested.setParentHandler(outer);

        FlowNode node = new FlowNode();
        node.setOrder(1);
        node.setDepth(0);
        node.setSchema("SCHEMA");
        node.setObjectName(objectName);
        node.setObjectType("PROCEDURE");
        node.setNodeId(nodeId);
        node.setExceptionHandlers(List.of(outer, nested));
        return node;
    }

    // ── (a) direct serialize/deserialize proves fix #1 + #2 ──

    @Test
    @DisplayName("a nested handler's parentExceptionName is not serialized (JsonIgnore)")
    void parentExceptionNameNotSerialized() throws IOException {
        ExceptionHandlerInfo outer = new ExceptionHandlerInfo();
        outer.setExceptionName("OTHERS");
        ExceptionHandlerInfo nested = new ExceptionHandlerInfo();
        nested.setExceptionName("NO_DATA_FOUND");
        nested.setParentHandler(outer);

        // Sanity: the derived getter itself still works at the Java level.
        assertEquals("OTHERS", nested.getParentExceptionName());

        String json = mapper.writeValueAsString(nested);
        assertFalse(json.contains("parentExceptionName"),
                "parentExceptionName must never be serialized — JSON was: " + json);
    }

    @Test
    @DisplayName("FlowNode with a nested exception handler serializes and deserializes without throwing")
    void nestedHandlerFlowNodeRoundTrips() throws IOException {
        FlowNode node = nodeWithNestedHandler("SCHEMA$PROC", "PROC");

        String json = mapper.writeValueAsString(node);
        assertFalse(json.contains("parentExceptionName"), "JSON was: " + json);

        FlowNode roundTripped = assertDoesNotThrow(() -> mapper.readValue(json, FlowNode.class),
                "Reading back a chunk with a nested exception handler must not throw "
                        + "UnrecognizedPropertyException");
        assertEquals(2, roundTripped.getExceptionHandlers().size());
    }

    // ── (a, continued) the real ChunkedFlowWriter write/merge path, not just a raw mapper ──

    @Test
    @DisplayName("ChunkedFlowWriter: a chunk with a nested exception handler merges successfully")
    void chunkedFlowWriterMergesNestedHandlerChunk() throws IOException {
        ChunkedFlowWriter writer = new ChunkedFlowWriter(
                new LocalArtifactStore(tempDir), ArtifactKey.of("test-build"));

        writer.writeChunk(nodeWithNestedHandler("SCHEMA$PROC", "PROC"));

        FlowResult result = assertDoesNotThrow(
                () -> writer.mergeChunks("PROC", "SCHEMA", 100, 0, new ArrayList<>()),
                "mergeChunks must not throw / silently return an empty result when a chunk "
                        + "contains a nested exception handler");

        assertEquals(1, result.getFlowNodes().size());
        assertEquals(1, result.getTotalObjectsCrawled());
        assertTrue(result.getErrors().isEmpty());
    }

    // ── (b) one corrupt chunk among several good ones: the good ones still merge ──

    @Test
    @DisplayName("mergeChunks skips one corrupt chunk and still returns the good nodes")
    void mergeSkipsCorruptChunkKeepsGoodNodes() throws IOException {
        LocalArtifactStore store = new LocalArtifactStore(tempDir);
        ArtifactKey buildKey = ArtifactKey.of("test-build");
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, buildKey);

        // Three good chunks, written the normal way (chunk_0001/2/3.json) — writer's own
        // AtomicInteger counter picks the next filename regardless of anything already on disk,
        // so the corrupt chunk below must use a name outside that sequence or a later writeChunk
        // call would silently overwrite it.
        writer.writeChunk(nodeWithNestedHandler("SCHEMA$P1", "P1"));
        writer.writeChunk(nodeWithNestedHandler("SCHEMA$P2", "P2"));
        writer.writeChunk(nodeWithNestedHandler("SCHEMA$P3", "P3"));

        // A corrupt chunk sorting BEFORE all three (so it is read first, proving the loop keeps
        // going and picks up every good chunk that follows): invalid JSON that will throw on
        // readValue, simulating a stale/incompatible chunk file left over from before today's fix
        // (or any future field mismatch).
        ArtifactKey corruptChunk = buildKey.resolve("chunks").resolve("chunk_0000.json");
        try (var out = store.write(corruptChunk)) {
            out.write("{ this is not valid json".getBytes(StandardCharsets.UTF_8));
        }

        List<String> errors = new ArrayList<>();
        FlowResult result = assertDoesNotThrow(
                () -> writer.mergeChunks("P1", "SCHEMA", 100, 0, errors),
                "one corrupt chunk must not abort the whole merge");

        List<String> ids = result.getFlowNodes().stream().map(FlowNode::getNodeId).toList();
        assertEquals(3, result.getFlowNodes().size(),
                "expected all 3 good nodes despite 1 corrupt chunk, got: " + ids);
        assertTrue(ids.contains("SCHEMA$P1"));
        assertTrue(ids.contains("SCHEMA$P2"));
        assertTrue(ids.contains("SCHEMA$P3"));
        assertEquals(3, result.getTotalObjectsCrawled());

        // The corrupt chunk must be visible in errors, not silently swallowed (Errors: 0 is what
        // hid the original incident).
        assertFalse(errors.isEmpty(), "the corrupt chunk should be recorded in errors");
        assertTrue(errors.stream().anyMatch(e -> e.contains("chunk_0000.json")));
    }
}
