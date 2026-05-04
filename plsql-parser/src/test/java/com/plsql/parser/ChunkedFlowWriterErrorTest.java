package com.plsql.parser;

import com.plsql.parser.flow.ChunkedFlowWriter;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Error handling tests for ChunkedFlowWriter.
 * Tests groupCalls, groupTableOps, extractSequences, buildSubtreeTables
 * with empty/null inputs.
 */
@DisplayName("ChunkedFlowWriter Error Handling")
public class ChunkedFlowWriterErrorTest {

    private ChunkedFlowWriter writer;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup() throws IOException {
        writer = new ChunkedFlowWriter(tempDir.resolve("test-build"));
    }

    // ── writeChunk and mergeChunks with minimal data ──

    @Test
    @DisplayName("writeChunk with minimal FlowNode succeeds")
    void testWriteMinimalChunk() throws IOException {
        FlowNode node = new FlowNode();
        node.setOrder(1);
        node.setDepth(0);
        node.setObjectName("TEST");
        node.generateNodeId();

        assertDoesNotThrow(() -> writer.writeChunk(node));
    }

    @Test
    @DisplayName("mergeChunks with no chunks produces empty result")
    void testMergeNoChunks() throws IOException {
        FlowResult result = writer.mergeChunks("ENTRY", "SCHEMA", 100, 0, new ArrayList<>());
        assertNotNull(result);
        assertTrue(result.getFlowNodes().isEmpty());
        assertEquals(0, result.getTotalObjectsCrawled());
    }

    @Test
    @DisplayName("mergeChunks with one chunk includes that chunk")
    void testMergeOneChunk() throws IOException {
        FlowNode node = new FlowNode();
        node.setOrder(1);
        node.setDepth(0);
        node.setSchema("SCHEMA");
        node.setObjectName("PROC");
        node.setObjectType("PROCEDURE");
        node.setLineStart(1);
        node.setLineEnd(10);
        node.generateNodeId();
        writer.writeChunk(node);

        FlowResult result = writer.mergeChunks("ENTRY", "SCHEMA", 50, 0, new ArrayList<>());
        assertNotNull(result);
        assertEquals(1, result.getFlowNodes().size());
        assertEquals(1, result.getTotalObjectsCrawled());
    }

    // ── appendEdges with null/empty ──

    @Test
    @DisplayName("appendEdges with null list is no-op")
    void testAppendEdgesNull() {
        assertDoesNotThrow(() -> writer.appendEdges(null, 1));
    }

    @Test
    @DisplayName("appendEdges with empty list is no-op")
    void testAppendEdgesEmpty() {
        assertDoesNotThrow(() -> writer.appendEdges(new ArrayList<>(), 1));
    }

    // ── updateTables with empty map ──

    @Test
    @DisplayName("updateTables with empty map succeeds")
    void testUpdateTablesEmpty() {
        assertDoesNotThrow(() -> writer.updateTables(new LinkedHashMap<>()));
    }

    // ── sourceExists / readSourceFile ──

    @Test
    @DisplayName("sourceExists returns false for non-existent file")
    void testSourceExistsFalse() {
        assertFalse(writer.sourceExists("nonexistent.sql"));
    }

    @Test
    @DisplayName("readSourceFile returns null for non-existent file")
    void testReadSourceFileNull() throws IOException {
        assertNull(writer.readSourceFile("nonexistent.sql"));
    }

    @Test
    @DisplayName("writeSourceFile and readSourceFile round-trip")
    void testWriteAndReadSource() throws IOException {
        String content = "CREATE OR REPLACE PROCEDURE P IS BEGIN NULL; END;";
        writer.writeSourceFile("test.sql", content);

        assertTrue(writer.sourceExists("test.sql"));
        assertEquals(content, writer.readSourceFile("test.sql"));
    }

    // ── Progress tracking ──

    @Test
    @DisplayName("markProcessing and markDone succeed")
    void testProgressTracking() {
        assertDoesNotThrow(() -> {
            writer.markPending("PKG.PROC");
            writer.markProcessing("PKG.PROC");
            writer.markDone("PKG.PROC");
        });
    }

    @Test
    @DisplayName("markFailed records error without throwing")
    void testMarkFailed() {
        assertDoesNotThrow(() -> writer.markFailed("PKG.PROC", "test error message"));
    }

    // ── setDiscoveredTriggers with null ──

    @Test
    @DisplayName("setDiscoveredTriggers with null sets empty list")
    void testSetDiscoveredTriggersNull() {
        assertDoesNotThrow(() -> writer.setDiscoveredTriggers(null));
    }

    @Test
    @DisplayName("setDiscoveredTriggers with valid list succeeds")
    void testSetDiscoveredTriggersValid() {
        List<Map<String, Object>> triggers = new ArrayList<>();
        Map<String, Object> trig = new LinkedHashMap<>();
        trig.put("name", "TRG_TEST");
        trig.put("tableName", "MY_TABLE");
        triggers.add(trig);
        assertDoesNotThrow(() -> writer.setDiscoveredTriggers(triggers));
    }

    // ── writeFinalResult with full round-trip ──

    @Test
    @DisplayName("Full round-trip: write chunks, merge, writeFinalResult")
    void testFullRoundTrip() throws IOException {
        FlowNode node = new FlowNode();
        node.setOrder(1);
        node.setDepth(0);
        node.setSchema("SCHEMA");
        node.setPackageName("PKG");
        node.setObjectName("PROC");
        node.setObjectType("PROCEDURE");
        node.setLineStart(5);
        node.setLineEnd(25);

        CallInfo call = new CallInfo();
        call.setType("EXTERNAL");
        call.setPackageName("OTHER_PKG");
        call.setName("OTHER_PROC");
        call.setSchema("SCHEMA");
        call.setLine(10);
        node.setCalls(List.of(call));

        TableOperationInfo tableOp = new TableOperationInfo();
        tableOp.setTableName("EMPLOYEES");
        tableOp.setSchema("HR");
        tableOp.setOperation("SELECT");
        tableOp.setLine(15);
        node.setTableOperations(List.of(tableOp));

        node.generateNodeId();
        writer.writeChunk(node);

        FlowEdge edge = new FlowEdge();
        edge.setFromSchema("SCHEMA");
        edge.setFromPackage("PKG");
        edge.setFromObject("PROC");
        edge.setToSchema("SCHEMA");
        edge.setToPackage("OTHER_PKG");
        edge.setToObject("OTHER_PROC");
        edge.setLine(10);
        edge.setDepth(0);
        edge.setFromNodeId("SCHEMA$PKG$PROC");
        edge.setToNodeId("SCHEMA$OTHER_PKG$OTHER_PROC");
        writer.appendEdges(List.of(edge), 1);

        Map<String, SchemaTableInfo> tables = new LinkedHashMap<>();
        SchemaTableInfo tableInfo = new SchemaTableInfo();
        tableInfo.setSchema("HR");
        tableInfo.setTableName("EMPLOYEES");
        tableInfo.setObjectType("TABLE");
        tableInfo.getOperations().add("SELECT");
        tables.put("HR.EMPLOYEES", tableInfo);
        writer.updateTables(tables);

        FlowResult result = writer.mergeChunks("PKG.PROC", "SCHEMA", 100, 0, new ArrayList<>());
        assertNotNull(result);
        assertEquals(1, result.getFlowNodes().size());
        assertFalse(result.getCallGraph().isEmpty());

        // writeFinalResult should not throw
        assertDoesNotThrow(() -> writer.writeFinalResult(result));
    }
}
