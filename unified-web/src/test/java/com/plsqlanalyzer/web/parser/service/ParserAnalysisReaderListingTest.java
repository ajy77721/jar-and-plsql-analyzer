package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.flow.ChunkedFlowWriter;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import com.plsql.parser.model.TableOperationInfo;
import com.plsqlanalyzer.web.parser.model.ParserAnalysisInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Item A — the manifest-based listing. For an analysis whose {@code api/manifest.json} is fresh, the
 * history listing is built from the manifest's {@code listing} scalars instead of reading
 * {@code api/index.json}, and the response is <b>byte-identical</b> to the index-served one. A stale
 * or absent manifest falls back to the index read, unchanged.
 *
 * <p>The fixture is produced by the real {@link ChunkedFlowWriter}, so the manifest the reader
 * consumes is the one the writer actually emits — not a hand-built document that could drift from
 * it.
 */
class ParserAnalysisReaderListingTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final ArtifactKey BUILD =
            ArtifactKey.of("plsql-parse", "DB", "PROJ", "ENV", "OBJ");

    private static ArtifactKey indexKey() {
        return BUILD.resolve("api", "index.json");
    }

    private static ArtifactKey manifestKey() {
        return BUILD.resolve("api", "manifest.json");
    }

    /** Writes a minimal but real analysis: one node, one table op, a populated result. */
    private static void writeAnalysis(InMemoryArtifactStore store) throws IOException {
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, BUILD);

        FlowNode n = new FlowNode();
        n.setNodeId("APP$PKG$P");
        n.setOrder(0);
        n.setDepth(0);
        n.setSchema("APP");
        n.setPackageName("PKG");
        n.setObjectName("P");
        n.setLinesOfCode(3);
        n.setReadable(true);
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
        result.setCrawlTimeMs(1234);
        result.setMaxDepthReached(2);
        result.setTotalLinesOfCode(42);
        result.setDbCallCount(3);
        result.setErrors(List.of("warn1"));
        writer.writeFinalResult(result, Map.of());
    }

    private static void put(InMemoryArtifactStore store, ArtifactKey key, String body) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static ParserAnalysisInfo only(InMemoryArtifactStore store) throws IOException {
        List<ParserAnalysisInfo> all = new ParserAnalysisReader(store).listAnalyses();
        assertEquals(1, all.size(), "expected exactly one analysis");
        return all.get(0);
    }

    @Test
    void manifestServedListingIsByteIdenticalToTheIndexServedListing() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        writeAnalysis(store);
        assertTrue(store.exists(manifestKey()), "the writer must have written a manifest");

        ParserAnalysisInfo fromManifest = only(store);
        assertEquals("DB~PROJ~ENV~OBJ", fromManifest.getName());
        assertEquals("APP.PKG.P", fromManifest.getEntryPoint());
        assertEquals("APP", fromManifest.getEntrySchema());
        assertEquals(1, fromManifest.getTotalNodes());
        assertEquals(42, fromManifest.getTotalLinesOfCode());
        assertEquals(2, fromManifest.getMaxDepth());
        assertEquals(1234L, fromManifest.getCrawlTimeMs());
        assertEquals(3, fromManifest.getDbCallCount());
        assertEquals(List.of("warn1"), fromManifest.getErrors());

        // The same analysis with the manifest removed must produce the identical row.
        store.delete(manifestKey());
        ParserAnalysisInfo fromIndex = only(store);
        assertArrayEquals(M.writeValueAsBytes(fromIndex), M.writeValueAsBytes(fromManifest),
                "the manifest-served listing must be byte-identical to the index-served one");
    }

    @Test
    void aFreshManifestIsPreferredOverTheIndex() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        writeAnalysis(store);
        long manifestMtime = store.lastModified(manifestKey());

        // Replace the index with DIFFERENT scalars, but keep it older than the manifest so the
        // manifest stays fresh and must be the source.
        put(store, indexKey(), "{\"entryPoint\":\"DIFFERENT\",\"totalNodes\":777}");
        store.setLastModifiedForTest(indexKey(), manifestMtime - 1000);

        ParserAnalysisInfo info = only(store);
        assertEquals("APP.PKG.P", info.getEntryPoint(), "the fresh manifest must be preferred");
        assertEquals(1, info.getTotalNodes());
    }

    @Test
    void aStaleManifestFallsBackToTheIndex() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        writeAnalysis(store);

        put(store, indexKey(), "{\"entryPoint\":\"DIFFERENT\",\"totalNodes\":777}");
        // The index is now newer than the manifest, so the manifest is stale and must be ignored.
        store.setLastModifiedForTest(manifestKey(), store.lastModified(indexKey()) - 1000);

        ParserAnalysisInfo info = only(store);
        assertEquals("DIFFERENT", info.getEntryPoint(), "a stale manifest must fall back to the index");
        assertEquals(777, info.getTotalNodes());
    }

    @Test
    void anAnalysisWithNoManifestIsListedFromTheIndex() throws IOException {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        put(store, indexKey(), "{\"entryPoint\":\"DEMO\",\"totalNodes\":5,\"totalTables\":2}");
        ParserAnalysisInfo info = only(store);
        assertNotNull(info);
        assertEquals("DEMO", info.getEntryPoint());
        assertEquals(5, info.getTotalNodes());
        assertEquals(2, info.getTotalTables());
    }
}
