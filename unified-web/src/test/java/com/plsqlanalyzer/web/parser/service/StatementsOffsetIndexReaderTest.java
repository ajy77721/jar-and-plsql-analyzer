package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.plsql.parser.flow.RowArrayOffsetWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plan 1e reader test — a page is served by seeking, not by parsing the whole statements index.
 *
 * <h2>Why a real local store</h2>
 *
 * <p>Server, seek and offset arithmetic are only exercised end to end against a real file, so the
 * document and its offsets sidecar are written with the production writer into a
 * {@link LocalArtifactStore}. The store is subclassed to count the bytes actually handed back, which
 * is how the "one small read" claim is checked rather than assumed: a two-row page must transfer a
 * fraction of the document.
 */
class StatementsOffsetIndexReaderTest {

    private static final String NAME = "DB~PROJ~ENV~OBJ";

    /** Counts bytes served by ranged reads so the test can prove paging does not read the document. */
    private static final class CountingStore extends LocalArtifactStore {
        long rangedBytes;
        int rangedCalls;

        CountingStore(Path root) {
            super(root);
        }

        @Override
        public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
            rangedCalls++;
            InputStream in = super.read(key, offset, length);
            return new FilterInputStream(in) {
                @Override
                public int read() throws IOException {
                    int b = super.read();
                    if (b >= 0) rangedBytes++;
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int n = super.read(b, off, len);
                    if (n > 0) rangedBytes += n;
                    return n;
                }
            };
        }
    }

    private static Map<String, Object> row(int i) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("nodeId", "APP$PKG$P" + i);
        r.put("nodeName", "P" + i);
        r.put("tableName", "T" + i);
        r.put("operation", "SELECT");
        // Deliberately bulky so the whole-document cost is clearly larger than one page.
        r.put("sqlText", ("SELECT a, b, c FROM t" + i + " WHERE x = 'v' -- " + "x".repeat(200)));
        r.put("line", i);
        return r;
    }

    private static void writeIndex(CountingStore store, List<Map<String, Object>> rows) throws IOException {
        ArtifactKey docKey = AnalysisPaths.resolveKey(NAME, StatementsOffsetIndexReader.INDEX_PATH);
        ArtifactKey offKey = AnalysisPaths.resolveKey(NAME, StatementsOffsetIndexReader.OFFSETS_PATH);
        Map<String, Object> leading = new LinkedHashMap<>();
        leading.put("totalStatements", rows.size());
        leading.put("statementsWithColumns", rows.size());
        RowArrayOffsetWriter.write(store, docKey, offKey, leading, "statements", rows);
    }

    @Test
    @DisplayName("a page is served by seeking to exactly those rows, not by parsing the document")
    void pageIsServedBySeek(@TempDir Path root) throws IOException {
        CountingStore store = new CountingStore(root);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) rows.add(row(i));
        writeIndex(store, rows);

        long docSize = store.size(AnalysisPaths.resolveKey(NAME, StatementsOffsetIndexReader.INDEX_PATH));
        store.rangedBytes = 0;
        store.rangedCalls = 0;

        StatementsOffsetIndexReader reader = new StatementsOffsetIndexReader();
        StatementsOffsetIndexReader.Page page = reader.readPage(store, NAME, 10, 2);

        assertNotNull(page, "offsets index should have been usable");
        assertEquals(50, page.rowCount());
        assertEquals(10, page.from());
        assertEquals(12, page.to());
        assertEquals(2, page.rows().size());
        assertEquals("APP$PKG$P10", page.rows().get(0).get("nodeId").asText());
        assertEquals("APP$PKG$P11", page.rows().get(1).get("nodeId").asText());
        assertEquals("T11", page.rows().get(1).get("tableName").asText());

        // The claim under test: two rows, two ranged reads, far less than the whole document.
        assertEquals(2, store.rangedCalls, "one ranged read per row expected");
        assertTrue(store.rangedBytes < docSize / 4,
                "a 2-row page read " + store.rangedBytes + " of " + docSize + " bytes — not a seek");
    }

    @Test
    @DisplayName("a limit below zero reads to the end; a page past the end is empty")
    void boundsAreClamped(@TempDir Path root) throws IOException {
        CountingStore store = new CountingStore(root);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 7; i++) rows.add(row(i));
        writeIndex(store, rows);

        StatementsOffsetIndexReader reader = new StatementsOffsetIndexReader();
        StatementsOffsetIndexReader.Page tail = reader.readPage(store, NAME, 5, -1);
        assertEquals(2, tail.rows().size(), "rows 5 and 6 remain");
        assertEquals("APP$PKG$P6", tail.rows().get(1).get("nodeId").asText());

        StatementsOffsetIndexReader.Page past = reader.readPage(store, NAME, 100, 5);
        assertEquals(7, past.rowCount());
        assertTrue(past.rows().isEmpty());
        assertEquals(7, reader.rowCount(store, NAME));
    }

    @Test
    @DisplayName("a missing offsets file returns null so the caller keeps its legacy path")
    void missingOffsetsDegrade(@TempDir Path root) throws IOException {
        CountingStore store = new CountingStore(root);
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row(0));
        writeIndex(store, rows);

        store.delete(AnalysisPaths.resolveKey(NAME, StatementsOffsetIndexReader.OFFSETS_PATH));

        StatementsOffsetIndexReader reader = new StatementsOffsetIndexReader();
        assertNull(reader.readPage(store, NAME, 0, 1), "missing offsets must degrade, not throw");
        assertNull(reader.rowCount(store, NAME));
    }

    @Test
    @DisplayName("a corrupt offsets file degrades too, rather than serving a wrong row")
    void corruptOffsetsDegrade(@TempDir Path root) throws IOException {
        CountingStore store = new CountingStore(root);
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row(0));
        writeIndex(store, rows);

        // Truncated / nonsense sidecar.
        ArtifactKey offKey = AnalysisPaths.resolveKey(NAME, StatementsOffsetIndexReader.OFFSETS_PATH);
        try (var out = store.write(offKey)) {
            out.write("{\"rowCount\": 1, \"offsets\": [0], \"lengths\": [..".getBytes());
        }

        StatementsOffsetIndexReader reader = new StatementsOffsetIndexReader();
        assertNull(reader.readPage(store, NAME, 0, 1));
    }
}
