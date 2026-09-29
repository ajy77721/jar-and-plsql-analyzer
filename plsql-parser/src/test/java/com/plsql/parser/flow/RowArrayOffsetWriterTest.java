package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plan 1e golden test — the offsets sidecar must point at exactly each row.
 *
 * <h2>What is pinned</h2>
 *
 * <p>For every row, the byte slice {@code [offset, offset+length)} of {@code index.json} must equal
 * the row's own JSON, byte for byte — including rows that contain the characters most likely to
 * desynchronise a hand-built offset (quotes, commas, braces, non-ASCII, embedded newlines). The whole
 * document is additionally compared against Jackson's compact serialisation of the equivalent object,
 * so the hand-framed writer cannot silently drift from what a reader would have parsed before.
 */
class RowArrayOffsetWriterTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private Map<String, Object> row(String nodeId, String table, String sql, List<String> cols) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("nodeId", nodeId);
        r.put("tableName", table);
        if (sql != null) r.put("sqlText", sql);
        r.put("columns", cols);
        r.put("line", 42);
        return r;
    }

    @Test
    @DisplayName("each row's offset slice is byte-identical to that row's JSON")
    void offsetsPointAtExactlyEachRow() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey docKey = ArtifactKey.of("b", "api", "statements", "index.json");
        ArtifactKey offKey = ArtifactKey.of("b", "api", "statements", "index.offsets.json");

        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row("APP$P$ONE", "T1", "SELECT a, b FROM t1 WHERE x = 'a,b'", List.of("A", "B")));
        // Braces, quotes and a comma inside a value — the classic offset-desync material.
        rows.add(row("APP$P$TWO", "T2", "UPDATE t2 SET c = '{\"k\": 1}' WHERE id = 3", List.of()));
        // Non-ASCII and an embedded newline.
        rows.add(row("APP$P$THREE", "T3", "INSERT -- café\nVALUES ('naïve')", List.of("C")));
        rows.add(row("APP$P$FOUR", "T4", null, List.of("D")));

        Map<String, Object> leading = new LinkedHashMap<>();
        leading.put("totalStatements", rows.size());
        leading.put("statementsWithColumns", 3);

        RowArrayOffsetWriter.Result result =
                RowArrayOffsetWriter.write(store, docKey, offKey, leading, "statements", rows);
        assertEquals(rows.size(), result.rowCount());

        byte[] docBytes;
        try (InputStream in = store.read(docKey)) {
            docBytes = in.readAllBytes();
        }
        assertEquals(docBytes.length, result.docLength(), "reported document length is wrong");

        // Whole-document golden: identical to a plain Jackson compact serialisation of the same doc.
        Map<String, Object> expected = new LinkedHashMap<>(leading);
        expected.put("statements", rows);
        assertArrayEquals(JSON.writeValueAsBytes(expected), docBytes,
                "hand-framed statements index differs from Jackson's compact output");

        JsonNode offsets = JSON.readTree(store.read(offKey));
        assertEquals(rows.size(), offsets.get("rowCount").asInt());
        assertEquals(rows.size(), offsets.get("offsets").size());
        assertEquals(rows.size(), offsets.get("lengths").size());

        for (int i = 0; i < rows.size(); i++) {
            long off = offsets.get("offsets").get(i).asLong();
            long len = offsets.get("lengths").get(i).asLong();
            byte[] slice;
            try (InputStream in = store.read(docKey, off, len)) {
                slice = in.readAllBytes();
            }
            // Golden: the slice is exactly this row, and parses to this row's document.
            assertArrayEquals(JSON.writeValueAsBytes(rows.get(i)), slice,
                    "row " + i + " slice is not that row's JSON");
            assertEquals(JSON.readTree(JSON.writeValueAsBytes(rows.get(i))), JSON.readTree(slice),
                    "row " + i + " parses to a different document");
        }
    }

    @Test
    @DisplayName("an empty row set still produces a valid document and offsets file")
    void emptyRowsStillValid() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey docKey = ArtifactKey.of("b", "api", "statements", "index.json");
        ArtifactKey offKey = ArtifactKey.of("b", "api", "statements", "index.offsets.json");

        Map<String, Object> leading = new LinkedHashMap<>();
        leading.put("totalStatements", 0);
        leading.put("statementsWithColumns", 0);

        RowArrayOffsetWriter.Result r =
                RowArrayOffsetWriter.write(store, docKey, offKey, leading, "statements", List.of());
        assertEquals(0, r.rowCount());

        JsonNode doc = JSON.readTree(store.read(docKey));
        assertEquals(0, doc.get("statements").size());
        assertEquals(0, doc.get("totalStatements").asInt());

        JsonNode offsets = JSON.readTree(store.read(offKey));
        assertEquals(0, offsets.get("rowCount").asInt());
        assertEquals(0, offsets.get("offsets").size());
    }
}
