package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Serves a page of {@code api/statements/index.json} by seeking, using the offset index the writer
 * emits alongside it (plan 1e).
 *
 * <h2>The problem it removes</h2>
 *
 * <p>The statements index is ordinal-addressed: a page request names a row range. Before this, the
 * only way to reach row N was to open and parse the whole document — for the largest analysis
 * measured, 5,015 rows / 2.3 MB per page request. With the offsets sidecar the reader reads only the
 * bytes of the requested rows.
 *
 * <h2>How the seek works</h2>
 *
 * <p>Everything goes through {@link ArtifactStore#read(ArtifactKey, long, long)}, which is the
 * backend's own ranged read: a file seek under {@code LocalArtifactStore}, an HTTP Range request
 * under {@code S3ArtifactStore} (and, when an S3 object is disk-cached, its local copy). No backend
 * branch is needed here — that is precisely the contract the store exposes.
 *
 * <h2>Degradation</h2>
 *
 * <p>If the offsets sidecar is absent or unreadable (older analyses, a partial write, a corrupt
 * file), {@link #readPage} returns {@code null} rather than throwing or guessing. The caller keeps
 * its existing full-document path unchanged; this class is purely additive.
 *
 * <h2>Offsets format</h2>
 *
 * <p>The compact two-array form written by {@code RowArrayOffsetWriter}:
 * <pre>{"rowCount": N, "offsets": [...], "lengths": [...]}</pre>
 * Row {@code i}'s bytes are exactly {@code [offsets[i], offsets[i] + lengths[i])}.
 */
public final class StatementsOffsetIndexReader {

    /** Document key, relative to the analysis directory. */
    public static final String INDEX_PATH = "api/statements/index.json";

    /** Offsets sidecar key, relative to the analysis directory. */
    public static final String OFFSETS_PATH = "api/statements/index.offsets.json";

    private final ObjectMapper mapper = new ObjectMapper();

    /** The parsed offsets sidecar. */
    public record OffsetIndex(int rowCount, long[] offsets, long[] lengths) {
    }

    /**
     * One page of statement rows.
     *
     * @param from     first row index served (inclusive)
     * @param to       last row index served (exclusive)
     * @param rowCount total rows in the document
     */
    public record Page(int from, int to, int rowCount, List<JsonNode> rows) {
    }

    /** Convenience overload resolving the two keys from an analysis identity name. */
    public Page readPage(ArtifactStore store, String name, int from, int limit) throws IOException {
        if (store == null || name == null || name.isBlank()) return null;
        return readPage(store,
                AnalysisPaths.resolveKey(name, INDEX_PATH),
                AnalysisPaths.resolveKey(name, OFFSETS_PATH),
                from, limit);
    }

    /**
     * Reads rows {@code [from, from+limit)} (limit {@code < 0} means "to the end").
     *
     * @return the page, or {@code null} when there is no usable offsets index — the caller must then
     *         fall back to reading the document whole
     */
    public Page readPage(ArtifactStore store, ArtifactKey docKey, ArtifactKey offsetsKey,
                         int from, int limit) throws IOException {
        OffsetIndex idx = loadOffsets(store, offsetsKey);
        if (idx == null) return null;
        int start = Math.max(0, from);
        if (start >= idx.rowCount()) {
            return new Page(start, start, idx.rowCount(), List.of());
        }
        int end = limit < 0 ? idx.rowCount() : Math.min(idx.rowCount(), start + limit);
        List<JsonNode> rows = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            rows.add(readRow(store, docKey, idx.offsets()[i], idx.lengths()[i]));
        }
        return new Page(start, end, idx.rowCount(), rows);
    }

    /** One row, read by offset. */
    public JsonNode readRow(ArtifactStore store, ArtifactKey docKey, long offset, long length)
            throws IOException {
        try (InputStream in = store.read(docKey, offset, length)) {
            return mapper.readTree(in);
        }
    }

    /**
     * Parses the offsets sidecar, or returns {@code null} when it is missing, malformed, or
     * internally inconsistent (a mismatched array length is treated as "no usable index" rather than
     * risking a wrong row).
     */
    public OffsetIndex loadOffsets(ArtifactStore store, ArtifactKey offsetsKey) throws IOException {
        if (store == null || offsetsKey == null) return null;
        JsonNode doc;
        try (InputStream in = store.read(offsetsKey)) {
            doc = mapper.readTree(in);
        } catch (IOException | RuntimeException e) {
            // Missing or corrupt: degrade to the caller's legacy path, never fail the request.
            return null;
        }
        JsonNode offs = doc.get("offsets");
        JsonNode lens = doc.get("lengths");
        if (offs == null || lens == null || !offs.isArray() || !lens.isArray()
                || offs.size() != lens.size()) {
            return null;
        }
        int n = offs.size();
        long[] offsets = new long[n];
        long[] lengths = new long[n];
        for (int i = 0; i < n; i++) {
            offsets[i] = offs.get(i).asLong();
            lengths[i] = lens.get(i).asLong();
        }
        int rowCount = doc.has("rowCount") ? doc.get("rowCount").asInt() : n;
        if (rowCount != n) return null;
        return new OffsetIndex(rowCount, offsets, lengths);
    }

    /** Total rows, or {@code null} when there is no usable offsets index. */
    public Integer rowCount(ArtifactStore store, String name) throws IOException {
        if (store == null || name == null || name.isBlank()) return null;
        OffsetIndex idx = loadOffsets(store, AnalysisPaths.resolveKey(name, OFFSETS_PATH));
        return idx == null ? null : idx.rowCount();
    }
}
