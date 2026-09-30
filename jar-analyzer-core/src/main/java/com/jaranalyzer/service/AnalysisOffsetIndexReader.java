package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serves one class or endpoint of {@code analysis.json} by seeking, using the offset index the
 * writer emits alongside it (plan 2a).
 *
 * <h2>The problem it removes</h2>
 *
 * <p>{@code /classes/by-index/{idx}} is ordinal-addressed: the request names a position. Before this
 * the only way to reach position N was to open and parse the whole document — for the largest
 * analysis measured, hundreds of MB read to return one object. With the offsets sidecar the reader
 * transfers only the bytes of the requested record.
 *
 * <h2>How the seek works</h2>
 *
 * <p>Everything goes through {@link ArtifactStore#read(ArtifactKey, long, long)} — the backend's own
 * ranged read: a file seek under a local store, an HTTP Range request under S3. No backend branch is
 * needed here; that is the contract the store exposes.
 *
 * <h2>Degradation</h2>
 *
 * <p>If the offsets sidecar is absent or unreadable (older analyses, a partial write, a corrupt
 * file), {@link #load} returns {@code null} rather than throwing or guessing. A caller keeps its
 * existing full-document path unchanged; this class is purely additive.
 *
 * <h2>Format</h2>
 *
 * <p>The compact two-array form written by {@link AnalysisOffsets}:
 * <pre>{"classes":{"rowCount":N,"offsets":[...],"lengths":[...]},"endpoints":{...}}</pre>
 */
public final class AnalysisOffsetIndexReader {

    private final ObjectMapper mapper = new ObjectMapper();

    /** One array's offset index. */
    public record Segment(int rowCount, long[] offsets, long[] lengths) {
        public boolean has(int index) { return index >= 0 && index < rowCount; }
    }

    /**
     * Parses the whole offsets sidecar into its segments, or returns an empty map when the file is
     * missing, malformed, or internally inconsistent (a mismatched array length is treated as "no
     * usable index" rather than risking a wrong record).
     */
    public Map<String, Segment> load(ArtifactStore store, ArtifactKey offsetsKey) throws IOException {
        Map<String, Segment> out = new LinkedHashMap<>();
        if (store == null || offsetsKey == null) return out;
        JsonNode doc;
        try (InputStream in = store.read(offsetsKey)) {
            doc = mapper.readTree(in);
        } catch (IOException | RuntimeException e) {
            // Missing or corrupt: degrade to the caller's legacy path, never fail the request.
            return out;
        }
        for (String name : new String[]{AnalysisOffsets.SEGMENT_CLASSES, AnalysisOffsets.SEGMENT_ENDPOINTS}) {
            Segment seg = parseSegment(doc.get(name));
            if (seg != null) out.put(name, seg);
        }
        return out;
    }

    /** One named segment, or {@code null} when it is absent or unusable. */
    public Segment loadSegment(ArtifactStore store, ArtifactKey offsetsKey, String name) throws IOException {
        return load(store, offsetsKey).get(name);
    }

    /** Convenience: the classes segment of the static or corrected document. */
    public Segment classes(ArtifactStore store, JarDataPaths paths, String jarName) throws IOException {
        return loadSegment(store, paths.analysisOffsetsKey(jarName), AnalysisOffsets.SEGMENT_CLASSES);
    }

    /** Convenience: the endpoints segment of the static or corrected document. */
    public Segment endpoints(ArtifactStore store, JarDataPaths paths, String jarName) throws IOException {
        return loadSegment(store, paths.analysisOffsetsKey(jarName), AnalysisOffsets.SEGMENT_ENDPOINTS);
    }

    /** One record, read by offset. Returns {@code null} when {@code index} is out of range. */
    public JsonNode readElement(ArtifactStore store, ArtifactKey docKey, Segment seg, int index)
            throws IOException {
        if (store == null || docKey == null || seg == null || !seg.has(index)) return null;
        try (InputStream in = store.read(docKey, seg.offsets()[index], seg.lengths()[index])) {
            return mapper.readTree(in);
        }
    }

    /** One class of the static document, by ordinal. */
    public JsonNode readClass(ArtifactStore store, JarDataPaths paths, String jarName, int index)
            throws IOException {
        Segment seg = classes(store, paths, jarName);
        return readElement(store, paths.analysisKey(jarName), seg, index);
    }

    /** One endpoint of the static document, by ordinal. */
    public JsonNode readEndpoint(ArtifactStore store, JarDataPaths paths, String jarName, int index)
            throws IOException {
        Segment seg = endpoints(store, paths, jarName);
        return readElement(store, paths.analysisKey(jarName), seg, index);
    }

    private static Segment parseSegment(JsonNode node) {
        if (node == null || !node.isObject()) return null;
        JsonNode offs = node.get("offsets");
        JsonNode lens = node.get("lengths");
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
        int rowCount = node.has("rowCount") ? node.get("rowCount").asInt() : n;
        if (rowCount != n) return null;
        return new Segment(rowCount, offsets, lengths);
    }
}
