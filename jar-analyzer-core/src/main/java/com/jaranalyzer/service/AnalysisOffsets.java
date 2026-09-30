package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The byte-offset index for {@code analysis.json}'s two ordinal-addressed arrays — plan 2a.
 *
 * <h2>Why an offsets file, and why here</h2>
 *
 * <p>{@code /classes/by-index/{idx}} and the endpoint slices reach record N by opening the whole
 * document and streaming past everything before it. For the largest analysis measured that is a
 * multi-hundred-MB read to return one ~100 KB object. With the offsets sidecar the reader seeks
 * straight to the record — see {@link AnalysisOffsetIndexReader}. The offsets cost nothing extra to
 * produce: {@link PersistenceService} is already serialising each element, so it records the file
 * position immediately before and after each one, exactly as {@code RowArrayOffsetWriter} does for
 * the PL/SQL statements index (plan 1e).
 *
 * <h2>Format</h2>
 *
 * <p>The compact <b>two-array</b> form, one segment per array:
 * <pre>{"classes":{"rowCount":N,"offsets":[...],"lengths":[...]},"endpoints":{...}}</pre>
 * The record index is the array position, so it is not repeated. Record {@code i}'s bytes are
 * exactly {@code [offsets[i], offsets[i] + lengths[i])}. The offsets point at the record itself —
 * <em>after</em> any separating comma — so the slice is a complete JSON value.
 *
 * <h2>Filled during the write, never by a second pass</h2>
 *
 * <p>The instance is a mutable collector: {@link PersistenceService} fills it while it writes the
 * {@code classes} and {@code endpoints} arrays, then calls {@link #writeTo} once the document is
 * committed. Nothing here re-reads {@code analysis.json}.
 */
public final class AnalysisOffsets {

    /** Compact (unindented) mapper — this file is read by a seeker, never by a human. */
    private static final ObjectMapper COMPACT = new ObjectMapper();

    public static final String SEGMENT_CLASSES = "classes";
    public static final String SEGMENT_ENDPOINTS = "endpoints";

    private long[] classOffsets = new long[0];
    private long[] classLengths = new long[0];
    private long[] endpointOffsets = new long[0];
    private long[] endpointLengths = new long[0];

    /** Records the classes segment as collected during the write. */
    public void setClasses(long[] offsets, long[] lengths) {
        this.classOffsets = offsets == null ? new long[0] : offsets;
        this.classLengths = lengths == null ? new long[0] : lengths;
    }

    /** Records the endpoints segment as collected during the write. */
    public void setEndpoints(long[] offsets, long[] lengths) {
        this.endpointOffsets = offsets == null ? new long[0] : offsets;
        this.endpointLengths = lengths == null ? new long[0] : lengths;
    }

    public int classCount() { return classOffsets.length; }

    public int endpointCount() { return endpointOffsets.length; }

    /** Serialises the sidecar to {@code key}, compact. */
    public void writeTo(ArtifactStore store, ArtifactKey key) throws IOException {
        try (java.io.OutputStream out = store.write(key)) {
            COMPACT.writeValue(out, toDocument());
        }
    }

    /** The sidecar as a document — the single place its shape is defined, and what tests compare. */
    public Map<String, Object> toDocument() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put(SEGMENT_CLASSES, segment(classOffsets, classLengths));
        doc.put(SEGMENT_ENDPOINTS, segment(endpointOffsets, endpointLengths));
        return doc;
    }

    private static Map<String, Object> segment(long[] offsets, long[] lengths) {
        Map<String, Object> seg = new LinkedHashMap<>();
        seg.put("rowCount", offsets.length);
        seg.put("offsets", offsets);
        seg.put("lengths", lengths);
        return seg;
    }
}
