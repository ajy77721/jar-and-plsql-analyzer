package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes a JSON object of the shape <code>{"&lt;scalar fields&gt;", "statements": [row, row, ...]}</code>
 * and, in the same pass, the byte offset and length of every row inside the document it just wrote.
 *
 * <h2>Plan 1e — why the offsets, and why here</h2>
 *
 * <p>{@code api/statements/index.json} is ordinal-addressed: a reader pages by row number. Today
 * reaching row N means parsing the whole document. With the companion offsets file the reader seeks
 * straight to the row — see {@code StatementsOffsetIndexReader}. The offsets cost nothing extra to
 * produce: this writer is already serialising each row, so it records the file position before and
 * after each one.
 *
 * <h2>Byte-exactness</h2>
 *
 * <p>The document is framed compactly here (Jackson's own compact form: no whitespace), independent
 * of {@link ArtifactFormatting#pretty()}. Every row is serialised with a plain compact mapper and
 * written verbatim, so {@code [offset, offset+length)} is exactly the bytes Jackson would produce
 * for that row alone — verified by the golden comparison in {@code RowArrayOffsetWriterTest}. The
 * debug indentation switch is intentionally not applied to this one file: an indented document and
 * a byte-accurate offset index cannot both be cheap, and this file is read by a seeker, never by a
 * human.
 *
 * <h2>Offsets sidecar format</h2>
 *
 * <p>Chosen as the compact <b>two-array</b> form:
 * <pre>{"rowCount": N, "offsets": [o0, o1, ...], "lengths": [l0, l1, ...]}</pre>
 * The row index is the array position, so it need not be repeated; for a few thousand statements
 * this is roughly half the size of the equivalent {@code [{idx, offset, length}, ...]} form. The
 * reader hides it behind {@code OffsetIndex}.
 */
public final class RowArrayOffsetWriter {

    /** Compact (unindented) mapper, matching Jackson's default output used elsewhere. */
    private static final ObjectMapper COMPACT = new ObjectMapper();

    private RowArrayOffsetWriter() {
    }

    /**
     * @param leadingFields scalar fields written before the array, in iteration order
     * @param arrayField    name of the row array (e.g. {@code "statements"})
     * @return the written document's length, the offsets sidecar's length, and the row count
     */
    public static Result write(ArtifactStore store,
                               ArtifactKey docKey,
                               ArtifactKey offsetsKey,
                               Map<String, Object> leadingFields,
                               String arrayField,
                               List<? extends Map<String, Object>> rows) throws IOException {
        long[] offsets = new long[rows.size()];
        long[] lengths = new long[rows.size()];
        long docLength;
        try (CountingOutputStream out = new CountingOutputStream(store.write(docKey))) {
            out.write('{');
            boolean first = true;
            for (Map.Entry<String, Object> e : leadingFields.entrySet()) {
                if (!first) out.write(',');
                first = false;
                out.write(COMPACT.writeValueAsBytes(e.getKey()));
                out.write(':');
                out.write(COMPACT.writeValueAsBytes(e.getValue()));
            }
            if (!first) out.write(',');
            out.write(COMPACT.writeValueAsBytes(arrayField));
            out.write(':');
            out.write('[');
            for (int i = 0; i < rows.size(); i++) {
                if (i > 0) out.write(',');
                byte[] row = COMPACT.writeValueAsBytes(rows.get(i));
                // Position is recorded after the separator, so the slice is the row alone.
                offsets[i] = out.count();
                lengths[i] = row.length;
                out.write(row);
            }
            out.write(']');
            out.write('}');
            docLength = out.count();
        }

        Map<String, Object> sidecar = new LinkedHashMap<>();
        sidecar.put("rowCount", rows.size());
        sidecar.put("offsets", offsets);
        sidecar.put("lengths", lengths);
        long offsetsLength;
        try (CountingOutputStream out = new CountingOutputStream(store.write(offsetsKey))) {
            COMPACT.writeValue(out, sidecar);
            offsetsLength = out.count();
        }
        return new Result(rows.size(), docLength, offsetsLength);
    }

    /** What was written, so the caller can record it in the layout manifest without re-stat'ing. */
    public record Result(int rowCount, long docLength, long offsetsLength) {
    }
}
