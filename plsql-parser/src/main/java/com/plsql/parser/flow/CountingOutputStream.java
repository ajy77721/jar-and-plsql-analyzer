package com.plsql.parser.flow;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Counts the bytes that pass through, so a writer learns the length of what it just wrote without
 * re-reading or re-stat'ing the artifact.
 *
 * <h2>Why this exists</h2>
 *
 * <p>The layout manifest (plan 1f) needs a byte length per artifact. Asking the store afterwards
 * means {@code stat} locally and — far worse — one HEAD request per file on object storage, which is
 * exactly the fan-out the manifest is meant to remove. But the writer already pushes every byte
 * through an {@link OutputStream}, so wrapping that stream gives the length for free, in the same
 * single write pass, for every backend.
 *
 * <p>{@link #count()} is valid after {@code close()}: closing commits the artifact but does not
 * reset the counter, and Jackson's generator closes the target stream itself on completion, so the
 * count is read after the stream is already closed in practice.
 */
public final class CountingOutputStream extends FilterOutputStream {

    private long count;

    public CountingOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        count++;
    }

    /**
     * The bulk form is delegated directly rather than inherited: {@link FilterOutputStream}'s
     * default loops one byte at a time, which is catastrophic for multi-MB artifacts and is the
     * same hazard {@code LocalArtifactStore}'s own wrapper guards against.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        count += len;
    }

    /** Bytes written so far, inclusive of anything already flushed. */
    public long count() {
        return count;
    }
}
