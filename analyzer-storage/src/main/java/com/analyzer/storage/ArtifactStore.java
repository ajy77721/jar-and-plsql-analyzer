package com.analyzer.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

/**
 * Storage abstraction for opaque byte artifacts (analysis JSON, call trees, endpoint output,
 * stored JARs, exports).
 *
 * <p>Two implementations are planned: {@code LocalArtifactStore} (today's behaviour, the default)
 * and an S3-backed one. Callers must never assume which is active.
 *
 * <p><b>Streaming is mandatory.</b> A single analysis artifact can be far larger than heap. No
 * method here returns or accepts a byte array, and implementations must never buffer a whole
 * object in memory. Callers must stream too — the existing Jackson
 * {@code JsonParser}/{@code JsonGenerator} paths already do.
 */
public interface ArtifactStore {

    /**
     * Opens a stream for writing. <b>{@code close()} is the commit point</b>: implementations
     * must be atomic-on-close, so a concurrent reader sees either the previous object or the
     * complete new one, never a partial write. Abandoning the stream without closing must leave
     * the previous object intact.
     */
    OutputStream write(ArtifactKey key) throws IOException;

    /**
     * As {@link #write(ArtifactKey)}, but declares the content length up front where the caller
     * knows it. Lets an implementation choose a single-request upload instead of multipart.
     *
     * @param contentLength byte count, or -1 when unknown (the common case here — JSON is
     *                      generated streaming, so the length is not known until the end)
     */
    OutputStream write(ArtifactKey key, long contentLength) throws IOException;

    /** Full sequential read from the start of the object. */
    InputStream read(ArtifactKey key) throws IOException;

    /**
     * Partial read. Backs random access into large artifacts without transferring the whole
     * object (a file seek locally, an HTTP Range request on S3).
     *
     * @param offset first byte to return, inclusive, >= 0
     * @param length number of bytes, or -1 for "to end of object"
     */
    InputStream read(ArtifactKey key, long offset, long length) throws IOException;

    boolean exists(ArtifactKey key);

    /** Size in bytes, or -1 if the artifact does not exist. */
    long size(ArtifactKey key);

    /** Last-modified time in millis since epoch, or -1 if the artifact does not exist. */
    long lastModified(ArtifactKey key);

    /** @return true if an artifact was deleted, false if it did not exist. */
    boolean delete(ArtifactKey key);

    /** Recursively deletes everything under {@code prefix}. @return count of artifacts removed. */
    int deletePrefix(ArtifactKey prefix) throws IOException;

    /**
     * Lists artifacts under {@code prefix}. Lazy — must not materialize the whole listing, which
     * for a large prefix can be millions of entries. The caller must close the stream.
     */
    Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException;

    /** Copies without round-tripping the bytes through this process where the backend allows. */
    void copy(ArtifactKey from, ArtifactKey to) throws IOException;

    /** Moves/renames. May be implemented as copy-then-delete where the backend has no rename. */
    void move(ArtifactKey from, ArtifactKey to) throws IOException;

    /**
     * <b>Escape hatch.</b> Materializes the artifact as a real local file, for the few callers
     * that genuinely require a {@link java.nio.file.Path} and cannot be given a stream — a
     * decompiler that opens a {@code JarFile}, a library that seeks with {@code RandomAccessFile},
     * an external process invoked with {@code --output-dir}.
     *
     * <p>Locally this is a zero-copy handle onto the existing file. Remotely it downloads to a
     * temp file. Either way the returned handle must be closed; closing deletes the temp copy
     * and is a no-op for a local file. Use sparingly — for an artifact larger than local disk
     * this cannot work, which is precisely why streaming is the default everywhere else.
     */
    LocalHandle materialize(ArtifactKey key) throws IOException;

    /**
     * Metadata for every artifact under {@code prefix}, in ONE round trip.
     *
     * <p>This exists because the natural-looking alternative is catastrophically slow on object
     * storage. A local {@code Files.exists} costs ~10 µs; an S3 HEAD costs ~25 ms — 2,500× more.
     * Code that loops calling {@link #exists}/{@link #size} per file therefore works fine locally
     * and collapses over S3: one real case issued two HEADs for each of 4,345 files, roughly
     * 3.6 minutes of pure latency, where a single listing took 0.13 s.
     *
     * <p>It is on the interface, rather than left to each caller to remember, so the fast path is
     * the obvious one. Prefer this over per-key checks whenever inspecting more than a couple of
     * artifacts.
     *
     * @return map keyed by artifact key; absent keys simply do not appear
     */
    default java.util.Map<ArtifactKey, ArtifactEntry> statPrefix(ArtifactKey prefix) throws IOException {
        java.util.Map<ArtifactKey, ArtifactEntry> out = new java.util.HashMap<>();
        try (java.util.stream.Stream<ArtifactEntry> s = list(prefix, true)) {
            s.filter(e -> !e.directory()).forEach(e -> out.put(e.key(), e));
        }
        return out;
    }
}
