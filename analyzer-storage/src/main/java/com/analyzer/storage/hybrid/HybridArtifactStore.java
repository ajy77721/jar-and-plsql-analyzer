package com.analyzer.storage.hybrid;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;
import com.analyzer.storage.mongo.MongoArtifactStore;
import com.analyzer.storage.s3.S3ArtifactStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * {@link ArtifactStore} that composes a {@link MongoArtifactStore} (small artifacts) and an
 * {@link S3ArtifactStore} (big artifacts), split on a byte threshold, {@code HybridRollover}
 * fashion — the same idea {@code GridFsRolloverOutputStream} already uses one level down (inline
 * document vs. GridFS), applied one level up (Mongo vs. S3).
 *
 * <h2>Write routing</h2>
 *
 * <ul>
 *   <li>A known {@code contentLength} at or above {@link #bigThreshold()} goes straight to S3;
 *       Mongo is never touched.</li>
 *   <li>Otherwise the bytes are buffered on the heap (bounded at {@code bigThreshold}). If the
 *       buffer would cross the threshold before {@code close()}, the write "rolls over": whatever
 *       was buffered is flushed into a fresh S3 upload and the remaining bytes stream straight into
 *       it. If the stream closes while still under threshold, the buffered bytes are committed by
 *       delegating to {@code mongo.write(key, len)} — {@code MongoArtifactStore}'s own,
 *       already-correct write path (which itself may keep the payload inline or promote it to
 *       GridFS according to its own, unrelated, inline threshold).</li>
 * </ul>
 *
 * <p>Either commit path removes any stale copy of the key from the <em>other</em> backend
 * afterwards (best-effort). This is what keeps an artifact that crosses the threshold on a
 * rewrite — small then big, or big then small — from leaving a stale, still-visible copy behind:
 * without it, {@link #read} would keep answering from the old backend (it is checked first) and
 * {@link #list}/{@link #statPrefix} would show the same key twice.
 *
 * <h2>Read/metadata routing</h2>
 *
 * <p>{@link #read}, {@link #exists}, {@link #size} and {@link #lastModified} all try Mongo first —
 * cheap, and most artifacts are small — and fall back to S3 only when Mongo does not have the key.
 * There is no separate routing index: which backend holds a key is discovered by asking, not
 * recorded anywhere. That is a deliberate simplicity choice, not an oversight — see the class-level
 * discussion above for why the two write paths keep it that way after every write.
 *
 * <h2>Listing, deleting, copying, moving</h2>
 *
 * <p>{@link #list} and {@link #statPrefix} query both backends and merge, de-duplicating by key
 * (Mongo wins a collision, which should not happen given the cleanup above but is handled rather
 * than assumed away). {@link #delete} and {@link #deletePrefix} attempt both backends
 * unconditionally: a key existing in only one is the normal case, not an error. {@link #copy} and
 * {@link #move} operate on whichever backend actually holds the source, then clean up any stale
 * copy of the destination in the other backend.
 */
public class HybridArtifactStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(HybridArtifactStore.class);

    /** Same default as {@link MongoArtifactStore#DEFAULT_INLINE_THRESHOLD} — see class doc. */
    public static final long DEFAULT_BIG_THRESHOLD = MongoArtifactStore.DEFAULT_INLINE_THRESHOLD;

    private final MongoArtifactStore mongo;
    private final S3ArtifactStore s3;
    private final long bigThreshold;

    /**
     * @param mongo        the small-artifact half. Built and owned by the caller (typically
     *                     {@code StorageConfig}) — this class builds no clients of its own.
     * @param s3           the big-artifact half. Same caveat.
     * @param bigThreshold bytes at or above which an artifact is routed to S3 rather than Mongo.
     */
    public HybridArtifactStore(MongoArtifactStore mongo, S3ArtifactStore s3, long bigThreshold) {
        if (mongo == null) throw new IllegalArgumentException("mongo store is required");
        if (s3 == null) throw new IllegalArgumentException("s3 store is required");
        if (bigThreshold <= 0) {
            throw new IllegalArgumentException(
                    "app.storage.hybrid.big-threshold must be > 0, was " + bigThreshold);
        }
        this.mongo = mongo;
        this.s3 = s3;
        this.bigThreshold = bigThreshold;
    }

    /** Bytes at or above which a write goes straight to S3. */
    public long bigThreshold() {
        return bigThreshold;
    }

    /** The small-artifact half. Exposed read-only, for tests and housekeeping. */
    public MongoArtifactStore mongoHalf() {
        return mongo;
    }

    /** The big-artifact half. Exposed read-only, for tests and housekeeping. */
    public S3ArtifactStore s3Half() {
        return s3;
    }

    // ── writes ───────────────────────────────────────────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return write(key, -1);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        if (contentLength >= 0 && contentLength >= bigThreshold) {
            // Known big upfront: skip Mongo entirely, but still clean up a stale small copy left
            // behind by a previous, smaller write of this same key.
            return new CleanupOnClose(s3.write(key, contentLength), key, /* wroteToS3 */ true);
        }
        return new RolloverOutputStream(key, contentLength);
    }

    /**
     * Buffers on the heap until {@code bigThreshold}, then rolls the whole write over into S3 —
     * mirroring {@code GridFsRolloverOutputStream}'s inline-vs-GridFS rollover exactly, one level
     * up. A partially-inline, partially-S3 write is not a state this class has: the write that
     * crosses the threshold is rolled over in full, not split.
     */
    private final class RolloverOutputStream extends OutputStream {
        private final ArtifactKey key;

        /** Non-null until rollover; null afterwards. */
        private ByteArrayOutputStream buffer;

        /** Non-null after rollover. */
        private OutputStream s3Upload;

        private long written;
        private boolean closed;

        RolloverOutputStream(ArtifactKey key, long contentLengthHint) {
            this.key = key;
            int initial = contentLengthHint >= 0 && contentLengthHint < bigThreshold
                    ? (int) Math.max(32, contentLengthHint)
                    : (int) Math.min(8 * 1024, bigThreshold);
            this.buffer = new ByteArrayOutputStream(initial);
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[]{(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (closed) throw new IOException("stream already closed: " + key);
            if (len <= 0) return;
            if (buffer != null) {
                if (written + len <= bigThreshold) {
                    buffer.write(b, off, len);
                    written += len;
                    return;
                }
                rollOver();
            }
            s3Upload.write(b, off, len);
            written += len;
        }

        /** Opens the S3 upload and hands it the bytes buffered so far. Releases the heap buffer. */
        private void rollOver() throws IOException {
            byte[] pending = buffer.toByteArray();
            buffer = null;
            s3Upload = s3.write(key, -1);
            if (pending.length > 0) s3Upload.write(pending);
        }

        @Override
        public void flush() throws IOException {
            if (s3Upload != null) s3Upload.flush();
        }

        @Override
        public void close() throws IOException {
            if (closed) return;      // idempotent, matching every other ArtifactStore stream
            closed = true;
            if (buffer != null) {
                byte[] all = buffer.toByteArray();
                buffer = null;
                // Delegate the actual commit to MongoArtifactStore's own, already-correct write
                // path rather than reimplementing inline-document writing here.
                try (OutputStream out = mongo.write(key, all.length)) {
                    out.write(all);
                }
                cleanupOther(key, /* wroteToS3 */ false);
            } else {
                s3Upload.close();
                cleanupOther(key, /* wroteToS3 */ true);
            }
        }
    }

    /**
     * Wraps a stream that already committed straight to one backend (the content-length-hint
     * fast path), so that on a successful close the other backend is swept of any stale copy of
     * the same key.
     */
    private final class CleanupOnClose extends OutputStream {
        private final OutputStream delegate;
        private final ArtifactKey key;
        private final boolean wroteToS3;
        private boolean closed;

        CleanupOnClose(OutputStream delegate, ArtifactKey key, boolean wroteToS3) {
            this.delegate = delegate;
            this.key = key;
            this.wroteToS3 = wroteToS3;
        }

        @Override public void write(int b) throws IOException { delegate.write(b); }

        @Override public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override public void flush() throws IOException { delegate.flush(); }

        @Override
        public void close() throws IOException {
            if (closed) return;
            closed = true;
            delegate.close();
            cleanupOther(key, wroteToS3);
        }
    }

    /**
     * Sweeps run off the write path. Every write otherwise pays a synchronous delete round trip to
     * the OTHER backend — and when the write went to Mongo, that other backend is S3, so a save of
     * a few hundred small artifacts was spending a minute-plus in no-op S3 DELETEs to a remote
     * region. The sweep is a no-op in the overwhelmingly common case (the key was never in the
     * other backend), so it does not need to block anybody: a stale copy lingering for the moments
     * this daemon needs to notice it is the same eventual visibility the write-then-read race
     * already tolerates. Daemon threads so a pending sweep never blocks JVM shutdown.
     */
    private static final java.util.concurrent.ExecutorService SWEEPER =
            java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "hybrid-stale-sweep");
                t.setDaemon(true);
                return t;
            });

    /**
     * Removes a stale copy of {@code key} from the backend that did NOT just receive the write,
     * best-effort and ASYNCHRONOUSLY (see {@link #SWEEPER}). Needed because a rewrite can cross the
     * threshold in either direction — without this, {@link #read} (which checks Mongo first) would
     * keep answering from a backend that no longer holds the current write, and {@link #list}/
     * {@link #statPrefix} would show the key twice.
     */
    private void cleanupOther(ArtifactKey key, boolean wroteToS3) {
        ArtifactStore other = wroteToS3 ? mongo : s3;
        SWEEPER.execute(() -> {
            try {
                other.delete(key);
            } catch (RuntimeException e) {
                log.warn("[hybrid] could not clean up stale copy of {} in the {} backend: {}",
                        key, wroteToS3 ? "mongo" : "s3", e.getMessage());
            }
        });
    }

    // ── reads ────────────────────────────────────────────────────────────────

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        try {
            return mongo.read(key);
        } catch (NoSuchFileException e) {
            return s3.read(key);
        }
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        try {
            return mongo.read(key, offset, length);
        } catch (NoSuchFileException e) {
            return s3.read(key, offset, length);
        }
    }

    // ── metadata ─────────────────────────────────────────────────────────────

    @Override
    public boolean exists(ArtifactKey key) {
        return mongo.exists(key) || s3.exists(key);
    }

    @Override
    public long size(ArtifactKey key) {
        long len = mongo.size(key);
        return len >= 0 ? len : s3.size(key);
    }

    @Override
    public long lastModified(ArtifactKey key) {
        long t = mongo.lastModified(key);
        return t >= 0 ? t : s3.lastModified(key);
    }

    // ── deletes ──────────────────────────────────────────────────────────────

    /**
     * Attempts both backends unconditionally. A key living in only one of them is the normal case,
     * not an error — so the result is "was it deleted from either", not "was it deleted from both".
     */
    @Override
    public boolean delete(ArtifactKey key) {
        boolean m = mongo.delete(key);
        boolean s = s3.delete(key);
        return m || s;
    }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException {
        int removed = mongo.deletePrefix(prefix);
        removed += s3.deletePrefix(prefix);
        return removed;
    }

    // ── listing ──────────────────────────────────────────────────────────────

    /**
     * Queries both backends and concatenates, de-duplicating by key. Relies on {@link Stream}'s
     * sequential-by-default evaluation: the whole Mongo stream is consumed (populating {@code
     * seen}) before the S3 stream is touched, so the S3-side filter sees a complete set. A caller
     * that forces this into a parallel stream would break that assumption — nothing on the
     * interface promises parallel-safety, so this does not attempt to support it.
     */
    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        Set<ArtifactKey> seen = new HashSet<>();
        Stream<ArtifactEntry> mongoStream = mongo.list(prefix, recursive).peek(e -> seen.add(e.key()));
        Stream<ArtifactEntry> s3Stream = s3.list(prefix, recursive).filter(e -> !seen.contains(e.key()));
        return Stream.concat(mongoStream, s3Stream);
    }

    /** As {@link #list}, but each backend's own one-round-trip {@code statPrefix} — merged, not concatenated. */
    @Override
    public Map<ArtifactKey, ArtifactEntry> statPrefix(ArtifactKey prefix) throws IOException {
        Map<ArtifactKey, ArtifactEntry> out = new LinkedHashMap<>(mongo.statPrefix(prefix));
        for (Map.Entry<ArtifactKey, ArtifactEntry> e : s3.statPrefix(prefix).entrySet()) {
            out.putIfAbsent(e.getKey(), e.getValue());
        }
        return out;
    }

    // ── copy / move / materialize ────────────────────────────────────────────

    /**
     * Copies within whichever backend holds {@code from}, then sweeps a stale copy of {@code to}
     * from the other backend — the same reasoning {@link #cleanupOther} exists for on the write
     * path, since a copy's destination can displace a previous write of the opposite size class.
     */
    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        if (mongo.exists(from)) {
            mongo.copy(from, to);
            cleanupOther(to, /* wroteToS3 */ false);
        } else if (s3.exists(from)) {
            s3.copy(from, to);
            cleanupOther(to, /* wroteToS3 */ true);
        } else {
            throw new NoSuchFileException(from.toString());
        }
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        if (mongo.exists(from)) {
            mongo.move(from, to);
            cleanupOther(to, /* wroteToS3 */ false);
        } else if (s3.exists(from)) {
            s3.move(from, to);
            cleanupOther(to, /* wroteToS3 */ true);
        } else {
            throw new NoSuchFileException(from.toString());
        }
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        if (mongo.exists(key)) return mongo.materialize(key);
        return s3.materialize(key);
    }
}
