package com.analyzer.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * Write-behind front for an {@link ArtifactStore} whose writes are small and many — the S3 case.
 *
 * <h2>Why</h2>
 *
 * <p>S3 mode turned every artifact into one HTTP round trip to a remote region. A single parse
 * writes ~300 objects — chunk flow files of a few hundred bytes, detail JSONs of a few KB — and
 * writing them strictly sequentially made the whole job pay ~300 × RTT of pure waiting: measured
 * at 131–153 s of save time where the bytes themselves are trivial. The uploads are independent
 * of each other and I/O-bound, so they parallelise for free: this class buffers a closed small
 * write on the heap and hands it to a bounded pool ({@value #DEFAULT_THREADS} threads) while the
 * analysis thread moves on to produce the next artifact.
 *
 * <h2>Semantics — deliberately unchanged</h2>
 *
 * <p>A {@code close()} that returned must not mean "the bytes are somewhere the caller can't
 * predict", so every read-shaped operation — {@link #read}, {@link #exists}, {@link #size},
 * {@link #lastModified}, {@link #list}, {@link #copy}, {@link #move}, {@link #delete},
 * {@link #materialize} — first <b>drains every pending upload</b> (normally a no-op: the queue is
 * empty by the time anything reads). Callers therefore see exactly the visibility a blocking
 * store gave them; the only thing that changed is that uploads overlap the analysis thread's own
 * work instead of serialising against it. The drain is also where an upload that FAILED is
 * rethrown — a background failure is never silently swallowed, it surfaces at the next reader,
 * which is the analysis thread itself in every real flow.
 *
 * <p>The read-shaped methods whose interface signatures declare no {@code IOException}
 * ({@code exists/size/lastModified/delete}) surface a failed drain as
 * {@link UncheckedIOException} — the same shape the S3 backend itself uses for its own
 * network failures on those paths.
 *
 * <h2>Big writes roll over, like HybridArtifactStore's</h2>
 *
 * <p>A stream whose bytes cross {@link #ROLLOVER_THRESHOLD} (or a caller-supplied
 * {@code contentLength} that big) is never buffered whole: it rolls over into a direct blocking
 * upload — byte-for-byte the pattern {@code HybridArtifactStore.RolloverOutputStream} already
 * established — without which a multi-GB JAR upload would sit in heap and OOM. The heap is
 * additionally bounded across ALL in-flight buffers ({@link #MAX_PENDING_BYTES}): {@code close()}
 * on a small write blocks briefly (backpressure) rather than enqueueing past that cap.
 */
public final class WriteBehindStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(WriteBehindStore.class);

    static final int DEFAULT_THREADS = 8;
    static final int ROLLOVER_THRESHOLD = 32 * 1024 * 1024;   // per-write buffer cap
    static final long MAX_PENDING_BYTES = 128L * 1024 * 1024; // across all in-flight buffers

    private final ArtifactStore delegate;
    private final ExecutorService pool;
    private final Set<Future<?>> pending = ConcurrentHashMap.newKeySet();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition underCap = lock.newCondition();
    private long pendingBytes;
    private volatile IOException firstFailure;

    public WriteBehindStore(ArtifactStore delegate) {
        this(delegate, DEFAULT_THREADS);
    }

    public WriteBehindStore(ArtifactStore delegate, int threads) {
        this.delegate = delegate;
        this.pool = Executors.newFixedThreadPool(Math.max(1, threads), r -> {
            Thread t = new Thread(r, "s3-write-behind");
            t.setDaemon(true);
            return t;
        });
    }

    // ── writes — the only intercepted operations ─────────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return write(key, -1);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        if (contentLength >= 0 && contentLength >= ROLLOVER_THRESHOLD) return delegate.write(key, contentLength);
        return new BufferedAsyncStream(key);
    }

    /**
     * Blocks until every queued upload finished — the barrier every read-shaped operation crosses
     * first, and callable directly where a caller knows a read-back is coming.
     *
     * @throws IOException the first upload failure, if any failed
     */
    public void awaitPending() throws IOException {
        List<Future<?>> waiting;
        synchronized (pending) {
            waiting = new ArrayList<>(pending);
        }
        for (Future<?> f : waiting) {
            try {
                f.get();
            } catch (java.lang.InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for pending uploads", e);
            } catch (java.util.concurrent.ExecutionException e) {
                // Unwrap: the real failure is the upload's, not the Future's wrapping.
                throw asIo(e.getCause() == null ? e : e.getCause());
            } catch (java.util.concurrent.CancellationException e) {
                throw new IOException("an upload was cancelled", e);
            }
        }
        IOException f = firstFailure;
        if (f != null) throw f;
    }

    private static IOException asIo(Throwable t) {
        return t instanceof IOException io ? io : new IOException(t);
    }

    /** Drain, surfacing failures as unchecked — for the read-shaped methods whose interface
     * signature declares no {@code IOException}. */
    private void drainBeforeReadUnchecked() {
        try {
            awaitPending();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Buffers small writes on the heap; close() enqueues the upload and returns immediately.
     * Crossing {@link #ROLLOVER_THRESHOLD} switches the stream into pass-through mode against a
     * directly-opened delegate upload — the caller's bytes keep flowing, just unbuffered.
     */
    private final class BufferedAsyncStream extends OutputStream {
        private final ArtifactKey key;
        private java.io.ByteArrayOutputStream buffer;
        private OutputStream passThrough;
        private boolean closed;

        BufferedAsyncStream(ArtifactKey key) {
            this.key = key;
            this.buffer = new java.io.ByteArrayOutputStream();
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[]{(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (closed) throw new IOException("stream already closed: " + key);
            if (passThrough != null) {
                passThrough.write(b, off, len);
                return;
            }
            if (buffer.size() + len > ROLLOVER_THRESHOLD) rollOver();
            if (passThrough != null) {
                passThrough.write(b, off, len);
                return;
            }
            buffer.write(b, off, len);
        }

        private void rollOver() throws IOException {
            // A big body is arriving: drain the small-upload backlog first so this blocking
            // upload doesn't queue behind everything else, then hand off through the delegate.
            try {
                awaitPending();
            } catch (IOException e) {
                throw new IOException("earlier async upload failed (before rollover of " + key + ")", e);
            }
            passThrough = delegate.write(key);
            byte[] pendingBuf = buffer.toByteArray();
            buffer = null;
            if (pendingBuf.length > 0) passThrough.write(pendingBuf);
        }

        @Override
        public void flush() throws IOException {
            if (passThrough != null) passThrough.flush();
        }

        @Override
        public void close() throws IOException {
            if (closed) return;
            if (passThrough != null) {
                closed = true;
                passThrough.close();
                return;
            }
            closed = true;
            byte[] all = buffer.toByteArray();
            buffer = null;
            enqueue(key, all);
        }
    }

    /** Enqueues one small upload; blocks (backpressure) while too much is in flight. */
    private void enqueue(ArtifactKey key, byte[] bytes) {
        lock.lock();
        try {
            while (pendingBytes + bytes.length > MAX_PENDING_BYTES) {
                try {
                    underCap.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Backpressure wait interrupted: upload synchronously rather than drop the
                    // bytes — the caller's close() still owes an upload.
                    log.warn("[write-behind] interrupted draining upload backlog; uploading {} inline", key);
                    try (OutputStream out = delegate.write(key, bytes.length)) {
                        out.write(bytes);
                    } catch (IOException io) {
                        if (firstFailure == null) firstFailure = io;
                    }
                    return;
                }
            }
            pendingBytes += bytes.length;
        } finally {
            lock.unlock();
        }
        AtomicReference<Future<?>> self = new AtomicReference<>();
        Future<?> f = pool.submit(() -> {
            try {
                try (OutputStream out = delegate.write(key, bytes.length)) {
                    out.write(bytes);
                }
            } catch (IOException e) {
                if (firstFailure == null) firstFailure = e;
                log.error("[write-behind] upload of {} failed: {}", key, e.toString());
            } finally {
                lock.lock();
                try {
                    pendingBytes -= bytes.length;
                    underCap.signalAll();
                } finally {
                    lock.unlock();
                }
                Future<?> mine = self.get();
                if (mine != null) pending.remove(mine);
            }
        });
        self.set(f);
        pending.add(f);
    }

    // ── reads — each drains first, so semantics match a blocking store ───────

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        awaitPending();
        return delegate.read(key);
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        awaitPending();
        return delegate.read(key, offset, length);
    }

    @Override
    public boolean exists(ArtifactKey key) {
        drainBeforeReadUnchecked();
        return delegate.exists(key);
    }

    @Override
    public long size(ArtifactKey key) {
        drainBeforeReadUnchecked();
        return delegate.size(key);
    }

    @Override
    public long lastModified(ArtifactKey key) {
        drainBeforeReadUnchecked();
        return delegate.lastModified(key);
    }

    @Override
    public boolean delete(ArtifactKey key) {
        drainBeforeReadUnchecked();
        return delegate.delete(key);
    }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException {
        awaitPending();
        return delegate.deletePrefix(prefix);
    }

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        awaitPending();
        return delegate.list(prefix, recursive);
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        awaitPending();
        delegate.copy(from, to);
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        awaitPending();
        delegate.move(from, to);
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        awaitPending();
        return delegate.materialize(key);
    }
}
