package com.analyzer.storage;

import com.analyzer.storage.sso.CapturedSsoContext;
import com.analyzer.storage.sso.SsoCredentialsResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 *
 * <h2>The caller's credential context follows each buffered entry to the flush thread</h2>
 *
 * <p>The upload pool's threads are created once and have no request context and no installed
 * {@code SsoJobContext}; the caller's write thread does. That is exactly the shape of bug the
 * {@code RequestScopedArtifactStore} and {@code SsoJobContext} mechanisms exist to fix — a
 * credential context must follow the <em>work</em>, not the thread that started it. Left unfixed,
 * every upload from the pool signed with the default AWS chain, which on a pod with no ambient
 * credentials fails, and a completed analysis's buffered artifacts never reach S3.
 *
 * <p>So {@link #write(ArtifactKey, long)} — called on the request/queue thread where the identity
 * is resolvable — snapshots the context ({@link CapturedSsoContext}) and each entry carries its
 * own snapshot, since one queue can hold entries from different users. The flush task installs the
 * entry's snapshot (the same bounded install/uninstall {@code SsoJobContext} provides) around the
 * delegate upload, so the S3 client signs as the entry's writer. When no context is in force the
 * snapshot is {@code null} and nothing is installed, preserving the legacy default-chain path.
 *
 * <h2>Write-ahead spill — the crash window, closed</h2>
 *
 * <p>The heap buffer alone is lost on a process kill: {@code close()} returns, the bytes are only
 * in memory, and a {@code kill -9} before the flush thread uploads drops them. The historical
 * answer was "that is the write-behind tradeoff". It is not necessary. When a {@code spillDir} is
 * configured, {@link #write}'s {@code close()} <b>first writes the entry to a local spill file</b>
 * (atomically: temp file + rename, so a crash mid-spill leaves a temp file that replay ignores),
 * <em>then</em> enqueues the upload. The flush thread deletes the spill file on a successful
 * upload. A process that dies between spill and upload leaves the file behind, and
 * {@link #replaySpills()} — called once at boot for the startup store — uploads every surviving
 * spill and deletes it on success. So the window shrinks from "heap until upload" to "the
 * microsecond between the local write and the S3 acknowledgement", and a {@code kill -9} loses
 * nothing that had reached {@code close()}.
 *
 * <p><b>No fsync.</b> The threat model is a process kill, not a machine power-loss. A plain
 * {@code write(2)} puts the bytes in the kernel page cache, which survives the process; fsync
 * would add a disk-flush per artifact to the analysis write path for a durability level the
 * deployment does not claim. The spill is deliberately cheap because it runs on every small write.
 *
 * <p><b>Bounded disk.</b> Spill files are deleted on upload success, so steady state is empty. If
 * uploads fail they accumulate, so the directory is capped at {@link #DEFAULT_MAX_SPILL_BYTES};
 * past it the oldest spills are evicted (logged) rather than growing without bound. A spill that
 * cannot be written (disk full, permissions) degrades to the old heap-only behaviour and never
 * fails the caller's {@code close()} — the upload is still attempted.
 *
 * <p><b>Credentials are never spilled.</b> Only the key and bytes touch disk; the captured SSO
 * context is heap-only. A replayed entry therefore signs with whatever the process resolves at
 * boot (the default chain / startup store credentials), which is the only identity available
 * before a request exists. That is stated here so nobody later "fixes" replay by serialising a
 * session token.
 *
 * <p>A graceful shutdown complements the spill: one JVM shutdown hook (registered once, for every
 * spill-enabled store) gives pending uploads a bounded {@value #SHUTDOWN_FLUSH_TIMEOUT_MS} ms to
 * drain on SIGTERM, so a normal stop usually needs no replay at all. The hook is best-effort and
 * time-boxed — it can never hang shutdown — because the spill, not the hook, is the durability
 * guarantee (a shutdown hook does not run on {@code kill -9}).
 */
public final class WriteBehindStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(WriteBehindStore.class);

    static final int DEFAULT_THREADS = 8;
    static final int ROLLOVER_THRESHOLD = 32 * 1024 * 1024;   // per-write buffer cap
    static final long MAX_PENDING_BYTES = 128L * 1024 * 1024; // across all in-flight buffers

    /** Cap on the spill directory; past this the oldest spills are evicted. */
    static final long DEFAULT_MAX_SPILL_BYTES = 256L * 1024 * 1024;

    /** How long the shutdown hook waits for in-flight uploads before giving up. */
    static final long SHUTDOWN_FLUSH_TIMEOUT_MS = 30_000;

    /** Spill record magic — 'W''B''S''1' — so a foreign/corrupt file is rejected, not replayed. */
    private static final int SPILL_MAGIC = 0x57425331;

    /**
     * Stores with a spill dir, for the single shutdown hook. <b>Weak</b> keys: a per-user store is
     * built on every connect, so a strong set would grow without bound over a long-lived process.
     * A collected store has no in-flight uploads to flush, so dropping it is harmless.
     */
    private static final Set<WriteBehindStore> ACTIVE =
            java.util.Collections.synchronizedSet(
                    java.util.Collections.newSetFromMap(new java.util.WeakHashMap<>()));
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final AtomicLong SPILL_SEQ = new AtomicLong();

    private final ArtifactStore delegate;
    private final ExecutorService pool;
    /** Source of the request thread's session credentials for {@link CapturedSsoContext}; may be null. */
    private final SsoCredentialsResolver credentialsResolver;
    /** Local write-ahead directory, or null when spill is disabled (heap-only legacy behaviour). */
    private final Path spillDir;
    private final long maxSpillBytes;
    private final Object spillLock = new Object();
    private long spillBytes;
    private final Set<Future<?>> pending = ConcurrentHashMap.newKeySet();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition underCap = lock.newCondition();
    private long pendingBytes;
    private volatile IOException firstFailure;

    public WriteBehindStore(ArtifactStore delegate) {
        this(delegate, DEFAULT_THREADS, null, null);
    }

    public WriteBehindStore(ArtifactStore delegate, int threads) {
        this(delegate, threads, null, null);
    }

    /**
     * @param credentialsResolver the resolver whose {@link SsoCredentialsResolver#captureCurrent()}
     *        supplies a request thread's session credentials when no {@code SsoJobContext} is
     *        installed; {@code null} for a store that already carries fixed per-user credentials.
     */
    public WriteBehindStore(ArtifactStore delegate, int threads,
                            SsoCredentialsResolver credentialsResolver) {
        this(delegate, threads, credentialsResolver, null);
    }

    /**
     * Full constructor.
     *
     * @param spillDir local write-ahead directory; {@code null} keeps the legacy heap-only buffer.
     *        When non-null it is created if absent; a directory that cannot be created degrades to
     *        heap-only rather than failing construction (the upload is still attempted either way).
     */
    public WriteBehindStore(ArtifactStore delegate, int threads,
                            SsoCredentialsResolver credentialsResolver, Path spillDir) {
        this.delegate = delegate;
        this.credentialsResolver = credentialsResolver;
        this.maxSpillBytes = DEFAULT_MAX_SPILL_BYTES;
        Path resolvedSpill = spillDir == null ? null : spillDir.toAbsolutePath().normalize();
        if (resolvedSpill != null) {
            try {
                Files.createDirectories(resolvedSpill);
            } catch (IOException e) {
                log.warn("[write-behind] spill directory {} unusable ({}); continuing heap-only",
                        resolvedSpill, e.toString());
                resolvedSpill = null;
            }
        }
        this.spillDir = resolvedSpill;
        if (this.spillDir != null) {
            ACTIVE.add(this);
            registerShutdownHookOnce();
        }
        this.pool = Executors.newFixedThreadPool(Math.max(1, threads), r -> {
            Thread t = new Thread(r, "s3-write-behind");
            t.setDaemon(true);
            return t;
        });
    }

    /** The spill directory in force, or {@code null} when spill is disabled. Test/ops visibility. */
    Path spillDir() {
        return spillDir;
    }

    // ── writes — the only intercepted operations ─────────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return write(key, -1);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        if (contentLength >= 0 && contentLength >= ROLLOVER_THRESHOLD) return delegate.write(key, contentLength);
        // Capture HERE, on the caller's thread: the request/job context is resolvable now and will
        // not be by the time the pool thread flushes. See the class Javadoc.
        return new BufferedAsyncStream(key, CapturedSsoContext.capture(credentialsResolver));
    }

    /**
     * Blocks until every queued upload finished — the barrier every read-shaped operation crosses
     * first, and callable directly where a caller knows a read-back is coming.
     *
     * @throws IOException the first upload failure, if any failed
     */
    public void awaitPending() throws IOException {
        awaitPending(Long.MAX_VALUE);
    }

    /**
     * Bounded form of {@link #awaitPending()}, for a shutdown path that must not hang.
     *
     * @return true if the queue drained within {@code timeoutMs}, false if it did not
     * @throws IOException the first upload failure, if any failed
     */
    public boolean awaitPending(long timeoutMs) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeoutMs));
        List<Future<?>> waiting;
        synchronized (pending) {
            waiting = new ArrayList<>(pending);
        }
        boolean drained = true;
        for (Future<?> f : waiting) {
            try {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    drained = false;
                    break;
                }
                f.get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                drained = false;
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for pending uploads", e);
            } catch (ExecutionException e) {
                // Unwrap: the real failure is the upload's, not the Future's wrapping.
                throw asIo(e.getCause() == null ? e : e.getCause());
            } catch (java.util.concurrent.CancellationException e) {
                throw new IOException("an upload was cancelled", e);
            }
        }
        IOException f = firstFailure;
        if (f != null) throw f;
        return drained;
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

    // ── graceful shutdown ────────────────────────────────────────────────────

    /**
     * Registers one JVM shutdown hook for every spill-enabled store. One hook, not one per store,
     * and it is time-boxed per store so a stuck upload can never block process exit.
     *
     * <p>Best-effort by design: {@code kill -9} does not run shutdown hooks at all, which is
     * exactly why {@link #replaySpills()} exists. This only spares a SIGTERM stop a replay.
     */
    private static void registerShutdownHookOnce() {
        if (!SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) return;
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // Snapshot under the set's lock: ACTIVE is a synchronized WeakHashMap-backed set,
                // and iterating it while a connect adds a store would otherwise throw.
                List<WriteBehindStore> stores;
                synchronized (ACTIVE) {
                    stores = new ArrayList<>(ACTIVE);
                }
                for (WriteBehindStore store : stores) {
                    try {
                        boolean drained = store.awaitPending(SHUTDOWN_FLUSH_TIMEOUT_MS);
                        if (!drained) {
                            log.warn("[write-behind] shutdown flush timed out after {} ms; "
                                    + "undrained entries remain in the spill and will replay at next boot",
                                    SHUTDOWN_FLUSH_TIMEOUT_MS);
                        }
                    } catch (Exception e) {
                        // A failing upload must not fail shutdown; the spill still holds the bytes.
                        log.warn("[write-behind] shutdown flush reported: {}", e.toString());
                    }
                }
            }, "s3-write-behind-shutdown"));
        } catch (IllegalStateException | SecurityException e) {
            // JVM already shutting down, or a security manager forbids hooks — the spill covers it.
            log.debug("[write-behind] could not register shutdown hook: {}", e.toString());
        }
    }

    /**
     * Unregisters this store from the shutdown hook and drains what it can within a bounded wait.
     * Idempotent; safe to call from tests.
     *
     * @return true if the queue drained within {@code timeoutMs}
     */
    public boolean close(long timeoutMs) {
        ACTIVE.remove(this);
        try {
            return awaitPending(timeoutMs);
        } catch (IOException e) {
            log.warn("[write-behind] close flush reported: {}", e.toString());
            return false;
        }
    }

    // ── write-ahead spill ────────────────────────────────────────────────────

    /**
     * Uploads every surviving spill file and deletes it on success. Called once at boot for the
     * startup store, before any request is served.
     *
     * <p>A file that fails to upload is left in place and retried at the next boot (or bounded out
     * by the spill cap); a file that cannot be parsed is deleted, since replaying garbage would be
     * worse than dropping it. Never throws — boot must not fail because one artifact could not be
     * recovered.
     *
     * @return the number of spills successfully replayed
     */
    public int replaySpills() {
        Path dir = spillDir;
        if (dir == null || !Files.isDirectory(dir)) return 0;
        List<Path> files = listSpillFiles();
        int replayed = 0;
        for (Path file : files) {
            SpillRecord record = readSpill(file);
            if (record == null) {
                // Corrupt or foreign: never replay it, but do reclaim the space.
                deleteSpill(file);
                continue;
            }
            try (OutputStream out = delegate.write(record.key(), record.payload().length)) {
                out.write(record.payload());
            } catch (IOException | RuntimeException e) {
                log.warn("[write-behind] replay of {} failed ({}); keeping spill for next boot",
                        record.key(), e.toString());
                continue;
            }
            deleteSpill(file);
            replayed++;
        }
        if (replayed > 0) {
            log.info("[write-behind] replayed {} spilled upload(s) from {}", replayed, dir);
        }
        return replayed;
    }

    /** Writes one entry to the spill directory, atomically. Returns null when spill is off/failed. */
    private Path spill(ArtifactKey key, byte[] bytes) {
        Path dir = spillDir;
        if (dir == null) return null;
        byte[] keyBytes = key.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] record = new byte[8 + keyBytes.length + bytes.length];
        writeInt(record, 0, SPILL_MAGIC);
        writeInt(record, 4, keyBytes.length);
        System.arraycopy(keyBytes, 0, record, 8, keyBytes.length);
        System.arraycopy(bytes, 0, record, 8 + keyBytes.length, bytes.length);
        Path target = dir.resolve(System.nanoTime() + "-" + SPILL_SEQ.incrementAndGet() + ".spill");
        try {
            synchronized (spillLock) {
                enforceSpillCap(record.length);
                Path temp = Files.createTempFile(dir, ".tmp-", ".spill");
                Files.write(temp, record);
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
                spillBytes += record.length;
            }
            return target;
        } catch (IOException e) {
            log.warn("[write-behind] could not spill {} to disk ({}); upload still attempted",
                    key, e.toString());
            return null;
        }
    }

    /** Deletes a spill file and releases its bytes from the accounting. */
    private void deleteSpill(Path file) {
        if (file == null) return;
        try {
            synchronized (spillLock) {
                long size = Files.size(file);
                if (Files.deleteIfExists(file)) spillBytes -= size;
            }
        } catch (IOException ignored) {
            // A file that vanished is already reclaimed; the cap walk will resync the accounting.
        }
    }

    /** Evicts oldest spill files until {@code incoming} more bytes fit under the cap. */
    private void enforceSpillCap(long incoming) {
        if (spillBytes + incoming <= maxSpillBytes) return;
        List<Path> files = listSpillFiles();
        for (Path f : files) {
            if (spillBytes + incoming <= maxSpillBytes) break;
            try {
                long size = Files.size(f);
                if (Files.deleteIfExists(f)) {
                    spillBytes -= size;
                    log.warn("[write-behind] spill cap ({} bytes) reached; evicted oldest spill {}",
                            maxSpillBytes, f.getFileName());
                }
            } catch (IOException ignored) {
                // Vanished already — accounting resyncs on the next successful delete.
            }
        }
    }

    /** Spill files oldest-first (name carries a monotonic sequence). */
    private List<Path> listSpillFiles() {
        Path dir = spillDir;
        if (dir == null || !Files.isDirectory(dir)) return List.of();
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".spill"))
                    .filter(p -> !p.getFileName().toString().startsWith(".tmp-"))
                    .sorted(Comparator.comparingLong(WriteBehindStore::mtimeOf))
                    .toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    private static long mtimeOf(Path p) {
        try {
            return Files.getLastModifiedTime(p).toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    private record SpillRecord(ArtifactKey key, byte[] payload) {}

    /** Parses a spill file, or null when it is not a well-formed record. */
    private static SpillRecord readSpill(Path file) {
        byte[] raw;
        try {
            raw = Files.readAllBytes(file);
        } catch (IOException e) {
            return null;
        }
        if (raw.length < 8 || readInt(raw, 0) != SPILL_MAGIC) return null;
        int keyLen = readInt(raw, 4);
        if (keyLen <= 0 || 8 + keyLen > raw.length) return null;
        String keyString = new String(raw, 8, keyLen, java.nio.charset.StandardCharsets.UTF_8);
        byte[] payload = new byte[raw.length - 8 - keyLen];
        System.arraycopy(raw, 8 + keyLen, payload, 0, payload.length);
        try {
            return new SpillRecord(ArtifactKey.of(keyString.split("/")), payload);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static void writeInt(byte[] b, int off, int v) {
        b[off] = (byte) (v >>> 24);
        b[off + 1] = (byte) (v >>> 16);
        b[off + 2] = (byte) (v >>> 8);
        b[off + 3] = (byte) v;
    }

    private static int readInt(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16)
                | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    /**
     * Buffers small writes on the heap; close() enqueues the upload and returns immediately.
     * Crossing {@link #ROLLOVER_THRESHOLD} switches the stream into pass-through mode against a
     * directly-opened delegate upload — the caller's bytes keep flowing, just unbuffered.
     */
    private final class BufferedAsyncStream extends OutputStream {
        private final ArtifactKey key;
        /** The writer's captured identity, carried to the flush thread with the bytes. */
        private final CapturedSsoContext context;
        private java.io.ByteArrayOutputStream buffer;
        private OutputStream passThrough;
        private boolean closed;

        BufferedAsyncStream(ArtifactKey key, CapturedSsoContext context) {
            this.key = key;
            this.context = context;
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
            enqueue(key, all, context);
        }
    }

    /**
     * Enqueues one small upload, carrying the writer's captured context; blocks (backpressure)
     * while too much is in flight. When spill is on, the bytes are made durable <em>before</em> the
     * upload is submitted.
     */
    private void enqueue(ArtifactKey key, byte[] bytes, CapturedSsoContext context) {
        // Write-ahead: the spill file exists before any upload can start, so a kill after close()
        // leaves recoverable bytes. Null when spill is off or the local write failed.
        Path spill = spill(key, bytes);
        lock.lock();
        try {
            while (pendingBytes + bytes.length > MAX_PENDING_BYTES) {
                try {
                    underCap.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Backpressure wait interrupted: upload synchronously rather than drop the
                    // bytes — the caller's close() still owes an upload. This runs on the CALLER's
                    // thread, which already holds the context the bytes were written under, so no
                    // install is needed here.
                    log.warn("[write-behind] interrupted draining upload backlog; uploading {} inline", key);
                    try (OutputStream out = delegate.write(key, bytes.length)) {
                        out.write(bytes);
                        deleteSpill(spill);
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
                // Re-install the writer's identity on this pool thread for this one upload. The
                // context is per-entry (one queue can hold several users' writes); close() restores
                // this worker's previous — empty — binding before the next entry runs. A null
                // context installs nothing, leaving the legacy default-chain path untouched.
                try (AutoCloseable installed = context == null ? null : context.install();
                     OutputStream out = delegate.write(key, bytes.length)) {
                    out.write(bytes);
                }
                // Upload committed: the spill has done its job.
                deleteSpill(spill);
            } catch (IOException e) {
                // No retry: this upload is recorded once as the store's first failure and surfaced
                // at the next read-shaped call (awaitPending rethrows it). The credential context is
                // now correct, so the historical "every upload fails" is gone; a genuinely terminal
                // failure (the captured credentials expired mid-job) drops this entry and logs one
                // WARN — deliberately no retry loop, so a dead principal can never spin the pool.
                // The spill file is deliberately KEPT so the next boot replays it.
                if (firstFailure == null) firstFailure = e;
                log.error("[write-behind] upload of {} failed: {}", key, e.toString());
            } catch (Exception e) {
                // install()/the context handle close() cannot throw a checked exception of its own
                // on this path, but an unexpected RuntimeException must not silently lose the
                // failure — record it exactly as an IOException so the next read barrier surfaces it.
                IOException io = asIo(e);
                if (firstFailure == null) firstFailure = io;
                log.error("[write-behind] upload of {} failed: {}", key, io.toString());
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
