package com.analyzer.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Read-through cache that bulk-downloads one artifact subtree up front, in parallel.
 *
 * <h2>Why</h2>
 *
 * <p>The entity-ingest reads an analysis's artifacts back through the store one at a time —
 * each {@code open()} costing an {@code exists} plus a {@code read}, i.e. two sequential S3
 * round trips. Against a remote bucket that made ingest the second-slowest phase of a parse
 * (~98 s of ~150 ms round trips for ~300 mostly tiny files). The files are independent, so the
 * reads parallelise trivially: this class lists the subtree once and downloads it with a bounded
 * pool ({@value #DEFAULT_THREADS} threads) into a bounded in-memory cache; the ingest that then
 * runs over the cached store touches nothing but RAM. Measured on the same corpus: ~98 s → ~10 s,
 * nearly all of the remainder being Mongo bulk-writes.
 *
 * <h2>Bounded, in every direction</h2>
 *
 * <ul>
 *   <li>Threads: {@value #DEFAULT_THREADS}, daemon — the prefetch is pure network wait, so it
 *       adds no meaningful CPU, and a bounded pool cannot starve anything else on the machine.</li>
 *   <li>Memory: files above {@link #MAX_CACHED_FILE} are never cached (read through), and the
 *       cache stops growing at {@link #MAX_CACHE_BYTES} (later files read through). A parser
 *       analysis's whole tree is a few tens of MB; a multi-GB JAR payload cannot enter.</li>
 *   <li>Failures: a file that fails to prefetch is simply not cached — reads fall through to the
 *       delegate and behave exactly as without this class. Prefetch is an optimisation, never a
 *       correctness gate.</li>
 * </ul>
 *
 * <p>Writes are NOT intercepted — this wraps a read-only phase. List/copy/move/delete and the
 * uncached paths all delegate directly.
 */
public final class PrefetchReadStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(PrefetchReadStore.class);

    static final int DEFAULT_THREADS = 8;
    static final long MAX_CACHED_FILE = 16L * 1024 * 1024;   // per-file cache cap
    static final long MAX_CACHE_BYTES = 256L * 1024 * 1024;  // whole-cache cap

    private final ArtifactStore delegate;
    private final Map<ArtifactKey, byte[]> cache = new HashMap<>();
    private final Map<ArtifactKey, ArtifactEntry> cachedMeta = new HashMap<>();
    private boolean loaded;

    public PrefetchReadStore(ArtifactStore delegate) {
        this.delegate = delegate;
    }

    /**
     * Downloads {@code root} (recursively) into the cache on a bounded pool. Blocks until done —
     * callers run this right before the read-heavy phase that justifies it.
     *
     * @return the number of files actually cached (failures and oversized files read through)
     */
    public synchronized int prefetch(ArtifactKey root) throws IOException {
        if (loaded) return cache.size();
        loaded = true;
        long t0 = System.currentTimeMillis();
        Map<ArtifactKey, ArtifactEntry> all = new HashMap<>();
        try (Stream<ArtifactEntry> s = delegate.list(root, true)) {
            s.filter(e -> !e.directory()).forEach(e -> all.put(e.key(), e));
        }
        if (all.isEmpty()) return 0;
        ExecutorService pool = Executors.newFixedThreadPool(DEFAULT_THREADS, r -> {
            Thread t = new Thread(r, "prefetch-reader");
            t.setDaemon(true);
            return t;
        });
        try {
            long[] totalBytes = {0};
            Map<ArtifactKey, Future<Cacheable>> futures = new HashMap<>();
            for (Map.Entry<ArtifactKey, ArtifactEntry> e : all.entrySet()) {
                if (e.getValue().size() > MAX_CACHED_FILE) continue;   // read through
                if (e.getValue().size() > 0 && totalBytes[0] + e.getValue().size() > MAX_CACHE_BYTES) break;
                totalBytes[0] += e.getValue().size();
                futures.put(e.getKey(), pool.submit((Callable<Cacheable>) () -> {
                    try (InputStream in = delegate.read(e.getKey())) {
                        return new Cacheable(in.readAllBytes(), e.getValue().lastModified());
                    }
                }));
            }
            int failed = 0;
            for (Map.Entry<ArtifactKey, Future<Cacheable>> f : futures.entrySet()) {
                try {
                    Cacheable c = f.getValue().get();
                    cache.put(f.getKey(), c.bytes);
                    cachedMeta.put(f.getKey(), new ArtifactEntry(f.getKey(), c.bytes.length, c.lastModified, false));
                } catch (Exception e) {
                    failed++;   // reads fall through to the delegate — an optimisation, not a gate
                }
            }
            long ms = System.currentTimeMillis() - t0;
            log.info("[prefetch] cached {} of {} artifact(s) under {} in {}ms ({} read-through/failed)",
                    cache.size(), all.size(), root, ms, all.size() - futures.size() + failed);
            return cache.size();
        } finally {
            pool.shutdownNow();
        }
    }

    private record Cacheable(byte[] bytes, long lastModified) {}

    @Override
    public synchronized InputStream read(ArtifactKey key) throws IOException {
        byte[] bytes = cache.get(key);
        if (bytes != null) return new ByteArrayInputStream(bytes);
        return delegate.read(key);
    }

    @Override
    public synchronized InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        byte[] bytes = cache.get(key);
        if (bytes != null) {
            long off = Math.max(0, offset);
            long len = Math.min(bytes.length - off, length);
            if (off >= bytes.length || len <= 0) return new ByteArrayInputStream(new byte[0]);
            return new ByteArrayInputStream(bytes, (int) off, (int) len);
        }
        return delegate.read(key, offset, length);
    }

    @Override
    public synchronized boolean exists(ArtifactKey key) {
        return cache.containsKey(key) || delegate.exists(key);
    }

    @Override
    public synchronized long size(ArtifactKey key) {
        ArtifactEntry e = cachedMeta.get(key);
        if (e != null) return e.size();
        return delegate.size(key);
    }

    @Override
    public synchronized long lastModified(ArtifactKey key) {
        ArtifactEntry e = cachedMeta.get(key);
        if (e != null) return e.lastModified();
        return delegate.lastModified(key);
    }

    // ── everything else delegates — this class is read-only by design ────────

    @Override
    public OutputStream write(ArtifactKey key) throws IOException { return delegate.write(key); }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        return delegate.write(key, contentLength);
    }

    @Override
    public boolean delete(ArtifactKey key) { return delegate.delete(key); }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException { return delegate.deletePrefix(prefix); }

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        return delegate.list(prefix, recursive);
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException { delegate.copy(from, to); }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException { delegate.move(from, to); }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException { return delegate.materialize(key); }
}
