package com.analyzer.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Disk-backed read-through cache wrapped around another {@link ArtifactStore}, so a repeated read
 * of an artifact that lives in S3 costs one local file open instead of a network round trip.
 *
 * <h2>What is cached, and where</h2>
 *
 * <p>Artifact bytes mirror to {@code {app.data-dir}/cache/store/{storeName}/{key}} as real files,
 * and small JSON sidecars under {@code .../.meta/} and {@code .../.listings/} record what a read
 * needs to stay honest.
 *
 * <h2>The heap micro-cache (small artifacts only)</h2>
 *
 * <p>Beneath the disk layer sits a <b>strictly capped</b> in-heap LRU for the artifacts that are
 * read over and over and are tiny — a sidecar, an index header, a facet summary. Only reads whose
 * bytes are <b>at most {@value #HEAP_MAX_ENTRY_BYTES}</b> qualify, and the whole thing is capped at
 * {@code app.cache.heap.max-mb} (default {@value #DEFAULT_HEAP_MAX_MB} MB; {@code 0} disables it
 * entirely). A multi-GB JAR payload can never enter it: an entry larger than the per-entry bound is
 * served from disk and never copied, so the bound on total heap is the cap, not the corpus.
 *
 * <p>It is populated lazily, only from artifacts actually read — never a preload — and it is
 * invalidated by the same mutations that invalidate the disk layer (write/delete/deletePrefix/move
 * of the key or its subtree). Entries age out of the heap after {@link #revalidateAfterMs} so the
 * heap can never serve an artifact for longer than the disk layer's own revalidation window.
 *
 * <h2>Read path</h2>
 *
 * <ol>
 *   <li>A fresh heap entry exists for the key → serve its bytes directly (no disk, no network).</li>
 *   <li>A local file exists and its metadata is younger than {@value #DEFAULT_REVALIDATE_MS} ms
 *       (24 h) → serve it.</li>
 *   <li>Older than that → one cheap {@link ArtifactStore#size} / {@link ArtifactStore#lastModified}
 *       probe against the wrapped store (a HEAD on S3). Same size and last-modified → serve the
 *       local copy and refresh its timestamp; anything else → discard and refetch. This is the only
 *       out-of-band-edit window, and it is bounded at 24 h.</li>
 *   <li>Miss → delegate read, mirror the bytes to disk atomically (write a sibling temp file, then
 *       move), and serve from the local copy.</li>
 * </ol>
 *
 * <h2>Write path — the wrapped store is the source of truth</h2>
 *
 * <p>Every mutation delegates to the wrapped store <b>first</b>; only once it has succeeded does the
 * cache invalidate. A {@code write} returns a stream whose {@code close()} is the commit point, so
 * invalidation happens after the delegate's own close, not before. Writes never populate the cache —
 * the next read fetches once and mirrors, which keeps the write path off the critical path of an
 * analysis that writes thousands of artifacts.
 *
 * <h2>Invalidation</h2>
 *
 * <p>A mutation at key {@code K} drops:
 * <ul>
 *   <li>the cached entry (bytes + metadata) for {@code K} — or the whole {@code K} subtree for
 *       {@code deletePrefix}/{@code move};</li>
 *   <li>the heap micro-cache entry for {@code K} (or every entry under {@code K}'s prefix for
 *       {@code deletePrefix}/{@code move}), by the same rule, so the heap can never outlive the
 *       disk layer's own invalidation;</li>
 *   <li>the cached listing at every ancestor of {@code K} <em>and</em> at {@code K} itself, for both
 *       the shallow and the recursive form, so a child appearing or disappearing refreshes its
 *       parent; and</li>
 *   <li>for a prefix delete/move, every cached listing at or under that prefix.</li>
 * </ul>
 * The listing cache file name is the Base64url of the prefix, so an ancestor's file is computed
 * directly rather than found by scanning; an app write is O(key depth) of local deletes.
 *
 * <h2>Size cap</h2>
 *
 * <p>After a mirror the cache is trimmed opportunistically on a daemon thread — never on the caller's
 * thread, so it can never block a read. Trim runs at most once every
 * {@value #TRIM_MIN_INTERVAL_MS} ms: it walks the entry files, and if their total exceeds
 * {@code app.cache.disk.max-mb} it deletes the least-recently-accessed first (access is the file's
 * own mtime, refreshed on a serve), deleting a file's metadata sidecar with it.
 *
 * <h2>What is deliberately not cached</h2>
 *
 * <p>{@link #materialize} delegates: it is the rare "I genuinely need a {@code Path}" escape hatch,
 * and handing callers a shared cache file they might mutate is not worth the saved download.
 * Ranged reads are cached only when the whole artifact already is — and are never heap-cached
 * (they bypass the disk layer's whole-object path, so they bypass the heap with it). Listings are
 * never heap-cached either; a listing over {@link #LISTING_CACHE_MAX_ENTRIES} is not even disk-
 * cached.
 */
public final class CachingArtifactStore implements RequestScopedArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(CachingArtifactStore.class);

    /** How long a mirror may be served before one HEAD decides whether it is still current. */
    public static final long DEFAULT_REVALIDATE_MS = 24L * 60 * 60 * 1000;

    /** Do not trim more often than this; the check is a directory walk, not free. */
    static final long TRIM_MIN_INTERVAL_MS = 30_000;

    /** Refreshing a file's access time on every read would be a write per read; batch it. */
    private static final long TOUCH_AFTER_MS = 60_000;

    /** Above this many entries a listing is streamed through but not persisted. */
    static final int LISTING_CACHE_MAX_ENTRIES = 50_000;

    /** The largest artifact the heap micro-cache will hold, in bytes. */
    static final long HEAP_MAX_ENTRY_BYTES = 256L * 1024;

    /** Default total heap cap, in MB — overridable with {@code app.cache.heap.max-mb}. */
    static final long DEFAULT_HEAP_MAX_MB = 64;

    private static final int COPY_BUFFER = 256 * 1024;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String storeName;
    private final ArtifactStore delegate;
    private final Path cacheRoot;
    private final Path entryRoot;
    private final Path metaRoot;
    private final Path listingRoot;
    private final long maxBytes;
    private final long revalidateAfterMs;

    /**
     * The strictly capped in-heap LRU, or {@code null} when disabled ({@code app.cache.heap.max-mb=0}).
     * Bounded by {@link #HEAP_MAX_ENTRY_BYTES} per entry and by its own byte budget in total.
     */
    private final HeapCache heap;

    private final ExecutorService trimPool = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "artifact-cache-trim");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean trimming = new AtomicBoolean();
    private final AtomicLong lastTrimMs = new AtomicLong();

    /**
     * @param storeName   {@code parser} or {@code jar} — the first path segment, so the two routed
     *                    features never collide even though they share one cache root
     * @param delegate    the store whose reads are cached and to which all mutations are delegated
     * @param cacheRoot   {@code {app.data-dir}/cache/store/}
     * @param maxBytes    LRU cap in bytes; {@code <= 0} means unbounded
     */
    public CachingArtifactStore(String storeName, ArtifactStore delegate, Path cacheRoot, long maxBytes) {
        this(storeName, delegate, cacheRoot, maxBytes, DEFAULT_REVALIDATE_MS,
                DEFAULT_HEAP_MAX_MB * 1024L * 1024L);
    }

    /** Constructor for tests that need to control the revalidation window. */
    public CachingArtifactStore(String storeName, ArtifactStore delegate, Path cacheRoot,
                                long maxBytes, long revalidateAfterMs) {
        this(storeName, delegate, cacheRoot, maxBytes, revalidateAfterMs,
                DEFAULT_HEAP_MAX_MB * 1024L * 1024L);
    }

    /**
     * Full constructor, for tests that need to control the heap cap as well as the revalidation
     * window.
     *
     * @param heapMaxBytes total heap micro-cache cap in bytes; {@code <= 0} disables the heap cache
     */
    public CachingArtifactStore(String storeName, ArtifactStore delegate, Path cacheRoot,
                                long maxBytes, long revalidateAfterMs, long heapMaxBytes) {
        this.storeName = Objects.requireNonNull(storeName, "storeName");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.cacheRoot = cacheRoot.toAbsolutePath().normalize();
        this.entryRoot = this.cacheRoot.resolve(storeName);
        // Metadata and listings live OUTSIDE the entry tree so a legitimate artifact whose key
        // begins with ".meta" or ".listings" can never collide with our bookkeeping.
        this.metaRoot = this.cacheRoot.resolve(".meta").resolve(storeName);
        this.listingRoot = this.cacheRoot.resolve(".listings").resolve(storeName);
        this.maxBytes = maxBytes;
        this.revalidateAfterMs = Math.max(0, revalidateAfterMs);
        this.heap = heapMaxBytes > 0 ? new HeapCache(heapMaxBytes) : null;
    }

    /** The store this cache wraps — reads fall through to it on a miss, mutations always do. */
    public ArtifactStore delegate() {
        return delegate;
    }

    /**
     * The cache wraps the per-user router, so a fan-out must capture the routing scope from the
     * router inside — capturing from the cache itself would carry nothing. A wrapped store that has
     * no request-scoped routing (local, MongoDB) yields {@link Scope#NONE}, leaving those paths
     * exactly as they were.
     */
    @Override
    public Scope captureScope() {
        return delegate instanceof RequestScopedArtifactStore routed
                ? routed.captureScope()
                : Scope.NONE;
    }

    /** This feature's cache directory, {@code {app.data-dir}/cache/store/{storeName}/}. */
    public Path cacheDir() {
        return entryRoot;
    }

    // ── read path ────────────────────────────────────────────────────────────

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        InputStream fromHeap = heapRead(key);
        if (fromHeap != null) return fromHeap;

        Path file = hitFile(key);
        if (file != null) {
            touch(file);
            try {
                return serveLocal(key, file);
            } catch (IOException race) {
                // Trim (or an operator clear) deleted the file between the hit check and the open;
                // fall through and refetch rather than failing a read that has a valid answer.
            }
        }
        return fetchAndServe(key);
    }

    /**
     * The heap micro-cache's answer for {@code key}, or {@code null} to consult the disk layer.
     *
     * <p>An entry older than {@link #revalidateAfterMs} is dropped rather than served: the heap must
     * never keep an artifact alive longer than the disk layer's own revalidation window, or an
     * out-of-band edit the disk layer would have caught would be masked indefinitely.
     */
    private InputStream heapRead(ArtifactKey key) {
        if (heap == null) return null;
        HeapCache.Entry entry = heap.get(key);
        if (entry == null) return null;
        // Strictly stricter than the disk layer's own `age <= window` rule (which is `>=` here), so
        // the heap can never serve an artifact the disk layer would have revalidated — and with a
        // zero window it never serves at all, matching a disabled cache.
        if (System.currentTimeMillis() - entry.storedAt() >= revalidateAfterMs) {
            heap.remove(key);
            return null;
        }
        return new ByteArrayInputStream(entry.bytes());
    }

    /**
     * Serves a local file, promoting a small one into the heap micro-cache on the way out.
     *
     * <p>The size check happens before any bytes are copied, so a large artifact is streamed
     * straight from disk and never buffered; only an artifact within
     * {@link #HEAP_MAX_ENTRY_BYTES} is read into memory, and only when the heap is enabled.
     */
    private InputStream serveLocal(ArtifactKey key, Path file) throws IOException {
        if (heap != null) {
            long size = Files.size(file);
            if (size >= 0 && size <= HEAP_MAX_ENTRY_BYTES) {
                byte[] bytes = Files.readAllBytes(file);
                heap.put(key, bytes);
                return new ByteArrayInputStream(bytes);
            }
        }
        return Files.newInputStream(file);
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0, was " + offset);
        Path file = entryPath(key);
        if (Files.isRegularFile(file)) {
            // A cached artifact serves ranges locally, matching LocalArtifactStore's seek semantics.
            touch(file);
            try {
                InputStream in = Files.newInputStream(file);
                try {
                    in.skipNBytes(offset);
                    return length < 0 ? in : new BoundedInputStream(in, length);
                } catch (IOException e) {
                    in.close();
                    throw e;
                }
            } catch (java.nio.file.NoSuchFileException race) {
                // Deleted between the check and the open — fall through to the wrapped store.
            }
        }
        return delegate.read(key, offset, length);
    }

    /**
     * The local file to serve for {@code key}, or null when the wrapped store must be consulted.
     * A file that is too old is revalidated with one cheap probe; a size or last-modified mismatch
     * (an out-of-band S3 edit) evicts it.
     */
    private Path hitFile(ArtifactKey key) {
        Path file = entryPath(key);
        if (!Files.isRegularFile(file)) return null;
        EntryMeta meta = readMeta(metaPath(key));
        if (meta == null) {
            // Bytes without metadata cannot be trusted or aged — drop and refetch.
            deleteQuietly(file);
            return null;
        }
        long age = System.currentTimeMillis() - meta.fetchedAt;
        if (age <= revalidateAfterMs) return file;

        long remoteSize = delegate.size(key);
        long remoteModified = delegate.lastModified(key);
        boolean unchanged = remoteSize >= 0
                && remoteSize == meta.size
                && (remoteModified <= 0 || remoteModified == meta.lastModified);
        if (unchanged) {
            writeMeta(metaPath(key), new EntryMeta(System.currentTimeMillis(), meta.size,
                    remoteModified > 0 ? remoteModified : meta.lastModified));
            return file;
        }
        deleteEntry(key);
        return null;
    }

    /** Delegates a read, mirrors the bytes to disk, and serves from the local copy. */
    private InputStream fetchAndServe(ArtifactKey key) throws IOException {
        InputStream in = delegate.read(key);
        try {
            Path target = mirror(key, in);
            return serveLocal(key, target);
        } catch (IOException cacheFailure) {
            // The mirror failed on local disk, not in storage: fall back to a straight read so a
            // broken cache can never be worse than no cache. The delegate stream was consumed.
            log.debug("[cache] mirror of {} failed ({}), reading through", key, cacheFailure.getMessage());
            closeQuietly(in);
            return delegate.read(key);
        }
    }

    private Path mirror(ArtifactKey key, InputStream in) throws IOException {
        Path target = entryPath(key);
        Files.createDirectories(target.getParent());
        // Temp sibling so the final move is same-filesystem and therefore atomic — a reader sees
        // either no file or the complete one, never a partial artifact.
        Path temp = Files.createTempFile(target.getParent(), "." + target.getFileName() + ".", ".tmp");
        long size = 0;
        try {
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(temp), COPY_BUFFER)) {
                byte[] buf = new byte[COPY_BUFFER];
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                    size += n;
                }
            }
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            deleteQuietly(temp);
            throw e;
        } finally {
            closeQuietly(in);
        }
        try {
            writeMeta(metaPath(key), new EntryMeta(System.currentTimeMillis(), size, delegate.lastModified(key)));
        } catch (RuntimeException ignored) {
            // A missing sidecar only costs a refetch next time; the bytes are still correct.
        }
        scheduleTrim();
        return target;
    }

    // ── metadata reads ───────────────────────────────────────────────────────

    @Override
    public boolean exists(ArtifactKey key) {
        return freshMeta(key) != null || delegate.exists(key);
    }

    @Override
    public long size(ArtifactKey key) {
        EntryMeta meta = freshMeta(key);
        return meta != null ? meta.size : delegate.size(key);
    }

    @Override
    public long lastModified(ArtifactKey key) {
        EntryMeta meta = freshMeta(key);
        return meta != null ? meta.lastModified : delegate.lastModified(key);
    }

    /** Cached metadata for a key whose mirror is still within its revalidation window, else null. */
    private EntryMeta freshMeta(ArtifactKey key) {
        if (!Files.isRegularFile(entryPath(key))) return null;
        EntryMeta meta = readMeta(metaPath(key));
        if (meta == null) return null;
        return System.currentTimeMillis() - meta.fetchedAt <= revalidateAfterMs ? meta : null;
    }

    // ── writes: delegate first, then invalidate ──────────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return invalidating(delegate.write(key), key);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        return invalidating(delegate.write(key, contentLength), key);
    }

    /**
     * Wraps the delegate's stream so the cache is invalidated only once the delegate has actually
     * committed on {@code close()}. An abandoned stream leaves the previous artifact in place and
     * the cache untouched, matching the store's atomic-on-close contract.
     */
    private OutputStream invalidating(OutputStream delegateStream, ArtifactKey key) {
        return new FilterOutputStream(delegateStream) {
            private boolean closed;

            // FilterOutputStream.write(byte[],int,int) loops single bytes; delegate the bulk form.
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                if (closed) return;
                closed = true;
                super.close();
                // Commit succeeded — the delegate is now authoritative for this key.
                invalidateKey(key);
            }
        };
    }

    @Override
    public boolean delete(ArtifactKey key) {
        boolean removed = delegate.delete(key);
        invalidateKey(key);
        return removed;
    }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException {
        int removed = delegate.deletePrefix(prefix);
        invalidateSubtree(prefix);
        return removed;
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        delegate.copy(from, to);
        invalidateKey(to);
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        delegate.move(from, to);
        invalidateSubtree(from);
        invalidateKey(to);
    }

    // ── listings ─────────────────────────────────────────────────────────────

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        Path file = listingPath(prefix, recursive);
        List<ArtifactEntry> cached = readListing(file);
        if (cached != null) return cached.stream();

        List<ArtifactEntry> entries = new ArrayList<>();
        try (Stream<ArtifactEntry> s = delegate.list(prefix, recursive)) {
            s.forEach(entries::add);
        }
        if (entries.size() <= LISTING_CACHE_MAX_ENTRIES) {
            writeListing(file, entries);
        }
        return entries.stream();
    }

    // ── pass-through ─────────────────────────────────────────────────────────

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        // Deliberately uncached: the caller gets a real file it could mutate, and a mutated cache
        // copy would be a stale answer no probe would catch. Rare enough not to matter.
        return delegate.materialize(key);
    }

    // ── cache housekeeping ───────────────────────────────────────────────────

    /**
     * Empties this feature's cache subtree and returns the bytes freed. Does not touch the other
     * feature's subtree; use {@link DiskCacheAdmin} for a deployment-wide clear.
     */
    public long clear() {
        long freed = DiskCacheAdmin.sizeOf(entryRoot) + DiskCacheAdmin.sizeOf(metaRoot)
                + DiskCacheAdmin.sizeOf(listingRoot);
        DiskCacheAdmin.deleteContents(entryRoot);
        DiskCacheAdmin.deleteContents(metaRoot);
        DiskCacheAdmin.deleteContents(listingRoot);
        if (heap != null) heap.clear();
        return freed;
    }

    /**
     * Trims the cache to {@link #maxBytes} by deleting the least-recently-accessed entries first.
     *
     * @return the number of artifact files deleted
     */
    int trimNow() {
        if (maxBytes <= 0 || !Files.isDirectory(entryRoot)) return 0;
        List<Path> files = new ArrayList<>();
        long[] total = {0};
        try (Stream<Path> walk = Files.walk(entryRoot)) {
            walk.filter(Files::isRegularFile)
                .filter(p -> !p.startsWith(metaRoot) && !p.startsWith(listingRoot))
                .forEach(p -> {
                    files.add(p);
                    try {
                        total[0] += Files.size(p);
                    } catch (IOException ignored) {
                        // vanished mid-walk
                    }
                });
        } catch (IOException e) {
            return 0;
        }
        if (total[0] <= maxBytes) return 0;

        files.sort(Comparator.comparingLong(CachingArtifactStore::accessTime));
        int deleted = 0;
        for (Path file : files) {
            if (total[0] <= maxBytes) break;
            long size;
            try {
                size = Files.size(file);
            } catch (IOException e) {
                continue;
            }
            if (deleteQuietly(file)) {
                deleteQuietly(metaPath(relativeKey(file)));
                total[0] -= size;
                deleted++;
            }
        }
        if (deleted > 0) {
            log.info("[cache] trim deleted {} artifact(s) from {} to stay under {} bytes",
                    deleted, entryRoot, maxBytes);
        }
        return deleted;
    }

    private void scheduleTrim() {
        long now = System.currentTimeMillis();
        if (now - lastTrimMs.get() < TRIM_MIN_INTERVAL_MS) return;
        if (!trimming.compareAndSet(false, true)) return;
        lastTrimMs.set(now);
        try {
            trimPool.submit(() -> {
                try {
                    trimNow();
                } finally {
                    trimming.set(false);
                }
            });
        } catch (RejectedExecutionException e) {
            trimming.set(false);
        }
    }

    private static long accessTime(Path p) {
        try {
            return Files.getLastModifiedTime(p).toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    private void touch(Path file) {
        try {
            long now = System.currentTimeMillis();
            if (now - Files.getLastModifiedTime(file).toMillis() > TOUCH_AFTER_MS) {
                Files.setLastModifiedTime(file, FileTime.fromMillis(now));
            }
        } catch (IOException ignored) {
            // best effort — a file that vanished is handled by the caller's read
        }
    }

    // ── invalidation ─────────────────────────────────────────────────────────

    /** Drops just the cached bytes and metadata for one key, leaving listings alone. */
    private void deleteEntry(ArtifactKey key) {
        deleteQuietly(entryPath(key));
        deleteQuietly(metaPath(key));
        if (heap != null) heap.remove(key);
    }

    /** Drops the entry for one key and every cached listing that could have named it. */
    private void invalidateKey(ArtifactKey key) {
        deleteQuietly(entryPath(key));
        deleteQuietly(metaPath(key));
        if (heap != null) heap.remove(key);
        for (ArtifactKey a = key; a != null; a = a.parent()) {
            deleteListingAt(a);
        }
    }

    /** Drops a whole subtree: used by prefix delete and by a move's source. */
    private void invalidateSubtree(ArtifactKey prefix) {
        DiskCacheAdmin.deleteRecursively(entryPath(prefix));
        DiskCacheAdmin.deleteRecursively(metaPath(prefix));
        if (heap != null) heap.removePrefix(prefix);
        deleteListingsUnder(prefix);
        for (ArtifactKey a = prefix; a != null; a = a.parent()) {
            deleteListingAt(a);
        }
    }

    private void deleteListingAt(ArtifactKey prefix) {
        deleteQuietly(listingPath(prefix, true));
        deleteQuietly(listingPath(prefix, false));
    }

    /** Deletes every cached listing whose prefix is {@code prefix} or a descendant of it. */
    private void deleteListingsUnder(ArtifactKey prefix) {
        if (!Files.isDirectory(listingRoot)) return;
        String p = prefix.toString();
        try (Stream<Path> children = Files.list(listingRoot)) {
            children.forEach(f -> {
                String key = decodeListingName(f.getFileName().toString());
                if (key != null && (key.equals(p) || key.startsWith(p + "/"))) deleteQuietly(f);
            });
        } catch (IOException ignored) {
            // A listing we cannot see cannot be deleted here; it will simply be regenerated later.
        }
    }

    // ── paths and (de)serialization ──────────────────────────────────────────

    private Path entryPath(ArtifactKey key) {
        return key.toPath(entryRoot);
    }

    private Path metaPath(ArtifactKey key) {
        Path base = key.toPath(metaRoot);
        return base.resolveSibling(base.getFileName() + ".json");
    }

    /** Reconstructs a key from a cache file under the entry root (the inverse of {@link #entryPath}). */
    private ArtifactKey relativeKey(Path file) {
        return ArtifactKey.of(entryRoot.relativize(file).toString().replace(java.io.File.separatorChar, '/')
                .split("/"));
    }

    private Path listingPath(ArtifactKey prefix, boolean recursive) {
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(prefix.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return listingRoot.resolve(encoded + (recursive ? "-r.json" : "-n.json"));
    }

    /** The prefix string encoded in a listing file name, or null if the name is not one of ours. */
    private static String decodeListingName(String name) {
        int dash = name.lastIndexOf('-');
        if (dash <= 0 || !name.endsWith(".json")) return null;
        try {
            byte[] raw = Base64.getUrlDecoder().decode(name.substring(0, dash));
            return new String(raw, java.nio.charset.StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private record EntryMeta(long fetchedAt, long size, long lastModified) {}

    private EntryMeta readMeta(Path file) {
        try {
            JsonNode n = JSON.readTree(file.toFile());
            if (n == null || !n.hasNonNull("fetchedAt") || !n.hasNonNull("size")) return null;
            return new EntryMeta(n.get("fetchedAt").asLong(), n.get("size").asLong(),
                    n.hasNonNull("lastModified") ? n.get("lastModified").asLong() : -1L);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private void writeMeta(Path file, EntryMeta meta) {
        ObjectNode node = JSON.createObjectNode();
        node.put("fetchedAt", meta.fetchedAt());
        node.put("size", meta.size());
        node.put("lastModified", meta.lastModified());
        atomicWrite(file, node.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private List<ArtifactEntry> readListing(Path file) {
        if (!Files.isRegularFile(file)) return null;
        try {
            JsonNode root = JSON.readTree(file.toFile());
            JsonNode arr = root == null ? null : root.get("entries");
            if (!(arr instanceof ArrayNode)) return null;
            List<ArtifactEntry> out = new ArrayList<>(arr.size());
            for (JsonNode e : arr) {
                String key = e.get("key").asText();
                ArtifactKey k = ArtifactKey.of(key.split("/"));
                out.add(new ArtifactEntry(k, e.path("size").asLong(),
                        e.path("lastModified").asLong(), e.path("directory").asBoolean()));
            }
            return out;
        } catch (IOException | RuntimeException e) {
            // A corrupt listing is not fatal: throw it away and re-list from the source.
            deleteQuietly(file);
            return null;
        }
    }

    private void writeListing(Path file, List<ArtifactEntry> entries) {
        ObjectNode root = JSON.createObjectNode();
        root.put("fetchedAt", System.currentTimeMillis());
        ArrayNode arr = root.putArray("entries");
        for (ArtifactEntry e : entries) {
            ObjectNode n = arr.addObject();
            n.put("key", e.key().toString());
            n.put("size", e.size());
            n.put("lastModified", e.lastModified());
            n.put("directory", e.directory());
        }
        try {
            atomicWrite(file, root.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (RuntimeException ignored) {
            // Caching the listing is an optimisation; failing to persist it must not fail the call.
        }
    }

    private static void atomicWrite(Path target, byte[] bytes) {
        try {
            Files.createDirectories(target.getParent());
            Path temp = Files.createTempFile(target.getParent(), "." + target.getFileName() + ".", ".tmp");
            try {
                Files.write(temp, bytes);
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                deleteQuietly(temp);
                throw new RuntimeException("Failed to write cache file " + target, e);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write cache file " + target, e);
        }
    }

    private static boolean deleteQuietly(Path path) {
        try {
            return Files.deleteIfExists(path);
        } catch (IOException e) {
            return false;
        }
    }

    private static void closeQuietly(InputStream in) {
        try {
            in.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    /**
     * A strictly byte-capped LRU over artifact bytes, in access order.
     *
     * <p>Guarded by its own monitor rather than a concurrent map: the critical sections are a hash
     * lookup plus an occasional eviction, and a read must never be blocked by anything slower. The
     * cap is enforced on insert, so the total held is never more than {@code maxBytes} plus the one
     * entry being inserted (itself bounded by {@link #HEAP_MAX_ENTRY_BYTES} by the caller).
     */
    static final class HeapCache {

        record Entry(byte[] bytes, long storedAt) {}

        private final long maxBytes;
        private final LinkedHashMap<ArtifactKey, Entry> entries =
                new LinkedHashMap<>(16, 0.75f, true); // accessOrder = true → eldest = least recently used
        private long bytes;

        HeapCache(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        synchronized Entry get(ArtifactKey key) {
            return entries.get(key);
        }

        synchronized void put(ArtifactKey key, byte[] value) {
            Entry previous = entries.put(key, new Entry(value, System.currentTimeMillis()));
            if (previous != null) bytes -= previous.bytes().length;
            bytes += value.length;
            // Evict least-recently-used entries until the cap is honoured.
            var it = entries.entrySet().iterator();
            while (bytes > maxBytes && it.hasNext()) {
                Entry evicted = it.next().getValue();
                bytes -= evicted.bytes().length;
                it.remove();
            }
        }

        synchronized void remove(ArtifactKey key) {
            Entry removed = entries.remove(key);
            if (removed != null) bytes -= removed.bytes().length;
        }

        /** Removes every entry whose key is {@code prefix} or sits under it. */
        synchronized void removePrefix(ArtifactKey prefix) {
            String p = prefix.toString();
            var it = entries.entrySet().iterator();
            while (it.hasNext()) {
                var e = it.next();
                String s = e.getKey().toString();
                if (s.equals(p) || s.startsWith(p + "/")) {
                    bytes -= e.getValue().bytes().length;
                    it.remove();
                }
            }
        }

        synchronized void clear() {
            entries.clear();
            bytes = 0;
        }

        /** Test hook: current number of heap entries. */
        synchronized int size() {
            return entries.size();
        }

        /** Test hook: current bytes held. */
        synchronized long bytes() {
            return bytes;
        }
    }

    /** Caps a cached range read at {@code remaining} bytes, mirroring LocalArtifactStore. */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream in;
        private long remaining;

        BoundedInputStream(InputStream in, long limit) {
            this.in = in;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) return -1;
            int b = in.read();
            if (b >= 0) remaining--;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) return -1;
            int n = in.read(b, off, (int) Math.min(len, remaining));
            if (n > 0) remaining -= n;
            return n;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(in.available(), remaining);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
