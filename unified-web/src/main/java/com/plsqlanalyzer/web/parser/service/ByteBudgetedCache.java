package com.plsqlanalyzer.web.parser.service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/**
 * A cache bounded by how much memory it holds, not by how many things it holds.
 *
 * <h2>Why the entry count was the wrong bound</h2>
 *
 * <p>The previous cache allowed 30 entries. That is a memory limit only if entries are a similar
 * size, and here they are not: the smallest cached document is a few kilobytes and the largest
 * artifact in the corpus is <b>49 MB</b>. Thirty of the small ones is nothing; thirty of the large
 * ones is well over a gigabyte. The bound was expressed in a unit that does not correspond to the
 * resource being protected, so it held whenever it did not matter and gave way whenever it did.
 *
 * <p>Bounding by bytes means the worst case is stated directly and cannot be exceeded by a change
 * in what happens to be cached.
 *
 * <h2>Sizes are estimated once, at insertion</h2>
 *
 * <p>Measuring a parsed document exactly means serialising it, which for a 49 MB artifact costs
 * more than the read the cache exists to avoid. The size is therefore computed once when the entry
 * goes in and stored alongside it — the estimate has to be cheap and stable, not precise, because
 * it is used to compare against a budget rather than to report memory use.
 *
 * <h2>Expiry as well as eviction</h2>
 *
 * <p>Retained from the previous cache, and for the reason recorded there: with LRU alone, a
 * document read regularly is served indefinitely, so re-running an analysis leaves the UI showing
 * the previous run's answers with nothing short of a restart to correct it.
 */
public final class ByteBudgetedCache<V> {

    /** Default ceiling. Chosen against the 49 MB largest artifact: room for a working set, not a copy of the corpus. */
    public static final long DEFAULT_MAX_BYTES = 128L * 1024 * 1024;

    public static final long DEFAULT_TTL_MS = 10 * 60 * 1000L;

    private record Entry<V>(V value, long bytes, long expiresAt) {}

    private final long maxBytes;
    private final long ttlMs;
    private final LongSupplier clock;
    private final ToLongFunction<V> sizer;

    /** Access-ordered, so the eldest entry is the least recently used. */
    private final LinkedHashMap<String, Entry<V>> map = new LinkedHashMap<>(16, 0.75f, true);

    private long heldBytes;
    private long evictions;

    public ByteBudgetedCache(long maxBytes, long ttlMs, ToLongFunction<V> sizer) {
        this(maxBytes, ttlMs, sizer, System::currentTimeMillis);
    }

    ByteBudgetedCache(long maxBytes, long ttlMs, ToLongFunction<V> sizer, LongSupplier clock) {
        this.maxBytes = Math.max(1, maxBytes);
        this.ttlMs = ttlMs;
        this.sizer = sizer;
        this.clock = clock;
    }

    public synchronized V get(String key) {
        Entry<V> e = map.get(key);
        if (e == null) return null;
        if (clock.getAsLong() >= e.expiresAt()) {
            map.remove(key);
            heldBytes -= e.bytes();
            return null;
        }
        return e.value();
    }

    /**
     * Inserts a value, evicting least-recently-used entries until the budget holds.
     *
     * <p>A value larger than the whole budget is <b>not</b> cached, rather than being cached after
     * evicting everything else. Admitting it would empty the cache to hold one item that then
     * cannot be kept alongside anything — the cache would thrash between single entries and serve
     * nothing, which is worse than not caching that one value.
     */
    public synchronized void put(String key, V value) {
        if (value == null) return;
        long bytes = Math.max(1, sizer.applyAsLong(value));

        Entry<V> existing = map.remove(key);
        if (existing != null) heldBytes -= existing.bytes();

        if (bytes > maxBytes) return;

        while (heldBytes + bytes > maxBytes && !map.isEmpty()) {
            var eldest = map.entrySet().iterator().next();
            map.remove(eldest.getKey());
            heldBytes -= eldest.getValue().bytes();
            evictions++;
        }
        map.put(key, new Entry<>(value, bytes, clock.getAsLong() + ttlMs));
        heldBytes += bytes;
    }

    /** Drops one entry, releasing its bytes. */
    public synchronized void remove(String key) {
        Entry<V> e = map.remove(key);
        if (e != null) heldBytes -= e.bytes();
    }

    public synchronized void clear() {
        map.clear();
        heldBytes = 0;
    }

    /** Bytes currently held, by the same estimate used to admit entries. */
    public synchronized long heldBytes() {
        return heldBytes;
    }

    public synchronized int size() {
        return map.size();
    }

    /** How many entries the budget has forced out — a cache thrashing says so here. */
    public synchronized long evictions() {
        return evictions;
    }

    public long maxBytes() {
        return maxBytes;
    }
}
