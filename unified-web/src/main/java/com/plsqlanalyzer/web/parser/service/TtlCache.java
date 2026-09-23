package com.plsqlanalyzer.web.parser.service;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * A small size-bounded cache whose entries also expire.
 *
 * <h2>Why a TTL and not just an LRU</h2>
 *
 * <p>The caches here hold <b>parsed analysis documents</b> — the largest measured is 3.6 MB of JSON
 * for one statement index, and the reader's cache holds up to 30 of them. An LRU bound alone has two
 * problems that a size limit cannot address:
 *
 * <ul>
 *   <li><b>Staleness.</b> Re-running an analysis rewrites the artifact underneath the cache. With
 *       no expiry, a frequently-read entry is refreshed to the head of the LRU on every hit and can
 *       therefore be served <i>forever</i>, so the UI keeps showing results from a superseded run.
 *       On a tool whose purpose is reporting what code contains, serving a stale answer
 *       indefinitely is the worst kind of wrong.</li>
 *   <li><b>Memory held for nothing.</b> Thirty documents stay resident whether or not anyone has
 *       looked at them since startup. A TTL returns that memory when the app goes quiet.</li>
 * </ul>
 *
 * <p>The eviction is lazy — checked on read and on write, not by a background thread. A scheduled
 * sweeper would be a second thing to configure, another timer to reason about, and would gain
 * nothing: an entry nobody reads costs only memory, which the size bound already limits.
 *
 * <p>Not thread-safe on its own; instances are wrapped in {@link Collections#synchronizedMap} by
 * the internal map, and the public methods are {@code synchronized} so a get/put pair cannot
 * interleave.
 */
public final class TtlCache<V> {

    /** Default lifetime. Long enough to serve a browsing session, short enough to notice a re-run. */
    public static final long DEFAULT_TTL_MS = 10 * 60 * 1000L;

    private record Entry<V>(V value, long expiresAt) {}

    private final long ttlMs;
    private final int maxEntries;
    private final LongSupplier clock;
    private final Map<String, Entry<V>> map;

    public TtlCache(int maxEntries, long ttlMs) {
        this(maxEntries, ttlMs, System::currentTimeMillis);
    }

    /** @param clock injectable so expiry can be tested without sleeping. */
    TtlCache(int maxEntries, long ttlMs, LongSupplier clock) {
        this.maxEntries = Math.max(1, maxEntries);
        this.ttlMs = ttlMs;
        this.clock = clock;
        this.map = new LinkedHashMap<>(32, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Entry<V>> eldest) {
                return size() > TtlCache.this.maxEntries;
            }
        };
    }

    /** The value, or null when absent or expired. An expired entry is dropped as it is found. */
    public synchronized V get(String key) {
        Entry<V> e = map.get(key);
        if (e == null) return null;
        if (isExpired(e)) {
            map.remove(key);
            return null;
        }
        return e.value();
    }

    public synchronized void put(String key, V value) {
        // Sweep on write: without this, entries that are never read again would sit until the size
        // bound pushed them out, which on a quiet system may be never.
        purgeExpired();
        map.put(key, new Entry<>(value, ttlMs <= 0 ? Long.MAX_VALUE : clock.getAsLong() + ttlMs));
    }

    public synchronized void remove(String key) {
        map.remove(key);
    }

    /** Drops everything — used when an analysis is rewritten and staleness must not wait for TTL. */
    public synchronized void clear() {
        map.clear();
    }

    /** Live entry count, expired ones excluded. For tests and diagnostics. */
    public synchronized int size() {
        purgeExpired();
        return map.size();
    }

    private boolean isExpired(Entry<V> e) {
        return clock.getAsLong() >= e.expiresAt();
    }

    private void purgeExpired() {
        for (Iterator<Map.Entry<String, Entry<V>>> it = map.entrySet().iterator(); it.hasNext(); ) {
            if (isExpired(it.next().getValue())) it.remove();
        }
    }
}
