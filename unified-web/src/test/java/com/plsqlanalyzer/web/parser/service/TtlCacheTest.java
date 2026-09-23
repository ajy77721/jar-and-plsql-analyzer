package com.plsqlanalyzer.web.parser.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Entries must expire, not merely be evicted when the cache is full.
 *
 * <p>The reader's cache was a pure LRU bounded at 30 parsed documents. An access moves an entry to
 * the head, so a document read regularly was never evicted — and re-running an analysis rewrites
 * the artifact underneath it. The UI could therefore serve the previous run's answers indefinitely,
 * with nothing short of a restart to correct it. On a tool whose job is reporting what code
 * contains, an indefinitely stale answer is the worst failure available to it.
 *
 * <p>The clock is injected so expiry is asserted rather than slept through: a test that waits for
 * real time is slow, and one that waits slightly too little is flaky.
 */
class TtlCacheTest {

    private final AtomicLong now = new AtomicLong(1_000_000L);

    private TtlCache<String> cache(int max, long ttl) {
        return new TtlCache<>(max, ttl, now::get);
    }

    private void advance(long ms) { now.addAndGet(ms); }

    @Test
    @DisplayName("a value is served until its TTL, and not after")
    void expiresOnTime() {
        TtlCache<String> c = cache(10, 60_000);
        c.put("a", "one");

        advance(59_999);
        assertEquals("one", c.get("a"), "still inside the window");

        advance(1);
        assertNull(c.get("a"), "at the TTL the entry must be gone, not merely stale");
    }

    @Test
    @DisplayName("reading does NOT extend the lifetime — the staleness bug in one assertion")
    void readingDoesNotRenew() {
        TtlCache<String> c = cache(10, 60_000);
        c.put("hot", "v1");

        // Read it constantly, as a busy screen would. Under the old LRU this is exactly what kept a
        // superseded document resident for the life of the process.
        for (int i = 0; i < 10; i++) {
            advance(10_000);
            c.get("hot");
        }
        assertNull(c.get("hot"),
                "frequent reads must not keep a superseded document alive; that was the defect");
    }

    @Test
    @DisplayName("re-putting a key restarts its clock")
    void writeRenews() {
        TtlCache<String> c = cache(10, 60_000);
        c.put("a", "v1");
        advance(50_000);
        c.put("a", "v2");          // re-read from the store
        advance(50_000);
        assertEquals("v2", c.get("a"), "a fresh read should be good for a fresh TTL");
    }

    @Test
    @DisplayName("the size bound still applies")
    void stillBounded() {
        TtlCache<String> c = cache(3, 60_000);
        for (int i = 0; i < 6; i++) c.put("k" + i, "v" + i);
        assertEquals(3, c.size(), "the LRU bound must survive the addition of a TTL");
        assertNull(c.get("k0"), "the oldest should have been evicted");
        assertEquals("v5", c.get("k5"));
    }

    @Test
    @DisplayName("expired entries are reclaimed, not just hidden")
    void expiredEntriesAreReclaimed() {
        TtlCache<String> c = cache(100, 1_000);
        for (int i = 0; i < 50; i++) c.put("k" + i, "v" + i);
        assertEquals(50, c.size());

        advance(1_001);
        // These hold megabytes of parsed JSON in production; hiding them is not enough, the memory
        // has to come back.
        assertEquals(0, c.size(), "expired entries must be dropped, not merely reported as missing");
    }

    @Test
    @DisplayName("clear() drops everything, for when an analysis is rewritten")
    void clearIsImmediate() {
        TtlCache<String> c = cache(10, 60_000);
        c.put("a", "1");
        c.put("b", "2");
        c.clear();
        assertEquals(0, c.size());
        assertNull(c.get("a"));
    }

    @Test
    @DisplayName("a zero or negative TTL means never expire")
    void zeroTtlKeepsForever() {
        TtlCache<String> c = cache(10, 0);
        c.put("a", "1");
        advance(Long.MAX_VALUE / 4);
        assertEquals("1", c.get("a"), "0 must be an explicit opt-out, not an instant expiry");
    }
}
