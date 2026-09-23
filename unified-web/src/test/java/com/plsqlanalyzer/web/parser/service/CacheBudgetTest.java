package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-011 — the cache is bounded by memory, and keyed by the version it parsed.
 *
 * <h2>Why an entry count was not a bound</h2>
 *
 * <p>The previous cache allowed 30 entries. That limits memory only if entries are a similar size,
 * and they are not: the smallest cached document is kilobytes, the largest artifact in the corpus is
 * <b>49 MB</b>. Thirty small ones is nothing; thirty large ones is over a gigabyte. The bound was
 * stated in a unit unrelated to the resource, so it held when it did not matter and gave way when
 * it did.
 */
class CacheBudgetTest {

    private static final ObjectMapper M = new ObjectMapper();

    /** Values whose declared size is exactly what the test asks for, so budgets are checkable. */
    private static ByteBudgetedCache<String> sized(long maxBytes) {
        return new ByteBudgetedCache<>(maxBytes, 60_000, s -> s.length());
    }

    @Test
    void thirtyLargeEntriesCannotAllBeHeld() {
        // The exact failure the entry-count bound allowed. Under a 1,000-byte budget, thirty
        // 100-byte entries cannot coexist; under a 30-entry bound they would, whatever their size.
        ByteBudgetedCache<String> cache = sized(1_000);
        for (int i = 0; i < 30; i++) cache.put("k" + i, "x".repeat(100));

        assertTrue(cache.heldBytes() <= 1_000,
                "cache holds " + cache.heldBytes() + " bytes against a 1,000-byte budget");
        assertTrue(cache.size() < 30, "every entry was retained despite the byte budget");
        assertTrue(cache.evictions() > 0, "nothing was evicted, so the budget was never applied");
    }

    @Test
    void theBudgetHoldsRegardlessOfEntrySize() {
        // Stated as the property: whatever mixture goes in, the total never exceeds the budget.
        ByteBudgetedCache<String> cache = sized(500);
        for (int i = 0; i < 50; i++) {
            cache.put("k" + i, "x".repeat((i % 7) * 30 + 10));
            assertTrue(cache.heldBytes() <= 500,
                    "budget exceeded after " + (i + 1) + " puts: " + cache.heldBytes());
        }
    }

    @Test
    void aValueLargerThanTheWholeBudgetIsNotCached() {
        // Admitting it would evict everything to hold one item that cannot then coexist with
        // anything — the cache would thrash between single entries and serve nothing.
        ByteBudgetedCache<String> cache = sized(100);
        cache.put("small", "x".repeat(50));
        cache.put("huge", "x".repeat(5_000));

        assertNull(cache.get("huge"), "an oversized value was cached");
        assertEquals("x".repeat(50), cache.get("small"),
                "an oversized value evicted the entries that did fit");
    }

    @Test
    void theLeastRecentlyUsedEntryGoesFirst() {
        ByteBudgetedCache<String> cache = sized(250);
        cache.put("a", "x".repeat(100));
        cache.put("b", "x".repeat(100));
        cache.get("a");                       // 'a' is now the more recently used
        cache.put("c", "x".repeat(100));      // forces one eviction

        assertNotNull(cache.get("a"), "the recently used entry was evicted");
        assertNull(cache.get("b"), "the least recently used entry survived");
    }

    @Test
    void entriesStillExpire() {
        // Retained from the previous cache for the reason recorded there: with LRU alone, a
        // regularly read document is served indefinitely and a re-analysis never surfaces.
        AtomicLong now = new AtomicLong(0);
        ByteBudgetedCache<String> cache =
                new ByteBudgetedCache<>(1_000, 100, String::length, now::get);
        cache.put("k", "value");
        assertEquals("value", cache.get("k"));

        now.set(101);
        assertNull(cache.get("k"), "an expired entry was served");
        assertEquals(0, cache.heldBytes(), "expiring an entry did not release its bytes");
    }

    @Test
    void removingAnEntryReleasesItsBytes() {
        ByteBudgetedCache<String> cache = sized(1_000);
        cache.put("k", "x".repeat(100));
        assertEquals(100, cache.heldBytes());
        cache.remove("k");
        assertEquals(0, cache.heldBytes(), "removed bytes were still counted against the budget");
    }

    @Test
    void theSizeEstimateGrowsWithTheDocument() {
        // It does not need to be accurate — it orders entries against a budget. It does need to be
        // monotonic, or a large document could be admitted as though it were small.
        ObjectNode small = M.createObjectNode();
        small.put("name", "ORDERS");

        ObjectNode large = M.createObjectNode();
        var arr = large.putArray("rows");
        for (int i = 0; i < 500; i++) arr.addObject().put("name", "TABLE_" + i);

        long smallBytes = ParserAnalysisReader.estimateBytes(small);
        long largeBytes = ParserAnalysisReader.estimateBytes(large);
        assertTrue(largeBytes > smallBytes * 50,
                "the estimate does not scale with document size: " + smallBytes + " vs " + largeBytes);
    }

    @Test
    void theEstimateIsCheapEnoughToRunOnEveryPut() {
        // Serialising to measure would cost more than the read the cache exists to avoid.
        ObjectNode big = M.createObjectNode();
        var arr = big.putArray("rows");
        for (int i = 0; i < 50_000; i++) arr.addObject().put("name", "TABLE_" + i).put("n", i);

        long started = System.nanoTime();
        ParserAnalysisReader.estimateBytes(big);
        long ms = (System.nanoTime() - started) / 1_000_000;
        assertTrue(ms < 500, "estimating a 50,000-row document took " + ms + "ms");
    }
}
