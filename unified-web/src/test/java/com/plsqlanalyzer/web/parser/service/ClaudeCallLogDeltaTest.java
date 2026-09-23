package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.storage.page.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Claude Sessions modal re-fetched the entire append-only call log every 5 s in order to
 * display seven totals. Two things had to become true for that to stop.
 *
 * <p><b>The totals must be memoisable.</b> An aggregate over an append-only log is a scan; the only
 * safe key for caching it is a value that cannot go backwards. Here that is the log's own length,
 * so the test that matters is that the cached answer and a freshly computed answer agree —
 * PLAN-08 §8.2's "assert rollup ≡ recomputed sum".
 *
 * <p><b>A delta must be addressable.</b> {@code sinceSeq} has to return exactly what was appended
 * after that line, with sequence numbers that count lines in the log rather than rows in the
 * response, or the client's next cursor skips records.
 */
class ClaudeCallLogDeltaTest {

    private ClaudeCallLogger logger(InMemoryArtifactStore store) {
        return new ClaudeCallLogger(store);
    }

    private void logCalls(ClaudeCallLogger l, String analysis, int n) {
        for (int i = 1; i <= n; i++) {
            l.logCall(analysis, "sess-" + ((i % 2) + 1), "FULL_SCAN", "ep" + i, i,
                    100L * i, 200L * i, 10L * i, i % 5 != 0, i % 5 == 0 ? "boom" : null);
        }
    }

    @Test
    @DisplayName("watermark is zero before the first call and grows with every append")
    void watermarkTracksTheLog() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ClaudeCallLogger l = logger(store);

        assertEquals(0, l.watermark("ERP_RUN"), "no log yet");
        logCalls(l, "ERP_RUN", 1);
        long one = l.watermark("ERP_RUN");
        assertTrue(one > 0);
        logCalls(l, "ERP_RUN", 1);
        assertTrue(l.watermark("ERP_RUN") > one, "an append must move the ETag");
    }

    @Test
    @DisplayName("the memoised totals equal a freshly recomputed aggregate")
    void cachedTotalsEqualRecomputedTotals() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ClaudeCallLogger l = logger(store);
        logCalls(l, "ERP_RUN", 12);

        Map<String, Object> first = l.getGlobalStats();     // computes
        Map<String, Object> second = l.getGlobalStats();    // memoised
        Map<String, Object> fresh = logger(store).getGlobalStats();   // cold instance

        assertEquals(12, ((Number) first.get("totalCalls")).intValue());
        assertEquals(first.get("totalCalls"), second.get("totalCalls"));
        assertEquals(first.get("totalDurationMs"), fresh.get("totalDurationMs"),
                "a cache that can disagree with the log is worse than no cache");
        assertEquals(first.get("totalPromptBytes"), fresh.get("totalPromptBytes"));
        assertEquals(first.get("errors"), fresh.get("errors"));
        assertEquals(first.get("sessionCount"), fresh.get("sessionCount"));
    }

    @Test
    @DisplayName("an append invalidates the memo — a stale total is the failure mode that matters")
    void appendInvalidatesTheMemo() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ClaudeCallLogger l = logger(store);
        logCalls(l, "ERP_RUN", 3);
        assertEquals(3, ((Number) l.getGlobalStats().get("totalCalls")).intValue());

        logCalls(l, "ERP_RUN", 2);
        assertEquals(5, ((Number) l.getGlobalStats().get("totalCalls")).intValue(),
                "the memo must not survive an append");
    }

    @Test
    @DisplayName("sinceSeq returns only later records, numbered by line in the log")
    void deltaReturnsOnlyLaterRecords() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ClaudeCallLogger l = logger(store);
        logCalls(l, "ERP_RUN", 5);

        Page<Map<String, Object>> all = l.callLog("ERP_RUN", null, 200);
        assertEquals(5, all.items().size());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L),
                all.items().stream().map(m -> ((Number) m.get("seq")).longValue()).toList());

        Page<Map<String, Object>> delta = l.callLog("ERP_RUN", 3L, 200);
        assertEquals(List.of(4L, 5L),
                delta.items().stream().map(m -> ((Number) m.get("seq")).longValue()).toList());
        assertFalse(delta.hasMore());
    }

    @Test
    @DisplayName("a caught-up client gets an empty page — the steady state of the poll")
    void caughtUpClientGetsNothing() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ClaudeCallLogger l = logger(store);
        logCalls(l, "ERP_RUN", 4);

        assertTrue(l.callLog("ERP_RUN", 4L, 200).items().isEmpty());
    }

    @Test
    @DisplayName("a delta longer than the limit pages, with a cursor")
    void deltaPagesAtTheLimit() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ClaudeCallLogger l = logger(store);
        logCalls(l, "ERP_RUN", 10);

        Page<Map<String, Object>> page = l.callLog("ERP_RUN", 0L, 4);
        assertEquals(4, page.items().size());
        assertTrue(page.hasMore());
        assertNotNull(page.page().nextCursor());
        assertEquals(4L, ((Number) page.items().get(3).get("seq")).longValue(),
                "the cursor must resume at the last row actually returned");
    }

    @Test
    @DisplayName("an analysis with no call log is an empty page, not a failure")
    void unknownAnalysisIsEmpty() {
        ClaudeCallLogger l = logger(new InMemoryArtifactStore());
        Page<Map<String, Object>> page = l.callLog("NEVER_RAN", null, 200);
        assertTrue(page.items().isEmpty());
        assertFalse(page.hasMore());
    }
}
