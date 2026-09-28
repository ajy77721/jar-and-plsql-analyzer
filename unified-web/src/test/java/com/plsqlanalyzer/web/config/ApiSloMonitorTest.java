package com.plsqlanalyzer.web.config;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The 200ms budget's enforcement: the WARN text, the once-per-endpoint-per-hour rate limit, and the
 * hourly roll-up. The clock is a stub so an hour passes in a variable assignment.
 */
class ApiSloMonitorTest {

    private static final String URI = "/api/parser/analyses/ORACLE~P~DEV~DEMO/call-tree/ERP$P1";

    @Test
    void warnTextIsTheDocumentedFormat() {
        assertEquals("[SLO] GET /api/x took 1234ms (budget 200ms)",
                ApiSloMonitor.message("GET", "/api/x", 1234, 200));
    }

    @Test
    void aRequestUnderBudgetWarnsNothing() {
        AtomicLong now = new AtomicLong(0);
        ApiSloMonitor monitor = new ApiSloMonitor(200, now::get);

        monitor.record("GET", URI, 200, 200);
        monitor.record("GET", URI, 1, 200);

        assertEquals(0, monitor.warnedCount(), "at or under budget must not warn");
    }

    @Test
    void theSameEndpointWarnsOncePerHour() {
        AtomicLong now = new AtomicLong(0);
        ApiSloMonitor monitor = new ApiSloMonitor(200, now::get);

        // Five over-budget calls in the same hour: one line, five counted.
        for (int i = 0; i < 5; i++) monitor.record("GET", URI, 200, 900);
        assertEquals(1, monitor.warnedCount(), "the same endpoint must warn once per hour");

        // A second endpoint+status is its own key, so it warns immediately.
        monitor.record("GET", URI, 500, 900);
        assertEquals(2, monitor.warnedCount(), "a different status is a different endpoint key");

        // An hour later the original may warn again.
        now.addAndGet(ApiSloMonitor.HOUR_MS);
        monitor.record("GET", URI, 200, 900);
        assertEquals(3, monitor.warnedCount(), "an hour later the endpoint warns again");
    }

    @Test
    void anHourlySummaryRollsOncePerWindow() {
        AtomicLong now = new AtomicLong(0);
        ApiSloMonitor monitor = new ApiSloMonitor(200, now::get);

        monitor.record("GET", URI, 200, 900);
        assertEquals(0, monitor.summaryCount(), "no summary inside the first window");

        now.addAndGet(ApiSloMonitor.HOUR_MS + 1);
        monitor.record("GET", URI, 200, 10); // under budget, but the window still rolls
        assertEquals(1, monitor.summaryCount(), "the window must roll after an hour");

        now.addAndGet(ApiSloMonitor.HOUR_MS + 1);
        monitor.record("GET", URI, 200, 900);
        assertEquals(2, monitor.summaryCount(), "and again the next hour");
    }

    @Test
    void zeroBudgetDisablesThePerCallWarning() {
        AtomicLong now = new AtomicLong(0);
        ApiSloMonitor monitor = new ApiSloMonitor(0, now::get);

        monitor.record("GET", URI, 200, 10_000);

        assertEquals(0, monitor.warnedCount(), "budget 0 disables enforcement");
    }
}
