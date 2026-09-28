package com.plsqlanalyzer.web.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * The API latency budget's enforcement, driven by {@link ApiLoggingFilter} once per request.
 *
 * <p>A request over {@code budgetMs} logs one <b>distinct</b> WARN line — {@code [SLO] GET /path took
 * Xms (budget 200ms)} — so an over-budget endpoint surfaces in the logs without the filter drowning
 * in the same line on every call: the same endpoint+status is warned at most once per hour. Every
 * over-budget request still counts toward an hourly roll-up ({@code [SLO] hourly summary: ...}) so a
 * sustained violation is visible as a number even while its per-call line is suppressed.
 *
 * <p>The budget and the clock are injected so the rate limiting and the window roll are testable
 * without waiting an hour; {@code budgetMs <= 0} disables the per-call warning (the summary still
 * rolls).
 */
class ApiSloMonitor {

    private static final Logger log = LoggerFactory.getLogger(ApiSloMonitor.class);

    /** The rate-limit and summary window. */
    static final long HOUR_MS = 3_600_000L;

    /** How many endpoints the hourly roll-up names before it just reports the total. */
    private static final int SUMMARY_TOP = 10;

    private final long budgetMs;
    private final LongSupplier clock;

    /** endpoint+status -> the epoch-ms of its last WARN; the gate on the once-per-hour line. */
    private final ConcurrentHashMap<String, Long> lastWarned = new ConcurrentHashMap<>();
    /** endpoint+status -> over-budget request count in the current window. */
    private final ConcurrentHashMap<String, Long> violations = new ConcurrentHashMap<>();

    private final AtomicLong windowStart = new AtomicLong();

    /** Test hooks: how many WARN lines and summaries were actually emitted. */
    private final AtomicLong warned = new AtomicLong();
    private final AtomicLong summaries = new AtomicLong();

    ApiSloMonitor(long budgetMs) {
        this(budgetMs, System::currentTimeMillis);
    }

    ApiSloMonitor(long budgetMs, LongSupplier clock) {
        this.budgetMs = budgetMs;
        this.clock = clock;
        this.windowStart.set(clock.getAsLong());
    }

    /** The exact WARN text for an over-budget call. */
    static String message(String method, String path, long elapsedMs, long budgetMs) {
        return "[SLO] " + method + " " + path + " took " + elapsedMs + "ms (budget " + budgetMs + "ms)";
    }

    /**
     * Records one finished request. Logs the distinct WARN when this endpoint+status has not been
     * warned within the hour, and rolls the hourly summary when the window has elapsed.
     */
    void record(String method, String uri, int status, long elapsedMs) {
        long now = clock.getAsLong();

        if (budgetMs > 0 && elapsedMs > budgetMs) {
            String key = method + " " + uri + " " + status;
            violations.merge(key, 1L, Long::sum);

            boolean[] first = {false};
            lastWarned.compute(key, (k, previous) -> {
                if (previous == null || now - previous >= HOUR_MS) {
                    first[0] = true;
                    return now;
                }
                return previous;
            });
            if (first[0]) {
                warned.incrementAndGet();
                log.warn(message(method, uri, elapsedMs, budgetMs));
            }
        }

        rollWindow(now);
    }

    private void rollWindow(long now) {
        long start = windowStart.get();
        if (now - start < HOUR_MS) return;
        if (!windowStart.compareAndSet(start, now)) return; // another thread already rolled it

        Map<String, Long> snapshot = new HashMap<>(violations);
        violations.clear();
        summaries.incrementAndGet();

        if (snapshot.isEmpty()) {
            log.warn("[SLO] hourly summary: no requests over budget ({}ms) in the last hour", budgetMs);
            return;
        }
        long total = snapshot.values().stream().mapToLong(Long::longValue).sum();
        StringBuilder top = new StringBuilder();
        snapshot.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(SUMMARY_TOP)
                .forEach(e -> {
                    if (top.length() > 0) top.append(", ");
                    top.append(e.getKey()).append(" -> ").append(e.getValue());
                });
        log.warn("[SLO] hourly summary: {} over-budget request(s) across {} endpoint(s) "
                + "(budget {}ms): {}", total, snapshot.size(), budgetMs, top);
    }

    long budgetMs() { return budgetMs; }
    long warnedCount() { return warned.get(); }
    long summaryCount() { return summaries.get(); }
}
