package com.analyzer.queue;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One <b>first-class</b> stage of a queue job's progress.
 *
 * <p>Before this existed a job reported a single {@code currentStep} string and a {@code [n/m]}
 * counter scraped out of that string, so the UI could only ever show one bar and the hidden
 * background work (sidecar warm-up, estate facets, entity ingest, decompile) sat outside the
 * progress entirely — the bar reached 100% while real work was still running. Every stage of a
 * job is now a {@code JobStage}: it carries its own state, its own size-scaled estimate, its own
 * unit count (N/M) and its own ETA, and the job's overall percentage is the calibrated-share
 * weighted average of them.
 *
 * <p>The estimate is <b>size aware</b>: {@link #unit} names what the stage's work scales with
 * ({@code proc}/{@code class}/{@code endpoint}/{@code facet}) and {@link #estimatedMs} is
 * recomputed from the observed {@code total} times the calibrated per-unit cost
 * ({@link QueueJob#msPerUnit}). A 1,000-endpoint JAR therefore gets a different, larger estimate —
 * and a different bar — than a 30-endpoint one, which is exactly what the user asked for.
 *
 * <p>Mutable fields are volatile because the queue worker, the crawl-progress poller and the SSE
 * broadcaster read them from different threads.
 */
public final class JobStage {

    public enum State { PENDING, RUNNING, COMPLETE, SKIPPED, FAILED }

    public final String id;
    public final String label;
    /**
     * The size unit this stage's work scales with ({@code proc}, {@code class}, {@code endpoint},
     * {@code facet}), or {@code null} for a fixed-cost stage. Drives the size-scaled estimate.
     */
    public final String unit;

    /** Calibrated share of the job's total work, 0..1. Weights the overall percentage. */
    public volatile double share;
    /** Size-scaled estimate in milliseconds, recomputed as counts become known. */
    public volatile long estimatedMs;
    public volatile State state = State.PENDING;
    public volatile long done;
    public volatile long total;
    public volatile long startedAtMs;
    public volatile long finishedAtMs;

    public JobStage(String id, String label, String unit, double share) {
        this.id = id;
        this.label = label;
        this.unit = unit;
        this.share = share;
    }

    /** Begins the stage, optionally with a known unit count (0 = unknown). */
    public void start(long total) {
        this.total = Math.max(0, total);
        this.done = 0;
        this.state = State.RUNNING;
        this.startedAtMs = System.currentTimeMillis();
        this.finishedAtMs = 0;
    }

    /** Records progress within the stage. The count only ever moves forward. */
    public void advance(long done, long total) {
        if (state != State.RUNNING) start(total);
        if (total > 0) this.total = total;
        long d = Math.max(this.done, done);
        this.done = this.total > 0 ? Math.min(d, this.total) : d;
    }

    public void complete() {
        if (state == State.COMPLETE) return;
        this.state = State.COMPLETE;
        if (total > 0) done = total;
        this.finishedAtMs = System.currentTimeMillis();
        if (startedAtMs == 0) startedAtMs = finishedAtMs;
    }

    public void skip() {
        if (state == State.COMPLETE || state == State.SKIPPED) return;
        this.state = State.SKIPPED;
        this.finishedAtMs = System.currentTimeMillis();
        if (startedAtMs == 0) startedAtMs = finishedAtMs;
    }

    public void fail() {
        if (state == State.FAILED) return;
        this.state = State.FAILED;
        this.finishedAtMs = System.currentTimeMillis();
    }

    public boolean terminal() {
        return state == State.COMPLETE || state == State.SKIPPED || state == State.FAILED;
    }

    /** Wall-clock time the stage took, or 0 while it is still running / never ran. */
    public long actualMs() {
        return finishedAtMs > startedAtMs ? finishedAtMs - startedAtMs : 0;
    }

    /**
     * How much of this stage is done, 0..1.
     *
     * <p>A finished (or deliberately skipped) stage is 1; a failed one is 0. A running stage is
     * driven by its unit count when one is known ({@code done/total}) and by elapsed time against
     * its estimate otherwise, so a stage whose size is not yet known still advances the bar instead
     * of sitting at zero.
     */
    public double fraction() {
        switch (state) {
            case COMPLETE:
            case SKIPPED:
                return 1.0;
            case FAILED:
                return 0.0;
            case RUNNING:
                double byCount = total > 0 ? Math.min(1.0, (double) done / total) : 0.0;
                double byTime = (estimatedMs > 0 && startedAtMs > 0)
                        ? Math.min(0.95, (double) (System.currentTimeMillis() - startedAtMs) / estimatedMs)
                        : 0.0;
                return Math.max(byCount, byTime);
            default:
                return 0.0;
        }
    }

    /** Remaining estimate for this stage in ms ({@code estimate x (1 - fraction)}). */
    public long etaMs() {
        if (terminal()) return 0L;
        if (estimatedMs <= 0) return 0L;
        return Math.max(0L, Math.round(estimatedMs * (1.0 - fraction())));
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("label", label);
        m.put("unit", unit);
        m.put("state", state.name());
        m.put("done", done);
        m.put("total", total);
        m.put("share", share);
        m.put("estimatedMs", estimatedMs);
        m.put("elapsedMs", state == State.RUNNING && startedAtMs > 0
                ? System.currentTimeMillis() - startedAtMs
                : actualMs());
        m.put("etaMs", etaMs());
        return m;
    }

    @Override
    public String toString() {
        return id + "[" + state + " " + done + "/" + total + " est=" + estimatedMs + "ms]";
    }
}
