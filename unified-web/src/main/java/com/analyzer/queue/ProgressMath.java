package com.analyzer.queue;

/**
 * The estimation math for the progress engine — pure, side-effect-free except for writing the
 * derived fields back onto the {@link QueueJob}, so it can be unit-tested without a Spring context.
 *
 * <h2>Size-aware estimation</h2>
 *
 * <p>Every stage's estimate is <b>scaled by the content it processes</b>. A stage that knows its
 * unit count ({@code total}) and whose unit has a calibrated per-unit cost gets
 * {@code estimate = total x msPerUnit[unit]}. The stages whose count is not yet known are then
 * scaled by the same implied job total, derived from the sized stages' known share:
 *
 * <pre>impliedTotal = knownMs / knownShare</pre>
 *
 * <p>So the moment a JAR parse reports "800 classes" the whole job's estimate grows to
 * {@code 800 x msPerClass / shareOfParse}, and the 30-endpoint and 1,000-endpoint runs get
 * genuinely different bars and ETAs. When nothing is sized yet the estimate falls back to the
 * calibrated default baseline.
 *
 * <h2>The percentage</h2>
 *
 * <p>The overall percentage is the <b>calibrated-share weighted</b> average of the stage fractions,
 * not {@code completedSteps / totalSteps}: a 24%-share decompile moves the bar ten times as much as
 * a 2%-share db-call facet, which is the point of calibrating the shares. The total ETA is
 * {@code estimate x (1 - progress)}.
 */
public final class ProgressMath {

    /** The fallback total estimate (ms) when neither the calibration nor the content says better. */
    public static final long DEFAULT_BASELINE_MS = 120_000L;

    private ProgressMath() {}

    /**
     * Recomputes every stage's size-scaled estimate and the job's weighted percent / ETA / step
     * counters from the stages' current state.
     */
    public static void recompute(QueueJob job) {
        if (job == null || job.stages.isEmpty()) return;

        // Pass 1: size-scaled estimates for every stage whose unit count is known.
        double knownMs = 0.0;
        double knownShare = 0.0;
        for (JobStage s : job.stages) {
            Double per = s.unit == null ? null : job.msPerUnit.get(s.unit);
            if (per != null && per > 0 && s.total > 0) {
                s.estimatedMs = Math.max(1L, Math.round(s.total * per));
                knownMs += s.estimatedMs;
                knownShare += s.share;
            } else {
                s.estimatedMs = 0L; // provisional; scaled below
            }
        }

        // The implied job total: how large the whole run is, extrapolated from what we know.
        double baseline = job.baselineEstimatedMs > 0 ? job.baselineEstimatedMs : DEFAULT_BASELINE_MS;
        double impliedTotal = knownShare > 0 ? knownMs / knownShare : baseline;

        // Pass 2: scale the remaining stages, then weight the fractions by calibrated share.
        long sumMs = 0L;
        double sumShare = 0.0;
        double weighted = 0.0;
        for (JobStage s : job.stages) {
            if (s.estimatedMs <= 0) {
                s.estimatedMs = Math.max(1L, Math.round(s.share * impliedTotal));
            }
            sumMs += s.estimatedMs;
            sumShare += s.share;
            weighted += s.share * s.fraction();
        }

        double fraction = sumShare > 0 ? weighted / sumShare : 0.0;
        fraction = Math.max(0.0, Math.min(1.0, fraction));

        job.estimatedTotalMs = sumMs;
        job.etaMs = Math.max(0L, Math.round(sumMs * (1.0 - fraction)));
        job.progressPercent = (int) Math.round(fraction * 100.0);

        int terminal = 0;
        JobStage running = null;
        for (JobStage s : job.stages) {
            if (s.terminal()) terminal++;
            if (running == null && s.state == JobStage.State.RUNNING) running = s;
        }
        job.stepNumber = terminal;
        job.totalSteps = job.stages.size();
        job.currentStage = running != null ? running.id : null;
        if (running != null) job.currentStep = running.label;
    }
}
