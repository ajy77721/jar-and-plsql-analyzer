package com.analyzer.queue;

import com.plsqlanalyzer.web.config.ConfigDirService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a job's stage list at submit time and folds the completed run back into the calibration
 * store — the two halves of the accurate progress engine that need Spring wiring.
 *
 * <p>{@link #prepare(QueueJob)} is called by {@link AnalysisQueueService} the moment a job starts
 * running, before the executor is dispatched. It creates one {@link JobStage} per
 * {@link StageModel.Spec}, applies the calibrated shares, sets the calibrated per-unit costs on the
 * job and produces the initial size-scaled estimate. The executors then only mutate stages through
 * {@link QueueJob#beginStage}/{@link QueueJob#advanceStage}/{@link QueueJob#completeStage}, and
 * {@link ProgressMath} keeps the derived percentage and ETA current.
 *
 * <p>{@link #finish(QueueJob)} runs once the executor returns: it completes any stage the executor
 * left open, measures the per-unit cost and the per-stage share from the run's own timings, and
 * feeds them to {@link ProgressCalibrationStore}. That is what makes the next run's estimates —
 * and the shares that weight the bar — better than this run's, with no manual tuning.
 */
@Service
public class ProgressEstimator {

    private static final Logger log = LoggerFactory.getLogger(ProgressEstimator.class);

    private final ProgressCalibrationStore store;

    public ProgressEstimator(ConfigDirService configDirService) {
        this.store = new ProgressCalibrationStore(configDirService.getConfigDir());
    }

    /** Test seam: build an estimator over a store rooted anywhere. */
    ProgressEstimator(ProgressCalibrationStore store) {
        this.store = store;
    }

    /**
     * Installs the calibrated stage model on a job. No-op for a job type the engine does not model,
     * which keeps its legacy single-step progress untouched.
     */
    public void prepare(QueueJob job) {
        if (job == null) return;
        List<StageModel.Spec> specs = StageModel.forType(job.type);
        if (specs.isEmpty()) return;

        String dialect = StageModel.dialectOf(job.type);
        ProgressCalibrationStore.Snapshot cal = store.get(dialect);

        job.progressDialect = dialect;
        job.msPerUnit = new LinkedHashMap<>(cal.msPerUnit());
        job.baselineEstimatedMs = baselineFor(job, dialect, cal);

        job.stages.clear();
        for (StageModel.Spec spec : specs) {
            job.stages.add(new JobStage(spec.id(), spec.label(), spec.unit(),
                    cal.share(spec.id(), spec.share())));
        }
        ProgressMath.recompute(job);
    }

    /**
     * Finalises a successfully completed job: closes any open stage, measures the run and folds it
     * into the calibration store, then recomputes so the reported numbers reflect the new
     * calibration. Never throws — a calibration problem must not fail a finished analysis.
     */
    public void finish(QueueJob job) {
        if (job == null || job.progressDialect == null || job.stages.isEmpty()) return;
        try {
            for (JobStage s : job.stages) {
                if (s.state == JobStage.State.RUNNING) s.complete();
                else if (s.state == JobStage.State.PENDING) s.skip();
            }

            // Per-unit costs: aggregate every stage sharing a unit (e.g. the three proc stages) so
            // one unit's cost is measured over all the work it covers. Sub-20 ms stages are noise
            // and are excluded so a fast/mocked run cannot poison the store.
            Map<String, Long> msByUnit = new LinkedHashMap<>();
            Map<String, Long> unitsByUnit = new LinkedHashMap<>();
            Map<String, Double> shares = new LinkedHashMap<>();
            long totalActual = 0L;
            for (JobStage s : job.stages) {
                long actual = s.actualMs();
                if (actual > 0) totalActual += actual;
                if (s.unit != null && s.total > 0 && actual >= 20) {
                    msByUnit.merge(s.unit, actual, Long::sum);
                    unitsByUnit.merge(s.unit, s.total, Long::sum);
                }
            }
            Map<String, Double> observedMsPerUnit = new LinkedHashMap<>();
            for (Map.Entry<String, Long> e : msByUnit.entrySet()) {
                long units = unitsByUnit.getOrDefault(e.getKey(), 0L);
                if (units > 0) observedMsPerUnit.put(e.getKey(), (double) e.getValue() / units);
            }
            if (totalActual > 0) {
                for (JobStage s : job.stages) {
                    long actual = s.actualMs();
                    if (actual > 0) shares.put(s.id, (double) actual / totalActual);
                }
            }
            if (!observedMsPerUnit.isEmpty() || !shares.isEmpty()) {
                store.observe(job.progressDialect, observedMsPerUnit, shares);
                job.calibrated = true;
            }

            // Re-apply the (now updated) calibration so the final reported figures are consistent.
            ProgressCalibrationStore.Snapshot cal = store.get(job.progressDialect);
            job.msPerUnit = new LinkedHashMap<>(cal.msPerUnit());
            for (JobStage s : job.stages) s.share = cal.share(s.id, s.share);
            ProgressMath.recompute(job);
            job.progressPercent = 100;
            job.etaMs = 0L;
        } catch (Exception e) {
            log.debug("progress calibration update failed for {}: {}", job.id, e.toString());
        }
    }

    /** Marks the running stage failed and recomputes; no calibration is learned from a failure. */
    public void fail(QueueJob job) {
        if (job == null || job.stages.isEmpty()) return;
        for (JobStage s : job.stages) if (s.state == JobStage.State.RUNNING) s.fail();
        ProgressMath.recompute(job);
    }

    /**
     * The starting estimate for the whole job before any stage count is known: the default unit
     * counts times the calibrated per-unit costs, floored by the upload's byte size for a JAR/WAR
     * (a 50 MB WAR cannot finish as fast as a 50 KB one). Corrected within seconds once the parse
     * stage reports its real class count.
     */
    private static long baselineFor(QueueJob job, String dialect,
                                    ProgressCalibrationStore.Snapshot cal) {
        long baseline = 0L;
        for (StageModel.Spec spec : StageModel.defaults(dialect)) {
            if (spec.unit() != null) {
                long count = StageModel.DEFAULT_UNIT_COUNTS.getOrDefault(spec.unit(), 10L);
                baseline += Math.round(count * cal.msPerUnit(spec.unit()));
            }
        }
        Object fileSize = job.metadata.get("fileSize");
        if (fileSize instanceof Number n && n.longValue() > 0) {
            // ~6 ms/KB is a loose order-of-magnitude floor; the class count corrects it moments later.
            baseline = Math.max(baseline, Math.round(n.longValue() / 1024.0 * 6.0));
        }
        return Math.max(10_000L, baseline);
    }
}
