package com.analyzer.queue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The accurate progress engine: the stage model, the size-aware estimation math and the
 * self-updating calibration. These are the three things the user's complaint turns on — bars that
 * scale with content, ETAs that mean something, and background work that is visible.
 */
class ProgressEstimatorTest {

    private static QueueJob jarJob(long fileSize) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", "app.jar");
        meta.put("fileSize", fileSize);
        return new QueueJob(QueueJob.Type.JAR_UPLOAD, "Analyze: app.jar", meta);
    }

    private static QueueJob plsqlJob() {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("entryPoint", "ERP_USP_RUN_CYCLE");
        meta.put("owner", "ANALYZER");
        meta.put("objectType", "PROCEDURE");
        meta.put("dbType", "ORACLE");
        return new QueueJob(QueueJob.Type.PARSER_ANALYSIS, "Parse: ERP_USP_RUN_CYCLE", meta);
    }

    private static ProgressEstimator estimator(Path dir) {
        return new ProgressEstimator(new ProgressCalibrationStore(dir));
    }

    // ── Stage model ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("both sides declare every stage, in order, with shares that sum to one")
    void stageModelDeclaresEveryStage() {
        var plsql = StageModel.forType(QueueJob.Type.PARSER_ANALYSIS);
        assertEquals(8, plsql.size(), "PL/SQL has eight first-class stages");
        assertEquals(java.util.List.of(
                        "connect-discover", "source-fetch", "parse", "call-graph", "save",
                        "sidecar-warmup", "estate-facets", "entity-ingest"),
                plsql.stream().map(StageModel.Spec::id).toList());

        var jar = StageModel.forType(QueueJob.Type.JAR_UPLOAD);
        assertEquals(9, jar.size(), "JAR has nine first-class stages");
        assertEquals(java.util.List.of(
                        "parse-classes", "config-extract", "call-graph", "save-document",
                        "endpoint-outputs", "decompile", "summary-tree", "db-calls", "ingest"),
                jar.stream().map(StageModel.Spec::id).toList());

        assertEquals(1.0, plsql.stream().mapToDouble(StageModel.Spec::share).sum(), 1e-9);
        assertEquals(1.0, jar.stream().mapToDouble(StageModel.Spec::share).sum(), 1e-9);

        // The background work is modelled, not hidden: warm-up and ingest are real stages.
        assertNotNull(StageModel.forType(QueueJob.Type.JAR_UPLOAD).stream()
                .filter(s -> s.id().equals("decompile")).findFirst().orElse(null));
        assertNotNull(StageModel.forType(QueueJob.Type.PARSER_ANALYSIS).stream()
                .filter(s -> s.id().equals("sidecar-warmup")).findFirst().orElse(null));

        // Unmodelled job types keep the legacy single-step progress.
        assertTrue(StageModel.forType(QueueJob.Type.CLAUDE_ENRICH).isEmpty());
    }

    // ── Size-aware estimation ────────────────────────────────────────────────

    @Test
    @DisplayName("the estimate scales with the discovered class count (30x content => ~30x estimate)")
    void estimateScalesWithContentSize(@TempDir Path dir) {
        ProgressEstimator est = estimator(dir);

        QueueJob small = jarJob(10_000L);
        est.prepare(small);
        small.advanceStage("parse-classes", 30, 30);

        QueueJob large = jarJob(10_000L);
        est.prepare(large);
        large.advanceStage("parse-classes", 900, 900);

        // The decompile stage's own size-scaled estimate must grow with the class count, because
        // its implied job total is extrapolated from the parse stage's known share.
        long smallDecompile = small.stage("decompile").estimatedMs;
        long largeDecompile = large.stage("decompile").estimatedMs;
        assertTrue(largeDecompile > smallDecompile * 20,
                "a 30x larger class count must produce a far larger estimate: "
                        + smallDecompile + " -> " + largeDecompile);

        assertTrue(large.estimatedTotalMs > small.estimatedTotalMs * 20,
                "the whole-job estimate must scale with content size");
        assertTrue(large.etaMs > small.etaMs, "the ETA must scale with content size too");
    }

    @Test
    @DisplayName("a 1,000-endpoint JAR gets a different bar than a 30-endpoint one")
    void endpointStagesScaleWithEndpointCount(@TempDir Path dir) {
        ProgressEstimator est = estimator(dir);
        QueueJob job = jarJob(1_000_000L);
        est.prepare(job);

        long before = job.stage("decompile").estimatedMs;
        job.advanceStage("decompile", 0, 1000);
        long after = job.stage("decompile").estimatedMs;
        assertEquals(1000.0 * job.msPerUnit.get("endpoint"), after, 1.0);
        assertTrue(after > before, "knowing 1,000 endpoints must raise the decompile estimate");
    }

    // ── Weighted percentage + ETA ────────────────────────────────────────────

    @Test
    @DisplayName("the percentage is weighted by calibrated share, not step count")
    void weightedPercentageTracksCalibratedShares(@TempDir Path dir) {
        ProgressEstimator est = estimator(dir);
        QueueJob job = jarJob(500_000L);
        est.prepare(job);

        // Complete a low-share stage (db-calls, 0.02) and a high-share one (decompile, 0.24) and
        // confirm the high-share one moves the bar further.
        QueueJob a = jarJob(500_000L);
        est.prepare(a);
        a.completeStage("db-calls");
        QueueJob b = jarJob(500_000L);
        est.prepare(b);
        b.completeStage("decompile");

        assertTrue(b.progressPercent > a.progressPercent,
                "the 24% stage must move the bar more than the 2% stage");
        assertTrue(a.progressPercent > 0, "even a small completed stage advances the bar");
    }

    @Test
    @DisplayName("the ETA shrinks as the work advances and is zero when every stage is done")
    void etaShrinksWithProgress(@TempDir Path dir) {
        ProgressEstimator est = estimator(dir);
        QueueJob job = jarJob(2_000_000L);
        est.prepare(job);
        assertTrue(job.etaMs > 0, "a fresh job has a positive ETA");

        // Once the real class count is known the estimate settles; from there the ETA must shrink
        // as the work advances.
        job.advanceStage("parse-classes", 500, 1000);
        long afterSizeKnown = job.etaMs;
        job.advanceStage("parse-classes", 900, 1000);
        long laterEta = job.etaMs;
        assertTrue(laterEta < afterSizeKnown, "the ETA must shrink as progress advances");

        for (StageModel.Spec spec : StageModel.jar()) job.completeStage(spec.id());
        assertEquals(0L, job.etaMs, "a fully complete job has no remaining time");
        assertEquals(100, job.progressPercent);
        assertTrue(job.allStagesComplete());
    }

    // ── Background stages keep the job incomplete ────────────────────────────

    @Test
    @DisplayName("the job is not complete while a background stage (warm-up/ingest) is still open")
    void backgroundStagesKeepTheJobIncomplete(@TempDir Path dir) {
        ProgressEstimator est = estimator(dir);
        QueueJob job = plsqlJob();
        est.prepare(job);

        for (String id : java.util.List.of("connect-discover", "source-fetch", "parse",
                "call-graph", "save")) {
            job.beginStage(id, 0);
            job.completeStage(id);
        }
        assertFalse(job.allStagesComplete(),
                "sidecar warm-up / estate facets / ingest are still pending");
        assertEquals(3, job.remainingStages());
        assertTrue(job.progressPercent < 100, "the bar must not read 100% before the background work");

        job.markReadyToBrowse();
        assertTrue(job.readyToBrowse(), "artifacts are browsable before the job is fully complete");

        job.beginStage("sidecar-warmup", 14);
        job.advanceStage("sidecar-warmup", 14, 14);
        job.completeStage("sidecar-warmup");
        job.completeStage("estate-facets");
        job.completeStage("entity-ingest");
        assertTrue(job.allStagesComplete());
    }

    // ── Calibration learning ─────────────────────────────────────────────────

    @Test
    @DisplayName("calibration warms up as a simple average then becomes a bounded decayed average")
    void calibrationLearnsAndDecays(@TempDir Path dir) {
        ProgressCalibrationStore store = new ProgressCalibrationStore(dir);

        ProgressCalibrationStore.Snapshot fresh = store.get("plsql");
        assertEquals(0, fresh.runs());
        assertEquals(800.0, fresh.msPerUnit("proc"), 1e-9, "sane default");
        assertEquals(1.0, fresh.shares().values().stream().mapToDouble(Double::doubleValue).sum(), 1e-9);

        // First observation lands exactly (alpha = 1/1).
        store.observe("plsql", Map.of("proc", 1000.0), Map.of());
        assertEquals(1000.0, store.get("plsql").msPerUnit("proc"), 1e-9);

        // Second observation is the mean of the two (alpha = 1/2).
        store.observe("plsql", Map.of("proc", 2000.0), Map.of());
        assertEquals(1500.0, store.get("plsql").msPerUnit("proc"), 1e-9);

        // Third is the mean of three (alpha = 1/3).
        store.observe("plsql", Map.of("proc", 1200.0), Map.of());
        assertEquals(1400.0, store.get("plsql").msPerUnit("proc"), 1e-9);
        assertEquals(3, store.get("plsql").runs());

        // A hostile observation cannot escape the clamp.
        store.observe("plsql", Map.of("proc", 1_000_000_000.0), Map.of());
        assertTrue(store.get("plsql").msPerUnit("proc") <= ProgressCalibrationStore.MAX_MS);
    }

    @Test
    @DisplayName("stage shares are learned and renormalised to sum to one")
    void sharesAreLearnedAndNormalised(@TempDir Path dir) {
        ProgressCalibrationStore store = new ProgressCalibrationStore(dir);
        store.observe("jar", Map.of(),
                Map.of("decompile", 60.0, "parse-classes", 40.0));
        var snap = store.get("jar");
        assertEquals(1.0, snap.shares().values().stream().mapToDouble(Double::doubleValue).sum(), 1e-9);
        // decompile was observed to take 60% of the run; it must now outweigh its default (0.24).
        assertTrue(snap.share("decompile", 0.0) > 0.24, "the observed share must be learned");
    }

    @Test
    @DisplayName("finish() folds a completed run into the store and marks the job calibrated")
    void finishLearnsFromTheRun(@TempDir Path dir) throws Exception {
        ProgressEstimator est = estimator(dir);
        QueueJob job = plsqlJob();
        est.prepare(job);

        // Give a sized stage a real (>= 20 ms) duration so the run is measurable.
        job.beginStage("parse", 100);
        Thread.sleep(30);
        job.completeStage("parse");

        est.finish(job);

        assertTrue(job.calibrated, "the run must be folded into the calibration");
        assertEquals(100, job.progressPercent);
        assertEquals(0L, job.etaMs);
        assertTrue(est instanceof ProgressEstimator); // sanity: no exception escaped finish()
        ProgressCalibrationStore store = new ProgressCalibrationStore(dir);
        assertEquals(1, store.get("plsql").runs(), "the run must have been observed");
    }

    @Test
    @DisplayName("a failed run learns nothing and leaves the running stage failed")
    void failureLearnsNothing(@TempDir Path dir) {
        ProgressEstimator est = estimator(dir);
        QueueJob job = jarJob(1000L);
        est.prepare(job);
        job.beginStage("parse-classes", 0);
        est.fail(job);

        assertEquals(JobStage.State.FAILED, job.stage("parse-classes").state);
        assertFalse(job.calibrated);
        assertEquals(0, new ProgressCalibrationStore(dir).get("jar").runs());
    }
}
