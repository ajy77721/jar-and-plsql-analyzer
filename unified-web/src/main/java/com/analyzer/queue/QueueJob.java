package com.analyzer.queue;

import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class QueueJob {

    public enum Type {
        JAR_UPLOAD, JAR_TARGETED, WAR_UPLOAD,
        PLSQL_ANALYSIS, PLSQL_FAST_ANALYSIS, PARSER_ANALYSIS,
        PARSER_VALIDATION,
        CLAUDE_ENRICH, CLAUDE_ENRICH_SINGLE, CLAUDE_RESCAN,
        CLAUDE_FULL_SCAN, CLAUDE_CORRECT, CLAUDE_CORRECT_SINGLE,
        PLSQL_CLAUDE_VERIFY,
        // UI-043: JAR-side "decompile every not-yet-cached class, then bundle" background job.
        JAR_EXPORT_BUNDLE,
        // §267: one Claude call per batch of procedures, generating purposeSummary for each.
        PLSQL_PURPOSE_SUMMARY
    }
    public enum Status { QUEUED, RUNNING, COMPLETE, FAILED, CANCELLED }

    public final String id;
    public final Type type;
    public final String displayName;
    public final Map<String, Object> metadata;
    public final Instant submittedAt;
    public final List<String> log = Collections.synchronizedList(new ArrayList<>());

    public volatile Status status = Status.QUEUED;
    public volatile String currentStep = "Queued";
    public volatile String lastMessage;
    public volatile int stepNumber;
    public volatile int totalSteps;
    public volatile int progressPercent;
    public volatile Instant startedAt;
    public volatile Instant completedAt;
    public volatile String resultName;
    public volatile String error;
    // 3-phase timing breakdown (parse/discover/save/total), populated by the analysis
    // executors (DependencyCrawler/FlowAnalysisMain for PL/SQL, JarAnalysisExecutor,
    // WarAnalysisExecutor) alongside the "[timing] ..." log line, so the Performance
    // Summary UI can render it for jobs of every analyzer type from one place.
    public volatile Long parseMs;
    public volatile Long discoverMs;
    public volatile Long saveMs;
    public volatile Long totalMs;
    public volatile Thread runThread;
    public volatile Map<String, Object> followUpJob;

    // ── The accurate progress engine ─────────────────────────────────────────
    //
    // Every stage of the job is first-class (see JobStage): the hidden background work — sidecar
    // warm-up, estate facets, entity ingest, decompile — is a stage like any other, so the job only
    // completes when all of them do. progressPercent/etaMs/stepNumber are derived from the stage
    // list by ProgressMath, weighted by calibrated stage shares and scaled by content size.

    /** The job's stages, in model order. Empty for job types the engine does not model. */
    public final List<JobStage> stages = new CopyOnWriteArrayList<>();
    /** Calibration dialect ("plsql"/"jar") once ProgressEstimator has prepared the job. */
    public volatile String progressDialect;
    /** Calibrated ms-per-unit costs applied to this job's stages (see ProgressEstimator). */
    public volatile Map<String, Double> msPerUnit = Map.of();
    /** Size hint for the whole job before any stage count is known (bytes-derived for JAR/WAR). */
    public volatile long baselineEstimatedMs;
    /** Size-scaled estimate of the whole job, recomputed as counts become known. */
    public volatile long estimatedTotalMs;
    /** Remaining estimate in ms (estimate x (1 - progress)). */
    public volatile long etaMs;
    /** The id of the currently running stage, or null. */
    public volatile String currentStage;
    /** True once this job's timing has been folded into the calibration store. */
    public volatile boolean calibrated;
    /**
     * Set when the analysis artifacts are written and browsable, even though background stages
     * (warm-up / ingest) may still be running. Lets the UI offer early access honestly:
     * "ready to browse — polishing in background (N stages left)".
     */
    public volatile Instant readyToBrowseAt;

    /**
     * The submitter's per-user SSO snapshot, captured on the request thread at submit time so the
     * job can run under their stores and role on a queue thread with no request context.
     *
     * <p>Deliberately <b>not</b> part of {@link #metadata}: it holds live credentials and is never
     * serialized or exposed. {@link #toMap()} / {@link #toSummaryMap()} omit it, no code writes a
     * {@code QueueJob} to disk or JSON, and {@code transient} documents the intent. A {@code null}
     * snapshot means "no SSO session" (e.g. {@code app.auth.sso.required=false}) and the job keeps
     * the legacy shared-bean / default-credentials path.
     */
    public transient CapturedStores capturedStores;
    public transient CapturedCredentials capturedCredentials;

    public QueueJob(Type type, String displayName, Map<String, Object> metadata) {
        this.id = UUID.randomUUID().toString().substring(0, 8);
        this.type = type;
        this.displayName = displayName;
        this.metadata = metadata != null ? new LinkedHashMap<>(metadata) : new LinkedHashMap<>();
        this.submittedAt = Instant.now();
    }

    public long elapsedMs() {
        Instant start = startedAt != null ? startedAt : submittedAt;
        Instant end = completedAt != null ? completedAt : Instant.now();
        return end.toEpochMilli() - start.toEpochMilli();
    }

    /**
     * The list-row projection: every field {@link #toMap()} carries except the log tail.
     *
     * <p>{@code toMap()} attaches the last 50 progress messages to every job, so a listing of the
     * queue plus 50 history entries shipped up to 2 550 log lines that no screen reads — the list
     * views render {@code currentStep} and {@code lastMessage} only. The lines are still available
     * for one job at {@code GET /api/queue/{id}}, which is where a caller that wants them asks.
     *
     * <p>Used only for a request that opts into filtering or limits, so the unparameterised
     * response stays byte-identical to what it was.
     */
    public Map<String, Object> toSummaryMap() {
        Map<String, Object> m = toMap();
        m.remove("log");
        return m;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("type", type.name());
        m.put("displayName", displayName);
        m.put("status", status.name());
        m.put("currentStep", currentStep);
        m.put("lastMessage", lastMessage);
        m.put("stepNumber", stepNumber);
        m.put("totalSteps", totalSteps);
        m.put("progressPercent", progressPercent);
        m.put("submittedAt", submittedAt.toString());
        m.put("startedAt", startedAt != null ? startedAt.toString() : null);
        m.put("completedAt", completedAt != null ? completedAt.toString() : null);
        m.put("elapsedMs", elapsedMs());
        m.put("resultName", resultName);
        m.put("error", error);
        m.put("parseMs", parseMs);
        m.put("discoverMs", discoverMs);
        m.put("saveMs", saveMs);
        m.put("totalMs", totalMs);
        // The accurate progress engine: the full stage list with per-stage progress/ETA, plus the
        // job-level estimate and the "browsable before complete" hint.
        m.put("stages", stages.stream().map(JobStage::toMap).toList());
        m.put("currentStage", currentStage);
        m.put("estimatedTotalMs", estimatedTotalMs);
        m.put("etaMs", etaMs);
        m.put("estimatedRemainingMs", etaMs);
        m.put("calibrated", calibrated);
        m.put("readyToBrowse", readyToBrowseAt != null);
        m.put("readyToBrowseAt", readyToBrowseAt != null ? readyToBrowseAt.toString() : null);
        m.put("remainingStages", remainingStages());
        m.put("metadata", metadata);
        List<String> recent;
        synchronized (log) {
            recent = new ArrayList<>(log.subList(Math.max(0, log.size() - 50), log.size()));
        }
        m.put("log", recent);
        return m;
    }

    /** Attaches the 3-phase timing breakdown, in milliseconds, to this job. */
    public void setTiming(long parseMs, long discoverMs, long saveMs, long totalMs) {
        this.parseMs = parseMs;
        this.discoverMs = discoverMs;
        this.saveMs = saveMs;
        this.totalMs = totalMs;
    }

    public void updateProgress(String message) {
        this.lastMessage = message;
        this.log.add(message);
        // With the stage engine active, currentStep/stepNumber/progressPercent are derived from the
        // stage list (ProgressMath) and must not be clobbered by the legacy "[n/m] message" scrape.
        if (!stages.isEmpty()) return;
        this.currentStep = message;
        if (message.startsWith("[")) {
            int slash = message.indexOf('/');
            if (slash > 1) {
                try {
                    this.stepNumber = Integer.parseInt(message.substring(1, slash));
                    int bracket = message.indexOf(']', slash);
                    if (bracket > slash + 1) {
                        this.totalSteps = Integer.parseInt(message.substring(slash + 1, bracket));
                        this.progressPercent = (int) ((stepNumber * 100.0) / totalSteps);
                    }
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    // ── Stage-driven progress ────────────────────────────────────────────────

    /** The stage with this id, or null. */
    public JobStage stage(String id) {
        for (JobStage s : stages) if (s.id.equals(id)) return s;
        return null;
    }

    /** Begins a stage, optionally with a known unit count (0 = unknown). */
    public void beginStage(String id, long total) {
        JobStage s = stage(id);
        if (s == null) return;
        s.start(total);
        ProgressMath.recompute(this);
    }

    /** Records progress within a stage; the count only moves forward. */
    public void advanceStage(String id, long done, long total) {
        JobStage s = stage(id);
        if (s == null) return;
        s.advance(done, total);
        ProgressMath.recompute(this);
    }

    /** Marks a stage complete (idempotent). */
    public void completeStage(String id) {
        JobStage s = stage(id);
        if (s == null) return;
        s.complete();
        ProgressMath.recompute(this);
    }

    /** Marks a stage deliberately skipped (disabled feature) so it counts as done, not missing. */
    public void skipStage(String id) {
        JobStage s = stage(id);
        if (s == null) return;
        s.skip();
        ProgressMath.recompute(this);
    }

    public void failStage(String id) {
        JobStage s = stage(id);
        if (s == null) return;
        s.fail();
        ProgressMath.recompute(this);
    }

    /** Records that the analysis artifacts are written and browsable while background stages run. */
    public void markReadyToBrowse() {
        if (readyToBrowseAt == null) readyToBrowseAt = Instant.now();
    }

    /** True once the artifacts are written and browsable, even if background stages still run. */
    public boolean readyToBrowse() {
        return readyToBrowseAt != null;
    }

    /** Stages not yet in a terminal state. */
    public long remainingStages() {
        long n = 0;
        for (JobStage s : stages) if (!s.terminal()) n++;
        return n;
    }

    /** True when every stage is terminal (the job is genuinely, fully complete). */
    public boolean allStagesComplete() {
        if (stages.isEmpty()) return true;
        for (JobStage s : stages) if (!s.terminal()) return false;
        return true;
    }
}
