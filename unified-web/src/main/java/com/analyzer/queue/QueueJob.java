package com.analyzer.queue;

import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;

import java.time.Instant;
import java.util.*;

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
}
