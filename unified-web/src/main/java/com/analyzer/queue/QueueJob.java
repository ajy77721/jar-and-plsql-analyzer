package com.analyzer.queue;

import java.time.Instant;
import java.util.*;

public class QueueJob {

    public enum Type {
        JAR_UPLOAD, WAR_UPLOAD,
        PLSQL_ANALYSIS, PLSQL_FAST_ANALYSIS, PARSER_ANALYSIS,
        CLAUDE_ENRICH, CLAUDE_ENRICH_SINGLE, CLAUDE_RESCAN,
        CLAUDE_FULL_SCAN, CLAUDE_CORRECT, CLAUDE_CORRECT_SINGLE,
        PLSQL_CLAUDE_VERIFY
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
    public volatile Thread runThread;
    public volatile Map<String, Object> followUpJob;

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
        m.put("metadata", metadata);
        List<String> recent;
        synchronized (log) {
            recent = new ArrayList<>(log.subList(Math.max(0, log.size() - 50), log.size()));
        }
        m.put("log", recent);
        return m;
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
