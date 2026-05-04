package com.plsqlanalyzer.web.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * Manages Claude verification sessions — background tasks that run Claude CLI
 * for chunked verification of static analysis results.
 *
 * Tracks session lifecycle: RUNNING → COMPLETE | FAILED | KILLED
 * Supports kill per session or per analysis.
 */
public class ClaudeSessionManager {

    private static final Logger log = LoggerFactory.getLogger(ClaudeSessionManager.class);
    private static final int MAX_WORKERS = 3;

    private final ExecutorService executor = Executors.newFixedThreadPool(MAX_WORKERS);
    private final Map<String, SessionInfo> sessions = new ConcurrentHashMap<>();

    /**
     * Submit a background task (Claude verification session).
     *
     * @param analysisName The analysis this session belongs to
     * @param type         Session type: FULL_SCAN, RESUME, SINGLE_CHUNK
     * @param detail       Human-readable detail (e.g. chunk name)
     * @param task         The actual work to run
     * @return Session ID
     */
    public String submit(String analysisName, String type, String detail, Runnable task) {
        String sessionId = UUID.randomUUID().toString().substring(0, 8);

        SessionInfo info = new SessionInfo();
        info.id = sessionId;
        info.analysisName = analysisName;
        info.type = type;
        info.detail = detail;
        info.status = "RUNNING";
        info.startedAt = LocalDateTime.now();

        Future<?> future = executor.submit(() -> {
            try {
                task.run();
                info.status = "COMPLETE";
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    info.status = "KILLED";
                } else {
                    info.status = "FAILED";
                    info.error = e.getMessage();
                    log.error("Session {} failed: {}", sessionId, e.getMessage(), e);
                }
            } finally {
                info.completedAt = LocalDateTime.now();
            }
        });
        info.future = future;
        info.threadId = -1; // will be set by the task itself if needed

        sessions.put(sessionId, info);
        log.info("Submitted session {} [{}] for analysis '{}': {}", sessionId, type, analysisName, detail);
        return sessionId;
    }

    /**
     * Register a session managed externally (e.g., by the queue system).
     * Caller is responsible for calling completeSession() when done.
     */
    public String registerSession(String analysisName, String detail) {
        String sessionId = UUID.randomUUID().toString().substring(0, 8);
        SessionInfo info = new SessionInfo();
        info.id = sessionId;
        info.analysisName = analysisName;
        info.type = "QUEUE_VERIFY";
        info.detail = detail;
        info.status = "RUNNING";
        info.startedAt = LocalDateTime.now();
        sessions.put(sessionId, info);
        log.info("Session {} registered for analysis '{}': {}", sessionId, analysisName, detail);
        return sessionId;
    }

    public void completeSession(String sessionId, boolean success, String error) {
        SessionInfo info = sessions.get(sessionId);
        if (info == null) return;
        info.status = success ? "COMPLETE" : "FAILED";
        info.error = error;
        info.completedAt = LocalDateTime.now();
    }

    /**
     * Mark a registered session as KILLED (e.g., when the queue cancels it).
     */
    public void killSession(String sessionId) {
        SessionInfo info = sessions.get(sessionId);
        if (info == null) return;
        info.status = "KILLED";
        info.completedAt = LocalDateTime.now();
    }

    /**
     * Kill a specific session by ID.
     */
    public boolean kill(String sessionId) {
        SessionInfo info = sessions.get(sessionId);
        if (info == null) return false;
        if (!"RUNNING".equals(info.status)) return false;

        info.status = "KILLED";
        info.completedAt = LocalDateTime.now();
        if (info.future != null) {
            info.future.cancel(true);
        }
        log.info("Killed session {}", sessionId);
        return true;
    }

    /**
     * Kill all running sessions for a given analysis.
     */
    public int killForAnalysis(String analysisName) {
        int killed = 0;
        for (SessionInfo info : sessions.values()) {
            if (analysisName.equals(info.analysisName) && "RUNNING".equals(info.status)) {
                info.status = "KILLED";
                info.completedAt = LocalDateTime.now();
                if (info.future != null) info.future.cancel(true);
                killed++;
            }
        }
        if (killed > 0) log.info("Killed {} sessions for analysis '{}'", killed, analysisName);
        return killed;
    }

    /**
     * List all sessions, optionally filtered by analysis name.
     */
    public List<Map<String, Object>> listSessions(String analysisName) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (SessionInfo info : sessions.values()) {
            if (analysisName != null && !analysisName.equals(info.analysisName)) continue;
            result.add(info.toMap());
        }
        result.sort((a, b) -> {
            String sa = (String) a.get("startedAt");
            String sb = (String) b.get("startedAt");
            return sb.compareTo(sa); // newest first
        });
        return result;
    }

    /**
     * Get info for a specific session.
     */
    public Map<String, Object> getSession(String sessionId) {
        SessionInfo info = sessions.get(sessionId);
        return info != null ? info.toMap() : null;
    }

    /**
     * Check if any session is currently running for the given analysis.
     */
    public boolean hasRunning(String analysisName) {
        return sessions.values().stream()
                .anyMatch(s -> analysisName.equals(s.analysisName) && "RUNNING".equals(s.status));
    }

    /**
     * Get counts summary.
     */
    public Map<String, Integer> getSummary() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        int running = 0, complete = 0, failed = 0, killed = 0;
        for (SessionInfo s : sessions.values()) {
            switch (s.status) {
                case "RUNNING" -> running++;
                case "COMPLETE" -> complete++;
                case "FAILED" -> failed++;
                case "KILLED" -> killed++;
            }
        }
        counts.put("total", sessions.size());
        counts.put("running", running);
        counts.put("complete", complete);
        counts.put("failed", failed);
        counts.put("killed", killed);
        return counts;
    }

    public void shutdown() {
        executor.shutdownNow();
    }

    // ---- Internal ----

    private static class SessionInfo {
        String id;
        String analysisName;
        String type;
        String detail;
        String status;
        String error;
        LocalDateTime startedAt;
        LocalDateTime completedAt;
        Future<?> future;
        long threadId;

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", id);
            m.put("analysisName", analysisName);
            m.put("type", type);
            m.put("detail", detail);
            m.put("status", status);
            m.put("error", error);
            m.put("startedAt", startedAt != null ? startedAt.toString() : null);
            m.put("completedAt", completedAt != null ? completedAt.toString() : null);
            if (startedAt != null && completedAt != null) {
                long durationMs = java.time.Duration.between(startedAt, completedAt).toMillis();
                m.put("durationMs", durationMs);
                m.put("durationFormatted", formatDuration(durationMs));
            }
            return m;
        }

        static String formatDuration(long ms) {
            if (ms < 1000) return ms + "ms";
            long sec = ms / 1000;
            if (sec < 60) return sec + "s";
            return (sec / 60) + "m " + (sec % 60) + "s";
        }
    }
}
