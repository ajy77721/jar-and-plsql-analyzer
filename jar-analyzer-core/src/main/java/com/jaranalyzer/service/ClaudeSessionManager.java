package com.jaranalyzer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * Manages all Claude enrichment sessions across the system.
 * Tracks running/completed/failed sessions with kill support.
 * Each session wraps a background Claude enrichment task.
 */
@Service("jarClaudeSessionManager")
public class ClaudeSessionManager {

    private static final Logger log = LoggerFactory.getLogger(ClaudeSessionManager.class);

    public enum SessionType { SINGLE_ENDPOINT, RESCAN, FRESH_SCAN, UPLOAD, FULL_SCAN }
    public enum SessionStatus { RUNNING, COMPLETE, FAILED, KILLED }

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private static final java.util.concurrent.atomic.AtomicInteger THREAD_SEQ = new java.util.concurrent.atomic.AtomicInteger(0);
    private final ExecutorService executor = Executors.newFixedThreadPool(3, r -> {
        Thread t = new Thread(r, "claude-scan-" + THREAD_SEQ.incrementAndGet());
        t.setDaemon(true);
        return t;
    });

    private final ClaudeProcessRunner processRunner;

    public ClaudeSessionManager(ClaudeProcessRunner processRunner) {
        this.processRunner = processRunner;
    }

    /**
     * Submit a new Claude session. Returns the session ID.
     * The jarName is normalized so kill/lookup works regardless of name variant.
     */
    public String submit(String jarName, SessionType type, String detail, Runnable task) {
        String sessionId = UUID.randomUUID().toString().substring(0, 8);

        Session session = new Session();
        session.id = sessionId;
        session.jarName = JarNameUtil.normalizeKey(jarName);
        session.type = type;
        session.detail = detail;
        session.status = SessionStatus.RUNNING;
        session.startedAt = LocalDateTime.now().toString();

        Future<?> future = executor.submit(() -> {
            session.thread = Thread.currentThread();
            try {
                task.run();
                session.status = SessionStatus.COMPLETE;
            } catch (Exception e) {
                if (session.status == SessionStatus.KILLED) {
                    log.info("Session {} was killed", sessionId);
                } else {
                    session.status = SessionStatus.FAILED;
                    session.error = e.getMessage();
                    log.warn("Session {} failed: {}", sessionId, e.getMessage());
                }
            } finally {
                session.completedAt = LocalDateTime.now().toString();
                session.thread = null;
            }
        });
        session.future = future;

        sessions.put(sessionId, session);
        log.info("Claude session {} started: {} [{}] {}", sessionId, jarName, type, detail);
        return sessionId;
    }

    /**
     * Register a session that is managed externally (e.g., by the queue system).
     * Returns the session ID. Caller is responsible for calling completeSession() when done.
     */
    public String registerSession(String jarName, SessionType type, String detail) {
        String sessionId = UUID.randomUUID().toString().substring(0, 8);
        Session session = new Session();
        session.id = sessionId;
        session.jarName = JarNameUtil.normalizeKey(jarName);
        session.type = type;
        session.detail = detail;
        session.status = SessionStatus.RUNNING;
        session.startedAt = LocalDateTime.now().toString();
        sessions.put(sessionId, session);
        log.info("Claude session {} registered: {} [{}] {}", sessionId, jarName, type, detail);
        return sessionId;
    }

    public void completeSession(String sessionId, boolean success, String error) {
        Session session = sessions.get(sessionId);
        if (session == null) return;
        session.status = success ? SessionStatus.COMPLETE : SessionStatus.FAILED;
        session.error = error;
        session.completedAt = LocalDateTime.now().toString();
    }

    /**
     * Mark a registered session as KILLED (e.g., when the queue cancels it).
     */
    public void killSession(String sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) return;
        session.status = SessionStatus.KILLED;
        session.completedAt = LocalDateTime.now().toString();
    }

    /**
     * Kill a running session by ID. Interrupts the thread and destroys any active Claude process.
     */
    public boolean kill(String sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) return false;
        if (session.status != SessionStatus.RUNNING) return false;

        log.info("Killing Claude session {}: {} [{}]", sessionId, session.jarName, session.type);
        session.status = SessionStatus.KILLED;

        // Destroy the Claude CLI process running on this session's thread
        Thread t = session.thread;
        processRunner.destroyProcessForThread(t);

        // Interrupt the thread
        if (t != null) t.interrupt();

        // Cancel the future
        if (session.future != null) session.future.cancel(true);

        session.completedAt = LocalDateTime.now().toString();
        return true;
    }

    /**
     * List all sessions (newest first).
     */
    public List<Map<String, Object>> listSessions() {
        List<Session> sorted = new ArrayList<>(sessions.values());
        sorted.sort((a, b) -> b.startedAt.compareTo(a.startedAt));

        List<Map<String, Object>> result = new ArrayList<>();
        for (Session s : sorted) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("id", s.id);
            map.put("jarName", s.jarName);
            map.put("type", s.type.name());
            map.put("detail", s.detail);
            map.put("status", s.status.name());
            map.put("startedAt", s.startedAt);
            map.put("completedAt", s.completedAt);
            if (s.startedAt != null && s.completedAt != null) {
                long durationMs = Duration.between(LocalDateTime.parse(s.startedAt), LocalDateTime.parse(s.completedAt)).toMillis();
                map.put("durationMs", durationMs);
                map.put("durationFormatted", formatDuration(durationMs));
            }
            if (s.error != null) map.put("error", s.error);
            result.add(map);
        }
        return result;
    }

    /**
     * Get a single session by ID.
     */
    public Map<String, Object> getSession(String sessionId) {
        Session s = sessions.get(sessionId);
        if (s == null) return null;
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", s.id);
        map.put("jarName", s.jarName);
        map.put("type", s.type.name());
        map.put("detail", s.detail);
        map.put("status", s.status.name());
        map.put("startedAt", s.startedAt);
        map.put("completedAt", s.completedAt);
        if (s.startedAt != null && s.completedAt != null) {
            long durationMs = Duration.between(LocalDateTime.parse(s.startedAt), LocalDateTime.parse(s.completedAt)).toMillis();
            map.put("durationMs", durationMs);
            map.put("durationFormatted", formatDuration(durationMs));
        }
        if (s.error != null) map.put("error", s.error);
        return map;
    }

    /**
     * Check if any session is currently running for a given JAR.
     */
    public boolean isRunningForJar(String jarName) {
        return sessions.values().stream()
                .anyMatch(s -> matchesJar(jarName, s.jarName) && s.status == SessionStatus.RUNNING);
    }

    /**
     * Kill all RUNNING sessions for a JAR (but keep session records).
     * Used when the user wants to cancel a running scan and start a new one.
     */
    public int killRunningForJar(String jarName) {
        int killed = 0;
        for (Map.Entry<String, Session> entry : sessions.entrySet()) {
            Session s = entry.getValue();
            if (matchesJar(jarName, s.jarName) && s.status == SessionStatus.RUNNING) {
                if (kill(entry.getKey())) killed++;
            }
        }
        return killed;
    }

    /**
     * Kill all running sessions for a JAR, then remove all session records for it.
     * Called when a JAR is deleted so background work stops before files are removed.
     * Returns the number of sessions killed.
     */
    public int killSessionsForJar(String jarName) {
        int killed = 0;
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, Session> entry : sessions.entrySet()) {
            Session s = entry.getValue();
            if (!matchesJar(jarName, s.jarName)) continue;
            if (s.status == SessionStatus.RUNNING) {
                log.info("Killing Claude session {} for deleted JAR {}", s.id, jarName);
                s.status = SessionStatus.KILLED;
                processRunner.destroyProcessForThread(s.thread);
                Thread t = s.thread;
                if (t != null) t.interrupt();
                if (s.future != null) s.future.cancel(true);
                s.completedAt = LocalDateTime.now().toString();
                killed++;
            }
            toRemove.add(entry.getKey());
        }
        for (String id : toRemove) sessions.remove(id);
        if (!toRemove.isEmpty()) {
            log.info("Removed {} session records for deleted JAR {} ({} killed)", toRemove.size(), jarName, killed);
        }
        return killed;
    }

    /**
     * Compare JAR names using normalized keys.
     * Both session jarName and query are normalized, so this is a simple equals.
     */
    private boolean matchesJar(String query, String sessionJarName) {
        if (query == null || sessionJarName == null) return false;
        return JarNameUtil.normalizeKey(query).equals(JarNameUtil.normalizeKey(sessionJarName));
    }

    private static String formatDuration(long ms) {
        if (ms < 1000) return ms + "ms";
        long sec = ms / 1000;
        if (sec < 60) return sec + "s";
        return (sec / 60) + "m " + (sec % 60) + "s";
    }

    private static class Session {
        String id;
        String jarName;
        SessionType type;
        String detail;
        volatile SessionStatus status;
        String startedAt;
        volatile String completedAt;
        String error;
        volatile Thread thread;
        Future<?> future;
    }
}
