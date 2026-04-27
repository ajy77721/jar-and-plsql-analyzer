package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks background Claude enrichment progress per JAR.
 * Persists state to data/jar/{normalizedKey}/claude/_tracker.json so it survives server restarts.
 */
@Service
public class ClaudeEnrichmentTracker {

    private static final Logger log = LoggerFactory.getLogger(ClaudeEnrichmentTracker.class);
    private static final String TRACKER_FILE = "_tracker.json";

    public enum Status { IDLE, RUNNING, COMPLETE, FAILED }
    public enum EpStatus { PENDING, PROCESSING, DONE, ERROR }

    private final Map<String, JobState> jobs = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JarDataPaths jarDataPaths;

    public ClaudeEnrichmentTracker(JarDataPaths jarDataPaths) {
        this.jarDataPaths = jarDataPaths;
    }

    public void startTracking(String jarName, List<String> endpointKeys) {
        String key = JarNameUtil.normalizeKey(jarName);
        JobState state = new JobState();
        state.status = Status.RUNNING;
        state.startedAt = LocalDateTime.now().toString();
        state.totalEndpoints = endpointKeys.size();
        for (String epKey : endpointKeys) {
            state.endpoints.put(epKey, EpStatus.PENDING);
        }
        jobs.put(key, state);
        persistState(key, state);
    }

    public void markProcessing(String jarName, String endpointKey) {
        String key = JarNameUtil.normalizeKey(jarName);
        JobState state = jobs.get(key);
        if (state == null) return;
        state.endpoints.put(endpointKey, EpStatus.PROCESSING);
        state.lastUpdatedAt = LocalDateTime.now().toString();
        persistState(key, state);
    }

    public void markEndpointComplete(String jarName, String endpointKey) {
        String key = JarNameUtil.normalizeKey(jarName);
        JobState state = jobs.get(key);
        if (state == null) return;
        state.endpoints.put(endpointKey, EpStatus.DONE);
        state.completedEndpoints++;
        state.lastUpdatedAt = LocalDateTime.now().toString();
        persistState(key, state);
    }

    public void markEndpointError(String jarName, String endpointKey, String error) {
        String key = JarNameUtil.normalizeKey(jarName);
        JobState state = jobs.get(key);
        if (state == null) return;
        state.endpoints.put(endpointKey, EpStatus.ERROR);
        state.completedEndpoints++;
        state.lastUpdatedAt = LocalDateTime.now().toString();
        state.errors.add(endpointKey + ": " + error);
        persistState(key, state);
    }

    public void markComplete(String jarName) {
        String key = JarNameUtil.normalizeKey(jarName);
        JobState state = jobs.get(key);
        if (state == null) return;
        state.status = Status.COMPLETE;
        state.completedAt = LocalDateTime.now().toString();
        state.lastUpdatedAt = state.completedAt;
        persistState(key, state);
    }

    public void markFailed(String jarName, String error) {
        String key = JarNameUtil.normalizeKey(jarName);
        JobState state = jobs.get(key);
        if (state == null) return;
        state.status = Status.FAILED;
        state.completedAt = LocalDateTime.now().toString();
        state.lastUpdatedAt = state.completedAt;
        state.errors.add(error);
        persistState(key, state);
    }

    /**
     * Remove all tracking state for a JAR (in-memory only — disk file deleted by PersistenceService).
     * Called when a JAR is deleted.
     */
    public void removeTracking(String jarName) {
        String key = JarNameUtil.normalizeKey(jarName);
        jobs.remove(key);
        log.info("Removed enrichment tracker for {} (key={})", jarName, key);
    }

    public Map<String, Object> getStatus(String jarName) {
        String key = JarNameUtil.normalizeKey(jarName);
        JobState state = jobs.get(key);
        if (state == null) {
            state = loadState(key);
            if (state == null) {
                return Map.of("status", "IDLE");
            }
            // Disk-loaded RUNNING means the process crashed/was killed — mark as FAILED
            if (state.status == Status.RUNNING) {
                log.info("Stale RUNNING state loaded from disk for {} — marking as FAILED", key);
                state.status = Status.FAILED;
                state.errors.add("Session was interrupted (recovered from disk state)");
                persistState(key, state);
            }
            jobs.put(key, state);
        }
        return stateToMap(state);
    }

    private Map<String, Object> stateToMap(JobState state) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", state.status.name());
        result.put("totalEndpoints", state.totalEndpoints);
        result.put("completedEndpoints", state.completedEndpoints);
        result.put("startedAt", state.startedAt);
        result.put("completedAt", state.completedAt);
        result.put("lastUpdatedAt", state.lastUpdatedAt);

        Map<String, String> epMap = new LinkedHashMap<>();
        state.endpoints.forEach((k, v) -> epMap.put(k, v.name()));
        result.put("endpoints", epMap);

        if (!state.errors.isEmpty()) {
            result.put("errors", new ArrayList<>(state.errors));
        }
        return result;
    }

    /** Persist current tracker state to {normalizedKey}/claude/_tracker.json (atomic write) */
    private void persistState(String normalizedKey, JobState state) {
        try {
            Path dir = jarDataPaths.claudeDir(normalizedKey);
            Files.createDirectories(dir);
            String json = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(stateToMap(state));
            // Write to temp file then atomic move — survives crashes mid-write
            Path target = dir.resolve(TRACKER_FILE);
            Path tmp = dir.resolve(TRACKER_FILE + ".tmp");
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            Files.move(tmp, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("Failed to persist tracker for {}: {}", normalizedKey, e.getMessage());
        }
    }

    /** Load tracker state from disk (called when in-memory map is empty). */
    private JobState loadState(String normalizedKey) {
        Path file = jarDataPaths.claudeDir(normalizedKey).resolve(TRACKER_FILE);
        if (!Files.exists(file)) return null;
        try {
            JsonNode root = objectMapper.readTree(Files.readString(file, StandardCharsets.UTF_8));
            JobState state = new JobState();
            state.status = Status.valueOf(root.path("status").asText("IDLE"));
            state.totalEndpoints = root.path("totalEndpoints").asInt(0);
            state.completedEndpoints = root.path("completedEndpoints").asInt(0);
            state.startedAt = root.path("startedAt").asText(null);
            state.completedAt = root.path("completedAt").asText(null);
            state.lastUpdatedAt = root.path("lastUpdatedAt").asText(null);

            JsonNode eps = root.path("endpoints");
            if (eps.isObject()) {
                eps.fields().forEachRemaining(entry ->
                        state.endpoints.put(entry.getKey(),
                                EpStatus.valueOf(entry.getValue().asText("PENDING"))));
            }

            JsonNode errs = root.path("errors");
            if (errs.isArray()) {
                errs.forEach(e -> state.errors.add(e.asText()));
            }

            log.info("Loaded tracker state for {} from disk ({}/{})",
                    normalizedKey, state.completedEndpoints, state.totalEndpoints);
            return state;
        } catch (Exception e) {
            log.debug("Failed to load tracker for {}: {}", normalizedKey, e.getMessage());
            return null;
        }
    }

    private static class JobState {
        volatile Status status = Status.IDLE;
        volatile int totalEndpoints;
        volatile int completedEndpoints;
        volatile String startedAt;
        volatile String completedAt;
        volatile String lastUpdatedAt;
        final Map<String, EpStatus> endpoints = new ConcurrentHashMap<>();
        final List<String> errors = Collections.synchronizedList(new ArrayList<>());
    }
}
