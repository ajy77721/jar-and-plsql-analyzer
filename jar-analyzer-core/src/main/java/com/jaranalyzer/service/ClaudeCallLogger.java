package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Tracks every Claude CLI subprocess call per JAR analysis.
 * Writes one JSONL line per call to data/jar/{normalizedKey}/claude/_call_log.jsonl.
 * Thread-safe, append-only, non-blocking (failures never propagate to callers).
 */
@Component
public class ClaudeCallLogger {

    private static final Logger log = LoggerFactory.getLogger(ClaudeCallLogger.class);
    private static final String CALL_LOG_FILE = "_call_log.jsonl";

    private final JarDataPaths jarDataPaths;
    private final ObjectMapper objectMapper;

    public ClaudeCallLogger(JarDataPaths jarDataPaths, ObjectMapper objectMapper) {
        this.jarDataPaths = jarDataPaths;
        this.objectMapper = objectMapper;
    }

    /**
     * Log a single Claude CLI call. Appends one JSONL line to the call log file.
     * Thread-safe via synchronized block. Never throws — all errors are swallowed.
     */
    public void logCall(String jarName, String sessionId, String sessionType,
                        String endpoint, int chunkIndex,
                        int promptSizeKb, int responseSizeKb,
                        long durationMs, boolean success, String error) {
        try {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("ts", LocalDateTime.now().toString());
            entry.put("sessionId", sessionId);
            entry.put("sessionType", sessionType);
            entry.put("endpoint", endpoint);
            entry.put("chunkIndex", chunkIndex);
            entry.put("promptSizeKb", promptSizeKb);
            entry.put("responseSizeKb", responseSizeKb);
            entry.put("durationMs", durationMs);
            entry.put("success", success);
            entry.put("error", error);

            String line = objectMapper.writeValueAsString(entry) + "\n";
            Path logFile = getLogFilePath(jarName);

            synchronized (this) {
                Files.createDirectories(logFile.getParent());
                try (FileWriter fw = new FileWriter(logFile.toFile(), StandardCharsets.UTF_8, true)) {
                    fw.write(line);
                }
            }
        } catch (Exception e) {
            log.debug("Failed to log Claude call for {}: {}", jarName, e.getMessage());
        }
    }

    /**
     * Get aggregated stats for all Claude calls for a JAR.
     * Returns totals, averages, session breakdown, and error count.
     */
    public Map<String, Object> getStats(String jarName) {
        Map<String, Object> stats = new LinkedHashMap<>();
        List<Map<String, Object>> entries = readAllEntries(jarName);

        if (entries.isEmpty()) {
            stats.put("totalCalls", 0);
            stats.put("totalDurationMs", 0L);
            stats.put("avgDurationMs", 0L);
            stats.put("totalPromptSizeKb", 0);
            stats.put("totalResponseSizeKb", 0);
            stats.put("sessionCount", 0);
            stats.put("sessions", List.of());
            stats.put("errors", 0);
            return stats;
        }

        long totalDuration = 0;
        int totalPromptKb = 0;
        int totalResponseKb = 0;
        int errorCount = 0;
        Map<String, List<Map<String, Object>>> sessionGroups = new LinkedHashMap<>();

        for (Map<String, Object> entry : entries) {
            totalDuration += toLong(entry.get("durationMs"));
            totalPromptKb += toInt(entry.get("promptSizeKb"));
            totalResponseKb += toInt(entry.get("responseSizeKb"));
            if (!Boolean.TRUE.equals(entry.get("success"))) {
                errorCount++;
            }
            String sid = String.valueOf(entry.getOrDefault("sessionId", "unknown"));
            sessionGroups.computeIfAbsent(sid, k -> new ArrayList<>()).add(entry);
        }

        stats.put("totalCalls", entries.size());
        stats.put("totalDurationMs", totalDuration);
        stats.put("avgDurationMs", entries.isEmpty() ? 0L : totalDuration / entries.size());
        stats.put("totalPromptSizeKb", totalPromptKb);
        stats.put("totalResponseSizeKb", totalResponseKb);
        stats.put("sessionCount", sessionGroups.size());
        stats.put("errors", errorCount);

        List<Map<String, Object>> sessions = new ArrayList<>();
        for (Map.Entry<String, List<Map<String, Object>>> group : sessionGroups.entrySet()) {
            List<Map<String, Object>> calls = group.getValue();
            Map<String, Object> session = new LinkedHashMap<>();
            session.put("sessionId", group.getKey());
            session.put("sessionType", calls.get(0).getOrDefault("sessionType", ""));
            session.put("callCount", calls.size());

            long sessionDuration = calls.stream().mapToLong(c -> toLong(c.get("durationMs"))).sum();
            session.put("durationMs", sessionDuration);

            long sessionErrors = calls.stream()
                    .filter(c -> !Boolean.TRUE.equals(c.get("success")))
                    .count();
            session.put("errorCount", sessionErrors);

            String startedAt = String.valueOf(calls.get(0).getOrDefault("ts", ""));
            String endedAt = String.valueOf(calls.get(calls.size() - 1).getOrDefault("ts", ""));
            session.put("startedAt", startedAt);
            session.put("endedAt", endedAt);

            sessions.add(session);
        }
        stats.put("sessions", sessions);

        return stats;
    }

    /**
     * Get all call entries for a specific session within a JAR.
     */
    public List<Map<String, Object>> getSessionDetail(String jarName, String sessionId) {
        List<Map<String, Object>> entries = readAllEntries(jarName);
        return entries.stream()
                .filter(e -> sessionId.equals(String.valueOf(e.getOrDefault("sessionId", ""))))
                .collect(Collectors.toList());
    }

    private Path getLogFilePath(String jarName) {
        return jarDataPaths.claudeDir(jarName).resolve(CALL_LOG_FILE);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readAllEntries(String jarName) {
        Path logFile = getLogFilePath(jarName);
        if (!Files.isRegularFile(logFile)) return List.of();

        List<Map<String, Object>> entries = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                try {
                    Map<String, Object> entry = objectMapper.readValue(line, Map.class);
                    entries.add(entry);
                } catch (Exception e) {
                    log.debug("Skipping malformed JSONL line: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.debug("Failed to read call log for {}: {}", jarName, e.getMessage());
        }
        return entries;
    }

    private long toLong(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        try { return Long.parseLong(String.valueOf(value)); } catch (Exception e) { return 0L; }
    }

    private int toInt(Object value) {
        if (value instanceof Number) return ((Number) value).intValue();
        try { return Integer.parseInt(String.valueOf(value)); } catch (Exception e) { return 0; }
    }
}
