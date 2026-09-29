package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles all artifact I/O for flow-analysis run data, through {@link ArtifactStore}.
 *
 * Layout:
 *   jar/flowanalysis/{jarId}/{runId}/
 *       status.json   — minimal live state (rewritten on every state change)
 *       log.jsonl     — append-only log, one JSON-encoded string per line
 *       results.json  — full FlowResult[] JSON (written only when done)
 *       report.md     — markdown report (written only when done)
 */
public class FlowRunStore {

    private static final Logger log = LoggerFactory.getLogger(FlowRunStore.class);

    /** Number of tail lines returned for the log array in status responses. */
    static final int LOG_TAIL_LINES = 30;

    private final ArtifactKey baseFlowDir; // jar/flowanalysis/
    private final ArtifactStore store;
    private final ObjectMapper objectMapper;

    /**
     * @param dataRoot the JAR subtree key (what {@code JarDataPaths.keyForPath(getBaseDir())}
     *                 yields). Passed in rather than rebuilt from a directory string: rebuilding
     *                 a root from a String is always the local filesystem, whatever the store is.
     */
    public FlowRunStore(ArtifactKey dataRoot, ArtifactStore store, ObjectMapper objectMapper) {
        this.baseFlowDir  = dataRoot.resolve("flowanalysis");
        this.store        = java.util.Objects.requireNonNull(store, "ArtifactStore is required");
        this.objectMapper = objectMapper;
    }

    // ── Location helpers ──────────────────────────────────────────────────────

    /**
     * The run "directory". Object storage has no directories and the store creates parents
     * implicitly on write, so there is nothing to create — the key is enough.
     */
    public ArtifactKey createRunDir(String jarId, String runId) throws IOException {
        return runDir(jarId, runId);
    }

    public ArtifactKey runDir(String jarId, String runId) {
        return baseFlowDir.resolve(safe(jarId)).resolve(safe(runId));
    }

    public ArtifactKey jarFlowDir(String jarId) {
        return baseFlowDir.resolve(safe(jarId));
    }

    // ── Store plumbing ────────────────────────────────────────────────────────

    private InputStream openRead(ArtifactKey key) throws IOException { return store.read(key); }

    private OutputStream openWrite(ArtifactKey key) throws IOException { return store.write(key); }

    private OutputStream openWrite(ArtifactKey key, long len) throws IOException {
        return store.write(key, len);
    }

    private void writeBytes(ArtifactKey key, byte[] body) throws IOException {
        try (OutputStream out = openWrite(key, body.length)) {
            out.write(body);
        }
    }

    private byte[] readBytes(ArtifactKey key) throws IOException {
        try (InputStream in = openRead(key)) {
            return in.readAllBytes();
        }
    }

    /** Whole-file line read. Only used for log.jsonl, which is bounded by the run's log volume. */
    private List<String> readLines(ArtifactKey key) throws IOException {
        return new ArrayList<>(List.of(new String(readBytes(key), StandardCharsets.UTF_8)
                .split("\n", -1)));
    }

    // ── status.json ───────────────────────────────────────────────────────────

    /**
     * Serialises the minimal fields of a {@link RunMeta} to {@code status.json}.
     * Silently logs errors — never throws; a status write failure must not crash the run.
     */
    public void writeStatus(RunMeta meta) {
        try {
            Map<String, Object> m = buildStatusMap(meta);
            writeBytes(statusFile(meta.runDir), objectMapper.writeValueAsBytes(m));
        } catch (Exception e) {
            log.warn("FlowRunStore: failed to write status.json for runId={}: {}", meta.runId, e.getMessage());
        }
    }

    /** Reads status.json from disk. Returns null when the file doesn't exist or is unreadable. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> readStatus(ArtifactKey runDir) {
        ArtifactKey f = statusFile(runDir);
        if (!store.exists(f)) return null;
        try (InputStream in = openRead(f)) {
            return objectMapper.readValue(in, Map.class);
        } catch (Exception e) {
            log.debug("FlowRunStore: cannot read status.json at {}: {}", f, e.getMessage());
            return null;
        }
    }

    // ── log.jsonl ─────────────────────────────────────────────────────────────

    /**
     * Appends one log entry to {@code log.jsonl}.
     * Each line is a JSON-encoded string (i.e., {@code "\"message\"\n"}).
     */
    public void appendLog(ArtifactKey runDir, String entry) {
        try {
            String line = objectMapper.writeValueAsString(entry) + "\n";
            // ArtifactStore has no append: object storage cannot append to an object, so the log
            // is read-modify-written. Runs emit tens of lines, not thousands, so this stays cheap.
            ArtifactKey f = logFile(runDir);
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            if (store.exists(f)) buf.write(readBytes(f));
            buf.write(line.getBytes(StandardCharsets.UTF_8));
            writeBytes(f, buf.toByteArray());
        } catch (Exception e) {
            log.debug("FlowRunStore: cannot append to log.jsonl: {}", e.getMessage());
        }
    }

    /**
     * Replaces the last occurrence of {@code oldEntry} in {@code log.jsonl}
     * with {@code newEntry}. If not found, appends {@code newEntry}.
     * This mirrors the in-memory {@code replaceLastLog} logic.
     */
    public void replaceLastLog(ArtifactKey runDir, String oldEntry, String newEntry) {
        ArtifactKey f = logFile(runDir);
        if (!store.exists(f)) {
            appendLog(runDir, newEntry);
            return;
        }
        try {
            List<String> rawLines = readLines(f);
            String encodedOld = objectMapper.writeValueAsString(oldEntry);
            int lastIdx = -1;
            for (int i = rawLines.size() - 1; i >= 0; i--) {
                if (rawLines.get(i).trim().equals(encodedOld)) {
                    lastIdx = i;
                    break;
                }
            }
            if (lastIdx >= 0) {
                rawLines.set(lastIdx, objectMapper.writeValueAsString(newEntry));
                writeBytes(f, String.join("\n", rawLines).getBytes(StandardCharsets.UTF_8));
            } else {
                appendLog(runDir, newEntry);
            }
        } catch (Exception e) {
            log.debug("FlowRunStore: replaceLastLog failed: {}", e.getMessage());
            appendLog(runDir, newEntry);
        }
    }

    /**
     * Reads the last {@value #LOG_TAIL_LINES} decoded log strings from {@code log.jsonl}.
     * Returns an empty list when the file doesn't exist or is unreadable.
     */
    public List<String> readLogTail(ArtifactKey runDir) {
        ArtifactKey f = logFile(runDir);
        if (!store.exists(f)) return List.of();
        try {
            List<String> rawLines = readLines(f);
            // Take last N lines
            int from = Math.max(0, rawLines.size() - LOG_TAIL_LINES);
            List<String> tail = rawLines.subList(from, rawLines.size());
            List<String> decoded = new ArrayList<>(tail.size());
            for (String raw : tail) {
                if (raw.isBlank()) continue;
                try {
                    decoded.add(objectMapper.readValue(raw, String.class));
                } catch (Exception ex) {
                    decoded.add(raw); // fallback: return as-is
                }
            }
            return decoded;
        } catch (Exception e) {
            log.debug("FlowRunStore: cannot read log.jsonl at {}: {}", f, e.getMessage());
            return List.of();
        }
    }

    // ── results.json ──────────────────────────────────────────────────────────

    public void writeResults(ArtifactKey runDir, String jsonReport) {
        try {
            writeBytes(resultsFile(runDir), jsonReport.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warn("FlowRunStore: failed to write results.json: {}", e.getMessage());
        }
    }

    /**
     * Reads results.json and parses it as a Java object (List/Map).
     * Returns null when the file doesn't exist or parsing fails.
     */
    public Object readResults(ArtifactKey runDir) {
        ArtifactKey f = resultsFile(runDir);
        if (!store.exists(f)) return null;
        try (InputStream in = openRead(f)) {
            return objectMapper.readValue(in, Object.class);
        } catch (Exception e) {
            log.warn("FlowRunStore: cannot parse results.json at {}: {}", f, e.getMessage());
            return null;
        }
    }

    /** Reads raw results.json text. Returns null when the file doesn't exist. */
    public String readResultsRaw(ArtifactKey runDir) {
        ArtifactKey f = resultsFile(runDir);
        if (!store.exists(f)) return null;
        try {
            return new String(readBytes(f), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("FlowRunStore: cannot read results.json: {}", e.getMessage());
            return null;
        }
    }

    // ── report.md ─────────────────────────────────────────────────────────────

    public void writeReport(ArtifactKey runDir, String markdown) {
        try {
            writeBytes(reportFile(runDir), markdown.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warn("FlowRunStore: failed to write report.md: {}", e.getMessage());
        }
    }

    /** Reads report.md bytes. Returns null when the file doesn't exist. */
    public byte[] readReport(ArtifactKey runDir) {
        ArtifactKey f = reportFile(runDir);
        if (!store.exists(f)) return null;
        try {
            return readBytes(f);
        } catch (IOException e) {
            log.warn("FlowRunStore: cannot read report.md: {}", e.getMessage());
            return null;
        }
    }

    // ── History scan ──────────────────────────────────────────────────────────

    /**
     * Scans {@code data/flowanalysis/{jarId}/} for run directories and returns
     * a list of their status maps. Each entry will have all fields from
     * status.json plus a synthetic {@code "runId"} field derived from the
     * directory name. Directories without a readable status.json are skipped.
     * Results are ordered newest-first (by the {@code "timestamp"} field when
     * present, otherwise by directory-name descending).
     */
    public List<Map<String, Object>> listRunStatuses(String jarId) {
        ArtifactKey dir = jarFlowDir(jarId);
        List<Map<String, Object>> entries = new ArrayList<>();
        try (var stream = store.list(dir, false)) {
            stream.filter(ArtifactEntry::directory)
                  .map(ArtifactEntry::key)
                  .forEach(runDir -> {
                      Map<String, Object> s = readStatus(runDir);
                      if (s != null) {
                          // Ensure runId is present
                          s.computeIfAbsent("runId", k -> runDir.leaf());
                          entries.add(s);
                      }
                  });
        } catch (IOException e) {
            log.debug("FlowRunStore: cannot list run dirs for jar {}: {}", jarId, e.getMessage());
        }
        // Sort newest-first: prefer numeric runId descending
        entries.sort((a, b) -> {
            long ta = toLong(a.get("timestamp"));
            long tb = toLong(b.get("timestamp"));
            if (ta != 0 || tb != 0) return Long.compare(tb, ta);
            String ra = String.valueOf(a.getOrDefault("runId", "0"));
            String rb = String.valueOf(b.getOrDefault("runId", "0"));
            try { return Long.compare(Long.parseLong(rb), Long.parseLong(ra)); }
            catch (NumberFormatException ignored) { return rb.compareTo(ra); }
        });
        return entries;
    }

    // ── Internal file path helpers ────────────────────────────────────────────

    public ArtifactKey statusFile(ArtifactKey runDir)  { return runDir.resolve("status.json"); }
    public ArtifactKey logFile(ArtifactKey runDir)     { return runDir.resolve("log.jsonl"); }
    public ArtifactKey resultsFile(ArtifactKey runDir) { return runDir.resolve("results.json"); }
    public ArtifactKey reportFile(ArtifactKey runDir)  { return runDir.resolve("report.md"); }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static String safe(String s) {
        // Replace characters that are invalid in file/directory names
        return s == null ? "_" : s.replaceAll("[^a-zA-Z0-9._\\-]", "_");
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        if (v instanceof String s) {
            try { return Long.parseLong(s); } catch (NumberFormatException ignored) {}
        }
        return 0L;
    }

    // ── Status map builder (shared by writeStatus and readStatus callers) ──────

    /** Builds a plain map from a {@link RunMeta} suitable for JSON serialisation. */
    public static Map<String, Object> buildStatusMap(RunMeta meta) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("runId",            meta.runId);
        m.put("status",           meta.status);
        m.put("phase",            meta.phase);
        m.put("totalEndpoints",   meta.totalEndpoints);
        m.put("doneEndpoints",    meta.doneEndpoints.get());
        m.put("totalClaudeNodes", meta.totalClaudeNodes.get());
        m.put("doneClaudeNodes",  meta.doneClaudeNodes.get());
        m.put("currentEndpoint",  meta.currentEndpoint);
        m.put("currentClaudeNode",meta.currentClaudeNode);
        m.put("error",            meta.error);
        m.put("timestamp",        meta.timestamp);
        m.put("owner",            meta.owner);
        m.put("mode",             meta.mode);
        m.put("endpoints",        meta.endpoints);
        m.put("highCount",        meta.highCount);
        m.put("medCount",         meta.medCount);
        m.put("lowCount",         meta.lowCount);
        m.put("datasetCount",     meta.datasetCount);
        return m;
    }

    // ── RunMeta — minimal in-memory state per run ─────────────────────────────

    /**
     * Minimal in-memory state — just enough to answer status polls quickly
     * without holding large JSON strings in the heap.
     */
    public static class RunMeta {
        public final String runId;
        public volatile String status           = "running";
        public volatile String phase            = "Starting…";
        public volatile int    totalEndpoints   = 0;
        public final AtomicInteger doneEndpoints   = new AtomicInteger(0);
        public volatile String currentEndpoint  = "";
        public final AtomicInteger totalClaudeNodes = new AtomicInteger(0);
        public final AtomicInteger doneClaudeNodes  = new AtomicInteger(0);
        public volatile String currentClaudeNode = "";
        public volatile String error            = null;
        public final ArtifactKey runDir;
        /** Timestamp (epoch ms) when the run was started. Used for history ordering. */
        public final long timestamp;
        /** Run mode (e.g. "optimize") — stored for history. */
        public volatile String mode;
        /**
         * Routing identity of the session that started this run, or {@code null} when none was
         * resolvable. Persisted in status.json so the ownership check survives eviction from the
         * in-memory map and a restart; {@code null} stays readable for the legacy path.
         */
        public volatile String owner;
        /** Selected endpoint paths — stored for history. */
        public volatile List<String> endpoints;
        /**
         * Set to true after results have been served once to a caller.
         * When true, the run is evicted from the in-memory map on the next
         * status poll so that memory is freed (disk has everything).
         */
        public volatile boolean resultsServed = false;

        // ── History summary counters — populated after run completes ─────────
        public volatile int highCount    = 0;
        public volatile int medCount     = 0;
        public volatile int lowCount     = 0;
        public volatile int datasetCount = 0;

        public RunMeta(String runId, ArtifactKey runDir) {
            this.runId     = runId;
            this.runDir    = runDir;
            this.timestamp = System.currentTimeMillis();
        }

        public void complete() {
            this.phase  = "Complete";
            this.status = "done";
        }

        public void fail(String msg) {
            this.error  = msg;
            this.phase  = "Failed";
            this.status = "error";
        }
    }
}
