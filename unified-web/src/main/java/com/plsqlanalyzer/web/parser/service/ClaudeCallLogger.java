package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Persistent per-analysis Claude call log for the PL/SQL parser — the parser
 * analogue of jar-analyzer's ClaudeCallLogger. One JSONL line per Claude CLI
 * call is appended to data/plsql/{analysisName}/claude/_call_log.jsonl.
 * Survives restarts, supports per-analysis and global aggregation. Never throws.
 */
@Component("parserClaudeCallLogger")
public class ClaudeCallLogger {

    private static final Logger log = LoggerFactory.getLogger(ClaudeCallLogger.class);
    private static final String CALL_LOG_FILE = "_call_log.jsonl";
    /** Claude sidecar subtree, {@code {root}/plsql} — see {@link AnalysisPaths#CLAUDE_ROOT}. */
    private static final String CLAUDE_ROOT = AnalysisPaths.CLAUDE_ROOT;

    /** The seq sort for {@link #callLog}; a cursor minted under it fits no other ordering. */
    private static final String SORT_SEQ_ASC = "SEQ_ASC";

    private final ObjectMapper mapper = new ObjectMapper();
    private final ArtifactStore store;

    /** Memoised global aggregate, keyed by the total size of every call log (see watermark()). */
    private long cachedGlobalWatermark = -1;
    private Map<String, Object> cachedGlobalStats;

    /**
     * @param store required, never optional — an optional store would leave a silent local-disk
     *              fallback behind whenever it happened to be absent
     */
    public ClaudeCallLogger(@Qualifier("parserArtifactStore") ArtifactStore store) {
        this.store = store;
    }

    /** Append one Claude call record. Thread-safe, best-effort (errors swallowed). */
    public void logCall(String analysisName, String sessionId, String sessionType,
                        String endpoint, int chunkIndex,
                        long promptBytes, long responseBytes,
                        long durationMs, boolean success, String error) {
        try {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("ts", LocalDateTime.now().toString());
            e.put("sessionId", sessionId);
            e.put("sessionType", sessionType);
            e.put("endpoint", endpoint);
            e.put("chunkIndex", chunkIndex);
            e.put("promptBytes", promptBytes);
            e.put("responseBytes", responseBytes);
            e.put("durationMs", durationMs);
            e.put("success", success);
            e.put("error", error);
            String line = mapper.writeValueAsString(e) + "\n";
            ArtifactKey logFile = logFileKey(analysisName);
            synchronized (this) {
                appendLine(logFile, line);
            }
        } catch (Exception ex) {
            log.debug("Failed to log Claude call for {}: {}", analysisName, ex.getMessage());
        }
    }

    /** Aggregated stats (totals + per-session breakdown) for one analysis. */
    public Map<String, Object> getStats(String analysisName) {
        return aggregate(readAllEntries(analysisName));
    }

    /**
     * A cheap, monotonic identity for the current contents of a call log — the ETag and the
     * cursor watermark in one number.
     *
     * <p>Aggregating an append-only log is a scan, and this one was scanned every 5 s by the Claude
     * Sessions modal in order to display seven totals. PLAN-08 §2.4 answers that with a maintained
     * rollup table; there is no table yet, so the equivalent here is to memoise the aggregate and
     * invalidate it on the one property of an append-only file that cannot go backwards: its
     * length. {@code size()} is a metadata call on both the local and the S3 store, so a poll that
     * finds nothing new costs no reading at all.
     *
     * <p>Deliberately <b>not</b> a hash of the body — computing the body in order to decide not to
     * send it defeats the purpose.
     */
    public long watermark(String analysisName) {
        ArtifactKey key = logFileKey(analysisName);
        return store.exists(key) ? store.size(key) : 0L;
    }

    /** As {@link #watermark(String)}, across every analysis. */
    public long globalWatermark() {
        long sum = 0;
        try (Stream<ArtifactEntry> dirs = store.list(ArtifactKey.of(CLAUDE_ROOT), false)) {
            for (ArtifactKey dir : dirs.filter(ArtifactEntry::directory)
                                       .map(ArtifactEntry::key).toList()) {
                sum += watermark(dir.leaf());
            }
        } catch (Exception e) {
            log.debug("Failed to compute call-log watermark: {}", e.getMessage());
            return -1;   // unknown: never claim a match, so the caller recomputes
        }
        return sum;
    }

    /**
     * A page of raw call records, addressed by line number.
     *
     * <p>Q10 in PLAN-08 §1.1: the log is append-only, so a client that has seen line {@code n} can
     * ask for {@code sinceSeq=n} and receive the two or three records appended since — instead of
     * the whole growing log, which is what the modal fetched every 5 s.
     *
     * @param sinceSeq return records after this 1-based line number; {@code null} means from the start
     * @param limit    maximum records, default 200, capped at {@link PageRequest#MAX_LIMIT}
     */
    public Page<Map<String, Object>> callLog(String analysisName, Long sinceSeq, Integer limit) {
        PageRequest req = PageRequest.sinceSeq(sinceSeq, limit, SORT_SEQ_ASC, 200);
        long from = sinceSeq == null ? 0 : sinceSeq;
        List<Map<String, Object>> out = new ArrayList<>();

        ArtifactKey key = logFileKey(analysisName);
        if (!store.exists(key)) return Page.empty(SORT_SEQ_ASC, req.limit());

        long seq = 0;
        boolean more = false;
        // Streamed, and the skipped prefix is parsed as nothing more than "a line": the JSON of a
        // record the caller already has is never decoded.
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(store.read(key), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) continue;
                seq++;
                if (seq <= from) continue;
                if (out.size() == req.limit()) { more = true; break; }
                Map<String, Object> entry;
                try { entry = mapper.readValue(line, Map.class); }
                catch (Exception ignore) { continue; }
                entry.put("seq", seq);
                out.add(entry);
            }
        } catch (Exception e) {
            log.debug("Failed to read call log for {}: {}", analysisName, e.getMessage());
        }

        return more
                ? Page.more(out, Cursor.ofSeq(SORT_SEQ_ASC, seq - 1), SORT_SEQ_ASC, req.limit(),
                            -1, false)
                : Page.last(out, SORT_SEQ_ASC, req.limit());
    }

    /** All call records for one session within an analysis. */
    public List<Map<String, Object>> getSessionDetail(String analysisName, String sessionId) {
        return readAllEntries(analysisName).stream()
                .filter(e -> sessionId.equals(String.valueOf(e.getOrDefault("sessionId", ""))))
                .collect(Collectors.toList());
    }

    /**
     * Appends one line.
     *
     * <p>{@link ArtifactStore} has no append — object stores cannot append to an existing object —
     * so this rewrites the log as old-bytes-then-new-line. {@code write()} is atomic-on-close onto
     * a temp and only then moved into place, so reading the previous copy while writing the new
     * one is safe, and a concurrent reader never sees a half-written log. The copy is streamed:
     * a long-running enrichment produces a large log and it is never held in memory.
     */
    private void appendLine(ArtifactKey key, String line) throws IOException {
        boolean existed = store.exists(key);
        try (OutputStream out = store.write(key)) {
            if (existed) {
                try (InputStream in = store.read(key)) { in.transferTo(out); }
            }
            out.write(line.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Aggregated stats across every analysis (survives restarts).
     *
     * <p>Memoised against {@link #globalWatermark()}. The Claude Sessions modal polled this every
     * 5 s, and each call re-read and re-aggregated every call log on disk to produce seven totals
     * that had not changed. Now a poll that arrives with nothing appended costs one {@code list}
     * and one {@code size} per analysis, and no reading.
     */
    public synchronized Map<String, Object> getGlobalStats() {
        long mark = globalWatermark();
        if (mark >= 0 && mark == cachedGlobalWatermark && cachedGlobalStats != null) {
            return cachedGlobalStats;
        }
        Map<String, Object> stats = computeGlobalStats();
        if (mark >= 0) {
            cachedGlobalWatermark = mark;
            cachedGlobalStats = stats;
        }
        return stats;
    }

    private Map<String, Object> computeGlobalStats() {
        List<Map<String, Object>> all = new ArrayList<>();
        try (Stream<ArtifactEntry> dirs = store.list(ArtifactKey.of(CLAUDE_ROOT), false)) {
            for (ArtifactKey dir : dirs.filter(ArtifactEntry::directory)
                                       .map(ArtifactEntry::key).toList()) {
                all.addAll(readAllEntries(dir.leaf()));
            }
        } catch (Exception e) {
            log.debug("Failed to scan call logs: {}", e.getMessage());
        }
        return aggregate(all);
    }

    private Map<String, Object> aggregate(List<Map<String, Object>> entries) {
        Map<String, Object> stats = new LinkedHashMap<>();
        long totalDuration = 0, totalPrompt = 0, totalResponse = 0;
        int errors = 0;
        Map<String, List<Map<String, Object>>> bySession = new LinkedHashMap<>();
        for (Map<String, Object> e : entries) {
            totalDuration += toLong(e.get("durationMs"));
            totalPrompt += toLong(e.get("promptBytes"));
            totalResponse += toLong(e.get("responseBytes"));
            if (!Boolean.TRUE.equals(e.get("success"))) errors++;
            bySession.computeIfAbsent(String.valueOf(e.getOrDefault("sessionId", "unknown")),
                    k -> new ArrayList<>()).add(e);
        }
        int n = entries.size();
        stats.put("totalCalls", n);
        stats.put("totalDurationMs", totalDuration);
        stats.put("avgDurationMs", n > 0 ? totalDuration / n : 0L);
        stats.put("totalPromptBytes", totalPrompt);
        stats.put("totalResponseBytes", totalResponse);
        stats.put("totalPromptMb", round2(totalPrompt / 1048576.0));
        stats.put("totalResponseMb", round2(totalResponse / 1048576.0));
        stats.put("sessionCount", bySession.size());
        stats.put("errors", errors);

        List<Map<String, Object>> sessions = new ArrayList<>();
        for (var g : bySession.entrySet()) {
            List<Map<String, Object>> calls = g.getValue();
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("sessionId", g.getKey());
            s.put("sessionType", calls.get(0).getOrDefault("sessionType", ""));
            s.put("callCount", calls.size());
            s.put("durationMs", calls.stream().mapToLong(c -> toLong(c.get("durationMs"))).sum());
            s.put("promptBytes", calls.stream().mapToLong(c -> toLong(c.get("promptBytes"))).sum());
            s.put("responseBytes", calls.stream().mapToLong(c -> toLong(c.get("responseBytes"))).sum());
            s.put("errorCount", calls.stream().filter(c -> !Boolean.TRUE.equals(c.get("success"))).count());
            s.put("startedAt", calls.get(0).getOrDefault("ts", ""));
            s.put("endedAt", calls.get(calls.size() - 1).getOrDefault("ts", ""));
            sessions.add(s);
        }
        stats.put("sessions", sessions);
        return stats;
    }

    /**
     * {@code plsql/<analysisName>/claude/_call_log.jsonl}.
     *
     * <p>The {@code plsql} segment is named explicitly rather than rebuilt from the
     * {@code plsql.data-dir} property: keys are relative to the DATA ROOT, and
     * {@code Path.of(dataBaseDir)} would have silently pinned this to the local filesystem.
     */
    private static ArtifactKey logFileKey(String analysisName) {
        return ArtifactKey.of(CLAUDE_ROOT, analysisName, "claude", CALL_LOG_FILE);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readAllEntries(String analysisName) {
        ArtifactKey logFile = logFileKey(analysisName);
        if (!store.exists(logFile)) return List.of();
        List<Map<String, Object>> entries = new ArrayList<>();
        // Line-by-line off the stream: the log is never materialised as one String.
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(store.read(logFile), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                try { entries.add(mapper.readValue(line, Map.class)); }
                catch (Exception ignore) { /* skip malformed line */ }
            }
        } catch (Exception e) {
            log.debug("Failed to read call log for {}: {}", analysisName, e.getMessage());
        }
        return entries;
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        try { return Long.parseLong(String.valueOf(v)); } catch (Exception e) { return 0L; }
    }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
}
