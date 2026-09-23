package com.plsqlanalyzer.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.VerificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads persisted verification results (final + partial) and progress from disk.
 * Extracted from ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudeResultLoader {

    private static final Logger log = LoggerFactory.getLogger(ClaudeResultLoader.class);

    private final ObjectMapper mapper;
    private final ArtifactStore store;
    private final ClaudeResponseParser parser;
    private final ClaudeResultMerger merger;

    ClaudeResultLoader(ObjectMapper mapper, ArtifactStore store,
                       ClaudeResponseParser parser, ClaudeResultMerger merger) {
        this.mapper = mapper;
        this.store = java.util.Objects.requireNonNull(store, "ArtifactStore is required");
        this.parser = parser;
        this.merger = merger;
    }

    /** {@code plsql/<name>/claude-legacy/} — must match ClaudeVerificationRunner's writer path
     *  (see that class's own comment: this "legacy" web.service stack used to share the exact
     *  same "claude" subfolder as the unrelated web.parser.service.ClaudeVerificationService,
     *  which could silently clobber that stack's chunk fragments — renamed here to match). */
    private ArtifactKey claudeDir(String analysisName) {
        return AnalysisPaths.claudeFileKey(analysisName, "claude-legacy");
    }

    /** Every artifact in one chunk folder, in a single round trip. */
    private Map<ArtifactKey, ArtifactEntry> stat(ArtifactKey dir) {
        try {
            return store.statPrefix(dir);
        } catch (IOException e) {
            return Map.of();
        }
    }

    VerificationResult loadResult(String analysisName) {
        ArtifactKey resultFile = claudeDir(analysisName).resolve("_result.json");
        if (!store.exists(resultFile)) return null;
        try (InputStream in = store.read(resultFile)) {
            return mapper.readValue(in, VerificationResult.class);
        } catch (IOException e) {
            log.error("Failed to load verification result for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    boolean hasVerificationData(String analysisName) {
        ArtifactKey dir = claudeDir(analysisName);
        // ONE listing instead of an exists() plus a directory scan.
        try (var stream = store.list(dir, false)) {
            return stream.anyMatch(e -> e.key().leaf().equals("_result.json")
                    || (e.key().leaf().endsWith("_output.json") && !e.key().leaf().startsWith("_")));
        } catch (IOException e) {
            log.debug("IO operation failed: {}", e.getMessage());
            return false;
        }
    }

    VerificationResult loadPartialResult(String analysisName, AnalysisResult staticResult) {
        ArtifactKey dir = claudeDir(analysisName);

        // First try the final result
        VerificationResult finalResult = loadResult(analysisName);
        if (finalResult != null) return finalResult;

        // No final result — build from completed chunk outputs
        Set<String> completedChunks = loadCheckpoint(dir);
        if (completedChunks.isEmpty()) return null;

        List<ChunkResult> chunkResults = new ArrayList<>();
        for (String chunkId : completedChunks) {
            ChunkResult cr = parser.loadChunkResult(store, dir, chunkId);
            if (cr != null) chunkResults.add(cr);
        }

        if (chunkResults.isEmpty()) return null;

        // Get total chunk count from index
        int totalChunks = 0;
        ArtifactKey indexFile = dir.resolve("_index.json");
        if (store.exists(indexFile)) {
            try (InputStream in = store.read(indexFile)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> index = mapper.readValue(in, LinkedHashMap.class);
                Object chunks = index.get("chunks");
                if (chunks instanceof List<?> list) totalChunks = list.size();
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }

        // Merge partial results
        VerificationResult partial = merger.mergeResults(staticResult, chunkResults);
        partial.totalChunks = totalChunks > 0 ? totalChunks : chunkResults.size();
        partial.error = "Partial result (" + chunkResults.size() + "/" + partial.totalChunks + " chunks completed)";
        log.info("Built partial verification result: {} chunks completed out of {}", chunkResults.size(), partial.totalChunks);
        return partial;
    }

    Map<String, Object> getProgress(String analysisName) {
        ArtifactKey dir = claudeDir(analysisName);
        Map<String, Object> progress = new LinkedHashMap<>();

        // ONE listing serves every probe below. This method is polled by the UI while a run is
        // in flight, and it used to issue four separate directory scans/stats per poll.
        Map<ArtifactKey, ArtifactEntry> stat = stat(dir);
        ArtifactKey checkpointFile = dir.resolve("_checkpoint.json");
        ArtifactKey indexFile = dir.resolve("_index.json");

        int completed = 0, total = 0, errorChunks = 0;
        if (stat.containsKey(checkpointFile)) {
            completed = loadCheckpoint(dir).size();
        }
        if (stat.containsKey(indexFile)) {
            try (InputStream in = store.read(indexFile)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> index = mapper.readValue(in, LinkedHashMap.class);
                Object chunks = index.get("chunks");
                if (chunks instanceof List<?> list) total = list.size();
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }

        // Count error files for error reporting.
        // A leading '_' marks an INTERNAL sidecar (_index/_checkpoint/_result and their error
        // sidecars), not a chunk — excluded here exactly as hasVerificationData() excludes them.
        errorChunks = (int) stat.keySet().stream()
                .filter(k -> k.leaf().endsWith("_error.txt") && !k.leaf().startsWith("_")).count();

        // Also count output files directly as a fallback for completed count
        // (checkpoint may not be updated yet if writing is delayed)
        if (completed == 0) {
            int outputCount = (int) stat.keySet().stream()
                    .filter(k -> k.leaf().endsWith("_output.json") && !k.leaf().startsWith("_")).count();
            if (outputCount > completed) completed = outputCount;
        }

        progress.put("completedChunks", completed);
        progress.put("totalChunks", total);
        progress.put("errorChunks", errorChunks);
        progress.put("percentComplete", total > 0 ? (int) ((completed * 100.0) / total) : 0);
        progress.put("hasResult", stat.containsKey(dir.resolve("_result.json")));
        progress.put("hasPartialResult", completed > 0);
        progress.put("isComplete", completed >= total && total > 0);
        return progress;
    }

    Set<String> loadCheckpoint(ArtifactKey dir) {
        ArtifactKey cpFile = dir.resolve("_checkpoint.json");
        if (!store.exists(cpFile)) return new HashSet<>();
        try (InputStream in = store.read(cpFile)) {
            @SuppressWarnings("unchecked")
            List<String> completed = mapper.readValue(in, List.class);
            return new HashSet<>(completed);
        } catch (IOException e) {
            return new HashSet<>();
        }
    }
}
