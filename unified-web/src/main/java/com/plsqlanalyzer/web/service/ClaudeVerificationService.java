package com.plsqlanalyzer.web.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;

import com.analyzer.storage.ArtifactStore;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Orchestrates Claude CLI verification of static analysis results.
 *
 * Flow:
 *  1. Takes the static analysis result (tables, operations, procedures, source)
 *  2. Splits into manageable chunks (by procedure groups)
 *  3. For each chunk: builds a prompt, calls Claude via ClaudeProcessRunner, parses response
 *  4. Compares Claude's findings with static analysis
 *  5. Produces a verification result: CONFIRMED / REMOVED / NEW for each table operation
 *
 * Fragment storage: data/plsql/{analysisName}/claude/
 *   {chunkId}_input.json   — what was sent to Claude
 *   {chunkId}_output.json  — Claude's response
 *   {chunkId}_error.txt    — error if failed
 *   _index.json            — map of chunk IDs to procedures
 *   _result.json           — merged verification result
 *   _checkpoint.json       — resume support
 *
 * This class stays thin: the heavy lifting (chunking, prompting, parsing, merging,
 * reporting, persistence, reading) is delegated to package-private collaborator helpers.
 */
public class ClaudeVerificationService {

    private int parallelChunks = 5;

    private final ObjectMapper mapper;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private final ClaudeVerificationRunner runner;
    private final ClaudeResultLoader resultLoader;
    private final ClaudeChunkReader chunkReader;

    public ClaudeVerificationService(ClaudeProcessRunner processRunner, ArtifactStore store) {
        this(processRunner, store, 5);
    }

    public ClaudeVerificationService(ClaudeProcessRunner processRunner, ArtifactStore store, int parallelChunks) {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.parallelChunks = Math.max(1, parallelChunks);

        ClaudePromptBuilder promptBuilder = new ClaudePromptBuilder();
        ClaudeChunkMerger chunkMerger = new ClaudeChunkMerger();
        ClaudeChunkBuilder chunkBuilder = new ClaudeChunkBuilder(chunkMerger);
        ClaudeResponseParser parser = new ClaudeResponseParser(mapper);
        ClaudeResultMerger merger = new ClaudeResultMerger();
        ClaudeRunReporter reporter = new ClaudeRunReporter(store);
        ClaudeFragmentWriter writer = new ClaudeFragmentWriter(mapper, store);
        this.resultLoader = new ClaudeResultLoader(mapper, store, parser, merger);
        this.chunkReader = new ClaudeChunkReader(mapper, store);
        this.runner = new ClaudeVerificationRunner(processRunner, store, cancelled,
                chunkBuilder, promptBuilder, parser, merger, reporter, resultLoader, writer);
    }

    /**
     * Run full verification of an analysis result.
     *
     * @param result           The static analysis result to verify
     * @param progressCallback Called with progress messages (for SSE streaming)
     * @return Verification result with per-table status
     */
    public VerificationResult verify(AnalysisResult result, Consumer<String> progressCallback) {
        return runner.verify(result, progressCallback, parallelChunks);
    }

    /**
     * Resume a previously interrupted verification.
     */
    public VerificationResult resume(AnalysisResult result, Consumer<String> progressCallback) {
        // Just call verify — it checks checkpoints automatically
        return verify(result, progressCallback);
    }

    /**
     * Load the saved verification result for an analysis.
     */
    public VerificationResult loadResult(String analysisName) {
        return resultLoader.loadResult(analysisName);
    }

    /**
     * Check if verification data exists for an analysis (final result OR partial chunks).
     */
    public boolean hasVerificationData(String analysisName) {
        return resultLoader.hasVerificationData(analysisName);
    }

    /**
     * Load partial results from completed chunks (even if final _result.json doesn't exist yet).
     * This allows showing results while verification is still running or was interrupted.
     */
    public VerificationResult loadPartialResult(String analysisName, AnalysisResult staticResult) {
        return resultLoader.loadPartialResult(analysisName, staticResult);
    }

    /**
     * Get verification progress (how many chunks completed).
     */
    public Map<String, Object> getProgress(String analysisName) {
        return resultLoader.getProgress(analysisName);
    }

    /**
     * Get the fragment (input/output) for a specific chunk.
     */
    public Map<String, Object> getChunkFragment(String analysisName, String chunkId) {
        return chunkReader.getChunkFragment(analysisName, chunkId);
    }

    /**
     * Build a reverse index: table name → list of chunk IDs that contain that table.
     */
    public Map<String, List<String>> getTableChunkMapping(String analysisName) {
        return chunkReader.getTableChunkMapping(analysisName);
    }

    /**
     * List all chunk IDs for an analysis.
     */
    public List<String> listChunks(String analysisName) {
        return chunkReader.listChunks(analysisName);
    }

    public List<Map<String, Object>> listChunkSummaries(String analysisName) {
        return chunkReader.listChunkSummaries(analysisName);
    }

    public void cancel() {
        cancelled.set(true);
    }

    public void setParallelChunks(int chunks) {
        this.parallelChunks = Math.max(1, chunks);
    }

    // ==================== DATA CLASSES ====================

    public static class OperationVerification {
        public String operation;
        public String status;     // CONFIRMED, REMOVED, NEW
        public String procedureName;
        public String procedureId;  // fully qualified: SCHEMA.PACKAGE.PROC
        public String sourceFile;   // SCHEMA.PACKAGE for source lookup
        public int lineNumber;
        public String reason;
    }

    public static class TableVerificationResult {
        public String tableName;
        public String schemaName;
        public String overallStatus;   // CONFIRMED, PARTIAL, EXPANDED, MODIFIED, UNVERIFIED
        public List<String> staticOperations = new ArrayList<>();
        public int staticAccessCount;
        public boolean isExternal;
        public List<OperationVerification> claudeVerifications = new ArrayList<>();
    }

    public static class VerificationResult {
        public String analysisName;
        public LocalDateTime timestamp;
        public List<TableVerificationResult> tables = new ArrayList<>();
        public int confirmedCount;
        public int removedCount;
        public int newCount;
        public int totalChunks;
        public int errorChunks;
        public String error;

        static VerificationResult error(String msg) {
            VerificationResult vr = new VerificationResult();
            vr.error = msg;
            vr.timestamp = LocalDateTime.now();
            return vr;
        }
    }
}
