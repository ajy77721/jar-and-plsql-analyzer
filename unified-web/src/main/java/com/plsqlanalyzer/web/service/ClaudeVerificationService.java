package com.plsqlanalyzer.web.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
 */
public class ClaudeVerificationService {

    private static final Logger log = LoggerFactory.getLogger(ClaudeVerificationService.class);

    /** Max characters of source code per chunk — each chunk now has at most 500 lines of source */
    private static final int MAX_CHUNK_CHARS = 50_000;

    /** Max total prompt size including context + tables + response format instructions */
    private static final int MAX_PROMPT_CHARS = 80_000;

    /** Max tables per chunk — prevents mega-chunks */
    private static final int MAX_TABLES_PER_CHUNK = 20;

    /** Target content size when merging small chunks together */
    private static final int MERGE_TARGET_CHARS = 40_000;

    private int parallelChunks = 5;

    private final ClaudeProcessRunner processRunner;
    private final ObjectMapper mapper;
    private final Path dataBaseDir;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public ClaudeVerificationService(ClaudeProcessRunner processRunner) {
        this(processRunner, "data/plsql", 5);
    }

    public ClaudeVerificationService(ClaudeProcessRunner processRunner, String dataBaseDir) {
        this(processRunner, dataBaseDir, 5);
    }

    public ClaudeVerificationService(ClaudeProcessRunner processRunner, String dataBaseDir, int parallelChunks) {
        this.processRunner = processRunner;
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.dataBaseDir = Paths.get(dataBaseDir);
        this.parallelChunks = Math.max(1, parallelChunks);
    }

    private Path claudeDir(String analysisName) {
        return dataBaseDir.resolve(analysisName).resolve("claude");
    }

    /**
     * Run full verification of an analysis result.
     *
     * @param result         The static analysis result to verify
     * @param progressCallback Called with progress messages (for SSE streaming)
     * @return Verification result with per-table status
     */
    public VerificationResult verify(AnalysisResult result, Consumer<String> progressCallback) {
        cancelled.set(false);
        String analysisName = result.getName();
        Path outputDir = claudeDir(analysisName);
        try { Files.createDirectories(outputDir); } catch (IOException e) {
            log.error("Cannot create output dir: {}", e.getMessage());
            return VerificationResult.error("Cannot create output directory: " + e.getMessage());
        }

        progress(progressCallback, "[1/5] Preparing verification chunks...");

        // Build chunks from the analysis
        List<VerificationChunk> chunks = buildChunks(result);
        progress(progressCallback, "[1/5] Created " + chunks.size() + " verification chunks");

        // Check for existing checkpoint (resume support)
        Set<String> completedChunks = loadCheckpoint(outputDir);
        int skipped = 0;

        // Save index
        saveIndex(outputDir, chunks);

        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger skippedCount = new AtomicInteger(skipped);
        int total = chunks.size();
        List<ChunkResult> chunkResults = Collections.synchronizedList(new ArrayList<>());

        // Separate already-completed from pending chunks
        List<VerificationChunk> pendingChunks = new ArrayList<>();
        for (VerificationChunk chunk : chunks) {
            if (completedChunks.contains(chunk.id)) {
                ChunkResult existing = loadChunkResult(outputDir, chunk.id);
                if (existing != null) {
                    chunkResults.add(existing);
                    skippedCount.incrementAndGet();
                    processed.incrementAndGet();
                    continue;
                }
            }
            pendingChunks.add(chunk);
        }

        if (skippedCount.get() > 0) {
            progress(progressCallback, "[2/5] Skipped " + skippedCount.get() + " already-completed chunks (resumed)");
        }

        int activeThreads = Math.min(parallelChunks, pendingChunks.size());
        progress(progressCallback, "[2/5] Running Claude verification (" + pendingChunks.size()
                + " pending chunks, " + activeThreads + " parallel)...");

        // Process chunks in parallel using CompletionService (non-blocking result collection)
        if (!pendingChunks.isEmpty()) {
            ExecutorService claudePool = Executors.newFixedThreadPool(Math.max(1, activeThreads));
            CompletionService<ChunkResult> completionService = new ExecutorCompletionService<>(claudePool);
            Set<String> syncCompletedChunks = Collections.synchronizedSet(completedChunks);
            int submitted = 0;

            for (VerificationChunk chunk : pendingChunks) {
                completionService.submit(() -> {
                    if (cancelled.get()) return ChunkResult.error(chunk.id, "Cancelled");

                    int num = processed.incrementAndGet();
                    progress(progressCallback, "[2/5] Chunk " + num + "/" + total + ": " + chunk.name);

                    // Build prompt
                    String prompt = buildPrompt(chunk, result);

                    // Save input — include the full prompt so it can be reviewed
                    Map<String, Object> inputData = new LinkedHashMap<>();
                    inputData.put("chunkId", chunk.id);
                    inputData.put("name", chunk.name);
                    inputData.put("procedures", chunk.procedureIds);
                    inputData.put("tableCount", chunk.tables.size());
                    inputData.put("promptLength", prompt.length());
                    inputData.put("timestamp", LocalDateTime.now().toString());
                    inputData.put("prompt", prompt);
                    saveFragment(outputDir, chunk.id + "_input.json", inputData);

                    // Run Claude with one retry on failure
                    int timeoutSecs = 600 + (chunk.tables.size() * 30);
                    String response = null;
                    for (int attempt = 1; attempt <= 2; attempt++) {
                        if (cancelled.get()) return ChunkResult.error(chunk.id, "Cancelled");
                        String sessionId = UUID.randomUUID().toString();
                        response = processRunner.run(prompt, sessionId, timeoutSecs);
                        if (response != null && !response.isBlank()) break;
                        if (attempt == 1) {
                            log.warn("Chunk {} attempt 1 failed, retrying...", chunk.id);
                        }
                    }

                    ChunkResult cr;
                    if (response == null || response.isBlank()) {
                        saveFragment(outputDir, chunk.id + "_error.txt", "No response from Claude CLI after 2 attempts");
                        log.warn("Chunk {} got no response from Claude CLI after 2 attempts", chunk.id);
                        cr = ChunkResult.error(chunk.id, "No response from Claude");
                    } else {
                        saveFragment(outputDir, chunk.id + "_output.json", response);
                        cr = parseResponse(chunk, response);
                        syncCompletedChunks.add(chunk.id);
                        saveCheckpoint(outputDir, syncCompletedChunks);
                    }
                    return cr;
                });
                submitted++;
            }

            // Collect results AS they complete (non-blocking order)
            for (int i = 0; i < submitted; i++) {
                try {
                    Future<ChunkResult> future = completionService.poll(900, TimeUnit.SECONDS);
                    if (future != null) {
                        ChunkResult cr = future.get();
                        if (cr != null) chunkResults.add(cr);
                        int done = chunkResults.size();
                        if (done % 10 == 0 || done == submitted) {
                            progress(progressCallback, "[2/5] Progress: " + done + "/" + submitted + " chunks completed");
                        }
                    } else {
                        log.warn("Timeout waiting for chunk result (poll returned null)");
                        chunkResults.add(ChunkResult.error("timeout_" + i, "Timeout waiting for result"));
                    }
                } catch (Exception e) {
                    log.warn("Claude chunk task failed: {}", e.getMessage());
                    chunkResults.add(ChunkResult.error("unknown", e.getMessage()));
                }
            }
            claudePool.shutdown();
            try {
                if (!claudePool.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                    claudePool.shutdownNow();
                }
            } catch (InterruptedException ie) {
                claudePool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        progress(progressCallback, "[3/5] Merging verification results...");

        // Merge all chunk results into a single verification result
        VerificationResult verResult = mergeResults(result, chunkResults);

        progress(progressCallback, "[4/5] Saving verification result...");

        // Save final result
        saveFragment(outputDir, "_result.json", verResult);

        int confirmed = verResult.confirmedCount;
        int removed = verResult.removedCount;
        int added = verResult.newCount;
        progress(progressCallback, "[5/5] Verification complete: " + confirmed + " confirmed, "
                + removed + " removed, " + added + " new findings");

        // Auto-generate improvement/run report
        generateRunReport(verResult, chunks.size(), chunkResults);

        return verResult;
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
        Path resultFile = claudeDir(analysisName).resolve("_result.json");
        if (!Files.exists(resultFile)) return null;
        try {
            return mapper.readValue(resultFile.toFile(), VerificationResult.class);
        } catch (IOException e) {
            log.error("Failed to load verification result for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    /**
     * Check if verification data exists for an analysis (final result OR partial chunks).
     */
    public boolean hasVerificationData(String analysisName) {
        Path dir = claudeDir(analysisName);
        if (Files.exists(dir.resolve("_result.json"))) return true;
        // Also check for partial chunk outputs
        if (Files.exists(dir)) {
            try (var stream = Files.list(dir)) {
                return stream.anyMatch(p -> p.getFileName().toString().endsWith("_output.json")
                        && !p.getFileName().toString().startsWith("_"));
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }
        return false;
    }

    /**
     * Load partial results from completed chunks (even if final _result.json doesn't exist yet).
     * This allows showing results while verification is still running or was interrupted.
     */
    public VerificationResult loadPartialResult(String analysisName, AnalysisResult staticResult) {
        Path dir = claudeDir(analysisName);
        if (!Files.exists(dir)) return null;

        // First try the final result
        VerificationResult finalResult = loadResult(analysisName);
        if (finalResult != null) return finalResult;

        // No final result — build from completed chunk outputs
        Set<String> completedChunks = loadCheckpoint(dir);
        if (completedChunks.isEmpty()) return null;

        List<ChunkResult> chunkResults = new ArrayList<>();
        for (String chunkId : completedChunks) {
            ChunkResult cr = loadChunkResult(dir, chunkId);
            if (cr != null) chunkResults.add(cr);
        }

        if (chunkResults.isEmpty()) return null;

        // Get total chunk count from index
        int totalChunks = 0;
        Path indexFile = dir.resolve("_index.json");
        if (Files.exists(indexFile)) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> index = mapper.readValue(indexFile.toFile(), LinkedHashMap.class);
                Object chunks = index.get("chunks");
                if (chunks instanceof List<?> list) totalChunks = list.size();
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }

        // Merge partial results
        VerificationResult partial = mergeResults(staticResult, chunkResults);
        partial.totalChunks = totalChunks > 0 ? totalChunks : chunkResults.size();
        partial.error = "Partial result (" + chunkResults.size() + "/" + partial.totalChunks + " chunks completed)";
        log.info("Built partial verification result: {} chunks completed out of {}", chunkResults.size(), partial.totalChunks);
        return partial;
    }

    /**
     * Get verification progress (how many chunks completed).
     */
    public Map<String, Object> getProgress(String analysisName) {
        Path dir = claudeDir(analysisName);
        Map<String, Object> progress = new LinkedHashMap<>();

        Path checkpointFile = dir.resolve("_checkpoint.json");
        Path indexFile = dir.resolve("_index.json");

        int completed = 0, total = 0, errorChunks = 0;
        if (Files.exists(checkpointFile)) {
            completed = loadCheckpoint(dir).size();
        }
        if (Files.exists(indexFile)) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> index = mapper.readValue(indexFile.toFile(), LinkedHashMap.class);
                Object chunks = index.get("chunks");
                if (chunks instanceof List<?> list) total = list.size();
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }

        // Count error files for error reporting
        if (Files.exists(dir)) {
            try (var stream = Files.list(dir)) {
                errorChunks = (int) stream
                        .filter(p -> p.getFileName().toString().endsWith("_error.txt"))
                        .count();
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }

        // Also count output files directly as a fallback for completed count
        // (checkpoint may not be updated yet if writing is delayed)
        if (completed == 0 && Files.exists(dir)) {
            try (var stream = Files.list(dir)) {
                int outputCount = (int) stream
                        .filter(p -> p.getFileName().toString().endsWith("_output.json"))
                        .count();
                if (outputCount > completed) completed = outputCount;
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }

        progress.put("completedChunks", completed);
        progress.put("totalChunks", total);
        progress.put("errorChunks", errorChunks);
        progress.put("percentComplete", total > 0 ? (int) ((completed * 100.0) / total) : 0);
        progress.put("hasResult", Files.exists(dir.resolve("_result.json")));
        progress.put("hasPartialResult", completed > 0);
        progress.put("isComplete", completed >= total && total > 0);
        return progress;
    }

    /**
     * Get the fragment (input/output) for a specific chunk.
     */
    public Map<String, Object> getChunkFragment(String analysisName, String chunkId) {
        Path dir = claudeDir(analysisName);
        Map<String, Object> fragment = new LinkedHashMap<>();
        fragment.put("chunkId", chunkId);

        // Load input
        Path inputFile = dir.resolve(chunkId + "_input.json");
        if (Files.exists(inputFile)) {
            try {
                fragment.put("input", mapper.readValue(inputFile.toFile(), Object.class));
            } catch (IOException e) {
                fragment.put("input", "Error reading: " + e.getMessage());
            }
        }

        // Load output
        Path outputFile = dir.resolve(chunkId + "_output.json");
        if (Files.exists(outputFile)) {
            try {
                String raw = Files.readString(outputFile);
                // Try to parse as JSON first
                try {
                    fragment.put("output", mapper.readValue(raw, Object.class));
                } catch (Exception e) {
                    fragment.put("output", raw); // raw text
                }
            } catch (IOException e) {
                fragment.put("output", "Error reading: " + e.getMessage());
            }
        }

        // Load error if exists
        Path errorFile = dir.resolve(chunkId + "_error.txt");
        if (Files.exists(errorFile)) {
            try {
                fragment.put("error", Files.readString(errorFile));
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }

        return fragment;
    }

    /**
     * Build a reverse index: table name → list of chunk IDs that contain that table.
     * Scans chunk output files for table names.
     */
    public Map<String, List<String>> getTableChunkMapping(String analysisName) {
        Path dir = claudeDir(analysisName);
        Map<String, List<String>> mapping = new LinkedHashMap<>();

        List<String> chunkIds = listChunks(analysisName);
        for (String chunkId : chunkIds) {
            Path outputFile = dir.resolve(chunkId + "_output.json");
            if (!Files.exists(outputFile)) continue;
            try {
                String raw = Files.readString(outputFile);
                // Try to parse as JSON to extract table names
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> output = mapper.readValue(raw, LinkedHashMap.class);
                    Object tablesObj = output.get("tables");
                    if (tablesObj instanceof List<?> tablesList) {
                        for (Object item : tablesList) {
                            if (item instanceof Map<?, ?> tableMap) {
                                Object nameObj = tableMap.get("tableName");
                                if (nameObj != null) {
                                    String name = nameObj.toString().toUpperCase();
                                    mapping.computeIfAbsent(name, k -> new ArrayList<>()).add(chunkId);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // Not JSON — scan raw text for table names (best effort)
                    // Look for patterns like "tableName": "XXX" in the raw text
                    java.util.regex.Pattern pat = java.util.regex.Pattern.compile("\"tableName\"\\s*:\\s*\"([^\"]+)\"");
                    java.util.regex.Matcher m = pat.matcher(raw);
                    while (m.find()) {
                        String name = m.group(1).toUpperCase();
                        mapping.computeIfAbsent(name, k -> new ArrayList<>()).add(chunkId);
                    }
                }
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }
        return mapping;
    }

    /**
     * List all chunk IDs for an analysis.
     */
    public List<String> listChunks(String analysisName) {
        Path dir = claudeDir(analysisName);
        Path indexFile = dir.resolve("_index.json");
        if (!Files.exists(indexFile)) return Collections.emptyList();
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> index = mapper.readValue(indexFile.toFile(), LinkedHashMap.class);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) index.get("chunks");
            if (chunks == null) return Collections.emptyList();
            return chunks.stream().map(c -> (String) c.get("id")).collect(Collectors.toList());
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> listChunkSummaries(String analysisName) {
        Path dir = claudeDir(analysisName);
        Path indexFile = dir.resolve("_index.json");
        if (!Files.exists(indexFile)) return Collections.emptyList();
        try {
            Map<String, Object> index = mapper.readValue(indexFile.toFile(), LinkedHashMap.class);
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) index.get("chunks");
            if (chunks == null) return Collections.emptyList();

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map<String, Object> chunk : chunks) {
                String id = (String) chunk.get("id");
                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("id", id);
                summary.put("name", chunk.get("name"));
                summary.put("procedures", chunk.get("procedures"));
                summary.put("tableCount", chunk.get("tableCount"));

                Path outputFile = dir.resolve(id + "_output.json");
                Path errorFile = dir.resolve(id + "_error.txt");
                if (Files.exists(outputFile)) {
                    summary.put("status", "COMPLETE");
                } else if (Files.exists(errorFile)) {
                    summary.put("status", "ERROR");
                    try { summary.put("error", Files.readString(errorFile).trim()); } catch (IOException ignored) {}
                } else {
                    summary.put("status", "PENDING");
                }
                result.add(summary);
            }
            return result;
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    public void cancel() {
        cancelled.set(true);
    }

    public void setParallelChunks(int chunks) {
        this.parallelChunks = Math.max(1, chunks);
    }

    // ==================== CHUNKING ====================

    /**
     * Max lines of source code per chunk sent to Claude.
     * Keeps prompts focused and within Claude's effective processing range.
     * Large procs are split into overlapping source windows.
     */
    private static final int MAX_SOURCE_LINES_PER_CHUNK = 500;

    /** Overlap between source windows so cross-boundary SQL isn't missed */
    private static final int SOURCE_WINDOW_OVERLAP = 100;

    /**
     * Build verification chunks.
     *
     * Strategy:
     * - Small procs (<500 lines): 1 chunk = 1 proc, sub-split by table count if >10 tables
     * - Large procs (>500 lines): split into overlapping source-window sub-chunks.
     *   Each window gets only the tables with operations in that line range.
     *   This ensures Claude sees the full source of even 10K+ line procs.
     *
     * Example for a 2000-line proc:
     *   chunk_1: PROC_LARGE [L100-600]  → 3 tables (ops in lines 100-600)
     *   chunk_2: PROC_LARGE [L500-1000] → 5 tables (ops in lines 500-1000)
     *   chunk_3: PROC_LARGE [L900-1400] → 2 tables (ops in lines 900-1400)
     *   chunk_4: PROC_LARGE [L1300-1800] → 4 tables (ops in lines 1300-1800)
     *   chunk_5: PROC_LARGE [L1700-2100] → 1 table  (ops in lines 1700-2100)
     *
     * Example for a small proc:
     *   chunk_6: PROC_SMALL → 3 tables (all source included)
     */
    private List<VerificationChunk> buildChunks(AnalysisResult result) {
        List<VerificationChunk> chunks = new ArrayList<>();

        Map<String, TableOperationSummary> allTables = result.getTableOperations();
        if (allTables == null || allTables.isEmpty()) return chunks;

        // Build: procId → list of {tableName, accessDetail} only for that proc
        Map<String, Map<String, List<TableOperationSummary.TableAccessDetail>>> procAccess = new LinkedHashMap<>();
        for (var entry : allTables.entrySet()) {
            String tableName = entry.getKey();
            TableOperationSummary summary = entry.getValue();
            for (var detail : summary.getAccessDetails()) {
                String procId = detail.getProcedureId();
                if (procId != null) {
                    procAccess
                        .computeIfAbsent(procId, k -> new LinkedHashMap<>())
                        .computeIfAbsent(tableName, k -> new ArrayList<>())
                        .add(detail);
                }
            }
        }

        int chunkNum = 0;
        int largeProcCount = 0;
        for (var entry : procAccess.entrySet()) {
            String procId = entry.getKey();
            Map<String, List<TableOperationSummary.TableAccessDetail>> tablesForProc = entry.getValue();

            // Build filtered table summaries for this proc
            Map<String, TableOperationSummary> filteredTables = new LinkedHashMap<>();
            for (var tableEntry : tablesForProc.entrySet()) {
                String tbl = tableEntry.getKey();
                TableOperationSummary original = allTables.get(tbl);
                if (original != null) {
                    TableOperationSummary filtered = new TableOperationSummary();
                    filtered.setTableName(original.getTableName());
                    filtered.setSchemaName(original.getSchemaName());
                    filtered.setTableType(original.getTableType());
                    filtered.setExternal(original.isExternal());
                    for (var d : tableEntry.getValue()) {
                        filtered.getAccessDetails().add(d);
                        if (d.getOperation() != null) filtered.getOperations().add(d.getOperation());
                    }
                    filtered.setAccessCount(filtered.getAccessDetails().size());
                    filteredTables.put(tbl, filtered);
                }
            }

            // Determine proc size from CallGraph node
            int procStartLine = 0;
            int procEndLine = 0;
            CallGraph graph = result.getCallGraph();
            if (graph != null) {
                com.plsqlanalyzer.analyzer.model.CallGraphNode cgNode = graph.getNode(procId);
                if (cgNode != null && cgNode.getStartLine() > 0 && cgNode.getEndLine() > cgNode.getStartLine()) {
                    procStartLine = cgNode.getStartLine();
                    procEndLine = cgNode.getEndLine();
                }
            }
            int procLines = (procEndLine > procStartLine) ? (procEndLine - procStartLine + 1) : 0;

            String shortProc = procId.contains(".") ? procId.substring(procId.lastIndexOf('.') + 1) : procId;

            if (procLines > MAX_SOURCE_LINES_PER_CHUNK) {
                // ---- LARGE PROC: split into source-window sub-chunks ----
                largeProcCount++;
                int step = MAX_SOURCE_LINES_PER_CHUNK - SOURCE_WINDOW_OVERLAP;

                for (int winStart = procStartLine; winStart < procEndLine; winStart += step) {
                    int winEnd = Math.min(winStart + MAX_SOURCE_LINES_PER_CHUNK, procEndLine);

                    // Only include tables with operations in this line range
                    Map<String, TableOperationSummary> windowTables = new LinkedHashMap<>();
                    for (var te : filteredTables.entrySet()) {
                        TableOperationSummary ts = te.getValue();
                        for (var ad : ts.getAccessDetails()) {
                            int ln = ad.getLineNumber();
                            // Include if line is in window, or if line is unknown (0)
                            if (ln == 0 || (ln >= winStart && ln <= winEnd)) {
                                windowTables.put(te.getKey(), ts);
                                break;
                            }
                        }
                    }

                    if (windowTables.isEmpty()) continue;

                    VerificationChunk chunk = new VerificationChunk();
                    chunk.id = "chunk_" + (++chunkNum);
                    chunk.procedureIds = List.of(procId);
                    chunk.tables = windowTables;
                    chunk.sourceWindowStart = winStart;
                    chunk.sourceWindowEnd = winEnd;
                    chunk.name = shortProc + " [L" + winStart + "-" + winEnd + "] → " + windowTables.size() + " tables";
                    chunks.add(chunk);
                }
            } else {
                // ---- NORMAL PROC: split by table count if >10 tables ----
                List<List<String>> tableBatches = new ArrayList<>();
                List<String> tableNames = new ArrayList<>(filteredTables.keySet());
                for (int i = 0; i < tableNames.size(); i += MAX_TABLES_PER_CHUNK) {
                    tableBatches.add(tableNames.subList(i, Math.min(i + MAX_TABLES_PER_CHUNK, tableNames.size())));
                }

                for (List<String> batch : tableBatches) {
                    VerificationChunk chunk = new VerificationChunk();
                    chunk.id = "chunk_" + (++chunkNum);
                    chunk.procedureIds = List.of(procId);
                    chunk.tables = new LinkedHashMap<>();
                    for (String tbl : batch) {
                        if (filteredTables.containsKey(tbl)) {
                            chunk.tables.put(tbl, filteredTables.get(tbl));
                        }
                    }
                    chunk.name = shortProc + " → " + batch.size() + " tables";
                    chunks.add(chunk);
                }
            }
        }

        log.info("Built {} initial chunks from {} procedures ({} large) across {} tables",
                chunks.size(), procAccess.size(), largeProcCount, allTables.size());

        // Merge small chunks to reduce Claude CLI invocations.
        // Small procs with few tables produce tiny prompts (~5-10KB).
        // Batching them into ~40KB groups cuts total chunks significantly.
        int beforeMerge = chunks.size();
        chunks = mergeSmallChunks(chunks, result);
        if (chunks.size() < beforeMerge) {
            log.info("Merged small chunks: {} → {} (saved {} Claude calls)",
                    beforeMerge, chunks.size(), beforeMerge - chunks.size());
        }

        // Re-number chunk IDs sequentially
        for (int i = 0; i < chunks.size(); i++) {
            chunks.get(i).id = "chunk_" + (i + 1);
        }

        return chunks;
    }

    /**
     * Merge small chunks (no source window, few tables, short estimated prompt) into larger batches
     * to reduce the number of Claude CLI invocations. Large/windowed chunks are kept as-is.
     */
    private List<VerificationChunk> mergeSmallChunks(List<VerificationChunk> chunks, AnalysisResult result) {
        List<VerificationChunk> large = new ArrayList<>();
        List<VerificationChunk> small = new ArrayList<>();

        for (VerificationChunk chunk : chunks) {
            boolean isWindowed = chunk.sourceWindowStart > 0 && chunk.sourceWindowEnd > chunk.sourceWindowStart;
            int estimated = estimatePromptSize(chunk, result);
            if (isWindowed || estimated > MERGE_TARGET_CHARS || chunk.tables.size() > MAX_TABLES_PER_CHUNK) {
                large.add(chunk);
            } else {
                small.add(chunk);
            }
        }

        if (small.size() <= 1) {
            // Nothing to merge
            List<VerificationChunk> all = new ArrayList<>(large);
            all.addAll(small);
            return all;
        }

        // Greedy bin-packing: add small chunks to current batch until target size exceeded
        List<VerificationChunk> merged = new ArrayList<>(large);
        VerificationChunk current = null;
        int currentSize = 0;

        for (VerificationChunk sc : small) {
            int scSize = estimatePromptSize(sc, result);

            if (current == null) {
                current = copyChunk(sc);
                currentSize = scSize;
                continue;
            }

            int combinedTables = current.tables.size() + sc.tables.size();
            if (currentSize + scSize > MERGE_TARGET_CHARS || combinedTables > MAX_TABLES_PER_CHUNK) {
                // Finalize current batch
                finalizeMergedName(current);
                merged.add(current);
                current = copyChunk(sc);
                currentSize = scSize;
            } else {
                // Add to current batch
                current.procedureIds = new ArrayList<>(current.procedureIds);
                for (String pid : sc.procedureIds) {
                    if (!current.procedureIds.contains(pid)) {
                        current.procedureIds.add(pid);
                    }
                }
                current.tables.putAll(sc.tables);
                currentSize += scSize;
            }
        }

        if (current != null) {
            finalizeMergedName(current);
            merged.add(current);
        }

        return merged;
    }

    private int estimatePromptSize(VerificationChunk chunk, AnalysisResult result) {
        int size = 0;

        // Estimate findings section size
        for (var entry : chunk.tables.entrySet()) {
            size += entry.getKey().length() + 30; // TABLE: header
            TableOperationSummary summary = entry.getValue();
            if (summary != null) {
                size += summary.getOperations().size() * 40;
                size += summary.getAccessDetails().size() * 80;
            }
        }

        // Estimate source code size
        for (String procId : chunk.procedureIds) {
            String[] parts = procId.toUpperCase().split("\\.");
            String sourceKey = parts.length >= 2 ? parts[0] + "." + parts[1] : procId.toUpperCase();
            List<String> lines = result.getSourceMap() != null ? result.getSourceMap().get(sourceKey) : null;
            if (lines == null) continue;

            CallGraph graph = result.getCallGraph();
            if (graph != null) {
                com.plsqlanalyzer.analyzer.model.CallGraphNode cgNode = graph.getNode(procId);
                if (cgNode != null && cgNode.getStartLine() > 0 && cgNode.getEndLine() > cgNode.getStartLine()) {
                    int lineCount = cgNode.getEndLine() - cgNode.getStartLine() + 1;
                    size += lineCount * 60; // average ~60 chars per line with line numbers
                    continue;
                }
            }
            size += Math.min(lines.size(), 200) * 60;
        }

        size += 1500; // template overhead (instructions, response format)
        return size;
    }

    private VerificationChunk copyChunk(VerificationChunk src) {
        VerificationChunk copy = new VerificationChunk();
        copy.procedureIds = new ArrayList<>(src.procedureIds);
        copy.tables = new LinkedHashMap<>(src.tables);
        copy.name = src.name;
        copy.sourceWindowStart = src.sourceWindowStart;
        copy.sourceWindowEnd = src.sourceWindowEnd;
        return copy;
    }

    private void finalizeMergedName(VerificationChunk chunk) {
        if (chunk.procedureIds.size() <= 1) return; // single-proc chunk, keep original name
        List<String> shortNames = new ArrayList<>();
        for (String pid : chunk.procedureIds) {
            String s = pid.contains(".") ? pid.substring(pid.lastIndexOf('.') + 1) : pid;
            shortNames.add(s);
        }
        String joined = shortNames.size() <= 3
                ? String.join(" + ", shortNames)
                : shortNames.get(0) + " + " + shortNames.get(1) + " + ... (" + shortNames.size() + " procs)";
        chunk.name = joined + " → " + chunk.tables.size() + " tables";
    }

    // ==================== PROMPT BUILDING ====================

    private String buildPrompt(VerificationChunk chunk, AnalysisResult result) {
        StringBuilder findingsSb = new StringBuilder();

        // Build static analysis findings section
        for (var entry : chunk.tables.entrySet()) {
            String tableName = entry.getKey();
            TableOperationSummary summary = entry.getValue();
            if (summary == null) continue;

            findingsSb.append("TABLE: ").append(tableName);
            if (summary.getSchemaName() != null) findingsSb.append(" (Schema: ").append(summary.getSchemaName()).append(")");
            findingsSb.append("\n");

            Set<?> ops = summary.getOperations();
            if (ops != null && !ops.isEmpty()) {
                findingsSb.append("  Operations: ");
                findingsSb.append(ops.stream().map(Object::toString).collect(Collectors.joining(", ")));
                findingsSb.append("\n");
            }

            for (var detail : summary.getAccessDetails()) {
                findingsSb.append("  - ").append(detail.getOperation()).append(" in ");
                findingsSb.append(detail.getProcedureName() != null ? detail.getProcedureName() : "?");
                if (detail.getLineNumber() > 0) findingsSb.append(" at line ").append(detail.getLineNumber());
                if (detail.getWhereFilters() != null && !detail.getWhereFilters().isEmpty()) {
                    findingsSb.append(" WHERE: ");
                    for (var wf : detail.getWhereFilters()) {
                        findingsSb.append(wf.getColumnName()).append(" ").append(wf.getOperator()).append(" ").append(wf.getValue()).append(", ");
                    }
                }
                findingsSb.append("\n");
            }
            findingsSb.append("\n");
        }

        // Build source window note
        boolean isWindowChunk = chunk.sourceWindowStart > 0 && chunk.sourceWindowEnd > chunk.sourceWindowStart;
        String sourceWindowNote = "";
        if (isWindowChunk) {
            sourceWindowNote = "-- NOTE: This is a source window chunk for a large procedure. " +
                    "Only lines " + chunk.sourceWindowStart + "-" + chunk.sourceWindowEnd +
                    " are shown. Verify table operations within this range. --";
        }

        // Build source code section
        StringBuilder sourceSb = new StringBuilder();
        int sourceCharsRemaining = MAX_CHUNK_CHARS;
        Set<String> includedSources = new HashSet<>();

        for (String procId : chunk.procedureIds) {
            if (sourceCharsRemaining <= 0) break;

            String[] parts = procId.toUpperCase().split("\\.");
            String sourceKey = parts.length >= 2 ? parts[0] + "." + parts[1] : procId.toUpperCase();
            String procName = parts.length >= 3 ? parts[2] : (parts.length >= 2 ? parts[1] : parts[0]);

            List<String> lines = result.getSourceMap() != null ? result.getSourceMap().get(sourceKey) : null;
            if (lines == null) continue;

            int includeFrom;
            int includeTo;

            if (isWindowChunk) {
                includeFrom = Math.max(0, chunk.sourceWindowStart - 1);
                includeTo = Math.min(lines.size(), chunk.sourceWindowEnd);
            } else {
                int procStart = -1;
                int procEnd = -1;

                CallGraph graph = result.getCallGraph();
                if (graph != null) {
                    com.plsqlanalyzer.analyzer.model.CallGraphNode cgNode = graph.getNode(procId);
                    if (cgNode != null && cgNode.getStartLine() > 0 && cgNode.getEndLine() > 0) {
                        procStart = Math.max(0, cgNode.getStartLine() - 3);
                        procEnd = Math.min(lines.size(), cgNode.getEndLine() + 1);
                    }
                }

                if (procStart < 0) {
                    String upperProcName = procName.toUpperCase();
                    for (int i = 0; i < lines.size(); i++) {
                        String lineUpper = lines.get(i).toUpperCase().trim();
                        if ((lineUpper.contains("PROCEDURE") || lineUpper.contains("FUNCTION"))
                                && lineUpper.contains(upperProcName)) {
                            procStart = Math.max(0, i - 2);
                            for (int j = i + 1; j < lines.size(); j++) {
                                String lu = lines.get(j).toUpperCase().trim();
                                if ((lu.startsWith("PROCEDURE ") || lu.startsWith("FUNCTION "))
                                        && !lu.contains(upperProcName)) {
                                    procEnd = j;
                                    break;
                                }
                                if (lu.matches("END\\s+" + upperProcName + "\\s*;.*")) {
                                    procEnd = j + 1;
                                    break;
                                }
                            }
                            if (procEnd < 0) procEnd = lines.size();
                            break;
                        }
                    }
                }

                if (procStart < 0) {
                    if (includedSources.contains(sourceKey)) continue;
                    procStart = 0;
                    procEnd = Math.min(lines.size(), 200);
                }

                includeFrom = procStart;
                includeTo = procEnd;
            }

            includedSources.add(sourceKey + ":" + procName);
            int lineCount = includeTo - includeFrom;

            sourceSb.append("-- Source: ").append(sourceKey).append(" > ").append(procName)
              .append(" (lines ").append(includeFrom + 1).append("-").append(includeTo)
              .append(", ").append(lineCount).append(" lines) --\n");

            for (int i = includeFrom; i < includeTo && sourceCharsRemaining > 0; i++) {
                String line = lines.get(i);
                String formatted = String.format("%5d: %s\n", i + 1, line);
                sourceSb.append(formatted);
                sourceCharsRemaining -= formatted.length();
            }
            if (sourceCharsRemaining <= 0) {
                sourceSb.append("... [source truncated — chunk char limit reached]\n");
            }
            sourceSb.append("\n");
        }

        // Load template and fill placeholders
        String prompt = com.jaranalyzer.service.PromptTemplates.buildPlsqlVerificationPrompt(
                findingsSb.toString(), sourceSb.toString(), sourceWindowNote);

        if (prompt.length() > MAX_PROMPT_CHARS) {
            prompt = prompt.substring(0, MAX_PROMPT_CHARS - 200) + "\n... [prompt truncated due to size]\n";
        }

        return prompt;
    }

    // ==================== RESPONSE PARSING ====================

    private ChunkResult parseResponse(VerificationChunk chunk, String response) {
        ChunkResult cr = new ChunkResult();
        cr.chunkId = chunk.id;
        cr.procedureIds = chunk.procedureIds;
        cr.tableVerifications = new ArrayList<>();

        // Extract JSON from response (might be wrapped in markdown code blocks)
        String json = extractJson(response);
        if (json == null) {
            cr.error = "Could not extract JSON from Claude response";
            log.warn("Chunk {}: no JSON in response (first 200 chars: {})",
                    chunk.id, response.substring(0, Math.min(200, response.length())));
            return cr;
        }

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = mapper.readValue(json, LinkedHashMap.class);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> tables = (List<Map<String, Object>>) parsed.get("tables");
            if (tables != null) {
                for (Map<String, Object> table : tables) {
                    TableVerification tv = new TableVerification();
                    tv.tableName = (String) table.get("tableName");
                    tv.operations = new ArrayList<>();

                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> ops = (List<Map<String, Object>>) table.get("operations");
                    if (ops != null) {
                        for (Map<String, Object> op : ops) {
                            OperationVerification ov = new OperationVerification();
                            ov.operation = (String) op.get("operation");
                            ov.status = (String) op.get("status");
                            ov.procedureName = (String) op.get("procedureName");
                            ov.lineNumber = op.get("lineNumber") instanceof Number n ? n.intValue() : 0;
                            ov.reason = (String) op.get("reason");
                            tv.operations.add(ov);
                        }
                    }
                    cr.tableVerifications.add(tv);
                }
            }

            cr.summary = (String) parsed.get("summary");
        } catch (Exception e) {
            cr.error = "Failed to parse JSON: " + e.getMessage();
            log.warn("Chunk {}: JSON parse error: {}", chunk.id, e.getMessage());
        }

        return cr;
    }

    private String extractJson(String response) {
        if (response == null) return null;
        String trimmed = response.trim();

        // Try: response is already valid JSON
        if (trimmed.startsWith("{")) return trimmed;

        // Try: extract from ```json ... ``` code block
        int jsonStart = trimmed.indexOf("```json");
        if (jsonStart >= 0) {
            int codeStart = trimmed.indexOf('\n', jsonStart);
            int codeEnd = trimmed.indexOf("```", codeStart + 1);
            if (codeStart >= 0 && codeEnd > codeStart) {
                return trimmed.substring(codeStart + 1, codeEnd).trim();
            }
        }

        // Try: extract from ``` ... ```
        int btStart = trimmed.indexOf("```");
        if (btStart >= 0) {
            int start = trimmed.indexOf('\n', btStart);
            int end = trimmed.indexOf("```", start + 1);
            if (start >= 0 && end > start) {
                String content = trimmed.substring(start + 1, end).trim();
                if (content.startsWith("{")) return content;
            }
        }

        // Try: find first { and last }
        int first = trimmed.indexOf('{');
        int last = trimmed.lastIndexOf('}');
        if (first >= 0 && last > first) {
            return trimmed.substring(first, last + 1);
        }

        return null;
    }

    // ==================== MERGING ====================

    private VerificationResult mergeResults(AnalysisResult staticResult, List<ChunkResult> chunkResults) {
        VerificationResult vr = new VerificationResult();
        vr.analysisName = staticResult.getName();
        vr.timestamp = LocalDateTime.now();
        vr.tables = new ArrayList<>();

        // Build a lookup of all static tables
        Map<String, TableOperationSummary> staticTables = staticResult.getTableOperations();

        // Build procedure ID lookup: bare name → list of fully qualified IDs (handles duplicates + overloads)
        Map<String, List<String>> bareToIds = new HashMap<>();
        CallGraph graph = staticResult.getCallGraph();
        if (graph != null) {
            for (var node : graph.getAllNodes()) {
                String nodeId = node.getId();
                // Use baseId for bare-name extraction (strips overload suffix like /3IN_2OUT)
                String baseId = node.getBaseId() != null ? node.getBaseId() : nodeId;
                String[] p = baseId.split("\\.");
                String bare = p[p.length - 1].toUpperCase();
                bareToIds.computeIfAbsent(bare, k -> new ArrayList<>()).add(nodeId);
            }
        }

        String entrySchema = staticResult.getEntrySchema();

        // Merge chunk results by table name
        Map<String, List<OperationVerification>> mergedOps = new LinkedHashMap<>();
        for (ChunkResult cr : chunkResults) {
            if (cr.tableVerifications == null) continue;

            // Build the set of qualified IDs from this chunk's procedure list for disambiguation
            Set<String> chunkProcUpper = new HashSet<>();
            if (cr.procedureIds != null) {
                for (String pid : cr.procedureIds) chunkProcUpper.add(pid.toUpperCase());
            }

            for (TableVerification tv : cr.tableVerifications) {
                if (tv.tableName == null) continue;
                String key = tv.tableName.toUpperCase();
                for (OperationVerification ov : tv.operations) {
                    resolveOperationIds(ov, bareToIds, chunkProcUpper, entrySchema);
                }
                mergedOps.computeIfAbsent(key, k -> new ArrayList<>()).addAll(tv.operations);
            }
        }

        // Build final table verification list
        Set<String> allTableNames = new LinkedHashSet<>();
        allTableNames.addAll(staticTables.keySet());
        allTableNames.addAll(mergedOps.keySet());

        int confirmed = 0, removed = 0, newOps = 0;

        for (String tableName : allTableNames) {
            TableVerificationResult tvr = new TableVerificationResult();
            tvr.tableName = tableName;
            tvr.staticOperations = new ArrayList<>();
            tvr.claudeVerifications = new ArrayList<>();

            // Static operations
            TableOperationSummary staticSummary = staticTables.get(tableName);
            if (staticSummary != null) {
                tvr.schemaName = staticSummary.getSchemaName();
                for (Object op : staticSummary.getOperations()) {
                    tvr.staticOperations.add(op.toString());
                }
                tvr.staticAccessCount = staticSummary.getAccessCount();
                tvr.isExternal = staticSummary.isExternal();
            }

            // Claude verifications
            List<OperationVerification> claudeOps = mergedOps.get(tableName);
            if (claudeOps != null) {
                // Deduplicate by operation + procedureName
                Map<String, OperationVerification> deduped = new LinkedHashMap<>();
                for (OperationVerification ov : claudeOps) {
                    String dedupKey = (ov.operation != null ? ov.operation : "?") + "|"
                            + (ov.procedureName != null ? ov.procedureName : "?");
                    deduped.putIfAbsent(dedupKey, ov);
                }
                tvr.claudeVerifications.addAll(deduped.values());

                for (OperationVerification ov : deduped.values()) {
                    String status = ov.status != null ? ov.status.toUpperCase() : "CONFIRMED";
                    switch (status) {
                        case "CONFIRMED" -> confirmed++;
                        case "REMOVED" -> removed++;
                        case "NEW" -> newOps++;
                    }
                }
            }

            // Determine overall status for this table
            if (claudeOps == null || claudeOps.isEmpty()) {
                tvr.overallStatus = "UNVERIFIED";
            } else {
                boolean hasRemoved = claudeOps.stream().anyMatch(o -> "REMOVED".equalsIgnoreCase(o.status));
                boolean hasNew = claudeOps.stream().anyMatch(o -> "NEW".equalsIgnoreCase(o.status));
                if (hasRemoved && hasNew) tvr.overallStatus = "MODIFIED";
                else if (hasRemoved) tvr.overallStatus = "PARTIAL";
                else if (hasNew) tvr.overallStatus = "EXPANDED";
                else tvr.overallStatus = "CONFIRMED";
            }

            vr.tables.add(tvr);
        }

        vr.confirmedCount = confirmed;
        vr.removedCount = removed;
        vr.newCount = newOps;
        vr.totalChunks = chunkResults.size();
        vr.errorChunks = (int) chunkResults.stream().filter(c -> c.error != null).count();

        return vr;
    }

    // ==================== RUN REPORT ====================

    private void generateRunReport(VerificationResult vr, int totalChunks, List<ChunkResult> chunkResults) {
        try {
            Path improvDir = dataBaseDir.resolve("improvement");
            Files.createDirectories(improvDir);

            String timestamp = LocalDateTime.now().toString().replace(":", "-").substring(0, 19);
            String safeName = (vr.analysisName != null ? vr.analysisName : "unknown")
                    .replaceAll("[^a-zA-Z0-9_-]", "_");
            Path reportFile = improvDir.resolve("run_" + safeName + "_" + timestamp + ".md");

            int errorChunks = (int) chunkResults.stream().filter(c -> c.error != null).count();
            int tablesVerified = 0, tablesUnverified = 0, tablesModified = 0;
            for (var t : (vr.tables != null ? vr.tables : Collections.<TableVerificationResult>emptyList())) {
                if ("UNVERIFIED".equals(t.overallStatus)) tablesUnverified++;
                else if ("MODIFIED".equals(t.overallStatus) || "PARTIAL".equals(t.overallStatus)) tablesModified++;
                else tablesVerified++;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("# Claude Verification Run Report\n\n");
            sb.append("- **Analysis:** ").append(vr.analysisName).append("\n");
            sb.append("- **Timestamp:** ").append(vr.timestamp).append("\n");
            sb.append("- **Total Chunks:** ").append(totalChunks).append("\n");
            sb.append("- **Error Chunks:** ").append(errorChunks).append("\n\n");

            sb.append("## Results Summary\n");
            sb.append("| Metric | Count |\n|--------|-------|\n");
            sb.append("| Confirmed operations | ").append(vr.confirmedCount).append(" |\n");
            sb.append("| New findings (missed by static) | ").append(vr.newCount).append(" |\n");
            sb.append("| Removed (false positives) | ").append(vr.removedCount).append(" |\n");
            sb.append("| Tables verified | ").append(tablesVerified).append(" |\n");
            sb.append("| Tables modified | ").append(tablesModified).append(" |\n");
            sb.append("| Tables unverified | ").append(tablesUnverified).append(" |\n\n");

            // List NEW findings (what static missed)
            if (vr.newCount > 0) {
                sb.append("## New Findings (Static Analysis Gaps)\n");
                sb.append("| Table | Operation | Procedure | Line |\n|-------|-----------|-----------|------|\n");
                for (var t : (vr.tables != null ? vr.tables : Collections.<TableVerificationResult>emptyList())) {
                    for (var v : t.claudeVerifications) {
                        if ("NEW".equalsIgnoreCase(v.status)) {
                            sb.append("| ").append(t.tableName != null ? t.tableName : "?");
                            sb.append(" | ").append(v.operation != null ? v.operation : "?");
                            sb.append(" | ").append(v.procedureName != null ? v.procedureName : "?");
                            sb.append(" | ").append(v.lineNumber > 0 ? String.valueOf(v.lineNumber) : "-");
                            sb.append(" |\n");
                        }
                    }
                }
                sb.append("\n");
            }

            // List REMOVED findings (false positives in static)
            if (vr.removedCount > 0) {
                sb.append("## Removed Findings (Static False Positives)\n");
                sb.append("| Table | Operation | Procedure | Reason |\n|-------|-----------|-----------|--------|\n");
                for (var t : (vr.tables != null ? vr.tables : Collections.<TableVerificationResult>emptyList())) {
                    for (var v : t.claudeVerifications) {
                        if ("REMOVED".equalsIgnoreCase(v.status)) {
                            sb.append("| ").append(t.tableName != null ? t.tableName : "?");
                            sb.append(" | ").append(v.operation != null ? v.operation : "?");
                            sb.append(" | ").append(v.procedureName != null ? v.procedureName : "?");
                            sb.append(" | ").append(v.reason != null ? v.reason : "-");
                            sb.append(" |\n");
                        }
                    }
                }
                sb.append("\n");
            }

            Files.writeString(reportFile, sb.toString());
            log.info("Generated run report: {}", reportFile);
        } catch (Exception e) {
            log.warn("Failed to generate run report: {}", e.getMessage());
        }
    }

    // ==================== PROCEDURE ID RESOLUTION ====================

    /**
     * Resolve bare procedure names to fully qualified IDs using call graph + chunk context.
     *
     * Disambiguation strategy:
     * 1. If procedureName is already qualified (contains '.'), use it directly.
     * 2. Look up bare name in call graph — if unique match, use it.
     * 3. If multiple matches (same name in different packages), prefer the one whose
     *    fully qualified ID appears in this chunk's procedureIds list.
     * 4. If still ambiguous, prefer 3-part IDs (SCHEMA.PACKAGE.PROC) over bare/2-part.
     * 5. For standalone procs (bare ID in call graph), construct sourceFile as SCHEMA.PROC.
     */
    private void resolveOperationIds(OperationVerification ov,
                                     Map<String, List<String>> bareToIds,
                                     Set<String> chunkProcIds,
                                     String entrySchema) {
        if (ov.procedureName == null || ov.procedureId != null) return;

        String bare = ov.procedureName.toUpperCase();

        // Already qualified — extract sourceFile from it
        if (bare.contains(".")) {
            ov.procedureId = ov.procedureName;
            String[] sp = bare.split("\\.");
            if (sp.length >= 2) ov.sourceFile = sp[0] + "." + sp[1];
            return;
        }

        List<String> candidates = bareToIds.get(bare);
        if (candidates == null || candidates.isEmpty()) {
            // Not in call graph — construct best-effort ID from entry schema
            if (entrySchema != null && !entrySchema.isEmpty()) {
                ov.procedureId = entrySchema + "." + ov.procedureName;
                ov.sourceFile = entrySchema + "." + ov.procedureName;
            }
            return;
        }

        if (candidates.size() == 1) {
            // Unique match — use it directly
            String nodeId = candidates.get(0);
            ov.procedureId = nodeId;
            ov.sourceFile = buildSourceFile(nodeId, entrySchema);
            return;
        }

        // Multiple candidates — disambiguate using chunk's procedure context
        String resolved = null;
        if (!chunkProcIds.isEmpty()) {
            for (String candidate : candidates) {
                // Check if this candidate (or its parent package) is in the chunk's procedure list
                String candidateUpper = candidate.toUpperCase();
                if (chunkProcIds.contains(candidateUpper)) {
                    resolved = candidate;
                    break;
                }
                // Also check if any chunk proc shares the same package as this candidate
                String[] cp = candidateUpper.split("\\.");
                if (cp.length >= 2) {
                    String candidatePkg = cp.length >= 3 ? cp[0] + "." + cp[1] : cp[0];
                    for (String chunkProc : chunkProcIds) {
                        if (chunkProc.startsWith(candidatePkg + ".")) {
                            resolved = candidate;
                            break;
                        }
                    }
                    if (resolved != null) break;
                }
            }
        }

        // Fallback: prefer 3-part IDs (SCHEMA.PACKAGE.PROC) over shorter ones
        if (resolved == null) {
            for (String candidate : candidates) {
                if (candidate.split("\\.").length >= 3) {
                    resolved = candidate;
                    break;
                }
            }
        }

        // Last resort: take first candidate
        if (resolved == null) {
            resolved = candidates.get(0);
        }

        ov.procedureId = resolved;
        ov.sourceFile = buildSourceFile(resolved, entrySchema);
    }

    private String buildSourceFile(String nodeId, String entrySchema) {
        // Strip overload suffix (e.g., "CUSTOMER.PKG.PROC/3IN_2OUT" → "CUSTOMER.PKG.PROC")
        String base = nodeId.contains("/") ? nodeId.substring(0, nodeId.indexOf('/')) : nodeId;
        String[] parts = base.split("\\.");
        if (parts.length >= 2) {
            return parts[0] + "." + parts[1];
        }
        // Standalone proc (bare ID) — use SCHEMA.PROC as sourceFile
        if (entrySchema != null && !entrySchema.isEmpty()) {
            return entrySchema + "." + parts[0];
        }
        return null;
    }

    // ==================== PERSISTENCE HELPERS ====================

    private void saveFragment(Path dir, String filename, Object data) {
        try {
            if (data instanceof String str) {
                Files.writeString(dir.resolve(filename), str);
            } else {
                mapper.writeValue(dir.resolve(filename).toFile(), data);
            }
        } catch (IOException e) {
            log.error("Failed to save fragment {}: {}", filename, e.getMessage());
        }
    }

    private void saveIndex(Path dir, List<VerificationChunk> chunks) {
        Map<String, Object> index = new LinkedHashMap<>();
        List<Map<String, Object>> chunkList = new ArrayList<>();
        for (VerificationChunk chunk : chunks) {
            Map<String, Object> cm = new LinkedHashMap<>();
            cm.put("id", chunk.id);
            cm.put("name", chunk.name);
            cm.put("procedures", chunk.procedureIds);
            cm.put("procedureCount", chunk.procedureIds.size());
            cm.put("tableCount", chunk.tables.size());
            chunkList.add(cm);
        }
        index.put("chunks", chunkList);
        index.put("timestamp", LocalDateTime.now().toString());
        saveFragment(dir, "_index.json", index);
    }

    private Set<String> loadCheckpoint(Path dir) {
        Path cpFile = dir.resolve("_checkpoint.json");
        if (!Files.exists(cpFile)) return new HashSet<>();
        try {
            @SuppressWarnings("unchecked")
            List<String> completed = mapper.readValue(cpFile.toFile(), List.class);
            return new HashSet<>(completed);
        } catch (IOException e) {
            return new HashSet<>();
        }
    }

    private void saveCheckpoint(Path dir, Set<String> completed) {
        saveFragment(dir, "_checkpoint.json", new ArrayList<>(completed));
    }

    private ChunkResult loadChunkResult(Path dir, String chunkId) {
        Path outputFile = dir.resolve(chunkId + "_output.json");
        if (!Files.exists(outputFile)) return null;
        try {
            String response = Files.readString(outputFile);
            VerificationChunk fakeChunk = new VerificationChunk();
            fakeChunk.id = chunkId;
            // Recover procedureIds from saved input for disambiguation
            Path inputFile = dir.resolve(chunkId + "_input.json");
            if (Files.exists(inputFile)) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> input = mapper.readValue(inputFile.toFile(), LinkedHashMap.class);
                    Object procs = input.get("procedures");
                    if (procs instanceof List<?> procList) {
                        fakeChunk.procedureIds = new ArrayList<>();
                        for (Object p : procList) fakeChunk.procedureIds.add(p.toString());
                    }
                } catch (IOException e) {
                    log.debug("Could not load chunk input for {}: {}", chunkId, e.getMessage());
                }
            }
            return parseResponse(fakeChunk, response);
        } catch (IOException e) {
            return null;
        }
    }

    private void progress(Consumer<String> callback, String message) {
        if (callback != null) callback.accept(message);
        log.info("[Claude] {}", message);
    }

    // ==================== DATA CLASSES ====================

    static class VerificationChunk {
        String id;
        String name;
        List<String> procedureIds = new ArrayList<>();
        Map<String, TableOperationSummary> tables = new LinkedHashMap<>();
        /** For large-proc source window chunks: which line range of the proc body this chunk covers */
        int sourceWindowStart = 0; // 0 means "include full proc"
        int sourceWindowEnd = 0;
    }

    static class ChunkResult {
        String chunkId;
        List<String> procedureIds;
        List<TableVerification> tableVerifications;
        String summary;
        String error;

        static ChunkResult error(String chunkId, String error) {
            ChunkResult cr = new ChunkResult();
            cr.chunkId = chunkId;
            cr.error = error;
            cr.tableVerifications = Collections.emptyList();
            return cr;
        }
    }

    static class TableVerification {
        String tableName;
        List<OperationVerification> operations = new ArrayList<>();
    }

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
