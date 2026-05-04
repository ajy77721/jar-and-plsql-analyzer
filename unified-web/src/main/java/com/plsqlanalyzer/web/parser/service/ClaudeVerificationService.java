package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Orchestrates chunked Claude CLI verification of plsql-parser static analysis.
 *
 * Reads from flow-output/{name}/api/ (nodes, tables, source files).
 * Stores Claude fragments in data/plsql/{name}/claude/.
 *
 * Delegates prompt building to ClaudePromptBuilder and uses models from
 * ClaudeVerificationModels.
 */
@Service("parserClaudeVerificationService")
public class ClaudeVerificationService {

    private static final Logger log = LoggerFactory.getLogger(ClaudeVerificationService.class);

    private static final int MAX_TABLES_PER_CHUNK = 20;
    private static final int MERGE_TARGET_CHARS = 40_000;
    private static final int MAX_SOURCE_LINES_PER_CHUNK = 500;
    private static final int SOURCE_WINDOW_OVERLAP = 100;

    private final ClaudeProcessRunner processRunner;
    private final StaticAnalysisReader staticReader;
    private final ClaudePromptBuilder promptBuilder;
    private final TriggerReAnalysisService triggerReAnalysis;
    private final ObjectMapper mapper;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicInteger claudeCallCount = new AtomicInteger(0);
    private volatile long claudeStartTime;

    @Value("${plsql.data-dir:data/plsql}")
    private String dataBaseDir;

    @Value("${claude.parallel-chunks:5}")
    private int parallelChunks;

    public ClaudeVerificationService(ClaudeProcessRunner processRunner,
                                     StaticAnalysisReader staticReader,
                                     TriggerReAnalysisService triggerReAnalysis) {
        this.processRunner = processRunner;
        this.staticReader = staticReader;
        this.triggerReAnalysis = triggerReAnalysis;
        this.promptBuilder = new ClaudePromptBuilder(staticReader);
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    private Path claudeDir(String analysisName) {
        return Paths.get(dataBaseDir).resolve(analysisName).resolve("claude");
    }

    /**
     * Run full verification of an analysis.
     *
     * @param analysisName     Name of the analysis folder in flow-output/
     * @param progressCallback Called with progress messages
     * @return Verification result with per-table status
     */
    public VerificationResult verify(String analysisName, Consumer<String> progressCallback) {
        cancelled.set(false);
        claudeCallCount.set(0);
        claudeStartTime = System.currentTimeMillis();
        Path outputDir = claudeDir(analysisName);
        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            return VerificationResult.error("Cannot create output directory: " + e.getMessage());
        }

        progress(progressCallback, "[1/5] Loading analysis data...");
        AnalysisData data = loadAnalysisData(analysisName);
        if (data == null) {
            return VerificationResult.error("Failed to load analysis data for: " + analysisName);
        }

        progress(progressCallback, "[1/5] Preparing verification chunks...");
        List<VerificationChunk> chunks = buildChunks(data);
        progress(progressCallback, "[1/5] Created " + chunks.size() + " verification chunks");

        Set<String> completedChunks = loadCheckpoint(outputDir);
        saveIndex(outputDir, chunks);

        AtomicInteger processed = new AtomicInteger(0);
        int total = chunks.size();
        List<ChunkResult> chunkResults = Collections.synchronizedList(new ArrayList<>());

        // Separate completed from pending
        List<VerificationChunk> pendingChunks = new ArrayList<>();
        int skipped = 0;
        for (VerificationChunk chunk : chunks) {
            if (completedChunks.contains(chunk.id)) {
                ChunkResult existing = loadChunkResult(outputDir, chunk.id);
                if (existing != null) {
                    chunkResults.add(existing);
                    skipped++;
                    processed.incrementAndGet();
                    continue;
                }
            }
            pendingChunks.add(chunk);
        }

        if (skipped > 0) {
            progress(progressCallback, "[2/5] Skipped " + skipped + " already-completed chunks");
        }

        processChunksInParallel(pendingChunks, data, outputDir, completedChunks,
                chunkResults, processed, total, progressCallback);

        progress(progressCallback, "[3/6] Merging verification results...");
        VerificationResult verResult = mergeResults(analysisName, data, chunkResults);

        verResult.claudeCallCount = claudeCallCount.get();
        verResult.claudeTimeMs = System.currentTimeMillis() - claudeStartTime;

        // Trigger re-analysis: discover triggers for NEW tables found by Claude
        if (verResult.newCount > 0 && triggerReAnalysis != null) {
            progress(progressCallback, "[4/6] Discovering triggers for new tables...");
            try {
                triggerReAnalysis.discoverTriggersForNewTables(verResult);
            } catch (Exception e) {
                log.warn("Trigger re-analysis failed (non-critical): {}", e.getMessage());
            }
        } else {
            progress(progressCallback, "[4/6] No new tables -- trigger re-analysis skipped");
        }

        progress(progressCallback, "[5/6] Saving verification result...");
        saveFragment(outputDir, "_result.json", verResult);

        progress(progressCallback, "[6/6] Verification complete: " + verResult.confirmedCount
                + " confirmed, " + verResult.removedCount + " removed, "
                + verResult.newCount + " new findings");
        return verResult;
    }

    /** Load the saved verification result for an analysis. */
    public VerificationResult loadResult(String analysisName) {
        Path resultFile = claudeDir(analysisName).resolve("_result.json");
        if (!Files.exists(resultFile)) return null;
        try {
            return mapper.readValue(resultFile.toFile(), VerificationResult.class);
        } catch (IOException e) {
            log.error("Failed to load result for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    /**
     * Load partial results from completed chunks (even if final _result.json doesn't exist yet).
     * Allows showing results while verification is still running or was interrupted.
     */
    public VerificationResult loadPartialResult(String analysisName) {
        Path dir = claudeDir(analysisName);
        if (!Files.exists(dir)) return null;

        // First try the final result
        VerificationResult finalResult = loadResult(analysisName);
        if (finalResult != null) return finalResult;

        // No final result -- build from completed chunk outputs
        Set<String> completedChunks = loadCheckpoint(dir);
        if (completedChunks.isEmpty()) return null;

        List<ChunkResult> chunkResults = new ArrayList<>();
        for (String chunkId : completedChunks) {
            ChunkResult cr = loadChunkResult(dir, chunkId);
            if (cr != null) chunkResults.add(cr);
        }
        if (chunkResults.isEmpty()) return null;

        // Load analysis data for merging
        AnalysisData data = loadAnalysisData(analysisName);
        if (data == null) return null;

        int totalChunks = loadTotalChunks(dir);

        VerificationResult partial = mergeResults(analysisName, data, chunkResults);
        partial.totalChunks = totalChunks > 0 ? totalChunks : chunkResults.size();
        partial.error = "Partial result (" + chunkResults.size() + "/" + partial.totalChunks
                + " chunks completed)";
        return partial;
    }

    /** Check if verification data exists for an analysis. */
    public boolean hasVerificationData(String analysisName) {
        Path dir = claudeDir(analysisName);
        if (Files.exists(dir.resolve("_result.json"))) return true;
        if (Files.exists(dir)) {
            try (var stream = Files.list(dir)) {
                return stream.anyMatch(p -> p.getFileName().toString().endsWith("_output.json")
                        && !p.getFileName().toString().startsWith("_"));
            } catch (IOException e) { /* ignore */ }
        }
        return false;
    }

    /** Get verification progress. */
    public Map<String, Object> getProgress(String analysisName) {
        Path dir = claudeDir(analysisName);
        Map<String, Object> progress = new LinkedHashMap<>();

        int completed = loadCheckpoint(dir).size();
        int total = loadTotalChunks(dir);
        int errorChunks = countFilesSuffix(dir, "_error.txt");

        progress.put("completedChunks", completed);
        progress.put("totalChunks", total);
        progress.put("errorChunks", errorChunks);
        progress.put("percentComplete", total > 0 ? (int) ((completed * 100.0) / total) : 0);
        progress.put("hasResult", Files.exists(dir.resolve("_result.json")));
        progress.put("hasPartialResult", completed > 0);
        progress.put("isComplete", completed >= total && total > 0);
        progress.put("claudeCallCount", claudeCallCount.get());
        progress.put("claudeTimeMs", claudeStartTime > 0 ? System.currentTimeMillis() - claudeStartTime : 0);
        return progress;
    }

    /** Get chunk fragment (input/output/error) for a specific chunk. */
    public Map<String, Object> getChunkFragment(String analysisName, String chunkId) {
        Path dir = claudeDir(analysisName);
        Map<String, Object> fragment = new LinkedHashMap<>();
        fragment.put("chunkId", chunkId);
        loadJsonFragment(dir, chunkId + "_input.json", "input", fragment);
        loadJsonFragment(dir, chunkId + "_output.json", "output", fragment);
        loadTextFragment(dir, chunkId + "_summary.txt", "lastMessage", fragment);
        loadTextFragment(dir, chunkId + "_error.txt", "error", fragment);
        return fragment;
    }

    /** List all chunk summaries for an analysis. */
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
                Map<String, Object> summary = new LinkedHashMap<>(chunk);
                if (Files.exists(dir.resolve(id + "_output.json"))) {
                    summary.put("status", "COMPLETE");
                } else if (Files.exists(dir.resolve(id + "_error.txt"))) {
                    summary.put("status", "ERROR");
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

    /**
     * List all chunk IDs for an analysis (simple ID list for backward compat).
     */
    @SuppressWarnings("unchecked")
    public List<String> listChunks(String analysisName) {
        Path dir = claudeDir(analysisName);
        Path indexFile = dir.resolve("_index.json");
        if (!Files.exists(indexFile)) return Collections.emptyList();
        try {
            Map<String, Object> index = mapper.readValue(indexFile.toFile(), LinkedHashMap.class);
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) index.get("chunks");
            if (chunks == null) return Collections.emptyList();
            List<String> ids = new ArrayList<>();
            for (Map<String, Object> c : chunks) {
                String id = (String) c.get("id");
                if (id != null) ids.add(id);
            }
            return ids;
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Build a reverse index: table name -> list of chunk IDs that contain that table.
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
                    // Not valid JSON — scan raw text for tableName patterns
                    java.util.regex.Pattern pat = java.util.regex.Pattern.compile(
                            "\"tableName\"\\s*:\\s*\"([^\"]+)\"");
                    java.util.regex.Matcher m = pat.matcher(raw);
                    while (m.find()) {
                        String name = m.group(1).toUpperCase();
                        mapping.computeIfAbsent(name, k -> new ArrayList<>()).add(chunkId);
                    }
                }
            } catch (IOException e) {
                log.debug("Cannot read chunk output {}: {}", chunkId, e.getMessage());
            }
        }
        return mapping;
    }

    public void cancel() {
        cancelled.set(true);
    }

    // ==================== PARALLEL EXECUTION ====================

    private void processChunksInParallel(List<VerificationChunk> pendingChunks,
                                         AnalysisData data, Path outputDir,
                                         Set<String> completedChunks,
                                         List<ChunkResult> chunkResults,
                                         AtomicInteger processed, int total,
                                         Consumer<String> progressCallback) {
        if (pendingChunks.isEmpty()) return;

        int activeThreads = Math.min(parallelChunks, pendingChunks.size());
        progress(progressCallback, "[2/5] Running Claude verification (" + pendingChunks.size()
                + " pending, " + activeThreads + " parallel)...");

        ExecutorService claudePool = Executors.newFixedThreadPool(Math.max(1, activeThreads));
        CompletionService<ChunkResult> cs = new ExecutorCompletionService<>(claudePool);
        Set<String> syncCompleted = Collections.synchronizedSet(completedChunks);

        int submitted = 0;
        for (VerificationChunk chunk : pendingChunks) {
            cs.submit(() -> processOneChunk(chunk, data, outputDir, syncCompleted,
                    processed, total, progressCallback));
            submitted++;
        }

        for (int i = 0; i < submitted; i++) {
            try {
                Future<ChunkResult> future = cs.poll(600 + MAX_TABLES_PER_CHUNK * 30 + 120, TimeUnit.SECONDS);
                if (future != null) {
                    ChunkResult cr = future.get();
                    if (cr != null) chunkResults.add(cr);
                } else {
                    chunkResults.add(ChunkResult.error("timeout_" + i, "Timeout"));
                }
            } catch (Exception e) {
                chunkResults.add(ChunkResult.error("unknown", e.getMessage()));
            }
        }

        claudePool.shutdown();
        try {
            if (!claudePool.awaitTermination(60, TimeUnit.SECONDS)) claudePool.shutdownNow();
        } catch (InterruptedException ie) {
            claudePool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private ChunkResult processOneChunk(VerificationChunk chunk, AnalysisData data,
                                        Path outputDir, Set<String> syncCompleted,
                                        AtomicInteger processed, int total,
                                        Consumer<String> progressCallback) {
        if (cancelled.get()) return ChunkResult.error(chunk.id, "Cancelled");

        int num = processed.incrementAndGet();
        progress(progressCallback, "[2/5] Chunk " + num + "/" + total + ": " + chunk.name);

        String prompt = promptBuilder.buildPrompt(chunk, data);

        // Save input fragment
        Map<String, Object> inputData = new LinkedHashMap<>();
        inputData.put("chunkId", chunk.id);
        inputData.put("name", chunk.name);
        inputData.put("nodeIds", chunk.nodeIds);
        inputData.put("tableCount", ChunkingUtils.countDistinctTables(chunk.tables));
        inputData.put("promptLength", prompt.length());
        inputData.put("timestamp", LocalDateTime.now().toString());
        inputData.put("prompt", prompt);
        saveFragment(outputDir, chunk.id + "_input.json", inputData);

        // Run Claude with one retry
        int timeoutSecs = 600 + (chunk.tables.size() * 30);
        String response = null;
        for (int attempt = 1; attempt <= 2; attempt++) {
            if (cancelled.get()) return ChunkResult.error(chunk.id, "Cancelled");
            claudeCallCount.incrementAndGet();
            response = processRunner.run(prompt, UUID.randomUUID().toString(), timeoutSecs);
            if (response != null && !response.isBlank()) break;
            if (attempt == 1) log.warn("Chunk {} attempt 1 failed, retrying...", chunk.id);
        }

        if (response == null || response.isBlank()) {
            saveFragment(outputDir, chunk.id + "_error.txt", "No response after 2 attempts");
            return ChunkResult.error(chunk.id, "No response from Claude");
        }

        saveFragment(outputDir, chunk.id + "_output.json", response);
        ChunkResult cr = parseResponse(chunk, response);
        if (cr.summary != null && !cr.summary.isBlank()) {
            saveFragment(outputDir, chunk.id + "_summary.txt", cr.summary);
        }
        syncCompleted.add(chunk.id);
        saveCheckpoint(outputDir, syncCompleted);
        return cr;
    }

    // ==================== ANALYSIS DATA LOADING ====================

    private AnalysisData loadAnalysisData(String analysisName) {
        try {
            AnalysisData data = new AnalysisData();
            data.analysisName = analysisName;

            JsonNode index = staticReader.getIndex(analysisName);
            data.entryPoint = textOrNull(index, "entryPoint");
            data.entrySchema = textOrNull(index, "entrySchema");

            JsonNode nodesArray = index.get("nodes");
            if (nodesArray != null && nodesArray.isArray()) {
                for (JsonNode nodeSummary : nodesArray) {
                    String nodeId = textOrNull(nodeSummary, "nodeId");
                    if (nodeId == null) continue;

                    NodeData nd = new NodeData();
                    nd.nodeId = nodeId;
                    nd.name = textOrNull(nodeSummary, "name");
                    nd.schema = textOrNull(nodeSummary, "schema");
                    nd.packageName = textOrNull(nodeSummary, "packageName");
                    nd.objectName = textOrNull(nodeSummary, "objectName");
                    nd.objectType = textOrNull(nodeSummary, "objectType");
                    nd.lineStart = intOrDefault(nodeSummary, "lineStart", 0);
                    nd.lineEnd = intOrDefault(nodeSummary, "lineEnd", 0);
                    nd.linesOfCode = intOrDefault(nodeSummary, "linesOfCode", 0);
                    nd.sourceFile = textOrNull(nodeSummary, "sourceFile");

                    String detailFile = textOrNull(nodeSummary, "detailFile");
                    if (detailFile != null) {
                        String fileName = detailFile.replace("nodes/", "");
                        try {
                            JsonNode detail = staticReader.getNodeDetail(analysisName, fileName);
                            nd.tableOps = extractTableOps(detail);
                        } catch (Exception e) {
                            log.debug("Cannot load detail for {}: {}", nodeId, e.getMessage());
                        }
                    }
                    data.nodes.put(nodeId, nd);
                }
            }

            loadCallGraph(data, analysisName);

            log.info("Loaded analysis data for '{}': {} nodes, {} call edges",
                    analysisName, data.nodes.size(), data.callGraph.values().stream().mapToInt(List::size).sum());
            return data;
        } catch (Exception e) {
            log.error("Failed to load analysis data for '{}': {}", analysisName, e.getMessage());
            return null;
        }
    }

    private List<TableOp> extractTableOps(JsonNode detail) {
        List<TableOp> ops = new ArrayList<>();
        JsonNode tables = detail.get("tables");
        if (tables == null || !tables.isArray()) return ops;

        for (JsonNode table : tables) {
            String tableName = textOrNull(table, "name");
            String schema = textOrNull(table, "schema");
            if (tableName == null) continue;

            JsonNode opsNode = table.get("operations");
            if (opsNode != null && opsNode.isObject()) {
                var fields = opsNode.fields();
                while (fields.hasNext()) {
                    var entry = fields.next();
                    String operation = entry.getKey();
                    JsonNode lines = entry.getValue();
                    if (lines.isArray()) {
                        for (JsonNode line : lines) {
                            TableOp op = new TableOp();
                            op.tableName = tableName;
                            op.schema = schema;
                            op.operation = operation;
                            op.line = line.asInt(0);
                            ops.add(op);
                        }
                    }
                }
            }
        }
        return ops;
    }

    /**
     * Load call graph edges into AnalysisData, filtering to only procedure-to-procedure calls
     * (skipping table column references that also appear as edges).
     */
    private void loadCallGraph(AnalysisData data, String analysisName) {
        try {
            JsonNode callGraphJson = staticReader.getCallGraph(analysisName);
            JsonNode edges = callGraphJson.get("edges");
            if (edges == null || !edges.isArray()) return;

            Set<String> knownNodeIds = data.nodes.keySet();

            for (JsonNode edge : edges) {
                String fromId = textOrNull(edge, "fromNodeId");
                String toId = textOrNull(edge, "toNodeId");
                if (fromId == null || toId == null) continue;

                // Only include edges where the target is a known procedure node
                // (this filters out table/column references that appear in the call graph)
                if (!knownNodeIds.contains(toId)) continue;

                CallEdge ce = new CallEdge();
                ce.fromNodeId = fromId;
                ce.fromName = textOrNull(edge, "from");
                ce.toNodeId = toId;
                ce.toName = textOrNull(edge, "to");

                data.callGraph.computeIfAbsent(fromId, k -> new ArrayList<>()).add(ce);
            }
        } catch (Exception e) {
            log.debug("No call graph available for '{}': {}", analysisName, e.getMessage());
        }
    }

    // ==================== CHUNKING ====================

    private List<VerificationChunk> buildChunks(AnalysisData data) {
        List<VerificationChunk> chunks = new ArrayList<>();
        int chunkNum = 0;

        for (var entry : data.nodes.entrySet()) {
            String nodeId = entry.getKey();
            NodeData node = entry.getValue();
            if (node.tableOps == null || node.tableOps.isEmpty()) continue;

            int procLines = (node.lineEnd > node.lineStart)
                    ? (node.lineEnd - node.lineStart + 1) : node.linesOfCode;
            String shortName = node.name != null ? node.name : nodeId;

            if (procLines > MAX_SOURCE_LINES_PER_CHUNK) {
                chunkNum = buildWindowChunks(chunks, chunkNum, nodeId, node, shortName);
            } else {
                chunkNum = buildTableBatchChunks(chunks, chunkNum, nodeId, node, shortName);
            }
        }

        log.info("Built {} initial chunks from {} nodes", chunks.size(), data.nodes.size());

        int beforeMerge = chunks.size();
        chunks = mergeSmallChunks(chunks, data);
        if (chunks.size() < beforeMerge) {
            log.info("Merged: {} -> {} (saved {} calls)", beforeMerge, chunks.size(), beforeMerge - chunks.size());
        }

        for (int i = 0; i < chunks.size(); i++) chunks.get(i).id = "chunk_" + (i + 1);
        return chunks;
    }

    /** Build overlapping source-window sub-chunks for large procs. */
    private int buildWindowChunks(List<VerificationChunk> chunks, int chunkNum,
                                  String nodeId, NodeData node, String shortName) {
        int step = MAX_SOURCE_LINES_PER_CHUNK - SOURCE_WINDOW_OVERLAP;
        int procStart = node.lineStart > 0 ? node.lineStart : 1;
        int procEnd = node.lineEnd > 0 ? node.lineEnd : procStart + node.linesOfCode;

        for (int winStart = procStart; winStart < procEnd; winStart += step) {
            int winEnd = Math.min(winStart + MAX_SOURCE_LINES_PER_CHUNK, procEnd);

            List<TableOp> windowOps = new ArrayList<>();
            for (TableOp op : node.tableOps) {
                if (op.line == 0 || (op.line >= winStart && op.line <= winEnd)) {
                    windowOps.add(op);
                }
            }
            if (windowOps.isEmpty()) continue;

            VerificationChunk chunk = new VerificationChunk();
            chunk.id = "chunk_" + (++chunkNum);
            chunk.nodeIds = List.of(nodeId);
            chunk.tables = windowOps;
            chunk.sourceWindowStart = winStart;
            chunk.sourceWindowEnd = winEnd;
            chunk.name = shortName + " [L" + winStart + "-" + winEnd + "] -> "
                    + ChunkingUtils.countDistinctTables(windowOps) + " tables";
            chunks.add(chunk);
        }
        return chunkNum;
    }

    /** Build table-batch chunks for normal-size procs. */
    private int buildTableBatchChunks(List<VerificationChunk> chunks, int chunkNum,
                                      String nodeId, NodeData node, String shortName) {
        Map<String, List<TableOp>> byTable = ChunkingUtils.groupByTable(node.tableOps);
        List<String> tableNames = new ArrayList<>(byTable.keySet());

        for (int i = 0; i < tableNames.size(); i += MAX_TABLES_PER_CHUNK) {
            List<String> batch = tableNames.subList(i, Math.min(i + MAX_TABLES_PER_CHUNK, tableNames.size()));

            VerificationChunk chunk = new VerificationChunk();
            chunk.id = "chunk_" + (++chunkNum);
            chunk.nodeIds = List.of(nodeId);
            chunk.tables = new ArrayList<>();
            for (String tbl : batch) chunk.tables.addAll(byTable.get(tbl));
            chunk.name = shortName + " -> " + batch.size() + " tables";
            chunks.add(chunk);
        }
        return chunkNum;
    }

    private List<VerificationChunk> mergeSmallChunks(List<VerificationChunk> chunks, AnalysisData data) {
        List<VerificationChunk> large = new ArrayList<>();
        List<VerificationChunk> small = new ArrayList<>();

        for (VerificationChunk c : chunks) {
            boolean isWindowed = c.sourceWindowStart > 0;
            int estimated = estimatePromptSize(c, data);
            if (isWindowed || estimated > MERGE_TARGET_CHARS
                    || ChunkingUtils.countDistinctTables(c.tables) > MAX_TABLES_PER_CHUNK) {
                large.add(c);
            } else {
                small.add(c);
            }
        }

        if (small.size() <= 1) {
            List<VerificationChunk> all = new ArrayList<>(large);
            all.addAll(small);
            return all;
        }

        List<VerificationChunk> merged = new ArrayList<>(large);
        VerificationChunk current = null;
        int currentSize = 0;

        for (VerificationChunk sc : small) {
            int scSize = estimatePromptSize(sc, data);
            if (current == null) {
                current = copyChunk(sc);
                currentSize = scSize;
                continue;
            }
            int combined = ChunkingUtils.countDistinctTables(current.tables) + ChunkingUtils.countDistinctTables(sc.tables);
            if (currentSize + scSize > MERGE_TARGET_CHARS || combined > MAX_TABLES_PER_CHUNK) {
                finalizeMergedName(current);
                merged.add(current);
                current = copyChunk(sc);
                currentSize = scSize;
            } else {
                current.nodeIds = new ArrayList<>(current.nodeIds);
                for (String nid : sc.nodeIds) {
                    if (!current.nodeIds.contains(nid)) current.nodeIds.add(nid);
                }
                current.tables.addAll(sc.tables);
                currentSize += scSize;
            }
        }
        if (current != null) {
            finalizeMergedName(current);
            merged.add(current);
        }
        return merged;
    }

    private int estimatePromptSize(VerificationChunk chunk, AnalysisData data) {
        int size = 1500;
        size += chunk.tables.size() * 80;
        for (String nodeId : chunk.nodeIds) {
            NodeData nd = data.nodes.get(nodeId);
            if (nd == null) continue;
            int lines = (nd.lineEnd > nd.lineStart) ? (nd.lineEnd - nd.lineStart + 1) : nd.linesOfCode;
            size += Math.min(lines, MAX_SOURCE_LINES_PER_CHUNK) * 60;
        }
        return size;
    }

    private VerificationChunk copyChunk(VerificationChunk src) {
        VerificationChunk c = new VerificationChunk();
        c.nodeIds = new ArrayList<>(src.nodeIds);
        c.tables = new ArrayList<>(src.tables);
        c.name = src.name;
        c.sourceWindowStart = src.sourceWindowStart;
        c.sourceWindowEnd = src.sourceWindowEnd;
        return c;
    }

    private void finalizeMergedName(VerificationChunk chunk) {
        if (chunk.nodeIds.size() <= 1) return;
        String joined = chunk.nodeIds.size() <= 3
                ? String.join(" + ", chunk.nodeIds)
                : chunk.nodeIds.get(0) + " + ... (" + chunk.nodeIds.size() + " procs)";
        chunk.name = joined + " -> " + ChunkingUtils.countDistinctTables(chunk.tables) + " tables";
    }

    // ==================== RESPONSE PARSING ====================

    private ChunkResult parseResponse(VerificationChunk chunk, String response) {
        ChunkResult cr = new ChunkResult();
        cr.chunkId = chunk.id;
        cr.nodeIds = chunk.nodeIds;
        cr.tableVerifications = new ArrayList<>();

        String json = extractJson(response);
        if (json == null) {
            cr.error = "Could not extract JSON from Claude response";
            log.warn("Chunk {}: no JSON found", chunk.id);
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
        }
        return cr;
    }

    private String extractJson(String response) {
        if (response == null) return null;
        String trimmed = response.trim();

        if (trimmed.startsWith("{")) return trimmed;

        int jsonStart = trimmed.indexOf("```json");
        if (jsonStart >= 0) {
            int codeStart = trimmed.indexOf('\n', jsonStart);
            int codeEnd = trimmed.indexOf("```", codeStart + 1);
            if (codeStart >= 0 && codeEnd > codeStart)
                return trimmed.substring(codeStart + 1, codeEnd).trim();
        }

        int btStart = trimmed.indexOf("```");
        if (btStart >= 0) {
            int start = trimmed.indexOf('\n', btStart);
            int end = trimmed.indexOf("```", start + 1);
            if (start >= 0 && end > start) {
                String content = trimmed.substring(start + 1, end).trim();
                if (content.startsWith("{")) return content;
            }
        }

        int first = trimmed.indexOf('{');
        int last = trimmed.lastIndexOf('}');
        if (first >= 0 && last > first) return trimmed.substring(first, last + 1);
        return null;
    }

    // ==================== MERGING ====================

    private VerificationResult mergeResults(String analysisName, AnalysisData data,
                                            List<ChunkResult> chunkResults) {
        VerificationResult vr = new VerificationResult();
        vr.analysisName = analysisName;
        vr.timestamp = LocalDateTime.now().toString();
        vr.tables = new ArrayList<>();

        // Collect static table operations
        Map<String, Set<String>> staticTableOps = new LinkedHashMap<>();
        for (NodeData nd : data.nodes.values()) {
            if (nd.tableOps == null) continue;
            for (TableOp op : nd.tableOps) {
                String key = (op.schema != null ? op.schema + "." : "") + op.tableName;
                staticTableOps.computeIfAbsent(key.toUpperCase(), k -> new LinkedHashSet<>())
                        .add(op.operation);
            }
        }

        // Merge Claude verifications by table
        Map<String, List<OperationVerification>> mergedOps = new LinkedHashMap<>();
        for (ChunkResult cr : chunkResults) {
            if (cr.tableVerifications == null) continue;
            for (TableVerification tv : cr.tableVerifications) {
                if (tv.tableName == null) continue;
                mergedOps.computeIfAbsent(tv.tableName.toUpperCase(), k -> new ArrayList<>())
                        .addAll(tv.operations);
            }
        }

        Set<String> allTableNames = new LinkedHashSet<>();
        allTableNames.addAll(staticTableOps.keySet());
        allTableNames.addAll(mergedOps.keySet());

        int confirmed = 0, removed = 0, newOps = 0;
        for (String tableName : allTableNames) {
            TableVerificationResult tvr = new TableVerificationResult();
            tvr.tableName = tableName;
            tvr.staticOperations = new ArrayList<>();

            Set<String> sOps = staticTableOps.get(tableName);
            if (sOps != null) tvr.staticOperations.addAll(sOps);

            List<OperationVerification> claudeOps = mergedOps.get(tableName);
            tvr.claudeVerifications = new ArrayList<>();
            if (claudeOps != null) {
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

            if (claudeOps == null || claudeOps.isEmpty()) tvr.overallStatus = "UNVERIFIED";
            else {
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

    // ==================== PERSISTENCE HELPERS ====================

    private void saveFragment(Path dir, String filename, Object data) {
        try {
            if (data instanceof String str) Files.writeString(dir.resolve(filename), str);
            else mapper.writeValue(dir.resolve(filename).toFile(), data);
        } catch (IOException e) {
            log.error("Failed to save {}: {}", filename, e.getMessage());
        }
    }

    private void saveIndex(Path dir, List<VerificationChunk> chunks) {
        Map<String, Object> index = new LinkedHashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();
        for (VerificationChunk c : chunks) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", c.id);
            m.put("name", c.name);
            m.put("nodeIds", c.nodeIds);
            m.put("tableCount", ChunkingUtils.countDistinctTables(c.tables));
            list.add(m);
        }
        index.put("chunks", list);
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
        } catch (IOException e) { return new HashSet<>(); }
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
            fakeChunk.nodeIds = new ArrayList<>();
            Path inputFile = dir.resolve(chunkId + "_input.json");
            if (Files.exists(inputFile)) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> input = mapper.readValue(inputFile.toFile(), LinkedHashMap.class);
                    Object nids = input.get("nodeIds");
                    if (nids instanceof List<?> list) {
                        for (Object p : list) fakeChunk.nodeIds.add(p.toString());
                    }
                } catch (IOException e) { /* ignore */ }
            }
            return parseResponse(fakeChunk, response);
        } catch (IOException e) { return null; }
    }

    @SuppressWarnings("unchecked")
    private int loadTotalChunks(Path dir) {
        Path indexFile = dir.resolve("_index.json");
        if (!Files.exists(indexFile)) return 0;
        try {
            Map<String, Object> index = mapper.readValue(indexFile.toFile(), LinkedHashMap.class);
            Object chunks = index.get("chunks");
            if (chunks instanceof List<?> list) return list.size();
        } catch (IOException e) { /* ignore */ }
        return 0;
    }

    private int countFilesSuffix(Path dir, String suffix) {
        if (!Files.exists(dir)) return 0;
        try (var stream = Files.list(dir)) {
            return (int) stream.filter(p -> p.getFileName().toString().endsWith(suffix)).count();
        } catch (IOException e) { return 0; }
    }

    private void loadJsonFragment(Path dir, String filename, String key, Map<String, Object> target) {
        Path file = dir.resolve(filename);
        if (!Files.exists(file)) return;
        try {
            String raw = Files.readString(file);
            try { target.put(key, mapper.readValue(raw, Object.class)); }
            catch (Exception e) { target.put(key, raw); }
        } catch (IOException e) { target.put(key, "Error: " + e.getMessage()); }
    }

    private void loadTextFragment(Path dir, String filename, String key, Map<String, Object> target) {
        Path file = dir.resolve(filename);
        if (!Files.exists(file)) return;
        try { target.put(key, Files.readString(file).trim()); } catch (IOException e) { /* ignore */ }
    }

    private void progress(Consumer<String> callback, String message) {
        if (callback != null) callback.accept(message);
        log.info("[Claude] {}", message);
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : null;
    }

    private static int intOrDefault(JsonNode node, String field, int def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asInt() : def;
    }
}
