package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.sso.CapturedSsoContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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

    @Value("${claude.parallel-chunks:5}")
    private int parallelChunks;

    private final ClaudeCallLogger callLogger;
    private final ArtifactStore store;

    public ClaudeVerificationService(ClaudeProcessRunner processRunner,
                                     StaticAnalysisReader staticReader,
                                     TriggerReAnalysisService triggerReAnalysis,
                                     ClaudeCallLogger callLogger,
                                     com.plsqlanalyzer.web.promptconfig.PromptConfigService promptConfig,
                                     @Qualifier("parserArtifactStore") ArtifactStore store) {
        this.processRunner = processRunner;
        this.staticReader = staticReader;
        this.triggerReAnalysis = triggerReAnalysis;
        this.callLogger = callLogger;
        this.store = Objects.requireNonNull(store, "ArtifactStore is required");
        this.promptBuilder = new ClaudePromptBuilder(staticReader, promptConfig);
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Claude sidecar data is stored flat, keyed by the (context-rich) identity name:
     * {@code plsql/<name>/claude/}. Built as an {@link ArtifactKey} rather than resolved from a
     * base directory string — the base could be an S3 root, and {@code Path.of} always means
     * local disk. The key also validates every segment, so an analysis name containing
     * {@code ".."} is rejected instead of escaping the data root as it would have before.
     */
    private ArtifactKey claudeDir(String analysisName) {
        return AnalysisPaths.claudeFileKey(analysisName, "claude");
    }

    /** Reads via the store. Callers must close. */
    private InputStream openRead(ArtifactKey key) throws IOException {
        return store.read(key);
    }

    /** Writes via the store; close() is the commit point. */
    private OutputStream openWrite(ArtifactKey key) throws IOException {
        return store.write(key);
    }

    private OutputStream openWrite(ArtifactKey key, long contentLength) throws IOException {
        return store.write(key, contentLength);
    }

    /** Every artifact in one chunk folder, in a single round trip. */
    private Map<ArtifactKey, ArtifactEntry> stat(ArtifactKey dir) {
        try {
            return store.statPrefix(dir);
        } catch (IOException e) {
            return Map.of();
        }
    }

    /**
     * Run full verification of an analysis.
     *
     * @param analysisName     Name of the analysis folder in flow-output/
     * @param progressCallback Called with progress messages
     * @return Verification result with per-table status
     */
    public VerificationResult verify(String analysisName, Consumer<String> progressCallback) {
        return verify(analysisName, false, progressCallback);
    }

    /**
     * @param force  Real gap this closes: {@code verify()} used to unconditionally skip any chunk
     *               that already had a persisted output file, with no way to bypass that — a
     *               "fresh" re-run after e.g. a prompt-template fix could silently keep serving old,
     *               pre-fix chunk results, discovered live this session (had to manually delete
     *               Mongo checkpoint artifacts to force a genuine re-run). {@code force=true} skips
     *               the checkpoint/existing-output check entirely, so every chunk is regenerated.
     */
    public VerificationResult verify(String analysisName, boolean force, Consumer<String> progressCallback) {
        cancelled.set(false);
        claudeCallCount.set(0);
        claudeStartTime = System.currentTimeMillis();
        ArtifactKey outputDir = claudeDir(analysisName);

        progress(progressCallback, "[1/5] Loading analysis data...");
        AnalysisData data = loadAnalysisData(analysisName);
        if (data == null) {
            return VerificationResult.error("Failed to load analysis data for: " + analysisName);
        }

        progress(progressCallback, "[1/5] Preparing verification chunks...");
        List<VerificationChunk> chunks = buildChunks(data);
        progress(progressCallback, "[1/5] Created " + chunks.size() + " verification chunks");

        Set<String> completedChunks = force ? Collections.emptySet() : loadCheckpoint(outputDir);
        if (force) progress(progressCallback, "[1/5] force=true — ignoring any existing checkpoint/chunk output");
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
        ArtifactKey resultFile = claudeDir(analysisName).resolve("_result.json");
        if (!store.exists(resultFile)) return null;
        try (InputStream in = openRead(resultFile)) {
            return mapper.readValue(in, VerificationResult.class);
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
        ArtifactKey dir = claudeDir(analysisName);

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
        ArtifactKey dir = claudeDir(analysisName);
        // ONE listing instead of an exists() plus a directory scan.
        try (var stream = store.list(dir, false)) {
            return stream.anyMatch(e -> e.key().leaf().equals("_result.json")
                    || (e.key().leaf().endsWith("_output.json") && !e.key().leaf().startsWith("_")));
        } catch (IOException e) {
            return false;
        }
    }

    /** Get verification progress. */
    public Map<String, Object> getProgress(String analysisName) {
        ArtifactKey dir = claudeDir(analysisName);
        Map<String, Object> progress = new LinkedHashMap<>();

        // ONE listing serves the error-chunk count and the _result.json check below.
        Map<ArtifactKey, ArtifactEntry> stat = stat(dir);
        int completed = loadCheckpoint(dir).size();
        int total = loadTotalChunks(dir);
        int errorChunks = (int) stat.keySet().stream()
                .filter(k -> k.leaf().endsWith("_error.txt")).count();

        progress.put("completedChunks", completed);
        progress.put("totalChunks", total);
        progress.put("errorChunks", errorChunks);
        progress.put("percentComplete", total > 0 ? (int) ((completed * 100.0) / total) : 0);
        progress.put("hasResult", stat.containsKey(dir.resolve("_result.json")));
        progress.put("hasPartialResult", completed > 0);
        progress.put("isComplete", completed >= total && total > 0);
        progress.put("claudeCallCount", claudeCallCount.get());
        progress.put("claudeTimeMs", claudeStartTime > 0 ? System.currentTimeMillis() - claudeStartTime : 0);
        return progress;
    }

    /** Get chunk fragment (input/output/error) for a specific chunk. */
    public Map<String, Object> getChunkFragment(String analysisName, String chunkId) {
        ArtifactKey dir = claudeDir(analysisName);
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
        ArtifactKey dir = claudeDir(analysisName);
        ArtifactKey indexFile = dir.resolve("_index.json");
        // ONE listing covers the index plus the two status probes per chunk. Previously this was
        // two exists() calls for every chunk in the run.
        Map<ArtifactKey, ArtifactEntry> stat = stat(dir);
        if (!stat.containsKey(indexFile)) return Collections.emptyList();
        try {
            Map<String, Object> index;
            try (InputStream in = openRead(indexFile)) {
                index = mapper.readValue(in, LinkedHashMap.class);
            }
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) index.get("chunks");
            if (chunks == null) return Collections.emptyList();

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map<String, Object> chunk : chunks) {
                String id = (String) chunk.get("id");
                Map<String, Object> summary = new LinkedHashMap<>(chunk);
                if (stat.containsKey(dir.resolve(id + "_output.json"))) {
                    summary.put("status", "COMPLETE");
                } else if (stat.containsKey(dir.resolve(id + "_error.txt"))) {
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
        ArtifactKey dir = claudeDir(analysisName);
        ArtifactKey indexFile = dir.resolve("_index.json");
        if (!store.exists(indexFile)) return Collections.emptyList();
        try {
            Map<String, Object> index;
            try (InputStream in = openRead(indexFile)) {
                index = mapper.readValue(in, LinkedHashMap.class);
            }
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
        ArtifactKey dir = claudeDir(analysisName);
        Map<String, List<String>> mapping = new LinkedHashMap<>();

        // ONE listing rather than an exists() per chunk.
        Map<ArtifactKey, ArtifactEntry> stat = stat(dir);
        List<String> chunkIds = listChunks(analysisName);
        for (String chunkId : chunkIds) {
            ArtifactKey outputFile = dir.resolve(chunkId + "_output.json");
            if (!stat.containsKey(outputFile)) continue;
            try {
                String raw = readString(outputFile);
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
                                         AnalysisData data, ArtifactKey outputDir,
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

        // The caller may be a request thread (routing scope) or a job thread (SsoJobContext); the
        // pool threads have neither. Capture both HERE, before submitting, and re-install per task.
        final RequestScopedArtifactStore.Scope routingScope = store instanceof RequestScopedArtifactStore routed
                ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;
        final CapturedSsoContext jobContext = CapturedSsoContext.capture(null);

        int submitted = 0;
        for (VerificationChunk chunk : pendingChunks) {
            cs.submit(() -> {
                try (AutoCloseable routed = routingScope.install();
                     AutoCloseable ctx = jobContext == null ? null : jobContext.install()) {
                    return processOneChunk(chunk, data, outputDir, syncCompleted,
                            processed, total, progressCallback);
                }
            });
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
                                        ArtifactKey outputDir, Set<String> syncCompleted,
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
        String sessionId = UUID.randomUUID().toString();
        for (int attempt = 1; attempt <= 2; attempt++) {
            if (cancelled.get()) return ChunkResult.error(chunk.id, "Cancelled");
            claudeCallCount.incrementAndGet();
            long __t0 = System.currentTimeMillis();
            response = processRunner.run(prompt, sessionId, timeoutSecs);
            long __dur = System.currentTimeMillis() - __t0;
            boolean ok = response != null && !response.isBlank();
            // Persistent per-analysis call log (per-session breakdown, survives restart).
            callLogger.logCall(data.analysisName, sessionId, "VERIFY", chunk.name, num,
                    prompt != null ? prompt.length() : 0,
                    ok ? response.length() : 0, __dur, ok,
                    ok ? null : "empty/failed response (attempt " + attempt + ")");
            if (ok) break;
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

            // Resolve the SQL dialect so the Claude prompt is not Oracle-specific.
            String dbType = textOrNull(index, "dbType");
            if (dbType != null && !dbType.isBlank()) {
                data.dialect = dbType.trim().toUpperCase();
            } else {
                data.dialect = com.plsqlanalyzer.web.util.DbDialectSupport
                        .dialectFromJdbcUrl(textOrNull(index, "jdbcUrl"));
            }

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
                            nd.joins = extractJoins(detail);
                            nd.sequences = extractSequences(detail);
                            nd.dynamicSql = extractDynamicSql(detail);
                        } catch (Exception e) {
                            log.debug("Cannot load detail for {}: {}", nodeId, e.getMessage());
                        }
                    }
                    data.nodes.put(nodeId, nd);
                }
            }

            loadPredicates(data, analysisName);
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

    private List<JoinOp> extractJoins(JsonNode detail) {
        List<JoinOp> joins = new ArrayList<>();
        JsonNode tables = detail.get("tables");
        if (tables == null || !tables.isArray()) return joins;
        for (JsonNode table : tables) {
            String containingTable = textOrNull(table, "name");
            JsonNode joinsNode = table.get("joins");
            if (joinsNode == null || !joinsNode.isArray()) continue;
            for (JsonNode j : joinsNode) {
                String rightTable = textOrNull(j, "joinedTable");
                if (rightTable == null) continue;
                JoinOp op = new JoinOp();
                String resolvedLeft = textOrNull(j, "leftTable");
                op.leftTable = resolvedLeft != null ? resolvedLeft : containingTable;
                op.rightTable = rightTable;
                op.joinType = textOrNull(j, "joinType");
                op.condition = textOrNull(j, "condition");
                op.line = j.has("line") ? j.get("line").asInt(0) : 0;
                joins.add(op);
            }
        }
        return joins;
    }

    private List<SequenceOp> extractSequences(JsonNode detail) {
        List<SequenceOp> seqs = new ArrayList<>();
        JsonNode sequences = detail.get("sequences");
        if (sequences == null || !sequences.isArray()) return seqs;
        for (JsonNode seq : sequences) {
            String name = textOrNull(seq, "name");
            if (name == null) continue;
            String schema = textOrNull(seq, "schema");
            JsonNode ops = seq.get("operations");
            if (ops != null && ops.isObject()) {
                var fields = ops.fields();
                while (fields.hasNext()) {
                    var entry = fields.next();
                    String opType = entry.getKey();
                    JsonNode lines = entry.getValue();
                    if (lines != null && lines.isArray()) {
                        for (JsonNode line : lines) {
                            SequenceOp op = new SequenceOp();
                            op.sequenceName = name;
                            op.schema = schema;
                            op.operation = opType;
                            op.line = line.asInt(0);
                            seqs.add(op);
                        }
                    }
                }
            }
        }
        return seqs;
    }

    private List<DynamicSqlOp> extractDynamicSql(JsonNode detail) {
        List<DynamicSqlOp> ops = new ArrayList<>();
        JsonNode dyn = detail.get("dynamicSql");
        if (dyn == null || !dyn.isArray()) return ops;
        for (JsonNode d : dyn) {
            DynamicSqlOp op = new DynamicSqlOp();
            op.type = textOrNull(d, "type");
            op.sqlExpression = textOrNull(d, "sqlExpression");
            op.line = d.has("line") ? d.get("line").asInt(0) : 0;
            ops.add(op);
        }
        return ops;
    }

    /**
     * Load WHERE-clause predicates via the aggregated tables facet (predicates aren't stored on the
     * per-node detail document; they live under each table's {@code usedBy[]} entries) and attach
     * them to the matching node by nodeId.
     */
    private void loadPredicates(AnalysisData data, String analysisName) {
        try {
            JsonNode tablesDoc = staticReader.getTables(analysisName);
            JsonNode tables = tablesDoc != null ? tablesDoc.get("tables") : null;
            if (tables == null || !tables.isArray()) return;
            for (JsonNode table : tables) {
                String tableName = textOrNull(table, "name");
                JsonNode usedBy = table.get("usedBy");
                if (usedBy == null || !usedBy.isArray()) continue;
                for (JsonNode u : usedBy) {
                    String nodeId = textOrNull(u, "nodeId");
                    if (nodeId == null) continue;
                    NodeData nd = data.nodes.get(nodeId);
                    if (nd == null) continue;
                    JsonNode preds = u.get("predicates");
                    if (preds == null || !preds.isArray()) continue;
                    for (JsonNode p : preds) {
                        PredicateOp op = new PredicateOp();
                        op.tableName = tableName;
                        op.columnName = textOrNull(p, "columnName");
                        op.operator = textOrNull(p, "operator");
                        op.value = textOrNull(p, "value");
                        op.line = p.has("lineNumber") ? p.get("lineNumber").asInt(0) : 0;
                        if (op.columnName != null) nd.predicates.add(op);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("No predicates available for '{}': {}", analysisName, e.getMessage());
        }
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
            boolean hasTableOps = node.tableOps != null && !node.tableOps.isEmpty();
            boolean hasExtraFacets = (node.joins != null && !node.joins.isEmpty())
                    || (node.sequences != null && !node.sequences.isEmpty())
                    || (node.dynamicSql != null && !node.dynamicSql.isEmpty())
                    || (node.predicates != null && !node.predicates.isEmpty());
            if (!hasTableOps && !hasExtraFacets) continue;

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
            boolean hasExtraFacets = (node.joins != null && !node.joins.isEmpty())
                    || (node.sequences != null && !node.sequences.isEmpty())
                    || (node.dynamicSql != null && !node.dynamicSql.isEmpty())
                    || (node.predicates != null && !node.predicates.isEmpty());
            if (windowOps.isEmpty() && !hasExtraFacets) continue;

            VerificationChunk chunk = new VerificationChunk();
            chunk.id = "chunk_" + (++chunkNum);
            chunk.nodeIds = List.of(nodeId);
            chunk.tables = windowOps;
            chunk.joins = node.joins != null ? node.joins : new ArrayList<>();
            chunk.sequences = node.sequences != null ? node.sequences : new ArrayList<>();
            chunk.dynamicSql = node.dynamicSql != null ? node.dynamicSql : new ArrayList<>();
            chunk.predicates = node.predicates != null ? node.predicates : new ArrayList<>();
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

        if (tableNames.isEmpty()) {
            boolean hasExtraFacets = (node.joins != null && !node.joins.isEmpty())
                    || (node.sequences != null && !node.sequences.isEmpty())
                    || (node.dynamicSql != null && !node.dynamicSql.isEmpty())
                    || (node.predicates != null && !node.predicates.isEmpty());
            if (hasExtraFacets) {
                VerificationChunk chunk = new VerificationChunk();
                chunk.id = "chunk_" + (++chunkNum);
                chunk.nodeIds = List.of(nodeId);
                chunk.tables = new ArrayList<>();
                chunk.joins = node.joins != null ? node.joins : new ArrayList<>();
                chunk.sequences = node.sequences != null ? node.sequences : new ArrayList<>();
                chunk.dynamicSql = node.dynamicSql != null ? node.dynamicSql : new ArrayList<>();
                chunk.predicates = node.predicates != null ? node.predicates : new ArrayList<>();
                chunk.name = shortName + " -> joins/sequences/dynamicSql/predicates";
                chunks.add(chunk);
            }
            return chunkNum;
        }

        for (int i = 0; i < tableNames.size(); i += MAX_TABLES_PER_CHUNK) {
            List<String> batch = tableNames.subList(i, Math.min(i + MAX_TABLES_PER_CHUNK, tableNames.size()));
            boolean firstBatch = i == 0;

            VerificationChunk chunk = new VerificationChunk();
            chunk.id = "chunk_" + (++chunkNum);
            chunk.nodeIds = List.of(nodeId);
            chunk.tables = new ArrayList<>();
            for (String tbl : batch) chunk.tables.addAll(byTable.get(tbl));
            // Extra facets aren't split per-table-batch; attach once (to the first batch) to avoid
            // asking Claude to re-verify the same joins/sequences/dynamicSql/predicates repeatedly.
            chunk.joins = firstBatch && node.joins != null ? node.joins : new ArrayList<>();
            chunk.sequences = firstBatch && node.sequences != null ? node.sequences : new ArrayList<>();
            chunk.dynamicSql = firstBatch && node.dynamicSql != null ? node.dynamicSql : new ArrayList<>();
            chunk.predicates = firstBatch && node.predicates != null ? node.predicates : new ArrayList<>();
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
                current.joins.addAll(sc.joins);
                current.sequences.addAll(sc.sequences);
                current.dynamicSql.addAll(sc.dynamicSql);
                current.predicates.addAll(sc.predicates);
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
        c.joins = new ArrayList<>(src.joins);
        c.sequences = new ArrayList<>(src.sequences);
        c.dynamicSql = new ArrayList<>(src.dynamicSql);
        c.predicates = new ArrayList<>(src.predicates);
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

            // Parse call-edge verifications (procedure→procedure). Optional/additive.
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> calls = (List<Map<String, Object>>) parsed.get("calls");
            if (calls != null) {
                for (Map<String, Object> call : calls) {
                    CallVerification cv = new CallVerification();
                    cv.fromProcedure = (String) call.get("fromProcedure");
                    cv.toProcedure = (String) call.get("toProcedure");
                    cv.status = (String) call.get("status");
                    cv.lineNumber = call.get("lineNumber") instanceof Number n ? n.intValue() : 0;
                    cv.reason = (String) call.get("reason");
                    if (cv.toProcedure != null && cv.status != null) cr.callVerifications.add(cv);
                }
            }
            // Parse join verifications. Optional/additive.
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> joins = (List<Map<String, Object>>) parsed.get("joins");
            if (joins != null) {
                for (Map<String, Object> j : joins) {
                    JoinVerification jv = new JoinVerification();
                    jv.leftTable = (String) j.get("leftTable");
                    jv.rightTable = (String) j.get("rightTable");
                    jv.joinType = (String) j.get("joinType");
                    jv.status = (String) j.get("status");
                    jv.lineNumber = j.get("lineNumber") instanceof Number n ? n.intValue() : 0;
                    jv.reason = (String) j.get("reason");
                    if (jv.rightTable != null && jv.status != null) cr.joinVerifications.add(jv);
                }
            }

            // Parse sequence verifications. Optional/additive.
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> sequences = (List<Map<String, Object>>) parsed.get("sequences");
            if (sequences != null) {
                for (Map<String, Object> s : sequences) {
                    SequenceVerification sv = new SequenceVerification();
                    sv.sequenceName = (String) s.get("sequenceName");
                    sv.operation = (String) s.get("operation");
                    sv.status = (String) s.get("status");
                    sv.lineNumber = s.get("lineNumber") instanceof Number n ? n.intValue() : 0;
                    sv.reason = (String) s.get("reason");
                    if (sv.sequenceName != null && sv.status != null) cr.sequenceVerifications.add(sv);
                }
            }

            // Parse dynamic-SQL verifications. Optional/additive.
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> dynamicSql = (List<Map<String, Object>>) parsed.get("dynamicSql");
            if (dynamicSql != null) {
                for (Map<String, Object> d : dynamicSql) {
                    DynamicSqlVerification dv = new DynamicSqlVerification();
                    dv.type = (String) d.get("type");
                    dv.sqlExpression = (String) d.get("sqlExpression");
                    dv.status = (String) d.get("status");
                    dv.lineNumber = d.get("lineNumber") instanceof Number n ? n.intValue() : 0;
                    dv.reason = (String) d.get("reason");
                    if (dv.status != null) cr.dynamicSqlVerifications.add(dv);
                }
            }

            // Parse predicate verifications. Optional/additive.
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> predicates = (List<Map<String, Object>>) parsed.get("predicates");
            if (predicates != null) {
                for (Map<String, Object> p : predicates) {
                    PredicateVerification pv = new PredicateVerification();
                    pv.tableName = (String) p.get("tableName");
                    pv.columnName = (String) p.get("columnName");
                    pv.operator = (String) p.get("operator");
                    pv.value = (String) p.get("value");
                    pv.status = (String) p.get("status");
                    pv.lineNumber = p.get("lineNumber") instanceof Number n ? n.intValue() : 0;
                    pv.reason = (String) p.get("reason");
                    if (pv.columnName != null && pv.status != null) cr.predicateVerifications.add(pv);
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

        // Merge call-edge verifications (dedup by from|to|status).
        Map<String, CallVerification> mergedCalls = new LinkedHashMap<>();
        for (ChunkResult cr : chunkResults) {
            if (cr.callVerifications == null) continue;
            for (CallVerification cv : cr.callVerifications) {
                if (cv.toProcedure == null) continue;
                String k = (cv.fromProcedure != null ? cv.fromProcedure.toUpperCase() : "?") + "|"
                        + cv.toProcedure.toUpperCase() + "|"
                        + (cv.status != null ? cv.status.toUpperCase() : "?");
                mergedCalls.putIfAbsent(k, cv);
            }
        }
        int callConfirmed = 0, callRemoved = 0, callNew = 0;
        for (CallVerification cv : mergedCalls.values()) {
            String st = cv.status != null ? cv.status.toUpperCase() : "CONFIRMED";
            switch (st) {
                case "CONFIRMED" -> callConfirmed++;
                case "REMOVED" -> callRemoved++;
                case "NEW" -> callNew++;
            }
            vr.calls.add(cv);
        }
        vr.callConfirmedCount = callConfirmed;
        vr.callRemovedCount = callRemoved;
        vr.callNewCount = callNew;

        mergeJoins(data, chunkResults, vr);
        mergeSequences(data, chunkResults, vr);
        mergeDynamicSql(data, chunkResults, vr);
        mergePredicates(data, chunkResults, vr);

        vr.totalChunks = chunkResults.size();
        vr.errorChunks = (int) chunkResults.stream().filter(c -> c.error != null).count();
        return vr;
    }

    private static String overallStatus(List<String> statuses) {
        if (statuses == null || statuses.isEmpty()) return "UNVERIFIED";
        boolean hasRemoved = statuses.stream().anyMatch(s -> "REMOVED".equalsIgnoreCase(s));
        boolean hasNew = statuses.stream().anyMatch(s -> "NEW".equalsIgnoreCase(s));
        if (hasRemoved && hasNew) return "MODIFIED";
        if (hasRemoved) return "PARTIAL";
        if (hasNew) return "EXPANDED";
        return "CONFIRMED";
    }

    /** Union of static + Claude JOIN findings, keyed by leftTable|rightTable|joinType. */
    private void mergeJoins(AnalysisData data, List<ChunkResult> chunkResults, VerificationResult vr) {
        Map<String, JoinOp> staticJoins = new LinkedHashMap<>();
        for (NodeData nd : data.nodes.values()) {
            if (nd.joins == null) continue;
            for (JoinOp j : nd.joins) {
                String key = up(j.leftTable) + "|" + up(j.rightTable) + "|" + up(j.joinType);
                staticJoins.putIfAbsent(key, j);
            }
        }
        Map<String, List<JoinVerification>> claudeJoins = new LinkedHashMap<>();
        for (ChunkResult cr : chunkResults) {
            if (cr.joinVerifications == null) continue;
            for (JoinVerification jv : cr.joinVerifications) {
                String key = up(jv.leftTable) + "|" + up(jv.rightTable) + "|" + up(jv.joinType);
                claudeJoins.computeIfAbsent(key, k -> new ArrayList<>()).add(jv);
            }
        }
        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(staticJoins.keySet());
        allKeys.addAll(claudeJoins.keySet());
        for (String key : allKeys) {
            JoinVerificationResult r = new JoinVerificationResult();
            JoinOp s = staticJoins.get(key);
            List<JoinVerification> cvs = claudeJoins.get(key);
            if (s != null) {
                r.leftTable = s.leftTable;
                r.rightTable = s.rightTable;
                r.joinType = s.joinType;
            } else if (cvs != null && !cvs.isEmpty()) {
                r.leftTable = cvs.get(0).leftTable;
                r.rightTable = cvs.get(0).rightTable;
                r.joinType = cvs.get(0).joinType;
            }
            r.claudeVerifications = cvs != null ? cvs : new ArrayList<>();
            r.overallStatus = overallStatus(r.claudeVerifications.stream().map(v -> v.status).toList());
            vr.joins.add(r);
        }
    }

    /** Union of static + Claude sequence findings, keyed by sequenceName. */
    private void mergeSequences(AnalysisData data, List<ChunkResult> chunkResults, VerificationResult vr) {
        Map<String, Set<String>> staticOps = new LinkedHashMap<>();
        for (NodeData nd : data.nodes.values()) {
            if (nd.sequences == null) continue;
            for (SequenceOp s : nd.sequences) {
                String key = up((s.schema != null ? s.schema + "." : "") + s.sequenceName);
                staticOps.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(s.operation);
            }
        }
        Map<String, List<SequenceVerification>> claudeSeq = new LinkedHashMap<>();
        for (ChunkResult cr : chunkResults) {
            if (cr.sequenceVerifications == null) continue;
            for (SequenceVerification sv : cr.sequenceVerifications) {
                claudeSeq.computeIfAbsent(up(sv.sequenceName), k -> new ArrayList<>()).add(sv);
            }
        }
        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(staticOps.keySet());
        allKeys.addAll(claudeSeq.keySet());
        for (String key : allKeys) {
            SequenceVerificationResult r = new SequenceVerificationResult();
            r.sequenceName = key;
            r.staticOperations = new ArrayList<>();
            Set<String> sOps = staticOps.get(key);
            if (sOps != null) r.staticOperations.addAll(sOps);
            List<SequenceVerification> cvs = claudeSeq.get(key);
            r.claudeVerifications = cvs != null ? cvs : new ArrayList<>();
            r.overallStatus = overallStatus(r.claudeVerifications.stream().map(v -> v.status).toList());
            vr.sequences.add(r);
        }
    }

    /** Union of static + Claude dynamic-SQL findings, keyed by a stable hash of type+line+sqlExpression. */
    private void mergeDynamicSql(AnalysisData data, List<ChunkResult> chunkResults, VerificationResult vr) {
        Map<String, DynamicSqlOp> staticDyn = new LinkedHashMap<>();
        for (NodeData nd : data.nodes.values()) {
            if (nd.dynamicSql == null) continue;
            for (DynamicSqlOp d : nd.dynamicSql) {
                String key = dynamicSqlKey(d.type, d.line, d.sqlExpression);
                staticDyn.putIfAbsent(key, d);
            }
        }
        Map<String, List<DynamicSqlVerification>> claudeDyn = new LinkedHashMap<>();
        for (ChunkResult cr : chunkResults) {
            if (cr.dynamicSqlVerifications == null) continue;
            for (DynamicSqlVerification dv : cr.dynamicSqlVerifications) {
                String key = dynamicSqlKey(dv.type, dv.lineNumber, dv.sqlExpression);
                claudeDyn.computeIfAbsent(key, k -> new ArrayList<>()).add(dv);
            }
        }
        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(staticDyn.keySet());
        allKeys.addAll(claudeDyn.keySet());
        for (String key : allKeys) {
            DynamicSqlVerificationResult r = new DynamicSqlVerificationResult();
            r.key = key;
            DynamicSqlOp s = staticDyn.get(key);
            List<DynamicSqlVerification> cvs = claudeDyn.get(key);
            if (s != null) {
                r.type = s.type;
                r.sqlExpression = s.sqlExpression;
            } else if (cvs != null && !cvs.isEmpty()) {
                r.type = cvs.get(0).type;
                r.sqlExpression = cvs.get(0).sqlExpression;
            }
            r.claudeVerifications = cvs != null ? cvs : new ArrayList<>();
            r.overallStatus = overallStatus(r.claudeVerifications.stream().map(v -> v.status).toList());
            vr.dynamicSql.add(r);
        }
    }

    private static String dynamicSqlKey(String type, int line, String sql) {
        int hash = (sql != null ? sql : "").hashCode();
        return up(type) + "|" + line + "|" + hash;
    }

    /** Union of static + Claude predicate findings, keyed by tableName|columnName. */
    private void mergePredicates(AnalysisData data, List<ChunkResult> chunkResults, VerificationResult vr) {
        Map<String, PredicateOp> staticPreds = new LinkedHashMap<>();
        for (NodeData nd : data.nodes.values()) {
            if (nd.predicates == null) continue;
            for (PredicateOp p : nd.predicates) {
                String key = up(p.tableName) + "|" + up(p.columnName);
                staticPreds.putIfAbsent(key, p);
            }
        }
        Map<String, List<PredicateVerification>> claudePreds = new LinkedHashMap<>();
        for (ChunkResult cr : chunkResults) {
            if (cr.predicateVerifications == null) continue;
            for (PredicateVerification pv : cr.predicateVerifications) {
                String key = up(pv.tableName) + "|" + up(pv.columnName);
                claudePreds.computeIfAbsent(key, k -> new ArrayList<>()).add(pv);
            }
        }
        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(staticPreds.keySet());
        allKeys.addAll(claudePreds.keySet());
        for (String key : allKeys) {
            PredicateVerificationResult r = new PredicateVerificationResult();
            PredicateOp s = staticPreds.get(key);
            List<PredicateVerification> cvs = claudePreds.get(key);
            if (s != null) {
                r.tableName = s.tableName;
                r.columnName = s.columnName;
            } else if (cvs != null && !cvs.isEmpty()) {
                r.tableName = cvs.get(0).tableName;
                r.columnName = cvs.get(0).columnName;
            }
            r.claudeVerifications = cvs != null ? cvs : new ArrayList<>();
            r.overallStatus = overallStatus(r.claudeVerifications.stream().map(v -> v.status).toList());
            vr.predicates.add(r);
        }
    }

    private static String up(String s) {
        return s != null ? s.toUpperCase() : "?";
    }

    // ==================== PERSISTENCE HELPERS ====================

    /** Full read of a small fragment as text. */
    private String readString(ArtifactKey key) throws IOException {
        try (InputStream in = openRead(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void saveFragment(ArtifactKey dir, String filename, Object data) {
        ArtifactKey key = dir.resolve(filename);
        try {
            if (data instanceof String str) {
                byte[] body = str.getBytes(StandardCharsets.UTF_8);
                try (OutputStream out = openWrite(key, body.length)) {
                    out.write(body);
                }
            } else {
                try (OutputStream out = openWrite(key)) {
                    mapper.writeValue(out, data);
                }
            }
        } catch (IOException e) {
            log.error("Failed to save {}: {}", filename, e.getMessage());
        }
    }

    private void saveIndex(ArtifactKey dir, List<VerificationChunk> chunks) {
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

    private Set<String> loadCheckpoint(ArtifactKey dir) {
        ArtifactKey cpFile = dir.resolve("_checkpoint.json");
        if (!store.exists(cpFile)) return new HashSet<>();
        try (InputStream in = openRead(cpFile)) {
            @SuppressWarnings("unchecked")
            List<String> completed = mapper.readValue(in, List.class);
            return new HashSet<>(completed);
        } catch (IOException e) { return new HashSet<>(); }
    }

    private void saveCheckpoint(ArtifactKey dir, Set<String> completed) {
        saveFragment(dir, "_checkpoint.json", new ArrayList<>(completed));
    }

    private ChunkResult loadChunkResult(ArtifactKey dir, String chunkId) {
        ArtifactKey outputFile = dir.resolve(chunkId + "_output.json");
        if (!store.exists(outputFile)) return null;
        try {
            String response = readString(outputFile);
            VerificationChunk fakeChunk = new VerificationChunk();
            fakeChunk.id = chunkId;
            fakeChunk.nodeIds = new ArrayList<>();
            ArtifactKey inputFile = dir.resolve(chunkId + "_input.json");
            if (store.exists(inputFile)) {
                try (InputStream in = openRead(inputFile)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> input = mapper.readValue(in, LinkedHashMap.class);
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
    private int loadTotalChunks(ArtifactKey dir) {
        ArtifactKey indexFile = dir.resolve("_index.json");
        if (!store.exists(indexFile)) return 0;
        try (InputStream in = openRead(indexFile)) {
            Map<String, Object> index = mapper.readValue(in, LinkedHashMap.class);
            Object chunks = index.get("chunks");
            if (chunks instanceof List<?> list) return list.size();
        } catch (IOException e) { /* ignore */ }
        return 0;
    }

    private void loadJsonFragment(ArtifactKey dir, String filename, String key, Map<String, Object> target) {
        ArtifactKey file = dir.resolve(filename);
        if (!store.exists(file)) return;
        try {
            String raw = readString(file);
            try { target.put(key, mapper.readValue(raw, Object.class)); }
            catch (Exception e) { target.put(key, raw); }
        } catch (IOException e) { target.put(key, "Error: " + e.getMessage()); }
    }

    private void loadTextFragment(ArtifactKey dir, String filename, String key, Map<String, Object> target) {
        ArtifactKey file = dir.resolve(filename);
        if (!store.exists(file)) return;
        try { target.put(key, readString(file).trim()); } catch (IOException e) { /* ignore */ }
    }

    private void progress(Consumer<String> callback, String message) {
        if (callback != null) callback.accept(message);
        log.info("[Claude] {}", message);
    }

    public int getParallelChunks() { return parallelChunks; }

    public void setParallelChunks(int n) { this.parallelChunks = Math.max(1, n); }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : null;
    }

    private static int intOrDefault(JsonNode node, String field, int def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asInt() : def;
    }
}
