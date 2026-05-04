package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.*;
import com.jaranalyzer.model.CorrectionResult.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Orchestrates Claude-based correction of static analysis results.
 * Sends each endpoint's static analysis + decompiled source to Claude,
 * asking it to verify/correct collection names, operation types, and categories.
 * Results stored separately via CorrectionPersistence for side-by-side comparison.
 *
 * Reuses existing infrastructure: ClaudeProcessRunner (subprocess), TreeChunker (chunking),
 * ClaudeSessionManager (background tasks), ClaudeEnrichmentTracker (progress).
 */
@Service
public class ClaudeCorrectionService {

    private static final Logger log = LoggerFactory.getLogger(ClaudeCorrectionService.class);
    private static final AtomicInteger ENDPOINT_THREAD_SEQ = new AtomicInteger(0);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ClaudeProcessRunner processRunner;
    private final TreeChunker treeChunker;
    private final CorrectionPersistence persistence;
    private final ClaudeResultMerger resultMerger;
    private final FragmentStore fragmentStore;
    private final ClaudeEnrichmentTracker tracker;
    private final ClaudeCallLogger callLogger;

    @Value("${claude.analysis.parallel-chunks:4}")
    private int parallelChunks;

    @Value("${claude.analysis.parallel-endpoints:3}")
    private int parallelEndpoints;

    @Value("${claude.timeout.executor-shutdown:30}")
    private long executorShutdownTimeout;

    public ClaudeCorrectionService(ClaudeProcessRunner processRunner, TreeChunker treeChunker,
                                    CorrectionPersistence persistence, ClaudeResultMerger resultMerger,
                                    FragmentStore fragmentStore, ClaudeEnrichmentTracker tracker,
                                    ClaudeCallLogger callLogger) {
        this.processRunner = processRunner;
        this.treeChunker = treeChunker;
        this.persistence = persistence;
        this.resultMerger = resultMerger;
        this.fragmentStore = fragmentStore;
        this.tracker = tracker;
        this.callLogger = callLogger;
    }

    /**
     * Correct all endpoints for a JAR. Called from background thread via ClaudeSessionManager.
     * Supports resume: skips endpoints that already have correction data.
     * Processes up to {@code parallelEndpoints} endpoints concurrently (default 3).
     */
    public void correctEndpoints(List<EndpointInfo> endpoints, String jarName,
                                  ProgressService progressService, boolean resume) {
        int total = endpoints.size();
        AtomicInteger done = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        AtomicInteger empty = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        // Per-run log file for this scan
        Path logPath = persistence.getWorkDir(jarName)
                .resolve("run_" + java.time.LocalDateTime.now().toString().replace(':', '-').replace('.', '-') + ".log");
        try { Files.createDirectories(logPath.getParent()); } catch (IOException ignored) {}

        runLog(logPath, "=== Claude Full Scan: " + jarName + " ===");
        runLog(logPath, "Started: " + java.time.LocalDateTime.now());
        runLog(logPath, "Endpoints: " + total + " | Resume: " + resume + " | Parallel: " + parallelEndpoints);
        runLog(logPath, "");

        // Phase 1: filter — skip already-corrected in resume mode
        List<EndpointInfo> toProcess = new ArrayList<>();
        for (EndpointInfo ep : endpoints) {
            String epKey = ep.getControllerSimpleName() + "." + ep.getMethodName();
            if (resume && persistence.loadCorrection(jarName, epKey) != null) {
                done.incrementAndGet();
                skipped.incrementAndGet();
                tracker.markEndpointComplete(jarName, epKey);
                runLog(logPath, "[SKIP] " + epKey + " — already corrected");
            } else {
                toProcess.add(ep);
            }
        }

        // Phase 2: process remaining endpoints in parallel
        ExecutorService subExecutor = Executors.newFixedThreadPool(parallelEndpoints, r -> {
            Thread t = new Thread(r, "claude-ep-" + jarName.hashCode() + "-" + ENDPOINT_THREAD_SEQ.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        try {
            List<Future<?>> futures = new ArrayList<>();
            for (EndpointInfo ep : toProcess) {
                futures.add(subExecutor.submit(() -> {
                    String epKey = ep.getControllerSimpleName() + "." + ep.getMethodName();
                    long epStart = System.currentTimeMillis();
                    try {
                        tracker.markProcessing(jarName, epKey);
                        int d = done.incrementAndGet();
                        if (progressService != null) {
                            progressService.detail("Correcting " + d + "/" + total + ": " + epKey);
                        }

                        CorrectionResult result = correctSingleEndpoint(ep, jarName);
                        long epMs = System.currentTimeMillis() - epStart;
                        if (result != null) {
                            persistence.saveCorrection(jarName, epKey, result);
                            int corrCount = result.getCorrections() != null ? result.getCorrections().size() : 0;
                            runLog(logPath, "[OK]   " + epKey + " — " + corrCount + " corrections (" + epMs + "ms)");
                        } else {
                            empty.incrementAndGet();
                            runLog(logPath, "[EMPTY] " + epKey + " — no response from Claude (" + epMs + "ms)");
                        }
                        tracker.markEndpointComplete(jarName, epKey);
                    } catch (Exception e) {
                        long epMs = System.currentTimeMillis() - epStart;
                        String errMsg = e.getMessage() != null ? e.getMessage()
                                : e.getClass().getSimpleName() + " (no message)";
                        log.warn("Correction failed for {}: {} [{}]", epKey, errMsg, e.getClass().getSimpleName());
                        tracker.markEndpointError(jarName, epKey, errMsg);
                        runLog(logPath, "[FAIL] " + epKey + " — " + errMsg + " (" + epMs + "ms)");
                        errors.incrementAndGet();
                    }
                }));
            }

            // Wait for all futures, respecting interruption (kill)
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.info("Correction interrupted for {} — shutting down parallel workers", jarName);
                    runLog(logPath, "\n[INTERRUPTED] — cancelling remaining endpoints");
                    break;
                } catch (ExecutionException e) {
                    // Already handled inside the task
                }
            }
        } finally {
            subExecutor.shutdownNow();
            try { subExecutor.awaitTermination(executorShutdownTimeout, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }

        long totalMs = System.currentTimeMillis() - startTime;
        int doneVal = done.get(), errVal = errors.get(), emptyVal = empty.get(), skipVal = skipped.get();

        // Write summary
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("jarName", jarName);
        summary.put("totalEndpoints", total);
        summary.put("corrected", doneVal - errVal - emptyVal - skipVal);
        summary.put("empty", emptyVal);
        summary.put("skipped", skipVal);
        summary.put("errors", errVal);
        summary.put("parallelEndpoints", parallelEndpoints);
        summary.put("durationMs", totalMs);
        summary.put("completedAt", java.time.LocalDateTime.now().toString());
        summary.put("logFile", logPath.getFileName().toString());
        persistence.saveSummary(jarName, summary);

        generateImprovementNotes(jarName);

        runLog(logPath, "");
        runLog(logPath, "=== Summary ===");
        runLog(logPath, "Total: " + total + " | OK: " + (doneVal - errVal - emptyVal - skipVal)
                + " | Empty: " + emptyVal + " | Errors: " + errVal + " | Skipped: " + skipVal);
        runLog(logPath, "Parallel: " + parallelEndpoints + " workers");
        runLog(logPath, "Duration: " + (totalMs / 1000) + "s");
        runLog(logPath, "Completed: " + java.time.LocalDateTime.now());

        log.info("Correction complete for {}: {}/{} endpoints, {} errors, {} empty, parallel={} ({}s) — log: {}",
                jarName, doneVal, total, errVal, emptyVal, parallelEndpoints, totalMs / 1000, logPath.getFileName());
    }

    /** Append a line to the per-run log file */
    private void runLog(Path logPath, String line) {
        try {
            Files.writeString(logPath, line + "\n",
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (IOException e) {
            // Best effort — don't fail the scan for logging
        }
    }

    private void generateImprovementNotes(String jarName) {
        try {
            Map<String, CorrectionResult> allCorrections = persistence.loadAllCorrections(jarName);
            if (allCorrections.isEmpty()) return;

            Path improvDir = persistence.getWorkDir(jarName).getParent().resolve("improvement");
            Files.createDirectories(improvDir);

            String ts = LocalDateTime.now().toString().replace(":", "-").substring(0, 19);
            String safeName = JarNameUtil.sanitize(jarName);
            Path reportFile = improvDir.resolve("run_" + safeName + "_" + ts + ".md");

            int totalEndpoints = allCorrections.size();
            int totalAdded = 0, totalRemoved = 0, totalVerified = 0, totalOpChanges = 0;
            List<String[]> addedRows = new ArrayList<>();
            List<String[]> removedRows = new ArrayList<>();
            List<String[]> opChangeRows = new ArrayList<>();

            for (var entry : allCorrections.entrySet()) {
                String epName = entry.getKey();
                CorrectionResult cr = entry.getValue();
                if (cr.getCorrections() == null) continue;

                for (NodeCorrection nc : cr.getCorrections()) {
                    if (nc.getCollections() != null) {
                        CollectionCorrections cc = nc.getCollections();
                        if (cc.getAdded() != null) {
                            for (CollectionEntry ce : cc.getAdded()) {
                                totalAdded++;
                                addedRows.add(new String[]{
                                        epName,
                                        nc.getNodeId() != null ? nc.getNodeId() : "-",
                                        ce.getName() != null ? ce.getName() : "?",
                                        ce.getOperation() != null ? ce.getOperation() : "-"
                                });
                            }
                        }
                        if (cc.getRemoved() != null) {
                            for (String rem : cc.getRemoved()) {
                                totalRemoved++;
                                removedRows.add(new String[]{
                                        epName,
                                        nc.getNodeId() != null ? nc.getNodeId() : "-",
                                        rem
                                });
                            }
                        }
                        if (cc.getVerified() != null) {
                            totalVerified += cc.getVerified().size();
                        }
                    }
                    if (nc.getOperationType() != null && nc.getOperationTypeReason() != null) {
                        totalOpChanges++;
                        opChangeRows.add(new String[]{
                                epName,
                                nc.getNodeId() != null ? nc.getNodeId() : "-",
                                nc.getOperationType(),
                                nc.getOperationTypeReason()
                        });
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append("# Improvement Notes — JAR Correction Run\n\n");
            sb.append("- **JAR:** ").append(jarName).append("\n");
            sb.append("- **Timestamp:** ").append(LocalDateTime.now()).append("\n");
            sb.append("- **Endpoints corrected:** ").append(totalEndpoints).append("\n\n");

            sb.append("## Summary\n");
            sb.append("| Metric | Count |\n|--------|-------|\n");
            sb.append("| Collections added (static missed) | ").append(totalAdded).append(" |\n");
            sb.append("| Collections removed (false positives) | ").append(totalRemoved).append(" |\n");
            sb.append("| Collections verified (correct) | ").append(totalVerified).append(" |\n");
            sb.append("| Operation type corrections | ").append(totalOpChanges).append(" |\n\n");

            if (!addedRows.isEmpty()) {
                sb.append("## Static Analysis Gaps (Collections Missed)\n");
                sb.append("| Endpoint | Node | Collection | Operation |\n");
                sb.append("|----------|------|------------|-----------|\n");
                for (String[] r : addedRows) {
                    sb.append("| ").append(r[0]).append(" | ").append(r[1])
                      .append(" | ").append(r[2]).append(" | ").append(r[3]).append(" |\n");
                }
                sb.append("\n");
            }

            if (!removedRows.isEmpty()) {
                sb.append("## Static Analysis False Positives (Collections Removed)\n");
                sb.append("| Endpoint | Node | Collection |\n");
                sb.append("|----------|------|------------|\n");
                for (String[] r : removedRows) {
                    sb.append("| ").append(r[0]).append(" | ").append(r[1])
                      .append(" | ").append(r[2]).append(" |\n");
                }
                sb.append("\n");
            }

            if (!opChangeRows.isEmpty()) {
                sb.append("## Operation Type Corrections\n");
                sb.append("| Endpoint | Node | Corrected To | Reason |\n");
                sb.append("|----------|------|-------------|--------|\n");
                for (String[] r : opChangeRows) {
                    sb.append("| ").append(r[0]).append(" | ").append(r[1])
                      .append(" | ").append(r[2]).append(" | ").append(r[3]).append(" |\n");
                }
                sb.append("\n");
            }

            if (totalAdded == 0 && totalRemoved == 0 && totalOpChanges == 0) {
                sb.append("No corrections needed — static analysis matched Claude verification.\n");
            }

            Files.writeString(reportFile, sb.toString());
            log.info("Generated JAR improvement notes: {}", reportFile);
        } catch (Exception e) {
            log.warn("Failed to generate improvement notes for {}: {}", jarName, e.getMessage());
        }
    }

    /**
     * Correct a single endpoint's static analysis using Claude.
     * For large trees (exceeding prompt limit), automatically chunks the tree
     * and processes each chunk separately, then merges all correction results.
     */
    public CorrectionResult correctSingleEndpoint(EndpointInfo ep, String jarName) throws Exception {
        if (ep.getCallTree() == null) return null;

        Path workDir = persistence.getWorkDir(jarName);
        Files.createDirectories(workDir);

        // Serialize tree to check size
        String treeJson;
        try {
            treeJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(ep.getCallTree());
        } catch (Exception e) {
            log.warn("Failed to serialize call tree for {}.{}", ep.getControllerSimpleName(), ep.getMethodName());
            return null;
        }

        int nodeCount = treeChunker.countNodes(ep.getCallTree());
        boolean needsChunking = treeJson.length() > TreeChunker.MAX_PROMPT_CHARS - 4000
                || nodeCount > TreeChunker.MAX_TREE_NODES;

        if (!needsChunking) {
            // Small tree — single prompt (original path)
            return correctSingleEndpointSingleShot(ep, jarName, workDir);
        }

        // Large tree — chunk and process each chunk separately
        log.info("  Large tree for {}.{}: {} chars, {} nodes -> chunked correction",
                ep.getControllerSimpleName(), ep.getMethodName(), treeJson.length(), nodeCount);

        List<TreeChunker.TreeChunk> chunks = treeChunker.chunkCallTree(ep, treeJson);
        log.info("  Split into {} chunks for correction", chunks.size());

        // Save chunk plan for debugging
        treeChunker.writeChunkPlan(workDir, ep.getMethodName() + "_corr", chunks, fragmentStore);

        // Process each chunk with a correction-specific prompt
        List<CorrectionResult> chunkResults = new ArrayList<>();
        ExecutorService chunkExecutor = Executors.newFixedThreadPool(
                Math.min(parallelChunks, chunks.size()),
                r -> { Thread t = new Thread(r, "corr-chunk-" + ep.getMethodName()); t.setDaemon(true); return t; });

        try {
            String corrEpKey = ep.getControllerSimpleName() + "." + ep.getMethodName();
            List<Future<CorrectionResult>> futures = new ArrayList<>();
            for (int i = 0; i < chunks.size(); i++) {
                final int ci = i;
                final TreeChunker.TreeChunk chunk = chunks.get(i);
                futures.add(chunkExecutor.submit(() -> {
                    String chunkPrompt = buildChunkedCorrectionPrompt(ep, chunk, ci, chunks.size());
                    List<String> cmd = List.of("claude", "-p", "--no-session-persistence");

                    // Save chunk prompt input
                    fragmentStore.writeFragmentSafe(workDir,
                            ep.getMethodName() + "_corr_chunk" + ci + "_input.txt", chunkPrompt);
                    log.info("  [PROMPT] {}.{} chunk {}/{} ({}): {} chars ({}KB)",
                            ep.getControllerSimpleName(), ep.getMethodName(),
                            ci + 1, chunks.size(), chunk.type(),
                            chunkPrompt.length(), chunkPrompt.length() / 1024);

                    long callStart = System.currentTimeMillis();
                    String rawOutput;
                    try {
                        rawOutput = processRunner.runClaudeProcess(cmd, chunkPrompt, null, null);
                        long _ccMs = System.currentTimeMillis() - callStart;
                        try { callLogger.logCall(jarName, "correction-chunk", "CORRECT", corrEpKey, ci,
                                chunkPrompt != null ? chunkPrompt.length() / 1024 : 0,
                                rawOutput != null ? rawOutput.length() / 1024 : 0,
                                _ccMs, true, null); } catch (Exception _ignored) {}
                    } catch (Exception _ccEx) {
                        long _ccMs = System.currentTimeMillis() - callStart;
                        try { callLogger.logCall(jarName, "correction-chunk", "CORRECT", corrEpKey, ci,
                                chunkPrompt != null ? chunkPrompt.length() / 1024 : 0,
                                0, _ccMs, false, _ccEx.getMessage()); } catch (Exception _ignored) {}
                        throw _ccEx;
                    }
                    long callMs = System.currentTimeMillis() - callStart;

                    // Save raw chunk output
                    fragmentStore.writeFragmentSafe(workDir,
                            ep.getMethodName() + "_corr_chunk" + ci + "_output.json",
                            rawOutput != null ? rawOutput : "");

                    if (rawOutput == null || rawOutput.isBlank()) {
                        log.warn("  [RESPONSE] {}.{} chunk {}/{} EMPTY after {}ms",
                                ep.getControllerSimpleName(), ep.getMethodName(), ci + 1, chunks.size(), callMs);
                        return null;
                    }
                    log.info("  [RESPONSE] {}.{} chunk {}/{}: {} chars ({}KB) in {}ms",
                            ep.getControllerSimpleName(), ep.getMethodName(),
                            ci + 1, chunks.size(),
                            rawOutput.length(), rawOutput.length() / 1024, callMs);
                    return parseCorrectionResult(rawOutput, ep);
                }));
            }

            for (Future<CorrectionResult> f : futures) {
                try {
                    CorrectionResult cr = f.get();
                    if (cr != null) chunkResults.add(cr);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (ExecutionException e) {
                    log.warn("Chunk correction failed: {}", e.getCause().getMessage());
                }
            }
        } finally {
            chunkExecutor.shutdownNow();
            try { chunkExecutor.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }

        if (chunkResults.isEmpty()) {
            log.warn("No chunk results for {}.{}", ep.getControllerSimpleName(), ep.getMethodName());
            return null;
        }

        // Merge all chunk results into one CorrectionResult
        CorrectionResult merged = mergeChunkResults(chunkResults, ep);
        log.info("  Merged {} chunk results -> {} corrections, {} collections for {}.{}",
                chunkResults.size(),
                merged.getCorrections() != null ? merged.getCorrections().size() : 0,
                merged.getEndpointSummary() != null && merged.getEndpointSummary().getAllCollections() != null
                        ? merged.getEndpointSummary().getAllCollections().size() : 0,
                ep.getControllerSimpleName(), ep.getMethodName());

        // Save merged result for debugging
        try {
            fragmentStore.writeFragmentSafe(workDir,
                    ep.getMethodName() + "_corr_merged.json",
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(merged));
        } catch (Exception ignored) {}

        return merged;
    }

    /**
     * Original single-shot correction for small trees.
     */
    private CorrectionResult correctSingleEndpointSingleShot(EndpointInfo ep, String jarName, Path workDir) throws Exception {
        String prompt = buildCorrectionPrompt(ep);
        List<String> cmd = List.of("claude", "-p", "--no-session-persistence");

        // Save prompt input
        fragmentStore.writeFragmentSafe(workDir,
                ep.getMethodName() + "_corr_input.txt", prompt);
        log.info("  [PROMPT] {}.{} single-shot: {} chars ({}KB)",
                ep.getControllerSimpleName(), ep.getMethodName(),
                prompt.length(), prompt.length() / 1024);

        String singleShotEpKey = ep.getControllerSimpleName() + "." + ep.getMethodName();
        long callStart = System.currentTimeMillis();
        String rawOutput;
        try {
            rawOutput = processRunner.runClaudeProcess(cmd, prompt, null, null);
            long _ssMs = System.currentTimeMillis() - callStart;
            try { callLogger.logCall(jarName, "correction-single", "CORRECT", singleShotEpKey, 0,
                    prompt != null ? prompt.length() / 1024 : 0,
                    rawOutput != null ? rawOutput.length() / 1024 : 0,
                    _ssMs, true, null); } catch (Exception _ignored) {}
        } catch (Exception _ssEx) {
            long _ssMs = System.currentTimeMillis() - callStart;
            try { callLogger.logCall(jarName, "correction-single", "CORRECT", singleShotEpKey, 0,
                    prompt != null ? prompt.length() / 1024 : 0,
                    0, _ssMs, false, _ssEx.getMessage()); } catch (Exception _ignored) {}
            throw _ssEx;
        }
        long callMs = System.currentTimeMillis() - callStart;

        if (rawOutput == null || rawOutput.isBlank()) {
            log.warn("  [RESPONSE] {}.{} EMPTY after {}ms",
                    ep.getControllerSimpleName(), ep.getMethodName(), callMs);
            return null;
        }

        log.info("  [RESPONSE] {}.{} single-shot: {} chars ({}KB) in {}ms",
                ep.getControllerSimpleName(), ep.getMethodName(),
                rawOutput.length(), rawOutput.length() / 1024, callMs);

        fragmentStore.writeFragmentSafe(workDir,
                ep.getMethodName() + "_corr_output.json", rawOutput);

        return parseCorrectionResult(rawOutput, ep);
    }

    /**
     * Merge CorrectionResults from multiple chunks into a single result.
     * Deduplicates node corrections by nodeId (last chunk wins for same node).
     * Unions all collection names for the endpoint summary.
     */
    private CorrectionResult mergeChunkResults(List<CorrectionResult> chunkResults, EndpointInfo ep) {
        CorrectionResult merged = new CorrectionResult();
        merged.setEndpointName(ep.getControllerSimpleName() + "." + ep.getMethodName());

        // Merge corrections: deduplicate by nodeId, last wins
        Map<String, NodeCorrection> correctionMap = new LinkedHashMap<>();
        for (CorrectionResult cr : chunkResults) {
            if (cr.getCorrections() != null) {
                for (NodeCorrection nc : cr.getCorrections()) {
                    if (nc.getNodeId() != null) {
                        correctionMap.put(nc.getNodeId(), nc);
                    }
                }
            }
        }
        merged.setCorrections(new ArrayList<>(correctionMap.values()));

        // Merge endpoint summaries: union all collections, take highest confidence
        Set<String> allCollections = new LinkedHashSet<>();
        Set<String> crossModuleCalls = new LinkedHashSet<>();
        String primaryOp = null;
        double maxConfidence = 0.0;

        for (CorrectionResult cr : chunkResults) {
            EndpointSummary es = cr.getEndpointSummary();
            if (es == null) continue;
            if (es.getAllCollections() != null) allCollections.addAll(es.getAllCollections());
            if (es.getCrossModuleCalls() != null) crossModuleCalls.addAll(es.getCrossModuleCalls());
            if (es.getPrimaryOperation() != null) primaryOp = es.getPrimaryOperation();
            if (es.getConfidence() > maxConfidence) maxConfidence = es.getConfidence();
        }

        EndpointSummary mergedSummary = new EndpointSummary();
        mergedSummary.setAllCollections(new ArrayList<>(allCollections));
        mergedSummary.setPrimaryOperation(primaryOp);
        mergedSummary.setCrossModuleCalls(new ArrayList<>(crossModuleCalls));
        mergedSummary.setConfidence(maxConfidence);
        merged.setEndpointSummary(mergedSummary);

        return merged;
    }

    /**
     * Build a correction prompt for a single chunk of a large call tree.
     * Each chunk gets the same rules/format but only its portion of the tree.
     */
    String buildChunkedCorrectionPrompt(EndpointInfo ep, TreeChunker.TreeChunk chunk,
                                         int chunkIndex, int totalChunks) {
        PromptTemplates.DbTechnology tech = PromptTemplates.detectTechnology(ep);
        String chunkData = chunk.context();
        if (chunkData.length() > TreeChunker.MAX_PROMPT_CHARS - 4000) {
            chunkData = chunkData.substring(0, TreeChunker.MAX_PROMPT_CHARS - 4000) + "\n... [truncated]";
        }
        return PromptTemplates.buildChunkedCorrectionPrompt(
                ep, chunkData, chunk.type(), chunk.label(), chunk.nodeCount(),
                chunkIndex, totalChunks, tech);
    }

    /**
     * Build the correction-focused prompt for Claude (single-shot for small trees).
     * Different from general analysis: specifically asks to verify/correct collections and operations.
     */
    String buildCorrectionPrompt(EndpointInfo ep) {
        PromptTemplates.DbTechnology tech = PromptTemplates.detectTechnology(ep);
        String treeJson;
        try {
            treeJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(ep.getCallTree());
            if (treeJson.length() > TreeChunker.MAX_PROMPT_CHARS - 4000) {
                treeJson = treeJson.substring(0, TreeChunker.MAX_PROMPT_CHARS - 4000) + "\n... [truncated]";
            }
        } catch (Exception e) {
            treeJson = "{ \"error\": \"Failed to serialize call tree\" }";
        }
        return PromptTemplates.buildCorrectionPrompt(ep, treeJson, tech);
    }

    /**
     * Parse Claude's correction response into a CorrectionResult.
     */
    CorrectionResult parseCorrectionResult(String rawOutput, EndpointInfo ep) {
        String json = resultMerger.extractJson(rawOutput);
        if (json == null) {
            log.warn("Could not extract JSON from Claude correction response");
            return null;
        }

        try {
            JsonNode root = objectMapper.readTree(json);
            CorrectionResult result = new CorrectionResult();
            result.setEndpointName(ep.getControllerSimpleName() + "." + ep.getMethodName());

            // Parse corrections
            List<NodeCorrection> corrections = new ArrayList<>();
            JsonNode corrs = root.get("corrections");
            if (corrs != null && corrs.isArray()) {
                for (JsonNode c : corrs) {
                    NodeCorrection nc = new NodeCorrection();
                    nc.setNodeId(textOrNull(c, "nodeId"));
                    nc.setOperationType(textOrNull(c, "operationType"));
                    nc.setOperationTypeReason(textOrNull(c, "operationTypeReason"));

                    JsonNode colls = c.get("collections");
                    if (colls != null) {
                        CollectionCorrections cc = new CollectionCorrections();
                        cc.setAdded(parseCollectionEntries(colls.get("added")));
                        cc.setRemoved(parseStringList(colls.get("removed")));
                        cc.setVerified(parseStringList(colls.get("verified")));
                        nc.setCollections(cc);
                    }
                    corrections.add(nc);
                }
            }
            result.setCorrections(corrections);

            // Parse endpoint summary
            JsonNode summary = root.get("endpointSummary");
            if (summary != null) {
                EndpointSummary es = new EndpointSummary();
                es.setAllCollections(parseStringList(summary.get("allCollections")));
                es.setPrimaryOperation(textOrNull(summary, "primaryOperation"));
                es.setCrossModuleCalls(parseStringList(summary.get("crossModuleCalls")));
                if (summary.has("confidence")) {
                    es.setConfidence(summary.get("confidence").asDouble(0.0));
                }
                result.setEndpointSummary(es);
            }

            return result;
        } catch (Exception e) {
            log.warn("Failed to parse correction JSON: {}", e.getMessage());
            return null;
        }
    }

    private List<CollectionEntry> parseCollectionEntries(JsonNode node) {
        if (node == null || !node.isArray()) return List.of();
        List<CollectionEntry> entries = new ArrayList<>();
        for (JsonNode n : node) {
            CollectionEntry ce = new CollectionEntry();
            ce.setName(textOrNull(n, "name"));
            ce.setSource(textOrNull(n, "source"));
            ce.setOperation(textOrNull(n, "operation"));
            entries.add(ce);
        }
        return entries;
    }

    private List<String> parseStringList(JsonNode node) {
        if (node == null || !node.isArray()) return List.of();
        List<String> list = new ArrayList<>();
        for (JsonNode n : node) {
            if (n.isTextual()) list.add(n.asText());
        }
        return list;
    }

    private String textOrNull(JsonNode node, String field) {
        JsonNode f = node.get(field);
        return f != null && f.isTextual() ? f.asText() : null;
    }
}
