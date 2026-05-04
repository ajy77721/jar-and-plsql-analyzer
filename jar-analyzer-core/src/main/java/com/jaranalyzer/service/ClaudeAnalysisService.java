package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.service.TreeChunker.TreeChunk;
import com.jaranalyzer.service.FragmentStore.FragmentIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/**
 * Integrates with Claude CLI for deep source-level analysis.
 * <p>
 * Fragment-based architecture:
 *   data/jar/{normalizedKey}/claude/
 *     _meta.json                              -- run metadata
 *     {Controller}.{method}_input.json        -- what we sent to Claude
 *     {Controller}.{method}_output.json       -- Claude's raw response
 *     {Controller}.{method}_error.txt         -- error if failed
 * <p>
 * Two modes:
 *   1. Source mode: claude -p --session-id {UUID} "/Flow-JSON-Analyzer {controller} {method}"
 *   2. Analysis mode: prompt piped via stdin to avoid Windows error 206 (32KB CLI limit)
 * <p>
 * NOT HTTP API -- uses the Claude CLI binary installed on the machine.
 */
@Service
public class ClaudeAnalysisService {

    private static final Logger log = LoggerFactory.getLogger(ClaudeAnalysisService.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ClaudeProcessRunner processRunner;
    private final TreeChunker treeChunker;
    private final SwarmClusterer swarmClusterer;
    private final EndpointOutputWriter endpointOutputWriter;
    private final ClaudeResultMerger resultMerger;
    private final FragmentStore fragmentStore;
    private final ClaudeEnrichmentTracker tracker;
    private final ClaudeCallLogger callLogger;

    @Value("${claude.analysis.max-endpoints:10}")
    private int maxEndpoints;

    @Value("${claude.analysis.parallel-chunks:4}")
    private int parallelChunks;

    public ClaudeAnalysisService(ClaudeProcessRunner processRunner,
                                 TreeChunker treeChunker,
                                 SwarmClusterer swarmClusterer,
                                 EndpointOutputWriter endpointOutputWriter,
                                 ClaudeResultMerger resultMerger,
                                 FragmentStore fragmentStore,
                                 ClaudeEnrichmentTracker tracker,
                                 ClaudeCallLogger callLogger) {
        this.processRunner = processRunner;
        this.treeChunker = treeChunker;
        this.swarmClusterer = swarmClusterer;
        this.endpointOutputWriter = endpointOutputWriter;
        this.resultMerger = resultMerger;
        this.fragmentStore = fragmentStore;
        this.tracker = tracker;
        this.callLogger = callLogger;
    }

    public int getMaxEndpoints() { return maxEndpoints; }

    /**
     * Check if Claude CLI is available on the machine.
     */
    public boolean isConfigured() {
        return processRunner.isConfigured();
    }

    /**
     * Integration test for the stdin pipe approach.
     */
    public Map<String, Object> runStdinPipeTest() {
        return processRunner.runStdinPipeTest(resultMerger);
    }

    /**
     * Get the fragment work directory for a JAR's Claude analysis.
     */
    public Path getWorkDir(String jarName) {
        return fragmentStore.getWorkDir(jarName);
    }

    /**
     * List fragment files for a JAR's Claude analysis.
     */
    public List<Map<String, Object>> listFragments(String jarName) {
        return fragmentStore.listFragments(jarName);
    }

    /**
     * Also generate per-endpoint output for static-only analysis (no Claude).
     * Called from controller after static analysis completes.
     */
    public void writeEndpointOutputs(String jarName, List<EndpointInfo> endpoints) {
        endpointOutputWriter.writeEndpointOutputs(jarName, endpoints);
    }

    /**
     * List per-endpoint output folders for a JAR.
     */
    public List<Map<String, Object>> listEndpointOutputs(String jarName) {
        return endpointOutputWriter.listEndpointOutputs(jarName);
    }

    String buildAnalysisContext(EndpointInfo ep) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of(
                    "controller", ep.getControllerSimpleName(),
                    "method", ep.getMethodName(),
                    "httpMethod", ep.getHttpMethod() != null ? ep.getHttpMethod() : "",
                    "path", ep.getFullPath() != null ? ep.getFullPath() : "",
                    "callTree", ep.getCallTree() != null ? ep.getCallTree() : Map.of()
            ));
        } catch (Exception e) {
            return ep.getControllerSimpleName() + "." + ep.getMethodName();
        }
    }

    /**
     * Apply the max-endpoints config limit.
     * -1 or negative = all endpoints. Positive = limit to that number.
     */
    List<EndpointInfo> applyEndpointLimit(List<EndpointInfo> endpoints,
                                          ProgressService progressService) {
        if (maxEndpoints < 0 || maxEndpoints >= endpoints.size()) {
            log.info("Endpoint limit: ALL ({} endpoints, config={})", endpoints.size(), maxEndpoints);
            return endpoints;
        }
        log.info("Endpoint limit: {} of {} (config={})", maxEndpoints, endpoints.size(), maxEndpoints);
        progressService.detail("Endpoint limit: processing " + maxEndpoints + " of " + endpoints.size()
                + " (set claude.analysis.max-endpoints=-1 for all)");
        return endpoints.subList(0, maxEndpoints);
    }

    /**
     * Enrich endpoints with Claude CLI using actual source project.
     * Prompt is short (skill command), so CLI args are safe -- no stdin piping needed.
     * Fragments stored in data/jar/{normalizedKey}/claude/ for visibility.
     */
    public void enrichEndpoints(List<EndpointInfo> endpoints, String projectPath,
                                String jarName, ProgressService progressService) {
        Path projectDir = Path.of(projectPath);
        if (!Files.isDirectory(projectDir)) {
            throw new IllegalArgumentException("Project path does not exist: " + projectPath);
        }

        if (!isConfigured()) {
            throw new IllegalStateException("Claude CLI is not installed or not in PATH. Install it with: npm install -g @anthropic-ai/claude-code");
        }

        List<EndpointInfo> targetEndpoints = applyEndpointLimit(endpoints, progressService);

        Path workDir = fragmentStore.getWorkDir(jarName);
        try { Files.createDirectories(workDir); } catch (IOException e) { throw new UncheckedIOException(e); }

        log.info("Starting Claude enrichment for {} endpoints (of {} total) in {}",
                targetEndpoints.size(), endpoints.size(), projectPath);
        log.info("Fragment directory: {}", workDir.toAbsolutePath());

        List<List<EndpointInfo>> clusters = swarmClusterer.clusterEndpoints(targetEndpoints);
        log.info("Swarm clustering: {} endpoints -> {} clusters", targetEndpoints.size(), clusters.size());
        progressService.detail("Clustered " + targetEndpoints.size() + " endpoints into " + clusters.size() + " groups");
        swarmClusterer.writeClusterPlan(workDir, clusters, fragmentStore);

        int done = 0;
        int total = targetEndpoints.size();

        for (int ci = 0; ci < clusters.size(); ci++) {
            List<EndpointInfo> cluster = clusters.get(ci);
            String sessionId = UUID.randomUUID().toString();

            String clusterLabel = "Cluster " + (ci + 1) + "/" + clusters.size()
                    + " (" + cluster.size() + " endpoints)";
            log.info("-- {} --", clusterLabel);
            progressService.step(clusterLabel);

            for (EndpointInfo ep : cluster) {
                done++;
                String controller = ep.getControllerSimpleName();
                String method = ep.getMethodName();
                String epKey = controller + "." + method;
                String fullClass = ep.getControllerClass() != null ? ep.getControllerClass() : controller;
                String fragmentBase = fragmentStore.sanitizeFragment(fullClass + "." + method);
                if (fragmentBase.length() > 80) fragmentBase = fragmentBase.substring(0, 80);
                String fragmentName = String.format("%03d_%s", done, fragmentBase);

                tracker.markProcessing(jarName, epKey);
                progressService.detail("Claude enriching [" + done + "/" + total + "]: " + fullClass + "." + method);
                log.info("  Enriching {}.{} ({}/{}) [{}]", controller, method, done, total, fullClass);

                try {
                    String fullContext = buildAnalysisContext(ep);
                    List<TreeChunk> chunks = treeChunker.chunkCallTree(ep, fullContext);
                    fragmentStore.writeFragment(workDir, fragmentName + "_input.json", fullContext);

                    if (chunks.size() == 1) {
                        List<String> command = List.of(
                                "claude", "-p", "--session-id", sessionId,
                                "--no-session-persistence",
                                "/Flow-JSON-Analyzer " + controller + " " + method
                        );
                        long _callStart = System.currentTimeMillis();
                        String result;
                        try {
                            result = processRunner.runClaudeProcess(command, null, projectDir, progressService);
                            long _callMs = System.currentTimeMillis() - _callStart;
                            try { callLogger.logCall(jarName, sessionId, "SOURCE_ENRICH", epKey, 0,
                                    0, result != null ? result.length() / 1024 : 0,
                                    _callMs, true, null); } catch (Exception _ignored) {}
                        } catch (Exception _ex) {
                            long _callMs = System.currentTimeMillis() - _callStart;
                            try { callLogger.logCall(jarName, sessionId, "SOURCE_ENRICH", epKey, 0,
                                    0, 0, _callMs, false, _ex.getMessage()); } catch (Exception _ignored) {}
                            throw _ex;
                        }

                        if (result != null && !result.isBlank()) {
                            fragmentStore.writeFragment(workDir, fragmentName + "_output.json", result);
                            resultMerger.mergeClaudeResult(ep, result);
                        }
                    } else {
                        log.info("    Large tree ({} chunks) for {}.{}", chunks.size(), controller, method);
                        progressService.detail("Large tree -> " + chunks.size() + " chunks (" + parallelChunks + " parallel)");
                        treeChunker.writeChunkPlan(workDir, fragmentName, chunks, fragmentStore);

                        List<String> branchOutputs = Collections.synchronizedList(new ArrayList<>());

                        // Skeleton first
                        {
                            TreeChunk sk = chunks.get(0);
                            String chunkFragName = fragmentName + "_chunk_0_skeleton";
                            String branchHint = "Analyzing skeleton of " + controller + "." + method + ".\n\n"
                                    + "/Flow-JSON-Analyzer " + controller + " " + method
                                    + "\n\nSkeleton:\n" + sk.context();
                            String skSessionId = UUID.randomUUID().toString();
                            List<String> cmd = List.of("claude", "-p", "--session-id", skSessionId, "--no-session-persistence");
                            long _skCallStart = System.currentTimeMillis();
                            String result;
                            try {
                                result = processRunner.runClaudeProcess(cmd, branchHint, projectDir, progressService);
                                long _skCallMs = System.currentTimeMillis() - _skCallStart;
                                try { callLogger.logCall(jarName, skSessionId, "SOURCE_ENRICH", epKey, 0,
                                        branchHint != null ? branchHint.length() / 1024 : 0,
                                        result != null ? result.length() / 1024 : 0,
                                        _skCallMs, true, null); } catch (Exception _ignored) {}
                            } catch (Exception _ex) {
                                long _skCallMs = System.currentTimeMillis() - _skCallStart;
                                try { callLogger.logCall(jarName, skSessionId, "SOURCE_ENRICH", epKey, 0,
                                        branchHint != null ? branchHint.length() / 1024 : 0,
                                        0, _skCallMs, false, _ex.getMessage()); } catch (Exception _ignored) {}
                                throw _ex;
                            }
                            if (result != null && !result.isBlank()) {
                                fragmentStore.writeFragment(workDir, chunkFragName + "_output.json", result);
                                branchOutputs.add(result);
                            }
                        }

                        // Parallel branches
                        List<TreeChunk> branchList = chunks.subList(1, chunks.size());
                        if (!branchList.isEmpty()) {
                            ExecutorService executor = Executors.newFixedThreadPool(
                                    Math.min(parallelChunks, branchList.size()));
                            int[] chunkDone = {1};
                            int totalChunks = chunks.size();
                            String fragBase = fragmentName;
                            String ctrl = controller;
                            String mth = method;
                            String epKeyForLambda = epKey;

                            List<Future<?>> futures = new ArrayList<>();
                            for (int chi = 0; chi < branchList.size(); chi++) {
                                final int idx = chi;
                                final int globalIdx = chi + 1;
                                futures.add(executor.submit(() -> {
                                    TreeChunk chunk = branchList.get(idx);
                                    String chunkFragName = fragBase + "_chunk_" + globalIdx;
                                    try {
                                        String branchHint = "Analyzing branch " + (globalIdx + 1) + "/" + totalChunks
                                                + " of " + ctrl + "." + mth + ". Focus on: " + chunk.label() + "\n\n"
                                                + "/Flow-JSON-Analyzer " + ctrl + "." + mth
                                                + "\n\nBranch context:\n" + chunk.context();
                                        String chunkSessionId = UUID.randomUUID().toString();
                                        List<String> cmd = List.of("claude", "-p",
                                                "--session-id", chunkSessionId, "--no-session-persistence");
                                        long _brCallStart = System.currentTimeMillis();
                                        String result;
                                        try {
                                            result = processRunner.runClaudeProcess(cmd, branchHint, projectDir, progressService);
                                            long _brCallMs = System.currentTimeMillis() - _brCallStart;
                                            try { callLogger.logCall(jarName, chunkSessionId, "SOURCE_ENRICH", epKeyForLambda, globalIdx,
                                                    branchHint != null ? branchHint.length() / 1024 : 0,
                                                    result != null ? result.length() / 1024 : 0,
                                                    _brCallMs, true, null); } catch (Exception _ignored) {}
                                        } catch (Exception _brEx) {
                                            long _brCallMs = System.currentTimeMillis() - _brCallStart;
                                            try { callLogger.logCall(jarName, chunkSessionId, "SOURCE_ENRICH", epKeyForLambda, globalIdx,
                                                    branchHint != null ? branchHint.length() / 1024 : 0,
                                                    0, _brCallMs, false, _brEx.getMessage()); } catch (Exception _ignored) {}
                                            throw _brEx;
                                        }
                                        if (result != null && !result.isBlank()) {
                                            fragmentStore.writeFragment(workDir, chunkFragName + "_output.json", result);
                                            branchOutputs.add(result);
                                        }
                                        synchronized (chunkDone) {
                                            chunkDone[0]++;
                                            progressService.detail("  Chunk " + chunkDone[0] + "/" + totalChunks + " done");
                                        }
                                    } catch (Exception e) {
                                        log.warn("    Chunk {} failed: {}", chunk.label(), e.getMessage());
                                        fragmentStore.writeFragmentSafe(workDir, chunkFragName + "_error.txt", e.getMessage());
                                    }
                                }));
                            }
                            executor.shutdown();
                            try {
                                long awaitSec = Math.max(processRunner.getTimeoutSeconds(),
                                        processRunner.getTimeoutSeconds() * branchList.size() / parallelChunks + 300);
                                if (!executor.awaitTermination(awaitSec, TimeUnit.SECONDS)) {
                                    log.warn("    Executor timed out, forcing shutdown");
                                    executor.shutdownNow();
                                }
                            } catch (InterruptedException ie) {
                                executor.shutdownNow();
                                Thread.currentThread().interrupt();
                            }
                        }

                        for (String output : branchOutputs) {
                            resultMerger.mergeClaudeResult(ep, output);
                        }
                        log.info("    Assembled {} branch outputs for {}.{}", branchOutputs.size(), controller, method);
                    }

                    endpointOutputWriter.writeEndpointOutput(jarName, ep, done);
                    tracker.markEndpointComplete(jarName, epKey);

                } catch (Exception e) {
                    log.warn("  Claude failed for {}.{}: {}", controller, method, e.getMessage());
                    fragmentStore.writeFragmentSafe(workDir, fragmentName + "_error.txt", e.getMessage());
                    tracker.markEndpointError(jarName, epKey, e.getMessage());
                }
            }
        }

        fragmentStore.writeMetaSafe(workDir, jarName, total, done, "source");
        log.info("Claude enrichment complete: {}/{} endpoints processed", done, total);
    }

    /**
     * Enrich endpoints from static analysis data (no source path needed).
     * Uses stdin piping to avoid Windows error 206 (command line too long).
     * Endpoints are clustered by shared dependencies (swarm clustering) so each
     * Claude session gets coherent context -- related endpoints analyzed together.
     */
    public void enrichFromAnalysis(List<EndpointInfo> endpoints, String jarName,
                                    ProgressService progressService) {
        enrichFromAnalysis(endpoints, jarName, progressService, false);
    }

    /**
     * Enrich endpoints from static analysis data.
     * @param resume if true, skip endpoints that already have a checkpoint (completed previously)
     */
    public void enrichFromAnalysis(List<EndpointInfo> endpoints, String jarName,
                                    ProgressService progressService, boolean resume) {
        if (!isConfigured()) {
            throw new IllegalStateException("Claude CLI is not installed or not in PATH");
        }

        List<EndpointInfo> targetEndpoints = applyEndpointLimit(endpoints, progressService);

        Path workDir = fragmentStore.getWorkDir(jarName);
        try { Files.createDirectories(workDir); } catch (IOException e) { throw new UncheckedIOException(e); }

        // Load checkpoint if resuming — skip already-completed endpoints
        Set<String> completedKeys = new HashSet<>();
        Path checkpointFile = workDir.resolve("_checkpoint.json");
        if (resume && Files.exists(checkpointFile)) {
            try {
                List<?> saved = objectMapper.readValue(checkpointFile.toFile(), List.class);
                saved.forEach(k -> completedKeys.add(String.valueOf(k)));
                log.info("Resuming: {} endpoints already completed", completedKeys.size());
                progressService.detail("Resuming — " + completedKeys.size() + " endpoints already done");
            } catch (Exception e) {
                log.warn("Could not read checkpoint: {}", e.getMessage());
            }
        }

        log.info("Starting Claude enrichment from static analysis for {} endpoints (of {} total)",
                targetEndpoints.size(), endpoints.size());
        log.info("Fragment directory: {}", workDir.toAbsolutePath());

        List<List<EndpointInfo>> clusters = swarmClusterer.clusterEndpoints(targetEndpoints);
        log.info("Swarm clustering: {} endpoints -> {} clusters", targetEndpoints.size(), clusters.size());
        progressService.detail("Clustered " + targetEndpoints.size() + " endpoints into " + clusters.size() + " groups");

        swarmClusterer.writeClusterPlan(workDir, clusters, fragmentStore);

        FragmentIndex index = fragmentStore.createIndex();

        int done = 0;
        int total = targetEndpoints.size();

        for (int ci = 0; ci < clusters.size(); ci++) {
            List<EndpointInfo> cluster = clusters.get(ci);
            String sessionId = UUID.randomUUID().toString();

            String clusterLabel = "Cluster " + (ci + 1) + "/" + clusters.size()
                    + " (" + cluster.size() + " endpoints)";
            log.info("-- {} --", clusterLabel);
            progressService.step(clusterLabel);

            String clusterContext = swarmClusterer.buildClusterContext(cluster);
            fragmentStore.writeFragmentSafe(workDir, "_cluster_" + (ci + 1) + "_context.json", clusterContext);

            for (EndpointInfo ep : cluster) {
                done++;
                String controller = ep.getControllerSimpleName();
                String method = ep.getMethodName();
                String epKey = controller + "." + method;
                String fullClass = ep.getControllerClass() != null ? ep.getControllerClass() : controller;

                // Skip if already completed (resume mode)
                if (completedKeys.contains(epKey)) {
                    tracker.markEndpointComplete(jarName, epKey);
                    log.info("  Skipping {} (already done) [{}/{}]", epKey, done, total);
                    progressService.detail("Skipping [" + done + "/" + total + "] " + epKey + " (already done)");
                    continue;
                }

                String epTraceId = index.nextEndpointId();

                tracker.markProcessing(jarName, epKey);
                progressService.detail("Claude enriching [" + done + "/" + total + "] " + epTraceId + ": " + fullClass + "." + method);
                log.info("  Enriching {} = {}.{} ({}/{}) [{}]", epTraceId, controller, method, done, total, fullClass);

                try {
                    String fullContext = buildAnalysisContext(ep);
                    List<TreeChunk> chunks = treeChunker.chunkCallTree(ep, fullContext);

                    Map<String, Object> epDetails = Map.of(
                            "endpoint", fullClass + "." + method,
                            "controller", controller,
                            "method", method,
                            "fullClass", fullClass,
                            "httpMethod", ep.getHttpMethod() != null ? ep.getHttpMethod() : "",
                            "path", ep.getFullPath() != null ? ep.getFullPath() : "",
                            "cluster", ci + 1,
                            "totalChunks", chunks.size()
                    );

                    if (chunks.size() == 1) {
                        String context = chunks.get(0).context();
                        String inputFile = epTraceId + "_input.json";
                        fragmentStore.writeFragment(workDir, inputFile, context);
                        index.register(epTraceId, inputFile, epDetails);

                        PromptTemplates.DbTechnology tech = PromptTemplates.detectTechnology(ep);
                        String prompt = PromptTemplates.buildAnalysisPrompt(clusterContext, context, tech);

                        List<String> command = List.of(
                                "claude", "-p", "--session-id", sessionId, "--no-session-persistence");
                        long _callStart = System.currentTimeMillis();
                        String result;
                        try {
                            result = processRunner.runClaudeProcess(command, prompt, null, progressService);
                            long _callMs = System.currentTimeMillis() - _callStart;
                            try { callLogger.logCall(jarName, sessionId, "ENRICH", epKey, 0,
                                    prompt != null ? prompt.length() / 1024 : 0,
                                    result != null ? result.length() / 1024 : 0,
                                    _callMs, true, null); } catch (Exception _ignored) {}
                        } catch (Exception _ex) {
                            long _callMs = System.currentTimeMillis() - _callStart;
                            try { callLogger.logCall(jarName, sessionId, "ENRICH", epKey, 0,
                                    prompt != null ? prompt.length() / 1024 : 0,
                                    0, _callMs, false, _ex.getMessage()); } catch (Exception _ignored) {}
                            throw _ex;
                        }

                        if (result != null && !result.isBlank()) {
                            String outputFile = epTraceId + "_output.json";
                            fragmentStore.writeFragment(workDir, outputFile, result);
                            index.register(epTraceId + "_out", outputFile, epDetails);
                            resultMerger.mergeClaudeResult(ep, result);
                        }
                    } else {
                        log.info("    Large call tree: {} chunks for {}.{}", chunks.size(), controller, method);
                        progressService.detail("Large tree -> " + chunks.size() + " chunks (" + parallelChunks + " parallel)");

                        treeChunker.writeChunkPlan(workDir, epTraceId, chunks, fragmentStore);

                        List<String> branchOutputs = Collections.synchronizedList(new ArrayList<>());
                        TreeChunk skeletonChunk = chunks.get(0);
                        {
                            String skTraceId = index.chunkId(epTraceId, 0);
                            String skInputFile = skTraceId + "_input.json";
                            fragmentStore.writeFragment(workDir, skInputFile, skeletonChunk.context());
                            index.register(skTraceId, skInputFile, Map.of(
                                    "endpoint", fullClass + "." + method,
                                    "chunkIndex", 0, "chunkType", "skeleton",
                                    "chunkLabel", "overview", "nodes", skeletonChunk.nodeCount()));

                            progressService.detail("  Chunk 1/" + chunks.size() + ": skeleton overview");
                            String skeletonPrompt = treeChunker.buildChunkPrompt(ep, skeletonChunk, 0, chunks.size(), clusterContext);
                            String skSessionId = UUID.randomUUID().toString();
                            List<String> cmd = List.of("claude", "-p", "--session-id", skSessionId, "--no-session-persistence");
                            long _skCallStart = System.currentTimeMillis();
                            String result;
                            try {
                                result = processRunner.runClaudeProcess(cmd, skeletonPrompt, null, progressService);
                                long _skCallMs = System.currentTimeMillis() - _skCallStart;
                                try { callLogger.logCall(jarName, skSessionId, "ENRICH", epKey, 0,
                                        skeletonPrompt != null ? skeletonPrompt.length() / 1024 : 0,
                                        result != null ? result.length() / 1024 : 0,
                                        _skCallMs, true, null); } catch (Exception _ignored) {}
                            } catch (Exception _ex) {
                                long _skCallMs = System.currentTimeMillis() - _skCallStart;
                                try { callLogger.logCall(jarName, skSessionId, "ENRICH", epKey, 0,
                                        skeletonPrompt != null ? skeletonPrompt.length() / 1024 : 0,
                                        0, _skCallMs, false, _ex.getMessage()); } catch (Exception _ignored) {}
                                throw _ex;
                            }
                            if (result != null && !result.isBlank()) {
                                String skOutputFile = skTraceId + "_output.json";
                                fragmentStore.writeFragment(workDir, skOutputFile, result);
                                index.register(skTraceId + "_out", skOutputFile, Map.of(
                                        "endpoint", fullClass + "." + method,
                                        "chunkIndex", 0, "chunkType", "skeleton"));
                                branchOutputs.add(result);
                            }
                        }

                        List<TreeChunk> branchChunks = chunks.subList(1, chunks.size());
                        if (!branchChunks.isEmpty()) {
                            ExecutorService executor = Executors.newFixedThreadPool(
                                    Math.min(parallelChunks, branchChunks.size()));
                            int[] chunkDone = {1};
                            int totalChunks = chunks.size();
                            String clCtx = clusterContext;
                            String epKeyForLambda = epKey;

                            List<Future<?>> futures = new ArrayList<>();
                            for (int chi = 0; chi < branchChunks.size(); chi++) {
                                final int idx = chi;
                                final int globalIdx = chi + 1;
                                futures.add(executor.submit(() -> {
                                    TreeChunk chunk = branchChunks.get(idx);
                                    String chunkLabel = chunk.label();
                                    if (chunkLabel.length() > 80) chunkLabel = chunkLabel.substring(0, 80) + "...";
                                    String cTraceId = index.chunkId(epTraceId, globalIdx);
                                    try {
                                        String cInputFile = cTraceId + "_input.json";
                                        fragmentStore.writeFragment(workDir, cInputFile, chunk.context());
                                        index.register(cTraceId, cInputFile, Map.of(
                                                "endpoint", fullClass + "." + method,
                                                "chunkIndex", globalIdx, "chunkType", chunk.type(),
                                                "chunkLabel", chunkLabel, "nodes", chunk.nodeCount()));

                                        String chunkPrompt = treeChunker.buildChunkPrompt(ep, chunk, globalIdx, totalChunks, clCtx);
                                        String chunkSessionId = UUID.randomUUID().toString();
                                        List<String> cmd = List.of("claude", "-p",
                                                "--session-id", chunkSessionId, "--no-session-persistence");
                                        long _chCallStart = System.currentTimeMillis();
                                        String result;
                                        try {
                                            result = processRunner.runClaudeProcess(cmd, chunkPrompt, null, progressService);
                                            long _chCallMs = System.currentTimeMillis() - _chCallStart;
                                            try { callLogger.logCall(jarName, chunkSessionId, "ENRICH", epKeyForLambda, globalIdx,
                                                    chunkPrompt != null ? chunkPrompt.length() / 1024 : 0,
                                                    result != null ? result.length() / 1024 : 0,
                                                    _chCallMs, true, null); } catch (Exception _ignored) {}
                                        } catch (Exception _chEx) {
                                            long _chCallMs = System.currentTimeMillis() - _chCallStart;
                                            try { callLogger.logCall(jarName, chunkSessionId, "ENRICH", epKeyForLambda, globalIdx,
                                                    chunkPrompt != null ? chunkPrompt.length() / 1024 : 0,
                                                    0, _chCallMs, false, _chEx.getMessage()); } catch (Exception _ignored) {}
                                            throw _chEx;
                                        }

                                        if (result != null && !result.isBlank()) {
                                            String cOutputFile = cTraceId + "_output.json";
                                            fragmentStore.writeFragment(workDir, cOutputFile, result);
                                            index.register(cTraceId + "_out", cOutputFile, Map.of(
                                                    "endpoint", fullClass + "." + method,
                                                    "chunkIndex", globalIdx, "chunkType", chunk.type()));
                                            branchOutputs.add(result);
                                        }

                                        synchronized (chunkDone) {
                                            chunkDone[0]++;
                                            progressService.detail("  Chunk " + chunkDone[0] + "/" + totalChunks
                                                    + " done [" + cTraceId + "]: " + chunkLabel);
                                        }
                                    } catch (Exception e) {
                                        log.warn("    Chunk {} [{}] failed: {}", chunkLabel, cTraceId, e.getMessage());
                                        fragmentStore.writeFragmentSafe(workDir, cTraceId + "_error.txt", e.getMessage());
                                    }
                                }));
                            }

                            executor.shutdown();
                            try {
                                long awaitSec = Math.max(processRunner.getTimeoutSeconds(),
                                        processRunner.getTimeoutSeconds() * branchChunks.size() / parallelChunks + 300);
                                if (!executor.awaitTermination(awaitSec, TimeUnit.SECONDS)) {
                                    log.warn("    Chunk processing timed out, forcing shutdown");
                                    executor.shutdownNow();
                                }
                            } catch (InterruptedException ie) {
                                log.warn("    Chunk processing interrupted");
                                executor.shutdownNow();
                                Thread.currentThread().interrupt();
                            }
                        }

                        for (String output : branchOutputs) {
                            resultMerger.mergeClaudeResult(ep, output);
                        }

                        String assembledFile = epTraceId + "_assembled.json";
                        fragmentStore.writeFragment(workDir, assembledFile,
                                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of(
                                        "endpoint", fullClass + "." + method,
                                        "traceId", epTraceId,
                                        "totalChunks", chunks.size(),
                                        "processedChunks", branchOutputs.size()
                                )));
                        index.register(epTraceId + "_asm", assembledFile, epDetails);
                        log.info("    Assembled {} branch outputs for {} = {}.{}", branchOutputs.size(), epTraceId, controller, method);
                    }

                    endpointOutputWriter.writeEndpointOutput(jarName, ep, done);
                    tracker.markEndpointComplete(jarName, epKey);
                    completedKeys.add(epKey);

                    // Checkpoint: save completed keys so enrichment can resume after interruption
                    writeCheckpoint(checkpointFile, completedKeys);

                    // Memory optimization: clear source code from call tree after processing
                    // (source is already saved in endpoint output files)
                    clearSourceFromCallTree(ep);

                } catch (Exception e) {
                    log.warn("  Claude failed for {} = {}.{}: {}", epTraceId, controller, method, e.getMessage());
                    fragmentStore.writeFragmentSafe(workDir, epTraceId + "_error.txt", e.getMessage());
                    tracker.markEndpointError(jarName, epKey, e.getMessage());
                }

                fragmentStore.writeIndex(workDir, index);
            }
        }

        fragmentStore.writeMetaSafe(workDir, jarName, total, done, "analysis");
        log.info("Claude enrichment (from analysis) complete: {}/{} endpoints", done, total);
    }

    /**
     * Write a checkpoint file with the set of completed endpoint keys.
     * This allows enrichment to resume after interruption without re-processing.
     */
    private void writeCheckpoint(Path checkpointFile, Set<String> completedKeys) {
        try {
            objectMapper.writeValue(checkpointFile.toFile(), new ArrayList<>(completedKeys));
        } catch (Exception e) {
            log.warn("Failed to write checkpoint: {}", e.getMessage());
        }
    }

    /**
     * Clear all transient/heavy data from a processed endpoint's call tree.
     * After processing, source is saved in fragment/endpoint output files on disk,
     * so keeping it in memory wastes RAM during long enrichment runs.
     */
    private void clearSourceFromCallTree(EndpointInfo ep) {
        if (ep.getCallTree() != null) {
            ep.getCallTree().clearTransient();
        }
    }
}
