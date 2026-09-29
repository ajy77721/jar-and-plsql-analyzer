package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.VerificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.sso.CapturedSsoContext;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Orchestrates the parallel Claude CLI verification run for a single analysis and persists
 * per-chunk fragments/checkpoints. Extracted from ClaudeVerificationService to keep files
 * focused; logic unchanged.
 */
class ClaudeVerificationRunner {

    private static final Logger log = LoggerFactory.getLogger(ClaudeVerificationRunner.class);

    private final ClaudeProcessRunner processRunner;
    private final ArtifactStore store;
    private final AtomicBoolean cancelled;
    private final ClaudeChunkBuilder chunkBuilder;
    private final ClaudePromptBuilder promptBuilder;
    private final ClaudeResponseParser parser;
    private final ClaudeResultMerger merger;
    private final ClaudeRunReporter reporter;
    private final ClaudeResultLoader loader;
    private final ClaudeFragmentWriter writer;

    ClaudeVerificationRunner(ClaudeProcessRunner processRunner, ArtifactStore store,
                             AtomicBoolean cancelled, ClaudeChunkBuilder chunkBuilder,
                             ClaudePromptBuilder promptBuilder, ClaudeResponseParser parser,
                             ClaudeResultMerger merger, ClaudeRunReporter reporter, ClaudeResultLoader loader,
                             ClaudeFragmentWriter writer) {
        this.processRunner = processRunner;
        this.store = java.util.Objects.requireNonNull(store, "ArtifactStore is required");
        this.cancelled = cancelled;
        this.chunkBuilder = chunkBuilder;
        this.promptBuilder = promptBuilder;
        this.parser = parser;
        this.merger = merger;
        this.reporter = reporter;
        this.loader = loader;
        this.writer = writer;
    }

    /**
     * Runs {@code body} on the calling (pool) thread with the captured request routing and job
     * context installed, so a chunk task's fragment/checkpoint writes land in the same bucket and
     * sign as the submitter instead of the shared default target. Both captures are captured on the
     * submitting thread; either may carry nothing (install is then a no-op), preserving today's
     * behaviour for a thread that has neither.
     */
    private static <T> T withContext(RequestScopedArtifactStore.Scope routingScope,
                                     CapturedSsoContext jobContext,
                                     Callable<T> body) throws Exception {
        try (AutoCloseable routed = routingScope.install();
             AutoCloseable ctx = jobContext == null ? null : jobContext.install()) {
            return body.call();
        }
    }

    /**
     * {@code plsql/<name>/claude-legacy/} — the sidecar folder for one analysis.
     *
     * <p>Was {@code AnalysisPaths.claudeFileKey(analysisName, "claude")} — the EXACT SAME key
     * {@code com.plsqlanalyzer.web.parser.service.ClaudeVerificationService} (a genuinely
     * different class with the same simple name, in a different package, wired to the main
     * Parser app's "Enable Claude" feature) also uses for its own chunk fragments
     * (see that class's own {@code claudeDir()}). Both stacks write identically-named files
     * ({@code <chunkId>_output.json}, {@code _result.json}, checkpoint, index) into whatever
     * this resolves to — confirmed by reading both classes side by side — so if this stack
     * (wired via ConversionController/FlowAnalysisController, see AppConfig.java) is ever run
     * against an analysis name that the parser stack also uses, their fragments silently
     * clobber each other. Renamed to a distinct subfolder so the two can never collide,
     * regardless of whether analysis-name overlap ever actually happens in practice — nothing
     * in either class enforces that it doesn't.
     */
    private ArtifactKey claudeDir(String analysisName) {
        return AnalysisPaths.claudeFileKey(analysisName, "claude-legacy");
    }

    VerificationResult verify(AnalysisResult result, Consumer<String> progressCallback, int parallelChunks) {
        cancelled.set(false);
        String analysisName = result.getName();
        ArtifactKey outputDir = claudeDir(analysisName);

        progress(progressCallback, "[1/5] Preparing verification chunks...");

        // Build chunks from the analysis
        List<VerificationChunk> chunks = chunkBuilder.buildChunks(result);
        progress(progressCallback, "[1/5] Created " + chunks.size() + " verification chunks");

        // Check for existing checkpoint (resume support)
        Set<String> completedChunks = loader.loadCheckpoint(outputDir);
        int skipped = 0;

        // Save index
        writer.saveIndex(outputDir, chunks);

        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger skippedCount = new AtomicInteger(skipped);
        int total = chunks.size();
        List<ChunkResult> chunkResults = Collections.synchronizedList(new ArrayList<>());

        // Separate already-completed from pending chunks
        List<VerificationChunk> pendingChunks = new ArrayList<>();
        for (VerificationChunk chunk : chunks) {
            if (completedChunks.contains(chunk.id)) {
                ChunkResult existing = parser.loadChunkResult(store, outputDir, chunk.id);
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
            // Caller may be a request thread (routing scope) or a job thread (SsoJobContext); the
            // pool threads have neither. Capture both before submitting, re-install per chunk task.
            final RequestScopedArtifactStore.Scope routingScope = store instanceof RequestScopedArtifactStore routed
                    ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;
            final CapturedSsoContext jobContext = CapturedSsoContext.capture(null);
            int submitted = 0;

            for (VerificationChunk chunk : pendingChunks) {
                completionService.submit(() -> withContext(routingScope, jobContext, () -> {
                    if (cancelled.get()) return ChunkResult.error(chunk.id, "Cancelled");

                    int num = processed.incrementAndGet();
                    progress(progressCallback, "[2/5] Chunk " + num + "/" + total + ": " + chunk.name);

                    // Build prompt
                    String prompt = promptBuilder.buildPrompt(chunk, result);

                    // Save input — include the full prompt so it can be reviewed
                    Map<String, Object> inputData = new LinkedHashMap<>();
                    inputData.put("chunkId", chunk.id);
                    inputData.put("name", chunk.name);
                    inputData.put("procedures", chunk.procedureIds);
                    inputData.put("tableCount", chunk.tables.size());
                    inputData.put("promptLength", prompt.length());
                    inputData.put("timestamp", LocalDateTime.now().toString());
                    inputData.put("prompt", prompt);
                    writer.saveFragment(outputDir, chunk.id + "_input.json", inputData);

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
                        writer.saveFragment(outputDir, chunk.id + "_error.txt", "No response from Claude CLI after 2 attempts");
                        log.warn("Chunk {} got no response from Claude CLI after 2 attempts", chunk.id);
                        cr = ChunkResult.error(chunk.id, "No response from Claude");
                    } else {
                        writer.saveFragment(outputDir, chunk.id + "_output.json", response);
                        cr = parser.parseResponse(chunk, response);
                        syncCompletedChunks.add(chunk.id);
                        writer.saveCheckpoint(outputDir, syncCompletedChunks);
                    }
                    return cr;
                }));
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
        VerificationResult verResult = merger.mergeResults(result, chunkResults);

        progress(progressCallback, "[4/5] Saving verification result...");

        // Save final result
        writer.saveFragment(outputDir, "_result.json", verResult);

        int confirmed = verResult.confirmedCount;
        int removed = verResult.removedCount;
        int added = verResult.newCount;
        progress(progressCallback, "[5/5] Verification complete: " + confirmed + " confirmed, "
                + removed + " removed, " + added + " new findings");

        // Auto-generate improvement/run report
        reporter.generateRunReport(verResult, chunks.size(), chunkResults);

        return verResult;
    }

    private void progress(Consumer<String> callback, String message) {
        if (callback != null) callback.accept(message);
        log.info("[Claude] {}", message);
    }
}
