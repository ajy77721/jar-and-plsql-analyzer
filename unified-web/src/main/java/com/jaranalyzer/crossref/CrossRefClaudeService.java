package com.jaranalyzer.crossref;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jaranalyzer.crossref.model.CrossRefProcLink;
import com.jaranalyzer.crossref.model.CrossRefResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Claude enrichment for cross-reference proc links.
 *
 * Design:
 * - One Claude call per proc link (one Java method → one Oracle proc)
 * - chunkId = stable hash(javaKey + oracleProcRef + plsqlSessionId) — skip if cached
 * - checkpoint.json persists completed chunkIds for resumable runs
 * - Results are written back into result.json in-place
 * - SSE stream reports progress to the UI
 */
@Service
public class CrossRefClaudeService {

    private static final Logger log = LoggerFactory.getLogger(CrossRefClaudeService.class);
    private static final String LOG_PREFIX = "[CrossRef-Claude]";

    @Value("${claude.crossref.parallel-chunks:3}")
    private int parallelChunks;

    @Value("${claude.crossref.max-source-lines:150}")
    private int maxSourceLines;

    private final CrossRefStore store;
    private final ObjectMapper objectMapper;
    /** JAR-side crossref run data (chunks/checkpoint/meta under {@code jar/crossref/...}). */
    private final ArtifactStore artifacts;
    /**
     * PL/SQL-side session reads ({@code PLSQL_PARSE_ROOT}-rooted keys) — this service also reads
     * Oracle proc/table detail out of a PL/SQL parser session (see {@link #plsqlSessionKey}), so
     * like {@link CrossRefService} it genuinely needs both {@code ArtifactStore} beans.
     */
    private final ArtifactStore parserArtifacts;
    private final com.jaranalyzer.service.JarDataPaths jarDataPaths;

    /**
     * Root of the parser output subtree, as a key. Relative to the data root, so it names the
     * {@code plsql-parse} segment explicitly — see
     * {@link com.plsqlanalyzer.web.parser.service.AnalysisPaths}.
     */
    private static final ArtifactKey PLSQL_PARSE_ROOT =
            ArtifactKey.of(com.plsqlanalyzer.web.parser.service.AnalysisPaths.PARSE_ROOT);

    // Per crossRefId → running executor (for cancel support)
    private final Map<String, ExecutorService> runningExecutors = new ConcurrentHashMap<>();
    private final Map<String, AtomicBoolean> cancelFlags = new ConcurrentHashMap<>();
    // Per crossRefId lock — serialize result.json writes from parallel chunk threads
    private final ConcurrentHashMap<String, Object> resultWriteLocks = new ConcurrentHashMap<>();

    public CrossRefClaudeService(CrossRefStore store, ObjectMapper objectMapper,
                                 @Qualifier("jarArtifactStore") ArtifactStore artifacts,
                                 @Qualifier("parserArtifactStore") ArtifactStore parserArtifacts,
                                 com.jaranalyzer.service.JarDataPaths jarDataPaths) {
        this.store = store;
        this.objectMapper = objectMapper;
        this.artifacts = artifacts;
        this.parserArtifacts = parserArtifacts;
        this.jarDataPaths = jarDataPaths;
    }

    private InputStream openRead(ArtifactKey key) throws IOException {
        return artifacts.read(key);
    }

    /** As {@link #openRead(ArtifactKey)}, against an explicitly chosen store — used for the
     *  PL/SQL-side ({@code parserArtifacts}) reads below. */
    private InputStream openRead(ArtifactStore fromStore, ArtifactKey key) throws IOException {
        return fromStore.read(key);
    }

    private OutputStream openWrite(ArtifactKey key, long contentLength) throws IOException {
        return artifacts.write(key, contentLength);
    }

    private JsonNode readTree(ArtifactKey key) throws IOException {
        try (InputStream in = openRead(key)) {
            return objectMapper.readTree(in);
        }
    }

    /** As {@link #readTree(ArtifactKey)}, against an explicitly chosen store. */
    private JsonNode readTree(ArtifactStore fromStore, ArtifactKey key) throws IOException {
        try (InputStream in = openRead(fromStore, key)) {
            return objectMapper.readTree(in);
        }
    }

    private String readString(ArtifactKey key) throws IOException {
        try (InputStream in = openRead(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** As {@link #readString(ArtifactKey)}, against an explicitly chosen store. */
    private String readString(ArtifactStore fromStore, ArtifactKey key) throws IOException {
        try (InputStream in = openRead(fromStore, key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private List<ArtifactKey> childDirs(ArtifactKey prefix) throws IOException {
        try (var s = artifacts.list(prefix, false)) {
            return s.filter(com.analyzer.storage.ArtifactEntry::directory)
                    .map(com.analyzer.storage.ArtifactEntry::key)
                    .collect(Collectors.toList());
        }
    }

    private List<ArtifactKey> childFiles(ArtifactKey prefix) throws IOException {
        return childFiles(artifacts, prefix);
    }

    /** As {@link #childFiles(ArtifactKey)}, against an explicitly chosen store. */
    private List<ArtifactKey> childFiles(ArtifactStore fromStore, ArtifactKey prefix) throws IOException {
        try (var s = fromStore.list(prefix, false)) {
            return s.filter(e -> !e.directory())
                    .map(com.analyzer.storage.ArtifactEntry::key)
                    .collect(Collectors.toList());
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Start full Claude enrichment: procLinks + javaOnlyProcs + oracleOnlyProcs + overall verdict.
     * Streams progress via SSE. Skips already-completed chunks (checkpoint.json).
     */
    public SseEmitter runEnrichment(String jarId, String crossRefId) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        CrossRefResult result = store.readResult(store.runDir(jarId, crossRefId));
        if (result == null) {
            try { emitter.send(SseEmitter.event().name("error").data("{\"error\":\"Result not found\"}")); }
            catch (Exception ignored) {}
            emitter.complete();
            return emitter;
        }

        cancelExisting(crossRefId);
        AtomicBoolean cancel = new AtomicBoolean(false);
        cancelFlags.put(crossRefId, cancel);
        resultWriteLocks.putIfAbsent(crossRefId, new Object());

        // No createDirectories: the store makes the container as part of the first write.
        ArtifactKey claudeDir = claudeDirKey(jarId, crossRefId);

        Set<String> checkpoint = loadCheckpoint(claudeDir);
        String plsqlSessionId = result.getPlsqlSessionId() != null ? result.getPlsqlSessionId() : "";

        // Build combined work list: procLinks + javaOnlyProcs + oracleOnlyProcs + 1 overall
        List<ChunkWork> workList = new ArrayList<>();

        List<CrossRefProcLink> links = result.getProcLinks() != null ? result.getProcLinks() : List.of();
        for (CrossRefProcLink link : links) {
            String javaKey = link.getJavaNodeId() != null ? link.getJavaNodeId()
                    : (link.getJavaClass() + "#" + link.getJavaMethod());
            String oracleRef = link.getResolvedProcRef() != null ? link.getResolvedProcRef() : "";
            String chunkId = stableHash("LINK|" + javaKey + "|" + oracleRef + "|" + plsqlSessionId);
            link.setClaudeChunkId(chunkId);
            workList.add(new ChunkWork(ChunkType.PROC_LINK, chunkId, null, null, link, null, null, result, jarId, crossRefId));
        }

        List<CrossRefResult.JavaOnlyProc> javaOnlyList = result.getJavaOnlyProcs() != null ? result.getJavaOnlyProcs() : List.of();
        for (CrossRefResult.JavaOnlyProc jp : javaOnlyList) {
            String key = (jp.getJavaClass() != null ? jp.getJavaClass() : "") + "#" + (jp.getJavaMethod() != null ? jp.getJavaMethod() : "");
            String chunkId = stableHash("JAVA_ONLY|" + key + "|" + plsqlSessionId);
            jp.setClaudeChunkId(chunkId);
            workList.add(new ChunkWork(ChunkType.JAVA_ONLY, chunkId, jp, null, null, null, null, result, jarId, crossRefId));
        }

        List<CrossRefResult.OracleOnlyProc> oracleOnlyList = result.getOracleOnlyProcs() != null ? result.getOracleOnlyProcs() : List.of();
        for (CrossRefResult.OracleOnlyProc op : oracleOnlyList) {
            String key = op.getProcRef() != null ? op.getProcRef() : "";
            String chunkId = stableHash("ORACLE_ONLY|" + key + "|" + plsqlSessionId);
            op.setClaudeChunkId(chunkId);
            workList.add(new ChunkWork(ChunkType.ORACLE_ONLY, chunkId, null, op, null, null, null, result, jarId, crossRefId));
        }

        // Overall summary chunk — runs last, after all individual chunks complete
        String overallChunkId = stableHash("OVERALL|" + crossRefId + "|" + plsqlSessionId);
        workList.add(new ChunkWork(ChunkType.OVERALL, overallChunkId, null, null, null, null, null, result, jarId, crossRefId));

        int total = workList.size();
        long skipped = workList.stream().filter(w -> checkpoint.contains(w.chunkId)).count();
        AtomicInteger completed = new AtomicInteger((int) skipped);
        AtomicInteger verdictConverted = new AtomicInteger(0);
        AtomicInteger verdictPartial = new AtomicInteger(0);
        AtomicInteger verdictNotConverted = new AtomicInteger(0);

        // Pre-load cached results for already-done chunks
        for (ChunkWork w : workList) {
            if (checkpoint.contains(w.chunkId)) {
                JsonNode cached = readChunkResult(claudeDir, w.chunkId);
                if (cached != null) applyCachedResult(w, cached, verdictConverted, verdictPartial, verdictNotConverted, result);
            }
        }

        log.info("{} Starting enrichment jar={} crossRef={} total={} skip={} parallel={}",
                LOG_PREFIX, jarId, crossRefId, total, skipped, parallelChunks);

        try {
            emitter.send(SseEmitter.event().name("progress").data(
                    progressJson(completed.get(), total, "IN_PROGRESS", verdictConverted.get(), verdictPartial.get(), verdictNotConverted.get())));
        } catch (Exception ignored) {}

        // Separate the overall chunk from the rest — it runs after all others
        List<ChunkWork> individualWork = workList.stream()
                .filter(w -> w.type != ChunkType.OVERALL).collect(Collectors.toList());
        ChunkWork overallWork = workList.stream()
                .filter(w -> w.type == ChunkType.OVERALL).findFirst().orElse(null);

        ExecutorService executor = Executors.newFixedThreadPool(parallelChunks,
                r -> { Thread t = new Thread(r, "cr-claude-" + crossRefId); t.setDaemon(true); return t; });
        runningExecutors.put(crossRefId, executor);

        List<Future<?>> futures = new ArrayList<>();
        for (ChunkWork w : individualWork) {
            if (checkpoint.contains(w.chunkId)) continue;
            futures.add(executor.submit(() -> {
                if (cancel.get()) return;
                try {
                    processChunk(w, claudeDir, checkpoint, verdictConverted, verdictPartial, verdictNotConverted, result);
                    int done = completed.incrementAndGet();
                    try {
                        emitter.send(SseEmitter.event().name("progress").data(
                                progressJson(done, total, "IN_PROGRESS",
                                        verdictConverted.get(), verdictPartial.get(), verdictNotConverted.get())));
                    } catch (Exception ignored) {}
                } catch (Exception e) {
                    log.warn("{} Chunk {} failed: {}", LOG_PREFIX, w.chunkId, e.getMessage());
                    completed.incrementAndGet();
                }
            }));
        }

        Thread waiter = new Thread(() -> {
            try {
                for (Future<?> f : futures) {
                    try { f.get(600, TimeUnit.SECONDS); }
                    catch (TimeoutException | CancellationException ignored) {}
                    catch (Exception e) { log.debug("{} Future error: {}", LOG_PREFIX, e.getMessage()); }
                }

                // Run overall chunk after all individual ones complete
                if (overallWork != null && !cancel.get() && !checkpoint.contains(overallWork.chunkId)) {
                    try {
                        processChunk(overallWork, claudeDir, checkpoint, verdictConverted, verdictPartial, verdictNotConverted, result);
                        completed.incrementAndGet();
                        try {
                            emitter.send(SseEmitter.event().name("progress").data(
                                    progressJson(completed.get(), total, "IN_PROGRESS",
                                            verdictConverted.get(), verdictPartial.get(), verdictNotConverted.get())));
                        } catch (Exception ignored) {}
                    } catch (Exception e) {
                        log.warn("{} Overall chunk failed: {}", LOG_PREFIX, e.getMessage());
                    }
                }
            } finally {
                executor.shutdownNow();
                runningExecutors.remove(crossRefId);
                cancelFlags.remove(crossRefId);
                resultWriteLocks.remove(crossRefId);

                // Final merge: pick up anything flushChunkToResultJson may have missed
                CrossRefResult finalResult = store.readResult(store.runDir(jarId, crossRefId));
                if (finalResult != null) {
                    mergeAllChunkResults(finalResult, claudeDir);
                    store.writeResult(store.runDir(jarId, crossRefId), finalResult);
                }

                String status = cancel.get() ? "CANCELLED" : "COMPLETE";
                log.info("{} Enrichment {} — {} done={}/{}", LOG_PREFIX, status, crossRefId, completed.get(), total);
                try {
                    emitter.send(SseEmitter.event().name("done").data(
                            progressJson(completed.get(), total, status,
                                    verdictConverted.get(), verdictPartial.get(), verdictNotConverted.get())));
                    emitter.complete();
                } catch (Exception ignored) {}
            }
        }, "cr-claude-waiter-" + crossRefId);
        waiter.setDaemon(true);
        waiter.start();

        return emitter;
    }

    /**
     * Re-merge all existing chunk files into result.json without re-running Claude.
     * Useful when a run completed but claudeDone fields were null in the saved result.
     */
    public Map<String, Object> mergeOnly(String jarId, String crossRefId) {
        CrossRefResult result = store.readResult(store.runDir(jarId, crossRefId));
        if (result == null) return Map.of("error", "result not found");
        ArtifactKey claudeDir = claudeDirKey(jarId, crossRefId);
        mergeAllChunkResults(result, claudeDir);
        store.writeResult(store.runDir(jarId, crossRefId), result);
        long done = (result.getProcLinks() != null
                ? result.getProcLinks().stream().filter(CrossRefProcLink::isClaudeDone).count() : 0)
                + (result.getJavaOnlyProcs() != null
                ? result.getJavaOnlyProcs().stream().filter(j -> j.isClaudeDone()).count() : 0)
                + (result.getOracleOnlyProcs() != null
                ? result.getOracleOnlyProcs().stream().filter(o -> o.isClaudeDone()).count() : 0);
        int total = (result.getProcLinks() != null ? result.getProcLinks().size() : 0)
                + (result.getJavaOnlyProcs() != null ? result.getJavaOnlyProcs().size() : 0)
                + (result.getOracleOnlyProcs() != null ? result.getOracleOnlyProcs().size() : 0) + 1;
        return Map.of("merged", done, "total", total,
                "overallVerdict", result.getClaudeOverallVerdict() != null ? result.getClaudeOverallVerdict() : "");
    }

    public boolean cancel(String jarId, String crossRefId) {
        AtomicBoolean flag = cancelFlags.get(crossRefId);
        if (flag != null) flag.set(true);
        ExecutorService ex = runningExecutors.get(crossRefId);
        if (ex != null) { ex.shutdownNow(); runningExecutors.remove(crossRefId); }
        return flag != null || ex != null;
    }

    public Map<String, Object> getProgress(String jarId, String crossRefId) {
        CrossRefResult result = store.readResult(store.runDir(jarId, crossRefId));
        if (result == null) return Map.of("error", "not found");
        List<CrossRefProcLink> links = result.getProcLinks() != null ? result.getProcLinks() : List.of();
        int javaOnly = result.getJavaOnlyProcs() != null ? result.getJavaOnlyProcs().size() : 0;
        int oracleOnly = result.getOracleOnlyProcs() != null ? result.getOracleOnlyProcs().size() : 0;
        int total = links.size() + javaOnly + oracleOnly + 1; // +1 for overall
        int done = (int) links.stream().filter(CrossRefProcLink::isClaudeDone).count();
        if (result.getJavaOnlyProcs() != null) done += (int) result.getJavaOnlyProcs().stream().filter(j -> j.isClaudeDone()).count();
        if (result.getOracleOnlyProcs() != null) done += (int) result.getOracleOnlyProcs().stream().filter(o -> o.isClaudeDone()).count();
        if (result.getClaudeOverallVerdict() != null && !result.getClaudeOverallVerdict().isBlank()) done++;
        int converted = (int) links.stream().filter(l -> "CONVERTED".equals(l.getClaudeVerdict())).count();
        int partial = (int) links.stream().filter(l -> "PARTIAL".equals(l.getClaudeVerdict())).count();
        int notConverted = (int) links.stream().filter(l -> "NOT_CONVERTED".equals(l.getClaudeVerdict())).count();
        boolean running = runningExecutors.containsKey(crossRefId);
        String status = running ? "IN_PROGRESS" : (done == total && total > 1 ? "COMPLETE" : (done > 0 ? "PARTIAL_DONE" : "NOT_STARTED"));
        Map<String, Object> m = new LinkedHashMap<>(progressMap(done, total, status, converted, partial, notConverted));
        if (result.getClaudeOverallVerdict() != null) {
            m.put("overallVerdict", result.getClaudeOverallVerdict());
            m.put("overallConfidence", result.getClaudeOverallConfidence());
            m.put("overallReason", result.getClaudeOverallReason());
            m.put("conversionPct", result.getClaudeConversionPct());
        }
        return m;
    }

    public Map<String, Object> getChunkDetail(String jarId, String crossRefId, String chunkId) {
        ArtifactKey claudeDir = claudeDirKey(jarId, crossRefId);
        if (claudeDir == null) return Map.of("error", "chunk not found");
        // chunkId comes straight off the URL: keep it to a single, separator-free key segment.
        if (chunkId == null || chunkId.contains("..") || chunkId.contains("/") || chunkId.contains("\\")) {
            return Map.of("error", "chunk not found");
        }
        ArtifactKey chunkFile = claudeDir.resolve(chunkId + ".json");
        if (!artifacts.exists(chunkFile)) return Map.of("error", "chunk not found");
        try {
            JsonNode node = readTree(chunkFile);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("chunkId", chunkId);
            if (node.has("prompt")) result.put("prompt", node.path("prompt").asText(""));
            if (node.has("response")) result.put("response", node.path("response").asText(""));
            if (node.has("verdict")) result.put("verdict", node.path("verdict").asText(""));
            if (node.has("confidence")) result.put("confidence", node.path("confidence").asText(""));
            if (node.has("reason")) result.put("reason", node.path("reason").asText(""));
            if (node.has("gaps")) result.put("gaps", node.path("gaps"));
            if (node.has("tableAnalysis")) result.put("tableAnalysis", node.path("tableAnalysis"));
            if (node.has("aggregationVerification")) result.put("aggregationVerification", node.path("aggregationVerification"));
            if (node.has("dynamicDispatchVerification")) result.put("dynamicDispatchVerification", node.path("dynamicDispatchVerification"));
            if (node.has("externalDependencyVerification")) result.put("externalDependencyVerification", node.path("externalDependencyVerification"));
            if (node.has("durationMs")) result.put("durationMs", node.path("durationMs").asLong(0));
            return result;
        } catch (Exception e) {
            return Map.of("error", e.getMessage());
        }
    }

    // ── Chunk types ───────────────────────────────────────────────────────────

    private enum ChunkType { PROC_LINK, JAVA_ONLY, ORACLE_ONLY, OVERALL }

    // ── Chunk processing ──────────────────────────────────────────────────────

    private void processChunk(ChunkWork w, ArtifactKey claudeDir, Set<String> checkpoint,
                               AtomicInteger converted, AtomicInteger partial, AtomicInteger notConverted,
                               CrossRefResult liveResult) {
        long start = System.currentTimeMillis();
        String prompt = switch (w.type) {
            case PROC_LINK    -> buildProcLinkPrompt(w.link, w.result, w.jarId);
            case JAVA_ONLY    -> buildJavaOnlyPrompt(w.javaOnly, w.result, w.jarId);
            case ORACLE_ONLY  -> buildOracleOnlyPrompt(w.oracleOnly, w.result);
            case OVERALL      -> buildOverallPrompt(liveResult);
        };

        log.debug("{} chunk={} type={} prompt_len={}", LOG_PREFIX, w.chunkId, w.type, prompt.length());
        String response = runClaude(prompt);
        long durationMs = System.currentTimeMillis() - start;

        if (response == null || response.isBlank()) {
            log.warn("{} chunk={} empty response after {}ms", LOG_PREFIX, w.chunkId, durationMs);
            return;
        }

        JsonNode parsed = parseClaudeJson(response);
        if (parsed == null) {
            log.warn("{} chunk={} cannot parse JSON", LOG_PREFIX, w.chunkId);
            return;
        }

        ObjectNode chunkNode = objectMapper.createObjectNode();
        chunkNode.put("chunkId", w.chunkId);
        chunkNode.put("type", w.type.name());
        chunkNode.put("prompt", prompt);
        chunkNode.put("response", response);
        chunkNode.put("durationMs", durationMs);
        chunkNode.set("verdict", parsed.path("verdict"));
        chunkNode.set("confidence", parsed.path("confidence"));
        chunkNode.set("reason", parsed.path("reason"));
        chunkNode.set("gaps", parsed.path("gaps"));
        if (parsed.has("conversionPct")) chunkNode.set("conversionPct", parsed.path("conversionPct"));
        if (parsed.has("tableAnalysis")) chunkNode.set("tableAnalysis", parsed.path("tableAnalysis"));
        // Additive: standalone verification of Aggregation/Dynamic Dispatch/External Dependency
        // static-analysis facets — no Oracle-side equivalent, so verified against Java source only.
        if (parsed.has("aggregationVerification")) chunkNode.set("aggregationVerification", parsed.path("aggregationVerification"));
        if (parsed.has("dynamicDispatchVerification")) chunkNode.set("dynamicDispatchVerification", parsed.path("dynamicDispatchVerification"));
        if (parsed.has("externalDependencyVerification")) chunkNode.set("externalDependencyVerification", parsed.path("externalDependencyVerification"));

        writeChunkResult(claudeDir, w.chunkId, chunkNode);
        applyChunkToResult(w, chunkNode, converted, partial, notConverted, liveResult);
        addToCheckpoint(claudeDir, checkpoint, w.chunkId);
        flushChunkToResultJson(w.jarId, w.crossRefId, w.chunkId, chunkNode);
    }

    private void applyCachedResult(ChunkWork w, JsonNode chunk,
                                    AtomicInteger converted, AtomicInteger partial,
                                    AtomicInteger notConverted, CrossRefResult result) {
        applyChunkToResult(w, chunk, converted, partial, notConverted, result);
    }

    private void applyChunkToResult(ChunkWork w, JsonNode chunk,
                                     AtomicInteger converted, AtomicInteger partial,
                                     AtomicInteger notConverted, CrossRefResult result) {
        String verdict = chunk.path("verdict").asText("");
        String confidence = chunk.path("confidence").asText("");
        String reason = chunk.path("reason").asText("");
        List<String> gaps = new ArrayList<>();
        for (JsonNode g : chunk.path("gaps")) gaps.add(g.asText());
        List<Map<String, Object>> aggVerification = jsonArrayToMapList(chunk.path("aggregationVerification"));
        List<Map<String, Object>> dynVerification = jsonArrayToMapList(chunk.path("dynamicDispatchVerification"));
        List<Map<String, Object>> extVerification = jsonArrayToMapList(chunk.path("externalDependencyVerification"));

        switch (w.type) {
            case PROC_LINK -> {
                if (w.link != null) {
                    w.link.setClaudeVerdict(verdict);
                    w.link.setClaudeConfidence(confidence);
                    w.link.setClaudeReason(reason);
                    w.link.setClaudeGaps(gaps);
                    w.link.setClaudeDone(true);
                    w.link.setClaudeAggregationVerification(aggVerification);
                    w.link.setClaudeDynamicDispatchVerification(dynVerification);
                    w.link.setClaudeExternalDependencyVerification(extVerification);
                    if ("CONVERTED".equals(verdict)) converted.incrementAndGet();
                    else if ("PARTIAL".equals(verdict)) partial.incrementAndGet();
                    else if ("NOT_CONVERTED".equals(verdict)) notConverted.incrementAndGet();
                }
            }
            case JAVA_ONLY -> {
                if (w.javaOnly != null) {
                    w.javaOnly.setClaudeVerdict(verdict);
                    w.javaOnly.setClaudeConfidence(confidence);
                    w.javaOnly.setClaudeReason(reason);
                    w.javaOnly.setClaudeDone(true);
                    w.javaOnly.setClaudeAggregationVerification(aggVerification);
                    w.javaOnly.setClaudeDynamicDispatchVerification(dynVerification);
                    w.javaOnly.setClaudeExternalDependencyVerification(extVerification);
                }
            }
            case ORACLE_ONLY -> {
                if (w.oracleOnly != null) {
                    w.oracleOnly.setClaudeVerdict(verdict);
                    w.oracleOnly.setClaudeConfidence(confidence);
                    w.oracleOnly.setClaudeReason(reason);
                    w.oracleOnly.setClaudeDone(true);
                }
            }
            case OVERALL -> {
                if (result != null) {
                    result.setClaudeOverallVerdict(verdict);
                    result.setClaudeOverallConfidence(confidence);
                    result.setClaudeOverallReason(reason);
                    result.setClaudeOverallGaps(gaps);
                    int pct = chunk.path("conversionPct").asInt(0);
                    result.setClaudeConversionPct(pct);
                }
            }
        }
    }

    private void mergeAllChunkResults(CrossRefResult result, ArtifactKey claudeDir) {
        // IMPORTANT: recompute chunkId from hash formula — claudeChunkId field is null
        // in the disk copy because it was only set on in-memory objects during the run.
        String plsqlSessionId = result.getPlsqlSessionId() != null ? result.getPlsqlSessionId() : "";

        if (result.getProcLinks() != null) {
            for (CrossRefProcLink link : result.getProcLinks()) {
                if (link.isClaudeDone()) continue;
                String javaKey = link.getJavaNodeId() != null ? link.getJavaNodeId()
                        : (link.getJavaClass() + "#" + link.getJavaMethod());
                String oracleRef = link.getResolvedProcRef() != null ? link.getResolvedProcRef() : "";
                String chunkId = stableHash("LINK|" + javaKey + "|" + oracleRef + "|" + plsqlSessionId);
                link.setClaudeChunkId(chunkId);
                JsonNode cached = readChunkResult(claudeDir, chunkId);
                if (cached != null) {
                    link.setClaudeVerdict(cached.path("verdict").asText(""));
                    link.setClaudeConfidence(cached.path("confidence").asText(""));
                    link.setClaudeReason(cached.path("reason").asText(""));
                    List<String> gaps = new ArrayList<>();
                    for (JsonNode g : cached.path("gaps")) gaps.add(g.asText());
                    link.setClaudeGaps(gaps);
                    link.setClaudeDone(true);
                    link.setClaudeAggregationVerification(jsonArrayToMapList(cached.path("aggregationVerification")));
                    link.setClaudeDynamicDispatchVerification(jsonArrayToMapList(cached.path("dynamicDispatchVerification")));
                    link.setClaudeExternalDependencyVerification(jsonArrayToMapList(cached.path("externalDependencyVerification")));
                }
            }
        }
        if (result.getJavaOnlyProcs() != null) {
            for (CrossRefResult.JavaOnlyProc jp : result.getJavaOnlyProcs()) {
                if (jp.isClaudeDone()) continue;
                String key = (jp.getJavaClass() != null ? jp.getJavaClass() : "") + "#"
                        + (jp.getJavaMethod() != null ? jp.getJavaMethod() : "");
                String chunkId = stableHash("JAVA_ONLY|" + key + "|" + plsqlSessionId);
                jp.setClaudeChunkId(chunkId);
                JsonNode cached = readChunkResult(claudeDir, chunkId);
                if (cached != null) {
                    jp.setClaudeVerdict(cached.path("verdict").asText(""));
                    jp.setClaudeConfidence(cached.path("confidence").asText(""));
                    jp.setClaudeReason(cached.path("reason").asText(""));
                    jp.setClaudeDone(true);
                    jp.setClaudeAggregationVerification(jsonArrayToMapList(cached.path("aggregationVerification")));
                    jp.setClaudeDynamicDispatchVerification(jsonArrayToMapList(cached.path("dynamicDispatchVerification")));
                    jp.setClaudeExternalDependencyVerification(jsonArrayToMapList(cached.path("externalDependencyVerification")));
                }
            }
        }
        if (result.getOracleOnlyProcs() != null) {
            for (CrossRefResult.OracleOnlyProc op : result.getOracleOnlyProcs()) {
                if (op.isClaudeDone()) continue;
                String key = op.getProcRef() != null ? op.getProcRef() : "";
                String chunkId = stableHash("ORACLE_ONLY|" + key + "|" + plsqlSessionId);
                op.setClaudeChunkId(chunkId);
                JsonNode cached = readChunkResult(claudeDir, chunkId);
                if (cached != null) {
                    op.setClaudeVerdict(cached.path("verdict").asText(""));
                    op.setClaudeConfidence(cached.path("confidence").asText(""));
                    op.setClaudeReason(cached.path("reason").asText(""));
                    op.setClaudeDone(true);
                }
            }
        }
        // Overall — from overall chunk file
        String overallChunkId = stableHash("OVERALL|" + result.getCrossRefId() + "|" + plsqlSessionId);
        result.setClaudeOverallChunkId(overallChunkId);
        JsonNode overallCached = readChunkResult(claudeDir, overallChunkId);
        if (overallCached != null) {
            result.setClaudeOverallVerdict(overallCached.path("verdict").asText(""));
            result.setClaudeOverallConfidence(overallCached.path("confidence").asText(""));
            result.setClaudeOverallReason(overallCached.path("reason").asText(""));
            List<String> gaps = new ArrayList<>();
            for (JsonNode g : overallCached.path("gaps")) gaps.add(g.asText());
            result.setClaudeOverallGaps(gaps);
            result.setClaudeConversionPct(overallCached.path("conversionPct").asInt(0));
        }
    }

    // ── Static facet lookup (Aggregation / Dynamic Dispatch / External Dependency) ─────────────

    /**
     * Per-jarId cache of {@code simpleClassName#methodName -> raw call-tree node JSON}, built by
     * scanning every {@code endpoints/*}/nodes/*.json} file once. Aggregation pipeline stages,
     * dispatch type and cross-module/HTTP-target info all live directly on the node itself (see
     * {@code com.jaranalyzer.model.CallNode}), so one index serves all three facets.
     */
    private final Map<String, Map<String, JsonNode>> javaNodeIndexCache = new ConcurrentHashMap<>();

    private Map<String, JsonNode> buildJavaNodeIndex(String jarId) {
        Map<String, JsonNode> idx = new HashMap<>();
        if (jarId == null) return idx;
        String effectiveJarId = jarId.endsWith(".jar") ? jarId.substring(0, jarId.length() - 4) : jarId;
        ArtifactKey jarDir = jarDataPaths.endpointsDirKey(effectiveJarId);
        try {
            for (ArtifactKey epDir : childDirs(jarDir)) {
                ArtifactKey nodesDir = epDir.resolve("nodes");
                for (ArtifactKey nodeFile : childFiles(nodesDir)) {
                    if (!nodeFile.leaf().endsWith(".json")) continue;
                    try {
                        JsonNode n = readTree(nodeFile);
                        String cls = n.path("simpleClassName").asText(
                                n.path("className").asText("").replaceAll(".*\\.", ""));
                        String meth = n.path("methodName").asText("");
                        if (cls.isBlank() && meth.isBlank()) continue;
                        idx.putIfAbsent(cls + "#" + meth, n);
                    } catch (Exception ignored) {}
                }
            }
        } catch (IOException e) {
            log.debug("{} Cannot build java node index for {}: {}", LOG_PREFIX, jarId, e.getMessage());
        }
        return idx;
    }

    private JsonNode lookupJavaNode(String jarId, String javaClass, String javaMethod) {
        if (jarId == null) return null;
        Map<String, JsonNode> idx = javaNodeIndexCache.computeIfAbsent(jarId, this::buildJavaNodeIndex);
        String simple = javaClass != null ? javaClass.replaceAll(".*\\.", "") : "";
        return idx.get(simple + "#" + (javaMethod != null ? javaMethod : ""));
    }

    /** Tracks which of the three standalone facets actually had static-analysis data to show. */
    private static final class StaticFacetsPresent {
        boolean aggregation;
        boolean dynamicDispatch;
        boolean externalDependency;
        boolean any() { return aggregation || dynamicDispatch || externalDependency; }
    }

    /**
     * Appends "## Aggregation Pipeline", "## Dynamic Dispatch", "## External Dependency" sections
     * to {@code sb} for whichever facets the static analysis actually found on this Java method's
     * call-tree node — each is verified standalone against the Java source (no Oracle side exists
     * to compare against). Sections with nothing to show are omitted entirely.
     */
    private StaticFacetsPresent appendStaticFacets(StringBuilder sb, JsonNode node) {
        StaticFacetsPresent present = new StaticFacetsPresent();
        if (node == null) return present;

        JsonNode agg = node.path("aggregationPipeline");
        if (agg.isObject() && agg.size() > 0) {
            present.aggregation = true;
            sb.append("\n## Aggregation Pipeline (static analysis found this on the Java method)\n");
            sb.append(agg.toString()).append("\n");
        }

        String dispatchType = node.path("dispatchType").asText("");
        String resolvedFrom = node.path("resolvedFrom").asText("");
        String qualifierHint = node.path("qualifierHint").asText("");
        if (!dispatchType.isBlank() && !"DIRECT".equalsIgnoreCase(dispatchType)) {
            present.dynamicDispatch = true;
            sb.append("\n## Dynamic Dispatch (static analysis found this on the Java method)\n");
            sb.append("Dispatch type: ").append(dispatchType).append("\n");
            if (!resolvedFrom.isBlank()) sb.append("Resolved from interface/abstract: ").append(resolvedFrom).append("\n");
            if (!qualifierHint.isBlank()) sb.append("Qualifier hint: ").append(qualifierHint).append("\n");
        }

        boolean crossModule = node.path("crossModule").asBoolean(false);
        String httpTargetMethod = node.path("httpTargetMethod").asText("");
        String httpTargetPath = node.path("httpTargetPath").asText("");
        String sourceJar = node.path("sourceJar").asText("");
        String module = node.path("module").asText("");
        if (crossModule || !httpTargetMethod.isBlank() || !httpTargetPath.isBlank()) {
            present.externalDependency = true;
            sb.append("\n## External Dependency (static analysis found this on the Java method)\n");
            if (crossModule) sb.append("Cross-module call: module=").append(module).append(" sourceJar=").append(sourceJar).append("\n");
            if (!httpTargetMethod.isBlank() || !httpTargetPath.isBlank())
                sb.append("HTTP target: ").append(httpTargetMethod).append(" ").append(httpTargetPath).append("\n");
        }
        return present;
    }

    /** Appended to the JSON response schema whenever any of the three facets were shown above. */
    private static final String STATIC_FACET_TASK_INSTRUCTION =
            "5. If any of 'Aggregation Pipeline', 'Dynamic Dispatch', or 'External Dependency' sections "
            + "are present above, verify each finding standalone against the actual Java source (there is "
            + "no Oracle-side equivalent to compare against) and return status CONFIRMED (still accurate), "
            + "REMOVED (static analysis was wrong / not actually present), or NEW (you spotted an additional "
            + "such finding not listed) with a one-sentence reason. Return an empty array for any section not "
            + "present above.\n";

    private static final String STATIC_FACET_JSON_SCHEMA =
            ",\"aggregationVerification\":[{\"item\":\"...\",\"status\":\"CONFIRMED|REMOVED|NEW\",\"reason\":\"...\"}]"
            + ",\"dynamicDispatchVerification\":[{\"item\":\"...\",\"status\":\"CONFIRMED|REMOVED|NEW\",\"reason\":\"...\"}]"
            + ",\"externalDependencyVerification\":[{\"item\":\"...\",\"status\":\"CONFIRMED|REMOVED|NEW\",\"reason\":\"...\"}]";

    // ── Prompt builders ───────────────────────────────────────────────────────

    private String buildProcLinkPrompt(CrossRefProcLink link, CrossRefResult result, String jarId) {
        return buildPrompt(link, result, jarId);
    }

    private String buildJavaOnlyPrompt(CrossRefResult.JavaOnlyProc jp, CrossRefResult result, String jarId) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are analyzing a Java→Oracle migration to check if Java code was converted to Oracle PL/SQL.\n\n");
        sb.append("## Context\n");
        sb.append("This Java method accesses MongoDB collections but NO matching Oracle proc was found by the static analysis.\n\n");
        sb.append("## Java Method\n");
        sb.append("Class: ").append(jp.getJavaClass() != null ? jp.getJavaClass().replaceAll(".*\\.", "") : "-").append("\n");
        sb.append("Method: ").append(jp.getJavaMethod() != null ? jp.getJavaMethod() : "-").append("\n");
        sb.append("Stereotype: ").append(jp.getStereotype() != null ? jp.getStereotype() : "-").append("\n");
        sb.append("Operation: ").append(jp.getOperationType() != null ? jp.getOperationType() : "-").append("\n");
        sb.append("MongoDB tables: ").append(jp.getTables() != null ? String.join(", ", jp.getTables()) : "-").append("\n");
        sb.append("Static analysis reason: ").append(jp.getReason() != null ? jp.getReason() : "-").append("\n\n");
        sb.append("## Available Oracle Packages\n");
        if (result.getOracleNodes() != null) {
            result.getOracleNodes().stream().filter(n -> n.getDepth() <= 1).limit(20).forEach(n -> {
                sb.append("  ").append(n.getPackageName() != null ? n.getPackageName() : "")
                  .append(".").append(n.getObjectName() != null ? n.getObjectName() : "").append("\n");
            });
        }
        // Standalone facets: aggregation/dispatch/external-dependency have no Oracle equivalent,
        // so they are verified against the Java source itself, not cross-checked with Oracle.
        JsonNode javaOnlyNode = lookupJavaNode(jarId, jp.getJavaClass(), jp.getJavaMethod());
        StaticFacetsPresent facets = appendStaticFacets(sb, javaOnlyNode);

        sb.append("\n## Task\n");
        sb.append("1. Determine: was this Java method converted to Oracle, or does it truly have no Oracle equivalent?\n");
        sb.append("Verdict: NOT_CONVERTED (no Oracle equivalent exists), HAS_ORACLE_EQUIVALENT (static analysis missed it — name the proc), INFRA_ONLY (pure Java infra, no Oracle needed), UNKNOWN\n");
        if (facets.any()) sb.append(STATIC_FACET_TASK_INSTRUCTION);
        sb.append("\nRespond as JSON only:\n");
        sb.append("{\"verdict\":\"NOT_CONVERTED|HAS_ORACLE_EQUIVALENT|INFRA_ONLY|UNKNOWN\",");
        sb.append("\"confidence\":\"HIGH|MEDIUM|LOW\",\"reason\":\"...\",\"gaps\":[\"...\"]");
        sb.append(STATIC_FACET_JSON_SCHEMA).append("}");
        return sb.toString();
    }

    private String buildOracleOnlyPrompt(CrossRefResult.OracleOnlyProc op, CrossRefResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are analyzing a Java→Oracle migration.\n\n");
        sb.append("## Context\n");
        sb.append("This Oracle proc has NO Java callers found by static analysis.\n\n");
        sb.append("## Oracle Proc\n");
        sb.append("Proc: ").append(op.getProcRef() != null ? op.getProcRef() : "-").append("\n");
        sb.append("Package: ").append(op.getPackageName() != null ? op.getPackageName() : "-").append("\n");
        sb.append("Depth: ").append(op.getDepth()).append("\n");
        sb.append("Tables accessed: ").append(op.getTables() != null ? String.join(", ", op.getTables()) : "-").append("\n\n");
        String detail = loadOracleProcDetail(result, op.getProcRef());
        if (detail != null) sb.append(detail).append("\n");
        sb.append("## Known Java→Oracle Links\n");
        if (result.getProcLinks() != null) {
            result.getProcLinks().stream().filter(l -> op.getPackageName() != null
                    && op.getPackageName().equalsIgnoreCase(
                            l.getResolvedProcRef() != null && l.getResolvedProcRef().contains(".")
                            ? l.getResolvedProcRef().substring(0, l.getResolvedProcRef().indexOf('.')) : ""))
                    .limit(5).forEach(l -> {
                sb.append("  ").append(l.getJavaClass() != null ? l.getJavaClass().replaceAll(".*\\.", "") : "")
                  .append(".").append(l.getJavaMethod() != null ? l.getJavaMethod() : "").append("\n");
            });
        }
        sb.append("\n## Task\n");
        sb.append("Determine: was the Oracle logic in this proc implemented in Java (migration complete), or is it missing?\n");
        sb.append("Verdict: NOT_IN_JAVA (missing Java equivalent), HAS_JAVA_EQUIVALENT (Java side exists, static analysis missed it), INTERNAL_ONLY (Oracle-internal helper, no Java needed), UNKNOWN\n\n");
        sb.append("Respond as JSON only:\n");
        sb.append("{\"verdict\":\"NOT_IN_JAVA|HAS_JAVA_EQUIVALENT|INTERNAL_ONLY|UNKNOWN\",");
        sb.append("\"confidence\":\"HIGH|MEDIUM|LOW\",\"reason\":\"...\",\"gaps\":[\"...\"]}");
        return sb.toString();
    }

    private String buildOverallPrompt(CrossRefResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are doing a final assessment of a Java (MongoDB) → Oracle PL/SQL migration cross-reference.\n\n");
        sb.append("## Migration Stats\n");
        List<CrossRefProcLink> links = result.getProcLinks() != null ? result.getProcLinks() : List.of();
        int total = links.size();
        int converted = (int) links.stream().filter(l -> "CONVERTED".equals(l.getClaudeVerdict())).count();
        int partial = (int) links.stream().filter(l -> "PARTIAL".equals(l.getClaudeVerdict())).count();
        int notConverted = (int) links.stream().filter(l -> "NOT_CONVERTED".equals(l.getClaudeVerdict())).count();
        int javaOnlyCount = result.getJavaOnlyProcs() != null ? result.getJavaOnlyProcs().size() : 0;
        int oracleOnlyCount = result.getOracleOnlyProcs() != null ? result.getOracleOnlyProcs().size() : 0;
        sb.append("Proc Links analyzed: ").append(total).append("\n");
        sb.append("  CONVERTED: ").append(converted).append("\n");
        sb.append("  PARTIAL: ").append(partial).append("\n");
        sb.append("  NOT_CONVERTED: ").append(notConverted).append("\n");
        sb.append("  NOT_YET_ANALYZED: ").append(total - converted - partial - notConverted).append("\n");
        sb.append("Java-only gaps: ").append(javaOnlyCount).append("\n");
        sb.append("Oracle-only gaps: ").append(oracleOnlyCount).append("\n\n");
        sb.append("## Top Gaps Found\n");
        // Collect gaps from all analyzed items
        links.stream().filter(l -> l.getClaudeGaps() != null && !l.getClaudeGaps().isEmpty()).limit(10)
                .forEach(l -> l.getClaudeGaps().forEach(g -> sb.append("  [LINK] ").append(g).append("\n")));
        if (result.getJavaOnlyProcs() != null) {
            result.getJavaOnlyProcs().stream().filter(j -> "NOT_CONVERTED".equals(j.getClaudeVerdict())).limit(5)
                    .forEach(j -> sb.append("  [JAVA_ONLY] ")
                            .append(j.getJavaClass() != null ? j.getJavaClass().replaceAll(".*\\.", "") : "")
                            .append(".").append(j.getJavaMethod() != null ? j.getJavaMethod() : "")
                            .append(": ").append(j.getClaudeReason() != null ? j.getClaudeReason() : "").append("\n"));
        }
        if (result.getOracleOnlyProcs() != null) {
            result.getOracleOnlyProcs().stream().filter(o -> "NOT_IN_JAVA".equals(o.getClaudeVerdict())).limit(5)
                    .forEach(o -> sb.append("  [ORACLE_ONLY] ").append(o.getProcRef() != null ? o.getProcRef() : "")
                            .append(": ").append(o.getClaudeReason() != null ? o.getClaudeReason() : "").append("\n"));
        }
        sb.append("\n## Endpoint\n");
        sb.append("Java endpoint: ").append(result.getEndpointPath() != null ? result.getEndpointPath() : "-").append("\n");
        sb.append("Oracle proc: ").append(result.getProcName() != null ? result.getProcName() : "-").append("\n\n");
        sb.append("## Task\n");
        sb.append("Give an overall verdict on the completeness of this Java→Oracle migration for this endpoint.\n");
        sb.append("Estimate what percentage of functionality is converted.\n");
        sb.append("Verdict: FULLY_CONVERTED (≥95%), MOSTLY_CONVERTED (75-94%), PARTIAL (40-74%), SIGNIFICANT_GAPS (<40%)\n\n");
        sb.append("Respond as JSON only:\n");
        sb.append("{\"verdict\":\"FULLY_CONVERTED|MOSTLY_CONVERTED|PARTIAL|SIGNIFICANT_GAPS\",");
        sb.append("\"confidence\":\"HIGH|MEDIUM|LOW\",\"conversionPct\":85,");
        sb.append("\"reason\":\"2-4 sentence overall assessment\",\"gaps\":[\"top 3-5 critical gaps\"]}");
        return sb.toString();
    }

    private String buildPrompt(CrossRefProcLink link, CrossRefResult result, String jarId) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are comparing Java (MongoDB) code to Oracle PL/SQL.\n\n");

        // Java side
        sb.append("## Java Method\n");
        sb.append("NodeId: ").append(link.getJavaNodeId() != null ? link.getJavaNodeId() : "-").append("\n");
        sb.append("Class: ").append(link.getJavaClass() != null ? link.getJavaClass().replaceAll(".*\\.", "") : "-").append("\n");
        sb.append("Method: ").append(link.getJavaMethod() != null ? link.getJavaMethod() : "-").append("\n");
        sb.append("Stereotype: ").append(link.getJavaStereotype() != null ? link.getJavaStereotype() : "-").append("\n");
        sb.append("Operation: ").append(link.getJavaOperationType() != null ? link.getJavaOperationType() : "-").append("\n");
        if (link.getJavaCollections() != null && !link.getJavaCollections().isEmpty()) {
            sb.append("Collections accessed:\n");
            link.getJavaCollections().forEach(c ->
                    sb.append("  - ").append(c.getCollection()).append(" (").append(c.getOperation()).append(")\n"));
        }
        if (link.isHasLogParameters()) sb.append("Annotations: @LogParameters\n");

        // Oracle side
        sb.append("\n## Oracle Proc\n");
        sb.append("Ref: ").append(link.getResolvedProcRef() != null ? link.getResolvedProcRef() : "-").append("\n");
        sb.append("Confidence: ").append(link.getMatchConfidence()).append(" (").append(link.getZoneSource()).append(")\n");
        sb.append("PL/SQL node found: ").append(link.isPlsqlNodeFound()).append("\n");

        // Try to load Oracle proc detail from plsql-parse session
        String oracleProcDetail = loadOracleProcDetail(result, link);
        if (oracleProcDetail != null) sb.append(oracleProcDetail);

        // Table comparison
        sb.append("\n## Table Comparison\n");
        if (link.getTableMatches() != null && !link.getTableMatches().isEmpty()) {
            link.getTableMatches().forEach(tm ->
                    sb.append("  ").append(tm.getMongoCollection()).append(": ").append(tm.getMatchStatus())
                      .append(" (Java:").append(tm.getJavaOperation()).append(" Oracle:").append(tm.getOracleOperation()).append(")\n"));
        } else {
            sb.append("  No table comparison data available.\n");
        }

        // Static verdict
        String staticVerdict = inferStaticVerdict(link);
        sb.append("\n## Static Pre-Analysis\n");
        sb.append("Static verdict: ").append(staticVerdict).append("\n");

        // Standalone facets: aggregation/dispatch/external-dependency have no Oracle equivalent,
        // so they are verified against the Java source itself, not cross-checked with Oracle.
        JsonNode javaNode = lookupJavaNode(jarId, link.getJavaClass(), link.getJavaMethod());
        StaticFacetsPresent facets = appendStaticFacets(sb, javaNode);

        // Task
        sb.append("\n## Task\n");
        sb.append("1. Is this Java method converted to Oracle? Verdict: CONVERTED / PARTIAL / NOT_CONVERTED / NO_ORACLE_EQUIVALENT\n");
        sb.append("2. List specific gaps if any (missing tables, wrong operations, missing logic)\n");
        sb.append("3. Rate confidence: HIGH / MEDIUM / LOW\n");
        sb.append("4. Give 1-2 sentence reason\n");
        if (facets.any()) sb.append(STATIC_FACET_TASK_INSTRUCTION);
        sb.append("\nRespond as JSON only:\n");
        sb.append("{\"verdict\":\"CONVERTED|PARTIAL|NOT_CONVERTED|NO_ORACLE_EQUIVALENT\",");
        sb.append("\"confidence\":\"HIGH|MEDIUM|LOW\",\"reason\":\"...\",");
        sb.append("\"gaps\":[\"...\"],");
        sb.append("\"tableAnalysis\":[{\"table\":\"...\",\"status\":\"MATCHED|JAVA_ONLY|ORACLE_ONLY|OP_MISMATCH\",\"detail\":\"...\"}]");
        sb.append(STATIC_FACET_JSON_SCHEMA).append("}");
        return sb.toString();
    }

    private String inferStaticVerdict(CrossRefProcLink link) {
        if (link.getTableMatches() == null || link.getTableMatches().isEmpty()) {
            return link.isPlsqlNodeFound() ? "PARTIAL" : "NOT_CONVERTED";
        }
        long matched = link.getTableMatches().stream().filter(m -> "MATCHED".equals(m.getMatchStatus())).count();
        long javaOnly = link.getTableMatches().stream().filter(m -> "JAVA_ONLY".equals(m.getMatchStatus())).count();
        long total = link.getTableMatches().size();
        if (javaOnly == 0 && matched > 0) return "CONVERTED";
        if (matched > 0 && javaOnly > 0) return "PARTIAL";
        if (javaOnly == total) return "NOT_CONVERTED";
        return "PARTIAL";
    }

    private String loadOracleProcDetail(CrossRefResult result, String ref) {
        if (result.getPlsqlSessionId() == null || ref == null) return null;
        return loadOracleProcDetailByRef(result.getPlsqlSessionId(), ref);
    }

    private String loadOracleProcDetail(CrossRefResult result, CrossRefProcLink link) {
        if (result.getPlsqlSessionId() == null) return null;
        String ref = link.getResolvedProcRef();
        return loadOracleProcDetailByRef(result.getPlsqlSessionId(), ref);
    }

    /**
     * {@code plsql-parse/<sessionId>}, or null when the id is not a single safe segment.
     *
     * <p>The session id reaches here from request parameters and from result.json, so it is
     * untrusted. There is no base directory left to compare against, hence the explicit rejection
     * of "..", separators and blanks in place of the old {@code startsWith(base)} check.
     */
    private ArtifactKey plsqlSessionKey(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) return null;
        if (sessionId.contains("..") || sessionId.contains("/") || sessionId.contains("\\")) return null;
        return PLSQL_PARSE_ROOT.resolve(sessionId);
    }

    /** Strips any directory part of a recorded source-file name — it is data, not a path. */
    private static String fileNameOf(String sourceFile) {
        String s = sourceFile.replace('\\', '/');
        int i = s.lastIndexOf('/');
        return i >= 0 ? s.substring(i + 1) : s;
    }

    private String loadOracleProcDetailByRef(String sessionId, String ref) {
        if (ref == null) return null;
        ArtifactKey sessionDir = plsqlSessionKey(sessionId);
        if (sessionDir == null) return null;
        ArtifactKey indexFile = sessionDir.resolve("api", "index.json");
        if (!parserArtifacts.exists(indexFile)) return null;
        try {
            JsonNode idx = readTree(parserArtifacts, indexFile);
            String refUpper = ref.toUpperCase();
            for (JsonNode node : idx.path("nodes")) {
                String pkg = node.path("packageName").asText("");
                String obj = node.path("objectName").asText("");
                String fullRef = (pkg.isBlank() ? obj : pkg + "." + obj).toUpperCase();
                if (!fullRef.equals(refUpper)) continue;
                StringBuilder sb = new StringBuilder();
                List<String> tables = new ArrayList<>();
                for (JsonNode t : node.path("tables")) tables.add(t.asText());
                sb.append("Tables: ").append(String.join(", ", tables)).append("\n");
                sb.append("LOC: ").append(node.path("linesOfCode").asInt(0)).append("\n");
                if (node.has("parameters")) {
                    List<String> params = new ArrayList<>();
                    for (JsonNode p : node.path("parameters"))
                        params.add(p.path("name").asText("") + ":" + p.path("direction").asText(""));
                    sb.append("Parameters: ").append(String.join(", ", params)).append("\n");
                }
                String nodeId = node.path("nodeId").asText("");
                if (!nodeId.isBlank()) {
                    ArtifactKey nodeDetail = sessionDir.resolve("api", "nodes", nodeId + ".json");
                    if (parserArtifacts.exists(nodeDetail)) {
                        JsonNode detail = readTree(parserArtifacts, nodeDetail);
                        String sourceFile = detail.path("sourceFile").asText("");
                        if (!sourceFile.isBlank()) {
                            ArtifactKey src = sessionDir.resolve("sources", fileNameOf(sourceFile));
                            if (parserArtifacts.exists(src)) {
                                List<String> lines = readString(parserArtifacts, src).lines().collect(Collectors.toList());
                                int limit = Math.min(maxSourceLines, lines.size());
                                sb.append("Source (first ").append(limit).append(" lines):\n```sql\n");
                                lines.stream().limit(limit).forEach(l -> sb.append(l).append("\n"));
                                sb.append("```\n");
                            }
                        }
                    }
                }
                return sb.toString();
            }
        } catch (Exception e) {
            log.debug("{} Cannot load Oracle proc detail for {}: {}", LOG_PREFIX, ref, e.getMessage());
        }
        return null;
    }

    // ── Source code retrieval (for side-by-side viewer) ───────────────────────

    /**
     * Load full Java source for a node from call-tree nodes directory.
     * Returns map with sourceCode, className, methodName, signature, annotations.
     */
    public Map<String, Object> getJavaSource(String jarId, String crossRefId, String javaNodeId) {
        CrossRefResult result = store.readResult(store.runDir(jarId, crossRefId));
        if (result == null) return Map.of("error", "result not found");

        // Find the endpoint index from the crossref meta to locate the nodes directory
        ArtifactKey metaFile = store.metaFile(store.runDir(jarId, crossRefId));
        String endpointPath = null;
        try {
            if (metaFile != null && artifacts.exists(metaFile)) {
                try (InputStream in = openRead(metaFile)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> meta = objectMapper.readValue(in, Map.class);
                    endpointPath = (String) meta.get("endpointPath");
                }
            }
        } catch (Exception e) { log.debug("Cannot read meta: {}", e.getMessage()); }

        // Strip .jar suffix — endpoints are stored under the base name without extension
        String effectiveJarId = jarId.endsWith(".jar")
                ? jarId.substring(0, jarId.length() - 4) : jarId;
        ArtifactKey jarDir = jarDataPaths.endpointsDirKey(effectiveJarId);

        // Search all endpoint dirs for the node
        try {
            for (ArtifactKey epDir : childDirs(jarDir)) {
                ArtifactKey nodesDir = epDir.resolve("nodes");
                for (ArtifactKey nodeFile : childFiles(nodesDir)) {
                    if (!nodeFile.leaf().endsWith(".json")) continue;
                    try {
                        JsonNode n = readTree(nodeFile);
                        String cls = n.path("simpleClassName").asText(
                                n.path("className").asText("").replaceAll(".*\\.", ""));
                        String meth = n.path("methodName").asText("");
                        String key = cls + "#" + meth;
                        if (!key.equals(javaNodeId)) continue;
                        Map<String, Object> r = new LinkedHashMap<>();
                        r.put("nodeId", javaNodeId);
                        r.put("className", n.path("className").asText(""));
                        r.put("simpleClassName", cls);
                        r.put("methodName", meth);
                        r.put("signature", n.path("signature").asText(""));
                        r.put("stereotype", n.path("stereotype").asText(""));
                        r.put("sourceCode", n.path("sourceCode").asText(""));
                        r.put("annotations", n.path("annotations"));
                        r.put("collectionsAccessed", n.path("collectionsAccessed"));
                        r.put("operationType", n.path("operationType").asText(""));
                        return r;
                    } catch (Exception ignored) {}
                }
            }
        } catch (IOException e) {
            log.debug("getJavaSource error: {}", e.getMessage());
        }
        return Map.of("error", "node not found: " + javaNodeId);
    }

    /**
     * Load full Oracle SQL source for a proc ref from the PL/SQL session.
     * Reads sourceFile from index.json (most reliable path) with fallback to node detail file.
     */
    public Map<String, Object> getOracleSource(String jarId, String crossRefId, String procRef) {
        CrossRefResult result = store.readResult(store.runDir(jarId, crossRefId));
        if (result == null) return Map.of("error", "result not found");
        String sessionId = result.getPlsqlSessionId();
        if (sessionId == null) return Map.of("error", "no plsql session");
        return getOracleSourceBySession(sessionId, procRef);
    }

    private Map<String, Object> getOracleSourceBySession(String sessionId, String procRef) {
        ArtifactKey sessionDir = plsqlSessionKey(sessionId);
        if (sessionDir == null) return Map.of("error", "session index not found");
        ArtifactKey indexFile = sessionDir.resolve("api", "index.json");
        if (!parserArtifacts.exists(indexFile)) return Map.of("error", "session index not found");

        try {
            JsonNode idx = readTree(parserArtifacts, indexFile);
            String refUpper = procRef.toUpperCase();
            for (JsonNode node : idx.path("nodes")) {
                String pkg = node.path("packageName").asText("");
                String obj = node.path("objectName").asText("");
                String fullRef = (pkg.isBlank() ? obj : pkg + "." + obj).toUpperCase();
                if (!fullRef.equals(refUpper)) continue;

                Map<String, Object> r = new LinkedHashMap<>();
                r.put("procRef", procRef);
                r.put("packageName", pkg);
                r.put("objectName", obj);
                r.put("objectType", node.path("objectType").asText(""));
                r.put("depth", node.path("depth").asInt(0));
                r.put("linesOfCode", node.path("linesOfCode").asInt(0));
                r.put("name", node.path("name").asText(""));

                // Tables — from index counts node or node detail
                List<String> tables = new ArrayList<>();
                for (JsonNode t : node.path("tables")) tables.add(t.asText());
                r.put("tables", tables);

                // Parameters — from index node
                List<Map<String, String>> params = new ArrayList<>();
                for (JsonNode p : node.path("parameters")) {
                    Map<String, String> pm = new LinkedHashMap<>();
                    pm.put("name", p.path("name").asText(""));
                    pm.put("direction", p.path("direction").asText(""));
                    pm.put("type", p.path("dataType").asText(""));
                    params.add(pm);
                }
                r.put("parameters", params);

                // Source SQL — try index.node.sourceFile first (most reliable),
                // then fall back to node detail file
                String sourceFile = node.path("sourceFile").asText("");
                if (!sourceFile.isBlank()) {
                    ArtifactKey src = sessionDir.resolve("sources", fileNameOf(sourceFile));
                    if (parserArtifacts.exists(src)) {
                        r.put("source", readString(parserArtifacts, src));
                        r.put("sourceFile", sourceFile);
                        return r;
                    }
                }
                // Fallback: load from node detail file
                String nodeId = node.path("nodeId").asText("");
                if (!nodeId.isBlank()) {
                    ArtifactKey nodeDetail = sessionDir.resolve("api", "nodes", nodeId + ".json");
                    if (parserArtifacts.exists(nodeDetail)) {
                        JsonNode detail = readTree(parserArtifacts, nodeDetail);
                        String detailSrcFile = detail.path("sourceFile").asText("");
                        if (!detailSrcFile.isBlank()) {
                            ArtifactKey src = sessionDir.resolve("sources", fileNameOf(detailSrcFile));
                            if (parserArtifacts.exists(src)) {
                                r.put("source", readString(parserArtifacts, src));
                                r.put("sourceFile", detailSrcFile);
                                return r;
                            }
                        }
                        // Last resort: try CUSTOMER.PKG.OBJ.sql (individual proc file)
                        if (!obj.isBlank()) {
                            for (String prefix : List.of("CUSTOMER.", "")) {
                                ArtifactKey procFile = sessionDir.resolve(
                                        "sources", prefix + pkg + "." + obj + ".sql");
                                if (parserArtifacts.exists(procFile)) {
                                    r.put("source", readString(parserArtifacts, procFile));
                                    r.put("sourceFile", procFile.leaf());
                                    return r;
                                }
                            }
                        }
                    }
                }
                // Package-level node — list all CUSTOMER.{PKG}.{PROC}.sql files
                if (!pkg.isBlank()) {
                    ArtifactKey sourcesDir = sessionDir.resolve("sources");
                    String pkgUpper = pkg.toUpperCase();
                    try {
                        List<ArtifactKey> pkgFiles = childFiles(parserArtifacts, sourcesDir).stream()
                            .filter(k -> k.leaf().toUpperCase()
                                    .startsWith("CUSTOMER." + pkgUpper + "."))
                            .sorted(Comparator.comparing(ArtifactKey::leaf))
                            .collect(Collectors.toList());
                        if (!pkgFiles.isEmpty()) {
                            StringBuilder combined = new StringBuilder();
                            combined.append("-- Package: ").append(pkg).append(" (").append(pkgFiles.size()).append(" objects)\n\n");
                            for (ArtifactKey pf : pkgFiles) {
                                combined.append("-- ─── ").append(pf.leaf()).append(" ───\n");
                                combined.append(readString(parserArtifacts, pf)).append("\n\n");
                            }
                            r.put("source", combined.toString());
                            r.put("sourceFile", "Package " + pkg + " (" + pkgFiles.size() + " files)");
                            return r;
                        }
                    } catch (IOException ignored) {}
                }
                r.put("source", "-- SQL source file not found on disk.");
                return r;
            }
        } catch (Exception e) {
            log.debug("getOracleSource error for {}: {}", procRef, e.getMessage());
        }
        return Map.of("error", "proc not found: " + procRef);
    }

    /**
     * Return all Oracle nodes in this cross-ref session as a tree structure
     * (flat list with depth/parentNodeId so the UI can render it as a tree).
     */
    public Map<String, Object> getOracleTree(String jarId, String crossRefId) {
        CrossRefResult result = store.readResult(store.runDir(jarId, crossRefId));
        if (result == null) return Map.of("error", "result not found", "nodes", List.of());
        String sessionId = result.getPlsqlSessionId();
        if (sessionId == null) return Map.of("error", "no plsql session", "nodes", List.of());

        ArtifactKey sessionDir = plsqlSessionKey(sessionId);
        if (sessionDir == null) return Map.of("error", "session index not found", "nodes", List.of());
        ArtifactKey indexFile = sessionDir.resolve("api", "index.json");
        if (!parserArtifacts.exists(indexFile)) return Map.of("error", "session index not found", "nodes", List.of());

        try {
            JsonNode idx = readTree(parserArtifacts, indexFile);
            List<Map<String, Object>> nodes = new ArrayList<>();
            for (JsonNode node : idx.path("nodes")) {
                Map<String, Object> n = new LinkedHashMap<>();
                String pkg = node.path("packageName").asText("");
                String obj = node.path("objectName").asText("");
                n.put("nodeId", node.path("nodeId").asText(""));
                n.put("name", node.path("name").asText(""));
                n.put("packageName", pkg.isBlank() ? null : pkg);
                n.put("objectName", obj.isBlank() ? null : obj);
                n.put("objectType", node.path("objectType").asText(""));
                n.put("depth", node.path("depth").asInt(0));
                n.put("parentNodeId", node.path("parentNodeId").isNull() ? null : node.path("parentNodeId").asText(null));
                n.put("linesOfCode", node.path("linesOfCode").asInt(0));
                // Collect table names only (not full objects) for compact display
                List<String> tables = new ArrayList<>();
                for (JsonNode t : node.path("tables")) {
                    String tname = t.isTextual() ? t.asText() : t.path("name").asText("");
                    if (!tname.isBlank()) tables.add(tname);
                }
                n.put("tables", tables);
                // Collect parameter names for tooltip
                List<String> paramNames = new ArrayList<>();
                for (JsonNode p : node.path("parameters")) {
                    String pname = p.path("name").asText("");
                    if (!pname.isBlank()) paramNames.add(pname);
                }
                n.put("parameterNames", paramNames);
                nodes.add(n);
            }
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("nodes", nodes);
            resp.put("sessionId", sessionId);
            resp.put("entryPoint", idx.path("entryPoint").asText(""));
            resp.put("totalNodes", nodes.size());
            return resp;
        } catch (Exception e) {
            log.debug("getOracleTree error: {}", e.getMessage());
            return Map.of("error", e.getMessage(), "nodes", List.of());
        }
    }

    // ── Claude CLI runner ─────────────────────────────────────────────────────

    private String runClaude(String prompt) {
        try {
            ProcessBuilder pb = new ProcessBuilder("claude", "-p", "--no-session-persistence");
            pb.environment().putAll(System.getenv());
            pb.redirectErrorStream(false);
            Process process = pb.start();

            byte[] bytes = prompt.getBytes(StandardCharsets.UTF_8);
            Thread stdinWriter = new Thread(() -> {
                try (OutputStream os = process.getOutputStream();
                     BufferedOutputStream bos = new BufferedOutputStream(os, 65536)) {
                    for (int off = 0; off < bytes.length; ) {
                        int len = Math.min(8192, bytes.length - off);
                        bos.write(bytes, off, len);
                        off += len;
                    }
                    bos.flush();
                } catch (IOException ignored) {}
            });
            stdinWriter.setDaemon(true);
            stdinWriter.start();

            StringBuilder stdout = new StringBuilder();
            StringBuilder stderr = new StringBuilder();
            Thread stdoutReader = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) stdout.append(line).append("\n");
                } catch (IOException ignored) {}
            });
            Thread stderrReader = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(
                        new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) stderr.append(line).append("\n");
                } catch (IOException ignored) {}
            });
            stdoutReader.setDaemon(true);
            stderrReader.setDaemon(true);
            stdoutReader.start();
            stderrReader.start();

            boolean finished = process.waitFor(600, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                log.warn("{} Claude timed out", LOG_PREFIX);
                return null;
            }
            stdoutReader.join(30_000);
            stderrReader.join(5_000);

            String result = stdout.toString().trim();
            if (result.isEmpty()) {
                log.warn("{} Claude empty stdout. stderr={}", LOG_PREFIX,
                        stderr.length() > 200 ? stderr.substring(0, 200) : stderr);
                return null;
            }
            return result;

        } catch (Exception e) {
            log.error("{} Claude CLI error: {}", LOG_PREFIX, e.getMessage());
            return null;
        }
    }

    // ── JSON parsing ──────────────────────────────────────────────────────────

    /** Converts a JSON array of objects into a plain List&lt;Map&gt; for typed-field storage. */
    private List<Map<String, Object>> jsonArrayToMapList(JsonNode arr) {
        List<Map<String, Object>> list = new ArrayList<>();
        if (arr == null || !arr.isArray()) return list;
        for (JsonNode item : arr) {
            if (!item.isObject()) continue;
            try {
                list.add(objectMapper.convertValue(item,
                        new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));
            } catch (Exception ignored) {}
        }
        return list;
    }

    private JsonNode parseClaudeJson(String response) {
        // Try to extract JSON block from response (Claude sometimes wraps in markdown)
        String json = response.trim();
        int start = json.indexOf('{');
        int end = json.lastIndexOf('}');
        if (start >= 0 && end > start) json = json.substring(start, end + 1);
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            log.debug("{} JSON parse failed: {}", LOG_PREFIX, e.getMessage());
            return null;
        }
    }

    // ── Checkpoint / file management ──────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Set<String> loadCheckpoint(ArtifactKey claudeDir) {
        if (claudeDir == null) return Collections.synchronizedSet(new HashSet<>());
        ArtifactKey f = claudeDir.resolve("checkpoint.json");
        if (!artifacts.exists(f)) return Collections.synchronizedSet(new HashSet<>());
        try (InputStream in = openRead(f)) {
            Set<String> set = objectMapper.readValue(in, Set.class);
            return Collections.synchronizedSet(new HashSet<>(set));
        } catch (Exception e) {
            return Collections.synchronizedSet(new HashSet<>());
        }
    }

    private void addToCheckpoint(ArtifactKey claudeDir, Set<String> checkpoint, String chunkId) {
        checkpoint.add(chunkId);
        if (claudeDir == null) return;
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(checkpoint);
            try (OutputStream out = openWrite(claudeDir.resolve("checkpoint.json"), bytes.length)) {
                out.write(bytes);
            }
        } catch (IOException e) {
            log.debug("{} Cannot write checkpoint: {}", LOG_PREFIX, e.getMessage());
        }
    }

    private void writeChunkResult(ArtifactKey claudeDir, String chunkId, JsonNode node) {
        if (claudeDir == null) return;
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(node);
            try (OutputStream out = openWrite(claudeDir.resolve(chunkId + ".json"), bytes.length)) {
                out.write(bytes);
            }
        } catch (IOException e) {
            log.debug("{} Cannot write chunk {}: {}", LOG_PREFIX, chunkId, e.getMessage());
        }
    }

    private JsonNode readChunkResult(ArtifactKey claudeDir, String chunkId) {
        if (claudeDir == null) return null;
        ArtifactKey f = claudeDir.resolve(chunkId + ".json");
        if (!artifacts.exists(f)) return null;
        try {
            return readTree(f);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * {@code jar/crossref/<jarId>/<crossRefId>/claude} — the per-run chunk cache.
     *
     * <p>Derived from {@link CrossRefStore#runDir} rather than rebuilt here, so the layout stays
     * owned by CrossRefStore and the two cannot drift apart.
     */
    private ArtifactKey claudeDirKey(String jarId, String crossRefId) {
        return store.runDir(jarId, crossRefId).resolve("claude");
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static String stableHash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) sb.append(String.format("%02x", digest[i]));
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(input.hashCode());
        }
    }

    private String progressJson(int done, int total, String status, int converted, int partial, int notConverted) {
        try {
            return objectMapper.writeValueAsString(progressMap(done, total, status, converted, partial, notConverted));
        } catch (Exception e) {
            return "{}";
        }
    }

    private Map<String, Object> progressMap(int done, int total, String status,
                                              int converted, int partial, int notConverted) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("completed", done);
        m.put("total", total);
        m.put("status", status);
        m.put("verdicts", Map.of("CONVERTED", converted, "PARTIAL", partial, "NOT_CONVERTED", notConverted));
        return m;
    }

    /**
     * After each chunk completes, immediately write its verdict into result.json so the
     * Discussion tab can show it even if the overall run is still in progress or the page
     * was refreshed mid-run. Uses a per-crossRefId lock to prevent concurrent write races.
     */
    private void flushChunkToResultJson(String jarId, String crossRefId, String chunkId, JsonNode chunkNode) {
        Object lock = resultWriteLocks.computeIfAbsent(crossRefId, k -> new Object());
        synchronized (lock) {
            try {
                ArtifactKey runDir = store.runDir(jarId, crossRefId);
                CrossRefResult saved = store.readResult(runDir);
                if (saved == null) return;

                String verdict    = chunkNode.path("verdict").asText("");
                String confidence = chunkNode.path("confidence").asText("");
                String reason     = chunkNode.path("reason").asText("");
                List<String> gaps = new ArrayList<>();
                for (JsonNode g : chunkNode.path("gaps")) gaps.add(g.asText());
                String type = chunkNode.path("type").asText("");
                List<Map<String, Object>> aggVerification = jsonArrayToMapList(chunkNode.path("aggregationVerification"));
                List<Map<String, Object>> dynVerification = jsonArrayToMapList(chunkNode.path("dynamicDispatchVerification"));
                List<Map<String, Object>> extVerification = jsonArrayToMapList(chunkNode.path("externalDependencyVerification"));

                // Match chunk back to its item by re-deriving chunkId and comparing
                String plsqlSessionId = saved.getPlsqlSessionId() != null ? saved.getPlsqlSessionId() : "";

                if ("OVERALL".equals(type)) {
                    saved.setClaudeOverallVerdict(verdict);
                    saved.setClaudeOverallConfidence(confidence);
                    saved.setClaudeOverallReason(reason);
                    saved.setClaudeOverallGaps(gaps);
                    saved.setClaudeConversionPct(chunkNode.path("conversionPct").asInt(0));
                    String overallId = stableHash("OVERALL|" + crossRefId + "|" + plsqlSessionId);
                    saved.setClaudeOverallChunkId(overallId);
                } else if ("PROC_LINK".equals(type) && saved.getProcLinks() != null) {
                    for (CrossRefProcLink link : saved.getProcLinks()) {
                        if (link.isClaudeDone()) continue;
                        String javaKey  = link.getJavaNodeId() != null ? link.getJavaNodeId()
                                : (link.getJavaClass() + "#" + link.getJavaMethod());
                        String oracleRef = link.getResolvedProcRef() != null ? link.getResolvedProcRef() : "";
                        String cid = stableHash("LINK|" + javaKey + "|" + oracleRef + "|" + plsqlSessionId);
                        if (cid.equals(chunkId)) {
                            link.setClaudeVerdict(verdict);
                            link.setClaudeConfidence(confidence);
                            link.setClaudeReason(reason);
                            link.setClaudeGaps(gaps);
                            link.setClaudeChunkId(chunkId);
                            link.setClaudeDone(true);
                            link.setClaudeAggregationVerification(aggVerification);
                            link.setClaudeDynamicDispatchVerification(dynVerification);
                            link.setClaudeExternalDependencyVerification(extVerification);
                            break;
                        }
                    }
                } else if ("JAVA_ONLY".equals(type) && saved.getJavaOnlyProcs() != null) {
                    for (CrossRefResult.JavaOnlyProc jp : saved.getJavaOnlyProcs()) {
                        if (jp.isClaudeDone()) continue;
                        String key = (jp.getJavaClass() != null ? jp.getJavaClass() : "") + "#"
                                + (jp.getJavaMethod() != null ? jp.getJavaMethod() : "");
                        String cid = stableHash("JAVA_ONLY|" + key + "|" + plsqlSessionId);
                        if (cid.equals(chunkId)) {
                            jp.setClaudeVerdict(verdict);
                            jp.setClaudeConfidence(confidence);
                            jp.setClaudeReason(reason);
                            jp.setClaudeChunkId(chunkId);
                            jp.setClaudeDone(true);
                            jp.setClaudeAggregationVerification(aggVerification);
                            jp.setClaudeDynamicDispatchVerification(dynVerification);
                            jp.setClaudeExternalDependencyVerification(extVerification);
                            break;
                        }
                    }
                } else if ("ORACLE_ONLY".equals(type) && saved.getOracleOnlyProcs() != null) {
                    for (CrossRefResult.OracleOnlyProc op : saved.getOracleOnlyProcs()) {
                        if (op.isClaudeDone()) continue;
                        String key = op.getProcRef() != null ? op.getProcRef() : "";
                        String cid = stableHash("ORACLE_ONLY|" + key + "|" + plsqlSessionId);
                        if (cid.equals(chunkId)) {
                            op.setClaudeVerdict(verdict);
                            op.setClaudeConfidence(confidence);
                            op.setClaudeReason(reason);
                            op.setClaudeChunkId(chunkId);
                            op.setClaudeDone(true);
                            break;
                        }
                    }
                }
                store.writeResult(runDir, saved);
                log.debug("{} flushed chunk={} type={} verdict={} to result.json", LOG_PREFIX, chunkId, type, verdict);
            } catch (Exception e) {
                log.debug("{} flushChunkToResultJson failed chunk={}: {}", LOG_PREFIX, chunkId, e.getMessage());
            }
        }
    }

    private void cancelExisting(String crossRefId) {
        AtomicBoolean old = cancelFlags.get(crossRefId);
        if (old != null) old.set(true);
        ExecutorService oldEx = runningExecutors.get(crossRefId);
        if (oldEx != null) { oldEx.shutdownNow(); runningExecutors.remove(crossRefId); }
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private static class ChunkWork {
        final ChunkType type;
        final String chunkId;
        final CrossRefResult.JavaOnlyProc javaOnly;
        final CrossRefResult.OracleOnlyProc oracleOnly;
        final CrossRefProcLink link;
        final Object unused1;
        final Object unused2;
        final CrossRefResult result;
        final String jarId;
        final String crossRefId;

        ChunkWork(ChunkType type, String chunkId,
                  CrossRefResult.JavaOnlyProc javaOnly, CrossRefResult.OracleOnlyProc oracleOnly,
                  CrossRefProcLink link, Object u1, Object u2,
                  CrossRefResult result, String jarId, String crossRefId) {
            this.type = type;
            this.chunkId = chunkId;
            this.javaOnly = javaOnly;
            this.oracleOnly = oracleOnly;
            this.link = link;
            this.unused1 = u1;
            this.unused2 = u2;
            this.result = result;
            this.jarId = jarId;
            this.crossRefId = crossRefId;
        }
    }
}
