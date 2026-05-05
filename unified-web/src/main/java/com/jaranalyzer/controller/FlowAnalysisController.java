package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.PocRunner;
import com.flowpoc.analyzer.AnalyzerPipeline;
import com.flowpoc.config.PocConfig;
import com.flowpoc.engine.FlowWalker;
import com.flowpoc.engine.SqlPredicateExtractor;
import com.flowpoc.engine.visitor.MongoOperationVisitor;
import com.flowpoc.engine.visitor.SqlOperationVisitor;
import com.flowpoc.model.FlowResult;
import com.flowpoc.report.MarkdownReporter;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.PersistenceService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exposes the flow-poc pipeline (test-data prep + optimization analysis) directly
 * inside the existing JAR Analyzer web app.
 *
 * The two modes share one endpoint — the caller picks "testdata" or "optimize":
 *
 *   POST /api/jar/jars/{id}/flow/run
 *        { "mode": "testdata"|"optimize", "mongoUri": "...", "mongoDb": "...",
 *          "claudeApiKey": "...", "maxEndpoints": 10 }
 *
 *   GET  /api/jar/jars/{id}/flow/result/{runId}
 *   GET  /api/jar/jars/{id}/flow/report/{runId}   ← .md download
 */
@RestController
@RequestMapping("/api/jar/jars")
public class FlowAnalysisController {

    private final JarDataPaths paths;
    private final PersistenceService persistenceService;
    private final ObjectMapper objectMapper;

    private final ConcurrentHashMap<String, FlowRunResult> runs = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(1);

    public FlowAnalysisController(JarDataPaths paths,
                                  PersistenceService persistenceService,
                                  ObjectMapper objectMapper) {
        this.paths             = paths;
        this.persistenceService = persistenceService;
        this.objectMapper      = objectMapper;
    }

    // ── Trigger ───────────────────────────────────────────────────────────────

    @PostMapping("/{id}/flow/run")
    public Map<String, Object> run(@PathVariable String id, @RequestBody FlowRunRequest req) {
        String runId = String.valueOf(counter.getAndIncrement());
        runs.put(runId, new FlowRunResult("running", null, null, null));

        new Thread(() -> {
            try {
                Path jsonPath = paths.analysisFile(id);
                if (!Files.exists(jsonPath)) {
                    runs.put(runId, new FlowRunResult("error", null, null,
                            "analysis.json not found — please analyze the JAR first"));
                    return;
                }

                // Pre-fill mongo connection from saved connections.json if not supplied
                Map<String, Object> conns = persistenceService.loadConnections(id);
                String mongoUri = resolve(req.mongoUri(), conns, "mongoUri", "mongodb://localhost:27017");
                String mongoDb  = resolve(req.mongoDb(),  conns, "mongoDb",  null);

                PocConfig.Builder builder = PocConfig.builder()
                        .analysisJson(jsonPath.toAbsolutePath().toString())
                        .sampleSize(10)
                        .maxEndpoints(req.maxEndpoints());

                if (mongoUri != null && mongoDb != null && !mongoDb.isBlank())
                    builder.mongo(mongoUri, mongoDb);

                // "optimize" mode: enable Layer 2 dynamic execution using stored.jar
                if ("optimize".equals(req.mode())) {
                    Path storedJar = paths.storedJarFile(id);
                    if (Files.exists(storedJar))
                        builder.layer2(storedJar.toAbsolutePath().toString());
                }

                if (req.claudeApiKey() != null && !req.claudeApiKey().isBlank())
                    builder.claude(req.claudeApiKey());

                PocConfig config = builder.build();

                // 1. Run full analysis → JSON
                ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
                new PocRunner(config).run(jsonOut);
                String jsonReport = jsonOut.toString(StandardCharsets.UTF_8);

                // 2. Static-only pass → Markdown report (cheap — no dynamic exec)
                String mdReport = generateMarkdown(config);

                runs.put(runId, new FlowRunResult("done", jsonReport, mdReport, null));

            } catch (Exception e) {
                runs.put(runId, new FlowRunResult("error", null, null, e.getMessage()));
            }
        }).start();

        return Map.of("runId", runId, "status", "running");
    }

    // ── Poll ──────────────────────────────────────────────────────────────────

    @GetMapping("/{id}/flow/result/{runId}")
    public ResponseEntity<Map<String, Object>> result(@PathVariable String id,
                                                       @PathVariable String runId) {
        FlowRunResult r = runs.get(runId);
        if (r == null) return ResponseEntity.notFound().build();

        Map<String, Object> resp = new java.util.LinkedHashMap<>();
        resp.put("status", r.status());
        if ("done".equals(r.status()) && r.jsonReport() != null) {
            try {
                resp.put("results", objectMapper.readValue(r.jsonReport(), Object.class));
            } catch (Exception e) {
                resp.put("results", r.jsonReport());
            }
        }
        if ("error".equals(r.status())) resp.put("error", r.error());
        return ResponseEntity.ok(resp);
    }

    // ── Download report ───────────────────────────────────────────────────────

    @GetMapping("/{id}/flow/report/{runId}")
    public ResponseEntity<byte[]> report(@PathVariable String id,
                                          @PathVariable String runId) {
        FlowRunResult r = runs.get(runId);
        if (r == null || r.mdReport() == null) return ResponseEntity.notFound().build();

        byte[] bytes = r.mdReport().getBytes(StandardCharsets.UTF_8);
        String filename = id.replaceAll("[^a-zA-Z0-9._-]", "_") + "-flow-analysis.md";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType("text/markdown; charset=UTF-8"))
                .body(bytes);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String generateMarkdown(PocConfig config) {
        try {
            var walker = new FlowWalker(List.of(
                    new MongoOperationVisitor(),
                    new SqlOperationVisitor(new SqlPredicateExtractor())));
            List<FlowResult> results = walker.walk(new File(config.getAnalysisJsonPath()));

            if (config.getMaxEndpoints() > 0 && results.size() > config.getMaxEndpoints())
                results = results.subList(0, config.getMaxEndpoints());

            var pipeline = config.isClaudeEnabled()
                    ? AnalyzerPipeline.builder().withMongoDefaultsAndClaude(config.getClaudeApiKey()).build()
                    : AnalyzerPipeline.builder().withMongoDefaults().build();
            results.forEach(pipeline::runAndAttach);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            new MarkdownReporter().write(results, out);
            return out.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "# Error generating report\n" + e.getMessage();
        }
    }

    @SuppressWarnings("unchecked")
    private static String resolve(String explicit, Map<String, Object> conns,
                                   String connKey, String fallback) {
        if (explicit != null && !explicit.isBlank()) return explicit;
        if (conns != null && conns.get(connKey) instanceof String s && !s.isBlank()) return s;
        return fallback;
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    public record FlowRunRequest(
            String mode,          // "testdata" | "optimize"
            String mongoUri,
            String mongoDb,
            String claudeApiKey,
            int    maxEndpoints
    ) {}

    private record FlowRunResult(String status, String jsonReport, String mdReport, String error) {}
}
