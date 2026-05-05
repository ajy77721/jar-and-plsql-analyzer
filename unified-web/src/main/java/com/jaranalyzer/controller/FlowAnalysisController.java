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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Integrates flow-poc into the existing JAR Analyzer web app.
 *
 * Two modes — both triggered from the "Flow Analysis" sub-tab in Summary:
 *   testdata  — static call-tree walk + MongoDB sample fetch → test datasets
 *   optimize  — static analyzers (N+1, missing index, bulk, aggregation) + Layer 2 dynamic exec
 *
 * MongoDB connection and Claude API key are loaded automatically from what
 * the system already knows about the JAR (connections.json / ANTHROPIC_API_KEY env var).
 * Users can override them via the hidden "Override connection settings" panel.
 *
 * Routes (all under the existing /api/jar/jars base path):
 *   POST /{id}/flow/run          — start async run, returns { runId, status }
 *   GET  /{id}/flow/result/{rid} — poll for completion
 *   GET  /{id}/flow/report/{rid} — download .md report
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
        this.paths              = paths;
        this.persistenceService = persistenceService;
        this.objectMapper       = objectMapper;
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

                // Read saved connections — structure is { mongodb: { uri, database }, oracle: {...} }
                Map<String, Object> conns = persistenceService.loadConnections(id);
                String mongoUri = resolveMongoUri(req.mongoUri(), conns);
                String mongoDb  = resolveMongoDb(req.mongoDb(), conns);

                // Claude API key: use explicit override or fall back to ANTHROPIC_API_KEY env var
                String claudeKey = (req.claudeApiKey() != null && !req.claudeApiKey().isBlank())
                        ? req.claudeApiKey()
                        : System.getenv("ANTHROPIC_API_KEY");

                PocConfig.Builder builder = PocConfig.builder()
                        .analysisJson(jsonPath.toAbsolutePath().toString())
                        .sampleSize(10)
                        .maxEndpoints(0); // we filter by endpointPaths below

                if (mongoUri != null && mongoDb != null && !mongoDb.isBlank())
                    builder.mongo(mongoUri, mongoDb);

                // "optimize" mode: enable Layer 2 dynamic execution against stored.jar
                if ("optimize".equals(req.mode())) {
                    Path storedJar = paths.storedJarFile(id);
                    if (Files.exists(storedJar))
                        builder.layer2(storedJar.toAbsolutePath().toString());
                }

                if (claudeKey != null && !claudeKey.isBlank())
                    builder.claude(claudeKey);

                PocConfig config = builder.build();

                // 1. Walk all endpoints, filter to the ones the user selected
                Set<String> selected = req.endpointPaths() != null
                        ? Set.copyOf(req.endpointPaths()) : Set.of();

                // 2. Run full analysis (JSON output)
                ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
                runFiltered(config, selected, jsonOut);
                String jsonReport = jsonOut.toString(StandardCharsets.UTF_8);

                // 3. Static pass for Markdown report (cheap — re-walks analysis.json, no dynamic exec)
                String mdReport = generateMarkdown(config, selected);

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

    // ── Download .md ──────────────────────────────────────────────────────────

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

    /**
     * Runs PocRunner but only keeps FlowResults whose endpoint path is in `selected`.
     * When `selected` is empty, all endpoints are included.
     */
    private void runFiltered(PocConfig config, Set<String> selected, ByteArrayOutputStream out) throws Exception {
        if (selected.isEmpty()) {
            new PocRunner(config).run(out);
            return;
        }
        // PocRunner doesn't support path-level filtering, so we run it with maxEndpoints=0
        // and rely on a thin wrapper that filters by endpoint path before writing the report.
        // Workaround: run full, then filter the JSON result.
        ByteArrayOutputStream fullOut = new ByteArrayOutputStream();
        new PocRunner(config).run(fullOut);
        String fullJson = fullOut.toString(StandardCharsets.UTF_8);

        // Filter the JSON array to only matching endpoints
        try {
            List<?> all = objectMapper.readValue(fullJson, List.class);
            List<?> filtered = all.stream()
                    .filter(item -> {
                        if (item instanceof Map<?,?> m) {
                            String ep = String.valueOf(m.get("endpoint"));
                            return selected.stream().anyMatch(ep::contains);
                        }
                        return false;
                    })
                    .collect(Collectors.toList());
            objectMapper.writeValue(out, filtered);
        } catch (Exception e) {
            // If filtering fails, return the full result
            out.write(fullJson.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String generateMarkdown(PocConfig config, Set<String> selected) {
        try {
            var walker = new FlowWalker(List.of(
                    new MongoOperationVisitor(),
                    new SqlOperationVisitor(new SqlPredicateExtractor())));
            List<FlowResult> results = walker.walk(new File(config.getAnalysisJsonPath()));

            // Filter to selected endpoints (empty = all)
            if (!selected.isEmpty()) {
                results = results.stream()
                        .filter(r -> selected.stream().anyMatch(r.getEndpointPath()::contains))
                        .collect(Collectors.toList());
            }

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

    /** Read MongoDB URI from saved connections.json (structure: mongodb.uri) or explicit override. */
    @SuppressWarnings("unchecked")
    private static String resolveMongoUri(String explicit, Map<String, Object> conns) {
        if (explicit != null && !explicit.isBlank()) return explicit;
        if (conns == null) return null;
        Object mongo = conns.get("mongodb");
        if (mongo instanceof Map<?,?> m) {
            Object uri = m.get("uri");
            if (uri instanceof String s && !s.isBlank() && !s.contains("***")) return s;
        }
        return null;
    }

    /** Read MongoDB database name from saved connections.json (structure: mongodb.database). */
    @SuppressWarnings("unchecked")
    private static String resolveMongoDb(String explicit, Map<String, Object> conns) {
        if (explicit != null && !explicit.isBlank()) return explicit;
        if (conns == null) return null;
        Object mongo = conns.get("mongodb");
        if (mongo instanceof Map<?,?> m) {
            Object db = m.get("database");
            if (db instanceof String s && !s.isBlank()) return s;
        }
        return null;
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    public record FlowRunRequest(
            String       mode,           // "testdata" | "optimize"
            List<String> endpointPaths,  // paths selected in the UI; empty = all
            String       mongoUri,       // optional override
            String       mongoDb,        // optional override
            String       claudeApiKey    // optional override (falls back to ANTHROPIC_API_KEY env)
    ) {}

    private record FlowRunResult(String status, String jsonReport, String mdReport, String error) {}
}
