package com.flowpoc.web;

import com.flowpoc.PocRunner;
import com.flowpoc.config.PocConfig;
import com.flowpoc.report.MarkdownReporter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * REST controller that exposes the flow analysis pipeline as an HTTP endpoint.
 *
 * Endpoints:
 *
 *   GET  /api/analysis/ui
 *        Returns an HTML page with a form + "Run Analysis" button.
 *        Paste this URL in a browser to trigger analysis interactively.
 *
 *   POST /api/analysis/run
 *        JSON body: { "analysisJson": "...", "mongoUri": "...", "mongoDb": "...",
 *                     "jarPath": "...", "claudeApiKey": "...", "maxEndpoints": 5 }
 *        Returns: { "id": "...", "status": "running" }
 *        Analysis runs asynchronously.
 *
 *   GET  /api/analysis/result/{id}
 *        Returns the completed analysis as JSON (same schema as PocRunner output).
 *
 *   GET  /api/analysis/report/{id}
 *        Returns the Markdown report as text/markdown (save as .md).
 *
 *   GET  /api/analysis/summary
 *        Returns a lightweight summary (finding counts by severity) for all
 *        completed analyses — suitable for a dashboard widget.
 *
 * To register this controller in your Spring Boot app, add a @ComponentScan
 * for "com.flowpoc.web" or import it explicitly:
 *   @Import(AnalysisController.class)
 */
@RestController
@RequestMapping("/api/analysis")
@ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
public class AnalysisController {

    // In-memory result store keyed by run ID (replace with Redis/DB for production)
    private final ConcurrentHashMap<String, RunResult> results = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(1);

    // ── UI page ───────────────────────────────────────────────────────────────

    @GetMapping(value = "/ui", produces = MediaType.TEXT_HTML_VALUE)
    public String ui() {
        return """
            <!DOCTYPE html>
            <html lang="en">
            <head>
              <meta charset="UTF-8">
              <title>Flow Analysis</title>
              <style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                       max-width: 900px; margin: 40px auto; padding: 0 20px; background: #f5f5f5; }
                h1 { color: #1a1a2e; }
                .card { background: white; border-radius: 8px; padding: 24px;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin-bottom: 20px; }
                label { display: block; font-weight: 600; margin: 12px 0 4px; color: #333; }
                input, select { width: 100%; padding: 8px 12px; border: 1px solid #ddd;
                                border-radius: 4px; font-size: 14px; box-sizing: border-box; }
                button { background: #4f46e5; color: white; border: none; padding: 12px 28px;
                         border-radius: 6px; font-size: 15px; cursor: pointer; margin-top: 16px; }
                button:hover { background: #4338ca; }
                button:disabled { background: #9ca3af; cursor: not-allowed; }
                .badge-high   { background:#fee2e2; color:#dc2626; padding:2px 8px; border-radius:12px; }
                .badge-medium { background:#fef3c7; color:#d97706; padding:2px 8px; border-radius:12px; }
                .badge-low    { background:#d1fae5; color:#059669; padding:2px 8px; border-radius:12px; }
                #results { display:none; }
                #findings-table { width:100%; border-collapse:collapse; }
                #findings-table th, #findings-table td { padding:10px 14px; text-align:left;
                       border-bottom:1px solid #e5e7eb; }
                #findings-table th { background:#f9fafb; font-weight:600; }
                pre { background:#1e1e2e; color:#cdd6f4; padding:16px; border-radius:6px;
                      overflow-x:auto; font-size:13px; }
                .spinner { display:inline-block; width:18px; height:18px; border:3px solid #e5e7eb;
                           border-top-color:#4f46e5; border-radius:50%; animation:spin 0.8s linear infinite; }
                @keyframes spin { to { transform:rotate(360deg); } }
              </style>
            </head>
            <body>
              <h1>🔍 Flow Analysis</h1>
              <p>Analyze an API endpoint call tree for MongoDB optimization opportunities,
                 static/transactional data classification, and test data generation.</p>

              <div class="card">
                <h2>Configuration</h2>

                <label>analysis.json path <small>(output from JAR Analyzer)</small></label>
                <input id="analysisJson" type="text" placeholder="/data/analysis.json">

                <label>MongoDB URI</label>
                <input id="mongoUri" type="text" value="mongodb://localhost:27017">

                <label>MongoDB Database</label>
                <input id="mongoDb" type="text" placeholder="myapp">

                <label>JAR file path <small>(optional — enables Layer 2 dynamic execution)</small></label>
                <input id="jarPath" type="text" placeholder="/apps/myapp.jar">

                <label>Anthropic API Key <small>(optional — enables Claude AI suggestions)</small></label>
                <input id="claudeKey" type="password" placeholder="sk-ant-...">

                <label>Max endpoints to analyze <small>(0 = all)</small></label>
                <input id="maxEndpoints" type="number" value="10">

                <button id="runBtn" onclick="runAnalysis()">▶ Run Analysis</button>
                <button id="downloadBtn" style="display:none;background:#059669;" onclick="downloadReport()">
                  ⬇ Download .md Report
                </button>
              </div>

              <div class="card" id="results">
                <h2>Results</h2>
                <div id="summary"></div>
                <div id="findings-container" style="margin-top:20px;"></div>
              </div>

              <script>
                let currentRunId = null;

                async function runAnalysis() {
                  const btn = document.getElementById('runBtn');
                  btn.disabled = true;
                  btn.innerHTML = '<span class="spinner"></span> Running...';
                  document.getElementById('results').style.display = 'none';
                  document.getElementById('downloadBtn').style.display = 'none';

                  const body = {
                    analysisJson:  document.getElementById('analysisJson').value,
                    mongoUri:      document.getElementById('mongoUri').value,
                    mongoDb:       document.getElementById('mongoDb').value,
                    jarPath:       document.getElementById('jarPath').value,
                    claudeApiKey:  document.getElementById('claudeKey').value,
                    maxEndpoints:  parseInt(document.getElementById('maxEndpoints').value) || 0
                  };

                  try {
                    const startResp = await fetch('/api/analysis/run', {
                      method: 'POST',
                      headers: {'Content-Type': 'application/json'},
                      body: JSON.stringify(body)
                    });
                    const start = await startResp.json();
                    currentRunId = start.id;
                    pollResult(currentRunId);
                  } catch(e) {
                    btn.disabled = false;
                    btn.innerHTML = '▶ Run Analysis';
                    alert('Error: ' + e.message);
                  }
                }

                async function pollResult(id) {
                  const resp = await fetch('/api/analysis/result/' + id);
                  const data = await resp.json();

                  if (data.status === 'running') {
                    setTimeout(() => pollResult(id), 2000);
                    return;
                  }

                  const btn = document.getElementById('runBtn');
                  btn.disabled = false;
                  btn.innerHTML = '▶ Run Analysis';

                  if (data.status === 'error') {
                    alert('Analysis failed: ' + data.error);
                    return;
                  }

                  renderResults(data.results);
                  document.getElementById('downloadBtn').style.display = 'inline-block';
                }

                function renderResults(results) {
                  document.getElementById('results').style.display = 'block';

                  // Summary counts
                  let high = 0, medium = 0, low = 0, endpoints = results.length;
                  results.forEach(r => (r.optimizations || []).forEach(f => {
                    if (f.severity === 'HIGH') high++;
                    else if (f.severity === 'MEDIUM') medium++;
                    else low++;
                  }));

                  document.getElementById('summary').innerHTML =
                    '<b>' + endpoints + '</b> endpoints &nbsp;'
                    + '<span class="badge-high">🔴 ' + high + ' HIGH</span> &nbsp;'
                    + '<span class="badge-medium">🟡 ' + medium + ' MEDIUM</span> &nbsp;'
                    + '<span class="badge-low">🟢 ' + low + ' LOW</span>';

                  // Findings table
                  let html = '<table id="findings-table"><thead><tr>'
                    + '<th>Severity</th><th>Category</th><th>Endpoint</th>'
                    + '<th>Collection</th><th>Description</th></tr></thead><tbody>';

                  results.forEach(r => {
                    (r.optimizations || []).forEach(f => {
                      const badge = f.severity === 'HIGH'
                        ? '<span class="badge-high">🔴 HIGH</span>'
                        : f.severity === 'MEDIUM'
                        ? '<span class="badge-medium">🟡 MEDIUM</span>'
                        : '<span class="badge-low">🟢 LOW</span>';
                      html += '<tr><td>' + badge + '</td>'
                        + '<td>' + (f.category||'').replace(/_/g,' ') + '</td>'
                        + '<td><code>' + (r.endpoint||'') + '</code></td>'
                        + '<td><code>' + (f.table||'-') + '</code></td>'
                        + '<td>' + (f.description||'') + '</td></tr>';
                    });
                  });
                  html += '</tbody></table>';
                  document.getElementById('findings-container').innerHTML = html;
                }

                async function downloadReport() {
                  if (!currentRunId) return;
                  const resp = await fetch('/api/analysis/report/' + currentRunId);
                  const text = await resp.text();
                  const blob = new Blob([text], { type: 'text/markdown' });
                  const a = document.createElement('a');
                  a.href = URL.createObjectURL(blob);
                  a.download = 'flow-analysis-report.md';
                  a.click();
                }
              </script>
            </body>
            </html>
            """;
    }

    // ── Trigger analysis ──────────────────────────────────────────────────────

    @PostMapping("/run")
    public Map<String, Object> run(@RequestBody AnalysisRequest req) {
        String id = String.valueOf(counter.getAndIncrement());
        results.put(id, new RunResult("running", null, null, null));

        new Thread(() -> {
            try {
                PocConfig.Builder builder = PocConfig.builder()
                        .analysisJson(req.analysisJson())
                        .mongo(req.mongoUri() != null ? req.mongoUri() : "mongodb://localhost:27017",
                               req.mongoDb())
                        .sampleSize(10)
                        .maxEndpoints(req.maxEndpoints());

                if (req.jarPath() != null && !req.jarPath().isBlank())
                    builder.layer2(req.jarPath());
                if (req.claudeApiKey() != null && !req.claudeApiKey().isBlank())
                    builder.claude(req.claudeApiKey());

                PocConfig config = builder.build();

                // Run JSON report
                ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
                new PocRunner(config).run(jsonOut);
                String jsonReport = jsonOut.toString(StandardCharsets.UTF_8);

                // Run Markdown report using same results
                ByteArrayOutputStream mdOut = new ByteArrayOutputStream();
                new PocRunner(config).run(mdOut);  // second run for MD (stateless)
                String mdReport = generateMarkdown(config);

                results.put(id, new RunResult("done", jsonReport, mdReport, null));
            } catch (Exception e) {
                results.put(id, new RunResult("error", null, null, e.getMessage()));
            }
        }).start();

        return Map.of("id", id, "status", "running");
    }

    @GetMapping("/result/{id}")
    public ResponseEntity<Map<String, Object>> result(@PathVariable String id) {
        RunResult r = results.get(id);
        if (r == null) return ResponseEntity.notFound().build();

        Map<String, Object> resp = new java.util.LinkedHashMap<>();
        resp.put("status", r.status());
        if ("done".equals(r.status()) && r.jsonReport() != null) {
            try {
                resp.put("results", new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(r.jsonReport(), Object.class));
            } catch (Exception e) {
                resp.put("results", r.jsonReport());
            }
        }
        if ("error".equals(r.status())) resp.put("error", r.error());
        return ResponseEntity.ok(resp);
    }

    @GetMapping(value = "/report/{id}", produces = "text/markdown")
    public ResponseEntity<String> report(@PathVariable String id) {
        RunResult r = results.get(id);
        if (r == null || r.mdReport() == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok()
                .header("Content-Disposition", "attachment; filename=flow-analysis-report.md")
                .body(r.mdReport());
    }

    @GetMapping("/summary")
    public Map<String, Object> summary() {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("totalRuns", results.size());
        long done   = results.values().stream().filter(r -> "done".equals(r.status())).count();
        long errors = results.values().stream().filter(r -> "error".equals(r.status())).count();
        out.put("completed", done);
        out.put("errors", errors);
        return out;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String generateMarkdown(PocConfig config) {
        try {
            // Re-run analysis to get FlowResult objects for MarkdownReporter
            var walker  = new com.flowpoc.engine.FlowWalker(java.util.List.of(
                    new com.flowpoc.engine.visitor.MongoOperationVisitor(),
                    new com.flowpoc.engine.visitor.SqlOperationVisitor(
                            new com.flowpoc.engine.SqlPredicateExtractor())));
            var results = walker.walk(new java.io.File(config.getAnalysisJsonPath()));

            var pipeline = com.flowpoc.analyzer.AnalyzerPipeline.builder()
                    .withMongoDefaults().build();
            if (config.isClaudeEnabled()) {
                // Claude is already in withMongoDefaults if registered; run separately if needed
            }
            results.forEach(pipeline::runAndAttach);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            new MarkdownReporter().write(results, out);
            return out.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "# Error generating report\n" + e.getMessage();
        }
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    public record AnalysisRequest(
            String analysisJson,
            String mongoUri,
            String mongoDb,
            String jarPath,
            String claudeApiKey,
            int    maxEndpoints
    ) {}

    private record RunResult(String status, String jsonReport, String mdReport, String error) {}
}
