/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.analyzer.storage.ArtifactKey
 *  com.analyzer.storage.ArtifactStore
 *  com.fasterxml.jackson.databind.ObjectMapper
 *  org.springframework.boot.autoconfigure.condition.ConditionalOnClass
 *  org.springframework.http.ResponseEntity
 *  org.springframework.http.ResponseEntity$BodyBuilder
 *  org.springframework.web.bind.annotation.GetMapping
 *  org.springframework.web.bind.annotation.PathVariable
 *  org.springframework.web.bind.annotation.PostMapping
 *  org.springframework.web.bind.annotation.RequestBody
 *  org.springframework.web.bind.annotation.RequestMapping
 *  org.springframework.web.bind.annotation.RestController
 */
package com.flowpoc.web;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value={"/api/analysis"})
@ConditionalOnClass(name={"org.springframework.web.bind.annotation.RestController"})
public class AnalysisController {
    private final ConcurrentHashMap<String, RunResult> results = new ConcurrentHashMap();
    private final AtomicLong counter = new AtomicLong(1L);
    private final ArtifactStore store;

    public AnalysisController(ArtifactStore store) {
        this.store = store;
    }

    private static ArtifactKey keyInsideDataRoot(String clientPath) {
        if (clientPath == null || clientPath.isBlank()) {
            throw new IllegalArgumentException("A path is required");
        }
        if (clientPath.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Illegal path");
        }
        ArrayList<String> segments = new ArrayList<String>();
        for (String raw : clientPath.replace('\\', '/').split("/")) {
            if (raw.isBlank()) continue;
            if (raw.equals(".") || raw.equals("..")) {
                throw new IllegalArgumentException("Path escapes the data directory");
            }
            segments.add(raw);
        }
        if (segments.isEmpty()) {
            throw new IllegalArgumentException("Path is empty");
        }
        return ArtifactKey.of((String[])segments.toArray(new String[0]));
    }

    @GetMapping(value={"/ui"}, produces={"text/html"})
    public String ui() {
        return "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n  <meta charset=\"UTF-8\">\n  <title>Flow Analysis</title>\n  <style>\n    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;\n           max-width: 900px; margin: 40px auto; padding: 0 20px; background: #f5f5f5; }\n    h1 { color: #1a1a2e; }\n    .card { background: white; border-radius: 8px; padding: 24px;\n            box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin-bottom: 20px; }\n    label { display: block; font-weight: 600; margin: 12px 0 4px; color: #333; }\n    input, select { width: 100%; padding: 8px 12px; border: 1px solid #ddd;\n                    border-radius: 4px; font-size: 14px; box-sizing: border-box; }\n    button { background: #4f46e5; color: white; border: none; padding: 12px 28px;\n             border-radius: 6px; font-size: 15px; cursor: pointer; margin-top: 16px; }\n    button:hover { background: #4338ca; }\n    button:disabled { background: #9ca3af; cursor: not-allowed; }\n    .badge-high   { background:#fee2e2; color:#dc2626; padding:2px 8px; border-radius:12px; }\n    .badge-medium { background:#fef3c7; color:#d97706; padding:2px 8px; border-radius:12px; }\n    .badge-low    { background:#d1fae5; color:#059669; padding:2px 8px; border-radius:12px; }\n    #results { display:none; }\n    #findings-table { width:100%; border-collapse:collapse; }\n    #findings-table th, #findings-table td { padding:10px 14px; text-align:left;\n           border-bottom:1px solid #e5e7eb; }\n    #findings-table th { background:#f9fafb; font-weight:600; }\n    pre { background:#1e1e2e; color:#cdd6f4; padding:16px; border-radius:6px;\n          overflow-x:auto; font-size:13px; }\n    .spinner { display:inline-block; width:18px; height:18px; border:3px solid #e5e7eb;\n               border-top-color:#4f46e5; border-radius:50%; animation:spin 0.8s linear infinite; }\n    @keyframes spin { to { transform:rotate(360deg); } }\n  </style>\n</head>\n<body>\n  <h1>\ud83d\udd0d Flow Analysis</h1>\n  <p>Analyze an API endpoint call tree for MongoDB optimization opportunities,\n     static/transactional data classification, and test data generation.</p>\n\n  <div class=\"card\">\n    <h2>Configuration</h2>\n\n    <label>analysis.json path <small>(output from JAR Analyzer)</small></label>\n    <input id=\"analysisJson\" type=\"text\" placeholder=\"/data/analysis.json\">\n\n    <label>MongoDB URI</label>\n    <input id=\"mongoUri\" type=\"text\" value=\"mongodb://localhost:27017\">\n\n    <label>MongoDB Database</label>\n    <input id=\"mongoDb\" type=\"text\" placeholder=\"myapp\">\n\n    <label>JAR file path <small>(optional \u2014 enables Layer 2 dynamic execution)</small></label>\n    <input id=\"jarPath\" type=\"text\" placeholder=\"/apps/myapp.jar\">\n\n    <label>Anthropic API Key <small>(optional \u2014 enables Claude AI suggestions)</small></label>\n    <input id=\"claudeKey\" type=\"password\" placeholder=\"sk-ant-...\">\n\n    <label>Max endpoints to analyze <small>(0 = all)</small></label>\n    <input id=\"maxEndpoints\" type=\"number\" value=\"10\">\n\n    <button id=\"runBtn\" onclick=\"runAnalysis()\">\u25b6 Run Analysis</button>\n    <button id=\"downloadBtn\" style=\"display:none;background:#059669;\" onclick=\"downloadReport()\">\n      \u2b07 Download .md Report\n    </button>\n  </div>\n\n  <div class=\"card\" id=\"results\">\n    <h2>Results</h2>\n    <div id=\"summary\"></div>\n    <div id=\"findings-container\" style=\"margin-top:20px;\"></div>\n  </div>\n\n  <script>\n    let currentRunId = null;\n\n    async function runAnalysis() {\n      const btn = document.getElementById('runBtn');\n      btn.disabled = true;\n      btn.innerHTML = '<span class=\"spinner\"></span> Running...';\n      document.getElementById('results').style.display = 'none';\n      document.getElementById('downloadBtn').style.display = 'none';\n\n      const body = {\n        analysisJson:  document.getElementById('analysisJson').value,\n        mongoUri:      document.getElementById('mongoUri').value,\n        mongoDb:       document.getElementById('mongoDb').value,\n        jarPath:       document.getElementById('jarPath').value,\n        claudeApiKey:  document.getElementById('claudeKey').value,\n        maxEndpoints:  parseInt(document.getElementById('maxEndpoints').value) || 0\n      };\n\n      try {\n        const startResp = await fetch('/api/analysis/run', {\n          method: 'POST',\n          headers: {'Content-Type': 'application/json'},\n          body: JSON.stringify(body)\n        });\n        const start = await startResp.json();\n        currentRunId = start.id;\n        pollResult(currentRunId);\n      } catch(e) {\n        btn.disabled = false;\n        btn.innerHTML = '\u25b6 Run Analysis';\n        alert('Error: ' + e.message);\n      }\n    }\n\n    async function pollResult(id) {\n      const resp = await fetch('/api/analysis/result/' + id);\n      const data = await resp.json();\n\n      if (data.status === 'running') {\n        setTimeout(() => pollResult(id), 2000);\n        return;\n      }\n\n      const btn = document.getElementById('runBtn');\n      btn.disabled = false;\n      btn.innerHTML = '\u25b6 Run Analysis';\n\n      if (data.status === 'error') {\n        alert('Analysis failed: ' + data.error);\n        return;\n      }\n\n      renderResults(data.results);\n      document.getElementById('downloadBtn').style.display = 'inline-block';\n    }\n\n    function renderResults(results) {\n      document.getElementById('results').style.display = 'block';\n\n      // Summary counts\n      let high = 0, medium = 0, low = 0, endpoints = results.length;\n      results.forEach(r => (r.optimizations || []).forEach(f => {\n        if (f.severity === 'HIGH') high++;\n        else if (f.severity === 'MEDIUM') medium++;\n        else low++;\n      }));\n\n      document.getElementById('summary').innerHTML =\n        '<b>' + endpoints + '</b> endpoints &nbsp;'\n        + '<span class=\"badge-high\">\ud83d\udd34 ' + high + ' HIGH</span> &nbsp;'\n        + '<span class=\"badge-medium\">\ud83d\udfe1 ' + medium + ' MEDIUM</span> &nbsp;'\n        + '<span class=\"badge-low\">\ud83d\udfe2 ' + low + ' LOW</span>';\n\n      // Findings table\n      let html = '<table id=\"findings-table\"><thead><tr>'\n        + '<th>Severity</th><th>Category</th><th>Endpoint</th>'\n        + '<th>Collection</th><th>Description</th></tr></thead><tbody>';\n\n      results.forEach(r => {\n        (r.optimizations || []).forEach(f => {\n          const badge = f.severity === 'HIGH'\n            ? '<span class=\"badge-high\">\ud83d\udd34 HIGH</span>'\n            : f.severity === 'MEDIUM'\n            ? '<span class=\"badge-medium\">\ud83d\udfe1 MEDIUM</span>'\n            : '<span class=\"badge-low\">\ud83d\udfe2 LOW</span>';\n          html += '<tr><td>' + badge + '</td>'\n            + '<td>' + (f.category||'').replace(/_/g,' ') + '</td>'\n            + '<td><code>' + (r.endpoint||'') + '</code></td>'\n            + '<td><code>' + (f.table||'-') + '</code></td>'\n            + '<td>' + (f.description||'') + '</td></tr>';\n        });\n      });\n      html += '</tbody></table>';\n      document.getElementById('findings-container').innerHTML = html;\n    }\n\n    async function downloadReport() {\n      if (!currentRunId) return;\n      const resp = await fetch('/api/analysis/report/' + currentRunId);\n      const text = await resp.text();\n      const blob = new Blob([text], { type: 'text/markdown' });\n      const a = document.createElement('a');\n      a.href = URL.createObjectURL(blob);\n      a.download = 'flow-analysis-report.md';\n      a.click();\n    }\n  </script>\n</body>\n</html>\n";
    }

    @PostMapping(value={"/run"})
    public Map<String, Object> run(@RequestBody AnalysisRequest req) {
        String id = String.valueOf(this.counter.getAndIncrement());
        this.results.put(id, new RunResult("running", null, null, null));
        new Thread(() -> {
            try {
                PocConfig.Builder builder = PocConfig.builder().analysis(this.store, AnalysisController.keyInsideDataRoot(req.analysisJson())).mongo(req.mongoUri() != null ? req.mongoUri() : "mongodb://localhost:27017", req.mongoDb()).sampleSize(10).maxEndpoints(req.maxEndpoints());
                if (req.jarPath() != null && !req.jarPath().isBlank()) {
                    builder.layer2(AnalysisController.keyInsideDataRoot(req.jarPath()));
                }
                if (req.claudeApiKey() != null && !req.claudeApiKey().isBlank()) {
                    builder.claude(req.claudeApiKey());
                }
                PocConfig config = builder.build();
                ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
                new PocRunner(config).run(jsonOut);
                String jsonReport = jsonOut.toString(StandardCharsets.UTF_8);
                ByteArrayOutputStream mdOut = new ByteArrayOutputStream();
                new PocRunner(config).run(mdOut);
                String mdReport = this.generateMarkdown(config);
                this.results.put(id, new RunResult("done", jsonReport, mdReport, null));
            }
            catch (Exception e) {
                this.results.put(id, new RunResult("error", null, null, e.getMessage()));
            }
        }).start();
        return Map.of("id", id, "status", "running");
    }

    @GetMapping(value={"/result/{id}"})
    public ResponseEntity<Map<String, Object>> result(@PathVariable String id) {
        RunResult r = this.results.get(id);
        if (r == null) {
            return ResponseEntity.notFound().build();
        }
        LinkedHashMap<String, Object> resp = new LinkedHashMap<String, Object>();
        resp.put("status", r.status());
        if ("done".equals(r.status()) && r.jsonReport() != null) {
            try {
                resp.put("results", new ObjectMapper().readValue(r.jsonReport(), Object.class));
            }
            catch (Exception e) {
                resp.put("results", r.jsonReport());
            }
        }
        if ("error".equals(r.status())) {
            resp.put("error", r.error());
        }
        return ResponseEntity.ok(resp);
    }

    @GetMapping(value={"/report/{id}"}, produces={"text/markdown"})
    public ResponseEntity<String> report(@PathVariable String id) {
        RunResult r = this.results.get(id);
        if (r == null || r.mdReport() == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok().header("Content-Disposition", "attachment; filename=flow-analysis-report.md").body(r.mdReport());
    }

    @GetMapping(value={"/summary"})
    public Map<String, Object> summary() {
        LinkedHashMap<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("totalRuns", this.results.size());
        long done = this.results.values().stream().filter(r -> "done".equals(r.status())).count();
        long errors = this.results.values().stream().filter(r -> "error".equals(r.status())).count();
        out.put("completed", done);
        out.put("errors", errors);
        return out;
    }

    private String generateMarkdown(PocConfig config) {
        try {
            List<FlowResult> results;
            FlowWalker walker = new FlowWalker(List.of(new MongoOperationVisitor(), new SqlOperationVisitor(new SqlPredicateExtractor())));
            try (InputStream in = config.getStore().read(config.getAnalysisKey());){
                results = walker.walk(in, Collections.emptySet());
            }
            AnalyzerPipeline pipeline = AnalyzerPipeline.builder().withMongoDefaults().build();
            if (config.isClaudeEnabled()) {
                // empty if block
            }
            results.forEach(pipeline::runAndAttach);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            new MarkdownReporter().write(results, out);
            return out.toString(StandardCharsets.UTF_8);
        }
        catch (Exception e) {
            return "# Error generating report\n" + e.getMessage();
        }
    }

    private record RunResult(String status, String jsonReport, String mdReport, String error) {
    }

    public record AnalysisRequest(String analysisJson, String mongoUri, String mongoDb, String jarPath, String claudeApiKey, int maxEndpoints) {
    }
}

