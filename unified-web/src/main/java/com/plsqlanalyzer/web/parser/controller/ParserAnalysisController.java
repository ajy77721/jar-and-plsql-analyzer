package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.parser.model.AnalysisInfo;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/parser")
public class ParserAnalysisController {

    private final AnalysisService service;
    private final AnalysisQueueService queueService;

    public ParserAnalysisController(@Qualifier("parserAnalysisService") AnalysisService service,
                                     AnalysisQueueService queueService) {
        this.service = service;
        this.queueService = queueService;
    }

    @GetMapping("/analyses")
    public ResponseEntity<List<AnalysisInfo>> listAnalyses() {
        try {
            return ResponseEntity.ok(service.listAnalyses());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/analyze")
    public ResponseEntity<Map<String, Object>> runAnalysis(@RequestBody Map<String, String> body) {
        String entryPoint = body.get("entryPoint");
        if (entryPoint == null || entryPoint.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "entryPoint is required"));
        }
        String owner = body.get("owner");
        String objectType = body.get("objectType");

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("entryPoint", entryPoint.trim());
        if (owner != null && !owner.isBlank()) metadata.put("owner", owner.trim());
        if (objectType != null && !objectType.isBlank()) metadata.put("objectType", objectType.trim());

        String displayName = "Parse: " + entryPoint.trim();
        QueueJob job = queueService.submit(QueueJob.Type.PARSER_ANALYSIS, displayName, metadata);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "queued");
        result.put("jobId", job.id);
        result.put("entryPoint", entryPoint.trim());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/analyses/{name}/index")
    public ResponseEntity<JsonNode> getIndex(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getIndex(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/node/{file}")
    public ResponseEntity<JsonNode> getNodeDetail(
            @PathVariable("name") String name,
            @PathVariable("file") String file) {
        try {
            return ResponseEntity.ok(service.getNodeDetail(name, file));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/tables")
    public ResponseEntity<JsonNode> getTables(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getTables(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/call-graph")
    public ResponseEntity<JsonNode> getCallGraph(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getCallGraph(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/source/{file}")
    public ResponseEntity<String> getSource(
            @PathVariable("name") String name,
            @PathVariable("file") String file) {
        try {
            return ResponseEntity.ok()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body(service.getSource(name, file));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/procedures")
    public ResponseEntity<JsonNode> getProcedures(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getProcedures(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/joins")
    public ResponseEntity<JsonNode> getJoins(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getJoins(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/cursors")
    public ResponseEntity<JsonNode> getCursors(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getCursors(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/sequences")
    public ResponseEntity<JsonNode> getSequences(@PathVariable("name") String name) {
        try {
            return ResponseEntity.ok(service.getSequences(name));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/call-tree/{nodeId}")
    public ResponseEntity<JsonNode> getCallTree(
            @PathVariable("name") String name,
            @PathVariable("nodeId") String nodeId) {
        try {
            return ResponseEntity.ok(service.getCallTree(name, nodeId));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/call-tree/{nodeId}/callers")
    public ResponseEntity<JsonNode> getCallers(
            @PathVariable("name") String name,
            @PathVariable("nodeId") String nodeId) {
        try {
            return ResponseEntity.ok(service.getCallers(name, nodeId));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/analyses/{name}/resolver/{type}")
    public ResponseEntity<JsonNode> getResolver(
            @PathVariable("name") String name,
            @PathVariable("type") String type) {
        try {
            return ResponseEntity.ok(service.getResolver(name, type));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }
}
