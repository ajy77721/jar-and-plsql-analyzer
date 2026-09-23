package com.jaranalyzer.crossref;

import com.jaranalyzer.crossref.model.CrossRefRequest;
import com.jaranalyzer.crossref.model.CrossRefResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * REST API for the cross-reference feature.
 *
 * All routes are under /api/{jarId}/cross-ref/.
 * Completely isolated — no shared state with FlowAnalysisController.
 *
 * Endpoints:
 *   POST   /api/{jarId}/cross-ref/link      → run cross-ref analysis, returns {crossRefId}
 *   GET    /api/{jarId}/cross-ref/result/{id}  → get CrossRefResult JSON
 *   GET    /api/{jarId}/cross-ref/report/{id}  → download Markdown report
 *   GET    /api/{jarId}/cross-ref/history      → list all cross-ref runs for this jar
 */
/*
 * DISABLED — the Cross-Reference feature is no longer supported.
 *
 * The UI entry point (tab button and js/cross-ref.js) has been removed, and these 14 routes are now
 * switched off by default so they do not sit in the API surface unreachable and unowned. Set
 *
 *     crossref.enabled=true
 *
 * to restore them; nothing else needs changing, which is why this is a condition rather than a
 * deletion. CrossRefService and CrossRefClaudeService are referenced by nothing else (verified),
 * so no other feature is affected.
 */
@RestController
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        name = "crossref.enabled", havingValue = "true", matchIfMissing = false)
@RequestMapping("/api/jar/jars/{jarId}/cross-ref")
public class CrossRefController {

    private static final Logger log = LoggerFactory.getLogger(CrossRefController.class);

    private final CrossRefService service;
    private final CrossRefClaudeService claudeService;

    public CrossRefController(CrossRefService service, CrossRefClaudeService claudeService) {
        this.service = service;
        this.claudeService = claudeService;
    }

    /**
     * Run a cross-reference analysis linking an endpoint to an Oracle proc.
     *
     * Request body:
     * {
     *   "endpointPath": "/api/v1/process-motor-auto-policy",
     *   "procName": "PG_UWGE_CI",
     *   "schema": "CUSTOMER",          // optional
     *   "endpointIndex": 186            // optional, speeds up call-tree lookup
     * }
     *
     * Response: { "crossRefId": "1746700000000", "mode": "STATIC_INFERENCE" }
     */
    @PostMapping("/link")
    public ResponseEntity<?> link(@PathVariable String jarId,
                                   @RequestBody CrossRefRequest request) {
        try {
            request.setJarId(jarId);
            String crossRefId = service.link(
                    jarId,
                    request.getEndpointPath(),
                    request.getProcName(),
                    request.getSchema(),
                    request.getEndpointIndex()
            );
            CrossRefResult result = service.getResult(jarId, crossRefId);
            return ResponseEntity.ok(Map.of(
                    "crossRefId", crossRefId,
                    "mode", result != null ? result.getMode() : "UNKNOWN",
                    "summary", result != null && result.getSummary() != null ? result.getSummary() : Map.of()
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (IOException e) {
            log.error("CrossRefController: link failed for jar={} endpoint={}: {}",
                      jarId, request.getEndpointPath(), e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get the full CrossRefResult JSON for a completed cross-ref run.
     */
    @GetMapping("/result/{crossRefId}")
    public ResponseEntity<CrossRefResult> getResult(@PathVariable String jarId,
                                                      @PathVariable String crossRefId) {
        CrossRefResult result = service.getResult(jarId, crossRefId);
        if (result == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(result);
    }

    /**
     * Download the Markdown comparison report.
     */
    @GetMapping("/report/{crossRefId}")
    public ResponseEntity<byte[]> getReport(@PathVariable String jarId,
                                             @PathVariable String crossRefId) {
        byte[] bytes = service.getReport(jarId, crossRefId);
        if (bytes == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=\"cross-ref-" + crossRefId + ".md\"")
                .contentType(MediaType.TEXT_PLAIN)
                .body(bytes);
    }

    /**
     * List all cross-ref runs for a given jar (newest first).
     * Each entry contains the meta.json fields: crossRefId, endpointPath, procName, mode, timestamp.
     */
    @GetMapping("/history")
    public ResponseEntity<List<Map<String, Object>>> history(@PathVariable String jarId) {
        return ResponseEntity.ok(service.listHistory(jarId));
    }

    /**
     * Delete a cross-ref run (removes the run directory from disk).
     */
    @DeleteMapping("/result/{crossRefId}")
    public ResponseEntity<Map<String, Object>> delete(@PathVariable String jarId,
                                                        @PathVariable String crossRefId) {
        boolean deleted = service.delete(jarId, crossRefId);
        if (!deleted) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("deleted", true, "crossRefId", crossRefId));
    }

    /**
     * List all Oracle proc/package names available from PL/SQL sessions on disk.
     */
    @GetMapping("/available-procs")
    public ResponseEntity<List<Map<String, Object>>> availableProcs(@PathVariable String jarId) {
        return ResponseEntity.ok(service.listAvailableProcs());
    }

    // ── Claude enrichment endpoints ───────────────────────────────────────────

    /**
     * Start Claude enrichment for all proc links in a cross-ref result.
     * Returns an SSE stream reporting progress.
     */
    @PostMapping(value = "/result/{crossRefId}/claude/run", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter claudeRun(@PathVariable String jarId, @PathVariable String crossRefId) {
        log.info("CrossRefController: claude/run jar={} crossRef={}", jarId, crossRefId);
        return claudeService.runEnrichment(jarId, crossRefId);
    }

    /**
     * Get current Claude enrichment progress (polling endpoint).
     */
    @GetMapping("/result/{crossRefId}/claude/progress")
    public ResponseEntity<Map<String, Object>> claudeProgress(@PathVariable String jarId,
                                                               @PathVariable String crossRefId) {
        return ResponseEntity.ok(claudeService.getProgress(jarId, crossRefId));
    }

    /**
     * Re-merge existing Claude chunk files into result.json without re-running Claude.
     * Use after a run completed but verdicts weren't visible (e.g. claudeDone was false).
     */
    @PostMapping("/result/{crossRefId}/claude/merge")
    public ResponseEntity<Map<String, Object>> claudeMerge(@PathVariable String jarId,
                                                            @PathVariable String crossRefId) {
        Map<String, Object> result = claudeService.mergeOnly(jarId, crossRefId);
        if (result.containsKey("error")) return ResponseEntity.status(404).body(result);
        return ResponseEntity.ok(result);
    }

    /**
     * Cancel a running Claude enrichment.
     */
    @DeleteMapping("/result/{crossRefId}/claude/cancel")
    public ResponseEntity<Map<String, Object>> claudeCancel(@PathVariable String jarId,
                                                              @PathVariable String crossRefId) {
        boolean cancelled = claudeService.cancel(jarId, crossRefId);
        return ResponseEntity.ok(Map.of("cancelled", cancelled));
    }

    /**
     * Get full prompt + response for a single chunk (for debugging).
     */
    @GetMapping("/result/{crossRefId}/claude/chunk/{chunkId}")
    public ResponseEntity<Map<String, Object>> claudeChunk(@PathVariable String jarId,
                                                             @PathVariable String crossRefId,
                                                             @PathVariable String chunkId) {
        Map<String, Object> detail = claudeService.getChunkDetail(jarId, crossRefId, chunkId);
        if (detail.containsKey("error")) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(detail);
    }

    // ── Source code endpoints (for side-by-side viewer) ───────────────────────

    /**
     * Get Java source code for a node.
     * Uses query param ?nodeId=ClassName%23methodName to avoid path-variable truncation issues.
     */
    @GetMapping("/result/{crossRefId}/source/java")
    public ResponseEntity<Map<String, Object>> javaSource(@PathVariable String jarId,
                                                           @PathVariable String crossRefId,
                                                           @RequestParam String nodeId) {
        Map<String, Object> src = claudeService.getJavaSource(jarId, crossRefId, nodeId);
        if (src.containsKey("error")) return ResponseEntity.status(404).body(src);
        return ResponseEntity.ok(src);
    }

    /**
     * Get Oracle SQL source for a proc.
     * Uses query param ?procRef=PKG.PROC to avoid path-variable truncation on dots.
     */
    @GetMapping("/result/{crossRefId}/source/oracle")
    public ResponseEntity<Map<String, Object>> oracleSource(@PathVariable String jarId,
                                                             @PathVariable String crossRefId,
                                                             @RequestParam String procRef) {
        Map<String, Object> src = claudeService.getOracleSource(jarId, crossRefId, procRef);
        if (src.containsKey("error")) return ResponseEntity.status(404).body(src);
        return ResponseEntity.ok(src);
    }

    /**
     * Return the full Oracle call tree (all nodes with depth/parent/tables/LOC)
     * for the given cross-ref run. Used by the tree source viewer.
     */
    @GetMapping("/result/{crossRefId}/source/oracle-tree")
    public ResponseEntity<Map<String, Object>> oracleTree(@PathVariable String jarId,
                                                           @PathVariable String crossRefId) {
        Map<String, Object> tree = claudeService.getOracleTree(jarId, crossRefId);
        return ResponseEntity.ok(tree);
    }
}
