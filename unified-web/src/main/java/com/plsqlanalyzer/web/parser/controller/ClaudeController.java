package com.plsqlanalyzer.web.parser.controller;

import com.plsqlanalyzer.web.parser.service.AnalysisReaderFactory;
import com.plsqlanalyzer.web.parser.service.ClaudePersistenceService;
import com.plsqlanalyzer.web.parser.service.ClaudeProcessRunner;
import com.plsqlanalyzer.web.parser.service.ClaudeSessionManager;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.VerificationResult;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * REST controller for Claude AI verification of PL/SQL static analysis.
 * Provides endpoints for starting verification, tracking progress,
 * managing sessions, and handling version rotation.
 */
@RestController
@RequestMapping("/api/parser")
public class ClaudeController {

    private static final Logger log = LoggerFactory.getLogger(ClaudeController.class);

    private final ClaudeVerificationService verificationService;
    private final ClaudeSessionManager sessionManager;
    private final ClaudePersistenceService persistenceService;
    private final ClaudeProcessRunner processRunner;
    private final AnalysisReaderFactory readerFactory;

    public ClaudeController(ClaudeVerificationService verificationService,
                            ClaudeSessionManager sessionManager,
                            ClaudePersistenceService persistenceService,
                            ClaudeProcessRunner processRunner,
                            AnalysisReaderFactory readerFactory) {
        this.verificationService = verificationService;
        this.sessionManager = sessionManager;
        this.persistenceService = persistenceService;
        this.processRunner = processRunner;
        this.readerFactory = readerFactory;
    }

    // ==================== VERIFICATION ====================

    /**
     * POST /api/analyses/{name}/claude/verify
     * Start a Claude verification session for the given analysis.
     * Accepts optional body: {"resume": true} to resume from checkpoint.
     */
    @PostMapping("/analyses/{name}/claude/verify")
    public ResponseEntity<Map<String, Object>> startVerification(
            @PathVariable("name") String name,
            @RequestBody(required = false) Map<String, Object> body) {
        try {
            if (!processRunner.isAvailable()) {
                return ResponseEntity.status(503).body(Map.of(
                        "error", "Claude CLI is not available"));
            }

            if (sessionManager.hasRunning(name)) {
                return ResponseEntity.status(409).body(Map.of(
                        "error", "Verification already running for this analysis"));
            }

            boolean resume = body != null && Boolean.TRUE.equals(body.get("resume"));
            String type = resume ? "RESUME" : "FULL_SCAN";
            String detail = resume ? "Resuming from checkpoint" : "Full verification scan";

            String sessionId = sessionManager.submit(name, type, detail, () -> {
                VerificationResult result = verificationService.verify(name, msg ->
                        log.info("[Session {}] {}", name, msg));
                if (result != null && result.error == null) {
                    persistenceService.saveClaudeResult(name, result);
                    readerFactory.setMode(name, "claude");
                    log.info("Auto-switched '{}' to Claude-enriched view", name);
                }
            });

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("sessionId", sessionId);
            response.put("analysisName", name);
            response.put("type", type);
            response.put("status", "RUNNING");
            return ResponseEntity.accepted().body(response);
        } catch (Exception e) {
            log.error("Failed to start verification for '{}': {}", name, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to start verification"));
        }
    }

    /**
     * GET /api/analyses/{name}/claude/progress
     * Get verification progress: percent complete, chunk counts, errors.
     */
    @GetMapping("/analyses/{name}/claude/progress")
    public ResponseEntity<Map<String, Object>> getProgress(@PathVariable("name") String name) {
        try {
            Map<String, Object> progress = verificationService.getProgress(name);
            progress.put("hasRunningSession", sessionManager.hasRunning(name));
            return ResponseEntity.ok(progress);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to get progress"));
        }
    }

    /**
     * GET /api/analyses/{name}/claude/progress/stream
     * Server-Sent Events stream for real-time progress updates.
     * Polls every 500ms until verification completes or client disconnects.
     */
    @GetMapping(value = "/analyses/{name}/claude/progress/stream",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamProgress(@PathVariable("name") String name) {
        SseEmitter emitter = new SseEmitter(600_000L); // 10 minute timeout

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        emitter.onCompletion(scheduler::shutdown);
        emitter.onTimeout(scheduler::shutdown);
        emitter.onError(e -> scheduler.shutdown());

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Map<String, Object> progress = verificationService.getProgress(name);
                progress.put("hasRunningSession", sessionManager.hasRunning(name));
                emitter.send(SseEmitter.event()
                        .name("progress")
                        .data(progress));

                boolean isComplete = Boolean.TRUE.equals(progress.get("isComplete"));
                boolean hasRunning = Boolean.TRUE.equals(progress.get("hasRunningSession"));

                if (isComplete && !hasRunning) {
                    emitter.send(SseEmitter.event()
                            .name("complete")
                            .data(Map.of("status", "done")));
                    emitter.complete();
                    scheduler.shutdown();
                }
            } catch (IOException e) {
                emitter.completeWithError(e);
                scheduler.shutdown();
            } catch (Exception e) {
                log.debug("SSE progress error for '{}': {}", name, e.getMessage());
                try {
                    emitter.send(SseEmitter.event()
                            .name("error")
                            .data(Map.of("error", e.getMessage())));
                } catch (IOException ignored) {}
                emitter.completeWithError(e);
                scheduler.shutdown();
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        return emitter;
    }

    /**
     * GET /api/analyses/{name}/claude/result
     * Get the verification result for the analysis.
     * Returns final result if available, otherwise partial result from completed chunks.
     */
    @GetMapping("/analyses/{name}/claude/result")
    public ResponseEntity<?> getResult(@PathVariable("name") String name) {
        try {
            // Prefer persistence copy (has user review decisions)
            VerificationResult result = persistenceService.loadClaudeResult(name);
            if (result == null) {
                result = verificationService.loadResult(name);
            }
            if (result == null) {
                result = verificationService.loadPartialResult(name);
            }
            if (result == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to load result"));
        }
    }

    // ==================== CHUNKS ====================

    /**
     * GET /api/analyses/{name}/claude/chunks
     * List all chunk IDs (simple list for backward compat).
     */
    @GetMapping("/analyses/{name}/claude/chunks")
    public ResponseEntity<?> listChunks(@PathVariable("name") String name) {
        try {
            List<String> chunks = verificationService.listChunks(name);
            return ResponseEntity.ok(chunks);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to list chunks"));
        }
    }

    /**
     * GET /api/analyses/{name}/claude/chunks/summary
     * List all chunk statuses (id, name, status: COMPLETE/ERROR/PENDING).
     */
    @GetMapping("/analyses/{name}/claude/chunks/summary")
    public ResponseEntity<?> getChunksSummary(@PathVariable("name") String name) {
        try {
            List<Map<String, Object>> summaries = verificationService.listChunkSummaries(name);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("analysisName", name);
            response.put("chunks", summaries);
            response.put("total", summaries.size());
            long complete = summaries.stream()
                    .filter(s -> "COMPLETE".equals(s.get("status"))).count();
            long errors = summaries.stream()
                    .filter(s -> "ERROR".equals(s.get("status"))).count();
            response.put("completeCount", complete);
            response.put("errorCount", errors);
            response.put("pendingCount", summaries.size() - complete - errors);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to list chunks"));
        }
    }

    /**
     * GET /api/analyses/{name}/claude/chunks/{id}
     * Get specific chunk detail: input prompt, output response, and error if any.
     */
    @GetMapping("/analyses/{name}/claude/chunks/{id}")
    public ResponseEntity<?> getChunkDetail(
            @PathVariable("name") String name,
            @PathVariable("id") String id) {
        try {
            Map<String, Object> fragment = verificationService.getChunkFragment(name, id);
            if (fragment.size() <= 1) { // only chunkId, no data found
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(fragment);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to load chunk"));
        }
    }

    /**
     * GET /api/analyses/{name}/claude/table-chunks
     * Get table -> chunk ID mapping (reverse index for per-table log viewing).
     */
    @GetMapping("/analyses/{name}/claude/table-chunks")
    public ResponseEntity<?> getTableChunkMapping(@PathVariable("name") String name) {
        try {
            Map<String, List<String>> mapping = verificationService.getTableChunkMapping(name);
            return ResponseEntity.ok(mapping);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to get table-chunk mapping"));
        }
    }

    // ==================== USER REVIEW ====================

    /**
     * POST /api/analyses/{name}/claude/review
     * Save user decisions on Claude verification operations.
     * Body: { "decisions": [ { "tableName": "..", "operation": "..", "procedureName": "..", "lineNumber": 0, "decision": "ACCEPTED"|"REJECTED" } ] }
     * Or: { "bulk": "ACCEPTED"|"REJECTED" } to accept/reject all pending operations.
     */
    @PostMapping("/analyses/{name}/claude/review")
    public ResponseEntity<?> saveReview(
            @PathVariable("name") String name,
            @RequestBody Map<String, Object> body) {
        try {
            VerificationResult result = persistenceService.loadClaudeResult(name);
            if (result == null) {
                return ResponseEntity.notFound().build();
            }

            String bulkDecision = body.containsKey("bulk") ? (String) body.get("bulk") : null;
            int updated = 0;

            if (bulkDecision != null) {
                for (var table : result.tables) {
                    for (var v : table.claudeVerifications) {
                        if (v.userDecision == null) {
                            v.userDecision = bulkDecision;
                            updated++;
                        }
                    }
                }
            } else {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> decisions =
                        (List<Map<String, Object>>) body.get("decisions");
                if (decisions != null) {
                    for (Map<String, Object> dec : decisions) {
                        String tbl = (String) dec.get("tableName");
                        String op = (String) dec.get("operation");
                        String proc = (String) dec.get("procedureName");
                        int line = dec.get("lineNumber") != null
                                ? ((Number) dec.get("lineNumber")).intValue() : 0;
                        String decision = (String) dec.get("decision");

                        for (var table : result.tables) {
                            if (!table.tableName.equalsIgnoreCase(tbl)) continue;
                            for (var v : table.claudeVerifications) {
                                if (v.operation.equalsIgnoreCase(op)
                                        && (v.procedureName == null ? proc == null
                                            : v.procedureName.equalsIgnoreCase(proc))
                                        && v.lineNumber == line) {
                                    v.userDecision = decision;
                                    updated++;
                                }
                            }
                        }
                    }
                }
            }

            persistenceService.updateClaudeResult(name, result);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("analysisName", name);
            response.put("updated", updated);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to save review for '{}': {}", name, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to save review"));
        }
    }

    // ==================== APPLY ACCEPTED CHANGES ====================

    /**
     * POST /api/analyses/{name}/claude/apply
     * Apply accepted Claude verification changes to the static analysis data.
     * For REMOVED+ACCEPTED: removes false-positive operations from tables.
     * For NEW+ACCEPTED: adds newly-discovered operations to tables.
     * Backs up original data before modifying.
     */
    @PostMapping("/analyses/{name}/claude/apply")
    public ResponseEntity<?> applyConfirmed(@PathVariable("name") String name) {
        try {
            Map<String, Object> result = persistenceService.applyAcceptedChanges(name);
            if (result == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "No Claude result or static data available to apply"));
            }
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Failed to apply Claude changes for '{}': {}", name, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to apply changes"));
        }
    }

    // ==================== SESSION MANAGEMENT (per-analysis) ====================

    /**
     * POST /api/analyses/{name}/claude/sessions/{sessionId}/kill
     * Kill a specific running session.
     */
    @PostMapping("/analyses/{name}/claude/sessions/{sessionId}/kill")
    public ResponseEntity<Map<String, Object>> killSession(
            @PathVariable("name") String name,
            @PathVariable("sessionId") String sessionId) {
        try {
            boolean killed = sessionManager.kill(sessionId);
            if (killed) {
                verificationService.cancel();
            }
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("sessionId", sessionId);
            response.put("killed", killed);
            response.put("message", killed ? "Session killed" : "Session not found or not running");
            return killed ? ResponseEntity.ok(response) : ResponseEntity.notFound().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to kill session"));
        }
    }

    // ==================== VERSIONS ====================

    /**
     * GET /api/analyses/{name}/claude/versions
     * Get version info: current mode, iteration count, timestamps, file sizes.
     */
    @GetMapping("/analyses/{name}/claude/versions")
    public ResponseEntity<?> getVersions(@PathVariable("name") String name) {
        try {
            Map<String, Object> info = persistenceService.getVersionInfo(name);
            return ResponseEntity.ok(info);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to get version info"));
        }
    }

    /**
     * POST /api/analyses/{name}/claude/versions/load-static
     * Switch view to static analysis (original parser output).
     */
    @PostMapping("/analyses/{name}/claude/versions/load-static")
    public ResponseEntity<Map<String, Object>> loadStatic(@PathVariable("name") String name) {
        try {
            Map<String, Object> info = persistenceService.getVersionInfo(name);
            if (!Boolean.TRUE.equals(info.get("hasStatic"))) {
                return ResponseEntity.notFound().build();
            }
            readerFactory.setMode(name, "static");
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("analysisName", name);
            response.put("mode", "static");
            response.put("message", "Switched to static analysis view");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to load static view"));
        }
    }

    /**
     * POST /api/analyses/{name}/claude/versions/load-claude
     * Switch view to Claude-enriched analysis.
     */
    @PostMapping("/analyses/{name}/claude/versions/load-claude")
    public ResponseEntity<?> loadClaude(@PathVariable("name") String name) {
        try {
            VerificationResult result = persistenceService.loadClaudeResult(name);
            if (result == null) {
                return ResponseEntity.notFound().build();
            }
            readerFactory.setMode(name, "claude");
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("analysisName", name);
            response.put("mode", "claude");
            response.put("confirmedCount", result.confirmedCount);
            response.put("removedCount", result.removedCount);
            response.put("newCount", result.newCount);
            response.put("message", "Switched to Claude enriched view");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to load Claude view"));
        }
    }

    /**
     * POST /api/analyses/{name}/claude/versions/load-prev
     * Switch view to the previous Claude-enriched version.
     */
    @PostMapping("/analyses/{name}/claude/versions/load-prev")
    public ResponseEntity<?> loadPrev(@PathVariable("name") String name) {
        try {
            VerificationResult prevResult = persistenceService.loadClaudePrev(name);
            if (prevResult == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "No previous Claude version available"));
            }
            readerFactory.setMode(name, "claude_prev");
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("analysisName", name);
            response.put("mode", "claude_prev");
            response.put("confirmedCount", prevResult.confirmedCount);
            response.put("removedCount", prevResult.removedCount);
            response.put("newCount", prevResult.newCount);
            response.put("message", "Switched to previous Claude version");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to load previous version"));
        }
    }

    /**
     * POST /api/analyses/{name}/claude/versions/revert
     * Revert to the previous Claude verification version.
     */
    @PostMapping("/analyses/{name}/claude/versions/revert")
    public ResponseEntity<Map<String, Object>> revertVersion(@PathVariable("name") String name) {
        try {
            boolean reverted = persistenceService.revertClaude(name);
            if (!reverted) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "No previous version available to revert to"));
            }
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("analysisName", name);
            response.put("reverted", true);
            response.put("message", "Reverted to previous Claude version");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to revert"));
        }
    }

    // ==================== GLOBAL CLAUDE STATUS ====================

    /**
     * GET /api/claude/status
     * Check if Claude CLI is available and return session summary.
     */
    @GetMapping("/claude/status")
    public ResponseEntity<Map<String, Object>> getClaudeStatus() {
        try {
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("available", processRunner.isAvailable());
            status.put("runningProcesses", processRunner.getRunningCount());
            status.put("sessions", sessionManager.getSummary());
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to check status"));
        }
    }

    /**
     * GET /api/claude/sessions
     * List all active Claude sessions across all analyses.
     */
    @GetMapping("/claude/sessions")
    public ResponseEntity<?> listAllSessions() {
        try {
            List<Map<String, Object>> sessions = sessionManager.listSessions(null);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("sessions", sessions);
            response.put("summary", sessionManager.getSummary());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to list sessions"));
        }
    }

    /**
     * POST /api/claude/sessions/kill-all
     * Kill all running Claude sessions and processes.
     */
    @PostMapping("/claude/sessions/kill-all")
    public ResponseEntity<Map<String, Object>> killAllSessions() {
        try {
            int processesKilled = processRunner.killAll();
            // Kill all sessions by iterating through running ones
            List<Map<String, Object>> sessions = sessionManager.listSessions(null);
            int sessionsKilled = 0;
            for (Map<String, Object> session : sessions) {
                if ("RUNNING".equals(session.get("status"))) {
                    String id = (String) session.get("id");
                    if (id != null && sessionManager.kill(id)) {
                        sessionsKilled++;
                    }
                }
            }
            verificationService.cancel();

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("processesKilled", processesKilled);
            response.put("sessionsKilled", sessionsKilled);
            response.put("message", "Killed " + sessionsKilled + " sessions and "
                    + processesKilled + " processes");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to kill sessions"));
        }
    }
}
