package com.plsqlanalyzer.web.controller;

import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary.TableAccessDetail;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.parser.model.SqlOperationType;
import com.plsqlanalyzer.web.service.ClaudeProcessRunner;
import com.plsqlanalyzer.web.service.ClaudeSessionManager;
import com.plsqlanalyzer.web.service.ClaudeVerificationService;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.OperationVerification;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.TableVerificationResult;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.VerificationResult;
import com.plsqlanalyzer.web.service.PersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * REST controller for Claude AI verification features.
 * Provides endpoints for starting/monitoring/killing verification sessions,
 * viewing results, and browsing input/output fragments.
 */
@RestController("plsqlClaudeController")
@RequestMapping("/api/plsql/claude")
public class ClaudeController {

    private static final Logger log = LoggerFactory.getLogger(ClaudeController.class);

    private final AnalysisService analysisService;
    private final ClaudeProcessRunner processRunner;
    private final ClaudeSessionManager sessionManager;
    private final ClaudeVerificationService verificationService;
    private final PersistenceService persistenceService;
    private final AnalysisQueueService queueService;

    /** SSE emitters for real-time progress */
    private final List<SseEmitter> progressEmitters = new CopyOnWriteArrayList<>();

    public ClaudeController(AnalysisService analysisService,
                            ClaudeProcessRunner processRunner,
                            ClaudeSessionManager sessionManager,
                            ClaudeVerificationService verificationService,
                            @org.springframework.beans.factory.annotation.Qualifier("plsqlPersistenceService")
                            PersistenceService persistenceService,
                            AnalysisQueueService queueService) {
        this.analysisService = analysisService;
        this.processRunner = processRunner;
        this.sessionManager = sessionManager;
        this.verificationService = verificationService;
        this.persistenceService = persistenceService;
        this.queueService = queueService;
    }

    // ==================== STATUS ====================

    /**
     * Check if Claude CLI is available and get overall status.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("cliAvailable", processRunner.isAvailable());
        status.put("runningProcesses", processRunner.getRunningCount());
        status.put("sessions", sessionManager.getSummary());

        AnalysisResult result = analysisService.getLatestResult();
        if (result != null) {
            String name = result.getName();
            status.put("currentAnalysis", name);
            status.put("hasVerification", verificationService.hasVerificationData(name));
            status.put("progress", verificationService.getProgress(name));
        }

        return ResponseEntity.ok(status);
    }

    // ==================== VERIFICATION ====================

    /**
     * Start a full verification scan of the current analysis.
     * Runs async — use SSE or polling to track progress.
     */
    @PostMapping("/verify")
    public ResponseEntity<Map<String, Object>> startVerification(
            @RequestBody(required = false) Map<String, String> body) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "No analysis loaded"));
        }

        String analysisName = result.getName();
        boolean resume = body != null && "true".equals(body.get("resume"));

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("analysisName", analysisName);
        meta.put("resume", resume);
        QueueJob job = queueService.submit(QueueJob.Type.PLSQL_CLAUDE_VERIFY,
                "PL/SQL verify: " + analysisName + (resume ? " (resume)" : ""), meta);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("analysisName", analysisName);
        response.put("resume", resume);
        return ResponseEntity.ok(response);
    }

    /**
     * Get verification progress for the current analysis.
     */
    @GetMapping("/progress")
    public ResponseEntity<Map<String, Object>> getProgress() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            return ResponseEntity.ok(Map.of("status", "no_analysis"));
        }
        Map<String, Object> progress = verificationService.getProgress(result.getName());
        progress.put("running", sessionManager.hasRunning(result.getName()));
        progress.put("analysisName", result.getName());
        return ResponseEntity.ok(progress);
    }

    /**
     * SSE stream for real-time verification progress.
     */
    @GetMapping("/progress/stream")
    public SseEmitter streamProgress() {
        SseEmitter emitter = new SseEmitter(1_800_000L); // 30 min timeout
        progressEmitters.add(emitter);
        emitter.onCompletion(() -> progressEmitters.remove(emitter));
        emitter.onTimeout(() -> progressEmitters.remove(emitter));
        emitter.onError(e -> progressEmitters.remove(emitter));
        return emitter;
    }

    // ==================== RESULTS ====================

    /**
     * Get the verification result for the current analysis.
     * Returns final result if available, otherwise partial result from completed chunks.
     */
    @GetMapping("/result")
    public ResponseEntity<Object> getResult() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            return ResponseEntity.ok(Map.of("status", "no_analysis"));
        }
        // Try final result first, then partial
        VerificationResult vr = verificationService.loadResult(result.getName());
        if (vr == null) {
            vr = verificationService.loadPartialResult(result.getName(), result);
        }
        if (vr == null) {
            return ResponseEntity.ok(Map.of("status", "no_verification"));
        }
        return ResponseEntity.ok(vr);
    }

    /**
     * Get verification result for a specific analysis by name.
     * Returns final result if available, otherwise partial from completed chunks.
     */
    @GetMapping("/result/{analysisName}")
    public ResponseEntity<Object> getResultByName(@PathVariable String analysisName) {
        VerificationResult vr = verificationService.loadResult(analysisName);
        if (vr == null) {
            AnalysisResult result = analysisService.getLatestResult();
            if (result != null && analysisName.equals(result.getName())) {
                vr = verificationService.loadPartialResult(analysisName, result);
            }
        }
        if (vr == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(vr);
    }

    // ==================== FRAGMENTS / LOGS ====================

    /**
     * List all chunks for the current analysis (ID strings only, backward compat).
     */
    @GetMapping("/chunks")
    public ResponseEntity<List<String>> listChunks() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.ok(Collections.emptyList());
        return ResponseEntity.ok(verificationService.listChunks(result.getName()));
    }

    /**
     * List all chunks with summary info (status, procedures, error).
     */
    @GetMapping("/chunks/summary")
    public ResponseEntity<List<Map<String, Object>>> listChunkSummaries() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.ok(Collections.emptyList());
        return ResponseEntity.ok(verificationService.listChunkSummaries(result.getName()));
    }

    /**
     * Get input/output fragment for a specific chunk.
     */
    @GetMapping("/chunks/{chunkId}")
    public ResponseEntity<Map<String, Object>> getChunkFragment(@PathVariable String chunkId) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(verificationService.getChunkFragment(result.getName(), chunkId));
    }

    /**
     * Get table → chunk ID mapping (reverse index for per-table log viewing).
     */
    @GetMapping("/table-chunks")
    public ResponseEntity<Map<String, List<String>>> getTableChunkMapping() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.ok(Collections.emptyMap());
        return ResponseEntity.ok(verificationService.getTableChunkMapping(result.getName()));
    }

    // ==================== SESSIONS ====================

    /**
     * List all Claude sessions.
     */
    @GetMapping("/sessions")
    public ResponseEntity<List<Map<String, Object>>> listSessions(
            @RequestParam(required = false) String analysisName) {
        return ResponseEntity.ok(sessionManager.listSessions(analysisName));
    }

    /**
     * Kill a specific session.
     */
    @PostMapping("/sessions/{sessionId}/kill")
    public ResponseEntity<Map<String, Object>> killSession(@PathVariable String sessionId) {
        boolean killed = sessionManager.kill(sessionId);
        verificationService.cancel();
        return ResponseEntity.ok(Map.of("killed", killed, "sessionId", sessionId));
    }

    /**
     * Kill all sessions for the current analysis.
     */
    @PostMapping("/sessions/kill-all")
    public ResponseEntity<Map<String, Object>> killAllSessions() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            return ResponseEntity.ok(Map.of("killed", 0));
        }
        int killed = sessionManager.killForAnalysis(result.getName());
        processRunner.killAll();
        verificationService.cancel();
        return ResponseEntity.ok(Map.of("killed", killed));
    }

    // ==================== VERSION MANAGEMENT ====================

    @GetMapping("/versions")
    public ResponseEntity<Map<String, Object>> getVersionInfo() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.ok(Map.of("status", "no_analysis"));
        Map<String, Object> info = persistenceService.getVersionInfo(result.getName());
        info.put("currentMode", result.getAnalysisMode());
        info.put("currentIteration", result.getClaudeIteration());
        info.put("claudeEnrichedAt", result.getClaudeEnrichedAt());
        return ResponseEntity.ok(info);
    }

    @PostMapping("/versions/load-static")
    public ResponseEntity<Map<String, Object>> loadStaticVersion() {
        AnalysisResult current = analysisService.getLatestResult();
        if (current == null) return ResponseEntity.badRequest().body(Map.of("error", "No analysis loaded"));
        AnalysisResult staticResult = persistenceService.loadStatic(current.getName());
        if (staticResult == null) return ResponseEntity.notFound().build();
        analysisService.setLatestResult(staticResult);
        return ResponseEntity.ok(Map.of("status", "loaded", "mode", "STATIC",
                "tables", staticResult.getTableOperations() != null ? staticResult.getTableOperations().size() : 0));
    }

    @PostMapping("/versions/load-claude")
    public ResponseEntity<Map<String, Object>> loadClaudeVersion() {
        AnalysisResult current = analysisService.getLatestResult();
        if (current == null) return ResponseEntity.badRequest().body(Map.of("error", "No analysis loaded"));
        AnalysisResult claudeResult = persistenceService.loadByName(current.getName());
        if (claudeResult == null) return ResponseEntity.notFound().build();
        analysisService.setLatestResult(claudeResult);
        return ResponseEntity.ok(Map.of("status", "loaded",
                "mode", claudeResult.getAnalysisMode() != null ? claudeResult.getAnalysisMode() : "STATIC",
                "iteration", claudeResult.getClaudeIteration(),
                "tables", claudeResult.getTableOperations() != null ? claudeResult.getTableOperations().size() : 0));
    }

    @PostMapping("/versions/load-prev")
    public ResponseEntity<Map<String, Object>> loadPreviousClaudeVersion() {
        AnalysisResult current = analysisService.getLatestResult();
        if (current == null) return ResponseEntity.badRequest().body(Map.of("error", "No analysis loaded"));
        AnalysisResult prev = persistenceService.loadClaudePrev(current.getName());
        if (prev == null) return ResponseEntity.badRequest().body(Map.of("error", "No previous Claude version available"));
        analysisService.setLatestResult(prev);
        return ResponseEntity.ok(Map.of("status", "loaded", "mode", "CLAUDE_ENRICHED_PREV",
                "iteration", prev.getClaudeIteration(),
                "tables", prev.getTableOperations() != null ? prev.getTableOperations().size() : 0));
    }

    @PostMapping("/versions/revert")
    public ResponseEntity<Map<String, Object>> revertToPreviousClaude() {
        AnalysisResult current = analysisService.getLatestResult();
        if (current == null) return ResponseEntity.badRequest().body(Map.of("error", "No analysis loaded"));
        boolean reverted = persistenceService.revertClaude(current.getName());
        if (!reverted) return ResponseEntity.badRequest().body(Map.of("error", "No previous Claude version to revert to"));
        AnalysisResult reloaded = persistenceService.loadByName(current.getName());
        if (reloaded != null) analysisService.setLatestResult(reloaded);
        return ResponseEntity.ok(Map.of("status", "reverted",
                "iteration", reloaded != null ? reloaded.getClaudeIteration() : 0));
    }

    // ==================== MERGE NEW FINDINGS ====================

    private void mergeNewFindings(AnalysisResult liveResult, VerificationResult verResult) {
        String name = liveResult.getName();
        broadcastProgress("[MERGE] Starting merge of " + verResult.newCount + " new findings...");

        // Always start from the static (original) analysis — never mutate it
        AnalysisResult staticResult = persistenceService.loadStatic(name);
        if (staticResult == null) {
            log.warn("[Claude Merge] No static analysis found for '{}', using live result as base", name);
            staticResult = liveResult;
        }

        // Deep-copy the table operations from static so we don't mutate the original
        Map<String, TableOperationSummary> mergedTableOps = new LinkedHashMap<>();
        if (staticResult.getTableOperations() != null) {
            mergedTableOps.putAll(staticResult.getTableOperations());
        }

        int newTables = 0;
        int newOps = 0;

        for (TableVerificationResult tvr : verResult.tables) {
            if (tvr.claudeVerifications == null) continue;

            List<OperationVerification> newFindings = tvr.claudeVerifications.stream()
                    .filter(ov -> "NEW".equalsIgnoreCase(ov.status))
                    .toList();
            if (newFindings.isEmpty()) continue;

            String tableKey = tvr.tableName.toUpperCase();
            TableOperationSummary summary = mergedTableOps.get(tableKey);

            if (summary == null) {
                summary = new TableOperationSummary(tvr.tableName, tvr.schemaName);
                summary.setTableType("TABLE");
                mergedTableOps.put(tableKey, summary);
                newTables++;
            }

            for (OperationVerification ov : newFindings) {
                SqlOperationType opType = parseOpType(ov.operation);
                if (opType != null) {
                    summary.getOperations().add(opType);
                }

                TableAccessDetail detail = new TableAccessDetail();
                detail.setProcedureId(ov.procedureId != null ? ov.procedureId : ov.procedureName);
                detail.setProcedureName(ov.procedureName);
                detail.setOperation(opType);
                detail.setLineNumber(ov.lineNumber);
                detail.setSourceFile(ov.sourceFile != null ? ov.sourceFile : "claude-verified");
                summary.getAccessDetails().add(detail);
                newOps++;
            }

            summary.setAccessCount(summary.getAccessDetails().size());
        }

        if (newOps > 0) {
            // Build the enriched result from static base
            staticResult.setTableOperations(mergedTableOps);

            // Get current iteration from existing claude version (if any)
            Map<String, Object> versionInfo = persistenceService.getVersionInfo(name);
            boolean hadClaude = Boolean.TRUE.equals(versionInfo.get("hasClaude"));
            int prevIteration = 0;
            if (hadClaude) {
                AnalysisResult prevClaude = persistenceService.loadByName(name);
                if (prevClaude != null) {
                    prevIteration = prevClaude.getClaudeIteration();
                }
            }

            staticResult.setAnalysisMode("CLAUDE_ENRICHED");
            staticResult.setClaudeIteration(prevIteration + 1);
            staticResult.setClaudeEnrichedAt(java.time.LocalDateTime.now().toString());

            log.info("[Claude Merge] Merging {} new ops ({} new tables) into '{}', iteration {}",
                    newOps, newTables, name, staticResult.getClaudeIteration());

            try {
                // saveClaude handles rotation: current claude → claude_prev
                persistenceService.saveClaude(staticResult);

                // Update in-memory latest result so UI sees changes immediately
                analysisService.setLatestResult(staticResult);

                log.info("[Claude Merge] Saved Claude-enriched '{}' (iteration {})", name, staticResult.getClaudeIteration());
                broadcastProgress("[MERGE] Merged " + newOps + " new findings (" + newTables
                        + " new tables) — iteration " + staticResult.getClaudeIteration()
                        + (hadClaude ? " (prev version preserved)" : ""));
            } catch (Exception e) {
                log.error("[Claude Merge] Failed to save: {}", e.getMessage());
                broadcastProgress("[MERGE] ERROR: " + e.getMessage());
            }
        } else {
            broadcastProgress("[MERGE] No new findings to merge");
        }
    }

    private SqlOperationType parseOpType(String operation) {
        if (operation == null) return null;
        try {
            return SqlOperationType.valueOf(operation.toUpperCase().trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // ==================== INTERNAL ====================

    private void broadcastProgress(String message) {
        for (SseEmitter emitter : progressEmitters) {
            try {
                emitter.send(SseEmitter.event().name("claude-progress").data(message));
            } catch (IOException e) {
                progressEmitters.remove(emitter);
            }
        }
    }
}
