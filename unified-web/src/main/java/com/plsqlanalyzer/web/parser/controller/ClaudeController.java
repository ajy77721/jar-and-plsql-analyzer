package com.plsqlanalyzer.web.parser.controller;

import com.plsqlanalyzer.web.parser.service.AnalysisReaderFactory;
import com.plsqlanalyzer.web.parser.service.ClaudeCallLogger;
import com.plsqlanalyzer.web.parser.service.ClaudePersistenceService;
import com.plsqlanalyzer.web.parser.service.ClaudeProcessRunner;
import com.plsqlanalyzer.web.parser.service.ClaudeSessionManager;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.VerificationResult;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.analyzer.auth.ConnectionGuard;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.sso.CapturedSsoContext;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
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
    private final ClaudeCallLogger callLogger;
    /**
     * The parser's routing store, held only so the request thread can snapshot its routing scope
     * for the background verification session (see {@link #startVerification}).
     */
    private final ArtifactStore store;

    /** Fails a verification start fast when the caller is logged in but has not connected a bucket. */
    private final ConnectionGuard connectionGuard;

    /** Carries the caller's role credentials onto the manager's pool (defense in depth). */
    private final SsoCredentialsResolver ssoCredentialsResolver;

    public ClaudeController(ClaudeVerificationService verificationService,
                            ClaudeSessionManager sessionManager,
                            ClaudePersistenceService persistenceService,
                            ClaudeProcessRunner processRunner,
                            AnalysisReaderFactory readerFactory,
                            ClaudeCallLogger callLogger,
                            @Qualifier("parserArtifactStore") ArtifactStore store,
                            ConnectionGuard connectionGuard,
                            SsoCredentialsResolver ssoCredentialsResolver) {
        this.verificationService = verificationService;
        this.sessionManager = sessionManager;
        this.persistenceService = persistenceService;
        this.processRunner = processRunner;
        this.readerFactory = readerFactory;
        this.callLogger = callLogger;
        this.store = store;
        this.connectionGuard = connectionGuard;
        this.ssoCredentialsResolver = ssoCredentialsResolver;
    }

    // ==================== CONFIG ====================

    /** GET /api/parser/config — current runtime config */
    @GetMapping("/config")
    public Map<String, Object> getConfig() {
        return Map.of("parallelChunks", verificationService.getParallelChunks());
    }

    /** PATCH /api/parser/config — update parallelChunks at runtime */
    @PatchMapping("/config")
    public Map<String, Object> patchConfig(@RequestBody Map<String, Object> body) {
        if (body.containsKey("parallelChunks")) {
            int n = ((Number) body.get("parallelChunks")).intValue();
            verificationService.setParallelChunks(n);
        }
        return Map.of("parallelChunks", verificationService.getParallelChunks());
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
        // Outside the try below on purpose: the guard throws a 409 ResponseStatusException, and the
        // catch(Exception) that maps handler failures to a 500 would otherwise swallow it.
        connectionGuard.requireParserConnected();
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
            // "force" — see ClaudeVerificationService.verify(name, force, callback)'s doc: bypasses
            // the checkpoint/existing-chunk-output skip entirely, so every chunk is regenerated
            // even if this exact analysis was verified before. Mutually exclusive with "resume" in
            // spirit (force wins if both are somehow sent) — resume means "continue," force means
            // "start over."
            boolean force = body != null && Boolean.TRUE.equals(body.get("force"));
            String type = force ? "FULL_SCAN" : (resume ? "RESUME" : "FULL_SCAN");
            String detail = force ? "Forced fresh verification (ignoring checkpoint)"
                    : (resume ? "Resuming from checkpoint" : "Full verification scan");

            // The session runs on the manager's own pool, which has NO request context: every store
            // write the verification makes (chunk/checkpoint fragments, the saved result) would
            // otherwise fall through to the shared default target. Capture the request thread's
            // routing scope and any installed job context HERE, before submit, and re-install them
            // for the whole task below. Either capture may carry nothing (install is then a no-op).
            final RequestScopedArtifactStore.Scope routingScope = store instanceof RequestScopedArtifactStore routed
                    ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;
            final CapturedSsoContext jobContext = CapturedSsoContext.capture(ssoCredentialsResolver);

            String sessionId = sessionManager.submit(name, type, detail, () -> {
                try (AutoCloseable routed = routingScope.install();
                     AutoCloseable ctx = jobContext == null ? null : jobContext.install()) {
                    VerificationResult result = verificationService.verify(name, force, msg ->
                            log.info("[Session {}] {}", name, msg));
                    if (result != null && result.error == null) {
                        persistenceService.saveClaudeResult(name, result);
                        readerFactory.setMode(name, "claude");
                        log.info("Auto-switched '{}' to Claude-enriched view", name);
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    // Only the close of an install handle can reach here; surface it so the session
                    // is recorded as FAILED rather than COMPLETE.
                    throw new IllegalStateException("Claude verification context install failed", e);
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
     * GET /api/parser/analyses/{name}/claude/progress
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
     * GET /api/parser/analyses/{name}/claude/progress/stream
     * Server-Sent Events stream for real-time progress updates.
     * Polls every 500ms until verification completes or client disconnects.
     */
    @GetMapping(value = "/analyses/{name}/claude/progress/stream",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamProgress(@PathVariable("name") String name) {
        SseEmitter emitter = new SseEmitter(0L); // no timeout — runs until complete/cancelled

        // Daemon threads: a browser that disconnects without completing the stream must not leave a
        // non-daemon poller holding the JVM open at shutdown. (The scheduler is still shut down on
        // completion/timeout/error below, so a live stream releases its thread promptly too.)
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "parser-progress-sse-" + name);
            t.setDaemon(true);
            return t;
        });

        emitter.onCompletion(scheduler::shutdown);
        emitter.onTimeout(scheduler::shutdown);
        emitter.onError(e -> scheduler.shutdown());

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Map<String, Object> progress = verificationService.getProgress(name);
                if (progress == null) return; // B4: guard null progress map
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
                // B3: shutdown in finally ensures leak-proof cleanup
                try { emitter.completeWithError(e); } finally { scheduler.shutdown(); }
            } catch (Exception e) {
                log.debug("SSE progress error for '{}': {}", name, e.getMessage());
                try {
                    emitter.send(SseEmitter.event()
                            .name("error")
                            .data(Map.of("error", e.getMessage())));
                } catch (IOException ignored) {}
                try { emitter.completeWithError(e); } finally { scheduler.shutdown(); }
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        return emitter;
    }

    /**
     * GET /api/parser/analyses/{name}/claude/result
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

    /**
     * GET /api/parser/analyses/{name}/claude/verification-summary
     *
     * <p>The seven scalars the verification grids actually render, instead of the whole result
     * document — measured at <b>138,301 bytes</b> for a document whose useful content here is under
     * 300. Five screens fetched the full document to show three to seven numbers.
     *
     * <p>Returns <b>204 No Content</b> when the analysis has simply never been verified, and 404
     * only when the analysis itself is unknown. That distinction matters: the previous code could
     * not tell "not verified" from "the URL is wrong", which is exactly how a permanently broken
     * grid went unnoticed — {@code perfSummary.js} called an unmapped path and rendered its 404 as
     * "No verification data".
     */
    @GetMapping("/analyses/{name}/claude/verification-summary")
    public ResponseEntity<?> getVerificationSummary(@PathVariable("name") String name) {
        try {
            VerificationResult result = persistenceService.loadClaudeResult(name);
            if (result == null) result = verificationService.loadResult(name);
            if (result == null) result = verificationService.loadPartialResult(name);
            if (result == null) {
                // Never verified. Not an error, and must not look like one.
                return ResponseEntity.noContent().build();
            }
            Map<String, Object> out = new java.util.LinkedHashMap<>();
            out.put("analysisName", result.analysisName);
            out.put("timestamp", result.timestamp);
            out.put("confirmedCount", result.confirmedCount);
            out.put("removedCount", result.removedCount);
            out.put("newCount", result.newCount);
            out.put("callConfirmedCount", result.callConfirmedCount);
            out.put("callRemovedCount", result.callRemovedCount);
            out.put("callNewCount", result.callNewCount);
            out.put("totalChunks", result.totalChunks);
            out.put("errorChunks", result.errorChunks);
            out.put("claudeCallCount", result.claudeCallCount);
            out.put("claudeTimeMs", result.claudeTimeMs);
            out.put("error", result.error);
            return ResponseEntity.ok(out);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to load summary"));
        }
    }

    /**
     * GET /api/parser/analyses/{name}/claude/verification-summary/tables
     *
     * <p>UI-013 §60/§62 — {@code parser/js/claudeEnrichment.js}'s overview modal fetched the whole
     * {@code /claude/result} document (the same one {@link #getResult} serves) solely to reduce it,
     * client-side, to one row per table with a verification: eight small counts
     * ({@code confirmed}/{@code removed}/{@code newOps}/{@code accepted}/{@code rejected}/
     * {@code pending}/{@code total}, plus {@code overallStatus}) folded out of every operation in
     * every table's {@code claudeVerifications} list. Unlike the {@code jar/js/summary-*} aggregates
     * blocked on UI-026, or {@code parser/js/statements.js}'s aggregation over {@code
     * PA.nodeIndex.all()} (already resident for ~20 unrelated consumers, see §62), the full Claude
     * result here is fetched by this one modal alone — nothing else on the page holds it for free —
     * so this aggregation genuinely shrinks what crosses the wire, the same shape as {@code
     * /claude/verification-summary} above.
     *
     * <p>Transcribed field-for-field from {@code claudeEnrichment.js}'s {@code
     * _renderOverviewBody}: a table with zero verifications is dropped from the output entirely
     * (matching {@code if (vfs.length === 0) continue;}), and status/decision comparisons are
     * upper-cased before matching, exactly as the client does.
     */
    @GetMapping("/analyses/{name}/claude/verification-summary/tables")
    public ResponseEntity<?> getVerificationSummaryTables(@PathVariable("name") String name) {
        try {
            VerificationResult result = persistenceService.loadClaudeResult(name);
            if (result == null) result = verificationService.loadResult(name);
            if (result == null) result = verificationService.loadPartialResult(name);
            if (result == null) {
                return ResponseEntity.noContent().build();
            }
            return ResponseEntity.ok(summarizeTables(result));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Failed to summarize tables"));
        }
    }

    /**
     * The per-table fold {@code claudeEnrichment.js}'s {@code _renderOverviewBody} does client-side,
     * transcribed. Package-private and a pure function of {@link VerificationResult} so it can be
     * tested directly, without standing up the controller and its five constructor dependencies.
     */
    static List<Map<String, Object>> summarizeTables(VerificationResult result) {
        List<Map<String, Object>> rows = new java.util.ArrayList<>();
        for (var t : result.tables) {
            var vfs = t.claudeVerifications;
            if (vfs == null || vfs.isEmpty()) continue;
            int confirmed = 0, removed = 0, newOps = 0, accepted = 0, rejected = 0, pending = 0;
            for (var v : vfs) {
                String st = v.status == null ? "" : v.status.toUpperCase(java.util.Locale.ROOT);
                String ud = v.userDecision == null ? "" : v.userDecision.toUpperCase(java.util.Locale.ROOT);
                if ("CONFIRMED".equals(st)) confirmed++;
                else if ("REMOVED".equals(st)) removed++;
                else if ("NEW".equals(st)) newOps++;
                if ("ACCEPTED".equals(ud)) accepted++;
                else if ("REJECTED".equals(ud)) rejected++;
                else pending++;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("tableName", t.tableName == null ? "" : t.tableName);
            row.put("overallStatus", t.overallStatus == null ? "" : t.overallStatus);
            row.put("total", vfs.size());
            row.put("confirmed", confirmed);
            row.put("removed", removed);
            row.put("newOps", newOps);
            row.put("accepted", accepted);
            row.put("rejected", rejected);
            row.put("pending", pending);
            rows.add(row);
        }
        return rows;
    }

    // ==================== CHUNKS ====================

    /**
     * GET /api/parser/analyses/{name}/claude/chunks
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
     * GET /api/parser/analyses/{name}/claude/chunks/summary
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
     * GET /api/parser/analyses/{name}/claude/chunks/{id}
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
     * GET /api/parser/analyses/{name}/claude/table-chunks
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
                if (result.calls != null) {
                    for (var cv : result.calls) {
                        if (cv.userDecision == null) { cv.userDecision = bulkDecision; updated++; }
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

                // Call-edge decisions: [{ fromProcedure, toProcedure, status, decision }]
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> callDecisions =
                        (List<Map<String, Object>>) body.get("callDecisions");
                if (callDecisions != null && result.calls != null) {
                    for (Map<String, Object> dec : callDecisions) {
                        String from = (String) dec.get("fromProcedure");
                        String to = (String) dec.get("toProcedure");
                        String status = (String) dec.get("status");
                        String decision = (String) dec.get("decision");
                        for (var cv : result.calls) {
                            if (cv.toProcedure != null && cv.toProcedure.equalsIgnoreCase(to)
                                    && (status == null || status.equalsIgnoreCase(cv.status))
                                    && (from == null || (cv.fromProcedure != null && cv.fromProcedure.equalsIgnoreCase(from)))) {
                                cv.userDecision = decision;
                                updated++;
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
     * GET /api/parser/analyses/{name}/claude/versions
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
     * GET /api/parser/claude/stats
     * Total API calls and prompt bytes sent to Claude CLI since server start.
     */
    /**
     * GET /api/parser/claude/stats — the seven Claude usage totals the Sessions modal renders.
     *
     * <p>Carries an {@code ETag} so a poll that finds nothing new is answered with a 304 of about
     * 200 bytes and no render at all. The tag is the total length of the call logs
     * ({@code ClaudeCallLogger.globalWatermark()}), which is monotonic because the logs are
     * append-only — not a hash of the body, since computing the body in order to decide not to
     * send it would defeat the purpose (PLAN-08 §5.4).
     *
     * <p>{@code Cache-Control: no-cache} means revalidate every time, never serve stale: these are
     * live counters.
     */
    @GetMapping("/claude/stats")
    public ResponseEntity<Map<String, Object>> getClaudeStats(
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {
        String etag = "\"calls-" + callLogger.globalWatermark() + "\"";
        if (etag.equals(ifNoneMatch)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED)
                    .eTag(etag).cacheControl(CacheControl.noCache()).build();
        }
        Map<String, Object> stats = new LinkedHashMap<>(processRunner.getStats());
        stats.put("sessions", sessionManager.getSummary());
        // Persistent aggregate across all analyses (survives restarts) + per-session breakdown.
        stats.put("log", callLogger.getGlobalStats());
        return ResponseEntity.ok().eTag(etag).cacheControl(CacheControl.noCache()).body(stats);
    }

    /**
     * GET /api/parser/analyses/{name}/claude/call-log?sinceSeq=&limit= — the call-log delta.
     *
     * <p>Q10 in PLAN-08 §1.1. The call log is append-only, so a client that has read up to line
     * {@code n} asks for {@code sinceSeq=n} and receives only the records appended since — normally
     * none, occasionally two or three. With no parameters it returns the first page from the start,
     * so the legacy full read is still expressible.
     */
    @GetMapping("/analyses/{name}/claude/call-log")
    public ResponseEntity<Page<Map<String, Object>>> getCallLogDelta(
            @PathVariable("name") String name,
            @RequestParam(required = false) Long sinceSeq,
            @RequestParam(required = false) Integer limit) {
        return ResponseEntity.ok()
                .eTag("\"calls-" + name + "-" + callLogger.watermark(name) + "\"")
                .cacheControl(CacheControl.noCache())
                .body(callLogger.callLog(name, sinceSeq, limit));
    }

    /** GET /api/parser/analyses/{name}/claude/stats — persistent per-analysis Claude usage. */
    @GetMapping("/analyses/{name}/claude/stats")
    public ResponseEntity<Map<String, Object>> getAnalysisClaudeStats(@PathVariable("name") String name) {
        return ResponseEntity.ok(callLogger.getStats(name));
    }

    /** GET /api/parser/analyses/{name}/claude/stats/{sessionId} — calls in one session. */
    @GetMapping("/analyses/{name}/claude/stats/{sessionId}")
    public ResponseEntity<List<Map<String, Object>>> getAnalysisClaudeSessionDetail(
            @PathVariable("name") String name, @PathVariable("sessionId") String sessionId) {
        return ResponseEntity.ok(callLogger.getSessionDetail(name, sessionId));
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
