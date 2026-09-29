package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.ConversionService;
import com.analyzer.auth.ConnectionGuard;
import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.sso.CapturedSsoContext;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.log.LineBufferTail;
import com.analyzer.storage.log.LogTailBody;
import com.plsqlanalyzer.web.service.ClaudeProcessRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.Stream;

/**
 * Oracle PL/SQL → Java conversion feature.
 *
 * Routes:
 *   POST /api/conversion/convert|validate|fix  — start job
 *   GET  /api/conversion/jobs/{id}             — the whole job (unchanged; report/export path)
 *   GET  /api/conversion/jobs/{id}/status      — the ~10 scalars a poll needs (Q11)
 *   GET  /api/conversion/jobs/{id}/log         — progress-log delta, ?sinceSeq= (Q11b)
 *   GET  /api/conversion/jobs/{id}/live        — live-output delta, ?sinceSeq= (Q11b)
 *   GET  /api/conversion/jobs/{id}/instruction — the immutable Claude prompt, fetched once
 *   GET  /api/conversion/jobs                  — list recent jobs
 *   GET  /api/conversion/claude-status         — check Claude CLI
 *
 * Observability: streams Claude stdout line-by-line into job state in real time.
 * Persistence:   jobs saved to data/conversion/ on every update — survive restart/refresh.
 *
 * <h2>Why the poll is split into four routes</h2>
 *
 * PLAN-03 §1.5 #2 measured this as the worst polled payload in the application: the detail screen
 * re-fetched {@code GET /jobs/{id}} <b>every 1.5 s</b>, and that response carries the entire
 * progress log, up to {@link #MAX_LIVE_LINES} live output lines, and {@code instructionMd} — a
 * document that is written once and never changes again. Nothing about it is incremental, so a job
 * that had produced no output since the last tick still cost its full size, and the client rebuilt
 * the whole log DOM from it.
 *
 * <p>{@code /jobs/{id}} is deliberately <b>untouched</b>: it still returns exactly the same bytes,
 * so an old tab, a bookmark or a script sees no change and this conversion is revertible on its
 * own (PLAN-08 §3.5). The poll moves to {@code /status} (scalars, with an ETag so an unchanged
 * phase costs a 304), the two streams move to {@code seq}-cursor deltas (PLAN-08 §5.3), and the
 * instruction is fetched once when {@code /status} first reports {@code hasInstruction}.
 */
@RestController
@RequestMapping("/api/conversion")
public class ConversionController {

    private static final Logger log = LoggerFactory.getLogger(ConversionController.class);

    private static final int MAX_LIVE_LINES  = 2000;  // rolling buffer cap
    private static final int STREAM_DRAIN_MS = 15_000;
    private static final int TIMEOUT_SEC     = 900;   // 15 min max

    private final ConversionService conversionService;
    private final ClaudeProcessRunner claudeRunner;
    private final ObjectMapper mapper;

    /** Required, not optional — an optional store would leave a silent filesystem fallback. */
    private final ArtifactStore store;

    /** Fails a start fast when the caller is logged in but has not connected a bucket. */
    private final ConnectionGuard connectionGuard;

    /**
     * Used to carry the caller's role credentials onto the bare worker thread (defense in depth
     * alongside the captured routing scope), so a write the job makes signs as the submitter.
     */
    private final SsoCredentialsResolver ssoCredentialsResolver;

    private final ConcurrentHashMap<String, JobState> jobs = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(1);

    /**
     * {@code {data}/conversion} — job state, one small JSON per job.
     *
     * <p>There is no createDirectories any more: {@code ArtifactStore.write} creates whatever
     * parents a local backend needs, and an object store has no directories to create.
     */
    private static final ArtifactKey JOBS = ArtifactKey.of("conversion");

    public ConversionController(ConversionService conversionService,
                                @Qualifier("plsqlClaudeProcessRunner") ClaudeProcessRunner claudeRunner,
                                ObjectMapper mapper,
                                @Qualifier("jarArtifactStore") ArtifactStore store,
                                ConnectionGuard connectionGuard,
                                SsoCredentialsResolver ssoCredentialsResolver) {
        this.conversionService  = conversionService;
        this.claudeRunner       = claudeRunner;
        this.mapper             = mapper;
        this.store              = store;
        this.connectionGuard    = connectionGuard;
        this.ssoCredentialsResolver = ssoCredentialsResolver;
    }

    /**
     * Whether the persisted job history has been read from the store yet.
     *
     * <p>The load is deferred to the first request that touches the job map. At startup nobody is
     * logged in, so the routing store's S3 call would run with no per-user credentials — and a boot
     * path must never depend on the store being reachable. The volatile flag plus double-checked
     * locking makes "first use loads exactly once" cheap on every later request.
     */
    private volatile boolean jobsLoaded = false;

    /**
     * Reads the persisted jobs from the store exactly once, on first use.
     *
     * <p>Only ever runs inside an authenticated request, so the store call signs with that user's
     * credentials instead of failing at boot. Never throws: {@link #loadPersistedJobs()} already
     * swallows an unreachable backend, and the in-memory map stays the answer.
     */
    private void ensureJobsLoaded() {
        if (jobsLoaded) return;                   // fast path, no lock after the first load
        synchronized (this) {
            if (jobsLoaded) return;
            loadPersistedJobs();
            jobsLoaded = true;
        }
    }

    // ── Claude availability ───────────────────────────────────────────────────

    @GetMapping("/claude-status")
    public Map<String, Object> claudeStatus() {
        boolean ok = claudeRunner.isAvailable();
        return Map.of("available", ok,
            "message", ok ? "Claude CLI ready" : "Claude CLI not found — run `claude login` first");
    }

    // ── Job list / status ─────────────────────────────────────────────────────

    @GetMapping("/jobs")
    public List<Map<String, Object>> listJobs() {
        ensureJobsLoaded();
        List<Map<String, Object>> result = new ArrayList<>();
        jobs.values().stream()
            .filter(j -> connectionGuard.owns(j.owner))
            .sorted(Comparator.comparing(j -> -j.startedAt))
            .limit(50)
            .forEach(j -> result.add(j.toMap(false)));
        return result;
    }

    /**
     * The whole job, exactly as before — log, live lines, instruction and all.
     *
     * <p>Kept byte-for-byte so that nothing which already depends on this shape has to change in
     * lockstep. The 1.5 s poll no longer uses it; {@link #jobStatusScalars} does.
     */
    @GetMapping("/jobs/{jobId}")
    public ResponseEntity<Map<String, Object>> jobStatus(@PathVariable String jobId) {
        ensureJobsLoaded();
        JobState s = jobs.get(jobId);
        if (s == null || !connectionGuard.owns(s.owner)) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(s.toMap(true));
    }

    /**
     * Q11 — the scalars a poll actually renders, plus the two stream watermarks.
     *
     * <p>No {@code log[]}, no {@code liveLines[]}, no {@code instructionMd}. {@code logSeq} and
     * {@code liveSeq} are the highest sequence numbers those streams have reached, so the client
     * knows whether to ask {@code /log} and {@code /live} for a delta at all — and a tick during
     * which Claude produced nothing costs one 304.
     *
     * <p><b>ETag.</b> Built from the values that actually change state, and deliberately
     * <em>excluding</em> {@code elapsedMs}: a wall-clock field would change on every tick and make
     * the ETag useless. The client already ticks its own elapsed display from {@code startedAt}
     * once a second without a request, so a 304 loses nothing. Per PLAN-08 §5.4 this is a cheap
     * monotonic value, never a hash of the body — computing the body in order to decide not to send
     * it would defeat the purpose.
     */
    @GetMapping("/jobs/{jobId}/status")
    public ResponseEntity<Map<String, Object>> jobStatusScalars(
            @PathVariable String jobId,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {

        ensureJobsLoaded();
        JobState s = jobs.get(jobId);
        if (s == null || !connectionGuard.owns(s.owner)) return ResponseEntity.notFound().build();

        String etag = "\"" + s.status + '|' + s.phase.hashCode() + '|' + s.log.size() + '|'
                + s.liveAppended.get() + '|' + (s.reportPath != null) + '|'
                + (s.instructionMd != null) + '|' + (s.error == null ? 0 : s.error.hashCode())
                + "\"";
        if (etag.equals(ifNoneMatch)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(etag).build();
        }

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("jobId",       s.jobId);
        m.put("action",      s.action);
        m.put("procedure",   s.req != null ? s.req.procedure() : "");
        m.put("module",      s.req != null ? s.req.module()    : "");
        m.put("projectPath", s.req != null && s.req.projectPath() != null ? s.req.projectPath() : "");
        m.put("status",      s.status);
        m.put("phase",       s.phase);
        m.put("startedAt",   s.startedAt);
        m.put("elapsedMs",   System.currentTimeMillis() - s.startedAt);
        m.put("logSeq",      s.log.size());
        m.put("liveSeq",     s.liveAppended.get());
        m.put("hasInstruction", s.instructionMd != null);
        if (s.reportPath != null) m.put("reportPath", s.reportPath);
        if (s.error      != null) m.put("error",      s.error);

        return ResponseEntity.ok()
                .eTag(etag)
                .cacheControl(CacheControl.noCache())
                .body(m);
    }

    /**
     * Q11b — the progress log as a {@code seq}-cursor delta.
     *
     * <p>{@code sinceSeq} absent means the tail, which is what a first paint wants. Present means
     * "only what arrived after this", which in the steady state is an empty {@code items} array
     * and a watermark.
     */
    @GetMapping("/jobs/{jobId}/log")
    public ResponseEntity<LogTailBody> jobLog(@PathVariable String jobId,
                                              @RequestParam(required = false) Long sinceSeq,
                                              @RequestParam(required = false) Integer limit,
                                              @RequestParam(required = false) String level,
                                              @RequestParam(required = false) String q) {
        ensureJobsLoaded();
        JobState s = jobs.get(jobId);
        if (s == null || !connectionGuard.owns(s.owner)) return ResponseEntity.notFound().build();
        // The progress log is never evicted, so lines ever appended == lines retained.
        List<String> snapshot = new ArrayList<>(s.log);
        return ResponseEntity.ok(LogTailBody.of(LineBufferTail.of(
                snapshot, snapshot.size(), sinceSeq, limit, level, q,
                ConversionController::classifyLine)));
    }

    /**
     * Q11b — the live Claude output as a {@code seq}-cursor delta.
     *
     * <p>The rolling {@link #MAX_LIVE_LINES} cap is why {@link JobState#liveAppended} exists: a
     * client whose cursor has been evicted is told {@code reset:true} with the retained
     * {@code fromSeq}, so it clears its buffer instead of appending new lines onto stale ones.
     */
    @GetMapping("/jobs/{jobId}/live")
    public ResponseEntity<LogTailBody> jobLive(@PathVariable String jobId,
                                               @RequestParam(required = false) Long sinceSeq,
                                               @RequestParam(required = false) Integer limit,
                                               @RequestParam(required = false) String level,
                                               @RequestParam(required = false) String q) {
        ensureJobsLoaded();
        JobState s = jobs.get(jobId);
        if (s == null || !connectionGuard.owns(s.owner)) return ResponseEntity.notFound().build();
        // Snapshot both together: liveAppended must not advance between reading the list and
        // reading the counter, or `fromSeq` would be computed one line short.
        List<String> snapshot;
        long appended;
        synchronized (s.liveLines) {
            snapshot = new ArrayList<>(s.liveLines);
            appended = s.liveAppended.get();
        }
        return ResponseEntity.ok(LogTailBody.of(LineBufferTail.of(
                snapshot, appended, sinceSeq, limit, level, q,
                ConversionController::classifyLine)));
    }

    /**
     * The generated Claude instruction — immutable once written, so it is fetched once.
     *
     * <p>It was in every 1.5 s tick because it was a field of the job document. It is a separate
     * resource because it has a completely different lifetime from everything the poll reads.
     *
     * <p>{@code 204} while the job is still building it: absent is not an error, and answering with
     * a {@code 4xx} would make "not ready yet" indistinguishable from "wrong job id".
     */
    @GetMapping(value = "/jobs/{jobId}/instruction", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> jobInstruction(@PathVariable String jobId) {
        ensureJobsLoaded();
        JobState s = jobs.get(jobId);
        if (s == null || !connectionGuard.owns(s.owner)) return ResponseEntity.notFound().build();
        if (s.instructionMd == null || s.instructionMd.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noCache())
                .body(s.instructionMd);
    }

    /**
     * The level of one live-output line.
     *
     * <p>PLAN-08 §6.1 #3: {@code conversion/index.html} re-ran five regexes over every line of the
     * buffer on every 1.5 s tick to pick a CSS class. The classification is a property of the line,
     * not of the view, so it is computed once here and shipped as {@code level} — which is also
     * what makes {@code ?level=} a server-side filter rather than a browser one.
     *
     * <p>The order of the tests is load-bearing and matches what the browser did, so the rendering
     * is unchanged: a line reading "phase 3 failed" was {@code phase} before and is {@code PHASE}
     * now.
     */
    static String classifyLine(String line) {
        if (line == null || line.isEmpty()) return "";
        String lo = line.toLowerCase(java.util.Locale.ROOT);
        if (PHASE_LINE.matcher(lo).find())                        return "PHASE";
        if (lo.startsWith("✓") || lo.startsWith("✅")
                || DONE_LINE.matcher(lo).find())                  return "DONE";
        if (lo.startsWith("⚠") || WARN_LINE.matcher(lo).find())   return "WARN";
        if (lo.startsWith("❌") || ERROR_LINE.matcher(lo).find())  return "ERROR";
        if (lo.startsWith("ℹ") || lo.contains("info:"))           return "INFO";
        return "";
    }

    private static final java.util.regex.Pattern PHASE_LINE =
            java.util.regex.Pattern.compile("phase \\d|---");
    private static final java.util.regex.Pattern DONE_LINE =
            java.util.regex.Pattern.compile("completed|done|passed");
    private static final java.util.regex.Pattern WARN_LINE =
            java.util.regex.Pattern.compile("warn|caution");
    private static final java.util.regex.Pattern ERROR_LINE =
            java.util.regex.Pattern.compile("error|fail");

    @GetMapping("/jobs/{jobId}/report")
    public ResponseEntity<Map<String, Object>> jobReport(@PathVariable String jobId) {
        ensureJobsLoaded();
        JobState s = jobs.get(jobId);
        if (s == null || !connectionGuard.owns(s.owner)) return ResponseEntity.notFound().build();
        if (s.reportPath == null || s.reportPath.isBlank())
            return ResponseEntity.ok(Map.of("found", false, "error", "No report path recorded yet"));

        // NOT an ArtifactStore read, deliberately. The conversion report is written by the Claude
        // CLI into the user's own Maven project on this machine (see runClaudeStreaming — the CLI
        // edits that checkout in place), so it lives outside the analysis data root entirely and
        // has no key. It stays a local read for the same reason ProcessBuilder needs a real
        // working directory.
        String projectPath = s.req != null ? s.req.projectPath() : null;
        try {
            Path report;
            if (java.nio.file.Paths.get(s.reportPath).isAbsolute()) {
                report = java.nio.file.Paths.get(s.reportPath).normalize();
            } else if (projectPath != null && !projectPath.isBlank()) {
                report = java.nio.file.Paths.get(projectPath).resolve(s.reportPath).normalize();
            } else {
                return ResponseEntity.ok(Map.of("found", false, "error",
                    "Report path is relative but no project path was set: " + s.reportPath));
            }
            if (!Files.exists(report))
                return ResponseEntity.ok(Map.of("found", false, "error",
                    "File not found: " + report, "path", report.toString()));
            String content = Files.readString(report, java.nio.charset.StandardCharsets.UTF_8);
            return ResponseEntity.ok(Map.of("found", true, "content", content, "path", report.toString()));
        } catch (IOException e) {
            return ResponseEntity.ok(Map.of("found", false, "error", e.getMessage()));
        }
    }

    // ── Start jobs ────────────────────────────────────────────────────────────

    @PostMapping("/convert")  public Map<String, Object> convert (@RequestBody ConversionRequest req) { return startJob("convert",  req); }
    @PostMapping("/validate") public Map<String, Object> validate(@RequestBody ConversionRequest req) { return startJob("validate", req); }
    @PostMapping("/fix")      public Map<String, Object> fix     (@RequestBody ConversionRequest req) { return startJob("fix",      req); }

    // ── Internal ──────────────────────────────────────────────────────────────

    private Map<String, Object> startJob(String action, ConversionRequest req) {
        ensureJobsLoaded();
        // Fail fast when the caller is logged in but has not connected a bucket, rather than queue
        // the job and let it fail (or silently use the default target) on the worker thread.
        connectionGuard.requireJarConnected();
        // Record the owner on the request thread, while the identity is resolvable, so a later poll
        // by another user's session can be refused.
        String owner = connectionGuard.currentIdentityKey();
        String jobId = String.valueOf(counter.getAndIncrement());
        JobState state = new JobState(jobId, action, req);
        state.owner = owner;
        jobs.put(jobId, state);
        persistJob(state);

        // The bare worker below has no request context, so every persistJob → store.write(JOBS/…)
        // it makes would fall through to the shared default target. Capture the request thread's
        // routing scope and any installed job context HERE, before the thread starts, and
        // re-install them around the whole runJob. Either capture may carry nothing (install is
        // then a no-op), preserving the default-chain path exactly.
        final RequestScopedArtifactStore.Scope routingScope = store instanceof RequestScopedArtifactStore routed
                ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;
        final CapturedSsoContext jobContext = CapturedSsoContext.capture(ssoCredentialsResolver);

        Thread t = new Thread(() -> {
            try (AutoCloseable routed = routingScope.install();
                 AutoCloseable ctx = jobContext == null ? null : jobContext.install()) {
                runJob(state, action, req);
            } catch (Exception e) {
                log.warn("Conversion job {} failed to run under captured context: {}", jobId, e.getMessage());
            }
        }, "conversion-" + jobId);
        t.setDaemon(true);
        t.start();

        return Map.of("jobId", jobId, "status", "running");
    }

    private void runJob(JobState state, String action, ConversionRequest req) {
        try {
            state.setPhase("Preparing instruction");
            state.addLog("⏳ Building Claude instruction file…");

            String instructionMd = conversionService.buildInstruction(action, req);
            state.instructionMd = instructionMd;
            state.addLog("✓ Instruction ready — " + instructionMd.length() + " chars");
            persistJob(state);

            if (!claudeRunner.isAvailable()) {
                state.fail("Claude CLI not available — run `claude login` first.");
                persistJob(state);
                return;
            }

            state.setPhase("Invoking Claude CLI");
            state.addLog("🤖 Claude CLI started (timeout: " + TIMEOUT_SEC + "s)…");
            persistJob(state);

            // Stream Claude stdout live using our own ProcessBuilder
            boolean ok = runClaudeStreaming(instructionMd, state);

            if (!ok) {
                if (!"failed".equals(state.status)) {
                    state.fail("Claude CLI returned no output or timed out.");
                    persistJob(state);
                }
                return;
            }

            // Extract report path from collected output
            state.reportPath = conversionService.extractReportPath(
                String.join("\n", state.liveLines), req);

            state.addLog("✅ Done — " + state.liveLines.size() + " output lines");
            state.complete();
            persistJob(state);

        } catch (Exception e) {
            log.error("Conversion job {} failed", state.jobId, e);
            state.fail("Error: " + e.getMessage());
            persistJob(state);
        }
    }

    /**
     * Run Claude CLI using --verbose --output-format stream-json so events arrive
     * line-by-line in real time (without a TTY Claude buffers all output until exit).
     *
     * Each JSON line is parsed and converted to a human-readable string that is
     * appended to state.liveLines so the UI can display it live.
     *
     * Event types emitted by Claude:
     *   system/init       — session bootstrap (ignored, just logs model)
     *   assistant         — text or tool-use blocks from Claude
     *   tool_result       — output from a tool call
     *   result            — final summary line (contains the full result text)
     *   error             — error event
     */
    private boolean runClaudeStreaming(String prompt, JobState state) {
        List<String> cmd = new ArrayList<>(List.of("claude",
            "--allowedTools", "Read,Grep,Glob,Bash,Write,Edit",
            "-p", "--no-session-persistence",
            "--verbose", "--output-format", "stream-json"));

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(false);

        Process process = null;
        try {
            process = pb.start();
            final Process proc = process;

            // Write prompt to stdin — must close stream so Claude knows input is done
            byte[] promptBytes = prompt.getBytes(StandardCharsets.UTF_8);
            Thread stdinWriter = new Thread(() -> {
                try (OutputStream os = new BufferedOutputStream(proc.getOutputStream(), 65536)) {
                    int off = 0;
                    while (off < promptBytes.length) {
                        int len = Math.min(8192, promptBytes.length - off);
                        os.write(promptBytes, off, len);
                        off += len;
                    }
                    // flush happens automatically on close via try-with-resources
                } catch (IOException e) {
                    if (!e.getMessage().contains("closed") && !e.getMessage().contains("ended"))
                        log.warn("stdin write error: {}", e.getMessage());
                }
            }, "conv-stdin");
            stdinWriter.setDaemon(true);
            stdinWriter.start();

            // Drain stderr silently (Claude writes some diagnostics there)
            Thread stderrReader = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(
                        new InputStreamReader(proc.getErrorStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        if (!line.isBlank()) log.debug("[claude stderr] {}", line);
                    }
                } catch (IOException ignored) {}
            }, "conv-stderr");
            stderrReader.setDaemon(true);
            stderrReader.start();

            // Read stream-json events LIVE
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String raw;
                int lineCount = 0;
                while ((raw = r.readLine()) != null) {
                    if (raw.isBlank()) continue;
                    String display = parseStreamEvent(raw);
                    if (display != null) {
                        // Split multi-line display strings so each line is its own entry
                        for (String dl : display.split("\n", -1)) {
                            state.appendLiveLine(dl);
                            updatePhaseFromOutput(state, dl);
                        }
                        lineCount++;
                        if (lineCount % 10 == 0) persistJob(state);
                    }
                }
            }

            boolean finished = process.waitFor(TIMEOUT_SEC, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                state.fail("Timed out after " + TIMEOUT_SEC + "s");
                return false;
            }

            stderrReader.join(STREAM_DRAIN_MS);

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                state.addLog("⚠ Claude exited with code " + exitCode);
                log.warn("Claude exited with code {}", exitCode);
            }

            return true;

        } catch (IOException e) {
            log.error("Failed to spawn Claude process: {}", e.getMessage());
            state.fail("Cannot start Claude CLI: " + e.getMessage());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (process != null) process.destroyForcibly();
            state.fail("Interrupted");
            return false;
        } finally {
            if (process != null && process.isAlive()) process.destroyForcibly();
        }
    }

    /**
     * Parse one stream-json line from Claude CLI into a human-readable string,
     * or return null to suppress it from the live feed.
     */
    @SuppressWarnings("unchecked")
    private String parseStreamEvent(String json) {
        try {
            Map<String, Object> ev = mapper.readValue(json, Map.class);
            String type = (String) ev.getOrDefault("type", "");

            return switch (type) {
                case "system" -> {
                    // init event — show model name
                    String sub = (String) ev.getOrDefault("subtype", "");
                    if ("init".equals(sub)) {
                        String model = (String) ev.getOrDefault("model", "");
                        yield "🤖 Session started — model: " + model;
                    }
                    yield null;
                }
                case "assistant" -> {
                    // Extract text and tool-use blocks
                    Object msgObj = ev.get("message");
                    if (!(msgObj instanceof Map)) yield null;
                    Map<String, Object> msg = (Map<String, Object>) msgObj;
                    Object contentObj = msg.get("content");
                    if (!(contentObj instanceof List)) yield null;
                    List<Map<String, Object>> content = (List<Map<String, Object>>) contentObj;
                    StringBuilder sb = new StringBuilder();
                    for (Map<String, Object> block : content) {
                        String btype = (String) block.getOrDefault("type", "");
                        if ("text".equals(btype)) {
                            String text = (String) block.getOrDefault("text", "");
                            if (!text.isBlank()) sb.append(text);
                        } else if ("tool_use".equals(btype)) {
                            String name   = (String) block.getOrDefault("name", "?");
                            Object input  = block.get("input");
                            String detail = "";
                            if (input instanceof Map<?,?> inp) {
                                // Show the most useful field: command, path, pattern, etc.
                                detail = Stream.of("command","path","pattern","query","file_path","glob")
                                    .filter(k -> inp.containsKey(k) && inp.get(k) != null)
                                    .map(k -> inp.get(k).toString())
                                    .findFirst().orElse("");
                            }
                            sb.append("\n🔧 [").append(name).append("] ")
                              .append(detail.length() > 120 ? detail.substring(0, 120) + "…" : detail);
                        }
                    }
                    String out = sb.toString().stripTrailing();
                    yield out.isBlank() ? null : out;
                }
                case "tool_result" -> {
                    // Show a compact summary of what the tool returned
                    Object content = ev.get("content");
                    String text = "";
                    if (content instanceof String s) {
                        text = s;
                    } else if (content instanceof List<?> list && !list.isEmpty()) {
                        Object first = list.get(0);
                        if (first instanceof Map<?,?>) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> m = (Map<String, Object>) first;
                            text = String.valueOf(m.getOrDefault("text", ""));
                        }
                    }
                    if (text.isBlank()) yield null;
                    // Trim long output — first 3 lines
                    String[] lines = text.split("\n", 4);
                    String preview = String.join("\n", java.util.Arrays.copyOf(lines, Math.min(3, lines.length)));
                    if (lines.length > 3) preview += "\n  … +" + (text.split("\n").length - 3) + " more lines";
                    yield "  ↳ " + preview.trim();
                }
                case "result" -> {
                    boolean isErr = Boolean.TRUE.equals(ev.get("is_error"));
                    String res = (String) ev.getOrDefault("result", "");
                    Object dur = ev.getOrDefault("duration_ms", 0);
                    String time = dur instanceof Number n ? (n.longValue() / 1000) + "s" : "";
                    if (isErr) yield "❌ Claude error: " + res;
                    yield "✅ Completed in " + time + (res != null && !res.isBlank() ? "\n" + res : "");
                }
                default -> null;
            };
        } catch (Exception e) {
            log.debug("Could not parse stream event: {}", json.length() > 80 ? json.substring(0,80) : json);
            return null;
        }
    }

    private void updatePhaseFromOutput(JobState state, String line) {
        String lo = line.toLowerCase();
        if (lo.contains("phase 0") || lo.contains("loading claude.md"))
            state.setPhase("Phase 0 — Loading project context");
        else if (lo.contains("phase 0b") || lo.contains("dependency tree"))
            state.setPhase("Phase 0B — Building dependency tree");
        else if (lo.contains("phase 1") && lo.contains("fetch"))
            state.setPhase("Phase 1 — Fetching Oracle source");
        else if (lo.contains("phase 1b") || lo.contains("chunk"))
            state.setPhase("Phase 1B — Chunked analysis");
        else if (lo.contains("phase 2") || lo.contains("decompos"))
            state.setPhase("Phase 2 — Decomposing Oracle logic");
        else if (lo.contains("phase 3") || lo.contains("locating java"))
            state.setPhase("Phase 3 — Locating Java files");
        else if (lo.contains("phase 4") || lo.contains("coverage mapping"))
            state.setPhase("Phase 4 — Logic coverage mapping");
        else if (lo.contains("phase 5") || lo.contains("validate.sh"))
            state.setPhase("Phase 5 — Running validate.sh");
        else if (lo.contains("sonar"))
            state.setPhase("Phase 5B — SonarQube analysis");
        else if (lo.contains("phase 6") || lo.contains("test coverage"))
            state.setPhase("Phase 6 — Test coverage analysis");
        else if (lo.contains("phase 7") || lo.contains("writing report") || lo.contains("validation_report"))
            state.setPhase("Phase 7 — Writing report");
        else if (lo.contains("phase 8") || lo.contains("completion summary"))
            state.setPhase("Phase 8 — Summary");
        else if (lo.contains("applying fix") || lo.contains("fix applied"))
            state.setPhase("Applying fixes");
        else if (lo.contains("mvn") && lo.contains("compile"))
            state.setPhase("Compiling");
        else if (lo.contains("implementing") || lo.contains("generating"))
            state.setPhase("Generating Java code");
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    private void persistJob(JobState state) {
        try (OutputStream out = store.write(JOBS.resolve(state.jobId + ".json"))) {
            mapper.writeValue(out, state.toMap(true));
        } catch (IOException e) {
            log.warn("Failed to persist job {}: {}", state.jobId, e.getMessage());
        }
    }

    private void loadPersistedJobs() {
        try (Stream<ArtifactEntry> stream = store.list(JOBS, false)) {
            stream.filter(java.util.function.Predicate.not(ArtifactEntry::directory))
                  .map(ArtifactEntry::key)
                  .filter(k -> k.leaf().endsWith(".json"))
                  .forEach(p -> {
                      try (InputStream in = store.read(p)) {
                          @SuppressWarnings("unchecked")
                          Map<String, Object> m = mapper.readValue(in, Map.class);
                          JobState s = JobState.fromMap(m);
                          jobs.put(s.jobId, s);
                          long id = Long.parseLong(s.jobId);
                          counter.updateAndGet(c -> Math.max(c, id + 1));
                          // Mark in-flight jobs as interrupted
                          if ("running".equals(s.status)) {
                              s.status = "interrupted";
                              s.phase  = "Interrupted — server restarted";
                              s.addLog("⚠ Job interrupted by server restart");
                          }
                      } catch (Exception e) {
                          log.debug("Could not load persisted job {}: {}", p.leaf(), e.getMessage());
                      }
                  });
        } catch (Exception e) {
            // Storage backend may be unreachable (e.g. missing AWS credentials) —
            // the app must still start, just without persisted job history.
            log.warn("Could not list conversion jobs dir: {}", e.getMessage());
        }
        log.info("Loaded {} persisted conversion jobs", jobs.size());
    }

    // ── Models ────────────────────────────────────────────────────────────────

    public record ConversionRequest(
        String procedure, String module, String service, String controller,
        String owner, String projectPath, String jarId, boolean batch
    ) {}

    static class JobState {
        String jobId;
        String action;
        ConversionRequest req;
        long startedAt = System.currentTimeMillis();

        /**
         * The routing identity of the session that started this job, or {@code null} when none was
         * resolvable (a local/CLI run). Read back on every poll so a different user cannot read
         * another's report by guessing a sequential id; {@code null} stays open for the legacy path.
         */
        volatile String owner;

        volatile String phase  = "Initializing";
        volatile String status = "running";
        volatile String reportPath;
        volatile String instructionMd;
        volatile String error;

        final CopyOnWriteArrayList<String> log      = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> liveLines = new CopyOnWriteArrayList<>();

        /**
         * Lines ever appended to {@link #liveLines} — the {@code seq} watermark of that stream.
         *
         * <p>Not {@code liveLines.size()}: the buffer is rolling (see {@link #appendLiveLine}), so
         * the index of a surviving line changes every time an old one is dropped. A cursor built on
         * the index would silently renumber lines the client already holds and make it re-render
         * output it had already shown. A monotonic count cannot go backwards, and eviction becomes
         * visible as {@code fromSeq} moving forward instead of as duplicated lines.
         */
        final AtomicLong liveAppended = new AtomicLong();

        JobState(String jobId, String action, ConversionRequest req) {
            this.jobId  = jobId;
            this.action = action;
            this.req    = req;
        }

        void addLog(String msg)         { log.add(msg); }
        void setPhase(String p)         { this.phase = p; }
        /**
         * Synchronised on {@code liveLines} purely so that the buffer and its counter can be read
         * together: a reader that saw the list after an append but the counter before it would
         * compute a {@code fromSeq} one line short and report a spurious reset.
         */
        void appendLiveLine(String line) {
            synchronized (liveLines) {
                liveLines.add(line);
                liveAppended.incrementAndGet();
                if (liveLines.size() > MAX_LIVE_LINES) {
                    liveLines.remove(0);
                }
            }
        }
        void complete() { this.status = "done";   this.phase = "Complete"; }
        void fail(String msg) {
            this.status = "failed"; this.phase = "Failed";
            log.add("❌ " + msg); this.error = msg;
        }

        Map<String, Object> toMap(boolean includeLive) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("jobId",       jobId);
            m.put("action",      action);
            m.put("procedure",   req != null ? req.procedure()   : "");
            m.put("module",      req != null ? req.module()      : "");
            m.put("projectPath", req != null && req.projectPath() != null ? req.projectPath() : "");
            m.put("status",      status);
            m.put("phase",       phase);
            m.put("log",         new ArrayList<>(log));
            m.put("startedAt",   startedAt);
            m.put("elapsedMs",   System.currentTimeMillis() - startedAt);
            // Persisted so ownership survives a restart; absent (null) for jobs from before this
            // field existed, which the guard treats as unowned rather than locking anyone out.
            if (owner != null) m.put("owner", owner);
            if (includeLive)     m.put("liveLines",    new ArrayList<>(liveLines));
            if (reportPath    != null) m.put("reportPath",    reportPath);
            // The instruction is immutable, ~70 KB measured, and only the detail screen's Prompt
            // tab has ever displayed it — via GET /jobs/{id}/instruction since PLAN-08 §4.3 step 2.
            // In the list projection it was 70 KB per job of payload with no reader, which made
            // GET /jobs a 70 902 B response for one job. It stays in the full projection, which is
            // also what is persisted, so nothing is lost.
            if (includeLive && instructionMd != null) m.put("instructionMd", instructionMd);
            if (error         != null) m.put("error",         error);
            return m;
        }

        @SuppressWarnings("unchecked")
        static JobState fromMap(Map<String, Object> m) {
            String jobId  = (String) m.getOrDefault("jobId",  "?");
            String action = (String) m.getOrDefault("action", "?");
            String proc   = (String) m.getOrDefault("procedure",   "");
            String mod    = (String) m.getOrDefault("module",      "");
            String path   = (String) m.getOrDefault("projectPath", "");
            ConversionRequest req = new ConversionRequest(proc, mod, null, null, null, path, null, false);
            JobState s = new JobState(jobId, action, req);
            s.phase  = (String) m.getOrDefault("phase",  "Unknown");
            s.status = (String) m.getOrDefault("status", "unknown");
            s.reportPath    = (String) m.get("reportPath");
            s.instructionMd = (String) m.get("instructionMd");
            s.error         = (String) m.get("error");
            s.owner         = (String) m.get("owner");
            Object st = m.get("startedAt");
            if (st instanceof Number n) s.startedAt = n.longValue();
            List<String> logs = (List<String>) m.get("log");
            if (logs != null) s.log.addAll(logs);
            List<String> live = (List<String>) m.get("liveLines");
            if (live != null) {
                s.liveLines.addAll(live);
                // A restored job is finished or interrupted, so nothing further is appended and the
                // surviving buffer is the whole stream: seq 1..size. If it had been truncated
                // before the restart the earlier lines are gone from the persisted copy too, so
                // there is nothing a larger watermark could point at.
                s.liveAppended.set(live.size());
            }
            return s;
        }
    }
}
