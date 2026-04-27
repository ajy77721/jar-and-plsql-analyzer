package com.jaranalyzer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs the Claude CLI process, handling stdin piping and stdout/stderr reading.
 * Encapsulates all ProcessBuilder interaction with the Claude binary.
 * Supports concurrent sessions: tracks active processes per-thread so kills
 * target only the correct session's process.
 */
@Component("jarClaudeProcessRunner")
class ClaudeProcessRunner {

    private static final Logger log = LoggerFactory.getLogger(ClaudeProcessRunner.class);

    @Value("${claude.timeout.per-endpoint:7200}")
    private long timeoutSeconds;

    @Value("${claude.timeout.version-check:30}")
    private long versionCheckTimeout;

    @Value("${claude.timeout.stream-drain:30}")
    private long streamDrainTimeout;

    @Value("${claude.allowed-tools:Read,Grep,Glob,Bash,Write,Edit}")
    private String allowedTools;

    /** Active Claude CLI processes keyed by thread ID. Supports concurrent sessions. */
    private final java.util.concurrent.ConcurrentHashMap<Long, Process> activeProcesses = new java.util.concurrent.ConcurrentHashMap<>();

    ClaudeProcessRunner() {}

    long getTimeoutSeconds() { return timeoutSeconds; }

    /**
     * Destroy ALL active Claude CLI processes (used when killing all sessions for a JAR).
     */
    void destroyActiveProcess() {
        for (Map.Entry<Long, Process> entry : activeProcesses.entrySet()) {
            destroyProcess(entry.getValue(), "all-kill");
        }
    }

    /**
     * Destroy the Claude CLI process running on a specific thread (targeted kill).
     */
    void destroyProcessForThread(Thread thread) {
        if (thread == null) return;
        Process p = activeProcesses.get(thread.getId());
        if (p != null) {
            destroyProcess(p, "thread-" + thread.getId());
        }
    }

    private void destroyProcess(Process p, String context) {
        if (p == null || !p.isAlive()) return;
        log.info("Destroying Claude CLI process [{}]", context);
        try {
            p.toHandle().descendants().forEach(ph -> {
                try { ph.destroyForcibly(); }
                catch (Exception ignored) {}
            });
        } catch (Exception e) {
            log.debug("Failed to kill descendant processes: {}", e.getMessage());
        }
        p.destroyForcibly();
    }

    /**
     * Check if Claude CLI is available on the machine.
     */
    boolean isConfigured() {
        try {
            ProcessBuilder pb = new ProcessBuilder("claude", "--version");
            pb.redirectErrorStream(true);
            Process process = pb.start();
            boolean finished = process.waitFor(versionCheckTimeout, TimeUnit.SECONDS);
            if (finished && process.exitValue() == 0) {
                String version = new String(process.getInputStream().readAllBytes()).trim();
                log.info("Claude CLI detected: {}", version);
                return true;
            }
        } catch (Exception e) {
            log.debug("Claude CLI not available: {}", e.getMessage());
        }
        return false;
    }

    /**
     * Run Claude CLI process.
     * @param command  the command args (without prompt when using stdin)
     * @param stdinContent  if non-null, piped to process stdin (avoids CLI arg length limit)
     * @param workDir  working directory for the process (null = current dir)
     * @param progressService  for progress updates
     * @return Claude's stdout output
     */
    String runClaudeProcess(List<String> command, String stdinContent, Path workDir,
                            ProgressService progressService) throws Exception {
        // Inject --allowedTools so Claude CLI never prompts for permissions (works on any machine)
        List<String> fullCommand = new ArrayList<>(command);
        if (allowedTools != null && !allowedTools.isBlank()) {
            fullCommand.add(1, "--allowedTools");
            fullCommand.add(2, allowedTools);
        }
        ProcessBuilder pb = new ProcessBuilder(fullCommand);
        if (workDir != null) pb.directory(workDir.toFile());
        pb.redirectErrorStream(false);

        if (progressService != null) progressService.detail("[Claude CLI] Running" +
                (stdinContent != null ? " (stdin pipe, " + stdinContent.length() + " chars)" : "") + "...");
        Process process = pb.start();
        activeProcesses.put(Thread.currentThread().getId(), process);

        // Pipe prompt via stdin if provided -- this is how we avoid error 206
        if (stdinContent != null) {
            log.info("Piping {} chars ({}KB) to Claude stdin", stdinContent.length(), stdinContent.length() / 1024);
            if (stdinContent.length() > TreeChunker.MAX_PROMPT_CHARS) {
                log.warn("Stdin content exceeds {}KB limit ({} chars) — truncating",
                        TreeChunker.MAX_PROMPT_CHARS / 1000, stdinContent.length());
                stdinContent = stdinContent.substring(0, TreeChunker.MAX_PROMPT_CHARS - 200)
                        + "\n\n[TRUNCATED — input exceeded " + TreeChunker.MAX_PROMPT_CHARS + " char limit]";
            }
            // Write stdin in a dedicated thread with buffered I/O.
            // CRITICAL: Must NOT use CompletableFuture.runAsync() here — it uses the shared
            // ForkJoinPool, which gets saturated during parallel scans (4 JARs × 3 endpoints).
            // Queued write tasks miss Claude CLI's 3-second stdin window, causing
            // "no stdin data received" errors and empty responses.
            final byte[] bytes = stdinContent.getBytes(StandardCharsets.UTF_8);
            Thread stdinWriter = new Thread(() -> {
                try {
                    OutputStream os = process.getOutputStream();
                    BufferedOutputStream bos = new BufferedOutputStream(os, 65536);
                    int off = 0;
                    while (off < bytes.length) {
                        int len = Math.min(8192, bytes.length - off);
                        bos.write(bytes, off, len);
                        off += len;
                    }
                    bos.flush();
                    bos.close();
                } catch (IOException e) {
                    if (!e.getMessage().contains("closed") && !e.getMessage().contains("ended")) {
                        log.warn("Failed to write to Claude stdin: {}", e.getMessage());
                    }
                }
            }, "claude-stdin-" + Thread.currentThread().getId());
            stdinWriter.setDaemon(true);
            stdinWriter.start();
        } else {
            // Close stdin immediately when not used
            process.getOutputStream().close();
        }

        StringBuilder stdoutBuf = new StringBuilder();
        CompletableFuture<Void> stdoutReader = CompletableFuture.runAsync(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stdoutBuf.append(line).append('\n');
                    String trimmed = line.trim();
                    if (progressService != null && !trimmed.startsWith("{") && !trimmed.startsWith("[") && !trimmed.isEmpty()) {
                        progressService.detail("[Claude] " + line);
                    }
                }
            } catch (IOException e) { /* stream closed */ }
        });

        StringBuilder stderrBuf = new StringBuilder();
        CompletableFuture<Void> stderrReader = CompletableFuture.runAsync(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        stderrBuf.append(line).append('\n');
                        log.debug("[Claude stderr] {}", line);
                        if (progressService != null) {
                            progressService.detail("[Claude stderr] " + line);
                        }
                    }
                }
            } catch (IOException e) { /* stream closed */ }
        });

        boolean finished;
        try {
            finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Session was killed — destroy this process cleanly
            destroyProcess(process, "interrupted-" + Thread.currentThread().getId());
            activeProcesses.remove(Thread.currentThread().getId());
            Thread.currentThread().interrupt();
            throw new Exception("Claude process interrupted (session killed)", e);
        }
        if (!finished) {
            // Kill entire process tree on timeout
            try {
                process.toHandle().descendants().forEach(ph -> {
                    try { ph.destroyForcibly(); } catch (Exception ignored) {}
                });
            } catch (Exception ignored) {}
            process.destroyForcibly();
            activeProcesses.remove(Thread.currentThread().getId());
            throw new TimeoutException("Claude CLI timed out after " + timeoutSeconds + "s");
        }

        int exitCode = process.exitValue();
        stdoutReader.get(streamDrainTimeout, TimeUnit.SECONDS);
        stderrReader.get(streamDrainTimeout, TimeUnit.SECONDS);
        activeProcesses.remove(Thread.currentThread().getId());

        if (exitCode != 0) {
            String stderr = stderrBuf.toString().trim();
            log.warn("Claude CLI exited with code {} | stderr: {}", exitCode,
                    stderr.isEmpty() ? "(empty)" : stderr.substring(0, Math.min(500, stderr.length())));
        }

        String result = stdoutBuf.toString();
        return result;
    }

    /**
     * Integration test for the stdin pipe approach.
     * Runs 3 tests through actual ProcessBuilder -> stdin -> Claude -> stdout.
     */
    Map<String, Object> runStdinPipeTest(ClaudeResultMerger resultMerger) {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("timestamp", LocalDateTime.now().toString());

        if (!isConfigured()) {
            results.put("status", "FAIL");
            results.put("error", "Claude CLI not available");
            return results;
        }

        // Dummy progress service for tests
        ProgressService testProgress = new ProgressService();

        // -- Test 1: Basic stdin pipe --
        try {
            log.info("[TEST 1] Basic stdin pipe...");
            List<String> cmd = List.of("claude", "-p", "--no-session-persistence");
            String response = runClaudeProcess(cmd, "Respond with exactly one word: PONG", null, testProgress);
            boolean pass = response != null && response.trim().contains("PONG");
            results.put("test1_basicStdin", Map.of(
                    "status", pass ? "PASS" : "FAIL",
                    "sent", "short prompt via stdin",
                    "received", response != null ? response.trim() : "null",
                    "charsSent", 35
            ));
            log.info("[TEST 1] {}: {}", pass ? "PASS" : "FAIL", response != null ? response.trim() : "null");
        } catch (Exception e) {
            results.put("test1_basicStdin", Map.of("status", "ERROR", "error", e.getMessage()));
            log.warn("[TEST 1] ERROR: {}", e.getMessage());
        }

        // -- Test 2: Large payload stdin (simulates error 206 scenario) --
        try {
            log.info("[TEST 2] Large payload stdin (simulating big JSON)...");
            StringBuilder bigPayload = new StringBuilder();
            bigPayload.append("I am sending you a large JSON payload via stdin. ");
            bigPayload.append("This tests that stdin piping works for payloads that would exceed ");
            bigPayload.append("the Windows 32KB command-line limit (error 206). ");
            bigPayload.append("Here is the payload:\n");
            bigPayload.append("{\"testData\": \"");
            for (int i = 0; i < 4000; i++) {
                bigPayload.append("class_").append(i).append("_Service,");
            }
            bigPayload.append("\"}\n\n");
            bigPayload.append("Respond with exactly: LARGE_PAYLOAD_OK and the approximate character count you received.");

            int payloadSize = bigPayload.length();
            List<String> cmd = List.of("claude", "-p", "--no-session-persistence");
            String response = runClaudeProcess(cmd, bigPayload.toString(), null, testProgress);
            boolean pass = response != null && response.contains("LARGE_PAYLOAD_OK");
            results.put("test2_largeStdin", Map.of(
                    "status", pass ? "PASS" : "FAIL",
                    "charsSent", payloadSize,
                    "kbSent", payloadSize / 1024,
                    "exceedsWinLimit", payloadSize > 32768,
                    "received", response != null ? response.trim().substring(0, Math.min(200, response.trim().length())) : "null"
            ));
            log.info("[TEST 2] {}: sent {}KB via stdin", pass ? "PASS" : "FAIL", payloadSize / 1024);
        } catch (Exception e) {
            results.put("test2_largeStdin", Map.of("status", "ERROR", "error", e.getMessage()));
            log.warn("[TEST 2] ERROR: {}", e.getMessage());
        }

        // -- Test 3: JSON response extraction (what enrichment actually uses) --
        try {
            log.info("[TEST 3] JSON response extraction...");
            String jsonPrompt = "Analyze this endpoint and respond with ONLY a JSON object, no markdown:\n" +
                    "{\"controller\":\"TestController\",\"method\":\"testMethod\"," +
                    "\"callTree\":{\"className\":\"com.test.TestService\",\"children\":[]}}\n\n" +
                    "Respond with this exact JSON: {\"procName\":\"TEST_PROC\",\"riskFlags\":[\"none\"],\"transactionRequired\":false}";

            List<String> cmd = List.of("claude", "-p", "--no-session-persistence");
            String response = runClaudeProcess(cmd, jsonPrompt, null, testProgress);

            String extracted = resultMerger.extractJson(response);
            boolean hasJson = extracted != null;
            boolean hasProcName = hasJson && extracted.contains("procName");

            results.put("test3_jsonExtraction", Map.of(
                    "status", hasJson && hasProcName ? "PASS" : "FAIL",
                    "rawResponseLength", response != null ? response.length() : 0,
                    "jsonExtracted", hasJson,
                    "extractedJson", extracted != null ? extracted.substring(0, Math.min(300, extracted.length())) : "null",
                    "containsProcName", hasProcName
            ));
            log.info("[TEST 3] {}: jsonExtracted={}, procName={}", hasJson && hasProcName ? "PASS" : "FAIL", hasJson, hasProcName);
        } catch (Exception e) {
            results.put("test3_jsonExtraction", Map.of("status", "ERROR", "error", e.getMessage()));
            log.warn("[TEST 3] ERROR: {}", e.getMessage());
        }

        // Summary
        long passed = results.entrySet().stream()
                .filter(e -> e.getKey().startsWith("test"))
                .filter(e -> e.getValue() instanceof Map && "PASS".equals(((Map<?, ?>) e.getValue()).get("status")))
                .count();
        long total = results.entrySet().stream().filter(e -> e.getKey().startsWith("test")).count();
        results.put("summary", passed + "/" + total + " tests passed");
        results.put("status", passed == total ? "ALL_PASS" : "SOME_FAIL");

        log.info("=== Claude Stdin Pipe Test: {} ===", results.get("summary"));
        return results;
    }
}
