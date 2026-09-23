package com.plsqlanalyzer.web.parser.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs the Claude CLI as an external process via ProcessBuilder.
 * Pipes prompts via stdin to avoid the Windows 32KB CLI argument limit.
 * Tracks running processes per thread for kill support.
 */
@Service("parserClaudeProcessRunner")
public class ClaudeProcessRunner {

    private static final Logger log = LoggerFactory.getLogger(ClaudeProcessRunner.class);

    private static final int DEFAULT_TIMEOUT_SECONDS = 600;
    private static final int STREAM_DRAIN_TIMEOUT_MS = 30_000;
    private static final long AVAILABLE_CACHE_MS = 60_000;

    private final Map<Long, Process> runningProcesses = new ConcurrentHashMap<>();
    private final Map<String, String> envVars = new ConcurrentHashMap<>();

    private final AtomicLong totalCalls = new AtomicLong(0);
    private final AtomicLong totalPromptBytes = new AtomicLong(0);
    private final AtomicLong totalResponseBytes = new AtomicLong(0);
    private final AtomicLong totalDurationMs = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    private volatile Boolean cachedAvailable = null;
    private volatile long cachedAvailableAt = 0;

    @Value("${claude.binary:claude}")
    private String claudeBinary;

    @Value("${claude.allowed-tools:Read,Grep,Glob,Bash}")
    private String allowedTools;

    public ClaudeProcessRunner() {
        // No default env vars — use Claude CLI's own authentication (set via `claude login`).
        // To use AWS Bedrock instead, call setEnv("CLAUDE_CODE_USE_BEDROCK", "1") etc. after construction.
    }

    /**
     * Run Claude CLI with the given prompt piped via stdin.
     *
     * @param prompt         Full prompt text
     * @param sessionId      Optional session ID (null for one-shot)
     * @param timeoutSeconds Max wait time (0 = default 600s)
     * @return Claude's response text, or null on error/timeout
     */
    public String run(String prompt, String sessionId, int timeoutSeconds) {
        int timeout = timeoutSeconds <= 0 ? DEFAULT_TIMEOUT_SECONDS : timeoutSeconds;
        long threadId = Thread.currentThread().getId();
        Process process = null;

        totalCalls.incrementAndGet();
        if (prompt != null) totalPromptBytes.addAndGet(prompt.length());
        long __startMs = System.currentTimeMillis();

        try {
            ProcessBuilder pb = buildProcess(sessionId);
            process = pb.start();
            runningProcesses.put(threadId, process);

            // Pipe prompt via stdin in a dedicated thread with 64KB buffer
            final byte[] bytes = prompt.getBytes(StandardCharsets.UTF_8);
            final Process proc = process;
            Thread stdinWriter = new Thread(() -> {
                try {
                    OutputStream os = proc.getOutputStream();
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
            }, "claude-stdin-" + threadId);
            stdinWriter.setDaemon(true);
            stdinWriter.start();

            // Read stdout/stderr in separate threads to prevent deadlock
            StringBuilder stdout = new StringBuilder();
            StringBuilder stderr = new StringBuilder();

            Thread stdoutReader = createStreamReader(proc.getInputStream(), stdout, "claude-stdout-" + threadId);
            Thread stderrReader = createStreamReader(proc.getErrorStream(), stderr, "claude-stderr-" + threadId);
            stdoutReader.start();
            stderrReader.start();

            boolean finished = process.waitFor(timeout, TimeUnit.SECONDS);
            if (!finished) {
                log.warn("Claude process timed out after {}s, destroying", timeout);
                destroyProcessTree(process);
                stdoutReader.interrupt();
                stderrReader.interrupt();
                totalErrors.incrementAndGet();
                return null;
            }

            stdoutReader.join(STREAM_DRAIN_TIMEOUT_MS);
            stderrReader.join(STREAM_DRAIN_TIMEOUT_MS);

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                log.warn("Claude CLI exited with code {}: stderr={}",
                        exitCode, truncate(stderr, 500));
            }

            String result = stdout.toString().trim();
            if (result.isEmpty() && stderr.length() > 0) {
                log.warn("Claude returned empty stdout. stderr: {}", truncate(stderr, 300));
                totalErrors.incrementAndGet();
                return null;
            }
            totalResponseBytes.addAndGet(result.getBytes(StandardCharsets.UTF_8).length);
            totalDurationMs.addAndGet(System.currentTimeMillis() - __startMs);
            return result;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Claude process interrupted (killed)");
            if (process != null) destroyProcessTree(process);
            return null;
        } catch (IOException e) {
            log.error("Failed to run Claude CLI: {}", e.getMessage());
            totalErrors.incrementAndGet();
            return null;
        } finally {
            runningProcesses.remove(threadId);
            if (process != null && process.isAlive()) {
                destroyProcessTree(process);
            }
        }
    }

    /** Kill the Claude process running on the given thread. */
    public boolean killProcess(long threadId) {
        Process p = runningProcesses.get(threadId);
        if (p != null && p.isAlive()) {
            destroyProcessTree(p);
            runningProcesses.remove(threadId);
            log.info("Killed Claude process for thread {}", threadId);
            return true;
        }
        return false;
    }

    /** Kill all running Claude processes. */
    public int killAll() {
        int killed = 0;
        for (var entry : runningProcesses.entrySet()) {
            Process p = entry.getValue();
            if (p != null && p.isAlive()) {
                destroyProcessTree(p);
                killed++;
            }
        }
        runningProcesses.clear();
        if (killed > 0) log.info("Killed {} Claude processes", killed);
        return killed;
    }

    /** Check if Claude CLI is available on the system (cached 60s). */
    public boolean isAvailable() {
        if (cachedAvailable != null && (System.currentTimeMillis() - cachedAvailableAt) < AVAILABLE_CACHE_MS) {
            return cachedAvailable;
        }
        try {
            ProcessBuilder pb = new ProcessBuilder(claudeBinary, "--version");
            configureEnvironment(pb);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            boolean finished = p.waitFor(10, TimeUnit.SECONDS);
            if (!finished) {
                p.destroyForcibly();
                cachedAvailable = false;
                cachedAvailableAt = System.currentTimeMillis();
                return false;
            }
            boolean available = p.exitValue() == 0;
            cachedAvailable = available;
            cachedAvailableAt = System.currentTimeMillis();
            return available;
        } catch (Exception e) {
            log.debug("Claude CLI not available: {}", e.getMessage());
            cachedAvailable = false;
            cachedAvailableAt = System.currentTimeMillis();
            return false;
        }
    }

    public int getRunningCount() {
        return (int) runningProcesses.values().stream().filter(Process::isAlive).count();
    }

    public Map<String, Object> getStats() {
        Map<String, Object> s = new LinkedHashMap<>();
        long calls = totalCalls.get();
        long promptBytes = totalPromptBytes.get();
        long respBytes = totalResponseBytes.get();
        long durMs = totalDurationMs.get();
        s.put("totalCalls", calls);
        s.put("totalPromptBytes", promptBytes);
        s.put("totalResponseBytes", respBytes);
        s.put("totalPromptMb", round2(promptBytes / 1048576.0));
        s.put("totalResponseMb", round2(respBytes / 1048576.0));
        s.put("totalDurationMs", durMs);
        s.put("avgResponseBytes", calls > 0 ? respBytes / calls : 0);
        s.put("avgDurationMs", calls > 0 ? durMs / calls : 0);
        s.put("errors", totalErrors.get());
        return s;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    public void setEnv(String key, String value) {
        envVars.put(key, value);
    }

    // ---- Internal ----

    private ProcessBuilder buildProcess(String sessionId) {
        List<String> cmd = new ArrayList<>();
        cmd.add(claudeBinary);
        if (allowedTools != null && !allowedTools.isBlank()) {
            cmd.add("--allowedTools");
            cmd.add(allowedTools);
        }
        cmd.add("-p");
        if (sessionId != null) {
            cmd.add("--session-id");
            cmd.add(sessionId);
        }
        cmd.add("--no-session-persistence");

        ProcessBuilder pb = new ProcessBuilder(cmd);
        configureEnvironment(pb);
        pb.redirectErrorStream(false);
        return pb;
    }

    private void configureEnvironment(ProcessBuilder pb) {
        pb.environment().putAll(envVars);
    }

    private void destroyProcessTree(Process p) {
        if (p == null || !p.isAlive()) return;
        try {
            p.toHandle().descendants().forEach(ph -> {
                try { ph.destroyForcibly(); } catch (Exception ignored) {}
            });
        } catch (Exception e) {
            log.debug("Failed to kill descendant processes: {}", e.getMessage());
        }
        p.destroyForcibly();
    }

    private Thread createStreamReader(InputStream is, StringBuilder target, String name) {
        Thread t = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    target.append(line).append("\n");
                }
            } catch (IOException e) {
                if (!Thread.currentThread().isInterrupted()) {
                    log.debug("{} read interrupted: {}", name, e.getMessage());
                }
            }
        }, name);
        t.setDaemon(true);
        return t;
    }

    private static String truncate(StringBuilder sb, int max) {
        return sb.length() > max ? sb.substring(0, max) + "..." : sb.toString();
    }
}
