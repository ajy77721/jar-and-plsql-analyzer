package com.plsqlanalyzer.web.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Runs the Claude CLI as an external process via ProcessBuilder.
 * Supports stdin piping for large prompts and per-thread process tracking for kill support.
 *
 * Adapted from jar-analyzer's ClaudeProcessRunner for PL/SQL verification use case.
 */
public class ClaudeProcessRunner {

    private static final Logger log = LoggerFactory.getLogger(ClaudeProcessRunner.class);

    /** Default timeout in seconds for Claude CLI invocation */
    private static final int DEFAULT_TIMEOUT_SECONDS = 600; // 10 minutes

    /** Track running processes per thread for kill support */
    private final Map<Long, Process> runningProcesses = new ConcurrentHashMap<>();

    /** Path to the Claude CLI binary — "claude" assumes it's on PATH */
    private String claudeBinary = "claude";

    /** Pre-approved tools so Claude CLI never prompts for interactive permission */
    private String allowedTools = "Read,Grep,Glob,Bash";

    /** Seconds to wait for stdout/stderr reader threads to finish after process exits */
    private static final int STREAM_DRAIN_TIMEOUT_MS = 30_000;

    /** Cache isAvailable() result for 60s to avoid spawning a process on every API call */
    private volatile Boolean cachedAvailable = null;
    private volatile long cachedAvailableAt = 0;
    private static final long AVAILABLE_CACHE_MS = 60_000;

    /** Environment variables to set for the Claude process */
    private final Map<String, String> envVars = new ConcurrentHashMap<>();

    public ClaudeProcessRunner() {
        // No default env vars — use Claude CLI's own authentication (set via `claude login`).
        // To use AWS Bedrock instead, call setEnv("CLAUDE_CODE_USE_BEDROCK", "1") etc. after construction.
    }

    /**
     * Run Claude CLI with the given prompt piped via stdin.
     * Uses: claude -p --no-session-persistence
     *
     * @param prompt The full prompt text to send
     * @param sessionId Optional session ID for continuity (null for one-shot)
     * @param timeoutSeconds Max seconds to wait (0 = use default)
     * @return Claude's response text, or null on error
     */
    public String run(String prompt, String sessionId, int timeoutSeconds) {
        final int timeout = timeoutSeconds <= 0 ? DEFAULT_TIMEOUT_SECONDS : timeoutSeconds;

        long threadId = Thread.currentThread().getId();
        Process process = null;

        try {
            ProcessBuilder pb = buildProcess(sessionId);
            process = pb.start();
            runningProcesses.put(threadId, process);

            // Pipe prompt via stdin in a dedicated thread with buffered I/O.
            // Matches JAR implementation: avoids blocking the main thread and
            // prevents stalls on large prompts (>64KB) that fill the OS pipe buffer.
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

            // Read stdout/stderr in separate threads to avoid deadlock
            StringBuilder stdout = new StringBuilder();
            StringBuilder stderr = new StringBuilder();

            Thread stdoutReader = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stdout.append(line).append("\n");
                    }
                } catch (IOException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        log.debug("stdout read interrupted: {}", e.getMessage());
                    }
                }
            }, "claude-stdout-" + threadId);

            Thread stderrReader = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(proc.getErrorStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stderr.append(line).append("\n");
                    }
                } catch (IOException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        log.debug("stderr read interrupted: {}", e.getMessage());
                    }
                }
            }, "claude-stderr-" + threadId);

            stdoutReader.start();
            stderrReader.start();

            boolean finished = process.waitFor(timeout, TimeUnit.SECONDS);

            if (!finished) {
                log.warn("Claude process timed out after {}s, destroying process tree", timeout);
                destroyProcessTree(process);
                stdoutReader.interrupt();
                stderrReader.interrupt();
                return null;
            }

            // Wait for readers to drain — 30s matches JAR (was 5s, truncated large responses)
            stdoutReader.join(STREAM_DRAIN_TIMEOUT_MS);
            stderrReader.join(STREAM_DRAIN_TIMEOUT_MS);

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                log.warn("Claude CLI exited with code {}: stderr={}", exitCode,
                        stderr.length() > 500 ? stderr.substring(0, 500) + "..." : stderr);
            }

            String result = stdout.toString().trim();
            if (result.isEmpty() && stderr.length() > 0) {
                log.warn("Claude returned empty stdout. stderr: {}",
                        stderr.length() > 300 ? stderr.substring(0, 300) : stderr);
                return null;
            }

            return result;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Claude process interrupted (killed)");
            if (process != null) destroyProcessTree(process);
            return null;
        } catch (IOException e) {
            log.error("Failed to run Claude CLI: {}", e.getMessage());
            return null;
        } finally {
            runningProcesses.remove(threadId);
            if (process != null && process.isAlive()) {
                destroyProcessTree(process);
            }
        }
    }

    /**
     * Kill the Claude process running on the given thread.
     */
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

    /**
     * Kill all running Claude processes.
     */
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

    /**
     * Destroy a process and all its descendants (prevents orphaned child processes).
     */
    private void destroyProcessTree(Process p) {
        if (p == null || !p.isAlive()) return;
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
     * Check if Claude CLI is available on the system.
     */
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

    // ---- Configuration ----

    public void setClaudeBinary(String path) {
        this.claudeBinary = path;
    }

    public void setEnv(String key, String value) {
        envVars.put(key, value);
    }

    public void setAllowedTools(String tools) {
        this.allowedTools = tools;
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
        Map<String, String> env = pb.environment();
        env.putAll(envVars);
    }
}
