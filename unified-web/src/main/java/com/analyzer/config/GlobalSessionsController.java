package com.analyzer.config;

import com.jaranalyzer.service.ClaudeCallLogger;
import com.jaranalyzer.service.ClaudeSessionManager;
import com.jaranalyzer.service.PersistenceService;
import com.plsqlanalyzer.web.parser.service.ClaudeProcessRunner;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/sessions")
public class GlobalSessionsController {

    private final ClaudeSessionManager jarSessionManager;
    private final com.plsqlanalyzer.web.service.ClaudeSessionManager plsqlSessionManager;
    private final com.plsqlanalyzer.web.parser.service.ClaudeSessionManager parserSessionManager;
    private final ClaudeCallLogger jarCallLogger;
    private final PersistenceService jarPersistenceService;
    private final ClaudeProcessRunner parserProcessRunner;
    private final com.plsqlanalyzer.web.service.ClaudeProcessRunner plsqlProcessRunner;

    public GlobalSessionsController(
            ClaudeSessionManager jarSessionManager,
            @Qualifier("plsqlClaudeSessionManager") com.plsqlanalyzer.web.service.ClaudeSessionManager plsqlSessionManager,
            @Qualifier("parserClaudeSessionManager") com.plsqlanalyzer.web.parser.service.ClaudeSessionManager parserSessionManager,
            ClaudeCallLogger jarCallLogger,
            PersistenceService jarPersistenceService,
            @Qualifier("parserClaudeProcessRunner") ClaudeProcessRunner parserProcessRunner,
            @Qualifier("plsqlClaudeProcessRunner") com.plsqlanalyzer.web.service.ClaudeProcessRunner plsqlProcessRunner) {
        this.jarSessionManager = jarSessionManager;
        this.plsqlSessionManager = plsqlSessionManager;
        this.parserSessionManager = parserSessionManager;
        this.jarCallLogger = jarCallLogger;
        this.jarPersistenceService = jarPersistenceService;
        this.parserProcessRunner = parserProcessRunner;
        this.plsqlProcessRunner = plsqlProcessRunner;
    }

    @GetMapping
    public List<Map<String, Object>> listAll() {
        List<Map<String, Object>> all = new ArrayList<>();

        for (Map<String, Object> s : jarSessionManager.listSessions()) {
            s.put("analyzer", "JAR");
            all.add(s);
        }
        for (Map<String, Object> s : plsqlSessionManager.listSessions(null)) {
            s.put("analyzer", "PL/SQL");
            all.add(s);
        }
        for (Map<String, Object> s : parserSessionManager.listSessions(null)) {
            s.put("analyzer", "Parser");
            all.add(s);
        }

        all.sort((a, b) -> {
            String sa = String.valueOf(a.getOrDefault("startedAt", ""));
            String sb = String.valueOf(b.getOrDefault("startedAt", ""));
            return sb.compareTo(sa);
        });
        return all;
    }

    /**
     * GET /api/sessions/running — is any Claude session running?
     *
     * <p>The single most wasteful recurring call in the suite was a full session list, fetched
     * every 30 s by every open page (and twice per tick on the Corrections tab), so the browser
     * could evaluate {@code sessions.some(s => s.status === 'RUNNING')}. Hundreds of rows, each
     * with its type, detail, timestamps and formatted duration, to produce one bit.
     *
     * <p>An endpoint rather than a stored column, deliberately (PLAN-08 §6.2): this is a predicate
     * over a whole table that changes on every session transition, so a column would need
     * invalidating on writes that have nothing to do with any one analysis. Read straight off the
     * three in-memory session maps, short-circuiting on the first RUNNING session.
     *
     * @param analyzer optional scope — {@code JAR}, {@code Parser} or {@code PL/SQL}. Absent means
     *                 all three. The scope matters: the JAR sidebar's activity dot has always
     *                 reflected JAR sessions only, and answering it globally would light the dot
     *                 for an unrelated PL/SQL run.
     */
    @GetMapping("/running")
    public Map<String, Object> anyRunning(@RequestParam(required = false) String analyzer) {
        boolean running;
        if (analyzer == null || analyzer.isBlank()) {
            running = jarSessionManager.anyRunning()
                   || plsqlSessionManager.anyRunning()
                   || parserSessionManager.anyRunning();
        } else {
            running = switch (analyzer.trim().toUpperCase(Locale.ROOT)) {
                case "JAR" -> jarSessionManager.anyRunning();
                case "PARSER" -> parserSessionManager.anyRunning();
                case "PLSQL", "PL/SQL" -> plsqlSessionManager.anyRunning();
                default -> throw new org.springframework.web.server.ResponseStatusException(
                        org.springframework.http.HttpStatus.BAD_REQUEST,
                        "Unknown analyzer '" + analyzer + "'; expected JAR, Parser or PL/SQL");
            };
        }
        return Map.of("running", running);
    }

    @GetMapping("/summary")
    public Map<String, Object> summary() {
        List<Map<String, Object>> all = listAll();
        int running = 0, complete = 0, failed = 0, killed = 0;
        for (var s : all) {
            String status = String.valueOf(s.getOrDefault("status", ""));
            if ("RUNNING".equals(status)) running++;
            else if ("COMPLETE".equals(status)) complete++;
            else if ("FAILED".equals(status)) failed++;
            else if ("KILLED".equals(status)) killed++;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", all.size());
        result.put("running", running);
        result.put("complete", complete);
        result.put("failed", failed);
        result.put("killed", killed);
        result.put("plsql", plsqlSessionManager.getSummary());
        result.put("parser", parserSessionManager.getSummary());
        return result;
    }

    /** GET /api/sessions/claude-stats — aggregated Claude API usage across all analyzers */
    @GetMapping("/claude-stats")
    public Map<String, Object> claudeStats() {
        // Parser (PL/SQL Parse Analyzer) — in-memory counters
        Map<String, Object> parserStats = parserProcessRunner.getStats();
        long parserCalls = toLong(parserStats.get("totalCalls"));
        long parserBytes = toLong(parserStats.get("totalPromptBytes"));
        Map<String, Integer> parserSessions = parserSessionManager.getSummary();

        // JAR Analyzer — file-based per-JAR call logs; sum across all JARs
        long jarCalls = 0, jarPromptKb = 0;
        int jarSessions = 0;
        try {
            List<Map<String, Object>> jars = jarPersistenceService.listJars();
            for (Map<String, Object> jar : jars) {
                String name = String.valueOf(jar.getOrDefault("id", jar.getOrDefault("name", "")));
                if (name.isBlank()) continue;
                Map<String, Object> s = jarCallLogger.getStats(name);
                jarCalls    += toLong(s.get("totalCalls"));
                jarPromptKb += toLong(s.get("totalPromptSizeKb"));
                jarSessions += toInt(s.get("sessionCount"));
            }
        } catch (Exception ignored) {}

        // PLSQL enrichment sessions + call stats
        Map<String, Integer> plsqlSessions = plsqlSessionManager.getSummary();
        Map<String, Object> plsqlStats = plsqlProcessRunner.getStats();
        long plsqlCalls = toLong(plsqlStats.get("totalCalls"));
        long plsqlBytes = toLong(plsqlStats.get("totalPromptBytes"));

        long totalCalls   = parserCalls + jarCalls + plsqlCalls;
        long totalBytes   = parserBytes + plsqlBytes + (jarPromptKb * 1024L);
        int  totalSessions = parserSessions.getOrDefault("total", 0)
                           + plsqlSessions.getOrDefault("total", 0)
                           + jarSessions;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalCalls",   totalCalls);
        result.put("totalPromptBytes", totalBytes);
        result.put("totalSessions", totalSessions);

        // Per-analyzer breakdown
        Map<String, Object> jar = new LinkedHashMap<>();
        jar.put("calls",    jarCalls);
        jar.put("promptKb", jarPromptKb);
        jar.put("sessions", jarSessions);
        result.put("jar", jar);

        Map<String, Object> parser = new LinkedHashMap<>();
        parser.put("calls",        parserCalls);
        parser.put("promptBytes",  parserBytes);
        parser.put("sessions",     parserSessions.getOrDefault("total", 0));
        parser.put("sessionsRunning", parserSessions.getOrDefault("running", 0));
        result.put("parser", parser);

        Map<String, Object> plsql = new LinkedHashMap<>();
        plsql.put("calls",           plsqlCalls);
        plsql.put("promptBytes",     plsqlBytes);
        plsql.put("sessions",        plsqlSessions.getOrDefault("total", 0));
        plsql.put("sessionsRunning", plsqlSessions.getOrDefault("running", 0));
        result.put("plsql", plsql);

        return result;
    }

    private long toLong(Object v) {
        if (v instanceof Number) return ((Number) v).longValue();
        try { return Long.parseLong(String.valueOf(v)); } catch (Exception e) { return 0L; }
    }
    private int toInt(Object v) {
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(String.valueOf(v)); } catch (Exception e) { return 0; }
    }
}
