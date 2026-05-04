package com.analyzer.config;

import com.jaranalyzer.service.ClaudeSessionManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/sessions")
public class GlobalSessionsController {

    private final ClaudeSessionManager jarSessionManager;
    private final com.plsqlanalyzer.web.service.ClaudeSessionManager plsqlSessionManager;
    private final com.plsqlanalyzer.web.parser.service.ClaudeSessionManager parserSessionManager;

    public GlobalSessionsController(
            ClaudeSessionManager jarSessionManager,
            @Qualifier("plsqlClaudeSessionManager") com.plsqlanalyzer.web.service.ClaudeSessionManager plsqlSessionManager,
            @Qualifier("parserClaudeSessionManager") com.plsqlanalyzer.web.parser.service.ClaudeSessionManager parserSessionManager) {
        this.jarSessionManager = jarSessionManager;
        this.plsqlSessionManager = plsqlSessionManager;
        this.parserSessionManager = parserSessionManager;
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
}
