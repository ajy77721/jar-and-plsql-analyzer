package com.plsqlanalyzer.web.parser.service;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the correct AnalysisDataReader based on the active mode per analysis.
 * Mode is set via the Claude version endpoints (load-static, load-claude, load-prev).
 * Default mode is "static" — always safe fallback.
 */
@Component("parserAnalysisReaderFactory")
public class AnalysisReaderFactory {

    private final StaticAnalysisReader staticReader;
    private final ClaudeAnalysisReader claudeReader;
    private final Map<String, String> modeMap = new ConcurrentHashMap<>();

    public AnalysisReaderFactory(StaticAnalysisReader staticReader,
                                 ClaudeAnalysisReader claudeReader) {
        this.staticReader = staticReader;
        this.claudeReader = claudeReader;
    }

    public AnalysisDataReader getReader(String analysisName) {
        String mode = modeMap.getOrDefault(analysisName, "static");
        return "claude".equals(mode) || "claude_prev".equals(mode)
                ? claudeReader : staticReader;
    }

    public void setMode(String analysisName, String mode) {
        modeMap.put(analysisName, mode != null ? mode : "static");
    }

    public String getMode(String analysisName) {
        return modeMap.getOrDefault(analysisName, "static");
    }
}
