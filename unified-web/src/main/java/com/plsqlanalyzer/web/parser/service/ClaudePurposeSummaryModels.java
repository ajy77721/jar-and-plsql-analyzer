package com.plsqlanalyzer.web.parser.service;

import java.util.ArrayList;
import java.util.List;

/**
 * Data classes for §267's purpose-summarization pipeline — modeled directly on
 * {@link ClaudeVerificationModels}, but for a distinct prompt intent: "what does this procedure do"
 * rather than "is this static finding still correct". Kept in its own file/class set rather than
 * folded into {@code ClaudeVerificationModels} because the two pipelines answer unrelated questions
 * and share only the CLI-invocation/chunking *pattern*, not any data.
 */
public final class ClaudePurposeSummaryModels {

    private ClaudePurposeSummaryModels() {}

    /** One procedure to summarize: its identity, already-extracted structural facts, and source. */
    static class PurposeTarget {
        String nodeId;
        String name;
        String schema;
        String packageName;
        String objectType;
        String sourceSnippet;
        /** SHA-256 hex digest of {@code sourceSnippet} — the staleness/cache key. */
        String sourceDigest;
        List<String> tableSummaries = new ArrayList<>();
        List<String> callSummaries = new ArrayList<>();
    }

    /** A batch of targets sent to Claude in a single CLI invocation. */
    static class PurposeChunk {
        String id;
        List<PurposeTarget> targets = new ArrayList<>();
    }

    /** One procedure's parsed result from a Claude response. */
    public static class ProcedurePurpose {
        public String nodeId;
        public String purposeSummary;
        public List<StatementIntent> perStatement = new ArrayList<>();
    }

    public static class StatementIntent {
        public int line;
        public String intent;
    }

    /** Persisted cache entry — one per node, keyed by analysis+nodeId, invalidated by sourceDigest. */
    public static class PurposeCacheEntry {
        public String nodeId;
        public String purposeSummary;
        public List<StatementIntent> perStatement = new ArrayList<>();
        public String sourceDigest;
        public String generatedAt;
    }

    /** Outcome of one generation run. */
    public static class PurposeSummaryRunResult {
        public String analysisName;
        public int totalProcedures;
        public int generated;
        public int skippedCached;
        public int failed;
        public int claudeCallCount;
        public long claudeTimeMs;
        public String error;
    }
}
