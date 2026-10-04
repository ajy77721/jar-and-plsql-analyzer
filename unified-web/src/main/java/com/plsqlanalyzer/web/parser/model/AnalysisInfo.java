package com.plsqlanalyzer.web.parser.model;

import java.util.List;
import java.util.Map;

public class AnalysisInfo {

    private String name;
    private String entryPoint;
    private String entrySchema;
    private int totalNodes;
    private int totalTables;
    private int totalEdges;
    private int totalLinesOfCode;
    private int maxDepth;
    private long crawlTimeMs;
    private int dbCallCount;
    private long timestamp;
    private List<String> errors;

    /** Per-node-type counts ({@code PROCEDURE}/{@code FUNCTION}/{@code TRIGGER}/…), when known. */
    private Map<String, Integer> typeCounts;

    // Connection context stored at analysis time
    private String project;
    private String environment;
    private String connectionName;
    private String jdbcUrl;

    public AnalysisInfo() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getEntryPoint() { return entryPoint; }
    public void setEntryPoint(String entryPoint) { this.entryPoint = entryPoint; }

    public String getEntrySchema() { return entrySchema; }
    public void setEntrySchema(String entrySchema) { this.entrySchema = entrySchema; }

    public int getTotalNodes() { return totalNodes; }
    public void setTotalNodes(int totalNodes) { this.totalNodes = totalNodes; }

    public int getTotalTables() { return totalTables; }
    public void setTotalTables(int totalTables) { this.totalTables = totalTables; }

    public int getTotalEdges() { return totalEdges; }
    public void setTotalEdges(int totalEdges) { this.totalEdges = totalEdges; }

    public int getTotalLinesOfCode() { return totalLinesOfCode; }
    public void setTotalLinesOfCode(int totalLinesOfCode) { this.totalLinesOfCode = totalLinesOfCode; }

    public int getMaxDepth() { return maxDepth; }
    public void setMaxDepth(int maxDepth) { this.maxDepth = maxDepth; }

    public long getCrawlTimeMs() { return crawlTimeMs; }
    public void setCrawlTimeMs(long crawlTimeMs) { this.crawlTimeMs = crawlTimeMs; }

    private long totalDurationMs; // full: crawl + enrichment + write (falls back to crawlTimeMs)
    public long getTotalDurationMs() { return totalDurationMs; }
    public void setTotalDurationMs(long v) { this.totalDurationMs = v; }

    // 3-phase timing breakdown, same shape as JAR/WAR analyses, for the shared Performance
    // Summary UI.
    private long parseMs;
    private long discoverMs;
    private long saveMs;
    public long getParseMs() { return parseMs; }
    public void setParseMs(long v) { this.parseMs = v; }
    public long getDiscoverMs() { return discoverMs; }
    public void setDiscoverMs(long v) { this.discoverMs = v; }
    public long getSaveMs() { return saveMs; }
    public void setSaveMs(long v) { this.saveMs = v; }

    public int getDbCallCount() { return dbCallCount; }
    public void setDbCallCount(int dbCallCount) { this.dbCallCount = dbCallCount; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }

    public Map<String, Integer> getTypeCounts() { return typeCounts; }
    public void setTypeCounts(Map<String, Integer> typeCounts) { this.typeCounts = typeCounts; }

    public String getProject() { return project; }
    public void setProject(String project) { this.project = project; }

    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }

    public String getConnectionName() { return connectionName; }
    public void setConnectionName(String connectionName) { this.connectionName = connectionName; }

    public String getJdbcUrl() { return jdbcUrl; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }
}
