package com.plsqlanalyzer.web.parser.model;

import java.util.List;

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
    private List<String> errors;

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

    public int getDbCallCount() { return dbCallCount; }
    public void setDbCallCount(int dbCallCount) { this.dbCallCount = dbCallCount; }

    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }
}
