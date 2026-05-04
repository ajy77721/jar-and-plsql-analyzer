package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class FlowResult {
    private String entryPoint;
    private String entrySchema;
    private long crawlTimeMs;
    private int totalObjectsCrawled;
    private int uniqueObjectsCrawled;
    private int totalProcedures;
    private int totalFunctions;
    private int totalPackageProcedures;
    private int totalPackageFunctions;
    private int totalLinesOfCode;
    private int maxDepthReached;
    private int dbCallCount;
    private List<FlowNode> flowNodes = new ArrayList<>();
    private List<FlowEdge> callGraph = new ArrayList<>();
    private List<SchemaTableInfo> allTables = new ArrayList<>();
    private List<String> errors = new ArrayList<>();

    public String getEntryPoint() { return entryPoint; }
    public void setEntryPoint(String entryPoint) { this.entryPoint = entryPoint; }
    public String getEntrySchema() { return entrySchema; }
    public void setEntrySchema(String entrySchema) { this.entrySchema = entrySchema; }
    public long getCrawlTimeMs() { return crawlTimeMs; }
    public void setCrawlTimeMs(long crawlTimeMs) { this.crawlTimeMs = crawlTimeMs; }
    public int getTotalObjectsCrawled() { return totalObjectsCrawled; }
    public void setTotalObjectsCrawled(int totalObjectsCrawled) { this.totalObjectsCrawled = totalObjectsCrawled; }
    public int getUniqueObjectsCrawled() { return uniqueObjectsCrawled; }
    public void setUniqueObjectsCrawled(int uniqueObjectsCrawled) { this.uniqueObjectsCrawled = uniqueObjectsCrawled; }
    public int getTotalProcedures() { return totalProcedures; }
    public void setTotalProcedures(int totalProcedures) { this.totalProcedures = totalProcedures; }
    public int getTotalFunctions() { return totalFunctions; }
    public void setTotalFunctions(int totalFunctions) { this.totalFunctions = totalFunctions; }
    public int getTotalPackageProcedures() { return totalPackageProcedures; }
    public void setTotalPackageProcedures(int totalPackageProcedures) { this.totalPackageProcedures = totalPackageProcedures; }
    public int getTotalPackageFunctions() { return totalPackageFunctions; }
    public void setTotalPackageFunctions(int totalPackageFunctions) { this.totalPackageFunctions = totalPackageFunctions; }
    public int getTotalLinesOfCode() { return totalLinesOfCode; }
    public void setTotalLinesOfCode(int totalLinesOfCode) { this.totalLinesOfCode = totalLinesOfCode; }
    public int getMaxDepthReached() { return maxDepthReached; }
    public void setMaxDepthReached(int maxDepthReached) { this.maxDepthReached = maxDepthReached; }
    public int getDbCallCount() { return dbCallCount; }
    public void setDbCallCount(int dbCallCount) { this.dbCallCount = dbCallCount; }
    public List<FlowNode> getFlowNodes() { return flowNodes; }
    public void setFlowNodes(List<FlowNode> flowNodes) { this.flowNodes = flowNodes; }
    public List<FlowEdge> getCallGraph() { return callGraph; }
    public void setCallGraph(List<FlowEdge> callGraph) { this.callGraph = callGraph; }
    public List<SchemaTableInfo> getAllTables() { return allTables; }
    public void setAllTables(List<SchemaTableInfo> allTables) { this.allTables = allTables; }
    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }
}
