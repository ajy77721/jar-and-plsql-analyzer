package com.jaranalyzer.crossref.model;

/** Aggregate statistics for a cross-reference result. */
public class CrossRefSummary {

    private int totalJavaNodes;
    private int highConfidenceLinks;
    private int mediumConfidenceLinks;
    private int lowConfidenceLinks;
    private int unmatchedJavaNodes;       // no Oracle proc name could be inferred
    private int plsqlNodesFound;          // nodes confirmed by FlowNode on disk

    private int totalJavaCollections;
    private int matchedTables;
    private int javaOnlyCollections;
    private int oracleOnlyTables;
    private int operationMismatches;

    private int parameterMatchCount;
    private int parameterInferredCount;

    // Gap analysis counts (Pass E)
    private int javaOnlyProcCount;
    private int oracleOnlyProcCount;
    private int operationGapCount;
    private int lowConfidenceHintCount;

    public CrossRefSummary() {}

    public int getTotalJavaNodes() { return totalJavaNodes; }
    public void setTotalJavaNodes(int totalJavaNodes) { this.totalJavaNodes = totalJavaNodes; }
    public int getHighConfidenceLinks() { return highConfidenceLinks; }
    public void setHighConfidenceLinks(int highConfidenceLinks) { this.highConfidenceLinks = highConfidenceLinks; }
    public int getMediumConfidenceLinks() { return mediumConfidenceLinks; }
    public void setMediumConfidenceLinks(int mediumConfidenceLinks) { this.mediumConfidenceLinks = mediumConfidenceLinks; }
    public int getLowConfidenceLinks() { return lowConfidenceLinks; }
    public void setLowConfidenceLinks(int lowConfidenceLinks) { this.lowConfidenceLinks = lowConfidenceLinks; }
    public int getUnmatchedJavaNodes() { return unmatchedJavaNodes; }
    public void setUnmatchedJavaNodes(int unmatchedJavaNodes) { this.unmatchedJavaNodes = unmatchedJavaNodes; }
    public int getPlsqlNodesFound() { return plsqlNodesFound; }
    public void setPlsqlNodesFound(int plsqlNodesFound) { this.plsqlNodesFound = plsqlNodesFound; }
    public int getTotalJavaCollections() { return totalJavaCollections; }
    public void setTotalJavaCollections(int totalJavaCollections) { this.totalJavaCollections = totalJavaCollections; }
    public int getMatchedTables() { return matchedTables; }
    public void setMatchedTables(int matchedTables) { this.matchedTables = matchedTables; }
    public int getJavaOnlyCollections() { return javaOnlyCollections; }
    public void setJavaOnlyCollections(int javaOnlyCollections) { this.javaOnlyCollections = javaOnlyCollections; }
    public int getOracleOnlyTables() { return oracleOnlyTables; }
    public void setOracleOnlyTables(int oracleOnlyTables) { this.oracleOnlyTables = oracleOnlyTables; }
    public int getOperationMismatches() { return operationMismatches; }
    public void setOperationMismatches(int operationMismatches) { this.operationMismatches = operationMismatches; }
    public int getParameterMatchCount() { return parameterMatchCount; }
    public void setParameterMatchCount(int parameterMatchCount) { this.parameterMatchCount = parameterMatchCount; }
    public int getParameterInferredCount() { return parameterInferredCount; }
    public void setParameterInferredCount(int parameterInferredCount) { this.parameterInferredCount = parameterInferredCount; }
    public int getJavaOnlyProcCount() { return javaOnlyProcCount; }
    public void setJavaOnlyProcCount(int v) { this.javaOnlyProcCount = v; }
    public int getOracleOnlyProcCount() { return oracleOnlyProcCount; }
    public void setOracleOnlyProcCount(int v) { this.oracleOnlyProcCount = v; }
    public int getOperationGapCount() { return operationGapCount; }
    public void setOperationGapCount(int v) { this.operationGapCount = v; }
    public int getLowConfidenceHintCount() { return lowConfidenceHintCount; }
    public void setLowConfidenceHintCount(int v) { this.lowConfidenceHintCount = v; }
}
