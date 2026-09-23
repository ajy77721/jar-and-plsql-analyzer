package com.jaranalyzer.crossref.model;

import java.util.ArrayList;
import java.util.List;

/**
 * One link between a Java service node and its corresponding Oracle proc/sub-proc.
 *
 * Each entry in {@link CrossRefResult#getProcLinks()} represents one Java Impl class+method
 * mapped to one Oracle sub-procedure within the target package.
 */
public class CrossRefProcLink {

    // ── Java side ─────────────────────────────────────────────────────────────

    /** Fully qualified Java class name. */
    private String javaClass;

    /** Java method name. */
    private String javaMethod;

    /** Java stereotype (SERVICE, REPOSITORY, DAO, etc.). */
    private String javaStereotype;

    /** Java operation type (READ, WRITE, UPDATE, DELETE, AGGREGATE). */
    private String javaOperationType;

    /** Unique node ID from the call tree (className#methodName). */
    private String javaNodeId;

    // ── Oracle side ───────────────────────────────────────────────────────────

    /**
     * Oracle proc reference resolved from bytecode SimpleJdbcCall wiring.
     * Format: "PROC:PG_UWGE_CN.INSERT_RISK" or "FUNC:...".
     * Null when not available from bytecode (only inference used).
     */
    private String oracleProcRef;

    /**
     * Oracle proc reference inferred from Java class/method name convention.
     * Always populated when the class follows the PgXxx naming pattern.
     * E.g. "PG_UWGE_CN.INSERT_RISK".
     */
    private String inferredProcRef;

    /** Combined best-guess Oracle proc name (oracleProcRef preferred over inferredProcRef). */
    private String resolvedProcRef;

    /**
     * Confidence in the Oracle proc mapping:
     *   HIGH   — from bytecode (simpleJdbcCallProcedures)
     *   MEDIUM — inferred from Java class name containing "Pg" marker
     *   LOW    — inferred from method name alone
     *   NONE   — infrastructure node (no Oracle equivalent expected)
     */
    private String matchConfidence;

    /** Whether a matching Oracle FlowNode was found in any PL/SQL session on disk. */
    private boolean plsqlNodeFound;

    /**
     * Which algorithm pass resolved this Oracle link:
     *   PASS_A          — table-based linkage (collectionsAccessed → tables.json)
     *   PASS_B          — Pg* class naming convention
     *   PASS_A+B        — both table linkage and Pg* naming confirmed this link
     *   PASS_C          — fuzzy package-segment match
     *   PASS_D_INHERITED — inherited Oracle zone from @LogParameters ancestor
     */
    private String zoneSource;

    /** Whether the Java node has @LogParameters annotation (marks facade boundary). */
    private boolean hasLogParameters;

    // ── Comparison data ───────────────────────────────────────────────────────

    /** Collections accessed by this Java node with their operations. */
    private List<CrossRefCollectionEntry> javaCollections = new ArrayList<>();

    /**
     * Oracle table operations from the matched FlowNode.
     * Empty in STATIC_INFERENCE mode (no PL/SQL session available).
     */
    private List<CrossRefOracleTableEntry> oracleTables = new ArrayList<>();

    /** Side-by-side table match results. */
    private List<CrossRefTableMatch> tableMatches = new ArrayList<>();

    /** Parameter name matches between Java string literals and Oracle proc params. */
    private List<CrossRefParamMatch> parameterMatches = new ArrayList<>();

    /**
     * Oracle cursors defined within this proc (FULL_COMPARISON mode only).
     * Each entry: {name, query, tables[]}
     */
    private List<java.util.Map<String, Object>> cursors = new ArrayList<>();

    /**
     * Oracle sub-proc calls made from within this proc (FULL_COMPARISON mode only).
     * Each entry: {packageName, objectName, resolvedJavaNode}
     */
    private List<java.util.Map<String, Object>> subCalls = new ArrayList<>();

    // ── Gap summary ───────────────────────────────────────────────────────────

    /** Collections present in Java but absent from Oracle tables list. */
    private List<String> javaOnlyCollections = new ArrayList<>();

    /** Tables present in Oracle FlowNode but absent from Java collections. */
    private List<String> oracleOnlyTables = new ArrayList<>();

    // ── Claude enrichment ─────────────────────────────────────────────────────

    /** Stable hash of (javaKey + oracleProcRef + plsqlSessionId) — cache key. */
    private String claudeChunkId;

    /** CONVERTED | PARTIAL | NOT_CONVERTED | NO_ORACLE_EQUIVALENT */
    private String claudeVerdict;

    /** HIGH | MEDIUM | LOW */
    private String claudeConfidence;

    /** 1-2 sentence explanation from Claude. */
    private String claudeReason;

    /** Specific gaps Claude found. */
    private List<String> claudeGaps = new ArrayList<>();

    /** Whether Claude analysis has completed for this chunk. */
    private boolean claudeDone;

    /**
     * Claude's verification of this Java method's static-analysis-detected Aggregation
     * Pipeline stages, checked standalone against the Java source (no Oracle side to compare
     * against). Each entry: {item, status: CONFIRMED|REMOVED|NEW, reason}.
     */
    private List<java.util.Map<String, Object>> claudeAggregationVerification = new ArrayList<>();

    /**
     * Claude's verification of this Java method's static-analysis-detected Dynamic
     * Dispatch/reflection call sites, checked standalone against the Java source.
     * Each entry: {item, status: CONFIRMED|REMOVED|NEW, reason}.
     */
    private List<java.util.Map<String, Object>> claudeDynamicDispatchVerification = new ArrayList<>();

    /**
     * Claude's verification of this Java method's static-analysis-detected External
     * Dependency calls, checked standalone against the Java source.
     * Each entry: {item, status: CONFIRMED|REMOVED|NEW, reason}.
     */
    private List<java.util.Map<String, Object>> claudeExternalDependencyVerification = new ArrayList<>();

    public CrossRefProcLink() {}

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getJavaClass() { return javaClass; }
    public void setJavaClass(String javaClass) { this.javaClass = javaClass; }
    public String getJavaMethod() { return javaMethod; }
    public void setJavaMethod(String javaMethod) { this.javaMethod = javaMethod; }
    public String getJavaStereotype() { return javaStereotype; }
    public void setJavaStereotype(String javaStereotype) { this.javaStereotype = javaStereotype; }
    public String getJavaOperationType() { return javaOperationType; }
    public void setJavaOperationType(String javaOperationType) { this.javaOperationType = javaOperationType; }
    public String getJavaNodeId() { return javaNodeId; }
    public void setJavaNodeId(String javaNodeId) { this.javaNodeId = javaNodeId; }
    public String getOracleProcRef() { return oracleProcRef; }
    public void setOracleProcRef(String oracleProcRef) { this.oracleProcRef = oracleProcRef; }
    public String getInferredProcRef() { return inferredProcRef; }
    public void setInferredProcRef(String inferredProcRef) { this.inferredProcRef = inferredProcRef; }
    public String getResolvedProcRef() { return resolvedProcRef; }
    public void setResolvedProcRef(String resolvedProcRef) { this.resolvedProcRef = resolvedProcRef; }
    public String getMatchConfidence() { return matchConfidence; }
    public void setMatchConfidence(String matchConfidence) { this.matchConfidence = matchConfidence; }
    public boolean isPlsqlNodeFound() { return plsqlNodeFound; }
    public void setPlsqlNodeFound(boolean plsqlNodeFound) { this.plsqlNodeFound = plsqlNodeFound; }
    public String getZoneSource() { return zoneSource; }
    public void setZoneSource(String zoneSource) { this.zoneSource = zoneSource; }
    public boolean isHasLogParameters() { return hasLogParameters; }
    public void setHasLogParameters(boolean hasLogParameters) { this.hasLogParameters = hasLogParameters; }
    public List<CrossRefCollectionEntry> getJavaCollections() { return javaCollections; }
    public void setJavaCollections(List<CrossRefCollectionEntry> javaCollections) { this.javaCollections = javaCollections; }
    public List<CrossRefOracleTableEntry> getOracleTables() { return oracleTables; }
    public void setOracleTables(List<CrossRefOracleTableEntry> oracleTables) { this.oracleTables = oracleTables; }
    public List<CrossRefTableMatch> getTableMatches() { return tableMatches; }
    public void setTableMatches(List<CrossRefTableMatch> tableMatches) { this.tableMatches = tableMatches; }
    public List<CrossRefParamMatch> getParameterMatches() { return parameterMatches; }
    public void setParameterMatches(List<CrossRefParamMatch> parameterMatches) { this.parameterMatches = parameterMatches; }
    public List<java.util.Map<String, Object>> getCursors() { return cursors; }
    public void setCursors(List<java.util.Map<String, Object>> cursors) { this.cursors = cursors; }
    public List<java.util.Map<String, Object>> getSubCalls() { return subCalls; }
    public void setSubCalls(List<java.util.Map<String, Object>> subCalls) { this.subCalls = subCalls; }
    public List<String> getJavaOnlyCollections() { return javaOnlyCollections; }
    public void setJavaOnlyCollections(List<String> javaOnlyCollections) { this.javaOnlyCollections = javaOnlyCollections; }
    public List<String> getOracleOnlyTables() { return oracleOnlyTables; }
    public void setOracleOnlyTables(List<String> oracleOnlyTables) { this.oracleOnlyTables = oracleOnlyTables; }
    public String getClaudeChunkId() { return claudeChunkId; }
    public void setClaudeChunkId(String v) { this.claudeChunkId = v; }
    public String getClaudeVerdict() { return claudeVerdict; }
    public void setClaudeVerdict(String v) { this.claudeVerdict = v; }
    public String getClaudeConfidence() { return claudeConfidence; }
    public void setClaudeConfidence(String v) { this.claudeConfidence = v; }
    public String getClaudeReason() { return claudeReason; }
    public void setClaudeReason(String v) { this.claudeReason = v; }
    public List<String> getClaudeGaps() { return claudeGaps; }
    public void setClaudeGaps(List<String> v) { this.claudeGaps = v; }
    public boolean isClaudeDone() { return claudeDone; }
    public void setClaudeDone(boolean v) { this.claudeDone = v; }
    public List<java.util.Map<String, Object>> getClaudeAggregationVerification() { return claudeAggregationVerification; }
    public void setClaudeAggregationVerification(List<java.util.Map<String, Object>> v) { this.claudeAggregationVerification = v; }
    public List<java.util.Map<String, Object>> getClaudeDynamicDispatchVerification() { return claudeDynamicDispatchVerification; }
    public void setClaudeDynamicDispatchVerification(List<java.util.Map<String, Object>> v) { this.claudeDynamicDispatchVerification = v; }
    public List<java.util.Map<String, Object>> getClaudeExternalDependencyVerification() { return claudeExternalDependencyVerification; }
    public void setClaudeExternalDependencyVerification(List<java.util.Map<String, Object>> v) { this.claudeExternalDependencyVerification = v; }
}
