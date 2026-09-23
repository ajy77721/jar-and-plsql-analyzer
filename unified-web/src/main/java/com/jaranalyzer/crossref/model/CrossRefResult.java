package com.jaranalyzer.crossref.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Full cross-reference result: Java endpoint ↔ Oracle PL/SQL proc comparison.
 *
 * Stored on disk as result.json under data/crossref/{jarId}/{crossRefId}/.
 */
public class CrossRefResult {

    /** Unique ID for this cross-ref run (epoch ms as string). */
    private String crossRefId;

    /** JAR identifier. */
    private String jarId;

    /** HTTP endpoint path that was analysed. */
    private String endpointPath;

    /** Oracle package/proc name supplied by the user. */
    private String procName;

    /** Oracle schema (may be null). */
    private String schema;

    /**
     * Operating mode:
     *   STATIC_INFERENCE — no PL/SQL session found; Oracle names inferred from class names.
     *   FULL_COMPARISON  — PL/SQL FlowNodes loaded from disk; operation-level comparison available.
     */
    private String mode;

    /** Epoch ms when this cross-ref was created. */
    private long timestamp;

    /** PL/SQL session ID used for FULL_COMPARISON mode. Null in STATIC_INFERENCE. */
    private String plsqlSessionId;

    /** Aggregate statistics. */
    private CrossRefSummary summary;

    /** One entry per Java service node that maps directly to an Oracle proc (Pg* naming layer). */
    private List<CrossRefProcLink> procLinks = new ArrayList<>();

    /**
     * Java routing nodes: non-Pg service/controller nodes that don't map to an Oracle proc
     * themselves but call Pg* nodes in their subtree.
     * These fill the call-chain gap between the HTTP endpoint and the direct Oracle callers.
     */
    private List<RoutingNode> routingNodes = new ArrayList<>();

    /**
     * Two-way trace map: Java "SimpleClass.method" → list of Oracle package names it calls
     * (directly or through children in the call tree).
     */
    private Map<String, List<String>> javaToOracleMap = new LinkedHashMap<>();

    /**
     * Two-way trace map: Oracle package name → list of Java "SimpleClass.method" nodes
     * that call into it (directly or as routing nodes up the call chain).
     */
    private Map<String, List<String>> oracleToJavaMap = new LinkedHashMap<>();

    /**
     * Java nodes that could not be linked to any Oracle proc
     * (infrastructure nodes: locks, logging, batch engine, sequence utilities).
     */
    private List<InfraNode> infraNodes = new ArrayList<>();

    /**
     * Full Oracle call tree nodes from the PL/SQL session (depth 0 = entry, depth 1 = direct Java-callable,
     * depth 2+ = Oracle-internal sub-calls).
     * Used for the Oracle→Java trace view.
     */
    private List<OracleNode> oracleNodes = new ArrayList<>();

    /**
     * Table-level cross-reference: Oracle table name → list of Java nodes accessing it.
     * Ground-truth linkage via collectionsAccessed matching tables.json.
     */
    private Map<String, List<String>> tableToJavaMap = new LinkedHashMap<>();

    /**
     * Table-level cross-reference: Java "SimpleClass.method" → list of Oracle table names it accesses.
     */
    private Map<String, List<String>> javaToTableMap = new LinkedHashMap<>();

    // ── Gap analysis (Pass E) ─────────────────────────────────────────────────

    /** Java methods with collectionsAccessed but no matching Oracle table/proc found. */
    private List<JavaOnlyProc> javaOnlyProcs = new ArrayList<>();

    /** Oracle procs (depth 0-2) with no Java callers from any pass. */
    private List<OracleOnlyProc> oracleOnlyProcs = new ArrayList<>();

    /** Tables present in Java collectionsAccessed with no Oracle equivalent found. */
    private List<OperationGap> operationGaps = new ArrayList<>();

    /** Low-confidence proc links (MEDIUM/LOW pass B, fuzzy pass C, inherited pass D) — kept for reference. */
    private List<CrossRefProcLink> lowConfidenceHints = new ArrayList<>();

    // ── Claude overall analysis ───────────────────────────────────────────────

    /** Overall Claude verdict: FULLY_CONVERTED | MOSTLY_CONVERTED | PARTIAL | SIGNIFICANT_GAPS | NOT_ANALYZED */
    private String claudeOverallVerdict;

    /** Claude overall confidence: HIGH | MEDIUM | LOW */
    private String claudeOverallConfidence;

    /** Claude overall reason (2-4 sentences). */
    private String claudeOverallReason;

    /** Top-level gaps Claude found across the entire cross-reference. */
    private List<String> claudeOverallGaps = new ArrayList<>();

    /** Conversion percentage estimate from Claude (0-100). */
    private int claudeConversionPct;

    /** ChunkId for the overall-verdict chunk file. */
    private String claudeOverallChunkId;

    public CrossRefResult() {}

    /**
     * A Java node in the call tree that doesn't map directly to an Oracle proc
     * but routes calls to one or more Oracle packages through its children.
     * E.g. AutoPolicyService.processPolicy() → calls PgUwgeCIInsertCoverImpl → PG_UWGE_CI
     */
    public static class RoutingNode {
        private String javaClass;
        private String javaMethod;
        private String javaStereotype;
        /** Oracle package names this node transitively calls. */
        private List<String> calledOraclePackages = new ArrayList<>();
        /** Child Pg* nodes it directly calls. */
        private List<String> directProcCallers = new ArrayList<>();

        public RoutingNode() {}
        public RoutingNode(String javaClass, String javaMethod, String javaStereotype) {
            this.javaClass = javaClass;
            this.javaMethod = javaMethod;
            this.javaStereotype = javaStereotype;
        }
        public String getJavaClass() { return javaClass; }
        public void setJavaClass(String c) { this.javaClass = c; }
        public String getJavaMethod() { return javaMethod; }
        public void setJavaMethod(String m) { this.javaMethod = m; }
        public String getJavaStereotype() { return javaStereotype; }
        public void setJavaStereotype(String s) { this.javaStereotype = s; }
        public List<String> getCalledOraclePackages() { return calledOraclePackages; }
        public void setCalledOraclePackages(List<String> l) { this.calledOraclePackages = l; }
        public List<String> getDirectProcCallers() { return directProcCallers; }
        public void setDirectProcCallers(List<String> l) { this.directProcCallers = l; }
    }

    /**
     * One Oracle node from the PL/SQL call tree.
     * depth 0 = entry point, depth 1 = direct Java-callable, depth 2+ = Oracle-internal.
     * javaCallers lists the Java "SimpleClass.method" nodes that call this Oracle proc.
     */
    public static class OracleNode {
        private String nodeId;       // SCHEMA$PACKAGE$PROC
        private String packageName;
        private String objectName;
        private String schema;
        private String objectType;   // PROCEDURE / FUNCTION
        private int depth;
        private String parentNodeId;
        private List<String> javaCallers = new ArrayList<>();   // Java keys that map here
        private List<String> javaTableCallers = new ArrayList<>(); // Java keys via table-based linkage

        public OracleNode() {}
        public String getNodeId() { return nodeId; }
        public void setNodeId(String v) { this.nodeId = v; }
        public String getPackageName() { return packageName; }
        public void setPackageName(String v) { this.packageName = v; }
        public String getObjectName() { return objectName; }
        public void setObjectName(String v) { this.objectName = v; }
        public String getSchema() { return schema; }
        public void setSchema(String v) { this.schema = v; }
        public String getObjectType() { return objectType; }
        public void setObjectType(String v) { this.objectType = v; }
        public int getDepth() { return depth; }
        public void setDepth(int v) { this.depth = v; }
        public String getParentNodeId() { return parentNodeId; }
        public void setParentNodeId(String v) { this.parentNodeId = v; }
        public List<String> getJavaCallers() { return javaCallers; }
        public void setJavaCallers(List<String> v) { this.javaCallers = v; }
        public List<String> getJavaTableCallers() { return javaTableCallers; }
        public void setJavaTableCallers(List<String> v) { this.javaTableCallers = v; }
    }

    /** Java method with collectionsAccessed but no matching Oracle proc/table found. */
    public static class JavaOnlyProc {
        private String javaClass;
        private String javaMethod;
        private String stereotype;
        private String operationType;
        private List<String> tables = new ArrayList<>();
        private String reason;
        // Claude enrichment
        private String claudeChunkId;
        private String claudeVerdict;     // NOT_CONVERTED | HAS_ORACLE_EQUIVALENT | INFRA_ONLY | UNKNOWN
        private String claudeConfidence;
        private String claudeReason;
        private boolean claudeDone;
        // Standalone verification of static-analysis facets against Java source (no Oracle
        // side to compare) — same CONFIRMED|REMOVED|NEW status pattern as CrossRefProcLink.
        private List<Map<String, Object>> claudeAggregationVerification = new ArrayList<>();
        private List<Map<String, Object>> claudeDynamicDispatchVerification = new ArrayList<>();
        private List<Map<String, Object>> claudeExternalDependencyVerification = new ArrayList<>();

        public JavaOnlyProc() {}
        public JavaOnlyProc(String javaClass, String javaMethod, String stereotype,
                            String operationType, List<String> tables, String reason) {
            this.javaClass = javaClass; this.javaMethod = javaMethod;
            this.stereotype = stereotype; this.operationType = operationType;
            this.tables = tables; this.reason = reason;
        }
        public String getJavaClass() { return javaClass; }
        public void setJavaClass(String v) { this.javaClass = v; }
        public String getJavaMethod() { return javaMethod; }
        public void setJavaMethod(String v) { this.javaMethod = v; }
        public String getStereotype() { return stereotype; }
        public void setStereotype(String v) { this.stereotype = v; }
        public String getOperationType() { return operationType; }
        public void setOperationType(String v) { this.operationType = v; }
        public List<String> getTables() { return tables; }
        public void setTables(List<String> v) { this.tables = v; }
        public String getReason() { return reason; }
        public void setReason(String v) { this.reason = v; }
        public String getClaudeChunkId() { return claudeChunkId; }
        public void setClaudeChunkId(String v) { this.claudeChunkId = v; }
        public String getClaudeVerdict() { return claudeVerdict; }
        public void setClaudeVerdict(String v) { this.claudeVerdict = v; }
        public String getClaudeConfidence() { return claudeConfidence; }
        public void setClaudeConfidence(String v) { this.claudeConfidence = v; }
        public String getClaudeReason() { return claudeReason; }
        public void setClaudeReason(String v) { this.claudeReason = v; }
        public boolean isClaudeDone() { return claudeDone; }
        public void setClaudeDone(boolean v) { this.claudeDone = v; }
        public List<Map<String, Object>> getClaudeAggregationVerification() { return claudeAggregationVerification; }
        public void setClaudeAggregationVerification(List<Map<String, Object>> v) { this.claudeAggregationVerification = v; }
        public List<Map<String, Object>> getClaudeDynamicDispatchVerification() { return claudeDynamicDispatchVerification; }
        public void setClaudeDynamicDispatchVerification(List<Map<String, Object>> v) { this.claudeDynamicDispatchVerification = v; }
        public List<Map<String, Object>> getClaudeExternalDependencyVerification() { return claudeExternalDependencyVerification; }
        public void setClaudeExternalDependencyVerification(List<Map<String, Object>> v) { this.claudeExternalDependencyVerification = v; }
    }

    /** Oracle proc (depth 0-2) with no Java callers from any analysis pass. */
    public static class OracleOnlyProc {
        private String procRef;      // "PKG.PROC"
        private String packageName;
        private String objectName;
        private int depth;
        private List<String> tables = new ArrayList<>();
        // Claude enrichment
        private String claudeChunkId;
        private String claudeVerdict;     // NOT_IN_JAVA | HAS_JAVA_EQUIVALENT | INTERNAL_ONLY | UNKNOWN
        private String claudeConfidence;
        private String claudeReason;
        private boolean claudeDone;

        public OracleOnlyProc() {}
        public OracleOnlyProc(String procRef, String packageName, String objectName,
                              int depth, List<String> tables) {
            this.procRef = procRef; this.packageName = packageName;
            this.objectName = objectName; this.depth = depth; this.tables = tables;
        }
        public String getProcRef() { return procRef; }
        public void setProcRef(String v) { this.procRef = v; }
        public String getPackageName() { return packageName; }
        public void setPackageName(String v) { this.packageName = v; }
        public String getObjectName() { return objectName; }
        public void setObjectName(String v) { this.objectName = v; }
        public int getDepth() { return depth; }
        public void setDepth(int v) { this.depth = v; }
        public List<String> getTables() { return tables; }
        public void setTables(List<String> v) { this.tables = v; }
        public String getClaudeChunkId() { return claudeChunkId; }
        public void setClaudeChunkId(String v) { this.claudeChunkId = v; }
        public String getClaudeVerdict() { return claudeVerdict; }
        public void setClaudeVerdict(String v) { this.claudeVerdict = v; }
        public String getClaudeConfidence() { return claudeConfidence; }
        public void setClaudeConfidence(String v) { this.claudeConfidence = v; }
        public String getClaudeReason() { return claudeReason; }
        public void setClaudeReason(String v) { this.claudeReason = v; }
        public boolean isClaudeDone() { return claudeDone; }
        public void setClaudeDone(boolean v) { this.claudeDone = v; }
    }

    /** A table that exists on both sides but has mismatched operation types, or is only on one side. */
    public static class OperationGap {
        private String table;
        private String javaOperation;    // READ / WRITE / null
        private String oracleOperation;  // SELECT / INSERT / UPDATE / DELETE / null
        private String javaNode;
        private String oracleProc;
        private String gapType;          // JAVA_ONLY_TABLE / ORACLE_ONLY_TABLE / OP_MISMATCH

        public OperationGap() {}
        public OperationGap(String table, String javaOp, String oracleOp,
                            String javaNode, String oracleProc, String gapType) {
            this.table = table; this.javaOperation = javaOp; this.oracleOperation = oracleOp;
            this.javaNode = javaNode; this.oracleProc = oracleProc; this.gapType = gapType;
        }
        public String getTable() { return table; }
        public void setTable(String v) { this.table = v; }
        public String getJavaOperation() { return javaOperation; }
        public void setJavaOperation(String v) { this.javaOperation = v; }
        public String getOracleOperation() { return oracleOperation; }
        public void setOracleOperation(String v) { this.oracleOperation = v; }
        public String getJavaNode() { return javaNode; }
        public void setJavaNode(String v) { this.javaNode = v; }
        public String getOracleProc() { return oracleProc; }
        public void setOracleProc(String v) { this.oracleProc = v; }
        public String getGapType() { return gapType; }
        public void setGapType(String v) { this.gapType = v; }
    }

    /** Minimal representation of a Java node excluded from Oracle mapping. */
    public static class InfraNode {
        private String javaClass;
        private String javaMethod;
        private String reason;  // e.g. "LOCK_MANAGEMENT", "LOGGING", "BATCH_INFRA", "SEQUENCE_SERVICE"

        public InfraNode() {}
        public InfraNode(String javaClass, String javaMethod, String reason) {
            this.javaClass = javaClass;
            this.javaMethod = javaMethod;
            this.reason = reason;
        }
        public String getJavaClass() { return javaClass; }
        public void setJavaClass(String javaClass) { this.javaClass = javaClass; }
        public String getJavaMethod() { return javaMethod; }
        public void setJavaMethod(String javaMethod) { this.javaMethod = javaMethod; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
    }

    public String getCrossRefId() { return crossRefId; }
    public void setCrossRefId(String crossRefId) { this.crossRefId = crossRefId; }
    public String getJarId() { return jarId; }
    public void setJarId(String jarId) { this.jarId = jarId; }
    public String getEndpointPath() { return endpointPath; }
    public void setEndpointPath(String endpointPath) { this.endpointPath = endpointPath; }
    public String getProcName() { return procName; }
    public void setProcName(String procName) { this.procName = procName; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public String getPlsqlSessionId() { return plsqlSessionId; }
    public void setPlsqlSessionId(String plsqlSessionId) { this.plsqlSessionId = plsqlSessionId; }
    public CrossRefSummary getSummary() { return summary; }
    public void setSummary(CrossRefSummary summary) { this.summary = summary; }
    public List<CrossRefProcLink> getProcLinks() { return procLinks; }
    public void setProcLinks(List<CrossRefProcLink> procLinks) { this.procLinks = procLinks; }
    public List<RoutingNode> getRoutingNodes() { return routingNodes; }
    public void setRoutingNodes(List<RoutingNode> routingNodes) { this.routingNodes = routingNodes; }
    public Map<String, List<String>> getJavaToOracleMap() { return javaToOracleMap; }
    public void setJavaToOracleMap(Map<String, List<String>> m) { this.javaToOracleMap = m; }
    public Map<String, List<String>> getOracleToJavaMap() { return oracleToJavaMap; }
    public void setOracleToJavaMap(Map<String, List<String>> m) { this.oracleToJavaMap = m; }
    public List<InfraNode> getInfraNodes() { return infraNodes; }
    public void setInfraNodes(List<InfraNode> infraNodes) { this.infraNodes = infraNodes; }
    public List<OracleNode> getOracleNodes() { return oracleNodes; }
    public void setOracleNodes(List<OracleNode> v) { this.oracleNodes = v; }
    public Map<String, List<String>> getTableToJavaMap() { return tableToJavaMap; }
    public void setTableToJavaMap(Map<String, List<String>> m) { this.tableToJavaMap = m; }
    public Map<String, List<String>> getJavaToTableMap() { return javaToTableMap; }
    public void setJavaToTableMap(Map<String, List<String>> m) { this.javaToTableMap = m; }
    public List<JavaOnlyProc> getJavaOnlyProcs() { return javaOnlyProcs; }
    public void setJavaOnlyProcs(List<JavaOnlyProc> v) { this.javaOnlyProcs = v; }
    public List<OracleOnlyProc> getOracleOnlyProcs() { return oracleOnlyProcs; }
    public void setOracleOnlyProcs(List<OracleOnlyProc> v) { this.oracleOnlyProcs = v; }
    public List<OperationGap> getOperationGaps() { return operationGaps; }
    public void setOperationGaps(List<OperationGap> v) { this.operationGaps = v; }
    public List<CrossRefProcLink> getLowConfidenceHints() { return lowConfidenceHints; }
    public void setLowConfidenceHints(List<CrossRefProcLink> v) { this.lowConfidenceHints = v; }
    public String getClaudeOverallVerdict() { return claudeOverallVerdict; }
    public void setClaudeOverallVerdict(String v) { this.claudeOverallVerdict = v; }
    public String getClaudeOverallConfidence() { return claudeOverallConfidence; }
    public void setClaudeOverallConfidence(String v) { this.claudeOverallConfidence = v; }
    public String getClaudeOverallReason() { return claudeOverallReason; }
    public void setClaudeOverallReason(String v) { this.claudeOverallReason = v; }
    public List<String> getClaudeOverallGaps() { return claudeOverallGaps; }
    public void setClaudeOverallGaps(List<String> v) { this.claudeOverallGaps = v; }
    public int getClaudeConversionPct() { return claudeConversionPct; }
    public void setClaudeConversionPct(int v) { this.claudeConversionPct = v; }
    public String getClaudeOverallChunkId() { return claudeOverallChunkId; }
    public void setClaudeOverallChunkId(String v) { this.claudeOverallChunkId = v; }
}
