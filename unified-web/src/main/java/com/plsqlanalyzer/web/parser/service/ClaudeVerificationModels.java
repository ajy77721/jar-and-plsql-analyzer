package com.plsqlanalyzer.web.parser.service;

import java.time.LocalDateTime;
import java.util.*;

/**
 * Data classes used by ClaudeVerificationService for chunking, parsing, and results.
 */
public final class ClaudeVerificationModels {

    private ClaudeVerificationModels() {} // utility class

    // ==================== INPUT DATA ====================

    /** Lightweight analysis data loaded from flow-output API files. */
    static class AnalysisData {
        String analysisName;
        String entryPoint;
        String entrySchema;
        /** SQL dialect (ORACLE/SQLSERVER/MYSQL/DB2/UNKNOWN); drives prompt wording. */
        String dialect = "ORACLE";
        Map<String, NodeData> nodes = new LinkedHashMap<>();
        /** Call graph edges: fromNodeId -> list of called node summaries. */
        Map<String, List<CallEdge>> callGraph = new LinkedHashMap<>();
    }

    /** A single call edge from one procedure to another. */
    static class CallEdge {
        String fromNodeId;
        String fromName;
        String toNodeId;
        String toName;
    }

    static class NodeData {
        String nodeId;
        String name;
        String schema;
        String packageName;
        String objectName;
        String objectType;
        int lineStart;
        int lineEnd;
        int linesOfCode;
        String sourceFile;
        List<TableOp> tableOps = new ArrayList<>();
        List<JoinOp> joins = new ArrayList<>();
        List<SequenceOp> sequences = new ArrayList<>();
        List<DynamicSqlOp> dynamicSql = new ArrayList<>();
        List<PredicateOp> predicates = new ArrayList<>();
    }

    static class TableOp {
        String tableName;
        String schema;
        String operation;
        int line;
    }

    /** A single JOIN occurrence found by static analysis (from a node's table entry). */
    static class JoinOp {
        String leftTable;
        String rightTable;
        String joinType;
        String condition;
        int line;
    }

    /** A single sequence NEXTVAL/CURRVAL usage found by static analysis. */
    static class SequenceOp {
        String sequenceName;
        String schema;
        String operation;
        int line;
    }

    /** A single dynamic-SQL execution found by static analysis. */
    static class DynamicSqlOp {
        String type;
        String sqlExpression;
        int line;
    }

    /** A single WHERE-clause predicate found by static analysis. */
    static class PredicateOp {
        String tableName;
        String columnName;
        String operator;
        String value;
        int line;
    }

    // ==================== CHUNKING ====================

    static class VerificationChunk {
        String id;
        String name;
        List<String> nodeIds = new ArrayList<>();
        List<TableOp> tables = new ArrayList<>();
        List<JoinOp> joins = new ArrayList<>();
        List<SequenceOp> sequences = new ArrayList<>();
        List<DynamicSqlOp> dynamicSql = new ArrayList<>();
        List<PredicateOp> predicates = new ArrayList<>();
        int sourceWindowStart = 0;
        int sourceWindowEnd = 0;
    }

    // ==================== CHUNK RESULTS ====================

    static class ChunkResult {
        String chunkId;
        List<String> nodeIds;
        List<TableVerification> tableVerifications;
        List<CallVerification> callVerifications = new ArrayList<>();
        List<JoinVerification> joinVerifications = new ArrayList<>();
        List<SequenceVerification> sequenceVerifications = new ArrayList<>();
        List<DynamicSqlVerification> dynamicSqlVerifications = new ArrayList<>();
        List<PredicateVerification> predicateVerifications = new ArrayList<>();
        String summary;
        String error;

        static ChunkResult error(String chunkId, String error) {
            ChunkResult cr = new ChunkResult();
            cr.chunkId = chunkId;
            cr.error = error;
            cr.tableVerifications = Collections.emptyList();
            cr.callVerifications = Collections.emptyList();
            cr.joinVerifications = Collections.emptyList();
            cr.sequenceVerifications = Collections.emptyList();
            cr.dynamicSqlVerifications = Collections.emptyList();
            cr.predicateVerifications = Collections.emptyList();
            return cr;
        }
    }

    static class TableVerification {
        String tableName;
        List<OperationVerification> operations = new ArrayList<>();
    }

    // ==================== PUBLIC RESULT CLASSES ====================

    public static class OperationVerification {
        public String operation;
        public String status;     // CONFIRMED, REMOVED, NEW
        public String procedureName;
        public int lineNumber;
        public String reason;
        public String userDecision; // null, ACCEPTED, REJECTED
    }

    /**
     * A Claude verification of a call-graph edge (procedure → procedure) or a stub.
     * status: CONFIRMED (real call), REMOVED (false-positive edge), NEW (missed call).
     */
    public static class CallVerification {
        public String fromProcedure;   // caller name or nodeId
        public String toProcedure;     // callee name
        public String status;          // CONFIRMED, REMOVED, NEW
        public int lineNumber;
        public String reason;
        public String userDecision;    // null, ACCEPTED, REJECTED
    }

    /** A Claude verification of a JOIN relationship. status: CONFIRMED, REMOVED, NEW. */
    public static class JoinVerification {
        public String leftTable;
        public String rightTable;
        public String joinType;
        public String status;
        public int lineNumber;
        public String reason;
        public String userDecision;
    }

    /** A Claude verification of a sequence NEXTVAL/CURRVAL usage. status: CONFIRMED, REMOVED, NEW. */
    public static class SequenceVerification {
        public String sequenceName;
        public String operation;
        public String status;
        public int lineNumber;
        public String reason;
        public String userDecision;
    }

    /** A Claude verification of a dynamic-SQL execution. status: CONFIRMED, REMOVED, NEW. */
    public static class DynamicSqlVerification {
        public String type;
        public String sqlExpression;
        public String status;
        public int lineNumber;
        public String reason;
        public String userDecision;
    }

    /** A Claude verification of a WHERE-clause predicate. status: CONFIRMED, REMOVED, NEW. */
    public static class PredicateVerification {
        public String tableName;
        public String columnName;
        public String operator;
        public String value;
        public String status;
        public int lineNumber;
        public String reason;
        public String userDecision;
    }

    public static class TableVerificationResult {
        public String tableName;
        public String overallStatus;   // CONFIRMED, PARTIAL, EXPANDED, MODIFIED, UNVERIFIED
        public List<String> staticOperations = new ArrayList<>();
        public List<OperationVerification> claudeVerifications = new ArrayList<>();
        /** Triggers discovered for this table (populated by trigger re-analysis for NEW tables). */
        public List<Map<String, Object>> triggers;
    }

    /** Union of static + Claude findings for a single JOIN relationship (leftTable+rightTable+joinType). */
    public static class JoinVerificationResult {
        public String leftTable;
        public String rightTable;
        public String joinType;
        public String overallStatus;
        public List<JoinVerification> claudeVerifications = new ArrayList<>();
    }

    /** Union of static + Claude findings for a single sequence (keyed by sequenceName). */
    public static class SequenceVerificationResult {
        public String sequenceName;
        public String overallStatus;
        public List<String> staticOperations = new ArrayList<>();
        public List<SequenceVerification> claudeVerifications = new ArrayList<>();
    }

    /** Union of static + Claude findings for one dynamic-SQL statement (keyed by a stable hash of its text+line). */
    public static class DynamicSqlVerificationResult {
        public String key;
        public String type;
        public String sqlExpression;
        public String overallStatus;
        public List<DynamicSqlVerification> claudeVerifications = new ArrayList<>();
    }

    /** Union of static + Claude findings for a single predicate column (keyed by tableName+columnName). */
    public static class PredicateVerificationResult {
        public String tableName;
        public String columnName;
        public String overallStatus;
        public List<PredicateVerification> claudeVerifications = new ArrayList<>();
    }

    public static class VerificationResult {
        public String analysisName;
        public String timestamp;
        public List<TableVerificationResult> tables = new ArrayList<>();
        public List<CallVerification> calls = new ArrayList<>();
        public List<JoinVerificationResult> joins = new ArrayList<>();
        public List<SequenceVerificationResult> sequences = new ArrayList<>();
        public List<DynamicSqlVerificationResult> dynamicSql = new ArrayList<>();
        public List<PredicateVerificationResult> predicates = new ArrayList<>();
        public int confirmedCount;
        public int removedCount;
        public int newCount;
        public int callConfirmedCount;
        public int callRemovedCount;
        public int callNewCount;
        public int totalChunks;
        public int errorChunks;
        public int claudeCallCount;
        public long claudeTimeMs;
        public String error;

        static VerificationResult error(String msg) {
            VerificationResult vr = new VerificationResult();
            vr.error = msg;
            vr.timestamp = LocalDateTime.now().toString();
            return vr;
        }
    }
}
