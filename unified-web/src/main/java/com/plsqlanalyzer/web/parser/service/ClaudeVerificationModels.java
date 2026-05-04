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
    }

    static class TableOp {
        String tableName;
        String schema;
        String operation;
        int line;
    }

    // ==================== CHUNKING ====================

    static class VerificationChunk {
        String id;
        String name;
        List<String> nodeIds = new ArrayList<>();
        List<TableOp> tables = new ArrayList<>();
        int sourceWindowStart = 0;
        int sourceWindowEnd = 0;
    }

    // ==================== CHUNK RESULTS ====================

    static class ChunkResult {
        String chunkId;
        List<String> nodeIds;
        List<TableVerification> tableVerifications;
        String summary;
        String error;

        static ChunkResult error(String chunkId, String error) {
            ChunkResult cr = new ChunkResult();
            cr.chunkId = chunkId;
            cr.error = error;
            cr.tableVerifications = Collections.emptyList();
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

    public static class TableVerificationResult {
        public String tableName;
        public String overallStatus;   // CONFIRMED, PARTIAL, EXPANDED, MODIFIED, UNVERIFIED
        public List<String> staticOperations = new ArrayList<>();
        public List<OperationVerification> claudeVerifications = new ArrayList<>();
        /** Triggers discovered for this table (populated by trigger re-analysis for NEW tables). */
        public List<Map<String, Object>> triggers;
    }

    public static class VerificationResult {
        public String analysisName;
        public String timestamp;
        public List<TableVerificationResult> tables = new ArrayList<>();
        public int confirmedCount;
        public int removedCount;
        public int newCount;
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
