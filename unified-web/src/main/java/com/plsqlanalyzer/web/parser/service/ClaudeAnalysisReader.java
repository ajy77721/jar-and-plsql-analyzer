package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.OperationVerification;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.TableVerificationResult;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.VerificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * Reads analysis data enriched with Claude verification results.
 * Delegates to StaticAnalysisReader for base data, then overlays Claude findings
 * on table operations (CONFIRMED/REMOVED/NEW badges, additional operations).
 * Call graph, sources, cursors, sequences, joins remain unchanged from static.
 */
@Component("parserClaudeAnalysisReader")
public class ClaudeAnalysisReader implements AnalysisDataReader {

    private static final Logger log = LoggerFactory.getLogger(ClaudeAnalysisReader.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final StaticAnalysisReader staticReader;
    private final ClaudePersistenceService persistenceService;

    public ClaudeAnalysisReader(StaticAnalysisReader staticReader,
                                ClaudePersistenceService persistenceService) {
        this.staticReader = staticReader;
        this.persistenceService = persistenceService;
    }

    @Override
    public JsonNode getIndex(String name) throws IOException {
        JsonNode index = staticReader.getIndex(name);
        VerificationResult result = persistenceService.loadClaudeResult(name);
        if (result == null) return index;

        ObjectNode enriched = index.deepCopy();
        enriched.put("claudeEnriched", true);
        enriched.put("claudeConfirmed", result.confirmedCount);
        enriched.put("claudeRemoved", result.removedCount);
        enriched.put("claudeNew", result.newCount);
        enriched.put("claudeCallConfirmed", result.callConfirmedCount);
        enriched.put("claudeCallRemoved", result.callRemovedCount);
        enriched.put("claudeCallNew", result.callNewCount);
        if (result.timestamp != null) enriched.put("claudeTimestamp", result.timestamp);
        return enriched;
    }

    /**
     * See {@link StaticAnalysisReader#getIndexWithoutNodes}: same enrichment as {@link #getIndex},
     * over the already-{@code nodes}-free document, so the deep copy below is cheap unlike the one
     * {@link #getIndex} does — there is no large array left to clone.
     */
    @Override
    public JsonNode getIndexWithoutNodes(String name) throws IOException {
        JsonNode index = staticReader.getIndexWithoutNodes(name);
        VerificationResult result = persistenceService.loadClaudeResult(name);
        if (result == null) return index;

        ObjectNode enriched = index.deepCopy();
        enriched.put("claudeEnriched", true);
        enriched.put("claudeConfirmed", result.confirmedCount);
        enriched.put("claudeRemoved", result.removedCount);
        enriched.put("claudeNew", result.newCount);
        enriched.put("claudeCallConfirmed", result.callConfirmedCount);
        enriched.put("claudeCallRemoved", result.callRemovedCount);
        enriched.put("claudeCallNew", result.callNewCount);
        if (result.timestamp != null) enriched.put("claudeTimestamp", result.timestamp);
        return enriched;
    }

    @Override
    public JsonNode getNodeDetail(String name, String fileName) throws IOException {
        JsonNode node = staticReader.getNodeDetail(name, fileName);
        VerificationResult result = persistenceService.loadClaudeResult(name);
        if (result == null || result.tables == null) return node;

        ObjectNode enriched = node.deepCopy();
        JsonNode tables = enriched.get("tables");
        if (tables != null && tables.isArray()) {
            for (JsonNode tableNode : tables) {
                if (tableNode instanceof ObjectNode tableObj) {
                    String tableName = StaticAnalysisReader.textOrNull(tableObj, "name");
                    if (tableName == null) continue;
                    TableVerificationResult tvr = findTableResult(result, tableName);
                    if (tvr != null) {
                        tableObj.put("claudeStatus", tvr.overallStatus != null ? tvr.overallStatus : "UNVERIFIED");
                        ArrayNode verifications = mapper.createArrayNode();
                        for (OperationVerification ov : tvr.claudeVerifications) {
                            ObjectNode v = mapper.createObjectNode();
                            v.put("operation", ov.operation != null ? ov.operation : "");
                            v.put("status", ov.status != null ? ov.status : "");
                            v.put("reason", ov.reason != null ? ov.reason : "");
                            v.put("lineNumber", ov.lineNumber);
                            verifications.add(v);
                        }
                        tableObj.set("claudeVerifications", verifications);
                    }
                }
            }
        }
        return enriched;
    }

    @Override
    public JsonNode getTables(String name) throws IOException {
        // If the user has applied accepted Claude changes, use the merged tables as
        // the base (accepted REMOVED ops dropped / NEW ops added), so "Apply" takes
        // effect in the Claude view. The static tables index is never modified.
        JsonNode merged = persistenceService.loadMergedTables(name);
        JsonNode tables = merged != null ? merged : staticReader.getTables(name);
        VerificationResult result = persistenceService.loadClaudeResult(name);
        if (result == null || result.tables == null) return tables;

        ObjectNode enriched = tables.deepCopy();
        JsonNode tableArray = enriched.get("tables");
        if (tableArray != null && tableArray.isArray()) {
            Set<String> seenTables = new HashSet<>();
            for (JsonNode tableNode : tableArray) {
                if (tableNode instanceof ObjectNode tableObj) {
                    String tableName = StaticAnalysisReader.textOrNull(tableObj, "name");
                    if (tableName == null) continue;
                    seenTables.add(tableName.toUpperCase());
                    TableVerificationResult tvr = findTableResult(result, tableName);
                    if (tvr != null) {
                        tableObj.put("claudeStatus", tvr.overallStatus != null ? tvr.overallStatus : "UNVERIFIED");
                        attachVerifications(tableObj, tvr);
                    }
                }
            }
            // Add NEW tables found by Claude that static missed
            ArrayNode tableArr = (ArrayNode) tableArray;
            for (TableVerificationResult tvr : result.tables) {
                if (tvr.tableName == null) continue;
                if (seenTables.contains(tvr.tableName.toUpperCase())) continue;
                boolean hasNew = tvr.claudeVerifications.stream()
                        .anyMatch(v -> "NEW".equals(v.status));
                if (!hasNew) continue;

                ObjectNode newTable = mapper.createObjectNode();
                newTable.put("name", tvr.tableName);
                newTable.put("schema", "");
                newTable.put("objectType", "TABLE");
                newTable.put("claudeStatus", "NEW");
                newTable.put("claudeDiscovered", true);
                attachVerifications(newTable, tvr);
                ArrayNode ops = mapper.createArrayNode();
                for (OperationVerification ov : tvr.claudeVerifications) {
                    if ("NEW".equals(ov.status) && ov.operation != null) ops.add(ov.operation);
                }
                newTable.set("allOperations", ops);
                newTable.set("usedBy", mapper.createArrayNode());
                // Use triggers from verification result if available (from trigger re-analysis)
                if (tvr.triggers != null && !tvr.triggers.isEmpty()) {
                    newTable.set("triggers", mapper.valueToTree(tvr.triggers));
                } else {
                    newTable.set("triggers", mapper.createArrayNode());
                }
                tableArr.add(newTable);
            }
        }
        return enriched;
    }

    @Override
    public JsonNode getCallGraph(String name) throws IOException {
        JsonNode base = staticReader.getCallGraph(name);
        VerificationResult r = persistenceService.loadClaudeResult(name);
        if (r == null || r.calls == null || r.calls.isEmpty()) return base;
        if (!(base.get("edges") instanceof ArrayNode)) return base;

        ObjectNode g = base.deepCopy();
        ArrayNode edges = (ArrayNode) g.get("edges");

        // Tag CONFIRMED/REMOVED edges; drop ACCEPTED REMOVED; add ACCEPTED NEW.
        for (var cv : r.calls) {
            if (cv.status == null || cv.toProcedure == null) continue;
            String st = cv.status.toUpperCase();
            if ("NEW".equals(st)) continue; // handled below
            for (JsonNode e : edges) {
                if (!(e instanceof ObjectNode edge)) continue;
                if (bareEquals(textOrNull(edge, "to"), cv.toProcedure)
                        && (cv.fromProcedure == null || bareEquals(textOrNull(edge, "from"), cv.fromProcedure))) {
                    edge.put("claudeStatus", st);
                    if (cv.reason != null) edge.put("claudeReason", cv.reason);
                }
            }
        }
        // Remove edges accepted as REMOVED (false positives).
        Iterator<JsonNode> it = edges.iterator();
        while (it.hasNext()) {
            JsonNode e = it.next();
            if ("REMOVED".equalsIgnoreCase(textOrNull((ObjectNode) e, "claudeStatus"))
                    && isAccepted(r, textOrNull((ObjectNode) e, "from"), textOrNull((ObjectNode) e, "to"), "REMOVED")) {
                it.remove();
            }
        }
        // Add edges accepted as NEW (missed calls). Resolve node ids by name so the
        // call-tree builder (which keys on fromNodeId/toNodeId) can include them.
        Map<String, String> nameToId = buildNameToIdMap(name);
        for (var cv : r.calls) {
            if (!"NEW".equalsIgnoreCase(cv.status) || !"ACCEPTED".equalsIgnoreCase(cv.userDecision)) continue;
            ObjectNode e = mapper.createObjectNode();
            e.put("from", cv.fromProcedure != null ? cv.fromProcedure : "");
            e.put("to", cv.toProcedure);
            String fromId = nameToId.get(bareUpper(cv.fromProcedure));
            String toId = nameToId.get(bareUpper(cv.toProcedure));
            if (fromId != null) e.put("fromNodeId", fromId);
            if (toId != null) e.put("toNodeId", toId);
            e.put("claudeStatus", "NEW");
            e.put("claudeDiscovered", true);
            if (cv.reason != null) e.put("claudeReason", cv.reason);
            e.set("lines", cv.lineNumber > 0 ? mapper.createArrayNode().add(cv.lineNumber) : mapper.createArrayNode());
            e.put("count", 1);
            edges.add(e);
        }
        return g;
    }

    /** Map bare (last-segment, upper) procedure name -> nodeId from the analysis index. */
    private Map<String, String> buildNameToIdMap(String name) {
        Map<String, String> m = new HashMap<>();
        try {
            JsonNode idx = staticReader.getIndex(name);
            JsonNode nodes = idx.get("nodes");
            if (nodes != null && nodes.isArray()) {
                for (JsonNode n : nodes) {
                    String id = StaticAnalysisReader.textOrNull(n, "nodeId");
                    String nm = StaticAnalysisReader.textOrNull(n, "name");
                    if (id != null && nm != null) m.putIfAbsent(bareUpper(nm), id);
                }
            }
        } catch (Exception ignore) { /* index unavailable */ }
        return m;
    }

    private static String bareUpper(String n) {
        if (n == null) return "";
        String b = n.contains(".") ? n.substring(n.lastIndexOf('.') + 1) : n;
        return b.toUpperCase();
    }

    private static String textOrNull(ObjectNode n, String field) {
        JsonNode v = n.get(field);
        return v != null && !v.isNull() ? v.asText() : null;
    }

    /** Compare two possibly-qualified names by their bare last segment, case-insensitively. */
    private static boolean bareEquals(String a, String b) {
        if (a == null || b == null) return false;
        String aa = a.contains(".") ? a.substring(a.lastIndexOf('.') + 1) : a;
        String bb = b.contains(".") ? b.substring(b.lastIndexOf('.') + 1) : b;
        return aa.equalsIgnoreCase(bb);
    }

    private boolean isAccepted(VerificationResult r, String from, String to, String status) {
        for (var cv : r.calls) {
            if (status.equalsIgnoreCase(cv.status) && "ACCEPTED".equalsIgnoreCase(cv.userDecision)
                    && bareEquals(cv.toProcedure, to)
                    && (cv.fromProcedure == null || bareEquals(cv.fromProcedure, from))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public JsonNode getProcedures(String name) throws IOException {
        return staticReader.getProcedures(name);
    }

    @Override
    public JsonNode getJoins(String name) throws IOException {
        return staticReader.getJoins(name);
    }

    @Override
    public JsonNode getCursors(String name) throws IOException {
        return staticReader.getCursors(name);
    }

    @Override
    public JsonNode getSequences(String name) throws IOException {
        return staticReader.getSequences(name);
    }

    @Override
    public JsonNode getDynamicSql(String name) throws IOException {
        return staticReader.getDynamicSql(name);
    }

    @Override
    public JsonNode getCallTree(String name, String nodeId) throws IOException {
        // Build the tree from the Claude-overlaid call graph so accepted call
        // corrections (dropped REMOVED / added NEW edges) show in the left tree too.
        return staticReader.getCallTree(name, nodeId, getCallGraph(name));
    }

    /** UI-019 (§142) — same Claude-overlaid graph as above, with {@code maxDepth} threaded through. */
    @Override
    public JsonNode getCallTree(String name, String nodeId, Integer maxDepth) throws IOException {
        return staticReader.getCallTree(name, nodeId, getCallGraph(name), maxDepth);
    }

    @Override
    public JsonNode getCallers(String name, String nodeId) throws IOException {
        return staticReader.getCallers(name, nodeId, getCallGraph(name));
    }

    @Override
    public String getSource(String name, String fileName) throws IOException {
        return staticReader.getSource(name, fileName);
    }

    @Override
    public JsonNode getResolver(String name, String type) throws IOException {
        return staticReader.getResolver(name, type);
    }

    // --- helpers ---

    private TableVerificationResult findTableResult(VerificationResult result, String tableName) {
        if (result.tables == null || tableName == null) return null;
        String upper = tableName.toUpperCase();
        for (TableVerificationResult tvr : result.tables) {
            if (tvr.tableName != null && tvr.tableName.toUpperCase().equals(upper)) return tvr;
        }
        return null;
    }

    private void attachVerifications(ObjectNode tableObj, TableVerificationResult tvr) {
        ArrayNode verifications = mapper.createArrayNode();
        for (OperationVerification ov : tvr.claudeVerifications) {
            ObjectNode v = mapper.createObjectNode();
            v.put("operation", ov.operation != null ? ov.operation : "");
            v.put("status", ov.status != null ? ov.status : "");
            v.put("procedureName", ov.procedureName != null ? ov.procedureName : "");
            v.put("lineNumber", ov.lineNumber);
            v.put("reason", ov.reason != null ? ov.reason : "");
            verifications.add(v);
        }
        tableObj.set("claudeVerifications", verifications);
    }
}
