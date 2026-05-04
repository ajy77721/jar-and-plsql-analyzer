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
        JsonNode tables = staticReader.getTables(name);
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
        return staticReader.getCallGraph(name);
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
    public JsonNode getCallTree(String name, String nodeId) throws IOException {
        return staticReader.getCallTree(name, nodeId);
    }

    @Override
    public JsonNode getCallers(String name, String nodeId) throws IOException {
        return staticReader.getCallers(name, nodeId);
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
