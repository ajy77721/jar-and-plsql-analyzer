package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.analyzer.storage.ArtifactKey;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.plsqlanalyzer.web.parser.service.StaticAnalysisReader.textOrNull;

/** Node-scan facet reads: getJoins + getCursors (both iterate collectNodeFiles). */
class NodeScanFacetReader {

    private final StaticAnalysisReader r;

    NodeScanFacetReader(StaticAnalysisReader r) {
        this.r = r;
    }

    /** Streams a node-detail document out of the store. */
    private JsonNode readTree(ArtifactKey key) throws IOException {
        try (InputStream in = r.openRead(key)) {
            return r.mapper.readTree(in);
        }
    }

    JsonNode getJoins(String name) throws IOException {
        ObjectMapper mapper = r.mapper;
        String key = r.cacheKey(name, "joins");
        JsonNode cached = r.synthesizedCache.get(key);
        if (cached != null) return cached;

        List<ArtifactKey> nodeFiles = r.collectNodeFiles(name);
        if (nodeFiles.isEmpty()) return mapper.createArrayNode();

        Map<String, ObjectNode> joinMap = new LinkedHashMap<>();
        {
            for (ArtifactKey nodeFile : nodeFiles) {
                JsonNode node = readTree(nodeFile);
                String nodeId = textOrNull(node, "nodeId");
                String nodeName = textOrNull(node, "name");
                String sourceFile = textOrNull(node, "sourceFile");
                JsonNode tables = node.get("tables");
                if (tables == null || !tables.isArray()) continue;

                for (JsonNode table : tables) {
                    String containingTable = textOrNull(table, "name");
                    if (containingTable == null) continue;
                    JsonNode joins = table.get("joins");
                    if (joins == null || !joins.isArray()) continue;

                    for (JsonNode join : joins) {
                        String rightTable = textOrNull(join, "joinedTable");
                        String joinType = textOrNull(join, "joinType");
                        String condition = textOrNull(join, "condition");
                        int line = join.has("line") ? join.get("line").asInt() : 0;
                        int queryId = join.has("queryId") ? join.get("queryId").asInt() : 0;
                        String resolvedLeft = textOrNull(join, "leftTable");
                        String leftTable = resolvedLeft != null ? resolvedLeft : containingTable;
                        if (rightTable == null) continue;

                        String pairKey = leftTable.compareToIgnoreCase(rightTable) <= 0
                                ? leftTable.toUpperCase() + "::" + rightTable.toUpperCase()
                                : rightTable.toUpperCase() + "::" + leftTable.toUpperCase();

                        ObjectNode entry = joinMap.get(pairKey);
                        if (entry == null) {
                            entry = mapper.createObjectNode();
                            entry.put("leftTable", leftTable.compareToIgnoreCase(rightTable) <= 0 ? leftTable : rightTable);
                            entry.put("rightTable", leftTable.compareToIgnoreCase(rightTable) <= 0 ? rightTable : leftTable);
                            entry.set("joinTypes", mapper.createArrayNode());
                            entry.put("accessCount", 0);
                            entry.set("accessDetails", mapper.createArrayNode());
                            joinMap.put(pairKey, entry);
                        }

                        ArrayNode joinTypes = (ArrayNode) entry.get("joinTypes");
                        if (joinType != null) {
                            boolean found = false;
                            for (JsonNode jt : joinTypes) {
                                if (jt.asText().equals(joinType)) { found = true; break; }
                            }
                            if (!found) joinTypes.add(joinType);
                        }

                        entry.put("accessCount", entry.get("accessCount").asInt() + 1);

                        ObjectNode detail = mapper.createObjectNode();
                        detail.put("joinType", joinType != null ? joinType : "");
                        detail.put("onPredicate", condition != null ? condition : "");
                        detail.put("procedureId", nodeId != null ? nodeId : "");
                        detail.put("procedureName", nodeName != null ? nodeName : "");
                        detail.put("lineNumber", line);
                        detail.put("sourceFile", sourceFile != null ? sourceFile : "");
                        detail.put("queryId", queryId);
                        ((ArrayNode) entry.get("accessDetails")).add(detail);
                    }
                }
            }
        }

        ArrayNode result = mapper.createArrayNode();
        joinMap.values().forEach(result::add);
        r.synthesizedCache.put(key, result);
        return result;
    }

    JsonNode getCursors(String name) throws IOException {
        ObjectMapper mapper = r.mapper;
        String key = r.cacheKey(name, "cursors");
        JsonNode cached = r.synthesizedCache.get(key);
        if (cached != null) return cached;

        List<ArtifactKey> nodeFiles = r.collectNodeFiles(name);
        if (nodeFiles.isEmpty()) return mapper.createArrayNode();

        Map<String, ObjectNode> cursorMap = new LinkedHashMap<>();
        {
            for (ArtifactKey nodeFile : nodeFiles) {
                JsonNode node = readTree(nodeFile);
                String nodeId = textOrNull(node, "nodeId");
                String nodeName = textOrNull(node, "name");
                String sourceFile = textOrNull(node, "sourceFile");
                JsonNode cursors = node.get("cursors");
                if (cursors == null || !cursors.isArray()) continue;

                for (JsonNode cursor : cursors) {
                    String cursorName = textOrNull(cursor, "name");
                    if (cursorName == null) cursorName = "UNNAMED";
                    String query = textOrNull(cursor, "query");
                    boolean forLoop = cursor.has("forLoop") && cursor.get("forLoop").asBoolean();
                    boolean refCursor = cursor.has("refCursor") && cursor.get("refCursor").asBoolean();
                    int line = cursor.has("line") ? cursor.get("line").asInt() : 0;

                    String cursorType = refCursor ? "REF_CURSOR" : forLoop ? "FOR_LOOP" : "EXPLICIT";
                    String baseOperation = forLoop ? "CURSOR_FOR_LOOP" : "DECLARE";
                    String mapKey = cursorName + "::" + (nodeId != null ? nodeId : "");

                    ObjectNode entry = cursorMap.get(mapKey);
                    if (entry == null) {
                        entry = mapper.createObjectNode();
                        entry.put("cursorName", cursorName);
                        entry.put("cursorType", cursorType);
                        entry.put("queryText", query != null ? query : "");
                        entry.set("operations", mapper.createArrayNode());
                        entry.put("accessCount", 0);
                        entry.set("accessDetails", mapper.createArrayNode());
                        cursorMap.put(mapKey, entry);
                    }

                    ArrayNode operations = (ArrayNode) entry.get("operations");
                    boolean opFound = false;
                    for (JsonNode op : operations) {
                        if (op.asText().equals(baseOperation)) { opFound = true; break; }
                    }
                    if (!opFound) operations.add(baseOperation);

                    entry.put("accessCount", entry.get("accessCount").asInt() + 1);

                    ObjectNode det = mapper.createObjectNode();
                    det.put("operation", baseOperation);
                    det.put("procedureId", nodeId != null ? nodeId : "");
                    det.put("procedureName", nodeName != null ? nodeName : "");
                    det.put("lineNumber", line);
                    det.put("sourceFile", sourceFile != null ? sourceFile : "");
                    det.put("queryText", query != null ? query : "");
                    ((ArrayNode) entry.get("accessDetails")).add(det);

                    JsonNode stmtOps = cursor.get("stmtOperations");
                    if (stmtOps != null && stmtOps.isArray()) {
                        for (JsonNode sop : stmtOps) {
                            String sopType = textOrNull(sop, "operation");
                            int sopLine = sop.has("line") ? sop.get("line").asInt() : 0;
                            if (sopType == null) continue;

                            boolean sopFound = false;
                            for (JsonNode existOp : operations) {
                                if (existOp.asText().equals(sopType)) { sopFound = true; break; }
                            }
                            if (!sopFound) operations.add(sopType);

                            entry.put("accessCount", entry.get("accessCount").asInt() + 1);

                            ObjectNode sopDetail = mapper.createObjectNode();
                            sopDetail.put("operation", sopType);
                            sopDetail.put("procedureId", nodeId != null ? nodeId : "");
                            sopDetail.put("procedureName", nodeName != null ? nodeName : "");
                            sopDetail.put("lineNumber", sopLine);
                            sopDetail.put("sourceFile", sourceFile != null ? sourceFile : "");
                            ((ArrayNode) entry.get("accessDetails")).add(sopDetail);
                        }
                    }
                }
            }
        }

        ArrayNode result = mapper.createArrayNode();
        cursorMap.values().forEach(result::add);
        r.synthesizedCache.put(key, result);
        return result;
    }
}
