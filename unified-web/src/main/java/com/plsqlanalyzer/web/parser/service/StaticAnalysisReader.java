package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Reads analysis data from the static flow-output directory.
 * This is the original parser output — immutable, written by ChunkedFlowWriter.
 */
@Component("parserStaticAnalysisReader")
public class StaticAnalysisReader implements AnalysisDataReader {

    private static final int MAX_CACHE_ENTRIES = 30;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, JsonNode> synthesizedCache = Collections.synchronizedMap(
            new LinkedHashMap<>(32, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, JsonNode> eldest) {
                    return size() > MAX_CACHE_ENTRIES;
                }
            });

    private final String flowOutputDir;

    public StaticAnalysisReader(@Value("${app.data-dir.base:data}") String dataDir) {
        this.flowOutputDir = Path.of(dataDir, "plsql-parse").toString();
    }

    @Override
    public JsonNode getIndex(String name) throws IOException {
        Path file = resolvePath(name, "api/index.json");
        return mapper.readTree(file.toFile());
    }

    @Override
    public JsonNode getNodeDetail(String name, String fileName) throws IOException {
        Path file = resolvePath(name, "api/nodes/" + fileName);
        return mapper.readTree(file.toFile());
    }

    @Override
    public JsonNode getTables(String name) throws IOException {
        Path file = resolvePath(name, "api/tables/index.json");
        return mapper.readTree(file.toFile());
    }

    @Override
    public JsonNode getCallGraph(String name) throws IOException {
        Path file = resolvePath(name, "api/call_graph.json");
        return mapper.readTree(file.toFile());
    }

    @Override
    public String getSource(String name, String fileName) throws IOException {
        Path file = Path.of(flowOutputDir, name, "sources", fileName);
        if (!Files.exists(file)) {
            Path dir = file.getParent();
            String base = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
            for (String ext : new String[]{".sql", ".prc", ".fnc", ".pkb", ".pks", ".trg"}) {
                Path alt = dir.resolve(base + ext);
                if (Files.exists(alt)) { file = alt; break; }
            }
        }
        return Files.readString(file);
    }

    @Override
    public JsonNode getResolver(String name, String type) throws IOException {
        String fileName = switch (type) {
            case "q1" -> "q1_object_resolution.json";
            case "q3" -> "q3_reverse_dependencies.json";
            case "q4" -> "q4_direct_dependencies.json";
            case "ambiguous" -> "q1_ambiguous_objects.json";
            default -> throw new IllegalArgumentException("Unknown resolver type: " + type);
        };
        Path file = resolvePath(name, "resolver/" + fileName);
        return mapper.readTree(file.toFile());
    }

    @Override
    public JsonNode getProcedures(String name) throws IOException {
        JsonNode index = getIndex(name);
        JsonNode nodes = index.get("nodes");
        return (nodes != null && nodes.isArray()) ? nodes : mapper.createArrayNode();
    }

    @Override
    public JsonNode getJoins(String name) throws IOException {
        String key = cacheKey(name, "joins");
        JsonNode cached = synthesizedCache.get(key);
        if (cached != null) return cached;

        Path nodesDir = Path.of(flowOutputDir, name, "api/nodes");
        if (!Files.isDirectory(nodesDir)) return mapper.createArrayNode();

        Map<String, ObjectNode> joinMap = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(nodesDir)) {
            List<Path> nodeFiles = files.filter(p -> p.toString().endsWith(".json")).toList();
            for (Path nodeFile : nodeFiles) {
                JsonNode node = mapper.readTree(nodeFile.toFile());
                String nodeId = textOrNull(node, "nodeId");
                String nodeName = textOrNull(node, "name");
                String sourceFile = textOrNull(node, "sourceFile");
                JsonNode tables = node.get("tables");
                if (tables == null || !tables.isArray()) continue;

                for (JsonNode table : tables) {
                    String leftTable = textOrNull(table, "name");
                    if (leftTable == null) continue;
                    JsonNode joins = table.get("joins");
                    if (joins == null || !joins.isArray()) continue;

                    for (JsonNode join : joins) {
                        String rightTable = textOrNull(join, "joinedTable");
                        String joinType = textOrNull(join, "joinType");
                        String condition = textOrNull(join, "condition");
                        int line = join.has("line") ? join.get("line").asInt() : 0;
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
                        ((ArrayNode) entry.get("accessDetails")).add(detail);
                    }
                }
            }
        }

        ArrayNode result = mapper.createArrayNode();
        joinMap.values().forEach(result::add);
        synthesizedCache.put(key, result);
        return result;
    }

    @Override
    public JsonNode getCursors(String name) throws IOException {
        String key = cacheKey(name, "cursors");
        JsonNode cached = synthesizedCache.get(key);
        if (cached != null) return cached;

        Path nodesDir = Path.of(flowOutputDir, name, "api/nodes");
        if (!Files.isDirectory(nodesDir)) return mapper.createArrayNode();

        Map<String, ObjectNode> cursorMap = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(nodesDir)) {
            List<Path> nodeFiles = files.filter(p -> p.toString().endsWith(".json")).toList();
            for (Path nodeFile : nodeFiles) {
                JsonNode node = mapper.readTree(nodeFile.toFile());
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
        synthesizedCache.put(key, result);
        return result;
    }

    @Override
    public JsonNode getSequences(String name) throws IOException {
        String key = cacheKey(name, "sequences");
        JsonNode cached = synthesizedCache.get(key);
        if (cached != null) return cached;

        Path nodesDir = Path.of(flowOutputDir, name, "api/nodes");
        if (!Files.isDirectory(nodesDir)) return mapper.createArrayNode();

        Map<String, ObjectNode> seqMap = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(nodesDir)) {
            List<Path> nodeFiles = files.filter(p -> p.toString().endsWith(".json")).toList();
            for (Path nodeFile : nodeFiles) {
                JsonNode node = mapper.readTree(nodeFile.toFile());
                String nodeId = textOrNull(node, "nodeId");
                String nodeName = textOrNull(node, "name");
                String sourceFile = textOrNull(node, "sourceFile");
                JsonNode sequences = node.get("sequences");
                if (sequences == null || !sequences.isArray()) continue;

                for (JsonNode seq : sequences) {
                    String seqName = textOrNull(seq, "name");
                    if (seqName == null) continue;
                    String seqSchema = textOrNull(seq, "schema");
                    String mapKey = (seqSchema != null ? seqSchema + "." : "") + seqName;

                    ObjectNode entry = seqMap.get(mapKey);
                    if (entry == null) {
                        entry = mapper.createObjectNode();
                        entry.put("sequenceName", seqName);
                        entry.put("sequenceSchema", seqSchema != null ? seqSchema : "");
                        entry.set("operations", mapper.createArrayNode());
                        entry.put("accessCount", 0);
                        entry.set("accessDetails", mapper.createArrayNode());
                        seqMap.put(mapKey, entry);
                    }

                    JsonNode ops = seq.get("operations");
                    if (ops != null && ops.isObject()) {
                        ArrayNode entryOps = (ArrayNode) entry.get("operations");
                        Iterator<String> fieldIter = ops.fieldNames();
                        while (fieldIter.hasNext()) {
                            String opType = fieldIter.next();
                            boolean found = false;
                            for (JsonNode existOp : entryOps) {
                                if (existOp.asText().equals(opType)) { found = true; break; }
                            }
                            if (!found) entryOps.add(opType);

                            JsonNode lines = ops.get(opType);
                            if (lines != null && lines.isArray()) {
                                for (JsonNode lineNode : lines) {
                                    entry.put("accessCount", entry.get("accessCount").asInt() + 1);
                                    ObjectNode detail = mapper.createObjectNode();
                                    detail.put("operation", opType);
                                    detail.put("procedureId", nodeId != null ? nodeId : "");
                                    detail.put("procedureName", nodeName != null ? nodeName : "");
                                    detail.put("lineNumber", lineNode.asInt());
                                    detail.put("sourceFile", sourceFile != null ? sourceFile : "");
                                    ((ArrayNode) entry.get("accessDetails")).add(detail);
                                }
                            }
                        }
                    }
                }
            }
        }

        ArrayNode result = mapper.createArrayNode();
        seqMap.values().forEach(result::add);
        synthesizedCache.put(key, result);
        return result;
    }

    private String cacheKey(String name, String type) {
        return name + "::" + type;
    }

    public void evictCache(String analysisName) {
        synthesizedCache.remove(cacheKey(analysisName, "joins"));
        synthesizedCache.remove(cacheKey(analysisName, "cursors"));
        synthesizedCache.remove(cacheKey(analysisName, "sequences"));
    }

    @Override
    public JsonNode getCallTree(String name, String nodeId) throws IOException {
        JsonNode index = getIndex(name);
        JsonNode callGraph = getCallGraph(name);
        JsonNode edges = callGraph.get("edges");
        Map<String, String> shortToFull = loadIdMap(name);

        Map<String, JsonNode> nodeMetadata = new HashMap<>();
        JsonNode nodes = index.get("nodes");
        if (nodes != null && nodes.isArray()) {
            for (JsonNode n : nodes) {
                String fullId = textOrNull(n, "nodeId");
                if (fullId != null) nodeMetadata.put(fullId, n);
            }
        }

        Map<String, List<JsonNode>> adjacency = new HashMap<>();
        if (edges != null && edges.isArray()) {
            for (JsonNode edge : edges) {
                String from = textOrNull(edge, "fromNodeId");
                if (from == null) continue;
                String fullFrom = resolveToFullId(from, shortToFull, nodeMetadata.keySet());
                adjacency.computeIfAbsent(fullFrom, k -> new ArrayList<>()).add(edge);
            }
        }

        String resolvedId = resolveToFullId(nodeId, shortToFull, nodeMetadata.keySet());
        Set<String> visited = new HashSet<>();
        return buildCallTreeNode(resolvedId, nodeMetadata, adjacency, shortToFull, visited, null, 0);
    }

    @Override
    public JsonNode getCallers(String name, String nodeId) throws IOException {
        JsonNode index = getIndex(name);
        JsonNode callGraph = getCallGraph(name);
        JsonNode edges = callGraph.get("edges");
        Map<String, String> shortToFull = loadIdMap(name);

        Map<String, JsonNode> nodeMetadata = new HashMap<>();
        JsonNode nodes = index.get("nodes");
        if (nodes != null && nodes.isArray()) {
            for (JsonNode n : nodes) {
                String fullId = textOrNull(n, "nodeId");
                if (fullId != null) nodeMetadata.put(fullId, n);
            }
        }

        String resolvedId = resolveToFullId(nodeId, shortToFull, nodeMetadata.keySet());

        ArrayNode result = mapper.createArrayNode();
        if (edges != null && edges.isArray()) {
            for (JsonNode edge : edges) {
                String toId = textOrNull(edge, "toNodeId");
                if (toId == null) continue;
                String fullToId = resolveToFullId(toId, shortToFull, nodeMetadata.keySet());
                if (!fullToId.equals(resolvedId)) continue;

                String fromId = textOrNull(edge, "fromNodeId");
                if (fromId == null) continue;
                String fullFromId = resolveToFullId(fromId, shortToFull, nodeMetadata.keySet());
                JsonNode meta = nodeMetadata.get(fullFromId);

                ObjectNode caller = mapper.createObjectNode();
                caller.put("id", fullFromId);
                caller.put("name", meta != null ? textOrDefault(meta, "name", fullFromId) : textOrDefault(edge, "from", fullFromId));
                caller.put("schemaName", meta != null ? textOrDefault(meta, "schema", "") : textOrDefault(edge, "fromSchema", ""));
                caller.put("packageName", meta != null ? textOrDefault(meta, "packageName", "") : "");
                caller.put("objectType", meta != null ? textOrDefault(meta, "objectType", "") : "");
                caller.put("sourceFile", meta != null ? textOrDefault(meta, "sourceFile", "") : "");
                caller.put("startLine", meta != null ? intOrDefault(meta, "lineStart", 0) : 0);
                caller.put("endLine", meta != null ? intOrDefault(meta, "lineEnd", 0) : 0);
                caller.put("unitType", meta != null ? textOrDefault(meta, "objectType", "PROCEDURE") : "PROCEDURE");

                JsonNode lines = edge.get("lines");
                if (lines != null && lines.isArray() && !lines.isEmpty()) {
                    caller.put("callLineNumber", lines.get(0).asInt());
                    ArrayNode allLines = mapper.createArrayNode();
                    for (JsonNode ln : lines) allLines.add(ln.asInt());
                    caller.set("callLines", allLines);
                } else {
                    caller.put("callLineNumber", 0);
                }

                String fromSchema = meta != null ? textOrNull(meta, "schema") : textOrNull(edge, "fromSchema");
                String toSchema = textOrNull(edge, "toSchema");
                String callType = (fromSchema != null && fromSchema.equals(toSchema)) ? "INTERNAL" : "EXTERNAL";
                caller.put("callType", callType);

                result.add(caller);
            }
        }

        return result;
    }

    // --- helpers (package-visible for reuse by Claude reader) ---

    Map<String, String> loadIdMap(String name) {
        try {
            Path file = resolvePath(name, "api/node_id_map.json");
            if (!Files.isRegularFile(file)) return Collections.emptyMap();
            JsonNode root = mapper.readTree(file.toFile());
            JsonNode stf = root.get("shortToFull");
            if (stf == null || !stf.isObject()) return Collections.emptyMap();
            Map<String, String> map = new HashMap<>();
            stf.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asText()));
            return map;
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    Path resolvePath(String name, String relative) {
        return Path.of(flowOutputDir, name, relative);
    }

    String getFlowOutputDir() {
        return flowOutputDir;
    }

    private ObjectNode buildCallTreeNode(
            String nodeId,
            Map<String, JsonNode> nodeMetadata,
            Map<String, List<JsonNode>> adjacency,
            Map<String, String> shortToFull,
            Set<String> visited,
            JsonNode incomingEdge,
            int depth) {

        ObjectNode treeNode = mapper.createObjectNode();
        String effectiveId = resolveToFullId(nodeId, shortToFull, nodeMetadata.keySet());
        JsonNode meta = nodeMetadata.get(effectiveId);

        treeNode.put("id", effectiveId);
        treeNode.put("name", meta != null ? textOrDefault(meta, "name", nodeId) : nodeId);
        treeNode.put("schemaName", meta != null ? textOrDefault(meta, "schema", "") : "");
        treeNode.put("packageName", meta != null ? textOrDefault(meta, "packageName", "") : "");
        treeNode.put("objectType", meta != null ? textOrDefault(meta, "objectType", "") : "");
        treeNode.put("unitType", meta != null ? textOrDefault(meta, "objectType", "PROCEDURE") : "PROCEDURE");
        treeNode.put("startLine", meta != null ? intOrDefault(meta, "lineStart", 0) : 0);
        treeNode.put("endLine", meta != null ? intOrDefault(meta, "lineEnd", 0) : 0);
        treeNode.put("sourceFile", meta != null ? textOrDefault(meta, "sourceFile", "") : "");
        boolean readable = meta == null || !meta.has("readable") || meta.get("readable").asBoolean(true);
        treeNode.put("readable", readable);

        if (incomingEdge != null) {
            String fromSchema = textOrNull(incomingEdge, "fromSchema");
            String toSchema = textOrNull(incomingEdge, "toSchema");
            String callType = (fromSchema != null && fromSchema.equals(toSchema)) ? "INTERNAL" : "EXTERNAL";
            treeNode.put("callType", callType);
            JsonNode lines = incomingEdge.get("lines");
            if (lines != null && lines.isArray() && !lines.isEmpty()) {
                treeNode.put("callLineNumber", lines.get(0).asInt());
            } else {
                treeNode.put("callLineNumber", 0);
            }
        } else {
            treeNode.put("callType", "ROOT");
            treeNode.put("callLineNumber", 0);
        }

        if (visited.contains(effectiveId)) {
            treeNode.put("circular", true);
            treeNode.set("children", mapper.createArrayNode());
            return treeNode;
        }

        visited.add(effectiveId);

        ArrayNode children = mapper.createArrayNode();
        List<JsonNode> outEdges = adjacency.get(effectiveId);
        if (outEdges != null) {
            for (JsonNode edge : outEdges) {
                String toId = textOrNull(edge, "toNodeId");
                if (toId != null) {
                    String fullToId = resolveToFullId(toId, shortToFull, nodeMetadata.keySet());
                    ObjectNode child = buildCallTreeNode(fullToId, nodeMetadata, adjacency, shortToFull, visited, edge, depth + 1);
                    children.add(child);
                }
            }
        }

        treeNode.set("children", children);
        visited.remove(effectiveId);
        return treeNode;
    }

    private String resolveToFullId(String id, Map<String, String> shortToFull, Set<String> knownFullIds) {
        if (id == null) return null;
        if (knownFullIds.contains(id)) return id;
        String mapped = shortToFull.get(id);
        if (mapped != null && knownFullIds.contains(mapped)) return mapped;
        for (String fullId : knownFullIds) {
            if (fullId.startsWith(id + "_") || fullId.startsWith(id + "$")) return fullId;
        }
        return id;
    }

    static String textOrDefault(JsonNode node, String field, String def) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : def;
    }

    static int intOrDefault(JsonNode node, String field, int def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asInt() : def;
    }

    static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : null;
    }
}
