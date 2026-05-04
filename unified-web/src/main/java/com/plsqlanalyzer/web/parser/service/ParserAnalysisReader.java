package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.parser.model.ParserAnalysisInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

@Service
public class ParserAnalysisReader {

    private static final Logger log = LoggerFactory.getLogger(ParserAnalysisReader.class);
    private static final int MAX_CACHE_ENTRIES = 30;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, JsonNode> cache = Collections.synchronizedMap(
            new LinkedHashMap<>(32, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, JsonNode> eldest) {
                    return size() > MAX_CACHE_ENTRIES;
                }
            });

    @Value("${app.data-dir.base:data}")
    private String dataDir;

    private Path baseDir() {
        return Path.of(dataDir, "plsql-parse");
    }

    public List<ParserAnalysisInfo> listAnalyses() throws IOException {
        Path root = baseDir();
        if (!Files.isDirectory(root)) return Collections.emptyList();

        List<ParserAnalysisInfo> results = new ArrayList<>();
        try (Stream<Path> dirs = Files.list(root)) {
            dirs.filter(Files::isDirectory).sorted().forEach(dir -> {
                try {
                    ParserAnalysisInfo info = buildInfo(dir);
                    if (info != null) results.add(info);
                } catch (IOException ignored) {}
            });
        }
        return results;
    }

    public JsonNode getIndex(String name) throws IOException {
        return mapper.readTree(resolve(name, "api/index.json").toFile());
    }

    public JsonNode getNodeDetail(String name, String fileName) throws IOException {
        return mapper.readTree(resolve(name, "api/nodes/" + fileName).toFile());
    }

    public JsonNode getTables(String name) throws IOException {
        return mapper.readTree(resolve(name, "api/tables/index.json").toFile());
    }

    public JsonNode getCallGraph(String name) throws IOException {
        return mapper.readTree(resolve(name, "api/call_graph.json").toFile());
    }

    public String getSource(String name, String fileName) throws IOException {
        Path file = Path.of(dataDir, "plsql-parse", name, "sources", fileName);
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

    public JsonNode getResolver(String name, String type) throws IOException {
        String fileName = switch (type) {
            case "q1" -> "q1_object_resolution.json";
            case "q3" -> "q3_reverse_dependencies.json";
            case "q4" -> "q4_direct_dependencies.json";
            case "ambiguous" -> "q1_ambiguous_objects.json";
            default -> throw new IllegalArgumentException("Unknown resolver type: " + type);
        };
        return mapper.readTree(resolve(name, "resolver/" + fileName).toFile());
    }

    public JsonNode getProcedures(String name) throws IOException {
        JsonNode index = getIndex(name);
        JsonNode nodes = index.get("nodes");
        return (nodes != null && nodes.isArray()) ? nodes : mapper.createArrayNode();
    }

    public JsonNode getJoins(String name) throws IOException {
        String key = name + "::joins";
        JsonNode cached = cache.get(key);
        if (cached != null) return cached;

        Path nodesDir = Path.of(dataDir, "plsql-parse", name, "api/nodes");
        if (!Files.isDirectory(nodesDir)) return mapper.createArrayNode();

        Map<String, ObjectNode> joinMap = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(nodesDir)) {
            for (Path nodeFile : files.filter(p -> p.toString().endsWith(".json")).toList()) {
                JsonNode node = mapper.readTree(nodeFile.toFile());
                String nodeId = txt(node, "nodeId");
                String nodeName = txt(node, "name");
                String sourceFile = txt(node, "sourceFile");
                JsonNode tables = node.get("tables");
                if (tables == null || !tables.isArray()) continue;

                for (JsonNode table : tables) {
                    String leftTable = txt(table, "name");
                    if (leftTable == null) continue;
                    JsonNode joins = table.get("joins");
                    if (joins == null || !joins.isArray()) continue;

                    for (JsonNode join : joins) {
                        String rightTable = txt(join, "joinedTable");
                        String joinType = txt(join, "joinType");
                        String condition = txt(join, "condition");
                        int line = join.has("line") ? join.get("line").asInt() : 0;
                        if (rightTable == null) continue;

                        String pairKey = leftTable.compareToIgnoreCase(rightTable) <= 0
                                ? leftTable.toUpperCase() + "::" + rightTable.toUpperCase()
                                : rightTable.toUpperCase() + "::" + leftTable.toUpperCase();

                        ObjectNode entry = joinMap.computeIfAbsent(pairKey, k -> {
                            ObjectNode e = mapper.createObjectNode();
                            e.put("leftTable", leftTable.compareToIgnoreCase(rightTable) <= 0 ? leftTable : rightTable);
                            e.put("rightTable", leftTable.compareToIgnoreCase(rightTable) <= 0 ? rightTable : leftTable);
                            e.set("joinTypes", mapper.createArrayNode());
                            e.put("accessCount", 0);
                            e.set("accessDetails", mapper.createArrayNode());
                            return e;
                        });

                        ArrayNode joinTypes = (ArrayNode) entry.get("joinTypes");
                        if (joinType != null) {
                            boolean found = false;
                            for (JsonNode jt : joinTypes) { if (jt.asText().equals(joinType)) { found = true; break; } }
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
        cache.put(key, result);
        return result;
    }

    public JsonNode getCursors(String name) throws IOException {
        String key = name + "::cursors";
        JsonNode cached = cache.get(key);
        if (cached != null) return cached;

        Path nodesDir = Path.of(dataDir, "plsql-parse", name, "api/nodes");
        if (!Files.isDirectory(nodesDir)) return mapper.createArrayNode();

        Map<String, ObjectNode> cursorMap = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(nodesDir)) {
            for (Path nodeFile : files.filter(p -> p.toString().endsWith(".json")).toList()) {
                JsonNode node = mapper.readTree(nodeFile.toFile());
                String nodeId = txt(node, "nodeId");
                String nodeName = txt(node, "name");
                String sourceFile = txt(node, "sourceFile");
                JsonNode cursors = node.get("cursors");
                if (cursors == null || !cursors.isArray()) continue;

                for (JsonNode cursor : cursors) {
                    String rawCursorName = txt(cursor, "name");
                    final String cursorName = rawCursorName != null ? rawCursorName : "UNNAMED";
                    String query = txt(cursor, "query");
                    boolean forLoop = cursor.has("forLoop") && cursor.get("forLoop").asBoolean();
                    boolean refCursor = cursor.has("refCursor") && cursor.get("refCursor").asBoolean();
                    int line = cursor.has("line") ? cursor.get("line").asInt() : 0;

                    final String cursorType = refCursor ? "REF_CURSOR" : forLoop ? "FOR_LOOP" : "EXPLICIT";
                    final String baseOp = forLoop ? "CURSOR_FOR_LOOP" : "DECLARE";
                    final String queryText = query != null ? query : "";
                    String mapKey = cursorName + "::" + (nodeId != null ? nodeId : "");

                    ObjectNode entry = cursorMap.computeIfAbsent(mapKey, k -> {
                        ObjectNode e = mapper.createObjectNode();
                        e.put("cursorName", cursorName);
                        e.put("cursorType", cursorType);
                        e.put("queryText", queryText);
                        e.set("operations", mapper.createArrayNode());
                        e.put("accessCount", 0);
                        e.set("accessDetails", mapper.createArrayNode());
                        return e;
                    });

                    addUnique((ArrayNode) entry.get("operations"), baseOp);
                    entry.put("accessCount", entry.get("accessCount").asInt() + 1);

                    ObjectNode det = mapper.createObjectNode();
                    det.put("operation", baseOp);
                    det.put("procedureId", nodeId != null ? nodeId : "");
                    det.put("procedureName", nodeName != null ? nodeName : "");
                    det.put("lineNumber", line);
                    det.put("sourceFile", sourceFile != null ? sourceFile : "");
                    det.put("queryText", query != null ? query : "");
                    ((ArrayNode) entry.get("accessDetails")).add(det);

                    JsonNode stmtOps = cursor.get("stmtOperations");
                    if (stmtOps != null && stmtOps.isArray()) {
                        for (JsonNode sop : stmtOps) {
                            String sopType = txt(sop, "operation");
                            int sopLine = sop.has("line") ? sop.get("line").asInt() : 0;
                            if (sopType == null) continue;
                            addUnique((ArrayNode) entry.get("operations"), sopType);
                            entry.put("accessCount", entry.get("accessCount").asInt() + 1);
                            ObjectNode sd = mapper.createObjectNode();
                            sd.put("operation", sopType);
                            sd.put("procedureId", nodeId != null ? nodeId : "");
                            sd.put("procedureName", nodeName != null ? nodeName : "");
                            sd.put("lineNumber", sopLine);
                            sd.put("sourceFile", sourceFile != null ? sourceFile : "");
                            ((ArrayNode) entry.get("accessDetails")).add(sd);
                        }
                    }
                }
            }
        }

        ArrayNode result = mapper.createArrayNode();
        cursorMap.values().forEach(result::add);
        cache.put(key, result);
        return result;
    }

    public JsonNode getSequences(String name) throws IOException {
        String key = name + "::sequences";
        JsonNode cached = cache.get(key);
        if (cached != null) return cached;

        Path nodesDir = Path.of(dataDir, "plsql-parse", name, "api/nodes");
        if (!Files.isDirectory(nodesDir)) return mapper.createArrayNode();

        Map<String, ObjectNode> seqMap = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(nodesDir)) {
            for (Path nodeFile : files.filter(p -> p.toString().endsWith(".json")).toList()) {
                JsonNode node = mapper.readTree(nodeFile.toFile());
                String nodeId = txt(node, "nodeId");
                String nodeName = txt(node, "name");
                String sourceFile = txt(node, "sourceFile");
                JsonNode sequences = node.get("sequences");
                if (sequences == null || !sequences.isArray()) continue;

                for (JsonNode seq : sequences) {
                    String seqName = txt(seq, "name");
                    if (seqName == null) continue;
                    String seqSchema = txt(seq, "schema");
                    String mapKey = (seqSchema != null ? seqSchema + "." : "") + seqName;

                    ObjectNode entry = seqMap.computeIfAbsent(mapKey, k -> {
                        ObjectNode e = mapper.createObjectNode();
                        e.put("sequenceName", seqName);
                        e.put("sequenceSchema", seqSchema != null ? seqSchema : "");
                        e.set("operations", mapper.createArrayNode());
                        e.put("accessCount", 0);
                        e.set("accessDetails", mapper.createArrayNode());
                        return e;
                    });

                    JsonNode ops = seq.get("operations");
                    if (ops != null && ops.isObject()) {
                        ArrayNode entryOps = (ArrayNode) entry.get("operations");
                        Iterator<String> fieldIter = ops.fieldNames();
                        while (fieldIter.hasNext()) {
                            String opType = fieldIter.next();
                            addUnique(entryOps, opType);
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
        cache.put(key, result);
        return result;
    }

    public JsonNode getCallTree(String name, String nodeId) throws IOException {
        JsonNode index = getIndex(name);
        JsonNode callGraph = getCallGraph(name);
        JsonNode edges = callGraph.get("edges");
        Map<String, String> shortToFull = loadIdMap(name);

        Map<String, JsonNode> nodeMetadata = new HashMap<>();
        JsonNode nodes = index.get("nodes");
        if (nodes != null && nodes.isArray()) {
            for (JsonNode n : nodes) {
                String fullId = txt(n, "nodeId");
                if (fullId != null) nodeMetadata.put(fullId, n);
            }
        }

        Map<String, List<JsonNode>> adjacency = new HashMap<>();
        if (edges != null && edges.isArray()) {
            for (JsonNode edge : edges) {
                String from = txt(edge, "fromNodeId");
                if (from == null) continue;
                String fullFrom = resolveId(from, shortToFull, nodeMetadata.keySet());
                adjacency.computeIfAbsent(fullFrom, k -> new ArrayList<>()).add(edge);
            }
        }

        String resolvedId = resolveId(nodeId, shortToFull, nodeMetadata.keySet());
        Set<String> visited = new HashSet<>();
        return buildTreeNode(resolvedId, nodeMetadata, adjacency, shortToFull, visited, null, 0);
    }

    public JsonNode getCallers(String name, String nodeId) throws IOException {
        JsonNode index = getIndex(name);
        JsonNode callGraph = getCallGraph(name);
        JsonNode edges = callGraph.get("edges");
        Map<String, String> shortToFull = loadIdMap(name);

        Map<String, JsonNode> nodeMetadata = new HashMap<>();
        JsonNode nodes = index.get("nodes");
        if (nodes != null && nodes.isArray()) {
            for (JsonNode n : nodes) {
                String fullId = txt(n, "nodeId");
                if (fullId != null) nodeMetadata.put(fullId, n);
            }
        }

        String resolvedId = resolveId(nodeId, shortToFull, nodeMetadata.keySet());
        ArrayNode result = mapper.createArrayNode();
        if (edges != null && edges.isArray()) {
            for (JsonNode edge : edges) {
                String toId = txt(edge, "toNodeId");
                if (toId == null) continue;
                String fullToId = resolveId(toId, shortToFull, nodeMetadata.keySet());
                if (!fullToId.equals(resolvedId)) continue;
                String fromId = txt(edge, "fromNodeId");
                if (fromId == null) continue;
                String fullFromId = resolveId(fromId, shortToFull, nodeMetadata.keySet());
                JsonNode meta = nodeMetadata.get(fullFromId);

                ObjectNode caller = mapper.createObjectNode();
                caller.put("id", fullFromId);
                caller.put("name", meta != null ? txtDef(meta, "name", fullFromId) : txtDef(edge, "from", fullFromId));
                caller.put("schemaName", meta != null ? txtDef(meta, "schema", "") : txtDef(edge, "fromSchema", ""));
                caller.put("packageName", meta != null ? txtDef(meta, "packageName", "") : "");
                caller.put("objectType", meta != null ? txtDef(meta, "objectType", "") : "");
                caller.put("sourceFile", meta != null ? txtDef(meta, "sourceFile", "") : "");

                JsonNode lines = edge.get("lines");
                if (lines != null && lines.isArray() && !lines.isEmpty()) {
                    caller.put("callLineNumber", lines.get(0).asInt());
                } else {
                    caller.put("callLineNumber", 0);
                }

                String fromSchema = meta != null ? txt(meta, "schema") : txt(edge, "fromSchema");
                String toSchema = txt(edge, "toSchema");
                caller.put("callType", (fromSchema != null && fromSchema.equals(toSchema)) ? "INTERNAL" : "EXTERNAL");
                result.add(caller);
            }
        }
        return result;
    }

    public void evictCache(String name) {
        cache.remove(name + "::joins");
        cache.remove(name + "::cursors");
        cache.remove(name + "::sequences");
    }

    // --- helpers ---

    private Path resolve(String name, String relative) {
        return Path.of(dataDir, "plsql-parse", name, relative);
    }

    private Map<String, String> loadIdMap(String name) {
        try {
            Path file = resolve(name, "api/node_id_map.json");
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

    private String resolveId(String id, Map<String, String> shortToFull, Set<String> knownFullIds) {
        if (id == null) return null;
        if (knownFullIds.contains(id)) return id;
        String mapped = shortToFull.get(id);
        if (mapped != null && knownFullIds.contains(mapped)) return mapped;
        for (String fullId : knownFullIds) {
            if (fullId.startsWith(id + "_") || fullId.startsWith(id + "$")) return fullId;
        }
        return id;
    }

    private ObjectNode buildTreeNode(String nodeId, Map<String, JsonNode> nodeMetadata,
                                     Map<String, List<JsonNode>> adjacency, Map<String, String> shortToFull,
                                     Set<String> visited, JsonNode incomingEdge, int depth) {
        ObjectNode treeNode = mapper.createObjectNode();
        String effectiveId = resolveId(nodeId, shortToFull, nodeMetadata.keySet());
        JsonNode meta = nodeMetadata.get(effectiveId);

        treeNode.put("id", effectiveId);
        treeNode.put("name", meta != null ? txtDef(meta, "name", nodeId) : nodeId);
        treeNode.put("schemaName", meta != null ? txtDef(meta, "schema", "") : "");
        treeNode.put("packageName", meta != null ? txtDef(meta, "packageName", "") : "");
        treeNode.put("objectType", meta != null ? txtDef(meta, "objectType", "") : "");
        treeNode.put("unitType", meta != null ? txtDef(meta, "objectType", "PROCEDURE") : "PROCEDURE");
        treeNode.put("startLine", meta != null ? intDef(meta, "lineStart", 0) : 0);
        treeNode.put("endLine", meta != null ? intDef(meta, "lineEnd", 0) : 0);
        treeNode.put("sourceFile", meta != null ? txtDef(meta, "sourceFile", "") : "");
        treeNode.put("readable", meta == null || !meta.has("readable") || meta.get("readable").asBoolean(true));

        if (incomingEdge != null) {
            String fromSchema = txt(incomingEdge, "fromSchema");
            String toSchema = txt(incomingEdge, "toSchema");
            treeNode.put("callType", (fromSchema != null && fromSchema.equals(toSchema)) ? "INTERNAL" : "EXTERNAL");
            JsonNode lines = incomingEdge.get("lines");
            treeNode.put("callLineNumber", (lines != null && lines.isArray() && !lines.isEmpty()) ? lines.get(0).asInt() : 0);
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
                String toId = txt(edge, "toNodeId");
                if (toId != null) {
                    children.add(buildTreeNode(resolveId(toId, shortToFull, nodeMetadata.keySet()),
                            nodeMetadata, adjacency, shortToFull, visited, edge, depth + 1));
                }
            }
        }
        treeNode.set("children", children);
        visited.remove(effectiveId);
        return treeNode;
    }

    private ParserAnalysisInfo buildInfo(Path dir) throws IOException {
        Path indexFile = dir.resolve("api/index.json");
        if (!Files.isRegularFile(indexFile)) return null;

        JsonNode root = mapper.readTree(indexFile.toFile());
        ParserAnalysisInfo info = new ParserAnalysisInfo();
        info.setName(dir.getFileName().toString());
        info.setEntryPoint(txtDef(root, "entryPoint", dir.getFileName().toString()));
        info.setEntrySchema(txtDef(root, "entrySchema", ""));
        info.setTotalNodes(intDef(root, "totalNodes", 0));
        info.setTotalTables(intDef(root, "totalTables", 0));
        info.setTotalEdges(intDef(root, "totalEdges", 0));
        info.setTotalLinesOfCode(intDef(root, "totalLinesOfCode", 0));
        info.setMaxDepth(intDef(root, "maxDepth", 0));
        info.setCrawlTimeMs(root.has("crawlTimeMs") && root.get("crawlTimeMs").isNumber() ? root.get("crawlTimeMs").asLong() : 0);
        info.setDbCallCount(intDef(root, "dbCallCount", 0));

        List<String> errors = new ArrayList<>();
        JsonNode errNode = root.get("errors");
        if (errNode != null && errNode.isArray()) errNode.forEach(e -> errors.add(e.asText()));
        info.setErrors(errors);
        return info;
    }

    private void addUnique(ArrayNode arr, String val) {
        for (JsonNode n : arr) { if (n.asText().equals(val)) return; }
        arr.add(val);
    }

    static String txt(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : null;
    }

    static String txtDef(JsonNode node, String field, String def) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : def;
    }

    static int intDef(JsonNode node, String field, int def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asInt() : def;
    }
}
