package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.parser.model.ParserAnalysisInfo;
import com.analyzer.auth.ConnectionGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;

@Service
public class ParserAnalysisReader {

    private static final Logger log = LoggerFactory.getLogger(ParserAnalysisReader.class);
    private static final int MAX_CACHE_ENTRIES = 30;

    /** How long a parsed document may be served before it is re-read. See TtlCache. */
    private static final long CACHE_TTL_MS = TtlCache.DEFAULT_TTL_MS;

    private final ObjectMapper mapper = new ObjectMapper();
    /**
     * Parsed documents, bounded by BOTH size and age.
     *
     * <p>This was an LRU with no expiry. Because an access moves an entry to the head, a document
     * that is read regularly could be served indefinitely — so re-running an analysis rewrote the
     * artifact while the UI carried on showing the previous run's answers, with nothing to
     * invalidate it short of a restart.
     */
    private final ByteBudgetedCache<JsonNode> cache = new ByteBudgetedCache<>(
            ByteBudgetedCache.DEFAULT_MAX_BYTES, CACHE_TTL_MS, ParserAnalysisReader::estimateBytes);

    private final ArtifactStore store;

    /**
     * Supplies the calling user's routing identity for the cache key, or {@code null} in a
     * stripped-down context (a unit test constructing this with only a store) where every read is
     * scoped to {@link ConnectionGuard#SHARED}.
     */
    private final ConnectionGuard connectionGuard;

    /** Test / non-Spring construction: no per-user identity, so the cache is shared as before. */
    public ParserAnalysisReader(@Qualifier("parserArtifactStore") ArtifactStore store) {
        this(store, null);
    }

    @Autowired
    public ParserAnalysisReader(@Qualifier("parserArtifactStore") ArtifactStore store,
                                ConnectionGuard connectionGuard) {
        this.store = store;
        this.connectionGuard = connectionGuard;
    }

    /**
     * A cheap, stable estimate of a parsed document's footprint.
     *
     * <p>Not {@code writeValueAsBytes}: serialising a 49 MB artifact to measure it costs more than
     * the read the cache exists to avoid. The estimate only has to order entries against a budget,
     * not report memory accurately, so it counts nodes and charges a fixed overhead per node —
     * proportional to the real cost, and computed in one walk.
     */
    static long estimateBytes(JsonNode node) {
        if (node == null) return 1;
        long bytes = 48;                                  // object header, roughly
        if (node.isValueNode()) {
            return bytes + (node.isTextual() ? node.asText().length() * 2L : 8);
        }
        for (JsonNode child : node) bytes += estimateBytes(child);
        if (node.isObject()) {
            var names = node.fieldNames();
            while (names.hasNext()) bytes += names.next().length() * 2L;
        }
        return bytes;
    }

    /**
     * A token that changes when the analysis on disk changes.
     *
     * <p>Cache keys were the analysis name alone, so within the TTL window a re-analysis was served
     * from the previous run's parse — the UI showing answers for source that no longer exists. The
     * TTL bounded how long that lasted; it did not prevent it.
     *
     * <p>Derived from {@code api/index.json}'s size and modification time: two cheap stats, no read.
     * A rewritten analysis changes at least one of them, and an unchanged one changes neither, so a
     * cache hit means the parse is still valid rather than merely recent.
     */
    private String versionToken(String name) {
        try {
            ArtifactKey index = AnalysisPaths.resolveKey(name, "api/index.json");
            return store.size(index) + "-" + store.lastModified(index);
        } catch (RuntimeException e) {
            // Cannot establish a version, so do not claim one: a unique token makes this read
            // uncacheable rather than silently sharing another run's entry.
            return "unversioned-" + System.nanoTime();
        }
    }

    /**
     * The cache key for one facet of one analysis.
     *
     * <p>Scoped to the routing identity as well as the version on disk: the parsed document depends
     * only on the artifact, but the store it was read through belongs to one user, so the same
     * analysis name in two users' buckets must not share an entry. Two requests that resolve to no
     * identity both fall back to {@link ConnectionGuard#SHARED} and keep sharing, exactly as before.
     */
    private String cacheKey(String name, String facet) {
        return routingIdentity() + "::" + name + "::" + facet + "::" + versionToken(name);
    }

    /** The calling user's routing identity, or {@code "shared"} when none is resolvable. */
    private String routingIdentity() {
        return connectionGuard == null ? ConnectionGuard.SHARED : connectionGuard.routingIdentityKey();
    }

    private InputStream openRead(ArtifactKey key) throws IOException { return store.read(key); }

    public List<ParserAnalysisInfo> listAnalyses() throws IOException {
        // ONE prefix listing for the whole tree. The previous Files.walk + per-directory
        // isRegularFile("api/index.json") probe is a stat per candidate — fine locally, minutes of
        // latency over object storage. The same listing also carries each candidate's mtime, so the
        // manifest-freshness check below costs no extra round trip.
        ArtifactKey root = com.analyzer.storage.ArtifactKey.of(AnalysisPaths.PARSE_ROOT);
        Map<ArtifactKey, ArtifactEntry> entries;
        try {
            entries = store.statPrefix(root);
        } catch (IOException e) {
            return Collections.emptyList();
        }
        List<ArtifactKey> indexKeys = new ArrayList<>();
        for (ArtifactKey k : entries.keySet()) {
            List<String> segs = k.segments();
            int n = segs.size();
            // .../<OBJECT>/api/index.json — the marker of an analysis directory.
            if (n >= 4 && "index.json".equals(segs.get(n - 1)) && "api".equals(segs.get(n - 2))) {
                indexKeys.add(k);
            }
        }

        List<ParserAnalysisInfo> results = new ArrayList<>();
        indexKeys.sort(Comparator.comparing(k -> AnalysisPaths.nameOfKey(k.parent().parent())));
        for (ArtifactKey indexKey : indexKeys) {
            try {
                ParserAnalysisInfo info = buildInfo(indexKey, entries);
                if (info != null) results.add(info);
            } catch (IOException ignored) {}
        }
        return results;
    }

    public JsonNode getIndex(String name) throws IOException {
        return readTree(AnalysisPaths.resolveKey(name, "api/index.json"));
    }

    public JsonNode getNodeDetail(String name, String fileName) throws IOException {
        // fileName is caller-supplied; ArtifactKey rejects separators, so reject traversal here
        // with a clear message rather than letting the key constructor throw.
        requireSafeName(fileName);
        return readTree(AnalysisPaths.resolveKey(name, "api/nodes/" + fileName));
    }

    public JsonNode getTables(String name) throws IOException {
        return readTree(AnalysisPaths.resolveKey(name, "api/tables/index.json"));
    }

    public JsonNode getCallGraph(String name) throws IOException {
        return readTree(AnalysisPaths.resolveKey(name, "api/call_graph.json"));
    }

    public String getSource(String name, String fileName) throws IOException {
        requireSafeName(fileName);
        ArtifactKey dir = AnalysisPaths.dirKey(name).resolve("sources");
        ArtifactKey key = dir.resolve(fileName);
        if (!store.exists(key)) {
            String base = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
            for (String ext : new String[]{".sql", ".prc", ".fnc", ".pkb", ".pks", ".trg"}) {
                ArtifactKey alt = dir.resolve(base + ext);
                if (store.exists(alt)) { key = alt; break; }
            }
        }
        try (InputStream in = openRead(key)) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    public JsonNode getResolver(String name, String type) throws IOException {
        String fileName = switch (type) {
            case "q1" -> "q1_object_resolution.json";
            case "q3" -> "q3_reverse_dependencies.json";
            case "q4" -> "q4_direct_dependencies.json";
            case "ambiguous" -> "q1_ambiguous_objects.json";
            default -> throw new IllegalArgumentException("Unknown resolver type: " + type);
        };
        return readTree(AnalysisPaths.resolveKey(name, "resolver/" + fileName));
    }

    private JsonNode readTree(ArtifactKey key) throws IOException {
        try (InputStream in = openRead(key)) {
            return mapper.readTree(in);
        }
    }

    /**
     * The traversal guard that {@code dest.startsWith(dir)} used to provide. ArtifactKey rejects
     * separators outright, so ".." is the only remaining shape worth naming explicitly.
     */
    private static void requireSafeName(String fileName) {
        if (fileName == null || fileName.contains("..")) {
            throw new IllegalArgumentException("Illegal file name: " + fileName);
        }
    }

    public JsonNode getProcedures(String name) throws IOException {
        JsonNode index = getIndex(name);
        JsonNode nodes = index.get("nodes");
        return (nodes != null && nodes.isArray()) ? nodes : mapper.createArrayNode();
    }

    public JsonNode getJoins(String name) throws IOException {
        String key = cacheKey(name, "joins");
        JsonNode cached = cache.get(key);
        if (cached != null) return cached;

        List<ArtifactKey> nodeFiles = nodeFileKeys(name);
        if (nodeFiles.isEmpty()) return mapper.createArrayNode();

        Map<String, ObjectNode> joinMap = new LinkedHashMap<>();
        {
            for (ArtifactKey nodeFile : nodeFiles) {
                JsonNode node = readTree(nodeFile);
                String nodeId = txt(node, "nodeId");
                String nodeName = txt(node, "name");
                String sourceFile = txt(node, "sourceFile");
                JsonNode tables = node.get("tables");
                if (tables == null || !tables.isArray()) continue;

                for (JsonNode table : tables) {
                    String containingTable = txt(table, "name");
                    if (containingTable == null) continue;
                    JsonNode joins = table.get("joins");
                    if (joins == null || !joins.isArray()) continue;

                    for (JsonNode join : joins) {
                        String rightTable = txt(join, "joinedTable");
                        String joinType = txt(join, "joinType");
                        String condition = txt(join, "condition");
                        int line = join.has("line") ? join.get("line").asInt() : 0;
                        int queryId = join.has("queryId") ? join.get("queryId").asInt() : 0;
                        // Left side resolved from the ON-condition aliases (visitor); fall back to the
                        // containing table op only when it could not be resolved.
                        String resolvedLeft = txt(join, "leftTable");
                        String leftTable = resolvedLeft != null ? resolvedLeft : containingTable;
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
                        // Originating query/statement id so the UI can group joins by the SELECT
                        // they came from (rather than a fragile line-bucket heuristic).
                        detail.put("queryId", queryId);
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
        String key = cacheKey(name, "cursors");
        JsonNode cached = cache.get(key);
        if (cached != null) return cached;

        List<ArtifactKey> nodeFiles = nodeFileKeys(name);
        if (nodeFiles.isEmpty()) return mapper.createArrayNode();

        Map<String, ObjectNode> cursorMap = new LinkedHashMap<>();
        {
            for (ArtifactKey nodeFile : nodeFiles) {
                JsonNode node = readTree(nodeFile);
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
                    // One row per cursor INSTANCE: keyed by the declaration's own line so Oracle's
                    // identically-named "(FOR R)" implicit FOR-loop cursors do not collapse.
                    String mapKey = cursorName + "::" + (nodeId != null ? nodeId : "") + "#" + line;

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
        String key = cacheKey(name, "sequences");
        JsonNode cached = cache.get(key);
        if (cached != null) return cached;

        List<ArtifactKey> nodeFiles = nodeFileKeys(name);
        if (nodeFiles.isEmpty()) return mapper.createArrayNode();

        Map<String, ObjectNode> seqMap = new LinkedHashMap<>();
        {
            for (ArtifactKey nodeFile : nodeFiles) {
                JsonNode node = readTree(nodeFile);
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

    /**
     * Nodes built before {@code getCallTree} stops descending — MID-006.
     *
     * <p>The cycle guard in {@code buildTreeNode} only breaks a <b>direct</b> cycle: {@code visited}
     * is added and removed around each recursive call, so the same node reached by two different
     * paths is built twice, in full, once per path. That is the identical shape to the JAR call-tree
     * explosion already measured in this registry — one measured JAR endpoint reaches
     * 300,167,238 nodes from a source graph small enough to look harmless. Today's PL/SQL corpus
     * (52 procedures) tops out at 277 nodes for its worst root, which is why this went unnoticed:
     * the mechanism was always there, the corpus was just too small to trigger it.
     */
    private static final int MAX_CALL_TREE_NODES = 2000;

    /** Shared across one call to {@code getCallTree}, so the whole tree is capped, not each branch. */
    private static final class TreeBudget {
        int left = MAX_CALL_TREE_NODES;
        boolean truncated;
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
        TreeBudget budget = new TreeBudget();
        ObjectNode root = buildTreeNode(resolvedId, nodeMetadata, adjacency, shortToFull, visited,
                null, 0, budget);
        // Also at the root, in addition to wherever childrenTruncated landed: a reader checking
        // "was this tree cut at all" should not have to walk it looking for the per-node badge,
        // and both this session's own JS renderers already read a root-level `truncated` on
        // other endpoints (jar/js/callGraph.js, the search results in parser/js/callGraph.js) —
        // this keeps the same name meaning the same thing everywhere it appears.
        root.put("truncated", budget.truncated);
        return root;
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
        cache.remove(cacheKey(name, "joins"));
        cache.remove(cacheKey(name, "cursors"));
        cache.remove(cacheKey(name, "sequences"));
    }

    // --- helpers ---

    /** Every {@code api/nodes/*.json} for an analysis, in one listing. */
    private List<ArtifactKey> nodeFileKeys(String name) throws IOException {
        // No exists() precheck: list() of a missing prefix is already empty, and an extra stat is
        // a whole round trip on object storage.
        ArtifactKey nodesDir = AnalysisPaths.resolveKey(name, "api/nodes");
        try (Stream<ArtifactEntry> s = store.list(nodesDir, false)) {
            return s.filter(e -> !e.directory())
                    .map(ArtifactEntry::key)
                    .filter(k -> k.leaf().endsWith(".json"))
                    .toList();
        }
    }

    private Map<String, String> loadIdMap(String name) {
        try {
            ArtifactKey key = AnalysisPaths.resolveKey(name, "api/node_id_map.json");
            if (!store.exists(key)) return Collections.emptyMap();
            JsonNode root = readTree(key);
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
                                     Set<String> visited, JsonNode incomingEdge, int depth,
                                     TreeBudget budget) {
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
            int built = 0;
            for (JsonNode edge : outEdges) {
                if (budget.left <= 0) {
                    // childrenTruncated / omittedChildCount — the exact pair both call-tree
                    // renderers (parser/js/callGraph.js:311, plsql/js/callGraph.js:248) already
                    // read and have rendered a badge for since before this fix existed. The UI
                    // half of MID-006 was built first; only the server half was missing.
                    treeNode.put("childrenTruncated", true);
                    treeNode.put("omittedChildCount", outEdges.size() - built);
                    budget.truncated = true;
                    break;
                }
                String toId = txt(edge, "toNodeId");
                if (toId != null) {
                    budget.left--;
                    built++;
                    children.add(buildTreeNode(resolveId(toId, shortToFull, nodeMetadata.keySet()),
                            nodeMetadata, adjacency, shortToFull, visited, edge, depth + 1, budget));
                }
            }
        }
        treeNode.set("children", children);
        visited.remove(effectiveId);
        return treeNode;
    }

    /**
     * One analysis's listing row.
     *
     * <p>When a <b>fresh</b> {@code api/manifest.json} is present — one written no earlier than
     * {@code api/index.json} — the row is built from the manifest's {@code listing} scalars, which
     * the writer recorded from the very index root this method would otherwise read. That avoids
     * opening (and, for a large analysis, streaming past the {@code nodes[]} array of) index.json
     * once per analysis; the mtimes are already in hand from the one prefix listing, so the check
     * adds no round trip. Absent or stale, it falls back to the index read, unchanged.
     *
     * <p>Byte-identical either way: the manifest's listing scalars ARE the index root's scalars
     * (see {@code ChunkedFlowWriter.listingScalars}), mapped through the same helpers.
     *
     * @param indexKey the {@code .../<OBJECT>/api/index.json} key that identifies the analysis
     * @param entries  the whole-tree listing, for the manifest/index mtimes
     */
    private ParserAnalysisInfo buildInfo(ArtifactKey indexKey, Map<ArtifactKey, ArtifactEntry> entries)
            throws IOException {
        ArtifactKey manifestKey = indexKey.parent().resolve("manifest.json");
        ArtifactEntry manifestEntry = entries.get(manifestKey);
        if (manifestEntry != null && manifestEntry.lastModified() >= 0) {
            ArtifactEntry indexEntry = entries.get(indexKey);
            boolean fresh = indexEntry == null
                    || manifestEntry.lastModified() >= indexEntry.lastModified();
            if (fresh) {
                JsonNode listing = readManifestListing(manifestKey);
                if (listing != null) {
                    String name = AnalysisPaths.nameOfKey(indexKey.parent().parent());
                    return buildInfoFromScalars(name, listing);
                }
            }
        }
        JsonNode root = readTree(indexKey);
        return buildInfoFromScalars(AnalysisPaths.nameOfKey(indexKey.parent().parent()), root);
    }

    /** The {@code listing} object of a manifest document, or {@code null} when it is absent. */
    private JsonNode readManifestListing(ArtifactKey manifestKey) {
        try {
            JsonNode doc = readTree(manifestKey);
            JsonNode listing = doc == null ? null : doc.get("listing");
            return (listing != null && listing.isObject()) ? listing : null;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Maps an analysis's listing scalars — the index root's own, whether read from {@code
     * api/index.json} or taken from the manifest's {@code listing} object — to the listing row.
     */
    private ParserAnalysisInfo buildInfoFromScalars(String name, JsonNode root) {
        ParserAnalysisInfo info = new ParserAnalysisInfo();
        info.setName(name);
        info.setEntryPoint(txtDef(root, "entryPoint", AnalysisPaths.leaf(name)));
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
