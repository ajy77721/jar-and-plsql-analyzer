package com.plsql.parser.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsql.parser.model.*;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunkedFlowWriter {

    private final Path buildDir;
    private final Path chunksDir;
    private final Path sourcesDir;
    private final Path progressFile;
    private final Path edgesFile;
    private final Path tablesFile;
    private final ObjectMapper mapper;
    private final AtomicInteger chunkCounter = new AtomicInteger(0);
    private final Path edgesDir;

    private final Map<String, String> progressMap = new LinkedHashMap<>();
    private List<Map<String, Object>> discoveredTriggers = new ArrayList<>();

    public ChunkedFlowWriter(Path buildDir) throws IOException {
        this.buildDir = buildDir;
        this.chunksDir = buildDir.resolve("chunks");
        this.sourcesDir = buildDir.resolve("sources");
        this.edgesDir = buildDir.resolve("edges");
        this.progressFile = buildDir.resolve("progress.json");
        this.edgesFile = buildDir.resolve("edges.json");
        this.tablesFile = buildDir.resolve("tables.json");

        Files.createDirectories(chunksDir);
        Files.createDirectories(sourcesDir);
        Files.createDirectories(edgesDir);

        this.mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        Files.writeString(tablesFile, "{}");
        writeProgress();
    }

    public Path getSourcesDir() {
        return sourcesDir;
    }

    public void setDiscoveredTriggers(List<Map<String, Object>> triggers) {
        this.discoveredTriggers = triggers != null ? triggers : new ArrayList<>();
    }

    public void markPending(String visitKey) throws IOException {
        progressMap.put(visitKey, "PENDING");
        writeProgress();
    }

    public void markProcessing(String visitKey) throws IOException {
        progressMap.put(visitKey, "PROCESSING");
        writeProgress();
    }

    public void markDone(String visitKey) throws IOException {
        progressMap.put(visitKey, "DONE");
        writeProgress();
    }

    public void markFailed(String visitKey, String error) throws IOException {
        progressMap.put(visitKey, "FAILED: " + error);
        writeProgress();
    }

    public void writeChunk(FlowNode node) throws IOException {
        int idx = chunkCounter.incrementAndGet();
        String fileName = String.format("chunk_%04d.json", idx);
        Path chunkFile = chunksDir.resolve(fileName);
        mapper.writeValue(chunkFile.toFile(), node);
    }

    public void appendEdges(List<FlowEdge> newEdges, int chunkIdx) throws IOException {
        if (newEdges == null || newEdges.isEmpty()) return;
        String fileName = String.format("edges_%04d.json", chunkIdx);
        mapper.writeValue(edgesDir.resolve(fileName).toFile(), newEdges);
    }

    public void updateTables(Map<String, SchemaTableInfo> tableInfoMap) throws IOException {
        mapper.writeValue(tablesFile.toFile(), tableInfoMap);
    }

    public void writeSourceFile(String fileName, String content) throws IOException {
        Files.writeString(sourcesDir.resolve(fileName), content);
    }

    public boolean sourceExists(String fileName) {
        return Files.exists(sourcesDir.resolve(fileName));
    }

    public String readSourceFile(String fileName) throws IOException {
        Path p = sourcesDir.resolve(fileName);
        if (Files.exists(p)) {
            return Files.readString(p);
        }
        return null;
    }

    public FlowResult mergeChunks(String entryPoint, String entrySchema, long crawlTimeMs,
                                   int maxDepthReached, List<String> errors) throws IOException {
        FlowResult result = new FlowResult();
        result.setEntryPoint(entryPoint);
        result.setEntrySchema(entrySchema);
        result.setCrawlTimeMs(crawlTimeMs);
        result.setMaxDepthReached(maxDepthReached);
        result.setErrors(errors);

        List<FlowNode> allNodes = new ArrayList<>();
        try (var stream = Files.list(chunksDir)) {
            List<Path> chunkFiles = stream
                    .filter(p -> p.getFileName().toString().startsWith("chunk_"))
                    .sorted()
                    .toList();

            for (Path cf : chunkFiles) {
                FlowNode node = mapper.readValue(cf.toFile(), FlowNode.class);
                allNodes.add(node);
            }
        }
        result.setFlowNodes(allNodes);

        List<FlowEdge> allEdges = new ArrayList<>();
        try (var edgeStream = Files.list(edgesDir)) {
            List<Path> edgeFiles = edgeStream
                    .filter(p -> p.getFileName().toString().startsWith("edges_"))
                    .sorted()
                    .toList();
            for (Path ef : edgeFiles) {
                FlowEdge[] batch = mapper.readValue(ef.toFile(), FlowEdge[].class);
                allEdges.addAll(Arrays.asList(batch));
            }
        }
        result.setCallGraph(allEdges);
        mapper.writeValue(edgesFile.toFile(), allEdges);

        String tablesContent = Files.readString(tablesFile);
        if (!tablesContent.isBlank() && !tablesContent.equals("{}")) {
            @SuppressWarnings("unchecked")
            Map<String, SchemaTableInfo> tableMap = mapper.readValue(tablesContent,
                    mapper.getTypeFactory().constructMapType(
                            LinkedHashMap.class, String.class, SchemaTableInfo.class));
            result.setAllTables(new ArrayList<>(tableMap.values()));
        }

        result.setTotalObjectsCrawled(allNodes.size());

        Set<String> uniqueNodeIds = new HashSet<>();
        int procs = 0, funcs = 0, pkgProcs = 0, pkgFuncs = 0, totalLoc = 0;
        for (FlowNode n : allNodes) {
            if (n.getNodeId() != null) uniqueNodeIds.add(n.getNodeId());
            totalLoc += n.getLinesOfCode();
            String type = n.getObjectType() != null ? n.getObjectType().toUpperCase() : "";
            boolean hasPkg = n.getPackageName() != null && !n.getPackageName().isEmpty();
            if (type.contains("PROCEDURE")) {
                if (hasPkg) pkgProcs++; else procs++;
            } else if (type.contains("FUNCTION")) {
                if (hasPkg) pkgFuncs++; else funcs++;
            }
        }
        result.setUniqueObjectsCrawled(uniqueNodeIds.size());
        result.setTotalProcedures(procs);
        result.setTotalFunctions(funcs);
        result.setTotalPackageProcedures(pkgProcs);
        result.setTotalPackageFunctions(pkgFuncs);
        result.setTotalLinesOfCode(totalLoc);

        Path flowJson = buildDir.resolve("flow.json");
        mapper.writeValue(flowJson.toFile(), result);
        System.err.println("[ChunkedFlowWriter] Merged " + allNodes.size()
                + " chunks into " + flowJson.toAbsolutePath());

        return result;
    }

    public void writeFinalResult(FlowResult result) throws IOException {
        Path flowJson = buildDir.resolve("flow.json");
        mapper.writeValue(flowJson.toFile(), result);
        System.err.println("[ChunkedFlowWriter] Final flow.json written with metrics: "
                + flowJson.toAbsolutePath());

        writeApiFiles(result);
    }

    // ═══════════════════════════════════════════════════════════════
    // API-ready output: index.json, nodes/{id}.json, tables/, call_graph.json
    // Designed for on-demand loading by a UI
    // ═══════════════════════════════════════════════════════════════

    @SuppressWarnings("unchecked")
    private void writeApiFiles(FlowResult result) throws IOException {
        Path apiDir = buildDir.resolve("api");
        Path nodesDir = apiDir.resolve("nodes");
        Files.createDirectories(nodesDir);

        List<FlowNode> nodes = result.getFlowNodes();
        List<FlowEdge> edges = result.getCallGraph();

        // Deduplicate short IDs: if two nodes share SCHEMA$PKG$OBJ, append $2, $3...
        Map<String, Integer> shortIdCount = new LinkedHashMap<>();
        for (FlowNode n : nodes) {
            String shortId = safeNodeId(n);
            shortIdCount.merge(shortId, 1, Integer::sum);
        }
        Map<String, Integer> shortIdSeq = new HashMap<>();
        for (FlowNode n : nodes) {
            String shortId = safeNodeId(n);
            if (shortIdCount.get(shortId) > 1) {
                int seq = shortIdSeq.merge(shortId, 1, Integer::sum);
                if (seq > 1) n.setNodeId(shortId + "$" + seq);
            }
        }

        // Build nodeId -> FlowNode index for lookups
        Map<String, FlowNode> nodeIndex = new LinkedHashMap<>();
        for (FlowNode n : nodes) {
            String nid = safeNodeId(n);
            nodeIndex.put(nid, n);
        }

        // Build parent map from edges: nodeId -> set of callerNodeIds
        Map<String, String> firstCallerMap = new LinkedHashMap<>();
        // Build adjacency map: parentNodeId -> set of childNodeIds (for subtree computation)
        Map<String, Set<String>> childrenMap = new LinkedHashMap<>();
        for (FlowEdge e : edges) {
            String fromPartialId = e.getFromNodeId();
            String toPartialId = e.getToNodeId();
            if (toPartialId == null) continue;

            // Resolve both IDs using prefix matching (edges have partial IDs without params)
            String fromId = fromPartialId != null ? findMatchingNodeId(fromPartialId, nodeIndex) : null;
            String toId = findMatchingNodeId(toPartialId, nodeIndex);
            if (toId == null) continue;

            if (!firstCallerMap.containsKey(toId) && fromId != null) {
                firstCallerMap.put(toId, fromId);
            }

            // Populate children adjacency
            if (fromId != null) {
                childrenMap.computeIfAbsent(fromId, k -> new LinkedHashSet<>()).add(toId);
            }
        }

        // Precompute subtree descendants for each node (DFS)
        Map<String, Set<String>> subtreeCache = new LinkedHashMap<>();
        for (String nodeId : nodeIndex.keySet()) {
            if (!subtreeCache.containsKey(nodeId)) {
                computeSubtreeDescendants(nodeId, childrenMap, subtreeCache);
            }
        }

        // ── 1. index.json: lightweight tree for sidebar/tree view ──
        List<Map<String, Object>> indexNodes = new ArrayList<>();
        for (FlowNode node : nodes) {
            Map<String, Object> idx = new LinkedHashMap<>();
            String nodeId = safeNodeId(node);
            String pkg = node.getPackageName() != null ? node.getPackageName() : "";
            String obj = node.getObjectName() != null ? node.getObjectName() : "";
            String fullName = pkg.isEmpty() ? obj : pkg + "." + obj;

            idx.put("nodeId", nodeId);
            idx.put("order", node.getOrder());
            idx.put("depth", node.getDepth());
            idx.put("schema", node.getSchema());
            idx.put("packageName", pkg.isEmpty() ? null : pkg);
            idx.put("objectName", obj);
            idx.put("name", fullName);
            idx.put("objectType", node.getObjectType());
            idx.put("lineStart", node.getLineStart());
            idx.put("lineEnd", node.getLineEnd());
            idx.put("linesOfCode", node.getLinesOfCode());
            idx.put("readable", node.isReadable());
            if (node.getMessage() != null) idx.put("message", node.getMessage());
            idx.put("parentNodeId", firstCallerMap.get(nodeId));
            idx.put("callCount", node.getCallCount());

            // Counts only — details in nodes/{id}.json
            int tableCount = node.getTableOperations() != null ? countDistinct(node.getTableOperations()) : 0;
            int callCountOut = 0;
            int internalCallCount = 0;
            int externalCallCount = 0;
            if (node.getCalls() != null) {
                Set<String> seen = new HashSet<>();
                for (CallInfo c : node.getCalls()) {
                    if ("BUILTIN".equalsIgnoreCase(c.getType())) continue;
                    String key = (c.getPackageName() != null ? c.getPackageName() + "." : "") + c.getName();
                    if (seen.add(key)) {
                        callCountOut++;
                        if ("INTERNAL".equalsIgnoreCase(c.getType())) internalCallCount++;
                        else externalCallCount++;
                    }
                }
            }
            int varCount = node.getLocalVariables() != null ? node.getLocalVariables().size() : 0;
            int cursorCount = node.getCursors() != null ? node.getCursors().size() : 0;
            int paramCount = node.getParameters() != null ? node.getParameters().size() : 0;
            int exHandlerCount = node.getExceptionHandlers() != null ? node.getExceptionHandlers().size() : 0;
            int sequenceCount = 0;
            if (node.getCalls() != null) {
                Set<String> seqNames = new HashSet<>();
                for (CallInfo c : node.getCalls()) {
                    String cName = c.getName();
                    if (cName != null && ("NEXTVAL".equals(cName) || "CURRVAL".equals(cName))
                            && c.getPackageName() != null) {
                        seqNames.add(c.getPackageName());
                    }
                }
                sequenceCount = seqNames.size();
            }

            Map<String, Object> counts = new LinkedHashMap<>();
            counts.put("tables", tableCount);
            counts.put("callsOut", callCountOut);
            counts.put("internalCalls", internalCallCount);
            counts.put("externalCalls", externalCallCount);
            counts.put("variables", varCount);
            counts.put("cursors", cursorCount);
            counts.put("parameters", paramCount);
            counts.put("exceptionHandlers", exHandlerCount);
            counts.put("sequences", sequenceCount);
            counts.put("statements", node.getStatementSummary());

            // Subtree counts
            Set<String> descendants = subtreeCache.getOrDefault(nodeId, Collections.emptySet());
            Set<String> subtreeTableNames = new HashSet<>();
            // Count tables from this node
            if (node.getTableOperations() != null) {
                for (TableOperationInfo t : node.getTableOperations()) {
                    subtreeTableNames.add((t.getSchema() != null ? t.getSchema() + "." : "") + t.getTableName());
                }
            }
            // Count tables from all descendants
            for (String descId : descendants) {
                FlowNode descNode = nodeIndex.get(descId);
                if (descNode != null && descNode.getTableOperations() != null) {
                    for (TableOperationInfo t : descNode.getTableOperations()) {
                        subtreeTableNames.add((t.getSchema() != null ? t.getSchema() + "." : "") + t.getTableName());
                    }
                }
            }
            counts.put("subtreeTablesCount", subtreeTableNames.size());
            counts.put("subtreeNodesCount", descendants.size());

            // Flow LOC: this node's LOC + all descendant LOC (full execution path)
            int flowLoc = node.getLinesOfCode();
            for (String descId : descendants) {
                FlowNode descNode = nodeIndex.get(descId);
                if (descNode != null) flowLoc += descNode.getLinesOfCode();
            }
            counts.put("flowLinesOfCode", flowLoc);

            idx.put("counts", counts);

            // File references for on-demand loading
            String sourceFile = buildSourceFileName(node);
            if (sourceFile != null) idx.put("sourceFile", sourceFile);
            idx.put("detailFile", "nodes/" + safeFileName(nodeId) + ".json");

            indexNodes.add(idx);
        }

        // Compute total flow LOC from entry point's subtree
        int totalFlowLoc = 0;
        if (!nodes.isEmpty()) {
            String entryNodeId = safeNodeId(nodes.get(0));
            Set<String> entryDesc = subtreeCache.getOrDefault(entryNodeId, Collections.emptySet());
            totalFlowLoc = nodes.get(0).getLinesOfCode();
            for (String descId : entryDesc) {
                FlowNode descNode = nodeIndex.get(descId);
                if (descNode != null) totalFlowLoc += descNode.getLinesOfCode();
            }
        }

        // Detect cycles in call graph
        List<List<String>> cycles = detectCycles(childrenMap, nodeIndex);

        // Build set of nodes involved in cycles for per-node marking
        Set<String> nodesInCycles = new LinkedHashSet<>();
        for (List<String> cycle : cycles) {
            nodesInCycles.addAll(cycle);
        }

        // Mark each index node with cycle involvement
        for (Map<String, Object> idx : indexNodes) {
            String nid = (String) idx.get("nodeId");
            @SuppressWarnings("unchecked")
            Map<String, Object> counts = (Map<String, Object>) idx.get("counts");
            counts.put("inCycle", nodesInCycles.contains(nid));
        }

        Map<String, Object> indexRoot = new LinkedHashMap<>();
        indexRoot.put("entryPoint", result.getEntryPoint());
        indexRoot.put("entrySchema", result.getEntrySchema());
        indexRoot.put("totalNodes", nodes.size());
        indexRoot.put("maxDepth", result.getMaxDepthReached());
        indexRoot.put("totalLinesOfCode", result.getTotalLinesOfCode());
        indexRoot.put("totalFlowLinesOfCode", totalFlowLoc);
        indexRoot.put("totalTables", result.getAllTables() != null ? result.getAllTables().size() : 0);
        indexRoot.put("totalEdges", edges.size());
        indexRoot.put("totalCycles", cycles.size());
        if (!cycles.isEmpty()) {
            List<Map<String, Object>> cycleSummaries = new ArrayList<>();
            for (List<String> cycle : cycles) {
                Map<String, Object> cs = new LinkedHashMap<>();
                List<String> readableNames = new ArrayList<>();
                for (String cid : cycle) {
                    FlowNode fn = nodeIndex.get(cid);
                    if (fn != null) {
                        String p = fn.getPackageName() != null ? fn.getPackageName() : "";
                        String o = fn.getObjectName() != null ? fn.getObjectName() : "";
                        readableNames.add(p.isEmpty() ? o : p + "." + o);
                    } else {
                        readableNames.add(cid);
                    }
                }
                cs.put("nodeIds", cycle);
                cs.put("path", readableNames);
                cs.put("length", cycle.size());
                cycleSummaries.add(cs);
            }
            indexRoot.put("cycles", cycleSummaries);
        }
        indexRoot.put("errors", result.getErrors());
        indexRoot.put("crawlTimeMs", result.getCrawlTimeMs());
        indexRoot.put("dbCallCount", result.getDbCallCount());
        indexRoot.put("nodes", indexNodes);

        mapper.writeValue(apiDir.resolve("index.json").toFile(), indexRoot);

        // ── 2. nodes/{nodeId}.json: full detail per node (on-demand) ──
        for (FlowNode node : nodes) {
            String nodeId = safeNodeId(node);
            Map<String, Object> detail = new LinkedHashMap<>();

            detail.put("nodeId", nodeId);
            String pkg = node.getPackageName() != null ? node.getPackageName() : "";
            String obj = node.getObjectName() != null ? node.getObjectName() : "";
            detail.put("name", pkg.isEmpty() ? obj : pkg + "." + obj);
            detail.put("schema", node.getSchema());
            detail.put("objectType", node.getObjectType());
            detail.put("depth", node.getDepth());
            detail.put("lineStart", node.getLineStart());
            detail.put("lineEnd", node.getLineEnd());
            detail.put("linesOfCode", node.getLinesOfCode());

            // Parameters
            if (node.getParameters() != null && !node.getParameters().isEmpty()) {
                List<Map<String, Object>> params = new ArrayList<>();
                for (ParameterInfo p : node.getParameters()) {
                    Map<String, Object> pm = new LinkedHashMap<>();
                    pm.put("name", p.getName());
                    pm.put("dataType", p.getDataType());
                    pm.put("direction", p.getDirection());
                    if (p.getDefaultValue() != null) pm.put("default", p.getDefaultValue());
                    pm.put("line", p.getLine());
                    params.add(pm);
                }
                detail.put("parameters", params);
            }

            // Variables — categorized
            if (node.getLocalVariables() != null && !node.getLocalVariables().isEmpty()) {
                detail.put("variables", categorizeVariables(node.getLocalVariables()));
            }

            // Tables — grouped by name with all operations and lines
            if (node.getTableOperations() != null && !node.getTableOperations().isEmpty()) {
                detail.put("tables", groupTableOps(node.getTableOperations()));
                detail.put("directTables", groupTableOps(node.getTableOperations()));
            }

            // Subtree tables — aggregate from this node + all descendants
            {
                Set<String> descendants = subtreeCache.getOrDefault(nodeId, Collections.emptySet());
                detail.put("subtreeTables", buildSubtreeTables(nodeId, node, descendants, nodeIndex));
            }

            // Cursors — enrich with OPEN/FETCH/CLOSE operations from statements
            if (node.getCursors() != null && !node.getCursors().isEmpty()) {
                // Build cursor name -> operations map from statements
                Map<String, List<Map<String, Object>>> cursorOpsFromStmts = new LinkedHashMap<>();
                if (node.getStatements() != null) {
                    for (StatementInfo s : node.getStatements()) {
                        String sType = s.getType();
                        if (sType == null) continue;
                        String cursorName = null;
                        String opType = null;
                        if ("OPEN_CURSOR".equals(sType) || "OPEN_FOR".equals(sType)) {
                            opType = "OPEN";
                            cursorName = extractCursorNameFromSql(s.getSqlText());
                        } else if ("FETCH".equals(sType) || "BULK_COLLECT".equals(sType)) {
                            opType = sType;
                            cursorName = extractCursorNameFromSql(s.getSqlText());
                        } else if ("CLOSE_CURSOR".equals(sType)) {
                            opType = "CLOSE";
                            cursorName = extractCursorNameFromSql(s.getSqlText());
                        }
                        if (cursorName != null && opType != null) {
                            Map<String, Object> opInfo = new LinkedHashMap<>();
                            opInfo.put("operation", opType);
                            opInfo.put("line", s.getLine());
                            cursorOpsFromStmts.computeIfAbsent(cursorName.toUpperCase(), k -> new ArrayList<>()).add(opInfo);
                        }
                    }
                }

                List<Map<String, Object>> cursors = new ArrayList<>();
                for (CursorInfo c : node.getCursors()) {
                    Map<String, Object> ci = new LinkedHashMap<>();
                    ci.put("name", c.getName());
                    ci.put("line", c.getLine());
                    ci.put("lineEnd", c.getLineEnd());
                    ci.put("forLoop", c.isForLoop());
                    ci.put("refCursor", c.isRefCursor());
                    if (c.getQuery() != null) ci.put("query", c.getQuery());
                    if (c.getTables() != null) ci.put("tables", c.getTables());
                    // Attach operations from statements
                    String cName = c.getName() != null ? c.getName().toUpperCase() : "";
                    List<Map<String, Object>> stmtOps = cursorOpsFromStmts.get(cName);
                    if (stmtOps != null && !stmtOps.isEmpty()) {
                        ci.put("stmtOperations", stmtOps);
                    }
                    cursors.add(ci);
                }
                detail.put("cursors", cursors);
            }

            // Sequences — extracted from calls with NEXTVAL/CURRVAL
            if (node.getCalls() != null && !node.getCalls().isEmpty()) {
                List<Map<String, Object>> seqList = extractSequences(node.getCalls());
                if (!seqList.isEmpty()) {
                    detail.put("sequences", seqList);
                }
            }

            // Calls — grouped by target (skip builtins)
            if (node.getCalls() != null && !node.getCalls().isEmpty()) {
                detail.put("calls", groupCalls(node.getCalls(), node.getPackageName(), node.getSchema()));
            }

            // Dynamic SQL
            if (node.getDynamicSql() != null && !node.getDynamicSql().isEmpty()) {
                List<Map<String, Object>> dynSql = new ArrayList<>();
                for (DynamicSqlInfo ds : node.getDynamicSql()) {
                    Map<String, Object> dm = new LinkedHashMap<>();
                    dm.put("type", ds.getType());
                    dm.put("line", ds.getLine());
                    dm.put("sqlExpression", ds.getSqlExpression());
                    if (ds.getUsingVariables() != null && !ds.getUsingVariables().isEmpty())
                        dm.put("usingVariables", ds.getUsingVariables());
                    dynSql.add(dm);
                }
                detail.put("dynamicSql", dynSql);
            }

            // Exception handlers
            if (node.getExceptionHandlers() != null && !node.getExceptionHandlers().isEmpty()) {
                List<Map<String, Object>> exHandlers = new ArrayList<>();
                for (ExceptionHandlerInfo eh : node.getExceptionHandlers()) {
                    Map<String, Object> em = new LinkedHashMap<>();
                    if (eh.getExceptionName() != null) em.put("exception", eh.getExceptionName());
                    em.put("line", eh.getLine());
                    em.put("lineEnd", eh.getLineEnd());
                    em.put("statementsCount", eh.getStatementsCount());
                    exHandlers.add(em);
                }
                detail.put("exceptionHandlers", exHandlers);
            }

            // Statement summary
            detail.put("statementSummary", node.getStatementSummary());

            // External package var refs
            if (node.getExternalPackageVarRefs() != null && !node.getExternalPackageVarRefs().isEmpty()) {
                detail.put("externalPackageVarRefs", node.getExternalPackageVarRefs());
            }

            // Source file ref
            String sourceFile = buildSourceFileName(node);
            if (sourceFile != null) detail.put("sourceFile", sourceFile);

            String safeId = safeFileName(nodeId);
            mapper.writeValue(nodesDir.resolve(safeId + ".json").toFile(), detail);
        }

        // ── 3. tables/index.json: all tables, who uses them, which operations ──
        writeTableIndex(apiDir, nodes);

        // ── 4. call_graph.json: deduplicated edges for graph rendering ──
        writeCallGraph(apiDir, edges, cycles, nodeIndex);

        // ── 5. node_id_map.json: shortId -> fullId lookup for resolving edge references ──
        writeNodeIdMap(apiDir, nodeIndex);

        System.err.println("[ChunkedFlowWriter] API files written: " + apiDir.toAbsolutePath());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> categorizeVariables(List<VariableInfo> vars) {
        List<Map<String, Object>> typeRefs = new ArrayList<>();
        List<Map<String, Object>> rowtypeRefs = new ArrayList<>();
        List<Map<String, Object>> collections = new ArrayList<>();
        List<Map<String, Object>> plain = new ArrayList<>();

        for (VariableInfo v : vars) {
            String dt = v.getDataType() != null ? v.getDataType().toUpperCase() : "";
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("name", v.getName());
            info.put("dataType", v.getDataType());
            if (v.getDefaultValue() != null) info.put("default", v.getDefaultValue());
            info.put("line", v.getLine());
            info.put("constant", v.isConstant());

            if (dt.contains("%ROWTYPE")) {
                info.put("refTable", dt.replace("%ROWTYPE", "").trim());
                rowtypeRefs.add(info);
            } else if (dt.contains("%TYPE")) {
                String ref = dt.replace("%TYPE", "").trim();
                info.put("refColumn", ref);
                if (ref.contains(".")) {
                    String[] parts = ref.split("\\.", 2);
                    info.put("refTable", parts[0]);
                    info.put("refField", parts[1]);
                }
                typeRefs.add(info);
            } else if (dt.contains("TABLE OF") || dt.contains("VARRAY")
                    || dt.contains("ASSOCIATIVE") || dt.contains("INDEX BY")
                    || dt.contains("NESTED TABLE")) {
                collections.add(info);
            } else {
                plain.add(info);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", vars.size());
        result.put("typeRefCount", typeRefs.size());
        result.put("rowtypeRefCount", rowtypeRefs.size());
        result.put("collectionCount", collections.size());
        result.put("plainCount", plain.size());
        if (!typeRefs.isEmpty()) result.put("typeRefs", typeRefs);
        if (!rowtypeRefs.isEmpty()) result.put("rowtypeRefs", rowtypeRefs);
        if (!collections.isEmpty()) result.put("collections", collections);
        if (!plain.isEmpty()) result.put("plain", plain);
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> groupTableOps(List<TableOperationInfo> ops) {
        Map<String, Map<String, Object>> grouped = new LinkedHashMap<>();
        for (TableOperationInfo top : ops) {
            String key = (top.getSchema() != null ? top.getSchema() + "." : "") + top.getTableName();
            Map<String, Object> info = grouped.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("schema", top.getSchema());
                m.put("name", top.getTableName());
                m.put("objectType", top.getObjectType() != null ? top.getObjectType() : "TABLE");
                m.put("operations", new LinkedHashMap<String, List<Integer>>());
                m.put("joins", new ArrayList<Map<String, Object>>());
                return m;
            });
            Map<String, List<Integer>> opsMap = (Map<String, List<Integer>>) info.get("operations");
            opsMap.computeIfAbsent(top.getOperation(), k -> new ArrayList<>()).add(top.getLine());

            if (top.getJoins() != null) {
                List<Map<String, Object>> joinsList = (List<Map<String, Object>>) info.get("joins");
                for (JoinInfo j : top.getJoins()) {
                    Map<String, Object> jm = new LinkedHashMap<>();
                    jm.put("joinType", j.getJoinType());
                    jm.put("joinedTable", j.getJoinedTable());
                    jm.put("joinedTableAlias", j.getJoinedTableAlias());
                    jm.put("condition", j.getCondition());
                    jm.put("line", j.getLine());
                    joinsList.add(jm);
                }
            }
        }
        return new ArrayList<>(grouped.values());
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> groupCalls(List<CallInfo> calls,
                                                  String nodePackageName, String nodeSchema) {
        Map<String, Map<String, Object>> grouped = new LinkedHashMap<>();
        for (CallInfo c : calls) {
            String callType = c.getType() != null ? c.getType() : "UNKNOWN";
            if ("BUILTIN".equalsIgnoreCase(callType)) continue;

            String cPkg = c.getPackageName() != null ? c.getPackageName() : "";
            String cName = c.getName() != null ? c.getName() : "";

            // INTERNAL calls without packageName inherit the node's package
            if (cPkg.isEmpty() && "INTERNAL".equalsIgnoreCase(callType)
                    && nodePackageName != null && !nodePackageName.isEmpty()) {
                cPkg = nodePackageName;
            }

            String schema = c.getSchema();
            if ((schema == null || schema.isEmpty()) && nodeSchema != null) {
                schema = nodeSchema;
            }

            String target = cPkg.isEmpty() ? cName : cPkg + "." + cName;
            String finalPkg = cPkg;
            String finalSchema = schema;

            Map<String, Object> info = grouped.computeIfAbsent(target, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("target", target);
                m.put("packageName", finalPkg.isEmpty() ? null : finalPkg);
                m.put("objectName", cName);
                m.put("type", callType);
                m.put("schema", finalSchema);
                m.put("lines", new ArrayList<Integer>());
                m.put("count", 0);
                return m;
            });
            ((List<Integer>) info.get("lines")).add(c.getLine());
            info.put("count", (int) info.get("count") + 1);
        }
        return new ArrayList<>(grouped.values());
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractSequences(List<CallInfo> calls) {
        Map<String, Map<String, Object>> grouped = new LinkedHashMap<>();
        for (CallInfo c : calls) {
            String name = c.getName();
            if (name == null) continue;
            if (!"NEXTVAL".equals(name) && !"CURRVAL".equals(name)) continue;
            String seqName = c.getPackageName();
            if (seqName == null || seqName.isEmpty()) continue;

            Map<String, Object> info = grouped.computeIfAbsent(seqName, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", seqName);
                m.put("schema", c.getSchema());
                m.put("operations", new LinkedHashMap<String, List<Integer>>());
                return m;
            });
            Map<String, List<Integer>> opsMap = (Map<String, List<Integer>>) info.get("operations");
            opsMap.computeIfAbsent(name, k -> new ArrayList<>()).add(c.getLine());
        }
        return new ArrayList<>(grouped.values());
    }

    @SuppressWarnings("unchecked")
    private void writeTableIndex(Path apiDir, List<FlowNode> nodes) throws IOException {
        Path tablesDir = apiDir.resolve("tables");
        Files.createDirectories(tablesDir);

        Map<String, String> tableObjectTypes = new LinkedHashMap<>();
        if (Files.exists(tablesFile)) {
            try {
                Map<String, SchemaTableInfo> tableMap = mapper.readValue(tablesFile.toFile(),
                        mapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, SchemaTableInfo.class));
                for (var e : tableMap.entrySet()) {
                    if (e.getValue().getObjectType() != null) tableObjectTypes.put(e.getKey(), e.getValue().getObjectType());
                }
            } catch (Exception ignored) {}
        }

        // Global table map: tableName -> { schema, name, type, usedBy: [{nodeId, nodeName, operations}] }
        Map<String, Map<String, Object>> globalTables = new LinkedHashMap<>();

        for (FlowNode node : nodes) {
            if (node.getTableOperations() == null) continue;
            String nodeId = safeNodeId(node);
            String pkg = node.getPackageName() != null ? node.getPackageName() : "";
            String obj = node.getObjectName() != null ? node.getObjectName() : "";
            String nodeName = pkg.isEmpty() ? obj : pkg + "." + obj;

            // Group this node's table ops by table name
            Map<String, Map<String, List<Integer>>> nodeTableOps = new LinkedHashMap<>();
            for (TableOperationInfo top : node.getTableOperations()) {
                String key = (top.getSchema() != null ? top.getSchema() + "." : "") + top.getTableName();
                nodeTableOps.computeIfAbsent(key, k -> new LinkedHashMap<>())
                        .computeIfAbsent(top.getOperation(), k -> new ArrayList<>())
                        .add(top.getLine());
            }

            for (var entry : nodeTableOps.entrySet()) {
                String tableKey = entry.getKey();
                Map<String, List<Integer>> opsForNode = entry.getValue();

                Map<String, Object> tableInfo = globalTables.computeIfAbsent(tableKey, k -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    String[] p = tableKey.split("\\.", 2);
                    if (p.length == 2) {
                        m.put("schema", p[0]);
                        m.put("name", p[1]);
                    } else {
                        m.put("name", tableKey);
                    }
                    String ot = tableObjectTypes.get(tableKey);
                    m.put("objectType", ot != null ? ot : "TABLE");
                    m.put("allOperations", new LinkedHashSet<String>());
                    m.put("usedBy", new ArrayList<Map<String, Object>>());
                    return m;
                });

                Set<String> allOps = (Set<String>) tableInfo.get("allOperations");
                allOps.addAll(opsForNode.keySet());

                Map<String, Object> usage = new LinkedHashMap<>();
                usage.put("nodeId", nodeId);
                usage.put("nodeName", nodeName);
                usage.put("nodeDepth", node.getDepth());
                usage.put("operations", opsForNode);
                ((List<Map<String, Object>>) tableInfo.get("usedBy")).add(usage);
            }

            // Also pick up tables from cursors
            if (node.getCursors() != null) {
                for (CursorInfo cursor : node.getCursors()) {
                    if (cursor.getTables() == null) continue;
                    for (String cursorTable : cursor.getTables()) {
                        String ctKey = cursorTable.toUpperCase();
                        // Check if already in globalTables with schema prefix
                        boolean found = false;
                        for (String gKey : globalTables.keySet()) {
                            if (gKey.endsWith("." + ctKey)) { found = true; break; }
                        }
                        if (!found && !globalTables.containsKey(ctKey)) {
                            Map<String, Object> m = new LinkedHashMap<>();
                            m.put("name", ctKey);
                            m.put("allOperations", new LinkedHashSet<>(Set.of("CURSOR_REF")));
                            List<Map<String, Object>> usedBy = new ArrayList<>();
                            Map<String, Object> usage = new LinkedHashMap<>();
                            usage.put("nodeId", nodeId);
                            usage.put("nodeName", nodeName);
                            usage.put("operations", Map.of("CURSOR_REF",
                                    List.of(cursor.getLine())));
                            usedBy.add(usage);
                            m.put("usedBy", usedBy);
                            globalTables.put(ctKey, m);
                        }
                    }
                }
            }
        }

        for (FlowNode node : nodes) {
            if (!"TRIGGER".equalsIgnoreCase(node.getObjectType())) continue;
            String trigTable = node.getTriggerTable();
            if (trigTable == null || trigTable.isEmpty()) continue;
            String trigKey = trigTable.toUpperCase();
            for (var entry : globalTables.entrySet()) {
                String name = (String) entry.getValue().get("name");
                if (name != null && name.equalsIgnoreCase(trigTable) || entry.getKey().equalsIgnoreCase(trigKey)
                        || entry.getKey().endsWith("." + trigKey)) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> triggers = (List<Map<String, Object>>) entry.getValue()
                            .computeIfAbsent("triggers", k -> new ArrayList<Map<String, Object>>());
                    Map<String, Object> trigInfo = new LinkedHashMap<>();
                    trigInfo.put("nodeId", safeNodeId(node));
                    trigInfo.put("name", node.getObjectName());
                    trigInfo.put("schema", node.getSchema());
                    trigInfo.put("event", node.getTriggerEvent());
                    trigInfo.put("timing", node.getTriggerTiming());
                    triggers.add(trigInfo);
                    break;
                }
            }
        }

        // Merge DB-discovered triggers (from ALL_TRIGGERS for DML tables)
        if (discoveredTriggers != null && !discoveredTriggers.isEmpty()) {
            for (Map<String, Object> dbTrig : discoveredTriggers) {
                String trigTableName = ((String) dbTrig.get("tableName"));
                if (trigTableName == null) continue;
                String trigKey = trigTableName.toUpperCase();

                for (var entry : globalTables.entrySet()) {
                    String name = (String) entry.getValue().get("name");
                    if (name != null && (name.equalsIgnoreCase(trigTableName)
                            || entry.getKey().equalsIgnoreCase(trigKey)
                            || entry.getKey().endsWith("." + trigKey))) {

                        List<Map<String, Object>> triggers = (List<Map<String, Object>>) entry.getValue()
                                .computeIfAbsent("triggers", k -> new ArrayList<Map<String, Object>>());

                        // Deduplicate: skip if trigger with same name already attached from source parsing
                        String trigName = (String) dbTrig.get("name");
                        boolean exists = triggers.stream().anyMatch(t ->
                                trigName != null && trigName.equalsIgnoreCase((String) t.get("name")));
                        if (exists) continue;

                        Map<String, Object> trigInfo = new LinkedHashMap<>();
                        trigInfo.put("name", trigName);
                        trigInfo.put("schema", dbTrig.get("schema"));
                        trigInfo.put("event", dbTrig.get("event"));
                        trigInfo.put("timing", dbTrig.get("timing"));
                        trigInfo.put("triggerType", dbTrig.get("triggerType"));
                        trigInfo.put("status", dbTrig.get("status"));
                        if (dbTrig.get("definition") != null) trigInfo.put("definition", dbTrig.get("definition"));
                        if (dbTrig.get("tableOps") != null) trigInfo.put("tableOps", dbTrig.get("tableOps"));
                        if (dbTrig.get("sequences") != null) trigInfo.put("sequences", dbTrig.get("sequences"));
                        trigInfo.put("source", "DATABASE");
                        triggers.add(trigInfo);
                        break;
                    }
                }
            }
        }

        // Convert sets to lists and write
        List<Map<String, Object>> tableList = new ArrayList<>();
        for (Map<String, Object> t : globalTables.values()) {
            Map<String, Object> clean = new LinkedHashMap<>(t);
            clean.put("allOperations", new ArrayList<>((Set<String>) t.get("allOperations")));
            tableList.add(clean);
        }
        tableList.sort(Comparator.comparing(m -> {
            String s = (String) m.get("schema");
            String n = (String) m.get("name");
            return (s != null ? s : "") + "." + (n != null ? n : "");
        }));

        Map<String, Object> tableIndex = new LinkedHashMap<>();
        tableIndex.put("totalTables", tableList.size());

        // Summary: group by operation type
        Map<String, Integer> opSummary = new LinkedHashMap<>();
        for (Map<String, Object> t : tableList) {
            List<String> ops = (List<String>) t.get("allOperations");
            for (String op : ops) opSummary.merge(op, 1, Integer::sum);
        }
        tableIndex.put("operationSummary", opSummary);
        tableIndex.put("tables", tableList);

        mapper.writeValue(tablesDir.resolve("index.json").toFile(), tableIndex);
    }

    private void writeCallGraph(Path apiDir, List<FlowEdge> edges,
                                List<List<String>> cycles,
                                Map<String, FlowNode> nodeIndex) throws IOException {
        // Build set of back-edges from detected cycles (last->first in each cycle)
        Set<String> backEdgeKeys = new HashSet<>();
        for (List<String> cycle : cycles) {
            if (cycle.size() >= 2) {
                String backFrom = cycle.get(cycle.size() - 1);
                String backTo = cycle.get(0);
                backEdgeKeys.add(backFrom + "->" + backTo);
            }
        }

        // Deduplicated edges: from->to with all occurrences
        Map<String, Map<String, Object>> deduped = new LinkedHashMap<>();
        for (FlowEdge e : edges) {
            String from = (e.getFromPackage() != null ? e.getFromPackage() + "." : "") + e.getFromObject();
            String to = (e.getToPackage() != null ? e.getToPackage() + "." : "") + e.getToObject();
            String key = from + " -> " + to;

            @SuppressWarnings("unchecked")
            Map<String, Object> info = deduped.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("fromNodeId", e.getFromNodeId());
                m.put("fromSchema", e.getFromSchema());
                m.put("from", from);
                m.put("toNodeId", e.getToNodeId());
                m.put("toSchema", e.getToSchema());
                m.put("to", to);
                m.put("lines", new ArrayList<Integer>());
                m.put("count", 0);
                m.put("minDepth", e.getDepth());
                m.put("backEdge", false);
                return m;
            });

            @SuppressWarnings("unchecked")
            List<Integer> lines = (List<Integer>) info.get("lines");
            lines.add(e.getLine());
            info.put("count", (int) info.get("count") + 1);
            if (e.getDepth() < (int) info.get("minDepth")) {
                info.put("minDepth", e.getDepth());
            }

            // Check if this edge is a back-edge in any cycle
            String fromNodeId = e.getFromNodeId();
            String toNodeId = e.getToNodeId();
            if (fromNodeId != null && toNodeId != null) {
                // Try exact match and prefix match
                for (String beKey : backEdgeKeys) {
                    String[] parts = beKey.split("->");
                    if (fromNodeId.startsWith(parts[0]) && toNodeId.startsWith(parts[1])) {
                        info.put("backEdge", true);
                        break;
                    }
                }
            }
        }

        int backEdgeCount = 0;
        for (Map<String, Object> info : deduped.values()) {
            if (Boolean.TRUE.equals(info.get("backEdge"))) backEdgeCount++;
        }

        Map<String, Object> graph = new LinkedHashMap<>();
        graph.put("totalEdges", edges.size());
        graph.put("uniqueEdges", deduped.size());
        graph.put("backEdges", backEdgeCount);
        graph.put("edges", new ArrayList<>(deduped.values()));

        mapper.writeValue(apiDir.resolve("call_graph.json").toFile(), graph);
    }

    /**
     * Write node_id_map.json: compact ID <-> full ID (with param details).
     * Compact ID is the main key used everywhere (index, edges, URLs).
     * Full ID has parameter suffixes for complete disambiguation.
     */
    private void writeNodeIdMap(Path apiDir, Map<String, FlowNode> nodeIndex) throws IOException {
        Map<String, String> idToFull = new LinkedHashMap<>();
        Map<String, String> fullToId = new LinkedHashMap<>();

        for (Map.Entry<String, FlowNode> entry : nodeIndex.entrySet()) {
            String compactId = entry.getKey();
            FlowNode node = entry.getValue();
            String fullId = node.buildFullId();

            idToFull.put(compactId, fullId);
            fullToId.put(fullId, compactId);
        }

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("idToFull", idToFull);
        map.put("fullToId", fullToId);

        mapper.writeValue(apiDir.resolve("node_id_map.json").toFile(), map);
    }

    // ═══════════════════════════════════════════════════════════════

    private String findMatchingNodeId(String edgeId, Map<String, FlowNode> nodeIndex) {
        if (nodeIndex.containsKey(edgeId)) return edgeId;
        for (String nid : nodeIndex.keySet()) {
            if (nid.startsWith(edgeId + "$") || nid.startsWith(edgeId + "_")) return nid;
        }
        return null;
    }

    private void computeSubtreeDescendants(String nodeId,
                                            Map<String, Set<String>> childrenMap,
                                            Map<String, Set<String>> subtreeCache) {
        computeSubtreeDescendants(nodeId, childrenMap, subtreeCache, new HashSet<>());
    }

    private void computeSubtreeDescendants(String nodeId,
                                            Map<String, Set<String>> childrenMap,
                                            Map<String, Set<String>> subtreeCache,
                                            Set<String> visiting) {
        if (subtreeCache.containsKey(nodeId)) return;
        if (!visiting.add(nodeId)) return; // cycle detected

        Set<String> descendants = new LinkedHashSet<>();
        Set<String> directChildren = childrenMap.getOrDefault(nodeId, Collections.emptySet());
        for (String child : directChildren) {
            if (visiting.contains(child)) continue; // skip back-edges
            descendants.add(child);
            if (!subtreeCache.containsKey(child)) {
                computeSubtreeDescendants(child, childrenMap, subtreeCache, visiting);
            }
            descendants.addAll(subtreeCache.getOrDefault(child, Collections.emptySet()));
        }
        subtreeCache.put(nodeId, descendants);
        visiting.remove(nodeId);
    }

    private List<List<String>> detectCycles(Map<String, Set<String>> childrenMap,
                                             Map<String, FlowNode> nodeIndex) {
        List<List<String>> cycles = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Set<String> onStack = new LinkedHashSet<>();
        Map<String, String> parentMap = new HashMap<>();

        for (String nodeId : nodeIndex.keySet()) {
            if (!visited.contains(nodeId)) {
                detectCyclesDfs(nodeId, childrenMap, visited, onStack, parentMap, cycles);
            }
        }
        return cycles;
    }

    private void detectCyclesDfs(String nodeId,
                                  Map<String, Set<String>> childrenMap,
                                  Set<String> visited,
                                  Set<String> onStack,
                                  Map<String, String> parentMap,
                                  List<List<String>> cycles) {
        visited.add(nodeId);
        onStack.add(nodeId);

        Set<String> children = childrenMap.getOrDefault(nodeId, Collections.emptySet());
        for (String child : children) {
            if (onStack.contains(child)) {
                // Back-edge found: extract cycle path from child -> ... -> nodeId -> child
                List<String> cycle = new ArrayList<>();
                cycle.add(child);
                String cur = nodeId;
                while (cur != null && !cur.equals(child)) {
                    cycle.add(1, cur);
                    cur = parentMap.get(cur);
                }
                cycles.add(cycle);
            } else if (!visited.contains(child)) {
                parentMap.put(child, nodeId);
                detectCyclesDfs(child, childrenMap, visited, onStack, parentMap, cycles);
            }
        }
        onStack.remove(nodeId);
    }

    /**
     * Build the subtreeTables list for a node by aggregating table operations
     * from this node and all its descendants.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> buildSubtreeTables(String nodeId, FlowNode node,
                                                          Set<String> descendants,
                                                          Map<String, FlowNode> nodeIndex) {
        // Collect all nodes in subtree (this node + descendants)
        List<String> subtreeNodeIds = new ArrayList<>();
        subtreeNodeIds.add(nodeId);
        subtreeNodeIds.addAll(descendants);

        // tableKey -> aggregated info
        Map<String, Map<String, Object>> tableMap = new LinkedHashMap<>();

        for (String nid : subtreeNodeIds) {
            FlowNode n = nodeIndex.get(nid);
            if (n == null || n.getTableOperations() == null || n.getTableOperations().isEmpty()) continue;

            String pkg = n.getPackageName() != null ? n.getPackageName() : "";
            String obj = n.getObjectName() != null ? n.getObjectName() : "";
            String nodeName = pkg.isEmpty() ? obj : pkg + "." + obj;
            boolean isDirect = nid.equals(nodeId);

            // Group this node's ops by table
            Map<String, List<TableOperationInfo>> opsByTable = new LinkedHashMap<>();
            for (TableOperationInfo top : n.getTableOperations()) {
                String key = (top.getSchema() != null ? top.getSchema() + "." : "") + top.getTableName();
                opsByTable.computeIfAbsent(key, k -> new ArrayList<>()).add(top);
            }

            for (var entry : opsByTable.entrySet()) {
                String tableKey = entry.getKey();
                List<TableOperationInfo> ops = entry.getValue();

                String firstObjType = ops.isEmpty() ? "TABLE"
                        : (ops.get(0).getObjectType() != null ? ops.get(0).getObjectType() : "TABLE");
                Map<String, Object> tableInfo = tableMap.computeIfAbsent(tableKey, k -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    String[] parts = tableKey.split("\\.", 2);
                    if (parts.length == 2) {
                        m.put("schema", parts[0]);
                        m.put("name", parts[1]);
                    } else {
                        m.put("name", tableKey);
                    }
                    m.put("objectType", firstObjType);
                    m.put("totalUsageCount", 0);
                    m.put("directUsageCount", 0);
                    m.put("operations", new LinkedHashMap<String, List<Integer>>());
                    m.put("usedByNodes", new ArrayList<Map<String, Object>>());
                    return m;
                });

                int opsCount = ops.size();
                tableInfo.put("totalUsageCount", (int) tableInfo.get("totalUsageCount") + opsCount);
                if (isDirect) {
                    tableInfo.put("directUsageCount", (int) tableInfo.get("directUsageCount") + opsCount);
                }

                // Aggregate operations across all nodes in the subtree
                Map<String, List<Integer>> opsMap = (Map<String, List<Integer>>) tableInfo.get("operations");
                Map<String, List<Integer>> nodeOpsMap = new LinkedHashMap<>();
                for (TableOperationInfo top : ops) {
                    opsMap.computeIfAbsent(top.getOperation(), o -> new ArrayList<>()).add(top.getLine());
                    nodeOpsMap.computeIfAbsent(top.getOperation(), o -> new ArrayList<>()).add(top.getLine());
                }

                // Add to usedByNodes
                Map<String, Object> nodeUsage = new LinkedHashMap<>();
                nodeUsage.put("nodeId", nid);
                nodeUsage.put("nodeName", nodeName);
                nodeUsage.put("depth", n.getDepth());
                nodeUsage.put("operations", nodeOpsMap);
                ((List<Map<String, Object>>) tableInfo.get("usedByNodes")).add(nodeUsage);
            }
        }

        return new ArrayList<>(tableMap.values());
    }

    public void writeResolverData(SchemaResolver resolver) throws IOException {
        Path resolverDir = buildDir.resolve("resolver");
        Files.createDirectories(resolverDir);

        mapper.writeValue(resolverDir.resolve("q1_object_resolution.json").toFile(),
                resolver.exportObjectResolution());

        mapper.writeValue(resolverDir.resolve("q4_direct_dependencies.json").toFile(),
                resolver.exportDirectDependencies());

        mapper.writeValue(resolverDir.resolve("q3_reverse_dependencies.json").toFile(),
                resolver.exportReverseDependencies());

        mapper.writeValue(resolverDir.resolve("q1_ambiguous_objects.json").toFile(),
                resolver.exportAmbiguousObjects());

        System.err.println("[ChunkedFlowWriter] Resolver data written to: " + resolverDir.toAbsolutePath());
    }

    private void writeProgress() throws IOException {
        mapper.writeValue(progressFile.toFile(), progressMap);
    }

    private String safeNodeId(FlowNode node) {
        if (node.getNodeId() != null && !node.getNodeId().isEmpty()) return node.getNodeId();
        StringBuilder sb = new StringBuilder();
        if (node.getSchema() != null) sb.append(node.getSchema()).append("$");
        if (node.getPackageName() != null) sb.append(node.getPackageName()).append("$");
        if (node.getObjectName() != null) sb.append(node.getObjectName());
        return sb.toString().toUpperCase();
    }

    private String buildSourceFileName(FlowNode node) {
        String schema = node.getSchema() != null ? node.getSchema() : "UNKNOWN";
        String obj = node.getObjectName() != null ? node.getObjectName() : "UNKNOWN";
        String pkg = node.getPackageName();
        if (pkg != null && !pkg.isEmpty()) {
            return schema + "." + pkg + "." + obj + ".sql";
        }
        return schema + "." + obj + ".sql";
    }

    private String safeFileName(String nodeId) {
        String safe = nodeId.replaceAll("[^A-Za-z0-9_$]", "_");
        if (safe.length() <= 150) return safe;
        // Truncate and append short hash to keep unique
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(safe.getBytes());
            String hex = String.format("%02x%02x%02x%02x", hash[0], hash[1], hash[2], hash[3]);
            return safe.substring(0, 140) + "_" + hex;
        } catch (Exception e) {
            return safe.substring(0, 150);
        }
    }

    private String extractCursorNameFromSql(String sqlText) {
        if (sqlText == null || sqlText.isEmpty()) return null;
        // Statement sqlText typically starts with the cursor name, e.g. "MY_CURSOR" or "OPEN MY_CURSOR" or "FETCH MY_CURSOR"
        String trimmed = sqlText.trim().toUpperCase();
        // Strip common prefixes
        for (String prefix : new String[]{"OPEN ", "FETCH ", "CLOSE ", "BULK COLLECT "}) {
            if (trimmed.startsWith(prefix)) {
                trimmed = trimmed.substring(prefix.length()).trim();
                break;
            }
        }
        // Take first word as cursor name
        String[] tokens = trimmed.split("[\\s(;,]+", 2);
        if (tokens.length > 0 && !tokens[0].isEmpty()) {
            return tokens[0];
        }
        return null;
    }

    private int countDistinct(List<TableOperationInfo> ops) {
        Set<String> seen = new HashSet<>();
        for (TableOperationInfo t : ops) {
            seen.add((t.getSchema() != null ? t.getSchema() + "." : "") + t.getTableName());
        }
        return seen.size();
    }
}
