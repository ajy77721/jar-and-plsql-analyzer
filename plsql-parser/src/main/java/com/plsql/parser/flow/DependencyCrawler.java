package com.plsql.parser.flow;

import com.plsql.parser.PlSqlParserEngine;
import com.plsql.parser.model.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main orchestrator for recursive dependency analysis with flow-ordered output.
 *
 * Given an entry point (e.g. "PG_AC_EINVOICE.PC_IRBM_SUBMISSION" or "FN_SOME_FUNC"),
 * it downloads the source from the database, parses it, finds external calls,
 * and recursively crawls each dependency using BFS for flow ordering.
 */
public class DependencyCrawler {

    private final PlSqlParserEngine engine;
    private final SourceDownloader downloader;
    private final SchemaResolver schemaResolver;

    private int maxDepth = -1;
    private long timeoutPerEntryMs = 120_000;
    private Path outputDir;

    // Cache of parse results: "OBJECT_NAME" -> ParseResult
    private final Map<String, ParseResult> parseCache = new ConcurrentHashMap<>();

    // Visited set to avoid infinite loops: "PACKAGE.SUBPROGRAM" or "STANDALONE_NAME"
    private final Set<String> visited = Collections.synchronizedSet(new LinkedHashSet<>());

    public DependencyCrawler(PlSqlParserEngine engine, SourceDownloader downloader,
                             SchemaResolver schemaResolver) {
        this.engine = engine;
        this.downloader = downloader;
        this.schemaResolver = schemaResolver;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public void setTimeoutPerEntryMs(long timeoutPerEntryMs) {
        this.timeoutPerEntryMs = timeoutPerEntryMs;
    }

    public void setOutputDir(Path outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * Crawl from the given entry point and produce a FlowResult.
     *
     * @param entryPoint e.g. "PG_AC_EINVOICE.PC_IRBM_SUBMISSION" or "FN_SOME_FUNC"
     * @return FlowResult with complete call chain, schema-resolved references
     */
    public FlowResult crawl(String entryPoint) {
        long startTime = System.currentTimeMillis();
        String upperEntry = entryPoint.toUpperCase().trim();
        String entrySchema = null;

        // Reset state
        visited.clear();
        List<String> errors = new ArrayList<>();
        Map<String, SchemaTableInfo> tableInfoMap = new LinkedHashMap<>();
        List<FlowEdge> edgeBatch = new ArrayList<>();
        int maxDepthReached = 0;
        int orderCounter = 0;

        // Setup chunked writer if output dir is set
        ChunkedFlowWriter chunkedWriter = null;
        if (outputDir != null) {
            try {
                String safeName = upperEntry.replace(".", "_").replace("/", "_").replace("\\", "_");
                Path buildDir = outputDir.resolve(safeName);
                chunkedWriter = new ChunkedFlowWriter(buildDir);
                downloader.setLocalCacheDir(chunkedWriter.getSourcesDir());
                System.err.println("[Crawler] Chunked output: " + buildDir.toAbsolutePath());
            } catch (IOException e) {
                System.err.println("[Crawler] Failed to init chunked writer: " + e.getMessage());
            }
        }

        // Parse entry point
        String packageName = null;
        String subprogramName;
        if (upperEntry.contains(".")) {
            String[] parts = upperEntry.split("\\.", 2);
            packageName = parts[0];
            subprogramName = parts[1];
        } else {
            subprogramName = upperEntry;
        }

        // Pre-warm transitive dependency cache
        String entryObjForDeps = packageName != null ? packageName : subprogramName;
        System.err.println("[Crawler] Pre-computing transitive dependencies for: " + entryObjForDeps);
        Set<String> entryDeps = schemaResolver.getTransitiveDependencies(entryObjForDeps);
        System.err.println("[Crawler] Found " + entryDeps.size() + " transitive dependencies");

        Queue<BfsItem> queue = new LinkedList<>();
        queue.add(new BfsItem(packageName, subprogramName, 0, null, null, 0));

        while (!queue.isEmpty()) {
            BfsItem item = queue.poll();
            String visitKey = buildVisitKey(item.packageName, item.subprogramName);

            if (visited.contains(visitKey)) continue;
            visited.add(visitKey);

            if (maxDepth >= 0 && item.depth > maxDepth) {
                errors.add("Max depth " + maxDepth + " exceeded for: " + visitKey);
                continue;
            }

            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > timeoutPerEntryMs * 10) {
                errors.add("Overall timeout exceeded after " + elapsed + "ms");
                break;
            }

            if (item.depth > maxDepthReached) maxDepthReached = item.depth;

            // Mark progress
            if (chunkedWriter != null) {
                try { chunkedWriter.markProcessing(visitKey); } catch (IOException ignored) {}
            }

            System.err.println("[Crawler] Processing: " + visitKey + " (depth=" + item.depth + ")");

            // Check if this is a TYPE/SYNONYM/SEQUENCE — create stub node, don't try to parse
            String resolvedType = schemaResolver.resolveObjectType(
                    item.packageName != null ? item.packageName : item.subprogramName);
            if (resolvedType != null && isNonParsableType(resolvedType)) {
                String ownerSch = schemaResolver.resolveSchema(
                        item.packageName != null ? item.packageName : item.subprogramName);
                orderCounter++;
                FlowNode stubNode = new FlowNode();
                stubNode.setOrder(orderCounter);
                stubNode.setDepth(item.depth);
                stubNode.setSchema(ownerSch);
                stubNode.setPackageName(item.packageName);
                stubNode.setObjectName(item.subprogramName);
                stubNode.setObjectType(resolvedType);
                stubNode.setReadable(false);
                stubNode.setMessage("Oracle " + resolvedType + " object - no parseable source");
                stubNode.generateNodeId();
                System.err.println("[Crawler] Stub node for " + resolvedType + ": " + visitKey);
                if (chunkedWriter != null) {
                    try {
                        chunkedWriter.writeChunk(stubNode);
                        chunkedWriter.markDone(visitKey);
                    } catch (IOException ignored) {}
                }
                continue;
            }

            try {
                ParseResult parseResult = downloadAndParse(item.packageName, item.subprogramName);
                if (parseResult == null) {
                    // Last chance: check if SourceDownloader found it was a TYPE via DB
                    String srcObj = item.packageName != null ? item.packageName : item.subprogramName;
                    String cachedType = downloader.getCachedType(srcObj);
                    if (cachedType != null && isNonParsableType(cachedType.replace(" ", "_"))) {
                        String ownerSch = downloader.getOwner(srcObj);
                        orderCounter++;
                        FlowNode stubNode = new FlowNode();
                        stubNode.setOrder(orderCounter);
                        stubNode.setDepth(item.depth);
                        stubNode.setSchema(ownerSch);
                        stubNode.setPackageName(item.packageName);
                        stubNode.setObjectName(item.subprogramName);
                        stubNode.setObjectType(cachedType);
                        stubNode.setReadable(false);
                        stubNode.setMessage("Oracle " + cachedType + " object - no parseable source");
                        stubNode.generateNodeId();
                        System.err.println("[Crawler] Stub node for " + cachedType + ": " + visitKey);
                        if (chunkedWriter != null) {
                            try {
                                chunkedWriter.writeChunk(stubNode);
                                chunkedWriter.markDone(visitKey);
                            } catch (IOException ignored) {}
                        }
                        continue;
                    }
                    System.err.println("[Crawler] No source found for: " + visitKey);
                    if (chunkedWriter != null) {
                        try { chunkedWriter.markFailed(visitKey, "no source"); } catch (IOException ignored) {}
                    }
                    continue;
                }

                SubprogramInfo targetSub = null;
                ParsedObject targetObj = null;
                String ownerSchema = null;

                for (ParsedObject obj : parseResult.getObjects()) {
                    String objName = obj.getName() != null ? obj.getName().toUpperCase() : "";
                    if (item.packageName != null) {
                        // Match: exact name, schema-prefixed, or synonym (downloaded source may
                        // have a different package name than the call reference due to synonyms)
                        boolean nameMatch = objName.equalsIgnoreCase(item.packageName)
                                || objName.endsWith("." + item.packageName.toUpperCase());
                        // Also accept if this is the only/first parsed object from the download
                        // (synonym resolved to the real package name)
                        if (!nameMatch && parseResult.getObjects().size() == 1
                                && obj.getType() != null
                                && obj.getType().toUpperCase().contains("PACKAGE")) {
                            nameMatch = true;
                        }
                        if (nameMatch) {
                            targetObj = obj;
                            ownerSchema = resolveObjectSchema(
                                    objName.isEmpty() ? item.packageName : objName,
                                    obj.getSchema());
                            for (SubprogramInfo sub : obj.getSubprograms()) {
                                if (sub.getName() != null
                                        && sub.getName().equalsIgnoreCase(item.subprogramName)) {
                                    targetSub = sub;
                                    break;
                                }
                            }
                            if (targetSub != null) break;
                        }
                    } else {
                        if (objName.equalsIgnoreCase(item.subprogramName)) {
                            targetObj = obj;
                            ownerSchema = resolveObjectSchema(item.subprogramName, obj.getSchema());
                            break;
                        }
                    }
                }

                // Fallback: if subprogram not found in parse, extract from raw source
                if (targetSub == null && item.packageName != null && targetObj != null) {
                    String rawSource = downloader.getCachedSource(item.packageName);
                    if (rawSource != null) {
                        String extracted = extractSubprogramSource(rawSource,
                                item.subprogramName);
                        if (extracted != null) {
                            ParseResult reParsed = engine.parseContent(
                                    "CREATE OR REPLACE PACKAGE BODY " + item.packageName
                                    + " AS\n" + extracted + "\nEND " + item.packageName + ";\n/",
                                    item.packageName + "_" + item.subprogramName + ".sql");
                            if (reParsed != null && !reParsed.getObjects().isEmpty()) {
                                ParsedObject rpObj = reParsed.getObjects().get(0);
                                for (SubprogramInfo sub : rpObj.getSubprograms()) {
                                    if (sub.getName() != null
                                            && sub.getName().equalsIgnoreCase(item.subprogramName)) {
                                        targetSub = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if (item.depth == 0 && ownerSchema != null) {
                    entrySchema = ownerSchema;
                }

                orderCounter++;
                FlowNode node = new FlowNode();
                node.setOrder(orderCounter);
                node.setDepth(item.depth);
                node.setSchema(ownerSchema);
                node.setPackageName(item.packageName);
                node.setObjectName(item.subprogramName);

                // Check if source is wrapped/encrypted
                String srcObjName = item.packageName != null ? item.packageName : item.subprogramName;
                if (downloader.isWrapped(srcObjName)
                        || PlSqlParserEngine.isWrappedSource(downloader.getCachedSource(srcObjName))) {
                    node.setReadable(false);
                    node.setMessage("Source is ENCRYPTED/WRAPPED - cannot be parsed");
                }

                edgeBatch.clear();

                if (targetSub != null) {
                    node.setObjectType(targetSub.getType());
                    node.setLineStart(targetSub.getLineStart());
                    node.setLineEnd(targetSub.getLineEnd());
                    node.setParameters(targetSub.getParameters());
                    node.setLocalVariables(targetSub.getLocalVariables());
                    node.setStatements(targetSub.getStatements());
                    node.setCursors(targetSub.getCursors());
                    node.setDynamicSql(targetSub.getDynamicSql());
                    node.setExceptionHandlers(targetSub.getExceptionHandlers());
                    node.setExternalPackageVarRefs(targetSub.getExternalPackageVarRefs());

                    List<CallInfo> resolvedCalls = resolveCallSchemas(targetSub.getCalls(), ownerSchema);
                    node.setCalls(resolvedCalls);
                    List<TableOperationInfo> resolvedTableOps = resolveTableSchemas(targetSub.getTableOperations(), ownerSchema);
                    node.setTableOperations(resolvedTableOps);
                    aggregateTableInfo(tableInfoMap, resolvedTableOps, visitKey);
                    enqueueExternalCalls(resolvedCalls, item.depth, item.packageName,
                            item.subprogramName, ownerSchema, queue, edgeBatch);

                } else if (targetObj != null) {
                    node.setObjectType(targetObj.getType());
                    node.setLineStart(targetObj.getLineStart());
                    node.setLineEnd(targetObj.getLineEnd());
                    node.setParameters(targetObj.getParameters());
                    if (targetObj.getTriggerEvent() != null) node.setTriggerEvent(targetObj.getTriggerEvent());
                    if (targetObj.getTriggerTable() != null) node.setTriggerTable(targetObj.getTriggerTable());
                    if (targetObj.getTriggerTiming() != null) node.setTriggerTiming(targetObj.getTriggerTiming());

                    // Aggregate package-level + ALL subprogram data onto the node
                    // so that table operations, calls, cursors, etc. are visible
                    List<CallInfo> allCalls = new ArrayList<>(
                            resolveCallSchemas(targetObj.getCalls(), ownerSchema));
                    List<TableOperationInfo> allTableOps = new ArrayList<>(
                            resolveTableSchemas(targetObj.getTableOperations(), ownerSchema));
                    List<StatementInfo> allStatements = new ArrayList<>();
                    List<CursorInfo> allCursors = new ArrayList<>();
                    List<DynamicSqlInfo> allDynSql = new ArrayList<>();
                    List<ExceptionHandlerInfo> allExHandlers = new ArrayList<>();
                    List<String> allExtPkgVarRefs = new ArrayList<>();
                    List<VariableInfo> allLocalVars = new ArrayList<>();

                    if (targetObj.getStatements() != null) allStatements.addAll(targetObj.getStatements());
                    if (targetObj.getCursors() != null) allCursors.addAll(targetObj.getCursors());
                    if (targetObj.getDynamicSql() != null) allDynSql.addAll(targetObj.getDynamicSql());
                    if (targetObj.getExceptionHandlers() != null) allExHandlers.addAll(targetObj.getExceptionHandlers());
                    if (targetObj.getExternalPackageVarRefs() != null) allExtPkgVarRefs.addAll(targetObj.getExternalPackageVarRefs());

                    for (SubprogramInfo sub : targetObj.getSubprograms()) {
                        List<CallInfo> subCalls = resolveCallSchemas(sub.getCalls(), ownerSchema);
                        allCalls.addAll(subCalls);
                        List<TableOperationInfo> subTableOps = resolveTableSchemas(sub.getTableOperations(), ownerSchema);
                        allTableOps.addAll(subTableOps);
                        if (sub.getStatements() != null) allStatements.addAll(sub.getStatements());
                        if (sub.getCursors() != null) allCursors.addAll(sub.getCursors());
                        if (sub.getDynamicSql() != null) allDynSql.addAll(sub.getDynamicSql());
                        if (sub.getExceptionHandlers() != null) allExHandlers.addAll(sub.getExceptionHandlers());
                        if (sub.getExternalPackageVarRefs() != null) {
                            for (String ref : sub.getExternalPackageVarRefs()) {
                                if (!allExtPkgVarRefs.contains(ref)) allExtPkgVarRefs.add(ref);
                            }
                        }
                        if (sub.getLocalVariables() != null) allLocalVars.addAll(sub.getLocalVariables());
                    }

                    node.setCalls(allCalls);
                    node.setTableOperations(allTableOps);
                    node.setStatements(allStatements);
                    node.setCursors(allCursors);
                    node.setDynamicSql(allDynSql);
                    node.setExceptionHandlers(allExHandlers);
                    node.setExternalPackageVarRefs(allExtPkgVarRefs);
                    node.setLocalVariables(allLocalVars);

                    aggregateTableInfo(tableInfoMap, allTableOps, visitKey);
                    enqueueExternalCalls(allCalls, item.depth, item.packageName,
                            item.subprogramName, ownerSchema, queue, edgeBatch);

                } else {
                    errors.add("Could not find specific subprogram '" + item.subprogramName
                            + "' in parsed result for: " + visitKey);
                    if (!parseResult.getObjects().isEmpty()) {
                        ParsedObject firstObj = parseResult.getObjects().get(0);
                        node.setObjectType(firstObj.getType());
                        ownerSchema = resolveObjectSchema(
                                item.packageName != null ? item.packageName : item.subprogramName,
                                firstObj.getSchema());
                        node.setSchema(ownerSchema);

                        // Aggregate calls and tables from ALL found subprograms into the node
                        List<CallInfo> allCalls = new ArrayList<>();
                        List<TableOperationInfo> allTableOps = new ArrayList<>();
                        List<StatementInfo> allStatements = new ArrayList<>();

                        for (SubprogramInfo sub : firstObj.getSubprograms()) {
                            List<CallInfo> subCalls = resolveCallSchemas(sub.getCalls(), ownerSchema);
                            allCalls.addAll(subCalls);
                            enqueueExternalCalls(subCalls, item.depth, item.packageName,
                                    item.subprogramName, ownerSchema, queue, edgeBatch);
                            List<TableOperationInfo> subTableOps = resolveTableSchemas(sub.getTableOperations(), ownerSchema);
                            allTableOps.addAll(subTableOps);
                            aggregateTableInfo(tableInfoMap, subTableOps, visitKey);
                            if (sub.getStatements() != null) allStatements.addAll(sub.getStatements());
                        }

                        List<CallInfo> objCalls = resolveCallSchemas(firstObj.getCalls(), ownerSchema);
                        allCalls.addAll(objCalls);
                        List<TableOperationInfo> objTableOps = resolveTableSchemas(firstObj.getTableOperations(), ownerSchema);
                        allTableOps.addAll(objTableOps);
                        if (firstObj.getStatements() != null) allStatements.addAll(firstObj.getStatements());

                        node.setCalls(allCalls);
                        node.setTableOperations(allTableOps);
                        node.setStatements(allStatements);
                        aggregateTableInfo(tableInfoMap, objTableOps, visitKey);
                        enqueueExternalCalls(objCalls, item.depth, item.packageName,
                                item.subprogramName, ownerSchema, queue, edgeBatch);
                    }
                }

                if (!node.isReadable()) {
                    // Encrypted/wrapped — suppress parse errors
                } else if (parseResult.getErrors() != null && !parseResult.getErrors().isEmpty()) {
                    for (String err : parseResult.getErrors()) {
                        errors.add("[" + visitKey + "] " + err);
                    }
                }

                // Compute per-node metrics inline
                if (node.getLineEnd() > 0 && node.getLineStart() > 0) {
                    node.setLinesOfCode(node.getLineEnd() - node.getLineStart() + 1);
                }
                Map<String, Integer> stmtSummary = new LinkedHashMap<>();
                if (node.getStatements() != null) {
                    for (StatementInfo s : node.getStatements()) {
                        String stype = s.getType() != null ? s.getType().toUpperCase() : "OTHER";
                        stmtSummary.merge(stype, 1, Integer::sum);
                    }
                }
                node.setStatementSummary(stmtSummary);

                node.generateNodeId();

                // Write chunk to disk and flush from memory
                if (chunkedWriter != null) {
                    try {
                        chunkedWriter.writeChunk(node);
                        chunkedWriter.appendEdges(new ArrayList<>(edgeBatch), orderCounter);
                        chunkedWriter.updateTables(tableInfoMap);
                        chunkedWriter.markDone(visitKey);

                        // Write source file for this node
                        writeNodeSource(chunkedWriter, node, ownerSchema);
                    } catch (IOException e) {
                        errors.add("Chunk write error for " + visitKey + ": " + e.getMessage());
                    }
                }

            } catch (Exception e) {
                errors.add("Error processing " + visitKey + ": " + e.getMessage());
                System.err.println("[Crawler] Error processing " + visitKey + ": " + e.getMessage());
                if (chunkedWriter != null) {
                    try { chunkedWriter.markFailed(visitKey, e.getMessage()); } catch (IOException ignored) {}
                }
            }
        }

        // Discover triggers from DB for tables with DML operations
        if (chunkedWriter != null) {
            try {
                TriggerDiscoverer triggerDiscoverer = new TriggerDiscoverer(downloader.getConnManager(), schemaResolver);
                List<Map<String, Object>> dbTriggers = triggerDiscoverer.discover(tableInfoMap);
                if (!dbTriggers.isEmpty()) {
                    chunkedWriter.setDiscoveredTriggers(dbTriggers);
                    System.err.println("[Crawler] Discovered " + dbTriggers.size()
                            + " triggers from ALL_TRIGGERS for DML tables");
                }
            } catch (Exception e) {
                errors.add("Trigger discovery warning: " + e.getMessage());
                System.err.println("[Crawler] Trigger discovery failed (non-fatal): " + e.getMessage());
            }
        }

        // Final merge: read all chunks back and assemble FlowResult
        if (chunkedWriter != null) {
            try {
                FlowResult result = chunkedWriter.mergeChunks(
                        upperEntry, entrySchema,
                        System.currentTimeMillis() - startTime,
                        maxDepthReached, errors);
                result.setDbCallCount(downloader.getConnManager() != null
                        ? downloader.getConnManager().getDbCallCount() : 0);
                // Compute callCount/calledBy on merged in-memory data
                computeNodeMetrics(result.getFlowNodes(), result.getCallGraph());
                // Write final flow.json WITH metrics applied
                chunkedWriter.writeFinalResult(result);
                // Write schema resolver query results as separate JSONs
                chunkedWriter.writeResolverData(schemaResolver);
                return result;
            } catch (IOException e) {
                System.err.println("[Crawler] Merge failed: " + e.getMessage());
            }
        }

        // Fallback: in-memory result (no output dir)
        return buildInMemoryResult(upperEntry, entrySchema, startTime,
                maxDepthReached, errors, tableInfoMap);
    }

    private void writeNodeSource(ChunkedFlowWriter writer, FlowNode node, String ownerSchema) {
        try {
            String schema = ownerSchema != null ? ownerSchema : "UNKNOWN";
            String objName = node.getObjectName() != null ? node.getObjectName() : "UNKNOWN";
            String pkgName = node.getPackageName();

            if (pkgName != null && !pkgName.isEmpty()) {
                String source = downloader.getCachedSource(pkgName);
                if (source != null) {
                    // Write extracted subprogram
                    String fileName = schema + "." + pkgName + "." + objName + ".sql";
                    if (!writer.sourceExists(fileName) && node.getLineStart() > 0 && node.getLineEnd() > 0) {
                        String[] lines = source.split("\\r?\\n");
                        int start = Math.max(0, node.getLineStart() - 1);
                        int end = Math.min(lines.length, node.getLineEnd());
                        StringBuilder extracted = new StringBuilder();
                        extracted.append("-- Extracted from ").append(schema).append(".").append(pkgName).append("\n");
                        extracted.append("-- Lines ").append(node.getLineStart()).append(" to ").append(node.getLineEnd()).append("\n\n");
                        for (int i = start; i < end; i++) {
                            extracted.append(lines[i]).append("\n");
                        }
                        writer.writeSourceFile(fileName, extracted.toString());
                    }
                    // Write full package body once
                    String pkbFile = schema + "." + pkgName + ".pkb";
                    if (!writer.sourceExists(pkbFile)) {
                        writer.writeSourceFile(pkbFile, source);
                    }
                }
            } else {
                String source = downloader.getCachedSource(objName);
                if (source != null) {
                    String ext = "sql";
                    String type = downloader.getCachedType(objName);
                    if ("FUNCTION".equals(type)) ext = "fnc";
                    else if ("PROCEDURE".equals(type)) ext = "prc";
                    String fileName = schema + "." + objName + "." + ext;
                    if (!writer.sourceExists(fileName)) {
                        writer.writeSourceFile(fileName, source);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[Crawler] Source write error: " + e.getMessage());
        }
    }

    private FlowResult buildInMemoryResult(String entryPoint, String entrySchema,
                                            long startTime, int maxDepthReached,
                                            List<String> errors,
                                            Map<String, SchemaTableInfo> tableInfoMap) {
        FlowResult result = new FlowResult();
        result.setEntryPoint(entryPoint);
        result.setEntrySchema(entrySchema);
        result.setAllTables(new ArrayList<>(tableInfoMap.values()));
        result.setMaxDepthReached(maxDepthReached);
        result.setErrors(errors);
        result.setCrawlTimeMs(System.currentTimeMillis() - startTime);
        result.setDbCallCount(downloader.getConnManager() != null
                ? downloader.getConnManager().getDbCallCount() : 0);
        return result;
    }

    /**
     * Compute per-node metrics: linesOfCode, callCount, calledBy, statementSummary.
     */
    private void computeNodeMetrics(List<FlowNode> nodes, List<FlowEdge> edges) {
        // Build nodeId -> FlowNode index
        Map<String, FlowNode> nodeIndex = new LinkedHashMap<>();
        for (FlowNode n : nodes) {
            if (n.getNodeId() != null) {
                nodeIndex.put(n.getNodeId(), n);
            }
        }

        // Compute linesOfCode and statementSummary for each node
        for (FlowNode n : nodes) {
            if (n.getLineEnd() > 0 && n.getLineStart() > 0) {
                n.setLinesOfCode(n.getLineEnd() - n.getLineStart() + 1);
            }

            Map<String, Integer> stmtSummary = new LinkedHashMap<>();
            if (n.getStatements() != null) {
                for (StatementInfo s : n.getStatements()) {
                    String type = s.getType() != null ? s.getType().toUpperCase() : "OTHER";
                    stmtSummary.merge(type, 1, Integer::sum);
                }
            }
            n.setStatementSummary(stmtSummary);
        }

        // Compute callCount and calledBy from edges
        Map<String, Integer> callCounts = new LinkedHashMap<>();
        Map<String, Set<String>> calledByMap = new LinkedHashMap<>();

        for (FlowEdge e : edges) {
            String toId = e.getToNodeId();
            String fromId = e.getFromNodeId();
            if (toId == null) continue;

            // Match toNodeId against actual nodes (edges have partial nodeId without params)
            String matchedToId = findMatchingNodeId(toId, nodeIndex);
            if (matchedToId != null) {
                callCounts.merge(matchedToId, 1, Integer::sum);
                calledByMap.computeIfAbsent(matchedToId, k -> new LinkedHashSet<>()).add(
                        fromId != null ? fromId : "UNKNOWN");
            }
        }

        for (FlowNode n : nodes) {
            String nid = n.getNodeId();
            if (nid != null) {
                n.setCallCount(callCounts.getOrDefault(nid, 0));
                Set<String> callers = calledByMap.get(nid);
                if (callers != null) {
                    n.setCalledBy(new ArrayList<>(callers));
                }
            }
        }
    }

    private String findMatchingNodeId(String edgeId, Map<String, FlowNode> nodeIndex) {
        if (nodeIndex.containsKey(edgeId)) return edgeId;
        for (String nid : nodeIndex.keySet()) {
            if (nid.startsWith(edgeId + "$") || nid.startsWith(edgeId + "_")) return nid;
        }
        return null;
    }

    /**
     * Download source from DB and parse it. Results are cached.
     */
    private ParseResult downloadAndParse(String packageName, String subprogramName) {
        String objectName = packageName != null ? packageName : subprogramName;
        String cacheKey = objectName.toUpperCase();

        ParseResult cached = parseCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        String source = null;

        if (packageName != null) {
            // It's a package subprogram - download the package body
            source = downloader.downloadPackageBody(packageName);
            if (source == null) {
                // Try package spec as fallback
                source = downloader.downloadPackageSpec(packageName);
            }
        } else {
            // Standalone - try all types
            source = downloader.downloadAny(subprogramName);
        }

        if (source == null) {
            System.err.println("[Crawler] No source found for: " + objectName);
            return null;
        }

        // Parse the source
        ParseResult result = engine.parseContent(source, objectName + ".sql");
        parseCache.put(cacheKey, result);
        return result;
    }

    /**
     * Resolve the schema for calls in the list.
     */
    private List<CallInfo> resolveCallSchemas(List<CallInfo> calls, String ownerSchema) {
        if (calls == null) return new ArrayList<>();
        List<CallInfo> resolved = new ArrayList<>();
        for (CallInfo call : calls) {
            // Create a copy to avoid modifying original
            CallInfo rc = new CallInfo();
            rc.setType(call.getType());
            rc.setPackageName(call.getPackageName());
            rc.setName(call.getName());
            rc.setLine(call.getLine());
            rc.setArguments(call.getArguments());

            // Resolve schema
            if (call.getSchema() != null && !call.getSchema().isEmpty()) {
                rc.setSchema(call.getSchema());
            } else {
                String target = call.getPackageName() != null
                        ? call.getPackageName() : call.getName();
                if (target != null) {
                    String resolvedSchema = schemaResolver.resolveSchema(target, null, null);
                    rc.setSchema(resolvedSchema != null ? resolvedSchema : ownerSchema);
                }
            }
            resolved.add(rc);
        }
        return resolved;
    }

    /**
     * Resolve schemas for table operations.
     */
    private List<TableOperationInfo> resolveTableSchemas(List<TableOperationInfo> ops,
                                                          String ownerSchema) {
        if (ops == null) return new ArrayList<>();

        List<String> unresolvedNames = new ArrayList<>();
        for (TableOperationInfo op : ops) {
            if ((op.getSchema() == null || op.getSchema().isEmpty()) && op.getTableName() != null
                    && !op.getTableName().contains(".")) {
                unresolvedNames.add(op.getTableName());
            }
        }
        if (!unresolvedNames.isEmpty()) {
            schemaResolver.batchResolveTableOwners(unresolvedNames);
        }

        List<TableOperationInfo> resolved = new ArrayList<>();
        for (TableOperationInfo op : ops) {
            TableOperationInfo rt = new TableOperationInfo();
            rt.setTableName(op.getTableName());
            rt.setOperation(op.getOperation());
            rt.setLine(op.getLine());
            rt.setAlias(op.getAlias());
            rt.setJoins(op.getJoins());

            // Resolve schema
            String resolvedName = op.getTableName();
            if (op.getSchema() != null && !op.getSchema().isEmpty()) {
                rt.setSchema(op.getSchema());
            } else if (resolvedName != null) {
                if (resolvedName.contains(".")) {
                    String[] parts = resolvedName.split("\\.", 2);
                    String candidate = schemaResolver.resolveSchema(parts[1], "TABLE", parts[0]);
                    if (candidate != null) {
                        rt.setSchema(candidate);
                        rt.setTableName(parts[1]);
                        resolvedName = parts[1];
                    } else {
                        rt.setSchema(parts[0].toUpperCase());
                        rt.setTableName(parts[1]);
                        resolvedName = parts[1];
                    }
                } else {
                    String resolvedSchema = schemaResolver.resolveSchema(resolvedName, "TABLE", null);
                    rt.setSchema(resolvedSchema != null ? resolvedSchema : ownerSchema);
                }
            }

            // Resolve objectType (TABLE, VIEW, MATERIALIZED VIEW)
            if (resolvedName != null) {
                String objType = schemaResolver.resolveObjectType(resolvedName.toUpperCase());
                rt.setObjectType(objType != null ? objType : "TABLE");
            }
            resolved.add(rt);
        }
        return resolved;
    }

    /**
     * Aggregate table operation info into the global map.
     */
    private void aggregateTableInfo(Map<String, SchemaTableInfo> tableInfoMap,
                                     List<TableOperationInfo> tableOps,
                                     String referencedBy) {
        if (tableOps == null) return;
        for (TableOperationInfo op : tableOps) {
            String schema = op.getSchema() != null ? op.getSchema() : "UNKNOWN";
            String tableName = op.getTableName() != null ? op.getTableName().toUpperCase() : "UNKNOWN";
            String key = schema + "." + tableName;

            SchemaTableInfo info = tableInfoMap.computeIfAbsent(key, k -> {
                SchemaTableInfo sti = new SchemaTableInfo();
                sti.setSchema(schema);
                sti.setTableName(tableName);
                // Try to resolve the object type
                String objType = schemaResolver.resolveObjectType(tableName);
                sti.setObjectType(objType != null ? objType : "TABLE");
                return sti;
            });

            if (op.getOperation() != null) {
                info.getOperations().add(op.getOperation().toUpperCase());
            }
            if (op.getLine() > 0) {
                info.getLines().add(op.getLine());
            }
            if (referencedBy != null) {
                info.getReferencedBy().add(referencedBy);
            }
        }
    }

    /**
     * Enqueue external calls for BFS processing.
     * Uses ALL_DEPENDENCIES to validate that the call is a real dependency,
     * filtering out false positives like record field accesses and type references.
     */
    private void enqueueExternalCalls(List<CallInfo> calls, int currentDepth,
                                       String callerPackage, String callerObject,
                                       String callerSchema,
                                       Queue<BfsItem> queue, List<FlowEdge> callGraph) {
        if (calls == null) return;

        String sourceObject = callerPackage != null ? callerPackage : callerObject;

        for (CallInfo call : calls) {
            String callType = call.getType();
            boolean isInternal = "INTERNAL".equalsIgnoreCase(callType);
            boolean isExternal = "EXTERNAL".equalsIgnoreCase(callType);

            if (!isExternal && !isInternal) {
                continue;
            }

            String targetPackage = call.getPackageName();
            String targetObject = call.getName();

            if (targetObject == null || targetObject.isEmpty()) {
                continue;
            }

            // For internal calls, the target is within the same package
            if (isInternal) {
                if (callerPackage == null) continue;
                targetPackage = callerPackage;
            }

            // Reclassify: EXTERNAL with no package may be internal to caller's package
            if (isExternal && targetPackage == null && callerPackage != null) {
                String rawSource = downloader.getCachedSource(callerPackage);
                if (rawSource != null && hasSubprogram(rawSource, targetObject)) {
                    targetPackage = callerPackage;
                    isExternal = false;
                    isInternal = true;
                }
            }

            if (isBuiltInPackage(targetPackage) || isBuiltInFunction(targetObject)) {
                continue;
            }

            // For external calls, validate against schema resolver.
            // Only filter if resolver positively has dep data for the source.
            if (isExternal) {
                String targetToCheck = targetPackage != null ? targetPackage : targetObject;
                if (schemaResolver.hasDependencyData(sourceObject)
                        && !schemaResolver.isDependency(sourceObject, targetToCheck)
                        && !schemaResolver.isKnownObject(targetToCheck)) {
                    continue;
                }
            }

            String visitKey = buildVisitKey(targetPackage, targetObject);
            if (visited.contains(visitKey)) {
                addCallEdge(callGraph, callerSchema, callerPackage, callerObject,
                        call.getSchema() != null ? call.getSchema() : callerSchema,
                        targetPackage, targetObject,
                        call.getLine(), currentDepth);
                continue;
            }

            addCallEdge(callGraph, callerSchema, callerPackage, callerObject,
                    call.getSchema() != null ? call.getSchema() : callerSchema,
                    targetPackage, targetObject,
                    call.getLine(), currentDepth);

            queue.add(new BfsItem(targetPackage, targetObject, currentDepth + 1,
                    callerPackage, callerObject, call.getLine()));
        }
    }

    /**
     * Add a call edge to the call graph.
     */
    private void addCallEdge(List<FlowEdge> callGraph,
                              String fromSchema, String fromPackage, String fromObject,
                              String toSchema, String toPackage, String toObject,
                              int line, int depth) {
        FlowEdge edge = new FlowEdge();
        edge.setFromSchema(fromSchema);
        edge.setFromPackage(fromPackage);
        edge.setFromObject(fromObject);
        edge.setToSchema(toSchema);
        edge.setToPackage(toPackage);
        edge.setToObject(toObject);
        edge.setLine(line);
        edge.setDepth(depth);

        // Build nodeIds — for FROM, also check visited set to resolve package
        String fromPkg = fromPackage;
        if (fromPkg == null && fromObject != null) {
            String visitKey = fromObject.toUpperCase();
            for (String v : visited) {
                if (v.endsWith("." + visitKey)) {
                    fromPkg = v.substring(0, v.length() - visitKey.length() - 1);
                    break;
                }
            }
        }
        edge.setFromNodeId(buildNodeId(fromSchema, fromPkg, fromObject));
        edge.setToNodeId(buildNodeId(toSchema, toPackage, toObject));
        callGraph.add(edge);
    }

    private String buildNodeId(String schema, String pkg, String object) {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isEmpty()) sb.append(schema);
        if (pkg != null && !pkg.isEmpty()) sb.append("$").append(pkg);
        if (object != null && !object.isEmpty()) sb.append("$").append(object);
        return sb.toString().toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
    }

    /**
     * Resolve the schema for a given object, using the SchemaResolver
     * and falling back to the source downloader's owner cache.
     */
    private String resolveObjectSchema(String objectName, String parsedSchema) {
        // If the parser already found a schema, use it
        if (parsedSchema != null && !parsedSchema.isEmpty()) {
            return parsedSchema.toUpperCase();
        }

        // Try schema resolver
        String resolved = schemaResolver.resolveSchema(objectName);
        if (resolved != null) {
            return resolved;
        }

        // Try the source downloader's owner cache
        String owner = downloader.getOwner(objectName);
        if (owner != null) {
            return owner.toUpperCase();
        }

        return null;
    }

    /**
     * Build a visit key for the visited set.
     */
    private String buildVisitKey(String packageName, String subprogramName) {
        if (packageName != null && !packageName.isEmpty()) {
            return packageName.toUpperCase() + "." + subprogramName.toUpperCase();
        }
        return subprogramName.toUpperCase();
    }

    /**
     * Check if an object type is non-parseable (TYPE, SYNONYM, SEQUENCE, etc.)
     */
    private boolean isNonParsableType(String objectType) {
        if (objectType == null) return false;
        String upper = objectType.toUpperCase().replace(" ", "_");
        return upper.equals("TYPE") || upper.equals("TYPE_BODY")
                || upper.equals("SYNONYM") || upper.equals("SEQUENCE")
                || upper.equals("TABLE") || upper.equals("VIEW")
                || upper.equals("MATERIALIZED_VIEW") || upper.equals("INDEX")
                || upper.equals("LIBRARY") || upper.equals("JAVA_CLASS");
    }

    private boolean hasSubprogram(String source, String name) {
        if (source == null || name == null) return false;
        String pat = "(?i)(?:PROCEDURE|FUNCTION)\\s+" + Pattern.quote(name) + "\\b";
        return Pattern.compile(pat).matcher(source).find();
    }

    /**
     * Extract a single subprogram source from a package body using regex.
     * Matches PROCEDURE/FUNCTION name ... END name; patterns.
     */
    private String extractSubprogramSource(String source, String subName) {
        if (source == null || subName == null) return null;
        // Try with END subName; first
        String pat = "(?is)((?:PROCEDURE|FUNCTION)\\s+" + Pattern.quote(subName)
                + "\\b.*?END\\s+" + Pattern.quote(subName) + "\\s*;)";
        Matcher m = Pattern.compile(pat).matcher(source);
        if (m.find()) {
            return m.group(1);
        }

        // Fallback: find start of subprogram, then find the matching END;
        // by counting BEGIN/END nesting
        String startPat = "(?i)(PROCEDURE|FUNCTION)\\s+" + Pattern.quote(subName) + "\\b";
        Matcher startM = Pattern.compile(startPat).matcher(source);
        if (startM.find()) {
            int startIdx = startM.start();
            // Find the first BEGIN after the declaration
            String afterDecl = source.substring(startIdx);
            int nestLevel = 0;
            boolean foundFirstBegin = false;
            int pos = 0;
            // Tokenize to find balanced BEGIN/END
            Pattern tokenPat = Pattern.compile("(?i)\\b(BEGIN|END)\\b");
            Matcher tokenM = tokenPat.matcher(afterDecl);
            while (tokenM.find()) {
                String token = tokenM.group(1).toUpperCase();
                if ("BEGIN".equals(token)) {
                    nestLevel++;
                    foundFirstBegin = true;
                } else if ("END".equals(token) && foundFirstBegin) {
                    nestLevel--;
                    if (nestLevel == 0) {
                        // Find the semicolon after END
                        int endPos = tokenM.end();
                        int semiIdx = afterDecl.indexOf(';', endPos);
                        if (semiIdx >= 0) {
                            return afterDecl.substring(0, semiIdx + 1);
                        }
                        return afterDecl.substring(0, endPos);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Check if a package name is a known Oracle built-in.
     */
    private boolean isBuiltInPackage(String packageName) {
        if (packageName == null) return false;
        String upper = packageName.toUpperCase();
        return upper.startsWith("DBMS_")
                || upper.startsWith("UTL_")
                || upper.startsWith("SYS.")
                || upper.equals("STANDARD")
                || upper.equals("DBMS_OUTPUT")
                || upper.equals("DBMS_LOB")
                || upper.equals("DBMS_SQL")
                || upper.equals("DBMS_UTILITY")
                || upper.equals("DBMS_SESSION")
                || upper.equals("DBMS_LOCK")
                || upper.equals("DBMS_SCHEDULER")
                || upper.equals("DBMS_METADATA")
                || upper.equals("DBMS_XMLDOM")
                || upper.equals("DBMS_XMLPARSER")
                || upper.equals("DBMS_XMLGEN")
                || upper.equals("DBMS_CRYPTO")
                || upper.equals("DBMS_RANDOM")
                || upper.equals("DBMS_APPLICATION_INFO")
                || upper.equals("APEX_JSON")
                || upper.equals("APEX_WEB_SERVICE")
                || upper.equals("HTF")
                || upper.equals("HTP")
                || upper.equals("OWA_UTIL");
    }

    /**
     * Check if a function name is a known Oracle built-in function.
     */
    private boolean isBuiltInFunction(String funcName) {
        if (funcName == null) return false;
        String upper = funcName.toUpperCase();
        return upper.equals("NVL")
                || upper.equals("NVL2")
                || upper.equals("DECODE")
                || upper.equals("TO_CHAR")
                || upper.equals("TO_DATE")
                || upper.equals("TO_NUMBER")
                || upper.equals("TO_TIMESTAMP")
                || upper.equals("SUBSTR")
                || upper.equals("INSTR")
                || upper.equals("LENGTH")
                || upper.equals("REPLACE")
                || upper.equals("TRIM")
                || upper.equals("LTRIM")
                || upper.equals("RTRIM")
                || upper.equals("UPPER")
                || upper.equals("LOWER")
                || upper.equals("INITCAP")
                || upper.equals("LPAD")
                || upper.equals("RPAD")
                || upper.equals("ROUND")
                || upper.equals("TRUNC")
                || upper.equals("MOD")
                || upper.equals("ABS")
                || upper.equals("CEIL")
                || upper.equals("FLOOR")
                || upper.equals("POWER")
                || upper.equals("SQRT")
                || upper.equals("SYSDATE")
                || upper.equals("SYSTIMESTAMP")
                || upper.equals("CURRENT_DATE")
                || upper.equals("CURRENT_TIMESTAMP")
                || upper.equals("ADD_MONTHS")
                || upper.equals("MONTHS_BETWEEN")
                || upper.equals("LAST_DAY")
                || upper.equals("NEXT_DAY")
                || upper.equals("EXTRACT")
                || upper.equals("COALESCE")
                || upper.equals("NULLIF")
                || upper.equals("GREATEST")
                || upper.equals("LEAST")
                || upper.equals("CASE")
                || upper.equals("CAST")
                || upper.equals("COUNT")
                || upper.equals("SUM")
                || upper.equals("AVG")
                || upper.equals("MIN")
                || upper.equals("MAX")
                || upper.equals("LISTAGG")
                || upper.equals("ROW_NUMBER")
                || upper.equals("RANK")
                || upper.equals("DENSE_RANK")
                || upper.equals("ROWNUM")
                || upper.equals("ROWID")
                || upper.equals("RAISE_APPLICATION_ERROR")
                || upper.equals("SQL%ROWCOUNT")
                || upper.equals("SQL%FOUND")
                || upper.equals("SQL%NOTFOUND")
                || upper.equals("SQLCODE")
                || upper.equals("SQLERRM")
                || upper.equals("USER")
                || upper.equals("SYS_CONTEXT")
                || upper.equals("REGEXP_LIKE")
                || upper.equals("REGEXP_REPLACE")
                || upper.equals("REGEXP_SUBSTR")
                || upper.equals("REGEXP_INSTR")
                || upper.equals("XMLTYPE")
                || upper.equals("XMLELEMENT")
                || upper.equals("XMLFOREST")
                || upper.equals("XMLAGG")
                || upper.equals("XMLQUERY")
                || upper.equals("EXISTSNODE")
                || upper.equals("CONCAT");
    }

    /**
     * BFS queue item.
     */
    private static class BfsItem {
        final String packageName;
        final String subprogramName;
        final int depth;
        final String callerPackage;
        final String callerObject;
        final int callLine;

        BfsItem(String packageName, String subprogramName, int depth,
                String callerPackage, String callerObject, int callLine) {
            this.packageName = packageName;
            this.subprogramName = subprogramName;
            this.depth = depth;
            this.callerPackage = callerPackage;
            this.callerObject = callerObject;
            this.callLine = callLine;
        }
    }
}
