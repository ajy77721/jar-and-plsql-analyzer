package com.plsqlanalyzer.web.controller;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.web.service.LazyAnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.CursorOperationSummary;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.config.DbUserConfig;
import com.plsqlanalyzer.config.EnvironmentConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.parser.service.OracleDictionaryService;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.DependencyRecord;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.ObjectRecord;
import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.plsqlanalyzer.web.service.AnalysisJobService;
import com.plsqlanalyzer.web.service.AnalysisJobService.Job;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import com.plsqlanalyzer.web.service.PersistenceService;
import com.plsqlanalyzer.web.service.ProgressService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RestController("plsqlAnalyzerController")
@RequestMapping("/api/plsql/analysis")
public class AnalyzerController {

    private static final Logger log = LoggerFactory.getLogger(AnalyzerController.class);

    private final AnalysisService analysisService;
    private final PlsqlConfig config;
    private final PersistenceService persistenceService;
    private final DbSourceFetcher dbFetcher;
    private final ProgressService progressService;
    private final AnalysisJobService jobService;
    private final AnalysisQueueService queueService;

    @Value("${plsql.tree.max-depth:50}")
    private int treeMaxDepth;

    @Value("${plsql.tree.max-nodes:2000}")
    private int treeMaxNodes;

    public AnalyzerController(AnalysisService analysisService, PlsqlConfig config,
                              @Qualifier("plsqlPersistenceService") PersistenceService persistenceService,
                              DbSourceFetcher dbFetcher,
                              @Qualifier("plsqlProgressService") ProgressService progressService,
                              AnalysisJobService jobService,
                              AnalysisQueueService queueService) {
        this.analysisService = analysisService;
        this.config = config;
        this.persistenceService = persistenceService;
        this.dbFetcher = dbFetcher;
        this.progressService = progressService;
        this.jobService = jobService;
        this.queueService = queueService;

        // Auto-load latest persisted analysis on startup
        if (analysisService.getLatestResult() == null) {
            AnalysisResult saved = persistenceService.loadLatest();
            if (saved != null) {
                analysisService.setLatestResult(saved);
                log.info("Auto-loaded analysis '{}' on startup", saved.getName());
            }
        }
    }

    /**
     * Start analysis from DB. Async with SSE progress.
     * Body: {username?, objectName, objectType?, procedureName?}
     *
     * Smart input — all of these work:
     *   "PG_AC_EINVOICE"                        → find schema, detect type (PACKAGE)
     *   "PG_AC_EINVOICE.PC_IRBM_UPD"            → package.procedure (auto-find schema)
     *   "OPUS_CORE.PG_AC_EINVOICE"               → schema.object (auto-detect type)
     *   "OPUS_CORE.PG_AC_EINVOICE.PC_IRBM_UPD"  → schema.package.procedure
     *   "MY_PROCEDURE"                            → standalone PROCEDURE (auto-find)
     *   "MY_FUNCTION"                             → standalone FUNCTION (auto-find)
     *   "MY_TRIGGER"                              → standalone TRIGGER (auto-find)
     */
    @PostMapping("/analyze")
    public ResponseEntity<Map<String, Object>> triggerAnalysis(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        String objectName = body.get("objectName");
        String objectType = body.get("objectType");
        String procedureName = body.get("procedureName");
        String projectName = body.get("project");
        String envName = body.get("env");

        // Resolve environment-specific JDBC URL and users if project/env specified
        String resolvedJdbcUrl = null;
        List<DbUserConfig> resolvedUsers = null;
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig envConfig = config.resolveEnvironment(projectName, envName);
            if (envConfig != null) {
                resolvedJdbcUrl = envConfig.getJdbcUrl();
                resolvedUsers = envConfig.getUsers();
                log.info("Using environment {}/{} -> {}", projectName, envName, resolvedJdbcUrl);
            }
        }

        if (objectName == null || objectName.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "objectName is required"));
        }

        // ---- Smart parsing: split dots ----
        String[] parts = objectName.toUpperCase().trim().split("\\.");

        if (parts.length == 3) {
            // SCHEMA.PKG.PROC — all three given
            if (username == null || username.isBlank()) username = parts[0];
            objectName = parts[1];
            if (procedureName == null || procedureName.isBlank()) procedureName = parts[2];
        } else if (parts.length == 2) {
            // Could be SCHEMA.OBJECT or PKG.PROC
            // First check: is parts[0] a known schema?
            List<DbUserConfig> effectiveUsers = resolvedUsers != null ? resolvedUsers : config.getDbUsers();
            boolean firstIsSchema = effectiveUsers.stream()
                    .anyMatch(u -> u.getUsername().equalsIgnoreCase(parts[0]));
            if (firstIsSchema) {
                // SCHEMA.OBJECT
                if (username == null || username.isBlank()) username = parts[0];
                objectName = parts[1];
            } else {
                // Not a schema — try to find parts[0] as an object (PKG.PROC pattern)
                // But first, verify parts[0] actually exists as an object somewhere
                ResolvedObject r0 = findObjectInAllSchemas(parts[0]);
                if (r0 != null && ("PACKAGE".equals(r0.objectType) || "PACKAGE BODY".equals(r0.objectType))) {
                    // parts[0] is a package → parts[1] is the procedure
                    objectName = parts[0];
                    if (procedureName == null || procedureName.isBlank()) procedureName = parts[1];
                    if (username == null || username.isBlank()) username = r0.schema;
                    objectType = "PACKAGE";
                } else if (r0 != null) {
                    // parts[0] is a standalone proc/func — parts[1] doesn't make sense; treat as schema.object
                    objectName = parts[0];
                    if (username == null || username.isBlank()) username = r0.schema;
                    objectType = r0.objectType;
                } else {
                    // parts[0] not found anywhere — maybe parts[1] is the object and parts[0] is an alias
                    objectName = parts[0];
                    if (procedureName == null || procedureName.isBlank()) procedureName = parts[1];
                }
            }
        } else {
            objectName = parts[0];
        }

        // ---- Auto-detect schema and type if not yet known ----
        if (username == null || username.isBlank()) {
            ResolvedObject resolved = findObjectInAllSchemas(objectName);
            if (resolved == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "Could not find object '" + objectName + "' in any configured schema"));
            }
            username = resolved.schema;
            if (objectType == null || objectType.isBlank()) {
                objectType = normalizeObjectType(resolved.objectType);
            }
            log.info("Auto-resolved: {} -> schema={}, type={}", objectName, username, objectType);
        }

        // If we have a schema but no type, detect the type
        if (objectType == null || objectType.isBlank()) {
            DbUserConfig tmpUser = findUser(username);
            if (tmpUser != null) {
                try (Connection conn = dbFetcher.getConnection(tmpUser)) {
                    ObjectRecord obj = analysisService.getDictionaryService()
                            .getObjectInfo(conn, username, objectName);
                    if (obj != null) {
                        objectType = normalizeObjectType(obj.objectType());
                    }
                } catch (Exception e) {
                    log.debug("Could not detect type for {}.{}: {}", username, objectName, e.getMessage());
                }
            }
            if (objectType == null || objectType.isBlank()) objectType = "PACKAGE";
        }

        List<DbUserConfig> activeUsers = resolvedUsers != null ? resolvedUsers : config.getDbUsers();
        DbUserConfig user = findUserIn(activeUsers, username);
        if (user == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "User not found in config: " + username));
        }

        log.info("Submitting to queue: schema={}, object={}, type={}, proc={}",
                username, objectName, objectType, procedureName);

        String displayName = "PL/SQL: " + objectName
                + (procedureName != null ? "." + procedureName : "");

        // Create legacy job first so its ID can be included in queue job metadata
        Job legacyJob = jobService.createJob(username, objectName, objectType, procedureName);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("username", username);
        metadata.put("objectName", objectName);
        metadata.put("objectType", objectType);
        metadata.put("procedureName", procedureName);
        metadata.put("project", projectName);
        metadata.put("env", envName);
        metadata.put("legacyJobId", legacyJob.id);

        QueueJob queueJob = queueService.submit(QueueJob.Type.PLSQL_ANALYSIS, displayName, metadata);

        // Link legacy job to queue job for backward compatibility
        legacyJob.queueJobId = queueJob.id;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", queueJob.id);
        response.put("legacyJobId", legacyJob.id);
        response.put("username", username);
        response.put("objectName", objectName);
        response.put("objectType", objectType);
        if (procedureName != null) response.put("procedureName", procedureName);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/analyze-fast")
    public ResponseEntity<Map<String, Object>> triggerFastAnalysis(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        String objectName = body.get("objectName");
        String objectType = body.getOrDefault("objectType", "PACKAGE");
        String procedureName = body.get("procedureName");
        String projectName = body.get("project");
        String envName = body.get("env");

        if (objectName == null || objectName.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "objectName is required"));
        }

        // Smart parsing: split dots (same logic as /analyze)
        String[] parts = objectName.toUpperCase().trim().split("\\.");
        if (parts.length == 3) {
            if (username == null || username.isBlank()) username = parts[0];
            objectName = parts[1];
            if (procedureName == null || procedureName.isBlank()) procedureName = parts[2];
        } else if (parts.length == 2) {
            List<DbUserConfig> effectiveUsers = config.getDbUsers();
            boolean firstIsSchema = effectiveUsers.stream()
                    .anyMatch(u -> u.getUsername().equalsIgnoreCase(parts[0]));
            if (firstIsSchema) {
                if (username == null || username.isBlank()) username = parts[0];
                objectName = parts[1];
            } else {
                objectName = parts[0];
                if (procedureName == null || procedureName.isBlank()) procedureName = parts[1];
            }
        } else {
            objectName = parts[0];
        }

        if (username == null || username.isBlank()) {
            ResolvedObject resolved = findObjectInAllSchemas(objectName);
            if (resolved != null) {
                username = resolved.schema;
                if (objectType == null || objectType.isBlank()) objectType = normalizeObjectType(resolved.objectType);
            }
        }

        if (username == null || username.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "username (schema) is required"));
        }

        AnalysisJobService.Job legacyJob = jobService.createJob(username, objectName, objectType, procedureName);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("username", username.toUpperCase());
        metadata.put("objectName", objectName.toUpperCase());
        metadata.put("objectType", objectType.toUpperCase());
        metadata.put("procedureName", procedureName != null ? procedureName.toUpperCase() : null);
        metadata.put("legacyJobId", legacyJob.id);
        metadata.put("project", projectName);
        metadata.put("env", envName);

        log.info("Submitting FAST analysis: schema={}, object={}, type={}, proc={}",
                username, objectName, objectType, procedureName);

        QueueJob queueJob = queueService.submit(
                QueueJob.Type.PLSQL_FAST_ANALYSIS,
                "Fast PL/SQL: " + objectName + (procedureName != null ? "." + procedureName : ""),
                metadata);
        legacyJob.queueJobId = queueJob.id;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("mode", "fast");
        response.put("jobId", queueJob.id);
        response.put("legacyJobId", legacyJob.id);
        response.put("username", username);
        response.put("objectName", objectName);
        response.put("objectType", objectType);
        if (procedureName != null) response.put("procedureName", procedureName);
        return ResponseEntity.ok(response);
    }

    /**
     * Get the latest/current analysis summary.
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getLatestResult() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            return ResponseEntity.ok(Map.of("status", "no_analysis"));
        }
        Map<String, Object> summary = buildSummary(result);
        return ResponseEntity.ok(summary);
    }

    /**
     * List all saved analyses (history).
     */
    @GetMapping("/history")
    public ResponseEntity<List<Map<String, Object>>> listHistory() {
        return ResponseEntity.ok(persistenceService.listHistory());
    }

    /**
     * Load a specific analysis by name.
     */
    @GetMapping("/history/{name}")
    public ResponseEntity<Map<String, Object>> loadAnalysis(@PathVariable String name) {
        AnalysisResult result = persistenceService.loadByName(name);
        if (result == null) return ResponseEntity.notFound().build();
        // Set as current
        analysisService.setLatestResult(result);
        Map<String, Object> summary = buildSummary(result);
        return ResponseEntity.ok(summary);
    }

    /**
     * Delete a saved analysis by name.
     */
    @DeleteMapping("/history/{name}")
    public ResponseEntity<Map<String, Object>> deleteAnalysis(@PathVariable String name) {
        boolean deleted = persistenceService.delete(name);
        if (!deleted) return ResponseEntity.notFound().build();

        // If the deleted analysis was the current one, clear it
        AnalysisResult current = analysisService.getLatestResult();
        if (current != null && name.equals(current.getName())) {
            analysisService.setLatestResult(null);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("deleted", true);
        response.put("name", name);
        return ResponseEntity.ok(response);
    }

    /** Get stored connection info for a specific analysis. */
    @GetMapping("/history/{name}/connections")
    public ResponseEntity<Map<String, Object>> getAnalysisConnections(@PathVariable String name) {
        Map<String, Object> info = persistenceService.loadConnections(name);
        if (info == null) return ResponseEntity.ok(Map.of("available", false));
        info.put("available", true);
        return ResponseEntity.ok(info);
    }

    /**
     * Legacy versions endpoint — delegates to history.
     */
    @GetMapping("/versions")
    public ResponseEntity<List<Map<String, Object>>> listVersions() {
        return ResponseEntity.ok(persistenceService.listHistory());
    }

    // ---- Call Graph / Call Tree ----

    @GetMapping("/call-graph")
    public ResponseEntity<Object> getFullCallGraph() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        CallGraph graph = result.getCallGraph();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("nodeCount", graph.getNodeCount());
        response.put("edgeCount", graph.getEdgeCount());

        List<Map<String, Object>> nodes = new ArrayList<>();
        for (CallGraphNode node : graph.getAllNodes()) {
            nodes.add(nodeToMap(node));
        }
        response.put("nodes", nodes);

        List<Map<String, Object>> edges = new ArrayList<>();
        for (var edge : graph.getAllEdges()) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("from", edge.getFromNodeId());
            e.put("to", edge.getToNodeId());
            e.put("lineNumber", edge.getCallLineNumber());
            e.put("dynamicSql", edge.isDynamicSql());
            e.put("callType", edge.getCallType());
            edges.add(e);
        }
        response.put("edges", edges);

        return ResponseEntity.ok(response);
    }

    /**
     * Get full detail for a procedure: call tree + stats + table summary + calls + calledBy +
     * variables + parameters + statements.
     */
    @GetMapping("/detail/{procName:.+}")
    public ResponseEntity<Map<String, Object>> getProcedureDetail(@PathVariable String procName) {
        log.info("Detail request for: '{}'", procName);
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            log.warn("Detail: no analysis loaded");
            return ResponseEntity.notFound().build();
        }

        CallGraph graph = result.getCallGraph();
        applyTreeLimits(graph);
        String nodeId = findNodeId(graph, procName);
        if (nodeId == null) {
            log.warn("Detail: node not found for '{}' (graph has {} nodes)", procName, graph.getNodeCount());
            return ResponseEntity.notFound().build();
        }
        log.info("Detail: resolved nodeId='{}', building detail", nodeId);

        CallGraphNode node = graph.getNode(nodeId);
        Map<String, Object> response = new LinkedHashMap<>();

        // Node info
        response.put("id", nodeId);
        if (node != null) {
            response.put("name", node.getProcedureName());
            response.put("packageName", node.getPackageName());
            response.put("schemaName", node.getSchemaName());
            response.put("unitType", node.getUnitType() != null ? node.getUnitType().name() : null);
            response.put("callType", node.getCallType());
            response.put("sourceFile", node.getSourceFile());
            response.put("startLine", node.getStartLine());
            response.put("endLine", node.getEndLine());
        }

        // Stats from call tree traversal
        response.put("stats", graph.getCallTreeStats(nodeId));

        // Full call tree (calls this proc makes)
        response.put("callTree", graph.getCallTree(nodeId));

        // Caller tree (who calls this proc)
        response.put("callerTree", graph.getCallerTree(nodeId));

        // Direct calls (flat list with line numbers)
        List<Map<String, Object>> calls = new ArrayList<>();
        for (var edge : graph.getOutgoingEdges(nodeId)) {
            Map<String, Object> callMap = new LinkedHashMap<>();
            callMap.put("targetId", edge.getToNodeId());
            CallGraphNode targetNode = graph.getNode(edge.getToNodeId());
            callMap.put("name", targetNode != null ? targetNode.getProcedureName() : edge.getToNodeId());
            callMap.put("packageName", targetNode != null ? targetNode.getPackageName() : null);
            callMap.put("lineNumber", edge.getCallLineNumber());
            callMap.put("callType", edge.getCallType());
            callMap.put("dynamicSql", edge.isDynamicSql());
            calls.add(callMap);
        }
        response.put("calls", calls);

        // CalledBy (flat list — who calls this)
        List<Map<String, Object>> calledBy = new ArrayList<>();
        for (var edge : graph.getIncomingEdges(nodeId)) {
            Map<String, Object> callerMap = new LinkedHashMap<>();
            callerMap.put("callerId", edge.getFromNodeId());
            CallGraphNode callerNode = graph.getNode(edge.getFromNodeId());
            callerMap.put("name", callerNode != null ? callerNode.getProcedureName() : edge.getFromNodeId());
            callerMap.put("packageName", callerNode != null ? callerNode.getPackageName() : null);
            callerMap.put("lineNumber", edge.getCallLineNumber());
            calledBy.add(callerMap);
        }
        response.put("calledBy", calledBy);

        // Table operations relevant to this proc's subtree
        Set<String> treeNodeIds = new HashSet<>();
        collectTreeNodeIds(graph.getCallTree(nodeId), treeNodeIds);
        List<Map<String, Object>> tables = new ArrayList<>();
        Set<String> operations = new LinkedHashSet<>();
        int dbOpCount = 0;
        boolean hasTransaction = false;
        int totalLoc = 0;

        try {
            for (var entry : result.getTableOperations().entrySet()) {
                TableOperationSummary summary = entry.getValue();
                // Collect access details relevant to this subtree
                List<Map<String, Object>> relevantDetails = new ArrayList<>();
                for (var detail : summary.getAccessDetails()) {
                    if (detail.getProcedureId() != null && matchesProcIdSet(detail.getProcedureId(), treeNodeIds)) {
                        Map<String, Object> dm = new LinkedHashMap<>();
                        dm.put("procedureId", detail.getProcedureId());
                        dm.put("procedureName", detail.getProcedureName());
                        dm.put("operation", detail.getOperation() != null ? detail.getOperation().name() : null);
                        dm.put("lineNumber", detail.getLineNumber());
                        dm.put("sourceFile", detail.getSourceFile());
                        relevantDetails.add(dm);
                        dbOpCount++;
                    }
                }
                if (!relevantDetails.isEmpty()) {
                    Map<String, Object> tm = new LinkedHashMap<>();
                    tm.put("tableName", summary.getTableName());
                    tm.put("schemaName", summary.getSchemaName());
                    tm.put("tableType", summary.getTableType());
                    Set<String> opStrings = new LinkedHashSet<>();
                    for (Object op : summary.getOperations()) {
                        opStrings.add(op instanceof Enum<?> ? ((Enum<?>) op).name() : String.valueOf(op));
                    }
                    tm.put("operations", opStrings);
                    tm.put("accessCount", relevantDetails.size());
                    tm.put("external", summary.isExternal());
                    tm.put("triggerCount", summary.getTriggers() != null ? summary.getTriggers().size() : 0);
                    tm.put("accessDetails", relevantDetails);
                    tables.add(tm);
                    operations.addAll(opStrings);
                }
            }
        } catch (Exception e) {
            log.error("Error computing table operations for detail: {}", e.getMessage(), e);
        }

        // Estimate LOC from source
        for (String nid : treeNodeIds) {
            CallGraphNode n = graph.getNode(nid);
            if (n != null && n.getEndLine() > 0 && n.getStartLine() > 0) {
                totalLoc += (n.getEndLine() - n.getStartLine() + 1);
            }
        }

        int nodeLoc = 0;
        if (node != null && node.getEndLine() > 0 && node.getStartLine() > 0) {
            nodeLoc = node.getEndLine() - node.getStartLine() + 1;
        }

        response.put("tables", tables);
        response.put("tableCount", tables.size());
        response.put("dbOpCount", dbOpCount);
        response.put("operations", operations);
        response.put("totalLoc", totalLoc);
        response.put("nodeLoc", nodeLoc);
        response.put("hasTransaction", hasTransaction);

        // Flow totals: walk the call tree WITH duplicates (if C is called twice, count it twice)
        Map<String, Object> callTree = graph.getCallTree(nodeId);
        Map<String, Object> flowTotals = computeFlowTotals(callTree, graph, result);
        response.put("flowTotals", flowTotals);

        // Node-only stats
        Map<String, Object> nodeStats = new LinkedHashMap<>();
        List<com.plsqlanalyzer.analyzer.model.CallEdge> directEdges = graph.getOutgoingEdges(nodeId);
        int nodeInternal = 0, nodeExternal = 0, nodeDynamic = 0;
        for (var edge : directEdges) {
            String ct = edge.getCallType();
            if ("INTERNAL".equals(ct)) nodeInternal++;
            else if ("EXTERNAL".equals(ct)) nodeExternal++;
            if (edge.isDynamicSql()) nodeDynamic++;
        }
        nodeStats.put("directCalls", directEdges.size());
        nodeStats.put("internalCalls", nodeInternal);
        nodeStats.put("externalCalls", nodeExternal);
        nodeStats.put("dynamicCalls", nodeDynamic);

        String nodeIdUpper = nodeId.toUpperCase();
        int nodeTableCount = 0;
        int nodeDbOps = 0;
        Set<String> nodeOps = new LinkedHashSet<>();
        List<Map<String, Object>> nodeTables = new ArrayList<>();
        try {
            for (var entry : result.getTableOperations().entrySet()) {
                TableOperationSummary summary = entry.getValue();
                List<Map<String, Object>> nodeDetails = new ArrayList<>();
                for (var detail : summary.getAccessDetails()) {
                    if (matchesProcId(detail.getProcedureId(), nodeIdUpper)) {
                        Map<String, Object> dm = new LinkedHashMap<>();
                        dm.put("procedureId", detail.getProcedureId());
                        dm.put("procedureName", detail.getProcedureName());
                        dm.put("operation", detail.getOperation() != null ? detail.getOperation().name() : null);
                        dm.put("lineNumber", detail.getLineNumber());
                        dm.put("sourceFile", detail.getSourceFile());
                        nodeDetails.add(dm);
                        nodeDbOps++;
                    }
                }
                if (!nodeDetails.isEmpty()) {
                    nodeTableCount++;
                    Set<String> nodeTableOps = new LinkedHashSet<>();
                    for (var dm : nodeDetails) {
                        if (dm.get("operation") != null) nodeTableOps.add((String) dm.get("operation"));
                    }
                    nodeOps.addAll(nodeTableOps);
                    Map<String, Object> ntm = new LinkedHashMap<>();
                    ntm.put("tableName", summary.getTableName());
                    ntm.put("schemaName", summary.getSchemaName());
                    ntm.put("operations", nodeTableOps);
                    ntm.put("accessCount", nodeDetails.size());
                    ntm.put("accessDetails", nodeDetails);
                    nodeTables.add(ntm);
                }
            }
        } catch (Exception e) {
            log.debug("Error computing node-only table stats: {}", e.getMessage());
        }
        nodeStats.put("tableCount", nodeTableCount);
        nodeStats.put("dbOps", nodeDbOps);
        nodeStats.put("operations", nodeOps);
        nodeStats.put("loc", nodeLoc);
        response.put("nodeStats", nodeStats);
        response.put("nodeTables", nodeTables);

        // Parsed procedure detail — parameters, variables, statements
        Map<String, Object> procDetail = findParsedProcedure(result, nodeId, node);
        if (procDetail != null) {
            response.put("parameters", procDetail.get("parameters"));
            response.put("variables", procDetail.get("variables"));
            response.put("statements", procDetail.get("statements"));
            response.put("statementCounts", procDetail.get("statementCounts"));
            response.put("variableCount", procDetail.get("variableCount"));
            response.put("parameterCount", procDetail.get("parameterCount"));
            response.put("statementCount", procDetail.get("statementCount"));
        }

        // Node-level sequence references
        List<Map<String, Object>> nodeSequences = new ArrayList<>();
        try {
            if (result.getSequenceOperations() != null) {
                for (var entry : result.getSequenceOperations().entrySet()) {
                    SequenceOperationSummary seqSummary = entry.getValue();
                    List<Map<String, Object>> seqNodeDetails = new ArrayList<>();
                    for (var sd : seqSummary.getAccessDetails()) {
                        if (matchesProcId(sd.getProcedureId(), nodeIdUpper)) {
                            Map<String, Object> dm = new LinkedHashMap<>();
                            dm.put("procedureId", sd.getProcedureId());
                            dm.put("procedureName", sd.getProcedureName());
                            dm.put("operation", sd.getOperation());
                            dm.put("lineNumber", sd.getLineNumber());
                            seqNodeDetails.add(dm);
                        }
                    }
                    if (!seqNodeDetails.isEmpty()) {
                        Map<String, Object> sm = new LinkedHashMap<>();
                        sm.put("sequenceName", seqSummary.getSequenceName());
                        sm.put("schemaName", seqSummary.getSchemaName());
                        sm.put("operations", seqSummary.getOperations());
                        sm.put("accessCount", seqNodeDetails.size());
                        sm.put("accessDetails", seqNodeDetails);
                        nodeSequences.add(sm);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Error computing node sequence stats: {}", e.getMessage());
        }
        response.put("nodeSequences", nodeSequences);

        // Node-level join references
        List<Map<String, Object>> nodeJoins = new ArrayList<>();
        try {
            if (result.getJoinOperations() != null) {
                for (var entry : result.getJoinOperations().entrySet()) {
                    JoinOperationSummary joinSummary = entry.getValue();
                    List<Map<String, Object>> joinNodeDetails = new ArrayList<>();
                    for (var jd : joinSummary.getAccessDetails()) {
                        if (matchesProcId(jd.getProcedureId(), nodeIdUpper)) {
                            Map<String, Object> dm = new LinkedHashMap<>();
                            dm.put("procedureId", jd.getProcedureId());
                            dm.put("procedureName", jd.getProcedureName());
                            dm.put("joinType", jd.getJoinType());
                            dm.put("onPredicate", jd.getOnPredicate());
                            dm.put("lineNumber", jd.getLineNumber());
                            joinNodeDetails.add(dm);
                        }
                    }
                    if (!joinNodeDetails.isEmpty()) {
                        Map<String, Object> jm = new LinkedHashMap<>();
                        jm.put("leftTable", joinSummary.getLeftTable());
                        jm.put("rightTable", joinSummary.getRightTable());
                        jm.put("joinTypes", joinSummary.getJoinTypes());
                        jm.put("accessCount", joinNodeDetails.size());
                        jm.put("accessDetails", joinNodeDetails);
                        nodeJoins.add(jm);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Error computing node join stats: {}", e.getMessage());
        }
        response.put("nodeJoins", nodeJoins);

        // Node-level cursor references
        List<Map<String, Object>> nodeCursors = new ArrayList<>();
        try {
            if (result.getCursorOperations() != null) {
                for (var entry : result.getCursorOperations().entrySet()) {
                    CursorOperationSummary cursorSummary = entry.getValue();
                    List<Map<String, Object>> cursorNodeDetails = new ArrayList<>();
                    for (var cd : cursorSummary.getAccessDetails()) {
                        if (matchesProcId(cd.getProcedureId(), nodeIdUpper)) {
                            Map<String, Object> dm = new LinkedHashMap<>();
                            dm.put("procedureId", cd.getProcedureId());
                            dm.put("procedureName", cd.getProcedureName());
                            dm.put("operation", cd.getOperation());
                            dm.put("cursorType", cd.getCursorType());
                            dm.put("queryText", cd.getQueryText());
                            dm.put("lineNumber", cd.getLineNumber());
                            cursorNodeDetails.add(dm);
                        }
                    }
                    if (!cursorNodeDetails.isEmpty()) {
                        Map<String, Object> cm = new LinkedHashMap<>();
                        cm.put("cursorName", cursorSummary.getCursorName());
                        cm.put("cursorType", cursorSummary.getCursorType());
                        cm.put("queryText", cursorSummary.getQueryText());
                        cm.put("operations", cursorSummary.getOperations());
                        cm.put("accessCount", cursorNodeDetails.size());
                        cm.put("accessDetails", cursorNodeDetails);
                        nodeCursors.add(cm);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Error computing node cursor stats: {}", e.getMessage());
        }
        response.put("nodeCursors", nodeCursors);

        return ResponseEntity.ok(response);
    }

    /**
     * Find the parsed PlsqlProcedure data from the analysis units.
     * Returns parameters, variables, statements as maps.
     */
    private Map<String, Object> findParsedProcedure(AnalysisResult result, String nodeId, CallGraphNode node) {
        if (result.getUnits() == null || node == null) return null;

        String targetName = node.getProcedureName();
        String targetPkg = node.getPackageName();
        int targetStart = node.getStartLine();

        for (var unit : result.getUnits()) {
            // Match by package name (case-insensitive)
            String unitName = unit.getName();
            if (targetPkg != null && unitName != null && !unitName.equalsIgnoreCase(targetPkg)) continue;

            for (var proc : unit.getProcedures()) {
                // Match by name + start line (both case-insensitive)
                boolean nameMatch = proc.getName() != null && targetName != null
                        && proc.getName().equalsIgnoreCase(targetName);
                boolean lineMatch = targetStart > 0 && proc.getStartLine() == targetStart;
                if (nameMatch || lineMatch) {
                    Map<String, Object> detail = new LinkedHashMap<>();
                    detail.put("parameters", proc.getParameters());
                    detail.put("variables", proc.getVariables());
                    detail.put("statements", proc.getStatements());
                    detail.put("parameterCount", proc.getParameters().size());
                    detail.put("variableCount", proc.getVariables().size());
                    detail.put("statementCount", proc.getStatements().size());

                    // Count statements by type
                    Map<String, Integer> counts = new LinkedHashMap<>();
                    for (var stmt : proc.getStatements()) {
                        counts.merge(stmt.getType(), 1, Integer::sum);
                    }
                    detail.put("statementCounts", counts);
                    return detail;
                }
            }
        }
        return null;
    }

    private void collectTreeNodeIds(Map<String, Object> tree, Set<String> ids) {
        Object id = tree.get("id");
        if (id != null) ids.add(id.toString().toUpperCase());
        Object children = tree.get("children");
        if (children instanceof List<?> list) {
            for (Object child : list) {
                if (child instanceof Map<?, ?> childMap) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> cm = (Map<String, Object>) childMap;
                    collectTreeNodeIds(cm, ids);
                }
            }
        }
    }

    /**
     * Walk the call tree WITH duplicates to compute true flow totals.
     * If proc C is called from A and from B, C's LOC/tables/ops count TWICE.
     * This gives the actual cumulative cost of the entire execution flow.
     */
    private Map<String, Object> computeFlowTotals(Map<String, Object> tree, CallGraph graph, AnalysisResult result) {
        int[] counters = new int[7]; // [loc, tables, dbOps, internal, external, dynamic, nodes]
        Set<String> allTables = new HashSet<>();
        computeFlowTotalsRecursive(tree, graph, result, counters, allTables, 0);

        Map<String, Object> ft = new LinkedHashMap<>();
        ft.put("loc", counters[0]);
        ft.put("tables", allTables.size());
        ft.put("dbOps", counters[2]);
        ft.put("internalCalls", counters[3]);
        ft.put("externalCalls", counters[4]);
        ft.put("dynamicCalls", counters[5]);
        ft.put("totalNodes", counters[6]);
        return ft;
    }

    @SuppressWarnings("unchecked")
    private void computeFlowTotalsRecursive(Map<String, Object> treeNode, CallGraph graph,
                                             AnalysisResult result, int[] counters,
                                             Set<String> allTables, int depth) {
        String nid = treeNode.get("id") != null ? treeNode.get("id").toString() : null;
        if (nid == null) return;

        counters[6]++; // node count (with duplicates)

        // Add this node's LOC (every occurrence counts)
        CallGraphNode cgNode = graph.getNode(nid);
        if (cgNode != null && cgNode.getEndLine() > 0 && cgNode.getStartLine() > 0) {
            counters[0] += (cgNode.getEndLine() - cgNode.getStartLine() + 1);
        }

        // Add this node's table operations
        String nidUpper = nid.toUpperCase();
        if (result.getTableOperations() != null) {
            for (var entry : result.getTableOperations().entrySet()) {
                String tableName = entry.getKey();
                TableOperationSummary summary = entry.getValue();
                for (var detail : summary.getAccessDetails()) {
                    if (matchesProcId(detail.getProcedureId(), nidUpper)) {
                        allTables.add(tableName.toUpperCase());
                        counters[2]++; // dbOps (with duplicates)
                    }
                }
            }
        }

        // Count edge types from this tree node's children
        Object children = treeNode.get("children");
        if (children instanceof List<?> list) {
            for (Object child : list) {
                if (child instanceof Map<?, ?> childMap) {
                    Map<String, Object> cm = (Map<String, Object>) childMap;
                    String callType = cm.get("callType") != null ? cm.get("callType").toString() : "";
                    Boolean dynamic = cm.get("dynamicSql") instanceof Boolean b ? b : false;
                    if ("INTERNAL".equals(callType)) counters[3]++;
                    else if ("EXTERNAL".equals(callType)) counters[4]++;
                    if (dynamic) counters[5]++;

                    Boolean circular = cm.get("circular") instanceof Boolean bc ? bc : false;
                    if (!circular) {
                        computeFlowTotalsRecursive(cm, graph, result, counters, allTables, depth + 1);
                    }
                }
            }
        }
    }

    /**
     * Check if a procedureId from table access details matches any ID in a set.
     * Handles mismatches like: nodeId = "FN_PROC" vs procedureId = "SCHEMA.PKG.FN_PROC".
     * Also tries last segment match (proc name only).
     */
    private boolean matchesProcIdSet(String procedureId, Set<String> nodeIds) {
        if (procedureId == null) return false;
        String upper = procedureId.toUpperCase();
        // Exact match
        if (nodeIds.contains(upper)) return true;
        // Try suffix: "SCHEMA.PKG.PROC" → check if "PKG.PROC" or "PROC" is in set
        int dot1 = upper.indexOf('.');
        if (dot1 >= 0) {
            String suffix = upper.substring(dot1 + 1);
            if (nodeIds.contains(suffix)) return true;
            int dot2 = suffix.indexOf('.');
            if (dot2 >= 0 && nodeIds.contains(suffix.substring(dot2 + 1))) return true;
        }
        // Try if any nodeId ends with this procedureId's last segment
        String lastSeg = upper.contains(".") ? upper.substring(upper.lastIndexOf('.') + 1) : upper;
        for (String nid : nodeIds) {
            String nidLast = nid.contains(".") ? nid.substring(nid.lastIndexOf('.') + 1) : nid;
            if (nidLast.equals(lastSeg)) return true;
        }
        return false;
    }

    /** Single-ID version of matchesProcIdSet */
    private boolean matchesProcId(String procedureId, String nodeId) {
        if (procedureId == null || nodeId == null) return false;
        return matchesProcIdSet(procedureId, Set.of(nodeId.toUpperCase()));
    }

    @GetMapping("/call-tree/{procName:.+}")
    public ResponseEntity<Map<String, Object>> getCallTree(@PathVariable String procName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        CallGraph graph = result.getCallGraph();
        applyTreeLimits(graph);
        String nodeId = findNodeId(graph, procName);
        if (nodeId == null) return ResponseEntity.notFound().build();

        return ResponseEntity.ok(graph.getCallTree(nodeId));
    }

    @GetMapping("/call-tree/{procName:.+}/callers")
    public ResponseEntity<Map<String, Object>> getCallerTree(@PathVariable String procName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        CallGraph graph = result.getCallGraph();
        applyTreeLimits(graph);
        String nodeId = findNodeId(graph, procName);
        if (nodeId == null) return ResponseEntity.notFound().build();

        return ResponseEntity.ok(graph.getCallerTree(nodeId));
    }

    // ---- Tables ----

    @GetMapping("/tables")
    public ResponseEntity<Collection<TableOperationSummary>> getTableOperations() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(result.getTableOperations().values());
    }

    @GetMapping("/tables/{tableName}")
    public ResponseEntity<TableOperationSummary> getTableDetail(@PathVariable String tableName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        TableOperationSummary summary = result.getTableOperations().get(tableName.toUpperCase());
        if (summary == null) {
            for (var entry : result.getTableOperations().entrySet()) {
                if (entry.getKey().contains(tableName.toUpperCase())) {
                    summary = entry.getValue();
                    break;
                }
            }
        }
        if (summary == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(summary);
    }

    /**
     * Get pre-cached table metadata (columns, constraints, indexes) from analysis result.
     * Avoids live DB queries — data was fetched during analysis.
     */
    @GetMapping("/tables/{tableName}/metadata")
    public ResponseEntity<Map<String, Object>> getTableMetadata(@PathVariable String tableName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        OracleDictionaryService.TableMetadata meta = result.getTableMetadata() != null
                ? result.getTableMetadata().get(tableName.toUpperCase()) : null;
        if (meta == null) {
            // Try partial match
            if (result.getTableMetadata() != null) {
                for (var entry : result.getTableMetadata().entrySet()) {
                    if (entry.getKey().contains(tableName.toUpperCase())) {
                        meta = entry.getValue();
                        break;
                    }
                }
            }
        }
        if (meta == null) return ResponseEntity.notFound().build();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("tableName", meta.tableName());
        response.put("schema", meta.owner());
        response.put("found", true);
        response.put("isView", meta.isView());
        response.put("columnCount", meta.columns().size());

        // Columns
        List<Map<String, Object>> columns = new ArrayList<>();
        for (var col : meta.columns()) {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("columnName", col.columnName());
            c.put("dataType", col.dataType());
            c.put("dataLength", col.dataLength());
            c.put("dataPrecision", col.dataPrecision());
            c.put("dataScale", col.dataScale());
            c.put("nullable", col.nullable());
            c.put("columnId", col.columnId());
            c.put("dataDefault", col.dataDefault());
            columns.add(c);
        }
        response.put("columns", columns);

        // Constraints
        List<Map<String, Object>> constraints = new ArrayList<>();
        for (var con : meta.constraints()) {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("constraintName", con.constraintName());
            c.put("constraintType", con.constraintType());
            c.put("columnName", con.columnName());
            c.put("position", con.position());
            c.put("refConstraint", con.refConstraint());
            constraints.add(c);
        }
        response.put("constraints", constraints);

        // Indexes
        List<Map<String, Object>> indexes = new ArrayList<>();
        for (var idx : meta.indexes()) {
            Map<String, Object> i = new LinkedHashMap<>();
            i.put("indexName", idx.indexName());
            i.put("uniqueness", idx.uniqueness());
            i.put("columnName", idx.columnName());
            i.put("position", idx.position());
            indexes.add(i);
        }
        response.put("indexes", indexes);

        // View definition (only present for views)
        if (meta.isView() && meta.viewDefinition() != null) {
            response.put("viewDefinition", meta.viewDefinition());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/tables/{tableName}/triggers")
    public ResponseEntity<List<Map<String, Object>>> getTableTriggers(@PathVariable String tableName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        TableOperationSummary summary = result.getTableOperations().get(tableName.toUpperCase());
        if (summary == null) {
            for (var entry : result.getTableOperations().entrySet()) {
                if (entry.getKey().contains(tableName.toUpperCase())) {
                    summary = entry.getValue();
                    break;
                }
            }
        }
        if (summary == null || summary.getTriggers() == null || summary.getTriggers().isEmpty()) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        List<Map<String, Object>> triggers = new ArrayList<>();
        for (var tr : summary.getTriggers()) {
            Map<String, Object> tm = new LinkedHashMap<>();
            tm.put("triggerName", tr.getTriggerName());
            tm.put("triggerOwner", tr.getTriggerOwner());
            tm.put("triggeringEvent", tr.getTriggeringEvent());
            tm.put("triggerType", tr.getTriggerType());
            tm.put("status", tr.getStatus());
            tm.put("calledProcedures", tr.getCalledProcedures());
            tm.put("triggerBody", tr.getTriggerBody());
            tm.put("tableOn", tableName.toUpperCase());

            // Extract referenced tables + DML operations from trigger body
            List<Map<String, Object>> referencedTableOps = new ArrayList<>();
            List<String> referencedTables = new ArrayList<>();
            if (tr.getTriggerBody() != null) {
                String body = tr.getTriggerBody().toUpperCase();
                for (var tableEntry : result.getTableOperations().entrySet()) {
                    String tName = tableEntry.getKey().toUpperCase();
                    if (body.contains(tName) && !tName.equals(tableName.toUpperCase())) {
                        referencedTables.add(tableEntry.getKey());
                        // Detect which DML ops this trigger performs on the referenced table
                        List<String> ops = new ArrayList<>();
                        if (body.contains("INSERT INTO " + tName) || body.contains("INSERT INTO\n" + tName)
                                || body.contains("INSERT  INTO " + tName))
                            ops.add("INSERT");
                        if (body.contains("UPDATE " + tName + " ") || body.contains("UPDATE " + tName + "\n")
                                || body.contains("UPDATE " + tName + "\r"))
                            ops.add("UPDATE");
                        if (body.contains("DELETE FROM " + tName) || body.contains("DELETE " + tName))
                            ops.add("DELETE");
                        if (body.contains("SELECT") && body.contains("FROM " + tName))
                            ops.add("SELECT");
                        if (ops.isEmpty() && body.contains(tName)) ops.add("REFERENCE");
                        Map<String, Object> refEntry = new LinkedHashMap<>();
                        refEntry.put("tableName", tableEntry.getKey());
                        refEntry.put("operations", ops);
                        referencedTableOps.add(refEntry);
                    }
                }
            }
            tm.put("referencedTables", referencedTables);
            tm.put("referencedTableOps", referencedTableOps);
            triggers.add(tm);
        }
        return ResponseEntity.ok(triggers);
    }

    // ---- Sequences ----

    @GetMapping("/sequences")
    public ResponseEntity<Collection<SequenceOperationSummary>> getSequenceOperations() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null || result.getSequenceOperations() == null) return ResponseEntity.ok(Collections.emptyList());
        return ResponseEntity.ok(result.getSequenceOperations().values());
    }

    // ---- Joins ----

    @GetMapping("/joins")
    public ResponseEntity<Collection<JoinOperationSummary>> getJoinOperations() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null || result.getJoinOperations() == null) return ResponseEntity.ok(Collections.emptyList());
        return ResponseEntity.ok(result.getJoinOperations().values());
    }

    // ---- Cursors ----

    @GetMapping("/cursors")
    public ResponseEntity<Collection<CursorOperationSummary>> getCursorOperations() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null || result.getCursorOperations() == null) return ResponseEntity.ok(Collections.emptyList());
        return ResponseEntity.ok(result.getCursorOperations().values());
    }

    // ---- Errors ----

    @GetMapping("/errors")
    public ResponseEntity<List<Map<String, Object>>> getParseErrors() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null || result.getUnits() == null) return ResponseEntity.ok(Collections.emptyList());

        List<Map<String, Object>> errors = new ArrayList<>();
        for (var unit : result.getUnits()) {
            if (unit.getParseErrors() != null && !unit.getParseErrors().isEmpty()) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("unitName", unit.getName());
                entry.put("schemaName", unit.getSchemaName());
                entry.put("unitType", unit.getUnitType() != null ? unit.getUnitType().name() : null);
                entry.put("sourceFile", unit.getSourceFile());
                entry.put("errorCount", unit.getParseErrors().size());
                entry.put("errors", unit.getParseErrors());
                errors.add(entry);
            }
        }
        return ResponseEntity.ok(errors);
    }

    // ---- Procedures ----

    @GetMapping("/procedures")
    public ResponseEntity<List<Map<String, Object>>> listProcedures() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        // Compute reachable set from entry point
        CallGraph graph = result.getCallGraph();
        applyTreeLimits(graph);
        Set<String> reachableIds = new HashSet<>();
        try {
            String rootId = findRootId(result, graph);
            if (rootId != null) {
                collectTreeNodeIds(graph.getCallTree(rootId), reachableIds);
            }
        } catch (Exception e) {
            log.debug("Error computing reachable set: {}", e.getMessage());
        }

        List<Map<String, Object>> procs = new ArrayList<>();
        for (CallGraphNode node : graph.getAllNodes()) {
            if (node.getProcedureName() == null) continue;
            Map<String, Object> p = nodeToMap(node);
            p.put("reachable", reachableIds.contains(node.getId().toUpperCase()));
            procs.add(p);
        }
        return ResponseEntity.ok(procs);
    }

    // ---- Source ----

    @GetMapping("/source/{owner}/{objectName:.+}")
    public ResponseEntity<Map<String, Object>> getSource(
            @PathVariable String owner, @PathVariable String objectName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        String key = owner.toUpperCase() + "." + objectName.toUpperCase();
        List<String> lines = result.getSourceMap() != null ? result.getSourceMap().get(key) : null;
        if (lines == null || lines.isEmpty()) return ResponseEntity.notFound().build();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("owner", owner.toUpperCase());
        response.put("objectName", objectName.toUpperCase());
        response.put("content", String.join("\n", lines));
        response.put("lineCount", lines.size());
        return ResponseEntity.ok(response);
    }

    // ---- References (used-by) ----

    @GetMapping("/references/{objectName:.+}")
    public ResponseEntity<List<Map<String, String>>> getReferences(
            @PathVariable String objectName,
            @RequestParam(required = false) String owner) {
        if (owner == null && config.getDbUsers().isEmpty()) return ResponseEntity.ok(Collections.emptyList());
        String resolvedOwner = owner != null ? owner : config.getDbUsers().get(0).getUsername();
        DbUserConfig user = findUser(resolvedOwner);
        if (user == null) return ResponseEntity.ok(Collections.emptyList());

        try (Connection conn = dbFetcher.getConnection(user)) {
            List<DependencyRecord> refs = analysisService.getReferences(conn, resolvedOwner, objectName);
            List<Map<String, String>> mapped = new ArrayList<>();
            for (DependencyRecord r : refs) {
                Map<String, String> m = new LinkedHashMap<>();
                m.put("owner", r.referencedOwner());
                m.put("name", r.referencedName());
                m.put("type", r.referencedType());
                mapped.add(m);
            }
            return ResponseEntity.ok(mapped);
        } catch (Exception e) {
            log.error("Failed to get references: {}", e.getMessage());
            return ResponseEntity.ok(Collections.emptyList());
        }
    }

    // ---- Search ----

    @GetMapping("/search")
    public ResponseEntity<List<Map<String, Object>>> search(
            @RequestParam String q,
            @RequestParam(defaultValue = "all") String type) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.ok(Collections.emptyList());

        List<Map<String, Object>> results = new ArrayList<>();
        String query = q.toUpperCase();

        if ("procedure".equals(type) || "all".equals(type)) {
            for (CallGraphNode node : result.getCallGraph().searchNodes(query)) {
                Map<String, Object> r = nodeToMap(node);
                r.put("type", "procedure");
                results.add(r);
            }
        }

        if ("table".equals(type) || "all".equals(type)) {
            for (var entry : result.getTableOperations().entrySet()) {
                if (entry.getKey().contains(query)) {
                    TableOperationSummary s = entry.getValue();
                    Map<String, Object> r = new LinkedHashMap<>();
                    r.put("type", "table");
                    r.put("name", s.getTableName());
                    r.put("schemaName", s.getSchemaName());
                    r.put("operations", s.getOperations());
                    r.put("accessCount", s.getAccessDetails().size());
                    r.put("external", s.isExternal());
                    results.add(r);
                }
            }
        }

        return ResponseEntity.ok(results);
    }

    // ---- Jobs ----

    /** List all analysis jobs (newest first) */
    @GetMapping("/jobs")
    public ResponseEntity<List<Map<String, Object>>> listJobs() {
        return ResponseEntity.ok(jobService.listJobs());
    }

    /** Get a specific job status */
    @GetMapping("/jobs/{jobId}")
    public ResponseEntity<Map<String, Object>> getJob(@PathVariable String jobId) {
        Job job = jobService.getJob(jobId);
        if (job == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(job.toMap());
    }

    /** Cancel a running job */
    @PostMapping("/jobs/{jobId}/cancel")
    public ResponseEntity<Map<String, Object>> cancelJob(@PathVariable String jobId) {
        boolean cancelled = jobService.cancel(jobId);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("jobId", jobId);
        response.put("cancelled", cancelled);
        return ResponseEntity.ok(response);
    }

    // ---- Helpers ----

    /** Apply configured tree limits to the call graph */
    private void applyTreeLimits(CallGraph graph) {
        graph.setMaxTreeDepth(treeMaxDepth);
        graph.setMaxTreeNodes(treeMaxNodes);
    }

    private String findNodeId(CallGraph graph, String name) {
        String upper = name.toUpperCase();
        // Strip overload suffix for matching (e.g. SCHEMA.PKG.PROC/3IN_2OUT → SCHEMA.PKG.PROC)
        String cleanUpper = upper.contains("/") ? upper.substring(0, upper.indexOf('/')) : upper;

        // Exact match (with or without overload suffix)
        if (graph.getNode(upper) != null) return upper;
        if (!upper.equals(cleanUpper) && graph.getNode(cleanUpper) != null) return cleanUpper;

        // Try baseId match for overloaded procs
        for (CallGraphNode node : graph.getAllNodes()) {
            String nid = node.getId().toUpperCase();
            String baseId = (node.getBaseId() != null ? node.getBaseId() : node.getId()).toUpperCase();
            if (nid.equals(upper) || baseId.equals(cleanUpper)) return node.getId();
        }

        // Try suffix match: e.g., "PKG.PROC" matches node "SCHEMA.PKG.PROC"
        for (CallGraphNode node : graph.getAllNodes()) {
            String nid = node.getId().toUpperCase();
            String baseId = (node.getBaseId() != null ? node.getBaseId() : node.getId()).toUpperCase();
            if (nid.endsWith("." + cleanUpper) || baseId.endsWith("." + cleanUpper)) return node.getId();
        }

        // Fuzzy prefix match: strip PL/SQL prefixes (P_, FN_, PROC_, etc.) and compare core name
        String[] parts = cleanUpper.split("\\.");
        if (parts.length >= 2) {
            String procPart = parts[parts.length - 1];
            String schemaPart = parts[0];
            String procCore = stripProcPrefix(procPart);
            for (CallGraphNode node : graph.getAllNodes()) {
                if (node.getProcedureName() == null) continue;
                if (schemaPart.length() > 0 && !schemaPart.equalsIgnoreCase(node.getSchemaName())) continue;
                String np = node.getProcedureName().toUpperCase();
                String npCore = stripProcPrefix(np);
                if (npCore.equals(procCore)
                        || np.endsWith(procPart) || procPart.endsWith(np)
                        || np.contains(procPart) || procPart.contains(np)) {
                    return node.getId();
                }
            }
        }

        // Try partial search — prefer node whose procedure name matches exactly
        List<CallGraphNode> matches = graph.searchNodes(cleanUpper);
        if (!matches.isEmpty()) {
            String lastPart = cleanUpper.contains(".") ? cleanUpper.substring(cleanUpper.lastIndexOf('.') + 1) : cleanUpper;
            for (CallGraphNode m : matches) {
                if (m.getProcedureName() != null && m.getProcedureName().equalsIgnoreCase(lastPart)) {
                    return m.getId();
                }
            }
            return matches.get(0).getId();
        }

        // Try dropping the last segment and matching package level
        if (parts.length >= 3) {
            String pkgId = parts[0] + "." + parts[1];
            CallGraphNode bestMatch = null;
            for (CallGraphNode node : graph.getAllNodes()) {
                String nid = node.getId().toUpperCase();
                if (nid.startsWith(pkgId + ".")) {
                    if (bestMatch == null) bestMatch = node;
                    if (nid.endsWith("." + parts[parts.length - 1])) {
                        return node.getId();
                    }
                }
            }
            if (bestMatch != null) return bestMatch.getId();
        }

        return null;
    }

    private Map<String, Object> nodeToMap(CallGraphNode node) {
        Map<String, Object> n = new LinkedHashMap<>();
        n.put("id", node.getId());
        n.put("name", node.getProcedureName());
        n.put("packageName", node.getPackageName());
        n.put("schemaName", node.getSchemaName());
        n.put("sourceFile", node.getSourceFile());
        n.put("startLine", node.getStartLine());
        n.put("endLine", node.getEndLine());
        n.put("unitType", node.getUnitType() != null ? node.getUnitType().name() : null);
        n.put("callType", node.getCallType());
        n.put("placeholder", node.isPlaceholder());
        if (node.getParamSignature() != null) n.put("paramSignature", node.getParamSignature());
        if (node.getParamCount() > 0) n.put("paramCount", node.getParamCount());
        if (node.getBaseId() != null && !node.getBaseId().equals(node.getId())) {
            n.put("baseId", node.getBaseId());
        }
        return n;
    }

    private Map<String, Object> buildSummary(AnalysisResult result) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("status", "complete");
        summary.put("name", result.getName());
        summary.put("timestamp", result.getTimestamp().toString());
        summary.put("entrySchema", result.getEntrySchema());
        summary.put("entryObjectName", result.getEntryObjectName());
        summary.put("entryObjectType", result.getEntryObjectType());
        summary.put("entryProcedure", result.getEntryProcedure());
        summary.put("fileCount", result.getFileCount());
        summary.put("procedureCount", result.getProcedureCount());
        summary.put("errorCount", result.getErrorCount());
        summary.put("version", result.getVersion());
        summary.put("analysisMode", result.getAnalysisMode() != null ? result.getAnalysisMode() : "STATIC");
        summary.put("claudeIteration", result.getClaudeIteration());
        summary.put("claudeEnrichedAt", result.getClaudeEnrichedAt());

        // Fast path: use precomputed metadata when sections aren't loaded yet
        if (result instanceof LazyAnalysisResult lazy && lazy.getMetadataCache() != null && !lazy.isGraphLoaded()) {
            Map<String, Object> meta = lazy.getMetadataCache();
            summary.put("nodeCount", metaInt(meta, "nodeCount"));
            summary.put("edgeCount", metaInt(meta, "edgeCount"));
            summary.put("tableCount", metaInt(meta, "tableCount"));
            summary.put("sequenceCount", metaInt(meta, "sequenceCount"));
            summary.put("joinCount", metaInt(meta, "joinCount"));
            summary.put("cursorCount", metaInt(meta, "cursorCount"));
            summary.put("flowNodeCount", metaInt(meta, "flowNodeCount"));
            summary.put("flowTableCount", metaInt(meta, "flowTableCount"));
            summary.put("flowSequenceCount", metaInt(meta, "flowSequenceCount"));
            summary.put("flowJoinCount", metaInt(meta, "flowJoinCount"));
            summary.put("flowCursorCount", metaInt(meta, "flowCursorCount"));
            summary.put("flowErrorCount", metaInt(meta, "flowErrorCount"));
            log.debug("buildSummary fast-path from metadata for '{}'", result.getName());
            return summary;
        }

        // Full computation path — loads graph and ops sections as needed
        summary.put("nodeCount", result.getCallGraph().getNodeCount());
        summary.put("edgeCount", result.getCallGraph().getEdgeCount());
        summary.put("tableCount", result.getTableOperations().size());
        summary.put("sequenceCount", result.getSequenceOperations() != null ? result.getSequenceOperations().size() : 0);
        summary.put("joinCount", result.getJoinOperations() != null ? result.getJoinOperations().size() : 0);
        summary.put("cursorCount", result.getCursorOperations() != null ? result.getCursorOperations().size() : 0);

        // Flow-level stats from root entry point
        try {
            CallGraph graph = result.getCallGraph();
            applyTreeLimits(graph);
            String rootId = findRootId(result, graph);
            if (rootId != null) {
                summary.put("rootId", rootId);
                Map<String, Object> flowStats = graph.getCallTreeStats(rootId);
                summary.put("flowStats", flowStats);

                Set<String> treeNodeIds = new HashSet<>();
                collectTreeNodeIds(graph.getCallTree(rootId), treeNodeIds);

                int flowTables = 0;
                int flowDbOps = 0;
                Set<String> flowOps = new LinkedHashSet<>();
                for (var entry : result.getTableOperations().entrySet()) {
                    TableOperationSummary ts = entry.getValue();
                    boolean relevant = false;
                    for (var d : ts.getAccessDetails()) {
                        if (d.getProcedureId() != null && matchesProcIdSet(d.getProcedureId(), treeNodeIds)) {
                            relevant = true;
                            flowDbOps++;
                        }
                    }
                    if (relevant) {
                        flowTables++;
                        for (Object op : ts.getOperations()) {
                            flowOps.add(op instanceof Enum<?> ? ((Enum<?>) op).name() : String.valueOf(op));
                        }
                    }
                }

                int flowLoc = 0;
                for (String nid : treeNodeIds) {
                    CallGraphNode n = graph.getNode(nid);
                    if (n != null && n.getEndLine() > 0 && n.getStartLine() > 0) {
                        flowLoc += (n.getEndLine() - n.getStartLine() + 1);
                    }
                }

                summary.put("flowTableCount", flowTables);
                summary.put("flowDbOps", flowDbOps);
                summary.put("flowOperations", flowOps);
                summary.put("flowLoc", flowLoc);
            }
        } catch (Exception e) {
            log.debug("Error computing flow stats for summary: {}", e.getMessage());
        }

        return summary;
    }

    private static int metaInt(Map<String, Object> meta, String key) {
        Object v = meta.get(key);
        if (v == null) return 0;
        if (v instanceof Number n) return n.intValue();
        try { return Integer.parseInt(v.toString()); } catch (Exception e) { return 0; }
    }

    /** Find the root node ID from analysis metadata */
    private String findRootId(AnalysisResult result, CallGraph graph) {
        String schema = result.getEntrySchema() != null ? result.getEntrySchema().toUpperCase() : "";
        String obj = result.getEntryObjectName() != null ? result.getEntryObjectName().toUpperCase() : "";
        String proc = result.getEntryProcedure() != null ? result.getEntryProcedure().toUpperCase() : "";

        if (!proc.isEmpty()) {
            // 1) Exact 3-part: SCHEMA.PKG.PROC
            String exact = schema + "." + obj + "." + proc;
            if (graph.getNode(exact) != null) return exact;

            // 2) Exact proc name in this schema (any package)
            for (CallGraphNode node : graph.getAllNodes()) {
                if (node.getProcedureName() != null
                        && node.getProcedureName().equalsIgnoreCase(proc)
                        && schema.equalsIgnoreCase(node.getSchemaName())) {
                    return node.getId();
                }
            }

            // 3) Fuzzy match: strip common PL/SQL prefixes (P_, FN_, PC_, PROC_, F_, FUN_)
            //    and compare the core name (SAVE_CLAIM_REG matches across P_ / FN_ prefixes)
            String procCore = stripProcPrefix(proc);
            for (CallGraphNode node : graph.getAllNodes()) {
                if (node.getProcedureName() == null || !schema.equalsIgnoreCase(node.getSchemaName())) continue;
                String np = node.getProcedureName().toUpperCase();
                String npCore = stripProcPrefix(np);
                if (npCore.equals(procCore)
                        || np.endsWith(proc) || proc.endsWith(np)
                        || np.contains(proc) || proc.contains(np)) {
                    return node.getId();
                }
            }

            // 4) findNodeId fuzzy (suffix, baseId, partial)
            String found = findNodeId(graph, exact);
            if (found != null) return found;
        }

        // Try 2-part (package-level node)
        String pkgId = schema + "." + obj;
        if (graph.getNode(pkgId) != null) return pkgId;

        // Fallback: use findNodeId
        return findNodeId(graph, schema.isEmpty() ? obj : pkgId);
    }

    private static final String[] PROC_PREFIXES = {"P_", "PC_", "FN_", "F_", "PROC_", "FUN_", "FUNC_"};

    private String stripProcPrefix(String name) {
        String upper = name.toUpperCase();
        for (String prefix : PROC_PREFIXES) {
            if (upper.startsWith(prefix)) return upper.substring(prefix.length());
        }
        return upper;
    }

    /**
     * Search for an object across all configured schemas, returning its schema and type.
     */
    private ResolvedObject findObjectInAllSchemas(String objectName) {
        for (DbUserConfig user : config.getDbUsers()) {
            try (Connection conn = dbFetcher.getConnection(user)) {
                ObjectRecord obj = analysisService.getDictionaryService()
                        .getObjectInfo(conn, user.getUsername(), objectName.toUpperCase());
                if (obj != null) {
                    log.info("Found {} in schema {} as {}", objectName, user.getUsername(), obj.objectType());
                    return new ResolvedObject(user.getUsername().toUpperCase(), obj.objectType());
                }
            } catch (Exception e) {
                log.debug("Error checking {} in {}: {}", objectName, user.getUsername(), e.getMessage());
            }
        }
        return null;
    }

    /**
     * Normalize object type — convert "PACKAGE BODY" to "PACKAGE" for analysis purposes,
     * since we always want to analyze the body for call tracing.
     */
    private String normalizeObjectType(String objectType) {
        if (objectType == null) return "PACKAGE";
        return switch (objectType.toUpperCase()) {
            case "PACKAGE BODY", "PACKAGE" -> "PACKAGE";
            case "PROCEDURE" -> "PROCEDURE";
            case "FUNCTION" -> "FUNCTION";
            case "TRIGGER" -> "TRIGGER";
            default -> "PACKAGE";
        };
    }

    private DbUserConfig findUser(String username) {
        return findUserIn(config.getDbUsers(), username);
    }

    private DbUserConfig findUserIn(List<DbUserConfig> users, String username) {
        return users.stream()
                .filter(u -> u.getUsername().equalsIgnoreCase(username))
                .findFirst()
                .orElse(null);
    }

    /**
     * Helper to hold a resolved schema + object type pair.
     */
    private static class ResolvedObject {
        final String schema;
        final String objectType;

        ResolvedObject(String schema, String objectType) {
            this.schema = schema;
            this.objectType = objectType;
        }
    }
}
