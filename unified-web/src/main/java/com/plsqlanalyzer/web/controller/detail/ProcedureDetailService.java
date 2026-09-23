package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.*;

/** Assembles the full procedure detail response (extracted from AnalyzerController). */
@Service
public class ProcedureDetailService {

    private static final Logger log = LoggerFactory.getLogger(ProcedureDetailService.class);

    private final AnalysisService analysisService;
    private final CallGraphNavigator navigator;
    private final GraphMapHelper graphMapHelper;
    private final FlowTotalsService flowTotalsService;
    private final NodeStatsService nodeStatsService;
    private final NodeReferenceService nodeReferenceService;

    public ProcedureDetailService(AnalysisService analysisService, CallGraphNavigator navigator,
                                  GraphMapHelper graphMapHelper, FlowTotalsService flowTotalsService,
                                  NodeStatsService nodeStatsService, NodeReferenceService nodeReferenceService) {
        this.analysisService = analysisService;
        this.navigator = navigator;
        this.graphMapHelper = graphMapHelper;
        this.flowTotalsService = flowTotalsService;
        this.nodeStatsService = nodeStatsService;
        this.nodeReferenceService = nodeReferenceService;
    }

    public ResponseEntity<Map<String, Object>> getProcedureDetail(String procName) {
        log.info("Detail request for: '{}'", procName);
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            log.warn("Detail: no analysis loaded");
            return ResponseEntity.notFound().build();
        }

        CallGraph graph = result.getCallGraph();
        navigator.applyTreeLimits(graph);
        String nodeId = navigator.findNodeId(graph, procName);
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

        response.put("stats", graph.getCallTreeStats(nodeId));
        response.put("callTree", graph.getCallTree(nodeId));
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
        graphMapHelper.collectTreeNodeIds(graph.getCallTree(nodeId), treeNodeIds);
        List<Map<String, Object>> tables = new ArrayList<>();
        Set<String> operations = new LinkedHashSet<>();
        int dbOpCount = 0;
        boolean hasTransaction = false;
        int totalLoc = 0;

        try {
            for (var entry : result.getTableOperations().entrySet()) {
                TableOperationSummary summary = entry.getValue();
                List<Map<String, Object>> relevantDetails = new ArrayList<>();
                for (var detail : summary.getAccessDetails()) {
                    if (detail.getProcedureId() != null && graphMapHelper.matchesProcIdSet(detail.getProcedureId(), treeNodeIds)) {
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

        // Flow totals: walk the call tree WITH duplicates
        Map<String, Object> callTree = graph.getCallTree(nodeId);
        Map<String, Object> flowTotals = flowTotalsService.computeFlowTotals(callTree, graph, result);
        response.put("flowTotals", flowTotals);

        // Node-only stats + tables
        nodeStatsService.populateNodeStats(response, graph, nodeId, result, nodeLoc);

        // Parsed procedure detail — parameters, variables, statements
        Map<String, Object> procDetail = nodeStatsService.findParsedProcedure(result, nodeId, node);
        if (procDetail != null) {
            response.put("parameters", procDetail.get("parameters"));
            response.put("variables", procDetail.get("variables"));
            response.put("statements", procDetail.get("statements"));
            response.put("statementCounts", procDetail.get("statementCounts"));
            response.put("variableCount", procDetail.get("variableCount"));
            response.put("parameterCount", procDetail.get("parameterCount"));
            response.put("statementCount", procDetail.get("statementCount"));
        }

        // Node-level sequence / join / cursor references
        nodeReferenceService.populateReferences(response, result, nodeId.toUpperCase());

        return ResponseEntity.ok(response);
    }
}
