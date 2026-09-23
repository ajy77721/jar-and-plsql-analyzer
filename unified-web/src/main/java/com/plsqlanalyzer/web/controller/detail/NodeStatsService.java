package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

/** Builds node-only stats/tables and parsed-procedure detail for a detail response. */
@Service
public class NodeStatsService {

    private static final Logger log = LoggerFactory.getLogger(NodeStatsService.class);

    private final GraphMapHelper graphMapHelper;

    public NodeStatsService(GraphMapHelper graphMapHelper) {
        this.graphMapHelper = graphMapHelper;
    }

    /** Populates "nodeStats" and "nodeTables" into the response, preserving key order. */
    public void populateNodeStats(Map<String, Object> response, CallGraph graph, String nodeId,
                                  AnalysisResult result, int nodeLoc) {
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
                    if (graphMapHelper.matchesProcId(detail.getProcedureId(), nodeIdUpper)) {
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
    }

    /**
     * Find the parsed PlsqlProcedure data from the analysis units.
     * Returns parameters, variables, statements as maps.
     */
    public Map<String, Object> findParsedProcedure(AnalysisResult result, String nodeId, CallGraphNode node) {
        if (result.getUnits() == null || node == null) return null;

        String targetName = node.getProcedureName();
        String targetPkg = node.getPackageName();
        int targetStart = node.getStartLine();

        for (var unit : result.getUnits()) {
            // Match by package name (case-insensitive)
            String unitName = unit.getName();
            if (targetPkg != null && unitName != null && !unitName.equalsIgnoreCase(targetPkg)) continue;

            for (var proc : unit.getProcedures()) {
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
}
