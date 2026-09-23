package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import org.springframework.stereotype.Service;

import java.util.*;

/** Computes cumulative flow totals by walking the call tree with duplicates. */
@Service
public class FlowTotalsService {

    private final GraphMapHelper graphMapHelper;

    public FlowTotalsService(GraphMapHelper graphMapHelper) {
        this.graphMapHelper = graphMapHelper;
    }

    /**
     * Walk the call tree WITH duplicates to compute true flow totals.
     * If proc C is called from A and from B, C's LOC/tables/ops count TWICE.
     */
    public Map<String, Object> computeFlowTotals(Map<String, Object> tree, CallGraph graph, AnalysisResult result) {
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
                    if (graphMapHelper.matchesProcId(detail.getProcedureId(), nidUpper)) {
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
}
