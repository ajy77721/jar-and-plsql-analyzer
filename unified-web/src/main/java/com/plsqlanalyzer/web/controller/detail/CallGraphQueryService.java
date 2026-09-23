package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.*;

/** Call-graph / call-tree / procedures endpoints (extracted from AnalyzerController). */
@Service
public class CallGraphQueryService {

    private static final Logger log = LoggerFactory.getLogger(CallGraphQueryService.class);

    private final AnalysisService analysisService;
    private final CallGraphNavigator navigator;
    private final GraphMapHelper graphMapHelper;

    public CallGraphQueryService(AnalysisService analysisService, CallGraphNavigator navigator,
                                 GraphMapHelper graphMapHelper) {
        this.analysisService = analysisService;
        this.navigator = navigator;
        this.graphMapHelper = graphMapHelper;
    }

    public ResponseEntity<Object> getFullCallGraph() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        CallGraph graph = result.getCallGraph();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("nodeCount", graph.getNodeCount());
        response.put("edgeCount", graph.getEdgeCount());

        List<Map<String, Object>> nodes = new ArrayList<>();
        for (CallGraphNode node : graph.getAllNodes()) {
            nodes.add(graphMapHelper.nodeToMap(node));
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

    public ResponseEntity<Map<String, Object>> getCallTree(String procName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        CallGraph graph = result.getCallGraph();
        navigator.applyTreeLimits(graph);
        String nodeId = navigator.findNodeId(graph, procName);
        if (nodeId == null) return ResponseEntity.notFound().build();

        return ResponseEntity.ok(graph.getCallTree(nodeId));
    }

    public ResponseEntity<Map<String, Object>> getCallerTree(String procName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        CallGraph graph = result.getCallGraph();
        navigator.applyTreeLimits(graph);
        String nodeId = navigator.findNodeId(graph, procName);
        if (nodeId == null) return ResponseEntity.notFound().build();

        return ResponseEntity.ok(graph.getCallerTree(nodeId));
    }

    public ResponseEntity<List<Map<String, Object>>> listProcedures() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        CallGraph graph = result.getCallGraph();
        navigator.applyTreeLimits(graph);
        Set<String> reachableIds = new HashSet<>();
        try {
            String rootId = navigator.findRootId(result, graph);
            if (rootId != null) {
                graphMapHelper.collectTreeNodeIds(graph.getCallTree(rootId), reachableIds);
            }
        } catch (Exception e) {
            log.debug("Error computing reachable set: {}", e.getMessage());
        }

        List<Map<String, Object>> procs = new ArrayList<>();
        for (CallGraphNode node : graph.getAllNodes()) {
            if (node.getProcedureName() == null) continue;
            Map<String, Object> p = graphMapHelper.nodeToMap(node);
            p.put("reachable", reachableIds.contains(node.getId().toUpperCase()));
            procs.add(p);
        }
        return ResponseEntity.ok(procs);
    }
}
