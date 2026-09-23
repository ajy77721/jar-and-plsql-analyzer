package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.web.service.LazyAnalysisResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

/** Builds the analysis summary map (extracted from AnalyzerController). */
@Service
public class SummaryService {

    private static final Logger log = LoggerFactory.getLogger(SummaryService.class);

    private final CallGraphNavigator navigator;
    private final GraphMapHelper graphMapHelper;

    public SummaryService(CallGraphNavigator navigator, GraphMapHelper graphMapHelper) {
        this.navigator = navigator;
        this.graphMapHelper = graphMapHelper;
    }

    public Map<String, Object> buildSummary(AnalysisResult result) {
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
            navigator.applyTreeLimits(graph);
            String rootId = navigator.findRootId(result, graph);
            if (rootId != null) {
                summary.put("rootId", rootId);
                Map<String, Object> flowStats = graph.getCallTreeStats(rootId);
                summary.put("flowStats", flowStats);

                Set<String> treeNodeIds = new HashSet<>();
                graphMapHelper.collectTreeNodeIds(graph.getCallTree(rootId), treeNodeIds);

                int flowTables = 0;
                int flowDbOps = 0;
                Set<String> flowOps = new LinkedHashSet<>();
                for (var entry : result.getTableOperations().entrySet()) {
                    TableOperationSummary ts = entry.getValue();
                    boolean relevant = false;
                    for (var d : ts.getAccessDetails()) {
                        if (d.getProcedureId() != null && graphMapHelper.matchesProcIdSet(d.getProcedureId(), treeNodeIds)) {
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
}
