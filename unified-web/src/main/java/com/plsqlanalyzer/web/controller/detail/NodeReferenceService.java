package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CursorOperationSummary;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

/** Populates node-level sequence/join/cursor references for a detail response. */
@Service
public class NodeReferenceService {

    private static final Logger log = LoggerFactory.getLogger(NodeReferenceService.class);

    private final GraphMapHelper graphMapHelper;

    public NodeReferenceService(GraphMapHelper graphMapHelper) {
        this.graphMapHelper = graphMapHelper;
    }

    /** Puts "nodeSequences", "nodeJoins", "nodeCursors" into response, preserving key order. */
    public void populateReferences(Map<String, Object> response, AnalysisResult result, String nodeIdUpper) {
        // Node-level sequence references
        List<Map<String, Object>> nodeSequences = new ArrayList<>();
        try {
            if (result.getSequenceOperations() != null) {
                for (var entry : result.getSequenceOperations().entrySet()) {
                    SequenceOperationSummary seqSummary = entry.getValue();
                    List<Map<String, Object>> seqNodeDetails = new ArrayList<>();
                    for (var sd : seqSummary.getAccessDetails()) {
                        if (graphMapHelper.matchesProcId(sd.getProcedureId(), nodeIdUpper)) {
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
                        if (graphMapHelper.matchesProcId(jd.getProcedureId(), nodeIdUpper)) {
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
                        if (graphMapHelper.matchesProcId(cd.getProcedureId(), nodeIdUpper)) {
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
    }
}
