package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import org.springframework.stereotype.Service;

import java.util.*;

/** Node/tree map conversion + procedure-id matching helpers. */
@Service
public class GraphMapHelper {

    /**
     * Check if a procedureId from table access details matches any ID in a set.
     * Handles mismatches like: nodeId = "FN_PROC" vs procedureId = "SCHEMA.PKG.FN_PROC".
     */
    public boolean matchesProcIdSet(String procedureId, Set<String> nodeIds) {
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
    public boolean matchesProcId(String procedureId, String nodeId) {
        if (procedureId == null || nodeId == null) return false;
        return matchesProcIdSet(procedureId, Set.of(nodeId.toUpperCase()));
    }

    public void collectTreeNodeIds(Map<String, Object> tree, Set<String> ids) {
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

    public Map<String, Object> nodeToMap(CallGraphNode node) {
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
}
