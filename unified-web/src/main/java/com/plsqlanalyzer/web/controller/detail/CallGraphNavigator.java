package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/** Call-graph node resolution helpers extracted from AnalyzerController. */
@Service
public class CallGraphNavigator {

    /** {@code <= 0} means unlimited — see applyTreeLimits. */
    @Value("${plsql.tree.max-depth:0}")
    private int treeMaxDepth;

    @Value("${plsql.tree.max-nodes:0}")
    private int treeMaxNodes;

    /**
     * Apply configured tree limits to the call graph.
     *
     * <p>Both default to unlimited: a truncated PL/SQL call graph silently under-reports the
     * tables and operations a procedure reaches, which is a wrong answer that looks like a right
     * one. Termination is guaranteed by cycle detection in the graph walk, not by these caps.
     * Set a positive value only to deliberately bound a pathological graph.
     */
    public void applyTreeLimits(CallGraph graph) {
        graph.setMaxTreeDepth(treeMaxDepth <= 0 ? Integer.MAX_VALUE : treeMaxDepth);
        graph.setMaxTreeNodes(treeMaxNodes <= 0 ? Integer.MAX_VALUE : treeMaxNodes);
    }

    public String findNodeId(CallGraph graph, String name) {
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

    /** Find the root node ID from analysis metadata */
    public String findRootId(AnalysisResult result, CallGraph graph) {
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

    public String stripProcPrefix(String name) {
        String upper = name.toUpperCase();
        for (String prefix : PROC_PREFIXES) {
            if (upper.startsWith(prefix)) return upper.substring(prefix.length());
        }
        return upper;
    }
}
