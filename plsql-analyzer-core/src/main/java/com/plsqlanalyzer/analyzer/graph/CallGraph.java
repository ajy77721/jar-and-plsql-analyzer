package com.plsqlanalyzer.analyzer.graph;

import com.plsqlanalyzer.analyzer.model.CallEdge;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;

import java.util.*;
import java.util.stream.Collectors;

public class CallGraph {

    /** Max recursion depth for call tree building — prevents stack overflow on deep chains */
    private int maxTreeDepth = 50;

    /** Max nodes to expand in a single call tree — prevents multi-MB JSON responses */
    private int maxTreeNodes = 2000;

    public void setMaxTreeDepth(int d) { this.maxTreeDepth = (d < 0) ? Integer.MAX_VALUE : d; }
    public void setMaxTreeNodes(int n) { this.maxTreeNodes = (n < 0) ? Integer.MAX_VALUE : n; }
    public int getMaxTreeDepth() { return maxTreeDepth; }
    public int getMaxTreeNodes() { return maxTreeNodes; }

    private final Map<String, CallGraphNode> nodes = new LinkedHashMap<>();
    private final List<CallEdge> edges = new ArrayList<>();
    private final Map<String, List<CallEdge>> outgoingEdges = new HashMap<>();
    private final Map<String, List<CallEdge>> incomingEdges = new HashMap<>();

    public void addNode(CallGraphNode node) {
        nodes.put(node.getId(), node);
    }

    public void addEdge(CallEdge edge) {
        edges.add(edge);
        outgoingEdges.computeIfAbsent(edge.getFromNodeId(), k -> new ArrayList<>()).add(edge);
        incomingEdges.computeIfAbsent(edge.getToNodeId(), k -> new ArrayList<>()).add(edge);
    }

    public CallGraphNode getNode(String id) {
        CallGraphNode node = nodes.get(id);
        if (node != null) return node;
        // Fallback: search by baseId for overloaded procs (returns first overload)
        for (CallGraphNode n : nodes.values()) {
            if (id.equals(n.getBaseId()) && !id.equals(n.getId())) return n;
        }
        return null;
    }

    public Collection<CallGraphNode> getAllNodes() {
        return nodes.values();
    }

    public List<CallEdge> getAllEdges() {
        return edges;
    }

    public List<CallEdge> getOutgoingEdges(String nodeId) {
        return outgoingEdges.getOrDefault(nodeId, Collections.emptyList());
    }

    public List<CallEdge> getIncomingEdges(String nodeId) {
        return incomingEdges.getOrDefault(nodeId, Collections.emptyList());
    }

    /**
     * Get the full call tree (callees) rooted at the given node ID.
     * Returns a tree structure as nested maps.
     * Guarded by depth and node count limits to handle large call graphs safely.
     */
    public Map<String, Object> getCallTree(String rootId) {
        Set<String> visited = new HashSet<>();
        int[] nodeCount = {0}; // mutable counter for tracking total expanded nodes
        return buildCallTree(rootId, visited, 0, nodeCount);
    }

    private Map<String, Object> buildCallTree(String nodeId, Set<String> visited, int depth, int[] nodeCount) {
        Map<String, Object> tree = new LinkedHashMap<>();
        CallGraphNode node = nodes.get(nodeId);

        tree.put("id", nodeId);
        if (node != null) {
            tree.put("name", node.getProcedureName());
            tree.put("packageName", node.getPackageName());
            tree.put("schemaName", node.getSchemaName());
            if (node.getBaseId() != null && !node.getBaseId().equals(nodeId)) {
                tree.put("baseId", node.getBaseId());
            }
            // sourceFile: use stored value, or construct from schema + package
            String sf = node.getSourceFile();
            if ((sf == null || sf.isEmpty()) && node.getSchemaName() != null && node.getPackageName() != null) {
                sf = node.getSchemaName().toUpperCase() + "." + node.getPackageName().toUpperCase();
            }
            tree.put("sourceFile", sf);
            tree.put("startLine", node.getStartLine());
            tree.put("endLine", node.getEndLine());
            tree.put("unitType", node.getUnitType() != null ? node.getUnitType().name() : null);
            tree.put("callType", node.getCallType());
            tree.put("placeholder", node.isPlaceholder());
            if (node.getParamSignature() != null) tree.put("paramSignature", node.getParamSignature());
            if (node.getParamCount() > 0) tree.put("paramCount", node.getParamCount());
        }

        nodeCount[0]++;

        if (visited.contains(nodeId)) {
            tree.put("circular", true);
            tree.put("children", Collections.emptyList());
            return tree;
        }

        // Guard: depth limit
        if (depth >= maxTreeDepth) {
            tree.put("truncated", true);
            tree.put("truncateReason", "max depth " + maxTreeDepth + " reached");
            tree.put("children", Collections.emptyList());
            return tree;
        }

        // Guard: node count limit
        if (nodeCount[0] >= maxTreeNodes) {
            tree.put("truncated", true);
            tree.put("truncateReason", "max nodes " + maxTreeNodes + " reached");
            tree.put("children", Collections.emptyList());
            return tree;
        }

        visited.add(nodeId);
        List<Map<String, Object>> children = new ArrayList<>();
        for (CallEdge edge : getOutgoingEdges(nodeId)) {
            Map<String, Object> child = buildCallTree(edge.getToNodeId(), visited, depth + 1, nodeCount);
            child.put("callLineNumber", edge.getCallLineNumber());
            child.put("dynamicSql", edge.isDynamicSql());
            child.put("callType", edge.getCallType());
            children.add(child);
            // Stop expanding more children if we hit the limit
            if (nodeCount[0] >= maxTreeNodes) break;
        }
        tree.put("children", children);
        visited.remove(nodeId);

        return tree;
    }

    /**
     * Get reverse call tree (callers) for a given node.
     * Guarded by same depth and node count limits.
     */
    public Map<String, Object> getCallerTree(String targetId) {
        Set<String> visited = new HashSet<>();
        int[] nodeCount = {0};
        return buildCallerTree(targetId, visited, 0, nodeCount);
    }

    private Map<String, Object> buildCallerTree(String nodeId, Set<String> visited, int depth, int[] nodeCount) {
        Map<String, Object> tree = new LinkedHashMap<>();
        CallGraphNode node = nodes.get(nodeId);

        tree.put("id", nodeId);
        if (node != null) {
            tree.put("name", node.getProcedureName());
            tree.put("packageName", node.getPackageName());
            tree.put("schemaName", node.getSchemaName());
            if (node.getBaseId() != null && !node.getBaseId().equals(nodeId)) {
                tree.put("baseId", node.getBaseId());
            }
            String sf = node.getSourceFile();
            if ((sf == null || sf.isEmpty()) && node.getSchemaName() != null && node.getPackageName() != null) {
                sf = node.getSchemaName().toUpperCase() + "." + node.getPackageName().toUpperCase();
            }
            tree.put("sourceFile", sf);
            tree.put("startLine", node.getStartLine());
            if (node.getParamSignature() != null) tree.put("paramSignature", node.getParamSignature());
            if (node.getParamCount() > 0) tree.put("paramCount", node.getParamCount());
        }

        nodeCount[0]++;

        if (visited.contains(nodeId)) {
            tree.put("circular", true);
            tree.put("children", Collections.emptyList());
            return tree;
        }

        if (depth >= maxTreeDepth || nodeCount[0] >= maxTreeNodes) {
            tree.put("truncated", true);
            tree.put("children", Collections.emptyList());
            return tree;
        }

        visited.add(nodeId);
        List<Map<String, Object>> callers = new ArrayList<>();
        for (CallEdge edge : getIncomingEdges(nodeId)) {
            Map<String, Object> caller = buildCallerTree(edge.getFromNodeId(), visited, depth + 1, nodeCount);
            caller.put("callLineNumber", edge.getCallLineNumber());
            callers.add(caller);
            if (nodeCount[0] >= maxTreeNodes) break;
        }
        tree.put("children", callers);
        visited.remove(nodeId);

        return tree;
    }

    /**
     * Compute summary stats for a procedure's call tree (used by the detail header).
     * Walks the tree and counts: total nodes, internal/external calls, depth, unique tables referenced.
     */
    public Map<String, Object> getCallTreeStats(String rootId) {
        Set<String> visited = new HashSet<>();
        int[] counters = new int[5]; // [totalNodes, internalCalls, externalCalls, dynamicCalls, maxDepth]
        collectStats(rootId, visited, counters, 0);

        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalNodes", counters[0]);
        stats.put("internalCalls", counters[1]);
        stats.put("externalCalls", counters[2]);
        stats.put("dynamicCalls", counters[3]);
        stats.put("maxDepth", counters[4]);
        return stats;
    }

    private void collectStats(String nodeId, Set<String> visited, int[] counters, int depth) {
        if (visited.contains(nodeId)) return;
        if (depth >= maxTreeDepth || counters[0] >= maxTreeNodes) return;
        visited.add(nodeId);
        counters[0]++; // totalNodes
        if (depth > counters[4]) counters[4] = depth; // maxDepth

        for (CallEdge edge : getOutgoingEdges(nodeId)) {
            String ct = edge.getCallType();
            if ("INTERNAL".equals(ct)) counters[1]++;
            else if ("EXTERNAL".equals(ct)) counters[2]++;
            if (edge.isDynamicSql()) counters[3]++;
            collectStats(edge.getToNodeId(), visited, counters, depth + 1);
        }
        visited.remove(nodeId);
    }

    /**
     * Search for nodes by name (case-insensitive partial match).
     */
    public List<CallGraphNode> searchNodes(String query) {
        String q = query.toUpperCase();
        return nodes.values().stream()
                .filter(n -> n.getId().contains(q) ||
                        (n.getProcedureName() != null && n.getProcedureName().contains(q)))
                .collect(Collectors.toList());
    }

    public int getNodeCount() { return nodes.size(); }
    public int getEdgeCount() { return edges.size(); }
}
