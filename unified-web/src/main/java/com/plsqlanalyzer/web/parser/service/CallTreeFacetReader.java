package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.*;

import static com.plsqlanalyzer.web.parser.service.StaticAnalysisReader.*;

/** Call-tree + callers facet reads. */
class CallTreeFacetReader {

    private final StaticAnalysisReader r;
    private final CallGraphNav nav;
    private final PackageTreeBuilder pkgBuilder;

    CallTreeFacetReader(StaticAnalysisReader r) {
        this.r = r;
        this.nav = new CallGraphNav(r.mapper);
        this.pkgBuilder = new PackageTreeBuilder(r);
    }

    /**
     * How many entry-point call trees {@link AnalysisViewWriter} precomputes at save time. The roots
     * plus their depth-1 callees are what a user opens first; past a few dozen the cost of building
     * each tree at save time outweighs the hit rate, and any node not precomputed still gets its
     * sidecar from the read path's lazy backfill on first access.
     */
    static final int ENTRY_POINT_CAP = 40;

    /**
     * The node ids the call-tree sidecars are precomputed for: the analysis's <b>roots</b> (nodes no
     * edge calls — the top-level procedures the UI opens first) plus their direct callees (the
     * depth-1 layer), de-duplicated and capped at {@link #ENTRY_POINT_CAP}.
     *
     * <p>Computed from the same three files the tree itself is built from, and the same id resolution
     * ({@link CallGraphNav#resolveToFullId}), so a node selected here is exactly a node the endpoint
     * can be asked for. An analysis with no {@code api/call_graph.json}, or one whose nodes are all
     * called (a cycle-only corpus with no root), falls back to the shallowest {@code depth} layer,
     * then to index order — a best-effort selection that never throws.
     */
    List<String> entryPointNodeIds(String name) throws IOException {
        JsonNode index;
        try {
            index = r.getIndex(name);
        } catch (IOException e) {
            return List.of();
        }
        JsonNode nodes = index.get("nodes");
        if (nodes == null || !nodes.isArray()) return List.of();

        List<String> ids = new ArrayList<>();
        Map<String, Integer> depth = new HashMap<>();
        for (JsonNode n : nodes) {
            String id = textOrNull(n, "nodeId");
            if (id == null || id.isBlank()) continue;
            ids.add(id);
            depth.put(id, intOrDefault(n, "depth", -1));
        }
        if (ids.isEmpty()) return List.of();

        Map<String, String> shortToFull = r.loadIdMap(name);
        Set<String> known = new HashSet<>(ids);

        JsonNode edges = null;
        try {
            JsonNode callGraph = r.getCallGraph(name);
            if (callGraph != null) edges = callGraph.get("edges");
        } catch (IOException e) {
            // No api/call_graph.json — an old run, or one whose crawl recorded no edges. Every node
            // is a root then, which is the correct reading of "nothing calls it".
            edges = null;
        }

        Set<String> hasCaller = new HashSet<>();
        Map<String, List<String>> children = new HashMap<>();
        if (edges != null && edges.isArray()) {
            for (JsonNode edge : edges) {
                String from = textOrNull(edge, "fromNodeId");
                String to = textOrNull(edge, "toNodeId");
                String fullTo = to == null ? null : nav.resolveToFullId(to, shortToFull, known);
                boolean toKnown = fullTo != null && known.contains(fullTo);
                if (toKnown) hasCaller.add(fullTo);
                if (from == null) continue;
                String fullFrom = nav.resolveToFullId(from, shortToFull, known);
                if (known.contains(fullFrom) && toKnown) {
                    children.computeIfAbsent(fullFrom, k -> new ArrayList<>()).add(fullTo);
                }
            }
        }

        LinkedHashSet<String> roots = new LinkedHashSet<>();
        for (String id : ids) {
            if (!hasCaller.contains(id)) roots.add(id);
        }
        if (roots.isEmpty()) {
            int min = Integer.MAX_VALUE;
            for (String id : ids) {
                int d = depth.getOrDefault(id, -1);
                if (d >= 0 && d < min) min = d;
            }
            if (min != Integer.MAX_VALUE) {
                for (String id : ids) {
                    if (depth.getOrDefault(id, -1) == min) roots.add(id);
                }
            }
            if (roots.isEmpty()) roots.addAll(ids);
        }

        LinkedHashSet<String> entries = new LinkedHashSet<>();
        for (String root : roots) {
            if (entries.size() >= ENTRY_POINT_CAP) break;
            entries.add(root);
        }
        for (String root : roots) {
            if (entries.size() >= ENTRY_POINT_CAP) break;
            for (String child : children.getOrDefault(root, List.of())) {
                if (entries.size() >= ENTRY_POINT_CAP) break;
                entries.add(child);
            }
        }
        return new ArrayList<>(entries);
    }

    JsonNode getCallTree(String name, String nodeId) throws IOException {
        return getCallTree(name, nodeId, r.getCallGraph(name), null);
    }

    /** Build the call tree using a supplied call graph (lets the Claude reader inject
     *  a corrected/overlaid graph so the tree reflects accepted call changes). */
    JsonNode getCallTree(String name, String nodeId, JsonNode suppliedCallGraph) throws IOException {
        return getCallTree(name, nodeId, suppliedCallGraph, null);
    }

    /**
     * UI-019 (§142) — {@code maxDepth} is {@link CallGraphNav#buildCallTreeNode}'s own new
     * parameter, threaded through unchanged: {@code null} preserves every existing caller's
     * behaviour (the whole tree, to the node-count budget), a real value fetches just that many
     * levels rooted at {@code nodeId}, marking the cut points {@code moreChildren} so a client can
     * ask again for the next slice. Package-analysis trees ({@code buildPackageVirtualTree} below)
     * are deliberately NOT threaded through in this pass — a different builder, own scope, left
     * unbounded as before.
     */
    JsonNode getCallTree(String name, String nodeId, JsonNode suppliedCallGraph, Integer maxDepth) throws IOException {
        ObjectMapper mapper = r.mapper;
        JsonNode index;
        try { index = r.getIndex(name); } catch (IOException e) { index = mapper.createObjectNode(); }

        // A package crawled as a single aggregated node (see FlowNode#getMembers) — the caller
        // may be asking either for the package's own virtual root (its member list) or for one
        // specific member, filtered out of that SAME node rather than a separate crawl/subfolder.
        if (pkgBuilder.isPackageEntry(index)) {
            JsonNode virtualTree = pkgBuilder.buildPackageVirtualTree(name, index);
            JsonNode children = virtualTree.get("children");
            if (children != null && children.size() > 0) {
                String requestedUpper = nodeId.toUpperCase().replace(".", "_").replace("/", "_").replace("\\", "_");
                String rootId = textOrNull(virtualTree, "id");
                if (rootId != null && rootId.equalsIgnoreCase(requestedUpper)) return virtualTree;
                for (JsonNode child : children) {
                    String cid = textOrNull(child, "id");
                    if (cid != null && cid.equalsIgnoreCase(requestedUpper)) return child;
                }
                // Fallback: return empty root
                ObjectNode root = mapper.createObjectNode();
                root.put("id", nodeId); root.put("name", nodeId);
                root.put("callType", "ROOT"); root.put("callLineNumber", 0);
                root.set("children", mapper.createArrayNode());
                return root;
            }
            // No members recorded (e.g. the requested subprogram could not be resolved during the
            // crawl) — fall through to the normal call-tree handling below.
        }

        // Normal (non-package) analysis
        JsonNode callGraph = suppliedCallGraph != null ? suppliedCallGraph : r.getCallGraph(name);
        JsonNode edges = callGraph.get("edges");
        Map<String, String> shortToFull = r.loadIdMap(name);

        Map<String, JsonNode> nodeMetadata = new HashMap<>();
        JsonNode nodes = index.get("nodes");
        if (nodes != null && nodes.isArray()) {
            for (JsonNode n : nodes) {
                String fullId = textOrNull(n, "nodeId");
                if (fullId != null) nodeMetadata.put(fullId, n);
            }
        }

        Map<String, List<JsonNode>> adjacency = new HashMap<>();
        if (edges != null && edges.isArray()) {
            for (JsonNode edge : edges) {
                String from = textOrNull(edge, "fromNodeId");
                if (from == null) continue;
                String fullFrom = nav.resolveToFullId(from, shortToFull, nodeMetadata.keySet());
                adjacency.computeIfAbsent(fullFrom, k -> new ArrayList<>()).add(edge);
            }
        }

        String resolvedId = nav.resolveToFullId(nodeId, shortToFull, nodeMetadata.keySet());
        Set<String> visited = new HashSet<>();
        CallGraphNav.TreeBudget budget = new CallGraphNav.TreeBudget();
        ObjectNode root = nav.buildCallTreeNode(resolvedId, nodeMetadata, adjacency, shortToFull, visited, null, 0, budget, maxDepth);
        // Root-level flag, in addition to wherever childrenTruncated landed deeper in the tree —
        // a caller checking "was this tree cut at all" reads one field instead of walking the
        // whole result looking for a per-node badge (same convention MID-006's own JS renderers
        // already use for other endpoints).
        root.put("truncated", budget.truncated);
        return root;
    }

    JsonNode getCallers(String name, String nodeId) throws IOException {
        return getCallers(name, nodeId, r.getCallGraph(name));
    }

    JsonNode getCallers(String name, String nodeId, JsonNode suppliedCallGraph) throws IOException {
        ObjectMapper mapper = r.mapper;
        JsonNode index = r.getIndex(name);
        JsonNode callGraph = suppliedCallGraph != null ? suppliedCallGraph : r.getCallGraph(name);
        JsonNode edges = callGraph.get("edges");
        Map<String, String> shortToFull = r.loadIdMap(name);

        Map<String, JsonNode> nodeMetadata = new HashMap<>();
        JsonNode nodes = index.get("nodes");
        if (nodes != null && nodes.isArray()) {
            for (JsonNode n : nodes) {
                String fullId = textOrNull(n, "nodeId");
                if (fullId != null) nodeMetadata.put(fullId, n);
            }
        }

        String resolvedId = nav.resolveToFullId(nodeId, shortToFull, nodeMetadata.keySet());

        ArrayNode result = mapper.createArrayNode();
        if (edges != null && edges.isArray()) {
            for (JsonNode edge : edges) {
                String toId = textOrNull(edge, "toNodeId");
                if (toId == null) continue;
                String fullToId = nav.resolveToFullId(toId, shortToFull, nodeMetadata.keySet());
                if (!fullToId.equals(resolvedId)) continue;

                String fromId = textOrNull(edge, "fromNodeId");
                if (fromId == null) continue;
                String fullFromId = nav.resolveToFullId(fromId, shortToFull, nodeMetadata.keySet());
                JsonNode meta = nodeMetadata.get(fullFromId);
                // Dangling edge: no FlowNode was ever written for the caller side either (same
                // "no source found for: ..." case CallGraphNav.buildCallTreeNode guards against
                // for the downward tree — see its isDanglingEdge doc). Skip rather than render a
                // blank/degenerate caller row.
                if (meta == null) continue;

                ObjectNode caller = mapper.createObjectNode();
                caller.put("id", fullFromId);
                caller.put("name", meta != null ? textOrDefault(meta, "name", fullFromId) : textOrDefault(edge, "from", fullFromId));
                caller.put("schemaName", meta != null ? textOrDefault(meta, "schema", "") : textOrDefault(edge, "fromSchema", ""));
                caller.put("packageName", meta != null ? textOrDefault(meta, "packageName", "") : "");
                caller.put("objectType", meta != null ? textOrDefault(meta, "objectType", "") : "");
                caller.put("sourceFile", meta != null ? textOrDefault(meta, "sourceFile", "") : "");
                caller.put("startLine", meta != null ? intOrDefault(meta, "lineStart", 0) : 0);
                caller.put("endLine", meta != null ? intOrDefault(meta, "lineEnd", 0) : 0);
                caller.put("unitType", meta != null ? textOrDefault(meta, "objectType", "PROCEDURE") : "PROCEDURE");

                JsonNode lines = edge.get("lines");
                if (lines != null && lines.isArray() && !lines.isEmpty()) {
                    caller.put("callLineNumber", lines.get(0).asInt());
                    ArrayNode allLines = mapper.createArrayNode();
                    for (JsonNode ln : lines) allLines.add(ln.asInt());
                    caller.set("callLines", allLines);
                } else {
                    caller.put("callLineNumber", 0);
                }

                // Same rule the downward call tree uses, so one edge cannot be INTERNAL in
                // Hierarchy/Trace and EXTERNAL in the caller list. The callee's metadata is the
                // one this helper needs for the "to" side — `meta` above is the CALLER's.
                caller.put("callType",
                        CallGraphNav.classifyCall(edge, nodeMetadata.get(resolvedId), resolvedId));

                result.add(caller);
            }
        }

        return result;
    }
}
