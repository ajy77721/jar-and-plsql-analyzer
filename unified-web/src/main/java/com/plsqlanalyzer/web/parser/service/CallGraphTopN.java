package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The most-connected subgraph of a call graph, chosen by node degree.
 *
 * <h2>Why the server picks</h2>
 *
 * <p>Degree is a property of the whole graph, so a client cannot select the top N without first
 * holding every edge — which is the transfer being avoided. Measured: 847 edges today, 35,776 at
 * 832 procedures in the load proof, so roughly 344,000 at 8,000 procedures. The renderer draws at
 * most 300 nodes either way (FE-018).
 *
 * <h2>What "top" means, and what it costs</h2>
 *
 * <p>Degree counts both directions: a procedure called by many and one that calls many are both
 * structurally interesting, and a reader looking at a call graph is looking for hubs.
 *
 * <p>The result is <b>not</b> the whole story and says so: {@code truncated} and {@code totalEdges}
 * travel with it, so the client can tell the reader that a slice is a slice. A subgraph presented as
 * a graph is the failure FE-018's notice exists to prevent, and it would be reintroduced here if the
 * counts were dropped.
 */
public final class CallGraphTopN {

    private CallGraphTopN() {
    }

    /**
     * @param limit maximum number of nodes to keep
     * @return {@code {edges, nodeCount, totalEdges, totalNodes, truncated}}
     */
    public static ObjectNode select(ObjectMapper mapper, JsonNode graph, int limit) {
        JsonNode edges = graph == null ? null : graph.path("edges");
        ObjectNode out = mapper.createObjectNode();
        ArrayNode kept = out.putArray("edges");

        if (edges == null || !edges.isArray() || edges.isEmpty()) {
            out.put("nodeCount", 0);
            out.put("totalEdges", 0);
            out.put("totalNodes", 0);
            out.put("truncated", false);
            return out;
        }

        Map<String, Integer> degree = new HashMap<>();
        for (JsonNode e : edges) {
            degree.merge(endpoint(e, "from"), 1, Integer::sum);
            degree.merge(endpoint(e, "to"), 1, Integer::sum);
        }
        degree.remove("");

        List<String> ranked = new ArrayList<>(degree.keySet());
        // Degree descending, then by name — so the same graph always yields the same slice. Without
        // the tiebreaker two calls could return different subgraphs for identical input, and a
        // reader comparing two screens would see a difference that is not in the data.
        ranked.sort(Comparator.<String>comparingInt(n -> -degree.get(n)).thenComparing(n -> n));

        Set<String> top = new HashSet<>(ranked.subList(0, Math.min(limit, ranked.size())));
        for (JsonNode e : edges) {
            // Both endpoints must survive: an edge to a dropped node draws a line to nothing.
            if (top.contains(endpoint(e, "from")) && top.contains(endpoint(e, "to"))) kept.add(e);
        }

        out.put("nodeCount", top.size());
        out.put("totalEdges", edges.size());
        out.put("totalNodes", degree.size());
        out.put("truncated", degree.size() > top.size());
        return out;
    }

    /** An edge's endpoint, tolerating both field spellings the artifact uses. */
    private static String endpoint(JsonNode edge, String side) {
        JsonNode id = edge.path(side + "NodeId");
        if (id.isMissingNode() || id.isNull() || id.asText("").isEmpty()) id = edge.path(side);
        return id.asText("");
    }
}
