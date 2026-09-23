package com.codeanalyzer.ingest.mongo.write;

import com.codeanalyzer.ingest.mongo.id.Escaping;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The transitive closure of "what can this endpoint reach", computed once with a visited set.
 *
 * <h2>The number that decides the design</h2>
 *
 * <p>Expanded as a tree, the largest endpoint in the measured analysis is <b>20,300,167,401</b>
 * nodes and the 321 endpoints together are <b>466,903,865,409</b>. Those are not slow figures, they
 * are impossible ones. Collapsed by a visited set the same traversal visits <b>57,700</b>
 * (endpoint, node) pairs, the largest endpoint reaching 2,297 distinct nodes.
 *
 * <p>The gap is the whole point of the DAG: the producer content-addresses subtrees, so identical
 * subtrees are one node with many parents. Walking it as a tree re-walks every shared subtree once
 * per path that reaches it, and the number of paths is what explodes. The visited set converts
 * "count the paths" into "count the nodes".
 *
 * <h2>Breadth-first, so depth means something</h2>
 *
 * <p>The first time BFS reaches a node it has come by a shortest path, so the recorded {@code depth}
 * is the minimum. Depth-first would record whichever path happened to arrive first, and "how far is
 * this from the endpoint" would depend on child ordering rather than on the graph.
 *
 * <h2>Bound</h2>
 *
 * <p>Memory is the adjacency map plus one visited set per endpoint — 3,784 nodes and 7,281 edges
 * here. Row count is bounded by (endpoints × distinct nodes) and is measured at 57,700, far below
 * that product because most endpoints reach a small part of the graph.
 */
public final class EndpointReachability {

    private final MongoDatabase db;

    public EndpointReachability(MongoDatabase db) {
        this.db = db;
    }

    /**
     * Computes and stores reachability for every endpoint in one run.
     *
     * @return how many (endpoint, node) rows were written
     */
    public int computeFor(String runId) {
        Map<String, List<String>> children = adjacency(runId);
        if (children.isEmpty()) return 0;

        List<WriteModel<Document>> writes = new ArrayList<>();
        int written = 0;
        for (Document endpoint : db.getCollection(Collections.ENDPOINT)
                .find(Filters.eq("runId", runId))) {
            String endpointKey = endpoint.getString("entityKey");
            String root = endpoint.getString("rootRef");
            if (endpointKey == null || root == null) continue;

            for (Map.Entry<String, Integer> hit : reachableFrom(root, children).entrySet()) {
                String key = Escaping.join(endpointKey, hit.getKey());
                writes.add(new ReplaceOneModel<>(Filters.eq("_id", key),
                        new Document("_id", key)
                                .append("key", key)
                                .append("runId", runId)
                                .append("endpointRef", endpointKey)
                                .append("nodeRef", hit.getKey())
                                .append("depth", hit.getValue()),
                        new ReplaceOptions().upsert(true)));
            }
            if (writes.size() >= 2000) {
                db.getCollection(Collections.ENDPOINT_REACHABLE).bulkWrite(writes);
                written += writes.size();
                writes.clear();
            }
        }
        if (!writes.isEmpty()) {
            db.getCollection(Collections.ENDPOINT_REACHABLE).bulkWrite(writes);
            written += writes.size();
        }
        return written;
    }

    /** Parent → children, read once. Re-reading per endpoint would be 321 scans of the same edges. */
    private Map<String, List<String>> adjacency(String runId) {
        Map<String, List<String>> children = new HashMap<>();
        for (Document e : db.getCollection(Collections.DAG_EDGE).find(Filters.eq("runId", runId))
                .projection(new Document("parentRef", 1).append("childRef", 1))) {
            String parent = e.getString("parentRef");
            String child = e.getString("childRef");
            if (parent == null || child == null) continue;
            children.computeIfAbsent(parent, k -> new ArrayList<>()).add(child);
        }
        return children;
    }

    /**
     * Every node reachable from {@code root}, with its minimum depth.
     *
     * <p>The {@code visited} check happens on <em>enqueue</em>, not on dequeue. Checking at dequeue
     * still terminates, but a node with many parents is queued once per parent, and in a graph
     * whose whole character is shared subtrees that is the difference between a queue of thousands
     * and a queue of billions.
     */
    private Map<String, Integer> reachableFrom(String root, Map<String, List<String>> children) {
        Map<String, Integer> depths = new HashMap<>();
        Set<String> visited = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();

        visited.add(root);
        depths.put(root, 0);
        queue.add(root);

        while (!queue.isEmpty()) {
            String node = queue.removeFirst();
            int depth = depths.get(node);
            for (String child : children.getOrDefault(node, List.of())) {
                if (!visited.add(child)) continue;      // already queued by a shorter-or-equal path
                depths.put(child, depth + 1);
                queue.addLast(child);
            }
        }
        return depths;
    }
}
