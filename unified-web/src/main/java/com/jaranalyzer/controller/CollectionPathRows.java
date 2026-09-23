package com.jaranalyzer.controller;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Q43 / L4 — every call path that performs one operation on one collection, from a single pass
 * over {@code endpoints[]}.
 *
 * <p><b>What this replaces.</b> The Collection popup
 * ({@code jar/js/summary-popups.js:_loadCollOpPaths}) used to run an unbounded
 * {@code Promise.all} of {@code /endpoints/by-index/{idx}/call-tree}, one request per endpoint
 * that touches the collection. On the measured {@code web-1.0.0-SNAPSHOT} analysis the worst
 * cell ({@code ENTERPRISE.ORDERS} / {@code READ}) is 15 endpoints: 15 requests and 114 411 B for
 * 15 table rows. The transferred bytes are only half of it — {@code streamCallTree} seeks
 * {@code endpoints[]} to <i>one</i> index, so each of those requests re-parses the whole
 * {@code analysis.json} (3.9 MB measured, 912 MB seen in the field). N popup requests were N full
 * document parses. This walks the array once and emits the rows.
 *
 * <p><b>The row semantics are the browser's, reproduced rather than improved.</b> Three details
 * of {@code _loadCollOpPaths} look accidental and are load-bearing, so they are kept:
 *
 * <ul>
 *   <li>A node whose operation cannot be determined at all matches <em>every</em> operation — the
 *       client's guard is {@code if (opsToShow.length && !opsToShow.includes(op)) return}, so an
 *       empty operation list falls through rather than filtering the row out. One node in the
 *       measured data is in that state ({@code OrderItem.calculateLineTotal}, an {@code ENTITY}
 *       whose name matches no configured prefix).</li>
 *   <li>{@code operationTypes} wins outright when present; {@code operationType} and the inferred
 *       type are only consulted when it is absent (75 of the 76 collection-touching nodes in the
 *       measured data carry {@code operationTypes}).</li>
 *   <li>Rows are de-duplicated on <em>endpoint plus target method</em>, not per path, so a method
 *       reached twice through different callers contributes one row — the first path found, in
 *       walk order.</li>
 * </ul>
 *
 * <p><b>Operation inference.</b> Where the client called its own {@code _inferOp}, this calls
 * {@code DomainConfigLoader.inferOperationType} — the same function that wrote
 * {@code operationType} onto the other 75 nodes during analysis. The two agree on every node in
 * every analysis in {@code data/jar}; where they could differ (the loader prefers the longest
 * matching prefix, the client the first in config order) the analyzer's own answer is the one the
 * rest of the document was built with, so it is the one to keep.
 *
 * <p><b>Memory.</b> Lazy, one endpoint at a time: the parser holds a single endpoint object as a
 * tree (46 KB average, 142 KB worst measured) and buffers only that endpoint's rows. Nothing
 * proportional to the analysis is retained, which is the constraint that lets a 912 MB
 * {@code analysis.json} be served from a 4 GB heap.
 *
 * <p><b>Identity.</b> Rows carry {@code endpointId} — the endpoint's natural key
 * ({@code AbstractAnalysisDataProvider.endpointId}) — and never an array index. The popup used
 * {@code sourceIdx}, a position in whichever endpoint array the screen had last fetched, and every
 * one of its callers omitted the {@code version} argument, so on a screen showing
 * {@code CORRECTED_PREV} the index was resolved against the current document. That is the same
 * class of bug as PLAN-03 §1.5 #4 and it is why the row identity here is a key.
 */
final class CollectionPathRows implements Iterator<JsonNode> {

    /** Set when {@code endpoints[]} was not present at all — a 204, not an empty page. */
    private final boolean present;

    private final JsonParser parser;
    private final ObjectMapper mapper;
    private final String collection;
    private final String op;
    /** {@code (methodName, stereotype) -> operation type}, or null. */
    private final BiFunction<String, String, String> inferOp;

    private final Deque<JsonNode> buffered = new ArrayDeque<>();
    private final Set<String> seen = new HashSet<>();
    private int seq;
    private boolean exhausted;

    /**
     * Node dictionary for documents written in the flat DAG format; empty for legacy documents,
     * which carry their trees inline. Picked up during {@link #open}, which is possible because
     * the writer puts the dictionary ahead of {@code endpoints}.
     */
    private final com.jaranalyzer.service.CallTreeInflater inflater;

    private CollectionPathRows(JsonParser parser, ObjectMapper mapper, boolean present,
                               String collection, String op,
                               BiFunction<String, String, String> inferOp,
                               com.jaranalyzer.service.CallTreeInflater inflater) {
        this.parser = parser;
        this.mapper = mapper;
        this.present = present;
        this.inflater = inflater;
        this.collection = collection;
        this.op = op.toUpperCase(Locale.ROOT);
        this.inferOp = inferOp;
        this.exhausted = !present;
    }

    /**
     * Positions {@code parser} on the first element of the top-level {@code endpoints} array.
     *
     * <p>Only the top-level field is matched, and every other top-level value is skipped whole:
     * a naive token scan for the name {@code endpoints} also matches nested maps that happen to
     * carry the key, which is the bug {@code AbstractAnalysisDataProvider.streamCallTree} guards
     * against in the same way.
     */
    static CollectionPathRows open(JsonParser parser, ObjectMapper mapper, String collection,
                                   String op, BiFunction<String, String, String> inferOp)
            throws IOException {
        boolean found = false;
        var inflater = com.jaranalyzer.service.CallTreeInflater.empty();
        if (parser.nextToken() == JsonToken.START_OBJECT) {
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String name = parser.currentName();
                parser.nextToken();
                if ("endpoints".equals(name)) {
                    found = parser.currentToken() == JsonToken.START_ARRAY;
                    break;
                }
                if (com.jaranalyzer.service.CallTreeFlattener.NODES_FIELD.equals(name)) {
                    inflater = com.jaranalyzer.service.CallTreeInflater.read(parser, mapper);
                    continue;
                }
                if (parser.currentToken() == JsonToken.START_ARRAY
                        || parser.currentToken() == JsonToken.START_OBJECT) {
                    parser.skipChildren();
                }
            }
        }
        return new CollectionPathRows(parser, mapper, found, collection, op, inferOp, inflater);
    }

    /** Whether the document had an {@code endpoints} array — false means 204, not an empty page. */
    boolean present() {
        return present;
    }

    /** The whole result set, for the unpaged legacy shape. */
    ArrayNode toArray() {
        ArrayNode out = mapper.createArrayNode();
        while (hasNext()) out.add(next());
        return out;
    }

    @Override
    public boolean hasNext() {
        while (buffered.isEmpty() && !exhausted) {
            try {
                advance();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return !buffered.isEmpty();
    }

    @Override
    public JsonNode next() {
        if (!hasNext()) throw new NoSuchElementException();
        return buffered.removeFirst();
    }

    /** Reads one endpoint and buffers whatever rows it contributes. */
    private void advance() throws IOException {
        JsonToken token = parser.nextToken();
        if (token == null || token == JsonToken.END_ARRAY) {
            exhausted = true;
            return;
        }
        if (token != JsonToken.START_OBJECT) return;
        JsonNode ep = parser.readValueAsTree();
        JsonNode tree = ep.path("callTree");
        if (!tree.isObject()) {
            // Flat format: the endpoint holds a dictionary key instead of the tree. Resolve to
            // the root record — the walk below reads the same per-node fields either way.
            JsonNode rootId = ep.get(com.jaranalyzer.service.CallTreeFlattener.ROOT_ID_FIELD);
            if (rootId != null && rootId.isTextual()) {
                JsonNode root = inflater.record(rootId.asText());
                if (root != null) tree = root;
            }
        }
        if (tree.isObject()) {
            walk(ep, tree, new ArrayList<>());
        } else {
            syntheticRow(ep);
        }
    }

    /**
     * The client's {@code _walkWithPath} plus its match predicate, over one endpoint's tree.
     *
     * <p>Recursive, like the browser's version, and for the same reason: a call tree is bounded by
     * the depth the analyzer wrote, not by the size of the analysis.
     */
    private void walk(JsonNode ep, JsonNode node, List<JsonNode> path) {
        path.add(node);
        if (touchesCollection(node)) {
            List<String> ops = operationsOf(node);
            if (ops.isEmpty() || ops.contains(op)) {
                String target = node.path("className").asText(
                        node.path("simpleClassName").asText("")) + '.'
                        + node.path("methodName").asText("");
                // The badge shows the operation that was asked for, not the node's whole list —
                // which is what the client rendered ({@code opsToShow: [opUpper]}) and is the
                // only one of them the popup is about.
                if (seen.add(endpointName(ep) + '|' + target)) {
                    buffered.addLast(row(ep, path, List.of(op), false));
                }
            }
        }
        JsonNode children = node.path("children");
        if (children.isArray()) {
            for (JsonNode child : children) {
                if (child.isObject()) walk(ep, child, path);
            }
        } else {
            // Flat format. Walked by id per PATH, not per distinct node, so the rows and their
            // breadcrumbs are exactly the ones the inline walk produced — the de-duplication on
            // endpoint+target below is what bounds the output, and adding a visited set here
            // would change which path each row reports.
            JsonNode childIds = node.path(com.jaranalyzer.service.CallTreeFlattener.CHILD_IDS_FIELD);
            if (childIds.isArray()) {
                for (JsonNode id : childIds) {
                    JsonNode child = inflater.record(id.asText());
                    if (child != null) walk(ep, child, path);
                }
            }
        }
        path.remove(path.size() - 1);
    }

    /**
     * The client's "no call tree" fallback, kept because dropping it would silently lose rows on
     * an analysis that has such an endpoint.
     *
     * <p>It fires when an endpoint has no {@code callTree} and the endpoint-level rollups say it
     * touches the collection with this operation. There is no path to show — the client builds a
     * one-segment breadcrumb from its own {@code collections[name].sources}, which is derived in
     * the browser and not present here — so the row is marked {@code synthetic} and the client
     * fills the breadcrumb in from data it already holds, exactly as it does today.
     *
     * <p><b>Untested against real data:</b> every endpoint in every analysis under
     * {@code data/jar} has a call tree, so this branch is reached only by
     * {@code CollectionPathRowsTest}'s fixture.
     */
    private void syntheticRow(JsonNode ep) {
        JsonNode collections = ep.path("aggregatedCollections");
        boolean touches = false;
        if (collections.isArray()) {
            for (JsonNode c : collections) {
                if (collection.equals(c.asText(""))) touches = true;
            }
        } else if (collections.isObject()) {
            touches = collections.has(collection);
        }
        if (!touches) return;
        String epOp = ep.path("aggregatedOperations").path(collection).asText("");
        if (!epOp.isEmpty() && !epOp.toUpperCase(Locale.ROOT).equals(op)) return;
        if (seen.add(endpointName(ep) + "|<synthetic>")) {
            buffered.addLast(row(ep, List.of(), List.of(op), true));
        }
    }

    private boolean touchesCollection(JsonNode node) {
        JsonNode accessed = node.path("collectionsAccessed");
        if (!accessed.isArray()) return false;
        for (JsonNode c : accessed) {
            if (collection.equals(c.asText(""))) return true;
        }
        return false;
    }

    /**
     * A node's operations, upper-cased. Empty means "unknown", which the client treats as matching
     * every operation rather than none.
     */
    private List<String> operationsOf(JsonNode node) {
        JsonNode many = node.path("operationTypes");
        if (many.isArray() && many.size() > 0) {
            List<String> out = new ArrayList<>(many.size());
            for (JsonNode o : many) {
                String v = o.asText("");
                if (!v.isEmpty()) out.add(v.toUpperCase(Locale.ROOT));
            }
            if (!out.isEmpty()) return out;
        }
        String single = node.path("operationType").asText("");
        if (single.isEmpty() && inferOp != null) {
            String inferred = inferOp.apply(node.path("methodName").asText(null),
                    node.path("stereotype").asText(null));
            if (inferred != null) single = inferred;
        }
        return single.isEmpty() ? List.of() : List.of(single.toUpperCase(Locale.ROOT));
    }

    /**
     * One table row: the endpoint's identity, the operation badge, and the breadcrumb segments —
     * label, class and method per level, which is everything the popup renders.
     *
     * <p>{@code seq} is the row's position in the scan, which is the default paging order, so
     * page 1 is the same rows in the same order the browser's fan-out produced. It is not the row
     * identity: re-running an analysis renumbers it.
     */
    private ObjectNode row(JsonNode ep, List<JsonNode> path, List<String> ops, boolean synthetic) {
        ObjectNode out = mapper.createObjectNode();
        out.put("endpointId", endpointId(ep));
        out.put("endpointName", endpointName(ep));
        out.put("httpMethod", ep.path("httpMethod").asText("ALL"));
        out.put("fullPath", ep.path("fullPath").asText("/"));
        out.put("seq", seq++);
        if (synthetic) out.put("synthetic", true);
        ArrayNode opsOut = out.putArray("ops");
        ops.forEach(opsOut::add);
        ArrayNode segments = out.putArray("path");
        for (JsonNode n : path) {
            ObjectNode seg = segments.addObject();
            String simple = n.path("simpleClassName").asText("");
            String className = n.path("className").asText("");
            if (simple.isEmpty()) {
                int dot = className.lastIndexOf('.');
                simple = dot < 0 ? className : className.substring(dot + 1);
            }
            String method = n.path("methodName").asText("");
            seg.put("label", (simple.isEmpty() ? "?" : simple) + '.'
                    + (method.isEmpty() ? "?" : method));
            seg.put("className", className.isEmpty() ? simple : className);
            seg.put("methodName", method);
        }
        return out;
    }

    /** {@code controllerSimpleName.methodName} — the label the popup's Endpoint column shows. */
    private static String endpointName(JsonNode ep) {
        return ep.path("controllerSimpleName").asText("") + '.'
                + ep.path("methodName").asText("");
    }

    /**
     * The endpoint's natural key, in the same four fields and the same order as
     * {@code AbstractAnalysisDataProvider.endpointId} and {@code JA.utils.endpointKey}. Written
     * out here rather than shared because that method is {@code protected} on a provider class and
     * this reader holds no provider — the formula is the contract, and
     * {@code CollectionPathRowsTest} asserts the two agree.
     */
    private static String endpointId(JsonNode ep) {
        return ep.path("httpMethod").asText("") + ' ' + ep.path("fullPath").asText("") + ' '
                + ep.path("controllerClass").asText("") + '#' + ep.path("methodName").asText("");
    }
}
