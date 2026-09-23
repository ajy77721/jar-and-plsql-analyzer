package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Rebuilds the nested call tree that the API has always served from the flat, content-addressed
 * node dictionary written by {@link CallTreeFlattener}.
 *
 * <p>The stored record keeps every field of the original {@code CallNode} in its original slot,
 * with {@code children} replaced in place by {@code childIds}. Expansion therefore only has to
 * copy the record and substitute that one field, which reproduces the pre-flattening bytes
 * field-for-field — the on-disk format changed, the wire format did not.
 *
 * <p>No visited set is needed. Ids are hashes over child ids (a Merkle DAG), so an id can never
 * reference itself transitively and expansion always terminates.
 *
 * <h2>Why termination was not enough</h2>
 *
 * <p>Expansion is <b>exponential in output size</b>: the DAG stores one copy of a shared subtree,
 * and expanding it writes one copy per path that reaches it. That was accepted as "exactly what the
 * endpoint returned before" — true, and still a hang. Measured in the field on
 * {@code unified-web-1.0.0-SNAPSHOT} endpoint 271, this ran past <b>295 seconds</b> without
 * returning, with six concurrent requests piled up behind it because the UI offered no feedback and
 * the user clicked again.
 *
 * <p>{@link #MAX_NODES} bounds it. The budget is deliberately far above any healthy tree, so every
 * endpoint that worked before is still written byte-for-byte; only a tree that was never going to
 * arrive is cut short. When it is cut, the node says so — {@code childrenTruncated} and
 * {@code omittedChildCount} — because a silently shortened call tree is a wrong answer about what
 * an endpoint calls, which is worse than a slow one.
 */
public final class CallTreeInflater {

    /**
     * Most nodes one expansion may write.
     *
     * <p>Sized from real data: the largest healthy tree observed here is in the low thousands, so
     * 200,000 leaves three orders of magnitude of headroom and no existing response changes. It
     * exists only to convert "never returns" into "returns, and says it was cut".
     */
    public static final int MAX_NODES = 200_000;

    private final Map<String, JsonNode> nodes;

    private CallTreeInflater(Map<String, JsonNode> nodes) {
        this.nodes = nodes;
    }

    /** True when this document was written in the flat format. */
    public boolean isFlat() {
        return !nodes.isEmpty();
    }

    public int size() {
        return nodes.size();
    }

    /**
     * Consumes the {@code callTreeNodes} object the parser is currently positioned on (its
     * START_OBJECT token) and returns an inflater over it.
     */
    public static CallTreeInflater read(JsonParser parser, ObjectMapper mapper) throws IOException {
        Map<String, JsonNode> map = new HashMap<>();
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            if (parser.currentToken() == JsonToken.START_ARRAY) parser.skipChildren();
            return new CallTreeInflater(map);
        }
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            if (parser.currentToken() != JsonToken.FIELD_NAME) continue;
            String id = parser.currentName();
            parser.nextToken();
            map.put(id, mapper.readTree(parser));
        }
        return new CallTreeInflater(map);
    }

    /**
     * Expands a per-endpoint {@code call-tree.json} sidecar back to the nested tree.
     *
     * <p>Accepts both shapes so nothing has to be migrated: a document carrying
     * {@code callTreeFormat} is a dictionary and gets expanded; anything else is a legacy sidecar
     * that already IS the tree and is returned unchanged.
     */
    public static JsonNode expandSidecar(JsonNode sidecar, ObjectMapper mapper) throws IOException {
        if (sidecar == null || !sidecar.isObject()
                || !sidecar.has(CallTreeFlattener.FORMAT_FIELD)) {
            return sidecar;
        }
        Map<String, JsonNode> map = new HashMap<>();
        JsonNode dict = sidecar.get(CallTreeFlattener.NODES_FIELD);
        if (dict != null) {
            dict.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue()));
        }
        JsonNode rootId = sidecar.get(CallTreeFlattener.ROOT_ID_FIELD);
        return new CallTreeInflater(map)
                .toTree(rootId == null || rootId.isNull() ? null : rootId.asText(), mapper);
    }

    /** An inflater that knows about nothing — used for legacy documents. */
    public static CallTreeInflater empty() {
        return new CallTreeInflater(Map.of());
    }

    /** Raw dictionary record for an id, or null. */
    public JsonNode record(String id) {
        return nodes.get(id);
    }

    /**
     * Writes only the nodes reachable from {@code id}, as a flat dictionary.
     *
     * <h2>Why this exists</h2>
     *
     * <p>Expansion is exponential in sharing, and on real data that is not a theoretical concern.
     * Measured on {@code unified-web-1.0.0-SNAPSHOT}, endpoint 271:
     *
     * <pre>
     *   expanded tree : 300,167,238 nodes
     *   reachable DAG :       2,282 nodes  (7.4 MB)
     * </pre>
     *
     * <p>A 131,000x difference. The 200,000-node budget added earlier bounded the response at
     * <b>0.07%</b> of the tree and still produced 120 MB after three and a half minutes — so it
     * converted a hang into a large, silently truncated answer rather than fixing anything.
     *
     * <p>Sending the DAG restores the property the flattener was built for: the payload is
     * proportional to distinct methods, not to paths through them. Only the reachable subgraph is
     * written, not the whole dictionary, so an endpoint pays for what it calls.
     *
     * @param id   the root node id
     * @param gen  written as {@code {format, rootId, nodeCount, nodes:{id:{...,childIds:[...]}}}}
     */
    public void writeReachable(String id, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("format", CallTreeFlattener.FORMAT);
        gen.writeStringField("rootId", id == null ? null : id);

        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        if (id != null && nodes.containsKey(id)) {
            java.util.Deque<String> stack = new java.util.ArrayDeque<>();
            stack.push(id);
            while (!stack.isEmpty()) {
                String cur = stack.pop();
                if (cur == null || !seen.add(cur)) continue;      // the visited set IS the dedup
                JsonNode rec = nodes.get(cur);
                if (rec == null) continue;
                JsonNode kids = rec.get(CallTreeFlattener.CHILD_IDS_FIELD);
                if (kids != null && kids.isArray()) {
                    for (JsonNode k : kids) stack.push(k.asText());
                }
            }
        }

        gen.writeNumberField("nodeCount", seen.size());
        gen.writeFieldName("nodes");
        gen.writeStartObject();
        for (String nid : seen) {
            JsonNode rec = nodes.get(nid);
            if (rec == null) continue;
            gen.writeFieldName(nid);
            gen.writeTree(rec);
        }
        gen.writeEndObject();
        gen.writeEndObject();
    }

    /**
     * Writes the fully expanded node for {@code id}. Writes JSON null when the id is unknown or
     * null, matching what {@code streamCallTree} emitted for a missing tree.
     */
    public void expand(String id, JsonGenerator gen) throws IOException {
        expand(id, gen, new int[]{0});
    }

    /**
     * @param budget single-element counter of nodes written so far, shared down the recursion
     */
    private void expand(String id, JsonGenerator gen, int[] budget) throws IOException {
        JsonNode record = id == null ? null : nodes.get(id);
        if (record == null) {
            gen.writeNull();
            return;
        }
        budget[0]++;
        gen.writeStartObject();
        Iterator<Map.Entry<String, JsonNode>> it = record.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            if (CallTreeFlattener.CHILD_IDS_FIELD.equals(e.getKey())) {
                gen.writeFieldName("children");
                gen.writeStartArray();
                int written = 0;
                for (JsonNode child : e.getValue()) {
                    if (budget[0] >= MAX_NODES) break;
                    expand(child.asText(), gen, budget);
                    written++;
                }
                gen.writeEndArray();
                int omitted = e.getValue().size() - written;
                if (omitted > 0) {
                    // Stated, never silent. A shortened tree that does not admit it is a wrong
                    // answer about what this endpoint calls.
                    gen.writeBooleanField("childrenTruncated", true);
                    gen.writeNumberField("omittedChildCount", omitted);
                }
            } else {
                gen.writeFieldName(e.getKey());
                gen.writeTree(e.getValue());
            }
        }
        gen.writeEndObject();
    }

    /**
     * Materialises the expanded tree as a JsonNode.
     *
     * <p>Only for the aggregate-recomputation fallback in {@code streamSummary}, which needs a
     * random-access tree. That fallback fires only for documents whose endpoints lack
     * pre-computed aggregates; every document written in this format has them, because
     * {@code EndpointInfo.computeAggregates()} runs before persistence. Expanding here rather
     * than walking the dictionary is deliberate: the legacy walker counts nodes per PATH, and
     * reproducing that exactly matters more here than being fast on a path that should not run.
     */
    public JsonNode toTree(String id, ObjectMapper mapper) throws IOException {
        if (id == null || !nodes.containsKey(id)) return null;
        try (var buf = new com.fasterxml.jackson.databind.util.TokenBuffer(mapper, false)) {
            expand(id, buf);
            return mapper.readTree(buf.asParser());
        }
    }

    /** Writes an array of expanded nodes for a list of ids (a fragment's overflow children). */
    public void expandAll(JsonNode ids, JsonGenerator gen) throws IOException {
        gen.writeStartArray();
        if (ids != null && ids.isArray()) {
            for (JsonNode id : ids) expand(id.asText(), gen);
        }
        gen.writeEndArray();
    }
}
