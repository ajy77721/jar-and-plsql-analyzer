package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pools the heavily-duplicated per-endpoint arrays, the same way {@link InvocationPool} pools
 * invocations.
 *
 * <p>Measured on the current document (the −16.3 MB in PLAN-12 was from the pre-ANTLR-fix 77 MB
 * version and is 4× stale):
 *
 * <pre>
 *   externalCalls   5,958 entries  277 distinct (4.6% unique)   2.25 MB -> saves 2.11 MB
 *   dynamicFlows    3,456 entries  1,357 distinct (39.3%)       2.14 MB -> saves 1.33 MB
 *   beans           2,818 entries  177 distinct (6.3%)          0.78 MB -> saves 0.72 MB
 * </pre>
 *
 * <p>{@code aggregationFlows} and {@code httpCalls} are 100% unique and deliberately excluded —
 * pooling them costs bytes rather than saving them.
 *
 * <h2>Ordinals live under a {@code Refs} key, and that key keeps its position</h2>
 * The pooled form is {@code externalCallsRefs: [0,3,7]}, not {@code externalCalls: [0,3,7]}. Two
 * reasons, both found the hard way:
 * <ul>
 *   <li>{@code EndpointInfo.externalCalls} is declared {@code List<Map<String,String>>}, so binding
 *       a document whose value is an array of integers <b>throws</b> — {@code load()} would fail
 *       outright on every pooled analysis.</li>
 *   <li>{@code AbstractAnalysisDataProvider.checkHasPrecomputedAggregates} decides "new format" by
 *       finding the literal {@code externalCalls} field on the first endpoint. That check now
 *       short-circuits on {@link #POOL_FIELD} instead, which is what makes renaming safe. Without
 *       it, the summary silently recomputes aggregates from the call tree and the collections and
 *       domains views change — with no error anywhere.</li>
 * </ul>
 * The refs key replaces the original <b>in its own position</b>, so an inflated endpoint has the
 * same field order, and therefore the same bytes, as one that was never pooled.
 */
final class EndpointArrayPool {

    private static final Logger log = LoggerFactory.getLogger(EndpointArrayPool.class);

    /** The pools themselves: {@code { "externalCalls": [ ... ], ... } }. */
    static final String POOL_FIELD = "endpointPool";

    /**
     * Only fields with real duplication. Adding a near-unique field here makes the document
     * bigger, not smaller — see the class javadoc.
     */
    static final List<String> POOLED_FIELDS = List.of("externalCalls", "dynamicFlows", "beans");

    /** {@code externalCalls} -> {@code externalCallsRefs}. */
    static String refsKey(String field) { return field + "Refs"; }

    private final ObjectMapper mapper;
    private final Map<String, Map<String, Integer>> ordinals = new LinkedHashMap<>();
    private final Map<String, List<JsonNode>> pools = new LinkedHashMap<>();
    private long references;

    EndpointArrayPool(ObjectMapper mapper) {
        this.mapper = mapper;
        for (String f : POOLED_FIELDS) {
            ordinals.put(f, new HashMap<>());
            pools.put(f, new ArrayList<>());
        }
    }

    /* ---------------- write ---------------- */

    /** Interns every entry of the pooled fields on one endpoint. Call for all endpoints first. */
    void scan(JsonNode endpoint) {
        for (String field : POOLED_FIELDS) {
            JsonNode arr = endpoint.get(field);
            if (arr == null || !arr.isArray()) continue;
            for (JsonNode item : arr) {
                intern(field, item);
                references++;
            }
        }
    }

    private int intern(String field, JsonNode item) {
        String key = item.toString();
        Map<String, Integer> ord = ordinals.get(field);
        Integer existing = ord.get(key);
        if (existing != null) return existing;
        List<JsonNode> pool = pools.get(field);
        int id = pool.size();
        ord.put(key, id);
        pool.add(item);
        return id;
    }

    boolean isEmpty() {
        return pools.values().stream().allMatch(List::isEmpty);
    }

    /**
     * Writes the pools. Must precede {@code endpoints}.
     *
     * <p>No separate format marker is emitted. An earlier version wrote {@code endpointPoolFormat}
     * as a top-level string, and it leaked straight into {@code /summary/header}, which copies the
     * scalar top-level fields — a 40-byte internal storage detail showing up in a public response.
     * The presence of {@link #POOL_FIELD} says everything the marker did, and it is an object, so
     * the header response skips it already.
     */
    void streamPool(JsonGenerator gen) throws IOException {
        gen.writeFieldName(POOL_FIELD);
        gen.writeStartObject();
        for (String field : POOLED_FIELDS) {
            List<JsonNode> pool = pools.get(field);
            if (pool.isEmpty()) continue;
            gen.writeFieldName(field);
            gen.writeStartArray();
            // Through the generator, not writeRawValue: a decompiled string constant can hold an
            // unpaired surrogate, which written raw fails the UTF-8 encoder mid-document. That
            // aborted three analyses when InvocationPool first did it the other way.
            for (JsonNode item : pool) mapper.writeValue(gen, item);
            gen.writeEndArray();
        }
        gen.writeEndObject();
        int distinct = pools.values().stream().mapToInt(List::size).sum();
        log.info("EndpointArrayPool: {} references over {} distinct entries across {} fields",
                references, distinct, POOLED_FIELDS.size());
    }

    /** Replaces each pooled field's objects with ordinals under its {@code Refs} key, in place. */
    void rewrite(ObjectNode endpoint) {
        for (String field : POOLED_FIELDS) {
            JsonNode arr = endpoint.get(field);
            if (arr == null || !arr.isArray() || arr.isEmpty()) continue;
            ArrayNode refs = mapper.createArrayNode();
            boolean allKnown = true;
            for (JsonNode item : arr) {
                Integer id = ordinals.get(field).get(item.toString());
                if (id == null) { allKnown = false; break; }
                refs.add(id.intValue());
            }
            if (allKnown) replaceInPlace(endpoint, field, refsKey(field), refs);
        }
    }

    /**
     * Swaps one field for another without moving it.
     *
     * <p>{@code remove} then {@code set} would append the new key at the end of the endpoint
     * object, changing field order for every endpoint in the document; inflation would then restore
     * the original key in the wrong place and every response echoing an endpoint would differ from
     * the pre-pooling bytes.
     */
    private void replaceInPlace(ObjectNode target, String oldField, String newField, JsonNode value) {
        List<Map.Entry<String, JsonNode>> entries = new ArrayList<>();
        target.fields().forEachRemaining(entries::add);
        target.removeAll();
        for (Map.Entry<String, JsonNode> e : entries) {
            if (e.getKey().equals(oldField)) target.set(newField, value);
            else target.set(e.getKey(), e.getValue());
        }
    }

    /* ---------------- read ---------------- */

    /** Turns ordinals back into the original objects. */
    static final class Inflater {
        private final Map<String, List<JsonNode>> pools;

        private Inflater(Map<String, List<JsonNode>> pools) { this.pools = pools; }

        static Inflater passthrough() { return new Inflater(Map.of()); }

        /** Built from a document's {@link #POOL_FIELD} object; absent means passthrough. */
        static Inflater of(JsonNode poolNode) {
            if (poolNode == null || !poolNode.isObject()) return passthrough();
            Map<String, List<JsonNode>> p = new LinkedHashMap<>();
            poolNode.fields().forEachRemaining(e -> {
                List<JsonNode> items = new ArrayList<>();
                e.getValue().forEach(items::add);
                p.put(e.getKey(), items);
            });
            return new Inflater(p);
        }

        boolean isEmpty() { return pools.isEmpty(); }

        /**
         * Resolves one field's value if it is an array of ordinals.
         *
         * <p>Distinguishes ordinals from real objects by element type, so a document written
         * before pooling — whose entries are objects — passes through untouched. There is no
         * migration step.
         *
         * @return the inflated array, or the input unchanged
         */
        /**
         * @param field the key as it appears in the document — either the original name (legacy,
         *              returned untouched) or its {@code Refs} form
         * @return the resolved array, or the input unchanged
         */
        JsonNode inflateField(String field, JsonNode value, ObjectMapper mapper) {
            if (value == null || !value.isArray() || value.isEmpty()) return value;
            String original = originalFieldFor(field);
            if (original == null) return value;          // not a pooled field
            List<JsonNode> pool = pools.get(original);
            if (pool == null) return value;
            if (!value.get(0).isNumber()) return value;   // legacy inline objects
            ArrayNode out = mapper.createArrayNode();
            for (JsonNode ref : value) {
                int id = ref.asInt(-1);
                // A dangling ordinal loses that one entry rather than failing the response.
                if (id >= 0 && id < pool.size()) out.add(pool.get(id));
            }
            return out;
        }

        /** The pooled field a document key refers to, or null if it is not one. */
        static String originalFieldFor(String documentKey) {
            for (String f : POOLED_FIELDS) {
                if (refsKey(f).equals(documentKey)) return f;
            }
            return null;
        }

        /** Inflates every pooled field on an endpoint node, preserving key order. */
        void inflateEndpoint(ObjectNode endpoint, ObjectMapper mapper) {
            if (pools.isEmpty()) return;
            for (String field : POOLED_FIELDS) {
                String rk = refsKey(field);
                JsonNode v = endpoint.get(rk);
                if (v == null) continue;
                JsonNode inflated = inflateField(rk, v, mapper);
                // Restore the ORIGINAL key, in the position the refs key occupied.
                List<Map.Entry<String, JsonNode>> entries = new ArrayList<>();
                endpoint.fields().forEachRemaining(entries::add);
                endpoint.removeAll();
                for (Map.Entry<String, JsonNode> e : entries) {
                    if (e.getKey().equals(rk)) endpoint.set(field, inflated);
                    else endpoint.set(e.getKey(), e.getValue());
                }
            }
        }
    }
}
