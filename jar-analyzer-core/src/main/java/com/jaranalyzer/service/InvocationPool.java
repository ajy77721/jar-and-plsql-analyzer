package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stores each distinct {@code MethodCallInfo} once and references it by ordinal.
 *
 * <p>{@code classes[].methods[].invocations} is the largest field in the analysis document — 22.3 MB
 * of a measured 53 MB — and the same call appears under many methods. Measured on
 * {@code unified-web}: <b>110,234 invocations, 63,301 distinct (57.4% unique)</b>, so pooling saves
 * about 6.1 MB. That ratio is the reason this uses a small integer ordinal rather than a content
 * hash like {@link CallTreeFlattener}: at 110k references a 20-char hash id would itself cost
 * 2.4 MB and eat 40% of the saving.
 *
 * <h2>The document stays readable either way</h2>
 * Pooling is a <b>storage</b> representation only. {@link #inflate} restores the exact
 * {@code invocations} array in its original position, so every API response is byte-identical and
 * no UI changes. A document written before pooling existed has no {@link #POOL_FIELD} and its
 * inline {@code invocations} are passed through untouched — there is no migration step, and both
 * shapes are readable forever.
 *
 * <h2>Why the pool is built in a separate pass</h2>
 * The pool has to be written <i>before</i> {@code classes}, because every reader is a forward-only
 * streaming pass and must have the pool before the references to it. But the pool is only fully
 * known after every class has been seen. So {@link #scan} runs first over the classes JSONL, then
 * the pool is emitted, then {@link #rewrite} replaces each class's invocations with ordinals as the
 * classes are streamed out. This mirrors what {@link CallTreeFlattener} does for call-tree nodes.
 *
 * <p><b>Memory.</b> The pool holds one serialised copy of each distinct invocation: 13.8 MB for the
 * 63,301 above. That is proportional to distinct calls in the JAR, not to the document size, and is
 * the one structure here that is not spilled to disk. If a JAR ever makes this the peak, spill it
 * the way {@code CallTreeFlattener} does rather than capping it.
 */
public final class InvocationPool {

    private static final Logger log = LoggerFactory.getLogger(InvocationPool.class);

    /** The pool array, written once before {@code classes}. */
    public static final String POOL_FIELD = "invocationPool";

    /** Replaces {@code invocations} in a method: ordinals into {@link #POOL_FIELD}. */
    public static final String REFS_FIELD = "invocationRefs";

    /** The original inline field, still read for documents written before pooling. */
    public static final String INLINE_FIELD = "invocations";

    private final ObjectMapper mapper;

    /** Canonical serialised invocation → its ordinal. */
    private final Map<String, Integer> ordinals = new HashMap<>();

    /**
     * Invocations by ordinal, in assignment order.
     *
     * <p>Held as nodes and written through the {@link JsonGenerator} rather than as pre-serialised
     * text written with {@code writeRawValue}. Decompiled string constants can contain an unpaired
     * surrogate — {@code 0xDF00} occurs in this JAR — and {@code JsonNode.toString()} emits that as
     * a raw char, which the UTF-8 encoder then rejects with "Incomplete surrogate pair" and fails
     * the whole analysis. Going through the generator applies the same escaping that wrote the
     * source JSONL in the first place.
     */
    private final List<JsonNode> pool = new ArrayList<>();

    private long totalReferences;

    InvocationPool(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /* ---------------- write side ---------------- */

    /**
     * First pass: assigns an ordinal to every distinct invocation across all classes.
     *
     * <p>A malformed line is skipped rather than failing: {@link #rewrite} leaves anything it
     * cannot parse inline, so a class missed here still ends up in the document with its
     * invocations intact — just not pooled.
     */
    void scan(BufferedReader classes) throws IOException {
        String line;
        while ((line = classes.readLine()) != null) {
            if (line.isBlank()) continue;
            JsonNode cls;
            try {
                cls = mapper.readTree(line);
            } catch (Exception skip) {
                continue;
            }
            for (JsonNode method : cls.path("methods")) {
                JsonNode invs = method.get(INLINE_FIELD);
                if (invs == null || !invs.isArray()) continue;
                for (JsonNode inv : invs) {
                    intern(inv);
                    totalReferences++;
                }
            }
        }
        if (!pool.isEmpty()) {
            log.info("InvocationPool: {} references over {} distinct invocations ({}% unique)",
                    totalReferences, pool.size(),
                    totalReferences == 0 ? 0 : (pool.size() * 100 / totalReferences));
        }
    }

    private int intern(JsonNode invocation) {
        // The key is the serialised form, so two invocations are the same only if they would write
        // identical bytes. Field order comes from the same serialiser for every class, so equal
        // objects always produce equal keys.
        String json = invocation.toString();
        Integer existing = ordinals.get(json);
        if (existing != null) return existing;
        int id = pool.size();
        ordinals.put(json, id);
        pool.add(invocation);
        return id;
    }

    boolean isEmpty() { return pool.isEmpty(); }

    int size() { return pool.size(); }

    /** Writes the pool array. Must be called before any class referencing it. */
    void streamPool(JsonGenerator gen) throws IOException {
        gen.writeFieldName(POOL_FIELD);
        gen.writeStartArray();
        for (JsonNode inv : pool) {
            mapper.writeValue(gen, inv);
        }
        gen.writeEndArray();
    }

    /**
     * Second pass: rewrites one class line, replacing each method's {@code invocations} with
     * {@link #REFS_FIELD} ordinals.
     *
     * <p>The refs field is placed <b>at the position the inline field occupied</b>, so
     * {@link #inflate} can restore the original key order exactly — which is what makes the
     * resulting API bytes identical.
     *
     * <p>Returns <b>null</b> when nothing changed, so the caller can write the original line
     * verbatim with {@code writeRawValue} — cheaper, and it keeps the exact bytes (including
     * escape sequences) that the parser produced.
     *
     * @return the rewritten class node, or null to write the input line unchanged
     */
    JsonNode rewrite(String line) {
        if (pool.isEmpty()) return null;
        JsonNode cls;
        try {
            cls = mapper.readTree(line);
        } catch (Exception skip) {
            return null;
        }
        JsonNode methods = cls.path("methods");
        if (!methods.isArray()) return null;

        boolean changed = false;
        for (JsonNode method : methods) {
            if (!(method instanceof ObjectNode mo)) continue;
            JsonNode invs = mo.get(INLINE_FIELD);
            if (invs == null || !invs.isArray()) continue;

            ArrayNode refs = mapper.createArrayNode();
            boolean allKnown = true;
            for (JsonNode inv : invs) {
                Integer id = ordinals.get(inv.toString());
                if (id == null) { allKnown = false; break; }
                refs.add(id.intValue());
            }
            // An unknown invocation means this line was not part of the scan; leaving it inline is
            // correct and still readable, so it is not an error.
            if (!allKnown) continue;

            replaceInPlace(mo, INLINE_FIELD, REFS_FIELD, refs);
            changed = true;
        }
        return changed ? cls : null;
    }

    /**
     * Swaps one field for another <b>without moving it</b>.
     *
     * <p>{@code ObjectNode.remove} then {@code set} would append the new field at the end, which
     * changes key order for every method in the document. Inflation would then restore
     * {@code invocations} in the wrong place and every response that echoes a method object would
     * differ from the pre-pooling bytes — the exact thing this change must not do. So the object is
     * rebuilt in order.
     */
    private void replaceInPlace(ObjectNode target, String oldField, String newField, JsonNode value) {
        List<Map.Entry<String, JsonNode>> entries = new ArrayList<>();
        target.fields().forEachRemaining(entries::add);
        target.removeAll();
        for (Map.Entry<String, JsonNode> e : entries) {
            if (e.getKey().equals(oldField)) {
                target.set(newField, value);
            } else {
                target.set(e.getKey(), e.getValue());
            }
        }
    }

    /* ---------------- read side ---------------- */

    /**
     * Restores inline {@code invocations} from a pool.
     *
     * <p>Built from the {@link #POOL_FIELD} array of a stored document. A document without that
     * field yields {@link #passthrough()}, which leaves every class exactly as written — that is
     * how pre-pooling analyses keep working with no migration.
     */
    public static final class Inflater {
        private final List<JsonNode> pool;

        private Inflater(List<JsonNode> pool) { this.pool = pool; }

        /**
         * Advances a streaming parser to the start of the {@code classes} array, capturing the
         * invocation pool on the way past it.
         *
         * <p>Every reader of the analysis document is a forward-only pass that seeks to
         * {@code classes}; the pool is written before it precisely so that one pass can pick it up.
         * Centralised here because there are several such seeks and each of them silently produces
         * uninflated methods if it forgets the pool — and "silently" is the problem: the
         * invocations simply come back empty.
         *
         * @return the inflater to use for the classes that follow; a passthrough one for a document
         *         with no pool
         */
        public static Inflater seekClasses(com.fasterxml.jackson.core.JsonParser parser,
                                           ObjectMapper mapper) throws IOException {
            Inflater found = passthrough();
            while (parser.nextToken() != null) {
                if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.FIELD_NAME) continue;
                String name = parser.currentName();
                if (POOL_FIELD.equals(name)) {
                    parser.nextToken();
                    found = of(mapper.readTree(parser));
                } else if ("classes".equals(name)) {
                    parser.nextToken();
                    return found;
                }
            }
            return found;
        }

        public static Inflater of(JsonNode poolArray) {
            if (poolArray == null || !poolArray.isArray()) return passthrough();
            List<JsonNode> p = new ArrayList<>(poolArray.size());
            poolArray.forEach(p::add);
            return new Inflater(p);
        }

        /** For documents with no pool, and for readers that have not seen one yet. */
        public static Inflater passthrough() { return new Inflater(List.of()); }

        public boolean isEmpty() { return pool.isEmpty(); }

        /**
         * Rewrites one method object in place: {@link #REFS_FIELD} becomes {@code invocations},
         * in the same position, with the pooled objects restored.
         *
         * <p>An ordinal outside the pool is skipped rather than throwing: a truncated or
         * hand-edited document should lose that one call, not fail the whole response.
         */
        public void inflateMethod(ObjectNode method, ObjectMapper mapper) {
            JsonNode refs = method.get(REFS_FIELD);
            if (refs == null || !refs.isArray()) return;
            ArrayNode invs = mapper.createArrayNode();
            for (JsonNode ref : refs) {
                int id = ref.asInt(-1);
                if (id >= 0 && id < pool.size()) invs.add(pool.get(id));
            }
            List<Map.Entry<String, JsonNode>> entries = new ArrayList<>();
            method.fields().forEachRemaining(entries::add);
            method.removeAll();
            for (Map.Entry<String, JsonNode> e : entries) {
                if (e.getKey().equals(REFS_FIELD)) {
                    method.set(INLINE_FIELD, invs);
                } else {
                    method.set(e.getKey(), e.getValue());
                }
            }
        }

        /** Inflates every method of a class node. Returns the same node, mutated. */
        public JsonNode inflateClass(JsonNode cls, ObjectMapper mapper) {
            if (pool.isEmpty()) return cls;
            for (JsonNode method : cls.path("methods")) {
                if (method instanceof ObjectNode mo) inflateMethod(mo, mapper);
            }
            return cls;
        }
    }
}
