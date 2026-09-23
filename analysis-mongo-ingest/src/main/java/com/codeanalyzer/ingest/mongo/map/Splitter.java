package com.codeanalyzer.ingest.mongo.map;

import com.codeanalyzer.ingest.mongo.id.Escaping;
import com.fasterxml.jackson.databind.JsonNode;
import org.bson.Document;

import java.util.Locale;
import java.util.Map;

/**
 * Turns a JSON object into a document containing <b>no arrays at any depth</b>, emitting every
 * array element as a row in its own collection.
 *
 * <h2>The rule, and why it is absolute</h2>
 *
 * <p>An earlier version embedded arrays that were "small on average". Averages are the wrong
 * statistic for a ceiling: one generated lexer holds 2,504 fields, one class 1,245 methods, one
 * node reached 293 KB with half its bytes in a single array. Any array grows with the code being
 * analysed, which is not something this system controls, and the failure arrives as a 16 MB write
 * rejection naming a document rather than the decision behind it.
 *
 * <p>Splitting walks the JSON structurally rather than working from a list of known field names, so
 * an array added upstream is split automatically instead of quietly becoming an embedded one.
 *
 * <h2>Collection names come from the schema, never from the data</h2>
 *
 * <p>This is the correction that matters most here. Naming child collections by recursing through
 * nested objects produced collections named after <em>values</em>: {@code usedBy[].operations} is a
 * map keyed {@code SELECT}/{@code INSERT}/{@code DELETE}, and the first version created
 * {@code am_db_object__used_by__s_e_l_e_c_t} and one sibling per operation encountered. Collection
 * count then grows with the content of the analysed code — unbounded, and every new value silently
 * creates another unindexed collection.
 *
 * <p>So the name is fixed by the <b>outermost array field only</b>. Everything below that subtree,
 * however deeply nested, lands in that same collection and records where it came from in
 * {@code path}. The number of collections is therefore bounded by the number of declared fields,
 * which is a property of the schema and cannot be moved by any input.
 */
public final class Splitter {

    /** {@code am_db_object} + {@code usedBy} → {@code am_db_object__used_by}. */
    public static String childCollection(String parentCollection, String field) {
        return parentCollection + "__" + snake(field);
    }

    /**
     * camelCase and ALLCAPS both to snake_case.
     *
     * <p>The naive "insert an underscore before every capital" rule turns {@code DELETE} into
     * {@code d_e_l_e_t_e}. Only a lower-to-upper transition marks a word boundary.
     */
    static String snake(String name) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean boundary = i > 0 && Character.isUpperCase(c)
                    && (Character.isLowerCase(name.charAt(i - 1))
                        || (i + 1 < name.length() && Character.isLowerCase(name.charAt(i + 1))));
            if (boundary) sb.append('_');
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

    private Splitter() {}

    /**
     * Extends a parent key with further components, <b>without re-escaping the parent</b>.
     *
     * <p>The parent key is itself a {@code ~}-joined string. Passing it through
     * {@code Escaping.join} as a single component escaped its separators a second time, producing
     * keys like {@code ORACLE\\~ORACLEDEMO\\~LOCAL\\~…} that grow more mangled at every level
     * of nesting and never match the parent they name. Only the new components need escaping.
     */
    private static String childKey(String parentKey, String... parts) {
        StringBuilder sb = new StringBuilder(parentKey);
        for (String part : parts) sb.append(Escaping.SEP).append(Escaping.escape(part));
        return sb.toString();
    }

    /**
     * Copies {@code node}'s scalars into a document and emits its arrays as child rows.
     *
     * @param collection the parent's collection, used to derive child collection names
     * @param parentKey  the parent's business key; every child stores it as {@code parentRef}
     */
    public static Document flatten(IngestContext ctx, DocSink sink,
                                   String collection, String parentKey, JsonNode node) {
        return flatten(ctx, sink, collection, parentKey, node, null, "");
    }

    /**
     * @param targetCollection non-null once inside an array subtree: every descendant row goes
     *                         here, so nested keys cannot invent new collections
     * @param path             dotted location <b>relative to {@code parentKey}</b>. Relative, not
     *                         absolute from the document root: a row is reattached by its owner,
     *                         and an absolute path cannot say which of several array elements it
     *                         belongs to.
     */
    private static Document flatten(IngestContext ctx, DocSink sink, String collection,
                                    String parentKey, JsonNode node,
                                    String targetCollection, String path) {
        Document out = new Document();
        node.fields().forEachRemaining(e -> {
            String field = e.getKey();
            JsonNode v = e.getValue();
            String childPath = path.isEmpty() ? field : path + "." + field;
            if (v.isArray()) {
                if (!v.isEmpty()) {
                    String target = targetCollection != null
                            ? targetCollection
                            : childCollection(collection, field);
                    emitArray(ctx, sink, collection, parentKey, field, v, target, childPath);
                }
            } else if (v.isObject()) {
                if (!v.isEmpty()) {
                    // The owner does NOT change when descending into a nested object: the object
                    // is part of this document, not a row of its own. Only the relative path
                    // grows. Minting an intermediate key here instead left arrays inside nested
                    // objects owned by a key that no row ever had, so nothing could reattach them.
                    Document nested = flatten(ctx, sink, collection,
                            parentKey, v, targetCollection, childPath);
                    if (!nested.isEmpty()) out.put(field, nested);
                }
            } else {
                Object scalar = JsonDocs.toValue(v);
                if (scalar != null) out.put(field, scalar);
            }
        });
        return out;
    }

    private static void emitArray(IngestContext ctx, DocSink sink, String parentCollection,
                                  String parentKey, String field, JsonNode arr,
                                  String target, String path) {
        int ordinal = 0;
        for (JsonNode el : arr) {
            String key = childKey(parentKey, path, String.valueOf(ordinal));
            Document d;
            if (el.isObject()) {
                // A new owner begins here — this element IS a row — so the relative path resets.
                // Stay in the same collection: a nested array inside an element belongs to this
                // field's subtree, and naming a collection after its keys is what produced the
                // value-derived explosion.
                d = flatten(ctx, sink, parentCollection, key, el, target, "");
            } else {
                // Scalar elements get rows too. The rule admits no "but this one is only strings"
                // exception, because that exception is how arrays creep back in.
                d = new Document("value", JsonDocs.toValue(el));
            }
            d.put("key", key);
            d.put("parentRef", parentKey);
            d.put("runId", ctx.runId());
            d.put("field", field);
            d.put("path", path);
            d.put("ordinal", ordinal);
            sink.emit(target, d);
            ordinal++;
        }
    }

    /** True when {@code value} holds an array anywhere — the invariant this class maintains. */
    /**
     * Largest key set a nested object may have before it is treated as keyed by data.
     *
     * <p>Set against measurement, not taste. Every legitimate nested object in the stored corpus is
     * a fixed schema shape; the widest observed data-keyed map was 99 keys (`localVariables`), and
     * the widest legitimate nested object was well under this. A schema object does not grow with
     * the corpus, so any bound comfortably above the schema and below the data is correct — 32
     * leaves room for a wide record without admitting a map.
     */
    public static final int MAX_SCHEMA_KEYS = 32;

    /**
     * An object whose <em>keys come from the data</em> — the array rule's blind spot.
     *
     * <h2>Why the array rule was not enough</h2>
     *
     * <p>{@link #containsArray} exists because an array grows without bound. A map keyed by data
     * grows without bound in exactly the same way and was never checked, because a map is not an
     * array. Two instances reached the database before this was noticed: an endpoint's
     * `aggregatedCollectionDomains`, keyed by collection name (ING-113), and a method's
     * `localVariables`, keyed by variable name — 11,996 keys across 2,857 methods.
     *
     * <h2>Two independent harms</h2>
     *
     * <ul>
     *   <li><b>Unaddressable.</b> A key containing a dot cannot be reached by a query path: it is
     *       ambiguous with a nested document. 69 of 155 collection names measured contain one. A
     *       key beginning with {@code $} collides with operator syntax.</li>
     *   <li><b>Unindexable and unbounded.</b> "Which methods declare `conn`" cannot use an index
     *       when the name is a field, and the key set grows with the corpus.</li>
     * </ul>
     *
     * @return a description of the first offending key, or null when the document is clean
     */
    public static String dataKeyedMap(Object value) {
        return dataKeyedMap(value, "", 0);
    }

    private static String dataKeyedMap(Object value, String path, int depth) {
        if (!(value instanceof Document d)) return null;
        if (depth > 0) {
            if (d.size() > MAX_SCHEMA_KEYS) {
                return path + " has " + d.size() + " keys; a schema object does not grow with the "
                        + "corpus, so this is a map keyed by data — split it into rows";
            }
            for (String key : d.keySet()) {
                if (key.indexOf('.') >= 0) {
                    return path + "." + key + " contains a dot; no query path can address it, "
                            + "because a dotted path is ambiguous with a nested document";
                }
                if (key.startsWith("$")) {
                    return path + "." + key + " begins with '$', which collides with operator syntax";
                }
            }
        }
        for (Map.Entry<String, Object> e : d.entrySet()) {
            String found = dataKeyedMap(e.getValue(),
                    path.isEmpty() ? e.getKey() : path + "." + e.getKey(), depth + 1);
            if (found != null) return found;
        }
        return null;
    }

    public static boolean containsArray(Object value) {
        if (value instanceof java.util.List) return true;
        if (value instanceof Document d) {
            for (Map.Entry<String, Object> e : d.entrySet()) {
                if (containsArray(e.getValue())) return true;
            }
        }
        return false;
    }
}
