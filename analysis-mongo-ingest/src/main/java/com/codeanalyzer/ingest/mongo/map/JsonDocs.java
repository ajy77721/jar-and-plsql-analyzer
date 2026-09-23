package com.codeanalyzer.ingest.mongo.map;

import com.fasterxml.jackson.databind.JsonNode;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts Jackson nodes to BSON, preserving <b>every</b> field.
 *
 * <h2>The lossless rule, implemented</h2>
 *
 * <p>The requirement is explicit: no field from the source JSON may be dropped, because the UI may
 * read one we have not audited and a missing field breaks a screen in a way nobody traces back to
 * the schema. So conversion is <em>structural</em> — it copies whatever is there — rather than a
 * hand-written mapping of known field names. A hand-written mapper silently drops anything added
 * upstream; this cannot.
 *
 * <p>Nulls and empty containers are omitted on write, which is <b>not</b> a loss: a missing key and
 * a key set to null read identically through the driver, and roughly 60% of some entries are
 * absent-by-default placeholders. The distinction that matters — "we recorded nothing" versus "we
 * recorded that there is nothing" — is carried by explicit fields where the model needs it, never
 * inferred from key presence.
 */
public final class JsonDocs {

    private JsonDocs() {}

    /** Copies every field of {@code node} into a BSON document. */
    public static Document toDocument(JsonNode node) {
        Document d = new Document();
        node.fields().forEachRemaining(e -> {
            Object v = toValue(e.getValue());
            if (v != null) d.put(e.getKey(), v);
        });
        return d;
    }

    /** Copies every field except those named — used where a field becomes its own collection. */
    public static Document toDocumentExcept(JsonNode node, String... exclude) {
        List<String> skip = List.of(exclude);
        Document d = new Document();
        node.fields().forEachRemaining(e -> {
            if (skip.contains(e.getKey())) return;
            Object v = toValue(e.getValue());
            if (v != null) d.put(e.getKey(), v);
        });
        return d;
    }

    /** Returns null for JSON null and for empty containers; see the class note on why. */
    public static Object toValue(JsonNode n) {
        if (n == null || n.isNull()) return null;
        if (n.isTextual()) return n.textValue();
        if (n.isInt()) return n.intValue();
        if (n.isLong()) return n.longValue();
        if (n.isDouble() || n.isFloat()) return n.doubleValue();
        if (n.isBigInteger() || n.isBigDecimal()) return n.decimalValue().toPlainString();
        if (n.isBoolean()) return n.booleanValue();
        if (n.isArray()) {
            if (n.isEmpty()) return null;
            List<Object> out = new ArrayList<>(n.size());
            // Element nulls ARE preserved: dropping one would shift every later element's
            // position, and several arrays here are positional (INTO targets by select-item).
            n.forEach(e -> out.add(e.isNull() ? null : toValue(e)));
            return out;
        }
        if (n.isObject()) {
            if (n.isEmpty()) return null;
            return toDocument(n);
        }
        return n.asText();
    }
}
