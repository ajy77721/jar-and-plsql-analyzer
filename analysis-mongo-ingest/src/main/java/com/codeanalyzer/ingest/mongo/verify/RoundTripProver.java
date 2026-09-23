package com.codeanalyzer.ingest.mongo.verify;

import com.fasterxml.jackson.databind.JsonNode;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Proves that nothing in the source JSON was lost on the way into MongoDB.
 *
 * <h2>Why this is a test and not a promise</h2>
 *
 * <p>The requirement — keep every field, because the UI may read one we have not audited — cannot
 * be met by inspection. A mapper that drops a field looks exactly like one that does not, and the
 * consequence shows up much later as an empty panel that nobody traces back to the schema.
 *
 * <p>So the check is mechanical: walk every leaf path of the source node, and assert each one is
 * present with an equal value in the stored document. Absences that are <em>deliberate</em> — null
 * and empty containers, which are omitted on write and read back identically — are accounted for
 * rather than excused, so a genuine loss cannot hide behind them.
 */
public final class RoundTripProver {

    /** A field present in the source and missing, or different, in the stored document. */
    public record Loss(String path, Object expected, Object actual) {
        @Override public String toString() {
            return path + ": expected " + expected + ", stored " + actual;
        }
    }

    private RoundTripProver() {}

    /** Every field of {@code source} that did not survive into {@code stored}. Empty is success. */
    public static List<Loss> compare(JsonNode source, Document stored) {
        List<Loss> losses = new ArrayList<>();
        walk("", source, stored, losses);
        return losses;
    }

    private static void walk(String path, JsonNode node, Object stored, List<Loss> out) {
        if (node == null || node.isNull()) return;                 // omitted by design
        if (node.isObject()) {
            if (node.isEmpty()) return;                            // omitted by design
            if (!(stored instanceof Document d)) { out.add(new Loss(path, "object", stored)); return; }
            node.fields().forEachRemaining(e ->
                    walk(path.isEmpty() ? e.getKey() : path + "." + e.getKey(),
                         e.getValue(), d.get(e.getKey()), out));
            return;
        }
        if (node.isArray()) {
            if (node.isEmpty()) return;                            // omitted by design
            if (!(stored instanceof List<?> l)) { out.add(new Loss(path, "array[" + node.size() + "]", stored)); return; }
            if (l.size() != node.size()) {
                out.add(new Loss(path + ".length", node.size(), l.size()));
                return;
            }
            for (int i = 0; i < node.size(); i++) walk(path + "[" + i + "]", node.get(i), l.get(i), out);
            return;
        }
        Object expected = scalar(node);
        if (!equalish(expected, stored)) out.add(new Loss(path, expected, stored));
    }

    private static Object scalar(JsonNode n) {
        if (n.isTextual()) return n.textValue();
        if (n.isInt())     return n.intValue();
        if (n.isLong())    return n.longValue();
        if (n.isBoolean()) return n.booleanValue();
        if (n.isDouble() || n.isFloat()) return n.doubleValue();
        return n.asText();
    }

    /**
     * Numeric width is not a loss.
     *
     * <p>An int read back as a long is the same value; failing on that would bury real losses in
     * noise and the check would stop being run. Anything else must match exactly.
     */
    private static boolean equalish(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (expected.equals(actual)) return true;
        if (expected instanceof Number a && actual instanceof Number b) {
            return a.doubleValue() == b.doubleValue();
        }
        return false;
    }
}
