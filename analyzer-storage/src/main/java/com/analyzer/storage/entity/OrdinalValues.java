package com.analyzer.storage.entity;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * The scalar-or-array normaliser. PLAN-11 §2.1 and §4 rule 2.
 *
 * <p>Six field paths change shape in real data. The canonical one is
 * {@code annotations[].attributes.value}, which is a {@code string} for
 * {@code @RequestMapping(value = "/orders")} and an {@code array<string>} for
 * {@code @RequestMapping(value = {"/orders", "/order"})} — <b>both shapes inside one
 * {@code analysis.json}</b>. Storing the scalar in a column and the array in a side table would
 * make "which endpoints map to {@code /orders}" return different answers depending on how the
 * annotation happened to be written.
 *
 * <p>So there is exactly one target shape: <b>a child collection with an {@code ordinal}, always</b>,
 * even for the single-scalar case (one row, {@code ordinal = 0}).
 *
 * <ul>
 *   <li>absent or {@code null} → <b>no rows</b> (PLAN-11 rule 4: emit nothing for null, or counts
 *       drift)</li>
 *   <li>scalar → one row</li>
 *   <li>array → n rows in array order — the ordinal is a <em>position within one value</em>, which
 *       is stable, and is never used as a key (rule 5)</li>
 *   <li>object, or an array containing a container → {@link EntityShapeException} (rule 6)</li>
 * </ul>
 */
public final class OrdinalValues {

    private OrdinalValues() {
    }

    /** One ordinal-tagged value of a scalar-or-array field. */
    public record Value(int ordinal, String value) {
    }

    /**
     * @param artifact the artifact the node came from — part of the identity of a field, per
     *                 PLAN-11 §2.4, and the only thing that makes a shape complaint actionable
     * @param path     the JSON path within that artifact
     */
    public static List<Value> of(JsonNode node, String artifact, String path) {
        List<Value> out = new ArrayList<>();
        if (node == null || node.isMissingNode() || node.isNull()) return out;
        if (node.isValueNode()) {
            out.add(new Value(0, node.asText()));
            return out;
        }
        if (node.isArray()) {
            int ordinal = 0;
            for (JsonNode element : node) {
                // A null inside an array is still a null: it emits nothing, but it does NOT shift
                // the ordinals of its siblings, because the ordinal describes the position the
                // value was written at.
                if (element.isNull()) {
                    ordinal++;
                    continue;
                }
                if (!element.isValueNode()) {
                    throw new EntityShapeException(artifact, path + "[]",
                            element.getNodeType().toString().toLowerCase(), "a scalar");
                }
                out.add(new Value(ordinal++, element.asText()));
            }
            return out;
        }
        throw new EntityShapeException(artifact, path,
                node.getNodeType().toString().toLowerCase(), "a scalar or an array of scalars");
    }
}
