package com.analyzer.storage.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PLAN-11 §2.1's hazard, and the test it asks for: <b>"the mapper must normalise on read: value
 * absent → no rows; scalar → one row; array → n rows. A test must feed it all three."</b>
 *
 * <p>The shapes are the ones the field scan actually observed in
 * {@code annotations[].attributes.value}: {@code @RequestMapping(value = "/orders")} yields a
 * string, {@code @RequestMapping(value = {"/orders", "/order"})} yields an array, and <b>both occur
 * inside one {@code analysis.json}</b>. Storing the scalar in a column and the array in a side table
 * would make "which endpoints map to {@code /orders}" answer differently depending on how the
 * annotation happened to be written — so there is one target shape, always with an ordinal.
 */
class OrdinalValuesTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String ART = "analysis.json";
    private static final String PATH = "annotations[].attributes.value";

    private static List<OrdinalValues.Value> of(String json) throws Exception {
        return OrdinalValues.of(M.readTree(json).get("value"), ART, PATH);
    }

    @Test
    @DisplayName("scalar → exactly one row, ordinal 0 — not a column")
    void scalarBecomesOneRow() throws Exception {
        assertEquals(List.of(new OrdinalValues.Value(0, "/orders")), of("{\"value\":\"/orders\"}"));
    }

    @Test
    @DisplayName("array → n rows in written order")
    void arrayBecomesNRows() throws Exception {
        assertEquals(List.of(new OrdinalValues.Value(0, "/orders"),
                             new OrdinalValues.Value(1, "/order")),
                of("{\"value\":[\"/orders\",\"/order\"]}"));
    }

    @Test
    @DisplayName("absent and null both emit nothing — PLAN-11 rule 4, or counts drift")
    void nullEmitsNothing() throws Exception {
        assertTrue(of("{}").isEmpty(), "absent must not become a row");
        assertTrue(of("{\"value\":null}").isEmpty(), "null must not become a zero row");
    }

    @Test
    @DisplayName("empty array is not the same as absent, and is still no rows")
    void emptyArrayEmitsNothing() throws Exception {
        assertTrue(of("{\"value\":[]}").isEmpty());
    }

    @Test
    @DisplayName("a null inside an array does not renumber its siblings")
    void nullElementKeepsOrdinals() throws Exception {
        // The ordinal describes the position the value was written at, so dropping element 0 must
        // not promote element 1 into its place — that would silently change which value "the first
        // one" refers to between two runs of a slightly different source file.
        assertEquals(List.of(new OrdinalValues.Value(1, "/order")),
                of("{\"value\":[null,\"/order\"]}"));
    }

    @Test
    @DisplayName("an object where only scalars were ever observed fails loudly — rule 6")
    void unobservedShapeThrows() {
        EntityShapeException e = assertThrows(EntityShapeException.class,
                () -> of("{\"value\":{\"path\":\"/orders\"}}"));
        // The artifact must be in the message: PLAN-11 §2.4 — a field means what it means in its
        // artifact, and a complaint naming only `value` would be unactionable.
        assertTrue(e.getMessage().contains(ART), e.getMessage());
        assertTrue(e.getMessage().contains(PATH), e.getMessage());
        assertEquals(ART, e.artifact());

        assertThrows(EntityShapeException.class, () -> of("{\"value\":[{\"a\":1}]}"),
                "a container inside the array is just as unobserved as one instead of it");
    }
}
