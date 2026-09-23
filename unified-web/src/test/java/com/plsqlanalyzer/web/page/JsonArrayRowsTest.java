package com.plsqlanalyzer.web.page;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The streaming row iterator behind the two Code Structure endpoints.
 *
 * <p>What is worth testing here is not "does it read an array" but the three things that would
 * silently corrupt a page: skipping a row, applying the projection to the wrong row, and failing to
 * find the array inside a wrapper object.
 */
class JsonArrayRowsTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static List<JsonNode> drain(String json, String field,
                                        java.util.function.Function<JsonNode, JsonNode> proj)
            throws Exception {
        try (var parser = M.getFactory().createParser(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
             var rows = JsonArrayRows.of(parser, M, field, proj)) {
            List<JsonNode> out = new ArrayList<>();
            while (rows.hasNext()) out.add(rows.next());
            return out;
        }
    }

    @Test
    @DisplayName("iterates a top-level array in document order, every element exactly once")
    void topLevelArray() throws Exception {
        var rows = drain("[{\"a\":1},{\"a\":2},{\"a\":3}]", null, null);
        assertEquals(List.of(1, 2, 3), rows.stream().map(r -> r.path("a").asInt()).toList());
    }

    @Test
    @DisplayName("finds the array under a named field and skips the fields before it")
    void namedField() throws Exception {
        String json = "{\"meta\":{\"n\":9},\"other\":[1,2,3],\"messages\":[{\"a\":1},{\"a\":2}]}";
        var rows = drain(json, "messages", null);
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).path("a").asInt());
    }

    @Test
    @DisplayName("a missing array field iterates empty rather than throwing")
    void missingField() throws Exception {
        assertTrue(drain("{\"meta\":1}", "messages", null).isEmpty());
        assertTrue(drain("\"just a string\"", "messages", null).isEmpty());
    }

    @Test
    @DisplayName("the projection is applied per row, and a null projection result drops the row")
    void projection() throws Exception {
        var rows = drain("[{\"a\":1},{\"a\":2},{\"a\":3}]", null,
                r -> r.path("a").asInt() == 2 ? null : r);
        assertEquals(List.of(1, 3), rows.stream().map(r -> r.path("a").asInt()).toList());
    }

    @Test
    @DisplayName("a projected row does not retain the bulk it dropped")
    void projectionDropsBulk() throws Exception {
        String big = "x".repeat(4096);
        var rows = drain("[{\"name\":\"j\",\"body\":\"" + big + "\"}]", null,
                r -> {
                    ObjectNode o = M.createObjectNode();
                    o.put("name", r.path("name").asText());
                    return o;
                });
        assertEquals(1, rows.size());
        assertFalse(rows.get(0).has("body"),
                "the projection must not carry the field it exists to remove");
    }

    @Test
    @DisplayName("a non-object element is skipped, not fatal")
    void skipsNonObjects() throws Exception {
        var rows = drain("[{\"a\":1},\"oops\",42,[7,8],{\"a\":2}]", null, null);
        assertEquals(List.of(1, 2), rows.stream().map(r -> r.path("a").asInt()).toList());
    }

    @Test
    @DisplayName("hasNext is idempotent — calling it twice does not consume a row")
    void hasNextIsIdempotent() throws Exception {
        try (var parser = M.getFactory().createParser("[{\"a\":1},{\"a\":2}]");
             var rows = JsonArrayRows.of(parser, M, null, null)) {
            assertTrue(rows.hasNext());
            assertTrue(rows.hasNext());
            assertEquals(1, rows.next().path("a").asInt());
            assertEquals(2, rows.next().path("a").asInt());
            assertFalse(rows.hasNext());
        }
    }
}
