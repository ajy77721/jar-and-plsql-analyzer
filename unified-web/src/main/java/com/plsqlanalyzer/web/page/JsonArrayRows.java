package com.plsqlanalyzer.web.page;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * The rows of a large JSON array, one at a time, off a {@link JsonParser}.
 *
 * <p><b>Why not {@code readTree} on the whole document.</b> {@link JsonFacetPager} already selects a
 * page with a bounded heap, so the only remaining place a large facet could cost memory
 * proportional to its size is the parse. Reading the array element by element removes that: the
 * peak is one row, whatever the file is. It is the difference between a 3 MB
 * {@code bundled-jars.json} costing 3 MB and costing a few kilobytes.
 *
 * <p><b>The projection is applied here, not later.</b> Some documents carry, inside each row, bulk
 * the list view never renders — the manifest and resource bodies of a bundled JAR, the method
 * summaries of a class. Projecting as the row is read means that bulk is parsed and dropped
 * immediately rather than retained for the whole scan, so a row's cost is its projection's cost.
 *
 * <p>Not thread-safe, and the returned nodes are only valid until {@link #next()} is called again
 * is <em>not</em> a restriction here — each {@code next()} returns a freshly built node, which is
 * what lets the pager keep a page of them in its heap.
 */
public final class JsonArrayRows implements Iterator<JsonNode>, Closeable {

    private final JsonParser parser;
    private final ObjectMapper mapper;
    private final Function<JsonNode, JsonNode> projection;
    private boolean exhausted;
    private JsonNode pending;

    private JsonArrayRows(JsonParser parser, ObjectMapper mapper,
                          Function<JsonNode, JsonNode> projection, boolean positioned) {
        this.parser = parser;
        this.mapper = mapper;
        this.projection = projection;
        this.exhausted = !positioned;
    }

    /**
     * Positions on a top-level array, or on the array under {@code arrayField} of a top-level
     * object, and iterates its elements.
     *
     * @param arrayField the field holding the array, or {@code null} when the document <i>is</i> the
     *                   array
     * @param projection applied to each element as it is read; {@code null} for the row as-is
     * @throws IOException if the document is not readable
     */
    public static JsonArrayRows of(JsonParser parser, ObjectMapper mapper, String arrayField,
                                   Function<JsonNode, JsonNode> projection) throws IOException {
        JsonToken first = parser.nextToken();
        if (first == JsonToken.START_ARRAY) {
            return new JsonArrayRows(parser, mapper, projection, true);
        }
        if (first != JsonToken.START_OBJECT || arrayField == null) {
            // Neither shape. An empty iteration rather than an exception: the caller distinguishes
            // "no data" (204) from "the filter matched nothing" by whether the document existed,
            // not by whether it happened to parse into the expected shape.
            return new JsonArrayRows(parser, mapper, projection, false);
        }
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            boolean wanted = arrayField.equals(parser.currentName());
            JsonToken value = parser.nextToken();
            if (wanted && value == JsonToken.START_ARRAY) {
                return new JsonArrayRows(parser, mapper, projection, true);
            }
            if (value == JsonToken.START_ARRAY || value == JsonToken.START_OBJECT) {
                parser.skipChildren();
            }
        }
        return new JsonArrayRows(parser, mapper, projection, false);
    }

    @Override
    public boolean hasNext() {
        if (pending != null) return true;
        if (exhausted) return false;
        try {
            while (true) {
                JsonToken t = parser.nextToken();
                if (t == null || t == JsonToken.END_ARRAY) {
                    exhausted = true;
                    return false;
                }
                if (t != JsonToken.START_OBJECT) {
                    // A scalar or nested array where a row was expected. Skipped rather than
                    // fatal, matching what the tree-streaming code already does for a malformed
                    // classes[] entry.
                    if (t == JsonToken.START_ARRAY) parser.skipChildren();
                    continue;
                }
                JsonNode row = mapper.readTree(parser);
                JsonNode projected = projection == null ? row : projection.apply(row);
                if (projected == null) continue;
                pending = projected;
                return true;
            }
        } catch (IOException e) {
            exhausted = true;
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonNode next() {
        if (!hasNext()) throw new NoSuchElementException();
        JsonNode row = pending;
        pending = null;
        return row;
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }
}
