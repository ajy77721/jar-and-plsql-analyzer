package com.jaranalyzer.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * MID-103 — lets one property on {@link JarAnalysis} ({@code callTreeNodes}) be redirected to an
 * externally supplied streaming writer for exactly one call, without this model class needing to
 * know anything about {@code CallTreeFlattener} (a service class) or about spill files. The writer
 * is threaded through per-call via a {@link SerializerProvider} attribute — {@code writeFlattened}
 * sets it with {@code ObjectWriter.withAttribute}; every other caller that serialises a
 * {@code JarAnalysis} the ordinary way (a test constructing one directly, any future ad-hoc dump)
 * finds no attribute present and falls straight back to writing the plain, already-materialised
 * {@code JsonNode} — this class changes nothing for them.
 *
 * <p>Registered via {@code @JsonSerialize(using = ..., nullsUsing = ...)} on {@code
 * getCallTreeNodes()}, alongside {@code @JsonInclude(Include.ALWAYS)} overriding the class's own
 * {@code NON_NULL} default for this one property — needed because the streaming caller leaves the
 * field {@code null} on the POJO (there is no longer a materialised dictionary to hold), and the
 * class default would otherwise drop the property before this serializer ever ran.
 */
public final class StreamableNodeSerializer extends JsonSerializer<JsonNode> {

    /** Attribute key {@code writeFlattened} supplies via {@code ObjectWriter.withAttribute(...)}. */
    public static final String STREAM_ATTR = "com.jaranalyzer.model.callTreeNodesStreamer";

    @FunctionalInterface
    public interface StreamWriter {
        /** Writes the JSON *value* for this property — no surrounding field name. */
        void writeValue(JsonGenerator gen) throws IOException;
    }

    @Override
    public void serialize(JsonNode value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        Object attr = serializers.getAttribute(STREAM_ATTR);
        if (attr instanceof StreamWriter writer) {
            writer.writeValue(gen);
            return;
        }
        // No streaming writer in scope: ordinary serialization, unaffected by this class existing.
        if (value == null) {
            gen.writeNull();
        } else {
            gen.writeTree(value);
        }
    }
}
