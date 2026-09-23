package com.codeanalyzer.ingest.mongo.map.jar;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code analysis.json} → the Java model: classes, methods, fields, and the invocation junction.
 *
 * <h2>Streamed, because this file is the scaling problem in miniature</h2>
 *
 * <p>The measured file is 49.4 MB for 97,607 lines of Java. At the stated target of 100M lines that
 * is roughly a 50 GB single JSON document, and it is read whole today — the analyser fails long
 * before MongoDB would. This mapper cannot fix the producer, but it refuses to repeat its mistake:
 * the top-level arrays are walked token by token and each element is converted and emitted before
 * the next is read, so peak memory is one class rather than 1,633.
 *
 * <h2>Classes hold no methods and no fields</h2>
 *
 * <p>Embedded, one generated parser class reaches <b>5.23 MB</b> — 31% of the BSON limit in a single
 * document, and it grows with the grammar. Referenced, the same class is 605 bytes. The mean (14 KB)
 * is what makes this look safe and is the wrong statistic entirely: 1.3% of classes hold the
 * problem, and all of them are machine-generated, so the tail grows on its own schedule.
 *
 * <h2>Invocations are pooled by the producer, and stay pooled</h2>
 *
 * <p>{@code invocationPool} holds 64,443 distinct call targets referenced 111,467 times from
 * {@code methods[].invocationRefs}. Expanding those references into one document per reference
 * would multiply the largest collection on the JAR side by 1.7 for no gain. The pool becomes
 * {@code am_invocation}; the references become a junction, which is what a many-to-many actually is.
 */
public final class AnalysisJsonMapper implements ArtifactMapper {

    public static final String ARTIFACT = "analysis.json";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return ARTIFACT;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        try (JsonParser p = json.getFactory().createParser(in)) {
            if (p.nextToken() != JsonToken.START_OBJECT) return;
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String field = p.currentName();
                p.nextToken();
                switch (field) {
                    case "invocationPool" -> readInvocationPool(ctx, sink, p);
                    case "classes" -> readClasses(ctx, sink, p);
                    default -> p.skipChildren();   // handled by other mappers, or scalar metadata
                }
            }
        }
    }

    private void readInvocationPool(IngestContext ctx, DocSink sink, JsonParser p) throws IOException {
        if (p.currentToken() != JsonToken.START_ARRAY) { p.skipChildren(); return; }
        int index = 0;
        while (p.nextToken() != JsonToken.END_ARRAY) {
            JsonNode entry = json.readTree(p);
            String entityKey = IdCodec.invocation(ctx.entityKey(), index).key();
            Document d = Splitter.flatten(ctx, sink, Collections.INVOCATION, entityKey, entry);
            d.put("runId", ctx.runId());
            d.put("poolIndex", index);
            Identity.ofSource(entityKey, entityKey, entry).stampOnto(d);
            sink.emit(Collections.INVOCATION, d);
            ctx.putInvocationKey(index, entityKey);
            index++;
        }
    }

    private void readClasses(IngestContext ctx, DocSink sink, JsonParser p) throws IOException {
        if (p.currentToken() != JsonToken.START_ARRAY) { p.skipChildren(); return; }
        while (p.nextToken() != JsonToken.END_ARRAY) {
            emitClass(ctx, sink, json.readTree(p));
        }
    }

    private void emitClass(IngestContext ctx, DocSink sink, JsonNode c) {
        String fqn = c.path("fullyQualifiedName").asText(null);
        if (fqn == null) return;
        // Keyed by the JAR, not the run: a run-scoped key can never dedup across runs.
        String classKey = IdCodec.javaClass(ctx.entityKey(), fqn).key();

        // Everything except the two unbounded arrays. Splitter takes the rest (annotations,
        // interfaces) into their own collections, so the class document holds scalars only.
        JsonNode methods = c.path("methods");
        JsonNode fields = c.path("fields");
        Document d = Splitter.flatten(ctx, sink, Collections.JAVA_CLASS, classKey,
                withoutMethodsAndFields(c));
        d.put("runId", ctx.runId());
        d.put("fullyQualifiedName", fqn);
        d.put("methodCount", methods.isArray() ? methods.size() : 0);
        d.put("fieldCount", fields.isArray() ? fields.size() : 0);

        Identity ident = Identity.ofSource(classKey, classKey, c);
        ident.stampOnto(d);
        sink.emit(Collections.JAVA_CLASS, d);
        sink.emit(RunMember.COLLECTION,
                RunMember.of(ctx.runId(), ident, Collections.JAVA_CLASS, null));

        if (fields.isArray()) {
            for (JsonNode f : fields) emitField(ctx, sink, classKey, f);
        }
        if (methods.isArray()) {
            for (JsonNode m : methods) emitMethod(ctx, sink, classKey, m);
        }
    }

    /** Jackson has no "copy without these fields", and mutating the parsed node would be worse. */
    private JsonNode withoutMethodsAndFields(JsonNode c) {
        com.fasterxml.jackson.databind.node.ObjectNode copy = c.deepCopy();
        copy.remove("methods");
        copy.remove("fields");
        return copy;
    }

    private void emitField(IngestContext ctx, DocSink sink, String classKey, JsonNode f) {
        String name = f.path("name").asText(null);
        if (name == null) return;
        String entityKey = IdCodec.javaField(classKey, name).key();
        Document d = Splitter.flatten(ctx, sink, Collections.JAVA_FIELD, entityKey, f);
        d.put("runId", ctx.runId());
        d.put("classRef", classKey);
        d.put("name", name);
        Identity.ofSource(entityKey, entityKey, f).stampOnto(d);
        sink.emit(Collections.JAVA_FIELD, d);
    }

    /**
     * The declared locals of one method, as rows.
     *
     * <p>Ordinal preserves declaration order, which the map's iteration order carried implicitly and
     * a relation would otherwise lose. The variable's own value — its type, or whatever the analyser
     * recorded — is kept under {@code detail} rather than spread into columns, because it is not a
     * fixed shape and inventing one would be guessing at the analyser's output.
     */
    private void emitLocalVariables(IngestContext ctx, DocSink sink, String methodKey,
                                    JsonNode locals) {
        if (locals == null || !locals.isObject()) return;
        int ordinal = 0;
        var names = locals.fieldNames();
        while (names.hasNext()) {
            String variable = names.next();
            String key = IdCodec.nested(Collections.JAVA_METHOD_LOCAL_VAR, methodKey,
                    String.valueOf(ordinal), variable).key();
            Document row = new Document("_id", key)
                    .append("key", key)
                    .append("runId", ctx.runId())
                    .append("methodRef", methodKey)
                    .append("variableName", variable)
                    .append("ordinal", ordinal++);
            JsonNode detail = locals.get(variable);
            if (detail != null && detail.isValueNode() && !detail.isNull()) {
                row.append("detail", detail.asText());
            }
            sink.emit(Collections.JAVA_METHOD_LOCAL_VAR, row);
        }
    }

    private void emitMethod(IngestContext ctx, DocSink sink, String classKey, JsonNode m) {
        String name = m.path("name").asText(null);
        if (name == null) return;
        String descriptor = m.path("descriptor").asText("");
        String entityKey = IdCodec.javaMethod(classKey, name, descriptor).key();

        // invocationRefs is handled as a junction rather than split like an ordinary array: each
        // reference is an index into the pool, and a junction row carries the resolved target.
        com.fasterxml.jackson.databind.node.ObjectNode copy = m.deepCopy();
        JsonNode refs = copy.remove("invocationRefs");
        // localVariables is an OBJECT keyed by variable name, so Splitter — which splits arrays —
        // walks past it and the map is stored inline. That makes field names out of data: the key
        // set is unbounded (99 in the widest method measured), "which methods declare `conn`"
        // cannot be indexed, and a Java identifier may legally contain '$', which is not a safe
        // field name. Removed here and emitted as rows below.
        JsonNode locals = copy.remove("localVariables");

        Document d = Splitter.flatten(ctx, sink, Collections.JAVA_METHOD, entityKey, copy);
        d.put("runId", ctx.runId());
        d.put("classRef", classKey);
        d.put("name", name);
        d.put("descriptor", descriptor);
        d.put("invocationCount", refs != null && refs.isArray() ? refs.size() : 0);

        d.put("localVariableCount", locals != null && locals.isObject() ? locals.size() : 0);

        Identity.ofSource(entityKey, entityKey, m).stampOnto(d);
        sink.emit(Collections.JAVA_METHOD, d);

        emitLocalVariables(ctx, sink, entityKey, locals);

        if (refs != null && refs.isArray()) {
            int ordinal = 0;
            for (JsonNode r : refs) {
                int poolIndex = r.asInt(-1);
                if (poolIndex < 0) continue;
                String jKey = IdCodec.methodInvocation(entityKey, poolIndex, ordinal).key();
                Document j = new Document("runId", ctx.runId())
                        .append("methodRef", entityKey)
                        .append("invocationRef", ctx.invocationKey(poolIndex))
                        .append("poolIndex", poolIndex)
                        .append("ordinal", ordinal);
                Identity.of(jKey, jKey, j).stampOnto(j);
                sink.emit(Collections.METHOD_INVOCATION, j);
                ordinal++;
            }
        }
    }
}
