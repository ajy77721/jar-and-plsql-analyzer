package com.codeanalyzer.ingest.mongo.map.jar;

import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Ids;
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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * {@code bundled-jars.json} → {@code am_bundled_jar} + {@code am_bundled_jar_resource} — ING-116.
 *
 * <h2>Not read before this class existed</h2>
 *
 * <p>{@code bundled-jars.json} sits at the analysis root alongside {@code analysis.json}, and
 * {@link MongoIngestService#ingestJar} named exactly one file to read: its own comment says so —
 * <i>"Deliberately reads only {@code analysis.json}."</i> That comment is accurate about the
 * per-endpoint directories it was written to justify skipping; it was not, so far as any code
 * shows, a decision about this file. Nothing ever opened it.
 *
 * <h2>Why the split, measured rather than assumed</h2>
 *
 * <p>100 bundled jars in this corpus. Each carries {@code name}, {@code size}, a {@code manifest}
 * string, and {@code resources} — a map from resource file name to file content, the classic
 * data-keyed shape Splitter's {@code dataKeyedMap} check exists to catch (an arbitrary key becomes
 * a BSON field name). The worst single entry, embedded, reaches <b>465,644 bytes</b>
 * ({@code spring-boot-autoconfigure-3.2.5.jar}); split, the worst remaining scalar document is
 * <b>64,390 bytes</b> ({@code mssql-jdbc-12.6.1.jre11.jar}, whose manifest alone is 61,276 bytes) —
 * the plan's own measurement of "≤62 KB" undershoots this by about 2 KB, which is worth recording
 * rather than rounding away. 229 resource files across 87 jars become their own rows.
 *
 * <p>{@code resources} is removed from the entry before {@link Splitter#flatten} sees it, for the
 * reason recorded in {@code DagAndEndpointMapper}: {@code Splitter} emits child rows as a side
 * effect during flatten, so removing a field from the returned document after the fact would leave
 * an embedded copy already written.
 */
public final class BundledJarMapper implements ArtifactMapper {

    public static final String ARTIFACT = "bundled-jars.json";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return ARTIFACT;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        try (JsonParser p = json.getFactory().createParser(in)) {
            if (p.nextToken() != JsonToken.START_ARRAY) return;
            while (p.nextToken() == JsonToken.START_OBJECT) {
                JsonNode e = json.readTree(p);
                String name = e.path("name").asText(null);
                if (name == null) continue;

                String entityKey = ctx.entityKey(name);
                JsonNode resources = e.path("resources");
                if (e.isObject() && resources.isObject()) {
                    ((ObjectNode) e).remove("resources");
                }

                Document d = Splitter.flatten(ctx, sink, Collections.BUNDLED_JAR, entityKey, e);
                d.put("runId", ctx.runId());
                d.put("jarName", name);
                Identity ident = Identity.ofSource(entityKey, entityKey, e);
                ident.stampOnto(d);
                sink.emit(Collections.BUNDLED_JAR, d);
                sink.emit(RunMember.COLLECTION,
                        RunMember.of(ctx.runId(), ident, Collections.BUNDLED_JAR, null));

                emitResources(ctx, sink, entityKey, resources);
            }
        }
    }

    /**
     * One row per resource file, keyed by its position — not its name. A resource file name is
     * itself the data this method exists to stop turning into structure (a Mongo field name), so
     * it is stored as a value, the same as {@code Splitter} stores any other scalar array element.
     */
    private void emitResources(IngestContext ctx, DocSink sink, String jarEntityKey,
                               JsonNode resources) {
        if (!resources.isObject()) return;
        int ordinal = 0;
        Iterator<Map.Entry<String, JsonNode>> it = resources.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> res = it.next();
            // nested(), not of(): jarEntityKey is already an escaped, joined key. of() would
            // escape it AGAIN as a single "part" — its internal '~' separators would come out as
            // literal '\~' in the stored key. Caught by inspecting a stored document rather than
            // by a test; nested() is exactly the method IdCodec's own javadoc describes for this.
            Ids id = IdCodec.nested(Collections.BUNDLED_JAR_RESOURCE,
                    jarEntityKey, String.valueOf(ordinal));
            Document d = new Document()
                    .append("runId", ctx.runId())
                    .append("parentRef", jarEntityKey)
                    .append("resourceName", res.getKey())
                    .append("content", res.getValue().isTextual()
                            ? res.getValue().asText() : res.getValue().toString())
                    .append("ordinal", ordinal);
            d.put("key", id.key());
            sink.emit(Collections.BUNDLED_JAR_RESOURCE, d);
            ordinal++;
        }
    }
}
