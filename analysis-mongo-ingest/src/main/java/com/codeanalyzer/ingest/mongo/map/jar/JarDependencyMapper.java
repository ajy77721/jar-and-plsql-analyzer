package com.codeanalyzer.ingest.mongo.map.jar;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Ids;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code jar-dep-map.json} → {@code am_jar_dependency} — ING-117.
 *
 * <h2>Shape, and why it needs its own reader rather than {@code Splitter}</h2>
 *
 * <p>The file is one object: {@code {"spring-web-6.1.6.jar": ["ControllerA", "ControllerB", ...]}},
 * a jar name mapped to the classes in <em>this</em> application that reference it. The jar name is
 * the classic data-keyed shape — 18 keys in this corpus, each one a value rather than a field a
 * schema should have — so {@code Splitter} is not reached for; a value cannot become a Mongo field
 * name here any more than a resource file name could in {@code BundledJarMapper}.
 *
 * <h2>Not embedded, though the plan recommended it</h2>
 *
 * <p>{@code RELATIONAL-MODEL-PLAN.md} §2364/§2780 models this as
 * {@code bundled_jar.dependsOn[]} — embedded in the jar it describes, measured at 1,445 B per jar,
 * comfortably small. Split anyway, for the reason {@code TablesIndexMapper}'s correction and
 * {@code BundledJarMapper}'s own resources already record in this module: the no-arrays invariant
 * is structural, not a per-array judgement call, because "bounded today" is a property of the data
 * and an embedded array is one schema change from not being.
 *
 * <p>Measured: 18 jars, 303 (jar, class) edges, worst jar referenced by 63 classes.
 */
public final class JarDependencyMapper implements ArtifactMapper {

    public static final String ARTIFACT = "jar-dep-map.json";

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
                String jarName = p.currentName();
                if (p.nextToken() != JsonToken.START_ARRAY) {
                    p.skipChildren();
                    continue;
                }
                String jarEntityKey = ctx.entityKey(jarName);
                int ordinal = 0;
                while (p.nextToken() != JsonToken.END_ARRAY) {
                    String className = p.getValueAsString();
                    if (className == null || className.isBlank()) continue;
                    Ids id = IdCodec.nested(Collections.JAR_DEPENDENCY, jarEntityKey,
                            String.valueOf(ordinal));
                    Document d = new Document()
                            .append("runId", ctx.runId())
                            .append("parentRef", jarEntityKey)
                            .append("jarName", jarName)
                            .append("dependentClassName", className)
                            .append("ordinal", ordinal);
                    d.put("key", id.key());
                    sink.emit(Collections.JAR_DEPENDENCY, d);
                    ordinal++;
                }
            }
        }
    }
}
