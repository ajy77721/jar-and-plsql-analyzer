package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Invocation pooling: stores each distinct call once, and must be invisible to every reader.
 *
 * <p>Measured on {@code unified-web}: 110,234 invocations, 63,301 distinct, and the document went
 * from 52,750,850 to 44,937,916 bytes — 14.8% — with all 804 compared API responses byte-identical
 * apart from timestamps. These tests pin the properties that proof depended on.
 */
class InvocationPoolTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private String inv(String owner, String name) {
        return "{\"ownerClass\":\"" + owner + "\",\"methodName\":\"" + name
                + "\",\"descriptor\":\"()V\",\"returnType\":\"void\",\"opcode\":182,\"lineNumber\":7}";
    }

    /** A class whose two methods both call the same target — the case pooling exists for. */
    private String classLine() {
        return "{\"fullyQualifiedName\":\"com.demo.A\",\"simpleName\":\"A\",\"methods\":["
                + "{\"name\":\"one\",\"descriptor\":\"()V\",\"invocations\":["
                + inv("com.demo.Repo", "save") + "," + inv("com.demo.Repo", "find") + "],"
                + "\"startLine\":1},"
                + "{\"name\":\"two\",\"descriptor\":\"()V\",\"invocations\":["
                + inv("com.demo.Repo", "save") + "],\"startLine\":9}"
                + "],\"stereotype\":\"SERVICE\"}";
    }

    private BufferedReader reader(String... lines) {
        return new BufferedReader(new StringReader(String.join("\n", lines)));
    }

    @Test
    @DisplayName("duplicates across methods collapse to one pool entry")
    void duplicatesCollapse() throws Exception {
        var pool = new InvocationPool(mapper);
        pool.scan(reader(classLine()));
        // three references, two distinct calls
        assertEquals(2, pool.size(), "Repo.save appears in both methods and must be stored once");
    }

    @Test
    @DisplayName("a rewritten class round-trips to byte-identical inflated JSON")
    void roundTripIsExact() throws Exception {
        String original = classLine();
        var pool = new InvocationPool(mapper);
        pool.scan(reader(original));

        JsonNode rewritten = pool.rewrite(original);
        assertNotNull(rewritten, "the class has poolable invocations");
        assertFalse(rewritten.toString().contains("\"invocations\""),
                "invocations must be replaced by refs: " + rewritten);
        assertTrue(rewritten.toString().contains("\"invocationRefs\""));

        var inflater = InvocationPool.Inflater.of(poolArray(pool));
        JsonNode back = inflater.inflateClass(rewritten.deepCopy(), mapper);

        // THE property the whole change rests on: what a reader sees is what was written before.
        assertEquals(mapper.readTree(original), back,
                "an inflated class must equal the original exactly, or API responses shift");
    }

    @Test
    @DisplayName("inflation restores invocations in their ORIGINAL key position")
    void keyOrderIsPreserved() throws Exception {
        String original = classLine();
        var pool = new InvocationPool(mapper);
        pool.scan(reader(original));
        JsonNode back = InvocationPool.Inflater.of(poolArray(pool))
                .inflateClass(pool.rewrite(original), mapper);

        // Not just equal content — equal ORDER. Remove-then-set would append `invocations` at the
        // end of every method object, and every response echoing a method would differ from the
        // pre-pooling bytes even though the data matched.
        var names = new java.util.ArrayList<String>();
        back.path("methods").get(0).fieldNames().forEachRemaining(names::add);
        assertEquals(java.util.List.of("name", "descriptor", "invocations", "startLine"), names,
                "invocations must come back where it was, got " + names);
    }

    @Test
    @DisplayName("a lone surrogate in a string constant survives a write")
    void loneSurrogateSurvives() throws Exception {
        // This crashed a real analysis: "Incomplete surrogate pair: first char 0xDF00". Decompiled
        // string constants can contain an unpaired surrogate, and JsonNode.toString() emits it as a
        // raw char which the UTF-8 encoder then rejects — aborting the write and, worse, leaving a
        // truncated analysis.json behind. Writing through the generator applies the same escaping
        // that produced the source JSONL.
        String weird = "{\"fullyQualifiedName\":\"com.demo.B\",\"methods\":[{\"name\":\"m\","
                + "\"invocations\":[{\"ownerClass\":\"com.demo.R\",\"methodName\":\"q\","
                + "\"recentStringArgs\":[\"\\uDF00\"],\"opcode\":182,\"lineNumber\":1}]}]}";
        var pool = new InvocationPool(mapper);
        pool.scan(reader(weird));
        assertEquals(1, pool.size());

        var out = new ByteArrayOutputStream();
        try (JsonGenerator gen = mapper.getFactory().createGenerator(out)) {
            gen.writeStartObject();
            pool.streamPool(gen);          // must not throw
            gen.writeFieldName("classes");
            gen.writeStartArray();
            mapper.writeValue(gen, pool.rewrite(weird));
            gen.writeEndArray();
            gen.writeEndObject();
        }
        String json = out.toString(StandardCharsets.UTF_8);
        assertTrue(json.contains("invocationPool"), "the pool must have been written");
        assertNotNull(mapper.readTree(json), "and the result must be parseable");
    }

    @Test
    @DisplayName("a class with nothing poolable is left byte-for-byte alone")
    void unchangedClassReturnsNull() throws Exception {
        String noInvocations = "{\"fullyQualifiedName\":\"com.demo.C\",\"methods\":"
                + "[{\"name\":\"m\",\"descriptor\":\"()V\"}]}";
        var pool = new InvocationPool(mapper);
        pool.scan(reader(classLine(), noInvocations));
        // null means "write the original line verbatim" — cheaper, and it keeps the exact escape
        // sequences the parser produced.
        assertNull(pool.rewrite(noInvocations));
    }

    @Test
    @DisplayName("a document with no pool passes classes through untouched")
    void legacyDocumentsStillWork() throws Exception {
        // There is no migration step: analyses already on disk have inline invocations and must
        // keep reading forever.
        String original = classLine();
        var passthrough = InvocationPool.Inflater.passthrough();
        assertTrue(passthrough.isEmpty());
        JsonNode node = mapper.readTree(original);
        assertEquals(mapper.readTree(original), passthrough.inflateClass(node, mapper),
                "a pre-pooling class must be returned exactly as-is");
    }

    @Test
    @DisplayName("an out-of-range ref drops that call instead of failing the response")
    void danglingRefIsSurvivable() throws Exception {
        ObjectNode method = mapper.createObjectNode();
        method.put("name", "m");
        method.putArray("invocationRefs").add(0).add(999);
        var inflater = InvocationPool.Inflater.of(mapper.readTree("[" + inv("com.demo.R", "q") + "]"));
        inflater.inflateMethod(method, mapper);
        // A truncated or hand-edited document should lose one call, not 500 the whole endpoint.
        assertEquals(1, method.path("invocations").size(),
                "the valid ref resolves and the dangling one is skipped");
    }

    @Test
    @DisplayName("seekClasses captures the pool and stops at the classes array")
    void seekCapturesPool() throws Exception {
        String doc = "{\"jarName\":\"x.jar\",\"invocationPool\":[" + inv("com.demo.R", "q")
                + "],\"classes\":[" + classLine() + "]}";
        try (var parser = mapper.getFactory().createParser(doc)) {
            var inflater = InvocationPool.Inflater.seekClasses(parser, mapper);
            assertFalse(inflater.isEmpty(), "the pool must have been picked up on the way past");
            assertEquals(com.fasterxml.jackson.core.JsonToken.START_ARRAY, parser.currentToken(),
                    "the parser must be positioned at the start of classes");
        }
    }

    @Test
    @DisplayName("seekClasses on a pre-pooling document yields a passthrough inflater")
    void seekWithoutPool() throws Exception {
        String doc = "{\"jarName\":\"x.jar\",\"classes\":[" + classLine() + "]}";
        try (var parser = mapper.getFactory().createParser(doc)) {
            var inflater = InvocationPool.Inflater.seekClasses(parser, mapper);
            assertTrue(inflater.isEmpty());
            assertEquals(com.fasterxml.jackson.core.JsonToken.START_ARRAY, parser.currentToken());
        }
    }

    /** Serialises the pool the way the document does, so the inflater can be built from it. */
    private JsonNode poolArray(InvocationPool pool) throws Exception {
        var out = new ByteArrayOutputStream();
        try (JsonGenerator gen = mapper.getFactory().createGenerator(out)) {
            gen.writeStartObject();
            pool.streamPool(gen);
            gen.writeEndObject();
        }
        return mapper.readTree(out.toByteArray()).get(InvocationPool.POOL_FIELD);
    }
}
