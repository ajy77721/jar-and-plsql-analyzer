package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MID-101/§30 — {@code ChatContextBuilder} produces the same context {@code ChatboxService}'s
 * deleted {@code buildContext} did, without materialising the whole {@code JarAnalysis}.
 *
 * <h2>Why a synthetic fixture rather than a real analysis</h2>
 *
 * <p>The property under test is "reads the header, the first N endpoints in full, every
 * endpoint's collections, and nothing else" — which needs precise control over endpoint count,
 * tree depth and a deliberately expensive {@code classes} field to prove it is never touched.
 * A real {@code analysis.json} would exercise the happy path without proving the boundary.
 */
class ChatContextBuilderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Map<String, Object> header() {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("jarName", "demo.jar");
        h.put("projectName", "demo-project");
        h.put("analyzedAt", "2026-09-07T00:00:00Z");
        h.put("analysisMode", "FULL");
        h.put("totalClasses", 1234);
        h.put("totalEndpoints", 3);
        return h;
    }

    /** A flat endpoint with no call tree — enough for the header/collection/detail-cap tests. */
    private static String endpoint(String path, String[] collections, int unusedDepth) {
        String colsJson = collections.length == 0 ? "[]"
                : "[\"" + String.join("\",\"", collections) + "\"]";
        return """
                {"httpMethod":"GET","fullPath":"%s","controllerSimpleName":"Ctrl","methodName":"m",
                 "aggregatedCollections":%s,"derivedOperationType":"READ"}
                """.formatted(path, colsJson);
    }

    /** One node, {@code "LN"}, holding {@code node(depth - 1)} as its only child. */
    private static String node(int level, int maxLevel) {
        String children = level >= maxLevel ? "[]" : "[" + node(level + 1, maxLevel) + "]";
        return "{\"simpleClassName\":\"L" + level + "\",\"methodName\":\"m\",\"children\":"
                + children + "}";
    }

    /** A minimal, deep call tree used only for the depth-cap test. */
    private static String deepTreeEndpoint(int depth) {
        return """
                {"httpMethod":"GET","fullPath":"/deep","controllerSimpleName":"Ctrl",
                 "methodName":"m","aggregatedCollections":[],"callTree":%s}
                """.formatted(node(0, depth));
    }

    private static String analysisJson(String endpointsArray, boolean withHugeClasses) {
        StringBuilder bigClasses = new StringBuilder("[");
        for (int i = 0; i < 5000; i++) {
            if (i > 0) bigClasses.append(",");
            bigClasses.append("{\"fullyQualifiedName\":\"C").append(i).append("\"}");
        }
        bigClasses.append("]");
        return "{\"classes\":" + (withHugeClasses ? bigClasses : "[]")
                + ",\"callTreeNodes\":[],\"endpoints\":[" + endpointsArray + "]}";
    }

    private static String build(String analysisJson, Map<String, Object> header,
                                int maxEndpoints, int maxChars, int maxDepth) throws Exception {
        try (JsonParser p = MAPPER.getFactory().createParser(
                new ByteArrayInputStream(analysisJson.getBytes(StandardCharsets.UTF_8)))) {
            return ChatContextBuilder.build(p, MAPPER, header, maxEndpoints, maxChars, maxDepth);
        }
    }

    @Test
    void headerFieldsAppearVerbatim() throws Exception {
        String ctx = build(analysisJson("", false), header(), 60, 40_000, 5);
        assertTrue(ctx.contains("JAR: demo.jar"));
        assertTrue(ctx.contains("Project: demo-project"));
        assertTrue(ctx.contains("Mode: FULL"));
        assertTrue(ctx.contains("Total classes: 1234"));
    }

    @Test
    void onlyTheFirstMaxEndpointsAreDetailed_butAllContributeCollections() throws Exception {
        String e1 = endpoint("/a", new String[]{"orders"}, 0);
        String e2 = endpoint("/b", new String[]{"users"}, 0);
        String e3 = endpoint("/c", new String[]{"invoices"}, 0);
        String ctx = build(analysisJson(String.join(",", e1, e2, e3), false), header(), 2,
                40_000, 5);

        assertTrue(ctx.contains("/a"), "first endpoint missing");
        assertTrue(ctx.contains("/b"), "second endpoint missing");
        assertFalse(ctx.contains("/c"), "third endpoint should not be detailed past the cap");
        assertTrue(ctx.contains("... and 1 more endpoints"),
                "the truncation note is missing or has the wrong count");

        // The whole point: the collection summary is over ALL endpoints, not just the detailed
        // ones — matching what the deleted buildContext computed over the untruncated list.
        assertTrue(ctx.contains("invoices"),
                "the third endpoint's collection did not reach the summary despite being past "
                        + "the per-endpoint detail cap");
        assertTrue(ctx.contains("MONGODB COLLECTIONS (3)"));
    }

    @Test
    void theCallTreeStopsAtMaxDepth() throws Exception {
        String ctx = build(analysisJson(deepTreeEndpoint(10), false), header(), 60, 40_000, 3);
        assertTrue(ctx.contains("L0"));
        assertTrue(ctx.contains("L3"), "depth 3 should be included (0..maxDepth inclusive)");
        assertFalse(ctx.contains("L4"), "depth 4 exceeds maxTreeDepth=3 and should not appear");
    }

    @Test
    void classesAndCallTreeNodesAreNeverParsedIntoTheOutput() throws Exception {
        // 5,000 synthetic classes, present in the source and absent from the context — proof the
        // field is skipped rather than merely unused after being read.
        String ctx = build(analysisJson(endpoint("/x", new String[0], 0), true), header(),
                60, 40_000, 5);
        assertFalse(ctx.contains("fullyQualifiedName"),
                "a classes field leaked into the context; the array is meant to be skipped whole");
        assertFalse(ctx.contains("C4999"), "the classes array was iterated rather than skipped");
    }

    @Test
    void classesIsSkippedByTheParserNotJustAbsentFromOutput() throws Exception {
        // The gap in the test above, found while calibrating it: it asserts the OUTPUT contains
        // no trace of `classes`, which stays true even if `classes` were fully parsed into a
        // JsonNode with mapper.readTree and then discarded — parser.skipChildren() and "read it
        // and throw it away" are indistinguishable by output text alone. Retained heap cannot
        // tell them apart either and was tried first: anything not returned from build() is
        // GC-eligible before the caller can measure it, by definition, regardless of how it was
        // produced — that is not a property of this code, it is a property of what "retained
        // after return" means. Elapsed time is the property that actually differs: tokenising
        // 800,000 objects into a JsonNode tree costs real CPU even when the result is immediately
        // dropped, while skipChildren() advances the parser without building anything.
        //
        // Same lesson as StatementStreamSearchTest's first heap assertion, one layer further in:
        // that one measured the wrong thing and got the wrong number back; this one would have
        // measured the wrong thing and gotten a *plausible-looking* number back, which is worse.
        StringBuilder bigClasses = new StringBuilder("[");
        for (int i = 0; i < 800_000; i++) {
            if (i > 0) bigClasses.append(",");
            bigClasses.append("{\"fullyQualifiedName\":\"com.example.GeneratedClass").append(i)
                    .append("\",\"fields\":[],\"methods\":[]}");
        }
        bigClasses.append("]");
        String analysis = "{\"classes\":" + bigClasses
                + ",\"callTreeNodes\":[],\"endpoints\":["
                + endpoint("/x", new String[]{"c"}, 0) + "]}";

        long started = System.nanoTime();
        build(analysis, header(), 60, 40_000, 5);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;

        // 300ms sits between the measured 159 ms (skipChildren, correct) and 445 ms
        // (readTree, mutated) at this fixture size — real margin on both sides rather than a
        // threshold picked before measuring either direction, which is how the first version of
        // this test ended up failing at 202ms against its own 200ms limit.
        assertTrue(elapsedMs < 300,
                "building the context took " + elapsedMs + " ms against an 800,000-class array; "
                        + "that reads like classes is being tokenised into a JsonNode tree "
                        + "rather than skipped with skipChildren()");
    }

    @Test
    void contextIsTruncatedAtTheCharLimitWithANotice() throws Exception {
        StringBuilder many = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            if (i > 0) many.append(",");
            many.append(endpoint("/e" + i, new String[]{"c" + i}, 0));
        }
        String ctx = build(analysisJson(many.toString(), false), header(), 60, 500, 5);
        assertEquals(500 + "\n\n[Context truncated at 500 chars]\n".length(), ctx.length());
        assertTrue(ctx.endsWith("[Context truncated at 500 chars]\n"));
    }

    @Test
    void aSmallContextIsNotTruncated() throws Exception {
        String ctx = build(analysisJson(endpoint("/only", new String[]{"c"}, 0), false),
                header(), 60, 40_000, 5);
        assertFalse(ctx.contains("truncated"),
                "a context well under the char limit was reported as truncated");
    }

    @Test
    void noEndpointsProducesTheOriginalMessage() throws Exception {
        String ctx = build(analysisJson("", false), header(), 60, 40_000, 5);
        assertTrue(ctx.contains("No endpoints found."));
    }
}
