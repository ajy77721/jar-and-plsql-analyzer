package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.EndpointInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The offsets emitted by the streaming write (plan 2a) must address the exact bytes of each class
 * and endpoint element, so a reader can serve one record by seek.
 *
 * <p>The oracle is independent: Jackson's own byte positions for each array element, read back from
 * the written document. If the writer's offsets disagree with those by even one byte, a slice is not
 * the element and this fails.
 */
class AnalysisOffsetsGoldenTest {

    @TempDir
    Path tmp;

    private static final String JAR = "demo.jar";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JarDataPaths paths(InMemoryArtifactStore store) {
        return new JarDataPaths(tmp.resolve("jar").toString(), store);
    }

    private Path classesJsonl(int classes) throws Exception {
        Path f = tmp.resolve("classes.jsonl");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < classes; i++) {
            sb.append("{\"fullyQualifiedName\":\"com.demo.C").append(i)
              .append("\",\"packageName\":\"com.demo.p").append(i % 3)
              .append("\",\"simpleName\":\"C").append(i)
              .append("\",\"stereotype\":\"SERVICE\",\"methods\":[]}\n");
        }
        Files.writeString(f, sb.toString(), StandardCharsets.UTF_8);
        return f;
    }

    private List<EndpointInfo> endpoints(int n) {
        List<EndpointInfo> eps = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            EndpointInfo ep = new EndpointInfo();
            ep.setHttpMethod("GET");
            ep.setFullPath("/api/demo/" + i);
            ep.setControllerClass("com.demo.C" + i);
            ep.setControllerSimpleName("C" + i);
            ep.setMethodName("handle" + i);
            ep.setReturnType("String");
            eps.add(ep);
        }
        return eps;
    }

    @Test
    @DisplayName("class and endpoint offsets address the exact element bytes")
    void offsetsMatchIndependentBytePositions() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        int nClasses = 7;
        Path jsonl = classesJsonl(nClasses);
        List<EndpointInfo> eps = endpoints(3);

        new PersistenceService(paths, store).writeAnalysisStreaming(
                JAR, "Demo", 1000L, "2026-01-01T00:00:00", nClasses, eps.size(),
                jsonl, eps, "STATIC", null);

        byte[] doc = store.read(paths.analysisKey(JAR)).readAllBytes();

        List<long[]> classRanges = elementRanges(doc, "classes");
        List<long[]> epRanges = elementRanges(doc, "endpoints");
        assertEquals(nClasses, classRanges.size());
        assertEquals(eps.size(), epRanges.size());

        Map<String, AnalysisOffsetIndexReader.Segment> segs =
                new AnalysisOffsetIndexReader().load(store, paths.analysisOffsetsKey(JAR));
        var cs = segs.get(AnalysisOffsets.SEGMENT_CLASSES);
        var es = segs.get(AnalysisOffsets.SEGMENT_ENDPOINTS);
        assertNotNull(cs, "classes offsets must be written");
        assertNotNull(es, "endpoints offsets must be written");

        assertRangesMatch("classes", classRanges, cs);
        assertRangesMatch("endpoints", epRanges, es);

        // A slice read by offset is the element, byte for byte, and starts/ends like one.
        for (int i = 0; i < nClasses; i++) {
            byte[] slice = slice(doc, cs.offsets()[i], cs.lengths()[i]);
            assertEquals('{', slice[0]);
            assertEquals('}', slice[slice.length - 1]);
        }
    }

    @Test
    @DisplayName("the classes slice is byte-identical to the JSONL line that produced it")
    void classSliceEqualsSourceLine() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        Path jsonl = classesJsonl(4);
        new PersistenceService(paths, store).writeAnalysisStreaming(
                JAR, "Demo", 1L, "t", 4, 0, jsonl, List.of(), "STATIC", null);

        byte[] doc = store.read(paths.analysisKey(JAR)).readAllBytes();
        var cs = new AnalysisOffsetIndexReader().loadSegment(
                store, paths.analysisOffsetsKey(JAR), AnalysisOffsets.SEGMENT_CLASSES);
        List<String> lines = Files.readAllLines(jsonl, StandardCharsets.UTF_8).stream()
                .filter(l -> !l.isBlank()).toList();
        for (int i = 0; i < lines.size(); i++) {
            byte[] slice = slice(doc, cs.offsets()[i], cs.lengths()[i]);
            assertArrayEquals(lines.get(i).getBytes(StandardCharsets.UTF_8), slice,
                    "class " + i + " slice must be the original line bytes");
        }
    }

    @Test
    @DisplayName("the reader serves one class and one endpoint by seek")
    void readerServesBySeek() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        int nClasses = 5;
        Path jsonl = classesJsonl(nClasses);
        List<EndpointInfo> eps = endpoints(2);
        new PersistenceService(paths, store).writeAnalysisStreaming(
                JAR, "Demo", 1L, "t", nClasses, eps.size(), jsonl, eps, "STATIC", null);

        JsonNode whole = MAPPER.readTree(store.read(paths.analysisKey(JAR)));
        AnalysisOffsetIndexReader reader = new AnalysisOffsetIndexReader();

        JsonNode cls = reader.readClass(store, paths, JAR, 2);
        assertNotNull(cls);
        assertEquals(whole.path("classes").get(2), cls,
                "the sought class must equal the class in the document");

        JsonNode ep = reader.readEndpoint(store, paths, JAR, 1);
        assertNotNull(ep);
        assertEquals(whole.path("endpoints").get(1), ep,
                "the sought endpoint must equal the endpoint in the document");

        // Out of range and absent index both degrade to null, never throw.
        assertNull(reader.readClass(store, paths, JAR, 99));
        assertTrue(reader.load(store, ArtifactKey.of("jar", "nope", "analysis.offsets.json")).isEmpty());
    }

    @Test
    @DisplayName("the endpoint bytes equal what the previous streamed form produced")
    void endpointBytesMatchStreamedForm() throws Exception {
        var store = new InMemoryArtifactStore();
        var paths = paths(store);
        List<EndpointInfo> eps = endpoints(1);
        new PersistenceService(paths, store).writeAnalysisStreaming(
                JAR, "Demo", 1L, "t", 1, 1, classesJsonl(1), eps, "STATIC", null);

        byte[] doc = store.read(paths.analysisKey(JAR)).readAllBytes();
        var es = new AnalysisOffsetIndexReader().loadSegment(
                store, paths.analysisOffsetsKey(JAR), AnalysisOffsets.SEGMENT_ENDPOINTS);
        byte[] slice = slice(doc, es.offsets()[0], es.lengths()[0]);

        // Reproduce the OLD streamed serialisation from the public model: fields in EndpointInfo
        // order, callTree's slot replaced by the (null) rootId, fragments dropped. If the raw-bytes
        // rewrite changed a single byte, this differs.
        EndpointInfo ep = eps.get(0);
        ep.setCallTree(null);
        ep.setFragments(null);
        com.fasterxml.jackson.databind.node.ObjectNode on = MAPPER.valueToTree(ep);
        java.io.ByteArrayOutputStream bo = new java.io.ByteArrayOutputStream();
        try (com.fasterxml.jackson.core.JsonGenerator g = MAPPER.getFactory().createGenerator(bo)) {
            g.writeStartObject();
            var fields = on.fields();
            while (fields.hasNext()) {
                var e = fields.next();
                if ("callTree".equals(e.getKey())) {
                    g.writeFieldName(com.jaranalyzer.service.CallTreeFlattener.ROOT_ID_FIELD);
                    g.writeNull();
                } else if ("fragments".equals(e.getKey())
                        || com.jaranalyzer.service.CallTreeFlattener.ROOT_ID_FIELD.equals(e.getKey())
                        || com.jaranalyzer.service.CallTreeFlattener.FRAGMENT_IDS_FIELD.equals(e.getKey())) {
                    // dropped — the reference fields are written in callTree's slot (or at the end)
                } else {
                    g.writeFieldName(e.getKey());
                    g.writeTree(e.getValue());
                }
            }
            g.writeEndObject();
            g.flush();
        }
        assertArrayEquals(bo.toByteArray(), slice,
                "the raw-written endpoint must be byte-identical to the streamed form");
    }

    private static void assertRangesMatch(String label, List<long[]> ranges,
                                          AnalysisOffsetIndexReader.Segment seg) {
        assertEquals(ranges.size(), seg.rowCount(), label + " rowCount");
        for (int i = 0; i < ranges.size(); i++) {
            long[] r = ranges.get(i);
            assertEquals(r[0], seg.offsets()[i], label + " offset " + i);
            assertEquals(r[1] - r[0], seg.lengths()[i], label + " length " + i);
        }
    }

    private static byte[] slice(byte[] doc, long offset, long length) {
        byte[] out = new byte[(int) length];
        System.arraycopy(doc, (int) offset, out, 0, (int) length);
        return out;
    }

    /**
     * The byte range of every element of a top-level array, from Jackson's own token positions.
     * Independent of the writer's counter — this is what makes the comparison a check.
     */
    private static List<long[]> elementRanges(byte[] doc, String field) throws Exception {
        List<long[]> out = new ArrayList<>();
        try (JsonParser p = MAPPER.getFactory().createParser(doc)) {
            if (p.nextToken() != JsonToken.START_OBJECT) return out;
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String name = p.currentName();
                p.nextToken();
                if (field.equals(name)) {
                    if (p.currentToken() != JsonToken.START_ARRAY) break;
                    while (p.nextToken() != JsonToken.END_ARRAY) {
                        if (p.currentToken() != JsonToken.START_OBJECT) continue;
                        // getTokenLocation is the START of the token (`{`); getCurrentLocation is
                        // the parser's position after it — the two differ by exactly the token.
                        long start = p.getTokenLocation().getByteOffset();
                        p.skipChildren();          // leaves the parser on END_OBJECT
                        long end = p.getTokenLocation().getByteOffset() + 1;
                        out.add(new long[]{start, end});
                    }
                    break;
                }
                if (p.currentToken() == JsonToken.START_ARRAY
                        || p.currentToken() == JsonToken.START_OBJECT) {
                    p.skipChildren();
                }
            }
        }
        return out;
    }
}
