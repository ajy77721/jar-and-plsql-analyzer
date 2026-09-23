package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The contract that makes the flat storage format safe to adopt: flattening a call tree into the
 * node dictionary and expanding it again reproduces the ORIGINAL JSON byte for byte.
 *
 * <p>Every API response that carries a call tree is ultimately
 * {@code objectMapper.writeValue(callNode)} — {@code streamCallTree}, the per-endpoint
 * {@code call-tree.json} sidecar, and the endpoint documents in {@code analysis.json} all emit
 * exactly those bytes. So byte equality here is the whole "the UI must not change" requirement,
 * checked at the one place it can be checked exhaustively rather than endpoint by endpoint.
 */
class CallTreeFlattenRoundTripTest {

    private final ObjectMapper mapper = new ObjectMapper();

    /** Flatten to a dictionary, expand back, return the JSON text. */
    private String roundTrip(CallNode root) throws IOException {
        ByteArrayOutputStream spill = new ByteArrayOutputStream();
        String rootId;
        String dictJson;
        try (CallTreeFlattener f = new CallTreeFlattener(mapper, spill,
                () -> new ByteArrayInputStream(spill.toByteArray()))) {
            rootId = f.flatten(root);
            StringWriter sw = new StringWriter();
            try (JsonGenerator gen = mapper.getFactory().createGenerator(sw)) {
                gen.writeStartObject();
                f.streamNodes(gen);
                gen.writeEndObject();
            }
            dictJson = sw.toString();
        }

        JsonNode doc = mapper.readTree(dictJson);
        var parser = mapper.getFactory().createParser(
                mapper.writeValueAsBytes(doc.get(CallTreeFlattener.NODES_FIELD)));
        parser.nextToken();
        CallTreeInflater inflater = CallTreeInflater.read(parser, mapper);

        StringWriter out = new StringWriter();
        try (JsonGenerator gen = mapper.getFactory().createGenerator(out)) {
            inflater.expand(rootId, gen);
        }
        return out.toString();
    }

    @Test
    void sharedSubtreeExpandsIdenticallyOnEveryPath() throws Exception {
        // The DAG that causes the blow-up: one shared node referenced from two parents. Nested
        // JSON has to write it twice; the dictionary stores it once and must still write it twice.
        CallNode shared = node("com.x.Repo", "findAll");
        shared.getCollectionsAccessed().add("orders");
        shared.setOperationType("READ");
        shared.getOperationTypes().add("READ");

        CallNode a = node("com.x.SvcA", "run");
        a.getChildren().add(shared);
        CallNode b = node("com.x.SvcB", "run");
        b.getChildren().add(shared);

        CallNode root = node("com.x.Ctrl", "handle");
        root.getChildren().add(a);
        root.getChildren().add(b);

        assertEquals(mapper.writeValueAsString(root), roundTrip(root));
    }

    @Test
    void recursiveBackEdgeSurvivesRoundTrip() throws Exception {
        // A back-edge node is childless and flagged recursive, so it hashes differently from the
        // real node with the same signature. If it did not, the id graph could contain a cycle
        // and expansion would not terminate.
        CallNode root = node("com.x.Svc", "loop");
        CallNode backEdge = node("com.x.Svc", "loop");
        backEdge.setRecursive(true);
        backEdge.setCallType("RECURSIVE");
        backEdge.setDispatchType("RECURSIVE");
        CallNode mid = node("com.x.Svc", "step");
        mid.getChildren().add(backEdge);
        root.getChildren().add(mid);

        assertEquals(mapper.writeValueAsString(root), roundTrip(root));
    }

    @Test
    void callSiteVariantsOfTheSameMethodStayDistinct() throws Exception {
        // CollectionResolver.tagCallSiteCollection produces copies with the same nodeId but
        // different collectionsAccessed. Keying the dictionary on nodeId would merge them and
        // silently change what the API serves; keying on content must not.
        CallNode base = node("com.x.Bulk", "addInsert");
        CallNode v1 = base.copyForCallSiteTagging();
        v1.getCollectionsAccessed().add("WIP_A");
        CallNode v2 = base.copyForCallSiteTagging();
        v2.getCollectionsAccessed().add("WIP_B");

        CallNode root = node("com.x.Ctrl", "handle");
        root.getChildren().add(v1);
        root.getChildren().add(v2);

        String out = roundTrip(root);
        assertEquals(mapper.writeValueAsString(root), out);
        assertTrue(out.contains("WIP_A") && out.contains("WIP_B"),
                "both call-site variants must survive");
    }

    @Test
    void richFieldsAndFieldOrderAredPreserved() throws Exception {
        CallNode root = node("com.x.Svc", "rich");
        root.setSourceCode("public void rich() { }");
        root.setLineCount(7);
        root.setStereotype("SERVICE");
        root.getAnnotations().add("Transactional");
        root.setAnnotationDetails(List.of(Map.of("name", "Transactional", "attrs", Map.of("readOnly", true))));
        root.getStringLiterals().add("orders");
        root.getCollectionDomains().put("orders", "SALES");
        root.getCollectionSources().put("orders", "REPOSITORY_MAPPING");
        root.setEntityFieldMappings(Map.of("orders", Map.of("id", "_id")));
        root.setCollectionVerification(Map.of("orders", "VERIFIED"));
        root.setAggregationPipeline(Map.of("stages", List.of("$match", "$group")));
        root.setSqlStatements(List.of("SELECT 1"));
        root.setCacheOperations(List.of(Map.of("type", "CACHEABLE")));
        root.setOracleProcRef("PROC:PKG.DO_IT");
        root.setHttpTargetMethod("GET");
        root.setHttpTargetPath("/api/x");
        root.setFragmentRef("frag_000001");
        root.setTruncatedChildCount(12);
        root.getChildren().add(node("com.x.Leaf", "leaf"));

        // Byte equality here also pins the field ORDER: childIds is stored in the slot children
        // occupied, so expansion puts children back exactly where Jackson would have written it.
        assertEquals(mapper.writeValueAsString(root), roundTrip(root));
    }

    @Test
    void sidecarRoundTripsThroughExpandSidecar() throws Exception {
        CallNode shared = node("com.x.Repo", "findAll");
        CallNode root = node("com.x.Ctrl", "handle");
        root.getChildren().add(shared);
        root.getChildren().add(shared);

        var sidecar = CallTreeFlattener.sidecar(mapper, root);
        JsonNode expanded = CallTreeInflater.expandSidecar(sidecar, mapper);
        assertEquals(mapper.readTree(mapper.writeValueAsString(root)), expanded);
    }

    @Test
    void legacySidecarPassesThroughUnchanged() throws Exception {
        // Documents already on disk are the expanded tree with no format marker. Reading one must
        // not go anywhere near the dictionary path.
        JsonNode legacy = mapper.readTree("{\"className\":\"com.x.C\",\"children\":[]}");
        assertEquals(legacy, CallTreeInflater.expandSidecar(legacy, mapper));
    }

    @Test
    void dictionaryStoresOneEntryPerDistinctNodeNotPerPath() throws Exception {
        // A diamond four levels deep: 2^4 = 16 paths to the leaf, one dictionary entry for it.
        CallNode leaf = node("com.x.Leaf", "leaf");
        CallNode level = leaf;
        for (int i = 0; i < 4; i++) {
            CallNode l = node("com.x.L" + i, "a");
            CallNode r = node("com.x.L" + i, "b");
            l.getChildren().add(level);
            r.getChildren().add(level);
            CallNode join = node("com.x.J" + i, "join");
            join.getChildren().add(l);
            join.getChildren().add(r);
            level = join;
        }

        ByteArrayOutputStream spill = new ByteArrayOutputStream();
        try (CallTreeFlattener f = new CallTreeFlattener(mapper, spill,
                () -> new ByteArrayInputStream(spill.toByteArray()))) {
            f.flatten(level);
            // 1 leaf + 4 levels x 3 nodes = 13 distinct nodes, against 2^4 = 16 leaf occurrences
            // and 31 node occurrences in the nested form.
            assertEquals(13, f.nodeCount());
        }

        // The expansion still writes the leaf once per path. Counted on "simpleClassName":"Leaf",
        // which occurs exactly once per emitted leaf node (the class name itself appears twice,
        // in `id` and `className`).
        String expanded = roundTrip(level);
        int leafOccurrences = expanded.split("\"simpleClassName\":\"Leaf\"", -1).length - 1;
        assertEquals(16, leafOccurrences, "expansion must still emit every path");
    }

    @Test
    void idsAreStableAcrossRunsAndIndependentOfInsertionOrder() throws Exception {
        CallNode one = node("com.x.Svc", "m");
        one.getChildren().add(node("com.x.Leaf", "leaf"));
        CallNode two = node("com.x.Svc", "m");
        two.getChildren().add(node("com.x.Leaf", "leaf"));

        assertEquals(idOf(one), idOf(two), "content-equal trees must get the same id");

        CallNode different = node("com.x.Svc", "m");
        different.getChildren().add(node("com.x.Other", "leaf"));
        assertTrue(!idOf(one).equals(idOf(different)), "differing children must change the id");
    }

    private String idOf(CallNode n) throws IOException {
        ByteArrayOutputStream spill = new ByteArrayOutputStream();
        try (CallTreeFlattener f = new CallTreeFlattener(mapper, spill,
                () -> new ByteArrayInputStream(spill.toByteArray()))) {
            return f.flatten(n);
        }
    }

    /**
     * Real production trees, when a previously written analysis is available. These are the
     * documents the old writer actually produced, so a byte-identical round trip over them is the
     * strongest available evidence that no response changes.
     */
    @Test
    void realAnalysisTreesRoundTripByteIdentically() throws Exception {
        java.nio.file.Path doc = java.nio.file.Path.of(
                "..", "data_old_1784577715", "jar", "web-1.0.0-SNAPSHOT", "analysis.json");
        org.junit.jupiter.api.Assumptions.assumeTrue(java.nio.file.Files.exists(doc),
                "no previously written analysis available in this checkout");

        int checked = 0;
        try (var in = java.nio.file.Files.newInputStream(doc);
             var parser = mapper.getFactory().createParser(new java.io.BufferedInputStream(in, 1 << 20))) {
            parser.nextToken();
            while (parser.nextToken() != null) {
                if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.FIELD_NAME) continue;
                if (!"endpoints".equals(parser.currentName())) {
                    parser.nextToken();
                    if (parser.currentToken() == com.fasterxml.jackson.core.JsonToken.START_ARRAY
                            || parser.currentToken() == com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                        parser.skipChildren();
                    }
                    continue;
                }
                parser.nextToken(); // START_ARRAY
                while (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_ARRAY) {
                    JsonNode ep = mapper.readTree(parser);
                    JsonNode tree = ep.get("callTree");
                    if (tree == null || tree.isNull()) continue;
                    CallNode root = mapper.treeToValue(tree, CallNode.class);
                    assertEquals(mapper.writeValueAsString(root), roundTrip(root),
                            "endpoint " + ep.path("fullPath").asText());
                    checked++;
                }
                break;
            }
        }
        assertTrue(checked > 0, "expected at least one real call tree to check");
        System.out.println("round-tripped " + checked + " real call trees byte-identically");
    }

    private CallNode node(String cls, String method) {
        CallNode n = new CallNode();
        n.setId(cls + "#" + method + "()V");
        n.setClassName(cls);
        n.setSimpleClassName(cls.substring(cls.lastIndexOf('.') + 1));
        n.setMethodName(method);
        n.setReturnType("void");
        n.setSourceJar("app.jar");
        n.setJarName("app.jar");
        return n;
    }

    @SuppressWarnings("unused")
    private static final String UTF8 = StandardCharsets.UTF_8.name();
}
