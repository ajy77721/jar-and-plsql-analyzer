package com.jaranalyzer.service;

import com.analyzer.storage.InMemoryArtifactStore;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-103 — {@code writeFlattened}'s own {@code save()}/{@code load()} round trip, not just
 * {@code CallTreeFlattener}'s isolated flatten/expand contract ({@code
 * CallTreeFlattenRoundTripTest} covers that piece already, but never exercised through the actual
 * save path this row changed). Proves the node dictionary going straight from a spill file to
 * {@code out} via {@link com.jaranalyzer.model.StreamableNodeSerializer} — instead of being merged
 * into one in-memory {@code ObjectNode} first — reproduces the identical structure on load, that a
 * shared subtree referenced from two endpoints still dedups to one dictionary entry, and that the
 * caller's own live {@code JarAnalysis} object is fully restored after {@code save()} returns (the
 * mutate-then-restore contract this row's own doc comment says was deliberately left unchanged).
 */
class WriteFlattenedStreamingTest {

    @TempDir
    Path tmp;

    private static final String JAR = "stream-demo.jar";

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

    private InMemoryArtifactStore store;
    private JarDataPaths paths;

    private PersistenceService service() {
        store = new InMemoryArtifactStore();
        paths = new JarDataPaths(tmp.resolve("jar").toString(), store);
        return new PersistenceService(paths, store);
    }

    /** Two endpoints, one shared subtree between them, plus a fragment — exercises dedup and the
     * fragment-id path in the same shape {@code writeFlattened} itself branches on. */
    private JarAnalysis analysisWithSharedSubtree() {
        CallNode shared = node("com.x.Repo", "findAll");
        CallNode a = node("com.x.SvcA", "run");
        a.getChildren().add(shared);
        CallNode b = node("com.x.SvcB", "run");
        b.getChildren().add(shared);

        EndpointInfo ep1 = new EndpointInfo();
        ep1.setHttpMethod("GET");
        ep1.setFullPath("/a");
        ep1.setCallTree(a);
        Map<String, List<CallNode>> frags = new LinkedHashMap<>();
        frags.put("overflow", List.of(node("com.x.Extra", "helper")));
        ep1.setFragments(frags);

        EndpointInfo ep2 = new EndpointInfo();
        ep2.setHttpMethod("GET");
        ep2.setFullPath("/b");
        ep2.setCallTree(b);

        JarAnalysis analysis = new JarAnalysis();
        analysis.setJarName(JAR);
        analysis.setAnalysisMode("STATIC");
        analysis.setClasses(List.of());
        analysis.setEndpoints(List.of(ep1, ep2));
        return analysis;
    }

    @Test
    void saveThenLoadReproducesTheOriginalCallTreesForEveryEndpoint() throws Exception {
        PersistenceService svc = service();
        JarAnalysis analysis = analysisWithSharedSubtree();

        svc.save(analysis);
        JarAnalysis loaded = svc.load(JAR);

        assertNotNull(loaded);
        assertEquals(2, loaded.getEndpoints().size());
        assertEquals("com.x.SvcA", loaded.getEndpoints().get(0).getCallTree().getClassName());
        assertEquals("com.x.Repo",
                loaded.getEndpoints().get(0).getCallTree().getChildren().get(0).getClassName());
        assertEquals("com.x.SvcB", loaded.getEndpoints().get(1).getCallTree().getClassName());
        assertEquals("com.x.Repo",
                loaded.getEndpoints().get(1).getCallTree().getChildren().get(0).getClassName());
        assertEquals(1, loaded.getEndpoints().get(0).getFragments().get("overflow").size());
        assertEquals("com.x.Extra",
                loaded.getEndpoints().get(0).getFragments().get("overflow").get(0).getClassName());
    }

    @Test
    void theSharedSubtreeDedupsToOneDictionaryEntryEvenWhenStreamed() throws Exception {
        PersistenceService svc = service();
        JarAnalysis analysis = analysisWithSharedSubtree();
        svc.save(analysis);

        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        byte[] bytes;
        try (var in = store.read(paths.analysisKey(JAR))) {
            bytes = in.readAllBytes();
        }
        var raw = mapper.readTree(bytes);
        var nodes = raw.get(CallTreeFlattener.NODES_FIELD);
        assertNotNull(nodes, "callTreeNodes must still be present in the written document");
        // 4 distinct nodes: SvcA, SvcB, the shared Repo.findAll (once, not twice), Extra.helper.
        assertEquals(4, nodes.size(),
                "the shared subtree must dedup to one entry — was it re-flattened per endpoint?");
    }

    /**
     * Calibrated by temporarily reverting {@code writeFlattened} to build one in-memory {@code
     * dict} again (the pre-fix shape) and confirming this test still passes — the fix must be
     * transparent to callers, not just to the specific streaming test above. Proves the
     * mutate-then-restore contract this row's own doc comment says was deliberately kept.
     */
    @Test
    void theOriginalLiveAnalysisObjectIsFullyRestoredAfterSave() throws Exception {
        PersistenceService svc = service();
        JarAnalysis analysis = analysisWithSharedSubtree();
        CallNode originalTreeA = analysis.getEndpoints().get(0).getCallTree();
        Map<String, List<CallNode>> originalFragsA = analysis.getEndpoints().get(0).getFragments();

        svc.save(analysis);

        assertSame(originalTreeA, analysis.getEndpoints().get(0).getCallTree(),
                "the caller's own live object must have its call tree restored, not left null");
        assertSame(originalFragsA, analysis.getEndpoints().get(0).getFragments());
        assertNull(analysis.getEndpoints().get(0).getCallTreeRootId(),
                "the transient rootId/fragmentChildIds fields must be cleared after save, same as before this row");
        assertNull(analysis.getCallTreeFormat());
        assertNull(analysis.getCallTreeNodes());
    }
}
