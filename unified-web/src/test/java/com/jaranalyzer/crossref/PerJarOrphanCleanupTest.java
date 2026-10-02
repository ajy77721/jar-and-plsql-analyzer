package com.jaranalyzer.crossref;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.controller.FlowRunStore;
import com.jaranalyzer.service.JarDataPaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deleting a JAR must remove its cross-reference and flow-analysis runs. Both live under
 * {@code jar/crossref/<id>} and {@code jar/flowanalysis/<id>} — <b>outside</b> {@code jar/<id>} —
 * so the JAR-root prefix delete cannot reach them and, with no S3 reaper, they would orphan
 * forever. These tests pin the two cleanup entry points the delete path now calls.
 */
class PerJarOrphanCleanupTest {

    @TempDir
    Path root;

    private ArtifactStore store;
    private JarDataPaths paths;
    private final ObjectMapper mapper = new ObjectMapper();

    private void setUp() {
        store = new LocalArtifactStore(root);
        paths = new JarDataPaths(root.resolve("jar").toString(), store);
    }

    private void write(ArtifactKey key, String body) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void deleteAllRunsRemovesEveryCrossRefRunAndLeavesOtherJarsAlone() throws IOException {
        setUp();
        CrossRefStore crossRef = new CrossRefStore(paths, store, mapper);

        write(crossRef.runDir("app", "run-1").resolve("meta.json"), "{}");
        write(crossRef.runDir("app", "run-2").resolve("result.json"), "{}");
        write(crossRef.runDir("other", "run-9").resolve("meta.json"), "{}");

        int removed = crossRef.deleteAllRuns("app");

        assertTrue(removed >= 2, "both of app's cross-ref runs must be removed, removed=" + removed);
        assertFalse(store.exists(crossRef.runDir("app", "run-1").resolve("meta.json")));
        assertFalse(store.exists(crossRef.runDir("app", "run-2").resolve("result.json")));
        assertTrue(store.exists(crossRef.runDir("other", "run-9").resolve("meta.json")),
                "another JAR's cross-ref runs must not be touched");
    }

    @Test
    void flowRootKeyAddressesTheJarsFlowRunsAndDeletionRemovesThem() throws IOException {
        setUp();
        ArtifactKey subtree = paths.keyForPath(paths.getBaseDir());
        FlowRunStore flow = new FlowRunStore(subtree, store, mapper);

        write(flow.runDir("app", "run-1").resolve("status.json"), "{}");
        write(flow.runDir("app", "run-2").resolve("log.jsonl"), "{}\n");

        ArtifactKey root = FlowRunStore.flowRootKey(subtree, "app");
        assertEquals(subtree.resolve("flowanalysis").resolve("app"), root,
                "flowRootKey must match the key FlowRunStore itself writes under");

        assertTrue(store.deletePrefix(root) > 0);
        assertFalse(store.exists(flow.runDir("app", "run-1").resolve("status.json")));
        assertFalse(store.exists(flow.runDir("app", "run-2").resolve("log.jsonl")));
    }
}
