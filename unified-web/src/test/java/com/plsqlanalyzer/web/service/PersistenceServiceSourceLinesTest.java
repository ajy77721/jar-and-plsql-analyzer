package com.plsqlanalyzer.web.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-027 (§124) — {@code PersistenceService.readSourceLines}/{@code getSourceLines}: one entry
 * from {@code analysis-source.json} without materialising the whole {@code sourceMap}. The real
 * consumer this was built for, {@code MiscQueryService.getSource}, asks for exactly one key per
 * request; the fix's own correctness (never loading the whole map) is independent of corpus scale,
 * which is why §37's "no live corpus to measure against" blocker does not apply to it the way it
 * does to the write-path rows.
 */
class PersistenceServiceSourceLinesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static InMemoryArtifactStore storeWithSourceFile(String name, Map<String, List<String>> sourceMap)
            throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey file = AnalysisPaths.claudeFileKey(name, "analysis-source.json");
        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("sourceMap", sourceMap);
        try (OutputStream out = store.write(file)) {
            MAPPER.writeValue(out, wrapper);
        }
        return store;
    }

    @Test
    void readsExactlyTheRequestedKeyFromAMultiKeyFile() throws Exception {
        Map<String, List<String>> sourceMap = new LinkedHashMap<>();
        sourceMap.put("ERP.PKG_A", List.of("line1", "line2", "line3"));
        sourceMap.put("ERP.PKG_B", List.of("other1", "other2"));
        sourceMap.put("ERP.PKG_C", List.of("third"));
        InMemoryArtifactStore store = storeWithSourceFile("A1", sourceMap);
        PersistenceService ps = new PersistenceService(store);

        List<String> lines = ps.readSourceLines("A1", "ERP.PKG_B");
        assertEquals(List.of("other1", "other2"), lines);
    }

    @Test
    void returnsNullForAMissingKeyOrMissingFile() throws Exception {
        InMemoryArtifactStore store = storeWithSourceFile("A1", Map.of("ERP.PKG_A", List.of("x")));
        PersistenceService ps = new PersistenceService(store);

        assertNull(ps.readSourceLines("A1", "ERP.NOT_THERE"));
        assertNull(ps.readSourceLines("A1-does-not-exist", "ERP.PKG_A"));
    }

    /**
     * The actual point of this fix, made directly checkable: a {@link LazyAnalysisResult} whose
     * source section has never been touched must still be able to answer one key WITHOUT its
     * {@code isSourceLoaded()} flag ever flipping to true — proof the whole map was never
     * deserialised, not just that the right value came back (which the old `getSourceMap().get`
     * path would also produce, at the cost of full residency).
     */
    @Test
    void getSourceLinesNeverForcesTheWholeMapResidentForALazyResult() throws Exception {
        Map<String, List<String>> sourceMap = new LinkedHashMap<>();
        sourceMap.put("ERP.PKG_A", List.of("line1"));
        sourceMap.put("ERP.PKG_B", List.of("line2"));
        InMemoryArtifactStore store = storeWithSourceFile("A1", sourceMap);
        PersistenceService ps = new PersistenceService(store);
        LazyAnalysisResult lazy = new LazyAnalysisResult("A1", ps);

        assertFalse(lazy.isSourceLoaded(), "must start unloaded");
        List<String> lines = ps.getSourceLines(lazy, "ERP.PKG_B");
        assertEquals(List.of("line2"), lines);
        assertFalse(lazy.isSourceLoaded(),
                "a single-key lookup must not have materialised the whole sourceMap");
    }

    /** A plain (non-lazy) AnalysisResult — the live-crawl case — must keep working unchanged. */
    @Test
    void getSourceLinesFallsBackForAPlainAnalysisResult() {
        AnalysisResult result = new AnalysisResult();
        Map<String, List<String>> sourceMap = new LinkedHashMap<>();
        sourceMap.put("ERP.PKG_A", List.of("hello"));
        result.setSourceMap(sourceMap);

        PersistenceService ps = new PersistenceService(new InMemoryArtifactStore());
        assertEquals(List.of("hello"), ps.getSourceLines(result, "ERP.PKG_A"));
    }

    /** Once a lazy result's source section IS already loaded, no extra store read should occur. */
    @Test
    void getSourceLinesUsesTheAlreadyLoadedMapWithoutRereadingTheStore() throws Exception {
        Map<String, List<String>> sourceMap = new LinkedHashMap<>();
        sourceMap.put("ERP.PKG_A", List.of("original"));
        InMemoryArtifactStore store = storeWithSourceFile("A1", sourceMap);
        PersistenceService ps = new PersistenceService(store);
        LazyAnalysisResult lazy = new LazyAnalysisResult("A1", ps);
        // Force-load, then mutate the map directly to prove getSourceLines reads the in-memory
        // object (already loaded) rather than re-reading the store.
        lazy.getSourceMap();
        assertTrue(lazy.isSourceLoaded());
        lazy.getSourceMap().put("ERP.PKG_A", List.of("mutated-in-memory"));

        assertEquals(List.of("mutated-in-memory"), ps.getSourceLines(lazy, "ERP.PKG_A"));
    }
}
