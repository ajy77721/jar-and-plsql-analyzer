package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.usercfg.UserConnectionStore;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.controller.detail.ObjectResolutionService;
import com.plsqlanalyzer.web.parser.model.AnalysisInfo;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The root listing manifest ({@code plsql-parse/manifest.json}) fast path. It is used only when,
 * for every analysis the repository reports, the row's recorded {@code indexMtime} equals the mtime
 * the repository reports — the repository's value comes from {@code store.lastModified} (the write
 * stamp). A mismatch falls back to the per-analysis index read, unchanged.
 *
 * <p>No {@code api/index.json} is written in the fast-path fixture, so a served row can only have
 * come from the manifest — a fallback would find nothing to read.
 */
class AnalysisServiceRootManifestTest {

    private static final String NAME = "DB~PROJ~ENV~OBJ";
    private static final ArtifactKey ROOT_MANIFEST =
            ArtifactKey.of(AnalysisPaths.PARSE_ROOT, "manifest.json");

    private static AnalysisService service(InMemoryArtifactStore store, long refMtime) throws Exception {
        AnalysisRepository repository = mock(AnalysisRepository.class);
        when(repository.analyses(any(AnalysisQuery.Dataset.class))).thenReturn(List.of(
                new AnalysisRef(NAME, AnalysisPaths.resolveKey(NAME, "api/index.json"), refMtime)));
        return new AnalysisService(
                "data",
                mock(AnalysisReaderFactory.class),
                mock(StaticAnalysisReader.class),
                mock(AnalysisViewStore.class),
                mock(PlsqlConfig.class),
                mock(ClaudePersistenceService.class),
                store,
                repository,
                mock(ObjectResolutionService.class),
                Path.of("config", "plsql-config.yaml"),
                mock(UserConnectionStore.class));
    }

    private static void writeRootManifest(InMemoryArtifactStore store, long indexMtime) throws Exception {
        String json = "{\"generatedAt\":1,\"analyses\":{\"" + NAME + "\":{"
                + "\"name\":\"" + NAME + "\",\"indexMtime\":" + indexMtime + ","
                + "\"header\":{\"entryPoint\":\"FROM_MANIFEST\",\"entrySchema\":\"APP\","
                + "\"totalNodes\":11,\"totalTables\":2,\"totalEdges\":3,\"totalLinesOfCode\":42}}}}";
        try (OutputStream out = store.write(ROOT_MANIFEST)) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void aMatchingIndexMtimeServesTheListingFromTheRootManifestAlone() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        writeRootManifest(store, 1_700_000_000_123L);

        List<AnalysisInfo> rows = service(store, 1_700_000_000_123L).listAnalyses();
        assertEquals(1, rows.size());
        AnalysisInfo row = rows.get(0);
        assertEquals("FROM_MANIFEST", row.getEntryPoint(), "the manifest row must be served");
        assertEquals(11, row.getTotalNodes());
        assertEquals(2, row.getTotalTables());
        assertEquals(3, row.getTotalEdges());
        assertEquals(1_700_000_000_123L, row.getTimestamp());
    }

    @Test
    void aMismatchedIndexMtimeFallsBackToThePerAnalysisIndex() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        writeRootManifest(store, 999L);   // the row describes a superseded run

        // The repository reports a different (stamped) mtime, so the manifest is not proven current
        // and the fallback runs. No api/index.json exists, so the row drops out rather than being
        // answered from the stale manifest.
        assertTrue(service(store, 1_700_000_000_123L).listAnalyses().isEmpty(),
                "a stale manifest row must never be served");
    }

    @Test
    void aRepositoryWithUnknownMtimeTakesTheFallback() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        writeRootManifest(store, -1L);

        assertTrue(service(store, -1L).listAnalyses().isEmpty(),
                "an unknown mtime cannot be proven against the manifest");
    }
}
