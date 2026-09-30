package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.usercfg.UserConnectionStore;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.controller.detail.ObjectResolutionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * A failed re-analysis must not empty the identity it was re-running.
 *
 * <p>{@link AnalysisService#runAnalysis} moves any existing analysis of the same identity aside
 * into a timestamped archive <em>before</em> crawling, so the fresh run writes into a clean folder.
 * If that fresh run then fails — the live case being the empty-crawl guard firing on a dead
 * database — leaving the archive in place would strip the identity of its {@code api/index.json}
 * even though nothing replaced it. {@code restoreArchivedOnFailure} puts it back; this test pins
 * that behaviour against an {@link InMemoryArtifactStore}.
 *
 * <p>Driven with the collaborators mocked and {@code configPath} left unset on the directly
 * constructed service, so the underlying {@code FlowAnalysisMain.run} fails before touching a
 * database. That stands in for any crawl failure, including the empty-crawl guard.
 */
class AnalysisServiceEmptyCrawlRestoreTest {

    private static final String IDENTITY = "ERP_USP_RUN_CYCLE";

    @Test
    @DisplayName("a failed runAnalysis restores the pre-existing analysis, untouched")
    void aFailedRunLeavesTheExistingAnalysisUntouched() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();

        // A populated analysis already exists under this identity (the 51 nodes from the incident).
        ArtifactKey indexKey = AnalysisPaths.resolveKey(IDENTITY, "api/index.json");
        byte[] existing = "{\"totalNodes\":51,\"nodes\":[1,2,3]}".getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = store.write(indexKey)) {
            out.write(existing);
        }

        AnalysisService service = new AnalysisService(
                "data",
                mock(AnalysisReaderFactory.class),
                mock(StaticAnalysisReader.class),
                mock(AnalysisViewStore.class),
                mock(PlsqlConfig.class),
                mock(ClaudePersistenceService.class),
                store,
                mock(AnalysisRepository.class),
                mock(ObjectResolutionService.class),
                Path.of("config", "plsql-config.yaml"),
                mock(UserConnectionStore.class));

        assertThrows(RuntimeException.class,
                () -> service.runAnalysis(IDENTITY, "ANALYZER", "PROCEDURE"));

        assertTrue(store.exists(indexKey), "the existing analysis must be restored, not emptied");
        try (var in = store.read(indexKey)) {
            assertArrayEquals(existing, in.readAllBytes(),
                    "the pre-existing index must be byte-for-byte untouched after a failed run");
        }
    }
}
