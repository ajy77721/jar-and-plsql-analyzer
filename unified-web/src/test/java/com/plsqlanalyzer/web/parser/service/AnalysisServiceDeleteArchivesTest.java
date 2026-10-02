package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.usercfg.UserConnectionStore;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.controller.detail.ObjectResolutionService;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Deleting an analysis must also remove its timestamped re-run archives. Those archives are
 * siblings named {@code <OBJECT>_<yyyyMMdd_HHmmss>}, OUTSIDE {@code dirKey(name)}, so the
 * object-dir prefix delete cannot reach them — and the keep-3 prune only runs on a re-run, so a
 * deleted object's archives would otherwise orphan forever.
 */
class AnalysisServiceDeleteArchivesTest {

    private static final String IDENTITY = "ERP_USP_RUN_CYCLE";

    private static void write(InMemoryArtifactStore store, ArtifactKey key) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write("{}".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void deleteAnalysisRemovesArchivesAndLeavesSiblingsAlone() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ArtifactKey objectDir = AnalysisPaths.dirKey(IDENTITY);
        ArtifactKey parent = AnalysisPaths.parentDirKey(IDENTITY);
        String leaf = AnalysisPaths.leaf(IDENTITY);

        write(store, objectDir.resolve("api").resolve("index.json"));
        ArtifactKey archive1 = parent.resolve(leaf + "_20260101_120000");
        ArtifactKey archive2 = parent.resolve(leaf + "_20260102_130000");
        write(store, archive1.resolve("api").resolve("index.json"));
        write(store, archive2.resolve("api").resolve("index.json"));
        // A sibling object in the same scope, and a name that merely starts with the leaf but has
        // no timestamp — neither is an archive and neither may be touched.
        ArtifactKey sibling = parent.resolve("SOME_OTHER_OBJECT");
        write(store, sibling.resolve("api").resolve("index.json"));
        ArtifactKey lookalike = parent.resolve(leaf + "_NOT_A_TIMESTAMP");
        write(store, lookalike.resolve("api").resolve("index.json"));

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

        service.deleteAnalysis(IDENTITY);

        assertFalse(store.exists(objectDir.resolve("api").resolve("index.json")), "the object itself is gone");
        assertFalse(store.exists(archive1.resolve("api").resolve("index.json")), "archive 1 must be gone");
        assertFalse(store.exists(archive2.resolve("api").resolve("index.json")), "archive 2 must be gone");
        assertTrue(store.exists(sibling.resolve("api").resolve("index.json")), "a sibling object must survive");
        assertTrue(store.exists(lookalike.resolve("api").resolve("index.json")),
                "a non-timestamped lookalike is not an archive and must survive");
    }
}
