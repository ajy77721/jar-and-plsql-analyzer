package com.analyzer.queue;

import com.analyzer.repo.EntityIngestTrigger;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.jaranalyzer.service.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * The JAR-side twin of the PL/SQL empty-crawl guard: a JAR that parses to zero classes must fail
 * loud before anything is written, rather than persisting an empty analysis over good data for the
 * same jar name.
 */
class JarAnalysisExecutorEmptyJarGuardTest {

    @Test
    @DisplayName("a 0-class JAR fails loud before any artifact write")
    void zeroClassJarFailsLoudBeforeWritingAnything() throws Exception {
        JarParserService parserService = mock(JarParserService.class);
        Path classesFile = Files.createTempFile("jar-classes-", ".jsonl");
        when(parserService.parseJarToFile(any(), any(), any()))
                .thenReturn(new JarParserService.ParseResult(classesFile, 0, new HashMap<>()));

        PersistenceService persistenceService = mock(PersistenceService.class);
        JarDataPaths paths = mock(JarDataPaths.class);
        when(paths.domainConfigKey(any())).thenReturn(ArtifactKey.of("jars", "empty.jar", "domain-config.json"));
        when(persistenceService.getPaths()).thenReturn(paths);

        InMemoryArtifactStore store = new InMemoryArtifactStore();

        JarAnalysisExecutor exec = new JarAnalysisExecutor(
                parserService,
                mock(CallGraphService.class),
                persistenceService,
                mock(ProgressService.class),
                mock(ClaudeAnalysisService.class),
                mock(SourceEnrichmentService.class),
                mock(MongoCatalogService.class),
                mock(DomainConfigLoader.class),
                mock(com.jaranalyzer.extapi.ExternalApiLinkService.class),
                mock(RepositoryStereotypeResolver.class),
                mock(EntityIngestTrigger.class),
                store);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", "empty.jar");
        meta.put("tempFilePath", Files.createTempFile("empty-", ".jar").toString());
        meta.put("fileSize", 100L);
        QueueJob job = new QueueJob(QueueJob.Type.JAR_UPLOAD, "Analyze: empty.jar", meta);

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> exec.execute(job, (j, event) -> {}));

        assertTrue(ex.getMessage().startsWith("Crawled 0 classes"), ex.getMessage());
        assertEquals(0, store.objectCount(), "nothing may be written for a 0-class JAR");
    }
}
