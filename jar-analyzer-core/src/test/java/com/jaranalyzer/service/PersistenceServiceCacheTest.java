package com.jaranalyzer.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PersistenceService — cache invalidation")
class PersistenceServiceCacheTest {

    @TempDir
    Path tmp;

    JarDataPaths paths;
    PersistenceService service;

    @BeforeEach
    void setUp() throws Exception {
        // Keys carry an explicit "jar/" segment (see JarDataPathsKeyParityTest), so the data dir
        // must end in /jar and the store must be rooted at its parent for key and path to address
        // the same file — exactly the production arrangement DirPropertyResolver produces.
        var store = new com.analyzer.storage.LocalArtifactStore(tmp);
        paths = new JarDataPaths(tmp.resolve("jar").toString(), store);
        service = new PersistenceService(paths, store);
        // ensureJarRoot is a no-op now that the store creates structure on write, but this test
        // seeds cache files with Files.writeString, so the directory has to exist first.
        Files.createDirectories(paths.jarRoot("test.jar"));
    }

    @Test
    @DisplayName("invalidateSummaryCache deletes the default class-tree cache")
    void invalidateDeletesDefaultClassTree() throws Exception {
        Path cache = paths.classTreeCache("test.jar");
        Files.writeString(cache, "{}", StandardCharsets.UTF_8);
        assertTrue(Files.exists(cache));

        service.invalidateSummaryCache("test.jar");

        assertFalse(Files.exists(cache), "default _class-tree.json must be deleted");
    }

    @Test
    @DisplayName("invalidateSummaryCache deletes version-specific class-tree caches")
    void invalidateDeletesVersionSpecificClassTree() throws Exception {
        Path defaultCache = paths.classTreeCache("test.jar", null);
        Path correctedCache = paths.classTreeCache("test.jar", "corrected");
        Files.writeString(defaultCache, "{}", StandardCharsets.UTF_8);
        Files.writeString(correctedCache, "{\"corrected\":true}", StandardCharsets.UTF_8);

        service.invalidateSummaryCache("test.jar");

        assertFalse(Files.exists(defaultCache), "default cache must be deleted");
        assertFalse(Files.exists(correctedCache), "corrected cache must be deleted");
    }

    @Test
    @DisplayName("invalidateSummaryCache deletes summary caches")
    void invalidateDeletesSummaryCaches() throws Exception {
        Files.writeString(paths.summaryCache("test.jar"), "[]", StandardCharsets.UTF_8);
        Files.writeString(paths.summaryCacheStatic("test.jar"), "[]", StandardCharsets.UTF_8);

        service.invalidateSummaryCache("test.jar");

        assertFalse(Files.exists(paths.summaryCache("test.jar")), "_summary.json must be deleted");
        assertFalse(Files.exists(paths.summaryCacheStatic("test.jar")), "_summary_static.json must be deleted");
    }

    @Test
    @DisplayName("invalidateSummaryCache is a no-op for a non-existent jar root")
    void invalidateNoopWhenJarRootAbsent() {
        // Should not throw even when the jar root doesn't exist
        assertDoesNotThrow(() -> service.invalidateSummaryCache("nonexistent.jar"));
    }

    @Test
    @DisplayName("invalidateSummaryCache also handles .tmp leftover")
    void invalidateDeletesTmpFile() throws Exception {
        Path tmp2 = paths.classTreeCache("test.jar").resolveSibling("_class-tree.json.tmp");
        Files.writeString(tmp2, "partial", StandardCharsets.UTF_8);

        service.invalidateSummaryCache("test.jar");

        assertFalse(Files.exists(tmp2), ".tmp file must be deleted");
    }

    @Test
    @DisplayName("invalidateSummaryCache drops the precomputed view summaries")
    void invalidateDeletesViewSummaries() throws Exception {
        // A re-analysis or a correction rewrites the document these summarise, so leaving them
        // behind would serve a pre-change header/collection report.
        Path views = paths.jarRootKey("test.jar").resolve("views").toPath(tmp.resolve("jar").getParent());
        Files.createDirectories(views);
        Files.writeString(views.resolve("summary-header.json"), "{}", StandardCharsets.UTF_8);
        Files.writeString(views.resolve("collections-count.json"), "[]", StandardCharsets.UTF_8);

        service.invalidateSummaryCache("test.jar");

        assertFalse(Files.exists(views.resolve("summary-header.json")),
                "a stale summary header must not survive a rewrite");
        assertFalse(Files.exists(views.resolve("collections-count.json")),
                "a stale collection report must not survive a rewrite");
    }
}
