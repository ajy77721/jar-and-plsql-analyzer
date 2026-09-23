package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalArtifactStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PromptTemplates must read the prompts that PromptConfigService writes — through the store, at the
 * same key. Previously it read {@code <dataDir>/prompts/<name>.txt} off local disk while the writer
 * had already moved to the store; the two agree in local mode and silently diverge on S3.
 */
class PromptTemplatesStoreTest {

    @AfterEach
    void unwire() {
        PromptTemplates.setTemplateStore(null);
    }

    /** The one address both sides use. Hard-coded here on purpose: if it moves, this must too. */
    @Test
    void templateKeyIsPromptsSlashNameDotTxt() {
        assertEquals(ArtifactKey.of("prompts", "java-mongo-analysis.txt"),
                PromptTemplates.templateKey("java-mongo-analysis"));
    }

    @Test
    void loadTemplateReadsTheStoredTemplate(@TempDir Path root) throws Exception {
        ArtifactStore store = new LocalArtifactStore(root);
        write(store, PromptTemplates.templateKey("java-mongo-analysis"), "STORED TEXT");

        PromptTemplates.setTemplateStore(store);
        assertEquals("STORED TEXT", PromptTemplates.loadTemplate("java-mongo-analysis"));
    }

    @Test
    void aSavedTemplateIsPickedUpAfterTheStoreIsRepointed(@TempDir Path root) throws Exception {
        ArtifactStore store = new LocalArtifactStore(root);
        ArtifactKey key = PromptTemplates.templateKey("java-oracle-analysis");
        write(store, key, "V1");

        PromptTemplates.setTemplateStore(store);
        assertEquals("V1", PromptTemplates.loadTemplate("java-oracle-analysis"));

        // What PromptConfigService.saveTemplate does: write, then repoint (which clears the cache).
        write(store, key, "V2");
        PromptTemplates.setTemplateStore(store);
        assertEquals("V2", PromptTemplates.loadTemplate("java-oracle-analysis"),
                "cache was not invalidated, so an edited prompt would never take effect");
    }

    @Test
    void fallsBackToTheClasspathWhenTheStoreHasNoOverride(@TempDir Path root) {
        PromptTemplates.setTemplateStore(new LocalArtifactStore(root));
        // Nothing stored and nothing on this module's classpath: the classpath tier is reached and
        // returns the documented empty string rather than throwing or leaking a filesystem error.
        // (The real defaults ship in unified-web's resources/prompts/.)
        assertEquals("", PromptTemplates.loadTemplate("java-mongo-correction"));
    }

    private static void write(ArtifactStore store, ArtifactKey key, String text) throws Exception {
        try (OutputStream out = store.write(key)) {
            out.write(text.getBytes(StandardCharsets.UTF_8));
        }
    }
}
