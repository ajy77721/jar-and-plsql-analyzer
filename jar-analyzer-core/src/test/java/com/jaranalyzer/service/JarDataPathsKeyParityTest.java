package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every {@code ArtifactKey} accessor must address the same location as its {@code Path} twin.
 *
 * <p>During the storage migration both forms coexist, and a key that disagrees with its path is
 * the worst possible failure: local mode keeps working while S3 mode silently writes to a
 * different location, so the divergence only shows up as missing data in production. This test
 * caught exactly that — {@code chatHistoryKey} omitted the {@code chat/} segment that
 * {@code chatHistoryFile} has.
 *
 * <p>The relationship being asserted: {@code baseDir} is already {@code {root}/jar} (see
 * DirPropertyResolver), so a key must equal {@code "jar/" + <path relative to baseDir>}.
 */
class JarDataPathsKeyParityTest {

    private static final String JAR = "my-service.jar";

    private JarDataPaths paths() {
        return new JarDataPaths("data/jar");
    }

    /** Renders a Path as the key string it ought to correspond to. */
    private String asKeyString(JarDataPaths p, Path path) {
        Path rel = p.getBaseDir().toAbsolutePath().normalize()
                .relativize(path.toAbsolutePath().normalize());
        return "jar/" + rel.toString().replace('\\', '/');
    }

    private void assertParity(String label, Path path, ArtifactKey key) {
        assertEquals(asKeyString(paths(), path), key.toString(),
                label + ": key and path must address the same location");
    }

    @Test
    @DisplayName("every key accessor matches its Path twin")
    void keysMatchPaths() {
        JarDataPaths p = paths();
        assertParity("jarRoot",        p.jarRoot(JAR),                 p.jarRootKey(JAR));
        assertParity("analysis",       p.analysisFile(JAR),            p.analysisKey(JAR));
        assertParity("corrected",      p.correctedFile(JAR),           p.correctedKey(JAR));
        assertParity("correctedPrev",  p.correctedPrevFile(JAR),       p.correctedPrevKey(JAR));
        assertParity("summaryCache",   p.summaryCache(JAR),            p.summaryCacheKey(JAR));
        assertParity("summaryStatic",  p.summaryCacheStatic(JAR),      p.summaryCacheStaticKey(JAR));
        assertParity("storedJar",      p.storedJarFile(JAR),           p.storedJarKey(JAR));
        assertParity("domainConfig",   p.domainConfigFile(JAR),        p.domainConfigKey(JAR));
        assertParity("connections",    p.connectionsFile(JAR),         p.connectionsKey(JAR));
        assertParity("claudeDir",      p.claudeDir(JAR),               p.claudeDirKey(JAR));
        assertParity("correctionsDir", p.correctionsDir(JAR),          p.correctionsDirKey(JAR));
        assertParity("endpointsDir",   p.endpointsDir(JAR),            p.endpointsDirKey(JAR));
        assertParity("improvementDir", p.improvementDir(JAR),          p.improvementDirKey(JAR));
        assertParity("resourcesDir",   p.resourcesDir(JAR),            p.resourcesDirKey(JAR));
        assertParity("mongoCatalog",   p.mongoCatalogFile(JAR),        p.mongoCatalogKey(JAR));
        assertParity("externalLinks",  p.externalLinksFile(JAR),       p.externalLinksKey(JAR));
        assertParity("extLinksPending",p.externalLinksPendingFile(JAR),p.externalLinksPendingKey(JAR));
        assertParity("classTree",      p.classTreeCache(JAR),          p.classTreeCacheKey(JAR, null));
        assertParity("classTreeVer",   p.classTreeCache(JAR, "corrected"),
                                                                       p.classTreeCacheKey(JAR, "corrected"));
    }

    @Test
    @DisplayName("chat history key keeps the chat/ segment")
    void chatHistoryKeepsItsSubdirectory() {
        JarDataPaths p = paths();
        // Regression guard: the key originally dropped "chat/", which would have written history
        // to a different object than the one the reader looks for.
        assertParity("chatHistory", p.chatHistoryFile(JAR), p.chatHistoryKey(JAR));
        assertTrue(p.chatHistoryKey(JAR).toString().contains("/chat/"));
    }

    @Test
    @DisplayName("keys carry the explicit jar/ prefix that Paths get from baseDir")
    void keysArePrefixedWithJar() {
        assertTrue(paths().analysisKey(JAR).toString().startsWith("jar/"),
                "keys are relative to the DATA ROOT, so they must name the jar subtree explicitly");
    }

    @Test
    @DisplayName("blank version falls back to the default class-tree key")
    void blankVersionFallsBack() {
        JarDataPaths p = paths();
        assertEquals(p.classTreeCacheKey(JAR, null), p.classTreeCacheKey(JAR, "   "));
    }

    @Test
    @DisplayName("endpoint file key nests under endpoints/<folder>")
    void endpointFileKeyNests() {
        JarDataPaths p = paths();
        assertEquals(p.endpointsDirKey(JAR) + "/062_Foo.bar/call-tree.json",
                p.endpointFileKey(JAR, "062_Foo.bar", "call-tree.json").toString());
    }

    /**
     * Each provider subclass picks a DIFFERENT summary-cache artifact, so a copy-paste slip would
     * send a summary write and the read that follows it to different objects — invisible locally,
     * silent data loss over S3. The deprecated {@code getSummaryCachePath()} twin is gone, so the
     * assertion is now against the canonical JarDataPaths key rather than a path/key pair.
     */
    @Test
    @DisplayName("provider summary cache key matches its path twin, per provider type")
    void providerSummaryCacheParity() {
        JarDataPaths p = paths();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        Path doc = p.analysisFile(JAR);

        var stat = new StaticAnalysisProvider(p.analysisKey(JAR), mapper, p, JAR);
        assertParity("static summary", p.summaryCacheStatic(JAR), stat.getSummaryCacheKey());

        var claude = new ClaudeAnalysisProvider(p.analysisKey(JAR), mapper, p, JAR, "claude");
        assertParity("claude summary", p.summaryCache(JAR), claude.getSummaryCacheKey());

        assertNotEquals(stat.getSummaryCacheKey(), claude.getSummaryCacheKey(),
                "static and corrected summaries must not share one cache object");
        assertEquals(p.analysisKey(JAR), stat.getDocumentKey(),
                "the document key must be exactly the key the provider was built from");
        assertParity("analysis document", doc, stat.getDocumentKey());
    }

    @Test
    @DisplayName("jar name normalisation is identical on both sides")
    void normalisationAgrees() {
        JarDataPaths p = paths();
        // A name needing sanitisation must normalise the same way for key and path, or the two
        // address different objects for the same JAR.
        String messy = "My Service v1.2.jar";
        assertParity("messy name", p.analysisFile(messy), p.analysisKey(messy));
    }

    @Test
    @DisplayName("keyForPath maps the base directory itself to the subtree root")
    void keyForPathHandlesTheBaseDirectory() {
        JarDataPaths p = paths();
        // Regression guard: base.relativize(base) is Path.of(""), whose getNameCount() is 1 with a
        // single EMPTY name. Iterating it fed ArtifactKey a blank segment, which it rejects, and the
        // catch-all turned that into null. CrossRefStore dereferences this at construction, so the
        // whole application failed to start.
        assertEquals("jar", p.keyForPath(p.getBaseDir()).toString());
        assertNotNull(p.keyForPath(p.getBaseDir().toAbsolutePath()));
    }

    @Test
    @DisplayName("keyForPath returns null only for paths outside the subtree")
    void keyForPathRejectsOutsidePaths() {
        JarDataPaths p = paths();
        assertNull(p.keyForPath(Path.of("/etc/passwd")));
        assertEquals("jar/x/y.json", p.keyForPath(p.getBaseDir().resolve("x/y.json")).toString());
    }
}
