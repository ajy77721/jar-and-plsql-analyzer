package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WAR artifacts must be keyed under {@code jar/war/}, matching where their Paths point.
 *
 * <p>{@link WarDataPaths} sets {@code baseDir} to {@code {root}/jar/war} but inherits its key
 * accessors, so before {@code keyRoot()} was overridable every WAR artifact was keyed as
 * {@code jar/<id>/...} — the JAR namespace. That orphans the existing {@code jar/war/} tree and
 * lets a WAR and a JAR with the same id overwrite each other. Local mode hid it, because the Path
 * side was still correct; only a store-backed write exposed the divergence.
 */
class WarDataPathsKeyParityTest {

    private static final String WAR = "shop-web.war";

    private WarDataPaths paths() {
        // Mirrors production: WarPersistenceService passes app.data-dir, already {root}/jar.
        return new WarDataPaths("data/jar");
    }

    /** Renders a Path as the key string it ought to correspond to, relative to the DATA ROOT. */
    private String asKeyString(Path path) {
        Path root = Path.of("data").toAbsolutePath().normalize();
        return root.relativize(path.toAbsolutePath().normalize()).toString().replace('\\', '/');
    }

    private void assertParity(String label, Path path, ArtifactKey key) {
        assertEquals(asKeyString(path), key.toString(),
                label + ": key and path must address the same location");
    }

    @Test
    @DisplayName("WAR keys are namespaced under jar/war, not jar")
    void warKeysAreNamespaced() {
        WarDataPaths p = paths();
        assertTrue(p.jarRootKey(WAR).toString().startsWith("jar/war/"),
                "a WAR keyed under jar/ collides with a JAR of the same id");
        assertParity("jarRoot",   p.jarRoot(WAR),      p.jarRootKey(WAR));
        assertParity("analysis",  p.analysisFile(WAR), p.analysisKey(WAR));
        assertParity("header",    p.jarRoot(WAR).resolve("_header.json"), p.headerKey(WAR));
        assertParity("corrected", p.correctedFile(WAR), p.correctedKey(WAR));
    }

    @Test
    @DisplayName("the stored archive is stored.war on both sides")
    void storedArchiveAgrees() {
        WarDataPaths p = paths();
        // storedJarFile() was overridden to stored.war but storedJarKey() was not, so the writer
        // and the reader named different objects.
        assertTrue(p.storedJarKey(WAR).toString().endsWith("/stored.war"));
        assertParity("storedArchive", p.storedJarFile(WAR), p.storedJarKey(WAR));
    }

    @Test
    @DisplayName("a WAR and a JAR with the same id do not collide")
    void warAndJarDoNotCollide() {
        assertNotEquals(new JarDataPaths("data/jar").analysisKey("shop-web").toString(),
                paths().analysisKey("shop-web").toString());
    }
}
