package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;

import java.nio.file.Path;

/**
 * Data paths for WAR analysis. Stores data under {data-dir}/war/{warName}/.
 * Extends JarDataPaths so it is compatible with all services that accept JarDataPaths.
 * Not a Spring @Component — instantiated directly by WarPersistenceService.
 */
public class WarDataPaths extends JarDataPaths {

    public WarDataPaths(String dataBase, com.analyzer.storage.ArtifactStore store) {
        super(dataBase + "/war", store);
    }

    /** Storage-less variant, for tests that only assert on keys and paths. */
    public WarDataPaths(String dataBase) {
        super(dataBase + "/war", null);
    }

    /**
     * Keys must carry the same {@code war} segment that {@code baseDir} adds on the Path side.
     * Without this, WAR artifacts land in the JAR namespace: the existing {@code jar/war/} tree
     * is orphaned and a WAR collides with a JAR of the same id.
     */
    @Override
    protected String[] keyRoot() {
        return new String[]{"jar", "war"};
    }

    /** WAR file is stored as stored.war (not stored.jar). */
    @Override
    public Path storedJarFile(String name) {
        return jarRoot(name).resolve("stored.war");
    }

    /** Key twin of {@link #storedJarFile} — must agree, or reads and writes address different objects. */
    @Override
    public ArtifactKey storedJarKey(String name) {
        return jarRootKey(name).resolve("stored.war");
    }
}
