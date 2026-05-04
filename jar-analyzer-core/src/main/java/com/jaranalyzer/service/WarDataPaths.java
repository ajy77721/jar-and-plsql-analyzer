package com.jaranalyzer.service;

import java.nio.file.Path;

/**
 * Data paths for WAR analysis. Stores data under {data-dir}/war/{warName}/.
 * Extends JarDataPaths so it is compatible with all services that accept JarDataPaths.
 * Not a Spring @Component — instantiated directly by WarPersistenceService.
 */
public class WarDataPaths extends JarDataPaths {

    public WarDataPaths(String dataBase) {
        super(dataBase + "/war");
    }

    /** WAR file is stored as stored.war (not stored.jar). */
    @Override
    public Path storedJarFile(String name) {
        return jarRoot(name).resolve("stored.war");
    }
}
