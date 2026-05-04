package com.jaranalyzer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Component
public class JarDataPaths {

    private static final Logger log = LoggerFactory.getLogger(JarDataPaths.class);
    private final Path baseDir;

    public JarDataPaths(@Value("${app.data-dir:data}") String dataBase) {
        this.baseDir = Path.of(dataBase);
    }

    public Path getBaseDir() {
        return baseDir;
    }

    public Path jarRoot(String jarName) {
        return baseDir.resolve(JarNameUtil.normalizeKey(jarName));
    }

    public Path analysisFile(String jarName) {
        return jarRoot(jarName).resolve("analysis.json");
    }

    public Path correctedFile(String jarName) {
        return jarRoot(jarName).resolve("analysis_corrected.json");
    }

    public Path correctedPrevFile(String jarName) {
        return jarRoot(jarName).resolve("analysis_corrected_prev.json");
    }

    public Path summaryCache(String jarName) {
        return jarRoot(jarName).resolve("_summary.json");
    }

    public Path summaryCacheStatic(String jarName) {
        return jarRoot(jarName).resolve("_summary_static.json");
    }

    public Path storedJarFile(String jarName) {
        return jarRoot(jarName).resolve("stored.jar");
    }

    public Path mongoCatalogFile(String jarName) {
        return jarRoot(jarName).resolve("mongo-catalog.json");
    }

    /** Per-JAR domain config override file, stored alongside analysis data. */
    public Path domainConfigFile(String jarName) {
        return jarRoot(jarName).resolve("domain-config.json");
    }

    /** Connection info discovered during analysis (MongoDB URI, Oracle JDBC URL). */
    public Path connectionsFile(String jarName) {
        return jarRoot(jarName).resolve("connections.json");
    }

    public Path claudeDir(String jarName) {
        return jarRoot(jarName).resolve("claude");
    }

    public Path correctionsDir(String jarName) {
        return jarRoot(jarName).resolve("corrections");
    }

    public Path endpointsDir(String jarName) {
        return jarRoot(jarName).resolve("endpoints");
    }

    public Path improvementDir(String jarName) {
        return jarRoot(jarName).resolve("improvement");
    }

    public Path chatHistoryFile(String jarName) {
        return jarRoot(jarName).resolve("chat").resolve("history.jsonl");
    }

    public void ensureJarRoot(String jarName) throws IOException {
        Files.createDirectories(jarRoot(jarName));
    }

    public List<Path> listJarRoots() {
        List<Path> roots = new ArrayList<>();
        if (!Files.isDirectory(baseDir)) return roots;
        try (Stream<Path> stream = Files.list(baseDir)) {
            stream.filter(Files::isDirectory)
                    .filter(d -> Files.exists(d.resolve("analysis.json"))
                            || Files.exists(d.resolve("analysis_corrected.json")))
                    .forEach(roots::add);
        } catch (IOException e) {
            log.debug("Failed to list jar roots: {}", e.getMessage());
        }
        return roots;
    }

    public void deleteJarRoot(String jarName) throws IOException {
        Path root = jarRoot(jarName);
        if (!Files.isDirectory(root)) return;
        try (var stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        }
        log.info("Deleted jar root: {}", root);
    }
}
