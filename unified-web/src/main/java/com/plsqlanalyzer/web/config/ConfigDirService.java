package com.plsqlanalyzer.web.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.JarNameUtil;
import com.jaranalyzer.service.PromptTemplates;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Stream;

/**
 * Central directory manager for config and data.
 *
 * Both directories are fully dynamic — pass any absolute or relative path:
 *   java -jar app.jar --app.config-dir=/etc/analyzer/config --app.data-dir=/var/data/analyzer
 *
 * Layout:
 *   config-dir/
 *     plsql-config.yaml
 *     domain-config.json
 *     prompts/*.txt
 *   data-dir/
 *     jar/              (JAR analyzer data)
 *     plsql/            (PL/SQL analyzer data)
 *     claude-chatbot/   (Classic chat sessions)
 *     unified-analyzer.log
 */
@Component
public class ConfigDirService {

    private static final Logger log = LoggerFactory.getLogger(ConfigDirService.class);

    @Value("${app.config-dir:config}")
    private String configDirProp;

    @Value("${app.data-dir.base:data}")
    private String dataDirProp;

    private Path configDir;
    private Path dataDir;

    private static final List<String> SEED_FILES = List.of(
            "plsql-config.yaml",
            "domain-config.json"
    );

    private static final List<String> SEED_PROMPTS = List.of(
            "plsql-verification.txt",
            "java-mongo-analysis.txt",
            "java-mongo-chunk-analysis.txt",
            "java-mongo-correction.txt",
            "java-both-analysis.txt",
            "java-both-chunk-analysis.txt",
            "java-both-correction.txt",
            "java-oracle-analysis.txt",
            "java-oracle-chunk-analysis.txt",
            "java-oracle-correction.txt"
    );

    @PostConstruct
    void init() {
        // Paths already resolved to absolute by DirPropertyResolver
        configDir = Path.of(configDirProp).toAbsolutePath().normalize();
        ensureDir(configDir, "Config");

        dataDir = Path.of(dataDirProp).toAbsolutePath().normalize();
        ensureDir(dataDir, "Data");
        ensureDir(dataDir.resolve("jar"), "Data/jar");
        ensureDir(dataDir.resolve("plsql"), "Data/plsql");
        ensureDir(dataDir.resolve("claude-chatbot"), "Data/claude-chatbot");

        log.info("Config directory: {}", configDir.toAbsolutePath());
        log.info("Data directory:   {}", dataDir.toAbsolutePath());

        // Migrate existing config files from legacy locations
        migrateLegacyFile("plsql-config.yaml");

        // Seed config files from classpath if missing
        for (String file : SEED_FILES) {
            seedFromClasspath(file, configDir.resolve(file));
        }

        // Seed prompt templates
        Path promptsDir = configDir.resolve("prompts");
        ensureDir(promptsDir, "Prompts");
        for (String prompt : SEED_PROMPTS) {
            seedFromClasspath("prompts/" + prompt, promptsDir.resolve(prompt));
        }

        // Wire PromptTemplates to check config dir first
        PromptTemplates.setExternalConfigDir(configDir);

        // Migrate existing flat layout to per-analysis layout
        migrateToPerAnalysisLayout();
    }

    // ── Getters ──

    public Path getConfigDir() { return configDir; }

    public Path getDataDir() { return dataDir; }

    public Path getJarDataDir() { return dataDir.resolve("jar"); }

    public Path getPlsqlDataDir() { return dataDir.resolve("plsql"); }

    public Path getChatbotDataDir() { return dataDir.resolve("claude-chatbot"); }

    public Path resolve(String filename) {
        return configDir.resolve(filename);
    }

    public boolean exists(String filename) {
        return Files.exists(configDir.resolve(filename));
    }

    // ── Data layout migration ──

    /**
     * Migrate existing data from the old flat layout to the new per-analysis layout.
     * Safe to run multiple times (idempotent).
     */
    private void migrateToPerAnalysisLayout() {
        migrateJarData();
        migratePlsqlData();
    }

    /**
     * JAR migration: move from flat directories under data/jar/ to per-JAR folders.
     * Old layout: data/jar/analysis/{name}.json, data/jar/jars/{name}.jar, etc.
     * New layout: data/jar/{normalizedKey}/analysis.json, stored.jar, etc.
     */
    private void migrateJarData() {
        Path jarDir = dataDir.resolve("jar");
        Path oldAnalysisDir = jarDir.resolve("analysis");
        if (!Files.isDirectory(oldAnalysisDir)) {
            log.debug("No old JAR analysis directory found — skipping JAR migration");
            return;
        }

        log.info("Migrating JAR data from flat layout to per-analysis layout...");
        ObjectMapper om = new ObjectMapper();

        try (Stream<Path> files = Files.list(oldAnalysisDir)) {
            files.filter(f -> f.toString().endsWith(".json"))
                 .filter(f -> {
                     String fname = f.getFileName().toString();
                     // Skip _corrected and _corrected_prev variants — they are handled with their base
                     return !fname.endsWith("_corrected.json") && !fname.endsWith("_corrected_prev.json");
                 })
                 .forEach(analysisFile -> {
                     try {
                         migrateOneJar(jarDir, analysisFile, om);
                     } catch (Exception e) {
                         log.error("Failed to migrate JAR analysis {}: {}", analysisFile.getFileName(), e.getMessage());
                     }
                 });
        } catch (IOException e) {
            log.error("Failed to list old JAR analysis directory: {}", e.getMessage());
            return;
        }

        // Clean up empty old directories
        for (String oldDir : List.of("analysis", "jars", "claude", "corrections", "endpoints", "chat", "mongo-catalog")) {
            Path dir = jarDir.resolve(oldDir);
            deleteDirectoryIfEmpty(dir);
        }
    }

    private void migrateOneJar(Path jarDir, Path analysisFile, ObjectMapper om) {
        String fileName = analysisFile.getFileName().toString(); // e.g. "core-accounts-0.0.1-SNAPSHOT.jar.json"
        String safeName = fileName.substring(0, fileName.length() - 5); // strip ".json" → "core-accounts-0.0.1-SNAPSHOT.jar"

        // Read jarName from JSON header using streaming parser (files are 100-400MB)
        String jarName = null;
        try (JsonParser parser = om.getFactory().createParser(analysisFile.toFile())) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                log.warn("Unexpected JSON structure in {}", analysisFile);
                return;
            }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                parser.nextToken();
                if ("jarName".equals(field)) {
                    jarName = parser.getText();
                    break;
                }
                parser.skipChildren();
            }
        } catch (IOException e) {
            log.error("Failed to read jarName from {}: {}", analysisFile, e.getMessage());
            return;
        }

        if (jarName == null || jarName.isBlank()) {
            log.warn("No jarName found in {} — using safeName as fallback", analysisFile);
            jarName = safeName;
        }

        String normalizedKey = JarNameUtil.normalizeKey(jarName);
        Path targetDir = jarDir.resolve(normalizedKey);
        try {
            Files.createDirectories(targetDir);
        } catch (IOException e) {
            log.error("Failed to create target directory {}: {}", targetDir, e.getMessage());
            return;
        }

        // Move analysis files
        moveFile(analysisFile, targetDir.resolve("analysis.json"));
        moveFile(jarDir.resolve("analysis").resolve(safeName + "_corrected.json"),
                 targetDir.resolve("analysis_corrected.json"));
        moveFile(jarDir.resolve("analysis").resolve(safeName + "_corrected_prev.json"),
                 targetDir.resolve("analysis_corrected_prev.json"));

        // Move stored JAR — safeName may or may not end with ".jar"
        // Try both forms: jars/{safeName}.jar and jars/{safeName}
        moveFile(jarDir.resolve("jars").resolve(safeName + ".jar"),
                 targetDir.resolve("stored.jar"));
        moveFile(jarDir.resolve("jars").resolve(safeName),
                 targetDir.resolve("stored.jar"));

        // Move mongo-catalog
        moveFile(jarDir.resolve("mongo-catalog").resolve(safeName + ".json"),
                 targetDir.resolve("mongo-catalog.json"));

        // Move directories (claude, corrections use normalizedKey; endpoints uses safeName)
        moveDirectory(jarDir.resolve("claude").resolve(normalizedKey),
                      targetDir.resolve("claude"));
        moveDirectory(jarDir.resolve("corrections").resolve(normalizedKey),
                      targetDir.resolve("corrections"));
        moveDirectory(jarDir.resolve("endpoints").resolve(safeName),
                      targetDir.resolve("endpoints"));
        moveDirectory(jarDir.resolve("chat").resolve(normalizedKey),
                      targetDir.resolve("chat"));

        log.info("Migrated JAR '{}' (key={}) to {}", jarName, normalizedKey, targetDir);
    }

    /**
     * PL/SQL migration: move claude data from data/plsql/claude/{name}/ to data/plsql/{name}/claude/.
     */
    private void migratePlsqlData() {
        Path plsqlDir = dataDir.resolve("plsql");
        Path oldClaudeDir = plsqlDir.resolve("claude");
        if (!Files.isDirectory(oldClaudeDir)) {
            log.debug("No old PL/SQL claude directory found — skipping PL/SQL migration");
            return;
        }

        log.info("Migrating PL/SQL data from flat layout to per-analysis layout...");

        try (Stream<Path> dirs = Files.list(oldClaudeDir)) {
            dirs.filter(Files::isDirectory)
                .forEach(analysisDir -> {
                    String analysisName = analysisDir.getFileName().toString();
                    Path targetClaudeDir = plsqlDir.resolve(analysisName).resolve("claude");
                    try {
                        Files.createDirectories(targetClaudeDir);
                        // Move contents of the old claude/{analysisName}/ to {analysisName}/claude/
                        try (Stream<Path> contents = Files.list(analysisDir)) {
                            contents.forEach(src -> {
                                Path dest = targetClaudeDir.resolve(src.getFileName());
                                moveFileOrDir(src, dest);
                            });
                        }
                        // Remove the now-empty source directory
                        deleteDirectoryIfEmpty(analysisDir);
                        log.info("Migrated PL/SQL claude data for '{}'", analysisName);
                    } catch (IOException e) {
                        log.error("Failed to migrate PL/SQL claude data for '{}': {}", analysisName, e.getMessage());
                    }
                });
        } catch (IOException e) {
            log.error("Failed to list old PL/SQL claude directory: {}", e.getMessage());
            return;
        }

        deleteDirectoryIfEmpty(oldClaudeDir);
    }

    /**
     * Move a single file if the source exists and destination does not.
     */
    private void moveFile(Path src, Path dest) {
        if (!Files.exists(src) || Files.isDirectory(src)) return;
        if (Files.exists(dest)) {
            log.debug("Skipping move — destination already exists: {}", dest);
            return;
        }
        try {
            Files.move(src, dest, StandardCopyOption.ATOMIC_MOVE);
            log.info("Moved {} -> {}", src, dest);
        } catch (IOException e) {
            // ATOMIC_MOVE may fail across filesystems — fall back to regular move
            try {
                Files.move(src, dest);
                log.info("Moved {} -> {}", src, dest);
            } catch (IOException e2) {
                log.error("Failed to move {} -> {}: {}", src, dest, e2.getMessage());
            }
        }
    }

    /**
     * Move an entire directory tree if the source exists and destination does not.
     */
    private void moveDirectory(Path src, Path dest) {
        if (!Files.isDirectory(src)) return;
        if (Files.isDirectory(dest)) {
            // Destination exists — merge contents
            try (Stream<Path> contents = Files.list(src)) {
                contents.forEach(child -> moveFileOrDir(child, dest.resolve(child.getFileName())));
            } catch (IOException e) {
                log.error("Failed to merge directory {} -> {}: {}", src, dest, e.getMessage());
                return;
            }
            deleteDirectoryIfEmpty(src);
            return;
        }
        try {
            Files.move(src, dest, StandardCopyOption.ATOMIC_MOVE);
            log.info("Moved directory {} -> {}", src, dest);
        } catch (IOException e) {
            // ATOMIC_MOVE may fail — fall back to walk and move
            try {
                Files.createDirectories(dest);
                try (Stream<Path> contents = Files.list(src)) {
                    contents.forEach(child -> moveFileOrDir(child, dest.resolve(child.getFileName())));
                }
                deleteDirectoryIfEmpty(src);
                log.info("Moved directory {} -> {}", src, dest);
            } catch (IOException e2) {
                log.error("Failed to move directory {} -> {}: {}", src, dest, e2.getMessage());
            }
        }
    }

    /**
     * Move a file or directory (dispatches to moveFile or moveDirectory).
     */
    private void moveFileOrDir(Path src, Path dest) {
        if (Files.isDirectory(src)) {
            moveDirectory(src, dest);
        } else {
            moveFile(src, dest);
        }
    }

    /**
     * Delete a directory only if it exists and is empty.
     */
    private void deleteDirectoryIfEmpty(Path dir) {
        if (!Files.isDirectory(dir)) return;
        try (Stream<Path> entries = Files.list(dir)) {
            if (entries.findFirst().isEmpty()) {
                Files.delete(dir);
                log.info("Removed empty directory: {}", dir);
            }
        } catch (IOException e) {
            log.debug("Could not remove directory {}: {}", dir, e.getMessage());
        }
    }

    // ── Internal helpers ──

    private void ensureDir(Path dir, String label) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            log.error("Failed to create {} directory {}: {}", label, dir, e.getMessage());
        }
    }

    private Path resolveDir(String dirProp) {
        Path p = Path.of(dirProp).toAbsolutePath().normalize();
        if (Files.isDirectory(p)) return p;
        // Try parent directory (handles running from unified-web/ subdirectory)
        Path parent = p.getParent();
        if (parent != null) {
            Path up = parent.getParent();
            if (up != null) {
                Path upDir = up.resolve(dirProp);
                if (Files.isDirectory(upDir)) return upDir;
            }
        }
        return p;
    }

    private void migrateLegacyFile(String filename) {
        Path target = configDir.resolve(filename);
        if (Files.exists(target)) return;
        Path cwd = Path.of("").toAbsolutePath();
        for (Path dir : List.of(cwd, cwd.getParent(), cwd.getParent() != null ? cwd.getParent().getParent() : cwd)) {
            if (dir == null) continue;
            Path legacy = dir.resolve(filename);
            if (Files.exists(legacy) && !legacy.toAbsolutePath().equals(target.toAbsolutePath())) {
                try {
                    Files.copy(legacy, target, StandardCopyOption.REPLACE_EXISTING);
                    log.info("Migrated legacy config {} -> {}", legacy, target);
                    return;
                } catch (IOException e) {
                    log.warn("Failed to migrate {}: {}", legacy, e.getMessage());
                }
            }
        }
    }

    private void seedFromClasspath(String classpathResource, Path target) {
        if (Files.exists(target)) return;
        String[] candidates = { classpathResource, "static/jar/" + classpathResource };
        for (String candidate : candidates) {
            try {
                ClassPathResource cpr = new ClassPathResource(candidate);
                if (cpr.exists()) {
                    try (InputStream is = cpr.getInputStream()) {
                        Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);
                        log.info("Seeded config: {} -> {}", candidate, target);
                        return;
                    }
                }
            } catch (IOException e) {
                // try next
            }
        }
    }
}
