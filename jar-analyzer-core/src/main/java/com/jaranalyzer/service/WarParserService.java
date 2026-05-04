package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.ClassInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.jar.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Parses WAR bytecode using ASM.
 * WAR structure: application classes in WEB-INF/classes/, libraries in WEB-INF/lib/.
 * Mirrors JarParserService but uses WAR-specific path prefixes.
 */
@Service
public class WarParserService {

    private static final Logger log = LoggerFactory.getLogger(WarParserService.class);

    private static final String CLASS_PREFIX = "WEB-INF/classes/";
    private static final String LIB_PREFIX   = "WEB-INF/lib/";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ProgressService progressService;
    private final BytecodeClassParser bytecodeClassParser;

    public WarParserService(ProgressService progressService, BytecodeClassParser bytecodeClassParser) {
        this.progressService = progressService;
        this.bytecodeClassParser = bytecodeClassParser;
    }

    private String detectBasePackage(Set<String> packages) {
        Map<String, Integer> counts = new HashMap<>();
        for (String pkg : packages) {
            String[] parts = pkg.split("\\.");
            if (parts.length >= 2) {
                String prefix = parts[0] + "." + parts[1];
                counts.merge(prefix, 1, Integer::sum);
            }
        }
        return counts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    private boolean containsBasePackageClasses(InputStream jarStream, String basePkgPath) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(jarStream)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().endsWith(".class") && entry.getName().startsWith(basePkgPath)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Parse a WAR file. Writes each parsed class as a JSON line to a temp file.
     * Phase 1: WEB-INF/classes/ — application classes.
     * Phase 2: WEB-INF/lib/ — dependency JARs matching the detected base package.
     */
    public JarParserService.ParseResult parseWarToFile(File warFile) throws IOException {
        return parseWarToFile(warFile, null);
    }

    public JarParserService.ParseResult parseWarToFile(File warFile, String explicitBasePackage) throws IOException {
        Path tempFile = Files.createTempFile("war-classes-", ".jsonl");
        int totalClasses = 0;
        Map<String, String> jarArtifactMap = new HashMap<>();

        log.info("=== Starting WAR parse: {} ({} MB) ===",
                warFile.getName(), warFile.length() / (1024 * 1024));

        try (JarFile war = new JarFile(warFile);
             BufferedWriter writer = Files.newBufferedWriter(tempFile)) {

            // Extract main WAR's artifactId from pom.properties inside WEB-INF/classes/
            Enumeration<JarEntry> pomEntries = war.entries();
            while (pomEntries.hasMoreElements()) {
                JarEntry entry = pomEntries.nextElement();
                String name = entry.getName();
                if (name.startsWith(CLASS_PREFIX + "META-INF/maven/") && name.endsWith("/pom.properties")) {
                    try (InputStream is = war.getInputStream(entry)) {
                        Properties props = new Properties();
                        props.load(is);
                        String aid = props.getProperty("artifactId");
                        if (aid != null && !aid.isBlank()) {
                            jarArtifactMap.put(null, aid.trim());
                            log.info("Main WAR artifactId: {}", aid.trim());
                        }
                    } catch (Exception e) { /* skip */ }
                    break;
                }
            }

            // Phase 1: WEB-INF/classes/ — application classes
            log.info("Phase 1: Parsing WAR application classes (WEB-INF/classes/)...");
            int mainCount = 0;
            Set<String> mainPackages = new HashSet<>();
            Enumeration<JarEntry> entries = war.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (!name.endsWith(".class")) continue;
                if (!name.startsWith(CLASS_PREFIX)) continue;
                if (name.endsWith("module-info.class")) continue;

                try (InputStream is = war.getInputStream(entry)) {
                    ClassInfo info = bytecodeClassParser.parseClass(is.readAllBytes());
                    if (info != null) {
                        writer.write(objectMapper.writeValueAsString(info));
                        writer.newLine();
                        mainCount++;
                        if (info.getPackageName() != null && !info.getPackageName().isEmpty()) {
                            mainPackages.add(info.getPackageName());
                        }
                        if (mainCount % 200 == 0) {
                            log.info("  ... {} application classes parsed", mainCount);
                            progressService.detail("Parsed " + mainCount + " application classes...");
                            writer.flush();
                        }
                    }
                } catch (Exception e) { /* skip */ }
            }
            totalClasses = mainCount;
            log.info("Phase 1 done: {} application classes", mainCount);

            // Phase 2: WEB-INF/lib/ — dependency JARs matching base package
            String basePackage = (explicitBasePackage != null && !explicitBasePackage.isBlank())
                    ? explicitBasePackage.trim()
                    : detectBasePackage(mainPackages);
            log.info("Phase 2: {} base package '{}', scanning WEB-INF/lib/ for matching JARs...",
                    (explicitBasePackage != null && !explicitBasePackage.isBlank()) ? "Using provided" : "Detected",
                    basePackage);
            progressService.detail("Base package: " + basePackage);

            List<String> libNames = new ArrayList<>();
            entries = war.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (name.startsWith(LIB_PREFIX) && name.endsWith(".jar")) {
                    libNames.add(name);
                }
            }

            String basePkgPath = basePackage != null ? basePackage.replace('.', '/') + "/" : null;
            int parsedLibs = 0, skippedLibs = 0;

            for (String libEntry : libNames) {
                String libName = libEntry.substring(LIB_PREFIX.length());
                JarEntry entry = war.getJarEntry(libEntry);
                if (entry == null) continue;

                if (basePkgPath != null) {
                    try (InputStream peekStream = war.getInputStream(entry)) {
                        if (!containsBasePackageClasses(peekStream, basePkgPath)) {
                            skippedLibs++;
                            continue;
                        }
                    }
                }

                try (InputStream is = war.getInputStream(war.getJarEntry(libEntry))) {
                    NestedJarResult result = parseNestedJar(is, libName, writer);
                    if (result.classCount() > 0) {
                        totalClasses += result.classCount();
                        parsedLibs++;
                        if (result.artifactId() != null) {
                            jarArtifactMap.put(libName, result.artifactId());
                            log.info("  {} -> {} classes (artifactId: {})", libName, result.classCount(), result.artifactId());
                        } else {
                            log.info("  {} -> {} classes", libName, result.classCount());
                        }
                        progressService.detail("Library: " + libName + " (" + result.classCount() + " classes)");
                        writer.flush();
                    }
                } catch (Exception e) {
                    log.warn("  {} -> FAILED: {}", libName, e.getMessage());
                }
            }
            log.info("Phase 2 done: {} libs parsed, {} skipped, {} total in WEB-INF/lib/",
                    parsedLibs, skippedLibs, libNames.size());

            writer.flush();
        }

        log.info("=== WAR parse complete: {} classes -> {} ({} KB) ===",
                totalClasses, tempFile.getFileName(), Files.size(tempFile) / 1024);
        return new JarParserService.ParseResult(tempFile, totalClasses, jarArtifactMap);
    }

    private record NestedJarResult(int classCount, String artifactId) {}

    private NestedJarResult parseNestedJar(InputStream jarStream, String libName,
                                           BufferedWriter writer) throws IOException {
        int count = 0;
        String artifactId = null;
        try (ZipInputStream zis = new ZipInputStream(jarStream)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String name = entry.getName();
                if (name.startsWith("META-INF/maven/") && name.endsWith("/pom.properties")) {
                    try {
                        Properties props = new Properties();
                        props.load(new ByteArrayInputStream(zis.readAllBytes()));
                        String aid = props.getProperty("artifactId");
                        if (aid != null && !aid.isBlank()) artifactId = aid.trim();
                    } catch (Exception e) { /* skip */ }
                    continue;
                }
                if (!name.endsWith(".class")) continue;
                if (name.endsWith("module-info.class")) continue;
                try {
                    ClassInfo info = bytecodeClassParser.parseClass(zis.readAllBytes());
                    if (info != null) {
                        info.setSourceJar(libName);
                        writer.write(objectMapper.writeValueAsString(info));
                        writer.newLine();
                        count++;
                    }
                } catch (Exception e) { /* skip */ }
            }
        }
        return new NestedJarResult(count, artifactId);
    }

    /**
     * Extract Spring config files from a WAR for MongoDB URI detection.
     * Config files live inside WEB-INF/classes/ in a WAR.
     */
    public Map<String, String> extractConfigFiles(File warFile) throws IOException {
        Map<String, String> configs = new LinkedHashMap<>();
        try (JarFile war = new JarFile(warFile)) {
            for (String name : MongoCatalogService.CONFIG_FILE_NAMES) {
                JarEntry entry = war.getJarEntry(CLASS_PREFIX + name);
                if (entry != null) {
                    try (InputStream is = war.getInputStream(entry)) {
                        configs.put(name, new String(is.readAllBytes(), StandardCharsets.UTF_8));
                    }
                }
            }
        }
        if (!configs.isEmpty()) {
            log.info("Extracted {} config file(s) from WAR: {}", configs.size(), configs.keySet());
        }
        return configs;
    }

    /** Stream-read classes from JSONL file, processing each with a callback. */
    public void streamClasses(Path jsonlFile, JarParserService.ClassConsumer consumer) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(jsonlFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    ClassInfo ci = objectMapper.readValue(line, ClassInfo.class);
                    consumer.accept(ci);
                }
            }
        }
    }
}
