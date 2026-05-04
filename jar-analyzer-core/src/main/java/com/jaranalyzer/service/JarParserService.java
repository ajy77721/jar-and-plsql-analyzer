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
 * Parses JAR bytecode using ASM.
 * Writes each parsed class as a JSON line to a temp file so memory stays bounded.
 */
@Service
public class JarParserService {

    private static final Logger log = LoggerFactory.getLogger(JarParserService.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ProgressService progressService;
    private final BytecodeClassParser bytecodeClassParser;

    public JarParserService(ProgressService progressService, BytecodeClassParser bytecodeClassParser) {
        this.progressService = progressService;
        this.bytecodeClassParser = bytecodeClassParser;
    }

    /**
     * Detect the base package from the main application classes.
     * Finds the most common 2-segment package prefix (e.g., "com.allianhealth").
     * Nested JARs are only parsed if they contain classes matching this prefix.
     */
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

    /**
     * Peek inside a nested JAR to check if any class belongs to the base package.
     * Only reads ZIP entry headers — does not parse bytecode.
     */
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
     * Parse a JAR file. Writes each class as a JSON line to a temp file.
     * Returns: path to JSONL temp file + total count.
     */
    public ParseResult parseJarToFile(File jarFile) throws IOException {
        return parseJarToFile(jarFile, null);
    }

    private static final byte[] NEWLINE = new byte[]{'\n'};

    public ParseResult parseJarToFile(File jarFile, String explicitBasePackage) throws IOException {
        Path tempFile = Files.createTempFile("jar-classes-", ".jsonl");
        int totalClasses = 0;
        Map<String, String> jarArtifactMap = new HashMap<>();

        log.info("=== Starting JAR parse: {} ({} MB) ===",
                jarFile.getName(), jarFile.length() / (1024 * 1024));

        // Use OutputStream + writeValueAsBytes to avoid charset encoder failures on
        // malformed surrogate characters that can appear in class string constants.
        try (JarFile jar = new JarFile(jarFile);
             BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(tempFile), 65536)) {

            boolean isSpringBoot = jar.getEntry("BOOT-INF/classes/") != null;
            String classPrefix = isSpringBoot ? "BOOT-INF/classes/" : "";
            log.info("JAR type: {}", isSpringBoot ? "Spring Boot fat JAR" : "Standard JAR");

            // Extract main JAR's own artifactId from pom.properties
            Enumeration<JarEntry> pomEntries = jar.entries();
            while (pomEntries.hasMoreElements()) {
                JarEntry entry = pomEntries.nextElement();
                String name = entry.getName();
                String pomPath = isSpringBoot ? "BOOT-INF/classes/META-INF/maven/" : "META-INF/maven/";
                if (name.startsWith(pomPath) && name.endsWith("/pom.properties")) {
                    try (InputStream is = jar.getInputStream(entry)) {
                        Properties props = new Properties();
                        props.load(is);
                        String aid = props.getProperty("artifactId");
                        if (aid != null && !aid.isBlank()) {
                            jarArtifactMap.put(null, aid.trim());
                            log.info("Main JAR artifactId: {}", aid.trim());
                        }
                    } catch (Exception e) { /* skip */ }
                    break;
                }
            }

            // Phase 1: application classes — also collect packages for base-package detection
            log.info("Phase 1: Parsing application classes...");
            int mainCount = 0;
            Set<String> mainPackages = new HashSet<>();
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (!name.endsWith(".class")) continue;
                if (!name.startsWith(classPrefix)) continue;
                if (name.startsWith("org/springframework/boot/loader/")) continue;
                if (name.endsWith("module-info.class")) continue;

                try (InputStream is = jar.getInputStream(entry)) {
                    ClassInfo info = bytecodeClassParser.parseClass(is.readAllBytes());
                    if (info != null) {
                        out.write(objectMapper.writeValueAsBytes(info));
                        out.write(NEWLINE);
                        mainCount++;
                        if (info.getPackageName() != null && !info.getPackageName().isEmpty()) {
                            mainPackages.add(info.getPackageName());
                        }
                        if (mainCount % 200 == 0) {
                            log.info("  ... {} application classes parsed", mainCount);
                            progressService.detail("Parsed " + mainCount + " application classes...");
                            out.flush();
                        }
                    }
                } catch (Exception e) { /* skip */ }
            }
            totalClasses = mainCount;
            log.info("Phase 1 done: {} application classes", mainCount);

            // Phase 2: nested library JARs — only scan those matching the base package
            if (isSpringBoot) {
                String basePackage = (explicitBasePackage != null && !explicitBasePackage.isBlank())
                        ? explicitBasePackage.trim()
                        : detectBasePackage(mainPackages);
                log.info("Phase 2: {} base package '{}', scanning BOOT-INF/lib/ for matching JARs...",
                        (explicitBasePackage != null && !explicitBasePackage.isBlank()) ? "Using provided" : "Detected",
                        basePackage);
                progressService.detail("Base package: " + basePackage);

                List<String> libNames = new ArrayList<>();
                entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    String name = entry.getName();
                    if (name.startsWith("BOOT-INF/lib/") && name.endsWith(".jar")) {
                        libNames.add(name);
                    }
                }

                String basePkgPath = basePackage != null ? basePackage.replace('.', '/') + "/" : null;
                int parsedLibs = 0, skippedLibs = 0;

                for (String libEntry : libNames) {
                    String libName = libEntry.substring("BOOT-INF/lib/".length());
                    JarEntry entry = jar.getJarEntry(libEntry);
                    if (entry == null) continue;

                    // Peek: check if any class in this JAR matches the base package
                    if (basePkgPath != null) {
                        try (InputStream peekStream = jar.getInputStream(entry)) {
                            if (!containsBasePackageClasses(peekStream, basePkgPath)) {
                                skippedLibs++;
                                continue;
                            }
                        }
                    }

                    // Parse: re-open and parse matching JAR
                    try (InputStream is = jar.getInputStream(jar.getJarEntry(libEntry))) {
                        NestedJarResult result = parseNestedJar(is, libName, out);
                        if (result.classCount > 0) {
                            totalClasses += result.classCount;
                            parsedLibs++;
                            if (result.artifactId != null) {
                                jarArtifactMap.put(libName, result.artifactId);
                                log.info("  {} -> {} classes (artifactId: {})", libName, result.classCount, result.artifactId);
                            } else {
                                log.info("  {} -> {} classes", libName, result.classCount);
                            }
                            progressService.detail("Library: " + libName + " (" + result.classCount + " classes)");
                            out.flush();
                        }
                    } catch (Exception e) {
                        log.warn("  {} -> FAILED: {}", libName, e.getMessage());
                    }
                }
                log.info("Phase 2 done: {} libs parsed, {} skipped (no base-package classes), {} total nested, {} with POM artifactId",
                        parsedLibs, skippedLibs, libNames.size(), jarArtifactMap.size());
            }

            out.flush();
        }

        log.info("=== Parse complete: {} classes -> {} ({} KB) ===",
                totalClasses, tempFile.getFileName(), Files.size(tempFile) / 1024);
        return new ParseResult(tempFile, totalClasses, jarArtifactMap);
    }

    /** Result of chunked parse */
    public record ParseResult(Path classesFile, int totalClasses, Map<String, String> jarArtifactMap) {}

    private record NestedJarResult(int classCount, String artifactId) {}

    private NestedJarResult parseNestedJar(InputStream jarStream, String libName,
                                           OutputStream out) throws IOException {
        int count = 0;
        String artifactId = null;
        try (ZipInputStream zis = new ZipInputStream(jarStream)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String name = entry.getName();
                // Extract artifactId from pom.properties inside nested JAR
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
                        out.write(objectMapper.writeValueAsBytes(info));
                        out.write(NEWLINE);
                        count++;
                    }
                } catch (Exception e) { /* skip */ }
            }
        }
        return new NestedJarResult(count, artifactId);
    }

    /**
     * Extract Spring Boot config files from a JAR for MongoDB URI detection.
     * Returns a map of filename → content for application.yml/properties variants.
     */
    public Map<String, String> extractConfigFiles(File jarFile) throws IOException {
        Map<String, String> configs = new LinkedHashMap<>();
        try (JarFile jar = new JarFile(jarFile)) {
            boolean isSpringBoot = jar.getEntry("BOOT-INF/classes/") != null;
            String prefix = isSpringBoot ? "BOOT-INF/classes/" : "";

            for (String name : MongoCatalogService.CONFIG_FILE_NAMES) {
                JarEntry entry = jar.getJarEntry(prefix + name);
                if (entry != null) {
                    try (InputStream is = jar.getInputStream(entry)) {
                        configs.put(name, new String(is.readAllBytes(), StandardCharsets.UTF_8));
                    }
                }
            }
        }
        if (!configs.isEmpty()) {
            log.info("Extracted {} config file(s) from JAR: {}", configs.size(), configs.keySet());
        }
        return configs;
    }

    private static final Set<String> RESOURCE_EXTENSIONS = Set.of(
            ".properties", ".yml", ".yaml", ".json", ".xml", ".sql", ".conf", ".txt"
    );
    private static final int MAX_RESOURCE_FILES = 50;
    private static final int MAX_RESOURCE_BYTES = 200 * 1024;

    public Map<String, String> extractResourceFiles(File jarFile) throws IOException {
        Map<String, String> resources = new LinkedHashMap<>();
        try (JarFile jar = new JarFile(jarFile)) {
            boolean isSpringBoot = jar.getEntry("BOOT-INF/classes/") != null;
            boolean isWar = jar.getEntry("WEB-INF/classes/") != null;

            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements() && resources.size() < MAX_RESOURCE_FILES) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (entry.isDirectory()) continue;
                if (name.endsWith(".class")) continue;
                if (name.contains("META-INF/maven/")) continue;

                String lower = name.toLowerCase();
                boolean matchesExt = false;
                for (String ext : RESOURCE_EXTENSIONS) {
                    if (lower.endsWith(ext)) { matchesExt = true; break; }
                }
                if (!matchesExt) continue;

                if (isWar) {
                    if (!name.startsWith("WEB-INF/classes/") && name.contains("/")) {
                        String topDir = name.substring(0, name.indexOf('/'));
                        if (!topDir.equals("WEB-INF") && name.contains("/")) continue;
                    }
                } else if (isSpringBoot) {
                    if (!name.startsWith("BOOT-INF/classes/") && name.contains("/")) {
                        String topDir = name.substring(0, name.indexOf('/'));
                        if (!topDir.equals("BOOT-INF") && name.contains("/")) continue;
                    }
                }

                String filename = name.contains("/") ? name.substring(name.lastIndexOf('/') + 1) : name;
                if (filename.isBlank()) continue;
                if (resources.containsKey(filename)) {
                    filename = name.replace('/', '_');
                }

                try (InputStream is = jar.getInputStream(entry)) {
                    byte[] bytes = is.readNBytes(MAX_RESOURCE_BYTES);
                    String content = new String(bytes, StandardCharsets.UTF_8);
                    resources.put(filename, content);
                } catch (Exception e) { /* skip binary or unreadable */ }
            }
        }
        if (!resources.isEmpty()) {
            log.info("Extracted {} resource file(s) from JAR: {}", resources.size(), resources.keySet());
        }
        return resources;
    }

    public static class BundledJarInfo {
        public String name;
        public long size;
        public String manifest;
        public Map<String, String> resources = new LinkedHashMap<>();
    }

    private static final int MAX_BUNDLED_JARS = 100;
    private static final int MAX_NESTED_RESOURCE_FILES = 10;
    private static final Set<String> NESTED_RESOURCE_EXTENSIONS = Set.of(
            ".properties", ".yml", ".yaml", ".json", ".xml"
    );

    public List<BundledJarInfo> extractBundledJarInfo(File jarFile) throws IOException {
        List<BundledJarInfo> result = new ArrayList<>();
        try (JarFile jar = new JarFile(jarFile)) {
            boolean isSpringBoot = jar.getEntry("BOOT-INF/classes/") != null;
            boolean isWar = !isSpringBoot && jar.getEntry("WEB-INF/classes/") != null;
            String libPrefix = isWar ? "WEB-INF/lib/" : "BOOT-INF/lib/";

            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements() && result.size() < MAX_BUNDLED_JARS) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (!name.startsWith(libPrefix) || !name.endsWith(".jar")) continue;
                String jarName = name.substring(libPrefix.length());
                if (jarName.isEmpty() || jarName.contains("/")) continue;

                BundledJarInfo info = new BundledJarInfo();
                info.name = jarName;
                info.size = entry.getSize() < 0 ? 0 : entry.getSize();

                try (InputStream is = jar.getInputStream(entry)) {
                    byte[] nestedBytes = is.readAllBytes();
                    if (info.size == 0) info.size = nestedBytes.length;
                    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(nestedBytes))) {
                        ZipEntry ze;
                        while ((ze = zis.getNextEntry()) != null) {
                            String zeName = ze.getName();
                            if (zeName.equals("META-INF/MANIFEST.MF") && info.manifest == null) {
                                try {
                                    byte[] bytes = zis.readAllBytes();
                                    info.manifest = new String(bytes, StandardCharsets.UTF_8);
                                } catch (Exception ignored) {}
                                continue;
                            }
                            if (info.resources.size() < MAX_NESTED_RESOURCE_FILES && !ze.isDirectory()) {
                                String lower = zeName.toLowerCase();
                                boolean matches = false;
                                for (String ext : NESTED_RESOURCE_EXTENSIONS) {
                                    if (lower.endsWith(ext)) { matches = true; break; }
                                }
                                if (matches) {
                                    String fname = zeName.contains("/") ? zeName.substring(zeName.lastIndexOf('/') + 1) : zeName;
                                    if (!fname.isBlank()) {
                                        try {
                                            byte[] bytes = zis.readNBytes(MAX_RESOURCE_BYTES);
                                            String key = info.resources.containsKey(fname) ? zeName.replace('/', '_') : fname;
                                            info.resources.put(key, new String(bytes, StandardCharsets.UTF_8));
                                        } catch (Exception ignored) {}
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to inspect nested JAR {}: {}", jarName, e.getMessage());
                }

                result.add(info);
            }
        }
        log.info("Extracted info for {} bundled JARs from {}", result.size(), jarFile.getName());
        return result;
    }

    public Map<String, Set<String>> buildJarDependencyMap(File jarFile, List<ClassInfo> appClasses) throws IOException {
        Map<String, String> classToJar = new HashMap<>();
        try (JarFile jar = new JarFile(jarFile)) {
            boolean isSpringBoot = jar.getEntry("BOOT-INF/classes/") != null;
            boolean isWar = !isSpringBoot && jar.getEntry("WEB-INF/classes/") != null;
            String libPrefix = isWar ? "WEB-INF/lib/" : "BOOT-INF/lib/";

            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (!name.startsWith(libPrefix) || !name.endsWith(".jar")) continue;
                String jarName = name.substring(libPrefix.length());
                if (jarName.isEmpty() || jarName.contains("/")) continue;

                try (InputStream is = jar.getInputStream(entry)) {
                    byte[] nestedBytes = is.readAllBytes();
                    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(nestedBytes))) {
                        ZipEntry ze;
                        while ((ze = zis.getNextEntry()) != null) {
                            String zeName = ze.getName();
                            if (!zeName.endsWith(".class") || zeName.endsWith("module-info.class")) continue;
                            String className = zeName.substring(0, zeName.length() - 6).replace('/', '.');
                            classToJar.put(className, jarName);
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to index nested JAR {} for dep map: {}", jarName, e.getMessage());
                }
            }
        }

        Map<String, Set<String>> depMap = new LinkedHashMap<>();
        for (ClassInfo cls : appClasses) {
            if (cls.getSourceJar() != null) continue;
            String appClassName = cls.getSimpleName();
            for (var method : cls.getMethods()) {
                for (var inv : method.getInvocations()) {
                    String owner = inv.getOwnerClass();
                    if (owner == null) continue;
                    String mappedJar = classToJar.get(owner);
                    if (mappedJar != null) {
                        depMap.computeIfAbsent(mappedJar, k -> new LinkedHashSet<>()).add(appClassName);
                    }
                }
            }
        }

        log.info("Built JAR dependency map: {} bundled JARs referenced by app classes", depMap.size());
        return depMap;
    }

    /**
     * Stream-read classes from JSONL file, processing each with a callback.
     * This avoids loading the whole list into memory.
     */
    public void streamClasses(Path jsonlFile, ClassConsumer consumer) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(jsonlFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    ClassInfo ci = objectMapper.readValue(line, ClassInfo.class);
                    consumer.accept(ci);
                }
            }
        }
    }

    @FunctionalInterface
    public interface ClassConsumer {
        void accept(ClassInfo classInfo) throws IOException;
    }
}
