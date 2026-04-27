package com.jaranalyzer.service;

import org.benf.cfr.reader.api.CfrDriver;
import org.benf.cfr.reader.api.OutputSinkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.jar.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Decompiles .class bytecode to actual Java source using CFR.
 * Works from stored JAR files — extracts class bytes, writes temp, runs CFR, returns source.
 */
@Service
public class DecompilerService {

    private static final Logger log = LoggerFactory.getLogger(DecompilerService.class);
    private static final long DECOMPILE_TIMEOUT_SECONDS = 30;
    private static final Map<String, String> CFR_OPTIONS = Map.of(
            "showversion", "false", "comments", "false",
            "decodestringswitch", "true", "sugarenums", "true",
            "removeboilerplate", "true", "removeinnerclasssynthetics", "true");
    private static final ExecutorService decompileExecutor =
            Executors.newFixedThreadPool(
                    Math.min(4, Runtime.getRuntime().availableProcessors()), r -> {
                Thread t = new Thread(r, "cfr-decompile");
                t.setDaemon(true);
                return t;
            });

    private static final ThreadLocal<Path> threadTempDir = new ThreadLocal<>();

    private Path getThreadTempDir() throws IOException {
        Path dir = threadTempDir.get();
        if (dir == null || !Files.isDirectory(dir)) {
            dir = Files.createTempDirectory("cfr-decompile-");
            threadTempDir.set(dir);
        }
        return dir;
    }

    /**
     * Decompile a single class from a JAR file.
     * @param jarPath path to the stored JAR file
     * @param className fully qualified class name (e.g. "com.example.MyService")
     * @return decompiled Java source code, or null if class not found
     */
    public String decompile(Path jarPath, String className) {
        if (jarPath == null || !Files.exists(jarPath)) return null;

        try {
            byte[] classBytes = extractClassBytes(jarPath, className);
            if (classBytes == null) {
                log.debug("Class {} not found in JAR {}", className, jarPath.getFileName());
                return null;
            }

            Path tempDir = getThreadTempDir();
            Future<String> future = decompileExecutor.submit(() -> decompileBytes(classBytes, className, tempDir));
            try {
                return future.get(DECOMPILE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                log.warn("Decompile timed out after {}s for {}", DECOMPILE_TIMEOUT_SECONDS, className);
                return null;
            } catch (ExecutionException e) {
                log.warn("Decompile failed for {}: {}", className, e.getCause().getMessage());
                return null;
            }
        } catch (Throwable e) {
            log.warn("Failed to decompile {}: {}", className, e.getMessage());
            return null;
        }
    }

    /**
     * Extract a specific method's source code from a decompiled class.
     * Decompiles the class with CFR, then finds the method by name and extracts its body.
     * Uses brace-matching to handle nested blocks correctly.
     *
     * @param fullSource the full decompiled class source
     * @param methodName the method to extract
     * @return the method's source code (signature + body), or null if not found
     */
    /**
     * Extract a method from decompiled source by name.
     * For overloaded methods, pass parameterTypes to match the right overload.
     */
    public String extractMethod(String fullSource, String methodName) {
        return extractMethod(fullSource, methodName, null);
    }

    /**
     * Extract a specific overloaded method by name + parameter types.
     * Handles: overloading (same name, different params), covariant return,
     * bridge methods, method hiding — all by matching the parameter signature.
     *
     * @param parameterTypes if null or empty, returns the first match (backward compatible).
     *                       If provided, matches the overload whose params contain these type names.
     */
    public String extractMethod(String fullSource, String methodName, List<String> parameterTypes) {
        if (fullSource == null || methodName == null) return null;

        String needle = methodName + "(";
        List<Integer> candidateOffsets = new ArrayList<>();

        // Phase 1: Find all declaration positions using indexOf (avoids split)
        int searchFrom = 0;
        while (searchFrom < fullSource.length()) {
            int pos = fullSource.indexOf(needle, searchFrom);
            if (pos < 0) break;
            searchFrom = pos + needle.length();

            // Find line start for this position
            int lineStart = fullSource.lastIndexOf('\n', pos);
            lineStart = lineStart < 0 ? 0 : lineStart + 1;
            String beforeOnLine = fullSource.substring(lineStart, pos).trim();

            if (beforeOnLine.startsWith("//") || beforeOnLine.startsWith("*") || beforeOnLine.startsWith("/*")) continue;
            // Check if this is a method call (char before method name is a dot) vs a declaration
            if (pos > 0 && fullSource.charAt(pos - 1) == '.') continue;
            String beforeMethod = beforeOnLine;
            if (beforeMethod.isEmpty()
                    || beforeMethod.endsWith("=") || beforeMethod.endsWith("(")) continue;
            candidateOffsets.add(lineStart);
        }

        if (candidateOffsets.isEmpty()) return null;

        // Phase 2: Pick best candidate based on parameter matching
        int bestOffset;
        if (candidateOffsets.size() == 1 || parameterTypes == null || parameterTypes.isEmpty()) {
            bestOffset = candidateOffsets.get(0);
        } else {
            int bestIdx = 0;
            int bestScore = -1;
            for (int c = 0; c < candidateOffsets.size(); c++) {
                int offset = candidateOffsets.get(c);
                int parenStart = fullSource.indexOf(needle, offset) + needle.length();
                int bracePos = fullSource.indexOf('{', parenStart);
                if (bracePos < 0) bracePos = Math.min(offset + 500, fullSource.length());
                int closeParen = fullSource.indexOf(')', parenStart);
                if (closeParen < 0 || closeParen > bracePos) closeParen = bracePos;
                String paramSection = fullSource.substring(parenStart, closeParen);

                int score = 0;
                for (String pt : parameterTypes) {
                    String shortType = pt.contains(".") ? pt.substring(pt.lastIndexOf('.') + 1) : pt;
                    if (paramSection.contains(shortType)) score++;
                }
                long sigParamCount = paramSection.isBlank() ? 0 :
                        paramSection.chars().filter(ch -> ch == ',').count() + 1;
                if (sigParamCount == parameterTypes.size()) score += 2;

                if (score > bestScore) {
                    bestScore = score;
                    bestIdx = c;
                }
            }
            bestOffset = candidateOffsets.get(bestIdx);
        }

        // Phase 3: Brace-match from bestOffset to find end of method
        int braceDepth = 0;
        boolean foundOpen = false;
        int end = bestOffset;
        for (int i = bestOffset; i < fullSource.length(); i++) {
            char ch = fullSource.charAt(i);
            if (ch == '{') { braceDepth++; foundOpen = true; }
            else if (ch == '}') {
                braceDepth--;
                if (foundOpen && braceDepth == 0) { end = i + 1; break; }
            }
        }
        if (!foundOpen) return null;

        return fullSource.substring(bestOffset, end);
    }

    /**
     * Decompile a class and extract a specific method in one call.
     * Caches nothing — caller should cache the full class source if extracting multiple methods.
     */
    public String decompileMethod(Path jarPath, String className, String methodName) {
        String fullSource = decompile(jarPath, className);
        if (fullSource == null) return null;
        return extractMethod(fullSource, methodName);
    }

    /**
     * Batch decompile: decompile a class once, extract multiple methods.
     * Returns a map of methodName → source code.
     * Much more efficient than calling decompileMethod() per method.
     */
    public Map<String, String> decompileMethods(Path jarPath, String className, List<String> methodNames) {
        Map<String, String> results = new LinkedHashMap<>();
        String fullSource = decompile(jarPath, className);
        if (fullSource == null) return results;

        for (String methodName : methodNames) {
            String methodSource = extractMethod(fullSource, methodName);
            if (methodSource != null) {
                results.put(methodName, methodSource);
            }
        }
        return results;
    }

    // Cache: package prefix -> nested JAR entry name (avoids scanning all 200+ nested JARs per class)
    private final Map<String, String> packageToNestedJar = new ConcurrentHashMap<>();
    private volatile boolean nestedJarIndexBuilt = false;
    private final Object indexLock = new Object();

    private void buildNestedJarIndex(Path jarPath) {
        if (nestedJarIndexBuilt) return;
        synchronized (indexLock) {
            if (nestedJarIndexBuilt) return;
            try (JarFile jar = new JarFile(jarPath.toFile())) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry libEntry = entries.nextElement();
                    String name = libEntry.getName();
                    if (!name.startsWith("BOOT-INF/lib/") || !name.endsWith(".jar")) continue;
                    try (InputStream libIs = jar.getInputStream(libEntry);
                         ZipInputStream zis = new ZipInputStream(libIs)) {
                        ZipEntry ze;
                        Set<String> seen = new HashSet<>();
                        while ((ze = zis.getNextEntry()) != null) {
                            String zeName = ze.getName();
                            if (!zeName.endsWith(".class")) continue;
                            int lastSlash = zeName.lastIndexOf('/');
                            if (lastSlash <= 0) continue;
                            String pkg = zeName.substring(0, lastSlash);
                            if (seen.add(pkg)) {
                                packageToNestedJar.putIfAbsent(pkg, name);
                            }
                        }
                    } catch (Exception e) {
                        // skip corrupt nested JARs
                    }
                }
                nestedJarIndexBuilt = true;
                log.info("Nested JAR index: {} packages mapped", packageToNestedJar.size());
            } catch (IOException e) {
                log.warn("Failed to build nested JAR index: {}", e.getMessage());
            }
        }
    }

    /**
     * Extract raw .class bytes from a JAR (handles both standard and Spring Boot fat JARs).
     */
    private byte[] extractClassBytes(Path jarPath, String className) throws IOException {
        String classFilePath = className.replace('.', '/') + ".class";
        String bootInfPath = "BOOT-INF/classes/" + classFilePath;

        int lastSlash = classFilePath.lastIndexOf('/');
        String packagePath = lastSlash > 0 ? classFilePath.substring(0, lastSlash) : "";

        try (JarFile jar = new JarFile(jarPath.toFile())) {
            JarEntry entry = jar.getJarEntry(classFilePath);
            if (entry != null) {
                try (InputStream is = jar.getInputStream(entry)) {
                    return is.readAllBytes();
                }
            }

            entry = jar.getJarEntry(bootInfPath);
            if (entry != null) {
                try (InputStream is = jar.getInputStream(entry)) {
                    return is.readAllBytes();
                }
            }

            // Build full package→nested JAR index on first miss
            buildNestedJarIndex(jarPath);

            String cachedJar = packageToNestedJar.get(packagePath);
            if (cachedJar != null) {
                entry = jar.getJarEntry(cachedJar);
                if (entry != null) {
                    try (InputStream libIs = jar.getInputStream(entry);
                         ZipInputStream zis = new ZipInputStream(libIs)) {
                        ZipEntry ze;
                        while ((ze = zis.getNextEntry()) != null) {
                            if (ze.getName().equals(classFilePath)) {
                                return zis.readAllBytes();
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Decompile class bytes using CFR. Temp dir is managed by caller.
     */
    private String decompileBytes(byte[] classBytes, String className, Path tempDir) throws IOException {
        String classFilePath = className.replace('.', '/') + ".class";
        Path classFile = tempDir.resolve(classFilePath);
        Files.createDirectories(classFile.getParent());
        Files.write(classFile, classBytes);
        try {
            return doDecompile(classFile, className);
        } finally {
            try { Files.deleteIfExists(classFile); } catch (Exception ignored) {}
        }
    }

    private String doDecompile(Path classFile, String className) {

        List<String> decompiled = new ArrayList<>();

        OutputSinkFactory sinkFactory = new OutputSinkFactory() {
            @Override
            public List<SinkClass> getSupportedSinks(SinkType sinkType, Collection<SinkClass> available) {
                return List.of(SinkClass.STRING);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Sink<T> getSink(SinkType sinkType, SinkClass sinkClass) {
                return (T value) -> {
                    if (sinkType == SinkType.JAVA) {
                        decompiled.add(value.toString());
                    }
                };
            }
        };

        CfrDriver driver = new CfrDriver.Builder()
                .withOptions(CFR_OPTIONS)
                .withOutputSink(sinkFactory)
                .build();

        try {
            driver.analyse(List.of(classFile.toString()));
        } catch (Throwable t) {
            log.debug("CFR failed for {}: {}", className, t.getMessage());
        }

        if (!decompiled.isEmpty()) {
            return String.join("\n", decompiled);
        }
        return null;
    }

    private void cleanTempDir(Path tempDir) {
        if (tempDir == null) return;
        try {
            try (var stream = Files.walk(tempDir)) {
                stream.sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try { Files.deleteIfExists(p); }
                            catch (Exception ex) { /* best effort */ }
                        });
            }
        } catch (Exception e) {
            log.debug("Temp cleanup failed: {}", tempDir);
        }
    }
}
