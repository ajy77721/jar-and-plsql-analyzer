package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

@Primary
@Service("jarPersistenceService")
public class PersistenceService {

    private static final Logger log = LoggerFactory.getLogger(PersistenceService.class);

    private final ObjectMapper objectMapper;
    private final JarDataPaths paths;

    public PersistenceService(JarDataPaths paths) {
        this.objectMapper = new ObjectMapper();
        this.paths = paths;
    }

    public JarDataPaths getPaths() { return paths; }

    /* ---- JAR file storage ---- */

    public Path storeJar(Path tempJar, String jarName) throws IOException {
        paths.ensureJarRoot(jarName);
        Path dest = paths.storedJarFile(jarName);
        Files.copy(tempJar, dest, StandardCopyOption.REPLACE_EXISTING);
        log.info("Stored JAR: {} ({} MB)", dest.getFileName(), Files.size(dest) / (1024 * 1024));
        return dest;
    }

    public Path getJarFilePath(String id) {
        Path file = paths.storedJarFile(id);
        return Files.exists(file) ? file : null;
    }

    /* ---- Per-JAR domain config storage ---- */

    public Path storeDomainConfig(Path tempConfig, String jarName) throws IOException {
        paths.ensureJarRoot(jarName);
        Path dest = paths.domainConfigFile(jarName);
        Files.copy(tempConfig, dest, StandardCopyOption.REPLACE_EXISTING);
        log.info("Stored per-JAR domain config: {}", dest);
        return dest;
    }

    public Path getDomainConfigPath(String jarName) {
        Path file = paths.domainConfigFile(jarName);
        return Files.exists(file) ? file : null;
    }

    /* ---- Connection info storage ---- */

    public void storeConnections(String jarName, Map<String, Object> info) {
        try {
            paths.ensureJarRoot(jarName);
            Path dest = paths.connectionsFile(jarName);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(dest.toFile(), info);
            log.info("Stored connections.json for {}", jarName);
        } catch (IOException e) {
            log.warn("Failed to store connections.json for {}: {}", jarName, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> loadConnections(String jarName) {
        Path file = paths.connectionsFile(jarName);
        if (!Files.exists(file)) return null;
        try {
            return objectMapper.readValue(file.toFile(), Map.class);
        } catch (Exception e) {
            log.warn("Failed to load connections.json for {}: {}", jarName, e.getMessage());
            return null;
        }
    }

    /* ---- Streaming write (upload time — always writes STATIC) ---- */

    public void writeAnalysisStreaming(String jarName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints) throws IOException {
        writeAnalysisStreaming(jarName, null, jarSize, analyzedAt, totalClasses, totalEndpoints, classesJsonlFile, endpoints);
    }

    public void writeAnalysisStreaming(String jarName, String projectName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints) throws IOException {
        writeAnalysisStreaming(jarName, projectName, jarSize, analyzedAt, totalClasses, totalEndpoints, classesJsonlFile, endpoints, "STATIC");
    }

    public void writeAnalysisStreaming(String jarName, String projectName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints, String analysisMode) throws IOException {
        writeAnalysisStreaming(jarName, projectName, jarSize, analyzedAt, totalClasses, totalEndpoints,
                classesJsonlFile, endpoints, analysisMode, null);
    }

    public void writeAnalysisStreaming(String jarName, String projectName, long jarSize, String analyzedAt,
                                       int totalClasses, int totalEndpoints,
                                       Path classesJsonlFile,
                                       List<EndpointInfo> endpoints, String analysisMode,
                                       String basePackage) throws IOException {
        paths.ensureJarRoot(jarName);
        writeAnalysisStreamingTo(paths.analysisFile(jarName), jarName, projectName, jarSize, analyzedAt,
                totalClasses, totalEndpoints, classesJsonlFile, endpoints, analysisMode, basePackage);
        invalidateSummaryCache(jarName);
        preGenerateSummaryCache(jarName, true);
    }

    public void writeAnalysisStreamingCorrected(String jarName, String projectName, long jarSize, String analyzedAt,
                                                 int totalClasses, int totalEndpoints,
                                                 Path classesJsonlFile,
                                                 List<EndpointInfo> endpoints, String analysisMode) throws IOException {
        paths.ensureJarRoot(jarName);
        writeAnalysisStreamingTo(paths.correctedFile(jarName), jarName, projectName, jarSize, analyzedAt,
                totalClasses, totalEndpoints, classesJsonlFile, endpoints, analysisMode, null);
        invalidateSummaryCache(jarName);
        preGenerateSummaryCache(jarName, false);
    }

    private void writeAnalysisStreamingTo(Path outPath, String jarName, String projectName, long jarSize,
                                           String analyzedAt, int totalClasses, int totalEndpoints,
                                           Path classesJsonlFile, List<EndpointInfo> endpoints,
                                           String analysisMode, String basePackage) throws IOException {
        log.info("Streaming analysis to {} ...", outPath);

        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(outPath), 256 * 1024);
             JsonGenerator gen = objectMapper.getFactory().createGenerator(os)) {

            gen.writeStartObject();
            gen.writeStringField("jarName", jarName);
            if (projectName != null) gen.writeStringField("projectName", projectName);
            gen.writeNumberField("jarSize", jarSize);
            gen.writeStringField("analyzedAt", analyzedAt);
            gen.writeNumberField("totalClasses", totalClasses);
            gen.writeNumberField("totalEndpoints", totalEndpoints);
            gen.writeStringField("analysisMode", analysisMode != null ? analysisMode : "STATIC");
            if (basePackage != null && !basePackage.isBlank()) gen.writeStringField("basePackage", basePackage);

            gen.writeFieldName("classes");
            gen.writeStartArray();
            int classCount = 0;
            try (BufferedReader reader = Files.newBufferedReader(classesJsonlFile)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isBlank()) {
                        gen.writeRawValue(line);
                        classCount++;
                        if (classCount % 500 == 0) {
                            gen.flush();
                            log.info("  ... {} classes written to JSON", classCount);
                        }
                    }
                }
            }
            gen.writeEndArray();
            log.info("  {} classes written", classCount);

            gen.writeFieldName("endpoints");
            gen.writeStartArray();
            int epCount = 0;
            for (EndpointInfo ep : endpoints) {
                objectMapper.writeValue(gen, ep);
                epCount++;
                if (epCount % 100 == 0) {
                    gen.flush();
                }
            }
            gen.writeEndArray();
            log.info("  {} endpoints written", epCount);

            gen.writeEndObject();
            gen.flush();
        }

        long sizeMB = Files.size(outPath) / (1024 * 1024);
        log.info("Analysis saved: {} ({} MB)", outPath.getFileName(), sizeMB);

        // Write lightweight header sidecar for fast listing
        writeHeaderSidecar(outPath.getParent(), jarName, projectName, jarSize, analyzedAt,
                totalClasses, totalEndpoints, analysisMode, basePackage);
    }

    private void writeHeaderSidecar(Path jarRoot, String jarName, String projectName,
                                     long jarSize, String analyzedAt,
                                     int totalClasses, int totalEndpoints, String analysisMode) {
        writeHeaderSidecar(jarRoot, jarName, projectName, jarSize, analyzedAt, totalClasses, totalEndpoints, analysisMode, null);
    }

    private void writeHeaderSidecar(Path jarRoot, String jarName, String projectName,
                                     long jarSize, String analyzedAt,
                                     int totalClasses, int totalEndpoints, String analysisMode,
                                     String basePackage) {
        try {
            Map<String, Object> header = new LinkedHashMap<>();
            header.put("jarName", jarName);
            if (projectName != null) header.put("projectName", projectName);
            header.put("jarSize", jarSize);
            header.put("analyzedAt", analyzedAt);
            header.put("totalClasses", totalClasses);
            header.put("totalEndpoints", totalEndpoints);
            header.put("analysisMode", analysisMode != null ? analysisMode : "STATIC");
            if (basePackage != null && !basePackage.isBlank()) header.put("basePackage", basePackage);
            objectMapper.writeValue(jarRoot.resolve("_header.json").toFile(), header);
        } catch (IOException e) {
            log.debug("Failed to write header sidecar: {}", e.getMessage());
        }
    }

    /* ---- Load / Save ---- */

    public void save(JarAnalysis analysis) throws IOException {
        paths.ensureJarRoot(analysis.getJarName());
        Path filePath = paths.analysisFile(analysis.getJarName());
        log.info("Saving static analysis to {} ...", filePath);
        objectMapper.writeValue(filePath.toFile(), analysis);
        long sizeMB = Files.size(filePath) / (1024 * 1024);
        log.info("Saved {} ({} MB)", filePath.getFileName(), sizeMB);
        writeHeaderSidecar(filePath.getParent(), analysis.getJarName(), analysis.getProjectName(),
                analysis.getJarSize(), analysis.getAnalyzedAt(),
                analysis.getTotalClasses(), analysis.getTotalEndpoints(), analysis.getAnalysisMode(),
                analysis.getBasePackage());
    }

    public void saveCorrected(JarAnalysis analysis) throws IOException {
        paths.ensureJarRoot(analysis.getJarName());
        Path filePath = paths.correctedFile(analysis.getJarName());
        log.info("Saving corrected analysis to {} ...", filePath);
        objectMapper.writeValue(filePath.toFile(), analysis);
        long sizeMB = Files.size(filePath) / (1024 * 1024);
        log.info("Saved corrected {} ({} MB)", filePath.getFileName(), sizeMB);
        writeHeaderSidecar(filePath.getParent(), analysis.getJarName(), analysis.getProjectName(),
                analysis.getJarSize(), analysis.getAnalyzedAt(),
                analysis.getTotalClasses(), analysis.getTotalEndpoints(), analysis.getAnalysisMode(),
                analysis.getBasePackage());
        invalidateSummaryCache(analysis.getJarName());
    }

    public JarAnalysis load(String jarName) throws IOException {
        Path corrected = paths.correctedFile(jarName);
        if (Files.exists(corrected)) {
            log.info("Loading corrected analysis from {} ({} MB)...",
                    corrected.getFileName(), Files.size(corrected) / (1024 * 1024));
            return objectMapper.readValue(corrected.toFile(), JarAnalysis.class);
        }
        Path staticFile = paths.analysisFile(jarName);
        if (Files.exists(staticFile)) {
            log.info("Loading static analysis from {} ({} MB)...",
                    staticFile.getFileName(), Files.size(staticFile) / (1024 * 1024));
            return objectMapper.readValue(staticFile.toFile(), JarAnalysis.class);
        }
        return null;
    }

    public JarAnalysis loadStatic(String jarName) throws IOException {
        Path file = paths.analysisFile(jarName);
        if (Files.exists(file)) {
            log.info("Loading static analysis from {} ({} MB)...",
                    file.getFileName(), Files.size(file) / (1024 * 1024));
            return objectMapper.readValue(file.toFile(), JarAnalysis.class);
        }
        return null;
    }

    public void invalidateSummaryCache(String jarName) {
        try {
            Files.deleteIfExists(paths.summaryCache(jarName));
            Files.deleteIfExists(Path.of(paths.summaryCache(jarName) + ".gz"));
            Files.deleteIfExists(paths.summaryCacheStatic(jarName));
            Files.deleteIfExists(Path.of(paths.summaryCacheStatic(jarName) + ".gz"));
        } catch (IOException e) { log.debug("Failed to invalidate summary cache: {}", e.getMessage()); }
    }

    /**
     * Pre-generate summary cache from the analysis file so first UI open is instant.
     * Called after saving analysis.json — adds ~20-30s to upload but saves that on every first open.
     */
    public void preGenerateSummaryCache(String jarName, boolean isStatic) {
        Path analysisFile = isStatic ? paths.analysisFile(jarName) : paths.correctedFile(jarName);
        Path summaryCache = isStatic ? paths.summaryCacheStatic(jarName) : paths.summaryCache(jarName);
        Path gzCache = Path.of(summaryCache + ".gz");
        if (!Files.exists(analysisFile)) return;
        try {
            log.info("Pre-generating summary cache for {} ...", jarName);
            long start = System.currentTimeMillis();
            AnalysisDataProvider provider = isStatic
                    ? new StaticAnalysisProvider(analysisFile, objectMapper, paths, jarName)
                    : new ClaudeAnalysisProvider(analysisFile, objectMapper, paths, jarName, "claude");
            java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(8 * 1024 * 1024);
            try (var parser = objectMapper.getFactory().createParser(analysisFile.toFile());
                 var gen = objectMapper.getFactory().createGenerator(buf)) {
                provider.streamSummary(parser, gen);
                gen.flush();
            }
            byte[] data = buf.toByteArray();
            Files.write(summaryCache, data);
            try (var gzOut = new java.util.zip.GZIPOutputStream(
                    new java.io.BufferedOutputStream(Files.newOutputStream(gzCache), 64 * 1024))) {
                gzOut.write(data);
            }
            long ms = System.currentTimeMillis() - start;
            log.info("Summary cache pre-generated: {} ({} MB, {}ms)", summaryCache.getFileName(),
                    data.length / (1024 * 1024), ms);
        } catch (Exception e) {
            log.warn("Failed to pre-generate summary cache: {}", e.getMessage());
        }
    }

    /* ---- File path getters for streaming responses ---- */

    public Path getFilePath(String jarName) {
        Path corrected = paths.correctedFile(jarName);
        if (Files.exists(corrected)) return corrected;
        Path staticFile = paths.analysisFile(jarName);
        return Files.exists(staticFile) ? staticFile : null;
    }

    public Path getStaticFilePath(String jarName) {
        Path file = paths.analysisFile(jarName);
        return Files.exists(file) ? file : null;
    }

    public Path getCorrectedFilePath(String jarName) {
        Path file = paths.correctedFile(jarName);
        return Files.exists(file) ? file : null;
    }

    public Path getCorrectedPrevFilePath(String jarName) {
        Path file = paths.correctedPrevFile(jarName);
        return Files.exists(file) ? file : null;
    }

    /* ---- Version management ---- */

    public void rotateCorrected(String jarName) throws IOException {
        Path current = paths.correctedFile(jarName);
        if (!Files.exists(current)) return;
        Path prev = paths.correctedPrevFile(jarName);
        Files.move(current, prev, StandardCopyOption.REPLACE_EXISTING);
        log.info("Rotated corrected -> prev: {}", prev.getFileName());
    }

    public void revertCorrected(String jarName) throws IOException {
        Path prev = paths.correctedPrevFile(jarName);
        if (!Files.exists(prev)) {
            throw new IOException("No previous corrected version to revert to");
        }
        Path current = paths.correctedFile(jarName);
        Files.move(prev, current, StandardCopyOption.REPLACE_EXISTING);
        log.info("Reverted: prev -> corrected: {}", current.getFileName());
    }

    public Map<String, Object> getVersionInfo(String jarName) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("hasStatic", Files.exists(paths.analysisFile(jarName)));
        info.put("hasCorrected", Files.exists(paths.correctedFile(jarName)));
        info.put("hasCorrectedPrev", Files.exists(paths.correctedPrevFile(jarName)));
        Path corrected = paths.correctedFile(jarName);
        if (Files.exists(corrected)) {
            try {
                Map<String, Object> header = readHeaderOnly(corrected);
                if (header != null) {
                    if (header.containsKey("claudeIteration"))
                        info.put("claudeIteration", header.get("claudeIteration"));
                    if (header.containsKey("correctionAppliedAt"))
                        info.put("correctionAppliedAt", header.get("correctionAppliedAt"));
                    if (header.containsKey("correctionCount"))
                        info.put("correctionCount", header.get("correctionCount"));
                }
            } catch (IOException e) { /* defaults */ }
        }
        info.putIfAbsent("claudeIteration", 0);
        return info;
    }

    /* ---- Listing ---- */

    public List<Map<String, Object>> listJars() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Path root : paths.listJarRoots()) {
            try {
                String id = root.getFileName().toString();
                Path headerFile = root.resolve("_header.json");
                Path corrected = root.resolve("analysis_corrected.json");
                Path staticFile = root.resolve("analysis.json");

                Map<String, Object> entry = null;

                // Fast path: read small sidecar file
                if (Files.exists(headerFile)) {
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> h = objectMapper.readValue(headerFile.toFile(), LinkedHashMap.class);
                        entry = h;
                    } catch (IOException ignore) {}
                }

                // Slow fallback: parse header from large analysis file, then write sidecar for next time
                if (entry == null) {
                    if (Files.exists(corrected)) {
                        entry = readHeaderOnly(corrected);
                    } else {
                        entry = readHeaderOnly(staticFile);
                    }
                    if (entry != null) {
                        try {
                            objectMapper.writeValue(headerFile.toFile(), entry);
                        } catch (IOException ignore) {}
                    }
                }

                if (entry != null) {
                    entry.put("id", id);
                    if (!entry.containsKey("analysisMode") || entry.get("analysisMode") == null) {
                        entry.put("analysisMode", Files.exists(corrected) ? "CORRECTED" : "STATIC");
                    }
                    entry.put("hasPerJarConfig", Files.exists(root.resolve("domain-config.json")));
                    entry.put("hasConnections", Files.exists(root.resolve("connections.json")));
                    // Enrich from corrected file header if it exists and sidecar didn't have correction info
                    if (Files.exists(corrected) && !entry.containsKey("correctionAppliedAt")) {
                        try {
                            Map<String, Object> corrHeader = readHeaderOnly(corrected);
                            if (corrHeader != null) {
                                if (corrHeader.containsKey("correctionAppliedAt")) entry.put("correctionAppliedAt", corrHeader.get("correctionAppliedAt"));
                                if (corrHeader.containsKey("correctionCount")) entry.put("correctionCount", corrHeader.get("correctionCount"));
                                if (corrHeader.containsKey("claudeIteration")) entry.put("claudeIteration", corrHeader.get("claudeIteration"));
                                if (corrHeader.containsKey("analysisMode")) entry.put("analysisMode", corrHeader.get("analysisMode"));
                            }
                        } catch (IOException ignore) {}
                    }
                    result.add(entry);
                }
            } catch (IOException e) {
                // Skip corrupt entries
            }
        }
        result.sort((a, b) -> {
            String sa = (String) a.get("analyzedAt");
            String sb = (String) b.get("analyzedAt");
            if (sa == null && sb == null) return 0;
            if (sa == null) return 1;
            if (sb == null) return -1;
            return sb.compareTo(sa);
        });
        return result;
    }

    private Map<String, Object> readHeaderOnly(Path file) throws IOException {
        try (com.fasterxml.jackson.core.JsonParser parser =
                     objectMapper.getFactory().createParser(file.toFile())) {
            Map<String, Object> entry = new LinkedHashMap<>();
            if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_OBJECT) return null;

            while (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_OBJECT) {
                String field = parser.currentName();
                parser.nextToken();

                switch (field) {
                    case "jarName" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("jarName", parser.getText());
                    }
                    case "projectName" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("projectName", parser.getText());
                    }
                    case "jarSize" -> entry.put("jarSize", parser.getLongValue());
                    case "analyzedAt" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("analyzedAt", parser.getText());
                    }
                    case "totalClasses" -> entry.put("totalClasses", parser.getIntValue());
                    case "totalEndpoints" -> entry.put("totalEndpoints", parser.getIntValue());
                    case "analysisMode" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("analysisMode", parser.getText());
                    }
                    case "correctionAppliedAt" -> {
                        if (parser.currentToken() != com.fasterxml.jackson.core.JsonToken.VALUE_NULL)
                            entry.put("correctionAppliedAt", parser.getText());
                    }
                    case "correctionCount" -> entry.put("correctionCount", parser.getIntValue());
                    case "claudeIteration" -> entry.put("claudeIteration", parser.getIntValue());
                    case "classes", "endpoints" -> {
                        parser.skipChildren();
                        if (entry.containsKey("jarName") && entry.containsKey("totalEndpoints")) return entry;
                    }
                    default -> parser.skipChildren();
                }
            }
            return entry.isEmpty() ? null : entry;
        }
    }

    /* ---- Delete ---- */

    public boolean delete(String jarName) throws IOException {
        Path root = paths.jarRoot(jarName);
        boolean existed = Files.isDirectory(root);
        paths.deleteJarRoot(jarName);
        return existed;
    }
}
