package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CorrectionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

@Component
public class CorrectionPersistence {

    private static final Logger log = LoggerFactory.getLogger(CorrectionPersistence.class);
    private final JarDataPaths jarDataPaths;
    private final ObjectMapper objectMapper = new ObjectMapper();

    CorrectionPersistence(JarDataPaths jarDataPaths) {
        this.jarDataPaths = jarDataPaths;
    }

    public Path getWorkDir(String jarName) {
        return jarDataPaths.correctionsDir(jarName);
    }

    public void saveCorrection(String jarName, String endpointKey, CorrectionResult result) {
        try {
            Path dir = getWorkDir(jarName);
            Files.createDirectories(dir);
            String safeName = sanitize(endpointKey) + "_correction.json";
            Path file = dir.resolve(safeName);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), result);
        } catch (IOException e) {
            log.warn("Failed to save correction for {}: {}", endpointKey, e.getMessage());
        }
    }

    public CorrectionResult loadCorrection(String jarName, String endpointKey) {
        try {
            Path file = getWorkDir(jarName).resolve(sanitize(endpointKey) + "_correction.json");
            if (Files.exists(file)) {
                return objectMapper.readValue(file.toFile(), CorrectionResult.class);
            }
        } catch (IOException e) {
            log.warn("Failed to load correction for {}: {}", endpointKey, e.getMessage());
        }
        return null;
    }

    public Map<String, CorrectionResult> loadAllCorrections(String jarName) {
        Map<String, CorrectionResult> results = new LinkedHashMap<>();
        Path dir = getWorkDir(jarName);
        if (!Files.exists(dir)) return results;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*_correction.json")) {
            for (Path file : stream) {
                try {
                    CorrectionResult cr = objectMapper.readValue(file.toFile(), CorrectionResult.class);
                    if (cr.getEndpointName() != null) {
                        results.put(cr.getEndpointName(), cr);
                    }
                } catch (IOException e) {
                    log.debug("Skipping malformed correction file: {}", file.getFileName());
                }
            }
        } catch (IOException e) {
            log.warn("Failed to list corrections for {}: {}", jarName, e.getMessage());
        }
        return results;
    }

    void saveSummary(String jarName, Map<String, Object> summary) {
        try {
            Path dir = getWorkDir(jarName);
            Files.createDirectories(dir);
            objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValue(dir.resolve("_summary.json").toFile(), summary);
        } catch (IOException e) {
            log.warn("Failed to save correction summary: {}", e.getMessage());
        }
    }

    public Map<String, Object> loadSummary(String jarName) {
        try {
            Path file = getWorkDir(jarName).resolve("_summary.json");
            if (Files.exists(file)) {
                return objectMapper.readValue(file.toFile(), Map.class);
            }
        } catch (IOException e) {
            log.debug("No correction summary for {}", jarName);
        }
        return null;
    }

    boolean hasData(String jarName) {
        Path dir = getWorkDir(jarName);
        if (!Files.exists(dir)) return false;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*_correction.json")) {
            return stream.iterator().hasNext();
        } catch (IOException e) {
            return false;
        }
    }

    public void deleteAll(String jarName) {
        Path dir = getWorkDir(jarName);
        if (!Files.exists(dir)) return;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path file : stream) Files.deleteIfExists(file);
            Files.deleteIfExists(dir);
            log.info("Deleted correction data for {}", jarName);
        } catch (IOException e) {
            log.warn("Failed to delete correction data for {}: {}", jarName, e.getMessage());
        }
    }

    private String sanitize(String name) {
        return JarNameUtil.sanitize(name);
    }
}
