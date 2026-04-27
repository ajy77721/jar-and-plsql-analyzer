package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class AnalysisDataProviderFactory {

    private final PersistenceService persistenceService;
    private final JarDataPaths jarDataPaths;
    private final ObjectMapper objectMapper;

    public AnalysisDataProviderFactory(PersistenceService persistenceService,
                                        JarDataPaths jarDataPaths,
                                        ObjectMapper objectMapper) {
        this.persistenceService = persistenceService;
        this.jarDataPaths = jarDataPaths;
        this.objectMapper = objectMapper;
    }

    public AnalysisDataProvider resolve(String jarName, String version) {
        if ("static".equals(version)) {
            Path filePath = persistenceService.getStaticFilePath(jarName);
            if (filePath == null || !Files.exists(filePath))
                throw new AnalysisNotFoundException("Static analysis not found: " + jarName);
            return new StaticAnalysisProvider(filePath, objectMapper, jarDataPaths, jarName);
        }
        if ("previous".equals(version)) {
            Path filePath = persistenceService.getCorrectedPrevFilePath(jarName);
            if (filePath == null || !Files.exists(filePath))
                throw new AnalysisNotFoundException("Previous corrected version not found: " + jarName);
            return new ClaudeAnalysisProvider(filePath, objectMapper, jarDataPaths, jarName, "previous");
        }

        // Default: best available (corrected first, then static)
        Path filePath = persistenceService.getFilePath(jarName);
        if (filePath == null || !Files.exists(filePath))
            throw new AnalysisNotFoundException(
                    version != null ? "Version '" + version + "' not found: " + jarName
                            : "Analysis not found: " + jarName);

        Path corrected = persistenceService.getCorrectedFilePath(jarName);
        if (corrected != null && filePath.equals(corrected)) {
            return new ClaudeAnalysisProvider(filePath, objectMapper, jarDataPaths, jarName, "claude");
        }
        return new StaticAnalysisProvider(filePath, objectMapper, jarDataPaths, jarName);
    }
}
