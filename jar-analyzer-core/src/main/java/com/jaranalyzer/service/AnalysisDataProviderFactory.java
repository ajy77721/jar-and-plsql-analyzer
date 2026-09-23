package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

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

    // The getXDocumentKey() accessors already answer through the store — they return null when
    // the artifact is absent — so no extra Files.exists() check: in s3 mode nothing exists locally.
    // Keys, not Paths: the provider's document identity is now the key itself.
    public AnalysisDataProvider resolve(String jarName, String version) {
        if ("static".equals(version)) {
            var key = persistenceService.getStaticDocumentKey(jarName);
            if (key == null)
                throw new AnalysisNotFoundException("Static analysis not found: " + jarName);
            return new StaticAnalysisProvider(key, objectMapper, jarDataPaths, jarName);
        }
        if ("previous".equals(version)) {
            var key = persistenceService.getCorrectedPrevDocumentKey(jarName);
            if (key == null)
                throw new AnalysisNotFoundException("Previous corrected version not found: " + jarName);
            return new ClaudeAnalysisProvider(key, objectMapper, jarDataPaths, jarName, "previous");
        }

        // Default: best available (corrected first, then static)
        var key = persistenceService.getDocumentKey(jarName);
        if (key == null)
            throw new AnalysisNotFoundException(
                    version != null ? "Version '" + version + "' not found: " + jarName
                            : "Analysis not found: " + jarName);

        var corrected = persistenceService.getCorrectedDocumentKey(jarName);
        if (corrected != null && key.equals(corrected)) {
            return new ClaudeAnalysisProvider(key, objectMapper, jarDataPaths, jarName, "claude");
        }
        return new StaticAnalysisProvider(key, objectMapper, jarDataPaths, jarName);
    }
}
