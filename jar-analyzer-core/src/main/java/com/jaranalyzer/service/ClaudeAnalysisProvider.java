package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;

public class ClaudeAnalysisProvider extends AbstractAnalysisDataProvider {

    private final String subType;

    public ClaudeAnalysisProvider(Path filePath, ObjectMapper objectMapper,
                                   JarDataPaths jarDataPaths, String jarName, String subType) {
        super(filePath, objectMapper, jarDataPaths, jarName);
        this.subType = subType;
    }

    @Override
    public String getProviderType() { return subType; }

    @Override
    public Path getSummaryCachePath() {
        return jarDataPaths.summaryCache(jarName);
    }

    @Override
    public boolean shouldComputeVerification() { return true; }
}
