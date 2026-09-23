package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ClaudeAnalysisProvider extends AbstractAnalysisDataProvider {

    private final String subType;

    public ClaudeAnalysisProvider(com.analyzer.storage.ArtifactKey documentKey, ObjectMapper objectMapper,
                                   JarDataPaths jarDataPaths, String jarName, String subType) {
        super(documentKey, objectMapper, jarDataPaths, jarName);
        this.subType = subType;
    }

    @Override
    public String getProviderType() { return subType; }


    @Override
    public com.analyzer.storage.ArtifactKey getSummaryCacheKey() {
        return jarDataPaths.summaryCacheKey(jarName);
    }

    @Override
    public boolean shouldComputeVerification() { return true; }
}
