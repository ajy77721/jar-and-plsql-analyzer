package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StaticAnalysisProvider extends AbstractAnalysisDataProvider {

    public StaticAnalysisProvider(com.analyzer.storage.ArtifactKey documentKey, ObjectMapper objectMapper,
                                   JarDataPaths jarDataPaths, String jarName) {
        super(documentKey, objectMapper, jarDataPaths, jarName);
    }

    @Override
    public String getProviderType() { return "static"; }


    @Override
    public com.analyzer.storage.ArtifactKey getSummaryCacheKey() {
        return jarDataPaths.summaryCacheStaticKey(jarName);
    }

    @Override
    public boolean shouldComputeVerification() { return false; }
}
