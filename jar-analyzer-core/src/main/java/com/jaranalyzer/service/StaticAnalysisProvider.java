package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;

public class StaticAnalysisProvider extends AbstractAnalysisDataProvider {

    public StaticAnalysisProvider(Path filePath, ObjectMapper objectMapper,
                                   JarDataPaths jarDataPaths, String jarName) {
        super(filePath, objectMapper, jarDataPaths, jarName);
    }

    @Override
    public String getProviderType() { return "static"; }

    @Override
    public Path getSummaryCachePath() {
        return jarDataPaths.summaryCacheStatic(jarName);
    }

    @Override
    public boolean shouldComputeVerification() { return false; }
}
