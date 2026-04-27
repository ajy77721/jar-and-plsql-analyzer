package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

public interface AnalysisDataProvider {

    Path getFilePath();

    String getProviderType();

    Path getSummaryCachePath();

    boolean shouldComputeVerification();

    void streamSummary(JsonParser parser, JsonGenerator gen) throws IOException;

    void streamAnalysis(JsonParser parser, JsonGenerator gen) throws IOException;

    void streamCallTree(JsonParser parser, JsonGenerator gen, int endpointIdx) throws IOException;

    void streamClassTree(JsonParser parser, JsonGenerator gen) throws IOException;

    void streamClassByIndex(JsonParser parser, JsonGenerator gen, int idx) throws IOException;

    /**
     * Stream a filtered slice of the summary: either headers-only (excluding heavy fields)
     * or a specific field extracted from endpoints.
     *
     * @param summaryParser parser positioned at the cached summary JSON
     * @param gen output generator
     * @param mode "headers" = full summary minus heavy endpoint fields;
     *             "slice" = extract only the specified fields from endpoints
     * @param fields for "slice" mode: the endpoint field names to extract (e.g., "externalCalls", "dynamicFlows")
     */
    void streamSummarySlice(JsonParser summaryParser, JsonGenerator gen,
                            String mode, Set<String> fields) throws IOException;
}
