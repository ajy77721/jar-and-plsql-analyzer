package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/**
 * Strategy interface for reading analysis data.
 * Implementations resolve to different data sources (static flow-output vs Claude-enriched).
 * All read methods use the analysis name to locate data.
 */
public interface AnalysisDataReader {

    JsonNode getIndex(String name) throws IOException;

    JsonNode getNodeDetail(String name, String fileName) throws IOException;

    JsonNode getTables(String name) throws IOException;

    JsonNode getCallGraph(String name) throws IOException;

    JsonNode getProcedures(String name) throws IOException;

    JsonNode getJoins(String name) throws IOException;

    JsonNode getCursors(String name) throws IOException;

    JsonNode getSequences(String name) throws IOException;

    JsonNode getCallTree(String name, String nodeId) throws IOException;

    JsonNode getCallers(String name, String nodeId) throws IOException;

    String getSource(String name, String fileName) throws IOException;

    JsonNode getResolver(String name, String type) throws IOException;
}
