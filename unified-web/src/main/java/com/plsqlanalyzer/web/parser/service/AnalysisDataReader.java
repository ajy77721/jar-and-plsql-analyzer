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

    /** MID-002: {@link #getIndex} without its {@code nodes} array — see {@code IndexFacetReader}. */
    JsonNode getIndexWithoutNodes(String name) throws IOException;

    JsonNode getNodeDetail(String name, String fileName) throws IOException;

    JsonNode getTables(String name) throws IOException;

    JsonNode getCallGraph(String name) throws IOException;

    JsonNode getProcedures(String name) throws IOException;

    JsonNode getJoins(String name) throws IOException;

    JsonNode getCursors(String name) throws IOException;

    JsonNode getSequences(String name) throws IOException;

    /** Aggregated dynamic-SQL executions across all procedures. Default: empty. */
    default JsonNode getDynamicSql(String name) throws IOException {
        return com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.arrayNode();
    }

    JsonNode getCallTree(String name, String nodeId) throws IOException;

    /**
     * UI-019 (§142) — {@code maxDepth} fetches only that many levels rooted at {@code nodeId},
     * marking the cut points {@code moreChildren} so the client can ask again for the next slice.
     * Defaults to the unbounded behaviour above so an implementor that hasn't been taught this yet
     * still works, exactly as {@link #getDynamicSql}'s own default above does for a different gap.
     */
    default JsonNode getCallTree(String name, String nodeId, Integer maxDepth) throws IOException {
        return getCallTree(name, nodeId);
    }

    JsonNode getCallers(String name, String nodeId) throws IOException;

    String getSource(String name, String fileName) throws IOException;

    JsonNode getResolver(String name, String type) throws IOException;
}
