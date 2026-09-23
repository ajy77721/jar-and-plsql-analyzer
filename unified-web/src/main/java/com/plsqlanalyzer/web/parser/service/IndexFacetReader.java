package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.analyzer.storage.ArtifactKey;

import java.io.IOException;
import java.io.InputStream;

/** Index-facet reads: getIndex + getProcedures + getNodeDetail. */
class IndexFacetReader {

    private final StaticAnalysisReader r;

    IndexFacetReader(StaticAnalysisReader r) {
        this.r = r;
    }

    /** Streams a JSON document out of the store. */
    private JsonNode readTree(ArtifactKey key) throws IOException {
        try (InputStream in = r.openRead(key)) {
            return r.mapper.readTree(in);
        }
    }

    JsonNode getIndex(String name) throws IOException {
        return readTree(r.resolveKey(name, "api/index.json"));
    }

    JsonNode getNodeDetail(String name, String fileName) throws IOException {
        // fileName comes from a REST path variable and becomes a key segment: guard traversal
        // explicitly, since there is no base path left to compare against.
        if (fileName == null || fileName.isBlank() || fileName.contains("..")
                || fileName.contains("/") || fileName.contains("\\")) {
            throw new IOException("Invalid node file name: " + fileName);
        }
        ArtifactKey file = r.resolveKey(name, "api/nodes/" + fileName);
        if (r.store().exists(file)) return readTree(file);
        throw new IOException("Node file not found: " + file);
    }

    /**
     * MID-002 — {@code /index} without its {@code nodes} array, which measured at 53.1% of the
     * response on this project's own largest analysis. {@code /procedures} already serves that same
     * array (paged, searchable via {@code q}), so a client fetches it there instead of on every
     * analysis load whether or not the sidebar is ever opened.
     *
     * <p>A field-reference copy, not {@code deepCopy()}: {@code deepCopy()} would clone the array
     * being dropped before discarding it, which is the exact eager-materialisation shape this row
     * exists to remove. Every other field on this document is a scalar or a small nested object, so
     * sharing references for them costs nothing {@link #getIndex} isn't already paying.
     */
    JsonNode getIndexWithoutNodes(String name) throws IOException {
        JsonNode full = r.getIndex(name);
        if (full == null || !full.has("nodes")) return full;
        ObjectMapper mapper = r.mapper;
        ObjectNode copy = mapper.createObjectNode();
        full.fields().forEachRemaining(e -> {
            if (!"nodes".equals(e.getKey())) copy.set(e.getKey(), e.getValue());
        });
        return copy;
    }

    JsonNode getProcedures(String name) throws IOException {
        ObjectMapper mapper = r.mapper;
        JsonNode index = r.getIndex(name);
        JsonNode nodes = index.get("nodes");
        if (nodes == null || !nodes.isArray()) return mapper.createArrayNode();
        return nodes;
    }
}
