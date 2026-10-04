package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.analyzer.storage.ArtifactKey;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

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
        // Truthful per-type counts for the header/topbar. New crawls already carry typeCounts on
        // the index root (ChunkedFlowWriter); analyses crawled before that field existed do not, so
        // compute the breakdown here from the nodes array this method already loaded. Additive:
        // every existing field is untouched.
        if (!copy.has("typeCounts") && full.path("nodes").isArray()) {
            copy.set("typeCounts", nodeTypeCounts(mapper, full.get("nodes")));
        }
        return copy;
    }

    /**
     * {@code /procedures}' rows: <b>only</b> routines — {@code PROCEDURE} and {@code FUNCTION}.
     *
     * <p>The crawler's {@code index.nodes[]} is every node in the analysis, so it also carries
     * {@code TRIGGER}, {@code TABLE}, {@code EVENT}, package and job nodes. Those belong to their
     * own facets (triggers → {@code /triggers}, tables → {@code /tables}), and lumping them into
     * the procedures list made "Procs" count every node type. A row with no {@code objectType} is
     * treated as a routine: the crawler's own {@code typeCounts} defaults a missing type to
     * {@code PROCEDURE}, and a handful of pre-{@code objectType} fixtures rely on the same default.
     */
    JsonNode getProcedures(String name) throws IOException {
        ObjectMapper mapper = r.mapper;
        JsonNode index = r.getIndex(name);
        JsonNode nodes = index.get("nodes");
        if (nodes == null || !nodes.isArray()) return mapper.createArrayNode();
        return proceduresOnly(mapper, nodes);
    }

    /** True when a node is a routine the procedures facet owns. */
    static boolean isRoutine(JsonNode node) {
        String ot = node.path("objectType").asText("").trim().toUpperCase(Locale.ROOT);
        return ot.isEmpty() || "PROCEDURE".equals(ot) || "FUNCTION".equals(ot);
    }

    /** Keeps only {@link #isRoutine} rows, preserving order. */
    static ArrayNode proceduresOnly(ObjectMapper mapper, JsonNode nodes) {
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode n : nodes) {
            if (isRoutine(n)) out.add(n);
        }
        return out;
    }

    /** {@code {OBJECT_TYPE -> count}} over a nodes array, upper-cased, blank defaulting to PROCEDURE. */
    static ObjectNode nodeTypeCounts(ObjectMapper mapper, JsonNode nodes) {
        Map<String, Integer> counts = new TreeMap<>();
        for (JsonNode n : nodes) {
            String t = n.path("objectType").asText("").trim().toUpperCase(Locale.ROOT);
            if (t.isEmpty()) t = "PROCEDURE";
            counts.merge(t, 1, Integer::sum);
        }
        ObjectNode out = mapper.createObjectNode();
        counts.forEach(out::put);
        return out;
    }
}
