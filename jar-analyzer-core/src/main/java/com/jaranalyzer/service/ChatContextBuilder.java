package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The chat context, built by streaming {@code analysis.json} rather than loading it — MID-101/§30.
 *
 * <h2>The ratio</h2>
 *
 * <p>{@code ChatboxService.chat()} called {@code persistenceService.load(jarId)} — the whole
 * {@code JarAnalysis} object graph, up to the 912 MB measured elsewhere in this registry for the
 * largest analysis — to build a context capped at {@code MAX_CONTEXT_CHARS} (40,000 characters).
 * Unlike the eight callers audited in §16, this one runs synchronously on every chat message: a
 * background job paying that cost once is a very different thing from a user waiting on it per
 * turn.
 *
 * <h2>What is read, following exactly what the original context builder read</h2>
 *
 * <p>Three things, none of which need the {@code classes} or {@code callTreeNodes} arrays that
 * make up most of the file:
 *
 * <ul>
 *   <li>The header ({@code jarName}, {@code projectName}, ...) — from the header sidecar, via
 *       {@link PersistenceService#readHeader}, not from {@code analysis.json} at all.</li>
 *   <li>The first {@code maxEndpoints} endpoints, in full, including their call tree walked to the
 *       same depth the original {@code appendTreeEnrichment} used.</li>
 *   <li>Every endpoint's {@code aggregatedCollections} — all of them, not just the first
 *       {@code maxEndpoints} — for the "MongoDB collections" summary, which the original computed
 *       over the whole endpoint list rather than the truncated one.</li>
 * </ul>
 *
 * <p>One streaming pass over the {@code endpoints} array serves both needs: each endpoint is
 * parsed into a {@link JsonNode} one at a time via {@link ObjectMapper#readTree}, which is what
 * keeps peak heap at one endpoint rather than the whole array — the same technique
 * {@code CollectionReportAggregator} already uses for the same reason.
 */
final class ChatContextBuilder {

    private ChatContextBuilder() {}

    static String build(JsonParser parser, ObjectMapper mapper, Map<String, Object> header,
                        int maxEndpoints, int maxContextChars, int maxTreeDepth)
            throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("=== JAR ANALYSIS CONTEXT ===\n");
        sb.append("JAR: ").append(header.getOrDefault("jarName", "")).append("\n");
        Object project = header.get("projectName");
        if (project != null) {
            sb.append("Project: ").append(project).append("\n");
        }
        sb.append("Analyzed: ").append(header.getOrDefault("analyzedAt", "")).append("\n");
        sb.append("Mode: ").append(header.getOrDefault("analysisMode", "")).append("\n");
        sb.append("Total classes: ").append(header.getOrDefault("totalClasses", 0)).append("\n");
        sb.append("Total endpoints: ").append(header.getOrDefault("totalEndpoints", 0)).append("\n\n");

        StringBuilder endpointsSection = new StringBuilder();
        Set<String> allCollections = new LinkedHashSet<>();
        int total = 0;

        if (parser.nextToken() == JsonToken.START_OBJECT) {
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String field = parser.currentName();
                parser.nextToken();
                if (!"endpoints".equals(field)) {
                    // classes and callTreeNodes — the bulk of the file — are never even parsed
                    // into a JsonNode, let alone materialised into JarAnalysis's object graph.
                    parser.skipChildren();
                    continue;
                }
                if (parser.currentToken() != JsonToken.START_ARRAY) {
                    parser.skipChildren();
                    continue;
                }
                while (parser.nextToken() == JsonToken.START_OBJECT) {
                    JsonNode ep = mapper.readTree(parser);
                    for (JsonNode c : ep.path("aggregatedCollections")) {
                        String name = c.asText(null);
                        if (name != null && !name.isEmpty()) allCollections.add(name);
                    }
                    if (total < maxEndpoints) {
                        appendEndpoint(endpointsSection, ep, maxTreeDepth);
                    }
                    total++;
                }
            }
        }

        if (total == 0) {
            sb.append("No endpoints found.\n");
        } else {
            sb.append("=== ENDPOINTS (").append(total).append(") ===\n");
            sb.append(endpointsSection);
            int shown = Math.min(total, maxEndpoints);
            if (total > shown) {
                sb.append("... and ").append(total - shown)
                        .append(" more endpoints (not shown to stay within context limit)\n");
            }
        }

        if (!allCollections.isEmpty()) {
            sb.append("\n=== MONGODB COLLECTIONS (").append(allCollections.size()).append(") ===\n");
            sb.append(String.join(", ", allCollections)).append("\n");
        }

        String result = sb.toString();
        if (result.length() > maxContextChars) {
            result = result.substring(0, maxContextChars)
                    + "\n\n[Context truncated at " + maxContextChars + " chars]\n";
        }
        return result;
    }

    private static void appendEndpoint(StringBuilder sb, JsonNode ep, int maxTreeDepth) {
        sb.append("\n--- ").append(ep.path("httpMethod").asText(""))
                .append(" ").append(ep.path("fullPath").asText("")).append(" ---\n");
        sb.append("Controller: ").append(ep.path("controllerSimpleName").asText(""))
                .append(".").append(ep.path("methodName").asText("")).append("\n");

        JsonNode collections = ep.path("aggregatedCollections");
        if (collections.isArray() && !collections.isEmpty()) {
            StringBuilder names = new StringBuilder();
            for (JsonNode c : collections) {
                if (names.length() > 0) names.append(", ");
                names.append(c.asText(""));
            }
            sb.append("Collections: ").append(names).append("\n");
        }
        String opType = ep.path("derivedOperationType").asText(null);
        if (opType != null) {
            sb.append("Operation: ").append(opType).append("\n");
        }

        JsonNode root = ep.path("callTree");
        if (root.isObject()) {
            String domain = root.path("domain").asText(null);
            String module = root.path("module").asText(null);
            if (domain != null) sb.append("Domain: ").append(domain).append("\n");
            if (module != null) sb.append("Module: ").append(module).append("\n");
            appendTreeEnrichment(sb, root, 0, maxTreeDepth);
        }
    }

    private static void appendTreeEnrichment(StringBuilder sb, JsonNode node, int depth,
                                              int maxTreeDepth) {
        if (node == null || node.isMissingNode() || depth > maxTreeDepth) return;

        String indent = "  ".repeat(depth);
        String simpleClassName = node.path("simpleClassName").asText(null);
        String className = node.path("className").asText(null);
        sb.append(indent).append(simpleClassName != null ? simpleClassName : className)
                .append(".").append(node.path("methodName").asText(""));

        String stereotype = node.path("stereotype").asText(null);
        if (stereotype != null) sb.append(" [").append(stereotype).append("]");

        JsonNode accessed = node.path("collectionsAccessed");
        if (accessed.isArray() && !accessed.isEmpty()) {
            StringBuilder names = new StringBuilder();
            for (JsonNode c : accessed) {
                if (names.length() > 0) names.append(", ");
                names.append(c.asText(""));
            }
            sb.append(" -> ").append(names);
            String opType = node.path("operationType").asText(null);
            if (opType != null) sb.append(" (").append(opType).append(")");
        }
        sb.append("\n");

        for (JsonNode child : node.path("children")) {
            appendTreeEnrichment(sb, child, depth + 1, maxTreeDepth);
        }
    }
}
