package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Centralized Claude AI prompt templates for JAR + PL/SQL analysis.
 * All prompt text lives in resources/prompts/*.txt files with {{PLACEHOLDER}} tokens.
 * This class loads, caches, and fills those templates at runtime.
 */
public final class PromptTemplates {

    private static final Logger log = LoggerFactory.getLogger(PromptTemplates.class);

    private PromptTemplates() {}

    public enum DbTechnology { MONGO_ONLY, ORACLE_ONLY, BOTH, UNKNOWN }

    private static final Map<String, String> templateCache = new ConcurrentHashMap<>();
    private static volatile Path externalConfigDir;

    private static final Set<String> MONGO_SOURCES = Set.of(
            "DOCUMENT_ANNOTATION", "REPOSITORY_MAPPING", "STRING_LITERAL",
            "FIELD_CONSTANT", "VALUE_PROPERTY", "TEMPLATE_QUERY", "PIPELINE_ANNOTATION",
            "QUERY_ANNOTATION", "CONTEXT_BASED", "REACTIVE_MONGO"
    );

    private static final Set<String> JPA_SOURCES = Set.of(
            "JPA_TABLE_ANNOTATION", "JPA_REPOSITORY_MAPPING", "JDBC_TEMPLATE",
            "ENTITY_MANAGER", "JPA_NATIVE_QUERY", "NAMED_QUERY", "JPQL_QUERY",
            "JPA_REPOSITORY", "DERIVED_QUERY", "JPA_PROCEDURE_ANNOTATION",
            "STORED_PROCEDURE", "CALLABLE_STATEMENT", "HIBERNATE_SQL_ANNOTATION"
    );

    // ======================== TEMPLATE LOADING ========================

    public static void setExternalConfigDir(Path dir) {
        externalConfigDir = dir;
        templateCache.clear();
    }

    /**
     * Load a template — tries external config dir first, then classpath.
     * Cached after first load.
     */
    public static String loadTemplate(String name) {
        return templateCache.computeIfAbsent(name, k -> {
            // 1. Try external config dir
            if (externalConfigDir != null) {
                Path extFile = externalConfigDir.resolve("prompts").resolve(k + ".txt");
                if (Files.exists(extFile)) {
                    try {
                        String content = Files.readString(extFile, StandardCharsets.UTF_8);
                        log.debug("Loaded prompt template from {}", extFile);
                        return content;
                    } catch (Exception e) {
                        log.warn("Failed to read external template {}: {}", extFile, e.getMessage());
                    }
                }
            }
            // 2. Fall back to classpath
            String path = "prompts/" + k + ".txt";
            try (InputStream is = PromptTemplates.class.getClassLoader().getResourceAsStream(path)) {
                if (is == null) {
                    log.warn("Prompt template not found: {}", path);
                    return "";
                }
                return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                        .lines().collect(Collectors.joining("\n"));
            } catch (Exception e) {
                log.error("Failed to load prompt template: {}", path, e);
                return "";
            }
        });
    }

    /**
     * Fill {{PLACEHOLDER}} tokens in a template string.
     */
    public static String fill(String template, Map<String, String> values) {
        String result = template;
        for (var entry : values.entrySet()) {
            result = result.replace("{{" + entry.getKey() + "}}", entry.getValue() != null ? entry.getValue() : "");
        }
        return result;
    }

    // ======================== TECHNOLOGY DETECTION ========================

    public static DbTechnology detectTechnology(EndpointInfo ep) {
        if (ep.getCallTree() == null) return DbTechnology.UNKNOWN;
        boolean[] flags = {false, false};
        detectTechWalk(ep.getCallTree(), flags);
        if (flags[0] && flags[1]) return DbTechnology.BOTH;
        if (flags[0]) return DbTechnology.MONGO_ONLY;
        if (flags[1]) return DbTechnology.ORACLE_ONLY;
        return DbTechnology.UNKNOWN;
    }

    public static DbTechnology detectTechnology(List<EndpointInfo> endpoints) {
        boolean hasMongo = false, hasJpa = false;
        for (EndpointInfo ep : endpoints) {
            DbTechnology t = detectTechnology(ep);
            if (t == DbTechnology.BOTH) return DbTechnology.BOTH;
            if (t == DbTechnology.MONGO_ONLY) hasMongo = true;
            if (t == DbTechnology.ORACLE_ONLY) hasJpa = true;
        }
        if (hasMongo && hasJpa) return DbTechnology.BOTH;
        if (hasMongo) return DbTechnology.MONGO_ONLY;
        if (hasJpa) return DbTechnology.ORACLE_ONLY;
        return DbTechnology.UNKNOWN;
    }

    private static void detectTechWalk(CallNode node, boolean[] flags) {
        if (node == null) return;
        Map<String, String> sources = node.getCollectionSources();
        if (sources != null) {
            for (String source : sources.values()) {
                if (MONGO_SOURCES.contains(source)) flags[0] = true;
                if (JPA_SOURCES.contains(source)) flags[1] = true;
            }
        }
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                detectTechWalk(child, flags);
            }
        }
    }

    // ======================== TEMPLATE NAME RESOLUTION ========================

    private static String techPrefix(DbTechnology tech) {
        return switch (tech) {
            case MONGO_ONLY -> "java-mongo";
            case ORACLE_ONLY -> "java-oracle";
            case BOTH -> "java-both";
            case UNKNOWN -> "java-mongo";
        };
    }

    // ======================== ANALYSIS PROMPTS ========================

    public static String buildAnalysisPrompt(String clusterContext, String treeContext, DbTechnology tech) {
        String template = loadTemplate(techPrefix(tech) + "-analysis");
        return fill(template, Map.of(
                "CLUSTER_CONTEXT", clusterContext,
                "TREE_CONTEXT", treeContext
        ));
    }

    public static String buildChunkAnalysisPrompt(EndpointInfo ep, String chunkContext,
            String chunkType, String chunkLabel,
            int chunkIndex, int totalChunks,
            String clusterContext, DbTechnology tech) {
        String template = loadTemplate(techPrefix(tech) + "-chunk-analysis");

        String chunkDesc;
        if ("skeleton".equals(chunkType)) {
            chunkDesc = "This is the SKELETON -- structural overview of the entire flow. " +
                    "Shows root controller + all major branch stubs. " +
                    "Identify overall flow pattern, domains involved, risk areas from the actual code.";
        } else if (chunkType != null && chunkType.contains("stub")) {
            chunkDesc = "This is a BRANCH STUB -- overview of a large branch. " +
                    "Sub-branches follow in subsequent chunks.";
        } else {
            chunkDesc = "This is a BRANCH -- a subtree with actual decompiled source at each level. " +
                    "Read the sourceCode fields to understand actual logic, conditions, and operations.";
        }

        return fill(template, Map.of(
                "TOTAL_CHUNKS", String.valueOf(totalChunks),
                "CHUNK_INDEX", String.valueOf(chunkIndex + 1),
                "CHUNK_TYPE_DESCRIPTION", chunkDesc,
                "CLUSTER_CONTEXT", clusterContext,
                "CHUNK_DATA", chunkContext
        ));
    }

    // ======================== CORRECTION PROMPTS ========================

    public static String buildCorrectionPrompt(EndpointInfo ep, String callTreeJson, DbTechnology tech) {
        String template = loadTemplate(techPrefix(tech) + "-correction");
        return fill(template, Map.of(
                "HTTP_METHOD", ep.getHttpMethod() != null ? ep.getHttpMethod() : "",
                "FULL_PATH", ep.getFullPath() != null ? ep.getFullPath() : "",
                "CONTROLLER_CLASS", ep.getControllerClass() != null ? ep.getControllerClass() : "",
                "METHOD_NAME", ep.getMethodName() != null ? ep.getMethodName() : "",
                "CHUNK_CONTEXT", "",
                "CALL_TREE_JSON", callTreeJson
        ));
    }

    public static String buildChunkedCorrectionPrompt(EndpointInfo ep, String chunkData,
            String chunkType, String chunkLabel, int nodeCount,
            int chunkIndex, int totalChunks, DbTechnology tech) {
        String chunkContext = "The call tree is too large for a single prompt, so it has been split into " +
                totalChunks + " chunks. This is chunk " + (chunkIndex + 1) + "/" + totalChunks + ".\n" +
                "Chunk type: " + chunkType + " — " + chunkLabel + "\n" +
                "Nodes in this chunk: " + nodeCount + "\n\n";

        if ("skeleton".equals(chunkType)) {
            chunkContext += "This is the SKELETON chunk — structural overview showing the root controller and all major branches.\n" +
                    "Verify the root node's collections/operations. Branch details follow in other chunks.\n" +
                    "For the endpointSummary, list only collections you can see in THIS chunk.\n";
        } else {
            chunkContext += "This is a BRANCH chunk — a subtree with decompiled source at each node.\n" +
                    "Verify collections and operations for each node in this branch.\n";
        }

        String template = loadTemplate(techPrefix(tech) + "-correction");
        return fill(template, Map.of(
                "HTTP_METHOD", ep.getHttpMethod() != null ? ep.getHttpMethod() : "",
                "FULL_PATH", ep.getFullPath() != null ? ep.getFullPath() : "",
                "CONTROLLER_CLASS", ep.getControllerClass() != null ? ep.getControllerClass() : "",
                "METHOD_NAME", ep.getMethodName() != null ? ep.getMethodName() : "",
                "CHUNK_CONTEXT", chunkContext,
                "CALL_TREE_JSON", chunkData
        ));
    }

    // ======================== PL/SQL VERIFICATION ========================

    public static String buildPlsqlVerificationPrompt(String staticFindings, String sourceCode,
            String sourceWindowNote) {
        String template = loadTemplate("plsql-verification");
        return fill(template, Map.of(
                "STATIC_ANALYSIS_FINDINGS", staticFindings,
                "SOURCE_WINDOW_NOTE", sourceWindowNote != null ? sourceWindowNote : "",
                "SOURCE_CODE", sourceCode
        ));
    }

    // ======================== COMPLEXITY RULES ========================

    public static Map<String, Object> buildComplexityRules(DbTechnology tech) {
        List<String> factors = new ArrayList<>();
        String entityName = switch (tech) {
            case MONGO_ONLY -> "collections";
            case ORACLE_ONLY -> "tables/procedures";
            case BOTH -> "collections and tables/procedures";
            case UNKNOWN -> "collections/tables";
        };
        factors.add("Number of endpoints accessing the " + entityName + " (weight: 1.0)");

        if (tech == DbTechnology.MONGO_ONLY || tech == DbTechnology.BOTH) {
            factors.add("Read operations: READ, COUNT, AGGREGATE (weight: 0.5 each)");
            factors.add("Write operations: WRITE, UPDATE, DELETE (weight: 1.5 each)");
            factors.add("Aggregation pipeline usage (weight: 2.0)");
            factors.add("Pipeline complexity — $lookup, $unwind, mapReduce (weight: 2.0)");
        }
        if (tech == DbTechnology.ORACLE_ONLY || tech == DbTechnology.BOTH) {
            factors.add("Read operations: READ/SELECT, COUNT (weight: 0.5 each)");
            factors.add("Write operations: WRITE/INSERT, UPDATE, DELETE, CALL (weight: 1.5 each)");
            factors.add("Stored procedure/function CALL operations (weight: 2.0)");
            factors.add("Complex SQL: JOINs, subqueries, CTEs (weight: 1.5)");
        }
        factors.add("Cross-domain access — " + entityName + " used by multiple business domains (weight: 3.0 per extra domain)");
        factors.add("Total usage count across endpoints (weight: 0.3 per reference)");

        Map<String, Object> opGroups = new LinkedHashMap<>();
        if (tech == DbTechnology.MONGO_ONLY || tech == DbTechnology.BOTH) {
            opGroups.put("read", List.of("READ", "COUNT", "AGGREGATE"));
            opGroups.put("write", List.of("WRITE", "UPDATE", "DELETE"));
        }
        if (tech == DbTechnology.ORACLE_ONLY || tech == DbTechnology.BOTH) {
            opGroups.put("read", List.of("READ", "COUNT"));
            opGroups.put("write", List.of("WRITE", "UPDATE", "DELETE", "CALL"));
        }

        return Map.of(
                "description", "When assessing " + entityName + " complexity, consider these weighted factors",
                "factors", factors,
                "thresholds", Map.of("Low", "score <= 4", "Medium", "4 < score <= 10", "High", "score > 10"),
                "operationGroups", opGroups
        );
    }
}
