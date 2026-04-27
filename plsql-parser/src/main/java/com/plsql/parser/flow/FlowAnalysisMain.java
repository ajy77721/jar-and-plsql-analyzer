package com.plsql.parser.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsql.parser.PlSqlParserEngine;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;

import java.nio.file.*;
import java.util.*;

/**
 * CLI entry point for flow analysis mode.
 * Orchestrates DB connection, schema resolution, source downloading,
 * and recursive dependency crawling.
 *
 * Usage:
 *   java -jar plsql-parser.jar --flow --config path/to/config.yaml --entry "PKG.PROC" [--entry "FUNC"] [--output-dir dir] [--max-depth N] [--pretty]
 */
public class FlowAnalysisMain {

    /**
     * Run the flow analysis with the given arguments.
     *
     * @param args remaining arguments after --flow has been consumed
     */
    public static void run(String[] args) {
        String configPath = null;
        List<String> entryPoints = new ArrayList<>();
        String outputDir = null;
        int maxDepth = -1;
        boolean pretty = false;
        boolean clearCache = false;

        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config":
                    if (i + 1 < args.length) {
                        configPath = args[++i];
                    } else {
                        System.err.println("Error: --config requires a path argument");
                        printUsage();
                        System.exit(1);
                    }
                    break;
                case "--entry":
                    if (i + 1 < args.length) {
                        entryPoints.add(args[++i]);
                    } else {
                        System.err.println("Error: --entry requires an entry point argument");
                        printUsage();
                        System.exit(1);
                    }
                    break;
                case "--output-dir":
                    if (i + 1 < args.length) {
                        outputDir = args[++i];
                    } else {
                        System.err.println("Error: --output-dir requires a path argument");
                        printUsage();
                        System.exit(1);
                    }
                    break;
                case "--max-depth":
                    if (i + 1 < args.length) {
                        try {
                            maxDepth = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Error: --max-depth requires a numeric argument");
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Error: --max-depth requires a numeric argument");
                        printUsage();
                        System.exit(1);
                    }
                    break;
                case "--pretty":
                    pretty = true;
                    break;
                case "--flow":
                    // Skip if passed through
                    break;
                case "--clear-cache":
                    clearCache = true;
                    break;
                default:
                    if (args[i].startsWith("-")) {
                        System.err.println("Unknown flow option: " + args[i]);
                        printUsage();
                        System.exit(1);
                    }
                    break;
            }
        }

        // Validate
        if (configPath == null) {
            System.err.println("Error: --config is required for flow analysis mode");
            printUsage();
            System.exit(1);
        }
        if (entryPoints.isEmpty()) {
            System.err.println("Error: at least one --entry is required");
            printUsage();
            System.exit(1);
        }

        // Run the analysis
        long totalStart = System.currentTimeMillis();

        try (DbConnectionManager connManager = new DbConnectionManager(configPath)) {
            System.err.println("=== PL/SQL Flow Analysis ===");
            System.err.println("Config: " + configPath);
            System.err.println("Schemas: " + connManager.getAvailableSchemas());
            System.err.println("Entry points: " + entryPoints);
            System.err.println("Max depth: " + (maxDepth < 0 ? "UNLIMITED" : maxDepth));
            System.err.println();

            // Clear disk cache if requested
            if (clearCache) {
                Path cacheDir = Paths.get("cache", "schema-resolver");
                if (Files.isDirectory(cacheDir)) {
                    try (var stream = Files.list(cacheDir)) {
                        stream.forEach(f -> { try { Files.delete(f); } catch (Exception ignored) {} });
                    }
                    System.err.println("Cleared schema resolver cache");
                }
            }

            // Initialize schema resolver (filtered by entry points)
            System.err.println("Initializing schema resolver for: " + entryPoints);
            SchemaResolver schemaResolver = new SchemaResolver(connManager, entryPoints);

            // Initialize source downloader
            SourceDownloader downloader = new SourceDownloader(connManager);

            // Initialize parser engine
            PlSqlParserEngine engine = new PlSqlParserEngine();

            // Initialize crawler
            DependencyCrawler crawler = new DependencyCrawler(engine, downloader, schemaResolver);
            crawler.setMaxDepth(maxDepth);
            if (outputDir != null) {
                crawler.setOutputDir(Paths.get(outputDir));
            }

            // Prepare output
            ObjectMapper mapper = new ObjectMapper();
            if (pretty) {
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
            }

            // Create output directory if specified
            if (outputDir != null) {
                Path outPath = Paths.get(outputDir);
                if (!Files.exists(outPath)) {
                    Files.createDirectories(outPath);
                    System.err.println("Created output directory: " + outputDir);
                }
            }

            // Process each entry point
            List<FlowResult> allResults = new ArrayList<>();
            for (String entry : entryPoints) {
                System.err.println();
                System.err.println("========================================");
                System.err.println("Crawling entry point: " + entry);
                System.err.println("========================================");

                FlowResult result = crawler.crawl(entry);
                allResults.add(result);

                System.err.println();
                System.err.println("Completed: " + entry);
                System.err.println("  Objects crawled: " + result.getTotalObjectsCrawled());
                System.err.println("  Max depth reached: " + result.getMaxDepthReached());
                System.err.println("  Tables found: " + result.getAllTables().size());
                System.err.println("  Call graph edges: " + result.getCallGraph().size());
                System.err.println("  Errors: " + result.getErrors().size());
                System.err.println("  Time: " + result.getCrawlTimeMs() + "ms");

                // Output
                if (outputDir != null) {
                    // Chunked writer already wrote flow.json, chunks/, sources/, progress.json
                    String safeName = entry.toUpperCase().replace(".", "_")
                            .replace("/", "_").replace("\\", "_");
                    Path buildDir = Paths.get(outputDir, safeName);
                    System.err.println("  Output: " + buildDir.toAbsolutePath());
                } else {
                    String json = mapper.writeValueAsString(result);
                    System.out.println(json);
                }
            }

            long totalTime = System.currentTimeMillis() - totalStart;
            System.err.println();
            System.err.println("=== Flow Analysis Complete ===");
            System.err.println("Total entry points: " + entryPoints.size());
            System.err.println("Total time: " + totalTime + "ms");

        } catch (Exception e) {
            System.err.println("Fatal error in flow analysis: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * Write source files into the build/sources folder.
     * For standalone proc/func: full source.
     * For package subprograms: extract only the required proc/func from the package body.
     */
    private static void writeBuildSources(FlowResult result, SourceDownloader downloader,
                                           Path srcDir, ObjectMapper mapper) {
        Set<String> writtenPackages = new HashSet<>();
        List<Map<String, Object>> manifest = new ArrayList<>();

        for (FlowNode node : result.getFlowNodes()) {
            String schema = node.getSchema() != null ? node.getSchema() : "UNKNOWN";
            String objName = node.getObjectName() != null ? node.getObjectName() : "UNKNOWN";
            String pkgName = node.getPackageName();

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("nodeId", node.getNodeId());
            entry.put("schema", schema);
            entry.put("objectName", objName);
            entry.put("objectType", node.getObjectType());
            entry.put("lineStart", node.getLineStart());
            entry.put("lineEnd", node.getLineEnd());
            entry.put("linesOfCode", node.getLinesOfCode());
            entry.put("callCount", node.getCallCount());

            try {
                if (pkgName != null && !pkgName.isEmpty()) {
                    // Package subprogram — extract just this proc/func
                    String source = downloader.getCachedSource(pkgName);
                    if (source != null) {
                        String fileName = schema + "." + pkgName + "." + objName + ".sql";
                        entry.put("sourceFile", fileName);

                        if (node.getLineStart() > 0 && node.getLineEnd() > 0) {
                            String[] lines = source.split("\\r?\\n");
                            int start = Math.max(0, node.getLineStart() - 1);
                            int end = Math.min(lines.length, node.getLineEnd());
                            StringBuilder extracted = new StringBuilder();
                            extracted.append("-- Extracted from " + schema + "." + pkgName + "\n");
                            extracted.append("-- Lines " + node.getLineStart() + " to " + node.getLineEnd() + "\n\n");
                            for (int i = start; i < end; i++) {
                                extracted.append(lines[i]).append("\n");
                            }
                            Files.writeString(srcDir.resolve(fileName), extracted.toString());
                        }

                        // Also write full package once
                        if (!writtenPackages.contains(pkgName.toUpperCase())) {
                            writtenPackages.add(pkgName.toUpperCase());
                            String fullFileName = schema + "." + pkgName + ".pkb";
                            Files.writeString(srcDir.resolve(fullFileName), source);
                        }
                    }
                } else {
                    // Standalone proc/func
                    String source = downloader.getCachedSource(objName);
                    if (source != null) {
                        String ext = "sql";
                        String type = downloader.getCachedType(objName);
                        if ("FUNCTION".equals(type)) ext = "fnc";
                        else if ("PROCEDURE".equals(type)) ext = "prc";
                        String fileName = schema + "." + objName + "." + ext;
                        entry.put("sourceFile", fileName);
                        Files.writeString(srcDir.resolve(fileName), source);
                    }
                }
            } catch (Exception e) {
                System.err.println("  [Build] Error writing source for " + objName + ": " + e.getMessage());
            }

            manifest.add(entry);
        }

        // Write manifest
        try {
            String manifestJson = mapper.writeValueAsString(manifest);
            Files.writeString(srcDir.getParent().resolve("manifest.json"), manifestJson);
        } catch (Exception e) {
            System.err.println("  [Build] Error writing manifest: " + e.getMessage());
        }
    }

    private static void printUsage() {
        System.err.println();
        System.err.println("PL/SQL Flow Analysis - Recursive dependency analysis");
        System.err.println();
        System.err.println("Usage:");
        System.err.println("  java -jar plsql-parser.jar --flow [options]");
        System.err.println();
        System.err.println("Required options:");
        System.err.println("  --config <path>       Path to plsql-config.yaml");
        System.err.println("  --entry <name>        Entry point (e.g. PKG_NAME.PROC_NAME or FUNC_NAME)");
        System.err.println("                        Can be specified multiple times");
        System.err.println();
        System.err.println("Optional:");
        System.err.println("  --output-dir <path>   Directory for output JSON files");
        System.err.println("  --max-depth <N>       Maximum recursion depth (default: -1 = unlimited)");
        System.err.println("  --pretty              Pretty-print the JSON output");
        System.err.println();
        System.err.println("Examples:");
        System.err.println("  java -jar plsql-parser.jar --flow --config config.yaml \\");
        System.err.println("    --entry \"PG_AC_EINVOICE.PC_IRBM_SUBMISSION\" --pretty --output-dir ./output");
        System.err.println();
        System.err.println("  java -jar plsql-parser.jar --flow --config config.yaml \\");
        System.err.println("    --entry \"FN_UWGE_GET_ENDTSCHD_QUERY\" --entry \"PG_AC_EINVOICE.PC_IRBM_SUBMISSION\"");
    }
}
