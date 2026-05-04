package com.plsql.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.plsql.parser.flow.FlowAnalysisMain;
import com.plsql.parser.model.ParseResult;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/**
 * CLI entry point for the PL/SQL parser.
 *
 * Usage:
 *   java -jar plsql-parser.jar [options] &lt;file-or-directory&gt;
 *
 * Options:
 *   -o, --output &lt;file&gt;   Write JSON output to file (default: stdout)
 *   --pretty               Pretty-print the JSON output
 *   -h, --help             Show help
 */
public class PlSqlParserMain {

    private static final Set<String> SQL_EXTENSIONS = new HashSet<>(Arrays.asList(
        ".sql", ".pks", ".pkb", ".prc", ".fnc", ".trg", ".vw", ".mv", ".seq"
    ));

    public static void main(String[] args) {
        // Check for --flow mode first
        for (String arg : args) {
            if ("--flow".equals(arg)) {
                FlowAnalysisMain.run(args);
                return;
            }
        }

        // Parse command line arguments
        String outputFile = null;
        boolean prettyPrint = false;
        String inputPath = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-o":
                case "--output":
                    if (i + 1 < args.length) {
                        outputFile = args[++i];
                    } else {
                        System.err.println("Error: --output requires a file path argument");
                        System.exit(1);
                    }
                    break;
                case "--pretty":
                    prettyPrint = true;
                    break;
                case "-h":
                case "--help":
                    printUsage();
                    return;
                default:
                    if (args[i].startsWith("-")) {
                        System.err.println("Unknown option: " + args[i]);
                        printUsage();
                        System.exit(1);
                    }
                    inputPath = args[i];
                    break;
            }
        }

        if (inputPath == null) {
            System.err.println("Error: no input file or directory specified");
            printUsage();
            System.exit(1);
        }

        Path input = Paths.get(inputPath);
        if (!Files.exists(input)) {
            System.err.println("Error: path does not exist: " + inputPath);
            System.exit(1);
        }

        PlSqlParserEngine engine = new PlSqlParserEngine();
        long totalStart = System.currentTimeMillis();

        try {
            ObjectMapper mapper = new ObjectMapper();
            if (prettyPrint) {
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
            }

            String jsonOutput;

            if (Files.isDirectory(input)) {
                // Directory mode: recursively scan for SQL files
                List<Path> files = findSqlFiles(input);
                System.err.println("Found " + files.size() + " SQL files in " + inputPath);

                List<ParseResult> results = new ArrayList<>();
                int processed = 0;

                for (Path file : files) {
                    processed++;
                    System.err.print("\rParsing [" + processed + "/" + files.size() + "] "
                        + file.getFileName() + "...");
                    try {
                        ParseResult result = engine.parseFile(file);
                        results.add(result);
                    } catch (Exception e) {
                        ParseResult errResult = new ParseResult();
                        errResult.setFileName(file.getFileName().toString());
                        errResult.getErrors().add("Fatal error: " + e.getMessage());
                        results.add(errResult);
                    }
                }
                System.err.println();

                jsonOutput = mapper.writeValueAsString(results);

                // Print stats
                long totalTime = System.currentTimeMillis() - totalStart;
                int totalErrors = results.stream().mapToInt(r -> r.getErrors().size()).sum();
                int totalObjects = results.stream().mapToInt(r -> r.getObjects().size()).sum();
                System.err.println("Completed: " + files.size() + " files, "
                    + totalObjects + " objects, "
                    + totalErrors + " errors, "
                    + totalTime + "ms total");

            } else {
                // Single file mode
                System.err.println("Parsing " + input.getFileName() + "...");
                ParseResult result = engine.parseFile(input);
                jsonOutput = mapper.writeValueAsString(result);

                long totalTime = System.currentTimeMillis() - totalStart;
                System.err.println("Completed: " + result.getObjects().size() + " objects, "
                    + result.getErrors().size() + " errors, "
                    + totalTime + "ms");
            }

            // Output
            if (outputFile != null) {
                Files.writeString(Paths.get(outputFile), jsonOutput);
                System.err.println("Output written to " + outputFile);
            } else {
                System.out.println(jsonOutput);
            }

        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * Recursively find all SQL files in a directory.
     */
    private static List<Path> findSqlFiles(Path directory) throws IOException {
        try (Stream<Path> walk = Files.walk(directory)) {
            return walk
                .filter(Files::isRegularFile)
                .filter(p -> {
                    String name = p.getFileName().toString().toLowerCase();
                    int dotIdx = name.lastIndexOf('.');
                    if (dotIdx < 0) return false;
                    String ext = name.substring(dotIdx);
                    return SQL_EXTENSIONS.contains(ext);
                })
                .sorted()
                .collect(Collectors.toList());
        }
    }

    private static void printUsage() {
        System.err.println("PL/SQL Parser - Static analysis tool for PL/SQL code");
        System.err.println();
        System.err.println("Usage:");
        System.err.println("  java -jar plsql-parser.jar [options] <file-or-directory>");
        System.err.println("  java -jar plsql-parser.jar --flow [flow-options]");
        System.err.println();
        System.err.println("Options:");
        System.err.println("  -o, --output <file>  Write JSON output to file (default: stdout)");
        System.err.println("  --pretty             Pretty-print the JSON output");
        System.err.println("  --flow               Enable flow analysis mode (recursive dependency crawl)");
        System.err.println("  -h, --help           Show this help message");
        System.err.println();
        System.err.println("Flow analysis options (use with --flow):");
        System.err.println("  --config <path>      Path to plsql-config.yaml (required)");
        System.err.println("  --entry <name>       Entry point, e.g. PKG.PROC or FUNC (repeatable)");
        System.err.println("  --output-dir <path>  Directory for output JSON files");
        System.err.println("  --max-depth <N>      Maximum recursion depth (default: 5)");
        System.err.println();
        System.err.println("Supported file extensions:");
        System.err.println("  .sql .pks .pkb .prc .fnc .trg .vw .mv .seq");
        System.err.println();
        System.err.println("Examples:");
        System.err.println("  java -jar plsql-parser.jar --pretty mypackage.pkb");
        System.err.println("  java -jar plsql-parser.jar -o results.json ./sql/");
        System.err.println("  java -jar plsql-parser.jar --flow --config config.yaml --entry \"PKG.PROC\" --pretty");
    }
}
