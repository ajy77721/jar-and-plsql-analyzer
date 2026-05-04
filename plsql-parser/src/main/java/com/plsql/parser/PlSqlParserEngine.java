package com.plsql.parser;

import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.visitor.PlSqlAnalysisVisitor;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Engine that drives the ANTLR4 lexer/parser and visitor to produce a ParseResult.
 */
public class PlSqlParserEngine {

    /**
     * Parse a file and return the result.
     */
    public ParseResult parseFile(Path filePath) {
        long startTime = System.currentTimeMillis();
        ParseResult result = new ParseResult();
        result.setFileName(filePath.getFileName().toString());

        try {
            String content = readFileWithFallback(filePath);
            result = parseContent(content, filePath.getFileName().toString());
        } catch (Exception e) {
            result.getErrors().add("Failed to read file: " + e.getMessage());
        }

        result.setParseTimeMs(System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * Check if source content is Oracle wrapped/encrypted.
     */
    public static boolean isWrappedSource(String content) {
        if (content == null || content.isEmpty()) return false;
        String firstChunk = content.length() > 300 ? content.substring(0, 300) : content;
        String upper = firstChunk.toUpperCase();
        if (upper.contains(" WRAPPED")) return true;
        if (firstChunk.matches("(?s).*\\b[0-9a-f]{8,}\\b.*")) return true;
        return false;
    }

    static String trimDuplicateDefinition(String content) {
        if (content == null || content.length() < 20) return content;
        String upper = content.toUpperCase();
        boolean isStandalone = false;
        for (String kw : new String[]{"FUNCTION", "PROCEDURE"}) {
            int idx = upper.indexOf(kw);
            if (idx >= 0 && idx < 60) {
                int pkgIdx = upper.indexOf("PACKAGE");
                if (pkgIdx < 0 || pkgIdx > idx) { isStandalone = true; }
                break;
            }
        }
        if (!isStandalone) return content;
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                "(?i)\\bEND\\s*;\\s*(?:FUNCTION|PROCEDURE)\\b");
        java.util.regex.Matcher m = p.matcher(content);
        if (m.find()) {
            int cut = m.start();
            int semi = content.indexOf(';', cut);
            if (semi >= 0) return content.substring(0, semi + 1);
        }
        return content;
    }

    /**
     * Extract basic object info (name, type) from the first line of PL/SQL source.
     */
    private static ParsedObject extractBasicInfo(String content, String fileName) {
        ParsedObject obj = new ParsedObject();
        obj.setName(fileName.replaceAll("\\.[^.]+$", "").toUpperCase());
        obj.setType("UNKNOWN");

        String upper = content.toUpperCase();
        String[] keywords = {"PACKAGE BODY", "PACKAGE", "PROCEDURE", "FUNCTION", "TRIGGER", "TYPE BODY", "TYPE"};
        for (String kw : keywords) {
            int idx = upper.indexOf(kw);
            if (idx >= 0 && idx < 200) {
                obj.setType(kw);
                String after = content.substring(idx + kw.length()).trim();
                String[] tokens = after.split("[\\s(;/]+", 2);
                if (tokens.length > 0 && !tokens[0].isEmpty()) {
                    String name = tokens[0].replaceAll("\"", "").toUpperCase();
                    if (name.contains(".")) {
                        String[] parts = name.split("\\.", 2);
                        obj.setSchema(parts[0]);
                        obj.setName(parts[1]);
                    } else {
                        obj.setName(name);
                    }
                }
                break;
            }
        }
        return obj;
    }

    /**
     * Parse a string of PL/SQL content and return the result.
     */
    public ParseResult parseContent(String content, String fileName) {
        long startTime = System.currentTimeMillis();
        ParseResult result = new ParseResult();
        result.setFileName(fileName);

        content = trimDuplicateDefinition(content);

        // Skip ANTLR parsing for wrapped/encrypted source
        if (isWrappedSource(content)) {
            ParsedObject obj = extractBasicInfo(content, fileName);
            obj.setType(obj.getType() + " (ENCRYPTED/WRAPPED)");
            result.getObjects().add(obj);
            result.setParseTimeMs(System.currentTimeMillis() - startTime);
            return result;
        }

        List<String> errors = new ArrayList<>();

        try {
            // Create the lexer and parser
            CharStream charStream = CharStreams.fromString(content);
            PlSqlLexer lexer = new PlSqlLexer(charStream);

            // Suppress lexer error output; collect errors
            lexer.removeErrorListeners();
            lexer.addErrorListener(new CollectingErrorListener(errors));

            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            PlSqlParser parser = new PlSqlParser(tokenStream);

            // --- Stage 1: SLL prediction mode (fast, low memory) ---
            // Remove all error listeners and use BailErrorStrategy so that
            // SLL fails fast without building expensive error-recovery structures.
            parser.removeErrorListeners();
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            parser.setErrorHandler(new BailErrorStrategy());

            ParseTree tree;
            try {
                tree = parser.sql_script();
            } catch (ParseCancellationException | RecognitionException e) {
                // --- Stage 2: LL prediction mode (robust, slower) ---
                // SLL failed, reset and retry with full LL prediction.
                tokenStream.seek(0);
                parser.reset();
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                parser.setErrorHandler(new DefaultErrorStrategy());
                parser.addErrorListener(new CollectingErrorListener(errors));
                tree = parser.sql_script();
            }

            // Free DFA cache to release memory for subsequent parses
            parser.getInterpreter().clearDFA();

            // Run the visitor
            PlSqlAnalysisVisitor visitor = new PlSqlAnalysisVisitor();
            visitor.visit(tree);

            result.setObjects(visitor.getResults());
            result.setErrors(errors);

        } catch (Exception e) {
            errors.add("Parse error: " + e.getMessage());
            result.setErrors(errors);
        }

        result.setParseTimeMs(System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * Read a file, trying UTF-8 first, then ISO-8859-1 as fallback.
     */
    private String readFileWithFallback(Path filePath) throws IOException {
        // Try UTF-8 first
        try {
            byte[] bytes = Files.readAllBytes(filePath);

            // Check for BOM
            if (bytes.length >= 3 && bytes[0] == (byte) 0xEF
                && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                return new String(bytes, 3, bytes.length - 3, StandardCharsets.UTF_8);
            }

            String content = new String(bytes, StandardCharsets.UTF_8);

            // Validate UTF-8 by checking for replacement character
            if (!content.contains("\uFFFD")) {
                return content;
            }
        } catch (Exception e) {
            // Fall through to ISO-8859-1
        }

        // Fallback to ISO-8859-1
        return new String(Files.readAllBytes(filePath), Charset.forName("ISO-8859-1"));
    }

    /**
     * Error listener that collects errors into a list.
     */
    private static class CollectingErrorListener extends BaseErrorListener {
        private final List<String> errors;

        CollectingErrorListener(List<String> errors) {
            this.errors = errors;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                                Object offendingSymbol,
                                int line, int charPositionInLine,
                                String msg,
                                RecognitionException e) {
            errors.add("line " + line + ":" + charPositionInLine + " " + msg);
        }
    }
}
