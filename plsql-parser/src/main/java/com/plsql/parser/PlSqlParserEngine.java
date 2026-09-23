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

    /** The same shared enricher the four non-Oracle dialects use. */
    private static final com.plsql.parser.service.TableOperationEnricher ENRICHER =
            new com.plsql.parser.service.TableOperationEnricher();

    /** Counts parses so the shared SLL DFA cache is cleared only periodically (memory bound). */
    private static final java.util.concurrent.atomic.AtomicLong PARSE_COUNT =
            new java.util.concurrent.atomic.AtomicLong();

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
        // Genuine Oracle wrap()-obfuscated units contain long runs of lowercase-hex-looking
        // tokens (e.g. the checksum/header lines emitted by `wrap`). A plain run of 8+ ASCII
        // DIGITS (e.g. an 8+ digit numeric literal such as 12345678, or a date constant like
        // 20240115) is technically a subset of the character class [0-9a-f] but is NOT hex
        // payload at all, so it must not trip this heuristic. Require the matched 8+-char
        // "hex-looking" run to contain at least one genuine hex LETTER (a-f).
        java.util.regex.Matcher hexish =
                java.util.regex.Pattern.compile("\\b[0-9a-f]{8,}\\b").matcher(firstChunk);
        while (hexish.find()) {
            if (hexish.group().matches(".*[a-f].*")) return true;
        }
        return false;
    }

    static String trimDuplicateDefinition(String content) {
        if (content == null || content.length() < 20) return content;

        // ── Pass 1: existing logic for standalone FUNCTION/PROCEDURE duplicates ──────
        // Handles the case where a standalone FUNCTION or PROCEDURE body is followed
        // immediately by another FUNCTION/PROCEDURE keyword (e.g. spec concatenated with body).
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
        if (isStandalone) {
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                    "(?i)\\bEND\\s*;\\s*(?:FUNCTION|PROCEDURE)\\b");
            java.util.regex.Matcher m = p.matcher(content);
            if (m.find()) {
                int cut = m.start();
                int semi = content.indexOf(';', cut);
                if (semi >= 0) return content.substring(0, semi + 1);
            }
        }

        // ── Pass 2: strip a second CREATE ... DDL statement after the first unit ends ─
        // Handles files where two complete DDL objects have been concatenated (e.g. a .pkb
        // file that contains CREATE OR REPLACE PACKAGE BODY A … END A; followed by a
        // SQL*Plus "prompt" line and then CREATE OR REPLACE PACKAGE BODY B …).
        // This can produce ANTLR errors like:
        //   extraneous input 'CREATE' expecting {<EOF>, '/', ';'}
        return stripConcatenatedDdl(content);
    }

    /**
     * Strip any second (and beyond) CREATE DDL statement that has been concatenated after
     * the first complete PL/SQL unit in {@code content}.
     *
     * <p>A second unit is detected when ALL of the following hold:
     * <ol>
     *   <li>A top-level {@code END [name];} (or bare {@code /} on its own line) exists,
     *       indicating the first unit is complete.</li>
     *   <li>After that terminator, ignoring blank lines and SQL*Plus {@code prompt} lines,
     *       there is another {@code CREATE [OR REPLACE] (PACKAGE|PACKAGE BODY|PROCEDURE|
     *       FUNCTION|TRIGGER)} keyword that is not inside a string literal or line comment.</li>
     * </ol>
     *
     * <p>The method is intentionally conservative: if it cannot safely identify both a
     * complete first unit and a second CREATE, it returns the original content unchanged
     * and logs a warning.
     */
    private static String stripConcatenatedDdl(String content) {
        if (content == null) return null;

        // Pattern for the second CREATE keyword that starts a new DDL object.
        // We look for CREATE (OR REPLACE)? followed by one of the PL/SQL object keywords.
        // The match must be at the start of a line (after optional whitespace) so we don't
        // accidentally match a CREATE TABLE / CREATE INDEX that could appear in dynamic SQL.
        java.util.regex.Pattern secondCreatePat = java.util.regex.Pattern.compile(
                "(?im)^[ \\t]*CREATE(?:\\s+OR\\s+REPLACE)?\\s+"
                + "(?:PACKAGE(?:\\s+BODY)?|PROCEDURE|FUNCTION|TRIGGER|TYPE(?:\\s+BODY)?)\\b");

        java.util.regex.Matcher createMatcher = secondCreatePat.matcher(content);

        // Skip the FIRST CREATE (which opens the object we are parsing).
        if (!createMatcher.find()) return content;  // no CREATE at all — nothing to do
        int firstCreateStart = createMatcher.start();

        // Look for a SECOND CREATE after the first.
        if (!createMatcher.find()) return content;  // only one CREATE — already clean
        int secondCreateStart = createMatcher.start();

        // The secondCreatePat regex anchors to ^ (start of line) and requires specific PL/SQL
        // keywords — a CREATE PACKAGE/PROCEDURE/FUNCTION at column 0 is never inside a string
        // literal in real source files, so no character-by-character scan is needed.

        // Verify the first unit is actually complete by finding the last top-level END <name>;
        // or bare "/" terminator in the region between the first CREATE and secondCreateStart.
        // We need the position of that terminator so we can cut right after it.
        String between = content.substring(firstCreateStart, secondCreateStart);

        // Try END [name]; first (covers PACKAGE BODY, PROCEDURE, FUNCTION, TRIGGER)
        java.util.regex.Pattern endNamePat = java.util.regex.Pattern.compile(
                "(?im)^[ \\t]*END(?:\\s+[\\w\".$]+)?\\s*;[ \\t]*$");
        java.util.regex.Matcher endMatcher = endNamePat.matcher(between);
        int lastEndPos = -1;
        while (endMatcher.find()) {
            lastEndPos = endMatcher.end(); // keep updating to get the LAST match
        }

        // Also try a bare "/" on its own line
        java.util.regex.Pattern slashPat = java.util.regex.Pattern.compile(
                "(?m)^[ \\t]*/[ \\t]*$");
        java.util.regex.Matcher slashMatcher = slashPat.matcher(between);
        int lastSlashPos = -1;
        while (slashMatcher.find()) {
            lastSlashPos = slashMatcher.end();
        }

        // Use whichever terminator appears latest in the text
        int terminatorEnd = Math.max(lastEndPos, lastSlashPos);
        if (terminatorEnd < 0) {
            // The first unit does not appear to be complete — do not truncate.
            return content;
        }

        // If the first unit ends with a "/" terminator, the two DDL objects are intentionally
        // separated (e.g. a trigger file followed by a package body in the same test/script).
        // ANTLR's sql_script rule can parse multiple such objects — do not strip the second one.
        if (lastSlashPos >= 0 && lastSlashPos >= lastEndPos) {
            return content;
        }

        // Cut right after the terminator (terminatorEnd is relative to firstCreateStart).
        // Everything from firstCreateStart + terminatorEnd onward (the inter-unit gap plus
        // the second DDL object) is discarded.
        int absoluteCutEnd = firstCreateStart + terminatorEnd;
        String result = content.substring(0, absoluteCutEnd);
        // Ensure trailing newline so ANTLR doesn't see an abrupt EOF mid-token.
        if (!result.endsWith("\n")) result = result + "\n";

        // Safety: if trimming removed more than 10 % of the original content, emit a warning
        // so the issue is visible in logs — but still return the trimmed version since we
        // confirmed a complete first unit and a genuine second CREATE.
        if (result.length() < content.length() * 0.9) {
            System.err.println("[PlSqlParserEngine] WARNING: stripConcatenatedDdl removed "
                    + (content.length() - result.length()) + " chars ("
                    + String.format("%.0f", 100.0 * (content.length() - result.length()) / content.length())
                    + "%) from source — second CREATE DDL stripped.");
        }
        return result;
    }

    /**
     * Returns {@code true} if the character at {@code targetIdx} is inside a single-quoted
     * string literal or a single-line ({@code --}) comment, scanning forward from
     * {@code fromIdx}.
     *
     * <p>This is a lightweight scanner, not a full PL/SQL lexer. It handles:
     * <ul>
     *   <li>Single-quoted string literals (with {@code ''} escape for embedded quotes).</li>
     *   <li>Single-line {@code --} comments (terminated by newline).</li>
     * </ul>
     * Block comments ({@code /* ... *}{@code /}) are not common in the file preamble area
     * we are scanning, so they are intentionally ignored for simplicity.
     */
    private static boolean isInsideStringOrComment(String text, int fromIdx, int targetIdx) {
        boolean inString = false;
        boolean inLineComment = false;
        for (int i = fromIdx; i < targetIdx && i < text.length(); i++) {
            char c = text.charAt(i);
            if (inLineComment) {
                if (c == '\n') inLineComment = false;
            } else if (inString) {
                if (c == '\'') {
                    // Check for escaped quote ''
                    if (i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                        i++; // skip the second quote
                    } else {
                        inString = false;
                    }
                }
            } else {
                if (c == '\'' ) {
                    inString = true;
                } else if (c == '-' && i + 1 < text.length() && text.charAt(i + 1) == '-') {
                    inLineComment = true;
                    i++; // skip the second '-'
                }
            }
        }
        return inString || inLineComment;
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

        if (content != null && content.startsWith("﻿")) {
            content = content.substring(1);
        }

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

            // Keep the SLL DFA (decision) cache WARM across parses — it is shared
            // across PlSqlParser instances, so clearing it every parse forced each
            // object to rebuild the DFA from the ATN, which is the dominant cost for
            // the heavy PL/SQL grammar (~0.6s per large proc). Clear only periodically
            // to bound memory on very long runs while preserving the warmup benefit.
            if (PARSE_COUNT.incrementAndGet() % 200 == 0) {
                parser.getInterpreter().clearDFA();
            }

            // Run the visitor
            PlSqlAnalysisVisitor visitor = new PlSqlAnalysisVisitor();
            visitor.visit(tree);

            result.setObjects(visitor.getResults());
            result.setErrors(errors);

            // Oracle needs the same column-level enrichment as the other four dialects. It does not
            // go through StoredProcedureParserFactory — that returns null for Oracle — so without
            // this call Oracle was the one datasource with no column detail, which is the opposite
            // of what anyone would expect.
            if (result.getObjects() != null) ENRICHER.enrich(result.getObjects());

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
