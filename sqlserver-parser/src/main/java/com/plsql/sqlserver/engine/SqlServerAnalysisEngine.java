package com.plsql.sqlserver.engine;

import com.plsql.sqlserver.grammar.SqlServerLexer;
import com.plsql.sqlserver.grammar.SqlServerParser;
import com.plsql.sqlserver.model.*;
import com.plsql.sqlserver.visitor.SqlServerAnalysisVisitor;
import com.plsql.sqlserver.visitor.SqlServerParseErrorListener;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for SQL Server T-SQL analysis (ANTLR4-based).
 *
 * Returns a self-contained SqlServerRoutineInfo — no dependency on the
 * plsql-parser module. The caller (SqlServerProcedureParser in plsql-parser)
 * converts the result to the shared ParseResult model.
 *
 * Flow:
 *   source text
 *     → SqlServerLexer  (ANTLR4 lexer from SqlServerLexer.g4)
 *     → SqlServerParser (ANTLR4 parser from SqlServerParser.g4)
 *     → SqlServerAnalysisVisitor (extract calls, table ops, variables, dynamic SQL)
 *     → SqlServerRoutineInfo
 */
public class SqlServerAnalysisEngine {

    private static final Logger log = LoggerFactory.getLogger(SqlServerAnalysisEngine.class);

    /**
     * Parse a single SQL Server PROCEDURE or FUNCTION source.
     *
     * @param source     full CREATE PROCEDURE / FUNCTION DDL text
     * @param objectName fallback name used when grammar cannot resolve it
     * @param schema     owner / database schema
     */
    public SqlServerRoutineInfo parse(String source, String objectName, String schema) {
        SqlServerRoutineInfo result = new SqlServerRoutineInfo();
        result.setName(objectName);
        result.setSchema(schema);
        result.setType(SqlServerRoutineInfo.RoutineType.PROCEDURE);

        try {
            SqlServerParseErrorListener errors = new SqlServerParseErrorListener();

            if (source != null && source.startsWith("﻿")) {
                source = source.substring(1);
            }

            // Lex
            CharStream chars = CharStreams.fromString(source);
            SqlServerLexer lexer = new SqlServerLexer(chars);
            lexer.removeErrorListeners();
            lexer.addErrorListener(errors);

            // Parse
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SqlServerParser parser = new SqlServerParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errors);

            ParseTree tree = parser.compilationUnit();

            // Visit
            SqlServerAnalysisVisitor visitor = new SqlServerAnalysisVisitor(objectName, schema);
            visitor.visit(tree);

            result = visitor.getResult();
            result.getErrors().addAll(errors.getErrors());

            // Ensure line numbers are set
            result.setLineStart(1);
            if (result.getLineEnd() == 0) {
                result.setLineEnd(countLines(source));
            }

            if (errors.hasErrors()) {
                log.debug("SqlServerAnalysisEngine: {} syntax error(s) in {}",
                        errors.getErrors().size(), objectName);
            }

        } catch (Exception e) {
            log.warn("SqlServerAnalysisEngine failure on {}: {}", objectName, e.getMessage(), e);
            result.getErrors().add("Parse error: " + e.getMessage());
        }

        return result;
    }

    // Standard T-SQL batch separator: "GO" alone on its own line (whitespace allowed either
    // side, case-insensitive). It is a client-tool directive, not real SQL syntax, so
    // compilationUnit only ever sees a single routine per parse() call.
    private static final java.util.regex.Pattern GO_SEPARATOR =
            java.util.regex.Pattern.compile("^[ \\t]*GO[ \\t]*$", java.util.regex.Pattern.CASE_INSENSITIVE);

    /**
     * Splits {@code source} on standalone "GO" batch-separator lines and parses each batch
     * independently, returning one {@link SqlServerRoutineInfo} per batch that contains real
     * content. Bug 7 fix: {@code compilationUnit} only accepts a single routine per parse, so
     * without this, a script with two CREATE PROCEDUREs separated by GO merged them into one
     * parse and misattributed the second procedure's calls to the first. Each batch keeps its
     * original absolute line numbers (blank-line padding for the lines occupied by earlier
     * batches), so error/call line numbers stay accurate.
     */
    public java.util.List<SqlServerRoutineInfo> parseBatch(String source, String objectName, String schema) {
        java.util.List<SqlServerRoutineInfo> out = new java.util.ArrayList<>();
        if (source == null) {
            out.add(parse(source, objectName, schema));
            return out;
        }
        String[] lines = source.split("\n", -1);
        StringBuilder current = new StringBuilder();
        int batchStartLine = 1;
        for (int i = 0; i < lines.length; i++) {
            if (GO_SEPARATOR.matcher(lines[i]).matches()) {
                flushBatch(out, current, batchStartLine, objectName, schema);
                current = new StringBuilder();
                batchStartLine = i + 2; // next line (1-based) after this GO line
            } else {
                current.append(lines[i]).append('\n');
            }
        }
        flushBatch(out, current, batchStartLine, objectName, schema);
        if (out.isEmpty()) {
            out.add(parse(source, objectName, schema));
        }
        return out;
    }

    private void flushBatch(java.util.List<SqlServerRoutineInfo> out, StringBuilder current,
                             int batchStartLine, String objectName, String schema) {
        if (current.toString().isBlank()) return;
        // Pad with blank lines so this batch's content lands on its original absolute line
        // numbers when lexed/parsed standalone.
        String padded = "\n".repeat(Math.max(0, batchStartLine - 1)) + current;
        out.add(parse(padded, objectName, schema));
    }

    /**
     * Extract the body text between the outermost BEGIN...END after the AS keyword.
     * Returns null if no AS keyword is found.
     * Used by tests and external callers expecting the raw body text.
     */
    public String extractBody(String source) {
        if (source == null) return null;
        String upper = source.toUpperCase();

        int asIdx = indexOfKeyword(upper, "AS", 0);
        if (asIdx < 0) return null;

        String afterAs = source.substring(asIdx + 2).stripLeading();
        String upperAfter = afterAs.toUpperCase();

        int beginIdx = indexOfKeyword(upperAfter, "BEGIN", 0);
        if (beginIdx < 0) {
            // No BEGIN — return everything after AS
            return afterAs.isBlank() ? null : afterAs;
        }

        // Track BEGIN/END depth to find the matching END
        int depth = 0;
        int i = beginIdx;
        while (i < upperAfter.length()) {
            if (isKeywordAt(upperAfter, i, "BEGIN")) {
                depth++;
                i += 5;
            } else if (isKeywordAt(upperAfter, i, "END")) {
                depth--;
                if (depth == 0) {
                    return afterAs.substring(beginIdx + 5, i).trim();
                }
                i += 3;
            } else {
                i++;
            }
        }
        // Unmatched BEGIN — return everything after it
        return afterAs.substring(beginIdx + 5).trim();
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private int countLines(String src) {
        int c = 1;
        for (char ch : src.toCharArray()) if (ch == '\n') c++;
        return c;
    }

    private int indexOfKeyword(String upper, String kw, int from) {
        int idx = from;
        while ((idx = upper.indexOf(kw, idx)) >= 0) {
            if (isKeywordAt(upper, idx, kw)) return idx;
            idx++;
        }
        return -1;
    }

    private boolean isKeywordAt(String src, int idx, String kw) {
        if (idx + kw.length() > src.length()) return false;
        if (!src.regionMatches(idx, kw, 0, kw.length())) return false;
        boolean before = idx == 0 || !Character.isLetterOrDigit(src.charAt(idx - 1));
        boolean after  = (idx + kw.length()) >= src.length()
                || !Character.isLetterOrDigit(src.charAt(idx + kw.length()));
        return before && after;
    }
}
