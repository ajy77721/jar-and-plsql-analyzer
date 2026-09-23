package com.plsql.db2.engine;

import com.plsql.db2.grammar.Db2Lexer;
import com.plsql.db2.grammar.Db2Parser;
import com.plsql.db2.model.*;
import com.plsql.db2.visitor.Db2AnalysisVisitor;
import com.plsql.db2.visitor.Db2ParseErrorListener;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for IBM DB2 SQL PL analysis (ANTLR4-based).
 *
 * Returns a self-contained Db2RoutineInfo — no dependency on the
 * plsql-parser module. The caller (Db2ProcedureParser in plsql-parser)
 * converts the result to the shared ParseResult model.
 *
 * Flow:
 *   source text
 *     → Db2Lexer  (ANTLR4 lexer from Db2Lexer.g4)
 *     → Db2Parser (ANTLR4 parser from Db2Parser.g4)
 *     → Db2AnalysisVisitor (extract calls, table ops, variables, dynamic SQL)
 *     → Db2RoutineInfo
 */
public class Db2AnalysisEngine {

    private static final Logger log = LoggerFactory.getLogger(Db2AnalysisEngine.class);

    /**
     * Parse a single DB2 PROCEDURE or FUNCTION source.
     *
     * @param source     full CREATE PROCEDURE / FUNCTION DDL text
     * @param objectName fallback name used when grammar cannot resolve it
     * @param schema     owner / database schema
     */
    public Db2RoutineInfo parse(String source, String objectName, String schema) {
        Db2RoutineInfo result = new Db2RoutineInfo();
        result.setName(objectName);
        result.setSchema(schema);
        result.setType(Db2RoutineInfo.RoutineType.PROCEDURE);

        try {
            Db2ParseErrorListener errors = new Db2ParseErrorListener();

            // Strip a leading BOM, if present: it is not a lexer-recognizable character and
            // otherwise produces a spurious token-recognition error before the real grammar starts.
            if (source != null && !source.isEmpty() && source.charAt(0) == '﻿') {
                source = source.substring(1);
            }

            // Lex
            CharStream chars = CharStreams.fromString(source);
            Db2Lexer lexer = new Db2Lexer(chars);
            lexer.removeErrorListeners();
            lexer.addErrorListener(errors);

            // Parse
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            Db2Parser parser = new Db2Parser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errors);

            ParseTree tree = parser.compilationUnit();

            // Visit
            Db2AnalysisVisitor visitor = new Db2AnalysisVisitor(objectName, schema);
            visitor.visit(tree);

            result = visitor.getResult();
            result.getErrors().addAll(errors.getErrors());

            // Ensure line numbers are set
            result.setLineStart(1);
            if (result.getLineEnd() == 0) {
                result.setLineEnd(countLines(source));
            }

            if (errors.hasErrors()) {
                log.debug("Db2AnalysisEngine: {} syntax error(s) in {}",
                        errors.getErrors().size(), objectName);
            }

        } catch (Exception e) {
            log.warn("Db2AnalysisEngine failure on {}: {}", objectName, e.getMessage(), e);
            result.getErrors().add("Parse error: " + e.getMessage());
        }

        return result;
    }

    /**
     * Extract the body text between the outermost BEGIN...END block.
     * Returns null if no BEGIN keyword is found.
     * Used by tests and external callers expecting the raw body text.
     *
     * Handles compound END keywords (END IF, END WHILE, END FOR, END LOOP,
     * END CASE, END REPEAT) — these do not count as END block closers.
     */
    public String extractBody(String source) {
        if (source == null) return null;
        String upper = source.toUpperCase();
        int beginIdx = indexOfKeyword(upper, "BEGIN", 0);
        if (beginIdx < 0) return null;
        int depth = 0;
        int i = beginIdx;
        while (i < upper.length()) {
            if (isKeywordAt(upper, i, "BEGIN")) {
                depth++;
                i += 5;
            } else if (isKeywordAt(upper, i, "END")) {
                if (!isCompoundEnd(upper, i)) {
                    depth--;
                    if (depth == 0) return source.substring(beginIdx + 5, i).trim();
                }
                i += 3;
            } else {
                i++;
            }
        }
        return source.substring(beginIdx + 5).trim();
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    /**
     * Returns true when "END" at idx is followed by a compound keyword
     * (IF, WHILE, FOR, LOOP, CASE, REPEAT) — meaning it's not a block closer.
     */
    private boolean isCompoundEnd(String upper, int idx) {
        int i = idx + 3;
        while (i < upper.length() && Character.isWhitespace(upper.charAt(i))) i++;
        for (String kw : new String[]{"IF", "WHILE", "FOR", "LOOP", "CASE", "REPEAT"}) {
            if (isKeywordAt(upper, i, kw)) return true;
        }
        return false;
    }

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

    private boolean isKeywordAt(String s, int idx, String kw) {
        if (idx + kw.length() > s.length()) return false;
        if (!s.regionMatches(idx, kw, 0, kw.length())) return false;
        boolean before = idx == 0 || !Character.isLetterOrDigit(s.charAt(idx - 1));
        boolean after = (idx + kw.length()) >= s.length()
                || !Character.isLetterOrDigit(s.charAt(idx + kw.length()));
        return before && after;
    }
}
