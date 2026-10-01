package com.plsql.postgres.engine;

import com.plsql.postgres.grammar.PostgresLexer;
import com.plsql.postgres.grammar.PostgresParser;
import com.plsql.postgres.model.*;
import com.plsql.postgres.visitor.PostgresAnalysisVisitor;
import com.plsql.postgres.visitor.PostgresParseErrorListener;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Entry point for IBM POSTGRES SQL PL analysis (ANTLR4-based).
 *
 * Returns a self-contained PostgresRoutineInfo — no dependency on the
 * plsql-parser module. The caller (PostgresProcedureParser in plsql-parser)
 * converts the result to the shared ParseResult model.
 *
 * Flow:
 *   source text
 *     → PostgresLexer  (ANTLR4 lexer from PostgresLexer.g4)
 *     → PostgresParser (ANTLR4 parser from PostgresParser.g4)
 *     → PostgresAnalysisVisitor (extract calls, table ops, variables, dynamic SQL)
 *     → PostgresRoutineInfo
 */
public class PostgresAnalysisEngine {

    private static final Logger log = LoggerFactory.getLogger(PostgresAnalysisEngine.class);

    /**
     * Number of times the lexer could not produce a {@code DOLLAR_QUOTED_STRING} token for the
     * PL/pgSQL wrapper and the legacy regex scan had to locate the opening tag instead. Exposed so
     * the regex-retirement rate of the dollar-quote migration is observable (should stay ~0).
     */
    public static final java.util.concurrent.atomic.AtomicLong DOLLAR_QUOTE_LEXER_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * Leading {@code $tag$} of the first dollar-quoted string in {@code source}, located from the
     * lexer token stream, as {@code {startIndex, endIndex}} (end exclusive), or null when the
     * source contains no lexable dollar-quoted string.
     */
    private static int[] findDollarQuoteOpenTag(String source) {
        try {
            PostgresLexer lexer = new PostgresLexer(CharStreams.fromString(source));
            lexer.removeErrorListeners();
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            tokens.fill();
            for (Token t : tokens.getTokens()) {
                if (t.getType() != PostgresLexer.DOLLAR_QUOTED_STRING) continue;
                String txt = t.getText();
                int secondDollar = txt.indexOf('$', 1);   // end of the opening `$tag$`
                if (secondDollar > 0) {
                    return new int[]{t.getStartIndex(), t.getStartIndex() + secondDollar + 1};
                }
            }
        } catch (RuntimeException ignore) {
            // fall through to the regex fallback
        }
        return null;
    }

    /**
     * Parse a single POSTGRES PROCEDURE or FUNCTION source.
     *
     * @param source     full CREATE PROCEDURE / FUNCTION DDL text
     * @param objectName fallback name used when grammar cannot resolve it
     * @param schema     owner / database schema
     */
    public PostgresRoutineInfo parse(String source, String objectName, String schema) {
        PostgresRoutineInfo result = new PostgresRoutineInfo();
        result.setName(objectName);
        result.setSchema(schema);
        result.setType(PostgresRoutineInfo.RoutineType.PROCEDURE);

        // Bug: a leading BOM (U+FEFF) at the very start of the source precedes any dollar-quote
        // span, so preprocess()/blankComments() never touch it, and it isn't part of the lexer's
        // WS skip rule — it surfaces as a spurious (but otherwise-recoverable) token recognition
        // error. Strip it here, before any other processing, since this is the true entry point
        // of the raw source string.
        if (source != null && !source.isEmpty() && source.charAt(0) == '﻿') {
            source = source.substring(1);
        }

        try {
            PostgresParseErrorListener errors = new PostgresParseErrorListener();

            // PL/pgSQL routines are wrapped as: CREATE ... LANGUAGE plpgsql AS $tag$ <body> $tag$.
            // Strip that wrapper (blank-in-place so line numbers stay correct) to expose the
            // DECLARE/BEGIN…END body the block-structured grammar understands.
            String parseSrc = preprocess(source);

            // Lex
            CharStream chars = CharStreams.fromString(parseSrc);
            PostgresLexer lexer = new PostgresLexer(chars);
            lexer.removeErrorListeners();
            lexer.addErrorListener(errors);

            // Parse
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PostgresParser parser = new PostgresParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errors);

            ParseTree tree = parser.compilationUnit();

            // Visit
            PostgresAnalysisVisitor visitor = new PostgresAnalysisVisitor(objectName, schema);
            visitor.visit(tree);

            result = visitor.getResult();
            result.getErrors().addAll(errors.getErrors());

            // Ensure line numbers are set
            result.setLineStart(1);
            if (result.getLineEnd() == 0) {
                result.setLineEnd(countLines(source));
            }

            if (errors.hasErrors()) {
                log.debug("PostgresAnalysisEngine: {} syntax error(s) in {}",
                        errors.getErrors().size(), objectName);
            }

        } catch (Exception e) {
            log.warn("PostgresAnalysisEngine failure on {}: {}", objectName, e.getMessage(), e);
            result.getErrors().add("Parse error: " + e.getMessage());
        }

        return result;
    }

    /**
     * Strip the PL/pgSQL dollar-quote wrapper in place, preserving every newline so line
     * numbers in the analysis still match the original source. Removes
     *   "... LANGUAGE plpgsql AS $tag$"  (prefix)  and  the closing "$tag$ ..."  (suffix),
     * leaving "CREATE [OR REPLACE] PROC/FUNC name(params) [RETURNS t]  <DECLARE…BEGIN…END>".
     * If no dollar-quoted body is found the source is returned unchanged (already block form).
     */
    String preprocess(String source) {
        if (source == null) return null;
        // Bug 10 fix: scan a comment-blanked copy for the opening tag, not the raw source, so a
        // doc-comment merely mentioning "$$"/"$tag$" in prose can't be mistaken for the real
        // dollar-quote delimiter. Comments are blanked (not removed) so character offsets into
        // the real `source` stay valid for the blank() calls below.
        // …and likewise blank the *contents* of single-quoted literals, so a "$$"/"$tag$"
        // sequence written inside a string (e.g. v_msg := 'cost $$ high') cannot be mistaken for
        // the closing delimiter — which truncated the body at that point and produced a lexer
        // error. Both passes preserve every character offset, so the blank() calls below stay
        // aligned with the real `source`.
        String scanText = blankQuotedLiterals(blankComments(source));
        // The opening `$tag$` is located by the LEXER (PostgresLexer.DOLLAR_QUOTED_STRING) rather
        // than a regex over hand-blanked text. The lexer already skips comments and tokenizes
        // string literals correctly, so it cannot be fooled by a "$$"/"$tag$" written in prose or
        // inside a quoted string — the exact class of bug the blanking passes existed to defend
        // against. The legacy regex survives only as a counted fallback for input the lexer cannot
        // tokenize as a dollar-quoted string (e.g. an unbalanced body).
        int[] openTag = findDollarQuoteOpenTag(source);
        String tag;
        int openStart, openEnd;
        if (openTag != null) {
            openStart = openTag[0];
            openEnd = openTag[1];
            tag = source.substring(openStart, openEnd);
        } else {
            DOLLAR_QUOTE_LEXER_FALLBACKS.incrementAndGet();
            Matcher m = Pattern.compile("\\$[A-Za-z0-9_]*\\$").matcher(scanText);
            if (!m.find()) return source;             // not a $-quoted PL/pgSQL body
            tag = m.group();
            openStart = m.start();
            openEnd = m.end();
        }
        int closeStart = scanText.indexOf(tag, openEnd);
        if (closeStart < 0) return source;            // unbalanced — leave as-is

        String upper = source.toUpperCase();
        int wrapStart = lastKeywordBefore(upper, "LANGUAGE", openStart);
        if (wrapStart < 0) wrapStart = lastKeywordBefore(upper, "AS", openStart);
        if (wrapStart < 0) wrapStart = openStart;

        char[] c = source.toCharArray();
        blank(c, wrapStart, openEnd);                 // remove "LANGUAGE plpgsql AS $tag$"
        blank(c, closeStart, c.length);               // remove closing "$tag$" + trailing
        return new String(c);
    }

    /**
     * Returns a copy of {@code source} with "--" line comments and "/* ... *\/" block comments
     * blanked out to spaces (newlines preserved), leaving every other character — including
     * offsets — untouched. Used only to scan for the real dollar-quote tag without being fooled
     * by one merely mentioned in a comment.
     */
    private static String blankComments(String source) {
        char[] c = source.toCharArray();
        int n = c.length;
        int i = 0;
        boolean inSingleQuote = false;
        boolean inEscapeString = false;
        while (i < n) {
            char ch = c[i];
            if (!inSingleQuote && ch == '-' && i + 1 < n && c[i + 1] == '-') {
                int j = i;
                while (j < n && c[j] != '\n') { c[j] = ' '; j++; }
                i = j;
                continue;
            }
            if (!inSingleQuote && ch == '/' && i + 1 < n && c[i + 1] == '*') {
                int j = i;
                c[j] = ' '; c[j + 1] = ' ';
                j += 2;
                while (j + 1 < n && !(c[j] == '*' && c[j + 1] == '/')) {
                    if (c[j] != '\n' && c[j] != '\r') c[j] = ' ';
                    j++;
                }
                if (j + 1 < n) { c[j] = ' '; c[j + 1] = ' '; j += 2; }
                i = j;
                continue;
            }
            if (inSingleQuote && inEscapeString && ch == '\\' && i + 1 < n) {
                i += 2;                      // \' inside an E'…' literal does NOT close it
                continue;
            }
            if (ch == '\'') {
                inEscapeString = !inSingleQuote && isEscapeStringStart(c, i);
                inSingleQuote = !inSingleQuote;
            }
            i++;
        }
        return new String(c);
    }

    /**
     * True when the single quote at {@code q} opens a PostgreSQL C-style escape string
     * ({@code E'…'} / {@code e'…'}), i.e. it is directly preceded by a standalone E/e. Inside
     * such a literal a backslash escapes the next character, so {@code E'x\'y'} is ONE literal —
     * the quote-counting scans below would otherwise desynchronize from that point on and
     * mis-locate the dollar-quote body delimiters.
     */
    private static boolean isEscapeStringStart(char[] c, int q) {
        if (q == 0) return false;
        char p = c[q - 1];
        if (p != 'e' && p != 'E') return false;
        if (q < 2) return true;
        char pp = c[q - 2];
        return !(Character.isLetterOrDigit(pp) || pp == '_' || pp == '$');
    }

    /**
     * Returns a copy of {@code source} with the interior of every single-quoted literal blanked
     * to spaces (the quotes themselves, and all line breaks, are kept so offsets and line numbers
     * are unchanged). Doubled '' escapes stay inside the literal. Used only for locating the real
     * dollar-quote delimiters; the parser always sees the untouched source.
     */
    private static String blankQuotedLiterals(String source) {
        char[] c = source.toCharArray();
        int n = c.length;
        int i = 0;
        while (i < n) {
            if (c[i] != '\'') { i++; continue; }
            boolean escapeString = isEscapeStringStart(c, i);
            int j = i + 1;
            while (j < n) {
                if (escapeString && c[j] == '\\' && j + 1 < n) {
                    c[j] = ' ';
                    if (c[j + 1] != '\n' && c[j + 1] != '\r') c[j + 1] = ' ';
                    j += 2;                                  // \' does not close an E'…' literal
                    continue;
                }
                if (c[j] == '\'') {
                    if (j + 1 < n && c[j + 1] == '\'') { c[j] = ' '; c[j + 1] = ' '; j += 2; continue; }
                    break;                                   // closing quote
                }
                if (c[j] != '\n' && c[j] != '\r') c[j] = ' ';
                j++;
            }
            i = j + 1;                                       // resume after the closing quote
        }
        return new String(c);
    }

    /** Replace [from,to) with spaces, keeping line breaks so offsets/lines are preserved. */
    private void blank(char[] c, int from, int to) {
        for (int i = Math.max(0, from); i < to && i < c.length; i++) {
            if (c[i] != '\n' && c[i] != '\r') c[i] = ' ';
        }
    }

    /** Last whole-word occurrence of kw at or before `before`, else -1. */
    private int lastKeywordBefore(String upper, String kw, int before) {
        int idx = -1, at = 0;
        while ((at = upper.indexOf(kw, at)) >= 0 && at < before) {
            if (isKeywordAt(upper, at, kw)) idx = at;
            at++;
        }
        return idx;
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
