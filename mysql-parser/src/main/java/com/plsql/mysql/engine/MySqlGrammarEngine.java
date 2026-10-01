package com.plsql.mysql.engine;

import com.plsql.mysql.grammar.MySqlLexer;
import com.plsql.mysql.grammar.MySqlParser;
import com.plsql.mysql.model.*;
import com.plsql.mysql.visitor.MySqlAnalysisVisitor;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the MySQL grammar-based parser.
 *
 * Returns a self-contained MySqlRoutineInfo — it does NOT depend on the
 * plsql-parser module, eliminating any circular Maven dependency.
 * The caller (MySqlProcedureParser in plsql-parser) converts the result
 * to the shared ParseResult model.
 *
 * Flow:
 *   source text
 *     → MySqlLexer  (ANTLR4 lexer from MySqlLexer.g4)
 *     → MySqlParser (ANTLR4 parser from MySqlParser.g4)
 *     → MySqlAnalysisVisitor (extract calls, table ops, variables, cursors)
 *     → MySqlRoutineInfo
 */
public class MySqlGrammarEngine {

    private static final Logger log = LoggerFactory.getLogger(MySqlGrammarEngine.class);

    /**
     * Parse a single MySQL PROCEDURE or FUNCTION source.
     *
     * @param source     full CREATE PROCEDURE / FUNCTION text from INFORMATION_SCHEMA
     * @param objectName fallback name (used if grammar cannot resolve it)
     * @param schema     owner / database name
     */
    public MySqlRoutineInfo parse(String source, String objectName, String schema) {
        MySqlParseErrorListener errors = new MySqlParseErrorListener();

        try {
            // ── Preprocess ───────────────────────────────────────────────────
            // DELIMITER is a mysql-CLIENT directive, not real SQL grammar - real MySQL
            // (and INFORMATION_SCHEMA.ROUTINES) never sees it. Strip it out (and normalize
            // any custom delimiter back to ';') before the ANTLR lexer ever sees the text,
            // the same way PostgresAnalysisEngine.preprocess() handles dollar-quoting.
            String preprocessed = preprocess(source);

            // ── Lex ──────────────────────────────────────────────────────────
            CharStream chars = CharStreams.fromString(preprocessed);
            MySqlLexer lexer = new MySqlLexer(chars);
            lexer.removeErrorListeners();
            lexer.addErrorListener(errors);

            // ── Parse ─────────────────────────────────────────────────────────
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            MySqlParser parser = new MySqlParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errors);

            ParseTree tree = parser.compilationUnit();

            // ── Visit ─────────────────────────────────────────────────────────
            MySqlAnalysisVisitor visitor = new MySqlAnalysisVisitor(objectName, schema);
            visitor.visit(tree);

            MySqlRoutineInfo result = visitor.getResult();
            result.getErrors().addAll(errors.getErrors());

            if (errors.hasErrors()) {
                log.debug("MySqlGrammarEngine: {} syntax error(s) in {}",
                        errors.getErrors().size(), objectName);
            }
            return result;

        } catch (Exception e) {
            log.warn("MySqlGrammarEngine hard failure on {}: {}", objectName, e.getMessage(), e);
            MySqlRoutineInfo fallback = new MySqlRoutineInfo();
            fallback.setName(objectName);
            fallback.setSchema(schema);
            fallback.setType(MySqlRoutineInfo.RoutineType.PROCEDURE);
            fallback.getErrors().add("Grammar engine error: " + e.getMessage());
            fallback.getErrors().addAll(errors.getErrors());
            return fallback;
        }
    }

    // ── Why DELIMITER handling stays a PRE-LEX step (not a MySqlLexer.g4 rule) ──────────────────
    // Batch 3 assessed moving this to a lexer rule and deliberately did not, because the construct
    // is not expressible as one:
    //   * DELIMITER is a mysql-CLIENT directive, not SQL — the server (and INFORMATION_SCHEMA,
    //     which is where this engine's source comes from) never sees it. A lexer rule could at
    //     best tokenize the directive; it could not make the parser skip it without a new grammar
    //     production for a non-SQL line.
    //   * The real work is STATEFUL substitution: after `DELIMITER $$`, every subsequent `$$` that
    //     terminates a statement must become `;` until the next directive. A lexer rule cannot
    //     rewrite matched text, and a lexer MODE would need the arbitrary, runtime-chosen
    //     delimiter string (`$$`, `//`, `;;`, …) as a mode trigger, which ANTLR cannot parameterise
    //     per match.
    //   * The substitution must also be quote/comment-aware (a `$$` inside a string literal or a
    //     comment is not a terminator) — see replaceDelimiterOutsideLiterals, which carries that
    //     state machine. That is precisely the kind of cross-line, context-sensitive rewrite a
    //     lexer cannot do.
    // So the pre-lex step is the correct home for it; this is a documented, justified retention,
    // not an un-migrated gap. (Contrast the Postgres dollar-quote case, which WAS moved to a lexer
    // rule: there the delimiter is self-describing and needs no substitution.)
    private static final java.util.regex.Pattern DELIMITER_LINE =
            // The trailing `(?:(?:--|#).*|/\*.*)?` tail lets a DELIMITER directive carry an
            // end-of-line comment (`DELIMITER $$ -- switch for the routine body`), a common way
            // of writing these in checked-in scripts. Without it the line did not match, the
            // directive survived into the token stream, and the ANTLR parser failed on the very
            // first token — losing the entire routine rather than one line.
            java.util.regex.Pattern.compile(
                    "^\\s*DELIMITER\\s+(\\S+)\\s*(?:(?:--|#).*|/\\*.*)?$",
                    java.util.regex.Pattern.CASE_INSENSITIVE);

    /**
     * Strips mysql-client-only "DELIMITER xxx" directive lines and normalizes any custom
     * statement terminator declared via them back to ';' for the remainder of the source, so
     * the ANTLR grammar (which only knows real MySQL SQL syntax) never sees either construct.
     * Line count is preserved (directive lines become blank) so downstream line numbers stay
     * accurate.
     */
    static String preprocess(String source) {
        if (source == null) {
            return null;
        }
        // Strip a leading byte-order-mark BEFORE any other processing (including the
        // DELIMITER-directive scan below). U+FEFF is not whitespace as far as \s is concerned,
        // so leaving it in place breaks both the ^\s*DELIMITER... regex anchor (when a BOM
        // precedes a DELIMITER directive) and the ANTLR lexer itself (when it precedes ordinary
        // SQL), since no lexer rule recognizes it. Stripping it here, at the true entry point of
        // preprocessing, fixes both cases uniformly.
        if (!source.isEmpty() && source.charAt(0) == '﻿') {
            source = source.substring(1);
        }
        if (source.indexOf("DELIMITER") < 0 && source.indexOf("delimiter") < 0) {
            return source;
        }
        String[] lines = source.split("\n", -1);
        StringBuilder out = new StringBuilder(source.length());
        String currentDelimiter = ";";
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            java.util.regex.Matcher m = DELIMITER_LINE.matcher(line);
            if (m.matches()) {
                currentDelimiter = m.group(1);
                // drop the directive but keep the line for line-number stability
            } else if (!";".equals(currentDelimiter) && line.contains(currentDelimiter)) {
                DelimiterScan scan = replaceDelimiterOutsideLiterals(line, currentDelimiter);
                out.append(scan.text());
                if (scan.newDelimiter() != null) {
                    currentDelimiter = scan.newDelimiter();
                }
            } else {
                out.append(line);
            }
            if (i < lines.length - 1) {
                out.append('\n');
            }
        }
        return out.toString();
    }

    /**
     * Replaces every occurrence of {@code delimiter} in {@code line} with {@code ;}, EXCEPT
     * occurrences that fall inside an active single- or double-quoted string literal, or inside
     * a {@code --}/{@code #} line comment or a {@code /* ... *}{@code /} block comment.
     *
     * <p>The naive {@code line.replace(delimiter, ";")} this replaces corrupted string-literal
     * content whenever the custom delimiter text (e.g. {@code $$}) coincidentally also appeared
     * inside a quoted string on the same line — see MySqlWhitespaceEncodingAuditTest for the
     * reproduction. This mirrors the quote/comment state-machine style used by
     * {@code StatementTextCapture}'s forward scan, adapted to scan forward for a
     * multi-character delimiter within a single line rather than a single {@code ;} terminator.
     *
     * <p>The scan doubles as the only safe place to spot a {@code DELIMITER} directive that is
     * <b>not</b> the first thing on its line — the widely-written {@code END$$ DELIMITER ;}. A bare
     * {@code ^\s*DELIMITER} line regex cannot see that one, so the directive survived into the
     * token stream and the parser died on it ("mismatched input 'DELIMITER'"), losing the routine.
     * Recognizing it here rather than with a looser regex is what makes it safe: we only test for
     * the directive at a point the state machine has already proven to be <i>outside</i> every
     * string literal and comment <i>and</i> immediately after a statement terminator, and we
     * additionally require the rest of the line to be nothing but {@code DELIMITER <token>} (plus
     * an optional trailing comment). A column, alias or literal that merely contains the text
     * "DELIMITER" cannot reach that position in valid SQL, so nothing legitimate is mangled.
     */
    private record DelimiterScan(String text, String newDelimiter) { }

    private static DelimiterScan replaceDelimiterOutsideLiterals(String line, String delimiter) {
        String newDelimiter = null;
        int n = line.length();
        int dlen = delimiter.length();
        if (dlen == 0) return new DelimiterScan(line, null);
        StringBuilder out = new StringBuilder(n);
        char quote = 0;
        int i = 0;
        while (i < n) {
            char ch = line.charAt(i);
            if (quote != 0) {
                // Backslash-escape: MySQL string literals (default sql_mode) treat \X as an
                // escaped literal character, so the escaped char can never end/toggle the quote.
                if (ch == '\\' && i + 1 < n) {
                    out.append(ch).append(line.charAt(i + 1));
                    i += 2;
                    continue;
                }
                if (ch == quote) {
                    // Doubled-quote escape ('' or ""): still inside the string, not the closer.
                    if (i + 1 < n && line.charAt(i + 1) == quote) {
                        out.append(ch).append(quote);
                        i += 2;
                        continue;
                    }
                    quote = 0;
                }
                out.append(ch);
                i++;
                continue;
            }
            if (ch == '\'' || ch == '"') {
                quote = ch;
                out.append(ch);
                i++;
                continue;
            }
            if ((ch == '-' && i + 1 < n && line.charAt(i + 1) == '-'
                        // Same rule as the LINE_COMMENT lexer rule: MySQL only starts a `--`
                        // comment when a whitespace/control char (or end of line) follows the
                        // second dash. `SET v = 10--3$$` is arithmetic, not a comment, and its
                        // trailing `$$` terminator still has to be normalized.
                        && (i + 2 >= n || line.charAt(i + 2) == ' ' || line.charAt(i + 2) == '\t'
                            || line.charAt(i + 2) == '\r'))
                    || ch == '#') {
                // Rest of the line is a line comment; delimiter occurrences there are inert.
                out.append(line, i, n);
                break;
            }
            if (ch == '/' && i + 1 < n && line.charAt(i + 1) == '*') {
                int end = line.indexOf("*/", i + 2);
                if (end < 0) {
                    out.append(line, i, n);
                    break;
                }
                out.append(line, i, end + 2);
                i = end + 2;
                continue;
            }
            if (i + dlen <= n && line.regionMatches(i, delimiter, 0, dlen)) {
                out.append(';');
                i += dlen;
                // `END$$ DELIMITER ;` — the directive trails a statement terminator instead of
                // starting the line. We are provably outside strings/comments here, so testing the
                // remainder against the same strict directive pattern is safe.
                java.util.regex.Matcher tail = DELIMITER_LINE.matcher(line.substring(i));
                if (tail.matches()) {
                    newDelimiter = tail.group(1);
                    break;
                }
                continue;
            }
            out.append(ch);
            i++;
        }
        return new DelimiterScan(out.toString(), newDelimiter);
    }
}
