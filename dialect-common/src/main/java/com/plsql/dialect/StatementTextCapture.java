package com.plsql.dialect;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import java.util.Set;

/**
 * Extracts the source text of a statement for the column enricher.
 *
 * <h2>Why this is shared rather than copied</h2>
 *
 * <p>The PostgreSQL and DB2 visitors held byte-identical copies of this logic, and both copies
 * carried the same two defects — a capture that ran past the read into the loop body around it, and
 * one that closed a parenthesis it never opened. Each produced text that is not a statement in any
 * dialect, so the enricher parsed nothing and the row lost <em>all</em> of its column detail rather
 * than part of it. 249 + 81 statements on PostgreSQL and 54 + 19 on DB2, measured.
 *
 * <p>Both were then fixed twice, which is the argument for this class existing: a defect that has
 * to be fixed in two places will eventually be fixed in one. The dialect modules are otherwise
 * deliberately independent, so this stays small and depends on nothing but the ANTLR runtime.
 *
 * <h2>Why the text must be extended past the context at all</h2>
 *
 * <p>A dialect's DML rule typically ends before the {@code SET}/{@code WHERE} clauses, so the
 * context's own interval yields {@code "UPDATE orders"} — which parses to nothing useful. The scan
 * therefore continues past {@code ctx.getStop()} and stops at the first boundary that means the
 * statement has ended.
 */
public final class StatementTextCapture {

    private StatementTextCapture() {}

    /**
     * Keywords that end a captured statement.
     *
     * <p>{@code THEN} and {@code END} are deliberately <b>absent</b>, and callers should not add
     * them: both occur inside a {@code CASE … END} expression in a select list or WHERE clause, so
     * cutting there truncates ordinary valid SQL. That would trade a known set of failures for an
     * unknown number of new ones — the worse deal, since the new ones would be statements that
     * parse correctly today.
     */
    public static final Set<String> DEFAULT_BLOCK_KEYWORDS = Set.of("LOOP", "BEGIN");

    /** DB2 spells its loop opener {@code FOR … DO}, so it needs one more. */
    public static final Set<String> DB2_BLOCK_KEYWORDS = Set.of("LOOP", "BEGIN", "DO");

    /**
     * The statement text for {@code ctx}, or null when it cannot be determined.
     *
     * @param blockKeywords words that terminate the capture; see {@link #DEFAULT_BLOCK_KEYWORDS}.
     */
    public static String capture(ParserRuleContext ctx, Set<String> blockKeywords) {
        if (ctx == null || ctx.getStart() == null) return null;
        try {
            CharStream cs = ctx.getStart().getInputStream();
            if (cs == null) return null;
            int start = ctx.getStart().getStartIndex();
            int stop = ctx.getStop() != null ? ctx.getStop().getStopIndex() : start;
            int size = cs.size();

            // Bug fix: some dialect rules (e.g. DB2's insert/update/deleteStatement, which end
            // in an optional SEMI that IS part of the matched grammar rule) already have their
            // own trailing `;` as ctx's very last token, so ctx.getStop() already marks the true
            // end of the statement. The forward scan below exists only for rules whose context
            // stops short of the real end (e.g. UPDATE ending before its own WHERE clause) and
            // must find the *next* terminator to reach it — but blindly starting that scan at
            // stop+1 when the terminator is already inside ctx just runs straight into the
            // following statement's text and stops at *its* semicolon instead, bleeding one
            // statement's sqlText into the next (and the last statement's into a trailing END).
            // If the character at the context's own stop index is already the terminator, the
            // capture is complete — skip the forward scan entirely.
            if (stop >= start && stop < size && charAt(cs, stop) == ';') {
                return cs.getText(Interval.of(start, stop));
            }

            int i = stop + 1;
            char quote = 0;
            int wordStart = -1;
            boolean inLineComment = false;
            boolean inBlockComment = false;
            // Depth of the text captured so far. A MERGE's source subquery gives a context that
            // stops before its closing paren; without this the scan runs out through it and the
            // captured text closes a paren it never opened.
            int depth = parenDepth(cs, start, stop);

            while (i < size) {
                char ch = charAt(cs, i);
                // Bug 8 fix: a `;` (or any other terminator-like character) inside a `--` line
                // comment or a `/* ... */` block comment is not a real statement terminator and
                // must not end the scan early — track comment state alongside quote state.
                if (inLineComment) {
                    if (ch == '\n') inLineComment = false;
                    i++;
                    continue;
                }
                if (inBlockComment) {
                    if (ch == '*' && i + 1 < size && charAt(cs, i + 1) == '/') {
                        inBlockComment = false;
                        i += 2;
                        continue;
                    }
                    i++;
                    continue;
                }
                if (quote == 0 && ch == '-' && i + 1 < size && charAt(cs, i + 1) == '-') {
                    inLineComment = true;
                    wordStart = -1;
                    i += 2;
                    continue;
                }
                if (quote == 0 && ch == '/' && i + 1 < size && charAt(cs, i + 1) == '*') {
                    inBlockComment = true;
                    wordStart = -1;
                    i += 2;
                    continue;
                }
                if (quote != 0) {
                    if (ch == quote) quote = 0;
                } else if (ch == '\'' || ch == '"') {
                    quote = ch;
                } else if (ch == ';') {
                    break;
                } else if (ch == '(') {
                    depth++;
                    wordStart = -1;
                } else if (ch == ')') {
                    if (depth == 0) {
                        i = Math.max(start, i - 1);   // stop before the stray close-paren
                        break;
                    }
                    depth--;
                    wordStart = -1;
                } else if (Character.isLetter(ch) || ch == '_') {
                    if (wordStart < 0) wordStart = i;
                } else if (wordStart >= 0) {
                    if (isBlockKeyword(cs, wordStart, i - 1, blockKeywords)) {
                        // Stop *before* the keyword: the extraction below is inclusive, so cutting
                        // at wordStart would leave its first letter dangling on the end of the SQL
                        // — an unparseable statement produced by the code meant to prevent one.
                        i = Math.max(start, wordStart - 1);
                        break;
                    }
                    wordStart = -1;
                }
                i++;
            }
            int end = Math.min(i, size - 1);
            if (end < start) return null;
            return cs.getText(Interval.of(start, end));
        } catch (Exception e) {
            return null;   // text is a bonus; never fail the parse for it
        }
    }

    /**
     * Net parenthesis depth of {@code [from, to]}, ignoring parens inside string literals and
     * inside line comments or block comments.
     */
    public static int parenDepth(CharStream cs, int from, int to) {
        int depth = 0;
        char quote = 0;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        for (int i = from; i <= to && i < cs.size(); i++) {
            char ch = charAt(cs, i);
            if (inLineComment) {
                if (ch == '\n') inLineComment = false;
                continue;
            }
            if (inBlockComment) {
                if (ch == '*' && i + 1 <= to && i + 1 < cs.size() && charAt(cs, i + 1) == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }
            if (quote == 0 && ch == '-' && i + 1 <= to && i + 1 < cs.size() && charAt(cs, i + 1) == '-') {
                inLineComment = true;
                i++;
                continue;
            }
            if (quote == 0 && ch == '/' && i + 1 <= to && i + 1 < cs.size() && charAt(cs, i + 1) == '*') {
                inBlockComment = true;
                i++;
                continue;
            }
            if (quote != 0) {
                if (ch == quote) quote = 0;
            } else if (ch == '\'' || ch == '"') {
                quote = ch;
            } else if (ch == '(') {
                depth++;
            } else if (ch == ')' && depth > 0) {
                depth--;
            }
        }
        return depth;
    }

    private static boolean isBlockKeyword(CharStream cs, int from, int to, Set<String> keywords) {
        if (from < 0 || to < from) return false;
        return keywords.contains(
                cs.getText(Interval.of(from, to)).toUpperCase(java.util.Locale.ROOT));
    }

    private static char charAt(CharStream cs, int i) {
        return cs.getText(Interval.of(i, i)).charAt(0);
    }
}
