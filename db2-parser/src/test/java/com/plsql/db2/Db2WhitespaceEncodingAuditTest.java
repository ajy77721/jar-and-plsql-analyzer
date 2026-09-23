package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2RoutineInfo;
import com.plsql.db2.model.Db2TableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep18, DB2 — read-only audit evidence for whitespace / line-ending / encoding edge cases in
 * {@code StatementTextCapture.capture()} (used via {@code Db2AnalysisVisitor.originalText}).
 *
 * <p>This class makes NO fixes; it is evidence only. Each test records the ACTUAL observed
 * behavior of the real grammar/visitor against a realistic snippet, with an assertion that either
 * pins the correct behavior (if it holds) or documents the wrong one (left failing/commented with
 * the finding, per audit convention we assert the actual observed fact so the test is green and
 * durable evidence, with a comment stating what would be "correct").
 */
class Db2WhitespaceEncodingAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String body) {
        return engine.parse(
                "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN\n" + body + "\nEND", "p", null);
    }

    private Db2RoutineInfo parseRaw(String fullSource) {
        return engine.parse(fullSource, "p", null);
    }

    private List<String> allSql(Db2RoutineInfo r) {
        return r.getTableOps().stream()
                .map(Db2TableOpInfo::getSqlText)
                .filter(Objects::nonNull)
                .toList();
    }

    private String firstSql(Db2RoutineInfo r) {
        return allSql(r).stream().findFirst().orElse(null);
    }

    // ------------------------------------------------------------------
    // 1. CRLF vs LF
    // ------------------------------------------------------------------

    @Test
    @DisplayName("1a: CRLF line-comment terminates at \\n exactly like LF (no bleed into next line)")
    void crlfLineCommentTerminatesCorrectly() {
        // A `-- comment` followed by CRLF, then a real WHERE clause on the next physical line.
        String bodyCrlf =
                "  SELECT id -- trailing note\r\n"
              + "    FROM ERP_SALES_ORDER WHERE id = 7;";
        String bodyLf = bodyCrlf.replace("\r\n", "\n");

        Db2RoutineInfo rCrlf = parse(bodyCrlf);
        Db2RoutineInfo rLf = parse(bodyLf);

        String sqlCrlf = firstSql(rCrlf);
        String sqlLf = firstSql(rLf);

        assertNotNull(sqlCrlf, "CRLF variant produced no captured SQL at all");
        assertNotNull(sqlLf, "LF variant produced no captured SQL at all");

        // CORRECT expectation: the comment ends at the \n; the WHERE clause on the following line
        // is real code and must be present in both variants, with the CR (if present at all)
        // being an internal, harmless artifact of the captured slice, not swallowing code.
        assertTrue(sqlCrlf.toUpperCase(Locale.ROOT).contains("WHERE"),
                "CRLF: line-comment state leaked past the newline and swallowed the WHERE clause: "
                        + sqlCrlf);
        assertTrue(sqlLf.toUpperCase(Locale.ROOT).contains("WHERE"),
                "LF: line-comment state leaked past the newline and swallowed the WHERE clause: "
                        + sqlLf);

        // ACTUAL: verified equal modulo the literal \r characters the CRLF source contributes.
        assertEquals(sqlLf, sqlCrlf.replace("\r", ""),
                "CRLF and LF captures differ beyond the raw \\r bytes: crlf=[" + sqlCrlf
                        + "] lf=[" + sqlLf + "]");
    }

    @Test
    @DisplayName("1b: CRLF line numbers match LF line numbers (ANTLR's own lexer, not the capture scan)")
    void crlfLineNumbersMatchLf() {
        String bodyCrlf =
                "  SET v_x = 1;\r\n"
              + "  SET v_y = 2;\r\n"
              + "  SELECT id INTO v_id FROM ERP_SALES_ORDER WHERE id = v_y;";
        String bodyLf = bodyCrlf.replace("\r\n", "\n");

        Db2RoutineInfo rCrlf = parse(bodyCrlf);
        Db2RoutineInfo rLf = parse(bodyLf);

        // ACTUAL/CORRECT: ANTLR's default line counting advances only on '\n', so CRLF and LF
        // sources must report identical line numbers for the corresponding statements.
        List<Integer> linesCrlf = rCrlf.getTableOps().stream().map(Db2TableOpInfo::getLine).toList();
        List<Integer> linesLf = rLf.getTableOps().stream().map(Db2TableOpInfo::getLine).toList();
        assertEquals(linesLf, linesCrlf,
                "line numbers differ between CRLF and LF sources: crlf=" + linesCrlf
                        + " lf=" + linesLf);
    }

    // ------------------------------------------------------------------
    // 2. Mixed tabs and spaces
    // ------------------------------------------------------------------

    @Test
    @DisplayName("2: mixed tabs/spaces indentation does not corrupt captured text")
    void mixedTabsAndSpacesPreserved() {
        String body =
                "\tSELECT id\n"
              + "  \t FROM ERP_SALES_ORDER\n"
              + "\t  WHERE id = 7;";
        Db2RoutineInfo r = parse(body);
        String sql = firstSql(r);
        assertNotNull(sql, "mixed tab/space statement produced no captured SQL");
        assertTrue(sql.toUpperCase(Locale.ROOT).contains("WHERE"),
                "mixed indentation truncated the statement: " + sql);
        // CORRECT: tabs are ordinary characters to the scanner; they must appear verbatim.
        assertTrue(sql.indexOf('\t') >= 0,
                "tabs were stripped/normalized out of the captured text: [" + sql + "]");
    }

    // ------------------------------------------------------------------
    // 3. Non-ASCII in comments, string literals, delimited identifiers
    // ------------------------------------------------------------------

    @Test
    @DisplayName("3a: non-ASCII text inside a -- comment does not corrupt capture or line tracking")
    void nonAsciiInLineComment() {
        String body =
                "  SELECT id -- café 中文 😀 note\n"
              + "    FROM ERP_SALES_ORDER WHERE id = 7;";
        Db2RoutineInfo r = parse(body);
        String sql = firstSql(r);
        assertNotNull(sql, "non-ASCII line comment produced no captured SQL");
        assertTrue(sql.toUpperCase(Locale.ROOT).contains("WHERE"),
                "non-ASCII comment content broke comment-state tracking: " + sql);
    }

    @Test
    @DisplayName("3b: non-ASCII text inside a string literal survives capture intact")
    void nonAsciiInStringLiteral() {
        // A bare `SET v_name = '...'` with no table reference produces no tableOp at all (by
        // design — it is not a table operation), so the literal must sit inside a statement that
        // does reference a table for the capture path under audit to run at all.
        String body =
                "  INSERT INTO ERP_SALES_ORDER (id, notes) VALUES (7, 'Español 中文 — nota');";
        Db2RoutineInfo r = parse(body);
        List<String> sqls = allSql(r);
        assertFalse(sqls.isEmpty(), "INSERT with non-ASCII string literal produced no captured SQL");
        // CORRECT: the literal, including its non-ASCII payload, is captured byte-for-char intact.
        assertTrue(sqls.stream().anyMatch(s -> s.contains("Español 中文")),
                "non-ASCII payload inside the string literal was corrupted or dropped: " + sqls);
    }

    @Test
    @DisplayName("3c: non-ASCII inside a delimited identifier does not corrupt capture")
    void nonAsciiInDelimitedIdentifier() {
        String body =
                "  SELECT \"col_é\" FROM ERP_SALES_ORDER WHERE id = 7;";
        Db2RoutineInfo r = parse(body);
        String sql = firstSql(r);
        assertNotNull(sql, "delimited identifier with non-ASCII char produced no captured SQL");
        assertTrue(sql.toUpperCase(Locale.ROOT).contains("WHERE"),
                "non-ASCII delimited identifier broke capture: " + sql);
    }

    // ------------------------------------------------------------------
    // 4. Byte-order-mark (BOM) at file start
    // ------------------------------------------------------------------

    @Test
    @DisplayName("4: a leading BOM character before CREATE PROCEDURE")
    void leadingBomCharacter() {
        String withBom = "﻿" + "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN\n"
                + "  SELECT id INTO v_id FROM ERP_SALES_ORDER WHERE id = 7;\n"
                + "END";
        Db2RoutineInfo r = parseRaw(withBom);

        // FIXED: Db2AnalysisEngine.parse() now strips a leading BOM before CharStreams.fromString,
        // so it no longer reaches the lexer as an unrecognized character and produces zero errors.
        boolean gotName = "p".equals(r.getName()) || r.getName() != null;
        assertTrue(gotName, "routine info missing entirely for BOM-prefixed source");
        assertEquals(java.util.List.of(), r.getErrors(),
                "leading BOM must now be stripped with zero errors, got: " + r.getErrors());
        System.out.println("[BOM audit] errors=" + r.getErrors()
                + " tableOps=" + r.getTableOps().size());
    }

    // ------------------------------------------------------------------
    // 5. Long single-line statement + multi-line statement with mixed comments over CRLF
    // ------------------------------------------------------------------

    @Test
    @DisplayName("5a: a very long single-line statement is captured in full")
    void veryLongSingleLineStatement() {
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < 400; i++) {
            if (i > 0) cols.append(", ");
            cols.append("col").append(i);
        }
        String body = "  SELECT " + cols + " FROM ERP_SALES_ORDER WHERE id = 7;";
        Db2RoutineInfo r = parse(body);
        String sql = firstSql(r);
        assertNotNull(sql, "very long single-line statement produced no captured SQL");
        assertTrue(sql.contains("col399"),
                "long single-line statement was truncated before its last column: " + sql);
        assertTrue(sql.toUpperCase(Locale.ROOT).contains("WHERE"),
                "long single-line statement lost its WHERE clause: " + sql);
    }

    @Test
    @DisplayName("5b: many CRLF lines with mixed -- and /* */ comments interleaved with real code")
    void manyLinesMixedCommentsOverCrlf() {
        String bodyCrlf =
                "  SET v_rate = ( -- start of subquery\r\n"
              + "    SELECT /* pick the max */ COALESCE(MAX(rate), 0)\r\n"
              + "    FROM ERP_TAX_RATE -- table with the rate schedule\r\n"
              + "    /* only rows\r\n"
              + "       for this country, spanning\r\n"
              + "       several physical CRLF lines */\r\n"
              + "    WHERE country_id = p_country_id -- final predicate\r\n"
              + "  );";
        Db2RoutineInfo r = parse(bodyCrlf);
        String sql = firstSql(r);
        assertNotNull(sql, "multi-line CRLF statement with mixed comments produced no captured SQL");
        String up = sql.toUpperCase(Locale.ROOT);
        assertTrue(up.contains("WHERE COUNTRY_ID"),
                "CRLF multi-line comment tracking lost the real WHERE clause: " + sql);
        long open = sql.chars().filter(c -> c == '(').count();
        long close = sql.chars().filter(c -> c == ')').count();
        assertTrue(close <= open,
                "capture over CRLF + mixed comments closes a paren it never opened: " + sql);
    }

    @Test
    @DisplayName("control: same statement WITHOUT the comment-embedded paren, for comparison")
    void parenControlWithoutComment() {
        String body =
                "  SET v_rate = (SELECT COALESCE(MAX(rate),0)\n"
              + "                FROM ERP_TAX_RATE WHERE country_id = p_country_id);";
        Db2RoutineInfo r = parse(body);
        String sql = firstSql(r);
        System.out.println("[control, no comment] sql=[" + sql + "]");
    }

    // ------------------------------------------------------------------
    // Bonus finding surfaced while auditing: parenDepth() does not skip comments.
    // ------------------------------------------------------------------

    @Test
    @DisplayName("BUG: parenDepth() counts a '(' inside a comment within ctx's own span as real depth")
    void parenInsideCommentWithinContextSpanCorruptsDepth() {
        // The context's own matched span (before the forward scan even starts) contains a
        // line comment with a stray, unmatched '(' character. parenDepth(cs, start, stop) only
        // special-cases string-literal quoting, not '--' or '/* */' comments, so it counts that
        // '(' as a real open paren. The forward scan then believes it is one level deeper than
        // it really is, and a subsequent real ')' that legitimately closes the SET's own value
        // subquery is swallowed as merely "closing" the phantom depth instead of ending capture.
        String body =
                "  SET v_rate = (SELECT COALESCE(MAX(rate),0) -- odd paren ( in a comment\n"
              + "                FROM ERP_TAX_RATE WHERE country_id = p_country_id);";
        Db2RoutineInfo r = parse(body);
        String sql = firstSql(r);
        assertNotNull(sql, "statement with a comment-embedded '(' produced no captured SQL");

        long open = sql.chars().filter(c -> c == '(').count();
        long close = sql.chars().filter(c -> c == ')').count();

        // CORRECT expectation: parens inside comments must not influence real paren-depth
        // accounting; the capture should end right after the SET's own closing ')', keeping the
        // text balanced.
        // ACTUAL: recorded here as observed. If this assertion fails, it is confirmation that the
        // phantom '(' inside the comment caused the scan to run past the real end of the
        // statement (into the next statement or to end-of-block) looking for one more ')' to
        // close a depth that was never really open, OR the capture is truncated early.
        System.out.println("[parenDepth/comment audit] sql=[" + sql + "] open=" + open
                + " close=" + close);

        // The control case (identical statement, no comment) below correctly stops the capture
        // right BEFORE the SET's own closing ')' — that paren belongs to the enclosing SET
        // assignment, not to the subquery, and including it would leave the captured text
        // ending in a dangling, unopened ')'.
        String controlBody =
                "  SET v_rate = (SELECT COALESCE(MAX(rate),0)\n"
              + "                FROM ERP_TAX_RATE WHERE country_id = p_country_id);";
        String controlSql = firstSql(parse(controlBody));
        assertNotNull(controlSql);
        assertFalse(controlSql.trim().endsWith(")"),
                "control expectation violated: capture normally excludes the enclosing SET's own "
                        + "closing paren: " + controlSql);

        // FIXED: parenDepth() now tracks comment state exactly like capture()'s forward scan, so
        // the '(' inside the `-- odd paren (` comment is skipped and never counted as real depth.
        // The capture therefore stops in the same place as the comment-free control above — right
        // before the SET's own closing ')' — matching it exactly except for the comment text.
        assertFalse(sql.trim().endsWith(")"),
                "parenDepth() must ignore parens inside comments; the capture should not swallow "
                        + "the enclosing SET's closing paren: " + sql);
    }
}
