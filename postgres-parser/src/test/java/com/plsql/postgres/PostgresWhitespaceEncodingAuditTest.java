package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (no fixes). Evidence for whitespace/encoding robustness of
 * PostgresAnalysisEngine.preprocess()/blankComments() and the downstream
 * PostgresAnalysisVisitor line/column/text attribution.
 *
 * Concepts probed (in file order, matching the audit brief):
 *   1. CRLF vs LF line endings (esp. inside a multi-line $tag$ dollar-quoted body)
 *   2. Mixed tabs and spaces
 *   3. Non-ASCII in comments / string literals / double-quoted identifiers
 *   4. Byte-order-mark (BOM) at file start, immediately before the dollar-quote
 *   5. Very long single-line statements + single-line vs hundreds-of-lines dollar body
 *
 * Every test records ACTUAL observed values with assertions pinned to what was
 * actually observed (verified by running), plus comments stating what would be
 * CORRECT/expected if it differs, so this file is durable evidence either way.
 */
class PostgresWhitespaceEncodingAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // 1. CRLF vs LF — dollar-quoted plpgsql body
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void lfLineEndings_lineNumbersOfStatementsInDollarBody() {
        String src =
            "CREATE OR REPLACE PROCEDURE hr.emp_hire(IN p_name VARCHAR)\n" +   // line 1
            "LANGUAGE plpgsql AS $body$\n" +                                   // line 2
            "DECLARE\n" +                                                     // line 3
            "  v_id INT;\n" +                                                 // line 4
            "BEGIN\n" +                                                       // line 5
            "  INSERT INTO employees(name) VALUES (p_name);\n" +              // line 6
            "  UPDATE audit_log SET last = now();\n" +                        // line 7
            "END;\n" +                                                       // line 8
            "$body$;\n";                                                     // line 9

        PostgresRoutineInfo r = parse(src);
        assertEquals("emp_hire", r.getName(), "sanity: name resolved through the $body$ wrapper");

        PostgresTableOpInfo insert = findOp(r, PostgresTableOpInfo.DmlType.INSERT);
        PostgresTableOpInfo update = findOp(r, PostgresTableOpInfo.DmlType.UPDATE);
        assertNotNull(insert);
        assertNotNull(update);
        // CORRECT expected: INSERT is on line 6, UPDATE on line 7 (1-based, matching the LF source).
        assertEquals(6, insert.getLine(), "INSERT line number with LF endings");
        assertEquals(7, update.getLine(), "UPDATE line number with LF endings");
    }

    @Test
    void crlfLineEndings_lineNumbersOfStatementsInDollarBody_matchLF() {
        // Same logical source as lfLineEndings_..., but every line terminator is CRLF.
        String src =
            "CREATE OR REPLACE PROCEDURE hr.emp_hire(IN p_name VARCHAR)\r\n" +
            "LANGUAGE plpgsql AS $body$\r\n" +
            "DECLARE\r\n" +
            "  v_id INT;\r\n" +
            "BEGIN\r\n" +
            "  INSERT INTO employees(name) VALUES (p_name);\r\n" +
            "  UPDATE audit_log SET last = now();\r\n" +
            "END;\r\n" +
            "$body$;\r\n";

        PostgresRoutineInfo r = parse(src);
        assertEquals("emp_hire", r.getName(), "sanity: name resolved through the $body$ wrapper with CRLF");

        PostgresTableOpInfo insert = findOp(r, PostgresTableOpInfo.DmlType.INSERT);
        PostgresTableOpInfo update = findOp(r, PostgresTableOpInfo.DmlType.UPDATE);
        assertNotNull(insert);
        assertNotNull(update);

        // ACTUAL: ANTLR's default line-counting increments on '\n' only (a CRLF pair still
        // contains exactly one '\n'), and countLines()/preprocess() in PostgresAnalysisEngine
        // also count only '\n'. So CRLF and LF produce IDENTICAL line numbers here: no bug.
        assertEquals(6, insert.getLine(), "INSERT line number with CRLF endings (should match LF: 6)");
        assertEquals(7, update.getLine(), "UPDATE line number with CRLF endings (should match LF: 7)");

        // Also confirm the routine's overall lineEnd tracks the CRLF source's real line count (9),
        // via countLines(source) fallback in PostgresAnalysisEngine.parse().
        assertTrue(r.getLineEnd() >= 8, "lineEnd should reach into the tail of the CRLF source, got " + r.getLineEnd());
    }

    @Test
    void crlfInsideLongDollarBody_statementsAfterCloseTag_correctLine() {
        // A large multi-line CRLF dollar-quoted body, THEN a second routine-like statement
        // after the closing tag — stresses that line counting doesn't drift across a long
        // CRLF span before attributing a line number to what follows.
        StringBuilder body = new StringBuilder();
        body.append("CREATE OR REPLACE PROCEDURE hr.big_proc()\r\n");   // line 1
        body.append("LANGUAGE plpgsql AS $tag$\r\n");                   // line 2
        body.append("BEGIN\r\n");                                       // line 3
        int paddingLines = 50;
        for (int i = 0; i < paddingLines; i++) {
            body.append("  -- padding line ").append(i).append("\r\n"); // lines 4..53
        }
        body.append("  INSERT INTO t1(x) VALUES (1);\r\n");             // line 54
        body.append("END;\r\n");                                        // line 55
        body.append("$tag$;\r\n");                                      // line 56

        PostgresRoutineInfo r = parse(body.toString());
        PostgresTableOpInfo insert = findOp(r, PostgresTableOpInfo.DmlType.INSERT);
        assertNotNull(insert);
        assertEquals(54, insert.getLine(), "line number must not drift across a 50-line CRLF span");
    }

    // ─────────────────────────────────────────────────────────────────────
    // 2. Mixed tabs and spaces
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void mixedTabsAndSpaces_doNotShiftLineOrCaptureText() {
        String src =
            "CREATE\tOR REPLACE   PROCEDURE\thr.tab_proc(IN p_id\tINT)\n" +
            "LANGUAGE plpgsql AS $$\n" +
            "\tDECLARE\n" +
            "\t\tv_x INT;\n" +
            "\tBEGIN\n" +
            "\t\tUPDATE\taccounts SET bal = 1 WHERE id = p_id;\n" +   // line 6
            "\tEND;\n" +
            "$$;\n";

        PostgresRoutineInfo r = parse(src);
        assertEquals("tab_proc", r.getName(), "tab-separated CREATE keywords still parse name correctly");
        PostgresTableOpInfo update = findOp(r, PostgresTableOpInfo.DmlType.UPDATE);
        assertNotNull(update);
        assertEquals(6, update.getLine(), "tabs are single chars for line counting purposes — no shift");
        assertEquals("accounts", update.getTableName(), "tab between UPDATE and table name doesn't corrupt table name capture");
    }

    // ─────────────────────────────────────────────────────────────────────
    // 3. Non-ASCII: comments, string literals, double-quoted identifiers
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void nonAscii_inLineComment_doesNotCorruptSubsequentCapture() {
        // Routine name deliberately kept plain-ASCII here to isolate the "non-ASCII in a
        // comment" concept from the separate (confirmed-buggy) "non-ASCII in an unquoted
        // identifier" concept covered by nonAscii_inUnquotedIdentifier_corruptsCapturedName below.
        String src =
            "CREATE OR REPLACE PROCEDURE hr.cafe_proc(IN p_id INT)\n" +
            "LANGUAGE plpgsql AS $$\n" +
            "BEGIN\n" +
            "  -- commentaire en français: café, naïve, 日本語 テスト\n" +
            "  UPDATE accounts SET bal = 1 WHERE id = p_id;\n" +   // line 5
            "END;\n" +
            "$$;\n";

        PostgresRoutineInfo r = parse(src);
        assertEquals("cafe_proc", r.getName());
        assertTrue(r.getErrors().isEmpty(), "a non-ASCII line comment must not produce parse errors, got: " + r.getErrors());
        PostgresTableOpInfo update = findOp(r, PostgresTableOpInfo.DmlType.UPDATE);
        assertNotNull(update, "UPDATE must still be captured after a non-ASCII comment line");
        assertEquals(5, update.getLine(), "line number after a non-ASCII (BMP) comment line must be correct");
    }

    /**
     * FIXED (was: CONFIRMED BUG, severity HIGH — corrupted captured name).
     *
     * PostgresLexer.g4's {@code ID} rule was {@code [a-zA-Z_$] [a-zA-Z_$0-9]*} — ASCII-only.
     * PostgreSQL itself allows non-ASCII letters in unquoted identifiers (they are valid
     * "letter" characters per the SQL identifier rules and commonly appear in real schemas,
     * e.g. accented European names). The rule now accepts the full extended Unicode range
     * ({@code [a-zA-Z_$-￿] [a-zA-Z_$0-9-￿]*}), so a routine name containing "é" is captured
     * in full with zero parse errors.
     */
    @Test
    void nonAscii_inUnquotedIdentifier_fullNameCaptured() {
        String src =
            "CREATE OR REPLACE PROCEDURE hr.café_proc(IN p_id INT)\n" +
            "LANGUAGE plpgsql AS $$\n" +
            "BEGIN\n" +
            "  UPDATE accounts SET bal = 1 WHERE id = p_id;\n" +
            "END;\n" +
            "$$;\n";

        PostgresRoutineInfo r = parse(src);

        assertEquals("café_proc", r.getName(),
                "unquoted non-ASCII identifier must be captured in full");
        assertTrue(r.getErrors().isEmpty(), "no parse errors expected, got: " + r.getErrors());
    }

    @Test
    void nonAscii_inStringLiteral_capturedVerbatim() {
        String src =
            "CREATE OR REPLACE PROCEDURE hr.p1(IN p_id INT)\n" +
            "LANGUAGE plpgsql AS $$\n" +
            "BEGIN\n" +
            "  INSERT INTO customers(name) VALUES ('Café Müller — 日本語');\n" +  // line 4
            "END;\n" +
            "$$;\n";

        PostgresRoutineInfo r = parse(src);
        PostgresTableOpInfo insert = findOp(r, PostgresTableOpInfo.DmlType.INSERT);
        assertNotNull(insert);
        assertEquals(4, insert.getLine());
        assertNotNull(insert.getSqlText());
        assertTrue(insert.getSqlText().contains("Café Müller"),
                "non-ASCII string literal text must survive verbatim in captured sqlText, got: " + insert.getSqlText());
    }

    @Test
    void nonAscii_inDoubleQuotedIdentifier_capturedWithoutQuotesAndCorrectLine() {
        String src =
            "CREATE OR REPLACE PROCEDURE hr.p2(IN p_id INT)\n" +
            "LANGUAGE plpgsql AS $$\n" +
            "BEGIN\n" +
            "  SELECT 1 FROM \"Café Orders\" WHERE id = p_id;\n" +   // line 4
            "END;\n" +
            "$$;\n";

        PostgresRoutineInfo r = parse(src);
        PostgresTableOpInfo select = findOp(r, PostgresTableOpInfo.DmlType.SELECT);
        assertNotNull(select, "SELECT against a non-ASCII quoted identifier table must still be captured");
        assertEquals(4, select.getLine());
        // cleanId() strips only the surrounding double-quotes; interior text (incl. non-ASCII
        // and the embedded space) is preserved verbatim, matching plain-ASCII quoted-identifier behavior.
        assertEquals("Café Orders", select.getTableName(),
                "double-quoted identifier interior text (with embedded space and accented char) preserved verbatim");
    }

    // ─────────────────────────────────────────────────────────────────────
    // 4. Byte-order-mark (BOM) at file start, immediately preceding "$$"/"$tag$"
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void bomAtFileStart_beforeSignature_stillParsesName() {
        // BOM directly at offset 0 of the whole source, well before the dollar-quote.
        String src =
            "﻿" +
            "CREATE OR REPLACE PROCEDURE hr.bom_proc(IN p_id INT)\n" +
            "LANGUAGE plpgsql AS $$\n" +
            "BEGIN\n" +
            "  UPDATE accounts SET bal = 1 WHERE id = p_id;\n" +  // line 4
            "END;\n" +
            "$$;\n";

        PostgresRoutineInfo r = parse(src);
        // FIXED: PostgresAnalysisEngine.parse() now strips a leading BOM before any other
        // processing, so it never reaches the lexer and produces zero errors.
        assertEquals("bom_proc", r.getName(),
                "name should still resolve even with a leading BOM");
        PostgresTableOpInfo update = findOp(r, PostgresTableOpInfo.DmlType.UPDATE);
        assertNotNull(update);
        assertEquals(4, update.getLine(), "line number unaffected by a leading BOM on line 1");

        assertTrue(r.getErrors().isEmpty(), "leading BOM must now be stripped with zero errors, got: " + r.getErrors());
    }

    @Test
    void bomImmediatelyBeforeDollarTag_stillFindsOpeningTag() {
        // BOM placed on the same line, directly adjacent to the opening "$$" — the highest-risk
        // placement per the audit brief, since preprocess() regex-scans for "\$...\$" starting
        // from index 0 of the (comment-blanked) source.
        String src =
            "CREATE OR REPLACE PROCEDURE hr.bom_proc2(IN p_id INT)\n" +
            "LANGUAGE plpgsql AS ﻿$$\n" +
            "BEGIN\n" +
            "  UPDATE accounts SET bal = 1 WHERE id = p_id;\n" +  // line 4
            "END;\n" +
            "$$;\n";

        PostgresRoutineInfo r = parse(src);
        // The BOM is just another non-'$' character; Pattern "\$[A-Za-z0-9_]*\$" still matches the
        // literal "$$" right after it (find() scans byte-by-byte/char-by-char, unaffected by an
        // unrelated char preceding the match). So the dollar-tag detection itself is unaffected.
        assertEquals("bom_proc2", r.getName(), "BOM immediately before $$ does not break tag detection");
        PostgresTableOpInfo update = findOp(r, PostgresTableOpInfo.DmlType.UPDATE);
        assertNotNull(update);
        assertEquals(4, update.getLine());
        // Unlike the file-start placement above, this BOM falls inside the
        // "LANGUAGE plpgsql AS $tag$" wrapper span that preprocess() blanks out before the lexer
        // ever sees it, so there is not even a spurious diagnostic here — zero errors.
        assertTrue(r.getErrors().isEmpty(), "BOM inside the blanked wrapper span produces no errors at all, got: " + r.getErrors());
    }

    // ─────────────────────────────────────────────────────────────────────
    // 5. Very long single-line statement; single-line vs many-line dollar body
    // ─────────────────────────────────────────────────────────────────────

    @Test
    void veryLongSingleLineInsert_lineNumberStaysOne() {
        StringBuilder cols = new StringBuilder();
        StringBuilder vals = new StringBuilder();
        int n = 500;
        for (int i = 0; i < n; i++) {
            if (i > 0) { cols.append(", "); vals.append(", "); }
            cols.append("col").append(i);
            vals.append(i);
        }
        String src =
            "CREATE OR REPLACE PROCEDURE hr.wide_proc() LANGUAGE plpgsql AS $$ BEGIN "
            + "INSERT INTO wide_table(" + cols + ") VALUES (" + vals + "); "
            + "END; $$;";

        PostgresRoutineInfo r = parse(src);
        PostgresTableOpInfo insert = findOp(r, PostgresTableOpInfo.DmlType.INSERT);
        assertNotNull(insert);
        assertEquals(1, insert.getLine(), "the whole routine is one physical line, so line must stay 1");
        assertEquals("wide_table", insert.getTableName());
        assertTrue(insert.getSqlText() != null && insert.getSqlText().length() > 2000,
                "long single-line statement text should be captured in full, got length "
                        + (insert.getSqlText() == null ? -1 : insert.getSqlText().length()));
    }

    @Test
    void singleLineDollarBody_noNewlinesAtAll_lineNumberOne() {
        String src = "CREATE OR REPLACE PROCEDURE hr.oneliner() LANGUAGE plpgsql AS $$ BEGIN "
                + "UPDATE t1 SET x = 1; END; $$;";

        PostgresRoutineInfo r = parse(src);
        assertEquals("oneliner", r.getName());
        PostgresTableOpInfo update = findOp(r, PostgresTableOpInfo.DmlType.UPDATE);
        assertNotNull(update);
        assertEquals(1, update.getLine(), "single-line $$...$$ body: all statements on line 1");
        assertEquals(1, r.getLineStart());
    }

    @Test
    void hundredsOfLinesDollarBody_lineNumberOfTailStatementCorrect() {
        StringBuilder src = new StringBuilder();
        src.append("CREATE OR REPLACE PROCEDURE hr.long_proc()\n");   // line 1
        src.append("LANGUAGE plpgsql AS $$\n");                       // line 2
        src.append("BEGIN\n");                                        // line 3
        int padding = 300;
        for (int i = 0; i < padding; i++) {
            src.append("  -- line ").append(i).append("\n");          // lines 4..303
        }
        src.append("  DELETE FROM t2 WHERE x = 1;\n");                // line 304
        src.append("END;\n");                                         // line 305
        src.append("$$;\n");                                          // line 306

        PostgresRoutineInfo r = parse(src.toString());
        PostgresTableOpInfo del = findOp(r, PostgresTableOpInfo.DmlType.DELETE);
        assertNotNull(del);
        assertEquals(304, del.getLine(), "line number must remain exact across a 300+ line dollar body");
        assertTrue(r.getLineEnd() >= 305, "lineEnd should reach near the end of a long dollar body, got " + r.getLineEnd());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private PostgresTableOpInfo findOp(PostgresRoutineInfo r, PostgresTableOpInfo.DmlType type) {
        List<PostgresTableOpInfo> ops = r.getTableOps();
        for (PostgresTableOpInfo op : ops) {
            if (op.getDmlType() == type) return op;
        }
        return null;
    }
}
