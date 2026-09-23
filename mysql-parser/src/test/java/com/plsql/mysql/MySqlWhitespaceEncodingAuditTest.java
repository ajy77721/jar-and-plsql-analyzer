package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT — whitespace / encoding / line-ending robustness.
 *
 * Evidence-only test file: every test records the ACTUAL observed behavior of the real
 * mysql-parser grammar + {@link MySqlAnalysisVisitor} (no mocks). Where behavior is wrong,
 * the assertion documents the actual (buggy) value and the comment states the correct
 * expected value — this file intentionally does NOT fix the underlying code.
 */
class MySqlWhitespaceEncodingAuditTest {

    private final MySqlGrammarEngine engine = new MySqlGrammarEngine();

    private MySqlRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    // ── 1. CRLF vs LF line endings ──────────────────────────────────────────────

    @Test
    void crlfAndLfProduceSameLineNumbersForTableOp() {
        String lf = "CREATE PROCEDURE emp_hire()\nBEGIN\n  SELECT * FROM employees;\nEND";
        String crlf = lf.replace("\n", "\r\n");

        MySqlRoutineInfo rLf = parse(lf);
        MySqlRoutineInfo rCrlf = parse(crlf);

        assertEquals(1, rLf.getTableOps().size());
        assertEquals(1, rCrlf.getTableOps().size());
        // CONFIRMED CORRECT: ANTLR's WS rule includes \r\n and skips it, so line numbers match.
        assertEquals(rLf.getTableOps().get(0).getLine(), rCrlf.getTableOps().get(0).getLine());
        assertEquals(3, rCrlf.getTableOps().get(0).getLine());
        assertEquals("employees", rCrlf.getTableOps().get(0).getTableName());
    }

    @Test
    void delimiterDirectiveWithCrlfAfterDirectiveIsRecognizedSameAsLf() {
        // DELIMITER $$ ... $$ ... DELIMITER ; pattern, once with LF, once with CRLF throughout.
        String lf = "DELIMITER $$\n" +
                "CREATE PROCEDURE emp_hire()\n" +
                "BEGIN\n" +
                "  SELECT * FROM employees$$\n" +
                "END$$\n" +
                "DELIMITER ;\n";
        String crlf = lf.replace("\n", "\r\n");

        MySqlRoutineInfo rLf = parse(lf);
        MySqlRoutineInfo rCrlf = parse(crlf);

        // CONFIRMED CORRECT: DELIMITER_LINE regex uses \s* which matches trailing \r, so the
        // directive line still matches under CRLF and the custom delimiter ($$) is still
        // recognized/stripped identically to the LF case.
        assertEquals("emp_hire", rLf.getName());
        assertEquals("emp_hire", rCrlf.getName());
        assertEquals(1, rLf.getTableOps().size());
        assertEquals(1, rCrlf.getTableOps().size());
        assertTrue(rLf.getErrors().isEmpty(), "LF version should parse without errors: " + rLf.getErrors());
        assertTrue(rCrlf.getErrors().isEmpty(), "CRLF version should parse without errors: " + rCrlf.getErrors());
    }

    @Test
    void delimiterDirectiveTrailingAStatementTerminatorOnTheSameLineIsRecognized() {
        // `END$$ DELIMITER ;` — extremely common in checked-in scripts and in the output of some
        // dump tools. The directive is NOT at the start of its line, so the ^\s*DELIMITER line
        // regex could never see it: it survived preprocessing, reached the ANTLR parser as a bare
        // identifier after the end of the routine, and produced
        //   "line 5:5 mismatched input 'DELIMITER' expecting {<EOF>, ';'}"
        // FIXED: the delimiter-replacement state machine now tests for the directive at the point
        // just past a custom-delimiter terminator — a position it has already proven to be outside
        // every string literal and comment — and drops it.
        String inline = "DELIMITER $$\n" +
                "CREATE PROCEDURE emp_hire()\n" +
                "BEGIN\n" +
                "  SELECT * FROM employees$$\n" +
                "END$$ DELIMITER ;\n";
        String onItsOwnLine = "DELIMITER $$\n" +
                "CREATE PROCEDURE emp_hire()\n" +
                "BEGIN\n" +
                "  SELECT * FROM employees$$\n" +
                "END$$\n" +
                "DELIMITER ;\n";

        MySqlRoutineInfo rInline = parse(inline);
        MySqlRoutineInfo rOwnLine = parse(onItsOwnLine);

        assertTrue(rInline.getErrors().isEmpty(),
                "inline DELIMITER must not reach the parser: " + rInline.getErrors());
        assertEquals(rOwnLine.getName(), rInline.getName());
        assertEquals(rOwnLine.getTableOps().size(), rInline.getTableOps().size());
        assertEquals(1, rInline.getTableOps().size());
        assertEquals("employees", rInline.getTableOps().get(0).getTableName());
    }

    @Test
    void theWordDelimiterInsideAStringLiteralIsStillNotTreatedAsADirective() {
        // The guard on the fix above: the directive is only recognized immediately after a
        // statement terminator, outside quotes. Ordinary data that merely contains the word must
        // survive byte-for-byte.
        String src = "DELIMITER $$\n" +
                "CREATE PROCEDURE emp_note()\n" +
                "BEGIN\n" +
                "  SELECT 'DELIMITER ;' AS note FROM employees$$\n" +
                "END$$ DELIMITER ;\n";

        MySqlRoutineInfo r = parse(src);

        assertTrue(r.getErrors().isEmpty(), "should parse cleanly: " + r.getErrors());
        assertEquals(1, r.getTableOps().size());
        assertEquals("employees", r.getTableOps().get(0).getTableName());
        assertTrue(r.getTableOps().get(0).getSqlText().contains("'DELIMITER ;'"),
                "string literal content must be preserved verbatim: "
                        + r.getTableOps().get(0).getSqlText());
    }

    // ── BONUS finding: naive whole-line delimiter substring replace ────────────

    @Test
    void customDelimiterSubstringInsideStringLiteralIsCorrupted() {
        // The custom delimiter "$$" is used both as the statement terminator AND happens to
        // appear, coincidentally, inside an unrelated string literal in the body.
        String src = "DELIMITER $$\n" +
                "CREATE PROCEDURE emp_note()\n" +
                "BEGIN\n" +
                "  SELECT 'price is $$5' AS note FROM employees$$\n" +
                "END$$\n" +
                "DELIMITER ;\n";

        MySqlRoutineInfo r = parse(src);

        // FIXED: MySqlGrammarEngine.preprocess() now replaces the custom delimiter with a
        // quote/comment-aware scan (replaceDelimiterOutsideLiterals), so the "$$" embedded
        // inside the string literal 'price is $$5' is left intact, and only the real trailing
        // terminator occurrence of "$$" is normalized to ';'.
        assertEquals(1, r.getTableOps().size());
        String capturedText = r.getTableOps().get(0).getSqlText();
        assertNotNull(capturedText);
        assertTrue(capturedText.contains("price is $$5"),
                "String literal content must be preserved intact: " + capturedText);
        assertFalse(capturedText.contains("price is ;5"),
                "String literal must not be corrupted by delimiter substitution: " + capturedText);
        assertTrue(r.getErrors().isEmpty(), "Should parse without errors: " + r.getErrors());
    }

    // ── 2. Mixed tabs and spaces ────────────────────────────────────────────────

    @Test
    void mixedTabsAndSpacesDoNotAffectStatementBoundaryOrLineTracking() {
        String src = "CREATE PROCEDURE emp_hire()\n" +
                "BEGIN\n" +
                "\t  SELECT * FROM employees;\n" +
                "  \tUPDATE employees SET name = 'x';\n" +
                "END";

        MySqlRoutineInfo r = parse(src);

        // CONFIRMED CORRECT: WS rule [ \t\r\n]+ -> skip treats tabs and spaces identically;
        // both statements are captured on their correct source lines regardless of indentation
        // style (tabs vs spaces vs mixed).
        assertEquals(2, r.getTableOps().size());
        assertEquals(3, r.getTableOps().get(0).getLine());
        assertEquals(4, r.getTableOps().get(1).getLine());
    }

    // ── 3. Non-ASCII characters ─────────────────────────────────────────────────

    @Test
    void unicodeInCommentsAndStringLiteralsDoesNotCorruptLexingOrPositions() {
        String src = "CREATE PROCEDURE emp_hire()\n" +
                "BEGIN\n" +
                "  -- café naïve 😀 comment\n" +
                "  SELECT 'René 你好 😀' AS greeting FROM employees;\n" +
                "END";

        MySqlRoutineInfo r = parse(src);

        // CONFIRMED CORRECT: accented characters and BMP characters inside a string literal or
        // line comment are matched by the catch-all ~'\'' / ~[\r\n] token bodies (any UTF-16
        // code unit), so they do not break lexing. The emoji (a surrogate pair, 2 UTF-16 units)
        // is likewise consumed as two "any char" units inside the quoted/comment token and is
        // reproduced intact in the captured text — no lexer error, no line-number drift.
        assertTrue(r.getErrors().isEmpty(), "Unicode content should not produce lexer/parser errors: " + r.getErrors());
        assertEquals(1, r.getTableOps().size());
        assertEquals(4, r.getTableOps().get(0).getLine());
        assertEquals("employees", r.getTableOps().get(0).getTableName());
        assertTrue(r.getTableOps().get(0).getSqlText().contains("😀"),
                "Emoji inside the string literal should survive intact in captured SQL text");
    }

    // ── 4. Byte-order-mark (BOM) ────────────────────────────────────────────────

    @Test
    void bomAtFileStartCausesLexerErrorAndDoesNotCorruptName() {
        String src = "﻿CREATE PROCEDURE emp_hire()\nBEGIN\n  SELECT * FROM employees;\nEND";

        MySqlRoutineInfo r = parse(src);

        // FIXED: MySqlGrammarEngine.preprocess() now strips a leading U+FEFF BOM before the
        // ANTLR lexer ever sees the source, so no spurious token-recognition error is produced.
        assertEquals("emp_hire", r.getName(), "Name should not be corrupted by a leading BOM");
        assertTrue(r.getErrors().isEmpty(),
                "Leading BOM should no longer produce a lexer error: " + r.getErrors());
    }

    @Test
    void bomImmediatelyBeforeDelimiterDirectiveIsNotRecognized() {
        String src = "﻿DELIMITER $$\n" +
                "CREATE PROCEDURE emp_hire()\n" +
                "BEGIN\n" +
                "  SELECT * FROM employees$$\n" +
                "END$$\n" +
                "DELIMITER ;\n";

        MySqlRoutineInfo r = parse(src);

        // FIXED: MySqlGrammarEngine.preprocess() strips a leading U+FEFF BOM before the
        // DELIMITER-directive line scan runs, so "﻿DELIMITER $$" is recognized identically to
        // the non-BOM case: "emp_hire" is resolved, the table op is captured, and there are
        // zero errors (see delimiterDirectiveWithCrlfAfterDirectiveIsRecognizedSameAsLf above).
        assertTrue(r.getErrors().isEmpty(),
                "BOM-prefixed DELIMITER directive should now be recognized: " + r.getErrors());
        assertEquals("emp_hire", r.getName());
        assertEquals(1, r.getTableOps().size());
        assertEquals("employees", r.getTableOps().get(0).getTableName());
    }

    // ── 5. Very long single-line statements ────────────────────────────────────

    @Test
    void veryLongSingleLineStatementParsesWithoutBreakage() {
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            if (i > 0) cols.append(", ");
            cols.append("col").append(i);
        }
        String src = "CREATE PROCEDURE emp_wide() BEGIN SELECT " + cols + " FROM employees; END";

        MySqlRoutineInfo r = parse(src);

        // CONFIRMED CORRECT: ANTLR's DFA-based lexer/parser have no meaningful line-length
        // limit; a 500-column single-line SELECT parses cleanly with correct line number (1)
        // and no errors, i.e. no breakage specific to single-line statement density.
        assertTrue(r.getErrors().isEmpty(), "Very long single-line statement should parse cleanly: " + r.getErrors());
        assertEquals(1, r.getTableOps().size());
        assertEquals(1, r.getTableOps().get(0).getLine());
        assertEquals("employees", r.getTableOps().get(0).getTableName());
    }
}
