package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT — does NOT touch main/ source. Uses the real
 * {@link SqlServerAnalysisEngine} (real ANTLR4 lexer/parser + the real, regex-heavy
 * {@code SqlServerAnalysisVisitor}) — no mocks — to stress-test whitespace/encoding edge
 * cases called out for this session's audit:
 *
 *   1. CRLF vs LF line endings (incl. inside the NEW maskStringLiterals()/
 *      stripLineAndBlockComments() helpers)
 *   2. Mixed tabs and spaces around JOIN / WITH (NOLOCK) / bracket-quoted identifiers
 *   3. Non-ASCII characters in N'...' strings, comments, and bracket-quoted identifiers
 *   4. Byte-order-mark (BOM) at file start
 *   5. Very long single-line statements, and GO batch-separator whitespace variants
 *
 * Each test records the ACTUAL observed behavior (via dump()/assertions) so it stands as
 * durable evidence regardless of whether the assertion passes or documents a bug.
 */
class SqlServerWhitespaceEncodingAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()
                        + (t.getJoins().isEmpty() ? "" : " joins=" + t.getJoins().stream()
                                .map(j -> j.getJoinType() + "->" + j.getJoinedTable() + "@" + j.getLine()).toList()))
                .toList());
        System.out.println("lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
        System.out.println("errors: " + r.getErrors());
    }

    // ── Concept 1: CRLF vs LF line-number/table-capture parity ─────────────────────────────

    @Test
    @DisplayName("Concept 1: CRLF body yields the SAME line numbers/tables as the LF equivalent")
    void crlfVsLf_lineNumberParity() {
        String lf =
                "CREATE PROCEDURE usp_Test\n" +
                "AS\n" +
                "BEGIN\n" +
                "    -- a comment mentioning FROM ghost_table\n" +
                "    /* a block\n" +
                "       comment spanning\n" +
                "       lines */\n" +
                "    SELECT a.Id\n" +
                "    FROM real_table a\n" +
                "    JOIN other_table b ON a.Id = b.Id\n" +
                "    WHERE a.Note = N'contains a fake JOIN keyword here';\n" +
                "END";
        String crlf = lf.replace("\n", "\r\n");

        SqlServerRoutineInfo rLf = parse(lf);
        SqlServerRoutineInfo rCrlf = parse(crlf);
        dump("crlf_lf: LF", rLf);
        dump("crlf_lf: CRLF", rCrlf);

        assertTrue(rLf.getErrors().isEmpty(), "unexpected LF parse errors: " + rLf.getErrors());
        assertTrue(rCrlf.getErrors().isEmpty(), "unexpected CRLF parse errors: " + rCrlf.getErrors());

        List<String> lfSummary = rLf.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()).toList();
        List<String> crlfSummary = rCrlf.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()).toList();

        // ACTUAL/EXPECTED: lineAt() only increments its line counter on '\n' (never on '\r'), and
        // both stripLineAndBlockComments()/maskStringLiterals() are length-preserving and pass
        // '\r' through unchanged (masked to a space) while preserving '\n' as-is at the exact same
        // offsets as the original text. So CRLF must NOT double-count lines, and every captured
        // table name/line must match the LF run exactly. CONFIRMED: no bug found here.
        assertEquals(lfSummary, crlfSummary,
                "CRLF must not change captured tables or their line numbers vs the LF equivalent");

        // The "fake JOIN" and "FROM ghost_table" text living inside a comment / string literal
        // must not have been mistaken for a real table/join in either encoding.
        boolean anyGhost = rCrlf.getTableOps().stream()
                .anyMatch(t -> "ghost_table".equalsIgnoreCase(t.getTableName()));
        assertFalse(anyGhost, "commented-out FROM must not be captured as a table (CRLF run)");

        SqlServerTableOpInfo realOp = rCrlf.getTableOps().stream()
                .filter(t -> "real_table".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(realOp, "real_table must be captured in the CRLF run");
        assertEquals(1, realOp.getJoins().size(), "join to other_table must survive in the CRLF run");
        assertEquals("other_table", realOp.getJoins().get(0).getJoinedTable());

        // Line numbers: CRLF adds one extra byte per line but must NOT add extra *lines*. The
        // FROM clause is on source line 9 in both the LF and CRLF variants.
        assertEquals(9, realOp.getLine(), "real_table FROM must be reported on line 9 in the CRLF run");
        SqlServerTableOpInfo realOpLf = rLf.getTableOps().stream()
                .filter(t -> "real_table".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(realOpLf, "sanity: real_table must be captured in the LF run too");
        assertEquals(realOpLf.getLine(), realOp.getLine(),
                "CRLF and LF runs must report the identical line number");
    }

    @Test
    @DisplayName("Concept 1b: CRLF comment immediately preceding a real FROM (stripLineAndBlockComments boundary)")
    void crlfLineComment_precedingFrom() {
        String crlf = "CREATE PROCEDURE usp_Test\r\n" +
                "AS\r\n" +
                "BEGIN\r\n" +
                "    -- trailing comment\r\n" +
                "    SELECT * FROM orders o JOIN customers c ON o.CustId = c.Id;\r\n" +
                "END";
        SqlServerRoutineInfo r = parse(crlf);
        dump("crlfLineComment_precedingFrom", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());

        SqlServerTableOpInfo op = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(op, "orders must be captured; tableOps=" + r.getTableOps());
        assertEquals(5, op.getLine(), "FROM orders is on line 5 of the CRLF source");
        assertEquals(1, op.getJoins().size(), "join to customers must be captured");
        assertEquals(5, op.getJoins().get(0).getLine(), "join line must also be line 5, not doubled");
    }

    // ── Concept 2: mixed tabs/spaces around JOIN / WITH (NOLOCK) / bracket identifiers ──────

    @Test
    @DisplayName("Concept 2: tabs around JOIN/WITH(NOLOCK) and a literal tab inside a bracket-quoted identifier")
    void mixedTabsAndSpaces_joinAndBracketIdentifier() {
        String src =
                "CREATE PROCEDURE usp_Test\n" +
                "AS\n" +
                "BEGIN\n" +
                "\tSELECT a.Id\n" +
                "\tFROM\t[table\tname] a WITH\t(NOLOCK)\n" +
                "\t\tJOIN\tother_table\tb\tON\ta.Id\t=\tb.Id;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("mixedTabsAndSpaces_joinAndBracketIdentifier", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());

        // Bracket-quoted identifier containing a literal TAB character is legal T-SQL
        // (BRACKET_ID: '[' (~']' | ']]')* ']') and must be captured whole, tab included.
        SqlServerTableOpInfo op = r.getTableOps().stream()
                .filter(t -> t.getTableName() != null && t.getTableName().contains("\t"))
                .findFirst().orElse(null);
        assertNotNull(op, "bracket-quoted identifier with an embedded tab must be captured verbatim; observed tableOps="
                + r.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());
        assertEquals("table\tname", op.getTableName());

        assertEquals(1, op.getJoins().size(), "tab-separated JOIN...ON must still be captured; joins=" + op.getJoins());
        assertEquals("other_table", op.getJoins().get(0).getJoinedTable());
        assertEquals("INNER", op.getJoins().get(0).getJoinType());
    }

    // ── Concept 3: non-ASCII in N'...' strings, comments, bracket identifiers ──────────────

    @Test
    @DisplayName("Concept 3: non-ASCII text in a Unicode string literal, a comment, and a bracket identifier")
    void nonAsciiCharacters_doNotCorruptCaptureOrLines() {
        String src =
                "CREATE PROCEDURE usp_Test\n" +
                "AS\n" +
                "BEGIN\n" +
                "    -- 日本語コメント: mentions FROM ghost_table\n" +
                "    SELECT a.Id, N'Café — 日本語 — данные' AS Note\n" +
                "    FROM [Café Orders] a\n" +
                "    JOIN customers b ON a.Id = b.Id\n" +
                "    WHERE a.Note = N'do not JOIN this literal FROM here';\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("nonAsciiCharacters_doNotCorruptCaptureOrLines", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());

        SqlServerTableOpInfo op = r.getTableOps().stream()
                .filter(t -> t.getTableName() != null && t.getTableName().contains("Café"))
                .findFirst().orElse(null);
        assertNotNull(op, "bracket-quoted [Café Orders] must be captured with the accented character intact; observed="
                + r.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());
        assertEquals("Café Orders", op.getTableName());
        // Line: the FROM is on source line 6 — multi-byte UTF-8 chars on earlier lines (comment,
        // string literal) must not perturb Java's UTF-16-code-unit-based line counting.
        assertEquals(6, op.getLine(), "FROM [Café Orders] must be reported on line 6");

        assertEquals(1, op.getJoins().size(), "join to customers must survive alongside non-ASCII text");
        assertEquals(7, op.getJoins().get(0).getLine(), "join line must be line 7");

        boolean anyGhost = r.getTableOps().stream().anyMatch(t -> "ghost_table".equalsIgnoreCase(t.getTableName()));
        assertFalse(anyGhost, "FROM inside a non-ASCII comment must not be captured");
    }

    // ── Concept 4: byte-order-mark (BOM) at file start ──────────────────────────────────────

    @Test
    @DisplayName("Concept 4: UTF-8 BOM (U+FEFF) at the very start of the source")
    void bomAtFileStart() {
        String withoutBom =
                "CREATE PROCEDURE usp_Test\n" +
                "AS\n" +
                "BEGIN\n" +
                "    SELECT * FROM real_table a JOIN other_table b ON a.Id = b.Id;\n" +
                "END";
        String withBom = "﻿" + withoutBom;

        SqlServerRoutineInfo rPlain = parse(withoutBom);
        SqlServerRoutineInfo rBom = parse(withBom);
        dump("bomAtFileStart: no BOM", rPlain);
        dump("bomAtFileStart: with BOM", rBom);

        assertTrue(rPlain.getErrors().isEmpty(), "unexpected errors without BOM: " + rPlain.getErrors());

        // FIXED: SqlServerAnalysisEngine.parse() now strips a leading U+FEFF BOM before handing the
        // source to CharStreams.fromString()/the lexer, so no spurious "token recognition error" is
        // produced at offset 0.
        System.out.println("BOM run errors = " + rBom.getErrors());
        assertTrue(rBom.getErrors().isEmpty(),
                "Leading BOM should now be stripped before lexing, producing zero errors; errors="
                        + rBom.getErrors());

        // Despite the spurious error, table/line capture for the REST of the routine must still be
        // correct (single bad character, not a corrupting one) — same table, same line as the
        // no-BOM run.
        SqlServerTableOpInfo bomOp = rBom.getTableOps().stream()
                .filter(t -> "real_table".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        SqlServerTableOpInfo plainOp = rPlain.getTableOps().stream()
                .filter(t -> "real_table".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(plainOp, "sanity: real_table must be captured without BOM");
        assertNotNull(bomOp, "real_table must still be captured despite the leading BOM; tableOps="
                + rBom.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());
        assertEquals(plainOp.getLine(), bomOp.getLine(),
                "BOM must not shift the line number of content after it");
    }

    // ── Concept 5a: very long single-line statement ─────────────────────────────────────────

    @Test
    @DisplayName("Concept 5a: a very long single-line SELECT with many JOINs is still fully captured")
    void veryLongSingleLineStatement() {
        StringBuilder sql = new StringBuilder("CREATE PROCEDURE usp_Test AS BEGIN SELECT * FROM t0 ");
        int joinCount = 40;
        for (int i = 1; i <= joinCount; i++) {
            sql.append("JOIN t").append(i).append(" ON t0.Id = t").append(i).append(".Id ");
        }
        sql.append("WHERE t0.Id = 1; END");
        String src = sql.toString();

        SqlServerRoutineInfo r = parse(src);
        System.out.println("veryLongSingleLineStatement: joins captured = "
                + (r.getTableOps().isEmpty() ? 0 : r.getTableOps().get(0).getJoins().size())
                + " errors=" + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());

        SqlServerTableOpInfo t0 = r.getTableOps().stream()
                .filter(t -> "t0".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(t0, "t0 must be captured");
        assertEquals(joinCount, t0.getJoins().size(),
                "all " + joinCount + " joins on one very long line must be captured, got "
                        + t0.getJoins().size() + "; joins=" + t0.getJoins());
        // Everything is on the single logical source line 1.
        assertEquals(1, t0.getLine());
        assertTrue(t0.getJoins().stream().allMatch(j -> j.getLine() == 1),
                "every join on the single long line must report line 1; joins=" + t0.getJoins());
    }

    // ── Concept 5b: GO batch separator whitespace variants ──────────────────────────────────

    /**
     * NOTE ON SCOPE: {@code sqlserver-parser}'s grammar ({@code SqlServerLexer.g4} /
     * {@code SqlServerParser.g4}) has NO {@code GO} token or batch-splitting rule at all — its
     * {@code compilationUnit} parses exactly one routine/trigger/view definition per invocation
     * (see {@code compilationUnit : (routineDefinition | triggerDefinition | viewDefinition)? EOF}).
     * "GO" is only referenced as a plain member of {@code STATEMENT_START_KEYWORDS} inside
     * {@code SqlServerAnalysisVisitor.statementText()} (a boundary keyword for capturing a DML
     * statement's full text), and as an ordinary {@code ID} token to the lexer otherwise — actual
     * batch splitting on "GO" (if it exists at all) must happen in a layer OUTSIDE this module
     * (e.g. wherever multiple CREATE PROCEDURE batches in one .sql file get split before being
     * handed one-by-one to {@code SqlServerAnalysisEngine.parse()}), so it is out of scope for this
     * module's audit. This test documents that a stray "GO" appearing where this module's own
     * {@code statementText()} treats it as a statement-boundary keyword does not corrupt capture of
     * the statement text preceding it, regardless of surrounding whitespace.
     */
    @Test
    @DisplayName("Concept 5b: GO (with unusual surrounding whitespace) as a statementText() boundary keyword")
    void goBoundaryKeyword_unusualWhitespace() {
        String src1 =
                "CREATE PROCEDURE usp_Test\n" +
                "AS\n" +
                "BEGIN\n" +
                "    INSERT INTO tab (a) SELECT a FROM src\n" +
                "GO \n" +
                "END";
        String src2 =
                "CREATE PROCEDURE usp_Test\n" +
                "AS\n" +
                "BEGIN\n" +
                "    INSERT INTO tab (a) SELECT a FROM src\n" +
                "\tGO\r\n" +
                "END";

        SqlServerRoutineInfo r1 = parse(src1);
        SqlServerRoutineInfo r2 = parse(src2);
        dump("goBoundaryKeyword: 'GO ' trailing space", r1);
        dump("goBoundaryKeyword: '\\tGO\\r\\n'", r2);

        // Both are expected to produce a parse error, since GO is not a real T-SQL statement
        // recognized by this grammar and appears where the grammar expects the routine to
        // continue/close — evidence, not a pass/fail claim either way.
        System.out.println("src1 errors=" + r1.getErrors());
        System.out.println("src2 errors=" + r2.getErrors());

        // Whatever happens structurally, the INSERT ... SELECT ... FROM src table op that precedes
        // GO in both variants must still be captured identically regardless of GO's surrounding
        // whitespace style (space vs tab, LF vs CRLF before END) — this module's own regex/text
        // scanning must not desync just because "GO" (with odd whitespace) sits nearby.
        SqlServerTableOpInfo srcOp1 = r1.getTableOps().stream()
                .filter(t -> "src".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        SqlServerTableOpInfo srcOp2 = r2.getTableOps().stream()
                .filter(t -> "src".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        assertNotNull(srcOp1, "src must be captured in 'GO ' (trailing space) variant; tableOps="
                + r1.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());
        assertNotNull(srcOp2, "src must be captured in '\\tGO\\r\\n' variant; tableOps="
                + r2.getTableOps().stream().map(SqlServerTableOpInfo::qualifiedTable).toList());
        assertEquals(srcOp1.getLine(), srcOp2.getLine(),
                "line number of the FROM src clause must match across both GO-whitespace variants");
    }
}
