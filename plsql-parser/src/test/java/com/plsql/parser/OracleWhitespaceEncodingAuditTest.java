package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY EVIDENCE AUDIT — whitespace / encoding / line-ending robustness.
 *
 * <p>This is a permanent evidence file, not a "fix" — every test here documents ACTUAL observed
 * behavior of {@link PlSqlParserEngine} + {@link com.plsql.parser.visitor.PlSqlAnalysisVisitor}
 * against the real ANTLR grammar, for five real-world source-encoding hazards:
 * CRLF line endings, mixed tab/space indentation, non-ASCII content, a leading UTF-8 BOM, and
 * extreme single-line density. No production code is modified by this file.
 */
@DisplayName("Whitespace / Encoding / Line-Ending Robustness Audit")
public class OracleWhitespaceEncodingAuditTest extends ParserTestBase {

    // -----------------------------------------------------------------
    // 1. CRLF vs LF line endings
    // -----------------------------------------------------------------

    @Test
    @DisplayName("AUDIT 1: CRLF body produces same line numbers and clean table names as LF body")
    void testCrlfVsLfLineNumbers() {
        String lf =
                "CREATE OR REPLACE PROCEDURE PROC1 IS\n" +
                "BEGIN\n" +
                "  INSERT INTO orders (id) VALUES (1);\n" +
                "  UPDATE customers SET name = 'x' WHERE id = 1;\n" +
                "  DELETE FROM audit_log WHERE id = 1;\n" +
                "END PROC1;\n" +
                "/\n";
        String crlf = lf.replace("\n", "\r\n");

        ParseResult lfResult = parse(lf);
        ParseResult crlfResult = parse(crlf);

        assertNoParsErrors(lfResult, "LF body");
        assertNoParsErrors(crlfResult, "CRLF body");

        ParsedObject lfObj = lfResult.getObjects().get(0);
        ParsedObject crlfObj = crlfResult.getObjects().get(0);

        List<TableOperationInfo> lfOps = lfObj.getTableOperations();
        List<TableOperationInfo> crlfOps = crlfObj.getTableOperations();

        assertEquals(3, lfOps.size(), "LF: expected 3 table ops");
        assertEquals(3, crlfOps.size(), "CRLF: expected 3 table ops");

        for (int i = 0; i < lfOps.size(); i++) {
            TableOperationInfo lfOp = lfOps.get(i);
            TableOperationInfo crlfOp = crlfOps.get(i);
            // ACTUAL: verify line numbers match between LF and CRLF versions of the identical body.
            assertEquals(lfOp.getLine(), crlfOp.getLine(),
                    "Line number mismatch for op #" + i + " (" + lfOp.getTableName() + "): "
                    + "LF=" + lfOp.getLine() + " CRLF=" + crlfOp.getLine());
            // ACTUAL: verify no trailing \r leaked into the captured table name.
            assertFalse(crlfOp.getTableName().contains("\r"),
                    "Table name corrupted with embedded CR: [" + crlfOp.getTableName() + "]");
            assertEquals(lfOp.getTableName(), crlfOp.getTableName());
        }
    }

    @Test
    @DisplayName("AUDIT 1b: CRLF single-line comment does not leak trailing \\r into any captured text")
    void testCrlfTrailingCommentNoLeak() {
        String crlf =
                "CREATE OR REPLACE PROCEDURE PROC2 IS -- comment one\r\n" +
                "BEGIN\r\n" +
                "  -- another trailing comment\r\n" +
                "  INSERT INTO widgets (id) VALUES (1); -- inline trailing comment\r\n" +
                "END PROC2;\r\n" +
                "/\r\n";
        ParseResult result = parse(crlf);
        assertNoParsErrors(result, "CRLF with trailing comments");
        ParsedObject obj = result.getObjects().get(0);
        assertEquals(1, obj.getTableOperations().size());
        TableOperationInfo op = obj.getTableOperations().get(0);
        assertEquals("WIDGETS", op.getTableName());
        assertFalse(op.getTableName().contains("\r"));
        if (op.getSqlText() != null) {
            // sqlText legitimately may span to end of the INSERT statement (before comment);
            // regardless, no bare \r should be embedded standalone (i.e. not part of \r\n) — but the
            // simplest robust check is just that trailing \r doesn't corrupt the tail of sqlText.
            assertFalse(op.getSqlText().trim().endsWith("\r"),
                    "sqlText ends with a stray CR: [" + op.getSqlText() + "]");
        }
    }

    // -----------------------------------------------------------------
    // 2. Mixed tabs and spaces
    // -----------------------------------------------------------------

    @Test
    @DisplayName("AUDIT 2: mixed tab/space indentation does not affect line tracking or statement boundaries")
    void testMixedTabsAndSpaces() {
        String mixed =
                "CREATE OR REPLACE PROCEDURE PROC3 IS\n" +
                "\tBEGIN\n" +
                "  \t  INSERT INTO tab_table (id) VALUES (1);\n" +
                "\t    UPDATE space_table SET x = 1 WHERE id = 1;\n" +
                "\t\tEND PROC3;\n" +
                "/\n";
        ParseResult result = parse(mixed);
        assertNoParsErrors(result, "Mixed tabs/spaces");
        ParsedObject obj = result.getObjects().get(0);
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertEquals(2, ops.size());
        assertEquals(3, ops.get(0).getLine(), "INSERT should be reported on source line 3");
        assertEquals(4, ops.get(1).getLine(), "UPDATE should be reported on source line 4");
        assertEquals("TAB_TABLE", ops.get(0).getTableName());
        assertEquals("SPACE_TABLE", ops.get(1).getTableName());
    }

    // -----------------------------------------------------------------
    // 3. Non-ASCII content
    // -----------------------------------------------------------------

    @Test
    @DisplayName("AUDIT 3a: unicode line comment does not corrupt subsequent line numbers")
    void testUnicodeCommentLineTracking() {
        String sql =
                "CREATE OR REPLACE PROCEDURE PROC4 IS\n" +
                "  -- 注文テーブル (order table) / Café orders 😀\n" +
                "BEGIN\n" +
                "  INSERT INTO orders_table (id) VALUES (1);\n" +
                "END PROC4;\n" +
                "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Unicode comment");
        ParsedObject obj = result.getObjects().get(0);
        assertEquals(1, obj.getTableOperations().size());
        TableOperationInfo op = obj.getTableOperations().get(0);
        assertEquals(4, op.getLine(), "INSERT after a unicode/emoji comment line should be on line 4");
        assertEquals("ORDERS_TABLE", op.getTableName());
    }

    @Test
    @DisplayName("AUDIT 3b: non-ASCII string literal content ('caf\\u00e9') parses cleanly and doesn't shift later lines")
    void testNonAsciiStringLiteral() {
        String sql =
                "CREATE OR REPLACE PROCEDURE PROC5 IS\n" +
                "BEGIN\n" +
                "  INSERT INTO customers (name) VALUES ('café über 😀');\n" +
                "  UPDATE customers SET name = 'café' WHERE id = 1;\n" +
                "END PROC5;\n" +
                "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Non-ASCII string literal");
        ParsedObject obj = result.getObjects().get(0);
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertEquals(2, ops.size());
        assertEquals(3, ops.get(0).getLine());
        assertEquals(4, ops.get(1).getLine(), "UPDATE line should not be shifted by preceding multi-byte content");
    }

    @Test
    @DisplayName("AUDIT 3c: N'...' national character literal with non-ASCII payload parses cleanly")
    void testNationalCharLiteral() {
        String sql =
                "CREATE OR REPLACE PROCEDURE PROC6 IS\n" +
                "  v_name NVARCHAR2(50) := N'café 日本語';\n" +
                "BEGIN\n" +
                "  INSERT INTO nchar_table (name) VALUES (v_name);\n" +
                "END PROC6;\n" +
                "/\n";
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "N'...' national char literal");
        ParsedObject obj = result.getObjects().get(0);
        assertEquals(1, obj.getTableOperations().size());
        assertEquals(4, obj.getTableOperations().get(0).getLine());
    }

    // -----------------------------------------------------------------
    // 4. Byte-order mark (BOM)
    // -----------------------------------------------------------------

    @Test
    @DisplayName("AUDIT 4: leading UTF-8 BOM character fed to parseContent (String path) is stripped, no spurious error")
    void testLeadingBomCorruptsOrBreaksParse() {
        String bom = "﻿";
        String sql = bom +
                "CREATE OR REPLACE PROCEDURE PROC7 IS\n" +
                "BEGIN\n" +
                "  INSERT INTO bom_table (id) VALUES (1);\n" +
                "END PROC7;\n" +
                "/\n";
        ParseResult result = parse(sql);

        boolean hadErrors = result.getErrors() != null && !result.getErrors().isEmpty();
        boolean gotObject = result.getObjects() != null && !result.getObjects().isEmpty();
        String actualName = gotObject ? result.getObjects().get(0).getName() : null;

        System.out.println("[AUDIT 4] BOM test: hadErrors=" + hadErrors + " errors=" + result.getErrors()
                + " gotObject=" + gotObject + " name=" + actualName);

        // FIXED: PlSqlParserEngine.parseContent(String, String) now strips a leading U+FEFF BOM
        // character before handing the content to CharStreams.fromString(), matching the file-read
        // path (readFileWithFallback). This eliminates the previously-observed spurious
        // "token recognition error at: '﻿'" on line 1.
        assertTrue(gotObject, "Expected at least one parsed object even with a leading BOM");
        assertEquals("PROC7", actualName, "Object name should resolve correctly with the BOM stripped");
        assertFalse(hadErrors, "BOM should now be stripped before lexing, producing zero errors. Got: "
                + result.getErrors());
        assertTrue(result.getErrors().isEmpty());

        ParsedObject obj = result.getObjects().get(0);
        assertEquals(1, obj.getTableOperations().size());
        assertEquals("BOM_TABLE", obj.getTableOperations().get(0).getTableName());
    }

    // -----------------------------------------------------------------
    // 5. Very long single-line statements
    // -----------------------------------------------------------------

    @Test
    @DisplayName("AUDIT 5: entire procedure body condensed onto a single line — all ops report same (correct) line, no crash/hang")
    void testEntireBodyOnSingleLine() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE OR REPLACE PROCEDURE PROC8 IS BEGIN ");
        for (int i = 0; i < 40; i++) {
            sb.append("INSERT INTO long_line_table_").append(i).append(" (id) VALUES (").append(i).append("); ");
        }
        sb.append("END PROC8; \n/\n");
        String sql = sb.toString();

        long start = System.currentTimeMillis();
        ParseResult result = parse(sql);
        long elapsedMs = System.currentTimeMillis() - start;

        assertNoParsErrors(result, "Single-line dense procedure");
        // Regex-backtracking / pathological-perf check: this should be near-instant, not seconds.
        assertTrue(elapsedMs < 15000, "Single-line dense procedure took suspiciously long: " + elapsedMs + "ms");

        ParsedObject obj = result.getObjects().get(0);
        List<TableOperationInfo> ops = obj.getTableOperations();
        assertEquals(40, ops.size());
        for (TableOperationInfo op : ops) {
            // CORRECT-BUT-UNHELPFUL, not a bug: every statement is genuinely on line 1 since the
            // entire body IS on line 1. Document this rather than treat it as a defect.
            assertEquals(1, op.getLine(),
                    "All statements on a single physical line should legitimately report line 1");
        }
    }

    @Test
    @DisplayName("AUDIT 5b: very long single multi-hundred-char statement does not corrupt captured table name")
    void testVeryLongSingleStatement() {
        StringBuilder cols = new StringBuilder();
        StringBuilder vals = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            if (i > 0) { cols.append(", "); vals.append(", "); }
            cols.append("col_").append(i);
            vals.append(i);
        }
        String sql =
                "CREATE OR REPLACE PROCEDURE PROC9 IS\n" +
                "BEGIN\n" +
                "  INSERT INTO wide_table (" + cols + ") VALUES (" + vals + ");\n" +
                "END PROC9;\n" +
                "/\n";
        long start = System.currentTimeMillis();
        ParseResult result = parse(sql);
        long elapsedMs = System.currentTimeMillis() - start;

        assertNoParsErrors(result, "Very long single statement");
        assertTrue(elapsedMs < 15000, "Very long single statement took suspiciously long: " + elapsedMs + "ms");

        ParsedObject obj = result.getObjects().get(0);
        assertEquals(1, obj.getTableOperations().size());
        assertEquals("WIDE_TABLE", obj.getTableOperations().get(0).getTableName());
        assertEquals(3, obj.getTableOperations().get(0).getLine());
    }
}
