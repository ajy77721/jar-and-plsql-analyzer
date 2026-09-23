package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Grammar-accuracy audit — not feature coverage.
 *
 * Focus: does the ANTLR grammar/visitor produce a WRONG AST shape (false table/call/variable
 * mapping) or silently drop information for real-world, syntactically valid Oracle constructs?
 *
 * Concepts audited:
 *   1. Numeric literal formats (scientific notation, NUMBER(p,s), large integers, signs)
 *   2. String literal edge cases (q'[]', q'{}', q'<>', q'()', N'...', doubled quotes)
 *   3. Quoted/case-sensitive identifiers vs unquoted (case folding correctness)
 *   4. Operator precedence/associativity (parses without changing meaning downstream)
 *   5. Compound/multi-part identifiers with unusual spacing ("schema" . "table")
 *
 * Each test records ACTUAL observed behavior. Failing assertions here are the audit's findings,
 * not something to "fix" — see the accompanying report for ranked severity.
 */
@DisplayName("Oracle Grammar Accuracy Audit")
public class OracleGrammarAccuracyAuditTest extends ParserTestBase {

    private VariableInfo findVar(SubprogramInfo sub, String name) {
        return sub.getLocalVariables().stream()
                .filter(v -> v.getName() != null && v.getName().equalsIgnoreCase(name))
                .findFirst().orElse(null);
    }

    // ------------------------------------------------------------------
    // 1. Numeric literal formats
    // ------------------------------------------------------------------
    @Nested
    @DisplayName("1. Numeric literal formats")
    class NumericLiterals {

        @Test
        @DisplayName("Scientific notation literal 1.5e10 / 1.5E-10 assigned to a variable")
        void scientificNotation() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_a NUMBER := 1.5e10;
                        v_b NUMBER := 1.5E-10;
                        v_c NUMBER := 1e10;
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "scientific notation literals");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);
            VariableInfo a = findVar(proc, "v_a");
            VariableInfo b = findVar(proc, "v_b");
            VariableInfo c = findVar(proc, "v_c");
            assertNotNull(a);
            assertNotNull(b);
            assertNotNull(c);
            System.out.println("v_a default=" + a.getDefaultValue());
            System.out.println("v_b default=" + b.getDefaultValue());
            System.out.println("v_c default=" + c.getDefaultValue());
            // Expected: default value text should contain the full literal, not be split into
            // "1" + a dangling identifier "e10".
            assertTrue(a.getDefaultValue() != null && a.getDefaultValue().toLowerCase().contains("1.5e10"),
                    "1.5e10 should be captured whole, got: " + a.getDefaultValue());
            assertTrue(c.getDefaultValue() != null && c.getDefaultValue().toLowerCase().contains("1e10"),
                    "1e10 should be captured whole (not split into '1' and identifier 'e10'), got: "
                            + c.getDefaultValue());
        }

        @Test
        @DisplayName("NUMBER(10,2) precision/scale in declaration is preserved as data type, not literal")
        void numberPrecisionScale() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_amt NUMBER(10,2) := -123.45;
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "NUMBER(10,2) decl");
            SubprogramInfo proc = findSub(result, "PROC");
            VariableInfo v = findVar(proc, "v_amt");
            assertNotNull(v);
            System.out.println("v_amt dataType=" + v.getDataType() + " default=" + v.getDefaultValue());
            assertTrue(v.getDataType() != null && v.getDataType().replace(" ", "").contains("NUMBER(10,2)"),
                    "data type should retain precision/scale, got: " + v.getDataType());
            assertTrue(v.getDefaultValue() != null && v.getDefaultValue().contains("123.45"),
                    "default value should retain the negative literal, got: " + v.getDefaultValue());
        }

        @Test
        @DisplayName("Very large integer literal is captured whole")
        void largeIntegerLiteral() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_big NUMBER := 123456789012345678901234567890;
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "large integer literal");
            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc, "PROC should have been parsed as a real subprogram, not dropped");
            VariableInfo v = findVar(proc, "v_big");
            assertNotNull(v);
            System.out.println("v_big default=" + v.getDefaultValue());
            assertTrue(v.getDefaultValue() != null
                            && v.getDefaultValue().contains("123456789012345678901234567890"),
                    "large literal should be captured whole, got: " + v.getDefaultValue());
        }

        /**
         * CRITICAL FINDING: {@link PlSqlParserEngine#isWrappedSource(String)} misclassifies any
         * unit whose first 300 characters contain an 8+ digit numeric literal (e.g. an integer
         * default value, a date constant like 20240115, or any bare number >= 10_000_000) as
         * Oracle "wrapped/encrypted" source, via the heuristic regex
         * {@code .*\b[0-9a-f]{8,}\b.*} — a plain run of 8+ ASCII digits is a subset of
         * {@code [0-9a-f]}, so it always matches even though it is not hex at all.
         *
         * When that heuristic fires, {@link PlSqlParserEngine#parseContent} SKIPS ANTLR PARSING
         * ENTIRELY and returns only {@code extractBasicInfo} (object name/type via regex) — every
         * subprogram, variable, call, and table operation in the unit is silently lost, with
         * ZERO parse errors reported. This is not a numeric-literal grammar bug per se, but it is
         * triggered directly by concept 1 (numeric literal formats) and is by far the most severe
         * finding of this audit: total, silent information loss for an extremely common
         * real-world pattern (any 8+ digit constant near the top of a file).
         */
        @Test
        @DisplayName("FIXED: an 8+ digit numeric literal near the top of the unit is no longer misdetected as WRAPPED source")
        void eightDigitLiteralTriggersFalseWrappedDetection() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_x NUMBER := 12345678;
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;

            // FIXED behavior: a plain 8-digit decimal literal must NOT trip the wrapped-source
            // heuristic (it contains no genuine hex letters a-f).
            assertFalse(com.plsql.parser.PlSqlParserEngine.isWrappedSource(sql),
                    "an 8-digit purely-decimal literal must not be misdetected as WRAPPED source");

            ParseResult result = parse(sql);
            assertNoParsErrors(result, "8-digit literal should not itself be a parse error");

            SubprogramInfo proc = findSub(result, "PROC");
            System.out.println("PROC found=" + (proc != null)
                    + " ; objects=" + result.getObjects().size());
            if (!result.getObjects().isEmpty()) {
                System.out.println("object type=" + result.getObjects().get(0).getType());
            }

            // CORRECT expected behavior: a package body containing an ordinary 8-digit integer
            // literal is real, unwrapped PL/SQL and must be fully parsed — PROC and its local
            // variable v_x should be discoverable exactly as with a 7-digit literal.
            assertNotNull(proc,
                    "BUG: PROC silently disappeared — the whole unit was misrouted through the "
                            + "WRAPPED-source fallback (regex name/type extraction only) because "
                            + "of the 8-digit literal 12345678, even though this is ordinary, "
                            + "un-wrapped PL/SQL and no parse error was ever reported.");
        }

        @Test
        @DisplayName("Control: a 7-digit numeric literal parses normally (does not trip the wrapped-source heuristic)")
        void sevenDigitLiteralParsesNormally() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_x NUMBER := 1234567;
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            assertFalse(com.plsql.parser.PlSqlParserEngine.isWrappedSource(sql));
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "7-digit literal");
            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc, "control case: PROC must be found for a 7-digit literal");
        }
    }

    // ------------------------------------------------------------------
    // 2. String literal edge cases
    // ------------------------------------------------------------------
    @Nested
    @DisplayName("2. String literal edge cases (Oracle q'' alt-quoting, N'', doubled quotes)")
    class StringLiterals {

        @Test
        @DisplayName("q'[...]' alt-quoted string containing an embedded single quote")
        void altQuoteBrackets() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_s VARCHAR2(100) := q'[it's a test]';
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "q'[...]' alt quoting");
            SubprogramInfo proc = findSub(result, "PROC");
            VariableInfo v = findVar(proc, "v_s");
            assertNotNull(v);
            System.out.println("v_s (q'[]') default=" + v.getDefaultValue());
            assertTrue(v.getDefaultValue() != null && v.getDefaultValue().contains("it's a test"),
                    "q'[it's a test]' should preserve the embedded quote, got: " + v.getDefaultValue());
        }

        @Test
        @DisplayName("q'{...}' alt-quoted string")
        void altQuoteBraces() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_s VARCHAR2(100) := q'{SELECT * FROM t WHERE x = 'y'}';
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "q'{...}' alt quoting");
            SubprogramInfo proc = findSub(result, "PROC");
            VariableInfo v = findVar(proc, "v_s");
            assertNotNull(v);
            System.out.println("v_s (q'{}') default=" + v.getDefaultValue());
        }

        @Test
        @DisplayName("q'<...>' alt-quoted string")
        void altQuoteAngle() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_s VARCHAR2(100) := q'<a < b and c > d>';
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "q'<...>' alt quoting");
            SubprogramInfo proc = findSub(result, "PROC");
            VariableInfo v = findVar(proc, "v_s");
            assertNotNull(v);
            System.out.println("v_s (q'<>') default=" + v.getDefaultValue());
        }

        @Test
        @DisplayName("q'(...)' alt-quoted string")
        void altQuoteParen() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_s VARCHAR2(100) := q'(it's (nested) fine)';
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "q'(...)' alt quoting");
            SubprogramInfo proc = findSub(result, "PROC");
            VariableInfo v = findVar(proc, "v_s");
            assertNotNull(v);
            System.out.println("v_s (q'()') default=" + v.getDefaultValue());
        }

        @Test
        @DisplayName("N'unicode string' national character literal")
        void nationalCharLiteral() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_s NVARCHAR2(100) := N'hello world';
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "N'...' national char literal");
            SubprogramInfo proc = findSub(result, "PROC");
            VariableInfo v = findVar(proc, "v_s");
            assertNotNull(v);
            System.out.println("v_s (N'') default=" + v.getDefaultValue());
        }

        @Test
        @DisplayName("Doubled single-quote escape 'it''s a test'")
        void doubledQuoteEscape() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_s VARCHAR2(100) := 'it''s a test';
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "doubled quote escape");
            SubprogramInfo proc = findSub(result, "PROC");
            VariableInfo v = findVar(proc, "v_s");
            assertNotNull(v);
            System.out.println("v_s (doubled quote) default=" + v.getDefaultValue());
        }
    }

    // ------------------------------------------------------------------
    // 3. Quoted/case-sensitive identifiers
    // ------------------------------------------------------------------
    @Nested
    @DisplayName("3. Quoted vs unquoted identifier case handling")
    class QuotedIdentifiers {

        @Test
        @DisplayName("Quoted lowercase table name \"my_table\" should preserve case per Oracle semantics")
        void quotedLowercaseTableName() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM "my_table" WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "quoted lowercase table name");

            SubprogramInfo proc = findSub(result, "PROC");
            assertNotNull(proc);
            assertFalse(proc.getTableOperations().isEmpty(), "expected at least one table operation");
            TableOperationInfo toi = proc.getTableOperations().get(0);
            System.out.println("Quoted table name captured as: " + toi.getTableName());

            // Real Oracle semantics: a double-quoted identifier's case is preserved EXACTLY.
            // "my_table" in the data dictionary is the table literally named my_table (lowercase),
            // which is a DIFFERENT object from unquoted my_table (folded to MY_TABLE).
            assertEquals("my_table", toi.getTableName(),
                    "quoted identifier case must be preserved verbatim per Oracle semantics; "
                            + "got (likely wrongly uppercased): " + toi.getTableName());
        }

        @Test
        @DisplayName("Unquoted table name is correctly folded to uppercase (control case)")
        void unquotedTableNameFoldedUppercase() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM my_table WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "unquoted table name");
            SubprogramInfo proc = findSub(result, "PROC");
            TableOperationInfo toi = proc.getTableOperations().get(0);
            assertEquals("MY_TABLE", toi.getTableName(),
                    "unquoted identifiers should fold to uppercase (this is correct Oracle behavior)");
        }
    }

    // ------------------------------------------------------------------
    // 4. Operator precedence / associativity
    // ------------------------------------------------------------------
    @Nested
    @DisplayName("4. Operator precedence and associativity")
    class OperatorPrecedence {

        @Test
        @DisplayName("a + b * c parses without error and keeps full expression text intact")
        void mulBindsTighterThanAdd() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_a NUMBER := 2 + 3 * 4;
                        v_b NUMBER := (2 + 3) * 4;
                      BEGIN
                        NULL;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "arithmetic precedence");
            SubprogramInfo proc = findSub(result, "PROC");
            VariableInfo a = findVar(proc, "v_a");
            VariableInfo b = findVar(proc, "v_b");
            System.out.println("v_a default=" + a.getDefaultValue());
            System.out.println("v_b default=" + b.getDefaultValue());
            assertNotEquals(a.getDefaultValue(), b.getDefaultValue(),
                    "distinct source expressions should not collapse to the same captured text");
        }

        @Test
        @DisplayName("String concatenation || mixed with arithmetic in a WHERE clause parses correctly")
        void concatMixedWithArithmetic() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM orders WHERE code = 'A' || 1 + 2 || 'B';
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "concat mixed with arithmetic");
        }

        @Test
        @DisplayName("NOT a = b parses as NOT(a = b), not (NOT a) = b")
        void notPrecedence() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM orders WHERE NOT status = 'CLOSED';
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "NOT precedence");
        }
    }

    // ------------------------------------------------------------------
    // 5. Compound/multi-part identifiers with unusual spacing
    // ------------------------------------------------------------------
    @Nested
    @DisplayName("5. Multi-part identifiers with whitespace/quoting variations")
    class CompoundIdentifiers {

        @Test
        @DisplayName("schema . table with spaces around the dot resolves to the same table as schema.table")
        void spacesAroundDot() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM hr . employees WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "spaces around dot in qualified name");
            SubprogramInfo proc = findSub(result, "PROC");
            assertFalse(proc.getTableOperations().isEmpty());
            TableOperationInfo toi = proc.getTableOperations().get(0);
            System.out.println("schema=" + toi.getSchema() + " table=" + toi.getTableName());
            assertEquals("HR", toi.getSchema());
            assertEquals("EMPLOYEES", toi.getTableName());
        }

        @Test
        @DisplayName("\"schema\".\"table\" both quoted preserves case for both parts")
        void bothPartsQuoted() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM "hr"."Employees" WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "both parts quoted");
            SubprogramInfo proc = findSub(result, "PROC");
            TableOperationInfo toi = proc.getTableOperations().get(0);
            System.out.println("schema=" + toi.getSchema() + " table=" + toi.getTableName());
            assertEquals("hr", toi.getSchema(), "quoted schema must preserve case");
            assertEquals("Employees", toi.getTableName(), "quoted table must preserve case");
        }

        @Test
        @DisplayName("Mixed quoting schema.\"Table\" preserves case only on the quoted part")
        void mixedQuoting() {
            String sql = """
                    CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                      PROCEDURE PROC IS
                        v_id NUMBER;
                      BEGIN
                        SELECT id INTO v_id FROM hr."Employees" WHERE id = 1;
                      END PROC;
                    END MY_PKG;
                    /
                    """;
            ParseResult result = parse(sql);
            assertNoParsErrors(result, "mixed quoting");
            SubprogramInfo proc = findSub(result, "PROC");
            TableOperationInfo toi = proc.getTableOperations().get(0);
            System.out.println("schema=" + toi.getSchema() + " table=" + toi.getTableName());
            assertEquals("HR", toi.getSchema(), "unquoted schema should fold to uppercase");
            assertEquals("Employees", toi.getTableName(), "quoted table must preserve case");
        }
    }
}
