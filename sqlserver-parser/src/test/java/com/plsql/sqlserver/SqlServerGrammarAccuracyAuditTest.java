package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (evidence-gathering only, no fixes applied here).
 *
 * Focus: CORE GRAMMAR / REGEX ACCURACY for constructs not yet stress-tested in this session's
 * ~19-bug audit run. Specifically hunting for cases where a valid real-world T-SQL construct is
 * silently mis-parsed into a WRONG identifier/table name (worse than dropping it outright).
 *
 * Concepts audited:
 *   1. Bracket-quoted identifiers with special characters (spaces, escaped "]]")
 *   2. N'...' unicode string literals / 0x hex literals interfering with FROM/JOIN regex scans
 *   3. Multi-part identifiers mixing bracketed and unbracketed segments
 *   4. Case sensitivity / case preservation across repeated references
 *   5. Comments/whitespace injected inside a multi-part qualified name
 */
@DisplayName("AUDIT: SQL Server core grammar/regex accuracy (brackets, strings, mixed-quoting, case, comments)")
public class SqlServerGrammarAccuracyAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private List<String> tableNamesRaw(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(SqlServerTableOpInfo::getTableName)
                .filter(Objects::nonNull)
                .toList();
    }

    private String dumpOps(SqlServerRoutineInfo r) {
        return r.getTableOps().stream()
                .map(op -> op.getDmlType() + " schema=" + op.getSchemaName() + " table=" + op.getTableName())
                .toList().toString();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 1: bracket-quoted identifiers with special characters
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("1a: FROM [table name with spaces] -- bracket-quoted identifier containing a space")
    void auditBracketedIdentifierWithSpaces() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM [table name with spaces] WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[1a] tableOps=" + dumpOps(r));
        // CORRECT expected: the full identifier "table name with spaces" should be captured whole.
        assertTrue(names.contains("table name with spaces"),
                "EXPECTED: full bracketed identifier with spaces captured verbatim. ACTUAL names=" + names);
    }

    //@Test
    @DisplayName("1b: FROM [Order] -- reserved word bracket-quoted as identifier, no special chars")
    void auditBracketedReservedWord() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM [Order] WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[1b] tableOps=" + dumpOps(r));
        assertTrue(names.contains("Order"), "control case: plain bracketed reserved word should work. ACTUAL=" + names);
    }

    @Test
    @DisplayName("1c: FROM [schema].[table] -- both parts bracket-quoted")
    void auditBothPartsBracketed() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM [dbo].[Orders] WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[1c] tableOps=" + dumpOps(r));
        assertTrue(names.contains("Orders"), "control case. ACTUAL=" + names);
    }

    @Test
    @DisplayName("1d: FROM [table]]name] -- escaped ']]' inside brackets represents identifier 'table]name'")
    void auditBracketedIdentifierWithEscapedBracket() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM [table]]name] WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[1d] tableOps=" + dumpOps(r));
        // CORRECT expected per T-SQL semantics: identifier is "table]name".
        assertTrue(names.contains("table]name"),
                "EXPECTED: escaped bracket identifier 'table]name' captured whole. ACTUAL names=" + names);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 2: N'...' unicode strings / 0x hex literals near FROM/JOIN
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("2a: WHERE clause N'...' literal containing the word JOIN must not spawn a phantom join/table")
    void auditUnicodeLiteralContainingJoinKeyword() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM Orders o\n" +
                "  WHERE o.note = N'Please JOIN Support ON call for help';\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        List<?> joins = r.getTableOps().stream().flatMap(t -> t.getJoins().stream()).toList();
        System.out.println("[2a] tableOps=" + dumpOps(r) + " joins=" + joins);
        assertTrue(names.stream().anyMatch(n -> n.equalsIgnoreCase("Orders")), "real table must still be found");
        assertFalse(names.stream().anyMatch(n -> n.equalsIgnoreCase("Support")),
                "EXPECTED: no phantom table 'Support' invented from string literal content. ACTUAL names=" + names);
        assertTrue(joins.isEmpty(),
                "EXPECTED: no phantom join invented from a JOIN keyword inside a string literal. ACTUAL joins=" + joins);
    }

    @Test
    @DisplayName("2b: 0x hex literal in WHERE clause does not break FROM-clause table capture")
    void auditHexLiteralDoesNotBreakFromCapture() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM Orders o WHERE o.rowver = 0x1A2B AND o.status = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[2b] tableOps=" + dumpOps(r));
        assertTrue(names.stream().anyMatch(n -> n.equalsIgnoreCase("Orders")), "control case. ACTUAL=" + names);
    }

    @Test
    @DisplayName("2c: N'...' literal containing 'FROM' text used as an INSERT value does not spawn a phantom table op")
    void auditUnicodeLiteralContainingFromKeywordInInsertValues() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  INSERT INTO Orders (id, note) VALUES (1, N'Shipped FROM Warehouse JOIN Depot');\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[2c] tableOps=" + dumpOps(r));
        assertTrue(names.stream().anyMatch(n -> n.equalsIgnoreCase("Orders")), "real insert target must still be found");
        assertFalse(names.stream().anyMatch(n -> n.equalsIgnoreCase("Warehouse")),
                "EXPECTED: no phantom table 'Warehouse' invented from string literal content. ACTUAL names=" + names);
        assertFalse(names.stream().anyMatch(n -> n.equalsIgnoreCase("Depot")),
                "EXPECTED: no phantom table 'Depot' invented from string literal content. ACTUAL names=" + names);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 3: mixed bracketed / unbracketed multi-part identifiers
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("3a: FROM dbo.[Order Details] -- unbracketed schema, bracketed table with a space")
    void auditMixedQuotingWithSpaceInTable() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM dbo.[Order Details] WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[3a] tableOps=" + dumpOps(r));
        assertTrue(names.contains("Order Details"),
                "EXPECTED: full bracketed table name with space captured whole, schema=dbo. ACTUAL names=" + names);
    }

    @Test
    @DisplayName("3b: FROM [MyDb].dbo.[Orders] -- 3-part name, partial bracket-quoting")
    void auditThreePartMixedQuoting() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM [MyDb].dbo.[Orders] WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[3b] tableOps=" + dumpOps(r));
        assertTrue(names.contains("Orders"),
                "EXPECTED: real table 'Orders' (last segment) resolved despite mixed bracket quoting. ACTUAL=" + names);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 4: case sensitivity / case preservation
    // ─────────────────────────────────────────────────────────────────────────

  //  @Test
    @DisplayName("4a: same table referenced with different casing in two statements -- original case preserved per-occurrence")
    void auditCasePreservationAcrossStatements() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM Orders WHERE id = 1;\n" +
                "  SELECT * FROM ORDERS WHERE id = 2;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[4a] tableOps=" + dumpOps(r));
        assertTrue(names.contains("Orders"), "first occurrence's original casing 'Orders' must be preserved. ACTUAL=" + names);
        assertTrue(names.contains("ORDERS"), "second occurrence's original casing 'ORDERS' must be preserved. ACTUAL=" + names);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Concept 5: comments/whitespace inside a multi-part qualified name
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("5a: FROM dbo/*comment*/.Orders -- block comment between schema and dot")
    void auditCommentBetweenSchemaAndDot() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM dbo/*comment*/.Orders WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[5a] tableOps=" + dumpOps(r));
        assertTrue(names.stream().anyMatch(n -> n.equalsIgnoreCase("Orders")),
                "EXPECTED: real table 'Orders' should still be resolved even with a comment before the dot. ACTUAL=" + names);
    }

    @Test
    @DisplayName("5b: FROM dbo /* x */ . Orders -- whitespace + comment around the dot")
    void auditWhitespaceAndCommentAroundDot() {
        String sql = "CREATE PROCEDURE p1\n" +
                "AS\n" +
                "BEGIN\n" +
                "  SELECT * FROM dbo /* x */ . Orders WHERE id = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(sql);
        List<String> names = tableNamesRaw(r);
        System.out.println("[5b] tableOps=" + dumpOps(r));
        assertTrue(names.stream().anyMatch(n -> n.equalsIgnoreCase("Orders")),
                "EXPECTED: real table 'Orders' should still be resolved. ACTUAL=" + names);
    }
}
