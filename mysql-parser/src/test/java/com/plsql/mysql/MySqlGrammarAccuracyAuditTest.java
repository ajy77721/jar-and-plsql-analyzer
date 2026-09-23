package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlParamInfo;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * READ-ONLY AUDIT (2026-09-18) — CORE GRAMMAR ACCURACY audit for mysql-parser: backtick
 * identifiers, MySQL literal formats (hex/bit), `:=` vs `=` assignment-vs-comparison,
 * data-type modifier chains, and keyword/identifier case sensitivity.
 *
 * Run against the real MySqlGrammarEngine / MySqlAnalysisVisitor. No hard assertions on
 * documented gaps — this test exists to print real captured evidence per the invoking audit's
 * method (mirrors MySqlUniqueConceptsAuditTest / MySqlDmlCaptureGapAuditTest style).
 */
public class MySqlGrammarAccuracyAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    // ── Concept 1: backtick-quoted identifiers ──────────────────────────────────────────────

    @Test
    @DisplayName("1a: backtick table name with embedded space")
    void backtickTableNameWithSpace() {
        String sql = """
                CREATE PROCEDURE p1()
                BEGIN
                  SELECT id FROM `order items`;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p1", null);
        System.out.println("[1a] tableOps: " + tableNames(r));
        System.out.println("[1a] errors: " + r.getErrors());
        // Expected: table name captured as exactly `order items` (no backticks, space preserved).
    }

    @Test
    @DisplayName("1b: reserved word used as backtick-quoted table name")
    void backtickReservedWordAsTableName() {
        String sql = """
                CREATE PROCEDURE p2()
                BEGIN
                  SELECT id FROM `select`;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p2", null);
        System.out.println("[1b] tableOps: " + tableNames(r));
        System.out.println("[1b] errors: " + r.getErrors());
        // Expected: table name captured as exactly `select` (bare word, no backticks).
    }

    @Test
    @DisplayName("1c: backtick schema.table")
    void backtickSchemaDotTable() {
        String sql = """
                CREATE PROCEDURE p3()
                BEGIN
                  SELECT id FROM `my schema`.`my table`;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p3", null);
        System.out.println("[1c] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getSchemaName() + "." + t.getTableName()).toList());
        System.out.println("[1c] errors: " + r.getErrors());
    }

    @Test
    @DisplayName("1d: doubled backtick escape inside backtick identifier")
    void backtickDoubledEscape() {
        String sql = """
                CREATE PROCEDURE p4()
                BEGIN
                  SELECT id FROM `weird``name`;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p4", null);
        System.out.println("[1d] tableOps: " + tableNames(r));
        System.out.println("[1d] errors: " + r.getErrors());
        // Per MySQL, `` inside backticks is an escaped literal backtick -> real name is
        // "weird`name". Expected captured name: weird`name (single backtick, no doubling).
        assertEquals(List.of("weird`name"), tableNames(r));
    }

    // ── Concept 2: MySQL-specific literal formats ───────────────────────────────────────────

    @Test
    @DisplayName("2a: hex literal 0x1A in WHERE clause")
    void hexLiteralZeroX() {
        String sql = """
                CREATE PROCEDURE p5()
                BEGIN
                  UPDATE flags SET status = 1 WHERE mask = 0x1A;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p5", null);
        System.out.println("[2a] tableOps: " + tableNames(r));
        System.out.println("[2a] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[2a] errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "0x1A must parse as one hex literal token");
        assertEquals(List.of("flags"), tableNames(r));
        assertTrue(r.getTableOps().stream().anyMatch(t -> t.getSqlText() != null && t.getSqlText().contains("0x1A")),
                "raw literal text 0x1A must be preserved intact in captured SQL");
    }

    @Test
    @DisplayName("2b: hex literal X'1A' quoted form")
    void hexLiteralQuotedForm() {
        String sql = """
                CREATE PROCEDURE p6()
                BEGIN
                  UPDATE flags SET status = 1 WHERE mask = X'1A';
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p6", null);
        System.out.println("[2b] tableOps: " + tableNames(r));
        System.out.println("[2b] errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "X'1A' must parse as one quoted hex literal token");
        assertEquals(List.of("flags"), tableNames(r));
        assertTrue(r.getTableOps().stream().anyMatch(t -> t.getSqlText() != null && t.getSqlText().contains("X'1A'")),
                "raw literal text X'1A' must be preserved intact in captured SQL");
    }

    @Test
    @DisplayName("2c: bit-value literal b'101'")
    void bitValueLiteralQuotedForm() {
        String sql = """
                CREATE PROCEDURE p7()
                BEGIN
                  UPDATE flags SET status = 1 WHERE mask = b'101';
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p7", null);
        System.out.println("[2c] tableOps: " + tableNames(r));
        System.out.println("[2c] errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "b'101' must parse as one quoted bit literal token");
        assertEquals(List.of("flags"), tableNames(r));
        assertTrue(r.getTableOps().stream().anyMatch(t -> t.getSqlText() != null && t.getSqlText().contains("b'101'")),
                "raw literal text b'101' must be preserved intact in captured SQL");
    }

    @Test
    @DisplayName("2d: bit-value literal 0b101")
    void bitValueLiteralZeroB() {
        String sql = """
                CREATE PROCEDURE p8()
                BEGIN
                  UPDATE flags SET status = 1 WHERE mask = 0b101;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p8", null);
        System.out.println("[2d] tableOps: " + tableNames(r));
        System.out.println("[2d] errors: " + r.getErrors());
        assertTrue(r.getErrors().isEmpty(), "0b101 must parse as one bit literal token");
        assertEquals(List.of("flags"), tableNames(r));
        assertTrue(r.getTableOps().stream().anyMatch(t -> t.getSqlText() != null && t.getSqlText().contains("0b101")),
                "raw literal text 0b101 must be preserved intact in captured SQL");
    }

    // ── Concept 3: `:=` vs `=` assignment vs comparison ─────────────────────────────────────

    @Test
    @DisplayName("3a: SET @x = 5 (EQ used as assignment)")
    void setWithEqAssignment() {
        String sql = """
                CREATE PROCEDURE p9()
                BEGIN
                  SET @x = 5;
                  SELECT @x;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p9", null);
        System.out.println("[3a] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[3a] errors: " + r.getErrors());
    }

    @Test
    @DisplayName("3b: SET @x := 5 (ASSIGN_OP used as assignment)")
    void setWithAssignOp() {
        String sql = """
                CREATE PROCEDURE p10()
                BEGIN
                  SET @x := 5;
                  SELECT @x;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p10", null);
        System.out.println("[3b] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[3b] errors: " + r.getErrors());
    }

    @Test
    @DisplayName("3c: WHERE x = 5 is a comparison, not assignment - must still parse as WHERE")
    void whereEqIsComparisonNotAssignment() {
        String sql = """
                CREATE PROCEDURE p11()
                BEGIN
                  SELECT id FROM accounts WHERE balance = 5;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p11", null);
        System.out.println("[3c] tableOps: " + tableNames(r));
        System.out.println("[3c] errors: " + r.getErrors());
        // Expected: parses fine as SELECT with WHERE comparison; no variable-write recorded
        // for "balance" (setAssignment rule is unreachable from a WHERE clause's expr grammar).
    }

    @Test
    @DisplayName("3d: assignment inside SELECT ... INTO variable via user var with := embedded in expr should not be confused")
    void assignInSetVsSelectInto() {
        String sql = """
                CREATE PROCEDURE p12()
                BEGIN
                  DECLARE v_id INT;
                  SELECT id INTO v_id FROM accounts WHERE id = 1;
                  SET @y := v_id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p12", null);
        System.out.println("[3d] tableOps: " + tableNames(r));
        System.out.println("[3d] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[3d] errors: " + r.getErrors());
    }

    // ── Concept 4: data type declarations with MySQL modifiers ─────────────────────────────

    @Test
    @DisplayName("4a: INT UNSIGNED and INT(11) ZEROFILL params do not corrupt param list")
    void unsignedAndZerofillParams() {
        String sql = """
                CREATE PROCEDURE p13(p_a INT UNSIGNED, p_b INT(11) ZEROFILL, p_c VARCHAR(20))
                BEGIN
                  SELECT p_a, p_b, p_c;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p13", null);
        System.out.println("[4a] params: " + r.getParams().stream()
                .map(p -> p.getDirection() + " " + p.getName() + " : " + p.getDataType()).toList());
        System.out.println("[4a] errors: " + r.getErrors());
        // Expected: exactly 3 params (p_a, p_b, p_c) recognized, none dropped/merged.
        assertEquals(3, r.getParams().size());
        assertEquals("INT UNSIGNED", r.getParams().get(0).getDataType());
        assertEquals("INT(11) ZEROFILL", r.getParams().get(1).getDataType());
        assertEquals("VARCHAR(20)", r.getParams().get(2).getDataType());
    }

    @Test
    @DisplayName("4b: DECIMAL(10,2) UNSIGNED param")
    void decimalUnsignedParam() {
        String sql = """
                CREATE PROCEDURE p14(p_amt DECIMAL(10,2) UNSIGNED)
                BEGIN
                  SELECT p_amt;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p14", null);
        System.out.println("[4b] params: " + r.getParams().stream()
                .map(p -> p.getDirection() + " " + p.getName() + " : " + p.getDataType()).toList());
        System.out.println("[4b] errors: " + r.getErrors());
        assertEquals(1, r.getParams().size());
        assertEquals("DECIMAL(10,2) UNSIGNED", r.getParams().get(0).getDataType());
    }

    @Test
    @DisplayName("4c: VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci followed by more params")
    void varcharCharsetCollateThenMoreParams() {
        String sql = """
                CREATE PROCEDURE p15(p_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, p_id INT)
                BEGIN
                  SELECT p_name, p_id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p15", null);
        System.out.println("[4c] params: " + r.getParams().stream()
                .map(p -> p.getDirection() + " " + p.getName() + " : " + p.getDataType()).toList());
        System.out.println("[4c] errors: " + r.getErrors());
        // Expected: 2 params, p_id NOT swallowed into p_name's type-modifier chain.
        assertEquals(2, r.getParams().size());
        assertEquals("VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
                r.getParams().get(0).getDataType());
        assertEquals("INT", r.getParams().get(1).getDataType());
    }

    // ── Concept 5: case sensitivity — keywords case-insensitive, identifiers case-preserving ─

    @Test
    @DisplayName("5a: lowercase keywords with MixedCase table/column names preserve exact case")
    void lowercaseKeywordsMixedCaseIdentifiers() {
        String sql = """
                create procedure p16()
                begin
                  select Id from CustomerOrders where Status = 'OPEN';
                end
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p16", null);
        System.out.println("[5a] tableOps: " + tableNames(r));
        System.out.println("[5a] errors: " + r.getErrors());
        // Expected: table name captured EXACTLY "CustomerOrders" (not "customerorders" or
        // "CUSTOMERORDERS") since MySQL keywords are case-insensitive but identifier case must
        // be preserved verbatim for correct downstream table matching.
    }

    @Test
    @DisplayName("5b: mixed-case keywords (Select/FROM/WhErE) still parse")
    void mixedCaseKeywords() {
        String sql = """
                CREATE PROCEDURE p17()
                BEGIN
                  Select id From orders WhErE id = 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p17", null);
        System.out.println("[5b] tableOps: " + tableNames(r));
        System.out.println("[5b] errors: " + r.getErrors());
    }

    @Test
    @DisplayName("5c: two tables differing only in case treated as distinct names by the visitor")
    void caseDistinctTableNamesPreserved() {
        String sql = """
                CREATE PROCEDURE p18()
                BEGIN
                  INSERT INTO Orders (id) VALUES (1);
                  INSERT INTO orders (id) VALUES (2);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "p18", null);
        System.out.println("[5c] tableOps: " + tableNames(r));
        System.out.println("[5c] errors: " + r.getErrors());
        // Expected: names captured verbatim as "Orders" and "orders" respectively (exact case
        // preserved per statement) -- whether the analyzer *merges* them elsewhere is a separate,
        // already-audited concern; this test only checks case is not silently folded at capture time.
    }
}
