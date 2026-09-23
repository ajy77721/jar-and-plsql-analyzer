package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (2026-09-17) — MySQL native multi-row batch INSERT audit.
 *
 * Scope note (verified by grep, not fabricated): MySQL has no first-class ARRAY type outside
 * JSON, and no Oracle-style collection constructs (VARRAY / TABLE OF / BULK COLLECT / FORALL).
 * `grep -ni "array"` and `grep -ni "varray|bulk collect|forall|nested table"` and
 * `grep -ni "table[[:space:]]*of"` against
 * mysql-parser/src/main/antlr4/com/plsql/mysql/grammar/{MySqlParser,MySqlLexer}.g4 all return
 * zero matches. Those concepts DO NOT APPLY to mysql-parser and are intentionally not audited
 * here — this file audits only the one applicable MySQL-native concept: multi-row batch INSERT
 * (`INSERT INTO tab VALUES (...), (...), (...)`).
 *
 * Source under audit (read fresh 2026-09-17):
 * mysql-parser/src/main/java/com/plsql/mysql/visitor/MySqlAnalysisVisitor.java, method
 * visitInsertStatement (around line 406):
 *
 *   public Void visitInsertStatement(MySqlParser.InsertStatementContext ctx) {
 *       addTableOp(ctx.tableRef(), MySqlTableOpInfo.DmlType.INSERT, ctx.getStart().getLine());
 *       return visitChildren(ctx);
 *   }
 *
 * addTableOp is called exactly once per InsertStatementContext (one INSERT statement = one
 * ANTLR parse-tree node), regardless of how many parenthesized row-tuples follow VALUES.
 * visitChildren(ctx) then walks into insertRowList / insertRowElement, but there is no override
 * for those rules that calls addTableOp again — so the row tuples themselves never register
 * additional table ops. This test proves that behavior empirically across row-count variations,
 * and also exercises multi-row INSERT with ON DUPLICATE KEY UPDATE.
 */
public class MySqlBatchInsertAuditTest {

    private static List<MySqlTableOpInfo> insertOpsFor(MySqlRoutineInfo r, String table) {
        return r.getTableOps().stream()
                .filter(t -> t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT
                        && table.equalsIgnoreCase(t.getTableName()))
                .toList();
    }

    @Test
    @DisplayName("B1: single-row INSERT INTO tab VALUES (1,'a') — exactly one INSERT op")
    void singleRowInsert() {
        String sql = """
                CREATE PROCEDURE proc_insert_1()
                BEGIN
                  INSERT INTO tab (id, name) VALUES (1, 'a');
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_insert_1", null);
        List<MySqlTableOpInfo> ops = insertOpsFor(r, "tab");
        System.out.println("[B1] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[B1] 'tab' INSERT op count = " + ops.size());
        System.out.println("[B1] errors: " + r.getErrors());

        assertEquals(1, ops.size(), "single-row INSERT must yield exactly one INSERT op for 'tab'");
        assertTrue(r.getErrors().isEmpty(), "single-row INSERT should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B2: two-row INSERT INTO tab VALUES (1,'a'), (2,'b') — exactly one INSERT op, not two")
    void twoRowInsert() {
        String sql = """
                CREATE PROCEDURE proc_insert_2()
                BEGIN
                  INSERT INTO tab (id, name) VALUES (1, 'a'), (2, 'b');
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_insert_2", null);
        List<MySqlTableOpInfo> ops = insertOpsFor(r, "tab");
        System.out.println("[B2] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[B2] 'tab' INSERT op count = " + ops.size());
        System.out.println("[B2] errors: " + r.getErrors());

        assertEquals(1, ops.size(),
                "2-row multi-value INSERT must be captured as ONE table-op against 'tab', "
                        + "not double-counted (one op per VALUES row)");
        assertTrue(r.getErrors().isEmpty(), "2-row INSERT should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B3: five-row INSERT INTO tab VALUES (...) x5 — exactly one INSERT op, not five")
    void fiveRowInsert() {
        String sql = """
                CREATE PROCEDURE proc_insert_5()
                BEGIN
                  INSERT INTO tab (id, name) VALUES
                    (1, 'a'),
                    (2, 'b'),
                    (3, 'c'),
                    (4, 'd'),
                    (5, 'e');
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_insert_5", null);
        List<MySqlTableOpInfo> ops = insertOpsFor(r, "tab");
        System.out.println("[B3] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[B3] 'tab' INSERT op count = " + ops.size());
        System.out.println("[B3] errors: " + r.getErrors());

        assertEquals(1, ops.size(),
                "5-row multi-value INSERT must be captured as ONE table-op against 'tab', "
                        + "not quintuple-counted (one op per VALUES row)");
        assertTrue(r.getErrors().isEmpty(), "5-row INSERT should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B4: multi-row INSERT ... ON DUPLICATE KEY UPDATE — one INSERT op + UPSERT statement recorded")
    void multiRowInsertOnDuplicateKeyUpdate() {
        String sql = """
                CREATE PROCEDURE proc_insert_upsert()
                BEGIN
                  INSERT INTO tab (id, name, hits) VALUES
                    (1, 'a', 1),
                    (2, 'b', 1),
                    (3, 'c', 1)
                  ON DUPLICATE KEY UPDATE hits = hits + 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_insert_upsert", null);
        List<MySqlTableOpInfo> ops = insertOpsFor(r, "tab");
        System.out.println("[B4] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine()).toList());
        System.out.println("[B4] 'tab' INSERT op count = " + ops.size());
        System.out.println("[B4] statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@L" + s.getLine()).toList());
        System.out.println("[B4] errors: " + r.getErrors());

        assertEquals(1, ops.size(),
                "3-row multi-value INSERT ... ON DUPLICATE KEY UPDATE must be captured as ONE "
                        + "INSERT table-op against 'tab', not triple-counted");
        assertTrue(r.getStatements().stream().anyMatch(s -> "UPSERT".equals(s.getType())),
                "ON DUPLICATE KEY UPDATE clause must be recorded as an UPSERT statement");
        assertTrue(r.getErrors().isEmpty(),
                "multi-row INSERT..ON DUPLICATE KEY UPDATE should not raise parse errors: " + r.getErrors());
    }
}
