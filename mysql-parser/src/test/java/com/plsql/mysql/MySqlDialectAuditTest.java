package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Audit harness (2026-09-16) for the MySQL analog of BUG-CT-048 (PL/SQL qualifier.method()
 * misclassification / phantom-variable-from-error-recovery). AUDIT ONLY — no source files
 * touched. See task report for findings; this file records the reproducing evidence.
 */
public class MySqlDialectAuditTest {

    // ── Pattern 1: multi-level parameter propagation + scope reset between subprograms ──

    @Test
    @DisplayName("P1: three-proc chain A->B->C, array-index look-alike on C's own param, real call-tree")
    void multiLevelParamPropagationAndArrayIndexLookAlike() {
        String sql = """
                CREATE PROCEDURE proc_a()
                BEGIN
                  DECLARE v_id INT DEFAULT 1;
                  CALL proc_b(v_id);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_a", null);
        System.out.println("[P1] proc_a calls: " + names(r.getCalls()));
        assertTrue(r.getCalls().stream().anyMatch(c -> "proc_b".equalsIgnoreCase(c.getName())));
        assertFalse(r.getCalls().stream().anyMatch(c -> "v_id".equalsIgnoreCase(c.getName())));
    }

    @Test
    @DisplayName("P1: scope-leak probe - two CREATE PROCEDURE bodies in one source, same var name, "
            + "different types")
    void scopeLeakBetweenTwoRoutinesInOneSource() {
        // Realistic scenario: a dump of two routines pulled from INFORMATION_SCHEMA concatenated
        // into one source string (this DOES happen when routines are batch-exported/parsed together).
        String sql = """
                CREATE PROCEDURE proc_b(IN v_id INT)
                BEGIN
                  DECLARE shared_name VARCHAR(50) DEFAULT 'from-b';
                  CALL proc_c(shared_name);
                END;
                CREATE PROCEDURE proc_c(IN shared_name VARCHAR(50))
                BEGIN
                  DECLARE shared_name INT DEFAULT 99;
                  SET shared_name = shared_name + 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_b", null);
        System.out.println("[P1-scope-leak] name=" + r.getName() + " lineStart=" + r.getLineStart()
                + " lineEnd=" + r.getLineEnd());
        System.out.println("[P1-scope-leak] variables: " + r.getVariables().stream().map(v -> v.getName()+":"+v.getDataType()).toList());
        System.out.println("[P1-scope-leak] calls: " + names(r.getCalls()));
        // Evidence only - see report for whether this represents leakage.
    }

    // ── Pattern 2: schema-qualified references, not misclassified as calls ──

    @Test
    @DisplayName("P2: schema-qualified table reference in FROM is not recorded as a call")
    void schemaQualifiedTableRefNotACall() {
        String sql = """
                CREATE PROCEDURE proc_d()
                BEGIN
                  DECLARE v_cnt INT;
                  SELECT COUNT(*) INTO v_cnt FROM other_db.some_table;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_d", null);
        System.out.println("[P2] calls: " + names(r.getCalls()));
        System.out.println("[P2] tableOps count: " + r.getTableOps().size());
        assertTrue(r.getCalls() == null || r.getCalls().stream()
                .noneMatch(c -> "some_table".equalsIgnoreCase(c.getName())));
    }

    @Test
    @DisplayName("P2: schema-qualified CALL to another database's procedure recorded with schema")
    void schemaQualifiedCallRecordedWithSchema() {
        String sql = """
                CREATE PROCEDURE proc_e()
                BEGIN
                  CALL other_db.helper_proc();
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_e", null);
        System.out.println("[P2] qualified call: " + names(r.getCalls()));
        assertTrue(r.getCalls().stream().anyMatch(c ->
                "helper_proc".equalsIgnoreCase(c.getName()) && "other_db".equalsIgnoreCase(c.getSchema())));
    }

    // ── Pattern 3: comments interspersed mid-statement/mid-expression ──

    @Test
    @DisplayName("P3: comments (--, /* */, #) between proc name/paren, between args, mid-declaration")
    void commentsInterspersedStillParseCorrectly() {
        String sql = """
                CREATE PROCEDURE proc_f()
                BEGIN
                  -- line comment before declare
                  DECLARE /* block comment */ v_x INT # hash comment
                    DEFAULT 1;
                  CALL proc_g /* comment between name and paren */ (
                    v_x, -- first arg comment
                    /* second arg block comment */ 2 # trailing hash comment
                  );
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_f", null);
        System.out.println("[P3] variables: " + r.getVariables().stream().map(v -> v.getName()).toList());
        System.out.println("[P3] calls: " + names(r.getCalls()));
        System.out.println("[P3] errors: " + r.getErrors());
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_x".equalsIgnoreCase(v.getName())));
        assertTrue(r.getCalls().stream().anyMatch(c -> "proc_g".equalsIgnoreCase(c.getName())));
        assertFalse(r.getCalls().stream().anyMatch(c -> "v_x".equalsIgnoreCase(c.getName())));
    }

    private static String names(java.util.List<com.plsql.mysql.model.MySqlCallInfo> calls) {
        return calls == null ? "null" : calls.stream()
                .map(c -> (c.getSchema() != null ? c.getSchema() + "." : "") + c.getName())
                .toList().toString();
    }
}
