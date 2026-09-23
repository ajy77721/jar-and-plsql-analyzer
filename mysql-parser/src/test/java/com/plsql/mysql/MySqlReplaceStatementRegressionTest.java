package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for {@code REPLACE INTO}, added in the 2026-09-19 audit pass.
 *
 * <p>MySQL's delete-then-insert DML had no grammar rule at all, so the statement dispatcher
 * rejected it as "unsupported statement skipped": the target table (and, for the
 * {@code REPLACE … SELECT} form, the source table) never appeared in the table inventory.
 * {@code MySqlTableOpInfo.DmlType.REPLACE} already existed in the model but nothing produced it.
 */
class MySqlReplaceStatementRegressionTest {

    private final MySqlGrammarEngine engine = new MySqlGrammarEngine();

    private MySqlRoutineInfo run(String body) {
        return engine.parse("CREATE PROCEDURE p()\nBEGIN\n" + body + "\nEND\n", "p", null);
    }

    private static List<String> ops(MySqlRoutineInfo r) {
        return r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList();
    }

    @Test
    @DisplayName("REPLACE INTO … VALUES records a REPLACE table op with no errors")
    void replaceIntoValues() {
        MySqlRoutineInfo r = run("REPLACE INTO t_rep(a, b) VALUES (1, 2);");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("REPLACE:t_rep"), ops(r));
    }

    @Test
    @DisplayName("REPLACE … SELECT captures both the target and the source table")
    void replaceSelectCapturesSource() {
        MySqlRoutineInfo r = run("REPLACE INTO t_rep(a) SELECT a FROM t_src WHERE a > 0;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertTrue(ops(r).contains("REPLACE:t_rep"), ops(r).toString());
        assertTrue(ops(r).contains("SELECT:t_src"), ops(r).toString());
    }

    @Test
    @DisplayName("REPLACE without INTO and with a priority modifier still parses")
    void replaceWithoutIntoAndWithPriority() {
        MySqlRoutineInfo r = run("REPLACE LOW_PRIORITY t_rep(a) VALUES (1);");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("REPLACE:t_rep"), ops(r));
    }

    @Test
    @DisplayName("REPLACE does not disturb the statements around it")
    void siblingStatementsSurvive() {
        MySqlRoutineInfo r = run("""
                INSERT INTO t_ins(a) VALUES (1);
                REPLACE INTO t_rep(a) VALUES (2);
                UPDATE t_upd SET a = 3;""");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("INSERT:t_ins", "REPLACE:t_rep", "UPDATE:t_upd"), ops(r));
    }

    @Test
    @DisplayName("REPLACE() the string function is not mistaken for a REPLACE statement")
    void replaceFunctionStillWorks() {
        MySqlRoutineInfo r = run("SELECT REPLACE(name, 'a', 'b') FROM t_names;");
        assertTrue(r.getErrors().isEmpty(), "errors: " + r.getErrors());
        assertEquals(List.of("SELECT:t_names"), ops(r));
    }
}
