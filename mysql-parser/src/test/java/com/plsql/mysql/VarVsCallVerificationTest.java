package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Throwaway verification harness (2026-09-16): MySQL has no packages, so pattern C
 * (package-level global var) doesn't apply the same way - MySQL only has session/user
 * variables (@var) and no cross-routine "global" scoping construct at all, so there is
 * nothing analogous to check there. Patterns A and B are checked instead.
 */
public class VarVsCallVerificationTest {

    @Test
    @DisplayName("A: plain local variable assignment is not recorded as a call (MySQL)")
    void plainLocalVariableIsNotACall() {
        String sql = """
                CREATE PROCEDURE main_proc()
                BEGIN
                  DECLARE v_result INT;
                  DECLARE some_var INT DEFAULT 10;
                  SET v_result = some_var;
                  SET v_result = some_var + 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "main_proc", null);
        System.out.println("[MySQL A] calls: " + r.getCalls());
        assertTrue(r.getCalls() == null || r.getCalls().stream()
                .noneMatch(c -> "some_var".equalsIgnoreCase(c.getName())
                        || "v_result".equalsIgnoreCase(c.getName())));
    }

    @Test
    @DisplayName("B: variable passed as an actual parameter to a real call: call recorded, arg not (MySQL)")
    void variableArgumentNotMisrecordedAsCall() {
        String sql = """
                CREATE PROCEDURE proc_a()
                BEGIN
                  DECLARE my_local_var INT DEFAULT 5;
                  CALL proc_b(my_local_var);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_a", null);
        System.out.println("[MySQL B] calls: " + r.getCalls());
        boolean calledProcB = r.getCalls() != null && r.getCalls().stream()
                .anyMatch(c -> "proc_b".equalsIgnoreCase(c.getName()));
        boolean spurious = r.getCalls() != null && r.getCalls().stream()
                .anyMatch(c -> "my_local_var".equalsIgnoreCase(c.getName()));
        assertTrue(calledProcB, "Real call proc_a -> proc_b must be recorded");
        assertFalse(spurious, "The argument variable must NOT be recorded as a separate call target");
    }
}
