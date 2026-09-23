package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * §201: a collection/varray-typed formal parameter indexed as p_arr(1) is syntactically identical
 * to a call NAME(args). Before this fix, addCall's existing local-variable filter
 * (isDeclaredVariableInScope) checked SubprogramInfo.getLocalVariables() but not
 * SubprogramInfo.getParameters() — a separate list — so an indexed parameter was misclassified as
 * an EXTERNAL call to a procedure named after the parameter.
 */
@DisplayName("Call Classification: collection-typed parameter is not a false-positive call")
public class CollectionParamCallFalsePositiveTest extends ParserTestBase {

    @Test
    @DisplayName("Indexed collection-typed local variable is correctly NOT a call (pre-existing filter)")
    void localArrayVariableNotMisclassifiedAsCall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  TYPE t_num_arr IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;

                  PROCEDURE MAIN_PROC IS
                    v_val NUMBER;
                    my_array t_num_arr;
                  BEGIN
                    v_val := my_array(1) + 1;
                  END MAIN_PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo main = findSub(result, "MAIN_PROC");
        assertNotNull(main);
        assertTrue(main.getCalls() == null || main.getCalls().stream()
                        .noneMatch(c -> "MY_ARRAY".equalsIgnoreCase(c.getName())),
                "Indexed local collection variable must not be recorded as a call");
    }

    @Test
    @DisplayName("Indexed collection-typed formal PARAMETER is correctly NOT a call (the real gap, now fixed)")
    void paramArrayNotMisclassifiedAsCall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  TYPE t_num_arr IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;

                  PROCEDURE MAIN_PROC(p_arr IN t_num_arr) IS
                    v_val NUMBER;
                  BEGIN
                    v_val := p_arr(1) + 1;
                  END MAIN_PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo main = findSub(result, "MAIN_PROC");
        assertNotNull(main);
        assertTrue(main.getCalls() == null || main.getCalls().stream()
                        .noneMatch(c -> "P_ARR".equalsIgnoreCase(c.getName())),
                "Indexed collection-typed parameter must not be recorded as a call");
    }
}
