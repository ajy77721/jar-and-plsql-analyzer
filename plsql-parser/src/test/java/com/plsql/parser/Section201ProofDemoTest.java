package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * §201 PROOF DEMO — not a permanent regression test, a throwaway harness used to capture
 * concrete BEFORE/AFTER console output for the docs/TASK-REGISTRY.md write-up. Prints the
 * full call list of MAIN_PROC so the phantom P_ARR/EXTERNAL call (or its absence) is visible
 * directly in the test-run console output.
 */
@DisplayName("§201 PROOF DEMO: print MAIN_PROC's call list for the collection-parameter case")
public class Section201ProofDemoTest extends ParserTestBase {

    @Test
    @DisplayName("Print calls recorded for a procedure with a collection-typed parameter indexed p_arr(1)")
    void printCallsForCollectionParamCase() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  TYPE t_num_arr IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;

                  PROCEDURE MAIN_PROC(p_arr IN t_num_arr) IS
                    v_val NUMBER;
                  BEGIN
                    v_val := p_arr(1) + 1;
                    DBMS_OUTPUT.PUT_LINE('done');
                  END MAIN_PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo main = findSub(result, "MAIN_PROC");
        System.out.println("=== §201 PROOF DEMO OUTPUT START ===");
        if (main.getCalls() == null || main.getCalls().isEmpty()) {
            System.out.println("MAIN_PROC.getCalls() = <empty>");
        } else {
            for (CallInfo ci : main.getCalls()) {
                System.out.println("MAIN_PROC call -> name=" + ci.getName()
                        + " package=" + ci.getPackageName()
                        + " type=" + ci.getType());
            }
        }
        System.out.println("=== §201 PROOF DEMO OUTPUT END ===");
    }
}
