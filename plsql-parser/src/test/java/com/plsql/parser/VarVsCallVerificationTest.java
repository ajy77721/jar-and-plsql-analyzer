package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Throwaway verification harness (2026-09-16) requested by user: confirms that the Oracle
 * PL/SQL analyzer correctly distinguishes real procedure/function CALLS from:
 *   A. a plain local variable reference/assignment
 *   B. a variable passed AS AN ARGUMENT to a real call (the call is recorded, the arg is not)
 *   C. a package-level global variable accessed qualified from another package (PKG.g_var)
 * Not a permanent regression suite by itself (overlaps CallClassificationTest /
 * CollectionParamCallFalsePositiveTest) - kept simple and printing actual output for evidence.
 */
public class VarVsCallVerificationTest extends ParserTestBase {

    private static String describe(java.util.List<CallInfo> calls) {
        if (calls == null) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (CallInfo c : calls) {
            sb.append(c.getName()).append(":").append(c.getType())
              .append(c.getPackageName() != null && !c.getPackageName().isEmpty() ? "@" + c.getPackageName() : "")
              .append(", ");
        }
        return sb.append("]").toString();
    }

    @Test
    @DisplayName("A: plain local variable assignment is never recorded as a call")
    void plainLocalVariableIsNotACall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE MAIN_PROC IS
                    v_result NUMBER;
                    some_var NUMBER := 10;
                  BEGIN
                    v_result := some_var;
                    v_result := some_var + 1;
                  END MAIN_PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo main = findSub(result, "MAIN_PROC");
        assertNotNull(main);
        System.out.println("[A] calls recorded for MAIN_PROC: " + describe(main.getCalls()));
        assertTrue(main.getCalls() == null || main.getCalls().stream()
                .noneMatch(c -> "SOME_VAR".equalsIgnoreCase(c.getName())
                        || "V_RESULT".equalsIgnoreCase(c.getName())));
    }

    @Test
    @DisplayName("B: variable passed as an actual PARAMETER to a real call: call recorded, arg not")
    void variableArgumentNotMisrecordedAsCall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC_B(p_val NUMBER) IS
                  BEGIN
                    NULL;
                  END PROC_B;

                  PROCEDURE PROC_A IS
                    my_local_var NUMBER := 5;
                  BEGIN
                    PROC_B(my_local_var);
                  END PROC_A;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo procA = findSub(result, "PROC_A");
        assertNotNull(procA);
        System.out.println("[B] calls recorded for PROC_A: " + describe(procA.getCalls()));
        boolean calledProcB = procA.getCalls() != null && procA.getCalls().stream()
                .anyMatch(c -> "PROC_B".equalsIgnoreCase(c.getName()) && "INTERNAL".equalsIgnoreCase(c.getType()));
        boolean spuriousArgCall = procA.getCalls() != null && procA.getCalls().stream()
                .anyMatch(c -> "MY_LOCAL_VAR".equalsIgnoreCase(c.getName()));
        assertTrue(calledProcB, "Real call PROC_A -> PROC_B must be recorded");
        assertFalse(spuriousArgCall, "The argument variable must NOT be recorded as a separate call target");
    }

    @Test
    @DisplayName("C: package-qualified global variable access (PKG_X.g_flag) is not a call")
    void qualifiedPackageGlobalVariableIsNotACall() {
        // g_some_flag is a package-level variable of PKG_X (declared in the package spec/body,
        // modeled here via a package body with a package-level variable), referenced qualified
        // from PKG_Y's procedure. This must not be recorded as a call to a procedure named
        // g_some_flag inside PKG_X.
        String sql = """
                CREATE OR REPLACE PACKAGE BODY PKG_Y AS
                  PROCEDURE USES_GLOBAL IS
                  BEGIN
                    PKG_X.G_SOME_FLAG := 'Y';
                    IF PKG_X.G_SOME_FLAG = 'Y' THEN
                      NULL;
                    END IF;
                  END USES_GLOBAL;
                END PKG_Y;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "USES_GLOBAL");
        assertNotNull(proc);
        System.out.println("[C] calls recorded for USES_GLOBAL: " + describe(proc.getCalls()));
        boolean spuriousCall = proc.getCalls() != null && proc.getCalls().stream()
                .anyMatch(c -> "G_SOME_FLAG".equalsIgnoreCase(c.getName()));
        assertFalse(spuriousCall,
                "A package-qualified global variable assignment/read must not be misclassified as a call "
                        + "to a routine named after the variable");
    }

    @Test
    @DisplayName("C2: package-qualified global variable used as a call ARGUMENT is not a call, but the real call is")
    void qualifiedPackageGlobalAsCallArgument() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY PKG_Y AS
                  PROCEDURE USES_GLOBAL IS
                  BEGIN
                    OTHER_PKG.DO_WORK(PKG_X.G_SOME_FLAG);
                  END USES_GLOBAL;
                END PKG_Y;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "USES_GLOBAL");
        assertNotNull(proc);
        System.out.println("[C2] calls recorded for USES_GLOBAL: " + describe(proc.getCalls()));
        boolean realCall = proc.getCalls() != null && proc.getCalls().stream()
                .anyMatch(c -> "DO_WORK".equalsIgnoreCase(c.getName()) && "EXTERNAL".equalsIgnoreCase(c.getType()));
        boolean spuriousCall = proc.getCalls() != null && proc.getCalls().stream()
                .anyMatch(c -> "G_SOME_FLAG".equalsIgnoreCase(c.getName()));
        assertTrue(realCall, "Real call to OTHER_PKG.DO_WORK must be recorded as EXTERNAL");
        assertFalse(spuriousCall, "The qualified global variable argument must not be its own call node");
    }
}
