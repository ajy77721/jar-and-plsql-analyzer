package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression suite (fixed 2026-09-16) for BUG-CT-048 and related scope-tracking issues for
 * Oracle PL/SQL, using realistic multi-procedure fixtures. Asserts the CORRECT (fixed) behavior:
 * qualifier.method() calls where the qualifier resolves to a declared variable/parameter/global
 * in the current scope are no longer misclassified as external calls to a package named after
 * that variable. Formerly named BugCt048OracleAuditTest (audit-only, several assertions
 * documented as expected failures); renamed once PlSqlAnalysisVisitor.addCall/
 * visitVariable_declaration/extractParameter were fixed so every assertion below now passes.
 */
public class VarQualifierScopeClassificationTest extends ParserTestBase {

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
    @DisplayName("1: multi-level param propagation A->B->C, C.v_subject_param.EXTEND() must not be an external call")
    void multiLevelParamPropagationExtend() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY ORDER_PKG AS

                  PROCEDURE PROC_C(v_subject_param IN OUT SYS.ODCIVARCHAR2LIST) IS
                  BEGIN
                    -- grow the collection passed down from A via B
                    v_subject_param.EXTEND();
                    v_subject_param(v_subject_param.COUNT) := 'X';
                  END PROC_C;

                  PROCEDURE PROC_B(v_subject_param IN OUT SYS.ODCIVARCHAR2LIST) IS
                  BEGIN
                    PROC_C(v_subject_param);
                  END PROC_B;

                  PROCEDURE PROC_A IS
                    v_subject_param SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
                  BEGIN
                    PROC_B(v_subject_param);
                  END PROC_A;

                END ORDER_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo procC = findSub(result, "PROC_C");
        assertNotNull(procC);
        System.out.println("[1] calls recorded for PROC_C: " + describe(procC.getCalls()));
        boolean spuriousExtendCall = procC.getCalls() != null && procC.getCalls().stream()
                .anyMatch(c -> "V_SUBJECT_PARAM".equalsIgnoreCase(c.getPackageName())
                        && "EXTEND".equalsIgnoreCase(c.getName()));
        System.out.println("[1] spuriousExtendCall(qualifier misread as package)=" + spuriousExtendCall);
        assertFalse(spuriousExtendCall,
                "BUG-CT-048: v_subject_param.EXTEND() on C's own IN OUT param must not be recorded as EXTERNAL call to package V_SUBJECT_PARAM");

        // scope-reset sanity: PROC_A and PROC_B also use the same param/var name - ensure no leakage
        SubprogramInfo procA = findSub(result, "PROC_A");
        SubprogramInfo procB = findSub(result, "PROC_B");
        assertNotNull(procA);
        assertNotNull(procB);
        boolean aCallsProcB = procA.getCalls() != null && procA.getCalls().stream()
                .anyMatch(c -> "PROC_B".equalsIgnoreCase(c.getName()));
        boolean bCallsProcC = procB.getCalls() != null && procB.getCalls().stream()
                .anyMatch(c -> "PROC_C".equalsIgnoreCase(c.getName()));
        System.out.println("[1] A->B recorded=" + aCallsProcB + " B->C recorded=" + bCallsProcC);
        assertTrue(aCallsProcB, "A must record call to B");
        assertTrue(bCallsProcC, "B must record call to C");
    }

    @Test
    @DisplayName("2a: package-global var SAME name as local var in a DIFFERENT proc, same package - EXTEND on the global")
    void globalVarSamePackageExtend() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY ORDER_PKG AS

                  v_subject_param SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();

                  PROCEDURE OTHER_PROC IS
                    v_subject_param SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
                  BEGIN
                    v_subject_param.EXTEND();
                  END OTHER_PROC;

                  PROCEDURE USE_GLOBAL_PROC IS
                  BEGIN
                    v_subject_param.EXTEND();
                  END USE_GLOBAL_PROC;

                END ORDER_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo useGlobal = findSub(result, "USE_GLOBAL_PROC");
        assertNotNull(useGlobal);
        System.out.println("[2a] calls recorded for USE_GLOBAL_PROC: " + describe(useGlobal.getCalls()));
        boolean spurious = useGlobal.getCalls() != null && useGlobal.getCalls().stream()
                .anyMatch(c -> "V_SUBJECT_PARAM".equalsIgnoreCase(c.getPackageName()) && "EXTEND".equalsIgnoreCase(c.getName()));
        assertFalse(spurious, "package-global var .EXTEND() referenced with no local shadow must not be a call");
    }

    @Test
    @DisplayName("2b: qualified access to a DIFFERENT package's global var: OTHER_PKG.v_subject_param.EXTEND()")
    void globalVarDifferentPackageExtend() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY CONSUMER_PKG AS

                  PROCEDURE CONSUME_IT IS
                  BEGIN
                    OTHER_PKG.v_subject_param.EXTEND();
                  END CONSUME_IT;

                END CONSUMER_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo consumeIt = findSub(result, "CONSUME_IT");
        assertNotNull(consumeIt);
        System.out.println("[2b] calls recorded for CONSUME_IT: " + describe(consumeIt.getCalls()));
        // We can't know OTHER_PKG's var declarations from here (cross-package), so the analyzer
        // will very likely (mis)classify this as EXTERNAL call OTHER_PKG.EXTEND / OTHER_PKG.v_subject_param.EXTEND.
        // This is expected/documented limitation for cross-package unresolved qualifiers, distinct from BUG-CT-048
        // (which is specifically about qualifiers resolvable to a LOCAL/param variable in the SAME scope).
        boolean recordedAsExternal = consumeIt.getCalls() != null && !consumeIt.getCalls().isEmpty();
        System.out.println("[2b] recordedAsExternal(expected, cross-package unresolvable)=" + recordedAsExternal);
    }

    @Test
    @DisplayName("3: comments interspersed mid-statement around a real call and around EXTEND()")
    void commentsInterspersedRealisticMultiProc() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY REPORT_PKG AS

                  /* helper that logs a message -- used everywhere */
                  PROCEDURE LOG_MSG /* proc name */ ( /* open paren comment */
                    p_msg -- the message
                      IN VARCHAR2
                  ) IS
                  BEGIN
                    NULL;
                  END LOG_MSG;

                  PROCEDURE BUILD_REPORT(
                    v_subject_param -- collection of subjects
                      IN OUT SYS.ODCIVARCHAR2LIST
                  ) IS
                    v_count -- local counter
                      NUMBER := 0; -- init
                  BEGIN
                    LOG_MSG /* which proc */ ( 'starting' /* first arg */ );
                    v_subject_param -- qualifier
                      . -- dot
                      EXTEND -- method name
                      (); -- call
                    v_count := v_subject_param.COUNT; -- real attribute access, not a call
                    LOG_MSG(
                      'done' -- second call, arg after comment
                    );
                  END BUILD_REPORT;

                END REPORT_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "comments-interspersed fixture must parse cleanly");
        SubprogramInfo build = findSub(result, "BUILD_REPORT");
        assertNotNull(build);
        System.out.println("[3] calls recorded for BUILD_REPORT: " + describe(build.getCalls()));
        long logMsgCalls = build.getCalls() == null ? 0 : build.getCalls().stream()
                .filter(c -> "LOG_MSG".equalsIgnoreCase(c.getName())).count();
        assertEquals(2, logMsgCalls, "both comment-interspersed LOG_MSG calls must be captured despite comments");
        boolean spuriousExtend = build.getCalls() != null && build.getCalls().stream()
                .anyMatch(c -> "V_SUBJECT_PARAM".equalsIgnoreCase(c.getPackageName()) && "EXTEND".equalsIgnoreCase(c.getName()));
        System.out.println("[3] spuriousExtend(with comments interspersed)=" + spuriousExtend);
        assertFalse(spuriousExtend, "comment-interspersed v_subject_param . EXTEND () on own param must not be a call");
    }

    @Test
    @DisplayName("4: messy multi-procedure file with unrelated broken syntax near EXTEND-on-param usage (phantom-var probe)")
    void messyFileWithUnrelatedBrokenSyntax() {
        // Deliberately includes a syntactically broken/unusual declaration in an unrelated sibling
        // procedure to see if ANTLR error recovery leaks a phantom variable into scope tracking
        // that affects PROC_REAL's classification of v_subject_param.EXTEND().
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MESSY_PKG AS

                  PROCEDURE BROKEN_NEIGHBOR IS
                    v_weird NUMBER(10,2 -- missing closing paren on purpose, unrelated broken decl
                    x VARCHAR2(10);
                  BEGIN
                    NULL;
                  END BROKEN_NEIGHBOR;

                  PROCEDURE PROC_REAL(v_subject_param IN OUT SYS.ODCIVARCHAR2LIST) IS
                    v_other NUMBER;
                  BEGIN
                    v_subject_param.EXTEND();
                    v_other := 1;
                  END PROC_REAL;

                END MESSY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("[4] parse errors: " + result.getErrors());
        SubprogramInfo procReal = findSub(result, "PROC_REAL");
        assertNotNull(procReal, "PROC_REAL should still be found despite unrelated broken sibling procedure");
        System.out.println("[4] PROC_REAL params: " + procReal.getParameters());
        System.out.println("[4] PROC_REAL localVars: " + procReal.getLocalVariables());
        System.out.println("[4] calls recorded for PROC_REAL: " + describe(procReal.getCalls()));
        boolean spuriousExtend = procReal.getCalls() != null && procReal.getCalls().stream()
                .anyMatch(c -> "V_SUBJECT_PARAM".equalsIgnoreCase(c.getPackageName()) && "EXTEND".equalsIgnoreCase(c.getName()));
        assertFalse(spuriousExtend, "unrelated broken sibling syntax should not affect PROC_REAL's own-param EXTEND classification");
    }
}
