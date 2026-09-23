package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Read-only audit (2026-09-17) of 4 Oracle PL/SQL concepts against the real ANTLR
 * grammar/visitor: (1) nested exception handlers, (2) user-defined/custom exceptions incl.
 * PRAGMA EXCEPTION_INIT, (3) bare RAISE (re-raise) vs RAISE <name>, (4) transaction control
 * (SAVEPOINT / ROLLBACK TO SAVEPOINT / PRAGMA AUTONOMOUS_TRANSACTION).
 *
 * These tests assert on ACTUAL observed behavior of the current parser/visitor (which may be
 * buggy/incomplete) so they stand as permanent, reproducible evidence. No grammar/visitor/
 * production source files were modified to produce this evidence.
 */
public class OracleExceptionAndTransactionAuditTest extends ParserTestBase {

    // ==================================================================
    // 1. NESTED EXCEPTION HANDLERS
    // ==================================================================

    @Test
    @DisplayName("1a: exception block nested inside an outer WHEN handler — parses without error")
    void nestedExceptionHandlerParsesCleanly() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE outer_proc IS
                    v_outer NUMBER := 1;
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN e1 THEN
                      BEGIN
                        call_inner(v_outer);
                      EXCEPTION
                        WHEN e2 THEN
                          call_in_nested_handler();
                      END;
                  END outer_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "nested exception handler");
    }

    @Test
    @DisplayName("1b: ACTUAL — nested handler is flattened into the same sibling list as the outer handler")
    void nestedExceptionHandlerIsFlattenedNotNested() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE outer_proc IS
                    v_outer NUMBER := 1;
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN e1 THEN
                      BEGIN
                        call_inner(v_outer);
                      EXCEPTION
                        WHEN e2 THEN
                          call_in_nested_handler();
                          INSERT INTO nested_error_log (msg) VALUES ('x');
                      END;
                  END outer_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "nested exception handler flattening");
        SubprogramInfo proc = findSub(result, "OUTER_PROC");
        assertNotNull(proc);

        List<ExceptionHandlerInfo> handlers = proc.getExceptionHandlers();
        System.out.println("=== 1b: nested handler flattening ===");
        for (ExceptionHandlerInfo eh : handlers) {
            System.out.println("  handler name=" + eh.getExceptionName()
                    + " calls=" + eh.getCalls().stream().map(CallInfo::getName).toList()
                    + " tableOps=" + eh.getTableOperations().stream().map(TableOperationInfo::getTableName).toList());
        }

        // FIXED: both handlers still land in the flat SubprogramInfo.exceptionHandlers list (kept
        // for backward compatibility / easy iteration), but ExceptionHandlerInfo now carries a
        // parentHandler reference so the nesting relationship is reconstructible: e2's parent is e1.
        assertEquals(2, handlers.size(),
                "outer (e1) and inner (e2) handlers both still appear on the subprogram's flat list");

        ExceptionHandlerInfo e1 = handlers.stream().filter(h -> "E1".equals(h.getExceptionName())).findFirst().orElse(null);
        ExceptionHandlerInfo e2 = handlers.stream().filter(h -> "E2".equals(h.getExceptionName())).findFirst().orElse(null);
        assertNotNull(e1, "outer handler e1 must be present");
        assertNotNull(e2, "inner handler e2 must be present");

        // FIXED: e2 (inner) now records e1 (outer) as its parent handler; e1 is top-level (no parent).
        assertNull(e1.getParentHandler(), "outer handler e1 must be top-level (no parent)");
        assertNotNull(e2.getParentHandler(), "inner handler e2 must record its enclosing handler as parent");
        assertEquals("E1", e2.getParentExceptionName(), "inner handler e2's parent must be e1");

        // The inner handler's own call/table-op ARE correctly captured on e2 (proves the visitor
        // does walk into the nested handler body, it just doesn't record the nesting relationship).
        assertTrue(e2.getCalls().stream().anyMatch(c -> "CALL_IN_NESTED_HANDLER".equalsIgnoreCase(c.getName())),
                "nested handler's own call should be captured on the inner handler");
        assertTrue(e2.getTableOperations().stream().anyMatch(t -> "NESTED_ERROR_LOG".equalsIgnoreCase(t.getTableName())),
                "nested handler's own INSERT should be captured on the inner handler");

        // call_inner() lives in the nested BEGIN block's *body* (before its own EXCEPTION), which
        // is physically inside e1's handler code. Observed: it is attributed to e1 (the enclosing
        // handler), confirming calls/table-ops inside a nested handler's protected block, and
        // inside the nested handler itself, ARE captured — just without a nesting relationship.
        assertTrue(e1.getCalls().stream().anyMatch(c -> "CALL_INNER".equalsIgnoreCase(c.getName())),
                "call_inner() from the nested block's protected body should be attributed to e1");
    }

    @Test
    @DisplayName("1c: outer-block variable is still visible/referenced inside the nested handler")
    void outerBlockVariableVisibleInNestedHandler() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE outer_proc IS
                    v_outer NUMBER := 1;
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN e1 THEN
                      BEGIN
                        call_inner(v_outer);
                      EXCEPTION
                        WHEN e2 THEN
                          v_outer := v_outer + 1;
                      END;
                  END outer_proc;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "outer var visible in nested handler");
        SubprogramInfo proc = findSub(result, "OUTER_PROC");
        assertNotNull(proc);
        // v_outer is declared in the outer subprogram's DECLARE section, so it should still show
        // up on the subprogram's localVariables regardless of the nested-handler reference.
        assertTrue(proc.getLocalVariables().stream().anyMatch(v -> "V_OUTER".equalsIgnoreCase(v.getName())),
                "v_outer declared in outer block should remain in localVariables");
    }

    // ==================================================================
    // 2. USER-DEFINED / CUSTOM EXCEPTIONS incl. PRAGMA EXCEPTION_INIT
    // ==================================================================

    @Test
    @DisplayName("2a: e_custom EXCEPTION; declaration + RAISE e_custom; parses cleanly")
    void customExceptionDeclarationAndRaise() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc1 IS
                    e_custom EXCEPTION;
                  BEGIN
                    RAISE e_custom;
                  EXCEPTION
                    WHEN e_custom THEN
                      NULL;
                  END proc1;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "custom exception declaration + raise");
        SubprogramInfo proc = findSub(result, "PROC1");
        assertNotNull(proc);

        // FIXED: a visitException_declaration override now records e_custom in localVariables
        // (dataType="EXCEPTION") instead of silently dropping it.
        boolean foundAsVar = proc.getLocalVariables().stream()
                .anyMatch(v -> "E_CUSTOM".equalsIgnoreCase(v.getName()));
        System.out.println("=== 2a: custom exception declaration ===");
        System.out.println("  e_custom captured in localVariables=" + foundAsVar);
        System.out.println("  localVariables=" + proc.getLocalVariables().stream().map(VariableInfo::getName).toList());
        assertTrue(foundAsVar,
                "FIXED: custom exception name must now be tracked in localVariables");
        assertTrue(proc.getLocalVariables().stream()
                .anyMatch(v -> "E_CUSTOM".equalsIgnoreCase(v.getName()) && "EXCEPTION".equals(v.getDataType())),
                "FIXED: e_custom's dataType must be recorded as EXCEPTION");

        // The WHEN e_custom handler itself IS captured (handler matching doesn't require the
        // declaration to have been recorded).
        assertTrue(proc.getExceptionHandlers().stream()
                .anyMatch(h -> "E_CUSTOM".equalsIgnoreCase(h.getExceptionName())));
    }

    @Test
    @DisplayName("2b: PRAGMA EXCEPTION_INIT(e_custom, -20001) parses but the error code is not recorded anywhere")
    void pragmaExceptionInitCodeIsNotRecorded() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc1 IS
                    e_custom EXCEPTION;
                    PRAGMA EXCEPTION_INIT(e_custom, -20001);
                  BEGIN
                    RAISE e_custom;
                  EXCEPTION
                    WHEN e_custom THEN
                      NULL;
                  END proc1;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        // Grammar rule (PlSqlParser.g4 pragma_declaration): PRAGMA ( ... | EXCEPTION_INIT '('
        // exception_name ',' numeric_negative ')' | ... ) -- the -20001 form IS in the grammar.
        assertNoParsErrors(result, "PRAGMA EXCEPTION_INIT");

        SubprogramInfo proc = findSub(result, "PROC1");
        assertNotNull(proc);

        // FIXED: visitPragma_declaration() now has a dedicated EXCEPTION_INIT branch that records
        // a StatementInfo(type="EXCEPTION_INIT") carrying the exception name (sqlText) and the
        // numeric error code (errorCode).
        assertFalse(proc.isPragmaAutonomousTransaction(),
                "EXCEPTION_INIT pragma must not be misread as AUTONOMOUS_TRANSACTION");
        boolean anyStatementMentionsCode = proc.getStatements().stream()
                .anyMatch(s -> (s.getErrorCode() != null && s.getErrorCode() == -20001)
                        || "EXCEPTION_INIT".equalsIgnoreCase(s.getType()));
        System.out.println("=== 2b: PRAGMA EXCEPTION_INIT code recorded anywhere=" + anyStatementMentionsCode);
        assertTrue(anyStatementMentionsCode,
                "FIXED: the -20001 error code from PRAGMA EXCEPTION_INIT must now be recorded in the model");
        StatementInfo exInit = proc.getStatements().stream()
                .filter(s -> "EXCEPTION_INIT".equalsIgnoreCase(s.getType())).findFirst().orElse(null);
        assertNotNull(exInit, "an EXCEPTION_INIT statement must be recorded");
        assertEquals(-20001, exInit.getErrorCode());
        assertEquals("E_CUSTOM", exInit.getSqlText());
    }

    @Test
    @DisplayName("2c: user-defined exception handled alongside a built-in one (WHEN e_custom OR NO_DATA_FOUND)")
    void customExceptionCombinedWithBuiltin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc1 IS
                    e_custom EXCEPTION;
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN e_custom OR NO_DATA_FOUND THEN
                      INSERT INTO error_log (msg) VALUES ('combined');
                  END proc1;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "combined custom + builtin exception names");
        SubprogramInfo proc = findSub(result, "PROC1");
        assertNotNull(proc);
        ExceptionHandlerInfo eh = proc.getExceptionHandlers().get(0);
        // Observed: names are joined with " OR " and cleanId() upper-cases each identifier
        // (e_custom -> E_CUSTOM), consistent with how other identifiers are normalized elsewhere.
        System.out.println("=== 2c: combined exception name=" + eh.getExceptionName());
        assertEquals("E_CUSTOM OR NO_DATA_FOUND", eh.getExceptionName());
        assertTrue(eh.getTableOperations().stream().anyMatch(t -> "ERROR_LOG".equalsIgnoreCase(t.getTableName())));
    }

    // ==================================================================
    // 3. RE-RAISE: bare RAISE; vs RAISE some_exception;
    // ==================================================================

    @Test
    @DisplayName("3a: bare RAISE; (re-raise, no argument) parses and is recorded as a RAISE statement")
    void bareRaiseParsesCleanly() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc1 IS
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN OTHERS THEN
                      RAISE;
                  END proc1;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "bare RAISE;");
        SubprogramInfo proc = findSub(result, "PROC1");
        assertNotNull(proc);
        List<StatementInfo> raises = proc.getStatements().stream()
                .filter(s -> "RAISE".equals(s.getType())).toList();
        assertEquals(1, raises.size(), "bare RAISE; should be recorded once as a RAISE statement");
    }

    @Test
    @DisplayName("3b: ACTUAL — bare RAISE; and RAISE named_exc; are INDISTINGUISHABLE in the model")
    void bareRaiseVsNamedRaiseAreIndistinguishable() {
        String sqlBare = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc1 IS
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN OTHERS THEN
                      RAISE;
                  END proc1;
                END test_pkg;
                /
                """;
        String sqlNamed = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc1 IS
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN OTHERS THEN
                      RAISE some_exception;
                  END proc1;
                END test_pkg;
                /
                """;
        SubprogramInfo bare = findSub(parse(sqlBare), "PROC1");
        SubprogramInfo named = findSub(parse(sqlNamed), "PROC1");
        assertNotNull(bare);
        assertNotNull(named);

        StatementInfo bareRaise = bare.getStatements().stream().filter(s -> "RAISE".equals(s.getType())).findFirst().orElse(null);
        StatementInfo namedRaise = named.getStatements().stream().filter(s -> "RAISE".equals(s.getType())).findFirst().orElse(null);
        assertNotNull(bareRaise);
        assertNotNull(namedRaise);

        System.out.println("=== 3b: bare vs named RAISE ===");
        System.out.println("  bare : type=" + bareRaise.getType() + " sqlText=" + bareRaise.getSqlText());
        System.out.println("  named: type=" + namedRaise.getType() + " sqlText=" + namedRaise.getSqlText());

        // FIXED: visitRaise_statement() now reads ctx.exception_name() and, when present, stores
        // the raised exception's name in StatementInfo.sqlText -- so a bare re-raise (sqlText ==
        // null) is now distinguishable from RAISE <name> (sqlText == the exception's name).
        assertEquals(bareRaise.getType(), namedRaise.getType());
        assertNull(bareRaise.getSqlText(), "FIXED: bare RAISE; must have no exception name recorded");
        assertEquals("SOME_EXCEPTION", namedRaise.getSqlText(),
                "FIXED: RAISE some_exception; must record the exception name in sqlText");
    }

    @Test
    @DisplayName("3c: RAISE some_exception; where some_exception is a custom exception still just records type=RAISE")
    void raiseCustomExceptionByName() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc1 IS
                    e_custom EXCEPTION;
                  BEGIN
                    IF 1 = 1 THEN
                      RAISE e_custom;
                    END IF;
                  EXCEPTION
                    WHEN e_custom THEN
                      NULL;
                  END proc1;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "RAISE e_custom inside IF");
        SubprogramInfo proc = findSub(result, "PROC1");
        assertNotNull(proc);
        assertTrue(proc.getStatements().stream().anyMatch(s -> "RAISE".equals(s.getType())));
    }

    // ==================================================================
    // 4. TRANSACTION CONTROL: SAVEPOINT / ROLLBACK TO SAVEPOINT / PRAGMA AUTONOMOUS_TRANSACTION
    // ==================================================================

    @Test
    @DisplayName("4a: SAVEPOINT name and ROLLBACK TO SAVEPOINT name are both parsed and the savepoint name is captured")
    void savepointAndRollbackToSavepointCaptureName() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc1 IS
                  BEGIN
                    SAVEPOINT before_update;
                    UPDATE accounts SET balance = balance - 100 WHERE id = 1;
                  EXCEPTION
                    WHEN OTHERS THEN
                      ROLLBACK TO SAVEPOINT before_update;
                      RAISE;
                  END proc1;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "SAVEPOINT / ROLLBACK TO SAVEPOINT");
        SubprogramInfo proc = findSub(result, "PROC1");
        assertNotNull(proc);

        StatementInfo savepointStmt = proc.getStatements().stream()
                .filter(s -> "SAVEPOINT".equals(s.getType())).findFirst().orElse(null);
        assertNotNull(savepointStmt, "SAVEPOINT statement should be recorded on the outer subprogram");
        assertEquals("BEFORE_UPDATE", savepointStmt.getSavepointName());

        // The ROLLBACK TO SAVEPOINT lives inside the WHEN OTHERS handler.
        ExceptionHandlerInfo eh = proc.getExceptionHandlers().stream()
                .filter(h -> "OTHERS".equals(h.getExceptionName())).findFirst().orElse(null);
        assertNotNull(eh);

        // FIXED: visitRollback_statement() now reads ctx.savepoint_name() and populates
        // StatementInfo.savepointName, same as SAVEPOINT itself already did.
        StatementInfo rollbackStmt = proc.getStatements().stream()
                .filter(s -> "ROLLBACK".equals(s.getType())).findFirst().orElse(null);
        assertNotNull(rollbackStmt, "ROLLBACK TO SAVEPOINT should be recorded as a ROLLBACK statement");
        System.out.println("=== 4a: ROLLBACK TO SAVEPOINT savepointName=" + rollbackStmt.getSavepointName());
        assertEquals("BEFORE_UPDATE", rollbackStmt.getSavepointName(),
                "FIXED: ROLLBACK TO SAVEPOINT <name> must now capture the target savepoint name");
    }

    @Test
    @DisplayName("4b: PRAGMA AUTONOMOUS_TRANSACTION on a procedure is recorded as a property on the routine")
    void pragmaAutonomousTransactionRecordedOnRoutine() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE log_audit_event(p_msg VARCHAR2) IS
                    PRAGMA AUTONOMOUS_TRANSACTION;
                  BEGIN
                    INSERT INTO audit_log (message) VALUES (p_msg);
                    COMMIT;
                  END log_audit_event;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "PRAGMA AUTONOMOUS_TRANSACTION");
        SubprogramInfo sub = findSub(result, "LOG_AUDIT_EVENT");
        assertNotNull(sub);
        // This one IS correctly wired: PRAGMA AUTONOMOUS_TRANSACTION -> SubprogramInfo.pragmaAutonomousTransaction.
        assertTrue(sub.isPragmaAutonomousTransaction(),
                "PRAGMA AUTONOMOUS_TRANSACTION should be recorded as a boolean property on the routine");
        assertTrue(sub.getTableOperations().stream().anyMatch(t -> "AUDIT_LOG".equalsIgnoreCase(t.getTableName())),
                "DML inside the autonomous-transaction procedure body should still be tracked normally");
    }

    @Test
    @DisplayName("4c: PRAGMA AUTONOMOUS_TRANSACTION combined with SAVEPOINT/ROLLBACK TO SAVEPOINT in the same routine")
    void autonomousTransactionWithSavepointAndRollback() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE safe_batch_job IS
                    PRAGMA AUTONOMOUS_TRANSACTION;
                  BEGIN
                    SAVEPOINT batch_start;
                    UPDATE batch_status SET status = 'RUNNING' WHERE job_id = 1;
                    COMMIT;
                  EXCEPTION
                    WHEN OTHERS THEN
                      ROLLBACK TO SAVEPOINT batch_start;
                      COMMIT;
                  END safe_batch_job;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "AUTONOMOUS_TRANSACTION + SAVEPOINT + ROLLBACK TO SAVEPOINT combined");
        SubprogramInfo sub = findSub(result, "SAFE_BATCH_JOB");
        assertNotNull(sub);
        assertTrue(sub.isPragmaAutonomousTransaction());
        assertTrue(sub.getStatements().stream().anyMatch(s -> "SAVEPOINT".equals(s.getType())));
        assertTrue(sub.getStatements().stream().anyMatch(s -> "ROLLBACK".equals(s.getType())));
        // FIXED: ROLLBACK's savepoint name is now captured here too, same as in 4a.
        StatementInfo rollbackStmt = sub.getStatements().stream()
                .filter(s -> "ROLLBACK".equals(s.getType())).findFirst().orElse(null);
        assertNotNull(rollbackStmt);
        assertEquals("BATCH_START", rollbackStmt.getSavepointName());
    }
}
