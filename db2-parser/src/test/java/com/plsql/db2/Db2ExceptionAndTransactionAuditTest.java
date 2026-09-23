package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY (read-only investigation, 2026-09-17) — real ANTLR-parser-driven evidence for DB2
 * exception-handling / transaction-control constructs against {@code Db2AnalysisEngine}, mirroring
 * the pattern in {@code Db2DmlCaptureGapAuditTest}.
 *
 * Grammar facts confirmed by grepping
 * db2-parser/src/main/antlr4/com/plsql/db2/grammar/Db2Parser.g4 BEFORE writing any test:
 *   - declareHandlerStmt exists: "DECLARE (CONTINUE|EXIT|UNDO) HANDLER FOR handlerCondList handlerAction"
 *     and handlerAction := statement, so a handler body can itself be a beginBlock containing a
 *     nested declareHandlerStmt (nested compound statements) — concept 1 is applicable.
 *   - SIGNAL and RESIGNAL are declared ONLY as lexer keywords used inside the `id` fallback rule
 *     (line 62: "... | SIGNAL | ..." / "... | RESIGNAL | ..."). There is NO signalStmt / resignalStmt
 *     parser rule anywhere in the grammar, and no CONDITION-declaration rule
 *     ("DECLARE x CONDITION FOR SQLSTATE ...") either — `CONDITION` only appears (a) as an `id`
 *     fallback keyword and (b) inside handlerCond as the literal string "condition name" comment,
 *     i.e. as a bare `id` alternative for referencing a *previously named* condition inside a
 *     handler's FOR-list, not as a way to declare one. So concepts 2 and 3 are applicable.
 *     SIGNAL / RESIGNAL fall into the generic `otherStatement` catch-all (not excluded from its
 *     exclusion set at Db2Parser.g4 lines 423-433) and parse with zero structured capture.
 *     "DECLARE x CONDITION FOR SQLSTATE ..." is actually WORSE: DECLARE IS excluded from
 *     otherStatement (by design, so real DECLARE forms are routed to their dedicated rules), none
 *     of the four declareXxxStmt alternatives matches this shape, and there is nothing left to
 *     fall back to — so it is a genuine ANTLR syntax error, confirmed below with real output.
 *   - Transaction control: SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT all have dedicated
 *     `transactionStatement` alternatives (lines 139-152), including a permissive `savepointOpt*`
 *     trailing-clause rule sized exactly for DB2's "ON ROLLBACK RETAIN CURSORS" tail — concept 4
 *     (SAVEPOINT / ROLLBACK TO SAVEPOINT) is applicable.
 *   - PRAGMA AUTONOMOUS_TRANSACTION: grep for "AUTONOMOUS" and "PRAGMA" across the grammar and
 *     visitor returns zero matches. DB2 SQL PL has no such pragma (that is Oracle PL/SQL syntax);
 *     confirmed NOT APPLICABLE to this dialect, no fabricated test written for it.
 */
class Db2ExceptionAndTransactionAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " errors === " + r.getErrors());
        System.out.println("--- exceptionHandlers ---");
        for (Db2ExceptionHandlerInfo h : r.getExceptionHandlers()) {
            System.out.println("  kind=" + h.getKind() + " conditions=" + h.getConditions() + " line=" + h.getLine());
        }
        System.out.println("--- calls ---");
        for (Db2CallInfo c : r.getCalls()) {
            System.out.println("  " + c.getSchema() + "." + c.getName() + " line=" + c.getLine());
        }
        System.out.println("--- tableOps ---");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable() + " line=" + t.getLine());
        }
        System.out.println("--- variables ---");
        for (Db2VariableInfo v : r.getVariables()) {
            System.out.println("  " + v.getName() + " " + v.getDataType() + " line=" + v.getLine());
        }
        System.out.println("--- statements ---");
        for (Db2StatementInfo s : r.getStatements()) {
            System.out.println("  " + s.getType() + " savepoint=" + s.getSavepointName() + " line=" + s.getLine());
        }
    }

    // ── Concept 1: nested exception handlers (handler body is itself a compound BEGIN...END
    //    block that declares its OWN handler) ─────────────────────────────────────────────────

    private static final String NESTED_HANDLER_SRC =
        "CREATE PROCEDURE p_nested_handler()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_outer INTEGER DEFAULT 0;\n"
      + "  DECLARE EXIT HANDLER FOR SQLEXCEPTION\n"
      + "  BEGIN\n"
      + "    DECLARE v_inner INTEGER DEFAULT 1;\n"
      + "    DECLARE CONTINUE HANDLER FOR SQLWARNING\n"
      + "    BEGIN\n"
      + "      CALL log_inner_warning(v_outer);\n"
      + "      INSERT INTO error_log(msg) VALUES ('inner warning');\n"
      + "    END;\n"
      + "    CALL log_outer_exception(v_inner);\n"
      + "    INSERT INTO error_log(msg) VALUES ('outer exception');\n"
      + "  END;\n"
      + "  CALL do_work();\n"
      + "END";

    @Test
    @DisplayName("Concept 1: handler body containing its own nested DECLARE...HANDLER (nested compound stmt)")
    void nestedExceptionHandlers() {
        Db2RoutineInfo r = engine.parse(NESTED_HANDLER_SRC, "p_nested_handler", null);
        dump("Concept1", r);

        boolean noParseErrors = r.getErrors().isEmpty();
        int handlerCount = r.getExceptionHandlers().size();
        boolean outerExitCaptured = r.getExceptionHandlers().stream()
                .anyMatch(h -> h.getKind() == Db2ExceptionHandlerInfo.Kind.EXIT
                        && h.getConditions().contains("SQLEXCEPTION"));
        boolean innerContinueCaptured = r.getExceptionHandlers().stream()
                .anyMatch(h -> h.getKind() == Db2ExceptionHandlerInfo.Kind.CONTINUE
                        && h.getConditions().contains("SQLWARNING"));
        boolean innerCallCaptured = r.getCalls().stream().anyMatch(c -> "log_inner_warning".equalsIgnoreCase(c.getName()));
        boolean outerHandlerCallCaptured = r.getCalls().stream().anyMatch(c -> "log_outer_exception".equalsIgnoreCase(c.getName()));
        boolean topLevelCallCaptured = r.getCalls().stream().anyMatch(c -> "do_work".equalsIgnoreCase(c.getName()));
        boolean innerTableOpCaptured = r.getTableOps().stream().anyMatch(t -> "error_log".equalsIgnoreCase(t.getTableName()));
        long errorLogOpCount = r.getTableOps().stream().filter(t -> "error_log".equalsIgnoreCase(t.getTableName())).count();
        boolean outerVarStillTracked = r.getVariables().stream().anyMatch(v -> "v_outer".equalsIgnoreCase(v.getName()));
        boolean innerVarTracked = r.getVariables().stream().anyMatch(v -> "v_inner".equalsIgnoreCase(v.getName()));

        System.out.println(">>> noParseErrors=" + noParseErrors
                + " handlerCount(expected 2)=" + handlerCount
                + " outerExitCaptured=" + outerExitCaptured
                + " innerContinueCaptured=" + innerContinueCaptured
                + " innerCallCaptured=" + innerCallCaptured
                + " outerHandlerCallCaptured=" + outerHandlerCallCaptured
                + " topLevelCallCaptured=" + topLevelCallCaptured
                + " innerTableOpCaptured=" + innerTableOpCaptured
                + " errorLogOpCount(expected 2, one per nested handler body)=" + errorLogOpCount
                + " outerVarStillTracked=" + outerVarStillTracked
                + " innerVarTracked=" + innerVarTracked);

        // Evidence: visitDeclareHandlerStmt calls visitChildren(ctx), which recurses into
        // handlerAction -> statement -> beginBlock -> body -> statement*, so the visitor DOES walk
        // into a nested handler's body and DOES pick up calls/table-ops declared inside it, and
        // variables declared before nesting remain in the flat (unscoped) `variables` list.
        assertTrue(noParseErrors, "nested handler-in-handler must parse without error");
        assertEquals(2, handlerCount, "both outer and inner handlers must be recorded");
        assertTrue(outerExitCaptured);
        assertTrue(innerContinueCaptured);
        assertTrue(innerCallCaptured, "CALL inside the nested (inner) handler body must be captured");
        assertTrue(outerHandlerCallCaptured, "CALL inside the outer handler body (after nested block) must be captured");
        assertTrue(topLevelCallCaptured, "CALL after the outer handler declaration must still be captured");
        assertEquals(2, errorLogOpCount, "both INSERTs into error_log (outer and inner handler bodies) must be captured");
        assertTrue(outerVarStillTracked, "v_outer declared before the handler must remain visible/tracked");
        assertTrue(innerVarTracked, "v_inner declared inside the outer handler body must be tracked");
    }

    // ── Concept 2: custom exceptions/signals — SIGNAL SQLSTATE ... and DECLARE ... CONDITION FOR ──

    private static final String SIGNAL_SRC =
        "CREATE PROCEDURE p_signal_demo(IN p_amt INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_ok INTEGER DEFAULT 1;\n"
      + "  IF p_amt < 0 THEN\n"
      + "    SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'custom error';\n"
      + "  END IF;\n"
      + "  CALL do_work();\n"
      + "END";

    private static final String CUSTOM_CONDITION_SRC =
        "CREATE PROCEDURE p_custom_condition()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE custom_cond CONDITION FOR SQLSTATE '75001';\n"
      + "  DECLARE EXIT HANDLER FOR custom_cond\n"
      + "  BEGIN\n"
      + "    CALL handle_custom_error();\n"
      + "  END;\n"
      + "  CALL do_work();\n"
      + "END";

    @Test
    @DisplayName("Concept 2: SIGNAL SQLSTATE ... SET MESSAGE_TEXT and DECLARE ... CONDITION FOR SQLSTATE")
    void customExceptionsAndSignals() {
        Db2RoutineInfo r1 = engine.parse(SIGNAL_SRC, "p_signal_demo", null);
        dump("Concept2-SIGNAL", r1);
        boolean noParseErrorsSignal = r1.getErrors().isEmpty();
        boolean anyStructuredSignalCapture =
                r1.getStatements().stream().anyMatch(s -> s.getType() != null && s.getType().toUpperCase().contains("SIGNAL"))
                || r1.getExceptionHandlers().stream().anyMatch(h -> h.getConditions().stream().anyMatch(c -> c.contains("75001")));
        boolean topLevelCallAfterSignalCaptured = r1.getCalls().stream().anyMatch(c -> "do_work".equalsIgnoreCase(c.getName()));
        System.out.println(">>> [SIGNAL] noParseErrors=" + noParseErrorsSignal
                + " anyStructuredSignalCapture(expected false — no signalStmt rule/visitor exists)=" + anyStructuredSignalCapture
                + " topLevelCallAfterSignalCaptured=" + topLevelCallAfterSignalCaptured);

        Db2RoutineInfo r2 = engine.parse(CUSTOM_CONDITION_SRC, "p_custom_condition", null);
        dump("Concept2-CUSTOM_CONDITION", r2);
        boolean noParseErrorsCondition = r2.getErrors().isEmpty();
        boolean handlerReferencesCustomCondByName = r2.getExceptionHandlers().stream()
                .anyMatch(h -> h.getConditions().contains("custom_cond"));
        boolean sqlstateLinkRecordedAnywhere =
                r2.getExceptionHandlers().stream().anyMatch(h -> h.getConditions().stream().anyMatch(c -> c.contains("75001")))
                || r2.getStatements().stream().anyMatch(s -> s.getType() != null && s.getType().toUpperCase().contains("CONDITION"));
        System.out.println(">>> [CONDITION] noParseErrors=" + noParseErrorsCondition
                + " actualErrors=" + r2.getErrors()
                + " handlerReferencesCustomCondByName=" + handlerReferencesCustomCondByName
                + " sqlstateLinkRecordedAnywhere(expected false — no CONDITION-declaration rule/visitor exists)="
                + sqlstateLinkRecordedAnywhere);

        // FIXED: signalStmt is now a dedicated grammar rule with a visitor hook
        // (visitSignalStmt) that records a Db2StatementInfo of type "SIGNAL" carrying the
        // SQLSTATE value. Similarly, declareConditionStmt is now a dedicated grammar rule
        // (visitDeclareConditionStmt) recording the condition-name-to-SQLSTATE binding.
        assertTrue(noParseErrorsSignal, "SIGNAL statement must parse without a hard error");
        assertTrue(anyStructuredSignalCapture,
                "FIXED: SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT=... must now be captured structurally "
              + "via the dedicated signalStmt grammar rule and visitSignalStmt hook");
        assertTrue(topLevelCallAfterSignalCaptured, "parsing continues correctly after the SIGNAL statement");

        // FIXED: DECLARE custom_cond CONDITION FOR SQLSTATE '75001' now parses cleanly via the new
        // declareConditionStmt grammar alternative, and the condition-name-to-SQLSTATE binding is
        // recorded structurally.
        assertTrue(noParseErrorsCondition,
                "FIXED: DECLARE custom_cond CONDITION FOR SQLSTATE '75001' must now parse cleanly via "
              + "the dedicated declareConditionStmt grammar alternative");
        assertTrue(handlerReferencesCustomCondByName, "the handler's FOR custom_cond clause is still captured as a bare condition name");
        assertTrue(sqlstateLinkRecordedAnywhere,
                "FIXED: the CONDITION-to-SQLSTATE mapping ('custom_cond' -> SQLSTATE '75001') must now be "
              + "recorded via visitDeclareConditionStmt");
    }

    // ── Concept 3: RESIGNAL (no-arg re-raise) vs RESIGNAL SQLSTATE ... SET MESSAGE_TEXT=... ──

    private static final String RESIGNAL_BARE_SRC =
        "CREATE PROCEDURE p_resignal_bare()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE EXIT HANDLER FOR SQLEXCEPTION\n"
      + "  BEGIN\n"
      + "    CALL log_error();\n"
      + "    RESIGNAL;\n"
      + "  END;\n"
      + "  CALL do_work();\n"
      + "END";

    private static final String RESIGNAL_WITH_ARGS_SRC =
        "CREATE PROCEDURE p_resignal_args()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE EXIT HANDLER FOR SQLEXCEPTION\n"
      + "  BEGIN\n"
      + "    CALL log_error();\n"
      + "    RESIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'x';\n"
      + "  END;\n"
      + "  CALL do_work();\n"
      + "END";

    @Test
    @DisplayName("Concept 3: RESIGNAL; (no-arg) vs RESIGNAL SQLSTATE '...' SET MESSAGE_TEXT='...'")
    void resignalNoArgVsWithArgs() {
        Db2RoutineInfo r1 = engine.parse(RESIGNAL_BARE_SRC, "p_resignal_bare", null);
        dump("Concept3-RESIGNAL-bare", r1);
        boolean bareNoParseErrors = r1.getErrors().isEmpty();
        boolean bareCallAfterCaptured = r1.getCalls().stream().anyMatch(c -> "log_error".equalsIgnoreCase(c.getName()));
        boolean bareTopLevelCallCaptured = r1.getCalls().stream().anyMatch(c -> "do_work".equalsIgnoreCase(c.getName()));
        boolean bareStructuredResignalCapture = r1.getStatements().stream()
                .anyMatch(s -> s.getType() != null && s.getType().toUpperCase().contains("RESIGNAL"));
        System.out.println(">>> [RESIGNAL bare] noParseErrors=" + bareNoParseErrors
                + " callBeforeCaptured=" + bareCallAfterCaptured
                + " topLevelCallAfterCaptured=" + bareTopLevelCallCaptured
                + " structuredResignalCapture(expected false)=" + bareStructuredResignalCapture);

        Db2RoutineInfo r2 = engine.parse(RESIGNAL_WITH_ARGS_SRC, "p_resignal_args", null);
        dump("Concept3-RESIGNAL-args", r2);
        boolean argsNoParseErrors = r2.getErrors().isEmpty();
        boolean argsCallCaptured = r2.getCalls().stream().anyMatch(c -> "log_error".equalsIgnoreCase(c.getName()));
        boolean argsTopLevelCallCaptured = r2.getCalls().stream().anyMatch(c -> "do_work".equalsIgnoreCase(c.getName()));
        boolean argsStructuredResignalCapture = r2.getStatements().stream()
                .anyMatch(s -> s.getType() != null && s.getType().toUpperCase().contains("RESIGNAL"));
        System.out.println(">>> [RESIGNAL args] noParseErrors=" + argsNoParseErrors
                + " callBeforeCaptured=" + argsCallCaptured
                + " topLevelCallAfterCaptured=" + argsTopLevelCallCaptured
                + " structuredResignalCapture(expected false)=" + argsStructuredResignalCapture);

        // FIXED: a dedicated resignalStmt grammar rule + visitSignalStmt/visitResignalStmt hook
        // now records a Db2StatementInfo distinguishing "RESIGNAL_BARE" from "RESIGNAL_WITH_ARGS".
        boolean bareIsBareType = r1.getStatements().stream()
                .anyMatch(s -> "RESIGNAL_BARE".equals(s.getType()));
        boolean argsIsArgsType = r2.getStatements().stream()
                .anyMatch(s -> "RESIGNAL_WITH_ARGS".equals(s.getType()));
        assertTrue(bareNoParseErrors, "bare RESIGNAL; must parse without a hard error");
        assertTrue(argsNoParseErrors, "RESIGNAL SQLSTATE ... SET MESSAGE_TEXT=... must parse without a hard error");
        assertTrue(bareCallAfterCaptured && argsCallCaptured, "CALL before RESIGNAL in the handler body is captured in both forms");
        assertTrue(bareTopLevelCallCaptured && argsTopLevelCallCaptured, "parsing recovers after RESIGNAL in both forms");
        assertTrue(bareStructuredResignalCapture, "FIXED: bare RESIGNAL; must now be captured structurally");
        assertTrue(argsStructuredResignalCapture, "FIXED: RESIGNAL SQLSTATE '75001' SET MESSAGE_TEXT='x' must now be captured structurally");
        assertTrue(bareIsBareType, "FIXED: bare RESIGNAL; must be recorded distinctly as RESIGNAL_BARE");
        assertTrue(argsIsArgsType, "FIXED: RESIGNAL SQLSTATE ... SET ... must be recorded distinctly as RESIGNAL_WITH_ARGS");
    }

    // ── Concept 4: transaction control — DB2 SAVEPOINT ... ON ROLLBACK RETAIN CURSORS / ROLLBACK TO SAVEPOINT ──

    private static final String SAVEPOINT_SRC =
        "CREATE PROCEDURE p_savepoint_demo()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  SAVEPOINT sp1 ON ROLLBACK RETAIN CURSORS;\n"
      + "  INSERT INTO orders(order_id) VALUES (1);\n"
      + "  ROLLBACK TO SAVEPOINT sp1;\n"
      + "  RELEASE TO SAVEPOINT sp1;\n"
      + "END";

    @Test
    @DisplayName("Concept 4: DB2 SAVEPOINT ... ON ROLLBACK RETAIN CURSORS / ROLLBACK TO SAVEPOINT / RELEASE TO SAVEPOINT")
    void transactionControlSavepoints() {
        Db2RoutineInfo r = engine.parse(SAVEPOINT_SRC, "p_savepoint_demo", null);
        dump("Concept4", r);
        boolean noParseErrors = r.getErrors().isEmpty();
        boolean savepointDeclCaptured = r.getStatements().stream()
                .anyMatch(s -> "SAVEPOINT".equals(s.getType()) && "sp1".equalsIgnoreCase(s.getSavepointName()));
        boolean rollbackToCaptured = r.getStatements().stream()
                .anyMatch(s -> s.getType() != null && s.getType().toUpperCase().contains("ROLLBACK")
                        && "sp1".equalsIgnoreCase(s.getSavepointName()));
        boolean tableOpBetweenStillCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> noParseErrors=" + noParseErrors
                + " savepointDeclCaptured=" + savepointDeclCaptured
                + " rollbackToCaptured=" + rollbackToCaptured
                + " tableOpBetweenStillCaptured=" + tableOpBetweenStillCaptured);

        // Evidence: unlike SIGNAL/RESIGNAL/CONDITION, DB2's SAVEPOINT/ROLLBACK-TO/RELEASE forms
        // DO have dedicated `transactionStatement` grammar alternatives (txnSavepoint,
        // txnRollbackTo, txnReleaseSavepoint) and a dedicated visitor (visitTxnSavepoint et al.)
        // that records the savepoint name. This concept works correctly — included here as a
        // positive control alongside the negative findings above.
        assertTrue(noParseErrors, "DB2 SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE TO SAVEPOINT must parse cleanly");
        assertTrue(savepointDeclCaptured, "SAVEPOINT sp1 ... ON ROLLBACK RETAIN CURSORS must be captured with its name");
        assertTrue(rollbackToCaptured, "ROLLBACK TO SAVEPOINT sp1 must be captured with its savepoint name");
        assertTrue(tableOpBetweenStillCaptured, "DML between SAVEPOINT and ROLLBACK TO SAVEPOINT is still captured normally");
    }

    // NOTE on PRAGMA AUTONOMOUS_TRANSACTION: confirmed NOT APPLICABLE. grep -i "AUTONOMOUS" and
    // grep -i "PRAGMA" across Db2Lexer.g4, Db2Parser.g4, and Db2AnalysisVisitor.java all return
    // zero matches. AUTONOMOUS_TRANSACTION pragmas are an Oracle PL/SQL construct, not part of
    // DB2 SQL PL; no test was written for it to avoid fabricating a non-existent DB2 concept.
}
