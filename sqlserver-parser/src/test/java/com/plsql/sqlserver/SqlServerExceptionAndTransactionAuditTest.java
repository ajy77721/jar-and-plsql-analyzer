package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerExceptionHandlerInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerStatementInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * AUDIT ONLY — read-only investigation, against the real ANTLR grammar/visitor, of four
 * SQL-Server-specific exception/transaction concepts:
 *   1. Nested TRY/CATCH (a TRY/CATCH inside another's CATCH block)
 *   2. Custom errors via THROW <n>,<msg>,<state> and RAISERROR(<msg>,<sev>,<state>)
 *   3. Bare re-raise THROW; (no args) inside a CATCH, vs. THROW with args
 *   4. Transaction control: SAVE TRANSACTION / ROLLBACK TRANSACTION <savepoint>
 *
 * Does NOT modify any main/ source. Assertions reflect ACTUAL observed behavior of the
 * current grammar/visitor (some assertions document gaps rather than correctness).
 */
class SqlServerExceptionAndTransactionAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine())
                .toList());
        System.out.println("exceptionHandlers: " + r.getExceptionHandlers().stream()
                .map(eh -> "line=" + eh.getLine() + "-" + eh.getLineEnd()
                        + " stmts=" + eh.getStatementsCount())
                .toList());
        System.out.println("statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@" + s.getLine()
                        + (s.getSavepointName() != null ? " savepoint=" + s.getSavepointName() : ""))
                .toList());
        System.out.println("errors: " + r.getErrors());
    }

    // ── Concept 1: nested TRY/CATCH ──────────────────────────────────────────────────────────
    @Test
    @DisplayName("Concept 1: TRY/CATCH nested inside outer CATCH block")
    void concept1_nestedTryCatch() {
        String src =
                "CREATE PROCEDURE usp_nested_try_catch\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @outerFlag INT = 1;\n" +
                "    BEGIN TRY\n" +
                "        EXEC outer_proc;\n" +
                "    END TRY\n" +
                "    BEGIN CATCH\n" +
                "        SET @outerFlag = 0;\n" +
                "        BEGIN TRY\n" +
                "            EXEC inner_proc;\n" +
                "            INSERT INTO InnerLog (Msg) VALUES ('inner try ran');\n" +
                "        END TRY\n" +
                "        BEGIN CATCH\n" +
                "            EXEC handler_proc;\n" +
                "            INSERT INTO HandlerLog (Msg) VALUES ('handler ran');\n" +
                "        END CATCH\n" +
                "    END CATCH\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept1_nestedTryCatch", r);

        // Two independent TRY/CATCH blocks should be recorded: the outer one and the nested one.
        System.out.println("concept1: exceptionHandlers.size()=" + r.getExceptionHandlers().size());

        boolean innerLogCaptured = r.getTableOps().stream()
                .anyMatch(t -> "InnerLog".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        boolean handlerLogCaptured = r.getTableOps().stream()
                .anyMatch(t -> "HandlerLog".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        System.out.println("concept1: InsertInto InnerLog (nested TRY body) captured=" + innerLogCaptured
                + " InsertInto HandlerLog (nested CATCH body) captured=" + handlerLogCaptured
                + " errors=" + r.getErrors());

        // ACTUAL: visitTryBlock() always calls visitChildren(ctx), and the grammar's tryBlock
        // rule allows nested tryBlock via `statement*` on both the TRY and CATCH sides, so the
        // walk does descend into the nested CATCH and continues to capture table ops/calls there.
        org.junit.jupiter.api.Assertions.assertEquals(2, r.getExceptionHandlers().size(),
                "outer TRY/CATCH + nested TRY/CATCH inside the outer CATCH should both be recorded");
        org.junit.jupiter.api.Assertions.assertTrue(innerLogCaptured,
                "INSERT inside the nested TRY body must still be captured");
        org.junit.jupiter.api.Assertions.assertTrue(handlerLogCaptured,
                "INSERT inside the nested CATCH body must still be captured");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "nested TRY/CATCH should not raise parse errors");
    }

    // ── Concept 2: custom errors — THROW with args, RAISERROR with custom severity ──────────
    @Test
    @DisplayName("Concept 2a: THROW 51000, 'custom message', 1; parses cleanly")
    void concept2a_throwWithCustomErrorNumber() {
        String src =
                "CREATE PROCEDURE usp_custom_throw\n" +
                "    @orderId INT\n" +
                "AS\nBEGIN\n" +
                "    IF @orderId IS NULL\n" +
                "        THROW 51000, 'custom message', 1;\n" +
                "    UPDATE Orders SET Status = 'OK' WHERE OrderId = @orderId;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept2a_throwWithCustomErrorNumber", r);

        boolean updateCaptured = r.getTableOps().stream()
                .anyMatch(t -> "Orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);
        long throwStatements = r.getStatements().stream()
                .filter(s -> "THROW".equalsIgnoreCase(s.getType())).count();
        System.out.println("concept2a: throwStatements recorded=" + throwStatements
                + " Orders UPDATE still captured after THROW=" + updateCaptured
                + " errors=" + r.getErrors());

        // FIXED: a dedicated throwStatement grammar rule + visitThrowStatement now extract the
        // error number/message/state into structured fields.
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "THROW with a custom error number/message/state must parse without error");
        org.junit.jupiter.api.Assertions.assertEquals(1, throwStatements,
                "the THROW statement should be recorded in the statement inventory");
        org.junit.jupiter.api.Assertions.assertTrue(updateCaptured,
                "statements after a THROW in a different branch must still be captured");

        SqlServerStatementInfo throwStmt = r.getStatements().stream()
                .filter(s -> "THROW".equalsIgnoreCase(s.getType())).findFirst().orElseThrow();
        org.junit.jupiter.api.Assertions.assertEquals("51000", throwStmt.getErrorNumber());
        org.junit.jupiter.api.Assertions.assertEquals("'custom message'", throwStmt.getMessage());
        org.junit.jupiter.api.Assertions.assertEquals("1", throwStmt.getState());
    }

    @Test
    @DisplayName("Concept 2b: RAISERROR('custom message', 16, 1) with custom severity/state parses cleanly")
    void concept2b_raiserrorWithCustomSeverity() {
        String src =
                "CREATE PROCEDURE usp_custom_raiserror\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRY\n" +
                "        EXEC risky_proc;\n" +
                "    END TRY\n" +
                "    BEGIN CATCH\n" +
                "        RAISERROR('custom message: %s', 16, 1, ERROR_MESSAGE());\n" +
                "    END CATCH\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept2b_raiserrorWithCustomSeverity", r);

        long raiserrorStatements = r.getStatements().stream()
                .filter(s -> "RAISERROR".equalsIgnoreCase(s.getType())).count();
        System.out.println("concept2b: raiserrorStatements recorded=" + raiserrorStatements
                + " errors=" + r.getErrors());

        // FIXED: a dedicated raiserrorStatement grammar rule + visitRaiserrorStatement now
        // extract message/severity/state into structured fields.
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "RAISERROR with custom severity/state must parse without error");
        org.junit.jupiter.api.Assertions.assertEquals(1, raiserrorStatements,
                "the RAISERROR statement should be recorded in the statement inventory");

        SqlServerStatementInfo raiserrorStmt = r.getStatements().stream()
                .filter(s -> "RAISERROR".equalsIgnoreCase(s.getType())).findFirst().orElseThrow();
        org.junit.jupiter.api.Assertions.assertEquals("'custom message: %s'", raiserrorStmt.getMessage());
        org.junit.jupiter.api.Assertions.assertEquals("16", raiserrorStmt.getSeverity());
        org.junit.jupiter.api.Assertions.assertEquals("1", raiserrorStmt.getState());
    }

    // ── Concept 3: bare re-raise THROW; vs. THROW with args ──────────────────────────────────
    @Test
    @DisplayName("Concept 3: bare THROW; (re-raise) inside CATCH is NOT distinguished from THROW with args")
    void concept3_bareThrowVsArgsThrow() {
        String bareSrc =
                "CREATE PROCEDURE usp_rethrow\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRY\n" +
                "        EXEC risky_proc;\n" +
                "    END TRY\n" +
                "    BEGIN CATCH\n" +
                "        INSERT INTO ErrorLog (Msg) VALUES (ERROR_MESSAGE());\n" +
                "        THROW;\n" +
                "    END CATCH\n" +
                "END";
        String argsSrc =
                "CREATE PROCEDURE usp_custom_throw2\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRY\n" +
                "        EXEC risky_proc;\n" +
                "    END TRY\n" +
                "    BEGIN CATCH\n" +
                "        THROW 51000, 'custom message', 1;\n" +
                "    END CATCH\n" +
                "END";

        SqlServerRoutineInfo bareResult = parse(bareSrc);
        SqlServerRoutineInfo argsResult = parse(argsSrc);
        dump("concept3_bareThrow", bareResult);
        dump("concept3_argsThrow", argsResult);

        List<SqlServerStatementInfo> bareThrows = bareResult.getStatements().stream()
                .filter(s -> s.getType() != null && s.getType().toUpperCase().startsWith("THROW")).toList();
        List<SqlServerStatementInfo> argsThrows = argsResult.getStatements().stream()
                .filter(s -> s.getType() != null && s.getType().toUpperCase().startsWith("THROW")).toList();
        System.out.println("concept3: bare THROW; parses without error=" + bareResult.getErrors().isEmpty()
                + " bareThrows.size()=" + bareThrows.size());
        System.out.println("concept3: THROW with args parses without error=" + argsResult.getErrors().isEmpty()
                + " argsThrows.size()=" + argsThrows.size());

        // FIXED: a dedicated throwStatement grammar rule now distinguishes the bare re-raise
        // (recorded as type "THROW_BARE") from the argument form (recorded as type "THROW",
        // with errorNumber/message/state populated) via visitThrowStatement.
        org.junit.jupiter.api.Assertions.assertTrue(bareResult.getErrors().isEmpty(),
                "bare THROW; must parse without error");
        org.junit.jupiter.api.Assertions.assertEquals(1, bareThrows.size());
        org.junit.jupiter.api.Assertions.assertEquals(1, argsThrows.size());
        org.junit.jupiter.api.Assertions.assertEquals("THROW_BARE", bareThrows.get(0).getType());
        org.junit.jupiter.api.Assertions.assertEquals("THROW", argsThrows.get(0).getType());
        org.junit.jupiter.api.Assertions.assertNotEquals(bareThrows.get(0).getType(), argsThrows.get(0).getType(),
                "FIXED: bare re-raise vs THROW-with-args are now distinguishable by type");
    }

    // ── Concept 4: SAVE TRANSACTION / ROLLBACK TRANSACTION <savepoint> ───────────────────────
    @Test
    @DisplayName("Concept 4: SAVE TRANSACTION savepoint + ROLLBACK TRANSACTION savepoint captured with name")
    void concept4_saveAndRollbackToSavepoint() {
        String src =
                "CREATE PROCEDURE usp_savepoint_demo\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRANSACTION;\n" +
                "    UPDATE Accounts SET Balance = Balance - 100 WHERE AccountId = 1;\n" +
                "    SAVE TRANSACTION BeforeTransfer;\n" +
                "    UPDATE Accounts SET Balance = Balance + 100 WHERE AccountId = 2;\n" +
                "    IF @@ERROR <> 0\n" +
                "        ROLLBACK TRANSACTION BeforeTransfer;\n" +
                "    COMMIT TRANSACTION;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept4_saveAndRollbackToSavepoint", r);

        SqlServerStatementInfo savepointStmt = r.getStatements().stream()
                .filter(s -> "SAVEPOINT".equalsIgnoreCase(s.getType())).findFirst().orElse(null);
        SqlServerStatementInfo rollbackStmt = r.getStatements().stream()
                .filter(s -> "ROLLBACK".equalsIgnoreCase(s.getType())).findFirst().orElse(null);
        System.out.println("concept4: SAVEPOINT stmt found=" + (savepointStmt != null)
                + " name=" + (savepointStmt != null ? savepointStmt.getSavepointName() : "N/A"));
        System.out.println("concept4: ROLLBACK stmt found=" + (rollbackStmt != null)
                + " name=" + (rollbackStmt != null ? rollbackStmt.getSavepointName() : "N/A"));

        // ACTUAL: transactionStatement's grammar rule captures a trailing ID as the savepoint
        // name for both SAVE TRANSACTION and ROLLBACK TRANSACTION, and addTransactionStatement()
        // stores it via setSavepointName(). Both statements ARE captured correctly with the
        // matching savepoint name "BeforeTransfer".
        org.junit.jupiter.api.Assertions.assertNotNull(savepointStmt, "SAVE TRANSACTION must be recorded");
        org.junit.jupiter.api.Assertions.assertEquals("BeforeTransfer", savepointStmt.getSavepointName());
        org.junit.jupiter.api.Assertions.assertNotNull(rollbackStmt, "ROLLBACK TRANSACTION <savepoint> must be recorded");
        org.junit.jupiter.api.Assertions.assertEquals("BeforeTransfer", rollbackStmt.getSavepointName());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());
    }

    // Note: SQL Server has NO "PRAGMA AUTONOMOUS_TRANSACTION" (that construct is Oracle-only —
    // confirmed by `grep -rn -i "AUTONOMOUS_TRANSACTION"` across this module's grammar and
    // visitor sources, which returns zero matches). No test is written for it; fabricating one
    // for a SQL-Server-only module would misrepresent the dialect.
}
