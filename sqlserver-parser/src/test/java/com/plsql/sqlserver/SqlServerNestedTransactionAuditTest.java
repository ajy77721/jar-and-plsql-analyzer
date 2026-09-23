package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerStatementInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * AUDIT ONLY — read-only investigation, against the real ANTLR grammar/visitor, going deeper
 * into SQL-Server transaction NESTING specifically (beyond what
 * SqlServerExceptionAndTransactionAuditTest already covers: TRY/CATCH nesting, THROW/RAISERROR
 * argument capture, SAVE TRANSACTION name capture).
 *
 * AUDIT CONCEPTS:
 *   1. Nested BEGIN TRAN/COMMIT/ROLLBACK depth tracking
 *   2. SET XACT_ABORT ON/OFF
 *   3. SAVE TRANSACTION combined with TRY/CATCH rollback-to-savepoint
 *   4. @@TRANCOUNT checks
 *
 * Does NOT modify any main/ source. Assertions reflect ACTUAL observed behavior of the
 * current grammar/visitor.
 */
class SqlServerNestedTransactionAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine())
                .toList());
        System.out.println("calls: " + r.getCalls().stream()
                .map(c -> c.getName() + "@" + c.getLine()).toList());
        System.out.println("statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@" + s.getLine()
                        + (s.getSavepointName() != null ? " savepoint=" + s.getSavepointName() : ""))
                .toList());
        System.out.println("exceptionHandlers.size()=" + r.getExceptionHandlers().size());
        System.out.println("errors: " + r.getErrors());
    }

    // ── Concept 1: nested BEGIN TRAN/COMMIT/ROLLBACK depth tracking ──────────────────────────
    @Test
    @DisplayName("Concept 1: nested BEGIN TRANSACTION/COMMIT depth IS tracked via transactionDepth")
    void concept1_nestedTransactionDepth() {
        String src =
                "CREATE PROCEDURE usp_nested_tran\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRANSACTION outer_tran;\n" +
                "    UPDATE tab_a SET x=1;\n" +
                "    BEGIN TRANSACTION inner_tran;\n" +
                "        UPDATE tab_b SET y=2;\n" +
                "    COMMIT TRANSACTION inner_tran;\n" +
                "    COMMIT TRANSACTION outer_tran;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept1_nestedTransactionDepth", r);

        List<SqlServerStatementInfo> beginTrans = r.getStatements().stream()
                .filter(s -> "BEGIN_TRAN".equalsIgnoreCase(s.getType())).toList();
        List<SqlServerStatementInfo> commits = r.getStatements().stream()
                .filter(s -> "COMMIT".equalsIgnoreCase(s.getType())).toList();

        boolean tabACaptured = r.getTableOps().stream()
                .anyMatch(t -> "tab_a".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);
        boolean tabBCaptured = r.getTableOps().stream()
                .anyMatch(t -> "tab_b".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);

        System.out.println("concept1: beginTrans.size()=" + beginTrans.size()
                + " commits.size()=" + commits.size()
                + " tabACaptured=" + tabACaptured + " tabBCaptured=" + tabBCaptured);

        // ACTUAL: SqlServerParser.g4's transactionStatement rule is
        //   BEGIN (TRAN|TRANSACTION) ID? SEMI? | COMMIT ... | ROLLBACK ... | SAVE ...
        // — it captures at most a trailing name token, never opens a nested scope/sub-list of
        // statements (unlike beginEndBlock's "BEGIN statement* END"). So BEGIN TRANSACTION,
        // COMMIT TRANSACTION, ROLLBACK TRANSACTION are all flat, independent statement records in
        // the routine body's linear statement list. addTransactionStatement() (visitor) likewise
        // has no counter/stack field — it maps each ctx to one SqlServerStatementInfo of type
        // BEGIN_TRAN/COMMIT/ROLLBACK/SAVEPOINT with no depth number and no correlation between a
        // BEGIN_TRAN and the COMMIT/ROLLBACK that (informally) matches it. There is NO nesting
        // depth tracking at all, named or unnamed. This is a real gap for anyone wanting to know
        // if a routine's transactions are balanced/how deep they nest.
        //
        // The good news: because transactionStatement doesn't consume any body statements (it is
        // not a block-forming rule), the "nested" BEGIN TRANSACTION does not swallow or hide
        // tab_a/tab_b — they are ordinary sibling statements in the flat list and both DMLs are
        // captured correctly regardless of transaction nesting.
        org.junit.jupiter.api.Assertions.assertEquals(2, beginTrans.size(),
                "both BEGIN TRANSACTION statements are recorded, but as flat unrelated entries");
        org.junit.jupiter.api.Assertions.assertEquals(2, commits.size());
        org.junit.jupiter.api.Assertions.assertTrue(tabACaptured, "tab_a UPDATE must survive nested tran markers");
        org.junit.jupiter.api.Assertions.assertTrue(tabBCaptured, "tab_b UPDATE must survive nested tran markers");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());

        // FIXED: transactionDepth now tracks nesting via a simple per-routine counter.
        // BEGIN_TRAN records the depth it enters; COMMIT/ROLLBACK record the depth they close.
        org.junit.jupiter.api.Assertions.assertEquals(1, beginTrans.get(0).getTransactionDepth(),
                "outer BEGIN TRANSACTION enters depth 1");
        org.junit.jupiter.api.Assertions.assertEquals(2, beginTrans.get(1).getTransactionDepth(),
                "inner BEGIN TRANSACTION enters depth 2");
        org.junit.jupiter.api.Assertions.assertEquals(2, commits.get(0).getTransactionDepth(),
                "inner COMMIT closes depth 2");
        org.junit.jupiter.api.Assertions.assertEquals(1, commits.get(1).getTransactionDepth(),
                "outer COMMIT closes depth 1");
    }

    @Test
    @DisplayName("Concept 1b: unnamed nested BEGIN TRAN/COMMIT/ROLLBACK — same flat treatment")
    void concept1b_unnamedNestedTransactions() {
        String src =
                "CREATE PROCEDURE usp_unnamed_nested_tran\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRANSACTION;\n" +
                "    UPDATE tab_a SET x=1;\n" +
                "    BEGIN TRANSACTION;\n" +
                "        UPDATE tab_b SET y=2;\n" +
                "    COMMIT TRANSACTION;\n" +
                "    ROLLBACK TRANSACTION;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept1b_unnamedNestedTransactions", r);

        List<SqlServerStatementInfo> beginTrans = r.getStatements().stream()
                .filter(s -> "BEGIN_TRAN".equalsIgnoreCase(s.getType())).toList();
        List<SqlServerStatementInfo> commits = r.getStatements().stream()
                .filter(s -> "COMMIT".equalsIgnoreCase(s.getType())).toList();
        List<SqlServerStatementInfo> rollbacks = r.getStatements().stream()
                .filter(s -> "ROLLBACK".equalsIgnoreCase(s.getType())).toList();
        boolean tabACaptured = r.getTableOps().stream()
                .anyMatch(t -> "tab_a".equalsIgnoreCase(t.getTableName()));
        boolean tabBCaptured = r.getTableOps().stream()
                .anyMatch(t -> "tab_b".equalsIgnoreCase(t.getTableName()));

        System.out.println("concept1b: beginTrans.size()=" + beginTrans.size()
                + " tabACaptured=" + tabACaptured + " tabBCaptured=" + tabBCaptured
                + " errors=" + r.getErrors());

        // ACTUAL: identical flat behaviour whether or not a transaction name is supplied — the
        // ID? in the grammar is simply absent, savepointName/name stays null. No difference in
        // nesting handling between named and unnamed transactions (depth tracking works the same
        // regardless of naming).
        org.junit.jupiter.api.Assertions.assertEquals(2, beginTrans.size());
        org.junit.jupiter.api.Assertions.assertTrue(tabACaptured);
        org.junit.jupiter.api.Assertions.assertTrue(tabBCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());

        // depth tracking: BEGIN(1), BEGIN(2), COMMIT closes 2, ROLLBACK closes 1
        org.junit.jupiter.api.Assertions.assertEquals(1, beginTrans.get(0).getTransactionDepth());
        org.junit.jupiter.api.Assertions.assertEquals(2, beginTrans.get(1).getTransactionDepth());
        org.junit.jupiter.api.Assertions.assertEquals(1, commits.size());
        org.junit.jupiter.api.Assertions.assertEquals(2, commits.get(0).getTransactionDepth());
        org.junit.jupiter.api.Assertions.assertEquals(1, rollbacks.size());
        org.junit.jupiter.api.Assertions.assertEquals(1, rollbacks.get(0).getTransactionDepth());
    }

    @Test
    @DisplayName("Concept 1c: unnested single BEGIN TRANSACTION/COMMIT pair shows consistent depth=1")
    void concept1c_flatSingleTransactionDepth() {
        String src =
                "CREATE PROCEDURE usp_flat_tran\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRANSACTION;\n" +
                "    UPDATE tab_a SET x=1;\n" +
                "    COMMIT TRANSACTION;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept1c_flatSingleTransactionDepth", r);

        SqlServerStatementInfo beginTran = r.getStatements().stream()
                .filter(s -> "BEGIN_TRAN".equalsIgnoreCase(s.getType())).findFirst().orElse(null);
        SqlServerStatementInfo commit = r.getStatements().stream()
                .filter(s -> "COMMIT".equalsIgnoreCase(s.getType())).findFirst().orElse(null);

        org.junit.jupiter.api.Assertions.assertNotNull(beginTran);
        org.junit.jupiter.api.Assertions.assertNotNull(commit);
        org.junit.jupiter.api.Assertions.assertEquals(1, beginTran.getTransactionDepth());
        org.junit.jupiter.api.Assertions.assertEquals(1, commit.getTransactionDepth());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());
    }

    // ── Concept 2: SET XACT_ABORT ON/OFF ──────────────────────────────────────────────────────
    @Test
    @DisplayName("Concept 2: SET XACT_ABORT ON/OFF parses cleanly via setStatement's session-option form")
    void concept2_setXactAbort() {
        String src =
                "CREATE PROCEDURE usp_xact_abort\n" +
                "AS\nBEGIN\n" +
                "    SET XACT_ABORT ON;\n" +
                "    BEGIN TRANSACTION;\n" +
                "    UPDATE tab_a SET x=1;\n" +
                "    COMMIT TRANSACTION;\n" +
                "    SET XACT_ABORT OFF;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept2_setXactAbort", r);

        long setStatements = r.getStatements().stream()
                .filter(s -> "SET".equalsIgnoreCase(s.getType())).count();
        boolean tabACaptured = r.getTableOps().stream()
                .anyMatch(t -> "tab_a".equalsIgnoreCase(t.getTableName()));

        System.out.println("concept2: setStatements recorded=" + setStatements
                + " tabACaptured=" + tabACaptured + " errors=" + r.getErrors());

        // ACTUAL: setStatement's second alternative "SET stmtTail SEMI?" matches "XACT_ABORT ON"
        // the same way it matches "NOCOUNT ON" — stmtTail is a permissive token-eating rule with
        // no keyword-specific logic, so XACT_ABORT is not special-cased but also not rejected.
        // visitSetStatement() returns immediately because ctx.expr() is null for this form (no
        // AT_VAR/EQ present), so there is no dedicated field recording "XACT_ABORT"/ON vs OFF —
        // it is recorded only generically as a "SET" statement in the statement inventory (same
        // as SET NOCOUNT ON would be), with the ON/OFF value not distinguished anywhere.
        // It parses cleanly (no errors) and does not disrupt the surrounding transaction/DML.
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "SET XACT_ABORT ON/OFF must parse without error");
        org.junit.jupiter.api.Assertions.assertEquals(2, setStatements,
                "both SET XACT_ABORT statements are recorded generically as type SET");
        org.junit.jupiter.api.Assertions.assertTrue(tabACaptured,
                "DML between the two SET XACT_ABORT statements must still be captured");
    }

    // ── Concept 3: SAVE TRANSACTION + TRY/CATCH rollback-to-savepoint ────────────────────────
    @Test
    @DisplayName("Concept 3: SAVE TRANSACTION inside TRY, ROLLBACK TRANSACTION <savepoint> inside CATCH")
    void concept3_saveTransactionWithTryCatch() {
        String src =
                "CREATE PROCEDURE usp_save_tran_try_catch\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRY\n" +
                "        BEGIN TRANSACTION;\n" +
                "        SAVE TRANSACTION sp1;\n" +
                "        UPDATE tab_a SET x=1;\n" +
                "        UPDATE tab_b SET y=2;\n" +
                "        COMMIT TRANSACTION;\n" +
                "    END TRY\n" +
                "    BEGIN CATCH\n" +
                "        ROLLBACK TRANSACTION sp1;\n" +
                "        INSERT INTO error_log (msg) VALUES (ERROR_MESSAGE());\n" +
                "    END CATCH\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept3_saveTransactionWithTryCatch", r);

        boolean tabACaptured = r.getTableOps().stream()
                .anyMatch(t -> "tab_a".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);
        boolean tabBCaptured = r.getTableOps().stream()
                .anyMatch(t -> "tab_b".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);
        boolean errorLogCaptured = r.getTableOps().stream()
                .anyMatch(t -> "error_log".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        SqlServerStatementInfo savepointStmt = r.getStatements().stream()
                .filter(s -> "SAVEPOINT".equalsIgnoreCase(s.getType())).findFirst().orElse(null);
        SqlServerStatementInfo rollbackStmt = r.getStatements().stream()
                .filter(s -> "ROLLBACK".equalsIgnoreCase(s.getType())).findFirst().orElse(null);

        // ERROR_MESSAGE() is a schema-less built-in call inside an INSERT VALUES() expression —
        // check it is not recorded as an outbound proc call (EXEC-based calls only) and does not
        // corrupt/crash the visit.
        boolean errorMessageMisrecordedAsCall = r.getCalls().stream()
                .anyMatch(c -> "ERROR_MESSAGE".equalsIgnoreCase(c.getName()));

        System.out.println("concept3: tabACaptured=" + tabACaptured + " tabBCaptured=" + tabBCaptured
                + " errorLogCaptured=" + errorLogCaptured
                + " savepointStmt=" + (savepointStmt != null ? savepointStmt.getSavepointName() : "N/A")
                + " rollbackStmt=" + (rollbackStmt != null ? rollbackStmt.getSavepointName() : "N/A")
                + " errorMessageMisrecordedAsCall=" + errorMessageMisrecordedAsCall
                + " exceptionHandlers.size()=" + r.getExceptionHandlers().size()
                + " errors=" + r.getErrors());

        // ACTUAL: visitTryBlock() calls visitChildren(ctx) unconditionally, so the TRY body
        // (BEGIN TRANSACTION, SAVE TRANSACTION sp1, both UPDATEs, COMMIT) and the CATCH body
        // (ROLLBACK TRANSACTION sp1, INSERT INTO error_log) are both walked and both DML ops and
        // both transaction-control statements are captured, matching the already-audited
        // TRY/CATCH-scope-preservation behaviour from SqlServerExceptionAndTransactionAuditTest.
        // Combining SAVE TRANSACTION with TRY/CATCH introduces no additional loss.
        //
        // ERROR_MESSAGE() only appears inside INSERT's stmtTail as plain tokens (it is never
        // routed through visitExecStatement, which is the only path that records outbound
        // SqlServerCallInfo entries) — so it is correctly NOT recorded as a call at all.
        org.junit.jupiter.api.Assertions.assertTrue(tabACaptured, "tab_a UPDATE inside TRY must be captured");
        org.junit.jupiter.api.Assertions.assertTrue(tabBCaptured, "tab_b UPDATE inside TRY must be captured");
        org.junit.jupiter.api.Assertions.assertTrue(errorLogCaptured, "INSERT INTO error_log inside CATCH must be captured");
        org.junit.jupiter.api.Assertions.assertNotNull(savepointStmt, "SAVE TRANSACTION sp1 inside TRY must be recorded");
        org.junit.jupiter.api.Assertions.assertEquals("sp1", savepointStmt.getSavepointName());
        org.junit.jupiter.api.Assertions.assertNotNull(rollbackStmt, "ROLLBACK TRANSACTION sp1 inside CATCH must be recorded");
        org.junit.jupiter.api.Assertions.assertEquals("sp1", rollbackStmt.getSavepointName());
        org.junit.jupiter.api.Assertions.assertFalse(errorMessageMisrecordedAsCall,
                "ERROR_MESSAGE() must not be misrecorded as an outbound proc call");
        org.junit.jupiter.api.Assertions.assertEquals(1, r.getExceptionHandlers().size());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());
    }

    // ── Concept 4: @@TRANCOUNT checks ─────────────────────────────────────────────────────────
    @Test
    @DisplayName("Concept 4: IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; parses cleanly")
    void concept4_atAtTrancountCheck() {
        String src =
                "CREATE PROCEDURE usp_trancount_check\n" +
                "AS\nBEGIN\n" +
                "    BEGIN TRANSACTION;\n" +
                "    UPDATE tab_a SET x=1;\n" +
                "    IF @@TRANCOUNT > 0\n" +
                "        ROLLBACK TRANSACTION;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept4_atAtTrancountCheck", r);

        boolean tabACaptured = r.getTableOps().stream()
                .anyMatch(t -> "tab_a".equalsIgnoreCase(t.getTableName()));
        long ifStatements = r.getStatements().stream()
                .filter(s -> "IF".equalsIgnoreCase(s.getType())).count();
        long rollbacks = r.getStatements().stream()
                .filter(s -> "ROLLBACK".equalsIgnoreCase(s.getType())).count();

        System.out.println("concept4: tabACaptured=" + tabACaptured
                + " ifStatements=" + ifStatements + " rollbacks=" + rollbacks
                + " errors=" + r.getErrors());

        // ACTUAL: the lexer has a dedicated AT_AT_VAR token ('@@' [a-zA-Z_$][a-zA-Z_$0-9]*) that
        // is listed BEFORE AT_VAR specifically so the '@@' prefix wins tokenization, and expr's
        // alternation includes AT_AT_VAR directly (SqlServerParser.g4 lines ~162/318). So
        // "@@TRANCOUNT > 0" parses as a normal comparison expr inside ifStatement, and the
        // ROLLBACK TRANSACTION in the (unbraced) single-statement THEN branch is captured as its
        // own transactionStatement. This defensive pattern parses cleanly with no special-casing
        // needed.
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; must parse without error");
        org.junit.jupiter.api.Assertions.assertEquals(1, ifStatements);
        org.junit.jupiter.api.Assertions.assertEquals(1, rollbacks);
        org.junit.jupiter.api.Assertions.assertTrue(tabACaptured);
    }

    @Test
    @DisplayName("Concept 4b: @@TRANCOUNT used directly in a SELECT (not just IF) also parses cleanly")
    void concept4b_atAtTrancountInSelect() {
        String src =
                "CREATE PROCEDURE usp_trancount_select\n" +
                "AS\nBEGIN\n" +
                "    SELECT @@TRANCOUNT AS CurrentTranCount;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept4b_atAtTrancountInSelect", r);

        System.out.println("concept4b: errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "SELECT @@TRANCOUNT must parse without error");
    }
}
