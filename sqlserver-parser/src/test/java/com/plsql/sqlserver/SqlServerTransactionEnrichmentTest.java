package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerStatementInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Enrichment tests for TRANSACTION-statement capture (additive-only contract):
 *   * transactionDepth on BEGIN_TRAN / COMMIT / ROLLBACK (depth entered / closed)
 *   * guard {kind, line, condition} on statements inside an IF / CASE / WHILE branch
 *   * handler {name:"CATCH", line} on statements inside a TRY...CATCH's CATCH block
 *   * isPartial on ROLLBACK with a savepoint name (ROLLBACK TO / SAVE TRAN consumed)
 *   * sqlText (whitespace-collapsed) on transaction rows
 *   * SET_TRANSACTION emitted for SET IMPLICIT_TRANSACTIONS ON|OFF
 *
 * These run against the real ANTLR grammar/visitor (same style as the module's existing
 * SqlServerNestedTransactionAuditTest / SqlServerExceptionAndTransactionAuditTest).
 */
class SqlServerTransactionEnrichmentTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    /** 1-based line number of the first line containing {@code needle} (case-insensitive). */
    private static int lineOf(String src, String needle) {
        String[] lines = src.split("\n", -1);
        String up = needle.toUpperCase();
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].toUpperCase().contains(up)) return i + 1;
        }
        throw new AssertionError("fixture line not found: " + needle);
    }

    private static SqlServerStatementInfo first(List<SqlServerStatementInfo> stmts, String type) {
        return stmts.stream()
                .filter(s -> type.equalsIgnoreCase(s.getType()))
                .findFirst().orElse(null);
    }

    private static List<SqlServerStatementInfo> all(List<SqlServerStatementInfo> stmts, String type) {
        return stmts.stream()
                .filter(s -> type.equalsIgnoreCase(s.getType()))
                .toList();
    }

    private static final String FIXTURE =
            "CREATE PROCEDURE usp_txn_enrichment\n" +          // 1
            "AS\n" +                                            // 2
            "BEGIN\n" +                                         // 3
            "    SET IMPLICIT_TRANSACTIONS ON;\n" +             // 4
            "    BEGIN TRANSACTION outer_tran;\n" +             // 5
            "    IF @flag = 1\n" +                              // 6
            "    BEGIN\n" +                                     // 7
            "        BEGIN TRANSACTION inner_tran;\n" +         // 8
            "        UPDATE tab_b SET y = 2;\n" +               // 9
            "        COMMIT TRANSACTION inner_tran;\n" +        // 10
            "    END\n" +                                       // 11
            "    COMMIT TRANSACTION outer_tran;\n" +            // 12
            "    BEGIN TRY\n" +                                 // 13
            "        SAVE TRANSACTION sp1;\n" +                 // 14
            "        UPDATE tab_a SET x = 1;\n" +               // 15
            "    END TRY\n" +                                   // 16
            "    BEGIN CATCH\n" +                               // 17
            "        ROLLBACK TRANSACTION sp1;\n" +             // 18
            "    END CATCH\n" +                                 // 19
            "END";                                              // 20

    @Test
    @DisplayName("transactionDepth: outer BEGIN TRAN=1, nested-in-IF BEGIN TRAN=2, inner COMMIT=2, outer COMMIT=1")
    void transactionDepthAcrossIf() {
        SqlServerRoutineInfo r = parse(FIXTURE);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "fixture must parse cleanly");

        List<SqlServerStatementInfo> begins = all(r.getStatements(), "BEGIN_TRAN");
        List<SqlServerStatementInfo> commits = all(r.getStatements(), "COMMIT");

        org.junit.jupiter.api.Assertions.assertEquals(2, begins.size(), "two BEGIN TRANSACTION rows");
        org.junit.jupiter.api.Assertions.assertEquals(2, commits.size(), "two COMMIT rows");

        org.junit.jupiter.api.Assertions.assertEquals(1, begins.get(0).getTransactionDepth(),
                "outer BEGIN TRAN enters depth 1");
        org.junit.jupiter.api.Assertions.assertEquals("outer_tran", begins.get(0).getSavepointName(),
                "named BEGIN TRAN keeps its name in the savepointName slot");

        org.junit.jupiter.api.Assertions.assertEquals(2, begins.get(1).getTransactionDepth(),
                "nested BEGIN TRAN inside the IF enters depth 2");
        org.junit.jupiter.api.Assertions.assertEquals(2, commits.get(0).getTransactionDepth(),
                "inner COMMIT closes depth 2");
        org.junit.jupiter.api.Assertions.assertEquals(1, commits.get(1).getTransactionDepth(),
                "outer COMMIT closes depth 1");
    }

    @Test
    @DisplayName("guard: statements in the IF branch carry {kind:IF, line, condition}")
    void guardOnStatementsInsideIf() {
        SqlServerRoutineInfo r = parse(FIXTURE);
        List<SqlServerStatementInfo> begins = all(r.getStatements(), "BEGIN_TRAN");

        SqlServerStatementInfo nestedBegin = begins.get(1);
        org.junit.jupiter.api.Assertions.assertEquals("IF", nestedBegin.getGuardKind(),
                "nested BEGIN TRAN is guarded by the enclosing IF");
        org.junit.jupiter.api.Assertions.assertEquals(Integer.valueOf(lineOf(FIXTURE, "IF @flag = 1")),
                nestedBegin.getGuardLine(), "guard line is the IF keyword's line");
        org.junit.jupiter.api.Assertions.assertNotNull(nestedBegin.getGuardCondition());
        org.junit.jupiter.api.Assertions.assertTrue(nestedBegin.getGuardCondition().contains("@flag"),
                "guard condition carries the IF expression: " + nestedBegin.getGuardCondition());
        org.junit.jupiter.api.Assertions.assertTrue(nestedBegin.getGuardCondition().length() <= 200,
                "guard condition is capped at 200 chars");

        // The inner COMMIT is inside the same IF branch → same guard.
        SqlServerStatementInfo innerCommit = all(r.getStatements(), "COMMIT").get(0);
        org.junit.jupiter.api.Assertions.assertEquals("IF", innerCommit.getGuardKind());
        org.junit.jupiter.api.Assertions.assertEquals(nestedBegin.getGuardLine(), innerCommit.getGuardLine());

        // Top-level transaction rows are NOT guarded.
        org.junit.jupiter.api.Assertions.assertNull(begins.get(0).getGuardKind(),
                "outer BEGIN TRAN at routine top level has no guard");
    }

    @Test
    @DisplayName("handler: ROLLBACK inside CATCH carries {name:CATCH, line}")
    void handlerLinkageOnCatchRollback() {
        SqlServerRoutineInfo r = parse(FIXTURE);
        SqlServerStatementInfo rollback = first(r.getStatements(), "ROLLBACK");
        org.junit.jupiter.api.Assertions.assertNotNull(rollback, "ROLLBACK row must exist");

        org.junit.jupiter.api.Assertions.assertEquals("CATCH", rollback.getHandlerName(),
                "ROLLBACK in the CATCH body carries the CATCH handler");
        org.junit.jupiter.api.Assertions.assertEquals(Integer.valueOf(lineOf(FIXTURE, "BEGIN CATCH")),
                rollback.getHandlerLine(), "handler line is the BEGIN CATCH keyword's line");
    }

    @Test
    @DisplayName("isPartial: ROLLBACK TRANSACTION <savepoint> is a partial rollback and keeps the savepoint name")
    void partialRollbackToSavepoint() {
        SqlServerRoutineInfo r = parse(FIXTURE);
        SqlServerStatementInfo rollback = first(r.getStatements(), "ROLLBACK");
        org.junit.jupiter.api.Assertions.assertNotNull(rollback);

        org.junit.jupiter.api.Assertions.assertEquals(Boolean.TRUE, rollback.getIsPartial(),
                "ROLLBACK TO <savepoint> is marked isPartial");
        org.junit.jupiter.api.Assertions.assertEquals("sp1", rollback.getSavepointName(),
                "the consumed savepoint name is retained");

        // A plain (non-savepoint) rollback would not be partial — not present in this fixture, but
        // assert the SAVEPOINT row's own savepoint name is captured as today.
        SqlServerStatementInfo savepoint = first(r.getStatements(), "SAVEPOINT");
        org.junit.jupiter.api.Assertions.assertNotNull(savepoint);
        org.junit.jupiter.api.Assertions.assertEquals("sp1", savepoint.getSavepointName());
        org.junit.jupiter.api.Assertions.assertNull(savepoint.getIsPartial(),
                "SAVE TRAN itself is not a partial rollback");
    }

    @Test
    @DisplayName("SET_TRANSACTION: SET IMPLICIT_TRANSACTIONS ON emits a SET_TRANSACTION row with sqlText")
    void setImplicitTransactionsRow() {
        SqlServerRoutineInfo r = parse(FIXTURE);
        SqlServerStatementInfo st = first(r.getStatements(), "SET_TRANSACTION");
        org.junit.jupiter.api.Assertions.assertNotNull(st,
                "SET IMPLICIT_TRANSACTIONS ON must be emitted as type SET_TRANSACTION");
        org.junit.jupiter.api.Assertions.assertEquals(Integer.valueOf(lineOf(FIXTURE, "SET IMPLICIT_TRANSACTIONS")),
                st.getLine());
        org.junit.jupiter.api.Assertions.assertNotNull(st.getSqlText());
        org.junit.jupiter.api.Assertions.assertTrue(
                st.getSqlText().toUpperCase().contains("IMPLICIT_TRANSACTIONS"),
                "sqlText carries the statement text: " + st.getSqlText());

        // No generic SET row for the same statement (it is re-typed, not duplicated).
        long plainSet = r.getStatements().stream()
                .filter(s -> "SET".equalsIgnoreCase(s.getType())).count();
        org.junit.jupiter.api.Assertions.assertEquals(0, plainSet,
                "the implicit-transactions switch is not also recorded as a plain SET");
    }

    @Test
    @DisplayName("guard: statements inside a WHILE loop carry {kind:LOOP, condition}")
    void guardOnStatementsInsideWhile() {
        String src =
                "CREATE PROCEDURE usp_loop_tran\n" +
                "AS\nBEGIN\n" +
                "    WHILE @i < 10\n" +
                "    BEGIN\n" +
                "        BEGIN TRANSACTION loop_tran;\n" +
                "        COMMIT TRANSACTION loop_tran;\n" +
                "    END\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "fixture must parse cleanly");

        SqlServerStatementInfo begin = first(r.getStatements(), "BEGIN_TRAN");
        SqlServerStatementInfo commit = first(r.getStatements(), "COMMIT");
        org.junit.jupiter.api.Assertions.assertNotNull(begin);
        org.junit.jupiter.api.Assertions.assertNotNull(commit);

        org.junit.jupiter.api.Assertions.assertEquals("LOOP", begin.getGuardKind(),
                "BEGIN TRAN inside a WHILE carries a LOOP guard");
        org.junit.jupiter.api.Assertions.assertEquals(Integer.valueOf(lineOf(src, "WHILE @i < 10")),
                begin.getGuardLine());
        org.junit.jupiter.api.Assertions.assertTrue(begin.getGuardCondition().contains("@i"),
                "loop condition: " + begin.getGuardCondition());
        org.junit.jupiter.api.Assertions.assertEquals("LOOP", commit.getGuardKind(),
                "COMMIT inside the same loop carries the LOOP guard too");
        org.junit.jupiter.api.Assertions.assertEquals(1, begin.getTransactionDepth());
        org.junit.jupiter.api.Assertions.assertEquals(1, commit.getTransactionDepth());
    }

    @Test
    @DisplayName("sqlText: transaction rows carry one-line, whitespace-collapsed text (<=300 chars)")
    void sqlTextOnTransactionRows() {
        SqlServerRoutineInfo r = parse(FIXTURE);
        for (SqlServerStatementInfo s : r.getStatements()) {
            if ("BEGIN_TRAN".equals(s.getType()) || "COMMIT".equals(s.getType())
                    || "ROLLBACK".equals(s.getType()) || "SAVEPOINT".equals(s.getType())) {
                org.junit.jupiter.api.Assertions.assertNotNull(s.getSqlText(), s.getType() + " row has sqlText");
                org.junit.jupiter.api.Assertions.assertFalse(s.getSqlText().contains("\n"),
                        "sqlText is collapsed to one line");
                org.junit.jupiter.api.Assertions.assertTrue(s.getSqlText().length() <= 300);
            }
        }
        SqlServerStatementInfo outerBegin = all(r.getStatements(), "BEGIN_TRAN").get(0);
        org.junit.jupiter.api.Assertions.assertTrue(
                outerBegin.getSqlText().toUpperCase().contains("BEGIN TRANSACTION"),
                "BEGIN TRAN sqlText: " + outerBegin.getSqlText());
    }
}
